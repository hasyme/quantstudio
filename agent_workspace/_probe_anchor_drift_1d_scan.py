"""只读取证探针 1d：时间剪枝广域扫描 + 机理判定。

前序结论：
  - stock_daily 比值恒等于 expect（日线正确）
  - stock_minutes 252 个 15:00 bar 中 18 个比值 = expect^2（多乘一次因子）
  - 反解写入锚 X = 30.671172 ≠ 当前 adj_latest 30.5076

本探针：
  (1) 用 time 范围剪枝（避开 1.95 亿行全表 modulo），广域扫描求真实 max-dev code
  (2) 检验 actual==expect^2 假设的覆盖率
  (3) 分钟 close 与日线 close 的一致性（跨日期）
全程只读。
"""
from __future__ import annotations
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
AUX = ROOT / "data" / "qfq_aux.db"
MAIN = ROOT / "data" / "quantstudio.db"
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1d_out.txt"

BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000
T1 = int(datetime(2026, 5, 1).timestamp() * 1000)
T2 = int(datetime(2026, 8, 1).timestamp() * 1000)

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def ts(ms):
    return datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d")


def clean_segments(pairs):
    ts_a = [p[0] for p in pairs]
    v_a = [p[1] for p in pairs]
    segs = []
    start = 0
    for i in range(1, len(v_a) + 1):
        if i == len(v_a) or v_a[i] != v_a[i - 1]:
            segs.append((int(ts_a[start]), int(ts_a[i - 1]), float(v_a[start])))
            start = i
    out = []
    for idx, (st, en, v) in enumerate(segs):
        is_spike = ((en - st) < 86_400_000 and 0 < idx < len(segs) - 1
                    and (v / segs[idx - 1][2] < 0.5 or v / segs[idx - 1][2] > 2.0))
        if not is_spike:
            out.append((st, v))
    return out


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        # 除权候选（09-26 审计口径 now=2026-09-26，窗口 [-120d,+7d]）
        NOW = datetime(2026, 9, 26, 21, 33, 22)
        lo = int((NOW - pd.Timedelta(days=120)).timestamp() * 1000)
        hi = int((NOW + pd.Timedelta(days=7)).timestamp() * 1000)
        codes = [str(r[0]) for r in con.execute(
            "SELECT DISTINCT code FROM stock_dividend WHERE ex_date BETWEEN ? AND ?",
            [lo, hi]).fetchall()]
        P(f"stock 除权候选 code 数 = {len(codes)}  (窗口 {ts(lo)}..{ts(hi)})")

        ph = ",".join("?" * len(codes))
        t0 = time.time()
        frows = aux.execute(
            f"SELECT code, time, adj_factor FROM adj_factor "
            f"WHERE code IN ({ph})", codes).fetchall()
        P(f"候选因子行数 = {len(frows):,}  读取 {time.time()-t0:.1f}s")
        fdf = pd.DataFrame(frows, columns=["code", "time", "adj_factor"])
        fdf["time"] = pd.to_numeric(fdf["time"], errors="coerce")
        fdf["adj_factor"] = pd.to_numeric(fdf["adj_factor"], errors="coerce")
        fdf = fdf.dropna()

        segs = []
        latest = {}
        for c, g in fdf.sort_values("time").groupby("code"):
            cl = clean_segments(list(zip(g["time"].astype("int64"),
                                         g["adj_factor"])))
            if not cl:
                continue
            latest[c] = cl[-1][1]
            for st, v in cl:
                segs.append((c, st, v))
        segdf = pd.DataFrame(segs, columns=["code", "time", "adj_factor"])
        P(f"清洗后段数 = {len(segdf):,}  覆盖 code = {len(latest):,}")

        # ---- 时间剪枝扫描 ----
        t0 = time.time()
        bars = con.execute(
            "SELECT code, time, close, close_front FROM stock_minutes "
            "WHERE time >= ? AND time < ? AND close > 0 "
            "AND close_front IS NOT NULL "
            "AND (time % 86400000) BETWEEN ? AND ?",
            [T1, T2, BAR_LO, BAR_HI]).fetchall()
        P(f"时间剪枝扫描 [{ts(T1)}..{ts(T2)}] bar 数 = {len(bars):,}  "
          f"耗时 {time.time()-t0:.1f}s")
        if not bars:
            raise SystemExit(0)

        bdf = pd.DataFrame(bars, columns=["code", "time", "close", "close_front"])
        bdf["time"] = bdf["time"].astype("int64")
        segdf["time"] = segdf["time"].astype("int64")
        m = pd.merge_asof(bdf.sort_values("time"), segdf.sort_values("time"),
                          on="time", by="code", direction="backward")
        m = m.dropna(subset=["adj_factor"])
        m["adj_latest"] = m["code"].map(latest)
        m = m.dropna(subset=["adj_latest"])
        m["expect"] = m["adj_factor"] / m["adj_latest"]
        m["actual"] = m["close_front"] / m["close"]
        m["dev"] = (m["actual"] / m["expect"] - 1).abs()
        m["devsq"] = (m["actual"] / m["expect"] ** 2 - 1).abs()
        P(f"可比 bar 数 = {len(m):,}  (code 数 {m['code'].nunique():,})")

        per = m.groupby("code").agg(dev=("dev", "max"),
                                    devsq=("devsq", "max"),
                                    n=("dev", "size"))
        fail = per[per["dev"] > 0.005].sort_values("dev", ascending=False)
        P("")
        P(f"=== FAIL(>0.5%) code 数 = {len(fail)} / {len(per)} ===")
        P(f"    dev   max = {per['dev'].max():.6f}")
        P(f"    devsq max = {per['devsq'].max():.6f}")
        P("")
        P(f"  {'code':10s} {'dev_max':>10s} {'devsq_max':>10s} {'n':>5s} "
          f"{'judge':>10s}")
        for c, r in fail.head(20).iterrows():
            judge = "SQ(^2)" if r["devsq"] < 1e-6 else (
                "expect" if r["dev"] < 1e-6 else "OTHER")
            P(f"  {c:10s} {r['dev']:10.6f} {r['devsq']:10.6f} {int(r['n']):5d} "
              f"{judge:>10s}")

        # ---- 假设覆盖率 ----
        P("")
        P("=== 假设覆盖率（全样本）===")
        bad = m[m["dev"] > 0.005]
        P(f"  偏离 bar 数={len(bad):,}")
        if len(bad):
            hit_sq = (bad["devsq"] < 1e-4).sum()
            P(f"  其中 actual≈expect^2 (tol 1e-4): {hit_sq:,} "
              f"({hit_sq/max(len(bad),1):.1%})")
            P(f"  devsq 中位数={bad['devsq'].median():.6f}")
            P(f"  devsq 分位: p10={bad['devsq'].quantile(.1):.6f} "
              f"p50={bad['devsq'].median():.6f} "
              f"p90={bad['devsq'].quantile(.9):.6f}")
            # 反解 X
            X = (bad["adj_factor"] / bad["actual"]).round(6)
            P(f"  反解 X(=adj_i/actual) 取值数={X.nunique()}  "
              f"样例={sorted(X.unique())[:8]}")
        # ---- 分钟 close vs 日线 close ----
        P("")
        P("=== 分钟 close vs 日线 close（000012 全窗口）===")
        d = con.execute(
            "SELECT time, close FROM stock_daily WHERE code='000012' "
            "AND time >= ? AND time < ? ORDER BY time",
            [int(datetime(2026, 6, 1).timestamp() * 1000),
             int(datetime(2026, 8, 1).timestamp() * 1000)]).fetchall()
        mm = m[m["code"] == "000012"][["time", "close", "close_front",
                                       "expect", "actual", "dev"]]
        P(f"  {'date':12s} {'d_close':>9s} {'m_close':>9s} {'m/d':>9s} "
          f"{'expect':>9s} {'m_ratio':>9s} {'dev':>9s}")
        for dtime, dclose in d:
            day = dtime // 86_400_000 * 86_400_000
            cand = mm[(mm["time"] >= day) & (mm["time"] < day + 86_400_000)]
            if not len(cand):
                continue
            r = cand.iloc[-1]
            P(f"  {ts(dtime):12s} {dclose:9.4f} {r['close']:9.4f} "
              f"{r['close']/dclose:9.6f} {r['expect']:9.6f} "
              f"{r['actual']:9.6f} {r['dev']:9.6f}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())