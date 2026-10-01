"""只读取证探针 1c：double-adjustment 判定 + 分钟/日线对照。

已知（探针 1b）：3/3 股票 code 上 actual == expect**2（6 位小数精确）。
本探针决定性验证：
  (a) 偏离 bar 的整行原始字段（data_source / update_time / *_ratio）
  (b) 同 code 同日期：stock_daily 的 close/close_front 比值 vs stock_minutes
      —— 若日线正确、分钟平方 → 缺陷在分钟写侧
  (c) expect**2 假设在更大样本上的命中率

全程只读。
"""
from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
AUX = ROOT / "data" / "qfq_aux.db"
MAIN = ROOT / "data" / "quantstudio.db"
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1c_out.txt"

BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def ts(ms):
    return datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M")


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
        code = "000012"
        frows = aux.execute(
            "SELECT time, adj_factor FROM adj_factor WHERE code=? ORDER BY time",
            [code]).fetchall()
        cl = clean_segments(frows)
        adj_latest = cl[-1][1]
        cdf = pd.DataFrame(cl, columns=["time", "adj_factor"])
        P(f"### {code}  adj_latest={adj_latest}  段数={len(cl)}")

        # ---------- (a) 偏离 bar 整行 ----------
        P("")
        P("--- (a) 偏离 bar 整行（含 data_source/update_time） ---")
        rows = con.execute(
            "SELECT * FROM stock_minutes WHERE code=? AND close>0 "
            "AND close_front IS NOT NULL AND (time % 86400000) BETWEEN ? AND ? "
            "AND time >= ? AND time < ? ORDER BY time",
            [code, BAR_LO, BAR_HI,
             int(datetime(2026, 6, 20).timestamp() * 1000),
             int(datetime(2026, 7, 10).timestamp() * 1000)]).fetchall()
        cols = [d[0] for d in con.description]
        keep = ["time", "close", "close_front", "close_front_ratio",
                "close_back", "close_back_ratio", "dividend_type",
                "data_source", "update_time", "preClose"]
        idx = {c: cols.index(c) for c in keep if c in cols}
        P(f"  列={list(idx.keys())}")
        for r in rows[:8]:
            pretty = {k: r[i] for k, i in idx.items()}
            pretty["time"] = ts(pretty["time"])
            if pretty.get("close") and pretty.get("close_front"):
                pretty["cf_over_c"] = round(pretty["close_front"] / pretty["close"], 6)
            P(f"  {pretty}")

        # ---------- (b) 日线 vs 分钟 对照 ----------
        P("")
        P("--- (b) stock_daily vs stock_minutes 同 code 同日期 ---")
        drows = con.execute(
            "SELECT time, close, close_front, open, open_front FROM stock_daily "
            "WHERE code=? AND time >= ? AND time < ? ORDER BY time",
            [code,
             int(datetime(2026, 6, 20).timestamp() * 1000),
             int(datetime(2026, 7, 10).timestamp() * 1000)]).fetchall()
        P(f"  stock_daily 行数={len(drows)}")
        P(f"  {'date':12s} {'d_close':>9s} {'d_front':>9s} {'d_ratio':>9s} "
          f"{'m_close':>9s} {'m_front':>9s} {'m_ratio':>9s} {'expect':>9s}")
        mrows = con.execute(
            "SELECT time, close, close_front FROM stock_minutes "
            "WHERE code=? AND close>0 AND close_front IS NOT NULL "
            "AND (time % 86400000) BETWEEN ? AND ? "
            "AND time >= ? AND time < ? ORDER BY time",
            [code, BAR_LO, BAR_HI,
             int(datetime(2026, 6, 20).timestamp() * 1000),
             int(datetime(2026, 7, 10).timestamp() * 1000)]).fetchall()
        mb = pd.DataFrame(mrows, columns=["time", "m_close", "m_front"])
        for d in drows:
            dtime = int(d[0])
            dd = datetime.utcfromtimestamp(dtime / 1000).strftime("%Y-%m-%d")
            mm = mb[(mb["time"] >= dtime) & (mb["time"] < dtime + 86_400_000)]
            mrow = mm.iloc[0] if len(mm) else None
            j = pd.merge_asof(pd.DataFrame([{"time": dtime}]).astype({"time": "int64"}),
                              cdf.astype({"time": "int64"}),
                              on="time", direction="backward")
            adj_i = float(j["adj_factor"].iloc[0])
            expect = adj_i / adj_latest
            d_ratio = d[2] / d[1] if (d[1] and d[2]) else None
            if mrow is not None:
                m_ratio = mrow["m_front"] / mrow["m_close"]
                P(f"  {dd:12s} {d[1]:9.4f} {d[2]:9.4f} "
                  f"{d_ratio:9.6f} {mrow['m_close']:9.4f} {mrow['m_front']:9.4f} "
                  f"{m_ratio:9.6f} {expect:9.6f}")
            else:
                P(f"  {dd:12s} {d[1]:9.4f} {d[2]:9.4f} {d_ratio:9.6f} "
                  f"{'(无分钟)':>9s}")

        # ---------- (c) 全量：actual vs expect**2 命中率 ----------
        P("")
        P("--- (c) 全表 000012：actual 与 expect / expect^2 的吻合度 ---")
        bars = con.execute(
            "SELECT time, close, close_front FROM stock_minutes "
            "WHERE code=? AND close>0 AND close_front IS NOT NULL "
            "AND (time % 86400000) BETWEEN ? AND ? ORDER BY time",
            [code, BAR_LO, BAR_HI]).fetchall()
        bdf = pd.DataFrame(bars, columns=["time", "close", "close_front"]).astype(
            {"time": "int64"})
        m = pd.merge_asof(bdf, cdf.astype({"time": "int64"}),
                          on="time", direction="backward").dropna(subset=["adj_factor"])
        m["expect"] = m["adj_factor"] / adj_latest
        m["actual"] = m["close_front"] / m["close"]
        m["dev"] = (m["actual"] / m["expect"] - 1).abs()
        m["dev_sq"] = (m["actual"] / (m["expect"] ** 2) - 1).abs()
        P(f"  bar 数={len(m)}")
        P(f"  |actual/expect - 1|      : max={m['dev'].max():.6f}  "
          f">0.5%={(m['dev']>0.005).sum()}")
        P(f"  |actual/expect^2 - 1|    : max={m['dev_sq'].max():.6f}  "
          f">0.5%={(m['dev_sq']>0.005).sum()}")
        P(f"  -> 若后者显著更小，则 actual==expect^2 成立（双重复权实锤）")
        # 分段看
        P("")
        P(f"  {'adj_i':>10s} {'expect':>10s} {'n':>6s} {'dev_med':>10s} "
          f"{'devsq_med':>10s}")
        for v, g in m.groupby("adj_factor"):
            P(f"  {v:10.4f} {g['expect'].iloc[0]:10.6f} {len(g):6d} "
              f"{g['dev'].median():10.6f} {g['dev_sq'].median():10.6f}")

        # ---------- (d) 独立算出「写入时所用锚」 ----------
        P("")
        P("--- (d) 反解：close_front/close = adj_i / X  →  X = ? ---")
        sub = m[m["dev"] > 0.005]
        if len(sub):
            xs = (sub["adj_factor"] / (sub["close_front"] / sub["close"])).unique()
            P(f"  偏离 bar 反解 X 值: {sorted(set(round(float(x), 6) for x in xs))}")
            P(f"  当前 adj_latest          = {adj_latest}")
            P(f"  因子段值(去重)={sorted({round(p[1],6) for p in cl})}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
