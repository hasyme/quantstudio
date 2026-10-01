"""只读取证探针 1b：按 code 定向深挖（快）。

策略：不做全表 modulo 扫描（stock_minutes 1.95 亿行 → 超时）；
改为「先按 code 定位（每 code 仅数万行）再算 15:00 bar」，
对 max-dev 领跑标的逐 bar 取证。

全程只读。
"""
from __future__ import annotations
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
AUX = ROOT / "data" / "qfq_aux.db"
MAIN = ROOT / "data" / "quantstudio.db"
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1b_out.txt"

BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000

TARGETS = [
    ("stock_minutes", "adj_factor", ["000012", "000019", "000025"]),
    ("etf_minutes", "fund_adj", ["159119", "159209"]),
]

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def ts(ms):
    return datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M")


def clean_segments(pairs):
    """(time, val) 序列 → 清洗后段 (seg_start, val)。逐字复刻审计算法。"""
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
    return segs, out


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        for tbl, ftbl, codes in TARGETS:
            for code in codes:
                P("=" * 76)
                P(f"### {code} / {tbl} / {ftbl}")
                P("=" * 76)

                # --- 因子序列 ---
                frows = aux.execute(
                    f"SELECT time, adj_factor FROM {ftbl} WHERE code=? ORDER BY time",
                    [code]).fetchall()
                if not frows:
                    P("  无因子行 → skip")
                    continue
                P(f"  因子行数={len(frows)}  首={frows[0]}  末={frows[-1]}")
                raw_segs, cl_segs = clean_segments(frows)
                P(f"  原始段数={len(raw_segs)}  清洗后段数={len(cl_segs)} "
                  f"(被剔尖刺={len(raw_segs)-len(cl_segs)})")
                P(f"  原始段 末6 (start→end, v):")
                for st, en, v in raw_segs[-6:]:
                    P(f"      {ts(st)} → {ts(en)}  v={v}")
                adj_latest = cl_segs[-1][1] if cl_segs else None
                P(f"  adj_latest(清洗后末段) = {adj_latest}")

                # --- 该 code 的 bar ---
                r = con.execute(
                    f"SELECT COUNT(*), MIN(time), MAX(time) FROM {tbl} WHERE code=?",
                    [code]).fetchone()
                P(f"  bar 行数={r[0]:,}  跨度 {ts(r[1])} → {ts(r[2])}")
                nfront = con.execute(
                    f"SELECT COUNT(*) FROM {tbl} WHERE code=? AND close_front IS NOT NULL",
                    [code]).fetchone()[0]
                P(f"  close_front 非空行数 = {nfront:,}")

                bars = con.execute(
                    f"SELECT time, close, close_front FROM {tbl} "
                    f"WHERE code=? AND close>0 AND close_front IS NOT NULL "
                    f"AND (time % 86400000) BETWEEN ? AND ? ORDER BY time",
                    [code, BAR_LO, BAR_HI]).fetchall()
                P(f"  14:59-15:01 收盘 bar 数 = {len(bars)}")
                if not bars:
                    continue

                # --- merge_asof: 每 bar 匹配其所处因子段 ---
                cdf = pd.DataFrame(cl_segs, columns=["time", "adj_factor"])
                bdf = pd.DataFrame(bars, columns=["time", "close", "close_front"])
                m = pd.merge_asof(bdf.sort_values("time"),
                                  cdf.sort_values("time"),
                                  on="time", direction="backward")
                m = m.dropna(subset=["adj_factor"])
                m["expect"] = m["adj_factor"] / adj_latest
                m["actual"] = m["close_front"] / m["close"]
                m["dev"] = (m["actual"] / m["expect"] - 1).abs()
                P(f"  dev: max={m['dev'].max():.6f} med={m['dev'].median():.6f} "
                  f"n(>0.5%)={(m['dev']>0.005).sum()}")

                top = m.sort_values("dev", ascending=False).head(6)
                P(f"  {'time':17s} {'close':>9s} {'close_fr':>10s} {'adj_i':>10s} "
                  f"{'expect':>10s} {'actual':>10s} {'dev':>9s}")
                for x in top.itertuples():
                    P(f"  {ts(x.time):17s} {x.close:9.4f} {x.close_front:10.4f} "
                      f"{x.adj_factor:10.4f} {x.expect:10.6f} {x.actual:10.6f} "
                      f"{x.dev:9.4f}")

                # --- 决定性判据 ---
                t0 = top.iloc[0]
                implied = float(t0["actual"]) * float(adj_latest)
                P(f"  -- 判据 --")
                P(f"     审计 adj_i      = {t0['adj_factor']:.6f} @ {ts(t0['time'])}")
                P(f"     implied adj_i   = actual*adj_latest = {implied:.6f}")
                P(f"     implied/审计adj_i = {implied/float(t0['adj_factor']):.6f}")
                vals = sorted({round(v, 6) for _, _, v in raw_segs})
                hit = [v for v in vals if abs(v - implied) <= max(1e-4, implied*1e-5)]
                P(f"     implied 命中因子段值: {hit if hit else '未命中'}")
                P(f"     因子段值(去重)={vals}")
                # actual 是否恒定（锚恒差特征）
                P(f"     actual 唯一值数={m['actual'].nunique()}  "
                  f"min={m['actual'].min():.6f} max={m['actual'].max():.6f}")
                P(f"     expect 唯一值数={m['expect'].nunique()}  "
                  f"min={m['expect'].min():.6f} max={m['expect'].max():.6f}")
                # 偏离 bar 的时间分布（是否集中在某日/某段）
                bad = m[m["dev"] > 0.005]
                if len(bad):
                    P(f"     偏离 bar 时间范围: {ts(bad['time'].min())} → "
                      f"{ts(bad['time'].max())}")
                    P(f"     偏离 bar 所处 adj_i 值: "
                      f"{sorted(bad['adj_factor'].unique())}")
                P("")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
