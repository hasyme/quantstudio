"""只读取证探针 1e：max-dev 标的 603093 深挖 + 日内序列核对。

前序：
  - 复刻扫描得到 FAIL=1095（审计 1099）、max dev=1.098838（审计 1.0988）→ 复刻保真
  - max-dev code = 603093（dev=1.0988 → actual/expect≈2.0988）
  - 分钟 close 与日线 close 出现 ±10% 级差异（000012）→ 需核实是否取错 bar
本探针：
  (A) 603093 因子段 + 偏离 bar 明细 + 反解
  (B) 000012 单日全部分钟 bar 原文（核实 15:00 bar 与日线 close 关系）
  (C) 重复 (code,time) 检查
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
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1e_out.txt"

BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def ts(ms, fmt="%Y-%m-%d %H:%M"):
    return datetime.utcfromtimestamp(int(ms) / 1000).strftime(fmt)


def clean_segments(pairs):
    a = [p[0] for p in pairs]
    v = [p[1] for p in pairs]
    segs = []
    start = 0
    for i in range(1, len(v) + 1):
        if i == len(v) or v[i] != v[i - 1]:
            segs.append((int(a[start]), int(a[i - 1]), float(v[start])))
            start = i
    out = []
    for idx, (st, en, vv) in enumerate(segs):
        is_spike = ((en - st) < 86_400_000 and 0 < idx < len(segs) - 1
                    and (vv / segs[idx - 1][2] < 0.5 or vv / segs[idx - 1][2] > 2.0))
        if not is_spike:
            out.append((st, vv))
    return segs, out


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        for code, tbl in (("603093", "stock_minutes"), ("000012", "stock_minutes")):
            P("=" * 78)
            P(f"### {code} / {tbl}")
            P("=" * 78)
            fr = aux.execute(
                "SELECT time, adj_factor FROM adj_factor WHERE code=? ORDER BY time",
                [code]).fetchall()
            raw_segs, cl = clean_segments(fr)
            adj_latest = cl[-1][1]
            P(f"  因子行数={len(fr)} 原始段={len(raw_segs)} 清洗段={len(cl)} "
              f"adj_latest={adj_latest}")
            P(f"  段 (start, value) 末 8:")
            for st, v in cl[-8:]:
                P(f"      {ts(st)}  v={v}")

            if code == "603093":
                rb = con.execute(
                    f"SELECT time, close, close_front, update_time, data_source "
                    f"FROM {tbl} WHERE code=? AND close>0 AND close_front IS NOT NULL "
                    f"AND (time % 86400000) BETWEEN ? AND ? ORDER BY time",
                    [code, BAR_LO, BAR_HI]).fetchall()
                P(f"  15:00 bar 数={len(rb)}")
                cdf = pd.DataFrame(cl, columns=["time", "adj_factor"])
                cdf["time"] = cdf["time"].astype("int64")
                bdf = pd.DataFrame(rb, columns=["time", "close", "close_front",
                                                "update_time", "data_source"])
                bdf["time"] = bdf["time"].astype("int64")
                m = pd.merge_asof(bdf, cdf, on="time", direction="backward")
                m = m.dropna(subset=["adj_factor"])
                m["expect"] = m["adj_factor"] / adj_latest
                m["actual"] = m["close_front"] / m["close"]
                m["dev"] = (m["actual"] / m["expect"] - 1).abs()
                m["r2"] = (m["actual"] / m["expect"] ** 2 - 1).abs()
                P(f"  dev max={m['dev'].max():.6f}  "
                  f"expect^2 吻合数={(m['r2']<1e-4).sum()}/{len(m)}")
                P(f"  {'time':17s} {'close':>9s} {'close_fr':>10s} {'adj_i':>10s} "
                  f"{'expect':>9s} {'actual':>9s} {'dev':>9s} {'update_time':>20s}")
                for x in m.sort_values("dev", ascending=False).head(8).itertuples():
                    P(f"  {ts(x.time):17s} {x.close:9.4f} {x.close_front:10.4f} "
                      f"{x.adj_factor:10.4f} {x.expect:9.6f} {x.actual:9.6f} "
                      f"{x.dev:9.4f} {str(x.update_time):>20s}")
                # 反解
                bad = m[m["dev"] > 0.005]
                if len(bad):
                    X = (bad["adj_factor"] / bad["actual"]).round(6)
                    P(f"  反解 X = adj_i/actual : 唯一值={sorted(set(X))[:10]}")
                    P(f"  因子段值 = {sorted({round(v,6) for _, v in cl})}")
                    P(f"  adj_i 分布 = {sorted(bad['adj_factor'].unique())}")
                    P(f"  update_time 分布 = {sorted(bad['update_time'].unique())[:6]}")

            # (B) 000012 单日全 bar
            if code == "000012":
                for day in ("2026-06-15", "2026-07-01", "2026-07-30"):
                    d0 = int(datetime.strptime(day, "%Y-%m-%d").timestamp() * 1000)
                    rows = con.execute(
                        f"SELECT time, close, close_front FROM {tbl} "
                        f"WHERE code=? AND time >= ? AND time < ? ORDER BY time",
                        [code, d0, d0 + 86_400_000]).fetchall()
                    dr = con.execute(
                        "SELECT time, close, open, high, low, close_front "
                        "FROM stock_daily WHERE code=? AND time >= ? AND time < ?",
                        [code, d0, d0 + 86_400_000]).fetchall()
                    P("")
                    P(f"  --- {day}  分钟 bar 数={len(rows)}  日线={dr} ---")
                    if rows:
                        P(f"      首: {ts(rows[0][0])} close={rows[0][1]} "
                          f"front={rows[0][2]}")
                        P(f"      末: {ts(rows[-1][0])} close={rows[-1][1]} "
                          f"front={rows[-1][2]}")
                        cs = [r[1] for r in rows]
                        P(f"      close min={min(cs)} max={max(cs)}")
                        for r in rows:
                            if (r[0] % 86400000) in (BAR_LO, BAR_LO+60000,
                                                     BAR_HI-60000, BAR_HI):
                                P(f"      bar {ts(r[0])} close={r[1]} "
                                  f"front={r[2]}")
            P("")

        # (C) 重复 (code,time)
        P("=" * 78)
        P("### (C) 重复 (code,time) 检查（000012 / 603093）")
        for code in ("000012", "603093"):
            r = con.execute(
                "SELECT COUNT(*) AS n, COUNT(DISTINCT time) AS d "
                "FROM stock_minutes WHERE code=?", [code]).fetchone()
            P(f"  {code}: rows={r[0]:,}  distinct_time={r[1]:,}  "
              f"重复={r[0]-r[1]:,}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())