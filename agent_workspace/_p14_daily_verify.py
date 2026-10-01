"""P1-4（只读）：全量验证 stock_daily 参考真值。

验证 stock_daily 的 close_front 契约 close_front == close × adj_i/adj_latest
在全表（时间剪枝）尺度是否成立 —— 这是方案 A 拿 stock_daily.close 作「参考真值」的
前提。若 stock_daily 自身也有偏差，方案 A 的判定器会误判。

输出：全表命中率、偏差分布、以及有偏差的 (code,date) 明细（若有）。
全程只读。
"""
from __future__ import annotations
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
AUX = ROOT / "data" / "qfq_aux.db"
MAIN = ROOT / "data" / "quantstudio.db"
OUT = ROOT / "agent_workspace" / "_p14_daily_out.txt"

CST = timezone(timedelta(hours=8))
T1 = int(datetime(2026, 3, 1).timestamp() * 1000)
T2 = int(datetime(2026, 9, 27).timestamp() * 1000)

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def cday(ms):
    return datetime.fromtimestamp(int(ms) / 1000, CST).strftime("%Y-%m-%d")


def clean_segments(pairs):
    a = [p[0] for p in pairs]
    v = [p[1] for p in pairs]
    segs, start = [], 0
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
    return out


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        P("读取全量 adj_factor（STOCK）...")
        frows = aux.execute("SELECT code, time, adj_factor FROM adj_factor").fetchall()
        fdf = pd.DataFrame(frows, columns=["code", "time", "adj_factor"])
        fdf["time"] = pd.to_numeric(fdf["time"], errors="coerce")
        fdf["adj_factor"] = pd.to_numeric(fdf["adj_factor"], errors="coerce")
        fdf = fdf.dropna()
        P(f"  因子行 {len(fdf):,}")
        segs, latest = [], {}
        for c, g in fdf.sort_values("time").groupby("code"):
            cl = clean_segments(list(zip(g["time"].astype("int64"), g["adj_factor"])))
            if not cl:
                continue
            latest[c] = cl[-1][1]
            segs.extend((c, st, v) for st, v in cl)
        segdf = pd.DataFrame(segs, columns=["code", "time", "adj_factor"])
        segdf["time"] = segdf["time"].astype("int64")
        P(f"  段 {len(segdf):,} / code {len(latest):,}")

        P("扫描 stock_daily（时间剪枝）...")
        db = con.execute(
            "SELECT code, time, close, close_front FROM stock_daily "
            "WHERE time>=? AND time<?", [T1, T2]).fetchall()
        P(f"  {len(db):,} 行")
        ddf = pd.DataFrame(db, columns=["code", "time", "close", "close_front"])
        ddf["time"] = ddf["time"].astype("int64")
        j = pd.merge_asof(ddf.sort_values("time"), segdf.sort_values("time"),
                          on="time", by="code", direction="backward")
        j = j.dropna(subset=["adj_factor"])
        j["adj_latest"] = j["code"].map(latest)
        j = j.dropna(subset=["adj_latest"])
        j["expect_front"] = j["close"] * j["adj_factor"] / j["adj_latest"]
        j["front_ok"] = (j["close_front"] - j["expect_front"]).abs() <= 0.01
        P(f"可比行 = {len(j):,}")

        P("")
        P("=" * 70)
        P("P1-4 stock_daily 契约验证结果")
        P("=" * 70)
        P(f"可比行                     = {len(j):,}")
        P(f"close_front == close×adj_i/adj_latest 命中 = "
          f"{int(j['front_ok'].sum()):,} ({j['front_ok'].mean():.2%})")
        P("")
        # 相对误差分布
        rel = (j["close_front"] / j["expect_front"] - 1).abs()
        P("相对误差分布（|front/expect - 1|）：")
        for thr in (1e-9, 0.001, 0.005, 0.01, 0.05, 0.2, 0.5, 1.0):
            P(f"   >{thr:8.1%} : {(rel > thr).sum():8,}")
        P("")
        bad = j[~j["front_ok"]].copy()
        P(f"偏差行（|front-expect|>0.01 元）= {len(bad):,}")
        if len(bad):
            P(f"  偏差 code 数 = {bad['code'].nunique()}")
            P("  偏差行明细（前 20，按相对误差降序）：")
            bad["rel"] = (bad["close_front"] / bad["expect_front"] - 1).abs()
            P(f"    {'code':9s} {'day':11s} {'close':>9s} {'close_front':>11s} "
              f"{'expect_front':>12s} {'rel':>9s} {'adj_i':>9s} {'adj_lat':>9s}")
            for x in bad.sort_values("rel", ascending=False).head(20).itertuples():
                P(f"    {str(x.code):9s} {cday(x.time):11s} {x.close:9.4f} "
                  f"{x.close_front:11.4f} {x.expect_front:12.4f} {x.rel:9.2%} "
                  f"{x.adj_factor:9.4f} {x.adj_latest:9.4f}")
            # 偏差集中在哪些 update_time?
            P("")
            P("  偏差行按 (code, 日期) 是否集中在特定日期带：")
            bad["d"] = bad["time"].map(cday)
            P(f"    日期跨度 {bad['d'].min()} ~ {bad['d'].max()}")
            P(f"    按 code 计数 top15:")
            for c2, n2 in bad["code"].value_counts().head(15).items():
                P(f"      {c2}  {n2}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
