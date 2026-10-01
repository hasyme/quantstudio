"""只读取证探针 1g：时间戳口径对齐核验（防自造 off-by-one 假象）。

背景：daily.time 与 minute.time 可能采用不同基准
  - 日线常存「当日 00:00 北京」= 前一日 16:00 UTC
  - 分钟存实际 bar 时刻（15:00 北京 = 07:00 UTC）
若按 UTC 日桶 join 会整体错配一天，制造「分钟滞后一日」的假象。
本探针打印原始 ms + UTC + 北京时间，按**北京日期**重新对齐。
全程只读。
"""
from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
AUX = ROOT / "data" / "qfq_aux.db"
MAIN = ROOT / "data" / "quantstudio.db"
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1g_out.txt"

CST = timezone(timedelta(hours=8))
BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000
T1 = int(datetime(2026, 5, 1).timestamp() * 1000)
T2 = int(datetime(2026, 8, 1).timestamp() * 1000)

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def u(ms):
    return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).strftime("%m-%d %H:%M")


def c(ms):
    return datetime.fromtimestamp(int(ms) / 1000, CST).strftime("%m-%d %H:%M")


def cday(ms):
    return datetime.fromtimestamp(int(ms) / 1000, CST).strftime("%Y-%m-%d")


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
    return out


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        for code in ("000012", "603093"):
            P("=" * 104)
            P(f"### {code}")
            P("=" * 104)
            # 原始时间戳样本
            P("-- 日线原始 time (末5) --")
            for t, cl_, cf in con.execute(
                    "SELECT time, close, close_front FROM stock_daily WHERE code=? "
                    "ORDER BY time DESC LIMIT 5", [code]).fetchall():
                P(f"   ms={t}  UTC={u(t)}  CST={c(t)}  CST日={cday(t)}  "
                  f"close={cl_}  front={cf}")
            P("-- 分钟 15:00 bar 原始 time (末5) --")
            for t, cl_, cf in con.execute(
                    "SELECT time, close, close_front FROM stock_minutes WHERE code=? "
                    "AND (time % 86400000) BETWEEN ? AND ? ORDER BY time DESC LIMIT 5",
                    [code, BAR_LO, BAR_HI]).fetchall():
                P(f"   ms={t}  UTC={u(t)}  CST={c(t)}  CST日={cday(t)}  "
                  f"close={cl_}  front={cf}")

            # 按 CST 日期对齐
            fr = aux.execute(
                "SELECT time, adj_factor FROM adj_factor WHERE code=? ORDER BY time",
                [code]).fetchall()
            cl = clean_segments(fr)
            adj_latest = cl[-1][1]
            cdf = pd.DataFrame(cl, columns=["time", "adj_factor"])
            cdf["time"] = cdf["time"].astype("int64")

            mb = con.execute(
                "SELECT time, close, close_front FROM stock_minutes "
                "WHERE code=? AND time>=? AND time<? AND close>0 "
                "AND close_front IS NOT NULL "
                "AND (time % 86400000) BETWEEN ? AND ? ORDER BY time",
                [code, T1, T2, BAR_LO, BAR_HI]).fetchall()
            db = con.execute(
                "SELECT time, close, close_front FROM stock_daily "
                "WHERE code=? AND time>=? AND time<? ORDER BY time",
                [code, T1 - 86_400_000, T2 + 86_400_000]).fetchall()
            mdf = pd.DataFrame(mb, columns=["mtime", "mclose", "mfront"])
            ddf = pd.DataFrame(db, columns=["dtime", "dclose", "dfront"])
            # 关键：按 CST 交易日对齐
            mdf["day"] = mdf["mtime"].map(cday)
            ddf["day"] = ddf["dtime"].map(cday)
            mg = mdf.groupby("day").last().reset_index()
            dg = ddf.groupby("day").last().reset_index()
            j = mg.merge(dg, on="day", how="inner")
            j["dkey"] = pd.to_datetime(j["day"]).astype("int64") // 1_000_000
            j = pd.merge_asof(j.sort_values("dkey"), cdf.rename(
                columns={"time": "dkey"}), on="dkey", direction="backward")
            j = j.dropna(subset=["adj_factor"])
            j["expect"] = j["adj_factor"] / adj_latest
            j["m_ratio"] = j["mfront"] / j["mclose"]
            j["d_ratio"] = j["dfront"] / j["dclose"]
            j["dev"] = (j["m_ratio"] / j["expect"] - 1).abs()
            j["mc_eq_dc"] = (j["mclose"] - j["dclose"]).abs() < 0.005
            P("")
            P(f"-- 按 CST 交易日对齐  adj_latest={adj_latest} "
              f"adj_i={sorted(j['adj_factor'].unique())} --")
            P(f"   {'day':12s} {'dC':>8s} {'mC':>8s} {'mC==dC':>7s} "
              f"{'d_ratio':>9s} {'m_ratio':>9s} {'expect':>9s} {'dev':>8s}")
            for x in j.sort_values("day").itertuples():
                P(f"   {x.day:12s} {x.dclose:8.4f} {x.mclose:8.4f} "
                  f"{'YES' if x.mc_eq_dc else 'no':>7s} {x.d_ratio:9.6f} "
                  f"{x.m_ratio:9.6f} {x.expect:9.6f} {x.dev:8.4f}")
            P("")
            P(f"   匹配日数={len(j)}  mC==dC 命中={j['mc_eq_dc'].sum()} "
              f"({j['mc_eq_dc'].mean():.1%})")
            P(f"   d_ratio==expect 命中={( (j['d_ratio']-j['expect']).abs()<1e-6 ).sum()}"
              f"/{len(j)}")
            P(f"   m_ratio==expect 命中={( (j['m_ratio']-j['expect']).abs()<1e-6 ).sum()}"
              f"/{len(j)}")
            P("")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
