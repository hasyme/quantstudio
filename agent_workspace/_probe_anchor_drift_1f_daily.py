"""只读取证探针 1f：判定 stock_minutes 的哪一列失真（对照 stock_daily 真值）。

假设检验：stock_minutes.close / close_front 与 stock_daily 的对应关系。
对每个高 dev code，在相同日期比较四个比值：
    m_close/d_close, m_front/d_front, m_close/d_front, m_front/d_close
哪个恒 ≈1，即说明该列与日线该列同源（日线已被证明正确）。
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
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1f_out.txt"

BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000
T1 = int(datetime(2026, 5, 1).timestamp() * 1000)
T2 = int(datetime(2026, 8, 1).timestamp() * 1000)

CODES = ["603093", "600649", "600516", "000963", "600251", "600428",
         "600699", "300407", "001331", "603538", "001388", "600230",
         "688037", "600785", "301196", "688388", "688583", "688286",
         "300750", "688231", "000012", "000019", "000025"]

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def ts(ms, fmt="%Y-%m-%d"):
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
    return out


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        P(f"{'code':8s} {'n':>4s} {'mC/dC':>10s} {'mF/dF':>10s} "
          f"{'mC/dF':>10s} {'mF/dC':>10s}  {'dev_max':>9s}  判定")
        P("-" * 100)
        summary = {"mC=dC_and_mF=dF": 0, "mC=dF_and_mF=dC": 0,
                   "mC=dC_only": 0, "mF=dF_only": 0, "neither": 0}
        for code in CODES:
            fr = aux.execute(
                "SELECT time, adj_factor FROM adj_factor WHERE code=? ORDER BY time",
                [code]).fetchall()
            if not fr:
                continue
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
                [code, T1, T2]).fetchall()
            if not mb or not db:
                continue
            mdf = pd.DataFrame(mb, columns=["mtime", "mclose", "mfront"])
            ddf = pd.DataFrame(db, columns=["dtime", "dclose", "dfront"])
            mdf["day"] = (mdf["mtime"] // 86_400_000) * 86_400_000
            ddf["day"] = (ddf["dtime"] // 86_400_000) * 86_400_000
            mg = mdf.groupby("day").last().reset_index()
            j = mg.merge(ddf, on="day", how="inner")
            if not len(j):
                continue
            j = pd.merge_asof(j.sort_values("day"), cdf.rename(
                columns={"time": "day"}), on="day", direction="backward")
            j = j.dropna(subset=["adj_factor"])
            if not len(j):
                continue
            j["expect"] = j["adj_factor"] / adj_latest
            j["dev"] = ((j["mfront"] / j["mclose"]) / j["expect"] - 1).abs()

            def med(a, b):
                r = (j[a] / j[b]).replace([float("inf")], pd.NA).dropna()
                return r.median() if len(r) else float("nan")

            r1, r2 = med("mclose", "dclose"), med("mfront", "dfront")
            r3, r4 = med("mclose", "dfront"), med("mfront", "dclose")
            flags = []
            if abs(r1 - 1) < 0.02:
                flags.append("mC=dC")
            if abs(r2 - 1) < 0.02:
                flags.append("mF=dF")
            if abs(r3 - 1) < 0.02:
                flags.append("mC=dF")
            if abs(r4 - 1) < 0.02:
                flags.append("mF=dC")
            verdict = "+".join(flags) if flags else "neither"
            if abs(r1 - 1) < 0.02 and abs(r2 - 1) < 0.02:
                summary["mC=dC_and_mF=dF"] += 1
            elif abs(r3 - 1) < 0.02 and abs(r4 - 1) < 0.02:
                summary["mC=dF_and_mF=dC"] += 1
            elif abs(r1 - 1) < 0.02:
                summary["mC=dC_only"] += 1
            elif abs(r2 - 1) < 0.02:
                summary["mF=dF_only"] += 1
            else:
                summary["neither"] += 1
            P(f"{code:8s} {len(j):4d} {r1:10.6f} {r2:10.6f} {r3:10.6f} "
              f"{r4:10.6f}  {j['dev'].max():9.6f}  {verdict}")

        P("")
        P("=== 归类汇总 ===")
        for k, v in summary.items():
            P(f"  {k:22s} {v}")

        # --- 单个标的完整对照表 ---
        for code in ("603093", "000012"):
            P("")
            P("=" * 96)
            P(f"### {code} 分钟 vs 日线 逐日对照")
            P("=" * 96)
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
                [code, T1, T2]).fetchall()
            mdf = pd.DataFrame(mb, columns=["mtime", "mclose", "mfront"])
            ddf = pd.DataFrame(db, columns=["dtime", "dclose", "dfront"])
            mdf["day"] = (mdf["mtime"] // 86_400_000) * 86_400_000
            ddf["day"] = (ddf["dtime"] // 86_400_000) * 86_400_000
            j = (mdf.groupby("day").last().reset_index()
                 .merge(ddf, on="day", how="inner"))
            j = pd.merge_asof(j.sort_values("day"), cdf.rename(
                columns={"time": "day"}), on="day", direction="backward")
            j = j.dropna(subset=["adj_factor"])
            j["expect"] = j["adj_factor"] / adj_latest
            j["m_ratio"] = j["mfront"] / j["mclose"]
            j["d_ratio"] = j["dfront"] / j["dclose"]
            j["dev"] = (j["m_ratio"] / j["expect"] - 1).abs()
            P(f"  adj_latest={adj_latest}  adj_i 取值="
              f"{sorted(j['adj_factor'].unique())}")
            P(f"  {'date':12s} {'dC':>8s} {'dF':>8s} {'d_ratio':>9s} "
              f"{'mC':>9s} {'mF':>8s} {'m_ratio':>9s} {'expect':>9s} "
              f"{'dev':>8s}")
            for x in j.sort_values("day").itertuples():
                P(f"  {ts(x.day):12s} {x.dclose:8.4f} {x.dfront:8.4f} "
                  f"{x.d_ratio:9.6f} {x.mclose:9.4f} {x.mfront:8.4f} "
                  f"{x.m_ratio:9.6f} {x.expect:9.6f} {x.dev:8.4f}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
