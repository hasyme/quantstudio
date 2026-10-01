"""只读取证探针 1i（收口）：偏离行中「哪一列失真」的定量判定。

对照基准 stock_daily（已证 d_ratio≡expect，完全正确）。
按 CST 交易日对齐后，对每个偏离行判定四个等式：
    mC==dC   mF==dF   mF==dC   mC==dF
命中情况直接指出错位方向：
    mF==dF 且 mC!=dC  → close_front 正确、close(raw) 失真
    mC==dC 且 mF!=dF  → close 正确、close_front 失真
    mF==dC 且 mC==dF  → 两列互换
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
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1i_out.txt"

CST = timezone(timedelta(hours=8))
BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000
T1 = int(datetime(2026, 5, 1).timestamp() * 1000)
T2 = int(datetime(2026, 8, 1).timestamp() * 1000)
TOL = 0.005   # 0.5% 相对容差（覆盖日线 2 位小数舍入）

CODES = ["603093", "600649", "600516", "000963", "600251", "600428",
         "600699", "300407", "001331", "603538", "001388", "600230",
         "688037", "600785", "301196", "688388", "688583", "688286",
         "300750", "688231", "000012", "000019", "000025", "600000",
         "600036", "000001", "600519", "000002", "600030", "601318"]

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


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


def close_to(a, b):
    if b is None or b == 0:
        return False
    return abs(a / b - 1) < TOL


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        verdict = Counter()
        per_code = []
        detail_rows = []
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
                "SELECT time, close, close_front, update_time FROM stock_minutes "
                "WHERE code=? AND time>=? AND time<? AND close>0 "
                "AND close_front IS NOT NULL "
                "AND (time % 86400000) BETWEEN ? AND ? ORDER BY time",
                [code, T1, T2, BAR_LO, BAR_HI]).fetchall()
            db = con.execute(
                "SELECT time, close, close_front FROM stock_daily "
                "WHERE code=? AND time>=? AND time<? ORDER BY time",
                [code, T1 - 86_400_000, T2 + 86_400_000]).fetchall()
            if not mb or not db:
                continue
            mdf = pd.DataFrame(mb, columns=["time", "mclose", "mfront", "ut"])
            ddf = pd.DataFrame(db, columns=["dtime", "dclose", "dfront"])
            mdf["day"] = mdf["time"].map(cday)
            ddf["day"] = ddf["dtime"].map(cday)
            j = (mdf.groupby("day").last().reset_index()
                 .merge(ddf.groupby("day").last().reset_index(), on="day"))
            j["dkey"] = pd.to_datetime(j["day"]).astype("int64") // 1_000_000
            j = pd.merge_asof(j.sort_values("dkey"), cdf.rename(
                columns={"time": "dkey"}), on="dkey", direction="backward")
            j = j.dropna(subset=["adj_factor"])
            if not len(j):
                continue
            j["expect"] = j["adj_factor"] / adj_latest
            j["dev"] = ((j["mfront"] / j["mclose"]) / j["expect"] - 1).abs()
            bad = j[j["dev"] > 0.005]
            c = Counter()
            for x in bad.itertuples():
                a = close_to(x.mclose, x.dclose)
                b = close_to(x.mfront, x.dfront)
                cc = close_to(x.mfront, x.dclose)
                d = close_to(x.mclose, x.dfront)
                if b and not a:
                    v = "mF=dF (front正确/raw失真)"
                elif a and not b:
                    v = "mC=dC (raw正确/front失真)"
                elif cc and d:
                    v = "两列互换"
                elif a and b:
                    v = "两列皆≈日线(舍入内)"
                elif cc and not d:
                    v = "mF=dC (front存了raw)"
                elif d and not cc:
                    v = "mC=dF (raw存了front)"
                else:
                    v = "四者皆不符"
                c[v] += 1
                verdict[v] += 1
                detail_rows.append((code, x.day, x.mclose, x.mfront,
                                    x.dclose, x.dfront, str(x.ut), v))
            per_code.append((code, len(bad), c.most_common(1)[0] if len(bad) else ("-", 0)))

        P("=== 偏离行「失真列」判定汇总（基准=stock_daily）===")
        tot = sum(verdict.values())
        for k, v in verdict.most_common():
            P(f"   {k:32s} {v:5d}  ({v/max(tot,1):.1%})")
        P(f"   -- 偏离行合计 {tot}")
        P("")
        P("=== 每 code 主导形态 ===")
        P(f"{'code':10s} {'偏离':>5s}  主导形态")
        for code, n, (k, v) in per_code:
            P(f"{code:10s} {n:5d}  {k} ({v})")
        P("")
        P("=== 明细（前 40 行）===")
        P(f"{'code':9s} {'day':11s} {'mC':>9s} {'mF':>9s} {'dC':>9s} "
          f"{'dF':>9s} {'update_time':>20s}  形态")
        for r in detail_rows[:40]:
            P(f"{r[0]:9s} {r[1]:11s} {r[2]:9.4f} {r[3]:9.4f} {r[4]:9.4f} "
              f"{r[5]:9.4f} {r[6]:>20s}  {r[7]}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
