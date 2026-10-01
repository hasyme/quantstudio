"""只读取证探针 1h：缺陷范围量化 + 写入批次关联（收口取证）。

已验证：
  - stock_daily 的 d_ratio 恒 == expect（100% 正确）
  - stock_minutes 偏离日 == close 与日线 raw 不一致之日
  - 偏离行带特定 update_time（000012→2026-09-07；603093→2026-08-09）

本探针量化：
  (1) 偏离行 vs 正常行的 update_time 分布（定位肇事写入批次）
  (2) 偏离日集合（是否成批、成段）
  (3) close 与「raw / 后复权」何者相符（判定列语义错位方向）
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
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1h_out.txt"

CST = timezone(timedelta(hours=8))
BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000
T1 = int(datetime(2026, 5, 1).timestamp() * 1000)
T2 = int(datetime(2026, 8, 1).timestamp() * 1000)

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


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        bad_ut: Counter = Counter()
        good_ut: Counter = Counter()
        bad_days: Counter = Counter()
        rows_stat = []
        col_semantics = Counter()
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
                "SELECT time, close, close_front, update_time, open, high, low, "
                "open_front, high_front, low_front FROM stock_minutes "
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
            mdf = pd.DataFrame(mb, columns=["time", "close", "close_front",
                                            "update_time", "open", "high", "low",
                                            "open_front", "high_front",
                                            "low_front"])
            ddf = pd.DataFrame(db, columns=["dtime", "dclose", "dfront"])
            mdf["day"] = mdf["time"].map(cday)
            ddf["day"] = ddf["dtime"].map(cday)
            mg = mdf.groupby("day").last().reset_index()
            dg = ddf.groupby("day").last().reset_index()
            j = mg.merge(dg, on="day", how="inner")
            j["dkey"] = pd.to_datetime(j["day"]).astype("int64") // 1_000_000
            j = pd.merge_asof(j.sort_values("dkey"), cdf.rename(
                columns={"time": "dkey"}), on="dkey", direction="backward")
            j = j.dropna(subset=["adj_factor"])
            if not len(j):
                continue
            j["expect"] = j["adj_factor"] / adj_latest
            j["m_ratio"] = j["close_front"] / j["close"]
            j["dev"] = (j["m_ratio"] / j["expect"] - 1).abs()
            j["inv_expect"] = 1.0 / j["expect"]
            bad = j[j["dev"] > 0.005]
            good = j[j["dev"] <= 0.005]
            for x in bad.itertuples():
                bad_ut[str(x.update_time)[:10]] += 1
                bad_days[x.day] += 1
                # 语义判定：close 是否 ≈ dclose*inv_expect（后复权）还是 ≈ dclose（raw）
                r_back = abs(x.close / (x.dclose * x.inv_expect) - 1) if x.dclose else 9
                r_raw = abs(x.close / x.dclose - 1) if x.dclose else 9
                r_fwd = abs(x.close / (x.dclose * x.expect) - 1) if x.dclose else 9
                if r_raw < 0.005:
                    col_semantics["close≈日线raw"] += 1
                elif r_back < 0.005:
                    col_semantics["close≈日线raw×adj_lat/adj_i(后复权)"] += 1
                elif r_fwd < 0.005:
                    col_semantics["close≈日线raw×adj_i/adj_lat(前复权)"] += 1
                else:
                    col_semantics["close 三者皆不符"] += 1
            for x in good.itertuples():
                good_ut[str(x.update_time)[:10]] += 1
            rows_stat.append((code, len(j), len(bad), len(good)))

        P("=== 每 code 匹配日数 / 偏离日数 ===")
        P(f"{'code':10s} {'匹配':>6s} {'偏离':>6s} {'正常':>6s}")
        for code, n, nb, ng in rows_stat:
            P(f"{code:10s} {n:6d} {nb:6d} {ng:6d}")

        P("")
        P("=== 偏离行 update_time 日期分布（Top15）===")
        tot_bad = sum(bad_ut.values())
        for k, v in bad_ut.most_common(15):
            P(f"   {k}  {v:5d}  ({v/max(tot_bad,1):.1%})")
        P(f"   -- 偏离行合计 {tot_bad}")
        P("")
        P("=== 正常行 update_time 日期分布（Top15）===")
        tot_good = sum(good_ut.values())
        for k, v in good_ut.most_common(15):
            P(f"   {k}  {v:5d}  ({v/max(tot_good,1):.1%})")
        P(f"   -- 正常行合计 {tot_good}")

        P("")
        P("=== 偏离日分布（Top20 交易日）===")
        for k, v in bad_days.most_common(20):
            P(f"   {k}  {v}")

        P("")
        P("=== close 列语义判定（偏离行）===")
        for k, v in col_semantics.most_common():
            P(f"   {k:36s} {v}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
