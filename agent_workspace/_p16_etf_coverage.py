"""P1-6（只读）：etf_minutes 全量取证，划定受污染边界。

复刻 step3（stock）的方法，表换成 etf：
  候选 = etf_dividend.ex_date >= 2026-03-02（P1-2 规则）
  因子 = fund_adj；分钟 = etf_minutes 15:00 bar；日线基准 = etf_daily

判定口径与 stock 完全一致（per-(code,day) 的 close_front/close vs adj_i/adj_latest）。
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
OUT = ROOT / "agent_workspace" / "_p16_etf_out.txt"
OUT_CSV = ROOT / "agent_workspace" / "_p16_etf_affected.csv"

CST = timezone(timedelta(hours=8))
BAR_LO = 6 * 3600_000 + 59 * 60_000
BAR_HI = 7 * 3600_000 + 1 * 60_000
LO_EX = int(datetime(2026, 3, 2).timestamp() * 1000)  # P1-2 候选窗口下限
T1 = int(datetime(2026, 3, 1).timestamp() * 1000)
T2 = int(datetime(2026, 9, 27).timestamp() * 1000)
TOL = 0.005

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
        codes = [str(r[0]) for r in con.execute(
            "SELECT DISTINCT code FROM etf_dividend WHERE ex_date >= ?", [LO_EX]).fetchall()]
        P(f"etf 除权候选（ex_date >= 2026-03-02）= {len(codes)}")

        ph = ",".join("?" * len(codes))
        frows = aux.execute(
            f"SELECT code, time, adj_factor FROM fund_adj WHERE code IN ({ph})",
            codes).fetchall()
        fdf = pd.DataFrame(frows, columns=["code", "time", "adj_factor"])
        fdf["time"] = pd.to_numeric(fdf["time"], errors="coerce")
        fdf["adj_factor"] = pd.to_numeric(fdf["adj_factor"], errors="coerce")
        fdf = fdf.dropna()
        P(f"因子行 {len(fdf):,}")

        segs, latest = [], {}
        for c, g in fdf.sort_values("time").groupby("code"):
            cl = clean_segments(list(zip(g["time"].astype("int64"), g["adj_factor"])))
            if not cl:
                continue
            latest[c] = cl[-1][1]
            segs.extend((c, st, v) for st, v in cl)
        segdf = pd.DataFrame(segs, columns=["code", "time", "adj_factor"])
        segdf["time"] = segdf["time"].astype("int64")
        P(f"段 {len(segdf):,} / code {len(latest):,}")

        mb = con.execute(
            "SELECT code, time, close, close_front, update_time FROM etf_minutes "
            "WHERE time>=? AND time<? AND close>0 AND close_front IS NOT NULL "
            "AND (time % 86400000) BETWEEN ? AND ?", [T1, T2, BAR_LO, BAR_HI]).fetchall()
        P(f"etf_minutes 15:00 bar = {len(mb):,}")
        db = con.execute(
            "SELECT code, time, close, close_front FROM etf_daily "
            "WHERE time>=? AND time<?", [T1 - 86_400_000, T2]).fetchall()
        P(f"etf_daily = {len(db):,}")

        mdf = pd.DataFrame(mb, columns=["code", "time", "mclose", "mfront", "ut"])
        ddf = pd.DataFrame(db, columns=["code", "dtime", "dclose", "dfront"])
        mdf["day"] = mdf["time"].map(cday)
        ddf["day"] = ddf["dtime"].map(cday)
        mg = (mdf.sort_values("time").groupby(["code", "day"]).last()
              .reset_index()[["code", "day", "mclose", "mfront", "ut", "time"]])
        dg = ddf.groupby(["code", "day"]).last().reset_index()[
            ["code", "day", "dclose", "dfront"]]
        j = mg.merge(dg, on=["code", "day"], how="inner")
        j = pd.merge_asof(j.sort_values("time"), segdf.sort_values("time"),
                          on="time", by="code", direction="backward")
        j = j.dropna(subset=["adj_factor"])
        j["adj_latest"] = j["code"].map(latest)
        j = j.dropna(subset=["adj_latest"])
        j["expect"] = j["adj_factor"] / j["adj_latest"]
        j["dev"] = ((j["mfront"] / j["mclose"]) / j["expect"] - 1).abs()
        P(f"可比 (code,day) = {len(j):,}")

        bad = j[j["dev"] > 0.005].copy()

        def close_to(a, b):
            if b is None or b == 0 or pd.isna(b) or pd.isna(a):
                return False
            return abs(a / b - 1) < TOL

        forms = []
        for x in bad.itertuples():
            a = close_to(x.mclose, x.dclose)
            b = close_to(x.mfront, x.dfront)
            cc = close_to(x.mfront, x.dclose)
            d = close_to(x.mclose, x.dfront)
            if b and not a:
                v = "front正确/raw失真"
            elif a and not b:
                v = "raw正确/front失真"
            elif cc and d:
                v = "两列互换"
            elif a and b:
                v = "两列皆≈日线"
            elif cc and not d:
                v = "front存了raw"
            elif d and not cc:
                v = "raw存了front"
            else:
                v = "四者皆不符"
            forms.append(v)
        bad["form"] = forms
        bad["update_date"] = bad["ut"].astype(str).str.slice(0, 10)

        P("")
        P("=" * 70)
        P("P1-6 etf_minutes 取证结果")
        P("=" * 70)
        P(f"候选 code              = {len(codes)}")
        P(f"可比 (code,day)        = {len(j):,}")
        P(f"偏离 (code,day)        = {len(bad):,}")
        P(f"偏离 code 数           = {bad['code'].nunique()}")
        P("")
        P("=== 失真形态分布 ===")
        for k, v in Counter(forms).most_common():
            P(f"   {k:22s} {v:6d}  ({v/max(len(bad),1):.1%})")
        P("")
        P(f"=== 日期边界 ===")
        if len(bad):
            P(f"   最早 {bad['day'].min()}  最晚 {bad['day'].max()}  "
              f"跨 {bad['day'].nunique()} 交易日")
        P("")
        P("=== 按月分布 ===")
        bad["ym"] = bad["day"].str.slice(0, 7)
        for k, v in bad["ym"].value_counts().sort_index().items():
            P(f"   {k}  {v:6d}")
        P("")
        P("=== update_time 分布 ===")
        for k, v in bad["update_date"].value_counts().head(12).items():
            P(f"   {k}  {v:6d}  ({v/len(bad):.1%})")
        P("")
        P("=== 偏离 code 按 dev 量级 ===")
        fc = bad.groupby("code")["dev"].max()
        for thr, lab in ((0.005, ">0.5%"), (0.05, ">5%"), (0.2, ">20%"),
                         (0.5, ">50%"), (1.0, ">100%")):
            P(f"   {lab:8s} {(fc > thr).sum():5d}")
        P("")
        P("=== Top20 最严重 ===")
        P(f"   {'code':9s} {'dev_max':>9s} {'形态(主导)':>20s} {'偏离日数':>7s}")
        for c, dv in fc.sort_values(ascending=False).head(20).items():
            sub = bad[bad["code"] == c]
            P(f"   {c:9s} {dv:9.4f} {Counter(sub['form']).most_common(1)[0][0]:>20s} "
              f"{len(sub):7d}")
        bad[["code", "day", "mclose", "mfront", "dclose", "dfront",
             "expect", "dev", "form", "update_date"]].to_csv(OUT_CSV,
             index=False, encoding="utf-8-sig")
        P("")
        P(f"受影响明细已写 {OUT_CSV.name}")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
