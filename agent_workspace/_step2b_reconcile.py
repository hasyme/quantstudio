"""步骤2b：云端真值 vs 主库落库值 的定量对账（决定性）。

对每个 (code, day, 15:00)：
  cloud_close  ← Raw Landing parquet（云端原始返回，还原前）
  stored_close ← 主库 stock_minutes（还原后）
  d_raw        ← stock_daily.close（不复权真值）
  adj_i        ← 云端行内 adj_factor；adj_latest ← qfq_aux.db 全局最新
判定：
  H1: cloud_close == d_raw      （云端返回的是 raw）
  H2: stored_close == cloud_close × adj_latest/adj_i  （还原公式被误用）
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
CLOUD = ROOT / "agent_workspace" / "_step2_cloud_rows.csv"
OUT = ROOT / "agent_workspace" / "_step2b_reconcile_out.txt"

CST = timezone(timedelta(hours=8))
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
    cloud = pd.read_csv(CLOUD)
    cloud = cloud[cloud["hhmm"] == "15:00"][["ts_code", "day", "close",
                                             "adj_factor", "is_qfq"]].copy()
    cloud["code"] = cloud["ts_code"].str.split(".").str[0]
    P(f"云端 15:00 行 = {len(cloud)}  codes={sorted(cloud['code'].unique())}")

    con = duckdb.connect(str(MAIN), read_only=True)
    aux = sqlite3.connect(f"file:{AUX}?mode=ro", uri=True, timeout=30)
    aux.execute("PRAGMA query_only=ON")
    try:
        codes = sorted(cloud["code"].unique())
        latest = {}
        segdf_all = []
        for c in codes:
            fr = aux.execute(
                "SELECT time, adj_factor FROM adj_factor WHERE code=? ORDER BY time",
                [c]).fetchall()
            if not fr:
                continue
            cl = clean_segments(fr)
            latest[c] = cl[-1][1]
            segdf_all.extend((c, st, v) for st, v in cl)
        segdf = pd.DataFrame(segdf_all, columns=["code", "time", "adj_factor"])
        segdf["time"] = segdf["time"].astype("int64")
        P(f"adj_latest: {latest}")

        ph = ",".join("?" * len(codes))
        mb = con.execute(
            f"SELECT code, time, close, close_front FROM stock_minutes "
            f"WHERE code IN ({ph}) AND close>0 "
            f"AND (time % 86400000) BETWEEN ? AND ?",
            codes + [6 * 3600_000 + 59 * 60_000, 7 * 3600_000 + 1 * 60_000]).fetchall()
        db = con.execute(
            f"SELECT code, time, close, close_front FROM stock_daily "
            f"WHERE code IN ({ph})", codes).fetchall()
        mdf = pd.DataFrame(mb, columns=["code", "time", "mclose", "mfront"])
        ddf = pd.DataFrame(db, columns=["code", "dtime", "dclose", "dfront"])
        mdf["day"] = mdf["time"].map(cday)
        ddf["day"] = ddf["dtime"].map(cday)
        mg = mdf.groupby(["code", "day"], as_index=False).last()
        dg = ddf.groupby(["code", "day"], as_index=False).last()
        j = cloud.merge(mg[["code", "day", "mclose", "mfront"]],
                        on=["code", "day"], how="left")
        j = j.merge(dg[["code", "day", "dclose", "dfront"]],
                    on=["code", "day"], how="left")
        j["adj_latest"] = j["code"].map(latest)
        j = j.dropna(subset=["mclose", "dclose", "adj_latest"])
        j["ratio"] = j["adj_latest"] / j["adj_factor"]
        j["h1_cloud_eq_dailyraw"] = (j["close"] - j["dclose"]).abs() < 0.005
        j["h2_stored_eq_cloud_x_ratio"] = (
            (j["mclose"] - j["close"] * j["ratio"]).abs()
            / (j["close"] * j["ratio"]) < 0.002)
        j["h3_stored_eq_cloud"] = (j["mclose"] - j["close"]).abs() < 0.005
        j["drift"] = ((j["mfront"] / j["mclose"])
                      / (j["adj_factor"] / j["adj_latest"]) - 1).abs()

        P("")
        P("=" * 78)
        P("步骤2b 对账结果")
        P("=" * 78)
        P(f"可比 (code,day) = {len(j)}")
        P("")
        P(f"H1 云端 close == 日线 raw(不复权)      : "
          f"{int(j['h1_cloud_eq_dailyraw'].sum())}/{len(j)} "
          f"({j['h1_cloud_eq_dailyraw'].mean():.1%})")
        P(f"H2 落库 close == 云端 close × ratio    : "
          f"{int(j['h2_stored_eq_cloud_x_ratio'].sum())}/{len(j)} "
          f"({j['h2_stored_eq_cloud_x_ratio'].mean():.1%})")
        P(f"H3 落库 close == 云端 close（未变换）  : "
          f"{int(j['h3_stored_eq_cloud'].sum())}/{len(j)} "
          f"({j['h3_stored_eq_cloud'].mean():.1%})")
        P("")
        P("按偏离/正常分组：")
        for lab, sub in (("偏离(dev>0.5%)", j[j["drift"] > 0.005]),
                         ("正常", j[j["drift"] <= 0.005])):
            if not len(sub):
                continue
            P(f"  {lab:16s} n={len(sub):6d}  "
              f"H1(云端=raw)={sub['h1_cloud_eq_dailyraw'].mean():.1%}  "
              f"H2(落库=云端×ratio)={sub['h2_stored_eq_cloud_x_ratio'].mean():.1%}  "
              f"H3(落库=云端)={sub['h3_stored_eq_cloud'].mean():.1%}")
        P("")
        P("按 code 分组：")
        for c, sub in j.groupby("code"):
            P(f"  {c}: n={len(sub):5d} 偏离={int((sub['drift']>0.005).sum()):5d}  "
              f"H1={sub['h1_cloud_eq_dailyraw'].mean():.1%}  "
              f"H2={sub['h2_stored_eq_cloud_x_ratio'].mean():.1%}")
        P("")
        P("=== 明细（偏离行，前 30）===")
        P(f"  {'code':8s} {'day':11s} {'云端close':>10s} {'日线raw':>9s} "
          f"{'落库close':>10s} {'ratio':>9s} {'云端×ratio':>11s} {'drift':>8s}")
        bad = j[j["drift"] > 0.005].sort_values(["code", "day"])
        for x in bad.head(30).itertuples():
            P(f"  {x.code:8s} {x.day:11s} {x.close:10.4f} {x.dclose:9.4f} "
              f"{x.mclose:10.4f} {x.ratio:9.6f} {x.close*x.ratio:11.4f} "
              f"{x.drift:8.4f}")
        P("")
        P("=== 正常行样本（前 10，用于对照）===")
        ok = j[j["drift"] <= 0.005].sort_values(["code", "day"]).head(10)
        for x in ok.itertuples():
            P(f"  {x.code:8s} {x.day:11s} {x.close:10.4f} {x.dclose:9.4f} "
              f"{x.mclose:10.4f} {x.ratio:9.6f} {x.close*x.ratio:11.4f} "
              f"{x.drift:8.4f}")
        j.to_csv(ROOT / "agent_workspace" / "_step2b_reconcile.csv",
                 index=False, encoding="utf-8-sig")
    finally:
        con.close()
        aux.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
