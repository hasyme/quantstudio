"""步骤2（修订）：外部源核验 —— Raw Landing parquet = 云端原始返回。

修正要点：parquet 的 trade_time 是 **naive CST 本地时间**（非 UTC），
按 CST 直接比较即可（qfq_fresh_capture 注释亦确认此口径）。

全程只读。
"""
from __future__ import annotations
import glob
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
LAND = ROOT / "data" / "mcp_landing"
OUT = ROOT / "agent_workspace" / "_step2_cloud_out.txt"
CSV = ROOT / "agent_workspace" / "_step2_cloud_rows.csv"

LO = pd.Timestamp("2026-06-01")
HI = pd.Timestamp("2026-08-01")
TARGETS = {"603093.SZ", "000012.SZ", "000963.SZ", "600649.SH"}

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def main() -> int:
    files = sorted(glob.glob(str(LAND / "exp_stock_minutes_*" / "*.parquet")))
    P(f"stock_minutes landing parquet = {len(files)}")
    kept, read, pruned, nostat = [], 0, 0, 0
    for i, f in enumerate(files):
        try:
            pf = pq.ParquetFile(f)
            md = pf.metadata
        except Exception:
            continue
        names = md.schema.names
        if "trade_time" not in names:
            continue
        ci = names.index("trade_time")
        mn = mx = None
        ok = True
        for rg in range(md.num_row_groups):
            st = md.row_group(rg).column(ci).statistics
            if st is None or st.min is None or st.max is None:
                ok = False
                break
            a, b = st.min, st.max
            if not isinstance(a, pd.Timestamp):
                a = pd.Timestamp(a)
            if not isinstance(b, pd.Timestamp):
                b = pd.Timestamp(b)
            if a.tzinfo is not None:
                a = a.tz_convert("Asia/Shanghai").tz_localize(None)
            if b.tzinfo is not None:
                b = b.tz_convert("Asia/Shanghai").tz_localize(None)
            mn = a if mn is None or a < mn else mn
            mx = b if mx is None or b > mx else mx
        if ok and mn is not None and (mx < LO or mn > HI):
            pruned += 1
            continue
        if not ok:
            nostat += 1
        try:
            df = pq.read_table(f, columns=["ts_code", "trade_time", "close",
                                           "adj_factor", "is_qfq"]).to_pandas()
        except Exception:
            continue
        read += 1
        if "ts_code" not in df.columns:
            continue
        df = df[df["ts_code"].isin(TARGETS)]
        if len(df):
            df = df[(df["trade_time"] >= LO) & (df["trade_time"] < HI)]
        if len(df):
            kept.append(df)
        if i % 1000 == 0:
            P(f"  {i}/{len(files)} read={read} kept={len(kept)} pruned={pruned}")

    P(f"实读={read}  统计剪枝={pruned}  无统计={nostat}  命中={len(kept)}")
    if not kept:
        P("!! 未命中")
        OUT.write_text("\n".join(lines), encoding="utf-8")
        return 0
    a = pd.concat(kept, ignore_index=True)
    a["day"] = a["trade_time"].dt.strftime("%Y-%m-%d")
    a["hhmm"] = a["trade_time"].dt.strftime("%H:%M")
    P(f"云端行数 = {len(a):,}")
    P("")
    P("=== is_qfq 总分布 ===")
    P("   " + str(a["is_qfq"].value_counts(dropna=False).to_dict()))
    P("")
    P("=== 按 code ===")
    for c, g in a.groupby("ts_code"):
        P(f"   {c}: n={len(g):7d} is_qfq={g['is_qfq'].value_counts(dropna=False).to_dict()}")
    P("")
    P("=== 按 code × 月 ===")
    a["ym"] = a["day"].str.slice(0, 7)
    for (c, ym), g in a.groupby(["ts_code", "ym"]):
        P(f"   {c} {ym}: n={len(g):7d} is_qfq="
          f"{g['is_qfq'].value_counts(dropna=False).to_dict()} "
          f"adj_factors={sorted(set(round(float(x),4) for x in g['adj_factor'].dropna().unique()))[:5]}")
    P("")
    P("=== 15:00 bar 明细 ===")
    P(f"   {'code':11s} {'day':11s} {'hhmm':6s} {'cloud_close':>12s} "
      f"{'adj_factor':>11s} {'is_qfq':>7s}")
    sel = a[a["hhmm"].isin(["14:59", "15:00"])].sort_values(["ts_code", "day"])
    for x in sel.itertuples():
        P(f"   {x.ts_code:11s} {x.day:11s} {x.hhmm:6s} {x.close:12.4f} "
          f"{x.adj_factor:11.4f} {str(x.is_qfq):>7s}")
    a.to_csv(CSV, index=False, encoding="utf-8-sig")
    P("")
    P(f"明细已写 {CSV.name}")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
