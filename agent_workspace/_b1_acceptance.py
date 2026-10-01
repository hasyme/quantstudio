"""B1 验收：两张日线表重拉后 vs 云端直取一致率。

判据（方案 §4.B1）：`stock_daily` / `etf_daily` vs 云端 → 一致率 ≥99.5%
（当前 stock_daily 有 6.4% 行偏高，重拉后应归零）。

方法：云端直取 N 个交易日 → 与本地逐 (code, day) 比对 close。
"""
from __future__ import annotations
import io
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quantstudio.pipeline.mcp.client import MCPClient

CST = timezone(timedelta(hours=8))
# 抽样日（覆盖各年份，含放大最严重的早期）
SAMPLE_DAYS = [
    "2018-06-15", "2019-06-14", "2020-06-15", "2021-06-15",
    "2022-06-15", "2023-06-15", "2024-06-14", "2025-06-16",
    "2026-01-12", "2026-06-15",
]
OUT = ROOT / "agent_workspace" / "_b1_acceptance.txt"


def _day_key(ts_series) -> pd.Series:
    return (pd.to_datetime(ts_series, unit="ms", utc=True)
            .dt.tz_convert("Asia/Shanghai").dt.strftime("%Y-%m-%d"))


def check_table(client: MCPClient, con, table: str, cloud_ds: str,
                sample_days: list) -> dict:
    """云端直取 vs 本地比对，返回统计。"""
    rows = []
    for d in sample_days:
        nxt = (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            arts = client.export_dataset(cloud_ds, page_size=5_000_000,
                                         time_start=d, time_end=nxt, async_mode=False)
        except Exception as e:
            rows.append({"day": d, "err": f"{type(e).__name__}: {str(e)[:60]}"})
            continue
        if not arts:
            rows.append({"day": d, "n": 0})
            continue
        cloud = pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes))
                           for a in arts], ignore_index=True)
        cloud["bare"] = cloud["ts_code"].astype(str).str.split(".").str[0]
        cloud = cloud[["bare", "close"]].rename(columns={"close": "cloud_close"})
        # 本地同日
        loc = con.execute(
            f"SELECT code, close FROM {table} "
            "WHERE (time+28800000)//86400000 = ?",
            [int((datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=CST).timestamp() * 1000
                  + 28800000) // 86400000)]).fetchdf()
        if loc.empty:
            rows.append({"day": d, "n": 0, "note": "local empty"})
            continue
        loc.columns = ["bare", "local_close"]
        m = cloud.merge(loc, on="bare", how="inner")
        if m.empty:
            rows.append({"day": d, "n": 0})
            continue
        m["ratio"] = m["local_close"] / pd.to_numeric(m["cloud_close"], errors="coerce")
        rows.append({
            "day": d, "n": len(m),
            "exact": int((m["ratio"] - 1).abs().lt(1e-9).sum()),
            "ok005": int((m["ratio"] - 1).abs().lt(0.005).sum()),
            "hi": int((m["ratio"] > 1.05).sum()),
            "lo": int((m["ratio"] < 0.95).sum()),
        })
    tot_n = sum(r.get("n", 0) for r in rows)
    tot_ok = sum(r.get("ok005", 0) for r in rows)
    tot_hi = sum(r.get("hi", 0) for r in rows)
    tot_lo = sum(r.get("lo", 0) for r in rows)
    return {"rows": rows, "n": tot_n, "ok005": tot_ok, "hi": tot_hi, "lo": tot_lo,
            "rate": (100.0 * tot_ok / tot_n) if tot_n else 0.0}


def main() -> int:
    out = []
    client = MCPClient(endpoint="https://124.223.159.234/mcp", tls_verify=False)
    client.handshake()
    con = duckdb.connect(str(ROOT / "data" / "quantstudio.db"), read_only=True)
    out.append("=== B1 验收：日线表 vs 云端直取 ===")
    out.append(f"抽样日: {len(SAMPLE_DAYS)} 天（跨 2018-2026）")
    out.append("")
    verdict = {}
    for table, cloud_ds in (("stock_daily", "stock_daily"),
                            ("etf_daily", "etf_daily")):
        out.append(f"--- {table} ---")
        try:
            r = check_table(client, con, table, cloud_ds, SAMPLE_DAYS)
        except Exception as e:
            out.append(f"  ERR {type(e).__name__}: {str(e)[:100]}")
            verdict[table] = False
            continue
        out.append(f"  {'day':<12} {'n':>6} {'exact':>7} {'ok0.5%':>8} {'hi>1.05':>8} {'lo<0.95':>8}")
        for row in r["rows"]:
            if "err" in row:
                out.append(f"  {row['day']:<12} ERR {row['err']}")
            elif row.get("n", 0) == 0:
                out.append(f"  {row['day']:<12} {'0':>6}  {row.get('note','')}")
            else:
                out.append(f"  {row['day']:<12} {row['n']:>6} {row['exact']:>7} "
                           f"{row['ok005']:>8} {row['hi']:>8} {row['lo']:>8}")
        out.append(f"  合计: n={r['n']}  一致率(≤0.5%)={r['rate']:.2f}%  "
                   f"偏高>1.05={r['hi']}  偏低<0.95={r['lo']}")
        ok = r["rate"] >= 99.5
        out.append(f"  B1 判据(≥99.5%): {'PASS' if ok else 'FAIL'}")
        verdict[table] = ok
        out.append("")
    con.close()
    out.append("=== B1 总判定 ===")
    for t, v in verdict.items():
        out.append(f"  {t}: {'PASS' if v else 'FAIL'}")
    out.append(f"  OVERALL: {'PASS' if all(verdict.values()) else 'FAIL'}")
    OUT.write_text("\n".join(out), encoding="utf-8")
    print("\n".join(out))
    return 0 if all(verdict.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
