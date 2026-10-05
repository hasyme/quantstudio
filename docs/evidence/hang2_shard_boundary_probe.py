# -*- coding: utf-8 -*-
"""判定事故2 挂起语句归属：生产库 stock_daily 已落库数据的日期边界取证（只读）。

问题：事故2（2026-10-02 14:50 运行，P1 写入分档已部署）挂在第 57 批。py-spy 报
`writers.py:919`（f-string 行号归因不可信）。若第 57 批主键**全为新键**，则 P1 分档应走
"纯 INSERT"（writers.py:915）分支 —— 即 **P1 生效了仍然挂起**；若第 57 批主键已存在，
则走 ON CONFLICT（903）分支 —— P1 未生效。

判定方法：读生产库（只读连接）取 stock_daily 的已落库日期集合与逐日行数，
与"每 shard ≈ 10 个交易日 × 全市场码"的导出形态对齐，定位第 56/57 shard 的日期边界，
从而判断第 57 批是否落在已落库日期区间内。

用法：python docs/evidence/hang2_shard_boundary_probe.py
输出：docs/evidence/hang2_shard_boundary_probe.txt
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import duckdb

DB = os.path.join("data", "quantstudio.db")
OUT = os.path.join("docs", "evidence", "hang2_shard_boundary_probe.txt")
TABLE = "stock_daily"
PER_SHARD_DAYS = 10


def business_days(start: datetime, n: int):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def main() -> None:
    lines = []
    conn = duckdb.connect(DB, read_only=True)
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        mn, mx = conn.execute(f"SELECT MIN(time), MAX(time) FROM {TABLE}").fetchone()
        codes = conn.execute(f"SELECT COUNT(DISTINCT code) FROM {TABLE}").fetchone()[0]
        lines.append(f"table={TABLE} rows={total} distinct_codes={codes}")
        lines.append(f"min_time={mn} ({datetime.fromtimestamp(mn / 1000)})")
        lines.append(f"max_time={mx} ({datetime.fromtimestamp(mx / 1000)})")
        rows = conn.execute(
            f"SELECT time, COUNT(*) c FROM {TABLE} GROUP BY time ORDER BY time"
        ).fetchall()
        days = [(datetime.fromtimestamp(t / 1000).date(), c) for t, c in rows]
        lines.append(f"distinct_days={len(days)}")
        lines.append("--- 尾部 25 个交易日行数 ---")
        for d, c in days[-25:]:
            lines.append(f"  {d} rows={c}")
        full = len(days) // PER_SHARD_DAYS
        lines.append(f"--- 按 {PER_SHARD_DAYS} 交易日/shard 假设：已落库 {len(days)} 交易日 "
                     f"≈ {len(days) / PER_SHARD_DAYS:.2f} shard ---")
        lines.append(f"完整 shard 数={full}，余 {len(days) % PER_SHARD_DAYS} 交易日")
        if full:
            boundary = days[full * PER_SHARD_DAYS - 1][0]
            nxt = business_days(datetime.combine(boundary, datetime.min.time()), 11)
            lines.append(f"第 {full} 个完整 shard 覆盖至 {boundary}；"
                         f"其后再 10 个交易日（=第 {full + 1} shard）：")
            for d in nxt[1:]:
                lines.append(f"    {d.date()}")
            lines.append(f"第 {full + 1} shard 末日 = {nxt[-1].date()}")
    finally:
        conn.close()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print("written", OUT)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
