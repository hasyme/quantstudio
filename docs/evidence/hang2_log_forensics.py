# -*- coding: utf-8 -*-
"""事故2（2026-10-02 DuckDB 写入挂起）日志取证脚本（只读）。

用途：对 data/logs/once_mcp_stock_daily_*.log 逐行提取
  - 每批 wrote 行（新增 X + 更新 Y）
  - 累计提交行数、批序号
  - 每批时间戳间隔（识别耗时突增）
输出：每个 run 的批次数、new/updated 分布、累计行数、末 5 行原文、TOP-5 最慢批。

用法：python docs/evidence/hang2_log_forensics.py
"""
from __future__ import annotations

import glob
import os
import re
from datetime import datetime, timedelta

LOG_DIR = os.path.join("data", "logs")
# 日志实测格式（2026-10-02）：
#   16:20:04 INFO quantstudio.pipeline.writers: [DuckDBWriter] stock_daily batch=<id>:
#       wrote 50000 rows (新增 0 + 更新 50000) 防重复 upsert
# 时间戳仅 HH:MM:SS（无日期），跨零点需按出现顺序推断。
PAT_WRITE = re.compile(
    r"^(?P<ts>\d{2}:\d{2}:\d{2})\s+\w+\s+quantstudio\.pipeline\.writers:\s*"
    r"\[DuckDBWriter\]\s+stock_daily\s+batch=(?P<batch>\S+?):\s+"
    r"wrote\s+(?P<n>\d+)\s+rows\s+\(新增\s+(?P<new>\d+)\s*\+\s*更新\s+(?P<upd>\d+)\)"
)
PAT_ANY_ERR = re.compile(r"ERROR|CRITICAL|Traceback|Exception")


def _parse_ts(prev: datetime | None, s: str):
    """HH:MM:SS -> datetime；跨零点自动进位（按出现顺序单调不减）。"""
    t = datetime.strptime(s, "%H:%M:%S")
    if prev is not None:
        base = prev.replace(hour=t.hour, minute=t.minute, second=t.second)
        if base < prev:
            base += timedelta(days=1)
        t = base
    return t


def analyze(path: str) -> dict:
    rows = []
    errs = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = PAT_WRITE.search(line)
            if m:
                rows.append(
                    {
                        "ts": _parse_ts(rows[-1]["ts"] if rows else None, m.group("ts")),
                        "batch": m.group("batch"),
                        "n": int(m.group("n")),
                        "new": int(m.group("new")),
                        "upd": int(m.group("upd")),
                    }
                )
            elif PAT_ANY_ERR.search(line):
                errs.append(line.strip()[:200])
    # 批间耗时（按出现顺序，包含同一 run 内重复批次）
    gaps = []
    for prev, cur in zip(rows, rows[1:]):
        if prev["ts"] and cur["ts"]:
            gaps.append(((cur["ts"] - prev["ts"]).total_seconds(), prev["batch"], cur["batch"]))
    gaps.sort(reverse=True)
    return {
        "path": path,
        "count": len(rows),
        "cum_rows": sum(r["n"] for r in rows),
        "cum_new": sum(r["new"] for r in rows),
        "cum_upd": sum(r["upd"] for r in rows),
        "pure_new": sum(1 for r in rows if r["upd"] == 0),
        "pure_upd": sum(1 for r in rows if r["new"] == 0),
        "mixed": sum(1 for r in rows if r["new"] and r["upd"]),
        "first_ts": rows[0]["ts"] if rows else None,
        "last_ts": rows[-1]["ts"] if rows else None,
        "top_gaps": gaps[:5],
        "last_rows": rows[-3:],
        "first_rows": rows[:2],
        "errs": errs[-5:],
    }


def main() -> None:
    for path in sorted(glob.glob(os.path.join(LOG_DIR, "once_mcp_stock_daily*.log"))):
        a = analyze(path)
        print("=" * 100)
        print(f"RUN {os.path.basename(a['path'])}")
        print(f"  批次数={a['count']}  累计提交行={a['cum_rows']}")
        print(f"  纯新增批={a['pure_new']}  纯更新批={a['pure_upd']}  混合批={a['mixed']}")
        print(f"  首={a['first_ts']}  末={a['last_ts']}")
        if a["first_ts"] and a["last_ts"]:
            span = (a["last_ts"] - a["first_ts"]).total_seconds()
            if a["count"] > 1 and span > 0:
                print(f"  跨度={span:.0f}s  平均每批={span / (a['count'] - 1):.1f}s")
        print("  首 2 批:")
        for r in a["first_rows"]:
            print(f"    {r['ts']} n={r['n']} new={r['new']} upd={r['upd']} batch={r['batch']}")
        print("  末 3 批:")
        for r in a["last_rows"]:
            print(f"    {r['ts']} n={r['n']} new={r['new']} upd={r['upd']} batch={r['batch']}")
        print("  TOP5 批间间隔(秒):")
        for g, b1, b2 in a["top_gaps"]:
            print(f"    {g:8.1f}s  {b1} -> {b2}")
        if a["errs"]:
            print("  ERROR/CRITICAL 末 5 条:")
            for e in a["errs"]:
                print(f"    {e}")


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    main()
