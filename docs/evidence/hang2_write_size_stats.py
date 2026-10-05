# -*- coding: utf-8 -*-
"""统计采集日志中单次写入行数分布（为写路径看门狗预算定标）。

写路径超时预算必须显著高于健康单批耗时，又必须远低于事故现场（2h38m 无进展）。
本脚本提取所有 [DuckDBWriter] <table> batch=...: wrote N rows 行，
输出按表的最大/中位单批行数。

用法：python docs/evidence/hang2_write_size_stats.py
"""
from __future__ import annotations

import glob
import os
import re
import statistics
import sys
from collections import defaultdict

PAT = re.compile(
    r"[[]DuckDBWriter[]][ ]+(?P<table>[^ ]+)[ ]+batch=(?P<batch>[^ :]+):[ ]+"
    r"wrote[ ]+(?P<n>[0-9]+)[ ]+rows[ ]+[(]新增[ ]+(?P<new>[0-9]+)[ ]*[+][ ]*更新[ ]+"
    r"(?P<upd>[0-9]+)[)]"
)
OUT = os.path.join("docs", "evidence", "hang2_write_size_stats.txt")


def main() -> None:
    per_table = defaultdict(list)
    for path in glob.glob(os.path.join("data", "logs", "*.log")):
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                m = PAT.search(line)
                if m:
                    per_table[m.group("table")].append(
                        (int(m.group("n")), int(m.group("new")), int(m.group("upd"))))
    lines = []
    for table in sorted(per_table, key=lambda t: -max(r[0] for r in per_table[t])):
        rows = [r[0] for r in per_table[table]]
        lines.append("%-38s n=%4d  max=%8d  p50=%8d  mean=%9.0f  total=%9d"
                     % (table, len(rows), max(rows), int(statistics.median(rows)),
                        statistics.mean(rows), sum(rows)))
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print("written", OUT)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
