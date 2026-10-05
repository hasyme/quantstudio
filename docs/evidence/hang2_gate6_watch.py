# -*- coding: utf-8 -*-
'''门6 实时监控：判定 mcp_stock_daily 拉取是否**持续健康推进**。

事故判据（docs/evidence/duckdb-write-hang-incident2-20261002.md）：
"进程存活、单核满载、日志/DB 零增长" = 静默挂起。故本监控**四项同时看**：
  1. 进程存活（PID 仍在）
  2. 日志持续推进（字节 + mtime）
  3. DB 持续写入（字节 + mtime）
  4. CPU 与实际推进耦合（30s 窗口 CPU 增量；单核满载而产出不增 = 停摆特征）

并统计：每表已写批次数、累计行数、ERROR/CRITICAL 条数、最近一批分段耗时。

用法：python docs/evidence/hang2_gate6_watch.py <pid> [logfile]
'''
from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime

import psutil

DB = os.path.join('data', 'quantstudio.db')
DEFAULT_LOG = os.path.join('data', 'logs', '_gate6_stock_daily.err.txt')
WROTE = re.compile(r'\[DuckDBWriter\]\s+(\S+)\s+batch=(\S+?):\s+wrote\s+(\d+)\s+rows')
TIMING = re.compile(r'\[timing count=([\d.]+)s dml=([\d.]+)s close=([\d.]+)s\]')


def _mb(n):
    return '%.1fMB' % (n / 1e6)


def main() -> None:
    pid = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    log = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_LOG
    now = datetime.now().strftime('%H:%M:%S')

    alive = False
    cpu_before = cpu_after = None
    if pid:
        try:
            p = psutil.Process(pid)
            alive = p.is_running() and p.status() != psutil.STATUS_ZOMBIE
            cpu_before = sum(p.cpu_times()[:2])
        except Exception:
            alive = False

    log_size = os.path.getsize(log) if os.path.exists(log) else -1
    db_size = os.path.getsize(DB) if os.path.exists(DB) else -1
    log_mtime = os.path.getmtime(log) if os.path.exists(log) else 0
    db_mtime = os.path.getmtime(DB) if os.path.exists(DB) else 0
    db_size2 = db_size

    # 30s 窗口：看日志/DB 是否增长 + CPU 增量
    time.sleep(30.0)
    log_size2 = os.path.getsize(log) if os.path.exists(log) else -1
    db_size2 = os.path.getsize(DB) if os.path.exists(DB) else -1
    if pid and alive:
        try:
            cpu_after = sum(psutil.Process(pid).cpu_times()[:2])
        except Exception:
            pass

    text = open(log, encoding='utf-8', errors='replace').read()
    batches = {}
    for m in WROTE.finditer(text):
        tbl, _bid, n = m.group(1), m.group(2), int(m.group(3))
        c, s = batches.get(tbl, (0, 0))
        batches[tbl] = (c + 1, s + n)
    errs = len(re.findall(r'\b(ERROR|CRITICAL)\b', text))
    stalls = len(re.findall(r'DuckDBWriteStalled', text))
    timings = [(float(a), float(b), float(c)) for a, b, c in TIMING.findall(text)]

    print('[%s] 门6 巡检' % now)
    print('  进程 pid=%s 存活=%s  CPU增量(30s窗口)=%s'
          % (pid, alive, ('%.1fs' % (cpu_after - cpu_before))
             if (cpu_after is not None and cpu_before is not None) else 'n/a'))
    print('  日志  %s -> %s (Δ=%+d) 静止=%s'
          % (_mb(log_size), _mb(log_size2), log_size2 - log_size,
             '%ds' % int(time.time() - log_mtime)))
    print('  DB    %s -> %s (Δ=%+d) 静止=%s'
          % (_mb(db_size), _mb(db_size2), db_size2 - db_size,
             '%ds' % int(time.time() - db_mtime)))
    for tbl, (c, s) in sorted(batches.items(), key=lambda kv: -kv[1][1]):
        print('  已写  %-24s 批次=%-4d 累计行=%d' % (tbl, c, s))
    if timings:
        dmls = [t[1] for t in timings]
        last = timings[-1]
        print('  分段耗时 末批 count=%.2fs dml=%.2fs close=%.2fs | dml max=%.2fs p50=%.2fs'
              % (last[0], last[1], last[2], max(dmls), sorted(dmls)[len(dmls) // 2]))
    print('  ERROR/CRITICAL=%d  DuckDBWriteStalled=%d' % (errs, stalls))
    healthy = (alive and (log_size2 - log_size) > 0)
    print('  判定: %s' % ('推进中（日志有增长）' if healthy else '⚠ 无推进——核对 CPU/DB/日志'))


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
