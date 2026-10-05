# -*- coding: utf-8 -*-
"""A0 在线实验前置门：生产库基线快照（只读）+ 四项在线判定器阈值确认。

用法：python docs/evidence/hang2_a0_baseline.py
输出：docs/evidence/hang2_a0_baseline.txt
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

import duckdb

DB = os.path.join('data', 'quantstudio.db')
OUT = os.path.join('docs', 'evidence', 'hang2_a0_baseline.txt')
TABLE = 'stock_daily'


def main() -> None:
    L = []
    L.append('A0 基线快照 @ %s' % datetime.now().isoformat(timespec='seconds'))
    L.append('db=%s bytes=%d' % (DB, os.path.getsize(DB)))
    wal = DB + '.wal'
    L.append('wal=%s bytes=%d' % (wal, os.path.getsize(wal) if os.path.exists(wal) else 0))
    c = duckdb.connect(DB, read_only=True)
    try:
        n = c.execute('SELECT COUNT(*) FROM %s' % TABLE).fetchone()[0]
        mn, mx = c.execute('SELECT MIN(time), MAX(time) FROM %s' % TABLE).fetchone()
        codes = c.execute('SELECT COUNT(DISTINCT code) FROM %s' % TABLE).fetchone()[0]
        days = c.execute('SELECT COUNT(DISTINCT time) FROM %s' % TABLE).fetchone()[0]
        s_close = c.execute('SELECT SUM(close) FROM %s' % TABLE).fetchone()[0]
        pk = c.execute('SELECT COUNT(*) FROM (SELECT code, time FROM %s '
                       'GROUP BY code, time HAVING COUNT(*) > 1)' % TABLE).fetchone()[0]
        L.append('rows=%d distinct_codes=%d distinct_days=%d' % (n, codes, days))
        L.append('min_time=%d (%s) max_time=%d (%s)'
                 % (mn, datetime.fromtimestamp(mn / 1000).date(),
                    mx, datetime.fromtimestamp(mx / 1000).date()))
        L.append('CHECKSUM sum_close=%.6f pk_duplicates=%d' % (s_close or 0.0, pk))
        wm = c.execute("SELECT source, table_name, freq, last_date FROM source_watermark "
                       "WHERE table_name=?", [TABLE]).fetchall()
        L.append('watermark=%s' % (wm if wm else 'None（⇒ 增量按全量回填）'))
        val = c.execute('SELECT COUNT(*) FROM stock_daily_valuation').fetchone()[0]
        L.append('stock_daily_valuation rows=%d' % val)
        L.append('gate6_baseline_match=%s' % ('YES' if (n == 2749437) else 'NO'))
    finally:
        c.close()
    L.append('--- 四项在线判定器阈值（已由门6 实测标定）---')
    L.append('1) 进程存活; 2) 日志字节 30s 窗口 > 0; 3) DB 字节 30s 窗口 > 0;')
    L.append('4) CPU 30s 窗口 < 20s（单核满载且产出不增 = 停摆）; 5) 总时上界 1800s')
    txt = '\n'.join(L)
    open(OUT, 'w', encoding='utf-8').write(txt + '\n')
    sys.stdout.reconfigure(encoding='utf-8')
    print(txt)


if __name__ == '__main__':
    main()
