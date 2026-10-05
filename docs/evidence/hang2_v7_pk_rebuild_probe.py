# -*- coding: utf-8 -*-
"""V7 离线实测：主键重建在生产库副本上的锁表窗口与空间回收。

DuckDB 1.4.5 **不支持** `ALTER TABLE DROP CONSTRAINT`（实测 NotImplementedException），
故 V7 只能走"新建带 PK 的表 + 全量 INSERT SELECT + DROP + RENAME"（等价 passthrough 换名）。
测量：重建耗时（= 独占锁窗口）、同配置第二连接的读阻塞时长、DB 体积变化、PK 是否生效。
用法：python docs/evidence/hang2_v7_pk_rebuild_probe.py
输出：docs/evidence/hang2_v7_pk_rebuild.txt
"""
from __future__ import annotations

import io
import os
import re
import shutil
import sys
import threading
import time

import duckdb

sys.path.insert(0, os.path.abspath('.'))
from quantstudio.pipeline.writers import DDL_DUCKDB, DuckDBWriter  # noqa: E402

SRC = os.path.join('data', 'quantstudio.db')
DST = os.path.join('data', 'bench', 'hang2_v7.db')
OUT = os.path.join('docs', 'evidence', 'hang2_v7_pk_rebuild.txt')
TABLE = 'stock_daily'
NEW = '_v7_stock_daily_new'


def main() -> None:
    L = []
    os.makedirs(os.path.dirname(DST), exist_ok=True)
    if not os.path.exists(DST):
        t0 = time.time()
        shutil.copyfile(SRC, DST)
        L.append('copy: %.2fGB in %.1fs' % (os.path.getsize(DST) / 1e9, time.time() - t0))
    c0 = duckdb.connect(DST, read_only=True)
    rows_before = c0.execute('SELECT COUNT(*) FROM %s' % TABLE).fetchone()[0]
    c0.close()
    size_before = os.path.getsize(DST)
    L.append('before: rows=%d db=%.2fGB' % (rows_before, size_before / 1e9))

    lat = []
    stop = threading.Event()
    timings = {}

    def _rebuild():
        try:
            cn = duckdb.connect(DST)
            # 以**实表 schema** 为准构建新表（静态 DDL_DUCKDB 已落后一列：缺 data_source，
            # 由 _migrate_add_columns 补入实表）——重建必须用实表，否则会丢列
            desc = cn.execute('DESCRIBE %s' % TABLE).fetchall()
            cols = ['"%s" %s' % (r[0], r[1]) for r in desc]
            pk = [r[0] for r in cn.execute(
                "SELECT constraint_text FROM duckdb_constraints() WHERE table_name=? "
                "AND constraint_type='PRIMARY KEY'", [TABLE]).fetchall()]
            pk_cols = []
            if pk:
                m = re.match(r'PRIMARY KEY\((.*)\)\s*$', pk[0], re.I)
                if m:
                    pk_cols = [c.strip().strip('"') for c in m.group(1).split(',') if c.strip()]
            collist = ', '.join(cols)
            if pk_cols:
                collist += ', PRIMARY KEY(%s)' % ', '.join('"%s"' % c for c in pk_cols)
            ddl = 'CREATE TABLE "%s" (%s)' % (NEW, collist)
            timings['ddl_cols'] = len(cols)
            timings['ddl_pk'] = len(pk_cols)
            t = time.time()
            cn.execute('DROP TABLE IF EXISTS "%s"' % NEW)
            cn.execute('BEGIN TRANSACTION')
            cn.execute(ddl)
            timings['create'] = time.time() - t
            t = time.time()
            cn.execute('INSERT INTO "%s" SELECT * FROM %s' % (NEW, TABLE))
            timings['copy'] = time.time() - t
            t = time.time()
            cn.execute('DROP TABLE %s' % TABLE)
            cn.execute('ALTER TABLE "%s" RENAME TO %s' % (NEW, TABLE))
            cn.execute('COMMIT')
            timings['swap'] = time.time() - t
            timings['total'] = sum(timings[k] for k in ('create', 'copy', 'swap'))
            cn.close()
        except BaseException as e:  # noqa: BLE001
            timings['error'] = '%s: %s' % (type(e).__name__, str(e)[:160])
        finally:
            stop.set()

    th = threading.Thread(target=_rebuild, daemon=True)
    th.start()
    time.sleep(0.5)
    rc = duckdb.connect(DST)
    while not stop.is_set():
        t0 = time.time()
        try:
            rc.execute('SELECT COUNT(*) FROM %s' % TABLE).fetchone()
            lat.append(time.time() - t0)
        except Exception:  # noqa: BLE001
            lat.append(-1.0)
        time.sleep(0.2)
    th.join(120)
    rc.close()

    c1 = duckdb.connect(DST, read_only=True)
    rows_after = c1.execute('SELECT COUNT(*) FROM %s' % TABLE).fetchone()[0]
    pk = c1.execute("SELECT constraint_text FROM duckdb_constraints() "
                    "WHERE table_name=? AND constraint_type='PRIMARY KEY'", [TABLE]).fetchall()
    c1.close()
    size_after = os.path.getsize(DST)
    ok = [x for x in lat if x >= 0]
    L.append('rebuild: %s' % {k: round(v, 1) for k, v in timings.items()
                            if isinstance(v, (int, float))})
    if 'error' in timings:
        L.append('rebuild_error: %s' % timings['error'])
    L.append('reader: samples=%d max=%.1fs blocked(>1s)=d count=%d total=%.1fs'
             % (len(lat), max(ok) if ok else -1, sum(1 for x in ok if x > 1.0),
                sum(x for x in ok if x > 1.0)))
    L.append('after: rows=%d pk=%s' % (rows_after, pk))
    L.append('db size: %.2fGB -> %.2fGB (%+.1fMB)'
             % (size_before / 1e9, size_after / 1e9, (size_after - size_before) / 1e6))
    total = timings.get('total', -1)
    if total < 0:
        L.append('VERDICT: REBUILD FAILED (%s)' % timings.get('error'))
    elif total < 60:
        L.append('VERDICT: window %.0fs < 60s => V7 feasible' % total)
    else:
        L.append('VERDICT: window %.0fs >= 60s => evaluate vs batch interval' % total)
    txt = '\n'.join(L)
    io.open(OUT, 'w', encoding='utf-8').write(txt + '\n')
    sys.stdout.reconfigure(encoding='utf-8')
    print(txt)


if __name__ == '__main__':
    main()
