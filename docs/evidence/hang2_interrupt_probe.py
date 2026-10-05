# -*- coding: utf-8 -*-
'''探针：验证 DuckDB 1.4.5 下 conn.interrupt() 能否打断**写事务**（写路径看门狗可行性）。

背景：回测读路径已有同款机制（quantstudio/backtest/providers/duckdb_data_access.py:233-288，
在 duckdb 1.5.5 实测 interrupt 生效、连接可复用）。但**写路径**（INSERT/ON CONFLICT 长事务）
从未验证过——而写路径才是本项目挂起事故的现场（writers._write_locked 无限期占用写锁）。

方法：worker 线程执行一条足量大的 INSERT（预计数秒~数十秒），主线程在预算到点调用
conn.interrupt()，观测：
  1. 是否抛出 duckdb.InterruptException（而非永久阻塞）；
  2. 打断生效延迟；
  3. 打断后连接是否仍可复用（SELECT 1 正常）；
  4. 事务是否回滚（表行数未推进）。

用法：python docs/evidence/hang2_interrupt_probe.py
'''
from __future__ import annotations

import os
import sys
import threading
import time

import duckdb
import numpy as np
import pandas as pd

DB = os.path.join('data', 'bench', 'hang2_interrupt_probe.db')
BUDGET_S = 3.0
N_ROWS = 20_000_000


def main() -> None:
    if os.path.exists(DB):
        os.remove(DB)
    conn = duckdb.connect(DB)
    conn.execute('CREATE TABLE t (k BIGINT, v DOUBLE, PRIMARY KEY(k))')
    box: dict = {}

    def _run():
        t0 = time.time()
        try:
            df = pd.DataFrame({
                'k': np.arange(N_ROWS, dtype=np.int64),
                'v': np.random.default_rng(7).random(N_ROWS),
            })
            box['gen_s'] = round(time.time() - t0, 2)
            conn.register('src', df)
            t1 = time.time()
            conn.execute('INSERT INTO t (k, v) SELECT k, v FROM src '
                         'ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v')
            box['exec_s'] = round(time.time() - t1, 2)
            box['outcome'] = 'completed'
        except BaseException as e:  # noqa: BLE001
            box['outcome'] = 'exc:%s.%s' % (type(e).__module__, type(e).__name__)
            box['err'] = str(e)[:200]
            box['exec_s'] = round(time.time() - t0, 2)

    th = threading.Thread(target=_run, daemon=True, name='writer')
    th.start()
    time.sleep(BUDGET_S)
    fired = time.time()
    try:
        conn.interrupt()
        print('interrupt() called, returned in %.3fs' % (time.time() - fired))
    except Exception as e:  # noqa: BLE001
        print('interrupt() raised:', type(e).__name__, e)
    th.join(60.0)
    print('worker alive after 60s join:', th.is_alive())
    print('outcome:', box.get('outcome'), 'err:', box.get('err'))
    print('gen_s:', box.get('gen_s'), 'exec_s:', box.get('exec_s'))
    try:
        print('rows after interrupt:',
              conn.execute('SELECT COUNT(*) FROM t').fetchone()[0])
        print('reusable SELECT 1:', conn.execute('SELECT 1').fetchone()[0])
    except Exception as e:  # noqa: BLE001
        print('post-interrupt query failed:', type(e).__name__, e)
    conn.close()


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
