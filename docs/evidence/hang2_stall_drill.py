# -*- coding: utf-8 -*-
"""门7 停摆注入演练：把写路径 DML 替换为永不返回的替身，验证有界收敛行为链。

  1. 抛 DuckDBWriteStalled（而非静默挂起）
  2. 3A 写锁已释放（后续写可继续拿到锁）
  3. 事务未提交（表内无半截数据）
  4. 诊断 JSONL 可定位到该批（表/批/行数/SQL 头）
  5. 恢复后可继续写入（幂等 upsert 语义不受影响）

用法：python docs/evidence/hang2_stall_drill.py
输出：docs/evidence/hang2_stall_drill.txt
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time

import duckdb
import pandas as pd

sys.path.insert(0, os.path.abspath('.'))

from quantstudio.pipeline import writers as W
from quantstudio.pipeline.writers import DuckDBWriteStalled, DuckDBWriter

OUT = os.path.join('docs', 'evidence', 'hang2_stall_drill.txt')
MARK = 'INSERT INTO stock_daily'
_DRILL_DEADLINE_S = 30.0        # 演练自身硬截止：看门狗未发 interrupt 即判演练失败
_FAKE_INTERRUPT_LATENCY_S = 0.5  # 压缩模拟 F5 实测 ~11.8s 的 interrupt 生效延迟
_INTERRUPT_SEEN = threading.Event()      # 场景A：看门狗已发 interrupt
_HARD_CALLED = threading.Event()         # 场景B：S2 已触发硬退出


class _ProxyConn:
    '''包装真实连接：记录 interrupt 到达时刻（替身据此"被中断"），其余原样转发。'''

    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def interrupt(self):
        _INTERRUPT_SEEN.set()
        return self._real.interrupt()


def _count(db):
    import duckdb
    c = duckdb.connect(db, read_only=True)
    try:
        return c.execute('SELECT COUNT(*) FROM stock_daily').fetchone()[0]
    finally:
        c.close()


def main():
    lines = []
    bench = os.path.join('data', 'bench', 'hang2_drill')
    os.makedirs(bench, exist_ok=True)
    db = os.path.join(bench, 'drill.db')
    for suffix in ('', '.wal'):
        if os.path.exists(db + suffix):
            os.remove(db + suffix)
    jsonl = os.path.join(bench, 'stall.jsonl')
    if os.path.exists(jsonl):
        os.remove(jsonl)
    os.environ['QS_DUCKDB_WRITE_TIMEOUT_S'] = '1.0'
    os.environ['QS_DUCKDB_WRITE_HARD_ABORT_S'] = '0'    # 演练不退出进程
    os.environ['QS_DUCKDB_WRITE_STALL_JSONL'] = jsonl

    w = DuckDBWriter({'path': db})
    df = pd.DataFrame({'code': ['000001', '000002'], 'time': [20240102, 20240102],
                       'open': [1.0, 2.0], 'close': [1.1, 2.2]})

    r1 = w.write(df, 'stock_daily', 'batch-ok')
    lines.append('正常写: submitted=%d new=%d updated=%d 守恒=%s'
                 % (int(r1), r1.new, r1.updated, r1.new + r1.updated == int(r1)))

    # 注入方式（**修正版**）：
    # 上一版用 `while True: sleep(0.2)` 模拟"永不返回"——该替身**不轮询中断标志**，
    # 因此 interrupt 发出后仍不退出，而演练又关闭了 S2 ⇒ 演练进程自身无限空转
    # （2026-10-02 23:29 实测：S1 日志已落、无最终产出、进程被人工取消）。
    # 修正：替身改为"收到 interrupt 后再抛 InterruptException"（对齐 F5 的真实行为），
    # 并加**演练自身硬截止**（看门狗未发 interrupt 或替身不退出 ⇒ 演练判失败而非挂起）。
    real_guarded = W._guarded
    real_conn = DuckDBWriter._conn
    DuckDBWriter._conn = lambda self: _ProxyConn(real_conn(self))

    def _stalling(conn, guard, sql, params=None):
        if MARK in str(sql):
            with guard:
                if not _INTERRUPT_SEEN.wait(_DRILL_DEADLINE_S):
                    raise AssertionError('演练失败：看门狗未在 %.0fs 内发出 interrupt'
                                         % _DRILL_DEADLINE_S)
                time.sleep(_FAKE_INTERRUPT_LATENCY_S)   # 模拟实测 ~11.8s 生效延迟（压缩）
                raise duckdb.InterruptException('INTERRUPT Error: Interrupted!')
        return real_guarded(conn, guard, sql, params)

    W._guarded = _stalling
    t0 = time.time()
    err = None
    try:
        w.write(df, 'stock_daily', 'batch-stall')
    except BaseException as exc:            # noqa: BLE001
        err = exc
    elapsed = time.time() - t0
    W._guarded = real_guarded
    DuckDBWriter._conn = real_conn
    lines.append('停摆注入: 异常类型=%s 有界收敛耗时=%.1fs（预算 1.0s）'
                 % (type(err).__name__, elapsed))
    lines.append('停摆消息: %s' % (str(err)[:240] if err else 'None'))

    if os.path.exists(jsonl):
        rec = json.loads(open(jsonl, encoding='utf-8').read().strip().splitlines()[-1])
        lines.append('诊断: table=%s batch=%s rows=%s phase=%s outcome=%s interrupt=%s hard=%s'
                     % (rec.get('table'), rec.get('batch_id'), rec.get('rows'),
                        rec.get('phase'), rec.get('outcome'), rec.get('interrupt_sent'),
                        rec.get('hard_abort')))
        lines.append('诊断SQL头: %s' % str(rec.get('sql_head'))[:110])
    else:
        lines.append('诊断: 缺失（异常！）')

    n = _count(db)
    lines.append('停摆后表内行数=%d（期望 2：第一批已提交，停摆批未提交）' % n)

    r3 = w.write(df, 'stock_daily', 'batch-after')
    n2 = _count(db)
    lines.append('恢复后写: submitted=%d new=%d updated=%d 守恒=%s 表内行数=%d（期望仍 2）'
                 % (int(r3), r3.new, r3.updated, r3.new + r3.updated == int(r3), n2))
    ok = (isinstance(err, DuckDBWriteStalled) and n == 2 and n2 == 2
          and os.path.exists(jsonl))
    lines.append('结论A（语句响应 interrupt）: %s'
                 % ('PASS 有界失败+锁释放+无半截+可续跑' if ok else 'FAIL 需排查'))

    # ── 场景B：语句**不响应 interrupt**（对齐 2026-10-02 演练卡死的真实形态）──
    # 即：S1 已发 interrupt，但语句内部的忙等不轮询中断标志；此时唯一的上界是 S2。
    lines.append('')
    lines.append('场景B：替身不响应 interrupt（无中断标志可轮询）')
    _INTERRUPT_SEEN.clear()
    _HARD_CALLED.clear()
    os.environ['QS_DUCKDB_WRITE_HARD_ABORT_S'] = '3.0'
    if os.path.exists(jsonl):
        os.remove(jsonl)
    real_exit = os._exit
    exits = []

    def _fake_exit(code):
        exits.append(code)
        _HARD_CALLED.set()

    os._exit = _fake_exit                    # 演练不真退出：记录后放行
    DuckDBWriter._conn = lambda self: _ProxyConn(real_conn(self))

    def _deaf_stalling(conn, guard, sql, params=None):
        if MARK in str(sql):
            with guard:
                # 不理会 interrupt，只等 S2 触发（或演练硬截止判失败）
                if not _HARD_CALLED.wait(_DRILL_DEADLINE_S):
                    raise AssertionError('演练失败：S2 未在 %.0fs 内触发硬退出'
                                         % _DRILL_DEADLINE_S)
                raise duckdb.InterruptException('INTERRUPT Error: Interrupted!')
        return real_guarded(conn, guard, sql, params)

    W._guarded = _deaf_stalling
    t1 = time.time()
    err_b = None
    try:
        w.write(df, 'stock_daily', 'batch-stall-deaf')
    except BaseException as exc:            # noqa: BLE001
        err_b = exc
    elapsed_b = time.time() - t1
    W._guarded = real_guarded
    DuckDBWriter._conn = real_conn
    os._exit = real_exit
    os.environ['QS_DUCKDB_WRITE_HARD_ABORT_S'] = '0'
    rec_b = {}
    if os.path.exists(jsonl):
        rec_b = json.loads(open(jsonl, encoding='utf-8').read().strip().splitlines()[-1])
    lines.append('场景B: 异常=%s 收敛耗时=%.1fs 硬退出码=%s 诊断hard_abort=%s'
                 % (type(err_b).__name__, elapsed_b, exits, rec_b.get('hard_abort')))
    lines.append('结论B（语句不响应 interrupt）: %s'
                 % ('PASS S2 兜底触发 os._exit(75)（生产须保持 S2>0）'
                    if 75 in exits else 'FAIL S2 未兜底'))
    text = '\n'.join(lines)
    with open(OUT, 'w', encoding='utf-8') as fh:
        fh.write(text + '\n')
    sys.stdout.reconfigure(encoding='utf-8')
    print(text)
    print('written', OUT)


if __name__ == '__main__':
    main()
