# -*- coding: utf-8 -*-
'''写路径双段看门狗（W1）验收单测 —— 对应设计 docs/duckdb-write-stall-mitigation-design.md 门2/门3。'''
from __future__ import annotations

import json
import os
import re
import threading
import time

import duckdb
import pandas as pd
import pytest

from quantstudio.pipeline import writers as W
from quantstudio.pipeline.writers import DuckDBWriteStalled, DuckDBWriter, _WriteGuard

_BATCH = 'b1'


@pytest.fixture(autouse=True)
def _no_hard_exit(monkeypatch):
    '''测试内永不真退出：默认关闭 S2，并拦截 os._exit 记录调用。'''
    calls = []
    monkeypatch.setenv('QS_DUCKDB_WRITE_HARD_ABORT_S', '0')
    monkeypatch.setattr(os, '_exit', lambda code: calls.append(code))
    return calls


class _FakeResult:
    def __init__(self, value=0):
        self._value = value

    def fetchone(self):
        return (self._value,)


class _FakeConn:
    '''可控连接：execute 可模拟不返回；记录 interrupt 调用。'''

    def __init__(self, block_s=0.0, exc=None, result=None):
        self.block_s = block_s
        self.exc = exc
        self.result = result if result is not None else _FakeResult(0)
        self.interrupts = 0
        self.sqls = []
        self.unregistered = []
        self.closed = False

    def execute(self, sql, params=None):
        self.sqls.append(sql)
        if self.block_s:
            deadline = time.time() + 5.0
            while self.interrupts == 0 and time.time() < deadline:
                time.sleep(0.01)
            time.sleep(self.block_s)
        if self.exc is not None:
            raise self.exc
        return self.result

    def interrupt(self):
        self.interrupts += 1

    def unregister(self, name):
        self.unregistered.append(name)

    def close(self):
        self.closed = True


def _writer(tmp_path, name='probe'):
    return DuckDBWriter({'path': str(tmp_path / (name + '.db'))})


def _jsonl(tmp_path):
    return tmp_path / 'stall.jsonl'


def _last_rec(tmp_path):
    return json.loads(_jsonl(tmp_path).read_text(encoding='utf-8').strip().splitlines()[-1])


# ── 预算内零行为变化 / 超预算有界失败 / 异常透传 ─────────────────────────
def test_within_budget_is_zero_behavior(tmp_path, monkeypatch):
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '30')
    monkeypatch.setenv('QS_DUCKDB_WRITE_STALL_JSONL', str(_jsonl(tmp_path)))
    w = _writer(tmp_path)
    conn = _FakeConn()
    guard = w._write_guard(conn, phase='dml', table='stock_daily', batch_id=_BATCH,
                           rows=10, sql='INSERT INTO t SELECT 1')
    res = W._guarded(conn, guard, 'INSERT INTO t SELECT 1')
    assert res is conn.result
    assert conn.interrupts == 0
    assert not _jsonl(tmp_path).exists()


def test_budget_exceeded_raises_bounded_failure(tmp_path, monkeypatch):
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '0.2')
    monkeypatch.setenv('QS_DUCKDB_WRITE_STALL_JSONL', str(_jsonl(tmp_path)))
    w = _writer(tmp_path)
    conn = _FakeConn(block_s=0.05)
    sql = 'INSERT INTO stock_daily (code) VALUES (?)'
    guard = w._write_guard(conn, phase='dml', table='stock_daily', batch_id=_BATCH,
                           rows=1234, sql=sql)
    with pytest.raises(DuckDBWriteStalled) as ei:
        W._guarded(conn, guard, sql)
    msg = str(ei.value)
    for token in ('stock_daily', _BATCH, '1234', 'INSERT INTO stock_daily'):
        assert token in msg, token
    assert conn.interrupts == 1
    rec = _last_rec(tmp_path)
    for k in ('ts', 'pid', 'phase', 'table', 'batch_id', 'rows', 'sql_head',
              'budget_s', 'interrupt_sent', 'hard_abort', 'outcome', 'db_path'):
        assert k in rec, k
    assert rec['rows'] == 1234 and rec['table'] == 'stock_daily'
    assert rec['interrupt_sent'] is True and rec['hard_abort'] is False
    assert rec['dedup_circuit_open'] is False   # W4：writer 侧上下文一并落账
    guard.disarm()


def test_nonstall_exception_passes_through(tmp_path, monkeypatch):
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '30')
    w = _writer(tmp_path)
    conn = _FakeConn(exc=ValueError('boom'))
    guard = w._write_guard(conn, phase='dml', table='t', batch_id=_BATCH, rows=1,
                           sql='INSERT INTO t VALUES (1)')
    with pytest.raises(ValueError):
        W._guarded(conn, guard, 'INSERT INTO t VALUES (1)')
    assert conn.interrupts == 0


# ── S2 硬退出 ───────────────────────────────────────────────────────────
def test_hard_abort_exits_with_75(tmp_path, monkeypatch):
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '0.15')
    monkeypatch.setenv('QS_DUCKDB_WRITE_HARD_ABORT_S', '0.25')
    monkeypatch.setenv('QS_DUCKDB_WRITE_STALL_JSONL', str(_jsonl(tmp_path)))
    exits = []
    monkeypatch.setattr(os, '_exit', lambda code: exits.append(code))
    w = _writer(tmp_path)
    conn = _FakeConn()
    conn.interrupt = lambda: exits.append('interrupt')  # type: ignore[assignment]
    guard = w._write_guard(conn, phase='dml', table='stock_daily', batch_id=_BATCH,
                           rows=7, sql='INSERT INTO stock_daily VALUES (1)')
    with pytest.raises(DuckDBWriteStalled):
        with guard:
            deadline = time.time() + 5.0
            while 'interrupt' not in exits and time.time() < deadline:
                time.sleep(0.01)
            time.sleep(0.6)
    assert 75 in exits, exits
    assert _last_rec(tmp_path)['hard_abort'] is True


def test_hard_abort_disabled_never_exits(tmp_path, monkeypatch):
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '0.15')
    monkeypatch.setenv('QS_DUCKDB_WRITE_HARD_ABORT_S', '0')
    exits = []
    monkeypatch.setattr(os, '_exit', lambda code: exits.append(code))
    w = _writer(tmp_path)
    conn = _FakeConn(block_s=0.05)
    guard = w._write_guard(conn, phase='dml', table='t', batch_id=_BATCH, rows=1,
                           sql='INSERT INTO t VALUES (1)')
    with pytest.raises(DuckDBWriteStalled):
        W._guarded(conn, guard, 'INSERT INTO t VALUES (1)')
    time.sleep(0.4)
    assert exits == []          # 只抛错、不退出（GUI/测试逃生阀）


# ── 真实 DuckDB 写事务被注入超时 ─────────────────────────────────────────
def test_real_duckdb_write_interrupts_and_closes(tmp_path, monkeypatch):
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '0.001')
    monkeypatch.setenv('QS_DUCKDB_WRITE_STALL_JSONL', str(_jsonl(tmp_path)))
    conn = duckdb.connect(str(tmp_path / 'real.db'))
    conn.execute('CREATE TABLE t (k BIGINT PRIMARY KEY, v DOUBLE)')
    n = 400_000
    df = pd.DataFrame({'k': pd.Series(range(n), dtype='int64'),
                       'v': pd.Series([1.0] * n)})
    conn.register('src', df)
    sql = 'INSERT INTO t (k, v) SELECT k, v FROM src'
    guard = _WriteGuard(conn, phase='dml', table='t', batch_id=_BATCH, rows=n,
                        sql_head='INSERT INTO t SELECT * FROM src')
    with pytest.raises(DuckDBWriteStalled):
        W._guarded(conn, guard, sql)
    # 连接关闭不得无界（守护线程内执行，超时即判失败，避免拖死测试进程）
    err = {}

    def _close():
        try:
            conn.close()
        except Exception as exc:  # noqa: BLE001
            err['exc'] = repr(exc)

    th = threading.Thread(target=_close, daemon=True)
    th.start()
    th.join(20.0)
    assert not th.is_alive(), 'conn.close() 在停摆后无界（硬退出兜底失效）'
    guard.disarm()


# ── 静态回归：写路径不得再有裸 conn.execute( ─────────────────────────────
_WRAPPED_FUNCS = ('_write_locked', '_write_passthrough', 'write_passthrough_chunked',
                  '_advance_watermark_locked', '_advance_watermark_on_conn',
                  '_upsert_pending_backfill_on_conn')


def _function_body(src: str, name: str) -> str:
    lines = src.splitlines()
    start = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith('def ' + name + '('):
            start = i
            break
    assert start is not None, name
    base_indent = len(lines[start]) - len(lines[start].lstrip())
    out = []
    for ln in lines[start + 1:]:
        if ln.strip() and (len(ln) - len(ln.lstrip())) <= base_indent:
            break
        out.append(ln)
    return '\n'.join(out)


def test_no_bare_execute_in_wrapped_write_paths():
    '''防"新写点绕过看门狗"：唯一裸 execute 入口是模块级 `_guarded`。'''
    src = open(W.__file__, encoding='utf-8').read()
    for fn in _WRAPPED_FUNCS:
        body = _function_body(src, fn)
        bare = [ln.strip() for ln in body.splitlines()
                if re.search(r'\bconn\.execute\(', ln) and '_guarded(' not in ln]
        assert not bare, (fn, bare)


def test_guarded_entrypoints_exist():
    assert callable(W._guarded) and callable(W._guarded_executemany)
    assert W._HARD_ABORT_EXIT_CODE == 75
    assert W._DEFAULT_WRITE_BUDGET_S == 600.0
    assert W._DEFAULT_HARD_ABORT_S == 300.0


# ── 计数 SELECT 停摆不得被 fail-closed 吞掉 ─────────────────────────────
def test_count_stall_is_not_swallowed_by_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '0.2')
    monkeypatch.setenv('QS_DUCKDB_WRITE_STALL_JSONL', str(_jsonl(tmp_path)))
    w = _writer(tmp_path)
    conn = _FakeConn(block_s=0.05)
    sql = 'SELECT COUNT(*) FROM stock_daily WHERE (code, time) IN (SELECT (code, time) FROM _tmp_write)'
    guard = w._write_guard(conn, phase='count', table='stock_daily', batch_id=_BATCH,
                           rows=5, sql=sql)
    with pytest.raises(DuckDBWriteStalled):
        W._guarded(conn, guard, sql)
    # 未被记为 fail-closed（否则会被误判为"计数不可用→静默回退 ON CONFLICT"）
    assert w._dedup_fail_window_hits() == 0
    guard.disarm()


# ── W5：批间隙 CHECKPOINT 默认关 ────────────────────────────────────────
def test_checkpoint_hook_default_off(tmp_path, monkeypatch):
    monkeypatch.delenv('QS_DUCKDB_WRITE_CHECKPOINT_BATCHES', raising=False)
    w = _writer(tmp_path)
    w._maybe_checkpoint_after_batch('stock_daily')      # 默认关：不得抛、不得执行
    monkeypatch.setenv('QS_DUCKDB_WRITE_CHECKPOINT_BATCHES', '2')
    calls = []

    def _fake_ckpt(db_path, timeout_s=120.0):
        calls.append(str(db_path))
        return True, 'fake'

    import quantstudio.pipeline.db_checkpoint as ck
    monkeypatch.setattr(ck, 'checkpoint_database', _fake_ckpt)
    w._maybe_checkpoint_after_batch('stock_daily')      # 第 1 批：未到间隔
    assert calls == []
    w._maybe_checkpoint_after_batch('stock_daily')      # 第 2 批：触发
    assert calls and calls[0].endswith('.db')
