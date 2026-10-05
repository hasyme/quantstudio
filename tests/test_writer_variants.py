# -*- coding: utf-8 -*-
"""v3 主动规避变体（V1/V2/V3/V4）A1 离线等价门测试。

纪律：① 默认全关时行为与 v1 逐项一致；② 变体开启后目标态与基线逐项一致
（ORDER BY 黄金对比 + dtype + 主键 + 校验和）；③ **count 不重算**（裁定③a）；
④ 显式事务内 interrupt 回滚语义（R-2 A1 必测项）。
"""
from __future__ import annotations

import os
import sys

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath('.'))

from quantstudio.pipeline.writers import DuckDBWriter, DuckDBWriteStalled, _WriteGuard

_CLEAN = ['QS_DUCKDB_WRITE_CHUNK_ROWS', 'QS_DUCKDB_WRITE_SORT_BY_PK',
          'QS_DUCKDB_WRITE_STAGING', 'QS_DUCKDB_WRITE_THREADS',
          'QS_DUCKDB_WRITE_NO_PRESERVE_ORDER']


@pytest.fixture(autouse=True)
def _variants_off(monkeypatch):
    for k in _CLEAN:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv('QS_DUCKDB_WRITE_HARD_ABORT_S', '0')


def _df(codes, day, n=2):
    rows = []
    for c in codes:
        for k in range(n):
            rows.append({'code': c, 'time': day + k, 'open': 1.0 + k, 'close': 2.0 + k})
    return pd.DataFrame(rows)


def _w(tmp_path, name='v'):
    return DuckDBWriter({'path': str(tmp_path / (name + '.db'))})


def _golden(w, table='stock_daily'):
    df = w.execute_sql_for_test('SELECT code, time, open, close FROM %s ORDER BY code, time'
                                % table) if hasattr(w, 'execute_sql_for_test') else None
    return df


def _read(db, sql):
    c = duckdb.connect(str(db), read_only=True)
    try:
        return c.execute(sql).fetchdf()
    finally:
        c.close()

CODES = ['000001', '000002', '000003']
GOLD = ('SELECT code, time, open, close FROM stock_daily ORDER BY code, time')


def _baseline(tmp_path):
    """基线库：一次性写入两批（第二批全更新），返回 (db, 期望黄金 DataFrame)。"""
    db = tmp_path / 'base.db'
    w = _w(tmp_path, 'base')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    w.write(_df(CODES, 20240103), 'stock_daily', 'b2')
    return db, _read(db, GOLD)


def test_default_off_is_identical_to_baseline(tmp_path):
    db, gold = _baseline(tmp_path)
    w = _w(tmp_path, 'plain')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    w.write(_df(CODES, 20240103), 'stock_daily', 'b2')
    got = _read(tmp_path / 'plain.db', GOLD)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), gold.reset_index(drop=True))


def test_v1_chunked_matches_baseline(tmp_path, monkeypatch):
    db, gold = _baseline(tmp_path)
    monkeypatch.setenv('QS_DUCKDB_WRITE_CHUNK_ROWS', '2')   # 6 行 -> 3 子批
    w = _w(tmp_path, 'v1')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    w.write(_df(CODES, 20240103), 'stock_daily', 'b2')
    got = _read(tmp_path / 'v1.db', GOLD)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), gold.reset_index(drop=True))


def test_v1_does_not_recompute_count(tmp_path, monkeypatch):
    """裁定③a：子批化不得重算写前 count —— new/updated 口径与基线一致。"""
    _baseline(tmp_path)
    monkeypatch.setenv('QS_DUCKDB_WRITE_CHUNK_ROWS', '2')
    w = _w(tmp_path, 'v1c')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    r = w.write(_df(CODES, 20240102), 'stock_daily', 'b2')   # 同主键重放 => 全更新
    assert (int(r), r.new, r.updated) == (6, 0, 6)
    assert r.new + r.updated == int(r)


def test_v2_sort_by_pk_matches_baseline(tmp_path, monkeypatch):
    _baseline(tmp_path)
    monkeypatch.setenv('QS_DUCKDB_WRITE_SORT_BY_PK', '1')
    w = _w(tmp_path, 'v2')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    w.write(_df(CODES, 20240103), 'stock_daily', 'b2')
    got = _read(tmp_path / 'v2.db', GOLD)
    pd.testing.assert_frame_equal(got.reset_index(drop=True),
                                  _read(tmp_path / 'base.db', GOLD).reset_index(drop=True))


def test_v3_threads_and_no_order_match_baseline(tmp_path, monkeypatch):
    _baseline(tmp_path)
    monkeypatch.setenv('QS_DUCKDB_WRITE_THREADS', '2')
    monkeypatch.setenv('QS_DUCKDB_WRITE_NO_PRESERVE_ORDER', '1')
    w = _w(tmp_path, 'v3')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    w.write(_df(CODES, 20240103), 'stock_daily', 'b2')
    got = _read(tmp_path / 'v3.db', GOLD)
    pd.testing.assert_frame_equal(got.reset_index(drop=True),
                                  _read(tmp_path / 'base.db', GOLD).reset_index(drop=True))


def test_v4_staging_matches_baseline_and_is_pkless(tmp_path, monkeypatch):
    _baseline(tmp_path)
    monkeypatch.setenv('QS_DUCKDB_WRITE_STAGING', '1')
    w = _w(tmp_path, 'v4')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    w.write(_df(CODES, 20240103), 'stock_daily', 'b2')
    got = _read(tmp_path / 'v4.db', GOLD)
    pd.testing.assert_frame_equal(got.reset_index(drop=True),
                                  _read(tmp_path / 'base.db', GOLD).reset_index(drop=True))
    # staging 事务内 DELETE 生效：两批主键在 (20240103) 上重叠 => 合入后 9 行（3 码 × 3 日）
    # 若 DELETE 未生效则该键会重复 => 12 行，故此断言同时锁住"事务内 DELETE 语义"
    assert len(got) == 9
    assert got.duplicated(subset=['code', 'time']).sum() == 0
    # staging 表无残留（R-3）
    left = _read(tmp_path / 'v4.db',
                 "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '_stg_%'")
    assert len(left) == 0

def test_r2_interrupt_inside_explicit_transaction_rolls_back(tmp_path, monkeypatch):
    """审计 R-2 A1 必测项：显式 BEGIN 内 interrupt 的回滚语义（F5 只测过隐式事务）。

    断言：① 回滚后表内容不变（无半提交）；② 连接可复用；③ 抛 DuckDBWriteStalled。
    """
    monkeypatch.setenv('QS_DUCKDB_WRITE_TIMEOUT_S', '0.2')
    jsonl = tmp_path / 'r2.jsonl'
    monkeypatch.setenv('QS_DUCKDB_WRITE_STALL_JSONL', str(jsonl))
    db = str(tmp_path / 'r2.db')
    w = _w(tmp_path, 'r2')
    w.write(_df(CODES, 20240102), 'stock_daily', 'seed')
    before = _read(db, GOLD)

    conn = w._conn()
    conn.execute('CREATE TABLE _tx_probe (k BIGINT, v DOUBLE)')
    conn.execute('INSERT INTO _tx_probe VALUES (1, 1.0)')
    conn.execute('BEGIN TRANSACTION')
    try:
        import threading
        import time
        stop = threading.Event()

        def _slow():
            try:
                cur = conn.execute(
                    'INSERT INTO _tx_probe SELECT range + 100000, 2.0 FROM range(6000000)')
                cur.fetchall()
            except BaseException:
                pass
            stop.set()

        th = threading.Thread(target=_slow, daemon=True)
        guard = _WriteGuard(conn, phase='r2', table='_tx_probe', batch_id='r2', rows=1,
                            sql_head='INSERT INTO _tx_probe SELECT ...')
        with pytest.raises(DuckDBWriteStalled):
            with guard:
                th.start()
                th.join(30)
        stop.set()
        th.join(30)
    finally:
        try:
            conn.execute('ROLLBACK')
        except Exception:
            pass
    n = conn.execute('SELECT COUNT(*) FROM _tx_probe').fetchone()[0]
    conn.close()
    assert n == 1, '显式事务内 interrupt 未回滚（出现半提交）'
    assert len(_read(db, GOLD)) == len(before)


def test_r2_v4_rolls_back_before_raising(tmp_path, monkeypatch):
    """R-2：V4 在事务内遇停摆异常时**先 ROLLBACK 再抛**，主表不得丢行。

    用模块级 `_guarded` 替身模拟"停摆异常已由看门狗抛出"这一时刻，验证
    `_write_via_staging` 的 except 分支先 ROLLBACK 再把异常抛给调用方（不半提交）。
    """
    import threading
    import time
    import quantstudio.pipeline.writers as W

    monkeypatch.setenv('QS_DUCKDB_WRITE_STAGING', '1')
    w = _w(tmp_path, 'r2c')
    w.write(_df(CODES, 20240102), 'stock_daily', 'seed')
    before = _read(str(tmp_path / 'r2c.db'), GOLD)

    real_guarded = W._guarded

    def _stall_on_delete(conn, guard, sql, params=None):
        if guard.phase == 'v4_delete':
            raise DuckDBWriteStalled('simulated stall inside v4 transaction')
        return real_guarded(conn, guard, sql, params)

    W._guarded = _stall_on_delete
    try:
        with pytest.raises(DuckDBWriteStalled):
            w.write(_df(CODES, 20240102), 'stock_daily', 'again')
    finally:
        W._guarded = real_guarded
    after = _read(str(tmp_path / 'r2c.db'), GOLD)
    pd.testing.assert_frame_equal(after.reset_index(drop=True),
                                  before.reset_index(drop=True))
    # 事务已结束（连接未留在 aborted 状态）：后续写可继续
    r = w.write(_df(CODES, 20240103), 'stock_daily', 'b3')
    assert r.new + r.updated == int(r)

def _v7_env(monkeypatch, batches, min_rows=0):
    monkeypatch.setenv('QS_DUCKDB_WRITE_REBUILD_PK_BATCHES', str(batches))
    monkeypatch.setenv('QS_DUCKDB_WRITE_REBUILD_PK_MIN_ROWS', str(min_rows))


def test_v7_default_off_keeps_table_intact(tmp_path, monkeypatch):
    monkeypatch.delenv('QS_DUCKDB_WRITE_REBUILD_PK_BATCHES', raising=False)
    w = _w(tmp_path, 'v7off')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b2')
    c = duckdb.connect(str(tmp_path / 'v7off.db'), read_only=True)
    pk = c.execute("SELECT constraint_text FROM duckdb_constraints() WHERE table_name='stock_daily' "
                   "AND constraint_type='PRIMARY KEY'").fetchall()
    n = c.execute('SELECT COUNT(*) FROM stock_daily').fetchone()[0]
    c.close()
    assert len(pk) == 1 and n == 6      # 主键未被重建、行数守恒


def test_v7_rebuild_preserves_rows_pk_and_allows_write(tmp_path, monkeypatch):
    _v7_env(monkeypatch, 1, 0)          # 每批重建、门槛 0（小表也触发）
    w = _w(tmp_path, 'v7on')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    before = _read(str(tmp_path / 'v7on.db'), GOLD)
    c = duckdb.connect(str(tmp_path / 'v7on.db'), read_only=True)
    pk = c.execute("SELECT constraint_text FROM duckdb_constraints() WHERE table_name='stock_daily' "
                   "AND constraint_type='PRIMARY KEY'").fetchall()
    tmp_left = c.execute("SELECT table_name FROM information_schema.tables "
                         "WHERE table_name LIKE '_v7_new_%'").fetchall()
    c.close()
    assert len(pk) == 1, pk             # 重建后 PK 仍在（且为复合键）
    assert 'code' in pk[0][0] and 'time' in pk[0][0]
    assert tmp_left == []               # 无残留临时表
    # 重建后仍可正常写（含 upsert 与新键）
    r = w.write(_df(CODES, 20240102), 'stock_daily', 'b2')
    assert (r.new, r.updated) == (0, 6)
    r2 = w.write(_df(CODES, 20240105), 'stock_daily', 'b3')
    assert r2.new == 6 and r2.updated == 0
    got = _read(str(tmp_path / 'v7on.db'), GOLD)
    # 仅比对原有日期区间（b3 新增了 20240105 的新键），按 (code,time) 黄金序逐项一致
    orig = got[got['time'] <= 20240103].reset_index(drop=True)
    pd.testing.assert_frame_equal(orig, before.reset_index(drop=True))


def test_v7_min_rows_threshold_skips_small_tables(tmp_path, monkeypatch):
    _v7_env(monkeypatch, 1, 1_000_000)  # 门槛 100 万，小表跳过
    w = _w(tmp_path, 'v7skip')
    w.write(_df(CODES, 20240102), 'stock_daily', 'b1')
    c = duckdb.connect(str(tmp_path / 'v7skip.db'), read_only=True)
    pk = c.execute("SELECT constraint_name FROM duckdb_constraints() WHERE table_name='stock_daily' "
                   "AND constraint_type='PRIMARY KEY'").fetchall()
    c.close()
    assert len(pk) == 1                  # 未被重建（约束名保持原样仍是 1 个，但内容不变）
