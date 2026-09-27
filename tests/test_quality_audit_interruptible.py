"""D 件：质量审计「可中断」验收测试。

设计：docs/daemon-interruptible-quality-audit-design.md §5（2/3/4/5/6）。
覆盖：无 should_stop 等价性、检查点序号差口径、中断标记诚实、
连接回收（own_conn / shared_conn）、真实失败不被误吞。
"""
from __future__ import annotations

import duckdb
import pytest

from quantstudio.pipeline.quality_audit import (
    AuditInterrupted,
    DataQualityAuditor,
    QualityReport,
)


def _make_schemas(names):
    return {n: {"columns": {"code": {"required": True}, "time": {"required": True}},
                "primary_key": ["code", "time"]} for n in names}


def _make_db(path, names, rows=2):
    with duckdb.connect(str(path)) as c:
        for n in names:
            c.execute(f"CREATE TABLE {n}(code VARCHAR, time BIGINT, PRIMARY KEY(code, time))")
            for i in range(rows):
                c.execute(f"INSERT INTO {n} VALUES (?, ?)", [f"60000{i}", 90000 + i])
    return _make_schemas(names)


def _fingerprint(report):
    return (report.checks_run, report.interrupted, report.skipped_after_stop,
            [(i.check, i.table, i.count, i.severity, i.detail) for i in report.issues])


# --- 等价性（§5-2 / §2.4-1）：should_stop 缺省与显式 None 逐位一致 ---

def test_default_matches_explicit_none(tmp_path):
    db = tmp_path / "q.db"
    schemas = _make_db(db, ("t1", "t2", "t3"))
    default = DataQualityAuditor(db, schemas).run()
    explicit = DataQualityAuditor(db, schemas, should_stop=None).run()
    assert _fingerprint(default) == _fingerprint(explicit)
    assert default.interrupted is False
    assert default.skipped_after_stop == 0


def test_never_stopping_predicate_matches_default(tmp_path):
    db = tmp_path / "q.db"
    schemas = _make_db(db, ("t1", "t2", "t3"))
    base = DataQualityAuditor(db, schemas).run()
    guarded = DataQualityAuditor(db, schemas, should_stop=lambda: False).run()
    assert _fingerprint(base) == _fingerprint(guarded)
    assert guarded.interrupted is False and guarded.skipped_after_stop == 0


# --- 检查点语义（含序号差口径） ---

def test_checkpoint_noop_without_should_stop(tmp_path):
    aud = DataQualityAuditor(tmp_path / "x.db", {})
    rep = QualityReport()
    aud._checkpoint(rep, "table:t1", top=True)
    assert rep.interrupted is False and rep.skipped_after_stop == 0
    # 未注入 should_stop 时顶层计数器不被触碰
    assert aud._ckpt_top_done == 0


def test_checkpoint_serial_diff_counting(tmp_path):
    aud = DataQualityAuditor(tmp_path / "x.db", {}, should_stop=lambda: True)
    aud._ckpt_top_total = 7
    aud._ckpt_top_done = 2
    rep = QualityReport()
    with pytest.raises(AuditInterrupted) as ei:
        aud._checkpoint(rep, "table:t3", top=True)
    assert ei.value.checkpoint == "table:t3"
    assert rep.interrupted is True
    assert rep.skipped_after_stop == 5      # 7 - 3 + 1（含当前未执行段）
    assert aud._ckpt_top_done == 3


def test_non_top_checkpoint_does_not_advance_serial(tmp_path):
    calls = {"n": 0}

    def _stop():
        calls["n"] += 1
        return calls["n"] >= 2

    aud = DataQualityAuditor(tmp_path / "x.db", {}, should_stop=_stop)
    aud._ckpt_top_total = 7
    rep = QualityReport()
    aud._checkpoint(rep, "prices:t1")            # 第 1 次：不中止
    with pytest.raises(AuditInterrupted):
        aud._checkpoint(rep, "anchor:t1")        # 第 2 次：中止
    assert aud._ckpt_top_done == 0               # 非顶层检查点不计序号
    assert rep.skipped_after_stop == 8           # max(1, 7 - 0 + 1)


# --- 端到端：中断生效、无异常冒泡、已执行项保留 ---

def test_interrupt_at_first_checkpoint(tmp_path):
    db = tmp_path / "q.db"
    schemas = _make_db(db, ("t1", "t2", "t3"))
    aud = DataQualityAuditor(db, schemas, should_stop=lambda: True)
    rep = aud.run()
    assert rep.interrupted is True
    assert rep.checks_run == 0
    assert aud._ckpt_top_total == 7              # 3 表 + 4 无条件全局段
    assert rep.skipped_after_stop == 7


def test_interrupt_mid_way_keeps_executed_conclusions(tmp_path):
    db = tmp_path / "q.db"
    schemas = _make_db(db, ("t1", "t2", "t3"))
    calls = {"n": 0}

    def _stop():
        calls["n"] += 1
        return calls["n"] >= 5

    aud = DataQualityAuditor(db, schemas, should_stop=_stop)
    rep = aud.run()
    assert rep.interrupted is True
    assert rep.checks_run > 0                    # 已执行项结论保留
    assert 1 <= rep.skipped_after_stop <= 7
    assert rep.skipped_after_stop == 7 - aud._ckpt_top_done + 1


def test_interrupt_closes_own_conn_exactly_once(tmp_path, monkeypatch):
    db = tmp_path / "q.db"
    schemas = _make_db(db, ("t1", "t2"))
    real_connect = duckdb.connect
    closed = {"n": 0}

    class _SpyConn:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def close(self):
            closed["n"] += 1
            return self._inner.close()

    monkeypatch.setattr(duckdb, "connect", lambda *a, **kw: _SpyConn(real_connect(*a, **kw)))
    rep = DataQualityAuditor(db, schemas, should_stop=lambda: True).run()
    assert rep.interrupted is True
    assert closed["n"] == 1                      # own_conn 恰好回收一次
    monkeypatch.undo()
    with duckdb.connect(str(db), read_only=True) as c:
        assert c.execute("SHOW TABLES").fetchall()   # 无残留锁


def test_interrupt_leaves_shared_conn_usable(tmp_path):
    db = tmp_path / "q.db"
    schemas = _make_db(db, ("t1", "t2"))
    with duckdb.connect(str(db)) as shared:
        rep = DataQualityAuditor(db, schemas, shared_conn=shared,
                                 should_stop=lambda: True).run()
        assert rep.interrupted is True
        # shared_conn 不被关闭（收尾项后续仍要用）
        assert shared.execute("SHOW TABLES").fetchall()


# --- 真实失败不被误吞（§2.4-5） ---

def test_real_query_error_propagates_not_as_interrupt(tmp_path):
    db = tmp_path / "q.db"
    with duckdb.connect(str(db)) as c:
        c.execute("CREATE TABLE t1(code VARCHAR)")
        c.execute(f"INSERT INTO t1 VALUES ('600000')")
    schemas = {"t1": {"columns": {"code": {"required": True, "regex": "("}},
                      "primary_key": ["code"]}}
    with pytest.raises(Exception) as ei:
        DataQualityAuditor(db, schemas, should_stop=lambda: False).run()
    assert not isinstance(ei.value, AuditInterrupted)


def test_connect_failure_propagates(tmp_path):
    missing = tmp_path / "nope" / "q.db"
    with pytest.raises(Exception) as ei:
        DataQualityAuditor(missing, {}, should_stop=lambda: True).run()
    assert not isinstance(ei.value, AuditInterrupted)
