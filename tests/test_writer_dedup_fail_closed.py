"""v3.1 写入分档 / fail-closed / 熔断 契约测试（2026-10-02）。

对应设计：docs/duckdb-conflict-hang-mitigation-design.md §3.1、§3.1.1，验收门 1 与门 6。

**本文件的真红态所在**：
  · 若 fail-closed 改回置 ``None``      → 842 行 ``len(df) - None`` TypeError，契约例全红；
  · 若 fail-closed 误走纯 INSERT        → 含更新批抛 IntegrityError（异常行为变更），红；
  · 若纯 INSERT 与 ON CONFLICT 不等价   → 等价性例红（P1 立论基础崩塌）；
  · 若熔断阈值失效                      → 持续 fail-closed 静默退化不可被发现，红。

背景：DuckDB 长期反复 ``INSERT ... ON CONFLICT`` 写入会导致索引异常停摆（1.5.x 全线
存在）。本线规避思路为"本批主键全新增时降级为纯 INSERT"，故**等价性是命门**——
纯 INSERT 必须与 ON CONFLICT 在无冲突时逐行一致，否则即为静默数据变更。
"""
from __future__ import annotations

import pandas as pd
import pytest

from quantstudio.pipeline.writers import DuckDBWriter

TABLE = "stock_daily"


def _full_df(writer, prefix):
    """按 DDL 全列构造（其余列留空），仅填 code/time/close——避免列数不匹配。"""
    cols = writer._table_columns(TABLE)
    data = {c: [None, None] for c in cols}
    data["code"] = [f"{prefix}0001.SZ", f"{prefix}0002.SZ"]
    data["time"] = [1700000000000, 1700000000000]
    data["close"] = [1.0, 2.0]
    return pd.DataFrame(data)


class _BoomConn:
    """包装 conn：计数 SELECT 抛错，其余透传；close 置 no-op 以便跨批复用。"""

    def __init__(self, real):
        self._real = real

    def execute(self, sql, *a, **kw):
        if "SELECT COUNT(*)" in str(sql):
            raise RuntimeError("mock: dedup count SELECT failed")
        return self._real.execute(sql, *a, **kw)

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._real, name)


def _writer(tmp_path, name):
    return DuckDBWriter({"type": "duckdb", "path": str(tmp_path / name)})


def _dump(writer):
    """按主键排序导出整表，供等价性逐行比对。"""
    conn = writer._conn()
    try:
        return sorted(conn.execute(
            f"SELECT code, time, close FROM {TABLE}").fetchall())
    finally:
        conn.close()


# ── 门 1：分档与等价性 ────────────────────────────────────────────────


def test_all_new_batch_reports_all_new(tmp_path):
    """全新增：new == len(df)，updated == 0（走纯 INSERT 分支）。"""
    w = _writer(tmp_path, "a.db")
    r = w.write(_full_df(w, "A"), TABLE, "batch-1")
    assert (int(r), r.new, r.updated) == (2, 2, 0)
    w.close()


def test_replay_batch_reports_all_updated(tmp_path):
    """重放同主键：updated == len(df)（走 ON CONFLICT 分支），且表不重复。"""
    w = _writer(tmp_path, "b.db")
    w.write(_full_df(w, "A"), TABLE, "batch-1")
    r = w.write(_full_df(w, "A"), TABLE, "batch-2")
    assert (int(r), r.new, r.updated) == (2, 0, 2)
    assert len(_dump(w)) == 2          # upsert 幂等，未产生重复行
    w.close()


def test_plain_insert_equals_on_conflict_when_no_conflict(tmp_path):
    """命门：全新增场景下，纯 INSERT 与 ON CONFLICT 落库结果必须逐行一致。

    左库走正常分档（==0 ⇒ 纯 INSERT）；右库置熔断态强制走 ON CONFLICT 原路径。
    两者数据逐行相同 ⇒ P1 的等价性立论成立。
    """
    left = _writer(tmp_path, "left.db")
    right = _writer(tmp_path, "right.db")
    right._dedup_circuit_open = True   # 强制右库一律走 ON CONFLICT 原路径

    left.write(_full_df(left, "A"), TABLE, "batch-1")
    right.write(_full_df(right, "A"), TABLE, "batch-1")

    assert _dump(left) == _dump(right)
    left.close()
    right.close()


def test_column_subset_batch_is_equivalent(tmp_path):
    """列子集写入：df 只是表的列子集时，两条路径都必须成功且等价。

    **回归锚点**：纯 INSERT 分支曾漏写列名列表（裸 ``INSERT INTO t SELECT *``），
    列子集场景直接抛 ``BinderException: table stock_daily has 42 columns but 3
    values were supplied``，而 ON CONFLICT 分支因显式带 ``({col_list})`` 不受影响
    ——两条路径因此不等价。本例钉死该形态。
    """
    left = _writer(tmp_path, "left_sub.db")
    right = _writer(tmp_path, "right_sub.db")
    right._dedup_circuit_open = True   # 强制右库走 ON CONFLICT 原路径

    def subset(prefix):
        return pd.DataFrame({
            "code": [f"{prefix}0001.SZ", f"{prefix}0002.SZ"],
            "time": [1700000000000, 1700000000000],
            "close": [1.0, 2.0],
        })

    left.write(subset("A"), TABLE, "batch-1")     # 纯 INSERT（P1 规避路径）
    right.write(subset("A"), TABLE, "batch-1")    # ON CONFLICT（原路径）

    assert _dump(left) == _dump(right)
    left.close()
    right.close()


# ── 门 6：fail-closed 契约 ───────────────────────────────────────────


def test_fail_closed_keeps_int_contract_and_conservation(tmp_path):
    """计数失败 ⇒ 哨兵 int，不得为 None；三字段 int 且 new+updated==len(df)。"""
    w = _writer(tmp_path, "c.db")
    real = w._conn()
    orig = w._conn
    w._conn = lambda: _BoomConn(real)
    try:
        r = w.write(_full_df(w, "B"), TABLE, "batch-1")
    finally:
        w._conn = orig

    # None 会让下游 len(df) - updated_rows 抛 TypeError，此处必须全是 int
    assert isinstance(r.new, int) and isinstance(r.updated, int)
    assert isinstance(int(r), int)
    assert r.new + r.updated == 2          # 守恒
    w.close()


def test_fail_closed_does_not_raise_integrity_error(tmp_path):
    """fail-closed 必须回退 ON CONFLICT，不得误走纯 INSERT 而抛 IntegrityError。"""
    w = _writer(tmp_path, "d.db")
    w.write(_full_df(w, "A"), TABLE, "batch-1")     # 先落库，制造"已存在主键"

    real = w._conn()
    orig = w._conn
    w._conn = lambda: _BoomConn(real)
    try:
        # 本批主键已存在；若误走纯 INSERT 会抛 IntegrityError
        r = w.write(_full_df(w, "A"), TABLE, "batch-2")
    finally:
        w._conn = orig

    assert int(r) == 2
    assert len(_dump(w)) == 2
    w.close()


def test_sustained_fail_closed_opens_circuit(tmp_path):
    """窗口内达 OPEN_AT ⇒ 自我熔断；熔断后写入仍成功（回退原路径，语义不变）。"""
    w = _writer(tmp_path, "e.db")
    real = w._conn()
    orig = w._conn
    w._conn = lambda: _BoomConn(real)
    try:
        for i in range(w._DEDUP_FAIL_OPEN_AT + 1):
            w.write(_full_df(w, f"C{i}"), TABLE, f"batch-{i}")
    finally:
        w._conn = orig

    assert w._dedup_circuit_open is True
    assert w._dedup_fail_window_hits() >= w._DEDUP_FAIL_OPEN_AT

    r = w.write(_full_df(w, "D"), TABLE, "batch-after")   # 熔断后仍应成功
    assert int(r) == 2
    w.close()


def test_circuit_reset_clears_state(tmp_path):
    """_dedup_circuit_reset 复位熔断与窗口计数（人工确认后的恢复通道）。"""
    w = _writer(tmp_path, "f.db")
    w._dedup_circuit_open = True
    w._dedup_fail_marks = [1, 2, 3]
    w._dedup_circuit_reset()
    assert w._dedup_circuit_open is False
    assert w._dedup_fail_window_hits() == 0
    w.close()


def test_window_slides_out_stale_marks(tmp_path):
    """滑动窗口：超出 WINDOW 批次的旧命中必须被剔除，避免永久累积误熔断。"""
    w = _writer(tmp_path, "g.db")
    w._write_batch_seq = 100
    w._dedup_fail_marks = [1, 2, 3]           # 早已过期的命中
    w._record_dedup_fail_closed(TABLE)        # 以当前 seq=100 记一次命中并滑动窗口
    assert w._dedup_fail_marks == [100]       # 旧命中（1,2,3）已滑出窗口
    assert w._dedup_circuit_open is False
    w.close()
