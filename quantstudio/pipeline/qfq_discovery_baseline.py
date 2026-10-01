"""Generation-specific discovery baseline with two-phase CAS.

The module owns only the DuckDB baseline ledger.  Trigger construction remains
in the event-discovery layer; this module supplies the atomic reservation and
commit contracts so concurrent discoverers cannot create B/C races or let an
old trigger roll back a newer applied payload.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Callable, Iterable, Optional, Sequence


BJ_TZ = timezone(timedelta(hours=8))


class DiscoveryBaselineError(RuntimeError):
    """Baseline invariant or CAS violation."""


@dataclass(frozen=True)
class BaselineIdentity:
    cutover_id: str
    price_source: str
    source_generation: str


def now_ts() -> str:
    return datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


def logical_key_stock_dividend(code: str, ex_date: int) -> str:
    return f"stock_dividend|{code}|{int(ex_date)}"


def _fetchone_returning(conn, sql: str, params: Sequence):
    return conn.execute(sql, list(params)).fetchone()


def establish_discovery_baseline(conn, *, identity: BaselineIdentity,
                                 rows: Iterable[Sequence],
                                 payload_hash: Callable[[Sequence], str],
                                 require_status: str = "baseline_building") -> int:
    """Build/update a baseline only while the cutover is baseline_building."""
    status = conn.execute(
        "SELECT status FROM qfq_source_cutover WHERE cutover_id=?",
        [identity.cutover_id]).fetchone()
    if status is None or status[0] != require_status:
        raise DiscoveryBaselineError(
            f"cutover={identity.cutover_id!r} 不允许覆盖 discovery baseline，"
            f"status={status[0] if status else None!r}")
    count = 0
    ts = now_ts()
    for row in rows:
        if len(row) != 13:
            raise DiscoveryBaselineError(
                f"stock_dividend baseline row 必须为 13 字段，收到 {len(row)}")
        code, ex_date = row[0], row[1]
        key = logical_key_stock_dividend(str(code), int(ex_date))
        ph = payload_hash(row)
        conn.execute(
            "INSERT INTO qfq_discovery_baseline "
            "(cutover_id, price_source, source_generation, event_logical_key, "
            " applied_payload_hash, pending_trigger_id, pending_payload_hash, "
            " last_trigger_id, applied_at, baselined_at, updated_at) "
            "VALUES (?,?,?,?,?,NULL,NULL,NULL,?,?,?) "
            "ON CONFLICT (cutover_id, event_logical_key) DO UPDATE SET "
            "price_source=excluded.price_source, "
            "source_generation=excluded.source_generation, "
            "applied_payload_hash=excluded.applied_payload_hash, "
            "updated_at=excluded.updated_at",
            [identity.cutover_id, identity.price_source, identity.source_generation,
             key, ph, ts, ts, ts],
        )
        count += 1
    return count


def reserve_pending_slot(conn, *, identity: BaselineIdentity,
                         event_logical_key: str, trigger_id: str,
                         payload_hash: str) -> bool:
    """Atomically reserve a baseline pending slot.

    Returns ``True`` only for the caller that owns the slot.  Existing applied
    payloads and an existing pending trigger both return ``False``.
    """
    ts = now_ts()
    row = _fetchone_returning(
        conn,
        "UPDATE qfq_discovery_baseline SET pending_trigger_id=?, "
        "pending_payload_hash=?, updated_at=? "
        "WHERE cutover_id=? AND event_logical_key=? "
        "AND pending_trigger_id IS NULL "
        "AND applied_payload_hash IS DISTINCT FROM ? "
        "RETURNING cutover_id, event_logical_key",
        [trigger_id, payload_hash, ts, identity.cutover_id, event_logical_key,
         payload_hash],
    )
    if row is not None:
        return True
    row = _fetchone_returning(
        conn,
        "INSERT INTO qfq_discovery_baseline "
        "(cutover_id, price_source, source_generation, event_logical_key, "
        " applied_payload_hash, pending_trigger_id, pending_payload_hash, "
        " last_trigger_id, applied_at, baselined_at, updated_at) "
        "VALUES (?,?,?,?,NULL,?,?,NULL,NULL,?,?) "
        "ON CONFLICT (cutover_id, event_logical_key) DO NOTHING "
        "RETURNING cutover_id, event_logical_key",
        [identity.cutover_id, identity.price_source, identity.source_generation,
         event_logical_key, trigger_id, payload_hash, ts, ts],
    )
    return row is not None


# ---------------------------------------------------------------------------
# 批量变体（A 件：O(N) 显式事务 → 常数批；逐行 API 全部保留，供其他调用方与
# 既有测试使用）。批量路径把 reserve_pending_slot 的两条语句（UPDATE 命中 /
# INSERT 建行）拆成「集合 INSERT 建行 → 集合 UPDATE 占槽」两步，判定条件逐条对应：
#   - 已 applied 同 payload → 不占槽（`IS DISTINCT FROM` 保留）
#   - 已存在 pending        → 不占槽（`pending_trigger_id IS NULL` 保留）
# batch_relation 由调用方建为 TEMP 表，须含列：
#   event_logical_key VARCHAR, trigger_id VARCHAR, payload_hash VARCHAR,
#   reserved BOOLEAN DEFAULT FALSE, trigger_pre_existing BOOLEAN DEFAULT FALSE
# 该 TEMP 表须按 event_logical_key 去重（保首行）= 逐行实现的「先到先占」语义。
# ---------------------------------------------------------------------------

def ensure_baseline_rows_for_batch(conn, *, identity: BaselineIdentity,
                                   batch_relation: str,
                                   ts: Optional[str] = None) -> None:
    """集合等价于 reserve_pending_slot 的 INSERT 分支：缺席 key 先建行（pending=NULL）。"""
    ts = ts or now_ts()
    conn.execute(
        "INSERT INTO qfq_discovery_baseline "
        "(cutover_id, price_source, source_generation, event_logical_key, "
        " applied_payload_hash, pending_trigger_id, pending_payload_hash, "
        " last_trigger_id, applied_at, baselined_at, updated_at) "
        "SELECT ?, ?, ?, c.event_logical_key, NULL, NULL, NULL, NULL, NULL, ?, ? "
        f"FROM {batch_relation} AS c "
        "ON CONFLICT (cutover_id, event_logical_key) DO NOTHING",
        [identity.cutover_id, identity.price_source, identity.source_generation,
         ts, ts])


def mark_batch_reserved(conn, *, identity: BaselineIdentity,
                        batch_relation: str) -> None:
    """在批量关系上标记 reserved = 逐行 reserve_pending_slot 返回 True 的集合。"""
    conn.execute(
        f"UPDATE {batch_relation} AS c SET reserved = TRUE "
        "WHERE EXISTS (SELECT 1 FROM qfq_discovery_baseline b "
        "WHERE b.cutover_id = ? AND b.event_logical_key = c.event_logical_key "
        "AND b.pending_trigger_id IS NULL "
        "AND b.applied_payload_hash IS DISTINCT FROM c.payload_hash)",
        [identity.cutover_id])


def reserve_pending_slots_from_batch(conn, *, identity: BaselineIdentity,
                                     batch_relation: str,
                                     ts: Optional[str] = None) -> None:
    """集合等价于 reserve_pending_slot 的 UPDATE 命中分支：占槽（pending CAS）。"""
    ts = ts or now_ts()
    conn.execute(
        "UPDATE qfq_discovery_baseline AS b "
        "SET pending_trigger_id = c.trigger_id, pending_payload_hash = c.payload_hash, "
        f"updated_at = ? FROM {batch_relation} AS c "
        "WHERE b.cutover_id = ? AND b.event_logical_key = c.event_logical_key "
        "AND b.pending_trigger_id IS NULL "
        "AND b.applied_payload_hash IS DISTINCT FROM c.payload_hash",
        [ts, identity.cutover_id])


def mark_batch_pre_existing_triggers(conn, batch_relation: str) -> None:
    """标记 reserved 中 trigger_id 已在队列的行（= 逐行 INSERT OR IGNORE 冲突集）。

    **必须在 trigger 落队之前调用**：只有插入前已在队列中的 trigger_id 才算冲突，
    与逐行实现 `inserted is None` 的断言触发条件逐位对应。
    """
    conn.execute(
        f"UPDATE {batch_relation} AS c SET trigger_pre_existing = TRUE "
        "WHERE c.reserved AND EXISTS (SELECT 1 FROM qfq_trigger_queue t "
        "WHERE t.trigger_id = c.trigger_id)")


def assert_batch_pending_slots_match(conn, *, identity: BaselineIdentity,
                                     batch_relation: str) -> None:
    """批量版 assert_existing_trigger_matches_pending_slot（告警不得被批量吞掉）。

    逐行实现只在 `INSERT OR IGNORE` 冲突（trigger_id 已存在）时断言；此处对同一集合
    （reserved ∧ trigger_pre_existing）逐条等价校验，任一不一致即抛 DiscoveryBaselineError。
    """
    bad = conn.execute(
        "SELECT c.event_logical_key, b.pending_trigger_id, b.pending_payload_hash, "
        "b.price_source, b.source_generation "
        f"FROM {batch_relation} AS c JOIN qfq_discovery_baseline AS b "
        "ON b.cutover_id = ? AND b.event_logical_key = c.event_logical_key "
        "WHERE c.reserved AND c.trigger_pre_existing AND ("
        "b.pending_trigger_id IS DISTINCT FROM c.trigger_id "
        "OR b.pending_payload_hash IS DISTINCT FROM c.payload_hash "
        "OR b.price_source IS DISTINCT FROM ? "
        "OR b.source_generation IS DISTINCT FROM ?)",
        [identity.cutover_id, identity.price_source,
         identity.source_generation]).fetchall()
    if bad:
        raise DiscoveryBaselineError(
            f"批量 pending slot 与既有 trigger 不一致（{len(bad)} 行）: {bad[:5]!r}")


def assert_existing_trigger_matches_pending_slot(conn, *, identity: BaselineIdentity,
                                                  event_logical_key: str,
                                                  trigger_id: str,
                                                  payload_hash: str) -> None:
    row = conn.execute(
        "SELECT pending_trigger_id, pending_payload_hash, price_source, "
        "source_generation FROM qfq_discovery_baseline "
        "WHERE cutover_id=? AND event_logical_key=?",
        [identity.cutover_id, event_logical_key]).fetchone()
    if row is None or row[0] != trigger_id or row[1] != payload_hash \
            or row[2] != identity.price_source or row[3] != identity.source_generation:
        raise DiscoveryBaselineError(
            f"trigger={trigger_id} 与 baseline pending slot 不一致: {row!r}")


def commit_pending_slot(conn, *, identity: BaselineIdentity,
                        event_logical_key: str, trigger_id: str,
                        payload_hash: str) -> str:
    """Advance ``applied_payload_hash`` using trigger-bound CAS.

    Returns ``committed`` for a successful CAS or ``idempotent`` when the same
    payload was already applied.  An empty result with a different pending
    trigger is a hard invariant failure.
    """
    ts = now_ts()
    row = _fetchone_returning(
        conn,
        "UPDATE qfq_discovery_baseline SET applied_payload_hash=?, "
        "pending_trigger_id=NULL, pending_payload_hash=NULL, last_trigger_id=?, "
        "applied_at=?, updated_at=? "
        "WHERE cutover_id=? AND event_logical_key=? "
        "AND pending_trigger_id=? AND pending_payload_hash=? "
        "RETURNING cutover_id, event_logical_key",
        [payload_hash, trigger_id, ts, ts, identity.cutover_id, event_logical_key,
         trigger_id, payload_hash],
    )
    if row is not None:
        return "committed"
    cur = conn.execute(
        "SELECT applied_payload_hash, pending_trigger_id, pending_payload_hash "
        "FROM qfq_discovery_baseline WHERE cutover_id=? AND event_logical_key=?",
        [identity.cutover_id, event_logical_key]).fetchone()
    if cur and cur[0] == payload_hash and cur[1] is None:
        return "idempotent"
    raise DiscoveryBaselineError(
        f"baseline commit CAS 失败 key={event_logical_key!r} trigger={trigger_id!r}: {cur!r}")


def audit_pending_slots(conn, *, identity: Optional[BaselineIdentity] = None) -> dict:
    where = ""
    params = []
    if identity is not None:
        where = "WHERE b.cutover_id=? AND b.price_source=? AND b.source_generation=?"
        params = [identity.cutover_id, identity.price_source, identity.source_generation]
    orphan = conn.execute(
        "SELECT COUNT(*) FROM qfq_discovery_baseline b "
        f"{where}{' AND' if where else 'WHERE'} b.pending_trigger_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM qfq_trigger_queue t "
        "WHERE t.trigger_id=b.pending_trigger_id)", params).fetchone()[0]
    mismatch_gen = conn.execute(
        "SELECT COUNT(*) FROM qfq_discovery_baseline b "
        f"{where}{' AND' if where else 'WHERE'} b.pending_trigger_id IS NOT NULL "
        "AND EXISTS (SELECT 1 FROM qfq_trigger_queue t WHERE t.trigger_id=b.pending_trigger_id "
        "AND (t.price_source<>b.price_source OR t.source_generation<>b.source_generation "
        "OR t.cutover_id<>b.cutover_id))", params).fetchone()[0]
    mismatch_payload = conn.execute(
        "SELECT COUNT(*) FROM qfq_discovery_baseline b "
        f"{where}{' AND' if where else 'WHERE'} b.pending_trigger_id IS NOT NULL "
        "AND EXISTS (SELECT 1 FROM qfq_trigger_queue t WHERE t.trigger_id=b.pending_trigger_id "
        "AND t.payload_hash<>b.pending_payload_hash)", params).fetchone()[0]
    return {"orphan_pending": int(orphan), "generation_mismatch": int(mismatch_gen),
            "payload_mismatch": int(mismatch_payload),
            "passed": int(orphan) == int(mismatch_gen) == int(mismatch_payload) == 0}
