"""QFQ 常驻增量事件发现（resident orchestrator v2 —— 事件/因子发现层）

本模块只做**事件/因子发现**，不拉价格（价格由 qfq_fresh_capture 负责，源锁定 xtquant）。
它把"可能发生复权跳变"的语义事件落进 ``qfq_trigger_queue``（DuckDB 主库），供后续
重锚执行消费。全部写入幂等（确定性 trigger_id + INSERT OR IGNORE），崩溃可重放。

三类发现源：
1. ``stock_dividend`` 表（DuckDB 主库，writers 维护）—— 全表哈希扫描，防晚到修订漏检。
2. ``adj_factor`` 表（SQLite 辅助库 qfq_aux.db）—— 股票复权因子快照 → 版本化 observation。
3. ``fund_adj`` 表（SQLite 辅助库 qfq_aux.db）—— ETF 复权因子快照 → 版本化 observation。

observation 写入 ``qfq_factor_observation``（qfq_observation.ObservationStore 管理），
修订自动产生 ``qfq_factor_revision_alert`` outbox；本模块 ``consume_revision_alerts``
把 pending alert 幂等转成 DuckDB trigger。

契约依赖（单一真相源，禁止重写）：
- ``qfq_orchestrator_types``：trigger_id_of / payload_hash_of / TriggerRecord / 枚举 / QFQOrchestratorConfig
- ``qfq_observation``：ObservationStore / ObservationResult
- ``qfq_reanchor_schema``：DDL + init_duckdb_schema / init_sqlite_schema

本模块 import 不触发任何 xtquant 网络连接（lazy，仅在真正拉因子时由维护层负责）。
"""
from __future__ import annotations

from quantstudio.pipeline.snapshot_lock import locked_connect  # 3A 写锁收口

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

from quantstudio.pipeline.code_contract import validate_sec_code
from quantstudio.pipeline.qfq_orchestrator_types import (
    QFQOrchestratorConfig,
    TriggerRecord,
    payload_hash_of,
    trigger_id_of,
    trigger_id_v2,
)
from quantstudio.pipeline.qfq_observation import ObservationResult, ObservationStore
from quantstudio.pipeline.qfq_reanchor_schema import init_sqlite_schema
# v2.4 B-1：分红 payload hash 单一真相源（中立模块，防 scan/establish 漂移 + 防循环 import）
from quantstudio.pipeline.qfq_dividend_payload import dividend_payload_hash, norm_div_val
from quantstudio.pipeline.qfq_discovery_baseline import (
    BaselineIdentity, logical_key_stock_dividend, reserve_pending_slot,
    assert_existing_trigger_matches_pending_slot,
    # A 件批量变体（集合 CAS；逐行 API 保留，供 §6 回退路径与既有调用方使用）
    assert_batch_pending_slots_match, ensure_baseline_rows_for_batch,
    mark_batch_pre_existing_triggers, mark_batch_reserved,
    reserve_pending_slots_from_batch,
)

logger = logging.getLogger(__name__)

# 北京时区（与 qfq_observation / qfq_revision 一致）
BJ_TZ = timezone(timedelta(hours=8))

# qfq_aux.db 因子快照表（与 qfq_maintenance.py 口径对齐：code 裸码，time 毫秒时间戳，
# adj_factor 因子值）。股票读 adj_factor；ETF 读 fund_adj（本模块预期存在的独立表）。
ADJ_FACTOR_TABLE = "adj_factor"
FUND_ADJ_TABLE = "fund_adj"


def _now_ts() -> str:
    """DuckDB TIMESTAMP 友好的北京墙钟字符串（无时区偏移，避免 TIMESTAMP 解析歧义）。

    统一口径：+08 墙钟，格式 ``YYYY-MM-DD HH:MM:SS``。
    """
    return datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _norm_div_val(v):
    """兼容别名（v2.4 B-1：生产路径已改用 ``qfq_dividend_payload.norm_div_val``）。

    保留此名仅为旧测试/外部导入兼容；指向中立模块的单一实现，**不**允许生产路径
    继续使用两套实现。迁移完成后可移除。
    """
    return norm_div_val(v)


class EventDiscovery:
    """QFQ 增量事件发现器。

    构造仅持有 cfg 与可选 aux_db 路径；不持有长连接（方法均接收外部 DuckDB ``conn``
    与 ``run_id``，观察类方法内部临时开 SQLite 连接并传 ``conn`` 给 ObservationStore）。

    Args:
        cfg: QFQOrchestratorConfig（仅需读 stock_factor_detector / etf_factor_detector / freqs）。
        aux_db: qfq_aux.db 路径（None → 由 ObservationStore 推导；测试传临时库）。
    """

    # A 件批量路径的临时关系名（每次扫描覆盖式重建，不落主库 DDL）
    _BATCH_RELATION = "_qfq_scan_dividend_batch"
    _CAND_DF_VIEW = "_qfq_scan_dividend_cand_df"

    def __init__(self, cfg: QFQOrchestratorConfig, aux_db: Optional[str] = None,
                 identity: Optional[dict] = None):
        self.cfg = cfg
        self.aux_db = aux_db
        if identity is None:
            from quantstudio.pipeline.qfq_schema_contracts import pre_cutover_qfq_identity
            self.identity = pre_cutover_qfq_identity(cfg.price_source)
        else:
            self.identity = dict(identity)
        self._obs_store: Optional[ObservationStore] = None

    def set_runtime(self, identity: dict, *, aux_db: Optional[str] = None) -> None:
        """Atomically switch the discovery identity and isolated auxiliary DB route."""
        self.identity = dict(identity)
        if aux_db is not None and str(aux_db) != str(self.aux_db):
            self.aux_db = str(aux_db)
            self._obs_store = None

    @property
    def obs_store(self) -> ObservationStore:
        if self._obs_store is None:
            self._obs_store = ObservationStore(self.aux_db)
        return self._obs_store

    # ------------------------------------------------------------------
    # 辅助：检测游标 upsert（DuckDB qfq_observation_cursor）
    # ------------------------------------------------------------------
    def _upsert_cursor(self, conn, detector_name: str, asset_type: str,
                       cursor_as_of: Optional[int], run_id: str,
                       status: str = "ok") -> None:
        _ident = self.identity
        now = _now_ts()
        # v2.4 B-3a.3 P0-2：统一静态 pre-cutover identity（price_source=真实值；
        # generation/cutover 固定 legacy 哨兵，不读 cfg.source_generation/cutover_id）。
        ident = self.identity
        conn.execute(
            "INSERT OR REPLACE INTO qfq_observation_cursor "
            "(detector_name, asset_type, price_source, source_generation, "
            " cursor_as_of, last_run_id, status, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [detector_name, asset_type, ident["price_source"], ident["source_generation"],
             cursor_as_of, run_id, status, now],
        )

    # ------------------------------------------------------------------
    # 1. stock_dividend 全表哈希扫描 → trigger（DuckDB 主库）
    # ------------------------------------------------------------------
    def scan_stock_dividend(self, conn, *, as_of_ms: int, run_id: str,
                            bootstrap: bool = False,
                            codes_filter: Optional[Sequence[str]] = None) -> List[TriggerRecord]:
        """扫描 stock_dividend（div_proc='实施' 且 ex_date 非空）生成 STOCK 分红 trigger。

        - 全表扫描（约 5 万行），不依赖日期 cursor —— 防晚到修订漏检。
        - 同语义事件用确定性 trigger_id → INSERT OR IGNORE 天然去重。
        - status：ex_date > as_of_ms → 'scheduled'（只调度不执行）；否则 'pending'。
        - bootstrap=True：不插入 trigger（避免全历史洪水），仅更新 observation cursor；
          返回空列表。
        - 无论 bootstrap 与否，都更新 qfq_observation_cursor(stock_dividend, STOCK)。

        Args:
            conn: 外部 DuckDB 连接（stock_dividend 与 qfq_trigger_queue 同库）。
            as_of_ms: 当前时刻（epoch-ms），用于划分 scheduled / pending。
            run_id: 本轮运行标识。
            bootstrap: 仅建 cursor 不插 trigger。
        Returns:
            本次**新插入**的 TriggerRecord 列表（已存在的不计入）。
        """
        # 任务4：动态探测列，兼容旧/新 schema（缺列按 NULL 处理，不阻断）
        present = {str(r[0]).lower() for r in conn.execute("DESCRIBE stock_dividend").fetchall()}
        if "ex_date" not in present:
            return []
        _ident = self.identity
        _wanted = ["code", "ex_date", "record_date", "ann_date", "end_date",
                   "cash_div_before_tax", "cash_div_after_tax", "cash_div",
                   "stk_div", "stk_bo_rate", "stk_co_rate", "div_rat", "div_proc"]
        _sel = []
        for _c in _wanted:
            if _c == "code":
                _sel.append("code")  # code 必然存在
            elif _c.lower() in present:
                _sel.append(_c)
            else:
                _sel.append(f"NULL AS {_c}")
        _where = "ex_date IS NOT NULL"
        if "div_proc" in present:
            _where = f"div_proc='实施' AND {_where}"
        _params: List[object] = []
        if codes_filter:
            placeholders = ", ".join(["?"] * len(codes_filter))
            _where += f" AND code IN ({placeholders})"
            _params.extend(codes_filter)
        rows = conn.execute(
            "SELECT {cols} FROM stock_dividend WHERE {where}".format(
                cols=", ".join(_sel), where=_where),
            _params,
        ).fetchall()

        new_records: List[TriggerRecord] = []
        now = _now_ts()
        # 全表最大 ex_date（bootstrap 与否都推进 cursor；与逐行实现同口径）
        ex_dates = [int(r[1]) for r in rows]
        max_ex_date: Optional[int] = max(ex_dates) if ex_dates else None

        if not bootstrap and rows:
            if _ident["source_generation"] == "xtquant-legacy":
                new_records = self._scan_dividend_legacy(conn, rows, as_of_ms, now)
            elif self._use_batch_scan():
                new_records = self._scan_dividend_v2_batch(conn, rows, as_of_ms, now)
            else:
                # §6 回退：置 QFQ_DIVIDEND_SCAN_BATCH=0 即切回逐行实现（默认走批量）
                new_records = self._scan_dividend_v2_rowwise(conn, rows, as_of_ms, now)
        # 无论 bootstrap 与否，更新检测游标（cursor_as_of = 最大 ex_date）
        self._upsert_cursor(
            conn, "stock_dividend", "STOCK", max_ex_date, run_id, status="ok")
        return new_records

    # ------------------------------------------------------------------
    # 1a. 逐行实现（§6 可切换回退路径；与 A 件修复前原实现逐行等价）
    # ------------------------------------------------------------------
    def _scan_dividend_v2_rowwise(self, conn, rows, as_of_ms: int,
                                  now: str) -> List[TriggerRecord]:
        """逐行显式事务实现（原 v2 分支原样保留，供 QFQ_DIVIDEND_SCAN_BATCH=0 回退）。"""
        _ident = self.identity
        new_records: List[TriggerRecord] = []
        for (code, ex_date, record_date, ann_date, end_date, cash_div_before_tax,
             cash_div_after_tax, cash_div, stk_div, stk_bo_rate, stk_co_rate,
             div_rat, div_proc) in rows:
            ex_date = int(ex_date)
            effective_date = ex_date
            payload_hash = dividend_payload_hash(
                code, ex_date, record_date, ann_date, end_date,
                cash_div_before_tax, cash_div_after_tax, cash_div,
                stk_div, stk_bo_rate, stk_co_rate, div_rat, div_proc)
            status = "scheduled" if ex_date > as_of_ms else "pending"
            trigger_id = trigger_id_v2(
                "STOCK", code, ex_date, "stock_dividend", payload_hash,
                _ident["price_source"], _ident["source_generation"])
            key = logical_key_stock_dividend(code, ex_date)
            conn.execute("BEGIN TRANSACTION")
            try:
                reserved = reserve_pending_slot(
                    conn, identity=BaselineIdentity(**_ident),
                    event_logical_key=key, trigger_id=trigger_id,
                    payload_hash=payload_hash)
                if reserved:
                    inserted = conn.execute(
                        "INSERT OR IGNORE INTO qfq_trigger_queue "
                        "(trigger_id, asset_type, code, trigger_type, detection_source, source_key, "
                        " effective_date, payload_hash, status, trigger_id_version, price_source, "
                        " source_generation, cutover_id, created_at, updated_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING trigger_id",
                        [trigger_id, "STOCK", code, "stock_dividend", "stock_dividend",
                         str(ex_date), effective_date, payload_hash, status, 2,
                         _ident["price_source"], _ident["source_generation"], _ident["cutover_id"],
                         now, now]).fetchone()
                    if inserted is None:
                        assert_existing_trigger_matches_pending_slot(
                            conn, identity=BaselineIdentity(**_ident),
                            event_logical_key=key, trigger_id=trigger_id,
                            payload_hash=payload_hash)
                    if inserted is not None:
                        new_records.append(TriggerRecord(
                            trigger_id=trigger_id, asset_type="STOCK", code=code,
                            trigger_type="stock_dividend", detection_source="stock_dividend",
                            source_key=str(ex_date), effective_date=effective_date,
                            payload_hash=payload_hash, status=status, trigger_id_version=2,
                            price_source=_ident["price_source"],
                            source_generation=_ident["source_generation"],
                            cutover_id=_ident["cutover_id"], created_at=now, updated_at=now))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return new_records

    # ------------------------------------------------------------------
    # 1b. 批量实现（A 件默认路径：O(N) 显式事务 → 常数批）
    # ------------------------------------------------------------------
    def _use_batch_scan(self) -> bool:
        """批量路径开关（默认开）。置 QFQ_DIVIDEND_SCAN_BATCH=0 即回退逐行实现。"""
        return os.environ.get("QFQ_DIVIDEND_SCAN_BATCH", "1") != "0"

    def _scan_dividend_v2_batch(self, conn, rows, as_of_ms: int,
                                now: str) -> List[TriggerRecord]:
        """集合 CAS + 单次 trigger 落队（语义与 _scan_dividend_v2_rowwise 一致）。

        仅有的实现差异（不影响生产成功路径）：
        - 所有行共享同一 ``now`` 作 pending 占槽的 ``updated_at``（逐行实现逐行取秒级 now_ts）；
        - 断言失败时整批回滚（逐行实现只回滚触发断言的那一行）——「不吞告警」语义不变。
        """
        _ident = self.identity
        # key 去重保首行：等价逐行实现的「先到先占」（重复 key 的第二行两分支均不改状态）
        cand: Dict[str, dict] = {}
        for r in rows:
            code = r[0]
            ex_date = int(r[1])
            key = logical_key_stock_dividend(code, ex_date)
            if key in cand:
                continue
            payload_hash = dividend_payload_hash(*r[:13])
            cand[key] = {
                "rn": len(cand), "key": key, "code": code, "ex_date": ex_date,
                "trigger_id": trigger_id_v2(
                    "STOCK", code, ex_date, "stock_dividend", payload_hash,
                    _ident["price_source"], _ident["source_generation"]),
                "payload_hash": payload_hash,
                "status": "scheduled" if ex_date > as_of_ms else "pending",
            }
        if not cand:
            return []

        import pandas as pd  # 惰性导入：仅批量路径需要（register DataFrame 供 DuckDB 零拷贝扫描）
        df = pd.DataFrame(list(cand.values()))
        ident = BaselineIdentity(**_ident)
        conn.register(self._CAND_DF_VIEW, df)
        try:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    "CREATE OR REPLACE TEMP TABLE " + self._BATCH_RELATION + " AS "
                    "SELECT CAST(rn AS BIGINT) AS rn, key AS event_logical_key, "
                    "CAST(code AS VARCHAR) AS code, CAST(ex_date AS BIGINT) AS ex_date, "
                    "CAST(trigger_id AS VARCHAR) AS trigger_id, "
                    "CAST(payload_hash AS VARCHAR) AS payload_hash, "
                    "CAST(status AS VARCHAR) AS status, "
                    "FALSE AS reserved, FALSE AS trigger_pre_existing "
                    f"FROM {self._CAND_DF_VIEW}")
                ensure_baseline_rows_for_batch(
                    conn, identity=ident, batch_relation=self._BATCH_RELATION, ts=now)
                mark_batch_reserved(
                    conn, identity=ident, batch_relation=self._BATCH_RELATION)
                reserve_pending_slots_from_batch(
                    conn, identity=ident, batch_relation=self._BATCH_RELATION, ts=now)
                mark_batch_pre_existing_triggers(conn, self._BATCH_RELATION)
                assert_batch_pending_slots_match(
                    conn, identity=ident, batch_relation=self._BATCH_RELATION)
                conn.execute(
                    "INSERT OR IGNORE INTO qfq_trigger_queue "
                    "(trigger_id, asset_type, code, trigger_type, detection_source, source_key, "
                    " effective_date, payload_hash, status, trigger_id_version, price_source, "
                    " source_generation, cutover_id, created_at, updated_at) "
                    "SELECT c.trigger_id, 'STOCK', c.code, 'stock_dividend', 'stock_dividend', "
                    "CAST(c.ex_date AS VARCHAR), c.ex_date, c.payload_hash, c.status, 2, ?, ?, ?, ?, ? "
                    f"FROM {self._BATCH_RELATION} AS c "
                    "WHERE c.reserved AND NOT c.trigger_pre_existing",
                    [_ident["price_source"], _ident["source_generation"],
                     _ident["cutover_id"], now, now])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            # new_records = reserved ∧ ¬pre_existing，按扫描序（= 逐行实现 append 顺序）
            fresh = conn.execute(
                "SELECT rn, trigger_id, code, ex_date, payload_hash, status "
                f"FROM {self._BATCH_RELATION} "
                "WHERE reserved AND NOT trigger_pre_existing ORDER BY rn").fetchall()
            conn.execute("DROP TABLE IF EXISTS " + self._BATCH_RELATION)
        finally:
            conn.unregister(self._CAND_DF_VIEW)

        return [
            TriggerRecord(
                trigger_id=tid, asset_type="STOCK", code=code,
                trigger_type="stock_dividend", detection_source="stock_dividend",
                source_key=str(ex_date), effective_date=int(ex_date),
                payload_hash=ph, status=st, trigger_id_version=2,
                price_source=_ident["price_source"],
                source_generation=_ident["source_generation"],
                cutover_id=_ident["cutover_id"], created_at=now, updated_at=now)
            for (_rn, tid, code, ex_date, ph, st) in fresh
        ]

    # ------------------------------------------------------------------
    # 1c. legacy 分支（xtquant-legacy identity；原样保留）
    # ------------------------------------------------------------------
    def _scan_dividend_legacy(self, conn, rows, as_of_ms: int,
                              now: str) -> List[TriggerRecord]:
        _ident = self.identity
        new_records: List[TriggerRecord] = []
        for (code, ex_date, record_date, ann_date, end_date, cash_div_before_tax,
             cash_div_after_tax, cash_div, stk_div, stk_bo_rate, stk_co_rate,
             div_rat, div_proc) in rows:
            ex_date = int(ex_date)
            effective_date = ex_date
            payload_hash = dividend_payload_hash(
                code, ex_date, record_date, ann_date, end_date,
                cash_div_before_tax, cash_div_after_tax, cash_div,
                stk_div, stk_bo_rate, stk_co_rate, div_rat, div_proc)
            status = "scheduled" if ex_date > as_of_ms else "pending"
            trigger_id = trigger_id_of(
                "STOCK", code, ex_date, "stock_dividend", payload_hash)
            existed = self._trigger_exists(conn, trigger_id)
            conn.execute(
                "INSERT OR IGNORE INTO qfq_trigger_queue "
                "(trigger_id, asset_type, code, trigger_type, detection_source, source_key, "
                " effective_date, payload_hash, status, trigger_id_version, price_source, "
                " source_generation, cutover_id, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [trigger_id, "STOCK", code, "stock_dividend", "stock_dividend",
                 str(ex_date), effective_date, payload_hash, status, 1,
                 _ident["price_source"], _ident["source_generation"], _ident["cutover_id"],
                 now, now])
            if not existed:
                new_records.append(TriggerRecord(
                    trigger_id=trigger_id, asset_type="STOCK", code=code,
                    trigger_type="stock_dividend", detection_source="stock_dividend",
                    source_key=str(ex_date), effective_date=effective_date,
                    payload_hash=payload_hash, status=status, trigger_id_version=1,
                    price_source=_ident["price_source"],
                    source_generation=_ident["source_generation"],
                    cutover_id=_ident["cutover_id"], created_at=now, updated_at=now))
        return new_records

    # ------------------------------------------------------------------
    # 2 & 3. 因子快照 → 版本化 observation（SQLite qfq_aux.db）
    # ------------------------------------------------------------------
    def _observe_factors(self, conn, *, as_of_ms: int, run_id: str,
                         bootstrap: bool, asset_type: str, table: str,
                         detector_name: str,
                         codes_filter: Optional[Sequence[str]] = None) -> ObservationResult:
        """读 qfq_aux.db 的因子快照表 → record_observations；更新观测游标。

        bootstrap 时仍记录 observation（首次 revision_no=1，不产生 alert，正好建立基线）。

        Args:
            conn: 外部 DuckDB 连接（仅用于更新 qfq_observation_cursor）。
            as_of_ms: 上界时刻（epoch-ms），future factor_time 计入 future_excluded。
            run_id: 本轮运行标识。
            bootstrap: 是否基线模式（此处观察行为不变，仅语义标记）。
            asset_type: 'STOCK' / 'ETF'。
            table: 读取的因子快照表名（adj_factor / fund_adj）。
            detector_name: 游标名（stock_adj_factor / etf_fund_adj）。
        Returns:
            ObservationResult。
        """
        _lc = locked_connect(lambda: sqlite3.connect(str(self.aux_db), timeout=30), "event_discovery:292")  # 3A 写锁
        aux_conn = _lc.__enter__()
        try:
            aux_conn.execute("PRAGMA journal_mode=WAL")
            aux_conn.execute("PRAGMA busy_timeout=30000")
            init_sqlite_schema(aux_conn)

            factor_sql = f"SELECT code, time, adj_factor FROM {table}"
            factor_params: List[object] = []
            if codes_filter:
                placeholders = ", ".join(["?"] * len(codes_filter))
                factor_sql += f" WHERE code IN ({placeholders})"
                factor_params.extend(codes_filter)
            factor_rows = aux_conn.execute(factor_sql, factor_params).fetchall()
            # T4 fail-safe（错误二，9-22 01:48 实录：aux 快照混入 FIXTEST →
            # record_observations 整批 ValueError → qfq_orch 全周期崩 → re-anchor 停摆）：
            # 快照行先过形式契约（code_contract），非契约行**源头 skip + 聚合可见**，
            # 不再打断周期。record_observations 的阻断2 整批契约保持不动（fail-safe
            # 在调用方；_normalize_code 本体保持严格）。
            observations = []
            _bad: List[str] = []
            for code, time, adj_factor in factor_rows:
                ok, cd, _reason = validate_sec_code(code)
                if not ok:
                    _bad.append(str(code))
                    continue
                observations.append((asset_type, cd, int(time), float(adj_factor)))
            if _bad:
                logger.warning(
                    f"[qfq_event] {table} code 契约预过滤 skip {len(_bad)} 行"
                    f"（样例 {_bad[:8]}），本周期不受影响；存量清理见错误二 T5")

            result = self.obs_store.record_observations(
                observations, run_id, as_of_ms=as_of_ms,
                source_generation=self.identity["source_generation"], conn=aux_conn)
            # 任务3：相邻 factor_time 值变化检测 → factor_new trigger（DuckDB）
            if getattr(result, "factor_new", None):
                self._emit_factor_new_triggers(conn, result.factor_new, run_id, as_of_ms)
            aux_conn.commit()
        finally:
            aux_conn.close()
            _lc.__exit__(None, None, None)  # 3A 写锁随连接释放

        # 局部代码扫描不能推进全局检测游标，否则后续全量周期可能漏检。
        if not codes_filter:
            self._upsert_cursor(conn, detector_name, asset_type, as_of_ms, run_id,
                                status="ok")
        return result

    # ------------------------------------------------------------------
    # 3b. 相邻 factor_time 值变化 → factor_new trigger（DuckDB，幂等）
    # ------------------------------------------------------------------
    def _trigger_exists(self, conn, trigger_id: str) -> bool:
        ident = self.identity
        return conn.execute(
            "SELECT 1 FROM qfq_trigger_queue WHERE trigger_id=? AND price_source=? "
            "AND source_generation=? AND cutover_id=?",
            [trigger_id, ident["price_source"], ident["source_generation"],
             ident["cutover_id"]]).fetchone() is not None

    # Factor events intentionally do not use qfq_discovery_baseline: their baseline
    # is the generation-isolated SQLite observation ledger. trigger_id_v2 plus the
    # physically isolated outbox provides idempotence without sharing dividend CAS slots.
    def _emit_factor_new_triggers(self, conn, factor_new_list, run_id, as_of_ms):
        """任务3：把 record_observations 收集的 factor_new 候选幂等落地为 qfq_trigger_queue。

        detection_source = tushare_adj_factor_new（股票）/ tushare_fund_adj_new（ETF）；
        trigger_type = factor_new；trigger_id 确定性；future factor_time → scheduled。
        """
        now = _now_ts()
        _ident = self.identity
        for fn in factor_new_list:
            ds = "tushare_fund_adj_new" if fn.asset_type == "ETF" else "tushare_adj_factor_new"
            payload_hash = payload_hash_of(
                [fn.code, fn.factor_time, fn.previous_value, fn.current_value])
            trigger_id = (trigger_id_v2(fn.asset_type, fn.code, fn.factor_time, ds,
                                         payload_hash, _ident["price_source"],
                                         _ident["source_generation"])
                          if _ident["source_generation"] != "xtquant-legacy"
                          else trigger_id_of(fn.asset_type, fn.code, fn.factor_time, ds,
                                             payload_hash))
            status = "scheduled" if fn.factor_time > as_of_ms else "pending"
            existed = self._trigger_exists(conn, trigger_id)
            conn.execute(
                "INSERT OR IGNORE INTO qfq_trigger_queue "
                "(trigger_id, asset_type, code, trigger_type, detection_source, source_key, "
                 " effective_date, payload_hash, factor_old, factor_new, factor_revision, "
                 " status, trigger_id_version, price_source, source_generation, "
                 " cutover_id, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [trigger_id, fn.asset_type, fn.code, "factor_new", ds,
                 str(fn.factor_time), fn.factor_time, payload_hash,
                 fn.previous_value, fn.current_value, None, status,
                     (2 if _ident["source_generation"] != "xtquant-legacy" else 1),
                     _ident["price_source"], _ident["source_generation"], _ident["cutover_id"],
                 now, now])
            if not existed:
                logger.info(
                    f"[qfq_event] factor_new trigger {trigger_id} "
                    f"({fn.asset_type} {fn.code} @ {fn.factor_time})")

    def observe_stock_adj_factor(self, conn, *, as_of_ms: int, run_id: str,
                                 bootstrap: bool = False,
                                 codes_filter: Optional[Sequence[str]] = None) -> ObservationResult:
        """股票 adj_factor 快照 → 版本化 observation。"""
        return self._observe_factors(
            conn, as_of_ms=as_of_ms, run_id=run_id, bootstrap=bootstrap,
            asset_type="STOCK", table=ADJ_FACTOR_TABLE,
            detector_name="stock_adj_factor", codes_filter=codes_filter)

    def observe_etf_fund_adj(self, conn, *, as_of_ms: int, run_id: str,
                             bootstrap: bool = False,
                             codes_filter: Optional[Sequence[str]] = None) -> ObservationResult:
        """ETF fund_adj 快照 → 版本化 observation。"""
        return self._observe_factors(
            conn, as_of_ms=as_of_ms, run_id=run_id, bootstrap=bootstrap,
            asset_type="ETF", table=FUND_ADJ_TABLE,
            detector_name="etf_fund_adj", codes_filter=codes_filter)

    # ------------------------------------------------------------------
    # 4. 消费 revision alert outbox → DuckDB trigger（幂等）
    # ------------------------------------------------------------------
    def consume_revision_alerts(self, conn, *, run_id: str,
                                as_of_ms: int,
                                codes_filter: Optional[Sequence[str]] = None) -> List[TriggerRecord]:
        """把 pending revision alert 幂等转成 DuckDB trigger。

        顺序（崩溃可重放）：先幂等落 DuckDB trigger，再 acknowledge_alert（SQLite）。
        若中间崩溃未 ack，下一轮重新消费：INSERT OR IGNORE 不重复生成，然后完成 ack。

        Returns:
            本次**新插入**的 TriggerRecord 列表（已存在的 trigger 不计入）。
        """
        aux_conn = sqlite3.connect(str(self.aux_db), timeout=30)
        new_records: List[TriggerRecord] = []
        _ident = self.identity
        try:
            aux_conn.execute("PRAGMA journal_mode=WAL")
            aux_conn.execute("PRAGMA busy_timeout=30000")
            init_sqlite_schema(aux_conn)

            alerts = self.obs_store.list_pending_alerts(conn=aux_conn)
            allowed_codes = set(codes_filter or ())
            for alert in alerts:
                asset_type = alert["asset_type"]
                code = alert["code"]
                if allowed_codes and code not in allowed_codes:
                    continue
                factor_time = int(alert["factor_time"])
                revision_no = int(alert["revision_no"])

                # 取该 (asset_type, code, factor_time, revision_no) 的 factor_value 作 new
                new_row = aux_conn.execute(
                    "SELECT factor_value FROM qfq_factor_observation "
                    "WHERE asset_type=? AND code=? AND factor_time=? AND revision_no=?",
                    [asset_type, code, factor_time, revision_no]).fetchone()
                if new_row is None:
                    logger.warning(
                        f"[qfq_event] alert {alert['alert_id']} 缺 observation 行，跳过")
                    continue
                new_factor_value = float(new_row[0])

                # old：revision_no-1 行（若有）
                old_factor: Optional[float] = None
                if revision_no > 1:
                    old_row = aux_conn.execute(
                        "SELECT factor_value FROM qfq_factor_observation "
                        "WHERE asset_type=? AND code=? AND factor_time=? AND revision_no=?",
                        [asset_type, code, factor_time, revision_no - 1]).fetchone()
                    if old_row is not None:
                        old_factor = float(old_row[0])

                if asset_type == "ETF":
                    detection_source = "tushare_fund_adj"
                    trigger_type = "etf_fund_adj"
                else:
                    detection_source = "tushare_adj_factor"
                    trigger_type = "factor_new" if revision_no == 1 else "factor_revision"

                payload_hash = payload_hash_of([new_factor_value])
                now = _now_ts()
                trigger_id = (trigger_id_v2(
                    asset_type, code, factor_time, detection_source, payload_hash,
                    _ident["price_source"], _ident["source_generation"])
                    if _ident["source_generation"] != "xtquant-legacy"
                    else trigger_id_of(asset_type, code, factor_time,
                                       detection_source, payload_hash))
                # 先幂等落 DuckDB trigger
                existed = self._trigger_exists(conn, trigger_id)
                conn.execute(
                    "INSERT OR IGNORE INTO qfq_trigger_queue "
                    "(trigger_id, asset_type, code, trigger_type, detection_source, source_key, "
                    " effective_date, payload_hash, factor_old, factor_new, factor_revision, "
                    " status, trigger_id_version, price_source, source_generation, "
                    " cutover_id, created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [trigger_id, asset_type, code, trigger_type, detection_source,
                     str(factor_time), factor_time, payload_hash, old_factor,
                     new_factor_value, revision_no, "pending",
                     (2 if _ident["source_generation"] != "xtquant-legacy" else 1),
                     _ident["price_source"], _ident["source_generation"], _ident["cutover_id"],
                     now, now])
                if not existed:
                    new_records.append(TriggerRecord(
                        trigger_id=trigger_id, asset_type=asset_type, code=code,
                        trigger_type=trigger_type, detection_source=detection_source,
                        source_key=str(factor_time), effective_date=factor_time,
                        payload_hash=payload_hash, factor_old=old_factor,
                        factor_new=new_factor_value, factor_revision=revision_no,
                        status="pending", trigger_id_version=(
                            2 if _ident["source_generation"] != "xtquant-legacy" else 1),
                        price_source=_ident["price_source"],
                        source_generation=_ident["source_generation"],
                        cutover_id=_ident["cutover_id"],
                        created_at=now, updated_at=now,
                    ))

                # 再 ack（崩溃可重放：上一步已落地，ack 失败下一轮重放不重复）
                self.obs_store.acknowledge_alert(alert["alert_id"], conn=aux_conn)

            aux_conn.commit()
        finally:
            aux_conn.close()

        return new_records

    # ------------------------------------------------------------------
    # 5. 建立基线（不灌历史 trigger，只建因子观察基线 + 分红 cursor）
    # ------------------------------------------------------------------
    def establish_baseline(self, conn, *, as_of_ms: int,
                           run_id: str) -> Dict[str, int]:
        """bootstrap 建立因子观察基线 + stock_dividend cursor（不向 trigger_queue 灌历史）。

        Returns:
            {"stock_adj_factor": <new_count>, "etf_fund_adj": <new_count>,
             "stock_dividend_cursor": <max ex_date or 0>}
        """
        res_stock = self.observe_stock_adj_factor(
            conn, as_of_ms=as_of_ms, run_id=run_id, bootstrap=True)
        res_etf = self.observe_etf_fund_adj(
            conn, as_of_ms=as_of_ms, run_id=run_id, bootstrap=True)
        self.scan_stock_dividend(
            conn, as_of_ms=as_of_ms, run_id=run_id, bootstrap=True)

        row = conn.execute(
            "SELECT cursor_as_of FROM qfq_observation_cursor "
            "WHERE detector_name='stock_dividend' AND asset_type='STOCK' "
            "AND price_source=? AND source_generation=?",
            [self.identity["price_source"], self.identity["source_generation"]]
        ).fetchone()
        cursor_as_of = int(row[0]) if row and row[0] is not None else 0

        return {
            "stock_adj_factor": res_stock.new_count,
            "etf_fund_adj": res_etf.new_count,
            "stock_dividend_cursor": cursor_as_of,
        }
