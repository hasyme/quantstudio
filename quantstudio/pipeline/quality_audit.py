"""Canonical 数据库的配置驱动质量审计。"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional


@dataclass
class QualityIssue:
    check: str
    table: str
    count: int
    severity: str
    detail: str = ""


@dataclass
class QualityReport:
    issues: List[QualityIssue] = field(default_factory=list)
    checks_run: int = 0
    # 可中断审计（daemon-interruptible-quality-audit-design.md）：中断时
    # interrupted=True；skipped_after_stop = 因停请求被跳过的**顶层段**数
    # （检查点序号差口径，见 DataQualityAuditor._checkpoint）。
    # 两字段默认值保证既有构造点/消费点逐位不变。
    interrupted: bool = False
    skipped_after_stop: int = 0

    @property
    def passed(self) -> bool:
        return not any(issue.severity == "error" and issue.count > 0 for issue in self.issues)


class AuditInterrupted(Exception):
    """审计检查点因停请求中止（私有类型：仅 run() 顶层捕获，不外泄）。

    真实失败（连接/查询错误等）仍按原路径冒泡，不被误吞。
    """

    def __init__(self, checkpoint: str):
        super().__init__(checkpoint)
        self.checkpoint = checkpoint


class DataQualityAuditor:
    PRICE_TABLES = ("stock_daily", "stock_minutes", "etf_daily", "etf_minutes")
    MINUTE_TABLES = ("stock_minutes", "etf_minutes")
    FREQ_MS = {"1min": 60_000, "5min": 300_000, "15min": 900_000,
               "30min": 1_800_000, "60min": 3_600_000}

    def __init__(self, db_path: str | Path, schemas: Dict,
                 batch_audit_path: Optional[str | Path] = None,
                 quarantine_path: Optional[str | Path] = None,
                 shared_conn=None,
                 authority_rules: Optional[Dict] = None,
                 qfq_thresholds: Optional[Dict] = None,
                  qfq_identity: Optional[Dict] = None,
                 qfq_aux_override: Optional[str | Path] = None,
                 qfq_aux_paths_config: Optional[str | Path] = None,
                 should_stop: Optional[Callable[[], bool]] = None):
        """shared_conn: 可选，外部传入的持久 read_write 连接（采集流程内复用 writer 连接，
        避免开 read_only 与 write 并发触发「different configuration」冲突）。
        不传则自开 read_only 短连接（CLI/独立运行场景）。
        authority_rules: 可选，来源权威性规则，格式：
        {table: {authoritative_source: str, allow_fallback: bool}}。
        qfq_thresholds: 可选，QFQ 编排专项门控阈值（collector_tasks.json 的
        qfq_orchestrator.quality_thresholds 块）。**None（默认）= 完全跳过 QFQ
        专项审计**（编排器 disabled 时保持旧行为，不因历史 qfq 表残留新增失败）；
        非 None 时启用 dead_letter / pending SLA / stale in_progress /
        残留 pending watermark intent 四项检查。
        should_stop: 可选，返回 True = 已收到停止请求 → 审计在下一个检查点中止
        （已执行项结论保留、未执行项计入 skipped_after_stop）。**缺省 None ⇒ 零
        开销且行为逐位不变**（CLI / 测试 / 未接入的调用点）。

        注意：判据由调用方提供，**只读调用方已缓存的 stop 标志**；本模块不读、
        也不得读 daemon_stop.request（该文件在收尾前已被消费删除）。"""
        self.db_path = Path(db_path)
        self.schemas = schemas
        self.batch_audit_path = Path(batch_audit_path) if batch_audit_path else None
        self.quarantine_path = Path(quarantine_path) if quarantine_path else None
        self._shared_conn = shared_conn
        self._authority_rules = authority_rules
        self._qfq_thresholds = qfq_thresholds
        self._qfq_identity = dict(qfq_identity) if qfq_identity else None
        # A1/A2 因子巡检（mcp-minute-front-anchor-design.md §4 阶段1）：
        # qfq_aux_override 显式注入（测试/hermetic 用）；qfq_aux_paths_config 为
        # qfq_aux_paths.json 路径，缺省 None → resolve_runtime_aux_path fail-secure
        # legacy（aux_db_path(main_db)）。因子取数必须跟随运行时路由（ZCode 执行
        # 注记 2：G2b 释放后路由切 gen1，巡检与重锚不得使用不同因子源）。
        self.qfq_aux_override = Path(qfq_aux_override) if qfq_aux_override else None
        self.qfq_aux_paths_config = Path(qfq_aux_paths_config) if qfq_aux_paths_config else None
        self._price_source = str((self._qfq_identity or {}).get("price_source", "mcp"))
        self._should_stop = should_stop
        # 检查点序号差口径的计数器（run() 内按实际表数/条件段重置；直调
        # _audit_* 且注入 should_stop 时不致 AttributeError）
        self._ckpt_top_total = 0
        self._ckpt_top_done = 0

    @classmethod
    def from_config(cls, db_path: str | Path, rules_path: str | Path,
                    batch_audit_path: Optional[str | Path] = None,
                    quarantine_path: Optional[str | Path] = None):
        rules = json.loads(Path(rules_path).read_text(encoding="utf-8"))
        return cls(db_path, rules["schemas"], batch_audit_path, quarantine_path)

    def _checkpoint(self, report: QualityReport, item: str, top: bool = False) -> None:
        """审计检查点：停请求已发出则中止本轮审计。

        - should_stop=None ⇒ 立即返回（零开销、行为逐位不变）。
        - top=True：计入**顶层段**序号（表边界 / 全局段边界），用于
          `skipped_after_stop = 顶层段总数 - 已完成顶层段数 + 1`（含当前段）。
        - 中止方式：置 report 标记后抛私有 AuditInterrupted，由 run() 顶层捕获
          ——不入库、不写盘，也不吞掉任何真实异常。
        """
        if self._should_stop is None:
            return
        if top:
            self._ckpt_top_done += 1
        if self._should_stop():
            report.interrupted = True
            report.skipped_after_stop = max(
                1, self._ckpt_top_total - self._ckpt_top_done + 1)
            raise AuditInterrupted(item)

    def run(self) -> QualityReport:
        import duckdb
        report = QualityReport()
        # 复用外部传入的 shared_conn（采集流程内，避免 read_only 与 write 并发配置冲突）；
        # 否则自开 read_only 短连接（CLI/独立运行）。
        own_conn = None
        conn = self._shared_conn
        if conn is None:
            own_conn = duckdb.connect(str(self.db_path), read_only=True)
            conn = own_conn
        self._ckpt_top_done = 0
        self._ckpt_top_total = 0
        try:
            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            # 顶层段总数（检查点序号差口径的分母）= 表循环 N 段
            # + 4 个无条件全局段（分钟锚点漂移 / 因子单调性 / batch_pipeline /
            #   quarantine）+ 2 个条件段（source_watermark / QFQ 编排）。
            self._ckpt_top_total = (len(self.schemas) + 4
                                    + (1 if "source_watermark" in tables else 0)
                                    + (1 if self._qfq_thresholds is not None else 0))
            for table, schema in self.schemas.items():
                self._checkpoint(report, f"table:{table}", top=True)
                if table not in tables:
                    self._add(report, "TableMissing", table, 1, "error", "schema 已定义但数据库无表")
                    continue
                columns = {row[0] for row in conn.execute(f'DESCRIBE "{table}"').fetchall()}
                for col, spec in schema.get("columns", {}).items():
                    if col not in columns:
                        severity = "error" if spec.get("required") else "warning"
                        self._add(report, "SchemaColumnMissing", table, 1, severity, col)
                        continue
                    if spec.get("required"):
                        count = conn.execute(
                            f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" IS NULL').fetchone()[0]
                        self._add(report, "RequiredValueNull", table, count, "error", col)
                    self._audit_schema_constraint(conn, report, table, col, spec)
                pk = [col for col in schema.get("primary_key", []) if col in columns]
                if pk:
                    keys = ",".join(f'"{col}"' for col in pk)
                    count = conn.execute(
                        f'SELECT COUNT(*) FROM (SELECT {keys}, COUNT(*) c FROM "{table}" '
                        f'GROUP BY {keys} HAVING COUNT(*)>1)').fetchone()[0]
                    self._add(report, "PrimaryKeyDuplicate", table, count, "error", ",".join(pk))
                if table in self.PRICE_TABLES:
                    self._audit_prices(conn, report, table, columns)
                if table in self.MINUTE_TABLES:
                    self._audit_frequency(conn, report, table, columns)
                self._audit_future_and_pit(conn, report, table, columns)
                if "data_source" in columns:
                    # SourceTraceability severity: elevated to error when authority rules
                    # require an authoritative source without fallback.
                    source_trace_severity = "warning"
                    if self._authority_rules and table in self._authority_rules:
                        rule = self._authority_rules[table]
                        if not rule.get("allow_fallback", True):
                            source_trace_severity = "error"
                    missing_source = conn.execute(
                        f'SELECT COUNT(*) FROM "{table}" WHERE data_source IS NULL').fetchone()[0]
                    self._add(report, "SourceTraceability", table, missing_source,
                              source_trace_severity)
                    # Authority source check: fully derived from authority_rules
                    if self._authority_rules and table in self._authority_rules:
                        rule = self._authority_rules[table]
                        authority = rule["authoritative_source"]
                        allow_fallback = rule.get("allow_fallback", True)
                        non_authority = conn.execute(
                            f"SELECT COUNT(*) FROM \"{table}\" "
                            f"WHERE data_source IS NOT NULL AND data_source != '{authority}'"
                        ).fetchone()[0]
                        severity = "error" if not allow_fallback else "warning"
                        self._add(report, "AuthoritySourceViolation", table, non_authority, severity,
                                  f"non-{authority} rows in authority-locked table")
                # Growth field coverage check (fin_indicator)
                if table == "fin_indicator" and {"np_yoy", "or_yoy"} <= columns:
                    tushare_rows = conn.execute(
                        "SELECT COUNT(*) FROM fin_indicator WHERE data_source='tushare'"
                    ).fetchone()[0]
                    if tushare_rows > 0:
                        np_null = conn.execute(
                            "SELECT COUNT(*) FROM fin_indicator "
                            "WHERE data_source='tushare' AND np_yoy IS NULL"
                        ).fetchone()[0]
                        or_null = conn.execute(
                            "SELECT COUNT(*) FROM fin_indicator "
                            "WHERE data_source='tushare' AND or_yoy IS NULL"
                        ).fetchone()[0]
                        if np_null == tushare_rows:
                            self._add(report, "GrowthFieldAllNull", table, tushare_rows, "error",
                                      "np_yoy is 100% NULL for tushare-sourced data")
                        if or_null == tushare_rows:
                            self._add(report, "GrowthFieldAllNull", table, tushare_rows, "error",
                                      "or_yoy is 100% NULL for tushare-sourced data")
                # P-A3：同源复制列跨表一致性门禁（eps ← income_statement.basic_eps）。
                # gap>0 = 回补规则未生效/漏跑/源端 schema 变化（新缺口入库未免疫）→ error。
                # income_statement 表缺失时跳过（不引用不存在表）。
                if table == "fin_indicator" and "eps" in columns and "income_statement" in tables:
                    from quantstudio.pipeline.eps_backfill import check_eps_backfill_gap
                    gap = check_eps_backfill_gap(conn)
                    if gap > 0:
                        self._add(report, "EpsBackfillGap", table, gap, "error",
                                  "eps NULL 但 income_statement 同 key basic_eps 非空（回补未生效或源 schema 变化）")
                # Dividend field validation (stock_dividend)
                if table == "stock_dividend" and {"cash_div_before_tax", "cash_div_after_tax"} <= columns:
                    # Both columns populated is normal (Tushare provides both pre-tax and post-tax)
                    # Check: pre-tax >= post-tax (with tolerance)
                    cross_check = conn.execute(
                        "SELECT COUNT(*) FROM stock_dividend "
                        "WHERE cash_div_before_tax IS NOT NULL AND cash_div_after_tax IS NOT NULL "
                        "AND cash_div_before_tax < cash_div_after_tax - 0.001"
                    ).fetchone()[0]
                    if cross_check > 0:
                        self._add(report, "DividendTaxInversion", table, cross_check, "error",
                                  "cash_div_after_tax > cash_div_before_tax (tax inversion)")
                    # Check non-negative
                    neg_before = conn.execute(
                        "SELECT COUNT(*) FROM stock_dividend WHERE cash_div_before_tax < 0"
                    ).fetchone()[0]
                    neg_after = conn.execute(
                        "SELECT COUNT(*) FROM stock_dividend WHERE cash_div_after_tax < 0"
                    ).fetchone()[0]
                    if neg_before:
                        self._add(report, "DividendNegative", table, neg_before, "error",
                                  "cash_div_before_tax is negative")
                    if neg_after:
                        self._add(report, "DividendNegative", table, neg_after, "error",
                                  "cash_div_after_tax is negative")
                if table == "stock_dividend" and "div_proc" in columns:
                    non_implemented = conn.execute(
                        "SELECT COUNT(*) FROM stock_dividend "
                        "WHERE div_proc IS NOT NULL AND div_proc != '实施'"
                    ).fetchone()[0]
                    if non_implemented > 0:
                        self._add(report, "DividendNonImplemented", table, non_implemented, "error",
                                  "non-implemented dividend records exist")
            if "source_watermark" in tables:
                self._checkpoint(report, "segment:watermarks", top=True)
                self._audit_watermarks(conn, report, tables)
            if self._qfq_thresholds is not None:
                self._checkpoint(report, "segment:qfq_orchestration", top=True)
                self._audit_qfq_orchestration(conn, report, tables)
            # A1：分钟 front 锚点漂移巡检（mcp-minute-front-anchor-design.md §4 阶段1）
            self._checkpoint(report, "segment:minute_anchor_drift", top=True)
            self._audit_minute_anchor_drift(conn, report, tables)
            # A2：因子序列非单调告警（同上）
            self._checkpoint(report, "segment:factor_monotonicity", top=True)
            self._audit_factor_monotonicity(conn, report)
            # A3（缺陷1 方案 A′ §5.3）：云端口径漂移门禁
            self._audit_caliber_drift(conn, report, tables)
            self._checkpoint(report, "segment:batch_pipeline", top=True)
            self._audit_batch_pipeline(report)
            self._checkpoint(report, "segment:quarantine", top=True)
            self._audit_quarantine(report)
        except AuditInterrupted:
            # 停请求在检查点生效：本轮审计未跑完。已执行项结论保留在 report，
            # 未执行项由 skipped_after_stop 如实标记（绝不伪造「已通过」）。
            pass
        finally:
            if own_conn is not None:
                own_conn.close()
        return report



    def _audit_batch_pipeline(self, report):
        """Audit stage-count conservation in the collector's batch ledger."""
        if not self.batch_audit_path or not self.batch_audit_path.exists():
            return
        import sqlite3
        with sqlite3.connect(self.batch_audit_path) as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(batch_audit)").fetchall()}
            if not cols:
                self._add(report, "BatchAuditMissing", "__pipeline__", 1, "error")
                return
            has_fixed = "rows_fixed" in cols
            fixed_expr = "rows_fixed" if has_fixed else "NULL AS rows_fixed"
            rows = conn.execute(
                "SELECT batch_id,rows_raw,rows_aligned,rows_passed,rows_rejected,"
                f"rows_written,{fixed_expr},status,finished_at FROM batch_audit "
                "ORDER BY finished_at DESC LIMIT 500").fetchall()
        conservation = sum(
            1 for _, _, aligned, passed, rejected, _, fixed, status, _ in rows
            if fixed is not None and status in {"success", "empty"}
            and int(aligned or 0) != int(passed or 0) + int(rejected or 0) + int(fixed or 0))
        self._add(report, "StageCountConservation", "__pipeline__",
                  conservation, "error", "aligned must equal passed + rejected + fixed")
        over_written = sum(
            1 for _, _, _, passed, _, written, _, status, _ in rows
            if status == "success" and int(written or 0) > int(passed or 0))
        self._add(report, "WriteCountConservation", "__pipeline__",
                  over_written, "error", "written cannot exceed passed")

        from datetime import datetime, timedelta
        cutoff = datetime.now() - timedelta(hours=24)
        recent_failed = 0
        for row in rows:
            status, finished = row[7], row[8]
            if status != "failed" or not finished:
                continue
            try:
                if datetime.fromisoformat(str(finished)) >= cutoff:
                    recent_failed += 1
            except ValueError:
                recent_failed += 1
        self._add(report, "RecentBatchFailure", "__pipeline__",
                  recent_failed, "warning", "failed collector batches in the last 24h")

        if self.quarantine_path and self.quarantine_path.exists():
            with sqlite3.connect(self.quarantine_path) as conn:
                q_counts = dict(conn.execute(
                    "SELECT batch_id,COUNT(*) FROM quarantine GROUP BY batch_id").fetchall())
            uncovered = sum(
                1 for batch_id, _, _, _, rejected, _, _, status, _ in rows
                if status == "success" and int(rejected or 0) > 0
                and int(q_counts.get(batch_id, 0)) < int(rejected or 0))
            self._add(report, "RejectedRowConservation", "__pipeline__",
                      uncovered, "warning", "rejected rows should be traceable in quarantine")

    def _audit_quarantine(self, report):
        """Expose quarantine backlog and malformed records in the same report."""
        if not self.quarantine_path or not self.quarantine_path.exists():
            return
        import sqlite3
        with sqlite3.connect(self.quarantine_path) as conn:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "quarantine" not in tables:
                self._add(report, "QuarantineTableMissing", "__pipeline__", 1, "error")
                return
            malformed = conn.execute(
                "SELECT COUNT(*) FROM quarantine WHERE batch_id IS NULL OR table_name IS NULL "
                "OR source IS NULL OR original_payload IS NULL OR failed_rules IS NULL").fetchone()[0]
            pending = conn.execute(
                "SELECT COUNT(*) FROM quarantine WHERE status='pending_repair'").fetchone()[0]
        self._add(report, "QuarantineRecordIntegrity", "__pipeline__",
                  malformed, "error")
        self._add(report, "QuarantineBacklog", "__pipeline__",
                  pending, "warning", "pending_repair rows require review or replay")

    def _audit_schema_constraint(self, conn, report, table, col, spec):
        """Mirror schema-driven pre-ingest constraints on persisted rows."""
        self._checkpoint(report, f"constraint:{table}.{col}")
        qcol = f'"{col}"'
        if spec.get("regex"):
            pattern = str(spec["regex"]).replace("'", "''")
            sql = (f'SELECT COUNT(*) FROM "{table}" WHERE {qcol} IS NOT NULL '
                   f"AND NOT regexp_matches(CAST({qcol} AS VARCHAR), '{pattern}')")
            count = conn.execute(sql).fetchone()[0]
            self._add(report, "CodeFormat", table, count, "error", col)
        if spec.get("enum"):
            enum_values = list(spec["enum"])
            placeholders = ",".join("?" for _ in enum_values)
            sql = (f'SELECT COUNT(*) FROM "{table}" WHERE {qcol} IS NOT NULL '
                   f'AND {qcol} NOT IN ({placeholders})')
            count = conn.execute(sql, enum_values).fetchone()[0]
            self._add(report, "EnumCheck", table, count, "error", col)
        if "gt" in spec:
            count = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE {qcol} IS NOT NULL AND {qcol}<=?',
                [spec["gt"]]).fetchone()[0]
            self._add(report, "GreaterThan", table, count, "error", f"{col}>{spec['gt']}")
        if "ge" in spec:
            count = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE {qcol} IS NOT NULL AND {qcol}<?',
                [spec["ge"]]).fetchone()[0]
            self._add(report, "GreaterEqual", table, count, "error", f"{col}>={spec['ge']}")

    def _audit_prices(self, conn, report, table, columns):
        self._checkpoint(report, f"prices:{table}")
        raw = {"open", "high", "low", "close"}
        if raw.issubset(columns):
            bad = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE open<=0 OR high<=0 OR low<=0 OR close<=0 '
                'OR high<GREATEST(open,low,close) OR low>LEAST(open,high,close)').fetchone()[0]
            self._add(report, "RawPriceOHLC", table, bad, "error")
        if {"amount", "volume", "close"}.issubset(columns):
            neg = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE volume<0 OR amount<0').fetchone()[0]
            self._add(report, "VolumeAmountNegative", table, neg, "error")
            unit = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE volume>0 AND close>0 AND amount>0 '
                'AND (amount/(close*volume)<0.5 OR amount/(close*volume)>2.0)').fetchone()[0]
            self._add(report, "UnitConsistency", table, unit, "warning")
        for side in ("front", "back"):
            self._checkpoint(report, f"prices:{table}/{side}")
            adjusted = [f"{name}_{side}" for name in ("open", "high", "low", "close")]
            if not set(adjusted).issubset(columns):
                continue
            null_sum = "+".join(f'CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END' for col in adjusted)
            partial = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE ({null_sum}) BETWEEN 1 AND 3').fetchone()[0]
            self._add(report, "AdjustmentCompleteness", table, partial, "error", side)
            q = (f'SELECT COUNT(*) FROM "{table}" WHERE "open_{side}" IS NOT NULL AND '
                 f'("open_{side}"<=0 OR "high_{side}"<=0 OR "low_{side}"<=0 OR "close_{side}"<=0 '
                 f'OR "high_{side}"<GREATEST("open_{side}","low_{side}","close_{side}") '
                 f'OR "low_{side}">LEAST("open_{side}","high_{side}","close_{side}"))')
            self._add(report, "AdjustmentOHLC", table, conn.execute(q).fetchone()[0], "error", side)
            if raw.issubset(columns):
                # AdjustmentFactorConsistency：同一根 K 线 OHLC 的复权因子应一致
                #（防止 front 因子错套到 back 列等严重错配）。
                # 阈值按数据源复权算法区分（2026-07-22 修复 xtquant 分钟 back 误报）：
                # - tushare/baostock/akshare：日级单一 adj_factor，OHLC 共用因子，严格 2%
                # - xtquant back（分钟）：逐 tick 累积后复权，同根 K 线 open/high/low/close
                #   来自不同时刻 tick，因子有 2-4% 微差（算法固有，非数据错误），放宽 5%
                #   实测 xtquant back 偏差全部 <4%（419/1877万 = 0.002%），>5% 仍是真错配。
                #   xtquant front 以最新日为基准，无此问题，保持 2%。
                has_source = "data_source" in columns
                if has_source:
                    # 分源统计：xtquant 用 5%，其余 2%
                    q_xt = (f'SELECT COUNT(*) FROM "{table}" WHERE "open_{side}" IS NOT NULL '
                            'AND open>0 AND high>0 AND low>0 AND close>0 '
                            "AND data_source='xtquant' AND "
                            f'(ABS(("open_{side}"/open)/("close_{side}"/close)-1)>0.05 OR '
                            f'ABS(("high_{side}"/high)/("close_{side}"/close)-1)>0.05 OR '
                            f'ABS(("low_{side}"/low)/("close_{side}"/close)-1)>0.05)')
                    q_other = (f'SELECT COUNT(*) FROM "{table}" WHERE "open_{side}" IS NOT NULL '
                               'AND open>0 AND high>0 AND low>0 AND close>0 '
                               "AND (data_source IS NULL OR data_source!='xtquant') AND "
                               f'(ABS(("open_{side}"/open)/("close_{side}"/close)-1)>0.02 OR '
                               f'ABS(("high_{side}"/high)/("close_{side}"/close)-1)>0.02 OR '
                               f'ABS(("low_{side}"/low)/("close_{side}"/close)-1)>0.02)')
                    bad_xt = conn.execute(q_xt).fetchone()[0]
                    bad_other = conn.execute(q_other).fetchone()[0]
                    # xtquant back 5% 超标是算法固有微差边缘 case（validator 已接受），
                    # 降为 warning 不阻断任务；非 xtquant 2% 超标是真错配保持 error。
                    if bad_xt > 0:
                        self._add(report, "AdjustmentFactorConsistency", table, bad_xt,
                                   "warning", f"xtquant/{side}")
                    if bad_other > 0:
                        self._add(report, "AdjustmentFactorConsistency", table, bad_other,
                                   "error", side)
                else:
                    q = (f'SELECT COUNT(*) FROM "{table}" WHERE "open_{side}" IS NOT NULL '
                         'AND open>0 AND high>0 AND low>0 AND close>0 AND '
                         f'(ABS(("open_{side}"/open)/("close_{side}"/close)-1)>0.02 OR '
                         f'ABS(("high_{side}"/high)/("close_{side}"/close)-1)>0.02 OR '
                         f'ABS(("low_{side}"/low)/("close_{side}"/close)-1)>0.02)')
                    bad = conn.execute(q).fetchone()[0]
                    self._add(report, "AdjustmentFactorConsistency", table, bad, "error", side)
            if "data_source" in columns:
                missing = conn.execute(
                    f'SELECT COUNT(*) FROM "{table}" WHERE data_source=\'tushare\' '
                    f'AND "close_{side}" IS NULL').fetchone()[0]
                self._add(report, "AdjustmentCoverage", table, missing, "error", f"tushare/{side}")
        if {"code", "time", "close", "close_front", "close_back"}.issubset(columns):
            # 最贵单语句（分钟表全表双 ROW_NUMBER）——须前置可退（设计 §2.3）
            self._checkpoint(report, f"anchor:{table}")
            anchor = conn.execute(
                f'''WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY code ORDER BY time) first_n,
                              ROW_NUMBER() OVER(PARTITION BY code ORDER BY time DESC) last_n
                    FROM "{table}" WHERE close>0)
                    SELECT COUNT(*) FROM ranked WHERE
                    (last_n=1 AND close_front IS NOT NULL AND ABS(close_front/close-1)>0.02)
                    OR (first_n=1 AND close_back IS NOT NULL AND ABS(close_back/close-1)>0.02)''').fetchone()[0]
            self._add(report, "AdjustmentAnchor", table, anchor, "warning",
                      "部分数据源复权基准不保证库内首尾=原价")
        if {"code", "time", "close_front", "pctChg"}.issubset(columns) and table.endswith("daily"):
            self._checkpoint(report, f"return_consistency:{table}")
            continuity = conn.execute(
                f'''WITH x AS (
                    SELECT code,time,close_front,pctChg,
                           LAG(close_front) OVER(PARTITION BY code ORDER BY time) prev_front
                    FROM "{table}")
                    SELECT COUNT(*) FROM x WHERE prev_front>0 AND pctChg IS NOT NULL
                    AND ABS((close_front/prev_front-1)*100-pctChg)>1.0''').fetchone()[0]
            self._add(report, "AdjustmentReturnConsistency", table, continuity, "warning",
                      "需结合源官方收益口径复核")

    def _audit_frequency(self, conn, report, table, columns):
        self._checkpoint(report, f"frequency:{table}")
        if not {"time", "freq"}.issubset(columns):
            self._add(report, "FrequencyColumns", table, 1, "error")
            return
        allowed = ",".join(f"'{freq}'" for freq in self.FREQ_MS)
        invalid = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE freq IS NULL OR freq NOT IN ({allowed})').fetchone()[0]
        self._add(report, "FrequencyValue", table, invalid, "error")
        condition = " OR ".join(f"(freq='{freq}' AND time%{ms}<>0)" for freq, ms in self.FREQ_MS.items())
        grid = conn.execute(f'SELECT COUNT(*) FROM "{table}" WHERE {condition}').fetchone()[0]
        self._add(report, "FrequencyGrid", table, grid, "error")

    def _audit_future_and_pit(self, conn, report, table, columns):
        self._checkpoint(report, f"future_pit:{table}")
        import time
        now_ms = int(time.time() * 1000)
        for col in ("time", "ann_date", "end_date", "ex_date", "change_date", "delist_date"):
            if col in columns:
                count = conn.execute(
                    f'SELECT COUNT(*) FROM "{table}" WHERE "{col}">{now_ms + 7*86_400_000}').fetchone()[0]
                # 已公告的未来除权日/退市日属于计划事件，告警即可；行情/财报未来日期必须拦截。
                severity = "warning" if col in {"ex_date", "delist_date"} else "error"
                self._add(report, "FutureTimestamp", table, count, severity, col)
        if {"ann_date", "end_date"}.issubset(columns) and table in {
                "balance_statement", "income_statement", "cashflow_statement", "fin_indicator"}:
            count = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE ann_date<end_date').fetchone()[0]
            self._add(report, "PitAnnDate", table, count, "error")

    def _audit_watermarks(self, conn, report, tables):
        rows = conn.execute(
            "SELECT source,table_name,freq,last_date FROM source_watermark").fetchall()
        for source, table, freq, watermark in rows:
            if table not in tables or watermark is None:
                continue
            columns = {row[0] for row in conn.execute(f'DESCRIBE "{table}"').fetchall()}
            # 仅 schema 显式声明 time_key 的表做「水位 vs 表内业务日期」一致性校验，
            # 与 daemon._get_safe_watermark 的「无 time_key 就不校验表内日期」口径一致。
            # 无 time_key 的表（含快照表）水位语义非「数据业务日期」，本校验不适用——
            # 旧版此处 fallback 到 delist_date 等候选列，会把快照表的退市日期误当业务日期，
            # 导致 WatermarkConsistency 误报（stock_basic 复现，2026-10-01）。
            schema_time_key = self.schemas.get(table, {}).get("time_key") if self.schemas else None
            if not schema_time_key or schema_time_key not in columns:
                continue
            time_col = schema_time_key
            where = []
            if "data_source" in columns:
                where.append(f"data_source='{str(source).replace(chr(39), chr(39)*2)}'")
            if "freq" in columns:
                where.append(f"freq='{str(freq).replace(chr(39), chr(39)*2)}'")
            clause = " WHERE " + " AND ".join(where) if where else ""
            maximum = conn.execute(
                f'SELECT MAX("{time_col}") FROM "{table}"{clause}').fetchone()[0]
            if maximum is not None and int(maximum) != int(watermark):
                severity = "error" if int(watermark) > int(maximum) else "warning"
                self._add(report, "WatermarkConsistency", table, 1, severity,
                          f"{source}/{freq}: watermark={watermark}, max={maximum}")

    def _audit_qfq_orchestration(self, conn, report, tables):
        """Run QFQ health checks scoped to one runtime generation when supplied."""
        thr = self._qfq_thresholds or {}
        dl_max = int(thr.get("dead_letter_max", 0))
        sla_h = int(thr.get("pending_sla_hours", 72))
        stale_h = int(thr.get("stale_in_progress_hours", 24))
        ident = self._qfq_identity
        if ident:
            trig_scope = "price_source=? AND source_generation=? AND cutover_id=?"
            trig_params = [ident["price_source"], ident["source_generation"], ident["cutover_id"]]
            intent_scope = "source_generation=? AND cutover_id=?"
            intent_params = [ident["source_generation"], ident["cutover_id"]]
        else:
            trig_scope, trig_params = "1=1", []
            intent_scope, intent_params = "1=1", []
        if "qfq_trigger_queue" in tables:
            dl = conn.execute(
                f"SELECT COUNT(*) FROM qfq_trigger_queue WHERE status='dead_letter' AND {trig_scope}",
                trig_params).fetchone()[0]
            if dl > dl_max:
                self._add(report, "QfqDeadLetter", "qfq_trigger_queue", dl, "error",
                          f"dead_letter={dl} exceeds {dl_max}; reopen or investigate")
            overdue = conn.execute(
                f"SELECT COUNT(*) FROM qfq_trigger_queue WHERE status IN ('pending','retryable_failed') "
                f"AND COALESCE(updated_at, created_at) < NOW() - INTERVAL {sla_h} HOUR "
                f"AND {trig_scope}", trig_params).fetchone()[0]
            self._add(report, "QfqPendingSla", "qfq_trigger_queue", overdue, "error",
                      f"pending/retryable_failed older than {sla_h}h SLA")
            stale_ip = conn.execute(
                f"SELECT COUNT(*) FROM qfq_trigger_queue WHERE status='in_progress' "
                f"AND (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL {stale_h} HOUR) "
                f"AND {trig_scope}", trig_params).fetchone()[0]
            self._add(report, "QfqStaleInProgress", "qfq_trigger_queue", stale_ip, "error",
                      f"in_progress older than {stale_h}h; recovery did not clear it")
        if "qfq_discovery_baseline" in tables and "qfq_trigger_queue" in tables and ident:
            b_scope = "b.cutover_id=? AND b.price_source=? AND b.source_generation=?"
            b_params = [ident["cutover_id"], ident["price_source"], ident["source_generation"]]
            orphan = conn.execute(
                "SELECT COUNT(*) FROM qfq_discovery_baseline b WHERE " + b_scope +
                " AND b.pending_trigger_id IS NOT NULL AND NOT EXISTS "
                "(SELECT 1 FROM qfq_trigger_queue t WHERE t.trigger_id=b.pending_trigger_id)", b_params).fetchone()[0]
            mismatch = conn.execute(
                "SELECT COUNT(*) FROM qfq_discovery_baseline b WHERE " + b_scope +
                " AND b.pending_trigger_id IS NOT NULL AND EXISTS "
                "(SELECT 1 FROM qfq_trigger_queue t WHERE t.trigger_id=b.pending_trigger_id "
                "AND (t.price_source<>b.price_source OR t.source_generation<>b.source_generation "
                "OR t.cutover_id<>b.cutover_id))", b_params).fetchone()[0]
            payload = conn.execute(
                "SELECT COUNT(*) FROM qfq_discovery_baseline b WHERE " + b_scope +
                " AND b.pending_trigger_id IS NOT NULL AND EXISTS "
                "(SELECT 1 FROM qfq_trigger_queue t WHERE t.trigger_id=b.pending_trigger_id "
                "AND t.payload_hash<>b.pending_payload_hash)", b_params).fetchone()[0]
            for check, value, detail in (("QfqBaselineOrphanPending", orphan, "pending slot has no trigger"),
                                         ("QfqBaselineGenerationMismatch", mismatch, "pending slot crosses generation"),
                                         ("QfqBaselinePayloadMismatch", payload, "pending payload differs from trigger")):
                self._add(report, check, "qfq_discovery_baseline", value, "error", detail)
        if "qfq_watermark_intent" in tables and "qfq_cycle_run" in tables:
            stale_pending = conn.execute(
                "SELECT COUNT(*) FROM qfq_watermark_intent wi "
                "LEFT JOIN qfq_cycle_run cr ON cr.cycle_id=wi.cycle_id "
                "WHERE wi.status='pending' AND " + intent_scope +
                " AND (cr.cycle_id IS NULL OR cr.status IN "
                "('finalized','finalized_held','failed','interrupted'))", intent_params).fetchone()[0]
            self._add(report, "QfqStaleWatermarkIntent", "qfq_watermark_intent",
                      stale_pending, "warning",
                      "terminal cycle has stale pending intent")
            held = conn.execute(
                "SELECT COUNT(*) FROM qfq_watermark_intent WHERE status='held' AND " + intent_scope,
                intent_params).fetchone()[0]
            self._add(report, "QfqWatermarkHeld", "qfq_watermark_intent", held,
                      "warning", "watermark held by hold_until_consistent gate")

    # ===========================================================================
    # A1 / A2：分钟 front 锚点漂移 + 因子非单调巡检
    # （docs/mcp-minute-front-anchor-design.md §4 阶段1；实测依据
    #   docs/mcp-minute-caliber-audit-20260816.md §3.5/§3.6/§5 V2/V4）
    # ===========================================================================
    # 判别公式（V2 实测成立）：front = raw × adj_i / adj_latest
    #   actual = close_front/close；expect = adj_i/adj_latest
    #   dev = |actual/expect - 1|；>0.3% WARN；>0.5% FAIL（R6 阈值）
    _ANCHOR_DRIFT_WARN = 0.003
    _ANCHOR_DRIFT_FAIL = 0.005
    # A1 除权候选窗口：除权日 ∈ [now-120d, now+7d]（覆盖 ETF/股票分红季）
    _DIV_WINDOW_BACK_DAYS = 120
    _DIV_WINDOW_FWD_DAYS = 7
    # 因子值变化点采样：变化点前 90 天内、每日 14:59-15:01 的收盘 bar
    # （time 为 epoch 毫秒，%86400000 得 UTC 时刻；15:00 CST = 07:00 UTC）
    _DRIFT_LOOKBACK_DAYS = 90
    _BAR_CLOSE_MS_LO = 6 * 3600_000 + 59 * 60_000      # 06:59 UTC（14:59 CST）
    _BAR_CLOSE_MS_HI = 7 * 3600_000 + 1 * 60_000       # 07:01 UTC（15:01 CST）
    _DAILY_BAR_MS = 16 * 3600_000                      # 00:00 CST（日线 bar 时刻约定）

    # A3（方案 A′ §5.3）口径漂移门禁参数（未决项 §7-3 「实施时定」）：
    _CALIBER_MIN_DIV_GAP = 0.02        # 除权日落差 <2% 的探针不可分辨 → 弃用
    _CALIBER_DECISIVE_MARGIN = 0.5     # 实测比值须落在某假设 0.5×gap 容差内才计票
    _CALIBER_MIN_PROBES = 20           # 少于此探针数 → 样本不足告警（口径未决）
    _CALIBER_MAJORITY = 0.5            # 表级多数票须 >50% 方定口径
    # §5.3 阻断开关：stock_minutes 口径翻转是否阻断。默认关（False）——取证证明
    # 修复前「缺陷1 污染」与「云端口径漂移」在本地表不可区分，阻断会常亮误报。
    # 数据修复验收通过后置 True 恢复阻断语义。
    # 见 docs/evidence/caliber-drift-gate-forensics-20260928.md §4。
    _CALIBER_BLOCK_STOCK_MINUTES = False

    def _resolve_aux_path(self, conn) -> Optional[Path]:
        """解析因子库路径（跟随运行时路由，ZCode 执行注记 2）。

        override（测试注入）优先；否则 resolve_runtime_aux_path（双条件：
        released=true + active cutover → gen1；否则 fail-secure legacy）。
        """
        if self.qfq_aux_override is not None:
            return self.qfq_aux_override if self.qfq_aux_override.is_file() else None
        try:
            from quantstudio.pipeline.qfq_aux_router import resolve_runtime_aux_path
            path, _reason = resolve_runtime_aux_path(
                main_db=str(self.db_path), duckdb_read=conn.execute,
                price_source=self._price_source,
                config_path=self.qfq_aux_paths_config)
            return path if path.is_file() else None
        except Exception:
            return None

    def _read_factor_table(self, aux_path: Path, factor_tbl: str,
                           codes: list) -> Optional[list]:
        """从 aux 因子库读 (code, time, adj_factor) 三元组（仅候选 code）。"""
        import sqlite3
        if not codes:
            return None
        try:
            conn = sqlite3.connect(str(aux_path), timeout=30)
            try:
                conn.execute("PRAGMA query_only=ON")
                rows = conn.execute(
                    f"SELECT code, time, adj_factor FROM {factor_tbl} "
                    f"WHERE code IN ({','.join('?' * len(codes))})",
                    list(codes)).fetchall()
            finally:
                conn.close()
            return rows
        except sqlite3.Error:
            return None

    def _audit_minute_anchor_drift(self, conn, report, tables):
        """A1：除权候选标的的分钟 front 锚点漂移检测（AdjustmentAnchorDrift）。

        候选 = 除权表（stock_dividend/etf_dividend）ex_date ∈ [now-120d, now+7d]；
        判别 bar = 因子值变化点前 90 天内每日 14:59-15:01 收盘 bar（采样）；
        每 code 取最大偏差：>0.5% → error；0.3%-0.5% → warning（R6）。
        """
        if "stock_minutes" not in tables and "etf_minutes" not in tables:
            return
        aux_path = self._resolve_aux_path(conn)
        if aux_path is None:
            self._add(report, "AnchorDriftAuxUnavailable", "__qfq__", 1, "warning",
                      "因子库不可用（override 缺失或路由解析失败），A1 锚点漂移巡检跳过")
            return
        import pandas as pd
        from datetime import datetime, timedelta
        now = datetime.now()
        lo = int((now - timedelta(days=self._DIV_WINDOW_BACK_DAYS)).timestamp() * 1000)
        hi = int((now + timedelta(days=self._DIV_WINDOW_FWD_DAYS)).timestamp() * 1000)
        for minute_tbl, div_tbl, factor_tbl in (
                ("stock_minutes", "stock_dividend", "adj_factor"),
                ("etf_minutes", "etf_dividend", "fund_adj")):
            if minute_tbl not in tables or div_tbl not in tables:
                continue
            try:
                codes = [str(r[0]) for r in conn.execute(
                    f"SELECT DISTINCT code FROM {div_tbl} "
                    "WHERE ex_date BETWEEN ? AND ?", [lo, hi]).fetchall()]
            except Exception:
                continue  # 除权表结构异常 → 跳过（不阻断其余审计）
            if not codes:
                continue
            factors = self._read_factor_table(aux_path, factor_tbl, codes)
            if not factors:
                self._add(report, "AnchorDriftFactorMissing", factor_tbl, len(codes),
                          "warning", f"候选 {len(codes)} code 无因子数据")
                continue
            fdf = pd.DataFrame(factors, columns=["code", "time", "adj_factor"])
            fdf["time"] = pd.to_numeric(fdf["time"], errors="coerce")
            fdf["adj_factor"] = pd.to_numeric(fdf["adj_factor"], errors="coerce")
            fdf = fdf.dropna(subset=["time", "adj_factor"])
            if fdf.empty:
                continue
            # 因子序列预处理：同值段合并 + 污染尖刺剔除
            # （实测：因子表存在世代切换污染尖刺，如 2026-07-01 批量归 1.0、次日恢复
            #   ——merge_asof 取到污染行会使 A1 判别失真，先剔除再判别）
            fdf = self._clean_factor_segments(fdf)
            # 每 code 因子值变化点（相邻值不同；分钟冗余同值行自然合并）
            changes = []
            for code, g in fdf.sort_values("time").groupby("code"):
                vals = g["adj_factor"].to_numpy()
                ts = g["time"].to_numpy()
                if len(vals) < 2:
                    continue
                diff = vals[1:] != vals[:-1]
                for t in ts[1:][diff]:
                    changes.append((str(code), int(t)))
            if not changes:
                continue  # 无因子变化（从未除权）→ 无漂移可能
            # 判别窗口：所有变化点前 90 天 → 最晚变化点
            w_lo = min(int(t) for _, t in changes) - self._DRIFT_LOOKBACK_DAYS * 86400_000
            w_hi = max(int(t) for _, t in changes)
            placeholders = ",".join("?" * len(codes))
            bars = conn.execute(
                f"SELECT code, time, close, close_front FROM {minute_tbl} "
                f"WHERE code IN ({placeholders}) AND time >= ? AND time < ? "
                f"AND (time % 86400000) BETWEEN ? AND ? AND close > 0 "
                f"AND close_front IS NOT NULL",
                codes + [w_lo, w_hi, self._BAR_CLOSE_MS_LO, self._BAR_CLOSE_MS_HI]).fetchall()
            if not bars:
                continue
            bdf = pd.DataFrame(bars, columns=["code", "time", "close", "close_front"])
            bdf["time"] = pd.to_numeric(bdf["time"], errors="coerce")
            bdf["close"] = pd.to_numeric(bdf["close"], errors="coerce")
            bdf["close_front"] = pd.to_numeric(bdf["close_front"], errors="coerce")
            bdf = bdf.dropna(subset=["time", "close", "close_front"])
            if bdf.empty:
                continue
            # 因子最新值（每 code 最大 time）与 merge_asof 逐 bar 因子
            latest = fdf.sort_values("time").groupby("code").tail(1).set_index("code")["adj_factor"]
            fdf_s = fdf.sort_values(["code", "time"])
            merged = pd.merge_asof(
                bdf.sort_values("time"), fdf_s.sort_values("time"),
                on="time", by="code", direction="backward")
            merged = merged.dropna(subset=["adj_factor"])
            if merged.empty:
                continue
            adj_latest = merged["code"].map(latest)
            expect = merged["adj_factor"] / adj_latest
            actual = merged["close_front"] / merged["close"]
            merged["dev"] = (actual / expect - 1).abs()
            per_code = merged.groupby("code")["dev"].max()
            fails = per_code[per_code > self._ANCHOR_DRIFT_FAIL]
            warns = per_code[(per_code > self._ANCHOR_DRIFT_WARN)
                             & (per_code <= self._ANCHOR_DRIFT_FAIL)]
            if len(fails):
                self._add(report, "AdjustmentAnchorDrift", minute_tbl, len(fails), "error",
                          f"FAIL>0.5%: {sorted(fails.index)[:10]} (max={per_code.max():.4f})")
            if len(warns):
                self._add(report, "AdjustmentAnchorDrift", minute_tbl, len(warns), "warning",
                          f"WARN 0.3-0.5%: {sorted(warns.index)[:10]}")

            # === 补漏检（缺陷1，2026-09-27）：minutes.close 对日线 close 独立校验 ===
            # A1 只验 close_front/close 的「互自洽」，两列同向偏移（如 600649 落库 close=57.13
            # vs 真值 3.50，+1532%）时比值仍对 → 漏检。此处直接比对 minutes.close 对日线 close，
            # 覆盖同向偏移。候选与采样 bar 口径与 A1 一致（除权候选 × 14:59~15:01 收盘 bar）。
            import datetime as _dt
            _CST = _dt.timezone(_dt.timedelta(hours=8))
            _day_s = (pd.to_datetime(merged["time"], unit="ms")
                      .dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
                      .dt.strftime("%Y-%m-%d"))
            merged["_day"] = _day_s
            _daily_tbl = "etf_daily" if minute_tbl == "etf_minutes" else "stock_daily"
            try:
                _daily_rows = conn.execute(
                    f"SELECT code, time, close FROM {_daily_tbl} "
                    f"WHERE code IN ({placeholders}) AND time >= ? AND time < ?",
                    codes + [w_lo - 86_400_000, w_hi + 86_400_000]).fetchall()
            except Exception:
                continue
            if _daily_rows:
                _ddf = pd.DataFrame(_daily_rows, columns=["code", "time", "close"])
                _ddf["_day"] = (pd.to_datetime(_ddf["time"], unit="ms")
                                .dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
                                .dt.strftime("%Y-%m-%d"))
                _ddf = _ddf.rename(columns={"close": "daily_close"})
                _m2 = merged.merge(_ddf[["code", "_day", "daily_close"]],
                                   on=["code", "_day"], how="left")
                _m2 = _m2.dropna(subset=["daily_close"])
                if len(_m2):
                    _m2["raw_dev"] = (_m2["close"] / _m2["daily_close"] - 1).abs()
                    _per_raw = _m2.groupby("code")["raw_dev"].max()
                    _raw_fails = _per_raw[_per_raw > self._ANCHOR_DRIFT_FAIL]
                    _raw_warns = _per_raw[(_per_raw > self._ANCHOR_DRIFT_WARN)
                                          & (_per_raw <= self._ANCHOR_DRIFT_FAIL)]
                    if len(_raw_fails):
                        self._add(report, "MinuteRawVsDaily", minute_tbl, len(_raw_fails),
                                  "error",
                                  f"FAIL>0.5%: {sorted(_raw_fails.index)[:10]} "
                                  f"(max={_per_raw.max():.4f})")
                    if len(_raw_warns):
                        self._add(report, "MinuteRawVsDaily", minute_tbl, len(_raw_warns),
                                  "warning",
                                  f"WARN 0.3-0.5%: {sorted(_raw_warns.index)[:10]}")

    def _audit_caliber_drift(self, conn, report, tables):
        """A3（缺陷1 方案 A′ §5.3）：云端口径漂移门禁（CaliberDrift）。

        背景：A′ 是**表级硬编码**口径方案（`_QFQ_CALIBER_TABLES = {stock_daily,
        etf_daily, etf_minutes}` → 还原 raw；`stock_minutes` → 不还原）。口径一旦
        漂移即**静默写错**（无异常无日志），故设本门禁做回归探测。

        > 口径矩阵（2026-09-27）**已由金标准方法再确认正确**（2026-09-30）：
        > 用 `amount/volume` 反推真实成交价（**与复权无关**）复核——
        > `stock_daily` err_qfq 比 err_raw 小 32×、`etf_daily` 小 75× → 均 qfq；
        > `stock_minutes` `close/(amount/volume)=1.0000` → raw。
        > 2026-09-30 曾有一次基于「云端 stock_minutes 作基准」的收缩复核，
        > 因**同型循环论证**（该表 raw 判定本身来自原矩阵）而错误，已回退。
        > 详见 `docs/evidence/caliber-recheck-gold-standard-20260930.md`。
        > 门禁**逻辑零改动**——四张本地表**还原后均期望 raw**，故期望矩阵不变。

        判别原理（N1 取证法推广，与 A1 同为「外参比」而非「自洽」）：
        除权日 D 处，令 k = adj_D/adj_{D-1} > 1（来自 qfq_aux.db 因子表，**独立于
        被测输出**），实测本表 close 的跨除权日比值 r = close_D/close_{D-1}：
          · r ≈ 1    → 输出为 qfq（未还原）
          · r ≈ 1/k  → 输出为 raw（已还原，A′ 预期态）
          · r ≈ 1/k² → **双重复权**（云端已 raw 而管线又还原一次）
        取「k 步长足够大」（1-1/k ≥ 2%）的探针，避免真实涨跌幅淹没信号；单探针须
        落在某假设的 0.5×gap 容差内才计票，再按**表级多数票**定口径（聚合抗噪，
        与取证 107/107 同法）。

        判级（§5.3）：
          · `stock_minutes` 口径翻转（raw→qfq）→ **error/阻断**（N5）——
            A′ 对该表不还原，云端若改返 qfq 将直接写入未还原值 = 缺陷1 回归；
          · 日线表 / `etf_minutes` 口径不符 → **warning**（影响面为还原值偏差，
            由 §5.2 结果导向一致率兜底）。

        局限（如实声明）：参照系为 qfq_aux.db 因子表——本门禁探测**云端口径漂移**，
        不覆盖因子表自身的世代污染（另有 FactorMonotonicity / A1 覆盖）。
        """
        import pandas as pd

        if "stock_minutes" not in tables and "stock_daily" not in tables \
                and "etf_daily" not in tables and "etf_minutes" not in tables:
            return
        aux_path = self._resolve_aux_path(conn)
        if aux_path is None:
            self._add(report, "CaliberDriftAuxUnavailable", "__qfq__", 1, "warning",
                      "因子库不可用（override 缺失或路由解析失败），口径漂移门禁跳过")
            return

        from datetime import datetime, timedelta
        now = datetime.now()
        lo = int((now - timedelta(days=self._DIV_WINDOW_BACK_DAYS)).timestamp() * 1000)
        hi = int((now + timedelta(days=self._DIV_WINDOW_FWD_DAYS)).timestamp() * 1000)

        for tbl, div_tbl, factor_tbl, is_blocking in (
                ("stock_minutes", "stock_dividend", "adj_factor", True),
                ("stock_daily", "stock_dividend", "adj_factor", False),
                ("etf_daily", "etf_dividend", "fund_adj", False),
                ("etf_minutes", "etf_dividend", "fund_adj", False)):
            if tbl not in tables or div_tbl not in tables:
                continue
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT code, ex_date FROM {div_tbl} "
                    "WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",
                    [lo, hi]).fetchall()
            except Exception:
                continue  # 除权表结构异常 → 跳过（不阻断其余审计）
            if not rows:
                continue
            ex_by_code: dict = {}
            for c, d in rows:
                try:
                    ex_by_code.setdefault(str(c), set()).add(int(d))
                except (TypeError, ValueError):
                    continue
            if not ex_by_code:
                continue
            codes = sorted(ex_by_code)
            factors = self._read_factor_table(aux_path, factor_tbl, codes)
            if not factors:
                self._add(report, "CaliberDriftFactorMissing", factor_tbl, len(codes),
                          "warning", f"探针候选 {len(codes)} code 无因子数据")
                continue

            votes = {"qfq": 0, "raw": 0, "double": 0}
            unclassified = 0
            probes = 0
            samples = 0
            specs: list = []
            offenders = {"qfq": [], "double": []}

            # 因子按 code 建索引一次（原按 code 线性扫全表为 O(n·m)，大表下不可接受）
            fby: dict = {}
            for c, t, v in factors:
                if v is None:
                    continue
                try:
                    fby.setdefault(str(c), []).append((int(t), float(v)))
                except (TypeError, ValueError):
                    continue
            for seq in fby.values():
                seq.sort()

            for code, ex_days in ex_by_code.items():
                f = fby.get(code)
                if not f or len(f) < 2:
                    continue
                for ex_ms in ex_days:
                    # 因子步进**恰在除权日**（实测：600812 ex=2026-06-11 step@2026-06-11
                    # 4.0577→4.0856；600602 ex=2026-07-28 step@2026-07-28）。
                    # 故分割点必须取 ex_ms 本身：before = 严格早于除权日的最后一个因子，
                    # after = 除权日及之后的第一个因子。
                    # ⚠ 早期版本误用 ex_ms-2d 作分割点——当步进落在除权日当天时，
                    # before 已含步进后的值 → a1<=a0 → 2070/2441 code 误判「无步进」。
                    before = [v for t, v in f if t < ex_ms]
                    after = [v for t, v in f if t >= ex_ms]
                    if not before or not after:
                        continue
                    a0, a1 = before[-1], after[0]
                    if a0 <= 0 or a1 <= a0:
                        continue
                    k = a1 / a0
                    gap = 1.0 - 1.0 / k          # raw 相对 qfq 的跨除权日落差
                    if gap < self._CALIBER_MIN_DIV_GAP:
                        continue                 # 分红过小 → 探针不可分辨，弃用
                    # 除权日前后各容 4/3 天（覆盖周末/停牌），只记探针规格，
                    # bar 数据留待下一步**单次批量查询**取（逐 code 查询实测 0.5s/探针，
                    # 2441 候选 code 下需 ~20 分钟 → 必须批量）
                    specs.append((code, k, gap, ex_ms - 4 * 86400_000,
                                  ex_ms + 3 * 86400_000,
                                  (ex_ms + 8 * 3600_000) // 86400_000))

            if not specs:
                # 区分「样本不足」与「结构性不可探」：ETF 分红实测最大 gap 仅 1.925%
                # （80 条 etf_dividend 全量），永远达不到 2% 可分辨门槛；即便降到 0.5%
                # 也只有 23 探针且票型三分（10 raw/5 double/8 qfq），无统计意义。
                # 如实报告「不可探」而非让「样本不足」长期误导运维。
                if div_tbl == "etf_dividend":
                    self._add(report, "CaliberDriftNotProbeable", tbl, max(len(codes), 1),
                              "warning",
                              f"候选 {len(codes)} code 无探针：ETF 分红过小"
                              f"（实测最大除权步进 1.93% < {self._CALIBER_MIN_DIV_GAP:.0%} 阈值），"
                              f"本方法对该表结构性不可探 → 口径改由 §5.2 结果导向一致率兜底")
                else:
                    self._add(report, "CaliberDriftInsufficientSample", tbl,
                              max(len(codes), 1), "warning",
                              f"可用探针 0 < {self._CALIBER_MIN_PROBES}"
                              f"（候选 {len(codes)} code，无 ≥"
                              f"{self._CALIBER_MIN_DIV_GAP:.0%} 除权步进可分辨），"
                              f"口径未决，请人工复核该表云端口径")
                continue

            # === 单次批量取 bar：所有探针窗口合并为一次查询（口径漂移门禁性能关键）===
            # 各表 bar 时刻约定不同（实测）：分钟表收盘 bar = 14:59-15:01 CST
            # （= 06:59-07:01 UTC → ms%86400000 ∈ [25140000, 25260000]）；
            # 日线表整根 bar 戳在 **00:00 CST**（ms%86400000 == 57600000）。
            # ⚠ 早期版本对四表统一用分钟收盘时刻过滤 → 三张日线/ETF 表 0 探针。
            if tbl.endswith("_minutes"):
                tod_lo, tod_hi = self._BAR_CLOSE_MS_LO, self._BAR_CLOSE_MS_HI
            else:
                tod_lo = tod_hi = self._DAILY_BAR_MS
            win_lo = min(s[3] for s in specs)
            win_hi = max(s[4] for s in specs)
            probe_codes = sorted({s[0] for s in specs})
            bars_by_code: dict = {}
            try:
                cur = conn.execute(
                    f"SELECT code, time, close FROM {tbl} "
                    f"WHERE code IN ({','.join('?' * len(probe_codes))}) "
                    "AND time >= ? AND time < ? AND close > 0 "
                    "AND (time % 86400000) BETWEEN ? AND ?",
                    probe_codes + [win_lo, win_hi, tod_lo, tod_hi])
                for _c, t, cl in cur.fetchall():
                    bars_by_code.setdefault(str(_c), []).append((int(t), float(cl)))
            except Exception:
                bars_by_code = {}
            for seq in bars_by_code.values():
                seq.sort()

            for code, k, gap, p_lo, p_hi, ex_day in specs:
                bars = [b for b in bars_by_code.get(code, []) if p_lo <= b[0] < p_hi]
                if len(bars) < 2:
                    continue
                samples += len(bars)
                day_close: dict = {}
                for t, cl in bars:
                    # 归一到 **CST 自然日**（+8h 后再整除）：
                    # 日线 bar 00:00 CST 与分钟收盘 bar 15:00 CST 同属该 CST 日，
                    # 但二者 UTC 日桶不同（00:00 CST = 前一日 16:00 UTC），
                    # 故必须用 CST 日对齐，否则 ex_day 匹配失败。
                    day_close[(t + 8 * 3600_000) // 86400_000] = cl
                # === 严格探针：必须取「除权日当天」对「其前一交易日」===
                # ⚠ 早期版本取「窗口内最后两个不同日」，当窗口内无除权日 bar 时
                # 会跨到除权日之后的区间，步进已被两端吸收 → r≈1 假判 qfq
                # （实测：stock_daily 由正确的 raw 被误判为 51.0% qfq）。
                if ex_day not in day_close:
                    continue                     # 除权日无 bar → 该探针不可用
                prior = [d for d in day_close if d < ex_day]
                if not prior:
                    continue
                c1 = day_close[ex_day]
                c0 = day_close[max(prior)]       # 除权日之前最近一个交易日
                if c0 <= 0 or c1 <= 0:
                    continue
                r = c1 / c0
                probes += 1
                hyp = {"qfq": 1.0, "raw": 1.0 / k, "double": 1.0 / (k * k)}
                tol = self._CALIBER_DECISIVE_MARGIN * gap
                best = min(hyp, key=lambda h: abs(r - hyp[h]))
                if abs(r - hyp[best]) > tol:
                    unclassified += 1   # 真实涨跌幅等噪声 → 不计票
                    continue
                votes[best] += 1
                if best != "raw":
                    offenders[best].append(f"{code}@{ex_day}")

            if probes < self._CALIBER_MIN_PROBES:
                # count 传候选 code 数（非 probes）——probes 可能为 0，而 _add 对
                # count=0 静默跳过，会使「全无可用探针」这一真实风险隐形。
                self._add(report, "CaliberDriftInsufficientSample", tbl, max(len(codes), 1),
                          "warning",
                          f"可用探针 {probes} < {self._CALIBER_MIN_PROBES}"
                          f"（候选 {len(codes)} code，除权样本不足或分红 <2% 不可分辨），"
                          f"口径未决，请人工复核该表云端口径")
                continue

            decided = max(votes, key=lambda h: votes[h])
            share = votes[decided] / probes
            if decided == "raw" or share <= self._CALIBER_MAJORITY:
                if share <= self._CALIBER_MAJORITY:
                    self._add(report, "CaliberDriftInconclusive", tbl, probes, "warning",
                              f"多数票 {share:.1%} 未过半（{votes}，探针 {probes}），"
                              f"口径未决; unclassified={unclassified}")
                continue

            # === 阻断档位（§5.3 设计意图 vs 实测可辨识性）===
            # 取证结论（docs/evidence/caliber-drift-gate-forensics-20260928.md §3-§4）：
            # 本门禁探的是**本地表**口径，而 stock_minutes 本地值 = 云端值逐行直写
            # （A′ 对该表不还原），故「云端改返 qfq」与「缺陷1 历史污染未修复」在本地
            # 叠加、不可分离。实测 median(minute/daily close) ≈ 0.999（主体 raw）+ 膨胀
            # 长尾（p99 1.11 / max 18.7）= 未修复污染的分布特征，非口径翻转。
            # 因此默认**只告警不阻断**，避免修复前后常亮误报（违背纯增益）；
            # 待数据修复验收通过后，将 _CALIBER_BLOCK_STOCK_MINUTES 置 True 恢复
            # §5.3 的阻断语义（届时本地应为纯 raw，翻转即真回归）。
            sev = "error" if (is_blocking and self._CALIBER_BLOCK_STOCK_MINUTES) else "warning"
            detail = (f"实测 {decided}（票 {votes[decided]}/{probes}"
                      f"={share:.1%}，期望 raw），sample={offenders[decided][:10]}")
            if is_blocking and decided == "qfq":
                if self._CALIBER_BLOCK_STOCK_MINUTES:
                    detail += "；stock_minutes 翻转 = 缺陷1 直接回归（A′ 对该表不还原），阻断"
                else:
                    detail += ("；stock_minutes 非 raw = 缺陷1 污染与口径漂移本地不可区分，"
                               "待数据修复验收后启用阻断（_CALIBER_BLOCK_STOCK_MINUTES）")
            self._add(report, "CaliberDrift", tbl, votes[decided], sev, detail)

    def _clean_factor_segments(self, fdf: pd.DataFrame) -> pd.DataFrame:
        """同值段合并 + 污染尖刺剔除。

        - 同值段：连续同 adj_factor 的时间戳合并为一段（**段值从段首 time 生效**，
          返回 time = 段首 start——merge_asof backward 以 start 为生效点，保证
          时间连续性；若取段末，长段在时间轴上只剩一个点，历史 bar 会错误匹配
          到更早的旧段，产生假阳性 dev）；
        - 尖刺：段跨度 < 1 天 且 与前一/后一段的比值差异 > 50%（如世代切换
          批量归 1.0 的临时行）→ 整段剔除；
        - 返回（code, time, adj_factor）干净序列（time 取段首时间戳）。
        """
        import numpy as np
        import pandas as pd
        out = []
        for code, g in fdf.sort_values("time").groupby("code"):
            ts = g["time"].to_numpy()
            vals = g["adj_factor"].to_numpy()
            segs = []
            start = 0
            for i in range(1, len(vals) + 1):
                if i == len(vals) or vals[i] != vals[i - 1]:
                    segs.append((int(ts[start]), int(ts[i - 1]), float(vals[start])))
                    start = i
            for idx, (st, en, v) in enumerate(segs):
                is_spike = ((en - st) < 86_400_000 and 0 < idx < len(segs) - 1
                            and (v / segs[idx - 1][2] < 0.5 or v / segs[idx - 1][2] > 2.0))
                if not is_spike:
                    out.append((code, st, v))
        if not out:
            return pd.DataFrame(columns=["code", "time", "adj_factor"])
        return pd.DataFrame(out, columns=["code", "time", "adj_factor"])

    def _audit_factor_monotonicity(self, conn, report):
        """A2：因子序列非单调告警（FactorMonotonicity，warning 级，不阻断）。

        对 aux 因子表（adj_factor/fund_adj）按 code LAG 扫描，回落 > 1e-9 即计数。
        现状（实测）：股票 83.3%、ETF 12.4% 非单调——告警先行，治理归阶段 3。
        """
        aux_path = self._resolve_aux_path(conn)
        if aux_path is None:
            return
        import sqlite3
        try:
            conn = sqlite3.connect(str(aux_path), timeout=30)
            try:
                conn.execute("PRAGMA query_only=ON")
                tables = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                for factor_tbl in ("adj_factor", "fund_adj"):
                    if factor_tbl not in tables:
                        continue
                    bad = conn.execute(
                        f"WITH x AS (SELECT code, adj_factor, "
                        f"LAG(adj_factor) OVER (PARTITION BY code ORDER BY time) AS prev "
                        f"FROM {factor_tbl}) "
                        f"SELECT DISTINCT code FROM x WHERE prev IS NOT NULL "
                        f"AND adj_factor < prev - 1e-9").fetchall()
                    if bad:
                        self._add(report, "FactorMonotonicity", factor_tbl, len(bad),
                                  "warning",
                                  f"非单调 {len(bad)} code（世代混存/修正痕迹，治理见阶段3），"
                                  f"sample={[b[0] for b in bad[:10]]}")
            finally:
                conn.close()
        except sqlite3.Error:
            return

    @staticmethod
    def _add(report, check, table, count, severity, detail=""):
        report.checks_run += 1
        if count:
            report.issues.append(QualityIssue(check, table, int(count), severity, detail))


def main():
    from quantstudio._paths import db_path, DATA_ROOT, quarantine_db_path
    root = Path(__file__).resolve().parents[2]
    report = DataQualityAuditor.from_config(
        db_path(), root / "config" / "alignment_rules.json",
        batch_audit_path=DATA_ROOT / "batch_audit.db",
        quarantine_path=quarantine_db_path()).run()
    print(f"checks={report.checks_run} passed={report.passed} issues={len(report.issues)}")
    for issue in report.issues:
        print(f"[{issue.severity}] {issue.table}/{issue.check}: {issue.count} {issue.detail}")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
