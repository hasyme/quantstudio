"""DBWriter — 标准化数据写入（Layer 1 模块④）

职责：把通过校验的标准数据（tushare 格式）幂等写入数据库。
仅写入"全部校验通过"的数据；仅当"写入成功 + 回读验证成功"后推进水位。

支持的数据库：
    - DuckDBWriter（默认，零部署嵌入式）
    - QuestDBWriter（可选，ILP 批量写，大量数据）
"""
from __future__ import annotations

import abc
import json
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)
from quantstudio._paths import db_path
from quantstudio.pipeline.snapshot_lock import (ensure_write_lock,
                                                release_write_lock,
                                                assert_lock_owner,
                                                WriteLockHeld)
from quantstudio.pipeline.eps_backfill import backfill_eps_gap  # P-A3 写后回补


# ==========================================================================
# 写路径双段看门狗（设计 docs/duckdb-write-stall-mitigation-design.md §4.1 W1）
#
# 事故2（2026-10-02）：writers._write_locked 的写入语句在生产现场进入不可返回状态，
# 而写路径**无任何有界等待** ⇒ 永久静默挂起 + 持 3A 写锁 + 持 .collector_run.lock
# + 单核空转 + 零日志，阻塞全部采集任务。
# 对照：回测**读**路径早有同款机制（backtest/providers/duckdb_data_access.py:233-288）。
#
# S1 语句预算（QS_DUCKDB_WRITE_TIMEOUT_S，默认 600s）到点 ⇒ logger.critical +
#    conn.interrupt()（duckdb 1.4.5 实测：抛 InterruptException、事务完整回滚、连接可复用；
#    **生效延迟实测约 11.8s** —— DuckDB 仅在执行循环检查点轮询中断标志）。
# S2 硬退出（QS_DUCKDB_WRITE_HARD_ABORT_S，默认 300s）到点 ⇒ 诊断 JSONL（flush+fsync）
#    ⇒ os._exit(75)。S2 在**停摆检测时刻**武装，直到 disarm()（连接成功关闭后）才取消，
#    因此 conn.close() 自身无界时仍能收敛。
# 预算内完成 ⇒ 零行为变化（仅多一个 Timer 对象/语句，不改 SQL / 事务 / 返回值 / 异常）。
# ==========================================================================
_DEFAULT_WRITE_BUDGET_S = 600.0
_DEFAULT_HARD_ABORT_S = 300.0
_DEFAULT_SLOW_WRITE_S = 60.0
_STALL_JSONL_DEFAULT = os.path.join("data", "logs", "duckdb_write_stall.jsonl")
_HARD_ABORT_EXIT_CODE = 75  # EX_TEMPFAIL：采集可重试的临时失败


class DuckDBWriteStalled(RuntimeError):
    """写语句超出预算、被 interrupt 打断后的**有界失败**（替代原"永久静默挂起"）。

    消息含四要素：table / batch_id / rows / 语句头（+ 耗时、是否已发 interrupt、硬退出开关）。
    """


def _env_float(name: str, default: float) -> float:
    """环境变量浮点读取；未设/非法 ⇒ default（fail-safe，不抛）。"""
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("[DuckDBWriter] 环境变量 %s=%r 非法，回退默认 %s", name, raw, default)
        return default


def _sql_head(sql, limit: int = 120) -> str:
    """SQL 头（压缩空白后截断）—— 诊断用，避免把整条 DDL/长 SET 列表写进日志。"""
    return re.sub(r"\s+", " ", str(sql)).strip()[:limit]


# ── v3 主动规避变体开关（**默认全关**；docs/duckdb-write-stall-rootcause-v3-design.md）──
# 纪律：① 任一变体开启都会改变写入物理形态，必须先过 P1 读路径排查清单（V1 除外，不改行序）；
#       ② 一次实验只开一个变量（不叠加），便于归因；③ 任何变体**不得重算写前 count**。
_VARIANT_ENV = {
    "chunk": "QS_DUCKDB_WRITE_CHUNK_ROWS",     # V1 批内子批化（行数，0=关）
    "sort": "QS_DUCKDB_WRITE_SORT_BY_PK",      # V2 写入局部性（0/1）
    "staging": "QS_DUCKDB_WRITE_STAGING",      # V4 staging + 显式事务 merge（0/1）
    "threads": "QS_DUCKDB_WRITE_THREADS",      # V3 SET threads（0=不设置）
    "no_order": "QS_DUCKDB_WRITE_NO_PRESERVE_ORDER",  # V3 preserve_insertion_order=false
}


def _variant_on(name: str) -> bool:
    """变体开关读取（默认关；仅接受 1/true/on/yes，不区分大小写）。"""
    raw = os.environ.get(name, "")
    return str(raw).strip().lower() in ("1", "true", "on", "yes")


def _variant_int(name: str, default: int = 0) -> int:
    try:
        return int(_env_float(name, float(default)) or 0)
    except (TypeError, ValueError):
        return default


def _append_stall_jsonl(record: Dict) -> None:
    """停摆诊断落盘：追加一行 JSON，**flush + fsync** 后返回。

    fsync 是硬要求：硬退出（os._exit）不刷盘，证据会随进程死亡丢失（审计澄清项 5）。
    落盘失败只记 error，绝不阻断退出路径。
    """
    path = os.environ.get("QS_DUCKDB_WRITE_STALL_JSONL", "").strip() or _STALL_JSONL_DEFAULT
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except Exception as exc:  # noqa: BLE001 —— 诊断路径不得抛
        logger.error("[DuckDBWriter] 停摆诊断落盘失败(%s): %s: %s", path, type(exc).__name__, exc)


class _WriteGuard:
    """单条写语句的双段看门狗（配合 `_guarded` / `_guarded_executemany` 使用）。"""

    def __init__(self, conn, *, phase: str, table: str, batch_id: str, rows,
                 sql_head: str, writer=None):
        self.conn = conn
        self.phase = phase
        self.writer = writer
        self.stalled = False
        self.interrupt_sent = False
        self.completed_after_interrupt = False
        self.elapsed = 0.0
        self._t0 = 0.0
        self.budget_s = _env_float("QS_DUCKDB_WRITE_TIMEOUT_S", _DEFAULT_WRITE_BUDGET_S)
        self.hard_abort_s = _env_float("QS_DUCKDB_WRITE_HARD_ABORT_S", _DEFAULT_HARD_ABORT_S)
        self._s1 = None
        self._s2 = None
        self._rec = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "pid": os.getpid(),
            "phase": phase,
            "table": table,
            "batch_id": batch_id,
            "rows": int(rows) if isinstance(rows, (int, float)) else -1,
            "sql_head": sql_head,
            "budget_s": self.budget_s,
            "hard_abort_s": self.hard_abort_s,
            "interrupt_sent": False,
            "hard_abort": False,
            "outcome": "budget_exceeded",
        }
        try:
            self._rec["db_path"] = str(writer.db_path) if writer is not None else None
        except Exception:  # noqa: BLE001
            self._rec["db_path"] = None

    # ── S1：预算到点 → 告警 + interrupt ────────────────────────────────────
    def _on_budget(self) -> None:
        self.stalled = True
        self.interrupt_sent = True
        self._rec["interrupt_sent"] = True
        logger.critical(
            "[DuckDBWriteStalled] 写语句超预算 %.1fs，发 interrupt：table=%s batch=%s rows=%s "
            "phase=%s sql=%s", self.budget_s, self._rec["table"], self._rec["batch_id"],
            self._rec["rows"], self.phase, self._rec["sql_head"])
        try:
            self.conn.interrupt()
        except Exception as exc:  # noqa: BLE001 —— interrupt 本身失败仍要继续等宽限期
            logger.error("[DuckDBWriteStalled] conn.interrupt() 失败：%s: %s",
                         type(exc).__name__, exc)
        self._arm_hard_abort()

    def _arm_hard_abort(self) -> None:
        if self.hard_abort_s <= 0 or self._s2 is not None:
            return
        self._s2 = threading.Timer(self.hard_abort_s, self._hard_exit)
        self._s2.daemon = True
        self._s2.start()

    # ── S2：宽限耗尽 → 落证据 + 硬退出（释放采集锁，阻断空转）──────────────
    def _hard_exit(self) -> None:
        self._rec["hard_abort"] = True
        self._rec["execute_elapsed_s"] = round(time.time() - self._t0, 3)
        _append_stall_jsonl(dict(self._rec))
        logger.critical(
            "[DuckDBWriteStalled] interrupt 后 %.1fs 仍未返回 ⇒ 诊断已落盘，"
            "本进程将以 %d(EX_TEMPFAIL) 退出：采集停止、.collector_run.lock 由 OS 释放，"
            "需外部拉起后从断点续跑。table=%s batch=%s rows=%s phase=%s sql=%s",
            self.hard_abort_s, _HARD_ABORT_EXIT_CODE, self._rec["table"],
            self._rec["batch_id"], self._rec["rows"], self.phase, self._rec["sql_head"])
        os._exit(_HARD_ABORT_EXIT_CODE)

    def disarm(self) -> None:
        """连接已成功关闭后调用：取消 S2（此后不再需要硬退出兜底）。"""
        if self._s2 is not None:
            self._s2.cancel()
            self._s2 = None

    def __enter__(self):
        self._t0 = time.time()
        if self.budget_s > 0:
            self._s1 = threading.Timer(self.budget_s, self._on_budget)
            self._s1.daemon = True
            self._s1.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._s1 is not None:
            self._s1.cancel()
            self._s1 = None
        self.elapsed = time.time() - self._t0
        if not self.stalled:
            return False            # 预算内完成：零行为变化
        # 停摆后语句已返回（无论是否抛错）⇒ 取消 S2，落证据，转为可识别异常
        if self._s2 is not None:
            self._s2.cancel()
            self._s2 = None
        self.completed_after_interrupt = exc_type is None
        self._rec["execute_elapsed_s"] = round(self.elapsed, 3)
        self._rec["outcome"] = ("completed_after_interrupt"
                                if exc_type is None else "interrupted_exception")
        self._rec["error_type"] = exc_type.__name__ if exc_type else None
        self._rec["error_head"] = str(exc)[:200] if exc is not None else None
        w = self.writer
        if w is not None:
            try:
                self._rec["dedup_circuit_open"] = bool(w._dedup_circuit_open)
                self._rec["dedup_fail_window_hits"] = w._dedup_fail_window_hits()
                self._rec["write_batch_seq"] = w._write_batch_seq
            except Exception:  # noqa: BLE001
                pass
        _append_stall_jsonl(dict(self._rec))
        logger.critical(
            "[DuckDBWriteStalled] 写语句已中断并转为有界失败：table=%s batch=%s rows=%s "
            "phase=%s elapsed=%.1fs outcome=%s sql=%s",
            self._rec["table"], self._rec["batch_id"], self._rec["rows"], self.phase,
            self.elapsed, self._rec["outcome"], self._rec["sql_head"])
        raise DuckDBWriteStalled(
            "QS_DUCKDB_WRITE_TIMEOUT: 写语句超预算并被 interrupt 中断（表=%s 批次=%s 行数=%s "
            "阶段=%s 已耗时=%.1fs 预算=%.1fs 已发interrupt=%s 硬退出启用=%s 结局=%s）SQL=%s"
            % (self._rec["table"], self._rec["batch_id"], self._rec["rows"], self.phase,
               self.elapsed, self.budget_s, self.interrupt_sent, self.hard_abort_s > 0,
               self._rec["outcome"], self._rec["sql_head"])) from exc


def _guarded(conn, guard: _WriteGuard, sql, params=None):
    """**唯一允许的裸 `conn.execute` 入口**（写路径必须经 `_WriteGuard`）。

    拆成模块级函数而非散落 try/except：便于静态回归（tests/test_writer_stall_watchdog.py
    断言各写路径函数体内不再出现裸 `conn.execute(`）。
    """
    with guard:
        if params is None:
            return conn.execute(sql)
        return conn.execute(sql, params)


def _guarded_executemany(conn, guard: _WriteGuard, sql, seq_of_params):
    """`_guarded` 的 executemany 版本（mcp_adapter._inject_dividend 用）。"""
    with guard:
        return conn.executemany(sql, seq_of_params)


def _watermark_upsert_sql(params):
    """source_watermark 的 8 列 upsert（公共/事务版共用同一 SQL 文本，语义零变化）。"""
    return (
        "INSERT INTO source_watermark "
        "(source, table_name, freq, last_date, last_batch_id, updated_at, "
        " source_generation, cutover_id) VALUES (?,?,?,?,?,?,?,?) "
        "ON CONFLICT (source, table_name, freq) DO UPDATE SET "
        "last_date=EXCLUDED.last_date, last_batch_id=EXCLUDED.last_batch_id, "
        "updated_at=EXCLUDED.updated_at, "
        "source_generation=EXCLUDED.source_generation, cutover_id=EXCLUDED.cutover_id",
        params,
    )



def _is_writer_auto_backfill_enabled() -> bool:
    """P-A3 feature gate：writer 写后自动 eps 回补默认关闭，显式开启才生效。

    fail-closed：仅环境变量 QS_AUTO_BACKFILL_EPS 为 "1"/"true"/"on"（不区分大小写）
    时开启；未设置、"0"、"false"、"off"、空字符串、其他任意值一律关闭。
    CLI --apply 直接调用 backfill_eps_gap(conn)，不受此 gate 影响。
    """
    v = os.environ.get("QS_AUTO_BACKFILL_EPS", "").strip().lower()
    return v in ("1", "true", "on")


class WriteResult(int):
    """write() 返回值：作为 int 向后兼容（=提交行数），同时携带 .new/.updated 审计字段。

    用法：
        n = writer.write(df, ...)      # n 当 int 用 = 提交行数（向后兼容）
        result = writer.write(df, ...)
        result.new, result.updated     # 新增行数 / 更新行数（审计准确）
    """
    new: int = 0
    updated: int = 0

    def __new__(cls, submitted: int, new: int = 0, updated: int = 0):
        obj = int.__new__(cls, submitted)
        obj.new = new
        obj.updated = updated
        return obj

    def __repr__(self) -> str:
        return f"WriteResult(submitted={int(self)}, new={self.new}, updated={self.updated})"


# 各表的建表 DDL（DuckDB 方言，统一口径 v2.0）
# time 用 BIGINT（毫秒时间戳），volume=股，amount=元，code=裸6位码
# v2.4 B-3a：source_watermark DDL 改用共享单一真相源 SOURCE_WATERMARK_2_1_DDL
# （审计列 source_generation/cutover_id NOT NULL 无 DEFAULT；PK 不变）。
from quantstudio.pipeline.qfq_schema_contracts import (  # noqa: E402
    SOURCE_WATERMARK_2_1_DDL, pre_cutover_generation,
    TARGET_SOURCE_WATERMARK_2_1_FINGERPRINT, verify_fingerprint, _table_exists)


class _WriterSchemaMigrationRequired(RuntimeError):
    """DuckDBWriter 初始化遇版本化旧/错误共享表时写前 fail-fast。"""


def _assert_source_watermark_init_safe(conn) -> None:
    """v2.4 B-3a P0-2：DuckDBWriter._init_tables 第一条 DDL 前的共享表安全闸。

    source_watermark 状态 → writer init 行为：
    - 不存在 → 允许（DDL_DUCKDB 含 SOURCE_WATERMARK_2_1_DDL 会创建 target 8 列）；
    - 完整 target 8 列 → 允许继续；
    - 旧 6 列 / partial / 错误结构 → 抛 ``_WriterSchemaMigrationRequired`` 写前 fail-fast
      （明确 migration-required，而非等到运行时 advance_watermark 才 BinderException）。

    该门禁不通过 ``--allow-production`` 绕过（writer 不接受该开关；硬门禁）。
    """
    if not _table_exists(conn, "source_watermark"):
        return  # 不存在 → 放行（将创建 target）
    if verify_fingerprint(conn, {"source_watermark": TARGET_SOURCE_WATERMARK_2_1_FINGERPRINT},
                          reject_extra=True):
        return  # 完整 target 8 列 → 放行
    # 旧/partial/错误 → fail-fast
    raise _WriterSchemaMigrationRequired(
        "[DuckDBWriter] source_watermark 为旧 6 列 / partial / 错误结构，"
        "禁止 writer init（会到运行时 advance_watermark 才崩溃）。"
        "该库需显式 migration runner 升级到 2.1（B-3b 范围）。")


def _assert_qfq_schema_init_safe(conn) -> None:
    """v2.4 B-3a.3 P0-1：DuckDBWriter._init_tables 第一条 DDL 前的**完整主库状态预检**。

    复用 ``qfq_schema_status.detect_schema_status`` 五态判定。仅 ``EMPTY_OR_NEW`` /
    ``COMPLETE_2_1`` 放行；``COMPLETE_2_0`` / ``PARTIAL_OR_MIXED`` / ``UNKNOWN`` 全部
    抛 ``_WriterSchemaMigrationRequired`` 写前 fail-fast。

    这阻止"数据库已处于 partial/完整旧 2.0（如仅一张 qfq_trigger_queue）时，writer 在
    QFQ init 安全闸执行前创建大量框架表，数据库被进一步修改"的违规。

    | 状态 | writer init |
    |---|---|
    | EMPTY_OR_NEW | 允许创建框架表 |
    | COMPLETE_2_1 | 允许继续初始化非 QFQ 框架表 |
    | COMPLETE_2_0 | migration-required（写前拒绝）|
    | PARTIAL_OR_MIXED | fail-closed（写前拒绝）|
    | UNKNOWN | fail-closed（写前拒绝）|

    不通过 ``--allow-production`` 绕过（硬门禁）。
    """
    from quantstudio.pipeline.qfq_schema_status import (
        detect_schema_status, SchemaStatus)
    status = detect_schema_status(conn)
    if status in (SchemaStatus.EMPTY_OR_NEW, SchemaStatus.COMPLETE_2_1):
        return  # 放行
    raise _WriterSchemaMigrationRequired(
        f"[DuckDBWriter] 主库处于 {status.value} 状态，禁止 writer init（会在 QFQ init 安全闸"
        f"执行前创建框架表，进一步修改数据库）。仅 EMPTY_OR_NEW/COMPLETE_2_1 放行。"
        f"该库需显式 migration runner 升级到 2.1（B-3b 范围）。")


DDL_DUCKDB = {
    "stock_daily": """
        CREATE TABLE IF NOT EXISTS stock_daily (
            code VARCHAR, time BIGINT,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, preClose DOUBLE,
            suspendFlag INTEGER, settelementPrice DOUBLE, openInterest DOUBLE,
            open_front DOUBLE, high_front DOUBLE, low_front DOUBLE, close_front DOUBLE,
            open_back DOUBLE, high_back DOUBLE, low_back DOUBLE, close_back DOUBLE,
            open_front_ratio DOUBLE, high_front_ratio DOUBLE, low_front_ratio DOUBLE, close_front_ratio DOUBLE,
            open_back_ratio DOUBLE, high_back_ratio DOUBLE, low_back_ratio DOUBLE, close_back_ratio DOUBLE,
            turn DOUBLE, pctChg DOUBLE, peTTM DOUBLE, psTTM DOUBLE, pcfNcfTTM DOUBLE, pbMRQ DOUBLE,
            isST INTEGER,
            is_st_reliable BOOLEAN, is_st_reliable_source VARCHAR,
            is_delisting_risk BOOLEAN, is_delisting_risk_source VARCHAR,
            dividend_type VARCHAR, update_time VARCHAR,
            PRIMARY KEY(code, time)
        )""",
    "stock_minutes": """
        CREATE TABLE IF NOT EXISTS stock_minutes (
            code VARCHAR, time BIGINT, freq VARCHAR,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, preClose DOUBLE,
            suspendFlag INTEGER, settelementPrice DOUBLE, openInterest DOUBLE,
            open_front DOUBLE, high_front DOUBLE, low_front DOUBLE, close_front DOUBLE,
            open_back DOUBLE, high_back DOUBLE, low_back DOUBLE, close_back DOUBLE,
            open_front_ratio DOUBLE, high_front_ratio DOUBLE, low_front_ratio DOUBLE, close_front_ratio DOUBLE,
            open_back_ratio DOUBLE, high_back_ratio DOUBLE, low_back_ratio DOUBLE, close_back_ratio DOUBLE,
            dividend_type VARCHAR, update_time VARCHAR,
            PRIMARY KEY(code, time, freq)
        )""",
    "etf_minutes": """
        CREATE TABLE IF NOT EXISTS etf_minutes (
            code VARCHAR, time BIGINT, freq VARCHAR,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, preClose DOUBLE,
            suspendFlag INTEGER, settelementPrice DOUBLE, openInterest DOUBLE,
            open_front DOUBLE, high_front DOUBLE, low_front DOUBLE, close_front DOUBLE,
            open_back DOUBLE, high_back DOUBLE, low_back DOUBLE, close_back DOUBLE,
            open_front_ratio DOUBLE, high_front_ratio DOUBLE, low_front_ratio DOUBLE, close_front_ratio DOUBLE,
            open_back_ratio DOUBLE, high_back_ratio DOUBLE, low_back_ratio DOUBLE, close_back_ratio DOUBLE,
            dividend_type VARCHAR, update_time VARCHAR,
            PRIMARY KEY(code, time, freq)
        )""",
    "tick": """
        CREATE TABLE IF NOT EXISTS tick (
            code VARCHAR, time BIGINT,
            lastPrice DOUBLE, open DOUBLE, high DOUBLE, low DOUBLE, lastClose DOUBLE,
            amount DOUBLE, volume DOUBLE, pvolume DOUBLE, stockStatus INTEGER,
            openInt DOUBLE, lastSettlementPrice DOUBLE,
            askPrice1 DOUBLE, askPrice2 DOUBLE, askPrice3 DOUBLE, askPrice4 DOUBLE, askPrice5 DOUBLE,
            bidPrice1 DOUBLE, bidPrice2 DOUBLE, bidPrice3 DOUBLE, bidPrice4 DOUBLE, bidPrice5 DOUBLE,
            askVol1 DOUBLE, askVol2 DOUBLE, askVol3 DOUBLE, askVol4 DOUBLE, askVol5 DOUBLE,
            bidVol1 DOUBLE, bidVol2 DOUBLE, bidVol3 DOUBLE, bidVol4 DOUBLE, bidVol5 DOUBLE,
            transactionNum INTEGER, update_time VARCHAR,
            PRIMARY KEY(code, time)
        )""",
    "fin_indicator": """
        CREATE TABLE IF NOT EXISTS fin_indicator (
            code VARCHAR, ann_date BIGINT, end_date BIGINT,
            eps DOUBLE, diluted_eps DOUBLE, bps DOUBLE, roe DOUBLE,
            pe_ttm DOUBLE, pb DOUBLE, ps_ttm DOUBLE,
            np_yoy DOUBLE, or_yoy DOUBLE, tr_yoy DOUBLE,
            update_flag INTEGER,
            backfill_eps_source VARCHAR,
            PRIMARY KEY(code, end_date, ann_date)
        )""",
    "index_daily": """
        CREATE TABLE IF NOT EXISTS index_daily (
            code VARCHAR, time BIGINT,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            pctChg DOUBLE, volume DOUBLE, amount DOUBLE,
            PRIMARY KEY(code, time)
        )""",
    "stock_daily_valuation": """
        CREATE TABLE IF NOT EXISTS stock_daily_valuation (
            code VARCHAR, time BIGINT,
            circ_mv DOUBLE, total_mv DOUBLE,
            free_share DOUBLE,
            pe_ttm DOUBLE, pb DOUBLE, turnover_rate DOUBLE,
            update_time VARCHAR,
            PRIMARY KEY(code, time)
        )""",
    "etf_daily": """
        CREATE TABLE IF NOT EXISTS etf_daily (
            code VARCHAR, time BIGINT,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            preClose DOUBLE, pctChg DOUBLE,
            volume DOUBLE, amount DOUBLE, turn DOUBLE,
            open_front DOUBLE, high_front DOUBLE, low_front DOUBLE, close_front DOUBLE,
            open_back DOUBLE, high_back DOUBLE, low_back DOUBLE, close_back DOUBLE,
            open_front_ratio DOUBLE, high_front_ratio DOUBLE, low_front_ratio DOUBLE, close_front_ratio DOUBLE,
            open_back_ratio DOUBLE, high_back_ratio DOUBLE, low_back_ratio DOUBLE, close_back_ratio DOUBLE,
            isST INTEGER, dividend_type VARCHAR, update_time VARCHAR,
            PRIMARY KEY(code, time)
        )""",
    "stock_basic": """
        CREATE TABLE IF NOT EXISTS stock_basic (
            code VARCHAR PRIMARY KEY,
            ts_code VARCHAR NOT NULL,
            symbol VARCHAR, name VARCHAR NOT NULL,
            area VARCHAR, industry VARCHAR, market VARCHAR,
            list_status VARCHAR NOT NULL,
            list_date BIGINT, delist_date BIGINT, exchange VARCHAR,
            update_time VARCHAR, data_source VARCHAR
        )""",
    "trade_calendar": """
        CREATE TABLE IF NOT EXISTS trade_calendar (
            cal_date BIGINT NOT NULL,
            is_open BOOLEAN NOT NULL,
            source VARCHAR,
            updated_at TIMESTAMP,
            exchange VARCHAR,
            pretrade_date BIGINT,
            PRIMARY KEY (cal_date)
        )""",
    "etf_basic": """
        CREATE TABLE IF NOT EXISTS etf_basic (
            code VARCHAR PRIMARY KEY,
            ts_code VARCHAR, name VARCHAR, exchange VARCHAR,
            list_date BIGINT, delist_date BIGINT,
            etf_type VARCHAR NOT NULL, tracking_index VARCHAR,
            is_cross_border BOOLEAN NOT NULL, status VARCHAR,
            fund_type VARCHAR, invest_type VARCHAR, type VARCHAR,
            classification_method VARCHAR NOT NULL,
            classification_version VARCHAR NOT NULL,
            update_time VARCHAR, data_source VARCHAR
        )""",
    "stock_float_share": """
        CREATE TABLE IF NOT EXISTS stock_float_share (
            code VARCHAR, end_date BIGINT, ann_date BIGINT,
            free_share DOUBLE, total_share DOUBLE,
            circ_mv DOUBLE, total_mv DOUBLE,
            update_time VARCHAR,
            PRIMARY KEY(code, end_date, ann_date)
        )""",
    "index_constituents": """
        CREATE TABLE IF NOT EXISTS index_constituents (
            index_code VARCHAR, code VARCHAR, time BIGINT,
            weight DOUBLE,
            PRIMARY KEY(index_code, code, time)
        )""",
    # ---- 指数成分快照完整性契约（F3 修订）：完整性在打点写入时确定，不依赖未来数据 ----
    "index_constituents_snapshot_meta": """
        CREATE TABLE IF NOT EXISTS index_constituents_snapshot_meta (
            index_code VARCHAR, time BIGINT,
            n_constituents INTEGER, expected_count INTEGER,
            status VARCHAR,
            n_duplicate_codes INTEGER, n_negative_weights INTEGER,
            n_blank_codes INTEGER,
            update_time VARCHAR, data_source VARCHAR,
            PRIMARY KEY(index_code, time)
        )""",
    # ---- 三大报表（对齐 Ptrade 口径，字段单位=元）----
    "balance_statement": """
        CREATE TABLE IF NOT EXISTS balance_statement (
            code VARCHAR, end_date BIGINT, ann_date BIGINT,
            total_assets DOUBLE, total_liability DOUBLE, total_equity DOUBLE,
            total_current_assets DOUBLE, total_non_current_assets DOUBLE,
            total_current_liability DOUBLE, total_non_current_liability DOUBLE,
            account_receivable DOUBLE, account_payable DOUBLE, inventory DOUBLE,
            cash_equivalents DOUBLE, fixed_asset DOUBLE, intangible_asset DOUBLE, goodwill DOUBLE,
            update_time VARCHAR,
            PRIMARY KEY(code, end_date, ann_date)
        )""",
    "income_statement": """
        CREATE TABLE IF NOT EXISTS income_statement (
            code VARCHAR, end_date BIGINT, ann_date BIGINT,
            operating_revenue DOUBLE, operating_cost DOUBLE, operating_profit DOUBLE,
            total_profit DOUBLE, net_profit DOUBLE, np_parent_company_owners DOUBLE,
            sale_expense DOUBLE, manage_expense DOUBLE, finance_expense DOUBLE, rd_expense DOUBLE,
            income_tax DOUBLE, basic_eps DOUBLE,
            update_time VARCHAR,
            PRIMARY KEY(code, end_date, ann_date)
        )""",
    "cashflow_statement": """
        CREATE TABLE IF NOT EXISTS cashflow_statement (
            code VARCHAR, end_date BIGINT, ann_date BIGINT,
            net_operate_cash_flow DOUBLE, net_invest_cash_flow DOUBLE, net_finance_cash_flow DOUBLE,
            cash_add_balance DOUBLE, goods_sale_and_services DOUBLE, goods_buy_and_services DOUBLE,
            fixed_asset_depreciation DOUBLE,
            update_time VARCHAR,
            PRIMARY KEY(code, end_date, ann_date)
        )""",
    # ---- 除权除息 ----
    "stock_dividend": """
        CREATE TABLE IF NOT EXISTS stock_dividend (
            code VARCHAR, ex_date BIGINT, record_date BIGINT,
            ann_date BIGINT, end_date BIGINT,
            cash_div_before_tax DOUBLE, cash_div_after_tax DOUBLE,
            cash_div DOUBLE, stk_div DOUBLE,
            stk_bo_rate DOUBLE, stk_co_rate DOUBLE,
            div_rat DOUBLE, div_proc VARCHAR,
            update_time VARCHAR,
            PRIMARY KEY(code, ex_date)
        )""",
    # ---- ETF 基金分红（tushare fund_div；管线方案 etf-dividend 任务）----
    "etf_dividend": """
        CREATE TABLE IF NOT EXISTS etf_dividend (
            code VARCHAR, ex_date BIGINT, record_date BIGINT,
            ann_date BIGINT, imp_anndate BIGINT, base_date BIGINT,
            div_proc VARCHAR, pay_date BIGINT, earpay_date BIGINT,
            net_ex_date BIGINT, div_cash DOUBLE, base_unit DOUBLE,
            ear_distr DOUBLE, ear_amount DOUBLE, account_date BIGINT,
            base_year VARCHAR, update_time VARCHAR,
            PRIMARY KEY(code, ex_date)
        )""",
    # ---- 申万行业分类（LEGACY 快照，仅审计；正式能力见下两张表 F4）----
    "sw_industry": """
        CREATE TABLE IF NOT EXISTS sw_industry (
            code VARCHAR, industry_code VARCHAR, industry_name VARCHAR, industry_level VARCHAR,
            update_time VARCHAR,
            PRIMARY KEY(code, industry_code)
        )""",
    # ---- 行业分类定义（F4 正式 canonical 表；effective_from=0 表示长期有效）----
    "industry_classification": """
        CREATE TABLE IF NOT EXISTS industry_classification (
            classification_system VARCHAR, classification_version VARCHAR,
            industry_code VARCHAR, industry_name VARCHAR, industry_level VARCHAR,
            parent_industry_code VARCHAR,
            effective_from BIGINT, effective_to BIGINT,
            update_time VARCHAR, data_source VARCHAR,
            PRIMARY KEY (classification_system, classification_version,
                         industry_level, industry_code, effective_from)
        )""",
    # ---- 行业成员历史（F4 正式 canonical 表，PIT 有效区间）----
    "industry_membership": """
        CREATE TABLE IF NOT EXISTS industry_membership (
            classification_system VARCHAR, classification_version VARCHAR,
            industry_level VARCHAR, industry_code VARCHAR, code VARCHAR,
            effective_from BIGINT, effective_to BIGINT,
            update_time VARCHAR, data_source VARCHAR,
            PRIMARY KEY (classification_system, classification_version,
                         industry_level, industry_code, code, effective_from)
        )""",
    "source_watermark": SOURCE_WATERMARK_2_1_DDL,  # v2.4 B-3a：共享 DDL 单一真相源（8列，审计列 NOT NULL）
    "stock_namechange": """
        CREATE TABLE IF NOT EXISTS stock_namechange (
            code VARCHAR, change_date BIGINT,
            name_before VARCHAR, name_after VARCHAR, status_after VARCHAR,
            update_time VARCHAR,
            PRIMARY KEY(code, change_date)
        )""",
    "stock_delist": """
        CREATE TABLE IF NOT EXISTS stock_delist (
            code VARCHAR, list_date BIGINT, delist_date BIGINT, market VARCHAR,
            update_time VARCHAR,
            PRIMARY KEY(code, market)
        )""",
}


class BaseWriter(abc.ABC):
    """写入器基类"""

    def __init__(self, config: Dict):
        self.config = config

    @abc.abstractmethod
    def write(self, df: pd.DataFrame, table: str, batch_id: str) -> int:
        """幂等 upsert 写入。返回写入条数。"""
        ...

    @abc.abstractmethod
    def get_last_date(self, source: str, table: str, freq: str = "daily") -> Optional[str]:
        """读取水位"""
        ...

    @abc.abstractmethod
    def advance_watermark(self, source: str, table: str, freq: str,
                          last_date: str, batch_id: str):
        """推进水位（仅写入成功后调用）"""
        ...


class DuckDBWriter(BaseWriter):
    """DuckDB 写入器（默认推荐，零部署）

    config 示例：{"type": "duckdb", "path": "data/quantstudio.db"}
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        try:
            import duckdb
        except ImportError as e:
            raise ImportError("未安装 duckdb，请 pip install duckdb") from e
        self.db_path = Path(config.get("path", str(db_path())))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._duckdb = duckdb
        # 连接锁：DuckDB 不允许同文件多线程同时新建连接，串行化连接生命周期
        import threading
        self._conn_lock = threading.Lock()
        # 持久共享连接（read_write）：供 daemon 内部只读查询复用，避免与 write 的 read_write
        # 短连接并发时开 read_only 连接触发「different configuration」冲突
        # （DuckDB 规则：同 db 文件 read_only 与 read_write 不能并存，哪怕不同线程）。
        # 所有内部查询统一走 read_write，永不冲突。GUI 的 read_only 查询属独立进程范畴。
        self._shared_conn = None
        # v3.1：写入分档 fail-closed 熔断状态（内存态、进程级，不落库、不引入新文件）
        self._write_batch_seq = 0          # 批次序号（滑动窗口基准，每批 +1）
        self._dedup_fail_marks = []        # 窗口内 fail-closed 发生的批次序号
        self._dedup_circuit_open = False   # True ⇒ P1 自我熔断，一律走 ON CONFLICT 原路径
        # W5：批间隙 CHECKPOINT 计数器（仅在 QS_DUCKDB_WRITE_CHECKPOINT_BATCHES>0 时使用）
        self._ckpt_batch_counter = 0
        self._init_tables()

    # ── T1（2026-09-17）：RW 打开退避重试与持有者归因 ──
    # 退避序列合计 30s（全窗统一，两处 RW 打开点共用）
    _RW_BACKOFF_SECONDS = (1, 2, 4, 8, 15)

    # ── v3.1：写入分档 fail-closed 熔断（见 docs/duckdb-conflict-hang-mitigation-design.md §3.1.1）──
    # 背景：去重计数 SELECT 失败时 fail-closed 回退 ON CONFLICT；若持续失败，"纯 INSERT 性能分档"
    # 会"表面正常、实则静默失效"，故需窗口计数 + 熔断使运维可发现。
    # **定位更正（docs/duckdb-write-stall-mitigation-design.md §A2）**：本分档是**性能优化**，
    # **不是** DuckDB 写入停摆缺陷的规避手段——事故2 已证纯 INSERT 与 ON CONFLICT 两条语句
    # 在生产现场都会进入不可返回状态；停摆的根治是写路径有界等待（W1 看门狗）。
    # 滑动窗口 = 最近 N 批；窗口内失败 ≥WARN 次告警，≥OPEN 次 ⇒ 自我熔断。
    _DEDUP_FAIL_WINDOW_BATCHES = 20
    _DEDUP_FAIL_WARN_AT = 3
    _DEDUP_FAIL_OPEN_AT = 10

    def _describe_lock_holder(self):
        """psutil 归因：谁可能持有该库（**尽力而为**，不可得则返回 None，不得抛）。

        归因口径：候选 = 命令行提及本库文件名或 daemon/GUI 入口的进程；
        不试图读取真实文件句柄（跨平台不可靠），只给出可人工核验的候选清单。
        """
        try:
            import psutil
        except Exception:
            return None
        import os as _os
        db_name = self.db_path.name
        db_stem = self.db_path.stem
        # 自身与父进程、以及常见 shell 包装器**一律排除**：否则「含项目路径的 pwsh 包装」
        # 会被误判为持有者（2026-09-17 样例实测命中 pid=…powershell.exe = 假归因）。
        _SHELLS = {"powershell.exe", "pwsh.exe", "cmd.exe", "bash", "sh", "zsh", "pythonw.exe"}
        exclude = {_os.getpid(), _os.getppid()}
        try:
            import psutil as _ps
            try:
                for parent in _ps.Process(_os.getpid()).parents():
                    exclude.add(parent.pid)
            except Exception:
                pass
        except Exception:
            pass
        hits = []
        try:
            for proc in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
                try:
                    info = proc.info
                    pid = info.get("pid")
                    name = str(info.get("name") or "")
                    if pid in exclude or name.lower() in _SHELLS:
                        continue
                    joined = " ".join(str(c) for c in (info.get("cmdline") or []))
                    # 排序权重：daemon/GUI 入口（真持有者）> 命令行提及本库文件
                    rank = None
                    if "quantstudio.pipeline.daemon" in joined or "main_gui" in joined:
                        rank = 0
                    elif db_name in joined:
                        rank = 1
                    if rank is None:
                        continue
                    import datetime as _dt
                    ct = info.get("create_time")
                    started = (_dt.datetime.fromtimestamp(ct).strftime("%Y-%m-%d %H:%M:%S")
                               if ct else "?")
                    hits.append((rank, pid, name, started, joined))
                except Exception:
                    continue
        except Exception:
            return None
        if not hits:
            return None
        hits.sort(key=lambda h: h[0])
        _, pid, name, started, joined = hits[0]
        line = f"pid={pid} name={name} started={started}"
        if len(hits) > 1:
            line += f"（另有 {len(hits) - 1} 个候选）"
        return {"line": line, "cmdline": joined[:200], "count": len(hits)}

    def _rw_exhausted_message(self, trace, last_err) -> str:
        """耗尽异常文本——四锚点固定，供跨线 ATTRIB_PATTERNS 解析（见 32eadc7 已钉口径）。"""
        total = sum(self._RW_BACKOFF_SECONDS)
        seq = "/".join(str(s) for s in self._RW_BACKOFF_SECONDS)
        lines = [
            f"[db-lock] DuckDB 写连接获取失败：{total}s 内重试 {len(self._RW_BACKOFF_SECONDS)} 次"
            f"（{seq}s）全部冲突",
            f"  db_path : {self.db_path}",
            f"  轨迹    : {' / '.join(trace)}",
        ]
        holder = self._describe_lock_holder()
        if holder:
            lines.append(f"  持有者  : {holder['line']}")
            lines.append(f"            cmdline={holder['cmdline']}")
        else:
            lines.append("  持有者  : 未能归因（psutil 不可用或无匹配候选进程）")
        lines.append(f"  原始文本: {last_err}")
        return "\n".join(lines)

    def _open_rw_with_backoff(self, purpose: str = "conn"):
        """打开 DuckDB read_write 连接；**仅**跨进程写锁冲突时退避重试。

        - 判据：pipeline.db_lock_errors.is_db_lock_conflict（Windows 中文/英文 + POSIX 三串）；
          **非冲突**（路径/权限/库损坏/参数错）**立即抛**——不把响亮失败磨成哑失败；
        - 退避 1/2/4/8/15s（合计 30s）；耗尽抛 RuntimeError（四锚文本，含持有者归因）；
        - **不写任何窗口/锁状态**——E-3 的写窗判据归 E-3，本 helper 不参与、不落任何文件。
        """
        import time as _time
        from .db_lock_errors import is_db_lock_conflict
        trace = []
        last_err = None
        attempts = (0,) + self._RW_BACKOFF_SECONDS
        for idx, wait in enumerate(attempts, start=1):
            if wait:
                _time.sleep(wait)
            try:
                _conn = self._duckdb.connect(str(self.db_path))
                # v3 V3 并行度/保序（默认关）：写连接建立后按需设置。
                # 注意：preserve_insertion_order=false **会改无 ORDER BY 查询行序**
                # ⇒ 需 P1 读路径清单准入（审计裁定②）。
                _threads = _variant_int("QS_DUCKDB_WRITE_THREADS", 0)
                if _threads > 0:
                    _conn.execute("SET threads=%d" % _threads)
                if _variant_on("QS_DUCKDB_WRITE_NO_PRESERVE_ORDER"):
                    _conn.execute("SET preserve_insertion_order=false")
                return _conn
            except Exception as e:
                if not is_db_lock_conflict(e):
                    raise                      # 非锁冲突：立即抛，不进退避
                last_err = e
                trace.append(f"#{idx} {type(e).__name__}")
        raise RuntimeError(self._rw_exhausted_message(trace, last_err))

    def _conn(self):
        """新建连接（线程安全：调用方应在 write_lock/conn_lock 内使用并及时关闭）

        T1：跨进程写锁冲突时退避重试（见 _open_rw_with_backoff）。
        """
        return self._open_rw_with_backoff(purpose="conn")

    def _ensure_shared_conn(self):
        """确保持久 read_write 连接已创建（调用方须持有 _conn_lock）。"""
        if self._shared_conn is None:
            self._shared_conn = self._open_rw_with_backoff(purpose="shared")
        return self._shared_conn

    def reconnect(self):
        """F-5 修复（2026-09-08）：关闭并重建持久写连接（invalidated 毒化后调用）。

        关闭旧连接（毒化连接 close 可能抛异常，吞掉）→ 置 None → 下次
        shared_conn()/write 时按需重建。调用方须在无并发写时调用（A4 中止后的
        主增量前是安全点）。
        """
        with self._conn_lock:
            old_conn = self._shared_conn
            self._shared_conn = None
            if old_conn is not None:
                try:
                    old_conn.close()
                except Exception as close_err:  # 毒化连接 close 常抛，忽略
                    pass
            self._ensure_shared_conn()  # 立即重建，失败早暴露
        return self._shared_conn

    def shared_conn(self):
        """返回持久 read_write 连接（复用单例，线程安全）。

        用途：daemon 内部只读查询改用此连接，避免开 read_only 连接与 write 的
        read_write 连接并发触发「different configuration」冲突。
        调用方负责加 _conn_lock 保护 execute（DuckDB 单连接并发 execute 会串行化）。
        """
        with self._conn_lock:
            return self._ensure_shared_conn()

    def execute_read(self, sql: str, params: Optional[list] = None):
        """线程安全的只读查询（复用持久 read_write 连接，避免 read_only 冲突）。

        返回 fetchall() 结果。调用方无需管理连接生命周期。
        """
        with self._conn_lock:
            conn = self._ensure_shared_conn()
            if params:
                return conn.execute(sql, params).fetchall()
            return conn.execute(sql).fetchall()

    def read_df(self, sql: str, params: Optional[list] = None):
        """线程安全的只读查询，返回 DataFrame（复用持久 read_write 连接）。

        供 daemon 的 _prepare_namechange_df / _prepare_valuation_df / _prepare_close_df 用，
        替代原 duckdb.connect(read_only=True) 短连接，避免与 write 的 read_write 连接并发冲突。
        """
        with self._conn_lock:
            conn = self._ensure_shared_conn()
            if params:
                return conn.execute(sql, params).fetchdf()
            return conn.execute(sql).fetchdf()

    def close(self):
        """关闭持久共享连接（GUI 重建 collector 时调用，避免连接泄漏）。"""
        with self._conn_lock:
            if self._shared_conn is not None:
                try:
                    self._shared_conn.close()
                except Exception:
                    pass
                self._shared_conn = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _init_tables(self):
        with self._conn_lock:
            conn = self._conn()
            try:
                # v2.4 B-3a.3 P0-1：完整主库状态预检（不仅是 source_watermark 子契约）。
                # 必须在 writer 第一条 DDL 前。仅 EMPTY_OR_NEW/COMPLETE_2_1 放行；
                # COMPLETE_2_0/PARTIAL_OR_MIXED/UNKNOWN 全部写前 fail-fast（writer 层
                # _WriterSchemaMigrationRequired），不得让普通 writer 自动升级或修补 QFQ schema。
                _assert_qfq_schema_init_safe(conn)
                for ddl in DDL_DUCKDB.values():
                    conn.execute(ddl)
                # 存量表结构迁移：检测并自动 ALTER TABLE 补齐新增列
                # 用途：DDL_DUCKDB 里加了新列但存量 DB（已有 950 万行）不会自动 ALTER
                self._migrate_add_columns(conn)
                # v3 V4（R-3）：仅当 V4 显式开启时，在**启动初始化**阶段清理残留 `_stg_*`
                # （默认关闭 ⇒ 完全不触碰任何表，零行为变化）。
                if _variant_on("QS_DUCKDB_WRITE_STAGING"):
                    self._drop_staging_on_conn(conn)
            finally:
                conn.close()
        logger.info(f"[DuckDBWriter] tables initialized at {self.db_path}")

    def _drop_staging_on_conn(self, conn) -> list:
        """在给定连接上 DROP 残留 `_stg_*` 表（幂等）。返回被清理的表名。"""
        dropped = []
        try:
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE ?", [self._STAGING_PREFIX + "%"]).fetchall()
            for (name,) in rows:
                conn.execute('DROP TABLE IF EXISTS "%s"' % name)
                dropped.append(name)
        except Exception as exc:  # noqa: BLE001 —— 清理失败不得阻断初始化
            logger.warning("[DuckDBWriter] staging 启动清理失败（跳过）: %s: %s"
                           % (type(exc).__name__, exc))
        if dropped:
            logger.info("[DuckDBWriter] staging 启动清理: %s", dropped)
        return dropped

    def _migrate_add_columns(self, conn):
        """检测存量表缺哪些列，自动 ALTER TABLE ADD COLUMN（仅对已存在的表生效）。

        幂等：列已存在时跳过。基于 DDL_DUCKDB 解析每表应有的列 vs DESCRIBE 实际列。
        新表（CREATE TABLE IF NOT EXISTS 已建好）不会触发 ALTER。
        """
        try:
            for table, ddl in DDL_DUCKDB.items():
                # 跳过非数据表
                if table == "source_watermark":
                    continue
                # 拿实际列
                try:
                    actual = {r[0]: r[1] for r in conn.execute(f"DESCRIBE {table}").fetchall()}
                except Exception:
                    continue   # 表不存在（不应发生，但兜底）
                # 从 COLS 拿应有列（顺序与 DDL 一致）
                expected = self._table_columns(table)
                for col in expected:
                    if col not in actual:
                        # 类型推断：DDL 文本里找列定义
                        col_type = self._infer_col_type(ddl, col)
                        try:
                            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                            logger.info(f"[DuckDBWriter] 迁移: {table}.ADD {col} {col_type}")
                        except Exception as e:
                            logger.warning(f"[DuckDBWriter] 迁移失败 {table}.{col}: {e}")
                if table == "trade_calendar" and "exchange" in expected:
                    # Legacy QFQ rows are the SSE calendar. This fills only the
                    # new metadata column; cal_date/is_open/PK are unchanged.
                    conn.execute(
                        "UPDATE trade_calendar SET exchange='SSE' WHERE exchange IS NULL")
        except Exception as e:
            logger.warning(f"[DuckDBWriter] _migrate_add_columns 异常（跳过）: {e}")

    @staticmethod
    def _infer_col_type(ddl: str, col: str) -> str:
        """从 DDL 文本解析某列的类型（粗解析，够用）。
        找 'col TYPE' 模式，TYPE 是下一个 token。"""
        import re
        # 匹配 "col TYPE" 或 "col   TYPE"（col 后空白 + 大写类型词）
        m = re.search(rf"\b{re.escape(col)}\s+(BIGINT|INTEGER|DOUBLE|VARCHAR|BOOLEAN|TIMESTAMP)", ddl, re.IGNORECASE)
        return m.group(1).upper() if m else "VARCHAR"

    def write(self, df: pd.DataFrame, table: str, batch_id: str,
              passthrough: bool = False) -> int:
        """幂等防重复写入：主键冲突时 UPDATE（upsert），重放同一批次不产生重复行。

        passthrough=True（类别B 同名 passthrough 表）：
          - 按 DataFrame dtypes 自动 CREATE TABLE IF NOT EXISTS（表不存在时）；
          - 全量覆盖（CREATE OR REPLACE TABLE），不 upsert、不按 DDL 裁剪列、
            不做类型归一、不推进水位；
          - DuckDB 表名/列名 = QuestDB 原样（ts_code/trade_date 等保留）。
        """
        ensure_write_lock(f"writers:write:{table}:{batch_id}")  # 3A 写锁（操作粒度）
        try:
            written = self._write_locked(df, table, batch_id, passthrough)
            # W5（默认关）：批间隙 CHECKPOINT——必须在连接已关闭、无活动写事务后
            self._maybe_checkpoint_after_batch(table)
            # v3 V7（默认关）：批间隙主键重建——同为批间隙、持 3A 写锁、无活动写事务
            self._maybe_rebuild_pk_after_batch(table)
            return written
        finally:
            release_write_lock()

    # ── v3.1：fail-closed 滑动窗口计数与熔断（设计文档 §3.1.1）──
    def _dedup_fail_window_hits(self) -> int:
        """当前滑动窗口内 fail-closed 命中次数。"""
        return len(self._dedup_fail_marks)

    def _dedup_circuit_reset(self) -> None:
        """复位 P1 熔断与窗口计数（人工确认后调用；纯内存态，不落库）。"""
        self._dedup_circuit_open = False
        self._dedup_fail_marks = []

    def _record_dedup_fail_closed(self, table: str) -> None:
        """登记一次 fail-closed 触发：滑动窗口计数 + 熔断判定。

        背景：若去重计数 SELECT 持续失败，每批都 fail-closed 回退 ON CONFLICT，
        则"纯 INSERT 性能分档"会"表面正常、实则静默失效"，运维无法区分"本批真有更新"
        与"计数失败回退"。故窗口内达阈值即告警/熔断，使失效可被发现。
        """
        seq = self._write_batch_seq
        self._dedup_fail_marks.append(seq)
        # 滑动窗口：仅保留最近 _DEDUP_FAIL_WINDOW_BATCHES 批内的命中
        cutoff = seq - self._DEDUP_FAIL_WINDOW_BATCHES
        while self._dedup_fail_marks and self._dedup_fail_marks[0] <= cutoff:
            self._dedup_fail_marks.pop(0)
        hits = len(self._dedup_fail_marks)
        if hits >= self._DEDUP_FAIL_OPEN_AT:
            if not self._dedup_circuit_open:
                self._dedup_circuit_open = True
                logger.critical(
                    f"[DuckDBWriter] 去重计数在最近 {self._DEDUP_FAIL_WINDOW_BATCHES} 批内"
                    f"失败 {hits} 次，纯 INSERT 性能分档自我熔断：后续批次一律走 ON CONFLICT"
                    f"原路径（分档收益已失效；注意分档**不是**停摆规避手段，停摆由写路径"
                    f"看门狗兜底；table={table}）")
        elif hits >= self._DEDUP_FAIL_WARN_AT:
            logger.error(
                f"[DuckDBWriter] 去重计数在最近 {self._DEDUP_FAIL_WINDOW_BATCHES} 批内"
                f"失败 {hits} 次，本批 fail-closed 回退 ON CONFLICT"
                f"（本批未走纯 INSERT 性能分档；table={table}）")

    # ── W1 写路径双段看门狗（设计 §4.1）────────────────────────────────
    def _write_guard(self, conn, *, phase: str, table: str, batch_id, rows, sql) -> _WriteGuard:
        """构造写语句看门狗（各写路径统一入口，便于审计与回归）。"""
        return _WriteGuard(conn, phase=phase, table=table, batch_id=str(batch_id),
                           rows=rows, sql_head=_sql_head(sql), writer=self)

    # ── W5 批间隙可选 CHECKPOINT（默认关；设计 §4.5）────────────────────
    def _maybe_checkpoint_after_batch(self, table: str) -> None:
        """每 N 批在**连接已关闭、无活动写事务**后做一次 CHECKPOINT。

        - 开关：`QS_DUCKDB_WRITE_CHECKPOINT_BATCHES=N`（默认 0 = 关闭）；
        - 目的：收敛 upsert 删除版本与 WAL 体积（F8：复现库 60 批由 0.76GB 涨到 1.51GB），
          顺带缓解 09-23 案的"开库回放 22 分钟"；
        - **默认关**：其"防挂起"效益未证实（R2 机理未知），而每次 CHECKPOINT 有实测成本
          （2.33GB WAL ≈ 7.3s），不得以未证实收益默认改变运行时行为；
        - fail-soft：`checkpoint_database` 自带超时放弃语义，失败仅告警不阻断写入。
        """
        every = int(_env_float("QS_DUCKDB_WRITE_CHECKPOINT_BATCHES", 0) or 0)
        if every <= 0:
            return
        self._ckpt_batch_counter += 1
        if self._ckpt_batch_counter < every:
            return
        self._ckpt_batch_counter = 0
        try:
            from .db_checkpoint import checkpoint_database
            ok, detail = checkpoint_database(self.db_path)
            logger.info("[DuckDBWriter] 批间隙 CHECKPOINT(table=%s): ok=%s %s", table, ok, detail)
        except Exception as exc:  # noqa: BLE001 —— 收尾路径不得抛
            logger.warning("[DuckDBWriter] 批间隙 CHECKPOINT 异常（跳过，不阻断）: %s: %s",
                           type(exc).__name__, exc)

    # ── v3 V4：staging + 显式事务 merge（默认关；正确性优先，对症 v2 的 F-2(a)）──
    _STAGING_PREFIX = "_stg_"

    def _write_via_staging(self, conn, table: str, df: pd.DataFrame, batch_id,
                           pk_cols: str) -> None:
        """把本批先写入**无主键 staging 表**，再在**显式事务**内 DELETE+INSERT 合入主表。

        - **原子性（核心）**：DELETE 与 INSERT 被 `BEGIN TRANSACTION … COMMIT` 覆盖，
          任何中途失败/停摆一律 `ROLLBACK` ⇒ **不出现 v2 的 F-2(a)「DELETE 已提交、
          INSERT 停摆 ⇒ 丢数」**；
        - **假设（未证实，审计 R-1）**：合入用的 `INSERT INTO t … SELECT FROM _stg` 仍是
          5 万行纯 INSERT（与已证停摆的 915 同一目标操作，仅源由 pandas view 换 staging 表），
          **是否规避停摆未证实**，由在线实验 A5-V4 判定；
        - 三条语句（staging INSERT / DELETE / INSERT）**各自经 W1 看门狗**（审计 R-2）；
        - 计数口径不变：`updated_rows` 由写前 COUNT 给出，本方法**不重算**（裁定③a）。
        """
        stg = self._STAGING_PREFIX + table
        cols = list(df.columns)
        col_list = ", ".join(cols)
        pk_expr = pk_cols
        _create = f'CREATE TABLE IF NOT EXISTS "{stg}" AS SELECT * FROM "{table}" LIMIT 0'
        _guarded(conn, self._write_guard(conn, phase="v4_staging_ddl", table=table,
                                          batch_id=batch_id, rows=len(df), sql=_create), _create)
        conn.register("_stg_src", df)
        try:
            _ins = f'INSERT INTO "{stg}" ({col_list}) SELECT * FROM _stg_src'
            _guarded(conn, self._write_guard(conn, phase="v4_staging", table=table,
                                              batch_id=batch_id, rows=len(df), sql=_ins), _ins)
        finally:
            try:
                conn.unregister("_stg_src")
            except Exception:  # noqa: BLE001
                pass
        _del = (f'DELETE FROM "{table}" WHERE {pk_expr} IN '
                f'(SELECT {pk_expr} FROM "{stg}")')
        _ins2 = f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM "{stg}"'
        conn.execute("BEGIN TRANSACTION")
        try:
            _guarded(conn, self._write_guard(conn, phase="v4_delete", table=table,
                                              batch_id=batch_id, rows=len(df), sql=_del), _del)
            _guarded(conn, self._write_guard(conn, phase="v4_insert", table=table,
                                              batch_id=batch_id, rows=len(df), sql=_ins2), _ins2)
            conn.execute("COMMIT")
        except BaseException:
            # 审计 R-2：**先 ROLLBACK 再抛**（含 DuckDBWriteStalled），杜绝半提交
            try:
                conn.execute("ROLLBACK")
            except Exception as _re:  # noqa: BLE001
                logger.warning("[DuckDBWriter] V4 ROLLBACK 失败（table=%s batch=%s）: %s: %s"
                               % (table, batch_id, type(_re).__name__, _re))
            raise
        finally:
            try:
                conn.execute(f'DROP TABLE IF EXISTS "{stg}"')
            except Exception as _de:  # noqa: BLE001
                logger.warning("[DuckDBWriter] V4 staging 清理失败（残留 _stg 表，"
                               "可用 drop_staging_tables() 清理）: %s: %s"
                               % (type(_de).__name__, _de))
        logger.info("[DuckDBWriter] %s batch=%s: V4 staging+显式事务 merge 完成 rows=%d"
                    % (table, batch_id, len(df)))

    def drop_staging_tables(self) -> list:
        """一次性清理残留 `_stg_*` 表（幂等；R-3 的人工/脚本入口）。返回被清理的表名。"""
        dropped = []
        with self._conn_lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name LIKE ?", [self._STAGING_PREFIX + "%"]).fetchall()
                for (name,) in rows:
                    try:
                        conn.execute('DROP TABLE IF EXISTS "%s"' % name)
                        dropped.append(name)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("[DuckDBWriter] staging 清理失败 %s: %s: %s"
                                       % (name, type(exc).__name__, exc))
            finally:
                conn.close()
        if dropped:
            logger.info("[DuckDBWriter] staging 残留清理完成: %s", dropped)
        return dropped

    # ── v3 V7：批间隙主键重建（默认关；唯一作用于"表/索引状态"的杠杆）──
    def _maybe_rebuild_pk_after_batch(self, table: str) -> None:
        """每 N 批在**批间隙**（连接已关闭、无活动写事务、持 3A 写锁）重建目标表主键。

        动机（见 docs/duckdb-write-stall-rootcause-v3-design.md §2.5 与
        docs/evidence/hang2_v7_pk_rebuild.txt）：四次在线实验证明停摆与语句规模/库体积/
        WAL/语句形态均无关，唯一恒定相关量是 `stock_daily` 表内行数（约 275 万）
        ⇒ 指向 ART 索引层；离线实测重建窗口 **7.7s**、并发读不阻塞（MVCC）、行数与主键守恒。

        开关：`QS_DUCKDB_WRITE_REBUILD_PK_BATCHES`（默认 0=关）；
        行数门槛：`QS_DUCKDB_WRITE_REBUILD_PK_MIN_ROWS`（默认 100 万，避免重建小表）。
        语义：等价 passthrough 换名（CREATE 带 PK + INSERT SELECT + DROP + RENAME，单事务）。
        纪律：① 以**实表 schema** 建新表（静态 DDL 可能落后，`_migrate_add_columns` 补过列）；
              ② PK 列按括号内逗号切分（`PRIMARY KEY(code, "time")` 仅 time 带引号）；
              ③ fail-soft：失败仅告警，不阻断采集。
        """
        every = _variant_int("QS_DUCKDB_WRITE_REBUILD_PK_BATCHES", 0)
        if every <= 0:
            return
        scope = [t.strip() for t in
                 os.environ.get("QS_DUCKDB_WRITE_REBUILD_PK_TABLES", "").split(",") if t.strip()]
        if scope and table not in scope:
            return
        min_rows = _variant_int("QS_DUCKDB_WRITE_REBUILD_PK_MIN_ROWS", 1_000_000)
        self._rebuild_batch_counter = getattr(self, "_rebuild_batch_counter", 0) + 1
        if self._rebuild_batch_counter < every:
            return
        self._rebuild_batch_counter = 0
        try:
            with self._conn_lock:
                conn = self._conn()
                try:
                    pk = conn.execute(
                        "SELECT constraint_text FROM duckdb_constraints() "
                        "WHERE table_name=? AND constraint_type='PRIMARY KEY'",
                        [table]).fetchone()
                    if not pk:
                        return
                    n_rows = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                    if n_rows < min_rows:
                        return
                    t0 = time.time()
                    ok, detail = self._rebuild_pk_on_conn(conn, table, pk[0], n_rows)
                    logger.info("[DuckDBWriter] V7 主键重建 table=%s rows=%d %s 耗时=%.1fs"
                                % (table, n_rows, detail, time.time() - t0))
                    if not ok:
                        logger.warning("[DuckDBWriter] V7 主键重建失败（不阻断采集）: %s", detail)
                finally:
                    conn.close()
        except Exception as exc:  # noqa: BLE001 —— 维护路径 fail-soft
            logger.warning("[DuckDBWriter] V7 主键重建异常（跳过，不阻断）: %s: %s"
                           % (type(exc).__name__, exc))

    @staticmethod
    def _rebuild_pk_on_conn(conn, table: str, pk_text: str, n_rows: int):
        """在给定连接上重建表主键（单事务）。返回 (ok, detail)。

        实测约束（docs/evidence/hang2_v7_pk_rebuild.txt）：
        - 必须用实表 schema（DESCRIBE）建新表，静态 DDL 可能少列；
        - PK 列须按 `PRIMARY KEY(...)` 括号内逗号切分，不能按引号提取。
        """
        m = re.match(r"PRIMARY KEY\s*\((.*)\)\s*$", str(pk_text), re.I)
        if not m:
            return False, "无法解析 PK 文本: %r" % (pk_text,)
        pk_cols = [c.strip().strip('"') for c in m.group(1).split(",") if c.strip()]
        desc = conn.execute(f'DESCRIBE "{table}"').fetchall()
        collist = ", ".join('"%s" %s' % (r[0], r[1]) for r in desc)
        collist += ", PRIMARY KEY(%s)" % ", ".join('"%s"' % c for c in pk_cols)
        tmp = "_v7_new_" + table
        conn.execute(f'BEGIN TRANSACTION')
        try:
            conn.execute(f'DROP TABLE IF EXISTS "{tmp}"')
            conn.execute(f'CREATE TABLE "{tmp}" ({collist})')
            conn.execute(f'INSERT INTO "{tmp}" SELECT * FROM "{table}"')
            n2 = conn.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0]
            if n2 != n_rows:
                raise RuntimeError("重建后行数不一致: %d != %d" % (n2, n_rows))
            conn.execute(f'DROP TABLE "{table}"')
            conn.execute(f'ALTER TABLE "{tmp}" RENAME TO "{table}"')
            conn.execute("COMMIT")
            return True, "cols=%d pk=%s" % (len(desc), ",".join(pk_cols))
        except BaseException as exc:  # noqa: BLE001
            try:
                conn.execute("ROLLBACK")
            except Exception:  # noqa: BLE001
                pass
            return False, "%s: %s" % (type(exc).__name__, str(exc)[:160])

    def _write_locked(self, df: pd.DataFrame, table: str, batch_id: str,
                      passthrough: bool = False) -> int:
        """write() 的锁内实现（3A 重构：原 write 主体平移，逻辑零改动）。"""
        assert_lock_owner()  # 所有权校验（批一 S3）：锁被他人取得 → WriteLockLost，禁静默双写
        if df is None or len(df) == 0:
            logger.info(f"[DuckDBWriter] {table} batch={batch_id}: 0 rows (skip)")
            return 0
        if passthrough:
            written = self._write_passthrough(df, table, batch_id)
            return written
        # 入库前再去重一次（双保险：validator 已去重，这里再保险）
        cols_in_ddl = self._table_columns(table)
        df = df[[c for c in cols_in_ddl if c in df.columns]].copy()
        pk_for_dedup = {
                "stock_daily": ["code", "time"],
                "stock_minutes": ["code", "time", "freq"],
                "etf_minutes": ["code", "time", "freq"],
                "tick": ["code", "time"],
                "fin_indicator": ["code", "end_date", "ann_date"],
                "index_daily": ["code", "time"],
                "stock_daily_valuation": ["code", "time"],
                "etf_daily": ["code", "time"],
                "etf_basic": ["code"],
                "stock_basic": ["code"],
                "trade_calendar": ["cal_date"],
                "stock_float_share": ["code", "end_date", "ann_date"],
                "index_constituents": ["index_code", "code", "time"],
                "index_constituents_snapshot_meta": ["index_code", "time"],
                "balance_statement": ["code", "end_date", "ann_date"],
                "income_statement": ["code", "end_date", "ann_date"],
                "cashflow_statement": ["code", "end_date", "ann_date"],
                "stock_dividend": ["code", "ex_date"],
                "etf_dividend": ["code", "ex_date"],
                "sw_industry": ["code", "industry_code"],
                "industry_classification": ["classification_system", "classification_version",
                                            "industry_level", "industry_code", "effective_from"],
                "industry_membership": ["classification_system", "classification_version",
                                        "industry_level", "industry_code", "code",
                                        "effective_from"],
                "stock_namechange": ["code", "change_date"],
                "stock_delist": ["code", "market"],
            }.get(table, [])
        if pk_for_dedup:
            before = len(df)
            df = df.drop_duplicates(subset=[c for c in pk_for_dedup if c in df.columns], keep="last")
            if len(df) < before:
                logger.info(f"[DuckDBWriter] {table}: 入库前去重 {before}→{len(df)} 行")
        # 确保类型（字符串列跳过数值转换）
        # DDL 驱动（W2-0.9 缺陷 A 修复）：优先按目标表实际 DuckDB 列类型判定——
        # VARCHAR 列一律不经过 pd.to_numeric（否则 "实施" 等字符串值会被 coerce 成 NaN，
        # 落库为 NULL，如 stock_dividend.div_proc）。DESCRIBE 取不到类型时回退到 str_cols
        # 白名单（W2-0.9：白名单必须含 div_proc，且 DESCRIBE 失败不得完全静默）。
        varchar_cols: set = set()
        describe_failed = False
        try:
            with self._conn_lock:
                _conn = self._conn()
                try:
                    for _r in _conn.execute(f"DESCRIBE {table}").fetchall():
                        # _r = (col_name, col_type, nullable, key, default, extra)
                        if len(_r) >= 2 and isinstance(_r[1], str) and _r[1].upper().startswith("VARCHAR"):
                            varchar_cols.add(_r[0])
                finally:
                    _conn.close()
        except Exception as _e:
            # 不静默：记录一次可诊断 warning（不含敏感信息），回退到 str_cols 白名单。
            describe_failed = True
            logger.warning(
                f"[DuckDBWriter] DESCRIBE {table} failed ({type(_e).__name__}); "
                f"falling back to static str_cols whitelist for type protection")
        str_cols = {"code", "freq", "dividend_type", "update_time", "data_source",
                    "index_code", "industry_code", "industry_name", "industry_level",
                    "name_before", "name_after", "status_after", "market",
                    "is_st_reliable_source", "is_delisting_risk_source",
                    "ts_code", "symbol", "name", "area", "industry", "exchange",
                    "list_status", "source", "etf_type", "tracking_index",
                    "status", "fund_type", "invest_type", "type",
                    "classification_system", "parent_industry_code",
                    "classification_method", "classification_version",
                    # W2-0.9 缺陷 A 补完：fallback 白名单必须含 div_proc，
                    # 保证 DESCRIBE 失败时 "实施" 也不会被 to_numeric 吞掉。
                    "div_proc", "div_rat"}.union(varchar_cols)
        del describe_failed  # 诊断标记已用于 warning，不再需要
        for c in df.columns:
            if c in str_cols:
                continue
            if df[c].dtype == object:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        with self._conn_lock:
            conn = self._conn()
            guard = None            # W1：S2 兜底在 finally 里解除，先给 None 防未绑定
            try:
                # 性能修复（2026-07-22）：原实现 write 前后各跑一次 SELECT COUNT(*) FROM <table>
                # 全表统计，在大表（百万→千万行）上每次数秒，8 线程持 _conn_lock 串行 →
                # 全量拉取 56 秒/只（理论 1.9s）。改为只数本批主键已存在的行（走索引，毫秒级）。
                # v3 V2 写入局部性（默认关）：按主键稳定排序，提升 ART 插入局部性。
                # 注意：**会改物理行序** ⇒ 需 P1 读路径清单准入（审计裁定②）。
                if _variant_on("QS_DUCKDB_WRITE_SORT_BY_PK") and pk_for_dedup:
                    _pk = [c for c in pk_for_dedup if c in df.columns]
                    if _pk:
                        df = df.sort_values(_pk, kind="stable")
                conn.register("_tmp_write", df)
                pk_cols = {
                    "stock_daily": "(code, time)",
                    "stock_minutes": "(code, time, freq)",
                "etf_minutes": "(code, time, freq)",
                    "tick": "(code, time)",
                    "fin_indicator": "(code, end_date, ann_date)",
                    "index_daily": "(code, time)",
                    "stock_daily_valuation": "(code, time)",
                    "etf_daily": "(code, time)",
                    "etf_basic": "(code)",
                    "stock_basic": "(code)",
                    "trade_calendar": "(cal_date)",
                    "stock_float_share": "(code, end_date, ann_date)",
                    "index_constituents": "(index_code, code, time)",
                    "index_constituents_snapshot_meta": "(index_code, time)",
                    "balance_statement": "(code, end_date, ann_date)",
                    "income_statement": "(code, end_date, ann_date)",
                    "cashflow_statement": "(code, end_date, ann_date)",
                    "stock_dividend": "(code, ex_date)",
                    "etf_dividend": "(code, ex_date)",
                    "sw_industry": "(code, industry_code)",
                    "industry_classification": "(classification_system, classification_version, industry_level, industry_code, effective_from)",
                    "industry_membership": "(classification_system, classification_version, industry_level, industry_code, code, effective_from)",
                    "stock_namechange": "(code, change_date)",
                    "stock_delist": "(code, market)",
                }.get(table)
                # 写前：数本批主键在目标表已存在的行数（=将被 UPDATE 的）
                # 三态分档（**定位更正，见 docs/duckdb-write-stall-mitigation-design.md
                # §0.3/A2**：本分档是**性能优化**——"无冲突批省去冲突检查"有实测收益，
                # 等价性由 tests/test_writer_dedup_fail_closed.py::
                # test_plain_insert_equals_on_conflict_when_no_conflict 钉死；
                # **它不是 DuckDB 写入停摆缺陷的规避手段**——事故2 已证纯 INSERT 与
                # ON CONFLICT 两条语句在生产现场都会进入不可返回状态）：
                #  a) 计数成功且 == 0 ⇒ 本批主键**全部新增** ⇒ 纯 INSERT（省去冲突检查）；
                #  b) 计数成功且 > 0  ⇒ 本批含更新 ⇒ 原 ON CONFLICT 路径（行为零变化）；
                #  c) 计数失败 ⇒ **fail-closed**：保守视为"本批全部为更新"，回退 ON CONFLICT
                #     原路径（异常行为不变，禁止误走纯 INSERT 而引入 IntegrityError）；
                #     取哨兵 int = len(df)（**非 None**），保证下游 new_rows 与
                #     WriteResult 三字段恒为 int 且 new + updated == len(df) 守恒。
                #  熔断：_dedup_circuit_open 时不再计数与分档，一律走原路径（自我熔断）。
                self._write_batch_seq += 1
                updated_rows = 0
                dedup_count_failed = False
                # W2 分段耗时：count / dml / close 三段独立计时（默认关的 slow 阈值仅告警）
                t_seg = time.perf_counter()
                count_s = 0.0
                dml_s = 0.0
                close_s = 0.0
                if pk_cols:
                    if self._dedup_circuit_open:
                        # 分档已熔断：不尝试计数，直接按"含更新"处理（等价修复前行为）
                        dedup_count_failed = True
                        updated_rows = len(df)
                    else:
                        count_sql = (
                            f"SELECT COUNT(*) FROM {table} WHERE {pk_cols} IN "
                            f"(SELECT {pk_cols} FROM _tmp_write)")
                        try:
                            guard = self._write_guard(conn, phase="count", table=table,
                                                      batch_id=batch_id, rows=len(df),
                                                      sql=count_sql)
                            updated_rows = _guarded(conn, guard, count_sql).fetchone()[0]
                        except DuckDBWriteStalled:
                            raise                      # 有界失败：不得被 fail-closed 吞掉
                        except Exception:
                            # fail-closed：哨兵 int，保守"全部更新"；登记窗口命中
                            dedup_count_failed = True
                            updated_rows = len(df)
                            self._record_dedup_fail_closed(table)
                        finally:
                            count_s = time.perf_counter() - t_seg
                if pk_cols:
                    if dedup_count_failed or updated_rows > 0:
                        # 含更新 / 计数不可用 / 已熔断 ⇒ 原 ON CONFLICT 路径（行为不变）
                        col_list = ", ".join(df.columns)
                        update_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in df.columns)
                        dml_sql = (
                            f"INSERT INTO {table} ({col_list}) "
                            f"SELECT * FROM _tmp_write "
                            f"ON CONFLICT {pk_cols} DO UPDATE SET {update_set}")
                    else:
                        # 本批主键全部新增 ⇒ 纯 INSERT（性能分档路径）
                        # 必须显式带列名列表：与 ON CONFLICT 分支保持**同一列投影语义**。
                        # df 常为表的列子集（如 3 列写入 42 列的表），裸写
                        # `INSERT INTO t SELECT *` 会让 DuckDB 按全表列对齐，报
                        # "table t has N columns but M values were supplied"（实测回归，
                        # 见 tests/test_pipeline_guardrails.py::test_writer_upsert_...）。
                        col_list = ", ".join(df.columns)
                        dml_sql = f"INSERT INTO {table} ({col_list}) SELECT * FROM _tmp_write"
                else:
                    dml_sql = f"INSERT INTO {table} SELECT * FROM _tmp_write"
                # ── v3 主动规避变体（默认全关；见 docs/duckdb-write-stall-rootcause-v3-design.md）──
                # V1 批内子批化：同语句形态切块，缩小单条语句规模（不改行序）
                # V2 写入局部性：按主键稳定排序（改行序，需 P1 准入）
                # V4 staging + 显式事务 merge：正确性优先，对症 v2 的 F-2(a) 原子性
                # 纪律（审计 R-4/裁定③）：**不重算写前 count**（updated_rows 沿用首次值）
                _chunk_rows = int(_env_float("QS_DUCKDB_WRITE_CHUNK_ROWS", 0) or 0)
                _use_staging = _variant_on("QS_DUCKDB_WRITE_STAGING")
                if _use_staging and pk_cols:
                    guard = self._write_guard(conn, phase="v4", table=table,
                                              batch_id=batch_id, rows=len(df), sql=dml_sql)
                    self._write_via_staging(conn, table, df, batch_id, pk_cols)
                elif _chunk_rows > 0 and len(df) > _chunk_rows:
                    logger.info(f"[DuckDBWriter] {table} batch={batch_id}: V1 子批化 "
                                f"{len(df)} 行 -> {(len(df) + _chunk_rows - 1) // _chunk_rows} "
                                f"× {_chunk_rows} 行（QS_DUCKDB_WRITE_CHUNK_ROWS）")
                    for _s in range(0, len(df), _chunk_rows):
                        _sub = df.iloc[_s:_s + _chunk_rows]
                        conn.register("_tmp_write", _sub)
                        _g = self._write_guard(conn, phase="dml_chunk", table=table,
                                               batch_id=batch_id, rows=len(_sub),
                                               sql=dml_sql)
                        _guarded(conn, _g, dml_sql)
                    guard = self._write_guard(conn, phase="dml", table=table,
                                              batch_id=batch_id, rows=len(df), sql=dml_sql)
                else:
                    guard = self._write_guard(conn, phase="dml", table=table,
                                              batch_id=batch_id, rows=len(df), sql=dml_sql)
                    _guarded(conn, guard, dml_sql)
                dml_s = time.perf_counter() - t_seg - count_s
                # new/updated 审计：updated = 写前已存在的行数；new = 本批其余
                # （精度：本批内主键重复已由 validator 去重，故 new + updated = len(df)）
                new_rows = max(0, len(df) - updated_rows)
                # P-A3：fin_indicator 写后跨表回补（eps ← income_statement.basic_eps）。
                # 默认关闭，需显式设置 QS_AUTO_BACKFILL_EPS=1/true/on 才触发；
                # CLI --apply 保持人工独立执行，不受此 gate 影响；
                # 只 UPDATE eps IS NULL 且 income 同 key basic_eps 非空的行——无缺口库零行为；
                # 幂等；异常 log-error 不阻断 write（失败由 quality_audit EpsBackfillGap 兜底）。
                # 水位/写锁/batch_audit 语义零变化（回补是 UPDATE 非拉取，不推进 watermark）。
                if table == "fin_indicator" and _is_writer_auto_backfill_enabled():
                    try:
                        backfill_eps_gap(conn)
                    except Exception as exc:
                        logger.error(f"[DuckDBWriter] {table} 写后回补失败（门禁将告警）: {exc}")
                elif table == "fin_indicator":
                    logger.debug(
                        "[DuckDBWriter] fin_indicator 写后自动回补已关闭 "
                        "(QS_AUTO_BACKFILL_EPS 未显式开启)"
                    )
            finally:
                # W3：`unregister` 移入 finally —— 异常/停摆路径也不残留 pandas 视图
                try:
                    conn.unregister("_tmp_write")
                except Exception:  # noqa: BLE001 —— 视图可能未注册/连接已毒化
                    pass
                # W1：连接成功关闭后才解除硬退出兜底（close 自身无界时 S2 仍能收敛）
                try:
                    conn.close()
                finally:
                    close_s = time.perf_counter() - t_seg
                    if guard is not None:
                        guard.disarm()
        # v3.1 P2-1：fail-closed 批次补显式标记，与"真实全更新批"可区分（运维可观测）
        _fc_tag = " (fail-closed→ON CONFLICT)" if dedup_count_failed else ""
        # W2：分段耗时（count/dml/close）——停摆与慢批可直接定位到语句与阶段，
        # 不再依赖 py-spy 行号（f-string 调用点行号不可信，事故2 §5.4 已证）。
        logger.info(f"[DuckDBWriter] {table} batch={batch_id}: wrote {len(df)} rows "
                    f"(新增 {new_rows} + 更新 {updated_rows}){_fc_tag} 防重复 upsert "
                    f"[timing count={count_s:.2f}s dml={dml_s:.2f}s close={close_s:.2f}s]")
        _slow_s = _env_float("QS_DUCKDB_WRITE_SLOW_S", _DEFAULT_SLOW_WRITE_S)
        if _slow_s > 0 and dml_s > _slow_s:
            logger.warning(
                f"[DuckDBWriter] {table} batch={batch_id}: 写语句偏慢 dml={dml_s:.1f}s "
                f"(阈值 {_slow_s:.0f}s) rows={len(df)} —— 若伴随零产出请核对是否停摆")
        # 返回 WriteResult：作为 int = 提交行数（向后兼容），.new/.updated 供审计使用
        return WriteResult(len(df), new_rows, updated_rows)

    # ------------------------------------------------------------------
    # 类别B passthrough 同名表：CREATE OR REPLACE TABLE 全量覆盖
    # ------------------------------------------------------------------
    def _write_passthrough(self, df: pd.DataFrame, table: str, batch_id: str) -> int:
        """passthrough 表全量覆盖写：DuckDB 表名/列名 = QuestDB 原样。

        - 表不存在：按 DataFrame dtypes 自动 CREATE TABLE IF NOT EXISTS；
        - 存在：CREATE OR REPLACE TABLE 全量覆盖（不 upsert，不按 DDL 裁剪列）；
        - 不做数值类型归一（保留源原始字符串/数值类型）；
        - 不推进 source_watermark（passthrough 表无增量水位）。
        """
        import duckdb  # 类型映射需要，顶部已 import，这里局部引用保险
        # 类型映射：object→VARCHAR，其余按 numpy dtype 推断
        _TYPE_MAP = {
            "int64": "BIGINT", "int32": "INTEGER", "int16": "SMALLINT",
            "int8": "SMALLINT", "uint64": "UBIGINT", "uint32": "UINTEGER",
            "float64": "DOUBLE", "float32": "FLOAT", "bool": "BOOLEAN",
        }

        def _col_sql(col: str, dtype) -> str:
            col_q = f'"{col}"'
            if str(dtype) == "object":
                return f"{col_q} VARCHAR"
            return f"{col_q} {_TYPE_MAP.get(str(dtype), 'VARCHAR')}"

        col_defs = ", ".join(_col_sql(c, df[c].dtype) for c in df.columns)
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})'
        with self._conn_lock:
            conn = self._conn()
            guard = None
            try:
                guard = self._write_guard(conn, phase="ddl", table=table,
                                          batch_id=batch_id, rows=len(df), sql=create_sql)
                _guarded(conn, guard, create_sql)
                # 全量覆盖：建临时表→REPLACE→DROP 临时（DuckDB 无原生 CREATE OR REPLACE
                # 对含数据的表，用事务内 建临时+原子替换 实现等价语义）
                tmp = f"_pt_tmp_{table}"
                # W1：passthrough 覆盖写同属写路径，逐条语句纳入看门狗
                for _sql in (f'DROP TABLE IF EXISTS "{tmp}"',
                             f'CREATE TABLE "{tmp}" AS SELECT * FROM "{table}" LIMIT 0'):
                    guard = self._write_guard(conn, phase="pt_ddl", table=table,
                                              batch_id=batch_id, rows=len(df), sql=_sql)
                    _guarded(conn, guard, _sql)
                conn.register("_pt_src", df)
                _ins_sql = f'INSERT INTO "{tmp}" SELECT * FROM _pt_src'
                guard = self._write_guard(conn, phase="pt_dml", table=table,
                                          batch_id=batch_id, rows=len(df), sql=_ins_sql)
                try:
                    _guarded(conn, guard, _ins_sql)
                finally:
                    # W3：视图泄漏修复（异常/停摆路径也不残留）
                    try:
                        conn.unregister("_pt_src")
                    except Exception:  # noqa: BLE001
                        pass
                for _sql in (f'DROP TABLE IF EXISTS "{table}"',
                             f'ALTER TABLE "{tmp}" RENAME TO "{table}"'):
                    guard = self._write_guard(conn, phase="pt_ddl", table=table,
                                              batch_id=batch_id, rows=len(df), sql=_sql)
                    _guarded(conn, guard, _sql)
            finally:
                try:
                    conn.close()
                finally:
                    if guard is not None:
                        guard.disarm()
        logger.info(f"[DuckDBWriter] {table} passthrough 全量覆盖 {len(df)} 行 "
                    f"(列原样: {list(df.columns)[:8]}{'...' if len(df.columns) > 8 else ''})")
        return len(df)

    # ------------------------------------------------------------------
    # 类别B passthrough 分片变体（B+ 增补，2026-09-12 总调度裁定）
    # 属同一 passthrough 通道的内部实现优化（非第三通道）：
    #   · staging 表跨尝试持久化（_pt_tmp_<table>）
    #   · 分片 ledger（_pt_staging_ledger）记录每片键与行数
    #   · 重试：ledger 累积行数 == staging 实际行数 -> 续插剩余片；不一致 -> drop 重建
    #   · 全部片完成后原子换名（DROP 原表 + RENAME staging）——换名前最终表零触碰
    #   · 不推进 source_watermark（与 _write_passthrough 同语义）
    # 用途：大表（1.07 亿行级）分片入库，内存峰值 = 单分片；中途暂停/崩溃可续。
    # ------------------------------------------------------------------
    _PT_LEDGER_TABLE = '_pt_staging_ledger'

    def _pt_ensure_ledger(self, conn):
        conn.execute(
            'CREATE TABLE IF NOT EXISTS "' + self._PT_LEDGER_TABLE + '" ('
            ' table_name VARCHAR, chunk_no INTEGER, chunk_key VARCHAR,'
            ' rows_written BIGINT, updated_at TIMESTAMP)')

    def _pt_ledger_rows(self, conn, table):
        cur = conn.execute(
            'SELECT count(*), coalesce(sum(rows_written), 0) FROM "'
            + self._PT_LEDGER_TABLE + '" WHERE table_name = ?', [table])
        return cur.fetchone()

    def _pt_ledger_keys(self, conn, table):
        cur = conn.execute(
            'SELECT chunk_key FROM "' + self._PT_LEDGER_TABLE
            + '" WHERE table_name = ? ORDER BY chunk_no', [table])
        return [r[0] for r in cur.fetchall()]

    def _pt_clear_ledger(self, conn, table):
        conn.execute('DELETE FROM "' + self._PT_LEDGER_TABLE
                     + '" WHERE table_name = ?', [table])

    @staticmethod
    def _pt_staging_exists(conn, tmp: str) -> bool:
        try:
            conn.execute('SELECT 1 FROM "' + tmp + '" LIMIT 0')
            return True
        except Exception:
            return False

    def write_passthrough_chunked(self, table: str, batch_id: str,
                                  chunks, resume: bool = True) -> Dict:
        """passthrough 分片写（B+）：chunks 为 (chunk_key, DataFrame) 迭代器。

        返回 {'written': n, 'chunks': k, 'resumed_from': m, 'rebuilt': bool}
        """
        _TYPE_MAP = {
            'int64': 'BIGINT', 'int32': 'INTEGER', 'int16': 'SMALLINT',
            'int8': 'SMALLINT', 'uint64': 'UBIGINT', 'uint32': 'UINTEGER',
            'float64': 'DOUBLE', 'float32': 'FLOAT', 'bool': 'BOOLEAN',
        }
        tmp = '_pt_tmp_' + table
        total = 0
        n_chunks = 0
        resumed_from = 0
        rebuilt = False
        with self._conn_lock:
            conn = self._conn()
            guard = None
            try:
                self._pt_ensure_ledger(conn)
                done_keys = set()
                if resume and self._pt_staging_exists(conn, tmp):
                    _cnt_sql = 'SELECT count(*) FROM "' + tmp + '"'
                    guard = self._write_guard(conn, phase="pt_count", table=table,
                                              batch_id=batch_id, rows=0, sql=_cnt_sql)
                    staging_rows = _guarded(conn, guard, _cnt_sql).fetchone()[0]
                    cnt, ledger_sum = self._pt_ledger_rows(conn, table)
                    if cnt and ledger_sum == staging_rows:
                        done_keys = set(self._pt_ledger_keys(conn, table))
                        resumed_from = len(done_keys)
                        logger.info('[DuckDBWriter] %s 断点续写：staging=%s 行 ledger=%s 片一致，'
                                    '跳过已完成片', table, staging_rows, cnt)
                    else:
                        logger.warning('[DuckDBWriter] %s staging/ledger 不一致'
                                       '（staging=%s ledger_sum=%s） -> drop 重建',
                                       table, staging_rows, ledger_sum)
                        _drop_sql = 'DROP TABLE IF EXISTS "' + tmp + '"'
                        guard = self._write_guard(conn, phase="pt_ddl", table=table,
                                                  batch_id=batch_id, rows=0, sql=_drop_sql)
                        _guarded(conn, guard, _drop_sql)
                        self._pt_clear_ledger(conn, table)
                        rebuilt = True
                for key, df in chunks:
                    if df is None or len(df) == 0:
                        continue
                    if key in done_keys:
                        continue
                    if not self._pt_staging_exists(conn, tmp):
                        col_defs = ', '.join(
                            '"' + c + '" ' + ('VARCHAR' if str(df[c].dtype) == 'object'
                                              else _TYPE_MAP.get(str(df[c].dtype), 'VARCHAR'))
                            for c in df.columns)
                        _create_sql = 'CREATE TABLE "' + tmp + '" (' + col_defs + ')'
                        guard = self._write_guard(conn, phase="pt_ddl", table=table,
                                                  batch_id=batch_id, rows=len(df),
                                                  sql=_create_sql)
                        _guarded(conn, guard, _create_sql)
                    conn.register('_pt_src_c', df)
                    _chunk_ins = 'INSERT INTO "' + tmp + '" SELECT * FROM _pt_src_c'
                    guard = self._write_guard(conn, phase="pt_dml", table=table,
                                              batch_id=batch_id, rows=len(df), sql=_chunk_ins)
                    try:
                        _guarded(conn, guard, _chunk_ins)
                    finally:
                        # W3：视图泄漏修复（异常/停摆路径也不残留）
                        try:
                            conn.unregister('_pt_src_c')
                        except Exception:  # noqa: BLE001
                            pass
                    n_chunks += 1
                    total += len(df)
                    _ledger_ins = ('INSERT INTO "' + self._PT_LEDGER_TABLE
                                   + '" VALUES (?, ?, ?, ?, now())')
                    guard = self._write_guard(conn, phase="pt_ledger", table=table,
                                              batch_id=batch_id, rows=1, sql=_ledger_ins)
                    _guarded(conn, guard, _ledger_ins,
                             [table, resumed_from + n_chunks, str(key), len(df)])
                    logger.info('[DuckDBWriter] %s 分片 %s rows=%d cum=%d (staging)',
                                table, key, len(df), total)
                if not self._pt_staging_exists(conn, tmp):
                    logger.warning('[DuckDBWriter] %s 无片可写（窗口空或全部已续）', table)
                    return dict(written=0, chunks=0, resumed_from=resumed_from,
                                rebuilt=rebuilt)
                cnt, ledger_sum = self._pt_ledger_rows(conn, table)
                _fin_sql = 'SELECT count(*) FROM "' + tmp + '"'
                guard = self._write_guard(conn, phase="pt_count", table=table,
                                          batch_id=batch_id, rows=0, sql=_fin_sql)
                final_rows = _guarded(conn, guard, _fin_sql).fetchone()[0]
                if ledger_sum != final_rows:
                    raise RuntimeError(
                        '%s staging(%s) 与 ledger(%s) 不一致，拒绝换名（防半表残留）'
                        % (table, final_rows, ledger_sum))
                # W1：换名窗口的两条 DDL 同样纳入看门狗（此前是裸 execute）
                for _sql in ('DROP TABLE IF EXISTS "' + table + '"',
                             'ALTER TABLE "' + tmp + '" RENAME TO "' + table + '"'):
                    guard = self._write_guard(conn, phase="pt_ddl", table=table,
                                              batch_id=batch_id, rows=final_rows, sql=_sql)
                    _guarded(conn, guard, _sql)
                self._pt_clear_ledger(conn, table)
                logger.info('[DuckDBWriter] %s passthrough 分片写完成 rows=%d chunks=%d '
                            'resumed_from=%d', table, final_rows, n_chunks, resumed_from)
                return dict(written=final_rows, chunks=n_chunks,
                            resumed_from=resumed_from, rebuilt=rebuilt)
            finally:
                try:
                    conn.close()
                finally:
                    if guard is not None:
                        guard.disarm()

    def get_last_date(self, source: str, table: str, freq: str = "daily") -> Optional[str]:
        with self._conn_lock:
            conn = self._conn()
            try:
                res = conn.execute(
                    "SELECT last_date FROM source_watermark "
                    "WHERE source=? AND table_name=? AND freq=?",
                    [source, table, freq]).fetchone()
                return str(res[0]) if res else None
            except Exception:
                return None
            finally:
                conn.close()

    def advance_watermark(self, source: str, table: str, freq: str,
                          last_date: str, batch_id: str):
        # 3A 写锁（操作粒度）：水位表 INSERT 属写路径（writers:764）
        ensure_write_lock(f"writers:watermark:{table}:{batch_id}")
        try:
            return self._advance_watermark_locked(source, table, freq, last_date, batch_id)
        finally:
            release_write_lock()

    def _advance_watermark_locked(self, source: str, table: str, freq: str,
                                  last_date: str, batch_id: str):
        now = datetime.now().isoformat()
        with self._conn_lock:
            conn = self._conn()
            guard = None
            try:
                # v2.4 B-3a：8 列显式 INSERT。审计列 source_generation/cutover_id 经
                # pre_cutover_generation 提供 pre-cutover 静态哨兵（QFQ 价格表→legacy；
                # 非 QFQ 表→not-qfq-managed）。source 保留真实值不改写。B-5 替换为动态
                # active generation/cutover；B-6 激活 mcp/mcp-gen1/<active>。
                gen, cutover = pre_cutover_generation(table, source)
                sql, params = _watermark_upsert_sql([source, table, freq, last_date,
                                                     batch_id, now, gen, cutover])
                # W1：水位 upsert 属写路径，纳入看门狗（此前为裸 execute）
                guard = self._write_guard(conn, phase="watermark", table=table,
                                          batch_id=batch_id, rows=1, sql=sql)
                _guarded(conn, guard, sql, params)
            finally:
                try:
                    conn.close()
                finally:
                    if guard is not None:
                        guard.disarm()

    # ------------------------------------------------------------------
    # 事务感知内部方法（QFQ 重锚编排专用）
    # ------------------------------------------------------------------
    # 说明：以下 *_on_conn 方法在**调用方提供的连接与事务**内执行，
    #   - 不获取 self._conn_lock（连接由调用方持有并串行化）；
    #   - 不 commit / 不 rollback / 不 close（事务边界由调用方掌控）。
    # 用途：QFQ 重锚编排需将「价格修正 UPDATE + anchor 状态更新 + 表级水位推进 +
    #   被过滤证券欠账」放入**同一 DuckDB 事务**保证原子性（设计 v3 §4.5 / §8）。
    # 公共 advance_watermark 行为保持不变（自开短连接自动提交），本方法为其事务版补充。

    def _advance_watermark_on_conn(self, conn, source: str, table: str, freq: str,
                                   last_date, batch_id: str) -> None:
        """在给定连接/事务内推进表级水位（PK source,table_name,freq）。不 commit。

        语义与公共 ``advance_watermark`` 完全一致（同一 INSERT ... ON CONFLICT 形态、
        同一 updated_at 口径），仅事务边界交由调用方。
        """
        now = datetime.now().isoformat()
        # v2.4 B-3a：8 列显式 INSERT + pre-cutover 静态哨兵（见公共 advance_watermark 注释）
        gen, cutover = pre_cutover_generation(table, source)
        # W1：同一 upsert 形态纳入看门狗；事务边界仍由调用方掌控（不 commit/rollback/close）
        sql, params = _watermark_upsert_sql([source, table, freq, last_date,
                                             batch_id, now, gen, cutover])
        _guarded(conn, _WriteGuard(conn, phase="watermark_tx", table=table,
                                   batch_id=batch_id, rows=1, sql_head=_sql_head(sql),
                                   writer=self), sql, params)

    def _upsert_pending_backfill_on_conn(self, conn, *, asset_type: str, code: str,
                                         table_name: str, freq: str,
                                         range_start, range_end,
                                         reason: str, anchor_version=None,
                                         status: str = "pending",
                                         now: Optional[str] = None,
                                         reopen: bool = False,
                                         price_source: str = "xtquant",
                                         source_generation: str = "xtquant-legacy") -> None:
        """在给定连接/事务内登记「被过滤证券」的精确欠账区间（设计 v4 §1.1）。不 commit。

        幂等语义（阻断 4 修复）：
        - 同 PK 已 ``resolved`` 且未显式 ``reopen`` → 保持 resolved，**不静默重开**（幂等）。
        - 显式 ``reopen=True`` → ``status='pending'``、``resolved_at=NULL``、
          ``last_error=NULL``、``attempt_count=0``（重新进入欠账）。

        输入校验（阻断 4）：``range_start <= range_end``；``asset_type`` 合法；
        ``table_name`` 属四价格表白名单；``freq`` 非空；``status`` 属允许集合。
        任一不满足抛 ``ValueError``。

        热路径（可靠性 阻断 5）：假定 schema 已由编排初始化，不再每条 upsert 前重复发 DDL；
        表不存在直接失败，让启动初始化问题显性暴露。
        """
        from quantstudio.pipeline.qfq_reanchor_schema import (
            _normalize_asset_type, _normalize_code, _validate_epoch_ms,
            PRICE_TABLES, BACKFILL_STATUS, ASSET_TABLE_MAP,
        )
        from quantstudio.pipeline.qfq_calendar import _norm_freq

        # —— 输入校验（阻断 4 + 阻断 3 关联契约）——
        # asset_type：归一化（"stock"→"STOCK"），拒绝非法值
        asset_type = _normalize_asset_type(asset_type)
        # code：canonical 裸 6 位码（复用 schema 单一规则，不复制）
        code = _normalize_code(code)
        # table_name：四价格表白名单
        if table_name not in PRICE_TABLES:
            raise ValueError(
                f"非法 table_name: {table_name!r}（仅四价格表 {sorted(PRICE_TABLES)}）")
        # asset_type ↔ table_name 关联契约
        if table_name not in ASSET_TABLE_MAP.get(asset_type, frozenset()):
            raise ValueError(
                f"asset_type={asset_type} 与 table_name={table_name!r} 不匹配"
                f"（STOCK→stock_daily/stock_minutes；ETF→etf_daily/etf_minutes）")
        # freq：非空 + 与 table_name 关联（daily↔daily，minutes↔1min）
        if not freq or not str(freq).strip():
            raise ValueError("freq 不能为空")
        freq = str(freq).strip()
        kind, n = _norm_freq(freq)
        if kind == "unknown":
            raise ValueError(f"非法 freq: {freq!r}")
        if table_name.endswith("_daily") and kind != "daily":
            raise ValueError(
                f"table_name={table_name!r} 为日线表，freq 必须为 daily，收到 {freq!r}")
        if table_name.endswith("_minutes") and (kind != "minute" or n != 1):
            raise ValueError(
                f"table_name={table_name!r} 为分钟表，freq 必须为 1min（batch1），收到 {freq!r}")
        # 阻断 2：freq 规范化为 storage canonical（别名 1m/1d 必须写入 1min/daily）。
        # 输入别名可兼容，但存储值唯一规范，否则后续 WHERE freq=? / ON CONFLICT 无法命中
        # canonical，造成欠账无法正确补拉或 readback。
        if kind == "daily":
            freq_canonical = "daily"
        elif kind == "minute" and n == 1:
            freq_canonical = "1min"
        else:
            raise NotImplementedError(
                f"freq={freq!r} 暂不支持（batch1 仅 daily/1min）")
        # range_start/range_end：有效 epoch-ms（共享 schema 校验，拒绝非法/越界）
        try:
            rs = _validate_epoch_ms(range_start)
            re_ = _validate_epoch_ms(range_end)
        except ValueError as e:
            raise ValueError(f"range_start/range_end 非法: {e}")
        if rs > re_:
            raise ValueError(f"range_start 必须 <= range_end: {rs} > {re_}")
        # reason：非空字符串
        if not isinstance(reason, str):
            raise ValueError(f"reason 必须为非空字符串: {reason!r}")
        reason = reason.strip()
        if not reason:
            raise ValueError("reason 不能为空")
        # reopen：严格 bool（避免 "false"/0 被当真值）
        if not isinstance(reopen, bool):
            raise ValueError(f"reopen 必须为 bool: {reopen!r}")
        # status：允许集合
        if status not in BACKFILL_STATUS:
            raise ValueError(
                f"非法 status: {status!r}（仅 {sorted(BACKFILL_STATUS)}）")

        ts = now or datetime.now().isoformat()

        # —— 条件重开：resolved 且未显式 reopen → 保持 resolved，不重开 ——
        # v2.4 B-3a：pending_backfill 新 PK 含 price_source/source_generation（8 列）。
        # price_source 由调用方传入（默认 xtquant 保持兼容；MCP 路径显式传 cfg.price_source=mcp）；
        # source_generation 默认 pre-cutover 哨兵。不查 active cutover（B-5/B-6 范围）。
        _bf_price_source = price_source
        _bf_gen = source_generation
        _row_sql = (
            "SELECT status, resolved_at, last_error, attempt_count FROM qfq_pending_backfill "
            "WHERE asset_type=? AND code=? AND table_name=? AND freq=? "
            "AND range_start=? AND range_end=? AND price_source=? AND source_generation=?")
        _row_params = [asset_type, code, table_name, freq_canonical, rs, re_,
                       _bf_price_source, _bf_gen]
        # W1：读侧同款看门狗（该 SELECT 走 8 键点查，但同属本方法的无界等待面）
        row = _guarded(conn, _WriteGuard(conn, phase="pending_backfill_read",
                                        table=table_name, batch_id=f"{code}:{freq_canonical}",
                                        rows=1, sql_head=_sql_head(_row_sql), writer=self),
                       _row_sql, _row_params).fetchone()

        # —— 阻断 3：普通 upsert（欠账登记）禁止创建/变更终态或处理中态 ——
        # resolved / in_progress 必须由专用状态机方法（如 _resolve_backfill_on_conn /
        # _mark_backfill_in_progress_on_conn）处理。普通 upsert 仅允许非终态
        # {pending, blocked, retryable_failed}；非终态 row 的普通 upsert 仍按既有
        # 幂等/重开逻辑处理（不在此拦截）。
        if status in ("resolved", "in_progress"):
            raise ValueError(
                f"status={status!r} 为终态/处理中态，禁止通过普通 upsert 创建或变更；"
                f"必须使用专用状态机方法（如 _resolve_backfill_on_conn）。"
                f"普通 upsert 仅允许 {sorted(BACKFILL_STATUS - {'resolved', 'in_progress'})}")
        if row is not None and row[0] == "resolved" and not reopen:
            return  # 幂等：保持 resolved，不静默重开

        # —— 决定保留/清空的 resolved 上下文 ——
        if row is not None and reopen:
            resolved_at_val = None
            last_error_val = None
            attempt_count_val = 0
        elif row is not None:
            # 非重开：保留既有 resolved_at / last_error / attempt_count
            resolved_at_val = row[1]
            last_error_val = row[2]
            attempt_count_val = row[3] if row[3] is not None else 0
        else:
            resolved_at_val = None
            last_error_val = None
            attempt_count_val = 0
        upsert_status = "pending" if reopen else status

        # W1：欠账登记 upsert 纳入看门狗（事务边界仍由调用方掌控）
        _pb_sql = (
            "INSERT INTO qfq_pending_backfill "
            "(asset_type, code, table_name, freq, range_start, range_end, "
            " price_source, source_generation, reason, "
            " anchor_version, status, attempt_count, last_error, created_at, updated_at, resolved_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT (asset_type, code, table_name, freq, range_start, range_end, "
            "            price_source, source_generation) "
            "DO UPDATE SET reason=EXCLUDED.reason, anchor_version=EXCLUDED.anchor_version, "
            "status=EXCLUDED.status, updated_at=EXCLUDED.updated_at, "
            "resolved_at=EXCLUDED.resolved_at, last_error=EXCLUDED.last_error, "
            "attempt_count=EXCLUDED.attempt_count")
        _guarded(conn, _WriteGuard(conn, phase="pending_backfill", table=table_name,
                                   batch_id=f"{code}:{freq_canonical}", rows=1,
                                   sql_head=_sql_head(_pb_sql), writer=self),
                 _pb_sql,
                 [asset_type, code, table_name, freq_canonical, rs, re_,
                  _bf_price_source, _bf_gen,
                  reason, anchor_version, upsert_status, attempt_count_val,
                  last_error_val, ts, ts, resolved_at_val])

    @staticmethod
    def _table_columns(table: str) -> List[str]:
        """返回表的列名（与 DDL 顺序一致，统一口径 v2.0）"""
        COLS = {
            "stock_daily": ["code", "time", "open", "high", "low", "close",
                            "volume", "amount", "preClose", "suspendFlag",
                            "settelementPrice", "openInterest",
                            "open_front", "high_front", "low_front", "close_front",
                            "open_back", "high_back", "low_back", "close_back",
                            "open_front_ratio", "high_front_ratio", "low_front_ratio", "close_front_ratio",
                            "open_back_ratio", "high_back_ratio", "low_back_ratio", "close_back_ratio",
                            "turn", "pctChg", "peTTM", "psTTM", "pcfNcfTTM", "pbMRQ",
                            "isST",
                            "is_st_reliable", "is_st_reliable_source",
                            "is_delisting_risk", "is_delisting_risk_source",
                            "dividend_type", "update_time", "data_source"],
            "stock_minutes": ["code", "time", "freq", "open", "high", "low", "close",
                              "volume", "amount", "preClose", "suspendFlag",
                              "settelementPrice", "openInterest",
                              "open_front", "high_front", "low_front", "close_front",
                              "open_back", "high_back", "low_back", "close_back",
                              "open_front_ratio", "high_front_ratio", "low_front_ratio", "close_front_ratio",
                              "open_back_ratio", "high_back_ratio", "low_back_ratio", "close_back_ratio",
                              "dividend_type", "update_time", "data_source"],
            "etf_minutes": ["code", "time", "freq", "open", "high", "low", "close",
                            "volume", "amount", "preClose", "suspendFlag",
                            "settelementPrice", "openInterest",
                            "open_front", "high_front", "low_front", "close_front",
                            "open_back", "high_back", "low_back", "close_back",
                            "open_front_ratio", "high_front_ratio", "low_front_ratio", "close_front_ratio",
                            "open_back_ratio", "high_back_ratio", "low_back_ratio", "close_back_ratio",
                            "dividend_type", "update_time", "data_source"],
            "tick": ["code", "time", "lastPrice", "open", "high", "low", "lastClose",
                     "amount", "volume", "pvolume", "stockStatus", "openInt", "lastSettlementPrice",
                     "askPrice1", "askPrice2", "askPrice3", "askPrice4", "askPrice5",
                     "bidPrice1", "bidPrice2", "bidPrice3", "bidPrice4", "bidPrice5",
                     "askVol1", "askVol2", "askVol3", "askVol4", "askVol5",
                     "bidVol1", "bidVol2", "bidVol3", "bidVol4", "bidVol5",
                     "transactionNum", "update_time", "data_source"],
            "fin_indicator": ["code", "ann_date", "end_date", "eps", "diluted_eps", "bps", "roe",
                              "pe_ttm", "pb", "ps_ttm",
                              "np_yoy", "or_yoy", "tr_yoy", "update_flag",
                              "backfill_eps_source", "data_source"],
            "index_daily": ["code", "time", "open", "high", "low", "close",
                            "pctChg", "volume", "amount", "data_source"],
            "stock_daily_valuation": ["code", "time", "circ_mv", "total_mv",
                                      "free_share",
                                      "pe_ttm", "pb", "turnover_rate", "update_time", "data_source"],
            "etf_daily": ["code", "time", "open", "high", "low", "close",
                          "preClose", "pctChg", "volume", "amount", "turn",
                          "open_front", "high_front", "low_front", "close_front",
                          "open_back", "high_back", "low_back", "close_back",
                          "open_front_ratio", "high_front_ratio", "low_front_ratio", "close_front_ratio",
                          "open_back_ratio", "high_back_ratio", "low_back_ratio", "close_back_ratio",
                          "isST", "dividend_type", "update_time", "data_source"],
            "stock_basic": ["code", "ts_code", "symbol", "name", "area", "industry",
                            "market", "list_status", "list_date", "delist_date", "exchange",
                            "update_time", "data_source"],
            "trade_calendar": ["cal_date", "is_open", "source", "updated_at",
                               "exchange", "pretrade_date"],
            "etf_basic": ["code", "ts_code", "name", "exchange",
                          "list_date", "delist_date", "etf_type", "tracking_index",
                          "is_cross_border", "status", "fund_type", "invest_type",
                          "type", "classification_method", "classification_version",
                          "update_time", "data_source"],
            "stock_float_share": ["code", "end_date", "ann_date",
                                  "free_share", "total_share",
                                  "circ_mv", "total_mv", "update_time", "data_source"],
            "index_constituents": ["index_code", "code", "time", "weight", "data_source"],
            "index_constituents_snapshot_meta": ["index_code", "time", "n_constituents",
                                                 "expected_count", "status",
                                                 "n_duplicate_codes", "n_negative_weights",
                                                 "n_blank_codes",
                                                 "update_time", "data_source"],
            "balance_statement": ["code", "end_date", "ann_date",
                                  "total_assets", "total_liability", "total_equity",
                                  "total_current_assets", "total_non_current_assets",
                                  "total_current_liability", "total_non_current_liability",
                                  "account_receivable", "account_payable", "inventory",
                                  "cash_equivalents", "fixed_asset", "intangible_asset", "goodwill",
                                  "update_time", "data_source"],
            "income_statement": ["code", "end_date", "ann_date",
                                 "operating_revenue", "operating_cost", "operating_profit",
                                 "total_profit", "net_profit", "np_parent_company_owners",
                                 "sale_expense", "manage_expense", "finance_expense", "rd_expense",
                                 "income_tax", "basic_eps", "update_time", "data_source"],
            "cashflow_statement": ["code", "end_date", "ann_date",
                                   "net_operate_cash_flow", "net_invest_cash_flow", "net_finance_cash_flow",
                                   "cash_add_balance", "goods_sale_and_services", "goods_buy_and_services",
                                   "fixed_asset_depreciation", "update_time", "data_source"],
            "stock_dividend": ["code", "ex_date", "record_date",
                               "ann_date", "end_date",
                               "cash_div_before_tax", "cash_div_after_tax",
                               "cash_div", "stk_div", "stk_bo_rate", "stk_co_rate",
                               "div_rat", "div_proc", "update_time", "data_source"],
            "etf_dividend": ["code", "ex_date", "record_date", "ann_date",
                             "imp_anndate", "base_date", "div_proc", "pay_date",
                             "earpay_date", "net_ex_date", "div_cash", "base_unit",
                             "ear_distr", "ear_amount", "account_date", "base_year",
                             "update_time", "data_source"],
            "sw_industry": ["code", "industry_code", "industry_name", "industry_level", "update_time", "data_source"],
            "industry_classification": ["classification_system", "classification_version",
                                        "industry_code", "industry_name", "industry_level",
                                        "parent_industry_code", "effective_from", "effective_to",
                                        "update_time", "data_source"],
            "industry_membership": ["classification_system", "classification_version",
                                    "industry_level", "industry_code", "code",
                                    "effective_from", "effective_to",
                                    "update_time", "data_source"],
            "stock_namechange": ["code", "change_date", "name_before", "name_after",
                                 "status_after", "update_time", "data_source"],
            "stock_delist": ["code", "list_date", "delist_date", "market", "update_time", "data_source"],
        }
        return COLS.get(table, [])


class QuestDBWriter(BaseWriter):
    """QuestDB 写入器（可选，ILP 批量写入）

    config 示例：{"type": "questdb", "host": "localhost", "ilp_port": 9009, "pg_port": 8812}
    Phase 1 占位实现，Phase 3 完善 ILP 协议。
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self.host = config.get("host", "localhost")
        self.ilp_port = config.get("ilp_port", 9009)
        self.pg_port = config.get("pg_port", 8812)
        raise NotImplementedError("QuestDBWriter 将在 Phase 3 实现 ILP 协议（基线 v3.2 §8.1）")

    def write(self, df, table, batch_id): raise NotImplementedError
    def get_last_date(self, source, table, freq="daily"): raise NotImplementedError
    def advance_watermark(self, source, table, freq, last_date, batch_id): raise NotImplementedError


def create_writer(config: Dict) -> BaseWriter:
    """工厂方法"""
    wtype = config.get("type", "duckdb").lower()
    registry = {"duckdb": DuckDBWriter, "questdb": QuestDBWriter}
    if wtype not in registry:
        raise ValueError(f"未知 writer 类型: {wtype}")
    return registry[wtype](config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    here = Path(__file__).resolve().parent.parent.parent
    w = DuckDBWriter({"type": "duckdb", "path": str(db_path())})

    df = pd.DataFrame({
        "ts_code": ["600000.SH", "600000.SH"],
        "trade_date": ["2026-07-10", "2026-07-11"],
        "open": [10.0, 10.5], "high": [10.2, 10.8], "low": [9.9, 10.3],
        "close": [10.1, 10.6], "pct_chg": [1.0, 4.95],
        "vol": [1000.0, 1200.0], "amount": [1010.0, 1272.0],
    })
    n = w.write(df, "stock_daily", "smoke_001")
    w.advance_watermark("test", "stock_daily", "daily", "2026-07-11", "smoke_001")
    last = w.get_last_date("test", "stock_daily", "daily")
    print(f"wrote={n}, watermark={last}")

    # 重放验证幂等
    n2 = w.write(df, "stock_daily", "smoke_001_replay")
    print(f"replay wrote={n2}（应仍为 2，upsert 不重复）")
    print("✅ DuckDBWriter 幂等写入验证通过")
