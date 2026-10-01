"""QFQ 常驻增量事件驱动闭环 —— 共享类型、状态枚举与配置（resident orchestrator v2）

本模块是新增编排层（qfq_event_discovery / qfq_fresh_capture / qfq_resident_orchestrator /
qfq_orchestrator_cli）与既有 ``qfq_reanchor_engine`` / ``qfq_reanchor_schema`` 的**共享契约**：

- 状态枚举与 ``qfq_reanchor_schema`` 的合法状态集合（BACKFILL_STATUS / TRIGGER_STATUS /
  WATERMARK_INTENT_STATUS / CYCLE_PHASES / FRESH_CAPTURE_STATUS / OBSERVATION_CURSOR_STATUS）
  **逐字对齐**，是单一真相源的 Python 侧投影；
- 确定性 trigger_id / payload_hash 生成器（幂等、可重放）；
- ``QFQOrchestratorConfig``：从 ``collector_tasks.json`` 的 ``qfq_orchestrator`` 块加载，
  带默认值 + **fail-fast** 校验（价格源非 xtquant / 1min 未配置 / watermark policy 非法
  直接抛 ``QFQConfigError``）。

本模块不依赖 DuckDB/SQLite 连接，可独立单测。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Sequence

# 状态集合直接复用 schema 单一真相源（避免两份清单漂移）
from quantstudio.pipeline.qfq_reanchor_schema import (  # noqa: F401
    BACKFILL_STATUS,
    TRIGGER_STATUS,
    WATERMARK_INTENT_STATUS,
    CYCLE_PHASES,
    CYCLE_STATUS,
    FRESH_CAPTURE_STATUS,
    OBSERVATION_CURSOR_STATUS,
    TRIGGER_TYPES,
    PRICE_TABLES,
    QFQ_ASSET_TYPES,
)


class QFQConfigError(Exception):
    """配置非法（config_lint / 加载时 fail-fast）。"""


# ---------------------------------------------------------------------------
# 状态枚举（str enum，与 schema 合法集合逐字一致）
# ---------------------------------------------------------------------------

class TriggerType(str, Enum):
    STOCK_DIVIDEND = "stock_dividend"
    STOCK_ADJ_FACTOR = "stock_adj_factor"
    ETF_FUND_ADJ = "etf_fund_adj"
    FACTOR_REVISION = "factor_revision"
    FACTOR_NEW = "factor_new"
    BOOTSTRAP = "bootstrap"


class TriggerStatus(str, Enum):
    SCHEDULED = "scheduled"
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMMITTED = "committed"
    RETRYABLE_FAILED = "retryable_failed"
    BLOCKED = "blocked"
    DEAD_LETTER = "dead_letter"
    SUPERSEDED = "superseded"  # v2.4 B-3：与 schema TRIGGER_STATUS 逐字对齐（legacy xtquant trigger 退役终态）


class CyclePhase(str, Enum):
    STARTED = "started"
    RECOVERING = "recovering"
    OBSERVING = "observing"
    FETCHING = "fetching"
    APPLYING = "applying"
    GATING = "gating"
    FINALIZED = "finalized"
    INTERRUPTED = "interrupted"
    FAILED = "failed"


class WatermarkIntentStatus(str, Enum):
    PENDING = "pending"
    HELD = "held"
    COMMITTED = "committed"
    SUPERSEDED = "superseded"


class FreshCaptureStatus(str, Enum):
    CAPTURED = "captured"
    APPLIED = "applied"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# 确定性 ID / hash 生成器（幂等、可重放）
# ---------------------------------------------------------------------------

# v2.4 B-2：trigger_id 显式算法版本。MCP 生产路径唯一可用 v2（纳入 price_source/
# source_generation），使同一分红事件跨世代生成不同 ID，不被旧 xtquant trigger 的
# INSERT OR IGNORE 跳过。v1 仅历史行 sha1 不重算/审计比对用。
TRIGGER_ID_VERSION = 2


def trigger_id_v1(asset_type: str, code: str, effective_date_ms: int,
                  detection_source: str, payload_hash: str) -> str:
    """历史算法 v1 = sha1(asset_type|code|effective_date|detector|payload_hash)。

    仅供历史行 sha1 不重算/审计比对；**MCP 生产路径禁用**（不含世代 → 跨世代 ID 冲突）。
    """
    raw = f"{asset_type}|{code}|{int(effective_date_ms)}|{detection_source}|{payload_hash}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def trigger_id_v2(asset_type: str, code: str, effective_date_ms: int,
                  detection_source: str, payload_hash: str,
                  price_source: str, source_generation: str) -> str:
    """生产算法 v2 = sha1(v2|asset_type|code|effective_date|detector|payload_hash|price_source|source_generation)。

    纳入 price_source/source_generation → 同一分红事件跨世代生成不同 ID。
    相同语义事件（同世代内）仍生成同一 ID → 天然去重 + 崩溃可重放。
    **MCP 生产路径唯一可用此算法**（v2.4 设计 §3.2.2）。
    """
    raw = (f"v2|{asset_type}|{code}|{int(effective_date_ms)}|{detection_source}"
           f"|{payload_hash}|{price_source}|{source_generation}")
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def trigger_id_of(asset_type: str, code: str, effective_date_ms: int,
                  detection_source: str, payload_hash: str) -> str:
    """兼容别名（= trigger_id_v1）。

    v2.4 B-2 保留为历史路径入口（xtquant 世代、尚未接入世代的旧调用点）。
    **新生产路径应显式选择 trigger_id_v1（xtquant 历史行）或 trigger_id_v2（MCP 生产）**，
    不依赖此别名做隐式世代判定。B-5 全链路 SQL 世代过滤时将替换所有调用点。
    """
    return trigger_id_v1(asset_type, code, effective_date_ms, detection_source, payload_hash)


def alert_id_v2(asset_type: str, code: str, factor_time: int, revision_no: int,
                source_generation: str) -> str:
    """生产算法 v2 = sha1(asset_type|code|factor_time|revision_no|source_generation)。

    纳入 source_generation → 防跨世代 alert_id 冲突（v2.4 设计 §3.3 P0-4）。
    与 trigger_id_v2 同理，MCP 世代独立基线不与旧 xtquant observation 比较产生伪 revision。
    """
    raw = f"{asset_type}|{code}|{int(factor_time)}|{int(revision_no)}|{source_generation}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()



def payload_hash_of(fields: Sequence) -> str:
    """对影响语义的字段做确定性 sha256（字段须可 JSON 序列化或已归一为 str）。

    用于 stock_dividend 整行语义、adj_factor / fund_adj 因子值对、factor revision。
    """
    norm = []
    for f in fields:
        if f is None:
            norm.append("∅")
        elif isinstance(f, float):
            norm.append(repr(f))          # repr 保证跨进程稳定
        elif isinstance(f, (list, tuple)):
            norm.append("[" + ",".join(str(x) for x in f) + "]")
        else:
            norm.append(str(f))
    blob = "|".join(norm).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def capture_id_of(asset_type: str, code: str, run_id: str) -> str:
    """确定性 capture_id（同券同轮一次采集）。"""
    raw = f"{asset_type}|{code}|{run_id}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def event_id_of(trigger_id: str, attempt: int, capture_id: str) -> str:
    """引擎 event_id 基于 trigger/work + attempt + capture，崩溃可重放。"""
    raw = f"{trigger_id}|{int(attempt)}|{capture_id}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 数据记录（DB ↔ 对象映射）
# ---------------------------------------------------------------------------

@dataclass
class TriggerRecord:
    trigger_id: str
    asset_type: str
    code: str
    trigger_type: str
    detection_source: str
    source_key: Optional[str] = None
    effective_date: Optional[int] = None
    payload_hash: Optional[str] = None
    factor_old: Optional[float] = None
    factor_new: Optional[float] = None
    factor_revision: Optional[int] = None
    status: str = "pending"
    attempt_count: int = 0
    next_retry_at: Optional[str] = None
    claimed_by: Optional[str] = None
    claimed_at: Optional[str] = None
    last_event_id: Optional[str] = None
    last_error: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    # v2.4 B-3a：trigger_id 算法版本（1=历史 v1/xtquant 世代，2=MCP 生产 v2/含世代）。
    # 持久化契约（B-3a 冻结）：新库 2.1 契约启用后，legacy/pre-cutover trigger 显式
    # 持久化 version=1；B-5 MCP v2 路径显式写 version=2。已纳入 COLS/as_insert_params
    # （B-3a schema 列已就绪）。
    # 理由：不能仅靠 ID 字符串猜测算法版本；同一队列将并存 v1/v2 行；历史审计与迁移需明确版本；
    # source_generation 不能完全替代算法版本（同一世代内理论上也可能有多种算法）。
    trigger_id_version: int = 1
    # v2.4 B-3a：世代隔离列（持久化）。pre-cutover 静态值由写入方提供
    # （price_source=真实配置值；source_generation=xtquant-legacy；cutover_id=哨兵）。
    # B-5 替换为动态 active generation/cutover；B-6 激活 mcp/mcp-gen1/<active>。
    price_source: str = "xtquant"
    source_generation: str = "xtquant-legacy"
    cutover_id: str = "legacy-xtquant-pre-cutover"
    retired_at: Optional[str] = None
    retire_reason: Optional[str] = None
    # 非持久字段：本轮合并进来的其它相关事件日期（仅用于 fresh 拉取 ex_dates_ms）
    merged_effective_dates: List[int] = field(default_factory=list)

    COLS = [
        "trigger_id", "asset_type", "code", "trigger_type", "detection_source",
        "source_key", "effective_date", "payload_hash", "factor_old", "factor_new",
        "factor_revision", "status", "attempt_count", "next_retry_at", "claimed_by",
        "claimed_at", "last_event_id", "last_error", "completed_at",
        # v2.4 B-3a 新增持久化列（与 DUCKDB_COLS 顺序一致，省略 dead_letter_at 不在 COLS）
        "trigger_id_version", "price_source", "source_generation", "cutover_id",
        "retired_at", "retire_reason",
        "created_at", "updated_at",
    ]

    def as_insert_params(self):
        return [
            getattr(self, c) for c in self.COLS
        ]

    @classmethod
    def from_row(cls, row) -> "TriggerRecord":
        cols = cls.COLS
        d = dict(zip(cols, row))
        return cls(**d)


@dataclass
class CycleRun:
    cycle_id: str
    business_date: Optional[int]
    trigger_surface: Optional[str]
    config_hash: Optional[str]
    schema_hash: Optional[str]
    phase: str
    discovered_count: int = 0
    executed_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    pending_count: int = 0
    status: str = "started"
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[str] = None
    # v2.4 B-3a：世代隔离列（持久化）。pre-cutover 静态值由写入方提供（见
    # qfq_resident_orchestrator.begin_cycle）。B-5 替换为动态；B-6 激活。
    # 字段顺序与 DDL 列顺序一致（detector_degraded 之后、updated_at 之前）。
    price_source: str = "xtquant"
    source_generation: str = "xtquant-legacy"
    cutover_id: str = "legacy-xtquant-pre-cutover"
    updated_at: Optional[str] = None


@dataclass
class FreshCaptureRecord:
    capture_id: str
    asset_type: str
    code: str
    source: Optional[str] = None
    daily_range_start: Optional[int] = None
    daily_range_end: Optional[int] = None
    minute_range_start: Optional[int] = None
    minute_range_end: Optional[int] = None
    daily_row_count: Optional[int] = None
    minute_row_count: Optional[int] = None
    daily_min_time: Optional[int] = None
    daily_max_time: Optional[int] = None
    minute_min_time: Optional[int] = None
    minute_max_time: Optional[int] = None
    daily_sha256: Optional[str] = None
    minute_sha256: Optional[str] = None
    metadata_sha256: Optional[str] = None
    download_trace: Optional[str] = None
    status: str = "captured"
    # v2.4 B-3a：世代隔离列（持久化）。pre-cutover 静态值由写入方提供（见
    # qfq_fresh_capture.write_fresh_capture）。B-5 替换为动态；B-6 激活。
    source_generation: str = "xtquant-legacy"
    cutover_id: str = "legacy-xtquant-pre-cutover"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class WatermarkIntent:
    cycle_id: str
    source: str
    table_name: str
    freq: str
    # v2.4 B-3a：世代隔离列（持久化）。pre-cutover 静态值由写入方提供（见
    # qfq_resident_orchestrator.defer_watermark）。B-5 替换为动态；B-6 激活。
    # 字段顺序与 DDL 列顺序一致（freq 之后、old_watermark 之前）。
    # old_watermark / candidate_watermark 显式给 None 默认值以保持 dataclass
    # 字段顺序合法（带默认字段后不能再有无默认字段）。
    source_generation: str = "xtquant-legacy"
    cutover_id: str = "legacy-xtquant-pre-cutover"
    old_watermark: Optional[str] = None
    candidate_watermark: Optional[str] = None
    status: str = "pending"
    hold_reason: Optional[str] = None
    committed_at: Optional[str] = None


@dataclass
class ReanchorOutcome:
    trigger_id: str
    asset_type: str
    code: str
    status: str          # committed / blocked / rolled_back / failed
    event_id: Optional[str] = None
    error: Optional[str] = None
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# 配置（collector_tasks.json 的 qfq_orchestrator 块）
# ---------------------------------------------------------------------------

# 缺省配置（enabled=False，安全默认；显式开启才进入协调周期）
DEFAULT_ORCHESTRATOR_CFG: Dict = {
    "enabled": False,
    "factor_refresh_enabled": False,
    # 工作包 D 防线 2.1（C 修复）：独立交叉源抽核开关（默认关闭，独立 opt-in）
    "factor_cross_check_enabled": False,
    "require_bootstrap": True,
    "price_source": "xtquant",
    # 2026-09-17 ConfigLint 登记：source_generation 为**真实被消费**的键
    # （_parse_identifier 于 :451 读取；qfq_orchestrator_cli.py:626 依其值分支）。
    # 此前未列入本真相源 ⇒ ConfigLint 每轮误报「未知配置键」（5 分钟一次）。
    # 默认值与 _parse_identifier(cfg, "source_generation", "xtquant-legacy") 的兜底**逐字相同**
    # ⇒ 登记行为中性：未设置的 profile 取值不变，已设置的 profile 由 cfg.update(file_cfg) 覆盖后亦不变。
    "source_generation": "xtquant-legacy",
    "generation_mode": "pre_cutover",
    "stock_factor_detector": "tushare_adj_factor",
    "etf_factor_detector": "tushare_fund_adj",
    "freqs": ["1min"],
    "factor_overlap_lookback_days": 30,
    "fresh_fetch_workers": 4,
    "claim_lease_sec": 600,
    "retry_max": 5,
    "retry_backoff_sec": [60, 120, 240, 480, 960],
    "dead_letter_policy": {"max_attempts": 8},
    "watermark_policy": "hold_until_consistent",
    "quality_thresholds": {
        "tick_error_abs": 1e-9,
        "tick_error_rel": 1e-6,
        "front_exact_match": True,
        "raw_unchanged": True,
        "row_conservation": True,
        "coverage_min": 0.0,
        "pending_sla_hours": 72,
        "dead_letter_max": 0,
    },
    "bootstrap": {"batch_size": 50, "max_parallel_securities": 8},
}


def _parse_identifier(cfg: Dict, key: str, default: str) -> str:
    """解析世代/cutover 标识符，fail-fast 拒绝空串/None/非字符串（v2.4 B-2.1）。

    数据库层虽 NOT NULL，但语义无效（空串/None→'None'/空白）仍须在此拦截，避免
    无效标识进入 trigger_id_v2 / 审计逻辑。要求：必须是字符串、strip() 后非空、
    禁止 'None' 字面量、禁止纯空白。
    """
    raw = cfg.get(key, default)
    if not isinstance(raw, str):
        raise QFQConfigError(f"{key} 必须为非空字符串，收到 {type(raw).__name__}: {raw!r}")
    value = raw.strip()
    if not value or value.lower() == "none":
        raise QFQConfigError(f"{key} 必须为非空有效标识，收到 {raw!r}")
    return value


@dataclass
class QFQOrchestratorConfig:
    enabled: bool = False
    factor_refresh_enabled: bool = False  # 任务2.2：主动因子刷新（默认关闭，独立 opt-in）
    # 工作包 D 防线 2.1（C 修复）：独立交叉源抽核开关（默认关闭，独立 opt-in）。
    # 此前 daemon getattr 恒 False，交叉核验为死代码（审核阻断项 C）。
    factor_cross_check_enabled: bool = False
    require_bootstrap: bool = True
    price_source: str = "xtquant"
    generation_mode: str = "pre_cutover"
    # v2.4 B-2：数据源世代隔离字段。
    # 默认值保守（xtquant-legacy / legacy-xtquant-pre-cutover 哨兵），确保：
    # (1) 旧配置文件不传这两个字段时 daemon 行为与 B-1 完全一致（不自动进 MCP 世代）；
    # (2) 在 B-3 schema 与 active-cutover 契约落地前，现有 daemon 不会进入 mcp-gen1 生产处理。
    # MCP 世代需在 B-6 cutover 激活后由配置显式传 source_generation='mcp-gen1' + 实际 cutover_id。
    source_generation: str = "xtquant-legacy"
    cutover_id: str = "legacy-xtquant-pre-cutover"
    stock_factor_detector: str = "tushare_adj_factor"
    etf_factor_detector: str = "tushare_fund_adj"
    freqs: List[str] = field(default_factory=lambda: ["1min"])
    factor_overlap_lookback_days: int = 30
    fresh_fetch_workers: int = 4
    claim_lease_sec: int = 600
    retry_max: int = 5
    retry_backoff_sec: List[int] = field(default_factory=lambda: [60, 120, 240, 480, 960])
    dead_letter_max_attempts: int = 8
    watermark_policy: str = "hold_until_consistent"
    quality_thresholds: Dict = field(default_factory=dict)
    bootstrap_batch_size: int = 50
    bootstrap_max_parallel: int = 8
    # P2②（2026-09-29，缺陷1 修复收尾方案 §3.P2）：逐股重锚链（applying 相位）的
    # 运行预算（秒）。默认 1800（30 min）；**0/负 = 不限**（回退旧行为）。
    # 背景：该链此前无任何边界——2026-09-28 事故中实测 47 只/5 小时（约 6.4 min/只），
    # 全市场 5,247 只 ≈ 数周量级。预算到点即「本轮截断、下轮续做」
    # （units 来自 qfq_trigger_queue 持久化 pending，下轮 recover_pending_due 回收）。
    apply_budget_sec: int = 1800
    raw: Dict = field(default_factory=dict)

    # —— 四张协调价格表（启用编排器时其水位延迟提交）——
    COORDINATED_PRICE_TABLES = frozenset(PRICE_TABLES)

    @classmethod
    def load(cls, config_dir: Optional[str | Path] = None,
             raw: Optional[Dict] = None) -> "QFQOrchestratorConfig":
        """从 ``collector_tasks.json`` 的 ``qfq_orchestrator`` 块加载；缺省合并。

        Args:
            config_dir: 含 collector_tasks.json 的目录；None 表示仅用 raw/缺省。
            raw: 直接传入的配置 dict（优先于文件）；便于测试与 CLI 注入。
        """
        cfg = dict(DEFAULT_ORCHESTRATOR_CFG)
        if config_dir is not None:
            p = Path(config_dir) / "collector_tasks.json"
            if p.exists():
                try:
                    full = json.loads(p.read_text(encoding="utf-8"))
                    file_cfg = full.get("qfq_orchestrator", {})
                    cfg.update(file_cfg)
                except Exception as e:  # pragma: no cover
                    raise QFQConfigError(f"读取 collector_tasks.json 失败: {e}") from e
        if raw:
            cfg.update(raw)
        return cls.from_dict(cfg)

    @classmethod
    def from_dict(cls, cfg: Dict) -> "QFQOrchestratorConfig":
        dl = cfg.get("dead_letter_policy", {}) or {}
        bs = cfg.get("bootstrap", {}) or {}
        qt = cfg.get("quality_thresholds", {}) or {}
        source_generation = _parse_identifier(cfg, "source_generation", "xtquant-legacy")
        requested_mode = cfg.get("generation_mode")
        generation_mode = (str(requested_mode) if requested_mode is not None else
                           ("dynamic" if source_generation != "xtquant-legacy" else "pre_cutover"))
        obj = cls(
            enabled=bool(cfg.get("enabled", False)),
            factor_refresh_enabled=bool(cfg.get("factor_refresh_enabled", False)),
            # 工作包 D 防线 2.1（C 修复）：from_dict 接线（此前字段缺失恒 False）
            factor_cross_check_enabled=bool(cfg.get("factor_cross_check_enabled", False)),
            require_bootstrap=bool(cfg.get("require_bootstrap", True)),
            price_source=str(cfg.get("price_source", "xtquant")),
            generation_mode=generation_mode,
            source_generation=source_generation,
            cutover_id=_parse_identifier(cfg, "cutover_id", "legacy-xtquant-pre-cutover"),
            stock_factor_detector=str(cfg.get("stock_factor_detector", "tushare_adj_factor")),
            etf_factor_detector=str(cfg.get("etf_factor_detector", "tushare_fund_adj")),
            freqs=list(cfg.get("freqs", ["1min"])),
            factor_overlap_lookback_days=int(cfg.get("factor_overlap_lookback_days", 30)),
            fresh_fetch_workers=int(cfg.get("fresh_fetch_workers", 4)),
            claim_lease_sec=int(cfg.get("claim_lease_sec", 600)),
            retry_max=int(cfg.get("retry_max", 5)),
            retry_backoff_sec=list(cfg.get("retry_backoff_sec", [60, 120, 240, 480, 960])),
            dead_letter_max_attempts=int(dl.get("max_attempts", 8)),
            watermark_policy=str(cfg.get("watermark_policy", "hold_until_consistent")),
            quality_thresholds=dict(qt),
            bootstrap_batch_size=int(bs.get("batch_size", 50)),
            bootstrap_max_parallel=int(bs.get("max_parallel_securities", 8)),
            apply_budget_sec=int(cfg.get("apply_budget_sec", 1800)),
            raw=dict(cfg),
        )
        obj.validate()
        return obj

    def validate(self) -> None:
        """fail-fast 校验（config_lint 复用同一逻辑）。"""
        if self.price_source not in ("xtquant", "mcp"):
            raise QFQConfigError(
                f"qfq_orchestrator.price_source 必须为 xtquant 或 mcp（价格修正源合法源），"
                f"收到 {self.price_source!r}")
        if self.generation_mode not in ("pre_cutover", "dynamic"):
            raise QFQConfigError("qfq_orchestrator.generation_mode must be pre_cutover or dynamic")
        if self.generation_mode == "dynamic" and self.source_generation == "xtquant-legacy":
            raise QFQConfigError("generation_mode=dynamic requires an explicit non-legacy source_generation")
        if self.generation_mode == "pre_cutover" and self.source_generation != "xtquant-legacy":
            raise QFQConfigError(
                "generation_mode=pre_cutover requires source_generation=xtquant-legacy")
        if "1min" not in self.freqs:
            raise QFQConfigError(
                f"qfq_orchestrator.freqs 必须含 '1min'（当前正式频率），收到 {self.freqs!r}")
        for f in self.freqs:
            if f not in ("1min", "1m", "daily"):
                raise QFQConfigError(f"qfq_orchestrator.freqs 含非法频率 {f!r}")
        if self.watermark_policy not in ("hold_until_consistent",):
            raise QFQConfigError(
                f"qfq_orchestrator.watermark_policy 仅允许 'hold_until_consistent'（禁止 "
                f"advance_with_pending），收到 {self.watermark_policy!r}")
        if self.fresh_fetch_workers < 1:
            raise QFQConfigError("qfq_orchestrator.fresh_fetch_workers 必须 >= 1")
        if self.retry_max < 1:
            raise QFQConfigError("qfq_orchestrator.retry_max 必须 >= 1")
        if self.claim_lease_sec < 1:
            raise QFQConfigError("qfq_orchestrator.claim_lease_sec 必须 >= 1")

    def can_coordinate_watermark(self, table: str) -> bool:
        """该表是否在本轮协调周期内延迟提交水位（四张价格表 + 启用）。"""
        return self.enabled and table in self.COORDINATED_PRICE_TABLES
