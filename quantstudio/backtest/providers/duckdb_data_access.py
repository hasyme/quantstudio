"""
DuckDBDataAccess — 把 PtradeAPI / BacktestEngine 的全部 SQL 查询收敛到单一类。

Phase 1（纯重构，零行为变更）：
- 每个方法内部的 SQL 与 ptrade_api.py / backtest_engine.py 原代码逐字符一致。
- 不暴露任何 Ptrade 代码格式转换 / 策略语义，只返回「源形状」DataFrame
  （部分方法因历史实现在内部做了裸码→Ptrade 格式转换，已在文档中标注）。
- 连接管理（持久只读连接）从 PtradeAPI._get_ro_conn() 迁移到此处的 _get_conn()。

设计原则：本类是「数据访问实现」，不是「数据源抽象」。Phase 2 会在此基础上
定义 MarketDataProvider / FundamentalDataProvider / ReferenceDataProvider /
CalendarProvider 抽象接口，并把本类作为 DuckDB 实现委托。
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Dict, List, Optional, Any

import pandas as pd
import numpy as np
from pathlib import Path

from .base import ReferenceDataCapabilityError
from .time_axis import day_start_ms, is_day_start_ms

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_valid_identifier(name: str, where: str) -> str:
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"[{where}] invalid identifier {name!r}: "
            "must match ^[A-Za-z_][A-Za-z0-9_]*$")
    return name


logger = logging.getLogger(__name__)

# ---- F-DUCKDB-LOCK（A2，2026-09-05，设计 docs/duckdb-lock-timeout-design.md）----
# per-statement 观测超时预算（秒）。语义钉死：单语句预算，绝非回测级总预算——
# 分片加载总耗时超预算被杀 = 新造「静默空数据出回测」事故（A1 复核硬性要求①）。
_DEFAULT_QUERY_BUDGET_S = 30.0
# P1 等价分片：每片码数（只切 WHERE code IN 的码集，不改列集/排序/后处理）。
_BARS_CACHE_CHUNK_DEFAULT = 200


def _env_pos_float(name: str, default: float) -> float:
    """读正浮点环境变量；缺省/非法回退 default（观测参数，绝不改变取数语义）。"""
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default


def _build_trade_date_map(time_series: pd.Series) -> pd.Series:
    """性能优化（2026-08-12，zcode+reasonix 联合定稿）：唯一 time 值 → 日期字符串
    map，再广播回全列（~10x 加速，逐位等价）。

    等价性：
    - pd.Timestamp(t, unit="ms").tz_localize("UTC").tz_convert("Asia/Shanghai")
      ≡ pd.to_datetime(t, unit="ms", utc=True).tz_convert("Asia/Shanghai")
      （同一 epoch-ms → 同一 UTC aware → 同一 +8 日期字符串；Asia/Shanghai 无 DST，
      跨日边界转换稳定）
    - time 列无 NaN（主键语义），map 不产生缺失键。
    """
    _unique_times = time_series.unique()
    # (a) 向量化改造（2026-09-26 纯性能优化，语义等价）：原实现为**逐唯一值 Python 循环**
    #     （每元素 4 次 pandas 调用：Timestamp/tz_localize/tz_convert/strftime）。
    #     等价性依据即本函数 docstring 已记载的恒等式：
    #       pd.Timestamp(t, unit="ms").tz_localize("UTC").tz_convert("Asia/Shanghai")
    #       ≡ pd.to_datetime(t, unit="ms", utc=True).tz_convert("Asia/Shanghai")
    #     此处仅把该恒等式一次施加于整个唯一值数组（C 层向量化），
    #     仍以 dict 映射后 .map() 广播回全列 —— 「唯一值 + 广播」结构不变。
    _uniq_dates = (
        pd.to_datetime(_unique_times, unit="ms", utc=True)
        .tz_convert("Asia/Shanghai")
        .strftime("%Y-%m-%d")
    )
    _td_map = {int(t): str(d) for t, d in zip(_unique_times, _uniq_dates)}
    return time_series.map(_td_map)

# 日线快照原始价口径（方案A逆转，2026-08-14 PTrade 实证决策），供 query_daily_snapshot
# 与 preload_daily_snapshots 共用，保证 per-day 查询与全期预取结果字节级一致。
#
# PTrade 平台实证（2026-08-13/14，4 次真实回测）：
#   日线成交价 = raw close（5/5 精确匹配）；分钟成交价 = bar raw close（6/6 精确匹配）；
#   持仓估值 last_price = raw close。撮合/估值链路必须用原始价对齐 PTrade。
#
# preClose 在行情源/入库数据中已是除权参考价语义（16 标的多板块抽查验证 2026-08-14）：
#   除权日 preClose = 前日 close × 复权因子；非除权日 preClose = 前日 raw close。
#   (raw close - preClose) / preClose 在所有日期（含除权日）均正确。
#
# 信号计算（get_history fq='pre'）独立走 query_bars_by_range（查 *_front 列），
# 不经过此快照 SQL，前复权信号不受影响。
_ADJ_OHLC_SQL = """
               open, high, low, close,"""
_ADJ_PRECLOSE_SQL = """
               preClose,"""


class DuckDBDataAccess:
    """DuckDB（QuantStudio 数据管线产出）数据访问实现。

    独占：连接创建、SQL、预加载内存缓存。
    不负责：撮合、指标计算、策略逻辑。
    """

    def __init__(self, db_path: Path):
        self._db_path = Path(db_path)
        self._ro_conn = None
        self._tables_cache: Optional[set] = None  # 表集合缓存（SHOW TABLES 结果，回测期只读恒定）
        # 预加载缓存（与 PtradeAPI 原缓存变量一一对应）
        self._preload_daily: Optional[pd.DataFrame] = None
        # code -> original row positions. Avoid scanning the full preload for
        # every portable per-security get_history call.
        self._preload_daily_code_positions: Optional[dict] = None
        self._preload_prev_ms: Optional[int] = None
        self._preload_float: Optional[pd.DataFrame] = None
        self._preload_listing: Optional[pd.DataFrame] = None
        # 阶段 2.5：{code: (listing_time, listing_source)} 字典（首次查询惰性构建），
        # 替代 query_security_info_from_preload / get_security_info ETF 分支内逐只
        # _preload_listing[code==x] 的 O(N) 布尔扫描（get_security_info 全市场万次
        # 调用→新 N+1）。取首行出现（等价原 iloc[0]）。
        self._preload_listing_by_code: Optional[dict] = None
        # ---- PR7 性能优化：bars 全历史内存缓存（query_bars_by_count_batch 预加载路径）----
        # (table, code) -> 全历史 DataFrame（原始列，未做 qfq/trade_date 后处理——后处理统一走 _post）
        # key 含表名：同一 code 跨表不可命中（如 510300 只属 etf_daily，若被 stock_daily
        # 循环误命中会跨表 concat 导致 NULL 字面量列的 dtype 提升——Int32 全 NA 列被
        # float64 合并，破坏与 SQL 路径逐表 fetchdf 的 dtype 一致性）。
        # 惰性按需加载：首次调用批量查一次 SQL，后续纯内存切片（time<=before_ms + tail(count)）。
        # 等价性：缓存数据来自同一 DB 同一 SELECT 列集；切片逻辑与 SQL
        # WHERE code IN(...) AND time<=? QUALIFY ROW_NUMBER(...)<=count 逐行一致。
        self._bars_history_cache: Dict[tuple, pd.DataFrame] = {}
        # 实例级开关：True 走原 SQL 路径（等价性对比测试/回滚用），False（默认）走内存缓存路径。
        self._use_sql_path = False
        # ---- M1 段一性能优化（2026-09-20，方案件 §1.2 路径 A）：当日单码调用回看式聚合 ----
        # 病灶（Phase 0 实证）：策略每日筛查经 get_history_batch shim 逐 code 发起单码
        # 调用（每码一次），PR7 路径下每日 ~5176 次 _ensure_bars_in_cache 单码装载 SQL
        # （第 1 日 943s 线程锁等待）+ 每日同型 _post 重跑（~87s/日）。
        # 机制：同族单码调用计数达阈值后，用当日已见全集一次批量执行（复用 impl 多码
        # 路径，零新 SQL 语义），结果按码拆存；此后同族单码调用直接命中拆存（深拷贝
        # 返回，防下游变异——get_price 路径对返回 df 有 trade_date 写入）。
        # 语义不变约束：结果与逐次单码执行值等价（同一 impl 代码路径；仅执行时机与
        # 批量度不同；返回经 .copy()，值/dtype/索引逐位一致）。族键=(before_ms,count,
        # use_qfq)；族变化（新交易日/不同参数）即整体重置——绝不跨族命中。
        # T1（2026-09-26）：bars 窗口缓存 —— **唯一** bars 缓存实现（T2 单一真相源）
        # 键 = (code, before_ms, use_qfq)；R3：before_ms 变化（新交易日）即整体回收，
        # 与 ptrade_api _query_cache 每日清空的 PIT 纪律同构
        self._bars_window_cache: Dict[tuple, Optional[pd.DataFrame]] = {}
        self._bars_window_ms: Optional[int] = None
        # 全池共享（2026-09-26）：A 通道写入的当日池 + 预取状态
        # 池 = 「同一份窗口缓存」的**批量填充范围**（非第二缓存，T2 单一真相源）
        self._bars_pool: list = []
        self._bars_pool_set: set = set()
        self._bars_pool_seeded = None
        self._bars_pool_w: int = 0
        self._bars_pool_expansions: int = 0
        self._bars_pool_expand_locked: bool = False

        self._preload_fs: Optional[pd.DataFrame] = None
        self._preload_fs_month: Optional[str] = None
        # 纯性能优化：日线快照内存缓存（query_daily_snapshot 结果）。
        # 由 preload_daily_snapshots 一次性区间预取填充，避免每日对行情表做全表扫描。
        self._daily_snapshot_cache: dict = {}
        self._daily_snapshot_loaded = False
        self._cached_min_ms = None
        self._cached_max_ms = None
        # ---- F-DUCKDB-LOCK（A2）：实例级诊断事件采集（源点落账，不依赖错误传播）----
        # 明细上限 50 条 + 计数器聚合；经 qs_diagnostics() 由引擎 run() 收尾无条件汇总输出。
        self._qs_diag_events: List[dict] = []
        self._qs_diag_counts: Dict[str, int] = {}

    # ===================== 连接管理 =====================

    def _get_conn(self):
        """懒创建持久只读 DuckDB 连接（回测期间复用，性能优化）。

        迁移自 PtradeAPI._get_ro_conn() (ptrade_api.py:341-353)

        F-DUCKDB-LOCK（A2-P3 静默消音）：连接失败仍返回 None（返回契约不变），
        但首次失败发 QS_DUCKDB_CONN_UNAVAILABLE WARNING（db_path + 真实异常文本），
        同类后续计数聚合——杜绝「连接不可得 → 引擎带空数据静默跑」的事故模式。
        """
        if self._ro_conn is not None:
            return self._ro_conn
        try:
            import duckdb as _ddb
            if self._db_path and self._db_path.exists():
                self._ro_conn = _ddb.connect(str(self._db_path), read_only=True)
                return self._ro_conn
        except Exception as e:
            self._qs_record_diag("QS_DUCKDB_CONN_UNAVAILABLE", {
                "db_path": str(self._db_path),
                "error": type(e).__name__ + ": " + str(e)[:200],
            }, warn_first_only=True)
        return None

    # ===================== F-DUCKDB-LOCK（A2）：诊断采集与超时执行 =====================

    def _qs_record_diag(self, kind: str, payload: dict, warn_first_only: bool = False) -> None:
        """诊断事件源点落账：明细上限 50 条 + 每类计数聚合（qs_diagnostics 汇总输出）。

        warn_first_only=True（连接失败类）：仅首次告警+落明细，后续只计数；
        超时类事件每次落账+告警（稀有且关键）。
        """
        n = self._qs_diag_counts.get(kind, 0) + 1
        self._qs_diag_counts[kind] = n
        first = (n == 1)
        if first or not warn_first_only:
            if len(self._qs_diag_events) < 50:
                ev = {"kind": kind}
                ev.update(payload)
                self._qs_diag_events.append(ev)
        if first:
            logger.warning("%s %s", kind, payload)

    def qs_diagnostics(self) -> List[dict]:
        """回测期诊断事件汇总（引擎 run() 收尾无条件调用输出，事后可检）。"""
        out = [dict(ev) for ev in self._qs_diag_events]
        for kind, count in sorted(self._qs_diag_counts.items()):
            if count > 1:
                out.append({"kind": kind, "occurrences": count})
        return out

    def _execute_with_timeout(self, conn, sql: str, params=None):
        """P2（A1 设计）：execute+fetchdf 的 per-statement 观测超时（看门狗 + conn.interrupt）。

        语义钉死：超时 = 单语句预算 QS_DUCKDB_QUERY_TIMEOUT_S（默认 30s），绝非回测级
        总预算；健康分片（单语句 P99 << 预算）永不挨刀。
        机制（duckdb 1.5.5 实测，agent_workspace/a1_interrupt_probe.py）：预算到点调
        conn.interrupt() → duckdb.InterruptException（"INTERRUPT Error: Interrupted!"），
        中断后连接仍可复用（后续 SELECT 正常）。
        超时处置：首次超时落账 QS_DUCKDB_QUERY_TIMEOUT（SQL 片段+预算+耗时+码数+attempt）
        并单次重试同语句（覆盖 CHECKPOINT 类瞬态窗口，重试计入事件；连接可复用已实证）；
        二次超时抛 RuntimeError 带归因（显式失败，禁止静默 None）。
        非超时异常原样透传（不改变异常行为契约）。
        """
        budget = _env_pos_float("QS_DUCKDB_QUERY_TIMEOUT_S", _DEFAULT_QUERY_BUDGET_S)
        sql_head = re.sub(r"\s+", " ", str(sql))[:80]
        n_params = len(params) if params is not None else 0
        attempt = 0
        while True:
            attempt += 1
            box: dict = {}

            def _run():
                try:
                    if params is not None:
                        box["df"] = conn.execute(sql, params).fetchdf()
                    else:
                        box["df"] = conn.execute(sql).fetchdf()
                except BaseException as e:  # 含 InterruptException；超时与否由存活判定区分
                    box["err"] = e

            th = threading.Thread(target=_run, daemon=True)
            t0 = time.time()
            th.start()
            th.join(budget)
            if th.is_alive():
                try:
                    conn.interrupt()
                except Exception:
                    pass
                th.join(10.0)
                elapsed = round(time.time() - t0, 3)
                self._qs_record_diag("QS_DUCKDB_QUERY_TIMEOUT", {
                    "sql": sql_head, "budget_s": budget, "elapsed_s": elapsed,
                    "codes_in_params": n_params, "attempt": attempt,
                    "worker_still_alive": th.is_alive(),
                })
                logger.warning(
                    "QS_DUCKDB_QUERY_TIMEOUT attempt=%d elapsed=%.1fs budget=%.1fs codes=%d sql=%s",
                    attempt, elapsed, budget, n_params, sql_head)
                if attempt == 1 and not th.is_alive():
                    continue  # 单次重试（同语句同预算）
                raise RuntimeError(
                    "QS_DUCKDB_QUERY_TIMEOUT: bulk cache query exceeded per-statement budget "
                    f"twice (budget={budget}s, last={elapsed}s, codes={n_params}, sql={sql_head!r})")
            if "err" in box:
                raise box["err"]  # 非超时异常原样透传
            return box["df"]

    def available(self) -> bool:
        """数据库文件是否可访问（替代原 self._cfg.db_path.exists() 直接判断）。"""
        return self._db_path is not None and self._db_path.exists()

    def close(self):
        """关闭连接"""
        self._tables_cache = None  # 释放表集合缓存，允许重连后看到新表
        self._daily_snapshot_cache = {}  # 释放日线快照缓存
        self._daily_snapshot_loaded = False
        self._cached_min_ms = None
        self._cached_max_ms = None
        if self._ro_conn is not None:
            try:
                self._ro_conn.close()
            except Exception:
                pass
            self._ro_conn = None

    # ===================== 预加载 =====================

    def preload_daily_bars(self, prev_date: str) -> None:
        """预加载全市场行情到内存（仅首次加载，回测期间复用，get_history_from_preload 按 prev_ms 过滤）。

        DEPRECATED/未调用（自 2026-07-25）：当前生产取数路径 get_history/get_history_batch
        走 get_bars_by_count() → query_bars_by_count_multi_table()，不读取 _preload_daily，
        也不调用 get_history_from_preload()；估值 PIT 与上市日期分别由 FundamentalProvider /
        ReferenceProvider 独立管理。DuckDBMarketDataProvider.preload() 已停止调用本方法，
        仅保留为未调用兼容代码，不删除。
        """
        prev_ms = int(pd.Timestamp(prev_date, tz='Asia/Shanghai').timestamp() * 1000) + 86_399_999
        # 估值数据（float_value/pe_ratio）每日重算 PIT 快照
        self.preload_fundamentals_pit(prev_ms, force=False)

        if self._preload_daily is not None:
            # 行情已加载，只更新 prev_ms（_get_history_from_preload 按 prev_ms 过滤）
            self._preload_prev_ms = prev_ms
            return
        try:
            conn = self._get_conn()
            if conn is None:
                return
            # 预加载足够大的行情窗口（回测全期；实测约 400 万行、约 759MiB（约 796MB 十进制），常驻内存。该方法现已不再被调用——保留为未调用兼容代码。）
            self._preload_daily = conn.execute("""
                SELECT code, time, open, high, low, close, volume, amount,
                       pctChg, preClose, turn, peTTM, pbMRQ, psTTM,
                       open_front, high_front, low_front, close_front
                FROM stock_daily
                ORDER BY time DESC
                LIMIT 4000000
            """).fetchdf()
            self._preload_prev_ms = prev_ms
            self._build_preload_daily_code_positions()
            logger.debug(f"[Preload] 加载 {len(self._preload_daily)} 行行情")
            # 预加载每只股票的上市日期（get_security_info 用，避免逐只 MIN(time) 查询）
            # 上市日是历史固定值，无 PIT 问题，可全局加载
            tables = self._existing_tables()
            if "stock_basic" in tables:
                self._preload_listing = conn.execute(
                    "SELECT code, list_date AS listing_time FROM stock_basic "
                    "WHERE list_date IS NOT NULL"
                ).fetchdf()
            else:
                # Compatibility fallback only. Strategy Compiler capability gates
                # requiring formal listing age must reject this fallback.
                self._preload_listing = conn.execute(
                    "SELECT code, MIN(time) as listing_time FROM stock_daily GROUP BY code"
                ).fetchdf()
        except Exception:
            pass

    def preload_fundamentals_pit(self, prev_ms: int, force: bool = False) -> None:
        """按 PIT（prev_ms）刷新估值快照。

        迁移自 PtradeAPI._refresh_fundamentals_pit() (ptrade_api.py:394-503)
        - 每日流通市值（circ_mv/total_mv/pe_ttm/pb/turnover）：来自 stock_daily_valuation，按日 PIT 重算。
        - 报告期总股本（total_share）与流通股本（free_share）：来自 stock_float_share，按月缓存。
        产出 self._preload_float：每只股票 <= prev_ms 的最新一期 circ_mv/pe_ratio 等。
        """
        try:
            conn = self._get_conn()
            if conn is None:
                return

            # ---- 报告期总股本 + 流通股本（total_share/free_share）：按月缓存，跨月重查 ----
            prev_month = pd.Timestamp(prev_ms, unit='ms', tz='Asia/Shanghai').strftime('%Y-%m')
            need_fs = force or self._preload_fs_month != prev_month or self._preload_fs is None
            if need_fs:
                float_share_df = self.query_float_share_for_preload(prev_ms)
                self._preload_fs = float_share_df
                self._preload_fs_month = prev_month
            else:
                float_share_df = self._preload_fs

            # ---- 每日流通市值 + a_floats（优先 free_share，fallback circ_mv/close）----
            # 按日 PIT 重算，不缓存！
            daily_val = self.query_valuation_for_preload(prev_ms)

            if len(daily_val) > 0:
                # 主路径：每日估值 + a_floats（优先 free_share，fallback circ_mv/close），join 报告期 total_share
                self._preload_float = daily_val.merge(
                    float_share_df[['code', 'total_share']], on='code', how='left')
            elif len(float_share_df) > 0:
                # 兼容回退：stock_daily_valuation 无数据时，用 stock_float_share 的 circ_mv
                # 同时从 stock_daily 取 prev_close 推导 a_floats（与主路径口径一致）
                fs_full = conn.execute(f"""
                    SELECT code, circ_mv AS float_value, total_mv AS total_value, total_share
                    FROM stock_float_share
                    WHERE ann_date <= {prev_ms}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY ann_date DESC) = 1
                """).fetchdf()
                daily_latest = conn.execute(f"""
                    WITH latest_day AS (SELECT MAX(time) AS t FROM stock_daily WHERE time <= {prev_ms})
                    SELECT d.code, d.close, d.peTTM AS pe_ratio, d.peTTM AS pe_ttm,
                           d.pbMRQ AS pb_ratio, d.psTTM AS ps_ratio, d.turn AS turnover_ratio
                    FROM stock_daily d, latest_day WHERE d.time = latest_day.t
                """).fetchdf()
                merged = fs_full.merge(daily_latest, on='code', how='left')
                if len(merged) > 0:
                    merged['a_floats'] = merged.apply(
                        lambda r: r['float_value'] / r['close'] if r.get('close') and r['close'] != 0 else None, axis=1)
                self._preload_float = merged
            else:
                self._preload_float = pd.DataFrame()

            logger.debug(f"[Preload] PIT 估值刷新（每日 circ_mv，month={prev_month}）: "
                         f"{len(self._preload_float)} 行 "
                         f"(daily_val={len(daily_val)}, float_share={len(float_share_df)})")
        except Exception:
            pass

    # ===================== 行情查询 =====================

    def query_daily_snapshot(self, date_ms: int) -> pd.DataFrame:
        """迁移自 BacktestEngine._get_daily_data() (backtest_engine.py:724-735)

        支持股票 + ETF：
        - stock_daily 提供完整股票快照
        - etf_daily 补充 ETF 行情，填充统一列结构，便于 Ptrade 策略在同一日同时访问股票/ETF/行业 ETF

        【前复权撮合口径（方案A，2026-07-25 决策）】
        本快照是引擎撮合/估值链路（链路②）的唯一价格来源：成交价、持仓估值、
        data[code].price、check_limit 比较价均取自这里。

        【原始价撮合口径（2026-08-14 PTrade 实证决策，逆转方案A）】
        OHLC 四价取原始价（raw），不再映射前复权列。PTrade 平台实证（4 次真实回测）
        确认撮合/估值用 raw close。preClose 保持行情源标准语义（除权日=除权参考价），
        (raw close - preClose)/preClose 在所有日期均正确（16 标的多板块抽查验证）。
        信号取数链路（链路①，get_history fq='pre'）独立走 query_bars_by_range
        查 *_front 列，不受此快照影响。volume/amount/pctChg 保持原始口径。

        纯性能优化：先查内存缓存（由 preload_daily_snapshots 一次性区间预取填充），
        未命中再回退到单日 per-day 查询；每次返回独立副本，避免跨日共享 DataFrame
        被调用方就地修改而污染缓存。
        """
        cached = self._daily_snapshot_cache.get(date_ms)
        if cached is not None:
            return cached.copy()
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        try:
            # P-D14 D3（2026-08-27 审计通过）：单日查询精确匹配 → 当日窗口匹配。
            # 根因：etf_daily 07-01 存在双 time 值（00:00 组 73 行 + 08:00 组 1974 行含
            # 515050/511260），time=精确匹配漏掉 08:00 组 → 首日 ETF no_price 拒单。
            # 窗口 [date_ms, date_ms+86_399_999] 与 preload 的 BETWEEN 契约对齐（保证
            # 两路径字节级一致）；08:00 在内、次日 00:00 恰好差 1ms 排除（不串日）。
            # 审计细化①：同 code 去重护栏——取最大 time 行（防未来批次异常双行），
            # 与窗口吸收的双值组共存安全（实证 07-01 双 time 重复 code 数=0，护栏为防御）。
            df = conn.execute(
                self._snapshot_sql(
                    f"time >= {date_ms} AND time <= {date_ms + 86_399_999}")
            ).fetchdf()
            if not df.empty:
                df = (df.sort_values('time')
                        .groupby('code', as_index=False, sort=False)
                        .tail(1))
        except Exception as e:
            logger.warning(f"[DuckDB] 日线快照查询失败 date_ms={date_ms}: {e}")
            return pd.DataFrame()
        # 防御（2026-09-18，F2 裁定）：仅当 date_ms 本身恰为 CST 日界时才落缓存。
        # 读键（412）与写键（437）同为 date_ms、逻辑零改动，自洽性不变；本分支只
        # 防一件事——若未来出现**非日界**的 date_ms，其查询窗口
        # [date_ms, date_ms+86_399_999] 已偏离真实当日窗口，结果一旦写入会污染
        # 真实当日键（午夜查询静默命中错位窗口数据）。
        # 纯防御：返回值与异常行为均不变，仅跳过写入。判据走 time_axis 真相源，
        # 不硬编码日界常量（防第二真相源）。
        if is_day_start_ms(date_ms):
            self._daily_snapshot_cache[date_ms] = df.copy()
        return df

    def _snapshot_sql(self, where_clause: str) -> str:
        """日线快照 SELECT（stock_daily UNION ALL etf_daily），per-day 与全期预取共用。

        where_clause 为 `time = {date_ms}` 或 `time BETWEEN {start_ms} AND {end_ms}`，
        列集合与前复权口径完全一致，仅过滤范围不同，保证结果字节级一致。
        """
        return f"""
            SELECT code, time,{_ADJ_OHLC_SQL}
                   volume, amount,
                   pctChg,{_ADJ_PRECLOSE_SQL}
                   turn, peTTM, pbMRQ, isST, suspendFlag,
                   is_st_reliable, is_st_reliable_source,
                   is_delisting_risk, is_delisting_risk_source
            FROM stock_daily
            WHERE {where_clause}
            UNION ALL
            SELECT code, time,{_ADJ_OHLC_SQL}
                   volume, amount,
                   pctChg,{_ADJ_PRECLOSE_SQL}
                   turn,
                   NULL AS peTTM, NULL AS pbMRQ,
                   COALESCE(isST, 0) AS isST,
                   0 AS suspendFlag,
                   FALSE AS is_st_reliable,
                   'etf_daily' AS is_st_reliable_source,
                   FALSE AS is_delisting_risk,
                   'etf_daily' AS is_delisting_risk_source
            FROM etf_daily
            WHERE {where_clause}
        """

    def preload_daily_snapshots(self, start_ms: int, end_ms: int) -> None:
        """纯性能优化：一次性预取 [start_ms, end_ms] 全期日线快照到内存。

        替代回测期间每日对 stock_daily（约 330 万行）做 `WHERE time = X` 全表扫描。
        结果按 time 分组缓存，query_daily_snapshot 直接命中内存；行/列/顺序与单日查询
        字节级一致。幂等：已预取则跳过，避免重复扫描。
        """
        # 按区间覆盖判断（可扩展），避免 attach_day 逐日 preload(prev_date) 提前置位
        # _daily_snapshot_loaded 导致引擎全期 preload 被硬 guard 跳过：全期预取总能扩展
        # 缓存以覆盖全部交易日，逐日/重复预取在已覆盖时直接跳过。
        if (self._daily_snapshot_loaded and self._cached_min_ms is not None
                and self._cached_min_ms <= start_ms and self._cached_max_ms >= end_ms):
            return
        conn = self._get_conn()
        if conn is None:
            self._daily_snapshot_loaded = True
            return
        try:
            df = conn.execute(
                self._snapshot_sql(f"time BETWEEN {start_ms} AND {end_ms}")
            ).fetchdf()
            if not df.empty:
                # P-D14 D3（2026-08-27）：预取缓存按【当日窗口】聚合键（与单日
                # 窗口匹配语义一致）——原实现按 time 精确值分组缓存，08:00 组
                # 作为独立键导致单日查询（time=当日00:00 键）命中不到 08:00 组
                # （T6 实证：预取 5584 行 vs 单日 7558 行，字节级一致契约被破坏）。
                # 修复：08:00 组并入当日 00:00 键（与 query_daily_snapshot 窗口
                # 语义对齐）；去重护栏同单日路径（同日同 code 取最大 time 行）。
                # 修复（2026-09-18，P-D14 D3 回归）：原式 `time // 86_400_000 * 86_400_000`
                # 是 **UTC** 日界截断（结果 mod 恒为 0），而查询侧 query_daily_snapshot
                # 用的 date_ms 来自 _start_ms 的 **CST** 日界（mod 恒为 57_600_000）——
                # 两个键空间交集为空，预取结果 100% 未被消费，每交易日退化为一次
                # 全市场窗口扫描。改用 time_axis.day_start_ms（CST 日界唯一真相源），
                # 预取键与查询键自然重合。
                df = df.assign(_day=day_start_ms(df['time']))
                df = (df.sort_values('time')
                        .groupby(['_day', 'code'], as_index=False, sort=False)
                        .tail(1))
                for day, grp in df.groupby("_day"):
                    self._daily_snapshot_cache[int(day)] = (
                        grp.drop(columns=['_day']).reset_index(drop=True))
                keys = list(self._daily_snapshot_cache.keys())
                self._cached_min_ms = min(min(keys), start_ms)
                self._cached_max_ms = max(max(keys), end_ms)
        except Exception as e:
            logger.warning(f"[DuckDB] 日线快照区间预取失败 [{start_ms},{end_ms}]: {e}")
        self._daily_snapshot_loaded = True

    def query_bars_by_range(self, code, start_ms, end_ms, use_qfq: bool = False) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_price() 的条件分支 (ptrade_api.py:1373-1384)

        start_ms / end_ms 任一为 None 时对应的时间下界/上界条件不生效。

        [R1-A] 新增 use_qfq 参数，使日线区间查询行为与 count 路径
        (query_bars_by_count*) 一致：fq='pre'/'dypre' 返回前复权 OHLC，
        fq=None/'none' 返回 raw OHLC。默认 use_qfq=False 保持历史 raw 行为，
        不破坏旧调用方。

        返回列式与修复前一致（仅 OHLC 列值因 use_qfq 变化）：
        code, time, open, high, low, close, volume, amount,
        preClose, pctChg, turn, peTTM, pbMRQ
        按契约要求，区间路径不向公共列集新增 *_front 列，
        前复用 `open_front AS open` 等别名在 SELECT 内就地替换列值。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        conditions = [f"code = '{code}'"]
        if start_ms is not None:
            conditions.append(f"time >= {start_ms}")
        if end_ms is not None:
            conditions.append(f"time <= {end_ms}")
        where = " AND ".join(conditions)
        if use_qfq:
            # 前复权：用 *_front 列替换公共 OHLC 列值，保持公共列集不变
            ohlc = "open_front AS open, high_front AS high, low_front AS low, close_front AS close"
        else:
            ohlc = "open, high, low, close"
        return conn.execute(f"""
            SELECT code, time, {ohlc}, volume, amount,
                   preClose, pctChg, turn, peTTM, pbMRQ
            FROM stock_daily WHERE {where} ORDER BY time
        """).fetchdf()

    def query_bars_by_count(self, code, count, before_ms) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_price() 的 count 分支 (ptrade_api.py:1363-1371)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            SELECT * FROM (
                SELECT code, time, open, high, low, close, volume, amount,
                       preClose, pctChg, turn, peTTM, pbMRQ
                FROM stock_daily WHERE code = '{code}' AND time <= {before_ms}
                ORDER BY time DESC LIMIT {count}
            ) ORDER BY time
        """).fetchdf()

    def query_bars_by_count_multi_table(self, code, count, before_ms, use_qfq: bool = False) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_history() 的多表 fallback 逻辑 (ptrade_api.py:1211-1243)

        依次尝试 stock_daily → etf_daily → index_daily + INDEX_ETF_MAP fallback
        （000300→510300 等）。返回单只代码的 DataFrame（已排序、已加 trade_date、已按 qfq 替换价）。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        INDEX_ETF_MAP = {"000300": "510300", "000905": "510500",
                         "000016": "510050", "000852": "510880"}
        df = pd.DataFrame()
        for tbl, cols in [("stock_daily", "code, time, open, high, low, close, volume, amount, pctChg, preClose, turn, peTTM, pbMRQ, open_front, high_front, low_front, close_front"),
                          ("etf_daily", "code, time, open, high, low, close, volume, amount, pctChg, preClose, turn, NULL as peTTM, NULL as pbMRQ, open_front, high_front, low_front, close_front"),
                          ("index_daily", "code, time, open, high, low, close, volume, amount, pctChg, NULL as preClose, NULL as turn, NULL as peTTM, NULL as pbMRQ, NULL as open_front, NULL as high_front, NULL as low_front, NULL as close_front")]:
            df = conn.execute(f"""
                SELECT {cols}
                FROM {tbl}
                WHERE code = '{code}' AND time <= {before_ms}
                ORDER BY time DESC LIMIT {int(count)}
            """).fetchdf()
            if len(df) > 0:
                break
        # 指数历史不足 → 用跟踪 ETF 替代（如 000300→510300，动量信号一致）
        if len(df) == 0 and code in INDEX_ETF_MAP:
            etf_code = INDEX_ETF_MAP[code]
            df = conn.execute(f"""
                SELECT code, time, open, high, low, close, volume, amount, pctChg, preClose, turn, NULL as peTTM, NULL as pbMRQ, open_front, high_front, low_front, close_front
                FROM etf_daily WHERE code = '{etf_code}' AND time <= {before_ms}
                ORDER BY time DESC LIMIT {int(count)}
            """).fetchdf()
        if len(df) > 0:
            # fq='pre'/'dypre'：用前复权列替换原始价
            if use_qfq:
                for orig, qfq in [("open", "open_front"), ("high", "high_front"),
                                  ("low", "low_front"), ("close", "close_front")]:
                    if qfq in df.columns and df[qfq].notna().any():
                        df[orig] = df[qfq]
            df = df.sort_values('time')
            df['trade_date'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')
        return df

    def query_index_day_bars(self, code, count, before_ms) -> pd.DataFrame:
        """get_index_day_bar 专用查询（docs/get-index-day-bar-design.md，2026-09-08）。

        与 query_bars_by_count_multi_table 的本质区别（该函数不可复用的根因）：
        - **显式钉表 index_daily**：裸码 000001 在 stock_daily（平安银行）与
          index_daily（上证指数）同期并存，multi_table 的 stock→etf→index
          先命中即断 fallback 链会静默命中平安银行——指数分支不可达。
        - **绝不触发 INDEX_ETF_MAP ETF 代理替换**："指数日线 API 永不静默
          换 ETF 数据"（终审钉死）。
        - 不做 qfq 列替换（指数无 *_front 列、无复权，raw 即契约）。

        返回：截至 before_ms 的最近 count 根已完成指数日线，时间升序、
        含 trade_date 列（与 multi_table 后处理口径一致）；无数据返回空 DataFrame。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        try:
            df = self._execute_with_timeout(
                conn,
                "SELECT code, time, open, high, low, close, volume, amount, pctChg "
                "FROM index_daily WHERE code = ? AND time <= ? "
                "ORDER BY time DESC LIMIT ?",
                [str(code), int(before_ms), int(count)])
        except Exception as e:
            logger.warning("QS_INDEX_BAR_QUERY_FAIL code=%s err=%s", code, e)
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.sort_values('time').reset_index(drop=True)
        df['trade_date'] = pd.to_datetime(df['time'], unit='ms', utc=True)\
            .dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')
        return df

    def _ensure_bars_in_cache(self, codes, table, cols) -> None:
        """PR7：确保 codes 的全历史数据在 _bars_history_cache 中（惰性批量加载）。

        只查未命中的 code（参数化占位），加载后按 (code, time) 升序缓存。
        缓存保存原始列（未做 qfq/trade_date 后处理），后处理统一由
        query_bars_by_count_batch 的 _post 完成——与 SQL 路径共享同一后处理逻辑。

        F-DUCKDB-LOCK（A2-P1/P2，2026-09-05）：
        - 等价分片：missing 码集按 QS_BARS_CACHE_CHUNK_SIZE（默认 200 码/片）切片，
          逐片 WHERE code IN 加载——只切码集，不改 SELECT 列集/排序/后处理。
          等价性论证：分片按码集划分，任一 code 的全部行必然落在同一片内，
          每片独立 sort_values(["code","time"]) 后 groupby 的组内行序与单条 SQL
          一致（bar 主键 (code,time) 唯一 → 组内排序确定）；缓存按键 (table,code)
          读取，填充顺序无语义。A2 单测含分片 vs 单条逐行等价断言。
        - 超时保护：每片经 _execute_with_timeout（per-statement 预算）——消除
          「大 IN 全历史 SELECT 分钟级无输出 → 被观测为挂起 → 40min 清理器终止」
          的 F-DUCKDB-LOCK 主因；超时显式归因失败（禁止静默 None）。
        - 进度心跳：总耗时 >1s 或多片时输出 QS_BARS_CACHE_PROGRESS
          （loaded/总码数 + 片数 + 耗时——A1 复核要求的进度信息）。
        """
        missing = [c for c in codes if (table, c) not in self._bars_history_cache]
        if not missing:
            return
        conn = self._get_conn()
        if conn is None:
            return
        chunk_size = max(1, int(_env_pos_float(
            "QS_BARS_CACHE_CHUNK_SIZE", _BARS_CACHE_CHUNK_DEFAULT)))
        t0 = time.time()
        loaded = 0
        for i in range(0, len(missing), chunk_size):
            part = missing[i:i + chunk_size]
            placeholders = ", ".join(["?"] * len(part))
            df = self._execute_with_timeout(
                conn,
                f"SELECT {cols} FROM {table} WHERE code IN ({placeholders})",
                part)
            if df is None or df.empty:
                continue
            df = df.sort_values(["code", "time"]).reset_index(drop=True)
            for c, sub in df.groupby("code", sort=False):
                self._bars_history_cache[(table, c)] = sub.reset_index(drop=True)
                loaded += 1
        elapsed = time.time() - t0
        if elapsed > 1.0 or len(missing) > chunk_size:
            logger.info(
                "QS_BARS_CACHE_PROGRESS table=%s loaded=%d/%d chunks=%d elapsed=%.1fs",
                table, loaded, len(missing),
                (len(missing) + chunk_size - 1) // chunk_size, elapsed)

    # T1（2026-09-26）：窗口缓存哨兵与最小窗口（覆盖本场景最大 count=27 并留余量）
    _BARS_MISS = object()          # 「未入缓存」哨兵（区别于 None=已入缓存但无数据）
    _BARS_WINDOW_MIN = 60
    # 全池共享（2026-09-26）：池预取最小窗口 / 扩窗上限 / 超限后的固定窗口
    _POOL_W_MIN = 30
    _POOL_EXPAND_LIMIT = 2
    _POOL_FIXED_W = 60

    def set_pool(self, codes) -> None:
        """A 通道（2026-09-26）：由 ptrade_api 在 set_universe(...)/attach_day 写入当日池。

        池 = 「同一份 bars 窗口缓存」的**批量填充范围**（非第二缓存 —— T2 单一真相源）。
        池变更即重置预取状态；日切换（before_ms 变化）由 query_bars_by_count_batch 回收。
        池为空/未声明 ⇒ 自然退化为按需填充同一缓存（零额外代码路径）。
        """
        _new = set(codes) if codes else set()
        if _new == self._bars_pool_set:
            return          # 幂等（2026-09-26）：池未变 ⇒ 不重置预取状态/窗口，避免打断命中
        self._bars_pool = sorted(_new)
        self._bars_pool_set = _new
        self._bars_pool_seeded = None
        self._bars_pool_w = 0
        self._bars_pool_expansions = 0
        self._bars_pool_expand_locked = False

    def query_bars_by_count_batch(self, codes, count, before_ms, use_qfq: bool = False) -> Dict[str, pd.DataFrame]:
        """单码调用**窗口缓存 + count 内存切片**（T1，2026-09-26 · 纯性能优化）。

        动因（族键诊断实证）：原 M1 单码回看式聚合以 (before_ms, count, use_qfq) 为族键，
        而同日内 count=5 与 count=27 **交替调用** ⇒ 每次切换即整体重置 ⇒ 聚合阈值(50)
        永不触发 ⇒ 逐码 SQL + 逐次 _post（含 _build_trade_date_map）⇒ 实测 1,334 次/日
        （断板反包：SQL 占 6%、Python 占 94%）。

        新契约（T1 逐位比对预证实录：400 codes × {count=5,27} ⇒ 12,799 行，不一致项 0）：
        - 缓存键 = **(code, before_ms, use_qfq)**：按**时间窗**缓存足量窗口（≥ 本场景最大 count），
          count 经 `tail(count)` **内存切片**返回 —— 同一 (code, before_ms) 下不同 count
          共享同一份底层数据（选项①「复用批量结果」在单码路径的落地形态）；
        - **切片后 reset_index(drop=True)**：对齐逐次 SQL 的 0-based index 契约
          （预证首次 FAIL 的唯一实差即 index，非值差异 —— 该纪律性停止避免了 index 契约漂移）；
        - **R3 按日回收**：`before_ms` 变化（新交易日）即整体清空 —— 与 ptrade_api
          `_query_cache` 每日清空的 PIT 纪律同构（绝不跨日复用），并天然按日释放内存；
        - 多码调用 / SQL 路径开关：完全透传 impl（与既有行为一致）；
        - **T2 单一真相源**：本函数为 bars 缓存的唯一实现；原 M1 族键聚合（`_agg_*`）已退役，
          避免双缓存真相源。
        分钟路径不经过本函数（走 query_minute_bars_by_count_batch），零触碰。
        """
        if self._use_sql_path or len(codes) != 1:
            return self._query_bars_by_count_batch_impl(codes, count, before_ms, use_qfq)
        code = codes[0]
        bms = int(before_ms)
        qfq = bool(use_qfq)
        if bms != self._bars_window_ms:          # R3：日/窗口切换 → 整体回收（PIT 隔离）
            self._bars_window_cache = {}
            self._bars_window_ms = bms
            self._bars_pool_seeded = None        # 池状态同步回收（绝不跨日复用）
            self._bars_pool_w = 0
            self._bars_pool_expansions = 0
            self._bars_pool_expand_locked = False
        need = int(count)
        key = (code, bms, qfq)
        # 池填充路径：池就绪且当日未预取 ⇒ 一次全池批量预填「同一份窗口缓存」
        if self._bars_pool_set and self._bars_pool_seeded != (bms, qfq):
            W = max(need, self._POOL_W_MIN)
            for _c, _df in self._query_bars_window_sql(list(self._bars_pool), W, bms, qfq).items():
                self._bars_window_cache[(_c, bms, qfq)] = _df
            self._bars_pool_seeded = (bms, qfq)
            self._bars_pool_w = W
        wide = self._bars_window_cache.get(key, self._BARS_MISS)
        # 池窗口不足 ⇒ 扩窗（重新批量）；超上限 ⇒ 锁定固定 W（防御性条款，§2.3）
        if (wide is not self._BARS_MISS and wide is not None and 0 < len(wide) < need
                and self._bars_pool_set):
            self._bars_pool_expansions += 1
            if self._bars_pool_expansions > self._POOL_EXPAND_LIMIT:
                self._bars_pool_expand_locked = True
            W = self._POOL_FIXED_W if self._bars_pool_expand_locked else max(need, self._bars_pool_w + 30)
            for _c, _df in self._query_bars_window_sql(list(self._bars_pool), W, bms, qfq).items():
                self._bars_window_cache[(_c, bms, qfq)] = _df
            self._bars_pool_w = W
            wide = self._bars_window_cache.get(key, self._BARS_MISS)
        # 按需填充路径（池缺失 / 非池 code）：填「同一份缓存」
        if wide is self._BARS_MISS:
            _need = max(need, self._BARS_WINDOW_MIN)
            wide = self._query_bars_by_count_batch_impl([code], _need, bms, qfq).get(code)
            self._bars_window_cache[key] = wide
        if wide is None or len(wide) == 0:
            return {}
        return {code: wide.tail(need).reset_index(drop=True)}

    def _query_bars_window_sql(self, codes, count, before_ms, use_qfq: bool = False) -> Dict[str, pd.DataFrame]:
        """窗口批量 SQL（单一实现）：主表路由 stock→etf→index + INDEX_ETF_MAP fallback。

        2026-10-01（bars 池预取窗口 SQL 化，纯性能型）：从
        `_query_bars_by_count_batch_impl` 的 **SQL 分支逐行抽取**（零逻辑改动）——
        - impl 的 `_use_sql_path=True` 分支改调本方法（回滚语义保持）；
        - **池预取**（`query_bars_by_count_batch`）改调本方法 ⇒ 只取 W 根窗口，
          不再触发 `_ensure_bars_in_cache` 的**全历史加载**（本件性能收益来源：
          原路径拉全历史（每码数千行）后仅 `tail(count)` 取窗口，Python 后处理占 86%）。

        语义：每码 time<=before_ms 的最近 count 根（QUALIFY ROW_NUMBER<=count），
        列集/排序/qfq/trade_date 后处理与逐只版一致（P2' 逐位比对 600 项 0 不一致）。
        """
        conn = self._get_conn()
        if conn is None:
            return {}
        if not codes:
            return {}
        count = int(count)
        INDEX_ETF_MAP = {"000300": "510300", "000905": "510500",
                         "000016": "510050", "000852": "510880"}
        TABLE_COLS = {
            "stock_daily": "code, time, open, high, low, close, volume, amount, pctChg, preClose, turn, peTTM, pbMRQ, open_front, high_front, low_front, close_front",
            "etf_daily": "code, time, open, high, low, close, volume, amount, pctChg, preClose, turn, NULL as peTTM, NULL as pbMRQ, open_front, high_front, low_front, close_front",
            "index_daily": "code, time, open, high, low, close, volume, amount, pctChg, NULL as preClose, NULL as turn, NULL as peTTM, NULL as pbMRQ, NULL as open_front, NULL as high_front, NULL as low_front, NULL as close_front",
        }
        ETF_FALLBACK_COLS = ("code, time, open, high, low, close, volume, amount, pctChg, preClose, "
                             "turn, NULL as peTTM, NULL as pbMRQ, open_front, high_front, low_front, close_front")

        result: Dict[str, pd.DataFrame] = {}

        def _post(df, use_qfq):
            """向量化后处理（与 impl 同款逐行一致）。"""
            df = df.sort_values(["code", "time"]).reset_index(drop=True)
            if use_qfq:
                for orig, qfq in (("open", "open_front"), ("high", "high_front"),
                                  ("low", "low_front"), ("close", "close_front")):
                    if qfq in df.columns:
                        grp = df[qfq].notna().groupby(df["code"]).transform("max")
                        df.loc[grp, orig] = df.loc[grp, qfq]
            df["trade_date"] = _build_trade_date_map(df["time"])
            return df

        for tbl, cols in (("stock_daily", TABLE_COLS["stock_daily"]),
                          ("etf_daily", TABLE_COLS["etf_daily"]),
                          ("index_daily", TABLE_COLS["index_daily"])):
            remaining = [c for c in codes if c not in result]
            if not remaining:
                break
            placeholders = ", ".join(["?"] * len(remaining))
            sql = (f"SELECT {cols} FROM {tbl} "
                   f"WHERE code IN ({placeholders}) AND time <= ? "
                   f"QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) <= ?")
            df = conn.execute(sql, remaining + [before_ms, count]).fetchdf()
            if df is None or df.empty:
                continue
            df = _post(df, use_qfq)
            for c, sub in df.groupby("code", sort=False):
                result[c] = sub.reset_index(drop=True)

        # ---- INDEX_ETF_MAP fallback：仍未命中的指数代码用跟踪 ETF 代理批量查一次 ----
        missing = [c for c in codes if c not in result and c in INDEX_ETF_MAP]
        if missing:
            proxy_map = {INDEX_ETF_MAP[c]: c for c in missing}
            proxies = list(proxy_map.keys())
            placeholders = ", ".join(["?"] * len(proxies))
            sql = (f"SELECT {ETF_FALLBACK_COLS} FROM etf_daily "
                   f"WHERE code IN ({placeholders}) AND time <= ? "
                   f"QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) <= ?")
            df = conn.execute(sql, proxies + [before_ms, count]).fetchdf()
            if df is not None and not df.empty:
                df = _post(df, use_qfq)
                for proxy_code, sub in df.groupby("code", sort=False):
                    result[proxy_map[proxy_code]] = sub.reset_index(drop=True)
        return result

    def _query_bars_by_count_batch_impl(self, codes, count, before_ms, use_qfq: bool = False) -> Dict[str, pd.DataFrame]:
        """阶段1 批量化：与 query_bars_by_count_multi_table 逐只调用字节级等价，
        但用单次/少量批量 SQL 取代 N 次单码 SQL（O(N) -> O(1)）。

        语义约束（逐项对齐单码版 query_bars_by_count_multi_table）：
        - 每只代码取 time <= before_ms 的最近 N 根（ROW_NUMBER PARTITION BY code
          ORDER BY time DESC <= N）。
        - 三表优先级：按代码类型路由到 stock/etf/index 单表（stock 命中即不查 etf/index），
          与 _resolve_minute_table 同款 is_etf/is_index 路由。
        - INDEX_ETF_MAP fallback：指数代码在 index_daily 取空时，用跟踪 ETF 代理批量查一次。
        - use_qfq=True 时用 *_front 替换 open/high/low/close（与单码版 L400-404 一致）。
        - 生成 trade_date 列（与单码版 L406 一致）。
        - 返回 {code: DataFrame}，列/行/排序/qfq/trade_date 与逐只版逐行一致。
        code 用参数化占位（code IN (?, ?, ...)），不拼 f-string，消除 SQL 注入。
        """
        # 2026-10-01（bars 池预取窗口 SQL 化）：SQL 路径抽取至 _query_bars_window_sql
        # （单一实现，逐行等价搬运；回滚开关 _use_sql_path 语义保持）。
        # 本方法其余部分仅剩 PR7 内存缓存路径（按需填充；池预取已改走窗口 SQL）。
        if self._use_sql_path:
            return self._query_bars_window_sql(codes, count, before_ms, use_qfq)
        conn = self._get_conn()
        if conn is None:
            return {}
        if not codes:
            return {}
        count = int(count)
        INDEX_ETF_MAP = {"000300": "510300", "000905": "510500",
                         "000016": "510050", "000852": "510880"}
        # 各表列定义（与单码版 SELECT 列集完全一致）
        TABLE_COLS = {
            "stock_daily": "code, time, open, high, low, close, volume, amount, pctChg, preClose, turn, peTTM, pbMRQ, open_front, high_front, low_front, close_front",
            "etf_daily": "code, time, open, high, low, close, volume, amount, pctChg, preClose, turn, NULL as peTTM, NULL as pbMRQ, open_front, high_front, low_front, close_front",
            "index_daily": "code, time, open, high, low, close, volume, amount, pctChg, NULL as preClose, NULL as turn, NULL as peTTM, NULL as pbMRQ, NULL as open_front, NULL as high_front, NULL as low_front, NULL as close_front",
        }
        ETF_FALLBACK_COLS = ("code, time, open, high, low, close, volume, amount, pctChg, preClose, "
                             "turn, NULL as peTTM, NULL as pbMRQ, open_front, high_front, low_front, close_front")

        result: Dict[str, pd.DataFrame] = {}

        def _post(df, use_qfq):
            """向量化后处理：对整张大 DataFrame 一次性完成排序 + qfq 替换 + trade_date
            生成，外层 groupby 仅做切片（不再逐只 sort/qfq/trade_date）。

            与原「逐只 _post」逐行字节级等价：
            - 排序：整表按 (code, time) 排一次，组内即 time 升序，等价于原每子集
              sort_values("time")。
            - qfq 替换：用 per-code 的 notna().groupby(code).transform("max") 掩码，
              仅当该 code 的 *_front 列存在任意非空才整列替换（与原 notna().any() 守卫
              逐行一致，含 qfq 局部为空时原列留 NaN 的边界）。
            - trade_date：整表一次性生成，与原逐行格式（%Y-%m-%d）一致。
            """
            # 整表排序一次（组内 time 升序），替代 5000 次子集 sort_values。
            df = df.sort_values(["code", "time"]).reset_index(drop=True)
            if use_qfq:
                for orig, qfq in (("open", "open_front"), ("high", "high_front"),
                                  ("low", "low_front"), ("close", "close_front")):
                    if qfq in df.columns:
                        # per-code 守卫：该 code 的 qfq 列任意非空 -> 整列替换原始列。
                        grp = df[qfq].notna().groupby(df["code"]).transform("max")
                        df.loc[grp, orig] = df.loc[grp, qfq]
            df["trade_date"] = _build_trade_date_map(df["time"])
            return df

        # ---- 逐表优先级 stock -> etf -> index，与单码版完全一致（一只代码只取一张表）----
        for tbl, cols in (("stock_daily", TABLE_COLS["stock_daily"]),
                          ("etf_daily", TABLE_COLS["etf_daily"]),
                          ("index_daily", TABLE_COLS["index_daily"])):
            remaining = [c for c in codes if c not in result]
            if not remaining:
                break
            # ---- PR7 内存缓存路径：全历史预加载 + time<=before 切片 + tail(count) ----
            # （SQL 路径已抽取至 _query_bars_window_sql；本方法头部按 _use_sql_path 分流）
            # 与 SQL 路径逐行等价：缓存来自同一 SELECT 列集；每组取
            # time<=before_ms 的最近 count 根（缓存已按 (code,time) 升序），
            # 后处理统一走 _post（排序/qfq 替换/trade_date 与 SQL 版完全一致）。
            self._ensure_bars_in_cache(remaining, tbl, cols)
            slices = []
            for c in remaining:
                full = self._bars_history_cache.get((tbl, c))
                if full is None:
                    continue
                sub = full[full["time"] <= before_ms]
                if sub.empty:
                    continue
                slices.append(sub.tail(count))
            if not slices:
                continue
            df = _post(pd.concat(slices, ignore_index=True), use_qfq)
            for c, sub in df.groupby("code", sort=False):
                result[c] = sub.reset_index(drop=True)

        # ---- INDEX_ETF_MAP fallback：仍未命中的指数代码用跟踪 ETF 代理批量查一次 ----
        missing = [c for c in codes if c not in result and c in INDEX_ETF_MAP]
        if missing:
            proxy_map = {INDEX_ETF_MAP[c]: c for c in missing}  # proxy_code -> 原 index code
            proxies = list(proxy_map.keys())
            # PR7 内存缓存路径：代理 ETF 同样从全历史缓存切片（复用 etf_daily 缓存）。
            # （SQL 路径的代理 fallback 已抽取至 _query_bars_window_sql）
            self._ensure_bars_in_cache(proxies, "etf_daily", ETF_FALLBACK_COLS)
            slices = []
            for p in proxies:
                full = self._bars_history_cache.get(("etf_daily", p))
                if full is None:
                    continue
                sub = full[full["time"] <= before_ms]
                if sub.empty:
                    continue
                slices.append(sub.tail(count))
            if slices:
                df = _post(pd.concat(slices, ignore_index=True), use_qfq)
                for proxy_code, sub in df.groupby("code", sort=False):
                    result[proxy_map[proxy_code]] = sub.reset_index(drop=True)
        return result

    # ===================== PR3: 分钟 bar 查询 =====================

    def _resolve_minute_table(self, code: str) -> Optional[str]:
        """PR3: 根据 code 类型解析对应的分钟表名。

        分类顺序：is_etf 先于 is_index（ETF 代码如 510300 不被指数规则误判）。
        返回 "stock_minutes" / "etf_minutes" / None（指数/可转债等无分钟表）。
        """
        from ..libs.security_code_rules import is_etf, is_index, is_convertible_bond
        # ETF 先判断（ETF 代码可能与指数区间重叠，如 510300）
        if is_etf(code):
            return "etf_minutes"
        # 指数/可转债无对应分钟表（index_minutes/cb_minutes 不存在）→ None → TABLE_MISSING
        if is_index(code) or is_convertible_bond(code):
            return None
        return "stock_minutes"

    def query_minute_bars_by_range(
        self, code: str, start_date: str, end_date: str, storage_freq: str,
        fq: Optional[str] = None, calendar_provider=None,
        bar_cutoff_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        """PR3: 查 stock_minutes/etf_minutes 的指定原生 freq（区间查询）。

        - 不聚合、不回退日线（严禁回退）。
        - 时段过滤用 Python 侧生成的 epoch 毫秒窗口（修正 v1 的 time%N bug）。
        - 表不存在/空/无该 freq → raise FrequencyCapabilityError（code 三分级）。
        - 复权：fq='pre'/'dypre' 用 *_front 列替换 OHLC；preClose 保持原始（已知简化）。
        - PR4 缺口 1：bar_cutoff_ms 非 None 时（分钟 Profile），当日窗口截断到此值（含当前 bar），
          防止未来 bar 泄漏；None 时走 PR3 原逻辑（end_date 当天 23:59:59 截断）。
        """
        from .frequency_labels import (
            FrequencyCapabilityError, ERR_TABLE_MISSING, ERR_TABLE_EMPTY,
            ERR_FREQ_NOT_IN_TABLE, api_to_storage)
        from .intraday_windows import (
            _as_date_str, build_intraday_sql_conditions, iter_trading_days_in_range)

        table = self._resolve_minute_table(code)
        if table is None:
            raise FrequencyCapabilityError(
                ERR_TABLE_MISSING, api_freq=None,
                table=f"index_minutes（指数无对应分钟表）",
                detail=f"code={code} 是指数，无分钟表")

        conn = self._get_conn()
        if conn is None:
            raise FrequencyCapabilityError(
                ERR_TABLE_EMPTY, api_freq=None, table=table,
                detail="DuckDB 连接不可用")

        # 确认该 code 在表中有数据（防 table_empty 静默返回空冒充）
        cnt_row = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE code = ?", [code]).fetchone()
        cnt = cnt_row[0] if cnt_row else 0
        if cnt == 0:
            raise FrequencyCapabilityError(
                ERR_TABLE_EMPTY, api_freq=None, table=table,
                detail=f"code={code} 在 {table} 中无数据")

        # 确认该 freq 存在（列出可用 freq 集合，便于调用方定位缺口）
        avail_rows = conn.execute(
            f"SELECT DISTINCT freq FROM {table} WHERE code = ?", [code]).fetchall()
        avail = [r[0] for r in avail_rows]
        if storage_freq not in avail:
            raise FrequencyCapabilityError(
                ERR_FREQ_NOT_IN_TABLE, api_freq=None, storage_freq=storage_freq,
                table=table, available_freqs=avail,
                detail=f"{table} 有数据但缺 freq={storage_freq}")

        # 时段窗口（本轮修正：Python 侧生成 epoch 毫秒区间）
        # F-LOCAL-MIN（B2 双保险）：调用方可能传 pd.Timestamp（ptrade_api anchor_date），
        # 先归一为 'YYYY-MM-DD'（B1 定谳根因：Timestamp 切片 TypeError 被上层吞成静默空）。
        day_strs = iter_trading_days_in_range(
            _as_date_str(start_date), _as_date_str(end_date), calendar_provider)
        # PR4 缺口 1：分钟 Profile 传 bar_cutoff_ms（当前 bar 的 epoch 毫秒），
        # 当日窗口截断到此值（含当前 bar，end-labeled 下已完成是可见历史）；
        # None 时走 PR3 原逻辑（end_date 当天 23:59:59 截断，日级全天窗口）。
        end_cutoff_ms = bar_cutoff_ms if bar_cutoff_ms is not None else self._end_ms(end_date)
        where_clause, win_params = build_intraday_sql_conditions(day_strs, end_cutoff_ms)

        df = conn.execute(f"""
            SELECT code, time, freq, open, high, low, close, volume, amount, preClose,
                   open_front, high_front, low_front, close_front,
                   open_back, high_back, low_back, close_back,
                   suspendFlag
            FROM {table}
            WHERE code = ? AND freq = ? AND ({where_clause})
            ORDER BY time
        """, [code, storage_freq] + win_params).fetchdf()

        # 复权替换（补齐 3：preClose 保持原始，已知简化）
        fq_norm = str(fq).lower() if fq else ""
        if fq_norm in ('pre', 'dypre'):
            for orig, qfq in [("open", "open_front"), ("high", "high_front"),
                              ("low", "low_front"), ("close", "close_front")]:
                if qfq in df.columns and df[qfq].notna().any():
                    df[orig] = df[qfq]
        elif fq_norm in ('post', 'dyback', 'dy_post'):
            for orig, qfq in [("open", "open_back"), ("high", "high_back"),
                              ("low", "low_back"), ("close", "close_back")]:
                if qfq in df.columns and df[qfq].notna().any():
                    df[orig] = df[qfq]
        return df

    def query_minute_bars_by_range_batch(
        self, codes, start_date: str, end_date: str, storage_freq: str,
        fq: Optional[str] = None, calendar_provider=None,
        bar_cutoff_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        """PR4 真实数据修复（2026-07-22）：批量查多 code 的分钟 bar，一次 SQL per 表。

        替代 _load_minute_snapshots 的逐 code 循环（5525 只 × 4 次查询 = 2.2 万次 DB 调用，
        导致 duckdb C 扩展 GIL 累积崩溃）。本方法按表分组（stock_minutes/etf_minutes），
        每表一次 SQL（WHERE code IN (...) AND freq=? AND 时段窗口），单日 DB 往返 ≤ 2 次。

        契约对齐 query_minute_bars_by_range：
        - 复权替换（fq='pre'/'post'）逻辑一致
        - 时段窗口（iter_trading_days_in_range + bar_cutoff_ms）一致
        - FrequencyCapabilityError 语义：整表空（所有 code 都无数据）才 raise TABLE_EMPTY；
          个别 code 无数据自然不在结果集（与原"逐 code 跳过"一致）
        - 指数/可转债 code（_resolve_minute_table=None）自动跳过，不报错
        """
        from .frequency_labels import FrequencyCapabilityError, ERR_TABLE_EMPTY
        from .intraday_windows import (
            _as_date_str, build_intraday_sql_conditions, iter_trading_days_in_range)
        import pandas as pd

        codes = [str(c) for c in (codes or []) if c is not None and str(c).strip()]
        if not codes:
            return pd.DataFrame()

        # 按表分组（stock_minutes / etf_minutes），指数/可转债（None）跳过
        table_codes = {}  # table -> [codes]
        for code in codes:
            table = self._resolve_minute_table(code)
            if table is None:
                continue  # 指数/可转债无分钟表，跳过（与原 except TABLE_MISSING 一致）
            table_codes.setdefault(table, []).append(code)

        if not table_codes:
            return pd.DataFrame()

        conn = self._get_conn()
        if conn is None:
            raise FrequencyCapabilityError(
                ERR_TABLE_EMPTY, api_freq=None, table="stock_minutes/etf_minutes",
                detail="DuckDB 连接不可用")

        # F-LOCAL-MIN（B2 双保险）：同上，入参归一防 Timestamp 契约违约。
        day_strs = iter_trading_days_in_range(
            _as_date_str(start_date), _as_date_str(end_date), calendar_provider)
        end_cutoff_ms = bar_cutoff_ms if bar_cutoff_ms is not None else self._end_ms(end_date)
        where_clause, win_params = build_intraday_sql_conditions(day_strs, end_cutoff_ms)

        parts = []
        any_table_has_freq = False
        for table, tbl_codes in table_codes.items():
            # 整表 freq 存在性检查（一次，非每 code）
            freq_check = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE freq = ? AND code IN (SELECT unnest(?))",
                [storage_freq, tbl_codes]).fetchone()
            if freq_check[0] == 0:
                # 该表这批 code 无此 freq 数据，跳过（整表空在最后统一判断）
                continue
            any_table_has_freq = True
            # 一次 SQL 拉全 code（DuckDB unnest 参数化，避免 SQL 过长）
            df = conn.execute(f"""
                SELECT code, time, freq, open, high, low, close, volume, amount, preClose,
                       open_front, high_front, low_front, close_front,
                       open_back, high_back, low_back, close_back,
                       suspendFlag
                FROM {table}
                WHERE freq = ? AND code IN (SELECT unnest(?)) AND ({where_clause})
                ORDER BY time
            """, [storage_freq, tbl_codes] + win_params).fetchdf()
            if len(df) > 0:
                parts.append(df)

        if not any_table_has_freq:
            raise FrequencyCapabilityError(
                ERR_TABLE_EMPTY, api_freq=None, table="stock_minutes/etf_minutes",
                detail=f"全 universe 在 {start_date}~{end_date} 无 freq={storage_freq} 分钟数据")

        if not parts:
            return pd.DataFrame()
        result = pd.concat(parts, ignore_index=True)
        result = result.sort_values('time').reset_index(drop=True)

        # 复权替换（与单 code 版完全一致）
        fq_norm = str(fq).lower() if fq else ""
        if fq_norm in ('pre', 'dypre'):
            for orig, qfq in [("open", "open_front"), ("high", "high_front"),
                              ("low", "low_front"), ("close", "close_front")]:
                if qfq in result.columns and result[qfq].notna().any():
                    result[orig] = result[qfq]
        elif fq_norm in ('post', 'dyback', 'dy_post'):
            for orig, qfq in [("open", "open_back"), ("high", "high_back"),
                              ("low", "low_back"), ("close", "close_back")]:
                if qfq in result.columns and result[qfq].notna().any():
                    result[orig] = result[qfq]
        return result


    def query_minute_bars_by_count(
        self, code: str, count: int, end_date: str, storage_freq: str,
        fq: Optional[str] = None, calendar_provider=None,
        bar_cutoff_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        """PR3: 查 stock_minutes/etf_minutes 的指定原生 freq（count 查询）。

        PR3 先实现"end_date 当日收盘前 N 根 bar"（不跨日回溯）；
        跨日 count 语义留待真实数据校准，文档注明。
        PR4 缺口 1：bar_cutoff_ms 非 None 时（分钟 Profile），count 从当前 bar 往前数（含当前 bar）。
        """
        df = self.query_minute_bars_by_range(
            code, end_date, end_date, storage_freq, fq, calendar_provider,
            bar_cutoff_ms=bar_cutoff_ms)
        if len(df) > count:
            df = df.tail(count).reset_index(drop=True)
        return df

    def query_minute_bars_by_count_batch(
        self, codes, count: int, end_date: str, storage_freq: str,
        fq: Optional[str] = None, calendar_provider=None,
        bar_cutoff_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        """F-LOCAL-MIN/B2'（2026-09-06）：分钟 count 查询跨日语义批量版。

        语义（平台 QSPROBE 定谳）：截止当前 bar（time <= cutoff）的最近 N 根，可跨交易日。
        与日线 query_bars_by_count_batch 同款窗口函数模式（QUALIFY ROW_NUMBER ... <= N），
        单 SQL、天然 PIT（time <= cutoff 天然排除未来 bar）。

        - cutoff：bar_cutoff_ms 非 None 时用之（分钟 Profile 含/不含当前 bar 由调用方
          按 include 语义折算——ptrade_api L1266-1281 既有逻辑不变）；None 时用
          end_date 当日 23:59:59.999（等价 PR3 原口径）。
        - freq 缺失/表缺失语义：复用既有三分类预检（_resolve_minute_table + 表级 freq
          检查），FrequencyCapabilityError 原样抛出（行为契约不变）。
        - 分片：沿用 A2 等价分片纪律（200 码/片 + _execute_with_timeout + 心跳）——
          本方法码集即分片（codes 全量入单 SQL 的 QUALIFY 窗口，缺失码自然不在结果集，
          与 Phase 4A「个别 code 无数据跳过」一致）。
        - 曝光于既有欠账：单只版 query_minute_bars_by_count（L994-1010，docstring 自认
          「不跨日回溯，跨日语义留待真实数据校准」）——本方法即该欠账的清偿（B2'），
          单只版保留不动（既有调用方行为不变），新调用方应使用本方法。
        """
        from .frequency_labels import FrequencyCapabilityError, ERR_TABLE_EMPTY
        from .intraday_windows import _as_date_str
        codes = [str(c) for c in (codes or []) if c is not None and str(c).strip()]
        if not codes:
            return pd.DataFrame()
        conn = self._get_conn()
        if conn is None:
            raise FrequencyCapabilityError(
                ERR_TABLE_EMPTY, api_freq=None, table="stock_minutes/etf_minutes",
                detail="DuckDB 连接不可用")
        cutoff_ms = (bar_cutoff_ms if bar_cutoff_ms is not None
                     else self._end_ms(_as_date_str(end_date)))
        # 按表分组（同 Phase 4A：stock/etf 路由；指数/可转债 None 跳过）
        table_codes = {}
        for code in codes:
            table = self._resolve_minute_table(code)
            if table is None:
                continue
            table_codes.setdefault(table, []).append(code)
        if not table_codes:
            return pd.DataFrame()
        parts = []
        for table, tbl_codes in table_codes.items():
            placeholders = ", ".join(["?"] * len(tbl_codes))
            sql = (
                f"SELECT * FROM (SELECT code, time, freq, open, high, low, close, volume, amount, "
                f"preClose, open_front, high_front, low_front, close_front, open_back, high_back, "
                f"low_back, close_back FROM {table} "
                f"WHERE freq = ? AND time <= ? AND code IN ({placeholders}) "
                f"QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) <= {int(count)}) "
                f"ORDER BY code, time")
            params = [storage_freq, cutoff_ms] + tbl_codes
            df = self._execute_with_timeout(conn, sql, params)
            if df is None or df.empty:
                # 表级 freq 缺失 → FREQ_NOT_IN_TABLE（三分类语义保持：与单只版 L853-857 一致）
                freq_check = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE freq = ? AND code IN "
                    f"(SELECT unnest([{', '.join(['?'] * len(tbl_codes))}]))",
                    [storage_freq] + tbl_codes).fetchone()
                if freq_check[0] == 0:
                    from .frequency_labels import ERR_FREQ_NOT_IN_TABLE
                    raise FrequencyCapabilityError(
                        ERR_FREQ_NOT_IN_TABLE, api_freq=None, storage_freq=storage_freq,
                        table=table, detail=f"{table} 有数据但缺 freq={storage_freq}")
                continue  # 有此 freq 但窗口内无 bar（合法空）
            parts.append(df)
        if not parts:
            return pd.DataFrame()
        result = pd.concat(parts, ignore_index=True)
        # fq 替换（与 query_minute_bars_by_range(_batch) 一致口径）
        fq_norm = str(fq).lower() if fq else ""
        if fq_norm in ('pre', 'dypre'):
            for orig, qfq in [("open", "open_front"), ("high", "high_front"),
                              ("low", "low_front"), ("close", "close_front")]:
                if qfq in result.columns and result[qfq].notna().any():
                    result[orig] = result[qfq]
        elif fq_norm in ('post', 'dyback', 'dy_post'):
            for orig, qfq in [("open", "open_back"), ("high", "high_back"),
                              ("low", "low_back"), ("close", "close_back")]:
                if qfq in result.columns and result[qfq].notna().any():
                    result[orig] = result[qfq]
        return result.sort_values(["code", "time"]).reset_index(drop=True)

    @staticmethod
    def _end_ms(date: str) -> int:
        """返回 date 当日 23:59:59.999 的 epoch 毫秒（end 当天截断用）。"""
        ts = pd.Timestamp(str(date)[:10]).tz_localize("Asia/Shanghai")
        return int((ts.value // 10**6) + 86_399_999)

    def query_benchmark(self, code, start_ms, end_ms) -> pd.DataFrame:
        """迁移自 BacktestEngine._get_benchmark() (backtest_engine.py:737-754)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            SELECT time, close FROM index_daily
            WHERE code = '{code}' AND time >= {start_ms} AND time <= {end_ms}
            ORDER BY time
        """).fetchdf()

    def query_strategy_events(self, event_type, effective_date=None, start_date=None,
                              end_date=None, codes=None) -> pd.DataFrame:
        """Query generic strategy events without exposing storage to strategies."""
        columns = [
            "event_type", "event_date", "effective_date", "code", "signal",
            "name", "category", "source", "source_row_id", "source_key", "payload", "imported_at",
        ]
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=columns)
        try:
            tables = self._existing_tables()
            if "strategy_events" not in tables:
                return pd.DataFrame(columns=columns)
            where = ["event_type = ?"]
            params = [str(event_type)]
            if effective_date is not None:
                where.append("effective_date = ?::DATE")
                params.append(str(effective_date)[:10])
            if start_date is not None:
                where.append("effective_date >= ?::DATE")
                params.append(str(start_date)[:10])
            if end_date is not None:
                where.append("effective_date <= ?::DATE")
                params.append(str(end_date)[:10])
            normalized_codes = [str(code) for code in (codes or [])]
            if normalized_codes:
                placeholders = ",".join(["?"] * len(normalized_codes))
                where.append(f"code IN ({placeholders})")
                params.extend(normalized_codes)
            sql = (
                "SELECT event_type, event_date, effective_date, code, signal, "
                "name, category, source, source_row_id, source_key, payload, imported_at "
                "FROM strategy_events WHERE " + " AND ".join(where) +
                " ORDER BY effective_date, event_date, source_row_id NULLS LAST, source_key"
            )
            return conn.execute(sql, params).fetchdf()
        except Exception as exc:
            logger.warning("query_strategy_events failed: %s", exc)
            return pd.DataFrame(columns=columns)

    def query_corporate_actions(self, date_ms: int) -> pd.DataFrame:
        """Return ex-date cash/stock distributions for a trading date.

        Schema-compatible: works with both old (cash_div only) and new schemas.
        Returns: code, cash_div_before_tax, cash_div_after_tax, cash_div (legacy), stk_div.
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=["code", "cash_div_before_tax", "cash_div_after_tax",
                                         "cash_div", "stk_div"])
        tables = self._existing_tables()
        if "stock_dividend" not in tables:
            return pd.DataFrame(columns=["code", "cash_div_before_tax", "cash_div_after_tax",
                                         "cash_div", "stk_div"])
        # Schema 兼容：动态检测新列是否已迁移
        actual_cols = {row[0] for row in conn.execute("DESCRIBE stock_dividend").fetchall()}
        if "cash_div_before_tax" in actual_cols:
            return conn.execute(
                """SELECT code,
                          COALESCE(cash_div_before_tax, cash_div) AS cash_div_before_tax,
                          cash_div_after_tax,
                          cash_div, stk_div
                   FROM stock_dividend WHERE ex_date = ?""",
                [int(date_ms)],
            ).fetchdf()
        else:
            # 旧 schema 兼容：只有 legacy cash_div
            return conn.execute(
                """SELECT code,
                          cash_div AS cash_div_before_tax,
                          CAST(NULL AS DOUBLE) AS cash_div_after_tax,
                          cash_div, stk_div
                   FROM stock_dividend WHERE ex_date = ?""",
                [int(date_ms)],
            ).fetchdf()

    def query_etf_dividends(self, date_ms: int) -> pd.DataFrame:
        """Return ETF cash dividends (div_cash per share) for an ex-date.

        阶段 2（引擎方案 v2-final §3.6）：etf_dividend 表（tushare fund_div，管线方案产出）。
        表不存在/无数据 → 空 DataFrame（no-op 设计，引擎侧不抛异常、不阻塞回测）。
        Returns: code, div_cash（每股派息，元/份；div_proc='实施' 过滤）。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=["code", "div_cash"])
        tables = self._existing_tables()
        if "etf_dividend" not in tables:
            return pd.DataFrame(columns=["code", "div_cash"])
        return conn.execute(
            """SELECT code, div_cash FROM etf_dividend
               WHERE ex_date = ? AND div_proc = '实施'""",
            [int(date_ms)],
        ).fetchdf()

    def query_stock_exrights(self, code: str, date_ms: int) -> Optional[pd.DataFrame]:
        """Query stock_dividend for ex-rights information on a given date.

        Schema-compatible: works with old schema (cash_div only) and new schema.
        Returns a DataFrame with PTrade-compatible columns indexed by date,
        or None if no data found or the table is missing.
        """
        conn = self._get_conn()
        if conn is None:
            return None
        tables = self._existing_tables()
        if "stock_dividend" not in tables:
            return None
        # Schema 兼容：动态检测列
        actual_cols = {row[0] for row in conn.execute("DESCRIBE stock_dividend").fetchall()}
        bonus_expr = ("COALESCE(cash_div_before_tax, cash_div)" if "cash_div_before_tax" in actual_cols
                      else "cash_div")
        df = conn.execute(
            f"SELECT ex_date, {bonus_expr} AS bonus_raw, stk_div "
            "FROM stock_dividend WHERE code = ? AND ex_date = ?",
            [str(code), int(date_ms)],
        ).fetchdf()
        if df.empty:
            return None
        # Map to PTrade-compatible columns
        df["bonus_ps"] = df["bonus_raw"]
        df["allotted_ps"] = df["stk_div"]
        df["rationed_ps"] = None
        df["rationed_px"] = None
        df["exer_forward_a"] = None
        df["exer_forward_b"] = None
        df["exer_backward_a"] = None
        df["exer_backward_b"] = None
        # Index by date (ex_date ms -> Timestamp)
        df["date"] = pd.to_datetime(
            df["ex_date"], unit="ms", utc=True
        ).dt.tz_convert("Asia/Shanghai")
        df = df.set_index("date")
        return df[[
            "allotted_ps", "rationed_ps", "rationed_px", "bonus_ps",
            "exer_forward_a", "exer_forward_b", "exer_backward_a", "exer_backward_b",
        ]]

    def query_listing_dates(self) -> pd.DataFrame:
        """统一上市日期查询（F2 修订版）：股票行为与修复前一致，仅扩展 ETF。

        迁移自 PtradeAPI._preload_market_data() 中的上市日期查询 (ptrade_api.py:388-390)。

        上市日期口径（2026-07-27 审核修订）：
        - 股票：**保持修复前行为** —— stock_daily.MIN(time)（'stock_daily_min'），
          不用 stock_basic 覆盖，保证 get_stock_info/get_security_info 股票值级一致；
        - ETF：etf_basic.list_date（'etf_basic'）；etf_basic 上市日缺失时按现有
          etf_basic 管线契约使用首个 etf_daily 交易日补齐
          （'etf_daily_min_fallback'，不得宣称正式上市日能力）；
        - 不使用代码前缀猜测上市日或资产类型（etf_basic 元数据成员资格判定）。
        返回列：code, listing_time, listing_source（旧消费方只读 code/listing_time）。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=["code", "listing_time", "listing_source"])
        tables = self._existing_tables()
        branches = []
        etf_exclusions = []
        if "etf_basic" in tables:
            branches.append(
                "SELECT code, list_date AS listing_time, 'etf_basic' AS listing_source "
                "FROM etf_basic WHERE list_date IS NOT NULL")
            etf_exclusions.append(
                "code NOT IN (SELECT code FROM etf_basic WHERE list_date IS NOT NULL)")
        if {"etf_basic", "etf_daily"} <= tables:
            branches.append(
                "SELECT e.code, MIN(d.time) AS listing_time, "
                "'etf_daily_min_fallback' AS listing_source "
                "FROM etf_basic e JOIN etf_daily d ON d.code = e.code "
                "WHERE e.list_date IS NULL GROUP BY e.code")
            etf_exclusions.append(
                "code NOT IN (SELECT code FROM etf_basic WHERE list_date IS NULL)")
        if "stock_daily" in tables:
            # 股票（及未被 etf_basic 覆盖的代码）：修复前行为 = stock_daily 首根K线
            where = ("WHERE " + " AND ".join(etf_exclusions)) if etf_exclusions else ""
            branches.append(
                "SELECT code, MIN(time) AS listing_time, "
                f"'stock_daily_min' AS listing_source "
                f"FROM stock_daily {where} GROUP BY code")
        if not branches:
            return pd.DataFrame(columns=["code", "listing_time", "listing_source"])
        return conn.execute(" UNION ALL ".join(branches)).fetchdf()

    def _build_preload_daily_code_positions(self):
        """Build a reusable code-to-row-position index for daily history."""
        if self._preload_daily is None:
            self._preload_daily_code_positions = None
            return
        if self._preload_daily_code_positions is None:
            self._preload_daily_code_positions = {
                str(code): positions
                for code, positions in self._preload_daily.groupby(
                    "code", sort=False, observed=True).indices.items()
            }

    def _preloaded_daily_for_code(self, bare):
        if self._preload_daily is None:
            return None
        self._build_preload_daily_code_positions()
        positions = (self._preload_daily_code_positions or {}).get(str(bare))
        if positions is None:
            return self._preload_daily.iloc[0:0]
        return self._preload_daily.iloc[positions]

    def get_history_from_preload(self, sec_list, count, fq, is_dict, fields, field_map) -> Optional[Any]:
        """从预加载内存查 get_history（避免 DuckDB 查询）。

        迁移自 PtradeAPI._get_history_from_preload() (ptrade_api.py:505-549)
        返回 None 表示预加载数据不足，需 fallback 到 DuckDB。
        """
        if self._preload_daily is None:
            return None
        use_qfq = str(fq).lower() in ("pre", "dypre")
        prev_ms = self._preload_prev_ms
        dfs = {}
        for sec in sec_list:
            bare = str(sec).split(".")[0]
            # PIT 过滤：只取 <= prev_date 的数据（预加载数据已按 time DESC 排序）
            sub_all = self._preloaded_daily_for_code(bare)
            if sub_all is None:
                return None
            if prev_ms is not None:
                sub_all = sub_all[sub_all['time'] <= prev_ms]
            sub = sub_all.head(int(count))
            if len(sub) == 0:
                # 尝试 ETF/指数 fallback（与 DuckDB 路径一致的 INDEX_ETF_MAP）
                INDEX_ETF_MAP = {"000300": "510300", "000905": "510500",
                                 "000016": "510050", "000852": "510880"}
                if bare in INDEX_ETF_MAP:
                    sub = self._preloaded_daily_for_code(INDEX_ETF_MAP[bare])
                    if prev_ms is not None:
                        sub = sub[sub['time'] <= prev_ms]
                    sub = sub.head(int(count))
            if len(sub) == 0:
                continue
            df = sub.sort_values('time').copy()
            if use_qfq:
                for orig, qfq in [("open", "open_front"), ("high", "high_front"),
                                  ("low", "low_front"), ("close", "close_front")]:
                    if qfq in df.columns and df[qfq].notna().any():
                        df[orig] = df[qfq]
            df['trade_date'] = pd.to_datetime(df['time'], unit='ms').dt.strftime('%Y-%m-%d')
            # 注：此处沿用原实现，按 Ptrade 格式输出键（与 DuckDB fallback 路径一致）
            dfs[self._to_ptrade_code(bare)] = df
        if not dfs:
            return None
        if is_dict:
            return CodeDict_clone(dfs)
        if len(dfs) == 1:
            df0 = list(dfs.values())[0]
        else:
            df0 = pd.concat(dfs.values(), ignore_index=False)
        if fields:
            mapped = [field_map.get(f, f) for f in fields]
            available = [m for m in mapped if m in df0.columns]
            if available:
                df0 = df0[available]
        return df0

    def get_fundamentals_from_preload(self, bare_codes, fields) -> Optional[pd.DataFrame]:
        """从预加载的估值 DataFrame 查 get_fundamentals(valuation)。

        迁移自 PtradeAPI._get_fundamentals_from_preload() (ptrade_api.py:551-568)
        返回 None 表示无预加载数据，需 fallback 到 DuckDB。
        """
        if self._preload_float is None:
            return None
        codes_in = bare_codes if isinstance(bare_codes, list) else [bare_codes]
        sub = self._preload_float[self._preload_float['code'].isin([str(c).split('.')[0] for c in codes_in])]
        if len(sub) == 0:
            return None
        df = sub.copy()
        # 注：此处沿用原实现，按 Ptrade 格式输出（code→index）
        df['code'] = df['code'].apply(self._to_ptrade_code)
        df = df.set_index('code')
        if fields:
            field_list = [fields] if isinstance(fields, str) else list(fields)
            available = [f for f in field_list if f in df.columns]
            if available:
                df = df[available]
        return df

    # ===================== 估值/基本面查询 =====================

    def query_float_share_for_preload(self, query_ms: int) -> pd.DataFrame:
        """迁移自 PtradeAPI._refresh_fundamentals_pit() 的 float_share 更新逻辑 (ptrade_api.py:430-437)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            SELECT code, total_share, free_share
            FROM stock_float_share
            WHERE ann_date <= {query_ms}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY ann_date DESC) = 1
        """).fetchdf()

    def query_valuation_for_preload(self, query_ms: int) -> pd.DataFrame:
        """迁移自 PtradeAPI._refresh_fundamentals_pit() 的每日估值查询 (ptrade_api.py:446-470)

        含 stock_daily_valuation + stock_daily(close) + stock_float_share(free_share) 三层 JOIN

        B1（2026-09-24 纯性能优化，语义等价）：三处 `QUALIFY ROW_NUMBER() OVER
        (PARTITION BY code ORDER BY <ts> DESC) = 1` 改为 `max(<ts>) GROUP BY code`
        哈希聚合 + 等值连接，消除三次全表排序（965 万/971 万/74 万行）。
        等价性依据（B0 前置双证）：①三表 `(code, <ts>)` 均唯一（实测 distinct == count，
        无 tie ⇒ ROW_NUMBER 与 max+等值连接结果集逐行相同）；②5 个交易日旧/新对照：
        行数、列序、dtype、排序后逐值全等；③唯一消费方
        `get_fundamentals_from_preload` 全程按 code 做 isin 掩码/逐元素 apply/set_index，
        无任何位置依赖 ⇒ 行序差异不影响可观察结果。实测单次 5.58-10.69s → 0.19-0.42s（约 24x）。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            WITH lv AS (
                SELECT code, max(time) AS t FROM stock_daily_valuation
                WHERE time <= {query_ms} GROUP BY code
            ), ld AS (
                SELECT code, max(time) AS t FROM stock_daily
                WHERE time <= {query_ms} GROUP BY code
            ), lf AS (
                SELECT code, max(end_date) AS a FROM stock_float_share
                WHERE end_date <= {query_ms} GROUP BY code
            )
            SELECT v.code, v.circ_mv AS float_value, v.total_mv AS total_value,
                   v.pe_ttm AS pe_ratio, v.pe_ttm, v.pb AS pb_ratio, v.turnover_rate AS turnover_ratio,
                   COALESCE(fs.free_share,
                            CASE WHEN d.close IS NULL OR d.close = 0 THEN NULL
                                 ELSE v.circ_mv / d.close END) AS a_floats
            FROM stock_daily_valuation v
            JOIN lv ON v.code = lv.code AND v.time = lv.t
            LEFT JOIN (
                SELECT s.code, s.close FROM stock_daily s
                JOIN ld ON s.code = ld.code AND s.time = ld.t
            ) d ON d.code = v.code
            LEFT JOIN (
                SELECT f.code, f.free_share FROM stock_float_share f
                JOIN lf ON f.code = lf.code AND f.end_date = lf.a
            ) fs ON fs.code = v.code
        """).fetchdf()

    def query_valuation_daily_pit(self, bare_codes, query_ms) -> pd.DataFrame:
        """迁移自 PtradeAPI._fundamentals_valuation() 主路径 (ptrade_api.py:757-784)

        stock_daily_valuation PIT + stock_daily close 推导 a_floats + stock_float_share free_share
        返回「源形状」裸码 DataFrame（不含 total_share/pe_ratio_lyr，由调用方 finalize）。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        codes_in = "','".join(bare_codes)
        return conn.execute(f"""
            SELECT s.code,
                   s.circ_mv          AS float_value,
                   s.circ_mv          AS circulating_market_cap,
                   s.total_mv         AS total_value,
                   s.total_mv         AS market_cap,
                   s.pe_ttm           AS pe_ratio,
                   s.pe_ttm           AS pe_ttm,
                   s.pb               AS pb_ratio,
                   s.turnover_rate    AS turnover_ratio,
                   COALESCE(fs.free_share,
                            CASE WHEN dc.close IS NULL OR dc.close = 0 THEN NULL
                                 ELSE s.circ_mv / dc.close END) AS a_floats
            FROM stock_daily_valuation s
            LEFT JOIN (
                SELECT code, close FROM stock_daily
                WHERE time <= {query_ms}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) = 1
            ) dc ON dc.code = s.code
            LEFT JOIN (
                SELECT code, free_share FROM stock_float_share
                WHERE end_date <= {query_ms}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY end_date DESC) = 1
            ) fs ON fs.code = s.code
            WHERE s.code IN ('{codes_in}') AND s.time <= {query_ms}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.code ORDER BY s.time DESC) = 1
        """).fetchdf()

    def query_valuation_monthly_fallback(self, bare_codes, query_ms) -> pd.DataFrame:
        """迁移自 PtradeAPI._fundamentals_valuation() 回退路径 (ptrade_api.py:786-808)

        stock_float_share + stock_daily fallback。

        修复（2026-07-29）：stock_float_share 表**无 time 列**（真实列：code/end_date/
        ann_date/free_share/total_share/circ_mv/total_mv/update_time/data_source），原 SQL
        误用 s.time 与 MAX(time) FROM stock_float_share，使 DuckDB 1.5.5 binder 把对不存在
        列的聚合解析走偏，对外报成误导性的 'WHERE clause cannot contain aggregates'。
        改用正确列名 end_date + CTE + QUALIFY 窗口函数（与 query_valuation_daily_pit :952
        同款写法，DuckDB 1.5.5 兼容）。PIT 语义不变：取 end_date<=query_ms 的最新一期
        stock_float_share（同报告期多次公告取最新 ann_date）+ time<=query_ms 的最新一日
        stock_daily，LEFT JOIN 到每只 code。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        codes_in = "','".join(bare_codes)
        return conn.execute(f"""
            WITH latest_share AS (
                SELECT code, circ_mv, total_mv, end_date, ann_date
                FROM stock_float_share
                WHERE code IN ('{codes_in}') AND end_date <= {query_ms}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY code ORDER BY end_date DESC, ann_date DESC) = 1
            ),
            latest_daily AS (
                SELECT code, peTTM, pbMRQ, psTTM, pcfNcfTTM, turn, close
                FROM stock_daily
                WHERE code IN ('{codes_in}') AND time <= {query_ms}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) = 1
            )
            SELECT s.code,
                   s.circ_mv        AS float_value,
                   s.circ_mv        AS circulating_market_cap,
                   s.total_mv       AS total_value,
                   s.total_mv       AS market_cap,
                   d.peTTM          AS pe_ratio,
                   d.peTTM          AS pe_ttm,
                   d.pbMRQ          AS pb_ratio,
                   d.psTTM          AS ps_ratio,
                   d.pcfNcfTTM      AS pcf_ratio,
                   d.turn           AS turnover_ratio,
                   CASE WHEN d.close IS NULL OR d.close = 0 THEN NULL
                        ELSE s.circ_mv / d.close END AS a_floats
            FROM latest_share s
            LEFT JOIN latest_daily d ON d.code = s.code
        """).fetchdf()

    def query_total_share(self, bare_codes, query_ms) -> pd.DataFrame:
        """迁移自 PtradeAPI._fundamentals_valuation() 的 total_share 补查 (ptrade_api.py:812-818)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        codes_in = "','".join(bare_codes)
        return conn.execute(f"""
            SELECT code, total_share
            FROM stock_float_share
            WHERE code IN ('{codes_in}') AND ann_date <= {query_ms}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY code ORDER BY ann_date DESC, end_date DESC) = 1
        """).fetchdf()

    def query_valuation_orm(self, query_ms: int) -> pd.DataFrame:
        """valuation ORM：逐证券 PIT；market_cap 口径为亿元。

        float_value/total_mv 继续保持元，兼容既有小市值策略。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            WITH valuation_pit AS (
                SELECT * FROM stock_daily_valuation
                WHERE time <= {query_ms}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) = 1
            ), share_pit AS (
                SELECT code, free_share, total_share
                FROM stock_float_share
                WHERE ann_date <= {query_ms}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY code ORDER BY ann_date DESC, end_date DESC) = 1
            ), daily_pit AS (
                SELECT code, close, psTTM
                FROM stock_daily
                WHERE time <= {query_ms}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) = 1
            )
            SELECT v.code,
                   v.total_mv / 1e8   AS market_cap,
                   v.circ_mv / 1e8    AS circulating_market_cap,
                   v.circ_mv          AS circ_mv,
                   v.circ_mv          AS float_value,
                   COALESCE(v.free_share, fs.free_share,
                            CASE WHEN d.close IS NULL OR d.close = 0 THEN NULL
                                 ELSE v.circ_mv / d.close END) AS a_floats,
                   v.total_mv          AS total_mv,
                   fs.total_share      AS total_share,
                   v.pe_ttm            AS pe_ratio,
                   v.pe_ttm            AS pe_ttm,
                   v.pb                AS pb_ratio,
                   d.psTTM             AS ps_ratio,
                   v.turnover_rate     AS turnover_ratio
            FROM valuation_pit v
            LEFT JOIN share_pit fs ON fs.code = v.code
            LEFT JOIN daily_pit d ON d.code = v.code
        """).fetchdf()
    def query_fin_indicator(self, codes, ann_date_ms, start_year, end_year, report_types) -> pd.DataFrame:
        """迁移自 PtradeAPI._fundamentals_fin_indicator() (ptrade_api.py:827-861)

        Schema 兼容：正式库可能缺少 or_yoy 列（W2 迁移前），动态检测并安全回退。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        codes_in = "','".join(codes)
        where_date = f"f.ann_date <= {ann_date_ms}"
        where_year = ""
        if start_year and end_year:
            sy_ms = int(pd.Timestamp(f"{start_year}-01-01", tz='Asia/Shanghai').timestamp() * 1000)
            ey_ms = int(pd.Timestamp(f"{end_year}-12-31", tz='Asia/Shanghai').timestamp() * 1000)
            where_year = f"AND f.ann_date BETWEEN {sy_ms} AND {ey_ms}"

        rt_cond = ""
        if report_types:
            rt_map = {'1': (3, 3), '2': (6, 6), '3': (9, 9), '4': (12, 12)}
            if report_types in rt_map:
                m_end = rt_map[report_types][1]
                rt_cond = f"AND (CAST(strftime('%m', make_timestamp(f.end_date*1000)) AS INTEGER) = {m_end})"

        # Schema 兼容：动态检测新列是否已迁移
        actual_cols = {row[0] for row in conn.execute("DESCRIBE fin_indicator").fetchall()}
        or_yoy_expr = "f.or_yoy" if "or_yoy" in actual_cols else "CAST(NULL AS DOUBLE) AS or_yoy"

        return conn.execute(f"""
            SELECT f.code,
                   f.end_date,
                   f.ann_date  AS publ_date,
                   f.eps, f.bps, f.roe,
                   f.pe_ttm, f.pb, f.ps_ttm, f.np_yoy, {or_yoy_expr}
            FROM fin_indicator f
            WHERE f.code IN ('{codes_in}')
              AND {where_date} {where_year} {rt_cond}
            ORDER BY f.code, f.end_date
        """).fetchdf()

    def query_statement_table(self, table, codes, ann_date_ms, fields,
                              start_year=None, end_year=None, report_types=None) -> pd.DataFrame:
        """三大报表 PIT 窗口查询（D4 修复 fundamentals-statement-wiring-design.md v2）。

        - table: income_statement / balance_statement / cashflow_statement
        - 返回 **PIT 窗口内全部报告期行**（ann_date <= ann_date_ms 且报告期在 [start_year, end_year]），
          支撑 F-Score 同比两期取数（策略层自取本期/去年同期）。
        - 列名契约归一：ann_date → publ_date（数值毫秒时间戳，与 fin_indicator 同构）。
        - report_types 默认合并报表（月=12 年报/季报按端日过滤；'合并'/'single' 仅日志提示不阻断）。
        - 缺列：请求字段不在表中 → log.warning（表名+缺失清单），返回缺列（NaN）。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        actual_cols = {row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()}
        codes_in = "','".join(codes)
        want = list(dict.fromkeys((fields or []) + ["code", "end_date"]))
        select_cols, missing = [], []
        for f in want:
            if f == "code":
                select_cols.append("code")
            elif f == "publ_date":
                select_cols.append("ann_date AS publ_date")
            elif f in actual_cols:
                select_cols.append(f)
            else:
                missing.append(f)
        # end_date 恒返回（策略层定位报告期）；缺的请求列以 NULL 占位（fail-open + 告警）
        for f in set(want) - {"code", "publ_date"} - set(actual_cols):
            select_cols.append(f"CAST(NULL AS DOUBLE) AS {f}")
        if "end_date" not in select_cols:
            select_cols.append("end_date")
        where_date = f"ann_date <= {ann_date_ms}"
        where_year = ""
        if start_year and end_year:
            sy_ms = int(pd.Timestamp(f"{start_year}-01-01", tz="Asia/Shanghai").timestamp() * 1000)
            ey_ms = int(pd.Timestamp(f"{end_year}-12-31", tz="Asia/Shanghai").timestamp() * 1000)
            where_year = f"AND end_date BETWEEN {sy_ms} AND {ey_ms}"
        rt_cond = ""
        if report_types:
            rt_map = {'1': 3, '2': 6, '3': 9, '4': 12}
            if str(report_types) in rt_map:
                m_end = rt_map[str(report_types)]
                rt_cond = (f"AND (CAST(strftime('%m', make_timestamp(end_date*1000)) "
                           f"AS INTEGER) = {m_end})")
        df = conn.execute(f"""
            SELECT {','.join(select_cols)}
            FROM {table}
            WHERE code IN ('{codes_in}')
              AND {where_date} {where_year} {rt_cond}
            ORDER BY code, end_date
        """).fetchdf()
        if missing:
            try:
                import logging
                logging.getLogger(__name__).warning(
                    "query_statement_table %s 缺列: %s（已置 NULL）", table, missing)
            except Exception:
                pass
        return df

    # ===================== 统一证券元数据（F2） =====================

    #: query_security_metadata 统一内部字段（列顺序固定，属数据契约）
    SECURITY_METADATA_COLUMNS = [
        "code", "name", "security_type", "exchange",
        "list_date", "delist_date", "status", "data_source",
    ]

    def _existing_tables(self) -> set:
        """返回当前数据库存在的表名集合（小写）。

        统一表存在性探测入口；所有 provider 方法经此查询，避免重复 SHOW TABLES。
        回测期间数据库只读、表集合恒定，故首次查询后缓存，后续返回防御性 set 副本；
        close() 将缓存置 None。
        """
        if self._tables_cache is None:
            conn = self._get_conn()
            if conn is None:
                return set()
            self._tables_cache = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        return set(self._tables_cache)

    def query_security_metadata(self, codes=None) -> pd.DataFrame:
        """统一股票/ETF 证券元数据查询（F2）。

        数据路由：stock_basic（security_type='stock'）∪ etf_basic（'etf'），
        输出固定列 ``SECURITY_METADATA_COLUMNS``（见类常量），日期列为毫秒时间戳。
        不为任何单只证券写代码分支；不使用代码前缀猜测资产类型。
        旧库缺某张表时只路由到存在的表；两张都缺返回带固定列的空 DataFrame。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=self.SECURITY_METADATA_COLUMNS)
        tables = self._existing_tables()
        branches = []
        params: list = []
        if "stock_basic" in tables:
            branches.append(
                "SELECT code, name, 'stock' AS security_type, exchange, "
                "list_date, delist_date, list_status AS status, data_source "
                "FROM stock_basic")
        if "etf_basic" in tables:
            branches.append(
                "SELECT code, name, 'etf' AS security_type, exchange, "
                "list_date, delist_date, status, data_source "
                "FROM etf_basic")
        if not branches:
            return pd.DataFrame(columns=self.SECURITY_METADATA_COLUMNS)
        sql = " UNION ALL ".join(branches)
        if codes:
            placeholders = ", ".join("?" for _ in codes)
            sql = (f"SELECT * FROM ({sql}) WHERE code IN ({placeholders}) "
                   "ORDER BY code")
            params = [str(c) for c in codes]
        else:
            sql = f"SELECT * FROM ({sql}) ORDER BY code"
        return conn.execute(sql, params).fetchdf()

    # ===================== 参考数据查询 =====================

    def query_index_constituents(self, index_code, as_of_ms=None) -> pd.DataFrame:
        """指数成分 PIT 查询（F3 修订：完整性来自 snapshot_meta 批次契约）。

        语义（§5.2 + 2026-07-27 审核修订）：
        - 只取不晚于 as_of_ms 的最近 status='complete' 快照；无则返回空，
          绝不向未来 fallback；
        - 完整性**只**由 index_constituents_snapshot_meta 在打点写入时判定
          （expected_count/status 契约），不依赖未来快照；无 meta 的指数
          fail-closed 返回空（不能证明完整）；
        - ``as_of_ms=None``：最新 complete 快照（Provider 直接调用的文档化兼容）；
        - 返回去重、按 code 排序，列：code, weight, time。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=["code", "weight", "time"])
        tables = self._existing_tables()
        if not {"index_constituents", "index_constituents_snapshot_meta"} <= tables:
            return pd.DataFrame(columns=["code", "weight", "time"])
        params: list = [str(index_code)]
        where = "index_code = ? AND status = 'complete'"
        if as_of_ms is not None:
            where += " AND time <= ?"
            params.append(int(as_of_ms))
        row = conn.execute(
            "SELECT MAX(time) FROM index_constituents_snapshot_meta "
            f"WHERE {where}", params).fetchone()
        if row is None or row[0] is None:
            return pd.DataFrame(columns=["code", "weight", "time"])
        snapshot_time = int(row[0])
        df = conn.execute(
            "SELECT code, weight, time FROM index_constituents "
            "WHERE index_code = ? AND time = ? ORDER BY code",
            [str(index_code), snapshot_time]).fetchdf()
        # 同日多源/重复行冲突：去重保序（质量门控由 query_index_constituents_quality 标记）
        return df.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)

    def query_index_constituents_quality(self, index_code=None) -> pd.DataFrame:
        """指数成分快照质量报告（F3 修订：以 snapshot_meta 批次契约为准）。

        每行一个 (index_code, time) 快照：n_constituents、expected_count、
        status（complete/partial，写入时判定，不依赖未来数据）、
        n_duplicate_codes、n_negative_weights、weight_sum（源端原值汇总，
        不因单位差异静默修改）。meta 缺失返回带固定列的空 DataFrame。
        """
        cols = ["index_code", "time", "n_constituents", "expected_count",
                "status", "n_duplicate_codes", "n_negative_weights",
                "n_blank_codes", "weight_sum"]
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=cols)
        tables = self._existing_tables()
        if "index_constituents_snapshot_meta" not in tables:
            return pd.DataFrame(columns=cols)
        params: list = []
        where = ""
        if index_code is not None:
            where = "WHERE m.index_code = ?"
            params = [str(index_code)]
        if "index_constituents" in tables:
            return conn.execute(f"""
                SELECT m.index_code, m.time, m.n_constituents, m.expected_count,
                       m.status,
                       COALESCE(m.n_duplicate_codes, 0) AS n_duplicate_codes,
                       COALESCE(m.n_negative_weights, 0) AS n_negative_weights,
                       COALESCE(m.n_blank_codes, 0) AS n_blank_codes,
                       s.weight_sum
                FROM index_constituents_snapshot_meta m
                LEFT JOIN (
                    SELECT index_code, time, SUM(weight) AS weight_sum
                    FROM index_constituents GROUP BY index_code, time
                ) s ON s.index_code = m.index_code AND s.time = m.time
                {where}
                ORDER BY m.index_code, m.time
            """, params).fetchdf()
        return conn.execute(f"""
            SELECT m.index_code, m.time, m.n_constituents, m.expected_count,
                   m.status, 0 AS n_duplicate_codes, 0 AS n_negative_weights,
                   0 AS n_blank_codes, NULL AS weight_sum
            FROM index_constituents_snapshot_meta m {where}
            ORDER BY m.index_code, m.time
        """, params).fetchdf()

    def query_all_stocks(self, before_ms) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_Ashares() (ptrade_api.py:1566-1570)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            SELECT DISTINCT code FROM stock_daily WHERE time = (
                SELECT MAX(time) FROM stock_daily WHERE time <= {before_ms}
            )
        """).fetchdf()

    def query_sw_industry(self, code):
        """迁移自 PtradeAPI.get_industry() (ptrade_api.py:2024-2026)

        【LEGACY 快照，仅审计用途】sw_industry 不是正式申万一级 PIT 分类
        （历史由 index_classify 名称匹配生成，含伪 SW_行业名 代码风险）。
        正式能力已迁移到 industry_classification + industry_membership（F4），
        Provider.get_industry 不再读取本方法。

        返回 (industry_code, industry_name) 或 None
        """
        conn = self._get_conn()
        if conn is None:
            return None
        return conn.execute(
            f"SELECT industry_code, industry_name FROM sw_industry WHERE code='{code}' LIMIT 1"
        ).fetchone()

    def query_industry_membership_quality(self, table: str = "industry_membership",
                                          classification_table: str = "industry_classification") -> dict:
        """industry_membership 区间质量门控（F4b）。

        返回 dict（表缺失时 present=False）：
        - positive_overlaps / boundary_touches / multi_current_codes /
          orphan_rows / bad_ranges / total_rows / codes 同前；
        - classification_present / quality_complete / ok / reason 为纠正新增：
          classification 表缺失必须 fail-closed（绝不能把"分类表不存在"
          误表示为 orphan_rows=0）。

        classification_table 必须来自标识符白名单，防止注入。
        """
        # F4 纠正：table 与 classification_table 均经标识符白名单校验，
        # 在拼入任何 SQL 之前拒绝注入式非法名。
        table = _require_valid_identifier(
            table, "query_industry_membership_quality.table")
        classification_table = _require_valid_identifier(
            classification_table, "query_industry_membership_quality.classification_table")
        conn = self._get_conn()
        if conn is None:
            return {"present": False}
        tables = self._existing_tables()
        if table not in tables:
            return {"present": False}
        classification_present = classification_table in tables
        if not classification_present:
            # 分类表缺失：绝不能把 orphan 误判为 0；quality 不完整，ok=False。
            total, codes = conn.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT code) "
                f"FROM {table} WHERE classification_system='SW' "
                f"AND industry_level='L1'").fetchone()
            return {
                "present": True,
                "classification_present": False,
                "quality_complete": False,
                "ok": False,
                "positive_overlaps": 0,
                "boundary_touches": 0,
                "multi_current_codes": 0,
                "orphan_rows": None,
                "bad_ranges": 0,
                "total_rows": int(total or 0),
                "codes": int(codes or 0),
                "reason": "classification_table_missing",
            }
        base = (f"FROM {table} WHERE classification_system='SW' "
                "AND industry_level='L1'")
        total, codes = conn.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT code) {base}").fetchone()
        pos, boundary = conn.execute(f"""
            WITH m AS (
              SELECT rowid, classification_system, classification_version,
                     industry_level, code, effective_from f,
                     COALESCE(effective_to, 9223372036854775807) t
              FROM {table}
              WHERE classification_system='SW' AND industry_level='L1'
            )
            SELECT
              SUM(CASE WHEN LEAST(a.t, b.t) - GREATEST(a.f, b.f) > 0
                       THEN 1 ELSE 0 END),
              SUM(CASE WHEN LEAST(a.t, b.t) - GREATEST(a.f, b.f) = 0
                       THEN 1 ELSE 0 END)
            FROM m a JOIN m b
              ON a.classification_system = b.classification_system
             AND a.classification_version = b.classification_version
             AND a.industry_level = b.industry_level
             AND a.code = b.code AND a.rowid < b.rowid
             AND GREATEST(a.f, b.f) <= LEAST(a.t, b.t)
        """).fetchone()
        multi_current = conn.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT code {base} AND effective_to IS NULL
              GROUP BY code HAVING COUNT(*) > 1)
        """).fetchone()[0]
        orphan = 0
        if classification_present:
            orphan = conn.execute(f"""
                SELECT COUNT(*) FROM {table} m
                WHERE m.classification_system='SW' AND m.industry_level='L1'
                  AND NOT EXISTS (
                    SELECT 1 FROM {classification_table} c
                    WHERE c.classification_system = m.classification_system
                      AND c.classification_version = m.classification_version
                      AND c.industry_level = m.industry_level
                      AND c.industry_code = m.industry_code)
            """).fetchone()[0]
        bad = conn.execute(
            f"SELECT COUNT(*) {base} AND effective_to IS NOT NULL "
            "AND effective_from > effective_to").fetchone()[0]
        ok = bool(total > 0 and int(multi_current or 0) == 0
                  and int(orphan or 0) == 0 and int(bad or 0) == 0)
        return {"present": True,
                "classification_present": True,
                "quality_complete": True,
                "ok": ok,
                "positive_overlaps": int(pos or 0),
                "boundary_touches": int(boundary or 0),
                "multi_current_codes": int(multi_current or 0),
                "orphan_rows": int(orphan or 0),
                "bad_ranges": int(bad or 0),
                "total_rows": int(total or 0),
                "codes": int(codes or 0),
                "reason": None if ok else "quality_incomplete"}

    def query_sw_index_daily_coverage(self) -> pd.DataFrame:
        """申万行业指数日线覆盖报告（F5 §7.6 数据质量门控 / F6 R1 能力核查）。

        以 industry_classification（SW / SW2021 / L1）为全集基准，LEFT JOIN
        index_daily 实际覆盖：返回 industry_code, industry_name, has_daily,
        min_time, max_time, n_rows。正式表缺失返回带固定列的空 DataFrame
        （能力判定的调用方应将其视为 DATA_BLOCKED）。
        """
        cols = ["industry_code", "industry_name", "has_daily",
                "min_time", "max_time", "n_rows"]
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=cols)
        tables = self._existing_tables()
        if "industry_classification" not in tables:
            return pd.DataFrame(columns=cols)
        if "index_daily" not in tables:
            return conn.execute("""
                SELECT industry_code, industry_name,
                       FALSE AS has_daily, NULL AS min_time, NULL AS max_time, 0 AS n_rows
                FROM industry_classification
                WHERE classification_system='SW' AND classification_version='SW2021'
                  AND industry_level='L1' ORDER BY industry_code
            """).fetchdf()
        return conn.execute("""
            SELECT c.industry_code, c.industry_name,
                   (d.n_rows IS NOT NULL AND d.n_rows > 0) AS has_daily,
                   d.min_time, d.max_time, COALESCE(d.n_rows, 0) AS n_rows
            FROM industry_classification c
            LEFT JOIN (
                SELECT code, MIN(time) AS min_time, MAX(time) AS max_time,
                       COUNT(*) AS n_rows
                FROM index_daily GROUP BY code
            ) d ON d.code = c.industry_code
            WHERE c.classification_system='SW' AND c.classification_version='SW2021'
              AND c.industry_level='L1'
            ORDER BY c.industry_code
        """).fetchdf()

    def query_industry_membership(self, code, as_of_ms=None):
        """正式行业归属 PIT 查询（F4 + 2026-07-27 审核语义边界）。

        语义（§6.6 + 审核修订）：
        - as_of_ms 传入：返回当日**唯一**有效归属（effective_from <= as_of AND
          (effective_to IS NULL OR effective_to >= as_of)）；
        - **歧义日期 fail-closed**：当日有多于一个**不同行业**的有效归属时
          （源端区间重叠属原始事实，官方 index_member 契约无冲突裁决规则，
          项目不得自定义裁决），抛 ReferenceDataCapabilityError，
          绝不用 ORDER BY 任意选一条；
        - as_of_ms=None：返回当前有效归属（同样要求唯一，否则 fail-closed）；
        - 无有效历史归属返回 None，绝不使用最新行业填充过去日期；
        - 正式表缺失抛 ReferenceDataCapabilityError（fail-closed），
          绝不回退到 legacy sw_industry 快照。

        返回 dict：industry_code/industry_name/classification_system/
        classification_version/effective_from/effective_to，或 None。
        """
        from .base import ReferenceDataCapabilityError
        conn = self._get_conn()
        if conn is None:
            raise ReferenceDataCapabilityError(
                "get_industry requires industry_membership/industry_classification; "
                "database connection unavailable")
        tables = self._existing_tables()
        missing = sorted({"industry_membership", "industry_classification"} - tables)
        if missing:
            raise ReferenceDataCapabilityError(
                "get_industry requires formal SW PIT tables "
                f"(missing: {missing}). Run the industry_classification / "
                "industry_membership pipeline; legacy sw_industry is audit-only "
                "and is never served as formal SW2021 L1 data.")
        params: list = [str(code)]
        if as_of_ms is not None:
            where = ("m.effective_from <= ? "
                     "AND (m.effective_to IS NULL OR m.effective_to >= ?)")
            params += [int(as_of_ms), int(as_of_ms)]
        else:
            where = "m.effective_to IS NULL"
        rows = conn.execute(f"""
            SELECT m.industry_code, c.industry_name,
                   m.classification_system, m.classification_version,
                   m.effective_from, m.effective_to
            FROM industry_membership m
            LEFT JOIN industry_classification c
              ON c.classification_system = m.classification_system
             AND c.classification_version = m.classification_version
             AND c.industry_code = m.industry_code
             AND c.industry_level = m.industry_level
            WHERE m.code = ?
              AND m.classification_system = 'SW'
              AND m.industry_level = 'L1'
              AND {where}
            ORDER BY m.effective_from DESC
        """, params).fetchall()
        if not rows:
            return None
        distinct = {r[0] for r in rows}
        if len(distinct) > 1:
            raise ReferenceDataCapabilityError(
                f"ambiguous industry membership for {code} at as_of={as_of_ms}: "
                f"{len(distinct)} distinct industries {sorted(distinct)} — "
                "source intervals overlap and the official index_member contract "
                "has no conflict-resolution rule; fail-closed instead of "
                "arbitral selection")
        r = rows[0]
        return {"industry_code": r[0], "industry_name": r[1],
                "classification_system": r[2], "classification_version": r[3],
                "effective_from": r[4], "effective_to": r[5]}

    def query_etf_list_active(self) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_etf_list() (ptrade_api.py:1621-1628)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute("""
            SELECT DISTINCT code FROM stock_daily
            WHERE (code LIKE '510%' OR code LIKE '511%' OR code LIKE '512%'
                   OR code LIKE '513%' OR code LIKE '515%' OR code LIKE '516%'
                   OR code LIKE '518%' OR code LIKE '588%' OR code LIKE '159%')
              AND time = (SELECT MAX(time) FROM stock_daily)
              AND volume > 0
        """).fetchdf()

    def query_etf_universe_pit(self, query_date_start_ms: int, query_date_end_ms: int,
                               etf_type: str = "equity", active_only: bool = True) -> pd.DataFrame:
        """Return the point-in-time local ETF universe from ``etf_basic``.

        The metadata table is authoritative for ETF classification and listing/delisting
        dates. ``etf_daily`` is consulted only to prove that at least one historical bar
        existed on or before the requested date. Strategy indicators and liquidity rules
        deliberately remain outside this data-access contract.
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame(columns=["code"])
        available_tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing_tables = sorted({"etf_basic", "etf_daily"} - available_tables)
        if missing_tables:
            raise ReferenceDataCapabilityError(
                "get_etf_list_local requires etf_basic metadata and etf_daily history; "
                f"missing table(s): {missing_tables}. Run scripts/sync_etf_basic.py "
                "after the ETF market-data table is available"
            )

        normalized_type = str(etf_type or "equity").strip().lower()
        allowed_types = {"all", "equity", "bond", "money", "commodity", "gold", "qdii"}
        if normalized_type not in allowed_types:
            raise ValueError(
                f"unsupported etf_type={etf_type!r}; expected one of {sorted(allowed_types)}"
            )

        predicates = [
            "e.list_date IS NOT NULL",
            "e.list_date <= ?",
            "EXISTS (SELECT 1 FROM etf_daily d WHERE d.code = e.code AND d.time <= ?)",
        ]
        params: list[Any] = [int(query_date_start_ms), int(query_date_end_ms)]
        if active_only:
            predicates.append("(e.delist_date IS NULL OR e.delist_date > ?)")
            params.append(int(query_date_start_ms))
        if normalized_type == "equity":
            predicates.extend(["e.etf_type = 'equity'", "COALESCE(e.is_cross_border, FALSE) = FALSE"])
        elif normalized_type != "all":
            predicates.append("e.etf_type = ?")
            params.append(normalized_type)

        sql = f"""
            SELECT e.code
            FROM etf_basic e
            WHERE {' AND '.join(predicates)}
            ORDER BY e.code
        """
        return conn.execute(sql, params).fetchdf()

    def query_etf_fund_types(self) -> dict:
        """per-code ETF T+0 分类装载（etf_basic.fund_type，仅上市状态）。

        引擎 per-code T+0（PR4 决策4 扩展）的唯一数据通道，与 get_etf_list_local
        同一数据源。返回 {code: fund_type}（fund_type 归一化为小写；缺失为空串）。
        连接不可用返回 {}；表缺失/查询异常向上抛出，由调用方 fail-closed（全 T+1 + warning）。
        """
        conn = self._get_conn()
        if conn is None:
            return {}
        rows = conn.execute(
            "SELECT code, fund_type FROM etf_basic WHERE status = 'L'"
        ).fetchall()
        out: dict = {}
        for code, ftype in rows:
            out[str(code)] = str(ftype).strip().lower() if ftype is not None else ""
        return out

    def query_cb_list_active(self) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_cb_list() (ptrade_api.py:1689-1695)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute("""
            SELECT DISTINCT code FROM stock_daily
            WHERE (code LIKE '110%' OR code LIKE '113%' OR code LIKE '118%'
                   OR code LIKE '123%' OR code LIKE '127%' OR code LIKE '128%')
              AND time = (SELECT MAX(time) FROM stock_daily)
              AND volume > 0
        """).fetchdf()

    def query_market_detail(self, table, condition) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_market_detail() (ptrade_api.py:1748-1751)"""
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            SELECT DISTINCT code FROM {table}
            WHERE ({condition}) AND time = (SELECT MAX(time) FROM {table})
        """).fetchdf()

    def query_daily_for_status(self, date_ms) -> pd.DataFrame:
        """简化版日线查询，专供 filter_stock_by_status / check_limit / get_stock_status 使用。

        迁移自 BacktestEngine._get_daily_data() 的简化列集。
        注：Phase 1 中这些 API 实际走内存（self._prev_day_data / self._current_day_data），
        本方法预留给 Phase 2 Provider 接口，当前不被直接调用。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        return conn.execute(f"""
            SELECT code, close, preClose, volume, suspendFlag,
                   is_st_reliable, is_st_reliable_source,
                   is_delisting_risk, is_delisting_risk_source
            FROM stock_daily WHERE time = {date_ms}
        """).fetchdf()

    def query_security_info_from_preload(self, code):
        """从 self._preload_listing 查上市日期（毫秒时间戳）或 None。

        迁移自 PtradeAPI.get_security_info() 的预加载分支 (ptrade_api.py:1989-1993)
        """
        if self._preload_listing is None:
            return None
        # 阶段 2.5：惰性构建 {code: (listing_time, listing_source)} 字典（首行出现，等价
        # 原 iloc[0]），替代逐只 _preload_listing[code==x] 的 O(N) 布尔扫描（get_security_info
        # 全市场万次调用→新 N+1）。listing_source 一并缓存，供 get_security_info 的 ETF
        # 分支复用，消除其第二处逐只扫描。字典构建仅一次。
        if self._preload_listing_by_code is None:
            has_ls = 'listing_source' in self._preload_listing.columns
            self._preload_listing_by_code = {}
            for _, r in self._preload_listing.iterrows():
                c = r['code']
                if c not in self._preload_listing_by_code:
                    self._preload_listing_by_code[c] = (
                        r['listing_time'],
                        r['listing_source'] if has_ls else None,
                    )
        entry = self._preload_listing_by_code.get(code)
        # 等价原 `if len(row) > 0 and row.iloc[0]['listing_time']` 的标量真值判定
        # （含 NaN 透传、0 视为 None），下游 get_security_info 对结果再判 `if listing_ms`。
        return entry[0] if entry and entry[0] else None

    # ===================== 交易日历查询 =====================

    def query_trade_days_range(self, start_ms, end_ms) -> pd.DataFrame:
        """迁移自 BacktestEngine._get_trade_days() (backtest_engine.py:712-720)

        start_ms / end_ms 任一为 None 时对应条件不生效（等价于 1=1）。
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        conditions = []
        if start_ms is not None:
            conditions.append(f"time >= {start_ms}")
        if end_ms is not None:
            conditions.append(f"time <= {end_ms}")
        where = " AND ".join(conditions) if conditions else "1=1"
        return conn.execute(f"SELECT DISTINCT time FROM stock_daily WHERE {where} ORDER BY time").fetchdf()

    def diagnose_stock_daily_range(self) -> dict:
        """诊断 stock_daily 表的数据覆盖范围与连接可用性。

        仅用于错误信息细化（"No trading days" 分支），不参与任何成功取数路径。
        返回 dict：
          {
            "db_path": str,            # 实际尝试连接的数据库路径
            "file_exists": bool,       # 数据库文件是否存在
            "connection_ok": bool,     # _get_conn() 是否成功拿到只读连接
            "table_exists": bool,      # stock_daily 表是否存在
            "min_time": int|None,      # stock_daily.time 最小值(ms)，无数据/无表为 None
            "max_time": int|None,      # stock_daily.time 最大值(ms)
            "distinct_days": int|None, # stock_daily 不同 time 计数
            "error": str|None,         # 连接/查询阶段的异常信息(若有)
          }
        每个 SQL 单独 try/except，任一失败仅置 error 字段，方法绝不向上抛
        （best-effort 诊断函数）。不做任何缓存（诊断仅失败路径触发，
        且避免与 _cached_min_ms/_max_ms 语义混淆）。
        """
        info = {
            "db_path": str(self._db_path) if self._db_path is not None else None,
            "file_exists": bool(self._db_path is not None and self._db_path.exists()),
            "connection_ok": False,
            "table_exists": False,
            "min_time": None,
            "max_time": None,
            "distinct_days": None,
            "error": None,
        }
        conn = self._get_conn()  # 复用现有契约：成功返回连接、失败返回 None（不改它）
        info["connection_ok"] = conn is not None
        if conn is None:
            # _get_conn 内 try/except: pass 吞掉了真实异常——为诊断补一次独立探测，
            # 仅取异常文本，不替换 _get_conn 的连接实例。
            try:
                import duckdb as _ddb
                _probe = _ddb.connect(str(self._db_path), read_only=True)
                _probe.close()
            except Exception as e:
                info["error"] = str(e)
            return info
        try:
            df = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'stock_daily'"
            ).fetchdf()
            info["table_exists"] = len(df) > 0
        except Exception as e:
            info["error"] = str(e)
            return info
        if not info["table_exists"]:
            return info
        try:
            row = conn.execute(
                "SELECT MIN(time), MAX(time), COUNT(DISTINCT time) FROM stock_daily"
            ).fetchone()
            if row is not None:
                info["min_time"] = int(row[0]) if row[0] is not None else None
                info["max_time"] = int(row[1]) if row[1] is not None else None
                info["distinct_days"] = int(row[2]) if row[2] is not None else None
        except Exception as e:
            info["error"] = str(e)
        return info

    def query_trade_day_offset(self, curr_ms, offset) -> pd.DataFrame:
        """迁移自 PtradeAPI.get_trading_day() (ptrade_api.py:1473-1481)

        offset=0: SELECT MAX(time) FROM stock_daily WHERE time <= curr_ms
        offset>0: SELECT DISTINCT time ... WHERE time > curr_ms ORDER BY time LIMIT offset → tail(1)
        offset<0: SELECT DISTINCT time ... WHERE time < curr_ms ORDER BY time DESC LIMIT abs(offset) → tail(1)
        """
        conn = self._get_conn()
        if conn is None:
            return pd.DataFrame()
        if offset == 0:
            df = conn.execute(f"SELECT MAX(time) as t FROM stock_daily WHERE time <= {curr_ms}").fetchdf()
        elif offset > 0:
            df = conn.execute(f"SELECT DISTINCT time FROM stock_daily WHERE time > {curr_ms} ORDER BY time LIMIT {offset}").fetchdf()
            df = df.tail(1)
        else:
            df = conn.execute(f"SELECT DISTINCT time FROM stock_daily WHERE time < {curr_ms} ORDER BY time DESC LIMIT {abs(offset)}").fetchdf()
            df = df.tail(1)
        return df

    def query_kline_count(self, before_ms) -> int:
        """迁移自 PtradeAPI.get_current_kline_count() (ptrade_api.py:1954-1956)"""
        conn = self._get_conn()
        if conn is None:
            return 0
        n = conn.execute(
            f"SELECT COUNT(DISTINCT time) FROM stock_daily WHERE time <= {before_ms}"
        ).fetchone()[0]
        return int(n)

    # ===================== 工具（与 PtradeAPI._to_ptrade_code 重复，保持行为一致）=====================

    @staticmethod
    def _to_ptrade_code(bare_code: str) -> str:
        """Normalize through the authoritative PTrade code rules."""
        from ..libs.security_code_rules import normalize_to_ptrade
        return normalize_to_ptrade(bare_code)


def CodeDict_clone(dfs):
    """复用 ptrade_api.CodeDict（跨模块导入在运行时完成，避免循环依赖）。"""
    from ..ptrade_api import CodeDict
    return CodeDict(dfs)
