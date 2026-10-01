"""QFQ front 数据质量不变量校验（工作包 D：第三道防线纯函数模块）。

背景（wp7e3-workpackage-D-task.md v1.1）：
    QFQ 复权基准 bug（批次内 groupby().last() 作 adj_latest → front=raw）破坏
    1442 万行而现有审计（AdjustmentAnchor 2% 近似检查）完全抓不住——
    front=raw 时 close_front/close=1，偏离=0，完美通过。
    本模块补"front 与 adj_factor 的精确自洽"盲区，含三道防线：

    防线 1（写入自洽）   check_qfq_invariant —— 对已 align 的 std_df 抽样行
                         独立重算 front == raw × adj_i / adj_latest（口径 A 写入时锚）；
    防线 2（因子完整性） audit_factor_integrity —— 扫 qfq_aux.db 因子表本身
                         （缺日/异常跳变/单日突增/独立交叉源抽核）；
    防线 3（黄金行冒烟） check_golden_rows —— 启动时对黄金行重算比对
                         （期望值带 anchor_version，因子变更时由
                          refresh_golden_rows_for_code 自动刷新）。

设计约束（任务书 §6 禁止事项）：
    - 纯只读/观测：不写任何主库价格表、不改 front/raw 值；
    - 独立重算路径：不 import aligner._apply_qfq 的内部计算，公式独立实现；
    - 三类行跳过（native 直通 / NULL front / 无因子日）防误报；
    - 相对容差 REL_TOL=1e-6；
    - 分层抽样：每 code 必含最新交易日行（adj_latest 演进最先影响处），
      随机抽样补充；上限每 code ≤ 20 行、总计 ≤ 5000 行；
    - 分钟表按交易日（bar_day）连接因子，非毫秒时间戳等值（同
      aligner.py:940-943 口径）。

能力边界（任务书 §1.4，必须写明）：
    本模块只能抓"代码/字段映射 bug"；抓不了"因子源数据错"——
    adj_i 与 adj_latest 同源于 qfq_aux.db，同错则自洽仍通过。
    因子源错由 audit_factor_integrity 的独立交叉源抽核承接
    （独立性以 MCP 为唯一权威因子源的终态为前提；过渡期库内
    tushare 历史因子被抽中时为同源核验，已知可接受，R2 声明）。
"""
from __future__ import annotations

import json
import random
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from quantstudio._paths import DATA_ROOT

logger = __import__("logging").getLogger(__name__)

# —— 常量（任务书 §1.2 / §8）——
REL_TOL = 1e-6                 # 相对容差：|front-expect| / max(|expect|, eps)
MAX_ROWS_PER_CODE = 20         # 抽样上限：每 code
MAX_ROWS_TOTAL = 5000          # 抽样上限：总计
CROSS_SOURCE_SAMPLE_N = 20     # 交叉源抽核默认 code 数
SINGLE_DAY_PCT_THRESHOLD = 0.05  # 单日多 code 突增阈值（全市场 5%）

PRICE_TABLES = frozenset({"stock_daily", "stock_minutes", "etf_daily", "etf_minutes"})
ETF_TABLES = frozenset({"etf_daily", "etf_minutes"})
# native 直通源：front 为 passthrough 值，非 raw×factor（aligner.py:414-420 同源直通）
NATIVE_ADJUSTMENT_SOURCES = frozenset({"baostock", "akshare", "xtquant"})

_PRICE_COLS = ("open", "high", "low", "close")


def default_aux_path(main_db_path=None) -> Path:
    """qfq_aux.db 路径（与 qfq_maintenance 同款推导：主库同目录）。"""
    if main_db_path is not None:
        return Path(main_db_path).resolve().parent / "qfq_aux.db"
    return DATA_ROOT / "qfq_aux.db"


def default_golden_rows_path() -> Path:
    """黄金行清单默认落盘位置（任务书 §3.2：config/profiles/mcp_only/）。"""
    root = Path(__file__).resolve().parent.parent.parent
    p = root / "config" / "profiles" / "mcp_only" / "qfq_golden_rows.json"
    return p


def open_ro_sqlite(path) -> sqlite3.Connection:
    """只读打开 SQLite（qfq_aux.db），避免任何写入面。"""
    conn = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _bar_day_from_ms(ms_values: pd.Series) -> pd.Series:
    """毫秒时间戳 → 上海时区交易日字符串（同 aligner bar_day 口径）。"""
    return (pd.to_datetime(ms_values, unit="ms", utc=True)
            .dt.tz_convert("Asia/Shanghai").dt.strftime("%Y-%m-%d"))


def _adj_table_of(table: str) -> str:
    return "fund_adj" if table in ETF_TABLES else "adj_factor"


def _close_or_none(conn, path):
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


# ============================================================================
# 防线 1：写入后精确自洽（口径 A 写入时锚）
# ============================================================================

def check_qfq_invariant(df: pd.DataFrame, table: str,
                        adj_latest_map: Optional[Dict[str, float]],
                        aux_conn: Optional[sqlite3.Connection] = None,
                        aux_path=None,
                        source: Optional[str] = None,
                        seed: Optional[int] = None,
                        anchor_source: str = "caller") -> Dict[str, Any]:
    """对已 align 的 std_df 抽样行做精确复权自洽校验（独立重算，不比对自己）。

    校验式（与 _apply_qfq 同公式、独立实现）：
        front_expect = raw × adj_i / adj_latest
        bad  ⇔  |front - front_expect| / max(|front_expect|, eps) > REL_TOL

    数据来源（错误一 T1 修订 2026-09-22——**复算输入与写入公式同源**）：
        raw        ← df 的 open/high/low/close（aligner 只算 front/back，raw 保留原值）
        adj_i      ← **优先 df 行内 adj_factor 列（=_apply_qfq 写入时同一值）**；
                     行内缺失 → 回退 qfq_aux.db 按 (code, bar_day) 精确查（分钟按交易日
                     连接）并计 cross_source_adj_i（跨源行可见——锚演进窗的旧误报源）
        adj_latest ← 调用方传入的快照 map（口径 A：本批 align 实际使用的写入时锚）
        anchor_source ← "caller"=map 沿调用链同批传入（bad 判定可信）；
                     "r3_reload"=daemon 兜底重载（9-22 06:44 实录路径：自检锚可能与
                     写入锚不一致）——锚不可信路径行级偏离一律计 **unknown 不计 bad
                     不计 streak**（UNKNOWN 不得吞真异常：真错写在 caller 路径照常 bad）。

    四类行跳过（任务书 §1.3 + T1 修订，防误报）：
        - native 直通源（source ∈ {baostock, akshare, xtquant}）→ 整批跳过；
        - NULL front（front 或 adj_i 缺失）→ 计入 skipped；
        - 无因子日（qfq_aux.db 查不到该 (code, bar_day)）→ 计入 skipped；
        - 锚 map 缺该 code → skipped（既有）+ unknown_rows（T1 新增维度）。

    返回（旧键全保留向后兼容；T1 新增 unknown_rows/unknown_rate/
    cross_source_adj_i/anchor_source）：
        {"sampled": int, "bad": int, "skipped": int,
         "bad_detail": [ {code, day, col, front, expect} ... ]（≤ 20 条）,
         "missing_factor_rows": int, "no_anchor_codes": int,
         "unknown_rows": int, "unknown_rate": float,
         "cross_source_adj_i": int, "anchor_source": str}
        adj_latest_map 为空 → {"sampled": 0, "skipped": len(df),
        "unknown_rows": len(df), "unknown_rate": 1.0}（测试 6：跳过不抛错，
        自检是观测，不承担 fail-fast 职责；UNKNOWN 计数可见）。
    """
    out: Dict[str, Any] = {"sampled": 0, "bad": 0, "skipped": 0,
                           "bad_detail": [], "missing_factor_rows": 0,
                           "no_anchor_codes": 0,
                           "unknown_rows": 0, "unknown_rate": 0.0,
                           "cross_source_adj_i": 0,
                           "anchor_source": anchor_source}
    if df is None or len(df) == 0:
        return out
    # 跳过类 1：native 直通源（front 是 passthrough 值，非 raw×factor）
    if source is not None and source in NATIVE_ADJUSTMENT_SOURCES:
        out["skipped"] = int(len(df))
        return out
    # 测试 6：无锚 → 全跳过（不抛错、不阻断）；T1：同时计 UNKNOWN 可见
    if not adj_latest_map:
        out["skipped"] = int(len(df))
        out["unknown_rows"] = int(len(df))
        out["unknown_rate"] = 1.0
        return out

    sampled = _stratified_sample(df, seed=seed)
    if sampled.empty:
        return out

    own_conn = False
    if aux_conn is None:
        if aux_path is None:
            out["skipped"] = int(len(sampled))
            return out
        try:
            aux_conn = open_ro_sqlite(aux_path)
            own_conn = True
        except Exception as exc:
            logger.warning(f"[QFQ-Invariant] qfq_aux.db 只读打开失败，自检跳过: {exc}")
            out["skipped"] = int(len(sampled))
            return out

    try:
        # 抽样行的 bar_day（分钟表按交易日连接，非毫秒等值）
        # 性能优化（2026-09-30，方案 qfq-invariant-perf）：**先算 days，再据此推导
        # 因子查询时间窗**——原实现先全量拉因子（无时间条件）再算 days，
        # 导致每片拉取 ~555 code × 全历史 ~287 万行（实测 22.45s/片，
        # 占写阶段总耗时 60%+）。加时间窗后实测 0.03s（**769×**）。
        # 等价性：factor_lookup 仅被下方 `factor_lookup.get((code, day))` 消费，
        # 而 day 全部来自本行 days → 窗口外的因子行**查了也从不使用**。
        days = _bar_day_from_ms(sampled["time"].astype("int64"))
        _win_lo, _win_hi = _factor_query_window_ms(days)
        # 独立查 adj_i：抽中 code 的因子（**限本片时间窗**）→ (code, bar_day) 索引
        factor_lookup = _load_factor_lookup(
            aux_conn, _adj_table_of(table), sorted(sampled["code"].astype(str).unique()),
            time_lo_ms=_win_lo, time_hi_ms=_win_hi)
        no_anchor_codes = set()
        for idx, row in sampled.iterrows():
            code = str(row["code"])
            adj_latest = adj_latest_map.get(code)
            if adj_latest is None or adj_latest <= 0:
                no_anchor_codes.add(code)
                out["skipped"] += 1        # 既有语义保留
                out["unknown_rows"] += 1   # T1：锚 map 缺该 code=锚不可信维度可见
                continue
            day = days.loc[idx]
            # T1 同源优先（错误一，2026-09-22）：行内 adj_factor 列 = _apply_qfq
            # 写入时同一值；行内缺失才回退 aux 查询（520550 类锚演进窗内
            # "行内旧因子 vs aux 新因子"正是跨源误报源）并计数可见。
            row_f = row.get("adj_factor")
            if row_f is not None and not pd.isna(row_f) and float(row_f) > 0:
                adj_i = float(row_f)
            else:
                adj_i = factor_lookup.get((code, day))
                out["cross_source_adj_i"] += 1
            if adj_i is None:
                out["missing_factor_rows"] += 1
                out["skipped"] += 1
                continue
            out["sampled"] += 1
            row_bad = False
            for col in _PRICE_COLS:
                raw_v = row.get(col)
                front_v = row.get(f"{col}_front")
                if front_v is None or pd.isna(front_v) or raw_v is None or pd.isna(raw_v):
                    # 跳过类 2：NULL front（因子缺失日/停牌）
                    out["skipped"] += 0  # 行已计入 sampled；NULL 列不判坏
                    continue
                expect = float(raw_v) * adj_i / adj_latest
                denom = max(abs(expect), 1e-12)
                if abs(float(front_v) - expect) / denom > REL_TOL:
                    row_bad = True
                    if len(out["bad_detail"]) < 20:
                        out["bad_detail"].append({
                            "code": code, "day": day, "col": f"{col}_front",
                            "front": float(front_v), "expect": expect})
            if row_bad:
                if anchor_source == "r3_reload":
                    # T1 核心（9-22 06:44 实录路径）：R3 兜底重载锚可能与写入锚
                    # 不一致——该路径行级偏离计 unknown（不判 bad 不计 streak），
                    # 真错写在 caller 路径照常 bad，UNKNOWN 不吞真异常。
                    out["unknown_rows"] += 1
                else:
                    out["bad"] += 1
        out["no_anchor_codes"] = len(no_anchor_codes)
        _denom_u = out["unknown_rows"] + out["sampled"]
        out["unknown_rate"] = (out["unknown_rows"] / _denom_u) if _denom_u else 0.0
        return out
    finally:
        if own_conn:
            _close_or_none(aux_conn, None)


def _stratified_sample(df: pd.DataFrame, seed=None) -> pd.DataFrame:
    """分层抽样（S5）：每 code 必含最新交易日行 + 随机补充；≤20/code、≤5000 总。"""
    rng = random.Random(seed)
    parts: List[pd.DataFrame] = []
    budget = MAX_ROWS_TOTAL
    # 每 code 取 ≤ MAX_ROWS_PER_CODE 行；最新行（time 最大）必选
    for _code, sub in df.groupby("code", sort=False):
        if budget <= 0:
            break
        sub = sub.sort_values("time")
        take_n = min(MAX_ROWS_PER_CODE, len(sub), budget)
        if take_n >= len(sub):
            parts.append(sub)
            budget -= len(sub)
            continue
        latest_idx = sub.index[-1]
        rest = sub.drop(index=latest_idx)
        extra_n = take_n - 1
        if extra_n > 0 and len(rest) > 0:
            picked = rest.sample(n=min(extra_n, len(rest)), random_state=rng.randint(0, 2**31 - 1))
            parts.append(pd.concat([sub.loc[[latest_idx]], picked]).sort_index())
            budget -= take_n
        else:
            parts.append(sub.loc[[latest_idx]])
            budget -= 1
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts)


def _factor_query_window_ms(days) -> Tuple[Optional[int], Optional[int]]:
    """由抽样行的 bar_day 集合推导因子查询的毫秒时间窗（性能优化 2026-09-30）。

    返回 `[lo, hi)`，**按整日取整并前后各留 1 天余量**：
    - `bar_day` 为 CST 交易日字符串（`_bar_day_from_ms` 口径）；
    - 因子表的 time 为毫秒；同一 (code, bar_day) 的多行因子**必须全部落入窗口**，
      否则 `_load_factor_lookup` 内 "取 time 最大一条" 的语义会变 → 破坏等价性；
    - 故窗口取整日后**两端各外扩 1 天**，确保边界（当日 00:00 / 23:59 CST、
      跨时区偏移）不遗漏。

    返回 (None, None) 表示**无法推导** → 调用方不施加时间条件（回退旧行为）。
    """
    try:
        vals = sorted({str(d) for d in days if d})
    except Exception:
        return None, None
    if not vals:
        return None, None
    try:
        # 与 _bar_day_from_ms 同口径（Asia/Shanghai）
        lo = pd.Timestamp(vals[0], tz="Asia/Shanghai")
        hi = pd.Timestamp(vals[-1], tz="Asia/Shanghai") + pd.Timedelta(days=1)
    except Exception:
        return None, None
    # 两端各外扩 1 天（覆盖整日边界与同因子日多行）
    lo = lo - pd.Timedelta(days=1)
    hi = hi + pd.Timedelta(days=1)
    return int(lo.timestamp() * 1000), int(hi.timestamp() * 1000)


def _load_factor_lookup(aux_conn: sqlite3.Connection, adj_table: str,
                        codes: Sequence[str],
                        time_lo_ms: Optional[int] = None,
                        time_hi_ms: Optional[int] = None) -> Dict[Tuple[str, str], float]:
    """从 qfq_aux.db 独立加载 (code, bar_day) → adj_factor。

    因子 time 为毫秒；按交易日（bar_day）建索引，与分钟 bar 连接口径一致
    （同因子日多行取 time 最大一条，对齐 aligner drop_duplicates keep='last'）。

    `time_lo_ms` / `time_hi_ms`（性能优化 2026-09-30，可选）：
        因子查询的时间窗下推。**默认 None = 不施加条件 = 旧行为逐位一致**
        （其他调用方 check_golden_rows / verify_reanchor_selfcheck /
        refresh_golden_rows_for_code 均不传，行为零变化）。
        `check_qfq_invariant` 传入由抽样行 bar_day 推导的窗口——
        因消费点 `factor_lookup.get((code, day))` 的 day 全部来自抽样行，
        窗口外的因子行**查了也从不使用**，故窗口下推为纯增益（实测 769×）。
    """
    lookup: Dict[Tuple[str, str], float] = {}
    if not codes:
        return lookup
    placeholders = ", ".join(["?"] * len(codes))
    # 时间窗下推（可选）：None → 不加条件（旧行为）
    _where = f"WHERE code IN ({placeholders})"
    _params: list = list(codes)
    if time_lo_ms is not None and time_hi_ms is not None:
        _where += " AND time >= ? AND time < ?"
        _params += [int(time_lo_ms), int(time_hi_ms)]
    try:
        rows = aux_conn.execute(
            f"SELECT code, time, adj_factor FROM {adj_table} "
            f"{_where}", _params).fetchall()
    except sqlite3.Error as exc:
        logger.warning(f"[QFQ-Invariant] 因子表 {adj_table} 查询失败: {exc}")
        return lookup
    # 向量化（性能修复 v6.7.53）：原实现对每行因子单独 pd.to_datetime(pd.Series([单行]))
    # （~1ms/行），stock_minutes 时间切片批次抽样 ~555 code × 全历史 ~150 万行
    # → ~24 分钟/片。改为先过滤 NULL（保留原过滤分支语义）后一次性批量转换，
    # 同一 _bar_day_from_ms、同一口径，输出逐键一致（等价性经真实 aux 10 万行验证）。
    valid = [(c, t, f) for c, t, f in rows if f is not None and t is not None]
    if not valid:
        return lookup
    days = _bar_day_from_ms(pd.Series([int(t) for _, t, _ in valid], dtype="int64"))
    best: Dict[Tuple[str, str], Tuple[int, float]] = {}
    for (code, t_ms, factor), day in zip(valid, days):
        key = (str(code), day)
        prev = best.get(key)
        if prev is None or int(t_ms) > prev[0]:
            best[key] = (int(t_ms), float(factor))
    lookup = {k: v[1] for k, v in best.items()}
    return lookup


# ============================================================================
# 防线 2：因子表完整性扫描（补"因子源错"缺口）
# ============================================================================

def audit_factor_integrity(aux_conn: sqlite3.Connection,
                           calendar_conn=None,
                           *,
                           cross_source_fn: Optional[Callable[[str], Optional[float]]] = None,
                           cross_sample_n: int = CROSS_SOURCE_SAMPLE_N,
                           per_table_page_size: int = 500_000) -> Dict[str, Any]:
    """扫 qfq_aux.db 的 adj_factor（股票）与 fund_adj（ETF）两表（只读，不写库）。

    检查项（任务书 §2.1）：
        1. 缺日            同 code 相邻两因子日之间出现交易日历空档（需 calendar_conn，
                          读主库 trade_calendar；不可用则该项跳过并记 stats）
        2. 非单调异常跳变  adj_i / prev_adj_i > 2× 或 < 0.5×（首版告警不自动定性，
                          排除送转定性留待接 dividend_type）
        3. 单日多 code 突增 单 trade_date 内因子值变化的 code 数 > 全市场 5%
        4. 独立交叉源抽核  cross_source_fn(code) 返回官方因子值；偏差 > REL_TOL → error。
                          抽核失败（None/异常）→ warning（网络抖动不制造假 error）。
                          独立性以 MCP 为唯一权威因子源终态为前提（R2）；
                          过渡期 tushare 历史因子被抽中为同源核验（已知可接受）。

    返回：{"warnings": [...], "errors": [...], "stats": {...}}
    """
    warnings: List[str] = []
    errors: List[str] = []
    stats: Dict[str, Any] = {"tables": {}}

    calendar_days: Optional[pd.DatetimeIndex] = None
    if calendar_conn is not None:
        try:
            cal = pd.read_sql("SELECT DISTINCT trade_date FROM trade_calendar",
                              calendar_conn)
            if len(cal) > 0:
                calendar_days = pd.to_datetime(cal["trade_date"]).sort_values()
        except Exception as exc:
            warnings.append(f"trade_calendar 不可读，缺日检查跳过: {exc}")

    for table in ("adj_factor", "fund_adj"):
        t_stats: Dict[str, Any] = {"rows": 0, "codes": 0, "gap_warnings": 0,
                                   "jump_warnings": 0, "spike_warnings": 0,
                                   "cross_checked": 0, "cross_errors": 0}
        try:
            # E 修复：keyset 分页全量读（code > last 游标），无 ORDER BY LIMIT 隐式
            # 截断——1586 万行 adj_factor 必须全覆盖，否则字典序靠后的 code 全漏检。
            frames = []
            last_code, last_time = "", -1
            while True:
                # keyset 复合游标 (code, time)：单 code 游标会丢同 code 的跨页行
                page = aux_conn.execute(
                    f"SELECT code, time, adj_factor FROM {table} "
                    f"WHERE code > ? OR (code = ? AND time > ?) "
                    f"ORDER BY code, time LIMIT {per_table_page_size}",
                    [last_code, last_code, last_time]).fetchall()
                if not page:
                    break
                frames.append(page)
                last_code, last_time = page[-1][0], page[-1][1]
            df = pd.DataFrame([r for page in frames for r in page],
                              columns=["code", "time", "adj_factor"])
        except sqlite3.Error as exc:
            warnings.append(f"因子表 {table} 读取失败: {exc}")
            stats["tables"][table] = t_stats
            continue
        if df.empty:
            stats["tables"][table] = t_stats
            continue
        df["day"] = _bar_day_from_ms(df["time"].astype("int64"))
        df = (df.drop_duplicates(["code", "day"], keep="last")
                .sort_values(["code", "day"]).reset_index(drop=True))
        t_stats["rows"] = int(len(df))
        t_stats["codes"] = int(df["code"].nunique())

        # 检查 2：非单调异常跳变（同 code 相邻因子日比值 >2× 或 <0.5×）
        grp = df.groupby("code")["adj_factor"]
        ratio = df["adj_factor"] / grp.shift(1)   # shift 保持索引对齐
        bad_jump = ((ratio > 2.0) | (ratio < 0.5)).fillna(False)
        n_jump = int(bad_jump.sum())
        t_stats["jump_warnings"] = n_jump
        if n_jump > 0:
            examples = df.loc[bad_jump].head(3)
            sample_txt = ", ".join(
                f"{r['code']}@{r['day']}({ratio.loc[i]:.3f}×)"
                for i, r in examples.iterrows())
            warnings.append(f"[{table}] 异常跳变 {n_jump} 处（比值>2×或<0.5×，"
                            f"含送转待定性）例: {sample_txt}")

        # 检查 3：单日多 code 突增（单日因子变化 code 数 > 全市场 5%）
        changed = df.assign(prev=grp.shift(1)).query("prev.notna() and adj_factor != prev")
        per_day_changed = changed.groupby("day")["code"].nunique()
        threshold = max(1, int(t_stats["codes"] * SINGLE_DAY_PCT_THRESHOLD))
        spiked = per_day_changed[per_day_changed > threshold]
        t_stats["spike_warnings"] = int(len(spiked))
        if len(spiked) > 0:
            top = spiked.sort_values(ascending=False).head(3)
            warnings.append(f"[{table}] 单日因子突增 {len(spiked)} 天超阈值({threshold})，"
                            f"例: {dict(top)}")

        # 检查 1：缺日（相邻因子日之间有交易日历空档）——向量化（E 修复：
        # 逐 code 双循环在千万行上是性能隐患，改为日历位置 searchsorted）
        if calendar_days is not None:
            cal_list = sorted(set(calendar_days.dt.strftime("%Y-%m-%d")))
            cal_pos = {d: i for i, d in enumerate(cal_list)}
            pos = df["day"].map(cal_pos)
            if pos.notna().all():
                df["_cal_i"] = pos.astype("int64")
                nxt = df.groupby("code")["_cal_i"].shift(-1)
                gap_mask = (nxt - df["_cal_i"] > 1) & nxt.notna()
                n_gap = int(gap_mask.sum())
                t_stats["gap_warnings"] = n_gap
                if n_gap > 0:
                    ex = df.loc[gap_mask].head(3)
                    gap_examples = [
                        f"{r['code']}@{r['day']}~缺{int(nxt.loc[i] - r['_cal_i'] - 1)}日"
                        for i, r in ex.iterrows()]
                    warnings.append(f"[{table}] 因子缺日 {n_gap} 处，例: {gap_examples}")
                df = df.drop(columns=["_cal_i"])
            else:
                n_off = int(pos.isna().sum())
                warnings.append(f"[{table}] 因子日存在日历外日期（{n_off} 行），"
                                "缺日检查按可映射子集执行")
                df = df[pos.notna().to_numpy()]

        # 检查 4：独立交叉源抽核（唯一能抓 qfq_aux.db 自身缺漏/错因子的手段）
        if cross_source_fn is not None:
            latest = df.sort_values("time").groupby("code")["adj_factor"].last()
            rng = random.Random(20260814)
            sample_codes = rng.sample(sorted(latest.index),
                                      min(cross_sample_n, len(latest)))
            for code in sample_codes:
                try:
                    official = cross_source_fn(str(code))
                except Exception as exc:
                    warnings.append(f"[{table}] 交叉源抽核 {code} 异常（降 warning）: {exc}")
                    continue
                t_stats["cross_checked"] += 1
                if official is None:
                    warnings.append(f"[{table}] 交叉源抽核 {code} 无返回（网络/权限，降 warning）")
                    continue
                if abs(float(official) - float(latest.loc[code])) / max(abs(float(official)), 1e-12) > REL_TOL:
                    errors.append(f"[{table}] 交叉源抽核 {code} 偏离: "
                                  f"aux={latest.loc[code]} vs official={official}")
                    t_stats["cross_errors"] += 1
        stats["tables"][table] = t_stats

    return {"warnings": warnings, "errors": errors, "stats": stats}


# ============================================================================
# 防线 3：黄金行启动自检（smoke test）
# ============================================================================

def _day_ms_range(day: str) -> Tuple[int, int]:
    """黄金行日期（上海时区零点）→ [start_ms, end_ms) 毫秒区间（_start_ms 同款口径）。

    Python 端计算毫秒，SQL 只做参数化范围比较——避免 DuckDB INTERVAL 拼写/
    时区陷阱，且与主库日线 time 存储口径（上海零点 epoch ms）精确对齐。
    """
    start_ms = int(pd.Timestamp(str(day), tz="Asia/Shanghai").value // 10**6)
    return start_ms, start_ms + 86_400_000


def check_golden_rows(golden_rows: Optional[List[Dict[str, Any]]] = None,
                      main_conn=None,
                      aux_conn: Optional[sqlite3.Connection] = None,
                      aux_path=None,
                      adj_latest_map: Optional[Dict[str, float]] = None,
                      golden_path=None,
                      main_db_path=None) -> Dict[str, Any]:
    """黄金行冒烟自检（定位：冒烟测试，非防线——只能证明这几个具体值对不对）。

    每行黄金行重算 close_front = raw_close × adj_i / adj_latest，
    与 close_front_expected 比对（相对容差 REL_TOL）；不匹配 → mismatch detail
    （含 anchor_version）。调用方据此告警（不阻断启动）。

    参数：
        golden_rows / golden_path 二选一（前者优先，便于测试）；
        main_conn  主库连接（读 raw close；None → 用 duckdb 只读打开主库，
                   路径 = main_db_path 或默认 db_path()）；
        aux_conn / aux_path  因子库（读 adj_i）；
        adj_latest_map  全局锚（None → 从 aux 独立取 per-code 最新因子）。
    """
    if golden_rows is None:
        golden_rows = load_golden_rows(golden_path or default_golden_rows_path())
    out: Dict[str, Any] = {"checked": 0, "mismatched": 0, "details": [],
                           "skipped": 0}
    if not golden_rows:
        return out

    own_aux = False
    own_main = False
    if aux_conn is None:
        try:
            aux_conn = open_ro_sqlite(aux_path or default_aux_path())
            own_aux = True
        except Exception as exc:
            logger.warning(f"[QFQ-Golden] qfq_aux.db 打开失败，黄金行自检跳过: {exc}")
            out["skipped"] = len(golden_rows)
            return out
    try:
        if main_conn is None:
            import duckdb
            from quantstudio._paths import db_path  # 治理方案第3步前置（记录项B）：
            # 快照副本巡检支持。默认 db_path() ≡ DATA_ROOT/"quantstudio.db"（_paths.py:53-55），
            # 不传 main_db_path 时行为与旧硬编码逐字符等价。
            main_conn = duckdb.connect(
                str(Path(main_db_path) if main_db_path is not None else db_path()),
                read_only=True)
            own_main = True  # 自开连接必须自关（F：连接泄漏修复）
        codes = sorted({str(g["code"]) for g in golden_rows})
        latest_map = adj_latest_map or {}
        for g in golden_rows:
            code, table = str(g["code"]), str(g["table"])
            day = str(g["date"])
            expected = float(g["close_front_expected"])
            try:
                _s, _e = _day_ms_range(day)
                row = main_conn.execute(
                    f"SELECT close FROM {table} WHERE code = ? AND time >= ? "
                    f"AND time < ? ORDER BY time DESC LIMIT 1",
                    [code, _s, _e]).fetchone()
            except Exception as exc:
                out["details"].append({**g, "error": f"主库查询失败: {exc}"})
                out["skipped"] += 1
                continue
            if not row or row[0] is None:
                out["details"].append({**g, "error": "主库无该行 raw close"})
                out["skipped"] += 1
                continue
            raw_close = float(row[0])
            factor = _load_factor_lookup(aux_conn, _adj_table_of(table), [code])
            # 黄金行 day 的因子：按 bar_day 精确取
            adj_i = factor.get((code, day))
            if adj_i is None:
                out["details"].append({**g, "error": "qfq_aux.db 无该日因子"})
                out["skipped"] += 1
                continue
            adj_latest = latest_map.get(code)
            if adj_latest is None:
                rows = aux_conn.execute(
                    f"SELECT adj_factor FROM {_adj_table_of(table)} WHERE code = ? "
                    f"ORDER BY time DESC LIMIT 1", [code]).fetchone()
                adj_latest = float(rows[0]) if rows and rows[0] is not None else None
            if adj_latest is None or adj_latest <= 0:
                out["details"].append({**g, "error": "无全局锚"})
                out["skipped"] += 1
                continue
            actual = raw_close * adj_i / adj_latest
            out["checked"] += 1
            if abs(actual - expected) / max(abs(expected), 1e-12) > REL_TOL:
                out["mismatched"] += 1
                out["details"].append({
                    **g, "actual": actual, "expected": expected,
                    "raw_close": raw_close, "adj_i": adj_i, "adj_latest": adj_latest,
                    "mismatch": True})
        return out
    finally:
        if own_aux:
            _close_or_none(aux_conn, None)
        if own_main:
            try:
                main_conn.close()
            except Exception:
                pass


def load_golden_rows(path=None) -> List[Dict[str, Any]]:
    p = Path(path or default_golden_rows_path())
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"[QFQ-Golden] 黄金行清单读取失败 {p}: {exc}")
        return []


def save_golden_rows(rows: List[Dict[str, Any]], path=None) -> None:
    p = Path(path or default_golden_rows_path())
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


# ============================================================================
# S2：黄金行期望值挂 reanchor committed 事件自动刷新
# ============================================================================

def refresh_golden_rows_for_code(code: str, table: str,
                                 main_conn, aux_conn: sqlite3.Connection,
                                 golden_path=None) -> Optional[Dict[str, Any]]:
    """reanchor committed 后自动重算该 code 黄金行期望值并递增 anchor_version。

    只写黄金行清单配置（json），不写任何主库价格表（任务书 §3.4）。
    anchor_version 递增规则：现有值形如 "v6.7.52-3" / "v6.7.52" → 尾号 +1。
    """
    path = Path(golden_path or default_golden_rows_path())
    rows = load_golden_rows(path)
    mine = [g for g in rows if str(g.get("code")) == str(code)
            and str(g.get("table")) == table]
    if not mine:
        return None
    # 独立取当前锚与该行因子
    anchor_row = aux_conn.execute(
        f"SELECT adj_factor FROM {_adj_table_of(table)} WHERE code = ? "
        f"ORDER BY time DESC LIMIT 1", [str(code)]).fetchone()
    if not anchor_row or anchor_row[0] is None:
        return None
    adj_latest = float(anchor_row[0])
    changed = 0
    for g in mine:
        day = str(g["date"])
        _s, _e = _day_ms_range(day)
        raw_row = main_conn.execute(
            f"SELECT close FROM {table} WHERE code = ? AND time >= ? "
            f"AND time < ? ORDER BY time DESC LIMIT 1",
            [str(code), _s, _e]).fetchone()
        if not raw_row or raw_row[0] is None:
            continue
        factor = _load_factor_lookup(aux_conn, _adj_table_of(table), [str(code)])
        adj_i = factor.get((str(code), day))
        if adj_i is None:
            continue
        new_expected = float(raw_row[0]) * adj_i / adj_latest
        if abs(new_expected - float(g["close_front_expected"])) / max(
                abs(float(g["close_front_expected"])), 1e-12) > REL_TOL:
            g["close_front_expected"] = new_expected
            g["anchor_version"] = _bump_anchor_version(g.get("anchor_version", "v0"))
            g["refreshed_at"] = pd.Timestamp.now(tz="Asia/Shanghai").isoformat()
            changed += 1
    if changed:
        save_golden_rows(rows, path)
        logger.info(f"[QFQ-Golden] {code}/{table} reanchor committed → "
                    f"黄金行期望值刷新 {changed} 行（anchor_version 已递增）")
    return {"code": code, "table": table, "refreshed": changed}


def _bump_anchor_version(v: str) -> str:
    """"v6.7.52-3" → "v6.7.52-4"；无尾号 → "-2"。"""
    s = str(v)
    if "-" in s:
        base, _, tail = s.rpartition("-")
        try:
            return f"{base}-{int(tail) + 1}"
        except ValueError:
            return f"{s}-2"
    return f"{s}-2"


# ============================================================================
# 2.2.4：重锚后全历史自洽（口径 B）
# ============================================================================

def verify_reanchor_selfcheck(main_conn, aux_conn: sqlite3.Connection, *,
                              code: str, table: str,
                              adj_latest_new: Optional[float] = None,
                              row_limit: int = 500_000) -> Dict[str, Any]:
    """重锚完成后，用本次重锚的新锚对该 code 全历史 front 精确校验（只读）。

    口径 B：锚刚更新、历史 front 刚被重锚 → 用新锚校验无基准演进行误报。
    偏离 = 0 即"重锚正确完成"；偏离 > 0 即"重锚有漏/错"，正是最需要抓的点。
    （S3 前置核实结论：引擎现有 postcheck 是"front vs fresh 源逐 bar"校验，
    未含本口径 B 全历史自洽 → 本函数为新增，不改变引擎重锚逻辑。）
    """
    out: Dict[str, Any] = {"code": code, "table": table, "rows": 0, "bad": 0,
                           "bad_detail": []}
    if adj_latest_new is None:
        anchor_row = aux_conn.execute(
            f"SELECT adj_factor FROM {_adj_table_of(table)} WHERE code = ? "
            f"ORDER BY time DESC LIMIT 1", [str(code)]).fetchone()
        if not anchor_row or anchor_row[0] is None:
            out["error"] = "无锚（qfq_aux.db 查不到该 code 最新因子）"
            return out
        adj_latest_new = float(anchor_row[0])
    if adj_latest_new <= 0:
        out["error"] = f"锚非法: {adj_latest_new}"
        return out
    try:
        df = main_conn.execute(
            f"SELECT code, time, open, high, low, close, "
            f"open_front, high_front, low_front, close_front "
            f"FROM {table} WHERE code = ? ORDER BY time LIMIT {row_limit}",
            [str(code)]).fetchdf()
    except Exception as exc:
        out["error"] = f"主库查询失败: {exc}"
        return out
    if df is None or df.empty:
        return out
    factor = _load_factor_lookup(aux_conn, _adj_table_of(table), [str(code)])
    days = _bar_day_from_ms(df["time"].astype("int64"))
    out["rows"] = int(len(df))
    for i, row in df.iterrows():
        day = days.loc[i]
        adj_i = factor.get((str(code), day))
        if adj_i is None:
            continue  # 因子缺失日/停牌：跳过
        for col in _PRICE_COLS:
            raw_v, front_v = row.get(col), row.get(f"{col}_front")
            if raw_v is None or pd.isna(raw_v) or front_v is None or pd.isna(front_v):
                continue
            expect = float(raw_v) * adj_i / adj_latest_new
            if abs(float(front_v) - expect) / max(abs(expect), 1e-12) > REL_TOL:
                out["bad"] += 1
                if len(out["bad_detail"]) < 20:
                    out["bad_detail"].append({"day": day, "col": f"{col}_front",
                                              "front": float(front_v), "expect": expect})
    return out
