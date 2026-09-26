"""
Ptrade API 兼容层
在 QuantStudio 回测引擎上模拟 Ptrade 平台的 API 接口
策略代码可原封不动从 Ptrade 移植运行

数据 100% 来自 DuckDB（QuantStudio 数据管线产出）
"""
from __future__ import annotations

import logging
import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import numpy as np

from .libs.security_code_rules import (
    bare_code, classify_security, exchange as security_exchange,
    normalize_security_code, normalize_to_ptrade, normalize_to_qmt,
)
# PR3: 频率查询能力错误（分钟数据缺失/不支持时上抛，严禁静默回退日线）
from .providers.frequency_labels import FrequencyCapabilityError
from .providers.base import ReferenceDataCapabilityError

logger = logging.getLogger(__name__)


# ==================== 按日行情索引构建（纯性能优化，无缓存） ====================
# 仅把 O(行数) 的全表布尔过滤替换为 O(1) 字典查找，不改变任何返回数据/字段/归一化/PIT 语义。
# 索引键为 df['code'] 的「原始元素」（不做任何转换：不 str、不 bare_code、不去空格、不改大小写），
# 查找键为调用处原本传给 `df['code'] == bare` 右侧的 bare 原值 —— 二者与 pandas 元素比较语义
# 逐字节一致，因此 `iloc[index]` 返回的正是原布尔过滤 `df[df['code'] == bare].iloc[0]` 那一行
# （取首次出现）。缓存不在此处做，由各调用方（DataDict / PtradeAPI / BacktestEngine）按实例
# 生命周期持有并在 DataFrame 切换时失效，避免跨实例共享状态与 GC/finalizer 时序依赖。
def _build_code_index(df):
    """构建 {raw_code_value: 首次出现 iloc} 索引（纯函数，无内部缓存）。

    返回语义（调用点据此严格区分两种状态）：
      - None : 无法安全构建（df 为 None / 缺 'code' 列 / 构建异常）→ 调用点必须回退原布尔过滤，
               以保留原路径的异常（如缺列 KeyError）、空值与 fallback 行为。
      - dict : 已构建成功。含「合法空 DataFrame（有 code 列但 0 行）」返回 {}；用 .get(raw) 取 iloc。
               注意：{} 与 None 不可混淆 —— 前者代表「成功但无数据」，后者代表「不可用，需回退」。
    """
    if df is None or not hasattr(df, 'columns') or 'code' not in df.columns:
        return None
    idx = {}
    try:
        col = df['code']
        vals = col.tolist()  # 一次性转 Python list，避免逐元素 pandas 标量访问（O(行数)→O(1) 字典构建）
        for i in range(len(vals)):
            idx.setdefault(vals[i], i)  # 重复 code 保留首次出现 iloc，对齐 .iloc[0]
    except Exception:
        return None
    return idx


# ==================== 全局对象 ====================

class GlobalVars:
    """模拟 Ptrade 的 g 全局变量对象"""
    def __init__(self):
        self.index = ""
        self.buy_stock_count = 5
        self.screen_stock_count = 10
        self.df = pd.DataFrame()
        self.pre_position_list = []


g = GlobalVars()


class LogWrapper:
    """模拟 Ptrade 的 log 对象（兼容 printf 风格多参：log.info(fmt, *args)）。"""
    @staticmethod
    def _render(msg, args):
        # 无额外参数：原样输出（兼容 f-string / 单字符串写法）
        if not args:
            return msg
        # 字符串按 % 风格格式化，对齐真实 Ptrade 行为
        if isinstance(msg, str):
            try:
                return msg % args
            except (TypeError, ValueError):
                # 占位符缺失/不匹配时退化为拼接，避免抛异常中断回测
                return msg + " " + " ".join(str(a) for a in args)
        return msg

    def info(self, msg, *args):
        logger.info(self._render(msg, args))
    def warning(self, msg, *args):
        logger.warning(self._render(msg, args))
    def error(self, msg, *args):
        logger.error(self._render(msg, args))
    def critical(self, msg, *args):
        logger.critical(self._render(msg, args))
    def debug(self, msg, *args):
        logger.debug(self._render(msg, args))


log = LogWrapper()


# ==================== Context / Data 对象 ====================

class Portfolio:
    """模拟 Ptrade 的 context.portfolio。

    D4-S7 Portfolio 活属性（方案 portfolio-live-cash-design.md v2，2026-08-28 ZCode 批准）：
    cash/market_value/total_value/portfolio_value/positions 全部委托引擎 account
    （单一真源：_api._engine.account），引擎成交直接改 account 后实时可读，与平台
    context.portfolio 实时语义对齐。构造期/断链（_api._engine 未 attach）回退初始化快照。
    """

    def __init__(self, cash: float, positions: dict):
        self._init_cash = float(cash)
        self._init_positions = dict(positions or {})
        # 无引擎场景（构造期/单测/非 ptrade 模式）快照：market_value 从传入 positions 推算
        self._snapshot_market_value = sum(
            (p.last_sale_price if hasattr(p, 'last_sale_price') else p.get('current_price', 0))
            * (p.amount if hasattr(p, 'amount') else p.get('volume', 0))
            for p in self._init_positions.values()
        )

    @staticmethod
    def _engine():
        """单次解析引擎对象（唯一解引用点）。

        positions / _engine_account 共用此入口，避免同一属性内二次解引用造成
        状态漂移窗口（2026-09-04 持仓视图契约修复，终审打磨项②）。
        """
        try:
            from quantstudio.backtest.ptrade_api import _api
            return getattr(_api, "_engine", None)
        except Exception:
            return None

    def _engine_account(self):
        """回测运行时委托引擎 account（单一真源）；无 engine → None（快照兜底）。"""
        eng = self._engine()
        if eng is not None:
            acc = getattr(eng, "account", None)
            if acc is not None:
                return acc
        return None

    @property
    def cash(self) -> float:
        acc = self._engine_account()
        return acc.cash if acc is not None else self._init_cash

    @property
    def market_value(self) -> float:
        acc = self._engine_account()
        if acc is not None:
            try:
                prices = getattr(_api, "_prices", None) or {}
                return acc.market_value_at_price(prices)
            except Exception:
                pass
        return self._snapshot_market_value

    @property
    def total_value(self) -> float:
        return self.cash + self.market_value

    @property
    def positions(self) -> dict:
        """D2：只读快照（copy）——策略改返回 dict 不得影响引擎持仓（平台 get_positions 快照语义）。

        契约（2026-09-04 持仓视图契约修复，docs/portfolio-position-view-contract-design.md）：
        引擎在场时必须经**唯一适配器** BacktestEngine._get_ptrade_positions 产出
        PTrade Position 契约对象（sid / amount / enable_amount / cost_basis /
        last_sale_price / avg_cost，键 .SS/.SZ/.BJ 精确匹配）。

        旧实现 `return dict(acc.positions)` 直吐引擎 dataclass（volume/can_sell），
        使策略侧契约字段全部 MISSING→0（静默错误：判空仓、不可卖、零市值），已修复。
        无引擎或无适配器时回退构造期快照，**形状与契约一致**（终审条件⑤）。
        """
        eng = self._engine()                     # 单次解析（与 _engine_account 同源）
        if eng is not None and getattr(eng, "account", None) is not None:
            getter = getattr(eng, "_get_ptrade_positions", None)
            if getter is not None:
                return getter(getattr(_api, "_prices", None) or {})
        return dict(self._init_positions)

    @property
    def portfolio_value(self):
        """PTrade 标准属性：组合总净值（= 现金 + 持仓市值）。

        PTrade 公开 API 为 context.portfolio.portfolio_value；本地此前仅有
        total_value 别名，导致可移植策略在本框架运行报 AttributeError。
        此处补齐标准属性，保持与 PTrade 语义一致（框架级可复用修复）。
        """
        return self.total_value

    @property
    def positions_value(self):
        """PTrade 标准属性：持仓市值（本地别名 market_value）。"""
        return self.market_value


class Position:
    """模拟 Ptrade 的 position 对象"""
    def __init__(self, sid: str, volume: int, avg_cost: float, current_price: float,
                 enable_amount: int = None):
        self.sid = sid
        self.amount = volume
        # T+1 可卖量（2026-09-04 持仓视图契约修复）：
        # 缺省回落 volume 以保持既有构造兼容；引擎适配器显式传入
        # can_sell - pending_sell_shares（close/open 模式下 pending 恒为 0）。
        self.enable_amount = volume if enable_amount is None else int(enable_amount)
        self.cost_basis = avg_cost
        self.last_sale_price = current_price
        self.avg_cost = avg_cost

    @property
    def market_value(self):
        """持仓市值 = 当前价 × 持仓量"""
        return (self.last_sale_price or 0) * (self.amount or 0)


class Context:
    """模拟 Ptrade 的 context 对象"""
    def __init__(self, current_date: str, prev_date: str, portfolio: Portfolio):
        self.current_dt = pd.Timestamp(current_date)
        self.previous_date = pd.Timestamp(prev_date)
        self.portfolio = portfolio
        # blotter.current_dt：部分策略用 context.blotter.current_dt.strftime() 取当前日期
        self.blotter = type("_Blotter", (), {"current_dt": self.current_dt})()

    @property
    def capital_base(self) -> float:
        """PTrade 平台 context.capital_base 等价语义 —— **恒定初始资金，非活值**。

        单一真源：引擎 _initial_capital（backtest_engine.py:414 由构造参数 capital 写入；
        先例 result_exporter.py:29 直读同一属性）。

        **为何不以 portfolio 为源**：Context 的 4 处构造点（backtest_engine.py
        :486 / :2159 / :2264 / :2423）传入 Portfolio(self.account.cash, ...)，
        而运行期 account.cash 是**活值**（成交后变化），刷新点取值 != 初始资金
        ==> portfolio 不可作为 capital_base 的来源，仅作无引擎兜底快照。

        无引擎语境（构造期 / 单测 / 非 ptrade 模式）回退 portfolio._init_cash 快照
        —— 与 Portfolio 既有「构造期快照兜底」设计同构（D4-S7 活属性模式）。

        平台文档说明：Context7 两 PTrade 文档源（/websites/ptradeapi、
        /kay-ou/ptradeapi）**均未收录** capital_base 条目；本语义依据策略内契约声明
        （四象限ETF轮动策略.py:26 注释 + :1226 元数据）与引擎初始资金口径确立。
        """
        eng = Portfolio._engine()   # 复用唯一解引用点，避免二次解引用漂移窗口
        init = getattr(eng, "_initial_capital", None) if eng is not None else None
        if init is not None:
            return float(init)
        return float(self.portfolio._init_cash)


class BarData:
    """模拟 Ptrade 的 data[security] 返回的 bar 数据"""
    def __init__(self, row: pd.Series, dt: str):
        self.dt = dt
        self.open = float(row.get('open', 0))
        self.high = float(row.get('high', 0))
        self.low = float(row.get('low', 0))
        self.close = float(row.get('close', 0))
        self.price = float(row.get('close', 0))  # 最新价
        self.volume = float(row.get('volume', 0))
        self.preclose = float(row.get('preClose', row.get('pre_close', 0)))
        # 涨跌停价计算（用 shared_ashare_rules）
        from .libs.shared_ashare_rules import get_price_limit_pct
        bare = bare_code(row.get('code', ''))
        qmt_code = normalize_to_qmt(bare)
        if self.preclose > 0:
            limit_pct = get_price_limit_pct(qmt_code)
            self.high_limit = round(self.preclose * (1 + limit_pct), 2)
            self.low_limit = round(self.preclose * (1 - limit_pct), 2)
        else:
            self.high_limit = 0
            self.low_limit = 0


class DataDict:
    """模拟 Ptrade 的 data 字典对象（data[security] → BarData）

    Ptrade 语义（文档 2783 行）：两位尾缀(.SS/.SZ)与四位尾缀(.XSHG/.XSHE)、
    甚至裸码皆可作为键取值，互通等价。本类按裸码归一化查找以支持该行为。
    性能优化：惰性构建 BarData，只在 data[code] 被访问时才从 curr_data 查找构建。"""
    def __init__(self):
        self._data: Dict[str, BarData] = {}
        self._curr_data = None  # 全市场 DataFrame（惰性构建时用）
        self._day_str = ""
        self._code_index = None  # 实例私有按日索引缓存；set_curr_data 时失效

    @staticmethod
    def _bare(code: str) -> str:
        """提取裸码（去后缀），用于跨后缀(.SS/.XSHG/.SZ/.XSHE/裸码)归一化查找。"""
        return bare_code(code)

    def set_curr_data(self, curr_data, day_str):
        """注入全市场 DataFrame（用于惰性构建 BarData，避免预先构建 5000 个对象）"""
        self._curr_data = curr_data
        self._day_str = day_str
        self._code_index = None  # DataFrame 切换 → 失效索引缓存

    def __getitem__(self, code: str) -> BarData:
        bare = self._bare(code)
        # 优先精确匹配
        if code in self._data:
            return self._data[code]
        for k, v in self._data.items():
            if self._bare(k) == bare:
                return v
        # 惰性构建：从 curr_data 按索引查找对应行（等价于原布尔过滤 .iloc[0]，O(1)）
        if self._curr_data is not None and 'code' in self._curr_data.columns:
            if self._code_index is None:
                self._code_index = _build_code_index(self._curr_data)
            if self._code_index is not None:
                i = self._code_index.get(bare)
                if i is not None:
                    bar = BarData(self._curr_data.iloc[i], self._day_str)
                    ptrade_code = code if '.' in str(code) else self._guess_ptrade_code(bare)
                    self._data[ptrade_code] = bar
                    return bar
            else:
                # 索引不可用 → 回退原布尔过滤（保留缺列 KeyError / 空值 / fallback 行为）
                row = self._curr_data[self._curr_data['code'] == bare]
                if len(row) > 0:
                    bar = BarData(row.iloc[0], self._day_str)
                    ptrade_code = code if '.' in str(code) else self._guess_ptrade_code(bare)
                    self._data[ptrade_code] = bar
                    return bar
        return BarData(pd.Series(), "")

    @staticmethod
    def _guess_ptrade_code(bare):
        return normalize_to_ptrade(bare)

    def __contains__(self, code: str) -> bool:
        bare = self._bare(code)
        if code in self._data:
            return True
        if any(self._bare(k) == bare for k in self._data.keys()):
            return True
        # 惰性检查：curr_data 里有没有
        if self._curr_data is not None and 'code' in self._curr_data.columns:
            if self._code_index is None:
                self._code_index = _build_code_index(self._curr_data)
            if self._code_index is not None:
                return bare in self._code_index
            # 索引不可用 → 回退原布尔过滤语义
            return bare in self._curr_data['code'].values
        return False

    def set(self, code: str, bar: BarData):
        self._data[code] = bar


def _ensure_money_alias(df):
    """B1：DB 物理列 'amount' → Ptrade 契约列 'money' 的返回端逆映射。

    当 DataFrame 含 'amount' 且不含 'money' 时，在末尾追加与 'amount' 同值的
    'money' 列（canonical），并保留 'amount' 别名。绝不删除 'amount'、绝不改变
    任何数值、绝不改变已有列顺序（新列追加在末尾）。幂等。
    """
    if df is None or not hasattr(df, "columns") or len(df) == 0:
        return df
    if "amount" in df.columns and "money" not in df.columns:
        df = df.copy()
        df["money"] = df["amount"]
    return df


class CodeDict(dict):
    """支持证券代码后缀互通的 dict 子类（Ptrade 语义：.SS/.XSHG/.SZ/.XSHE/裸码等价）。

    用于 get_history(is_dict=True)/get_price(is_dict=True)/check_limit 等
    所有返回 {ptrade_code: value} 的 API，使策略用任意后缀都能取到值。
    内部仍按四位后缀(.XSHG/.XSHE)存储，查询时按裸码归一化匹配。"""

    @staticmethod
    def _bare(code: str) -> str:
        return bare_code(code)

    def __getitem__(self, code):
        if super().__contains__(code):
            return super().__getitem__(code)
        bare = self._bare(code)
        for k in self.keys():
            if self._bare(k) == bare:
                return super().__getitem__(k)
        raise KeyError(code)

    def __contains__(self, code) -> bool:
        if super().__contains__(code):
            return True
        return any(self._bare(k) == self._bare(code) for k in self.keys())

    def get(self, code, default=None):
        try:
            return self[code]
        except KeyError:
            return default


# ==================== Ptrade API 函数 ====================

# ===================== Ptrade ORM: query / valuation =====================
# 轻量 ORM 模拟 Ptrade 的 query(valuation.code, valuation.market_cap).filter(...) 链式查询。
# 策略常写：q = query(valuation.code, valuation.market_cap).filter(market_cap>=X).order_by(market_cap.asc())
#          df = get_fundamentals(q)

class _Field:
    """valuation 表字段描述符（支持比较运算符生成过滤条件）。"""
    def __init__(self, name: str, table: str = "valuation"):
        self.name = name      # 字段名（如 'market_cap'）
        self.table = table    # 表名（valuation）

    def __ge__(self, v): return _Filter(self.name, ">=", v)
    def __le__(self, v): return _Filter(self.name, "<=", v)
    def __gt__(self, v): return _Filter(self.name, ">", v)
    def __lt__(self, v): return _Filter(self.name, "<", v)
    def __eq__(self, v): return _Filter(self.name, "==", v)

    def asc(self): return _OrderBy(self.name, "asc")
    def desc(self): return _OrderBy(self.name, "desc")


class _Filter:
    def __init__(self, field: str, op: str, value):
        self.field, self.op, self.value = field, op, value


class _OrderBy:
    def __init__(self, field: str, direction: str):
        self.field, self.direction = field, direction


class _ValuationTable:
    """valuation 表的 ORM 描述。strategy: valuation.code / valuation.market_cap / valuation.circulating_market_cap"""
    def __init__(self):
        self.code = _Field("code")
        self.market_cap = _Field("market_cap")                # 总市值（亿元）
        self.circulating_market_cap = _Field("circ_mv")       # 流通市值（亿元）
        self.float_value = _Field("circ_mv")
        self.a_floats = _Field("free_share")
        self.total_value = _Field("total_mv")
        self.total_share = _Field("total_share")
        self.pe_ratio = _Field("pe_ratio")
        self.pb_ratio = _Field("pb_ratio")
        self.ps_ratio = _Field("ps_ratio")
        self.turnover_ratio = _Field("turnover_ratio")


class QueryBuilder:
    """query() 返回的查询构建器，支持链式 .filter() / .order_by() / .limit()。"""
    def __init__(self, fields: list):
        self._fields = fields             # [_Field, ...]
        self._filters: list = []          # [_Filter, ...]
        self._order_by: list = []         # [_OrderBy, ...]
        self._limit: int = None

    def filter(self, *conditions) -> "QueryBuilder":
        self._filters.extend(conditions)
        return self

    def order_by(self, *orders) -> "QueryBuilder":
        self._order_by.extend(orders)
        return self

    def limit(self, n: int) -> "QueryBuilder":
        self._limit = n
        return self

    @property
    def field_names(self) -> list:
        return [f.name if isinstance(f, _Field) else str(f) for f in self._fields]


def query(*fields) -> QueryBuilder:
    """Ptrade ORM 查询入口：query(valuation.code, valuation.market_cap) → QueryBuilder"""
    return QueryBuilder(list(fields))


# valuation 表描述符（全局单例，策略通过 valuation.market_cap 访问）
valuation = _ValuationTable()


class PtradeAPI:
    """Ptrade API 兼容层。所有 Ptrade 策略调用的函数在这里实现。
    数据来自 DuckDB，通过 _engine 注入。"""

    def __init__(self, market=None, fundamental=None, reference=None, calendar=None,
                 fidelity_config=None):
        self._engine = None  # BacktestEngine 引用
        self._cfg = None     # EngineConfig（A1：由 attach 从 engine.config 注入，消除 19 处硬编码）
        self._market = market
        self._fundamental = fundamental
        self._reference = reference
        self._calendar = calendar
        # P-A0/P-A2：PTrade 保真模式配置（None = 全关，本地正确语义锚；opt-in 验证用途）
        self._fidelity = fidelity_config
        self._fidelity_ashare_codes = None  # P-A0 快照裸码缓存（首次 get_Ashares 惰性加载）
        self._current_day_data = None  # 当日全市场数据
        self._code_index = None        # 实例私有按日索引缓存；attach_day/attach_bar/reset_session 失效
        self._prev_day_data = None     # 前一日全市场数据
        self._current_date = ""
        self._prev_date = ""
        self._benchmark = "000300"
        self._limit_mode = "LIMIT"
        self._prices = {}  # 当日价格字典（供即时执行用）
        # Phase 4B：当日全 universe 分钟历史内存缓存（引擎 _run_minute_day 注入）。
        self._day_minute_history = None   # 原始列（fq=None 未替换 OHLC）的当日分钟 DataFrame
        self._day_minute_date = None      # 缓存对应的交易日 'YYYY-MM-DD'

    def reset_session(self):
        """清理一次策略运行的可变状态。"""
        self._engine = None
        self._cfg = None
        self._current_day_data = None
        self._prev_day_data = None
        self._current_date = ""
        self._prev_date = ""
        self._benchmark = "000300"
        self._limit_mode = "LIMIT"
        self._prices = {}
        self._query_cache = {}
        self._daily_tasks = []
        self._current_bar_ts = None   # PR4 缺口 1：分钟 Profile 当前 bar 时间戳
        self._pct_chg_map = None      # PR4：日级 pctChg（分钟涨跌停判断）
        self._market = None
        self._fundamental = None
        self._reference = None
        self._calendar = None
        # P-A0/P-A2：保真配置是引擎级运行配置（跨日持久），reset_session 不清除；
        # 仅清派生缓存（快照裸码需在下次 attach 后惰性重载）。调用方在 run 前
        # set_fidelity_config(cfg) 或经 engine.config.fidelity_config 注入。
        self._fidelity_ashare_codes = None
        self._code_index = None       # 会话重置 → 失效索引缓存
        # Phase 4B：清空当日分钟历史内存缓存
        self._day_minute_history = None
        self._day_minute_date = None
        g.__dict__.clear()
        g.__dict__.update(GlobalVars().__dict__)

    def set_fidelity_config(self, fidelity_config):
        """P-A0/P-A2：注入 PTrade 保真模式配置（opt-in，验证转换产物用）。

        调用时机：run 之前（reset_session 不清除本配置，跨日持久）。
        配置为 None/默认全关时，本地保持正确语义锚（P-D9 裁定），零行为影响。
        """
        self._fidelity = fidelity_config
        self._fidelity_ashare_codes = None  # 配置变更 → 失效快照缓存

    def attach(self, engine, curr_data: pd.DataFrame, prev_data: pd.DataFrame,
               curr_date: str, prev_date: str, prices: dict = None):
        """每个交易日注入引擎数据（向后兼容别名 = attach_day）。

        PR4：日线循环调此方法（= attach_day，含 preload）。分钟循环用 attach_day（每日一次）
        + attach_bar（每 bar 一次，跳过 preload）。
        """
        self.attach_day(engine, curr_data, prev_data, curr_date, prev_date, prices)

    def attach_day(self, engine, curr_data: pd.DataFrame, prev_data: pd.DataFrame,
                   curr_date: str, prev_date: str, prices: dict = None):
        """PR4: 每日一次注入引擎数据，含 preload（行情/估值/参考数据预加载）。

        【修正缺口 2】分钟 Profile 的 _prices 应传昨日收盘价（非当日 close），
        因 before_trading_start 在 09:31 前执行，不能见到当日收盘。日线 Profile
        仍传当日 close（日级 now 未定义的约定，与现有行为一致）。调用方负责传入正确的 prices。
        """
        self._engine = engine
        # A1：从 engine 注入 EngineConfig，替代散落的 19 处硬编码绝对路径
        self._cfg = getattr(engine, 'config', None)
        # P-A0/P-A2：fidelity 配置兜底注入（set_fidelity_config 优先；engine.config
        # 若未来携带 fidelity_config 字段则自动拾取，保持向后兼容）。
        if self._fidelity is None and self._cfg is not None:
            self._fidelity = getattr(self._cfg, 'fidelity_config', None)
        self._current_day_data = curr_data
        self._prev_day_data = prev_data
        self._code_index = None       # 每日 DataFrame 切换 → 失效索引缓存
        self._current_date = curr_date
        self._prev_date = prev_date
        self._prices = prices or {}
        # 清空当日查询缓存（PIT 语义：每天重新查，不跨日复用）
        self._query_cache = {}
        # Phase 4B：新交易日作废旧日分钟历史缓存（引擎随后重新注入当日数据）
        self._day_minute_history = None
        self._day_minute_date = None
        # PR4：分钟 Profile 的 current_bar_ts 每日重置（由 attach_bar 逐 bar 更新）
        self._current_bar_ts = None
        self._pct_chg_map = None
        # PR4：日级 curr_data（分钟 Profile 下供 _immediate_execute 的涨跌停判断用日级 pctChg）。
        # attach_day 注入当日日线 snapshot；attach_bar 不覆盖它（bar 快照存 _current_day_data）。
        self._daily_curr_data = curr_data
        if self._market is None and self._cfg is not None:
            from .providers.base import DataProviderRegistry
            registry = getattr(engine, '_providers', None) or DataProviderRegistry.from_duckdb(
                self._cfg.db_path)
            self._market = registry.market
            self._fundamental = registry.fundamental
            self._reference = registry.reference
            self._calendar = registry.calendar
        if self._market is not None:
            self._market.preload(prev_date, prev_date)
        if self._fundamental is not None:
            self._fundamental.preload(prev_date)
        if self._reference is not None:
            self._reference.preload()
        # A 通道（2026-09-26）：日切换后同步当日池给数据访问层（池状态随日回收 → 重新预填）
        self._sync_pool_to_data_access()

    def attach_bar(self, engine, bar_data: pd.DataFrame, curr_date: str, prev_date: str,
                   prices: dict = None, pct_chg_map: dict = None,
                   current_bar_ts=None):
        """PR4: 每 bar 一次注入引擎数据，跳过 preload（避免每 bar DB 查询）。

        【修正缺口 1】注入 current_bar_ts，get_history/get_price 的分钟查询锚定到此，
        当日窗口截断到 current_bar_ts（含，因 end-labeled 下当前 bar 已完成是可见历史），
        不含未来 bar。
        """
        self._engine = engine
        self._current_day_data = bar_data
        self._current_date = curr_date
        self._prev_date = prev_date
        self._code_index = None       # 每 bar DataFrame 切换 → 失效索引缓存
        self._prices = prices or {}
        # 每 bar 清空查询缓存（PIT 语义：每根 bar 重新查）
        self._query_cache = {}
        self._pct_chg_map = pct_chg_map   # 日级 pctChg（分钟涨跌停判断用）
        self._current_bar_ts = current_bar_ts   # 【修正缺口 1】

    def attach_day_minute_history(self, df: pd.DataFrame, day_str: str):
        """Phase 4B：注入当日全 universe 的全量分钟 bar（引擎 _run_minute_day 调用）。

        契约（写死，勿改）：
        - df 必须是 query_minute_bars_by_range_batch(..., fq=None) 的原始返回——
          OHLC 未做 fq 替换（front/back 列完整保留）。get_history 内存切片时
          按请求 fq 做替换（与 query_minute_bars_by_range 同一逻辑）；
        - time 列为 epoch 毫秒 int，与 bar_cutoff_ms 可直接比较；
        - 每日由引擎重新注入，前一日的缓存被替换，不跨日误用。
        """
        self._day_minute_history = df
        self._day_minute_date = day_str

    def get_signals(self) -> list:
        """兼容旧接口（即时执行模式下返回空列表）"""
        return []

    # -------- 设置函数 --------

    def set_benchmark(self, sids):
        """设置基准指数"""
        code = bare_code(sids)
        self._benchmark = code

    def set_limit_mode(self, mode):
        self._limit_mode = mode

    _POOL_MAX_CODES = 6000          # A2：池规模上限（R1 缓解，超限截断保确定性）

    def _offer_pool_codes(self, codes) -> None:
        """A2 第二来源（2026-09-26）：把「指数成分发现」型策略的成分集并入当日池。

        断板反包等策略不调 set_universe，而以 get_index_stocks 发现成分（全库 33 策略中仅
        4 个调 set_universe）⇒ 仅靠显式声明会使池为空、功能惰性。此处按 **并集** 并入：
        池 = set_universe 声明 ∪ get_index_stocks 返回值（超集作**预取范围**，未命中仍走
        按需路径，正确性不受影响）。池规模设上限（R1），超限截断并保持确定性顺序。
        """
        try:
            _raw = set(codes or [])
        except Exception:
            return
        if not _raw:
            return
        # 双形态入池（2026-09-26 实证修正）：池与请求的 code 形态可能不同
        # （策略常以 _bare 剥后缀后请求，而 get_index_stocks 返回带 .SS/.SZ 的形式）
        # ⇒ 同时收「原形 + 裸码形」，免疫形态差异；池仅为预取范围，多余项无副作用。
        _add = set(_raw)
        for _c in _raw:
            try:
                _add.add(str(_c).split('.')[0])
            except Exception:
                pass
        _cur = set(getattr(self, '_universe_pool', None) or [])
        _cur |= _add
        if len(_cur) > self._POOL_MAX_CODES:
            _cur = set(sorted(_cur)[:self._POOL_MAX_CODES])
        self._universe_pool = sorted(_cur)
        self._sync_pool_to_data_access()

    def _sync_pool_to_data_access(self) -> None:
        """A 通道（2026-09-26）：把当日池写入数据访问层（全池共享 bars 缓存）。

        运行时自适应定位 set_pool 宿主（market provider 自身或其内部数据访问对象），
        不硬编码内部属性名；句柄/池缺失即静默跳过 —— 数据访问层自然退化为按需填充
        同一缓存（零额外代码路径，不阻塞回测）。
        """
        pool = list(getattr(self, '_universe_pool', None) or [])
        if not pool:
            return
        market = getattr(self, '_market', None)
        cands = [market]
        if market is not None:
            try:
                cands.extend(vars(market).values())
            except Exception:
                pass
        for obj in cands:
            if obj is not None and hasattr(obj, 'set_pool'):
                try:
                    obj.set_pool(pool)
                except Exception:
                    pass
                return

    def set_universe(self, security_list):
        """A 通道（2026-09-26）：记录策略当日 PIT 池（原为 no-op：DuckDB 模式无需订阅）。

        本改动**不改变既有对外行为**（无返回值、无异常、不涉订阅语义），仅额外记录池；
        实际推送给数据访问层在 attach_day（该时点 provider 句柄已就绪）与本次调用时各同步一次。
        """
        try:
            self._universe_pool = list(security_list) if security_list else []
        except Exception:
            self._universe_pool = []
        self._sync_pool_to_data_access()

    def set_commission(self, **kwargs):
        """支持策略在 initialize 中动态调整回测成本。

        目前主要服务 ETF 策略：
        - commission_ratio / min_commission 生效
        - type='ETF' 时默认关闭印花税与过户费，贴近场内 ETF 交易口径
        """
        if self._engine is None or not hasattr(self._engine, 'cost') or self._engine.cost is None:
            return
        if 'commission_ratio' in kwargs and kwargs['commission_ratio'] is not None:
            self._engine.cost.commission_rate = float(kwargs['commission_ratio'])
        if 'min_commission' in kwargs and kwargs['min_commission'] is not None:
            self._engine.cost.min_commission = float(kwargs['min_commission'])
        sec_type = str(kwargs.get('type', '')).upper()
        if sec_type == 'ETF':
            self._engine.cost.stamp_tax_rate = 0.0
            self._engine.cost.transfer_fee_rate = 0.0

    def set_slippage(self, slippage=0.0):
        """Set proportional slippage with the real PTrade call signature.

        PTrade accepts ``set_slippage(slippage=...)`` or one positional value.
        Local-only aliases are rejected so local success predicts platform success.
        """
        if self._engine is None or not hasattr(self._engine, "cost"):
            return
        self._engine.cost.slippage_rate = max(0.0, float(slippage or 0.0))
        self._engine.cost.fixed_slippage = 0.0

    def set_fixed_slippage(self, fixedslippage=0.0):
        """Set fixed yuan-per-share slippage with the PTrade signature."""
        if self._engine is None or not hasattr(self._engine, "cost"):
            return
        self._engine.cost.fixed_slippage = max(0.0, float(fixedslippage or 0.0))
        self._engine.cost.slippage_rate = 0.0

    # -------- 数据查询函数 --------

    def get_index_stocks(self, index_code: str, date=None) -> list:
        """获取指数成分股（对应 Ptrade get_index_stocks），严格 PIT（F3）。

        - 显式 ``date``：标准化为 YYYY-MM-DD 后严格 as-of 查询；
        - 未显式传入：回测上下文注入当前回测日期（绝不使用数据库全局最新快照）；
          非回测直接调用由 Provider 保留"最新快照"兼容行为；
        - 返回标准 .SS/.SZ 成分股代码，去重且顺序确定。
        """
        bare = bare_code(index_code)
        # 日期契约：显式 date 标准化；未传时回测期间注入当前回测日期
        if date is not None:
            effective_date = str(date)[:10]
        elif self._current_date:
            effective_date = str(self._current_date)[:10]
        else:
            effective_date = None
        try:
            if self._reference is None:
                return []
            constituents = self._reference.get_index_constituents(bare, effective_date)
            # 双保险去重（Provider 已保证，这里保序去重防御旧 Provider 实现）
            seen, unique = set(), []
            for code in constituents:
                if code not in seen:
                    seen.add(code)
                    unique.append(code)
            # A2（2026-09-26）：池注入必须用**映射后**代码（与策略请求同形），
            # 否则池 code 与请求 code 不同形 ⇒ 命中率恒为 0（功能惰性）。
            _pool_codes = [self._to_ptrade_code(code) for code in unique]
            self._offer_pool_codes(_pool_codes)
            return _pool_codes
        except Exception as e:
            logger.warning(f"get_index_stocks({index_code}) 失败: {e}")
        return []

    # ---- get_fundamentals 辅助：各表的字段定义与数据来源 ----
    # valuation / 三大报表 / 5 张能力表 → 各表完整字段名（按 Ptrade 口径）
    # DuckDB 现状：valuation 完整可用（stock_float_share + stock_daily）；
    #   fin_indicator 表就绪（eps/bps/roe/pe_ttm/pb/ps_ttm/np_yoy，待 ETL 填充）；
    #   balance/income/cashflow 三大报表无表 → 返回带字段名的空 DataFrame（Ptrade 语义：无数据返回空）。
    _FUND_TABLES: Dict[str, List[str]] = {
        "valuation": ["code", "float_value", "a_floats", "total_value", "total_share",
                      "market_cap", "circulating_market_cap",
                      "pe_ratio", "pe_ratio_lyr", "pb_ratio", "ps_ratio", "pcf_ratio",
                      "turnover_ratio"],
        "balance_statement": ["code", "end_date", "publ_date", "total_assets", "total_liability",
                              "total_equity", "total_parent_equity", "minority_interest",
                              "total_current_assets", "total_non_current_assets",
                              "total_current_liability", "total_non_current_liability",
                              "cash_equivalents", "account_receivable", "account_payable",
                              "inventory", "notes_payable", "advance_payment",
                              "fixed_asset", "intangible_asset", "goodwill"],
        "income_statement": ["code", "end_date", "publ_date", "operating_revenue", "operating_cost",
                             "operating_profit", "total_profit", "net_profit",
                             "np_parent_company_owners", "minority_profit",
                             "total_operating_revenue", "total_operating_cost",
                             "operating_tax_surcharges", "sale_expense", "manage_expense",
                             "finance_expense", "rd_expense", "invest_income",
                             "non_operating_income", "non_operating_expense",
                             "income_tax", "basic_eps", "diluted_eps"],
        "cashflow_statement": ["code", "end_date", "publ_date",
                               "net_operate_cash_flow", "net_invest_cash_flow",
                               "net_finance_cash_flow", "cash_add_balance",
                               "end_cash_and_equiv", "sale_services",
                               "buy_services", "goods_sale_and_services",
                               "goods_buy_and_services",
                               "invest_long_asset", "invest_other",
                               "fixed_asset_depreciation", "intangible_asset_amortization",
                               "debt_to_assets", "debt_paying_cash", "dividend_interest_payment"],
        "eps": ["code", "end_date", "publ_date", "eps", "bps", "diluted_eps",
                "total_asset_share", "deducted_eps", "operating_eps"],
        "profit_ability": ["code", "end_date", "publ_date",
                           "roe", "roa", "roic", "net_profit_margin", "gross_profit_margin",
                           "operating_profit_margin", "total_profit_net_profit",
                           "expense_to_revenue", "operate_profit_to_profit",
                           "net_profit_to_balance", "roe_avg", "roa_avg"],
        "growth_ability": ["code", "end_date", "publ_date",
                           "np_yoy", "or_yoy", "equity_yoy", "netasset_yoy",
                           "net_profit_5y_cagr", "operating_revenue_5y_cagr",
                           "total_assets_yoy", "deducted_np_yoy"],
        "operating_ability": ["code", "end_date", "publ_date",
                              "accounts_receivable_turnover", "inventory_turnover",
                              "accounts_payable_turnover", "total_asset_turnover",
                              "current_asset_turnover", "fixed_asset_turnover",
                              "equity_turnover", "operating_cycle", "asset_turnover_days"],
        "debt_paying_ability": ["code", "end_date", "publ_date",
                                "current_ratio", "quick_ratio", "cash_ratio",
                                "debt_to_assets", "asset_to_liability",
                                "equity_multiplier", "long_debt_to_assets",
                                "long_debt_to_equity", "interest_protection_ratio",
                                "operating_cashflow_to_liability", "operating_cashflow_to_debt"],
    }

    def _execute_query(self, qb: QueryBuilder) -> pd.DataFrame:
        """执行 ORM query（query(valuation...).filter().order_by()）。

        从 stock_float_share + stock_daily 查 valuation 字段，按 QueryBuilder 的
        filter/order_by/limit 处理，返回 DataFrame（含 code 列，Ptrade 格式）。
        market_cap/circ_mv/total_mv 单位：亿元（stock_float_share 存元 → /1e8）。
        """
        try:
            if self._fundamental is None:
                return pd.DataFrame()
            qd = self._prev_date or self._current_date
            df = self._fundamental.get_valuation_query(
                [{'field': item.field, 'op': item.op, 'value': item.value} for item in qb._filters],
                [{'field': item.field, 'direction': item.direction} for item in qb._order_by],
                qb._limit, qd, qb.field_names)
            # code 转为 Ptrade 格式
            if "code" in df.columns:
                df["code"] = df["code"].apply(self._to_ptrade_code)
            return df.reset_index(drop=True)
        except Exception as e:
            logger.warning(f"_execute_query 失败: {e}")
            return pd.DataFrame()

    def get_fundamentals(self, security, table="valuation", fields=None, date=None,
                         start_year=None, end_year=None, report_types=None,
                         merge_type=None, is_dataframe=False, **kwargs) -> pd.DataFrame:
        """获取财务/估值数据（对应 Ptrade get_fundamentals）。

        支持 10 张表（Ptrade 口径）：
          valuation(估值) / balance_statement(资产负债) / income_statement(利润) /
          cashflow_statement(现金流) / eps(每股) / profit_ability(盈利) /
          growth_ability(成长) / operating_ability(营运) / debt_paying_ability(偿债)

        数据来源（DuckDB）：
          - valuation: stock_float_share + stock_daily（完整可用）
          - eps/profit/growth: fin_indicator（字段 eps/bps/roe/pe_ttm/pb/ps_ttm/np_yoy）
          - 三大报表/operating/debt_paying: 暂无源表 → 返回带字段名的空 DataFrame

        返回：DataFrame，索引为 Ptrade 格式代码。无数据时返回空 DataFrame（Ptrade 语义）。
        """
        # ---- 当日查询缓存（同一天内相同参数只查一次，大幅加速 score_stocks 等逐只查询场景）----
        # D4 修复（2026-08-28）：缓存键须含 start_year/end_year/report_types——
        # 否则不同过滤参数（如同比两期 vs 单期）互相污染命中早前空结果（F-Score 取数 bug）。
        cache_key = ("fund", tuple(sorted([str(s) for s in (security if isinstance(security, list) else [security])])),
                     str(table), str(fields), str(date), str(start_year), str(end_year), str(report_types))
        if hasattr(self, '_query_cache') and cache_key in self._query_cache:
            return self._query_cache[cache_key]

        # ---- ORM 模式：第一个参数是 QueryBuilder（query(valuation...).filter(...)）----
        if isinstance(security, QueryBuilder):
            return self._execute_query(security)

        sec_list = security if isinstance(security, list) else [security]
        bare_codes = [bare_code(s) for s in sec_list]
        table = str(table).strip()

        # 未提供表名 → 默认 valuation（向后兼容旧调用）
        if table not in self._FUND_TABLES:
            logger.warning(f"get_fundamentals 未知表名 '{table}'，回退 valuation")
            table = "valuation"

        try:
            if self._fundamental is None:
                return pd.DataFrame(columns=self._FUND_TABLES[table])
            # 确定查询日期：date > 回测上一交易日 > 当日
            # 注意：query_ms 用当天 23:59:59（+86400000-1），
            # 因为 stock_float_share 等表的 time 是当天收盘时刻(15:00)，不能用 00:00 查
            if date:
                qd = pd.Timestamp(date).strftime('%Y%m%d')
                query_ms = int(pd.Timestamp(qd, tz='Asia/Shanghai').timestamp() * 1000) + 86_399_999
            else:
                qd = self._prev_date or self._current_date
                query_ms = int(pd.Timestamp(qd, tz='Asia/Shanghai').timestamp() * 1000) + 86_399_999

            # B2 修复（2026-09-04，docs/valuation-date-pit-fix-design.md）：
            # valuation 的 date 分界。预加载快照锚点 = prev_date（T-1，实测定谳）。
            #   date 为空 / date >= T-1  -> 快照路径（逐位不变）
            #   date <  T-1             -> 真 as-of（query_valuation_daily_pit）
            # 归一化与既有 qd 同 canon（'YYYYMMDD'）后再比较，防格式差穿界；
            # date 解析失败维持既有 pd.Timestamp 抛异常行为，不新增 fail-soft。
            # 当日 _query_cache 于 attach/attach_bar 清空，故同日 force 判定稳定。
            _force_as_of = False
            if date:
                _prev_qd = (pd.Timestamp(self._prev_date).strftime('%Y%m%d')
                            if self._prev_date else None)
                _force_as_of = bool(_prev_qd and qd < _prev_qd)

            if table == "valuation":
                if _force_as_of:
                    df = self._fundamental.get_valuation(
                        bare_codes, qd, fields, force_as_of=True)
                else:
                    df = self._fundamental.get_valuation(bare_codes, qd, fields)
                    if len(df) == 0 and date:
                        df = self._fundamental.get_valuation(bare_codes, self._current_date, fields)
                if len(df) > 0:
                    df = df.copy()
                    df.index = [self._to_ptrade_code(code) for code in df.index]
            elif table in ("eps", "profit_ability", "growth_ability"):
                df = self._fundamental.get_financial(
                    bare_codes, table, qd, fields, start_year, end_year, report_types)
                if len(df) > 0:
                    df = df.copy()
                    df.index = [self._to_ptrade_code(code) for code in df.index]
            elif table in ("income_statement", "balance_statement", "cashflow_statement"):
                # D4 修复（2026-08-28，fundamentals-statement-wiring-design.md v2）：
                # 三大报表 PIT 接线（此前恒返回空表）——F-Score 类策略依赖。PIT 窗口全报告期行
                # （ann_date <= qd 23:59:59），策略层自取本期/去年同期（D3 同比口径）。
                df = getattr(self._fundamental, "get_" + table)(
                    bare_codes, qd, fields, start_year, end_year, report_types)
                if len(df) > 0:
                    df = df.copy()
                    df.index = [self._to_ptrade_code(code) for code in df.index]
            else:
                df = pd.DataFrame(columns=self._FUND_TABLES[table])

            # 字段筛选
            if fields:
                field_list = [fields] if isinstance(fields, str) else list(fields)
                available = [f for f in field_list if f in df.columns or f == 'code']
                if available:
                    df = df[available]

            # 写入当日缓存
            if hasattr(self, '_query_cache'):
                self._query_cache[cache_key] = df
            return df
        except Exception as e:
            logger.warning(f"get_fundamentals 失败: {e}")
            return pd.DataFrame(columns=self._FUND_TABLES.get(table, []))

    def _resolve_status_source(self, stocks, query_date=None):
        """Resolve stock-status data for an explicit PIT date.

        No query_date keeps the historical strategy behavior and uses the
        preloaded previous-trading-day snapshot.  An explicit current/previous
        date uses the matching in-memory daily snapshot; any other date is
        delegated to ReferenceDataProvider.  This makes reusable multi-day
        suspension/ST checks possible without direct database access.
        """
        query_key = str(query_date)[:10] if query_date is not None else None
        current_key = str(getattr(self, '_current_date', '') or '')[:10]
        previous_key = str(getattr(self, '_prev_date', '') or '')[:10]

        if query_key is None:
            return self._prev_day_data, None
        if query_key == current_key:
            current_daily = getattr(self, '_daily_curr_data', None)
            if current_daily is not None:
                return current_daily, None
            if self._current_day_data is not None:
                return self._current_day_data, None
        if query_key == previous_key and self._prev_day_data is not None:
            return self._prev_day_data, None
        if self._reference is not None:
            try:
                status = self._reference.get_stock_status(
                    [bare_code(stock) for stock in stocks], query_key)
                return None, status
            except Exception:
                return None, None
        return None, None

    def filter_stock_by_status(self, stocks: list, filter_type=None, query_date=None) -> list:
        """过滤 ST/停牌/退市（对应 Ptrade filter_stock_by_status，4 种 filter_type 全支持）。

        本地实现读 stock_daily 的 4 个新字段（在 aligner 数据层预计算）：
          - isST (旧字段，xtquant 不可靠，仅作 HALT 列保留)
          - is_st_reliable: 官方 ST/*ST（从 stock_namechange PIT 推导）
          - is_st_reliable_source: 'namechange' | 'none'
          - is_delisting_risk: 退市风险兜底（close<1 OR 近20日 circ_mv<5亿）
          - is_delisting_risk_source: 'price'|'market_cap'|'both'|'none'

        filter_type 语义（与 Ptrade 官方对齐，2026-08-22 实证修正）：
          - 'ST': is_st_reliable==True OR is_delisting_risk==True
                  （实证修正：真实 PTrade 平台 'ST' 仅官方 ST 标记——退市整理期仙股无 ST
                  标记会留池；本地为正确性扩展（退市风险兜底 close<1 OR circ_mv<5亿），
                  等价实现由转换侧 _QS_FILTER_STATUS_EXT 注入（P-D9，2026-08-22））
          - 'HALT': suspendFlag==1 OR volume==0（停牌）
          - 'DELISTING': 前一日无数据行（已退市）
          - 'DELISTING_SORTING': is_delisting_risk==True（退市整理期）

        默认 ['ST','HALT','DELISTING']（向后兼容，不含 DELISTING_SORTING）。
        """
        if not filter_type:
            filter_type = ["ST", "HALT", "DELISTING"]

        result = list(stocks)
        try:
            data, status = self._resolve_status_source(result, query_date)
            if data is not None:
                if len(data) == 0:
                    return result
                status = pd.DataFrame({
                    'code': data['code'],
                    'is_st': data.get('is_st_reliable', data.get('is_st', False)),
                    'is_halt': ((data.get('suspendFlag', 0) == 1) |
                                (data.get('volume', 0) == 0) |
                                data.get('is_halt', False)),
                    'is_delisting_risk': data.get('is_delisting_risk', False),
                    'is_delisting_risk_source': data.get('is_delisting_risk_source', 'none'),
                    'is_delisted': data.get('is_delisted', False),
                })
            elif status is None:
                return result
            if status is None or len(status) == 0:
                return result

            # P-A1 保真模式：fidelity_st_filter=True 时，退市风险兜底仅平台口径有效
            # （platform get_Ashares 与 filter 均无 circ_mv 分支；本地为对齐把扩展
            #  分支降到 price 语义：source in ('price','both') 才视为有效）。
            # 默认关闭 → 保持本地现状（circ_mv 扩展保留，本地正确语义锚）。
            fidelity = getattr(self, '_fidelity', None)
            strict_platform = bool(
                fidelity is not None and getattr(fidelity, 'fidelity_st_filter', False))

            # 纯性能优化：将全市场 status 预构建为 {code: 字段字典}（一次性向量化），
            # 循环内改为 O(1) 查找，消除原 status[status['code']==bare] 的 O(N²) 布尔扫描。
            # 不可用 status.groupby('code') + g.iloc[0]：逐组 iloc 触发巨量 pandas 行抽取开销
            # （6634 组×76 天≈50万次 iloc → 反而比原 O(N²) 布尔扫描更慢）。
            # set_index('code').to_dict('index') 为单趟 C 级操作；每个 code 在 status 中唯一
            # （日线快照一行一码 / get_stock_status 亦一行一码），无需 iloc[0] 取首行语义。
            # 下游 r.get(...) 对 dict / Series 均等价；'code' 缺失时回退为空字典（等价原空匹配）。
            if 'code' in status.columns:
                status_by_code = status.set_index('code').to_dict('index')
            else:
                status_by_code = {}

            filtered = []
            for stock in result:
                bare = bare_code(stock)
                row = status_by_code.get(bare)
                if row is None:
                    if "DELISTING" in filter_type:
                        continue
                    filtered.append(stock)
                    continue

                r = row
                if "DELISTING" in filter_type and bool(r.get('is_delisted', False)): continue
                if "HALT" in filter_type and bool(r.get('is_halt', False)): continue
                if "ST" in filter_type:
                    st_flag = bool(r.get('is_st', False))
                    dr_flag = bool(r.get('is_delisting_risk', False))
                    if strict_platform and dr_flag:
                        # P-A1：仅 price/both 有效（平台口径无 circ_mv 分支）
                        dr_flag = r.get('is_delisting_risk_source', 'none') in ('price', 'both')
                    if st_flag or dr_flag:
                        continue
                if "DELISTING_SORTING" in filter_type and bool(
                        r.get('is_delisting_risk', False)): continue
                filtered.append(stock)

            return filtered
        except Exception:
            return result

    def check_limit(self, security, query_date=None) -> dict:
        """检查涨跌停（对应 Ptrade check_limit）
        使用 shared_ashare_rules 精确涨跌停规则（主板10%/创业板20%/科创板20%/北交所30%/ST5%）
        返回 {code: 1(涨停)/-1(跌停)/0(正常)}"""
        from .libs.shared_ashare_rules import get_price_limit_pct
        result = CodeDict()
        securities = [security] if isinstance(security, str) else security
        data = self._current_day_data
        if data is not None:
            status = data.copy()
        elif self._reference is not None and (query_date or self._current_date):
            status = self._reference.get_stock_status(
                [bare_code(sec) for sec in securities], query_date or self._current_date)
        else:
            return result
        for sec in securities:
            bare = bare_code(sec)
            qmt_code = normalize_to_qmt(bare)
            ptrade_code = self._to_ptrade_code(bare)
            row = status[status['code'] == bare] if 'code' in status.columns else pd.DataFrame()
            if len(row) > 0:
                r = row.iloc[0]
                close = r.get('close', 0)
                preclose = r.get('preClose', 0)
                if preclose and preclose > 0:
                    pct = (close - preclose) / preclose
                    limit_pct = get_price_limit_pct(qmt_code)
                    if pct >= limit_pct - 0.002:  # 容差0.2%
                        result[ptrade_code] = 1  # 涨停
                    elif pct <= -(limit_pct - 0.002):
                        result[ptrade_code] = -1  # 跌停
                    else:
                        result[ptrade_code] = 0
                else:
                    result[ptrade_code] = 0
            else:
                result[ptrade_code] = 0
        return result

    def get_positions(self, security=None) -> dict:
        """获取持仓（对应 Ptrade get_positions）
        Ptrade 语义：两位/四位尾缀皆可作键取值。

        2026-09-04 持仓视图契约修复：传入当日价字典，使 last_sale_price 实际命中
        （此前不传价 → 一律回落 avg_cost，属契约违背）。take-price 命中/回退两态见
        tests/test_position_view_engine_contract.py。"""
        positions = self._engine._get_ptrade_positions(getattr(self, "_prices", None) or {})
        if security:
            pos = self._lookup_position(positions, security)
            return {security: pos} if pos is not None else {}
        return positions

    # -------- 交易函数（即时执行模式）--------

    def _route_next_open(self, security, instruction, target_value=None, shares=None):
        """G1-I: next_open 模式订单路由（设计 v2 §3.6）。

        - basket_active 且在 handle_data 内（_current_basket 非 None）→ order_in_basket
          （before_trading_start / run_daily 调用时 _current_basket=None → 走 legacy pending）
        - 否则 → _create_pending_order（legacy 单订单队列，§12 行为不变）
        """
        eng = self._engine
        if getattr(eng, 'basket_active', False) and eng._current_basket is not None:
            return eng.order_in_basket(security, instruction,
                                       target_value=target_value, shares=shares)
        return eng._create_pending_order(security, instruction,
                                         target_value=target_value, shares=shares)

    def order_target_value(self, security: str, value: float, limit_price=None):
        """目标市值调仓（对应 Ptrade order_target_value）
        即时执行：调用后 Account 立即更新，策略下一行可见最新状态。
        value=0 全卖；value>0 调仓到目标市值。
        A3: 返回 Order 对象，策略可检查 order.status 感知失败（涨跌停阻断/资金不足）。

        PR2: next_open 模式下换算前分流到 _create_pending_order（T 日只入队 + 预扣，
        T+1 drain 成交）。close/open 路径逐行不变。
        G1-I: basket_active 且 handle_data 内 → order_in_basket。"""
        if self._engine.match_price_mode == "next_open":
            return self._route_next_open(security, "target_value", target_value=value)
        order = self._engine._immediate_execute(
            security, target_value=value, prices=self._prices,
            date=self._current_date, curr_data=self._curr_data_for_execute())
        # 成交后原地刷新 portfolio（策略持有的 context 引用不变）
        self._engine.refresh_portfolio(self._prices)
        return order

    def order(self, security: str, amount: int, limit_price=None):
        """按股数下单（即时执行）
        amount>0 买入，amount<0 卖出。
        A3: 返回 Order 对象。

        PR2: next_open 模式下换算前分流到 _create_pending_order。close/open 不变。
        G1-I: basket_active 且 handle_data 内 → order_in_basket。"""
        if self._engine.match_price_mode == "next_open":
            instr = "buy_shares" if amount >= 0 else "sell_shares"
            return self._route_next_open(security, instr, shares=abs(amount))
        order = self._engine._immediate_execute(
            security, shares=amount, prices=self._prices,
            date=self._current_date, curr_data=self._curr_data_for_execute())
        self._engine.refresh_portfolio(self._prices)
        return order

    # -------- 扩展交易函数（P1 新增）--------

    def order_at_price(self, security: str, amount: int, execution_price: float):
        """QuantStudio-only explicit-price execution for completed-bar research.

        The caller must use a price already visible at its decision clock. This
        supports causal daily strategies that enter through next-open pending
        orders but exit at the completed current-day close.
        """
        if self._engine is None:
            return None
        price = float(execution_price or 0.0)
        if price <= 0:
            return None
        qmt_code = self._bare_to_qmt(bare_code(security))
        order_result = self._engine._immediate_execute(
            security, shares=int(amount), prices={qmt_code: price},
            date=self._current_date, curr_data=self._curr_data_for_execute())
        self._engine.refresh_portfolio({qmt_code: price})
        return order_result

    def order_value(self, security: str, value: float, limit_price=None):
        """按金额买入/卖出（对应 Ptrade order_value，增量操作）
        value>0 买入指定金额（增量加仓），value<0 卖出指定金额（增量减仓）。
        注意：order_value 是增量，order_target_value 才是调仓到目标市值（绝对）。
        A3: 返回 Order 对象。

        PR2: next_open 模式下换算前分流到 _create_pending_order（存原始 value，
        T+1 drain 时用 T+1 open 重新换算股数，避免 T 日价换算的穿越）。close/open 不变。
        G1-I: basket_active 且 handle_data 内 → order_in_basket。"""
        if self._engine.match_price_mode == "next_open":
            if value == 0:
                return self._make_noop_order(security)
            instr = "buy_value" if value > 0 else "sell_value"
            return self._route_next_open(security, instr, target_value=abs(value))
        bare = bare_code(security)
        price = self._get_current_price(bare)
        last_order = None
        if price <= 0:
            from .backtest_engine import Order
            return Order(order_id=f"ord_{security}_{self._current_date}", security=security,
                         direction="unknown", status="rejected", reason="no_price",
                         created_dt=self._current_date)
        if value > 0:
            # 买入指定金额（增量）：按金额/价格算股数，向下取整到100股整数倍
            buy_shares = int(value / price)
            if buy_shares > 0:
                last_order = self._engine._immediate_execute(
                    security, shares=buy_shares, prices=self._prices,
                    date=self._current_date, curr_data=self._curr_data_for_execute())
        elif value < 0:
            # 卖出指定金额（增量）
            sell_shares = int(abs(value) / price)
            if sell_shares > 0:
                last_order = self._engine._immediate_execute(
                    security, shares=-sell_shares, prices=self._prices,
                    date=self._current_date, curr_data=self._curr_data_for_execute())
        self._engine.refresh_portfolio(self._prices)
        return last_order if last_order is not None else self._make_noop_order(security)

    def order_target(self, security: str, target_amount: int, limit_price=None):
        """调仓到目标股数（对应 Ptrade order_target）。
        A3: 返回 Order 对象。

        PR2: next_open 模式下换算前分流到 _create_pending_order（存原始 target_amount，
        T+1 drain 时用 T+1 持仓重新算 delta）。close/open 不变。
        G1-I: basket_active 且 handle_data 内 → order_in_basket。"""
        if self._engine.match_price_mode == "next_open":
            return self._route_next_open(security, "target_shares", shares=target_amount)
        bare = bare_code(security)
        qmt_code = self._bare_to_qmt(bare)
        pos = self._engine.account.positions.get(qmt_code)
        current = pos.volume if pos else 0
        delta = target_amount - current
        if delta != 0:
            order = self._engine._immediate_execute(
                security, shares=delta, prices=self._prices,
                date=self._current_date, curr_data=self._curr_data_for_execute())
            self._engine.refresh_portfolio(self._prices)
            return order
        return self._make_noop_order(security)

    # -------- 数据查询函数（P1 新增）--------

    def _get_proxy_intraday_history(self, sec_list, count, fields, is_dict, field_map):
        """Return only completed synthetic bars for daily-open-close-proxy-v1."""
        bars = getattr(self._engine, '_proxy_intraday_bars', []) if self._engine else []
        dfs = {}
        for security in sec_list:
            bare = bare_code(security)
            parts = []
            for snapshot in bars:
                if snapshot is None or len(snapshot) == 0 or 'code' not in snapshot.columns:
                    continue
                selected = snapshot[snapshot['code'].astype(str) == str(bare)]
                if len(selected) > 0:
                    parts.append(selected.copy())
            if not parts:
                continue
            frame = pd.concat(parts, ignore_index=True)
            if 'time' in frame.columns:
                frame = frame.sort_values('time')
            frame = frame.tail(int(count)).copy()
            frame.index = range(-len(frame), 0)
            if fields:
                mapped = [field_map.get(value, value) for value in fields]
                available = [value for value in mapped if value in frame.columns]
                if available:
                    frame = frame[available]
            dfs[self._to_ptrade_code(bare)] = frame
        if is_dict:
            return CodeDict(dfs)
        if not dfs:
            return pd.DataFrame()
        if len(dfs) == 1:
            return next(iter(dfs.values()))
        return pd.concat(dfs.values(), ignore_index=False)

    def get_history(self, security=None, count=None, unit='1d', fields=None,
                    frequency=None, field=None, security_list=None,
                    fq='pre', include=False, fill='nan', is_dict=False) -> pd.DataFrame:
        """获取历史数据（对应 Ptrade get_history），兼容两种签名：

        签名 A（security-first，向后兼容）：
            get_history(security, count, unit='1d', fields=None)
        签名 B（count-first，Ptrade 官方）：
            get_history(count, frequency='1d', field='close', security_list=None, ...)

        识别规则：第一个位置参数为 int → count-first 模式。
        字段映射：money→amount, price→close, factor→pctChg。
        is_dict=True 时返回 {code: DataFrame}。"""
        # ---- 签名识别：security 为 int → count-first 模式 ----
        if isinstance(security, int):
            # Ptrade 官方签名: get_history(count, frequency='1d', field='close', security_list=None, ...)
            # 位置参数映射：security(实参)=count, count(实参)=frequency, unit(实参)=field, fields(实参)=security_list
            _count = security
            _freq = count  # 第2个位置参数实际是 frequency
            _field = unit   # 第3个位置参数实际是 field
            _sec_list = fields  # 第4个位置参数实际是 security_list
            count = _count
            unit = _freq or frequency or '1d'   # PR4: count-first 模式下也接受 frequency 关键字
            if field is not None:
                fields = field
            elif _field and _field != '1d':
                fields = _field
            security = security_list if security_list is not None else _sec_list
        # security_list 优先（Ptrade 官方参数名）
        if security is None:
            security = security_list
        if security is None:
            return pd.DataFrame()
        if count is None:
            count = 20
        # PR4: frequency 关键字作为 unit 的别名（兼容 get_history(60, frequency='1m', ...) 调用）
        if frequency and unit == '1d':
            unit = frequency

        sec_list = [security] if isinstance(security, str) else list(security)

        # ---- 当日查询缓存（同一天内相同参数只查一次）----
        cache_key = ("hist", tuple(sorted([str(s) for s in sec_list])),
                     int(count), str(unit), str(fields), str(fq),
                     bool(is_dict), bool(include))
        if hasattr(self, '_query_cache') and cache_key in self._query_cache:
            return self._query_cache[cache_key]

        # Ptrade 字段 → DuckDB 列名映射
        field_map = {'money': 'amount', 'amount': 'amount', 'price': 'close',
                     'factor': 'pctChg', 'pctChg': 'pctChg', 'preclose': 'preClose',
                     'is_open': 'volume'}
        if isinstance(fields, str):
            fields = [fields]

        if (include and unit != '1d' and self._engine is not None
                and getattr(self._engine, 'engine_profile', None) == 'daily-open-close-proxy-v1'):
            result = self._get_proxy_intraday_history(
                sec_list, count, fields, is_dict, field_map)
            if hasattr(self, '_query_cache'):
                self._query_cache[cache_key] = result
            return result

        try:
            if self._market is None:
                return {} if is_dict else pd.DataFrame()
            dfs = {}
            bare_codes = [bare_code(sec) for sec in sec_list]
            # 分钟 include 语义修正（2026-08-13，PTrade 分钟实测确认）：
            # - 分钟 include=False 锚定当天 + 排除当前 bar（前一 bar 及之前，PIT 正确语义）
            # - 分钟 include=True 锚定当天 + 含当前 bar（不变）
            # - 日线 include 不变（False=prev_date 前一交易日, True=current_date）
            # bar 时间戳为 end-labeled（bar 结束时刻）→ 减 interval 即前一 bar 结束时刻。
            cur_bar_ts = getattr(self, '_current_bar_ts', None)
            if unit != '1d' and cur_bar_ts is not None:
                anchor_date = (self._current_date or self._prev_date)
                bar_cutoff_ms = int(pd.Timestamp(cur_bar_ts).value // 10**6)
                if not include:
                    _bar_interval_ms = {
                        '1m': 60000, '5m': 300000, '15m': 900000,
                        '30m': 1800000, '60m': 3600000,
                    }.get(unit, 60000)
                    bar_cutoff_ms -= _bar_interval_ms
            else:
                # 日线模式（或分钟无 bar 锚点）：保持原逻辑
                # PTrade include semantics: include=False stops at previous_date;
                # include=True extends the window through current_date.
                anchor_date = ((self._current_date or self._prev_date) if include
                               else (self._prev_date or self._current_date))
                bar_cutoff_ms = None
            # ---- Phase 4B：当日分钟历史内存切片（零 DB 往返）----
            # 命中条件：分钟路径 + 已有当前 bar 锚点（bar_cutoff_ms 非 None，防未来泄漏）
            # + 引擎已注入当日缓存 + 缓存日期与 anchor_date（include=True=当日）匹配。
            # 未命中（含 include=False 锚定前一日、日线 Profile）→ fallback SQL 路径。
            # 命中但请求 code 不在缓存（当日无日线/停牌等）→ 对缺失 code 补查 SQL，
            # 保持与无缓存路径完全一致的异常语义与数据完整性（铁律：不改变空值/异常行为）。
            mem_sliced = False
            if (unit != '1d' and bar_cutoff_ms is not None
                    and self._day_minute_history is not None
                    and self._day_minute_date == str(anchor_date)[:10]):
                mem = self._day_minute_history
                sliced = mem[(mem['code'].isin(bare_codes))
                             & (mem['time'] <= bar_cutoff_ms)].copy()
                found = set(sliced['code'].unique()) if len(sliced) > 0 else set()
                missing = [c for c in bare_codes if c not in found]
                if len(sliced) > 0:
                    # fq 替换（与 query_minute_bars_by_range 一致；缓存为 fq=None 原始值）
                    fq_norm = str(fq).lower() if fq else ""
                    if fq_norm in ('pre', 'dypre'):
                        for orig, qfq in [("open", "open_front"), ("high", "high_front"),
                                          ("low", "low_front"), ("close", "close_front")]:
                            if qfq in sliced.columns and sliced[qfq].notna().any():
                                sliced[orig] = sliced[qfq]
                    elif fq_norm in ('post', 'dyback', 'dy_post'):
                        for orig, qfq in [("open", "open_back"), ("high", "high_back"),
                                          ("low", "low_back"), ("close", "close_back")]:
                            if qfq in sliced.columns and sliced[qfq].notna().any():
                                sliced[orig] = sliced[qfq]
                    for bare in bare_codes:
                        sub = sliced[sliced['code'] == bare].tail(count)
                        if not sub.empty:
                            sub = sub.copy()
                            sub.index = range(-len(sub), 0)
                            sub = _ensure_money_alias(sub)
                            dfs[self._to_ptrade_code(bare)] = sub
                if missing:
                    logger.debug(
                        f"[4B] 内存缓存缺失 code={missing} date={self._day_minute_date}，"
                        f"补查 SQL（保持与无缓存路径一致）")
                    for bare, df in self._market.get_bars_by_count(
                            missing, count, anchor_date, None, fq, frequency=unit,
                            bar_cutoff_ms=bar_cutoff_ms).items():
                        df = df.copy()
                        df.index = range(-len(df), 0)
                        df = _ensure_money_alias(df)
                        dfs[self._to_ptrade_code(bare)] = df
                mem_sliced = True
            if not mem_sliced:
                for bare, df in self._market.get_bars_by_count(
                        bare_codes, count, anchor_date, None, fq, frequency=unit,
                        bar_cutoff_ms=bar_cutoff_ms).items():
                    df = df.copy()
                    df.index = range(-len(df), 0)
                    df = _ensure_money_alias(df)
                    dfs[self._to_ptrade_code(bare)] = df
            if is_dict:
                _result = CodeDict(dfs)
                if hasattr(self, '_query_cache'): self._query_cache[cache_key] = _result
                return _result
            if not dfs:
                return pd.DataFrame()
            if len(dfs) == 1:
                df0 = list(dfs.values())[0]
            else:
                # 多股票且 is_dict=False：纵向拼接，保留 code 列区分（Ptrade 多股票返回含证券代码列）
                df0 = pd.concat(dfs.values(), ignore_index=False)
            # 字段筛选（含 Ptrade 字段名映射）
            if fields:
                mapped = [field_map.get(f, f) for f in fields]
                available = [m for m in mapped if m in df0.columns]
                if available:
                    df0 = df0[list(dict.fromkeys(available))]
                    # B1：唯一闸门——选列后含 'amount' 即补同值 'money'（含请求 fields=['money'] 场景）
                    df0 = _ensure_money_alias(df0)
            if hasattr(self, '_query_cache'): self._query_cache[cache_key] = df0
            # P-D13 C3a：窗口审计行（count≥100 才输出——vol_regime q 分位差定位器，
            # log.info 级直接可见：QS_HISTORY_WINDOW code count actual first last）
            try:
                _req_count = int(count or 0)
                if _req_count >= 100 and hasattr(df0, '__len__') and len(df0) > 0:
                    _first = str(df0.index[0])[:10] if len(df0.index) > 0 else '?'
                    _last = str(df0.index[-1])[:10] if len(df0.index) > 0 else '?'
                    _codes = [str(s) for s in sec_list[:3]]
                    logger.info("QS_HISTORY_WINDOW codes=%s count=%d actual=%d "
                                "first=%s last=%s",
                                _codes, _req_count, len(df0), _first, _last)
            except Exception:
                pass
            return df0
        except FrequencyCapabilityError:
            # PR3: 能力错误必须上抛，不静默吞成空 DataFrame（否则策略会把"无分钟数据"当"无信号"，
            # 违反主计划 7.19 "严禁频率缺失时回退到日线；数据缺失返回结构化能力错误"）。
            raise
        except Exception as e:
            # F-LOCAL-MIN（B2 辅修复，2026-09-05）：静默消音——原 logger.debug 默认不可见，
            # 「分钟链 TypeError」被吞成空返回（B1 定谳，docs/evidence/f-local-min-b1-verdict-20260905.md）。
            # 改为限频 warning：每类异常文本首条全量告警 + 计数聚合；返回契约不变（仍空返回）。
            _err_key = type(e).__name__ + ": " + str(e)[:120]
            _n = getattr(self, "_qs_hist_fail_counts", None)
            if _n is None:
                _n = self._qs_hist_fail_counts = {}
            _c = _n.get(_err_key, 0) + 1
            _n[_err_key] = _c
            if _c == 1:
                logger.warning("QS_HIST_FAIL %s | args: count=%s unit=%s fields=%s secs=%d",
                               _err_key, count, unit, fields, len(sec_list))
            else:
                logger.debug("QS_HIST_FAIL(occurrence #%d) %s", _c, _err_key)
            return {} if is_dict else pd.DataFrame()

    def attribute_history(self, security, count, unit='1d', fields=None) -> pd.Series:
        """获取历史数据（对应 Ptrade attribute_history，返回单列 Series）"""
        df = self.get_history(security, count, unit, fields)
        if len(df) == 0:
            return pd.Series(dtype=float)
        # 取最后一行（最近一日）
        return df.iloc[-1]

    def current_price(self, security) -> float:
        """获取当前价（对应 Ptrade current_price）"""
        bare = bare_code(security)
        return self._get_current_price(bare)

    def get_current_data(self) -> dict:
        """返回当日全部行情数据（对应 Ptrade get_current_data）"""
        data = {}
        if self._current_day_data is not None:
            for _, row in self._current_day_data.iterrows():
                bare = str(row['code'])
                ptrade_code = self._to_ptrade_code(bare)
                from .ptrade_api import BarData
                data[ptrade_code] = BarData(row, self._current_date)
        return data

    # -------- 辅助 --------

    def _get_current_price(self, bare_code: str) -> float:
        """从当日数据获取最新价"""
        if self._current_day_data is not None:
            if self._code_index is None:
                self._code_index = _build_code_index(self._current_day_data)
            if self._code_index is not None:
                i = self._code_index.get(bare_code)
                if i is not None:
                    return float(self._current_day_data.iloc[i].get('close', 0))
            else:
                # 索引不可用 → 回退原布尔过滤（保留缺 'code' 列时的 KeyError 原行为）
                row = self._current_day_data[self._current_day_data['code'] == bare_code]
                if len(row) > 0:
                    return float(row.iloc[0].get('close', 0))
        # fallback to prices dict
        qmt = self._bare_to_qmt(bare_code)
        return self._prices.get(qmt, 0)

    def _curr_data_for_execute(self):
        """PR4: _immediate_execute 的 curr_data 来源。

        分钟 Profile：用 _daily_curr_data（当日日线 snapshot），涨跌停判断用日级 pctChg
        （分钟 bar 的 preClose 不复权，除权日风险）。
        日线 Profile：用 _current_day_data（当日日线，与现有行为一致）。
        """
        if (self._engine is not None
                and getattr(self._engine, 'engine_profile', None) == 'daily-open-close-proxy-v1'
                and self._current_bar_ts is not None):
            return self._current_day_data
        daily = getattr(self, '_daily_curr_data', None)
        if daily is not None:
            return daily
        return self._current_day_data

    def _make_noop_order(self, security: str):
        """构造无操作 Order（order_target delta=0 等）— A3。
        老策略 if order: 会判定为 False（filled=0），兼容。"""
        from .backtest_engine import Order
        return Order(order_id=f"noop_{security}_{self._current_date}", security=security,
                     direction="noop", status="rejected", reason="no_operation_needed",
                     created_dt=self._current_date)

    @staticmethod
    def _bare_to_qmt(bare: str) -> str:
        return normalize_to_qmt(bare)

    @staticmethod
    def _to_ptrade_code(bare_code: str) -> str:
        """Return PTrade strategy/CSV codes via the authoritative normalizer."""
        return normalize_to_ptrade(bare_code)

    def get_price(self, security, start_date=None, end_date=None, frequency='1d',
                  fields=None, fq='pre', count=None, is_dict=False):
        """获取历史行情（对应 Ptrade get_price）
        security: 单个代码或列表
        start_date/end_date: 'YYYY-MM-DD'
        frequency: '1d'/'1m'/'5m' 等
        fields: ['open','high','low','close','volume','amount'] 等
        fq: 'pre'(前复权，默认)/'post'(后复权)/None(不复权)
        count: 获取数量（与 end_date 配合）
        is_dict: True 返回 dict[code→DataFrame]，False 返回 DataFrame"""
        try:
            if self._market is None:
                return {} if is_dict else pd.DataFrame()

            securities = [security] if isinstance(security, str) else security
            dfs = {}
            bare_codes = [bare_code(sec) for sec in securities]
            # PR4 缺口 1：分钟 Profile 下锚定到 _current_bar_ts（防未来 bar 泄漏）。
            bar_cutoff_ms = None
            cur_bar_ts = getattr(self, '_current_bar_ts', None)
            if cur_bar_ts is not None and frequency != '1d':
                bar_cutoff_ms = int(pd.Timestamp(cur_bar_ts).value // 10**6)
            if count and end_date:
                provider_result = self._market.get_bars_by_count(
                    bare_codes, count, pd.Timestamp(end_date).strftime('%Y-%m-%d'), None, fq,
                    frequency=frequency, bar_cutoff_ms=bar_cutoff_ms)
            else:
                provider_result = self._market.get_bars(
                    bare_codes, pd.Timestamp(start_date or '1900-01-01').strftime('%Y-%m-%d'),
                    pd.Timestamp(end_date or self._prev_date or self._current_date).strftime('%Y-%m-%d'),
                    None, fq, frequency=frequency, bar_cutoff_ms=bar_cutoff_ms)
            for bare, df in provider_result.items():
                if len(df) > 0:
                    df['trade_date'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')
                    # 复权处理
                    if fq == 'pre' and 'close_front' not in df.columns:
                        pass  # stock_daily 已有 front 字段（当前查询未含，简化处理）
                    if fields:
                        # B1：canonical 'money' 映射到 DB 物理列 'amount'（选列用），返回端再补 'money' 别名
                        mapped_fields = ['amount' if f == 'money' else f for f in fields]
                        available = [f for f in mapped_fields if f in df.columns]
                        if available:
                            df = df[available]
                    # B1：唯一闸门——含 'amount' 即补同值 'money'（选列后统一处理）
                    df = _ensure_money_alias(df)
                    dfs[self._to_ptrade_code(bare)] = df
            if is_dict:
                return CodeDict(dfs)
            if len(dfs) == 1:
                return list(dfs.values())[0]
            return CodeDict(dfs)
        except FrequencyCapabilityError:
            # PR3: 能力错误必须上抛（同 get_history），不静默吞成空 DataFrame。
            raise
        except Exception as e:
            logger.debug(f"get_price 失败: {e}")
            return {} if is_dict else pd.DataFrame()

    # ===================== B1 批量取数 API（性能优化，策略可选）=====================
    # 设计原则：
    #   1. 只新增，不改动 get_fundamentals/get_history 的签名和返回格式（向后兼容）
    #   2. 内部复用 _preload_market_data / _preload_float（避免重复预加载）
    #   3. 语义与 get_fundamentals/get_history 一致，仅强制 list 入参、明确批量意图
    #   4. 消除策略层逐只调用 get_fundamentals/get_history 的 N+1 模式

    def get_fundamentals_batch(self, security_list, table='valuation',
                               fields=None, date=None) -> pd.DataFrame:
        """B1：批量查询多只股票基本面。返回 DataFrame，index=ptrade_code。

        与 get_fundamentals(security_list, ...) 的区别：
        - 强制 list 入参，语义明确（杜绝策略层逐只循环）
        - 始终走预加载内存路径（_preload_float），单次过滤，无 N 次 DuckDB 往返
        - 返回格式与 get_fundamentals 完全一致（index=code，columns=fields）

        典型用法（替代 小市值策略2.py score_stocks 的逐只循环）：
            df = get_fundamentals_batch(stock_pool, 'valuation',
                                        fields=['float_value','pe_ttm'],
                                        date=context.previous_date)
            # df.index 是 ptrade_code，df['float_value'] 直接是各股票流通市值

        空列表 / 无数据 → 返回空 DataFrame（不报错，Ptrade 语义）。
        """
        if not security_list:
            return pd.DataFrame(columns=[fields] if isinstance(fields, str) else (fields or []))
        # 直接复用 get_fundamentals（它已支持 list + 预加载路径）
        # 这里的价值是：(1) 明确批量意图 (2) 强制 list (3) 固化测试
        return self.get_fundamentals(security_list, table=table, fields=fields, date=date)

    def get_history_batch(self, security_list, count, unit='1d',
                          fields=None, fq='pre', include=False) -> 'CodeDict':
        """B1：批量查询多只股票历史行情。返回 CodeDict（{ptrade_code: DataFrame}）。

        与 get_history(security_list, ...) 的区别：
        - 强制 list 入参 + 强制 is_dict 语义（返回 {code: df}，便于逐只访问）
        - 复用 get_history（走 get_bars_by_count 活跃路径），不读取 _preload_daily 全市场缓存；
          日线路径已批量化（单条 SQL：code IN(...) + ROW_NUMBER PARTITION BY code，O(N)→O(1)，
          duckdb_data_access.py:425-520）；分钟路径仍逐只循环（duckdb_provider.py:90-95）。
          注意：当日查询缓存（ptrade_api.py:1122-1127）的 cache_key 含完整 sec_list tuple，
          动态池每日变化时缓存不命中，但日线路径仅 1 条 SQL/天，不构成瓶颈。
        - count/unit/fields/fq/include 语义与 get_history 一致

        典型用法（替代逐只 get_history 算动量/反转因子）：
            hist = get_history_batch(stock_pool, 21, '1d', fields=['close'], fq='pre')
            for code, df in hist.items():
                ret_20d = df['close'].iloc[-1] / df['close'].iloc[0] - 1

        空列表 → 返回空 CodeDict。单只 → 返回只含一个 key 的 CodeDict。
        """
        if not security_list:
            return CodeDict({})
        # 复用 get_history（已支持 list + 预加载），强制 is_dict=True 返回 dict 形式
        result = self.get_history(security=security_list, count=count, unit=unit,
                                  fields=fields, fq=fq, include=include, is_dict=True)
        if isinstance(result, CodeDict):
            return result
        if isinstance(result, dict):
            return CodeDict(result)
        # get_history 可能返回单 DataFrame（单只时），包装成 CodeDict
        if isinstance(result, pd.DataFrame) and len(security_list) == 1:
            return CodeDict({self._to_ptrade_code(bare_code(security_list[0])): result})
        return CodeDict({})

    def get_trading_day(self, day=0):
        """获取交易日（day>0 未来N天，day<0 过去N天，day=0 当天）
        返回 datetime.date 对象（Ptrade 原生行为）：支持 .strftime() 和日期减法 .days"""
        try:
            if self._calendar is None or not self._current_date:
                return self._current_date
            return self._calendar.get_trading_day(self._current_date, day)
        except Exception:
            return self._current_date

    def get_trade_days(self, start_date=None, end_date=None, count=None):
        """获取交易日列表，返回 ndarray"""
        try:
            if self._calendar is None:
                return np.array([])
            dates = self._calendar.get_trade_days(start_date or '1900-01-01',
                                                  end_date or self._current_date)
            if count:
                dates = dates[-count:]
            return np.array([date.strftime('%Y-%m-%d') for date in dates])
        except Exception:
            return np.array([])

    def get_all_trades_days(self, date=None):
        """获取全部交易日，返回 ndarray"""
        return self.get_trade_days()

    def get_trading_day_by_date(self, query_date, day=0):
        """根据日期获取对应的交易日"""
        if day == 0:
            return str(query_date)[:10]
        return self.get_trading_day(day)

    def run_daily(self, context, func, time='9:31'):
        """定时任务（回测模式中等效于在 handle_data 中调用）
        在日线回测中，run_daily 注册的 func 每日由引擎执行（无 handle_data 时自动触发）。
        注意：不在 initialize 时立即执行（此时 context 尚不完整），仅注册。"""
        if not hasattr(self, '_daily_tasks'):
            self._daily_tasks = []
        self._daily_tasks.append((func, time))

    def get_position(self, security):
        """获取单只标的持仓（对应 Ptrade get_position）

        Ptrade 语义：两位/四位尾缀皆可作键取值；空仓返回 amount=0 的 Position（非 None），
        策略常写 `get_position(code).amount == 0` 判断空仓，依赖此行为。

        2026-09-04 持仓视图契约修复：传入当日价字典（同 get_positions）。"""
        positions = self._engine._get_ptrade_positions(getattr(self, "_prices", None) or {})
        pos = self._lookup_position(positions, security)
        if pos is not None:
            return pos
        # 空仓：返回 amount=0 的默认 Position（Ptrade 真实平台行为）
        return Position(sid=security, volume=0, avg_cost=0.0, current_price=0.0)

    @staticmethod
    def _lookup_position(positions: dict, security: str):
        """在持仓 dict 中按裸码归一化查找（支持 .SS/.XSHG/.SZ/.XSHE/裸码 互通）。"""
        if not positions:
            return None
        if security in positions:
            return positions[security]
        bare = bare_code(security)
        for k, v in positions.items():
            if bare_code(k) == bare:
                return v
        return None

    def get_Ashares(self, date=None, exclude_bse=None, include_delisted=None):
        """获取全 A 股列表

        任务一（2026-09-06，总调度批复①②③）：数据源唯一化口径升级——
        - 默认**排除北交所**（920 前缀 + BSE legacy + 4xx/8xx 全段 blanket，
          is_bje_excluded 谓词；用户拍板"沪深 A 股不含北交"，P-D9 opt-in 约定
          经用户授权推翻）；exclude_bse=False 显式回退旧含北交语义（逃生门）；
        - PIT 退市过滤（批复③收窄版）：date 非空时按 stock_delist 过滤
          「上市 ≤ T AND (退市 IS NULL OR 退市 > T)」；ST@T 为后续标记项。

        P-D13 C1a/C1b 既有：板块统计审计行（QS_ASHARES_BREAKDOWN）+
        fidelity_ashares_snapshot 保真模式（优先级最高，不受默认翻转影响）。
        """
        try:
            # P-A0 保真模式：fidelity_ashares_snapshot=True 时用平台快照（2026-07-01）
            # 替代本地全市场股池（模拟平台候选池，仅转换产物对账验证用途）。
            fidelity = getattr(self, '_fidelity', None)
            if fidelity is not None and getattr(fidelity, 'fidelity_ashares_snapshot', False):
                return self._fidelity_ashares(date)
            if self._reference is None or not (self._current_date or date):
                return []
            codes = [self._to_ptrade_code(code) for code in
                     self._reference.get_all_stocks(date or self._current_date)]
            # P-D13 C1a：板块统计审计行（定位宇宙差——322 北交所 vs 0）
            try:
                from .libs.security_code_rules import is_bse_market
                _bse = sum(1 for c in codes if is_bse_market(c))
                logger.debug("QS_ASHARES_BREAKDOWN total=%d bse=%d non_bse=%d",
                             len(codes), _bse, len(codes) - _bse)
            except Exception:
                pass
            # 任务一（批复①②）：北交排除默认开（blanket 口径 is_bje_excluded）。
            # exclude_bse=False 显式回退含北交旧语义（逃生门）；None=跟随默认排除。
            from .libs.security_code_rules import is_bje_excluded
            if exclude_bse is None:
                exclude_bse = True  # 任务一默认翻转（用户口径 2026-09-06）
            if exclude_bse:
                codes = [c for c in codes if not is_bje_excluded(c)]
            # 任务一（批复③收窄版）：PIT 退市过滤——date 非空时按 stock_delist 过滤
            # 「上市 ≤ T AND (退市 IS NULL OR 退市 > T)」；date=None 跳过（兼容）。
            if include_delisted is None:
                include_delisted = not (self._current_date or date)
            if not include_delisted:
                codes = self._filter_delisted_pit(
                    codes, date or self._current_date)
            return codes
        except Exception as e:
            logger.debug(f"get_Ashares 失败: {e}")
            return []

    def _filter_delisted_pit(self, codes, date_str):
        """任务一批复③：PIT 退市过滤（stock_delist 334 条退市日历消费）。

        规则：退市日 ≤ T 的码剔除（在册口径对齐平台当月池；上市日条件由
        get_all_stocks 源头天然满足——入库行情即上市后）。
        实现：stock_delist 全表**惰性一次性预载**内存 set（热路径零 DB 往返）；
        防御式列名（delist_date/code 别名容错）；表不可用/加载失败 → 原样返回
        （fail-open，P-D13b 同款防护语义）。date 为空不过滤。
        """
        if not date_str or not codes:
            return codes
        cached = getattr(self, '_delisted_set_cache', None)
        if cached is None:
            try:
                import duckdb
                db = (getattr(getattr(self, '_reference', None), 'db_path', None)
                      or 'data/quantstudio.db')
                conn = duckdb.connect(str(db), read_only=True)
                rows = conn.execute(
                    "SELECT code, delist_date FROM stock_delist "
                    "WHERE delist_date IS NOT NULL").fetchall()
                conn.close()
                cached = set()
                for code, dd in rows:
                    bare = str(code).split('.')[0]
                    # delist_date 兼容 ms epoch 数值 / 'YYYYMMDD' 字符串两种形态
                    try:
                        dd8 = str(int(float(dd)))[:8] if dd is not None else None
                    except (TypeError, ValueError):
                        dd8 = str(dd)[:8].replace('-', '') if dd else None
                    if dd8:
                        cached.add((bare, dd8))
                self._delisted_set_cache = cached
            except Exception as e:
                logger.debug(f"PIT 退市集加载失败（fail-open 不过滤）: {e}")
                self._delisted_set_cache = set()
                return codes
        t8 = str(date_str)[:10].replace('-', '')

        def _delisted(code):
            bare = str(code).split('.')[0]
            return any(b == bare and dd <= t8 for b, dd in self._delisted_set_cache)

        kept = [c for c in codes if not _delisted(c)]
        dropped = len(codes) - len(kept)
        if dropped:
            logger.debug(f"PIT 退市过滤 {date_str}: 剔除 {dropped} 只已退市码")
        return kept

    def _fidelity_ashares(self, date=None):
        """P-A0：平台 A 股池快照消费（裸码 → Ptrade 格式 .SZ/.SS，惰性加载缓存）。

        PIT 门禁由 fidelity_config.resolve(backtest_start_date) 在注入前执行
        （回测起点 < 快照日 → ValueError fail-closed；超龄 >30 天 WARNING）。
        快照文件缺失 → FileNotFoundError fail-closed（不得静默退回本地池）。
        """
        fidelity = self._fidelity
        if self._fidelity_ashare_codes is None:
            # 惰性加载：读 parquet（构建器 probe_platform_ashares.py 产，sha256 已校验落盘）
            self._fidelity_ashare_codes = fidelity.load_ashare_snapshot()
        return [self._to_ptrade_code(code) for code in self._fidelity_ashare_codes]

    # ===================== 第2批新增 API =====================

    def get_stock_exrights(self, security, date=None):
        """获取证券除权除息信息（对应 Ptrade get_stock_exrights）
        返回 DataFrame（index=date，列: allotted_ps/rationed_ps/rationed_px/
        bonus_ps/exer_forward_a/b/exer_backward_a/b），无数据返回 None。"""
        return self._reference.get_exrights(bare_code(security), date)

    def get_stock_blocks(self, stock_code):
        """获取证券所属板块（对应 Ptrade get_stock_blocks）
        返回 dict {HY:行业, DY:地域, GN:概念, ZJHHY:证监会行业, ...}，无数据返回 None。
        DuckDB 暂无板块表 → 返回 None（Ptrade 语义：已退市/无数据返回 None）。"""
        return self._reference.get_blocks(bare_code(stock_code))

    def get_industry_stocks(self, industry_code):
        """获取行业成份股（对应 Ptrade get_industry_stocks）
        industry_code 尾缀须为 .XBHS（聚源行业编码），返回 list[str]。
        DuckDB 暂无行业成分表 → 返回空 list。"""
        return [self._to_ptrade_code(code) for code in
                self._reference.get_industry_stocks(industry_code)]

    def get_reits_list(self, date=None):
        """获取公募 REITs 基金代码列表（对应 Ptrade get_reits_list）
        DuckDB 暂无 REITs 表 → 返回空 list。"""
        return [self._to_ptrade_code(code) for code in self._reference.get_reits_list(date)]

    # -------- ETF 相关 --------

    def get_etf_list(self):
        """获取 ETF 代码列表（对应 Ptrade get_etf_list）
        从 stock_daily 按 ETF 代码段（510/511/512/513/515/588/159 开头）提取最新交易日活跃品种。"""
        try:
            if self._reference is None:
                return []
            return [self._to_ptrade_code(code) for code in self._reference.get_etf_list()]
        except Exception as e:
            logger.debug(f"get_etf_list 失败: {e}")
            return []

    def get_etf_list_local(self, query_date=None, etf_type="equity", active_only=True):
        """Return a PIT ETF universe for QuantStudio-only local backtests.

        This is deliberately a separate local extension. ``get_etf_list`` keeps its
        PTrade-name compatibility contract and must not be used to imply that PTrade
        backtests support dynamic ETF-universe discovery.
        """
        if self._reference is None:
            raise ReferenceDataCapabilityError(
                "get_etf_list_local requires an attached QuantStudio reference-data provider"
            )
        effective_date = query_date
        if effective_date is None:
            effective_date = self._current_date
        if effective_date is None or not str(effective_date).strip():
            raise ReferenceDataCapabilityError(
                "get_etf_list_local(query_date=None) requires an active backtest date; "
                "pass query_date explicitly outside a running backtest"
            )
        bare_codes = self._reference.get_etf_list_local(
            query_date=effective_date, etf_type=etf_type, active_only=active_only
        )
        return [self._to_ptrade_code(code) for code in bare_codes]

    def get_index_day_bar(self, security, count=1, fields=None):
        """返回已完成指数日线（QuantStudio 本地专用 API，docs/get-index-day-bar-design.md 2026-09-08）。

        本地扩展（平台无对应 API）：已登记 local_only_symbols，PTrade 目标
        TARGET-LOCAL-EXTENSION-BAN fail-closed BLOCK；转换重写映射与平台探针
        门禁见方案 §3.3（探针未过前含本 API 的源拒绝转换）。

        契约（终审钉死）：
        - 独占 index_daily 路由（绝不进 stock→etf fallback、绝不触发
          INDEX_ETF_MAP ETF 代理替换）；security 经 bare_code 归一查询。
        - profile-aware"已完成"上界：daily-bar-v1 含当前回测日 T；
          daily-open-close-proxy-v1 以回调上下文判定时钟（15:00 完成日线快照含 T、
          09:31 不含、无法可靠判定 fail-closed 不含 T 保守侧）；minute-bar-v1
          永不含 T（上界=上一完整交易日）。
        - 不暴露 fq（指数无复权，raw 即契约）；fields 枚举
          open/high/low/close/pctChg/volume/amount/trade_date，None=全列。
        - count 越界（<1 或 >250）显性 ValueError（禁止静默截断）。
        - fail-closed：无连接/无数据 → 空 DataFrame（策略侧 fail-soft + 审计行）。
        - 行序时间升序、index=trade_date；每次调用输出 QS_INDEX_BAR 诊断日志。
        """
        _INDEX_BAR_FIELDS = ("open", "high", "low", "close", "pctChg",
                             "volume", "amount", "trade_date")
        n = int(count)
        if n < 1 or n > 250:
            raise ValueError(
                f"get_index_day_bar count must be in [1, 250], got {count!r}")
        if fields is not None:
            req = [fields] if isinstance(fields, str) else list(fields)
            bad = [f for f in req if f not in _INDEX_BAR_FIELDS]
            if bad:
                raise ValueError(
                    f"get_index_day_bar unsupported fields {bad!r}; "
                    f"allowed: {list(_INDEX_BAR_FIELDS)!r}")
        if self._market is None or self._engine is None:
            return pd.DataFrame()
        profile = getattr(self._engine, "engine_profile", "daily-bar-v1")
        # ---- profile-aware "已完成"上界（end-of-day ms）----
        current_date = self._current_date
        if not current_date:
            return pd.DataFrame()
        day_end_ms = int((pd.Timestamp(str(current_date), tz="Asia/Shanghai")
                          + pd.Timedelta(hours=23, minutes=59, seconds=59))
                         .value // 10**6)
        prev_day = None
        try:
            prev_day = self.get_trading_day(-1)
        except Exception:
            prev_day = None
        # fail-closed 守卫：上一交易日不可得、或并非严格早于当前日（如 calendar 缺失时
        # get_trading_day 回退 current_date 的兼容行为）→ excl_T 语义无法保证 → 空。
        prev_day_valid = bool(prev_day) and str(prev_day)[:10] < str(current_date)[:10]
        day_prev_end_ms = (
            int((pd.Timestamp(str(prev_day), tz="Asia/Shanghai")
                 + pd.Timedelta(hours=23, minutes=59, seconds=59)).value // 10**6)
            if prev_day_valid else None)
        if profile == "daily-bar-v1":
            before_ms = day_end_ms
            mode = "daily_incl_T"
        elif profile == "minute-bar-v1":
            if not prev_day_valid:
                return pd.DataFrame()
            before_ms = day_prev_end_ms
            mode = "minute_excl_T"
        elif profile == "daily-open-close-proxy-v1":
            # 以引擎既有回调上下文判定当前时钟（ptrade_api.py:1159 同源判定先例）：
            # _proxy_intraday_bars 的最后一条快照即当前已推进到的合成时钟。
            include_t = False
            try:
                bars = getattr(self._engine, "_proxy_intraday_bars", []) or []
                if bars:
                    last_ts = int(bars[-1]["time"].iloc[0])
                    last_hhmm = pd.Timestamp(last_ts, unit="ms", tz="UTC")\
                        .tz_convert("Asia/Shanghai").strftime("%H:%M")
                    include_t = (last_hhmm >= "15:00")
            except Exception:
                include_t = False
            if not include_t:
                if not prev_day_valid:
                    return pd.DataFrame()
                before_ms = day_prev_end_ms
                mode = "proxy_excl_T_conservative"
            else:
                before_ms = day_end_ms
                mode = "proxy_incl_T"
        else:
            # 未知 profile → 保守侧不含 T
            if not prev_day_valid:
                return pd.DataFrame()
            before_ms = day_prev_end_ms
            mode = "unknown_profile_excl_T"
        bare = bare_code(security)
        df = self._market.get_index_day_bars(bare, n, before_ms)
        if df is None or df.empty:
            logger.info("QS_INDEX_BAR code=%s date=%s rows=0 mode=%s",
                        bare, current_date, mode)
            return pd.DataFrame()
        df = df.reset_index(drop=True)
        df = df.set_index("trade_date")
        if fields is not None:
            keep = [f for f in (req if isinstance(req, list) else [req])
                    if f in df.columns]
            df = df[keep]
        logger.info("QS_INDEX_BAR code=%s date=%s rows=%d mode=%s",
                    bare, str(df.index[-1]) if len(df) else current_date,
                    len(df), mode)
        return df

    def get_etf_info(self, etf_code):
        """获取 ETF 信息（对应 Ptrade get_etf_info）
        返回 {code: {etf_redemption_code, publish, report_unit, ...}}。
        DuckDB 无 ETF 申赎明细表 → 仅返回基础字段（从 stock_daily 推算 last_price）。"""
        etfs = [etf_code] if isinstance(etf_code, str) else list(etf_code)
        bare_codes = [bare_code(code) for code in etfs]
        info = self._reference.get_etf_info(bare_codes)
        return {self._to_ptrade_code(code): values for code, values in info.items()}

    def get_etf_stock_list(self, etf_code):
        """获取 ETF 成分券列表（对应 Ptrade get_etf_stock_list）
        DuckDB 无 ETF 成分券表 → 返回空 list。"""
        return [self._to_ptrade_code(code) for code in
                self._reference.get_etf_stock_list(bare_code(etf_code))]

    def get_etf_stock_info(self, etf_code, security):
        """获取 ETF 成分券信息（对应 Ptrade get_etf_stock_info）
        DuckDB 无 ETF 成分券表 → 返回空 dict。"""
        return self._reference.get_etf_stock_info(
            bare_code(etf_code), bare_code(security))

    def get_ipo_stocks(self):
        """获取当日 IPO 申购标的（对应 Ptrade get_ipo_stocks）
        回测模式无法获取当日柜台 IPO 列表 → 返回空 dict。"""
        return self._reference.get_ipo_stocks()

    # -------- 可转债相关 --------

    def get_cb_list(self):
        """获取可转债市场代码列表（对应 Ptrade get_cb_list）
        可转债代码：沪市 11x/13x，深市 12x。从 stock_daily 提取最新交易日活跃品种。"""
        try:
            if self._reference is None:
                return []
            return [self._to_ptrade_code(code) for code in self._reference.get_cb_list()]
        except Exception as e:
            logger.debug(f"get_cb_list 失败: {e}")
            return []

    def get_cb_info(self):
        """获取可转债基础信息（对应 Ptrade get_cb_info）
        返回 DataFrame（列: bond_code/bond_name/stock_code/stock_name/list_date/
        premium_rate/convert_date/maturity_date/convert_rate/convert_price/convert_value）。
        DuckDB 无可转债基础信息表 → 返回空 DataFrame（Ptrade 语义：无权限/无数据返回空）。"""
        return self._reference.get_cb_info()

    # ===================== 第3批新增 API（市场/文件/持仓/参数/期货降级）=====================

    # -------- 市场信息 --------

    def get_market_list(self) -> pd.DataFrame:
        """Return exchange, index, and block market identifiers."""
        return pd.DataFrame([
            {"finance_mic": "SS", "finance_name": "???????"},
            {"finance_mic": "SZ", "finance_name": "???????"},
            {"finance_mic": "BJ", "finance_name": "???????"},
            {"finance_mic": "CSI", "finance_name": "????"},
            {"finance_mic": "XBHS", "finance_name": "????"},
        ])

    def get_market_detail(self, finance_mic) -> pd.DataFrame:
        """获取市场详细信息（对应 Ptrade get_market_detail）
        从 DuckDB 提取该市场下的产品代码列表。
        finance_mic: 'XSHG'/'SS'(沪) / 'XSHE'/'SZ'(深) / 'CSI'(指数) / 'XBHS'(板块)。"""
        mic = str(finance_mic).upper()
        try:
            if self._reference is None:
                return pd.DataFrame(columns=['hq_type_code', 'prod_code', 'prod_name', 'trade_time_rule'])
            return self._reference.get_market_detail(mic)
        except Exception as e:
            logger.debug(f"get_market_detail 失败: {e}")
            return pd.DataFrame(columns=['hq_type_code', 'prod_code', 'prod_name', 'trade_time_rule'])

    def get_trend_data(self, date=None, stocks=None, market=None):
        """获取集中竞价期间代码数据（对应 Ptrade get_trend_data）
        DuckDB 无集合竞价明细 → 用当日日线 bar 近似（open 价、当日量额）。
        返回 {ptrade_code: {time_stamp/hq_px/wavg_px/business_amount/business_balance/amount}}。"""
        data = self._current_day_data
        if data is None or len(data) == 0:
            return {}
        # 筛选股票
        if stocks is not None:
            wanted = [stocks] if isinstance(stocks, str) else list(stocks)
            wanted_bare = {bare_code(s) for s in wanted}
            sub = data[data['code'].isin(wanted_bare)]
        elif market is not None:
            mkts = [market] if isinstance(market, str) else list(market)
            mkts = [m.upper() for m in mkts]
            def in_market(c):
                market = security_exchange(c)
                return ((market == 'SH' and 'XSHG' in mkts)
                        or (market == 'SZ' and 'XSHE' in mkts)
                        or (market == 'BJ' and ('XBJ' in mkts or 'BJ' in mkts)))
            sub = data[data['code'].apply(in_market)]
        else:
            sub = data
        try:
            ts = int(pd.Timestamp(self._current_date, tz='Asia/Shanghai').timestamp())
        except Exception:
            ts = 0
        result = {}
        for _, r in sub.iterrows():
            ptrade_code = self._to_ptrade_code(str(r.get('code', '')))
            result[ptrade_code] = {
                'time_stamp': ts,
                'hq_px': float(r.get('open', 0)),
                'wavg_px': float(r.get('amount', 0)) / float(r['volume']) if r.get('volume', 0) > 0 else float(r.get('open', 0)),
                'business_amount': int(r.get('volume', 0)),
                'business_balance': int(r.get('amount', 0)),
                'amount': 0,
            }
        return result

    # -------- 文件/持仓工具 --------

    def create_dir(self, user_path) -> bool:
        """创建文件子目录路径（对应 Ptrade create_dir）
        因 Ptrade 禁用 os 模块而提供；此处基于回测输出根目录创建。返回是否成功。"""
        try:
            base = self._cfg.research_dir if self._cfg else Path("output/research")
            target = base / str(user_path)
            target.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.warning(f"create_dir({user_path}) 失败: {e}")
            return False

    def get_trades_file(self, save_path='') -> Optional[str]:
        """获取对账数据文件（对应 Ptrade get_trades_file，仅回测）
        导出引擎成交记录为 CSV（表头: order_id,trading_id,entrust_id,security_code,
        order_type,volume,price,total_money,trading_fee,trade_time）。成功返回路径。"""
        try:
            trades = getattr(self._engine, '_all_trades', None) or getattr(self._engine, '_today_trades', [])
            base = self._cfg.output_dir if self._cfg else Path("output")
            out_dir = base / str(save_path) if save_path else base
            out_dir.mkdir(parents=True, exist_ok=True)
            fname = f"trades_{self._current_date.replace('-', '')}.csv"
            out_path = out_dir / fname
            cols = ['order_id', 'trading_id', 'entrust_id', 'security_code', 'order_type',
                    'volume', 'price', 'total_money', 'trading_fee', 'trade_time']
            if isinstance(trades, list) and len(trades) > 0:
                df = pd.DataFrame(trades)
                for c in cols:
                    if c not in df.columns:
                        df[c] = ''
                df[cols].to_csv(out_path, index=False, encoding='utf-8-sig')
            else:
                pd.DataFrame(columns=cols).to_csv(out_path, index=False, encoding='utf-8-sig')
            return str(out_path)
        except Exception as e:
            logger.warning(f"get_trades_file 失败: {e}")
            return None

    def get_all_positions(self) -> list:
        """获取全部持仓（对应 Ptrade get_all_positions，仅交易）
        回测降级：把引擎 Position 对象转成 Ptrade 柜台格式的 dict list。"""
        if not self._engine:
            return []
        result = []
        for qmt_code, pos in self._engine.account.positions.items():
            bare = normalize_security_code(qmt_code, "bare")
            exchange_type = {'SH': '1', 'SZ': '2', 'BJ': '3'}[
                security_exchange(qmt_code)]
            ptrade_code = self._to_ptrade_code(bare)
            last_price = getattr(pos, 'last_sale_price', None) or getattr(pos, 'current_price', 0) or 0
            amount = getattr(pos, 'volume', 0) or getattr(pos, 'amount', 0) or 0
            cost = getattr(pos, 'avg_cost', 0) or getattr(pos, 'cost_basis', 0) or 0
            result.append({
                'position_str': '', 'exchange_type': exchange_type,
                'stock_code': bare, 'stock_name': ptrade_code, 'stock_type': '0',
                'current_amount': amount, 'enable_amount': getattr(pos, 'can_sell', amount),
                'last_price': last_price, 'cost_price': cost,
                'market_value': last_price * amount, 'income_balance': (last_price - cost) * amount,
                'profit_ratio': ((last_price - cost) / cost * 100) if cost > 0 else 0,
                'delist_flag': '0', 'delist_date': 0,
            })
        return result

    def convert_position_from_csv(self, path) -> list:
        """从 CSV 读取底仓参数列表（对应 Ptrade convert_position_from_csv，仅回测）
        CSV 格式: sid,enable_amount,amount,cost_basis。返回 list[dict]。"""
        try:
            full = Path(path)
            if not full.is_absolute():
                base = self._cfg.research_dir if self._cfg else Path("output/research")
                full = base / path
            df = pd.read_csv(full, dtype=str)
            result = []
            for _, row in df.iterrows():
                result.append({
                    'sid': str(row.get('sid', '')),
                    'amount': str(row.get('amount', '')),
                    'enable_amount': str(row.get('enable_amount', row.get('amount', ''))),
                    'cost_basis': str(row.get('cost_basis', '')),
                })
            return result
        except Exception as e:
            logger.warning(f"convert_position_from_csv({path}) 失败: {e}")
            return []

    def load_research_signals(self, csv_path, fallback=None):
        """读取每日更新的首次覆盖研报 CSV（框架侧 I/O，供策略经注入 API 调用）。

        返回 (rows, source)：rows 为 [(code6, name, industry, pub), ...]，仅保留
        买入(评级代码007)/增持(006)；发布日期归一化为 YYYY-MM-DD（兼容
        "2026-06-26 00:00:00.000" 时间戳后缀）。source 为 "csv:<文件名>" 或 "embedded"。
        csv_path 缺失/解析失败时回退 fallback（默认空列表），source="embedded"。
        """
        try:
            p = Path(csv_path)
            if p.is_file():
                df = pd.read_csv(p, dtype=str, encoding="utf-8-sig")
                rows = []
                for _, r in df.iterrows():
                    code = str(r.get("股票代码") or "").strip()
                    name = str(r.get("股票名称") or "").strip()
                    industry = str(r.get("行业") or "").strip()
                    pub_raw = str(r.get("发布日期") or "").strip()
                    rc = str(r.get("评级代码") or "").strip()
                    rating = str(r.get("评级") or "").strip()
                    if not code or not pub_raw:
                        continue
                    # 评级代码存在多种写法：006/007（带前导零）或单数字 6/7；
                    # 统一接受买入/增持（文本 评级 列兜底，兼容编码缺失/不规范行）。
                    if rc not in ("006", "007", "6", "7") and rating not in ("买入", "增持"):
                        continue
                    rows.append((code.zfill(6), name, industry, pub_raw.split()[0]))
                if rows:
                    return rows, "csv:" + p.name
        except Exception as e:
            logger.warning(f"[load_research_signals] 解析失败 {csv_path}: {e}")
        fb = list(fallback) if fallback else []
        return fb, "embedded"

    # -------- 参数与期货（降级实现）--------

    def set_parameters(self, **kwargs):
        """设置策略配置参数（对应 Ptrade set_parameters，仅交易）
        回测模式：记录参数，返回 None。"""
        if not hasattr(self, '_parameters'):
            self._parameters = {}
        self._parameters.update(kwargs)
        return None

    def get_instruments(self, contract) -> dict:
        """Return basic security metadata; futures remain unsupported locally."""
        bare = normalize_security_code(contract, "bare")
        sec_type = classify_security(contract)
        supported = {'main_board', 'chinext', 'star_market', 'bse', 'etf', 'convertible_bond'}
        if sec_type in supported:
            market = security_exchange(contract)
            return {
                'contract_code': self._to_ptrade_code(bare),
                'contract_name': self._to_ptrade_code(bare),
                'exchange': {'SH': '???', 'SZ': '???', 'BJ': '???'}[market],
                'trade_unit': 100,
                'contract_multiplier': 1.0,
                'trade_code': bare,
                'margin_rate': 1.0,
            }
        return {}

    def get_dominant_contract(self, contract, date=None) -> dict:
        """获取主力合约代码（对应 Ptrade get_dominant_contract）
        DuckDB 无期货数据 → 返回空 dict。"""
        logger.debug(f"get_dominant_contract({contract}): DuckDB 无期货数据，返回 {{}}")
        return {}

    def get_margin_rate(self, transaction_code) -> float:
        """获取保证金比例（对应 Ptrade get_margin_rate，期货）
        DuckDB 无期货 → 返回 1.0（全额）。"""
        return 1.0

    def get_underlying_code(self, symbols) -> dict:
        """获取证券关联代码（对应 Ptrade get_underlying_code，交易模块）
        DuckDB 无关联代码表 → 返回空 dict。"""
        logger.debug("get_underlying_code: DuckDB 无关联代码表，返回 {}")
        return {}

    def get_user_name(self, login_account=True) -> Optional[str]:
        """获取登录终端的资金账号（对应 Ptrade get_user_name，回测/交易）
        回测模式无真实柜台账号 → 返回固定回测账号标识。"""
        return "BACKTEST_ACCOUNT"

    def get_research_path(self) -> str:
        """获取研究界面根目录路径（对应 Ptrade get_research_path，回测/交易）
        返回 QuantStudio 研究目录路径。"""
        return str(self._cfg.research_dir) if self._cfg else "output/research"

    def get_current_kline_count(self) -> int:
        """获取当前时间的分钟 bar 数量（对应 Ptrade get_current_kline_count）
        日线回测模式：返回当前回测日在全市场交易日历中的序号（近似分钟 bar 数）。
        一交易日 240 分钟，日线回测取当日已过去的分钟数（收盘=240）。"""
        try:
            if self._calendar is None or not self._current_date:
                return 0
            return self._calendar.get_kline_count(self._current_date)
        except Exception:
            return 0

    def get_stock_name(self, stocks):
        """获取股票名称（DuckDB 无 name 字段，返回代码作为名称）"""
        if isinstance(stocks, str):
            return {stocks: stocks}
        return {s: s for s in stocks}

    def get_strategy_events(self, event_type, effective_date=None, start_date=None,
                            end_date=None, security_list=None):
        """Return generic locally-ingested strategy events through the API layer.

        This is a QuantStudio local-backtest extension. Strategy source remains
        storage-isolated; the ReferenceDataProvider owns the DuckDB query.
        """
        columns = [
            "event_type", "event_date", "effective_date", "code", "signal",
            "name", "category", "source", "source_row_id", "source_key", "payload", "imported_at",
        ]
        if self._reference is None:
            return pd.DataFrame(columns=columns)
        codes = None
        if security_list is not None:
            values = [security_list] if isinstance(security_list, str) else list(security_list)
            codes = [normalize_to_ptrade(code) for code in values]
        try:
            return self._reference.get_strategy_events(
                event_type, effective_date=effective_date, start_date=start_date,
                end_date=end_date, codes=codes)
        except Exception as exc:
            logger.warning("get_strategy_events failed: %s", exc)
            return pd.DataFrame(columns=columns)

    def get_stock_info(self, stocks, field=None):
        """Return PTrade-shaped stock metadata from the formal reference layer.

        ``field`` accepts a field name or list such as ``['listed_date']``.
        Dates use the documented ``YYYY-MM-DD`` string shape, and the result is
        always keyed by the caller's original security code.
        """
        stock_list = [stocks] if isinstance(stocks, str) else list(stocks or [])
        requested = [field] if isinstance(field, str) else list(field or [])
        result = {}
        for security in stock_list:
            bare = bare_code(security)
            info = self._reference.get_security_info(bare) if self._reference is not None else None
            start_date = info.get('start_date') if info else None
            end_date = info.get('end_date') if info else None
            listed_date = None
            if start_date is not None:
                try:
                    listed_date = pd.Timestamp(start_date).strftime('%Y-%m-%d')
                except Exception:
                    listed_date = None
            de_listed_date = None
            if end_date is not None:
                try:
                    de_listed_date = pd.Timestamp(end_date).strftime('%Y-%m-%d')
                except Exception:
                    de_listed_date = None
            record = {
                'stock_name': (info or {}).get('display_name', security),
                # F2：股票/ETF 统一元数据；未知证券保持既有兼容行为（'stock' + 空值）
                'stock_type': (info or {}).get('security_type', 'stock'),
                'listed_date': listed_date,
                'de_listed_date': de_listed_date,
                'exchange_type': security_exchange(security),
                'code': security,
            }
            if requested:
                record = {name: record.get(name) for name in requested}
            result[security] = record
        return result

    def get_security_info(self, code):
        """获取证券基础信息（对应 Ptrade get_security_info）
        返回含 start_date（上市日期）的对象，策略用于剔除次新股。"""
        bare = bare_code(code)
        # 当日缓存（上市日期不变，同一天内重复查直接返回）
        cache_key = ("sec_info", bare)
        if hasattr(self, '_query_cache') and cache_key in self._query_cache:
            return self._query_cache[cache_key]
        # 优先从预加载查（避免逐只 MIN(time) 查询）
        info = self._reference.get_security_info(bare) if self._reference is not None else None
        start_dt = info.get('start_date') if info else None
        # 返回轻量对象（支持 .start_date 属性访问）
        result = type("SecurityInfo", (), {
            "code": code, "start_date": start_dt,
            "display_name": code, "name": code,
        })()
        if hasattr(self, '_query_cache'):
            self._query_cache[cache_key] = result
        return result

    def get_industry(self, code):
        """获取证券行业信息（LOCAL_ONLY 本地扩展，非 Ptrade API），APPROXIMATION_REQUIRES_CONFIRMATION（F4，非 PIT READY）。

        PTrade 官方无独立 get_industry，仅有 get_industry_stocks(industry_code)
        （返回行业成分股列表）；本函数返回个股行业归属——语义方向相反、无直接等价。
        双目标策略禁用本 API（skill 档案已标 unsupported_on_ptrade:true，
        Validator 会 BLOCK），须改用 get_industry_stocks 重建或降级为本地专有。

        返回 {'sw_l1': {'industry_code', 'industry_name',
        'classification_system', 'classification_version'}} 格式。
        签名不变；回测上下文自动注入当前回测日期（as-of），无有效历史归属
        返回 None，绝不使用最新行业。正式表缺失时 ReferenceDataCapabilityError
        向上传播（fail-closed），绝不回退 legacy sw_industry 快照。"""
        bare = bare_code(code)
        if self._reference is None:
            return None
        effective_date = str(self._current_date)[:10] if self._current_date else None
        return self._reference.get_industry(bare, effective_date)

    def get_stock_status(self, stocks, query_type='ST', query_date=None):
        """获取股票状态（与 PTrade 公共签名对齐）。

        PTrade 公共 ``query_type`` 取值为 ``ST``、``HALT`` 和
        ``DELISTING``。``DELISTING_SORTING`` 仅作为本地向后兼容别名保留；
        双端可移植策略源码必须使用 ``DELISTING``。

        数据来源：stock_daily 的 is_st_reliable / is_delisting_risk（aligner 预计算）。
        """
        query_type = str(query_type or 'ST').upper()
        if isinstance(stocks, str):
            stocks = [stocks]
        result = {}
        effective_date = query_date if query_date is not None else None
        data, status = self._resolve_status_source(stocks, effective_date)
        if data is None and status is None:
            return {s: False for s in stocks}
        for s in stocks:
            bare = bare_code(s)
            row = ((status[status['code'] == bare] if 'code' in status.columns else pd.DataFrame())
                   if status is not None else
                   (data[data['code'] == bare] if 'code' in data.columns else pd.DataFrame()))
            if len(row) > 0:
                r = row.iloc[0]
                if query_type == 'ST':
                    result[s] = bool(r.get('is_st', r.get('is_st_reliable', False)) or
                                     r.get('is_delisting_risk', False))
                elif query_type == 'HALT':
                    volume_halt = ('volume' in r.index and r.get('volume') == 0)
                    result[s] = bool(r.get('is_halt', False) or
                                     r.get('suspendFlag', 0) == 1 or volume_halt)
                elif query_type in {'DELISTING', 'DELISTING_SORTING'}:
                    result[s] = bool(r.get('is_delisting_risk', False))
                else:
                    result[s] = False
            else:
                result[s] = False
        return result

    def get_orders(self, security=None):
        """获取当日订单列表"""
        if not hasattr(self._engine, '_today_orders'):
            return []
        orders = self._engine._today_orders
        if security:
            return [o for o in orders if o.get('code') == security]
        return orders

    def get_trades(self):
        """获取当日成交列表"""
        if not hasattr(self._engine, '_today_trades'):
            return []
        return self._engine._today_trades

    def get_open_orders(self, security=None):
        """获取未成交订单。

        PR2: next_open 模式返回当前 pending queue（仅 status=pending）；
        close/open 即时执行模式下始终为空（保持 legacy 行为）。
        G1-I audit-fix 阻断3: basket_active 时同时包含 basket 内 pending orders。"""
        if self._engine.match_price_mode != "next_open":
            return []
        pending = [po for po in self._engine._pending_orders if po.status == "pending"]
        # G1-I: basket pending orders（所有 status=pending 的 basket 订单）
        if getattr(self._engine, 'basket_active', False):
            for b in getattr(self._engine, '_baskets', []):
                if b.status != "pending":
                    continue
                for po in b.sell_orders + b.buy_orders:
                    if po.status == "pending":
                        pending.append(po)
        orders = [self._engine._po_to_order(po, filled=False) for po in pending]
        if security:
            bare = bare_code(security)
            return [o for o in orders if bare_code(o.security) == bare]
        return orders

    def get_order(self, order_id):
        """根据 order_id 查订单。

        PR2: next_open 模式查 _pending_orders + _today_orders；close/open 返回 None。
        G1-I audit-fix 阻断3: basket_active 时同时查 basket pending/filled/rejected/
        cancelled/expired 订单（全生命周期可见）。"""
        if self._engine.match_price_mode != "next_open":
            return None
        for po in self._engine._pending_orders:
            if po.order_id == order_id:
                return self._engine._po_to_order(po, filled=(po.status == "filled"))
        # G1-I: basket orders（pending + 已 drain 状态）
        if getattr(self._engine, 'basket_active', False):
            for b in getattr(self._engine, '_baskets', []):
                for po in b.sell_orders + b.buy_orders:
                    if po.order_id == order_id:
                        return self._engine._po_to_order(po, filled=(po.status == "filled"))
        for o in getattr(self._engine, '_today_orders', []):
            if getattr(o, 'order_id', '') == order_id:
                return o
        return None

    def cancel_order(self, order_param):
        """撤单。

        PR2: next_open 模式按 order_id 精确移除 pending 订单 + 归还预扣；
        close/open 即时执行模式下 no-op（订单已即时成交，无法撤单）。

        order_param 可以是 Order 对象（取 order_id）或 order_id 字符串。
        G1-I audit-fix 阻断3: basket_active 时若 order_id 命中 basket 订单，
        调用 cancel_basket_order（并触发 mandatory sell 中止 buy leg，见阻断4）。"""
        if self._engine.match_price_mode != "next_open":
            return
        order_id = order_param
        if hasattr(order_param, 'order_id'):
            order_id = order_param.order_id
        if order_id is None:
            return
        # G1-I: 先查 basket pending orders（命中则走 basket cancel）
        if getattr(self._engine, 'basket_active', False):
            for b in getattr(self._engine, '_baskets', []):
                if b.status != "pending":
                    continue
                for po in b.sell_orders + b.buy_orders:
                    if po.order_id == order_id and po.status == "pending":
                        self._engine.cancel_basket_order(b, po)
                        return
        self._engine._cancel_pending_order(order_id)

    def set_volume_ratio(self, volume_ratio=0.25):
        """设置成交量占比限制"""
        self._volume_ratio = volume_ratio

    def set_yesterday_position(self, poslist):
        """设置初始持仓（底仓）"""
        if self._engine:
            for pos in poslist:
                code = pos.get('sid', pos.get('code', ''))
                bare = bare_code(code)
                qmt = normalize_to_qmt(code)
                volume = int(pos.get('amount', pos.get('volume', 0)))
                cost = float(pos.get('cost_basis', pos.get('avg_cost', 10.0)))
                from .backtest_engine import Position as Pos
                self._engine.account.positions[qmt] = Pos(
                    code=qmt, volume=volume, avg_cost=cost, can_sell=volume)
                self._engine.account.cash -= volume * cost

    def get_snapshot(self, security, frequency="1d"):
        """获取实时快照（回测模式返回当日 bar 数据）。

        PR3: frequency 形参仅签名对齐主计划 7.17；当前实现读 _current_day_data（日线），
        行为不变。分钟快照留待 PR4 引擎提供 _current_minute_data。"""
        bare = bare_code(security)
        data = self._current_day_data
        if data is None:
            return {}
        row = data[data['code'] == bare] if 'code' in data.columns else pd.DataFrame()
        if len(row) == 0:
            return {}
        r = row.iloc[0]
        return {
            'last_price': r.get('close', 0),
            'open': r.get('open', 0),
            'high': r.get('high', 0),
            'low': r.get('low', 0),
            'volume': r.get('volume', 0),
            'amount': r.get('amount', 0),
            'preclose': r.get('preClose', 0),
        }

    def get_frequency(self):
        """获取回测频率"""
        return 'daily'

    def get_business_type(self):
        """获取业务类型"""
        return 'stock'

    def get_MACD(self, close, short=12, long=26, m=9):
        """Ptrade 原生 MACD"""
        from .libs.MyTT import MACD
        return MACD(np.array(close), short, long, m)

    def get_KDJ(self, high, low, close, n=9, m1=3, m2=3):
        """Ptrade 原生 KDJ"""
        from .libs.MyTT import KDJ
        return KDJ(np.array(high), np.array(low), np.array(close), n, m1, m2)

    def get_RSI(self, close, n=6):
        """Ptrade 原生 RSI"""
        from .libs.MyTT import RSI
        return RSI(np.array(close), n)

    def get_CCI(self, high, low, close, n=14):
        """Ptrade 原生 CCI"""
        from .libs.MyTT import CCI
        return CCI(np.array(high), np.array(low), np.array(close), n)


# ==================== 全局 API 实例 ====================

_api = PtradeAPI()

# 导出为 Ptrade 策略期望的全局函数名
set_benchmark = _api.set_benchmark
set_limit_mode = _api.set_limit_mode
set_backtest = lambda *a, **kw: None
set_universe = _api.set_universe
set_commission = _api.set_commission
set_slippage = _api.set_slippage
set_fixed_slippage = _api.set_fixed_slippage
get_index_stocks = _api.get_index_stocks
get_fundamentals = _api.get_fundamentals
filter_stock_by_status = _api.filter_stock_by_status
check_limit = _api.check_limit
get_positions = _api.get_positions
get_position = _api.get_position
order_at_price = _api.order_at_price
order_target = _api.order_target
# A1 本地拆单接线（2026-08-22，与转换侧注入模板同构）：订单 API 金额语义按统一链
# ① 层 px 拆（>49,000 股拆多笔），使双端订单序列/费用逐笔一致。
# 注：本地 PtradeAPI 无 order_target_percent/order_percent 方法（percent 类为平台语义，
# 本地不支持 → 双端无分叉）；本地接线覆盖 order_target_value/order/order_value 三入口，
# 与模板的 percent 回退原 API 语义自然一致。原 API 引用经 class 属性承载（与模板一致）。
#
# P-D12（2026-08-26，docs/pd12-target-value-semantics-design.md）：order_target_value
# 恢复目标市值语义——delta = value − amount×T-1px；加仓对 delta 走拆单、减仓走原生
# 命令、清仓保底、微调跳过（阈值=引擎 min_rebalance_pct 0.005，T10 防漂移）、
# delta<1 手显式告警（B3）。根因=E-2（双端同构接线把 target 降级为全额买入，
# 引擎原生 delta 本身正确——B2 复现对照组绿）。
class _QSOrderWiringState:
    target_orig = None
    order_orig = None
    value_orig = None


# P-D12：微调跳过阈值——与 backtest_engine.BacktestEngine(min_rebalance_pct=0.005)
# 默认值一致（独立常量：转换模板自包含需要；T10 差分测试钉死防漂移）。
_QS_MIN_REBALANCE_PCT = 0.005


def _qs_current_value(security, px):
    """P-D12 D1：接线层现值 = amount × 统一链 ① 层前收（T-1 px）。

    不用 get_position().market_value——本地 _get_ptrade_positions(prices={}) 的
    current_price 回退 avg_cost（成本价），会低估减仓 delta；T-1 px 与决策时钟
    一致（PIT）且双端同源。异常视为空仓（D5：fail-open 继承现状语义——
    现值不可得 → delta=全额买入，与修复前行为等价，非新增设计）。"""
    try:
        pos = _api.get_position(security)
        amt = getattr(pos, 'amount', 0) or 0
        if amt <= 0 or px <= 0:
            return 0.0
        return float(amt) * float(px)
    except Exception:
        return 0.0


def _qs_warn_zero_order(api, security, value, px, reason):
    """P-D12 B3：0 股委托显式告警（不吞单不静默；含代码/目标金额/参考价/1 手价值）。"""
    try:
        logger.warning(
            "QS_ZERO_ORDER api=%s code=%s delta=%.1f px=%.4f one_lot_value=%.1f"
            " reason=%s", api, security, float(value or 0), float(px or 0),
            float(px or 0) * 100, reason)
    except Exception:
        pass


def _qs_report_noop(security, direction, reason):
    """F2-A（2026-09-23）：接线层 no-op 计数上报引擎 QS_FILL_AUDIT。

    背景：order_target_value 的接线层（本模块）在 `_qs_split_order` 返回空列表时走
    `_qs_noop_target`，不经引擎 `_finalize_immediate` → 不进 `_day_rejections` →
    QS_FILL_AUDIT 出现 submitted=N / filled=M / rejected=K 的对账缺口（N≠M+K），
    「计划 vs 实际」机械对齐失效（实测缺口 201/574，本策略 R5）。

    语义边界（行为等价）：仅向引擎的 `_day_rejections` 追加一条
    `(qmt_code, direction, reason)`——该列表只被 `_emit_fill_audit` 消费用于生成日志行，
    不参与任何订单、资金、持仓、估值或信号计算。故本上报**只增加可观测性，
    不改变任何可观察回测结果**。P-D12 B3 的 `QS_ZERO_ORDER` 告警原样保留。

    引擎未 attach / 列表不可得 → 静默跳过（行为与改动前等价）。
    """
    try:
        engine = getattr(_api, '_engine', None)
        if engine is None:
            return
        bucket = getattr(engine, '_day_rejections', None)
        if bucket is None:
            return
        to_qmt = getattr(engine, '_to_qmt', None)
        code = to_qmt(bare_code(security)) if to_qmt is not None else bare_code(security)
        bucket.append((code, direction, reason))
    except Exception:
        pass


def _qs_noop_target(security, delta, reason):
    """P-D12：接线层 no-op Order（对齐引擎原生 below_rebalance_threshold 形态——
    status='rejected'+reason，bool(filled)==False，策略可感知跳过）。"""
    from .backtest_engine import Order
    return Order(order_id="noop_%s" % security, security=security,
                 direction="buy" if (delta or 0) > 0 else "sell",
                 target=abs(delta or 0), status="rejected", reason=reason)


def _qs_wire_order_target_value(security, value, *args, **kwargs):
    """P-D12：target 语义恢复（delta 修复）。

    分支序：value=0 清仓保底 → px 缺失回退原生 → 现值/delta → 微调跳过（0.5%）
    → 减仓走原生（D3：卖出无 49k 分板上限，引擎原生含涨跌停/停牌防护）
    → 加仓对 delta 拆单（B1 同构）→ delta<1 手告警 no-op（B3）。"""
    if value is None:
        return _QSOrderWiringState.target_orig(security, value, *args, **kwargs)
    try:
        value = float(value)
    except (TypeError, ValueError):
        return _QSOrderWiringState.target_orig(security, value, *args, **kwargs)
    if value == 0:
        return _QSOrderWiringState.target_orig(security, 0, *args, **kwargs)
    # D4-S6（拆分换算价修复，2026-08-27，docs/bbrev-pd12-split-px-design.md v3）：
    # 换算价用 ② 层原语义（_QSPriceState.orig = _api.current_price，close 模式=当日收盘），
    # 与引擎成交基准一致，消除长假缺口价差（超支/拒单）。delta 保持 ① 层
    # （目标市值语义，_qs_current_value PIT 理由在案）——新仓 amount=0 → delta=value。
    px_exec = _QSPriceState.orig(security)
    if px_exec <= 0:
        return _QSOrderWiringState.target_orig(security, value, *args, **kwargs)
    px = _qs_last_close_lookup(security)
    if px <= 0:
        return _QSOrderWiringState.target_orig(security, value, *args, **kwargs)
    current = _qs_current_value(security, px)
    delta = value - current
    if current > 0 and abs(delta) / current < _QS_MIN_REBALANCE_PCT:
        return _qs_noop_target(security, delta, 'below_rebalance_threshold')
    if delta <= 0:
        if current > 0:
            return _QSOrderWiringState.target_orig(security, value, *args, **kwargs)
        return _qs_noop_target(security, 0.0, 'already_flat')
    cash_avail = getattr(getattr(_api, '_engine', None), 'account', None)
    orders, _tot = _qs_split_order(
        security, delta, px_exec,
        cash_avail=(float(cash_avail.cash) if cash_avail is not None else None))
    if not orders:
        _qs_warn_zero_order('order_target_value', security, delta, px_exec,
                            reason='delta_below_one_lot')
        # F2-A：no-op 计数上报（仅日志可观测性，零行为影响）
        _qs_report_noop(security, 'buy' if (delta or 0) > 0 else 'sell',
                        'delta_below_one_lot')
        return _qs_noop_target(security, delta, 'delta_below_one_lot')
    ids = []
    for code, amt in orders:
        ids.append(_qs_wire_order(code, amt))
    return ids[-1] if ids else None


def _qs_wire_order(security, amount, *args, **kwargs):
    if amount is None or amount <= 0 or amount <= _QS_MAX_ORDER_SHARES:
        return _QSOrderWiringState.order_orig(security, amount, *args, **kwargs)
    k = int((amount + _QS_MAX_ORDER_SHARES - 1) // _QS_MAX_ORDER_SHARES)
    per = int((amount // k) / _QS_SPLIT_LOT) * _QS_SPLIT_LOT
    if per <= 0:
        return _QSOrderWiringState.order_orig(security, amount, *args, **kwargs)
    oid = None
    placed = 0
    for i in range(k):
        seg = per if i < k - 1 else amount - placed
        seg = int(seg / _QS_SPLIT_LOT) * _QS_SPLIT_LOT
        if seg <= 0:
            break
        oid = _QSOrderWiringState.order_orig(security, seg, *args, **kwargs)
        placed += seg
    return oid if oid is not None else _QSOrderWiringState.order_orig(
        security, amount, *args, **kwargs)


def _qs_wire_order_value(security, value, *args, **kwargs):
    """order_value 拆单包装：金额语义 → ② 层换算价（D4-S6 修复，与 order_target_value 同链路）。"""
    px_exec = _QSPriceState.orig(security)
    if px_exec <= 0:
        return _QSOrderWiringState.value_orig(security, value, *args, **kwargs)
    cash_avail = getattr(getattr(_api, '_engine', None), 'account', None)
    orders, _tot = _qs_split_order(
        security, value, px_exec,
        cash_avail=(float(cash_avail.cash) if cash_avail is not None else None))
    if not orders:
        return _QSOrderWiringState.value_orig(security, value, *args, **kwargs)
    ids = []
    for code, amt in orders:
        ids.append(_qs_wire_order(code, amt))
    return ids[-1] if ids else None


_QSOrderWiringState.target_orig = _api.order_target_value
_QSOrderWiringState.order_orig = _api.order
_QSOrderWiringState.value_orig = _api.order_value
order_target_value = _qs_wire_order_target_value
order = _qs_wire_order
order_value = _qs_wire_order_value
# 2026-09-08 本地注入 API（已完成指数日线，docs/get-index-day-bar-design.md）：
# 独占 index_daily 路由、profile-aware 已完成上界、count ValueError、QS_INDEX_BAR 日志。
get_index_day_bar = _api.get_index_day_bar
get_history = _api.get_history
# A2 统一链 ① 层：get_history 最近收盘自动记录（PIT 纪律：cache 带日期戳每日重置）。
# 代理绑定使所有策略调用路径（ptrade_import 注入的模块级名）都经由此记录。
_QSHistoryRecordState = type("_QSHistoryRecordState", (),
                             {"orig": None, "day": None})


def _qs_record_trade_day(df):
    """从 get_history 返回体最后一行推断交易日（'YYYY-MM-DD'）。"""
    if df is None or not hasattr(df, 'iloc') or len(df) == 0:
        return ''
    try:
        r = df.iloc[-1]
        for col in ('trade_date', 'time', 'datetime'):
            if col in df.columns:
                v = r.get(col)
                if v is None:
                    continue
                s = str(v)
                if s and s != 'nan':
                    return s[:10]
        idx = df.index
        if len(idx) > 0:
            s = str(idx[-1])
            if s and s != 'nan':
                return s[:10]
    except Exception:
        pass
    return ''


def _qs_history_record_wrapper(*args, **kwargs):
    """调用原 get_history 后，提取每码最近一根已完成日线 close 写入
    _QSLastCloseState.cache（仅当日有效；stamp 跨日失效，PIT 纪律）。"""
    result = _QSHistoryRecordState.orig(*args, **kwargs)
    try:
        unit = kwargs.get('frequency') or kwargs.get('unit') or '1d'
        if str(unit) != '1d':
            return result  # 只记录日线（分钟 close 语义不同，不入统一链 ①）
        fq = kwargs.get('fq')
        if fq is None or str(fq) not in ('pre',):
            return result  # ① 层 = 前复权信号链（与撮合 raw 分离），仅记录 pre
        import pandas as _pd
        sample = result if not isinstance(result, dict) else (
            next(iter(result.values()), None))
        day = _qs_record_trade_day(sample)
        if not day:
            return result
        cache = _QSLastCloseState.cache
        if _QSLastCloseState.stamp != day:
            if cache is None:
                cache = {}
                _QSLastCloseState.cache = cache
            else:
                cache.clear()          # PIT 纪律：跨日失效，不复用陈旧前收
            _QSLastCloseState.stamp = day
        if isinstance(result, dict):
            for code, df in result.items():
                if df is None or not hasattr(df, 'iloc') or len(df) == 0:
                    continue
                try:
                    v = float(df.iloc[-1].get('close', 0))
                except Exception:
                    v = 0.0
                if v and v > 0:
                    cache[bare_code(str(code))] = (day, v)
        else:
            df = result
            if df is not None and hasattr(df, 'iloc') and len(df) > 0:
                try:
                    codes = df['code'].values if 'code' in df.columns else None
                except Exception:
                    codes = None
                if codes is not None:
                    for i in range(len(df)):
                        try:
                            v = float(df.iloc[i].get('close', 0))
                        except Exception:
                            v = 0.0
                        if v and v > 0:
                            cache[bare_code(str(codes[i]))] = (day, v)
                else:
                    try:
                        v = float(df.iloc[-1].get('close', 0))
                    except Exception:
                        v = 0.0
                    if v and v > 0:
                        # 单码返回：无法从数据取 code → 尝试从 kwargs 取
                        sec = (kwargs.get('security_list') or kwargs.get('security')
                               or (args[0] if args else None))
                        if sec is not None:
                            if isinstance(sec, (list, tuple)):
                                sec = sec[0]
                            cache[bare_code(str(sec))] = (day, v)
    except Exception:
        pass
    return result


_QSHistoryRecordState.orig = get_history
get_history = _qs_history_record_wrapper
get_price = _api.get_price
attribute_history = _api.attribute_history
# B1 批量取数 API（模块级绑定，供 ptrade_import 注入）
get_fundamentals_batch = _api.get_fundamentals_batch
get_history_batch = _api.get_history_batch
# A2 统一链 current_price：① 前收(_qs_last_close_lookup) → ② 原语义(当日/最近bar) → ③ get_history 兜底。
# 原 API 语义不变（_api.current_price 直调可仍返回 0=当日缺失）；模块级名供策略用统一链。
class _QSPriceState:
    """原始 current_price 引用（class 属性，与注入模板同构）。"""
    orig = None


_QSPriceState.orig = _api.current_price


def current_price(security):
    v = _qs_last_close_lookup(security)
    if v and v > 0:
        return v
    try:
        p = _QSPriceState.orig(security)
        if p is not None and p > 0:
            return float(p)
    except Exception:
        pass
    try:
        df = get_history(count=1, frequency="1d", field=["close"],
                         security_list=security, fq="pre", include=False)
        if df is not None and hasattr(df, "iloc") and len(df) > 0:
            c = float(df.iloc[-1].get("close", 0) or 0)
            if c > 0:
                return c
    except Exception:
        pass
    return 0.0


get_current_data = _api.get_current_data
get_trading_day = _api.get_trading_day
get_trade_days = _api.get_trade_days
get_all_trades_days = _api.get_all_trades_days
get_trading_day_by_date = _api.get_trading_day_by_date
run_daily = _api.run_daily
get_Ashares = _api.get_Ashares
get_strategy_events = _api.get_strategy_events
get_stock_name = _api.get_stock_name
get_stock_info = _api.get_stock_info
get_stock_status = _api.get_stock_status
get_security_info = _api.get_security_info
get_industry = _api.get_industry
get_orders = _api.get_orders
get_trades = _api.get_trades
get_open_orders = _api.get_open_orders
get_order = _api.get_order
cancel_order = _api.cancel_order
set_volume_ratio = _api.set_volume_ratio
set_yesterday_position = _api.set_yesterday_position
get_snapshot = _api.get_snapshot
get_frequency = _api.get_frequency
get_business_type = _api.get_business_type
get_MACD = _api.get_MACD
get_KDJ = _api.get_KDJ
get_RSI = _api.get_RSI
get_CCI = _api.get_CCI
is_trade = lambda: False  # 回测模式返回 False（Ptrade 语义）

# ===== A1 市价单拆单（本地同构 helper，2026-08-22）=====
# 与 source_import._QS_ORDER_SPLIT_EXT 注入模板同构（同常量同算法）→
# 本地与 PTrade 转换产物订单序列逐笔一致。平台实测（测试456/908）：
# 创业板/科创板市价单单笔上限 50,000 股（51,000 拒），沪深主板 ≥86,900 通过；
# 超限 = 整单取消。拆单把 >49,000 股目标拆多笔，全板规避上限。
_QS_MAX_ORDER_SHARES = 49000      # 单笔安全上限（低于创业板/科创板 50,000；主板远高）
_QS_SPLIT_LOT = 100               # A股整手
# D4-S6 修复后（2026-08-27）：换算价=②层当日撮合价，价差已消除 → buffer=1.0 不再降档
# （审计 R1 裁定：0.95 会在多笔拆单引入 5% 系统性低配，超出修复范围；可负担性由引擎
#  _execute_buy 既有单手回退兜底）。保留参数位置/命名以维持双端模板同构。
_QS_SPLIT_PX_BUFFER = 1.0        # 整手可负担预筛缓冲（价差归零后仅兜多笔竞对边角）

# 框架级最近收盘缓存（统一链 ① 层，A2 由 get_history 链记录）
# 格式 {bare_code: (day, close)}；stamp = 最近记录交易日（PIT：每日失效，不复用陈旧前收）
_QSLastCloseState = type("_QSLastCloseState", (), {"cache": None, "stamp": None})


def _qs_last_close_lookup(code):
    """统一链 ① 层（前收）：_qs_last_close 框架缓存。"""
    try:
        cache = _QSLastCloseState.cache or {}
        v = cache.get(bare_code(str(code)), 0.0)
        if isinstance(v, (tuple, list)) and len(v) == 2:
            return float(v[1])
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return 0.0


def _qs_split_order(security, value, px, cash_avail=None):
    """把目标金额拆成 ≤_QS_MAX_ORDER_SHARES 股的多笔 order(amount=...)。

    与 source_import 注入模板 `_QS_ORDER_SPLIT_EXT` 中的实现逐字同构
    （双端订单序列一致）。返回 (order_list, total_shares)；无法拆返回 ([], 0)。
    """
    if px is None or px <= 0 or value is None or value <= 0:
        return [], 0
    n = value / px
    if n <= _QS_MAX_ORDER_SHARES:
        # D4-S7 #9（2026-08-28）：单笔分支同样应用 cash_avail 钳制（此前只在拆单分支生效，
        # 现金竞对日超买——4b 单测暴露）。budget = min(cash, value)，无折扣（M1 系数 1.0）。
        _budget = min(cash_avail, value) if cash_avail is not None else value
        if _budget < value:
            n = _budget / px
        amount = int(n / _QS_SPLIT_LOT) * _QS_SPLIT_LOT
        return ([(security, amount)], amount) if amount > 0 else ([], 0)
    k = int((n + _QS_MAX_ORDER_SHARES - 1) // _QS_MAX_ORDER_SHARES)  # ceil
    budget = min(cash_avail, value) if cash_avail is not None else value
    use_buffer = cash_avail is not None
    per = budget / k
    orders = []
    total_cost = 0.0
    for _i in range(k):
        seg_amount = int(per / px / _QS_SPLIT_LOT) * _QS_SPLIT_LOT
        if seg_amount <= 0:
            seg_amount = _QS_SPLIT_LOT
        seg_amount = min(seg_amount, _QS_MAX_ORDER_SHARES)
        cost = seg_amount * px
        if use_buffer and total_cost + cost > budget * _QS_SPLIT_PX_BUFFER:
            break
        orders.append((security, seg_amount))
        total_cost += cost
    return (orders, sum(a for _, a in orders)) if orders else ([], 0)

# ===== 第2批新增：财务/除权/板块/行业/ETF/可转债 =====
get_stock_exrights = _api.get_stock_exrights
get_stock_blocks = _api.get_stock_blocks
get_industry_stocks = _api.get_industry_stocks
get_reits_list = _api.get_reits_list
get_etf_list = _api.get_etf_list
get_etf_list_local = _api.get_etf_list_local
get_etf_info = _api.get_etf_info
get_etf_stock_list = _api.get_etf_stock_list
get_etf_stock_info = _api.get_etf_stock_info
get_ipo_stocks = _api.get_ipo_stocks
get_cb_list = _api.get_cb_list
get_cb_info = _api.get_cb_info

# ===== 第3批新增：市场/文件/持仓/参数/期货降级 =====
get_market_list = _api.get_market_list
get_market_detail = _api.get_market_detail
get_trend_data = _api.get_trend_data
create_dir = _api.create_dir
get_trades_file = _api.get_trades_file
get_all_positions = _api.get_all_positions
convert_position_from_csv = _api.convert_position_from_csv
set_parameters = _api.set_parameters
get_instruments = _api.get_instruments
get_dominant_contract = _api.get_dominant_contract
get_margin_rate = _api.get_margin_rate
get_underlying_code = _api.get_underlying_code
get_user_name = _api.get_user_name
get_research_path = _api.get_research_path
get_current_kline_count = _api.get_current_kline_count
load_research_signals = _api.load_research_signals

# ===== ORM 查询（query/valuation，模块级函数/对象）=====
# query 和 valuation 是模块级定义的，非 PtradeAPI 实例方法，已在上方定义
# 此处显式声明确保 ptrade_import 可导入（Python 模块顶层名称自动可见，无需赋值）
