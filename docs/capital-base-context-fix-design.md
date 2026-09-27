# capital_base 契约补全方案（六步①，待审）

- 日期：2026-09-27｜归属：dev｜类型：框架层修复（共享核心文件 `ptrade_api.py`）
- 状态：**待总调度审计**（未过审不动码）

## 一、问题定义

四象限 ETF 轮动策略回测报 `[Ptrade] initialize error: 'Context' object has no attribute 'capital_base'`。

**根因（已由总调度定谳，日历 §三四八，本件不重复归因）**：本地 `Context` 类（`ptrade_api.py:224`）未提供
PTrade 平台的 `context.capital_base` 属性；策略侧 `safe_float(context.capital_base)` 因 Python **实参急切求值**
使防御失效，`AttributeError` 抛穿 `initialize`；策略 `:957` 的 `hasattr` 自愈使回测照常出结果，
但**风控高水位基准漂移**（以首检净值替代初始资金）。**非回归**（9/17 首跑即有，与性能链无关）。

**本件取证（只读）**：
| 项 | 证据 |
|---|---|
| `capital_base` 在 `ptrade_api.py` 中的出现次数 | **0**（grep 全文件零命中） |
| `Context.__init__`（`:226`）现有属性 | `current_dt` / `previous_date` / `portfolio` / `blotter` —— **无 `capital_base`** |
| 策略侧契约（只读引用，**不改**） | `四象限ETF轮动策略.py:26`（注释声明本地不提供）、`:892`（本地适配①）、`:896`/`:900`（两处 `safe_float(context.capital_base)`）、`:1226`（元数据 `capital_base: 1000000`） |

## 二、文档查证（铁律「平台代码先查文档」前置动作，如实登记）

| 库 ID | 查询 | 结果 |
|---|---|---|
| `/websites/ptradeapi` | context capital_base initialize | **未取到 `capital_base` 条目**——返回 Portfolio 属性表（`cash`/`positions`/`portfolio_value`/`positions_value`/`capital_used`/`returns`/`pnl`/`start_date`）。其中 `returns: 当前的收益比例, 相对于初始资金` 提及「初始资金」概念，但**未给出该属性的平台名** |
| `/kay-ou/ptradeapi` | 同上 | **未取到**——返回 Ptrade 策略基本结构模板（initialize/handle_data） |

⇒ **两个 PTrade 文档源均未取到 `capital_base` 条目**（如实登记，不伪装为查证结论）。
⇒ 语义依据 = **策略内契约声明**（`:26` + 元数据 `capital_base: 1000000`）+ **引擎初始资金**（下述）。

## 三、取值路径核验（自行核验，禁臆测）

**Context 构造链**（`backtest_engine.py`，5 处）：
```
:486   init_ctx = Context(first_day, first_day, Portfolio(self.account.cash, {}))
:2138  self._ptrade_context.portfolio = Portfolio(self.account.cash, ptrade_positions)   # 刷新
:2159  portfolio = Portfolio(self.account.cash, ptrade_positions); ctx = Context(day_str, prev_day_str, portfolio)
:2264  同上
:2423  同上
```

**初始资金存放点**：
```
:405  self.account = Account(cash=capital)
:414  self._initial_capital = capital            ← 初始资金持久存放点
:415  self.result.initial_capital = float(capital)
:321  capital: float = 100_000                   ← 构造参数默认（对齐 PTrade 平台默认 10 万）
```

**关键判读（决定取值源）**：`account.cash` 在**运行期是活值**（成交后变化）⇒ 刷新点传入的 `Portfolio._init_cash`
= 当时活值，**≠ 初始资金** ⇒ **不得以 portfolio 为源**；须以引擎 `_initial_capital` 为源。

**既有先例**：`result_exporter.py:29` 已有 `'init_capital': engine._initial_capital` —— 该读取路径在本仓**已有先例**，非新造。

## 四、改动范围（最小，单文件）

**仅 `quantstudio/backtest/ptrade_api.py`**：`Context` 增补 `capital_base`（**引擎委托式只读 property**）。

```python
@property
def capital_base(self) -> float:
    """PTrade 平台 context.capital_base 等价语义 = 引擎初始资金。

    单一真源：引擎 _initial_capital（:414 写入；先例 result_exporter.py:29）。
    构造期/无引擎（单测/非 ptrade 模式）回退 Portfolio._init_cash 快照——
    与 Portfolio 既有「构造期快照兜底」设计同构（D4-S7 活属性模式）。
    """
    eng = ...   # 复用 Portfolio._engine() 的同源解析（唯一解引用点，避免二次解引用漂移）
    init = getattr(eng, "_initial_capital", None) if eng is not None else None
    if init is not None:
        return float(init)
    return float(self.portfolio._init_cash)
```

**荐形理由**：
- 与 `Portfolio` 既有的「引擎委托 + 快照兜底」模式**完全同构**（该模式已由 D4-S7 方案批准并落地）；
- **不改 `backtest_engine.py`** ⇒ 5 处构造点**自动受益、零漏点风险**（漏点即复现本缺陷）；
- 改动面收敛在**本件指定的文件域**（`ptrade_api.py`），与块二文件域分离；
- 禁改任何策略 `.py`（铁律），本件零策略改动。

**备选（B）显式构造参数**：`Context(..., capital_base=...)` + 5 处调用点同步——**不荐**：
触及第二文件（`backtest_engine.py`）、且 5 点漏一即复现，风险与改动面均劣于 A。

## 五、影响面

| 面 | 判断 |
|---|---|
| 不读 `capital_base` 的既有策略 | **零感知**（纯新增属性，无签名/返回结构/行为变更） |
| 读 `capital_base` 的策略（如四象限） | 由 `hasattr` 自愈（错值=首检净值）→ **真值=初始资金** ⇒ **风控高水位基准纠正**（本件目的） |
| 公共 API 面 | 新增只读属性；不改变任何既有函数签名/默认值/返回字段 |
| 性能 | property 惰性求值；仅被读时解析一次引擎引用，无热点路径开销 |

## 六、验收标准

| # | 项 | 判据 |
|---|---|---|
| V1 | 四象限策略重跑 | `initialize` **无 ERROR** + 出现「策略初始化完成」日志 + `g.high_water == 初始资金` |
| V2 | 既有策略逐位不变 | `scripts/ab_perf_chain_runner.py` 方法学：改动前后**同库同窗 sha 一致**（参照 `docs/evidence/ab-perf-chain-bitwise-verification-20260927.md`） |
| V3 | 单元契约 | 新增用例：有引擎 → `engine._initial_capital`；无引擎 → `portfolio._init_cash` 兜底 |
| V4 | 回归 | pytest 相关套件全绿 + **55 既有失败基线逐项复现**（零新增） |
| V5 | 多策略横验证 | `api_portability` **6 策略 PASS** |

## 七、回退条件

- 单文件**纯新增属性**；回退 = 删除该 property（逐文件精确回退，无批量 git）；
- V2 出现任何非预期 diff ⇒ 立即回退（纯增益失败即停）；
- V4 出现新增失败 ⇒ 回退并归因。

## 八、纯增益审计登记（修复前置三型判定铁律）

| 三问 | 结论 |
|---|---|
| ① 是否影响项目其他功能 | **否**——纯新增只读属性 |
| ② 是否影响回测性能 | **否**——property 惰性求值，无热点开销 |
| ③ 是否影响回测精度 | **有影响，且属有意修正**——读该属性的策略由错值（首检净值）→ 真值（初始资金）。**非漂移，属本件修复目标**；不读该属性的策略精度零变化（V2 逐位证明） |

**形式判定**：diff **纯新增、无既有行为漂移**。
按既有三型（纯性能型/纯恢复型/数据修正型/新增检测型）映射，最贴近**纯恢复型**（回到 PTrade 平台语义原态）。
总调度登记为「**纯增益契约补全型**」——**该类型若需正式入册三型清单，请总调度裁定**；本件不自行新增类型。

## 九、待审问题（呈裁）

1. **私有属性访问**：荐形 A 读 `engine._initial_capital`（有先例 `result_exporter.py:29`）。
   若审方要求消除私有依赖 → 需在 `backtest_engine.py` 补公开只读属性 `initial_capital`，**触及第二文件**（块二文件域）⇒ 请裁：接受先例（荐）／要求补公开访问器（需声明串行）。
2. **兜底语义**：无引擎场景回退 `portfolio._init_cash`（构造期=初始资金；刷新点=当时活值）。
   是否要求兜底亦严格取初始资金？若要求 → 须在引擎侧注入，改动面上移到第二文件 ⇒ 请裁。
3. **元数据关系**：策略元数据声明 `capital_base: 1000000`，而引擎默认 `capital = 100_000`（`:321`）。
   本件以**引擎实际初始资金**为准（总调度口径）；元数据声明与实参不一致时**不自动对齐**——如需一致性校验另立小件。

## 十、纪律承诺

- `ptrade_api.py` 属**共享核心文件**：写前快照（`stash create` + `stash store`）→ 每次 edit 后 `git diff` 自检 → **精确文件清单提交**（禁 `add -A`）；
- pytest 全库跑前按 **daemon 生命周期跨会话占用纪律**声明窗口；
- 与块二**文件域分离**；若确需触及 `backtest_engine.py` 先声明**串行**；
- **禁改任何策略 `.py`**（铁律：修复仅限框架层，策略源码零改动）。