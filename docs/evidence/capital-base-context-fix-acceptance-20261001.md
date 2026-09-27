# capital_base 契约补全 · 验收证据（④实施-验收，2026-10-01）

- 归属：dev｜方案件：`docs/capital-base-context-fix-design.md`（审计通过）｜状态：**实施完成，V1/V3 已证；V2/V4/V5 待跑**

## 一、实施内容（形 A，单文件）

- 文件：`quantstudio/backtest/ptrade_api.py` —— `Context` 增补 `capital_base` 只读 property（**+25 行纯新增**）；
- 语义：引擎 `_initial_capital` 优先（恒定初始资金，非活值）；无引擎回退 `portfolio._init_cash` 快照；返回 `float()`；
- docstring 已注明 PTrade 契约、「恒定初始资金非活值」语义、以及 **Context7 两源未收录该条目**的事实；
- **策略源码零改动**（铁律）。

```
git diff --stat: quantstudio/backtest/ptrade_api.py | 25 +++++++++++++++++++++++++
COMPILE: 0 ｜ isinstance(Context.__dict__['capital_base'], property) = True
```

## 二、Context 构造点全量复扫（总调度勘正复核）

`Select-String -Path quantstudio\**\*.py -Pattern "Context\("` 结果：

| 行 | 内容 | 是否 Context 构造 |
|---|---|---|
| `backtest_engine.py:486` | `Context(first_day, first_day, Portfolio(self.account.cash, {}))` | ✅ |
| `backtest_engine.py:2159` | `ctx = Context(day_str, prev_day_str, portfolio)` | ✅ |
| `backtest_engine.py:2264` | 同上 | ✅ |
| `backtest_engine.py:2423` | 同上 | ✅ |
| `backtest_engine.py:2138` | `self._ptrade_context.portfolio = Portfolio(...)` | ❌ **Portfolio 刷新，非 Context 构造** |
| 其余命中 | `mp.get_context("spawn")` / `stack.enter_context(...)` / `push_basket_context` 等 | ❌ 同名不同物 |

⇒ **实测 4 处构造点**（总调度勘正属实；本件原报 5 处系误将 :2138 计入）。
⇒ **委托式 property 对这 4 处自动覆盖**（属性挂在 Context 类上，与构造点数量无关）——**零漏点**。

## 三、V1 四象限策略重跑 —— **PASS（三判据全达）**

```
python scripts/ab_perf_chain_runner.py --root . --strategy "四象限ETF轮动策略.py" \
    --start 2026-01-01 --end 2026-09-01 --out <tmp>
→ [ab] 四象限ETF轮动策略.py @ QuantStudio: nav=161 trades=27 sha=9e4c123cd67a8744 wall=52s
```

| # | 判据 | 实测 |
|---|---|---|
| V1-1 | `initialize` 无 ERROR | `initialize error\|AttributeError\|capital_base` 检索 = **0 命中** |
| V1-2 | 出现「策略初始化完成」 | ✅ `05:29:34 [四象限ETF][初始化] 策略初始化完成，初始净值高点=100000.00，组合回撤阈值=5.00%，单标的止损阈值=5.00%` |
| V1-3 | `g.high_water == 初始资金` | **初始净值高点=100000.00** = 引擎初始资金（`backtest_engine.py:321 capital: float = 100_000`）✅ |

**补充**：回测完整跑完 161 天（`[Backtest] completed: 161 days`），末行 `组合净值=97979.14 … 历史高点=104964.20` —— 高水位基准贯通全程，未见基准漂移。

## 四、V3 单元契约 —— **PASS（6 passed）**

`tests/test_capital_base_context.py`（新增）：

| 用例 | 断言 |
|---|---|
| `test_attribute_exists` | 修复前必红：Context 必须提供 capital_base |
| `test_engine_path_returns_initial_capital` | 有引擎 → `engine._initial_capital` |
| `test_engine_path_not_live_cash` | **关键**：引擎耐久值优先于运行期活值 cash（防回到错值路径） |
| `test_fallback_without_engine` | 无引擎 → `portfolio._init_cash` 快照 |
| `test_returns_float` | 返回恒为 float |
| `test_read_only_property` | 只读（防误写污染引擎真源） |

```
python -m pytest tests/test_capital_base_context.py -q  →  6 passed
```

**挂点说明（测试台自纠）**：引擎引用位于模块级单例 `_api._engine`（`ptrade_api.py:2695 _api = PtradeAPI()`），
非 `api._engine`；首版测试挂错对象致 2 条假失败，修正后 6/6。

## 五、V2 / V4 / V5 —— **待跑（如实登记，不预填结论）**

| # | 项 | 命令/方法学 | 状态 |
|---|---|---|---|
| V2 | 至少 2 个不读 `capital_base` 的代表策略逐位一致 | `scripts/ab_perf_chain_runner.py` 同库同窗 sha 比对（改动前后；参照 `docs/evidence/ab-perf-chain-bitwise-verification-20260927.md` 方法学） | ⏳ 待跑 |
| V4 | pytest 相关套件全绿 + **55 既有失败基线逐项列表复现**（零新增） | 全库 pytest + 基线列表逐项对表 | ⏳ 待跑 |
| V5 | `api_portability` 6 策略 PASS | 6 策略重转 + api_portability | ⏳ 待跑 |

**V4 前置**：按 **daemon 生命周期跨会话占用纪律**，全库 pytest 跑前须声明窗口（daemon 占用期不跑）。

## 六、纪律核对

| 项 | 状态 |
|---|---|
| 写前快照 | ✅ `7836606f03c7244e10f219a2fe3fed9f6508779b`（stash create + store） |
| edit 后 git diff 自检 | ✅ 每次实施后核 diff（+25 行纯新增，单文件） |
| 精确文件清单提交 | ⏳ 提交时执行（禁 `add -A`） |
| 策略源码零改动 | ✅ **本会话对 `strategies/` 零改动** |
| 他会话在途隔离 | ✅ `strategies/` 现 9 处改动（`fall_reversal` M、两 candidate 删除、断板反包 M + 4 report JSON + 1 `.bak`）**均非本会话**，提交时精确隔离 |
| 文件域 | ✅ 仅 `ptrade_api.py`（未触 `backtest_engine.py`，裁定①） |

## 七、待办

1. V2：选 2 个不读 `capital_base` 的代表策略，改动前后同库同窗 sha 比对；
2. V4：声明窗口后跑全库 pytest + 55 基线逐项对表；
3. V5：api_portability 6 策略；
4. 全部通过后：证据件定稿 → 精确清单提交 → 呈④ → ⑤用户确认（总调度承办）→ ⑥双推+同步门。

## 八、V2 / V5 更新（2026-10-01 晚）

### V5 api_portability 6 策略 —— **PASS**

```
python scripts/run_contract_gate.py --strategies --skip-matrix
→ 契约套件（pytest，受控文件清单 + 既有失败白名单放行）：全部通过；既有白名单无触发
→ 6 策略 api_portability 冒烟（同受控清单 -k 子集）：全部通过；既有白名单无触发
→ ===== CONTRACT GATE : PASS =====
```

附注：既有失败白名单**无触发**（受控子集内连既有失败都未出现）——纯新增改动零回归的旁证。

### V2 逐位一致 —— **采信传递证据（核验中）**

总调度裁定（2026-10-01）：双端对齐会话的 V2 **Before 侧恰为本件 `d156f98` worktree**
（断板反包 `7d7dfde6…` / 全球轮动 `f2eae7…`，与总调度 **9/27 独立基线逐位同源**）⇒ 暂定采信为**传递证据**。

| 项 | 内容 |
|---|---|
| 证据状态 | **采信传递证据**（非本会话自跑，如实声明） |
| 传递指针 | 双端对齐会话 V2（Before 侧 = `d156f98` worktree）；总调度 9/27 独立基线 |
| 生效条件 | **总调度完成其镜像工件核验**（已另令该会话补交镜像） |
| 兜底路径 | 若核验不成立或未获交 → 本会话在 daemon 空闲窗用 **worktree at `d156f98`** 自跑双策略补证 |
| 自跑能力已备 | V1 实跑得 `nav=161 trades=27 sha=9e4c123cd67a8744 wall=52s`（同一方法学） |

**核验未完成前 V2 不判 PASS。**

### V4 全库 pytest + 55 基线 —— **待窗口**

- 授权：按「daemon 生命周期跨会话占用纪律」**由本会话向会话群声明窗口**（经用户转发）；
- 现状：daemon 正忙（**PID 38284 长任务中**；总调度今晨 3 次只读连接被锁实证）；
- 窗口建议：任务间隙或夜间；**同窗总调度搭车复跑 V2 独立核验（只读、错峰）**；
- 必须项：**55 既有失败基线逐项对表**（零新增）；
- 若窗口内仍遇锁失败：按双端对齐会话先例采用「**唯一变量对照法**」+ 释放窗补跑条款。
