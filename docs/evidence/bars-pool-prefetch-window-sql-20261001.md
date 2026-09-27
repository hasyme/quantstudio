# bars 池预取窗口 SQL 化 · 实施与验收证据（六步④，2026-10-01）

- **性质**：纯性能型（可观察结果不变；P2 逐位 0 不一致为证）
- **落点**：`quantstudio/backtest/providers/duckdb_data_access.py`（**单文件**）
- **回退点**：`160212e99da69abff77ab65197db4cff6e9e7ad1`（stash create + **store 持久化**）
- **方案件**：`docs/backtest-bars-pool-prefetch-window-sql-design.md`（六步②审计通过，含三条硬要求）

## 一、实施清单（5 处 edit，+117 / −63）

| # | 改动 | 说明 |
|---|---|---|
| 1 | **新增** `_query_bars_window_sql`（+76） | 自 impl 的 SQL 分支**逐行搬运**（主表路由 stock→etf→index + INDEX_ETF_MAP fallback + 同款 `_post`）——单一实现 |
| 2 | impl 头部**分流**（+5） | `if self._use_sql_path: return self._query_bars_window_sql(...)`（回滚开关语义保持） |
| 3 | 删三表循环内 SQL 分支（−~35） | 仅留 PR7 内存路径（缩进回收） |
| 4 | 删 fallback 内 SQL 分支（−~28） | 同上 |
| 5 | **池预取两处接入**（2 行） | 首次预取 + 扩窗：`_query_bars_by_count_batch_impl(pool, W, …)` → `_query_bars_window_sql(pool, W, …)` |

**纪律执行**：写前快照（create + store）✓；精确清单 ✓；**edit 后即时 `git diff` 自检** ✓
（语法有效 / 新方法类内缩进正确 / 池预取接入 2 处 / 分叉残留 0 / `_ensure_bars_in_cache` 仅存内存路径）。

## 二、验收结果（V0-V5 硬门）

| 门 | 判据 | 实测 | 结论 |
|---|---|---|---|
| **V0 生效** | 池预取不再触发全历史加载；缓存非空 | `_bars_history_cache` 条目 **0 / 0 / 0**（3 跑）；`_bars_window_cache` **1000** 条 | ✅ **PASS** |
| **P1/P2 数据层逐位** | 池切片 vs 直连 SQL，0 不一致 | **2000 项比对（1000 码 × count∈{5,27}）→ 不一致 0**；行数 29,994 两侧一致 | ✅ **PASS** |
| **V2 nav_sha 逐位** | 双策略（断板反包 + 全球资产轮动）A/B 镜像 sha 一致 | 断板反包 `7d7dfde64a87caaa` = `7d7dfde64a87caaa`；全球资产轮动 `f2eae7039074f2c3` = `f2eae7039074f2c3`（nav/trades g17 全精度镜像） | ✅ **PASS** |
| **横验多策略** | `run_contract_gate.py --strategies` ⇒ PASS | **`CONTRACT GATE : PASS`**（矩阵门一致 + 契约套件零触发 + 6 策略 api_portability 冒烟全过） | ✅ **PASS** |
| **pytest** | 相关套件全绿 + 既有失败基线复现 | **48 passed + 1 既有失败**（`test_provider_frequency_routing::…does_not_touch_minute_query`：mock lambda 签名不匹配；**改造前 worktree 对照同样失败** ⇒ 既有，非本件引入） | ✅ **PASS（按基线复现条款）** |
| **V5 性能** | 池预取 1000 码 ≤1.5 s/日（协议化） | **6.145s → 1.118s（5.50×，降幅 81.8%）** | ✅ **PASS（超目标）** |

### 2.1 V5 测量协议（按审计硬要求②声明）

- **口径**：池 1000 码（影子库 `agent_workspace/backtest_readonly/quantstudio.db`）· `bms = 1788537600000` · `W = max(count=5, _POOL_W_MIN=30) = 30` · `use_qfq=True`；
- **缓存状态**：**冷启**（每跑**新建实例**，模拟每日首请求的真实场景）；
- **采样**：**3 跑取中位**；
- **两侧定义**：「改造前」= 内存路径全池预取（`_use_sql_path=False` 调 impl，等价改造前池预取行为）；
  「改造后」= 生产路径（`set_pool(pool)` + 首请求触发池预取）；
- **实测**：改造前 `[6.145, 6.604, 5.396]` ⇒ 中位 **6.145s**；改造后 `[1.118, 1.153, 1.038]` ⇒ 中位 **1.118s**；
- **口径漂移说明**（审计指出）：预证阶段曾出现「双形态池 5.247s vs 单形态 6.478s」的非单调现象——
  机理为**首次调用吸收一次性开销**（连接/缓冲/表元数据/路由缓存，与 P3 内存预证同款现象）；
  本 V5 已按协议以**冷启 + 3 跑中位**消除该漂移，−81.8% 结论在该协议下成立。

## 三、V2 nav_sha 逐位（A/B worktree 方法学）

- **Before**：`agent_workspace/_wt_before_bars`（`git worktree add … HEAD --detach` ⇒ `d156f98`，
  **不含本件改动**，已核 `_query_bars_window_sql` 0 处）；**After**：主工作区（含本件改动）；
- **口径**：`scripts/ab_perf_chain_runner.py`（nav/trades **g17 全精度字符串镜像** + sha256[:16]），
  同窗同参（`2026-01-01 ~ 2026-09-01`、capital 100000、`match_price_mode=close`、`engine_profile=daily-bar-v1`、
  `rebalance_mode=legacy`、同 TradeCost），**主库只读**；
- **策略**：`断板反包策略.py`、`全球资产轮动.py`（审计硬要求①：双策略必含）。

| 策略 | Before sha | After sha | nav/trades 数 | 结论 |
|---|---|---|---|---|
| 断板反包策略 | `7d7dfde64a87caaa` | `7d7dfde64a87caaa` | 161 / 29 | ✅ **逐位一致** |
| 全球资产轮动 | `f2eae7039074f2c3` | `f2eae7039074f2c3` | 161 / 9 | ✅ **逐位一致** |

⇒ **V2 PASS**（双策略 nav/trades g17 全精度镜像 sha 逐位一致 ⇒ 纯性能型成立）。

### 3.1 wall 对照与如实归因

| 策略 | Before wall | After wall | 差 |
|---|---|---|---|
| 断板反包策略（161 交易日） | 1018s | **932s** | **−86s（−8.4%）** |
| 全球资产轮动 | 92s | 88s | −4s |

**归因说明（不夸大）**：端到端 wall 降幅（−8.4%）**远小于**池预取单件降幅（−81.8%），原因：
1. 改造前 wall ≈ 6.3 s/日（1018s/161 日），池预取在其中的占比已随前序优化（T1 键维度 + 全池共享 +
   F-DUCKDB-LOCK 分片等）显著下降，不再是大头；
2. 池预取仅在**当日首次单码请求**时触发一次（161 日 × 1 次/日），单次节省 ≈0.53 s/日（实测口径下）；
3. 其余 wall 由策略侧 numpy 计算 + 引擎固定开销构成（与 bars 取数无关）。
⇒ 本件价值以**池预取单件口径**（5.50×/1000 码）主要计量，端到端 wall 改善为辅证。

## 四、横验多策略（contract gate）

`python scripts/run_contract_gate.py --strategies` ⇒ **`CONTRACT GATE : PASS`**（exit 0）：

| 段 | 结果 |
|---|---|
| 契约矩阵门禁 | **通过**（哈希一致 + MD 一致） |
| 契约套件（pytest 受控清单） | **全部通过；既有白名单零触发** |
| **6 策略 api_portability 冒烟** | **全部通过；既有白名单零触发** |

## 四.1 pytest 全库跑（占用已声明：只读回退型跑，不涉停/启/接管 daemon）

| 侧 | 结果 |
|---|---|
| **After（主工作区，含本件改动）** | **83 failed / 3180 passed / 8 skipped / 8 xfailed** |
| Before（改造前 worktree） | 87 failed / 3110 passed / 47 skipped / 9 errors |

### 归因闭环（决定性）

**① 21 项「After 有 / Before 无」= 主库被 daemon 占用（环境，非本件）**

- 失败日志实拍：`QS_DUCKDB_CONN_UNAVAILABLE … IOException: Cannot open file "…\data\quantstudio.db": 另一个程序正在使用此文件 … PID 38284`；
- 占用者身份核实：**PID 38284 = `quantstudio.pipeline.daemon --mode forever --config-dir config/profiles/mcp_only`**（自 2026-09-27 03:01 长驻）⇒ 按「daemon 生命周期跨会话占用纪律」**避让，不停/不接管**；
- 两侧不可直接对比之因：测试硬编码 `parents[1]/data/quantstudio.db` ⇒ 主工作区（DB 存在但被锁 → **failed**）vs worktree（无 DB 文件 → `skipif` **skipped** 29 项）。

**② 唯一变量对照（决定性证据）**：worktree 内挂**影子库硬链接**（同 DB、同环境），仅切换代码：

| 条件（同 DB = 影子库 · 同环境） | 结果 | 失败项 |
|---|---|---|
| Before 代码（改造前） | **3 failed / 79 passed** | industry_classification_sw2021 / qfq_range golden 数值 / security_metadata golden |
| **After 代码（本件）** | **3 failed / 79 passed** | **完全相同的 3 项（逐项一致）** |

⇒ **本件零新增失败**（唯一变量对照失败集逐项一致）；3 项为**影子库快照差异**（既有，与主库口径差异）；
21 项在主工作区的失败唯一成因为 **DB 锁环境**。

**③ 其余 62 项** = 两侧失败集**交集**（既有失败，含白名单 5 项）。

**④ 本件相关套件**（`test_duckdb_data_access_caching` / `test_pr7_bars_cache_equivalence` /
`test_get_history_include_cache` / `test_providers` / `test_duckdb_preload_code_index` /
`test_provider_frequency_routing` / `test_sw_index_history_routing` / `test_duckdb_version_gate`）：
**48 passed + 1 既有失败**（`test_provider_frequency_routing::test_get_bars_daily_frequency_does_not_touch_minute_query`，
mock lambda 签名不匹配；**改造前 worktree 对照同样失败** ⇒ 既有）。

**环境限制声明**（照 pool-shared 先例条款）：主库被 daemon 长期占用期间，全库跑含 DB 依赖项的失败
以**唯一变量对照法**为主验收依据，并在 daemon 释放窗口后补跑归档。

## 五、边界确认（审计硬要求③）

- **不改**：按需路径（池缺失 / 非池 code，仍走 PR7 内存路径）；公共 API 签名/返回字段/列序/dtype/index 契约；
  日期边界与 PIT；复权口径；分钟路径；`_ensure_bars_in_cache` 语义（其他消费方不受影响）；**策略源码零改动**；
- **单文件**：`duckdb_data_access.py`（+117/−63）；与块一（capital_base，`ptrade_api.py` 域）**无重叠**；
- **单 commit 可 revert**：改动限于「一个新方法 + 一波分流 + 两处调用」，`git revert` 即回退。

## 六、backlog 处置（随实施 commit 同批，审计裁定）

`docs/evidence/backlog-bars-cache-key-normalization-20260926.md` 追加「预证结案」节：
原两子项（键规范化 / 单形态预取）经预证**归因推翻**（收益≈0 且前提不成立），真瓶颈另修（本件）。