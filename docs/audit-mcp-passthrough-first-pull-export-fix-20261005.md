# 审计结论：MCP passthrough 首轮全量拉取修复方案（六步流水线·步骤 2）

- 审计对象：`docs/mcp-passthrough-first-pull-export-fix-design.md`（2026-10-05）
- 审计基线：`docs/handoff/baseline-20261005-0209-mcp-passthrough-fix.txt`
- 审计时间：2026-10-05 02:16（dsh 会话 / 只读审计）
- 审计性质：**只读**。本轮未修改任何代码、配置、测试；未触碰其他会话未提交改动。
- **总判定：需修订后复审（Conditional Pass）**——必须修正项 M1–M9 修订完成并复审通过后，方可进入步骤 3 实施。

## 0. 基线与行号有效性核验（前置结论）

| 项 | 核验结果 |
|---|---|
| 工作区未提交改动 | 与本基线一致（`git status --porcelain` 复核），`mcp_adapter.py` M、`writers.py` M 等仍为他人改动 |
| 他人改动是否污染本方案锚点 | **否**。`git diff -- mcp_adapter.py` 显示他人 +33 行全部落在 `:2267` 之后的 `stock_dividend` 写入看门狗（W1/W6 审计澄清项），本方案全部锚点 ≤ `:1926`，**行号全部仍有效** |
| `writers.py` +784 行 | 与本方案无关，未卷入 |
| 审计动作 | 仅 Read / Grep / 只读 Python 遍历，**零写入**（除本审计文档） |

---

## 1. 根因证据核验（审计重点 1）

### R1 —— F-1 根因（export 宽文本路径三处未贯通）：**✅ 成立，行号有效**

| 方案主张 | 代码证据 | 结论 |
|---|---|---|
| ① 未传 `async_mode` | `mcp_adapter.py:580-583` → `export_dataset(dataset_id=..., page_size=50_000, time_start=None, time_end=None, row_limit=None)` 无 `async_mode` | ✅ 成立 |
| 开关只在 3 处被读 | 全项目传 `async_mode` 的位置仅 `:1228` / `:1619` / `:1926`（`_fetch_export_direct` / 缓存路径 / 流式路径），`:581` 不在其中 | ✅ 成立 |
| ② `time_start/time_end/row_limit` 全 None | 同上 `:583` | ✅ 成立 |
| ③ 不消费 `hint/suggested_shards` | `errors.py:74-91` `MCPExportBudgetError(hint, suggested_shards)`；`client.py:840-846` 抛出；`_fetch_export_passthrough`（`:573-592`）无任何 `except` | ✅ 成立 |
| 配置事实 | `config/profiles/mcp_only/sources_config.json` → `sources.mcp.export_async = true`（附 `_note_export_async`） | ✅ 成立 |
| 佐证（同族表成功） | 复盘：13 shards / 129,977 行成功 | ✅ 成立 |

> 未被当前改动推翻。另核：`self.export_async = bool(config.get("export_async", False))`（`mcp_adapter.py:394`）默认 False，与方案 §3.1「默认 False=旧行为不变」一致。

### R2 —— F-2 根因（fetch_page 无下推 + 超时无退避）：**✅ 成立，行号有效**

| 方案主张 | 代码证据 | 结论 |
|---|---|---|
| ① `_fetch_all_pages(qdb_table)` 无过滤 | `mcp_adapter.py:604`；`_fetch_all_pages` 定义 `:539-568`，`fetch_page` 仅传 dataset/cursor/page_size（`:551-552`） | ✅ 成立 |
| 客户端裁剪在拉完之后 | `:605-615`（`_norm_date` / `date|trade_date|cal_date`） | ✅ 成立 |
| ② `call_timeout` 固定 90s | `client.py:265` `call_timeout: float = 90.0`；`:267` `backoff_sec=(30,60,120,240,480)` | ✅ 成立 |
| ③ 该表无 export 退避通道 | `_WIDE_TEXT_PASSTHROUGH`（`:238-244`）不含 `ws_asfund`，路由走 `:518` | ✅ 成立 |
| 5 次重试空转 | 复盘日志：5 次 attempt 全超时，902.4s；`DEFAULT_RETRY_BUDGET_SEC=900.0`（`client.py:173`）与 ~900s 吻合 | ✅ 成立（机制补充见 C2） |

### R3 —— 任务级执行参数未贯通 MCPClient：**✅ 成立，行号有效**

| 方案主张 | 代码证据 | 结论 |
|---|---|---|
| client 惰性构造只传 3 参 | `mcp_adapter.py:432-444`（endpoint / tls_verify / api_key） | ✅ 成立 |
| `configure_execution` 设的参数对 MCP 是死参数 | `base.py:79-106` 设 `self.retry_max` / `self.retry_backoff_sec` / `self.call_timeout` / `self.rate_limiter.*`；**`mcp_adapter.py` 全文件对这四个属性 0 次引用**（grep 无命中） | ✅ 成立 |
| MCPAdapter 未覆写 | 全文件无 `configure_execution` 定义 | ✅ 成立 |
| 「任务级 call_timeout 覆写配置未找到」 | `collector_tasks.json` 88 个任务中 `call_timeout` 键 **0 个**；`sources_config.json` mcp 段无 timeout/retry 键 | ✅ 成立 |

---

## 2. 必须修正（M1–M9）——阻断级，修订后方可实施

### M1 【致命】§3.2 降级触发的异常类型与实际不符，降级将永不触发

- **证据**：`_call_with_retry` 的单次超时由 `_run_bounded` 抛 `TimeoutError`（`client.py:528`），但该异常被 `:570 except Exception` 捕获进入重试；循环结束后走 `client.py:596` 抛 **`MCPTransportError(f"MCP 重试 {n} 次仍失败: {last_err}") from last_err`。** 即 `max_attempts=1` 时上层看到的是 `MCPTransportError`，`TimeoutError` 只存在于 `__cause__` 链里。
- **实机佐证**：复盘 F-2 堆栈终态为 `quantstudio.pipeline.mcp.errors.MCPTransportError: MCP 重试 5 次仍失败: _call_tool 超过 89.546s 未返回`（`__cause__`=TimeoutError）。
- **风险**：方案 §3.2 写「若底层为 `TimeoutError` / `MCPRetryBudgetExhausted` → 降级」，照此实现，**降级分支永远进不去，F-2 原样失败**。
- **修正建议**（二选一，写死进方案）：
  - (a) `max_attempts=1` 时**直接 re-raise `last_err`**（保留 `TimeoutError` 类型），不构造 `MCPTransportError`；或
  - (b) 判定条件改为「`MCPTransportError` 且 `__cause__`/`__context__` 链中存在 `TimeoutError`，或异常为 `MCPRetryBudgetExhausted`」。
  - 并在 `tests` 中显式断言：模拟首页超时后 `_fetch_passthrough` 触发 export 降级（用真实异常类型构造，不用手写 TimeoutError 冒充）。

### M2 【致命】降级判据过宽会把「可自愈的连接错误」拉去走未验证通道，直接违反「成功路径零改动」

- **证据**：`MCPRetryBudgetExhausted` 继承 `MCPTransportError`（`errors.py:26`）；`MCPTransportError` 同时覆盖连接重置/非 2xx（`client.py:576-588` 的 P3-2 10053/ECONNRESET 场景，重试前会 `_reset_connection` 重握手后**成功**）。
- **风险**：方案 §3.2 第 3 点只排除 `MCPAuthError` / `MCPProtocolError`。若按此实现，一个本来靠「重连+重试」在第 2~5 次成功的任务会被判定为降级，改走**从未在该数据集上验证过的 export 通道** → 行数/列序可能与 fetch_page 不一致 → 黄金对比失败。这与 §3.2 第 4 点「成功路径零改动、72 个成功任务行为不变」自相矛盾。
- **修正建议**：降级判据**收敛为「真超时」族**——仅 `TimeoutError`（含 `__cause__` 链）与 `MCPRetryBudgetExhausted`；其余 `MCPTransportError`（连接重置、非 2xx）保留既有 `retry_max=5` + 重连重试语义，不降级。

### M3 【致命】§3.3 同步 `rate_per_min` 会改变**成功路径**，且与「成功路径零改动」直接冲突

- **证据**：
  - `collector_tasks.json` 实测 `rate_limit.calls_per_min` 分布：**85 个任务 = 30，`mcp_etf_minutes`/`mcp_stock_minutes` = 1，`mcp_broker_recommend_pt` = 26**；
  - `MCPClient` 默认 `rate_per_min=200`（`client.py:269`），且 `_acquire_rate()` 在**每次调用前**执行（`client.py:319-324`，被 `:553` 调用——在 `try` 内、业务调用**之前**）；
  - 该 client 还被 `MCPUpdateDetector` 共用（`daemon.py:2518`）。
- **风险**：一旦同步，全部 MCP 工具调用被限到 30 次/分钟（分钟表任务 1 次/分钟＝每次调用前 sleep 60s），跑批耗时与 A4 探测一并被拖垮。这是**成功路径**行为变更，铁律「性能优化不得改变引擎行为」「纯增益」均不允许。
- **修正建议**：从 §3.3 同步清单中**剔除 `rate_per_min`**；若确需贯通，必须限定为「任务显式声明且取值 ≥ client 当前值」才同步，并在 §4 影响面单独列项。

### M4 §3.3「MCPClient 构造参数默认值不变 → 旧行为保留」不成立（backoff 序列会被改写）

- **证据**：`base.py:70-72` 默认 `retry.max=5`、`backoff_sec=(60,120,240,480,960)`；`collector_tasks.json` 88/88 任务显式配置 `backoff_sec=[60,120,240,480,960]`。而 `client.py:267` 默认 `(30,60,120,240,480)`。且 `base.configure_execution` 每个任务边界都会先 reset 回 base 默认值（`base.py:88-92`），daemon 复用同一 adapter 实例（`daemon.py:209`、`:2495-2519`）。
- **风险**：同步后 88 个任务的失败退避序列由 30/60/120/240/480 变为 60/120/240/480/960，最坏退避总时长显著拉长（`retry_budget_sec` 默认 900s 边界下的行为随之变化）。方案 §3.3 与 §4.3 只笼统说「仅影响失败后的退避时长」，未量化、未列入验收。
- **修正建议**：① 明确「仅当任务显式声明时才覆盖 client；未声明则回落 client 自身默认（30,60,120,240,480）」；② 若坚持按 base 默认同步，必须在 §4 影响面写明「88 个任务失败退避序列由 X 变为 Y」并在 §5 增加对应回归断言。

### M5 §6 回退条件不成立 / 不可执行

- **证据**：
  - 3.1 的「窗口下推」与「分片收缩」是纯代码行为，**不受 `export_async:false` 控制** → 「配置回退 → 恢复修复前通道行为」不成立；
  - 3.2 的降级开关 `passthrough_export_fallback` 在 §3.4 中仅为「如审计要求可加」，**未进入 §3 改动文件表** → 现状无开关可关；
  - 「按精确文件清单 `git revert`」在**共享工作区且他人未提交改动**的场景下不可执行：`git revert` 作用于 commit，文件级 revert 会抹掉他人 `mcp_adapter.py` +33 行。
- **修正建议**：① 3.1 窗口下推、3.2 降级各自挂到**显式配置键（默认 true）**，使配置回退名副其实；② 回退动作改为「实施前 `git stash create -u` + `git stash store` 回退点 + 文件级定向恢复」，删除 `git revert` 表述；③ 回退判据补充一条：任一大表出现「降级后行数 ≠ 未降级行数且无法归因」即回退。

### M6 §3.1 窗口下推的「不改变覆盖范围」是无证据前提，可能静默改变行数

- **证据**：`_fetch_export_direct` 的窗口口径（`mcp_adapter.py:1209-1214`）只在**映射表**路径验证过；passthrough 数据集的服务端时间过滤列与语义**无任何本地证据**——复盘「五、缺失信息」明确列出服务端侧行为未获取，`suggested_shards` 实际值也未落盘。
- **风险**：若服务端对 passthrough 数据集的 `time_start/time_end` 不作用于 `date/trade_date` 列（或该列类型不同），下推会静默改变结果集，极端情况 0 行——而 passthrough 是 `CREATE OR REPLACE` 全量覆盖语义，0 行 = **直接清空目标表**。
- **修正建议**：① 实施前先跑一次「带窗口 vs 不带窗口」双跑行数比对探针，作为准入证据写入 `docs/evidence/`，不通过则仅保留 async 不下推窗口；② 代码内加保护：带窗口返回 0 行而不带窗口 >0 行时 WARNING + 回落不带窗口；③ 把「6 张宽文本表：窗口下推行数 == 不带窗口行数」列为 §5 硬性前置项，而非只比对 10-04 黄金。

### M7 §3.2 探测范围未限定 → 改动外溢到 4 个无关调用点

- **证据**：`_fetch_all_pages` 共有 5 个调用点——`:604`（passthrough）、**`:652`（`_fetch_small_table`）**、**`:2137`（stock_basic）**、**`:2158`（etf_basic）**、**`:2179`（index_daily）**。
- **风险**：若把「首页 `max_attempts=1` 探测」实现在 `_fetch_all_pages` 内部，`_fetch_small_table` 与三个 basic/index 辅助路径会被一并改变，属范围外溢，违反「纯增益、不修一送一」。
- **修正建议**：探测**仅作用于 `_fetch_passthrough` 链路**——为 `_fetch_all_pages` 新增可选参数 `max_attempts`（默认 `None` = 逐行等效旧行为），由 `:604` 显式传 1；并在 §5 验收列出这 4 个调用点的「调用序列不变」回归断言。

### M8 §3.1 子窗聚合缺少「半开区间不变量 / 去重」约束，存在静默重复行风险

- **证据**：3.1 第 3 点「逐子窗异步重取…聚合 frames」；子窗来源为服务端 `suggested_shards`（值未知，复盘列为缺失信息）或本地二分。passthrough 为 `CREATE OR REPLACE` 全量覆盖（`daemon.py:988-991`）。
- **风险**：子窗若重叠（服务端建议窗口重叠，或二分未严格半开），concat 后产生重复行，直接污染目标表；黄金对比只在「行数增加」时才可能暴露。
- **修正建议**：① 明确所有子窗统一为半开区间 `[start, end+1day)`（与 `:1213-1214` 同口径）；② 聚合后断言 `sum(子窗 rows) == manifest.total_rows`，或对全列去重；③ 单测构造「重叠 suggested_shards」用例验证不重复。

### M9 配置契约缺口：`call_timeout` 0/88 任务存在，R3 的核心落点未兑现

- **证据**：`collector_tasks.json` 88 个任务中 `call_timeout` 键 **0 个**。而方案 §3.3 论证是「任务级 `call_timeout` 从此可配置，用于特殊大表放窗口」，§3 改动文件表却把 `collector_tasks.json` 标为「可选」。
- **风险**：按现状实施，R3 只兑现了 backoff 贯通，`call_timeout` 仍恒为 90s——「F-2 的 90s 硬超时无解」这一放大因素实际上没有被消除，方案自证目标落空。
- **修正建议**（二选一并写死）：① 把「为 `mcp_ws_asfund_pt` 等大表新增 `call_timeout`」列入必改清单并给出具体取值；或 ② 在 §2 R3 与 §3.3 明确「本次仅做贯通机制，**不改任何任务配置**，`call_timeout` 仍为 90s 默认；F-2 的解决完全依赖 3.2 的 export 降级」，避免论证与交付不一致。

---

## 3. 建议补充（S1–S10）——不阻断，但应并入修订版

| # | 问题 | 证据 | 建议 |
|---|---|---|---|
| S1 | export 通道无 `codes` 过滤，与 fetch_page 路径语义不一致 | `_fetch_export_passthrough`（`:573-592`）无 `codes` 参数；`_fetch_passthrough` 有（`:616-623`） | 当前 69 个 passthrough 任务 `codes` 全为 `["ALL"]` 故无实际差异；建议加守卫：降级前若 `codes` 非 ALL → WARNING（或在 export 路径补同口径过滤） |
| S2 | 降级路径 meta 缺 `source/freq/table/upstream_authority/lineage` | 对比 `:624-639`（fetch_page）vs `:590-591`（export，仅 3 键） | 已核实 daemon 未消费 `meta.lineage`（权威守卫走配置，`daemon.py:831-839`），风险低；仍建议明确「降级 meta 至少补齐 source/table/freq/fetch_mode=export_fallback」 |
| S3 | `_call_with_retry` 新增 `max_attempts` 的位置未规定 | 现签名 `def _call_with_retry(self, fn, *args, **kwargs)`（`client.py:533`），kwargs 会原样透传给 `_call_tool` | 必须写成 `def _call_with_retry(self, fn, *args, max_attempts=None, **kwargs)`（置于 `*args` 后即自动关键字专用）；验收断言 `fetch_page` 发出的 args 不含 `max_attempts` |
| S4 | 「首页探测」的边界未声明 | `_fetch_all_pages` 为 while 循环（`:550-565`） | 明确「探测仅覆盖首页；第 2 页起保持 `retry_max=5` 旧行为（有意为之，防外溢）」，并把 F-2 <300s 建立在「首页即超时」前提上 |
| S5 | §5.3 实机复验口径不可判定 | 复盘：74 distinct task / 76 行，success 59 / empty 15 / failed 2，`mcp_trade_calendar` 计 2 条 empty | 钉死为：success ≥59、empty 集合与 20261004 跑批**逐项一致**、failed=0 |
| S6 | §5.5「F-2 <300s」受服务端负载影响 | — | 标注为观测指标（3 次跑批中位数），并写明构成 = 首轮探测 90s + export 异步等待 |
| S7 | `call_timeout=0` 是陷阱值 | `base.py:106` 允许 `max(0.0, …)`；`client.py:524-528` `join(0)` 会**立即**抛 TimeoutError | 同步时做 `>0` 校验/夹取，并在方案中标注取值域 |
| S8 | 命名冲突：代码注释称宽文本 export 路径为「F-2 路径」 | `mcp_adapter.py:503/511/575`（历史「F-2 修复 2026-09-08」）与本轮失败编号 F-1 撞名 | 方案与实施注释统一标注「本轮 F-1 / 历史 F-2（宽文本路由）」避免后续审计误读 |
| S9 | R2.1 未被直接修复 | fetch_page 通道本身仍无服务端过滤下推 | 在 §4 写明残余风险：export 不可用的数据集仍会撞 R2.1 |
| S10 | §3.3 影响面未覆盖非 passthrough 与非 adapter 消费者 | `MCPUpdateDetector(self._adapters[source].client)`（`daemon.py:2518`） | 把「88 个 mcp 任务 + 更新探测器共用 client」列入 §4 影响面 |

---

## 4. 已确认（C1–C8）——无需修订

| # | 确认项 | 证据 |
|---|---|---|
| C1 | R1 三项全部成立且行号有效 | `mcp_adapter.py:580-583` / `:1228/:1619/:1926` / `errors.py:74-91` / `client.py:840-846` / `sources_config.json export_async=true` |
| C2 | R2 成立，并补充真实机制：总耗时 ~900s 由 `DEFAULT_RETRY_BUDGET_SEC=900.0`（`client.py:173`）封顶，而非方案写的「450s 退避 + 5×90s」巧合 | `client.py:173/265/267/528/596`；复盘 F-2 时间轴 |
| C3 | R3 成立，`retry_max/retry_backoff_sec/call_timeout/rate_limiter` 在 `mcp_adapter.py` 确为死参数（grep 0 命中） | `base.py:79-106` vs `mcp_adapter.py` 全文件 |
| C4 | 他人改动不污染锚点，行号有效 | `git diff` 显示 +33 行位于 `:2267` 之后；本方案锚点 ≤ `:1926` |
| C5 | 修复 1 所需的底层能力**已全部就位**，无需动服务端/协议 | `async_mode` 支持 `client.py:1003-1042`；`get_manifest(await_ready=True)` 轮询 `client.py:855-882`；ISO 窗口口径可复用 `mcp_adapter.py:1209-1214` |
| C6 | 配置事实核对完毕 | `sources_config.json`：`export_async=true`，`export_wide_text` 未配置（默认 True，`:509-510`）；`collector_tasks.json`：88 任务 / 69 passthrough / codes 全 ALL / retry.max 全 5 / backoff 全 (60,120,240,480,960) / call_timeout 0 个 |
| C7 | 「既有 mcp 测试 10 个文件」属实，新增测试无重名冲突 | `tests/test_mcp_*.py` 实测 10 个；`test_mcp_passthrough_export_fallback.py` 不存在 |
| C8 | 「不改 `daemon.py`」成立 | `configure_execution` 在 fetch 前调用（`daemon.py:984`），start/end 已透传（`:1420-1422`、`:1430`） |

---

## 5. 审计重点对照总表

| 审计重点 | 结论 |
|---|---|
| 1. 根因证据是否成立 | **成立**。R1/R2/R3 逐条比对实际代码，行号全部有效，结论未被当前改动推翻（C1–C4）。补充：R3 死参数、`retry_budget` 900s 真实机制 |
| 2. 修复覆盖度 | **基本覆盖，但有 2 处外溢/缺漏**：R1 ✅全部覆盖；R2 仅「间接」覆盖（R2.1 未直接修复，S9）；R3 覆盖不全（`call_timeout` 无配置可配，M9）。范围外溢 2 处：`rate_per_min` 同步（M3）、探测可能外溢到 `_fetch_all_pages` 另 4 个调用点（M7） |
| 3. 兼容性论证 | **不成立**，须修订。`max_attempts=1` 的降级触发类型与实际抛出的 `MCPTransportError` 不符（M1）；判据过宽会把可自愈连接错误拉去降级（M2）；`rate_per_min` 同步破坏成功路径零改动（M3）；backoff 默认值改写（M4）。「通用覆盖任意表」仅在**真超时**前提下成立 |
| 4. 配置契约 | **部分对齐**。`retry.max` / `retry.backoff_sec` 与 `base.py:101-104` 一一对齐 ✅；`call_timeout` 与 `base.py:105-106` 对齐但 **0/88 任务配置**（M9）；`rate_per_min` 与 client `_acquire_rate` 语义**不对齐**（M3）；`call_timeout=0` 取值域缺保护（S7） |
| 5. 可验收性 | **部分可判定**。单测项（§5.1/5.2）可判定 ✅；实机复验「empty 口径与上次一致」不可判定（S5）；耗时 <300s 受外部负载影响（S6）；回退条件不可执行（M5）；窗口下推零影响缺前置证据（M6） |

---

## 6. 复审要求（修订后需重新提交）

1. M1–M9 逐条在方案文档中给出明确修订文本（不接受「实施时注意」这类口头承诺）；
2. M1 需在方案中给出**可断言的异常契约**（类型 + `__cause__` 链），并对应到一条单测；
3. M3/M4 需在 §4 影响面给出**量化后的行为差异清单**；
4. M5 需给出可执行回退步骤（配置键 + stash 回退点 + 文件级定向恢复）；
5. M6 需给出「带窗口 vs 不带窗口」探针的**准入判据**（不通过则不下推窗口）。

---

## 7. 本轮审计的纪律声明

- 只读审计，**未修改任何代码 / 配置 / 测试**；
- 未执行任何 `git` 写操作（未 stash / 未 checkout / 未 revert），他人未提交改动（`mcp_adapter.py` +33、`writers.py` +784 等）**完整保留**；
- 本轮唯一新增文件：本审计文档。
