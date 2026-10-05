# 修复方案 v4：MCP-only 采集 passthrough 首轮全量拉取失败（F-1/F-2）——六步流水线·步骤 1（第四版，审计收口）

- 方案日期：2026-10-05（v4 定稿：2026-10-05 02:53；v3→v4 响应定点复核 R-1~R-7）
- 审计链：v1 → 审计 `docs/audit-mcp-passthrough-first-pull-export-fix-20261005.md`（M1–M9/S1–S10）
  → v2 → 复审 `docs/reaudit-mcp-passthrough-first-pull-export-fix-v2-20261005.md`
  （❌ 2 严重 + 7 中等 + 6 轻微）→ v3 → 定点复核
  `docs/recheck-mcp-passthrough-fix-v3-20261005.md`（⚠️ 有条件通过：补 R-1 即放行，无需再复审）
- 依据复盘：`output/mcp_only_collector_run_20261004_214251/failure_review.md`
- 适用铁律：框架层改动六步流水线；「框架问题立即解决」；「全链路修复仅限框架层·纯增益」；
  「性能优化不得改变引擎行为」；MCP 全数据源替代任务进度报告实时更新。
- **v4 状态：步骤 1 修订完成；步骤 2 审计收口（有条件通过，R-1 已补 + R-2~R-7 同批修订）；**
  **步骤 3（实施）已解锁，待启动。**

> **v4 修订总览**（本轮 R-1~R-7）：R-1（分片收缩 = 窗口下推的一种形式，仅白名单内表可用，
> 非名单表 budget error 直接上抛 + 父窗定义 + §5.1 用例）；R-2（「三分支」→「五出口」措辞）、
> R-3（兜底举例分类更正）、R-4（裁剪/meta 影响点补列）、R-5/R-6（重连次数与尝试次数口径量化）、
> R-7（名单元素命名）同批修订。
> v2→v3 修订（B1 三处写死、B2 白名单制、M-1~M-7、m-1~m-7）全部保留生效。

---

## 1. 问题定义

2026-10-04 21:42 → 2026-10-05 01:46 的 mcp_only 采集跑批（库内 88 任务，启用 74）2 任务失败：

| 失败任务 | 目标表 | 取数通道 | 直接错误 |
|---|---|---|---|
| F-1 `mcp_llm_text_events_enriched_pt` | `llm_text_events_enriched` | export Parquet（宽文本路径） | `MCPExportBudgetError: export_exceeds_time_budget`（服务端 60s 软预算） |
| F-2 `mcp_ws_asfund_pt` | `ws_asfund` | fetch_page cursor 分页 | `MCPTransportError: MCP 重试 5 次仍失败: _call_tool 超过 89.5s 未返回`（`__cause__`=TimeoutError，~900s 由 `DEFAULT_RETRY_BUDGET_SEC=900.0` 封顶） |

共性：passthrough 表首次全量拉取（`last_watermark=None`），请求体量是「整表」；两条通道都没把
「时间窗口 / 异步导出 / 分片收缩」贯通到服务端。两任务均未到写入阶段，DB 未被污染。
已排除环境问题（handshake ok，同窗口 72 任务正常）与依赖问题（ConfigLint 0 错误 0 警告）。

---

## 2. 根因定位（审计确认成立；锚点复核有效，他人 +33 行在 :2267 之后不污染）

### R1 —— F-1：export 宽文本路径三处未贯通（`mcp_adapter.py:573-592`）
1. 未传 `async_mode`（`:581-583`）；`export_async` 配置仅 `:1228/:1619/:1926` 三处被读，宽文本路径不在其中；
2. `time_start/time_end/row_limit` 全 None，服务端整表同步导出撞 60s 软预算；
3. `MCPExportBudgetError`（`errors.py:74-91`，含 `hint/suggested_shards`）在宽文本路径无任何 `except`。

### R2 —— F-2：fetch_page 无服务端过滤下推 + 超时无退避通道（`mcp_adapter.py:594-642`）
1. `:604` `_fetch_all_pages(qdb_table)` 无过滤；日期裁剪是拉完后的客户端后置（`:605-615`），
   服务端首页扫全表 >90s；
2. `call_timeout=90s`（`client.py:265`）固定；5 次重试纯重复超时，总耗时由
   `DEFAULT_RETRY_BUDGET_SEC=900.0`（`client.py:173`）封顶；
3. `ws_asfund` 不在 `_WIDE_TEXT_PASSTHROUGH`，无 export 退避通道。
   **残余风险（S9）**：R2.1（fetch_page 通道本身无服务端过滤下推）在本方案中**不直接修复**，
   export 不可用的数据集仍会撞 R2.1——见 §3.2 降级与 §4.4 残余风险声明。

### R3 —— 共性：任务级执行参数未贯通 MCPClient（`mcp_adapter.py:432-444`）
client 惰性构造只传 `endpoint/tls_verify/api_key`；`base.py:79-106` 的
`retry_max/retry_backoff_sec/call_timeout/rate_limiter` 在 mcp_adapter 内 0 引用（死参数）。
**本方案处置（M3/M4/M9）**：本方案仅贯通 `call_timeout`（机制层），不贯通 retry/backoff/rate
——量化理由见 §4.3。R3 不是 F-2 的解法（F-2 由 §3.2 降级解决），而是独立小修。

---

## 3. 修复方案（改动范围）

改动文件（框架层，策略源码零改动）：

| 文件 | 改动 |
|---|---|
| `quantstudio/pipeline/sources/mcp_adapter.py` | 主改动（3.1 + 3.2 + 3.3） |
| `quantstudio/pipeline/mcp/client.py` | `_call_with_retry` / `fetch_page` 增加 `max_attempts`（kw-only，置于 `*args` 后，S3）；**`:580` 与 `:589-595` 末次跳退避判定改用「本次调用有效 attempts 上限」**（M-2） |
| `config/profiles/mcp_only/sources_config.json` | 新增 2 个显式回退键（§3.4），`export_async: true` 不动 |
| `tests/test_mcp_passthrough_export_fallback.py` | 新增测试 |
| `docs/evidence/mcp-passthrough-window-pushdown-probe-*.md` | 探针证据（§3.0，准入前置） |
| `collector_tasks.json` | **不改**（M9 选②：0 任务配置变更） |
| `README.md` + 引用文档 | 验收后同步（见 §7） |

### 3.0 实施步骤 0（M6 准入探针，先于任何代码改动，只读；对象与 B2 白名单一致）
- 用**临时脚本**（不落库、不进 git；复用既有 `MCPAdapter.client.export_dataset` 能力）对
  以下 8 张表做「带窗口 vs 不带窗口」双跑导出（异步）：7 张宽文本表 + `ws_asfund`
  （即 §3.4 白名单的**初始候选**；宽文本表是 export 常驻路径，`ws_asfund` 是降级首触发者）。
  带窗口 = `[start_date, (跑批日+1天)T00:00:00)` 半开 ISO（与 `mcp_adapter.py:1213-1214`
  同口径，m-5）；不带窗口 = 全表；两者都经与 `_fetch_passthrough` 同口径的客户端日期裁剪后
  比对行数。
- **准入判据（逐表，M-5）**：该表「带窗口行数 == 不带窗口行数」（裁剪后）→ 该表列入
  `passthrough_export_window_tables` 名单；不等（或 0 行 / 无日期列）→ 该表**不列入**名单，
  永不窗口下推（仅 async，且不做分片收缩——分片收缩同为窗口下推的一种形式，
  R-1）。逐表结果**持久化方式 = 名单本身**（无其他运行时状态）。
- 证据落盘 `docs/evidence/`；探针不通过的表**不阻塞**其余修复（async 贯通 + 降级通道不受影响）。

### 3.1 修复 1（F-1 主修复）：export 宽文本路径贯通（异步 + 白名单窗口 + 分片收缩）

`_fetch_export_passthrough(table, freq, start, end)`（历史注释中的「F-2 路径」统一改称
「本轮 F-1 / 历史 F-2（宽文本路由）」，S8）：

1. **异步贯通**：`async_mode=bool(self.export_async)`（配置已 true；默认 False=旧行为）→
   `get_manifest(await_ready=True)` 轮询（client 既有能力）；
2. **窗口下推（白名单制，B2/M-5/M-7）**：**仅当** `table ∈ passthrough_export_window_tables`
   （配置名单，代码默认**空** = 全局不下推 = fail-safe）时，才把 `start/end` 按
   `_fetch_export_direct` 同口径转 ISO 半开区间（`start→YYYY-MM-DDT00:00:00`，`end+1day`）
   传入 `export_dataset`；不在名单 / 解析失败 → 不传（等效旧行为，记 WARNING）。
   **降级路径（§3.2）同样只对名单内表下推窗口——未探表一律不下推**；
3. **预算错误消费（M8 半开不变量；R-1 白名单门控）**：捕获 `MCPExportBudgetError` →
   **分片收缩（`suggested_shards` 消费 / 二分）视为窗口下推的一种形式，同样要求
   `table ∈ passthrough_export_window_tables`（本次请求实际携带窗口、父窗存在）才可用**；
   **非名单表触发 budget error → 不消费 `suggested_shards`、不做二分，直接上抛**
   （与 §6.1 M-4「修复前必失败」判定一致）。
   名单内：子窗来源二选一——`suggested_shards` 有效（列表非空、各窗可解析、归一为半开区间后
   **两两不交且并集覆盖父窗**）则用之；否则按父窗二分（深度上限 5、子作业上限 16）。
   **父窗定义（R-1）**：名单内 = 本次请求的 `[start, end+1day)` 半开窗口；非名单 = 无父窗
   （本次请求未携带窗口），故不存在任何子窗/二分可能。
   逐子窗异步重取；**聚合断言失败动作（M-6）**：`sum(各子窗 rows) != concat 后 rows`
   ⇒ 先按**全列去重**重试一次；仍不等 ⇒ **上抛并禁止写入**（不得落库，全量覆盖语义下
   静默写重复行 = 数据污染）；相等 ⇒ 正常聚合；
4. **语义对齐**：concat 后按 `_fetch_passthrough` 同口径客户端日期裁剪
   （`date/trade_date/cal_date` 列存在时）——裁剪是**语义权威**，下推只是性能层；
5. **0 行守卫（M6② / m-2）**：**仅当本次请求实际携带了 `time_start/time_end`** 且结果 0 行时，
   WARNING + **单次**回落不带窗口重取（防「下推 0 行 = CREATE OR REPLACE 清空表」）；
   回落结果仍 0 行 → 视为真空窗正常返回；未下推时的 0 行是合法空表，**不**触发回落；
6. meta 补齐（S2）：`source/freq/table/fetch_mode/passthrough/wide_text_export/async/window/
   subjobs/rows/lineage`；
7. `export_wide_text=False` 回落 fetch_page 的既有路由不变。

### 3.2 修复 2（F-2 主修复）：passthrough fetch_page 首页超时自适应降级 export

**总开关语义（B1，写死）**：`passthrough_export_fallback=false` ⇒ `_fetch_all_pages` 传
`max_attempts=None`（**完全不探测**），本小节步骤 1 全部出口（五出口，R-2）均不生效，
`_fetch_passthrough` 与修复前**逐行等效**（§3.4 表、§6.1 与此处三处一致）。

异常契约（M1，审计确认准确）：
- 探测经 `_call_with_retry(..., max_attempts=1)` 执行；单次超时 `TimeoutError` 被
  `client.py:570` 捕获进重试，循环结束走 `client.py:596` 抛
  `MCPTransportError(…) from last_err` —— adapter 层看到 `MCPTransportError`，
  `TimeoutError` 在 `__cause__` 链；预算耗尽直接抛 `MCPRetryBudgetExhausted`
  （`client.py:564-566` 上抛不包裹）。
- 判定函数 `_is_timeout_family(exc)`：`isinstance(exc, MCPRetryBudgetExhausted)` 为真；
  或 `isinstance(exc, MCPTransportError)` 且 `__cause__/__context__` 链（≤3 层）存在 `TimeoutError`。
- **单测必须用真实异常链构造**（模拟 `_call_with_retry` 终态），禁止手写 TimeoutError 冒充。

**零空转（M-2）**：`client.py:580` 与 `:589-595` 必须改为按**本次调用的有效 attempts 上限**
（`eff_max = max_attempts or self.retry_max`）判定——`max_attempts=1` 探针单次失败后
**不 sleep、不 reset**，直接抛终态异常（单测断言：`max_attempts=1` 路径总 sleep == 0）。

流程（`_fetch_passthrough` 内部，开关为 true 时；**探测仅覆盖首页**，S4）：

1. `_fetch_all_pages(qdb_table, max_attempts=1)`（新可选参，仅 `:604` 传 1；M7）：
   - 首页成功 → 后续页沿用旧行为（`max_attempts=None` → 本次调用有效上限 = client
     `retry_max`，与修复前逐行一致，**有意为之防外溢**，S4）；
   - `MCPAuthError` / `MCPProtocolError` → 原样上抛（不降级、不重试，与旧语义一致）；
   - `_is_timeout_family` 为真 → **降级 export**（步骤 2），不空转重试；
   - 其余 `MCPTransportError`（ECONNRESET/10053、非 2xx，M2）→ **不降级**，
     以 `max_attempts=max(1, client.retry_max-1)` 重发（m-4：`retry_max==1` 时夹取为 1，
     防 `range(0)` 零次尝试 + `last_err=None`；保留重连自愈，总尝试次数 = 修复前 5 次）；
   - **兜底（m-3/R-3）**：未经 `_call_with_retry` 包装的异常（如 `_fetch_all_pages:561-563`
     cursor 不前进 `ValueError`，在 `_call_with_retry` 之外抛出）**不降级，按既有语义上抛**；
     注：`MCPToolError` 经 `_call_with_retry` 重试后会包装成 `MCPTransportError`
     （`client.py:596`），实际落入上一「其余传输错误 → 重发」分支，不进入兜底；
2. 降级 = 复用 3.1 的 export 实现（异步 + 白名单窗口 + 分片收缩 + 0 行守卫），
   meta `fetch_mode="export_fallback"`；**窗口下推仅对 §3.4 名单内表生效（B2）**；
3. 降级结果按 `_fetch_passthrough` 同口径做日期裁剪 **与 codes 过滤**（S1：codes 非
   ALL 时 WARNING 后同口径过滤——当前全部 passthrough 任务（69 库内 / 57 启用）codes 均
   `["ALL"]`，无实际差异）；
4. 降级也失败 → 上抛降级路径错误，日志合并保留首页超时原始错误（`__cause__` 链）；
5. **成功路径零改动**：首页一次成功的表（当前走 fetch_page 的 62 张 passthrough 表实际状态）
   调用序列/返回/语义不变——修复只对「首页真超时」的表生效。

### 3.3 修复 3（R3 收窄版，M3/M4/M9/S7）：仅贯通 `call_timeout` 机制，零任务配置变更

1. `MCPAdapter` 覆写 `configure_execution(task)`：调用 `super()` 后，仅把
   **`call_timeout`** 同步给 client（client 未建 → 构造时传入；已建 → client `_lock` 内更新）：
   - 取值域守卫（S7）：`<=0` → 回退 client 默认 90（禁止 `join(0)` 陷阱值）；
   - **不同步** `retry_max / retry_backoff_sec / rate_per_min`（M3/M4，量化见 §4.3）；
2. `collector_tasks.json` **0 任务配置变更**（M9 选②）：当前 0/88 任务含 `call_timeout` 键 →
   本修复落地后行为**逐位不变**，仅兑现「任务级 call_timeout 从此可配」的机制能力；
   F-2 的解决完全依赖 3.2 降级，不依赖放大超时；
3. 共用 client 的 `MCPUpdateDetector`（`daemon.py:2518`，S10）：因无任务配置 `call_timeout`，
   探测器路径行为不变（影响面见 §4.3）。

### 3.4 配置层（显式回退键，M5/B2/M-7）

| 键 | 默认 | 语义 |
|---|---|---|
| `export_async`（既有） | true（profile 已设） | 异步导出总开关 |
| `passthrough_export_fallback`（新增） | true | 3.2 降级总开关；**false = `max_attempts=None` 完全不探测，`_fetch_passthrough` 与修复前逐行等效（B1）** |
| `passthrough_export_window_tables`（新增，**取代 v2 的布尔键**） | `[]`（空名单，代码默认**不下推**，fail-safe，M-7） | 窗口下推**白名单**（B2）：仅名单内表下推窗口；**分片收缩（suggested_shards/二分）同为窗口下推，仅名单内表可用；非名单表触发 budget error 直接上抛（R-1）**。**名单元素 = canonical `table` 名**（`fetch_table` 入参，非 `_CANONICAL_TO_QUESTDB` 映射后的 `qdb_table`，R-7）；名单 = §3.0 探针通过的表，由 profile 显式填写；未列名单一律不下推（仅 async） |

---

## 4. 影响面

### 4.1 允许改动范围
- 仅 §3 改动文件表所列；不改 `daemon.py`（`configure_execution` 已在 fetch 前调用
  `:984`，start/end 已透传）；不改策略源码；不改服务端；不改 passthrough 全量覆盖语义；
- 不改 `call_timeout` 客户端默认值（90s）、不改 client 默认 retry/backoff/rate 序列
  （`max_attempts=None` 时逐行等效）。

### 4.2 行为影响点（必须验收盯住；数字口径 M-1）
- 库内 88 任务 / 启用 74；passthrough 69（启用 57）；其中 **7 张宽文本走 export 通道**；
  **走 fetch_page 的 passthrough 表 = 62 张（库内）/ 50 张（跑批口径）**；
1. **7 张宽文本表**（含 6 张已成功表 + `enriched`）：export 调用变为「异步」；窗口下推
   仅对 §3.4 名单内表生效（名单 = 探针通过的表，探针准入判据 = 行数等价，§5.4 硬性前置）；
   **另两项成功路径新行为（R-4，现行 `:584-592` 均无）**：§3.1.4 客户端日期裁剪（与
   `_fetch_passthrough` 同口径）与 §3.1.6 meta 补齐——由 §5.5 黄金对比盯住；
2. **62 张 fetch_page passthrough 表（跑批 50）**：开关 true 时首页新增 1 次
   `max_attempts=1` 探测；首页成功 → 后续完全旧语义；首页真超时 → 降级（当前仅 `ws_asfund`
   实证触发）；开关 false → 完全不探测，逐行等效修复前（B1）；
3. **4 个无关调用点零外溢**（M7）：`_fetch_all_pages` 的 `:652/:2137/:2158/:2179`
   调用点签名/序列不变（回归断言锁定）。

### 4.3 量化行为差异清单（M-2 已定稿为确定性数值）

| 项 | 修订前（v2 方案） | 修订后（v4 定稿） | 差异说明 |
|---|---|---|---|
| `rate_per_min` 同步 | 已剔除 | **剔除（不变）** | 若同步：client 200/min → 85 任务 30/min、分钟表 1/min、broker_recommend 26/min，`_acquire_rate()` 每次调用前执行 → 成功路径限速 + 影响共用 client 的 `MCPUpdateDetector`。剔除后零变化 |
| retry/backoff 同步 | 已剔除 | **剔除（不变）** | 若同步：88/88 任务显式 `backoff_sec=[60,120,240,480,960]` 改写 client 默认 `(30,60,120,240,480)`，失败路径退避拉长。剔除后逐位一致 |
| `call_timeout` 同步 | 保留（仅此项） | **保留（仅此项）** | 0/88 任务配置该键 → 落地后行为逐位不变；`<=0` 守卫回退 90 |
| 首页探测 | 新增 1 次（69 表） | **开关门控 + 零 sleep（62 表）** | 开关 true：首页成功 → 无差异；首页超时 → 降级（旧=900s 后失败，新=降级成功）；首页非超时传输错误 → 重发 `max(1, retry_max-1)`，总尝试 5 次不变、重连自愈保留；**探针零 sleep（:580 改有效上限后，确定性 0s）**。开关 false：完全不探测，逐行等效 |
| 非超时传输错误退避（确定性，M-2） | 「时延微调」不可判定 | **定稿**：重发分支总退避 = `backoff[0..2]`=30+60+120=**210s**（4 次尝试：末次无 sleep）；修复前同场景 = `backoff[0..3]`=30+60+120+240=**450s**（5 次尝试）。仅失败路径时延差异，成功路径不变，测试锁定。**重连次数由 4 次降为 3 次**（探针与末次在新判定下均不 `_reset_connection`，R-5）；**「总尝试次数 5 次不变」仅在 `retry_max=5`（当前全部任务）下成立**，`retry_max=1` 时新逻辑为 1+1=2 次（R-6） |
| 宽文本 export | 异步；窗口布尔键默认 true | **异步；窗口白名单默认空（fail-safe）** | 名单外（未探表）一律不下推；数据行数必须与 2026-10-04 黄金一致（§5.4/§5.5） |

### 4.4 残余风险声明（S9）
- fetch_page 通道本身仍无服务端过滤下推（R2.1 未直接修复）：若某数据集 export 不可用且
  首页 >90s，任务仍会失败——由 3.2 降级覆盖可 export 的数据集，残余场景留待
  「任务级 `call_timeout`」机制（3.3）未来按任务放窗，不在本次范围。
- **F-2 降级路径残余（步骤 3 追加，V-3）**：`ws_asfund` 探针实测通过
  （42,974 == 42,974），但本轮名单保持空（fail-safe，见探针证据 §4）⇒
  `ws_asfund` **未进白名单**，其 §3.2 降级 export 不带窗口；若降级后仍撞
  `MCPExportBudgetError`（非名单表 ⇒ 按 §3.1.3 不消费 `suggested_shards`、
  不二分、直接上抛），则**停止推进**，按 §6.3 触发回退判据第 4 项
  （F-2 复验仍失败）**回退并上报**，不得改白名单强行放行、不得改服务端、
  不得放宽任何容差。另：本次降级复测中同族三表曾出现瞬时
  `export exceeded the server time budget`（复测 12/12 成功），属负载抖动，
  按 §4.4 残余风险承接，不作为改白名单的理由；

### 4.5 禁止改动范围
- 禁止顺带改引擎/撮合/策略/校验器任何行为；禁止「优化」其他无关通道；禁止放宽容差；
- 禁止与其他会话在途改动（`mcp_adapter.py` +33、`writers.py` 等）混入同一提交。

---

## 5. 验收标准

1. **单元测试**（新增 `tests/test_mcp_passthrough_export_fallback.py`）：
   - 3.1：`export_dataset` 收到 `time_start/time_end/async_mode` 正确断言（窗口仅当表在
     `passthrough_export_window_tables` 名单内）；**非名单表 + `MCPExportBudgetError` →
     不消费 `suggested_shards`、不二分，直接上抛（R-1 用例）**；`MCPExportBudgetError` → 有效
     `suggested_shards` 消费 / 无效（重叠）时回落二分（M8：构造**重叠** suggested_shards →
     断言聚合无重复行）；深度上限；半开区间断言；**聚合断言失败 ⇒ 全列去重一次，仍不等 ⇒
     上抛且不写库（M-6 用例）**；0 行守卫仅在「实际携带窗口」时触发回落（m-2 用例）；
     客户端裁剪与 `_fetch_passthrough` 同口径；
   - 3.2：真实异常链构造（M1）——首页超时 → 降级触发；`MCPAuthError/MCPProtocolError`
     不降级；非超时 `MCPTransportError` → 不降级且总尝试次数==5（重连自愈保留）；
     兜底：非超时异常不降级按既有语义上抛（m-3）；首页成功 → 调用序列与修复前逐行一致（回归）；
     **`max_attempts=1` 路径总 sleep == 0（M-2）**；`retry_max==1` 时重发分支
     `max(1, retry_max-1)` 边界用例（m-4）；
   - **B1 用例：开关 false + 首页超时 → 调用序列与修复前完全一致（5 次尝试，不降级）**；
   - 3.3：`configure_execution` 后 client `call_timeout` 同步断言；`<=0` 回退 90；断言
     `retry_max/backoff_sec/rate_per_min` **不被同步**（M3/M4 锁定）；
   - S3：断言 `fetch_page` 实际发出的服务端 payload **不含** `max_attempts`（kw-only 不透传）；
   - M7：断言 `:652/:2137/:2158/:2179` 四个调用点调用序列不变。
2. **既有回归全绿**：`tests/test_mcp_*.py`（10 个既有文件）+ 相关管线测试套件。
3. **实机复验（口径定稿，M-3）**：mcp_only profile 单跑 F-1、F-2 → 非 failed；随后全量跑批，
   对照 20261004 台账，三判据**同时**成立：
   - `failed = 0`；
   - `success ≥ 59`；
   - `empty` 集合 ⊆（20261004 的 15 项 ∪ {`mcp_llm_text_events_enriched_pt`,
     `mcp_ws_asfund_pt`}）；若两任务返回 **0 行**，必须额外给出「该表在服务端确为 0 行」
     的证据（manifest total_rows 对账），否则判为验收不通过。
4. **探针准入证据（M6③ 硬性前置）**：§3.0 探针证据落盘 `docs/evidence/`，
   逐表「带窗口行数 == 不带窗口行数（同口径裁剪后）」；通过的列入白名单；
   **未见探针证据的表一律不在名单（窗口下推保持关闭，M-7）**。
5. **黄金对比（m-1 补充）**：6 张宽文本表修复前后行数/内容与 2026-10-04 跑批一致
   （llm_text_events 129,977 行等）；非宽文本 passthrough 表行数与上次一致；
   `batch_audit` 逐项比对；任何差异逐项归因，禁止以「差异很小」放行。
   **`llm_text_events_enriched`（F-1）无 10-04 黄金基线**（上次失败 0 行）——其验收口径 =
   单跑 success + 行数 > 0 + 与 manifest `total_rows` 对账一致。
6. **耗时回归（观测指标，S6/M-2 修订）**：F-2 由 ~900s 降至 <300s；构成 = 首页探测
   ~90s（单次调用上限，零 sleep）+ export 异步等待；取 3 次跑批中位数，
   受服务端负载影响不作为硬性失败判据。

---

## 6. 回退条件（B1/M-4/M-5 修订：可执行且逐行等效）

1. **配置回退（主）**：`sources_config.json` 置 `passthrough_export_fallback=false` +
   `passthrough_export_window_tables=[]` + `export_async=false` →
   - 3.2 完全不探测（B1：`max_attempts=None`，`_fetch_passthrough` 逐行等效修复前）；
   - 3.1 窗口下推全关（白名单空）；export 回到同步无窗口；
   - **§3.1.3/§3.1.5 已随白名单置空完全失效，回退覆盖完整**（步骤 3 实测收口）：
     白名单 `passthrough_export_window_tables=[]` ⇒ 无表携带窗口 ⇒ 无父窗 ⇒
     §3.1.3 分片收缩（suggested_shards 消费/二分）与 §3.1.5 0 行守卫在任何表上
     都不会被触发，配置回退对这两条的覆盖是**完整的**（非「可能成功」残余）。
     §4.5 同批登记：二者为纯增益错误路径逻辑，无独立回退键（M-4 选 b）；
2. **代码回退（辅）**：实施前先 `git stash create -u -m "baseline-<ts>"` 并 **`git stash store`
   持久化**回退点（零副作用，不动工作区）；实施时把本方案自身 diff 另存 patch 文件；
   回退 = **文件级定向 reverse-apply 本方案 patch**（只动本方案 hunk）。
   **禁用 `git revert` / `git checkout --`**——共享工作区含他人未提交改动（`mcp_adapter.py` +33
   等），文件级盲回退会抹他人工作（M5）；
3. **触发回退判据**：任一黄金对比行数不一致且无法归因 / 任一既有测试红 / 原 72 个成功任务
   出现新失败 / F-1、F-2 复验仍失败 / **任一大表「降级后行数 ≠ 未降级行数」且无法归因**——
   任一命中即停止并回退，不得带病推进。

---

## 7. 文档同步范围（步骤 4-5 完成后执行）

- `README.md`：MCP 数据采集相关章节（passthrough/export 采集行为段落）；
- `docs/data-pipeline-contract.md`（当前已被其他会话改动，实施时协调叠加）；
- `docs/strategy_toolbox.md` / `docs/prompt_engineering.md`：数据管线修复，经核实无涉及内容
  则不更新（验收阶段核查确认）；
- MCP 全数据源替代任务**实时进度报告**（`D:\miniQMT策略实盘\私募工作文件\QuantStudio-MCP全数据源替代任务文件\实时进度报告.md`）：
  按铁律仅在 **复审通过后**更新，附证据（SHA/测试结果/跑批数据/探针证据）。

---

## 8. 六步流水线状态

| 步骤 | 内容 | 状态 |
|---|---|---|
| 1 方案 | 本文档 v4（响应定点复核 R-1~R-7） | ✅ v4 修订完成 |
| 2 审计 | 定点复核 ⚠️ 有条件通过（R-1 已补，无需再复审） | ✅ 审计收口 |
| 3 实施 | 步骤 0 探针准入 → 按 v4 实施（写前快照+patch 留痕） | ⏸ 已解锁，待启动 |
| 4 验收 | §5 全部通过，证据写入 `docs/evidence/*.md` | ⏸ |
| 5 用户确认 | 完整汇报（改动+验收证据），经用户明确同意 | ⏸ |
| 6 双仓库推送 | `git push origin` 双仓库 + HEAD 逐位核对 + QuantStudio-trading 同步门 | ⏸ |

---

## 9. 审计/复审修订对照表（v4 全量状态，标记与复核结论对齐）

### 9.1 一审（M1–M9 / S1–S10）

| 审计项 | v4 落点 | 状态 |
|---|---|---|
| M1 异常契约 | §3.2 异常契约段（复审确认准确） | ✅ |
| M2 判据收敛 | §3.2 步骤 1 五出口 + 兜底（m-3/R-3） | ✅ |
| M3 rate 剔除 | §3.3.1 + §4.3 第 1 行 + 单测锁定 | ✅ |
| M4 backoff 剔除 | §3.3.1 + §4.3 第 2 行 + 单测锁定 | ✅ |
| M5 回退可执行 | ⚠️ v2 部分闭环 → **v3 闭环**：§3.4 三键（fallback 总开关语义 B1 写死）+ §6 三步 + 判据补「降级行数不等」+ M-4 选 b 显式声明 | ✅（复审 B1/M-4 修正后） |
| M6 窗口下推证据 | ⚠️ v2 部分闭环 → **v3 闭环**：§3.0 逐表探针 + §3.4 白名单 + §3.1.2 名单门控 + 0 行守卫（仅实际下推时生效）+ §5.4 前置 | ✅（复审 B2/M-5 修正后） |
| M7 探测范围 | §3.2 仅 `:604` 传 1；4 调用点回归断言（§5.1） | ✅ |
| M8 半开/去重 | ⚠️ v2 断言失败动作未定义 → **v3 定义**：全列去重一次，仍不等 ⇒ 上抛禁写库（M-6） | ✅ |
| M9 call_timeout 落点 | §3.3.2 明确选② + §2 R3 同步改写 | ✅ |
| S1 codes 守卫 | §3.2.3 | ✅ |
| S2 meta 补齐 | §3.1.6 | ✅ |
| S3 参数位置 | §3 改动表 + §5.1 payload 断言 | ✅ |
| S4 探测边界 | §3.2 步骤 1（仅首页） | ✅ |
| S5 口径钉死 | ⚠️ v2 内部冲突 → **v3 定稿**：三判据同立 + 0 行补服务端证据（M-3） | ✅ |
| S6 耗时口径 | ✅ 口径（数值经 M-2 修订为 ~90s 探测零 sleep + export 异步） | ✅ |
| S7 取值域 | §3.3.1 | ✅ |
| S8 命名消歧 | §3.1 首行 | ✅ |
| S9 残余风险 | §4.4 | ✅ |
| S10 共用 client 影响 | §3.3.3 + §4.3 | ✅ |

### 9.2 复审（B1/B2、M-1~M-7、m-1~m-7）

| 复审项 | v4 落点 | 状态 |
|---|---|---|
| B1 开关 false 不探测 | §3.2 总开关语义段（写死 `max_attempts=None`）+ §3.4 表 + §6.1 + §5.1 B1 用例 | ✅ |
| B2 白名单制 | §3.0（逐表探针）+ §3.1.2 + §3.2 步骤 2 + §3.4 `passthrough_export_window_tables` 默认 `[]` + §4.2 同步改写 | ✅ |
| M-1 数字口径 | §4.2（62 张 fetch_page / 跑批 50；7 张宽文本 export）+ §3.2.5 + §4.3 表 | ✅ |
| M-2 有效 attempts 上限 | §3 改动表（`:580`/`:589-595` 改有效上限）+ §3.2「零空转」段 + §4.3 确定性退避数值 + §5.1 sleep==0 用例 + §5.6 构成 | ✅ |
| M-3 验收口径互斥 | §5.3 三判据同立 + 0 行补服务端证据 | ✅ |
| M-4 3.1.3/3.1.5 无回退键 | §6.1 选 b：纯增益错误路径逻辑，修复前必失败，无等价性差异 | ✅ |
| M-5 全局/逐表矛盾 | §3.0 逐表判据 + 名单即持久化（删除全局布尔键） | ✅ |
| M-6 断言失败动作 | §3.1.3（全列去重一次 → 仍不等上抛禁写）+ §5.1 不写库用例 | ✅ |
| M-7 默认 fail-safe | §3.4 白名单默认 `[]`（代码层默认不下推）+ §5.4 | ✅ |
| m-1 enriched 无黄金基线 | §5.5 末段（success + 行数>0 + manifest total_rows 对账） | ✅ |
| m-2 0 行守卫条件 | §3.1.5（仅实际携带窗口时生效）+ §5.1 用例 | ✅ |
| m-3 兜底分支 | §3.2 步骤 1 兜底行 | ✅ |
| m-4 retry_max==1 边界 | §3.2 步骤 1 `max(1, retry_max-1)` + §5.1 边界用例 | ✅ |
| m-5 窗口记法 | §3.0（`[start_date, (跑批日+1天)T00:00:00)`，与 `:1213-1214` 同口径） | ✅ |
| m-6 §9 标记对齐 | 本节（9.1/9.2 状态如实标注） | ✅ |
| m-7 时间戳 | 文首 v4 定稿时间 02:53（实际定稿时间） | ✅ |
| R-1 分片收缩绕过白名单 | §3.1.3（选 A：分片收缩=窗口下推，仅名单内可用；非名单直接上抛）+ §3.4 语义行 + 父窗定义 + §5.1 用例 | ✅ |
| R-2 「三分支」措辞 | §3.2 总开关语义段 + §9.1 改「五出口」 | ✅ |
| R-3 兜底举例分类 | §3.2 兜底行（改「未经 `_call_with_retry` 包装的异常」+ 注明 MCPToolError 落入重发分支） | ✅ |
| R-4 裁剪/meta 影响点 | §4.2.1 补列 §3.1.4/§3.1.6 两项成功路径新行为 | ✅ |
| R-5 重连次数量化 | §4.3 表（重连 4→3 次） | ✅ |
| R-6 retry_max=1 限定 | §4.3 表（总尝试次数不变仅限 retry_max=5） | ✅ |
| R-7 名单元素命名 | §3.4 表（名单元素 = canonical `table` 名） | ✅ |

---

## 10. 证据指针

- 复盘：`output/mcp_only_collector_run_20261004_214251/failure_review.md`；
- 一审：`docs/audit-mcp-passthrough-first-pull-export-fix-20261005.md`；
- 复审：`docs/reaudit-mcp-passthrough-first-pull-export-fix-v2-20261005.md`；
- 定点复核：`docs/recheck-mcp-passthrough-fix-v3-20261005.md`（⚠️ 有条件通过，R-1 已补）；
- 代码锚点：`mcp_adapter.py:394/502-520/573-592/594-642/652/1209-1214/1228/2137/2158/2179`、
  `client.py:173/265/528/533/548-597/580/589-595/767-778/800-853/1003-1042`、
  `base.py:79-106`、`errors.py:26/74-91`、`daemon.py:984/988-991/2518`；
- 配置：`config/profiles/mcp_only/sources_config.json`、`collector_tasks.json`
  （88 任务 / 0 个 call_timeout / passthrough 69（启用 57）/ 宽文本 7 / codes 全 ALL /
  calls_per_min 85×30+2×1+1×26）；
- 基线：`docs/handoff/baseline-20261005-0209-mcp-passthrough-fix.txt`。
