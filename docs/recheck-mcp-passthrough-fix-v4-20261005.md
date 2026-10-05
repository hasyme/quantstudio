# v4 复核收口 + 步骤 3 实施提示词

- 复核对象：`docs/mcp-passthrough-first-pull-export-fix-design.md`（v4，定稿 2026-10-05 02:55）
- 复核依据：定点复核 `docs/recheck-mcp-passthrough-fix-v3-20261005.md`（⚠️ 有条件通过：补 R-1 即放行）
- 复核时间：2026-10-05 02:57（只读复核，未改动代码/配置/测试）
- **复核判定：✅ 通过 —— 步骤 2 审计收口，步骤 3（实施）可启动。**
  遗留 **3 项中等 / 2 项轻微**，其中 **V-1 必须在实施同批修掉**（一行，且属数据安全性规则）；
  V-2/V-3 建议同批；V-4/V-5 为表述精度，可顺带。

---

## 1. 定点复核项 R-1~R-7 闭环核对

| 复核项 | v4 落点 | 判定 |
|---|---|---|
| **R-1** 分片收缩绕过白名单 | §3.1.3（L99-107，选 A：分片 = 窗口下推的一种形式，仅名单内可用；非名单表 **不消费 `suggested_shards`、不二分、直接上抛**）+ 父窗定义（名单内 = `[start, end+1day)`；非名单 = 无父窗）+ §3.4 语义行（L181）+ §5.1 用例（L232-233） | ✅ **完整闭环**，与 §6.1 M-4「修复前必失败」判定自洽 |
| R-2 「三分支」措辞 | §3.2 L123、§9.1 L321 改「五出口」 | ✅（L321 表述冗余，见 V-4） |
| R-3 兜底举例分类 | §3.2 L150-153：改为「未经 `_call_with_retry` 包装的异常」，并注明 `MCPToolError` 落入重发分支 | ✅ 准确 |
| R-4 裁剪/meta 影响点 | §4.2.1 L198-199 补列 §3.1.4 客户端裁剪 + §3.1.6 meta 补齐为成功路径新行为 | ✅ |
| R-5 重连次数量化 | §4.3 L214 补「重连 4→3 次」 | ✅ |
| R-6 `retry_max=1` 限定 | §4.3 L214 补「总尝试不变仅限 `retry_max=5`（当前全部任务）」 | ✅ |
| R-7 名单元素命名 | §3.4 L181 明确「名单元素 = canonical `table` 名（非 `qdb_table`）」 | ✅ |

**结论**：R-1 已按建议 A 完整落地，定点复核的放行条件已满足；B1/B2、M-1~M-7、m-1~m-7 保持生效未被回退。

---

## 2. v4 新发现（R-1 修订引入的不一致）

### V-1【中等·必须同批修】§3.0 仍写「仅 async + 分片」，与 R-1 后的 §3.1.3/§3.4 矛盾

- **位置**：§3.0 L84 —— 「…→ 该表**不列入**名单，永不窗口下推（**仅 async + 分片**）」
- **冲突**：R-1 已规定非名单表**不做分片收缩**（budget error 直接上抛），§3.4 L181 也已改为「未列名单一律不下推（**仅 async**）」。§3.0 遗漏同步，仍允诺「+ 分片」。
- **风险**：实施者若只按 §3.0 理解，会给未探表恢复分片下推，正是 R-1 要封堵的口子。
- **修改**：§3.0 L84 改为「永不窗口下推、**且不做分片收缩**（仅 async）；触发 `MCPExportBudgetError` 时直接上抛（见 §3.1.3 R-1）」。

### V-2【中等】§6.1 的 M-4 声明与 R-1 后的事实不符

- **位置**：§6.1 L276-278 ——「§3.1.3 分片收缩与 §3.1.5 0 行守卫…**无独立回退键**…配置回退后仅把必失败变为可能成功」
- **冲突**：R-1 后，§3.1.3 已由白名单门控、§3.1.5 仅在「实际携带窗口」时生效；配置回退把 `passthrough_export_window_tables=[]` 后，**二者完全失效**。故回退覆盖是完整的，并非「无回退键」。
- **修改**：改为「§3.1.3 分片收缩与 §3.1.5 0 行守卫已随白名单置空而**完全失效**（R-1 后二者均由白名单/实际下推门控），配置回退覆盖完整；M-4 选 b 的声明保留为兜底说明」。

### V-3【中等】未声明 R-1 引入的新残余场景

- **位置**：§4.4 残余风险 / §5.3 实机复验
- **问题**：R-1 使非名单表降级后**无法用分片缓解 budget error**。若 `ws_asfund` 在 §3.0 探针中不通过（不入名单），且其 export 撞 `export_exceeds_time_budget`，则 F-2 仍会失败 —— §5.3 的 `failed=0` 不可达，但方案未给出该分支的处置口径。
- **修改**：在 §4.4 补一句——「若 `ws_asfund` 探针不通过且降级后仍撞服务端预算错误（非名单表不分片），F-2 不由本方案解决：停止推进，按 §6.3 触发回退/上报，并转 R3 任务级 `call_timeout` 或服务端扩容，不在本次范围」。

### V-4【轻微】§9.1 L321「五出口 + 兜底」冗余
兜底是五出口之一（首页成功 / 鉴权协议上抛 / 超时降级 / 非超时重发 / 兜底）。改为「五出口（含兜底）」。

### V-5【轻微】§4.3 末行「名单外一律不下推」
建议补「且不做分片收缩（R-1）」，与 §3.4 L181 表述一致。

---

## 3. 复核确认无误项（不再列出）

- §3.1.3 父窗定义、非名单直接上抛、M-6 去重→上抛禁写库三者的层级关系正确；
- §3.2 五出口 + 总开关语义（B1）在 §3.2/§3.4/§6.1 三处一致；
- `eff_max = max_attempts or self.retry_max`（§3.2 L136-138）与 §4.3 确定性数值（210s / 450s / 探针 0s / 重连 3 次）自洽并经独立复算；
- 数字口径全文一致（88/74、69(57)、7 宽文本、62(50) fetch_page）；
- §9.1/§9.2 状态标注与审计链一致；§10 证据指针完整；代码锚点全部有效。

---

# 4. 步骤 3（实施）提示词

> 以下为可直接复制到新会话/交给实施 Agent 的完整提示词。已内置 v4 全部约束与 V-1~V-3 的同批修订。

---

**【角色与范围】**
你是 QuantStudio 项目的实施 Agent。任务：按已通过审计的方案 **v4** 实施「MCP passthrough 首轮全量拉取失败」修复（六步流水线步骤 3）。
- 方案唯一权威文档：`D:\hasym\PycharmProjects\QuantStudio\docs\mcp-passthrough-first-pull-export-fix-design.md`（v4）
- 审计链（须先读，且不得违反其中任何一条裁定）：
  - `docs/audit-mcp-passthrough-first-pull-export-fix-20261005.md`（一审 M1–M9/S1–S10）
  - `docs/reaudit-mcp-passthrough-first-pull-export-fix-v2-20261005.md`（复审 B1/B2、M-1~M-7）
  - `docs/recheck-mcp-passthrough-fix-v3-20261005.md`（定点复核 R-1~R-7）
  - `docs/recheck-mcp-passthrough-fix-v4-20261005.md`（本文档：V-1~V-5）
- 工作目录：`D:\hasym\PycharmProjects\QuantStudio`

**【硬约束（违反即停止）】**
1. **只改这 5 个文件**（精确清单，不得扩范围）：
   - `quantstudio/pipeline/sources/mcp_adapter.py`
   - `quantstudio/pipeline/mcp/client.py`
   - `config/profiles/mcp_only/sources_config.json`
   - `tests/test_mcp_passthrough_export_fallback.py`（新增）
   - `docs/evidence/mcp-passthrough-window-pushdown-probe-*.md`（新增证据）
2. **不改** `collector_tasks.json`（0 任务配置变更）、**不改** `daemon.py`、**不改**策略源码、**不改**服务端、**不改** `call_timeout`(90s)/retry(5)/backoff(30,60,120,240,480)/rate(200) 任何客户端默认值。
3. **工作区有他人未提交改动**：`mcp_adapter.py` 已 M（他人 +33 行，落在 `:2267` 之后 stock_dividend 写看门狗）、`writers.py` 已 M（+784 行）。
   - 动手前先 `git diff -- quantstudio/pipeline/sources/mcp_adapter.py` 核对叠加事实并记录；
   - **禁止** `git checkout --` / `git revert` / `git stash pop` 等任何批量回退操作；
   - 你的所有编辑只做**定点 Edit**，每次编辑前后确认未触碰 `:2267` 之后的他人 hunk。
4. **写前快照**（第一步，先于任何编辑）：
   `git stash create -u -m "baseline-<时间戳>"` → 拿到 hash 后**立即** `git stash store <hash>` 持久化，把 hash 写入实施日志。
5. **禁止推送**：不得执行 `git push` / `git commit`。步骤 6 由用户确认后另行执行。
6. 纯增益：不得改变任何成功路径行为；不得为通过测试放宽容差；不得顺带「优化」无关通道。

**【步骤 0：准入探针（先于任何代码改动，只读）】**
写一个**临时脚本**（不落库、不进 git、不写入工作区代码文件），复用既有 `MCPAdapter.client.export_dataset`，对 **8 张表**做「带窗口 vs 不带窗口」双跑异步导出并比对行数：
- 表清单 = 7 张宽文本表（`cnthesims_events`、`ai_research_snapshot`、`llm_text_events`、`llm_text_events_enriched`、`llm_text_raw_feed`、`rsshub_raw`、`tdx_theme_news`）+ `ws_asfund`
- 带窗口 = `[start_date, (跑批日+1天)T00:00:00)` 半开 ISO（与 `mcp_adapter.py:1213-1214` 同口径）；不带窗口 = 全表
- 两者都经与 `_fetch_passthrough`（`mcp_adapter.py:605-615`）同口径的客户端日期裁剪后比对行数
- **逐表判据**：相等 → 该表进入候选名单；不等 / 0 行 / 无日期列 → **不进入**名单
- 结论落盘 `docs/evidence/mcp-passthrough-window-pushdown-probe-<ts>.md`，写明每表的两组行数与判定
- 探针期间避开跑批高峰。**未拿到探针证据前，名单一律留空 `[]`**

**【实施 A：`client.py` 改动】**
1. `_call_with_retry`（`client.py:533`）签名改为
   `def _call_with_retry(self, fn, *args, max_attempts: Optional[int] = None, **kwargs):`
   —— `max_attempts` 必须置于 `*args` 之后成为关键字专用参数，**不得**随 `**kwargs` 透传给 `_call_tool`（服务端 payload 中不得出现 `max_attempts`）。
2. 新增 `eff_max = int(max_attempts) if max_attempts else self.retry_max`，并把：
   - `:548` 循环上界改为 `range(eff_max)`
   - `:580` 的 `if attempt + 1 < self.retry_max` 改为 `if attempt + 1 < eff_max`
   - `:589-595` 末次跳退避判定同样改用 `eff_max`
   —— 目标是 `max_attempts=1` 时**单次失败后不 sleep、不 `_reset_connection`，直接抛终态异常**。
   - 等价性要求：`max_attempts=None` 时 `eff_max == self.retry_max`，与改动前**逐行等效**（`:652/2137/2158/2179` 及 export/get_manifest 全部调用点零影响）。
3. `fetch_page`（`client.py:767`）新增 `max_attempts: Optional[int] = None` 参数并透传给 `_call_with_retry`。

**【实施 B：`mcp_adapter.py` 3.1 —— `_fetch_export_passthrough`（`:573-592`）】**
1. 异步：调 `export_dataset` 时传 `async_mode=bool(self.export_async)`（`self.export_async` 见 `:394`，profile 已 `true`，代码默认 False）。
2. 窗口下推（**白名单制**）：新增读取 `self._config.get("passthrough_export_window_tables", [])`；**仅当 `table ∈ 名单`**（名单元素是 canonical `table` 名，即本函数入参，不是 `:580` 映射后的 `qdb_table`）时，才按 `_fetch_export_direct` 同口径（`:1209-1214`）传 `time_start=start→T00:00:00`、`time_end=(end+1天)T00:00:00`；不在名单或解析失败 → 不传，记 WARNING（等效旧行为）。**名单默认 `[]` = 不下推（fail-safe）**。
3. **预算错误消费（仅名单内表可用）**：`except MCPExportBudgetError`：
   - 若 `table ∉ 名单` → **不消费 `suggested_shards`、不二分，直接原样上抛**（R-1）；
   - 名单内：优先用 `suggested_shards`（须校验：非空、可解析、归一为半开区间后**两两不交且并集覆盖父窗**），否则按父窗二分（深度 ≤5、子作业 ≤16）。**父窗 = 本次请求的 `[start, end+1day)` 半开窗口**。
   - 聚合后断言 `sum(各子窗 rows) == len(concat)`；不等 → 先全列去重重试一次；仍不等 → **上抛并禁止写入**（不得落库）。
4. concat 后按 `_fetch_passthrough` 同口径做客户端日期裁剪（`date/trade_date/cal_date` 列存在时）。
5. 0 行守卫：**仅当本次请求实际携带了 `time_start/time_end`** 且结果 0 行 → WARNING + **单次**回落不带窗口重取；回落仍 0 行 → 正常返回空表。
6. meta 补齐 `source/freq/table/fetch_mode/passthrough/wide_text_export/async/window/subjobs/rows/lineage`。
7. 保留 `export_wide_text=False` 回落 fetch_page 的既有路由（`:509-510`）。

**【实施 C：`mcp_adapter.py` 3.2 —— `_fetch_passthrough`（`:594-642`）】**
1. `_fetch_all_pages`（`:539`）新增 `max_attempts: Optional[int] = None` 参数并透传给 `client.fetch_page`；**只有 `:604` 这一处传 `1`**，其余 4 个调用点（`:652`、`:2137`、`:2158`、`:2179`）保持不传。
2. 总开关：`self._config.get("passthrough_export_fallback", True)` 为 **false ⇒ 传 `max_attempts=None`（完全不探测）**，以下全部分支不生效，`_fetch_passthrough` 与修复前逐行等效。
3. 开关为 true 时，捕获首页异常，按**五出口**分发：
   - 首页成功 → 后续页 `max_attempts=None`（旧行为）
   - `MCPAuthError` / `MCPProtocolError` → 原样上抛
   - `_is_timeout_family(exc)` 为真 → **降级 export**
   - 其余 `MCPTransportError` → 不降级，以 `max_attempts=max(1, client.retry_max - 1)` 重发
   - 兜底：未被 `_call_with_retry` 包装的异常（如 `:561-563` cursor 不前进 `ValueError`）→ 不降级、按既有语义上抛
4. `_is_timeout_family(exc)`：`isinstance(exc, MCPRetryBudgetExhausted)` 为真；或 `isinstance(exc, MCPTransportError)` 且 `__cause__/__context__` 链（≤3 层）中存在 `TimeoutError`。**必须按此实现**——`max_attempts=1` 时 adapter 层收到的是 `client.py:596` 的 `MCPTransportError`，`TimeoutError` 只在 `__cause__` 链里。
5. 降级 = 复用 3.1 实现，meta 置 `fetch_mode="export_fallback"`；降级结果补做**日期裁剪 + codes 过滤**（与 `:616-623` 同口径，codes 非 ALL 时先 WARNING）；降级失败 → 上抛并保留首页原始错误于 `__cause__`。

**【实施 D：`mcp_adapter.py` 3.3 —— `configure_execution`】**
新增 `MCPAdapter.configure_execution(task)`：调用 `super().configure_execution(task)` 后，**只**把 `self.call_timeout` 同步给 client（client 未建 → 构造时传 `call_timeout=`；已建 → 在 `client._lock`（`client.py:307`）内更新 `client.call_timeout`）。
- `<=0` → 回退 client 默认 90（禁止 `join(0)`）
- **绝不同步** `retry_max` / `retry_backoff_sec` / `rate_per_min`

**【实施 E：`sources_config.json`】**
在 `sources.mcp` 下新增（**不改**既有 `export_async: true`）：
- `passthrough_export_fallback: true`
- `passthrough_export_window_tables: []`（名单等步骤 0 探针通过后由人工填入；**未拿到证据前保持空**）

**【同批修订（V-1~V-3，文档层）】**
- V-1：§3.0 L84「永不窗口下推（仅 async + 分片）」→「永不窗口下推、**且不做分片收缩**（仅 async）；触发 `MCPExportBudgetError` 时直接上抛（§3.1.3 R-1）」
- V-2：§6.1 L276-278 改写为「§3.1.3/§3.1.5 已随白名单置空而完全失效，配置回退覆盖完整」
- V-3：§4.4 补「`ws_asfund` 探针不通过 + 降级后仍撞预算错误 → 停止推进，按 §6.3 回退/上报，转 R3 或服务端扩容」

**【实施 F：新增测试 `tests/test_mcp_passthrough_export_fallback.py`】**
至少覆盖（全部用 monkeypatch/fake，不得依赖真实网络）：
1. 3.1：`export_dataset` 收到正确的 `time_start/time_end/async_mode`（名单内）；**名单外表 + `MCPExportBudgetError` → 不消费 `suggested_shards`、不二分、直接上抛**；有效 `suggested_shards` 被消费；**重叠** `suggested_shards` → 聚合无重复行；深度上限；半开区间；聚合断言失败 → 去重一次 → 仍不等上抛**且不写库**；0 行守卫仅在「实际携带窗口」时触发。
2. 3.2：用**真实异常链**构造（`MCPTransportError(...) from TimeoutError(...)`）——首页超时触发降级；`MCPAuthError/MCPProtocolError` 不降级；非超时 `MCPTransportError` → 不降级且总尝试 ==5；`max_attempts=1` 路径**总 sleep == 0**；`retry_max=1` 时 `max(1, retry_max-1)` 边界；开关 false + 首页超时 → 序列与修复前一致（5 次尝试）；首页成功 → 序列逐行一致。
3. 3.3：`configure_execution` 后 client `call_timeout` 已同步、`<=0` 回退 90，且 `retry_max/backoff_sec/rate_per_min` **未被同步**。
4. S3：`fetch_page` 发往服务端的 payload **不含** `max_attempts`。
5. M7：`:652/2137/2158/2179` 四调用点调用序列不变。

**【验收与交付】**
1. 新增测试全绿 + `tests/test_mcp_*.py`（既有 10 个）回归全绿 + 相关管线套件全绿。
2. 把本方案自身 diff 另存为 patch 文件（供回退用），并记录 stash hash、改动文件 SHA/行数。
3. 实机复验（需用户确认时机）：单跑 `mcp_llm_text_events_enriched_pt`、`mcp_ws_asfund_pt` → 非 failed；再全量跑批，须同时满足 `failed=0`、`success≥59`、`empty` ⊆（20261004 的 15 项 ∪ 这两项）。
4. 黄金对比：6 张宽文本表行数/内容与 2026-10-04 一致（`llm_text_events` 129,977 行）；`llm_text_events_enriched` 无黄金基线，口径 = success + 行数 >0 + manifest `total_rows` 对账。
5. 证据写入 `docs/evidence/`；**不推送、不提交**；完成后汇报（改动清单 + 行数 + 测试结果 + stash hash + patch 路径），等用户确认再进入步骤 5/6。
6. 任一命中下列即**停止并回退**（文件级 reverse-apply 本方案 patch）：黄金行数不一致且无法归因 / 任一既有测试红 / 原 72 个成功任务出现新失败 / F-1、F-2 复验仍失败 / 任一大表「降级后行数 ≠ 未降级行数」且无法归因。

---

## 5. 本轮复核纪律声明

- 只读复核，**未修改任何代码 / 配置 / 测试**，未对被审方案文档写入；
- 未执行任何 `git` 写操作，他人未提交改动完整保留；
- 本轮唯一新增文件：本文档。
