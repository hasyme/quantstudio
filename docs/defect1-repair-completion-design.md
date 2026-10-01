# 缺陷1 修复收尾 · 框架层方案（V2，返工定稿）

> 状态：**V2 定稿，待复审**
> V1 审计结论：**NO-GO**（`docs/audit-defect1-repair-completion-design-20260929.md`）
> 本版针对审计 F1-F6 全部返工，采纳审计建议 §3.1-§3.6。
>
> 关联取证：
> - `docs/evidence/defect1-root-cause-confirmed-20260928.md`（**§5.4/§6.1 因果表述须修正，见 §6**）
> - `docs/evidence/defect1-repair-attempt1-failure-20260928.md`
> - `docs/evidence/defect1-repair-diagnosis-correction-20260928.md`
> - `docs/anchor-drift-fix-design-a-prime.md`（A′，已实施，**本方案不改其口径逻辑**）

---

## 0. V1 → V2 变更摘要（回应审计）

| 审计发现 | V1 错误 | V2 修正 |
|---|---|---|
| **F1**（P0）根因 B 定性错误 | 称「失败后无退避重放循环」 | **改正**：真身是**失败任务仍执行 `qfq_run_post_ingest` → 逐股重锚链无界**（§1.3） |
| **F1** P2 改动点错位 | 拟改「daemon 源链失败分支」 | **移位**：改 `execute_task` finally 进入条件 + 重锚链边界（§3.P2） |
| **F1** client 层重复建设 | 拟做「确定性错误不重放」 | **删除**：client 层 2026-09-18 已具备（`mcp/client.py:155-167`），V2 仅引用确认 |
| **F2**（P0）P1 不覆盖失败相位 | 软停止只能在写片边界触发 | **改为窗口分段驱动**（审计建议 2b），不再依赖写片边界（§3.P1） |
| **F2** 根因 A 因果表述错误 | 称「整体不落库/事务回滚」 | **改正**：逐片 autocommit 即持久；「零写入」= **fetch 前置全量未产出分片**（§1.2） |
| **F3** once CLI 软停止出口未定义 | 未定义 | **消失**：V2 无软停止（窗口分段后不需要）（§3.P1） |
| **F4** budget 错误分类依据不足 | 未取证 | **已实验取证**：属**瞬时/配额类**（同日重试全部成功），故**不得**归为确定性不重试（§4） |
| **F5** A3 残留行风险未讨论 | 未讨论 | **新增** A3 残留行核查步骤（§5.A3） |
| **F6** 文字瑕疵 | 「判废三重校验」实列四项 | **改为四重**（§1.4） |

**V1 的 P2（重试纪律）整条撤销**——审计核实 client 层已具备且源链单源只跑一次，
V1 的 P2 与真实现象错位且属重复建设。V2 的 P2 改为**重锚链定界**。

---

## 1. 问题定义

### 1.1 现象（两次实测）

| 次 | 命令 | 结果 |
|---|---|---|
| 1 | `--mode once --task mcp_stock_minutes --pull-mode full_range`（sync） | fetch 阶段 **batch 130/136** 的 export 失败：日志 12:20:57 记 `批次 130/136: 2026-09-16 → 2026-09-17`，**12:21:58** 报 `MCPExportBudgetError`——**间隔恰 61 秒**，直证命中服务端 **60s 同步处理预算** |
| 2 | 同上（`export_async=true`） | 看门狗 120 分钟到时终止于 fetch 阶段 batch 28/136（22:20:18）；**全程 0 budget 错误、0 ERROR** |

**两次均未写入任何一行**（全表 `update_time` 停在 **2026-09-24 15:06**，早于两次运行）。

> **批号口径统一（C4）**：真实失败批次 = **130/136**（窗口 2026-09-16→09-17）。
> 依据：`_resolve_shard_paths` 的批次日志打在 `export_dataset` **调用之前**
> （`mcp_adapter.py:1603-1610`），故最后一条 `批次 130/136` 即失败那批。
> V1 审计/attempt1 所记「batch 126」为叙事误差，本版统一为 **130**。

### 1.2 根因 A：全量 fetch 超出单次可完成规模，且 fetch 阶段不可分段

**代码事实（关键）**：`_streaming_export` 的调用序是

1. `_resolve_shard_paths`（`mcp_adapter.py:1596-1626`）**先 `for i, (bs,be) in enumerate(batches)`
   循环创建全部 136 个 export 作业、逐批落盘 parquet，全部完成后才 `return paths, job_id`**；
2. 之后 daemon 才拿到分片迭代器，开始逐片 align→validate→write。

→ **fetch 是「全量前置阻塞」**：136 批 ≈ 10 小时（实测 sync ≈1.9 min/批、async ≈4.3 min/批）
在 daemon 侧**没有任何写片边界**可停、可分轮。

**「零写入」的正确因果**（修正 V1）：
- writer 逐片 `write()` 独立连接、**无显式事务、upsert 后关连接 → 逐片 autocommit 即持久**
  （`writers.py:788-860`）；「全部片写完后一次」的**只有水位**（`daemon.py:1343-1350`）。
- 故「零写入」**不是**写事务回滚，而是 **fetch 前置全量未完成 → 一片都未 yield → 无任何写发生**。

**死锁**：单次跑不完 10 小时 fetch；而游标只在写片后推进（`daemon.py:1322-1326`），
fetch 期间游标不动 → 下一轮 `_open_day_resume` 命不中 → fetch 窗口无法缩小。

### 1.3 根因 B：任务失败后 `qfq_run_post_ingest` 无界执行逐股重锚链

**日志复验（决定性）**：

| 事实 | 数值 |
|---|---|
| 2026-09-28 全天 ERROR 总数 | **2 条**（12:11、12:21；09:58/10:00 为 retry WARNING） |
| 唯一 `export_exceeds_time_budget` | **12:21:58**（fetch batch **130**/136 那一次，间隔 61s） |
| 13:18–18:16「空转」窗口内 ERROR | **0 条** |
| `export 分批=128` 出现次数 | **115 次**，每次对应**不同 code**（非重放） |
| 该窗口内 `stock_minutes` 写入次数 | **0 次** |

**真实机理**：
```
任务失败(task_ok=False, 未 cancelled)
  → execute_task finally 分支条件仅判 `not _cancelled`，不判 task_ok
     （daemon.py:664: `if (not _cancelled) and owns_qfq_cycle and ...`）
  → 仍调用 qfq_run_post_ingest(run_id)（daemon.py:666）
  → orchestrator: recover → discover → _claim_and_merge
  → `for unit in units:` 逐股 _reanchor_security（qfq_resident_orchestrator.py:1481-1487）
     —— **该循环无任何上限/预算/可中断边界**
  → 日志实证：ReanchorCheck 000002→000534 共 47 只通过，13:19–18:15 约 6.4 min/只
  → 若遍历全部 5,247 只 ≈ 数周量级
```

这就是「5 小时 CPU ~120%、零写入」的真身：**不是重试循环，是失败任务后无界执行的 QFQ 收尾链**。

**附：client 层重试纪律已具备（V2 不再触碰）**
`mcp/client.py:155-167`（`retry_max`/`backoff_sec`/`retry_budget_sec`）、
`416-427`（握手重试与预算）、`533-597`（确定性/瞬时错误分类，确定性不重试）。
V1 拟做的「确定性错误不重放」**已存在**，属重复建设。

### 1.4 关键约束（已核实）

| 事实 | 出处 |
|---|---|
| 续传游标 `TaskResumeCursor` 功能完整 | `task_resume.py:83-310` |
| 流式路径已接线续传（每片写后 `cursor.advance`） | `daemon.py:1322-1326` |
| **但续传只在 `cancel_check` 非 None 时启用** | `daemon.py:745`、`777` |
| **CLI `--mode once` 不传 `cancel_check`** | `daemon.py:2199` |
| **续传对 fetch 相位无效**（游标只在写片后推进） | §1.2 + `daemon.py:1322-1326` |
| 判废**四重**校验：schema_version / task+mode / 窗口指纹 / 水位 | `task_resume.py:114-141` |
| `QUANTSTUDIO_RESUME_DIR` 可重定向游标目录（测试隔离） | `task_resume.py:47-56` |
| writer upsert 幂等 | `writers.py:836` |
| `_apply_qfq` 只写 `{col}_front/back` 新列，**不回写 close** | `aligner.py:986-996` |
| 源链回退循环单源只执行一次 | `daemon.py:566-586` |
| `run_once` 无 `TaskCancelled` 捕获 | `daemon.py:2192-2201` |
| `--mode once` 无 `--start-date`/`--end-date` 参数 | `daemon.py:3616-3641`（参数表） |
| `full_range` 的 end 取 `task.get("end_date")` | `daemon.py:890-894` |
| 逐股重锚链无界循环 | `qfq_resident_orchestrator.py:1481-1487` |

---

## 2. 方案总览（V2 设计取向）

**核心取向（采纳审计 §3.2-b）**：**不再试图中断 fetch，而是把 fetch 切小到「每轮可完整跑完」，
用「正常完成」取代「软停止续传」。**

理由：
- fetch 是「全量前置阻塞」（§1.2），任何「中途软停止」都碰不到它；
- 窗口分段后每轮 fetch 有界，**每轮都能自然跑完 → 写入持久 + 水位推进**，
  无需软停止、无需依赖游标、无需改流式语义 → F2/F3 一并消解；
- 与「不新造机制」初衷最贴合（审计原话）。

| 组件 | 内容 | 对应审计 |
|---|---|---|
| **P1** | `--start-date`/`--end-date` CLI 覆盖 → 窗口分段驱动分轮修复 | F2、F3 |
| **P2** | ①`task_ok=False` 时跳过 `qfq_run_post_ingest`；②为逐股重锚链加边界预算 | F1 |
| **不采纳** | V1 的 `--resumable` / 软停止（F2/F3 使其无效）；V1 的 P2 重试纪律（client 层已有） | — |

---

## 3. 改动范围

### P1：窗口分段驱动（替代 V1 软停止续传）

**改动**：`quantstudio/pipeline/daemon.py`

1. 新增 CLI 参数（`--mode once` 语义下生效）：
   ```
   --start-date YYYY-MM-DD    # 覆盖 task.start_date
   --end-date   YYYY-MM-DD    # 覆盖 task.end_date（full_range 的 end）
   ```
2. `run_once` 内对目标 task 施加覆盖（仅一次运行内生效，**不改任务配置、不落盘**）：
   - 仅 `--mode once` 且显式传入时生效；未传 = 现状零变化。

**执行形态**（数据修复）：

| 轮 | 窗口 | 约批数 | 约耗时(sync) |
|---|---|---|---|
| R1 | 2026-01-01 ~ 2026-02-15 | ~23 | ~45 min |
| R2 | 2026-02-16 ~ 2026-03-31 | ~23 | ~45 min |
| R3 | 2026-04-01 ~ 2026-05-15 | ~23 | ~45 min |
| R4 | 2026-05-16 ~ 2026-06-30 | ~23 | ~45 min |
| R5 | 2026-07-01 ~ 2026-08-15 | ~23 | ~45 min |
| R6 | 2026-08-16 ~ 2026-09-28 | ~21 | ~45 min |

> 批数估算：2026-01-01~09-28 共 ~271 天 / 136 批 ≈ 2 天/批（实测窗口粒度 1 天 × 交易日）。

**每轮语义**：正常 `--mode once --pull-mode full_range`，跑完即
①逐片 upsert 持久 ②水位推进 ③post_ingest（受 P2 约束）。
**失败重试**：某轮 fetch 失败 → 该轮重跑（`export_cache` TTL=7 天，已缓存批次跳过重取）。

**执行 SOP（C1 裁定：全程 `export_async=true`）**——写死，非执行期可调项：

| 项 | 裁定 | 依据 |
|---|---|---|
| 导出模式 | **全程 `export_async=true`** | run 2（async）28 批 **0 budget 错误 / 0 ERROR**；且 run 1 失败间隔 **61s = 60s 同步预算**，证明该预算是**单作业同步处理时长**，async 转后台处理即绕过 |
| 前置验证 | **A0：实施后先跑 1 轮 async 大窗（≥20 批），断言 0 budget 错误**，通过才继续后续轮 | 回应审计「先用 1 轮 async 实测确认」（本方案已有 run 2 证据，A0 作为实施期正式确认） |
| 同日多轮风险 | **允许同日连跑**；但**任一轮失败即停止当日**，该轮改**次日重试** | F4 已证「次日同窗口必过」（9/9 成功）；同日累计负载盲区由该止损规则兜住 |
| 排序 | 按 §3.P1 表 R1→R6 顺序执行 | 各轮独立子窗，互不依赖水位 |

> **为何不选「每轮隔日」（审计选项 a）**：async 的绕预算机理已被 run 2 直接实证
> （28 批 0 预算错误），且 A0 提供实施期再确认；隔日需 6 个自然日，
> 在机理已明的前提下属过度保守。止损规则（任一轮失败→当日停止→次日重试）
> 已覆盖审计指出的「同日累计负载」盲区。

**为何不注入 `cancel_check`**：分段后每轮天然跑完，不需要软停止；
保留 `--mode once` 不传 `cancel_check` 的现状 = **常驻链/GUI/既有 CLI 行为零变化**。

### P2：重锚链定界（替代 V1 的重试纪律）

**改动**：`quantstudio/pipeline/daemon.py` + `quantstudio/pipeline/qfq_resident_orchestrator.py`

1. **失败任务跳过 post-ingest**（直击本次事故）
   `daemon.py:664` 条件增加 `task_ok`：
   ```python
   if (not _cancelled) and task_ok and owns_qfq_cycle and self._qfq_cycle_id is not None:
   ```
   理由：任务失败 = ingest 不完整 → 水位本就不会推进；
   跑数小时重锚链无收益（gate 仍 hold），且会阻塞后续排程。
   日志明确记录「task_ok=False → 跳过 post-ingest（水位保持）」。
   注：`recover_stale_in_progress` 等清理在下一次成功轮次开头照常执行（幂等），不丢职责。

2. **重锚链加边界预算**（纵深防御，防成功路径下同类无界）
   `qfq_resident_orchestrator.py:1481` 的 `for unit in units:` 循环内加边界检查：
   - 每 unit 完成后检查「本轮已耗时 ≥ 预算」或「外部停止谓词命中」→
     **记录剩余未处理 units 到 cycle summary 并安全退出**（已处理部分照常 gate/提交）；
   - 预算默认值由配置给出（建议 30 min），0/负 = 不限（回退旧行为）。
   - 语义 = **本轮截断、下轮续做**（units 来自持久化的 trigger 表，未处理者仍 pending）。

**明确不改**：
- `mcp/client.py`（既有重试纪律保留不动，仅引用确认）
- `mcp_adapter._streaming_export` / `_resolve_shard_paths` 的 fetch 语义
- A′ 口径逻辑（`_QFQ_CALIBER_TABLES` / `_restore_to_raw`）
- aligner / writer 写入逻辑

### 不做（明确排除）

| 项 | 理由 |
|---|---|
| V1 的 `--resumable` + 软停止 | 审计 F2：fetch 相位无法触发；分段后不需要 |
| V1 的 P2「确定性错误不重放 + 重试上限」 | 审计 F1：client 层已有；源链单源只跑一次 |
| 改 close 写入 / A′ 口径 | 已取证排除（`_apply_qfq` 不回写 close） |
| 缩窗（`minute_export_target_rows`） | 见 §4：budget 属瞬时类，**非** A3 前置，仍为可选另案 |

---

## 4. F4 判定实验结论（新增取证）

**实验**：对 09-28 失败窗口组（batch 126-136 覆盖的日期）在 09-29 用**生产原模式（sync）**
逐个重试 `create_export_job`：

```
2026-09-08->09-09 OK 12.1s    2026-09-18->09-19 OK 13.0s    2026-09-24->09-25 OK 12.4s
2026-09-10->09-11 OK 12.1s    2026-09-20->09-21 OK  0.1s    2026-09-26->09-27 OK  0.1s
2026-09-16->09-17 OK 15.1s    2026-09-22->09-23 OK 12.1s    2026-09-27->09-28 OK  0.1s
```

**结论：属「瞬时/配额类」（随时间恢复），非「规模类」。** 依据：
- 同一批窗口在次日**全部成功**（12~15s，远低于 60s 预算）；
- 若属规模类，同窗口应**稳定复现**失败——未复现。

**推论**：
1. **缩窗不是 A3 的前置依赖**（V1 的「另案」分类**成立**，审计 F4 的担忧方向不成立）；
2. `MCPExportBudgetError` **不得**归为「确定性、不重试」——否则误杀次日可成功的重跑
   （V1 若实施其 P2 反而会造成此误杀；V2 已撤销该设计）；
3. **`export_async=true` 绕过 60s 预算——已实测验证**（非推断）：

   | 证据 | 数值 |
   |---|---|
   | run 1（sync）失败间隔 | batch 130 日志 12:20:57 → 报错 12:21:58 = **61s**（= 60s 同步处理预算） |
   | run 2（async）预算错误 | **0 条**（28 批 / 2 小时 / 0 ERROR） |
   | 直接实测 `create_export_job` | sync 11~15s vs **async 0.1~0.3s** |
   | 直接实测 async 端到端 | **387s / 26 shards 成功** |

   → 机理：该预算是**单作业同步处理时长**，async 转后台处理即不受其约束。
   代价 = 吞吐下降（sync ~1.9 min/批 vs async ~4.3 min/批）。
   **执行 SOP 已定：全程 async（§3.P1），并以 A0 前置实测再确认。**

---

## 5. 验收标准

### A. P1 窗口分段（核心）

- **A0（前置验证，C1 条件）**：实施后先跑 **1 轮 async 大窗（≥20 批）**，断言
  **0 条 `export_exceeds_time_budget`**；通过方继续 R1-R6。不通过 → 回退审计选项 (a) 每轮隔日。
- **A1**：单轮小窗（如 2026-01-01~2026-01-15，约 8 批）跑通，验证：
  ①正常完成（`task_ok=True`）；②数据**已落库**（该窗 `(code,day)` 值 = 云端 raw）；
  ③水位推进；④`--start-date/--end-date` 确实生效（日志 FULL_RANGE 显示覆盖后的范围）。
- **A2**：6 轮全部完成 → `stock_minutes` 覆盖 2026-01-05~2026-09-16 **无缺口**
  （逐日 bar 数与该日交易日历一致）。
- **A3**：**数据修复效果**（§5.2 主判据）：
  `stock_minutes` vs `stock_daily` 全量配对**逐位相等率由 51.34% → ≥99.5%**，
  `>5%` 重度污染率由 3.78% → ≈0。
  **并新增残留行核查（回应审计 F5）**：
  - 逐轮记录「重拉前/后行数差」；
  - 输出「本地有、云端无」的多余行清单（尤其仅含 14:58/14:59 占位 bar 的 code-day）；
  - 若存在 upsert 无法清除的残留脏 bar，**显式列清单并声明**：
    或追加删除步骤，或作为独立清理案（不得默认忽略）。
- **A4**：既有功能零衰减——**不传** `--start-date/--end-date` 时，
  `--mode once` 行为与实施前逐位一致（同窗口跑一次，写入行数/水位/batch_audit 三者一致）。

### B. P2 重锚链定界

- **B1**：构造「任务失败」场景（如故意无效窗口触发失败），验证：
  ①日志出现「跳过 post-ingest（水位保持）」；②**不再进入逐股重锚链**（进程有限时间退出）。
- **B2**（**C3 强化**）：三个子例全覆盖——
  ①**截断后续做**：注入小预算使链在预算处截断 → 未处理 units 仍为 `pending`、
     下轮 `recover_pending_due` 可回收；
  ②**零重复**：已 `committed` units 不重复提交（`_already_committed` 去重）；
  ③**不误截日常**：**正常低事件量 `post_ingest` 不触顶 30 min 预算**
     （防 forever 周期被误截断）——断言该场景下 `summary` 无截断标记、units 全处理。
  最终断言：截断-续做循环收敛后 **全部 units 被 claimed 且零重复**。
- **B3**：既有测试套件中 daemon 失败路径 + QFQ orchestrator 相关用例全绿。

### D. 通用（原编号 C 与审计条件 C1-C4 冲突，改为 D 以示区分）

- **D1**：`py_compile` + 相关测试全绿；**新增测试**覆盖 A1（日期覆盖生效）与 B1/B2（定界语义）。
- **D2**：6 策略重转 `api_portability` 全 PASS（依「全链路修复」铁律）。
- **D3**：证据写入 `docs/evidence/`。

---

## 6. 证据文档因果订正（C2 —— **已完成**）

`docs/evidence/defect1-root-cause-confirmed-20260928.md` 的 §5.4 / §6.1 错误因果表述
**已于本轮返工中直接修正**（复审 C2 要求「随实施一并落地」，实际已在方案定稿时完成）：

| 原表述 | 已改为 |
|---|---|
| 「流式为『全部分片成功后才提交』→ 整体不落库 / 事务回滚」 | 「writer 逐片 **autocommit 即持久**（`writers.py:788-860`）；『零写入』真因 = **fetch 前置全量未产出任何分片**（`mcp_adapter.py:1596-1626`）」 |
| 「整体不落库」 | 「水位在全部片后才推进（`daemon.py:1343-1350`）；数据本身逐片已持久」 |

**同步订正范围**（均为本轮完成）：
- `defect1-root-cause-confirmed-20260928.md` §5 第 4 条、§6.1「为什么没写」
- `defect1-repair-attempt1-failure-20260928.md` §2.2（加「已被审计推翻」标注 + §2.2-corrected）、
  §2.3 → §2.3-corrected、§4 四项待决全部更新（含「重试循环」错误定性撤销）
- `defect1-rework-verification-20260929.md`（新增，含 F1/F2 复核与 F4 实验）

**校核结果**：`grep` 全部证据文档，`整体不落库`/`全部分片成功才提交` 已无残留
（仅存「**不是**写事务回滚」「**非**写事务回滚」两处修正性表述）。

---

## 7. 影响面

| 面 | 影响 | 依据 |
|---|---|---|
| 常驻 daemon 链 | **零影响**：`--start-date/--end-date` 仅 once + 显式传入；P2① 只改 `task_ok=False` 分支 | `daemon.py:664` 条件收紧仅影响失败路径 |
| GUI 路径 | 零影响（不传新参数） | 同上 |
| 既有 CLI `--mode once` | **零影响**（不传新参数时 `task_ok` 判定不变——成功任务行为逐位一致） | A4 验收 |
| QFQ 成功路径 | 重锚链默认预算 30 min：正常事件量下不触顶 → 行为不变；触顶时截断续做 | B2 + 默认值保守 |
| 水位语义 | 零影响 | 沿用既有 `_commit_or_hold_watermarks` |
| A′ 口径 | 零影响 | 不改 |

> ### ⚠ 共享核心文件叠加事实（依「共享核心文件提交纪律」第 4 条，显式记录）
>
> 实施时检测到 `quantstudio/pipeline/daemon.py` 已含**其他会话的未提交改动**
> （工作包 B，2026-09-23 诊断性修复：`_last_source_chain_skips` / `skip_reasons`
> 逐源跳过原因旁路，+24/-3）。
>
> **本方案的 P1/P2① 改动与上述改动叠加在同一文件**：
> - 叠加事实已记录，未静默带过；
> - 提交时**禁止 `git add -A`**；须精确核对文件清单，
>   且提交信息中不得把他会话改动记为本次成果；
> - 本次改动统计：`daemon.py` +42/-4（含叠加前基线 +24/-3）。
>
> 另两个改动文件 `qfq_resident_orchestrator.py` / `qfq_orchestrator_types.py`
> **无他人未提交改动**（干净）。

---

## 8. 回退条件与手段

**回退条件**（满足任一即停并回退）：
1. A1 显示日期覆盖未生效，或覆盖后写入数据与云端 raw 不一致；
2. **A4 显示不传参数时 `--mode once` 行为有任何可观察差异**；
3. **P2① 导致成功任务的水位/写入行为变化**（应只在 `task_ok=False` 生效）；
4. P2② 截断导致 units 丢失或重复提交；
5. A3 修复后一致率未达 99.5%，或出现新坏值；
6. A3 残留行清单出现无法解释的多余行且影响策略层。

**回退手段**：
- 代码：`git reset --hard <实施前 stash hash>`；既有回退点
  `935a9ffe` / `7aea0d58` / `34b73787` / `acdafb7d`
- 数据：P1 仅**覆盖性 upsert**（不新增行型态），必要时以用户全库备份还原

---

## 9. 风险与对策

| 风险 | 等级 | 对策 |
|---|---|---|
| 单轮窗口仍过大（sync 1.9min/批 × 23 批 ≈ 45 min；async × 23 ≈ 100 min） | 低 | 窗口可按实测收窄；A1 先小窗验证；**执行按 async（§3.P1 SOP）** |
| 某轮 fetch 因瞬时 budget 失败（含同日累计负载，C1） | 中 | **任一轮失败即当日停止，该轮次日重试**（F4 已证次日必过）；`export_cache` TTL 7 天命中跳过已取批次 |
| async 仍报 budget（A0 未过） | 低-中 | A0 前置实测拦截；不通过 → 回退审计选项 (a) **每轮隔日** |
| P2① 误伤「部分成功」任务 | 中 | 条件严格限 `task_ok=False`；B3 回归 + A4 |
| P2② 截断致 units 重复提交 / 误截日常（C3） | 中 | B2 三子例专测；units 状态机既有幂等（`_apply_trigger_outcome`） |
| upsert 不清除「本地有云端无」残留行（F5） | 中 | A3 增残留行核查 + 显式声明处置 |
| 多轮水位推进致数据断层 | 低 | 每轮独立 `full_range` 窗口，互不依赖水位；A2 逐日核验 |

---

## 10. 未决项（已定稿）

1. **软停止阈值** → **V2 取消**（窗口分段后不需要软停止；F2/F3 使 V1 该设计无效）。
2. **P1 与 P2 是否同批** → **同批**。依据：P1 分轮后每轮结束都会跑 post_ingest，
   若 P2 未落，**每轮失败都仍触发无界重锚链**（事故会在每轮重演）——P2 是 P1 的前置。
3. **分几轮 / 总时长** → **6 轮**；执行按 async SOP（§3.P1），每轮约 100 min，
   合计约 10 小时（若 A0 显示 async 顺利可同日连跑；任一轮失败改次日）。缩窗仍为可选另案（§4：非前置）。
4. **`--start-date/--end-date` 是否长期保留** → **长期保留，默认不生效**（不传即零变化）。
   理由：通用能力（任何长任务分段），默认关符合「零影响」要求。
5. **执行节奏（C1）** → **已裁定：全程 `export_async=true`**，写死于 §3.P1 执行 SOP
   （非执行期可调项）；含 A0 前置实测与「失败即当日停、次日重试」止损规则。

---

## 11. 流程状态

| 阶段 | 状态 |
|---|---|
| 取证（根因） | ✅ 完成；因果表述已订正（§6 / C2） |
| 方案 V1 | ❌ 审计 NO-GO（`audit-defect1-repair-completion-design-20260929.md`） |
| 方案 V2 | ✅ **复审通过（CONDITIONAL PASS）**（`audit-defect1-repair-v2-20260929.md`） |
| 条件闭环 | ✅ C1/C2 已闭环（§12）；C3/C4 纳入验收 |
| **实施（代码）** | ✅ **完成**（2026-09-29）：<br>· P1 `daemon.py`：`--start-date`/`--end-date` + `run_once` 窗口覆盖<br>· P2① `daemon.py`：`task_ok=False` 跳过 post-ingest<br>· P2② `qfq_resident_orchestrator.py` + `qfq_orchestrator_types.py`：重锚链预算截断<br>· 新增 `tests/test_defect1_repair_completion.py`（12 例） |
| 验收（代码部分） | ✅ 3 文件 `py_compile` OK；新增 12 例 PASS；`test_qfq_orchestrator_types.py` 37 例 PASS；daemon/QFQ 回归 **0 真实失败**（其余为沙箱 `PermissionError`／`sleep unable to open database` 环境阻塞） |
| 验收（数据修复 A0-A4） | ⬜ **待执行**（需跑 6 轮窗口分段） |
| 验收（B1/B2 集成） | ⬜ 待执行（需 DB 环境） |
| 用户确认 | ⬜ |
| 双仓库推送 | ⬜ |

---

## 12. 复审条件闭环对照（`audit-defect1-repair-v2-20260929.md`）

| 条件 | 要求 | 闭环处置 |
|---|---|---|
| **C1**（低-中）执行节奏须显式裁定 | 二选一写死到执行 SOP，闭环 F4 的「同日多轮」盲区；若选 async 须先用 1 轮实测确认 | ✅ **选 async 写死**（§3.P1 执行 SOP 表）：全程 `export_async=true` + **A0 前置实测** + **任一轮失败即当日停止、次日重试**（F4 已证次日必过）。证据：run 2（async）28 批 0 budget 错误；run 1 失败间隔 61s = 60s 同步预算 |
| **C2**（低-中，文档卫生）因果订正随实施落地 | 在实施 PR 内直接修正 2 处表述，勿另立待办 | ✅ **已完成**（§6）：根因文档 §5.4/§6.1 + attempt1 §2.2/§2.3/§4 全部订正；grep 校核无残留 |
| **C3**（低危）P2② 30-min 预算须 B2 充分验证 | B2 覆盖①截断后续做 ②零重复 ③**正常低事件量不触顶** | ✅ 已并入 B2 三子例（§5.B2） |
| **C4**（nit）run 1 批号不一致 | 统一口径 | ✅ 已统一为 **batch 130/136**（§1.1），含 61s 间隔证据 |

> 复审总体评价（原文摘）：*「V2 是一次高质量的返工……所有被引用的代码事实经复审独立核对全部属实」*。

---

## 13. 生效声明

- 本方案 **V2 已获复审通过（CONDITIONAL PASS）**；
  C1/C2 已闭环（§12），C3/C4 纳入验收（§5）。
- **可进入实施**（六步流水线第 3 步）。
- 实施前须建回退点（`git stash create -u` + `stash store`），
  按 §3 改动范围实施，按 §5 验收，**验收通过并获用户确认后**方可提交/推送。
