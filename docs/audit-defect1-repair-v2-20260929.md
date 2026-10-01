# 复审报告：defect1-repair-completion-design.md（V2）（2026-09-29）

> 复审对象：`docs/defect1-repair-completion-design.md`（V2 定稿）
> 关联取证：`docs/evidence/defect1-rework-verification-20260929.md`
> 前序审计：`docs/audit-defect1-repair-completion-design-20260929.md`（V1 NO-GO）
>
> ## 复审结论：**通过（CONDITIONAL PASS）—— 可进入实施，实施前须消 C1/C2 两个条件**
>
> V1 的两个 P0（F1 重锚链、F2 fetch 相位）均被**正确且可验证地**修正；
> F3/F4/F5/F6 全部闭环。复审期间我**独立核验了 V2 的关键代码断言**（非照单接受），
> 结论与 V2 自述一致。仅余 2 个实施前置条件（C1 执行节奏、C2 证据文档因果订正）
> 与 2 个低危提示（C3/C4），均不阻塞方案本身。

---

## 1. 独立代码核验（V2 关键断言逐条确认）

| V2 关键断言 | 复审核验 | 结果 |
|---|---|---|
| 失败后仍跑 `qfq_run_post_ingest` → 重锚循环 | `daemon.py:666` finally 仅判 `not _cancelled`；`qfq_run_post_ingest`(366)→`orch.run_post_ingest`(qfq_resident_orchestrator.py:1407)→**1481-1487 `for unit in units: _reanchor_security`** | ✅ 链路确凿 |
| P2① 守卫 `task_ok` 能止住事故 | 失败任务 `task_ok=False`、未 cancelled → 加 `task_ok` 后分支跳过 `qfq_run_post_ingest` → 重锚链不启动 | ✅ 直击根因 |
| P1 窗口分段传导到 fetch | `full_range` 的 `start=task.get("start_date")`、`end=task.get("end_date")`（`daemon.py:890-894`）；`run_once` 覆盖 task dict → `execute_task` 内 `run_task=dict(task)` 复制 → 流式 `_resolve_shard_paths(start,end)` 仅该子窗 | ✅ 有效 |
| P2②「截断续做」前提（units 持久化） | `_claim_and_merge`（qfq_resident_orchestrator.py:407-453）：`SELECT ... FROM qfq_trigger_queue WHERE status='pending'`；已处理者被 `_apply_trigger_outcome` 置非 pending；失败/中断者由 `recover_stale_in_progress`(1464)/`recover_pending_due`(1465) 回收 | ✅ 可续做 |
| `--start-date/--end-date` 当前不存在于参数表 | `daemon.py:3616-3641` 参数表确认无此二项（V2 为新增） | ✅ |
| client 层重试纪律已具备 | `mcp/client.py:155-167/416-427/533-597` 实测存在 retry_max/退避/预算/确定性不重试 | ✅ V1 P2 确属重复建设 |
| 续传对 fetch 无效 | 游标 `advance` 仅在写片后（`daemon.py:1322-1326`），fetch 前置无边界 | ✅ V2 放弃软停止正确 |

---

## 2. V1 审计项处置复核

| 项 | V1→V2 处置 | 复审 |
|---|---|---|
| **F1（P0）** 根因 B 定性 | 改为「失败后无界重锚链」；P2 移位至 `daemon.py:664` + 重锚循环加界 | ✅ 正确采纳；代码链路已独立确认 |
| **F1** P2 改动点错位 | 删除 client 层重复建设；改重锚链定界 | ✅ |
| **F2（P0）** P1 不覆盖 fetch | 改窗口分段驱动（放弃软停止），fetch 每轮有界 | ✅ 机理正确；传导链已确认 |
| **F2** 根因 A 因果 | 改为「fetch 前置全量未产出分片」，writer 逐片 autocommit | ✅ 与 writer.py:788-860 一致 |
| **F3** CLI 软停止出口 | 随软停止取消而消失 | ✅ |
| **F4** budget 分类 | F4 实验判为瞬时类；撤销「确定性不重试」 | ✅ 见 §3 C1 残留提示 |
| **F5** A3 残留行 | 增残留行核查 + 清单声明 | ✅ |
| **F6** 四重校验 | 改为「四重」 | ✅ |

---

## 3. 实施前置条件（须消，不阻塞方案定稿）

### C1（低-中）执行节奏须显式裁定，闭环 F4 的「同日多轮」盲区
F4 实验仅证明**次日**重试同窗口全部成功（9 个窗口 / 1 天），据此判「瞬时/配额类」。
但 `export_exceeds_time_budget` 是**单 export 作业的「服务端处理时长」预算**，非日累计配额；
F4 未验证**同一天内连跑 6 轮**（累计服务端负载）是否复现。V2 §10.3 计划 6 轮 / 4.5–10h，
若同一天连跑，存在复触发该预算的可能（run 1 即在单日内 12:21 触发）。

建议二选一写死到执行 SOP（而非「执行期可调项」）：
- **(a) 每轮隔日执行**（6 个自然日，与 F4 已证明的「次日即过」直接吻合）；或
- **(b) 全程 `export_async=true`** 并显式声明「async 绕过 60s 同步预算」——但 V2 §4 推论3
  称 async「绕过预算」属**未实验验证**的推断，建议先用 1 轮 async 实测确认不再报 budget 错误，再固化为默认。

（注：窗口分段本身已把单作业缩到 2 天粒度，单作业命中时间预算的概率显著下降，
故风险等级定为低-中，非阻塞。）

### C2（低-中，文档卫生）根因文档因果订正须随实施一并落地
V2 §6 承认 `defect1-root-cause-confirmed-20260928.md` §5.4/§6.1 仍保留错误的
「整体不落库 / 事务回滚」表述，并「在其后单列」修订。该证据文档会被实施者阅读，
错误因果会误导后续维护。**建议在 V2 实施 PR 内直接修正这 2 处表述**（约 2 行），
而非另立待办。

---

## 4. 低危提示（不阻塞）

### C3 P2② 30-min 默认预算须 B2 充分验证「截断-续做」不丢不重
`qfq_trigger_queue` 状态机（pending→in_progress→committed）支持续做，机理成立；
但 B2 必须覆盖：①截断后未处理 units 仍为 pending、下轮 `recover_pending_due` 回收；
②已 committed units 不重复提交（`_already_committed` 470 已有去重）；
③**正常低事件量 post_ingest 不触顶**（避免日常 forever 周期被 30min 预算误截断）。
建议 B2 增一个「成功任务 + 注入小预算」子例，断言截断后续轮最终全部 claimed 且零重复。

### C4（nit）run 1 批号不一致
V2 §1.1 写 run 1 失败于「batch ~131」，而 attempt1 / V1 取证均记「batch 126/136」。
属取证文档间口径不一致，建议统一（仅叙事精度，不影响方案）。

---

## 5. 总体评价

V2 是一次高质量的返工：
- 没有推倒重写，而是按审计 §3 建议精准移位（F1 重锚链定界、F2 窗口分段），
  并删除了 V1 中与真实现象错位且重复的 client 层 P2——判断克制、改动面小、符合「零影响」铁律；
- 所有被引用的代码事实（行号、链路、参数表）经复审**独立核对全部属实**；
- F4 实验为「瞬时类」判定提供了可复现证据，使「缩窗非前置」的结论成立，消解了 V1 的 A3 依赖矛盾。

**复审判定：通过（条件通过）。** 建议实施前消 C1（执行节奏裁定）、C2（证据文档 2 行订正），
C3/C4 在验收阶段闭环即可。按 V2 自身红线，复审通过后即可进入实施。

---

## 6. 复审证据索引

| 证据 | 位置 |
|---|---|
| post_ingest → 重锚循环链路 | `daemon.py:366/379/666` + `qfq_resident_orchestrator.py:1407/1481-1487` |
| 失败仍跑 post_ingest | `daemon.py:662-666`（条件仅 `not _cancelled`） |
| full_range start/end | `daemon.py:890-894` |
| units 持久化 + 续做 | `qfq_resident_orchestrator.py:407-453`、`_already_committed:470`、`recover_*:1464-1465` |
| 窗口分段参数不存在（待新增） | `daemon.py:3616-3641` |
| client 既有重试纪律 | `mcp/client.py:155-167/416-427/533-597` |
| F4 实验 | `docs/evidence/defect1-rework-verification-20260929.md` §3.2 |
