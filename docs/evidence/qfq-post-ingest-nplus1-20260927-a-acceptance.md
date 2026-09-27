# QFQ post-ingest 事件发现 N+1 事务缺陷 — 实施验收证据（六步流水线 ④）

| 项 | 值 |
|---|---|
| 实施 | TraeCode（主仓线执行会话） |
| 呈审 | 跨任务总调度 |
| 日期 | 2026-09-27 |
| 关联方案件 | `docs/qfq-post-ingest-discovery-nplus1-design.md`（A 件 ①，已定稿） |
| 关联取证件 | `docs/evidence/qfq-post-ingest-nplus1-20260927.md`（H1 定谳 + 双基线） |
| 状态 | **实施完成 + 影子/scratch 验收全项 PASS**（待总调度独立实测复采） |
| 缺陷号 | A（`post_ingest` 事件发现相位·性能） |

---

## 1. 落盘清单与前后指纹

| 文件 | 写前 SHA256 | 写后 SHA256 | git diff |
|---|---|---|---|
| `quantstudio/pipeline/qfq_event_discovery.py` | `BBB1A7B0…98D6` | `2B40AC63…C998` | +271 / −67 |
| `quantstudio/pipeline/qfq_discovery_baseline.py` | `8C6B6B4C…D2C1` | `ED451A90…1051` | +92 |

- **写前快照（create + store）**：`git stash@{0} = "A-impl pre-snapshot 20260927 (bef1b6f63f05fe8390c5462da6570caef34c2275)"`（QuantStudio 仓）；
  cwd 侧另存字节级备份 `d:\miniQMT策略实盘\trading-battle-back\_a_snapshot\*.pre|*.post`。
- **精确清单**：本件仅动上述 2 文件；`git diff --stat` 复核 `2 files changed, 296 insertions(+), 67 deletions(-)`。
  （仓内另有 21 个与本件无关的既有改动，未触碰、未纳入。）
- 编码核验：`qfq_discovery_baseline.py` 保留原 BOM（EF BB BF）；`qfq_event_discovery.py` 无 BOM（与原一致）。
- 语法核验：`py -3.11` import / compile 通过。

## 2. 批粒度选择依据（裁定 §8-2「选择依据入证据」）

1. **400KB 上限的来源 = QuestDB 约定，非 DuckDB**：`project_memory.md` 记「**QuestDB** 的 INSERT 语句必须限制为每条 ≤400KB 防止服务器崩溃（1MB+ 语句导致断开连接）」。本件写入面为 **DuckDB**。
2. **DuckDB 实测无此约束**（能力探针 `_a_probe_bulk_caps.py`，DuckDB 1.4.5 / pandas 3.0.1）：

| 档位 | SQL 文本字节数 | 行数 | 单条 INSERT 耗时 |
|---|---|---|---|
| 400KB 档 | 254,738 B | 1,200 | 0.182 s（ok） |
| 4MB 档 | **2,557,138 B** | 12,000 | 1.787 s（ok） |

3. **本实现的 SQL 文本与规模无关**：批量路径用 `INSERT OR IGNORE … SELECT … FROM <TEMP 关系>`，
   **SQL 文本 ≈1 KB、绑定参数 5 个**，54,387 行数据经 TEMP 关系 + DataFrame 视图流转，**不进入 SQL 文本**。
4. **结论：单批全量（批数 = 1）**，不按 code 分桶；「超限按 code 分桶」备用路径**未启用**（无触发条件）。
5. 单体规模：**N = 54,387** 行单批。

## 3. §5 六项验收逐条实测

**环境**：DuckDB **1.4.5**；`py -3.11`（Python 3.11.9 / pandas 2.3.3）；候选集与 baseline 均为**生产库只读快照**
（候选 54,387 行、baseline 54,387 行）；全部写入 `%TEMP%\qfq_forensics_20260927\` scratch 库；
**生产库全程零写**（仅 read_only 连接）。
**产物**：harness `_a_batch_verify.py` → `a_batch_verify_result.json`。对照物为取证期**原逐行实现**产物 `scr_a_run1/2/3.db`。

### 验收 1｜等价性（最高优先）— PASS

逐位对照口径：`qfq_trigger_queue` 16 业务列 + `qfq_discovery_baseline` 8 业务列；**排除** `created_at`/`updated_at`/`applied_at`/`baselined_at` 时间戳列（见 §5-1 差异声明）。

| 场景 | baseline 前置态 | batch trig n / 对照 n | batch base n / 对照 n | 集合差 | equal |
|---|---|---|---|---|---|
| run1 | 空表 | 54,387 / 54,387 | 54,387 / 54,387 | ∅ / ∅ | **true** |
| run2 | 生产 baseline | 0 / 0 | 54,387 / 54,387 | ∅ / ∅ | **true** |
| run3 | `applied=NULL & pending=NULL` | 54,387 / 54,387 | 54,387 / 54,387 | ∅ / ∅ | **true** |
| 子集 3,000 行 | 空表 | 3,000 / 3,000（rowwise） | 3,000 / 3,000 | ∅ / ∅ | **true** |

- 子集另做 `new_records` **顺序**对照（batch 按 `rn` 保扫描序 vs rowwise append 序）：**逐位相等**（`new_records_equal=true`）。
- ⇒ 三全尺度场景以**原逐行实现**为真值，业务列逐位等价；子集对 refactor 后 rowwise 亦等价。

### 验收 2｜事务量 O(N) → O(1) — PASS

| 场景 | BEGIN | COMMIT |
|---|---|---|
| run1 / run2 / run3 / run4（全尺度） | 1 | **1** |
| 子集 batch | 1 | **1** |
| 子集 rowwise（对照） | 3,000 | **3,000** |

- 对照：改前全尺度 = 54,387 × 3 = **163,161** 次显式 COMMIT（取证件实测）⇒ 降至 **1**。

### 验收 3｜性能相对实测双基线下降 ≥90% — PASS

| 场景 | 逐行实测（基线） | 批量实测 wall | 降幅 | 提速 | 判据目标 | 结论 |
|---|---|---|---|---|---|---|
| run1（空 baseline，基线 A 最坏） | 1362.24 s | **2.406 s** | 99.82% | **566×** | ≤136.2 s | **PASS** |
| run2（生产 baseline，零 reservation） | 725.89 s | **1.340 s** | 99.82% | **541×** | ≤136.2 s | **PASS** |
| run3（半满，基线 B 现场最可能） | 1103.85 s | **2.463 s** | 99.78% | **448×** | ≤110.4 s | **PASS** |
| 子集 3,000 行 | 64.382 s | 0.276 s | 99.57% | 233× | — | PASS |

CPU 时间：run1/2/3 批量 3.531 / 1.641 / 3.156 s vs 逐行 3821.4 / 1373.1 / 2877.5 s。

### 验收 4｜幂等（二次 NOOP）— PASS

对 run1 结果库**复跑**：`n_new_records=0`、COMMIT=1、`qfq_trigger_queue` 仍 **54,387**、`pending` 仍 **54,387**
⇒ 不重复施加（decided==0）。

### 验收 5｜告警不丢失 — PASS

构造 `baseline` 身份不一致（`price_source='wrong_src'`）+ trigger 预置，两路径**均抛**：

- rowwise：`DiscoveryBaselineError: trigger=990efacd… 与 baseline pending slot 不一致`；
- batch：`DiscoveryBaselineError: 批量 pending slot 与既有 trigger 不一致（1 行）: [('stock_dividend|600812|1781107200000', '990efacd…', …)]`。

另做**全尺度**断言分支实测（run4：54,387 条一致 trigger 预置）：
`assert_branch = no-raise(expected)`、wall **1.393 s**、COMMIT=1、`n_new=0` ⇒ 断言分支本身不吞告警、且在大规模下不引入额外开销。

### 验收 6｜回归 — PASS（详见 §4）

## 4. 回归测试

目标子集（与本件改动面直接相关的 7 个测试文件）：**156 passed / 1 failed**。

- 唯一 FAIL = `tests/test_qfq_resident_orchestrator.py::test_require_bootstrap_fail_closed`
  （断言 `s.bootstrap_required is True`，实得 `False`；`status='finalized_held'`、error 文案为 require_bootstrap fail-closed）。
- **证伪为 pre-existing（与 A 件无关）**：将两文件回退至写前指纹（`BBB1A7B0…` / `8C6B6B4C…`）后，
  **同一用例、同一断言、同一 error 文案同样 FAILED**；恢复写后指纹后结果不变。
  ⇒ A 件**非其原因**，本件**不越界修改**（登记为既有缺陷，另案）。
- `tests/test_qfq_event_discovery.py` **9 passed**（该套走 `xtquant-legacy` 分支，**不含**新批量路径；
  批量路径由 §3 全尺度 scratch 覆盖 —— 该套未覆盖批量路径系已知事实，已按裁定走影子/scratch 补全尺度等价）。

## 5. 实现差异（诚实边界）

1. **时间戳**：全批共享同一 `now` 作 pending 占槽 `updated_at`（逐行实现逐行取秒级 `now_ts`）
   —— 仅影响 `*_at` 时间戳列，业务列逐位相同（§3 验收 1 的对照口径即已排除）。
2. **回滚粒度**：断言失败时**整批回滚**（逐行实现只回滚触发该断言的行）
   —— 失败即停轮，「不吞告警」语义不变；**生产成功路径不受影响**。
3. **断言覆盖面**：批量断言对 `reserved ∧ trigger_pre_existing` 集合**逐条**校验（不降级为抽样）。

## 6. 回退

- **开关式（首选）**：`QFQ_DIVIDEND_SCAN_BATCH=0` → 逐行实现（默认批量）；不改数据、不改 schema、无需重启即生效。
- **代码回退**：`git revert` 本件单提交（2 文件）。
- 回退判据：任一硬不变量证明失败，或生产出现 trigger 丢失 / 重复。

## 7. 生产前置态旁证（只读实测，就绪条件 ② 相关，仅登记）

2026-09-27 23:3x 静止只读实测（生产库零写）：

| 指标 | 值 |
|---|---|
| `qfq_discovery_baseline` 行数 | 54,387 |
| `pending_trigger_id IS NOT NULL` | **54,288** |
| `applied_payload_hash IS NOT NULL` | 2,276 |
| 两者均 NULL | 0 |
| `qfq_trigger_queue` 行数 | **111,610** |
| 候选（`div_proc='实施' AND ex_date IS NOT NULL`） | 54,387 |

- ⇒ 生产 baseline 已大面积**占槽**（中断轮次遗留），对应 **run2 场景**：重启后本轮 `scan_stock_dividend`
  预期 `reservation≈0`、单批 ≈1.3 s 量级（实弹以周一实测为准，见 §8）。
- `qfq_trigger_queue=111,610` 与方案件 §3.3 现场值一致，印证候选 2（索引维护成本）为生产/scratch 差额候因之一。

## 8. 待办（裁定追加项）

- **周一重启后实测生产 `post_ingest` 时长入证据**（实弹对照 scratch 基线）；
  若仍显著慢于 run2 量级，则把候因 1（WAL 量级）/ 2（`qfq_trigger_queue` 索引维护）转正式归因。
