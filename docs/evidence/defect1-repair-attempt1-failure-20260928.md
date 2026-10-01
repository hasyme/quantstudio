# 缺陷1 数据修复第一次尝试 — 失败与事故记录（2026-09-28）

> **结论：8 小时运行（08:14–18:16）零写入，数据与修复前逐位一致；进程陷入约 5 小时
> 空转重试循环，已人工终止。**

---

## 1. 结果：数据未发生任何变化

| 判据 | 修复前基线 | 本次运行后 | 结论 |
|---|---|---|---|
| 配对数 | 1,561,348 | 1,561,348 | 同 |
| 逐位相等 | 801,582 (51.34%) | 801,582 (51.34%) | **同** |
| ≤0.5% | 1,272,766 (81.52%) | 1,272,766 (81.52%) | **同** |
| >5%（重度） | 59,045 (3.78%) | 59,045 (3.78%) | **同** |
| `stock_minutes` 行数 | 194,994,044 | 194,994,044 | 同 |
| `max_time` | 1789542000000 (2026-09-17) | 1789542000000 | 同 |

**判定：修复未生效，且无副作用（未写坏数据）。**

---

## 2. 故障根因

### 2.1 直接原因：云端导出超时预算

```
ERROR FAILED[streaming]: create_export_job 服务端错误: export_exceeds_time_budget
  hint=split the request: narrow time_start/time_end, filter ts_codes, or lower row_limit
  → quantstudio.pipeline.mcp.errors.MCPExportBudgetError
  触发于 126/136 批（2026-09-08 之后），时刻 12:21:58
```

前兆：12:13–12:21 期间两次 `TimeoutError: _fetch 超过 90.0s 未返回`。

### 2.2 放大器：失败后陷入重试循环（**本次最重要的教训**）

> ## ⚠ 本节定性已于 2026-09-29 被审计推翻（F1），修正见 §2.2-corrected

### 2.2-corrected（2026-09-29，依审计 F1 复验日志）

**原定性「失败后重试循环」错误。** 真身是**失败任务仍执行 QFQ 收尾链**：

| 日志复验 | 数值 |
|---|---|
| 09-28 全天 ERROR | 仅 **2 条**（12:11、12:21） |
| 唯一 `export_exceeds_time_budget` | **12:21:58** |
| 13:18–18:16 窗口内 ERROR | **0 条**（不存在"同一处再次失败"） |
| `export 分批=128` 出现次数 | **115 次**，每次对应**不同 code** |
| 该窗口 `stock_minutes` 写入 | **0 次** |

**真实机理**：任务失败（`task_ok=False`，未 cancelled）→ `execute_task` finally 分支
条件仅判 `not _cancelled`、**不判 `task_ok`**（`daemon.py:664`）→ 仍调用
`qfq_run_post_ingest`（`daemon.py:666`）→ orchestrator 逐股重锚
（`for unit in units:`，`qfq_resident_orchestrator.py:1481-1487`，**无上限/无预算**）
→ 日志实证 ReanchorCheck 000002→000534 共 **47 只**通过、约 6.4 min/只
→ 若遍历 5,247 只 ≈ **数周量级**。

即「5 小时 CPU ~120%、零写入」= **失败任务后无界执行的 QFQ 收尾链**，非重试循环。
（重试纪律在 `mcp/client.py:155-167/416-427/533-597` 早已具备，本轮无需改动。）
修正后方案见 `docs/defect1-repair-completion-design.md` §3.P2。

### 2.3-corrected：为何"125/136 批成功"却没写库

**原表述「流式事务整体提交/回滚」错误。**

- writer 逐片 `write()` 独立连接、**无显式事务、upsert 后关连接 → 逐片 autocommit 即持久**
  （`writers.py:788-860`）；「全部片写完后一次」的**只有水位**（`daemon.py:1343-1350`）。
- 真因 = **fetch 是「全量前置阻塞」**：`_resolve_shard_paths`（`mcp_adapter.py:1596-1626`）
  先循环取完全部 136 批并落盘，**才** return 分片迭代器 →
  fetch 未完成 → **一片都未 yield → 无任何写发生**。
- 缓存 parquet 仍留在 `data/mcp_landing/export`，可复用。

---

## 3. 已确认的正面结论（不受本次失败影响）

运行日志中反复出现口径分支行，**直接证明 A′ 按设计工作**：

```
线1 还原 stock_minutes/1min: 0 行 / 0 码 → raw（口径=raw）     ← A′ 不还原（缺陷1 根修）
线1 还原 stock_daily/daily:  2124 行 / 1 码 → raw（口径=qfq）  ← A′ 还原
```

且**全程零** `ValueError: 线1 还原无参考`——v3 的参考死锁
（旧日志 `159119@09-07`、`159325@09-11` 崩溃）**未再出现**，A′ 拆锁有效。

---

## 4. 待决问题（**均已解决，2026-09-29 更新**）

1. **`export_exceeds_time_budget` 的边界** → ✅ **已实验判定**：
   09-29 对失败窗口组（09-08~09-28 各 2 天窗）用 sync 原模式逐个重试，**全部成功（12~15s）**
   → 属**瞬时/配额类（随时间恢复）**，非「规模类」。
   故**缩窗不是 A3 前置**（仍为可选另案），且**不得**将该错误归为「确定性、不重试」。
2. **是否需要调参** → ✅ 结论：**不需要缩窗**（依 1）。`export_async=true` 已配置为「稳」
   选项；sync/async 为执行期可调项（sync ~1.9 min/批 vs async ~4.3 min/批）。
3. **缓存复用** → ✅ `data/mcp_landing/export` 缓存有效（TTL=604800s=7 天），
   重跑命中缓存跳过已取批次。
4. ~~**重试循环属框架缺陷**~~ → ❌ **该定性错误（审计 F1）**：
   真身是**失败任务仍执行 `qfq_run_post_ingest` 的无界逐股重锚链**
   （`daemon.py:664` 条件不判 `task_ok`；`qfq_resident_orchestrator.py:1481-1487` 循环无上限）。
   已立项于 `docs/defect1-repair-completion-design.md` §3.P2（同批实施）。

---

## 5. 处置

- 已 `Stop-Process -Force` 终止 PID 21480；确认无残留 python 进程，DB 锁已释放。
- 数据零变化 → **无需回退**（回退点 `34b73787` 仍有效，未使用）。
- 下一步：按 V2 方案（`docs/defect1-repair-completion-design.md`）复审 → 实施
  （窗口分段驱动 + 重锚链定界）。
