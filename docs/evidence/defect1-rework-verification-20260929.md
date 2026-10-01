# 缺陷1 返工取证：审计 F1/F2/F4 复核与新证据（2026-09-29）

> 本文记录针对审计报告（`docs/audit-defect1-repair-completion-design-20260929.md`）
> F1/F2/F4 三项的**独立复核结论**与**新增实验证据**，作为 V2 方案的事实依据。

---

## 1. F1 复核：5 小时空转真身 = 失败后无界重锚链（**审计正确**）

### 1.1 日志统计（`data/logs/daemon.log`，2026-09-28）

| 指标 | 实测 |
|---|---|
| 全天 ERROR 条数 | **2**（12:11:18、12:21:58；09:58/10:00 为 retry WARNING） |
| 唯一 `export_exceeds_time_budget` | **12:21:58** |
| 13:18–18:16 窗口 ERROR | **0** |
| `export 分批=128` 出现次数 | **115**（每次对应不同 code） |

### 1.2 逐股重锚链证据

```
[QFQ-ReanchorCheck] 000002/stock_daily 重锚后自洽通过 (2118 行)
[QFQ-ReanchorCheck] 000006/stock_daily ... (2076 行)
...
[QFQ-ReanchorCheck] 000534/stock_daily ... (2012 行)
→ 共 47 只，13:19–18:15（约 5 小时）≈ 6.4 min/只
```

### 1.3 代码机理（确证）

```python
# daemon.py:662-666  —— 条件只判 not _cancelled，不判 task_ok
finally:
    _cancelled = bool(self._task_cancelled)
    if (not _cancelled) and owns_qfq_cycle and self._qfq_cycle_id is not None:
        run_id = f"manual_{run_task.get('name','task')}_{uuid.uuid4().hex[:12]}"
        self._last_qfq_cycle_summary = self.qfq_run_post_ingest(run_id)   # ← 失败也执行

# qfq_resident_orchestrator.py:1480-1487 —— 循环无上限/无预算
for unit in units:
    outcome = self._reanchor_security(conn, run_id=run_id, asset_type=unit["asset_type"],
                                      code=unit["code"], trigger_ids=unit["triggers"], ...)
    self._apply_trigger_outcome(conn, run_id=run_id, unit=unit, outcome=outcome, ...)
```

**结论**：任务失败 → 仍跑 `qfq_run_post_ingest` → 逐股重锚 47 只/5h → 全市场 5,247 只
≈ **数周量级**。这就是「CPU ~120%、零写入」的真因，**非重试循环**。

### 1.4 附带确认：client 层重试纪律已具备（V1 的 P2 属重复建设）

`mcp/client.py:155-167`（`retry_max=5`、`backoff_sec=(30,60,120,240,480)`、`retry_budget_sec`）、
`416-427`（握手重试+预算）、`533-597`（确定性 vs 瞬时错误分类）。
→ V1 拟做的「确定性错误不重放 + 重试上限」**已存在**，V2 已撤销该设计。

---

## 2. F2 复核：fetch 是「全量前置阻塞」，软停止碰不到（**审计正确**）

### 2.1 代码事实

```python
# mcp_adapter.py:1596-1626  _resolve_shard_paths
for i, (bs, be) in enumerate(batches):          # ← 循环全部 136 批
    arts = self.client.export_dataset(...)      #    逐批 export
    for art in arts:
        local_parquet.write_bytes(...)          #    逐批落盘
        paths.append(local_parquet)
...
return paths, job_id                            # ← 全部完成后才返回
```

daemon 侧 `_run_with_source_streaming` 拿到分片迭代器后，才开始逐片
align→validate→write，`_task_boundary` 只在**每片写完后**调用（`daemon.py:1322-1326`）。

→ **fetch 期间（~10 小时）在 daemon 侧无任何边界可停**。

### 2.2 run 2 决定性证据

```
22:20:18  export 批次 28/136      ← 看门狗终止点
```
彼时**仍在 fetch 阶段，一片未 yield、一行未写**。

### 2.3 对 V1 设计的否定

- `--max-runtime-min 100` 在 fetch 阶段**永不触发**；
- 游标只在写片后推进 → fetch 期间游标不动 → 下轮 `_open_day_resume` 命不中 → 窗口不缩小；
- V1 §7.3「100 min/轮 × 7 轮」时间预算**不成立**；
- V1 的 A1 小样本验收会**假阳性通过**（3 批小窗 fetch 秒级完成，恰好绕开被审相位）。

**V2 处置**：改用**窗口分段驱动**（每轮 CLI 限定 start/end），
使每轮 fetch 有界、可自然跑完 → 无需软停止。

---

## 3. F4 判定实验：budget 错误属「瞬时/配额类」（**新增证据**）

### 3.1 实验设计

对 09-28 失败窗口组（batch 126-136 覆盖日期），09-29 用**生产原模式（sync）**
逐个重试 `create_export_job`：

### 3.2 结果

| 窗口 | 结果 | 耗时 |
|---|---|---|
| 2026-09-08 → 09-09 | OK | 12.1s |
| 2026-09-10 → 09-11 | OK | 12.1s |
| 2026-09-16 → 09-17 | OK | 15.1s |
| 2026-09-18 → 09-19 | OK | 13.0s |
| 2026-09-20 → 09-21 | OK | 0.1s |
| 2026-09-22 → 09-23 | OK | 12.1s |
| 2026-09-24 → 09-25 | OK | 12.4s |
| 2026-09-26 → 09-27 | OK | 0.1s |
| 2026-09-27 → 09-28 | OK | 0.1s |

**全部成功，无一复现。**

### 3.3 判定与推论

**属「瞬时/配额类」（随时间恢复），非「规模类」。**

1. **缩窗（`minute_export_target_rows`）不是 A3 的前置依赖** → 仍为可选另案；
2. 该错误**不得**归为「确定性、不重试」——否则误杀次日可成功的重跑；
3. `export_async=true` 为「稳」选项（绕过 60s 预算），sync 为「快」选项
   （~1.9 min/批 vs ~4.3 min/批）——执行期可调，非方案决策点。

---

## 4. 汇总：审计 F1-F6 处置对照

| 审计项 | 复核结论 | V2 处置 |
|---|---|---|
| F1（P0）根因 B 定性错误 | ✅ 成立 | 改正为「失败后无界重锚链」；P2 移位至 `daemon.py:664` + 重锚循环 |
| F1 P2 改动点错位 | ✅ 成立 | 同上 |
| F1 client 层重复建设 | ✅ 成立 | 删除 V1 的 P2 重试纪律设计 |
| F2（P0）P1 不覆盖 fetch 相位 | ✅ 成立 | 改为窗口分段驱动（放弃软停止） |
| F2 根因 A 因果表述错误 | ✅ 成立 | 已修正 root-cause §5.4/§6.1、attempt1 §2.3 |
| F3（P1）once CLI 软停止出口未定义 | ✅ 成立 | 随软停止取消而消失 |
| F4（P1）budget 分类依据不足 | ✅ 成立 | §3 实验判定为瞬时类 |
| F5（中）A3 残留行风险 | ✅ 成立 | A3 增残留行核查步骤 |
| F6（低）「三重」实为四项 | ✅ 成立 | 改为「四重」 |

---

## 5. 证据索引

| 证据 | 位置 |
|---|---|
| 09-28 全量日志（19,188 行） | `data/logs/daemon.log` |
| 逐股重锚链 | 同上（ReanchorCheck 47 条） |
| fetch 前置全量语义 | `mcp_adapter.py:1596-1626` |
| 失败仍执行 post-ingest | `daemon.py:662-666` |
| 重锚循环无界 | `qfq_resident_orchestrator.py:1480-1487` |
| client 层重试纪律 | `mcp/client.py:155-167`、`416-427`、`533-597` |
| F4 实验（本机直取 MCP） | 本文 §3.2 |
