# 修复方案：`mcp_stock_basic` 补齐 `dataset_kind: "snapshot"`（快照表水位一致性）

- 状态：**待审计**（未实施）
- 日期：2026-10-01
- 性质：框架层配置一致性修复（不涉及引擎行为/性能变更）
- 铁律流程：方案 → 审计 → 实施 → 验收 → 用户确认 → 双仓库推送

---

## 1. 问题定义

### 1.1 现象

`mcp_stock_basic` 增量拉取**成功后**，`source_watermark` 中无 `mcp/stock_basic/daily` 记录，
本轮日志显示 `watermark→None`。因此每次增量运行都把 `stock_basic` 当作「首次全量」，
从 `start_date`（2020-01-01）重拉。

本轮（2026-10-01）实测日志：

```
22:05:23 [mcp_stock_basic_mcp_20261001_220513_5a662c] ✅ raw=5243 ... watermark→None
```

启动前基线核对：`source_watermark` 仅含 `etf_basic`、`etf_daily` 两条，无 `stock_basic`。

### 1.2 根因

水位推进逻辑按 `task.get("dataset_kind")` 分流（`quantstudio/pipeline/daemon.py`）：

```python
# L1160（非流式）与 L1362（流式）同款分流
if task.get("dataset_kind") == "snapshot":
    new_watermark = self._snapshot_watermark(end)   # 窗口结束日 Asia/Shanghai 午夜毫秒，恒有值
else:
    new_watermark = self._max_date(res.passed_df, table)  # 取表内 time_key 最大值；无时间列→None
```

- `_snapshot_watermark(end)`（L3228）：把窗口 `end` 归一化为 Asia/Shanghai 午夜毫秒，**恒返回非 None**。
- `_max_date(df, table)`（L3470）：按 schema 的 `time_key` 取本批最大日期；`time_key` 不在列中或空表 → 返回 `None`。

`stock_basic` 是**股票基础信息快照表**（主键 `code`，无按日时间列），`_max_date` 找不到
`time_key` 列 → 返回 `None` → 水位不推进。

### 1.3 不一致证据

| 位置 | `stock_basic` 声明 | `etf_basic` 声明 | 一致性 |
|---|---|---|---|
| `alignment_rules.json`（对齐契约） | `"dataset_kind": "snapshot"`（L1985） | `"dataset_kind": "snapshot"`（L1095） | 两者一致 |
| `collector_tasks.json`（mcp_only） | **缺失** | `"dataset_kind": "snapshot"`（L14） | **不一致** |
| `collector_tasks.json`（prehandover_staging） | 缺失 | 有（L17） | **同病** |

结论：对齐契约早已把 `stock_basic` 归为快照表，但 `collector_tasks.json`（两个 profile）漏标。

---

## 2. 改动范围

**仅 1 个文件、1 处字段**：

- 文件：`config/profiles/mcp_only/collector_tasks.json`
- 位置：`mcp_stock_basic` 任务（约 L190-218），在 `"freq": "daily",` 之后补一行：

```json
"dataset_kind": "snapshot",
```

（与 `mcp_etf_basic` 的字段位置保持一致。）

**明确不做的**：
- 不改任何 `.py`（引擎/适配器/对齐器零改动）；
- 不改 `alignment_rules.json`（已正确）；
- 不触碰 `config/profiles/prehandover_staging/collector_tasks.json`（该文件当前为他会话在途未提交改动，本次不涉入，仅列为后续同步项）；
- 不改 `config_legacy_deprecated/`（已废弃目录，且其中无 `stock_basic` 任务——仅含 legacy tushare 源 `etf_basic`，与本修复无关）。

---

## 3. 影响面

`dataset_kind` 在 `daemon.py` 共 3 个消费点，逐项评估：

| 消费点 | 位置 | 修复前 | 修复后 | 是否受影响 |
|---|---|---|---|---|
| 非流式水位分流 | L1160 | `_max_date`→None | `_snapshot_watermark(end)` | **是（预期）** |
| 流式水位分流 | L1362 | `_max_date`→None | `_snapshot_watermark(end)` | 是（stock_basic 走非流式，此项逻辑同款但未命中） |
| `skip_unchanged` 快照行过滤 | L3168 | 不触发 | 不触发 | 否（需 `skip_unchanged:true`，stock_basic 无此旗标） |

修复后行为：
- `stock_basic` 水位推进 = 窗口结束日 Asia/Shanghai 午夜毫秒，与 `etf_basic` **完全一致**；
- 写入路径（align → validate → upsert，主键 `code`）**不变**，行数/隔离/校验结果不变。

增量窗口语义（与 `etf_basic` 既有行为同源）：
- 首次：`last=None` → `start=start_date` 全量拉 → 水位 = 当日午夜；
- 同日重跑：`start = 水位+1 > end` → 「水位已追平，无需拉取」跳过；
- 跨日：`start = 水位+1` 正常进入拉取。

---

## 4. 验收标准

1. **配置一致性**：`mcp_stock_basic` 与 `mcp_etf_basic` 的 `dataset_kind` 均为 `"snapshot"`，
   且与 `alignment_rules.json` 中 `stock_basic` / `etf_basic` 的声明一致。
2. **行为验收（水位推进）**：单跑 `--task mcp_stock_basic` 后，
   `source_watermark` 出现 `mcp/stock_basic/daily`，`last_date` = 窗口结束日 Asia/Shanghai 午夜毫秒；
   日志 `watermark→` 非 `None`。
3. **续传验收**：立即重跑同命令，`INCREMENTAL` 起点 = 水位+1，且「水位已追平，无需拉取」跳过
   （与 `etf_basic` 本轮行为一致）。
4. **回归验收（纯增益）**：
   - 其余 18 张映射表 + 67 张 passthrough 表的 `dataset_kind` 字段不变；
   - `stock_basic` 本批写入结果（rows / 隔离 / validator 通过数）与修复前逐项一致；
   - 相关测试套件全绿，至少覆盖 `tests/test_etf_basic_pipeline.py`（含
     `test_etf_basic_task_is_single_source_snapshot_and_daily_watermark`、
     `test_snapshot_incremental_write_skips_unchanged_and_upserts_changes`）；
   - 建议（可选）新增 `stock_basic` 配置一致性断言（对齐 etf_basic 既有断言），
     作为长期回归保障；若不加，则以验收项 1/2/3 手动覆盖。

---

## 5. 回退条件

- 触发条件：修复后 `stock_basic` 出现「水位推进到未来导致永久跳过」或「数据漏拉」等异常。
- 回退方式：删除 `mcp_stock_basic` 的 `dataset_kind` 字段即可恢复原行为
  （水位回退到 `_max_date→None`，每次全量重拉）；若已写入误水位，清理 `source_watermark`
  中 `mcp/stock_basic/daily` 记录。
- 回退后需重新审计，禁止静默回退。

---

## 6. 后续同步项（非本次范围）

- `config/profiles/prehandover_staging/collector_tasks.json` 存在同病（`stock_basic` 漏标
  `dataset_kind: "snapshot"`）。该文件当前为他会话在途改动，待其落地后按同方案补齐，并在
  双仓库同步时一并核对。
