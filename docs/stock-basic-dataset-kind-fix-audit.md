# 审计报告：`mcp_stock_basic` 补齐 `dataset_kind: "snapshot"` 修复方案

- 审计对象：`docs/stock-basic-dataset-kind-fix-design.md`
- 审计日期：2026-10-01
- 审计结论：**通过（修订后）**

---

## 一、总体结论

方案根因判定正确、改动范围最小、回退路径清晰，属「纯增益」配置一致性修复，
不涉及引擎行为/性能变更。审计提出 2 项文档完整性补充，已随方案修订；修订后予以通过。

## 二、逐项审计

### 1. 根因是否确凿 —— 通过

- 运行日志实证：`mcp_stock_basic` 完成后 `watermark→None`（2026-10-01 实测）。
- 代码路径复核（`daemon.py`）：
  - L1160/L1362 水位分流：`dataset_kind != "snapshot"` → 走 `_max_date`；
  - `_max_date`（L3470）按 `schema.time_key or "time"` 取最大值，无该列 → `None`；
  - `stock_basic` schema（`alignment_rules.json` L1978）`primary_key=["code"]`，
    列集合无 `time`/`trade_date`，顶层无 `time_key` → `_max_date` 必返 `None`。判定成立。
- 契约不一致坐实：`alignment_rules.json` 已声明 `stock_basic.dataset_kind=="snapshot"`，
  而 `collector_tasks.json` 漏标（仅 `etf_basic` 有）。

### 2. 改动范围是否最小、正确 —— 通过

仅 `config/profiles/mcp_only/collector_tasks.json` 的 `mcp_stock_basic` 补 1 个字段，
与 `mcp_etf_basic` 字段位置一致，不改代码、不改契约、不碰他会话在途文件。

### 3. 影响面是否完整 —— 通过（补 1 处）

`dataset_kind` 全项目消费点核实为 `daemon.py` 3 处（L1160 / L1362 / L3168）：

- L1160/L1362（水位分流）：本次修复目标，预期变更；
- L3168（`skip_unchanged` 快照行过滤）：需 `skip_unchanged:true` 才触发，
  `stock_basic` 无此旗标 → 不受影响。

方案已列全 3 处并逐项判定。**补充项**：测试覆盖（见下）。

### 4. 测试覆盖（审计补充）—— 已补进方案

`tests/test_etf_basic_pipeline.py` 是 `dataset_kind/snapshot` 行为的既有回归基线：

- `test_etf_basic_task_is_single_source_snapshot_and_daily_watermark`（L138）：
  断言 `mcp_etf_basic` 的 `dataset_kind=="snapshot"` 及 `_snapshot_watermark` 语义，
  **当前无 `stock_basic` 对应断言**。
- `test_snapshot_incremental_write_skips_unchanged_and_upserts_changes`（L100）：
  覆盖 `snapshot + skip_unchanged` 写入路径（内联 task，不读配置，不受本改动影响）。

审计要求：方案验收标准已将上述测试显式纳入回归基线，并建议（可选）新增
`stock_basic` 一致性断言作为长期保障。

### 5. 配置同步范围 —— 通过（补 1 处）

- `prehandover_staging`：同病，方案已列为后续同步项（他会话在途，不涉入）✅；
- `config_legacy_deprecated/`：审计核实该目录已废弃，且**无 `stock_basic` 任务**
  （仅 legacy tushare `etf_basic`），与本修复无关——方案已显式声明不纳入。

### 6. 验收标准是否可执行 —— 通过

配置一致性 / 水位推进 / 续传跳过 / 回归全绿 四项均可实证，且已具体化测试基线。

### 7. 回退条件 —— 通过

删除字段即恢复原行为 + 可选清理误写水位，回退路径清晰、无副作用。

## 三、审计提出并已修订的项

| # | 项 | 处置 |
|---|---|---|
| 1 | 方案「明确不做的」未声明 `config_legacy_deprecated/` | 已补声明 |
| 2 | 验收标准「相关测试套件全绿」未具体化测试基线 | 已具体化 + 建议补 stock_basic 断言 |

## 四、遗留提示（不阻塞）

- 快照表水位语义固有限制：`_snapshot_watermark(end)` 将水位推进至窗口结束日午夜，
  「同日重跑跳过、跨日重拉」与 `etf_basic` 既有行为一致，非本次引入，不属本修复风险。
- 建议后续（独立项，非本次范围）：考虑让 `config_lint` 增加
  `dataset_kind` 与 `alignment_rules` 的一致性校验，从源头防同类漏标回归。

## 五、结论

审计通过。可进入实施阶段（仅改 1 文件 1 字段），实施后按方案 §4 验收。
