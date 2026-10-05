# 审计报告：分钟数据支持与「全量/增量拉取」机制删除方案

> **审计对象**：`docs/minute-data-pull-removal-design.md`  
> **被审计方案**：`docs/minute-data-pull-removal-design.md`（HEAD `7c3e7fb`）  
> **审计日期**：2026-10-01  
> **审计结论**：**不通过，方案须修订后重审**  
> **禁止事项**：在 R1/R2 未定夺前，禁止进入步骤③实施。

---

## 1. 审计背景

本次审计依据 QuantStudio 项目铁律《框架层改动六步流水线》执行，针对「删除分钟数据支持与全量/增量拉取机制」设计方案进行步骤②审计。审计过程中核对了 `quantstudio/`、`config/`、`tests/`、`scripts/`、`README.md` 及相关 docs 的代码事实，确认方案在**改动范围精确性**上存在多处重大遗漏与事实错误，可能引入静默语义变更。

---

## 2. 阻断级缺陷（必须修订）

### R1. Part A 与 QFQ 水位协调链路严重耦合，方案完全未识别

**问题描述**：方案将 `_advance_or_defer_watermark`（`daemon.py:477-506`）列入删除清单，标注为"水位推进唯一入口（红线）"，但**未识别它同时是 QFQ 编排器水位延迟提交的必经通道**。

**代码事实**：

- `daemon.py:488` → `cfg.can_coordinate_watermark(table)`
- `qfq_orchestrator_types.py:526-528` → `return self.enabled and table in self.COORDINATED_PRICE_TABLES`
- `COORDINATED_PRICE_TABLES = frozenset(PRICE_TABLES)`（`:433`），即四张价格表

**当前 QFQ 编排器处于启用状态**（`config/profiles/mcp_only/collector_tasks.json:2720-2743`）：

```json
"qfq_orchestrator": {
  "enabled": true,
  "require_bootstrap": true,
  "price_source": "mcp",
  "freqs": ["1min"],
  "watermark_policy": "hold_until_consistent",
  "generation_mode": "dynamic",
  "source_generation": "mcp-gen1"
}
```

**风险**：`_advance_or_defer_watermark` 删除后，`stock_daily`/`etf_daily` 的水位将绕过 `defer_watermark` 直写 `source_watermark`；而编排器 `_commit_or_hold_watermarks`（`qfq_resident_orchestrator.py:893-917`）依据 `qfq_watermark_intent` 的 pending 记录决定提交/hold。intent 永不再产生 ⇒ gate 永久空转，"gate 通过才推进水位"红线被静默解除，属于**回测数据正确性层面的语义变更**。

**同时，方案第 64 行结论"`source_watermark` 表本体保留但停止写入"与本条冲突且事实错误**：只要 `writer.advance_watermark` 之外的调用点仍在（见 R2），水位仍在写。

---

### R2. `advance_watermark` / `get_last_date` 删除清单不完整，且与 A-β 决定自相矛盾

**问题描述**：方案 A4 只列了 `BaseWriter` + `DuckDBWriter` 两处定义（`writers.py:394-403`、`1033-1077`），**漏掉 3 类活跃调用方**。

**遗漏清单**：

| 遗漏项 | 位置 | 性质 |
|---|---|---|
| 编排器直接 upsert | `qfq_resident_orchestrator.py:919-936` `_advance_watermark`（显式写 8 列 `source_watermark`） | **生产活跃**，非死代码 |
| `watermark_advancer` 回调注入 | `daemon.py:328` 把 `self.writer.advance_watermark` 注入编排器 | R1 同一链路 |
| `get_last_date` 消费方 | `audit_watermark_coverage.py:113,175`、`fm_export.py:159`、`gui/db_helper.py:294` | 只读，方案 A4 已决定改语义，但未列这些文件 |

**语义变更风险**：方案 A4 裁定 `get_last_date` "语义改为表内实际 MAX(time) 直查"。但 `writers.py:1033-1041` 现签名 `(source, table, freq)` 是**按 source 分区**读水位；改为表内 MAX 后，`index_daily` 等多个 source 共表的场景返回值将从"该源进度"变成"全表最大日期"。`tests/test_index_daily_daemon_pipeline.py:206-219`、`:293-330` 直接断言该语义。

**结论**：`get_last_date` 语义变更不是等价重构，而是行为变更，方案却将其写在"删除"项下，未标注为需独立验证的语义变更。

---

## 3. 中等级缺陷（须补充）

### R3. GUI/入口删除清单不完整

方案 A5 只列 `workers.py` / `task_tab.py` / `main_window.py`。实际还有：

- `quantstudio/gui/daemon_process.py:135` — `start_once_subprocess(..., mode: str = "incremental")`
- `quantstudio/pipeline/daemon_lifecycle.py:655,662` — `task.get("mode","incremental") != "incremental"` + `execute_task(..., mode="incremental")`（**常驻循环的实际调度点，方案完全未提**）

漏掉 `daemon_lifecycle.py` 将直接导致常驻循环在 `mode` 字段移除后行为未定义。

---

### R4. `mode` 移除会连带破坏 3 处非"拉取"语义依赖

方案称 `mode` 字段移除后"`full_range` 语义成为唯一语义"。但 `mode` 还被以下逻辑消费：

1. `daemon.py:1814` / `:2074` — `has_usable_result = total_written[0] > 0 or (mode == "incremental" and fail_count[0] == 0)`（**失败判定语义**）
2. `daemon.py:1473,1545,1865` — per_date/per_stock 分支
3. `_authority_reconcile`（`:2339-2347`）— `runtime mode=full_range` 是**触发 DELETE 的前置条件**，且 `config_lint.py:199-206` 校验 `scope='full_range_only'`。

当前 `collector_tasks.json` 中**无任何任务声明 `authority_reconciliation`**（已核，0 命中），故该路径现为休眠。但删除后需明确它是"随之废弃"还是"改由其他条件触发"，方案未交代。

---

### R5. Part B 数据源清单遗漏 3 个生产文件

方案 B4 声称覆盖数据源适配层，但漏（均含分钟表活跃引用）：

- `quantstudio/pipeline/exporter.py:34-38,119` — `1min/5min/15min/30min/60min → stock_minutes` 映射表
- `quantstudio/pipeline/fm_export.py:45` — `WATERMARK_TABLES` 含两张分钟表
- `quantstudio/pipeline/source_capabilities.py:23-39` — 方案 B4 **写了此文件，但路径错误**：实际在 `quantstudio/pipeline/source_capabilities.py`，方案写成 `quantstudio/pipeline/sources/source_capabilities.py`（`sources/` 下无此文件）

**测试计数口径需修正**：方案 B9 称"测试约 232 处命中"，实测 **62 个测试文件**命中（`tests/` 共 272 个文件），且 `test_*minute*.py` 有 **18 个**（方案未列举 `test_ptrade_api_minute_include.py` 等）。

**QFQ 文件清单补充**：方案 B3 列了 8 个 QFQ 文件，实测至少还有：

- `qfq_formal_watermark_release.py:52-54` — 硬编码 `mcp_etf_minutes`/`mcp_stock_minutes` 任务名
- `qfq_cutover_activation.py:20`、`qfq_formal_canary.py:34` — `PRICE_TABLES` 定义
- `qfq_reanchor_schema.py:110-111,245-247,604-605` — `rows_stock_minutes` 列 + `STOCK/ETF` 分类
- `qfq_schema_contracts.py:63,626-628,907-909` — **建表 DDL 列**
- `qfq_orchestrator_cli.py:261`

其中 **`qfq_schema_contracts.py` 的 DDL 列与 schema 指纹直接相关**——删除会影响 schema 指纹校验（`qfq_schema_status.py` / `qfq_schema_migration.py`），方案对此**零提及**，属高风险遗漏。

---

## 4. 核对通过项

| 方案主张 | 核验结果 |
|---|---|
| `update_detector.py` / `task_resume.py` 可整文件删除 | ✅ 二者为独立模块，仅 daemon/GUI 导入 |
| `_run_minute_day` / `_load_minute_snapshots` 位置 | ✅ `backtest_engine.py:2331,2372` 准确 |
| 6 策略均为日线档 | ✅ 抽查 `agent_workspace/*/run_r5.py` 均 `daily-bar-v1` |
| 他人未提交改动存在（方案第 6 节硬边界） | ✅ 已核：`collector_tasks.json`、`sources_config.json`、4 个 `data/snapshots/*.json`（已删） |
| `PRICE_TABLES` 四表定义位置 | ✅ `qfq_reanchor_schema.py:64` 等多处，但方案 B3 表未列全（见 R5） |

---

## 5. 必须修订的 5 项（按优先级）

1. **R1（阻断）**：明确 `_advance_or_defer_watermark` 的去留。二者择一：
   - **(a) 推荐**：保留该函数与 `qfq_watermark_intent` 链路，仅删 A4 变更检测/续传游标/`mode` 分支——改动面最小且不触碰正确性红线；
   - **(b)** 同步退役 `qfq_orchestrator` 水位协调（`enabled=false` + 移除 `defer/hold` 语义）——须作为**独立框架行为变更**另行立项审计，不得并入本方案。
2. **R2（阻断）**：补齐 `advance_watermark`/`get_last_date` 全部调用方（含 `qfq_resident_orchestrator.py:919-936`）；`get_last_date` 语义变更须单列为**语义变更项**并给出多 source 共表场景的差异归因验收。
3. **R3**：补 `daemon_lifecycle.py:655,662`、`daemon_process.py:135`。
4. **R4**：逐一定夺 `mode` 的 3 处非拉取语义依赖（失败判定 / `_authority_reconcile` 触发条件 / config_lint 契约）。
5. **R5**：补 `exporter.py`、`fm_export.py`；修正 `source_capabilities.py` 路径；补 B3 的 `qfq_schema_contracts.py` DDL 与 schema 指纹影响评估；修正测试计数口径（62 文件 / 18 个 `test_*minute*`）。

**另需补充**：
- 第 3 节验收标准缺 **QFQ schema 指纹校验**与 **`qfq_watermark_intent` 断流检测**两项；当前 9 条验收**无法检出 R1 所引入的静默语义变更**。
- 第 6 节硬边界的"他人未提交改动"清单应更新为实测状态（`data/snapshots/*.json` 已为 `D`，非 `M`）。

---

## 6. 审计裁定

**结论：不通过。**

方案"A-β 保守删除增量机制"的核心假设——增量机制可被干净剥离——**在 QFQ 编排器启用状态下不成立**。方案的 `_advance_or_defer_watermark` 删除项与"QFQ gate 才推进水位"这一既有红线直接冲突，且未识别 `qfq_watermark_intent` 断流后果。同时，`get_last_date` 语义变更、QFQ schema 指纹影响、测试计数口径等问题均未得到充分说明。

修订后请提交重审；**R1/R2 未定夺前不建议进入步骤③实施**。

---

## 7. 附录

### 7.1 基线记录

已按项目写前快照纪律落盘：

- `docs/handoff/baseline-20261001-202056.txt`（HEAD `7c3e7fb`）

### 7.2 本次审计未修改任何框架代码

符合"禁止带病实施"要求。

### 7.3 关键代码索引

| 文件 | 关键位置 | 说明 |
|---|---|---|
| `quantstudio/pipeline/daemon.py` | 477-506, 328, 1814, 2074, 1473, 1545, 1865, 2339-2347, 2718-2778 | 水位推进、mode 消费、权威源调和 |
| `quantstudio/pipeline/qfq_orchestrator_types.py` | 425-528 | QFQ 配置校验、协调表集合 |
| `quantstudio/pipeline/qfq_resident_orchestrator.py` | 871-936, 1413-1558 | defer/hold/commit 水位链路 |
| `quantstudio/pipeline/writers.py` | 394-403, 1033-1077 | `get_last_date` / `advance_watermark` 定义 |
| `quantstudio/pipeline/daemon_lifecycle.py` | 655, 662 | 常驻循环调度 |
| `quantstudio/gui/daemon_process.py` | 135 | GUI 单次采集入口 |
| `config/profiles/mcp_only/collector_tasks.json` | 2720-2744 | QFQ 编排器启用配置 |
| `quantstudio/pipeline/qfq_schema_contracts.py` | 63, 626-628, 907-909 | 分钟表 DDL 列与 schema 指纹 |
