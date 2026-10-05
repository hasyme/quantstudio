# 审计报告：分钟数据支持与「全量/增量拉取」机制删除方案 v2

> **审计对象**：`docs/minute-data-pull-removal-design.md`（v2 修订稿）  
> **前置审计**：`docs/audit-minute-data-pull-removal-design.md`（v1 结论：不通过）  
> **审计日期**：2026-10-01  
> **审计结论**：**仍不通过，须再修订后重审**  
> **禁止事项**：在 N1/N2 未定夺前，禁止进入步骤③实施。

---

## 1. 审计背景

本次为 v1 审计后的重审。v2 已就 R1–R5 作出实质性修订：撤回水位线/QFQ gate 删除、保留 `get_last_date` 语义、补 `daemon_lifecycle.py`/`daemon_process.py`、定夺 `mode` 非拉取语义、补 `exporter.py`/`fm_export.py` 及 schema 指纹风险。这些修订方向正确。

但在核对 v2 新边界时，发现 **2 处新的阻断级问题**：QFQ 编排器内部存在对 `1min` 的硬编码校验与运行时路径，v2 仅修改了配置文件 `freqs`，未修改对应代码，实施后将直接触发 `config_lint`/`QFQConfigError` 或运行时重锚异常。

---

## 2. 阻断级缺陷（必须修订）

### N1. `qfq_orchestrator_types.py` 强制要求 `freqs` 含 `"1min"`，v2 仅改配置不改校验

**问题描述**：v2 在 B7 中提出将 `qfq_orchestrator.freqs` 从 `["1min"]` 清空/调整，但未修改 `qfq_orchestrator_types.py` 的 `validate()` 方法。

**代码事实**：

```python
# quantstudio/pipeline/qfq_orchestrator_types.py:509-514
def validate(self) -> None:
    ...
    if "1min" not in self.freqs:
        raise QFQConfigError(
            f"qfq_orchestrator.freqs 必须含 '1min'（当前正式频率），收到 {self.freqs!r}")
    for f in self.freqs:
        if f not in ("1min", "1m", "daily"):
            raise QFQConfigError(f"qfq_orchestrator.freqs 含非法频率 {f!r}")
```

同时缺省配置也硬编码 `freqs: ["1min"]`：

```python
# quantstudio/pipeline/qfq_orchestrator_types.py:356
DEFAULT_ORCHESTRATOR_CFG: Dict = {
    ...
    "freqs": ["1min"],
    ...
}
```

`config_lint.py:263-275` 直接调用 `QFQOrchestratorConfig.load()` → `validate()`。只要 `collector_tasks.json` 中 `qfq_orchestrator.freqs` 不含 `"1min"`，`config_lint` 即失败。

**风险**：v2 的 B7 配置改动与未修改的代码校验直接冲突，实施第一步就会破坏配置校验。

---

### N2. `qfq_resident_orchestrator.py` 重锚路径硬编码 `freqs=("1min",)` 与分钟表检测

**问题描述**：v2 保留 QFQ 编排器用于日线水位协调，但编排器内部的重锚引擎调用把 `"1min"` 写死在 resident reanchor 路径中。

**代码事实**：

```python
# quantstudio/pipeline/qfq_resident_orchestrator.py:602-619
res = apply_reanchor_for_security(
    conn, asset_type=asset_type, code=code,
    fresh_daily=fresh_daily, calendar=self.calendar,
    freqs=("1min",),                       # ← 硬编码 1min
    ex_dates_ms=ex_dates_ms,
    model="fresh_authoritative_rebase",
    ...
    fresh_minutes=fresh_minute,
    ...
)
```

以及 pending backfill 入队时按表名后缀判断频率：

```python
# quantstudio/pipeline/qfq_resident_orchestrator.py:794-796
for table in tables:
    freq = "1min" if table.endswith("minutes") else "daily"
    rs, re = ranges[1] if freq == "1min" else ranges[0]
```

`apply_reanchor_for_security` 与 `_security_range` 均会按 `freqs` 去拉取/校验分钟数据。分钟表删除后，这些路径要么找不到表，要么构造出空/退化区间，导致重锚失败或整轮 gate hold。

**风险**：QFQ 编排器保留方案（R1 推荐 a）的隐含前提是"编排器可平滑退化为日线专用"，但代码并未支持此退化。实施后将导致 QFQ resident 周期无法完成，日线水位亦无法推进。

---

## 3. 中等级缺陷（须补充）

### N3. GUI 导出页分钟选项未纳入清理范围

`quantstudio/gui/tabs/export_tab.py:126-132` 仍保留 `cb_1min` / `cb_5min` 导出复选框。v2 B4 已补 `exporter.py` 映射表，但未提 `export_tab.py` UI 入口。若 `exporter.py` 删除分钟映射而 UI 仍可提供 1min/5min 选项，用户点击后将报错或导出空文件。

---

### N4. 多处分钟常量/配置残留未列清单

以下项随分钟表删除后成为死代码或无效配置，v2 未列清理计划：

| 位置 | 内容 | 建议 |
|---|---|---|
| `quantstudio/pipeline/daemon.py:62` | `_MINUTE_FREQS = frozenset({"1min", ...})` | 随分钟失败率阈值分支一并移除 |
| `quantstudio/pipeline/daemon.py:3498` | 健康检查示例 `stale_alert_hours_by_freq."1min": 20` | 配置文件清理示例 |
| `quantstudio/pipeline/quality_audit.py:49` | `FREQ_MS = {"1min": 60_000, ...}` | 可保留（仅 `MINUTE_TABLES` 引用），但 `MINUTE_TABLES` 删除后变为死代码 |
| `quantstudio/backtest/providers/frequency_labels.py` | 分钟 freq 标签与错误码 | 若分钟查询 API 全部删除，相关标签/错误码可清理 |

---

## 4. v1 R1–R5 修订情况核对

| 原缺陷 | v2 处理 | 核对结论 |
|---|---|---|
| R1 QFQ 水位耦合 | 撤回水位线/`qfq_watermark_intent` 删除，全部保留 | ✅ 已采纳推荐 (a) |
| R2 `advance_watermark`/`get_last_date` 清单不全 | 补齐调用方，`get_last_date` 不改语义 | ✅ 已落实 |
| R3 GUI/入口遗漏 | 补 `daemon_lifecycle.py`、`daemon_process.py` | ✅ 已落实 |
| R4 mode 非拉取语义 | 逐一定夺 `has_usable_result`、`_authority_reconcile`、per_date/per_stock | ✅ 已落实，但 `has_usable_result` 语义变更须验收 |
| R5 数据源/QFQ 清单遗漏 | 补 `exporter.py`、`fm_export.py`、schema 指纹风险 | ✅ 已补，但新增 N1/N2 说明 QFQ 内部硬编码未清 |

---

## 5. 影响面更新

v2 在 I7 新增 schema 指纹风险，方向正确。但需追加：

- **I9 QFQ 编排器日常化能力风险（高）**：编排器原设计以 `"1min"` 为强制频率，退化为日线专用需要代码改造；未改造即实施会导致 QFQ 周期失败。
- **I10 配置-代码一致性风险（高）**：配置文件删除 `"1min"` 但代码校验仍强制要求，直接触发 `config_lint` 失败。

---

## 6. 验收标准缺口

v2 验收标准 1–11 基本覆盖，但缺少针对 N1/N2 的专项验收：

- 应补充：**`config_lint` 在 `qfq_orchestrator.freqs` 不含 `"1min"` 时仍通过**。
- 应补充：**QFQ resident 周期在 `PRICE_TABLES={stock_daily, etf_daily}` 且 `freqs` 不含 `"1min"` 时仍能完成，日线水位正常 defer→gate→commit**。
- 应补充：**`apply_reanchor_for_security` 不再接收 `freqs=("1min",)`，且对分钟表 trigger 不再入队 `freq='1min'`**。

---

## 7. 审计裁定

**结论：v2 仍不通过。**

v2 正确回应了 v1 的 R1–R5，但引入/遗留了新的阻断级问题 N1/N2：QFQ 编排器并非"配置一改即可日线化"，其校验器、缺省值、resident reanchor 路径均硬编码 `"1min"`。在不修改这些代码的情况下删除配置文件中的 `"1min"` 并移除分钟表，会导致 `config_lint` 失败和 QFQ 周期失败。

### 必须再修订的 4 项

1. **N1（阻断）**：修改 `qfq_orchestrator_types.py`：
   - `validate()` 取消 `"1min"` 强制要求；
   - 允许 `freqs` 仅含 `"daily"` 或为空；
   - `DEFAULT_ORCHESTRATOR_CFG["freqs"]` 同步调整（建议 `[]` 或 `["daily"]`，需与 resident 路径协商一致）。
2. **N2（阻断）**：修改 `qfq_resident_orchestrator.py`：
   - `:605` `apply_reanchor_for_security(..., freqs=("1min",), ...)` 不得硬编码 1min；应基于当前 `PRICE_TABLES`/`cfg.freqs` 推导；
   - `:795` `freq = "1min" if table.endswith("minutes") else "daily"` 删除分钟分支；
   - 校验 `_security_range`、`FreshCapture.capture` 在纯日线场景下不构造分钟区间。
3. **N3（中）**：`gui/tabs/export_tab.py` 移除 1min/5min 导出复选框，与 `exporter.py` 保持一致。
4. **N4（中）**：补充 `_MINUTE_FREQS`、`stale_alert_hours_by_freq.1min`、`FREQ_MS`、分钟 freq 标签等残留清理项。

### 替代方案（建议用户/设计者裁定）

若 N1/N2 改造成本过高，可考虑：

- **方案 A（推荐）**：随本次删除同步将 QFQ 编排器**禁用**（`enabled=false`），把整个 QFQ resident 周期作为"与分钟数据强绑定、当前暂不启用"的组件整体挂起。这样 N1/N2 不触发，水位线退化为纯 `writer.advance_watermark` 模式（v2 A0 已保留该路径）。但需评估：当前生产是否依赖 QFQ 编排器的 gate 行为？若依赖，禁用同样是行为变更，须单独评估。
- **方案 B**：保留并改造 QFQ 编排器为日线专用。须补充 N1/N2 的代码改动、测试与验收。

无论选 A 或 B，都必须在方案中显式说明并补充验收项，不得默认"配置改完即可工作"。

---

## 8. 附录

### 8.1 关键代码索引

| 文件 | 关键位置 | 说明 |
|---|---|---|
| `quantstudio/pipeline/qfq_orchestrator_types.py` | 356, 509–514 | freqs 缺省值与强制校验 |
| `quantstudio/pipeline/qfq_resident_orchestrator.py` | 602–619, 794–796 | 硬编码 1min reanchor 与分钟表频率推导 |
| `quantstudio/pipeline/config_lint.py` | 245–275 | 调用 QFQOrchestratorConfig.validate |
| `quantstudio/gui/tabs/export_tab.py` | 126–132 | 分钟导出 UI |
| `quantstudio/pipeline/daemon.py` | 62, 3456, 3498 | 分钟常量/阈值/健康检查示例 |
| `quantstudio/pipeline/quality_audit.py` | 49 | `FREQ_MS` |

### 8.2 本次审计未修改任何框架代码

符合项目铁律要求。
