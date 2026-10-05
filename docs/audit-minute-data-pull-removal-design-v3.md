# 审计报告：分钟数据支持与「全量/增量拉取」机制删除方案 v3

> **审计对象**：`docs/minute-data-pull-removal-design.md`（v3 修订稿）  
> **前置审计**：`docs/audit-minute-data-pull-removal-design-v2.md`（v2 结论：仍不通过）  
> **审计日期**：2026-10-01  
> **审计结论**：**仍不通过，须再修订后重审**  
> **禁止事项**：在 QFQ 编排器日线化真实改造范围未定夺前，禁止进入步骤③实施。

---

## 1. 审计背景

本次为 v2 审计后的重审。v3 已就 N1–N4 作出实质性修订：明确采纳「方案 B」将 QFQ 编排器改造为日线专用，列明 `qfq_orchestrator_types.py` 校验收敛、`qfq_resident_orchestrator.py` 重锚路径改造、GUI 导出页清理、分钟常量清理等。修订方向正确且清单比 v2 完整。

但在核对 v3 中 QFQ 编排器日线化的**具体实现路径**时，发现该改造涉及的范围远大于 v3 当前描述：QFQ resident reanchor 引擎（`qfq_reanchor_engine.py`）与 fresh 捕获器（`qfq_fresh_capture.py`）在函数签名、校验器、表 schema 等多处**深度硬编码分钟数据**，直接把 `freqs=("1min",)` 改成 `freqs=("daily",)` 会在运行时触发 `ValueError`。v3 未识别这些底层依赖，实施后将导致 QFQ resident 周期崩溃。

---

## 2. 阻断级缺陷：方案 B 的实施范围被严重低估

### P1. `qfq_reanchor_engine.py` 的 `_canon_minute_freq` 拒绝 `"daily"`

**代码事实**：

```python
# quantstudio/pipeline/qfq_reanchor_engine.py:310-315
def _canon_minute_freq(freq: str) -> str:
    """分钟 freq → 存储 canonical 字面量（"1min"/"5min"…）；日线/未知抛 ValueError。"""
    kind, n = _norm_freq(freq)
    if kind != "minute" or n <= 0:
        raise ValueError(f"非分钟 freq: {freq!r}")
    return f"{n}min"
```

`apply_reanchor_for_security`（resident 编排器调用的模型入口）在 `model in ("fresh_staged", "fresh_authoritative_rebase")` 分支中对每个 `freqs` 元素调用 `_canon_minute_freq`：

```python
# quantstudio/pipeline/qfq_reanchor_engine.py:2093-2097
if not freqs:
    raise ValueError(f"model={model!r} 必须提供非空 freqs")
for _f in freqs:
    _canon_minute_freq(_f)   # 非分钟 freq → ValueError（事务外）
```

**风险**：v3 C2 计划把 resident 调用点的 `freqs=("1min",)` 改为 `freqs=("daily",)`，但未修改 `_canon_minute_freq` 校验逻辑。实施到 resident 重锚路径时，`_canon_minute_freq("daily")` 会直接抛出 `ValueError`，导致单证券重锚失败并上抛为整轮失败。

---

### P2. `apply_reanchor_for_security` 的 `fresh_authoritative_rebase` 模型**强制要求非空 `fresh_minutes`**

**代码事实**：

```python
# quantstudio/pipeline/qfq_reanchor_engine.py:2072-2078
if model in ("fresh_staged", "fresh_authoritative_rebase"):
    ...
    if fresh_minutes is None or len(fresh_minutes) == 0:
        raise ValueError(f"model={model!r} 必须提供非空 fresh_minutes")
```

v3 C2 计划「移除 `fresh_minutes` / `allow_partial_minute` 分钟参数，日线参数 `fresh_daily` 保留」。但 resident 编排器当前使用 `model="fresh_authoritative_rebase"`（`qfq_resident_orchestrator.py:607`），该模型**强制要求** `fresh_minutes` 非空。仅移除参数会导致函数在入口处失败。

**风险**：方案 B 若沿用 `fresh_authoritative_rebase` 模型，必须重写该模型 internals 以支持仅日线 fresh；若改用 `ratio` 模型，则行为语义完全不同（ratio 是方法 B/A 黄金抽验，fresh_authoritative_rebase 是 fresh 源逐值写入），不能简单替换。

---

### P3. `qfq_reanchor_engine.py` 的 `_tables_of` 强制返回 minute_table

**代码事实**：

```python
# quantstudio/pipeline/qfq_reanchor_engine.py:318-323
def _tables_of(asset_type: str) -> Tuple[str, str]:
    """asset_type → (daily_table, minute_table)。"""
    tabs = ASSET_TABLE_MAP[asset_type]
    daily = next(t for t in tabs if t.endswith("_daily"))
    minute = next(t for t in tabs if t.endswith("_minutes"))
    return daily, minute
```

`ASSET_TABLE_MAP` 当前包含 `stock_minutes` / `etf_minutes`。`apply_reanchor_for_security` 内部多处调用 `_tables_of` 并按 minute_table 构造 SQL/校验。若 minute_table 被删除，该函数会抛 `StopIteration` 或后续 SQL 会报 `"table does not exist"`。

**风险**：v3 未列出对 `_tables_of` / `ASSET_TABLE_MAP` 的改造，这是方案 B 必须完成的基础重构。

---

### P4. `qfq_fresh_capture.py` 的 `FreshCapture.capture` 始终拉取并返回分钟数据

**代码事实**：

```python
# quantstudio/pipeline/qfq_fresh_capture.py:396-502
def capture(..., daily_range_ms, minute_range_ms, ...):
    ...
    minute_period = self._period_for(f"{asset_type.lower()}_minutes", "1min")
    none_m, front_m = fetcher.fetch_none_front(
        asset_type, xt_code, minute_period, ms, me)
    fresh_minute = _to_fresh_frame(none_m, front_m, asset_type, code, freq="1min")
    ...
    return record, fresh_daily, fresh_minute
```

`FreshCaptureRecord` 也包含 `minute_range_start`、`minute_range_end`、`minute_row_count` 等分钟字段（`qfq_orchestrator_types.py:283-286`），且 `qfq_fresh_capture` 表 DDL 同样包含这些分钟列（`qfq_reanchor_schema.py:415-425`）。

**风险**：v3 C2 仅一句话「移除 `FreshCapture.capture` 的分钟区间构造，仅保留日线」，未涉及：
- 函数签名 `minute_range_ms` 的移除/可空化；
- `FreshCaptureRecord` 分钟字段的可空化或移除；
- `qfq_fresh_capture` 表 schema 变更与指纹重算；
- `fetcher.fetch_none_front` 在日线-only 场景下的调用方式。

---

### P5. `qfq_resident_orchestrator.py` 多处依赖 `ASSET_PRICE_TABLES` 分钟表

**代码事实**：

```python
# quantstudio/pipeline/qfq_resident_orchestrator.py:69-72
ASSET_PRICE_TABLES = {
    "STOCK": ("stock_daily", "stock_minutes"),
    "ETF": ("etf_daily", "etf_minutes"),
}
```

`_security_range`（`:464-474`）直接 `SELECT MIN(time), MAX(time) FROM {minute_t}`；`_bulk_admissible`（`:1088`）也引用 `ASSET_PRICE_TABLES[asset_type][0]`。

**风险**：v3 C2 仅提到 `_security_range`「校验纯日线场景下不构造分钟区间」，但未要求修改 `ASSET_PRICE_TABLES` 本身。若 minute_t 被删除，`_security_range` 的 SQL 直接报错。

---

## 3. 中等级缺陷

### M1. v3 验收标准 10 仍不足

验收标准 10 要求：
> "QFQ resident 周期在 `PRICE_TABLES={stock_daily, etf_daily}` + `freqs=["daily"]` 下能完整完成"

但未要求验证：
- `apply_reanchor_for_security` 接收 `"daily"` 时不触发 `_canon_minute_freq` 报错；
- `fresh_authoritative_rebase` 模型在仅日线 fresh 下不触发 `fresh_minutes` 非空校验；
- `qfq_fresh_capture` 表在去掉分钟列后仍能正常写入/读取。

应补充为分项验收。

---

### M2. 未评估方案 B 与 schema 指纹的叠加风险

v3 在 I7/R-FP 已列 schema 指纹风险（来自分钟 DDL 删除），但未叠加 QFQ 编排器日线化带来的额外 schema 变更：
- `qfq_fresh_capture` 表去掉 `minute_range_*` / `minute_row_count` 列会再次改变指纹；
- `QFQOrchestratorConfig` 结构本身不变，但其语义变更可能导致既有 bootstrap/cutover 记录失效（`require_bootstrap=true` 且历史 bootstrap 含分钟口径）。

---

## 4. v1/v2 缺陷修订情况核对

| 原缺陷 | v3 处理 | 核对结论 |
|---|---|---|
| R1 QFQ 水位耦合 | 保留水位线/qfq_watermark_intent | ✅ 已落实 |
| R2 advance_watermark/get_last_date | 保留语义、补调用方 | ✅ 已落实 |
| R3 GUI/入口遗漏 | 补 daemon_lifecycle/daemon_process | ✅ 已落实 |
| R4 mode 非拉取语义 | 逐一定夺 | ✅ 已落实 |
| R5 数据源/QFQ 清单 | 补 exporter/fm_export/schema 指纹 | ✅ 已补 |
| N1 `1min` 强制校验 | C1 列明改造 | ✅ 设计层已识别 |
| N2 resident 硬编码 1min | C2 列明改造 | ⚠️ 但底层依赖（P1–P5）未识别 |
| N3 导出页 | C4 列明清理 | ✅ 已落实 |
| N4 分钟常量 | C3 列明清理 | ✅ 已落实 |

---

## 5. 影响面更新

v3 的 I9/I10 方向正确，但需追加：

- **I11 QFQ reanchor 引擎深度改造风险（高）**：`qfq_reanchor_engine.py` / `qfq_fresh_capture.py` 以分钟数据为设计前提，改为日线专用需修改函数签名、校验器、表 schema、 resident 调用链，范围不亚于一次小型子系统重构。
- **I12 模型语义选择风险（高）**：`fresh_authoritative_rebase` 强制要求分钟 fresh；若改为仅日线，需重写模型 internals 或替换为其他模型，两种选择都会改变 QFQ 修正行为，须独立验收。

---

## 6. 审计裁定

**结论：v3 仍不通过。**

v3 正确识别了 N1/N2 的表面症状（`1min` 校验与硬编码 freqs），但**未识别底层病因**：QFQ resident reanchor 子系统从 freq 校验、fresh 捕获、表 schema 到 SQL 构造，均以"分钟数据必然存在"为前提。方案 B 的实施范围被严重低估，若按当前清单实施，会在运行时连续触发 `_canon_minute_freq` 报错、`fresh_minutes` 非空校验失败、分钟表不存在等错误。

### 必须再修订的选项（二选一，需用户/设计者裁定）

#### 选项 A：退回「禁用 QFQ 编排器」（推荐，风险最低）

将 `qfq_orchestrator.enabled` 改为 `false`，保留 `source_watermark` 直写路径（v3 A0 已保留）。这样：
- 不需要改造 `qfq_reanchor_engine.py` / `qfq_fresh_capture.py`；
- 不需要修改 `ASSET_PRICE_TABLES` / `_security_range`；
- N1/N2/P1–P5 均不触发；
- 水位线仍正常推进，只是不再经过 QFQ gate。

**风险**：当前生产是否依赖 QFQ gate 的 hold 行为？若依赖，禁用意味着"任何日线采集成功后立即推进水位"，与"gate 通过才推进"不同。但 v3 本身已决定删除分钟数据，QFQ gate 原本就是为空/分钟/日线四表一致性服务的； minute 表删除后，gate 的实际作用对象已减少。此风险可控，但须作为影响面显式披露。

#### 选项 B：完整重构 QFQ 为日线专用（范围大幅扩张）

若坚持方案 B，v3 必须补全以下改造项：

1. `qfq_reanchor_engine.py`：
   - 新增或改造 `_canon_freq` 支持 `"daily"`，或让 resident 路径不再经过分钟 freq 校验；
   - 改造 `_tables_of` / `ASSET_TABLE_MAP` 支持无 minute_table；
   - 重写 `apply_reanchor_for_security` 的 `fresh_authoritative_rebase` 模型 internals，支持仅 `fresh_daily`；或改用新的日线专用模型并给出书面原因；
   - 移除/可空化所有 minute_table SQL。
2. `qfq_fresh_capture.py`：
   - 改造 `FreshCapture.capture` 签名，使 `minute_range_ms` 可空/可选；
   - `FreshCaptureRecord` 分钟字段可空化；
   - `qfq_fresh_capture` 表 DDL 去掉分钟列并更新 schema 指纹。
3. `qfq_resident_orchestrator.py`：
   - 修改 `ASSET_PRICE_TABLES` 不再含 minute 表；
   - `_security_range` 只返回日线区间；
   - resident 调用 `apply_reanchor_for_security` 时传 `freqs=("daily",)` 并移除 `fresh_minutes` / `allow_partial_minute`（前提是引擎已支持）。
4. 新增验收：
   - `apply_reanchor_for_security(..., freqs=("daily",), fresh_minutes=None, model=...)` 不抛错并返回 committed；
   - resident 完整周期在仅日线场景下跑通；
   - `qfq_fresh_capture` 表去掉分钟列后读写正常；
   - schema 指纹二次重基线成功。

**风险**：选项 B 的实施量约为原方案的 2–3 倍，且会触及 QFQ 核心正确性路径，建议作为独立子项目/独立方案另行审计，不宜与「删除分钟数据 + 删除增量机制」合并为单一大方案。

---

## 7. 建议

从风险控制和六步流水线纪律出发，**建议采纳选项 A**：在本次删除范围内将 QFQ 编排器禁用（`enabled=false`），把 QFQ 日线化作为后续独立变更项另行立项。这样当前方案可在 1–2 轮小修后通过审计；否则方案 B 需重写为独立大方案，审计周期显著拉长。

---

## 8. 附录

### 8.1 关键代码索引

| 文件 | 关键位置 | 说明 |
|---|---|---|
| `quantstudio/pipeline/qfq_reanchor_engine.py` | 310–315, 318–323, 2027–2099 | `_canon_minute_freq`、`_tables_of`、`apply_reanchor_for_security` 分钟强制校验 |
| `quantstudio/pipeline/qfq_fresh_capture.py` | 396–502 | `FreshCapture.capture` 始终拉取分钟 |
| `quantstudio/pipeline/qfq_orchestrator_types.py` | 283–286 | `FreshCaptureRecord` 分钟字段 |
| `quantstudio/pipeline/qfq_reanchor_schema.py` | 415–425 | `qfq_fresh_capture` 表分钟列 DDL |
| `quantstudio/pipeline/qfq_resident_orchestrator.py` | 69–72, 464–474, 602–619, 794–796 | `ASSET_PRICE_TABLES`、`_security_range`、resident 重锚调用 |

### 8.2 本次审计未修改任何框架代码

符合项目铁律要求。
