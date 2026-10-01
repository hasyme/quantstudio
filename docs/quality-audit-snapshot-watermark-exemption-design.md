# 修复方案：`quality_audit` WatermarkConsistency 水位口径对齐（快照表误报修复）

- 状态：**待审计**（未实施）
- 日期：2026-10-01
- 性质：框架层正确性修复（审计侧 `time_col` 判定与读取侧 `_get_safe_watermark` 口径不一致导致的误报）
- 前置：`docs/stock-basic-dataset-kind-fix-design.md`（第一层：config 补 `dataset_kind`）
- 铁律流程：方案 → 审计 → 实施 → 验收 → 用户确认 → 双仓库推送

> 修订记录：初稿 §2 为「按 `dataset_kind == "snapshot"` 豁免」，经评审改为「无 schema
> `time_key` → 跳过（与 `_get_safe_watermark` 同口径）」。`dataset_kind` 仅作注释说明，
> 不作为判据。理由见 §2.2。

---

## 1. 问题定义

### 1.1 现象

第一层修复（`mcp_stock_basic` 补 `dataset_kind: "snapshot"`）生效后，单跑 `mcp_stock_basic`
水位成功推进（`watermark→1790784000000`），但 `QualityAudit` 报错、整次运行以非零码退出：

```
23:28:16 ERROR [QualityAudit] 1 类错误，8 类警告；checks=319
23:28:16 ERROR [QualityAudit] stock_basic/WatermarkConsistency: count=1 mcp/daily: watermark=1790784000000, max=1783267200000
23:28:17 ERROR [CLI] quality audit failed        # 退出码 1
```

### 1.2 根因

`quantstudio/pipeline/quality_audit.py::_audit_watermarks`（L524-552）对快照表（无 `time_key`）
**错误地 fallback 到业务日期候选列**，把 `delist_date` 当成「数据业务日期」去比对水位。

- 快照表水位语义 = **快照成功日期**（`_snapshot_watermark(end)` = 窗口结束日午夜），
  与表内 `delist_date`（退市日期）**无可比性**；
- `stock_basic` 表 `delist_date` 有非 NULL 值 → `MAX=1783267200000`（约 2026-07）→
  `watermark(2026-10-01) > max` → 误报 `error`。

更深一层的不对称（本次真正要修的点）：

- **读取侧** `daemon.py::_get_safe_watermark`（L2718-2726）已确立口径——「无 `time_key`
  就不做表内日期校验」：

```2718:2726:quantstudio/pipeline/daemon.py
def _get_safe_watermark(self, source, table, freq):
    stored = self.writer.get_last_date(source, table, freq)
    if not stored:
        return None
    schema = self.aligner.schemas.get(table, {})
    time_col = schema.get("time_key")
    if not time_col:
        return stored   # 无 time_key → 直接返回，不做「表内日期」校验
```

- **审计侧** `_audit_watermarks` 却违背了这个口径，无 `time_key` 时硬从候选列找一个日期列：

```533:540:quantstudio/pipeline/quality_audit.py
schema_time_key = self.schemas.get(table, {}).get("time_key") if self.schemas else None
if schema_time_key and schema_time_key in columns:
    time_col = schema_time_key
else:
    time_col = next((col for col in ("time", "end_date", "ex_date", "change_date", "delist_date")
                     if col in columns), None)
if not time_col:
    continue
```

审计侧这个 fallback 候选列**唯一命中的就是两张快照表的 `delist_date`**（见 §3 实测），
是冗余且有害的。

### 1.3 为什么 etf_basic 未暴露

`etf_basic` 同为快照表，但其表内 `delist_date` 全为 NULL（0 行非空）→ `MAX=NULL` →
`if maximum is not None` 为假 → 检查跳过。故该缺陷此前被 etf_basic 的「数据恰好全 NULL」
掩盖，直到 `stock_basic`（`delist_date` 有真实退市日期）首次获得水位才暴露。

### 1.4 历史证据（daemon.log）

| 时间 | 任务 | QualityAudit | 结论 |
|---|---|---|---|
| 21:39:50 | etf_basic | 0 错误 | 通过 |
| 21:53:12 | etf_daily | 0 错误 | 通过 |
| 22:43~23:11 | 多任务 | 0 错误 | 通过 |
| 23:28:16 | stock_basic（修复后） | **1 错误 WatermarkConsistency** | 失败 |

---

## 2. 改动范围

**仅 1 文件、1 处**：`quantstudio/pipeline/quality_audit.py::_audit_watermarks`，把
`time_col` 判定改为「与 `_get_safe_watermark` 同口径：仅 schema 显式声明 `time_key` 的表
做校验」。

### 2.1 改动内容

```python
# 修复后：无 schema time_key 的表（含快照表）水位语义非「数据业务日期」，
# 本校验不适用——与 daemon._get_safe_watermark 的「无 time_key 就不校验表内日期」口径一致。
schema_time_key = self.schemas.get(table, {}).get("time_key") if self.schemas else None
if not schema_time_key or schema_time_key not in columns:
    continue
time_col = schema_time_key
```

即：删除原 `else` 分支的 fallback 候选列逻辑，仅保留 `schema_time_key` 判定。

> 有意从严说明（审计观察 1）：`schema_time_key not in columns` 比读取侧
> `_get_safe_watermark` 多判一档——读取侧只判 schema 有无 `time_key`、不查实际列；
> 审计侧因随后直接 `SELECT MAX(time_key)`，若该列实际不存在会抛错，故先确认列存在再校验。
> 该判定方向更保守，当前数据下无额外收缩（7 张声明 `time_key` 的表该列均在）。

### 2.2 为什么用「无 time_key → 跳过」而不是「dataset_kind == snapshot → 跳过」

两种判据的**有效影响面完全相同**（见 §3 实测：全项目只有 `etf_basic`/`stock_basic` 两张
快照表「无 time_key 且会被 fallback 命中」），但对齐版更抗腐蚀：

1. 不依赖 config 里 `dataset_kind` 填对——legacy profile 或未来新快照表忘填 `dataset_kind`，
   豁免版会立刻复现同样误报；对齐版不会（它只认 schema 的 `time_key`，而 schema 是契约、不会漏）。
2. 把「审计口径 == 读取口径」变成结构不变式，而不是靠人维护一个标签。
3. 仍只有 1 处改动、同样可逆（恢复原 fallback 判定即回退）。

`dataset_kind` 在本方案中仅作为**注释说明**出现，不作为任何分支判据。

**明确不做的**：
- 不改 `_snapshot_watermark` 语义、不改 `_max_date`、不改 daemon 水位推进主路径；
- 不改 `_audit_watermarks` 对「有 `time_key` 表」的任何现有校验逻辑；
- 不改其他 QualityAudit 检查。

---

## 3. 影响面（实测）

### 3.1 覆盖表集合对比（改动前 vs 改动后）

实测（2026-10-01，`data/quantstudio.db` + `alignment_rules.json`）：

- `schemas` 共 23 张表；`source_watermark` 当前 9 行。
- **「有水位的行、但无 schema 条目」= 空**（孤儿表 0 个）→ 对齐版**零覆盖损失**。

| 类别 | 表 | 改动后是否仍校验 |
|---|---|---|
| 有 `time_key` 且有水位（7 张） | balance_statement / cashflow_statement / etf_daily / index_constituents / index_daily / stock_daily_valuation / stock_namechange | **仍校验（逐位不变）** |
| 无 `time_key` 且有水位（2 张，快照表） | etf_basic / stock_basic | **退出校验（本次修复目标）** |
| 无 schema 条目但有水位 | （无） | 无此类表 |

即：改动前后 `WatermarkConsistency` 覆盖表集合只允许 `etf_basic`/`stock_basic` 退出，
其余 7 张有 `time_key` 的表逐项不变。

### 3.2 其他影响

- 其余 QualityAudit 检查（`TableMissing`/`StageCountConservation`/`RequiredValueNull` 等）不受影响；
- 读取侧 `_get_safe_watermark`、水位推进、续传行为**均不变**。

---

## 4. 验收标准

1. **stock_basic 行为验收**：单跑 `--task mcp_stock_basic`，`QualityAudit` 0 类错误，
   `[CLI] once 完成`，退出码 0。
2. **水位仍推进**：`source_watermark` 中 `mcp/stock_basic/daily` 存在，`last_date` 非 None
   （与第一层验收一致）。
3. **etf_basic 无回退**：单跑 `--task mcp_etf_basic`，QualityAudit 仍 0 类错误。
4. **覆盖表集合对比（核心）**：改动前后 `WatermarkConsistency` 覆盖表集合，只允许
   `etf_basic`/`stock_basic` 退出，其余 7 张有 `time_key` 的表逐项不变（以 §3.1 实测为基线）。
5. **非快照表检查仍生效（回归）**：构造/利用一个映射表「水位 > 数据最大日期」场景，
   `WatermarkConsistency` 仍报 error（证明对齐未扩散到有 `time_key` 的表）。
6. **测试套件**：`tests/` 中 quality_audit / watermark 相关测试全绿。

---

## 5. 回退条件

- 触发：对齐后出现「某张无 `time_key` 表的数据陈旧但不再被审计发现」的漏报。
- 回退：恢复 `_audit_watermarks` 原 fallback 判定即回退。
- 回退后重新审计，禁止静默回退。

---

## 6. 与第一层、层次 2 的关系

### 6.1 与第一层（config 补 dataset_kind）

两层共同构成「快照表水位一致性」闭环：
- 第一层：`mcp_stock_basic` 补 `dataset_kind: "snapshot"`，使水位走 `_snapshot_watermark`；
- 第二层（本方案）：审计口径与读取侧对齐，消除误报。

两层须**同步**验收与推送：
- 只合第一层会引入 `quality audit failed` 非零退出（已复现，23:28:16）；
- 只合第二层（不合第一层）会让 `stock_basic` 水位冻结在旧值、同日跳过失效、恢复每轮全量重拉
  （约 5000 行级，可容忍但浪费）——两层同步推送纪律正确（审计观察 3 反向说明落档）。

### 6.2 层次 2（快照表水位语义重设计，方向 B1，单独立项）

快照表不再写 `source_watermark.last_date`，其「上次拉取时间」改由 `batch_audit.finished_at`
承载。立项文档须写实以下两条：

1. **消费方穷举**：水位推进、`_get_safe_watermark` 续传、审计、GUI 展示、「同一天跳过」。
   「同一天跳过」丢掉后的替代保护，须以**实测数字**写进方案（快照表全量拉取成本实测：
   `stock_basic` 5243 行 / 约 1~2 秒，upsert 幂等），不能只作口头理由。
2. **层次 1 自然失效**：B1 落地后 `source_watermark` 不再有快照表的行，本方案（层次 1）
   那处「无 `time_key` → 跳过」将变成防御性死代码——这是好事，但立项文档须写明
   「层次 1 修复在 B1 落地后自然失效，届时可一并清理」，避免两份文档对不上。

---

## 7. 同步清单（双仓库推送时）

- 框架代码：`quantstudio/pipeline/quality_audit.py`（本方案）+ 第一层的
  `config/profiles/mcp_only/collector_tasks.json`；
- 文档：`docs/stock-basic-dataset-kind-fix-design.md`、本方案、
  `docs/stock-basic-dataset-kind-fix-audit.md`（如涉及）；
- `README.md` / `docs/` 中涉及快照表水位语义的表述同步核对。
