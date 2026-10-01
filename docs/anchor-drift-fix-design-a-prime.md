# 缺陷1 修复方案 A′：表级复权口径（修订版·取代方案 A 的逐日判定）

> **文档性质**：六步流水线之**「方案」**阶段产物。**尚未实施**。
> **与 v3 的关系**：本文档**修订 v3 方案 A** 的核心设计——由「逐 (code,day) 参考判定」
> 改为「**表级口径**」。v3 的其余部分（补漏检、数据修复、验收框架）沿用。
> **依据**：`docs/evidence/cloud-caliber-matrix-forensics-20260927.md`（口径矩阵，第 1 步补测完成）

- **日期**：2026-09-27
- **层级判定**：**框架层**（数据适配层 `mcp_adapter._restore_to_raw`）
- **关联铁律**：「框架层改动六步流水线」「框架问题立即解决」「策略生成与转换全链路修复仅限框架层」

---

## 1. 问题定义

### 1.1 背景（缺陷1 回顾）

`MCPAdapter._restore_to_raw` 对价格列无条件施加 `raw = qfq × adj_latest/adj_i`。
取证证实：**该公式仅对「云端返回 qfq」的表成立**，对「云端返回 raw」的表会把 raw 放大。

### 1.2 v3 方案 A 的设计与其缺陷

v3（已审计通过并实现）的判定逻辑：**逐 (code,day) 用本地日线 raw 反推**云端是 raw 还是 qfq。
其隐含假设是「**表内口径异质**」——同一张表内有的日期给 raw、有的给 qfq。

### 1.3 口径取证结论（**推翻 v3 假设**）

> ## ✅ 本表结论**已由金标准方法再确认**（2026-09-30）
>
> 2026-09-30 曾有一次复核判定两张日线表为 RAW 并据此收缩还原集合，
> **该结论已被推翻——本表正确**。
>
> 金标准方法（`amount/volume` 反推真实成交价，与复权无关）：
> - `stock_daily`：`err_raw=0.2621` vs `err_qfq=0.0082` → **QFQ**（差 32×）
> - `etf_daily`：`err_raw=0.2509` vs `err_qfq=0.0033` → **QFQ**（差 75×）
> - `stock_minutes`：`close/(amount/volume) = 1.0000` → **RAW** ✓
>
> **错误复核的缺陷**：用云端 `stock_minutes` 作"独立基准"，
> 而其 raw 判定**本身来自本表** → **同型循环论证**。
>
> **完整复盘**：`docs/evidence/caliber-recheck-gold-standard-20260930.md`

| 表 | 云端口径 | 稳定度 |
|---|---|---|
| **etf_daily** | 恒 **qfq** | 286/286 = 100%（金标准再确认） |
| **stock_daily** | 恒 **qfq** | 剔 300803 后 100%（金标准再确认） |
| **etf_minutes** | 恒 **qfq** | 剔 159325/159307 后 4/4 code 100% |
| **stock_minutes** | 恒 **raw** | **107/107 = 100%**（金标准再确认） |

**曾观察到的「表内异质」（2.2%~8.1%）经逐一归因，全部是少数 code
（300803 / 159325 / 159307）的本地参照失配，无一是云端口径真异质。**

### 1.4 v3 方案 A 的两个真实代价（均为「参考依赖」的副产品）

数据修复首轮试跑实测暴露：

1. **read_only 连接冲突**：`_get_local_daily_close` 用 `read_only=True` 读主库，
   与 daemon writer 的 read-write 连接冲突（DuckDB「different configuration」）→
   参考查询全失败 → 无参考 → 整批 fail-fast。
2. **历史缺日死锁**：159325 @ 2026-09-11（本地缺日 + ratio=1.999）→ 无参考 → fail-fast →
   参考永远写不进 → **死锁**（审计 R1 裁定的「不写+下批重试」未覆盖此死锁）。

**根因定性**：v3 的「参考依赖」是为应对一个**不存在的异质**而引入，
其代价（连接冲突 + 死锁）纯属多余。

### 1.5 关键澄清：缺陷1 的实际范围

`_restore_to_raw` 的公式 `× adj_latest/adj_i`：

| 表 | 云端口径 | 公式效果 |
|---|---|---|
| **日线表（stock_daily / etf_daily）** | qfq | **在 `_restore_to_raw` 还原层本来就正确**（qfq × ratio = raw）✓ |
| **分钟表（stock_minutes / etf_minutes）** | raw | **错误**（raw × ratio = 放大）✗ |

> **限定（N4）**：上表「正确 ✓」**仅指 `_restore_to_raw` 还原层**。
> 日线表仍有 **aligner 重锚层**的缺陷2（`stock_daily.close_front` 冻结 54,442 行），
> 与本表结论不矛盾——两者是不同层、不同列（缺陷2 是 front 列，本表说的是还原层对 raw 列的处理）。

→ **「raw 污染」实际只发生于分钟表**（这解释了为何 stock_daily 的 raw 列基本正确）。

---

## 2. 改动范围

### 2.1 允许改动（框架层）

| 文件 | 改动性质 |
|---|---|
| `quantstudio/pipeline/sources/mcp_adapter.py` | `_restore_to_raw` 改为**表级口径**；删除 v3 的参考判定与 `_get_local_daily_close` |
| `quantstudio/pipeline/quality_audit.py` | 保留 v3 的 `MinuteRawVsDaily` 补漏检（不变） |

### 2.2 核心改动（方向）

```python
# 表级口径映射（口径矩阵钉死）
_QFQ_CALIBER_TABLES = frozenset({"stock_daily", "etf_daily", "etf_minutes"})
# 云端返回 qfq → 需还原；未列出的（stock_minutes）云端返回 raw → 不还原

# _restore_to_raw 内：
if str(table) in _QFQ_CALIBER_TABLES:
    ratio = (adj_latest / adj_i).where(valid, 1.0)   # 恒还原（沿用原公式）
else:
    ratio = 1.0                                       # 恒不还原（保持 raw）
```

### 2.3 删除项（v3 遗留）

- `_classify_raw_qfq`（纯判定函数）
- `_get_local_daily_close`（读主库参考，**连接冲突与死锁之源**）
- `_bar_day_of`（交易日提取，仅判定用）
- `_restore_to_raw` 内的 per-(code,day) 判定循环
- meta 里的 `classify_judged_raw` / `classify_judged_qfq` / `classify_no_ref_passthrough`

**保留**：`quality_audit.py` 的 `MinuteRawVsDaily`（独立门禁，不依赖 `_get_local_daily_close`）。

> **N7 确认**：`MinuteRawVsDaily` 的交易日提取用 `quality_audit.py:714-716` 的内联
> `pd.to_datetime(merged["time"], unit="ms").dt.tz_convert("Asia/Shanghai")`，
> **不经过 `_bar_day_of`** → 删除 `_bar_day_of` **无残留依赖**（已核验源码）。

> **连带影响（须同步标注）**：删除 `_bar_day_of` 同时使 v3 验收的
> **C1（trade_date 整型防御）** 与 **C3（`classify_no_ref_passthrough` 监控）** 两项**前提消失**
> ——该两项在 A′ 下不再适用，验收 checklist 应同步移除（避免按过时清单追问）。

### 2.4 禁止改动

- 任何 `quantstudio/backtest/strategies/**` 策略源码
- aligner `_apply_qfq` 的既有契约（`front = raw × adj_i/adj_latest`）
- 作者已合入的 P2-1 / P1-2b（缓存层，与本方案正交）

---

## 3. 与 v3 方案 A 的对比

| 维度 | v3 方案 A（逐日参考判定） | **方案 A′（表级口径）** |
|---|---|---|
| 判据 | 本地日线 raw 参考 | **表名（表级口径矩阵）** |
| 依赖 | 主库读取（连接敏感） | **无外部依赖** |
| read_only 冲突 | 有（已修，改 read_only=False） | **无** |
| 历史缺日死锁 | **有** | **无** |
| 表内异质 | 逐日判定覆盖 | 归因确认**不存在**，无需覆盖 |
| 复杂度 | 高（判定循环 + 3 辅助函数 + 读库） | **低（一个 frozenset 判断）** |
| 性能 | 需时间剪枝（已优化至 0.59s/3000 code） | **零额外开销** |

**结论**：A′ 在正确性等价的前提下，**消除了连接冲突、死锁、性能开销与复杂度**，
符合铁律「最小、可逆、语义等价」。

---

## 4. 影响面

### 4.1 行为变化（严格等价性论证）

| 场景 | v3 行为 | A′ 行为 | 是否等价 |
|---|---|---|---|
| 日线表（云端 qfq） | 判定 qfq → ×ratio | ×ratio | **等价** |
| 分钟表（云端 raw） | 判定 raw → 不还原 | 不还原 | **等价** |
| 参照失配 code（300803 等） | 可能误判（d_qfq=0.45） | **按表级口径处理** | **A′ 更正确** |
| 历史缺日（ratio≠1） | fail-fast（死锁） | 按表级口径正常写入 | **A′ 修复死锁** |

**关键**：A′ 对日线表与分钟表的**主流场景与 v3 逐值等价**；
差异仅在「参照失配 code」与「历史缺日」两个 v3 处理不当的边界。

### 4.2 风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| 口径矩阵样本量（C-4 修订） | 结论基于数千样本 + 剔异常后 100%（证据等级 A/B+）；非全量 | 验收用**§5.2 结果导向验收 + §5.3 口径漂移门禁**（原「全量口径校验」已并入此二者） |
| 参照失配 code | 300803/159325/159307 的根因未定 | 列为**未决项**，另案；A′ 按表级口径处理它们是**确定性行为** |
| 云端口径变更 | 若云端将来改口径，表级硬编码会失效 | 加**口径校验门禁**（见 §5.3） |

---

## 5. 验收标准

### 5.1 单元/集成

- [ ] `_restore_to_raw` 表级口径单测：日线表还原、分钟表不还原、未知名表不还原
- [ ] 既有 `_RESTORE_*` 相关测试套件**全绿**（零衰减）
- [ ] 删除项确认无残留引用（`_classify_raw_qfq` / `_get_local_daily_close` / `_bar_day_of`）
- [ ] `quality_audit.py` 的 `MinuteRawVsDaily` 测试仍全绿

### 5.2 数据契约（等价性）

> **正确性背书声明（N3）**：A′ 的正确性**不以口径矩阵自证**（口径矩阵仅作方向依据——
> 若用本地参照复验云端口径，与 v3 判定同源，构成循环验证）。
> **A′ 的正确性背书 = 结果导向验收**：即下方 `close` 对参考的一致率、
> `close_front` 契约、`AdjustmentAnchorDrift` 归零、以及 13,309 + 1,327 单元格逐项复验。
> 口径矩阵的作用是**解释 A′ 为何更简洁**，不是它正确的证明。

- [ ] 修复后重拉，`stock_minutes.close` 对 `stock_daily.close` 一致率 **≥99.5%**
  （**阈值依据 N6**：容差 0.5% 用于吸收①两源末位舍入差异 ②日线参照自身的少量口径差异；
  13,309 个已确认失真单元格须**逐项 100% 复位**，不适用该容差——两者分开判）
- [ ] 日线表 `close_front` ＝ `close × adj_i/adj_latest` 逐值成立（aligner 契约）
- [ ] `AdjustmentAnchorDrift` 从 1099 → **0**（或逐项解释残差）
- [ ] 受影响单元格（13,309 + etf 1,327）**逐项复验通过**（须 100% 复位）
- [ ] **原有正确数据零回归**：未受影响 code 修复前后**逐值一致**

### 5.3 口径漂移门禁（新增·纯增益，含阻断项 N5）

- [x] `quality_audit.py` 增「云端口径漂移」检查 `_audit_caliber_drift`（CaliberDrift）：
  抽样验证各表云端口径是否仍符合口径矩阵：
  - **`stock_minutes` 口径翻转（raw→qfq）→ 阻断**（N5）：该表口径翻转 = 缺陷1 直接回归
    （A′ 对其不还原，若云端改返 qfq 则会写入未还原值）→ 必须**阻断**而非告警；
  - **日线表 / etf_minutes 口径翻转 → 告警**（不阻断）：影响面为还原值偏差，由 §5.2 一致率兜底。
  - 依据：A′ 是**表级硬编码**方案，口径漂移 = 静默写错；股票分钟翻转后果最重，故阻断。
- [x] 门禁阈值与告警通道在实施时定（未决项 §7）——已定为：
  `_CALIBER_MIN_DIV_GAP=0.02`、`_CALIBER_DECISIVE_MARGIN=0.5`、`_CALIBER_MIN_PROBES=20`、
  `_CALIBER_MAJORITY=0.5`；告警通道 = `QualityIssue(severity=warning/error)`。

> **实施期重大修正（2026-09-28，取证见
> `docs/evidence/caliber-drift-gate-forensics-20260928.md`）**：
> 本门禁探的是**本地表**口径，而 A′ 对 `stock_minutes` **不还原** → 本地表 = 云端值逐行直写，
> 故「云端改返 qfq」与「缺陷1 历史污染未修复」在本地叠加、**不可分离**。
> 实测（156 万配对：51.3% 逐位相等、median 比值 0.9993 + 膨胀长尾）证明当前
> `stock_minutes` 的「非 raw」信号**来自已诊断的污染，而非云端口径翻转**。
> 若按原设计无条件阻断，将**在修复前后均常亮误报**，违背纯增益。
>
> 因此实施为：`_CALIBER_BLOCK_STOCK_MINUTES = False`（默认）→ `stock_minutes` 翻转
> **只告警不阻断**，detail 如实标注「本地不可区分，待数据修复验收后启用阻断」；
> **数据修复验收通过后置 `True`**，恢复 §5.3 阻断语义。该双态由测试双向固化。
>
> 另：`etf_daily`/`etf_minutes` 因 ETF 分红过小（实测最大除权步进 1.925% < 2% 阈值）
> **结构性不可探**，已如实分类为 `CaliberDriftNotProbeable`，口径由 §5.2 一致率兜底。

### 5.4 多策略横验证（依「全链路修复」铁律）

- [ ] 6 策略重转 `api_portability` 全 PASS；既有功能回归全绿

### 5.5 证据落盘

- [ ] 验收结论写入 `docs/evidence/`

---

## 6. 回退条件

满足任一条即**停止并回退**：

1. 修复后任一 `(code,day)` 的 `close` 偏离其表应有口径（日线/H分钟应对 qfq 还原、
   股票分钟应保持 raw）且无法归因；
2. 原有正确数据出现**任何**可观察差异；
3. 6 策略横验证任一 FAIL；
4. 结果导向验收（§5.2）或口径漂移门禁（§5.3）判定**某表口径与矩阵不符**
   （说明矩阵有误，须回到取证）；
5. 数据修复后 `AdjustmentAnchorDrift` 未显著下降。

**回退手段**：`git reset --hard <baseline-stash-hash>` + 数据快照还原。
**回退前置**：实施前先建 `git stash create -u` + `git stash store` 持久化
（现有回退点：`adc3e25f` 合并前、`6bc04544` 实施前）。

---

## 7. 未决项

1. **参照失配 code 根因**（300803 / 159325 / 159307）：本地数据缺失还是云端特殊处理？
   → 另案或并入缺陷2。
2. **口径漂移门禁的抽样量**（C-4 修订：原「全量口径校验抽样量」已并入 §5.3 门禁）：
   ✅ **已定**（2026-09-28 实施）：按除权日探针构造，全量除权 code 覆盖（实测
   `stock_minutes` 525 探针 / `stock_daily` 683 探针），单次批量查询取数。
3. **口径漂移门禁的阈值与告警通道**：✅ **已定**（同上）。
4. **新增未决（2026-09-28 取证发现）**：`stock_minutes` 口径门禁的**阻断开关启用时点**
   ——须待数据修复（13,309 + 1,327 单元格复位）验收通过后方可置
   `_CALIBER_BLOCK_STOCK_MINUTES = True`；启用前该表只告警。
   依据：修复前污染与漂移在本地表不可区分（见 §5.3 修正块）。

---

## 8. 流程状态

| 阶段 | 状态 |
|---|---|
| 取证（口径矩阵） | ✅ 完成（`docs/evidence/cloud-caliber-matrix-forensics-20260927.md`） |
| **方案（本文档 A′）** | ✅ **定稿** |
| 审计 | ⬜ 待送审（与口径取证一并送） |
| 实施 | ⬜ 未开始（须审计通过） |
| 验收 | ⬜（§5 checklist） |
| 用户确认 | ⬜ |
| 双仓库推送 | ⬜ |

> **本文档未获审计通过前，禁止实施。** 现有 v3 实现（工作区未提交）在 A′ 通过前保持原样，
> 便于对照与回退。

> **实施动作说明（审计 §8 要求）**：A′ 通过后的实施 = **「回退 v3 代码 + 实施 A′」**，
> 不是从零开始。工作区当前已有 v3 已实施代码（未提交，`mcp_adapter.py` +188 行、
> `quality_audit.py` +43 行、2 个测试文件）。回退点 `6bc04544`（实施前）仍有效。

> **A′ 回退 v3 的实质代价（审计 §3）**：v3 的方案 A 核心设计（逐日判定）被废弃，
> 但**取证链**（缺陷机理、失真数据、front 契约、补漏检、口径矩阵）**全部沿用**，
> 不构成返工——这是**认知升级**（取证钉死口径矩阵），不是返工浪费。
