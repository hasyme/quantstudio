# 口径矩阵修正 V2.1 实施验收（2026-09-30）

> # ❌ 本验收记录已作废（2026-09-30 当日）
>
> **本验收所覆盖的实施（P1 口径集合收缩）建立在错误取证之上，已全部回退。**
>
> | 项 | 状态 |
> |---|---|
> | P1 代码收缩（`{etf_minutes}`） | ❌ **已回退**为三表 |
> | 测试语义反转 | ❌ **已回退**（13/13 PASS，日线表恢复"应还原"） |
> | P4 文档/注释标注 | ❌ **已回退**（改为「金标准再确认」块） |
> | 数据（P2 部分重拉的 209,640 行） | ✅ **已修复重拉并验证** |
>
> **作废原因**：方案前提「云端两张日线表 = RAW」错误（实为 **QFQ**）。
> 金标准方法（`amount/volume` 反推真实成交价，与复权无关）确证：
> `err_qfq` 比 `err_raw` 小 32×（stock_daily）/ 75×（etf_daily）。
>
> **反思**：本验收的 A1~A4 全部「通过」，但**验收的是错误的实现**——
> 验收标准本身由错误的前提推导而来。**这印证了「验收不能自证前提」**：
> A 类框架层验收须有**独立于方案前提**的哨兵（如生产校验器隔离率）。
>
> **复盘**：`docs/evidence/caliber-recheck-gold-standard-20260930.md`
>
> **以下为原验收内容，保留仅供追溯。**

---

> 方案：`docs/caliber-matrix-correction-design.md`（V2.1，复审通过，免重审）
> 复审意见：`docs/evidence/caliber-matrix-correction-design-v2-reaudit-20260930.md`
> 回退点：`48815f17e48399f542ac6682ef854c0a6e0a7193`

---

## 1. 实施内容

### P1（框架层）：`_QFQ_CALIBER_TABLES` 收缩

**文件**：`quantstudio/pipeline/sources/mcp_adapter.py:305`

```python
# 改前
_QFQ_CALIBER_TABLES = frozenset({"stock_daily", "etf_daily", "etf_minutes"})
# 改后（V2.1）
_QFQ_CALIBER_TABLES = frozenset({"etf_minutes"})
```

**语义**（依 `mcp_adapter.py:2072-2085`）：

| 表 | `ratio` | `caliber` | `is_qfq_restored` |
|---|---|---|---|
| `etf_minutes` | `adj_latest/adj_i` | `"qfq"` | `True` |
| `stock_daily` | `1.0` | `"raw"` | `False` |
| `etf_daily` | `1.0` | `"raw"` | `False` |
| `stock_minutes` | `1.0` | `"raw"` | `False` |

### P4（文档/注释层）

| # | 对象 | 处理 |
|---|---|---|
| ① | `quality_audit.py:772` 门禁 docstring | 更新为新矩阵 + 纠正说明 + 「逻辑零改动」 |
| ② | `cloud-caliber-matrix-forensics-20260927.md:14` | 加 **⚠⚠ 重大纠正** 块；两张日线表行标注作废 |
| ③ | `anchor-drift-fix-design-a-prime.md:28` | §1.3 加纠正块 + 表格标注 |
| ④ | `mcp_adapter.py:296` / `:2068`（C-5） | 两处注释同步更新为「仅 etf_minutes 需还原」 |

---

## 2. 验收结果

### A. 框架层（P1）—— ✅ 通过

| 项 | 结果 |
|---|---|
| **A1** 集合成员与逐表 `ratio`/`caliber` | ✅ `_QFQ_CALIBER_TABLES == {"etf_minutes"}`；四表语义经单测逐项验证 |
| **A2** `meta` 字段语义 | ✅ `etf_minutes` 报 `caliber="qfq"`/`is_qfq_restored=True`；其余三表 `raw`/`False` |
| **A3** 测试**语义反转**（C-6） | ✅ `stock_daily`/`etf_daily` 用例从「应还原」反转为「不应还原」 |
| **A4**（R-3）非成员表 fail-fast 锁定 | ✅ **新增 2 条断言**：缺 `adj_factor` 列、因子值无效均 `raise ValueError` |

### 测试结果

```
tests/test_restore_to_raw_caliber.py   14 passed
+ test_caliber_drift_gate.py
+ test_minute_raw_vs_daily.py
+ test_defect1_repair_completion.py
─────────────────────────────────────
合计 43 passed, 0 failed, 0 errors
```

`py_compile`：三个文件全 OK。

---

## 3. C-6 语义反转明细（关键）

| 用例 | 改前（旧矩阵） | 改后（V2.1） |
|---|---|---|
| `test_caliber_set_membership` | 期望 3 表成员 | 期望 `{"etf_minutes"}` |
| `test_daily_qfq_is_restored` | 断言 `close == 2.286`（放大后） | → `test_stock_daily_raw_not_restored`：断言 `close == 2.182`（原值） |
| `test_etf_daily_qfq_is_restored` | 同上 | → `test_etf_daily_raw_not_restored` |
| `test_caliber_meta_reports_qfq_for_stock_daily` | 断言 `caliber=="qfq"` | → `test_etf_minutes_meta_reports_qfq`：改测 `etf_minutes` |
| **新增** `test_stock_daily_big_ratio_still_raw` | — | 用实测场景（688286：云端 59.787 / 误还原 83.690）锁定 |
| **新增** `test_daily_all_price_cols_untouched` | — | 5 个价格列全保持 |
| **新增 A4** ×2 | — | 非成员表 fail-fast 锁定 |

---

## 4. 影响面

| 面 | 影响 |
|---|---|
| `etf_minutes` 路径 | **零变化**（仍在集合，行为不变） |
| `stock_minutes` 路径 | **零变化**（本就不在集合） |
| **两张日线表** | **行为变更**：不再还原 → 直通云端 raw |
| 口径门禁 | **逻辑零改动**（期望矩阵不变）；仅 docstring 更新 |
| 常驻 daemon / GUI / CLI | 经上述表路径生效 |

---

## 5. 未完成项（依方案 §5.5 顺序）

| 项 | 状态 |
|---|---|
| P1 代码收缩 | ✅ **本次完成** |
| P4 文档/注释 | ✅ **本次完成** |
| **P2 重拉两张日线表**（978 万 + 217 万行） | ⬜ **待执行**（受益性能方案 769× 加速） |
| 验收 B1（重拉后 vs 云端 ≥99.5%） | ⬜ |
| 验收 B2（残留清单落盘 + 处置决策） | ⬜ |
| C3（6 策略横验证） | ⬜ |
| 用户确认 → 双仓库推送 | ⬜ |
| `_CALIBER_BLOCK_STOCK_MINUTES` 置 True | ⬜ **`stock_minutes` 全历史重拉验收后**（**非 P2 后**，R-1） |

---

## 6. 证据索引

| 证据 | 位置 |
|---|---|
| 方案（V2.1） | `docs/caliber-matrix-correction-design.md` |
| 复审意见 | `docs/evidence/caliber-matrix-correction-design-v2-reaudit-20260930.md` |
| 四表复核 | `docs/evidence/caliber-matrix-full-review-20260930.md` |
| etf_minutes 纠正 | `docs/evidence/etf-minutes-caliber-correction-20260930.md` |
| 反转后测试 | `tests/test_restore_to_raw_caliber.py`（14 例） |
| junit | `agent_workspace/_junit_v21.xml` |
