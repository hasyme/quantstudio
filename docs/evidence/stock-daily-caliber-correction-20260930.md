# 口径矩阵复核：`stock_daily` 云端口径判定纠正（2026-09-30）

> **结论：云端 `stock_daily` = RAW（非 qfq）。原口径矩阵「恒 qfq」判定错误。**
> 该错误导致 A′ 对 `stock_daily` 无条件施加 `× adj_latest/adj_i` → 放大 6.4% 行（最高 1.94 倍）。

---

## 1. 判定方法（修正原取证的方法论缺陷）

### 1.1 原取证的缺陷

`cloud-caliber-matrix-forensics-20260927.md` §5.2 判定 `stock_daily` 口径的方法：
> 「`close × ratio` 对**日线 raw** 的偏差 vs `close` 直接对**日线 raw** 的偏差」

**问题：用 `stock_daily` 自己当参照，来判断 `stock_daily` 的口径** —— 循环论证。

**同一缺陷已在 `stock_minutes` 判定中被发现并修正**（§5.4）：
> 「此前记录的『0.5% qfq』实为**日线参照本身差异**导致的判定失败」

但 **`stock_daily` 自身的判定未同步修正**。

### 1.2 本次方法（独立基准）

**基准 = 云端 `stock_minutes` 15:00 bar**，理由：
- `stock_minutes` 云端 = **raw**（已确证 107/107，且 A′ 对其不还原）；
- 独立于 `stock_daily`，无循环论证。

**判定**：比较 `cloud_daily / cloud_minutes_raw` 更接近哪个假设。

---

## 2. 判定结果（决定性）

样本：2026-01-12，10,351 配对。

| 假设 | 预期比值 | 中位误差 |
|---|---|---|
| **RAW**（daily = raw） | 1.0 | **0.0081** ✅ |
| QFQ（daily = qfq@latest） | `adj_i/adj_latest` | 0.9963 |

**→ 显著更接近 RAW 假设（误差小 123 倍）。**

### 2.1 按 adj_factor 分组（排除"因子接近 1 导致不可分辨"）

| adj_factor 区间 | 配对数 | ratio 中位数 |
|---|---|---|
| adj == 1.0 | 208 | **1.0000** |
| 1.0 < adj ≤ 1.05 | 1,486 | **1.0000** |
| 1.05 < adj ≤ 1.5 | 1,669 | **1.0000** |
| **adj > 1.5** | **6,988** | **0.9949** |

**关键**：即使 `adj_factor > 1.5`（6,988 个样本，因子差异显著），
比值中位数仍是 **0.9949 ≈ 1** —— 若云端是 qfq，此组比值应显著偏离 1。

**→ 排除"因子太小不可分辨"的解释，判定成立。**

### 2.2 因子分布（确认样本有区分力）

`adj_factor`: min=1.0000, max=**10055.64**, median=**2.4615**

中位因子 2.46（非接近 1）→ 若为 qfq，比值应明显偏离 1。实测未偏离 → **RAW**。

---

## 3. 与原矩阵的冲突对照

| 项 | 原矩阵（2026-09-27） | 本次复核（2026-09-30） |
|---|---|---|
| `stock_daily` 口径 | **恒 qfq**（剔 300803 后 100%） | **RAW** |
| 判定参照 | `stock_daily` 自身（循环论证） | 云端 `stock_minutes`（独立） |
| 原始数据中的异常 | "2.2% raw + 8.1% OTHER 全归 300803" | 实测 6.4% 本地偏高，**非单一 code** |
| 结论 | 无条件还原 | **不应还原**（还原即放大） |

---

## 4. 影响链

```
口径矩阵误判 stock_daily = qfq
  → A′ 将 stock_daily 列入 _QFQ_CALIBER_TABLES
  → _restore_to_raw 对其无条件施加 × adj_latest/adj_i
  → 云端 raw × ratio = 放大（实测 6.4% 行偏高，最高 1.94 倍）
  → 本地 stock_daily 成为错误的"基准表"
  → A3 判据（本地 minutes vs 本地 daily）失效
  → 误判"缺陷1 修复无效"（实为有效，见 a3-baseline-decision 文档）
```

---

## 5. 待确认（下一步取证）

1. **`etf_daily` / `etf_minutes` 是否同样误判**？（同属 `_QFQ_CALIBER_TABLES`）
2. **`stock_minutes` 的 raw 判定是否稳固**？（本次作为基准使用，须独立复核）
3. **本地放大 6.4% 与"重拉后仍偏高"的关系**：
   若 A′ 修正后不再还原，下次重拉是否自然修复？

---

## 6. 修复方向（初步，待方案阶段细化）

| 方案 | 内容 | 影响 |
|---|---|---|
| **A** | 从 `_QFQ_CALIBER_TABLES` 移除 `stock_daily` | 该表不再还原（= 直通云端 raw）；需同步修正数据 |
| **B** | 重新取证全部四表口径，整体修正矩阵 | 最彻底；可能影响 `etf_*` |
| 数据修复 | 重拉 `stock_daily`（覆盖放大值） | 与框架修复配套 |

---

## 7. 证据索引

| 证据 | 位置 |
|---|---|
| 判定脚本 | `agent_workspace/_sdaily_verdict.py`、`_sdaily_caliber.py` |
| 判定结果 | 本文 §2 |
| 原矩阵 | `docs/evidence/cloud-caliber-matrix-forensics-20260927.md` §5.2/§5.4 |
| 放大取证 | `docs/evidence/stock-daily-inflation-forensics-20260930.md` |
| A′ 表集合 | `mcp_adapter.py:301`（`_QFQ_CALIBER_TABLES`） |
