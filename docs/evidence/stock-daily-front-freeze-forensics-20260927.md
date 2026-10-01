# stock_daily front 冻结根因取证报告（P1-4 扩充·只读）

- **日期**：2026-09-27
- **性质**：**只读取证**，未改任何代码/数据
- **触发**：P1-4 全量验证发现 `stock_daily.close_front` 有 430 只 code、54,442 行失真
  （front 未随因子演进重锚，冻结在旧基准）—— 本文档是缺陷2 单独立项的证据基础
- **复现脚本**：`agent_workspace/_p14_daily_verify.py`（含 `_p14_daily_out.txt`）

---

## 一、结论

> `stock_daily.close_front` 冻结 = **「QFQ 重锚子系统对 MCP 日线的覆盖缺口」**，
> 与缺陷1（`_restore_to_raw` 的 minutes raw 污染）**时间线不同、子系统不同，但因果耦合**。

**核心因果链**：

```
缺陷1：_restore_to_raw 把 raw 当 qfq 误乘 → minutes raw 污染
   │
   ▼（因果耦合，非独立）
重锚引擎检测到 raw 不一致 → daily_raw_mismatch / minute_raw_mismatch → 阻断/回滚
   │
   ▼
front 无法重锚 → 冻结持续
```

**但 2025-03 起的存量冻结独立于此**（重锚子系统当时尚未上线）。

---

## 二、四个取证问题的答案（对应用户列的 4 问）

### Q1：430 只冻结是「bootstrap 前存量」还是「每次除权都漏」？

**主要是存量**。重锚子系统上线时间线：

| 事件 | 时间 |
|---|---|
| 300803 最早一次除权（因子 1.003→1.4544，比值随之变化） | 2025-03-16 |
| 重锚子系统 xtquant 事件最早 | 2026-07-31 |
| 重锚子系统 MCP 事件最早 | 2026-08-09 |

> ⚠️ **P2-2 更正（审计第三轮）**：300803 的 2025-03-17「front 冻结」实为**比值变化日**——
> 该日 front 随除权正常重锚（比值 0.689→0.999 是写入时按当时因子重锚的**正常表现**），
> 并非冻结起点。**该 code front 真正过期是在 2026-09-21 除权（1.4555→2.1106）后重锚未生效**
> （比值 1.000 未回落到应有的 0.689）。定性结论（「存量 front 未随因子演进重锚」）不受影响，
> 但「冻结起点早于重锚上线 17 个月」的表述**撤回**——存量冻结的主因是重锚子系统 2026-08 才上线、
> 上线前除权均无重锚机制，而非某只 code 自 2025-03 持续冻结。

### Q2：冻结 front = raw？（确认「未重锚」而非「重锚到错误基准」）

**是「未重锚」**。300803 的 `close_front == close`（比值从除权前的 0.689 跳到 ≈1.0），
即 front 停留在「除权前的旧基准」——当时 `adj_i == adj_latest_OLD`，故 `front = raw × 1 = raw`。
除权后 `adj_latest` 演进，历史 front 应缩到 `×0.689`，但**从未被更新**。

### Q3：重锚子系统对 MCP 写出的 stock_daily 有无触发路径？

**有路径、但失效**。证据（`qfq_reanchor_event` 表）：

| 事实 | 数据 |
|---|---|
| MCP 事件总数 | 5,598 条 |
| MCP committed | 4,723 条 |
| **MCP committed 但 `rows_stock_daily` 全为 NULL（四张行情表 rows 全 NULL）** | **4,723 / 4,723** |
| MCP blocked（`daily_raw_mismatch` 248 + `daily_coverage_mismatch` 31 + `minute_raw_mismatch` 11） | 290 条（282 集中在 2026-08） |
| MCP rolled_back（postcheck「分钟 front 不匹配」） | 585 条 |

> ⚠️ **P2-1 更正（审计第三轮）**：`rows_stock_daily` 实为 **NULL**（非 0），且四张行情表 rows 全 NULL——
> committed 事件**未写入任何价格行**（比「=0」更强：`=0` 可能代表「写了 0 行」，NULL 代表「未记录写入量」）。

→ MCP 日线**在重锚发现范围内**（有 5,598 条事件），但：
1. **committed 的事件从未真正改写任何行情表价格**（`rows_*` 全 NULL）；
2. 大量 blocked/rolled_back 是缺陷1（raw 污染）的直接次生——重锚引擎 fail-safe 地
   拒绝在「脏数据」上重锚。

### Q4：minutes 重拉后，aligner 用当前 adj_latest 重算 front 是否正确？

**是**（间接证实）。rolled_back 的 585 条 error 均为「postcheck 发现主库分钟 front 与
期望不符」——反推：**重锚引擎算出的 front 是正确值，是主库里已有的 front 被污染了**。
故 minutes 重拉后，aligner 用当前因子重算 front，应得到正确值 → 「两列同修靠重拉一次」成立。

---

## 三、两个缺陷的关系（关键修订）

| 维度 | 缺陷1：minutes raw 污染 | 缺陷2：daily front 冻结 |
|---|---|---|
| 落点 | `_restore_to_raw`（adapter 还原层） | `qfq_reanchor_*`（重锚编排层） |
| 污染列 | `close`（raw 被 ×ratio 放大） | `close_front`（未随因子演进重锚） |
| 时间线 | 2026-08 起（P1-③ 之后） | **2025-03 起**（重锚上线前存量）+ 2026-08 后因 raw 污染被阻断 |
| 触发源 | MCP 分钟路径 | MCP 日线路径 × 重锚编排（发现但未生效） |
| 关系 | **上游** | **下游**（部分被缺陷1 持续化）+ 独立存量部分 |

**结论**：二者**子系统/时间线不同，但非完全独立**——
缺陷1 的 raw 污染会通过 `daily_raw_mismatch`/`minute_raw_mismatch` 阻断重锚引擎，
使缺陷2 的修复（重锚）无法落地。**因此拆分修复时，缺陷1 必须先行**。

---

## 四、对方案的影响（修订）

1. **方案 A（缺陷1）核心前提仍成立**：判定参考是 `stock_daily.close`（raw 列），
   P1-4 污染的是 `close_front`。但「stock_daily 100% 自洽」降级为「raw 列需取证确认干净」。
2. **缺陷2 单独立项**（禁止挂账）：需走独立六步流水线。根因方向已锁定：
   - MCP committed 事件 `rows_*` 全 NULL → 重锚引擎对 MCP 行情表「只记事件、不写价格」；
   - 需查 `qfq_reanchor_engine.apply_reanchor_for_security` 对 daily 表的写入路径。
3. **数据修复合并规划**：重拉一次同时修两列（close 靠还原修正、close_front 靠 aligner 重算）。
4. **缺陷1 先行**：raw 污染修好后，重锚引擎不再被 `raw_mismatch` 阻断，缺陷2 的重锚才能生效。

---

## 五、未决项（缺陷2 实施前需继续取证）

1. **committed 全 NULL 代码路径**（审计三问③a）：为何 committed 事件不改写任何行情表价格？
   （`apply_reanchor_for_security` 对 daily 表是「仅记审计不写价格」还是「写了但被回滚」？）
2. **430 只冻结 code 的精确边界**：哪些是「上线前存量」（需 bootstrap 重锚）、哪些是「上线后因 raw 污染被阻断」（缺陷1 修好后自动恢复）？
3. **bootstrap 覆盖判定**（审计三问③b）：bootstrap 机制（`require_bootstrap=true`）是否已对存量 stale 证券重锚？还是因 fail-closed 未跑完？
4. **全历史边界扫描（审计第三轮 P1-2）**：54,442/430 是时间剪枝窗口（2026-03-02~09-23）内的**下界**，
   非全量；2025-03~2026-03-01 的行同样可能冻结但未扫描。**缺陷2 数据修复范围不得锁定为 54,442，
   须含全历史扫描**（不设窗口下限，全市场比对 `stock_daily.close_front` 对 `close × adj_i/adj_latest`）。
