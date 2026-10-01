# 双方案审计意见：口径矩阵修正（方案乙）+ QFQ 自检性能优化（2026-09-30）

> 审计对象：
> - `docs/caliber-matrix-correction-design.md`（口径矩阵修正·四表纠正与 A′ 还原集合收缩）
> - `docs/qfq-invariant-perf-design.md`（QFQ 自检因子查询优化·写阶段瓶颈消除）
>
> 审计方式：逐项核对生产代码（mcp_adapter.py / qfq_invariant.py / daemon.py /
> quality_audit.py / tests）与全部引用证据文档，不采信方案自述。

---

## 总结论

| 方案 | 结论 | 理由 |
|---|---|---|
| **qfq-invariant-perf** | ✅ **审计通过**（附 4 项小修订，实施时并入） | 代码引用属实、等价性论证严密、符合性能优化铁律 |
| **caliber-matrix-correction** | ❌ **审计不通过，退回修订**（技术路线合格，文档自相矛盾） | 验收标准 A1/A2 与改动 P1 直接冲突（P5 翻案遗留），禁止带病实施 |

---

## 一、qfq-invariant-perf-design.md：通过

### 1.1 核对确认（全部属实）

| 方案论断 | 核对结果 |
|---|---|
| `_load_factor_lookup`（qfq_invariant.py:276）SQL 无时间范围 | ✅ L288-290 实测确认 |
| 消费点唯一：`factor_lookup.get((code, day))`，day 全部来自抽样行 bar_day | ✅ L206/L188，无第二消费点 |
| 窗口整日取整下 keep='last' 语义不变 | ✅ 同 (code,day) 的全部因子行均落入整日窗 → max(time) 不变 → 逐键等价 |
| 其他调用方零影响 | ✅ check_golden_rows(L543)/verify_reanchor_selfcheck(L702)/refresh_golden_rows_for_code(L633) 默认 None=旧行为 |
| 实测收益 22.45s→0.03s（769.6×） | ✅ 证据文档 `defect1-write-phase-bottleneck-20260930.md` 数据一致 |

### 1.2 铁律符合性

- 单点改动、可选参数、默认旧行为、未决项全部单列不夹带 ✅
- 不触碰取数/复权/生命周期/撮合任何行为面 ✅
- A1（同输入逐项一致）为硬门，回退条件明确 ✅

### 1.3 小修订项（不阻断，实施前并入）

| # | 项 | 说明 |
|---|---|---|
| P-1 | 行号勘误 | `_stamp_and_write` 实际在 daemon.py:3122（方案写 3155，3155 是 probe 计时行） |
| P-2 | 实施顺序细节 | 现代码 `days` 计算在 `_load_factor_lookup` **之后**（L188 vs L185），实施需先算窗口再传参 |
| P-3 | A1 测试须固定 `seed` | 抽样含随机性（`_stratified_sample`），等价性对比须固定 seed 才可复现 |
| P-4 | 打点口径标注 | invariant=1800s 打点取自 **etf_minutes** 批次，外推 stock_minutes 占比属假设，B2/B3 以实测为准（方案已注明，建议正文明示该外推） |

---

## 二、caliber-matrix-correction-design.md：退回修订

### 2.1 技术路线核对（合格部分）

| 项 | 核对结果 |
|---|---|
| P1 改动点 mcp_adapter.py:301 现状引用 | ✅ 属实 |
| 收缩语义（成员还原/非成员直通 ratio=1.0） | ✅ L2072-2085 与方案描述一致 |
| stock_daily 证据（10,351 配对 + adj>1.5 组 6,988 样本实质构成大幅分层） | ✅ 证据充分 |
| etf_minutes 翻案（57,583 样本，>10% 组差 11.5 倍） | ✅ P5 判定力充足，翻案成立 |
| etf_daily 证据（2,789 配对，raw/qfq 误差差 1,870 倍） | ✅ 信号极强，可接受 |
| 与性能方案并行（文件不重叠） | ✅ 属实 |

### 2.2 阻断项（必须修订，修订后复审）

| # | 问题 | 详情 |
|---|---|---|
| C-1 🔴 | **验收 A1/A2 与改动 P1 自相矛盾** | P1：收缩为 `frozenset({"etf_minutes"})`；A1 却写「`_QFQ_CALIBER_TABLES` 为**空集**、任意表 ratio==1.0」；A2 写「四表均报 raw、is_qfq_restored=False」。按代码 L2079-2085，收缩后 **etf_minutes 必须报 `caliber="qfq"`、`is_qfq_restored=True`**（这正是它被还原的证据）。实施者无法同时满足 P1 与 A1/A2——按 A1 验收会把正确实现判为失败。A1/A2 系初版（收缩为空集）遗留，P5 翻案后未同步 |
| C-2 🔴 | **旧文案残留与自身结论冲突** | §1.3「误判三表为 qfq」（实际误判两张，etf_minutes 原判定正确）；§3.2「三张表均被误还原」「+ 重拉三张日线/ETF 表」（etf_minutes 的还原是**正确行为**、不需重拉，与 §1.1/§2 P2 表格矛盾）；§3.1「若四表均不还原……」前提已失效（etf_minutes 仍还原） |

### 2.3 非阻断修订项

| # | 项 | 说明 |
|---|---|---|
| C-3 | P4 清单遗漏 | `caliber-matrix-full-review-20260930.md` 的 etf_minutes=RAW 结论已被 P5 推翻，该文档需加纠错标注（P4 只列了 20260927 原矩阵与 A′ 文档） |
| C-4 | §3.1 门禁「需调整」过度声明 | 门禁代码（quality_audit.py L765-1007）对四表**本地表**期望统一 raw（L982 `decided=="raw"` 即通过），P1 收缩后期望矩阵不变、**门禁逻辑零改动**。实际仅两件小事：① L768-769 docstring 引用的旧矩阵描述同步更新；② `_CALIBER_BLOCK_STOCK_MINUTES`（L580，现 False）应在 P2 重拉验收通过后置 True——建议把该动作明确写入方案而非"重新评估" |
| C-5 | 注释同步遗漏 | mcp_adapter.py L296-299 注释（「云端 close 恒为 qfq 的表」）需随 P1 更新，方案未列 |
| C-6 | A3 测试更新量低估 | test_restore_to_raw_caliber.py 中 stock_daily/etf_daily 的还原用例（L60-78）需**语义反转**（改为不还原、caliber=="raw"），非仅改断言值 |
| C-7 | B2 残留处置 | 建议由「显式声明残留」升级为「残留清单落盘 + 明确处置决策（清除/保留+理由）」 |

### 2.4 对方案一的重要补充：执行顺序协同（两方案文档均未指出）

方案一 P2 重拉（978 万 + 217 万行）走 daemon 写路径 → `_stamp_and_write` →
`_qfq_invariant_after_align` → `check_qfq_invariant`——**同样承受方案二定位的
invariant 全历史拉取瓶颈**（日线批次抽样同样 ~555 code 全历史因子）。

**推荐顺序**：方案二先实施（纯性能、无数据变更、收益已被实测锚定）→
方案一修订复审通过后实施 P1（代码收缩）→ 再执行 P2 重拉（**直接受益于方案二的
769× 加速，重拉耗时同比例下降**）。两方案"文件不重叠可并行"的结论成立，
但 P2 的执行时机应在方案二之后，否则重拉按 60% 冗余成本空转。

---

## 三、审计处置

1. **方案二**：通过。P-1~P-4 小修订单实施时并入，不构成重新送审条件。
2. **方案一**：退回。按 C-1、C-2 修订（同步消除 2.3 各项），**修订后重新送审**；
   修订范围仅限文档一致性，技术路线（P1 收缩 + P2 重拉 + P3 基准改云端 + P5 已完成）
   本审计无异议。
3. 顺序建议采纳与否由用户决定；若采纳，方案二实施可与方案一修订并行推进。
