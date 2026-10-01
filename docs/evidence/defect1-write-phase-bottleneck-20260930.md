# 方案乙取证：写阶段耗时构成（2026-09-30）

> 目的：定位写阶段 1.32 分钟/片的瓶颈，为性能优化方案提供依据
> **结论：瓶颈 = QFQ 不变量自检的因子查询（全历史无时间窗），可优化 769 倍**

---

## 1. 写阶段耗时构成（用 daemon 既有 A1a 打点，无需插桩）

daemon 自 2026-09-14 起已有分段打点（`daemon.py:1269/1322/1326/1334/1372`），
每批输出一行汇总。从日志提取的实测（`etf_minutes`，规模最接近 `stock_minutes`）：

```
total=2982.8s | other=541.5s align=74.9s validate=18.1s write=2348.3s | write_share=78.7%
              write_split: copy=0.2s  invariant=1800.0s  sqlwrite=547.9s
```

| 段 | 耗时 | 占比 |
|---|---|---|
| **`write` 段** | 2348.3s | **78.7%** |
| ├─ **`invariant`（QFQ 不变量自检）** | **1800.0s** | **60.3%（总）** |
| ├─ `sqlwrite`（真正 SQL 写入） | 547.9s | 18.4% |
| └─ `copy` | 0.2s | 0.0% |
| `other`（取数/迭代/门禁） | 541.5s | 18.2% |
| `align` | 74.9s | 2.5% |
| `validate` | 18.1s | 0.6% |

**另一条独立样本**（`etf_minutes` 191604）：
```
total=3460.4s | write=2956.2s (85.4%) | write_split: invariant=2349.8s sqlwrite=606.0s
```

**两条互证：`invariant` 占总耗时 60–68%，是绝对瓶颈。**

---

## 2. 瓶颈定位（代码级）

调用链：
```
daemon._stamp_and_write (L3155)
  └─ self._qfq_invariant_after_align(df, table, batch_id, source, ...)   # daemon.py:2886
       └─ check_qfq_invariant(df, table, latest, aux_path=..., ...)      # qfq_invariant.py:106
            ├─ _stratified_sample(df)                    # ≤20 行/code、≤5000 总行（轻）
            ├─ open_ro_sqlite(aux_path)                  # 每片新开 SQLite 连接（9.8GB 库）
            └─ _load_factor_lookup(aux_conn, adj_table, codes)   # qfq_invariant.py:276
                 └─ SELECT code, time, adj_factor FROM adj_factor
                    WHERE code IN (...)                  # ← 无时间范围！
```

### 2.1 根因：因子查询无时间窗，拉取全量历史

```sql
-- 现状（qfq_invariant.py:290）
SELECT code, time, adj_factor FROM adj_factor WHERE code IN (?, ?, ...)
```

抽样规模（`_stratified_sample`）：**≤5000 行、约 555 个 code**
→ 每个 code 拉取 **2018 年至今的全量因子历史**。

**代码注释中已记录此问题**（`qfq_invariant.py:296`）：

> 「原实现对每行因子单独 `pd.to_datetime`（≈1ms/行），
> `stock_minutes` 时间切片批次抽样 ~555 code × **全历史 ~150 万行** → **~24 分钟/片**」

（注：该注释描述的 24 分钟/片是更早版本；当前已有向量化优化，
实测降到 1.32 分钟/片，但**全历史拉取的结构性问题仍在**。）

---

## 3. 优化空间实测（决定性数据）

模拟 `_load_factor_lookup` 的查询（555 code）：

| 查询方式 | 返回行数 | 耗时 | 加速比 |
|---|---|---|---|
| **现状：全历史** | **2,870,262** | **22.45 s** | — |
| **加时间窗（该片 2 天）** | 14,024 | **0.03 s** | **769.6×** |

**只需给查询加上「该片的时间范围」条件，即可获得 769 倍加速。**

### 3.1 语义等价性分析

| 项 | 现状 | 优化后 | 等价？ |
|---|---|---|---|
| 查询目标 | `(code, bar_day) → adj_factor` | 同 | ✅ |
| 实际消费 | 仅抽样行所在的 `bar_day` | 同 | ✅ |
| 未消费数据 | 全历史其他日期的因子行 | 不查 | ✅ **本就未被使用** |
| 索引 | `PRIMARY KEY (code, time)` | 同（时间窗可利用索引） | ✅ |

**关键**：`_load_factor_lookup` 返回的 dict 只被 `factor_lookup.get((code, day))` 消费，
其中 `day` 全部来自**抽样行的 bar_day**（即该片覆盖的日期）。
全历史中其他日期的因子**查了但从不使用** → **加时间窗是纯增益，语义完全等价**。

### 3.2 按铁律分类

此优化 = **「语义完全等价的内部实现优化」**（性能优化铁律允许范围）：
- 不改 API 签名/返回类型/返回字段；
- 不改数据语义、复权口径、字段映射；
- 不改自检判据、阈值、告警行为；
- 不改写入结果、水位、门禁；
- 仅减少**未被消费**的数据拉取。

---

## 4. 预期收益

| 项 | 现状 | 优化后（估算） |
|---|---|---|
| `invariant` 段（每片） | ~60% 总耗时 | ~0.1%（769× 加速） |
| 写阶段吞吐 | 1.32 分钟/片 | **~0.5 分钟/片**（去 60% 后再计） |
| 全量 4,529 片 | ~99.6 小时 | **~35 小时** |

> 更精确的收益需实施后实测；上表为按 `invariant` 占比的线性推算。

---

## 5. 附带发现（不在本方案范围）

| 发现 | 处置 |
|---|---|
| `open_ro_sqlite` 每片新开连接（9.8GB 库） | 可考虑连接复用，但属独立优化项 |
| 每片重复注入 adj_factor 5,190 行 | 日志占主导但耗时可忽略（实测写入快）；**独立评估，不夹带** |
| `other` 段占 18% | 需进一步细分（取数/迭代/门禁），**本轮不动** |

---

## 6. 下一步（依六步流水线）

1. ✅ **取证**（本文档）
2. ⬜ **出优化方案**（含：改动点、等价性论证、验收标准、回退条件）
3. ⬜ 审计
4. ⬜ 实施
5. ⬜ 验收（须证明行为等价：同一输入下自检结果 `sampled/bad/skipped/unknown` 逐项一致）
6. ⬜ 用户确认 → 推送

---

## 7. 证据索引

| 证据 | 位置 |
|---|---|
| A1a 分段打点（既有） | `daemon.py:1269/1322/1326/1334/1372` |
| 分段汇总实测行 | `data/logs/daemon.log*`（11 条） |
| 瓶颈代码 | `qfq_invariant.py:276`（`_load_factor_lookup`）、`:106`（`check_qfq_invariant`） |
| 调用链 | `daemon.py:2886`（`_qfq_invariant_after_align`）、`:3155`（`_stamp_and_write`） |
| 查询成本实测 | §3（本机 sqlite3，555 code） |
| 原始注释记录 | `qfq_invariant.py:296`（"~555 code × 全历史 ~150 万行 → ~24 分钟/片"） |
