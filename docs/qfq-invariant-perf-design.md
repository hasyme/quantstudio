# QFQ 自检因子查询优化方案（性能）· 写阶段瓶颈消除

> 状态：**审计通过 · 已实施 · 已验收**（六步流水线第 1 步产物；验收见 `docs/evidence/qfq-invariant-perf-acceptance-20260930.md`）
> 关联取证：`docs/evidence/defect1-write-phase-bottleneck-20260930.md`
> 分类依据：**「性能优化不得改变引擎行为与逻辑」铁律**——本方案属
> **语义完全等价的内部实现优化**（详见 §4 等价性论证）
>
> **本方案不改口径、不改判据、不改任何可观察结果**，仅消除**未被消费**的数据拉取。

---

## 1. 问题定义

### 1.1 现象（实测）

写阶段是缺陷1 数据修复的**主成本**（fetch 的 24 倍）：

| 阶段 | 实测 | 全量 136 批 |
|---|---|---|
| fetch | 1.8 分钟/批 | 4.1 小时 |
| **写阶段** | **1.32 分钟/片** | **99.6 小时** |
| 合计 | — | ≈104 小时 |

### 1.2 耗时构成（daemon 既有 A1a 打点，无需插桩）

```
total=2982.8s | other=541.5s align=74.9s validate=18.1s write=2348.3s | write_share=78.7%
              write_split: copy=0.2s  invariant=1800.0s  sqlwrite=547.9s
```

| 段 | 耗时 | 占比 |
|---|---|---|
| **`invariant`（QFQ 不变量自检）** | **1800.0s** | **60.3%（总）** |
| `sqlwrite`（真正写入） | 547.9s | 18.4% |
| `other` | 541.5s | 18.2% |
| `align` | 74.9s | 2.5% |
| `validate` | 18.1s | 0.6% |

**另一独立样本互证**：`invariant=2349.8s / total=3460.4s = 67.9%`。

**→ 瓶颈是 `invariant`（QFQ 写前自检），占 60–68%。**

### 1.3 根因（代码级）

```
daemon._stamp_and_write (daemon.py:3155)
  └─ _qfq_invariant_after_align (daemon.py:2886)
       └─ check_qfq_invariant (qfq_invariant.py:106)
            ├─ _stratified_sample(df)                 # ≤20 行/code、≤5000 总行（轻）
            ├─ open_ro_sqlite(aux_path)               # 每片新开 SQLite 连接（9.8GB 库）
            └─ _load_factor_lookup(...)               # qfq_invariant.py:276
                 └─ SELECT code, time, adj_factor FROM adj_factor
                    WHERE code IN (...)               # ← **无时间范围**
```

**每片抽样 ~555 个 code，每个拉取 2018 年至今的完整因子历史（约 287 万行）。**

代码注释已记录此问题（`qfq_invariant.py:296`）：
> 「`stock_minutes` 时间切片批次抽样 ~555 code × **全历史 ~150 万行** → **~24 分钟/片**」

---

## 2. 优化方案

### 2.1 改动点（单点）

**文件**：`quantstudio/pipeline/qfq_invariant.py`
**函数**：`_load_factor_lookup`（L276）

**现状**：
```python
rows = aux_conn.execute(
    f"SELECT code, time, adj_factor FROM {adj_table} "
    f"WHERE code IN ({placeholders})", list(codes)).fetchall()
```

**改为**：增加时间范围下推（该片实际需要的 `bar_day` 区间）：
```python
rows = aux_conn.execute(
    f"SELECT code, time, adj_factor FROM {adj_table} "
    f"WHERE code IN ({placeholders}) AND time >= ? AND time < ?",
    list(codes) + [lo_ms, hi_ms]).fetchall()
```

其中 `lo_ms` / `hi_ms` 由调用方从**抽样行的 time 范围**推导
（`check_qfq_invariant` 已计算 `days = _bar_day_from_ms(sampled["time"])`，
可同源得到区间）。

### 2.2 参数传递

`_load_factor_lookup(aux_conn, adj_table, codes)` 增加两个可选参数：
```python
def _load_factor_lookup(aux_conn, adj_table, codes,
                        time_lo_ms=None, time_hi_ms=None) -> Dict[...]:
```
- 二者为 `None` → **不施加时间条件**（**回退旧行为**，保证既有调用方零变化）；
- 由 `check_qfq_invariant` 传入抽样行的实际区间。

**边界处理**：为覆盖 `bar_day` 归一（CST 日桶）带来的边界，
时间窗按 `[min_day_start, max_day_end + 1day)` 取整日，避免漏掉边界 bar。

---

## 3. 收益（实测）

模拟 `_load_factor_lookup` 的查询（555 code，2026-01-12 窗口）：

| 查询方式 | 返回行数 | 耗时 | 加速比 |
|---|---|---|---|
| **现状：全历史** | **2,870,262** | **22.45 s** | — |
| **加时间窗** | 14,024 | **0.03 s** | **769.6×** |

**预期整体收益**（按 `invariant` 占比线性推算）：

| 项 | 现状 | 优化后（估算） |
|---|---|---|
| `invariant` 段 | 60.3% 总耗时 | ≈0.1% |
| 写阶段 | 1.32 分钟/片 | **≈0.53 分钟/片** |
| 全量 4,529 片 | 99.6 小时 | **≈40 小时** |

> 精确收益待实施后实测；上表为按占比的保守推算。

---

## 4. 等价性论证（性能优化铁律要求）

### 4.1 语义等价性（核心）

| 项 | 现状 | 优化后 | 等价？ |
|---|---|---|---|
| 查询目标 | `(code, bar_day) → adj_factor` | 同 | ✅ |
| **实际消费** | **仅抽样行所在的 `bar_day`** | 同 | ✅ |
| 未被消费的数据 | 全历史其他日期的因子行 | **不查** | ✅ **本就未被使用** |
| 索引利用 | `PRIMARY KEY (code, time)` | 同（时间窗走索引） | ✅ |

**关键论证**：`_load_factor_lookup` 的返回 dict 仅被以下代码消费：
```python
adj_i = factor_lookup.get((code, day))    # day 全部来自抽样行的 bar_day
```
抽样行来自**当前分片**（时间窗内）→ 全历史其他日期的因子**查了但从不使用**。

**→ 加时间窗是纯增益，不改变任何被消费的值。**

### 4.2 逐条对照铁律禁止项

| 铁律禁止项 | 本方案 |
|---|---|
| 不改 API 函数名/签名/默认值/返回类型/返回字段 | ✅ 新增参数**可选**（默认 None = 旧行为） |
| 不改行情取数范围/日期边界/PIT/复权口径/字段映射/数据源优先级 | ✅ 不涉及（自检查询非取数路径） |
| 不改生命周期调用时机/顺序/次数/上下文 | ✅ 不涉及 |
| 不改撮合/订单/费用/持仓/公司行为 | ✅ 不涉及 |
| 不改策略信号/选股/净值/回测指标 | ✅ 不涉及 |
| 不以"性能优化"为名混入正确性修复 | ✅ **单点改动，不夹带** |

### 4.3 不改变的行为（须验收证明）

- 自检判据 `expect = raw × adj_i / adj_latest`、`REL_TOL` 阈值：**不变**；
- 自检返回 `{"sampled","bad","skipped","unknown_rows","cross_source_adj_i","bad_detail",...}`：
  **同输入下逐项一致**；
- 告警/阻断逻辑（`_qfq_selfcheck_log`、streak 计数）：**不变**；
- 写入结果、水位、门禁：**不变**。

---

## 5. 验收标准

### A. 行为等价（核心，铁律强制）

- **A1**：**同一输入下自检结果逐项一致**——
  构造固定 DataFrame + 固定 `adj_latest_map`，优化前后 `check_qfq_invariant`
  返回的 `sampled / bad / skipped / unknown_rows / cross_source_adj_i / bad_detail`
  **逐项相等**（含 `bad_detail` 内容）。
- **A2**：**边界覆盖**——抽样行位于 `bar_day` 边界（当日 00:00 / 23:59 CST）、
  跨日、跨周末、停牌日时，结果一致。
- **A3**：**回退路径**——`time_lo_ms/time_hi_ms=None` 时行为与实施前**逐位一致**。

### B. 性能

- **B1**：`_load_factor_lookup` 单次耗时下降 **≥100×**（实测 22.45s → 目标 ≤0.2s）。
- **B2**：写阶段 `invariant` 段占比从 60% 降至 **≤5%**。
- **B3**：全量重跑（或代表性窗口）实测吞吐改善，写阶段 **≤0.6 分钟/片**。

### C. 回归

- **C1**：`qfq_invariant.py` 相关测试全绿（`tests/test_qfq_invariant.py` 等）。
- **C2**：daemon/QFQ 相关测试全绿。
- **C3**：6 策略重转 `api_portability` 全 PASS。

### D. 证据

- **D1**：优化前后对比数据（耗时、返回行数、自检结果一致性）写入 `docs/evidence/`。

---

## 6. 回退条件与手段

**回退条件**（满足任一即停并回退）：
1. **A1 出现任何一项不一致**（哪怕 1 行 `bad` 差异）→ **立即回退**；
2. A2 边界场景不一致；
3. B1/B2 性能改善未达预期（说明瓶颈判断有误）；
4. C1/C2 出现任何非预期失败。

**回退手段**：
- 代码：`git reset --hard <实施前 stash hash>`（既有回退点：`5962cf0e` / `5f376892` / `f41be1d7`）
- 本方案**不涉及数据变更**，无需数据回退。

---

## 7. 影响面

| 面 | 影响 |
|---|---|
| QFQ 自检 | 查询范围缩小（仅被消费的数据）→ 结果不变 |
| 写入路径 | 零影响（自检是只读观测，不阻断） |
| 常驻 daemon / GUI / CLI | 零影响（同路径受益） |
| 数据 | **零变更** |
| 回测结果 | **零变更**（自检不参与回测） |

---

## 8. 风险与对策

| 风险 | 等级 | 对策 |
|---|---|---|
| 时间窗边界漏掉边界 bar → 自检 `skipped` 增加 | **中-高** | 窗口按整日取整 + A2 边界专测；**若 `skipped` 有变化即视为不等价** |
| `bar_day` 归一与 time 窗不一致 | 中 | 复用同一 `_bar_day_from_ms` 口径推导窗口 |
| 抽样行跨多个交易日 | 中 | 窗口取 `[min, max]` 全区间（不逐日拆分），保证覆盖 |
| 其他调用方受影响 | 低 | 新参数可选，默认 None = 旧行为 |
| 与口径修正方案叠加 | 低 | **改动文件不重叠**（本方案改 `qfq_invariant.py`；口径方案改 `mcp_adapter.py`） |

---

## 9. 未决项

1. **是否同时优化 `open_ro_sqlite` 每片新开连接**（9.8GB 库）？
   → 建议**单列**（独立优化项，不夹带）。
2. **`other` 段占 18%** 是否进一步细分？
   → 建议**本轮不动**（先拿 769× 的主要收益）。
3. **`sqlwrite` 占 18%** 是否可优化（DuckDB upsert）？
   → 建议**单列评估**。
4. **每片重复注入 adj_factor 5,190 行**（日志占主导但耗时低）？
   → 建议**单列**，不夹带。

---

## 10. 流程状态

| 阶段 | 状态 |
|---|---|
| 取证（瓶颈定位） | ✅ 完成（`defect1-write-phase-bottleneck-20260930.md`） |
| **方案（本文档）** | ✅ 产出 |
| 审计 | ✅ **审计通过**（`caliber-and-qfq-perf-designs-audit-20260930.md`，P-1~P-4 小修订已并入） |
| 实施 | ✅ 完成（`qfq_invariant.py`，单文件） |
| 验收 | ✅ 完成（`qfq-invariant-perf-acceptance-20260930.md`：A1 行为等价 / B1 ≥100× / C1/C2 回归 / C3 6 策略横验证全 PASS） |
| 用户确认 | ✅ 已确认（2026-10-01） |
| 双仓库推送 | ⬜ 执行中 |

> 复核闭环（依「总调度审核回复分序」惯例）：审计小修订 P-1~P-4 已全部并入实施（见验收证据 §1.1）；A1 行为逐项一致为硬门，已通过。
