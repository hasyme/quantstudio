# bars 池预取窗口 SQL 化 · 方案件（六步①，2026-10-01）

- **性质判定**：**纯性能型**（可观察结果不变；P2' 逐位 0 不一致为证）
- **落点**：`quantstudio/backtest/providers/duckdb_data_access.py`（**单文件**，与块一 capital_base 域分离）
- **立项动因**：backlog `218400d`（bars 缓存键规范化 + 单形态预取，目标 4.4→2.2 s/日）——
  **预证推翻其归因**（见 §二），真瓶颈另在；本件按预证结论重定向，**收益远超原目标**
- **回退**：改动限单文件内池预取路径，**单 commit revert**

---

## 一、问题定义（真瓶颈）

**池预取路径「拉全历史只为取 30 根」**：

```
query_bars_by_count_batch(codes=[首请求码], count=5, ...)
  → 池就绪且当日未预取 ⇒ W = max(5, _POOL_W_MIN=30)
  → _query_bars_by_count_batch_impl(pool, W, ...)
      → _use_sql_path=False ⇒ _ensure_bars_in_cache(pool, tbl, cols)
          → SQL: SELECT {cols} FROM {tbl} WHERE code IN (...)      ← 无 time/count 过滤（全历史）
          → 每码数千行入 _bars_history_cache
      → 逐码 full[full["time"] <= before_ms].tail(30)              ← 实际只用 30 根
      → _post(...)（整表排序 + qfq + trade_date）
```

⇒ 1000 码池：**加载 ~200 万行**（全历史）→ 排序/qfq/分组/切片 → **5.29 s/日**。

## 二、预证实录（2026-10-01，全部实测 · 只读影子库 · 未改产品代码）

### 2.1 backlog 归因推翻（两项）

| backlog 主张 | 实测 | 判定 |
|---|---|---|
| 「双形态入池 ⇒ 预取代价翻倍」（2000 entries ≈ 2-3s vs 单形态 0.99s） | 双形态池（1000 有效 + 1000 带后缀）预取 **5.247s**；单形态 1000 有效码 **6.478s**（同为内存路径）⇒ 带后缀无效码**近零成本** | **不成立**（无效码在 SQL 中查不到，无后处理） |
| 「池需双形态以免疫 code 形态差异」 | 池/请求/缓存键**均为库内裸码**；数据访问层收到的请求码**恒为裸码**（上游 `ptrade_api` 以 `bare_codes` 调用，L1441/L1450） | **前提误判**（形态差异在真实链上不存在） |
| —（附带取证） | 绕过 ptrade_api 直接以带后缀形请求数据访问层 ⇒ 返回**空**（`[]`，取不到数据）；SQL 确认库内 code 为裸码 | 双形态入池**并未实现**形态免疫 |

### 2.2 真瓶颈定位（耗时构成）

| 口径 | 实测 |
|---|---|
| 等效 SQL 直取（同池同 W，QUALIFY 窗口） | **0.821s** |
| impl 全路径（PR7 内存：全历史加载 + 切片） | **5.800s** |
| ⇒ **Python 后处理占比** | **≈ 5.0s ＝ 86%** |

| code 数 | 内存路径耗时 | 行数 |
|---|---|---|
| 100 | 0.692s | 3,000 |
| 300 | 2.038s | 9,000 |
| 1000 | 6.478s | 29,994 |
| 2000 | 11.538s | 59,994 |

（W 由 `max(count, 30)` 驱动——传参不改变 W；实测 W∈{5,10,30,60} 耗时无差异，符合设计）

### 2.3 目标路径等价性与性能（P2'/P3'）

产品内**两条既有路径**对照（`_use_sql_path=False` 内存路径 vs `True` 窗口 SQL）：

| 池规模 | A 内存路径（现状） | B 窗口 SQL | 加速 | 条目/行数 |
|---|---|---|---|---|
| 300 | 1.377s | **0.520s** | **2.6×** | 300 / 9,000 一致 |
| 1000 | 5.293s | **1.221s** | **4.3×** | 1000 / 29,994 一致 |
| 2000 | 11.949s | **2.403s** | **5.0×** | 2000 / 59,994 一致 |

**P2' 逐位等价**：300 codes × count∈{5,27} ⇒ **600 项比对，不一致 0**
（行数/列序/dtype/index/值全等，NaN 安全）⇒ **PASS**

⇒ 池预取改走窗口 SQL：**1000 码 5.29 → 1.22 s/日（−77%）**，超 backlog 目标（2.2 s/日）一倍余量。

## 三、设计（改动限单文件）

### 3.1 抽取共用实现（单一真相源）

新增私有方法 `_query_bars_window_sql(codes, count, before_ms, use_qfq) -> Dict[str, DataFrame]`：
- 内容 = 现 `_query_bars_by_count_batch_impl` 的 **SQL 分支体**（逐表 stock→etf→index 的
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) <= ?` 查询 + `_post` + groupby 切片 + INDEX_ETF_MAP fallback）；
- **纯抽取，无逻辑改动**（逐行搬运 + 参数名一致）。

### 3.2 两处接入

| 位置 | 改动 |
|---|---|
| `_query_bars_by_count_batch_impl` 的 `if self._use_sql_path:` 分支 | 分支体改为 `res = self._query_bars_window_sql(remaining, count, before_ms, use_qfq)` 后并入 result（**保持 use_sql_path 回滚语义**） |
| `query_bars_by_count_batch` 的**池预取**两处（首次预取 L799、扩窗 L811） | 由 `self._query_bars_by_count_batch_impl(list(pool), W, ...)` 改为 `self._query_bars_window_sql(list(pool), W, ...)` ⇒ **不再触发全历史加载** |

### 3.3 不改（边界）

- **按需路径**（池缺失 / 非池 code，L816-819）保持现状（内存路径 PR7）——`_bars_history_cache` 语义与消费方不变；
- 公共 API 签名/返回字段/列序/dtype/index 契约；日期边界与 PIT；复权口径；分钟路径；**策略源码零改动**；
- 不改 `_ensure_bars_in_cache`（其他消费方）；
- **不做** backlog 的「键规范化」与「单形态预取」——见 §六 裁定项。

## 四、影响面与等价性论证

| 维度 | 论证 |
|---|---|
| 数据来源 | 两路径**同一 DB、同一 SELECT 列集**；差别仅在「取全历史后内存切片」vs「SQL 侧 QUALIFY 取 N 根」 |
| 逐位等价 | **P2' 实测**：600 项 0 不一致（含 index 契约、dtype、NaN 安全） |
| 排序/组内序 | SQL 分支 `ORDER BY code, time` + `_post` 整表排序 ⇒ 与内存路径同款；bar 主键 (code,time) 唯一 ⇒ 组内序确定 |
| 缓存语义 | 池写入的仍是**同一份 `_bars_window_cache`**（T2 单一真相源不变）；仅填充来源由「全历史切片」变为「窗口 SQL」 |
| PIT/窗口 | `W = max(count, _POOL_W_MIN=30)`、扩窗、日切换回收**全部不变** |
| 回滚 | `_use_sql_path` 开关语义保留（SQL 分支改调共用方法，行为不变） |

## 五、验收标准（照全池共享 V0-V5 先例，全项硬门）

| 门 | 判据 |
|---|---|
| **V0 生效** | 池预取不再调用 `_ensure_bars_in_cache`（日志/计数为证）；`_bars_window_cache` 非空；命中率不降 |
| **P1/P2 数据层逐位** | 池切片（1000 码 × {5,27}）vs 直连 SQL 逐位 ⇒ **0 不一致** |
| **nav_sha 逐位一致** | `ab_perf_chain_runner` 方法学，至少覆盖**断板反包 + 全球资产轮动**双策略（卷内基线 sha 前置实测钉死） |
| **横验多策略** | 「6 策略横验」`run_contract_gate.py --strategies` ⇒ `CONTRACT GATE : PASS` |
| **pytest** | 本批相关套件全绿 + 既有失败基线复现 + 逐项声明 |
| **V5 性能** | 池预取 1000 码：**5.29 → ≤1.5 s/日**（实测 1.221s）；断板反包 wall 前后对照入证 |
| **任何可观察结果不一致** | **即停且回退**（不接受「差异很小」） |

## 六、待总调度审计裁定事项

1. **主件范围**：采本件「池预取窗口 SQL 化」（收益 −77%，纯性能型，P2' 已过）；
2. **backlog 两子项处置建议**（预证结论：均**收益≈0 且前提不成立**）：
   - 「单形态预取」——**不做**（无效码近零成本；且会削弱既有形态免疫兜底）；
   - 「键规范化」——**不随本件**（真实链请求码恒裸码 ⇒ 无影响面、无收益；若将来出现带后缀请求路径，另行作为**功能修正件**立项，因其属行为变化：空 → 有数据）；
   - backlog 件 `218400d` 状态更新为「预证结案：归因推翻，真瓶颈另修（本件）」。
3. **实施窗**：与块一（capital_base，ptrade_api 域）**文件域分离**——本件单文件 `duckdb_data_access.py`；
   pytest 全库跑 / daemon 窗口按「daemon 生命周期跨会话占用纪律」**声明后执行**。

## 七、风险与回退

| # | 风险 | 缓解 |
|---|---|---|
| R1 | 窗口 SQL 与内存路径不等价（已 P2' 600 项 0 不一致） | 实施后**同款预证复跑**（1000 码规模）+ V0/P1/P2 硬门 |
| R2 | SQL 分支抽取引入回归 | 纯搬运；抽取后 `_use_sql_path` 回滚路径与池预取共用同一实现（单一真相源） |
| R3 | 非池 code 路径受影响 | 明确不改（§3.3）；回归含非池码场景 |
| R4 | 与并行会话冲突（共享核心文件） | 写前快照（create + **store 持久化**）+ 精确清单 add + **edit 后即时 `git diff` 自检** |
| R5 | 内存峰值（全历史缓存不再被池填充） | 正面效应（内存下降）；实测前后 RSS 入证 |

**回退**：单 commit revert（改动限单文件内两处调用 + 一个新方法）。