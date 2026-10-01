# AdjustmentAnchorDrift 根因取证报告（终版·只读）

- **日期**：2026-09-26
- **性质**：**只读取证，未改动任何代码或数据**（DuckDB `read_only=True`；SQLite `mode=ro` + `PRAGMA query_only=ON`）
- **对象**：`[QualityAudit] AdjustmentAnchorDrift`（`stock_minutes` 1099 只 / `etf_minutes` 53 只，max dev = 1.0988）
- **本文档取代**同名早期版本（该版本仅完成现象层定位，代码路径当时判为「未证实」）
- **复现脚本**：`agent_workspace/_probe_anchor_drift_*.py`、`_step2*.py`、`_step3*.py`（含 `*_out.txt` 落盘）

---

## 一、结论（已证实）

> **根因 = `mcp_adapter._restore_to_raw`（入口 `_restore_qfq_if_required`）的「无条件还原」公式被误用。**
>
> 云端实际返回的是**不复权（raw）价**，但 `is_qfq` 标志为 `True`；
> 适配器据此对价格列一律乘以 `adj_latest/adj_i`，把 raw 抬成复权值；
> 随后 aligner 用这条**已被放大的 `close`** 再算 front（× `adj_i/adj_latest`），
> 数学上恰好把 front 又还原成 raw —— **两列双双失真**（详见 §二失效链）。

> ⚠️ **机理表述更正**（本版相对早期版本）：
> `aligner.py:414` 的 native 白名单 `{"baostock","akshare","xtquant"}` **不含 `mcp`**，
> 故 MCP 源**不经过 `qfq_native_passthrough`**，而是**总是**走 `aligner._apply_qfq`
> 用本地因子计算 front。因此「front 列不受影响」的早期说法**不成立**——
> front 是由被污染的 `close` 推导出来的，同样是错误值（值恰好 ≈ raw）。

| 断言 | 证据强度 |
|---|---|
| 云端 `close` == 本地日线 raw（不复权） | **100/107 = 93.5%**（偏离行 **100%**） |
| `is_qfq` 标志与数据实际状态**不符** | 抽样 31,812 行 **全部 `is_qfq=True`**，但其中 93.5% 的值是 raw |
| 落库 `close` == 云端 close × `adj_latest/adj_i` | 000012 偏离行 **9/9 精确吻合** |
| 受影响写入批次全部晚于 P1-③ 变更日 | 100% 落在 **6 个批次**，最早 2026-08-09 > 变更日 2026-08-03 |
| 审计为**部分检出**（存在漏检） | 见 §五 |

---

## 二、写入链路（步骤 1 产物）

```
云端 QuestDB ──(close=raw, is_qfq=True)──► MCP artifact
        │
        │ ① _fetch_export_direct / _resolve_shard_paths
        │    local_parquet.write_bytes(art.parquet_bytes)   ← Raw Landing（还原前，含 is_qfq）
        ▼
② MCPAdapter._restore_to_raw             [mcp_adapter.py:1835-1972]（入口 _restore_qfq_if_required:1617）
      价格列 open/high/low/close/pre_close
      ratio = adj_latest_local / adj_i     (adj_latest 取自 qfq_aux.db 全局最新)
      out[col] = out[col] * ratio          ← **无条件施加，不看 is_qfq 真假**
        │
        │   * _RESTORE_TABLES = {stock_daily, etf_daily, stock_minutes, etf_minutes}
        │   * _RESTORE_PRICE_COLS = (open, high, low, close, pre_close)
        │   * *_front / *_back 不在还原范围
        ▼
③ aligner._apply_qfq                    [aligner.py:985-988]
      front_col = price × adj_i / adj_latest    （price=②还原后的 close；adj_i=云端行内因子）
      ⚠ MCP 不在 native 白名单 {"baostock","akshare","xtquant"}，故必走本步，非 passthrough
        ▼
④ DuckDBWriter ← stock_minutes
```

**失效点（精确闭环）**：cloud 给 raw、is_qfq=True →
② `close_stored = raw × adj_latest/adj_i`（放大，**错**）→
③ `close_front = close_stored × adj_i/adj_latest = raw`（**错**：值=raw，而非前复权价）。

于是：`close` = `raw×ratio`、`close_front` = `raw`，
`close_front/close = 1/ratio`，而契约要求 `= adj_i/adj_latest = 1/ratio`——
**当 `ratio ≠ 1` 即 `adj_i ≠ adj_latest`（历次除权之后）时两列不自洽**，审计告警；
当 `ratio = 1`（无除权）时两列自洽、审计通过，但 `close` 仍可能被后续批次用**新锚**重放污染。

### 关键代码事实

- `mcp_adapter.py:1842-1850` 注释载明 P1-③（**2026-08-03**，commit `f7df29c`）决策：
  > 「**全部行走还原公式，不按 is_qfq 分流**……is_qfq=False 行并非真 raw」
  该决策基于**单点观测**（300182.SZ 108x 尺度断层）；
  **本次取证证明该前提不成立于云端当前实际行为**。
- `_RESTORE_MISSING_FACTOR_FAIL_FAST = True`：因子缺失会 fail-fast，
  但**「因子存在却被误施加」这条路径没有任何守卫**。

---

## 三、外部源核验（步骤 2 产物）

数据源：`data/mcp_landing/exp_stock_minutes_*/*.parquet`（5,750 个文件，实读 1,127 个）
—— 落盘的是 **MCP artifact 原始字节（还原之前）**，且含 `is_qfq` 列 = **云端真值**。

### 3.1 决定性单例（000012.SZ @ 2026-06-15）

| 量 | 值 | 说明 |
|---|---|---|
| 云端 `close` | **4.1100** | 含 `is_qfq=True` |
| 本地日线 raw `close` | **4.1100** | ← **与云端逐位相同** |
| `adj_i` / `adj_latest` | 30.3449 / 30.5076 | ratio = **1.005362** |
| 落库 `close` | **4.1300** | = 4.1100 × 1.005362 = **4.1320** ✓ |
| 落库 `close_front` | 4.0861 | = raw × adj_i/adj_latest ✓（该列正确） |

**云端给 raw，落库被乘了 ratio。** 全表对账：

```
H1 云端 close == 日线 raw        : 100/107 (93.5%)   偏离行 100.0%
H2 落库 close == 云端 × ratio    :  62/107 (57.9%)
```

### 3.2 极端案例（600649.SH @ 2026-06-25）—— 16 倍污染

| 量 | 值 |
|---|---|
| 云端 `close` / 日线 raw | 3.5000 |
| `adj_i` / `adj_latest` | 1.0000 / 16.2775 |
| **落库 `close`** | **57.1300**（= 3.50 × 16.2775） |
| 落库 `close_front` | 3.5098（≈ raw） |
| 相对误差 | **+1532.3%** |
| 审计 drift | **0.0000 → 未告警** |

---

## 四、全量归类（步骤 3 产物）

范围：`stock_dividend` 除权候选 **2,441** 只；可比 `(code, day)` **263,881**。

| 指标 | 值 |
|---|---|
| 审计 FAIL `(code,day)` 单元格 | **13,309** |
| 审计 FAIL code 数 | **1,096**（审计实报 1099，差异来自扫描窗口边界） |
| 偏离日期边界 | 2026-03-02 ~ 2026-09-04（109 个交易日） |
| 按月 | 3月 1232 / 4月 1176 / 5月 1009 / **6月 4739** / 7月 3331 / 8月 1510 / 9月 312 |

### 失真形态分布（13,309 单元格）

| 形态 | 单元格 | 占比 |
|---|---|---|
| `front` 正确 / **raw 失真** | 6,024 | 45.3% |
| 两列互换 | 3,831 | 28.8% |
| 两列皆≈日线（容差内） | 1,887 | 14.2% |
| raw 存了 front | 1,083 | 8.1% |
| raw 正确 / front 失真 | 369 | 2.8% |
| 四者皆不符 | 107 | 0.8% |
| front 存了 raw | 8 | 0.1% |

### 量级分布（按 code）

| 阈值 | code 数 |
|---|---|
| >0.5% | 1,096 |
| >5% | 158 |
| >20% | 112 |
| >50% | 16 |
| >100% | 1 |

### 写入批次关联（**定位关键**）

偏离单元格的 `update_time` **100% 集中在 6 个批次**：

| 批次日 | 占比 |
|---|---|
| 2026-09-07 | 46.1% |
| 2026-09-16 | 25.7% |
| 2026-08-15 | 11.0% |
| 2026-09-08 | 7.5% |
| 2026-08-09 | 6.8% |
| 2026-09-06 | 2.9% |

**最早批次 2026-08-09 > P1-③ 变更日 2026-08-03** —— 与「无条件还原」决策的上线时点吻合。

---

## 五、⚠️ 审计为「部分检出」——实际污染面大于告警面

限定在**审计实际评估范围**（`close_front` 非空，n=72）：

| 口径 | 命中 |
|---|---|
| 审计检出（drift>0.5%） | 40 / 72（55.6%） |
| **落库 `close` ≠ 日线 raw（真失真）** | **27 / 72（37.5%）** |
| 真阳性（检出 ∧ 失真） | 20 |
| **漏检（未检出 ∧ 失真）** | **7** |
| 检出但 `close` 正常（错在 front 列） | 20 |

**机理**：审计只验「两列**互**不自洽」；
当两列**同向偏移**（如 `front` 存了 raw、`close` 被放大同倍），比值仍满足契约 → **静默通过**。

漏检中最严重者即 §3.2 的 **600649 @ 2026-06-25（+1532%）**。

> **推论**：`AdjustmentAnchorDrift` 既非纯误报、也非完备检测器。
> 修复写入路径后，审计判据本身也需复核（应增加「raw 列对日线 raw 的独立校验」）。

---

## 六、未证实 / 尚存不确定项

1. **云端 `is_qfq=True` 为何与数据不符**——属云端（MCP server）侧契约问题，本次未核验其代码；
   仅证实**现象**（值=raw，标志=True）。**外部依赖**。
2. **「两列互换」「raw 存了 front」两类形态（合计 36.9%）的确切写入路径**未逐一还原；
   已验证其与「一次 ratio 偏移」自洽，但未逐行归因到具体代码分支。
3. **`etf_minutes` 53 只**未按同法取证（本次聚焦 `stock_minutes`）。
4. **`stock_daily` 为何 100% 正确**未解释——同为 `_RESTORE_TABLES` 成员，
   推测与其写入批次/云端日线是否真为 qfq 有关，**未验证**。
5. 本次未评估**下游策略受影响程度**（需结合具体策略的取数口径）。

---

## 七、影响面评估

- `stock_minutes` 的 **不复权列（open/high/low/close/pre_close）在 13,309 个 `(code,day)` 单元格上不是真实成交价**，
  偏差量级 0.3% ~ **+1532%**；**158 只 code 偏差 >5%，16 只 >50%**。
- **任何直接读取分钟不复权价的策略/回测，在受影响区间会得到错误价格。**
- `close_front`（前复权）在 45.3% 的受影响单元格上是正确的，但另有 36.9% 形态下也不可靠。
- `stock_daily` 未受本次取证发现的影响。

---

## 八、复现命令

```powershell
$py = "C:\Users\hasym\.conda\envs\quant310\python.exe"
cd D:\hasym\PycharmProjects\QuantStudio
& $py agent_workspace\_probe_anchor_drift_1d_scan.py    # 广域扫描（复刻审计，求 max）
& $py agent_workspace\_probe_anchor_drift_1g_align.py   # CST 对齐核验（必跑，防自身 join 假象）
& $py agent_workspace\_probe_anchor_drift_1i_which.py   # 失真列归属
& $py agent_workspace\_step2_cloud_source.py            # 云端 Raw Landing 取真值
& $py agent_workspace\_step2b_reconcile.py              # 云端 vs 落库 vs 日线 三方对账
& $py agent_workspace\_step2c_blast.py                  # 真实污染面 vs 审计检出面
& $py agent_workspace\_step3_full_coverage.py           # 全量 2441 code 归类
```

**性能提示**：`stock_minutes` 1.95 亿行，`time % 86400000` 谓词全表扫描会 >120s 超时；
须先用 `time` 范围剪枝（1.4s），或按 `code` 定向。
