# P1 读路径物理行序依赖排查（裁定② 交付物，2026-10-03）

> 用途：V2/V3/V4/V5-主键重建 会改变主表**物理行序**，上线前必须证明读路径不依赖行序。
> 判定标准：满足 (a) 含 ORDER BY / (b) 聚合或去重 / (c) 主键点查 / (d) 消费方按键或集合处理
> ⇒ **不依赖**；若读取后按返回顺序做位置敏感处理（取前 N 行、zip、逐行比较、拼串）⇒ **依赖**。

## 结论（先说结果）

- 共 **46 条**读路径（生产代码 + 脚本），其中 **依赖物理行序 7 条**，**阻塞级 3 条**。
- **存在"物理行序变化会导致可观察结果变化"的直接证据** ⇒
  **不修改读路径就上线 V2/V3/V4 是不可接受的**；必须先落地 B1/B2/B3（均为单行 SQL 补 ORDER BY）。

## 阻塞项（必须先修）

| # | 位置 | 位置敏感点 | 影响面 | 处置 |
|---|---|---|---|---|
| **B1** | `quantstudio/backtest/providers/duckdb_provider.py:224`（上游 SQL `duckdb_data_access.py:1789-1824`） | `get_valuation_query(filters, order_by=[], limit=N)` 时，`df.head(limit)` 在**无 ORDER BY** 的估值查询结果上按物理行序截断 | 策略注入 API `get_valuation_query()`（`ptrade_api.py:871`）→ 选出的前 N 只变化 ⇒ 持仓/净值/回测结论变化 | 上游 SQL 末尾补 `ORDER BY v.code`，或 `order_by` 为空时先 `sort_values('code')` |
| **B2** | `duckdb_data_access.py:2072-2076` | `SELECT DISTINCT code FROM stock_daily WHERE time=(SELECT MAX(time)…)` **无序**，`get_Ashares()` 以 list 原序交给策略 | 策略做 `codes[:N]` / tie-break / `zip(池,权重)` 即漂移；仓内 `fall_reversal_quantstudio.py:67` 等直接消费 | 补 `ORDER BY code`（或 provider 层 `sorted()`） |
| **B3** | `duckdb_data_access.py:2314-2321`（`get_etf_list`）、`2401-2407`（`get_cb_list`）、`2414-2417`（`get_market_detail`） | 同 B2 的 `SELECT DISTINCT code … LIMIT` 无序 | 同上；同族 `query_etf_universe_pit`（2370-2376）**已**带 `ORDER BY e.code`，说明"列表类 API 必须定序"是既有契约，这三处是漏网 | 三处统一补 `ORDER BY code` |

## 非阻塞项（记录处置建议，不阻塞 V1）

| # | 位置 | 说明 | 处置建议 |
|---|---|---|---|
| B4 | `duckdb_data_access.py:333-340` | `ORDER BY time DESC LIMIT 4000000` 的 `time` 非全序，入选集合随扫描序；该方法已 DEPRECATED/生产未调用 | 下线或补 `ORDER BY time DESC, code` |
| B5 | `pipeline/daemon.py:3216` | `drop_duplicates(pk, keep="last")` 是位置性取舍；DELETE+INSERT 不产生重复 ⇒ 当前不触发 | 改 `keep=False` 或按时间列显式择一 |
| B6 | `gui/tabs/quality_tab.py:329-342`（`_sample`，7 处调用） | 异常样本 `LIMIT 5` 无 ORDER BY ⇒ 展示哪 5 行随机 | 补 `ORDER BY code, time`（展示层，低成本） |
| B7 | `gui/tabs/export_tab.py:114-116` | 无序 code 列表拼串填入导出框 | 补 `ORDER BY code` |
| B8 | `gui/tabs/browser_tab.py:90,121` | 默认 SQL `LIMIT 100` 人工浏览 | 可不改 |

## 已核实不依赖（39 条，摘要）

- `query_daily_snapshot` / `preload_daily_snapshots`（459-466、530-549）：返回序未定义，但**所有消费方**
  （`backtest_engine.py:1245/1281/1968` 的 `_df_index`、930-932 的 `dict(zip(code, close))`、947-950 过滤）
  均按键访问 ⇒ 不依赖；
- `_ensure_bars_in_cache`（718-722）：无 ORDER BY，但落缓存后必先 `sort_values(["code","time"])`（952、981-984）；
- `quality_audit.py`（151-290、392-404）：全部 `COUNT(*)` / `GROUP BY … HAVING` 聚合；
- `aligner.py:1132-1135`、`qfq_maintenance.py:218-258`、`exporter.py:127-128`、`events.py:52-55`、
  `ptrade_baseline.py:331-339`、`source_import.py:4736-4760`、各 scripts：均含 ORDER BY / 聚合 / 集合比较；
- **`governance_snapshot.py` 表级 hash 用全 PK 排序键**（`ORDER BY (code,time)` 等，见
  `docs/governance-snapshot-design.md:56`）⇒ 是全序，**hash 与物理行序无关** ⇒ 可直接作为变体前后的
  差异验证工具（注意 `data/snapshots/sort_keys.json` 当前缺失，脚本会 fail-closed，需先恢复）。

## 准入判定

| 变体 | 是否改行序 | P1 准入 | 当前状态 |
|---|---|---|---|
| **V1 子批化** | 否 | **豁免** | ✅ 可进入 A5 在线实验 |
| V2 局部性排序 | 是 | 需要 | ⛔ 被 B1/B2/B3 阻塞 |
| V3 并行度/保序 | `preserve_insertion_order=false` 是 | 需要 | ⛔ 被阻塞（仅 `threads` 不改行序，可单独实验） |
| **V4 staging+显式事务** | 是（DELETE+INSERT 追加表尾） | 需要 | ⛔ 被阻塞 |
| V5 主键重建 | 是 | 需要 | ⛔ 被阻塞（且本次不实施） |
