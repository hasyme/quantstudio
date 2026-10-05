# 数据管线契约（Data Pipeline Contract）

> 版本: v1.0 | 日期: 2026-07-18
> 适用对象: **数据管线维护者**（新增数据源、扩展字段、调整清洗规则的人）
> 配套: `docs/interface-contract.md`（回测层契约）

本文档定义 QuantStudio 数据管线的**核心原则与扩展规范**。任何对 adapter/aligner/validator/writers 的改动都应遵循本契约。

---

## 一、核心原则（不可违反）

### 1. 所有清洗/补算在 aligner 层统一完成
- 字段重命名、单位换算、派生计算（如市值补算）**全部在 aligner 层**
- **回测层只读现成数据，不在回测阶段临时计算**
- 各数据源（tushare/baostock/akshare/xtquant）的数据，经 aligner 统一清洗后入库，DuckDB 里的数据格式/字段/单位完全一致
- 回测的契约对象只有 DuckDB，DuckDB 里的数据从哪来、哪个源、怎么算的，回测完全不知道

### 2. 各源数据清洗成一致格式入库
- 同一张 DuckDB 表，无论数据来自哪个源，字段名/单位/语义必须一致
- 源特有的字段（如 xtquant 有但 tushare 无的）由 aligner 补 NULL 或补算
- 单位统一：股=股、元=元、时间=毫秒戳、比率=小数或%

### 3. 数据项应包含所需全部，不甩给回测临时算
- 凡是回测/策略可能用到的数据项（如 circ_mv、pe_ttm），DuckDB 表里就应该有
- 不存在"回测时用 free_share×close 现算 circ_mv"——应该在 aligner 层提前算好入库
- 新增数据需求时，优先扩展 DuckDB schema + aligner 补算，而非改回测层

---

## 二、两表两层市值口径（D10 修复后的设计）

流通市值（circ_mv）有两个层次的需求，分两张表存：

| 表 | 口径 | 颗粒度 | 字段 | 数据源 | circ_mv 计算 |
|---|---|---|---|---|---|
| `stock_float_share` | 报告期股本+报告期末市值 | 每季一条 | code/end_date/ann_date/free_share/total_share/circ_mv/total_mv | **xtquant Capital**（历史 PIT） | aligner 补算 = free_share × end_date 最近交易日 close |
| `stock_daily_valuation` | 每日估值 | 每日一条 | code/time/circ_mv/total_mv/pe_ttm/pb/turnover_rate | **tushare daily_basic** | 直接取（daily_basic 本就提供每日值） |

### 为什么分两张表
- `free_share`（流通股本）只在财报/限售解禁/增发/送转时变化（每季几次），属报告期数据
- `circ_mv`（流通市值）每天随股价变化，属交易日数据
- 把两者塞进一张表会混两种时间语义，且数据量膨胀（940万行 vs 40万行）
- 分表后：stock_float_share 按报告期 PIT（ann_date），stock_daily_valuation 按交易日 PIT（time）

---

## 三、xtquant 源的 PIT 约束（关键）

### 禁止用 get_instrument_detail 取历史数据
- `get_instrument_detail(code)` 只返回**当前最新**的 FloatVolume/TotalVolume
- 用它取历史 = "用今天的股本算过去的市值" = **未来函数**
- **xtquant 源必须用 `get_financial_data`（Capital 报表）**，按 m_timetag/m_anntime 取历史报告期值

### Capital 报表字段映射
| xtquant Capital | DuckDB stock_float_share | 说明 |
|---|---|---|
| m_timetag | end_date | 报告期末日（YYYYMMDD→ms） |
| m_anntime | ann_date | 公告日（PIT 关键） |
| circulating_capital | free_share | 流通股本（股） |
| total_capital | total_share | 总股本（股） |
| （无） | circ_mv/total_mv | aligner 用 free_share×close 补算 |

### 已下架票自动剔除
- xtquant API 对已退市/下架的票（如 002231）返回空数据
- 这从根上解决了 tushare 保留死票导致候选池污染的问题

---

## 四、aligner 补算规范（derive_fields 层）

### 触发条件
- 仅当 schema 字段存在 + df 缺失（NULL 或 ≤0）时触发
- 源已提供值时保留源值（不覆盖），仅补缺失

### 非交易日处理（merge_asof backward）
- 报告期末日（end_date）可能是周末/假期，当天无收盘价
- 用 `pd.merge_asof(direction="backward")` 取 ≤ end_date 的最近交易日 close
- 不要用"找最近交易日"的复杂逻辑，merge_asof 是标准方案

### 源值比对告警
- 若源已提供 circ_mv 且补算值与源值差异 >5%，logger.warning（不拒绝，记录归因）
- 用于发现源口径错误（如单位换算没做）

---

## 五、新增数据源/字段的扩展规范（发现即扩展）

### 新增数据源支持某表
1. adapter 实现 `fetch_table` 对应分支（返回原始字段）
2. `alignment_rules.json` → `source_mappings.<新源>.<表>` 加 column_map（字段重命名）
3. `SOURCE_CAPABILITY`（config_editor_tab.py + task_tab.py）加该源
4. 若 adapter 已预重命名字段（如 xtquant 财务表），用 `{"identity": true}` 空映射

### 新增字段到已有表
1. `alignment_rules.json` → `schemas.<表>.columns` 加字段定义（type/unit/required）
2. `writers.py` DDL 加列 + dedup键/冲突子句/upsert列（4处）
3. 若字段需补算（非源直供），aligner derive_fields 加补算逻辑
4. DuckDB 既有表需 ALTER TABLE 加列（CREATE IF NOT EXISTS 不会改已有表）

### 发现新字段立即扩展
- 不要拖。发现 xtquant/tushare 有新可用字段，立即补映射 + schema
- 避免"临时在回测层现算"的诱惑——那违背解耦原则

---

## 六、管线唯一性（所有入库路径的咽喉）

```
adapter.fetch_table → aligner.align → validator.validate → writer.write
```

- **validator.validate 是所有入库的唯一咽喉**（daemon 的 4 处 writer.write 全部在其后）
- 增量写入、常驻进程、全量拉取，全部走同一条管线
- PIT 校验（AnnDateLogic）、正值校验（PositiveNumeric）、inf 校验（InfCheck）自动覆盖所有路径

### 写入停摆防护契约（2026-10-02 新增）

`writer.write` 内部**所有**读写语句（去重计数 SELECT、upsert/INSERT、DDL、水位与欠账 upsert、
passthrough 覆盖写、以及 `mcp_adapter` 的第二条主库写通道）一律经 `_WriteGuard` 执行：

- 单条语句超预算（`QS_DUCKDB_WRITE_TIMEOUT_S`，默认 600s）⇒ `logger.critical` + `conn.interrupt()`；
- interrupt 后经 `QS_DUCKDB_WRITE_HARD_ABORT_S`（默认 300s）仍不返回 ⇒ 落诊断 JSONL（flush+fsync）
  后 `os._exit(75)`，释放锁、避免无限空转；
- 停摆一律转为 `DuckDBWriteStalled` 抛给调用方 ⇒ **任务失败、锁释放、水位不推进**，下轮从断点续跑；
  事务回滚保证不存在半截数据；
- 每批日志附 `count/dml/close` 分段耗时，慢批（默认 60s）额外告警；
- **新增写点必须复用 `_guarded` / `_guarded_executemany`**，由
  `tests/test_writer_stall_watchdog.py::test_no_bare_execute_in_wrapped_write_paths` 静态锁定。

设计/证据：`docs/duckdb-write-stall-mitigation-design.md`、`docs/evidence/hang2_stall_drill.txt`。

---

## 七、相关文件

| 用途 | 路径 |
|---|---|
| 字段对齐规则（schema + source_mappings） | `config/alignment_rules.json` |
| 对齐器（清洗/补算） | `quantstudio/pipeline/aligner.py` |
| 入库校验（13 条规则） | `quantstudio/pipeline/validator.py` |
| DuckDB 写入（DDL + upsert） | `quantstudio/pipeline/writers.py` |
| 采集任务配置 | `config/collector_tasks.json` |
| 数据源适配器 | `quantstudio/pipeline/sources/*.py` |
| 采集 daemon（管线编排） | `quantstudio/pipeline/daemon.py` |

---

## 变更记录

| 版本 | 日期 | 改动 |
|---|---|---|
| v1.0 | 2026-07-18 | 初版：两表两层市值口径、xtquant PIT 约束、derive_fields 补算规范、扩展规范 |
| v1.1 | 2026-07-27 | F3-F5：新增 index_constituents 快照完整性契约、SW 行业分类参考数据契约（industry_classification/industry_membership）、index_daily/sw_daily 统一指数日线管线契约 |

## ETF reference metadata for local PIT universes

`etf_daily` is market data only and cannot identify domestic equity, bond, money, commodity, gold, or QDII/cross-border classes. QuantStudio therefore maintains a separate `etf_basic` reference table for `get_etf_list_local`.

- Synchronize with: `python scripts/sync_etf_basic.py --db data/quantstudio.db`.
- Source: Tushare `fund_basic(market="E")`; classification version `etf-basic-v1`.
- Required PIT fields: `code`, `exchange`, `list_date`, `delist_date`, `etf_type`, `is_cross_border`, source/version audit fields.
- `etf_daily` is joined only to confirm bars existed on or before the query date and to fill missing metadata dates during synchronization.
- Strategy indicators, liquidity screens, momentum, abnormal volume, and ranking are prohibited in the provider/data-adapter layer.
- Missing `etf_basic` is an explicit capability error; no code-prefix or latest-list fallback may claim to be a domestic-equity PIT universe.

Because Saturday, July 25, 2026 is the synchronization date, rows whose `list_date` is after July 25, 2026 remain metadata-only future listings and are excluded by the PIT query until their actual listing date and historical bars are available.

## User-selected backtest window provenance

For user-PyQt R5, the Skill records the project DuckDB path and recommends ETF listing/warm-up bounds, but does not select or hardcode the actual dates. Submitted evidence must record the user's actual start/end dates and cannot start before the confirmed ETF-pool hard lower bound.

## `etf_basic` pipeline contract

- **Authority/source:** Tushare only; task source chain is exactly `["tushare"]`. The endpoint is `fund_basic(market="E")`.
- **Granularity:** reference snapshot, not a time-series slice. A successful incremental/resident run records a daily snapshot watermark; the next day fetches the complete source snapshot again.
- **Canonical baseline:** the current DuckDB `etf_basic` schema and semantics. `code` is bare six digits, `ts_code` retains `.SH/.SZ`, `exchange` is `SS/SZ`, and list/delist dates are milliseconds at Asia/Shanghai 00:00.
- **Unit isolation:** only baseline-consumed fields are requested. `issue_amount`, `p_value`, and other numeric fields with unrelated units are not admitted into the table.
- **Enrichment:** missing `list_date` uses the first `etf_daily` bar; a delisted row with no `delist_date` uses the last bar plus one calendar day.
- **Write semantics:** validate the full canonical snapshot, compare it with stored rows excluding volatile `update_time`, and upsert only new or semantically changed rows by primary key `code`. Source-side temporary absence never physically deletes retained historical metadata.
- **Entry-point parity:** CLI full range, CLI/GUI incremental, and resident scheduling call the same adapter, standardizer, validator, diff, writer, and watermark implementation.
- **Compatibility script:** `python scripts/sync_etf_basic.py --db data/quantstudio.db` remains available for bootstrap/manual replacement and imports the same standardizer and DDL as the pipeline task.

## `index_constituents` snapshot integrity contract (F3 §5.6)

- **PIT 语义**：`get_index_constituents(index_code, date)` / `get_index_stocks(date)` 只取不晚于 as-of 日的**最近完整快照**；不是历史并集、绝不使用未来快照、无历史快照返回空（fail-closed）。回测期间 API 注入当前回测日期，绝不读数据库全局最新快照。
- **完整性批次契约（2026-07-27 审核修订）**：完整性只由 `index_constituents_snapshot_meta` 在打点写入时判定（`n_constituents` / `expected_count` / `status`，expected_count 来自 `config/index_constituents_expectations.json` 指数公开元数据），**绝不依赖未来快照**；API 只消费 `status='complete'` 快照，无 meta 的指数 fail-closed；未来数据写入不得改变历史查询结果（测试锁定）。daemon 在 `index_constituents` 任务写入后自动打点（幂等）。
- **写入**：主键 `(index_code, code, time)`，changed-row upsert；同日多源冲突须在写入前消除或按既有数据源优先级处理。

## SW industry classification reference contract (F4)

- **正式权威**：tushare `index_classify(level='L1', src='SW2021')`（分类定义 → `industry_classification`）+ `index_member`（成员历史 → `industry_membership`，`in_date/out_date` → `effective_from/effective_to` 毫秒，`NULL` 表示当前有效）。主键含 `industry_level`（2026-07-27 升级）。安全重建工具：`scripts/rebuild_industry_tables.py`（staging + 门控 + 单事务原子交换，失败正式表不变）。
- **禁止**：不得用 `stock_basic.industry` 中文名匹配生成行业代码；不得生成伪 `SW_<行业名>` 代码；不得用当前快照回填历史；数据源不可用或字段漂移 → fail-closed（成员表 all-or-nothing，不写部分快照）。
- **区间重叠语义边界（F4b，2026-07-27 审核修订）**：官方 `index_member` 契约只有 in_date/out_date，**没有冲突裁决规则**。transform 严格 1:1 保留原始区间（仅剔除 from > to 脏数据），**严禁项目自定义裁决**；同一证券同一 system/version/level 当日有多于一个不同行业归属的**歧义日期**由 Provider 层 fail-closed（`ReferenceDataCapabilityError`）。`query_industry_membership_quality()` 报告重叠对数（原始事实）；硬性门控：orphan=0、bad_ranges=0。能力定级：存在歧义区间时 industry_membership 为 **APPROXIMATION_REQUIRES_CONFIRMATION / DATA_BLOCKED**，不得宣称正式 PIT READY。单行业成员空结果 → all-or-nothing fail-closed。
- **legacy 治理**：旧 `sw_industry` 表仅保留作审计（legacy snapshot），任何公开 API 不得再把它当作正式 SW2021 L1 PIT 数据；Provider 正式读取 `industry_membership`，正式表缺失时 `get_industry` fail-closed。
- **质量门控契约（F4 纠正 hotfix，2026-07-27，基于 036de67）**：`query_industry_membership_quality(table, classification_table)` 已 fail-closed 化——① 入参 `table`/`classification_table` 经标识符白名单（`^[A-Za-z_][A-Za-z0-9_]*$`）校验，注入式非法名直接拒绝；② 当 `classification_table` 不存在时返回 `{"present": True, "classification_present": False, "quality_complete": False, "ok": False, "orphan_rows": None, "reason": "classification_table_missing"}`，**绝不**读取/估算成员质量，避免把"分类缺失"误判为"成员干净"；③ 齐备时 `ok = total>0 and multi_current==0 and orphan==0 and bad==0`，`reason=None if ok else "quality_incomplete"`。`rebuild_industry_tables._gate_membership` 新增四键 `(classification_system, classification_version, industry_level, code)` 的 `multi_current_codes` 硬门禁（同一证券同一分类层级存在 >1 条 `effective_to IS NULL` 即 `ok=False` 阻断交换）；`_assert_constraints` 按官方 DDL 精确比对列名/类型/NOT NULL/PRIMARY KEY 顺序（PK 列隐式 NOT NULL、PK 顺序取列定义顺序），不符即 `RebuildError`。
- **采集任务**：`industry_classification` / `industry_membership`（`config/collector_tasks.json`，source 链 `["tushare"]`，full/incremental/resident 同一路径）。

## `index_daily` / `sw_daily` pipeline contract (F5)

- **统一 canonical 表**：申万行业指数与普通指数同写 `index_daily`（code/time/OHLC/pctChg/volume[股]/amount[元]），不新建策略私有行情表。
- **源端路由**：tushare 普通指数 → `index_daily` 接口；申万行业指数（`.SI` 后缀或 SW2021 L1 宇宙成员）→ `sw_daily` 正式接口。行业指数范围来自 SW2021 L1 分类（adapter probe `index_classify`），允许配置显式代码列表，不写策略白名单。
- **单位**：`sw_daily` vol=万股/amount=万元 → adapter 换算为 `index_daily` 接口单位（手/千元）→ aligner 统一映射为 股/元，与既有 `index_daily` 完全一致；OHLC 单位=点。
- **代码**：内部 canonical 裸码（`801010`），`.SI` 后缀仅存在于源端映射；不加复权列；`fq='pre'` 对指数回退原始 OHLC；行业指数不写入 `stock_daily`。
- **正式宇宙与增量水位（2026-07-27 审核修订）**：daemon per-stock 路径消费 `get_index_daily_universe()`（普通指数 + SW2021 L1，probe 缓存），full/incremental/resident 同一路径；source_watermark、重试、限流、缺口审计、changed-row upsert、源端空数据 fail-closed（字段漂移 RuntimeError、单代码空数据跳过）、最新交易日完整性检查，全部走统一 daemon 路径（full/incremental/次日更新/run_once 有集成测试锁定）。
- **质量门控**：SW2021 L1 31 个行业定义完整；每个行业指数有日线；OHLC 合法（high ≥ max(open,close)、low ≤ min(open,close)）、成交额非负、日期不重复、区间连续、最新交易日覆盖（`query_sw_index_daily_coverage` 报告）。
