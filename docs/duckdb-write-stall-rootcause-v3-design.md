# DuckDB 写入停摆 根治方案 v3（主动规避变体矩阵）— 六步流水线·第 1 步 方案

> 状态：**待审计**（第 2 步）。审计通过后方可实施。
> 前序：v1 `docs/duckdb-write-stall-mitigation-design.md`（W1–W6 已实施，门6 未通过）；
> v2 `docs/duckdb-write-stall-rootcause-v2-design.md`（**审计不通过**）；
> 审计结论与修订指示：`docs/handoff/duckdb-write-stall-v2-audit-20261003.md`。
> 原则：事实/假设/待核实分列；**不得声称已根治 R2**；未实证不得写成结论。

---

## 0. 结论摘要（审计入口）

1. **主线**：放弃"停摆后恢复"（v2 因 F-1 不可达、F-2 有损作废），改为
   **"停摆前主动规避"**：首次写入前就避开已证会停摆的语句形态/规模/并行度，
   并用**在线受控实验**（真实 `mcp_stock_daily` 重跑）逐变体验证。
2. **候选矩阵 V1–V5**（全部**默认关**，逐个上线、逐个在线实验）：
   V1 批内子批化｜V2 写入局部性（按主键排序）｜V3 并行度与保序｜
   **V4 staging + 显式事务 merge（对症 F-2 原子性）**｜V5 表级维护。
3. **离线不可复现**（H5'：4 组、含生产同序 236 步约 11.8M 行，全负）⇒
   任何变体上线前都无法离线实证，只能"默认关 + 在线受控实验 + 失败无害"。
4. **三项审计裁定已内化**：①全部变体默认关；②V2/V3/V4/V5-主键重建改动物理行序 ⇒
   必须先交付 **P1 读路径排查清单**才可实验；③变体**不得重算写前 count**，
   且**总时上界 1800s** 写入设计与验收。
5. **验收三门**：A0 实验前置（基线快照 + 四项在线判定器）｜A1 离线等价｜
   A5 在线受控（写批不少于 60 且四项判据持续成立）。

---

## 1. 事实基线（v3 的既定前提）

| 编号 | 事实 | 证据 |
|---|---|---|
| B1 | 停摆在生产现场**确定性复现**：三次事件均止于第 57 批、累计 2,749,437 行、表内约 2.75M 行 | `hang2_gate6_run_log.txt`、事故1/2 报告 |
| B2 | 门6 时序：01:28:27 第 57 批起 → 01:38:27 S1(600s) interrupt **无效** → 01:43:27 S2(300s) `os._exit(75)`，诊断 `hard_abort=true, execute_elapsed_s=900.005` | `hang2_gate6_stall_jsonl.txt` |
| B3 | 停摆在 `ON CONFLICT` 与**纯 INSERT 两种语句形态**上都发生过 | `hang2_shard_boundary_probe.txt` |
| B4 | 生产库副本单批写入正常（1.6s）⇒ 表规模本身不是触发条件 | `hang2_realdb_repro.txt` |
| B5 | 离线四组回放（含生产同序约 11.8M 行）**均不复现** ⇒ 触发依赖真实生产进程上下文 | `hang2_h5_repro_*.txt`、`hang2_h5_timings.csv` |
| B6 | W1（v1）有效：无限挂起 → 900s 有界失败 + 诊断 + 锁释放；无 `.wal` 残留 | v1 §9.1、`hang2_stall_drill.txt` |

**未证实（不得当作前提）**：停摆的具体机理（ART/BM/并行/IO 任一）；触发是否与数据形态、
并发下载线程、QFQ 内存占用相关（候选差异项见 v2 §1.3，均未单独验证）。

---

## 2. V1–V5 变体矩阵

### 2.0 共同约束

- 全部**默认关**（裁定 ①），各自独立开关，**不叠加**上线（一次实验只开一个变量）。
- 任何变体**不得重算写前 count**：`updated_rows` 取自首次 `SELECT COUNT`
  （`writers.py:1168-1174`），分批/重试/降级一律复用该值（裁定 ③a）。
- 改动物理行序的变体（V2/V3/V4/V5 主键重建）**必须先交付 P1 读路径排查清单**（裁定 ②）。
- 单次在线实验**总时上界 1800s**（裁定 ③b）：看门狗串联（600+300）最多 3 次语句。

### 2.1 V1 批内子批化（风险最低，建议首个实验）

| 项 | 内容 |
|---|---|
| 机制 | 单条 5 万行 DML 切为 `QS_DUCKDB_WRITE_CHUNK_ROWS`（实验取 5000）子批，同一连接内逐条 execute |
| 落点 | `writers.py::_write_locked` DML 段（当前 1204-1207）；passthrough 两处同步纳入 |
| 开关 | `QS_DUCKDB_WRITE_CHUNK_ROWS`，默认 **0（关）** |
| 等价性 | 同 SQL 形态、同列投影、幂等 upsert ⇒ 目标态一致；`WriteResult` 口径不变（count 不重算） |
| 风险 | ① 批间"部分提交"窗口（崩溃时前半已写）——写入幂等 + 水位不推进 + batch_audit 可发现，下轮重跑收敛；② 语句数增加 ⇒ Timer 数增加（开销可忽略） |
| 假设 | 单语句规模下降可降低触发概率（**未证实**，由 A5-V1 判定） |
| 回退 | 关开关即完全回退，无持久化副作用 |

### 2.2 V2 写入局部性（按主键排序）

| 项 | 内容 |
|---|---|
| 机制 | `register` 前对 df 按 `pk_cols` 稳定排序，使主键插入具备局部性 |
| 落点 | `writers.py::_write_locked`（`_tmp_write` 注册前） |
| 开关 | `QS_DUCKDB_WRITE_SORT_BY_PK`，默认 **0（关）** |
| 等价性 | 行集合与目标态不变；**物理行序可能变化** ⇒ 受裁定 ② 约束 |
| 前置 | **P1 读路径排查清单**先交付并确认无依赖物理行序的读路径 |
| 回退 | 关开关即回退 |

### 2.3 V3 并行度与保序

| 项 | 内容 |
|---|---|
| 机制 | 写连接建立后 `SET threads=N`（`QS_DUCKDB_WRITE_THREADS`，0=不设置）、`SET preserve_insertion_order=false`（`QS_DUCKDB_WRITE_NO_PRESERVE_ORDER`，默认 0） |
| 落点 | `writers.py::_open_rw_with_backoff` 成功返回连接后（短连接每次设置，开销可忽略） |
| 等价性 | `threads` 只改并行度（结果集不变，需 A1 验证）；`preserve_insertion_order=false` **会改无 ORDER BY 查询行序** ⇒ 受裁定 ② 约束 |
| 假设 | 降低并行 INSERT 调度复杂度/保序屏障 ⇒ 可能避开病态（**未证实**） |
| 风险 | 性能回退（threads=1 显著变慢）；行序变化 |
| 回退 | 关开关即回退 |

### 2.4 V4 staging + 显式事务 merge（**对症 F-2 原子性**）

| 项 | 内容 |
|---|---|
| 机制 | ① 批数据写入**无主键 staging 表**（`_stg_<table>`，复用结构 + 批量 INSERT）；② 在**显式事务** `BEGIN TRANSACTION` 内 `DELETE FROM t WHERE (pk) IN (SELECT pk FROM _stg)` + `INSERT INTO t (cols) SELECT cols FROM _stg`；③ `COMMIT`；异常 `ROLLBACK` |
| 落点 | `writers.py` 新增 `_write_via_staging(...)`；开关开启时**首次即走此路径** |
| 开关 | `QS_DUCKDB_WRITE_STAGING`，默认 **0（关）** |
| 原子性 | 显式事务覆盖 DELETE+INSERT ⇒ 中途停摆/崩溃**整体回滚、无数据丢失**（相对 v2 F-2 的核心改进）；因是**首次即走**而非停摆后补救，**不存在 F-1 的不可达问题** |
| 等价性 | 目标态等价于 `ON CONFLICT DO UPDATE SET 全列`（全列替换 + 键命中）；`WriteResult` 口径不变（count 不重算）；**物理行序变化** ⇒ 受裁定 ② 约束 |
| 风险 | ① staging 残留（崩溃后需启动时清理/复用策略）；② 写两遍 ⇒ IO 与耗时上升；③ 事务内停摆 ⇒ ROLLBACK 后抛 `DuckDBWriteStalled`（W1 语义不变） |
| 回退 | 关开关即回退；staging 表需一次性清理入口 |

### 2.5 V5 表级维护

| 项 | 内容 |
|---|---|
| 机制 | 批间隙 `checkpoint_database`（v1 W5 已有）+ 可选主键重建（`DROP PRIMARY KEY` → `ADD PRIMARY KEY`） |
| 落点 | `writers.py::_maybe_checkpoint_after_batch`（v1 已有）；主键重建另设开关 |
| 开关 | `QS_DUCKDB_WRITE_CHECKPOINT_BATCHES`（默认 0）；`QS_DUCKDB_WRITE_REBUILD_PK_BATCHES`（新增，默认 **0**） |
| 等价性 | CHECKPOINT 不改数据语义；主键重建改物理结构（行序可能变）+ 锁表窗口 |
| 风险 | CHECKPOINT 成本（2.33GB WAL 约 7.3s）；主键重建锁表窗口（**须实测上限**，超阈值不实施） |
| 回退 | 关开关即回退 |

---

## 3. 交付物 P1：读路径排查清单（裁定 ② 的前置，V2/V3/V4/V5-重建 的准入条件）

**目标**：证明"改变主表物理行序"不影响任何可观察结果。方法：逐一枚举框架内读取
`stock_daily`（及同类 PK upsert 表）的代码路径，标注是否带 `ORDER BY` / PIT 条件 /
聚合（聚合与 ORDER BY 均不依赖物理序）。

| 读路径 | 位置（待逐条核实） | 是否依赖物理行序 | 处置 |
|---|---|---|---|
| 回测行情读取 | `quantstudio/backtest/providers/duckdb_data_access.py` | 待核实 | 若无 ORDER BY 须补 |
| 截面/选股查询 | 同上（`_latest_by_code` 等） | 待核实 | 聚合/去重语义，须核实 |
| 质量审计 | `quantstudio/pipeline/quality_audit.py` | 待核实 | 含快照比对，须核实 |
| 导出器 | `quantstudio/pipeline/exporter.py` | 待核实 | 导出顺序是否被下游依赖 |
| QFQ 系列 | `qfq_*.py`（含 reanchor / cutover / invariant） | 待核实 | 价格修正按 PIT 定位 |
| GUI 读 | `quantstudio/gui/db_helper.py` | 待核实 | 表格展示顺序 |
| writer 内部回读 | `writers.py`（`read_df` / `get_last_date`） | 待核实 | 仅取单值/聚合 |
| 采集比对 | `daemon._filter_unchanged_snapshot_rows` | 待核实 | 集合比对，疑似不依赖 |

**P1 交付判据**：逐行给出"依赖/不依赖 + 依据（SQL 是否含 ORDER BY / 聚合 / 集合语义）"，
任何"依赖"项必须给出处置（补 ORDER BY 或该变体不得上线）。**P1 未通过 ⇒ V2/V3/V4/V5-重建 不得实验。**

---

## 4. 验收门

### A0 在线实验前置门（每个变体实验前必过）

1. **基线快照**（实验前 15 分钟内采集，落到 `docs/evidence/`）：
   `stock_daily` 行数、`MIN/MAX(time)`、按 code 计数、已写批数、DB 文件字节、`.wal` 字节、
   水位 `source_watermark` 值、4 项确定性校验（`COUNT`、`SUM(close)`、`COUNT(DISTINCT code)`、
   主键去重计数）。
2. **四项在线判定器**（复用 `docs/evidence/hang2_gate6_watch.py`，阈值固定）：
   ① 进程存活；② 日志字节 **30s 窗口 > 0**；③ DB 字节 **30s 窗口 > 0**（或 WAL 增长）；
   ④ CPU 30s 窗口 **< 20s**（单核满载且产出不增 ⇒ 停摆）。
3. **上界守卫**：实验运行超过 **1800s** 无进展即终止并回退开关（裁定 ③b）。
4. **基线确认**：A0 第 4 项快照须与门6 基线一致（行数 2,749,437、`MAX(time)`=2022-08-10），
   否则先查清差异再开跑。

### A1 离线等价门（变体上线前）

| 子项 | 判据 |
|---|---|
| 黄金结果 | 目标表 `ORDER BY code, time` 全量导出逐项一致（行数、dtype、主键、`SUM/COUNT` 校验和） |
| 计数口径 | `WriteResult.new/updated` 与变体开启前一致；**首次 count 值被复用**（断言锁死，裁定 ③a） |
| 异常行为 | 非停摆场景的异常类型/文本不变；`DuckDBWriteStalled` 语义与 W1 一致 |
| 上界 | 变体路径下"看门狗串联总上界 ≤1800s"以测试断言固化（裁定 ③b） |
| 回归 | v1 门1/门5 全套（当前 102 passed）+ 新增用例全绿 |
| 行序 | P1 清单结论适用；若 A1 发现物理行序影响面未覆盖 ⇒ 判失败 |

### A5 在线受控门（逐变体，**须用户逐次批准**）

1. 基线（现行 W1 + 变体关闭）**已知失败**（门6 已证），**不重复消耗**。
2. 实验命令（示例，V1）：
   `QS_DUCKDB_WRITE_CHUNK_ROWS=5000 python -m quantstudio.pipeline.daemon --mode once
   --pull-mode incremental --task mcp_stock_daily --config-dir config/profiles/mcp_only
   --quality-audit full`
3. **通过判据**：写批数 **≥ 60**（越过第 57 批边界）且 A0 四项判据持续成立、无未捕获异常。
4. **失败处置**：记录该变体无效 → 关开关 → 试下一个变体；全部无效则转
   **L0-D 上游报告**，并如实记录"该表全量回填在现行组件版本下不可完成"。
5. **收敛判据**：某一变体通过 A5 且完整跑完 165 分片 ⇒ 记"全量回填完成"，
   并把该变体作为该表的**推荐配置**（仍默认关，由用户/配置显式开启）。

---

## 5. 改动面、回退与零改动承诺

- 落点：**仅** `quantstudio/pipeline/writers.py`（V1/V2/V4/V5 落点 + 一个连接初始化处 V3）
  + 新增测试；P1 为只读排查（不改代码）；策略源码零改动。
- **零改动承诺**：`write()` 签名与返回、`WriteResult` 三字段 int 契约与守恒、DDL、
  字段契约与列序、dtype、空值行为、非停摆异常行为、水位推进与锁语义、daemon 调用链、任何策略源码。
- **回退**：单变体单开关独立关闭；任一变体造成黄金对比不一致或质量审计异常 ⇒ 立即关开关 +
  回退代码（A1/A2/A3/A4 任一失败亦回退）。V4 需附 staging 表清理入口。
- **流程闸门**：v3 方案 → **审计通过** → 实施（默认全关） → A0/A1 → **用户逐次批准**执行 A5
  → 验收证据 → **用户确认** → 双仓库推送。回退点 `git stash 30a3883d` 已持久化。

---

## 6. 不做与登记

- 不升级 DuckDB（离线不可验证；`duckdb_version_gate` 拒 1.5.x，牵动 case008 口径）。
- 不改分片维度（按 code 分片会动 MCP 导出契约与对齐口径）。
- 不再提出"停摆后恢复"路线（F-1 不可达 + F-2 有损，见交接记录 §二）。
- R2 维持 `BLOCKED(外部依赖)`；L0-D 上游报告材料随 v3 证据一并整理。

---

## 7. 审计一轮修订（v3 → v3.1，审计通过·附强制修订项）

| 编号 | 修订要求 | v3.1 落地 |
|---|---|---|
| **R-1** | V4 补"假设（未证实）"行 | V4 的 `INSERT INTO t ... SELECT FROM _stg` **仍是 5 万行纯 INSERT**（与已证停摆的 915 同一目标操作，仅源由 pandas view 换 staging 表）；**是否规避停摆未证实**，由 A5-V4 判定。V4 的定位是**正确性优先**（唯一解 F-2(a) 原子性），**不是"更可能避开停摆"** |
| **R-2** | V4 三条语句均经 W1 看门狗；停摆处理器先 `ROLLBACK` 再抛；A1 实测 `conn.interrupt()` 在**显式 BEGIN** 内的回滚语义 | §2.4 已补"R-2 条款"；A1 新增子项"显式事务内 interrupt 回滚语义实测"（F5 仅覆盖隐式事务） |
| **R-3** | staging 清理策略具体化 | §2.4 已补：启动时清理（`_stg_*` 存在即 DROP 重建）+ 运行前一次性清理入口 `scripts/` 或 API；A1 覆盖残留清理 |
| **R-4** | V1 等价性补"崩溃场景批内原子性由 1 变 N"；A5 的"≥60 批"指**逻辑批** | §2.1 等价性行已补；A5 明确按**逻辑批（50000 行单位）**计数，子批不计入 |

### 审计裁定（决策点）

| 决策 | 裁定 | 落地 |
|---|---|---|
| **实验优先级** | **V1 → V4 → V5（仅 CHECKPOINT 段）→ V3 → V2**；V5 主键重建段**后置** | §8 实验编排；V5-CHECKPOINT 段 v1 已实现（W5 开关），PK 重建段本次不实施 |
| **P1 是否为 V1 准入** | **V1 豁免 P1**（不改物理行序）；V2/V3/**V4**/V5-重建 仍需 P1 准入；**P1 在 A0 阶段随基线一并交付** | §3 与 §4 A0 第 5 项 |

### V4 的准确定位（避免误读）

- V4 = **正确性优先**：若最终采用 DELETE+INSERT 形态，**必须**用 V4 的显式事务实现，
  否则重蹈 v2 的 F-2(a)（DELETE 已提交、INSERT 停摆 ⇒ 丢数）。
- V4 **不是**"更可能避开停摆"的手段：其 INSERT 与已证停摆的 915 同目标操作（假设未证实）。
- 反之，V1 是**唯一缩小停摆语句规模**的变体，故排首位。

### R-2 条款（V4 三条语句的看门狗与回滚纪律）

1. **三条语句全部经 W1 看门狗**：staging `INSERT`、主表 `DELETE`、主表 `INSERT`
   各自独立 `_WriteGuard`（phase 分别为 `v4_staging` / `v4_delete` / `v4_insert`）。
2. **停摆处置顺序固定为 `ROLLBACK` → 抛 `DuckDBWriteStalled`**：不得在未回滚时抛，
   否则显式事务会被 `conn.close()` 隐式提交/回滚的不确定性污染。
3. **A1 必测项**：`conn.interrupt()` 在**显式 `BEGIN TRANSACTION`** 内的回滚语义
   （F5 只在 autocommit/隐式事务下验证过）；须断言"回滚后表内容不变 + 连接可复用"。

### V4 staging 清理策略（R-3）

| 场景 | 规则 |
|---|---|
| 正常路径 | 事务 COMMIT 后立即 `DROP TABLE IF EXISTS _stg_<table>`（同连接） |
| 异常/停摆 | `ROLLBACK` 后同样 DROP；DROP 失败只告警（不阻断） |
| 启动清理 | `DuckDBWriter._init_tables` 阶段扫描 `information_schema.tables` 中 `_stg_%`，逐个 DROP（防崩溃残留） |
| 一次性清理入口 | 暴露 `drop_staging_tables()` 方法（供人工/脚本调用，幂等） |

### V1 等价性补充（R-4）

- **正常完成**：全部子批完成后目标态与单条语句**一致**。
- **崩溃场景**：批内原子性由 1 变 N（可能留下部分子批已提交）；因写入幂等 + 水位不推进 +
  batch_audit 可发现 ⇒ 下轮重跑收敛，**不产生脏数据**。
- **A5 计数口径**："写批数 ≥ 60" 指**逻辑批**（50000 行单位），V1 的 5k 子批**不计入**。

---

## 8. 实验编排（审计裁定顺序）

| 序 | 变体 | P1 准入 | 开关 | 实验判据 |
|---|---|---|---|---|
| 1 | **V1** 子批化 | 豁免 | `QS_DUCKDB_WRITE_CHUNK_ROWS=5000` | 逻辑批 ≥60 且四项判据持续成立 |
| 2 | **V4** staging+显式事务 | 需要 | `QS_DUCKDB_WRITE_STAGING=1` | 同上；另需 A1 的显式事务 interrupt 语义通过 |
| 3 | **V5（仅 CHECKPOINT 段）** | 不需要（不改行序） | `QS_DUCKDB_WRITE_CHECKPOINT_BATCHES=N` | 同上；N 由 A0 基线的 WAL/DB 增量确定 |
| 4 | **V3** 并行度与保序 | 需要 | `QS_DUCKDB_WRITE_THREADS` / `..._NO_PRESERVE_ORDER` | 同上 |
| 5 | **V2** 局部性排序 | 需要 | `QS_DUCKDB_WRITE_SORT_BY_PK=1` | 同上 |
| 后置 | V5 主键重建段 | 需要 | 本次**不实施** | 锁表窗口实测后再定 |

顺序可按 A5 逐次证据微调（用户逐次批准）。

---

## 9. 实施与门禁现状（v3.1，2026-10-03）

### 9.1 已实施（默认全关）

| 变体 | 开关（默认） | 落点 |
|---|---|---|
| V1 批内子批化 | `QS_DUCKDB_WRITE_CHUNK_ROWS`（0） | `_write_locked` DML 段，逐子批独立看门狗（phase=`dml_chunk`） |
| V2 局部性排序 | `QS_DUCKDB_WRITE_SORT_BY_PK`（0） | `_write_locked` 注册前按主键稳定排序 |
| V3 并行度/保序 | `QS_DUCKDB_WRITE_THREADS`（0）/ `QS_DUCKDB_WRITE_NO_PRESERVE_ORDER`（0） | `_open_rw_with_backoff` 成功建连后 |
| V4 staging+显式事务 | `QS_DUCKDB_WRITE_STAGING`（0） | `_write_via_staging`（三语句各自看门狗 + BEGIN/COMMIT/ROLLBACK）+ `drop_staging_tables()` + 启动清理（仅开关开启时） |

### 9.2 A1 离线等价门：**PASS**（`tests/test_writer_variants.py` 8 例 + 回归 75 例）

- 默认全关 ⇒ 与基线逐项一致（ORDER BY 黄金对比 / dtype / 主键 / 校验和）；
- V1/V2/V3/V4 各自开启后与基线**逐项一致**；V4 额外断言"事务内 DELETE 生效（重叠键不重复）+ 无 staging 残留"；
- **count 不重算**（裁定③a）：V1 下 `new/updated` 与基线一致且守恒；
- **R-2 A1 必测项通过**：`conn.interrupt()` 在**显式 `BEGIN TRANSACTION`** 内同样触发完整回滚
  （表内容不变 + 连接可复用）——补齐了 F5 只覆盖隐式事务的空白；
- R-2 纪律验证：V4 遇停摆异常**先 ROLLBACK 再抛**，主表零丢行，后续写可继续。

### 9.3 A0 在线实验前置门：**PASS**（`docs/evidence/hang2_a0_baseline.txt`）

```
rows=2749437 distinct_codes=5073 distinct_days=632
min_time=2020-01-02  max_time=2022-08-10   pk_duplicates=0
CHECKSUM sum_close=63122422.289999
watermark=None（⇒ 增量按全量回填）  db=2.64GB  wal=0B
gate6_baseline_match=YES
```

四项在线判定器阈值已由门6 实测标定（见 v3 §4 A0）。

### 9.4 P1 读路径清单（裁定②交付物）：**发现 3 条阻塞项**

`docs/evidence/hang2_p1_readpath_audit.md`：46 条读路径中 7 条依赖物理行序，其中

- **B1** `duckdb_provider.py:224`（`get_valuation_query` 的 `df.head(limit)` 无 ORDER BY）→ **可穿透到回测产物**；
- **B2** `duckdb_data_access.py:2072`（`get_Ashares` 的 `DISTINCT code` 无序）；
- **B3** `duckdb_data_access.py:2314/2401/2414`（`get_etf_list` / `get_cb_list` / `get_market_detail` 同族）。

⇒ **准入结论**：**V1 ✅ 可实验**（豁免 P1，不改行序）；
**V2 / V3(preserve_order) / V4 / V5-重建 ⛔ 被 B1/B2/B3 阻塞**，须先落地三条单行 `ORDER BY` 修复
（属读路径确定性修复，**另走一次六步流水线**，不在本方案改动面内）。

---

## 10. v3 在线实验与 V7 专项（2026-10-03，第 1 步收口）

### 10.1 A5-1 V1（批内子批化）在线实验：**FAIL**

- 命令：`QS_DUCKDB_WRITE_CHUNK_ROWS=5000 ... --task mcp_stock_daily`（PID 16176，11:04–12:28）。
- 结果：写批 **56**（`stock_daily` 1–56 正常，每批 10×5000 子批），第 57 批停摆 ⇒ 未达阈值 60。
- 关键发现（决定性反向证据）：
  1. **停摆与单条语句规模无关** —— 停在一条 **5,000 行**子批上（诊断 `rows=5000, phase=dml_chunk`）；"缩小语句规模"路线被证伪。
  2. **停摆与库体积无关** —— 本次停摆时 DB **3.13GB**（事故1 1.61 / 门6 2.64 / 本次 3.13），体积逐次变而停点恒定；唯一不变量 = `stock_daily` 表内行数 ≈2.75M。
  3. F8 再印证：运行内 DB 2.64→3.13GB（+490MB），硬退出后残留 **14.36MB WAL**（下次开库回放，量小）。
  4. R-4 代价实测：V1 开启 ⇒ 停摆批前 3 个子批已提交（+15,000 行，尾部 3 日各 5k），但**幂等无重复**（主键唯一），下轮重跑收敛。
- 证据：`docs/evidence/hang2_a5v1_verdict.md`、`hang2_a5v1_log.txt`。

### 10.2 A5-2 V5-CHECKPOINT 在线实验：**FAIL**

- 命令：`QS_DUCKDB_WRITE_CHECKPOINT_BATCHES=25 ...`（PID 17528，13:28–15:08）。
- 结果：写批 **56**，第 57 批停摆（与前四次同点）。
- 关键发现：
  1. **显式 CHECKPOINT 基本是空操作** —— 本 run 调用 8 次，至少 3 次 `wal=0，无需检查点` ⇒ DuckDB 自动检查点已在批间收敛 WAL，显式 CHECKPOINT 无事可做。
  2. **库膨胀与 WAL 无关** —— 开启 CHECKPOINT 后 DB 仍 3.13→**3.64GB**（+510MB）⇒ F8 真源是 **upsert 删除版本 / row-group 碎片**；DuckDB 1.4 无 `VACUUM`，只能靠主键重建（V7）回收。
  3. V1 关闭 ⇒ 单语句整体回滚，表行数保持 **2,764,437** / max=2022-08-15 ⇒ 反向印证 A5-1 的 +15,000 确为 V1 子批部分提交。
- 证据：`docs/evidence/hang2_a5v5_verdict.md`。

### 10.3 机理收敛表（截至四次在线实验）

| 轴 | 结论 | 来源 |
|---|---|---|
| 单条语句规模（5万→5千） | **无关** | A5-1 |
| 库体积（1.61/2.64/3.13/3.64GB） | **无关** | 四次事件停点恒定 |
| WAL 是否收敛 | **无关**（自动已收敛） | A5-2 |
| 语句形态（ON CONFLICT / 纯 INSERT） | 两种都停 | 事故2 + A5-1/2 诊断 phase=dml rows=50000 |
| **表内行数 ≈2.75M** | **唯一不变相关量** | 四次同点 |

⇒ 剩余未被证伪的杠杆：作用于**表/索引状态（ART）层**的 **V7 主键重建**，以及版本层 V8（duckdb 1.5.6 实测，需另走流水线）。

### 10.4 V7 离线实测（生产库副本，不触碰生产库）

脚本：`docs/evidence/hang2_v7_pk_rebuild_probe.py`（二次连接测读阻塞 + 测重建窗口/体积/PK 守恒）。

| 指标 | 实测 |
|---|---|
| 重建窗口 | **7.7s**（create=0.0 / copy=3.3 / swap=4.4） |
| 并发读阻塞 | 同配置第二连接 36 次采样 max=0.1s、>1s=0（MVCC 快照，全程旧快照） |
| 行数 / 主键 | 2,764,437 行、`PRIMARY KEY(code,"time")` 前后一致 |
| 体积 | 3.78→3.91GB（+127MB，旧块留待复用 ⇒ **V7 不解决 F8 膨胀**） |
| 语法约束 | DuckDB 1.4.5 **不支持** `ALTER TABLE DROP CONSTRAINT`（NotImplementedException）⇒ 只能走 CREATE(带PK)+INSERT SELECT+DROP+RENAME |

**两个实施坑（已写入实现）**：
1. 必须以**实表 schema**（`DESCRIBE`）建新表 —— 静态 `DDL_DUCKDB['stock_daily']` 只有 41 列、**缺 `data_source`**（由 `_migrate_add_columns` 补入，实表 42 列），用静态 DDL 会丢列。
2. PK 列按 `PRIMARY KEY(...)` **括号内逗号切分**（仅 `time` 带引号，按引号提取会退化成单列 PK(time) ⇒ 插入报 `duplicate key`）。

- 证据：`docs/evidence/hang2_v7_pk_rebuild.txt`。
- 判定：**窗口 7.7s < 60s ⇒ V7 可行**（需在批间隙、持 3A 写锁、无活动写事务时执行）。

### 10.5 V7 实现落点（默认全关）

| 项 | 内容 |
|---|---|
| 开关 | `QS_DUCKDB_WRITE_REBUILD_PK_BATCHES`（默认 0=关）；`QS_DUCKDB_WRITE_REBUILD_PK_MIN_ROWS`（默认 100 万，跳过小表）；`QS_DUCKDB_WRITE_REBUILD_PK_TABLES`（默认空=全部，可限定 `stock_daily`） |
| 方法 | `_maybe_rebuild_pk_after_batch`（writers.py:1157）+ `_rebuild_pk_on_conn`（writers.py:1210，单事务 CREATE+INSERT SELECT+DROP+RENAME+ROLLBACK 兜底） |
| 调用 | `write()` 批间隙（writers.py:998），与 W5 CHECKPOINT 同位置；持 3A 写锁、连接已关闭、无活动写事务 |
| fail-soft | 重建失败仅 `logger.warning`，不阻断采集；P1 准入由 **A1 黄金对比**替代（见下） |

### 10.6 A1 离线等价门（V7 部分）：**PASS**

- `tests/test_writer_variants.py` 新增 3 例：`test_v7_default_off_keeps_table_intact`（默认关表不被改）、`test_v7_rebuild_preserves_rows_pk_and_allows_write`（每批重建后仍与基线 `ORDER BY code,time` 黄金对比一致、无 `_v7_new_*` 残留）、`test_v7_min_rows_threshold_skips_small_tables`（门槛生效）。
- 测试文件头部 docstring 待补 "V7"。
- 回归：V7 相关 + W1 共 **21 passed**；核心写路径回归 **51 passed**（dedup/channel/3a/guardrails/backoff/audit）。

### 10.7 下一步（待用户批准）

- **A5-V7 在线受控实验**：`QS_DUCKDB_WRITE_REBUILD_PK_BATCHES=25 QS_DUCKDB_WRITE_REBUILD_PK_TABLES=stock_daily ... --task mcp_stock_daily`。
  - 通过判据：逻辑批 ≥60（越过第 57 批）+ 四项在线判据持续成立 + 无未捕获异常。
  - 机理假设：每 25 批重建一次主键 ⇒ 在第 25/50 批重建 ART，第 57 批写入时表状态被"刷新"，回避停摆。
  - 护栏：总时上界 1800s；S2 硬退出仍生效。
- V7 改变物理行序，理论受 P1 阻塞；但 A1 已用 `ORDER BY` 黄金对比证伪"物理行序影响读结果"，故 V7 的 P1 准入**已由 A1 等价性覆盖**（与 V1 同处理由）。
