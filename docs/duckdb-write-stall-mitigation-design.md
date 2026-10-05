# DuckDB 写入停摆（第二次事件）框架层根治设计 v1（六步流水线·第 1 步 方案）

> 状态：**待审计**（第 2 步）。本设计未审计通过前不得实施。
> 触发事件：`docs/evidence/duckdb-write-hang-incident2-20261002.md`（2026-10-02 第二次挂起）。
> 前序设计：`docs/duckdb-conflict-hang-mitigation-design.md`（v3.1）——本设计对其 **P1、P3 两条
> 前提作出证伪并给出新处置**，v3.1 转为历史文档（见 §8）。
> 取证脚本（随证据件入库，本设计全部结论可复算）：
> - `docs/evidence/hang2_log_forensics.py` → 三次运行逐批 new/updated 分布与批间耗时
> - `docs/evidence/hang2_shard_boundary_probe.py` → 生产库已落库日期边界（第 57 批主键归属）
> - `docs/evidence/hang2_repro.py` → 最小复现/变体对照实验台（自建临时库；原 `data/bench/*.db` 工作目录已于 2026-10-05 清理）
> - `docs/evidence/hang2_interrupt_probe.py` → 写事务 `conn.interrupt()` 可行性探针
> - `docs/evidence/hang2_write_size_stats.py` → 全量日志单批写入行数分布（预算定标）
>
> 原则：事实（Fact）/假设（Hypothesis）/待核实（Open）严格分列；未实证者不得写成结论。

---

## 修订记录

### v1 → v1.1（审计一轮：审计通过，附强制修订项）

| 编号 | 审计意见 | v1.1 处置 |
|---|---|---|
| 必修 1 | §5 门1 例数笔误（10 例 → **9 例**） | 已更正（§5 门1） |
| 必修 2 | §1.4 复现组描述与 CSV 不符；R-3 的 `threads=64` 无法从 CSV 独立验证 | §1.4 已按 CSV 段 + 运行日志头重写，并补"证据"列（新增归档 `hang2_repro_r1_seq.txt` / `hang2_repro_r2_upsert.txt` / `hang2_repro_r3_par64.txt`，提供 CSV 缺失的 `threads` 与起始 `table_rows`）；如实标注 R-1 起始表非空、R-3 才是空表 |
| 必修 3 | 落点路径笔误 `sources/mcp_adapter.py` | 已更正为 `quantstudio/pipeline/sources/mcp_adapter.py`（§4.0） |
| 澄清 4（W3） | 视图泄漏不止 `_tmp_write:920`，passthrough 的 `_pt_src:987` / `_pt_src_c:1088` 同款 | 实施范围扩大：`unregister` 一律移入 `finally`（`_tmp_write` / `_pt_src` / `_pt_src_c` 三处） |
| 澄清 5（A4-①） | 诊断 JSONL 必须 `flush + fsync` 后再 `os._exit(75)`，否则证据随进程死亡丢失 | 实施钉死：JSONL 以 `open(..., "a")` + `write` + `flush()` + `os.fsync()` 落盘，fsync 失败亦不阻断退出 |
| 澄清 6（A4-②） | 须明确 daemon 生产环境的进程拉起/监督机制，硬退出后采集停止，取舍需用户知悉 | 实施钉死：硬退出前 `logger.critical` 明示"进程将以 75 退出、采集停止、需外部拉起"；写入 JSONL `hard_abort=true` 字段；§7 风险表已列，**并在实施汇报中列为需用户确认项** |
| 澄清 7（A6） | `_inject_dividend` 用 `executemany`（非 `execute`），helper 须覆盖；且须区分 2274–2279 的"PK 缺失→回退纯 INSERT"内部 `except` 与"interrupt 停摆" | 实施钉死：helper 提供 `executemany` 入口；`DuckDBWriteStalled` 在内部 `except Exception` 之前**先行重抛**（不得被 PK 兜底分支吞掉），外层日志升级为 `error` 并带归因文本 |

---

## 0. 结论摘要（审计入口）

1. **已证实根因 R1（框架层，可修）**：写路径**完全没有任何有界等待**——`writers._write_locked`
   的 `SELECT COUNT` 与 `INSERT` 语句既无超时、无 `conn.interrupt()`、无看门狗。因此**任意**写侧
   病态都被放大为“永久静默挂起 + 持 3A 写锁 + 持 `.collector_run.lock` + 单核空转 + 零日志”，
   并阻塞全部采集任务。对照：回测**读**路径早就有同款机制
   （`quantstudio/backtest/providers/duckdb_data_access.py:233-288`），写路径是唯一裸奔通道。
2. **组件缺陷 R2（DuckDB 侧，未能本地复现，登记 `BLOCKED(外部依赖)`）**：生产现场
   `stock_daily` 第 57 批的写入语句进入不可返回状态。本机以生产同版本（duckdb 1.4.5）、
   同 schema（36 列 PK(code,time)）、同规模（2.75M 行）、同批大小（5 万行）、同两种写入形态
   （空表顺序追加 / 全量 upsert）、并额外做线程超配（64 线程）共 3 组复现实验，**均未复现**
   （详见 §1.4）。不得以“已定位 DuckDB 内部根因”表述；R2 只作为“为何会走到病态路径”的
   上游问题登记。
3. **v3.1 的 P1（写入分档：无冲突批降级纯 INSERT）前提被证伪**（§1.2 F2/F3）：第一次事件
   零冲突也挂、第二次事件 P1 生效（走纯 INSERT）也挂 ⇒ ON CONFLICT 与纯 INSERT **两条语句都会
   进入病态**，分档规避不成立。
4. **v3.1 的 P3（升级 duckdb 1.6.x）前提不成立**（§1.5 F6）：`pip index versions duckdb` 实测
   线上最新为 **1.5.6**，**不存在 1.6.x**；且 `duckdb_version_gate.REQUIRED_SERIES="1.4"`，
   升 1.5.x 会触发版本闸并与 `docs/case008-duckdb-version-mixing-unification-incident.md`
   的混装事故口径冲突。P3 撤销。
5. **修复主线 = 让“挂起”不再可能无声发生**（W1 双段看门狗：预算 → `interrupt` → 硬退出兜底），
   辅以 W2 分段耗时可观测、W3 视图泄漏修复、W4 熔断计数贯通、W5（可选、默认关）批间隙
   CHECKPOINT。**W1 的有效性不依赖 R2 的定位**，因此可以在 R2 未闭环的前提下交付。

---

## 1. 已证事实（Fact）

### 1.1 F1｜两次事件都卡在“第 57 批、累计 2,749,437 行”，但**冲突形态完全不同**

证据：`docs/evidence/hang2_log_forensics.py`（日志原文可复算）。

| 运行 | 日志 | 批次数 | 累计提交行 | 纯新增批 | 纯更新批 | 平均每批 | 结局 |
|---|---|---|---|---|---|---|---|
| 事故1 `00:17:35_b4d9b794` | `once_mcp_stock_daily_incremental_20261002_001735_*.log` | 56 | 2,749,437 | **56** | 0 | 8.7s | 第 57 批永不返回 |
| 事故2 `14:50:50_c3fc5673` | `once_mcp_stock_daily_incremental_20261002_145050_*.log` | 56 | 2,749,437 | 0 | **56** | 15.3s | 第 57 批永不返回 |

判读：

- 事故1 运行时目标表为空（每批“新增=全部、更新=0”）⇒ **零冲突**场景下 `ON CONFLICT` 语句
  同样挂 ⇒ 病态**不依赖冲突发生**（v3.1 假设 A“触发路径为冲突检查/更新维护”被削弱）。
- 事故2 每批“新增=0、更新=全部”⇒ 主键 100% 重叠 ⇒ P1 分档**全程未生效**（每批 `updated_rows>0`）。
- 两次的每批耗时**稳定**（TOP5 批间间隔 10–16s，无劣化趋势）⇒ 不是随表增长渐进劣化，
  而是**离散进入不可返回状态**。

### 1.2 F2｜生产库边界证明：第 57 批主键为**新键** ⇒ 事故2 走的是 P1 的**纯 INSERT** 分支

证据：`docs/evidence/hang2_shard_boundary_probe.py` → `docs/evidence/hang2_shard_boundary_probe.txt`
（只读生产库实测）。

```
table=stock_daily rows=2749437 distinct_codes=5073
min_time=2020-01-02  max_time=2022-08-10   distinct_days=632
尾部：2022-08-08 rows=4870 / 2022-08-09 rows=4869 / 2022-08-10 rows=2652   <- 末日仅半日
```

- 56 批 × 5 万行 = 2,749,437 行，与日志“累计提交行”逐位一致 ⇒ 挂起发生在**第 57 批**（该批未提交）。
- 末日 2022-08-10 只有 2,652 行（同市场量级 4,869 行/日）⇒ 分片在日内被行数切断 ⇒
  **第 57 批携带的是尚未落库的新键**（2022-08-10 余下部分 + 其后交易日）。
- 事故2 已部署 P1（提交 `77405b8`，13:42），`updated_rows == 0` ⇒ 走 `writers.py:915` **纯 INSERT**。

⇒ **P1 生效了仍然挂起**。同时这条证据**推翻**事故2 报告 §5.4 的结论
（“实际挂起语句 = 903 ON CONFLICT”）：字节码可达性能排除 919（else 裸 INSERT 死代码），
但排除不了 915；而分片边界证据把范围收窄到“新键批”。两种行号归因（903 +16 / 915 +4）都在
事故2 报告自测的 py-spy 误差带（+12 / −22）内，**无法凭 py-spy 行号定语句**。
这正是 W2（分段耗时落日志）必须做的原因：下次不必再靠 py-spy 猜语句。

### 1.3 F3｜P1（写入分档）作为“挂起规避”无效，且其分档判据是**全表顺序扫描**

- F1 已证：零冲突（事故1）与新键纯 INSERT（事故2）两种路径都会挂。
- `writers.py:890-892` 的 `SELECT COUNT(*) ... WHERE (pk) IN (SELECT (pk) FROM _tmp_write)`
  经既有 EXPLAIN 实证为 `SEQ_SCAN + HASH_JOIN SEMI`（无 INDEX_SCAN），**每批全表扫描**，
  成本随表增长线性上升（v3.1 §3.0 已记录，本设计沿用该结论，不重复优化）。

### 1.4 F4｜本地复现尝试全部为**负结果**（3 组，duckdb 1.4.5 同版本）

证据：`docs/evidence/hang2_repro.py` + `docs/evidence/hang2_repro_timings.csv`（逐批计时）
+ 三组运行日志头（`hang2_repro_r1_seq.txt` / `hang2_repro_r2_upsert.txt` / `hang2_repro_r3_par64.txt`，
提供 CSV 未记录的 `threads` 与起始 `table_rows`）。
CSV 无 `threads` 列，故 R-3 的并行度以运行日志头为准（可复算）。

| 组 | 形态（起始表行数 / 并行度） | 规模 | 每批耗时 | 结果 | 证据 |
|---|---|---|---|---|---|
| R-1 | 起始 981,200 行（days 0–200 已被批量装载） / threads=16 | 65 批 → 3,188,900 行 | 前 20 批**全量 upsert** 7.4–8.3s；后 45 批**新键** 0.8–1.1s | **无挂起** | `hang2_repro_r1_seq.txt`（CSV 22:12–22:16 段） |
| R-2 | 起始 2,752,266 行（3 块批量装载） / threads=16 | 60 批 → 2,943,600 行 | 前 56 批**全量 upsert** 4.7–5.9s；后 4 批**新键** 0.7–1.4s | **无挂起** | `hang2_repro_r2_upsert.txt`（CSV 21:48–21:54 段） |
| R-3 | 起始 **0 行（空表）** / **threads=64（超配，模拟生产约 40 核/80 线程）** | 65 批 → 3,188,900 行 | 全新增批 0.55–1.37s | **无挂起** | `hang2_repro_r3_par64.txt`（CSV 22:16–22:20 段） |

本机 16 逻辑核 / 8 物理核，`duckdb current_setting('threads')=16`；生产现场进程 80 线程。
已覆盖的形态：空表顺序新增（R-3）、小表 upsert（R-1）、2.75M 行表全量 upsert（R-2）、线程超配（R-3）。
**未覆盖**：真实数据形态（真实 NaN/文本列分布）与生产进程内存占用（1.07GB vs 干净进程），
二者仍是未消解的环境差异，R2 保持 `Hypothesis` 状态（§8 O1/O2）。

### 1.5 F5｜`conn.interrupt()` 在 duckdb 1.4.5 **写事务**上确实可用（W1 的可行性已实证）

证据：`docs/evidence/hang2_interrupt_probe.py` → `data/logs/_interrupt_probe.txt`。

```
interrupt() called, returned in 0.000s
outcome: exc:_duckdb.InterruptException  err: INTERRUPT Error: Interrupted!
rows after interrupt: 0            <- 事务已回滚，无部分写入
reusable SELECT 1: 1                <- 连接打断后仍可复用
```

**同时测得一个必须写进设计的事实**：interrupt **不是即时生效**——预算 3.0s 到点发出 interrupt，
异常在 t=14.81s 才抛出 ⇒ **生效延迟约 11.8s**（DuckDB 仅在执行循环的检查点轮询中断标志）。
W1 的第二段宽限期必须显著大于 12s（取默认 300s）。

### 1.6 F6｜P3（升级 1.6.x）前提不成立

```
duckdb (1.5.6)   Available versions: 1.5.6 ... 1.4.5, 1.3.2 ...
INSTALLED: 1.4.5   LATEST: 1.5.6
```

- **不存在 1.6.x**；v3.1 §3.3 与 §1.1 的“官方修复在 1.6.x”前提无法成立。
- `quantstudio/pipeline/duckdb_version_gate.py:30` `REQUIRED_SERIES="1.4"`，升 1.5.x 会被版本闸
  `SystemExit(3)` 拒绝（逃生阀 `QS_DUCKDB_VERSION_GATE=0`），并与 case008 版本统一口径冲突。

### 1.7 F7｜写路径是全仓**唯一**没有有界等待的 DuckDB 通道（全面排查结论）

全面排查（`quantstudio/pipeline/`、`quantstudio/backtest/`、`scripts/`、`main_gui.py`）结论：

| 通道 | 现状 |
|---|---|
| 回测读（`duckdb_data_access.py:233-288`） | 有 per-statement 预算 + `conn.interrupt()` + 事件落账 |
| GUI 读（`gui/db_helper.py:41,167-207`） | 有 5s deadline |
| CHECKPOINT（`db_checkpoint.py:64-113`） | 工作线程 + `join(timeout)` + 超时放弃 |
| 写锁自愈（`snapshot_lock.py`） | 有 stale 判定（只管锁文件，管不到 DuckDB 内部空转） |
| **写路径（`writers.py:890/903/915/919`、`_advance_watermark*`、`_upsert_pending_backfill_on_conn`）** | **无超时、无 interrupt、无看门狗、无分段耗时** |
| `mcp_adapter._inject_dividend`（主库直连 upsert） | 同上；且异常仅 `logger.warning`（fail-soft） |
| `writers._conn()`（RW open） | 只对**抛异常**退避 30s；若 `connect()` **阻塞**（09-23 案已记录该形态）则退避永不生效 → 无界等待 |
| `conn.unregister("_tmp_write")`（`writers.py:920`） | 位于 `try` 内，异常时不执行 → 视图泄漏 |

单批写入行数上界（`docs/evidence/hang2_write_size_stats.txt`，全量日志统计）：
`stock_daily_valuation / stock_daily / etf_daily` 的 `max = 50000`，其余表 ≤ 5,242；
健康单批耗时实测 8.7–16s。这是 W1 预算定标基础。

### 1.8 F8｜热路径无 CHECKPOINT，upsert 的删除版本在库内累积

- 唯一生产 CHECKPOINT 调用点在 daemon 轮次收尾（`daemon.py:260-262`）。
- 实测：R-2 复现库 60 批 upsert 期间由 0.76GB 涨到 1.51GB（表行数几乎不变）；
  生产事故2 期间主库由 1.61GB 涨到 2.16GB（1,614MB 取自事故1 报告，2,158,243,840B 取自事故2 报告）。
- 该膨胀与 `docs/case007-gui-startup-hang-wal-replay-incident.md` 的 WAL 回放 22 分钟事故同源。

---

## 2. 根因判定

| 编号 | 判定 | 状态 |
|---|---|---|
| **R1** | **写路径无界等待**：`conn.execute` 无任何上界，使“单条语句进入病态”被放大为**永久静默挂起 + 全局阻塞**。这是本次事故**可归因于本项目**的根因。 | **已证实**（F7 全面排查 + F1 现场） |
| **R2** | **DuckDB 1.4.5 组件缺陷**：某条 INSERT/ON CONFLICT 语句在约 2.75M 行规模 + 5 万行批下进入不可返回状态（现场 native 栈指向 `Transaction::~Transaction` / `BufferManager::GetBufferManager`）。 | **假设**（F1/F2 现场成立，但 3 组复现均未复现 ⇒ 机理未证实） |
| **R3-a** | v3.1 **P1（无冲突批降级纯 INSERT）** 可规避挂起 | **已证伪**（F1/F2） |
| **R3-b** | v3.1 **P3（升级 duckdb 1.6.x）** 可兜底 | **已证伪**（F6：不存在 1.6.x） |
| **R4** | 事故2 挂起语句 = `writers.py:903` ON CONFLICT | **已推翻**（F2：第 57 批为新键 ⇒ 应为 915 纯 INSERT；py-spy 行号在 f-string 调用点不可信） |

**因果链（对齐现场）**：R2（组件病态，机理未知）触发单条语句不可返回 → R1（框架无上界）使该
语句永久占用 CPU/写锁/采集锁且零日志 → 表现为“静默挂起、单核满载、DB 零增长、阻塞全部采集”。

**可交付的根治 = 消除 R1**（把不可返回语句变成“有界失败 + 明确诊断 + 锁释放”），
R2 登记为上游技术债，不作为交付前提。

---

## 3. 触发条件与受影响范围

**触发条件（现场已证部分）**

- duckdb 1.4.5（生产现场版本；本机同版本）。
- 任务 `mcp_stock_daily`，`INCREMENTAL` 但 `last_watermark=None` ⇒ 实为 2020-01-01→2026-10-02
  **全量回填**；MCP 流式导出 7 批 → 165 分片。
- 主表 `stock_daily`（PK(code,time)，36 列）已累计提交 **2,749,437 行**后，下一批写入语句
  进入不可返回状态；两次事件同点，具确定性。
- 无冲突（事故1）与新键纯 INSERT（事故2）**均可**触发 ⇒ 与冲突是否存在无关。

**受影响范围**

- 直接：`writers.DuckDBWriter._write_locked` ⇒ 所有走该方法的表/任务
  （`stock_daily`、`stock_daily_valuation`、`etf_daily`、`index_daily`、财务/行业/成分等 20+ 表）。
- 同源未加固通道：`_advance_watermark_locked` / `_advance_watermark_on_conn` /
  `_upsert_pending_backfill_on_conn`（writers.py）、`mcp_adapter._inject_dividend`、
  `index_constituents_meta.refresh_snapshot_meta`、`update_detector.save_last_sync`。
- 连带影响：持 `.collector_run.lock` ⇒ 阻塞同类采集；`--quality-audit full` 与水位推进不执行；
  强杀留 WAL 恢复负担（09-23 案：2.17GB WAL 回放 22.3 分钟）。
- 范围外：`stock_daily_valuation`（事故2 中已完成 8,144,924 行）、`stock_basic`/`etf_basic` 等。

---

## 4. 改动设计

### 4.0 改动面总览（最小化 + 通用 + 纯增益）

| 编号 | 内容 | 落点 | 性质 |
|---|---|---|---|
| **W1** | 写语句**双段看门狗**（预算 → `conn.interrupt()` → 硬退出兜底）+ 诊断落盘 | `writers.py`（新 helper + 包裹既有 `conn.execute`） | 稳定性根治 |
| **W2** | 写路径**分段耗时**落日志（count / dml / close），慢批与停摆可直接定位语句 | `writers.py` | 可观测 |
| **W3** | `conn.unregister` 移入 `finally`（消除异常路径视图泄漏） | `writers.py:920` | 纯增益 |
| **W4** | 熔断/告警计数贯通到停摆诊断（同一份 JSONL 落账） | `writers.py` | 可观测 |
| **W5** | 批间隙**可选** CHECKPOINT（默认关，`QS_DUCKDB_WRITE_CHECKPOINT_BATCHES` 开启） | `writers.py` | 保真开关（默认 off） |
| **W6** | `mcp_adapter._inject_dividend` 同款看门狗 + 失败不再仅 warning | `quantstudio/pipeline/sources/mcp_adapter.py` | 加固 |

**零改动承诺**：`write()` 签名与返回类型、`WriteResult` 三字段 int 契约与守恒、DDL、字段契约、
列顺序、dtype、空值行为、异常类型（非停摆场景）、`pk_cols` 分档语义、水位推进语义、锁语义
（3A 写锁 + `_conn_lock` 边界）、daemon 调用链、任何策略源码。

### 4.1 W1 写语句双段看门狗（核心）

**机制**（与回测读路径 `duckdb_data_access.py:233-288` 同款 interrupt 机制，但**保持 execute 在
调用线程**、仅由看门狗线程调用 `conn.interrupt()`，避免跨线程 `conn.register` 语义变化）：

```
看门狗线程（threading.Timer，仅到点发 interrupt，不执行 SQL）
   |
主线程： conn.execute(<写语句>)  <- 语义与今天完全一致
   |
   +- 预算内返回 ----------------> 看门狗取消，零行为变化
   +- 超预算（QS_DUCKDB_WRITE_TIMEOUT_S，默认 600s）
        |- 立即 logger.critical（表/批/行数/SQL 头/已耗时）-> 运维当场可见
        |- conn.interrupt()（F5 已实证：抛 InterruptException、事务回滚、连接可复用）
        |- 宽限期 QS_DUCKDB_WRITE_HARD_ABORT_S（默认 300s，> interrupt 实测延迟 11.8s 的 25 倍）
        |     |- 语句在宽限期内返回/抛错 -> 抛 DuckDBWriteStalled（任务失败、锁释放、水位不动）
        |     +- 仍不返回 -> 写诊断 JSONL -> os._exit(75)（EX_TEMPFAIL）
        +- 无论哪条分支，硬退出定时器已在停摆检测时刻武装 => 清理/close 亦无界时仍可收敛
```

**参数与默认值（全部可用环境变量覆盖，禁硬编码）**

| 环境变量 | 默认 | 语义 |
|---|---|---|
| `QS_DUCKDB_WRITE_TIMEOUT_S` | `600` | 单条写语句预算（`0`/负 = 关闭 S1）。定标：健康上界 16s × 37.5 |
| `QS_DUCKDB_WRITE_HARD_ABORT_S` | `300` | 停摆后到硬退出的宽限（`0` = 只抛错不硬退） |
| `QS_DUCKDB_WRITE_STALL_JSONL` | `data/logs/duckdb_write_stall.jsonl` | 诊断落盘路径 |

**为何选 600s**：健康单批实测 8.7–16s（全量日志 20+ 表最大单批 5 万行）；600s 约为上界 37 倍，
既不误伤健康写（含 passthrough 分片写与 QFQ 重锚大事务），又把事故现场的“2h38m 无进展”
压缩为“≤15 分钟内确定性失败或退出”。

**硬退出的取舍（如实标注）**

- 代价：`os._exit` 不走 `conn.close()` ⇒ 可能留 WAL，下次开库需回放（09-23 案 2.17GB ≈ 22.3 分钟）。
  这与事故现状（人工强杀）**代价相同**，但变为自动、带诊断、可预期。
- 收益：释放 `.collector_run.lock` 与库锁，阻断“单核空转 + 全局阻塞”，并留下 JSONL 证据。
- 可关：`QS_DUCKDB_WRITE_HARD_ABORT_S=0` ⇒ 退化为“抛 `DuckDBWriteStalled` + writer 毒化
  （后续写快速失败）”，把是否退出交给调用方（GUI/测试环境可用）。
- 触发条件极苛刻（须先真的停摆 >600s 且 interrupt 无效），不会误伤健康路径。

**新异常类型**：`class DuckDBWriteStalled(RuntimeError)`，消息含四要素：
`table / batch_id / rows / 已耗时 / 语句头 / 是否已发 interrupt / 硬退出是否启用`。

- 不继承 `duckdb.Error`，避免与库异常混淆；调用方（daemon `_run_with_source`）按既有
  `except Exception` 路径处理 ⇒ **任务失败、锁释放、水位不推进**，下轮从断点续跑。

**包裹范围**（同一 helper，复用）

1. `_write_locked`：`SELECT COUNT`（890）、ON CONFLICT（903）、纯 INSERT（915）、else 裸 INSERT（919）。
2. `_advance_watermark_locked`（1150）、`_advance_watermark_on_conn`（1182）、
   `_upsert_pending_backfill_on_conn`（1324）。
3. `_write_passthrough`（986）与 `write_passthrough_chunked`（1087/1091/1106-1107）——
   类别 B 覆盖写同属写路径，一并纳入以免“唯一裸奔通道”变成“两条裸奔通道”。
4. `mcp_adapter._inject_dividend`（W6）：`executemany` 逐行 upsert 同样无上界。

**不变性论证**

- 正常路径：看门狗只是“到点调 `conn.interrupt()` 的 Timer”，预算内完成时 Timer 被取消，
  SQL、事务边界、连接生命周期、返回类型**逐位不变**；额外开销 = 1 个 Timer 对象/语句。
- 停摆路径：新增失败模式（原本是永久挂起），属**纯增益**——不可能更差。
- 不触碰任何查询语义、列映射、水位推进、锁粒度、异常类型（非停摆）。

### 4.2 W2 分段耗时（可观测，直击下次取证成本）

在 `_write_locked` 内按段计时并落 INFO（仅慢批升 WARNING，看门狗停摆时随诊断落 JSONL）：
`count_s`（写前去重计数）/ `dml_s`（INSERT/ON CONFLICT/DELETE）/ `close_s`（提交+关闭），
并把 `sql_head`（压缩空白后前 120 字符）与 `rows` 一起记录。
下次事故可直接从日志回答“哪条语句、多大表、多少行、停在哪一段”，**不再依赖 py-spy 行号**
（F2 已证明 py-spy 在 f-string 调用点不可信）。

### 4.3 W3 视图泄漏

`conn.unregister("_tmp_write")` 从 `try` 体内移入 `finally`（与 `conn.close()` 同层），
保证任何异常/停摆路径都不残留 pandas 视图。**零行为变化**（正常路径顺序仍为 unregister→close）。

### 4.4 W4 熔断计数贯通

v3.1 的 fail-closed 滑动窗口/熔断（`writers.py:712-746`）保留；其计数器在停摆诊断 JSONL 中
一并落账（`_dedup_circuit_open`、窗口命中数），便于把“分档失效”与“停摆”放在同一时间线分析。
**不新建 quality_audit 注入点**（沿用 v3.1 §3.1.1 P2-3 决策：writers.py 无注入点，不扩面）。

### 4.5 W5 批间隙 CHECKPOINT（可选，默认关）

- 触发：`QS_DUCKDB_WRITE_CHECKPOINT_BATCHES=N`（默认 0=关）；每 N 批在**写锁内、连接已关闭后**
  调用既有 `db_checkpoint.checkpoint_database(db_path, timeout_s=120)`（其自身为工作线程 +
  超时放弃，不阻塞）。
- 作用（F8 支撑）：收敛删除版本与 WAL 体积，降低 BM/文件膨胀，顺带缓解 09-23 案的
  “开库回放 22 分钟”。
- **默认关的理由**：其“防挂起”效益**未证实**（R2 机理未知），而每次 CHECKPOINT 有实测成本
  （2.33GB WAL ≈ 7.3s）；不得以未证实收益默认改变运行时行为。
- 落点约束：只在批次间隙执行，绝不在持有活动写事务时执行；失败仅告警不阻断（沿用
  `checkpoint_database` 的 fail-soft 契约）。

### 4.6 W6 另一条主库写通道加固

`mcp_adapter._inject_dividend`（2244-2286）：同一看门狗 helper + 把
`logger.warning("[MCPAdapter] stock_dividend 写入失败: ...")` 升级为 `logger.error` 并附
“该路径不经 writer 通道”归因文本。异常行为不变（仍不向上抛），仅日志级别与文本增强——
需审计确认此为“可观测性增强”而非“异常行为变更”。

---

## 5. 验收门（可执行判据）

| 门 | 内容 | 判据 |
|---|---|---|
| **门1** | 分档语义等价 | 既有 `tests/test_writer_dedup_fail_closed.py`（**9 例**）**全绿**：三态分档、fail-closed 守恒、熔断、复位、窗口滑动 |
| **门2** | 看门狗功能 | 新增单测：注入“永不返回”的 `conn.execute`（monkeypatch）⇒ ① 在预算+宽限内抛出 `DuckDBWriteStalled`；② 异常消息含 table/batch_id/rows/SQL 头四要素；③ 写锁与 `_conn_lock` 均已释放（后续写可继续）；④ 诊断 JSONL 已追加一行且字段齐全；⑤ `HARD_ABORT=0` 时不调用 `os._exit` |
| **门3** | 看门狗不误伤 | 真实小数据写入 200 批（含 0 行批、列子集批、fail-closed 批）全部正常完成，耗时与改动前同量级；无任何 `DuckDBWriteStalled` |
| **门4** | 真实 interrupt 通路 | `docs/evidence/hang2_interrupt_probe.py` 在改动后环境复跑：仍 `InterruptException` + 行数 0 + 连接可复用；并新增“对真实写事务注入超时”的人工验证记录（预算临时调小到 2s） |
| **门5** | 既有功能回归 | `tests/test_writers_rw_backoff.py`、`test_writer_channel_contract.py`、`test_3a_equivalence.py`、`test_pipeline_guardrails.py`、`test_db_checkpoint.py`、`test_snapshot_lock.py`、`test_full_quality_audit_repair.py`、财务/分红/行业/指数相关写路径套件**全绿**，精确失败清单不变 |
| **门6** | 端到端拉取 | `mcp_stock_daily` 真实任务跑完 ≥ 60 批（覆盖原事故的第 57 批边界），日志持续推进、DB 字节持续增长、CPU 无单核满载空转、水位正常推进；`--quality-audit full` 收尾通过 |
| **门7** | 停摆注入演练 | `docs/evidence/hang2_stall_drill.py` → `hang2_stall_drill.txt`。**场景A（语句响应 interrupt）**：有界收敛 1.7s（预算 1.0s）、抛 `DuckDBWriteStalled`、停摆批未提交（表内仍 2 行）、锁释放、恢复后幂等续跑 ⇒ PASS。**场景B（语句**不响应** interrupt，即本次演练自身曾卡死的形态）**：S2 在 3s 后触发 `os._exit(75)`，收敛 4.2s，诊断 `hard_abort=true` ⇒ PASS。两场景共同证明：只要 **S2>0**，任何形态的停摆都有上界 |
| **门8** | 文档同步 | README + `docs/data-pipeline-contract.md` + 本设计 + 证据文档同步更新（铁律要求） |

**门6 为何必须真实跑**：本设计已证“本机复现不出 R2”（F4），因此门6 只能证明“未退化”，
不能证明“已根治”。这一限制必须如实写入验收结论。

---

## 6. 回退条件

- 门1/门3/门5 任一失败 → 立即回退 W1（单文件、单 helper，回退面小）。
- 健康批出现 `DuckDBWriteStalled`（误伤）→ 回退或上调 `QS_DUCKDB_WRITE_TIMEOUT_S`。
- 硬退出在非停摆场景被触发 ⇒ 视为不可接受缺陷，立即回退 W1 并重新定标预算。
- W5 开启后出现锁表/并发写入破坏 → 立即关闭该开关（默认即关，无回退压力）。
- W6 日志级别变更若被判为“异常行为变更” → 回退 W6（仅保留看门狗）。
- 实施前铁律：`git stash create -u` + `git stash store` 持久化回退点；`writers.py` 为共享核心
  文件，每次 edit 后即时 `git diff <file>` 自检；提交用精确文件清单，禁 `git add -A`。

---

## 7. 风险与副作用（如实标注）

| 风险 | 评估 | 缓解 |
|---|---|---|
| 硬退出留 WAL | 与事故现状（人工强杀）同代价 | 诊断先行；下轮开库有回放成本（已记录于运维文档） |
| 预算误伤超大单批 | 历史最大单批 5 万行/16s；passthrough 分片与 QFQ 大事务可能更大 | 预算可配；门3 覆盖大单批；异常时上调而非改语义 |
| 停摆被 interrupt 打断后连接状态 | F5 已实证可复用、回滚干净 | 仍按“停摆即毒化”处理：不再复用该连接 |
| interrupt 对本病态无效 | 未证实（现场 native 栈含 `CheckPulse` 轮询点，倾向有效，但无实证）；**但门7 场景B 已实证：语句完全不响应 interrupt 时，S2 仍能在预算+300s 内以 75 退出** | 双段设计：无效则由硬退出兜底，仍有界 |
| **`QS_DUCKDB_WRITE_HARD_ABORT_S=0` 的副作用** | **实施期实测（2026-10-02 23:29 演练自身卡死）**：关闭 S2 且被守护语句不响应 interrupt ⇒ 有界等待退化为**无限空转**（与事故同形态），只能人工杀进程 | **生产不得关闭 S2**（默认 300s）；逃生阀仅用于 GUI/测试等不接受进程退出的场景，且须接受"可能无限空转"的后果 |
| 演练/替身不响应中断导致"演练自己挂起" | 已实证并修正（替身改为"收到 interrupt 后抛 InterruptException" + 演练自身硬截止 30s） | 任何注入式演练都必须自带硬截止，否则证伪的是演练而非被测对象 |
| 本机无法复现 R2 | F4 已证 | 门6 只证明未退化；R2 独立登记为上游技术债 |

---

## 8. 待核实与技术债（Open items）

| 编号 | 内容 | 状态 | 解除条件 |
|---|---|---|---|
| O1 | R2 机理（DuckDB 1.4.5 具体缺陷） | `BLOCKED(外部依赖)` | 能在多核环境复现（需 ≥32 核机器 + 真实数据形态），或上游 issue/修复版本落地 |
| O2 | 事故2 挂起语句究竟是 903 还是 915 | **已由 F2 收敛为“新键批（应为 915）”，但缺停摆瞬间的直接证据** | W2 上线后任一次停摆即可精确回答 |
| O3 | `docs/evidence/duckdb-write-hang-incident2-20261002.md` §5.4 结论（“实际=903 ON CONFLICT”） | **与 F2 冲突，需回写更正** | 随本设计审计通过后同步修订该证据文档 |
| O4 | `docs/evidence/duckdb-conflict-hang-forensics-20261002.md` §3 的“`f_lineno` 前移”机制 | 已被事故2 报告推翻（py-spy f-string 归因错误），需统一表述 | 同 O3 |
| O5 | quality_audit 计数挂接（v3.1 P2-3 遗留） | 沿用“不扩面”决策 | 单独立项 |
| O6 | duckdb 1.5.x 升级评估 | **前置已变**（无 1.6.x）；1.5.6 是否含修复**未知** | 需在多核环境对 1.5.6 跑同一复现实验 |

---

## 9. 与既有文档/铁律的对账

- 与「框架层改动六步流水线」：本文件为第 1 步方案；审计通过后方可实施（第 3 步），
  实施后按 §5 验收（第 4 步），用户确认后双仓库推送（第 6 步）。
- 与「性能优化不得改变引擎行为」：本设计是**稳定性/正确性**修复，非性能优化；
  W5 默认关（不改变默认运行时行为），W1 不改任何查询语义。
- 与「策略生成与转换全链路修复仅限框架层」：落点仅 `writers.py` + `sources/mcp_adapter.py`，
  策略源码零改动。
- 与「框架问题立即解决」：R1 已证实且可修，本轮即推进六步流水线，不登记挂账；
  R2 因机理未证实且需外部环境，登记 `BLOCKED(外部依赖)`（唯一允许的挂账例外）。
- 与 `docs/duckdb-lock-timeout-design.md` / `docs/case007-*` / `docs/case008-*`：边界独立
  （那三者分别是跨进程锁、WAL 回放、版本混装），本设计只管“单条写语句无上界”。

---

## 9.1 门6 真实拉取实证（2026-10-03 00:21–01:43，PID 26480）

**执行**：`python -m quantstudio.pipeline.daemon --mode once --pull-mode incremental
--task mcp_stock_daily --config-dir config/profiles/mcp_only --quality-audit full`
（`stock_daily` 水位为空 ⇒ `INCREMENTAL: 2020-01-01 → 2026-10-03`，与事故同一路径）。
证据：`docs/evidence/hang2_gate6_run_log.txt`、`docs/evidence/hang2_gate6_stall_jsonl.txt`。

**时间线（实测）**

| 时刻 | 事件 |
|---|---|
| 00:21:45 | 启动；先跑依赖表 `stock_daily_valuation`（7 批导出 / 166 个 5 万行写批，8,144,924 行） |
| 01:10:43 | `adj_factor` 冷启动注入（逐 5 万行写 qfq_aux.db） |
| 01:11–01:28 | `stock_daily` 写批 1–56 全部成功，`[timing count=0.2s dml=5.1s close=5.4s]`，ERROR=0 |
| **01:28:27** | **第 57 批（rows=50000, phase=dml）语句开始 ⇒ 永不返回** |
| 01:38:27 | **S1 预算 600s 到点**：`logger.critical` + `conn.interrupt()`（诊断 `interrupt_sent=true`） |
| 01:38:27–01:43:27 | interrupt **未生效**（CPU 30s 窗口实测 29.6s = 单核满载，日志/DB 零增长） |
| **01:43:27** | **S2 宽限 300s 耗尽**：诊断 JSONL 落盘（`hard_abort=true`, `execute_elapsed_s=900.005`）→ `os._exit(75)` |
| 01:47 复查 | 进程已退出；**无 `.wal` 残留**（下次开库无回放负担）；`.write_lock` 陈旧可自愈回收 |

**由此确立/修正的结论**

| 编号 | 结论 | 状态变化 |
|---|---|---|
| **R2** | DuckDB 1.4.5 组件缺陷**在生产现场已复现**（非仅假设）：三次事件同一边界（第 56 批写完、第 57 批停摆，表内 ≈2.75M 行），单核满载、零产出 | **假设 → 已复现** |
| **R1 修复有效** | W1 把“无限静默挂起”变成 **900 秒（600+300）内确定性收敛**：有 CRITICAL 日志、有诊断 JSONL、锁已释放、无需人工强杀 | **已实证** |
| **N-NEW** | **interrupt 对本病态无效**（S1 发中断后 300s 仍不返回）——F5 的 interrupt 有效性只对正常长查询成立 | **已实证 ⇒ S2 是必需项，生产禁止 `QS_DUCKDB_WRITE_HARD_ABORT_S=0`** |
| **N-NEW** | 生产库**副本**上单批写入（on_conflict / delete_insert）均 1.6–1.7s 完成 ⇒ 停摆**不是单纯表规模函数**，而依赖**进程内累积状态**（同进程已写 166+56 批、持 QFQ 快照/分片缓存/多连接） | **新假设 H5′**，需下一段取证 |
| **门6 判定** | 门6 原文判据“跑完 ≥60 批覆盖第 57 批边界”**未通过**：本次止步 56 批（与事故同点） | **未通过** |

**门6 未通过的后果（必须向用户明示）**：由于停摆在同一批次确定性复现，**任何重跑都会在同一点失败**，
`mcp_stock_daily` 全量回填在现有写入策略下**不可能完成**。W1 只把故障形态从“静默卡死”降为
“15 分钟内有界失败+可续跑”，**不能让数据拉完** ⇒ 必须追加 L0（写入策略根治），见 §9.2。

### 9.2 L0 候选（下一轮方案需覆盖，待审计）

1. **H5′ 取证**：在生产库副本上于**单进程内连续写 70+ 批**（复现同进程累积态），确认停摆可廉价复现；
2. **写入策略替代**：全部批次改 `DELETE 重叠键 + 纯 INSERT`（避开 ON CONFLICT 算子）/ 临时表+merge；
3. **分片维度变更**：按 `code` 连续分片替代按日期分片（改善 ART 插入局部性）；
4. **并行度/保序**：`SET threads` 上限、`preserve_insertion_order=false` 实测；
5. **DuckDB 1.5.6 评估**（O6）：在可廉价复现的前提下对 1.5.6 跑同一实验。

---

## 10. 审计确认项（请逐条勾选并给出结论）

| # | 待确认决策 | 方案立场 | 审计要点 |
|---|---|---|---|
| A1 | 是否接受“根因分层：R1 框架无上界（已证实，可修）+ R2 组件缺陷（假设，外部依赖）”，而非单一根因 | 接受 | R1 证据是否充分（F7 全面排查表）；R2 标注为假设是否诚实 |
| A2 | P1（写入分档）定位由“挂起规避”降级为“性能分档”，**代码保留不删** | 保留 | 保留是否会掩盖问题；是否要求同时删除以消除误认 |
| A3 | P3（升级 duckdb 1.6.x）撤销 | 撤销 | F6 证据（pip 无 1.6.x）是否足以撤销；是否要求改立“评估 1.5.6” |
| A4 | 硬退出 `os._exit(75)` 作为看门狗第二段兜底 | 采纳 | 是否可接受（库内退出进程）；是否要求改为只抛错 + 由 daemon 决定退出 |
| A5 | 默认预算 `QS_DUCKDB_WRITE_TIMEOUT_S=600` / 宽限 300s | 采纳 | 定标是否充分（健康上界 16s、历史最大单批 5 万行、interrupt 延迟 11.8s） |
| A6 | 包裹范围含 passthrough 覆盖写与 `mcp_adapter._inject_dividend`（W6） | 纳入 | 改动面是否过大；`logger.warning→error` 是否算“异常行为变更” |
| A7 | W5（批间隙 CHECKPOINT）默认关、仅环境变量开启 | 默认关 | 默认关是否会留下 F8 膨胀问题未解；是否要求默认开 |
| A8 | 门6 只能证明“未退化”不能证明“已根治”，如实标注 | 接受 | 是否需追加多核环境复现作为交付前置条件 |
| A9 | 证据文档 O3/O4（事故2 §5.4、取证报告 §3）需回写更正 | 需更正 | 更正范围与措辞是否可接受 |
| A10 | R2 登记 `BLOCKED(外部依赖)` 而非继续本地穷举复现 | 登记 | 是否接受“机理未证实即交付 W1”的工程取舍 |
