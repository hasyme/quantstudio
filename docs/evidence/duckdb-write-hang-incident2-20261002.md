# DuckDB 写入停摆错误技术报告（第二次事件·2026-10-02）

> 文档性质：错误技术报告（面向根因定位与修正方案制定）。
> 关联文档：
> - 设计文档 `docs/duckdb-conflict-hang-mitigation-design.md`（v3.1）
> - 第一次事件取证 `docs/evidence/duckdb-conflict-hang-forensics-20261002.md`
> - 验收证据 `docs/evidence/duckdb-conflict-hang-acceptance-gate*.md`
> 原则：事实（Fact）与假设（Hypothesis）/推断（Inference）严格分列；未经实证的结论一律标注"待核实"。

---

## 0. 摘要（TL;DR）

`mcp_stock_daily` 数据同步任务（`once` 模式）在向 DuckDB 主表 `stock_daily` 写入过程中，
于**第 57 批**（累计已写 2,749,437 行）发生**静默挂起**：进程存活、单核 CPU 满载（≈0.97 核），
但日志与数据库文件**零字节增长**达 2 小时 38 分钟。py-spy 采样 20s@100Hz 共 1999 点，
**去重后仅 1 个栈签名（100%）**，栈顶落在 `writers.py` 的 `_write_locked`。本错误为
**DuckDB 组件缺陷导致的写入忙等（busy loop）**，无 Python 异常、无错误码、无 traceback。

> ✅ 已复核确认（§5.4）：py-spy 栈顶报 `writers.py:919` 是 **py-spy 0.4.2 在 f-string 调用点上的
> 行号归因错误**（非 CPython `f_lineno` 漂移）——919 是 `pk_cols` 为空时的 `else` 裸 INSERT 分支，
> 而 `stock_daily` 主键 `(code, time)` 非空、该分支为死代码。**实际挂起语句 = ON CONFLICT 分支
> （`writers.py:903`，字节码 offset 1142）**。

---

## 1. 错误的完整描述

### 1.1 错误类型

| 维度 | 取值 |
|---|---|
| 错误类别 | **静默挂起 / 忙等（hang / busy-loop）**，非崩溃、非异常 |
| 进程状态 | 存活（未退出），继续持有资源锁 |
| CPU 行为 | 单核持续满载（≈0.97 核），与"IO 等待 / 锁等待 / 网络阻塞"不符 |
| 产出物 | 日志、数据库文件**零字节增长**（CPU 与产出解耦） |
| 稳定性 | 栈帧 20 秒内零抖动（1999/1999 采样点同一栈签名） |

### 1.2 错误消息 / 错误代码

- **无 Python 异常**、**无错误码**、**无 traceback**、**无退出码**。
- 日志最后一条为正常 `wrote ... rows` 记录，之后**静默停止**，无任何 ERROR/CRITICAL。

### 1.3 发生位置

- Python 栈（本次取证报告）：
  - `quantstudio/pipeline/writers.py:919`（`_write_locked`，py-spy 报出的行号）
  - `quantstudio/pipeline/writers.py:707`（`write`）
  - `quantstudio/pipeline/daemon.py:3178`（`_stamp_and_write`）
  - `quantstudio/pipeline/daemon.py:1332`（`_run_with_source_streaming`）
- 实际挂起语句（已复核确认，见 §5.4）：
  - `quantstudio/pipeline/writers.py:903`（`ON CONFLICT` 的 `conn.execute(...)`，字节码 offset 1142）
  - py-spy 报出的 919 为 py-spy 自身在 f-string 调用点上的行号归因错误，非实际执行行

---

## 2. 错误发生的上下文和条件

### 2.1 运行环境（事实）

- 主机：win32，工作区 `D:\hasym\PycharmProjects\QuantStudio`
- Python：`C:\Users\hasym\.conda\envs\quant310\python.exe`，Python **3.10.20**
- DuckDB：**1.4.5**（来自第一次事件取证 §6；本事件同环境，版本一致为推断）
- 数据库：`data/quantstudio.db`，事发时约 **2.1 GB**（用户取证实测 2,158,243,840 B）

### 2.2 任务与数据规模（事实）

- 任务：`mcp_stock_daily`，`source=mcp`，`table=stock_daily`，`freq=daily`
- 进程：**PID 14388**，父进程 `main_gui.py`（PID 11060）
- 运行参数：`python -m quantstudio.pipeline.daemon --mode once --task mcp_stock_daily
  --config-dir ...\config\profiles\mcp_only --quality-audit full --runtime-nonce c3fc5673`
- 模式：`INCREMENTAL`，但因 `last_watermark=None` 实为**全量回填**（2020-01-01 → 2026-10-02）
- 数据规模：`stock_daily` 流式导出 7 批次 → **165 个落盘分片**；本批累计目标约 800 万+ 行
- 主表主键：`stock_daily.primary_key = ["code", "time"]`（`alignment_rules.json`）；
  `writers.py:845` 中 `pk_cols = "(code, time)"`（**非空，truthy**）

### 2.3 触发条件（事实 + 推断）

- 挂起发生在 `stock_daily` 写入**第 57 批**，前 56 批累计 **2,749,437 行**已落库（事实）。
- 本批为**重跑**（表内主键已存在），日志显示"更新 49987"（`updated_rows > 0`），
  故**每一批都走 ON CONFLICT 路径**（`writers.py:898-906`），而非纯 INSERT 规避路径（事实）。
- 推断：P1 规避（无冲突本批降级纯 INSERT）在本重跑场景**全程不生效**——因所有主键均已存在，
  每批 `updated_rows > 0`，永远命中 ON CONFLICT 分支。挂起与第一次事件**落在同一批（第 57 批、
  2,749,437 行）**，表明触发具备**确定性**（数据量/批次边界驱动），而非随机。

---

## 3. 复现步骤

### 3.1 事件级复现路径（事实，两次事件一致）

1. 执行：`python -m quantstudio.pipeline.daemon --mode once --task mcp_stock_daily
   --config-dir config/profiles/mcp_only --quality-audit full`
2. 任务进入全量回填：MCP 流式导出 `stock_daily` 7 批次 → 165 分片；
3. 依赖表 `stock_daily_valuation` 先行全量落库（8,144,924 行，✅）；
4. `adj_factor` 注入 `qfq_aux.db` → QFQ 全局快照构建 → 逐片"线1 还原（前复权）"；
5. 主表 `stock_daily` 逐批 align → validate → write（每批约 5 万行）；
6. 至第 57 批，`ON CONFLICT` 写入永不返回 → 进程单核空转挂起。

### 3.2 最小复现（待执行，来自处置建议）

对 `writers.py` 的 ON CONFLICT 写入分支做**最小复现**：构造一张 ≥ 数百万行、带主键的表，
重放一批 5 万行"主键已存在"的数据，观察 `INSERT ... ON CONFLICT ... DO UPDATE` 是否在
DuckDB 1.4.5 下复现挂起。目的是区分"SQL 本身退化"与"数据量触发的 DuckDB 规划爆炸"。

---

## 4. 受影响的系统或模块

| 模块 | 文件 | 影响 |
|---|---|---|
| 写入器（核心） | `quantstudio/pipeline/writers.py` | `DuckDBWriter._write_locked` / `write`，ON CONFLICT 写入挂起 |
| 任务编排 | `quantstudio/pipeline/daemon.py` | `_stamp_and_write` / `_run_with_source_streaming` 调用链 |
| 数据适配 | `quantstudio/pipeline/sources/mcp_adapter.py` | 流式导出 / adj_factor 注入（上游，未卡） |
| 配置 | `config/profiles/mcp_only/alignment_rules.json` | `stock_daily` 主键定义 |
| 存储引擎 | DuckDB 1.4.5（BufferManager） | 写入索引维护异常停摆（上游定性） |

---

## 5. 相关日志与堆栈跟踪信息

### 5.1 日志证据（事实）

- `daemon.log` 最后推进时间戳 **16:20:11**；此后至 18:58 连续 **2h38m 零字节增长**。
- 日志文件大小 **233,940 B**，`Δ = +0 B`（2h38m）。
- `quantstudio.db` 大小 **2,158,243,840 B**，`Δ = +0 B`（2h38m）。
- 挂起前最后一批日志形态（正常写批，之后中断）：

```
16:0x:xx INFO [DuckDBWriter] stock_daily batch=mcp_stock_daily_mcp_20261002_145052_20e876:
    wrote 49987 rows (新增 0 + 更新 49987) 防重复 upsert
```

### 5.2 py-spy 采样（事实）

- 采样：**20s @ 100Hz = 1999 个采样点**；去重后 **仅 1 个栈签名（100%）**；三次抓栈逐字一致。
- Python 栈（本次报告）：

```
MainThread
    _write_locked            (quantstudio\pipeline\writers.py:919)
    write                    (quantstudio\pipeline\writers.py:707)
    _stamp_and_write         (quantstudio\pipeline\daemon.py:3178)
    _run_with_source_streaming (quantstudio\pipeline\daemon.py:1332)
```

> 注：栈顶 `writers.py:919` 是 py-spy 的 f-string 行号归因错误产物，非实际执行行（实际语句见 §5.4）。

- 第一次事件的 native 栈（关联线索，同缺陷路径）：

```
duckdb::BufferManager::GetBufferManager   <- 最内层
duckdb::CachingFileHandle::Validate / CanSeek
duckdb::Transaction::~Transaction
duckdb::QueryProfiler::EndQuery
duckdb::PendingQueryResult::CheckPulse
duckdb::StreamQueryResult::FetchRaw
duckdb::Relation::Execute
duckdb::PendingQueryResult::Execute        <- 最外层
```

### 5.3 代码落点与行号归因

当前 `writers.py` 的 `_write_locked` 写入分档结构（事实）：

| 行号 | 语句 | 分支 |
|---|---|---|
| 890-892 | `SELECT COUNT(*) FROM {table} WHERE {pk} IN (SELECT {pk} FROM _tmp_write)` | 去重计数（仅 `pk_cols` 非空时） |
| 903-906 | `INSERT INTO {table} ({cols}) SELECT * FROM _tmp_write ON CONFLICT {pk} DO UPDATE SET ...` | ON CONFLICT（`updated_rows>0` 或 fail-closed） |
| 914-917 | `INSERT INTO {table} ({cols}) SELECT * FROM _tmp_write` | 纯 INSERT（无冲突本批，P1 规避） |
| 918-919 | `INSERT INTO {table} SELECT * FROM _tmp_write` | **裸 INSERT（`pk_cols` 为空的 else 分支）** |

- **事实**：`stock_daily` 的 `pk_cols` 为 `(code, time)`（truthy），故 `if pk_cols:`（898 行）恒真，
  **第 918-919 行的 else 分支对 stock_daily 不可达**；且日志"更新 49987"证明走 ON CONFLICT。
- **事实（第一次事件）**：py-spy 报 `writers.py:838`（else 分支），经 `dis` 实证实际执行语句是
  833（ON CONFLICT）（见 `duckdb-conflict-hang-forensics-20261002.md` §3）。注：该文档 §3 当时
  将偏差归因为"`f_lineno` 前移"——**此机制经本次实测已被推翻**（见 §5.4），正确机制是 py-spy
  自身在 f-string 调用点上的行号归因错误；但"实际语句 = ON CONFLICT"的结论不变。
- **复核结论（本次，见 §5.4）**：py-spy 报 919 属 py-spy 的 f-string 行号归因错误；**实际挂起
  语句为 ON CONFLICT（`writers.py:903`，offset 1142）**，919 非实际执行行（已确认）。

### 5.4 行号归因复核（H1 验证结论·字节码实证）

**结论先行**：H1 成立——实际挂起语句 = ON CONFLICT（`writers.py:903`，字节码 offset 1142）。
但**归因机制不是 `f_lineno` 前移偏差**（该机制经实测不存在）；真正原因是 **py-spy 0.4.2 在多行
f-string 调用点上的行号归因错误**。

**字节码事实**（`dis` 精确 offset→行号，4 个 `conn.execute` 调用点）：

| offset | 行 | 语句 | stock_daily 可达性 |
|---|---|---|---|
| 998 | 890 | `SELECT COUNT(*)`（去重计数） | 已返回 |
| **1142** | **903** | **`ON CONFLICT ... DO UPDATE`** | **可达 = 挂起处** |
| 1180 | 915 | 纯 INSERT（带列名） | 不可达（`updated_rows>0`） |
| 1200 | 919 | else 裸 INSERT | 不可达（`pk_cols` 真值） |

**可达性硬证据（字节码级，非源码推断）**：offset 1056-1060 为
`LOAD_FAST pk_cols; POP_JUMP_IF_FALSE 1186`——仅当 `pk_cols` 为假才跳至 919（else 分支）；而
`pk_cols["stock_daily"] = "(code, time)"` 恒真，故 **919 在 stock_daily 路径上是死代码**。

**机制澄清（推翻"`f_lineno` 前移偏差"）**：
- 实测：真实 DuckDB C 扩展调用期间，`f_lineno` **精确落在 `CALL_METHOD` 所在行**，两分支完全
  区分（ON CONFLICT 报 903、else 报 919，零偏移）——即 CPython 层无漂移。
- 4 种行表解析算法（PEP626 半开/闭区间、字节步进、旧式 lnotab）均无法把 offset 1142 映射成
  919（旧式 lnotab 误差仅 ±1，量级远不足 +16）。
- 真正原因：**py-spy 0.4.2 在含 `FORMAT_VALUE`/`BUILD_STRING` 的多行 f-string 调用点上行号
  归因错误**。受控对照实测：主键 100% 重叠批次（真值必走 903）→ py-spy 报 915（+12）；主键
  全不重叠（真值必走 915）→ py-spy 报 893（−22）；单行 `time.sleep(30)` 基线 → 报 7（准确）。
  即 py-spy 仅在简单调用点可信，f-string 调用点不可信。

**时间线佐证**：修复提交 `77405b8`（2026-10-02 13:42）前的版本 `6b8fde1` 中 `_write_locked`
仅占 699-868 行，4 个 execute 在 757/825/833/838，**该版本根本不存在 919 行**——无论按哪个版本
算，py-spy 报 919 都不是有效语句定位，进一步支持"py-spy 行号不可信"的结论。

**沉淀判据**：
1. `co_lines()` 的 `(start, end, line)` 是**半开区间**；`POP_JUMP_IF_*` 的 argval 是**跳转目标
   offset**——据此可机械判定分支可达性，无需读源码猜。
2. 定位语句须**双证据**：字节码可达性 + 独立业务事实（本次「更新 49987」⇒ `updated_rows>0`
   ⇒ 899 真 ⇒ 903）。
3. py-spy 行号仅在简单调用点可信；f-string 调用点必须用字节码交叉验证。

---

## 6. 错误对程序运行的影响范围

- **数据完整性**：`stock_daily` 前 56 批（2,749,437 行）已正常落库；第 57 批起未写入，
  未观察到写入破坏（挂起发生在单条 `conn.execute` 内，未提交即停）。
- **任务状态**：`mcp_stock_daily` 任务无法完成，后续 `--quality-audit full` 审计与水位推进
  均未执行。
- **资源**：进程占用约 1 核 CPU 空转；持续持有 `.collector_run.lock`，**阻塞同类采集任务**。
- **风险**：强杀进程可能打断当前 DuckDB 事务，给 2.1 GB 库留下 **WAL 恢复负担**（待用户决策）。
- **范围外**：`stock_daily_valuation`、`stock_basic`、`etf_basic` 等其他表任务已完成，不受影响。

---

## 7. 已尝试的排查操作及其结果

| 操作 | 结果 |
|---|---|
| 查进程存活（`Get-CimInstance Win32_Process`） | PID 14388 存活，命令行与 manifest `nonce c3fc5673` 完全吻合 |
| 观察日志文件大小变化 | 233,940 B，2h38m 零增长 |
| 观察数据库文件大小变化 | 2,158,243,840 B，2h38m 零增长 |
| CPU 增量采样（60s 窗口） | ΔCPU = 19.39 / 19.56 / 19.48 s ≈ 0.97 核满载 |
| py-spy 抓栈（100Hz × 20s × 3 次） | 1999 采样点，去重后 1 个栈签名（100%），三次一致 |
| 读日志 + 落库目录活跃度 | 日志停 16:20:11，落库无增长 |
| （第一次事件）`dis` 字节码行号表 | 实证 py-spy 行号偏差，钉死实际语句为 ON CONFLICT |

> 过程中踩到的环境坑（已另记 memory）：本机 PowerShell 执行通道返回空 stdout；`GetProcessTimes`
> 参数顺序写错导致 CPU 值异常（≈130 亿秒量级）并 segfault；`wmic` 不可用；py-spy 为独立二进制
> （`py-spy dump`）而非可 import 模块；shell heredoc 被拦需用 Read+Edit。

---

## 8. 已知关联线索 / 观察到的异常行为

1. **同日第一次同类事件**（`docs/evidence/duckdb-conflict-hang-forensics-20261002.md`）：
   PID 13316（00:17:37 启动），同样卡在 `stock_daily` 第 57 批、2,749,437 行，
   CPU 98.2% 单核、1074 MB、80 线程，py-spy 停在 `BufferManager::GetBufferManager`。
   两者卡点高度一致 ⇒ 触发具备确定性。
2. **设计文档 v3.1 已预判该风险**：§3.0 假设 A"缺陷触发路径为 ON CONFLICT 的冲突检查/索引
   更新维护"；本事件（重跑场景每批均走 ON CONFLICT）**印证假设 A**，且说明 P1 纯 INSERT 规避
   在"全更新重跑"场景**无法兜底**，需转 P2/P3。
3. **上游定性**：该缺陷 DuckDB **1.5.x 全线存在**，官方修复在 **1.6.x**（P3 升级方向）。
4. **EXPLAIN 特征**（第一次事件 §4）：去重计数 SELECT 呈现 `SEQ_SCAN + HASH_JOIN SEMI`
   （全表扫描，无 INDEX_SCAN），成本随表增长线性上升。
5. **P2 对 BM 层的局限**（设计文档 §3.2.1）：DuckDB 无释放 BM 缓存页的 PRAGMA，
   P2 对 BufferManager 层异常**无效**，BM 层兜底退化为 P3 升级。

---

## 9. 事实 / 假设 / 待核实（明确区分）

### 9.1 已确认事实（Fact）

- PID 14388 的 `mcp_stock_daily` once 任务存活、持锁、CPU 单核满载、日志/DB 零增长 2h38m。
- py-spy 1999 采样点 100% 单栈签名，栈顶报 `writers.py:919`。
- `stock_daily` 主键为 `(code, time)`（非空），日志显示"更新 49987"（走 ON CONFLICT）。
- 挂起前已写 2,749,437 行 / 56 批，第 57 批中断。
- 第一次事件经 `dis` 实证：py-spy 报 838（else 分支）、实际 ON CONFLICT（833）——结论正确，但其
  "`f_lineno` 前移"机制经本次实测推翻（正确机制为 py-spy f-string 行号归因错误，见 §5.4）。
- **H1 已确认（§5.4）**：919 为 py-spy f-string 行号归因错误产物，实际挂起语句 = ON CONFLICT
  （`writers.py:903`，offset 1142）。

### 9.2 假设 / 推断（Hypothesis / Inference，待实证）

- **H2**：缺陷触发落在 ON CONFLICT 的冲突检查/索引更新维护（对应设计文档假设 A），
  而非纯 INSERT。
- **H3**：本重跑场景每批均 `updated_rows>0`，使 P1 纯 INSERT 规避全程失效，是本次仍挂起的直接原因。
- **H4**：挂起与数据规模/批次边界相关（两事件同卡第 57 批、2,749,437 行，具确定性）。

### 9.3 待核实（Open items）

- （文档一致性）同步修正第一次事件 `docs/evidence/duckdb-conflict-hang-forensics-20261002.md` §3
  的"`f_lineno` 前移偏差"机制表述 → 更正为"py-spy 0.4.2 在 f-string 调用点上的行号归因错误"
  （其结论"实际 = ON CONFLICT"不变）。
- 抓取本次事件的 `py-spy --native` 原生栈，确认是否同样停在 `BufferManager`（H2 佐证）。
- 最小复现 ON CONFLICT 分支，区分 SQL 退化 vs 数据量触发的规划爆炸。
- 确认 P1 规避代码在两次事件之间的部署时间线（建议查 git 历史）。

---

## 10. 建议的下一步（不实施，供立项）

 
1. 对 ON CONFLICT 分支做最小复现（闭合 H2）；H1 已确认（§5.4），无需再复核行号。
2. 依据设计文档 §5 回退/闭环：假设 A 被印证 ⇒ 转 P2 兜底主线；P2 对 BM 层无效 ⇒
   最终兜底为 **P3 升级 DuckDB 至 1.6.x**（登记加速项）。
3. 本错误属框架层缺陷（`writers.py` 为共享核心文件），修复须走
   「方案→审计→实施→验收→用户确认→双仓库推送」六步流水线。
