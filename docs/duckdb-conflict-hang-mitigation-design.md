# DuckDB 组件缺陷（长期写入→索引异常→采集停摆）框架层规避设计（A 设计文档 v3·审计修订版）

> 六步流水线第 1 步（方案）。根因已被上游官方定性（DuckDB 缺陷，1.5.x 全线存在，
> 官方修复在下一大版本），并经项目作者《DuckDB 组件缺陷预警与预防性更新》确认。
> 取证证据：2026-10-02 凌晨 `mcp_stock_daily` 任务死循环，py-spy 抓栈定位（见 §1.2）。
>
> **v2**：吸收审计一轮（假设 B 显式化、fail-closed、P2 提级、长时回归强化）。
> **v3**：吸收审计二轮——**P0-1 硬阻塞修复** + P1/P2/P3 全部条目（见 §0.2）。

## 0. 修订记录

### 0.1 v1 → v2（审计一轮）

| 审计项 | v2 处置 |
|---|---|
| 必修点 1：漏标假设 B | §3.0 新增假设 B，与 A 并列；实施前 `EXPLAIN` 钉死索引访问方式 |
| 必修点 2：except 吞错红线 | §3.1 fail-closed 设计 + §4 门 6 单测 |
| 对齐点：落点行号 | §1.2 钉死 833-836；838 为 else 分支 |
| 建议：P2 提级 | §3.2 提级为兜底主线准备 |
| 验收补强 | 门 2 改 3×40min / ≥120min；新增门 6 |

### 0.2 v2 → v3（审计二轮，本版）

| 编号 | 审计问题 | v3 处置 |
|---|---|---|
| **P0-1** | fail-closed 置 `None` → 842 `len(df)-None` TypeError + 864 `WriteResult.updated=None` 破坏 int 契约（修一送一） | §3.1 改为**哨兵 int**：`updated_rows = len(df)`，使 `new_rows=0`、`WriteResult(len(df),0,len(df))`，三字段合法 int 且守恒；分档改用独立布尔标记 |
| P1-1 | "短连接⇒内存态不跨批累积"过于乐观；BM 是进程级共享、短连接绕不开，且 py-spy 停在 `BufferManager::GetBufferManager` 是反信号 | §3.2 内存态细分为 **ART 结构**（随连接释放）vs **BM 缓存页**（进程级共享，短连接不可规避）；补"BM 重置/连接池清空"备选；长时回归区分归因 |
| P1-2 | P2 缺"行为不变"论证 | §3.2 新增 §3.2.1 行为不变论证（锁持有、重建窗口、写入路由、ANALYZE） |
| P2-1 | 长时回归"更新混合比例"未定义 | §4 门 2 明确两场景：(a) 全新增（验假设 A）、(b) 高更新 ≥50%（压 ON CONFLICT + P2 兜底） |
| P2-2 | 未分离 SELECT COUNT / INSERT 耗时，假设 A 与 B 证伪归因混淆 | §4 门 2 补：每批分别记录两段耗时 + 卡死时 py-spy 定位行并写入证据文档 |
| P3-1 | EXPLAIN 无判定标准 | §3.0 补判定特征 |
| P3-2 | 行号偏移属推测 | §1.2 已用 `dis` 实证（见下），结论钉死 |
| P3-3 | 假设 B 证伪后未衔接触发 P2 | §5 补闭环 |
| P3-4 | fail-closed 告警未挂门禁 | §3.1 挂告警计数（见 §0.3 决策：本次用 logger + 内部计数，不新建 quality_audit） |

### 0.3 v3 → v3.1（审计三轮，本版）

| 编号 | 审计问题 | v3.1 处置 |
|---|---|---|
| **P1-1** | fail-closed 持续触发时 P1 **静默失效**（表面"含更新走 ON CONFLICT"，实则 P1 全程未生效，运维无法区分） | §3.1 补**滑动窗口熔断**：20 批内触发 ≥3 次 ⇒ error 告警；≥10 次 ⇒ **P1 自我熔断**（停止分档、全回退 ON CONFLICT）+ 提示转 P2/P3；门 6 覆盖 |
| **P1-2** | 若 BM 重置只剩"进程重启"，则等价于缺陷固有特征（重启≈37 分钟再触发），**P2 对 BM 层实际无效** | §3.2.1 钉死可行性结论：DuckDB 无释放 BM 缓存页的 PRAGMA ⇒ **P2 对 BM 层无效，BM 层兜底退化为 P3 升级**（如实标注，避免误当万能兜底） |
| P2-1 | fail-closed 日志"全部更新"易与真实全更新批混淆 | §3.1 日志补标记：`(fail-closed→ON CONFLICT, {new} new + {updated} updated)` |
| P2-2 | "高更新 ≥50%"场景构造方式未定义 | §4 门 2 补：重放历史批次制造主键重叠 |
| **P2-3** | `_emit_quality_audit_counter` 是否已存在 | **已核实：不存在**（writers.py 无 quality_audit 注入点，847 行注释仅为 fin_indicator eps 回补的"QA 去发现"）⇒ **本次不新建**，降级为 `logger` + writers.py 内部计数，**保持 §2 改动面仅 writers.py**；quality_audit 挂接登记为待办 |
| P2-4 | P2 不可行时未闭环到 P3 | §5 补：P2 不可行 ⇒ 兜底退化为 P3 升级（登记加速） |
| P3-1 | dis 实证输出未落证据 | 实施期贴入 `docs/evidence/` |
| P3-2 | 告警阈值未定义 | 与 P1-1 熔断阈值一并定义（§3.1） |

## 1. 问题定义

### 1.1 缺陷本质（上游定性）
DuckDB 组件在**长期反复写入**后，其内部**主键索引（ART）结构出现异常**：当程序尝试
更新某张表时，更新动作会失败，进而使整个数据库连接不可用 ⇒ 后续采集任务全部停摆。
特征：不报小错直接停摆；重启仅临时恢复（实测约 37 分钟再触发）；**1.5.x 全线存在**，
官方修复在**下一大版本（1.6.x）**；不影响/损坏已有数据（纯停摆）。

### 1.2 本地取证证据（2026-10-02 凌晨）

- 现象：`mcp_stock_daily` 主表 `stock_daily` 写入第 57 批时卡死，此后无日志、无 DB 写入，
  进程 CPU 98% 单核满载（空转）。
- py-spy 抓栈（两次逐字一致）：Python `_write_locked (writers.py:838)`；
  native `duckdb::Transaction::~Transaction → CachingFileHandle::CanSeek/Validate →
  BufferManager::GetBufferManager`。
- **落点行号（`dis` 实证，v3 钉死）**：
  对 `_write_locked` 编译字节码取行号表，确认 **833 与 838 是两条独立的 `conn.execute`
  CALL 指令**（`line=833 CALL_METHOD` = ON CONFLICT 分支；`line=838` = else 纯 INSERT 分支），
  **并非同一语句的行号偏移**。
  判定链：`stock_daily` 的 `pk_cols` 在 796 行有定义（`(code, time)`，truthy）⇒ 走 if 分支；
  且前 56 批均为 upsert 语义成功落库 ⇒ 实际卡顿语句为 **833 的 ON CONFLICT**，
  py-spy 报 838 属 C 扩展调用期间 `f_lineno` 前移偏差。
  **实施落点 = 833（ON CONFLICT 分支）；838 不得改动**（其为无主键表的纯 INSERT 路径）。
- 环境：DuckDB 1.4.5，DB 1614 MB，主表已写 275 万行。

### 1.3 触发路径与并发前提
`daemon._stamp_and_write (3178) → DuckDBWriter.write (695) → _write_locked →
conn.execute(INSERT ... ON CONFLICT)`（833-836）。短连接模式（789 取连接 /
859-860 `finally close()`）。热路径无 CHECKPOINT / 索引重建机制。

**并发前提（审计已核实）**：788 行 `with self._conn_lock:` 包住"计数+写入"全流程，
SELECT 与 INSERT 之间无窗口竞态——P1 等价性论证在"无并发"层面成立。

## 2. 改动范围与影响面

- **仅** `quantstudio/pipeline/writers.py` 的 `DuckDBWriter._write_locked`。
- **零改动**：`daemon.py` 调用链、`write()` 签名/返回值、`WriteResult` **int 契约**（v3 硬约束，
  见 §3.1）、DDL、字段契约、列顺序、dtype、空值行为、异常行为。
- **策略源码零改动**（符合「仅限框架层」铁律）。
- 影响面：所有走 ON CONFLICT 的表，修复必须通用、不针对单一表。

## 3. 设计本体

### 3.0 关键假设清单（须长时回归实证，禁止视为既成事实）

| 编号 | 假设 | 验证 | 证伪后 |
|---|---|---|---|
| **A** | 缺陷触发路径为「ON CONFLICT 的冲突检查/索引更新维护」；纯 INSERT 不触发 | §4 门 2 场景 (a) | P1 失败 → 转 P2 主线（§5） |
| **B** | `updated_rows` 计数 SELECT 不触发缺陷（取决于优化器编成 index probe 还是 semi-join 全扫） | 实施前 `EXPLAIN` + §4 门 2 | 分档不可用 → 回退 P1 整体 → 转 P2 主线（§5） |

**B 的 EXPLAIN 判定标准（P3-1）**：plan 出现 `SEQ_SCAN` + `HASH_SEMI_JOIN`（即未走
索引 probe、退化为全表扫描 + 半连接）⇒ 判定为全扫路径，**假设 B 风险升高**，须在长时回归
中重点观测该 SELECT 的耗时分布（§4 门 2 分段观测）。

**B 的 EXPLAIN 实证结果（实施前已跑，2026-10-02）**：以真实形态验证
（`_tmp_write` = `conn.register` 的 50000 行 DataFrame，与 `_write_locked` 一致）：

```
UNGROUPED_AGGREGATE (count_star)
└── HASH_JOIN  Join Type: SEMI
    ├── SEQ_SCAN   Table: stock_daily   Type: Sequential Scan   ~2,749,437 rows
    └── PROJECTION → PANDAS_SCAN        ~50,000 rows
```

⇒ **命中全扫判定**：该计数 SELECT **不走主键索引（无 INDEX_SCAN）**，而是**每批全表
扫描 stock_daily 全部行**（当前 275 万行，且随表增长单调变慢）后做 hash semi join。
结论：
1. **假设 B 风险确认升高**——须按 §4 门 2 分段观测该 SELECT 耗时分布；
2. 该 SELECT 为**只读**（不写索引），理论上不触发"写入索引"类缺陷，但其单批成本
   随表增长线性上升，是既有性能热点；
3. **本次不优化该 SELECT**（属性能优化范畴，须另走流程并满足"不得改变引擎行为"铁律）——
   仅记录，避免与本次规避改动混叠（防修一送一）。

### 3.1 P1 写入分档：无冲突本批降级为纯 INSERT（主规避）

利用现有 `updated_rows` 计数（825-827）分档：
- `updated_rows == 0`（全新增）：`INSERT INTO {table} SELECT * FROM _tmp_write`（不带 ON CONFLICT）；
- `updated_rows > 0`（含更新）：保留 ON CONFLICT（833-836 不变）。

**等价性**：`updated_rows == 0` 时 DO UPDATE 不命中任何行，结果与纯 INSERT 逐位一致。

**fail-closed（v3 修正 P0-1 硬阻塞）**：

v2 曾置 `updated_rows = None`，该值下游两处消费均不兼容——842 `new_rows =
max(0, len(df) - updated_rows)` 抛 `TypeError`；864 `WriteResult(len(df), new_rows,
updated_rows)` 使 `.updated = None` 破坏 int 契约（`daemon.py:1156/1689/2011` 的
`getattr(wr,"updated",0)` 语义被污染）。属"修一送一"。

→ **v3 取值规则（哨兵 int + 独立标记）**：
```python
dedup_count_failed = False                      # 仅用于分档与告警，不污染计数
try:
    updated_rows = conn.execute(SELECT COUNT ...).fetchone()[0]
except Exception:
    # fail-closed：计数不可用 ⇒ 保守视为"本批全部为更新"
    # ① 分档回退 ON CONFLICT 原路径（异常行为不变，不误走纯 INSERT）
    # ② 取哨兵 int = len(df) ⇒ new_rows = 0 且 new+updated == len(df) 守恒
    # ③ 下游 842/864 保持 int 契约，WriteResult(len(df), 0, len(df)) 合法
    dedup_count_failed = True
    updated_rows = len(df)
    self._record_dedup_fail_closed(table)       # 滑动窗口计数 + 熔断判定（见 §3.1.1）
    logger.warning("[DuckDBWriter] %s 去重计数失败，fail-closed 回退 ON CONFLICT"
                   "（窗口内触发 %d 次）", table, self._dedup_fail_window_hits())

# 分档：dedup_count_failed 或 updated_rows > 0 ⇒ ON CONFLICT；
#       仅 (not failed) 且 updated_rows == 0 ⇒ 纯 INSERT
```
哨兵口径保守但不撒谎：实际可能含新增，但审计侧记"全部更新"是安全方向；三字段均为合法
int 且满足 `new + updated == len(df)`。

#### 3.1.1 fail-closed 滑动窗口熔断（v3.1 新增，补 P1-1 静默失效）

**问题**：v2 的 `None` 会崩在 842 行，故"SELECT COUNT 持续抛普通异常"场景走不到观察；
v3 修了崩溃后该路径可存活，真问题浮出——若 SELECT COUNT 持续失败，每批均
fail-closed 回退 ON CONFLICT，则 **P1 纯 INSERT 规避全程未生效**，但日志显示为
"含更新、走 ON CONFLICT"，**表面正常、实则 P1 已静默退化为原缺陷路径**，运维无法
区分"本批真有更新"与"计数失败回退"。与本次规避初衷相悖，必须可被发现。

**熔断规则（钉死）**——滑动窗口基于 writers 实例内部计数器（不依赖外部组件）：

| 条件（滑动窗口 = 最近 20 批） | 处置 |
|---|---|
| 窗口内 fail-closed 触发 **≥3 次** | `logger.error` 显式告警（含表名、窗口命中次数、最近异常文本），运维可发现 |
| 窗口内 fail-closed 触发 **≥10 次** | **P1 自我熔断**：置 `_dedup_circuit_open = True`，停止分档判定，后续批次**一律走 ON CONFLICT 原路径**（等价修复前行为），并 `logger.critical` 提示"P1 不可用，转 P2/P3" |

- 熔断仅影响**是否尝试纯 INSERT**，不改变任何写入语义/契约（回退到的是修复前的原路径）；
- 熔断状态可复位（`_dedup_circuit_reset()` 或进程重启），复位需人工确认并记录；
- 计数器为内存态、进程级，不落库、不引入新文件（保改动面最小）。

**日志标记（P2-1）**：fail-closed 批次的 wrote 日志补显式标记，与真实全更新批区分：
`wrote {n} rows (fail-closed→ON CONFLICT, {new} new + {updated} updated) 防重复 upsert`

**quality_audit 挂接决策（P2-3，已核实）**：`writers.py` **无现成 quality_audit 注入点**
（847 行注释仅为 fin_indicator eps 回补的"由 QA 去发现"，非 writer 主动注入）。
新建会扩大 §2 声明的改动面 ⇒ **本次不新建**，降级为 `logger.error/critical` +
writers.py 内部计数；quality_audit 计数挂接**登记为待办**（技术债，不阻断本线）。

**假设 A 成立概率（审计共识）**：ON CONFLICT 冲突检测与纯 INSERT 索引插入在 ART 操作层面
高度重叠，若缺陷落在"索引插入"阶段则纯 INSERT 同样触发。**不能把宝压在 P1**——P2 按
"很可能要顶上"同步准备。

### 3.2 P2 兜底主线准备

#### 3.2.1 机制分析（v3 修正 P1-1，内存态分层）

v2 曾以"短连接 ⇒ 内存态不跨批累积 ⇒ 缺陷在磁盘态 ⇒ CHECKPOINT 无效、需重建主键"推论，
该推论**对 ART 结构成立、对 BufferManager 不成立**：

| 层 | 是否随连接释放 | 短连接能否规避 | 结论 |
|---|---|---|---|
| **ART 结构**（主键索引） | 是（连接关闭即释放） | 可 | 缺陷若在此层，重建磁盘主键有效 |
| **BM 缓存页**（BufferManager） | **否——BM 为进程级共享** | **不可** | 缺陷若在此层，短连接绕不开，重建磁盘索引后 BM 仍可能持有异常页 |

**反信号**：py-spy native 栈正停在 `BufferManager::GetBufferManager`，提示缺陷可能落在
BM 缓存页层面。故 P2 兜底手段须分层准备：
1. **重建磁盘主键**（DROP PRIMARY KEY → ADD PRIMARY KEY）：针对 ART 层；
2. **BM 重置**：针对 BM 层。

**BM 重置可行性钉死（v3.1，补 P1-2 价值追问）**：
审计追问——若 BM 重置的可行手段只剩"进程重启"，则它等价于 §1.1 已记录的缺陷固有特征
（重启 ≈ 37 分钟后再触发），**不是规避，只是把触发周期重置**；此时 P2 作为"兜底主线"
实际无效，真兜底只剩 P3。

→ **结论（如实标注）**：DuckDB **未提供释放 BM 缓存页的 PRAGMA / API**；BM 为进程级
共享，关闭单连接不释放其缓冲页。可行的"BM 重置"实质只有：
- 关闭**全部**连接（含长连接 `_shared_conn`）后重建：效果不确定，且中断在线读写；
- 进程重启：**等价于缺陷固有特征，不算规避**。

⇒ **P2 对 BM 层异常无效**；BM 层兜底**退化为 P3 升级 1.6.x**（登记加速，见 §5）。
本条必须显式标注，避免实施期误把 P2 当万能兜底。P2 的有效范围**仅限 ART 层**。

`CHECKPOINT` 保留但**降级为辅助**（仅控 WAL 体积、降磁盘压力），明确禁止以"已做
CHECKPOINT"掩盖 ART/BM 未重置的事实。

**参数**：间隔 N 保守初值 **50 批**（≈250 万行/次），并入 §4 门 2 校准；默认关闭、
`QS_DUCKDB_WRITE_CHECKPOINT_BATCHES` 显式开启（保真开关，默认 off）。

#### 3.2.2 行为不变论证（v3 新增，补 P1-2）

P2 提级为主线后须证明自身不改变既有行为：

| 项 | 处理 |
|---|---|
| CHECKPOINT 在写锁内执行，是否延长锁持有、改变其他写入线程等待特征 | 明确 CHECKPOINT 所处锁区间；若持锁执行须量化额外持锁时长，超阈值则移出锁外并在批次间隙执行 |
| DROP/ADD PRIMARY KEY 在 275 万行 / 1.6 GB 表上的锁表窗口 | 实施前实测窗口时长并记入证据；窗口需显著小于批次间隔，否则改分片/离线窗口 |
| 重建瞬间若有批次正在走 ON CONFLICT，主键临时缺失是否致其退化/报错 | 重建须在 `_conn_lock` 保护下、**无并发写入**的批次间隙执行；重建期间写入路由显式排队（不得让无主键窗口暴露给 ON CONFLICT） |
| 重建后统计信息 | 重建索引后补 `ANALYZE`，避免统计信息缺失导致查询计划退化（计划变化属行为变更，须校验） |

### 3.3 P3 升级 DuckDB 至 1.6.x（长期兜底，登记不实施）

官方下一大版本修复后评估升级；本次不实施，登记技术债
`BLOCKED(外部依赖：DuckDB 1.6.x 未发布)`，解除条件 = 官方发布含修复的版本；
升级后须重新验证 P1/P2 规避代码可否回退（避免"规避代码 + 新版本"叠加未知行为）。

## 4. 验收门

1. **单测（分档正确性）**：构造 `updated_rows==0` / `>0` 本批，断言分档结果与 ON CONFLICT
   原路径落库行数、字段、dtype、主键索引逐位一致。
2. **长时写入回归（核心，v3 补 P2-1/P2-2）**：
   - 强度：**3 次独立 × 40 分钟** 或 **单次 ≥120 分钟**；
   - **场景 (a) 全新增**：验假设 A（纯 INSERT 是否仍停摆）。构造：写入目标表中
     **不存在**的日期区间（如全新历史区段），使 `updated_rows == 0` 恒成立；
   - **场景 (b) 高更新比例 ≥50%**：压 ON CONFLICT 路径本身 + P2 兜底能力。构造：
     **重放已写入的历史批次**（重复提交既有 `code + time` 区间）制造主键重叠，
     使 `updated_rows / len(df) ≥ 50%`；区块大小与真实场景一致（5 万行/批）；
   - **分段观测**：每批分别记录 SELECT COUNT（825）与 INSERT（833）**各自耗时分布**，
     使假设 A / B 的证伪可归因；
   - 卡死时 py-spy 抓栈定位具体行 + dump BM/连接池状态（区分 ART 异常 vs BM 异常），
     写入证据文档；
   - 通过判据：进程存活 + CPU 不过载 + 日志持续推进 + DB 持续写入，四项同时成立。
3. **多策略横验证**：6 策略重转 api_portability 全 PASS。
4. **既有功能回归全绿**：相关套件 + 契约门 + 精确失败清单不变。
5. **黄金结果对比**：股票 / ETF / 指数 fallback 修复前后写入结果逐项一致。
6. **异常路径单测（fail-closed，v3/v3.1）**：注入 SELECT COUNT 抛错，断言
   ① 回退 ON CONFLICT、不误走纯 INSERT；② **WriteResult 三字段均为 int 且
   `new + updated == len(df)`**（直接防 842 TypeError 与契约污染）；③ 异常行为表征
   与未抛错场景一致（不新增 IntegrityError、无数据回滚丢失）；
   ④ **日志含 fail-closed 标记**（P2-1，与真实全更新批可区分）；
   ⑤ **熔断路径**（P1-1）：窗口内注入 3 次 ⇒ 断言 `logger.error` 触发；
      注入 10 次 ⇒ 断言 `_dedup_circuit_open = True` 且**后续批次一律走 ON CONFLICT**
      （P1 自我熔断生效）；
   ⑥ 熔断状态下写入语义与修复前逐项一致（回退的是原路径，无契约变化）。

## 5. 回退条件（v3 补 P3-3 闭环）

- 纯 INSERT 与 ON CONFLICT（无冲突场景）落库结果任何不一致 → 回退（等价性破损）；
- 引入主键冲突/数据错误/**异常行为变更**（含 fail-closed 未生效）→ 回退；
- **假设 A 被证伪**（纯 INSERT 仍停摆）→ 回退 P1，**启用 P2 兜底主线**（§3.2，含 BM 重置）；
- **假设 B 被证伪**（SELECT COUNT 触发缺陷）→ 分档不可用 → 回退 P1 整体，**启用 P2 兜底主线**；
- P2 实测锁表窗口超阈值或破坏并发写入 → 回退 P2 参数、改离线窗口；
- **P2 不可行闭环（P2-4）**：若 P2 离线窗口亦不可行，或缺陷被证实落在 **BM 层**
  （§3.2.1 已标注 P2 对 BM 层无效）⇒ **兜底退化为 P3 升级 1.6.x**，并将该待办
  **登记为加速项**（解除条件：DuckDB 官方发布含修复的版本）；此时 P1 维持熔断态
  或整体关闭，直至 1.6.x 到位。兜底链不得悬空；
- 破坏既有测试或 6 策略横验证 → 回退。
- 实施前 `git stash create -u` + `git stash store` 持久化（铁律③写前快照）+ 精确文件清单
  add；共享核心文件每次 edit 后即时 `git diff` 自检。

## 6. 与铁律对账

- 与「性能优化不得改变引擎行为」：本设计为**稳定性/正确性修复**，非性能优化；但改动写入
  语句形态，故 §4 强制等价性黄金对比 + 门 6 异常行为/契约单测。
  **v2 的 fail-closed 曾引入契约破坏（修一送一），v3 已由哨兵 int 消除——修复后即干净。**
- 与「策略生成与转换全链路修复仅限框架层」：落点仅 `writers.py`，策略源码零改动，
  多策略横验证为门槛。
- 与「框架问题立即解决」：缺陷已定位 + 根因已证实，进入六步流水线，非挂账。
- 与既有 `duckdb-lock-timeout-design.md` / `case008-duckdb-version-mixing-*` 无冲突（边界独立）。
- 技术债：P3 升级 1.6.x，`BLOCKED(外部依赖)`，解除条件 = 官方发布含修复版本。
