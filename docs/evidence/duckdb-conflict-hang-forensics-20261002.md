# DuckDB 写入停摆缺陷：现场取证证据（2026-10-02）

> 关联设计：`docs/duckdb-conflict-hang-mitigation-design.md`（v3.1）。
> 用途：钉死根因、落点行号、假设 B 的 EXPLAIN 判定，供方案/验收/回退复核。

## 1. 停摆现象

- 任务：`mcp_stock_daily`（daemon once 模式，pid 13316，2026-10-02 00:17:37 启动）
- 卡死时点：主表 `stock_daily` 写入**第 57 批**（前 56 批 = 2,749,437 行已落库）
- 表现：日志停在 `01:13:48`，DuckDB 最后写入 `01:13:42`，此后无日志、无写库；
  进程存活但 **CPU 98.2% 单核满载**（15s 墙钟消耗 14.75s CPU），内存 1074 MB，80 线程。
- 定性：busy loop（死循环），非死锁（死锁应 CPU 空闲）。

## 2. py-spy 抓栈（两次采样逐字一致）

```
Process 13316: python.exe -m quantstudio.pipeline.daemon --mode once --task mcp_stock_daily ...
Python v3.10.20

Thread 5024 (active): "MainThread"
    _write_locked        (quantstudio\pipeline\writers.py:838)
    write                (quantstudio\pipeline\writers.py:695)
    _stamp_and_write     (quantstudio\pipeline\daemon.py:3178)
    _run_with_source_streaming (daemon.py:1332)
    _run_with_source     (daemon.py:1008)
    _execute_task        (daemon.py:570)
    execute_task         (daemon.py:647)
    run_once             (daemon.py:2233)
    main                 (daemon.py:3834)
```

native 栈（`--native`）：

```
duckdb::BufferManager::GetBufferManager        <- 最内层（正在执行）
duckdb::CachingFileHandle::Validate
duckdb::CachingFileHandle::CanSeek
duckdb::Transaction::~Transaction   (x3)
duckdb::QueryProfiler::EndQuery
duckdb::PendingQueryResult::CheckPulse
duckdb::StreamQueryResult::FetchRaw
duckdb::Relation::Execute
duckdb::PendingQueryResult::Execute            <- 最外层
```

两次 dump 栈帧逐字一致 ⇒ 稳定卡死，非缓慢推进。

## 3. 落点行号（`dis` 实证）

对 `_write_locked` 编译字节码取行号表（只编译不执行，820-845 区间）：

```
line= 825  LOAD_FAST   conn          # SELECT COUNT(*) ...
line= 828  DUP_TOP / RERAISE         # except Exception: updated_rows = 0
line= 830  LOAD_FAST   pk_cols       # if pk_cols:
line= 833  CALL_METHOD               # ON CONFLICT 分支的 conn.execute
line= 838  LOAD_FAST   conn          # else 分支（纯 INSERT）的 conn.execute
line= 842  LOAD_GLOBAL max           # new_rows = max(0, len(df) - updated_rows)
```

**结论**：833 与 838 是**两条独立分支的不同 CALL 指令**，并非同一语句的行号偏移。
判定链：`stock_daily` 的 `pk_cols`（writers.py:796）为 `(code, time)`（truthy）⇒ 走 if 分支；
且前 56 批均为 upsert 语义成功落库 ⇒ **实际卡顿语句 = 833 的 ON CONFLICT**；
py-spy 报 838 属 C 扩展调用期间 `f_lineno` 前移偏差。
**实施落点 = 833（ON CONFLICT 分支）；838 不得改动。**

## 4. 假设 B 的 EXPLAIN 实证

真实形态（`_tmp_write` = `conn.register` 的 50000 行 DataFrame，`stock_daily` 2,749,437 行）：

```
UNGROUPED_AGGREGATE (count_star)
└── HASH_JOIN  Join Type: SEMI
    ├── SEQ_SCAN   Table: stock_daily   Type: Sequential Scan   ~2,749,437 rows
    └── PROJECTION → PANDAS_SCAN        ~50,000 rows
```

**判定**：命中审计 P3-1 的全扫特征（`SEQ_SCAN` + `HASH_JOIN SEMI`，无 INDEX_SCAN）
⇒ **假设 B 风险升高**。该计数 SELECT 每批全表扫描全表，成本随表增长线性上升
（只读、不写索引）。本次不优化该 SELECT（属性能优化范畴，另走流程，防修一送一）。

## 5. v3.1 改动后的最小验证（临时库，非生产库）

| 场景 | 构造 | 结果 |
|---|---|---|
| S1 全新增（`updated_rows==0`） | 新主键 | `WriteResult(submitted=2, new=2, updated=0)`，守恒 ✓ 走纯 INSERT |
| S2 同主键重放（`>0`） | 重放既有 PK | `new=0, updated=2`，守恒 ✓ 走 ON CONFLICT |
| S3 单次 fail-closed | mock 计数 SELECT 抛错 | 三字段均 int、`new+updated==len(df)`、不抛 TypeError ✓ |
| S4 持续 fail-closed | 连续 11 批抛错 | 窗口命中 3 ⇒ `logger.error`；命中 10 ⇒ `logger.critical` + `circuit_open=True` ✓ |
| S5 熔断后写入 | 熔断态下正常写 | `new=0, updated=2`，守恒 ✓ 回退原路径语义不变 |

## 6. 环境

- DuckDB **1.4.5**（`C:\Users\hasym\.conda\envs\quant310`，Python 3.10.20）
- DB：`data/quantstudio.db`，1614 MB，主表 `stock_daily` 已 2,749,437 行
- 预警：上游确认该缺陷 **1.5.x 全线存在**，官方修复在下一大版本（1.6.x）
- 处置：卡死进程 pid 13316 已终止（止损）；已建 stash 回退点
  `baseline-20261002-duckdb-hang-mitigation-preedit`（`f8828fe7`）
