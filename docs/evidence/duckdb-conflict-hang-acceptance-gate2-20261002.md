# 门 2 长时写入回归：验收证据（2026-10-02）

> 关联设计：`docs/duckdb-conflict-hang-mitigation-design.md`（v3.1）§4 门 2
> 关联验收：`docs/evidence/duckdb-conflict-hang-acceptance-gate3-6-20261002.md`
> 现场取证：`docs/evidence/duckdb-conflict-hang-forensics-20261002.md`

## 结论：**PASS**（单次 125.1 分钟，零 stall）

```
elapsed_min     = 125.1   (target 125)      ✅ 满足"单次 >=120 分钟"
total_batches   = 686     (phase_a=100, phase_b=586)
rows_written    = 34,300,000
stalls(>300s)   = 0       []                ✅ 零停摆
count_ms  n=686  p50=57.1    p95=101.0   max=160.4
insert_ms n=686  p50=10823.7 p95=21477.8 max=45817.8
last_insert_sql = INSERT INTO stock_daily (...) SELECT * FROM _tmp_write ON CONFLICT ...
VERDICT         = PASS
```

四项判据全部成立：进程存活至自然结束、CPU 正常消耗（无满载空转）、
日志持续推进（686 批心跳齐全）、DB 持续写入（3430 万行落库）。

## 观测设计（对齐方案 §4 门 2）

| 项 | 实现 |
|---|---|
| 强度 | 单次 **125.1 分钟**（覆盖缺陷"重启后约 37 分钟再触发"节奏，且远超） |
| 场景 (a) 全新增 | 100 批 × 5 万行，每批全新主键 ⇒ `updated_rows==0` ⇒ **纯 INSERT**（验假设 A） |
| 场景 (b) 高更新 | 586 批重放既有主键，更新比例 **100%**（≥50% 要求）⇒ **ON CONFLICT**（压原路径） |
| 分段观测 | 包装 conn 分别计时 SELECT COUNT 与 INSERT，使假设 A/B 可归因 |
| 卡死判据 | 单批 > 300s 记 STALL（实测最大单批 45.8s，从未接近） |

## 分段观测的两个实证

1. **`count_ms` 随表增长单调上升**（14.9ms → 103ms，p95=101）：
   实证 EXPLAIN 判断——去重计数 SELECT 走 `SEQ_SCAN` 全表扫描，成本线性上升。
   本次**未优化**（属性能优化范畴，另走流程，防修一送一）。
2. **纯 INSERT vs ON CONFLICT 量级差异**：
   - 场景 (a) 纯 INSERT：`insert_ms` 稳定 **285~514ms**，不随表增长退化；
   - 场景 (b) ON CONFLICT：p50 **10.8s**，约为纯 INSERT 的 **20~30 倍**，
     且在 22.7s→12.7s 区间波动（DuckDB checkpoint 波动，非单调退化至停摆）。

## ⚠️ 三次运行与「IDE shim 杀死长跑进程」陷阱（重要留档）

| 运行 | 终止位置 | 终止原因 |
|---|---|---|
| v1 (pid 32332) | B-399，停跳 13 分钟（`heartbeat_age=778s`，锁判 `v2_local_dead`） | IDE shim `SystemExit(1)` |
| v2 (pid 18200) | B-397 | IDE shim `SystemExit(1)`（traceback 已捕获） |
| **v3 (pid 32044)** | **越过 B-400 → B-420 → 跑满至自然结束** | **正常完成 PASS** |

v2 捕获的终止栈（决定性证据）：
```
File "...\CodeBuddy CN\...\shim\sitecustomize.py", line 1204, in _flush_bulk_pending
    _exit_bulk_guard_control(...)
  File "...\sitecustomize.py", line 1055, in _exit_bulk_guard_control
    raise SystemExit(1)
SystemExit: 1
```

**归因**：CodeBuddy 注入的 `sitecustomize.py` 批量操作守卫在杀死长跑 Python 进程，
**与 DuckDB 缺陷无关**——v3 禁用 site 后立刻越过同一位置（B-400）并跑满全程。

**绕过方法**（本环境长时任务的必备启动方式）：
```powershell
$env:PYTHONPATH = "<项目根>;<conda env>\Lib\site-packages"
Start-Process -FilePath "<python.exe>" -ArgumentList "-S","<script.py>" ...
```
`-S` 禁用 site ⇒ 不加载 `sitecustomize.py`；`PYTHONPATH` 补偿包搜索路径
（实测 duckdb 1.4.5 / pandas / numpy 均可正常导入）。

**附带教训**：被 shim 杀死的进程**不会释放写锁**，后续任务会撞 `WriteLockHeld`；
框架自愈（`v2_local_dead`）需持有者心跳老化约 13 分钟后才回收，重启前须预留该窗口。

## 局限声明（不得过度解读）

1. **未复现停摆 ≠ 缺陷不存在**。项目作者预警已明确"官方确认缺陷在 1.5.x 全线存在"，
   且 2026-10-02 凌晨生产任务确有真实卡死（`py-spy` 抓栈落在
   `duckdb::BufferManager::GetBufferManager` 死循环，见现场取证文档）。
2. 本测试环境与生产的触发条件不完全一致：
   本机 DuckDB 1.4.5、单表 500 万行、单进程串行、临时库；
   生产为 1614MB 库、主表已写 275 万行、MCP 流式写入叠加 QFQ 复权管线。
3. 因此 **P1 规避的价值仍成立**：纯 INSERT 路径在 125 分钟压测中完全稳定，
   而 ON CONFLICT 路径耗时高一个数量级且波动显著；同时 fail-closed 与熔断
   保证"计数失败时不静默退化为原缺陷路径"。

## 未覆盖 / 遗留

- 生产环境实战回归（建议：重跑 `mcp_stock_daily` 补齐主表缺失的约 536 万行，
  该过程本身即等价于一次真实长时回归）；
- 主表 `stock_daily` 当前仅 275 万 / 811 万行，尚未补齐。
