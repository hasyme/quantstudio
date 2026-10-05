# 交接记录：DuckDB 写入停摆 v2 方案审计结论（2026-10-03）

> 性质：**审计结论 + 修订指示交接件**（自包含、可溯源）。
> 审计对象：`docs/duckdb-write-stall-rootcause-v2-design.md`（v2，2026-10-03，含 L0-A 停摆自动降级重试）。
> 前序：`docs/duckdb-write-stall-mitigation-design.md`（v1，W1–W6 已实施；门1–5/7/8 通过，**门6 未通过**）。
> 生产现状：`stock_daily` 全量回填止步 **56 批 / 2,749,437 行（2020-01-02 → 2022-08-10 半日）**；
> 库 2.64GB、**无 `.wal` 残留**、`.write_lock` 为陈旧锁（可自愈回收）。
> 流程定位：六步流水线第 2 步（审计）**未通过** ⇒ v3 方案须重写后再次送审。

---

## §一 审计结论：v2 不通过

| 项 | 结论 |
|---|---|
| 审计对象 | `docs/duckdb-write-stall-rootcause-v2-design.md`（v2） |
| 裁定 | **不通过（NO-GO）**；主线 **L0-A（停摆自动降级重试）不可实施** |
| 否决性质 | **决定性否决**（F-1、F-2），非"补材料可续" |
| 后续动作 | 主线转为**主动规避**（停摆前规避），由**在线受控实验**逐变体验证（用户裁定，见 §四） |
| v1 状态 | 不受影响，继续有效（W1 仍是当前唯一防护：把无限挂起降为 900s 有界失败） |

---

## §二 决定性否决理由

### F-1｜L0-A 降级路径在**生产病态下不可达**（死代码）

机制链（当前实现，`quantstudio/pipeline/writers.py`）：

| 环节 | 落点 | 行为 |
|---|---|---|
| S1 语句预算 600s 到点 | `writers.py:138 _WriteGuard._on_budget` | `logger.critical` + `conn.interrupt()` |
| 语句**返回**后才抛 | `writers.py:~200 _WriteGuard.__exit__` | 抛 `DuckDBWriteStalled` ⇒ v2 的降级分支在此之后 |
| S2 宽限 300s 耗尽 | `writers.py:161 _WriteGuard._hard_exit` → `writers.py:171 os._exit(75)` | **进程直接死亡** |

门6 实证（`docs/evidence/hang2_gate6_stall_jsonl.txt`）：`interrupt_sent=true`、
`hard_abort=true`、`execute_elapsed_s=900.005`、`outcome=budget_exceeded`
⇒ **interrupt 对本病态无效，进程在降级逻辑可能被执行前就已 `os._exit(75)` 退出**。

⇒ 降级分支恰在其唯一目标场景中**不可达**。若要让其可达，必须关闭 S2，而门7 场景B
（`docs/evidence/hang2_stall_drill.txt`）已证：**语句不响应 interrupt 且 S2=0 ⇒ 退回无限空转**
（演练自身曾因此空转 16 分钟）。故 F-1 无法用"调参"绕过。

### F-2｜DELETE+INSERT 缺原子性论证，且其 INSERT 分量即**已证停摆的 915**

1. **缺原子性（潜在真实数据丢失）**：DuckDB Python autocommit 下 `DELETE` 与 `INSERT` **各自独立提交**。
   v2 §3.1 声称的"事务已由 interrupt 回滚、零数据损失"**只对被中断的那一条语句成立**，不覆盖降级的两步。
   若 `DELETE` 已提交、而 `INSERT` 再次停摆或崩溃 ⇒ **该批旧行已删且未写回 = 数据丢失**，
   直接违反 v2 自己的约束 C2（失败无害）。
2. **分量同形**：事故2 边界探针（`docs/evidence/hang2_shard_boundary_probe.txt`：末日 2022-08-10 仅
   2,652 行，同市场量级 4,869 行/日 ⇒ 末批为新键）判定当次停摆走的是 **纯 INSERT（`writers.py:1201`）**；
   门6 停摆语句 `phase=dml`（`writers.py:1204-1207`，未区分 903/1201 两种形态）。
   即 **v2 降级路径的 INSERT 分量与已证会停摆的语句形态相同**。

⇒ F-2 使 L0-A 同时具备"不可达"与"可达时有损"双重缺陷。

---

## §三 三个审计重点裁定

| # | 裁定 | 落实要求（v3 必须体现） |
|---|---|---|
| ① | **默认关** —— 不同意 v2 的"默认开" | L0-A 即便实现也须 `QS_DUCKDB_WRITE_STALL_FALLBACK` **默认 0**；在无复现环境、F-1/F-2 未解前不得默认启用未验证的物理形态变更 |
| ② | **ORDER BY 黄金对比方向正确**，但需**补读路径排查清单** | v3 须给出"框架内所有读路径是否均带 `ORDER BY`/PIT 条件"的**排查清单**（backtest providers、quality_audit、exporter、qfq_*、GUI 读、writer 内部回读），凡依赖物理行序者须逐条列出并给出处理方式 |
| ③ | **WriteResult 口径正确**，但需补两点 | (a) **count 相不覆盖**：任何重试/降级路径**不得重算写前 `SELECT COUNT`**（`writers.py:1168-1174`），否则 `new/updated` 口径被污染；须以断言锁死"首次 count 值被复用"。(b) **双看门狗 1800s 总上界**：若引入多级看门狗/重试，须显式给出**串联后的总上界（如 3×600s=1800s）**并写入设计与验收，防止多级重试把"有界等待"重新变成无界 |

---

## §四 用户裁定：方向 = 在线受控实验 · 主动规避变体（2026-10-03）

1. **放弃"停摆后恢复"路线**（L0-A 作废）：不在语句停摆后补救，改为**停摆前主动规避**。
2. **手段 = 在线受控实验**：用真实 `mcp_stock_daily` 重跑逐个验证规避变体，选出能越过第 57 批的方案。
3. **基线不重复消耗**：门6 已证"现行 W1 不开规避 ⇒ 第 57 批停摆"，该基线不再重跑。
4. **成本与授权**：每次实验约 1.5 小时且**写生产库**；须**逐次经用户批准**后执行。

---

## §五 修订指示（v3 必须覆盖）

### 5.1 状态与主线

- v2 状态改「**审计不通过**」，主线由"停摆后降级重试"转为"**停摆前主动规避**"。
- v1（W1–W6）**保持有效**，不因 v2 作废而回退：它是当前唯一把无限挂起降为有界失败的防护。

### 5.2 V1–V5 变体矩阵（**本记录提出，待审计确认/修订**）

| 编号 | 变体 | 机制 | 默认 | 风险/注意 |
|---|---|---|---|---|
| **V1** | 批内子批化 | 单条 5 万行 DML 切 5k 子批逐条执行（同一连接，单语句规模↓） | 关（`QS_DUCKDB_WRITE_CHUNK_ROWS=0`） | 批间"部分提交"窗口；写入幂等，重跑收敛 |
| **V2** | 写入局部性 | 批内按 `code` 排序；或按 code 区间分片 | 关 | 可能影响 MCP 导出契约与对齐口径，需先做影响面排查 |
| **V3** | 并行度与保序 | `SET threads=N` 上限 / `preserve_insertion_order=false` | 关 | 改变并行与行序，须评估对读路径与性能的影响 |
| **V4** | staging + **显式事务** merge | 先写无主键 staging（批量 INSERT/COPY），再在 `BEGIN…COMMIT` 内 `DELETE`+`INSERT` 合入主表 | 关 | **原子性可证**（修 F-2 的对症方案）；需新增 staging 表生命周期与续跑清理 |
| **V5** | 表级维护 | 批量 CHECKPOINT / 主键重建（`DROP+ADD PRIMARY KEY`） | 关 | CHECKPOINT 有实测成本（2.33GB WAL≈7.3s）；重建有锁表窗口 |

### 5.3 验收门重设

| 门 | 内容 | 判据 |
|---|---|---|
| **A0** | 在线实验前置门 | 每个变体实验前建立**可复算基线快照**（表行数 / `MAX(time)` / 分片序号 / DB 字节 / 库 WAL 状态）+ **四项在线判定器**（进程存活、日志推进、DB 字节、CPU 30s 窗口与产出耦合）。缺任一项不得开跑 |
| **A1** | 离线等价门 | 变体上线前：结果集 `ORDER BY` 逐项一致、dtype/主键/行数一致、`WriteResult` 口径不变且 **count 不重算**（裁定 ③a）、总时上界 ≤1800s（裁定 ③b） |
| **A5** | 在线受控门 | 真实 `mcp_stock_daily` 重跑：**写批数 ≥ 60**（越过原第 57 批边界）且 A0 四项判据持续成立；跑完 165 分片则记"全量回填完成" |

### 5.4 回退 / 零改动承诺 / 流程闸门

- **回退**：单变体单开关、可独立关闭；任一变体造成数据不一致（黄金对比或质量审计）立即关闭并回退。
- **零改动承诺**：`write()` 签名与返回、`WriteResult` 三字段 int 契约与守恒、DDL、字段契约与列序、dtype、
  空值行为、非停摆异常行为、水位推进与锁语义、daemon 调用链、任何策略源码。
- **流程闸门**：v3 方案 → **审计通过** → 实施 → A0/A1 → **用户逐次批准**后执行 A5（在线实验）→
  验收证据 → **用户确认** → 双仓库推送（`git stash` 回退点已持久化：`30a3883d`）。

---

## §六 证据指针（逐条可复算）

| # | 证据 | 路径 / 落点 | 关键读数 |
|---|---|---|---|
| E1 | 门6 生产实证（运行日志） | `data/logs/_gate6_stock_daily.err.txt`；归档 `docs/evidence/hang2_gate6_run_log.txt` | 01:28:27 第 57 批起 / 01:38:27 S1 / 01:43:27 S2；`stock_daily` 写批 1–56 正常 |
| E2 | 门6 停摆诊断（JSONL） | `docs/evidence/hang2_gate6_stall_jsonl.txt`（抽取脚本 `hang2_gate6_stall_jsonl.py`） | `budget_s=600 / hard_abort_s=300 / interrupt_sent=true / hard_abort=true / execute_elapsed_s=900.005 / phase=dml / rows=50000` |
| E3 | interrupt 无效实证 | 同 E1 时间线 + `docs/evidence/hang2_interrupt_probe.txt` | 正常长查询 11.5s 可被打断；门6 病态 300s 未被打断 |
| E4 | 生产库副本单批试写 | `hang2_realdb_repro.txt`（产出脚本 `hang2_realdb_repro.py` 已于 2026-10-05 随 `data/bench` 清理移除） | `on_conflict 1.6s` / `delete_insert 1.7s` ⇒ 表规模 2.75M 行本身不触发 |
| E5 | H5' 离线回放（4 组全负） | `docs/evidence/hang2_h5_repro.py` / `hang2_h5_repro_production.txt` / `hang2_h5_repro_prodseq_production.txt` / `hang2_h5_timings.csv` | 最大一组"生产同序 236 步 ≈11.8M 行"仍无停摆 ⇒ 离线无可复现环境 || E6 | 门7 演练（含 S2 兜底） | `docs/evidence/hang2_stall_drill.py` / `hang2_stall_drill.txt` | 场景A 1.7s 有界失败；场景B（不响应 interrupt）S2 触发 `os._exit(75)`；演练自身曾因 S2=0 空转 |
| E7 | writers.py 落点 | `quantstudio/pipeline/writers.py` | `_WriteGuard:101`、`_on_budget:138`、`_hard_exit:161`、`os._exit:171`、`_guarded:226`、`_guarded_executemany:238`、`_write_guard:985`、`_write_locked:1016`、写前 count `1168/1174`、ON CONFLICT `1189-1192`、纯 INSERT `1201`、else 裸 INSERT `1203`、DML 看门狗 `1204-1207` |
| E8 | 事故原始报告 | `docs/evidence/duckdb-write-hang-incident2-20261002.md`、`docs/evidence/duckdb-conflict-hang-forensics-20261002.md` | 事故1/2 均卡第 57 批、累计 2,749,437 行 |
| E9 | 第 57 批为新键的判定 | `docs/evidence/hang2_shard_boundary_probe.py` / `hang2_shard_boundary_probe.txt` | 已落库 632 交易日、末日 2022-08-10 仅 2,652 行 ⇒ 末批为新键 ⇒ 走纯 INSERT 形态 |
| E10 | 设计文档 | `docs/duckdb-write-stall-mitigation-design.md`（v1，§9.1 门6 实证）、`docs/duckdb-write-stall-rootcause-v2-design.md`（v2，本记录审计对象） | v1 §4.1 双段看门狗；v2 §3.1 L0-A |
| E11 | 复算方式 | `python docs/evidence/hang2_log_forensics.py` / `hang2_h5_repro.py` | 上述脚本随证据件保留，参数见文件头 docstring。（`hang2_realdb_repro.py` 已于 2026-10-05 移除，其结论以 E4 归档文本为准） |

---

## §七 交接状态一览

| 项 | 状态 |
|---|---|
| 六步流水线 | v1：已实施 + 验收（门6 未通过）｜v2：**审计不通过**｜v3：**待重写** |
| 代码状态 | `writers.py` / `mcp_adapter.py` 已含 v1 改动（未提交）；`tests/test_writer_stall_watchdog.py` 10 passed；写路径回归 102 passed |
| 提交状态 | **未提交**（按铁律：用户确认后才双仓库推送） |
| 回退点 | `git stash` `30a3883d`（`baseline-20261002-writestall-preedit`）已 store |
| 待用户动作 | ① 审核本记录与 v3 的 V1–V5 矩阵（§5.2 为本记录提出，需审计确认）；② 逐次批准 A5 在线实验 |
