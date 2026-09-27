# D 件：daemon 写后全库质量审计「可中断」修复 —— ④验收证据

| 项 | 值 |
|---|---|
| 件 | **D 件**（`docs/daemon-interruptible-quality-audit-design.md`） |
| 裁定 | 日历 §三六八（十一项裁定）+ §三七一（D 放行；§6 收窄照准 / 双字段入 run_state / 检查点序号差口径） |
| 实施 | 2026-09-28 00:53:42（三核心文件落码）～ 00:55:25（测试落码），主仓线执行会话 |
| 取证 | 2026-09-28 01:02:36–01:08:12（显式只读实测窗）；01:09–01:11（daemon 复核） |
| 取证脚本 | `docs/evidence/daemon-interruptible-quality-audit-20260928-d-anchor-baseline.py`（随证据归档） |
| 状态 | **实施完成 · ④证据齐 · 待呈报**（未推送；本次未改 daemon 运行中行为） |

---

## 1. 实施清单（精确清单，与提交一致）

### 1.1 物理证据（mtime + SHA256 前 16 位 + 字节数）

| 文件 | mtime | bytes | sha256[:16] |
|---|---|---|---|
| `quantstudio/pipeline/quality_audit.py` | 2026-09-28 00:53:42 | 51805 | `87DF3548A57BE2A6` |
| `quantstudio/pipeline/daemon.py` | 2026-09-28 00:53:42 | 226281 | `46DD374B5C1BC982` |
| `quantstudio/pipeline/daemon_lifecycle.py` | 2026-09-28 00:53:42 | 45991 | `AC3E82B755C12949` |
| `tests/test_quality_audit_interruptible.py`（新增） | 2026-09-28 00:55:25 | 6990 | `D5587109BBE133BC` |
| `tests/test_daemon_lifecycle.py` | 2026-09-28 00:54:56 | 25147 | `2F7D246817B79AA2` |
| `docs/daemon-interruptible-quality-audit-design.md`（§8-4 勘正 + §9） | 2026-09-28 01:11:22 | 21594 | `5606402EBC905827` |
| `docs/evidence/daemon-interruptible-quality-audit-20260928-d-anchor-baseline.py`（取证脚本归档） | 2026-09-28 01:11:22 | 2044 | `C2885BC92D0D70EA` |

> 复核方式（第三方可复现）：
> `py -3.11 -c "import hashlib,pathlib;p=pathlib.Path(r'<file>');b=p.read_bytes();print(len(b), hashlib.sha256(b).hexdigest()[:16].upper())"`
> 取证脚本自身用法：`py -3.11 docs/evidence/daemon-interruptible-quality-audit-20260928-d-anchor-baseline.py measure`
> （**只读** `read_only=True`；重跑会与 daemon 的 300s tick 争锁，须按 §5 占用纪律声明窗口）。

`git diff --stat`（改前工作树为基线）：

```
 quantstudio/pipeline/daemon.py           | 29 +++++++++--
 quantstudio/pipeline/daemon_lifecycle.py | 19 ++++++-
 quantstudio/pipeline/quality_audit.py    | 85 ++++++++++++++++++++++++++++++--
 tests/test_daemon_lifecycle.py           | 59 +++++++++++++++++++++-
 4 files changed, 180 insertions(+), 12 deletions(-)
```

### 1.2 逐点改动

| # | 文件 | 改动 | 设计锚点 |
|---|---|---|---|
| 1 | `quality_audit.py` | `QualityReport` 增 `interrupted: bool = False` / `skipped_after_stop: int = 0` | §2.1(3) |
| 2 | `quality_audit.py` | 新增私有异常 `AuditInterrupted(Exception)`（携带 `checkpoint` 标签） | §2.1(3) / §2.4-5 |
| 3 | `quality_audit.py` | `DataQualityAuditor.__init__` 增 `should_stop: Optional[Callable[[], bool]] = None` | §2.1(3) |
| 4 | `quality_audit.py` | 新增 `_checkpoint(report, item, top=False)`：`should_stop=None` 零开销早退；`top=True` 计顶层段序号；命中即置标记并抛 `AuditInterrupted` | §2.1(3) / §8-1 |
| 5 | `quality_audit.py` | `run()`：算 `_ckpt_top_total`；`except AuditInterrupted: pass` 顶层捕获（不吞真实异常） | §2.1(3) |
| 6 | `quality_audit.py` | **① 表边界**：表循环首行 `top=True` 检查点 | §2.1 三级 |
| 7 | `quality_audit.py` | **② 表内边界**：`_audit_prices` 入口 / `for side` 每轮 / **AdjustmentAnchor 查询前** / AdjustmentReturnConsistency 前；`_audit_schema_constraint`、`_audit_frequency`、`_audit_future_and_pit` 各入口 | §2.1 三级 |
| 8 | `quality_audit.py` | **③ 全局段边界**：`watermarks` / `qfq_orchestration` / `minute_anchor_drift` / `factor_monotonicity` / `batch_pipeline` / `quarantine` 六段前各加检查点（前两者按条件） | §2.1 三级 |
| 9 | `daemon.py` | `_run_full_quality_audit(self, should_stop=None)` 透传至 `DataQualityAuditor(..., should_stop=should_stop)`；`report.interrupted` 时写**专属** warning 日志并 `return False`；记 `self._last_quality_audit` | §2.1(2) |
| 10 | `daemon_lifecycle.py` | 收尾调用点传 `should_stop=lambda: stop_requested or not self._running`（**只读已缓存标志**，不重读 `daemon_stop.request`） | §2.1(1) / §2.4-3 |
| 11 | `daemon_lifecycle.py` | run_state 双字段 `interrupted` / `skipped_after_stop`：轮次起点归零 + 3 处结论落点写入 | §8-1 裁定 |
| 12 | `docs/...design.md` | §8-4 陈旧表述勘正（A 件不含 `daemon_lifecycle.py` ⇒ 无同文件冲突面）+ §9 实施记录 | §8-4 裁定 |

**零写面复核**：本件 diff 内无 `INSERT` / `UPDATE` / `DELETE` / `CREATE` / `ALTER` 语句，
唯一新增 I/O 为 `logger.warning` 与 `write_run_state`（后者为既有 run_state 机制，非库写）。

---

## 2. §5 验收逐条对账

### §5-1 中止延迟（核心）——**实测，非外推**

显式只读窗 **2026-09-28 01:02:36 → 01:08:12**，`db = data/quantstudio.db`，
`duckdb.connect(read_only=True)`，语句 = `_audit_prices` 的 AdjustmentAnchor 原文（双 `ROW_NUMBER` CTE）：

| 表 | 行数 | anchor 命中 | **单语句墙钟** |
|---|---|---|---|
| `etf_daily` | 2,168,964 | 5 | **0.439 s** |
| `stock_daily` | 9,792,638 | 0 | **2.813 s** |
| `etf_minutes` | 132,815,959 | 3 | **59.104 s** |
| `stock_minutes` | 203,428,234 | 8 | **225.861 s** |

- **单条最慢语句实测 = 225.861 s（`stock_minutes` anchor）** ⇒ 本方案中止延迟上界
  ≈ `225.861 × 1.2 + 收尾固定开销` ≈ **271 s**；相对现状 **689 s** 压缩 **60.7%**。
- **勘正（重要）**：方案件 §2.3 / §5-1 的「分钟表外推 **48 s / 73 s**」被实测推翻 ——
  实测 **59.104 s / 225.861 s**，其中 `stock_minutes` 偏离乐观下界 **3.1×**。
  根因：`ROW_NUMBER() OVER(PARTITION BY code …)` 对 2.03 亿行排序需溢写，
  成本对行数非线性（162 s/亿行 vs 外推 24 s/亿行）。已回写方案件 §9。
- **诚实边界**：单条 SQL 在 DuckDB C++ 层执行，Python 侧无检查点可插
  ⇒ 本方案只保证「**语句之间**可退」。**不声称「停即瞬断」**；最坏仍是
  单条最慢语句量级（~226 s）。
- 进一步压低该语句（§8-2 (b)/(c)：限窗/抽样/语义收窄）**正交且未做**，候裁。

### §5-2 等价性（最高优先）——**通过**

`should_stop=None`（含不传）与显式 `should_stop=None` / `lambda: False`，
对同一库两次 `run()` 的 `issues` 逐项（check/table/count/severity/detail）与
`checks_run` **逐位相同**，且 `interrupted=False`、`skipped_after_stop=0`。
→ `tests/test_quality_audit_interruptible.py::test_default_matches_explicit_none`、
`::test_never_stopping_predicate_matches_default`

### §5-3 中断标记诚实 ——**通过**

- 中断时 `report.interrupted=True` 且 `skipped_after_stop>0`（`test_interrupt_at_first_checkpoint`
  断言 = 7；`test_checkpoint_serial_diff_counting` 断言序号差公式 = 5）。
- `_run_full_quality_audit` 返回 `False` 并写**专属**日志行（文案
  `[QualityAudit] 收到停止请求，审计已中止：已完成 N 项、跳过 M 项（本项为未完整审计，非审计失败）`），
  与下方 `except` 路径的 `[QualityAudit] 全库审计执行失败: …` **可区分、不混同**。

### §5-4 无副作用 ——**通过**

- `test_interrupt_closes_own_conn_exactly_once`：`own_conn.close()` **恰好 1 次**，
  且随后 `read_only` 重连成功（无残留锁）。
- `test_interrupt_leaves_shared_conn_usable`：`shared_conn` 不被关闭，中断后仍可
  `SHOW TABLES`（收尾项 `_audit_qfq_factor_integrity` / FM 导出可继续用）。
- 本方案零写语句（见 §1.2 复核）⇒ 无事务、无 WAL 增量、无需回滚。
- `finally` 结构未改动关闭语义（`except AuditInterrupted` 与 `finally` 并存）。

### §5-5 覆盖不丢（已执行项） ——**通过**

`test_interrupt_mid_way_keeps_executed_conclusions`：中途中断时 `checks_run > 0`
（已执行项结论全部保留在 `report.issues`），未执行项以
`skipped_after_stop = 顶层段总数 - 已完成顶层段数 + 1` 如实标记。

### §5-6 测试 ——**通过**

- 新增 `tests/test_quality_audit_interruptible.py`：**11 例**（等价性 2 / 检查点语义 3 /
  端到端中断 4 / 真实失败不被误吞 2）。
- `tests/test_daemon_lifecycle.py`：fake collector 签名补 `should_stop=None`，
  新增 `TestInterruptibleQualityAudit` **2 例**（中断入 run_state / 完整轮次归零）。
- 回归（10 文件非 GUI 集，`PYTHONDONTWRITEBYTECODE=1 -p no:cacheprovider`）：

```
131 passed in 94.99s (0:01:34)
```
  含 `test_quality_audit.py` / `test_quality_audit_anchor.py` /
  `test_resident_quality_audit.py` / `test_full_quality_audit_repair.py` /
  `test_daemon_lifecycle.py` / `test_daemon_qfq_integration.py` /
  `test_data_quality_contract.py` / `test_quality_gate_by_freq.py` /
  `test_quality_orchestrator.py` / `test_quality_audit_interruptible.py`
  —— **既有用例逐位不变（全绿）**。

### §5-7 终态不回归 ——**通过**

`test_interrupted_audit_recorded_in_run_state`：stop 在 `post_task_a` 消费后，
轮次仍落 `status="interrupted"`、`traversal_completed=False`、`stop_requested=True`；
`quality_audit_ok=False` 仅体现在 run_state **记录**，`can_complete`
（`daemon_lifecycle.py:can_complete = traversal_completed and not stop_requested and close_ok`）
**不含**该变量 ⇒ 控制流不变。

---

## 3. run_state 双字段（§8-1 裁定落地）

| 字段 | 类型 | 语义 |
|---|---|---|
| `interrupted` | bool | 本轮质量审计是否因停请求**未跑完** |
| `skipped_after_stop` | int | 因停请求被跳过的**顶层段**数（检查点序号差口径） |

- **口径**：`skipped_after_stop = max(1, 顶层段总数 - 已完成顶层段数 + 1)`；
  `顶层段总数 = len(schemas) + 4 + (source_watermark 存在 ? 1 : 0) + (QFQ 编排启用 ? 1 : 0)`
  （4 = 无条件的 `minute_anchor_drift` / `factor_monotonicity` / `batch_pipeline` / `quarantine`）。
- **落点**：4 处 `write_run_state`（轮次起点显式归零 `interrupted=False, skipped_after_stop=0`
  + 3 处结论落点：异常 interrupted / completed / 未完成 interrupted）。
- **三态可辨**（本项目的）：
  `quality_audit_ok=True` → 审计完整通过；
  `quality_audit_ok=False & interrupted=True` → **停请求中断（非失败）**；
  `quality_audit_ok=False & interrupted=False` → **审计执行失败**。

---

## 4. §2.4 硬不变量对账（六条）

| # | 不变量 | 结论 | 证据 |
|---|---|---|---|
| 1 | 零 stop 时逐位不变 | ✅ | §5-2 两例；`should_stop=None` 早退 ⇒ 检查点零开销 |
| 2 | 审计仍无写面 | ✅ | §1.2 零写语句复核；§5-4 两例 |
| 3 | 不得二次消费 stop 文件 | ✅ | `daemon_lifecycle.py` 判据 = `stop_requested or not self._running`（均为 `run_one_cycle` 内已缓存值）；全仓无新增 `daemon_stop.request` 读取 |
| 4 | 终态判定不回归 | ✅ | §5-7；`can_complete` 表达式未改动 |
| 5 | 真实失败不被误吞 | ✅ | `except AuditInterrupted` 为窄类型且仅在 `run()` 顶层；`test_real_query_error_propagates_not_as_interrupt`（无效 regex 触发 DuckDB 异常）、`test_connect_failure_propagates`（路径不存在）均**未被**转为 interrupted |
| 6 | 收尾项不被连带跳过 | ✅ | `_audit_qfq_factor_integrity`（`daemon_lifecycle.py` 其后的 getattr 调用）与 FM 导出两处**未改动**，仍在审计之后照常执行 |

---

## 5. 占用纪律与窗口声明（诚实记录）

- **只读实测窗 01:02:36–01:08:12**：持 `read_only` 连接约 5m36s。
  DuckDB 为单写者模型（read-only 与 read-write 互斥），据此：
  - daemon 的 **01:05 轮轻量检查被静默跳过** —— `data/logs/daemon.log` 在
    `01:00:02` 之后**无新行**（该路径 `from_configs` 异常被 `run_health_check` 的
    `except Exception` 捕获，仅 DEBUG 级 → INFO 日志不落盘），
    即「一次健康检查降级」，**非报错、非崩溃**。
  - **未硬碰、未停 daemon**：daemon 进程 `pid 32580` 全程存活
    （StartTime 2026-09-28 00:09:51，`daemon_status.json.status=running`）。
  - 下一轮 01:10 tick 恢复正常（见 §6 复核）。
- **编辑期与 daemon 的关系**：daemon 代码已载入内存，文件编辑/回归测试不影响其运行；
  本件**未**重启、未触发 stop、未改其运行中行为。
- **测试默认走隔离**：新单测全部用 `tmp_path` 临时库，**不触生产库**；
  唯一触生产库的动作是上述「只读实测」（设计 §3 明列的待补实测项）。

---

## 6. 复核（daemon 未受影响）

复核时点 **2026-09-28 01:11:22**（取证会话）。物理留痕如下：

**(a) daemon 进程存活**——`Get-CimInstance Win32_Process -Filter "ProcessId=32580"`：

```
ProcessId    : 32580
CreationDate : 2026/9/28 0:09:51
```

`data/daemon_status.json` 同刻内容：`"pid": 32580`、`"status": "running"`、
`"started_at": "2026-09-28T00:09:52"`、`instance_token=1785af315ec64838bf59f3139209527f`。

**(b) daemon.log 于只读窗之后正常恢复**——`data/logs/daemon.log`（当时 28 行 / 3128 bytes）尾部：

```
26  01:00:02 INFO quantstudio.pipeline.writers: [DuckDBWriter] tables initialized at data\quantstudio.db
27  01:00:02 INFO quantstudio.pipeline.config_lint: [ConfigLint] 校验通过（0 错误，0 警告）
28  01:10:37 INFO quantstudio.pipeline.writers: [DuckDBWriter] tables initialized at data\quantstudio.db
29  01:10:37 INFO quantstudio.pipeline.config_lint: [ConfigLint] 校验通过（0 错误，0 警告）
```

即：**01:00:02（进入只读窗前最后一次）→ 01:10:37（退出只读窗后第一次）**，
中间 `01:05` 轮缺行，与 §5 记录的「只读窗 01:02:36–01:08:12 静默吞掉 01:05 tick」
**逐点吻合**；退出后 tick 立即恢复 300s 节奏 ⇒ **无残留影响**。

- **06:00 定时轮次不受影响**（本件非阻塞项；实测窗在 01:0x，距 06:00 有 4h49m）。

---

## 7. 诚实边界与未做项

- **§6 设计取舍已照准并落地**：stop 时**不再保证全量覆盖**，改为
  「已执行项结论可信 + 未执行项如实标记（`skipped_after_stop`）」。
- **单条 SQL 不可中断**：最坏中止延迟 = 单条最慢语句 225.861 s（§5-1），
  **不为零**，本件不声称「停即瞬断」。
- **未做（正交、候裁）**：§8-2 (b)/(c) 降 `AdjustmentAnchor` 成本配套。
- **未做**：§8-3「停请求后直接跳过审计」快路径（裁定不取，仅登记）。
- **未做**：r2 生产 `post_ingest` 时长实测（待 06:00 轮次跑完后按 r2 令执行）。
- **勘正项**：§8-4「同文件冲突面：`daemon_lifecycle.py`」系陈旧表述 ——
  A 件（post-ingest N+1，`scan_stock_dividend` 逐行事务化）**不触及**
  `daemon_lifecycle.py`，两件无同文件冲突面；本轮实施未与 A 件叠加。
