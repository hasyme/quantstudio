# daemon 写后全库质量审计「可中断」修复方案（六步流水线 ①方案件）

| 项 | 值 |
|---|---|
| 提案 | TraeCode（主仓线执行会话） |
| 呈审 | 跨任务总调度 |
| 日期 | 2026-09-27 |
| 关联事件 | 2026-09-27 daemon 停写窗口事件（PID 8724：停请求已消费，仍不可中断地跑审计 689 s 后遭硬停） |
| 关联证据 | `docs/evidence/daemon-post-task-audit-stall-20260927.md`（缺陷 D 定性，含 py-spy 双帧同帧与代码逐行复核） |
| 关联裁定 | 2026-09-27 四裁定 ①「D 独立成件照准」；重启就绪条件旁注项（非阻塞） |
| 状态 | **已实施（2026-09-28）** —— 见 §9 实施记录；④验收证据：
`docs/evidence/daemon-interruptible-quality-audit-20260928-d-acceptance.md` |

## 1. 问题定义

### 1.1 现象

2026-09-27 17:48:32，`mcp_only` profile 采集 daemon（PID 8724，`--mode forever`）在
`post_task_mcp_etf_minutes` 任务边界消费停请求（`data/logs/daemon.log` L4922），
按设计转入收尾流程；此后 **daemon 自身零日志 689 s**，直至 18:00:01 被外部硬停。

### 1.2 现场证据（详见证据件，本节只摘要）

- **E1｜py-spy 双帧同帧**（17:59:07 / 17:59:36，间隔 29 s）：最内帧为**单条 SQL 文本行**
  `quality_audit.py:399`（AdjustmentAnchor CTE 起行），调用链
  `_audit_prices(:399) ← run(:113) ← _run_full_quality_audit(daemon.py:2411) ←
  run_one_cycle(daemon_lifecycle.py:706) ← run_forever(:474) ← main(daemon.py:3784)`；
  CPU 28 s 内 +38.7 s（≈1.4 核）⇒ 单查询多线程在跑，**非 Python 级自旋**。
- **E2｜零日志 ⇔ 未返回**：`_run_full_quality_audit` 仅在 `run()` 返回后才写结论日志
  （`daemon.py:2414-2424`），L4922 之后零日志 ⇒ `run()` 未返回。
- **E3｜表规模与卡点成本**：`etf_minutes` 132,815,959 行 / `stock_minutes` 203,428,234 行；
  AdjustmentAnchor 实测 `etf_daily` 0.573 s、`stock_daily` 3.539 s，分钟表仅 `EXPLAIN`
  （窗口输入估计 ~31.03M / ~54.27M 行），线性外推 ≈48 s / ≈73 s 为**乐观下界**
  （窗口排序 O(n log n) + 1.3–2.0 亿行内存溢写风险）。

### 1.3 根因（代码级，逐行复核）

```python
# daemon_lifecycle.py:701-706
# Review FIX-1：质量审计开始前消费 stop
if not stop_requested:
    _check_stop_at_boundary("pre_quality_audit")     # :703 ← stop 在此被消费（文件已删）
# finally 收尾：质量审计（即使 stop 也执行，保证审计覆盖已采集数据）
try:
    quality_audit_ok = collector._run_full_quality_audit()   # :706 ← 无条件调用
```

1. **stop 在此处被消费且不可逆**：`_check_stop_at_boundary`（`:551-565`）调
   `consume_stop_request_if_matched(self.instance_token)` —— **删除** `daemon_stop.request`
   文件并置 `stop_requested=True` / `self._running=False`。此后文件已不存在，
   任何「再读一次 stop 文件」的判读都**恒为 False**。
2. **审计全函数无 stop 判读**：`quality_audit.py::run()`（`:78-216`）逐行确认无
   stop / should_stop 检查；`:90` 起的表循环体、`:203-210` 的四个全局审计段、
   `_audit_prices`（`:322-417`）内部各检查项**均无中断点**。
3. **故必须整体跑完 `:706` 才能到达 `:738` 的 `post_quality_audit` 边界。**
   现场即「stop 已消费 + 审计未走完 ⇒ 既不推进也不退出」。

### 1.4 缺陷类型与影响面

- **类型：正确但不可中断的长任务**（非死锁、非自旋、**无写面**：审计全为只读 SELECT）。
- **不影响正确性**：审计结果不写库、不改变数据；中断亦无 WAL / 无锁残留 / 无需回滚。
- **不阻断重启**：审计无写面，WAL 已 checkpoint（证据件 §2.4）。故本项**不列为重启阻塞项**，
  仅作就绪条件旁注（见 §7）。
- **影响面 = 优雅停 SLA**：现状最坏 689 s（本次实测）。含硬停兜底时的「停不干净」风险
  （外部兜底杀死进程 → 收尾项 `_audit_qfq_factor_integrity` / FM 导出可能被截断）。

### 1.5 与 A / C 缺陷的边界（不同源）

| 缺陷 | 相位 | 根因 | 类型 |
|---|---|---|---|
| A | `post_ingest_running`（采集后·事件发现） | `scan_stock_dividend` 逐行显式事务 N+1 | 性能 |
| C | 采集写内（align→invariant） | `_bar_day_from_ms` 走 `DatetimeArray.strftime` 元素级 Python 循环 | 性能 |
| **D** | 采集后·**收尾审计**（`run_one_cycle` finally） | 停请求消费后仍执行**不可中断**的全库审计 | **可中断性缺失** |

三者相位互不重叠、代码无交集；D 的 py-spy 帧不含 `reserve_pending_slot` / `strftime`。
因修复面（审计可中断 vs. 逐行事务化 vs. 向量化）与追溯链（③实施口径）均不同，
故按四裁定 ① 独立成件，不并入 A/C。

## 2. 改动范围

**核心：为质量审计引入 stop 判读，使其在「停请求已消费」后可分段中止；审计语义与无 stop 时逐位不变。**

### 2.1 逐点改动清单（3 文件，均为最小侵入）

**（1）`quantstudio/pipeline/daemon_lifecycle.py`（调用侧，1 处）**

```python
# :701-706 拟改为：
# Review FIX-1：质量审计开始前消费 stop
if not stop_requested:
    _check_stop_at_boundary("pre_quality_audit")
# finally 收尾：质量审计（stop 已消费时按检查点分段中止，避免长尾阻塞优雅停）
try:
    quality_audit_ok = collector._run_full_quality_audit(
        should_stop=lambda: stop_requested or not self._running)
except Exception as e:
    ...
```

- **关键**：`should_stop` 必须读 `run_one_cycle` 内**已缓存的** `stop_requested`
  （`nonlocal` 变量）/ `self._running`，**严禁再读 `daemon_stop.request`**
  ——该文件已在 `:703` 被删除，重读恒 False（§1.3 第 1 点）。
- `self._running` 一并纳入判据：`_check_stop_at_boundary` 同时置两者
  （`:554`），且 `_graceful_exit` / `request_stop` 路径亦可能置 `_running=False`，
  取「或」可覆盖 stop 文件已消费后的所有停意图。

**（2）`quantstudio/pipeline/daemon.py`（透传，1 处）**

- `_run_full_quality_audit(self, should_stop: Optional[Callable[[], bool]] = None)`
  （`:2388`）→ 透传到 `DataQualityAuditor(..., should_stop=should_stop)`（`:2405-2411`）。
- 返回语义扩展（保持 `bool` 不变，仅新增可分辨日志）：

```python
report = ...run()
if getattr(report, "interrupted", False):
    logger.warning(f"[QualityAudit] 收到停止请求，审计已中止："
                   f"已完成 {report.checks_run} 项、跳过 {report.skipped_after_stop} 项"
                   f"（本项为**未完整审计**，非审计失败）")
    return False          # 诚实：本轮审计未完整
errors = [issue for issue in report.issues if issue.severity == "error"]
...
```

**（3）`quantstudio/pipeline/quality_audit.py`（判读侧，核心）**

- `QualityReport` 增两字段（默认值保证旧构造点不变）：
  `interrupted: bool = False`、`skipped_after_stop: int = 0`。
- `DataQualityAuditor.__init__` 增可选参
  `should_stop: Optional[Callable[[], bool]] = None`（**默认 None ⇒ 行为逐位不变**）。
- 新增私有异常 `class AuditInterrupted(Exception)` 与检查点方法：

```python
def _checkpoint(self, report, item: str) -> None:
    """审计检查点：stop 已请求则中止。无 should_stop 时零开销。"""
    if self._should_stop is not None and self._should_stop():
        report.interrupted = True
        raise AuditInterrupted(item)
```

- `run()` 顶层捕获（`:211-215` 的 `finally: own_conn.close()` 已保证连接回收）：

```python
try:
    tables = ...
    for table, schema in self.schemas.items():
        self._checkpoint(report, f"table:{table}")        # ① 表边界
        ...
finally:
    if own_conn is not None:
        own_conn.close()
```

- **中断粒度（三级检查点，须逐级落地）**：
  - **① 表边界**：`:90` 表循环头（每表 ≤1 项开销）。
  - **② 表内检查项边界**：`_audit_prices`（`:322-417`）每个检查项/SQL 之前、
    `_audit_schema_constraint`、`_audit_frequency`、`_audit_future_and_pit` 各检查项之前；
    特别是 `:397-405` 的 AdjustmentAnchor 查询**之前**（最贵单语句，须前置可退）。
  - **③ 全局段边界**：`:203`（`_audit_watermarks`）、`:205`（`_audit_qfq_orchestration`）、
    `:208`（`_audit_minute_anchor_drift`）、`:210`（`_audit_factor_monotonicity`）、
    以及 `:214-215`（`_audit_batch_pipeline` / `_audit_quarantine`）之前。
- 中止时长与计数：抛出后由 `run()` 捕获，`report.skipped_after_stop` 记被跳过项数
  （以检查点序号差或 `self.schemas` 剩余表数估算，实施时定稿口径）。

### 2.2 机制选型：异常式（M1）优于返回值式（M2）

| 方案 | 描述 | 评估 |
|---|---|---|
| **M1 异常式（推荐）** | 检查点抛私有 `AuditInterrupted`，`run()` 顶层捕获 | 侵入最小；不依赖跨 6 层函数逐层 `return` 传播；`finally` 已有 ⇒ 连接回收不变 |
| M2 返回值式 | 每层检查点 `return report` 逐层判定 | 需改 `_audit_prices` / `_audit_schema_constraint` / `_audit_watermarks` 等**所有**被调函数签名与调用点，漏改即静默失效 ⇒ 风险高，不取 |

### 2.3 诚实边界：单条 SQL 语句不可中断

DuckDB 的单条 SQL 在 C++ 层执行，Python 侧**无检查点可插**。故本方案**只保证「语句之间」可退**，
最坏中止延迟 = **单条最慢语句耗时**：

- 最坏候选 = `_audit_prices` 的 AdjustmentAnchor（分钟表，外推 ≈48 s / ≈73 s，**乐观下界**）。
- 相对现状（689 s）已按压 一个数量级，但**不为零**；本方案**不声称「停即瞬断」**。
- 若要进一步压低该语句成本，需走 §8 待裁决点 2 的候选 (b)/(c)（降成本/收窄窗口），
  与本次修复面（可中断）正交，**不在本方案改动范围内**。

### 2.4 硬不变量（验收须逐条证明）

1. **零 stop 时逐位不变**：`should_stop=None` ⇒ 与改前对同一库产出**逐位相同**的
   `QualityReport`（`issues` 逐项、`checks_run`），且 `interrupted=False`。
2. **审计仍无写面**：本方案不新增任何写语句；中止路径下无事务、无 WAL 增量、无锁残留。
3. **不得二次消费 stop 文件**：判读只用缓存标志（§2.1 第 1 点）；不得新增对
   `daemon_stop.request` 的读取。
4. **终态判定不回归**：`stop_requested=True` ⇒ `can_complete=False`（`:778`
   `traversal_completed and not stop_requested and close_ok`），轮次仍走 interrupted 分支；
   `quality_audit_ok` **仅作记录、不参与控制流**（全仓引用仅 7 处：1 处初始化、3 处赋值、
   3 处 `write_run_state` 入参；`can_complete`（`:778`）不含该变量），
   故「中断 ⇒ False」不改变任何终态。
5. **真实失败不被误吞**：`AuditInterrupted` 为私有类型，仅在 `run()` 顶层捕获；
   其余异常仍按原路径冒泡为「审计执行失败」（`daemon.py:2426-2428`）。
6. **收尾项不被连带跳过**：`:706` 之后的 `_audit_qfq_factor_integrity`（`:715`）与 FM 导出
   （`:727`）仍照常执行（本方案不改这两处）。

### 2.5 明确不改

价格表写入、`_reanchor_security`、`_qfq_gate`、水位提交/保持、QFQ 编排路径、
审计**检查项内容与阈值**、`daemon_run_state.json` 字段结构、`:738` 之后收尾序列。

## 3. 阶段 0：取证前置（**已完成**）

本件的取证前置已于 2026-09-27 20:15–21:1x 独占只读窗内完成，产出
`docs/evidence/daemon-post-task-audit-stall-20260927.md`（定性：非自旋 + 根因定谳 +
卡点语句 + 表规模与成本实测）。**取证已定谳 D 的性质与根因，且止于「只定性、不改码」；
实施方案（本件）即为取证结论的直接落地。**

**待补实测（实施前）**：中止延迟基线 —— 需在静止只读窗内实测分钟表 AdjustmentAnchor
单语句耗时（证据件仅给线性外推下界），用于校准 §5-1 的 SLA 判据。

## 4. 影响面

- **唯一入口**：`run_one_cycle` 的 finally 收尾（`:706`）；daemon 采集主链零改动。
- **复用方**：CLI 独立运行 `python -m quantstudio.pipeline.quality_audit`（`main()` `:782`）
  与测试桩**均不传 `should_stop`** ⇒ 行为逐位不变（由 §2.4-1 覆盖）。
- 不影响：采集正确性、四价格表水位、QFQ 事件发现与重锚、GUI 读库、FM 导出。
- 风险面：**误中断**（未发 stop 却中止）与**漏中断**（发了 stop 仍跑完）——
  两者均以 §5 验收第 3/6 项与单测固定。

## 5. 验收标准

1. **中止延迟（核心）**：stop 已消费后，审计在**首个可达检查点**中止，
   端到端优雅停耗时 ≤ 「单条最慢语句实测耗时 × 1.2 + 收尾固定开销」。
   基线数据：现状 = 689 s（本次实测）；目标量级 = 单语句（分钟表 anchor，外推 48–73 s，
   须以实施前实测校准）。**实测值须写入证据件**。
2. **等价性（最高优先）**：`should_stop=None` 与改前版本对同一库的
   `QualityReport` 逐位相同（`issues` 逐项含 check/table/count/severity/detail、`checks_run`）。
3. **中断标记诚实**：中断时 `report.interrupted=True` 且 `skipped_after_stop>0`；
   `_run_full_quality_audit` 返回 False 并写**专属**日志行（文案与「审计执行失败」
   可区分，禁止二者混同）。
4. **无副作用**：中断路径下 `quantstudio.db.wal` 无增量、无锁残留、无未关闭连接
   （`own_conn` 已由 `finally` 回收）；`shared_conn` 仍可被后续收尾（因子完整性扫描、
   FM 导出）正常使用。
5. **覆盖不丢（已执行项）**：中断只减少**执行项数**，不改变任何**已执行项**的结论
   —— 以「同一 stop 点两次运行，已执行部分的 `issues` 逐位一致」验证。
6. **测试**：新增单测（注入 `should_stop` 在第 N 项返回 True）断言：中止生效、
   报告 partial 且标记正确、无异常冒泡、`own_conn` 已关闭、`skipped_after_stop` 合理；
   回归 QFQ / 质量审计既有用例逐位不变。
7. **终态不回归**：构造「stop 在 `post_task_*` 消费」场景，断言轮次仍落 interrupted，
   且 `quality_audit_ok=False` 仅体现在 run_state 记录、不改 `can_complete` 判定。

## 6. 回退条件

- **开关式回退（首选，零数据影响）**：`should_stop` 为可选参，**不传即恢复旧行为**
  （`daemon_lifecycle.py:706` 去掉实参一行即可）；无需回滚数据、无需改 schema。
- **代码回退**：`git revert` 单提交（3 文件最小改动的原子提交）。
- **回退判据（任一即回退）**：
  1. §5-2 等价性或 §5-7 终态不回归失败；
  2. 出现**未发 stop 却中断**（误中断）；
  3. 出现**真实审计失败被误标为 interrupted**（漏判）；
  4. 中断路径出现 WAL 增量 / 连接泄漏 / 收尾项被连带跳过。
- **设计取舍须总调度确认（重要）**：`:704` 注记原意为
  *「即使 stop 也执行，保证审计覆盖已采集数据」*；本方案**有意部分收窄**该保证
  ——stop 时不再保证**全量覆盖**（改为「已执行项结论可信、未执行项如实标记未审计」）。
  若不接受此取舍，则本方案不成立，应改走 §8-2 的 (b)/(c)（降单语句成本、保留全量覆盖）。

## 7. 附录：与 A 件就绪条件的关系（旁注，非阻塞）

- **D 不阻断重启**：审计无写面、不推进水位、不改数据；本次 689 s 仅影响优雅停 SLA。
- **与 A 件 §7 就绪条件 ② 的关系**：就绪条件 ② 关注的是「陈旧周期态」的血缘正确性
  （`qfq_cycle_run` 非终态残留、`qfq_trigger_queue` / `qfq_watermark_intent` 槽位归位），
  与本件的「优雅停 SLA」**不同维度**；D 仅作该清单的**旁注项**登记，不参与其通过/不通过判定。
- **就绪条件 ① 的关系**：① 指「自旋根因修复或安全缓解落地」，其对象是 A（post-ingest N+1）；
  D 非自旋（证据件 §1），**不属 ① 的修复对象**。
- **与重启链的衔接**：本件若不在周一 06:00 前实施，重启链**不受影响**（D 非阻塞）；
  实施后周一重启将同步获得「优雅停 ≤ 单语句量级」的 SLA 改善。

## 8. 待裁决点

1. **中断项的口径与落库**：`skipped_after_stop` 用「检查点序号差」还是「剩余表数 × 项数」
   估算？是否需要把 `interrupted` / `skipped_after_stop` 同时写入 `daemon_run_state.json`
   （便于事后区分「审计完整通过」与「审计被中断」）？
   —— 建议：入 run_state（两个新字段），避免事后把中断误读为通过。
2. **是否需要正交的降成本配套**（本方案改动范围外，候裁）：
   - (b) 降 `AdjustmentAnchor` 成本：限定窗口（近 N 日）或抽样 code，替代全表双 `ROW_NUMBER()`；
   - (c) 语义收窄：停请求后仅审计「本轮已采集表 + 缩小窗口」。
   二者可把「单条最慢语句」进一步压低，从而把 §5-1 的中止延迟再压一档；
   但均改动审计**覆盖语义**，须独立裁定、独立取证，不并入本件。
3. **是否把「停请求后跳过审计」作为快路径开关**：即在 `:703` 消费 stop 后**直接跳过**
   `:706` 全库审计（本轮不再审计），把 SLA 压到接近 0。
   —— 代价是本轮审计完全不跑（采集正确性不受影响，但审计覆盖为零）；
   建议**不取**（本方案的「分段可退」已足够且保留部分覆盖），仅登记为候裁。
4. **实施窗口**：本件实施涉及 `daemon_lifecycle.py` / `daemon.py` / `quality_audit.py`
   三文件的原子改动 + 单测，建议安排在**周一重启链确认后**（避免与就绪条件 ① 的
   A 件改动物理叠加，便于独立追溯）；如总调度要求 06:00 前落地，
   须先确认 A 件改动是否同期进行（同文件冲突面：`daemon_lifecycle.py`）。

> **【2026-09-28 勘正】** 上句「同文件冲突面：`daemon_lifecycle.py`」为**陈旧表述**，
> 据实勘正：A 件（`post_ingest` N+1，`scan_stock_dividend` 逐行事务化）的改动面
> **不含** `daemon_lifecycle.py`，故两件之间**不存在同文件冲突面**，无需错期实施。
> 本件已于 2026-09-28 00:53 单独落地（见 §9），未与 A 件叠加。

---

## 9. 实施记录（2026-09-28）

### 9.1 落地清单（与 ④证据件 §1 一致）

- `quantstudio/pipeline/quality_audit.py`：`QualityReport` 双字段、`AuditInterrupted`、
  `__init__(should_stop=…)`、`_checkpoint(...)`、三级检查点、`run()` 顶层捕获。
- `quantstudio/pipeline/daemon.py`：`_run_full_quality_audit(should_stop=None)` 透传 +
  中断专属 warning 日志 + `self._last_quality_audit` 结论缓存。
- `quantstudio/pipeline/daemon_lifecycle.py`：收尾调用点传
  `should_stop=lambda: stop_requested or not self._running`（只读已缓存标志）；
  run_state 双字段 `interrupted` / `skipped_after_stop`。
- 测试：`tests/test_quality_audit_interruptible.py`（新增 11 例）+
  `tests/test_daemon_lifecycle.py`（fake 签名补正 + 新增 2 例）。

### 9.2 定稿口径（§8-1 裁定）

- `skipped_after_stop` 取**检查点序号差**：
  `max(1, 顶层段总数 - 已完成顶层段数 + 1)`；
  `顶层段总数 = len(schemas) + 4 + (source_watermark ? 1 : 0) + (QFQ 编排 ? 1 : 0)`。
- 双字段 `interrupted` / `skipped_after_stop` **入 `daemon_run_state.json`**
  （4 处写点，含轮次起点归零），使「完整通过 / 停请求中断 / 审计失败」三态可辨。
- 例外：§2.5 原列「不改 `daemon_run_state.json` 字段结构」被 §8-1 裁定**覆盖**
  （新增两字段，不删不改既有字段）。

### 9.3 **重大勘正**：单条最慢语句实测远超乐观外推

§2.3 / §5-1 原记「分钟表 anchor 外推 ≈48 s / ≈73 s（乐观下界）」；
**2026-09-28 01:02:36–01:08:12 只读窗实测**为：

| 表 | 行数 | 实测墙钟 |
|---|---|---|
| `etf_daily` | 2,168,964 | 0.439 s |
| `stock_daily` | 9,792,638 | 2.813 s |
| `etf_minutes` | 132,815,959 | 59.104 s |
| `stock_minutes` | 203,428,234 | **225.861 s** |

结论：本方案的**最坏中止延迟上界 ≈ 226 s（×1.2 ≈ 271 s）**，而非原估 73 s；
相对现状 689 s 压缩约 **60.7%**。根因：双 `ROW_NUMBER()` 对 2 亿行排序溢写，
成本对行数**非线性**（162 s/亿行 vs 外推 24 s/亿行）。
**§5-1 的 SLA 判据据此校准为「单条最慢语句实测 × 1.2 + 收尾固定开销」。**
若需再压一档，须走 §8-2 (b)/(c)（独立裁定、独立取证）。

### 9.4 回归

10 文件非 GUI 回归集 **131 passed**（含新增 11 例）；既有质量审计 / QFQ / daemon
生命周期用例全绿（详见 ④证据件 §2 §5-6）。GUI 两文件同进程运行的进程级崩溃
经**改前代码回退复现**证实为既存问题，与本件无关。

### 9.5 回退

- 首选开关式：`daemon_lifecycle.py` 去掉 `should_stop=…` 实参一行即恢复旧行为；
- 代码回退：`git revert` 本件单提交（3 核心文件 + 2 测试文件 + 本方案件 + ④证据件）。
