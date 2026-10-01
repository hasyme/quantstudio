# r1 验收证据：就绪四条件核验 + 周一重启（daemon v3）

| 项 | 值 |
|---|---|
| 件 | r1（③实施令「就绪四条件核验+周一重启」链步） |
| 关联 | A 件 `b481186`（①）、A 件 §7（②）、B 件 `d9e70ca`/`ab7f02e`（hold 门，重启前置） |
| 四条件源 | `docs/handoff/dispatch-calendar-20260911.md` 三五三 |
| 日期 | 2026-09-28（周一） |
| 执行 | TraeCode（主仓线执行会话） |
| 状态 | **四条件全数具备；daemon 已重启并回执齐备** |
| 占用窗口 | 自 2026-09-28 00:08 起；本线独占 daemon 起停 |

## 1. 四就绪条件核验

| # | 条件 | 判定 | 证据 |
|---|---|---|---|
| ① | 自旋根因修复或安全缓解 | **就绪** | A 件 `b481186` 已提交（单批集合 CAS 替代 O(N) 逐行事务）；获总调度 ④ 采信 |
| ② | 陈旧周期态回收 | **就绪（重启前新鲜复验）** | 见 §2 |
| ③ | WAL 零残留复核 | **就绪** | `data/` 全扫（depth≤2，`*.wal`）**命中 0**；时点 = 重启前 00:08 |
| ④ | 新 instance-token detached 重启回执 | **就绪** | 见 §3 |

## 2. ② 新鲜只读复验（重启前）

**harness**：`docs/evidence/_r1_readiness_cycle_state.py`（**纯只读** `read_only=True`；不写、不 checkpoint）。

| 表（`quantstudio.db`） | 实测 | 判读 |
|---|---|---|
| `qfq_cycle_run` | interrupted **44** / failed 20 / finalized_held 15 / finalized 3 | **非终态 `started` = 0** ⇒ 无残留（44 = 既有 5 + A 件 §7.3 处置 39） |
| `qfq_trigger_queue` | pending 56608 / superseded 27836 / committed 27166 | **`in_progress` = 0** ⇒ 无在途槽位 |
| `qfq_watermark_intent` | pending 50 / superseded 41 / committed 3 | 与 A 件 §7.2 同；由 `begin_cycle` 的 `supersede_stale_intents` 清障 |
| `qfq_discovery_baseline` | total 54387 | 与 A 件 §7.2 同 |

`qfq_aux.db` 无上述任何表（已实测确认，非误扫）。

结论：② **逐值与 A 件 §7.4 判定吻合**，未见退化。

## 3. ④ 重启回执

命令：`py -3.11 scripts/launch_daemon.py --config-dir config/profiles/mcp_only` → **EXIT=0**（00:09:51）。

| 项 | 观测 |
|---|---|
| 拉起 | `launched pid=32580 token=1785af315ec64838bf59f3139209527f` |
| 启动横幅 | `data/logs/daemon_bootstrap_1785af315ec64838bf59f3139209527f.log`：`exe=…Python311\python.exe`、`prefix=…Python311`、`duckdb=1.4.5`、`token=1785af31…`、`config_dir=…config\profiles\mcp_only`、`hold_marker=…data\daemon_hold.marker` |
| status 文件 | `data/daemon_status.json`：`pid=32580`、`instance_token=1785af315ec64838bf59f3139209527f`、`started_at=2026-09-28T00:09:52`、`status="running"` |
| hold 门留痕 | `data/logs/daemon_hold_check.log`：`2026-09-28T00:09:51 PASS entry=scripts/launch_daemon.py pid=32240 reason=no_marker` ＋ `2026-09-28T00:09:51 PASS entry=quantstudio.pipeline.daemon::main pid=32580 reason=no_marker` ⇒ **两入口同帧 PASS，含 #3 兜底真实触发** |
| 进程五验 | PID 32580 / `CreationDate=2026-09-28 00:09:51` / `exe=…Python311\python.exe` / `cmdline=…-m quantstudio.pipeline.daemon --mode forever --config-dir … --instance-token 1785af31…` 全部一致 |
| 启动日志 | `data/logs/daemon.log`：`常驻模式（v3 DaemonLifecycle）max_iter=None token=provided git_commit=ab7f02e`；`status 已发布 (pid=32580, token=1785af31...)`；**`启动轻量调度循环。daily_time=06:00, check_interval=300s, skip_weekdays=[6], today_completed=False, today_pending_rerun=False`**；`[DuckDBWriter] tables initialized at data\quantstudio.db`；`[ConfigLint] 校验通过（0 错误，0 警告）` |

**附带实证**：`git_commit=ab7f02e` ⇒ 常驻进程运行的正是含 B 件补正的提交；`skip_weekdays=[6]` ⇒ **B 件 §7-1 在新常驻进程上生效**（周日抑制语义已载入）；`today_pending_rerun=False` ⇒ 陈旧 9/27 `running` 态未被误判为今日补跑。

## 4. 诚实边界

1. **③ 的判定时点为重启前**；重启后 daemon 正常写盘将产生 WAL，属预期，非 ③ 违例。
2. **陈旧 `daemon_run_state.json` 未主动清理**（`scheduled_date=2026-09-27`、`status=running`、`qfq_phase=interrupted`）：由今日 06:00 轮次覆写；日志已示 `today_pending_rerun=False` ⇒ 未触发误补跑。
3. **06:00 实跑结果未含**：本轮仅覆盖「就绪 + 重启回执」；生产 `post_ingest` 时长实测（A 件 §8-4 追加要求，实弹对照 scratch 基线）须待 06:00 轮次后单独入证据。
4. **②/③ 为单次采样**，非连续监控；如需窗口内多次采样需另立占用声明。
5. 本文 harness `_r1_readiness_cycle_state.py` 已随证据件归档（照「一切取证/验收 harness 随证据件入库」新纪律）。

## 5. 复现命令

```powershell
cd D:\miniQMT策略实盘\QuantStudio

# ② 只读复验
py -3.11 docs/evidence/_r1_readiness_cycle_state.py

# ③ WAL 零残留
Get-ChildItem -Path data -File -Recurse -Depth 2 | Where-Object { $_.Name -match '\.wal$' }

# ④ 重启（hold 门 + 横幅 + detached Popen；EXIT=0 = 已拉起）
py -3.11 scripts/launch_daemon.py --config-dir config/profiles/mcp_only
```

## 6. 回退

| 目标 | 操作 |
|---|---|
| 停 daemon | 写 `data/daemon_stop.request`（含当前 `instance_token`）→ 任务边界消费；或 GUI 停止 |
| 撤销本次重启 | 无需回退：本步仅拉起，无代码/配置/数据变更 |
