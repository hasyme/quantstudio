# B 件 ④ 验收证据：`skip_weekdays` v3 生效 + launcher hold 门 + 启动入口收敛

| 项 | 值 |
|---|---|
| 件 | B（六步流水线 ④ 验收证据） |
| 方案件 | `docs/daemon-skip-weekdays-v3-design.md` |
| 裁定 | 六步② 审计 **PASS**；§7-1 裁 **P1**、§7-2 仅呈报、§7-3 照准、§7-4 裁**到期自动放行**；§3.3 收敛**必选**、`daemon.py main()` 兜底**必选** |
| 日期 | 2026-09-27 |
| 执行 | TraeCode（主仓线执行会话） |
| 状态 | **实施完成 + 自测/实证齐备，待总调度逐件实测独采** |
| 回退 | 配置置空（无需改码）/ 删除 marker（无需改码、无需重启）/ `git revert` 单提交 |

## 1. 交付清单（物理证据）

写前快照：`git stash create/ store` = `stash@{0}` "B-impl pre-snapshot 20260927"（`23ec2195beffb71fe7903b02045d41b753e6cd74`）。

### 1.1 改动（版本管理内）

| # | 文件 | pre blob（HEAD） | post blob | 变更 |
|---|---|---|---|---|
| 1 | `quantstudio/pipeline/daemon_lifecycle.py` | `4e5f6d08a669a1cc11eb9d93903017a561c5db28` | `c1e213cd79c39e0761ffaa3d1009b04681853ded` | +27 −2 |
| 2 | `quantstudio/gui/daemon_process.py` | `56a770e03f93da2e59c0efaaecae26cb3a47f71d` | `1fc4ce4a2ceebd5da06c7b5e83e0085b426db5dd` | +6 |
| 3 | `quantstudio/pipeline/daemon.py` | `c52d59701858a2084bc7cd516b9afafe22bed6f4` | `b9d953b938784b8b67803d7964ce718c2f04c8ff` | +5 |

`git diff --stat`（三文件）：`3 files changed, 38 insertions(+), 2 deletions(-)`。

### 1.2 新增（版本管理内）

| 文件 | 字节 | SHA256(前 16) | 说明 |
|---|---|---|---|
| `quantstudio/pipeline/daemon_hold_gate.py` | 11,513 | `A661A6E02E4E527C` | hold 门**唯一实现点**（纯只读、仅 stdlib） |
| `scripts/launch_daemon.py` | 4,419 | `FE4E6400364D4ADB` | **仓内正式启动入口**（hold 门 + 四验 + detached Popen） |
| `tests/test_daemon_hold_and_skip_weekdays.py` | — | `AB0A26AA131B053F` | §5 验收单测（31 项） |

### 1.3 标废弃（**非版本管理目录**，`data/logs/` 未被 git 跟踪）

| 文件 | SHA256(前 16) | 处置 |
|---|---|---|
| `data/logs/launch_gen_main.py` | `0A174B91AD666798` | 文件头加 `DEPRECATED → scripts/launch_daemon.py` 注记；**逻辑未改** |
| `data/logs/launch_daemon_detached.py` | `7F90F41D674D9252` | 同上 |

> 说明：该目录不在版本管理内（`git diff --stat` 对其无输出），故上述两处注记**不可经 git 回溯**，以 SHA256 留痕替代。

## 2. 逐条验收（对应方案件 §5）

### 2.1 §5.1 真值表（`{weekday ∈ / ∉ skip}` × `{completed/running/interrupted/无 state}`）

实驱 `DaemonLifecycle.run_forever`（注入时钟 + `max_iterations=1` + 记录是否发起轮次），12 组：

| now | skip | daily_time | status | 预期 run | 实测 run | skip_reason 写入 |
|---|---|---|---|---|---|---|
| 周日 06:30 | [6] | 06:00 | 无 | 否 | 否 ✓ | `skip_weekday` ✓ |
| 周日 06:30 | [6] | 06:00 | running | 否 | 否 ✓ | ✓ |
| 周日 06:30 | [6] | 06:00 | interrupted | 否 | 否 ✓ | ✓ |
| 周日 06:30 | [6] | 06:00 | completed | 否 | 否 ✓ | 不写 ✓ |
| 周六 06:30 | [6] | 06:00 | 无 | 是 | 是 ✓ | 不写 ✓ |
| 周六 06:30 | [6] | 06:00 | running | 是 | 是 ✓ | 不写 ✓ |
| 周六 06:30 | [6] | 06:00 | interrupted | 是 | 是 ✓ | 不写 ✓ |
| 周六 06:30 | [6] | 06:00 | completed | 否 | 否 ✓ | 不写 ✓ |
| 周六 06:30 | [6] | 08:00 | 无 | 否 | 否 ✓ | 不写 ✓ |
| 周日 06:30 | [6] | 08:00 | running | 否 | 否 ✓ | ✓（pending_rerun 补跑分支亦被抑制） |
| 周日 06:30 | **[]** | 06:00 | 无 | 是 | 是 ✓ | 不写 ✓（空集合 == 旧行为） |
| 周日 06:30 | **[]** | 06:00 | running | 是 | 是 ✓ | 不写 ✓ |

补充用例：`skip_weekdays` 键**缺省**（未声明）→ 不抑制（向后兼容）；抑制事实**每跳过日只写一次**（`max_iterations=2` 下 `属 skip_weekdays` 日志恰 1 条）。

### 2.2 §5.2 星期口径

- `date(2026,9,27).weekday() == 6`（周日）、`09-26 == 5`、`09-25 == 4`、`09-23 == 2`、`09-21 == 0`（周一）；
- 源码断言旧实现同口径：`daemon.py` 含 `now.weekday() in skip_weekdays`（防后续被改成 cron 的 0=周日 语义）。

### 2.3 §5.3 hold 门 m1~m5

| 用例 | 断言 | 结果 |
|---|---|---|
| m1 | marker 未过期 → 拒启 + `SystemExit(3)` + `daemon_launch_rejected_*.log` 生成（含 marker 全文）+ check log `REJECT` | ✓ |
| m2 | marker 缺失 → 放行 + check log `PASS`，**不**产生拒启件、**不**创建 marker | ✓ |
| m3 | marker 过期 → 放行 + check log `PASS(reason=expired)`；**纯只读**：marker 内容逐字节不变、未删除 | ✓ |
| m4 | 5 种畸形：`{not json` / 空 / `[]` / 缺 `expires_at` / `expires_at` 不可解析 → 放行 + 留痕，**无未捕获异常** | ✓（5/5） |
| m5 | check log 在拒绝 / 通过两态各留一行（行序 + `\tREJECT\t` / `\tPASS\t` 校验） | ✓ |
| 附加 | 门模块源码不含 `duckdb` / `requests` / `socket` / `psutil`（硬不变量 4：不依赖网络/DB） | ✓ |

### 2.4 §5.4 入口收敛 + 绕过用例

- **共享实现**：`scripts/launch_daemon.py`（`enforce_hold_or_exit`）、`quantstudio/gui/daemon_process.py`（`ensure_not_held`）、`quantstudio/pipeline/daemon.py`（`enforce_hold_or_exit`）三处均引用 `daemon_hold_gate`（源码断言）。
- **#2 GUI 路径**：marker 生效时 `subprocess.Popen` **被替换为断言函数**（调用即失败）→ 实测抛 `HoldActiveError` 且 Popen **零调用**，拒启留痕生成 ✓。
- **#3 绕过用例**（真子进程）：`py -3.11 -m quantstudio.pipeline.daemon --mode forever --config-dir <tmp>` → `returncode == 3`、输出含「拒启」、拒启留痕生成、`.daemon.lock` **未创建**（证明兜底时点在取锁之前）✓。
- **#1 新入口**（真子进程）：`py -3.11 scripts/launch_daemon.py` → `returncode == 3` + 留痕 ✓（见 §3 实证）。

### 2.5 §5.5 回归

| 套件 | 结果 |
|---|---|
| `tests/test_daemon_hold_and_skip_weekdays.py`（新增 31 项） | **31 passed in 302.62s** |
| `tests/test_daemon_lifecycle.py` + `test_daemon_once_exit_contract.py` + `test_daemon_identity_macos.py` + `test_lock_preserve_file.py` | **45 passed in 4.22s** |
| `tests/test_daemon_qfq_integration.py` | **32 passed in 6.34s** |

非跳过日逐位不变：以改动前语义参考实现 `not completed and (now_hm >= daily_time or pending_rerun)` 为基线，对 `{05:30, 06:30, 23:30} × {无/completed/running/interrupted}` 12 组笛卡尔网格逐组比对，**全部一致**。

### 2.6 §5.6 实证（真实路径）

见 §3。

## 3. 实证：真实路径 hold 门拒启（§5.6 / ③实施令口径）

**证据文件**：`%TEMP%\qfq_forensics_20260927\b_hold_gate_empirical.json`（脚本
`verify_b_hold_gate_real.py`，在**真实** `data/daemon_hold.marker` 上执行）。

| 阶段 | 观测 |
|---|---|
| pre | marker 不存在；`daemon_status.json` 存在（陈旧，进程已死）；daemon 进程 **0**；拒启件 **0**；check log 行数 **0** |
| 签发临时 marker | `data/daemon_hold.marker`，264 B，`ruling_id=B-hold-gate-empirical-20260927`，`expires_at=2026-09-27T23:50:03`（+3 min） |
| 试启（新入口） | `py -3.11 scripts/launch_daemon.py --config-dir config/profiles/mcp_only` → **returncode = 3**，耗时 0.63 s |
| stderr | `[hold-gate] 拒启：hold 生效至 2026-09-27T23:50:03（entry=scripts/launch_daemon.py, marker=D:\miniQMT策略实盘\QuantStudio\data\daemon_hold.marker）。裁定未解除前不得启动 daemon；解除 = 删除 marker 或等待 expires_at 到期。` |
| 拒启留痕 | `data/logs/daemon_launch_rejected_20260927_234704.log`（含 `reason=hold_active`、`expires_at`、**marker 全文**） |
| 审计留痕 | `data/logs/daemon_hold_check.log`：`2026-09-27T23:47:04\tREJECT\tentry=scripts/launch_daemon.py\tpid=18860\treason=hold_active; ...` |
| 副作用 | 新增 `daemon_bootstrap_*.log` = **0**（⇒ **未 Popen**）；daemon 进程 = **0** |
| 撤 marker | marker 已删除，`marker_exists_after = false` |
| 收尾 | daemon 进程 = **0**（实证前后生产态无变化） |

## 4. §7-2 全 profile 清单核对（**仅呈报，不改行为**）

| 路径 | daily_time | skip_weekdays | tasks |
|---|---|---|---|
| `config/profiles/mcp_only/collector_tasks.json` | 06:00 | `[6]` | 88 |
| `config/profiles/prehandover_staging/collector_tasks.json` | 23:17 | `[6]` | 88 |
| `config_legacy_deprecated/collector_tasks.json`（已废弃） | 06:00 | `[6]` | 19 |
| `agent_workspace/shadow_lockprobe/config/collector_tasks.json`（影子副本） | 06:00 | `[6]` | 88 |
| `config/collector_tasks.json`（默认 `--config-dir config`） | — | **不存在** | — |

结论：**无集合分歧**（活跃 profile 与影子副本一律 `[6]`）；默认 config 目录**未声明** `skip_weekdays`
⇒ 走缺省分支 = 旧行为（向后兼容，符合 §6 回退条件）。无需行为变更。

**旧副本引用扫描**（§3.3 收敛前置）：仓内 grep `launch_gen_main|launch_daemon_detached`
仅命中 `docs/`（本设计件与 dispatch 记录），**无脚本/代码引用**；桌面快捷方式（`%USERPROFILE%\Desktop`
+ `%PUBLIC%\Desktop`）指向 QuantStudio 的 `.lnk` = **0**；计划任务（`schtasks /query /v`）命中 = **0**
⇒ 迁指新入口**无需改外部配置**。

## 5. 诚实边界（未覆盖 / 已知代价）

1. **`unreadable` 分支无直测**：`evaluate_hold` 中 `marker 读取失败（OSError）→ 按失效处理` 分支
   未构造单测（需模拟读失败）。该分支与 m4 同属「门自身故障 → 不阻断启动」取舍族，**代码可达、未经实测**。
2. **单条最慢语句 / 单次启动的最坏延迟**：hold 检查为一次 `exists()` + 一次 `read_text()`，
   实测启动拒启耗时 0.63 s（含 Python 解释器冷启）；无 marker 时仅一次 `exists()`。
3. **星期抑制的持久化判据可被外部篡改**：`skip_reason/skip_weekday_date` 若被手工清除，日志会多写一条
   （幂等性依赖 run_state 字段，非独立哨兵）；影响仅限审计日志重复，**不影响调度正确性**。
4. **裁定 P1 的已知代价**：跳过日失败轮次顺延至下一可跑日补跑（周日失败 → 周一补），
   这是 §7-1 明示取舍，非缺陷。
5. **收敛后的「入口唯一性」是代码级保证**：#1/#2/#3 均设门、#4/#5 标废弃且经上表扫描确认无外部引用；
   但**运行期无法阻止**有人新写第三个直调 `Popen` 的脚本 —— 该类新增需靠评审发现（§3.4 第 5 条为
   设计目标而非运行期不可绕过保证）。
6. **`data/logs/` 两副本的废弃注记不在版本管理内**（目录未跟踪），git 层面不可回溯，已以 SHA256 留痕。

## 6. 复现命令

```powershell
# 单测（31 项，含 2 个真子进程绕过用例，约 5 分钟）
cd D:\miniQMT策略实盘\QuantStudio
py -3.11 -m pytest tests/test_daemon_hold_and_skip_weekdays.py -q

# 既有调度/生命周期回归
py -3.11 -m pytest tests/test_daemon_lifecycle.py tests/test_daemon_once_exit_contract.py `
    tests/test_daemon_identity_macos.py tests/test_lock_preserve_file.py -q
py -3.11 -m pytest tests/test_daemon_qfq_integration.py -q

# 真实路径拒启实证（临时 marker，3 分钟自动失效；收尾删 marker）
py -3.11 docs/evidence/_b_hold_gate_empirical_runner.py
```

## 7. 回退

| 目标 | 操作 | 需改码 | 需重启 |
|---|---|---|---|
| 关掉星期抑制 | `collector_tasks.json` 的 `skip_weekdays` 置 `[]` | 否 | 无需（daemon **热加载**，下个 tick 生效） |
| 解除 hold | 删除 `data/daemon_hold.marker` | 否 | 否 |
| 整体回退 | `git revert` 单提交（`data/logs/` 两副本逻辑未被改动，可继续使用） | — | 是 |

不涉 schema / 数据面，无状态迁移。
