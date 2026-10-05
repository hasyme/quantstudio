# H9 完整 daemon 副本 A/B（1.4.5 vs 1.5.6）—— **决定性结果：1.5.6 修复了 stock_daily 写停摆**

日期：2026-10-04 | 完整 daemon + 副本（零生产风险）

## 突破：完整 daemon 在副本上复现了停摆

此前 H5'/H9 简化 harness（即使使用真实分片 + 真实 aligner/validator/writer 语义 + 生产库副本 +
WAL + 并发读）都**不挂起**，结论一度是"副本无判定力"。本轮改为启动**完整 daemon**（含 63 线程、
_write_guard 看门狗、3A 写锁、带 namechange/valuation 的 aligner、QFQ 快照读取、长生命周期连接）
指向**副本**，首次在副本上复现成功：

```
11:00:31 CRITICAL [DuckDBWriteStalled] 写语句超预算 600.0s，发 interrupt：
  table=stock_daily batch=mcp_stock_daily_mcp_20261004_100020_76a2a0 rows=50000 phase=dml
11:05:31 ... 本进程将以 75(EX_TEMPFAIL) 退出        # S1=600s, S2=300s, 与四次线上事故逐秒一致
```

⇒ 之前"副本无判定力"是因为简化 harness 漏掉了 daemon 的进程级环境；**真 daemon + 副本 = 可用的复现 harness**。

## A/B 设计（单变量：duckdb 版本）

| 项 | 控制组（1.4.5） | 实验组（1.5.6） |
|---|---|---|
| 解释器/环境 | quant310（duckdb 1.4.5） | 同 quant310，仅 duckdb 临时换 1.5.6，测完回退 1.4.5 |
| 入口 | `python -m quantstudio.pipeline.daemon --mode once --pull-mode incremental --task mcp_stock_daily` | 同左 |
| 配置 | `config/profiles/_bench_h9`（复制自 mcp_only，主库/qfq_aux/quarantine 全改绝对路径指向 `data/bench`，并 `--allow-non-main-target` 显式允许写非主库） | 同左 |
| 数据底库 | 同一份 `data/quantstudio.db` 的逐字节副本（2,764,437 行，水印 2022-08-12，无 WAL） | 同左（重新复制，确保初始态一致）|
| 版本闸 | 无（1.4.5 合规） | `QS_DUCKDB_VERSION_GATE=0`（正式逃生阀，仅测试期使用）|
| 其他开关 | 不设 REBUILD_PK（保持与四次事故/先前实验同构）| 同左 |

## 结果

| 阶段 | 1.4.5（控制组，PID 13452） | 1.5.6（实验组，PID 15796） |
|---|---|---|
| valuation 写批 | 111 | 111 |
| adj_factor 注入 | 109 | 109 |
| **stock_daily 首批（混合批，新增42090+更新7910）** | **600s 停摆 → 硬退 75** | **5s 成功写入** |
| stock_daily 后续批 | —（首批准死） | 连续成功，**共 109 批 / 5,345,531 行（新增 5,326,870）** |
| STALL 计数 | 2（S1+S2 诊断） | **0** |
| 退出码 | 75 (EX_TEMPFAIL) | **0（once 完成，task + audit 全部通过）** |

日志关键行（1.5.6）：
```
12:20:59 [Validator] stock_daily batch=...: passed=50000 rejected=0 warned=27
12:21:04 [DuckDBWriter] stock_daily batch=...: wrote 50000 rows (新增 42090 + 更新 7910) 防重复 upsert
...
12:35:01 [mcp_stock_daily_...] streaming raw=5345538 aligned=5345538 passed=5345531 written=5345531 (new 5326870)
12:35:03 [CLI] once 完成（task + audit 全部通过）
```

## 判定

**唯一变量 = duckdb 版本，且 1.4.5 复现停摆、1.5.6 完整跑通 ⇒ duckdb 1.5.6 修复了 stock_daily 写入停摆。**

机理修正：此前 a5v5 将根因收敛到"ART/表状态层"，V7（PK 重建）测试未改观，说明不是索引
"陈旧/退化"；本实验证明根因是 **duckdb 1.4.x 在向 stock_daily 插入新主键时的写路径缺陷**，
该缺陷在 **1.5.6 已被修复**（不动索引、不重建 PK 即可消除）。

## 治理提示（重要，需人工决策）

- 项目 `requirements.txt` / `pyproject.toml` 双处钉版 `duckdb>=1.4.5,<1.5`，依据是
  **duckdb#23645（1.5.x ART index-DELETE fatal）**，并注明"修复未随 1.x 发布、v2.0 重议"。
- 本实验修复的是 **INSERT/新主键写入**挂起；#23645 是 **index-DELETE fatal** —— 两类操作不同。
  本工作负载以 upsert（`INSERT ... ON CONFLICT DO UPDATE`）为主，**不显式删除** stock_daily 行，
  故 #23645 触发面可能不覆盖本场景；但钉版是**全局**警示，升级 1.5.6 仍属"违反钉版 + 绕过版本闸"，
  必须人工拍板并相应改 `requirements.txt` / `pyproject.toml`（及复核 gate 逃生阀用途）。
- 测试已严格遵守零生产风险：主库 `data/quantstudio.db` 全程未写入；quant310 的 duckdb 已回退 1.4.5。

## 产物

- bench profile：`config/profiles/_bench_h9`（隔离，含 allow-non-main-target 用法范例）
  —— **已于 2026-10-05 随 `data/bench` 一并清理**（实验结论已归档，工作副本不再保留）
- 副本库：`data/bench/hang2_h9_daemon.db`（1.4.5 控制组跑后状态）、`data/bench/hang2_h9_qfq_aux.db`
  —— **已删除**（`data/bench` 于 2026-10-05 整体清理）
- 日志：`data/logs/_h9_daemon.err.txt`（1.4.5 复现）、`data/logs/_h9_156.err.txt`（1.5.6 跑通）
- 启动器：`docs/evidence/launch_156.py`（1.5.6 引导，重排 sys.path 让 venv156 的 1.5.6 优先；
  实际最终用 quant310 临时换 1.5.6 完成，因 venv156 的 TLS 栈与 MCP 服务端握手不兼容）
  —— **该脚本已随 `data/bench/venv156` 清理而移除**（2026-10-05），此处仅存历史记录；
  如需复跑 1.5.6 引导，请重新建隔离 venv 并按本节所述原理重排 sys.path
- 监视器：`docs/evidence/hang2_h9_daemon_monitor.py`
  —— **该脚本已移除**（2026-10-05，其唯一目标库 `data/bench/hang2_h9_daemon.db` 已不存在）
