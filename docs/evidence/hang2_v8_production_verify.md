# H9 / V8 生产验证：duckdb 1.5.6 修复 stock_daily 写停摆（已闭环）

日期：2026-10-04 | 环境：quant310，duckdb 1.5.6 | 目标：生产库 `data/quantstudio.db`

## 0. 前置（不可逆风险处置）

- duckdb 1.5.x 写入会**提升主库存储版本**，1.4.5 此后无法再读（记忆 ID 88876160 已确认）。
- **动生产库前已完整备份**：`data/bench/prod_backup_20261004_132226.db` + `.wal`（3.57GB，逐字节副本）。
- 升级后备份即"升级前状态"复原点（升级写入的新数据会丢失，但主库结构/旧数据可恢复）。

## 1. 改動清單（最小、可逆/可回退）

| 文件 | 改动 |
|---|---|
| `requirements.txt` | `duckdb>=1.4.5,<1.5` → `>=1.4.5,<1.6` |
| `pyproject.toml`（L20、L50） | 同上（两处钉版放开） |
| `quantstudio/pipeline/duckdb_version_gate.py` | `REQUIRED_SERIES` `"1.4"` → `("1.4","1.5")`；check 改为允许多系列；`_REMEDY`/拒绝文案同步 `<1.6`；保留 `QS_DUCKDB_VERSION_GATE` 逃生阀 |
| `quant310` 环境 | `duckdb==1.4.5` → `==1.5.6` |

版本闸验证：1.5.6 合规通过（无需逃生阀）、1.4.5 合规、2.0 仍拒 `SystemExit(3)`。

## 2. 生产运行（PID 16344，profile `mcp_only` → 真实 `data/quantstudio.db`）

```
14:32:09 [mcp_stock_daily_...] streaming raw=5345538 aligned=5345538 passed=5345531 rejected=7 written=5345531 (new 5326870)
14:32:10 [CLI] once 完成（task + audit 全部通过）
```
- `val_writes=111 sd_writes=109`，`STALL=0 intr=0`（看门狗全程未触发）。
- 首批 stock_daily（正是 1.4.5 下 600s 卡死、退出 75 的断点）5s 内成功写入。

## 3. 落盘后核验

| 指标 | 升级前 | 升级后 |
|---|---|---|
| stock_daily 行数 | 2,764,437 | **8,091,307**（+5,326,870） |
| 水位 / max_time | 2022-08-12 | **2026-10-04**（全量追平） |
| 退出码 | 75（四次事故） | **0** |
| WAL 残留 | — | 否（退出时 checkpoint 干净） |

## 4. 结论

**duckdb 1.5.6 彻底修复了导致四次生产事故的 stock_daily 写停摆**（根因：1.4.x 向 stock_daily 插入新主键的写路径缺陷，1.5.6 已修复，与 V7 PK 重建无关）。生产库已全量回填至最新，task + audit 全部通过。

## 5. 残留风险（已评估接受）

- **#23645（1.5.x ART index-DELETE fatal）**：本负载以 upsert（`INSERT … ON CONFLICT DO UPDATE`）为主，不显式删除 stock_daily 行，触发面不覆盖本场景。
- **不可逆转**：主库存储版本已升 1.5.x，1.4.5 不再能读；升级前备份保留于 `data/bench/prod_backup_20261004_132226.db`。
- 版本闸仍保留 2.0 护栏与逃生阀。

## 6. 待办（铁律六步收尾）

- [ ] 用户确认后：`git add` 精确文件清单（requirements.txt / pyproject.toml / duckdb_version_gate.py）→ 提交 → `git push origin`（quantstudio-plus 与 quantstudio 双仓库一致）。
- [ ] 同步 README.md / docs/strategy_toolbox.md / docs/prompt_engineering.md 中涉及 duckdb 钉版表述（如有）。
- [ ] 清理 bench 隔离产物（`config/profiles/_bench_h9`、`data/bench/*_h9_*`、`docs/evidence/_*.py` 临时脚本）为可选项。
