# H9 / V8 方案：duckdb 升级到 1.5.6 修复 stock_daily 写停摆

日期：2026-10-04 | 作者：本会话（总调度） | 状态：✅ 已实施并生产验证通过（2026-10-04，exit 0，stock_daily 2,764,437→8,091,307 行，水位追平 2026-10-04）

## 1. 问题定义

- 四次线上 `mcp_stock_daily` 事故（2026-09-23~10-04）在 **首个 stock_daily 批**（混合批，新增 42090 + 更新 7910）卡死于 `DuckDBWriter` 写语句，600s 看门狗发 interrupt 无效 → 硬退 75。
- 根因已通过**完整 daemon + 生产库副本**的 A/B 定位：唯一变量 = duckdb 版本。
  - 1.4.5（控制组）：stock_daily 首批 600s 停摆 → exit 75。
  - 1.5.6（实验组，同副本同 daemon 同配置）：首批 5s 成功，**全量 109 批 / 5,345,531 行** → exit 0。
- 结论：停摆是 **duckdb 1.4.x 向 stock_daily 插入新主键的写路径缺陷**，1.5.6 已修复；V7（PK 重建）无效已印证非索引退化问题。

## 2. 改动范围（最小、可逆/可回退）

| 文件 | 改动 | 可逆性 |
|---|---|---|
| `requirements.txt` | `duckdb>=1.4.5,<1.5` → `duckdb>=1.4.5,<1.6`，注释同步 | 是（改回即可） |
| `pyproject.toml`（两处：L20、L50） | 同上钉版放开 | 是 |
| `quantstudio/pipeline/duckdb_version_gate.py` | `REQUIRED_SERIES` 由 `"1.4"` 改为接受 `["1.4","1.5"]`；`_REMEDY`/拒绝文案中 `>=1.4.5,<1.5` 改为 `>=1.4.5,<1.6`；保留 `QS_DUCKDB_VERSION_GATE` 逃生阀 | 是 |
| 运行环境 `quant310` | 安装 `duckdb==1.5.6`（测完可 `pip install duckdb==1.4.5` 回退） | 是 |

**不改**：任何框架代码逻辑、API 行为、索引/ART 结构、写入路径语义。本次仅是依赖版本闸口放开。

## 3. 影响面

- 版本闸：放开后 1.4.x 与 1.5.x 均合规，1.6+/2.0 仍被拒（保留闸作为未来大版本护栏）。
- 生产库：1.5.6 运行时会**提升主库存储版本**（不可逆，但已备份）。1.5.6 可读 1.4 老库。
- #23645 风险提示：仍成立（1.5.x ART index-DELETE fatal）。本负载以 upsert（INSERT…ON CONFLICT DO UPDATE）为主，**不显式删除** stock_daily 行，触发面不覆盖本场景；属已评估接受风险。

## 4. 验收标准

- [ ] 版本闸对 1.5.6 放行（无需逃生阀即启动成功），对 2.0 仍拒绝。
- [ ] `pip show duckdb` = 1.5.6。
- [ ] 生产库 `mcp_stock_daily` 跑通：stock_daily 首批成功写入、连续批次无 600s 停摆、进程 exit 0、stock_daily 行数显著增长（从 2,764,437 起全量回填）。
- [ ] 主库无 WAL 残留、无“写锁/看门狗”告警。
- [ ] 验收证据写入 `docs/evidence/hang2_v8_production_verify.md`。

## 5. 回退条件与步骤（若生产验证异常）

1. `pip install duckdb==1.4.5` 回退解释器依赖。
2. 文件改回 `<1.5` 钉版、`REQUIRED_SERIES="1.4"`。
3. 主库**已在升级前完整备份**（`data/bench/prod_backup_<ts>.db` + WAL），异常时可整体复原（注意：升级后写入的数据会丢失，但备份保留了升级前状态）。

## 6. 不可逆转风险处置（强制先备份）

- duckdb 1.5.6 写主库会提升存储版本，1.4.5 此后无法再读。
- **实施第一步**：复制 `data/quantstudio.db`（及 `.wal`）到 `data/bench/prod_backup_<ts>.db`，确认大小一致后再动生产。
- 生产验证 daemon 启动前再次确认备份存在。

## 7. 双仓库推送（最后一步，需用户确认后执行）

- 验收通过后，按铁律六步：`git add` 精确文件清单 → 提交 → `git push origin`（quantstudio-plus 与 quantstudio 双仓库一致）。
- 同步 `README.md` / `docs/strategy_toolbox.md` / `docs/prompt_engineering.md` 中涉及 duckdb 钉版的表述（如有）。
