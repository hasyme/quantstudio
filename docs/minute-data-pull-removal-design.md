# 分钟数据支持与「全量/增量拉取」机制删除方案（六步流水线·步骤①修订稿 v4）

> **日期**：2026-10-01（v4 修订，响应审计 `docs/audit-minute-data-pull-removal-design-v3.md`）
> **性质**：框架层行为变更（删除 + 禁用 QFQ 编排器），适用「框架层改动六步流水线」铁律
> **审计结论（v3）**：仍不通过。阻断项 P1–P5（QFQ reanchor 子系统深度硬编码分钟数据），揭示「方案 B 日线化」实为 2–3 倍体量的小型子系统重构。
> **本稿（v4）修订摘要（采纳用户裁定「选项 B：独立立项」）**：
> - **撤回方案 B（日线化）**：QFQ 编排器日线化（重写 `qfq_reanchor_engine.py` / `qfq_fresh_capture.py` 等）**拆出为独立子项目**，另行立项审计，**不在本次方案范围内**。
> - **本次改为「禁用 QFQ 编排器」**：`qfq_orchestrator.enabled=false`，QFQ resident 周期整体挂起，水位退化为纯 `writer.advance_watermark` 直推。P1–P5 不触发，零 QFQ 引擎代码改动。
> - **N3/N4 保留**：GUI 导出页分钟选项、`_MINUTE_FREQS`/`FREQ_MS` 等分钟残留清理仍属本次删除范围。
> - **新增影响 I9/I10/I11**：QFQ gate 失效（水位直推）、QFQ 日线化拆项遗留（`freqs=["1min"]` 惰性保留）、QFQ reanchor 引擎深度改造（拆项）。
> **当前状态**：步骤①方案 v4，待重审。

---

## 0. 问题定义

QuantStudio 数据管线当前具备两类能力，用户裁定**删除**：

1. **「全量/增量拉取」机制**（增量侧）：daemon（`ResidentCollector`）按 `full_range` / `incremental` 两种 mode 采集，依赖 A4 云端变更检测（`UpdateDetector`）、续传游标（`TaskResumeCursor`）完成增量续传与变更重拉。
2. **分钟数据支持**：`etf_minutes` / `stock_minutes` 两张 1min 行情表，贯穿回测引擎（分钟事件驱动回测）、数据访问层、QFQ 复权管线、数据源适配层、校验/审计层、配置与 GUI。

**经审计修正后的删除边界**：仅删除「增量续传 / A4 变更检测 / mode 区分」三部分；**水位线（`source_watermark`）与 `qfq_watermark_intent` 表不删除**（保留结构供后续 QFQ 子项目复用），但**QFQ 编排器本次禁用**（见下）。

**第三项能力（v4 定夺：禁用，非改造）**：QFQ resident 编排器是「分钟专用」子系统（校验器强制 `freqs` 含 `"1min"`、重锚引擎 `qfq_reanchor_engine.py`/`qfq_fresh_capture.py` 深度硬编码分钟数据，审计 P1–P5）。移除分钟数据后，该编排器失去作用对象，且「日线化」需重写 QFQ 引擎（2–3 倍体量）。经用户裁定采纳**选项 B（独立立项）**：本次**禁用 QFQ 编排器**（`enabled=false`），QFQ 日线化改造**拆出为独立子项目**另行立项审计，不在本次范围。

---

## 1. 改动范围（精确清单）

### Part A：删除「增量机制」（保留水位线与 QFQ gate）

#### A0. 关键裁定：水位线链路**保留**（R1 修正）

以下链路**不删除、不改语义**（v1 错误列入删除清单，已撤回）：
- `daemon.py` `_advance_or_defer_watermark`（477–506）：水位推进唯一入口（红线），QFQ 编排器 enabled 时走 `defer_watermark`（写 `qfq_watermark_intent`），disabled 时走 `writer.advance_watermark`。
- `daemon.py` `_get_safe_watermark`（2718–2748）、`_advance_actual_watermark`（2750–2778）、`_max_date`（3470–3480）。
- `writers.py` `BaseWriter.get_last_date`（394–397）、`BaseWriter.advance_watermark`（399–403）、`DuckDBWriter.get_last_date`（1033–1045）、`DuckDBWriter.advance_watermark`（1047–1077）——**语义不变**，`get_last_date` 保持按 source 分区读水位（不改为表内 MAX，多 source 共表场景返回「该源进度」不变）。
- `qfq_resident_orchestrator.py` `defer_watermark` / `_commit_or_hold_watermarks`（893–917）/ `_advance_watermark`（919–936，8 列 `source_watermark` 显式 upsert）。
- `daemon.py:328` `watermark_advancer=self.writer.advance_watermark` 注入点。
- `source_watermark`、`qfq_watermark_intent` 两表 DDL 与读写。

#### A1. 删除 A4 变更检测（整链路）
| 目标 | 位置 | 说明 |
|---|---|---|
| `update_detector.py` | 整文件 | `UpdateDetector` Protocol / `MockUpdateDetector` / `MCPUpdateDetector` / `load_last_sync` / `save_last_sync` / `ensure_last_sync_table` |
| `UpdateDetector` 注入 | daemon.py 213–216、2515–2518 | Mock/MCP 后端注入 |
| `_check_cloud_updates_and_repull` | daemon.py 2607–2716 | A4 云端变更检测 + 局部重拉 |
| A4 检测调用 | daemon.py 929–930、935–936 | 调用点移除 |
| `_a4_boundary` | daemon.py 732–… | A4 修复段边界进度 |
| `_maybe_skip_deferred_replay` | daemon.py 508–… | A4「无需重拉」短路 |
| `should_skip_deferred_replay` | daemon.py 模块级 3575–3578 | deferred 重放跳过（模块级） |
| `save_last_sync` 调用 | daemon.py 1184–1186、1391–1393 | 同步移除 |

#### A2. 删除续传游标（整链路）
| 目标 | 位置 | 说明 |
|---|---|---|
| `task_resume.py` | 整文件 | `TaskResumeCursor` / `UNIT_TRADE_DATE` / `UNIT_STOCK` / `TaskCancelled` / `next_trade_date_after` |
| `TaskResumeCursor` 注入/清除 | daemon.py 639–690、704–806 | `_task_resume` 生命周期 |
| `TaskCancelled` 停止语义 | daemon.py 652–654 | 协作式停止由其他机制承接 |

#### A3. 删除 mode 分支与增量循环（含 R3/R4 定夺）
| 目标 | 位置 | 说明 |
|---|---|---|
| mode 校验 | daemon.py 621–622、2206–2207 | `full_range/incremental` 白名单校验 |
| `_run_with_source` mode 分支 | daemon.py 905–916 | 恒走 `start_date/end_date`（原 `full_range` 语义成为唯一语义） |
| `_execute_task_per_trade_date` mode 分支 | daemon.py 1472–1478 | 非 mcp 死代码分支清理 |
| `_execute_task_per_stock` mode 分支 | daemon.py 1864–1870 | 非 mcp 死代码分支清理 |
| `_run_passthrough_task` mode 判断 | daemon.py 1419–1425 | passthrough 恒走全量 |
| `_run_incremental_cycle` | daemon.py 2250–2268 | 常驻增量循环移除 |
| `_bump_date` | daemon.py 3382–3404 | 水位+1（仅增量用，随之废弃） |
| `_date_range_empty` | daemon.py 3462–3468 | start>end 判空（仅增量跳过用，随之废弃） |
| **常驻循环调度** | **daemon_lifecycle.py 655、662** | R3：`task.get("mode","incremental") != "incremental": continue` 与 `execute_task(..., mode="incremental")` → 改为「所有 enabled 任务恒 full-range」，不再按 mode 过滤 |
| **GUI 单次采集入口** | **daemon_process.py 135** | R3：`start_once_subprocess(..., mode="incremental")` 移除 mode 参数 |
| GUI workers | workers.py 47–48、96–102、185–231、283–322 | `TaskWorker` mode 参数、常驻增量 worker、批量采集 mode |
| GUI tabs/main | task_tab.py、main_window.py | 全量/增量按钮与 mode 传递移除，仅保留「单次采集」 |

> **【R4 定夺】mode 的三处非拉取语义依赖**：
> 1. **失败判定 `has_usable_result`**（daemon.py 1814、2074）：`total_written[0] > 0 or (mode == "incremental" and fail_count[0] == 0)` → 删除 `mode == "incremental"` 子句，改为 `total_written[0] > 0`。**语义变更**：取消「增量无新数据=成功」场景（增量已移除，该场景不再存在）。列为独立语义变更项，验收单独核验。
> 2. **`_authority_reconcile` 触发条件**（daemon.py 2339–2347）：`rt_mode != "full_range"` 是 DELETE 前置条件。现 `collector_tasks.json` 中 0 任务声明 `authority_reconciliation`（已核，休眠）。**定夺：随之废弃**——去掉 mode 判断（mode 移除后条件恒真，但无任务启用故零行为变化），保留函数与 `config_lint.py:199–206` 的 `scope='full_range_only'` 校验（留盘休眠，不产生运行时影响）。
> 3. **per_date/per_stock 分支**（daemon.py 1473、1545、1865）：属 tushare/非 mcp 死代码路径（source 恒=mcp），随 mode 移除一并清理，不影响运行路径。

---

### Part B：完整移除 `etf_minutes` / `stock_minutes` 分钟数据支持

#### B1. 回测引擎与数据访问层
| 文件 | 位置 | 内容 |
|---|---|---|
| `quantstudio/backtest/backtest_engine.py` | 2331–2370、2372 | `_load_minute_snapshots`、`_run_minute_day`、`TABLE_EMPTY` |
| `quantstudio/backtest/providers/duckdb_data_access.py` | 1016–1233 | `_resolve_minute_table`、`query_minute_bars_by_range*`、`query_minute_bars_by_count` |
| `quantstudio/backtest/providers/duckdb_provider.py` | 69–169 | 分钟 bar 代理、`_raise_minute_capability_gap`、`get_snapshot` 分钟分支 |
| `quantstudio/backtest/providers/frequency_labels.py` | 3–5、36–38 | 分钟表 freq 标签、分钟错误码 |

> **【已裁定：保留占位提示】**：删除分钟回测实现，但 `backtest_engine` 保留 `engine_profile` 分钟档入口，运行时抛「分钟回测已移除/不支持」兼容提示（不静默降级为日线）。

#### B2. 数据管线（校验/审计/对齐/写入/巡检）
| 文件 | 位置 | 内容 |
|---|---|---|
| `quantstudio/pipeline/validator.py` | 264–310+ | 分钟频率网格 + 复权完整性（`stock_minutes/etf_minutes` 分支） |
| `quantstudio/pipeline/quality_audit.py` | 46–50、648–655、695、840、1093、1125 | `MINUTE_TABLES`、`_CALIBER_BLOCK_STOCK_MINUTES`、`_audit_minute_anchor_drift`、`_audit_caliber_drift` 分钟分支、`_clean_factor_segments`、`_audit_factor_monotonicity` |
| `quantstudio/pipeline/writers.py` | 149–174、712–715、795–798、1132–1164、1274–1289 | 两表 DDL、主键映射、`_upsert_pending_backfill_on_conn` 分钟约束、列清单 |
| `quantstudio/pipeline/aligner.py` | 274–280、415–437、470–476、523 | 分钟复权、`freq` 列、`_get_mapping` 分钟 fallback、ETF 3 位小数 |
| `quantstudio/pipeline/config_lint.py` | 36–42 | `_WRITER_PK_REFERENCE` 分钟表主键 |
| `quantstudio/pipeline/daemon.py` | 846–865、897–899、1042、1050–1052、1301–1302、1881–1882、1982–1986、3433–3437、3697–3700 | 分钟表权威源守卫、全市场路由、adj_factor 提取、`_failure_gate` 分钟阈值、CLI 注释（`_QFQ_PRICE_TABLES` 1204–1205 **不改**，归 QFQ 日线化子项目） |

#### B3. QFQ 复权管线（v4：**整体拆出，不在本次范围**）
> **【选项 B 拆项】**：以下 QFQ 引擎文件的分钟引用清理**全部归入 QFQ 日线化独立子项目**，本次**不删除、不改**（QFQ 编排器已禁用，这些文件不启动，零运行影响）：
> - `qfq_reanchor_schema.py`（62–65 `PRICE_TABLES`、110–111/245–247/604–605 `rows_stock_minutes` 列与 `STOCK/ETF` 分类）
> - `qfq_schema_contracts.py`（63/626–628/907–909 分钟 DDL 列）、`qfq_schema_status.py` / `qfq_schema_migration.py`（schema 指纹校验）
> - `qfq_resident_orchestrator.py`、`qfq_invariant.py`、`qfq_fresh_capture.py`
> - `qfq_staging_canary.py` / `qfq_formal_canary.py`（20/34 PRICE_TABLES）/ `qfq_formal_watermark_release.py`（52–54 硬编码任务名）/ `qfq_cutover_activation.py` / `qfq_orchestrator_cli.py`（261）

> **【风险 R-FP 调整】**：因本次不删除 QFQ 分钟 DDL，`qfq_schema_contracts.py` 的 schema 指纹**不变**，无需本次迁移重基线。该风险与指纹迁移**随 QFQ 日线化子项目**一并处理。

#### B4. 数据源适配层（R5 修正路径）
| 文件 | 内容 |
|---|---|
| `quantstudio/pipeline/sources/mcp_adapter.py` | `_MCP_SUPPORTED`（分钟 5 频段）、`_EXPORT_TABLES`、`_CANONICAL_TO_QUESTDB`、`_QFQ_CALIBER_TABLES`（`etf_minutes`）、`_QFQ_ADJFACTOR_TABLES`、`_EXPORT_MINUTE_WINDOW_DAYS`、`_EXPORT_ROW_ESTIMATE`、`_STREAMING_TABLES`、`_restore_qfq_if_required` 分钟分支、`get_last_date` |
| `quantstudio/pipeline/sources/base.py` | 114–116、125–128 docstring |
| `quantstudio/pipeline/sources/xtquant_adapter.py` | 38–50、348–350、380–383 分钟分支（死代码） |
| `quantstudio/pipeline/sources/tushare_adapter.py` | 23–28、178–181、226–227（死代码） |
| `quantstudio/pipeline/sources/baostock_adapter.py` | 61–63、165–166、337–339（死代码） |
| `quantstudio/pipeline/sources/astockdata_adapter.py` | 143–144（死代码） |
| `quantstudio/pipeline/sources/akshare_adapter.py` | 67–69、117–119、249–280（死代码） |
| `quantstudio/pipeline/source_capabilities.py` | 23–39 `KNOWN_TABLE_FREQS` 分钟频段（**路径修正**：`pipeline/source_capabilities.py`，非 `pipeline/sources/`） |
| `quantstudio/pipeline/mcp/client.py` | 1016–1019 分钟 row_limit 说明 |
| **`quantstudio/pipeline/exporter.py`** | **34–38、119** `1min/5min/15min/30min/60min → stock_minutes` 映射表（R5 补齐） |
| **`quantstudio/pipeline/fm_export.py`** | **45** `WATERMARK_TABLES = (..., "stock_minutes", ..., "etf_minutes")` 删两分钟表（R5 补齐） |

#### B5. 策略编译器
| 文件 | 内容 |
|---|---|
| `quantstudio/strategy_compiler/build_strategy_ir.py` | 203–208 `data_requirements.datasets` 分钟数据集校验 |
| `quantstudio/strategy_compiler/source_import.py` | 541–543 注释 |
| `quantstudio/strategy_compiler/examples/capability_report.example.json` | 32–34 示例文本 |

#### B6. GUI
| 文件 | 内容 |
|---|---|
| `quantstudio/gui/tabs/config_editor_tab.py` | 61–70、91–95、112–116、133–134 分钟表描述/分组 |
| `quantstudio/gui/tabs/quality_tab.py` | 42–44 注释 |
| `quantstudio/gui/tabs/task_tab.py` | 790–793 `supports_qfq` 分钟分支 |

#### B7. 配置
| 文件 | 内容 |
|---|---|
| `config/profiles/mcp_only/collector_tasks.json` | 69–76 `mcp_etf_minutes`、369–376 `mcp_stock_minutes` 任务删除；2716–2744 `qfq_orchestrator.enabled` 改为 `false`（`freqs=["1min"]` 惰性保留，归 QFQ 日线化子项目） |
| `config/profiles/mcp_only/alignment_rules.json` | 275、1747 两表 schema；2131、2394、2441、2702、2763、2806、2873、2936、2981、3269、3314 各源 column_map |
| `config/profiles/mcp_only/sources_config.json` | 无需改动（`qfq_orchestrator` 段不在此文件，见 collector_tasks.json） |
| `config/profiles/prehandover_staging/collector_tasks.json` | 同删两分钟任务 |
| `config/mcp_dataset_requirements.json` / `config/help.json` | 分钟表引用 |

#### B8. 脚本（`scripts/`）
重点验证/修复脚本（命中分钟表），核心清单：
- `scripts/etf_minute_reanchor.py`、`fix_minutes_pollution.py`、`purge_stock_minutes_cache.py`、`overwrite_minutes_from_cloud.py`、`dryrun_minutes_chain.py`、`restore_minutes_raw.py`、`restore_minutes_frontback.py`、`verify_mcp_qfq_restore.py`
- `scripts/acceptance/derive_minute_gate_threshold.py`、`verify_close_basis_direction.py`、`verify_unitchk_normalization_falsified.py`、`verify_quarantine_protection.py`、`verify_t1_window_equivalence.py`
- `scripts/frontfix_check_etfm.py`、`_verify_minutes_fix.py`、`_verify_front_fix.py`、`_verify_canary_front.py`、`_scan_front_corruption.py`、`probe_xtquant_minute_depth.py`、`preflight_raw_admission.py`、`preflight_raw_fullmarket.py` 等

#### B9. 测试（`tests/`，R5 修正口径）
- **62 个测试文件**命中 `etf_minutes|stock_minutes`（`tests/` 共 272 文件）；
- `test_*minute*.py` 共 **18 个**（含 `test_ptrade_api_minute_include.py` 等）；
- 需删除/改造：`test_minute_*.py`、`test_phase4_minute_*.py`、`test_qfq_*.py`（分钟部分）、`test_validator_behavior.py`、`test_quality_audit*.py`、`test_source_capabilities.py`、`test_index_daily_daemon_pipeline.py`（若涉 `get_last_date` 语义，见 A0 保留不改故不改此测试）等。

---

### Part C：禁用 QFQ 编排器（选项 B 独立立项，v4 修订）

> 用户裁定（2026-10-01）采纳**选项 B（独立立项）**：QFQ 编排器日线化改造（重写 `qfq_reanchor_engine.py` / `qfq_fresh_capture.py` 等，审计 P1–P5 揭示的 2–3 倍体量）**拆出为独立子项目**，另行立项审计。本次方案**只禁用、不改造** QFQ 编排器。

#### C1. 禁用 QFQ 编排器（配置级，零代码改动）
| 目标 | 位置 | 改动 |
|---|---|---|
| `qfq_orchestrator.enabled` | `config/profiles/mcp_only/collector_tasks.json` 2716–2744 | `true` → `false` |
| 水位推进语义 | daemon.py 477–506（`_advance_or_defer_watermark`） | `enabled=false` → `can_coordinate_watermark()` 返回 False → 走 `writer.advance_watermark` 直推（**已保留的 else 分支，零代码改动**） |

> **【惰性保留说明】**：`qfq_orchestrator.freqs=["1min"]` 与 `qfq_orchestrator_types.py:509` 的 `"1min"` 校验**本次不改**——`enabled=false` 下 `validate()` 仍通过（`freqs` 仍含 `"1min"`），且 orchestrator 不启动故 P1–P5 不触发。`freqs` 清理与校验改造**归入 QFQ 日线化子项目**。

#### C2. QFQ 相关分钟残留清理（N4 部分并入，属本次删除）
| 目标 | 位置 | 改动 |
|---|---|---|
| `_MINUTE_FREQS` | daemon.py 62 | 随分钟失败率阈值分支一并移除 |
| 分钟失败率阈值 | daemon.py 3433–3437 | `_failure_gate` 分钟表独立阈值分支移除 |
| `stale_alert_hours_by_freq."1min"` | daemon.py 3498（健康检查示例） | 移除 `1min` 条目 |
| `FREQ_MS` | quality_audit.py 49 | `MINUTE_TABLES` 删除后 `FREQ_MS` 的分钟键成为死代码，清理 |
| 分钟 freq 标签/错误码 | `backtest/providers/frequency_labels.py` | 分钟标签与 `TABLE_MISSING/TABLE_EMPTY/FREQ_NOT_IN_TABLE` 分钟分支清理 |

#### C3. GUI 导出页分钟选项（N3，属本次删除）
| 目标 | 位置 | 改动 |
|---|---|---|
| `cb_1min` / `cb_5min` 导出复选框 | `quantstudio/gui/tabs/export_tab.py` 126–132 | 移除分钟导出 UI，与 `exporter.py` 删除分钟映射保持一致 |

#### C4. 拆项登记（不在本次范围，独立子项目）
- **QFQ 编排器日线化**：`qfq_reanchor_engine.py` 的 `_canon_minute_freq`（310–315）/`_tables_of`（318–323）/`apply_reanchor_for_security` 分钟强制校验（2072–2097）、`qfq_fresh_capture.py` 的 `FreshCapture.capture` 分钟捕获（396–502）、`qfq_orchestrator_types.py` 的 `FreshCaptureRecord` 分钟字段（283–286）与 `freqs` 校验、`qfq_reanchor_schema.py` 的 `qfq_fresh_capture` 分钟列 DDL（415–425）、`qfq_resident_orchestrator.py` 的 `ASSET_PRICE_TABLES`（69–72）/`_security_range`（464–474）。
- 上述改造**另行立项审计**，与本次删除方案**拆分**；本次仅禁用、零 QFQ 引擎代码改动。

---

## 2. 影响面（关键风险）

| # | 影响 | 等级 | 说明 |
|---|---|---|---|
| I1 | **回测引擎失去分钟回测能力** | 高 | `engine_profile` 分钟档、`_run_minute_day` 不可用；保留占位入口抛「不支持」提示 |
| I2 | **采集管线失去增量续传** | 高 | A-β：每轮恒全量重拉（`start_date/end_date`），重复采集成本升高；**水位线保留但不再用于确定采集起点**（`enabled=false` 后亦不再作 QFQ gate 锚点，退化为被动记录） |
| I3 | **QFQ 四价格表缩减**（v4 拆项） | 中 | `PRICE_TABLES` 改 `{stock_daily, etf_daily}` **归入 QFQ 日线化子项目**，本次不改（QFQ 引擎禁用后惰性） |
| I4 | **数据源适配层死代码清理** | 低 | 非 MCP 源已停用，删其分钟分支不影响运行 |
| I5 | **测试套件大规模删减** | 中 | 62 个测试文件 / 18 个 `test_*minute*` 需删/改 |
| I6 | **客户交付物变化** | 高 | 若客户依赖分钟数据/分钟回测，属行为变更，须走客户通知流程 |
| I7 | **schema 指纹变化**（v4 拆项） | 中 | 本次不改 QFQ 分钟 DDL，指纹不变；该风险随 QFQ 日线化子项目处理 |
| I8 | **失败判定语义变化**（R4） | 中 | `has_usable_result` 删「增量无新数据=成功」子句，全量路径下「0 写入=失败」 |
| I9 | **QFQ gate 失效（水位直推）**（v4） | 高 | `qfq_orchestrator.enabled=false` 后 `hold_until_consistent` gate 不再生效，日线采集成功即直推水位；审计判定「可控」（QFQ gate 本就主要为分钟服务），但须显式披露 |
| I10 | **拆项遗留（`freqs=["1min"]` 惰性保留）**（v4） | 中 | `qfq_orchestrator.freqs` 与 `qfq_orchestrator_types.py` 的 `"1min"` 校验本次不改，`enabled=false` 下惰性保留，归 QFQ 日线化子项目清理 |
| I11 | **QFQ reanchor 引擎深度改造**（P1–P5） | 高 | 已拆出为独立子项目，不在本次范围；本次零 QFQ 引擎改动 |

---

## 3. 验收标准

1. **零残留增量机制**：全仓 `grep -rn "incremental\|UpdateDetector\|TaskResumeCursor\|_check_cloud_updates\|_run_incremental_cycle"` 生产路径无活动引用（`mode` 字段、A4、续传全部移除）。
2. **水位链路完整保留**：`grep -rn "_advance_or_defer_watermark\|qfq_watermark_intent\|advance_watermark\|get_last_date"` 生产路径仍活动且语义不变（`get_last_date` 多 source 共表返回「该源进度」不变，`test_index_daily_daemon_pipeline.py` 全绿）。
3. **零残留分钟表（非 QFQ 范围）**：全仓 `grep -rn "etf_minutes\|stock_minutes"`（`.py`/`.json`，**排除 docs/agent_workspace 归档 + QFQ 引擎文件（B3 拆项范围）**）命中为空或仅剩归档/「已移除」注释。
4. **导入无崩**：`python -c "import quantstudio.pipeline.daemon; import quantstudio.backtest.backtest_engine; import quantstudio.pipeline.validator; import quantstudio.pipeline.quality_audit; import quantstudio.pipeline.qfq_resident_orchestrator"` 全通过。
5. **ConfigLint 通过**：删除两分钟任务、`qfq_orchestrator.enabled=false`（`freqs` 惰性保留）后 `config_lint` 全绿。
6. **日线回测零回归**：6 策略（CANSLIM / fall_reversal / tech_etf_mvo_rotation / vol_regime_mom_rev / weekly_smallcap_growth / 周频小市值）日线档回测信号/成交/净值逐位一致。
7. **日线采集零回归**：`stock_daily`/`etf_daily`/`index_daily` 单次全量采集跑通，行数/列序/dtype 与删除前一致；水位经 `_advance_or_defer_watermark` 走 `writer.advance_watermark` 直推（`enabled=false` 分支）。
8. **分钟档占位提示生效**：`engine_profile` 分钟档运行时抛「不支持」提示，不静默降级、不崩溃。
9. **QFQ 编排器禁用生效**：`qfq_orchestrator.enabled=false` 下 `qfq_resident_orchestrator` 不启动（P1–P5 不触发），`can_coordinate_watermark()` 返回 False，水位直推正常；QFQ 引擎文件（B3）未被改动（`git diff` 确认）。
10. **剩余测试套件全绿**：`pytest`（剔除分钟相关用例后）零失败。
11. **文档同步**：`README.md`、`docs/strategy_toolbox.md`、`docs/prompt_engineering.md` 中涉及分钟数据/全量增量拉取的表述同步更新。

---

## 4. 回退条件

- 任一验收项不达标 → 立即回退该项，不议「差异很小」；
- 实施前 `git stash create -u -m "baseline-<时间戳>"` + `git stash store <hash>` 建立零副作用回退点并登记；
- 删除仅限本清单文件，禁止顺手重构其他逻辑；
- 每个逻辑分区独立 commit，可单项回退；
- **R1 红线守护**：若实施中发现 `_advance_or_defer_watermark` / `qfq_watermark_intent` 被误删或语义改变，无条件回退并重新归因；
- 推送前保留用户确认闸门（六步流水线步骤⑤）。

---

## 5. 残留引用与依赖清理（同步执行）

- `docs/` 引用文档（`strategy_toolbox.md` / `prompt_engineering.md` / `README.md` / `mcp_migration/*.md` / 各 design/evidence）中分钟数据与增量拉取表述标注「已移除」或删除；
- `config/help.json`、`config/mcp_dataset_requirements.json` 分钟条目删除；
- `.j2` 模板（`strategy_compiler/templates/*.j2`）中分钟相关注释/分支清理；
- `agent_workspace/` 归档目录**不动**（历史取证，非生产路径）。

---

## 6. 明确不在本次范围（硬边界）

- 不删除/清空已入库的分钟表数据（DuckDB 物理表，除非用户明确要求 DROP）；
- **不删除/不改** QFQ 水位协调链路（`_advance_or_defer_watermark` / `qfq_watermark_intent` / `advance_watermark` / `get_last_date`）——R1 修正后的硬边界；
- **不改 QFQ 引擎代码**（`qfq_reanchor_engine.py` / `qfq_fresh_capture.py` / `qfq_resident_orchestrator.py` / `qfq_orchestrator_types.py` 的 `freqs` 校验与 `FreshCaptureRecord` 分钟字段 / `qfq_reanchor_schema.py` 的 `qfq_fresh_capture` 分钟列 DDL）——这些归 QFQ 日线化独立子项目（选项 B 拆项）；
- 不动 `agent_workspace/`、`docs/evidence/` 历史取证文件；
- 不动其他会话在途未提交改动。**当前实测状态（审计 R6 更新）**：`config/profiles/mcp_only/collector_tasks.json`、`sources_config.json` 为 `M`；`data/snapshots/connection_semantics.json`、`lock_adoption_log.json`、`read_exemption_evidence.json`、`write_path_registry.json` 为 `D`（已删除）。本线只做精确文件清单叠加，不覆盖、不 `git add -A`。
