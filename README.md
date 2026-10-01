# QuantStudio

统一数据管线 + PTrade 兼容量化回测框架。

## 快速开始（客户上手）

> 下面是一段**可直接复制给智能体的完整部署提示词**。你不需要懂编程——把它整段发给任意 AI 编程助手，它会替你从 GitHub 拉代码、装依赖、配好 MCP 数据源凭证，并启动 PyQt 回测界面，最后教你在界面上跑回测。

把下面整段（从【部署提示词开始】到【部署提示词结束】）复制，连同你的任何额外需求，一起发给智能体：

```text
【部署提示词开始】

请你帮我在本机部署 QuantStudio 量化回测框架，并启动它的 PyQt 图形界面，让我能在界面上跑策略回测。我是计算机小白，请严格按以下顺序执行，并在每一步用大白话向我说明你在做什么、是否需要我提供信息：

0. 检查旧目录：在本机查找是否已存在 QuantStudio 项目目录（特征：目录内有 main_gui.py 与 quantstudio/ 子目录）。如已存在：向我报告路径，提醒我备份自写策略（quantstudio/backtest/strategies/）与回测结果（output/），经我确认后删除整个旧目录，再重新克隆，避免新旧代码混用。
1. 获取代码：从 GitHub 仓库 https://github.com/yangyizhu8/quantstudio 克隆到本机一个好找的目录（例如 D:/QuantStudio），然后进入该目录。
2. 安装依赖：在该目录下执行 `pip install -e ".[all]"` 安装全部依赖（纯 Python + 预编译包，无需本地编译）。如果遇到报错，用通俗语言告诉我原因和解决办法，不要跳过。
3. 配置 MCP 数据源凭证：本项目默认使用云端 MCP server 作为权威数据源（无需自备 Tushare token 或 miniQMT 客户端）。请向我询问 MCP API Key（项目方提供）。拿到后，把它写入 `config/secrets.env`（若文件不存在，先 `cp config/secrets.env.example config/secrets.env` 创建），格式为 `MCP_API_KEY=我的key`。说明：程序启动时会自动读取 `config/secrets.env`，无需我手动 `source` 或 `export`。
4. 防混源检查（首次全量拉取前必做）：检查 `data/` 目录下是否存在旧的 quantstudio.db、qfq_aux.db 数据库文件。存在则向我说明风险（新旧数据混库会导致复权基准不一致等隐蔽问题），经我确认后删除。全新克隆的仓库 `data/` 为空属正常。
5. 首次拉取数据：数据库无需预置大文件——首次启动采集后，数据会通过 MCP server 自动拉取入库到 `data/quantstudio.db`。在 GUI「采集任务」标签页点击「▶▶ 全部执行（全量）」即可（首次全量拉取行情表（日线+分钟线）约需 1-2 小时：云端 19 个月分钟线 ≈4.6 亿行经流式分片写入，内存峰值 ≤1GB，8GB 内存机器可跑；支持断点续传——中断后重跑只补缺失部分，GUI 采集界面显示分片进度）。
6. 开启常驻增量拉取（部署收尾）：全量拉取完成后，教我在 GUI「采集任务」标签页点击「进程常驻增量拉取」开关，开启后 7×24 自动增量拉取，无需再手动管数据。
7. 启动界面：运行 `python main_gui.py` 启动 PyQt 控制台。启动成功后，告诉我在界面里如何操作：进入「策略回测」模块 → 在策略文件栏选中一个策略 → 设置起止日期和初始资金 → 点击「运行回测」，即可可视化查看收益曲线、持仓和成交记录。

注意：整个过程你只负责把环境搭好并启动界面，**不要替我跑回测**；回测由我在界面上自己点。如果某一步卡住（例如 MCP Key 无效、网络失败、依赖装不上），请用通俗语言告诉我原因和下一步该怎么办。

【部署提示词结束】
```

要点（给智能体参考）：
- 依赖通过 pip 安装（纯 Python + 预编译 wheel，无需本地编译）。
- 默认数据源是 **MCP server**（云端权威源），客户只需一个 `MCP_API_KEY`，无需自备 Tushare token / miniQMT / 预置大数据库。
- `secrets.env` 在程序启动时**自动加载**，无需手动 `source` / `export`。
- 数据通过 MCP 首次拉取入库，无需单独获取 `quantstudio.db` 大文件。
- 传统多源（Tushare/xtquant）仍可在「数据源模式」切换后使用（需额外配置对应凭证），见下文「凭证配置」。

## 架构保证

- **强制统一入库链**：`adapter -> aligner -> validator -> writer -> watermark -> quality audit`，任何数据源和运行模式都不能绕过。
- **三种运行方式，同一实现**：全量、单次增量、常驻增量只改变日期范围，清洗、质量门禁和幂等写入完全相同。
- **配置驱动标准化**：代码、时间、字段、单位、频率、复权、PIT 和派生字段在数据层统一；策略不识别数据源。
- **策略完全解耦**：策略只写 PTrade 生命周期并调用注入的数据、指标和交易 API；provider/adapter/DuckDB 均位于策略边界以下。
- **底层集中修复**：数据或平台语义问题修复在 adapter/aligner/provider/PtradeAPI/engine，不要求修改具体策略。
- **PTrade 保真验证**：内置 PTrade 导出导入器和 L1-L4 fidelity comparator。
- **get_fundamentals 字段名映射（2026-08-24 P-D10）**：`growth_ability` 等财务表在转 PTrade 时自动完成本地→平台字段名映射（如 `or_yoy → operating_revenue_grow_rate`），策略仍按本地字段名书写；缺失字段触发 `QS_SHIM_FIELD_MISSING` 显性警报，不静默返回空。
- **get_fundamentals 平台可移植性 v8 系列（2026-08-31，框架层吸收，策略零改动）**：统一 wrapper（唯一 `get_fundamentals`，eps 常量烘焙防多模板覆盖）：`date+range` 走平台 range 多期 + multi2 拍平 + publ_date PIT（未披露 NaN 占位期剔除）+ end_date **epoch 毫秒**契约（对齐本地 `np.datetime64(int(end_date),'ms')` 消费）；平台 range publ_date 全空缺省容错；`report_types` 按 end_date 月日过滤；大池（>32）批量预取 SKIP 幂等（v8.8 起 range 形态改 3 码/组分组预取 + B6c 缓存命中，≈2.4 倍提速，行为等价）；平台缺列（total_share 等 8 字段）首调短路 + 全缺 1 行 NaN 契约（NaN 比较恒 False 不误加分）。复验轨迹：fscore_pass 166→35 + roa_removed 0→15 + total_share 0（九次平台复验收敛）。门禁：`scripts/run_contract_gate.py`（契约套件 + 矩阵哈希门）+ 契约矩阵 `docs/evidence/fundamentals-contract-matrix.yaml`（唯一事实源）。转换公共 helper（`_qs_g_obj` 等）无条件注入（2026-09-01 鲁棒性修复：行业/金融池段不再因fundamentals 段条件未注入而悬空）。**pctChg 字段平台可移植性 v1-v5（2026-09-01~02 五轮平台实证回归，框架层，策略源码零改动）**：①PTrade 合法字段无 `pctChg` → 无条件 `get_history_wrapper` **请求侧剔除 pctChg、返回侧由 close/preClose 合成**（同本地 aligner 口径，验收 maxdiff ≤1e-6）；②平台返回列=请求列 → 剔除后**自动注入 preclose 基列**（基础版+trade_date 门控版双版本同步）；③分钟频率平台不支持 preclose → 返回侧由**日线昨收合成 preClose**（fail-soft，返回已含则 guard 短路本地零影响）；④平台分钟对「security_list 列表+is_dict」返回空 → **is_dict 逐码路径**下沉基础 wrapper（裸字符串形态，D4-S7 R1 同构）；⑤`QS_MINUTE_DIAG` 分钟形状诊断（v5 支持 OrderedDict 形态打印 omap_keys）；⑥v7 分钟请求剥离 preclose（平台分钟含不支持字段→静默空返回，与日线抛错行为分叉）；⑦v7.1 版本自标识（diag 内嵌版本号）；⑧v9 QSPROBE 双形态探针。**known-limitation（2026-09-03 审核批准登记）**：平台回测分钟 include 双模式均不可安全表达"上一已完成 bar"（False=昨日 bar / True=含未来 bar，价格级实证）→ 打板买入路径回测验证降级，**平台验证以模拟盘为准**（实盘语义天然正确）；「data[code] 方案」已批准单独立项。**v10 data[code] 分钟 bar 捕获与合成（2026-09-03~04 立项六步闭环）**：handle_data 入口注入 `_qs_capture_data(context, data)` 双捕获（复用 D4-S7 #5 机械安全模式）→ wrapper 分钟路径**日期守卫**（bar 日期==current_dt 才合成；防御性）+ 合成行 **omit preClose**（平台 BarData.preclose 返回 0.0 伪值——第二十一轮三元组定谳的真根因，v10.2 曾穿透 g.prev_close 回退致 100% 假阳性，已切断）→ preClose 由 v3 日线昨收统一填充；`QS_MINUTE_SYNTH` 三元组审计（close/preclose/t 逐码）按日打点。**第二十二轮平台实证负向正确闭环**（四候选与本地权威行情逐码一致、零订单）+ **第二十三轮本地参照对照**（v10.4 产物×权威数据×平台语义 mock 逐日驱动：候选四码逐码一致、07-01 真封板 6 只落选归因 6/6=量能维度合法过滤、**正向注入探针 BUY 8200 股与平台真实订单逐位吻合=买入路径实证存活**）。E2E 引擎路径 duckdb 写锁 BLOCKED 登记为独立框架问题（F-DUCKDB-LOCK/F-LOCAL-MIN）。平台实跑验收：全市场 skip 消除 / 筛选链平台全通（sentiment/candidates/audit 正常）/ 速度 52 秒/交易日；CONTRACT GATE PASS ×4（含 6 策略 api_portability）。详见 `docs/evidence/pctchg-portability-20260901.md`、`docs/evidence/v10-delta-notes-20260903.md`、`docs/evidence/pctchg-minute-include-probe-archive-20260903.md`、`docs/evidence/framework-portability-v8-20260831.md`。
- **PTrade 保真模式（2026-08-24 P-A0/P-A1/P-A2，默认关闭）**：可选对齐开关，使本地回测在验证转换产物时与平台行为一致——`fidelity_ashares_snapshot`（A 股池用平台快照 parquet）、`fidelity_st_filter`（ST 过滤仅按平台 close<1 口径）、`fidelity_eps_basis`（eps 表请求按双端实证映射到平台 `basic_eps`/`diluted_eps`，本地 eps 语义锚；双端缺失档位显性报错，禁止静默单端 fallback）。默认全部关闭时本地是正确语义锚、不做任何平台迁就；产物默认逐字节不变（哈希级验收）。
- **eps 跨表回补（2026-08-25 P-A3，管线级免疫）**：fin_indicator.eps 与 income_statement.basic_eps 为同源复制列（历史对账 98.1% 精确相等）。MCP 源端回填错位造成 fin_indicator.eps NULL 而 income_statement.basic_eps 已有值时，写路径自动跨表回补（同 (code,end_date)、ann_date 取两表较大者=PIT 保守、新增 `backfill_eps_source` 打标列可审计、幂等可逆、无缺口库零行为）。质量门禁 `EpsBackfillGap` 每日防再发；消费侧 `_latest_by_code` 改为「最新有值行」口径（最新公告行 NULL 时回退上一有值报告期，对齐平台语义；次新上市前报告期因 income 无行保持 NULL——不造数据）。diluted_eps/or_yoy 因 source 无对应列显式排除，保持 NULL。详见 `docs/p-a3-eps-backfill-design.md`。

详细合规结果见 `docs/architecture-compliance-audit-20260720.md`。

## 安装

```bash
pip install -e ".[all]"
```

## 凭证配置（config/secrets.env）

程序在启动时（包导入 / 各入口）会**自动加载** `config/secrets.env` 到进程环境变量，无需手动 `source` 或 `export`。

1. 复制模板并填入真实凭证（文件不提交 git，含明文）：
   ```bash
   cp config/secrets.env.example config/secrets.env
   ```
   - `MCP_API_KEY`：**MCP server API Key（默认数据源必需）**。项目方提供，客户唯一必需凭证。配置后即可通过云端 MCP server 拉取全量行情/财务/基础数据，无需自备 Tushare token 或 miniQMT。
   - `TUSHARE_TOKEN`：Tushare Pro 付费 token（**仅切换到「传统多源」模式时需要**，MCP 默认模式下可留空）
   - `JQ_TOKEN`：聚宽 token（可选）
   - `QMT_PATH`：miniQMT 客户端目录（**仅传统模式 xtquant 数据源需要**，如 `D:/国金QMT/userdata_mini`）
   - `CUSTOM_API_KEY` / `ALERT_WEBHOOK`：自定义 API / 告警 webhook（可选）

2. `${ENV_VAR}` 占位符：`config/sources_config.json` 中可写 `${TUSHARE_TOKEN}`、`${QMT_PATH}` 等，
   运行时自动展开为对应环境变量的值；也可直接填实际值（如 QMT 直接填路径）。

3. 优先级：**进程已有环境变量 > secrets.env**。若你已 `export` 同名变量，或在测试中以
   `monkeypatch` 注入，则文件中的值不会覆盖它们。

> **数据源模式切换**：GUI 默认 MCP 模式（只需 `MCP_API_KEY`）。如需使用传统多源（Tushare/xtquant），在 GUI「数据源」模块切换到「传统多源」模式，并配置对应凭证。

## 数据采集

```bash
# 全量（使用任务 start_date/end_date）
python -m quantstudio.pipeline.daemon --mode once --pull-mode full_range --task kline_1d

# 增量（水位下一日 -> 当前日期）
python -m quantstudio.pipeline.daemon --mode once --pull-mode incremental --task kline_1d

# 常驻增量（每天按 daemon_schedule 执行）
python -m quantstudio.pipeline.daemon --mode forever
```

GUI 中的“全量拉取”“增量拉取”“进程常驻增量拉取”调用相同公共入口。

指数日线（F5）：`index_daily` 任务的正式动态宇宙（`get_index_daily_universe`）统一覆盖普通指数与 31 个 SW2021 L1 申万行业指数，full/incremental/resident 同一路径——tushare 普通指数走 `index_daily` 接口、申万指数走 `sw_daily` 正式接口，同一 canonical schema（股/元）；`industry_classification` / `industry_membership` 任务维护正式 SW2021 行业分类与 PIT 成员历史（tushare `index_classify`/`index_member`），旧 `sw_industry` 仅为审计快照。契约详见 `docs/data-pipeline-contract.md`。

### MCP 数据源与 is_qfq 还原（线1）

MCP cloud data stores qfq rather than canonical raw prices. To prevent double adjustment, `MCPAdapter` restores `raw_i = qfq_i ? adj_latest_global / adj_factor_i`. The anchor is the factor at the greatest timestamp in `qfq_aux.db` (`MAX(time)`), never the export-window tail and never historical `MAX(adj_factor)`. ETF split/consolidation can make factors decrease, so the historical maximum is not the latest anchor. The restored raw prices then follow the standard aligner `front = raw ? adj_i / adj_latest` path.

- 仅还原价格列（OHLC + pre_close），非价格列原样保留。**全部行统一走还原公式**（`is_qfq=False` 行并非真 raw，而是"写入时当时最新 adj_factor"算的旧基准前复权，直通会导致跨批次尺度断层），`is_qfq` 列只进 metadata 作追溯，不参与是否还原的决策。
- Missing factors or failure to synchronize the current batch factor snapshot is fail-fast. Do not classify `adj_factor_i > adj_latest_global` as stale: valid non-monotonic ETF history can be above the factor at the latest timestamp.
- **MCP collection routing (2026-08-04)**: QFQ restore is an explicit `(table, freq)` whitelist for stock/ETF K-line tables only. Non-QFQ export tables such as `index_constituents`, financial tables, and valuation tables never touch the factor snapshot. All non-export mapped and passthrough tables use verified `fetch_page` cursor pagination rather than `query_snapshot` (which has a 10,000-row server cap); metadata records `fetch_mode=export|fetch_page`.
- `index_constituents` keeps the canonical six-digit index/member-code contract. Out-of-contract `Hxxxxx.CSI` source indices are counted and recorded in metadata before alignment; composite code columns are normalized through explicit `code_cols`/`code_fields` rules. Nullable `NaT` dates are preserved as null and evaluated by the existing required/PIT gates instead of crashing the batch.
- **已知限制**：MCP 还原走云端因子系列（latest≈1.9495），tushare 系列≈1.9816，差 ~1.6%，故 MCP 路径 `*_front` 与 tushare 路径 front 不会 tick 一致（跨源比较须注意锚差异）；ETF 还原依赖 `fund_adj`，首次需冷启动灌库。
- 该还原是管线内部（adapter 侧）行为，策略注入 API（`get_history` 等）仍默认 `fq='pre'`，策略层无感。
- 细节：`docs/mcp_migration/is_qfq_restore-raw-task.md`、`docs/mcp_migration/mcp_protocol_probe.md` §7.4、验收 `docs/evidence/mcp_qfq_restore_verify_2026-08-03.md`。

## 策略回测

```python
from quantstudio.backtest.strategy_runner import StrategyRunner

runner = StrategyRunner()
engine, payload = runner.run(
    "my_ptrade_strategy.py",
    "2026-01-01",
    "2026-06-30",
    match_price_mode="close",
)
result, output_dir = payload
```

策略文件无需导入数据库或 provider，只实现 `initialize`、`before_trading_start`、`handle_data` 和可选 `after_trading_end`。

## 策略生态与外部贡献（2026-09-19）

策略生态开在**公开仓** [`yangyizhu8/quantstudio`](https://github.com/yangyizhu8/quantstudio)：
外部用户（含客户）可通过 **fork + Pull Request** 贡献策略，**变更范围严格限于**——

```
quantstudio/backtest/strategies/     （且仅 .py 与 .md）
```

**准入四道门**（前两道为该 PR 的 CI 机械门，后两道由维护者执行）：

| # | 门 | 实现 | 作用 |
|---|---|---|---|
| 1 | **路径门** | `.github/workflows/strategy-contrib-path-gate.yml` | 越界改动、**改名跨越目录**、**符号链接（git mode 120000）**、**子模块（160000）**一律拒绝；白名单正则 `^quantstudio/backtest/strategies/` |
| 2 | **静态扫描** | `.github/workflows/strategy-contrib-scan.yml` + `scripts/scan_contrib_strategy.py` | 禁用网络 / 进程 / 动态执行 / 文件 I/O / 内部模块；对**新增**策略另校验元信息头（`scripts/check_strategy_meta.py`） |
| 3 | **本地沙箱** | `scripts/run_contrib_sandbox.ps1` | 维护者执行：**只读影子库** + 隔离目录 + **断网三层降级**（防火墙 → 代理黑洞 → 告警）+ 硬超时与进程树清理，短窗试跑 |
| 4 | **人工审查** | — | 前视偏差、策略逻辑、风险控制 |

**为什么不用「目录级权限」？** GitHub **没有**目录级权限（CODEOWNERS 对已有 push 权限者
不生效）；Push rulesets（含 Restrict file paths）在**公开仓不可用**——仅 private/internal。
因此路径锁的执行点是**合入门**而非推送门：把上述两个 CI 检查设为 `main` 分支保护的
**required status checks**，越界 PR 即**无法合入**（外部贡献者本无 write 权限，
不存在可拦的推送动作）。

**贡献者入口**：见 [`CONTRIBUTING-STRATEGIES.md`](CONTRIBUTING-STRATEGIES.md) ——
含公开性确认、**plus 分发知情条款**（贡献将随客户分发包交付全体客户）、MIT 许可模板与免责、
元信息头模板、以及首次贡献者需维护者批准运行 CI 的说明。

**维护者推送前**：除既有三查（领先清单 / `ls-remote` 双远程 / 闸门状态）外，须增加
**第四查**——`git fetch eco` 并确认 `eco/main` 未领先本地。因为贡献合入公开仓后该仓会领先，
而本地对它是 **push-only**，直接双推会被**非快进拒绝**。详见
`docs/ops-verification-conventions.md` 的 **C7 第四查**。

**边界声明**：策略贡献**只落在公开仓的 `strategies/` 目录**，不触碰任何框架代码；
合入策略**不自动进入实盘**，实盘需另经独立集成流程。

## 策略工具箱（PTrade 兼容 API + QuantStudio 本地扩展）

写策略 / 移植 PTrade 策略时，可直接使用的**全部生命周期回调、注入式 API 函数、MyTT 指标库与 A股交易规则**，详见 **[`docs/strategy_toolbox.md`](docs/strategy_toolbox.md)**。其中 `get_etf_list()` 保持 PTrade 同名契约且禁止用于回测动态池；本地单端策略可使用 `get_etf_list_local(query_date=None, etf_type="equity", active_only=True)`，该接口经 ReferenceDataProvider → DuckDB 数据适配层按 `etf_basic` + `etf_daily` 做 PIT 查询。

要点：
- 运行时依赖分层：QuantStudio 本地会注入 API、`g`/`log` 以及 `np`/`pd`；真实 PTrade 不注入 `numpy`/`pandas` 别名，因此双端/PTrade 源码使用它们时必须显式写 `import numpy as np` / `import pandas as pd`。数据库驱动、框架内部模块和直接文件 I/O 仍被禁止。
- `log` 对象兼容 **printf 风格多参**：`log.info("信号=%s 条数=%d", src, n)` 按 `%` 风格格式化；双端/PTrade 可移植代码仅使用 `debug/info/warning/error/critical`，`log.warn(...)` 会被 Validator 阻断。
- 生命周期分层：PTrade 可移植回调为 `initialize`（必需）+ `before_trading_start` / `handle_data` / `after_trading_end`（可选）；`set_backtest()` 与 `is_trade()` 仅属 QuantStudio 本地扩展，双端/PTrade 代码会被 Validator 阻断。
- PTrade Profile 1.9.0 对股票双端常用 API 登记精确签名：`set_benchmark`、`run_daily`、`get_Ashares`、`get_index_stocks`、`get_stock_status`、`get_positions`、`get_position`、`get_trade_days`、`get_fundamentals`、`get_industry`。双端设计或源码调用未登记的顶层注入 API 时默认 `BLOCK`，不得用"近似确认"绕过。
- **get_index_day_bar（2026-09-08 新增本地注入 API）**：已完成指数日线读数（独占 index_daily 路由，杜绝 000001 指数/平安银行同码串数据；profile-aware 上界：daily 含 T、minute 永不含 T、proxy 仅 15:00 时钟含 T）；count ∈ [1,250] 越界 ValueError；校验器 PREOPEN-INDEX-BAR / MINUTE-PROFILE-INDEX-BAR 双 BLOCK；已登记 local_only_symbols，PTrade 转换 fail-closed BLOCKED（重写映射+平台探针为 D4 后续项）。适用于同日指数状态读数的本地事件策略。详见 docs/get-index-day-bar-design.md。 转换门禁（2026-09-08）：转 PTrade fail-closed BLOCK，显式失败不生成静默空仓产物（docs/conversion-local-only-gate-design.md）。 2026-09-09 增强：可信 design 元数据自动解析（docs/design-metadata-auto-profile-design.md v4）——转换时按 agent_workspace 可信链（ledger 路径+SHA+design schema）自动带出 engine_profile，skill 生成策略无需手动选择引擎周期；可信 design profile 权威，显式冲突 BLOCK；legacy 无 design 可手工指定。
- PTrade Profile 1.8.0 登记 `get_history(..., is_dict=True)` 返回形状契约：mapping item 可能是 pandas DataFrame / NumPy structured array / recarray。提取字段必须先 `np.asarray(...)` 归一化再参与数值计算；对 history item 的无保护 `.values`/`.iloc`/`.to_numpy()` 等 pandas 专属访问会被 Validator 阻断，并须通过 agent-first 运行时形状 fixture（`validate_runtime_shapes.py`）。
- **PTrade 平台差异吸收矩阵（2026-08-22，框架/转换管线层吸收，策略层零平台知识）**：① 市价单单笔上限（创业板/科创板 50,000 股，沪深主板 ≥86,900；超限整单取消）→ 转换管线自动拆单（>49,000 股拆多笔，`_qs_split_order`，阈值常量 `_QS_MAX_ORDER_SHARES=49000`）；② `current_price` 平台不可用（非真实 PTrade API，模块加载期引用即 NameError）→ 统一链 ① 前收(框架缓存) → ③ get_history 兜底（PTrade 侧）；本地 QuantStudio 侧另有 ② 原 API 语义；③ `get_trade_days` 全量日历/格式混用 → 转换侧归一 'YYYY-MM-DD' + 未来日期过滤；④ `get_stock_info(listed_date)` 格式 → 转换侧归一。策略代码**禁止手写平台兜底**（`def _normalize_date_str`/`def _current_raw_price`/`g.last_close` 自维护 → Validator `PTRADE-PLATFORM-FALLBACK-BAN` BLOCK）；费用已实证平台最低佣金 ≈5 元/笔（与本地同构，拆单双端对称）。详见 [`docs/source_import-ptrade-order-split-design.md`](docs/source_import-ptrade-order-split-design.md) 与登记表 D4-S2/D4-S3/D4-S4。
- **平台吸收 2026-09-03（get_Ashares exclude_bse / set_commission 值域，框架层，策略零改动）**：① `get_Ashares(exclude_bse=…)` 的 `exclude_bse` 为本地扩展参数（平台仅接受 `date`，真实 TypeError 实证）→ 转换管线调用点剥离 kwarg，北交所过滤按**源语义**烘焙 `_QS_EXCLUDE_BSE`（源常量/模块常量解析；动态表达式回退 CLI + WARN 不掩差异）；② `set_commission` 平台 arg_checker 要求佣金费率/最低佣金均 >0（IQInvalidArgument 实证）→ 常量 ≤0 吸收为可表达下限（min_commission 0.01 / commission_ratio 1e-6）、动态表达式包装 `max(expr, floor)`，仅改 kwarg 值区间（无关调用逐字节不变）；校验器新增 `PORTABILITY-COMMISSION-MIN-ZERO/POSITIONAL` 与 `PORTABILITY-ASHARES-EXCLUDE_BSE` 防复发（本地专用策略不受限）。设计/证据：`docs/strategy-compiler/ptrade-platform-absorptions-design.md`、`docs/evidence/ptrade-platform-absorptions-20260903.md`。
- **平台吸收 2026-09-04（get_fundamentals 平台形态六项，框架层 wrapper，策略零改动，九轮平台复验定谳）**：① `date=None` 白名单财务表 → 前日 PIT 拼接（`QS_GF_DATE_SYNTH`，平台 date=None 实测空返）；② list 单调用码数上限 → ≤500 自递归分块（`_QS_GF_LIST_CHUNK`；500 码 P-D10 实证、800 码空返实证）；③ 报表表（income/balance/cashflow）平台 date 形态=披露时点**单期**语义 → 未显式传参自动 range 路由 5 年窗口（`QS_GF_STATEMENT_RANGE`，多期 CAGR/ROE 必需；显式传参零影响）；④ range PIT `publ_date` epoch 毫秒数值优先归一 + `report_types` 报告期 +8h UTC 偏移（两环境形态自适应）；⑤ **valuation 表列名双环境相反**（平台=聚源短名 `pe_ttm/pb/ps/pcf/turnover_rate/total_shares`，无 `pe_ratio/turnover_ratio/pb_ratio`）→ 运行时 `QS_VAL_MODE` 列名探针判型（每回测一次，g 缓存）才启用映射 + `turnover_ratio↔turnover_rate` 直映 + 兜底合成（vol×close/float_value×100 中值量级自校准）；⑥ `validate_local_strategy` LOCAL-API-WHITELIST 改全深度收集（注入模板嵌套 def/嵌套 import 不再误 BLOCK）。证据链（逐轮平台日志+本地强制判型 harness 数值校准）：`docs/evidence/ptrade-platform-absorptions-20260903.md` §11-18；设计：`docs/strategy-compiler/gf-date-synthesis-design.md`。**教训登记**：诊断探针输出截断参与推断致 §17 误判（§18.2/18.5 修正——审计行须完整证据）。
- `get_stock_status` 的可移植 `query_type` 仅为 `ST` / `HALT` / `DELISTING`；`DELISTING_SORTING` 仅是 `filter_stock_by_status` 的过滤类型及本地向后兼容别名。
- 数据 100% 来自 DuckDB（QuantStudio 数据管线产出），策略禁止直连数据库（强制隔离）。
- 框架取数（注入 API 与底层数据适配层）默认前复权（`fq='pre'`）：策略不传 `fq` 即获得前复权价；需不复权请显式 `fq=None`。**日线日期区间路径（`start_date`/`end_date`）与 count 路径（`count`）前复权行为一致**——同一代码、同一区间、同一 fq 返回逐值一致的前复权价（R1-A 已修复区间路径曾错误返回 raw 价的缺陷）。
- **分钟取数性能（Phase 4，内部实现优化，行为逐位等价）**：分钟 `get_bars_by_count` 走单次批量 SQL（一次 SQL per 表）再按 code 分组取 `tail(count)`；分钟 Profile 下 `get_history(frequency='1m', include=True)` 优先从引擎注入的**当日全量分钟历史**内存切片（零 DB 往返），PIT 截断仍由 `bar_cutoff_ms`（当前 bar 时间戳）保证；请求 code 不在当日缓存时补查 SQL，异常语义与逐只路径一致。已通过优化前后 23 交易日回测逐位相等（容差 0）验证。详见 [`docs/profiling_report_smallcap_phase4_20260812.md`](docs/profiling_report_smallcap_phase4_20260812.md)。
- **撮合/估值 raw 口径、取数链路前复权（2026-08-14 PTrade 实证对齐）**：引擎每日全市场快照（`query_daily_snapshot`，成交价、持仓估值、`data[code].price`、涨跌停比较价的唯一来源）OHLC 使用**原始价（raw，不复权）**——PTrade 平台实证确认撮合价 = raw close（日线 5/5、分钟 6/6 精确匹配）、估值 last_price = raw close。`preClose` 为行情源除权参考价语义（除权日 = 前收 × 复权因子），`(close-preClose)/preClose` 在所有日期均正确。**信号取数链路独立保持前复权**：`get_history`/`get_price` 默认 `fq='pre'` 走 `*_front` 列，技术指标不受除权跳价影响；两条链路 SQL 独立（`query_daily_snapshot` vs `query_bars_by_range`），互不影响。`pctChg`/`volume`/`amount` 保持原始口径（`pctChg` 已是复权校正后的真实涨跌幅）。**ETF 除权补正（2026-08-16，引擎方案 v2-final）**：`stock_dividend` 无 ETF 记录，ETF 除权日送股/份额折算/合并由引擎 `_apply_factor_derived_split` 用 preClose 反推补正（`ratio = prev_close / preClose`，**仅 ETF，股票零侵入**）：`ratio≥1.10` 吸附 0.5 倍数送股（未吸附按原值 + WARN）、`1.01~1.10` 现金分红带跳过 + WARN（阶段 2 由 etf_dividend 精确入账）、`<0.99` 份额合并对称处理、`0.99~1.01` 非除权跳过；已处理标的按裸码比对跳过（不重复送股）。**ETF 现金分红入账（2026-08-16，阶段 2）**：引擎 `_apply_etf_cash_dividends` 读 `etf_dividend` 表（tushare fund_div），除息日 `cash += volume × div_cash × 0.80` **按税前 0.8 入账**（**2026-08-16 PTrade 实测**：平台对 ETF 分红与股票统一扣 20%，600000 11500×0.42×0.8=3864.00、510500 10800×0.149×0.8=1287.36 与平台现金增量逐分吻合；公募税法免税与平台实现不一致，回测以平台实测为准，`tax_policy='etf_pre_tax_x_0.80'`）；与送股反推不互斥（同日分红+送股同时发生）；表未落地 → no-op（缺口由现金分红带 WARN 兜底检测）。
- **ETF T+0/T+1 按代码分类执行（2026-08-16，per-code，仅 `minute-bar-v1` + `--etf-t0 true` 生效）**：`etf_basic.fund_type ∈ {qdii, gold, commodity, bond, money}` → 当日买入即时解锁可卖（T+0）；`equity` → T+1（当日新买 `can_sell=0`，卖出成交 0 股，次日盘前解锁）；未知代码（LOF 等）fail-closed 按 T+1。分类缓存经 provider 层装载（仅 minute+true 触发查询；daily/false 零查询零 warning）；查询失败 → 全 T+1 + warning。**行为变更（带理由）**：`--etf-t0 true` 语义由"全部 ETF T+0"重定义为"按分类"——旧语义允许实盘不存在的国内股票型 ETF 同日买卖，CLI 侧旧模式不可达；GUI 布尔 `etf_t0` 默认 False=全部T+1（与 CLI 默认一致、双端统一），勾选 True=按分类；"全部T+0 研究模式"未实现（见 `docs/etf-t0-per-code-design.md` §13 T2 降级说明）。**平台差异**（2026-08-15 PTrade 探针实测）：520830/LOF 平台回测按 T+1 拒单（真实规则 T+0）、513100 平台分钟 bar 零量无法成交（本地零量 bar 仍成交 = 已知撮合近似）——策略用"触发即锁→尝试卖出→成交 0 股→次日顺延"拒绝处理模式自动吸收（不查类别、不读订单返回字段）；**skill 生成的 PTrade 可移植策略禁止读取 `order()` 返回值的 `.status`/`.reason`**（Validator `ORDER-RETURN-FIELD-READ` BLOCK），本地专用策略不受限。设计/证据：`docs/etf-t0-per-code-design.md`、`docs/evidence/etf-t0-g2-regression-20260816.md`。
- **成交额列双端契约（B1，返回端逆映射）**：DB 物理列为 `amount`，Ptrade 官方契约列名为 `money`。`get_history`/`get_price` 返回的 DataFrame 在含 `amount` 列时**同步追加同值 `money` 列**（追加在列尾，`amount` 保留、数值不变）；请求 `fields=['money']` 时也正确返回 `money` 列。**双端/PTrade 目标策略必须只读 `money`**（读 `amount`/`close_front` 等本地物理列名会被 Validator 以 `PTRADE-LOCAL-COLUMN` 规则阻断）；本地单端策略读 `amount` 仍兼容。该映射为纯返回端别名，黄金回归证明对回测数值零影响。
- **Agent-first 唯一价格契约**：`market_data_contract.signal_price_adjustment="pre"`（信号 OHLC 前复权）且 `execution_price_basis="raw_trade_price"`（撮合、现金、成交、持仓估值、`data[code].price`、BarData OHLC 用原始价——PTrade 实证对齐）。`pre_adjusted_price` 已从新设计 Schema 中移除；旧前复权执行契约设计必须回到 R2 迁移并重新通过 R2.5/R4/R5。
- **统一证券元数据（F2）**：`get_stock_info` 股票行为与历史完全一致（名称=裸码、上市日=行情首根K线），仅扩展 ETF（真实名称、`stock_type='etf'`、上市/退市日 `YYYY-MM-DD`，fallback 显式标记）；未知代码保持兼容空值。本地 ETF 元数据支持 ≠ PTrade 真实 ETF 支持（未验证）。
- **指数成分严格 PIT（F3）**：`get_index_stocks(date)` 只取不晚于该日的最近 `complete` 快照——非历史并集、无未来泄漏、无快照返回空；完整性由 `index_constituents_snapshot_meta` 批次契约（expected_count/status）在打点判定，不依赖未来数据；回测中不传 date 自动用当前回测日期。
- **行业归属 APPROXIMATION_REQUIRES_CONFIRMATION（F4，非 PIT READY）**：`get_industry` 按当前回测日期 as-of 查正式 SW2021 成员表（`industry_classification`/`industry_membership`）。**关键语义边界**：官方 `index_member` 仅给 `in_date`/`out_date`，无冲突裁决规则；canonical 表**原样保留重叠区间**（如 SW2021 重新分类），**不应用自定义“生效日较新者胜”裁决**，每日唯一门控不再要求为 0。重叠命中时 `get_industry` 抛 `ReferenceDataCapabilityError`（fail-closed），绝不返回任意自定义裁决近似；能力标注 APPROXIMATION_REQUIRES_CONFIRMATION（因 canonical 表原样保留重叠区间、非 PIT READY，但运行时重叠一律 fail-closed）。无有效历史归属返回 `None`；正式表缺失 fail-closed；旧 `sw_industry` 仅为审计快照。
- **申万行业指数日线（F5）**：31 个 SW2021 L1 行业指数与普通指数统一入 `index_daily`（tushare `sw_daily` 路由，股/元单位）；`get_history` 对 801xxx 走 index_daily，`fq='pre'` 回退原始 OHLC。
- **调仓模式（F1）**：PyQt 回测面板透出 `rebalance_mode`（默认 `legacy`；`callback_basket` 仅 daily-bar-v1+next_open，导出 `engine_semantics_version=0.4.0-next_open_basket`，close/open 被 GUI 阻断；`run_daily` 订单永不进入 basket）。
- **回测审计行（计划 vs 实际，2026-08-17）**：策略层 `QS_REBALANCE_AUDIT`/`QS_PORTFOLIO_AUDIT`（计划）由策略打印；引擎层日末新增 `QS_FILL_AUDIT`（实际成交：`sell_filled/buy_filled/sell_rejected/buy_rejected/positions_total/rejected_detail`，有拒单 WARNING）。全部拒单路径集中采集（no_price / 涨跌停 / halted / 资金不足/整手不足），`no_price` 等拒单从此可见（原零日志）。回测后对照两条审计行即可发现"策略想买 5 只、实际只成交 3 只"类静默扭曲，并可与 PTrade 日志逐日机械对齐。详见 `docs/strategy_toolbox.md` §3.7.2 与 `docs/backtest-align-diagnosability-design.md`。
- 读取**外部文件数据**（研报 CSV、信号表等）必须经由框架注入 API，例如 `load_research_signals(csv_path, fallback=...)`，文件 I/O 逻辑下沉到框架侧 `ptrade_api`；策略内直接 `open()` / `read_csv()` 会被 `StrategyIsolationGuard` 静态拦截并抛 `StrategyIsolationError`。

> **让 AI 帮你写策略**：想把策略需求交给其他智能体、自动产出可运行策略并落入 GUI 可选目录？参见 **[`docs/prompt_engineering.md`](docs/prompt_engineering.md)**。Agent-first 流程会在 R0 先根据你的提示词生成一张策略工作流图（mermaid）供你审核，确认结构后再给出语义矛盾表与审核表——提示词未定义的分支会以「❓待定」节点标出并进入确认清单；随后再分别确认目标平台，以及R5由Agent执行还是由用户在PyQt执行；双端 ETF 策略固化用户确认的静态白名单，本地单端 ETF 策略才允许动态调用 `get_etf_list_local()`。**中文命名契约（2026-08-22）**：skill 生成的本地回测策略一律采用中文策略名——`strategy_name` 即发布文件名（至少一个汉字、文件名安全：不以 `_`/空白开头、不以 `.`/空白结尾、不含 `\ / : * ? " < > |`、≤50 字符，且与现存策略文件 stem 不得冲突），`strategy_id` 保持 ASCII 内部标识。用户PyQt模式在R4后只生成 `quantstudio/backtest/strategies/<strategy_name>__candidate_quantstudio.py`，用户自行选择回测日期并提交日志；R5 证据 2.0 要求绑定真实回测产物（`config.csv`/`daily_stats.csv`/`trades.csv`/运行日志及各自 SHA-256），由 review 脚本自动解析实际本金、持仓部署与拒单计数——"回测跑完无异常"不再是 PASS 依据；证据PASS后，R5.5 统计鲁棒性验证（2026-08-28，skill 0.9.0）对已验三件套运行 WF 5 折 + Monte Carlo n=1000 + G1-G6 门控（年化/回撤/夏普/胜率/正窗口/统计显著性，阈值照 simple-quant-factory 原版；入口哈希预验 fail-closed；折内零交易折不入正窗口分母；迭代最多 2 轮、第 3 轮 FAIL 终止 ROBUSTNESS_FAILED；豁免需 verbatim 确认），门控全过才进入 R6生成正式文件并删除临时候选文件。R5.4 参数寻优（2026-08-28，skill 1.0.0，可选）：design 2.3 `parameter_optimization_contract` 显式授权（R2.5 verbatim 确认搜索空间与成本公式）后，在授权空间内做嵌套 WF 网格/Optuna 寻优（内搜外验防过拟合、预算双熔断、多数票聚合产出提案），提案须经客户 verbatim 再确认并重走 R3→R4→R5→R5.5 才可发布；禁用时管线行为与既往完全一致。默认正式输出分别为 `quantstudio/backtest/strategies/<strategy_name>.py`（中文名）与（仅双端）`ptrade/<strategy_id>_ptrade.py`。
>
> ⚠️ **AI 生成策略同样受 `StrategyIsolationGuard` 约束**：自动生成的策略代码**不得包含 `open()` / `read_csv()` 等直接文件 I/O**，也不得 import 框架内部模块；需要外部数据（如研报/信号 CSV）时，必须调用框架注入的 `load_research_signals` 等 API，否则加载即报 `StrategyIsolationError`。提示词中应明确该约束。

## 质量与对齐

```bash
python -m quantstudio.pipeline.quality_audit
python -m pytest -q
```

PTrade 对照：

```bash
python -m quantstudio.backtest.run_ptrade_strategy strategy.py 2026-01-01 2026-06-30 \
  --match-price close --compare --ptrade-dir <Ptrade导出目录> --output output/compare.json
```

---

## QFQ 重锚引擎（ratio / fresh_staged / fresh_authoritative_rebase 三模型）

> 模块：`quantstudio.pipeline.qfq_reanchor_engine`。负责把日线/分钟前复权（QFQ）价格
> 从「比例锚（ratio）」修正升级到「fresh xtquant 逐值写入（fresh_staged）」和
> 「权威全历史重基准（fresh_authoritative_rebase）」。
> 本文档仅记语义边界，完整 API 见 [`docs/strategy_toolbox.md`](docs/strategy_toolbox.md) 第 4 节。

### 三模型选择语义（铁律）

`apply_reanchor_for_security(conn, *, asset_type, code, fresh_daily, calendar, ...,
model="ratio", model_reason=None, fresh_minutes=None, fresh_source=None,
fresh_capture_id=None, fresh_metadata_sha256=None)`：

- **`model` 必须显式传入**，引擎内**不存在**任何 BLOCK 后静默回退路径。
- **`ratio`**（默认）：方法 B + 方法 A 黄金抽验，原行为逐位不变；**禁止**传 `fresh_minutes`（防呆）。
- **`fresh_staged`**：fresh xtquant 分钟前复权逐值写入。必须提供 `fresh_minutes`
  （列 = `code/time/freq?` + OHLC raw + 四 `*_front`；多 freq 须含 `freq` 列），且
  **`model_reason` 必填**（写入事件审计，留痕为何切换模型）。
- **`fresh_authoritative_rebase`**（新增）：权威全历史重基准。将 frozen xtquant front
  作为权威 oracle，全历史覆盖 + raw 逐 bar 对齐 + 写后精确一致 + capture 不可变契约。
  **移除**理想化乘法/加法 precheck 假设（真实 xtquant 前复权不严格满足纯乘法或纯加法，
  旧假设会导致真实除权场景 BLOCK）。信任边界：框架不独立证明 oracle 的经济复权语义
  正确性（需独立 oracle，属未来研究）。详见
  [`docs/superpowers/specs/2026-07-29-fresh-authoritative-rebase-design.md`](docs/superpowers/specs/2026-07-29-fresh-authoritative-rebase-design.md)。
- **`allow_partial_minute`（D 方案，2026-08-02 批准）**：`fresh_authoritative_rebase` /
  resident 重锚调用可传 `allow_partial_minute=True`。当库内分钟历史缺失（`fresh ⊃ target`，
  即 fresh 多出历史行、库内已有行全部与 fresh 对齐）时，不再 BLOCK，而是降级为
  **partial deferred**：仅对共有区间 UPDATE 分钟 `*_front`（不 INSERT 新行，契约不变），
  审计写 `minute_front_coverage='partial'` 标记历史 front 不完整。日线 rebase 行为完全不变。
  此路径用于分钟采集缺口（`collector_tasks.json` 的 1m 任务 `start_date` 偏晚导致库内分钟
  历史不全）下的全市场日线 rebase 推进；分钟历史完整回填后（C 方案）应撤除 partial 走全量。
- 切换模型必须由调用方显式改写 `model` 并给出书面 `model_reason`，**禁止静默切换**。

### 事务与四态事件审计

单证券调用在一个显式连接上完成，成功与失败路径审计如下（全部带 `model` /
`model_reason` / `model_audit` 键，含 `fresh_source`、`fresh_capture_id`、
`metadata_sha256`、`tick_size`、`freqs`、`minute_coverage` / precheck 摘要）：

| status | 含义 | 事务 |
|--------|------|------|
| `committed` | 成功 | anchor 推进 + 价格修正 **同一事务** |
| `blocked` | 方法 B/A 或数据契约失败 | 回滚 + 独立短事务 `blocked` 事件 |
| `rolled_back` | postcheck 失败 | 回滚 + 独立短事务 `rolled_back` 事件 |
| `failed` | 其它异常 | 记录 `failed` 事件后**重新抛出** |

三种失败路径都**绝不推进 anchor**，绝不污染已提交数据。

### 纵深防御 postcheck（COMMIT 前硬门禁）

`minute_staged_match`（精确）> `scale_consistency`（容差）> `minute_tick_error`
（≤1 tick）> `minute_raw_match`（eps=1e-9 最精确）。其中：

- `minute_raw_match` 先显式拦截 raw 一侧 `IS NULL OR NOT isfinite() OR <=0`
  （SQL 三值逻辑陷阱：`ABS(NULL-x)>eps` 结果为 NULL，WHERE 按非真过滤会**静默漏检**），
  再比逐 bar abs diff；`n_invalid>0` 直接抛 `minute_raw_match`。
- `minute_tick_error` 同样先拦截 NULL/NaN/Inf/<=0，再比 `ABS(diff) <= tick_size`。

### tick_size 资产路由（第六轮阻断 4）

`tick_size` **不能写死 0.01**，须按资产/市场路由：`resolve_tick_size(asset_type, tol)`
= `STOCK=0.01` / `ETF=0.001`；显式 `tol.tick_size` 可覆盖；未知资产抛异常。
事件 `model_audit.tick_size` 记录实际使用值。

### 交易日历校验（第六轮阻断 2）

`stage_fresh_minutes` 对**每个**自然日调用 `CalendarService.is_trading_day` 校验：
周末或非开市日整券 BLOCK（`fresh_minutes_non_trading_day`）；日历 provider 未覆盖的
未知日 BLOCK（`fresh_minutes_unknown_day`）。钟面时刻合法（如周六 09:31）≠ 自然日开市，
必须逐日校验。`calendar=None` 直接抛 `ValueError`。

回测区间查询不到交易日时仍抛 `ValueError("No trading days...")`，异常类型与成功路径契约不变；
错误消息会进一步区分 DuckDB 文件/连接失败与 `stock_daily` 数据范围不覆盖，并显示数据库路径、
文件存在性或实际日期范围。连接失败时先关闭占用数据库的 daemon/其它 GUI 实例并核对
`config/data_config.json`；范围缺失时在「采集任务」Tab 补采对应区间后重试。诊断不可用或异常时
自动退化为原始单行错误消息。

### 主动因子刷新与 detector degraded（常驻编排器）

> 模块：`quantstudio.pipeline.qfq_factor_refresh`（`QFQFactorRefresher`）+
> `quantstudio.pipeline.qfq_maintenance`（`resolve_ts_codes`）+
> `quantstudio.pipeline.aligner`（`raw_to_tushare_ts_code`）。

常驻 QFQ 编排器在事件发现之前主动刷新股票 `adj_factor` 与 ETF `fund_adj`（分别写
独立 SQLite 表），避免把陈旧因子快照误解释为"今天没有事件"。

- **默认关闭**：`qfq_orchestrator.factor_refresh_enabled` 默认 `False`（独立 opt-in）。
  主动因子刷新默认关闭。生产启用前必须通过股票/ETF ts_code 转换、刷新失败降级、
  水位 hold 和全量回归测试，并取得用户明确部署确认。
- **degraded 契约**：某资产类别**全部逐码请求失败**（`fetch_adj_factor` 抛
  `FactorRefreshError`）→ `degraded=True` → daemon 四价格表水位强制 hold、
  `qfq_cycle_run.detector_degraded=1`。**正常返回空数据不降级**（区间内无复权事件，
  返回 0 行不误报）。**部分码失败不降级**——保留成功结果落库，失败码仅 WARNING；
  这是当前明确但有风险的契约（部分失败码可能继续使用旧快照），是否升级为
  "任意单码失败即 degraded"另立后续正确性变更审核。
- **ts_code 转换**：`QFQFactorRefresher` 在调用 Tushare `adj_factor`/`fund_adj` 前，
  在各资产类别**自己的 try 块内**统一将裸码解析为 Tushare ts_code——`resolve_ts_codes`
  对裸码优先查 `stock_basic`/`etf_basic` 元数据表权威 ts_code，**已带合法 Tushare 后缀
  （.SH/.SZ/.BJ，含 .SS→.SH）的输入幂等保留、不被元数据覆盖**；裸码元数据 miss 时用资产类型感知的
  前缀规则（`raw_to_tushare_ts_code`：STOCK 6→SH/0,3→SZ/4,8→BJ；ETF 5→SH/1→SZ）
  fallback，不丢弃任何码；未知首位前缀防御性 fallback 到 .BJ 并记 WARNING。股票转换异常不影响 ETF（跨资产类别隔离）。
  > 注：本转换仅作用于 `QFQFactorRefresher` 的主动刷新路径，不覆盖 daemon 其它
  > Tushare 因子调用方（如 `daemon._fetch_adj_factor` 收到裸 ETF 时仍依赖现有
  > `market_of_code()`，可能错误推导 `.BJ`，作为独立残余风险）。
- **职责分离**：Tushare 负责因子（`adj_factor`/`fund_adj`），xtquant 负责价格
  （fresh_capture 单源锁定）；两者来源隔离，因子刷新不触碰价格表。
- **生产 bootstrap 准入门禁**：Step C 必须执行
  `qfq_orchestrator_cli bootstrap-plan --admissible`，消费
  `config/qfq_rebase_admissible_securities.json` 的 5487 只 loose 名单。名单外候选记录为
  `excluded/NOT_ADMISSIBLE`，不进入 fresh 下载/rebase，也不阻止
  `bootstrap_completed`；准入证券执行产生的 `blocked` 仍是硬阻塞，必须为 0。
  （分钟线 partial 降级有**严格边界**：仅当库内分钟表**已有数据**（target>0）且
  fresh ⊇ target（fresh 多出历史行 = 库内缺失）时才记为 `partial` deferred、
  `status=committed`、**不计入 blocked**，其 `minute_front_coverage='partial'` 须审计
  可见；库内行在 fresh 中缺失（`missing_staged>0`）或**库内分钟表为空（target=0）**
  均严格 BLOCK（`minute_coverage_mismatch`）——故 bootstrap 前置必须先由 daemon
  mcp 采集灌分钟表（近 3 个月起步，2026-08-08 决策），窗口 = 主库分钟表 MIN/MAX。）
  完整操作见 [`docs/qfq-resident-runbook.md`](docs/qfq-resident-runbook.md)。
  2026-08-08 取数适配修复详见 [`docs/framework-fix-report-20260807.md`](docs/framework-fix-report-20260807.md)。

### QFQ 数据质量三道防线（2026-08-15）

> 模块：`quantstudio.pipeline.qfq_invariant`。针对 QFQ 前复权基准 bug（某写入路径批次内
> `groupby().last()` 作 adj_latest → 分片窗口不含最新因子时 front 被错算成 raw，1442 万行
> 被破坏却未被现有 2% 近似审计发现）补的**精确复权自洽**防线。管线内置观测，策略层无感。

- **aligner fail-fast（Phase 3）**：`_apply_qfq` 无全局快照（`adj_latest_map` /
  `adj_earliest_map`）时直接 `raise`，禁止批次内基准——宁可任务失败也不静默写错 front。
- **防线 1 · 写入自洽（口径 A）**：`_stamp_and_write` 落库前对抽样行精确校验
  `front == raw × adj_i / adj_latest`；快照沿调用链从四路径（per_date / per_stock /
  普通 / 流式）传入，同批写入锚与自检锚一致。
- **防线 2 · 因子完整性扫描**：常驻轮次末尾（与 `_run_full_quality_audit` 并列、不依赖
  编排器开关）扫 `qfq_aux.db` 的缺日 / 异常跳变 / 单日突增 / 独立交叉源抽核；交叉源在
  `mcp_only` profile 下为禁用态（tushare enabled=false）。
- **防线 3 · 黄金行启动冒烟**：启动时重算黄金行（159995@2026-05-26 = 1.3539738908618015），
  `anchor_version` 由 reanchor committed 事件自动刷新（S2）。
- **告警升级链**：单批偏离告警；连续 3 个批 / 单批偏离率 >5% 阻断该表下一轮（good 批自动
  解除）；审计计数落 `batch_audit.db.qfq_selfcheck_log`。
- **TD-D2 因子库统一路由（2026-08-15 实施，⑤ C-6 释放前置）**：写入锚/读取锚/防线监测/
  refresher 全部收敛到 `qfq_aux_router.resolve_runtime_aux_path()` 单一入口，代码中不存在
  第二处 aux 路径推导（grep 审计测试锁定）。切换双条件齐备才指向 mcp-gen1 世代库
  （`qfq_aux_mcp_gen1.db`）：① 主库 `qfq_active_cutover` 存在记录（实查，当前
  b6_formal_20260807_v2 已 active）；② `qfq_aux_paths.json` 顶层 `"released": true`
  （⑤ 释放门，当前 false）。任一不满足 → fail-secure legacy。**⑤ 释放时切配置不切代码**：
  释放流程完成后置 released=true 即完成切换。分支 A 决策（同值性抽核 165,486 个
  (code,day) 对比点逐日差=0）：存量 front 与 gen1 锚自洽，切换无需全量重锚。

---

## Strategy Compiler 0.3.0-mvp — 策略编译器

> **⚠ 完善中（请勿使用）**：该模块仍在开发中，**暂时不要用于策略生成**，短期内会开放。待完善完成后会移除本提示。

将自然语言策略想法编译成经验证的双平台（QuantStudio + PTrade）策略包。

### 三层关系

| 层 | 角色 | 谁用 |
|---|---|---|
| **Skill** | AI 工作流：理解自然语言 → Spec → 用户确认 → 编排验证 + 交付 | 普通用户（通过 AI） |
| **orchestrator** | 验证引擎：Spec → IR → 双 Renderer → 7 validators → run_card | Skill 自动调用 |
| **qs-compile** | CLI 交付：Spec → IR → dual Renderer → strategy package | Skill 自动调用；高级用户直接用 |

### 推荐安装

```bash
# 独立虚拟环境
python -m venv qs-env
qs-env\Scripts\activate
pip install quantstudio-0.3.0+mvp-py3-none-any.whl jinja2 jsonschema pyyaml packaging

# 验证
qs-compile --help

# 安装 Skill
python skills/quantstudio-strategy-compiler/scripts/install_skill.py --agent zcode
```

### 普通用户使用方式

向 AI 说："帮我生成一个双均线策略。" AI 自动完成：
1. 理解策略 → 2. 能力检查 → 3. 生成 Spec → 4. 展示确认 → 5. 验证 → 6. 交付包

**普通用户不需要手动调用 qs-compile。** Skill 自动串联 orchestrator + qs-compile。

### 高级用户 CLI

```bash
qs-compile package spec.json --out output/packages [--g2-frozen-dir <dir>]
```

### 输出目录

```
output/strategy_deliveries/<strategy_id>/
├── validation/      ← orchestrator 验证产物 (run_card, capability_report, ...)
├── package/         ← qs-compile 策略包 (manifest, dual .py, spec/IR)
└── DELIVERY_REPORT.md  ← 统一交付摘要
```

### 验证报告

- `run_card.json`：总验收卡（stage/status/各验证结果）
- `capability_report.json`：数据/执行能力就绪状态
- `variant_consistency_report.json`：QS vs PTrade 14 维一致性
- `manifest.json`：artifact SHA-256 digests（可审计）

### data_digest blocked 说明

`data_digest_status=blocked`：真实市场数据 digest **deferred**（后置，不伪造）。本 MVP
使用 Hermetic/Synthetic 场景验证 pipeline 正确性，**不等于**真实数据 Fidelity 已验证。
真实 Fidelity/Reference 验证、live QMT、resident daemon 均不在本次 MVP 范围。

### 常见错误

| 场景 | exit code | 说明 |
|---|---|---|
| success | 0 | 包生成成功 |
| invalid/missing spec | 2 | Spec 不存在或 JSON 无效 |
| Golden Protection | 3 | 策略 ID 在保护名单中 |
| G2 frozen incomplete | 4 | G2 frozen artifact 缺失 |

### 不在本 MVP 范围

真实市场数据接入、live QMT、resident daemon、PR7 自动 Fidelity、GUI 集成、部署产品化。

### 更多文档

- 用户指南：`docs/strategy-compiler/USER_GUIDE.md`
- Skill 操作手册：`skills/quantstudio-strategy-compiler/SKILL.md`
- 发布说明：`quantstudio/strategy_compiler/release/RELEASE_NOTES.md`
- 项目状态：`docs/strategy-compiler/implementation-status.md`

### ETF basic reference collection

`etf_basic` is now a first-class pipeline task. Its authority and only configured source are Tushare (`fund_basic(market="E")`). Full, incremental, and resident modes all use the same path: fetch snapshot -> canonical baseline standardization -> validation -> changed-row DuckDB upsert. Tushare `YYYYMMDD` list/delist dates are converted to Asia/Shanghai midnight milliseconds, `.SH` is normalized to `SS`, and fields with unrelated units (such as `issue_amount` and `p_value`) are excluded. Missing list/delist dates may be filled from the first/last `etf_daily` bar. Restart an already-running resident collector after changing this task configuration。

## W2 staging / 框架修复（2026-07-27 session, file timestamp 2026-07-28）

> 状态：框架代码补正完成（W1→W2-0.7A）→ staging 安全闭环测试（W2-0.7B，58 项）→ W2-0.8 框架修复（审核不通过）→ **W2-0.9 最终收口**（5 项缺陷 A-E + Phase 5 + C/F 补完 + 性能回归回退，全量 1571 passed）。W2 真实执行（Phase 0-4）暴露的框架缺陷已全部本地修复。数据等待用 **Git worktree 外** staging 路径重新执行完整 W2。禁止在审核通过前执行真实 staging prepare / Tushare 回填 / promotion / 修改正式库 / Git stage-commit-push。

### W2-0.9 框架修复（2026-07-28 最终收口）

W2-0.8 审核发现 9 项问题，W2-0.9 逐项关闭（详见 `docs/framework-fix-report-20260728.md` 与 `output/w2_fin_growth_dividend_20260728/phase4_defect_report.md`）：

- **A — writer VARCHAR 类型保护（补完）**：`writers.py` 入库前用 `DESCRIBE` 取目标列 DuckDB 类型，VARCHAR 列不再被 `pd.to_numeric` 吞掉（修复 `div_proc="实施"`→NULL）；**fallback 白名单补 div_proc/div_rat**（W2-0.8 漏）；DESCRIBE 失败记录 warning 不静默。
- **B — 通用 authority_reconciliation（补完）**：完整契约严格校验（mode=`purge_non_authoritative`/scope=`full_range_only` 未知值 config_lint + 运行时 fail-fast）；全触发条件（full_range + authority + fallback=false + actual source==authority）；**原子事务**（BEGIN/DELETE/校验/COMMIT 或 ROLLBACK）；cleanup_source_watermark=false 不删 watermark；空表不静默 PASS；标识符校验防注入。
- **D — run_once/CLI 退出码传播**：`run_once` 返回 `{task_found, task_ok, audit_run, audit_ok}`；task 不存在/failed/audit failed → CLI exit 1；staging `phase_run_task` 增加 batch ledger final status cross-check + 真实 CLI 子进程退出契约测试。
- **E — --quality-audit full|none**：daemon CLI 新增该参数（默认 full，生产语义不变）；staging 分阶段装载用 none。
- **Phase 5 — baseline-delta audit（接入重做）**：`validate_audit_evidence` 不再要求 `errors_count==0`，改为要求 `baseline_delta_passed=True`；目标表零 error，非目标表允许继承，new/regressed/severity-upgrade BLOCK；phase_audit 顺序调整（先 delta 再 strict validate）。
- **C/F — Git worktree 外 staging 路径（强制）**：非 dry-run prepare 必须 fail-closed BLOCK staging_root 在 Git worktree/项目根/data 内；preflight 独占检查；detached runner **不自动 kill**（scan 只报告；kill 显式+再验证；同 root 阻断）。
- **性能回归**：duckdb_data_access/duckdb_provider/ptrade_api 3 文件**回退到 HEAD**（batch 优化无条件查不存在的表 + 破坏测试替身）；纯性能等价修复组=回退状态。
- **G — watermark 审计口径对齐（W2-0.9 修正）**：`quality_audit._audit_watermarks` 优先用 schema 的 `time_key` 选业务时间列，回退再用硬编码优先级，与 daemon `_advance_actual_watermark`/`_get_safe_watermark` 推进口径对齐；修复 stock_dividend（`time_key=ex_date`）按除权日推进水位却被审计误用 `end_date` 比较导致的 `WatermarkConsistency` 误报。
- **H — batch 守恒口径修正（W2-0.9 修正）**：守恒等式由 `rows_passed + rows_rejected == rows_raw` 放宽为 `<= rows_raw`（DataValidator 分流前主键去重属合法收敛，去重只减不增）+ 新增 `rows_written == rows_passed` 入库一致性；`evidence.passed` 改为仅标记结构收集成功（`len(evidence_errors)==0`），raw audit 的 inherited error 归 baseline_delta 门禁负责，不再误伤 evidence 通过。

测试：新增 5 个测试文件（43 项）+ 现有 staging 测试适配；W2-0.9 专项 **165 passed**；全量 **1676 passed, 1 warning**（唯一 warning 与 W2 无关，原样保留）。

### Profile 1.10.0

- PTrade Profile 升级至 1.10.0：正式登记 `get_stock_exrights(security, date=None)`（返回 DataFrame，index=date，列: allotted_ps/rationed_ps/rationed_px/bonus_ps/exer_forward_a/exer_backward_a/bexer_backward_a/b）。portable usage 必须显式传 `date`；`date=None` 返回 `None`（底层查询需具体日期）。
- 注：`get_stock_exrights` 受 Tushare 接口频率限制（每分钟最多 200 次），批量调用需加间隔。

### 增长字段（fin_indicator）

新增：`or_yoy`（营收同比增长率，%）、`tr_yoy`（营业总收入同比增长率，%）`、`update_flag`（0=初版/1=修订版，PIT 去重）、`diluted_eps`（稀释 EPS，独立于 eps 列）。

### 分红字段（stock_dividend）

标准化：`cash_div_before_tax`（税前每股现金分红）、`cash_div_after_tax`（税后每股现金分红）、`stk_bo_rate`（送股比例）、`stk_co_rate`（转增比例）、`div_proc`（仅入库"实施"记录）。公司行为引擎税务策略：`pre_tax × 0.80`。

### get_stock_exrights API

完整签名：`get_stock_exrights(security, date=None)`。contexts: research/backtest/trade。返回 DataFrame（date 索引, 8 列 PTrade 兼容）或 None。源表 `stock_dividend`（tushare 权威源），schema 兼容旧列 `cash_div`。portable usage 必须显式传 `date`；`date=None` 返回 `None`。受 Tushare 频率限制（~200/min），批量建议间隔。

### 持仓视图契约修复（2026-09-04）

`context.portfolio.positions` 在引擎在场时曾 `return dict(acc.positions)` —— 直吐引擎 dataclass
（`volume`/`can_sell`），**绕过唯一适配器** `BacktestEngine._get_ptrade_positions`，导致策略侧
`amount`/`enable_amount`/`cost_basis`/`last_sale_price`/`sid` 全部 MISSING→0（静默错误：判空仓、不可卖、市值记 0）。

- **三个本地持仓入口统一契约**：`context.portfolio.positions` / `get_positions()` / `get_position()`
  一律经 `_get_ptrade_positions` 产出 PTrade `Position`（键 `.SS/.SZ/.BJ` 精确匹配）；
- 字段映射：`amount←volume`、`enable_amount←can_sell − pending_sell_shares`、
  `cost_basis/avg_cost←avg_cost`、`last_sale_price←当日价`（缺失**仅**在盘前/收盘后空窗或当日无行情时回退 `avg_cost`）；
- `get_positions()`/`get_position()` 现传入当日价字典（此前不传价 → `last_sale_price` 实际落 `avg_cost`）；
- **不下沉、不触转换模板** → 转换产物零变化、矩阵哈希无需 reverify；
- 设计 `docs/portfolio-position-view-contract-design.md`；验收 `docs/evidence/portfolio-position-view-acceptance.md`；
  取证 `docs/evidence/portfolio-position-view-defect.md`；影响面 `docs/evidence/portfolio-position-view-impact.md`。

### get_fundamentals(valuation) date PIT 语义修复（2026-09-04）

`get_fundamentals(security, table='valuation', fields=[...], date=D)` 的 `date` 分界修复：

- **预加载快照锚点 = prev_date（T-1）**（实测：attach date=2026-07-30 / prev=2026-07-29 时，
  600519 turnover_ratio 快照值 0.4986 = 07-29 真值，非 07-30 的 0.5749；流通市值同源吻合）。
  默认路径命中预加载快照即返回。
- `date` 未传 / `date >= T-1` → **快照路径，行为逐位不变**；
- `date < T-1` → 走真 as-of（`query_valuation_daily_pit`），返回该日真实估值
  —— 修复前该分支静默返回快照数据（错误数据，无任何策略依赖）。
- provider 新增 `force_as_of` 开关（**默认关闭**，默认路径零改动）；
  **不新增注入 API**：历史估值序列由策略按日循环
  `get_fundamentals(pool_list, 'valuation', fields=[...], date=D_i)` 取得（list 批量、当日缓存已含 date 键）。
- 设计文档 `docs/valuation-date-pit-fix-design.md`；验收证据 `docs/evidence/valuation-date-pit-acceptance.md`
  （分界测试逐位一致 / as-of 对拍一致 / 6 策略转换产物逐位一致 / 回归零新增失败 / 黄金结果逐位一致）。

### W2-0.7B staging 安全闭环（2026-07-28）

staging 回填工具 `scripts/backfill_fin_growth_dividend_staging.py` 提供 `prepare / run-task / audit / promote` 四阶段，全程只操作 staging 副本，绝不修改正式库；`--promote` 仅 dry-run（打印命令，不执行）。安全门控：

- **prepare**：daemon/collector 活跃检测（`verify_daemon_identity`，stale 放行 / alive+denied+corrupt BLOCK）、磁盘 ≥ 2x 源库、源库 SHA-256 一致性、`--reset-staging` 需 `.quantstudio_staging.json` marker 校验且目标不得为磁盘根/项目根/data 根/源库父或含源库。
- **run-task**：config `db_path` 必须指向 staging.db（否则 SAFETY BLOCK）；子进程写 runtime manifest（atomic `os.replace` + nonce replay 防护），父进程严格校验 `format_version/task/nonce/QUANTSTUDIO_DATA_ROOT/imported_DATA_ROOT` + 七个路径字段全部 resolve 到 staging root；`timeout=0` 表示无超时，`timeout>0` 按 elapsed 终止；heartbeat 每 30s 可见。
- **audit / promote**：版本分离（`data_schema_version=2.0` 来自 alignment_rules vs `ptrade_profile_version=1.10.0` 来自 ptrade-api-signatures）；batch conservation（`rows_passed + rows_rejected == rows_raw`）；batch ID 唯一性 + 一任务一批；runtime manifest 内容校验；authority_rules 锁定 tushare 单源（`allow_fallback=false`）；audit `checks_run>0` 且 `errors_count==0`。

测试覆盖：`tests/test_fin_growth_dividend_staging_tool.py` 共 **58 项**（原 13 + W2-0.7B 新增 45），含负向门控（daemon alive/denied/corrupt BLOCK、stale 放行；collector lock held BLOCK、stale 放行；timeout=0/正数/heartbeat；reset marker 四类 BLOCK；缺配置/磁盘失败/源库不可读/size mismatch/SHA mismatch BLOCK；child manifest 缺失/stale nonce/wrong task/wrong DATA_ROOT/wrong path/wrong PID/stale/future timestamp BLOCK；duplicate batch/same-task/conservation/schema/profile/audit errors BLOCK；growth/dividend 全零 BLOCK；manifest audit 后漂移/删除 BLOCK）+ 两类双任务 E2E：**真实子进程 E2E**（真实 `python -m quantstudio.pipeline.daemon` 子进程 + `sitecustomize.py` 注入 FAKE `TushareAdapter`，验证 manifest pid == 实际 Popen PID、created_at ∈ 生命周期窗口、源库字节不变）与快速模拟 E2E（prepare→fin→div→audit→promote dry-run）。详见 `docs/staging-runbook.md` 与 `docs/framework-fix-report-20260728.md`。

### 当前 DB 状态

正式库仍为旧 schema（fin_indicator 11 列 167,028 行，增长字段全 NULL；stock_dividend 旧 cash_div 列，口径不明）。数据等待 W2 staging 回填：schema 迁移（`_migrate_add_columns` 幂等）-> staging prepare + fin_indicator 回填 -> staging stock_dividend 回填 -> 质量审计 -> promotion（原子替换）。正式库旧 schema 待 W2 回填后才会迁移；当前 staging 工具与所有 DataAccess 查询均支持旧/新 schema 兼容（动态检测列存在性）。

详见 `docs/framework-fix-report-20260728.md`（W2-0.7B 最终测试矩阵）与 `docs/framework-fix-report-20260727.md`（W1→W2-0.7A 框架修复）。

## 框架层变更审阅记录（perf/datadict-day-index）

本次 `quantstudio/backtest/ptrade_api.py`、`quantstudio/backtest/backtest_engine.py` 新增 DataDict/BacktestEngine 当日 DataFrame 的 `{raw_code: first_iloc}` 实例代码索引，将 `df['code'] == bare` 的 O(N) 布尔过滤替换为 O(1) 索引查找；`None`（无法构建）时严格回退原布尔过滤。

**AGENTS.md 框架铁律适用**：本变更为纯性能优化，已审阅确认未改变任何公共/注入 API 的函数名、签名、默认值、返回类型、返回字段、列顺序、索引、dtype、空值行为、异常行为或兼容行为；未改变行情取数范围、复权口径、生命周期调用时机、撮合/费用/持仓/现金/涨跌停处理、策略信号或回测指标；`data[code]`（DataDict）、`get_current_price`、`is_halted`、`pct_chg` 等接口语义与返回结构不变。因此本文档相关表述不受影响，无需修改。

## 框架层变更审阅记录（perf/backtest-data-cache）

`quantstudio/backtest/providers/duckdb_data_access.py` 的内部语义等价性能优化（仅 1 项）：
`_existing_tables()` 缓存 `SHOW TABLES` 结果，消除 `preload_daily_bars` /
`query_strategy_events` / `query_corporate_actions` 三处直接 `SHOW TABLES` 查询（小市值策略
76 交易日实测 SHOW TABLES 调用 **152 → 1**）；返回防御性 `set` 副本；`close()` 将
`_tables_cache` 恢复 `None`；原直接 `SHOW TABLES` 路径统一走 `_existing_tables()`。

**调用路径事实（b41400d）**：当前共有 **10 个**表存在性检查调用方共享 `_existing_tables()`，
其中 **7 个**是 b41400d 既有调用方，另有 `preload_daily_bars` / `query_strategy_events` /
`query_corporate_actions` 3 个原直接执行 `SHOW TABLES` 的调用方在本次收敛至统一入口。
`query_daily_snapshot` 在 b41400d 中已直接查询 `stock_daily`/`etf_daily`，不属于上述 10 个调用方。
旧基线 d8a0791 曾包含该调用方，因此历史数量为 11，但不适用于本次发布基线。

**AGENTS.md 框架铁律适用**：本变更为纯性能优化，语义完全等价——`_existing_tables()` 返回集合
与优化前逐字符一致，仅避免重复 `SHOW TABLES`；未改变任何公共/注入 API、取数范围、复权口径、
生命周期、撮合/费用/持仓/现金/涨跌停处理、策略信号或回测指标。完整验证见
`docs/performance_optimization.md`。

**provider-level get_history 缓存（`query_bars_by_count_multi_table`）本轮拒绝实施、进入
backlog**：小市值与双均线真实策略 `get_history` 命中均为 0；`PtradeAPI.get_history()` 已有
`_query_cache` 层；synthetic 86× 不构成生产收益证据；4096 条目上限是条目数而非字节内存上限；
后续实施须满足 byte-bounded LRU + 真实生产命中证据。

## 框架层变更审阅记录（perf/daily-snapshot-cache-key，2026-09-18）

`quantstudio/backtest/providers/` 的内部语义等价性能**回归修复**（P-D14 D3 引入）：

- **缺陷**（2026-08-27 commit `bb602f3` 引入）：`preload_daily_snapshots` 用
  `time // 86_400_000 * 86_400_000`（**UTC 日界**截断，结果 mod 恒为 0）生成缓存键，而
  `query_daily_snapshot` 的查询键来自 `_start_ms`（**CST 日界**，mod 恒为 57_600_000）——
  两个键空间**交集为空**，预取结果 **100% 未被消费**，每个交易日退化为一次对
  `stock_daily`/`etf_daily` 的全市场窗口扫描。该提交之前的实现为 `groupby("time")`
  原值键（= 查询键，命中有效），故本次为**性能回归**而非新需求。
- **修复**：日界语义收敛到唯一真相源 `quantstudio/backtest/providers/time_axis.py`
  （`CST_OFFSET_MS` / `start_ms` / `end_ms` / `day_start_ms`），预取键与查询键由同一函数派生；
  `duckdb_provider._start_ms/_end_ms` 改为 re-export（实现与签名逐字符不变、全部既有引用点零改动，
  `preload` 中原有的 `str(start_date)[:10]` 归一留在原调用点不挪不删）。
- **防御**：`query_daily_snapshot` 落缓存前增加**纯防御**（非 CST 日界键不写入）——读键与写键
  仍同为 `date_ms`、逻辑零改动；防的是未来出现非日界 `date_ms` 时，其错位窗口
  `[date_ms, date_ms+86_399_999]` 的结果污染真实当日键（午夜查询静默命中错数据）。
  返回值与异常行为均不变，判据走 `time_axis` 真相源、不硬编码日界常量（避免第二真相源）。
- **实测**：预取区间内逐交易日缓存命中率 **0% → 100%**（只读探针 + 断网取证）。
- **防回归**：`tests/test_daily_snapshot_cache_key.py` T-1~T-5 —— 日界同源恒等 / 预取后
  **真命中**（断网取证）/ 缓存命中与 DB 兜底逐值等价 / 08:00 组归并不回归 / 防御分支自身覆盖。
  P-D14 的 T6 只比较行数与 code 集合、**未断言缓存确实命中**，缺陷当年被 DB 兜底掩盖；
  T-2 直接堵住该验收漏洞。

**AGENTS.md 框架铁律适用**：本变更为纯性能优化。端到端黄金对照证明语义完全等价——同一策略
同一区间（影子库，2026-01-05~2026-03-13）：修前等价态 `cache hit/miss = 0/45`、修后 `45/0`
（**确证走的是不同路径**），而 `nav_sha` / `trades_sha` / 净值长度 / 交易数 / 结果属性集
**逐项全等**（净值末值 101328.63776）。未改变任何公共/注入 API 的函数名、签名、默认值、
返回类型、返回字段、列顺序、索引、dtype、空值行为、异常行为或兼容行为；未改变行情取数范围、
复权口径、生命周期调用时机、撮合/费用/持仓/现金/涨跌停处理、策略信号或回测指标。

完整验证见 `docs/performance_optimization.md`。

## 框架层变更审阅记录（perf/prev-close-map-deiterrows，2026-09-19）

`quantstudio/backtest/backtest_engine.py::_apply_factor_derived_split` 内部的
`prev_close_map` 构造**去 `iterrows` 化**（纯性能优化·语义等价）：

- **问题**（主因件归因 + 全仓运行时普查）：静态 58 处 `iterrows` → **回测路径实触 4 处** →
  该调用点**独占回测总耗时 64.04 %**（85.094 s / 132.87 s；245 交易日、179.7 万行迭代）；
- **修复**：改为向量化构造 `dict(zip(col.astype(str), col))`；`close` 列缺失时构造**全 0 map**
  （与原式 `row.get('close', 0)` **实现级**全等）。净 **+10 / −2 行**，**无开关**；
  其余 3 处被触发的调用点合计 0.16 %，**登记不改**；`strategies/` 零触碰；
- **等价性要害**：`iterrows` 走 `DataFrame.values` 会**提升公共 dtype**，故仅当 `code` 列为
  **object** 时 `str(row['code'])` 与 `astype(str)` 逐值一致（实测恒成立）；
  该前提由 **E-1 哨兵断言**锁定，数值型 code 的差异由 **E-4 显式登记**；
- **验证**：契约测试 **17 passed**（含 ETF 除权四带区与缺列语义）；端到端黄金对比
  `双均线策略` / `ETF轮动` 的 `nav_sha`/`trades_sha`/`ca_sha` **逐位全等**（文件级还原取证）；
  A/B 各 3 轮：单股 309 交易日**中位数 159.674 s → 44.621 s（−72.1 %）**，
  **路径分离已证**（`iterrows:907` 计数 **307 → 0**）。

**AGENTS.md 框架铁律适用**：本变更为纯性能优化，未改变任何公共/注入 API 的函数名、签名、
默认值、返回类型、返回字段、列顺序、索引、dtype、空值行为、异常行为或兼容行为；未改变
行情取数范围、复权口径、生命周期调用时机、撮合/费用/持仓/现金/涨跌停处理、策略信号或
回测指标。完整验证见 `docs/performance_optimization.md`。

## 框架层变更审阅记录（fix/source-import-industry-extract，2026-09-19）

`quantstudio/strategy_compiler/source_import.py::_extract_industry_codes` 的**提取判据重写**
（**正确性修复**·非性能优化）。该函数从策略源码提取「行业剔除码集」，再经
`_QS_INDUSTRY_EXT` 模板的 `__QS_INDUSTRY_CODES__` 占位符烘焙进 PTrade 转换产物。

- **缺陷**：旧判据「`ast.walk` 遍历**全文件全部字符串常量**，凡纯 6 位数字即收」
  **无法区分「行业码」与「业务哨兵 / 无关代码表」**。四策略直测实测三例误提取：
  `断板反包策略` 的 `_QS_BSE_LEGACY`（n=248 北交所映射快照，**另一语义**）被**整表收走**（≈200 码）；
  `连板梯队龙头打板套利策略` 的业务哨兵 `'999999'` 被收走；`F-Score选股RSRS择时` 恰好正确
  （源码内只有那 4 个 6 位数字）——**属侥幸，非判据有效**。
  **误收后果**：无关集合被烘焙进产物 ⇒ 平台端逐个查行业（**池查询灾难**）。
- **修复**：判据改为「**两路锚定 + 三面排除**」——
  **路 A** `Compare(op=In)` 右侧字面量元组（左值须为行业语义变量名，如 `ic in (...)`）；
  **路 B** `Assign` 目标名匹配 `INDUSTRY_EXCLUDE*` / `_QS_INDUSTRY_CODES`；
  **排除** docstring/注释（**结构性排除——不再遍历全量 Constant，非补丁过滤**）、
  **哨兵/魔法值**（`999999`/`000000`）、**已知无关集合常量名**（`*BSE*` 映射等）。
  判据**保守偏空**：宁返回空集（仍触发转换器告警 + 下游 fail-open，RD-3）也不误收。
- **验证**：四策略直测 —— `F-Score` 结果**与修复前逐字相同**（**零变更硬门**，重转产物
  `_QS_INDUSTRY_CODES` 与现存 12 版一致）；`断板反包` 由 ≈200 码 → `()`；
  `连板梯队` 由 `('999999',)` → `()`（**与现存产物一致 ⇒ 该策略无需重转**）；
  新增回归测试 `tests/test_industry_code_extract.py` **16 项**，相关套件 **125 passed / 1 skipped**。
- **契约矩阵**：`scripts/check_fund_matrix.py --check` **哈希一致 + MD 一致**
  （本修复只动函数体，未触碰 `_QS_INDUSTRY_EXT` / `_QS_FUNDAMENTALS_EXT` 模板字符串）。
- **零改动**：**任何策略源码未改**（铁律）；渲染格式（tuple 字面量）与空集语义（告警 + fail-open）不变。

完整方案与归因见 `docs/source-import-industry-extract-fix-design.md`。

> **PyQt single-task result contract (2026-08-03)**: manual task status is determined by that task's fetch?align?validate?write result. Full-database `QualityAudit` still runs afterward, but unrelated-table failures are displayed as `success (audit warning)` rather than falsely marking the data pull as failed. Real task failures remain failures.

> **Full-database audit reference-table contract (2026-08-03)**: MCP `stock_basic` and the shared MCP/QFQ `trade_calendar` are writer-managed canonical tables. `trade_calendar` keeps its original `cal_date` single primary key and QFQ behavior; `exchange/pretrade_date` are compatibility metadata. Enum auditing uses native typed values, and QFQ pending SLA uses the current-state update time.

## MCP cutover B-4 staging 演练（本地完成，CodeBuddy 独立复审通过）

> B-4 实施证据归档日：2026-08-05；CodeBuddy 独立复审通过日：2026-08-06。原始演练时间戳按证据文件保留。

`scripts/qfq_b4_staging_drill.py` 是生产 cutover 前的独立演练工具，不在 daemon 生产调用链中。默认仅执行零数据库写、零 run-dir 写的 preflight；只有显式传 `--execute` 才会在 `output/mcp_migration/<run-id>/` 创建全量 staging 副本并执行迁移/恢复演练。复制正式库时同时持有 `.daemon.lock` 与 `.collector_run.lock`，并在演练前后比较正式主库与 aux 的 canonical path、size、mtime_ns、SHA-256。

```powershell
# 零写前置检查
python scripts/qfq_b4_staging_drill.py --run-id b4_preflight_20260805

# 全量 staging 演练；只写 output 下副本，不写正式库
python scripts/qfq_b4_staging_drill.py --run-id b4_20260805_final --execute
```

B-4 全量实测结果：baseline=`COMPLETE_2_0`，normal/recovery=`COMPLETE_2_1`；正常分支覆盖 `DRY_RUN_COMPLETE → ROLLED_BACK → MIGRATION_COMMITTED → ALREADY_CURRENT`，恢复分支覆盖 COMMIT 后中断后用新 report 执行 `ALREADY_CURRENT` 审计恢复。MCP 配置的离线 bootstrap 不灌历史 trigger；当前 **pre-B-5** `stock_dividend` discovery 仍是全表内容 hash 扫描，所以首轮实测新增 2181 个 dividend trigger，立即原样重放新增为 0。此数字是 B-5 discovery-baseline/CAS 实施前的真实基线，不能误写成“首轮 discover 必须为 0”。演练保持 `qfq_active_cutover=0`、所有表 `mcp-gen1=0`，未激活 B-6。

证据：`output/mcp_migration/b4_20260805_final/b4_drill_report.json`。该报告明确 `production_ready=false`、`git_sync_authorized=false`；B-4 已通过独立复审，B-5 可进入本地实施；B-6、正式库迁移及 GitHub 同步均未获授权。

### B-4 Windows COMMIT 后 hard-crash 边界补修

最终串行验收前曾出现预期 `os._exit(92)`、实际 Windows `0xC0000005`。未放宽断言，也未接受 access violation。最小修复将 `after_commit_before_report` 故障点移动到 durable COMMIT 成功且 `committed=True` 之后、正常 DuckDB connection cleanup/report 更新之前；正常和受控异常路径仍由 `finally` 关闭连接。严格串行验证：直接 `os._exit` 30/30、原始 pytest 20/20、migration+B-4 87 passed/1 skipped、扩展回归 827 passed/1 skipped。并行启动多个 DuckDB pytest 进程可能产生 Windows native 干扰，因此该 crash suite 必须串行运行。

### MCP cutover B-5 local staging (2026-08-06)

B-5 local framework work is now scoped to staging primitives only. The resident QFQ orchestrator carries an explicit `(price_source, source_generation, cutover_id)` identity through discovery, trigger claiming, reanchor events, anchors, captures, backfills, bootstrap records, cycles, watermark intents, and quality audits. Dividend discovery uses a shared payload hash and two-phase baseline CAS; the pending baseline slot and trigger insert are one transaction, and new logical keys start with `applied_payload_hash=NULL`.

Generation-specific factor observations are physically routed by `AuxDbRouter`; a missing dynamic auxiliary file fails closed and is never replaced by the legacy `qfq_aux.db`. The CLI exposes `aux-init`, generation-filtered status/audit views, and `baseline-build`. A plain transition to `active` is blocked; active-pointer CAS and `mcp-gen1` activation remain B-6 gates. Formal database migration and mcp-gen1 activation remain unauthorized. The complete B-5 code/test/config/document set is synchronized only under the explicit post-repair confirmation received on 2026-08-06.

Configuration note: an explicit non-legacy `source_generation` implies `generation_mode=dynamic` when omitted; `pre_cutover` with a non-legacy generation is rejected fail-fast.

### PyQt manual full-pull watermark contract (2026-08-06)

When QFQ coordination is enabled, a PyQt single-task pull of one of the four
price tables (`stock_daily`, `stock_minutes`, `etf_daily`, `etf_minutes`) now
opens a one-task coordination cycle before fetching and runs the normal
post-ingest gate before releasing the worker. This fixes the previous path in
which the write succeeded but no cycle existed, so the fail-closed watermark
helper discarded the candidate and `source_watermark` stayed absent/stale.
There is no direct watermark bypass: a passed gate commits the watermark; a
held/failed gate leaves it unchanged and the GUI reports a watermark-gate
warning. The same contract now covers GUI **Run All**: each task carries its
QFQ cycle result, the collector connection and cross-process lock are released
before the final signal, and held/failed tasks are named in the aggregate GUI
warning. A successful task that produced a candidate but has neither a
committed nor held terminal intent is reported as a watermark-contract anomaly;
a true empty/no-new-data pull remains a normal zero-candidate completion.

Watermark rendering prefers the configured source, but if that source has no
row after an allowed fallback, the GUI displays the newest matching actual
source watermark and identifies that source in the tooltip instead of showing
a false `none`. Database-backed tests cover all four QFQ price tables in both
`full_range` and `incremental` modes, plus commit, hold, run-all, no-intent, and
fallback-source display behavior. Formal database migration and formal PyQt
execution remain separately gated.

### MCP cutover B-6 local/staging implementation (2026-08-06)

B-6 staging now provides immutable evidence freeze/verification,
Evidence hashing streams sorted rows in bounded batches, so full-table staging evidence does not materialize multi-gigabyte price tables in Python memory.
expected-old active-pointer CAS, legacy non-terminal trigger retirement
(including `scheduled`), stale-cycle interruption, pending watermark-intent
supersede, dead-letter preservation, and transaction fault-injection rollback.
Use `cutover-evidence` followed by the staging-only `cutover-activate` CLI
command. The activation command rejects the configured formal database even
when `--allow-production` is supplied; no formal migration or real `mcp-gen1`
activation is performed by this local implementation.


### MCP cutover B-6 WP4 command gate (2026-08-06)

WP4 freezes the pre-formal-cutover command surface without authorizing formal
operations:

```powershell
# read-only activation plan; no BEGIN, writes, or evidence file
python -m quantstudio.pipeline.qfq_orchestrator_cli --db <staging.db> --json `
  cutover-activate --cutover-id <id> --expected-old <old-or-empty> --dry-run

# copy only an explicit staging/hermetic source; formal main/aux are rejected
python -m quantstudio.pipeline.qfq_orchestrator_cli --execute `
  cutover-prep-staging --source-db <source-staging.db> `
  --source-aux <source-staging-aux.db> --dest <new-staging-dir>

# bounded post-activation canary; scoped gate holds global watermark by design
python -m quantstudio.pipeline.qfq_orchestrator_cli --db <staging.db> --execute `
  cutover-canary --aux-db <staging-aux.db> `
  --codes 510500,159919,000001 --cutover-id <active-cutover-id> `
  --output <staging-dir>/evidence/post_activation_canary.json
```

`cutover-prep-staging` holds `.daemon.lock` and `.collector_run.lock` during the
copy, rejects non-empty WAL/journal sidecars, writes exclusive marker/manifest
files, and refuses a source equal to the configured formal main/aux.
`cutover-activate --dry-run` uses read-only SQL to print the expected-old CAS,
legacy retirement, intent supersede, pointer transition, and postcondition
sequence; it never creates evidence or starts a transaction.
`cutover-canary` recovers staging-only aborted cycles (`started` to
`interrupted`, SQLite integrity/WAL checkpoint) before running the bounded
scoped dynamic-identity canary.

### B-6 WP6/WP7 formal cutover runner (2026-08-07)

The **formal** cutover runner (`quantstudio.pipeline.qfq_formal_cutover`,
`qfq_formal_cutover_cli`, `qfq_formal_authorization`,
`qfq_formal_postcutever_audit`, `qfq_formal_canary`, `qfq_formal_observation`)
is the only authorized path to the formal production main/aux databases.  It
reuses the shared policy-free activation core `_do_activate_in_txn` (extracted
from `activate_cutover_staging`) so its six-point fault matrix is byte-for-byte
equivalent to staging, while substituting the staging-only guard with the full
authorization + dual-lock + backup chain.  The staging guard
(`_assert_staging_db`, `_assert_not_formal`, `_assert_staging_aux`) and the
migration guard (`_assert_not_production`) are unchanged and keep refusing
formal paths unconditionally.  Formal migration, `mcp-gen1` activation, formal
canary and formal watermark release remain unauthorized pending G2 and a
separate explicit user authorization.  See
`docs/mcp_migration/b6-formal-cutover-runbook.md` and
`docs/mcp_migration/b6-post-cutover-observation-runbook.md`.

---

## 全量同步说明（2026-10-01，fork 覆盖推送）

本节对照 `hasyme/quantstudio` fork 与本地工作树的**全部差异**做完整说明。
本次为**全量覆盖推送**（用户裁定方案 B：以本地为准），fork 原有 5 个提交
（A/B/D 件 + R1 就绪）已备份至本地分支 `backup_hashyme_main_20261001`，
如需找回可 `git checkout backup_hashyme_main_20261001`。

**提交**：`7f63f3d`（相对共同祖先 `8264273`，共 271 个文件、+63,376 / −17 行）

### 一、框架性能优化（六步流水线完整闭环）

| 项 | 内容 |
|---|---|
| 方案 | `docs/qfq-invariant-perf-design.md` |
| 审计 | `docs/evidence/caliber-and-qfq-perf-designs-audit-20260930.md`（**通过**，P-1~P-4 小修订已并入） |
| 实施 | `quantstudio/pipeline/qfq_invariant.py`（+59/−5） |
| 验收 | `docs/evidence/qfq-invariant-perf-acceptance-20260930.md` |

**问题**：QFQ 写前自检 `check_qfq_invariant` 的 `_load_factor_lookup` 按 code 拉取
**全历史**因子（~555 code × 287 万行），单次 22.45 s，占写阶段总耗时 **60.3%**
（实测打点 `invariant=1800.0s / total=2982.8s`）。

**改动**：新增 `_factor_query_window_ms(days)` —— 由抽样行 `bar_day` 推导因子查询
毫秒窗（整日取整 + **两端各外扩 1 天**），时间条件下推到 SQL；
`_load_factor_lookup` 增加**可选参数** `time_lo_ms` / `time_hi_ms`。

**语义等价论证**：`factor_lookup` 的唯一消费点是 `factor_lookup.get((code, day))`，
而 `day` 全部来自**抽样行**（当前分片）→ 窗口外的因子行**查了但从不使用**；
窗口按整日取整保证同一 `(code, bar_day)` 的全部因子行落入窗口 →
`keep='last'`（取 time 最大）语义不变。

**实测收益**：净查询 **74.77 s → 0.744 s（100.5×）**；`invariant` 段占比
60.3% → 约 1%；写阶段 1.32 分钟/片 → 约 0.53 分钟/片。

**等价性验收（硬门）**：A1 同输入下自检结果**逐项一致**——缺失集合相同、
**取值差异 0**（`VERDICT: EQUIVALENT`）；其余 3 处调用方
（`check_golden_rows` / `refresh_golden_rows_for_code` / `verify_reanchor_selfcheck`）
均不传新参数 → 走旧行为分支，**行为逐位一致**。

**通用性横验证（C3）**：6 策略重转 `api_portability` **全 PASS（6/6）**——
CANSLIM / fall_reversal / tech_etf_mvo_rotation / vol_regime_mom_rev /
weekly_smallcap_growth / 周频小市值成长动量（三层止损），
各策略 `convert errors=0` + `validate_ptrade_portability` blocks=0。

> **本优化属「性能优化不得改变引擎行为与逻辑」铁律下的纯优化**：未改变任何
> API 函数名/签名/默认值/返回类型/返回字段；未改变取数范围、复权口径、
> 生命周期调用、撮合/费用/持仓语义或任何回测可观察结果。

### 二、云端复权口径复核（金标准终裁）

| 表 | 云端口径 | 是否还原 raw |
|---|---|---|
| `stock_daily` | **qfq**（前复权） | ✅ 还原 |
| `etf_daily` | **qfq** | ✅ 还原 |
| `etf_minutes` | **qfq** | ✅ 还原 |
| `stock_minutes` | **raw**（不复权） | ❌ 不还原 |

**金标准方法**（`docs/evidence/caliber-recheck-gold-standard-20260930.md`）：
用 `amount / volume` **反推真实成交价（VWAP）**——该基准与复权口径**正交**
（不依赖任何口径判定），是本议题唯一无循环的判据。云端单位：
日线 vol 以「手」计、amount 以「千元」计 → 隐含价 = `amount/vol × 10`；
分钟 vol 以「股」计、amount 以「元」计 → 隐含价 = `amount/vol × 1`。

**终裁结论**：两日线表在「大幅度子集」（`|adj_latest/adj_i − 1| > 5%`，
判定力最强）上 `err_qfq` 比 `err_raw` 小 **32~75 倍** → 云端为 qfq；
`stock_minutes` 的 `close/implied = 1.0000` → 云端为 raw。

**一次错误复核已推翻并回退**（教训固化）：
2026-09-30 曾据 `caliber-matrix-full-review-20260930.md` 得出
「两日线表 = raw」并据此把 `_QFQ_CALIBER_TABLES` 收缩为 `{etf_minutes}`（P1），
在 P2 重拉中**写入 209,640 行错误数据**（2018-01-02~2018-04-27）。
该结论的「独立基准」用的是**云端 `stock_minutes`**——而该表同为需判定对象，
构成**与原矩阵同型的循环论证**。经生产 `UnitCheck` 校验器
（`amount/(close×volume)≈1`，正交检查）抛出隔离告警后发现，
已回退代码与测试、修复受损行（重拉 2018-01-01~2018-05-01 并复核通过）。

**代码落点**：`quantstudio/pipeline/sources/mcp_adapter.py` ——
`_QFQ_CALIBER_TABLES = frozenset({"stock_daily", "etf_daily", "etf_minutes"})`
（维持原状），还原分支按表判定；元数据新增 `caliber` 字段
（`"qfq"` / `"raw"`）。历史错误结论文档已就地加 **作废标注**。

### 三、缺陷 1 修复与运维改进

| 文件 | 改动 | 说明 |
|---|---|---|
| `quantstudio/pipeline/daemon.py` | +72/−6 | ① `run_once` 窗口覆盖（`--start-date` / `--end-date`，仅本次运行、dict 复制不动 `self.tasks_cfg`）；② 失败任务**跳过** QFQ 收尾链（原条件仅判 `not _cancelled`、**不判 `task_ok`**，导致 fetch 失败仍进入逐股重锚→实测 47 只/5 小时且零写入）；③ 候选链逐源跳过原因（工作包 B，日志面归属，不参与行为/门禁决策） |
| `quantstudio/pipeline/qfq_resident_orchestrator.py` | +28 | applying 相位**运行预算截断**（本轮截断、下轮续做；未处理者仍为 pending，由下一轮 `recover_pending_due` 幂等回收，不丢不重）；`CycleSummary` 增 `apply_truncated/apply_processed/apply_remaining` |
| `quantstudio/pipeline/qfq_orchestrator_types.py` | +7 | 新增 `apply_budget_sec`（默认 **1800 s**，**0/负 = 不限**，回退旧行为） |
| `quantstudio/pipeline/quality_audit.py` | +310 | 新增**口径漂移阻断门禁** `_audit_caliber_drift` —— 监控 `_QFQ_CALIBER_TABLES` 成员表口径翻转，`stock_minutes` 一旦翻转为 qfq 即阻断 |
| `quantstudio/pipeline/mcp/client.py` | +9 | **TLS 契约修复**：请求级显式传 `verify=self.tls_verify`。仅设 `_session.verify` 不足——请求级 `verify=None` 时 requests 的 `merge_environment_settings` 会优先采用环境变量 `REQUESTS_CA_BUNDLE`/`CURL_CA_BUNDLE`，使自签证书开发 IP（`tls_verify=false`）握手必然失败 |
| `config/profiles/mcp_only/sources_config.json` | +1 | 启用 `export_async` |

### 四、新增测试套件（+910 行）

| 测试文件 | 用例数 | 覆盖 |
|---|---|---|
| `tests/test_caliber_drift_gate.py` | 15 | 口径漂移门禁：成员表翻转阻断 / 非成员表放行 / fail-fast 锁 |
| `tests/test_restore_to_raw_caliber.py` | 13 | 表级还原语义（日线表应还原）+ A4/R-3 fail-fast 锁 |
| `tests/test_defect1_repair_completion.py` | 12 | P1 窗口覆盖 + P2① 失败跳过 + P2② 预算截断 |
| `tests/test_minute_raw_vs_daily.py` | 2 | 分钟 raw 与日线 qfq 的口径区分 |

### 五、证据与设计文档（35 个新增，均入 `docs/`）

- **口径线**：`cloud-caliber-matrix-forensics-20260927.md`（原矩阵取证）、
  `caliber-recheck-gold-standard-20260930.md`（**金标准终裁 + 完整事故复盘**）、
  `caliber-drift-gate-forensics-20260928.md`、`etf-minutes-caliber-correction-20260930.md`、
  `stock-daily-caliber-correction-20260930.md`、`stock-daily-inflation-forensics-20260930.md`
- **已作废标注**：`caliber-matrix-full-review-20260930.md`（❌ 被金标准推翻）、
  `caliber-matrix-correction-design.md`（❌❌ 方案作废）、
  `caliber-matrix-correction-design-v2-reaudit-20260930.md`（❌ 复审结论作废）、
  `caliber-matrix-correction-v21-acceptance-20260930.md`（❌ 验收作废）
- **缺陷 1 线**：`defect1-root-cause-confirmed-20260928.md`、
  `defect1-repair-attempt1-failure-20260928.md`、`defect1-write-phase-bottleneck-20260930.md`、
  `defect1-a1-validation-20260929.md`、`defect1-a2r1-throughput-20260930.md`、
  `defect1-a3-baseline-decision-20260930.md`、`defect1-rework-verification-20260929.md` 等
- **锚点漂移线**：`anchor-drift-forensics-20260926.md`、`anchor-drift-fix-design.md`、
  `anchor-drift-fix-design-a-prime.md` 及对应审计文档
- **交付记录**：`docs/handoff/` 下 4 份基线/差异快照

### 六、仓库卫生

本次全量同步中发现 **76 个 pytest 临时产物**
（`agent_workspace/_pytmp*/tmp*.db`、`tmpdir*/`、`pytest_basetemp*/`）
曾被误纳入，**已从版本控制移除**，并在 `.gitignore` 补齐对应忽略规则防复发。
真实交付的探针脚本与证据输出（`agent_workspace/_probe_*`、`_a1_*`、`_b1_*` 等）正常保留入库。

---

### 附：本次同步的完整文件变更清单

| 类别 | 文件数 | 主要路径 |
|---|---|---|
| 框架代码 | 7 | `quantstudio/pipeline/{daemon,qfq_invariant,qfq_resident_orchestrator,qfq_orchestrator_types,quality_audit}.py`、`pipeline/mcp/client.py`、`pipeline/sources/mcp_adapter.py` |
| 测试 | 4 | `tests/test_{caliber_drift_gate,restore_to_raw_caliber,defect1_repair_completion,minute_raw_vs_daily}.py` |
| 文档 | 35 | `docs/**`（设计 / 审计 / 证据 / 交付记录） |
| 配置 | 1 | `config/profiles/mcp_only/sources_config.json` |
| 探针与证据输出 | 148 | `agent_workspace/**`（真实脚本与输出，排除已清理的临时产物） |
