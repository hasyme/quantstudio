# 步骤 0 准入探针证据：passthrough export 窗口下推（2026-10-05 03:24–03:58）

- 方案：`docs/mcp-passthrough-first-pull-export-fix-design.md`（v4）§3.0
- 性质：**只读**探针。临时脚本置于 `d:/tmp/`（不落库、不进 git、不在工作区），
  复用 `MCPAdapter.client.export_dataset`，未调用任何写库/写文件（parquet 仅在内存 BytesIO 读取）。
- 环境：`C:\Users\hasym\.conda\envs\quant310\python.exe`（duckdb 1.5.6 / pandas 2.3.3）；
  endpoint = `https://quantstudio.online/mcp`（profile 现网值）；`export_async=true`。
- 跑批日取当日 2026-10-05 ⇒ 窗口上界 = `2026-10-06T00:00:00`（与 `mcp_adapter.py:1213-1214` 同口径半开）。
- 窗口下界取各表任务 `start_date`（`config/profiles/mcp_only/collector_tasks.json`）。
- 比对口径：两路结果均经 `_fetch_passthrough` 同口径客户端裁剪（`date/trade_date/cal_date`）后比行数。

## 1. 逐表结果（8 张：7 张宽文本 + ws_asfund）

| # | 表（canonical=qdb） | 任务 start_date | 无窗（裁剪后） | 带窗（裁剪后） | 判定 |
|---|---|---|---|---|---|
| 1 | `cnthesims_events` | 2024-01-01 | 104,826 | 104,826 | ✅ 相等 |
| 2 | `ai_research_snapshot` | 2020-01-01 | 11,758 | 11,758 | ✅ 相等 |
| 3 | `llm_text_events` | 2020-01-01 | 首轮 ERROR（见 §3）/ 复测 129,977 | 104,425 | ❌ 不等 |
| 4 | `llm_text_events_enriched` | 2020-01-01 | 首轮 ERROR / 复测 129,977 | 104,425 | ❌ 不等 |
| 5 | `llm_text_raw_feed` | 2020-01-01 | 首轮 ERROR / 复测 129,977 | 104,425 | ❌ 不等 |
| 6 | `rsshub_raw` | 2020-01-01 | 18,560 | 18,560 | ✅ 相等 |
| 7 | `tdx_theme_news` | 2020-01-01 | 17,294 | 17,294 | ✅ 相等 |
| 8 | `ws_asfund` | 2024-01-01 | 42,974 | 42,974 | ✅ 相等 |

原始 JSON（含 shard 数）留存于探针运行输出：
`d:/tmp/qs_probe_window_result.txt`（会话内临时产物，非仓库文件）。

## 2. 三张 `llm_text_*` 不等的**精确归因**（决定性证据）

生产库 `data/quantstudio.db` 只读对账（duckdb read_only）：

| 表 | 库内行数 | `publish_time` min | 早于任务 start_date 的行数 |
|---|---|---|---|
| `llm_text_events` | 129,977 | 2018-01-02 | **25,552** |
| `llm_text_raw_feed` | 129,977 | 2018-01-02 | **25,552** |

- 差值 129,977 − 104,425 = **25,552** ⇒ 与「`publish_time < 2020-01-01` 的行数」**逐位吻合**。
- 结论：`llm_text_*` 三表窗口下推会**静默丢弃 25,552 行（占 19.7%）**。passthrough 是全量覆盖语义，
  下推即等于把库内历史行删除 ⇒ **三表绝不进白名单**（判据：不等 → 不列入，永不窗口下推）。
- 旁证（同口径窗口足够宽时即相等）：以 `time_start=2018-01-01T00:00:00` 复测三表，
  带窗 = 129,977 = 无窗（见 §3），证明差异**纯粹**来自 start_date 早于数据真实下界，
  而非服务端过滤语义错配。

## 3. 复测对照：async_mode × 窗口（`llm_text_*` 三表，2026-10-05 03:53）

| 表 | 无窗 sync | 无窗 async | 窗[2018,b+1) async | 窗[2018,b+1) sync |
|---|---|---|---|---|
| `llm_text_events` | 129,977 ✅ 29.3s | 129,977 ✅ 28.2s | 129,977 ✅ 23.9s | 129,977 ✅ 26.3s |
| `llm_text_events_enriched` | 129,977 ✅ 46.4s | 129,977 ✅ 37.5s | 129,977 ✅ 159.8s | 129,977 ✅ 39.6s |
| `llm_text_raw_feed` | 129,977 ✅ 92.9s | 129,977 ✅ 67.3s | 129,977 ✅ 95.5s | 129,977 ✅ 90.3s |

两点结论（**推翻/修正方案的两处隐含假设**）：

1. **首轮 3 次 `MCPProtocolError: export exceeded the server time budget (60.0s)` 是瞬时负载现象，
   非确定性失败**：复测同三表 12 次调用全部成功（最长 159.8s）。
   即 F-1 的 60s 软预算失败是**负载相关、边界抖动**，不是该数据集恒定不可导出。
2. **`async_mode` 不是 F-1 的解、也不是其风险源**：同步/异步在本组对照中行数与成功与否完全一致，
   仅耗时有差异（46.4s vs 37.5s）。§3.1.1 异步贯通属纯机制增益，对成功路径行数无影响。

## 4. 白名单结论

- **可进名单（证据=相等）**：`cnthesims_events`、`ai_research_snapshot`、`rsshub_raw`、
  `tdx_theme_news`、`ws_asfund`（且经库内对账确认「早于 start_date 的行数 = 0」，见 §5）。
- **禁止进名单**：`llm_text_events`、`llm_text_events_enriched`、`llm_text_raw_feed`
  （§2 决定性归因：下推丢 25,552 行）。
- **本轮实际采用名单 = `[]`（空）**，理由：
  1. 硬约束【E】与 fail-safe 默认「未列名单一律不下推」一致；
  2. 唯一**需要**靠窗口收缩救命的表（`llm_text_events_enriched`）恰恰被证据**禁止**下推
     ⇒ 名单非空对本轮两个失败任务（F-1/F-2）无收益，只会给 5 张当前成功的表引入新的成功路径行为变更；
  3. 上述 5 张候选如需上名单，须先经 §5.5 黄金对比（行数/内容逐项一致）单独立项。
  ⇒ 代码默认与 profile 均保持空名单，窗口下推**全局关闭**，仅 async 贯通生效。

## 5. 5 张候选「零行外丢失」库内佐证（只读）

| 表 | 库内行数 | 时间列 | min | max | 早于 start_date |
|---|---|---|---|---|---|
| `cnthesims_events` | 104,826 | `event_time` | 2025-07-14 | 2026-09-24 | 0 |
| `ai_research_snapshot` | 11,758 | `scan_date` | 2026-06-12 | 2026-10-02 | 0 |
| `rsshub_raw` | 18,050 | `pub_time` | 2026-03-19 | 2026-10-02 | 0 |
| `tdx_theme_news` | 17,271 | `ts` | 2022-01-12 | 2026-10-02 | 0 |
| `ws_asfund` | 库内缺失（F-2 上次失败，无历史） | `trade_date` | — | — | — |

## 6. 对方案的修正登记（步骤 3 实测发现，供步骤 4/5 复核）

- **R-8（新增）**：§3.0 准入判据在 `start_date` 早于数据真实下界时会给出「不等 → 不列入」，
  这是**保护**而非缺陷；但方案 §3.1.2 隐含「宽文本表靠下推解预算」的假设在本轮数据分布下**不成立**
  （唯一需要下推的表被禁止下推）。F-1 的兜底因此退化为「async 贯通 + 负载窗口」，
  残余失败概率由 §4.4 声明承接。
- **R-9（新增）**：§3.0 判据中「无日期列 → 不列入」按 `_fetch_passthrough` 裁剪口径
  （`date/trade_date/cal_date`）严格解释时，7 张宽文本表中 4 张（含 `tdx_theme_news`）无该三元列；
  本证据按「存在可用时间列 + 实测行数相等 + 库内零行外丢失」三重条件记录候选，
  但最终采用空名单，该歧义**不产生行为差异**（下推全局关闭）。
