# V7 在线实验 A1 结论：停摆依旧 —— 但 V7 **从未作用于 stock_daily**（配置性失效，非 V7 失效）

日期：2026-10-03 23:56:16 → 2026-10-04 00:43:05 | PID 11676 | 退出码 75(EX_TEMPFAIL)
解释器：`C:\Users\hasym\.conda\envs\quant310\python.exe`（duckdb **1.4.5**，合规、未过闸逃生阀）
开关：`QS_DUCKDB_WRITE_REBUILD_PK_BATCHES=25`（V7 唯一变量；V1/V2/V4 全关；看门狗 600s/300s 默认开）
备份：`data/snapshots/quantstudio_before_v7online_20261003.db`（运行前，3.69GB）
日志：`data/logs/_v7online.err.txt`；诊断：`data/logs/duckdb_write_stall.jsonl`

## 时间线

| 时刻 | 事件 |
|---|---|
| 23:56:16 | 启动；`INCREMENTAL: 2022-08-12 → 2026-10-03`（`last_watermark=1660147200000`） |
| 23:56:18 | 自动补跑依赖表 `stock_daily_valuation`（范围 2022-07-13~2026-10-03，est 1400 万行） |
| 00:05:50 | **V7 首次触发**：`table=stock_daily_valuation rows=8125540 cols=10 pk=code,time 耗时=30.8s` |
| 00:07→00:13 | valuation 写批持续（全为「新增 0 + 更新 50000」），V7 累计触发 **4 次**（每 25 批一次） |
| 00:28:04 | `stock_daily` 首批通过校验（passed=50000）→ 写批开始 |
| **00:38:05** | **S1 600s 超时 ⇒ CRITICAL + interrupt**（table=stock_daily rows=50000） |
| **00:43:05** | **S2 300s ⇒ 诊断落盘 + `os._exit(75)`** |

## 判定

**FAIL（停摆依旧）**，但**不可归因于 V7 无效**，理由见下。

## 关键发现

### 1. V7 从未作用于出事的表（配置性失效）
`_maybe_rebuild_pk_after_batch()` 由 `write()` 对**每张表**调用，且 `_rebuild_batch_counter`
**跨表共享**（`writers.py:1180`），而 `QS_DUCKDB_WRITE_REBUILD_PK_TABLES` 默认不限定作用域。
⇒ valuation 先跑 111 批，把 4 次重建**全部消耗在 valuation 上**（00:05:50 等，rows=8,125,540）。
`stock_daily` **一个批都没写成**（第 1 批即停），**永远等不到第 25 批的重建**。
⇒ 本轮 V7 对目标表零作用。**要检验 V7，必须让它在 stock_daily 首个写批之前生效。**

### 2. 机理修正：触发点 =「首个含新主键的批」，而非「第 57 批」
落地分片实证（`data/mcp_landing/.../j_1791044025_stock_daily_part_00000.parquet`）：
rows=50000，time 范围 **1660262400000 ~ 1661472000000 = 2022-08-12 ~ 2022-08-26**；
而库内 `stock_daily` 数据只到 **2022-08-15**（max_time=1660492800000）
⇒ **首批即含 2022-08-16 起的新主键**，是混合批（count>0 ⇒ ON CONFLICT 分支）。

| 场景 | 前序纯更新批 | 停摆批序号 | 停摆批语句形态 | 含新主键 |
|---|---|---|---|---|
| 四次事故（无水印全量回填） | 56 批 | 57 | 纯 INSERT（count=0） | ✅ |
| 本轮（有水印增量） | 0 批 | **1** | ON CONFLICT（count>0，混合批） | ✅ |

⇒ 停摆与**批序号（57 vs 1）**、**语句形态（纯 INSERT vs ON CONFLICT）**均无关；
两次唯一共同点是**「本批向 stock_daily 引入了尚不存在的新主键」**。
机理假设由「累积 56 批 upsert 后 ART 退化」**收紧为「首次插入新主键时触发」**。

### 3. V7 在线确有余兴收益：回收空间（缓解 F8）
库体积 3.96GB →（valuation 写入膨胀）**4.53GB** →（V7 重建 + 自动检查点）**回到 3.96GB**。
离线探针（`hang2_v7_pk_rebuild.txt`）只测到重建瞬间 +126.9MB，**未含检查点回收**；
在线实测证明 **V7 + 检查点可真正回收膨胀**。V7 对 F8 有效。

### 4. 水印状态（影响实验同构性）
`source_watermark` 中 `('mcp','stock_daily','daily').last_date = 1660147200000`（2022-08-12），
由 17:59 那一轮写入（updated_at 18:34:22）。A0 基线（17:58:43）时确实为 None。
⇒ 本轮是**增量**而非四次事故的**全量回填**，批结构不同（已在判定中计入）。

## 下一步（待批准）

要真正检验 V7，须让重建发生在 **stock_daily 首个写批之前**：
- 方案 A（不写代码）：批前对生产库 `stock_daily` 手动做一次 PK 重建（离线实测 7.7s、
  MVCC 不阻塞读、行数与主键守恒），立即重跑同一增量，对比首批是否仍停摆。
- 方案 B（代码）：给 V7 增加「任务/表首次写批前置重建」触发点，并用
  `QS_DUCKDB_WRITE_REBUILD_PK_TABLES=stock_daily` 限定作用域、消除跨表计数器耦合。
- 若 A/B 仍停摆 ⇒ V7 证伪，再按约定回到 1.5.6 讨论（届时需显式绕过版本闸并违反钉版）。
