# H9：用真实分片 + 真实 writer 语义在副本上复现 —— **失败（副本无判定力）**

日期：2026-10-04 | 脚本：`docs/evidence/hang2_h9_realbatch.py`
（**该脚本及其依赖的 `data/bench` 工作目录已于 2026-10-05 清理**，结论与数据引用保留；复跑需重建副本库）
目的：V7（A1/A2）已证伪，回议 1.5.6；但 1.5.6 只有在**能复现**的前提下才可判定。
H5'/V8 等离线复现均不挂起，怀疑其 df 由 SQL fetchdf 构造，与线上真实链路不等价，
故本轮改用**线上停摆的同一块真实 parquet 分片**走真实 aligner/validator/writer 语义。

## 数据（与线上逐项核对一致）

源头分片：`data/mcp_landing/exp_stock_daily_5b6c0ef5/j_1791050501_stock_daily_part_00000.parquet`
- 该片即 A2（02:26:17 停摆）的首个写批，mtime 02:03:52
- 时间范围 **2022-08-12 ~ 2022-08-26**；生产 GM 库 `stock_daily` 数据截至 **2022-08-15**
  ⇒ 首批即含 2022-08-16 起的新主键（与 A1 结论一致）
- daemon 日志 "50000 行 / 4911 码"；本 harness aligned 结果 **50000 行 / 4911 码 / 11 个交易日** ✔

链路复刻度：
- `normalize_mcp_adj_factor_df` 构造 `adj_factor_df` + 从 raw drop `adj_factor`（对齐 daemon:1055-1081）
- `qfq_snapshot_kwargs(...)` 提供 `adj_latest_map/adj_earliest_map`（daemon `_qfq_snapshot_kwargs`）
- `aligner.align(raw, 'stock_daily', 'mcp', adj_factor_df=..., freq='daily', **snap)`（daemon:1125）
- 写前按实表 42 列投影、并补 `data_source`（对齐 `_stamp_and_write`，daemon:3159-3166）
- 写语句严格照抄 `writers.py:1403/1423/1435`：count SELECT + ON CONFLICT DO UPDATE（或纯 INSERT）

偏差（已知、已记录）：本 harness 未传 `namechange_df`/`valuation_df`，
导致 validator rejected=1888（线上 passed=50000 rejected=0）；故写 df 取未剔除的
aligned 全集 50000 行，与线上行规模一致。

## 结果：全部成功，无一挂起（duckdb 1.4.5）

| 变体 | 分支 | updated | count_s | dml_s | 结果 |
|---|---|---|---|---|---|
| 真实首片（2022-08-12~26，混合批） | ON CONFLICT | 7910 | 0.06 | 2.10 | **ok 2.3s** |
| 同上 + 生产 `quantstudio.db.wal`（11.57MB） | ON CONFLICT | 7910 | 0.05 | 2.38 | **ok 18.1s**（含 WAL 回放） |
| 次片 part_00001（2022-08-26 起，全为新键） | **纯 INSERT** | 0 | 0.06 | 1.39 | **ok 1.6s** |
| 同上 + 第二 RW 连接并发读（19 次） | **纯 INSERT** | 0 | 0.03 | 1.42 | **ok 1.5s** |

（生产同场景：>600s 停摆、interrupt 无效、硬退出 75）

## 判定

**副本环境对 stock_daily 写入停摆无判定力。** 已逐项排除：

| 已排除因素 | 依据 |
|---|---|
| SQL 语句形态 | ON CONFLICT 与纯 INSERT 在副本均 1.6~2.3s 成功 |
| df 来源/dt粗细粒度（真实 parquet vs 合成 fetchdf） | 真实分片 + 真实 aligner 仍成功 |
| 库内容与表状态（含 V7 重建后的全新 ART） | 副本由生产库逐字节复制（2,764,437 行） |
| WAL 残留（上次硬退出事务） | 连同 11.57MB WAL 复制，仍成功 |
| 并发读（simulate shared_conn） | 第二 RW 连接并发读 19 次，仍成功 |

⇒ 触发因素**不在数据、不在 SQL、不在库文件内容**，而只能存在于**真实 daemon 进程/运行环境**
（多线程、`_write_guard` 定时器、3A 写锁、带 namechange/valuation 的完整 aligner、
长生命周期连接的内部状态等）。

## 对 1.5.6 决策的影响（重要）

**任何基于副本的 1.5.6 测试都不可判读**：1.4.5 在副本上本就 1.5~2.3s 通过，
因此"1.5.6 在副本上通过"不构成任何结论（ baseline 已通过）。
要判定 1.5.6，必须先获得**能复现**的环境。

## 下一步候选（按推荐度）

1. **完整 daemon 指向副本**（唯一还剩 fidelity 的零风险方案）：
   复制 profile 并把 `data_config.json` 的 `path` 改指向 `data/bench/...` 副本，
   跑完整增量（联网 ~1.5h）。
   - 若在 **1.4.5** 下复现 ⇒ 得到可用 harness ⇒ 再用同一 harness 跑 **1.5.6**，决定性且零生产风险。
   - 若仍不复现 ⇒ 停摆与生产进程强绑定，副本路线彻底走不通，需在生产上做版本决策。
2. 直接把 1.5.6 用在生产（需 `QS_DUCKDB_VERSION_GATE=0` + 违反 `duckdb>=1.4.5,<1.5` 钉版）。
