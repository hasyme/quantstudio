# DuckDB 写入分档规避：验收门 3-6 证据（2026-10-02）

> 关联设计：`docs/duckdb-conflict-hang-mitigation-design.md`（v3.1）
> 现场取证：`docs/evidence/duckdb-conflict-hang-forensics-20261002.md`
> 门 1/门 6 用例：`tests/test_writer_dedup_fail_closed.py`（9 例）
> 门 2（长时写入回归）已单独执行并 PASS，见
> `docs/evidence/duckdb-conflict-hang-acceptance-gate2-20261002.md`
> （单次 125.1 分钟、零 stall）。本文件覆盖门 3-6。

## 门 3：契约门 + 6 策略 api_portability —— PASS

```
$ python scripts/run_contract_gate.py --strategies
== 契约套件 ==   全部通过
== 矩阵门禁 ==   OK：契约矩阵门通过（哈希一致 + MD 一致）
== 6 策略 api_portability 冒烟 ==  全部通过
===== CONTRACT GATE : PASS =====
```

## 门 4：既有功能回归 —— 23 failed / 979 passed（均已归因，非本线引入）

工具：`pytest tests/ -k "writer or daemon or duckdb or db_ or 3a or contract or
portability or validator or aligner or quality or checkpoint or lock"`
（排除收集期报错的 `test_consume_whitelist_guard.py`，其 `ModuleNotFoundError` 为既有问题）

**修复前 34 failed → 修复后 23 failed**，本线修复消除了 11 个失败：

| 失败项 | 处置 |
|---|---|
| `test_eps_backfill.py::test_writer_backfill_gate_*`（9 例） | **已由本线修复消除**（列子集回归，见下） |
| `test_pipeline_guardrails.py::test_writer_upsert_distinguishes_new_and_updated` | **已修复**（同上） |

**基线对照**（`git checkout HEAD -- writers.py` 后同集跑）确认以下为**既有失败**，与本线无关：

- `test_fund_contract_wiring_matrix`（7）、`test_lock_preserve_file`（3）、
  `test_quality_orchestrator`（1）、`test_ptrade_public_signature_contract`（1）、
  `test_security_metadata_api`（2）、`test_strategy_name_chinese_contract`（1）、
  `test_inspect_capabilities_f6`（1）——基线版同样失败；
- `test_daemon_lifecycle::test_schedule_skip_weekdays_logic`、
  `test_daemon_hold_and_skip_weekdays::*`（2）——归因**其他会话改动**：
  HEAD 版 `config/profiles/mcp_only/collector_tasks.json` 含 `"skip_weekdays"`，
  工作区版本已移除（`+48/-50`），导致 `KeyError: 'skip_weekdays'`；
- `test_pr6b1_orchestrator`（3）、`test_fin_growth_dividend_staging_tool`（3）、
  `test_first_cover_event_daily_agent`（2 errors）——capability / 生产库状态 /
  控制台 `UnicodeDecodeError: 'gbk'` 等环境类失败，非写入语义回归。

## ⚠️ 本线引入并已修复的真实回归（重要记录）

**现象**：`test_pipeline_guardrails::test_writer_upsert_distinguishes_new_and_updated`
报 `BinderException: table stock_daily has 42 columns but 3 values were supplied`。

**根因**：P1 的纯 INSERT 分支初版写作
`INSERT INTO {table} SELECT * FROM _tmp_write`——**未带列名列表**；
而原 ON CONFLICT 分支为 `INSERT INTO {table} ({col_list}) SELECT * ...`（显式列名）。
当 df 是表的**列子集**（3 列写 42 列表）时，裸 `SELECT *` 会让 DuckDB 按全表列对齐而报错，
**两条路径因此不等价**。

**修复**：纯 INSERT 分支补列名列表，与 ON CONFLICT 保持同一列投影语义：
```python
col_list = ", ".join(df.columns)
conn.execute(f"INSERT INTO {table} ({col_list}) SELECT * FROM _tmp_write")
```

**防复发**：`tests/test_writer_dedup_fail_closed.py::test_column_subset_batch_is_equivalent`
以列子集场景做双库对照（正常态纯 INSERT vs 熔断态强制 ON CONFLICT），钉死该形态。

## 门 5：黄金结果对比 —— 修复前/后逐行一致

方法：同一脚本（`_golden_tmp.py`，场景含全新增 / 同主键重放 / 列子集新增 / 列子集重放）
分别在**基线版**（`git checkout HEAD -- writers.py`）与**修复版**落库并整表导出。

```
修复前 (before):              修复后 (after):
rowcount=4                    rowcount=4
('A0001.SZ', 1700000000000, 1.0)   ('A0001.SZ', 1700000000000, 1.0)
('A0002.SZ', 1700000000000, 2.0)   ('A0002.SZ', 1700000000000, 2.0)
('B0001.SZ', 1700000000000, 3.0)   ('B0001.SZ', 1700000000000, 3.0)
('B0002.SZ', 1700000000000, 4.0)   ('B0002.SZ', 1700000000000, 4.0)
```
⇒ **逐行完全一致**，等价性成立（未改变写入结果）。

## 门 6：异常路径单测 —— PASS（9 例）

`tests/test_writer_dedup_fail_closed.py`：

| 用例 | 覆盖 |
|---|---|
| `test_plain_insert_equals_on_conflict_when_no_conflict` | 双库对照等价性（命门） |
| `test_column_subset_batch_is_equivalent` | 列子集等价性（上述回归锚点） |
| `test_all_new_batch_reports_all_new` / `test_replay_batch_reports_all_updated` | 分档正确性 + upsert 幂等 |
| `test_fail_closed_keeps_int_contract_and_conservation` | 三字段 int + 守恒（防 842 TypeError） |
| `test_fail_closed_does_not_raise_integrity_error` | fail-closed 不误走纯 INSERT |
| `test_sustained_fail_closed_opens_circuit` / `test_circuit_reset_clears_state` / `test_window_slides_out_stale_marks` | 熔断、复位、窗口滑动 |

**真红态验证**：将哨兵临时改回 `None` 后，3 例立即变红，报
`writers.py:915 TypeError: unsupported operand type(s) for -: 'int' and 'NoneType'`
——与审计 P0-1 预测逐字一致；复原后回绿，无探针残留。证明用例非空转。

## 未覆盖

- **门 2 长时写入回归**（3×40min 或 ≥120min，含全新增 / 高更新 ≥50% 两场景、
  SELECT COUNT 与 INSERT 分段耗时观测）——尚未执行，是本线剩余的**核心验收缺口**。

## 环境备注（踩坑留档）

PowerShell 5.x 的 `>` 重定向默认写 **UTF-16LE**（BOM `FF FE`），
`git apply` 会报 `No valid patches in input`。备份 patch 须显式转 UTF-8 无 BOM：
`[System.IO.File]::WriteAllText($dst, $text, (New-Object System.Text.UTF8Encoding($false)))`。
本轮曾因此导致改动临时丢失，靠转码后的 patch 恢复（改动完整，+97/-14 校验通过）。
