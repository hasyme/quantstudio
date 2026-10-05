# V7 在线实验 A2 结论：**V7 证伪** —— 批前重建主键未能阻止停摆

日期：2026-10-04 01:31 → 02:41 | PID 24848 | 退出码 75(EX_TEMPFAIL)
解释器：`C:\Users\hasym\.conda\envs\quant310\python.exe`（duckdb **1.4.5**，合规、未用闸逃生阀）
开关：`QS_DUCKDB_WRITE_REBUILD_PK_BATCHES=25`（与 A1 **完全相同**）
备份：`data/snapshots/quantstudio_before_v7online_20261003.db`
日志：`data/logs/_v7a2.err.txt`；重建记录：`hang2_v7_online_prerebuild.txt`

## 实验设计（单变量 A/B）

A1 已证明：`stock_daily` **首个写批**即停摆，而 V7 的 4 次重建全被 valuation 消耗，
**从未作用于 stock_daily**。故 A2 在重跑之前，先**手动把 stock_daily 的主键/ART 重建为全新状态**，
其余配置与 A1 逐项相同 ⇒ 唯一变量 = **首写批之前 stock_daily 是否刚被重建**。

### 批前重建（生产库，单事务）
```
before: rows=2764437 cols=42 pk=PRIMARY KEY(code, "time")  min/max=1577894400000/1660492800000
依赖视图: 0（DROP+RENAME 无断依赖风险）
ddl: cols=42 pk=['code','time']
timing: create=16.9s copy=5.2s swap=6.0s TOTAL=28.2s
after : rows=2764437 cols=42 pk=PRIMARY KEY(code, "time")  min/max 不变
db: 3.96GB -> 3.57GB（-390MB，再次证实 V7 可回收膨胀）
VERDICT: 守恒 OK
```

## 结果：与 A1 **逐秒吻合**

| 项 | A1（无批前重建） | A2（批前重建） |
|---|---|---|
| valuation 写批 | 111 | 111 |
| V7 重建次数（全部落在 valuation） | 4 | 4 |
| stock_daily 首批校验通过 | 00:28:04 | 02:26:17 |
| **S1 600s 超预算 interrupt** | **00:38:05** | **02:36:18** |
| S2 300s 硬退出 75 | 00:43:05 | 02:41:18 |
| 诊断 | phase=dml rows=50000 | phase=dml rows=50000 |
| stock_daily 成功写批数 | **0** | **0** |

诊断原文（A2）：
```
{"ts":"2026-10-04T02:26:18","pid":24848,"phase":"dml","table":"stock_daily",
 "rows":50000,"budget_s":600.0,"hard_abort_s":300.0,
 "interrupt_sent":true,"hard_abort":true,"outcome":"budget_exceeded"}
```

## 判定

**V7 证伪**：把 stock_daily 的 ART/主键重建为全新状态（28.2s，行数与主键守恒）后，
首个写批**依然在 600s 处停摆**，与未重建时逐秒一致。
⇒ **停摆不是由"ART 索引陈旧/退化"引起的**，V7（主键重建）不能作为修复手段。

## 副作用与安全性

停摆后生产库状态干净：rows=**2,764,437（未变）**、42 列、`PRIMARY KEY(code,"time")` 守恒、
min/max 不变、**重复主键组 = 0** ⇒ 无部分提交、无脏数据。

## 机理收敛（截至 A2）

| 轴 | 结论 |
|---|---|
| 单条语句规模（5万→5千） | 无关（A5-1 证伪） |
| 库体积（1.61/2.64/3.13/3.64/3.96GB） | 无关（各次停点恒定） |
| WAL 是否收敛 | 无关（自动收敛；显式 CHECKPOINT 空操作） |
| 语句形态（ON CONFLICT / 纯 INSERT） | 两种都停 |
| 批序号（第 57 批 / 第 1 批） | 无关 —— 见 A1 |
| **ART 索引是否全新（V7 重建）** | **无关（A2 证伪）** |
| **唯一恒定相关量** | **本批向 stock_daily 引入了尚不存在的新主键** |

⇒ 触发条件收敛为：**「向 stock_daily 插入新主键的 DML」本身无限挂起**；
且它与表内已积累的行数（约 275 万）共同出现，但**与索引新旧无关**。

## 下一步

V7 已证伪，按事先约定回到 **1.5.6** 讨论。可选路径（均需重新权衡钉版与版本闸）：
1. **真链路 + 副本 + 1.4.5**：先验证"副本 + 真实 daemon"能否复现停摆；
   若能复现，则获得可用的复现 harness，再用同一 harness 跑 1.5.6（零生产风险、决定性）。
   若不能复现，则副本无判定力，只能上生产。
2. **真链路 + 副本 + 1.5.6**（需 `QS_DUCKDB_VERSION_GATE=0`，但只写副本，不碰主库）。
3. **1.5.6 直接写生产库**（绕闸 + 违反钉版，风险最高）。
