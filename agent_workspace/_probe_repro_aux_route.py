"""一次性复现脚本（用完即删）：dynamic 世代路由对「迁移前绝对路径」的行为。

只读主库，不写任何数据。
"""
from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from quantstudio.pipeline.qfq_orchestrator_types import QFQOrchestratorConfig  # noqa: E402
from quantstudio.pipeline.qfq_resident_orchestrator import QFQResidentOrchestrator  # noqa: E402
from quantstudio.pipeline.qfq_cutover import (  # noqa: E402
    resolve_runtime_identity, runtime_cutover_record)
from quantstudio.pipeline.qfq_aux_router import (  # noqa: E402
    AuxDbRouter, resolve_runtime_aux_path)

MAIN_DB = ROOT / "data" / "quantstudio.db"
AUX_CFG = ROOT / "config" / "profiles" / "mcp_only" / "qfq_aux_paths.json"

tasks = json.loads(
    (ROOT / "config" / "profiles" / "mcp_only" / "collector_tasks.json").read_text(
        encoding="utf-8"))
raw_qfq = tasks["qfq_orchestrator"]
cfg = QFQOrchestratorConfig.load(raw=raw_qfq)
print(f"[cfg] enabled={cfg.enabled} price_source={cfg.price_source} "
      f"generation_mode={cfg.generation_mode} gen={cfg.source_generation} "
      f"cutover_id={cfg.cutover_id}")

conn = duckdb.connect(str(MAIN_DB), read_only=True)
try:
    ident = resolve_runtime_identity(conn, cfg, allow_prepared=False)
    print(f"[ident] {ident}")
    record = runtime_cutover_record(conn, ident)
    print(f"[record] status={record['status']} aux_db_path={record['aux_db_path']!r} "
          f"evidence_path={record['evidence_path']!r}")

    aux_from_record = Path(record["aux_db_path"])
    print(f"[exists?] record aux exists={aux_from_record.is_file()} "
          f"(旧机目录存在={Path(aux_from_record.anchor).exists()})")
    print(f"[exists?] same-name in current data root="
          f"{(MAIN_DB.parent / aux_from_record.name).is_file()}")

    # daemon 侧统一路由（配置解析，不经记录路径）
    def _db_read(sql, params=None):
        return conn.execute(sql, list(params or [])).fetchall()

    p, reason = resolve_runtime_aux_path(
        main_db=MAIN_DB, duckdb_read=_db_read, price_source=cfg.price_source,
        config_path=AUX_CFG)
    print(f"[daemon route] path={p} exists={p.is_file()} reason={reason}")

    # orchestrator 侧（生产 daemon 走 begin_cycle → prepare_runtime 默认 require_aux=True）
    orch = QFQResidentOrchestrator(
        cfg, main_db=str(MAIN_DB), aux_db=str(MAIN_DB.parent / "qfq_aux.db"),
        fetcher=object(), calendar=object())
    orch.qfq_aux_paths_config = AUX_CFG
    try:
        got = orch.prepare_runtime(conn, require_aux=True)
        print(f"[orch require_aux=True] OK ident={got} aux_db={orch.aux_db}")
    except Exception as exc:  # noqa: BLE001
        print(f"[orch require_aux=True] RAISED {type(exc).__name__}: {exc}")
        traceback.print_exc()

    # 对照：require_aux=False（测试用的调用形态）
    orch2 = QFQResidentOrchestrator(
        cfg, main_db=str(MAIN_DB), aux_db=str(MAIN_DB.parent / "qfq_aux.db"),
        fetcher=object(), calendar=object())
    orch2.qfq_aux_paths_config = AUX_CFG
    got2 = orch2.prepare_runtime(conn, require_aux=False)
    print(f"[orch require_aux=False] OK ident={got2} aux_db={orch2.aux_db}")

    # _aux_query 通道（读取因子观察）是否同样受阻
    try:
        orch2._aux_query("SELECT 1")
        print("[orch _aux_query] OK")
    except Exception as exc:  # noqa: BLE001
        print(f"[orch _aux_query] RAISED {type(exc).__name__}: {exc}")

    router = AuxDbRouter(main_db=MAIN_DB, config_path=None,
                         routes={ident["source_generation"]: record["aux_db_path"]})
    print(f"[router route] {router.routes}")
    try:
        router.path_for(ident["source_generation"], require_exists=True)
        print("[router path_for require_exists=True] OK")
    except Exception as exc:  # noqa: BLE001
        print(f"[router path_for require_exists=True] RAISED {type(exc).__name__}: {exc}")
finally:
    conn.close()
