"""方案 C 实施脚本（用户裁定）：主库 cutover 记录 aux_db_path 迁移重锚。

仅改 `qfq_source_cutover.aux_db_path` 一列（+ updated_at 留痕）；evidence_path 不动。
执行前只读预检（原值形态 + 目标文件存在性），执行后读回验证；输出 JSON 供证据落档。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from quantstudio.pipeline.snapshot_lock import locked_connect  # noqa: E402

DB = ROOT / "data" / "quantstudio.db"
TARGET = DB.parent / "qfq_aux_mcp_gen1.db"
CUTOVER_ID = "b6_formal_20260807_v2"
APPLY = "--apply" in sys.argv

report: dict = {
    "db": str(DB),
    "cutover_id": CUTOVER_ID,
    "target": str(TARGET),
    "target_is_file": TARGET.is_file(),
    "mode": "apply" if APPLY else "dry-run",
    "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
}

# ---------- 阶段 1：只读预检 ----------
ro = duckdb.connect(str(DB), read_only=True)
try:
    rows = ro.execute(
        "SELECT cutover_id, price_source, source_generation, status, aux_db_path, "
        "evidence_path, updated_at FROM qfq_source_cutover").fetchall()
    report["rows_before"] = [list(map(str, r)) for r in rows]
    assert len(rows) == 1, f"预期 1 条 cutover 记录，实测 {len(rows)}"
    cutover_id, price_source, src_gen, status, cur_aux, cur_ev, cur_upd = rows[0]
    assert cutover_id == CUTOVER_ID, f"cutover_id 不符: {cutover_id}"
    assert status == "active", f"status 非 active: {status}"
    assert cur_aux and "miniQMT" in cur_aux, f"原值不含旧机标识，拒绝执行: {cur_aux!r}"
    assert Path(cur_aux).name == TARGET.name, f"原值 basename 与目标不一致: {cur_aux!r}"
    assert not Path(cur_aux).is_file(), f"原路径竟然存在，无需重锚: {cur_aux}"
    assert TARGET.is_file(), f"目标文件不存在，拒绝执行: {TARGET}"
    old_aux = str(cur_aux)
    report["old_aux_db_path"] = old_aux
    report["new_aux_db_path"] = str(TARGET)
    report["evidence_path_unchanged"] = str(cur_ev)
    report["updated_at_before"] = str(cur_upd)
finally:
    ro.close()

# ---------- 阶段 2：写锁 + UPDATE ----------
if APPLY:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lc = locked_connect(lambda: duckdb.connect(str(DB)), "aux-path-reanchor:manual")
    conn = lc.__enter__()
    try:
        conn.execute(
            "UPDATE qfq_source_cutover SET aux_db_path=?, updated_at=? WHERE cutover_id=?",
            [str(TARGET), now, CUTOVER_ID])
        after = conn.execute(
            "SELECT cutover_id, aux_db_path, evidence_path, updated_at "
            "FROM qfq_source_cutover WHERE cutover_id=?", [CUTOVER_ID]).fetchone()
        report["rows_after"] = [str(x) for x in after]
        assert after[1] == str(TARGET), f"写回校验失败: {after[1]!r}"
    finally:
        conn.close()
        lc.__exit__(None, None, None)
else:
    report["note"] = "dry-run：未写入；加 --apply 执行"

print(json.dumps(report, ensure_ascii=False, indent=2))
