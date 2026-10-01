# -*- coding: utf-8 -*-
"""r1 就绪条件 ② 复验（**纯只读**）：qfq 周期态 / 槽位 / 水位口径。

用途：在周一重启窗内，对生产库做一次只读复核，确认「陈旧周期态已回收」未退化。
      对应 A 件 §7.4 判定的对象：qfq_cycle_run（非终态残留）、qfq_trigger_queue
      （in_progress 槽位）、qfq_watermark_intent（pending）、qfq_discovery_baseline。

性质：`duckdb.connect(..., read_only=True)`；不写、不 checkpoint、不建索引、不启动进程。
      每库 open/close 各一次。

用法：
    py -3.11 docs/evidence/_r1_readiness_cycle_state.py
"""
from __future__ import annotations

from pathlib import Path

import duckdb

QS_ROOT = Path(__file__).resolve().parent.parent.parent
DATA = QS_ROOT / "data"
DBS = [DATA / "quantstudio.db", DATA / "qfq_aux.db"]
TABLES = (
    "qfq_cycle_run",
    "qfq_trigger_queue",
    "qfq_watermark_intent",
    "qfq_discovery_baseline",
)


def main() -> int:
    for db in DBS:
        print(f"=== {db.name} ===")
        if not db.exists():
            print("  (db 不存在)")
            continue
        try:
            con = duckdb.connect(str(db), read_only=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [只读打开失败] {type(exc).__name__}: {exc}")
            continue
        try:
            names = {
                r[0]
                for r in con.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            }
            for t in TABLES:
                if t not in names:
                    print(f"  {t}: (本库无此表)")
                    continue
                if t == "qfq_discovery_baseline":
                    n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                    print(f"  {t}: total={n}")
                else:
                    rows = con.execute(
                        f"SELECT status, count(*) FROM {t} GROUP BY status ORDER BY 2 DESC"
                    ).fetchall()
                    print(f"  {t}: {rows}")
        finally:
            con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
