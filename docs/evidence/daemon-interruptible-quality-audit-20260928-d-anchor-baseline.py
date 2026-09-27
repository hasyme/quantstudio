# -*- coding: utf-8 -*-
"""D 件 §3 待补实测：分钟表 AdjustmentAnchor 单语句耗时基线（只读、显式窗口）。

用法：python tmp_d_anchor_baseline.py probe|measure [tables...]
  probe   —— 仅探锁（SHOW TABLES + 行数），不做重查询
  measure —— 逐表实测 anchor SQL 墙钟（默认四价格表）
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import duckdb

DB = Path(r"D:\miniQMT策略实盘\QuantStudio\data\quantstudio.db")

ANCHOR_SQL = '''WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY code ORDER BY time) first_n,
              ROW_NUMBER() OVER(PARTITION BY code ORDER BY time DESC) last_n
    FROM "{table}" WHERE close>0)
    SELECT COUNT(*) FROM ranked WHERE
    (last_n=1 AND close_front IS NOT NULL AND ABS(close_front/close-1)>0.02)
    OR (first_n=1 AND close_back IS NOT NULL AND ABS(close_back/close-1)>0.02)'''


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "probe"
    tables = sys.argv[2:] or ["etf_daily", "stock_daily", "etf_minutes", "stock_minutes"]
    print(f"[cfg] mode={mode} tables={tables} db={DB} exists={DB.exists()}")
    conn = duckdb.connect(str(DB), read_only=True)
    try:
        present = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        print(f"[probe] SHOW TABLES ok, tables={len(present)}")
        for t in tables:
            if t not in present:
                print(f"[skip] {t} 不存在")
                continue
            n = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            if mode == "probe":
                print(f"[rowcount] {t} = {n}")
                continue
            t0 = time.perf_counter()
            anchor = conn.execute(ANCHOR_SQL.format(table=t)).fetchone()[0]
            dt = time.perf_counter() - t0
            print(f"[anchor] {t} rows={n} anchor_hits={anchor} elapsed={dt:.3f}s")
    finally:
        conn.close()
    print("[done]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
