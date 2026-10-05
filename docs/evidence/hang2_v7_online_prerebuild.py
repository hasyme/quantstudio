# -*- coding: utf-8 -*-
"""V7 在线实验 A2：对**生产库** stock_daily 做批前主键重建（默认只检查，--apply 才执行）。

用途：A1 已证明 stock_daily 首个写批即停摆，且 V7 因跨表共享计数器从未作用于该表。
本脚本在**重跑在线增量之前**先把 stock_daily 的 ART 索引重建为全新状态，
构成单变量 A/B：唯一差异 = 首写批之前 stock_daily 是否刚被重建。

用法：
  python docs/evidence/hang2_v7_online_prerebuild.py            # 只检查，不改动
  python docs/evidence/hang2_v7_online_prerebuild.py --apply    # 执行重建

安全：重建逻辑复用已排坑的离线探针（实表 schema + PK 按括号内逗号切分），
单事务 CREATE(带PK) + INSERT SELECT + DROP + RENAME；执行前后校验行数与主键守恒。
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import time

import duckdb

DB = os.path.join("data", "quantstudio.db")
TABLE = "stock_daily"
NEW = "_v7_stock_daily_new"
OUT = os.path.join("docs", "evidence", "hang2_v7_online_prerebuild.txt")


def log(L, s):
    L.append(s)
    print(s)


def snapshot(conn):
    rows = conn.execute("SELECT COUNT(*) FROM %s" % TABLE).fetchone()[0]
    cols = [r[0] for r in conn.execute("DESCRIBE %s" % TABLE).fetchall()]
    pk = conn.execute(
        "SELECT constraint_text FROM duckdb_constraints() WHERE table_name=? "
        "AND constraint_type='PRIMARY KEY'", [TABLE]).fetchall()
    mn, mx = conn.execute("SELECT MIN(time), MAX(time) FROM %s" % TABLE).fetchone()
    return dict(rows=rows, ncols=len(cols), pk=[p[0] for p in pk], mn=mn, mx=mx)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="实际执行重建（默认只检查）")
    args = ap.parse_args()

    L = []
    L.append("V7 A2 批前重建 @ %s  apply=%s" % (time.strftime("%Y-%m-%d %H:%M:%S"), args.apply))
    log(L, "db=%s size=%.2fGB" % (DB, os.path.getsize(DB) / 1e9))

    c = duckdb.connect(DB, read_only=True)
    try:
        before = snapshot(c)
        try:
            views = c.execute(
                "SELECT view_name FROM duckdb_views() WHERE sql LIKE '%stock_daily%'").fetchall()
        except Exception as e:  # noqa: BLE001
            views = []
            log(L, "views query skipped: %s" % str(e)[:60])
        log(L, "before: rows=%d cols=%d pk=%s" % (before["rows"], before["ncols"], before["pk"]))
        log(L, "before: min=%d max=%d" % (before["mn"], before["mx"]))
        log(L, "views referencing stock_daily=%d %s" % (len(views), [v[0] for v in views[:6]]))
    finally:
        c.close()

    if not args.apply:
        log(L, "CHECK-ONLY：未做任何改动。确认无误后加 --apply 执行。")
        io.open(OUT, "w", encoding="utf-8").write("\n".join(L) + "\n")
        return

    t0 = time.time()
    cn = duckdb.connect(DB)
    try:
        desc = cn.execute("DESCRIBE %s" % TABLE).fetchall()
        cols = ['"%s" %s' % (r[0], r[1]) for r in desc]
        pk_rows = cn.execute(
            "SELECT constraint_text FROM duckdb_constraints() WHERE table_name=? "
            "AND constraint_type='PRIMARY KEY'", [TABLE]).fetchall()
        pk_cols = []
        if pk_rows:
            m = re.match(r"PRIMARY KEY\((.*)\)\s*$", pk_rows[0][0], re.I)
            if m:
                pk_cols = [x.strip().strip('"') for x in m.group(1).split(",") if x.strip()]
        collist = ", ".join(cols)
        if pk_cols:
            collist += ", PRIMARY KEY(%s)" % ", ".join('"%s"' % x for x in pk_cols)
        log(L, "ddl: cols=%d pk=%s" % (len(cols), pk_cols))
        cn.execute('DROP TABLE IF EXISTS "%s"' % NEW)
        cn.execute("BEGIN TRANSACTION")
        cn.execute('CREATE TABLE "%s" (%s)' % (NEW, collist))
        t1 = time.time()
        cn.execute('INSERT INTO "%s" SELECT * FROM %s' % (NEW, TABLE))
        t2 = time.time()
        cn.execute("DROP TABLE %s" % TABLE)
        cn.execute('ALTER TABLE "%s" RENAME TO %s' % (NEW, TABLE))
        cn.execute("COMMIT")
        t3 = time.time()
        log(L, "timing: create=%.1fs copy=%.1fs swap=%.1fs TOTAL=%.1fs"
            % (t1 - t0, t2 - t1, t3 - t2, t3 - t0))
        cn.execute("CHECKPOINT")
    except BaseException as e:  # noqa: BLE001
        log(L, "REBUILD ERROR: %s: %s" % (type(e).__name__, str(e)[:200]))
        try:
            cn.execute("ROLLBACK")
        except Exception:  # noqa: BLE001
            pass
        io.open(OUT, "w", encoding="utf-8").write("\n".join(L) + "\n")
        sys.exit(1)
    finally:
        cn.close()

    c2 = duckdb.connect(DB, read_only=True)
    try:
        after = snapshot(c2)
    finally:
        c2.close()
    log(L, "after: rows=%d cols=%d pk=%s" % (after["rows"], after["ncols"], after["pk"]))
    log(L, "after: min=%d max=%d" % (after["mn"], after["mx"]))
    log(L, "db_size_after=%.2fGB" % (os.path.getsize(DB) / 1e9))
    ok = (after["rows"] == before["rows"] and after["ncols"] == before["ncols"]
          and after["pk"] == before["pk"] and after["mn"] == before["mn"]
          and after["mx"] == before["mx"])
    log(L, "VERDICT: %s" % ("守恒 OK —— 可重跑在线增量" if ok else "**不守恒，禁止重跑**"))
    io.open(OUT, "w", encoding="utf-8").write("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
