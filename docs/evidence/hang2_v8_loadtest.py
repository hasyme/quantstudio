# -*- coding: utf-8 -*-
"""V8 前置验证：duckdb 1.5.6 在生产库副本上的同规模负载 + 读兼容。

用法：<python> docs/evidence/hang2_v8_loadtest.py --db <src> --copy <dst> --log <out>
仅依赖 duckdb（纯 SQL temp 表，不需 pandas）。
"""
from __future__ import annotations
import argparse, io, sys, time, shutil, os

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--copy", required=True)
    ap.add_argument("--log", required=True)
    ap.add_argument("--update-batches", type=int, default=56)
    ap.add_argument("--new-batches", type=int, default=14)
    ap.add_argument("--batch", type=int, default=50000)
    ap.add_argument("--budget", type=float, default=120.0)
    args = ap.parse_args()
    import duckdb
    L = []
    def log(m): L.append(m); print(m, flush=True)

    # 关键前置：1.5.6 能否读取现有库
    c0 = duckdb.connect(args.db, read_only=True)
    n = c0.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
    cols = [r[0] for r in c0.execute("DESCRIBE stock_daily").fetchall()]
    mx = c0.execute("SELECT MAX(time) FROM stock_daily").fetchone()[0]
    c0.close()
    log("READ OK version=%s rows=%d cols=%d max_time=%d" % (duckdb.__version__, n, len(cols), mx))

    if os.path.exists(args.copy): os.remove(args.copy)
    t0 = time.time(); shutil.copyfile(args.db, args.copy)
    log("COPY %.2fGB in %.1fs" % (os.path.getsize(args.copy)/1e9, time.time()-t0))

    conn = duckdb.connect(args.copy)
    col_list = ", ".join(cols)
    update_set = ", ".join("%s=EXCLUDED.%s" % (c, c) for c in cols)
    on_conflict = ("INSERT INTO stock_daily (%s) SELECT * FROM _tmp_write "
                   "ON CONFLICT (code, time) DO UPDATE SET %s" % (col_list, update_set))
    plain = "INSERT INTO stock_daily (%s) SELECT * FROM _tmp_write" % col_list
    day0 = mx + 86400000
    other_cols = [c for c in cols if c not in ("code", "time")]
    ncodes = conn.execute("SELECT COUNT(DISTINCT code) FROM stock_daily").fetchone()[0]

    total = 0.0
    for b in range(1, args.update_batches + 1):
        t = time.time()
        conn.execute("DROP TABLE IF EXISTS _tmp_write")
        conn.execute("CREATE TABLE _tmp_write AS SELECT * FROM stock_daily "
                     "LIMIT %d OFFSET %d" % (args.batch, (b-1)*args.batch))
        upd = conn.execute("SELECT COUNT(*) FROM stock_daily WHERE (code,time) IN "
                           "(SELECT code,time FROM _tmp_write)").fetchone()[0]
        if upd > 0:
            conn.execute(on_conflict)
        else:
            conn.execute(plain)
        conn.execute("DROP TABLE IF EXISTS _tmp_write")
        dt = time.time() - t
        total += dt
        flag = " STALL?" if dt > args.budget else ""
        log("upd  batch %03d rows=%d count=%d %.2fs%s" % (b, args.batch, upd, dt, flag))
        if dt > args.budget:
            log("ABORT: batch %d exceeded budget (possible stall)" % b); break

    for b in range(1, args.new_batches + 1):
        t = time.time()
        # 新键：以 MAX(time)+b 天为基，按行号跨足够多新交易日，保证 (code,time) 唯一
        base = mx + b * 86400000
        sel = ("code, CAST(%d AS BIGINT) + (row_number() OVER (ORDER BY code)) / %d * 86400000 AS time, "
               % (base, ncodes) + ", ".join(other_cols))
        new_sql = ("CREATE TABLE _tmp_write AS SELECT %s FROM "
                   "(SELECT * FROM stock_daily ORDER BY time, code LIMIT %d)"
                   % (sel, args.batch))
        conn.execute("DROP TABLE IF EXISTS _tmp_write")
        conn.execute(new_sql)
        conn.execute(plain)
        conn.execute("DROP TABLE IF EXISTS _tmp_write")
        dt = time.time() - t
        total += dt
        log("new  batch %03d rows=%d %.2fs" % (b, args.batch, dt))

    n_after = conn.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
    conn.close()
    log("DONE total=%.1fs rows_before=%d rows_after=%d" % (total, n, n_after))
    io.open(args.log, "w", encoding="utf-8").write("\n".join(L) + "\n")

if __name__ == "__main__":
    main()
