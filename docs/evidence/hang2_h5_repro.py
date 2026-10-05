# -*- coding: utf-8 -*-
"""H5' 取证：单进程连续写批能否复现 stock_daily 停摆，并筛选根治写入策略。

背景（门6 实证 2026-10-03）：生产 PID 26480 写批 1-56 成功、第 57 批永不返回；
生产库副本单批写入 1.6s 正常 => 触发依赖进程内累积状态；合成数据三组复现均失败。
本脚本在生产库副本上单进程连续写批，镜像 writers.py 真实语句形态
（短连接 + register + 计数 SELECT + ON CONFLICT/纯 INSERT + unregister + close）。

变体：
  production : 计数 SELECT + ON CONFLICT（生产基线）
  p1         : 现行分档（计数==0 -> 纯 INSERT，否则 ON CONFLICT）
  delins     : DELETE 重叠键 + 纯 INSERT（避开 ON CONFLICT 算子）
  chunk      : ON CONFLICT，批内切 5k 子批（缩小单事务）

用法：python docs/evidence/hang2_h5_repro.py --variants production,delins --batches 70
输出：docs/evidence/hang2_h5_repro_<variant>.txt + hang2_h5_timings.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
import threading
import time
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd

SRC = os.path.join("data", "quantstudio.db")
BENCH = os.path.join("data", "bench")
TABLE = "stock_daily"
BATCH = 50_000
UPDATE_BATCHES = 56
CSV_PATH = os.path.join("docs", "evidence", "hang2_h5_timings.csv")
CSV_HEADER = ["ts", "variant", "batch", "phase", "rows", "updated", "seconds", "outcome"]


def log(msg):
    sys.stdout.reconfigure(encoding="utf-8")
    print(msg, flush=True)


def fresh_copy(tag):
    os.makedirs(BENCH, exist_ok=True)
    dst = os.path.join(BENCH, ("hang2_h5_%s.db" % tag))
    if os.path.exists(dst):
        os.remove(dst)
    t0 = time.time()
    shutil.copyfile(SRC, dst)
    log("[setup] copied %.2fGB in %.1fs" % (os.path.getsize(dst) / 1e9, time.time() - t0))
    return dst


def prep_src(dst):
    c = duckdb.connect(dst)
    c.execute("DROP TABLE IF EXISTS repro_src")
    c.execute("CREATE TABLE repro_src AS SELECT * FROM %s ORDER BY time, code" % TABLE)
    n = c.execute("SELECT COUNT(*) FROM repro_src").fetchone()[0]
    c.close()
    log("[setup] repro_src rows=%d" % n)


def fetch_existing(dst, offset, limit=BATCH):
    c = duckdb.connect(dst, read_only=True)
    try:
        return c.execute("SELECT * FROM repro_src LIMIT ? OFFSET ?", [limit, offset]).fetchdf()
    finally:
        c.close()


def make_new_keys(dst, seq, limit=BATCH):
    c = duckdb.connect(dst, read_only=True)
    try:
        cols = [r[0] for r in c.execute("DESCRIBE %s" % TABLE).fetchall()]
        codes = [r[0] for r in c.execute(
            "SELECT DISTINCT code FROM %s LIMIT 5000" % TABLE).fetchall()]
        mx = c.execute("SELECT MAX(time) FROM %s" % TABLE).fetchone()[0]
    finally:
        c.close()
    day = 86400000
    base = mx + (seq + 1) * day
    codes = np.array(codes, dtype=object)
    reps = int(np.ceil(limit / len(codes)))
    code_col = np.resize(codes, reps * len(codes))[:limit]
    days = np.repeat(np.arange(base, base + (reps + 1) * day, day), len(codes))[:limit]
    rng = np.random.default_rng(1000 + seq)
    vals = rng.uniform(8, 40, limit)
    df = pd.DataFrame({"code": code_col, "time": days.astype("int64")})
    for col in cols:
        if col not in ("code", "time"):
            df[col] = vals
    return df[cols]

CUR = {"table": TABLE}


def _sql(df, pk=True):
    cols = list(df.columns)
    col_list = ", ".join(cols)
    if not pk:
        return "INSERT INTO %s (%s) SELECT * FROM _tmp_write" % (CUR["table"], col_list)
    update_set = ", ".join("%s=EXCLUDED.%s" % (c, c) for c in cols)
    return ("INSERT INTO %s (%s) SELECT * FROM _tmp_write ON CONFLICT (code, time) "
            "DO UPDATE SET %s" % (CUR["table"], col_list, update_set))


def run_batch(dst, df, variant, budget, chunk=5000, table=None):
    """在守护线程内跑一批（镜像生产：短连接 + 计数 SELECT + DML + close）。

    返回 (outcome, seconds, updated)；超时判 STALL（停摆语句不可 interrupt，只能进程退出）。
    """
    box = {}

    def _work():
        try:
            CUR["table"] = table or TABLE
            t0 = time.time()
            conn = duckdb.connect(dst)
            try:
                conn.register("_tmp_write", df)
                cols = ", ".join(df.columns)
                upd = conn.execute(
                    "SELECT COUNT(*) FROM %s WHERE (code, time) IN "
                    "(SELECT code, time FROM _tmp_write)" % CUR["table"]).fetchone()[0]
                t1 = time.time()
                if variant == "production":
                    conn.execute(_sql(df))
                elif variant == "p1":
                    conn.execute(_sql(df, pk=(upd > 0)))
                elif variant == "delins":
                    conn.execute("DELETE FROM %s WHERE (code, time) IN "
                                 "(SELECT code, time FROM _tmp_write)" % CUR["table"])
                    conn.execute(_sql(df, pk=False))
                elif variant == "chunk":
                    for s in range(0, len(df), chunk):
                        sub = df.iloc[s:s + chunk]
                        conn.register("_c", sub)
                        conn.execute("INSERT INTO %s (%s) SELECT * FROM _c ON CONFLICT "
                                     "(code, time) DO UPDATE SET %s"
                                     % (CUR["table"], ", ".join(sub.columns),
                                        ", ".join("%s=EXCLUDED.%s" % (c, c)
                                                  for c in sub.columns)))
                        conn.unregister("_c")
                else:
                    raise SystemExit("unknown variant %s" % variant)
                box["dml_s"] = time.time() - t1
                conn.unregister("_tmp_write")
            finally:
                conn.close()
            box["updated"] = upd
            box["ok"] = True
        except BaseException as e:  # noqa: BLE001
            box["err"] = "%s: %s" % (type(e).__name__, str(e)[:100])

    th = threading.Thread(target=_work, daemon=True)
    t0 = time.time()
    th.start()
    th.join(budget)
    dt = time.time() - t0
    if th.is_alive():
        return "STALL", dt, -1
    if not box.get("ok"):
        return "EXC " + str(box.get("err"))[:50], dt, -1
    return "ok", dt, box.get("updated", -1)

def run_variant(variant, batches, budget, keep):
    dst = fresh_copy(variant)
    prep_src(dst)
    out = os.path.join("docs", "evidence", "hang2_h5_repro_%s.txt" % variant)
    lines = ["variant=%s batches=%d budget=%ss" % (variant, batches, budget),
             "db=%s" % dst]
    stalled_at = None
    try:
        for b in range(1, batches + 1):
            if b <= UPDATE_BATCHES:
                df = fetch_existing(dst, (b - 1) * BATCH)
                phase = "update"
            else:
                df = make_new_keys(dst, b - UPDATE_BATCHES)
                phase = "newkey"
            outcome, secs, upd = run_batch(dst, df, variant, budget)
            log("  batch%03d %-7s rows=%d upd=%s %7.1fs %s"
                % (b, phase, len(df), upd, secs, outcome))
            lines.append("batch%03d %-7s rows=%d updated=%s %.1fs %s"
                         % (b, phase, len(df), upd, secs, outcome))
            with open(CSV_PATH, "a", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
                    w.writerow(CSV_HEADER)
                w.writerow([datetime.now().isoformat(timespec="seconds"), variant, b,
                            phase, len(df), upd, round(secs, 2), outcome])
            if outcome == "STALL":
                stalled_at = b
                break
    finally:
        with open(out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        log("[%s] evidence -> %s ; stall_at=%s" % (variant, out, stalled_at))
        if not keep:
            try:
                os.remove(dst)
            except OSError:
                pass
    return stalled_at


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variants", default="production")
    ap.add_argument("--batches", type=int, default=70)
    ap.add_argument("--budget", type=float, default=90.0)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--plan", default="daily", choices=["daily", "prodseq"])
    ap.add_argument("--val-batches", type=int, default=166)
    args = ap.parse_args()
    log("[h5] variants=%s batches=%d budget=%ss" % (args.variants, args.batches, args.budget))
    summary = []
    if args.plan == "prodseq":
        for v in args.variants.split(","):
            v = v.strip()
            if not v:
                continue
            st = run_prodseq(v, args.budget, args.keep, val_batches=args.val_batches)
            summary.append((v, st))
            if st:
                break
        log("[h5] prodseq summary: %s" % summary)
        return
    for v in args.variants.split(","):
        v = v.strip()
        if not v:
            continue
        st = run_variant(v, args.batches, args.budget, args.keep)
        summary.append((v, st))
        if st:
            log("[h5] %s 在第 %d 批停摆 -> 后续变体需换新库重跑" % (v, st))
            break
    log("[h5] summary: %s" % summary)


VAL_TABLE = "stock_daily_valuation"
VAL_BATCHES = 166


def prep_src2(dst, table):
    c = duckdb.connect(dst)
    name = "repro_src_" + table
    c.execute("DROP TABLE IF EXISTS " + name)
    c.execute("CREATE TABLE %s AS SELECT * FROM %s ORDER BY 1, 2" % (name, table))
    n = c.execute("SELECT COUNT(*) FROM %s" % name).fetchone()[0]
    c.close()
    log("[setup] %s rows=%d" % (name, n))
    return name


def fetch_existing2(dst, src, offset, limit=BATCH):
    c = duckdb.connect(dst, read_only=True)
    try:
        return c.execute("SELECT * FROM %s LIMIT ? OFFSET ?" % src,
                         [limit, offset]).fetchdf()
    finally:
        c.close()

def run_prodseq(variant, budget, keep, val_batches=VAL_BATCHES, daily_update=56, daily_new=14):
    """生产同序回放：valuation 166 批（更新）→ stock_daily 56 批（更新）→ 14 批（新键）。

    这是与门6 现场同序的负载（累计约 1100 万行写入），用于判定停摆是否需要
    "完整生产同进程上下文"才能复现。
    """
    tag = "prodseq_" + variant
    dst = fresh_copy(tag)
    src_val = prep_src2(dst, VAL_TABLE)
    src_daily = prep_src2(dst, TABLE)
    out = os.path.join("docs", "evidence", "hang2_h5_repro_%s.txt" % tag)
    lines = ["variant=%s plan=prodseq budget=%ss" % (variant, budget)]
    steps = ([(VAL_TABLE, src_val, "upd", i) for i in range(val_batches)]
             + [(TABLE, src_daily, "upd", i) for i in range(daily_update)]
             + [(TABLE, None, "new", i) for i in range(daily_new)])
    stalled_at = None
    try:
        for idx, (table, src, phase, i) in enumerate(steps, start=1):
            if phase == "upd":
                df = fetch_existing2(dst, src, i * BATCH)
            else:
                df = make_new_keys(dst, i)
            outcome, secs, upd = run_batch(dst, df, variant, budget, table=table)
            log("  step%03d %-21s %-4s rows=%d upd=%s %7.1fs %s"
                % (idx, table, phase, len(df), upd, secs, outcome))
            lines.append("step%03d %-21s %-4s rows=%d updated=%s %.1fs %s"
                         % (idx, table, phase, len(df), upd, secs, outcome))
            with open(CSV_PATH, "a", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                if os.path.getsize(CSV_PATH) == 0:
                    w.writerow(CSV_HEADER)
                w.writerow([datetime.now().isoformat(timespec="seconds"), tag, idx,
                            table + "/" + phase, len(df), upd, round(secs, 2), outcome])
            if outcome == "STALL":
                stalled_at = (idx, table, phase)
                break
    finally:
        with open(out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        log("[%s] evidence -> %s ; stall=%s" % (tag, out, stalled_at))
        if not keep:
            try:
                os.remove(dst)
            except OSError:
                pass
    return stalled_at


if __name__ == "__main__":
    main()
