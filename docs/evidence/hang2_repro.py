# -*- coding: utf-8 -*-
"""事故2（2026-10-02 DuckDB 写入挂起）最小复现 / 变体对照实验台。

用途（不触碰生产库，全部在 data/bench 下新建临时库）：
  1. 复现 `DuckDBWriter._write_locked` 的 ON CONFLICT 写入在 2.75M 行规模 + 5 万行批下
     的"单核满载、永不返回"挂起（事故1=零冲突纯新增语义、事故2=全冲突 upsert 语义，
     两次均卡在累计 2,749,437 行后的第 57 批）。
  2. 对照候选规避变体，判定哪一个真正避开病态路径：
       on_conflict        —— 生产 SQL 原样（update_set 含主键列）
       on_conflict_nopk   —— update_set 排除主键列
       delete_insert      —— 先 DELETE 重叠键再纯 INSERT（避开 ON CONFLICT 算子）
       on_conflict_chunk  —— 批内再切子批（默认 5k）逐条 execute
       on_conflict_noorder—— on_conflict + SET preserve_insertion_order=false
  3. 验证 `conn.interrupt()` 看门狗能否把"挂起"变为"有界失败"（写路径超时防护可行性）。

每批计时 append 到 docs/evidence/hang2_repro_timings.csv。

用法（分阶段，便于复用同一 2.75M 行表做多变体对照）：
  python docs/evidence/hang2_repro.py --phase load  --target-rows 2749437
  python docs/evidence/hang2_repro.py --phase replay --variant on_conflict --batches 60
  python docs/evidence/hang2_repro.py --phase replay --variant delete_insert --batches 60
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import threading
import time
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath("."))
from quantstudio.pipeline.writers import DDL_DUCKDB  # noqa: E402

CSV_PATH = os.path.join("docs", "evidence", "hang2_repro_timings.csv")
N_CODES = 4906            # 真实形态：单 shard 50000 行 / 4906 码 ≈ 10 个交易日
DAYS_PER_BATCH = 10       # 165 shard × 10 交易日 = 1650 交易日 ≈ 2020-01→2026-10
BASE_DATE = datetime(2020, 1, 2)
CSV_HEADER = [
    "ts", "phase", "variant", "batch_idx", "rows", "seconds",
    "outcome", "table_rows", "db_bytes", "note",
]


# ── 交易日 / 数据构造（贴近真实：code × day 一行）────────────────────────────
def business_days(n: int):
    """返回前 n 个交易日的 epoch 毫秒（DuckDB time BIGINT 口径）。"""
    out, d = [], BASE_DATE
    while len(out) < n:
        if d.weekday() < 5:
            out.append(int(d.timestamp() * 1000))
        d += timedelta(days=1)
    return out


def make_df(day_lo: int, day_hi: int) -> pd.DataFrame:
    """构造 [day_lo, day_hi) 交易日的全 code 面板，形状对齐真实 shard。"""
    days = business_days(day_hi)[day_lo:day_hi]
    codes = [f"{600000 + i:06d}" for i in range(N_CODES)]
    n = len(days) * len(codes)
    code_col = np.repeat(np.array(codes, dtype=object), len(days))
    time_col = np.tile(np.array(days, dtype=np.int64), len(codes))
    rng = np.random.default_rng(20261002 + day_lo)
    base = rng.uniform(8.0, 40.0, n)
    df = pd.DataFrame({
        "code": code_col,
        "time": time_col,
        "open": base,
        "high": base * 1.02,
        "low": base * 0.98,
        "close": base * 1.01,
        "volume": rng.uniform(1e5, 1e7, n),
        "amount": rng.uniform(1e6, 1e9, n),
        "preClose": base * 0.99,
        "suspendFlag": np.zeros(n, dtype=np.int32),
        "settelementPrice": base * 1.01,
        "openInterest": np.zeros(n, dtype=np.float64),
    })
    for c in ("open_front", "high_front", "low_front", "close_front",
              "open_back", "high_back", "low_back", "close_back"):
        df[c] = base * 1.01
    for c in ("open_front_ratio", "high_front_ratio", "low_front_ratio", "close_front_ratio",
              "open_back_ratio", "high_back_ratio", "low_back_ratio", "close_back_ratio",
              "turn", "pctChg", "peTTM", "psTTM", "pcfNcfTTM", "pbMRQ"):
        df[c] = rng.uniform(0.0, 30.0, n)
    df["isST"] = np.zeros(n, dtype=np.int32)
    df["is_st_reliable"] = np.ones(n, dtype=bool)
    df["is_st_reliable_source"] = np.array(["mcp"] * n, dtype=object)
    df["is_delisting_risk"] = np.zeros(n, dtype=bool)
    df["is_delisting_risk_source"] = np.array([""] * n, dtype=object)
    df["dividend_type"] = np.array([""] * n, dtype=object)
    df["update_time"] = np.array(["2026-10-02 00:00:00"] * n, dtype=object)
    return df


# ── 生产 SQL 形态（与 writers.py:890-919 逐句对齐）─────────────────────────
def sql_count(conn, table, pk_cols):
    return conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {pk_cols} IN "
        f"(SELECT {pk_cols} FROM _tmp_write)").fetchone()[0]


def sql_upsert(conn, table, cols, pk_cols, include_pk_in_set=True):
    set_cols = [c for c in cols if include_pk_in_set or c not in ("code", "time")]
    update_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in set_cols)
    return conn.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) "
        f"SELECT * FROM _tmp_write "
        f"ON CONFLICT {pk_cols} DO UPDATE SET {update_set}")


def sql_plain_insert(conn, table, cols):
    return conn.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) SELECT * FROM _tmp_write")


class _Watchdog:
    """到点调用 conn.interrupt()：验证挂起能否被有界打断。"""

    def __init__(self, conn, seconds: float):
        self.conn = conn
        self.seconds = seconds
        self.fired = threading.Event()
        self._t = threading.Timer(seconds, self._fire)
        self._t.daemon = True

    def _fire(self):
        self.fired.set()
        try:
            self.conn.interrupt()
        except Exception:
            pass

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._t.cancel()
        return False


def _record(row: dict) -> None:
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_HEADER)
        if not exists:
            w.writeheader()
        w.writerow(row)


def _db_bytes(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def _table_rows(conn, table) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ── phases ─────────────────────────────────────────────────────────────────
def phase_load(args) -> None:
    conn = duckdb.connect(args.db)
    conn.execute(DDL_DUCKDB["stock_daily"])
    table = "stock_daily"
    days_total = int(np.ceil(args.target_rows / N_CODES))
    chunk_days = 200
    print(f"[load] target_rows={args.target_rows} days={days_total} chunk_days={chunk_days}")
    t_all = time.time()
    lo, idx = 0, 0
    while lo < days_total:
        hi = min(lo + chunk_days, days_total)
        df = make_df(lo, hi)
        conn.register("_tmp_load", df)
        t0 = time.time()
        conn.execute(f"INSERT INTO {table} ({', '.join(df.columns)}) SELECT * FROM _tmp_load")
        conn.unregister("_tmp_load")
        dt = time.time() - t0
        idx += 1
        rows_now = _table_rows(conn, table)
        print(f"[load] chunk{idx} days[{lo},{hi}) rows={len(df)} {dt:.1f}s table_rows={rows_now}")
        _record({"ts": datetime.now().isoformat(timespec="seconds"), "phase": "load",
                 "variant": "plain_insert", "batch_idx": idx, "rows": len(df),
                 "seconds": round(dt, 3), "outcome": "ok", "table_rows": rows_now,
                 "db_bytes": _db_bytes(args.db), "note": f"days[{lo},{hi})"})
        lo = hi
    conn.close()
    print(f"[load] done in {time.time() - t_all:.1f}s -> {_db_bytes(args.db)} bytes")


def phase_replay(args) -> None:
    table = "stock_daily"
    pk_cols = "(code, time)"
    conn = duckdb.connect(args.db)
    if args.no_preserve_order:
        conn.execute("SET preserve_insertion_order=false")
    if args.threads:
        conn.execute("SET threads=%d" % args.threads)
    base_rows = _table_rows(conn, table)
    print(f"[replay] variant={args.variant} table_rows={base_rows} batches={args.batches} "
          f"watchdog={args.watchdog}s start_day={args.start_day} threads={args.threads} "
          f"no_preserve_order={args.no_preserve_order}", flush=True)
    for i in range(args.batches):
        lo = args.start_day + i * DAYS_PER_BATCH
        df = make_df(lo, lo + DAYS_PER_BATCH)
        cols = list(df.columns)
        conn.register("_tmp_write", df)
        note, outcome, upd = "", "ok", -1
        t0 = time.time()
        try:
            with _Watchdog(conn, args.watchdog) as wd:
                upd = sql_count(conn, table, pk_cols)
                if args.variant == "on_conflict":
                    sql_upsert(conn, table, cols, pk_cols, include_pk_in_set=True)
                elif args.variant == "on_conflict_nopk":
                    sql_upsert(conn, table, cols, pk_cols, include_pk_in_set=False)
                elif args.variant == "delete_insert":
                    conn.execute(
                        f"DELETE FROM {table} WHERE {pk_cols} IN "
                        f"(SELECT {pk_cols} FROM _tmp_write)")
                    sql_plain_insert(conn, table, cols)
                elif args.variant == "on_conflict_chunk":
                    for s in range(0, len(df), args.chunk):
                        sub = df.iloc[s:s + args.chunk]
                        conn.register("_tmp_chunk", sub)
                        sql_upsert(conn, table, list(sub.columns), pk_cols, True)
                        conn.unregister("_tmp_chunk")
                elif args.variant == "on_conflict_noorder":
                    conn.execute("SET preserve_insertion_order=false")
                    sql_upsert(conn, table, cols, pk_cols, include_pk_in_set=True)
                elif args.variant == "plain_insert":
                    # P1 路径（writers.py:915）：本批主键全为新键时的纯 INSERT
                    sql_plain_insert(conn, table, cols)
                elif args.variant == "plain_insert_noorder":
                    conn.execute("SET preserve_insertion_order=false")
                    sql_plain_insert(conn, table, cols)
                else:
                    raise SystemExit(f"unknown variant {args.variant}")
            if wd.fired.is_set():
                outcome = "watchdog_fired"
                note = "execute returned after interrupt"
        except Exception as exc:  # noqa: BLE001 —— 实验台需记录任意异常类型
            outcome = f"exc:{type(exc).__name__}"
            note = str(exc)[:160].replace("\n", " ")
            try:
                conn.close()
            except Exception:
                pass
            conn = duckdb.connect(args.db)
            if args.no_preserve_order:
                conn.execute("SET preserve_insertion_order=false")
        finally:
            try:
                conn.unregister("_tmp_write")
            except Exception:
                pass
        dt = time.time() - t0
        rows_now = _table_rows(conn, table)
        print(f"[replay] batch{i + 1:03d} rows={len(df)} {dt:8.1f}s {outcome:24s} "
              f"upd={upd} table_rows={rows_now} db={_db_bytes(args.db)}", flush=True)
        _record({"ts": datetime.now().isoformat(timespec="seconds"), "phase": "replay",
                 "variant": args.variant, "batch_idx": i + 1, "rows": len(df),
                 "seconds": round(dt, 3), "outcome": outcome, "table_rows": rows_now,
                 "db_bytes": _db_bytes(args.db), "note": note or f"upd={upd}"})
        if outcome != "ok":
            print("[replay] 非 ok 结局，停止本变体", flush=True)
            break
    conn.close()


def phase_schema(args) -> None:
    """只建表（复现事故1「从空表顺序追加」形态的前置步骤）。"""
    conn = duckdb.connect(args.db)
    conn.execute(DDL_DUCKDB["stock_daily"])
    conn.close()
    print(f"[schema] stock_daily created at {args.db}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join("data", "bench", "hang2_repro.db"))
    ap.add_argument("--phase", choices=["schema", "load", "replay"], required=True)
    ap.add_argument("--target-rows", type=int, default=2_749_437)
    ap.add_argument("--batches", type=int, default=60)
    ap.add_argument("--start-day", type=int, default=0)
    ap.add_argument("--variant", default="on_conflict")
    ap.add_argument("--chunk", type=int, default=5000)
    ap.add_argument("--watchdog", type=float, default=120.0)
    ap.add_argument("--no-preserve-order", action="store_true")
    ap.add_argument("--threads", type=int, default=0,
                    help="SET threads=N（>0 时）；用于模拟生产现场约 40 核/80 线程的并行度")
    args = ap.parse_args()
    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    if args.phase == "schema":
        phase_schema(args)
    elif args.phase == "load":
        phase_load(args)
    else:
        phase_replay(args)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
