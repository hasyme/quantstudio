# -*- coding: utf-8 -*-
"""V7 在线实验监视器（只读，不触碰生产库写路径）。

用法：python docs/evidence/hang2_v7_online_monitor.py <pid> [tag] [tail_lines]
  tag 决定日志文件：data/logs/_<tag>.err.txt（默认 v7online）
输出：进程存活 / 日志尾部（自动判编码）/ V7 重建次数 / 各表写批数 / 停摆 jsonl / 库体积
"""
from __future__ import annotations

import io
import os
import sys
import time

STALL = os.path.join("data", "logs", "duckdb_write_stall.jsonl")
DB = os.path.join("data", "quantstudio.db")
PATTERNS = ("V7", "batch=", "timing", "STALL", "INTERRUPT", "CRITICAL",
            "CHECKPOINT", "写批", "停摆", "task=", "ERROR", "Error", "wrote")


def read_text(path, limit=None):
    if not os.path.exists(path):
        return []
    raw = open(path, "rb").read()
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            txt = raw.decode(enc)
            if enc != "latin-1" or "�" not in txt:
                break
        except Exception:
            continue
    lines = txt.splitlines()
    return lines[-limit:] if limit else lines


def alive(pid):
    try:
        import psutil
        p = psutil.Process(pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE, p.status()
    except Exception:
        return False, "gone"


def main():
    pid = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    tag = sys.argv[2] if len(sys.argv) > 2 else "v7online"
    tail = int(sys.argv[3]) if len(sys.argv) > 3 else 6
    err = os.path.join("data", "logs", "_%s.err.txt" % tag)
    w = io.open(os.path.join("data", "logs", "_%s_monitor.txt" % tag), "w",
                encoding="utf-8", errors="replace")

    def p(s=""):
        w.write(s + "\n")
        print(s)

    p("=== monitor [%s] @ %s ===" % (tag, time.strftime("%H:%M:%S")))
    ok, st = alive(pid)
    p("pid=%s alive=%s status=%s" % (pid, ok, st))

    lines = read_text(err)
    p("log_lines=%d" % len(lines))
    p("valuation_writes=%d" % sum(
        1 for l in lines if "stock_daily_valuation batch=" in l and "wrote" in l))
    p("stock_daily_writes=%d" % sum(
        1 for l in lines if "stock_daily batch=" in l and "wrote" in l))
    p("V7_rebuild=%d" % sum(1 for l in lines if "V7" in l))
    p("stall_markers=%d" % sum(
        1 for l in lines if "STALL" in l or "停摆" in l or "CRITICAL" in l))

    hits = [l for l in lines if any(k in l for k in PATTERNS)]
    p("--- last %d matched ---" % min(tail, len(hits)))
    for l in hits[-tail:]:
        p("  " + l.strip()[:180])

    for l in lines:
        if "V7" in l:
            p("V7> " + l.strip()[:170])

    if os.path.exists(STALL):
        sl = read_text(STALL, 2)
        p("--- stall jsonl (%d bytes) ---" % os.path.getsize(STALL))
        for l in sl:
            p("  " + l.strip()[:200])
    else:
        p("stall jsonl: absent")

    if os.path.exists(DB):
        p("db_size=%.2fGB" % (os.path.getsize(DB) / 1e9))
        wal = DB + ".wal"
        if os.path.exists(wal):
            p("wal=%.1fMB" % (os.path.getsize(wal) / 1e6))
    w.close()


if __name__ == "__main__":
    main()
