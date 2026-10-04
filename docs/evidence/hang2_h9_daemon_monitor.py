# -*- coding: utf-8 -*-
"""H9 bench-daemon 监视器：读取指定 err 日志，统计进度与停摆标记。

用法：python docs/evidence/hang2_h9_daemon_monitor.py <pid> <err_log> [verbosity]
verbosity: 数字越大输出越多行（默认 4）
"""
import sys, os, time, io
import psutil

PID = int(sys.argv[1])
ERR = sys.argv[2]
VERB = int(sys.argv[3]) if len(sys.argv) > 3 else 4

def read_lines(p):
    try:
        raw = open(p, "rb").read()
    except Exception:
        return []
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            txt = raw.decode(enc)
            if enc != "latin-1" or "\ufffd" not in txt:
                break
        except Exception:
            continue
    return txt.splitlines()

def main():
    now = time.strftime("%H:%M:%S")
    alive = psutil.pid_exists(PID)
    L = read_lines(ERR)
    val = sum(1 for l in L if "stock_daily_valuation batch=" in l and "wrote" in l)
    sd = sum(1 for l in L if "stock_daily batch=" in l and "wrote" in l)
    v7 = sum(1 for l in L if "V7" in l)
    val_valid = sum(1 for l in L if "stock_daily_valuation batch=" in l and "passed=" in l)
    sd_valid = sum(1 for l in L if "stock_daily batch=" in l and "passed=" in l)
    adj = sum(1 for l in L if "adj_factor" in l and ("注入" in l or "inject" in l.lower()))
    stall = sum(1 for l in L if "STALL" in l or "停摆" in l or "CRITICAL" in l or "DuckDBWriteStalled" in l)
    interrupt = sum(1 for l in L if "interrupt" in l.lower())
    db = "data/bench/hang2_h9_daemon.db"
    dbsize = "%.2fGB" % (os.path.getsize(db) / 1e9) if os.path.exists(db) else "?"
    print("[%s] alive=%s | val_writes=%d sd_writes=%d | val_valid=%d sd_valid=%d | V7=%d adj=%d | STALL=%d intr=%d | db=%s"
          % (now, alive, val, sd, val_valid, sd_valid, v7, adj, stall, interrupt, dbsize))
    if VERB and VERB > 0:
        print("--- last %d ---" % min(VERB, len(L)))
        for l in L[-VERB:]:
            print("  ", l.strip()[:170])

if __name__ == "__main__":
    main()
