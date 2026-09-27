# -*- coding: utf-8 -*-
"""B 件 ④ 实证（设计件 §5.6 / ③实施令口径）：
签发临时 marker → 走新入口试启 → 断言拒启（非零退出 + 留痕 + 零 daemon 进程）→ 撤 marker。

在**真实路径**上执行（data/daemon_hold.marker），以证生产 DATA_ROOT 解析下的门位置正确。
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

QS = Path(r"D:\miniQMT策略实盘\QuantStudio")
DATA = QS / "data"
MARKER = DATA / "daemon_hold.marker"
LOGS = DATA / "logs"
OUT = Path(os.environ.get("TEMP", ".")) / "qfq_forensics_20260927"
OUT.mkdir(parents=True, exist_ok=True)
RESULT = OUT / "b_hold_gate_empirical.json"

evidence = {"started_at": datetime.now().isoformat(timespec="seconds")}


def daemon_procs():
    try:
        import psutil
    except Exception as e:                                    # pragma: no cover
        return [f"psutil unavailable: {e}"]
    hits = []
    for p in psutil.process_iter(["pid", "cmdline", "create_time"]):
        try:
            line = " ".join(p.info.get("cmdline") or [])
        except Exception:
            continue
        if "quantstudio.pipeline.daemon" in line:
            hits.append({"pid": p.info["pid"], "cmdline": line[:300],
                         "create_time": p.info.get("create_time")})
    return hits


evidence["pre"] = {
    "marker_exists": MARKER.exists(),
    "status_file_exists": (DATA / "daemon_status.json").exists(),
    "daemon_processes": daemon_procs(),
    "reject_traces_before": sorted(x.name for x in LOGS.glob("daemon_launch_rejected_*.log")),
    "check_log_lines_before": (
        len((LOGS / "daemon_hold_check.log").read_text(encoding="utf-8").splitlines())
        if (LOGS / "daemon_hold_check.log").exists() else 0),
}

marker_payload = {
    "ruling_id": "B-hold-gate-empirical-20260927",
    "issued_at": datetime.now().isoformat(timespec="seconds"),
    "expires_at": (datetime.now() + timedelta(minutes=3)).isoformat(timespec="seconds"),
    "reason": "B 件 hold 门实证（临时 marker，3 分钟后自动失效）",
    "issued_by": "TraeCode 主仓线执行会话",
}

try:
    MARKER.write_text(json.dumps(marker_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    evidence["marker_written"] = {
        "path": str(MARKER), "bytes": MARKER.stat().st_size,
        "content": marker_payload,
    }
    t0 = time.time()
    proc = subprocess.run(
        [sys.executable, str(QS / "scripts" / "launch_daemon.py"),
         "--config-dir", "config/profiles/mcp_only", "--wait-sec", "0"],
        cwd=str(QS), capture_output=True, text=True, timeout=180,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    evidence["launch_attempt"] = {
        "returncode": proc.returncode,
        "elapsed_sec": round(time.time() - t0, 2),
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
    }
    time.sleep(1)
    after = sorted(x.name for x in LOGS.glob("daemon_launch_rejected_*.log"))
    new_traces = [x for x in after if x not in evidence["pre"]["reject_traces_before"]]
    evidence["post_attempt"] = {
        "new_reject_traces": new_traces,
        "new_trace_content": (
            (LOGS / new_traces[-1]).read_text(encoding="utf-8") if new_traces else None),
        "check_log_last_lines": (
            (LOGS / "daemon_hold_check.log").read_text(encoding="utf-8").splitlines()[-3:]
            if (LOGS / "daemon_hold_check.log").exists() else []),
        "daemon_processes": daemon_procs(),
        "bootstrap_files_new": sorted(
            x.name for x in LOGS.glob("daemon_bootstrap_*.log")
            if x.stat().st_mtime >= t0),
    }
finally:
    removed = False
    if MARKER.exists():
        MARKER.unlink()
        removed = True
    evidence["cleanup"] = {
        "marker_removed": removed,
        "marker_exists_after": MARKER.exists(),
        "daemon_processes_after": daemon_procs(),
    }
    evidence["finished_at"] = datetime.now().isoformat(timespec="seconds")
    RESULT.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2))
    print("RESULT_FILE=" + str(RESULT))
