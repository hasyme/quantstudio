# -*- coding: utf-8 -*-
"""B 件 §7-2 全 profile `skip_weekdays` 清单核对（取证 harness，随证据件归档）。

用途：重现 docs/evidence/daemon-skip-weekdays-hold-gate-20260927-b-acceptance.md
      §4 表格（路径 / daily_time / skip_weekdays / tasks 数）。

性质：**纯只读** —— 仅读取各 profile 的 collector_tasks.json；
      不写任何文件、不连数据库、不联网、不启动任何进程。

用法：
    py -3.11 docs/evidence/_b_scan_profiles.py
"""
from __future__ import annotations

import json
from pathlib import Path

# docs/evidence/_b_scan_profiles.py -> 项目根
QS_ROOT = Path(__file__).resolve().parent.parent.parent

TARGETS = [
    "config/profiles/mcp_only/collector_tasks.json",
    "config/profiles/prehandover_staging/collector_tasks.json",
    "config_legacy_deprecated/collector_tasks.json",
    "agent_workspace/shadow_lockprobe/config/collector_tasks.json",
    "config/collector_tasks.json",  # 默认 --config-dir config（预期不存在）
]


def main() -> int:
    print(f"{'path':<62} {'daily_time':<11} {'skip_weekdays':<15} tasks")
    print("-" * 100)
    for rel in TARGETS:
        p = QS_ROOT / rel
        if not p.exists():
            print(f"{rel:<62} {'-':<11} {'(不存在)':<15} -")
            continue
        cfg = json.loads(p.read_text(encoding="utf-8"))
        sched = cfg.get("daemon_schedule") or {}
        dt = sched.get("daily_time", "-")
        sw = sched.get("skip_weekdays", "-")
        n = len(cfg.get("tasks", []) or [])
        print(f"{rel:<62} {str(dt):<11} {json.dumps(sw, ensure_ascii=False):<15} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
