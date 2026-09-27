# -*- coding: utf-8 -*-
"""B 件验收单测（设计件 §5）：skip_weekdays 真值表 + hold 门 m1~m5 + 入口收敛绕过用例。

隔离：临时 DATA_ROOT（monkeypatch `quantstudio._paths.DATA_ROOT` 及各模块 `_data_root`），
不触碰正式库；子进程用例用 `QUANTSTUDIO_DATA_ROOT` 环境变量重定向。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

SUNDAY = datetime(2026, 9, 27, 6, 30)      # 周日（skip_weekdays=[6] 命中）
SATURDAY = datetime(2026, 9, 26, 6, 30)    # 周六（非跳过日）


@pytest.fixture
def tmp_root(monkeypatch, tmp_path):
    """隔离的临时 DATA_ROOT（含 logs/）。"""
    import quantstudio._paths as qp
    import quantstudio.gui.daemon_process as dp
    import quantstudio.pipeline.daemon_hold_gate as hg
    import quantstudio.pipeline.daemon_lifecycle as dl
    monkeypatch.setattr(qp, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(dl, "_data_root", lambda: tmp_path)
    monkeypatch.setattr(dp, "_data_root", lambda: tmp_path)
    monkeypatch.setattr(hg, "_data_root", lambda: tmp_path)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    return tmp_path


# ===========================================================================
# 一、hold 门单测 m1~m5（§5.3）
# ===========================================================================

def _write_marker(root: Path, expires_at, **extra):
    marker = {"ruling_id": "t-1", "issued_at": "2026-09-27T00:00:00",
              "expires_at": expires_at, "reason": "pytest", "issued_by": "pytest"}
    marker.update(extra)
    (root / "daemon_hold.marker").write_text(
        json.dumps(marker, ensure_ascii=False), encoding="utf-8")


def _reject_traces(root: Path):
    return sorted((root / "logs").glob("daemon_launch_rejected_*.log"))


def _check_log(root: Path) -> str:
    path = root / "logs" / "daemon_hold_check.log"
    return path.read_text(encoding="utf-8") if path.exists() else ""


def test_m1_hold_active_rejects_with_nonzero_exit_and_trace(tmp_root):
    """m1：marker 存在且未过期 → 拒启 + 非零退出 + 留痕文件生成。"""
    from quantstudio.pipeline.daemon_hold_gate import (
        HOLD_REJECT_EXIT_CODE, enforce_hold_or_exit)
    _write_marker(tmp_root, "2999-01-01T00:00:00")
    with pytest.raises(SystemExit) as ei:
        enforce_hold_or_exit("pytest::m1")
    assert ei.value.code == HOLD_REJECT_EXIT_CODE == 3
    traces = _reject_traces(tmp_root)
    assert len(traces) == 1
    body = traces[0].read_text(encoding="utf-8")
    assert "pytest::m1" in body and "hold_active" in body and "t-1" in body
    log = _check_log(tmp_root)
    assert "REJECT" in log and "entry=pytest::m1" in log


def test_m2_no_marker_passes_and_behavior_unchanged(tmp_root):
    """m2：marker 缺失 → 放行 + 留痕 PASS + 不产生拒启件、不创建 marker。"""
    from quantstudio.pipeline.daemon_hold_gate import check_hold
    decision = check_hold("pytest::m2")
    assert decision.hold is False and decision.reason == "no_marker"
    assert _reject_traces(tmp_root) == []
    assert not (tmp_root / "daemon_hold.marker").exists()
    log = _check_log(tmp_root)
    assert "PASS" in log and "reason=no_marker" in log


def test_m3_expired_marker_released_with_warning_and_pure_read(tmp_root):
    """m3：marker 存在但 expires_at 已过 → 放行 + 留痕告警；且纯只读（不删、不改）。"""
    from quantstudio.pipeline.daemon_hold_gate import check_hold
    _write_marker(tmp_root, "2020-01-01T00:00:00")
    before = (tmp_root / "daemon_hold.marker").read_text(encoding="utf-8")
    decision = check_hold("pytest::m3")
    assert decision.hold is False and decision.reason == "expired"
    assert (tmp_root / "daemon_hold.marker").read_text(encoding="utf-8") == before
    log = _check_log(tmp_root)
    assert "PASS" in log and "reason=expired" in log
    assert _reject_traces(tmp_root) == []


@pytest.mark.parametrize("payload,expected", [
    ("{not json", "malformed"),
    ("", "malformed"),
    ("[]", "malformed"),
    ('{"ruling_id": "x"}', "missing_expires_at"),
    ('{"expires_at": "not-a-date"}', "unparsable_expires_at"),
])
def test_m4_malformed_or_missing_expires_released_without_exception(tmp_root, payload, expected):
    """m4：JSON 损坏 / 缺 expires_at / 不可解析 → 按失效处理 + 留痕，不得抛未捕获异常。"""
    from quantstudio.pipeline.daemon_hold_gate import check_hold
    (tmp_root / "daemon_hold.marker").write_text(payload, encoding="utf-8")
    decision = check_hold("pytest::m4")          # 不抛异常
    assert decision.hold is False and decision.reason == expected
    log = _check_log(tmp_root)
    assert "PASS" in log and f"reason={expected}" in log


def test_m5_check_log_one_line_in_both_states(tmp_root):
    """m5：daemon_hold_check.log 在拒绝 / 通过两态各留一行（可审计）。"""
    from quantstudio.pipeline.daemon_hold_gate import check_hold, enforce_hold_or_exit
    _write_marker(tmp_root, "2999-01-01T00:00:00")
    with pytest.raises(SystemExit):
        enforce_hold_or_exit("pytest::m5-reject")
    (tmp_root / "daemon_hold.marker").unlink()
    check_hold("pytest::m5-pass")
    lines = [ln for ln in _check_log(tmp_root).splitlines() if ln.strip()]
    assert len(lines) == 2
    assert "\tREJECT\t" in lines[0] and "entry=pytest::m5-reject" in lines[0]
    assert "\tPASS\t" in lines[1] and "entry=pytest::m5-pass" in lines[1]


def test_gate_module_is_stdlib_only(tmp_root):
    """硬不变量 4：hold 判定不得依赖网络 / 数据库。"""
    src = (_ROOT / "quantstudio" / "pipeline" / "daemon_hold_gate.py").read_text(encoding="utf-8")
    for forbidden in ("import duckdb", "import requests", "import socket", "import psutil"):
        assert forbidden not in src


# ===========================================================================
# 二、星期口径（§5.2）
# ===========================================================================

def test_weekday_semantics_matches_legacy():
    """Python 语义：0=周一 … 6=周日（与 daemon.py:2136 `now.weekday()` 逐位一致）。"""
    assert date(2026, 9, 27).weekday() == 6    # 周日 = skip 项 [6]
    assert date(2026, 9, 26).weekday() == 5    # 周六
    assert date(2026, 9, 25).weekday() == 4    # 周五
    assert date(2026, 9, 23).weekday() == 2    # 周三
    assert date(2026, 9, 21).weekday() == 0    # 周一
    # 旧实现同口径（源码断言，防后续被改成 cron 的 0=周日 语义）
    legacy = (_ROOT / "quantstudio" / "pipeline" / "daemon.py").read_text(encoding="utf-8")
    assert "now.weekday() in skip_weekdays" in legacy


# ===========================================================================
# 三、§7-1 真值表（run_forever 实驱动）
# ===========================================================================

def _write_cfg(root: Path, daily_time="06:00", skip_weekdays=(6,), omit_skip=False):
    sched = {"daily_time": daily_time, "check_interval_sec": 300}
    if not omit_skip:
        sched["skip_weekdays"] = list(skip_weekdays)
    (root / "collector_tasks.json").write_text(
        json.dumps({"daemon_schedule": sched, "tasks": []}, ensure_ascii=False),
        encoding="utf-8")


def _set_state(root: Path, status: str, scheduled_date: str):
    (root / "daemon_run_state.json").write_text(
        json.dumps({"scheduled_date": scheduled_date, "status": status}), encoding="utf-8")


def _make_lc(root: Path, monkeypatch, now: datetime):
    """构造测试用 DaemonLifecycle：注入时钟 + 记录是否发起轮次。"""
    import quantstudio.pipeline.daemon_lifecycle as dl
    from quantstudio.pipeline.daemon_lifecycle import DaemonLifecycle
    lifecycle = DaemonLifecycle.__new__(DaemonLifecycle)
    lifecycle.config_dir = root
    lifecycle.instance_token = "t_" + uuid.uuid4().hex[:8]
    lifecycle.max_iterations = 1               # 单轮即退，避免 sleep
    lifecycle._running = True
    lifecycle._status = None
    lifecycle._instance_lock = None
    ran = []
    lifecycle.run_one_cycle = lambda cfg: ran.append(1)
    lifecycle.run_health_check = lambda cfg: None
    monkeypatch.setattr(dl, "datetime",
                        type("_DT", (), {"now": staticmethod(lambda: now)}))
    return lifecycle, ran


def _state(root: Path) -> dict:
    path = root / "daemon_run_state.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _legacy_should_run(now_hm: str, daily_time: str, completed: bool, pending_rerun: bool) -> bool:
    """改动前的 should_run 语义（对照基线）。"""
    should_run = (now_hm >= daily_time and not completed)
    if pending_rerun and not completed:
        should_run = True
    return should_run


@pytest.mark.parametrize("now,skip,daily_time,status,expect_run,expect_skipped", [
    # 跳过日（周日）：全部抑制（含 pending_rerun 补跑分支）
    (SUNDAY,   (6,), "06:00", None,          False, True),
    (SUNDAY,   (6,), "06:00", "running",     False, True),
    (SUNDAY,   (6,), "06:00", "interrupted", False, True),
    (SUNDAY,   (6,), "06:00", "completed",   False, False),
    # 非跳过日（周六）：与旧逻辑逐位一致
    (SATURDAY, (6,), "06:00", None,          True,  False),
    (SATURDAY, (6,), "06:00", "running",     True,  False),
    (SATURDAY, (6,), "06:00", "interrupted", True,  False),
    (SATURDAY, (6,), "06:00", "completed",   False, False),
    # 未到点
    (SATURDAY, (6,), "08:00", None,          False, False),
    (SUNDAY,   (6,), "08:00", "running",     False, True),
    # 空 skip 集合 == 旧行为（向后兼容 / 紧急回退）
    (SUNDAY,   (),  "06:00", None,           True,  False),
    (SUNDAY,   (),  "06:00", "running",      True,  False),
])
def test_skip_weekdays_truth_table(tmp_root, monkeypatch, now, skip, daily_time,
                                   status, expect_run, expect_skipped):
    _write_cfg(tmp_root, daily_time=daily_time, skip_weekdays=skip)
    if status is not None:
        _set_state(tmp_root, status, now.strftime("%Y-%m-%d"))
    lifecycle, ran = _make_lc(tmp_root, monkeypatch, now)
    lifecycle.run_forever()

    assert bool(ran) is expect_run
    state = _state(tmp_root)
    assert (state.get("skip_reason") == "skip_weekday") is expect_skipped
    if expect_skipped:
        assert state.get("skip_weekday") == now.weekday()
        assert state.get("skip_weekday_date") == now.strftime("%Y-%m-%d")


def test_missing_skip_key_is_backward_compatible(tmp_root, monkeypatch):
    """缺省 / 未声明 skip_weekdays == 旧行为（不抑制）。"""
    _write_cfg(tmp_root, daily_time="06:00", omit_skip=True)
    lifecycle, ran = _make_lc(tmp_root, monkeypatch, SUNDAY)
    lifecycle.run_forever()
    assert bool(ran) is True
    assert _state(tmp_root).get("skip_reason") is None


def test_skip_marker_written_once_per_day(tmp_root, monkeypatch, caplog):
    """抑制事实每跳过日只写一次（避免每 tick 刷盘）——第二次 tick 不再记录。"""
    import logging as _logging
    _write_cfg(tmp_root, daily_time="06:00", skip_weekdays=(6,))
    lifecycle, ran = _make_lc(tmp_root, monkeypatch, SUNDAY)
    lifecycle.max_iterations = 2
    with caplog.at_level(_logging.INFO,
                         logger="quantstudio.pipeline.daemon_lifecycle"):
        lifecycle.run_forever()
    assert bool(ran) is False
    hits = [r for r in caplog.records if "属 skip_weekdays" in r.getMessage()]
    assert len(hits) == 1


def test_round_clears_skip_marker_fields(tmp_root, monkeypatch):
    """真起轮次即清空跳过日标记（防陈旧字段误导）——非跳过日轮次无残留。"""
    from quantstudio.pipeline.daemon_lifecycle import write_run_state
    write_run_state(skip_reason="skip_weekday", skip_weekday=6,
                    skip_weekday_date="2026-09-27")
    _write_cfg(tmp_root, daily_time="06:00", skip_weekdays=(6,))
    lifecycle, ran = _make_lc(tmp_root, monkeypatch, SATURDAY)

    def _fake_cycle(cfg):
        ran.append(1)
        write_run_state(scheduled_date="2026-09-26", status="running",
                        skip_reason=None, skip_weekday=None, skip_weekday_date=None)

    lifecycle.run_one_cycle = _fake_cycle
    lifecycle.run_forever()
    assert bool(ran) is True
    state = _state(tmp_root)
    assert "skip_reason" in state and state["skip_reason"] is None
    assert "skip_weekday" in state and state["skip_weekday"] is None


def test_non_skip_day_matches_legacy_reference(tmp_root, monkeypatch):
    """§5.5 回归：非跳过日的 should_run 与改动前参考实现逐位一致（笛卡尔网格）。"""
    for now_hm in ("05:00", "06:00", "23:00"):
        for status in (None, "completed", "running", "interrupted"):
            now = datetime(2026, 9, 26, int(now_hm[:2]), 30)   # 周六
            _write_cfg(tmp_root, daily_time="06:00", skip_weekdays=(6,))
            state_path = tmp_root / "daemon_run_state.json"
            state_path.unlink(missing_ok=True)
            if status is not None:
                _set_state(tmp_root, status, now.strftime("%Y-%m-%d"))
            lifecycle, ran = _make_lc(tmp_root, monkeypatch, now)
            lifecycle.run_forever()
            expected = _legacy_should_run(
                now.strftime("%H:%M"), "06:00", completed=(status == "completed"),
                pending_rerun=(status in ("running", "interrupted")))
            assert bool(ran) is expected, (now_hm, status)


# ===========================================================================
# 四、入口收敛（§5.4）：三处共用同一门实现 + 绕过用例被拦
# ===========================================================================

def test_three_entrypoints_share_single_gate_impl():
    """#1/#2/#3 三处入口共用同一门实现（daemon_hold_gate）。"""
    targets = {
        "scripts/launch_daemon.py": "enforce_hold_or_exit",
        "quantstudio/gui/daemon_process.py": "ensure_not_held",
        "quantstudio/pipeline/daemon.py": "enforce_hold_or_exit",
    }
    for rel, api in targets.items():
        text = (_ROOT / rel).read_text(encoding="utf-8")
        assert "daemon_hold_gate" in text, rel
        assert api in text, rel


def test_gui_entry_blocked_by_hold_gate(tmp_root, monkeypatch):
    """#2 GUI 路径：marker 生效时不 Popen，抛 HoldActiveError + 留痕。"""
    import quantstudio.gui.daemon_process as dp
    from quantstudio.pipeline.daemon_hold_gate import HoldActiveError
    _write_marker(tmp_root, "2999-01-01T00:00:00")

    def _boom(*args, **kwargs):
        raise AssertionError("hold 门生效时不得调用 Popen")

    monkeypatch.setattr(dp.subprocess, "Popen", _boom)
    with pytest.raises(HoldActiveError):
        dp.start_daemon_subprocess(tmp_root)
    assert len(_reject_traces(tmp_root)) == 1


def _run_subprocess(args, data_root: Path, timeout=240):
    env = {**os.environ, "QUANTSTUDIO_DATA_ROOT": str(data_root),
           "PYTHONIOENCODING": "utf-8"}
    return subprocess.run([sys.executable] + args, cwd=str(_ROOT), env=env,
                          capture_output=True, text=True, timeout=timeout)


def test_direct_module_invocation_blocked_by_fallback(tmp_root):
    """#3 绕过用例：直跑 `python -m quantstudio.pipeline.daemon` 仍被兜底拦截。"""
    _write_marker(tmp_root, "2999-01-01T00:00:00")
    cfg_dir = tmp_root / "cfg"
    cfg_dir.mkdir(exist_ok=True)
    result = _run_subprocess(
        ["-m", "quantstudio.pipeline.daemon", "--mode", "forever",
         "--config-dir", str(cfg_dir)], tmp_root)
    assert result.returncode == 3, (result.returncode, result.stdout[-2000:], result.stderr[-2000:])
    assert "拒启" in (result.stdout + result.stderr)
    assert len(_reject_traces(tmp_root)) == 1
    # 时点校验：兜底发生在取 .daemon.lock 之前
    assert not (tmp_root / ".daemon.lock").exists()


def test_launcher_script_blocked(tmp_root):
    """#1 新入口：scripts/launch_daemon.py 在 marker 生效时非零退出 + 留痕。"""
    _write_marker(tmp_root, "2999-01-01T00:00:00")
    result = _run_subprocess(
        ["scripts/launch_daemon.py", "--config-dir", "config/profiles/mcp_only"], tmp_root)
    assert result.returncode == 3, (result.returncode, result.stdout[-2000:], result.stderr[-2000:])
    assert "拒启" in (result.stdout + result.stderr)
    assert len(_reject_traces(tmp_root)) == 1
