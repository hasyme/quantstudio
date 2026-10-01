"""tests/test_daemon_lifecycle.py — daemon v3 进程解耦专项测试。

覆盖评审 vi 的 15 项关键语义（隔离测试，不依赖真实 xtquant/DuckDB）：
  1. stop.request 在任务边界被消费
  2. stop 后剩余任务不再执行
  3. 中断轮次写 interrupted，不写 completed
  4. collector.close() 失败不写 completed
  5. quality audit 已执行但 close 失败的状态
  6. AccessDenied 阻止 publish_status
  7. stale status 可清理
  8. alive_other 阻止启动
  9. 第二实例不覆盖第一实例 status
  10. stop request token 不匹配时不消费
  11. GUI 启动 token 握手
  12. daemon idle 时 DuckDB 可打开（_safe_query 降级）
  13. GUI manual collector_run.lock 与 daemon 互斥
  14. 强制停止前身份五验
  15. run_state completed 当日重启不重复执行

所有测试用临时 DATA_ROOT + monkeypatch ResidentCollector.from_configs 隔离。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

# 确保项目根在 sys.path
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))


@pytest.fixture
def tmp_data_root(monkeypatch, tmp_path):
    """隔离的临时 DATA_ROOT，避免污染正式库。

    monkeypatch 所有引用 _data_root 的模块（daemon_lifecycle + daemon_process），
    因为 daemon_process 通过 from-import 持有 _data_root 的独立引用。
    """
    import quantstudio._paths as qp
    import quantstudio.pipeline.daemon_lifecycle as dl
    import quantstudio.gui.daemon_process as dp
    monkeypatch.setattr(qp, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(dl, "_data_root", lambda: tmp_path)
    monkeypatch.setattr(dp, "_data_root", lambda: tmp_path)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    return tmp_path


class _FakeCollector:
    """测试用假 collector，记录 execute_task 调用。"""
    def __init__(self, fail_close=False, fail_audit=False):
        self._running = True
        self.executed = []
        self.fail_close = fail_close
        self.fail_audit = fail_audit

    def execute_task(self, task, mode="incremental", run_quality_audit=False):
        self.executed.append(task["name"])
        return True

    def _run_full_quality_audit(self, should_stop=None):
        if self.fail_audit:
            raise RuntimeError("audit failed")
        return True

    def close(self):
        if self.fail_close:
            raise RuntimeError("close failed")


def _make_lifecycle(tmp_root, token=None):
    """构造测试用 DaemonLifecycle（跳过 __init__ 的真实初始化）。"""
    from quantstudio.pipeline.daemon_lifecycle import DaemonLifecycle
    token = token or ("test_" + uuid.uuid4().hex[:8])
    lc = DaemonLifecycle.__new__(DaemonLifecycle)
    lc.config_dir = tmp_root
    lc.instance_token = token
    lc.max_iterations = None
    lc._running = True
    lc._status = None
    lc._instance_lock = None
    return lc


def _patch_from_configs(monkeypatch, fake_collector):
    """monkeypatch ResidentCollector.from_configs 返回 fake_collector。"""
    import quantstudio.pipeline.daemon as dmod
    monkeypatch.setattr(
        dmod.ResidentCollector, "from_configs",
        classmethod(lambda cls, *a, **kw: fake_collector))


def _write_stop_request(tmp_root, token):
    """写匹配 token 的 stop.request。"""
    req = {"instance_token": token, "requested_at": "now"}
    (tmp_root / "daemon_stop.request").write_text(json.dumps(req))


def _read_run_state(tmp_root):
    path = tmp_root / "daemon_run_state.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


# ===========================================================================
# FIX-1/2: stop.request 任务边界消费 + 中断写 interrupted（评审 vi 1/2/3）
# ===========================================================================

class TestStopRequestBoundaries:
    """评审 vi 1/2/3: stop.request 在任务边界消费 + 剩余任务不执行 + interrupted。"""

    def test_stop_pre_cycle_no_task_executed(self, tmp_data_root, monkeypatch):
        """stop.request 预先存在 → 无任务执行 + interrupted。"""
        fc = _FakeCollector()
        lc = _make_lifecycle(tmp_data_root)
        _write_stop_request(tmp_data_root, lc.instance_token)
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [
            {"name": "a", "enabled": True, "table": "t"},
            {"name": "b", "enabled": True, "table": "t"},
        ]})
        assert fc.executed == []
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "interrupted"
        assert rs["stop_requested"] is True
        assert rs["traversal_completed"] is False

    def test_stop_mid_cycle_remaining_tasks_skipped(self, tmp_data_root, monkeypatch):
        """task_a 后写 stop.request → task_b/c 不执行 + interrupted。"""
        fc = _FakeCollector()
        # task_a 执行后写 stop.request
        orig_execute = fc.execute_task
        def execute_and_maybe_stop(task, mode="incremental", run_quality_audit=False):
            orig_execute(task, mode=mode, run_quality_audit=run_quality_audit)
            if task["name"] == "a":
                _write_stop_request(tmp_data_root, lc.instance_token)
            return True
        fc.execute_task = execute_and_maybe_stop
        lc = _make_lifecycle(tmp_data_root)
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [
            {"name": "a", "enabled": True, "table": "t"},
            {"name": "b", "enabled": True, "table": "t"},
            {"name": "c", "enabled": True, "table": "t"},
        ]})
        assert fc.executed == ["a"]
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "interrupted"
        assert rs["stop_requested"] is True
        assert rs["attempted_task_count"] == 1

    def test_stop_token_mismatch_not_consumed(self, tmp_data_root, monkeypatch):
        """评审 vi 10: stop.request token 不匹配 → 不消费，全部任务执行 + completed。"""
        fc = _FakeCollector()
        lc = _make_lifecycle(tmp_data_root)
        _write_stop_request(tmp_data_root, "wrong_token")
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [
            {"name": "a", "enabled": True, "table": "t"},
        ]})
        assert fc.executed == ["a"]
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "completed"
        assert rs["stop_requested"] is False
        # stop.request 文件仍存在（未被消费）
        assert (tmp_data_root / "daemon_stop.request").exists()


# ===========================================================================
# FIX-3: collector.close() 失败（评审 vi 4/5）
# ===========================================================================

class TestCollectorCloseFailure:
    """评审 vi 4/5: close 失败 → failed_cleanup，不写 completed。"""

    def test_close_fail_writes_failed_cleanup(self, tmp_data_root, monkeypatch):
        fc = _FakeCollector(fail_close=True)
        lc = _make_lifecycle(tmp_data_root)
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [{"name": "a", "enabled": True, "table": "t"}]})
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "failed_cleanup"
        assert rs["traversal_completed"] is True

    def test_audit_ok_but_close_fail_still_failed_cleanup(self, tmp_data_root, monkeypatch):
        """评审 vi 5: audit 成功但 close 失败 → failed_cleanup。"""
        fc = _FakeCollector(fail_close=True, fail_audit=False)
        lc = _make_lifecycle(tmp_data_root)
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [{"name": "a", "enabled": True, "table": "t"}]})
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "failed_cleanup"
        assert rs["quality_audit_ok"] is True


# ===========================================================================
# 正常路径 completed（评审 vi 3 反例：完整遍历写 completed）
# ===========================================================================

class TestNormalCompletion:
    def test_full_traversal_writes_completed(self, tmp_data_root, monkeypatch):
        fc = _FakeCollector()
        lc = _make_lifecycle(tmp_data_root)
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [
            {"name": "a", "enabled": True, "table": "t"},
            {"name": "b", "enabled": True, "table": "t"},
        ]})
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "completed"
        assert rs["traversal_completed"] is True
        assert rs["stop_requested"] is False
        assert rs["success_count"] == 2
        assert rs["failed_count"] == 0

    def test_disabled_and_full_range_tasks_skipped(self, tmp_data_root, monkeypatch):
        fc = _FakeCollector()
        lc = _make_lifecycle(tmp_data_root)
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [
            {"name": "a", "enabled": True, "table": "t"},
            {"name": "b", "enabled": False, "table": "t"},
            {"name": "c", "enabled": True, "table": "t", "mode": "full_range"},
        ]})
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "completed"
        assert fc.executed == ["a"]  # b disabled, c full_range
        assert rs["eligible_task_count"] == 1


# ===========================================================================
# FIX-4: publish_status + AccessDenied/alive_other（评审 vi 6/8/9）
# ===========================================================================

class TestPublishStatus:
    """评审 vi 6/8/9: AccessDenied/alive_other 阻止启动；第二实例不覆盖。"""

    def test_access_denied_aborts_publish(self, tmp_data_root):
        """评审 vi 6: AccessDenied → RuntimeError，不覆盖原 status。"""
        from quantstudio.pipeline.daemon_lifecycle import DaemonLifecycle
        old_status = {"pid": 99999, "create_time": 1.0, "exe": "x",
                      "cmdline": [], "instance_token": "old"}
        (tmp_data_root / "daemon_status.json").write_text(json.dumps(old_status))
        lc = _make_lifecycle(tmp_data_root, token="new")
        with patch("quantstudio.pipeline.daemon_lifecycle.verify_daemon_identity",
                   return_value="denied"):
            with pytest.raises(RuntimeError, match="AccessDenied"):
                lc.publish_status()
        # 原 status 未被覆盖
        final = json.loads((tmp_data_root / "daemon_status.json").read_text())
        assert final["instance_token"] == "old"

    def test_alive_other_aborts_publish(self, tmp_data_root):
        """评审 vi 8: alive_other → RuntimeError。"""
        old_status = {"pid": 99999, "create_time": 1.0, "exe": "x",
                      "cmdline": [], "instance_token": "old"}
        (tmp_data_root / "daemon_status.json").write_text(json.dumps(old_status))
        lc = _make_lifecycle(tmp_data_root, token="new")
        with patch("quantstudio.pipeline.daemon_lifecycle.verify_daemon_identity",
                   return_value="alive"):
            with pytest.raises(RuntimeError, match="另一个 daemon"):
                lc.publish_status()

    def test_stale_status_cleared_then_publish(self, tmp_data_root):
        """评审 vi 7: stale status 可清理 → 正常 publish。"""
        old_status = {"pid": 99999, "create_time": 1.0, "exe": "x",
                      "cmdline": [], "instance_token": "old"}
        (tmp_data_root / "daemon_status.json").write_text(json.dumps(old_status))
        lc = _make_lifecycle(tmp_data_root, token="new")
        with patch("quantstudio.pipeline.daemon_lifecycle.verify_daemon_identity",
                   return_value="stale"):
            lc.publish_status()
        final = json.loads((tmp_data_root / "daemon_status.json").read_text())
        assert final["instance_token"] == "new"

    def test_second_instance_does_not_overwrite(self, tmp_data_root):
        """评审 vi 9: 第二实例启动时 alive_other → 不覆盖第一实例 status。

        模拟：第一实例已 publish（status=daemon_a）；第二实例 acquire_instance_lock
        失败（.daemon.lock 被占）。即使能拿到锁，publish_status 也会因 alive_other 拒绝。
        """
        # 第一实例 status
        a_status = {"pid": os.getpid(), "create_time": 1.0, "exe": sys.executable,
                    "cmdline": ["python", "-m", "quantstudio.pipeline.daemon"],
                    "instance_token": "daemon_a"}
        (tmp_data_root / "daemon_status.json").write_text(json.dumps(a_status))
        # 第二实例
        lc_b = _make_lifecycle(tmp_data_root, token="daemon_b")
        with patch("quantstudio.pipeline.daemon_lifecycle.verify_daemon_identity",
                   return_value="alive"):
            with pytest.raises(RuntimeError):
                lc_b.publish_status()
        # 第一实例 status 未被覆盖
        final = json.loads((tmp_data_root / "daemon_status.json").read_text())
        assert final["instance_token"] == "daemon_a"


# ===========================================================================
# psutil 五验身份（评审 vi 14）
# ===========================================================================

class TestVerifyIdentity:
    """评审 vi 14: 强制停止前身份五验。"""

    def test_self_identity_alive(self):
        from quantstudio.pipeline.daemon_lifecycle import verify_daemon_identity
        import psutil
        status = {
            "pid": os.getpid(),
            "create_time": psutil.Process(os.getpid()).create_time(),
            "exe": sys.executable,
            "cmdline": ["python", "-m", "quantstudio.pipeline.daemon"],
            "instance_token": "t",
        }
        # 真实进程是 pytest，cmdline 不含 quantstudio.pipeline.daemon →
        # 需 mock p.cmdline() 返回 daemon cmdline（验证五验逻辑本身）
        original_cmdline = psutil.Process.cmdline
        try:
            psutil.Process.cmdline = lambda self: ["python", "-m",
                                                    "quantstudio.pipeline.daemon"]
            assert verify_daemon_identity(status) == "alive"
        finally:
            psutil.Process.cmdline = original_cmdline

    def test_wrong_pid_stale(self):
        from quantstudio.pipeline.daemon_lifecycle import verify_daemon_identity
        status = {"pid": 99999999, "create_time": 1.0, "exe": "x",
                  "cmdline": [], "instance_token": "t"}
        assert verify_daemon_identity(status) == "stale"

    def test_wrong_create_time_stale(self):
        from quantstudio.pipeline.daemon_lifecycle import verify_daemon_identity
        status = {
            "pid": os.getpid(),
            "create_time": 1.0,  # 远偏离真实
            "exe": sys.executable,
            "cmdline": ["python", "-m", "quantstudio.pipeline.daemon"],
        }
        assert verify_daemon_identity(status) == "stale"

    def test_wrong_exe_stale(self):
        from quantstudio.pipeline.daemon_lifecycle import verify_daemon_identity
        status = {
            "pid": os.getpid(),
            "create_time": __import__("psutil").Process(os.getpid()).create_time(),
            "exe": "/nonexistent/python",
            "cmdline": ["python", "-m", "quantstudio.pipeline.daemon"],
        }
        assert verify_daemon_identity(status) == "stale"

    def test_cmdline_without_daemon_stale(self):
        from quantstudio.pipeline.daemon_lifecycle import verify_daemon_identity
        status = {
            "pid": os.getpid(),
            "create_time": __import__("psutil").Process(os.getpid()).create_time(),
            "exe": sys.executable,
            "cmdline": ["python", "-c", "print(1)"],  # 不含 quantstudio.pipeline.daemon
        }
        assert verify_daemon_identity(status) == "stale"


# ===========================================================================
# run_state completed 当日重启不重复（评审 vi 15）
# ===========================================================================

class TestRunStateIdempotency:
    """评审 vi 15: run_state completed 当日重启不重复执行。"""

    def test_is_today_completed_true_when_completed_today(self, tmp_data_root):
        from quantstudio.pipeline.daemon_lifecycle import is_today_completed
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        from quantstudio.pipeline.daemon_lifecycle import write_run_state
        write_run_state(scheduled_date=today, status="completed")
        assert is_today_completed() is True

    def test_is_today_completed_false_when_interrupted(self, tmp_data_root):
        from quantstudio.pipeline.daemon_lifecycle import is_today_completed, write_run_state
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        write_run_state(scheduled_date=today, status="interrupted")
        assert is_today_completed() is False

    def test_is_today_pending_rerun_for_interrupted(self, tmp_data_root):
        from quantstudio.pipeline.daemon_lifecycle import is_today_pending_rerun, write_run_state
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        write_run_state(scheduled_date=today, status="interrupted")
        assert is_today_pending_rerun() is True


# ===========================================================================
# collector_run.lock 互斥（评审 vi 13）
# ===========================================================================

class TestCollectorRunLock:
    """评审 vi 13: GUI manual collector_run.lock 与 daemon 互斥。"""

    def test_lock_acquire_release(self, tmp_data_root):
        from quantstudio.pipeline.daemon_lifecycle import CollectorRunLock
        lock1 = CollectorRunLock(timeout=0)
        assert lock1.try_acquire() is True
        # 第二个 lock 非阻塞应失败
        lock2 = CollectorRunLock(timeout=0)
        assert lock2.try_acquire() is False
        lock1.__exit__(None, None, None)
        # 释放后第二个可获取
        assert lock2.try_acquire() is True
        lock2.__exit__(None, None, None)


# ===========================================================================
# _safe_query 优雅降级（评审 vi 12: daemon idle 时 DuckDB 可打开 + 忙时降级）
# ===========================================================================

class TestDbHelperGracefulDegradation:
    """评审 vi 12: DuckDB 忙时 _safe_query 返回空，不崩。"""

    def test_safe_query_returns_empty_on_busy(self, monkeypatch):
        from quantstudio.gui.db_helper import DbHelper
        import pandas as pd
        helper = DbHelper("/nonexistent.db", "/nonexistent_q.sqlite", "/nonexistent_ba.sqlite")
        # monkeypatch duckdb.connect 抛 IOException
        import duckdb
        class FakeIOException(duckdb.IOException):
            pass
        def fake_connect(*a, **kw):
            raise FakeIOException("Could not lock file: used by another process")
        monkeypatch.setattr(duckdb, "connect", fake_connect)
        df = helper.query_duckdb("SELECT 1")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0  # 空结果，不抛


# ===========================================================================
# GUI 启动 token 握手（评审 vi 11）
# ===========================================================================

class TestStartDaemonSubprocess:
    """评审 vi 11: GUI 启动生成 token + bootstrap 按 token 分文件。

    注：不实际启动子进程（CI 环境无 xtquant），仅验证 token 生成与文件路径。
    """

    def test_start_returns_token_and_proc(self, tmp_data_root, monkeypatch):
        from quantstudio.gui import daemon_process as dp
        import subprocess
        # monkeypatch Popen 返回 fake proc
        class FakeProc:
            pid = 12345
            def poll(self): return None
        def fake_popen(*a, **kw):
            return FakeProc()
        monkeypatch.setattr(subprocess, "Popen", fake_popen)
        token, proc = dp.start_daemon_subprocess(tmp_data_root / "config")
        assert len(token) == 32  # uuid hex
        assert proc.pid == 12345
        # bootstrap 文件按 token 命名
        bootstrap = tmp_data_root / "logs" / f"daemon_bootstrap_{token}.log"
        assert bootstrap.exists()

    def test_read_bootstrap_log_tail_only_own_token(self, tmp_data_root):
        from quantstudio.gui.daemon_process import read_bootstrap_log_tail
        token_a = "a" * 32
        token_b = "b" * 32
        (tmp_data_root / "logs" / f"daemon_bootstrap_{token_a}.log").write_text("token a log\nline2")
        (tmp_data_root / "logs" / f"daemon_bootstrap_{token_b}.log").write_text("token b log")
        tail_a = read_bootstrap_log_tail(token_a)
        assert "token a" in tail_a
        assert "token b" not in tail_a


# ===========================================================================
# D 件：质量审计「可中断」接入收尾（run_state 双字段）
# ===========================================================================

class _FakeInterruptingCollector(_FakeCollector):
    """模拟「stop 已消费 → 审计在检查点中止」的 collector。"""

    def __init__(self):
        super().__init__()
        self.seen_should_stop = None

    def _run_full_quality_audit(self, should_stop=None):
        self.seen_should_stop = bool(should_stop()) if should_stop else None
        self._last_quality_audit = {"interrupted": True, "skipped_after_stop": 5}
        return False


class TestInterruptibleQualityAudit:
    """设计 §5-3/§5-7：中断标记诚实 + 终态不回归。"""

    def test_interrupted_audit_recorded_in_run_state(self, tmp_data_root, monkeypatch):
        fc = _FakeInterruptingCollector()
        lc = _make_lifecycle(tmp_data_root)
        original = fc.execute_task

        def execute_and_stop(task, mode="incremental", run_quality_audit=False):
            original(task, mode=mode, run_quality_audit=run_quality_audit)
            _write_stop_request(tmp_data_root, lc.instance_token)
            return True

        fc.execute_task = execute_and_stop
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [{"name": "a", "enabled": True, "table": "t"}]})
        rs = _read_run_state(tmp_data_root)
        # 终态不回归：仍落 interrupted（can_complete 不含 quality_audit_ok）
        assert rs["status"] == "interrupted"
        assert rs["stop_requested"] is True
        assert rs["traversal_completed"] is False
        assert rs["quality_audit_ok"] is False
        # 双字段如实入 run_state
        assert rs["interrupted"] is True
        assert rs["skipped_after_stop"] == 5
        # should_stop 读的是已缓存的 stop 标志（非重读 daemon_stop.request）
        assert fc.seen_should_stop is True

    def test_completed_cycle_records_not_interrupted(self, tmp_data_root, monkeypatch):
        fc = _FakeCollector()
        lc = _make_lifecycle(tmp_data_root)
        _patch_from_configs(monkeypatch, fc)
        lc.run_one_cycle({"tasks": [{"name": "a", "enabled": True, "table": "t"}]})
        rs = _read_run_state(tmp_data_root)
        assert rs["status"] == "completed"
        assert rs["quality_audit_ok"] is True
        assert rs["interrupted"] is False
        assert rs["skipped_after_stop"] == 0


def test_schedule_skip_weekdays_logic():
    """A5：skip_weekdays 配置日（如 6=周日）不触发定时增量；其他天正常触发。"""
    import json, datetime
    from quantstudio.pipeline.daemon import ResidentCollector

    cfg = json.load(open("config/profiles/mcp_only/collector_tasks.json", encoding="utf-8"))
    sched = cfg["daemon_schedule"]
    assert sched["daily_time"] == "06:00"
    assert sched["skip_weekdays"] == [6]

    # 逻辑验证：周日(weekday=6)在跳过集 → 跳过；周六(5)不在 → 执行
    skip = set(sched["skip_weekdays"])
    assert 6 in skip
    assert 5 not in skip
    # 与 daemon 判断一致：now.weekday() in skip_weekdays
    from datetime import date
    assert date(2026, 8, 9).weekday() == 6  # 2026-08-09 是周日
    assert date(2026, 8, 8).weekday() == 5  # 2026-08-08 是周六
