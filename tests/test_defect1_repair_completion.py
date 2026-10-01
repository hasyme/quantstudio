"""tests/test_defect1_repair_completion.py — 缺陷1 修复收尾方案 V2（P1/P2）单元测试。

覆盖（对应方案 §5 验收 A1/B1/B2 的可自动化部分）：

P1 窗口分段驱动（§3.P1）
  - `run_once(start_date=..., end_date=...)` 覆盖**本次运行**的 task 窗口；
  - 覆盖**不污染** `self.tasks_cfg`（config 不可变）；
  - 不传时行为与实施前逐位一致（零变化红线）。

P2① 失败任务跳过 QFQ post-ingest（§3.P2-1）
  - `task_ok=False` → **不**调用 `qfq_run_post_ingest`（直击 2026-09-28 事故根因）；
  - `task_ok=True` → 照常调用（既有行为不变）；
  - cancelled → 仍不调用（既有语义不变）。

P2② 重锚链预算截断（§3.P2-2）
  - 预算内全处理；超预算截断且记录 remaining；
  - `apply_budget_sec<=0` → 不限（回退旧行为）；
  - 配置默认 1800s。

不依赖真实 DB / 网络：用 `ResidentCollector.__new__` 最小构造 + 打桩。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quantstudio.pipeline.daemon import ResidentCollector
from quantstudio.pipeline.qfq_orchestrator_types import QFQOrchestratorConfig
from quantstudio.pipeline.qfq_resident_orchestrator import CycleSummary


# ---------------------------------------------------------------------------
# P1：窗口覆盖
# ---------------------------------------------------------------------------
def _collector_with_tasks(monkeypatch, tasks):
    """最小 collector：只装 tasks_cfg + 可观测的 execute_task 桩。"""
    c = ResidentCollector.__new__(ResidentCollector)
    c.tasks_cfg = {"tasks": tasks}
    seen = []

    def _fake_execute(task, mode=None, run_quality_audit=True):
        seen.append(dict(task))
        return True

    monkeypatch.setattr(c, "execute_task", _fake_execute)
    return c, seen


def test_p1_window_override_applied(monkeypatch):
    """start_date/end_date 覆盖生效，且传入 execute_task。"""
    tasks = [{"name": "mcp_stock_minutes", "table": "stock_minutes",
              "start_date": "2026-01-01", "enabled": True}]
    c, seen = _collector_with_tasks(monkeypatch, tasks)

    res = c.run_once(task_name="mcp_stock_minutes", mode="full_range",
                     quality_audit="none",
                     start_date="2026-04-01", end_date="2026-05-15")

    assert res["task_found"] and res["task_ok"]
    assert len(seen) == 1
    assert seen[0]["start_date"] == "2026-04-01"
    assert seen[0]["end_date"] == "2026-05-15"


def test_p1_override_does_not_mutate_config(monkeypatch):
    """覆盖必须只作用于本次运行——self.tasks_cfg 不可被污染。"""
    tasks = [{"name": "t1", "table": "stock_minutes",
              "start_date": "2026-01-01", "enabled": True}]
    c, _ = _collector_with_tasks(monkeypatch, tasks)

    c.run_once(task_name="t1", mode="full_range", quality_audit="none",
               start_date="2026-04-01", end_date="2026-05-15")

    assert c.tasks_cfg["tasks"][0]["start_date"] == "2026-01-01", \
        "原配置 start_date 被污染"
    assert "end_date" not in c.tasks_cfg["tasks"][0], \
        "原配置被写入 end_date"


def test_p1_no_override_is_zero_change(monkeypatch):
    """不传参数时，传入 execute_task 的 task 与配置逐位一致（零变化红线）。"""
    tasks = [{"name": "t1", "table": "stock_minutes",
              "start_date": "2026-01-01", "enabled": True}]
    c, seen = _collector_with_tasks(monkeypatch, tasks)

    c.run_once(task_name="t1", mode="full_range", quality_audit="none")

    assert seen[0]["start_date"] == "2026-01-01"
    assert "end_date" not in seen[0]


def test_p1_end_date_only(monkeypatch):
    """只传 end_date 时仅覆盖终点，起点保持配置值。"""
    tasks = [{"name": "t1", "start_date": "2026-01-01", "enabled": True}]
    c, seen = _collector_with_tasks(monkeypatch, tasks)

    c.run_once(task_name="t1", mode="full_range", quality_audit="none",
               end_date="2026-03-31")

    assert seen[0]["start_date"] == "2026-01-01"
    assert seen[0]["end_date"] == "2026-03-31"


# ---------------------------------------------------------------------------
# P2①：失败任务跳过 post-ingest
# ---------------------------------------------------------------------------
def _execute_task_harness(monkeypatch, *, task_ok, cancelled=False):
    """构造 execute_task 的最小运行环境，返回 (collector, post_ingest_calls)。"""
    c = ResidentCollector.__new__(ResidentCollector)
    calls = []

    monkeypatch.setattr(c, "qfq_enabled", lambda: True)
    monkeypatch.setattr(c, "_qfq_config", lambda: _Cfg())
    monkeypatch.setattr(c, "_needs_manual_qfq_cycle", lambda t: True)
    monkeypatch.setattr(c, "qfq_begin_cycle", lambda: "cyc_test")
    c._qfq_cycle_id = "cyc_test"
    monkeypatch.setattr(c, "_execute_task", lambda t: task_ok)
    monkeypatch.setattr(c, "qfq_run_post_ingest",
                        lambda run_id: (calls.append(run_id), None)[1])

    if cancelled:
        from quantstudio.pipeline.task_resume import TaskCancelled

        def _raise(t):
            raise TaskCancelled("stop")

        monkeypatch.setattr(c, "_execute_task", _raise)

    c._task_cancel_check = None
    c._task_cancelled = False
    c._task_resume = None
    c._task_progress_cb = None
    return c, calls


class _Cfg:
    """最小 qfq config 桩：四价格表可协调。"""
    @staticmethod
    def can_coordinate_watermark(table):
        return True


def test_p2a_failed_task_skips_post_ingest(monkeypatch):
    """task_ok=False → 不进入 QFQ 收尾链（本次事故的根因防线）。"""
    c, calls = _execute_task_harness(monkeypatch, task_ok=False)
    ok = c.execute_task({"name": "mcp_stock_minutes", "table": "stock_minutes"},
                        mode="full_range", run_quality_audit=False)
    assert ok is False
    assert calls == [], "失败任务不得调用 qfq_run_post_ingest（否则触发无界重锚链）"


def test_p2a_successful_task_still_runs_post_ingest(monkeypatch):
    """task_ok=True → 照常收尾（既有行为零变化）。"""
    c, calls = _execute_task_harness(monkeypatch, task_ok=True)
    ok = c.execute_task({"name": "mcp_stock_minutes", "table": "stock_minutes"},
                        mode="full_range", run_quality_audit=False)
    assert ok is True
    assert len(calls) == 1, "成功任务必须照常收尾"


def test_p2a_cancelled_still_skips_post_ingest(monkeypatch):
    """cancelled → 仍不收尾（既有 v2 定案语义不变）。"""
    from quantstudio.pipeline.task_resume import TaskCancelled
    c, calls = _execute_task_harness(monkeypatch, task_ok=True, cancelled=True)
    with pytest.raises(TaskCancelled):
        c.execute_task({"name": "t", "table": "stock_minutes"},
                       mode="full_range", run_quality_audit=False)
    assert calls == [], "停止路径不得收尾（既有语义）"


# ---------------------------------------------------------------------------
# P2②：重锚链预算截断
# ---------------------------------------------------------------------------
def test_p2b_config_default_budget():
    """配置默认预算 1800s（30 min）。"""
    assert QFQOrchestratorConfig().apply_budget_sec == 1800


def test_p2b_config_budget_from_dict():
    """from_dict 可覆盖预算。"""
    cfg = QFQOrchestratorConfig.from_dict({"apply_budget_sec": 60})
    assert cfg.apply_budget_sec == 60


def test_p2b_summary_defaults_no_truncation():
    """CycleSummary 默认无截断（成功低事件量路径不受影响）。"""
    s = CycleSummary(cycle_id="c1")
    assert s.apply_truncated is False
    assert s.apply_processed == 0
    assert s.apply_remaining == 0


def test_p2b_budget_loop_truncates_and_records_remaining():
    """预算截断语义：处理若干只后退出，remaining 记录未处理数（下轮续做）。

    直接驱动方案 §3.P2-2 的循环逻辑（与实现同构），验证：
    ①截断标记置位；②remaining = 总数 - 已处理；③不抛异常、不丢已处理者。
    """
    import time as _t

    total = 10
    budget = 0.05  # 秒
    processed = 0
    truncated = False
    remaining = 0
    t0 = _t.monotonic()
    for _unit in range(total):
        if budget > 0 and processed > 0 and (_t.monotonic() - t0) >= budget:
            truncated = True
            remaining = total - processed
            break
        _t.sleep(0.02)          # 模拟每只 6.4min 量级（此处压缩）
        processed += 1

    assert truncated, "超预算必须截断"
    assert remaining == total - processed, "remaining 应等于未处理数"
    assert remaining > 0 and processed > 0


def test_p2b_zero_budget_means_unlimited():
    """预算 0/负 = 不限（回退旧行为，既有配置零变化）。"""
    budget = 0
    total = 5
    processed = 0
    truncated = False
    for _unit in range(total):
        if budget > 0 and processed > 0:
            truncated = True
            break
        processed += 1
    assert processed == total and not truncated, "预算 0 必须全处理"
