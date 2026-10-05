# -*- coding: utf-8 -*-
"""MCP passthrough 首轮拉取修复（F-1/F-2）的单元测试。

方案：docs/mcp-passthrough-first-pull-export-fix-design.md（v4）§5.1
探针证据：docs/evidence/mcp-passthrough-window-pushdown-probe-20261005-0403.md

全部用例 monkeypatch，**不依赖网络 / 不连服务端 / 不写生产库**。
覆盖：
  §3.1  export 异步贯通 + 白名单窗口 + 预算错误消费（含 R-1 非名单表直接上抛、
        重叠 suggested_shards 回落二分、聚合断言失败禁写库、0 行守卫触发条件）；
  §3.2  fetch_page 首页探测五出口（超时降级 / Auth·Protocol 不降级 / 非超时重发 /
        兜底未包装异常 / 首页成功后续页不变）+ B1 开关 false 逐行等效 + M-2 零 sleep；
  §3.3  configure_execution 仅同步 call_timeout（M3/M4 锁定不同步其余三项）；
  S3    max_attempts 不进入服务端 payload（kw-only）；
  M7    四个无关调用点序列不变。
"""
import io
import sys
import threading
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from quantstudio.pipeline.mcp.client import MCPClient  # noqa: E402
from quantstudio.pipeline.mcp.errors import (  # noqa: E402
    MCPAuthError, MCPClientError, MCPExportBudgetError, MCPProtocolError,
    MCPToolError, MCPTransportError,
)
from quantstudio.pipeline.mcp.models import Artifact, ExportManifest, Shard  # noqa: E402
from quantstudio.pipeline.sources import mcp_adapter as ma  # noqa: E402


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------
def _df(rows):
    """rows: [(date, code)] → 含 date/ts_code 列的 DataFrame。"""
    return pd.DataFrame({"date": [r[0] for r in rows],
                         "ts_code": [r[1] for r in rows],
                         "x": list(range(len(rows)))})


def _bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _artifact(df, aid="job/s0"):
    return Artifact(artifact_id=aid, sha256="", size_bytes=1,
                    parquet_bytes=_bytes(df), raw={"job_id": "job"})


class _FakeClient:
    """模拟 MCPClient：忠实复刻 _call_with_retry 的「有效尝试上限」语义。"""

    def __init__(self):
        self.retry_max = 5
        self.call_timeout = 90.0
        self.backoff_sec = (30, 60, 120, 240, 480)
        self.rate_per_min = 200
        self._lock = threading.Lock()
        # 记录
        self.fetch_page_calls = []      # dict(dataset_id, cursor, max_attempts)
        self.attempts = 0               # 服务端实际尝试次数（模拟重试循环）
        self.export_calls = []          # export_dataset kwargs
        self.create_calls = []          # create_export_job kwargs
        self.get_manifest_calls = []
        # 行为注入
        self.page_fn = None             # (cursor, attempt_no) -> payload | raise
        self.export_fn = None           # (**kw) -> List[Artifact] | raise
        self.subwindow_fn = None        # (ts, te) -> DataFrame（子窗数据）
        self.reported_rows_fn = None    # (ts, te, df) -> int（manifest 申报行数）
        self._job_seq = 0

    # --- fetch_page ---
    def fetch_page(self, dataset_id, cursor="", page_size=50000, columns=None,
                   *, max_attempts=None):
        eff = max_attempts if max_attempts else self.retry_max
        self.fetch_page_calls.append({"dataset_id": dataset_id, "cursor": cursor,
                                      "max_attempts": max_attempts, "eff": eff})
        last = None
        for i in range(eff):
            self.attempts += 1
            try:
                if self.page_fn is None:
                    return {"rows": [], "next_cursor": None}
                return self.page_fn(cursor, i)
            except (MCPAuthError, MCPProtocolError):
                raise
            except BaseException as e:  # noqa: BLE001
                last = e
        raise MCPTransportError(f"MCP 重试 {eff} 次仍失败: {last}") from last

    # --- export ---
    def export_dataset(self, dataset_id, page_size=50000, *, verify_each_shard=True,
                       verify_concat=False, time_start=None, time_end=None,
                       row_limit=None, async_mode=False, poll_timeout_sec=600.0):
        kw = dict(dataset_id=dataset_id, page_size=page_size, time_start=time_start,
                  time_end=time_end, row_limit=row_limit, async_mode=async_mode,
                  poll_timeout_sec=poll_timeout_sec)
        self.export_calls.append(kw)
        if self.export_fn is None:
            return []
        return self.export_fn(**kw)

    def create_export_job(self, dataset_id, page_size=50000, *, idempotency_key=None,
                          time_start=None, time_end=None, row_limit=None,
                          async_mode=False):
        self.create_calls.append(dict(dataset_id=dataset_id, time_start=time_start,
                                      time_end=time_end, async_mode=async_mode))
        self._job_seq += 1
        return f"job{self._job_seq}"

    def get_manifest(self, job_id, *, await_ready=False, poll_interval_sec=2.0,
                     poll_timeout_sec=600.0):
        self.get_manifest_calls.append(job_id)
        ts = self.create_calls[-1]["time_start"]
        te = self.create_calls[-1]["time_end"]
        df = self.subwindow_fn(ts, te) if self.subwindow_fn else pd.DataFrame()
        rows = self.reported_rows_fn(ts, te, df) if self.reported_rows_fn else len(df)
        return ExportManifest(job_id=job_id, dataset_id="d", table="t",
                              total_rows=int(rows), shard_count=1,
                              shards=[Shard(shard_id="s0", parquet_uri="u",
                                            rows=int(rows))],
                              raw={})

    def get_artifact(self, job_id, artifact_id, *, verify_sha256=True):
        ts = self.create_calls[-1]["time_start"]
        te = self.create_calls[-1]["time_end"]
        df = self.subwindow_fn(ts, te) if self.subwindow_fn else pd.DataFrame()
        return _artifact(df, aid=f"{job_id}_{artifact_id}".replace("/", "_"))


def _adapter(monkeypatch, tmp_path, cfg=None):
    a = ma.MCPAdapter(dict(cfg or {"name": "mcp"}))
    fake = _FakeClient()
    a._client = fake
    monkeypatch.setattr(a, "_landing_path",
                        lambda job_id, shard_name: tmp_path / f"{job_id}_{shard_name}.parquet")
    return a, fake


def _client(monkeypatch):
    """真实 MCPClient（无网络调用），供 _call_with_retry 语义用例使用。"""
    c = MCPClient(endpoint="http://127.0.0.1:9/mcp", tls_verify=False,
                  api_key="k", retry_budget_sec=0)
    monkeypatch.setattr(c, "_acquire_rate", lambda: None)
    monkeypatch.setattr(c, "_sleep_with_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(c, "_reset_connection", lambda *a, **k: None)
    return c


# ===========================================================================
# §5.1-A  client.py：_call_with_retry / fetch_page（M-2 零空转、S3 payload）
# ===========================================================================
def test_max_attempts_one_zero_sleep(monkeypatch):
    """M-2：max_attempts=1 单次失败 ⇒ 不 sleep、不 reset，直接抛终态。"""
    c = _client(monkeypatch)
    sleeps, resets, calls = [], [], []

    def boom(*a, **k):
        calls.append(1)
        raise TimeoutError("_call_tool 超过 89.5s 未返回")

    monkeypatch.setattr(c, "_sleep_with_heartbeat", lambda *a, **k: sleeps.append(a[0]))
    monkeypatch.setattr(c, "_reset_connection", lambda *a, **k: resets.append(1))
    with pytest.raises(MCPTransportError) as ei:
        c._call_with_retry(boom, max_attempts=1)
    assert len(calls) == 1, f"应仅尝试 1 次: {len(calls)}"
    assert sleeps == [], f"max_attempts=1 不得 sleep: {sleeps}"
    assert resets == [], f"max_attempts=1 不得重置连接: {resets}"
    assert isinstance(ei.value.__cause__, TimeoutError)


def test_max_attempts_none_equivalent_to_retry_max(monkeypatch):
    """max_attempts=None ⇒ 与改动前逐行等效（5 次尝试、4 次退避）。"""
    c = _client(monkeypatch)
    sleeps, calls = [], []

    def boom(*a, **k):
        calls.append(1)
        raise MCPToolError("tool error")

    monkeypatch.setattr(c, "_sleep_with_heartbeat", lambda *a, **k: sleeps.append(a[0]))
    with pytest.raises(MCPTransportError) as ei:
        c._call_with_retry(boom)
    assert len(calls) == 5, f"默认应 5 次尝试: {len(calls)}"
    assert sleeps == [30, 60, 120, 240], f"退避序列被改变: {sleeps}"
    assert "MCP 重试 5 次仍失败" in str(ei.value)


def test_retry_max_one_boundary_still_one_attempt(monkeypatch):
    """retry_max=1 且未传 max_attempts ⇒ 1 次尝试、0 次退避。"""
    c = _client(monkeypatch)
    c.retry_max = 1
    sleeps, calls = [], []
    monkeypatch.setattr(c, "_sleep_with_heartbeat", lambda *a, **k: sleeps.append(a[0]))
    with pytest.raises(MCPTransportError):
        c._call_with_retry(lambda *a, **k: calls.append(1) or (_ for _ in ()).throw(
            MCPToolError("x")))
    assert len(calls) == 1 and sleeps == []


def test_fetch_page_payload_has_no_max_attempts(monkeypatch):
    """S3：max_attempts 为 kw-only，绝不进入服务端 payload。"""
    c = _client(monkeypatch)
    seen = {}
    monkeypatch.setattr(c, "_call_tool",
                        lambda tool, args: seen.update(tool=tool, args=args)
                        or {"rows": [], "next_cursor": None})
    c.fetch_page(dataset_id="t", max_attempts=1)
    assert seen["tool"] == "fetch_page"
    assert "max_attempts" not in seen["args"], f"payload 泄漏 max_attempts: {seen['args']}"
    # kw-only：位置传参必须 TypeError
    with pytest.raises(TypeError):
        c.fetch_page("t", "", 50000, None, 1)


def test_fetch_page_forwards_max_attempts_to_retry(monkeypatch):
    """fetch_page 透传 max_attempts 给 _call_with_retry（默认 None）。"""
    c = _client(monkeypatch)
    got = []
    orig = c._call_with_retry
    monkeypatch.setattr(c, "_call_with_retry",
                        lambda fn, *a, **k: got.append(k.get("max_attempts")) or orig(fn, *a, **k))
    monkeypatch.setattr(c, "_call_tool",
                        lambda tool, args: {"rows": [], "next_cursor": None})
    c.fetch_page(dataset_id="t")
    c.fetch_page(dataset_id="t", max_attempts=1)
    assert got == [None, 1], f"透传序列异常: {got}"


# ===========================================================================
# §5.1-B  _fetch_export_passthrough（白名单窗口 / 预算错误 / 聚合 / 0 行守卫）
# ===========================================================================
def test_window_pushed_only_for_whitelisted_table(monkeypatch, tmp_path):
    """名单内表：下推半开区间 + async；名单外表：不下推（async 仍贯通）。"""
    uni = _df([("2026-01-01", "A"), ("2026-02-01", "B")])
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "export_async": True,
        "passthrough_export_window_tables": ["llm_text_events"]})
    fake.export_fn = lambda **kw: [_artifact(uni)]

    df, meta = a._fetch_export_passthrough("llm_text_events", "daily",
                                           "2026-01-01", "2026-10-05")
    kw = fake.export_calls[-1]
    assert kw["time_start"] == "2026-01-01T00:00:00"
    assert kw["time_end"] == "2026-10-06T00:00:00"     # 半开：end+1day
    assert kw["async_mode"] is True
    assert meta["window"] is True and meta["async"] is True

    # 名单外（rsshub_raw 不在名单）
    df2, meta2 = a._fetch_export_passthrough("rsshub_raw", "daily",
                                             "2026-01-01", "2026-10-05")
    kw2 = fake.export_calls[-1]
    assert kw2["time_start"] is None and kw2["time_end"] is None
    assert kw2["async_mode"] is True                    # async 不受白名单约束
    assert meta2["window"] is False


def test_async_default_false_when_not_configured(monkeypatch, tmp_path):
    """export_async 未配置 ⇒ async_mode=False（默认关，旧行为）。"""
    a, fake = _adapter(monkeypatch, tmp_path, {"name": "mcp"})
    fake.export_fn = lambda **kw: [_artifact(_df([("2026-01-01", "A")]))]
    a._fetch_export_passthrough("rsshub_raw", "daily", "2026-01-01", "2026-10-05")
    assert fake.export_calls[-1]["async_mode"] is False


def test_non_whitelisted_budget_error_propagates_without_sharding(monkeypatch, tmp_path):
    """R-1：非名单表（无父窗）撞预算错误 ⇒ 不消费 suggested_shards、不二分，直接上抛。"""
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "export_async": True})          # 白名单为空

    def boom(**kw):
        raise MCPExportBudgetError(
            "budget", error_code="export_exceeds_time_budget",
            hint="shard it",
            suggested_shards=[{"time_start": "2020-01-01", "time_end": "2023-01-01"},
                              {"time_start": "2023-01-01", "time_end": "2026-10-06"}])
    fake.export_fn = boom
    with pytest.raises(MCPExportBudgetError):
        a._fetch_export_passthrough("llm_text_events_enriched", "daily",
                                    "2020-01-01", "2026-10-05")
    assert len(fake.export_calls) == 1, "不得二次尝试"
    assert fake.create_calls == [], "不得消费 suggested_shards / 不得二分"


def test_overlapping_suggested_shards_falls_back_to_bisect_no_duplicates(
        monkeypatch, tmp_path):
    """重叠 suggested_shards（不两两大概率不交）⇒ 回落父窗二分 ⇒ 结果无重复行。"""
    uni = _df([("2024-02-01", "A"), ("2024-05-01", "B"), ("2025-01-01", "C"),
               ("2025-07-01", "D"), ("2026-01-01", "E"), ("2026-09-01", "F")])

    def _sub(ts, te):
        d = ts[:10].replace("-", "")
        e = te[:10].replace("-", "")
        key = uni["date"].astype(str).str.replace("-", "", regex=False)
        return uni[(key >= d) & (key < e)].reset_index(drop=True)

    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "export_async": True,
        "passthrough_export_window_tables": ["cnthesims_events"]})
    calls = {"n": 0}

    def boom(**kw):
        calls["n"] += 1
        raise MCPExportBudgetError(
            "budget", error_code="export_exceeds_time_budget",
            # 故意重叠：第二段起点早于第一段终点
            suggested_shards=[{"time_start": "2024-01-01", "time_end": "2025-06-01"},
                              {"time_start": "2025-01-01", "time_end": "2026-10-06"}])
    fake.export_fn = boom
    fake.subwindow_fn = _sub

    df, meta = a._fetch_export_passthrough("cnthesims_events", "daily",
                                           "2024-01-01", "2026-10-05")
    assert calls["n"] == 1, "父窗仅应失败一次"
    assert meta["subjobs"] >= 2 and meta["subjobs"] <= 16, f"子作业越界: {meta['subjobs']}"
    # 重叠建议窗未被采用
    used = {(c["time_start"], c["time_end"]) for c in fake.create_calls}
    assert ("2024-01-01T00:00:00", "2025-06-01T00:00:00") not in used
    assert ("2025-01-01T00:00:00", "2026-10-06T00:00:00") not in used
    # 二分窗两两不交
    srt = sorted(used)
    for i in range(1, len(srt)):
        assert srt[i][0] >= srt[i - 1][1], f"子窗重叠: {srt[i - 1]} / {srt[i]}"
    # 无重复行，且覆盖全量
    assert len(df) == len(uni), f"行数不符: {len(df)} vs {len(uni)}"
    assert not df.duplicated().any(), "存在重复行"
    assert sorted(df["ts_code"]) == sorted(uni["ts_code"])


def test_aggregation_mismatch_raises_and_never_writes_db(monkeypatch, tmp_path):
    """M-6：聚合断言失败（全列去重后仍不等）⇒ 上抛且不写库。"""
    uni = _df([("2025-01-01", "A"), ("2026-01-01", "B")])
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "export_async": True,
        "passthrough_export_window_tables": ["cnthesims_events"]})
    fake.export_fn = lambda **kw: (_ for _ in ()).throw(
        MCPExportBudgetError("budget", error_code="export_exceeds_time_budget"))
    def _sub(ts, te):
        d = ts[:10].replace("-", "")
        e = te[:10].replace("-", "")
        key = uni["date"].astype(str).str.replace("-", "", regex=False)
        return uni[(key >= d) & (key < e)].reset_index(drop=True)
    fake.subwindow_fn = _sub
    # 申报行数恒比实际多 1 ⇒ 去重也无法对齐
    fake.reported_rows_fn = lambda ts, te, df: len(df) + 1

    writes = []
    monkeypatch.setattr(ma, "locked_connect", lambda *a, **k: writes.append(1))
    with pytest.raises(MCPClientError):
        a._fetch_export_passthrough("cnthesims_events", "daily",
                                    "2024-01-01", "2026-10-05")
    assert writes == [], "聚合断言失败期间不得打开任何写库连接"


def test_zero_row_guard_only_when_window_actually_pushed(monkeypatch, tmp_path):
    """m-2：0 行守卫仅在本次实际携带窗口时触发单次回落。"""
    # 名单内 + 窗口 ⇒ 回落一次（第二次不带窗）
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "export_async": True,
        "passthrough_export_window_tables": ["rsshub_raw"]})
    seq = {"n": 0}

    def empty_then_rows(**kw):
        seq["n"] += 1
        return [] if seq["n"] == 1 else [_artifact(_df([("2026-03-01", "A")]))]
    fake.export_fn = empty_then_rows
    df, meta = a._fetch_export_passthrough("rsshub_raw", "daily",
                                           "2026-01-01", "2026-10-05")
    assert len(fake.export_calls) == 2, "应单次回落"
    assert fake.export_calls[1]["time_start"] is None
    assert fake.export_calls[1]["time_end"] is None
    assert len(df) == 1

    # 名单外（未下推）⇒ 0 行是合法空表，不回落
    a2, fake2 = _adapter(monkeypatch, tmp_path, {"name": "mcp", "export_async": True})
    fake2.export_fn = lambda **kw: []
    df2, meta2 = a2._fetch_export_passthrough("rsshub_raw", "daily",
                                              "2026-01-01", "2026-10-05")
    assert len(fake2.export_calls) == 1, "未下推时不得回落"
    assert len(df2) == 0 and meta2["window"] is False


def test_export_clipping_same_caliber_as_fetch_passthrough(monkeypatch, tmp_path):
    """§3.1.4：export 路径客户端裁剪与 _fetch_passthrough 同口径。"""
    uni = _df([("2025-12-31", "OLD"), ("2026-01-01", "IN1"),
               ("2026-06-30", "IN2"), ("2026-07-01", "NEW")])
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "export_async": True,
        "passthrough_export_window_tables": ["rsshub_raw"]})
    fake.export_fn = lambda **kw: [_artifact(uni)]
    df_e, _ = a._fetch_export_passthrough("rsshub_raw", "daily",
                                          "2026-01-01", "2026-06-30")

    # fetch_page 通道：同口径裁剪
    a2, fake2 = _adapter(monkeypatch, tmp_path, {"name": "mcp"})
    fake2.page_fn = lambda cursor, i: (
        {"rows": uni.to_dict("records"), "next_cursor": None})
    df_p, _ = a2._fetch_passthrough("rsshub_raw", "daily",
                                    "2026-01-01", "2026-06-30", None)
    assert sorted(df_e["ts_code"]) == sorted(df_p["ts_code"]) == ["IN1", "IN2"]


# ===========================================================================
# §5.1-C  _fetch_passthrough 五出口 + B1 + M7
# ===========================================================================
def _timeout_page_fn(*a, **k):
    raise TimeoutError("_call_tool 超过 89.5s 未返回")


def test_timeout_first_page_triggers_export_fallback(monkeypatch, tmp_path):
    """真实异常链（MCPTransportError from TimeoutError）⇒ 降级 export。"""
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "passthrough_export_fallback": True, "export_async": True})
    fake.page_fn = _timeout_page_fn
    fake.export_fn = lambda **kw: [_artifact(_df([("2026-01-01", "A")]))]
    df, meta = a._fetch_passthrough("ws_asfund", "daily", "2026-01-01", "2026-10-05",
                                    ["ALL"])
    assert meta["fetch_mode"] == "export_fallback", f"未降级: {meta}"
    assert len(df) == 1
    assert len(fake.export_calls) == 1
    assert fake.fetch_page_calls[0]["max_attempts"] == 1   # 仅首页探测


def test_auth_and_protocol_errors_not_degraded(monkeypatch, tmp_path):
    """MCPAuthError / MCPProtocolError ⇒ 原样上抛，不降级。"""
    for exc in (MCPAuthError("401"), MCPProtocolError("bad protocol")):
        a, fake = _adapter(monkeypatch, tmp_path, {
            "name": "mcp", "passthrough_export_fallback": True})
        fake.page_fn = lambda *a, **k: (_ for _ in ()).throw(exc)
        with pytest.raises(type(exc)):
            a._fetch_passthrough("ws_asfund", "daily", "2026-01-01", "2026-10-05", None)
        assert fake.export_calls == [], f"{type(exc).__name__} 不应降级"


def test_non_timeout_transport_error_resends_five_attempts(monkeypatch, tmp_path):
    """非超时 MCPTransportError ⇒ 不降级，重发后总尝试 == 5（retry_max=5）。"""
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "passthrough_export_fallback": True})
    fake.page_fn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("ECONNRESET 10053"))
    with pytest.raises(MCPTransportError):
        a._fetch_passthrough("ws_asfund", "daily", "2026-01-01", "2026-10-05", None)
    assert fake.attempts == 5, f"总尝试应为 1+(5-1)=5: {fake.attempts}"
    assert fake.export_calls == [], "非超时错误不得降级"
    assert [c["max_attempts"] for c in fake.fetch_page_calls] == [1, 4], \
        f"重发上限应为 max(1, retry_max-1)=4: {fake.fetch_page_calls}"


def test_retry_max_one_boundary_resend(monkeypatch, tmp_path):
    """m-4：retry_max==1 ⇒ 重发上限夹取为 1（总尝试 2，不出现 0 次尝试）。"""
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "passthrough_export_fallback": True})
    fake.retry_max = 1
    fake.page_fn = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("reset"))
    with pytest.raises(MCPTransportError):
        a._fetch_passthrough("ws_asfund", "daily", "2026-01-01", "2026-10-05", None)
    assert [c["max_attempts"] for c in fake.fetch_page_calls] == [1, 1]
    assert fake.attempts == 2


def test_switch_off_is_line_equivalent_to_before(monkeypatch, tmp_path):
    """B1：开关 false + 首页超时 ⇒ 完全不探测，5 次尝试，不降级。"""
    a, fake = _adapter(monkeypatch, tmp_path, {"name": "mcp"})  # 默认 false
    assert a.passthrough_export_fallback is False
    fake.page_fn = _timeout_page_fn
    with pytest.raises(MCPTransportError):
        a._fetch_passthrough("ws_asfund", "daily", "2026-01-01", "2026-10-05", None)
    assert fake.attempts == 5, f"修复前语义为 5 次尝试: {fake.attempts}"
    assert all(c["max_attempts"] is None for c in fake.fetch_page_calls)
    assert fake.export_calls == [], "开关 false 不得降级"


def test_unwrapped_exception_not_degraded(monkeypatch, tmp_path):
    """m-3：未经 _call_with_retry 包装的异常（cursor 不前进 ValueError）不降级。"""
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "passthrough_export_fallback": True})
    fake.page_fn = lambda cursor, i: ({"rows": [{"a": 1}], "next_cursor": "SAME"}
                                      if i == 0 else {"rows": [], "next_cursor": None})
    with pytest.raises(ValueError):
        a._fetch_passthrough("ws_asfund", "daily", "2026-01-01", "2026-10-05", None)
    assert fake.export_calls == [], "未包装异常不得降级"


def test_first_page_success_keeps_old_behaviour(monkeypatch, tmp_path):
    """S4：首页成功 ⇒ 后续页 max_attempts=None（与修复前逐行一致）。"""
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "passthrough_export_fallback": True})
    pages = [{"rows": [{"date": "2026-01-01"}], "next_cursor": "c1"},
             {"rows": [{"date": "2026-01-02"}], "next_cursor": None}]
    fake.page_fn = lambda cursor, i: pages[0] if cursor in ("", None) else pages[1]
    df, meta = a._fetch_passthrough("ws_asfund", "daily", "2026-01-01", "2026-10-05", None)
    assert meta["fetch_mode"] == "fetch_page"
    assert len(df) == 2
    assert [c["max_attempts"] for c in fake.fetch_page_calls] == [1, None], \
        f"后续页必须为 None: {fake.fetch_page_calls}"
    assert fake.export_calls == []


def test_four_other_call_sites_unchanged(monkeypatch, tmp_path):
    """M7：:652/:2137/:2158/:2179 四个调用点不传 max_attempts。"""
    a, fake = _adapter(monkeypatch, tmp_path, {
        "name": "mcp", "passthrough_export_fallback": True})
    fake.page_fn = lambda cursor, i: {"rows": [{"ts_code": "600000.SH"}],
                                      "next_cursor": None}
    a._fetch_small_table("stock_basic", "daily", "2026-01-01", "2026-10-05", None)
    a.get_all_stock_codes()
    a.get_etf_codes()
    a.get_index_codes()
    assert fake.fetch_page_calls, "四个调用点应各发起分页"
    assert all(c["max_attempts"] is None for c in fake.fetch_page_calls), \
        f"无关调用点被外溢: {fake.fetch_page_calls}"


# ===========================================================================
# §5.1-D  configure_execution：仅同步 call_timeout（M3/M4/S7）
# ===========================================================================
def test_configure_execution_syncs_only_call_timeout_no_client(monkeypatch, tmp_path):
    """client 未建 ⇒ 记桥接值，构造时传入；<=0 回退 90。"""
    a, _ = _adapter(monkeypatch, tmp_path, {"name": "mcp"})
    a._client = None
    a.configure_execution({"call_timeout": 30})
    assert a._client_call_timeout == 30.0

    a.configure_execution({"call_timeout": 0})
    assert a._client_call_timeout == 90.0, "<=0 必须回退 90（禁止 join(0)）"
    a.configure_execution({"call_timeout": -5})
    assert a._client_call_timeout == 90.0

    captured = {}

    class _FakeMCPClient:
        def __init__(self, **kw):
            captured.update(kw)
            self.call_timeout = kw.get("call_timeout", 90.0)
            self._lock = threading.Lock()

        def handshake_bounded(self, *a, **k):
            return None

    monkeypatch.setattr(ma, "MCPClient", _FakeMCPClient)
    monkeypatch.setattr(ma, "load_mcp_api_key", lambda *a, **k: "k")
    a.configure_execution({"call_timeout": 25})
    _ = a.client
    assert captured.get("call_timeout") == 25.0
    # M3/M4：绝不同步其余三项
    for k in ("retry_max", "retry_backoff_sec", "backoff_sec", "rate_per_min"):
        assert k not in captured, f"{k} 被同步（M3/M4 禁止）"


def test_configure_execution_updates_existing_client_under_lock(monkeypatch, tmp_path):
    """client 已建 ⇒ 在 _lock 内更新 call_timeout，其余字段零改动。"""
    a, fake = _adapter(monkeypatch, tmp_path, {"name": "mcp"})
    a.configure_execution({"call_timeout": 45})
    assert fake.call_timeout == 45.0
    assert fake.retry_max == 5
    assert fake.backoff_sec == (30, 60, 120, 240, 480)
    assert fake.rate_per_min == 200


def test_configure_execution_keeps_base_semantics(monkeypatch, tmp_path):
    """基类语义保留：retry/backoff 仍按任务配置写入 adapter（不影响 client）。"""
    a, fake = _adapter(monkeypatch, tmp_path, {"name": "mcp"})
    a.configure_execution({"retry": {"max": 3, "backoff_sec": [1, 2, 3]},
                           "call_timeout": 12})
    assert a.retry_max == 3 and tuple(a.retry_backoff_sec) == (1, 2, 3)
    assert fake.retry_max == 5 and fake.backoff_sec == (30, 60, 120, 240, 480)


def test_default_whitelist_and_switch():
    """默认：白名单空（fail-safe 不下推）+ 降级开关 false。"""
    a = ma.MCPAdapter({"name": "mcp"})
    assert a.passthrough_export_window_tables == frozenset()
    assert a.passthrough_export_fallback is False
    assert ma.MCPAdapter._is_timeout_family(MCPTransportError("x")) is False


def test_is_timeout_family_uses_cause_chain():
    """异常链 ≤3 层内出现 TimeoutError 即判为超时族。"""
    try:
        try:
            raise TimeoutError("deep")
        except TimeoutError as te:
            raise MCPToolError("wrapped") from te
    except MCPToolError as mte:
        # 模拟 client 终态：MCPTransportError(…) from <上层异常>
        exc = MCPTransportError("MCP 重试 1 次仍失败")
        exc.__cause__ = mte
        exc.__context__ = mte
    assert ma.MCPAdapter._is_timeout_family(exc) is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
