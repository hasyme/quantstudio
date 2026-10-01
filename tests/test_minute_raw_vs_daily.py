"""tests/test_minute_raw_vs_daily.py — 缺陷1 补漏检（MinuteRawVsDaily）单元测试。

验证：A1 新增的 minutes.close 对日线 close 独立校验，能检出同向偏移
（600649 @ 2026-06-25 落库 close=57.13 vs 真值 3.50，+1532%，原 A1 漏检）。

mock conn.execute（按 SQL 关键词分发）+ mock _read_factor_table / _resolve_aux_path，
不连真实库。
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quantstudio.pipeline.quality_audit import DataQualityAuditor, QualityReport

CST = timezone(timedelta(hours=8))


def _ms(y, m, d, hh=0, mm=0):
    return int(datetime(y, m, d, hh, mm, tzinfo=CST).timestamp() * 1000)


class _Result:
    """模拟 DuckDB execute(...) 返回的结果对象（有 .fetchall()）。"""
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class FakeConn:
    def __init__(self, bars, daily_rows, codes):
        self._bars = bars
        self._daily = daily_rows
        self._codes = codes

    def execute(self, sql, *args):
        sql = str(sql)
        if "SHOW TABLES" in sql:
            return _Result([("stock_minutes",), ("stock_daily",), ("stock_dividend",)])
        if "SELECT DISTINCT code FROM stock_dividend" in sql:
            return _Result([(c,) for c in self._codes])
        if "SELECT code, time, close, close_front FROM stock_minutes" in sql:
            return _Result(self._bars)
        if "SELECT code, time, close FROM stock_daily" in sql:
            return _Result(self._daily)
        raise AssertionError(f"unexpected SQL: {sql[:80]}")


def _make_auditor():
    aud = DataQualityAuditor("unused.duckdb", schemas={})
    aud.qfq_aux_override = Path("/nonexistent/fake_aux.db")
    return aud


def test_minute_raw_vs_daily_detects_600649(monkeypatch):
    aud = _make_auditor()
    # 因子：600649 除权前 1.0 → 除权后 16.2775（大除权，ratio=16.2775）
    ex_ms = _ms(2026, 9, 21)
    factors = [("600649", _ms(2026, 1, 1), 1.0), ("600649", ex_ms, 16.2775)]
    monkeypatch.setattr(aud, "_read_factor_table",
                        lambda aux_path, ft, codes: factors)
    monkeypatch.setattr(aud, "_resolve_aux_path",
                        lambda conn: Path("/nonexistent/fake_aux.db"))

    # bar：600649 @ 2026-06-25 15:00（close 被放大 16.27 倍 = 同向偏移；front 也错）
    bar_ms = _ms(2026, 6, 25, 15, 0)
    bars = [("600649", bar_ms, 57.13, 3.5098)]
    # 日线：600649 @ 2026-06-25 真实 raw close = 3.50
    daily_ms = _ms(2026, 6, 25)
    daily_rows = [("600649", daily_ms, 3.50)]

    conn = FakeConn(bars, daily_rows, codes=["600649"])
    report = QualityReport()
    aud._audit_minute_anchor_drift(conn, report, {"stock_minutes", "stock_daily",
                                                   "stock_dividend", "etf_minutes",
                                                   "etf_daily", "etf_dividend"})

    checks = {i.check for i in report.issues}
    assert "MinuteRawVsDaily" in checks, f"补漏检未触发，issues={[(i.check, i.detail) for i in report.issues]}"
    # 600649 应作为 FAIL 出现在 MinuteRawVsDaily 明细里
    mv = [i for i in report.issues if i.check == "MinuteRawVsDaily" and i.severity == "error"]
    assert any("600649" in i.detail for i in mv), f"600649 未被补漏检标记: {[i.detail for i in mv]}"


def test_minute_raw_vs_daily_clean_when_match(monkeypatch):
    """close 与日线一致时，补漏检不误报。"""
    aud = _make_auditor()
    ex_ms = _ms(2026, 9, 21)
    factors = [("000012", _ms(2026, 1, 1), 30.3449), ("000012", ex_ms, 30.5076)]
    monkeypatch.setattr(aud, "_read_factor_table",
                        lambda aux_path, ft, codes: factors)
    monkeypatch.setattr(aud, "_resolve_aux_path",
                        lambda conn: Path("/nonexistent/fake_aux.db"))
    bar_ms = _ms(2026, 6, 25, 15, 0)
    # 正常案例：close 正确（4.11），front = 4.11 × 30.3449/30.5076 = 4.088
    bars = [("000012", bar_ms, 4.11, 4.088)]
    daily_ms = _ms(2026, 6, 25)
    daily_rows = [("000012", daily_ms, 4.11)]

    conn = FakeConn(bars, daily_rows, codes=["000012"])
    report = QualityReport()
    aud._audit_minute_anchor_drift(conn, report, {"stock_minutes", "stock_daily",
                                                   "stock_dividend", "etf_minutes",
                                                   "etf_daily", "etf_dividend"})
    # close==daily(4.11==4.11) → raw_dev=0 → 不误报 MinuteRawVsDaily FAIL
    mv_fail = [i for i in report.issues
               if i.check == "MinuteRawVsDaily" and i.severity == "error"]
    assert not mv_fail, f"正常案例被误报: {[i.detail for i in mv_fail]}"
