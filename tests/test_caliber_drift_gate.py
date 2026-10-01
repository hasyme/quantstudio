"""tests/test_caliber_drift_gate.py — 方案 A′ §5.3 云端口径漂移门禁（CaliberDrift）。

判别模型：除权日 D 处令 k = adj_D/adj_{D-1}（外部因子库，独立于被测输出），
实测本表 close 跨除权日比值 r：
  · r ≈ 1    → 输出 qfq（未还原）
  · r ≈ 1/k  → 输出 raw（A′ 预期态，正确）
  · r ≈ 1/k² → 双重复权（云端已 raw 而管线又还原一次）
按表级多数票定口径；stock_minutes 翻转为阻断（error），其余表告警（warning）。

本文件同时固化了两个真实库实测得出的关键约定（早期版本因此二者出过实质缺陷）：
  1. 因子步进**恰在除权日**（600812 ex=2026-06-11 step@2026-06-11）；
  2. bar 时刻约定按表不同——分钟表 15:00 CST 收盘 bar，日线表戳 00:00 CST。

mock conn.execute（按 SQL 关键词分发，并模拟 IN/时间窗/时刻过滤）+ mock
_read_factor_table / _resolve_aux_path，不连真实库。
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quantstudio.pipeline.quality_audit import DataQualityAuditor, QualityReport

CST = timezone(timedelta(hours=8))

EX_DAY = (2026, 9, 21)          # 除权日
FACTOR_PRE = 10.0               # 除权前因子
FACTOR_POST = 12.0              # 除权后因子 → k=1.2，gap=1-1/1.2≈16.7%（可分辨）


def _ms(y, m, d, hh=0, mm=0):
    return int(datetime(y, m, d, hh, mm, tzinfo=CST).timestamp() * 1000)


class _Result:
    """模拟 DuckDB execute(...) 结果对象。"""
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class FakeConn:
    """按 SQL 关键词分发：除权表 / 行情表；行情查询模拟 IN + 时间窗 + 时刻过滤。"""

    def __init__(self, div_rows, bars_by_table):
        self._div = div_rows        # {"stock_dividend": [(code, ex_ms), ...], ...}
        self._bars = bars_by_table  # {"stock_minutes": [(code, time, close), ...]}

    def execute(self, sql, *args):
        sql = str(sql)
        for div_tbl, rows in self._div.items():
            if f"FROM {div_tbl}" in sql:
                return _Result(rows)
        for tbl, rows in self._bars.items():
            if f"FROM {tbl} " in sql or f"FROM {tbl}\n" in sql:
                # 批量契约：execute(sql, [code1..codeN, win_lo, win_hi, tod_lo, tod_hi])
                params = args[0] if args and isinstance(args[0], (list, tuple)) else args
                wanted = set(params[:-4]) if len(params) > 4 else set(params)
                win_lo, win_hi, tod_lo, tod_hi = params[-4:]
                return _Result([
                    r for r in rows
                    if r[0] in wanted and win_lo <= r[1] < win_hi
                    and tod_lo <= (r[1] % 86400000) <= tod_hi])
        raise AssertionError(f"unexpected SQL: {sql[:100]}")


def _make_auditor(monkeypatch, factor_tbl="adj_factor", pre=FACTOR_PRE, post=FACTOR_POST):
    """因子序列：日频，步进**恰在除权日**（与真实库语义一致——见 600812/600602 实测）。

    早期门禁误用 ex-2d 作分割点，会吞掉除权日当天的步进（实测 2070/2441 code 误判
    「无步进」）；此 fixture 以「步进落在 EX_DAY」构造，可捕获该类回归。
    """
    aud = DataQualityAuditor("unused.duckdb", schemas={})
    aud.qfq_aux_override = Path("/nonexistent/fake_aux.db")
    ex_ms = _ms(*EX_DAY)
    monkeypatch.setattr(aud, "_resolve_aux_path",
                        lambda conn: Path("/nonexistent/fake_aux.db"))
    monkeypatch.setattr(
        aud, "_read_factor_table",
        lambda aux_path, ft, codes: [
            (c, _ms(2026, 8, 1) + d * 86400_000 + 8 * 3600_000,
             pre if _ms(2026, 8, 1) + d * 86400_000 < ex_ms else post)
            for c in codes for d in range(60)])
    return aud


def _bars_for(code, r_target, tbl="stock_minutes", factor_post=FACTOR_POST,
              factor_pre=FACTOR_PRE):
    """按目标比值 r_target 造除权日前后各一根收盘 bar。

    前一日收盘 = 10.0；除权日收盘 = 10.0 × r_target。
    bar 时刻按表约定：分钟表 15:00 CST（07:00 UTC）；日线表 00:00 CST。
    """
    hh = 15 if tbl.endswith("_minutes") else 0
    prev = _ms(2026, 9, 18, hh)      # 除权日前一交易日
    cur = _ms(2026, 9, 21, hh)       # 除权日
    return [(code, prev, 10.0), (code, cur, 10.0 * r_target)]


def _codes(n=30, prefix="0000"):
    return [f"{prefix}{i:02d}" for i in range(n)]


def _run(aud, tbl, div_tbl, factor_tbl, div_rows, bars):
    conn = FakeConn({div_tbl: div_rows}, {tbl: bars})
    report = QualityReport()
    aud._audit_caliber_drift(conn, report, {tbl, div_tbl})
    return report


# --------------------------------------------------------------------------
# 1. raw 正确态（A′ 预期）：r = 1/k → 无 issue
# --------------------------------------------------------------------------
def test_raw_correct_no_issue(monkeypatch):
    aud = _make_auditor(monkeypatch)
    codes = _codes(30)
    k = FACTOR_POST / FACTOR_PRE
    bars = [b for c in codes for b in _bars_for(c, 1.0 / k)]
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, "stock_minutes", "stock_dividend", "adj_factor", div, bars)
    assert not [i for i in report.issues if i.check == "CaliberDrift"], \
        f"raw 正确态误报: {[(i.table, i.detail) for i in report.issues]}"


# --------------------------------------------------------------------------
# 2. stock_minutes 翻转 qfq（r ≈ 1）→ 阻断 error
# --------------------------------------------------------------------------
def test_stock_minutes_qfq_flip_is_blocking(monkeypatch):
    """开关开启时（数据修复验收后）：stock_minutes 翻转 qfq → 阻断 error。"""
    aud = _make_auditor(monkeypatch)
    monkeypatch.setattr(DataQualityAuditor, "_CALIBER_BLOCK_STOCK_MINUTES", True)
    codes = _codes(30)
    bars = [b for c in codes for b in _bars_for(c, 1.0)]     # r ≈ 1 → qfq
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, "stock_minutes", "stock_dividend", "adj_factor", div, bars)
    cd = [i for i in report.issues if i.check == "CaliberDrift"]
    assert cd, "stock_minutes 口径翻转未被检出"
    assert cd[0].severity == "error", f"应阻断却为 {cd[0].severity}"
    assert cd[0].table == "stock_minutes"
    assert "缺陷1" in cd[0].detail


def test_stock_minutes_flip_warn_only_when_switch_off(monkeypatch):
    """开关默认关（修复前）：同一翻转只告警不阻断——避免污染期常亮误报。

    取证依据：本地 stock_minutes = 云端值逐行直写，缺陷1 污染与口径漂移不可分离
    （median(m/d close)≈0.999 + 膨胀长尾）。见 caliber-drift-gate-forensics-20260928.md §4。
    """
    assert DataQualityAuditor._CALIBER_BLOCK_STOCK_MINUTES is False, "默认应为不阻断"
    aud = _make_auditor(monkeypatch)
    codes = _codes(30)
    bars = [b for c in codes for b in _bars_for(c, 1.0)]
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, "stock_minutes", "stock_dividend", "adj_factor", div, bars)
    cd = [i for i in report.issues if i.check == "CaliberDrift"]
    assert cd and cd[0].severity == "warning", \
        f"修复前应为告警，实得 {[(i.check, i.severity) for i in report.issues]}"
    assert "不可区分" in cd[0].detail


# --------------------------------------------------------------------------
# 3. etf_minutes / 日线表翻转 → 仅告警（不阻断）
# --------------------------------------------------------------------------
@pytest.mark.parametrize("tbl,div_tbl,factor_tbl", [
    ("etf_minutes", "etf_dividend", "fund_adj"),
    ("stock_daily", "stock_dividend", "adj_factor"),
    ("etf_daily", "etf_dividend", "fund_adj"),
])
def test_non_blocking_tables_warn_only(monkeypatch, tbl, div_tbl, factor_tbl):
    aud = _make_auditor(monkeypatch, factor_tbl=factor_tbl)
    codes = _codes(30)
    bars = [b for c in codes for b in _bars_for(c, 1.0, tbl)]
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, tbl, div_tbl, factor_tbl, div, bars)
    cd = [i for i in report.issues if i.check == "CaliberDrift"]
    assert cd, f"{tbl} 口径翻转未被检出"
    assert cd[0].severity == "warning", f"{tbl} 不应阻断，却为 {cd[0].severity}"


# --------------------------------------------------------------------------
# 4. 双重复权（r ≈ 1/k²）→ 检出
# --------------------------------------------------------------------------
def test_double_adjustment_detected(monkeypatch):
    aud = _make_auditor(monkeypatch)
    codes = _codes(30)
    k = FACTOR_POST / FACTOR_PRE
    bars = [b for c in codes for b in _bars_for(c, 1.0 / (k * k), tbl="stock_daily")]
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, "stock_daily", "stock_dividend", "adj_factor", div, bars)
    cd = [i for i in report.issues if i.check == "CaliberDrift"]
    assert cd and cd[0].severity == "warning"
    assert "double" in cd[0].detail


# --------------------------------------------------------------------------
# 5. 分红过小（gap < 2%）→ 探针弃用 → 样本不足告警，不误判口径
# --------------------------------------------------------------------------
def test_small_dividend_probe_discarded(monkeypatch):
    aud = _make_auditor(monkeypatch, post=FACTOR_PRE * 1.001)   # gap ≈ 0.1%
    codes = _codes(30)
    bars = [b for c in codes for b in _bars_for(c, 1.0)]
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, "stock_minutes", "stock_dividend", "adj_factor", div, bars)
    checks = {i.check for i in report.issues}
    assert "CaliberDrift" not in checks, "小分红探针不应定口径"
    ins = [i for i in report.issues if i.check == "CaliberDriftInsufficientSample"]
    assert ins and ins[0].severity == "warning"


# --------------------------------------------------------------------------
# 6. 真实涨跌幅噪声（落在两假设容差外）→ 不计票
# --------------------------------------------------------------------------
def test_noise_probe_not_voted(monkeypatch):
    aud = _make_auditor(monkeypatch)
    codes = _codes(30)
    k = FACTOR_POST / FACTOR_PRE               # gap ≈ 16.7%，tol ≈ 8.3%
    bars = [b for c in codes for b in _bars_for(c, 1.0 / k * 1.5)]  # 偏离 >tol
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, "stock_minutes", "stock_dividend", "adj_factor", div, bars)
    checks = {i.check for i in report.issues}
    assert "CaliberDrift" not in checks, "噪声探针不应计票定口径"


# --------------------------------------------------------------------------
# 7. 对半票（口径未决）→ CaliberDriftInconclusive 告警，不臆断
# --------------------------------------------------------------------------
def test_inconclusive_split_vote(monkeypatch):
    aud = _make_auditor(monkeypatch)
    k = FACTOR_POST / FACTOR_PRE
    qfq_codes, raw_codes = _codes(15, "1000"), _codes(15, "2000")
    bars = [b for c in qfq_codes for b in _bars_for(c, 1.0)]
    bars += [b for c in raw_codes for b in _bars_for(c, 1.0 / k)]
    div = [(c, _ms(*EX_DAY)) for c in qfq_codes + raw_codes]
    report = _run(aud, "stock_minutes", "stock_dividend", "adj_factor", div, bars)
    checks = {i.check for i in report.issues}
    assert "CaliberDriftInconclusive" in checks, \
        f"对半票应判口径未决，实得 {[(i.check, i.severity) for i in report.issues]}"
    assert "CaliberDrift" not in checks, "对半票不应臆断为口径漂移"


# --------------------------------------------------------------------------
# 8. 因子库不可用 → 跳过并告警（不抛异常）
# --------------------------------------------------------------------------
def test_aux_unavailable_skips(monkeypatch):
    aud = DataQualityAuditor("unused.duckdb", schemas={})
    monkeypatch.setattr(aud, "_resolve_aux_path", lambda conn: None)
    conn = FakeConn({"stock_dividend": [("000001", _ms(*EX_DAY))]},
                    {"stock_minutes": _bars_for("000001", 1.0)})
    report = QualityReport()
    aud._audit_caliber_drift(conn, report, {"stock_minutes", "stock_dividend"})
    checks = {i.check for i in report.issues}
    assert "CaliberDriftAuxUnavailable" in checks
    assert "CaliberDrift" not in checks


# --------------------------------------------------------------------------
# 9. 表缺失 → 静默跳过
# --------------------------------------------------------------------------
def test_missing_table_noop(monkeypatch):
    aud = _make_auditor(monkeypatch)
    conn = FakeConn({}, {})
    report = QualityReport()
    aud._audit_caliber_drift(conn, report, {"stock_dividend"})
    assert not report.issues


# --------------------------------------------------------------------------
# 10. 常量与设计一致
# --------------------------------------------------------------------------
def test_constants_match_design():
    assert DataQualityAuditor._CALIBER_MIN_DIV_GAP == 0.02
    assert DataQualityAuditor._CALIBER_DECISIVE_MARGIN == 0.5
    assert DataQualityAuditor._CALIBER_MIN_PROBES == 20
    assert DataQualityAuditor._CALIBER_MAJORITY == 0.5


# --------------------------------------------------------------------------
# 11. 日线表 bar 时刻约定（00:00 CST）——时刻过滤误用分钟收盘时刻的真实缺陷回归
# --------------------------------------------------------------------------
def test_daily_table_bar_timestamp_convention(monkeypatch):
    """日线 bar 戳在 00:00 CST；门禁须按表选用时刻过滤。

    早期版本对四表统一用 14:59-15:01 CST 过滤 → 三张日线/ETF 表 0 探针（静默失效）。
    """
    aud = _make_auditor(monkeypatch)
    codes = _codes(30)
    bars = [b for c in codes for b in _bars_for(c, 1.0, tbl="stock_daily")]
    div = [(c, _ms(*EX_DAY)) for c in codes]
    report = _run(aud, "stock_daily", "stock_dividend", "adj_factor", div, bars)
    cd = [i for i in report.issues if i.check == "CaliberDrift"]
    assert cd, "日线表 0 探针（时刻过滤误用分钟收盘时刻）未被捕获"
    assert not [i for i in report.issues
                if i.check == "CaliberDriftInsufficientSample"], \
        "日线表 bar 时刻过滤错误 → 误报样本不足"


# --------------------------------------------------------------------------
# 12. 规模回归：多 code × 多因子不得退化为 O(n·m)（大表可跑性守卫）
# --------------------------------------------------------------------------
def test_scales_with_many_codes_and_factors(monkeypatch):
    import time
    n_codes, n_factors = 800, 60
    aud = DataQualityAuditor("unused.duckdb", schemas={})
    aud.qfq_aux_override = Path("/nonexistent/fake_aux.db")
    ex_ms = _ms(*EX_DAY)
    monkeypatch.setattr(aud, "_resolve_aux_path",
                        lambda conn: Path("/nonexistent/fake_aux.db"))
    codes = [f"{i:06d}" for i in range(n_codes)]
    factors = []
    for c in codes:
        for j in range(n_factors):                    # 全历史因子（含步进）
            factors.append((c, _ms(2025, 1, 1) + j * 5 * 86400_000,
                            FACTOR_PRE + (FACTOR_POST - FACTOR_PRE) * (j >= 35)))
    monkeypatch.setattr(aud, "_read_factor_table", lambda ap, ft, cs: factors)
    k = FACTOR_POST / FACTOR_PRE
    bars = [b for c in codes for b in _bars_for(c, 1.0 / k)]
    div = [(c, ex_ms) for c in codes]
    t0 = time.perf_counter()
    report = _run(aud, "stock_minutes", "stock_dividend", "adj_factor", div, bars)
    elapsed = time.perf_counter() - t0
    assert not [i for i in report.issues if i.check == "CaliberDrift"]
    assert elapsed < 5.0, f"规模回归：{n_codes}code×{n_factors}因子耗时 {elapsed:.2f}s"
