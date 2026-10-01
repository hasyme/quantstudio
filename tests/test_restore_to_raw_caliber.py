"""tests/test_restore_to_raw_caliber.py — 缺陷1 方案 A′（表级复权口径）单元测试。

**口径结论（2026-09-30 金标准复核后定稿）**：
云端四表口径（`amount/volume` 反推真实成交价，口径无关方法确证）：
  · `stock_daily` / `etf_daily` / `etf_minutes` 云端 **qfq** → **还原** ×adj_latest/adj_i
  · `stock_minutes` 云端 **raw** → **不还原**

> ⚠ 过程记录：2026-09-30 曾据「云端 stock_daily/etf_daily = raw」的复核
> 把集合收缩为 `{etf_minutes}` 并反转本文件用例，**该结论已被推翻**
> （其"独立基准"用的是云端 stock_minutes，而该表同为 qfq → 同型循环论证）。
> 金标准复核确证两日线表为 qfq（大幅度子集 err_qfq 比 err_raw 小 32~75 倍），
> 集合与用例均已回退至本版。
> 详见 `docs/evidence/caliber-recheck-gold-standard-20260930.md`。

覆盖：
1. 表级口径映射（`_QFQ_CALIBER_TABLES` 成员与非成员）
2. 日线表还原行为（qfq × ratio = raw）
3. 分钟表不还原行为（raw 保持）
4. meta 的 caliber 字段
5. 未知名表不还原
6. `_RESTORE_TABLES` 与 `_QFQ_CALIBER_TABLES` 的关系
7. **A4（R-3）**：非成员表因子缺失仍 fail-fast（显式锁定，防误优化）

不连真实库：构造 df + 传 adj_latest_map。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quantstudio.pipeline.sources.mcp_adapter import (
    MCPAdapter, _QFQ_CALIBER_TABLES, _RESTORE_TABLES,
)


def _adapter():
    return MCPAdapter({"main_db": "unused", "enable_qfq_restore": True})


def _df(code, close, adj_factor, trade_date=None, trade_time=None):
    d = {"ts_code": [code], "open": [close], "high": [close], "low": [close],
         "close": [close], "pre_close": [close], "adj_factor": [adj_factor],
         "is_qfq": [True]}
    if trade_date is not None:
        d["trade_date"] = [trade_date]
    if trade_time is not None:
        d["trade_time"] = [pd.Timestamp(trade_time)]
    return pd.DataFrame(d)


# ---------------- 口径映射 ----------------

def test_caliber_set_membership():
    """A′ 口径映射（金标准复核后定稿）：日线两表 + etf_minutes 需还原；stock_minutes 不还原。"""
    assert _QFQ_CALIBER_TABLES == {"stock_daily", "etf_daily", "etf_minutes"}
    assert "stock_minutes" not in _QFQ_CALIBER_TABLES
    # 口径表 ⊆ 还原表（不应引入未知表）
    assert _QFQ_CALIBER_TABLES <= _RESTORE_TABLES


# ---------------- 日线表（qfq → 还原） ----------------

def test_stock_daily_qfq_is_restored():
    """stock_daily 云端 qfq（金标准复核确证）→ 还原 raw。

    实证（000039 @ 2018-06-15）：云端 close=6.642（qfq），
    amount/volume×10 = 14.84（真实成交价），ratio=2.234 → 还原后 ≈ raw ✓
    """
    a = _adapter()
    # ratio = adj_latest/adj_i = 1.4478/1.3820 = 1.0476
    df = _df("510050.SH", 2.1820, 1.3820, trade_date="2023-12-19")
    out, meta = a._restore_to_raw(df, "stock_daily", "daily",
                                  adj_latest_map={"510050": 1.4478})
    assert out["close"].iloc[0] == pytest.approx(2.2860, abs=1e-3)
    assert meta["caliber"] == "qfq"
    assert meta["is_qfq_restored"] is True


def test_etf_daily_qfq_is_restored():
    """etf_daily 云端 qfq（金标准复核：大幅度子集 err_qfq 比 err_raw 小 75 倍）→ 还原。"""
    a = _adapter()
    df = _df("510050.SH", 2.1820, 1.3820, trade_date="2023-12-19")
    out, meta = a._restore_to_raw(df, "etf_daily", "daily",
                                  adj_latest_map={"510050": 1.4478})
    assert out["close"].iloc[0] == pytest.approx(2.2860, abs=1e-3)
    assert meta["caliber"] == "qfq"


def test_etf_minutes_qfq_is_restored():
    """etf_minutes 云端 qfq → 还原（N1 独立确证 + P5 分层复核）。"""
    a = _adapter()
    df = _df("159119.SZ", 0.9710, 1.0174, trade_time="2026-08-14 15:00:00")
    out, meta = a._restore_to_raw(df, "etf_minutes", "1min",
                                  adj_latest_map={"159119": 1.0205})
    # 0.9710 × (1.0205/1.0174) = 0.9710 × 1.00305 ≈ 0.9740
    assert out["close"].iloc[0] == pytest.approx(0.9740, abs=1e-3)
    assert meta["caliber"] == "qfq"
    assert meta["is_qfq_restored"] is True


def test_etf_minutes_meta_reports_qfq():
    """etf_minutes 报 caliber=qfq（这是它被正确还原的证据，不得期望 raw）。"""
    a = _adapter()
    df = _df("159119.SZ", 0.9710, 1.0174, trade_time="2026-08-14 15:00:00")
    _, meta = a._restore_to_raw(df, "etf_minutes", "1min",
                                adj_latest_map={"159119": 1.0205})
    assert meta["caliber"] == "qfq"
    assert meta["restored_rows"] == 1


def test_stock_daily_all_price_cols_restored():
    """日线表还原时所有价格列同乘 ratio（不止 close）。"""
    a = _adapter()
    df = pd.DataFrame([{"ts_code": "600000.SH", "open": 10.0, "high": 10.5,
                        "low": 9.8, "close": 10.2, "pre_close": 10.1,
                        "adj_factor": 1.3820, "is_qfq": True,
                        "trade_date": "2023-12-19"}])
    out, _ = a._restore_to_raw(df, "stock_daily", "daily",
                               adj_latest_map={"600000": 1.4478})
    r = 1.4478 / 1.3820   # ≈1.0476
    for c, v in (("open", 10.0), ("high", 10.5), ("low", 9.8),
                 ("close", 10.2), ("pre_close", 10.1)):
        assert out[c].iloc[0] == pytest.approx(v * r, abs=1e-6), c


# ---------------- stock_minutes（raw → 不还原） ----------------

def test_stock_minutes_raw_not_restored():
    """stock_minutes 云端 raw（107/107 全 raw）→ 不还原，原值保持。"""
    a = _adapter()
    # ratio = 1.0476，若被误乘则 4.11 → 4.31（缺陷1 的表现）
    df = _df("000012.SZ", 4.1100, 30.3449, trade_time="2026-06-15 15:00:00")
    out, meta = a._restore_to_raw(df, "stock_minutes", "1min",
                                  adj_latest_map={"000012": 30.5076})
    assert out["close"].iloc[0] == pytest.approx(4.1100)   # 不被放大
    assert meta["caliber"] == "raw"
    assert meta["is_qfq_restored"] is False


def test_stock_minutes_big_ratio_still_raw():
    """大 ratio（2 倍除权）下 stock_minutes 仍不还原。"""
    a = _adapter()
    df = _df("600649.SH", 3.5100, 16.0936, trade_time="2026-06-25 15:00:00")
    out, meta = a._restore_to_raw(df, "stock_minutes", "1min",
                                  adj_latest_map={"600649": 16.2775})
    # 若误还原：3.51 × (16.2775/16.0936) = 3.55；实测应保持 3.51
    assert out["close"].iloc[0] == pytest.approx(3.5100)
    assert meta["caliber"] == "raw"


def test_stock_minutes_all_price_cols_untouched():
    """不还原时所有价格列保持原值。"""
    a = _adapter()
    df = pd.DataFrame([{"ts_code": "000012.SZ", "open": 4.00, "high": 4.20,
                        "low": 3.90, "close": 4.11, "pre_close": 4.05,
                        "adj_factor": 30.3449, "is_qfq": True,
                        "trade_time": pd.Timestamp("2026-06-15 15:00:00")}])
    out, _ = a._restore_to_raw(df, "stock_minutes", "1min",
                               adj_latest_map={"000012": 30.5076})
    for c, v in (("open", 4.00), ("high", 4.20), ("low", 3.90),
                 ("close", 4.11), ("pre_close", 4.05)):
        assert out[c].iloc[0] == pytest.approx(v), c


# ---------------- 边界 ----------------

def test_ratio_eq1_equivalent_under_both_calibers():
    """ratio==1 时，两种口径行为等价（还原×1 == 不还原）。"""
    a = _adapter()
    df1 = _df("000012.SZ", 3.81, 30.5076, trade_date="2026-07-23")
    out1, _ = a._restore_to_raw(df1, "etf_minutes", "1min",
                                adj_latest_map={"000012": 30.5076})
    df2 = _df("000012.SZ", 3.81, 30.5076, trade_time="2026-07-23 15:00:00")
    out2, _ = a._restore_to_raw(df2, "stock_minutes", "1min",
                                adj_latest_map={"000012": 30.5076})
    assert out1["close"].iloc[0] == pytest.approx(3.81)
    assert out2["close"].iloc[0] == pytest.approx(3.81)


def test_unknown_table_not_restored():
    """不在 _RESTORE_TABLES 的表直接放行（skip）。"""
    a = _adapter()
    df = _df("000012.SZ", 4.11, 30.3449, trade_date="2026-06-15")
    out, meta = a._restore_to_raw(df, "some_other_table", "daily",
                                  adj_latest_map={"000012": 30.5076})
    assert out["close"].iloc[0] == pytest.approx(4.11)
    assert meta["is_qfq_restored"] is False
    assert "restore_skip_reason" in meta


# ---------------- A4（R-3）：非成员表因子缺失仍 fail-fast ----------------
# 注：口径集合已回退（三表还原），故"非成员表"指 stock_minutes（云端 raw，不还原）。

def test_a4_non_member_missing_adj_factor_col_still_fails():
    """A4（R-3 显式锁定）：**非成员表缺 adj_factor 列仍 fail-fast**。

    「不还原的表仍要求因子完整」是既有语义——
    防止未来被误当作「不还原就不需要因子」而优化掉。
    """
    a = _adapter()
    df = pd.DataFrame([{"ts_code": "000012.SZ", "open": 4.00, "high": 4.20,
                        "low": 3.90, "close": 4.11, "pre_close": 4.05,
                        "is_qfq": True, "trade_time": pd.Timestamp("2026-06-15 15:00:00")}])
    with pytest.raises(ValueError):
        a._restore_to_raw(df, "stock_minutes", "1min",
                          adj_latest_map={"000012": 30.5076})


def test_a4_non_member_missing_factor_value_still_fails():
    """A4（R-3）：非成员表**因子值无效**（无 adj_latest）仍 fail-fast。"""
    a = _adapter()
    df = _df("000012.SZ", 4.11, 30.3449, trade_time="2026-06-15 15:00:00")
    with pytest.raises(ValueError):
        # adj_latest_map 不含该 code → valid=False → fail-fast
        a._restore_to_raw(df, "stock_minutes", "1min", adj_latest_map={})
