"""V3 单元契约：Context.capital_base（capital_base 契约补全件，2026-10-01）。

红态依据：修复前 Context 无 capital_base 属性 → 策略侧 safe_float(context.capital_base)
因实参急切求值使防御失效 → AttributeError 抛穿 initialize。

语义（形 A 引擎委托式）：
  有引擎 → engine._initial_capital（恒定初始资金，非活值）
  无引擎 → portfolio._init_cash 快照兜底（构造期/单测/非 ptrade 模式）
"""
from __future__ import annotations

import pytest

import quantstudio.backtest.ptrade_api as api
from quantstudio.backtest.ptrade_api import Context, Portfolio


class _FakeEngine:
    def __init__(self, init_cap):
        self._initial_capital = init_cap


def _ctx(cash=1_000_000.0):
    return Context("2025-04-21", "2025-04-18", Portfolio(cash, {}))


def test_attribute_exists():
    """修复前必红：Context 必须提供 capital_base。"""
    c = _ctx()
    assert hasattr(c, "capital_base"), "Context 缺 capital_base（本件修复目标）"


def _patch_engine(monkeypatch, eng):
    """挂点 = 模块级单例 _api._engine（Portfolio._engine() 的唯一解引用点，:2695）。"""
    monkeypatch.setattr(api._api, "_engine", eng)


def test_engine_path_returns_initial_capital(monkeypatch):
    """有引擎：取 engine._initial_capital（恒定初始资金）。"""
    _patch_engine(monkeypatch, _FakeEngine(1_000_000.0))
    assert _ctx(cash=3.14).capital_base == 1_000_000.0, "有引擎必须取引擎初始资金，而非活值 cash"


def test_engine_path_not_live_cash(monkeypatch):
    """关键语义：引擎耐久值优先于运行期活值 cash（防回到错值路径）。"""
    _patch_engine(monkeypatch, _FakeEngine(500_000.0))
    c = _ctx(cash=123.0)                      # 模拟刷新点传入的活值
    assert c.capital_base == 500_000.0
    assert c.capital_base != 123.0


def test_fallback_without_engine(monkeypatch):
    """无引擎：回退 portfolio._init_cash 快照（仅非引擎语境生效）。"""
    _patch_engine(monkeypatch, None)
    assert _ctx(cash=250_000.0).capital_base == 250_000.0


def test_returns_float(monkeypatch):
    """返回类型恒为 float（契约：数值型，供 safe_float 直接消费）。"""
    _patch_engine(monkeypatch, _FakeEngine(1_000_000.0))
    assert isinstance(_ctx().capital_base, float)


def test_read_only_property(monkeypatch):
    """只读属性：不可赋值（防策略侧误写污染引擎真源）。"""
    c = _ctx()
    with pytest.raises(AttributeError):
        c.capital_base = 1.0
