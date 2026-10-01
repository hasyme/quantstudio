# -*- coding: utf-8 -*-
"""C3：6 策略 api_portability 横验证（内存重转 + PTrade 可移植性校验，零临时文件写盘）。

口径（依「全链路修复」铁律 + 既往验收证据）：
  canonical 6 策略 = CANSLIM / fall_reversal / tech_etf_mvo_rotation /
  vol_regime_mom_rev / weekly_smallcap_growth / 周频小市值成长动量（三层止损）
  对每个策略文件：convert_source(path) → errors 必须为空 → 对转换产物
  validate_ptrade_portability(converted_code) → ok 必须 True（blocks=0）。

本脚本纯内存转换，不写任何临时文件，规避沙箱 tempfile/mkdtemp 写权限限制。
"""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quantstudio.strategy_compiler.source_import import convert_source  # noqa: E402
from quantstudio.strategy_compiler.validators.validate_ptrade_portability import (  # noqa: E402
    validate_ptrade_portability,
)

STRATEGIES = ROOT / "quantstudio" / "backtest" / "strategies"

# (label, filename, etf_pool_start_date 或 None)
SIX = [
    ("CANSLIM", "CANSLIM突破成长选股策略.py", None),
    ("fall_reversal", "fall_reversal_quantstudio.py", None),
    ("tech_etf_mvo_rotation", "tech_etf_mvo_rotation_quantstudio.py", "2024-01-01"),
    ("vol_regime_mom_rev", "vol_regime_mom_rev_quantstudio.py", None),
    ("weekly_smallcap_growth", "weekly_smallcap_growth_momentum_10_quantstudio.py", None),
    ("周频小市值成长动量(三层止损)", "周频小市值成长动量（三层止损）.py", None),
]


def main():
    fails = 0
    for label, fname, etf_start in SIX:
        p = STRATEGIES / fname
        if not p.exists():
            print(f"[{label}] MISSING FILE: {fname}")
            fails += 1
            continue
        try:
            res = convert_source(
                p,
                strategy_id=label,
                etf_pool_start_date=etf_start,
                db_path=str(ROOT / "data" / "quantstudio.db"),
            )
        except Exception as e:  # noqa: BLE001
            print(f"[{label}] CONVERT EXCEPTION: {type(e).__name__}: {e}")
            fails += 1
            continue

        conv_errors = list(res.errors or [])
        if conv_errors:
            print(f"[{label}] CONVERT ERRORS ({len(conv_errors)}): {conv_errors[:5]}")
            fails += 1
            continue

        if not res.converted_code:
            print(f"[{label}] EMPTY converted_code")
            fails += 1
            continue

        ok, violations, _ = validate_ptrade_portability(res.converted_code, ir=None, spec=None)
        blocks = [v for v in violations if getattr(v, "severity", "") == "BLOCK"]
        status = "PASS" if ok else f"FAIL (blocks={len(blocks)})"
        print(f"[{label}] convert ok (errors=0, {len(res.converted_code)} chars) | "
              f"api_portability = {status}")
        if not ok:
            for v in blocks:
                print(f"    BLOCK {getattr(v, 'rule_id', '?')}: {v.message}")
            fails += 1

    print()
    print("===== C3 6-STRATEGY api_portability : %s =====" % ("PASS" if fails == 0 else "FAIL"))
    sys.exit(0 if fails == 0 else 1)


if __name__ == "__main__":
    main()
