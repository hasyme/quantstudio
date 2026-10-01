"""修订取证：P0-4 —— 归因「云端 close ≠ 日线 raw」的 7/107 行。

审计要求：方案 A fail-fast 分支的实测触发率下限 = 这 7 行。
须判定它们是真 qfq、舍入差、还是口径差异，据此定 tol。

只读 _step2b_reconcile.csv。
"""
from __future__ import annotations
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
CSV = ROOT / "agent_workspace" / "_step2b_reconcile.csv"
OUT = ROOT / "agent_workspace" / "_p04_out.txt"

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def main() -> int:
    j = pd.read_csv(CSV, dtype={"code": str, "day": str})
    P(f"样本 n={len(j)}")
    # H1 失败：云端 close != 日线 raw
    j["diff"] = (j["close"] - j["dclose"]).abs()
    j["h1_ok"] = j["diff"] < 0.005
    bad = j[~j["h1_ok"]].sort_values("diff")
    P(f"H1 失败行 = {len(bad)}")
    P("")
    P(("  {:8s} {:11s} {:>10s} {:>9s} {:>10s} {:>10s} {:>9s} {:>8s} {:>8s}"
       ).format("code", "day", "云端close", "日线raw", "落库close", "ratio",
                "diff", "drift", "is_qfq"))
    for x in bad.itertuples():
        iq = getattr(x, "is_qfq", "")
        P(("  {:8s} {:11s} {:10.4f} {:9.4f} {:10.4f} {:10.6f} {:9.4f} {:8.4f} {}"
           ).format(x.code, x.day, x.close, x.dclose, x.mclose, x.ratio,
                    x.diff, x.drift, iq))
    P("")
    # 归因分类
    for x in bad.itertuples():
        d = x.diff
        rel = d / x.dclose if x.dclose else 0
        cand_qfq = x.close * x.ratio
        if rel < 0.001:
            cls = "舍入级(<0.1%)"
        elif abs(cand_qfq - x.dclose) < 0.005:
            cls = "云端值×ratio≈日线raw => 云端是【qfq】"
        else:
            cls = "其他(需人工看)"
        P(f"  {x.code} {x.day}: diff={d:.4f} rel={rel:.2%} "
          f"cand_qfq={cand_qfq:.4f} vs raw={x.dclose:.4f} => [{cls}]")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
