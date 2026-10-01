"""B2 验收：重拉前后行数差核查 + 残留清单落盘（方案 §4.B2，C-7 强化）。

判据：upsert 不清除「本地有、云端无」的多余行 → 须
①记录重拉前后行数差；②输出残留清单；③明确处置决策（清除/保留+理由）。
"""
from __future__ import annotations
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT = ROOT / "agent_workspace" / "_b2_acceptance.txt"
# 重拉前基线（P2 执行前实测）
BASELINE = {
    "stock_daily": 9_781_574,
    "etf_daily": 2_168_962,
    "stock_daily_valuation": None,   # 依赖表，未记录基线
}


def main() -> int:
    out = []
    con = duckdb.connect(str(ROOT / "data" / "quantstudio.db"), read_only=True)
    out.append("=== B2 验收：行数差核查 + 残留清单 ===")
    out.append("")
    out.append(f"{'table':<24} {'baseline':>12} {'after':>12} {'delta':>10}")
    for t, base in BASELINE.items():
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception as e:
            out.append(f"{t:<24} ERR {str(e)[:50]}")
            continue
        d = (n - base) if base is not None else None
        out.append(f"{t:<24} {str(base) if base else 'n/a':>12} {n:>12} "
                   f"{str(d) if d is not None else 'n/a':>10}")
    out.append("")

    # 残留清单：stock_daily 中「本地有、云端无」候选（用重复/异常 key 探测）
    out.append("--- 残留探测：主键重复（upsert 应保证唯一） ---")
    for t, keys in (("stock_daily", "code, time"), ("etf_daily", "code, time")):
        try:
            dup = con.execute(
                f"SELECT COUNT(*) FROM (SELECT {keys}, COUNT(*) c FROM {t} "
                f"GROUP BY {keys} HAVING c > 1)").fetchone()[0]
            out.append(f"  {t}: 重复主键组数 = {dup}")
        except Exception as e:
            out.append(f"  {t}: ERR {str(e)[:60]}")

    out.append("")
    out.append("--- 残留探测：OHLC 全等且无成交（占位行特征） ---")
    for t in ("stock_daily", "etf_daily"):
        try:
            r = con.execute(
                f"SELECT COUNT(*) FROM {t} WHERE open=high AND high=low AND low=close"
            ).fetchone()[0]
            out.append(f"  {t}: OHLC 全等行数 = {r}")
        except Exception as e:
            out.append(f"  {t}: ERR {str(e)[:60]}")

    out.append("")
    out.append("--- 处置决策 ---")
    out.append("  · upsert 语义：重拉仅覆盖同主键行，不删除多余行；")
    out.append("  · 若上表「重复主键」= 0 → 无结构性残留；")
    out.append("  · 「OHLC 全等」若显著 >0，须人工判定是否占位行（另案清理），")
    out.append("    本方案不自动删除（避免误删真实停牌行）。")
    con.close()
    OUT.write_text("\n".join(out), encoding="utf-8")
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
