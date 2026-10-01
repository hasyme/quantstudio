"""步骤2c：真实污染面 vs 审计检出面的差集量化。

关键发现：存在「落库 close ≠ 日线 raw，但审计 drift=0」的行 —— 即
审计只检出「两列互不自洽」，检不出「两列同向偏移」。
本探针量化两个口径各自的命中率：
  A. 审计口径：front/close != adj_i/adj_latest   （drift > 0.5%）
  B. 真值口径：stored_close != daily_raw          （close 列本身失真）
只读分析既有对账 CSV。
"""
from __future__ import annotations
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
CSV = ROOT / "agent_workspace" / "_step2b_reconcile.csv"
OUT = ROOT / "agent_workspace" / "_step2c_blast_out.txt"

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def main() -> int:
    j = pd.read_csv(CSV, dtype={"code": str, "day": str})
    P(f"对账样本 = {len(j)} (code,day)")
    P("")
    # A: 审计口径
    j["audit_flag"] = j["drift"] > 0.005
    # B: 真值口径 —— 落库 close 是否等于日线 raw
    j["stored_close_eq_raw"] = (j["mclose"] - j["dclose"]).abs() < 0.005
    j["stored_close_bad"] = ~j["stored_close_eq_raw"]
    # C: 落库 front 是否等于应有的前复权价
    j["expect_front"] = j["dclose"] * j["adj_factor"] / j["adj_latest"]
    j["stored_front_ok"] = (
        (j["mfront"] - j["expect_front"]).abs()
        / j["expect_front"].replace(0, pd.NA) < 0.005)
    # D: 云端 close 是否等于日线 raw
    j["cloud_eq_raw"] = (j["close"] - j["dclose"]).abs() < 0.005

    P("=" * 76)
    P("口径命中率对比")
    P("=" * 76)
    P(f"A 审计检出 (drift>0.5%)            : {int(j['audit_flag'].sum()):4d}/{len(j)} "
      f"({j['audit_flag'].mean():.1%})")
    P(f"B 落库 close ≠ 日线 raw（真失真）  : {int(j['stored_close_bad'].sum()):4d}/{len(j)} "
      f"({j['stored_close_bad'].mean():.1%})")
    P(f"C 落库 front ≈ 应有前复权价        : {int(j['stored_front_ok'].sum()):4d}/{len(j)} "
      f"({j['stored_front_ok'].mean():.1%})")
    P(f"D 云端 close == 日线 raw           : {int(j['cloud_eq_raw'].sum()):4d}/{len(j)} "
      f"({j['cloud_eq_raw'].mean():.1%})")
    P("")
    P("=== 2×2 交叉：审计检出 × 真失真 ===")
    ct = pd.crosstab(j["audit_flag"], j["stored_close_bad"],
                     rownames=["审计检出"], colnames=["close真失真"])
    P(ct.to_string())
    P("")
    both = j[j["audit_flag"] & j["stored_close_bad"]]
    only_audit = j[j["audit_flag"] & ~j["stored_close_bad"]]
    only_true = j[~j["audit_flag"] & j["stored_close_bad"]]
    P(f"  审计检出 且 真失真   : {len(both):4d}  ← 命中")
    P(f"  审计检出 但 close正常: {len(only_audit):4d}  ← 另一列(front)失真")
    P(f"  审计未检出 但 真失真 : {len(only_true):4d}  ← **漏检**")
    P("")
    if len(only_true):
        P("=== 漏检样本（审计 drift=0 但落库 close 已失真）===")
        P(f"  {'code':8s} {'day':11s} {'云端close':>10s} {'日线raw':>9s} "
          f"{'落库close':>10s} {'ratio':>9s} {'drift':>8s} "
          f"{'落库front':>10s} {'应有front':>10s}")
        for x in only_true.head(25).itertuples():
            P(f"  {str(x.code):8s} {str(x.day):11s} {x.close:10.4f} {x.dclose:9.4f} "
              f"{x.mclose:10.4f} {x.ratio:9.6f} {x.drift:8.4f} "
              f"{x.mfront:10.4f} {x.expect_front:10.4f}")
    P("")
    P("=== 每 code 两口径对比 ===")
    P(f"  {'code':8s} {'n':>5s} {'审计检出':>9s} {'真失真':>8s} {'漏检':>6s}")
    for c, g in j.groupby("code"):
        P(f"  {c:8s} {len(g):5d} {int(g['audit_flag'].sum()):9d} "
          f"{int(g['stored_close_bad'].sum()):8d} "
          f"{int((~g['audit_flag'] & g['stored_close_bad']).sum()):6d}")
    P("")
    P("注：样本仅覆盖 landing 中留存 06-01~07-31 的 3 只 code；")
    P("    全量需步骤3的全库扫描结果（但步骤3按审计口径，同样会漏检同向偏移行）。")
    P("")
    P("=== 限定「front 非空」= 审计实际评估范围，重算 ===")
    jv = j[j["mfront"].notna() & j["expect_front"].notna()].copy()
    jv["af"] = jv["drift"] > 0.005
    jv["bad"] = (jv["mclose"] - jv["dclose"]).abs() >= 0.005
    P(f"  n={len(jv)}（原样本 {len(j)}；NaN front 行审计会 dropna 跳过）")
    P(f"  审计检出         : {int(jv['af'].sum()):4d} ({jv['af'].mean():.1%})")
    P(f"  真失真           : {int(jv['bad'].sum()):4d} ({jv['bad'].mean():.1%})")
    P(f"  真阳性(检出&失真): {int((jv['af'] & jv['bad']).sum()):4d}")
    P(f"  漏检(未检出&失真): {int((~jv['af'] & jv['bad']).sum()):4d}")
    P(f"  误报(检出&不失真): {int((jv['af'] & ~jv['bad']).sum()):4d}")
    P("")
    P("  漏检明细（top10 by 相对误差）:")
    miss = jv[~jv["af"] & jv["bad"]].copy()
    miss["rel"] = (miss["mclose"] / miss["dclose"] - 1).abs()
    P(f"    {'code':8s} {'day':11s} {'云端':>8s} {'日线raw':>9s} {'落库close':>10s} "
      f"{'相对误差':>9s} {'落库front':>10s}")
    for x in miss.sort_values("rel", ascending=False).head(10).itertuples():
        P(f"    {str(x.code):8s} {str(x.day):11s} {x.close:8.4f} {x.dclose:9.4f} "
          f"{x.mclose:10.4f} {x.rel:9.1%} {x.mfront:10.4f}")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nwritten: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
