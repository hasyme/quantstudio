"""只读取证探针 1a：表规模与索引（快，仅元数据）。"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
MAIN = ROOT / "data" / "quantstudio.db"
OUT = ROOT / "agent_workspace" / "_probe_anchor_drift_1a_out.txt"

lines: list[str] = []


def P(s=""):
    lines.append(s)
    print(s, flush=True)


def main() -> int:
    import duckdb
    con = duckdb.connect(str(MAIN), read_only=True)
    try:
        P("=== stock_minutes schema ===")
        for r in con.execute("DESCRIBE stock_minutes").fetchall():
            P(f"  {r[0]:20s} {r[1]}")
        P("")
        P("=== 行数 ===")
        for t in ("stock_minutes", "etf_minutes", "stock_dividend", "etf_dividend"):
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                P(f"  {t:20s} {n:>14,}")
            except Exception as exc:
                P(f"  {t:20s} !! {exc}")
        P("")
        P("=== 000012 在 stock_minutes 的行数 / 时间跨度 ===")
        r = con.execute(
            "SELECT COUNT(*), MIN(time), MAX(time) FROM stock_minutes "
            "WHERE code='000012'").fetchone()
        P(f"  rows={r[0]:,}  min={r[1]}  max={r[2]}")
        P("")
        P("=== stock_dividend schema + 000012 除权记录 ===")
        try:
            for x in con.execute("DESCRIBE stock_dividend").fetchall():
                P(f"  {x[0]:20s} {x[1]}")
        except Exception as exc:
            P(f"  !! {exc}")
        try:
            for x in con.execute(
                "SELECT * FROM stock_dividend WHERE code='000012' "
                "ORDER BY ex_date DESC LIMIT 8").fetchall():
                P(f"  {x}")
        except Exception as exc:
            P(f"  !! {exc}")
    finally:
        con.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
