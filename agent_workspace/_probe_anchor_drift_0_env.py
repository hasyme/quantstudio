"""只读取证探针 0：环境与路径解析（不改任何数据）。

用途：确认 duckdb 可用性、aux 因子库实际路由、以及 QualityAudit
      AdjustmentAnchorDrift 判据所消费的两个数据源是否可读。
"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(r"D:\hasym\PycharmProjects\QuantStudio")
sys.path.insert(0, str(ROOT))


def main() -> int:
    print("=" * 72)
    print("[1] duckdb / 依赖可用性")
    try:
        import duckdb
        print(f"    duckdb  = {duckdb.__version__}")
    except Exception as exc:
        print(f"    duckdb  !! {type(exc).__name__}: {exc}")
    for mod in ("pandas", "numpy", "sqlite3"):
        try:
            m = __import__(mod)
            print(f"    {mod:8s}= {getattr(m, '__version__', 'ok')}")
        except Exception as exc:
            print(f"    {mod:8s}!! {exc}")

    print("=" * 72)
    print("[2] aux 因子库路由解析（与 QualityAudit._resolve_aux_path 同源）")
    db_path = ROOT / "data" / "quantstudio.db"
    print(f"    main_db = {db_path}  exists={db_path.is_file()}")
    aux_resolved = None
    try:
        import duckdb
        from quantstudio.pipeline.qfq_aux_router import resolve_runtime_aux_path
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            aux_resolved, reason = resolve_runtime_aux_path(
                main_db=str(db_path),
                duckdb_read=con.execute,
                price_source="mcp",
                config_path=None,
            )
            print(f"    resolved aux = {aux_resolved}")
            print(f"    reason       = {reason}")
        finally:
            con.close()
    except Exception as exc:
        print(f"    !! 路由解析失败 {type(exc).__name__}: {exc}")

    print("=" * 72)
    print("[3] 候选 aux 库文件实际存在情况")
    for name in ("qfq_aux.db", "qfq_aux_mcp_gen1.db"):
        p = ROOT / "data" / name
        if p.is_file():
            print(f"    {name:24s} {p.stat().st_size/1024/1024:10.1f} MB")
        else:
            print(f"    {name:24s} (不存在)")

    print("=" * 72)
    print("[4] aux 因子表清单 + 目标 code 因子行数")
    import sqlite3
    target_aux = aux_resolved if (aux_resolved and Path(aux_resolved).is_file()) \
        else (ROOT / "data" / "qfq_aux.db")
    print(f"    使用 aux = {target_aux}")
    try:
        con = sqlite3.connect(f"file:{target_aux}?mode=ro", uri=True, timeout=30)
        try:
            con.execute("PRAGMA query_only=ON")
            tables = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            print(f"    表: {tables}")
            for tbl, codes in (("adj_factor", ["000012", "000012.SZ"]),
                               ("fund_adj", ["159119", "159119.SZ"])):
                if tbl not in tables:
                    continue
                print(f"    --- {tbl} ---")
                for c in codes:
                    n = con.execute(
                        f"SELECT COUNT(*) FROM {tbl} WHERE code=?", [c]).fetchone()[0]
                    print(f"        code={c:12s} rows={n}")
                    if n:
                        rows = con.execute(
                            f"SELECT time, adj_factor FROM {tbl} WHERE code=? "
                            f"ORDER BY time", [c]).fetchall()
                        print(f"        首: {rows[0]}")
                        print(f"        末: {rows[-1]}")
                        print(f"        末5: {rows[-5:]}")
        finally:
            con.close()
    except Exception as exc:
        print(f"    !! {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
