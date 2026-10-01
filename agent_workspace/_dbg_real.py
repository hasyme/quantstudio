import sys; sys.path.insert(0,".")
from quantstudio.pipeline.quality_audit import DataQualityAuditor
from quantstudio._paths import db_path
from pathlib import Path
p = Path("config/profiles/mcp_only/alignment_rules.json")
aud = DataQualityAuditor.from_config(db_path(), p)
print("db:", db_path())
print("price_source:", getattr(aud,"_price_source",None))
print("qfq_paths_cfg:", getattr(aud,"qfq_aux_paths_config",None))
import duckdb
con = duckdb.connect(str(db_path()), read_only=True)
tabs = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
for t in ("stock_minutes","stock_daily","etf_daily","etf_minutes","stock_dividend","etf_dividend"):
    print(t, t in tabs)
aux = aud._resolve_aux_path(con)
print("aux:", aux)
con.close()
