import sys; sys.path.insert(0,".")
from quantstudio.pipeline.quality_audit import DataQualityAuditor, QualityReport
from quantstudio._paths import db_path
from pathlib import Path
import duckdb
p = Path("config/profiles/mcp_only/alignment_rules.json")
aud = DataQualityAuditor.from_config(db_path(), p)
con = duckdb.connect(str(db_path()), read_only=True)
tabs = set(r[0] for r in con.execute("SHOW TABLES").fetchall())
rep = QualityReport()
# 隔离运行：只跑口径漂移门禁（不跑全量审计）
aud._audit_caliber_drift(con, rep, tabs)
print("checks_run:", rep.checks_run)
for i in rep.issues:
    print(f"[{i.severity}] {i.table}/{i.check}: {i.count}")
    print("    ", i.detail[:300])
con.close()
