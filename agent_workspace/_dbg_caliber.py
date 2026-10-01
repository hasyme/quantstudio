import sys; sys.path.insert(0, ".")
sys.argv=["x"]
from datetime import datetime, timezone, timedelta
from pathlib import Path
from quantstudio.pipeline.quality_audit import DataQualityAuditor, QualityReport
CST = timezone(timedelta(hours=8))
def _ms(y,m,d,hh=0,mm=0): return int(datetime(y,m,d,hh,mm,tzinfo=CST).timestamp()*1000)
class R:
    def __init__(s,r): s._r=r
    def fetchall(s): return s._r
    def __iter__(s): return iter(s._r)
class C:
    def execute(s, sql, *a):
        print("SQL>>", str(sql)[:150].replace("\n"," "))
        print("ARG>>", a)
        return R([])
aud = DataQualityAuditor("u.duckdb", schemas={})
aud.qfq_aux_override = Path("/nonexistent/x.db")
aud._resolve_aux_path = lambda conn: Path("/nonexistent/x.db")
aud._read_factor_table = lambda ap, ft, codes: [(c, _ms(2026,1,1), 10.0) for c in codes]+[(c,_ms(2026,9,21),12.0) for c in codes]
rep = QualityReport()
aud._audit_caliber_drift(C(), rep, {"stock_minutes","stock_dividend"})
print("issues:", [(i.check,i.severity) for i in rep.issues])
