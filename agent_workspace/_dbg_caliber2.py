import sys; sys.path.insert(0, ".")
from datetime import datetime, timezone, timedelta
from pathlib import Path
from quantstudio.pipeline.quality_audit import DataQualityAuditor, QualityReport
CST = timezone(timedelta(hours=8))
def _ms(y,m,d,hh=0,mm=0): return int(datetime(y,m,d,hh,mm,tzinfo=CST).timestamp()*1000)
EX=_ms(2026,9,21)
class R:
    def __init__(s,r): s._r=r
    def fetchall(s): return s._r
    def __iter__(s): return iter(s._r)
class C:
    def execute(s, sql, *a):
        q=str(sql)
        if "FROM stock_dividend" in q:
            return R([(f"0000{i:02d}", EX) for i in range(30)])
        if "FROM stock_minutes" in q:
            if not hasattr(s,"n"): s.n=0
            s.n+=1
            code=a[0][0]
            prev=_ms(2026,9,18,15,0); cur=_ms(2026,9,21,15,0)
            return R([(code,prev,10.0),(code,cur,10.0*1.0)])
        raise AssertionError(q[:80])
aud = DataQualityAuditor("u.duckdb", schemas={})
aud.qfq_aux_override = Path("/nonexistent/x.db")
aud._resolve_aux_path = lambda conn: Path("/nonexistent/x.db")
aud._read_factor_table = lambda ap, ft, codes: [(c, _ms(2026,1,1), 10.0) for c in codes]+[(c,EX,12.0) for c in codes]
rep = QualityReport()
aud._audit_caliber_drift(C(), rep, {"stock_minutes","stock_dividend"})
print("probes-bar-calls:", )
print("issues:", [(i.check,i.severity,i.count,i.detail) for i in rep.issues])
