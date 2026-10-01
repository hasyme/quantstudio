import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
def dd(v):
    return datetime.fromtimestamp(int(v)*86400, CST).strftime("%Y-%m-%d")
d=duckdb.connect(str(db_path()), read_only=True)
for thr,label in ((0.05,"heavy >5pct"),(0.005,"dev >0.5pct")):
    r=d.execute("""
    SELECT MIN(dd), MAX(dd), COUNT(*), COUNT(DISTINCT c) FROM (
      SELECT m.code c, ((m.time+28800000)//86400000) dd
      FROM stock_minutes m JOIN stock_daily d
       ON m.code=d.code AND ((m.time+28800000)//86400000)=((d.time+28800000)//86400000)
      WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d.close>0
        AND abs(m.close/d.close-1)> ?
    ) t
    """,[thr]).fetchone()
    print("%-14s min=%s max=%s pairs=%d codes=%d"%(label,dd(r[0]),dd(r[1]),int(r[2]),int(r[3])))
# 表自身日期范围
for t in ("stock_minutes","etf_minutes"):
    a=d.execute("SELECT MIN(time),MAX(time) FROM %s"%t).fetchone()
    print("%-14s own range %s .. %s"%(t,dd(int(a[0])//86400000),dd(int(a[1])//86400000)))
d.close()
