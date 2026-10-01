import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
c=duckdb.connect(str(db_path()), read_only=True)
# update_time 分布：污染行 vs 正常行的写入时刻
q="""
SELECT update_time, COUNT(*) n,
  COALESCE(SUM(CASE WHEN (time%86400000)=25140000 THEN 1 ELSE 0 END),0) n1459,
  COALESCE(SUM(CASE WHEN (time%86400000)=25200000 THEN 1 ELSE 0 END),0) n1500
FROM stock_minutes
WHERE code='000963'
GROUP BY 1 ORDER BY 1"""
for ut,n,a,b in c.execute(q).fetchall():
    print('update_time=%-22s rows=%-6d 14:59=%-5d 15:00=%d'%(ut,int(n),int(a),int(b)))
c.close()
