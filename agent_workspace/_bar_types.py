import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
c=duckdb.connect(str(db_path()), read_only=True)
# 全表：14:59 与 15:00 两种 bar 的数量与质量特征
r=c.execute('''
SELECT (time%86400000) tod, COUNT(*) n,
  COALESCE(SUM(CASE WHEN volume=0 THEN 1 ELSE 0 END),0) vol0,
  COALESCE(SUM(CASE WHEN suspendFlag=1 THEN 1 ELSE 0 END),0) susp
FROM stock_minutes
WHERE (time%86400000) IN (25134000, 25140000, 25200000)
GROUP BY 1 ORDER BY 1''').fetchall()
print('%-10s %-12s %-12s %s'%('ms%86400000','rows','vol=0','suspend=1'))
for tod,n,v,s in r:
    hhmm={25134000:'14:58',25140000:'14:59',25200000:'15:00'}.get(tod,str(tod))
    print('%-10s %-12d %-12d %d'%(hhmm,int(n),int(v),int(s)))
c.close()
