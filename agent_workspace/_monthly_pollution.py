import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
d=duckdb.connect(str(db_path()), read_only=True)
def dt(v): return datetime.fromtimestamp(int(v)*86400, CST).strftime("%Y-%m-%d")
# 按月看污染率，定位"已重拉"与"未重拉"的分界
q="""
SELECT ((m.time+28800000)//86400000) d, COUNT(*) n,
  COALESCE(SUM(CASE WHEN abs(m.close/d2.close-1)>0.05 THEN 1 ELSE 0 END),0) bad
FROM stock_minutes m JOIN stock_daily d2
 ON m.code=d2.code AND ((m.time+28800000)//86400000)=((d2.time+28800000)//86400000)
WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d2.close>0
GROUP BY 1 ORDER BY 1
"""
rows=d.execute(q).fetchall()
# 按月聚合
import collections
agg=collections.OrderedDict()
for day,n,bad in rows:
    key=dt(day)[:7]
    a=agg.setdefault(key,[0,0]); a[0]+=int(n); a[1]+=int(bad)
print("%-9s %-12s %-10s %s"%("month","pairs","bad>5%","rate"))
for k,(n,bad) in agg.items():
    print("%-9s %-12d %-10d %.2f%%"%(k,n,bad,100*bad/n if n else 0))
d.close()
