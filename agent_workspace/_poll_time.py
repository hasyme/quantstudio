import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
d=duckdb.connect(str(db_path()), read_only=True)
# 污染的时间分布：按月统计 >5% 偏离的配对数
r=d.execute("""
SELECT ((m.time+28800000)//86400000/30) AS bucket, COUNT(*) n,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)>0.05 THEN 1 ELSE 0 END),0) bad
FROM stock_minutes m JOIN stock_daily d
 ON m.code=d.code AND ((m.time+28800000)//86400000)=((d.time+28800000)//86400000)
WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d.close>0
GROUP BY 1 ORDER BY 1
""").fetchall()
print("bucket(30d)  pairs      bad(>5%)")
for b,n,bad in r:
    if int(bad)>0 or (n and int(bad)/int(n)>0.001):
        print("%-11s %-10d %d (%.1f%%)"%(b,n,bad,100*int(bad)/int(n)))
d.close()
