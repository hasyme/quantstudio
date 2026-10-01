import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
# 只读连接检查早期已写入批次的正确性（Jan 5-8 已回写）
d=duckdb.connect(str(db_path()), read_only=True)
r=d.execute("""
SELECT COUNT(*) n,
  COALESCE(SUM(CASE WHEN abs(m.close/d2.close-1)<1e-9 THEN 1 ELSE 0 END),0) ex,
  COALESCE(SUM(CASE WHEN abs(m.close/d2.close-1)>0.05 THEN 1 ELSE 0 END),0) bad
FROM stock_minutes m JOIN stock_daily d2
 ON m.code=d2.code AND ((m.time+28800000)//86400000)=((d2.time+28800000)//86400000)
WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d2.close>0
  AND m.time >= 1767571200000 AND m.time < 1767888000000
""").fetchone()
n,ex,bad=[int(x) for x in r]
print("Jan05-08 已回写批次: pairs=%d exact=%d(%.1f%%) bad>5%%=%d"%(n,ex,(100*ex/n if n else 0),bad))
d.close()
