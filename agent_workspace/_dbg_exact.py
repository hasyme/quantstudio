import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
d=duckdb.connect(str(db_path()), read_only=True)
r=d.execute("""
SELECT COUNT(*) n,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)<1e-9 THEN 1 ELSE 0 END),0) ex,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)<0.005 THEN 1 ELSE 0 END),0) nr
FROM stock_minutes m JOIN stock_daily d
 ON m.code=d.code AND ((m.time+28800000)//86400000)=((d.time+28800000)//86400000)
WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d.close>0
""").fetchone()
n,ex,nr=int(r[0]),int(r[1]),int(r[2])
print("pairs=%d exact=%d(%.2f%%) near0.5=%d(%.2f%%)"%(n,ex,100*ex/n,nr,100*nr/n))
d.close()
