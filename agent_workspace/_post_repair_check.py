import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
d=duckdb.connect(str(db_path()), read_only=True)
r=d.execute("""
SELECT COUNT(*) n,
  COALESCE(SUM(CASE WHEN abs(m.close/d2.close-1)<1e-9 THEN 1 ELSE 0 END),0) ex,
  COALESCE(SUM(CASE WHEN abs(m.close/d2.close-1)<=0.005 THEN 1 ELSE 0 END),0) nr,
  COALESCE(SUM(CASE WHEN abs(m.close/d2.close-1)>0.05 THEN 1 ELSE 0 END),0) bad
FROM stock_minutes m JOIN stock_daily d2
 ON m.code=d2.code AND ((m.time+28800000)//86400000)=((d2.time+28800000)//86400000)
WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d2.close>0
""").fetchone()
n,ex,nr,bad=[int(x) for x in r]
print("修复后 stock_minutes 配对: n=%d"%n)
print("  exact=%d (%.2f%%)   <=0.5%%=%d (%.2f%%)   >5%%=%d (%.2f%%)"%(ex,100*ex/n,nr,100*nr/n,bad,100*bad/n))
print("  [修复前基线: exact=51.34%%  <=0.5%%=81.52%%  >5%%=3.78%%]")
d.close()
