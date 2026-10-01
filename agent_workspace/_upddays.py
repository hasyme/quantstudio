import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
c=duckdb.connect(str(db_path()), read_only=True)
q="""
SELECT substr(CAST(update_time AS VARCHAR),1,10) upd_day,
  COUNT(*) n,
  COALESCE(SUM(CASE WHEN volume=0 THEN 1 ELSE 0 END),0) vol0,
  COUNT(DISTINCT code) n_codes,
  COUNT(DISTINCT ((time+28800000)//86400000)) n_days
FROM stock_minutes
GROUP BY 1 ORDER BY 1"""
print('%-12s %-14s %-14s %-9s %s'%('update_day','rows','vol=0','codes','days'))
for d,n,v,nc,nd in c.execute(q).fetchall():
    print('%-12s %-14d %-14d %-9d %d'%(str(d),int(n),int(v),int(nc),int(nd)))
c.close()
