import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
c=duckdb.connect(str(db_path()), read_only=True)
r=c.execute("""
WITH x AS (
  SELECT code, ((time+28800000)//86400000) d,
    MAX(CASE WHEN (time%86400000)=25140000 THEN close END) c1459,
    MAX(CASE WHEN (time%86400000)=25200000 THEN close END) c1500
  FROM stock_minutes
  WHERE (time%86400000) IN (25140000,25200000)
  GROUP BY 1,2
)
SELECT COUNT(*) n,
  COALESCE(SUM(CASE WHEN c1459 IS NOT NULL AND c1500 IS NOT NULL THEN 1 ELSE 0 END),0) n_both,
  COALESCE(SUM(CASE WHEN c1459 IS NOT NULL AND c1500 IS NOT NULL
                    AND abs(c1459/c1500-1)<1e-9 THEN 1 ELSE 0 END),0) n_same,
  COALESCE(SUM(CASE WHEN c1459 IS NOT NULL AND c1500 IS NULL THEN 1 ELSE 0 END),0) n_only59
FROM x""").fetchone()
n,nb,ns,no=[int(v) for v in r]
print('(code,day) 组数          :', n)
print('同时有 14:59 与 15:00    :', nb, '  其中相等:', ns, '(%.1f%%)'%(100*ns/nb if nb else 0))
print('只有 14:59（无 15:00）   :', no)
c.close()
