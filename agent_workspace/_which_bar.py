import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
c=duckdb.connect(str(db_path()), read_only=True)
# 分别用 14:59 与 15:00 对日线 raw 比较，判断哪个才是真收盘
q="""
WITH x AS (
  SELECT m.code, ((m.time+28800000)//86400000) d,
    MAX(CASE WHEN (m.time%86400000)=25140000 THEN m.close END) c1459,
    MAX(CASE WHEN (m.time%86400000)=25200000 THEN m.close END) c1500
  FROM stock_minutes m
  WHERE (m.time%86400000) IN (25140000,25200000)
  GROUP BY 1,2
)
SELECT COUNT(*) n,
  COALESCE(SUM(CASE WHEN x.c1459 IS NOT NULL AND abs(x.c1459/d.close-1)<0.005 THEN 1 ELSE 0 END),0) ok59,
  COALESCE(SUM(CASE WHEN x.c1500 IS NOT NULL AND abs(x.c1500/d.close-1)<0.005 THEN 1 ELSE 0 END),0) ok00,
  COALESCE(SUM(CASE WHEN x.c1459 IS NOT NULL THEN 1 ELSE 0 END),0) has59,
  COALESCE(SUM(CASE WHEN x.c1500 IS NOT NULL THEN 1 ELSE 0 END),0) has00
FROM x JOIN stock_daily d
  ON x.code=d.code AND x.d=((d.time+28800000)//86400000)
WHERE d.close>0
"""
n,ok59,ok00,h59,h00=[int(v) for v in c.execute(q).fetchone()]
print('配对组数        :', n)
print('14:59 对日线一致:', ok59, '/', h59, '(%.2f%%)'%(100*ok59/h59 if h59 else 0))
print('15:00 对日线一致:', ok00, '/', h00, '(%.2f%%)'%(100*ok00/h00 if h00 else 0))
c.close()
