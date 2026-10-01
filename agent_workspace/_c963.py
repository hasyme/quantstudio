import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
c=duckdb.connect(str(db_path()), read_only=True)
# 000963 在两个 bar 上的表现（该 code 是重度污染样本）
q="""
SELECT (m.time%86400000) tod, m.time, m.close, m.volume, m.suspendFlag, d.close AS daily
FROM stock_minutes m LEFT JOIN stock_daily d
  ON m.code=d.code AND ((m.time+28800000)//86400000)=((d.time+28800000)//86400000)
WHERE m.code='000963' AND ((m.time+28800000)//86400000)=?
  AND (m.time%86400000) IN (25140000,25200000)
ORDER BY m.time"""
d0=int((datetime(2026,1,8,tzinfo=CST).timestamp()*1000+28800000)//86400000)
for tod,t,cl,v,sf,dy in c.execute(q,[d0]).fetchall():
    print('%s close=%-10s vol=%-10s susp=%s daily=%s'%(
        '14:59' if tod==25140000 else '15:00', cl, v, sf, dy))
c.close()
