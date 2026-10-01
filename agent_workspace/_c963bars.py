import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
c=duckdb.connect(str(db_path()), read_only=True)
# 1月8日的所有 bar（应有 241 根真 bar 才对）
r=c.execute("""SELECT time, close, volume, suspendFlag FROM stock_minutes
 WHERE code='000963' AND ((time+28800000)//86400000)=?
 ORDER BY time""",
 [int((datetime(2026,1,8,tzinfo=CST).timestamp()*1000+28800000)//86400000)]).fetchall()
print('000963 @2026-01-08 bar 数:', len(r))
for t,cl,v,sf in r:
    print('   %s close=%-10s vol=%-8s susp=%s'%(
        datetime.fromtimestamp(int(t)/1000,CST).strftime('%H:%M'),cl,v,sf))
c.close()
