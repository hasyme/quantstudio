import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
c=duckdb.connect(str(db_path()), read_only=True)
# 关键判别：若同一 (code,time,freq) 被 upsert 覆盖，只有一份。
# 检查 000963 @01-08 的 14:58/14:59 是否也存在于别的时间戳（即"错位写入"）
r=c.execute("""SELECT time, close, volume, suspendFlag, update_time
 FROM stock_minutes WHERE code='000963'
   AND ((time+28800000)//86400000)=?
 ORDER BY time""",
 [int((datetime(2026,1,8,tzinfo=CST).timestamp()*1000+28800000)//86400000)]).fetchall()
for t,cl,v,sf,ut in r:
    print('%s close=%-10s vol=%-6s susp=%s upd=%s'%(
        datetime.fromtimestamp(int(t)/1000,CST).strftime('%H:%M:%S'),cl,v,sf,ut))
print()
# 对比：同一天有 241 根 bar 的日期（如 09-09）长什么样
r2=c.execute("""SELECT COUNT(*), MIN(time), MAX(time) FROM stock_minutes
 WHERE code='000963' AND ((time+28800000)//86400000)=?""",
 [int((datetime(2026,9,9,tzinfo=CST).timestamp()*1000+28800000)//86400000)]).fetchone()
print('09-09 bar 数=%d  %s .. %s'%(int(r2[0]),
  datetime.fromtimestamp(int(r2[1])/1000,CST).strftime('%H:%M'),
  datetime.fromtimestamp(int(r2[2])/1000,CST).strftime('%H:%M')))
c.close()
