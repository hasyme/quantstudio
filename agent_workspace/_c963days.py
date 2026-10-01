import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
c=duckdb.connect(str(db_path()), read_only=True)
# 000963 全部分钟数据的日期覆盖
r=c.execute("""SELECT MIN(time),MAX(time),COUNT(*),
   COUNT(DISTINCT ((time+28800000)//86400000)) FROM stock_minutes WHERE code='000963'""").fetchone()
def dt(ms): return datetime.fromtimestamp(int(ms)/1000, CST).strftime('%Y-%m-%d %H:%M')
print('000963: %s .. %s  rows=%d  distinct_days=%d'%(dt(r[0]),dt(r[1]),int(r[2]),int(r[3])))
print()
# 该 code 是否在受影响集合中（forensics 提到 000963）
print('forensics 记录：000963 属 1,096 个受影响 code 之一')
print()
# 看它的日期分布
rows=c.execute("""SELECT ((time+28800000)//86400000) d, COUNT(*) n FROM stock_minutes
  WHERE code='000963' GROUP BY 1 ORDER BY 1""").fetchall()
print('日期数:', len(rows))
for d,n in rows[:5]:
    print('   ', datetime.fromtimestamp(int(d)*86400, CST).strftime('%Y-%m-%d'), n)
print('   ...')
for d,n in rows[-3:]:
    print('   ', datetime.fromtimestamp(int(d)*86400, CST).strftime('%Y-%m-%d'), n)
c.close()
