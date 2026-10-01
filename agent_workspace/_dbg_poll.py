import sys, sqlite3, statistics; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
# 对同一 (code,day)：minute_close / daily_close 与 adj_latest/adj_i 的关系
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=d.execute("SELECT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
recs=[]
for code,ex in rows[:400]:
    exday=int(ex)//86400000
    m=d.execute("SELECT time,close FROM stock_minutes WHERE code=? AND (time%86400000) BETWEEN 25140000 AND 25260000 AND time>=? AND time<? ORDER BY time",[str(code),ex-8*86400000,ex+2*86400000]).fetchall()
    if not m: continue
    md={int(t)//86400000:float(c) for t,c in m}
    dd=d.execute("SELECT time,close FROM stock_daily WHERE code=? AND time>=? AND time<? ORDER BY time",[str(code),ex-8*86400000,ex+2*86400000]).fetchall()
    dw={int(t)//86400000:float(c) for t,c in dd}
    common=sorted(set(md)&set(dw))
    if not common: continue
    for day in common:
        if md[day]<=0 or dw[day]<=0: continue
        recs.append(md[day]/dw[day])
print("common (code,day) pairs:",len(recs))
if recs:
    import collections
    near1=sum(1 for r in recs if abs(r-1)<0.005)
    print("ratio m/d: median=%.4f  min=%.3f max=%.3f  |r-1|<0.5%%的占比=%.1f%%"%(statistics.median(recs),min(recs),max(recs),100*near1/len(recs)))
con.close(); d.close()
