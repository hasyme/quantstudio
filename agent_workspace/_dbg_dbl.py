import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=d.execute("SELECT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
n=0
for code,ex in rows:
    f=con.execute("SELECT time,adj_factor FROM adj_factor WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-12*86400000,ex+12*86400000)).fetchall()
    if len(f)<2: continue
    bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
    if not bef or not aft: continue
    a0,a1=bef[-1],aft[0]
    if a0<=0 or a1<=a0: continue
    k=a1/a0; gap=1-1/k
    if gap<0.02: continue
    exday=(ex+8*3600000)//86400000
    b=d.execute("SELECT time,close FROM stock_minutes WHERE code=? AND (time%86400000) BETWEEN 25140000 AND 25260000 AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-6*86400000,ex+4*86400000]).fetchall()
    dc={(t+8*3600000)//86400000:float(c) for t,c in b}
    if exday not in dc: continue
    prior=[x for x in dc if x<exday]
    if not prior: continue
    r=dc[exday]/dc[max(prior)]
    # 同时取日线同日
    dd=d.execute("SELECT time,close FROM stock_daily WHERE code=? AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-6*86400000,ex+4*86400000]).fetchall()
    dw={(t+8*3600000)//86400000:float(c) for t,c in dd}
    hyp={"qfq":1.0,"raw":1/k,"double":1/(k*k)}
    best=min(hyp,key=lambda h:abs(r-hyp[h]))
    if best=="double" and n<10:
        md=dc[exday]; dl=dw.get(exday)
        print(f"{code} gap={gap:.3f} k={k:.4f} r={r:.4f} 1/k={1/k:.4f} 1/k2={1/(k*k):.4f} min={md:.3f} daily={dl} min/daily={md/dl if dl else None}")
        n+=1
con.close(); d.close()
