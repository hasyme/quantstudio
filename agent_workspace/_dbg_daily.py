import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=d.execute("SELECT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
res=[]
for code,ex in rows:
    f=con.execute("SELECT time,adj_factor FROM adj_factor WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-12*86400000,ex+12*86400000)).fetchall()
    if len(f)<2: continue
    # step at/before ex
    bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
    if not bef or not aft: continue
    a0,a1=bef[-1],aft[0]
    if a0<=0 or a1<=a0: continue
    k=a1/a0; gap=1-1/k
    if gap<0.02: continue
    # daily bars around ex
    b=d.execute("SELECT time,close FROM stock_daily WHERE code=? AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-6*86400000,ex+4*86400000]).fetchall()
    if len(b)<2: continue
    dc={int(t)//86400000:float(c) for t,c in b}
    days=sorted(dc)
    r=dc[days[-1]]/dc[days[-2]]
    res.append((str(code),gap,r,1/k,1/(k*k)))
print("probes:",len(res))
import statistics
qfq=[x for x in res if abs(x[2]-1)<abs(x[2]-x[3]) and abs(x[2]-1)<abs(x[2]-x[4])]
raw=[x for x in res if abs(x[2]-x[3])<abs(x[2]-1) and abs(x[2]-x[3])<abs(x[2]-x[4])]
dbl=[x for x in res if abs(x[2]-x[4])<abs(x[2]-1) and abs(x[2]-x[4])<abs(x[2]-x[3])]
print("nearest-class: qfq",len(qfq),"raw",len(raw),"double",len(dbl))
print("median r:", round(statistics.median(x[2] for x in res),4))
print("median 1/k:", round(statistics.median(x[3] for x in res),4))
print("top-gap examples (code, gap, r, 1/k, 1/k^2):")
for x in sorted(res,key=lambda y:-y[1])[:8]:
    print("  ",x[0],"gap=%.3f r=%.4f raw_exp=%.4f dbl_exp=%.4f"%(x[1],x[2],x[3],x[4]))
con.close(); d.close()
