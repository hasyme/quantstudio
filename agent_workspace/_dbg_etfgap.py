import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=d.execute("SELECT DISTINCT code, ex_date FROM etf_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
gaps=[]
for code,ex in rows:
    f=con.execute("SELECT time,adj_factor FROM fund_adj WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-20*86400000,ex+20*86400000)).fetchall()
    if len(f)<2: gaps.append((str(code),None)); continue
    bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
    if not bef or not aft: gaps.append((str(code),"nosplit")); continue
    a0,a1=bef[-1],aft[0]
    if a0<=0 or a1<=a0: gaps.append((str(code),"nostep")); continue
    gaps.append((str(code),1-1/(a1/a0)))
ok=[g for c,g in gaps if isinstance(g,float)]
print("total",len(gaps),"numeric",len(ok))
if ok:
    ok.sort(reverse=True)
    print("gaps top12:",[round(x*100,3) for x in ok[:12]])
    print("max gap %:",round(max(ok)*100,3))
    print("count >=2%:",sum(1 for x in ok if x>=0.02))
print("non-numeric:",[(c,g) for c,g in gaps if not isinstance(g,float)][:8])
con.close(); d.close()
