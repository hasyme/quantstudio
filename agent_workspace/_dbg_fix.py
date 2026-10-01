import sys, sqlite3, statistics; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
def probe(tbl,gapmin,exact):
    rows=d.execute("SELECT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
    rs=[]
    for code,ex in rows:
        f=con.execute("SELECT time,adj_factor FROM adj_factor WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-12*86400000,ex+12*86400000)).fetchall()
        if len(f)<2: continue
        bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
        if not bef or not aft: continue
        a0,a1=bef[-1],aft[0]
        if a0<=0 or a1<=a0: continue
        k=a1/a0; gap=1-1/k
        if gap<gapmin: continue
        tod="(time%86400000) BETWEEN 25140000 AND 25260000" if tbl.endswith("_minutes") else "(time%86400000)=57600000"
        b=d.execute(f"SELECT time,close FROM {tbl} WHERE code=? AND time>=? AND time<? AND close>0 AND {tod} ORDER BY time",[str(code),ex-8*86400000,ex+4*86400000]).fetchall()
        if len(b)<2: continue
        daybar={int(t)//86400000:float(c) for t,c in b}
        exday=int(ex)//86400000
        if exact:
            if exday not in daybar: continue
            before=[dd for dd in sorted(daybar) if dd<exday]
            if not before: continue
            r=daybar[exday]/daybar[before[-1]]
        else:
            days=sorted(daybar)
            if len(days)<2: continue
            r=daybar[days[-1]]/daybar[days[-2]]
        rs.append((str(code),gap,r))
    return rs
for tbl in ("stock_daily","stock_minutes"):
    for exact in (False,True):
        rs=probe(tbl,0.10,exact)
        if not rs: print(f"{tbl} exact={exact}: 0"); continue
        n=statistics.median(abs(x[2]-1)/x[1] for x in rs)
        print(f"{tbl} exact={exact}: n={len(rs):4d} median(|r-1|/gap)={n:.3f} median_r={statistics.median(x[2] for x in rs):.4f}")
con.close(); d.close()
