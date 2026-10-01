import sys, sqlite3, statistics; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=d.execute("SELECT DISTINCT code, ex_date FROM etf_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
for gapmin in (0.005,0.01,0.02):
    recs=[]
    for code,ex in rows:
        f=con.execute("SELECT time,adj_factor FROM fund_adj WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-20*86400000,ex+20*86400000)).fetchall()
        if len(f)<2: continue
        bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
        if not bef or not aft: continue
        a0,a1=bef[-1],aft[0]
        if a0<=0 or a1<=a0: continue
        k=a1/a0; gap=1-1/k
        if gap<gapmin: continue
        exday=(ex+8*3600000)//86400000
        for tbl,tod in (("etf_daily","(time%86400000)=57600000"),("etf_minutes","(time%86400000) BETWEEN 25140000 AND 25260000")):
            b=d.execute(f"SELECT time,close FROM {tbl} WHERE code=? AND {tod} AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-6*86400000,ex+4*86400000]).fetchall()
            dc={(t+8*3600000)//86400000:float(c) for t,c in b}
            if exday not in dc: continue
            prior=[x for x in dc if x<exday]
            if not prior: continue
            r=dc[exday]/dc[max(prior)]
            hyp={"qfq":1.0,"raw":1/k,"double":1/(k*k)}
            best=min(hyp,key=lambda h:abs(r-hyp[h]))
            if abs(r-hyp[best])<=0.5*gap: recs.append((tbl,best,r,gap))
    from collections import Counter
    for tbl in ("etf_daily","etf_minutes"):
        sub=[x for x in recs if x[0]==tbl]
        print(f"gapmin={gapmin} {tbl}: probes={len(sub)} votes={dict(Counter(x[1] for x in sub))}")
con.close(); d.close()
