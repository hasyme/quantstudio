import sys, sqlite3, statistics; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
def probe(tbl,div_tbl,factor_tbl,gapmin):
    rows=d.execute(f"SELECT code, ex_date FROM {div_tbl} WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
    rs=[]
    for code,ex in rows:
        f=con.execute(f"SELECT time,adj_factor FROM {factor_tbl} WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-12*86400000,ex+12*86400000)).fetchall()
        if len(f)<2: continue
        bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
        if not bef or not aft: continue
        a0,a1=bef[-1],aft[0]
        if a0<=0 or a1<=a0: continue
        k=a1/a0; gap=1-1/k
        if gap<gapmin: continue
        # 严格：除权日当天 bar + 除权日前最后一个 bar
        b=d.execute(f"SELECT time,close FROM {tbl} WHERE code=? AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-8*86400000,ex+3*86400000]).fetchall()
        if len(b)<2: continue
        daybar={}
        for t,c in b: daybar[int(t)//86400000]=(int(t),float(c))
        exday=int(ex)//86400000
        before=[(dd,) + daybar[dd] for dd in sorted(daybar) if dd<exday]
        at=[daybar[dd] for dd in sorted(daybar) if dd==exday]
        if not before or not at: continue
        t0,c0=before[-1][1],before[-1][2]
        t1,c1=at[0]
        rs.append((str(code),gap,c1/c0))
    return rs
for tbl in ("stock_daily","stock_minutes"):
    for g in (0.02,0.10,0.20):
        rs=probe(tbl,"stock_dividend","adj_factor",g)
        if not rs: print(f"{tbl} gap>={g}: 0"); continue
        n=statistics.median(abs(x[2]-1)/x[1] for x in rs)
        print(f"{tbl} gap>={g}: n={len(rs):4d} median(|r-1|/gap)={n:.3f}  median_r={statistics.median(x[2] for x in rs):.4f}")
con.close(); d.close()
