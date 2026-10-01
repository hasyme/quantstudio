import sys, sqlite3, statistics; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)

def probe(tbl, div_tbl, factor_tbl):
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
        if gap<0.02: continue
        b=d.execute(f"SELECT time,close FROM {tbl} WHERE code=? AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-6*86400000,ex+4*86400000]).fetchall()
        if len(b)<2: continue
        dc={int(t)//86400000:float(c) for t,c in b}
        days=sorted(dc)
        if len(days)<2: continue
        r=dc[days[-1]]/dc[days[-2]]
        rs.append((str(code),gap,r))
    return rs

for tbl,divt,ft in (("stock_daily","stock_dividend","adj_factor"),
                    ("stock_minutes","stock_dividend","adj_factor"),
                    ("etf_daily","etf_dividend","fund_adj"),
                    ("etf_minutes","etf_dividend","fund_adj")):
    try:
        rs=probe(tbl,divt,ft)
    except Exception as e:
        print(tbl,"ERR",type(e).__name__,str(e)[:80]); continue
    if not rs: print(tbl,"0 probes"); continue
    # r 与 1 的接近度 vs 与 1/k 的接近度
    med_r=statistics.median(x[2] for x in rs)
    # 相对 gap 归一化的 |r-1|/gap 中位数（raw 态应 ~1，qfq 态应 ~0）
    norm=statistics.median(abs(x[2]-1)/x[1] for x in rs)
    print(f"{tbl}: probes={len(rs)} median_r={med_r:.4f} median(|r-1|/gap)={norm:.4f}  (>0.8=raw, <0.2=qfq)")
con.close(); d.close()
