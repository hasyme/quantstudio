import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from pathlib import Path
from datetime import datetime, timezone, timedelta
import duckdb, statistics
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=d.execute("SELECT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
gaps=[]
for code,ex in rows:
    f=con.execute("SELECT time, adj_factor FROM adj_factor WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code), ex-10*86400000, ex+10*86400000)).fetchall()
    if len(f)<2: continue
    # 步进点
    prev=None; step=None
    for t,v in f:
        if prev is not None and v!=prev: step=(prev,v); break
        prev=v
    if not step: continue
    a0,a1=step
    if a0>0 and a1>a0: gaps.append((str(code), a1/a0-1, 1-1/(a1/a0)))
gaps.sort(key=lambda x:-x[1])
print("codes with factor step near ex:", len(gaps))
import collections
buckets=collections.Counter()
for c,dk,gp in gaps:
    if gp<0.001: buckets["<0.1%"]+=1
    elif gp<0.005: buckets["0.1-0.5%"]+=1
    elif gp<0.01: buckets["0.5-1%"]+=1
    elif gp<0.02: buckets["1-2%"]+=1
    else: buckets[">2%"]+=1
print(dict(buckets))
print("top10:", [(c,round(dk*100,2)) for c,dk,gp in gaps[:10]])
con.close(); d.close()
