import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
# 603409: ex=2026-06-18, factor step?  001388: ex=2026-07-17
for code,exs in (("603409","2026-06-18"),("001388","2026-07-17"),("688576",None)):
    ex = int(datetime.strptime(exs,"%Y-%m-%d").replace(tzinfo=CST).timestamp()*1000) if exs else None
    f=con.execute("SELECT time,adj_factor FROM adj_factor WHERE code=? ORDER BY time",(code,)).fetchall()
    prev=None; steps=[]
    for t,v in f:
        if prev is not None and v!=prev: steps.append((t,prev,v))
        prev=v
    if ex:
        near=[s for s in steps if abs(s[0]-ex)<15*86400000]
        print(f"{code} ex={exs} steps_near={[(datetime.fromtimestamp(t/1000,CST).strftime('%m-%d'),round(a,4),round(b,4)) for t,a,b in near]}")
    # 日线 around ex
    if ex:
        b=d.execute("SELECT time,close,close_front FROM stock_daily WHERE code=? AND time>=? AND time<? ORDER BY time",[code,ex-5*86400000,ex+3*86400000]).fetchall()
        print("   daily:",[(datetime.fromtimestamp(t/1000,CST).strftime("%m-%d"),c,cf) for t,c,cf in b])
con.close(); d.close()
