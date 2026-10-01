import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from pathlib import Path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
# 603409 是 gap 最大的（50.45%）
for code in ("603409","001388"):
    ex=d.execute("SELECT ex_date FROM stock_dividend WHERE code=? AND ex_date BETWEEN ? AND ?",[code,lo,hi]).fetchall()
    print(code,"ex rows:",[datetime.fromtimestamp(e[0]/1000,CST).strftime("%Y-%m-%d") for e in ex])
    if not ex: continue
    e=ex[0][0]
    f=con.execute("SELECT time,adj_factor FROM adj_factor WHERE code=? AND time>=? AND time<=? ORDER BY time",(code,e-6*86400000,e+6*86400000)).fetchall()
    print("  factor near:",[(datetime.fromtimestamp(t/1000,CST).strftime("%m-%d %H:%M"),v) for t,v in f][:8])
    for tbl in ("stock_minutes","stock_daily"):
        b=d.execute(f"SELECT time,close FROM {tbl} WHERE code=? AND time>=? AND time<? ORDER BY time LIMIT 8",[code,e-4*86400000,e+4*86400000]).fetchall()
        print(f"  {tbl}:",[(datetime.fromtimestamp(t/1000,CST).strftime("%m-%d %H:%M"),c) for t,c in b][:8])
con.close(); d.close()
