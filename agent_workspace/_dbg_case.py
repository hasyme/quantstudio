import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from pathlib import Path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
aux = Path("data/qfq_aux.db")
con = sqlite3.connect(str(aux)); con.execute("PRAGMA query_only=ON")
def fac(code):
    return con.execute("SELECT time, adj_factor FROM adj_factor WHERE code=? ORDER BY time",(code,)).fetchall()
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
d = duckdb.connect(str(db_path()), read_only=True)
rows = d.execute("SELECT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL LIMIT 12",[lo,hi]).fetchall()
for code, ex in rows:
    f = fac(code)
    exd = datetime.fromtimestamp(ex/1000, CST).strftime("%Y-%m-%d")
    if not f:
        print(f"{code} ex={exd} FACTORS=NONE"); continue
    # 找 ex 附近的因子步进
    near = [(t,v) for t,v in f if abs(t-ex) < 20*86400000]
    print(f"{code} ex={exd} nfac={len(f)} first={datetime.fromtimestamp(f[0][0]/1000,CST).strftime('%Y-%m-%d')} last={datetime.fromtimestamp(f[-1][0]/1000,CST).strftime('%Y-%m-%d')} nearstep={[(datetime.fromtimestamp(t/1000,CST).strftime('%m-%d'),v) for t,v in near][:6]}")
con.close(); d.close()
