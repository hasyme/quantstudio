import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=d.execute("SELECT DISTINCT code, ex_date FROM etf_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
print("etf_dividend rows:",len(rows),"codes:",len(set(str(r[0]) for r in rows)))
print("sample:",[(str(c),datetime.fromtimestamp(e/1000,CST).strftime("%Y-%m-%d")) for c,e in rows[:8]])
# 因子
codes=sorted(set(str(r[0]) for r in rows))
n=0
for c,e in rows[:10]:
    f=con.execute("SELECT time,adj_factor FROM fund_adj WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(c),e-20*86400000,e+20*86400000)).fetchall()
    print(str(c),"ex",datetime.fromtimestamp(e/1000,CST).strftime("%m-%d"),"fund_adj_near:",len(f), [(datetime.fromtimestamp(t/1000,CST).strftime("%m-%d"),v) for t,v in f[:4]])
print("etf_daily bars for first code:", d.execute("SELECT COUNT(*) FROM etf_daily WHERE code=?",[str(rows[0][0])]).fetchone() if rows else None)
print("etf_minutes bars for first code:", d.execute("SELECT COUNT(*) FROM etf_minutes WHERE code=?",[str(rows[0][0])]).fetchone() if rows else None)
con.close(); d.close()
