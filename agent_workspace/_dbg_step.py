import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
CST=timezone(timedelta(hours=8))
con = sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
for code, exs in (("600812","2026-06-11"),("600602","2026-07-28")):
    f = con.execute("SELECT time, adj_factor FROM adj_factor WHERE code=? ORDER BY time",(code,)).fetchall()
    ex = int(datetime.strptime(exs,"%Y-%m-%d").replace(tzinfo=CST).timestamp()*1000)
    # 找 ex 前后 45 天内的所有“值变化点”
    prev=None
    print(f"=== {code} ex={exs} ===")
    for t,v in f:
        if prev is not None and v!=prev and abs(t-ex) < 45*86400000:
            print("  step@", datetime.fromtimestamp(t/1000,CST).strftime("%Y-%m-%d"), prev,"->",v)
        prev=v
    # 打印 ex 前后各 3 天因子
    for t,v in f:
        if abs(t-ex) < 3*86400000:
            print("   near", datetime.fromtimestamp(t/1000,CST).strftime("%Y-%m-%d"), v)
con.close()
