import sys, sqlite3, statistics; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
# 取若干已知 qfq-class 与 raw-class code，比较 stock_minutes.close vs stock_daily.close 同日
for code in ("003018","300394","603409","001388"):
    m=d.execute("SELECT time,close FROM stock_minutes WHERE code=? AND (time%86400000) BETWEEN 25140000 AND 25260000 ORDER BY time DESC LIMIT 5",[code]).fetchall()
    print(code,"minutes(close bars desc):",[(datetime.fromtimestamp(t/1000,CST).strftime("%m-%d"),round(c,3)) for t,c in m])
    dd=d.execute("SELECT time,close,close_front FROM stock_daily WHERE code=? ORDER BY time DESC LIMIT 5",[code]).fetchall()
    print("   daily:",[(datetime.fromtimestamp(t/1000,CST).strftime("%m-%d"),round(c,3),round(cf,3)) for t,c,cf in dd])
con.close(); d.close()
