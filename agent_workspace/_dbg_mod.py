import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
d=duckdb.connect(str(db_path()), read_only=True)
for tbl in ("stock_daily","etf_daily","stock_minutes","etf_minutes"):
    r=d.execute(f"SELECT time FROM {tbl} WHERE code='603409' ORDER BY time LIMIT 3").fetchall() if tbl not in ("etf_daily","etf_minutes") else []
    if not r:
        r=d.execute(f"SELECT time FROM {tbl} ORDER BY time LIMIT 3").fetchall()
    print(tbl, [(t[0], t[0]%86400000, datetime.fromtimestamp(t[0]/1000,CST).strftime("%m-%d %H:%M")) for t in r])
d.close()
