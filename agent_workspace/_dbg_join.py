import sys, sqlite3, statistics; sys.path.insert(0,".")
from quantstudio._paths import db_path
from datetime import datetime, timezone, timedelta
import duckdb
CST=timezone(timedelta(hours=8))
d=duckdb.connect(str(db_path()), read_only=True)
# 全量抽样：stock_minutes 收盘 bar vs 同日 stock_daily.close
rows=d.execute("""
SELECT m.code, m.time, m.close, d.close
FROM stock_minutes m JOIN stock_daily d
  ON m.code=d.code AND (m.time - (m.time % 86400000)) = (d.time - 57600000)
WHERE (m.time % 86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d.close>0
LIMIT 200000
""").fetchall()
print("pairs:",len(rows))
r=[a[2]/a[3] for a in rows]
near=sum(1 for x in r if abs(x-1)<0.005)
print("median=%.5f  |r-1|<0.5%%: %.2f%%  p95=%.4f p99=%.4f max=%.3f"%(statistics.median(r),100*near/len(r),sorted(r)[int(.95*len(r))],sorted(r)[int(.99*len(r))],max(r)))
d.close()
