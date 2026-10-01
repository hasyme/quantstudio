import sys, time; sys.path.insert(0,".")
from quantstudio.pipeline.quality_audit import DataQualityAuditor
from quantstudio._paths import db_path
from pathlib import Path
import duckdb
from datetime import datetime, timedelta
p = Path("config/profiles/mcp_only/alignment_rules.json")
aud = DataQualityAuditor.from_config(db_path(), p)
con = duckdb.connect(str(db_path()), read_only=True)
now = datetime.now()
lo = int((now - timedelta(days=120)).timestamp()*1000)
hi = int((now + timedelta(days=7)).timestamp()*1000)
for div_tbl in ("stock_dividend","etf_dividend"):
    t0=time.perf_counter()
    try:
        rows = con.execute(f"SELECT DISTINCT code, ex_date FROM {div_tbl} WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
    except Exception as e:
        print(div_tbl,"ERR",type(e).__name__,str(e)[:120]); continue
    print(div_tbl,"rows",len(rows),"codes",len(set(r[0] for r in rows)),f"{time.perf_counter()-t0:.2f}s")
# 单探针 bar 查询耗时（stock_minutes）
t0=time.perf_counter()
c=rows[0][0] if rows else "000001"
b=con.execute("SELECT code,time,close FROM stock_minutes WHERE code=? AND time>=? AND time<? AND close>0 AND (time % 86400000) BETWEEN ? AND ? ORDER BY time LIMIT 20",[c,lo,hi,25140000,25260000]).fetchall()
print("1 probe bars",len(b),f"{time.perf_counter()-t0:.2f}s")
con.close()
