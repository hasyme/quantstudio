import sys; sys.path.insert(0,".")
from quantstudio._paths import db_path
import duckdb, json, time
t0=time.time()
d=duckdb.connect(str(db_path()), read_only=True)
print("=== 修复前基线 ===  (db=%s)"%db_path())

# 1) stock_minutes vs stock_daily 全量配对
r=d.execute("""
SELECT COUNT(*) n,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)<1e-9 THEN 1 ELSE 0 END),0) ex,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)<0.005 THEN 1 ELSE 0 END),0) nr,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)>0.05 THEN 1 ELSE 0 END),0) bad
FROM stock_minutes m JOIN stock_daily d
 ON m.code=d.code AND ((m.time+28800000)//86400000)=((d.time+28800000)//86400000)
WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d.close>0
""").fetchone()
n,ex,nr,bad=[int(x) for x in r]
print("stock_minutes 配对: n=%d exact=%d(%.2f%%) <=0.5%%=%d(%.2f%%) >5%%=%d(%.2f%%)"%(
    n,ex,100*ex/n,nr,100*nr/n,bad,100*bad/n))

# 2) ETF 同构
r2=d.execute("""
SELECT COUNT(*) n,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)<1e-9 THEN 1 ELSE 0 END),0) ex,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)<0.005 THEN 1 ELSE 0 END),0) nr,
  COALESCE(SUM(CASE WHEN abs(m.close/d.close-1)>0.05 THEN 1 ELSE 0 END),0) bad
FROM etf_minutes m JOIN etf_daily d
 ON m.code=d.code AND ((m.time+28800000)//86400000)=((d.time+28800000)//86400000)
WHERE (m.time%86400000) BETWEEN 25140000 AND 25260000 AND m.close>0 AND d.close>0
""").fetchone()
n2,ex2,nr2,bad2=[int(x) for x in r2]
print("etf_minutes   配对: n=%d exact=%d(%.2f%%) <=0.5%%=%d(%.2f%%) >5%%=%d(%.2f%%)"%(
    n2,ex2,(100*ex2/n2 if n2 else 0),nr2,(100*nr2/n2 if n2 else 0),bad2,(100*bad2/n2 if n2 else 0)))

# 3) 表水位
for t in ("stock_minutes","stock_daily","etf_minutes","etf_daily"):
    mx=d.execute("SELECT MAX(time) FROM %s"%t).fetchone()[0]
    cnt=d.execute("SELECT COUNT(*) FROM %s"%t).fetchone()[0]
    print("%-14s rows=%-12d max_time=%s"%(t,cnt,mx))
d.close()
print("elapsed %.1fs"%(time.time()-t0))
