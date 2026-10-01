import sys, sqlite3; sys.path.insert(0,".")
from quantstudio._paths import db_path
from pathlib import Path
from datetime import datetime, timezone, timedelta
import duckdb, csv
CST=timezone(timedelta(hours=8))
# 已知受影响集合
aff=set()
p=Path("agent_workspace/_p16_etf_affected.csv")
con=sqlite3.connect("data/qfq_aux.db"); con.execute("PRAGMA query_only=ON")
d=duckdb.connect(str(db_path()), read_only=True)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
# 取 stock_minutes 上判为 qfq 的高 gap 探针 code
rows=d.execute("SELECT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
qfq_codes=[]; raw_codes=[]
for code,ex in rows:
    f=con.execute("SELECT time,adj_factor FROM adj_factor WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-12*86400000,ex+12*86400000)).fetchall()
    if len(f)<2: continue
    bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
    if not bef or not aft: continue
    a0,a1=bef[-1],aft[0]
    if a0<=0 or a1<=a0: continue
    k=a1/a0; gap=1-1/k
    if gap<0.10: continue
    b=d.execute("SELECT time,close FROM stock_minutes WHERE code=? AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-8*86400000,ex+3*86400000]).fetchall()
    if len(b)<2: continue
hist={}
for code,ex in rows:
    f=con.execute("SELECT time,adj_factor FROM adj_factor WHERE code=? AND time>=? AND time<=? ORDER BY time",(str(code),ex-12*86400000,ex+12*86400000)).fetchall()
    if len(f)<2: continue
    bef=[v for t,v in f if t<ex]; aft=[v for t,v in f if t>=ex]
    if not bef or not aft: continue
    a0,a1=bef[-1],aft[0]
    if a0<=0 or a1<=a0: continue
    k=a1/a0; gap=1-1/k
    if gap<0.10: continue
    b=d.execute("SELECT time,close FROM stock_minutes WHERE code=? AND time>=? AND time<? AND close>0 ORDER BY time",[str(code),ex-8*86400000,ex+3*86400000]).fetchall()
    if len(b)<2: continue
    daybar={}
    for t,c in b: daybar[int(t)//86400000]=float(c)
    exday=int(ex)//86400000
    before=[dd for dd in sorted(daybar) if dd<exday]
    if not before or exday not in daybar: continue
    r=daybar[exday]/daybar[before[-1]]
    # 判类
    hyp={"qfq":1.0,"raw":1/k,"double":1/(k*k)}
    best=min(hyp,key=lambda h:abs(r-hyp[h]))
    (qfq_codes if best=="qfq" else raw_codes).append(str(code))
print("hf gap>10%: qfq-class",len(qfq_codes),"raw-class",len(raw_codes))
print("qfq sample:",qfq_codes[:15])
print("raw sample:",raw_codes[:15])
# 与已知受影响集合交集
affc=set()
for pth in ("agent_workspace/_step3_affected_cells.csv",):
    pp=Path(pth)
    if pp.exists():
        with pp.open(encoding="utf-8") as fh:
            rd=csv.DictReader(fh)
            for row in rd:
                for k in ("code","ts_code","股票代码"):
                    if k in row: affc.add(str(row[k]).split(".")[0]); break
print("affected set size:",len(affc))
if affc:
    print("qfq-class ∩ affected:",len(set(qfq_codes)&affc),"/",len(qfq_codes))
    print("raw-class ∩ affected:",len(set(raw_codes)&affc),"/",len(raw_codes))
con.close(); d.close()
