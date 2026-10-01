import sys; sys.path.insert(0,".")
from quantstudio.pipeline.quality_audit import DataQualityAuditor
from quantstudio._paths import db_path
from pathlib import Path
import duckdb
from datetime import datetime, timedelta
p = Path("config/profiles/mcp_only/alignment_rules.json")
aud = DataQualityAuditor.from_config(db_path(), p)
con = duckdb.connect(str(db_path()), read_only=True)
aux = aud._resolve_aux_path(con)
print("aux:", aux)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows = con.execute("SELECT DISTINCT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
codes = sorted(set(str(r[0]) for r in rows))
print("cand codes:", len(codes), "example:", codes[:5])
f = aud._read_factor_table(aux, "adj_factor", codes)
print("factor rows:", len(f) if f else None)
from collections import defaultdict
fby=defaultdict(list)
for c,t,v in (f or []):
    if v is None: continue
    fby[str(c)].append((int(t),float(v)))
for s in fby.values(): s.sort()
nstep=0; ngap=0
for code,ex in [(str(a),int(b)) for a,b in rows]:
    seq=fby.get(code)
    if not seq or len(seq)<2: continue
    before=[v for t,v in seq if t < ex-2*86400000]
    after=[v for t,v in seq if t >= ex-2*86400000]
    if not before or not after: continue
    a0,a1=before[-1],after[0]
    if a0<=0 or a1<=a0: continue
    nstep+=1
    k=a1/a0
    if 1-1/k >= 0.02: ngap+=1
print("probes with factor step (a1>a0):", nstep, " with gap>=2%:", ngap)
# 看看典型 a1/a0
cnt=0
for code,ex in [(str(a),int(b)) for a,b in rows][:8]:
    seq=fby.get(code)
    if not seq: print(code,"no factor"); continue
    print(code,"ex",ex,"nfactor",len(seq),"sample",seq[:3])
con.close()
