import sys; sys.path.insert(0,".")
from quantstudio.pipeline.quality_audit import DataQualityAuditor
from quantstudio._paths import db_path
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import duckdb
p=Path("config/profiles/mcp_only/alignment_rules.json")
aud=DataQualityAuditor.from_config(db_path(), p)
con=duckdb.connect(str(db_path()), read_only=True)
aux=aud._resolve_aux_path(con)
now=datetime.now(); lo=int((now-timedelta(days=120)).timestamp()*1000); hi=int((now+timedelta(days=7)).timestamp()*1000)
rows=con.execute("SELECT DISTINCT code, ex_date FROM stock_dividend WHERE ex_date BETWEEN ? AND ? AND code IS NOT NULL",[lo,hi]).fetchall()
ex_by_code={}
for c,d in rows: ex_by_code.setdefault(str(c),set()).add(int(d))
codes=sorted(ex_by_code)
f=aud._read_factor_table(aux,"adj_factor",codes)
fby=defaultdict(list)
for c,t,v in (f or []):
    if v is None: continue
    try: fby[str(c)].append((int(t),float(v)))
    except Exception: pass
for s in fby.values(): s.sort()
n_cnt=defaultdict(int)
for code,exs in ex_by_code.items():
    ff=fby.get(code)
    if not ff or len(ff)<2: n_cnt["no_factor"]+=1; continue
    for ex in exs:
        before=[v for t,v in ff if t < ex-2*86400000]
        after=[v for t,v in ff if t >= ex-2*86400000]
        if not before or not after: n_cnt["no_split"]+=1; continue
        a0,a1=before[-1],after[0]
        if a0<=0 or a1<=0: n_cnt["bad_val"]+=1; continue
        if a1<=a0: n_cnt["no_step"]+=1; continue
        k=a1/a0; gap=1-1/k
        n_cnt["gap>=2%" if gap>=0.02 else "gap<2%"]+=1
print(dict(n_cnt))
con.close()
