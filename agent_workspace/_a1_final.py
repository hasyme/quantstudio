"""A1 强化（修正判据）：等价性 = old/new 的"消费键缺失集合"与"取值"完全相同。
既有缺失（如 aux 表无该日因子）不是优化引入的，只要 old/new 一致即等价。
"""
import sys; sys.path.insert(0,'.')
import pandas as pd, sqlite3
from quantstudio.pipeline.qfq_invariant import (
    _load_factor_lookup, _factor_query_window_ms, _bar_day_from_ms, _stratified_sample)
out=[]
AUX='data/qfq_aux.db'
con=sqlite3.connect(AUX); con.execute('PRAGMA query_only=ON')
codes=[r[0] for r in con.execute('SELECT DISTINCT code FROM adj_factor LIMIT 40').fetchall()]
rows=[]
for c in codes:
    for d,base in (('2026-01-12',10.0),('2026-01-13',10.2)):
        t=int(pd.Timestamp(d,tz='Asia/Shanghai').timestamp()*1000)+15*3600*1000
        rows.append({'code':c,'time':t,'close':base})
df=pd.DataFrame(rows)
sampled=_stratified_sample(df, seed=42)
days=_bar_day_from_ms(sampled['time'].astype('int64'))
lo,hi=_factor_query_window_ms(days)
codelist=sorted(sampled['code'].astype(str).unique())
lk_old=_load_factor_lookup(con,'adj_factor',codelist)
lk_new=_load_factor_lookup(con,'adj_factor',codelist,time_lo_ms=lo,time_hi_ms=hi)
consumed=set()
for idx,row in sampled.iterrows():
    consumed.add((str(row['code']), days.loc[idx]))
miss_old={k for k in consumed if k not in lk_old}
miss_new={k for k in consumed if k not in lk_new}
val_diff=[k for k in consumed if k in lk_old and k in lk_new and abs(lk_old[k]-lk_new[k])>1e-12]
out.append('=== A1 强化（修正判据）===')
out.append('消费键数: %d'%len(consumed))
out.append('缺失集合相同: %s  (old=%d, new=%d)'%(miss_old==miss_new,len(miss_old),len(miss_new)))
out.append('取值差异数: %d'%len(val_diff))
out.append('')
ok = (miss_old==miss_new) and (len(val_diff)==0)
out.append('VERDICT: %s'%('EQUIVALENT' if ok else 'NOT EQUIVALENT'))
out.append('')
out.append('未消费键（old 有 new 无）: %d 个 —— 优化仅移除这些'%(len(lk_old)-len(lk_new)))
out.append('')
out.append('缺失样例（aux 表无该日因子，old/new 一致缺失）:')
for k in sorted(miss_old)[:5]: out.append('   %s'%(k,))
open('agent_workspace/_a1_final.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
con.close()
