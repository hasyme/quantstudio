"""A1 强化：验证"被消费的键"完全一致（等价性核心）。
关键：消费点 = factor_lookup.get((code, day))，day 来自抽样行 bar_day。
断言：所有 (code, day) 消费键在 old/new 中取值完全相同。
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
# 模拟 check_qfq_invariant 的抽样（固定 seed）
sampled=_stratified_sample(df, seed=42)
days=_bar_day_from_ms(sampled['time'].astype('int64'))
lo,hi=_factor_query_window_ms(days)
lk_old=_load_factor_lookup(con,'adj_factor',sorted(sampled['code'].astype(str).unique()))
lk_new=_load_factor_lookup(con,'adj_factor',sorted(sampled['code'].astype(str).unique()),time_lo_ms=lo,time_hi_ms=hi)
# 构造真实消费键集合（与 check_qfq_invariant 内部一致）
consumed=set()
for idx,row in sampled.iterrows():
    consumed.add((str(row['code']), days.loc[idx]))
out.append('=== A1 强化：消费键等价性 ===')
out.append('抽样行数: %d'%len(sampled))
out.append('消费键数: %d'%len(consumed))
out.append('')
miss_old=[k for k in consumed if k not in lk_old]
miss_new=[k for k in consumed if k not in lk_new]
out.append('消费键缺失: old=%d  new=%d'%(len(miss_old),len(miss_new)))
val_diff=[k for k in consumed if k in lk_old and k in lk_new and abs(lk_old[k]-lk_new[k])>1e-12]
out.append('消费键取值差异: %d'%len(val_diff))
out.append('')
out.append('VERDICT: %s'%('EQUIVALENT (消费键完全一致)' if (not miss_new and not val_diff and len(miss_old)==0) else 'NOT EQUIVALENT'))
out.append('')
out.append('说明: old 含 %d 键, new 含 %d 键; 差集 %d 键**从未被消费**'%(
    len(lk_old),len(lk_new),len(lk_old)-len(lk_new)))
open('agent_workspace/_a1_strict.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
con.close()
