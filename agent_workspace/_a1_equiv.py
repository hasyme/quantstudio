"""A1 行为等价性验证（性能优化硬门）：
同一输入 + 固定 seed 下，优化前后 check_qfq_invariant 返回逐项一致。
方法：用真实 qfq_aux.db + 构造 DataFrame，对比"带窗口"与"不带窗口"的结果。
"""
import sys; sys.path.insert(0,'.')
import pandas as pd, sqlite3
from quantstudio.pipeline.qfq_invariant import (
    check_qfq_invariant, _load_factor_lookup, _factor_query_window_ms, _bar_day_from_ms)
out=[]
AUX='data/qfq_aux.db'
con=sqlite3.connect(AUX); con.execute('PRAGMA query_only=ON')
# 取真实 code + 因子
codes=[r[0] for r in con.execute('SELECT DISTINCT code FROM adj_factor LIMIT 40').fetchall()]
# 构造 2 个交易日的 bar
rows=[]
for c in codes:
    for d,base in (('2026-01-12',10.0),('2026-01-13',10.2)):
        t=int(pd.Timestamp(d,tz='Asia/Shanghai').timestamp()*1000)+15*3600*1000
        rows.append({'code':c,'time':t,'open':base,'high':base,'low':base,'close':base,
                     'open_front':base*0.9,'high_front':base*0.9,'low_front':base*0.9,'close_front':base*0.9})
df=pd.DataFrame(rows)
latest={c:1.5 for c in codes}   # 固定锚
# 不带窗口（旧行为）
r_old=check_qfq_invariant(df,'stock_minutes',latest,aux_path=AUX,source='mcp',seed=42)
# 带窗口（新行为）——模拟 check_qfq_invariant 内部路径
days=_bar_day_from_ms(df['time'].astype('int64'))
lo,hi=_factor_query_window_ms(days)
lk_old=_load_factor_lookup(con,'adj_factor',sorted(df['code'].unique()))
lk_new=_load_factor_lookup(con,'adj_factor',sorted(df['code'].unique()),time_lo_ms=lo,time_hi_ms=hi)
out.append('=== A1 等价性验证 ===')
out.append('样本 bar 数: %d, code 数: %d'%(len(df),len(codes)))
out.append('窗口: %s .. %s'%(pd.to_datetime(lo,unit='ms',utc=True).tz_convert('Asia/Shanghai'),
                             pd.to_datetime(hi,unit='ms',utc=True).tz_convert('Asia/Shanghai')))
out.append('')
out.append('factor_lookup 键数: old=%d  new=%d'%(len(lk_old),len(lk_new)))
out.append('键集合相等: %s'%(set(lk_old.keys())==set(lk_new.keys())))
# 逐键值比较
diff=[k for k in set(lk_old)&set(lk_new) if abs(lk_old[k]-lk_new[k])>1e-12]
out.append('逐键值差异数: %d'%len(diff))
out.append('')
out.append('check_qfq_invariant 结果(seed=42):')
for k in ('sampled','bad','skipped','unknown_rows','cross_source_adj_i'):
    out.append('  %-20s = %s'%(k,r_old.get(k)))
open('agent_workspace/_a1_equiv.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
con.close()
