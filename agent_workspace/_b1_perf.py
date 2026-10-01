"""B1 性能验证：优化前后单次查询耗时对比（真实 aux 库，555 code）"""
import sys, time; sys.path.insert(0,'.')
import pandas as pd, sqlite3
from quantstudio.pipeline.qfq_invariant import _load_factor_lookup, _factor_query_window_ms
out=[]
AUX='data/qfq_aux.db'
con=sqlite3.connect(AUX); con.execute('PRAGMA query_only=ON')
codes=[r[0] for r in con.execute('SELECT DISTINCT code FROM adj_factor LIMIT 555').fetchall()]
days=pd.Series(['2026-01-12','2026-01-13'])
lo,hi=_factor_query_window_ms(days)
out.append('=== B1 性能验证（555 code）===')
out.append('窗口: %s .. %s'%(pd.to_datetime(lo,unit='ms',utc=True).tz_convert('Asia/Shanghai'),
                             pd.to_datetime(hi,unit='ms',utc=True).tz_convert('Asia/Shanghai')))
# 旧行为（无窗口）
t0=time.time(); lk_old=_load_factor_lookup(con,'adj_factor',codes); t_old=time.time()-t0
# 新行为（带窗口）
t0=time.time(); lk_new=_load_factor_lookup(con,'adj_factor',codes,time_lo_ms=lo,time_hi_ms=hi); t_new=time.time()-t0
out.append('')
out.append('old(全历史): %.2f s, 键数=%d'%(t_old,len(lk_old)))
out.append('new(时间窗): %.3f s, 键数=%d'%(t_new,len(lk_new)))
out.append('加速比: %.1fx'%(t_old/max(t_new,0.0001)))
out.append('')
out.append('B1 通过(>=100x): %s'%(t_old/max(t_new,0.0001)>=100))
open('agent_workspace/_b1_perf.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
con.close()
