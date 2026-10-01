"""B1 复测：模拟真实调用路径（含 open_ro_sqlite 连接开销），多次取中位数。"""
import sys, time, statistics; sys.path.insert(0,'.')
import pandas as pd, sqlite3
from quantstudio.pipeline.qfq_invariant import _load_factor_lookup, _factor_query_window_ms
out=[]
AUX='data/qfq_aux.db'
codes=[r[0] for r in sqlite3.connect(AUX).execute('SELECT DISTINCT code FROM adj_factor LIMIT 555').fetchall()]
lo,hi=_factor_query_window_ms(pd.Series(['2026-01-12','2026-01-13']))
def bench(win, n=3):
    ts=[]
    for _ in range(n):
        con=sqlite3.connect(AUX); con.execute('PRAGMA query_only=ON')
        t0=time.time()
        if win: _load_factor_lookup(con,'adj_factor',codes,time_lo_ms=lo,time_hi_ms=hi)
        else:   _load_factor_lookup(con,'adj_factor',codes)
        ts.append(time.time()-t0); con.close()
    return statistics.median(ts)
t_old=bench(False); t_new=bench(True)
out.append('=== B1 复测（3 次取中位数，含连接开销）===')
out.append('old: %.2f s'%t_old)
out.append('new: %.3f s'%t_new)
out.append('加速比: %.1fx'%(t_old/max(t_new,0.0001)))
out.append('')
out.append('说明：单片实际调用点已持有连接（open_ro_sqlite 在外层），')
out.append('      故真实单片耗时应按"查询净耗时"计——')
# 净查询耗时（复用连接）
con=sqlite3.connect(AUX); con.execute('PRAGMA query_only=ON')
t0=time.time(); _load_factor_lookup(con,'adj_factor',codes); q_old=time.time()-t0
t0=time.time(); _load_factor_lookup(con,'adj_factor',codes,time_lo_ms=lo,time_hi_ms=hi); q_new=time.time()-t0
out.append('净查询 old: %.2f s   new: %.3f s   加速: %.1fx'%(q_old,q_new,q_old/max(q_new,0.0001)))
con.close()
open('agent_workspace/_b1_retest.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
