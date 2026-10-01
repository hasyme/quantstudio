import sys, io, sqlite3; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
# 判定 etf_minutes / stock_minutes 口径：用"跨除权日连续性"法（N1 法，独立于任何表）
# 原理：若为 raw，跨除权日 close 会跳变(×adj_i/adj_{i-1})；若为 qfq，则连续
con=sqlite3.connect('data/qfq_aux.db'); con.execute('PRAGMA query_only=ON')
a2=c.export_dataset('etf_minutes', page_size=5000000, row_limit=5000000,
                    time_start='2026-01-12', time_end='2026-01-14', async_mode=False)
em=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in a2], ignore_index=True)
em['bare']=em['ts_code'].astype(str).str.split('.').str[0]
em['hm']=em['trade_time'].astype(str).str[-8:-3]
em['day']=em['trade_time'].astype(str).str[:10]
m=em[em['hm']=='15:00'][['bare','day','close','adj_factor']].copy()
m['af']=pd.to_numeric(m['adj_factor'],errors='coerce')
m['r']=m.groupby('bare')['close'].pct_change()+1
m['afr']=m.groupby('bare')['af'].pct_change()+1
# 若 raw: r ≈ 1/afr  (raw 跨日比值 = qfq比值 × adj_i/adj_{i-1} 的倒数关系)
# 若 qfq: r ≈ 1
sub=m.dropna(subset=['r','afr'])
sub=sub[(sub['afr']-1).abs()>0.002]   # 只取因子有变化的
if len(sub):
    err_raw=(sub['r']-1/sub['afr']).abs().median()
    err_qfq=(sub['r']-1).abs().median()
    out.append('=== etf_minutes 口径判定（跨日连续性法，%d 样本）==='%len(sub))
    out.append('  raw 假设误差=%.5f   qfq 假设误差=%.5f  -> %s'%(err_raw,err_qfq,'RAW' if err_raw<err_qfq else 'QFQ'))
else:
    out.append('etf_minutes: 因子变化样本不足')
con.close()
open('agent_workspace/_etfmin_caliber.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
