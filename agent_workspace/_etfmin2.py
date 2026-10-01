import sys, io, sqlite3; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
# 扩大窗口到 1 个月，取更多除权样本
a=c.export_dataset('etf_minutes', page_size=5000000, row_limit=5000000,
                   time_start='2026-01-01', time_end='2026-02-01', async_mode=False)
em=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in a], ignore_index=True)
out.append('etf_minutes 行数: %d'%len(em))
em['bare']=em['ts_code'].astype(str).str.split('.').str[0]
em['hm']=em['trade_time'].astype(str).str[-8:-3]
em['day']=em['trade_time'].astype(str).str[:10]
m=em[em['hm']=='15:00'][['bare','day','close','adj_factor']].copy()
m['af']=pd.to_numeric(m['adj_factor'],errors='coerce')
m=m.sort_values(['bare','day'])
m['r']=m.groupby('bare')['close'].pct_change()+1
m['afr']=m.groupby('bare')['af'].pct_change()+1
sub=m.dropna(subset=['r','afr'])
sub=sub[(sub['afr']-1).abs()>0.002]
out.append('因子变化样本数: %d'%len(sub))
if len(sub)>=10:
    err_raw=(sub['r']-1/sub['afr']).abs().median()
    err_qfq=(sub['r']-1).abs().median()
    out.append('raw 假设误差=%.5f  qfq 假设误差=%.5f -> %s'%(err_raw,err_qfq,'RAW' if err_raw<err_qfq else 'QFQ'))
    out.append('')
    out.append('样例（应看 r 是否≈1/afr）:')
    out.append(sub[['bare','day','r','afr']].head(6).to_string(index=False))
open('agent_workspace/_etfmin2.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
