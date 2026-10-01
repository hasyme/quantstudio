import sys, io, duckdb; sys.path.insert(0,'.')
import pandas as pd, sqlite3
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
# 云端 minutes（已确证 raw）作为独立基准
arts=c.export_dataset('stock_minutes', page_size=5000000, row_limit=5000000,
                      time_start='2026-01-12', time_end='2026-01-13', async_mode=False)
cm=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in arts], ignore_index=True)
cm['bare']=cm['ts_code'].astype(str).str.split('.').str[0]
cm['hm']=cm['trade_time'].astype(str).str[-8:-3]
raw=cm[cm['hm']=='15:00'][['bare','close']].rename(columns={'close':'cloud_raw'})
# 云端 stock_daily
a2=c.export_dataset('stock_daily', page_size=5000000, time_start='2026-01-12', time_end='2026-01-13', async_mode=False)
cd=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in a2], ignore_index=True)
cd['bare']=cd['ts_code'].astype(str).str.split('.').str[0]
mg=raw.merge(cd[['bare','close','adj_factor']].rename(columns={'close':'cloud_daily'}), on='bare')
mg['r']=mg['cloud_daily']/mg['cloud_raw']
out.append('=== 云端 stock_daily 口径判定（以云端 minutes raw 为独立基准）===')
out.append('配对: %d'%len(mg))
out.append('cloud_daily/cloud_raw: median=%.6f  ==1:%.1f%%'%(mg['r'].median(),100*(abs(mg['r']-1)<1e-6).mean()))
out.append('')
out.append('按 adj_factor 分组看比值:')
mg['af']=pd.to_numeric(mg['adj_factor'],errors='coerce')
for lo,hi,lab in ((0,1.0001,'adj==1.0'),(1.0001,1.05,'1.0<adj<=1.05'),(1.05,1.5,'1.05<adj<=1.5'),(1.5,1e9,'adj>1.5')):
    s=mg[(mg['af']>lo)&(mg['af']<=hi)]
    if len(s): out.append('  %-16s n=%-6d ratio_median=%.4f  ==1:%.1f%%'%(lab,len(s),s['r'].median(),100*(abs(s['r']-1)<1e-6).mean()))
open('agent_workspace/_sdaily_caliber.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
