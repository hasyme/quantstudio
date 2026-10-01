import sys, io, duckdb; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
arts=c.export_dataset('stock_minutes', page_size=5000000, row_limit=5000000,
                      time_start='2026-01-12', time_end='2026-01-13', async_mode=False)
cm=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in arts], ignore_index=True)
cm['bare']=cm['ts_code'].astype(str).str.split('.').str[0]
cm['hm']=cm['trade_time'].astype(str).str[-8:-3]
m1500=cm[cm['hm']=='15:00'][['bare','close']].rename(columns={'close':'cloud_raw'})
d=duckdb.connect('data/quantstudio.db', read_only=True)
day=int((datetime(2026,1,12,tzinfo=CST).timestamp()*1000+28800000)//86400000)
loc=d.execute('SELECT code, close FROM stock_daily WHERE (time+28800000)//86400000=?',[day]).fetchdf()
d.close(); loc.columns=['bare','local_daily']
mg=m1500.merge(loc,on='bare')
mg['ratio']=mg['local_daily']/mg['cloud_raw']
out.append('pairs=%d'%len(mg))
out.append('local_daily / cloud_minutes_raw: median=%.4f  ==1:%.1f%%  >1.05:%.1f%%'%(
    mg['ratio'].median(),100*(abs(mg['ratio']-1)<1e-6).mean(),100*(mg['ratio']>1.05).mean()))
# 反推：本地 daily 是否 = cloud_raw * adj_latest/adj_i
out.append('')
out.append('sample:')
out.append(mg.head(6).to_string(index=False))
open('agent_workspace/_baseline_check.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
