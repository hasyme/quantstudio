import sys, io, duckdb; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
# 云端 minutes 2026-01-12（raw）
arts=c.export_dataset('stock_minutes', page_size=5000000, row_limit=5000000,
                      time_start='2026-01-12', time_end='2026-01-13', async_mode=False)
cm=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in arts], ignore_index=True)
cm['bare']=cm['ts_code'].astype(str).str.split('.').str[0]
cm['hm']=cm['trade_time'].astype(str).str[-8:-3]
cm['ms']=pd.to_datetime(cm['trade_time']).astype('int64')//10**6 - 8*3600*1000  # CST->UTC ms
cloud1500=cm[cm['hm']=='15:00'][['bare','ms','close']].rename(columns={'close':'cloud_raw'})
d=duckdb.connect('data/quantstudio.db', read_only=True)
loc=d.execute('''SELECT code, time, close, update_time FROM stock_minutes
 WHERE (time%86400000)=25200000 AND time>=? AND time<?''',
 [int(datetime(2026,1,12,tzinfo=CST).timestamp()*1000), int(datetime(2026,1,13,tzinfo=CST).timestamp()*1000)]).fetchdf()
d.close()
loc.columns=['bare','ms','local_close','upd']
mg=cloud1500.merge(loc,on=['bare','ms'],how='inner')
mg['ratio']=mg['local_close']/mg['cloud_raw']
out=[]
out.append('pairs=%d'%len(mg))
out.append('local_minutes / cloud_minutes_raw:')
out.append('  median=%.6f  ==1:%.1f%%  >1.05:%.1f%%  <0.95:%.1f%%'%(
  mg['ratio'].median(),100*(abs(mg['ratio']-1)<1e-6).mean(),
  100*(mg['ratio']>1.05).mean(),100*(mg['ratio']<0.95).mean()))
out.append('')
out.append('update_time 分布:')
out.append(mg['upd'].astype(str).str[:10].value_counts().head(5).to_string())
open('agent_workspace/_a3_baseline.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
