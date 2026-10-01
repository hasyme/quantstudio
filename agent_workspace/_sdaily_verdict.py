import sys, io; sys.path.insert(0,'.')
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
raw=cm[cm['hm']=='15:00'][['bare','close']].rename(columns={'close':'cloud_raw'})
a2=c.export_dataset('stock_daily', page_size=5000000, time_start='2026-01-12', time_end='2026-01-13', async_mode=False)
cd=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in a2], ignore_index=True)
cd['bare']=cd['ts_code'].astype(str).str.split('.').str[0]
mg=raw.merge(cd[['bare','close','adj_factor']].rename(columns={'close':'cloud_daily'}), on='bare')
mg['r']=mg['cloud_daily']/mg['cloud_raw']
mg['af']=pd.to_numeric(mg['adj_factor'],errors='coerce')
out.append('配对: %d'%len(mg))
out.append('')
out.append('判定：若 cloud_daily == cloud_raw（严格相等），则 daily 云端 = raw')
out.append('  exact equal (1e-9): %.1f%%'%(100*(abs(mg['r']-1)<1e-9).mean()))
out.append('  within 0.5%%:        %.1f%%'%(100*(abs(mg['r']-1)<0.005).mean()))
out.append('')
out.append('反证：若 daily 是 qfq(基准=最新)，则 ratio 应 = adj_i/adj_latest')
mg['qfq_pred']=mg['af']/mg['af'].max()
mg['raw_pred']=1.0
err_raw=(mg['r']-1).abs().median()
err_qfq=(mg['r']-mg['af']/mg['af'].max()).abs().median()
out.append('  与 raw 假设 中位误差: %.4f'%err_raw)
out.append('  与 qfq 假设 中位误差: %.4f'%err_qfq)
out.append('  -> 更接近: %s'%('RAW' if err_raw<err_qfq else 'QFQ'))
out.append('')
out.append('adj_factor 分布: min=%.4f max=%.4f median=%.4f'%(mg['af'].min(),mg['af'].max(),mg['af'].median()))
open('agent_workspace/_sdaily_verdict.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
