import sys, io; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
frames=[]
for s,e in [('2025-10-01','2025-11-01'),('2026-01-01','2026-02-01'),('2026-05-01','2026-06-01')]:
    a=c.export_dataset('etf_minutes', page_size=5000000, row_limit=5000000,
                       time_start=s, time_end=e, async_mode=False)
    frames.append(pd.concat([pd.read_parquet(io.BytesIO(x.parquet_bytes)) for x in a], ignore_index=True))
em=pd.concat(frames, ignore_index=True)
em['bare']=em['ts_code'].astype(str).str.split('.').str[0]
em['hm']=em['trade_time'].astype(str).str[-8:-3]
em['day']=em['trade_time'].astype(str).str[:10]
m=em[em['hm']=='15:00'][['bare','day','close','adj_factor']].copy()
m['af']=pd.to_numeric(m['adj_factor'],errors='coerce')
m=m.sort_values(['bare','day']).drop_duplicates(['bare','day'])
m['r']=m.groupby('bare')['close'].pct_change()+1
m['afr']=m.groupby('bare')['af'].pct_change()+1
sub=m.dropna(subset=['r','afr'])
sub['amp']=(sub['afr']-1).abs()
out.append('all factor-change samples: %d'%len(sub))
out.append('')
out.append('%-12s %-8s %-12s %-12s %s'%('amp','n','err_raw','err_qfq','verdict'))
for lo,hi,lab in ((0.002,0.01,'0.2-1pct'),(0.01,0.03,'1-3pct'),(0.03,0.10,'3-10pct'),(0.10,10,'gt10pct')):
    s2=sub[(sub['amp']>=lo)&(sub['amp']<hi)]
    if len(s2)<3:
        out.append('%-12s %-8d (too few)'%(lab,len(s2))); continue
    er=(s2['r']-1/s2['afr']).abs().median()
    eq=(s2['r']-1).abs().median()
    out.append('%-12s %-8d %-12.5f %-12.5f %s'%(lab,len(s2),er,eq,'RAW' if er<eq else 'QFQ'))
open('agent_workspace/_p5_layer.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
