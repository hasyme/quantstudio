import sys, io, sqlite3; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
# P5：etf_minutes 扩样复核（跨多季度，目标 >=200 因子变化样本）
# 方法：跨除权日连续性法（独立于任何本地表）
# 取 3 个季度各一段
segs=[('2025-10-01','2025-11-01'),('2026-01-01','2026-02-01'),('2026-05-01','2026-06-01')]
frames=[]
for s,e in segs:
    try:
        a=c.export_dataset('etf_minutes', page_size=5000000, row_limit=5000000,
                           time_start=s, time_end=e, async_mode=False)
        df=pd.concat([pd.read_parquet(io.BytesIO(x.parquet_bytes)) for x in a], ignore_index=True)
        frames.append(df)
        out.append('%s~%s: %d 行'%(s,e,len(df)))
    except Exception as ex:
        out.append('%s~%s FAIL: %s'%(s,e,str(ex)[:80]))
if frames:
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
    sub=sub[(sub['afr']-1).abs()>0.002]
    out.append('')
    out.append('=== P5 扩样复核（etf_minutes）===')
    out.append('因子变化样本数: %d'%len(sub))
    if len(sub)>=20:
        er=(sub['r']-1/sub['afr']).abs().median()
        eq=(sub['r']-1).abs().median()
        out.append('raw 假设误差=%.5f  qfq 假设误差=%.5f -> %s'%(er,eq,'RAW' if er<eq else 'QFQ'))
        out.append('覆盖 code 数: %d'%sub['bare'].nunique())
open('agent_workspace/_p5_etfmin.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
