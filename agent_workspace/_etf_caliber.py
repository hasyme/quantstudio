import sys, io, sqlite3; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
# 用独立基准复核 etf_daily：以 etf_minutes 为基准（需先确证其口径）
# 先取 etf_daily 与 etf_minutes 云端
try:
    a1=c.export_dataset('etf_daily', page_size=5000000, time_start='2026-01-12', time_end='2026-01-13', async_mode=False)
    ed=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in a1], ignore_index=True)
    ed['bare']=ed['ts_code'].astype(str).str.split('.').str[0]
    out.append('etf_daily 云端行数: %d'%len(ed))
    out.append('  cols: %s'%list(ed.columns)[:12])
except Exception as e:
    out.append('etf_daily 取数失败: %s'%str(e)[:120])
try:
    a2=c.export_dataset('etf_minutes', page_size=5000000, row_limit=5000000, time_start='2026-01-12', time_end='2026-01-13', async_mode=False)
    em=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in a2], ignore_index=True)
    em['bare']=em['ts_code'].astype(str).str.split('.').str[0]
    em['hm']=em['trade_time'].astype(str).str[-8:-3]
    out.append('etf_minutes 云端行数: %d'%len(em))
    m1500=em[em['hm']=='15:00'][['bare','close']].rename(columns={'close':'em_raw'})
    mg=ed[['bare','close','adj_factor']].rename(columns={'close':'ed_close'}).merge(m1500,on='bare')
    if len(mg):
        mg['r']=mg['ed_close']/mg['em_raw']
        mg['af']=pd.to_numeric(mg['adj_factor'],errors='coerce')
        out.append('')
        out.append('=== etf_daily vs etf_minutes(基准) ===')
        out.append('配对=%d  ratio_median=%.6f  ==1:%.1f%%'%(len(mg),mg['r'].median(),100*(abs(mg['r']-1)<1e-6).mean()))
        err_raw=(mg['r']-1).abs().median()
        err_qfq=(mg['r']-mg['af']/mg['af'].max()).abs().median()
        out.append('  raw 假设误差=%.4f  qfq 假设误差=%.4f  -> %s'%(err_raw,err_qfq,'RAW' if err_raw<err_qfq else 'QFQ'))
except Exception as e:
    out.append('etf_minutes 取数失败: %s'%str(e)[:120])
open('agent_workspace/_etf_caliber.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
