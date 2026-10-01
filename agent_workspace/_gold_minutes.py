import sys, io, sqlite3; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
aux=sqlite3.connect('data/qfq_aux.db'); aux.execute('PRAGMA query_only=ON')
for tbl,ds,ftbl,d0,d1 in (('stock_minutes','stock_minutes','adj_factor','2026-06-15','2026-06-16'),
                          ('etf_minutes','etf_minutes','fund_adj','2026-06-15','2026-06-16')):
    out.append('=== %s (%s) ==='%(tbl,d0))
    try:
        arts=c.export_dataset(ds, page_size=5000000, row_limit=5000000,
                              time_start=d0, time_end=d1, async_mode=False)
    except Exception as e:
        out.append('  fetch fail: %s'%str(e)[:70]); out.append(''); continue
    if not arts:
        out.append('  empty'); out.append(''); continue
    df=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in arts], ignore_index=True)
    vcol='vol' if 'vol' in df.columns else 'volume'
    df['bare']=df['ts_code'].astype(str).str.split('.').str[0]
    df['c']=pd.to_numeric(df['close'],errors='coerce')
    df['v']=pd.to_numeric(df[vcol],errors='coerce')
    df['a']=pd.to_numeric(df['amount'],errors='coerce')
    df=df[(df['v']>0)&(df['a']>0)&(df['c']>0)].copy()
    df['implied']=df['a']/df['v']*10.0
    df['_t']=df['trade_time'].astype(str)
    df=df.sort_values(['bare','_t']).groupby('bare',as_index=False).tail(1)
    t0ms=int(datetime.strptime(d0,'%Y-%m-%d').replace(tzinfo=CST).timestamp()*1000)
    lat={}; cur={}
    for x in df['bare'].unique():
        r=aux.execute(f'SELECT adj_factor FROM {ftbl} WHERE code=? ORDER BY time DESC LIMIT 1',(x,)).fetchone()
        lat[x]=r[0] if r else None
        r2=aux.execute(f'SELECT adj_factor FROM {ftbl} WHERE code=? AND time<=? ORDER BY time DESC LIMIT 1',(x,t0ms)).fetchone()
        cur[x]=r2[0] if r2 else None
    df['al']=df['bare'].map(lat); df['ai']=df['bare'].map(cur)
    df=df.dropna(subset=['al','ai']); df=df[df['ai']>0]
    if df.empty: out.append('  no factor pairs'); out.append(''); continue
    df['err_raw']=(df['c']-df['implied']).abs()/df['implied']
    df['err_qfq']=(df['c']*df['al']/df['ai']-df['implied']).abs()/df['implied']
    out.append('  n=%d  err_raw=%.6f  err_qfq=%.6f  -> %s'%(len(df),
               df['err_raw'].median(),df['err_qfq'].median(),
               'RAW' if df['err_raw'].median()<df['err_qfq'].median() else 'QFQ'))
    big=df[(df['al']/df['ai']-1).abs()>0.05]
    if len(big)>=5:
        out.append('  [big n=%d] err_raw=%.6f err_qfq=%.6f -> %s'%(len(big),
                   big['err_raw'].median(),big['err_qfq'].median(),
                   'RAW' if big['err_raw'].median()<big['err_qfq'].median() else 'QFQ'))
    out.append('')
aux.close()
open('agent_workspace/_gold_minutes.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
