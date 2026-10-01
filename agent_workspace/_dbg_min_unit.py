import sys, io; sys.path.insert(0,'.')
import pandas as pd
from quantstudio.pipeline.mcp.client import MCPClient
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
arts=c.export_dataset('stock_minutes', page_size=5000000, row_limit=5000000,
                      time_start='2026-06-15', time_end='2026-06-16', async_mode=False)
df=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in arts], ignore_index=True)
print('cols:', list(df.columns))
print('rows:', len(df))
d=df[df['ts_code'].astype(str).str.startswith('000001')]
print()
print('000001 尾部 3 行:')
print(d[['ts_code','trade_time','close','vol','amount']].tail(3).to_string(index=False))
print()
d2=d.copy(); d2['imp_raw']=d2['amount']/d2['vol']; d2['imp_x10']=d2['amount']/d2['vol']*10
print('close/implied(no scale) 中位: %.4f'%(d2['close']/d2['imp_raw']).median())
print('close/implied(x10)      中位: %.4f'%(d2['close']/d2['imp_x10']).median())
