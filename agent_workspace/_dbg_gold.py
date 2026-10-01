import sys, io; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
c=MCPClient(endpoint='https://124.223.159.234/mcp', tls_verify=False); c.handshake()
arts=c.export_dataset('stock_daily', page_size=5000000, time_start='2018-06-15', time_end='2018-06-16', async_mode=False)
df=pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes)) for a in arts], ignore_index=True)
print('列:', list(df.columns))
print('行数:', len(df))
print()
print('前 5 行原始值:')
print(df[['ts_code','trade_date','close','vol','amount']].head(5).to_string(index=False))
print()
df['implied']=df['amount']/df['vol']
df['r']=df['close']/df['implied']
print('close/(amount/vol) 分布:')
print(df['r'].describe().to_string())
print()
print('样例（含 000039）:')
s=df[df['ts_code'].astype(str).str.startswith('000039')]
if len(s): print(s[['ts_code','trade_date','close','vol','amount','implied','r']].to_string(index=False))
