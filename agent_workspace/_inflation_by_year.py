import sys, io, duckdb, sqlite3; sys.path.insert(0,'.')
import pandas as pd
from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
from quantstudio.pipeline.mcp.client import MCPClient
out=[]
# 用因子比值推断放大程度：ratio = adj_latest/adj_i（越大越被放大）
con=sqlite3.connect('data/qfq_aux.db'); con.execute('PRAGMA query_only=ON')
# 取若干年份的 adj_i 与 adj_latest，估算放大倍数
rows=con.execute('''SELECT code, time, adj_factor FROM adj_factor
 WHERE code IN ('000001','600000','000002','600519','000858') ORDER BY code, time''').fetchall()
df=pd.DataFrame(rows, columns=['code','time','af'])
df['year']=pd.to_datetime(df['time'],unit='ms',utc=True).dt.tz_convert('Asia/Shanghai').dt.year
out.append('=== 各年份因子中位数与放大倍数估算（ratio=adj_latest/adj_i）===')
out.append('%-8s %-12s %-12s %s'%('year','adj_median','adj_latest','放大倍数'))
for y,g in df.groupby('year'):
    lat=df[df['code'].isin(g['code'])].groupby('code')['af'].last()
    med=g.groupby('code')['af'].median()
    r=(lat/med).median()
    out.append('%-8d %-12.4f %-12.4f %.4f'%(y,med.median(),lat.median(),r))
con.close()
open('agent_workspace/_inflation_by_year.txt','w',encoding='utf-8').write('\n'.join(out))
print('\n'.join(out))
