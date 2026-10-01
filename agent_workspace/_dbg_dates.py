from datetime import datetime, timezone, timedelta
CST=timezone(timedelta(hours=8))
for ms in (1780329600000, 1785168000000, 1784217600000, 1782144000000, 1781020800000):
    print(ms, datetime.fromtimestamp(ms/1000, CST).strftime("%Y-%m-%d %H:%M CST"))
print("--- now ---")
print(datetime.now(CST))
