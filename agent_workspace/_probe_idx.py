from pathlib import Path
p = Path("quantstudio/pipeline/sources/mcp_adapter.py")
lines = p.read_text(encoding="utf-8").splitlines(keepends=True)
# read 工具 1-based：行1935 -> idx 1934；行2046 -> idx 2045
for i in range(1930, 2050):
    s = lines[i].rstrip("\n")
    if s.strip().startswith("@staticmethod") or "_classify_raw_qfq" in s or "_bar_day_of" in s \
       or "return s.dt.strftime" in s or s.startswith("    def _restore_to_raw"):
        print(f"idx={i} (行{i+1}): {s[:70]}")
