from pathlib import Path
p = Path("quantstudio/pipeline/sources/mcp_adapter.py")
lines = p.read_text(encoding="utf-8").splitlines(keepends=True)
assert lines[1934].strip() == "@staticmethod"
assert "_classify_raw_qfq" in lines[1935]
assert lines[2044].strip() == 'return s.dt.strftime("%Y-%m-%d")'
assert lines[2046].strip().startswith("def _restore_to_raw")
del lines[1934:2046]      # 删 idx 1934..2045（行 1935..2046）
p.write_text("".join(lines), encoding="utf-8")
print("已删除 3 个辅助方法（原行 1935-2046）")
