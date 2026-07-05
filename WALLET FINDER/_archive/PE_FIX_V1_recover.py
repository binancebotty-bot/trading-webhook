"""PE_FIX_V1 recovery: locate and remove orphan return-dict block that was
stranded inside sync_equity after PE_FIX_V1_final.py partial application."""
import sys

PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Locate orphan start: '        return _local_env_value(key)' at 8-space indent,
# occurring AFTER sync_equity signature (line ~1673).
start = None
for i, line in enumerate(lines):
    if i >= 1600 and line.lstrip().startswith("return _local_env_value(key)"):
        start = i
        break
if start is None:
    print("FAIL: orphan return _local_env_value not found")
    sys.exit(2)
print(f"Orphan START: line {start + 1}: {lines[start].rstrip()}")

# Locate orphan end: the matching 4-space '    }' that closes the return dict.
end = None
for j in range(start + 1, min(start + 80, len(lines))):
    if lines[j].rstrip() == "    }":
        end = j + 1
        break
if end is None:
    print("FAIL: orphan closing brace not found")
    sys.exit(3)
print(f"Orphan END: line {end}: {lines[end - 1].rstrip()}")
print(f"Span: {start + 1}..{end} ({end - start} lines)")
print("--- orphan contents (first 5) ---")
for k in range(start, min(start + 5, end)):
    print(f"  {k + 1}: {lines[k].rstrip()[:120]}")
print("--- orphan contents (last 5) ---")
for k in range(max(start, end - 5), end):
    print(f"  {k + 1}: {lines[k].rstrip()[:120]}")

new_lines = lines[:start] + lines[end:]
with open(PATH, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
print(f"OK: removed {end - start} lines; total {len(lines)} -> {len(new_lines)}")
