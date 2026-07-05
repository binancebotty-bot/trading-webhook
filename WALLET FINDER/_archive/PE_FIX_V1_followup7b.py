"""PE_FIX_V1 final fix: re-indent sync_equity to 4 spaces, deduplicate comments,
ensure `global _PE_FIX_V1_RESET_DONE` is declared at the top of the function body."""
import re, sys

PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

# (1) Indent `def sync_equity(self) -> None:` to 4 spaces (it's inside WalletModel class).
m = re.search(r"(^|\n)(def sync_equity\(self\) -> None:\n)", content)
if m:
    new_content = content[:m.start(2)] + "    " + m.group(2) + content[m.end(2):]
    if new_content != content:
        content = new_content
        print("OK: def sync_equity indented to 4 spaces")
else:
    print("INFO: indented def sync_equity already")

# (2) Find sync_equity body (after indented header)
m = re.search(r"\n    def sync_equity\(self\) -> None:\n", content)
if not m:
    print("FAIL: cannot find indented sync_equity", file=sys.stderr)
    sys.exit(2)
body_start = m.end()
# Find next method (4-space-indented def) or class boundary
scan = body_start
end = len(content)
while scan < len(content):
    nl = content.find("\n", scan)
    if nl < 0:
        break
    line_start = nl + 1
    if content[line_start:line_start + 8] == "    def ":
        end = line_start
        break
    scan = nl + 1

body = content[body_start:end]

# (3) Deduplicate "    # One-shot reset..." comments (keep one canonical comment)
body = re.sub(
    r"(?:    # One-shot reset[^\n]*\n)+",
    "    # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1).\n",
    body,
)
print("OK: comments dedup'd")

# (4) Ensure `global _PE_FIX_V1_RESET_DONE` is at top of body, AFTER the first comment.
global_decl = "    # PE_FIX_V1: declare module-level reset flag as global (read+write).\n    global _PE_FIX_V1_RESET_DONE\n"
if "global _PE_FIX_V1_RESET_DONE" in body[:400]:
    print("INFO: global already at top of body")
else:
    # If a global exists elsewhere, remove first occurrence and prepend.
    body_wo_global = re.sub(r"\n    global _PE_FIX_V1_RESET_DONE\n", "\n", body, count=1)
    # Prepend at very top of body (just after the function header).
    body = global_decl + body_wo_global.lstrip("\n")
    print("OK: global prepended to body top")

# Splice back
new_content = content[:body_start] + body + content[end:]
with open(PATH, "w", encoding="utf-8") as f:
    f.write(new_content)

print(f"DONE ({len(content)} -> {len(new_content)} bytes)")
