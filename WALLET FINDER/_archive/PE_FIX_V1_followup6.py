"""Final PE_FIX_V1 fix using direct Python byte-string ops.
Reads raw bytes, locates sync_equity body, and rewrites both the broken reset
block AND injects `global _PE_FIX_V1_RESET_DONE` declaration.
"""
import sys

PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "rb") as f:
    raw = f.read()
content = raw.decode("utf-8")

# Locate sync_equity start + end (so we only touch within this function body).
start_marker = "def sync_equity(self) -> None:\n"
# Don't assume leading whitespace - just find marker anywhere.
start_idx = content.find(start_marker)
if start_idx < 0:
    print("FAIL: sync_equity start marker not found", file=sys.stderr)
    sys.exit(2)
body_start = start_idx + len(start_marker)

# Find end of sync_equity — next method (4-space-indented def) or class boundary.
# Scan forward: look for "\n    def " (4-space indent) at start of line.
end_idx = -1
i = body_start
while i < len(content):
    nl = content.find("\n", i)
    if nl < 0:
        break
    line_start = nl + 1
    # check if line begins with "    def " (4-space indent) — next method in class
    if content[line_start:line_start + 8] == "    def ":
        end_idx = line_start
        break
    i = nl + 1
if end_idx < 0:
    # fallback: end of next blank-line + dedent
    end_idx = content.find("\n\n", body_start)
    if end_idx < 0:
        end_idx = len(content)

print(f"sync_equity body span: {body_start}..{end_idx}")
print("--- current body (first 500 chars) ---")
print(content[body_start:body_start + 500])
print("--- rest current body ---")
print(content[body_start + 500:end_idx])

# Read what comes BEFORE the reset block to figure out the indentation level
# reset block currently has the dual-global broken pattern OR may already
# be clean. We need to handle either case.

# First, normalize: remove the broken followup1 pattern if present.
broken_block = """    try:
        global _PE_FIX_V1_RESET_DONE
        reset_state = int(_PE_FIX_V1_RESET_DONE)
    except Exception:
        reset_state = 0
    if reset_state == 0:
        self.copy_max_drawdown = dd_copy
        try:
            global _PE_FIX_V1_RESET_DONE
            _PE_FIX_V1_RESET_DONE = 1
        except Exception:
            pass
    else:
        self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)
"""
# Detect if body uses 4-space + 8-space inner or 0-space.
# The "    " is the class-method indent; "        " is inside try.
# Use the first body's leading whitespace as reference.
body_text = content[body_start:end_idx]

if "    try:\n        global _PE_FIX_V1_RESET_DONE" in body_text:
    # broken nested pattern is present — remove it
    new_body = body_text.replace(broken_block, "")
    print("OK: removed broken nested try/except from body")
elif "global _PE_FIX_V1_RESET_DONE" in body_text:
    print("INFO: some global already exists; preserving")
    new_body = body_text
else:
    new_body = body_text

# Second, normalize the reset block (the clean version we want) into a
# canonical form. Find the existing "if int(_PE_FIX_V1_RESET_DONE) == 0:" block.
canonical_reset = """    # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1).
    if int(_PE_FIX_V1_RESET_DONE) == 0:
        self.copy_max_drawdown = dd_copy
        _PE_FIX_V1_RESET_DONE = 1
    else:
        self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)
"""

# Strip ALL "    # One-shot reset..." comment duplicates so we end with exactly one comment.
# But only if the canonical block is already present.
import re
# remove duplicate comments of that form
pattern = "\n    # One-shot reset of stale inflated copy_max_drawdown(?: \\(PE_FIX_V1\\)\\.)?(?:#[^\n]*\n)?"
while True:
    matches = list(re.finditer(pattern, new_body))
    if len(matches) <= 1:
        break
    # remove all but the first
    for m in reversed(matches[1:]):
        new_body = new_body[:m.start()] + new_body[m.end():]

# Ensure canonical reset block exists.
if "if int(_PE_FIX_V1_RESET_DONE) == 0:" in new_body:
    print("INFO: canonical reset block already present")
else:
    # Append it before "self.lead_max_drawdown = max(self.lead_max_drawdown, dd_lead)"
    lead_max_marker = "self.lead_max_drawdown = max(self.lead_max_drawdown, dd_lead)"
    idx = new_body.rfind(lead_max_marker)
    if idx < 0:
        # fallback: append at end of body
        new_body = new_body.rstrip() + "\n" + canonical_reset + "\n"
    else:
        new_body = new_body[:idx] + canonical_reset + "\n    " + new_body[idx:]
    print("OK: canonical reset block inserted")

# Third, ensure `global _PE_FIX_V1_RESET_DONE` is at top of body (right after
# "def sync_equity(self) -> None:"). This is BEFORE any usage.
global_decl = "    # PE_FIX_V1: declare module-level reset flag as global (read+write).\n    global _PE_FIX_V1_RESET_DONE\n"

# After sync_equity header line ends with "\n", the next character is what comes
# after. If the existing first content is "# PE_FIX_V1: derive copy_alloc..."
# insert global_decl before it. Simpler: just inject right after the
# start_marker. Since we read body_text starting from after start_marker,
# new_body's first line should be after the function header.
if "global _PE_FIX_V1_RESET_DONE" not in new_body:
    # insert global_decl at the top of new_body
    new_body = global_decl + new_body
    print("OK: global declaration injected at top")
else:
    # ensure it's at TOP — check first 200 chars
    if "global _PE_FIX_V1_RESET_DONE" not in new_body[:400]:
        # remove any later occurrence and re-add at top
        # only do this if there's a global later but not at top
        # careful: there might be a global in the reset block (which we wanted to remove)
        new_body = re.sub(r"\n    global _PE_FIX_V1_RESET_DONE\n", "\n", new_body)
        new_body = global_decl + new_body
        print("OK: moved global declaration to top")

# Splice back into content
new_content = content[:body_start] + new_body + content[end_idx:]

if new_content == content:
    print("INFO: no changes needed")
else:
    print(f"OK: rewriting sync_equity ({len(content)} -> {len(new_content)} bytes)")
    with open(PATH, "w", encoding="utf-8") as f:
        f.write(new_content)

# Verify the body now
with open(PATH, "r", encoding="utf-8") as f:
    verify = f.read()
print("--- VERIFY first 800 chars of new body ---")
m = re.search(r"def sync_equity\(self\) -> None:\n(.*?)\n    def ", verify, re.DOTALL)
if m:
    print(m.group(1)[:800])
else:
    print("could not extract sync_equity for verify")

print("DONE")
