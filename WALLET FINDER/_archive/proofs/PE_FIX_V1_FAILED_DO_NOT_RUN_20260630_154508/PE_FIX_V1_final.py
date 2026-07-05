"""PE_FIX_V1 FINAL fix: replace entire sync_equity body with a clean,
correctly 8-space-indented version. Bypasses all indentation debt.
"""
import re, sys

PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

# Locate "    def sync_equity(self) -> None:\n" (must be 4-space indented).
m = re.search(r"\n    def sync_equity\(self\) -> None:\n", content)
if not m:
    print("FAIL: indented sync_equity signature not found", file=sys.stderr)
    sys.exit(2)
sig_end = m.end()  # right after the \n

# Find next method (4-space-indented def) or class boundary.
scan = sig_end
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

print(f"sync_equity body span: {sig_end}..{end}")
print("--- existing body (first 200 chars) ---")
print(repr(content[sig_end:sig_end + 200]))

# Canonical body, 8-space indented.
canonical_body = """        # PE_FIX_V1: declare module-level reset flag as global (read+write).
        global _PE_FIX_V1_RESET_DONE
        # PE_FIX_V1: derive copy_alloc from existing UI sizing fields.
        try:
            ui = load_ui_state() if 'load_ui_state' in globals() else {}
        except Exception:
            ui = {}
        copy_alloc = _compute_copy_alloc(self.alloc, ui)
        self.copy_alloc = copy_alloc

        self.lead_equity = self.alloc + self.lead_realized + self.lead_unrealized
        self.copy_equity = copy_alloc + self.copy_realized + self.copy_unrealized

        self.lead_peak = max(self.lead_peak or self.alloc, self.lead_equity)
        self.copy_peak = max(self.copy_peak or copy_alloc, self.copy_equity)

        dd_lead = max(0.0, self.lead_peak - self.lead_equity)
        dd_copy = max(0.0, self.copy_peak - self.copy_equity)
        # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1).
        if int(_PE_FIX_V1_RESET_DONE) == 0:
            self.copy_max_drawdown = dd_copy
            _PE_FIX_V1_RESET_DONE = 1
        else:
            self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)
        self.lead_max_drawdown = max(self.lead_max_drawdown, dd_lead)
"""

new_content = content[:sig_end] + canonical_body + content[end:]
with open(PATH, "w", encoding="utf-8") as f:
    f.write(new_content)
print(f"OK: sync_equity body replaced ({len(content)} -> {len(new_content)} bytes)")
