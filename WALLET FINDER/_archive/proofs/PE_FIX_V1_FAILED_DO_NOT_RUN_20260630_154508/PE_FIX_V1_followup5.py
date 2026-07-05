"""Final PE_FIX_V1 patch: replace broken nested try/except with clean version.
The `_PE_FIX_V1_RESET_DONE` global must be declared BEFORE any reference."""
import sys
PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

# (1) The broken block currently lives just before "self.lead_max_drawdown = max(...)".
old_broken_block = (
    "    try:\n"
    "        global _PE_FIX_V1_RESET_DONE\n"
    "        reset_state = int(_PE_FIX_V1_RESET_DONE)\n"
    "    except Exception:\n"
    "        reset_state = 0\n"
    "    if reset_state == 0:\n"
    "        self.copy_max_drawdown = dd_copy\n"
    "        try:\n"
    "            global _PE_FIX_V1_RESET_DONE\n"
    "            _PE_FIX_V1_RESET_DONE = 1\n"
    "        except Exception:\n"
    "            pass\n"
    "    else:\n"
    "        self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)\n"
)
new_clean_block = (
    "    # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1).\n"
    "    # Relies on `global _PE_FIX_V1_RESET_DONE` declared at top of this function.\n"
    "    if int(_PE_FIX_V1_RESET_DONE) == 0:\n"
    "        self.copy_max_drawdown = dd_copy\n"
    "        _PE_FIX_V1_RESET_DONE = 1\n"
    "    else:\n"
    "        self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)\n"
)
if old_broken_block not in content:
    print("FAIL: broken block pattern not found", file=sys.stderr)
    sys.exit(2)
content = content.replace(old_broken_block, new_clean_block, 1)
print("OK: broken reset block replaced with clean version")

# (2) Inject `global _PE_FIX_V1_RESET_DONE` at the very top of sync_equity body.
old_top_anchor = "    def sync_equity(self) -> None:\n"
new_top_anchor = (
    "    def sync_equity(self) -> None:\n"
    "        # PE_FIX_V1: declare module-level reset flag as global (read+write).\n"
    "        global _PE_FIX_V1_RESET_DONE\n"
)
if old_top_anchor not in content:
    # fallback: maybe already inserted
    if "global _PE_FIX_V1_RESET_DONE" in content.split("def sync_equity(self) -> None:")[1].split("def sync_equity")[0]:
        print("INFO: global already declared at top of sync_equity (skipping)")
    else:
        print("FAIL: sync_equity top anchor not found", file=sys.stderr)
        sys.exit(3)
else:
    # replace ONLY the first occurrence (the sync_equity one)
    idx = content.find(old_top_anchor)
    content = content[:idx] + new_top_anchor + content[idx + len(old_top_anchor):]
    print("OK: global declaration injected at top of sync_equity")

with open(PATH, "w", encoding="utf-8") as f:
    f.write(content)
print("DONE")
