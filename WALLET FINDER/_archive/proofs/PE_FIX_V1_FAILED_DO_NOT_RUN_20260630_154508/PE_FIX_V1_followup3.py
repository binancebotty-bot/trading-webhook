"""Fix sync_equity: declare `_PE_FIX_V1_RESET_DONE` global at top of function
so it can be both read and assigned. Collapse the broken nested global into one
clean branch."""
import sys
PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

# The broken block lives just before "self.lead_max_drawdown = max(...)".
old = (
    "    # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1).\n"
    "    # Advance flag to 1 after first reset so subsequent syncs grow normally.\n"
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
new = (
    "    # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1).\n"
    "    # Reads/writes rely on `global _PE_FIX_V1_RESET_DONE` declared at top.\n"
    "    if int(_PE_FIX_V1_RESET_DONE) == 0:\n"
    "        self.copy_max_drawdown = dd_copy\n"
    "        _PE_FIX_V1_RESET_DONE = 1\n"
    "    else:\n"
    "        self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)\n"
)
if old not in content:
    print("FAIL: broken reset block pattern not found", file=sys.stderr)
    sys.exit(2)
content = content.replace(old, new, 1)
print("OK: reset block simplified")

# Inject `global _PE_FIX_V1_RESET_DONE` at top of sync_equity body.
old_top = (
    "    def sync_equity(self) -> None:\n"
    "        # PE_FIX_V1: derive copy_alloc from existing UI sizing fields.\n"
)
new_top = (
    "    def sync_equity(self) -> None:\n"
    "        # PE_FIX_V1: declare module-level flag as global for read+write.\n"
    "        global _PE_FIX_V1_RESET_DONE\n"
    "        # PE_FIX_V1: derive copy_alloc from existing UI sizing fields.\n"
)
if old_top not in content:
    print("FAIL: sync_equity top pattern not found", file=sys.stderr)
    sys.exit(3)
content = content.replace(old_top, new_top, 1)
print("OK: global declared at top of sync_equity")

with open(PATH, "w", encoding="utf-8") as f:
    f.write(content)
print("DONE")
