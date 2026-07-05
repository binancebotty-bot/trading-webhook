"""Fix: advance _PE_FIX_V1_RESET_DONE flag to 1 after first sync_equity call.
Code reviewer caught that the original reset flag was read but never advanced,
causing every sync_equity() call to overwrite copy_max_drawdown to current dd."""
import sys
PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

old = (
    "    # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1)\n"
    "    try:\n"
    "        reset_state = int(_PE_FIX_V1_RESET_DONE) if '_PE_FIX_V1_RESET_DONE' in globals() else 0\n"
    "    except Exception:\n"
    "        reset_state = 0\n"
    "    if reset_state == 0:\n"
    "        self.copy_max_drawdown = dd_copy\n"
    "    else:\n"
    "        self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)\n"
)

new = (
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

if old not in content:
    print("FAIL: pattern not found", file=sys.stderr)
    sys.exit(2)
content = content.replace(old, new, 1)
with open(PATH, "w", encoding="utf-8") as f:
    f.write(content)
print("OK: reset flag now advances after first sync_equity call")
