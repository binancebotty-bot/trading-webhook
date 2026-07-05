"""Fix syntax: 'global _PE_FIX_V1_RESET_DONE' must be declared before any read."""
import sys
PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

old = (
    "    def sync_equity(self) -> None:\n"
    "    # PE_FIX_V1: derive copy_alloc from existing UI sizing fields.\n"
    "    try:\n"
    "        ui = load_ui_state() if 'load_ui_state' in globals() else {}\n"
    "    except Exception:\n"
    "        ui = {}\n"
    "    copy_alloc = _compute_copy_alloc(self.alloc, ui)\n"
    "    self.copy_alloc = copy_alloc\n"
    "\n"
    "    self.lead_equity = self.alloc + self.lead_realized + self.lead_unrealized\n"
    "    self.copy_equity = copy_alloc + self.copy_realized + self.copy_unrealized\n"
    "\n"
    "    self.lead_peak = max(self.lead_peak or self.alloc, self.lead_equity)\n"
    "    self.copy_peak = max(self.copy_peak or copy_alloc, self.copy_equity)\n"
    "\n"
    "    dd_lead = max(0.0, self.lead_peak - self.lead_equity)\n"
    "    dd_copy = max(0.0, self.copy_peak - self.copy_equity)\n"
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
    "    self.lead_max_drawdown = max(self.lead_max_drawdown, dd_lead)\n"
)

new = (
    "    def sync_equity(self) -> None:\n"
    "    # PE_FIX_V1: declare global first so subsequent reads are valid.\n"
    "    global _PE_FIX_V1_RESET_DONE\n"
    "    # PE_FIX_V1: derive copy_alloc from existing UI sizing fields.\n"
    "    try:\n"
    "        ui = load_ui_state() if 'load_ui_state' in globals() else {}\n"
    "    except Exception:\n"
    "        ui = {}\n"
    "    copy_alloc = _compute_copy_alloc(self.alloc, ui)\n"
    "    self.copy_alloc = copy_alloc\n"
    "\n"
    "    self.lead_equity = self.alloc + self.lead_realized + self.lead_unrealized\n"
    "    self.copy_equity = copy_alloc + self.copy_realized + self.copy_unrealized\n"
    "\n"
    "    self.lead_peak = max(self.lead_peak or self.alloc, self.lead_equity)\n"
    "    self.copy_peak = max(self.copy_peak or copy_alloc, self.copy_equity)\n"
    "\n"
    "    dd_lead = max(0.0, self.lead_peak - self.lead_equity)\n"
    "    dd_copy = max(0.0, self.copy_peak - self.copy_equity)\n"
    "    # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1).\n"
    "    if int(_PE_FIX_V1_RESET_DONE) == 0:\n"
    "        self.copy_max_drawdown = dd_copy\n"
    "        _PE_FIX_V1_RESET_DONE = 1\n"
    "    else:\n"
    "        self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)\n"
    "    self.lead_max_drawdown = max(self.lead_max_drawdown, dd_lead)\n"
)

if old not in content:
    print("FAIL: sync_equity pattern not found", file=sys.stderr)
    sys.exit(2)
content = content.replace(old, new, 1)
with open(PATH, "w", encoding="utf-8") as f:
    f.write(content)
print("OK: global declared at top of sync_equity; syntax should pass")
