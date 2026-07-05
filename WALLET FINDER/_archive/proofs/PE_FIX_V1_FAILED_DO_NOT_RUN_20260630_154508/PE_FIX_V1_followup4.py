"""PE_FIX_V1 final follow-up: fix two critical bugs caught by reviewer.

Bug A: `global _PE_FIX_V1_RESET_DONE` declaration was missing from
sync_equity, causing NameError on `int(_PE_FIX_V1_RESET_DONE)`.

Bug B: `_pe_unified_dd` reads curve points via `pt.get("lead")`/"copy"`.
But curve points are FLAT dicts {ts, lead_equity, copy_equity, ...},
not nested. So pt.get(side_key) returned None → live_dd/max_dd always 0.
"""
import sys
PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

changes = 0

# ---------------------------------------------------------------
# Bug A: Inject `global _PE_FIX_V1_RESET_DONE` at top of sync_equity
# ---------------------------------------------------------------
old_top = "    def sync_equity(self) -> None:\n        # PE_FIX_V1: derive copy_alloc from existing UI sizing fields.\n"
new_top = ("    def sync_equity(self) -> None:\n"
           "        # PE_FIX_V1: declare module-level reset flag as global for read+write.\n"
           "        global _PE_FIX_V1_RESET_DONE\n"
           "        # PE_FIX_V1: derive copy_alloc from existing UI sizing fields.\n")
if old_top in content:
    content = content.replace(old_top, new_top, 1)
    changes += 1
    print("OK: global declaration injected in sync_equity")
else:
    print("WARN: sync_equity top pattern missing - check file state")

# ---------------------------------------------------------------
# Bug B: Fix _pe_unified_dd to read flat curve keys
# the m.curve point shape from sync_equity append is:
#   {ts: str, lead_equity: float, copy_equity: float, lead_drawdown: float, ...}
# ---------------------------------------------------------------
old_unified = (
    "    def _pe_unified_dd(rows, side_key):\n"
    "        ts_to_rows = {}\n"
    "        last_eq = {}\n"
    "        for r in rows:\n"
    "            last_eq[id(r)] = fnum((r.get(side_key) or {}).get(\"equity\"))\n"
    "            for pt in (r.get(\"curve\") or []):\n"
    "                ts = str(pt.get(\"ts\") or \"\")\n"
    "                if not ts:\n"
    "                    continue\n"
    "                sk = pt.get(side_key)\n"
    "                eq = fnum(sk.get(\"equity\")) if isinstance(sk, dict) else 0.0\n"
    "                ts_to_rows.setdefault(ts, []).append((id(r), eq))\n"
)
new_unified = (
    "    def _pe_unified_dd(rows, side_key):\n"
    "        # m.curve points are FLAT: {ts, lead_equity, copy_equity, lead_drawdown, copy_drawdown, ...}\n"
    "        # side_key is 'lead' or 'copy'; the equity key on each point is f\"{side_key}_equity\".\n"
    "        ts_to_rows = {}\n"
    "        last_eq = {}\n"
    "        equity_key = f\"{side_key}_equity\"\n"
    "        for r in rows:\n"
    "            last_eq[id(r)] = fnum((r.get(side_key) or {}).get(\"equity\"))\n"
    "            for pt in (r.get(\"curve\") or []):\n"
    "                ts = str(pt.get(\"ts\") or \"\")\n"
    "                if not ts:\n"
    "                    continue\n"
    "                eq = fnum(pt.get(equity_key))\n"
    "                ts_to_rows.setdefault(ts, []).append((id(r), eq))\n"
)
if old_unified in content:
    content = content.replace(old_unified, new_unified, 1)
    changes += 2
    print("OK: _pe_unified_dd reads flat curve keys")
else:
    print("WARN: _pe_unified_dd pattern missing - may already be modified")

if changes < 2:
    print("FAIL: not all changes applied", file=sys.stderr)
    sys.exit(2)

with open(PATH, "w", encoding="utf-8") as f:
    f.write(content)
print(f"DONE: {changes} edits applied")
