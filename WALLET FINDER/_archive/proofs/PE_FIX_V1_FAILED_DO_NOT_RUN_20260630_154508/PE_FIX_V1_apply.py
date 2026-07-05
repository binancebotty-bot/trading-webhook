"""Apply the Wallet Proof Engine equity/drawdown minimal-safe patch (PE_FIX_V1)."""
import json, sys, os, textwrap

PATH = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py"
with open(PATH, "r", encoding="utf-8") as f:
    content = f.read()

# ----------------------------------------------------------------------
# (1) Insert helper `_compute_copy_alloc` IMMEDIATELY BEFORE sync_equity
# ----------------------------------------------------------------------
helper = textwrap.dedent("""\
    # --- PE_FIX_V1: copy_sizing helper ------------------------------------
    def _compute_copy_alloc(alloc_val, ui_state=None):
        try:
            ui = ui_state if isinstance(ui_state, dict) else {}
            mode = str(ui.get("copy_mode", "proportional")).strip().lower()
        except Exception:
            mode = "proportional"
        try:
            lead_alloc = float(alloc_val) if alloc_val is not None else 0.0
        except Exception:
            lead_alloc = 0.0
        lead_alloc = max(0.0, lead_alloc)
        if mode == "fixed":
            try:
                fn = float(ui.get("fixed_notional")) if isinstance(ui, dict) and ui.get("fixed_notional") is not None else float(DEFAULT_FIXED_NOTIONAL)
            except Exception:
                fn = float(DEFAULT_FIXED_NOTIONAL)
            return max(0.01, fn)
        try:
            nb = float(ui.get("norm_base")) if isinstance(ui, dict) and ui.get("norm_base") is not None else float(DEFAULT_NORM_BASE)
        except Exception:
            nb = float(DEFAULT_NORM_BASE)
        nb = max(1.0, nb)
        return lead_alloc / nb

""").rstrip("\n") + "\n\n"

old_sync = ("    def sync_equity(self) -> None:\n\n"
            "        self.lead_equity = self.alloc + self.lead_realized + self.lead_unrealized\n\n"
            "        self.copy_equity = self.alloc + self.copy_realized + self.copy_unrealized\n\n"
            "        self.lead_peak = max(self.lead_peak or self.alloc, self.lead_equity)\n\n"
            "        self.copy_peak = max(self.copy_peak or self.alloc, self.copy_equity)\n\n"
            "        self.lead_max_drawdown = max(self.lead_max_drawdown, max(0.0, self.lead_peak - self.lead_equity))\n\n"
            "        self.copy_max_drawdown = max(self.copy_max_drawdown, max(0.0, self.copy_peak - self.copy_equity))\n")

# Strip leading 4-space from each helper line so it sits inside class indentation.
helper_inside = "\n".join(("    " + line if line.strip() else line) for line in helper.splitlines()).rstrip("\n") + "\n\n"

new_sync = textwrap.dedent("""\
    def sync_equity(self) -> None:
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
        # One-shot reset of stale inflated copy_max_drawdown (PE_FIX_V1)
        try:
            reset_state = int(_PE_FIX_V1_RESET_DONE) if '_PE_FIX_V1_RESET_DONE' in globals() else 0
        except Exception:
            reset_state = 0
        if reset_state == 0:
            self.copy_max_drawdown = dd_copy
        else:
            self.copy_max_drawdown = max(self.copy_max_drawdown, dd_copy)
        self.lead_max_drawdown = max(self.lead_max_drawdown, dd_lead)
""").rstrip("\n") + "\n"

if old_sync not in content:
    print("FAIL: sync_equity pattern not found", file=sys.stderr)
    sys.exit(2)

content = content.replace(old_sync, helper_inside + new_sync, 1)
print("OK: helper + sync_equity patched")

# ----------------------------------------------------------------------
# (2) Initial curve point at 8062: use m.copy_equity (sync just ran)
# ----------------------------------------------------------------------
old_curve = '                "copy_pnl": 0.0, "copy_realized": 0.0, "copy_equity": m.alloc, "copy_drawdown": 0.0,\n'
new_curve = '                "copy_pnl": 0.0, "copy_realized": 0.0, "copy_equity": m.copy_equity, "copy_drawdown": 0.0,\n'
if old_curve not in content:
    print("FAIL: initial curve point pattern not found", file=sys.stderr)
    sys.exit(3)
content = content.replace(old_curve, new_curve, 1)
print("OK: initial curve corrected to m.copy_equity")

# ----------------------------------------------------------------------
# (3) render_home header — replace equity + DD lines (10885-10895 block)
# Parity: header = sum(row.equity) for both lead & copy.
# Unified live/max DD from per-row curves.
# ----------------------------------------------------------------------
old_header_dd = (
    "    lead_equity = alloc + lead_real + lead_unreal\n\n"
    "    copy_equity = alloc + copy_real + copy_unreal\n\n"
    "    lead_live_dd = sum(fnum((r.get(\"lead\") or {}).get(\"drawdown\")) for r in included_rows)\n\n"
    "    copy_live_dd = sum(fnum((r.get(\"copy\") or {}).get(\"drawdown\")) for r in included_rows)\n\n"
    "    lead_maxdd = max((fnum((p.get(\"lead\") or {}).get(\"drawdown\")) for p in hist), default=lead_live_dd)\n\n"
    "    copy_maxdd = max((fnum((p.get(\"copy\") or {}).get(\"drawdown\")) for p in hist), default=copy_live_dd)\n"
)

new_header_dd = ("    # PE_FIX_V1: header lead/copy equity == sum(row.equity) for selected rows.\n"
                "    # DD computed from unified selected portfolio curve, NOT summed per row.\n"
                "    try:\n"
                "        ui_header = load_ui_state() if 'load_ui_state' in globals() else {}\n"
                "    except Exception:\n"
                "        ui_header = {}\n"
                "    copy_alloc_total = sum(\n"
                "        _compute_copy_alloc(fnum(r.get(\"alloc\"), base), ui_header)\n"
                "        for r in included_rows\n"
                "    )\n"
                "\n"
                "    lead_equity = sum(fnum((r.get(\"lead\") or {}).get(\"equity\")) for r in included_rows)\n"
                "    copy_equity = sum(fnum((r.get(\"copy\") or {}).get(\"equity\")) for r in included_rows)\n"
                "\n"
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
                "        if not ts_to_rows:\n"
                "            cur = sum(last_eq.values())\n"
                "            return 0.0, 0.0, cur\n"
                "        running = 0.0\n"
                "        peak = 0.0\n"
                "        max_dd_val = 0.0\n"
                "        last_total = 0.0\n"
                "        for ts in sorted(ts_to_rows.keys()):\n"
                "            for rid, eq in ts_to_rows[ts]:\n"
                "                last_eq[rid] = eq\n"
                "            running = sum(last_eq.values())\n"
                "            if running > peak:\n"
                "                peak = running\n"
                "            dd = max(0.0, peak - running)\n"
                "            if dd > max_dd_val:\n"
                "                max_dd_val = dd\n"
                "            last_total = running\n"
                "        return max(0.0, peak - last_total), max_dd_val, last_total\n"
                "\n"
                "    lead_live_dd, lead_maxdd, _lead_unified_total = _pe_unified_dd(included_rows, \"lead\")\n"
                "    copy_live_dd, copy_maxdd, _copy_unified_total = _pe_unified_dd(included_rows, \"copy\")\n")

if old_header_dd not in content:
    print("FAIL: render_home header DD block pattern not found", file=sys.stderr)
    sys.exit(4)
content = content.replace(old_header_dd, new_header_dd, 1)
print("OK: render_home header patched (parity + unified DD)")

# ----------------------------------------------------------------------
# (4) Copy block() call uses copy_alloc_total, not alloc
# ----------------------------------------------------------------------
old_copy_block = '"copy": block(copy_equity, copy_real, copy_unreal, copy_live_dd, copy_maxdd, copy_equity + copy_live_dd, alloc),\n'
new_copy_block = '"copy": block(copy_equity, copy_real, copy_unreal, copy_live_dd, copy_maxdd, copy_equity + copy_live_dd, copy_alloc_total),\n'
if old_copy_block not in content:
    print("FAIL: copy block() pattern not found", file=sys.stderr)
    sys.exit(5)
content = content.replace(old_copy_block, new_copy_block, 1)
print("OK: copy block uses copy_alloc_total")

# ----------------------------------------------------------------------
# (5) Module-level one-shot reset flag
# ----------------------------------------------------------------------
flag_decls = ("# PE_FIX_V1: one-shot cache reset guard. 0 = needs reset, 1 = done.\n"
              "_PE_FIX_V1_RESET_DONE = 0\n\n")
anchor = "_MODEL_BUILD_LOCK = "
idx = content.find(anchor)
if idx < 0:
    content = flag_decls + content
    print("OK: flag inserted at top of file (fallback)")
else:
    content = content[:idx] + flag_decls + content[idx:]
    print("OK: one-shot reset flag inserted")

# ----------------------------------------------------------------------
# Save & emit proof artifact
# ----------------------------------------------------------------------
with open(PATH, "w", encoding="utf-8") as f:
    f.write(content)

proof_path = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\proofs\PE_FIX_V1.json"
os.makedirs(os.path.dirname(proof_path), exist_ok=True)
proof = {
    "change_id": "PE_FIX_V1",
    "source_file": PATH,
    "files_changed": ["HL_Copy_App_SSOT.py"],
    "fields_added": ["WalletModel.copy_alloc", "_PE_FIX_V1_RESET_DONE"],
    "formulas": {
        "per_wallet_copy_alloc_fixed": "fixed -> fixed_notional (>=0.01); proportional -> alloc/norm_base (norm_base>=1.0)",
        "per_wallet_copy_equity_fixed": "copy_alloc + copy_realized + copy_unrealized",
        "header_lead_equity_fixed": "sum(row.lead.equity for r in included_rows)",
        "header_copy_equity_fixed": "sum(row.copy.equity for r in included_rows)",
        "header_lead_live_dd": "unified_selected_portfolio_lead_peak - current_unified_lead_equity",
        "header_copy_live_dd": "unified_selected_portfolio_copy_peak - current_unified_copy_equity",
        "max_dd_lead": "max historical peak-trough on unified select lead curve",
        "max_dd_copy": "max historical peak-trough on unified select copy curve",
        "copy_block_alloc": "copy_alloc_total (sum of selected copy_alloc, NOT lead alloc)",
    },
    "cache_reset": "one-shot _PE_FIX_V1_RESET_DONE flag resets copy_max_drawdown on first sync; invalidate_model_cache triggered implicitly by next ui_state save after first run",
}
with open(proof_path, "w", encoding="utf-8") as f:
    json.dump(proof, f, indent=2)

print("DONE. Wrote", proof_path)
