# HYBRID RECOVERY V2 graft plan

Target: C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\HL_Copy_App_SSOT.py

Scope: runtime recovery only for Wallet Proof Engine on port 8014. This plan does not apply the equity/drawdown formula fix, does not redesign UI, does not run PE_FIX scripts, and does not restore HL_Copy_App_SSOT.py.nobom over the current file.

## Symbols to add

1. WALLET_META_TAGS
   - Source: _CLEANUP_QUARANTINE_/tier4_deadcode/CUNT_FILES/HL_Copy_App_SSOT.py lines 607-608.
   - Reason: sanitize_wallet_meta in the already-grafted recovery block references this constant.
   - Insertion point: immediately before sanitize_wallet_meta, before if __name__ == "__main__".

2. WALLET_META_COLORS
   - Source: _CLEANUP_QUARANTINE_/tier4_deadcode/CUNT_FILES/HL_Copy_App_SSOT.py lines 607-608.
   - Reason: sanitize_wallet_meta in the already-grafted recovery block references this constant.
   - Insertion point: immediately before sanitize_wallet_meta, before if __name__ == "__main__".

3. wallet_included_for_row
   - Source: no full source definition found in current file, .nobom, proof freezes, candidate probe files, backups, quarantine, or graphify AST cache.
   - Reconstruction reason: all call sites pass (row, ui) and expect a boolean include/exclude decision based on the existing wallet_included(wallet, ui) semantics. Missing wallet/address defaults to included, matching current wallet_included default.
   - Insertion point: immediately after wallet_included, before if __name__ == "__main__".

4. _GLOBAL_CONTROLS_DEFAULTS
   - Source: _CLEANUP_QUARANTINE_/tier4_deadcode/CUNT_FILES/HL_Copy_App_SSOT.py lines 713-722.
   - Reason: _normalise_global_controls in the already-grafted recovery block references this constant.
   - Insertion point: immediately before _normalise_global_controls, before if __name__ == "__main__".

5. _compute_copy_alloc
   - Source: identical body already present in the current target at lines 1649-1671, but incorrectly scoped as WalletModel._compute_copy_alloc while render_home calls a module-level helper.
   - Reason: render_home currently calls _compute_copy_alloc(...) during selected-wallet header/table rendering. Adding the same body at module level restores the missing global symbol without changing the formula.
   - Insertion point: near wallet_included_for_row in the recovery helper block, before if __name__ == "__main__".

## Known deferred symbol risk

copy_alloc_total is referenced inside build_model_state. That path is formula-related PE_FIX fallout and is not changed in this recovery unless runtime proves the normal port-8014 startup path hits it. If it fails there, stop and update undefined_symbol_scan.json before any further patch.

sync_live_config_from_wallet_include is only reached via ?sync_copy=1 and is not required for normal Wallet Proof Engine render at /. It is not grafted in this pass.

## Dependencies

The grafted helpers depend only on existing imports/types already present in the file: Any, Dict, Optional, fnum, DEFAULT_FIXED_NOTIONAL, DEFAULT_NORM_BASE, and wallet_included.

## Runtime-recovery boundary

This graft restores missing names caused by PE_FIX_V1 corruption so the already-present port-8014 Wallet Proof Engine can import and render. It does not alter equity/drawdown calculations, copy allocation calculations, selected-wallet persistence semantics, purge behavior, or UI layout.
