# Council summary

Question: What is the minimal safe sequence to stabilise Wallet Proof Engine 8014, restore /?sync_copy=1, fix selected-wallet equity/DD mismatches, prove correctness, and commit without damaging today’s recovered baseline?

## Stability reviewer
- Freeze the recovered V2 baseline before changing code.
- Patch only missing sync helper first; formula work must be separate.
- Restart only the process serving port 8014 after static checks.
- Prove /, /?sync_copy=1, and /api/live-config/sync-inc.

## /?sync_copy=1 route reviewer
- No original source definition for sync_live_config_from_wallet_include was found in current file, backups, .nobom, quarantine, proofs, or git history.
- Reconstruct minimal helper from call sites and existing live-config helpers.
- Use _load_live_copy_config, _normalise_live_wallet_payload, repair_live_config_consistency, _enforce_live_copy_cap, _save_live_copy_config, and _live_copy_config_response if available.
- New synced wallets must default OFF, not LIVE/CLO.
- Deselected wallets should be archived OFF, not deleted destructively.

## Equity/accounting reviewer
- WalletModel.sync_equity is already using copy_alloc for copy equity.
- Row copy block currently passes lead allocation as copy alloc; change later in Phase 2.
- build_model_state needs copy_alloc_total from _compute_copy_alloc for selected copy equity; change later in Phase 2.
- build_portfolio_history copy equity still uses lead alloc; change later in Phase 2.
- render_home copy block should pass copy_alloc_total; change later in Phase 2.

## Drawdown reviewer
- selected_aggregate/build_portfolio_history/build_model_state still use summed row drawdowns.
- Correct fix is unified selected portfolio equity curve drawdown, not row DD sums.
- Update validation contract with the same unified curve logic in Phase 2.

## Regression/git reviewer
- Do not commit until Phase 1 and Phase 2 both PASS.
- Parent repo has untracked WALLET FINDER material and dirty LIVE WALLET TRADING marker; never use git add -A.
- Stage only explicit intended files after PASS.

## Minimal sequence
1. Freeze recovered baseline and prove current / and failing /?sync_copy=1.
2. Reconstruct only sync_live_config_from_wallet_include before __main__.
3. Static verify, restart 8014, prove /, /?sync_copy=1, and /api/live-config/sync-inc.
4. Freeze RECOVERED_BASELINE_8014.
5. Apply Phase 2 formula/DD patch separately.
6. Prove selected row/header/accounting/DD invariants.
7. Commit only after all gates pass.
