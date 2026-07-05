# PE_FIX_V2 SSOT Clean Status

Current action: stopped before formula patch because cache stabilisation failed.

- Freeze folder: C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\proofs\PE_FIX_V2_SSOT_CLEAN_20260630_215236
- Baseline hash: F5550F30DE498D1B4B078C20F8ADCF6F0EB9291E49019141AA050D3125F69C47
- Sanctioned rebuild attempted: POST /api/snapshot
- Rebuild result: timed out after 240 seconds
- Additional bounded wait: cache_stabilisation_poll.json and cache_stabilisation_poll_2.json
- Final cache status: STALE_REBUILDING
- UI restore action: stopped PID 6064 and restarted normal command python -B HL_Copy_App_SSOT.py
- Source patch applied: none
- Commit: none

Trace conclusion:
- Wallet rows mostly come from WalletModel.sync_equity/build_model_state.
- render_home still recomputes selected portfolio totals and drawdown locally.
- validate_render_contract compares copy equity with lead alloc instead of copy alloc.
- build_portfolio_history/selected_combined_curve_stats still sum row drawdowns rather than computing unified selected portfolio peak-to-trough.

Per instruction, do not commit or claim numeric PASS until cache becomes fresh and numeric proof can be run.
