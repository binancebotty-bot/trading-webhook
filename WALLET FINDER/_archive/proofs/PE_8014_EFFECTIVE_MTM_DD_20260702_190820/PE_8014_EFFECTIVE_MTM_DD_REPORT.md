# PE 8014 Effective MTM DD Report

Timestamp: 2026-07-02T18:08:46.757682+00:00

## Status
- 8014 loaded: True
- Wallet Proof Engine visible: True
- TRACKED WALLETS visible: True
- Effective DD source path: `live_audit_summary.live_leader_performance[wallet].allTime_max_drawdown_mtm / max_drawdown_mtm populated by _build_live_leader_performance() via hl_mtm_lookup.get_mtm_stats()`
- app_model_state wallet_rows MTM used: `False`
- CSV endpoint: `http://127.0.0.1:8014/api/metrics.csv`
- CSV effective columns present: `lead_maxdd_effective_usd`, `lead_maxdd_effective_pct`, `lead_maxdd_effective_source`, `copy_maxdd_effective_usd`, `copy_maxdd_effective_pct`, `copy_maxdd_effective_source`, `realised_maxdd_diagnostic_usd`

## Selected Graph/Header Method
Selected DD is computed from the unified time-aligned selected portfolio equity curve: sum selected lead/copy equity at each timestamp, then rolling peak-to-current and peak-to-trough. It does not sum per-wallet maxDD.

## Sanity Samples
See `old_vs_effective_grid_dd_sample.csv`.

## Screenshot
`screenshot_8014_after_patch.png`

## Blockers
None for the patch. Some wallets without live-audit MTM show labelled realised fallback, never unlabelled primary closedPnl.
