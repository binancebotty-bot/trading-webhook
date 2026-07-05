# Phase 1 sync helper reconstruction plan

No source definition for sync_live_config_from_wallet_include was found. The helper is reconstructed from the two call sites and existing live-config helpers.

Insertion point: after _enforce_live_copy_cap and before later live helper functions, which is still before if __name__ == "__main__".

Behavior:
- Load live copy config with _load_live_copy_config.
- Determine desired wallets from cached model wallet_rows, excluding user wallet and applying wallet_included_for_row(row, ui).
- If no cached rows exist, fallback to ui.wallet_include entries explicitly set true.
- Preserve currently configured desired wallets.
- Restore desired wallets from archived_wallets if present.
- Create new desired wallets with mode OFF and current UI sizing defaults.
- Move currently configured non-desired wallets to archived_wallets as OFF/disabled.
- Run repair_live_config_consistency and _enforce_live_copy_cap.
- Save via _save_live_copy_config.
- Return _live_copy_config_response plus sync counts.

Safety boundary:
- Does not enable LIVE/CLO for any new wallet.
- Does not edit equity/drawdown formulas.
- Does not touch LIVE WALLET TRADING.
