# WALLET FINDER — Pre-Live Proving Dashboard

Separated upstream wallet discovery, deep scanning, and proving dashboard
running in parallel with the live copy engine. **Never touches live engine files.**

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start the proving dashboard
python start.py
# Visit http://localhost:8012/

# Stop the dashboard
python stop.py
```

## Pipeline Order

The full wallet finder pipeline runs in stages:

1. **`1WalletFinder.py`** — Discover wallets via HL WebSocket + polling → `data/hl_wallets_filtered.csv`
2. **`2hl_Stage1_Filter.py`** — Safety-prune filter → `data/hl_stage1_pass.csv`
3. **`3hl_stage2_useful_wallet_scanner.py`** — Deep scan with 30+ KPIs + MTM truth → `data/summary.csv`
4. **`fetch_hl_portfolios.py`** — Pull HL accountValueHistory for MTM drawdown → `data/wallet_portfolios/`
5. **`enrich_summary_mtm.py`** — Join MTM stats into summary → `data/summary_enriched.csv`
6. **`wallet_copyability_test.py`** — Copyability gate with MTM-aware scoring → `data/copyable_wallets.csv`
7. **`universe_builder.py`** — Build equity curves + wallet universe → `data/wallet_universe.csv`, `data/equity_curves/`
8. **Dashboard** (`app.py`) — Proving dashboard on port 8012 reads from `data/`

## WARNING: Rate Limits

All scanner components (`1WalletFinder.py`, `3hl_stage2_useful_wallet_scanner.py`,
`universe_builder.py`) hit the **same Hyperliquid API** as the live copy engine.
**Do not run both the scanner pipeline and the live copy engine simultaneously**
unless you have verified your rate limit headroom.

The dashboard (`app.py`) is read-only and safe to run concurrently — it only
reads local CSV files and does not make HL API calls.

## Data Directory

All pipeline outputs and dashboard inputs live in `data/`:
- `data/summary.csv` — Deep scan results
- `data/wallet_universe.csv` — Universe for dashboard
- `data/equity_curves/<wallet>.csv` — Per-wallet equity curves
- `data/wallet_portfolios/<wallet>.json` — Cached HL portfolio API responses
- `data/copyable_wallets.csv` — Final copyable candidates

## Ports

- **8012** — WALLET FINDER proving dashboard (this package)
- **8011** — Live copy shadow engine (separate)
- **8000** — Live copy main dashboard (separate, if running)

No port collision with live copy engine.

## Tools

- `extract_hl_leaderboard_wallets.py` — Extract leaderboard wallets (requires CMM API token or playwright)
- `hl_info_readonly.py` — Read-only HL clearinghouse state client
- `build_hl_asset_universe_cache.py` — Build asset universe snapshot
- `copy_selection_run/` — Full copy selection pipeline (01-07 + v2 variants)

## SSOT Mechanics

The dashboard retains all single-source-of-truth mechanics from the original
proving dashboard: equity curve loading, slippage modeling, risk normalization,
copy score computation, wallet reordering, date filtering, and proof_wallets.txt export.
