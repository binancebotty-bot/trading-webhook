# Stage 2 - STALE_DATA_GAP_AUDIT
## Wallet Finder Repair - 2026-06-27

### Data Freshness Summary

| Dataset | Type | Size | Newest | Oldest | Gap |
|---|---|---|---|---|---|
| equity_curves/ (1467 files) | Derived metrics CSV | Varies | 2026-06-02 | 2026-04-12 | 25 days |
| wallet_universe.csv | Derived metrics | 160 KB | 2026-05-28 | - | 30 days |
| summary.csv | Derived KPIs | 957 KB | 2026-06-03 | - | 24 days |
| all_trades.csv | Raw exchange data | 8.5 MB (70K) | 2026-06-27 | Apr 2026 | 0 days |
| copy_trades.csv | Derived copy tracking | 42 MB (118K) | 2026-06-03 | Apr 2026 | 24 days |
| raw_live_fills.csv | Raw exchange data | 292 MB (386K) | 2026-05-29 | Apr 2026 | 29 days |
| expected_copy_fills.csv | Derived expectations | 76 MB (193K) | 2026-06-03 | Apr 2026 | 24 days |
| app_model_state.json | Cached UI model | 543 MB | 2026-06-03 | - | 24 days |
| engine_truth.json | Engine state | 1.9 MB | 2026-05-29 | - | 29 days |
| equity_history.json | Derived history | 56 MB | 2026-06-03 | - | 24 days |
| portfolio_history.json | Derived history | 3 MB | 2026-06-03 | - | 24 days |
| hl_wallets_filtered.csv | Raw wallet list | 95 KB | 2026-06-27 | - | 0 days |
| wallet_portfolios/ (937 files) | Derived portfolio JSON | Varies | Unknown | Unknown | Unknown |

### Primary Gap: June 3 to June 27, 2026 (~24 days)

### Data Categories:
1. Raw Exchange Data (needs API re-fetch): raw_live_fills.csv, all_trades.csv
2. Derived Pipeline Outputs (regenerated): summary.csv, wallet_universe.csv, equity_curves/
3. Live Copy Engine Outputs (OUT OF SCOPE): copy_trades.csv, expected_copy_fills.csv, app_model_state.json, engine_truth.json

### Feasibility: FULL CATCH-UP FEASIBLE
- Gap: ~24 days (not 2+ years)
- Hyperliquid API is public and read-only
- Pipeline uses async parallel processing with semaphore rate limiting
- 1467 wallets is manageable batch size
- Estimated: 2-6 hours for full pipeline re-run

### Risk Factors:
- Hyperliquid API rate limits (mitigated by existing semaphore)
- API endpoint changes since June 3 (unlikely)
- Memory usage (mitigated by chunking)
- Wallet list churn (new wallets, inactive ones)

### Recommendation:
- Run pipeline in chunks of 100-200 wallets
- Verify API connectivity with small test batch first
- Use resumable checkpoints
