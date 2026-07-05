# Stage 1 - Current-State Proof Artifact
## Wallet Finder Repair - 2026-06-27

### Git Status
- Repository: Hyperliquid scanner/
- Branch: live-plumbing-from-dashboard-fixed
- HEAD: 1fd284b - Fix live dashboard plumbing from clean core audit files
- Modified (unstaged): 14 files in parent directory
- WALLET FINDER status: Entirely UNTRACKED (75 files)
- Backup: backups/wallet_finder_pre_refresh_20260627_1430SS/ (1.8GB snapshot)

### System Architecture

#### UI 1 - Proving Engine Dashboard (Port 8012)
- Entrypoint: app.py / FastAPI + HTMX + Jinja2
- Purpose: Wallet discovery, filtering, ranking, compound scoring
- Data: wallet_universe.csv, summary.csv, equity_curves/, all_trades.csv
- Templates: templates/index.html, partials/table.html, partials/portfolio.html
- Features: Compound scoring (MTM Calmar 40%, PnL stability 15%, fee resilience 15%, recency 10%)

#### UI 2 - Live Copy Dashboard SSOT (Port 8014)
- Entrypoint: HL_Copy_App_SSOT.py / FastAPI + background model builder
- Purpose: Live copy config, wallet details, audit summaries
- Data: raw_live_fills.csv, engine_truth.json, app_model_state.json
- Features: Cache freshness tracking, atomic writes, wallet config management

### Wallet Discovery Pipeline
1. 1WalletFinder.py -> WebSocket stream -> hl_wallets_filtered.csv
2. 2hl_Stage1_Filter.py -> userFills API -> hl_stage1_pass.csv
3. hl_stage1_5_mtm_filter.py -> portfolio API -> hl_stage1_5_mtm_pass.csv
4. 3hl_stage2_useful_wallet_scanner.py -> userFillsByTime -> summary.csv
5. enrich_mtm_async.py -> enriches summary.csv with MTM data
6. universe_builder.py -> wallet_universe.csv + equity_curves/*.csv

### Copy Selection Sub-Pipeline (copy_selection_run/)
01_funnel -> 02_slice_tape -> 03_per_wallet_grid -> 04_shortlist_and_curves -> 05_portfolio -> 06_oos_validation -> 07_final_report

### Launch Commands
- UI 1: uvicorn app:app --host 0.0.0.0 --port 8012 --reload
- UI 2: uvicorn HL_Copy_App_SSOT:app --host 0.0.0.0 --port 8014 --reload

### Python Environment
- Python 3.14.0, fastapi 0.128.8, uvicorn 0.31.1, pandas 3.0.1, Jinja2 3.1.6, aiohttp 3.13.3

### Wallet Counts
- leaderboard_wallets.txt: 2000 wallets
- manual_wallets.txt: 59 wallets
- equity_curves/: 1467 CSV files
- wallet_portfolios/: 937 JSON files

### Known Issues Before Repair
- System off ~24 days (June 3-27, 2026)
- Pipeline data frozen as of June 2-3
- Both UIs untested in current state
- all_trades.csv and hl_wallets_filtered.csv updated today (partial activity)
