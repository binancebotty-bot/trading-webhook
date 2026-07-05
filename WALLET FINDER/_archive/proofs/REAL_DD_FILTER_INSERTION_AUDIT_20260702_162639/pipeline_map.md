# WALLET FINDER Upstream Pipeline Map

## Overview

The pipeline discovers wallets on Hyperliquid, filters them through increasingly expensive stages, simulates copy performance, and hands survivors to the Wallet Proof Engine for proving.

**Two parallel paths exist:**
1. **Stage Pipeline** (traditional): WebSocket → Stage 1 → Stage 1.5 → Stage 2 → copyability → universe → proving
2. **Full Universe Scan** (brute force): all_trades.csv → scan ALL wallets → rank → bankroll → proving

---

## Stage Pipeline (Path 1)

### Stage 0: WebSocket Discovery
- **Script**: `1WalletFinder.py` (240 lines)
- **Input**: HL WebSocket `wss://api.hyperliquid.xyz/ws` — live trade stream
- **Output**: `data/hl_wallets_filtered.csv` — wallet addresses discovered from live trades
- **Compute cost**: LIGHT — runs continuously, appends new wallets
- **Wallet identity**: Yes — addresses only, no analysis
- **Key functions**: `stream_worker()` (line 102), `get_coins()` (line 81), `append_csv()` (line 45)

### Stage 1: Basic Fill Filter
- **Script**: `2hl_Stage1_Filter.py` (210 lines)
- **Input**: `data/hl_wallets_filtered.csv`
- **Output**: `data/hl_stage1_pass.csv` — wallets passing basic filters
- **Compute cost**: LIGHT — 1 API call per wallet (`userFills`, 100 fill limit)
- **Wallet identity**: Yes — addresses + basic metrics (trades, PnL, win rate, volume)
- **Gate**: trades ≥ 30, PnL > $1, avg_pnl > 0, win_rate > 15%, trades_7d ≥ 1, volume > $20K
- **Key functions**: `process_wallet()` (line 116), `analyse_fills()` (line 77)
- **Concurrency**: 5

### Stage 1.5: MTM Calmar Pre-Filter ⭐ KEY INSERTION POINT
- **Script**: `hl_stage1_5_mtm_filter.py` (253 lines)
- **Input**: `data/hl_stage1_pass.csv`
- **Output**: `data/hl_stage1_5_mtm_pass.csv` — Calmar-ranked survivors
- **Compute cost**: MEDIUM — 1 portfolio API call per wallet (vs 5-10 paginated calls in Stage 2)
- **Wallet identity**: Yes — addresses + full MTM stats from accountValueHistory
- **Gate**: mtm_calmar ≥ 1.0, equity_collapse = 0, acctV_end ≥ $5000, negative_total = 0, source != unavailable
- **Key functions**: `gate_mtm()` (line 99), `fetch_mtm()` (line 127), `main()` (line 145)
- **Concurrency**: 30
- **Portfolio data**: Fetches from HL portfolio API via `get_mtm_stats_async()` → cached in `data/wallet_portfolios/*.json`
- **THIS IS WHERE accountValueHistory IS ALREADY FETCHED** — real DD can be extracted from the same data at zero additional API cost

### Stage 2+3: Deep Wallet Scanner
- **Script**: `3hl_stage2_useful_wallet_scanner.py` (423 lines)
- **Input**: `data/hl_stage1_5_mtm_pass.csv` (or fallback `hl_stage1_pass.csv`)
- **Output**: `data/summary.csv` — 50+ KPIs per wallet (the master wallet database)
- **Compute cost**: HEAVY — 5000 fill fetches per wallet via `userFillsByTime` (paginated), 50 concurrent
- **Wallet identity**: Yes — addresses + full KPIs (PnL, drawdown, win rate, martingale flags, MTM truth)
- **Key functions**: `process_wallet()` (line ~60+), `analyse_fills()` variants
- **THIS IS THE EXPENSIVE STEP** — eliminated 80-90% of calls by Stage 1.5 pre-filter

### Copyability Gate
- **Script**: `wallet_copyability_test.py` (317 lines)
- **Input**: `data/summary.csv` + `data/all_trades.csv`
- **Output**: `data/copyable_wallets.csv` — wallets passing copyability gate (204 wallets)
- **Compute cost**: MEDIUM — reads 4.97M row trades file, runs FIFO simulation per wallet
- **Wallet identity**: Yes — addresses + copyability score + MTM columns
- **Gate**: MTM Calmar ≥ 1.0, realised score ≥ 0.01, positive month MTM, no equity collapse
- **Key functions**: `simulate_wallet()` (line 116), `evaluate_copyability_gate()` (line 244), `main()` (line 262)

### Universe Builder
- **Script**: `universe_builder.py` (438 lines)
- **Input**: `data/summary.csv`
- **Output**: `data/wallet_universe.csv` — ranked active wallets (376 wallets)
- **Compute cost**: MEDIUM — continuous polling, equity curve building
- **Wallet identity**: Yes — addresses + realised metrics + score
- **Key functions**: `is_garbage()` (line 86), equity curve building
- **Output columns**: wallet, realised_pnl, total_notional, efficiency, trades, trades_7d, last_trade_time, score, status

---

## Full Universe Scan (Path 2)

### Full Universe Scan
- **Script**: `full_universe_scan.py` (306 lines)
- **Input**: `data/all_trades.csv` (4.97M rows, 1250 wallets) + `data/trade_based_mtm.csv`
- **Output**: `data/full_universe_results.csv` + `data/full_universe_best.csv`
- **Compute cost**: HEAVIEST — simulates ALL 1004 wallets with 100+ trades across fixed $12 + proportional grid (NB_GRID = 12-5000)
- **Wallet identity**: Yes — ALL wallets from all_trades.csv, not just pre-filtered
- **Key functions**: `simulate_fixed()` (line 54), `simulate_proportional()` (line 66), `score_wallet()` (line 77)
- **NB_GRID**: 12-100 (step 1) + 120-1000 (step 20) + 1050-5000 (step 50) = ~200 grid points per wallet
- **This is the single most expensive script in the pipeline**

### Wallet Brute-Force
- **Script**: `wallet_bruteforce.py` (211 lines)
- **Input**: `data/copyable_wallets.csv` + `data/all_trades.csv`
- **Output**: `data/wallet_bruteforce.csv` — 197 wallets simulated
- **Compute cost**: HEAVY — simulates all copyable wallets with fixed $12 + proportional grid [100,200,500,1000,2000,5000]
- **Wallet identity**: Yes — copyable wallets only (204 → 197 with 50+ trades)
- **Key functions**: `replay_fixed()` (line 41), `replay_prop()` (line 57), `calc_metrics()` (line 29)

### Trade-Based MTM
- **Script**: `compute_trade_mtm.py` (250 lines)
- **Input**: `data/all_trades.csv`
- **Output**: `data/trade_based_mtm.csv` — MTM-equivalent stats from closedPnl equity curve
- **Compute cost**: MEDIUM — groupby + cumsum per wallet, 1095 wallets
- **Wallet identity**: Yes — ALL wallets with 50+ trades (100% MTM coverage)
- **Key functions**: `compute_trade_mtm()` (line 34)

### Bankroll Computation
- **Script**: `compute_bankroll.py` (261 lines)
- **Input**: `data/full_universe_best.csv` + `data/all_trades.csv`
- **Output**: `data/wallet_bankroll_requirements.csv` — capital requirements for top 30
- **Compute cost**: MEDIUM — per-wallet concurrency estimation
- **Key functions**: `compute_bankroll()` (line 33)

### Re-rank with Real DD
- **Script**: `rerank_with_real_dd.py`
- **Input**: `data/wallet_bruteforce.csv` + `copy_selection_run/wallet_portfolios/*.json`
- **Output**: `data/rerank_results.csv` — re-ranked with real accountValueHistory DD
- **Compute cost**: LIGHT — reads cached JSONs, no API calls
- **Key insight**: DD ratios (real/sim) range from 16x to 68,870x

---

## Portfolio Data Sources

### Portfolio JSON Cache
- **Location**: `copy_selection_run/wallet_portfolios/*.json` (4,463 files)
- **Structure**: Array of [period_label, {accountValueHistory, pnlHistory, vlm}]
- **Periods**: day, week, month, allTime
- **Fetcher**: `fetch_hl_portfolios.py` — calls HL portfolio API, caches locally
- **MTM Lookup**: `hl_mtm_lookup.py` — reads cached JSONs, computes MTM stats
- **Async Enrichment**: `enrich_mtm_async.py` — batch enriches summary.csv with MTM data

### Real DD Already Available
- `fetch_hl_portfolios.py` `summarise()` (line 57): computes `{period}_acctV_mdd_mtm` = accountValue max drawdown
- `hl_mtm_lookup.py` `_summarise()` (line 63): computes `allTime_max_drawdown_mtm` from accountValueHistory
- Both use the SAME portfolio API data — accountValueHistory includes unrealised PnL

---

## Proving / Output

### Selection Pack
- **Script**: `gen_selection_pack.py` (171 lines)
- **Input**: `hl_copy_output/app_model_state.json`
- **Output**: Selection pack with tiered wallet recommendations

### Final Proof
- **Script**: `gen_final_proof.py` (172 lines)
- **Input**: Various output files
- **Output**: Proof JSON documenting the full refresh

### Wallet Finder Dashboard (port 8012)
- **Script**: `app.py` (2074 lines)
- **Input**: `data/wallet_universe.csv` + `data/summary.csv` + `data/equity_curves/`
- **Output**: FastAPI + HTMX dashboard
- **Port**: 8012

### Wallet Proof Engine (port 8014)
- **Script**: `HL_Copy_App_SSOT.py` (9978 lines)
- **Input**: Live copy data from engine state
- **Output**: Proving dashboard with LEAD vs COPY metrics
- **Port**: 8014
- **Known issue**: `dd_max()` (line 6321) ignores MTM fields, uses only realised closedPnl DD

---

## Data Flow Diagram

```
1WalletFinder.py
    ↓ ws trades
data/hl_wallets_filtered.csv (addresses)
    ↓
2hl_Stage1_Filter.py (1 API call/wallet)
    ↓ basic filters
data/hl_stage1_pass.csv (~1250 wallets)
    ↓
hl_stage1_5_mtm_filter.py (1 portfolio API call/wallet) ⭐ INSERTION POINT
    ↓ MTM Calmar gate + ★★★ REAL DD GATE ★★★
data/hl_stage1_5_mtm_pass.csv (~75-150 wallets)
    ↓
3hl_stage2_useful_wallet_scanner.py (5000 fills/wallet) ← HEAVY
    ↓ 50+ KPIs
data/summary.csv (~2798 wallets)
    ↓
wallet_copyability_test.py (FIFO sim)
    ↓ copyability gate
data/copyable_wallets.csv (~204 wallets)
    ↓
wallet_bruteforce.py ← HEAVY (simulates all copyable)
    ↓
data/wallet_bruteforce.csv (197 wallets)
    ↓
rerank_with_real_dd.py (uses portfolio JSONs)
    ↓
data/rerank_results.csv

--- PARALLEL PATH ---
all_trades.csv (4.97M rows)
    ↓
full_universe_scan.py ← HEAVIEST (scans ALL 1004 wallets)
    ↓
data/full_universe_best.csv (top per wallet)
    ↓
compute_bankroll.py ← MEDIUM
    ↓
data/wallet_bankroll_requirements.csv
    ↓
gen_selection_pack.py → gen_final_proof.py
    ↓
app.py (port 8012) + HL_Copy_App_SSOT.py (port 8014)
```

---

## Where Heavy Compute Starts

| Step | Script | Cost | Wallets | API Calls | Triggers |
|------|--------|------|---------|-----------|----------|
| Stage 2 | `3hl_stage2_*.py` | HEAVY | 75-150 | 5000/wallet | After Stage 1.5 |
| Brute-force | `wallet_bruteforce.py` | HEAVY | 197 | 0 (local) | Manual |
| Full scan | `full_universe_scan.py` | HEAVIEST | 1004 | 0 (local) | Manual |
| Bankroll | `compute_bankroll.py` | MEDIUM | 30 | 0 (local) | After full scan |

**The single most expensive step is `full_universe_scan.py`** — it simulates 1004 wallets × ~200 grid points = ~200,000 simulations on 4.97M rows.

**The single most expensive API step is Stage 2** (`3hl_stage2_useful_wallet_scanner.py`) — 5000 fill fetches per wallet at 50 concurrency.
