# Candidate Insertion Points for Real DD Upstream Filter

## Context

ClosedPnL-only drawdown is invalid because it misses intra-position unrealised losses (1,000-3,600x understatement discovered). Real drawdown must come from `accountValueHistory` (the HL portfolio API equity curve including unrealised PnL). Portfolio JSONs containing this data already exist at `copy_selection_run/wallet_portfolios/*.json` for 4,463 wallets.

---

## Insertion Point A: Inside `hl_stage1_5_mtm_filter.py` `gate_mtm()` function

**Location**: `hl_stage1_5_mtm_filter.py`, line 99, function `gate_mtm(stats: dict)`

**How**: Add a new gate check to `gate_mtm()` that reads `allTime_max_drawdown_mtm` from the MTM stats dict (already computed by `get_mtm_stats_async()` from the same portfolio API call) and rejects wallets where real DD exceeds a threshold.

**Pros**:
- **Zero additional API calls** — portfolio data is already fetched in Stage 1.5 for every wallet
- **Earliest possible point** — before Stage 2 (the most expensive API step: 5000 fill fetches/wallet)
- **Prevents Stage 2 entirely for rejected wallets** — saves 5000 API calls per rejected wallet
- **Data already in `stats` dict** — `allTime_max_drawdown_mtm` is a field returned by `get_mtm_stats_async()`
- **Portfolio JSONs already cached** — `data/wallet_portfolios/*.json` persists the data for reuse
- **Single point of change** — one function, one gate, one threshold
- **Non-breaking** — adds a new gate, doesn't change existing gates

**Cons**:
- Only filters wallets going through the Stage Pipeline (Path 1), not the Full Universe Scan (Path 2)
- Stage 1.5 already filters by `mtm_calmar >= 1.0` which partially correlates with DD (high DD → low Calmar)
- `allTime_max_drawdown_mtm` uses only monthly data points from `accountValueHistory` — may miss intra-month spikes

**Compute savings**: Each wallet rejected at Stage 1.5 saves ~5000 API calls in Stage 2. If 30% of Stage 1.5 survivors fail the real DD gate (conservative estimate: 20-50 wallets), that's 100K-250K API calls saved.

**Risk**: LOW — purely additive gate, existing behavior unchanged when threshold is set permissively

---

## Insertion Point B: New Stage 1.75 script between Stage 1.5 and Stage 2

**Location**: New file `hl_stage1_75_real_dd_filter.py` that reads `hl_stage1_5_mtm_pass.csv`, loads portfolio JSONs, computes real DD, and writes `hl_stage1_75_real_dd_pass.csv`.

**How**: Standalone script that:
1. Reads `hl_stage1_5_mtm_pass.csv` (wallet addresses + MTM stats)
2. For each wallet, loads `data/wallet_portfolios/{wallet}.json` (already cached)
3. Computes real DD from `accountValueHistory` (allTime period)
4. Applies DD threshold gate
5. Writes filtered output for Stage 2

**Pros**:
- **Clean separation of concerns** — real DD logic isolated from MTM Calmar logic
- **Testable independently** — can unit test the DD computation without Stage 1.5
- **Can be re-run cheaply** — reads cached JSONs, no API calls
- **Produces deterministic CSV columns** — `real_max_dd_usd`, `real_max_dd_pct`, `dd_gate_pass`, `dd_gate_reason`
- **Reusable by Full Universe Scan** — `full_universe_scan.py` can import the same DD computation

**Cons**:
- Extra script to maintain
- Extra CSV in the pipeline (more files to track)
- Wallets must already have portfolio JSONs cached — if Stage 1.5 hasn't run yet, no JSONs exist
- Adds a pipeline step (though very cheap — reads local JSONs only)

**Compute savings**: Same as Point A — saves Stage 2 API calls for rejected wallets

**Risk**: LOW — new script, no existing code modified

---

## Insertion Point C: Inside `full_universe_scan.py` before the simulation loop

**Location**: `full_universe_scan.py`, line 152, before `for i, wallet in enumerate(qualifying_wallets):`

**How**: Before the per-wallet simulation loop, load portfolio JSONs and filter out wallets with real DD exceeding threshold. This avoids the expensive NB_GRID simulation (200 grid points × per-wallet trade filtering) for high-DD wallets.

**Pros**:
- **Prevents the heaviest compute in the entire pipeline** — `full_universe_scan.py` is the single most expensive script (1004 wallets × ~200 simulations)
- **Portfolio JSONs already exist** for most wallets — `copy_selection_run/wallet_portfolios/*.json` has 4,463 files
- **No additional API calls needed** for wallets with cached JSONs
- **Addresses the Full Universe Scan path** (Path 2) which Point A doesn't cover
- **Can be gated by CLI flag** — `--real-dd-threshold` to enable/disable

**Cons**:
- Only filters wallets going through Full Universe Scan, not Stage Pipeline
- For wallets without cached portfolio JSONs, would need to fetch (1 API call each) or skip
- The scan already reads `all_trades.csv` (4.97M rows) — the expensive part is the NB_GRID simulation, not the data loading
- Some wallets in `all_trades.csv` (813 of 1004) are NOT in `copyable_wallets.csv` — may not have portfolio JSONs

**Compute savings**: If 40% of 1004 wallets fail real DD gate, that's ~400 wallets × ~200 simulations = 80,000 simulations saved. Each simulation processes up to thousands of trades.

**Risk**: MEDIUM — modifies the heaviest compute script; must not break existing simulation logic

---

## Insertion Point D: Inside `wallet_bruteforce.py` before the simulation loop

**Location**: `wallet_bruteforce.py`, line 100, before `for n, cw in enumerate(cand.itertuples()):`

**How**: Before the per-wallet simulation loop, load portfolio JSONs and filter out wallets with real DD exceeding threshold.

**Pros**:
- Prevents expensive simulation for high-DD wallets
- `copyable_wallets.csv` wallets mostly have portfolio JSONs cached
- Simple filter — just check cached JSON before entering sim loop

**Cons**:
- Only covers 197 copyable wallets (subset of the full universe)
- Less impactful than Point C (fewer wallets, less compute saved)
- Same issue as Point C: some wallets may lack cached JSONs

**Compute savings**: If 30% of 197 wallets fail, that's ~60 wallets × 6 NB grid points = 360 simulations saved. Moderate.

**Risk**: LOW — small script, simple filter

---

## Insertion Point E: Inside `compute_bankroll.py` before bankroll computation

**Location**: `compute_bankroll.py`, line 155, before `for _, row in best.head(30).iterrows():`

**How**: Add a real DD check before computing bankroll requirements. Reject wallets where real DD exceeds bankroll threshold.

**Pros**:
- Last gate before output — catches wallets that slipped through earlier filters
- Bankroll computation is MEDIUM cost, so savings are moderate
- Produces clean output: wallets with both sim DD and real DD in the same CSV

**Cons**:
- Too late — Stage 2 and full scan already completed by this point
- Only processes 30 wallets (top from full_universe_best.csv)
- Compute savings minimal — bankroll is not the bottleneck

**Compute savings**: Negligible — bankroll processes only 30 wallets

**Risk**: LOW — but too late to prevent expensive compute upstream

---

## Insertion Point F: Inside `wallet_copyability_test.py` `evaluate_copyability_gate()`

**Location**: `wallet_copyability_test.py`, line 244, function `evaluate_copyability_gate()`

**How**: Add a real DD check to the copyability gate. Reject wallets where `allTime_max_drawdown_mtm` exceeds threshold.

**Pros**:
- Happens before `wallet_bruteforce.py` and `universe_builder.py` — prevents downstream expensive steps
- MTM data is already loaded via `get_mtm_stats()` — `allTime_max_drawdown_mtm` is available
- Single function change, minimal code

**Cons**:
- Only covers wallets going through copyability gate (subset of all wallets)
- `get_mtm_stats()` may not have fresh `allTime_max_drawdown_mtm` for all wallets (48% had http_429)
- The gate already checks `equity_collapse_flag_mtm` which partially catches massive DD
- Doesn't help `full_universe_scan.py` path

**Compute savings**: Saves bruteforce simulation for rejected wallets (~60 wallets × 6 simulations)

**Risk**: LOW — but limited coverage

---

## Summary Comparison

| Point | Location | API Calls Saved | Compute Saved | Coverage | Code Change | Risk |
|-------|----------|----------------|---------------|----------|-------------|------|
| **A** | `hl_stage1_5_mtm_filter.py:99` | 5000/wallet rejected | Stage 2 for rejected | Stage Pipeline only | 1 function | LOW |
| **B** | New script `hl_stage1_75_*.py` | Same as A | Same as A | Stage Pipeline only | New file | LOW |
| **C** | `full_universe_scan.py:152` | 0 (local sim) | 80K simulations | Full Universe Scan | 1 loop filter | MEDIUM |
| **D** | `wallet_bruteforce.py:100` | 0 (local sim) | 360 simulations | Copyable subset | 1 loop filter | LOW |
| **E** | `compute_bankroll.py:155` | 0 | Negligible | Top 30 only | 1 loop filter | LOW |
| **F** | `wallet_copyability_test.py:244` | 0 | 360 simulations | Copyable subset | 1 gate function | LOW |

**Note**: Points A and B save API calls (real money/time). Points C-F save local compute only.
