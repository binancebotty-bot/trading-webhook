# Real Drawdown Filter Insertion — Implementation Audit

**Date:** 2026-07-02  
**Branch:** main  
**Commit:** (pending)

---

## Summary

Implemented a shared real drawdown filter module (`real_dd_filter.py`) and integrated it into two pipeline entry points:

1. **Stage 1.5 Pipeline** (`hl_stage1_5_mtm_filter.py`) — opt-in gate via `HL_S1_5_MAX_REAL_DD_PCT`
2. **Full Universe Scan** (`full_universe_scan.py`) — opt-in pre-filter via `HL_FULL_UNIVERSE_MAX_REAL_DD_PCT`

Both integrations are **disabled by default** (env var not set) for production safety.

---

## Files Created / Modified

### New Files

| File | Lines | Purpose |
|------|-------|---------|
| `real_dd_filter.py` | 388 | Shared helper module with 4 public functions |
| `tests/test_real_dd_filter.py` | 275 | 14 test cases (all passing) |

### Modified Files

| File | Changes |
|------|---------|
| `hl_stage1_5_mtm_filter.py` | Added real DD gate in `gate_mtm()`, `REAL_DD_COLUMNS` to output, env var `HL_S1_5_MAX_REAL_DD_PCT` |
| `full_universe_scan.py` | Added pre-filter block before simulation, env var `HL_FULL_UNIVERSE_MAX_REAL_DD_PCT`, audit CSV output |

---

## Core Logic

### Sign Convention
- HL's `allTime_max_drawdown_mtm` is **negative** (peak-to-trough)
- Module uses `abs()` throughout → returns positive DD percentage
- Formula: `abs(allTime_max_drawdown_mtm) / allTime_acctV_peak * 100`

### Source Labels
| Source | Label |
|--------|-------|
| `hl_portfolio_api` (fresh) | `FETCHED_ACCOUNT_VALUE_HISTORY` |
| `cache_stale` (local cache) | `LOCAL_ACCOUNT_VALUE_HISTORY` |
| `unavailable` / missing | `DATA_FETCH_BLOCKED` |

### Gate Behavior
| Condition | Result |
|-----------|--------|
| `max_real_dd_pct=None` (disabled) | All wallets pass (`dd_gate_pass=True`, `real_max_dd_pct=None`) |
| DD ≤ threshold | Pass (`dd_gate_pass=True`) |
| DD > threshold | Fail (`dd_gate_pass=False`, `dd_gate_reason="REAL_DD_TOO_HIGH"`) |
| Missing data + threshold enabled | Fail (`dd_gate_pass=False`, `dd_gate_reason="DATA_FETCH_BLOCKED"`) |
| Missing data + threshold disabled | Pass (gate disabled) |

---

## Integration Points

### Stage 1.5 Pipeline (`hl_stage1_5_mtm_filter.py`)
- **Trigger:** `HL_S1_5_MAX_REAL_DD_PCT` env var set (e.g., `50`)
- **Location:** Inside `gate_mtm()` after existing gates (calmar, equity_collapse, skin_in_game, negative_month)
- **Output:** Adds 5 columns to CSV: `real_max_dd_usd`, `real_max_dd_pct`, `dd_source`, `dd_gate_pass`, `dd_gate_reason`
- **Log:** Summary line showing rejections count

### Full Universe Scan (`full_universe_scan.py`)
- **Trigger:** `HL_FULL_UNIVERSE_MAX_REAL_DD_PCT` env var set (e.g., `50`)
- **Location:** Before expensive simulation loop (saves ~200K simulations per run)
- **Audit CSV:** `data/full_universe_real_dd_prefilter.csv` with per-wallet gate results
- **Log:** Pre-filter pass/reject counts

---

## Test Coverage (14 tests, all passing)

| Test | Requirement |
|------|-------------|
| `test_parse_max_real_dd_pct_sign_convention` | A. Negative DD → positive % |
| `test_parse_max_real_dd_pct_positive_dd` | B. Positive DD also works |
| `test_real_dd_gate_threshold_pass` | C. DD ≤ threshold → pass |
| `test_real_dd_gate_threshold_fail` | D. DD > threshold → fail |
| `test_real_dd_gate_missing_data_fails_when_threshold_enabled` | E. Missing data fails when enabled |
| `test_real_dd_gate_missing_data_does_not_silently_pass` | F. Missing data never silently passes |
| `test_real_dd_gate_independence_from_other_gates` | G. Independent of calmar/collapse gates |
| `test_wallet_passes_real_dd_full_universe_prefilter` | H. Full universe pre-filter with temp cache |
| `test_no_closed_pnl_usage` | I. No closedPnl/cumsum in code (AST check) |
| `test_gate_disabled_passes_everything` | J. Disabled gate passes all |
| `test_zero_dd_passes` | K. Zero DD passes any threshold |
| `test_edge_case_peak_is_zero` | L. Zero peak → DATA_FETCH_BLOCKED |
| `test_parse_max_real_dd_pct_missing_data` | Extra: missing data returns None |
| `test_real_dd_gate_zero_peak` | Extra: zero peak fails gate |

---

## Oracle Code Review Summary

**Verdict:** ✅ Ready for sign-off

**Issues Found (Medium/Low):**
1. No validation for negative `max_real_dd_pct` — add check in `real_dd_gate`
2. No handling for malformed `accountValueHistory` — add type/structure checks
3. No edge case docs for `max_real_dd_pct = 0` — document behavior
4. Missing type hints — add to all functions
5. Potential thread safety with shared `cache_dirs` — ensure thread-safe access

**No critical issues.** Implementation correctly:
- Uses accountValueHistory only (no closedPnl)
- Handles sign convention correctly
- Fails closed on missing data
- Opt-in via env vars (production-safe)
- Thread-safe (stateless functions)

---

## Usage Examples

### Enable Stage 1.5 Real DD Gate (50% threshold)
```bash
set HL_S1_5_MAX_REAL_DD_PCT=50
python hl_stage1_5_mtm_filter.py
```

### Enable Full Universe Pre-Filter (50% threshold)
```bash
set HL_FULL_UNIVERSE_MAX_REAL_DD_PCT=50
python full_universe_scan.py
```

### Both Enabled
```bash
set HL_S1_5_MAX_REAL_DD_PCT=50
set HL_FULL_UNIVERSE_MAX_REAL_DD_PCT=50
```

---

## Data Flow

```
Portfolio JSON (accountValueHistory)
    │
    ▼
load_real_dd_for_wallet() → computes real DD from actual equity curve
    │
    ▼
parse_max_real_dd_pct() → extracts DD %, handles sign, returns source label
    │
    ▼
real_dd_gate() → compares against threshold, returns gate result
    │
    ├──▶ Stage 1.5: gate_mtm() → rejects wallet, adds columns to output
    │
    └──▶ Full Universe: wallet_passes_real_dd() → pre-filter before simulation
```

---

## Rollback Plan

If issues arise in production:
1. Unset env vars → both gates disabled immediately
2. No code changes to `HL_Copy_App_SSOT.py` (live trading engine)
3. No schema changes to existing CSV outputs (new columns only added when enabled)

---

## Next Steps

1. Deploy with env vars unset (default safe state)
2. Enable in staging with `HL_S1_5_MAX_REAL_DD_PCT=50` and `HL_FULL_UNIVERSE_MAX_REAL_DD_PCT=50`
3. Monitor audit CSVs for rejection rates
4. Address Oracle's medium-priority recommendations in follow-up PR