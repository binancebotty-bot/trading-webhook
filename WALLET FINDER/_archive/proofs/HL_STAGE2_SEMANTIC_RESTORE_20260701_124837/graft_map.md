# Graft Map: hl_stage2 → WALLET FINDER Semantic Restore

**Strategy:** C — Use original hl_stage2 as semantic base, graft only minimal Wallet Finder features
**Date:** 2026-07-01

## Original hl_stage2 Files (Semantic Authority)

| File | SHA256 | Lines | Bytes |
|------|--------|-------|-------|
| HL_Copy_App_SSOT.py | ad30360395645a815ca3f8d86d117b58b28128d6c168950b22b6d175832e765c | 9,940 | 598,872 |
| HL_Copy_Engine_SSOT.py | 04c7d6ee26b487903b496e4c159164a5d2dd637df9b841a4a90c14d4646ca53b | 1,421 | 62,145 |

## Grafts Applied (Minimal Set)

### 1. Port: 8000 → 8014
- **Reason:** Wallet Finder operates on port 8014
- **Semantic impact:** NONE
- **Lines changed:** 3 (uvicorn.run, JS reference, command-line comment)

### 2. Titles: "HL Copy Dashboard/Engine" → "Wallet Proof Engine"
- **Reason:** Wallet Finder branding
- **Semantic impact:** NONE (cosmetic only)
- **Lines changed:** 6 (FastAPI title, <title> tags, <h2>, <b> elements)

### 3. /api/cache-health endpoint
- **Reason:** Monitoring endpoint required by Wallet Finder tooling
- **Semantic impact:** NONE (read-only wrapper around existing model-cache-status)
- **Lines added:** ~15

## Features Preserved from Original (NOT Changed)

### Accounting Semantics
- **Source:** Original hl_stage2 accounting functions — UNCHANGED
- `lead_equity`, `copy_equity` — identical to hl_stage2
- `lead_realised`, `copy_realised` — identical to hl_stage2
- Portfolio allocation — identical to hl_stage2
- Drawdown computation — identical to hl_stage2
- Fee/friction handling — identical to hl_stage2

### Settings/Defaults
- **Source:** Original hl_stage2 load_ui_state/save_ui_state — UNCHANGED
- DEFAULT_NORM_BASE, DEFAULT_FIXED_NOTIONAL, DEFAULT_FEE_BPS — identical
- Settings persistence via UI_STATE_FILE — identical to hl_stage2
- No hardcoded defaults overwriting saved values

### Render/Header
- **Source:** Original hl_stage2 render_home — UNCHANGED (except title strings)
- COMBINED PORTFOLIO — NON-USER WALLETS semantics preserved
- Header card derivation from selected_aggregate — identical
- Follower/user wallet row sourcing — identical

### Model/Cache Loading
- **Source:** Original hl_stage2 model system — UNCHANGED
- Non-blocking background warm-up via daemon thread
- Stale cache fallback with meta-refresh
- get_model_state_cached with max_age_sec
- _MODEL_REFRESH_STATUS and _MODEL_REFRESH_LOCK

## Features NOT Grafted (Deliberately Excluded)

- Current file's broken accounting formulas (_compute_copy_alloc, etc.)
- Current file's hardcoded setting defaults
- Current file's CACHE_FRESH/STALE constants (original uses simpler timestamp comparison)
- Current file's over-complicated cache-health infrastructure
- Current file's broken header/settings logic

## Proof of Correctness

1. Original hl_stage2 hashes verified — exact copy
2. Port change is trivial string replacement
3. Title changes are cosmetic string replacements
4. cache-health is read-only wrapper — no semantic changes
5. All accounting/settings/render logic is UNMODIFIED from original
6. Original already had non-blocking model warm-up — no deadlock
