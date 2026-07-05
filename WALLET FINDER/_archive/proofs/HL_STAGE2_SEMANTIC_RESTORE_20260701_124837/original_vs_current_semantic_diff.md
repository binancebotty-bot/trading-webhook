# Semantic Diff: Original hl_stage2 vs Current WALLET FINDER

## A. UI Settings/Defaults/Persistence

| Aspect | Original hl_stage2 | Current WALLET FINDER (broken) |
|--------|-------------------|-------------------------------|
| load_ui_state location | Line 765 | Line 12608 |
| save_ui_state location | Line 799 | Line 12637 |
| Default enforcement | Centralized max() calls | Fragmented None checks across file |
| Config system | Single-tier UI state | Two-tier (cfg + ui) confusion |
| Settings reset bug | NOT present | Present — defaults overwrite saved values |

## B. Accounting Semantics

| Aspect | Original hl_stage2 | Current WALLET FINDER (broken) |
|--------|-------------------|-------------------------------|
| copy_equity formula | alloc + realized + unrealized | copy_alloc + realized + unrealized |
| alloc source | Same alloc for lead & copy | _compute_copy_alloc (line 1730) derives separate copy_alloc |
| Accounting scope | Lines 547-593 (compact) | Lines 1649-1767 + 6588-6878 (spread across file) |
| Portfolio delta | Correct | Broken (wrong copy_alloc base) |

## C. Drawdown Semantics

| Aspect | Original hl_stage2 | Current WALLET FINDER (broken) |
|--------|-------------------|-------------------------------|
| Drawdown method | Original summed rows/portfolio curve | Potentially changed (4K+ lines added) |

## D. Render/Header Semantics

| Aspect | Original hl_stage2 | Current WALLET FINDER (broken) |
|--------|-------------------|-------------------------------|
| App title | HL Copy Engine | Wallet Proof Engine |
| render_home params | (state) | (state, refresh_chart=True) |
| Header cards | Derived from selected_aggregate | Changed — absurd figures reported |
| COMBINED PORTFOLIO | Correct NON-USER WALLETS | Confused with follower/copy figures |
| Follower/user row | Correctly sourced | Wrong — combined with non-user |

## E. Performance/Loading

| Aspect | Original hl_stage2 | Current WALLET FINDER |
|--------|-------------------|----------------------|
| Model warm-up | Daemon thread, non-blocking | CACHE_FRESH/STALE system |
| Stale cache fallback | Warming shell with meta-refresh | Yes but with deadlock bug |
| Background refresh | _kick_model_cache_refresh_background | Same (but deadlocked when stale load fails) |
| Cache-health endpoint | /api/model-cache-status | /api/cache-health (more complex) |
| Lines of code | 9,940 | 14,063 (+4,123) |
| File size | 598,872 bytes | 465,156 bytes (-133,716) |

## Root Cause Summary

The current file added 4,123 lines but lost 133,716 bytes — indicating extensive structural changes:
1. Stripped inline data/content from original
2. Added fragmented accounting with _compute_copy_alloc
3. Added two-tier config system that conflicts with original defaults
4. Spread accounting logic across many more lines (harder to reason about)
5. Introduced CACHE_FRESH/STALE deadlock via silent exception swallowing

The original hl_stage2 is semantically correct and has no deadlock — the cache warm-up is properly non-blocking.
