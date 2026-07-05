# Root Cause: Post-Commit 8014 Load Failure

**Date:** 2026-07-01
**Proof Folder:** POST_COMMIT_8014_LOAD_FAILURE_20260701_113711
**Resolved:** Yes

## Symptom

http://localhost:8014 was stuck showing only:
```
Building model state from fills...
This page will auto-refresh in 5 seconds.
```

The Wallet Proof Engine UI never loaded despite the process running for ~1 hour.

## Root Cause

The stale cache (app_model_state.json, 542MB) silently failed to load during startup. The `_build()` function at line ~393 used `except Exception: pass` which swallowed the error (likely MemoryError or JSONDecodeError). Because `_MODEL_CACHE.get("state")` remained None, and the conditional gate at line ~399 checked `if _MODEL_CACHE.get("state") is not None:` before triggering the background refresh, the app entered a permanent deadlock: the background refresh was never started, and the home page showed "Building model state..." forever.

The uvicorn log from a subsequent failed restart attempt showed:
```
[cache] STALE — loading old cache for instant UI, then rebuilding
[startup] Home page pre-rendered from stale cache
ERROR: [Errno 10048] error while attempting to bind on address ('127.0.0.1', 8014)
```

But that was PID 19776 (failed restart). The running process (PID 57448) never had a successful stale cache load.

## Evidence

- cache-health: status=STALE, model_state_present=false
- build_count: 0 (never rebuilt)
- refresh_status.last_marker: "IDLE" (never started)
- app_model_state.json: EXISTS, 542MB, modified Jun 27
- raw_live_fills.csv: EXISTS, 291MB, modified May 29

## Fix Applied

**Fix 1 — _build() stale cache handling (lines ~392-406):**
1. Replaced `except Exception: pass` with `except Exception as exc: print(...)` to log the error
2. Preserved original gate ordering: `if _MODEL_CACHE.get("state") is not None:` first
3. Added fall-through log message when state is None

**Fix 2 — home() last-resort disk load (lines ~10167-10191):**
1. Before showing "Building model state...", attempt to load APP_MODEL_STATE_JSON from disk
2. If load succeeds, render normally with stale data
3. If load fails, trigger background refresh and show wait page

## Result

After restart with fixes:
- model_state_present: True
- UI loads Wallet Proof Engine with TRACKED WALLETS
- Background refresh running (STALE_REBUILDING)
- Page no longer shows "Building model state..."

## Source Hash Changes

- Before fix: e684a82aeb17bbd5ca2bf658f2403950210e025c632ca05de59e3b059a1864b3 (committed dfc90e0)
- After fix: 0f2e279f7bb36b372bd233a4089a1e002b46deda8c1808dbdf7fc80aed95b713
