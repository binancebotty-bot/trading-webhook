# UI 2 Blocker Report - Live Copy Dashboard SSOT (Port 8014)

## Status: BLOCKED_WITH_EXACT_REASON

### Symptom
Uvicorn server starts successfully ("Application startup complete"), binds to port 8014, accepts TCP connections, but ALL HTTP requests timeout indefinitely (HTTP 000 after 15+ seconds), including endpoints that do not access the model cache (e.g., /api/cache-health).

### Root Cause
GIL starvation from the background thread's model build. The _build() function in _startup_build spawns a daemon thread that:
1. Checks cache freshness (determined to be FRESH after cache touch fix)
2. Loads 518MB app_model_state.json via json.load()
3. Processes the data into model structures

Even on the fast path (cache fresh), Python's GIL prevents the main asyncio event loop from processing HTTP requests while the background thread holds the GIL during JSON parsing and model construction. The 518MB JSON with 117k+ copy_trades and 193k+ expected_copy_fills causes extended GIL hold times.

### Attempted Fixes
1. BOM removal from HL_Copy_App_SSOT.py - SUCCESS (file now imports cleanly)
2. Using python -m uvicorn (correct Python env with pandas) - SUCCESS  
3. Touch cache file to force FRESH status - SUCCESS (cache confirmed fresh)
4. --no-access-log / --log-level warning (pipe buffer theory) - NO EFFECT
5. Extended wait times (5+ minutes) - NO EFFECT

### Recommended Fix
Use multiprocessing (ProcessPoolExecutor) instead of threading for model building. This bypasses the GIL entirely:



Alternatively, pre-build the model state synchronously before starting uvicorn:



### Evidence
- Server starts: INFO: Uvicorn running on http://0.0.0.0:8014
- TCP connection accepted: netstat shows ESTABLISHED
- HTTP response: 000 after 15s timeout for all endpoints
- Import time: 1.6s (fast)
- JSON load time: 2.8s (acceptable)
- build_model_state(): >180s (blocks GIL)
