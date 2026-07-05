@echo off
cd /d "%~dp0"

echo.
echo ============================================
echo   WALLET FINDER — Starting Both Dashboards
echo ============================================
echo.

:: ── Proving Engine (8012) ────────────────────
echo [1/2] Starting Proving Engine on port 8012...
start "Proving Engine :8012" /MIN python -m uvicorn app:app --host 127.0.0.1 --port 8012 --log-level warning
echo   Proving Engine launching on http://localhost:8012/

:: ── Live Copy Dashboard (8014) ───────────────
echo [2/2] Starting Live Copy Dashboard on port 8014...
start "Live Copy :8014" /MIN python -m uvicorn HL_Copy_App_SSOT:app --host 127.0.0.1 --port 8014 --log-level warning
echo   Live Copy Dashboard launching on http://localhost:8014/live-copy

echo.
echo Both servers starting. Wait ~10s for data to load.
echo.
echo   Proving Engine:  http://localhost:8012/
echo   Live Copy:       http://localhost:8014/live-copy
echo.
pause
