@echo off
cd /d "%~dp0"

:: Hyperliquid meters /info per source IP, not per process. These dashboards
:: share that budget with the Build-4 copy runtime and the SSOT tracker, and
:: share nothing else. The limit is the sum of the PARTICIPATING slices --
:: Build-4 runs from its own frozen release and does not participate yet.
if not defined HL_IP_BUDGET_PATH set "HL_IP_BUDGET_PATH=C:\Users\Public\HyperliquidProduction\ip_weight_budget.bin"
if not defined HL_IP_BUDGET_WEIGHT_PER_MIN set "HL_IP_BUDGET_WEIGHT_PER_MIN=250"

echo.
echo ============================================
echo   WALLET FINDER — Starting Both Dashboards
echo ============================================
echo.

:: ── Proving Engine (8012) ────────────────────
echo [1/2] Starting Wallet Talent Scout on port 8012...
start "Wallet Talent Scout :8012" /MIN python -m uvicorn wallet_talent_scout_8012:app --host 127.0.0.1 --port 8012 --log-level warning
echo   Wallet Talent Scout launching on http://localhost:8012/

:: ── Live Copy Dashboard (8014) ───────────────
echo [2/2] Starting Wallet Proof Engine on port 8014...
start "Wallet Proof Engine :8014" /MIN python -m uvicorn wallet_proof_engine_8014:app --host 127.0.0.1 --port 8014 --log-level warning
echo   Wallet Proof Engine launching on http://localhost:8014/

echo.
echo Both servers starting. Wait ~10s for data to load.
echo.
echo   Wallet Talent Scout:  http://localhost:8012/
echo   Wallet Proof Engine:  http://localhost:8014/
echo.
pause
