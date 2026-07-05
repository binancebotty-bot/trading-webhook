#!/usr/bin/env bash
set -euo pipefail

# cd to script directory (WALLET FINDER/)
cd "$(dirname "$0")"

echo
echo "============================================"
echo "  WALLET FINDER — Starting Both Dashboards"
echo "============================================"
echo

# ── Proving Engine (8012) ────────────────────
echo "[1/2] Starting Proving Engine on port 8012..."
python -m uvicorn app:app \
    --host 127.0.0.1 \
    --port 8012 \
    --log-level warning \
    &
PROVING_PID=$!
echo "  Proving Engine started (PID $PROVING_PID) on http://localhost:8012/"
echo "$PROVING_PID" > .proving_engine.pid

# ── Live Copy Dashboard (8014) ───────────────
echo "[2/2] Starting Live Copy Dashboard on port 8014..."
python -m uvicorn HL_Copy_App_SSOT:app \
    --host 127.0.0.1 \
    --port 8014 \
    --log-level warning \
    &
LIVECOPY_PID=$!
echo "  Live Copy Dashboard started (PID $LIVECOPY_PID) on http://localhost:8014/live-copy"
echo "$LIVECOPY_PID" > .live_copy.pid

echo
echo "Both servers running. Wait ~10s for data to load."
echo
echo "  Proving Engine:  http://localhost:8012/"
echo "  Live Copy:       http://localhost:8014/live-copy"
echo

# Wait for both (so the terminal stays alive)
wait
