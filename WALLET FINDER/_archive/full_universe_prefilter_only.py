"""
full_universe_prefilter_only.py — BOUNDED MODE: real DD prefilter only.

Runs ONLY the real DD prefilter from full_universe_scan.py without
the expensive simulation loop. Processes all wallets from all_trades.csv
and produces the prefilter audit CSV + pass/fail counts.

NO simulations, NO proportional sweep, NO expensive computation.
Estimated runtime: <60s (reads 4.97M row CSV for trade counts, then
checks cached portfolio JSONs for each qualifying wallet).
"""
import csv
import json
import os
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
DATA_DIR = _HERE / "data"
sys.path.insert(0, str(_HERE))

from real_dd_filter import wallet_passes_real_dd

# ── Config ──────────────────────────────────────────────────────
MAX_REAL_DD_PCT = float(os.getenv("HL_FULL_UNIVERSE_MAX_REAL_DD_PCT", "50"))
MIN_TRADES = int(os.getenv("HL_FULL_UNIVERSE_MIN_TRADES", "50"))
OUTPUT_FILE = DATA_DIR / "full_universe_real_dd_prefilter.csv"
PREFILTER_SUMMARY = DATA_DIR / "full_universe_prefilter_summary.json"


def main():
    t0 = time.time()
    print(f"=== FULL UNIVERSE PREFILTER (BOUNDED MODE) — {MAX_REAL_DD_PCT}% DD gate ===")
    print(f"Min trades: {MIN_TRADES}")
    print()

    # Step 1: Get wallet trade counts from all_trades.csv
    print("Loading all_trades.csv for wallet trade counts...")
    from trades_path import trades_csv
    trades_path = trades_csv()
    print(f"  Reading: {trades_path}")

    trade_counts = {}
    with open(trades_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = row["wallet"].strip().lower()
            trade_counts[w] = trade_counts.get(w, 0) + 1

    qualifying = [w for w, c in trade_counts.items() if c >= MIN_TRADES]
    print(f"  Total wallets in all_trades: {len(trade_counts)}")
    print(f"  Wallets with {MIN_TRADES}+ trades: {len(qualifying)}")

    # Step 2: Run real DD prefilter
    print(f"\nRunning real DD prefilter ({MAX_REAL_DD_PCT}% gate)...")
    prefilter_rows = []
    passing = []
    rejected = []
    blocked = []

    for i, w in enumerate(qualifying):
        result = wallet_passes_real_dd(w, MAX_REAL_DD_PCT)
        result["n_trades"] = trade_counts[w]
        prefilter_rows.append(result)
        if result["dd_gate_pass"]:
            passing.append(result)
        elif result["dd_source"] == "DATA_FETCH_BLOCKED":
            blocked.append(result)
        else:
            rejected.append(result)

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            print(f"  Progress: {i+1}/{len(qualifying)} ({elapsed:.1f}s)")

    # Step 3: Write outputs
    if prefilter_rows:
        fieldnames = list(prefilter_rows[0].keys())
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(prefilter_rows)

    # Write summary JSON
    summary = {
        "threshold_pct": MAX_REAL_DD_PCT,
        "min_trades": MIN_TRADES,
        "total_wallets_in_all_trades": len(trade_counts),
        "qualifying_wallets": len(qualifying),
        "passed": len(passing),
        "rejected_real_dd_too_high": len(rejected),
        "blocked_no_data": len(blocked),
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    with open(PREFILTER_SUMMARY, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Print results
    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"FULL UNIVERSE PREFILTER COMPLETE ({elapsed:.1f}s)")
    print(f"  Threshold:         {MAX_REAL_DD_PCT}%")
    print(f"  Total wallets:     {len(trade_counts)}")
    print(f"  Qualifying:        {len(qualifying)} (≥{MIN_TRADES} trades)")
    print(f"  Passed (≤DD):      {len(passing)}")
    print(f"  Rejected (>DD):    {len(rejected)}")
    print(f"  Blocked (no data): {len(blocked)}")
    print(f"  Output:            {OUTPUT_FILE}")
    print(f"  Summary:           {PREFILTER_SUMMARY}")

    # Show rejected
    if rejected:
        rejected_sorted = sorted(rejected, key=lambda r: r.get("real_max_dd_pct", 0) or 0, reverse=True)
        print(f"\nTop rejected wallets (highest real DD):")
        for r in rejected_sorted[:15]:
            dd = r.get("real_max_dd_pct", "?")
            trades = r.get("n_trades", "?")
            print(f"  {r['wallet'][:10]}…{r['wallet'][-6:]} | DD={dd}% | trades={trades}")

    # Show blocked
    if blocked:
        print(f"\nBlocked wallets (no portfolio JSON):")
        for r in blocked[:10]:
            print(f"  {r['wallet'][:10]}…{r['wallet'][-6:]} | trades={r.get('n_trades', '?')}")


if __name__ == "__main__":
    main()
