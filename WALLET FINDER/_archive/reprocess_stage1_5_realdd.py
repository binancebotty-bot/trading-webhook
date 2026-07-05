"""
reprocess_stage1_5_realdd.py — Offline reprocessor for Stage 1.5 output.

Reads the EXISTING hl_stage1_5_mtm_pass.csv + cached portfolio JSONs,
applies the real DD gate, and writes fresh output with real DD columns.
NO API calls — purely offline computation from cached data.
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

from real_dd_filter import real_dd_gate, load_real_dd_for_wallet

# ── Config ──────────────────────────────────────────────────────
REAL_DD_PCT = float(os.getenv("HL_S1_5_MAX_REAL_DD_PCT", "50"))
INPUT_FILE = DATA_DIR / "hl_stage1_5_mtm_pass.csv"
OUTPUT_FILE = DATA_DIR / "hl_stage1_5_mtm_pass_realdd.csv"
REJECTED_FILE = DATA_DIR / "rejected_high_dd_wallets.csv"
BLOCKED_FILE = DATA_DIR / "DATA_FETCH_BLOCKED_wallets.csv"

# Existing columns in Stage 1.5 output
STAGE1_5_COLUMNS = [
    "wallet", "mtm_calmar", "month_pnl_chg_mtm", "month_acctV_end",
    "month_acctV_peak", "max_drawdown_mtm", "allTime_max_drawdown_mtm",
    "allTime_pnl_chg_mtm", "allTime_acctV_peak", "allTime_vlm",
    "equity_collapse_flag_mtm", "negative_total_flag_mtm", "mtm_source",
    "mtm_fetched_at", "copyability_gate",
]
REAL_DD_COLUMNS = [
    "real_max_dd_usd", "real_max_dd_pct", "dd_source",
    "dd_gate_pass", "dd_gate_reason",
]
OUTPUT_COLUMNS = STAGE1_5_COLUMNS + REAL_DD_COLUMNS


def main():
    t0 = time.time()
    print(f"=== REPROCESS STAGE 1.5 WITH REAL DD GATE ({REAL_DD_PCT}%) ===")
    print(f"Input:  {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    if not INPUT_FILE.exists():
        print(f"[!] Missing input: {INPUT_FILE}")
        sys.exit(1)

    # Read existing Stage 1.5 output
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} wallets from existing Stage 1.5 output")

    # Process each wallet
    passed = []
    rejected = []
    blocked = []

    for row in rows:
        wallet = row["wallet"].strip().lower()
        # Build stats dict from existing row
        stats = {}
        for k in STAGE1_5_COLUMNS:
            v = row.get(k)
            # Convert numeric fields
            if k in ("mtm_calmar", "month_pnl_chg_mtm", "month_acctV_end",
                      "month_acctV_peak", "max_drawdown_mtm",
                      "allTime_max_drawdown_mtm", "allTime_pnl_chg_mtm",
                      "allTime_acctV_peak", "allTime_vlm"):
                try:
                    stats[k] = float(v) if v and v.strip() else None
                except (ValueError, AttributeError):
                    stats[k] = None
            elif k in ("equity_collapse_flag_mtm", "negative_total_flag_mtm"):
                try:
                    stats[k] = int(v) if v and v.strip() else None
                except (ValueError, AttributeError):
                    stats[k] = None
            else:
                stats[k] = v

        # Apply real DD gate using cached portfolio JSONs
        dd_result = real_dd_gate(stats, REAL_DD_PCT)

        # Merge real DD columns into row
        out_row = {**row}
        for col in REAL_DD_COLUMNS:
            out_row[col] = dd_result.get(col, "")

        if dd_result["dd_gate_pass"]:
            passed.append(out_row)
        elif dd_result["dd_source"] == "DATA_FETCH_BLOCKED":
            blocked.append(out_row)
        else:
            rejected.append(out_row)

    # Write outputs
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(passed)

    if rejected:
        with open(REJECTED_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rejected)

    if blocked:
        with open(BLOCKED_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(blocked)

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"REAL DD REPROCESSING COMPLETE ({elapsed:.1f}s)")
    print(f"  Input wallets:   {len(rows)}")
    print(f"  Passed (≤{REAL_DD_PCT}% DD): {len(passed)}")
    print(f"  Rejected (>DD):  {len(rejected)}")
    print(f"  Blocked (no data): {len(blocked)}")
    print(f"  Output:          {OUTPUT_FILE}")
    if rejected:
        print(f"  Rejected list:   {REJECTED_FILE}")
    if blocked:
        print(f"  Blocked list:    {BLOCKED_FILE}")

    # Show rejected wallets
    if rejected:
        print(f"\nRejected wallets (real DD > {REAL_DD_PCT}%):")
        for r in rejected[:20]:
            dd_pct = r.get("real_max_dd_pct", "?")
            reason = r.get("dd_gate_reason", "?")
            print(f"  {r['wallet'][:10]}…{r['wallet'][-6:]} | DD={dd_pct}% | {reason}")

    # Show blocked wallets
    if blocked:
        print(f"\nBlocked wallets (no portfolio data):")
        for r in blocked[:20]:
            print(f"  {r['wallet'][:10]}…{r['wallet'][-6:]} | {r.get('dd_gate_reason', '?')}")


if __name__ == "__main__":
    main()
