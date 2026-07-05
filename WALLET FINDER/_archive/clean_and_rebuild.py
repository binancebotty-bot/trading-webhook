"""
clean_and_rebuild.py — Nuclear cleanup: quarantine old pipeline artifacts,
fix summary.csv trades cap, rebuild wallet_universe.csv from stage1.

Run AFTER stage1_simple.py completes. Produces clean data for 8012.

Steps:
  1. Quarantine stale pipeline files (old hl_stage1_pass, copyable_wallets, etc.)
  2. Fix summary.csv: replace trades column with correct counts from all_trades.csv
  3. Run build_universe_from_stage1.py → new wallet_universe.csv
  4. Validate output
"""
import csv
import json
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
QUARANTINE_DIR = DATA_DIR / "pre_pipeline_restart_03jul26"

# Files to quarantine (old pipeline artifacts that contaminate 8012)
QUARANTINE_TARGETS = [
    "hl_stage1_pass.csv",
    "hl_stage1_5_mtm_pass.csv",
    "copyable_wallets.csv",
    "wallet_gate.json",
    "equity_curves",           # old equity curve dir
]

# Active files that need fixing, not quarantine
SUMMARY_FILE = DATA_DIR / "summary.csv"
ALL_TRADES_FILE = DATA_DIR / "all_trades.csv"
PASS_FILE = DATA_DIR / "simple_filter_pass.csv"
UNIVERSE_FILE = DATA_DIR / "wallet_universe.csv"


def quarantine_old_files():
    """Move stale old-pipeline files to quarantine directory."""
    print("=" * 60)
    print("STEP 1: QUARANTINE OLD PIPELINE ARTIFACTS")
    print("=" * 60)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    quarantined = 0
    for name in QUARANTINE_TARGETS:
        src = DATA_DIR / name
        if not src.exists():
            continue
        dst = QUARANTINE_DIR / name
        if dst.exists():
            if dst.is_dir():
                shutil.rmtree(dst)
            else:
                dst.unlink()
        shutil.move(str(src), str(dst))
        print(f"  Quarantined: {name}")
        quarantined += 1

    print(f"  Total quarantined: {quarantined}")
    return quarantined


def fix_summary_trades():
    """Fix summary.csv trades column using correct counts from all_trades.csv.

    77 of 224 wallets in summary.csv have trades capped at exactly 2,000
    (from old universe_builder.py's userFillsByTime with 24h windows).
    all_trades.csv has the true counts (max 29,519).
    """
    print("\n" + "=" * 60)
    print("STEP 2: FIX SUMMARY.CSV TRADES COLUMN")
    print("=" * 60)

    if not SUMMARY_FILE.exists():
        print("  WARNING: summary.csv not found, skipping")
        return 0
    if not ALL_TRADES_FILE.exists():
        print("  WARNING: all_trades.csv not found, skipping fix")
        return 0

    # Build correct trade counts from all_trades.csv
    print("  Loading correct trade counts from all_trades.csv...")
    correct_trades = Counter()
    with open(ALL_TRADES_FILE, newline="") as f:
        for row in csv.DictReader(f):
            correct_trades[row["wallet"].lower()] += 1
    print(f"  {len(correct_trades)} wallets with trade data in all_trades.csv")

    # Fix summary.csv
    print("  Reading summary.csv...")
    with open(SUMMARY_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    print(f"  {len(rows)} wallets in summary.csv")

    fixed = 0
    already_ok = 0
    no_data = 0
    capped_at_2000 = 0

    for row in rows:
        w = row["wallet"].lower()
        old_trades = int(float(row.get("trades", 0)))

        if old_trades == 2000:
            capped_at_2000 += 1

        if w in correct_trades:
            new_trades = correct_trades[w]
            if new_trades != old_trades:
                row["trades"] = str(new_trades)
                fixed += 1
            else:
                already_ok += 1
        else:
            # Wallet not in all_trades.csv — leave as-is
            no_data += 1

    # Write fixed summary.csv
    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  Results:")
    print(f"    Capped at 2,000 (found): {capped_at_2000}")
    print(f"    Fixed (trades corrected): {fixed}")
    print(f"    Already correct: {already_ok}")
    print(f"    No data in all_trades.csv: {no_data}")

    # Verify fix
    verify_trades = []
    with open(SUMMARY_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            verify_trades.append(int(float(row.get("trades", 0))))
    still_capped = sum(1 for t in verify_trades if t == 2000)
    print(f"\n  Verification: {still_capped} wallets still at 2,000 after fix")

    return fixed


def run_bridge_script():
    """Run build_universe_from_stage1.py to produce new wallet_universe.csv."""
    print("\n" + "=" * 60)
    print("STEP 3: BUILD NEW WALLET_UNIVERSE.CSV")
    print("=" * 60)

    # Check prerequisite
    if not PASS_FILE.exists():
        print(f"  ERROR: {PASS_FILE} not found. stage1_simple.py must complete first.")
        return False

    pass_count = sum(1 for _ in open(PASS_FILE)) - 1  # subtract header
    print(f"  Stage1 pass list: {pass_count} wallets")

    # Run bridge script
    bridge_script = BASE_DIR / "build_universe_from_stage1.py"
    print(f"  Running {bridge_script.name}...")
    result = subprocess.run(
        [sys.executable, str(bridge_script)],
        capture_output=True, text=True, cwd=str(BASE_DIR)
    )

    if result.returncode != 0:
        print(f"  ERROR: Bridge script failed (exit {result.returncode})")
        print(f"  stdout: {result.stdout[-500:]}")
        print(f"  stderr: {result.stderr[-500:]}")
        return False

    print(result.stdout)
    return True


def validate_output():
    """Validate the new wallet_universe.csv and check for contamination."""
    print("\n" + "=" * 60)
    print("STEP 4: VALIDATE OUTPUT")
    print("=" * 60)

    if not UNIVERSE_FILE.exists():
        print(f"  ERROR: {UNIVERSE_FILE} not found!")
        return False

    with open(UNIVERSE_FILE, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = reader.fieldnames

    print(f"  Wallet universe: {len(rows)} wallets")
    print(f"  Columns: {fields}")

    # Check for 2,000 trade cap contamination
    trades_2000 = 0
    trades_vals = []
    for row in rows:
        t = row.get("trades", "")
        if t:
            try:
                tv = int(float(t))
                trades_vals.append(tv)
                if tv == 2000:
                    trades_2000 += 1
            except (ValueError, TypeError):
                pass

    print(f"\n  TRADES column check:")
    print(f"    Wallets with trades data: {len(trades_vals)}")
    print(f"    Capped at exactly 2,000: {trades_2000}")
    if trades_vals:
        print(f"    Range: {min(trades_vals)} to {max(trades_vals)}")

    # Check for spot contamination in DD
    dd_vals = [float(row.get("max_dd_pct", 0)) for row in rows if row.get("max_dd_pct")]
    if dd_vals:
        print(f"\n  DD% column check:")
        print(f"    Wallets with DD%: {len(dd_vals)}")
        print(f"    Range: {min(dd_vals):.2f}% to {max(dd_vals):.2f}%")
        print(f"    All < 40%: {all(d < 40 for d in dd_vals)}")

    # Check for stale old-pipeline wallets
    if rows:
        sample_wallets = [r["wallet"][:20] for r in rows[:3]]
        print(f"\n  Sample wallets: {sample_wallets}")

    # Summary.csv contamination check
    if SUMMARY_FILE.exists():
        with open(SUMMARY_FILE, newline="", encoding="utf-8") as f:
            sum_rows = list(csv.DictReader(f))
        sum_trades_2000 = sum(
            1 for r in sum_rows
            if int(float(r.get("trades", 0))) == 2000
        )
        print(f"\n  Summary.csv check:")
        print(f"    Total wallets: {len(sum_rows)}")
        print(f"    Still capped at 2,000: {sum_trades_2000}")

    ok = trades_2000 == 0
    print(f"\n  {'PASS' if ok else 'FAIL'}: No 2,000 trade cap contamination")
    return ok


def main():
    t0 = time.time()
    print("CLEAN AND REBUILD — NUCLEAR PIPELINE CLEANUP")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Step 1: Quarantine old files
    quarantine_old_files()

    # Step 2: Fix summary.csv trades
    fix_summary_trades()

    # Step 3: Rebuild wallet_universe.csv
    ok = run_bridge_script()
    if not ok:
        print("\nABORT: Bridge script failed")
        sys.exit(1)

    # Step 4: Validate
    clean = validate_output()

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"DONE in {elapsed:.1f}s")
    print(f"Pipeline {'CLEAN' if clean else 'NEEDS ATTENTION'}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
