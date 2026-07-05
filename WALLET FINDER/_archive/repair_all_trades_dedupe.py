"""
repair_all_trades_dedupe.py — One-shot dedup of data/all_trades.csv

Removes exact full-row duplicates (all 7 fields match).
Writes deduped output to data/all_trades_clean.csv.
Prints stats before/after.

Usage:
    python repair_all_trades_dedupe.py
    python repair_all_trades_dedupe.py --in-place   # DANGEROUS: may remove legitimate identical fills
"""
import csv
import os
import sys

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
INPUT_FILE = os.path.join(DATA_DIR, "all_trades.csv")
CLEAN_FILE = os.path.join(DATA_DIR, "all_trades_clean.csv")
FIELDS = ["wallet", "time", "coin", "side", "px", "sz", "closedPnl"]


def canonical_key(row):
    return (
        str(row.get("wallet", "")).strip().lower(),
        str(row.get("time", "")).strip(),
        str(row.get("coin", "")).strip().upper(),
        str(row.get("side", "")).strip().lower(),
        str(row.get("px", "")).strip(),
        str(row.get("sz", "")).strip(),
        str(row.get("closedPnl", 0)).strip(),
    )


def dedup():
    in_place = "--in-place" in sys.argv
    if in_place and "--i-understand-identical-fills-can-be-real" not in sys.argv:
        print("[!] Refusing --in-place: identical visible fill rows can be legitimate Hyperliquid fills.")
        print("    Re-run with --in-place --i-understand-identical-fills-can-be-real only for a known duplicated file.")
        sys.exit(2)

    if not os.path.exists(INPUT_FILE):
        print(f"[!] Not found: {INPUT_FILE}")
        sys.exit(1)

    total = 0
    seen = set()
    unique_rows = []
    dupes_by_wallet = {}

    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            key = canonical_key(row)
            if key in seen:
                wallet = str(row.get("wallet", "")).strip().lower()
                dupes_by_wallet[wallet] = dupes_by_wallet.get(wallet, 0) + 1
                continue
            seen.add(key)
            unique_rows.append(row)

    removed = total - len(unique_rows)
    print(f"=== REPAIR DEDUP REPORT ===")
    print(f"Input:       {INPUT_FILE}")
    print(f"Total rows:  {total:,}")
    print(f"Unique rows: {len(unique_rows):,}")
    print(f"Duplicates:  {removed:,}")
    if removed:
        print(f"\nTop wallets by dupe count:")
        for w, c in sorted(dupes_by_wallet.items(), key=lambda x: -x[1])[:10]:
            print(f"  {w}: {c:,}")
    else:
        print("\nNo duplicates found. File is clean.")

    if removed == 0:
        print("\nNothing to do.")
        return

    out_path = CLEAN_FILE if not in_place else INPUT_FILE
    tmp_path = out_path + ".tmp"

    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(unique_rows)

    os.replace(tmp_path, out_path)
    print(f"\nWrote {len(unique_rows):,} unique rows -> {out_path}")

    if not in_place:
        orig_size = os.path.getsize(INPUT_FILE)
        clean_size = os.path.getsize(out_path)
        print(f"Original kept: {INPUT_FILE} ({orig_size:,} bytes)")
        print(f"Clean copy:    {out_path} ({clean_size:,} bytes)")
        print(f"\nNext steps:")
        print(f"  1. Verify: python -c \"import pandas as pd; df=pd.read_csv('{CLEAN_FILE}'); print(df.shape, df.duplicated().sum())\"")
        print(f"  2. Replace: move {CLEAN_FILE} -> {INPUT_FILE}")
        print(f"  3. Or re-run with --in-place to replace directly")


if __name__ == "__main__":
    dedup()
