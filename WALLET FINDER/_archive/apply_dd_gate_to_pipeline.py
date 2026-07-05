"""
apply_dd_gate_to_pipeline.py — Offline re-filtering of pipeline outputs.

Applies the real DD gate (allTime accountValueHistory) to:
  1. summary.csv  → filtered_summary.csv  (same columns, fewer rows)
  2. wallet_universe.csv → filtered_wallet_universe.csv
  3. copyable_wallets.csv → filtered_copyable_wallets.csv

Then OVERWRITES the originals so the 8012 dashboard reads clean data.

No API calls. Pure CSV manipulation using existing MTM columns.
"""
import csv
import json
import os
import shutil
from pathlib import Path

BASE = Path(__file__).parent
DATA = BASE / "data"
PORTF_DIR = BASE / "copy_selection_run" / "wallet_portfolios"

MAX_REAL_DD_PCT = float(os.getenv("HL_S1_5_MAX_REAL_DD_PCT", "50.0"))

SUMMARY = DATA / "summary.csv"
UNIVERSE = DATA / "wallet_universe.csv"
COPYABLE = DATA / "copyable_wallets.csv"


def calc_dd_from_json(wallet_full: str) -> dict | None:
    """Fallback: compute DD directly from portfolio JSON if CSV columns missing."""
    path = PORTF_DIR / f"{wallet_full}.json"
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        periods = {}
        for entry in data:
            if isinstance(entry, list) and len(entry) == 2:
                periods[entry[0]] = entry[1]
        allt = periods.get("allTime", {})
        avh = allt.get("accountValueHistory", [])
        if not avh:
            return None
        vals = [float(v) for _, v in avh]
        peak = vals[0]
        mdd = 0.0
        peak_global = vals[0]
        for v in vals:
            if v > peak:
                peak = v
            if v > peak_global:
                peak_global = v
            if v - peak < mdd:
                mdd = v - peak
        return {"dd_usd": abs(mdd), "dd_pct": abs(mdd) / peak_global * 100 if peak_global > 0 else 0, "peak": peak_global}
    except Exception:
        return None


def compute_real_dd(row: dict) -> tuple[float, float, str]:
    """Return (dd_usd, dd_pct, source) from a summary.csv row.
    
    Priority:
      1. allTime_max_drawdown_mtm / allTime_acctV_peak (from Stage 1.5 MTM fetch)
      2. Portfolio JSON fallback
      3. Return (0, 0, 'unknown') if nothing available
    """
    at_dd = row.get("allTime_max_drawdown_mtm", "")
    at_peak = row.get("allTime_acctV_peak", "")
    
    if at_dd and at_peak:
        try:
            dd_usd = abs(float(at_dd))
            peak = float(at_peak)
            if peak > 1:
                return dd_usd, dd_usd / peak * 100, "mtm_alltime"
        except (ValueError, TypeError):
            pass
    
    # Fallback to portfolio JSON
    wallet = row.get("wallet", "").strip().lower()
    if wallet:
        json_dd = calc_dd_from_json(wallet)
        if json_dd:
            return json_dd["dd_usd"], json_dd["dd_pct"], "portfolio_json"
    
    return 0.0, 0.0, "unknown"


def filter_csv(input_path: Path, output_path: Path, dd_threshold: float) -> dict:
    """Filter a CSV by real DD gate. Returns stats."""
    if not input_path.exists():
        return {"error": f"{input_path.name} not found"}
    
    with open(input_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    passed = []
    rejected = []
    blocked = []
    
    for row in rows:
        dd_usd, dd_pct, source = compute_real_dd(row)
        
        if source == "unknown":
            # No MTM data available — keep but flag (don't block)
            row["_real_dd_pct"] = ""
            row["_real_dd_source"] = "no_mtm_data"
            passed.append(row)
        elif dd_pct <= dd_threshold:
            row["_real_dd_pct"] = f"{dd_pct:.1f}"
            row["_real_dd_source"] = source
            passed.append(row)
        else:
            row["_real_dd_pct"] = f"{dd_pct:.1f}"
            row["_real_dd_source"] = source
            rejected.append(row)
    
    # Write filtered CSV (without the internal _real_dd columns)
    out_fields = [f for f in fieldnames if not f.startswith("_real_dd")]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(passed)
    
    return {
        "total": len(rows),
        "passed": len(passed),
        "rejected": len(rejected),
        "rejected_wallets": [(r["wallet"], r.get("_real_dd_pct", "?"), r.get("_real_dd_source", "?")) for r in rejected],
    }


def main():
    print(f"Real DD Gate: {MAX_REAL_DD_PCT}% threshold")
    print("=" * 80)
    
    # Backup originals
    backup_dir = DATA / "pre_dd_gate_backup"
    backup_dir.mkdir(exist_ok=True)
    for f in [SUMMARY, UNIVERSE, COPYABLE]:
        if f.exists():
            dst = backup_dir / f.name
            if not dst.exists():
                shutil.copy2(f, dst)
                print(f"Backed up: {f.name} -> {dst}")
    
    # 1. Filter summary.csv
    print(f"\n--- Filtering summary.csv ---")
    summary_result = filter_csv(SUMMARY, SUMMARY, MAX_REAL_DD_PCT)
    print(f"  Total: {summary_result['total']}, Passed: {summary_result['passed']}, Rejected: {summary_result['rejected']}")
    if summary_result.get("rejected_wallets"):
        print(f"  Rejected wallets (DD > {MAX_REAL_DD_PCT}%):")
        for w, dd, src in summary_result["rejected_wallets"]:
            print(f"    {w[:12]}... DD={dd}% source={src}")
    
    # 2. Filter wallet_universe.csv — only keep wallets that are still in summary
    print(f"\n--- Filtering wallet_universe.csv ---")
    # Reload the filtered summary wallet set
    with open(SUMMARY, encoding="utf-8") as f:
        kept_wallets = {row["wallet"].strip().lower() for row in csv.DictReader(f)}
    
    with open(UNIVERSE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        universe_fields = reader.fieldnames
        universe_rows = list(reader)
    
    universe_passed = [r for r in universe_rows if r["wallet"].strip().lower() in kept_wallets]
    universe_rejected = [r for r in universe_rows if r["wallet"].strip().lower() not in kept_wallets]
    
    with open(UNIVERSE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=universe_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(universe_passed)
    
    print(f"  Total: {len(universe_rows)}, Passed: {len(universe_passed)}, Rejected: {len(universe_rejected)}")
    for r in universe_rejected[:10]:
        print(f"    Removed: {r['wallet'][:12]}... (score={r.get('score', '?')})")
    
    # 3. Filter copyable_wallets.csv
    print(f"\n--- Filtering copyable_wallets.csv ---")
    with open(COPYABLE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        copyable_fields = reader.fieldnames
        copyable_rows = list(reader)
    
    copyable_passed = [r for r in copyable_rows if r["wallet"].strip().lower() in kept_wallets]
    copyable_rejected = [r for r in copyable_rows if r["wallet"].strip().lower() not in kept_wallets]
    
    with open(COPYABLE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=copyable_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(copyable_passed)
    
    print(f"  Total: {len(copyable_rows)}, Passed: {len(copyable_passed)}, Rejected: {len(copyable_rejected)}")
    for r in copyable_rejected[:10]:
        print(f"    Removed: {r['wallet'][:12]}...")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"PIPELINE RE-FILTER COMPLETE")
    print(f"{'='*80}")
    print(f"  DD threshold: {MAX_REAL_DD_PCT}%")
    print(f"  summary.csv:      {summary_result['passed']}/{summary_result['total']} wallets")
    print(f"  wallet_universe:  {len(universe_passed)}/{len(universe_rows)} wallets")
    print(f"  copyable_wallets: {len(copyable_passed)}/{len(copyable_rows)} wallets")
    print(f"  Backups saved to: {backup_dir}")
    
    # Cross-check: which of the original 10 dashboard wallets survived?
    print(f"\n--- Cross-check: Original 10 dashboard wallets ---")
    original_10 = [
        "0x716d7ae4", "0xfbf4b6bf", "0x6f83ab88", "0x4d293ca7",
        "0x67ab0e9e", "0x6dfda7c3", "0x1b5c2bbf", "0x5ce7b350",
        "0x373b036b", "0x98d0e608",
    ]
    for prefix in original_10:
        # Find in rejected list
        found_rejected = any(w.startswith(prefix) for w, _, _ in summary_result.get("rejected_wallets", []))
        found_kept = any(w.startswith(prefix) for w in kept_wallets)
        status = "REMOVED" if found_rejected else "KEPT" if found_kept else "NOT_IN_SUMMARY"
        print(f"  {prefix}... → {status}")


if __name__ == "__main__":
    main()
