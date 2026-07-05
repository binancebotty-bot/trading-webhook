"""
Re-rank wallets using REAL accountValueHistory maxDD from portfolio JSONs.
Sidecar proof run — does not modify any existing files or live systems.

Compares old composite (sim DD) vs new composite (real DD from MTM equity curve).
"""
import csv
import json
import os
import glob
import numpy as np
from pathlib import Path

BASE = Path(r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER")
DATA = BASE / "data"
PORTF_DIR = BASE / "copy_selection_run" / "wallet_portfolios"

# ─── Load simulation results (bruteforce has all wallets) ───
def load_bruteforce():
    """Load wallet_bruteforce.csv — all 197 copyable wallets with sim results."""
    path = DATA / "wallet_bruteforce.csv"
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append({
                    "wallet": r["wallet"],
                    "n_trades": int(r["n_trades"]),
                    "mtm_calmar": float(r["mtm_calmar"]) if r["mtm_calmar"] else 0,
                    "month_acctV_end": float(r["month_acctV_end"]) if r["month_acctV_end"] else 0,
                    "equity_collapse": int(r["equity_collapse"]) if r["equity_collapse"] else 0,
                    "negative_total": int(r["negative_total"]) if r["negative_total"] else 0,
                    "best_mode": r["best_mode"],
                    "best_nb": int(r["best_nb"]) if r["best_nb"] else 0,
                    "best_calmar": float(r["best_calmar"]) if r["best_calmar"] else 0,
                    "best_pnl": float(r["best_pnl"]) if r["best_pnl"] else 0,
                    "best_dd": float(r["best_dd"]) if r["best_dd"] else 0,
                    "best_win_rate": float(r["best_win_rate"]) if r["best_win_rate"] else 0,
                    "best_capture": float(r["best_capture"]) if r["best_capture"] else 0,
                    "best_composite": float(r["best_composite"]) if r["best_composite"] else 0,
                    "fixed_calmar": float(r["fixed_calmar"]) if r["fixed_calmar"] else 0,
                    "fixed_pnl": float(r["fixed_pnl"]) if r["fixed_pnl"] else 0,
                    "fixed_dd": float(r["fixed_dd"]) if r["fixed_dd"] else 0,
                    "passes_mtm_filter": r["passes_mtm_filter"] == "True",
                })
            except (ValueError, KeyError):
                continue
    return rows


# ─── Load trade-based MTM from summary_mtm.csv ───
def load_summary_mtm():
    """Load summary_mtm.csv for allTime MTM DD and Calmar."""
    path = DATA / "summary_mtm.csv"
    mtm = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            wallet = r["wallet"]
            try:
                allTime_dd = abs(float(r["allTime_acctV_mdd_mtm"])) if r.get("allTime_acctV_mdd_mtm") else 0
                allTime_end = float(r["allTime_acctV_end"]) if r.get("allTime_acctV_end") else 0
                allTime_chg = float(r["allTime_acctV_chg"]) if r.get("allTime_acctV_chg") else 0
                allTime_start = float(r["allTime_acctV_start"]) if r.get("allTime_acctV_start") else 0
                month_calmar = float(r["month_calmar_mtm"]) if r.get("month_calmar_mtm") else 0
                # Peak = end + abs(chg) if chg negative, else end
                # Actually: peak = max value in the series; simplest: peak ≈ end - chg if chg < 0, else end
                # But we have dd_mtm which is peak-to-trough, so peak >= dd
                # We can estimate: peak ≈ end + allTime_dd if it went down, but this is rough
                # Better: peak ≈ allTime_start - allTime_chg if allTime_chg < 0 (it lost value)
                # No, let's use: peak >= allTime_dd, and return_pct = chg/start * 100
                mtm[wallet] = {
                    "allTime_dd_mtm": allTime_dd,
                    "allTime_end": allTime_end,
                    "allTime_chg": allTime_chg,
                    "allTime_start": allTime_start,
                    "month_calmar_mtm": month_calmar,
                }
            except (ValueError, KeyError):
                continue
    return mtm


# ─── Compute real DD from portfolio JSON accountValueHistory ───
def compute_real_dd(json_path):
    """Read portfolio JSON, compute real max DD from accountValueHistory."""
    try:
        with open(json_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    # Find the entry with the most data points (usually "allTime" or last entry)
    best_entry = None
    best_count = 0
    for entry in data:
        if isinstance(entry, list) and len(entry) == 2:
            label, metrics = entry
            avh = metrics.get("accountValueHistory", [])
            if len(avh) > best_count:
                best_count = len(avh)
                best_entry = entry

    if best_entry is None or best_count < 2:
        return None

    label, metrics = best_entry
    avh = metrics.get("accountValueHistory", [])
    acct_values = np.array([float(v) for _, v in avh], dtype=np.float64)

    if len(acct_values) < 2:
        return None

    peak = float(np.max(acct_values))
    running_max = np.maximum.accumulate(acct_values)
    drawdowns = acct_values - running_max
    max_dd = float(np.min(drawdowns))
    trough = float(acct_values[np.argmin(drawdowns)])
    final = float(acct_values[-1])
    initial = float(acct_values[0])

    # Time span
    timestamps = [int(t) for t, _ in avh]
    days = (timestamps[-1] - timestamps[0]) / 86400000

    return {
        "real_dd_usd": round(abs(max_dd), 2),
        "real_dd_pct": round(abs(max_dd) / peak * 100, 2) if peak > 0 else 0,
        "peak_acctV": round(peak, 2),
        "trough_acctV": round(trough, 2),
        "final_acctV": round(final, 2),
        "initial_acctV": round(initial, 2),
        "real_return_pct": round((final - initial) / initial * 100, 2) if initial > 0 else 0,
        "real_return_usd": round(final - initial, 2),
        "data_points": best_count,
        "period_label": label,
        "days": round(days, 1),
    }


# ─── Composite scoring ───
def compute_composite(calmar, sortino, dd, pnl, win_rate, real_dd_usd=None):
    """
    Balanced composite score.
    Uses real DD if provided, otherwise sim DD.
    """
    dd_for_score = real_dd_usd if real_dd_usd is not None else abs(dd)

    # Capped Calmar (max 25)
    calmar_c = min(calmar, 25.0)
    # Capped Sortino (max 15) — estimate from Calmar * 0.6 if not available
    sortino_c = min(calmar * 0.6, 15.0)
    # Log PnL (min $100 to avoid log(0))
    pnl_log = np.log10(max(pnl, 100)) if pnl > 0 else 0
    # Inverse DD (smaller DD = better)
    dd_inv = 1.0 / (1.0 + np.log10(1.0 + dd_for_score)) if dd_for_score > 0 else 1.0
    # Win rate
    wr = win_rate

    composite = (
        calmar_c * 0.25
        + sortino_c * 0.15
        + pnl_log * 0.35
        + dd_inv * 0.10
        + wr * 0.15
    )
    return round(composite, 4)


# ─── Main ───
def main():
    print("=" * 120)
    print("RE-RANKING: SIM DD vs REAL accountValueHistory DD")
    print("=" * 120)

    # Load data
    bruteforce = load_bruteforce()
    summary_mtm = load_summary_mtm()
    print(f"\nLoaded {len(bruteforce)} wallets from bruteforce")
    print(f"Loaded {len(summary_mtm)} wallets from summary_mtm")

    # List portfolio JSONs
    portfolio_files = {}
    for fp in glob.glob(str(PORTF_DIR / "*.json")):
        wallet = Path(fp).stem
        portfolio_files[wallet] = fp
    print(f"Found {len(portfolio_files)} portfolio JSONs")

    # ─── Process each wallet ───
    results = []
    n_real_dd = 0
    n_no_json = 0
    n_json_error = 0

    for w in bruteforce:
        wallet = w["wallet"]

        # Get real DD from portfolio JSON
        real = None
        if wallet in portfolio_files:
            real = compute_real_dd(portfolio_files[wallet])
            if real:
                n_real_dd += 1
            else:
                n_json_error += 1
        else:
            n_no_json += 1

        # Get MTM data from summary_mtm
        mtm = summary_mtm.get(wallet, {})

        # Old composite (sim DD)
        old_composite = w["best_composite"]

        # New composite (real DD)
        if real and real["real_dd_usd"] > 0:
            new_composite = compute_composite(
                w["best_calmar"],
                0,  # sortino not in bruteforce, estimate from calmar
                w["best_dd"],
                w["best_pnl"],
                w["best_win_rate"],
                real_dd_usd=real["real_dd_usd"],
            )
        else:
            # No real DD available — keep old composite
            new_composite = old_composite

        results.append({
            **w,
            "old_composite": old_composite,
            "new_composite": new_composite,
            "real_dd_usd": real["real_dd_usd"] if real else None,
            "real_dd_pct": real["real_dd_pct"] if real else None,
            "peak_acctV": real["peak_acctV"] if real else None,
            "real_return_pct": real["real_return_pct"] if real else None,
            "real_return_usd": real["real_return_usd"] if real else None,
            "real_days": real["days"] if real else None,
            "real_data_points": real["data_points"] if real else None,
            "allTime_dd_mtm": mtm.get("allTime_dd_mtm"),
            "allTime_end": mtm.get("allTime_end"),
            "month_calmar_mtm": mtm.get("month_calmar_mtm"),
        })

    print(f"\nReal DD computed: {n_real_dd} wallets")
    print(f"No portfolio JSON: {n_no_json} wallets")
    print(f"JSON parse error: {n_json_error} wallets")

    # ─── Sort by new composite ───
    results.sort(key=lambda x: x["new_composite"], reverse=True)

    # Assign new rank
    for i, r in enumerate(results):
        r["new_rank"] = i + 1

    # ─── Old rank (from original best_composite sort) ───
    old_sorted = sorted(results, key=lambda x: x["old_composite"], reverse=True)
    for i, r in enumerate(old_sorted):
        r["old_rank"] = i + 1

    # Restore new_rank after old_rank assignment
    results.sort(key=lambda x: x["new_composite"], reverse=True)
    for i, r in enumerate(results):
        r["new_rank"] = i + 1

    # ─── Output: Top 30 comparison ───
    print("\n" + "=" * 180)
    print(f"{'#':>3} {'Wallet':>12} {'OldRank':>7} {'NewRank':>7} {'RankChg':>7} | "
          f"{'SimDD':>10} {'RealDD':>10} {'DD Ratio':>8} | "
          f"{'OldComp':>8} {'NewComp':>8} {'CompChg':>8} | "
          f"{'PnL':>10} {'WinRate':>7} {'Capture':>7} {'Mode':>8} {'Trades':>6} | "
          f"{'RealRet%':>8} {'PeakAcctV':>10}")
    print("-" * 180)

    for r in results[:30]:
        old_rank = r.get("old_rank", "?")
        new_rank = r["new_rank"]
        rank_chg = old_rank - new_rank if isinstance(old_rank, int) else 0
        rank_str = f"+{rank_chg}" if rank_chg > 0 else str(rank_chg)

        sim_dd = abs(r["best_dd"])
        real_dd = r["real_dd_usd"] if r["real_dd_usd"] else 0
        dd_ratio = f"{real_dd/sim_dd:.1f}x" if sim_dd > 0 and real_dd > 0 else "N/A"

        comp_chg = r["new_composite"] - r["old_composite"]
        comp_str = f"+{comp_chg:.2f}" if comp_chg > 0 else f"{comp_chg:.2f}"

        real_ret = f"{r['real_return_pct']:.1f}" if r["real_return_pct"] is not None else "N/A"
        peak = f"${r['peak_acctV']:,.0f}" if r["peak_acctV"] else "N/A"

        print(f"{new_rank:>3} {r['wallet'][:12]}... {old_rank:>7} {new_rank:>7} {rank_str:>7} | "
              f"${sim_dd:>9,.0f} ${real_dd:>9,.0f} {dd_ratio:>8} | "
              f"{r['old_composite']:>8.2f} {r['new_composite']:>8.2f} {comp_str:>8} | "
              f"${r['best_pnl']:>9,.0f} {r['best_win_rate']:>6.1%} {r['best_capture']:>6.1f}% {r['best_mode']:>8} {r['n_trades']:>6} | "
              f"{real_ret:>7}% {peak:>10}")

    # ─── Output: Wallets where rank changed significantly ───
    print("\n" + "=" * 140)
    print("BIGGEST RANK CHANGES (real DD reshuffled the ranking)")
    print("=" * 140)

    big_changes = [r for r in results if abs(r["new_rank"] - r.get("old_rank", r["new_rank"])) >= 3]
    big_changes.sort(key=lambda x: abs(x["new_rank"] - x.get("old_rank", x["new_rank"])), reverse=True)

    print(f"{'Wallet':>12} {'OldRank':>7} {'NewRank':>7} {'RankChg':>7} | {'SimDD':>10} {'RealDD':>10} {'DD Ratio':>8} | {'OldComp':>8} {'NewComp':>8}")
    print("-" * 120)
    for r in big_changes[:20]:
        old_rank = r.get("old_rank", "?")
        rank_chg = old_rank - r["new_rank"]
        sim_dd = abs(r["best_dd"])
        real_dd = r["real_dd_usd"] if r["real_dd_usd"] else 0
        dd_ratio = f"{real_dd/sim_dd:.1f}x" if sim_dd > 0 and real_dd > 0 else "N/A"
        print(f"{r['wallet'][:12]}... {old_rank:>7} {r['new_rank']:>7} {rank_chg:>+7} | "
              f"${sim_dd:>9,.0f} ${real_dd:>9,.0f} {dd_ratio:>8} | "
              f"{r['old_composite']:>8.2f} {r['new_composite']:>8.2f}")

    # ─── Filter: candidates with real DD available and passes MTM filter ───
    print("\n" + "=" * 140)
    print("FINAL CANDIDATE LIST: Real DD available + passes MTM filter + real DD > $0")
    print("=" * 140)

    candidates = [r for r in results if r["real_dd_usd"] is not None and r["real_dd_usd"] > 0 and r["passes_mtm_filter"]]
    candidates.sort(key=lambda x: x["new_composite"], reverse=True)

    print(f"\nFound {len(candidates)} candidates (from {len(results)} total)")
    print(f"\n{'#':>3} {'Wallet':>12} {'NewRank':>7} {'SimDD':>10} {'RealDD':>10} {'DD Ratio':>8} | "
          f"{'PnL':>10} {'RealRet%':>8} {'WinRate':>7} {'Cap%':>6} {'Mode':>8} {'Nb':>5} {'Trades':>6}")
    print("-" * 140)

    for i, r in enumerate(candidates[:30]):
        sim_dd = abs(r["best_dd"])
        real_dd = r["real_dd_usd"]
        dd_ratio = f"{real_dd/sim_dd:.1f}x" if sim_dd > 0 else "N/A"
        real_ret = f"{r['real_return_pct']:.1f}" if r["real_return_pct"] is not None else "N/A"
        nb = r["best_nb"] if r["best_mode"] == "proportional" else 0

        print(f"{i+1:>3} {r['wallet'][:12]}... {r['new_rank']:>7} ${sim_dd:>9,.0f} ${real_dd:>9,.0f} {dd_ratio:>8} | "
              f"${r['best_pnl']:>9,.0f} {real_ret:>7}% {r['best_win_rate']:>6.1%} {r['best_capture']:>5.1f}% {r['best_mode']:>8} {nb:>5} {r['n_trades']:>6}")

    # ─── Blocked wallets (no portfolio JSON) ───
    blocked = [r for r in results if r["real_dd_usd"] is None]
    print(f"\n{'='*80}")
    print(f"WALLETS WITHOUT REAL DD (no portfolio JSON): {len(blocked)}")
    print(f"{'='*80}")
    for r in blocked[:10]:
        print(f"  {r['wallet'][:16]}... (sim DD: ${abs(r['best_dd']):,.0f}, trades: {r['n_trades']})")
    if len(blocked) > 10:
        print(f"  ... and {len(blocked)-10} more")

    # ─── Save full results ───
    out_path = DATA / "rerank_results.csv"
    fieldnames = [
        "wallet", "n_trades", "old_rank", "new_rank",
        "old_composite", "new_composite",
        "best_mode", "best_nb", "best_calmar", "best_pnl", "best_dd", "best_win_rate", "best_capture",
        "real_dd_usd", "real_dd_pct", "real_return_pct", "real_return_usd",
        "peak_acctV", "real_days", "real_data_points",
        "allTime_dd_mtm", "month_calmar_mtm",
        "passes_mtm_filter", "equity_collapse", "negative_total",
    ]

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"\nFull results saved to: {out_path}")

    # ─── Summary stats ───
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total wallets analyzed: {len(results)}")
    print(f"Wallets with real DD: {n_real_dd}")
    print(f"Wallets without portfolio JSON: {n_no_json}")
    print(f"Portfolio JSON errors: {n_json_error}")
    print(f"Candidates (real DD + MTM filter): {len(candidates)}")

    # DD ratio distribution
    ratios = [r["real_dd_usd"] / abs(r["best_dd"]) for r in results
              if r["real_dd_usd"] and r["best_dd"] > 0]
    if ratios:
        print(f"\nDD ratio (real/sim) distribution:")
        print(f"  Min: {min(ratios):.1f}x")
        print(f"  Median: {np.median(ratios):.1f}x")
        print(f"  Max: {max(ratios):.1f}x")
        print(f"  Mean: {np.mean(ratios):.1f}x")

    # How many wallets changed rank by >=3
    moved = [r for r in results if r["real_dd_usd"] and abs(r["new_rank"] - r.get("old_rank", r["new_rank"])) >= 3]
    print(f"\nWallets with rank change >= 3: {len(moved)}")


if __name__ == "__main__":
    main()
