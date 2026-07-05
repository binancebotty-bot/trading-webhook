#!/usr/bin/env python3
"""
Portfolio simulation v2: uses closedPnl from raw trade data with proper scaling.

Instead of trying to replicate PE's FIFO position tracking (which needs 
start_position/end_position data not in all_trades.csv), we use the exchange's
closedPnl directly and apply the copy scale factor.

For proportional mode:
  copy_notional = leader_notional * (norm_base / 10000)
  trade captured if copy_notional >= $12
  copy_pnl = closedPnl * (norm_base / 10000)  [for captured trades]
  
For fixed mode:
  ratio = $12 / leader_notional
  copy_pnl = closedPnl * ratio  [for every trade]

The equity curve is cumsum(copy_pnl), and we build time-aligned portfolio
curves to compute concurrent drawdown correctly.
"""

import csv
import math
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

# === CONFIG ===
USER_WALLETS = [
    "0xd405f0",
    "0x9db82c502472d76742fdd69609dfcc6e01327401",
    "0x82d7ebbd8106b08e91f8ac9f4ca97fbd98125c29",
    "0xf83858e57d9f804f5ca1603bce82558119aeac7b",
    "0x811e8f6d80f38a2f0f8b606cb743a950638f0ad4",
]
USER_WALLET_SHORT = ["0xd405f0", "0x9db82c", "0x82d7eb", "0xf83858", "0x811e8f"]

TOTAL_SEED = 3000.0
NUM_WALLETS = len(USER_WALLETS)
SEED_PER_WALLET = TOTAL_SEED / NUM_WALLETS  # $600 each

LEADER_EQUITY_BASE = 10000.0
MIN_COPY_NOTIONAL = 12.0
COPY_FEE_BPS = 5.0

NORM_BASE_GRID = [50, 100, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000]

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TRADES_FILE = os.path.join(DATA_DIR, "all_trades.csv")


def load_wallet_trades(wallet: str) -> List[dict]:
    trades = []
    w_lower = wallet.lower()
    with open(TRADES_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rw = row["wallet"].lower()
            if rw == w_lower or rw.startswith(w_lower):
                trades.append(row)
    trades.sort(key=lambda x: int(x["time"]))
    return trades


def simulate_proportional(trades: List[dict], norm_base: float, alloc: float) -> Dict[str, Any]:
    """
    Proportional copy: scale = norm_base / LEADER_EQUITY_BASE
    copy_notional = leader_notional * scale
    Trade captured if copy_notional >= $12 (after applying copy fee)
    """
    scale = norm_base / LEADER_EQUITY_BASE
    equity_curve = [(0, alloc)]  # (time_ms, equity)
    peak = alloc
    max_dd = 0.0
    total_pnl = 0.0
    wins = 0
    losses = 0
    captured = 0
    dropped = 0
    captured_pnl = 0.0
    leader_pnl = 0.0

    for t in trades:
        px = float(t["px"])
        sz = float(t["sz"])
        closed_pnl = float(t["closedPnl"])
        time_ms = int(t["time"])

        if px <= 0 or sz <= 0:
            continue

        leader_notional = abs(px * sz)
        copy_notional = leader_notional * scale

        # Apply fee to copy notional: effective trade cost
        copy_fee = copy_notional * COPY_FEE_BPS / 10000.0

        # Check minimum notional
        if copy_notional < MIN_COPY_NOTIONAL:
            dropped += 1
            continue

        captured += 1

        # Copy PnL = leader's PnL scaled to our position size
        # Fee: entry + exit = 2 * copy_fee
        copy_pnl = closed_pnl * scale - 2 * copy_fee
        total_pnl += copy_pnl
        captured_pnl += copy_pnl
        leader_pnl += closed_pnl

        if copy_pnl >= 0:
            wins += 1
        else:
            losses += 1

        equity = alloc + total_pnl
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
        equity_curve.append((time_ms, equity))

    total = wins + losses
    return {
        "mode": "proportional",
        "norm_base": norm_base,
        "scale": scale,
        "alloc": alloc,
        "total_pnl": total_pnl,
        "max_dd": max_dd,
        "calmar": (total_pnl / max_dd) if max_dd > 0 else 0,
        "win_rate": (wins / total * 100) if total > 0 else 0,
        "wins": wins,
        "losses": losses,
        "captured": captured,
        "dropped": dropped,
        "capture_pct": (captured / (captured + dropped) * 100) if (captured + dropped) > 0 else 0,
        "equity_curve": equity_curve,
        "leader_pnl": leader_pnl,
    }


def simulate_fixed(trades: List[dict], fixed_notional: float, alloc: float) -> Dict[str, Any]:
    """
    Fixed copy: every trade copied at fixed_notional.
    ratio = fixed_notional / leader_notional
    copy_pnl = closedPnl * ratio
    """
    equity_curve = [(0, alloc)]
    peak = alloc
    max_dd = 0.0
    total_pnl = 0.0
    wins = 0
    losses = 0
    leader_pnl = 0.0
    copy_fee_total = 0.0

    for t in trades:
        px = float(t["px"])
        sz = float(t["sz"])
        closed_pnl = float(t["closedPnl"])
        time_ms = int(t["time"])

        if px <= 0 or sz <= 0:
            continue

        leader_notional = abs(px * sz)
        if leader_notional < 0.01:
            continue

        ratio = fixed_notional / leader_notional
        copy_fee = fixed_notional * COPY_FEE_BPS / 10000.0
        copy_pnl = closed_pnl * ratio - 2 * copy_fee
        total_pnl += copy_pnl
        copy_fee_total += 2 * copy_fee
        leader_pnl += closed_pnl

        if copy_pnl >= 0:
            wins += 1
        else:
            losses += 1

        equity = alloc + total_pnl
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
        equity_curve.append((time_ms, equity))

    total = wins + losses
    return {
        "mode": "fixed",
        "norm_base": 0,
        "scale": fixed_notional,
        "alloc": alloc,
        "total_pnl": total_pnl,
        "max_dd": max_dd,
        "calmar": (total_pnl / max_dd) if max_dd > 0 else 0,
        "win_rate": (wins / total * 100) if total > 0 else 0,
        "wins": wins,
        "losses": losses,
        "captured": total,
        "dropped": 0,
        "capture_pct": 100.0,
        "equity_curve": equity_curve,
        "leader_pnl": leader_pnl,
    }


def build_portfolio_curve(wallet_curves: Dict[str, List[Tuple[int, float]]]) -> List[Tuple[int, float]]:
    """Merge per-wallet equity curves into portfolio equity using step interpolation."""
    all_timestamps = set()
    for curve in wallet_curves.values():
        for ts, eq in curve:
            all_timestamps.add(ts)

    sorted_curves = {w: sorted(c, key=lambda x: x[0]) for w, c in wallet_curves.items()}
    sorted_ts = sorted(all_timestamps)

    portfolio = []
    current_eq = {}
    idx = {w: 0 for w in wallet_curves}

    for ts in sorted_ts:
        for w in wallet_curves:
            c = sorted_curves[w]
            i = idx[w]
            while i < len(c) and c[i][0] <= ts:
                current_eq[w] = c[i][1]
                i += 1
            idx[w] = i
        portfolio.append((ts, sum(current_eq.values())))

    return portfolio


def calc_drawdown(curve: List[Tuple[int, float]]) -> Tuple[float, float]:
    """Returns (max_drawdown, current_drawdown)."""
    if not curve:
        return 0.0, 0.0
    peak = curve[0][1]
    max_dd = 0.0
    for _, eq in curve:
        peak = max(peak, eq)
        dd = peak - eq
        max_dd = max(max_dd, dd)
    current_dd = peak - curve[-1][1]
    return max_dd, current_dd


def main():
    print(f"=== PORTFOLIO SIMULATION v2: {NUM_WALLETS} wallets, ${TOTAL_SEED:.0f} total (${SEED_PER_WALLET:.0f}/wallet) ===")
    print(f"Fee: {COPY_FEE_BPS} bps copy friction | Min notional: ${MIN_COPY_NOTIONAL} | Base: ${LEADER_EQUITY_BASE:.0f}")
    print()

    # Load trades
    all_trades = {}
    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        trades = load_wallet_trades(wallet)
        all_trades[wallet] = trades
        print(f"  {short}: {len(trades)} trades")
        if trades:
            # Quick stats
            total_lp = sum(float(t["closedPnl"]) for t in trades)
            total_notional = sum(abs(float(t["px"]) * float(t["sz"])) for t in trades)
            print(f"         Leader PnL: ${total_lp:,.2f} | Total notional: ${total_notional:,.0f}")
    print()

    # ============================================================
    # PART 1: Per-wallet sweep
    # ============================================================
    print("=" * 130)
    print("PART 1: PER-WALLET RESULTS")
    print("=" * 130)

    best_per_wallet = {}  # wallet -> best result dict

    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        trades = all_trades[wallet]
        if not trades:
            print(f"\n--- {short}: NO TRADES ---")
            continue

        print(f"\n--- {short} ({len(trades)} trades) ---")

        # Fixed $12
        r_fixed = simulate_fixed(trades, 12.0, SEED_PER_WALLET)
        print(f"  {'fixed $12':20s} | PnL: ${r_fixed['total_pnl']:>9.2f} | DD: -${r_fixed['max_dd']:>7.2f} | "
              f"Calmar: {r_fixed['calmar']:>7.2f} | Win: {r_fixed['win_rate']:>5.1f}% ({r_fixed['wins']}/{r_fixed['wins']+r_fixed['losses']}) | "
              f"Cap: {r_fixed['capture_pct']:>5.1f}%")

        all_results = [("fixed $12", r_fixed)]

        for nb in NORM_BASE_GRID:
            r = simulate_proportional(trades, nb, SEED_PER_WALLET)
            if r["captured"] >= 10:
                all_results.append((f"nb{nb}", r))
                print(f"  {'nb'+str(nb):20s} | PnL: ${r['total_pnl']:>9.2f} | DD: -${r['max_dd']:>7.2f} | "
                      f"Calmar: {r['calmar']:>7.2f} | Win: {r['win_rate']:>5.1f}% ({r['wins']}/{r['wins']+r['losses']}) | "
                      f"Cap: {r['capture_pct']:>5.1f}% ({r['captured']}/{r['captured']+r['dropped']})")

        # Best by Calmar (require min 20 wins)
        best = None
        best_calmar = -999
        for label, r in all_results:
            if r["wins"] + r["losses"] >= 20 and r["total_pnl"] > 0:
                if r["calmar"] > best_calmar:
                    best_calmar = r["calmar"]
                    best = r

        if not best:
            # Fallback: best by PnL
            best = max(all_results, key=lambda x: x[1]["total_pnl"])[1]
        
        best_per_wallet[wallet] = best
        if best["mode"] == "proportional":
            print(f"  >>> BEST: nb{int(best['norm_base'])} Calmar={best['calmar']:.2f}")
        else:
            print(f"  >>> BEST: fixed $12 Calmar={best['calmar']:.2f}")

    # ============================================================
    # PART 2: Portfolio concurrent drawdown
    # ============================================================
    print("\n" + "=" * 130)
    print("PART 2: PORTFOLIO CONCURRENT DRAWDOWN (best config per wallet)")
    print("=" * 130)

    # Build portfolio curves
    wallet_curves = {}
    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        if wallet not in best_per_wallet:
            continue
        r = best_per_wallet[wallet]
        wallet_curves[wallet] = r["equity_curve"]
        if r["mode"] == "proportional":
            print(f"  {short}: prop nb{int(r['norm_base'])} | PnL: ${r['total_pnl']:>8.2f} | DD: -${r['max_dd']:>7.2f}")
        else:
            print(f"  {short}: fixed $12 | PnL: ${r['total_pnl']:>8.2f} | DD: -${r['max_dd']:>7.2f}")

    if len(wallet_curves) < 2:
        print("Need at least 2 wallets")
        return

    port_curve = build_portfolio_curve(wallet_curves)
    port_dd, port_cdd = calc_drawdown(port_curve)
    port_pnl = sum(best_per_wallet[w]["total_pnl"] for w in wallet_curves)
    port_alloc = sum(best_per_wallet[w]["alloc"] for w in wallet_curves)
    port_calmar = (port_pnl / port_dd) if port_dd > 0 else 0
    sum_indiv_dd = sum(best_per_wallet[w]["max_dd"] for w in wallet_curves)
    corr = port_dd / sum_indiv_dd if sum_indiv_dd > 0 else 0

    # Individual DD overlap analysis
    print(f"\n  PORTFOLIO (best mixed configs):")
    print(f"    Seed:                ${port_alloc:,.2f}")
    print(f"    Total PnL:           ${port_pnl:,.2f}")
    print(f"    Portfolio MaxDD:     ${port_dd:,.2f}")
    print(f"    Portfolio Calmar:    {port_calmar:,.2f}")
    print(f"    Current DD:          ${port_cdd:,.2f}")
    print(f"    Sum of indiv DDs:    ${sum_indiv_dd:,.2f}")
    print(f"    DD diversification:  {corr:.2f}x (1.0=same time, <1=beneficial stagger)")
    print(f"    Bankroll (DD+25%):   ${port_dd * 1.25:,.2f}")
    print(f"    Bankroll (3x DD):    ${port_dd * 3:,.2f}")

    # ============================================================
    # PART 3: Portfolio sweep — uniform norm_base
    # ============================================================
    print("\n" + "=" * 130)
    print("PART 3: PORTFOLIO SWEEP — uniform norm_base for all wallets")
    print("=" * 130)

    sweep_results = []

    for nb in NORM_BASE_GRID:
        wallet_curves_nb = {}
        total_pnl = 0
        total_wins = 0
        total_losses = 0
        total_captured = 0
        total_trades = 0

        for wallet in USER_WALLETS:
            trades = all_trades[wallet]
            if not trades:
                continue
            r = simulate_proportional(trades, nb, SEED_PER_WALLET)
            wallet_curves_nb[wallet] = r["equity_curve"]
            total_pnl += r["total_pnl"]
            total_wins += r["wins"]
            total_losses += r["losses"]
            total_captured += r["captured"]
            total_trades += r["captured"] + r["dropped"]

        if len(wallet_curves_nb) < 2:
            continue

        port_c = build_portfolio_curve(wallet_curves_nb)
        p_dd, _ = calc_drawdown(port_c)
        p_calmar = (total_pnl / p_dd) if p_dd > 0 else 0
        total_all = total_wins + total_losses
        p_wr = (total_wins / total_all * 100) if total_all > 0 else 0
        p_cap = (total_captured / total_trades * 100) if total_trades > 0 else 0
        ind_dd = sum(simulate_proportional(all_trades[w], nb, SEED_PER_WALLET)["max_dd"] for w in USER_WALLETS if all_trades[w])
        corr_val = p_dd / ind_dd if ind_dd > 0 else 0

        sweep_results.append({
            "nb": nb, "pnl": total_pnl, "dd": p_dd, "calmar": p_calmar,
            "wr": p_wr, "cap": p_cap, "corr": corr_val,
        })

        print(f"  nb={nb:>5d} | PnL: ${total_pnl:>9.2f} | MaxDD: ${p_dd:>8.2f} | "
              f"Calmar: {p_calmar:>7.2f} | Win: {p_wr:>5.1f}% | Cap: {p_cap:>5.1f}% | Corr: {corr_val:.2f}")

    # Fixed at portfolio level
    wallet_curves_f = {}
    total_pnl_f = 0
    total_wins_f = 0
    total_losses_f = 0
    p_dd_f = 0.0
    p_calmar_f = 0.0
    p_wr_f = 0.0
    corr_f = 0.0
    for wallet in USER_WALLETS:
        trades = all_trades[wallet]
        if not trades:
            continue
        r = simulate_fixed(trades, 12.0, SEED_PER_WALLET)
        wallet_curves_f[wallet] = r["equity_curve"]
        total_pnl_f += r["total_pnl"]
        total_wins_f += r["wins"]
        total_losses_f += r["losses"]

    if len(wallet_curves_f) >= 2:
        port_cf = build_portfolio_curve(wallet_curves_f)
        p_dd_f, _ = calc_drawdown(port_cf)
        p_calmar_f = (total_pnl_f / p_dd_f) if p_dd_f > 0 else 0
        total_f = total_wins_f + total_losses_f
        p_wr_f = (total_wins_f / total_f * 100) if total_f > 0 else 0
        ind_dd_f = sum(simulate_fixed(all_trades[w], 12.0, SEED_PER_WALLET)["max_dd"] for w in USER_WALLETS if all_trades[w])
        corr_f = p_dd_f / ind_dd_f if ind_dd_f > 0 else 0

        print(f"\n  FIXED  | PnL: ${total_pnl_f:>9.2f} | MaxDD: ${p_dd_f:>8.2f} | "
              f"Calmar: {p_calmar_f:>7.2f} | Win: {p_wr_f:>5.1f}% | Corr: {corr_f:.2f}")

    # ============================================================
    # PART 4: Summary comparison
    # ============================================================
    print("\n" + "=" * 130)
    print("PART 4: SUMMARY — BEST OPTION FROM EACH CATEGORY")
    print("=" * 130)

    all_portfolio = sweep_results.copy()
    if len(wallet_curves_f) >= 2:
        all_portfolio.append({
            "nb": 0, "pnl": total_pnl_f, "dd": p_dd_f,
            "calmar": p_calmar_f, "wr": p_wr_f, "cap": 100.0, "corr": corr_f,
            "label": "FIXED $12",
        })

    # Add mixed
    all_portfolio.append({
        "nb": -1, "pnl": port_pnl, "dd": port_dd,
        "calmar": port_calmar, "wr": 0, "cap": 0, "corr": corr,
        "label": "MIXED BEST",
    })

    # Sort by Calmar
    all_portfolio.sort(key=lambda x: x["calmar"], reverse=True)

    print(f"\n  {'Config':20s} | {'PnL':>10s} | {'MaxDD':>10s} | {'Calmar':>8s} | {'WinRate':>7s} | {'Capture':>8s} | {'Corr':>5s}")
    print(f"  {'-'*20} | {'-'*10} | {'-'*10} | {'-'*8} | {'-'*7} | {'-'*8} | {'-'*5}")
    for r in all_portfolio:
        label = r.get("label", f"nb{r['nb']}")
        print(f"  {label:20s} | ${r['pnl']:>9.2f} | ${r['dd']:>9.2f} | {r['calmar']:>8.2f} | {r['wr']:>6.1f}% | {r['cap']:>7.1f}% | {r['corr']:.2f}")


if __name__ == "__main__":
    main()
