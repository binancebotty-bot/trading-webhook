#!/usr/bin/env python3
"""
Portfolio simulation for 5 user wallets using PE-identical FIFO replay.

Tests proportional mode (norm_base sweep) + fixed $12.
Builds time-aligned equity curves to compute TRUE portfolio concurrent drawdown.

Key insight: Portfolio maxDD ≠ sum of individual maxDDs. Drawdowns that overlap
in time create deeper portfolio drawdowns. Staggered drawdowns are less severe.
"""

import csv
import math
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# === CONFIG ===
USER_WALLETS = [
    "0xd405f0",   # no trades in all_trades.csv
    "0x9db82c502472d76742fdd69609dfcc6e01327401",
    "0x82d7ebbd8106b08e91f8ac9f4ca97fbd98125c29",
    "0xf83858e57d9f804f5ca1603bce82558119aeac7b",
    "0x811e8f6d80f38a2f0f8b606cb743a950638f0ad4",
]
USER_WALLET_SHORT = ["0xd405f0", "0x9db82c", "0x82d7eb", "0xf83858", "0x811e8f"]

TOTAL_SEED = 3000.0  # $3000 total
NUM_WALLETS = len(USER_WALLETS)
SEED_PER_WALLET = TOTAL_SEED / NUM_WALLETS  # $600 each

LEADER_EQUITY_BASE = 10000.0
FEE_BPS = 0.0          # leader fee
COPY_FEE_BPS = 5.0     # copy friction
MIN_COPY_NOTIONAL = 12.0

# norm_base grid to test for proportional mode
# Lower = smaller trades, higher capture of small trades
# Higher = bigger trades, more risk per trade but more capture
NORM_BASE_GRID = [50, 100, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000]

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TRADES_FILE = os.path.join(DATA_DIR, "all_trades.csv")


@dataclass
class Position:
    trade_id: str
    coin: str
    side: str
    entry_time_ms: int
    entry_time_iso: str
    entry_price_lead: float
    entry_price_copy: float
    leader_size_units: float
    copy_size_units: float
    leader_notional: float
    copy_notional: float
    entry_fee_copy: float


@dataclass
class WalletSim:
    wallet: str
    alloc: float
    copy_mode: str
    norm_base: float = 0.0
    # Running state
    realized: float = 0.0
    unrealized: float = 0.0
    equity: float = 0.0
    peak: float = 0.0
    max_drawdown: float = 0.0
    win_count: int = 0
    loss_count: int = 0
    entry_count: int = 0
    exit_count: int = 0
    total_notional: float = 0.0
    # Positions
    positions: List[Position] = field(default_factory=list)
    # Equity curve: list of (timestamp_ms, equity)
    curve: List[Tuple[int, float]] = field(default_factory=list)
    # Trades log
    trades: List[Dict[str, Any]] = field(default_factory=list)
    # Flags
    copy_notional_below_min: int = 0
    total_leader_fills: int = 0

    def sync_equity(self):
        self.equity = self.alloc + self.realized + self.unrealized
        self.peak = max(self.peak, self.equity)
        self.max_drawdown = max(self.max_drawdown, self.peak - self.equity)


def sign_from_side(side: str) -> float:
    return 1.0 if side.upper() == "BUY" else -1.0


def bps_fee(notional: float, fee_bps: float) -> float:
    return abs(notional) * fee_bps / 10000.0


def calc_unrealized(side: str, entry_px: float, mark_px: float, size_units: float) -> float:
    if entry_px <= 0 or mark_px <= 0 or size_units <= 0:
        return 0.0
    direction = 1.0 if side == "BUY" else -1.0
    return (mark_px - entry_px) * size_units * direction


def close_positions_fifo(positions: List[Position], raw_size_to_close: float) -> List[Tuple[Position, float]]:
    remaining = abs(raw_size_to_close)
    closed = []
    kept = []
    for p in positions:
        if remaining <= 1e-12:
            kept.append(p)
            continue
        take_leader = min(p.leader_size_units, remaining)
        frac = take_leader / p.leader_size_units if p.leader_size_units > 0 else 0.0
        if frac > 0:
            closed.append((p, frac))
        remaining -= take_leader
        leftover = p.leader_size_units - take_leader
        if leftover > 1e-12:
            ratio = leftover / p.leader_size_units
            kept.append(Position(
                trade_id=p.trade_id, coin=p.coin, side=p.side,
                entry_time_ms=p.entry_time_ms, entry_time_iso=p.entry_time_iso,
                entry_price_lead=p.entry_price_lead, entry_price_copy=p.entry_price_copy,
                leader_size_units=leftover,
                copy_size_units=p.copy_size_units * ratio,
                leader_notional=p.leader_notional * ratio,
                copy_notional=p.copy_notional * ratio,
                entry_fee_copy=p.entry_fee_copy * ratio,
            ))
    positions[:] = kept
    return closed


def model_copy_notional(leader_notional: float, copy_mode: str, norm_base: float,
                        fixed_notional: float) -> float:
    if copy_mode == "fixed":
        return fixed_notional
    return leader_notional * (norm_base / LEADER_EQUITY_BASE)


def simulate_wallet(wallet: str, trades: List[dict], alloc: float,
                    copy_mode: str, norm_base: float = 0.0,
                    fixed_notional: float = 12.0) -> WalletSim:
    """
    PE-identical FIFO replay for a single wallet.
    """
    sim = WalletSim(
        wallet=wallet, alloc=alloc, copy_mode=copy_mode,
        norm_base=norm_base if copy_mode == "proportional" else 0.0
    )
    sim.equity = alloc
    sim.peak = alloc

    coin_positions: Dict[str, List[Position]] = defaultdict(list)
    mark_prices: Dict[str, float] = {}

    for i, t in enumerate(trades):
        coin = t["coin"]
        side = t["side"]
        px = float(t["px"])
        sz = float(t["sz"])
        time_ms = int(t["time"])

        if px <= 0 or sz <= 0:
            continue

        mark_prices[coin] = px
        leader_notional = abs(px * sz)
        sim.total_leader_fills += 1

        # Determine entry vs exit
        positions = coin_positions[coin]
        delta = sz * sign_from_side(side)

        if not positions:
            is_entry = True
        else:
            first_side = sign_from_side(positions[0].side)
            is_entry = (delta > 0 and first_side > 0) or (delta < 0 and first_side < 0)

        if is_entry:
            # Calculate copy notional
            copy_n = model_copy_notional(leader_notional, copy_mode, norm_base, fixed_notional)

            # For proportional: drop trades below $12 minimum
            if copy_mode == "proportional" and copy_n < MIN_COPY_NOTIONAL:
                sim.copy_notional_below_min += 1
                continue

            copy_fee = bps_fee(copy_n, COPY_FEE_BPS)
            copy_size = copy_n / px if px > 0 else 0.0

            pos = Position(
                trade_id=f"T{i:08d}", coin=coin, side=side,
                entry_time_ms=time_ms, entry_time_iso=t.get("time_iso", str(time_ms)),
                entry_price_lead=px, entry_price_copy=px,
                leader_size_units=sz, copy_size_units=copy_size,
                leader_notional=leader_notional, copy_notional=copy_n,
                entry_fee_copy=copy_fee,
            )
            positions.append(pos)
            sim.entry_count += 1
            sim.total_notional += copy_n
            sim.realized -= copy_fee  # entry fee deducted from realized

        else:
            # Exit
            closed = close_positions_fifo(positions, abs(delta))
            for p, frac in closed:
                exit_fee = bps_fee(p.copy_notional * frac, COPY_FEE_BPS)
                gross_pnl = calc_unrealized(p.side, p.entry_price_copy, px, p.copy_size_units * frac)
                net_pnl = gross_pnl - (p.entry_fee_copy * frac) - exit_fee
                sim.realized += net_pnl - exit_fee
                sim.exit_count += 1
                if net_pnl >= 0:
                    sim.win_count += 1
                else:
                    sim.loss_count += 1

        # Update unrealized from all open positions
        sim.unrealized = 0.0
        for coin_key, plist in coin_positions.items():
            mp = mark_prices.get(coin_key, 0.0)
            for p in plist:
                sim.unrealized += calc_unrealized(p.side, p.entry_price_copy, mp, p.copy_size_units)

        sim.sync_equity()
        sim.curve.append((time_ms, sim.equity))

    # Close any remaining positions at last mark price
    for coin_key, plist in coin_positions.items():
        mp = mark_prices.get(coin_key, 0.0)
        for p in plist[:]:
            exit_fee = bps_fee(p.copy_notional, COPY_FEE_BPS)
            gross_pnl = calc_unrealized(p.side, p.entry_price_copy, mp, p.copy_size_units)
            net_pnl = gross_pnl - (p.entry_fee_copy) - exit_fee
            sim.realized += net_pnl - exit_fee
            sim.exit_count += 1
            if net_pnl >= 0:
                sim.win_count += 1
            else:
                sim.loss_count += 1
        plist.clear()
    sim.unrealized = 0.0
    sim.sync_equity()

    return sim


def build_portfolio_curve(wallet_sims: Dict[str, WalletSim]) -> List[Tuple[int, float]]:
    """
    Merge per-wallet equity curves into portfolio equity at each timestamp.
    
    Uses interpolation: at each unique timestamp, sum all wallet equities.
    For wallets that don't have a data point at a given timestamp, use their
    most recent equity value (step interpolation).
    """
    # Collect all unique timestamps
    all_timestamps = set()
    for sim in wallet_sims.values():
        for ts, eq in sim.curve:
            all_timestamps.add(ts)
    
    # Build per-wallet lookup: sorted (ts, eq) pairs
    wallet_curves = {}
    for w, sim in wallet_sims.items():
        wallet_curves[w] = sorted(sim.curve, key=lambda x: x[0])
    
    sorted_timestamps = sorted(all_timestamps)
    portfolio_curve = []
    
    # Track current equity for each wallet (step interpolation)
    current_equity = {w: sims.alloc for w, sims in wallet_sims.items()}
    wallet_idx = {w: 0 for w in wallet_sims}
    
    for ts in sorted_timestamps:
        for w in wallet_sims:
            curve = wallet_curves[w]
            idx = wallet_idx[w]
            # Advance past any timestamps <= current ts
            while idx < len(curve) and curve[idx][0] <= ts:
                current_equity[w] = curve[idx][1]
                idx += 1
            wallet_idx[w] = idx
        
        total_eq = sum(current_equity.values())
        portfolio_curve.append((ts, total_eq))
    
    return portfolio_curve


def calc_max_drawdown_from_curve(curve: List[Tuple[int, float]]) -> Tuple[float, float]:
    """Returns (max_drawdown, current_drawdown) from equity curve."""
    if not curve:
        return 0.0, 0.0
    peak = curve[0][1]
    max_dd = 0.0
    for ts, eq in curve:
        peak = max(peak, eq)
        dd = peak - eq
        max_dd = max(max_dd, dd)
    current_dd = peak - curve[-1][1]
    return max_dd, current_dd


def load_wallet_trades(wallet: str) -> List[dict]:
    """Load all trades for a wallet from all_trades.csv, sorted by time.
    Supports both full addresses and short prefixes."""
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


def print_wallet_metrics(sim: WalletSim, label: str = ""):
    """Print key metrics for a single wallet simulation."""
    total_trades = sim.win_count + sim.loss_count
    win_rate = (sim.win_count / total_trades * 100) if total_trades > 0 else 0
    pnl = sim.realized
    dd = sim.max_drawdown
    calmar = (pnl / dd) if dd > 0 else 0
    capture_pct = (sim.exit_count / sim.total_leader_fills * 100) if sim.total_leader_fills > 0 else 0
    mode_label = f"prop nb{int(sim.norm_base)}" if sim.copy_mode == "proportional" else f"fixed ${sim.alloc:.0f}"
    
    print(f"  {label:20s} | Mode: {mode_label:12s} | PnL: ${pnl:>8.2f} | DD: -${dd:>7.2f} | "
          f"Calmar: {calmar:>7.1f} | Win: {win_rate:>5.1f}% ({sim.win_count}/{total_trades}) | "
          f"Capt: {capture_pct:>5.1f}% ({sim.exit_count}/{sim.total_leader_fills})")


def main():
    print(f"=== PORTFOLIO SIMULATION: {NUM_WALLETS} wallets, ${TOTAL_SEED:.0f} total seed (${SEED_PER_WALLET:.0f}/wallet) ===")
    print(f"Copy fee: {COPY_FEE_BPS} bps | Min notional: ${MIN_COPY_NOTIONAL} | Leader equity base: ${LEADER_EQUITY_BASE:.0f}")
    print()

    # Load trades for all wallets
    all_wallet_trades = {}
    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        trades = load_wallet_trades(wallet)
        all_wallet_trades[wallet] = trades
        print(f"  {short}: {len(trades)} trades loaded")
    print()

    # ============================================================
    # PART 1: Per-wallet sweep across norm_bases + fixed
    # ============================================================
    print("=" * 120)
    print("PART 1: PER-WALLET RESULTS (best config per wallet)")
    print("=" * 120)
    
    best_configs = {}  # wallet -> best sim
    
    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        trades = all_wallet_trades[wallet]
        if not trades:
            print(f"  {short}: NO TRADES - skipping")
            continue
        
        print(f"\n--- {short} ({len(trades)} trades) ---")
        
        # Test fixed $12
        sim_fixed = simulate_wallet(wallet, trades, SEED_PER_WALLET, "fixed", fixed_notional=12.0)
        print_wallet_metrics(sim_fixed, "fixed $12")
        
        # Test proportional grid
        all_sims = [("fixed $12", sim_fixed)]
        for nb in NORM_BASE_GRID:
            sim_prop = simulate_wallet(wallet, trades, SEED_PER_WALLET, "proportional", norm_base=nb)
            if sim_prop.exit_count > 0:
                all_sims.append((f"prop nb{nb}", sim_prop))
                print_wallet_metrics(sim_prop, f"prop nb{nb}")
        
        # Pick best by Calmar (but require min 50 trades)
        best = None
        best_calmar = -1
        for label, sim in all_sims:
            total = sim.win_count + sim.loss_count
            if total >= 50:
                dd = sim.max_drawdown
                calmar = (sim.realized / dd) if dd > 0 else 0
                if calmar > best_calmar:
                    best_calmar = calmar
                    best = sim
        
        if best:
            best_configs[wallet] = best
            mode_label = f"prop nb{int(best.norm_base)}" if best.copy_mode == "proportional" else "fixed $12"
            print(f"  >>> BEST: {mode_label} Calmar={best_calmar:.1f}")

    # ============================================================
    # PART 2: Portfolio-level analysis with concurrent drawdown
    # ============================================================
    print("\n" + "=" * 120)
    print("PART 2: PORTFOLIO CONCURRENT DRAWDOWN (best configs)")
    print("=" * 120)
    
    if len(best_configs) < 2:
        print("Need at least 2 wallets for portfolio analysis")
        return
    
    # Run best configs and collect curves
    wallet_sims = {}
    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        if wallet not in best_configs:
            print(f"  {short}: skipped (no valid config)")
            continue
        trades = all_wallet_trades[wallet]
        sim = best_configs[wallet]
        # Re-run to get the curve (we already have it from above but need to re-reference)
        # Actually, the sim object already has the curve
        wallet_sims[wallet] = sim
        mode_label = f"prop nb{int(sim.norm_base)}" if sim.copy_mode == "proportional" else "fixed $12"
        print(f"  {short}: {mode_label} | Curve points: {len(sim.curve)}")
    
    # Build portfolio equity curve
    portfolio_curve = build_portfolio_curve(wallet_sims)
    
    # Portfolio metrics
    port_alloc = sum(sims.alloc for sims in wallet_sims.values())
    port_pnl = sum(sims.realized for sims in wallet_sims.values())
    port_peak = max(eq for _, eq in portfolio_curve) if portfolio_curve else port_alloc
    
    port_max_dd, port_current_dd = calc_max_drawdown_from_curve(portfolio_curve)
    port_calmar = (port_pnl / port_max_dd) if port_max_dd > 0 else 0
    
    # Individual metrics for comparison
    individual_sum_dd = sum(sims.max_drawdown for sims in wallet_sims.values())
    
    print(f"\n  PORTFOLIO AGGREGATE:")
    print(f"    Seed:              ${port_alloc:,.2f}")
    print(f"    Total PnL:         ${port_pnl:,.2f}")
    print(f"    Portfolio MaxDD:   ${port_max_dd:,.2f}")
    print(f"    Portfolio Calmar:  {port_calmar:,.1f}")
    print(f"    Current DD:        ${port_current_dd:,.2f}")
    print(f"    Sum of indiv DDs:  ${individual_sum_dd:,.2f}")
    print(f"    DD correlation:    {port_max_dd/individual_sum_dd:.2f}x (1.0=perfectly correlated, <1=beneficial diversification)")
    print(f"    Equity points:     {len(portfolio_curve)}")
    
    # ============================================================
    # PART 3: Sweep norm_base at PORTFOLIO level
    # ============================================================
    print("\n" + "=" * 120)
    print("PART 3: PORTFOLIO SWEEP — same norm_base for all wallets")
    print("=" * 120)
    
    portfolio_results = []
    
    for nb in NORM_BASE_GRID:
        sims = {}
        for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
            trades = all_wallet_trades[wallet]
            if not trades:
                continue
            sim = simulate_wallet(wallet, trades, SEED_PER_WALLET, "proportional", norm_base=nb)
            sims[wallet] = sim
        
        if len(sims) < 2:
            continue
        
        port_curve = build_portfolio_curve(sims)
        p_dd, p_cdd = calc_max_drawdown_from_curve(port_curve)
        p_pnl = sum(s.realized for s in sims.values())
        p_calmar = (p_pnl / p_dd) if p_dd > 0 else 0
        p_wins = sum(s.win_count for s in sims.values())
        p_losses = sum(s.loss_count for s in sims.values())
        p_wr = (p_wins / (p_wins + p_losses) * 100) if (p_wins + p_losses) > 0 else 0
        p_exit = sum(s.exit_count for s in sims.values())
        p_fills = sum(s.total_leader_fills for s in sims.values())
        p_cap = (p_exit / p_fills * 100) if p_fills > 0 else 0
        ind_dd = sum(s.max_drawdown for s in sims.values())
        corr = p_dd / ind_dd if ind_dd > 0 else 0
        
        portfolio_results.append({
            "norm_base": nb, "pnl": p_pnl, "max_dd": p_dd,
            "calmar": p_calmar, "win_rate": p_wr, "capture": p_cap,
            "ind_dd_sum": ind_dd, "correlation": corr,
            "trades": p_wins + p_losses,
        })
        
        print(f"  nb={nb:>5d} | PnL: ${p_pnl:>9.2f} | MaxDD: ${p_dd:>8.2f} | Calmar: {p_calmar:>7.1f} | "
              f"Win: {p_wr:>5.1f}% | Cap: {p_cap:>5.1f}% | Corr: {corr:.2f}")
    
    # Also test fixed $12 at portfolio level
    sims_fixed = {}
    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        trades = all_wallet_trades[wallet]
        if not trades:
            continue
        sim = simulate_wallet(wallet, trades, SEED_PER_WALLET, "fixed", fixed_notional=12.0)
        sims_fixed[wallet] = sim
    
    if len(sims_fixed) >= 2:
        port_curve_f = build_portfolio_curve(sims_fixed)
        p_dd_f, _ = calc_max_drawdown_from_curve(port_curve_f)
        p_pnl_f = sum(s.realized for s in sims_fixed.values())
        p_calmar_f = (p_pnl_f / p_dd_f) if p_dd_f > 0 else 0
        p_wins_f = sum(s.win_count for s in sims_fixed.values())
        p_losses_f = sum(s.loss_count for s in sims_fixed.values())
        p_wr_f = (p_wins_f / (p_wins_f + p_losses_f) * 100) if (p_wins_f + p_losses_f) > 0 else 0
        ind_dd_f = sum(s.max_drawdown for s in sims_fixed.values())
        corr_f = p_dd_f / ind_dd_f if ind_dd_f > 0 else 0
        
        print(f"\n  FIXED  | PnL: ${p_pnl_f:>9.2f} | MaxDD: ${p_dd_f:>8.2f} | Calmar: {p_calmar_f:>7.1f} | "
              f"Win: {p_wr_f:>5.1f}% | Corr: {corr_f:.2f}")
    
    # ============================================================
    # PART 4: Optimal mixed configs
    # ============================================================
    print("\n" + "=" * 120)
    print("PART 4: OPTIMAL MIXED CONFIG (best mode per wallet, portfolio view)")
    print("=" * 120)
    
    # best_configs already has the per-wallet best
    mixed_sims = {}
    for wallet, short in zip(USER_WALLETS, USER_WALLET_SHORT):
        if wallet in best_configs:
            mixed_sims[wallet] = best_configs[wallet]
            sim = best_configs[wallet]
            mode_label = f"prop nb{int(sim.norm_base)}" if sim.copy_mode == "proportional" else "fixed $12"
            print(f"  {short}: {mode_label}")
    
    if len(mixed_sims) >= 2:
        port_curve_m = build_portfolio_curve(mixed_sims)
        p_dd_m, p_cdd_m = calc_max_drawdown_from_curve(port_curve_m)
        p_pnl_m = sum(s.realized for s in mixed_sims.values())
        p_calmar_m = (p_pnl_m / p_dd_m) if p_dd_m > 0 else 0
        p_wins_m = sum(s.win_count for s in mixed_sims.values())
        p_losses_m = sum(s.loss_count for s in mixed_sims.values())
        p_wr_m = (p_wins_m / (p_wins_m + p_losses_m) * 100) if (p_wins_m + p_losses_m) > 0 else 0
        ind_dd_m = sum(s.max_drawdown for s in mixed_sims.values())
        corr_m = p_dd_m / ind_dd_m if ind_dd_m > 0 else 0
        
        print(f"\n  MIXED PORTFOLIO:")
        print(f"    Seed:              ${sum(s.alloc for s in mixed_sims.values()):,.2f}")
        print(f"    Total PnL:         ${p_pnl_m:,.2f}")
        print(f"    Portfolio MaxDD:   ${p_dd_m:,.2f}")
        print(f"    Portfolio Calmar:  {p_calmar_m:,.1f}")
        print(f"    Current DD:        ${p_cdd_m:,.2f}")
        print(f"    Win Rate:          {p_wr_m:.1f}%")
        print(f"    DD correlation:    {corr_m:.2f}x")
        print(f"    Required bankroll: ${p_dd_m * 1.25:,.2f} (MaxDD + 25% buffer)")
        print(f"    Conservative:      ${p_dd_m * 3:,.2f} (3x MaxDD)")


if __name__ == "__main__":
    main()
