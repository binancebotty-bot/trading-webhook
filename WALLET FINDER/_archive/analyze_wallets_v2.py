"""Wallet Finder V2: min-300 trades + multi-norm_base proportional + fixed $12.

For each copyable wallet with 300+ trades:
  1) Filter by MTM criteria (Calmar>=1.5, no collapse, no negative total, acctV>=5000)
  2) Simulate proportional model at multiple norm_base values (50, 100, 200, 500, 1000, 2000)
     - Each trade is only copied if scaled notional >= $12 (HL minimum)
  3) Simulate fixed $12 model
  4) Pick best model+params per wallet by Calmar ratio
  5) Rank top 10 by composite: Calmar 40%, Sortino 30%, MaxDD inverse 20%, WinRate 10%
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
import time
import sys

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
COPY_SEL = ROOT / "copy_selection_run"

# Leverage table (inline to avoid sys.path issues)
LEV_DEFAULT = 10
LEV_MAP = {
    "BTC": 40, "ETH": 25,
    "SOL": 20, "XRP": 20, "BNB": 10, "AVAX": 20, "MATIC": 20, "LTC": 20,
    "ARB": 20, "OP": 20, "APT": 20, "ATOM": 20, "DOGE": 20, "LINK": 20,
    "DOT": 20, "TRX": 20, "BCH": 20, "FIL": 20, "NEAR": 20, "INJ": 20,
    "SUI": 20, "TIA": 20, "JUP": 20, "AAVE": 20, "ADA": 20, "TON": 20,
    "HYPE": 10, "PENDLE": 10, "FARTCOIN": 5, "KPEPE": 5, "WIF": 5,
}
def lev(coin: str) -> int:
    return LEV_MAP.get((coin or "").upper(), LEV_DEFAULT)

MIN_COPY_NOTIONAL = 12.0
LEADER_EQUITY_BASE = 10000.0
NORM_BASE_GRID = [50, 100, 200, 500, 1000, 2000]
MIN_TRADES = 300


# ─── Metrics ────────────────────────────────────────────────────────────
def calc_sortino(returns: np.ndarray) -> float:
    """Annualized Sortino from per-trade returns (approximation)."""
    if len(returns) < 2:
        return 0.0
    mu = np.mean(returns)
    neg = returns[returns < 0]
    if len(neg) == 0:
        return 10.0 if mu > 0 else 0.0
    dd = np.std(neg)
    if dd < 1e-12:
        return 10.0 if mu > 0 else 0.0
    # Annualize roughly: sqrt(N_trades_per_year) ~ 252 for daily
    ann_factor = np.sqrt(min(len(returns), 252))
    return float(mu / dd * ann_factor)


def calc_max_dd(equity: np.ndarray) -> float:
    if len(equity) == 0:
        return 0.0
    rm = np.maximum.accumulate(equity)
    return float((equity - rm).min())


def calc_metrics(equity: np.ndarray, pnl_per_trade: np.ndarray) -> dict:
    total = float(equity[-1]) if len(equity) else 0.0
    mdd = calc_max_dd(equity)
    calmar = total / abs(mdd) if mdd < -1 else (total / 1.0 if total > 0 else 0.0)
    wins = np.sum(pnl_per_trade > 0)
    losses = np.sum(pnl_per_trade < 0)
    win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0
    sortino = calc_sortino(pnl_per_trade)
    return {
        "total_pnl": total,
        "max_dd": mdd,
        "calmar": calmar,
        "sortino": sortino,
        "win_rate": win_rate,
        "n_trades_executed": int(wins + losses),
    }


# ─── Fixed $12 replay ──────────────────────────────────────────────────
def replay_fixed(df: pd.DataFrame, fn: float) -> dict:
    """Replay trades at fixed notional. Every trade executes."""
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    side = df["side"].to_numpy()
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    coin = df["coin"].to_numpy()
    leader_n = np.abs(px * sz)
    leader_n_safe = np.where(leader_n > 0, leader_n, 1e-9)
    ratio = fn / leader_n_safe
    pnl = closed * ratio
    equity = np.cumsum(pnl)
    return calc_metrics(equity, pnl)


# ─── Proportional replay with $12 min cutoff ──────────────────────────
def replay_proportional(df: pd.DataFrame, norm_base: float) -> dict:
    """Replay trades proportionally, dropping trades where scaled notional < $12."""
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    side = df["side"].to_numpy()
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    leader_n = np.abs(px * sz)
    scale = norm_base / LEADER_EQUITY_BASE
    scaled_n = leader_n * scale
    # Filter: only copy trades where scaled notional >= $12
    mask = scaled_n >= MIN_COPY_NOTIONAL
    if mask.sum() == 0:
        return {"total_pnl": 0.0, "max_dd": 0.0, "calmar": 0.0, "sortino": 0.0,
                "win_rate": 0.0, "n_trades_executed": 0, "trade_capture_pct": 0.0}
    pnl = closed[mask] * scale
    equity = np.cumsum(pnl)
    m = calc_metrics(equity, pnl)
    m["trade_capture_pct"] = float(mask.sum()) / len(mask) * 100
    return m


# ─── Main ───────────────────────────────────────────────────────────────
print("Loading copyable_wallets.csv...")
cand = pd.read_csv(DATA / "copyable_wallets.csv")
cand["wallet"] = cand["wallet"].str.lower()
for c in ("mtm_calmar", "equity_collapse_flag_mtm", "negative_total_flag_mtm",
          "month_acctV_end", "month_pnl_chg_mtm", "max_drawdown_mtm", "trades"):
    cand[c] = pd.to_numeric(cand[c], errors="coerce")

# Rename for consistency
cand = cand.rename(columns={
    "month_pnl_chg_mtm": "month_acctV_chg",
    "max_drawdown_mtm": "month_acctV_mdd_mtm",
})

print(f"Total copyable wallets: {len(cand)}")

# ── Stage 1: MTM filter + min trades ──────────────────────────────────
mtm_pass = cand[
    (cand.mtm_calmar >= 1.5) &
    (cand.equity_collapse_flag_mtm == 0) &
    (cand.negative_total_flag_mtm == 0) &
    (cand.month_acctV_end >= 5000) &
    (cand.mtm_source != "unavailable")
].copy()
print(f"After MTM filter: {len(mtm_pass)}")

mtm_pass = mtm_pass[mtm_pass.trades >= MIN_TRADES].copy()
print(f"After min {MIN_TRADES} trades filter: {len(mtm_pass)}")

# ── Stage 2: Load trades from all_trades.csv ──────────────────────────
print("Loading all_trades.csv (this may take a moment)...")
t0 = time.time()
all_trades = pd.read_csv(DATA / "all_trades.csv", usecols=["wallet", "time", "coin", "side", "px", "sz", "closedPnl"])
all_trades["wallet"] = all_trades["wallet"].str.lower()
print(f"Loaded {len(all_trades):,} trades for {all_trades.wallet.nunique()} wallets in {time.time()-t0:.1f}s")

# ── Stage 3: Simulate each wallet ─────────────────────────────────────
print(f"\nSimulating {len(mtm_pass)} wallets × ({len(NORM_BASE_GRID)} proportional + 1 fixed)...")
t0 = time.time()
rows = []
skipped = 0

for n, cw in enumerate(mtm_pass.itertuples()):
    w = cw.wallet
    wt = all_trades[all_trades.wallet == w].sort_values("time").reset_index(drop=True)
    if len(wt) < MIN_TRADES:
        skipped += 1
        continue

    # Fixed $12
    fixed = replay_fixed(wt, 12.0)

    # Proportional at each norm_base
    prop_results = {}
    for nb in NORM_BASE_GRID:
        prop_results[nb] = replay_proportional(wt, nb)

    # Pick best per-wallet option: compare all proportional variants + fixed
    candidates = []
    for nb in NORM_BASE_GRID:
        pr = prop_results[nb]
        if pr["calmar"] > 0 and pr["n_trades_executed"] >= 30:
            candidates.append({
                "mode": "proportional",
                "params": f"norm_base={nb}",
                "norm_base": nb,
                **pr,
            })
    if fixed["calmar"] > 0 and fixed["n_trades_executed"] >= 30:
        candidates.append({
            "mode": "fixed",
            "params": "fixed_notional=12",
            "norm_base": 0,
            **fixed,
        })

    if not candidates:
        skipped += 1
        continue

    # Pick best by calmar
    best = max(candidates, key=lambda x: x["calmar"])

    rows.append({
        "wallet": w,
        "trades_total": int(cw.trades),
        "mtm_calmar": cw.mtm_calmar,
        "month_acctV_chg": cw.month_acctV_chg,
        "month_acctV_end": cw.month_acctV_end,
        "chosen_mode": best["mode"],
        "chosen_params": best["params"],
        "chosen_norm_base": best.get("norm_base", 0),
        "total_pnl": best["total_pnl"],
        "max_dd": best["max_dd"],
        "calmar": best["calmar"],
        "sortino": best["sortino"],
        "win_rate": best["win_rate"],
        "n_trades_executed": best["n_trades_executed"],
        "trade_capture_pct": best.get("trade_capture_pct", 100.0),
        # Also store proportional variants for comparison
        "prop_nb50_calmar": prop_results[50]["calmar"],
        "prop_nb100_calmar": prop_results[100]["calmar"],
        "prop_nb200_calmar": prop_results[200]["calmar"],
        "prop_nb500_calmar": prop_results[500]["calmar"],
        "prop_nb1000_calmar": prop_results[1000]["calmar"],
        "prop_nb2000_calmar": prop_results[2000]["calmar"],
        "fixed_calmar": fixed["calmar"],
        "prop_nb50_capture": prop_results[50].get("trade_capture_pct", 0),
        "prop_nb100_capture": prop_results[100].get("trade_capture_pct", 0),
        "prop_nb200_capture": prop_results[200].get("trade_capture_pct", 0),
        "prop_nb500_capture": prop_results[500].get("trade_capture_pct", 0),
        "prop_nb1000_capture": prop_results[1000].get("trade_capture_pct", 0),
        "prop_nb2000_capture": prop_results[2000].get("trade_capture_pct", 0),
    })

    if (n + 1) % 10 == 0:
        print(f"  {n+1}/{len(mtm_pass)}  dt={time.time()-t0:.1f}s")

print(f"\nSimulation done in {time.time()-t0:.1f}s")
print(f"  Processed: {len(rows)}, Skipped: {skipped}")

# ── Stage 4: Rank top 10 by composite ─────────────────────────────────
results = pd.DataFrame(rows)

# Composite: Calmar 40%, Sortino 30%, MaxDD inverse 20%, WinRate 10%
def rank_composite(r):
    c = r["calmar"]
    s = r["sortino"]
    d = -r["max_dd"] if r["max_dd"] < 0 else 1.0  # inverse: less DD = better
    w = r["win_rate"]
    return 0.4 * c + 0.3 * s + 0.2 * d + 0.1 * (w * 100)

results["composite"] = results.apply(rank_composite, axis=1)
results = results.sort_values("composite", ascending=False).reset_index(drop=True)

top10 = results.head(10)
print(f"\n{'='*100}")
print(f"TOP 10 WALLETS (min {MIN_TRADES} trades, best model per wallet)")
print(f"{'='*100}")

for i, r in top10.iterrows():
    print(f"\n#{i+1} {r.wallet}")
    print(f"   Mode: {r.chosen_mode} ({r.chosen_params})")
    print(f"   Calmar: {r.calmar:.2f} | Sortino: {r.sortino:.2f} | MaxDD: ${r.max_dd:,.0f} | WinRate: {r.win_rate:.1%}")
    print(f"   Total PnL: ${r.total_pnl:,.0f} | Trades executed: {r.n_trades_executed} / {r.trades_total}")
    print(f"   Trade capture: {r.trade_capture_pct:.1f}%")
    print(f"   MTM Calmar: {r.mtm_calmar:.2f} | Leader acctV: ${r.month_acctV_end:,.0f}")
    print(f"   Composite: {r.composite:.2f}")
    # Show norm_base comparison
    nb_calmars = [f"nb{nb}={r[f'prop_nb{nb}_calmar']:.1f}" for nb in NORM_BASE_GRID]
    print(f"   Proportional Calmars: {', '.join(nb_calmars)}")
    print(f"   Fixed Calmar: {r.fixed_calmar:.1f}")

# ── Stage 5: Summary ──────────────────────────────────────────────────
print(f"\n{'='*100}")
print("SUMMARY")
print(f"{'='*100}")
mode_counts = top10["chosen_mode"].value_counts()
print(f"Mode distribution in top 10: {mode_counts.to_dict()}")

# Show how norm_base affects trade capture
print(f"\nProportional trade capture by norm_base (top 10 wallets):")
for nb in NORM_BASE_GRID:
    col = f"prop_nb{nb}_capture"
    avg_cap = top10[col].mean()
    print(f"  norm_base={nb:>5}: avg capture {avg_cap:.1f}%")

# Save full results
results.to_csv(DATA / "wallet_analysis_v2.csv", index=False)
top10.to_csv(DATA / "top10_wallets_v2.csv", index=False)
print(f"\nResults saved to: {DATA / 'wallet_analysis_v2.csv'}")
print(f"Top 10 saved to: {DATA / 'top10_wallets_v2.csv'}")
