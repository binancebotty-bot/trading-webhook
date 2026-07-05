"""Wallet Finder V3: Every wallet gets its optimal config (mode + params).

For each eligible wallet:
  1) Simulate fixed $12
  2) Simulate proportional at nb ∈ {50, 100, 150, 200, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000}
  3) Apply capture gate: reject configs with < MIN_CAPTURE% trade capture
  4) Pick best qualifying config per wallet by composite score
  5) Rank all wallets by their best config's composite

This answers: "What is the best wallet AND the best way to copy it?"
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"

MIN_COPY_NOTIONAL = 12.0
LEADER_EQUITY_BASE = 10000.0
NORM_BASE_GRID = [50, 100, 150, 200, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000]
MIN_TRADES = 300
MIN_CAPTURE_PCT = 50.0  # reject configs that capture <50% of trades
MIN_EXEC_TRADES = 30    # minimum executed trades for valid metrics


# ─── Metrics ────────────────────────────────────────────────────────────
def calc_sortino(returns: np.ndarray) -> float:
    if len(returns) < 2:
        return 0.0
    mu = np.mean(returns)
    neg = returns[returns < 0]
    if len(neg) == 0:
        return 10.0 if mu > 0 else 0.0
    dd = np.std(neg)
    if dd < 1e-12:
        return 10.0 if mu > 0 else 0.0
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


def composite_score(c: float, s: float, dd: float, w: float) -> float:
    """Composite: Calmar 40%, Sortino 30%, MaxDD inverse 20%, WinRate 10%.
    DD term: 1/(1+abs(dd)) so smaller DD → higher score (capped at 1.0).
    """
    dd_inv = 1.0 / (1.0 + abs(dd))  # ranges 0..1, higher is better (less DD)
    return 0.4 * c + 0.3 * s + 0.2 * (dd_inv * 100) + 0.1 * (w * 100)


# ─── Fixed $12 replay ──────────────────────────────────────────────────
def replay_fixed(df: pd.DataFrame, fn: float) -> dict:
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    leader_n = np.abs(px * sz)
    n_total = len(leader_n)
    # Filter: only copy trades where leader notional > $1 (skip zero/micro fills)
    mask = leader_n > 1.0
    n_kept = int(mask.sum())
    if n_kept == 0:
        return {"total_pnl": 0.0, "max_dd": 0.0, "calmar": 0.0, "sortino": 0.0,
                "win_rate": 0.0, "n_trades_executed": 0, "trade_capture_pct": 0.0}
    ratio = fn / leader_n[mask]
    pnl = closed[mask] * ratio
    equity = np.cumsum(pnl)
    m = calc_metrics(equity, pnl)
    m["trade_capture_pct"] = n_kept / n_total * 100
    return m


# ─── Proportional replay with $12 min cutoff ──────────────────────────
def replay_proportional(df: pd.DataFrame, norm_base: float) -> dict:
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    leader_n = np.abs(px * sz)
    scale = norm_base / LEADER_EQUITY_BASE
    scaled_n = leader_n * scale
    mask = scaled_n >= MIN_COPY_NOTIONAL
    n_total = len(mask)
    n_kept = int(mask.sum())
    if n_kept == 0:
        return {"total_pnl": 0.0, "max_dd": 0.0, "calmar": 0.0, "sortino": 0.0,
                "win_rate": 0.0, "n_trades_executed": 0, "trade_capture_pct": 0.0}
    pnl = closed[mask] * scale
    equity = np.cumsum(pnl)
    m = calc_metrics(equity, pnl)
    m["trade_capture_pct"] = n_kept / n_total * 100
    m["scale"] = scale
    m["avg_copy_size"] = float(np.mean(leader_n[mask] * scale)) if n_kept > 0 else 0.0
    return m


# ─── Main ───────────────────────────────────────────────────────────────
print("=" * 100)
print("WALLET FINDER V3 — Optimal config per wallet, ranked by composite")
print(f"Min capture gate: {MIN_CAPTURE_PCT}%")
print(f"Norm base grid: {NORM_BASE_GRID}")
print("=" * 100)

print("\nLoading copyable_wallets.csv...")
cand = pd.read_csv(DATA / "copyable_wallets.csv")
cand["wallet"] = cand["wallet"].str.lower()
for c in ("mtm_calmar", "equity_collapse_flag_mtm", "negative_total_flag_mtm",
          "month_acctV_end", "month_pnl_chg_mtm", "max_drawdown_mtm", "trades"):
    cand[c] = pd.to_numeric(cand[c], errors="coerce")
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
print(f"After min {MIN_TRADES} trades: {len(mtm_pass)}")

# ── Stage 2: Load trades ──────────────────────────────────────────────
print("Loading all_trades.csv...")
t0 = time.time()
from trades_path import trades_csv
all_trades = pd.read_csv(trades_csv(),
                         usecols=["wallet", "time", "coin", "side", "px", "sz", "closedPnl"])
all_trades["wallet"] = all_trades["wallet"].str.lower()
print(f"Loaded {len(all_trades):,} trades for {all_trades.wallet.nunique()} wallets in {time.time()-t0:.1f}s")

# ── Stage 3: Simulate every config for every wallet ───────────────────
n_configs = len(NORM_BASE_GRID) + 1  # proportional grid + fixed
print(f"\nSimulating {len(mtm_pass)} wallets × {n_configs} configs = {len(mtm_pass)*n_configs} simulations...")
t0 = time.time()

wallet_results = []  # list of {wallet, meta, configs: [best configs]}

for n, cw in enumerate(mtm_pass.itertuples()):
    w = cw.wallet
    wt = all_trades[all_trades.wallet == w].sort_values("time").reset_index(drop=True)
    if len(wt) < MIN_TRADES:
        continue

    # Fixed $12
    fixed = replay_fixed(wt, 12.0)

    # Proportional at each nb
    prop_results = {}
    for nb in NORM_BASE_GRID:
        prop_results[nb] = replay_proportional(wt, nb)

    # Collect ALL qualifying configs (capture >= gate, enough trades)
    candidates = []

    # Fixed always qualifies (100% capture)
    if fixed["n_trades_executed"] >= MIN_EXEC_TRADES:
        candidates.append({
            "mode": "fixed",
            "params": "fixed_notional=12",
            "norm_base": 0,
            **fixed,
        })

    for nb in NORM_BASE_GRID:
        pr = prop_results[nb]
        if (pr["trade_capture_pct"] >= MIN_CAPTURE_PCT and
                pr["n_trades_executed"] >= MIN_EXEC_TRADES):
            candidates.append({
                "mode": "proportional",
                "params": f"norm_base={nb}",
                "norm_base": nb,
                **pr,
            })

    if not candidates:
        continue

    # Rank candidates by composite
    for c in candidates:
        c["composite"] = composite_score(c["calmar"], c["sortino"], c["max_dd"], c["win_rate"])

    best = max(candidates, key=lambda x: x["composite"])

    # Store all qualifying configs for this wallet
    wallet_results.append({
        "wallet": w,
        "trades_total": int(cw.trades),
        "mtm_calmar": cw.mtm_calmar,
        "month_acctV_chg": cw.month_acctV_chg,
        "month_acctV_end": cw.month_acctV_end,
        # Best config
        "best_mode": best["mode"],
        "best_params": best["params"],
        "best_norm_base": best.get("norm_base", 0),
        "best_scale": best.get("scale", 1.0) if best["mode"] == "proportional" else 1.0,
        "best_avg_copy_size": best.get("avg_copy_size", 12.0),
        "best_capture_pct": best["trade_capture_pct"],
        "best_pnl": best["total_pnl"],
        "best_dd": best["max_dd"],
        "best_calmar": best["calmar"],
        "best_sortino": best["sortino"],
        "best_win_rate": best["win_rate"],
        "best_trades_exec": best["n_trades_executed"],
        "best_composite": best["composite"],
        "n_qualifying_configs": len(candidates),
        # Store fixed stats for comparison
        "fixed_calmar": fixed["calmar"],
        "fixed_pnl": fixed["total_pnl"],
        "fixed_dd": fixed["max_dd"],
        "fixed_win_rate": fixed["win_rate"],
        # Store best proportional for comparison
        "best_prop_nb": best.get("norm_base", 0),
        "best_prop_calmar": best["calmar"] if best["mode"] == "proportional" else 0,
        "best_prop_capture": best.get("trade_capture_pct", 0) if best["mode"] == "proportional" else 0,
    })

    if (n + 1) % 10 == 0:
        print(f"  {n+1}/{len(mtm_pass)}  dt={time.time()-t0:.1f}s")

print(f"\nSimulation done in {time.time()-t0:.1f}s")
print(f"Wallets with qualifying configs: {len(wallet_results)}")

# ── Stage 4: Rank all wallets ─────────────────────────────────────────
results = pd.DataFrame(wallet_results)
results = results.sort_values("best_composite", ascending=False).reset_index(drop=True)

# ── Stage 5: Output ───────────────────────────────────────────────────
print(f"\n{'='*110}")
print(f"TOP 20 WALLETS — Optimal config per wallet (min {MIN_CAPTURE_PCT}% capture)")
print(f"{'='*110}")

for i, r in results.head(20).iterrows():
    print(f"\n#{i+1} {r.wallet}")
    print(f"   Config: {r.best_mode} ({r.best_params})")
    if r.best_mode == "proportional":
        print(f"   Scale: {r.best_scale:.4f} | Avg copy size: ${r.best_avg_copy_size:,.0f}")
    print(f"   Calmar: {r.best_calmar:.2f} | Sortino: {r.best_sortino:.2f} | "
          f"MaxDD: ${r.best_dd:,.0f} | Win: {r.best_win_rate:.1%}")
    print(f"   PnL: ${r.best_pnl:,.0f} | Trades: {r.best_trades_exec}/{r.trades_total} "
          f"({r.best_capture_pct:.0f}% capture)")
    print(f"   Composite: {r.best_composite:.2f} | Qualifying configs: {r.n_qualifying_configs}")
    print(f"   Leader: acctV=${r.month_acctV_end:,.0f} | MTM Calmar: {r.mtm_calmar:.2f}")
    if r.fixed_calmar > 0:
        print(f"   (Fixed alternative: Calmar={r.fixed_calmar:.1f}, PnL=${r.fixed_pnl:,.0f}, "
              f"DD=${r.fixed_dd:,.0f}, Win={r.fixed_win_rate:.0%})")

# ── Summary ────────────────────────────────────────────────────────────
print(f"\n{'='*110}")
print("SUMMARY")
print(f"{'='*110}")

top20 = results.head(20)
mode_counts = top20["best_mode"].value_counts()
print(f"Mode distribution (top 20): {mode_counts.to_dict()}")

print(f"\nNorm base distribution (proportional wallets only):")
prop_top = top20[top20.best_mode == "proportional"]
if len(prop_top) > 0:
    nb_dist = prop_top["best_norm_base"].value_counts().sort_index()
    for nb, cnt in nb_dist.items():
        avg_cap = prop_top[prop_top.best_norm_base == nb]["best_capture_pct"].mean()
        avg_pnl = prop_top[prop_top.best_norm_base == nb]["best_pnl"].mean()
        print(f"  nb={nb:>6}: {cnt} wallets, avg capture={avg_cap:.0f}%, avg PnL=${avg_pnl:,.0f}")

print(f"\nCapture% stats (top 20):")
print(f"  Min: {top20.best_capture_pct.min():.0f}%")
print(f"  Max: {top20.best_capture_pct.max():.0f}%")
print(f"  Median: {top20.best_capture_pct.median():.0f}%")

# Save
results.to_csv(DATA / "wallet_analysis_v3.csv", index=False)
results.head(20).to_csv(DATA / "top20_wallets_v3.csv", index=False)
print(f"\nFull results: {DATA / 'wallet_analysis_v3.csv'}")
print(f"Top 20: {DATA / 'top20_wallets_v3.csv'}")
