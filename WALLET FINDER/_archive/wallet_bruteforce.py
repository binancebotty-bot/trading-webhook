"""Brute-force check: simulate ALL 221 copyable wallets (fixed $12 only)
to find the absolute best, regardless of MTM filter or trade count.
Then compare against the filtered set to see what we're missing."""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"

MIN_COPY_NOTIONAL = 12.0
LEADER_EQUITY_BASE = 10000.0

def calc_sortino(returns):
    if len(returns) < 2: return 0.0
    mu = np.mean(returns)
    neg = returns[returns < 0]
    if len(neg) == 0: return 10.0 if mu > 0 else 0.0
    dd = np.std(neg)
    if dd < 1e-12: return 10.0 if mu > 0 else 0.0
    return float(mu / dd * np.sqrt(min(len(returns), 252)))

def calc_max_dd(equity):
    if len(equity) == 0: return 0.0
    return float((equity - np.maximum.accumulate(equity)).min())

def calc_metrics(equity, pnl):
    total = float(equity[-1]) if len(equity) else 0.0
    mdd = calc_max_dd(equity)
    calmar = total / abs(mdd) if mdd < -1 else (total if total > 0 else 0.0)
    wins = np.sum(pnl > 0); losses = np.sum(pnl < 0)
    wr = wins / (wins + losses) if (wins + losses) > 0 else 0.0
    sortino = calc_sortino(pnl)
    dd_inv = 1.0 / (1.0 + abs(mdd))
    composite = 0.4 * calmar + 0.3 * sortino + 0.2 * (dd_inv * 100) + 0.1 * (wr * 100)
    return {"total_pnl": total, "max_dd": mdd, "calmar": calmar, "sortino": sortino,
            "win_rate": wr, "n_exec": int(wins + losses), "composite": composite}

def replay_fixed(df, fn=12.0):
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    leader_n = np.abs(px * sz)
    mask = leader_n > 1.0
    if mask.sum() == 0:
        return {"total_pnl": 0, "max_dd": 0, "calmar": 0, "sortino": 0,
                "win_rate": 0, "n_exec": 0, "composite": 0, "capture": 0}
    ratio = fn / leader_n[mask]
    pnl = closed[mask] * ratio
    equity = np.cumsum(pnl)
    m = calc_metrics(equity, pnl)
    m["capture"] = mask.sum() / len(mask) * 100
    return m

def replay_prop(df, nb):
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    leader_n = np.abs(px * sz)
    scale = nb / LEADER_EQUITY_BASE
    scaled_n = leader_n * scale
    mask = scaled_n >= MIN_COPY_NOTIONAL
    n_total = len(mask)
    n_kept = int(mask.sum())
    if n_kept == 0:
        return {"total_pnl": 0, "max_dd": 0, "calmar": 0, "sortino": 0,
                "win_rate": 0, "n_exec": 0, "composite": 0, "capture": 0}
    pnl = closed[mask] * scale
    equity = np.cumsum(pnl)
    m = calc_metrics(equity, pnl)
    m["capture"] = n_kept / n_total * 100
    m["scale"] = scale
    m["avg_copy_size"] = float(np.mean(leader_n[mask] * scale))
    return m

# Load all data
print("Loading data...")
cand = pd.read_csv(DATA / "copyable_wallets.csv")
cand["wallet"] = cand["wallet"].str.lower()
for c in ("mtm_calmar", "equity_collapse_flag_mtm", "negative_total_flag_mtm",
          "month_acctV_end", "month_pnl_chg_mtm", "max_drawdown_mtm", "trades"):
    cand[c] = pd.to_numeric(cand[c], errors="coerce")
cand = cand.rename(columns={"month_pnl_chg_mtm": "month_acctV_chg",
                             "max_drawdown_mtm": "month_acctV_mdd_mtm"})

from trades_path import trades_csv
all_trades = pd.read_csv(trades_csv(),
                         usecols=["wallet", "time", "coin", "side", "px", "sz", "closedPnl"])
all_trades["wallet"] = all_trades["wallet"].str.lower()
print(f"Copyable wallets: {len(cand)}, Trades: {len(all_trades):,}")

# ── Step 1: Simulate ALL wallets with fixed $12 ───────────────────────
print(f"\nSimulating ALL {len(cand)} wallets (fixed $12 + proportional grid)...")
t0 = time.time()
NB_GRID = [100, 200, 500, 1000, 2000, 5000]

rows = []
for n, cw in enumerate(cand.itertuples()):
    w = cw.wallet
    wt = all_trades[all_trades.wallet == w].sort_values("time").reset_index(drop=True)
    n_trades = len(wt)
    if n_trades < 50:  # absolute minimum
        continue

    # Fixed $12
    fixed = replay_fixed(wt)

    # Proportional at best grid point (just nb=500 and nb=2000 for speed)
    props = {}
    for nb in NB_GRID:
        props[nb] = replay_prop(wt, nb)

    # Best proportional (by composite, min 50% capture)
    best_prop = None
    for nb in NB_GRID:
        pr = props[nb]
        if pr["capture"] >= 50 and pr["n_exec"] >= 30:
            if best_prop is None or pr["composite"] > best_prop["composite"]:
                best_prop = {**pr, "norm_base": nb}

    # Pick best overall (fixed vs proportional)
    candidates = []
    if fixed["n_exec"] >= 30:
        candidates.append({**fixed, "mode": "fixed", "norm_base": 0})
    if best_prop:
        candidates.append({**best_prop, "mode": "proportional"})

    if not candidates:
        continue

    best = max(candidates, key=lambda x: x["composite"])

    rows.append({
        "wallet": w,
        "n_trades": n_trades,
        "mtm_calmar": cw.mtm_calmar,
        "month_acctV_end": cw.month_acctV_end,
        "equity_collapse": cw.equity_collapse_flag_mtm,
        "negative_total": cw.negative_total_flag_mtm,
        "best_mode": best["mode"],
        "best_nb": best.get("norm_base", 0),
        "best_calmar": best["calmar"],
        "best_pnl": best["total_pnl"],
        "best_dd": best["max_dd"],
        "best_win_rate": best["win_rate"],
        "best_capture": best["capture"],
        "best_composite": best["composite"],
        "fixed_calmar": fixed["calmar"],
        "fixed_pnl": fixed["total_pnl"],
        "fixed_dd": fixed["max_dd"],
        "fixed_capture": fixed["capture"],
        # Qualification flags
        "passes_mtm_filter": bool(cw.mtm_calmar >= 1.5 and
                              cw.equity_collapse_flag_mtm == 0 and
                              cw.negative_total_flag_mtm == 0 and
                              cw.month_acctV_end >= 5000),
        "has_300_trades": n_trades >= 300,
    })

    if (n + 1) % 50 == 0:
        print(f"  {n+1}/{len(cand)}  dt={time.time()-t0:.1f}s")

print(f"\nDone in {time.time()-t0:.1f}s — {len(rows)} wallets simulated")

# ── Step 2: Rank and compare ──────────────────────────────────────────
results = pd.DataFrame(rows).sort_values("best_composite", ascending=False).reset_index(drop=True)

print(f"\n{'='*110}")
print(f"TOP 30 WALLETS — ABSOLUTE BEST (no MTM filter, min 50 trades)")
print(f"{'='*110}")

for i, r in results.head(30).iterrows():
    passes = "✓" if r.passes_mtm_filter else "✗"
    trades_ok = "✓" if r.has_300_trades else f"({r.n_trades})"
    print(f"\n#{i+1} {r.wallet}  [MTM:{passes} Trades:{trades_ok}]")
    print(f"   {r.best_mode} ({'nb='+str(int(r.best_nb)) if r.best_nb > 0 else '$12'}) | "
          f"Calmar={r.best_calmar:.1f} | PnL=${r.best_pnl:,.0f} | DD=${r.best_dd:,.0f} | "
          f"Win={r.best_win_rate:.0%} | Cap={r.best_capture:.0f}% | Comp={r.best_composite:.1f}")
    if r.fixed_calmar != r.best_calmar or r.best_mode != "fixed":
        print(f"   Fixed alt: Calmar={r.fixed_calmar:.1f} PnL=${r.fixed_pnl:,.0f} DD=${r.fixed_dd:,.0f} Cap={r.fixed_capture:.0f}%")

# ── Step 3: Coverage analysis ─────────────────────────────────────────
print(f"\n{'='*110}")
print("COVERAGE ANALYSIS")
print(f"{'='*110}")

top30 = results.head(30)
in_top30_mtm = top30.passes_mtm_filter.sum()
in_top30_trades = top30.has_300_trades.sum()
print(f"Top 30 wallets that PASS MTM filter: {in_top30_mtm}/30")
print(f"Top 30 wallets with 300+ trades: {in_top30_trades}/30")

# Wallets we're MISSING (good but filtered out)
missed = results[~results.passes_mtm_filter & (results.n_trades >= 200)].head(10)
if len(missed) > 0:
    print(f"\n--- POTENTIALLY MISSED WALLET (no MTM filter, 200+ trades) ---")
    for _, r in missed.iterrows():
        print(f"  {r.wallet} Calmar={r.best_calmar:.1f} PnL=${r.best_pnl:,.0f} "
              f"DD=${r.best_dd:,.0f} Trades={r.n_trades} Mode={r.best_mode}")

small_trade_good = results[results.n_trades.between(100, 299)].head(10)
if len(small_trade_good) > 0:
    print(f"\n--- HIGH PERFORMING BUT FEW TRADES (100-299) ---")
    for _, r in small_trade_good.iterrows():
        print(f"  {r.wallet} Calmar={r.best_calmar:.1f} PnL=${r.best_pnl:,.0f} "
              f"DD=${r.best_dd:,.0f} Trades={r.n_trades} Cap={r.best_capture:.0f}%")

results.to_csv(DATA / "wallet_bruteforce.csv", index=False)
print(f"\nFull results: {DATA / 'wallet_bruteforce.csv'}")
