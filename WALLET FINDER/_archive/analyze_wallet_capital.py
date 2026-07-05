"""Wallet Capital Requirements: find minimum norm_base per wallet for target capture%.

For each qualifying wallet:
  1) Sweep norm_base from 12 to 5000 in fine steps
  2) At each norm_base, calculate trade capture% and Calmar
  3) Find minimum norm_base for 50%, 70%, 90% capture thresholds
  4) Report capital requirements and associated risk metrics

Output tells you: "To copy wallet X with Y% trade capture, you need $Z per slot"
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
MIN_TRADES = 300
CAPTURE_TARGETS = [50, 70, 80, 90]  # % trade capture thresholds


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


def calc_metrics(equity: np.ndarray, pnl_per_trade: np.ndarray, n_total: int) -> dict:
    total = float(equity[-1]) if len(equity) else 0.0
    mdd = calc_max_dd(equity)
    calmar = total / abs(mdd) if mdd < -1 else (total / 1.0 if total > 0 else 0.0)
    wins = np.sum(pnl_per_trade > 0)
    losses = np.sum(pnl_per_trade < 0)
    win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0
    sortino = calc_sortino(pnl_per_trade)
    n_exec = int(wins + losses)
    return {
        "total_pnl": total,
        "max_dd": mdd,
        "calmar": calmar,
        "sortino": sortino,
        "win_rate": win_rate,
        "n_trades_executed": n_exec,
        "trade_capture_pct": n_exec / n_total * 100 if n_total > 0 else 0.0,
    }


def replay_proportional_fast(px, sz, closed, norm_base):
    """Fast proportional replay — returns metrics dict."""
    leader_n = np.abs(px * sz)
    scale = norm_base / LEADER_EQUITY_BASE
    scaled_n = leader_n * scale
    mask = scaled_n >= MIN_COPY_NOTIONAL
    n_total = len(leader_n)
    n_kept = int(mask.sum())
    if n_kept == 0:
        return {"total_pnl": 0, "max_dd": 0, "calmar": 0, "sortino": 0,
                "win_rate": 0, "n_trades_executed": 0, "trade_capture_pct": 0}
    pnl = closed[mask] * scale
    equity = np.cumsum(pnl)
    return calc_metrics(equity, pnl, n_total)


# ─── Load data ──────────────────────────────────────────────────────────
print("Loading data...")
cand = pd.read_csv(DATA / "copyable_wallets.csv")
cand["wallet"] = cand["wallet"].str.lower()
for c in ("mtm_calmar", "equity_collapse_flag_mtm", "negative_total_flag_mtm",
          "month_acctV_end", "month_pnl_chg_mtm", "max_drawdown_mtm", "trades"):
    cand[c] = pd.to_numeric(cand[c], errors="coerce")
cand = cand.rename(columns={"month_pnl_chg_mtm": "month_acctV_chg", "max_drawdown_mtm": "month_acctV_mdd_mtm"})

# MTM filter + min trades
mtm_pass = cand[
    (cand.mtm_calmar >= 1.5) &
    (cand.equity_collapse_flag_mtm == 0) &
    (cand.negative_total_flag_mtm == 0) &
    (cand.month_acctV_end >= 5000) &
    (cand.mtm_source != "unavailable") &
    (cand.trades >= MIN_TRADES)
].copy()
print(f"Qualifying wallets: {len(mtm_pass)}")

# Load trades
print("Loading all_trades.csv...")
t0 = time.time()
from trades_path import trades_csv
all_trades = pd.read_csv(trades_csv(),
                         usecols=["wallet", "time", "coin", "side", "px", "sz", "closedPnl"])
all_trades["wallet"] = all_trades["wallet"].str.lower()
print(f"Loaded {len(all_trades):,} trades in {time.time()-t0:.1f}s")

# ─── Per-wallet norm_base sweep ─────────────────────────────────────────
# Sweep norm_base from 12 to 5000 to find capture curve
NB_SWEEP = list(range(12, 201, 4)) + list(range(200, 1001, 25)) + list(range(1000, 5001, 100))
# Deduplicate and sort
NB_SWEEP = sorted(set(NB_SWEEP))

print(f"\nSweeping {len(mtm_pass)} wallets × {len(NB_SWEEP)} norm_base values...")
t0 = time.time()

rows = []
for n, cw in enumerate(mtm_pass.itertuples()):
    w = cw.wallet
    wt = all_trades[all_trades.wallet == w].sort_values("time").reset_index(drop=True)
    if len(wt) < MIN_TRADES:
        continue

    px = wt["px"].to_numpy(dtype=np.float64)
    sz = wt["sz"].to_numpy(dtype=np.float64)
    closed = wt["closedPnl"].to_numpy(dtype=np.float64)
    n_total = len(wt)

    # Leader trade size distribution
    leader_n = np.abs(px * sz)
    med_leader_n = float(np.median(leader_n))
    p25_leader_n = float(np.percentile(leader_n, 25))
    p75_leader_n = float(np.percentile(leader_n, 75))
    p90_leader_n = float(np.percentile(leader_n, 90))

    # Sweep: find capture% at each norm_base
    sweep_results = []
    for nb in NB_SWEEP:
        m = replay_proportional_fast(px, sz, closed, nb)
        sweep_results.append({
            "nb": nb,
            "capture": m["trade_capture_pct"],
            "calmar": m["calmar"],
            "pnl": m["total_pnl"],
            "max_dd": m["max_dd"],
            "win_rate": m["win_rate"],
            "n_exec": m["n_trades_executed"],
        })

    # Find minimum norm_base for each capture target
    min_nb_for_target = {}
    calmar_at_target = {}
    pnl_at_target = {}
    dd_at_target = {}
    for target in CAPTURE_TARGETS:
        # Find first nb where capture >= target
        hits = [s for s in sweep_results if s["capture"] >= target]
        if hits:
            best = hits[0]  # first (lowest nb) that meets target
            min_nb_for_target[target] = best["nb"]
            calmar_at_target[target] = best["calmar"]
            pnl_at_target[target] = best["pnl"]
            dd_at_target[target] = best["max_dd"]
        else:
            min_nb_for_target[target] = None
            calmar_at_target[target] = None
            pnl_at_target[target] = None
            dd_at_target[target] = None

    # Also get fixed $12 stats for reference
    leader_n_safe = np.where(leader_n > 0, leader_n, 1e-9)
    ratio = 12.0 / leader_n_safe
    fixed_pnl = closed * ratio
    fixed_equity = np.cumsum(fixed_pnl)
    fixed_m = calc_metrics(fixed_equity, fixed_pnl, n_total)

    row = {
        "wallet": w,
        "trades_total": n_total,
        "mtm_calmar": cw.mtm_calmar,
        "month_acctV_end": cw.month_acctV_end,
        "leader_median_fill": med_leader_n,
        "leader_p25_fill": p25_leader_n,
        "leader_p75_fill": p75_leader_n,
        "leader_p90_fill": p90_leader_n,
        "fixed_calmar": fixed_m["calmar"],
        "fixed_pnl": fixed_m["total_pnl"],
        "fixed_capture": 100.0,
    }
    for target in CAPTURE_TARGETS:
        row[f"min_nb_{target}pct"] = min_nb_for_target[target]
        row[f"calmar_at_{target}pct"] = calmar_at_target[target]
        row[f"pnl_at_{target}pct"] = pnl_at_target[target]
        row[f"dd_at_{target}pct"] = dd_at_target[target]

    rows.append(row)
    if (n + 1) % 10 == 0:
        print(f"  {n+1}/{len(mtm_pass)}  dt={time.time()-t0:.1f}s")

print(f"\nDone in {time.time()-t0:.1f}s — {len(rows)} wallets processed")

# ─── Output ─────────────────────────────────────────────────────────────
results = pd.DataFrame(rows)
results.to_csv(DATA / "wallet_capital_requirements.csv", index=False)

print(f"\n{'='*120}")
print("WALLET CAPITAL REQUIREMENTS")
print(f"{'='*120}")
print(f"{'Wallet':<44} {'Trades':>7} {'MedFill':>10} {'Fixed':>8}")
print(f"{'':44} {'':>7} {'':>10} {'Calmar':>8}", end="")
for t in CAPTURE_TARGETS:
    print(f"  {t}%capture", end="")
    print(f" → nb", end="")
    print(f"   Calmar", end="")
    print(f"     PnL", end="")
print()

print("-" * 120)
for _, r in results.iterrows():
    print(f"{r.wallet:<44} {r.trades_total:>7} ${r.leader_median_fill:>8,.0f} {r.fixed_calmar:>8.1f}")
    print(f"{'':44} {'':>7} {'':>10} {'':>8}", end="")
    for t in CAPTURE_TARGETS:
        nb = r[f"min_nb_{t}pct"]
        cal = r[f"calmar_at_{t}pct"]
        pnl = r[f"pnl_at_{t}pct"]
        dd = r[f"dd_at_{t}pct"]
        if nb is not None:
            print(f"  nb={nb:<5} → {cal:>7.1f}  ${pnl:>8,.0f}  dd=${dd:>6,.0f}", end="")
        else:
            print(f"  {'N/A':>5}   {'N/A':>7}  {'N/A':>8}  {'N/A':>8}", end="")
    print()
    print()

# ─── Summary: capital required at each capture tier ─────────────────────
print(f"\n{'='*120}")
print("SUMMARY: Capital Required (norm_base = $ per slot)")
print(f"{'='*120}")
print(f"norm_base is your copy position size. With PER_SLOT_MARGIN=$450:")
print(f"  - norm_base <= $450: fits in 1 slot")
print(f"  - norm_base $450-$900: needs 2 slots")
print(f"  - norm_base $900-$1350: needs 3 slots")
print()

for t in CAPTURE_TARGETS:
    col = f"min_nb_{t}pct"
    valid = results[results[col].notna()]
    if len(valid) == 0:
        continue
    vals = valid[col]
    print(f"\nAt {t}% trade capture:")
    print(f"  Wallets achievable: {len(valid)}/{len(results)}")
    print(f"  Min norm_base:  ${vals.min():>8,.0f}")
    print(f"  Median:         ${vals.median():>8,.0f}")
    print(f"  Max:            ${vals.max():>8,.0f}")
    # Slots needed
    slots = (vals / 450).apply(np.ceil).astype(int)
    print(f"  Slots needed (median): {slots.median():.0f}")
    print(f"  Slots needed (max):    {slots.max():.0f}")

print(f"\nFull results saved to: {DATA / 'wallet_capital_requirements.csv'}")
