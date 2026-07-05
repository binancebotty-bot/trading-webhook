"""
FULL UNIVERSE SCAN — starts from raw all_trades.csv, not pre-filtered wallets.

1. Find every wallet in all_trades.csv with 100+ trades
2. For each wallet, find the MINIMUM norm_base that gives >= 70% trade capture
3. Simulate at that nb (proportional) AND fixed $12
4. If DD is still zero at 70%+ capture → flag as "insufficient loss data"
5. Rank by balanced composite: PnL_weighted + Calmar + Sortino + WinRate
6. Cross-reference with summary.csv for MTM data where available
"""
import pandas as pd
import numpy as np
import time, json, os, csv as _csv_mod
from pathlib import Path
from real_dd_filter import wallet_passes_real_dd as _wallet_passes_real_dd

DIR = Path(__file__).parent
DATA = DIR / "data"

MIN_TRADES = 100
MIN_COPY_NOTIONAL = 12
TARGET_CAPTURE = 0.70  # want at least 70% of trades captured

# ── Real DD pre-filter (opt-in via env var) ──────────────────────
_real_dd_pct_str = os.getenv("HL_FULL_UNIVERSE_MAX_REAL_DD_PCT")
HL_MAX_REAL_DD_PCT: float | None = float(_real_dd_pct_str) if _real_dd_pct_str else None

NB_GRID = list(range(12, 101)) + list(range(120, 1001, 20)) + list(range(1050, 5001, 50))
NB_GRID = sorted(set(NB_GRID))

def calc_equity_metrics(pnl_arr):
    if len(pnl_arr) == 0:
        return {"pnl": 0, "max_dd": 0, "win_rate": 0, "n": 0, "trades": 0}
    equity = np.cumsum(pnl_arr)
    rm = np.maximum.accumulate(equity)
    dd = equity - rm
    max_dd = dd.min()
    wins = (pnl_arr > 0).sum()
    losses = (pnl_arr < 0).sum()
    total = wins + losses
    return {
        "pnl": float(equity[-1]),
        "max_dd": float(max_dd),
        "win_rate": wins / total if total > 0 else 0,
        "n": int(total),
        "trades": int(len(pnl_arr)),
    }

def calc_sortino(pnl_arr):
    if len(pnl_arr) < 10:
        return 0
    neg = pnl_arr[pnl_arr < 0]
    if len(neg) == 0:
        return 100.0  # no downside
    downside_std = np.std(neg)
    if downside_std == 0:
        return 100.0
    return float(np.mean(pnl_arr) / downside_std * np.sqrt(252))

def simulate_fixed(trades_df):
    """Fixed $12: ratio = 12 / |px*sz|"""
    notional = (trades_df["px"].abs() * trades_df["sz"].abs()).values
    closed = trades_df["closedPnl"].values
    mask = notional > 1.0
    if mask.sum() == 0:
        return None
    safe_n = np.where(mask, notional, 1.0)
    ratio = np.where(mask, MIN_COPY_NOTIONAL / safe_n, 0)
    pnl = closed * ratio
    return pnl

def simulate_proportional(trades_df, norm_base):
    """Proportional: scale = norm_base / 10000, drop trades < $12 scaled"""
    notional = (trades_df["px"].abs() * trades_df["sz"].abs()).values
    scale = norm_base / 10000.0
    scaled_n = notional * scale
    mask = scaled_n >= MIN_COPY_NOTIONAL
    if mask.sum() == 0:
        return None, 0
    pnl = trades_df["closedPnl"].values[mask] * scale
    return pnl, mask.sum() / len(trades_df)

def score_wallet(m):
    """Balanced composite: PnL matters, not just risk-adjusted."""
    calmar = m.get("calmar", 0)
    sortino = m.get("sortino", 0)
    pnl = m.get("pnl", 0)
    win_rate = m.get("win_rate", 0)
    dd = m.get("max_dd", 0)
    cap = m.get("capture", 0)
    n = m.get("n", 0)

    # Calmar: capped at 50 to prevent artefactual dominance
    c = min(calmar, 50)
    # Sortino: capped at 20
    s = min(sortino, 20)
    # PnL score: logarithmic scale, $100=1, $1000=2, $10000=3, $100000=4
    p = np.log10(max(pnl, 1)) if pnl > 0 else 0
    # DD penalty: larger DD = worse, but logarithmic
    d = 1.0 / (1.0 + np.log10(1 + abs(dd)))
    # Win rate
    w = win_rate
    # Capture bonus: penalize < 70%
    cap_bonus = 1.0 if cap >= 0.70 else cap / 0.70
    # Trade count bonus: more trades = more confident
    trade_bonus = min(n / 500, 1.0)  # max bonus at 500+ trades

    score = (c * 0.25 + s * 0.15 + p * 0.35 + d * 10 * 0.1 + w * 5 * 0.15) * cap_bonus * trade_bonus
    return round(score, 2)

print("Loading data...")
t0 = time.time()
from trades_path import trades_csv
all_trades = pd.read_csv(trades_csv())
print(f"  Trades: {len(all_trades):,} rows, {all_trades['wallet'].nunique()} unique wallets ({time.time()-t0:.1f}s)")

# Load trade-based MTM (computed from all_trades.csv equity curve — complete coverage)
try:
    trade_mtm = pd.read_csv(DATA / "trade_based_mtm.csv")
    mtm_map = {}
    for _, row in trade_mtm.iterrows():
        w = row["wallet"]
        mtm_map[w] = {
            "mtm_calmar": row.get("mtm_calmar", np.nan),
            "equity_collapse_flag_mtm": row.get("equity_collapse_flag_mtm", 0),
            "negative_total_flag_mtm": row.get("negative_total_flag_mtm", 0),
            "month_acctV_end": row.get("month_acctV_end", np.nan),
            "max_drawdown_mtm": row.get("max_drawdown_mtm", np.nan),
            "allTime_max_drawdown_mtm": row.get("allTime_max_drawdown_mtm", np.nan),
            "raw_win_rate": row.get("raw_win_rate", np.nan),
            "n_months_active": row.get("n_months_active", 0),
        }
    print(f"  Trade-based MTM loaded: {len(mtm_map)} wallets (100% coverage)")
except Exception as e:
    mtm_map = {}
    print(f"  No trade_based_mtm.csv found: {e}")

# Filter to wallets with enough trades
wallet_trade_counts = all_trades.groupby("wallet").size()
qualifying_wallets = wallet_trade_counts[wallet_trade_counts >= MIN_TRADES].index.tolist()
print(f"\nWallets with {MIN_TRADES}+ trades: {len(qualifying_wallets)}")

# Also check copyable wallets
try:
    copyable = pd.read_csv(DATA / "copyable_wallets.csv")
    copyable_set = set(copyable["wallet"].values)
    in_copyable = sum(1 for w in qualifying_wallets if w in copyable_set)
    not_in_copyable = [w for w in qualifying_wallets if w not in copyable_set]
    print(f"  In copyable_wallets.csv: {in_copyable}")
    print(f"  NOT in copyable_wallets.csv: {len(not_in_copyable)}")
except:
    copyable_set = set()

print(f"\nSimulating {len(qualifying_wallets)} wallets across fixed + proportional grid...")

# ── Real DD pre-filter (before expensive simulation) ───────────
if HL_MAX_REAL_DD_PCT is not None:
    print(f"\nReal DD pre-filter enabled: {HL_MAX_REAL_DD_PCT}% threshold")
    prefilter_rows = []
    passing_wallets = []
    for w in qualifying_wallets:
        result = _wallet_passes_real_dd(w, HL_MAX_REAL_DD_PCT)
        prefilter_rows.append(result)
        if result["dd_gate_pass"]:
            passing_wallets.append(w)
    # Write audit CSV
    audit_path = DATA / "full_universe_real_dd_prefilter.csv"
    if prefilter_rows:
        fieldnames = list(prefilter_rows[0].keys())
        with open(audit_path, "w", newline="", encoding="utf-8") as f:
            writer = _csv_mod.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(prefilter_rows)
        print(f"  Pre-filter audit: {audit_path} ({len(prefilter_rows)} wallets)")
    rejected = len(qualifying_wallets) - len(passing_wallets)
    print(f"  Real DD pre-filter: {len(passing_wallets)}/{len(qualifying_wallets)} passed ({rejected} rejected)")
    qualifying_wallets = passing_wallets
else:
    print(f"  Real DD pre-filter: disabled (set HL_FULL_UNIVERSE_MAX_REAL_DD_PCT to enable)")

t0 = time.time()
results = []

for i, wallet in enumerate(qualifying_wallets):
    if (i + 1) % 25 == 0:
        dt = time.time() - t0
        print(f"  {i+1}/{len(qualifying_wallets)}  dt={dt:.1f}s")

    wt = all_trades[all_trades["wallet"] == wallet].copy()
    wt = wt.sort_values("time").reset_index(drop=True)
    total_trades = len(wt)

    # --- Fixed $12 ---
    pnl_fixed = simulate_fixed(wt)
    if pnl_fixed is not None and len(pnl_fixed) > 10:
        m = calc_equity_metrics(pnl_fixed)
        m["sortino"] = calc_sortino(pnl_fixed)
        m["calmar"] = abs(m["pnl"] / m["max_dd"]) if m["max_dd"] < -1 else 0
        m["capture"] = m["n"] / total_trades
        m["total_trades"] = total_trades
        m["model"] = "fixed"
        m["norm_base"] = None
        m["score"] = score_wallet(m)
        results.append({"wallet": wallet, **m})

    # --- Proportional sweep: find minimum nb for target capture ---
    best_prop = None
    for nb in NB_GRID:
        pnl_prop, capture = simulate_proportional(wt, nb)
        if pnl_prop is None or len(pnl_prop) < 10:
            continue
        if capture >= TARGET_CAPTURE:
            m = calc_equity_metrics(pnl_prop)
            m["sortino"] = calc_sortino(pnl_prop)
            m["calmar"] = abs(m["pnl"] / m["max_dd"]) if m["max_dd"] < -1 else 0
            m["capture"] = capture
            m["total_trades"] = total_trades
            m["model"] = "proportional"
            m["norm_base"] = nb
            m["score"] = score_wallet(m)
            best_prop = {"wallet": wallet, **m}
            break  # first nb that hits target

    if best_prop:
        results.append(best_prop)
    else:
        # Try max nb even if below target
        pnl_prop, capture = simulate_proportional(wt, NB_GRID[-1])
        if pnl_prop is not None and len(pnl_prop) > 10:
            m = calc_equity_metrics(pnl_prop)
            m["sortino"] = calc_sortino(pnl_prop)
            m["calmar"] = abs(m["pnl"] / m["max_dd"]) if m["max_dd"] < -1 else 0
            m["capture"] = capture
            m["total_trades"] = total_trades
            m["model"] = "proportional"
            m["norm_base"] = NB_GRID[-1]
            m["score"] = score_wallet(m)
            results.append({"wallet": wallet, **m})

print(f"\nSimulation done in {time.time()-t0:.1f}s — {len(results)} configs")

df = pd.DataFrame(results)

# Add trade-based MTM data
df["mtm_calmar"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("mtm_calmar", np.nan))
df["equity_collapse"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("equity_collapse_flag_mtm", 0))
df["negative_total"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("negative_total_flag_mtm", 0))
df["month_acctV_end"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("month_acctV_end", np.nan))
df["max_dd_mtm"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("max_drawdown_mtm", np.nan))
df["alltime_dd_mtm"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("allTime_max_drawdown_mtm", np.nan))
df["raw_win_rate"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("raw_win_rate", np.nan))
df["n_months"] = df["wallet"].map(lambda w: mtm_map.get(w, {}).get("n_months_active", 0))
df["in_copyable"] = df["wallet"].map(lambda w: w in copyable_set)

# Trade-based MTM filter pass
df["mtm_pass"] = (
    (df["mtm_calmar"].fillna(0) >= 1.5) &
    (df["equity_collapse"] == 0) &
    (df["negative_total"] == 0) &
    (df["month_acctV_end"].fillna(0) >= 5000)
)

# Per wallet: pick best model
best_per_wallet = df.sort_values("score", ascending=False).groupby("wallet").first().reset_index()
best_per_wallet = best_per_wallet.sort_values("score", ascending=False)

# Save
df.to_csv(DATA / "full_universe_results.csv", index=False)
best_per_wallet.to_csv(DATA / "full_universe_best.csv", index=False)

# --- Report ---
print("\n" + "=" * 120)
print("TOP 30 WALLETS — FULL UNIVERSE (best model per wallet)")
print("=" * 120)
print(f"{'#':<4} {'Wallet':<44} {'MTM':<5} {'Model':<14} {'nb':<6} {'Calmar':<10} {'PnL':<12} {'DD':<10} {'Win%':<6} {'Cap%':<6} {'#Trd':<6} {'Score':<8}")
print("-" * 120)

for i, (_, r) in enumerate(best_per_wallet.head(30).iterrows()):
    mtm_flag = "✓" if r.get("mtm_pass", False) else "✗"
    nb_str = str(int(r["norm_base"])) if r.get("norm_base") else "-"
    model_str = f"{r['model'][:5]}({nb_str})" if r["model"] == "proportional" else "fixed"
    dd_flag = " ⚠DD=0" if r["max_dd"] == 0 else ""
    calmar_trade = r.get("mtm_calmar", np.nan)
    calmar_str = f"{calmar_trade:.1f}" if pd.notna(calmar_trade) else "N/A"
    acctV = r.get("month_acctV_end", np.nan)
    acctV_str = f"${acctV:,.0f}" if pd.notna(acctV) else "N/A"
    print(f"#{i+1:<3} {r['wallet'][:42]:<44} {mtm_flag:<5} {model_str:<14} {r.get('calmar',0):<10.1f} ${r.get('pnl',0):>9,.0f} ${r.get('max_dd',0):>8,.0f} {r.get('win_rate',0)*100:>5.1f}% {r.get('capture',0)*100:>5.1f}% {r.get('total_trades',0):>5} {r.get('score',0):>7.1f}{dd_flag}  TCalmar={calmar_str} AcctV={acctV_str}")

# --- Quality checks ---
print("\n" + "=" * 120)
print("QUALITY CHECKS")
print("=" * 120)

zero_dd = best_per_wallet[best_per_wallet["max_dd"] == 0]
print(f"Zero-DD wallets in top 30: {len(zero_dd[zero_dd.index < 30])}")
if len(zero_dd) > 0:
    print("  These wallets have no drawdown in the simulation. Check if:")
    print("  (a) All losing trades were filtered by $12 min cutoff → increase nb")
    print("  (b) Wallet genuinely has no losses → verify in raw data")
    for _, z in zero_dd.head(5).iterrows():
        nb_str = f"nb={int(z['norm_base'])}" if z.get("norm_base") else "fixed"
        print(f"  {z['wallet'][:20]}... {nb_str} cap={z['capture']*100:.0f}% n={z['n']} pnl=${z['pnl']:,.0f}")

# Check raw loss rates
print("\nRaw loss analysis (top 30 wallets):")
for _, r in best_per_wallet.head(30).iterrows():
    wt = all_trades[all_trades["wallet"] == r["wallet"]]
    raw_losses = (wt["closedPnl"] < 0).sum()
    raw_wins = (wt["closedPnl"] > 0).sum()
    raw_total = raw_wins + raw_losses
    raw_loss_rate = raw_losses / raw_total if raw_total > 0 else 0
    nb_str = f"nb={int(r['norm_base'])}" if r.get("norm_base") else "fixed"
    print(f"  {r['wallet'][:20]}... raw: {raw_wins}W/{raw_losses}L ({raw_loss_rate*100:.1f}% loss) | sim: {r['win_rate']*100:.1f}% win, cap={r['capture']*100:.0f}% | {nb_str}")

# Summary stats
print(f"\nSummary:")
print(f"  Total wallets scanned: {len(qualifying_wallets)}")
print(f"  Total configs simulated: {len(results)}")
print(f"  Wallets with trade-based MTM: {df['mtm_calmar'].notna().sum()}/{len(df)} (100%)")
print(f"  Trade-based MTM pass: {df['mtm_pass'].sum()}")
print(f"  Proportional picks: {(best_per_wallet['model']=='proportional').sum()}")
print(f"  Fixed picks: {(best_per_wallet['model']=='fixed').sum()}")
print(f"  Zero-DD wallets: {len(zero_dd)}")
print(f"  In copyable set: {best_per_wallet['in_copyable'].sum()}/{len(best_per_wallet)}")

# Trade-based MTM filter analysis
print(f"\nTrade-based MTM filter breakdown (top 30):")
for _, r in best_per_wallet.head(30).iterrows():
    fails = []
    mc = r.get("mtm_calmar")
    if pd.isna(mc) or mc < 1.5: fails.append(f"calmar={mc}")
    if r.get("equity_collapse", 0) == 1: fails.append("collapse=1")
    if r.get("negative_total", 0) == 1: fails.append("negative=1")
    if pd.isna(r.get("month_acctV_end")) or r.get("month_acctV_end", 0) < 5000: fails.append(f"acctV={r.get('month_acctV_end',0)}")
    status = "PASS" if len(fails)==0 else "FAIL: " + " | ".join(fails)
    print(f"  {r['wallet'][:20]}... {status}")

print(f"\nSaved: data/full_universe_results.csv, data/full_universe_best.csv")
