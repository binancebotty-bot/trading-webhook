"""
MINIMUM BANKROLL CALCULATOR — v2 (fast)
Compute the minimum capital needed to safely copy each wallet.

Components:
  1. Max Drawdown buffer: absorb worst historical loss
  2. Position margin: notional / leverage per open position
  3. Peak concurrency: worst-case simultaneous positions (sampled)
  4. Safety buffer: 25% on DD + fees
"""
import pandas as pd
import numpy as np
import time
from pathlib import Path

DIR = Path(__file__).parent
DATA = DIR / "data"

LEV_MAP = {
    "BTC": 40, "ETH": 25,
    "SOL": 20, "XRP": 20, "BNB": 10, "AVAX": 20, "MATIC": 20, "LTC": 20,
    "ARB": 20, "OP": 20, "APT": 20, "ATOM": 20, "DOGE": 20, "LINK": 20,
    "DOT": 20, "TRX": 20, "BCH": 20, "FIL": 20, "NEAR": 20, "INJ": 20,
    "SUI": 20, "TIA": 20, "JUP": 20, "AAVE": 20, "ADA": 20, "TON": 20,
    "HYPE": 10, "PENDLE": 10, "FARTCOIN": 5, "KPEPE": 5, "WIF": 5,
}
DEFAULT_LEV = 10
MIN_COPY_NOTIONAL = 12

def get_leverage(coin):
    return LEV_MAP.get(str(coin).upper(), DEFAULT_LEV)

def compute_bankroll(trades_df, model, norm_base=None):
    """Compute minimum bankroll for a wallet + model combination."""
    df = trades_df.sort_values("time").reset_index(drop=True)
    notional = (df["px"].abs() * df["sz"].abs()).values
    closed = df["closedPnl"].values
    coins = df["coin"].values.astype(str)
    times = df["time"].values.astype(np.int64)

    if model == "fixed":
        mask = notional > 1.0
        safe_n = np.where(mask, notional, 1.0)
        ratio = np.where(mask, MIN_COPY_NOTIONAL / safe_n, 0)
        scaled_n = np.where(mask, MIN_COPY_NOTIONAL, 0)
        pnl_all = closed * ratio
    elif model == "proportional" and norm_base:
        scale = norm_base / 10000.0
        scaled_n_full = notional * scale
        mask = scaled_n_full >= MIN_COPY_NOTIONAL
        scaled_n = scaled_n_full[mask]
        pnl_all = closed[mask] * scale
    else:
        return None

    if mask.sum() < 10:
        return None

    pnl = pnl_all  # already filtered
    sn = scaled_n if model == "proportional" else scaled_n[mask]
    coin_arr = coins[mask]
    time_arr = times[mask]
    lev_arr = np.array([get_leverage(c) for c in coin_arr])

    # --- 1. Max Drawdown ---
    equity = np.cumsum(pnl)
    rm = np.maximum.accumulate(equity)
    dd = equity - rm
    max_dd = abs(float(dd.min()))

    # --- 2. Position margins ---
    pos_margins = sn / lev_arr
    avg_pos_margin = float(np.mean(pos_margins))

    # --- 3. Peak concurrency (fast O(n log n) sweep) ---
    # Sort by open time, simulate position opens/closes
    # Approximate: positions last ~1 hour on average
    sort_idx = np.argsort(time_arr)
    t_sorted = time_arr[sort_idx]
    c_sorted = coin_arr[sort_idx]
    n_sorted = sn[sort_idx]
    m_sorted = pos_margins[sort_idx]

    WINDOW_MS = 3600 * 1000  # 1 hour estimated hold
    peak_concurrent = 0
    peak_margin = 0
    peak_ts = 0
    
    # Sliding window: for each trade, count how many are still open 1hr later
    j = 0
    active_margin = 0
    active_count = 0
    for i in range(len(t_sorted)):
        # Remove trades that have "closed" (exceeded window)
        while j < i and t_sorted[j] < t_sorted[i] - WINDOW_MS:
            active_margin -= m_sorted[j]
            active_count -= 1
            j += 1
        # Add current trade
        active_margin += m_sorted[i]
        active_count += 1
        if active_count > peak_concurrent:
            peak_concurrent = active_count
            peak_margin = active_margin
            peak_ts = t_sorted[i]

    peak_concurrent = max(peak_concurrent, 1)

    # --- 4. Bankroll calculation ---
    dd_buffer = max_dd * 1.25  # 25% tail risk buffer
    margin_buffer = peak_margin * 1.20  # 20% slippage/fee buffer
    total_notional = float(np.sum(sn))
    fee_buffer = total_notional * 0.0005  # 5bps round-trip estimate

    bankroll = max(dd_buffer, margin_buffer) + fee_buffer + 50  # $50 min reserve

    # --- 5. Win/loss stats ---
    wins = (pnl > 0).sum()
    losses = (pnl < 0).sum()
    total = wins + losses
    win_rate = wins / total if total > 0 else 0
    avg_win = float(np.mean(pnl[pnl > 0])) if wins > 0 else 0
    avg_loss = float(np.mean(pnl[pnl < 0])) if losses > 0 else 0
    profit_factor = abs(avg_win * wins / (avg_loss * losses)) if losses > 0 and avg_loss != 0 else 999

    return {
        "max_dd": round(max_dd, 2),
        "total_pnl": round(float(equity[-1]), 2),
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "peak_concurrent": peak_concurrent,
        "avg_pos_margin": round(avg_pos_margin, 2),
        "peak_margin": round(peak_margin, 2),
        "dd_buffer": round(dd_buffer, 2),
        "margin_buffer": round(margin_buffer, 2),
        "fee_buffer": round(fee_buffer, 2),
        "bankroll": round(bankroll, 2),
    }


# Load data
print("Loading data...")
t0 = time.time()
from trades_path import trades_csv
all_trades = pd.read_csv(trades_csv())
best = pd.read_csv(DATA / "full_universe_best.csv")
print(f"  {len(all_trades):,} trades, {len(best)} top wallets ({time.time()-t0:.1f}s)")

# Compute bankroll for each top wallet
print("\nComputing bankroll requirements for top 30...")
t0 = time.time()
results = []
for _, row in best.head(30).iterrows():
    wallet = row["wallet"]
    model = row["model"]
    nb = row.get("norm_base")

    wt = all_trades[all_trades["wallet"] == wallet].copy()

    br = compute_bankroll(wt, model, nb)
    if br:
        results.append({
            "wallet": wallet,
            "model": model,
            "norm_base": nb,
            "sim_pnl": row.get("pnl", 0),
            "sim_dd": row.get("max_dd", 0),
            "sim_capture": row.get("capture", 0),
            "sim_trades": row.get("total_trades", 0),
            **br,
        })

print(f"Done in {time.time()-t0:.1f}s")
df = pd.DataFrame(results)
df.to_csv(DATA / "wallet_bankroll_requirements.csv", index=False)

# --- Print report ---
print("\n" + "=" * 150)
print("MINIMUM BANKROLL REQUIREMENTS — TOP 30 WALLETS")
print("=" * 150)
header = f"{'#':<3} {'Wallet':<20} {'Model':<16} {'PnL':>10} {'MaxDD':>10} {'Win%':>6} {'PF':>6} {'Concur':>7} {'PeakMarg':>10} {'BANKROLL':>10} {'DD/BR':>6}"
print(header)
print("-" * 150)

for i, r in df.iterrows():
    nb_str = f"nb{int(r['norm_base'])}" if pd.notna(r.get("norm_base")) else "fixed"
    model_str = f"prop({nb_str})" if r["model"] == "proportional" else "fixed"
    dd_ratio = r["max_dd"] / max(r["bankroll"], 1)
    print(f"#{i+1:<2} {r['wallet'][:18]:<20} {model_str:<16} ${r['sim_pnl']:>8,.0f} ${r['max_dd']:>8,.0f} {r['win_rate']*100:>5.1f}% {r['profit_factor']:>5.1f} {r['peak_concurrent']:>5}pos ${r['peak_margin']:>8,.1f} ${r['bankroll']:>8,.1f} {dd_ratio:>5.1%}")

# --- Portfolio summary ---
print("\n" + "=" * 150)
print("PORTFOLIO-LEVEL BANKROLL (Top 10)")
print("=" * 150)
top10 = df.head(10)
total_bankroll = top10["bankroll"].sum()
total_pnl = top10["sim_pnl"].sum()
total_dd = top10["max_dd"].sum()
total_margin = top10["peak_margin"].sum()

print(f"  Total bankroll needed:       ${total_bankroll:>10,.1f}")
print(f"  Total sim PnL:               ${total_pnl:>10,.1f}")
print(f"  Sum of max DDs:              ${total_dd:>10,.1f}")
print(f"  Sum of peak margins:         ${total_margin:>10,.1f}")
print(f"  PnL / Bankroll:              {total_pnl/total_bankroll:>10.2f}x")
print(f"  Monthly return estimate:     {total_pnl/total_bankroll/3*100:>10.1f}% (3-month data)")
print(f"\n  Conservative (1.5x):         ${total_bankroll * 1.5:>10,.1f}")
print(f"  Very conservative (2x):      ${total_bankroll * 2.0:>10,.1f}")

print(f"\n  Per-wallet stats:")
print(f"    Min bankroll:   ${df['bankroll'].min():>10,.1f}")
print(f"    Max bankroll:   ${df['bankroll'].max():>10,.1f}")
print(f"    Avg bankroll:   ${df['bankroll'].mean():>10,.1f}")
print(f"    Median:         ${df['bankroll'].median():>10,.1f}")

# --- Safety rating ---
print(f"\n{'=' * 150}")
print("SAFETY ASSESSMENT")
print("=" * 150)
for i, r in df.head(10).iterrows():
    # Buffer ratio: how many times can the DD eat into the bankroll
    buffer_ratio = r["bankroll"] / max(r["max_dd"], 1)
    # Profit factor > 2 = very profitable
    # Win rate > 70% = consistent
    # DD < 5% of bankroll = safe
    dd_pct = r["max_dd"] / max(r["bankroll"], 1) * 100
    
    if buffer_ratio > 3 and r["profit_factor"] > 2:
        rating = "★★★ SAFE"
    elif buffer_ratio > 2 and r["profit_factor"] > 1.5:
        rating = "★★  MODERATE"
    elif buffer_ratio > 1.5:
        rating = "★   TIGHT"
    else:
        rating = "⚠   RISKY"
    
    print(f"  {r['wallet'][:18]}... ${r['bankroll']:>7,.0f} bankroll | DD eats {dd_pct:.0f}% | PF={r['profit_factor']:.1f} | {rating}")

# --- Tiered recommendations ---
print(f"\n{'=' * 150}")
print("TIERED RECOMMENDATIONS")
print("=" * 150)

tiers = [
    ("TIER 1 — Micro (1-2 wallets)", df.head(2)),
    ("TIER 2 — Small (3-5 wallets)", df.head(5)),
    ("TIER 3 — Full (10 wallets)", df.head(10)),
]

for name, subset in tiers:
    total = subset["bankroll"].sum()
    pnl = subset["sim_pnl"].sum()
    print(f"\n  {name}:")
    print(f"    Bankroll: ${total:,.0f} | Expected PnL: ${pnl:,.0f} | Return: {pnl/total:.1f}x over period")
    for _, r in subset.iterrows():
        nb_str = f"nb{int(r['norm_base'])}" if pd.notna(r.get("norm_base")) else "fixed"
        print(f"      {r['wallet'][:20]} ${r['bankroll']:>7,.0f} ({r['model'][:4]} {nb_str})")

print(f"\nSaved: data/wallet_bankroll_requirements.csv")
