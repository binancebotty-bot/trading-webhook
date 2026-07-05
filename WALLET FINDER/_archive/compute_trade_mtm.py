"""
TRADE-BASED MTM COMPUTATION
Compute full MTM metrics from all_trades.csv equity curve for ALL wallets.
Replaces the API-dependent MTM data that's missing for 48% of wallets.

MTM fields computed (matching hl_mtm_lookup.py schema):
  - max_drawdown_mtm: monthly max drawdown from equity curve
  - month_pnl_chg_mtm: monthly PnL change
  - month_acctV_end: month-end account value (cumsum at month boundary)
  - month_acctV_peak: peak account value within month
  - mtm_calmar: calmar ratio = month_pnl_chg / abs(max_drawdown)
  - allTime_max_drawdown_mtm: all-time max drawdown
  - allTime_pnl_chg_mtm: all-time total PnL
  - allTime_acctV_peak: all-time peak equity
  - equity_collapse_flag_mtm: 1 if peak > 0 and end < 50% of peak
  - negative_total_flag_mtm: 1 if month PnL change < 0
  - mtm_source: "trade_replay" (computed from closedPnl, not API)

Also produces per-wallet stats:
  - full equity curve, drawdown series
  - win rate, trade count, notional stats
"""
import pandas as pd
import numpy as np
import time
import json
from pathlib import Path

DIR = Path(__file__).parent
DATA = DIR / "data"

MIN_TRADES = 50

def compute_trade_mtm(trades_df):
    """Compute MTM-equivalent stats from a single wallet's trade history.
    
    trades_df must have columns: time, px, sz, closedPnl, coin, side
    Returns dict with all MTM fields.
    """
    if len(trades_df) == 0:
        return None
    
    # Sort by time
    df = trades_df.sort_values("time").reset_index(drop=True)
    
    # Equity curve from cumulative closedPnl (starting from 0)
    pnl = df["closedPnl"].values
    equity = np.cumsum(pnl)
    
    # Notional for size analysis
    notional = (df["px"].abs() * df["sz"].abs()).values
    
    # All-time stats
    alltime_pnl = float(equity[-1])
    alltime_peak = float(np.max(equity)) if len(equity) > 0 else 0
    alltime_vlm = float(np.sum(notional))
    
    # All-time max drawdown
    running_max = np.maximum.accumulate(equity)
    dd = equity - running_max
    alltime_mdd = float(np.min(dd)) if len(dd) > 0 else 0
    
    # Monthly stats — group by calendar month
    # Convert timestamp (ms) to datetime
    times = pd.to_datetime(df["time"], unit="ms")
    months = times.dt.to_period("M")
    
    monthly_data = {}
    for month_key, idx in months.groupby(months).groups.items():
        month_pnl = pnl[idx]
        month_equity = equity[idx]
        month_notional = notional[idx]
        
        # Account value at start of month = equity before first trade of month
        # (i.e., cumsum up to but not including this month's first trade)
        if idx[0] > 0:
            acctV_start = float(equity[idx[0] - 1])
        else:
            acctV_start = 0.0  # wallet started from 0
        
        acctV_end = float(equity[idx[-1]])
        acctV_peak = float(np.max(month_equity))
        month_chg = acctV_end - acctV_start
        
        # Monthly max drawdown
        running_max_m = np.maximum.accumulate(month_equity)
        dd_m = month_equity - running_max_m
        month_mdd = float(np.min(dd_m)) if len(dd_m) > 0 else 0
        
        monthly_data[str(month_key)] = {
            "start": acctV_start,
            "end": acctV_end,
            "peak": acctV_peak,
            "chg": month_chg,
            "mdd": month_mdd,
            "n_trades": len(idx),
            "pnl": float(np.sum(month_pnl)),
            "notional": float(np.sum(month_notional)),
        }
    
    # Use the MOST RECENT complete month for "monthly" stats
    # (or the month with the most trades)
    if monthly_data:
        # Get last month
        sorted_months = sorted(monthly_data.keys())
        last_month = monthly_data[sorted_months[-1]]
        
        m_start = last_month["start"]
        m_end = last_month["end"]
        m_peak = last_month["peak"]
        m_mdd = last_month["mdd"]
        month_chg = last_month["chg"]
        
        # If last month is very short (< 5 trades), use the month before
        if last_month["n_trades"] < 5 and len(sorted_months) > 1:
            prev_month = monthly_data[sorted_months[-2]]
            m_start = prev_month["start"]
            m_end = prev_month["end"]
            m_peak = prev_month["peak"]
            m_mdd = prev_month["mdd"]
            month_chg = prev_month["chg"]
    else:
        m_start = 0
        m_end = alltime_pnl
        m_peak = alltime_peak
        m_mdd = alltime_mdd
        month_chg = alltime_pnl
    
    # Calmar = monthly PnL change / |monthly max drawdown|
    calmar = round(month_chg / abs(m_mdd), 3) if abs(m_mdd) > 1.0 else None
    
    # Flags (matching hl_mtm_lookup.py logic)
    equity_collapse_flag = int(m_peak > 0 and m_end < 0.5 * m_peak)
    negative_total_flag = int(month_chg < 0)
    
    # Win/loss stats
    wins = (pnl > 0).sum()
    losses = (pnl < 0).sum()
    total_trades = wins + losses
    
    return {
        # MTM fields (matching API schema)
        "max_drawdown_mtm": round(m_mdd, 4),
        "month_pnl_chg_mtm": round(month_chg, 4),
        "month_acctV_end": round(m_end, 4),
        "month_acctV_peak": round(m_peak, 4),
        "mtm_calmar": calmar,
        "allTime_max_drawdown_mtm": round(alltime_mdd, 4),
        "allTime_pnl_chg_mtm": round(alltime_pnl, 4),
        "allTime_acctV_peak": round(alltime_peak, 4),
        "allTime_vlm": round(alltime_vlm, 4),
        "equity_collapse_flag_mtm": equity_collapse_flag,
        "negative_total_flag_mtm": negative_total_flag,
        "mtm_source": "trade_replay",
        "mtm_fetched_at": int(time.time()),
        
        # Additional trade-based stats
        "total_trades_raw": int(len(df)),
        "total_wins": int(wins),
        "total_losses": int(losses),
        "raw_win_rate": round(wins / total_trades, 4) if total_trades > 0 else 0,
        "raw_avg_notional": round(float(np.mean(notional)), 2),
        "raw_median_notional": round(float(np.median(notional)), 2),
        "raw_total_pnl": round(alltime_pnl, 4),
        "raw_max_dd": round(alltime_mdd, 4),
        "n_months_active": len(monthly_data),
        "first_trade_time": int(df["time"].iloc[0]),
        "last_trade_time": int(df["time"].iloc[-1]),
    }


print("Loading all_trades.csv...")
t0 = time.time()
from trades_path import trades_csv
all_trades = pd.read_csv(trades_csv())
print(f"  {len(all_trades):,} rows, {all_trades['wallet'].nunique()} wallets ({time.time()-t0:.1f}s)")

# Filter to wallets with enough trades
wallet_counts = all_trades.groupby("wallet").size()
qualifying = wallet_counts[wallet_counts >= MIN_TRADES].index.tolist()
print(f"\nWallets with {MIN_TRADES}+ trades: {len(qualifying)}")

# Compute MTM for all qualifying wallets
print(f"Computing trade-based MTM for {len(qualifying)} wallets...")
t0 = time.time()
results = []
for i, wallet in enumerate(qualifying):
    if (i + 1) % 100 == 0:
        dt = time.time() - t0
        print(f"  {i+1}/{len(qualifying)}  dt={dt:.1f}s")
    wt = all_trades[all_trades["wallet"] == wallet]
    mtm = compute_trade_mtm(wt)
    if mtm:
        mtm["wallet"] = wallet
        results.append(mtm)

print(f"Done in {time.time()-t0:.1f}s — {len(results)} wallets")

df = pd.DataFrame(results)

# Save trade-based MTM
df.to_csv(DATA / "trade_based_mtm.csv", index=False)

# Compare with API-based MTM where available
try:
    api_summary = pd.read_csv(DATA / "summary.csv")
    api_mtm = api_summary[api_summary["mtm_source"].isin(["hl_portfolio_api", "cache_stale"])]
    
    print(f"\n=== COMPARISON: Trade-based vs API MTM (where both exist) ===")
    merged = df.merge(api_mtm[["wallet", "mtm_calmar", "max_drawdown_mtm", "equity_collapse_flag_mtm", 
                                "negative_total_flag_mtm", "month_acctV_end", "mtm_source"]], 
                       on="wallet", suffixes=("_trade", "_api"), how="inner")
    
    print(f"  Wallets with both: {len(merged)}")
    
    # Calmar correlation
    valid = merged.dropna(subset=["mtm_calmar_trade", "mtm_calmar_api"])
    if len(valid) > 0:
        corr = valid["mtm_calmar_trade"].corr(valid["mtm_calmar_api"])
        print(f"  Calmar correlation: {corr:.3f}")
    
    # Show a few examples
    print(f"\n  Sample comparisons (first 10):")
    for _, r in merged.head(10).iterrows():
        print(f"  {r['wallet'][:20]}... trade_calmar={r['mtm_calmar_trade']:.3f} api_calmar={r['mtm_calmar_api']:.3f} | "
              f"trade_collapse={r['equity_collapse_flag_mtm_trade']} api_collapse={r['equity_collapse_flag_mtm_api']}")
except:
    print("  No summary.csv to compare against")

# Summary stats
print(f"\n=== TRADE-BASED MTM SUMMARY ===")
print(f"  Total wallets: {len(df)}")
print(f"  MTM calmar >= 1.5: {(df['mtm_calmar'].dropna() >= 1.5).sum()}")
print(f"  MTM calmar >= 1.0: {(df['mtm_calmar'].dropna() >= 1.0).sum()}")
print(f"  MTM calmar >= 0.5: {(df['mtm_calmar'].dropna() >= 0.5).sum()}")
print(f"  MTM calmar NaN: {df['mtm_calmar'].isna().sum()}")
print(f"  equity_collapse_flag=1: {(df['equity_collapse_flag_mtm'] == 1).sum()}")
print(f"  negative_total_flag=1: {(df['negative_total_flag_mtm'] == 1).sum()}")
print(f"  month_acctV_end >= 5000: {(df['month_acctV_end'] >= 5000).sum()}")
print(f"  month_acctV_end < 5000: {(df['month_acctV_end'] < 5000).sum()}")

# MTM filter pass rate
passed = df[
    (df["mtm_calmar"].fillna(0) >= 1.5) & 
    (df["equity_collapse_flag_mtm"] == 0) & 
    (df["negative_total_flag_mtm"] == 0) &
    (df["month_acctV_end"] >= 5000)
]
print(f"\n  Pass MTM filter (calmar>=1.5, no collapse, no negative, acctV>=5000): {len(passed)}")
print(f"\nSaved: data/trade_based_mtm.csv")
