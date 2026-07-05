import pandas as pd, numpy as np
DIR = r'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\data'
at = pd.read_csv(DIR + r'\all_trades.csv')

w = '0xae58db322be15464c1831b29540e42264cff2b23'
wt = at[at['wallet'] == w].sort_values('time')
print(f'0xae58 trades: {len(wt)}')
span_days = (wt.time.max() - wt.time.min()) / 86400000
print(f'Time span: {span_days:.1f} days')
print(f'Trades per day: {len(wt) / max(span_days, 1):.0f}')
print(f'Median gap between trades: {wt.time.diff().median() / 1000:.1f}s')

# Realistic concurrency: 15-min window
times = wt.time.values
max_15m = 0
max_5m = 0
for i in range(0, len(times), 100):  # sample every 100th trade
    in_15m = ((times >= times[i]) & (times <= times[i] + 900000)).sum()
    in_5m = ((times >= times[i]) & (times <= times[i] + 300000)).sum()
    if in_15m > max_15m: max_15m = in_15m
    if in_5m > max_5m: max_5m = in_5m

print(f'Max trades in 5-min window (sampled): {max_5m}')
print(f'Max trades in 15-min window (sampled): {max_15m}')

# For nb=860, what are the per-position margins?
coins = wt.coin.values
sn = (wt.px.abs() * wt.sz.abs() * 860 / 10000).values
LEV = {'BTC':40,'ETH':25,'SOL':20}
lev_arr = np.array([LEV.get(str(c).upper(), 10) for c in coins])
margins = sn / lev_arr
mask = sn >= 12
margins_active = margins[mask]
print(f'Positions above $12 notional: {mask.sum()} / {len(mask)}')
print(f'Average margin per active position: ${np.mean(margins_active):.1f}')
print(f'P95 margin per position: ${np.percentile(margins_active, 95):.1f}')
print(f'At 15 concurrent (15-min window): ${np.percentile(margins_active, 95) * 15:.0f}')
print(f'At 5 concurrent: ${np.percentile(margins_active, 95) * 5:.0f}')

# Also check the top 5 wallets for realistic concurrency
print("\n=== REALISTIC CONCURRENCY (top 10 wallets, 15-min window) ===")
best = pd.read_csv(DIR + r'\full_universe_best.csv')
for _, row in best.head(10).iterrows():
    wallet = row['wallet']
    wt2 = at[at['wallet'] == wallet].sort_values('time')
    if len(wt2) == 0: continue
    times2 = wt2.time.values
    # sample
    sample_idx = np.linspace(0, len(times2)-1, min(500, len(times2)), dtype=int)
    max_15m = 1
    for i in sample_idx:
        in_15m = ((times2 >= times2[i]) & (times2 <= times2[i] + 900000)).sum()
        if in_15m > max_15m:
            max_15m = in_15m
    model = row['model']
    nb = row.get('norm_base')
    print(f'  {wallet[:20]}... {model} nb={nb} max_15min={max_15m} trades={len(wt2)}')
