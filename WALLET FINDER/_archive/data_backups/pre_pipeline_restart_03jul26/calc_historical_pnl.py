import pandas as pd
import json
import numpy as np
from pathlib import Path

wallets = [
    ('0xac2ce04019a700eeda9b97a36a2c3b259ff6b0cd', 789),
    ('0x9f3e77cb89df964003053aa5b438e5697c77f4f9', 626),
    ('0x96f0cd31e972e86a77921e27bf06cf7ded45cc8a', 359),
    ('0xd47935ce22e71b37f20d30617afa998a6c89887c', 342),
    ('0xb6bfa5b79c1f06d5cf21d21e00f25d8da9fcfdb1', 340),
    ('0x22439c28f279979d86f9851dfb37e7b279ac4f7b', 143),
    ('0x1c6ad8b5b9de20ff6c3193ed96cb6a6540533146', 121),
    ('0x0d7ec7b730308983cde9e2f9dee495ced880d3c9', 97),
    ('0x53570dce00f0df6f941c98f901ed49f05cc0f7', 95),
    ('0x045779cad3b3930453651fbb1fc5e89aab7fc770', 89),
]

ROOT = Path(r'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER')
PORTF = ROOT / 'data' / 'wallet_portfolios'

acct_hist = {}
for w, alloc in wallets:
    p = PORTF / f'{w}.json'
    if not p.exists():
        continue
    data = json.loads(p.read_text())
    periods = {x[0]: x[1] for x in data}
    avh = periods.get('month', {}).get('accountValueHistory') or []
    acct_hist[w] = sorted([(int(t), float(v)) for t, v in avh])

print(f'Loaded account history for {len(acct_hist)} wallets')

trades = pd.read_csv(ROOT / 'data' / 'all_trades.csv', usecols=['wallet', 'time', 'closedPnl'])
trades['wallet'] = trades['wallet'].str.lower()
trades['time'] = pd.to_datetime(trades['time'], unit='ms', utc=True)

now = pd.Timestamp.now(tz='UTC')
three_months_ago = now - pd.Timedelta(days=90)
one_month_ago = now - pd.Timedelta(days=30)

results = []
for w, alloc in wallets:
    if w not in acct_hist:
        continue
    avh = acct_hist[w]
    if not avh:
        continue

    wt = trades[trades['wallet'] == w.lower()].copy()
    if len(wt) == 0:
        continue

    ts_arr = np.array([t for t, _ in avh], dtype=np.int64)
    vals = np.array([v for _, v in avh], dtype=np.float64)

    def get_acctV(trade_ts_ms):
        idx = np.searchsorted(ts_arr, trade_ts_ms, side='right') - 1
        return vals[0] if idx < 0 else vals[idx]

    wt['trade_ts_ms'] = wt['time'].astype(np.int64)
    wt['leader_acctV'] = wt['trade_ts_ms'].apply(get_acctV)
    wt['copy_ratio'] = alloc / wt['leader_acctV']
    wt['my_pnl'] = wt['closedPnl'] * wt['copy_ratio']

    wt3 = wt[wt['time'] > three_months_ago]
    wt1 = wt[wt['time'] > one_month_ago]

    results.append({
        'wallet': w,
        'alloc': alloc,
        'pnl_1m': wt1['my_pnl'].sum(),
        'pnl_3m': wt3['my_pnl'].sum(),
        'trades_1m': len(wt1),
        'trades_3m': len(wt3),
    })

df = pd.DataFrame(results).sort_values('pnl_3m', ascending=False)

print('=== REAL HISTORICAL COPY PnL (PROPORTIONAL MODE) ===')
for _, r in df.iterrows():
    print(f"{r['wallet'][:12]}... Alloc=${r['alloc']} 1M=${r['pnl_1m']:.2f} 3M=${r['pnl_3m']:.2f} 1M_trades={int(r['trades_1m'])} 3M_trades={int(r['trades_3m'])}")

print(f"\nTOTAL 1M PnL: ${df['pnl_1m'].sum():.2f}")
print(f"TOTAL 3M PnL: ${df['pnl_3m'].sum():.2f}")
print(f"Return on $3000 seed: {df['pnl_3m'].sum()/3000*100:.1f}% over 3 months")