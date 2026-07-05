#!/usr/bin/env python3
"""Deep verification of trade history completeness for user wallets."""

import csv
from datetime import datetime
from collections import defaultdict

WALLETS = {
    '0x9db82c': '0x9db82c502472d76742fdd69609dfcc6e01327401',
    '0x82d7eb': '0x82d7ebbd8106b08e91f8ac9f4ca97fbd98125c29',
    '0xf83858': '0xf83858e57d9f804f5ca1603bce82558119aeac7b',
    '0x811e8f': '0x811e8f6d80f38a2f0f8b606cb743a950638f0ad4',
}

def load_trades(full_addr):
    trades = []
    with open('data/all_trades.csv', 'r') as f:
        r = csv.DictReader(f)
        for row in r:
            if row['wallet'].lower() == full_addr.lower():
                trades.append(row)
    trades.sort(key=lambda x: int(x['time']))
    return trades

for short, full in WALLETS.items():
    trades = load_trades(full)
    if not trades:
        print(f'{short}: NO TRADES')
        continue

    first_ts = int(trades[0]['time'])
    last_ts = int(trades[-1]['time'])
    first_dt = datetime.utcfromtimestamp(first_ts / 1000).strftime('%Y-%m-%d %H:%M')
    last_dt = datetime.utcfromtimestamp(last_ts / 1000).strftime('%Y-%m-%d %H:%M')
    days = (last_ts - first_ts) / (1000 * 86400)

    pnls = [float(t['closedPnl']) for t in trades]
    total_pnl = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    zero_pnl = len(pnls) - wins - losses
    worst = min(pnls)
    best = max(pnls)

    # Largest losing streak (cumulative)
    cumsum = 0.0
    max_cumloss = 0.0
    streak = 0
    max_streak = 0
    for p in pnls:
        if p < 0:
            cumsum += p
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            cumsum = 0
            streak = 0
        max_cumloss = min(max_cumloss, cumsum)

    # Notional stats
    notionials = [abs(float(t['px']) * float(t['sz'])) for t in trades]

    # Check for duplicate timestamps (batch trades)
    ts_counts = defaultdict(int)
    for t in trades:
        ts_counts[int(t['time'])] += 1
    max_batch = max(ts_counts.values()) if ts_counts else 0
    unique_ts = len(ts_counts)

    # Leader equity curve (cumsum of closedPnl)
    equity = [0.0]
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        equity.append(equity[-1] + p)
        peak = max(peak, equity[-1])
        dd = peak - equity[-1]
        max_dd = max(max_dd, dd)

    # How many trades per day
    trades_per_day = len(trades) / days if days > 0 else 0

    print('=' * 70)
    print(f'  {short} ({full[:10]}...)')
    print('=' * 70)
    print(f'  Trades:           {len(trades):,}')
    print(f'  Date range:       {first_dt} to {last_dt}')
    print(f'  Days:             {days:.1f}')
    print(f'  Trades/day:       {trades_per_day:.0f}')
    print(f'  Unique timestamps:{unique_ts:,} (max batch: {max_batch})')
    print(f'  Leader PnL:       ${total_pnl:,.2f}')
    print(f'  Leader MaxDD:     ${max_dd:,.2f}')
    print(f'  Wins/Losses/Zero: {wins}/{losses}/{zero_pnl}')
    print(f'  Win rate:         {wins/(wins+losses)*100:.1f}% (excl zero)')
    print(f'  Worst trade:      ${worst:,.2f}')
    print(f'  Best trade:       ${best:,.2f}')
    print(f'  Max cumul loss:   ${max_cumloss:,.2f} (streak: {max_streak} trades)')
    print(f'  Notional range:   ${min(notionials):,.0f} to ${max(notionials):,.0f}')
    print(f'  Avg notional:     ${sum(notionials)/len(notionials):,.0f}')
    print(f'  Median notional:  ${sorted(notionials)[len(notionials)//2]:,.0f}')

    # Show 10 worst trades
    worst_trades = sorted(enumerate(trades), key=lambda x: float(x[1]['closedPnl']))
    print(f'\n  10 WORST TRADES:')
    for idx, t in worst_trades[:10]:
        px = float(t['px'])
        sz = float(t['sz'])
        notional = abs(px * sz)
        dt = datetime.utcfromtimestamp(int(t['time']) / 1000).strftime('%Y-%m-%d %H:%M')
        print(f'    {dt} | {t["coin"]:8s} {t["side"]:4s} | PnL: ${float(t["closedPnl"]):>10,.2f} | Notional: ${notional:>10,.0f}')

    # Show 10 biggest trades by notional
    big_trades = sorted(trades, key=lambda t: abs(float(t['px']) * float(t['sz'])), reverse=True)
    print(f'\n  10 LARGEST TRADES (by notional):')
    for t in big_trades[:10]:
        px = float(t['px'])
        sz = float(t['sz'])
        notional = abs(px * sz)
        dt = datetime.utcfromtimestamp(int(t['time']) / 1000).strftime('%Y-%m-%d %H:%M')
        print(f'    {dt} | {t["coin"]:8s} {t["side"]:4s} | Notional: ${notional:>10,.0f} | PnL: ${float(t["closedPnl"]):>10,.2f}')

    # Check if trades have gaps > 1 day
    gaps = []
    for i in range(1, len(trades)):
        gap_h = (int(trades[i]['time']) - int(trades[i-1]['time'])) / (1000 * 3600)
        if gap_h > 24:
            gaps.append((gap_h, trades[i-1], trades[i]))
    if gaps:
        gaps.sort(key=lambda x: x[0], reverse=True)
        print(f'\n  GAPS > 24h: {len(gaps)} found')
        for gap_h, t1, t2 in gaps[:5]:
            dt1 = datetime.utcfromtimestamp(int(t1['time']) / 1000).strftime('%Y-%m-%d %H:%M')
            dt2 = datetime.utcfromtimestamp(int(t2['time']) / 1000).strftime('%Y-%m-%d %H:%M')
            print(f'    {dt1} -> {dt2}: {gap_h:.0f}h gap')
    else:
        print(f'\n  No gaps > 24h')
    print()
