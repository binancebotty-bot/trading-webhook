#!/usr/bin/env python3
"""
Two-part analysis:
1. Gap overlap check — do gaps coincide across wallets? (database issue vs normal breaks)
2. Intra-position drawdown — track unrealized PnL through position marks to find
   the drawdown we're missing from the closedPnl-only model.
"""

import csv
from datetime import datetime, timedelta
from collections import defaultdict

WALLETS = {
    '0x9db82c': '0x9db82c502472d76742fdd69609dfcc6e01327401',
    '0x82d7eb': '0x82d7ebbd8106b08e91f8ac9f4ca97fbd98125c29',
    '0xf83858': '0xf83858e57d9f804f5ca1603bce82558119aeac7b',
    '0x811e8f': '0x811e8f6d80f38a2f0f8b606cb743a950638f0ad4',
}

FEE_BPS = 5.0
SEED = 600.0  # per wallet
LEADER_BASE = 10000.0


def load_trades(full_addr):
    trades = []
    with open('data/all_trades.csv', 'r') as f:
        r = csv.DictReader(f)
        for row in r:
            if row['wallet'].lower() == full_addr.lower():
                trades.append(row)
    trades.sort(key=lambda x: int(x['time']))
    return trades


def find_gaps(trades, min_gap_hours=6):
    """Find gaps longer than min_gap_hours."""
    gaps = []
    for i in range(1, len(trades)):
        gap_h = (int(trades[i]['time']) - int(trades[i-1]['time'])) / (1000 * 3600)
        if gap_h > min_gap_hours:
            gaps.append({
                'start_ms': int(trades[i-1]['time']),
                'end_ms': int(trades[i]['time']),
                'hours': gap_h,
                'start_coin': trades[i-1]['coin'],
                'end_coin': trades[i]['coin'],
            })
    return gaps


def simulate_with_unrealized(trades, copy_mode, norm_base=0, fixed_notional=12.0, alloc=600.0):
    """
    Simulate copy with full position tracking including unrealized PnL.
    This captures intra-position drawdown that closedPnl-only misses.
    """
    scale = norm_base / LEADER_BASE if copy_mode == 'proportional' else 0
    equity = alloc
    peak = alloc
    max_dd = 0.0
    total_realized = 0.0

    # Position tracking: {coin: [(entry_price, size_units, side, copy_notional)]}
    positions = defaultdict(list)
    mark_prices = {}

    # For drawdown tracking, sample equity at every fill
    equity_curve = [(0, alloc)]

    for t in trades:
        px = float(t['px'])
        sz = float(t['sz'])
        closed_pnl = float(t['closedPnl'])
        coin = t['coin']
        side = t['side']
        time_ms = int(t['time'])

        if px <= 0 or sz <= 0:
            continue

        mark_prices[coin] = px
        leader_notional = abs(px * sz)

        if copy_mode == 'fixed':
            copy_notional = fixed_notional
        else:
            copy_notional = leader_notional * scale
            if copy_notional < 12.0:
                # Trade not captured — but position might still exist from before
                # Recalc unrealized from existing positions
                unrealized = 0.0
                for c, plist in positions.items():
                    mp = mark_prices.get(c, 0.0)
                    for ep, es, eside, _ in plist:
                        if eside == 'BUY':
                            unrealized += (mp - ep) * es
                        else:
                            unrealized += (ep - mp) * es
                equity = alloc + total_realized + unrealized
                peak = max(peak, equity)
                dd = peak - equity
                max_dd = max(max_dd, dd)
                continue

        # Determine entry vs exit based on position state
        pos_list = positions[coin]
        copy_size = copy_notional / px if px > 0 else 0
        fee = copy_notional * FEE_BPS / 10000.0

        if side == 'BUY':
            if not pos_list or pos_list[-1][2] == 'BUY':
                # Entry (add to long)
                positions[coin].append((px, copy_size, 'BUY', copy_notional))
                total_realized -= fee  # entry fee
            else:
                # Exit (reduce short)
                total_to_close = copy_size
                while total_to_close > 1e-12 and pos_list:
                    ep, es, eside, en = pos_list[0]
                    if eside != 'SELL':
                        break
                    frac = min(es, total_to_close) / es if es > 0 else 0
                    gross = (px - ep) * es * frac  # short: entry - exit
                    exit_fee = en * frac * FEE_BPS / 10000.0
                    total_realized += gross - exit_fee
                    remaining_es = es - es * frac
                    if remaining_es < 1e-12:
                        pos_list.pop(0)
                    else:
                        pos_list[0] = (ep, remaining_es, eside, en * (remaining_es / es))
                    total_to_close -= es * frac
                # Any excess becomes a new long entry
                if total_to_close > 1e-12:
                    new_cn = total_to_close * px
                    positions[coin].append((px, total_to_close, 'BUY', new_cn))
                    total_realized -= new_cn * FEE_BPS / 10000.0
        else:  # SELL
            if not pos_list or pos_list[-1][2] == 'SELL':
                # Entry (add to short)
                positions[coin].append((px, copy_size, 'SELL', copy_notional))
                total_realized -= fee
            else:
                # Exit (reduce long)
                total_to_close = copy_size
                while total_to_close > 1e-12 and pos_list:
                    ep, es, eside, en = pos_list[0]
                    if eside != 'BUY':
                        break
                    frac = min(es, total_to_close) / es if es > 0 else 0
                    gross = (px - ep) * es * frac  # long: exit - entry
                    exit_fee = en * frac * FEE_BPS / 10000.0
                    total_realized += gross - exit_fee
                    remaining_es = es - es * frac
                    if remaining_es < 1e-12:
                        pos_list.pop(0)
                    else:
                        pos_list[0] = (ep, remaining_es, eside, en * (remaining_es / es))
                    total_to_close -= es * frac
                if total_to_close > 1e-12:
                    new_cn = total_to_close * px
                    positions[coin].append((px, total_to_close, 'SELL', new_cn))
                    total_realized -= new_cn * FEE_BPS / 10000.0

        # Compute unrealized from all open positions
        unrealized = 0.0
        for c, plist in positions.items():
            mp = mark_prices.get(c, 0.0)
            for ep, es, eside, _ in plist:
                if eside == 'BUY':
                    unrealized += (mp - ep) * es
                else:
                    unrealized += (ep - mp) * es

        equity = alloc + total_realized + unrealized
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
        equity_curve.append((time_ms, equity))

    # Close remaining positions at last mark
    for coin, plist in positions.items():
        mp = mark_prices.get(coin, 0.0)
        for ep, es, eside, en in plist:
            if eside == 'BUY':
                gross = (mp - ep) * es
            else:
                gross = (ep - mp) * es
            exit_fee = en * FEE_BPS / 10000.0
            total_realized += gross - exit_fee

    unrealized = 0.0
    equity = alloc + total_realized + unrealized
    peak = max(peak, equity)
    dd = peak - equity
    max_dd = max(max_dd, dd)

    total_wins = sum(1 for _, eq in equity_curve if eq >= alloc)  # simplified
    return {
        'mode': copy_mode,
        'norm_base': norm_base,
        'total_pnl': total_realized,
        'max_dd': max_dd,
        'calmar': (total_realized / max_dd) if max_dd > 0 else 0,
        'equity_curve': equity_curve,
        'final_equity': equity,
        'positions_remaining': sum(len(v) for v in positions.values()),
    }


def main():
    # ============================================================
    # PART 1: Gap overlap analysis
    # ============================================================
    print("=" * 80)
    print("PART 1: GAP OVERLAP ANALYSIS")
    print("=" * 80)

    all_gaps = {}
    for short, full in WALLETS.items():
        trades = load_trades(full)
        if not trades:
            continue
        gaps = find_gaps(trades, min_gap_hours=6)
        all_gaps[short] = gaps
        if gaps:
            print(f"\n  {short}: {len(gaps)} gaps > 6h")
            for g in gaps:
                s = datetime.utcfromtimestamp(g['start_ms'] / 1000)
                e = datetime.utcfromtimestamp(g['end_ms'] / 1000)
                print(f"    {s.strftime('%Y-%m-%d %H:%M')} -> {e.strftime('%Y-%m-%d %H:%M')} ({g['hours']:.0f}h)")
        else:
            print(f"\n  {short}: no gaps > 6h")

    # Check overlap: convert gaps to day-level buckets
    print("\n  GAP OVERLAP MATRIX (which wallets are idle on which days):")
    all_wallets = list(all_gaps.keys())
    # Build set of idle days per wallet
    idle_days = {}
    for w, gaps in all_gaps.items():
        days = set()
        for g in gaps:
            s = datetime.utcfromtimestamp(g['start_ms'] / 1000)
            e = datetime.utcfromtimestamp(g['end_ms'] / 1000)
            d = s.date()
            while d <= e.date():
                days.add(d)
                d += timedelta(days=1)
        idle_days[w] = days

    # Find days where multiple wallets are idle
    all_days = set()
    for days in idle_days.values():
        all_days |= days

    overlap_days = []
    for day in sorted(all_days):
        idle_on = [w for w in all_wallets if day in idle_days.get(w, set())]
        if len(idle_on) >= 2:
            overlap_days.append((day, idle_on))

    if overlap_days:
        print(f"  Found {len(overlap_days)} days where 2+ wallets idle simultaneously:")
        for day, idle_on in overlap_days[:10]:
            print(f"    {day}: {', '.join(idle_on)}")
    else:
        print("  NO overlapping idle days — all gaps are independent (normal trader breaks)")

    # ============================================================
    # PART 2: Unrealized drawdown comparison
    # ============================================================
    print("\n" + "=" * 80)
    print("PART 2: REALISED vs UNREALISED DRAWDOWN (the missing drawdown)")
    print("=" * 80)

    for short, full in WALLETS.items():
        trades = load_trades(full)
        if not trades:
            continue

        print(f"\n--- {short} ({len(trades)} trades) ---")

        # Fixed $12: realised-only vs unrealised-tracking
        r_realised = simulate_with_unrealized(trades, 'fixed', fixed_notional=12.0, alloc=SEED)
        # The function already tracks unrealized — both use the same code
        # So r_realised IS the unrealised-tracking version

        print(f"  fixed $12 (unrealised-aware):")
        print(f"    PnL:    ${r_realised['total_pnl']:>8.2f}")
        print(f"    MaxDD:  ${r_realised['max_dd']:>8.2f}")
        print(f"    Calmar: {r_realised['calmar']:>8.2f}")
        print(f"    Final:  ${r_realised['final_equity']:>8.2f}")
        print(f"    Open:   {r_realised['positions_remaining']} positions")

        # Also test a few proportional norms
        for nb in [500, 1000, 2000]:
            r = simulate_with_unrealized(trades, 'proportional', norm_base=nb, alloc=SEED)
            if r['total_pnl'] != 0 or r['max_dd'] > 0:
                print(f"  prop nb{nb} (unrealised-aware):")
                print(f"    PnL:    ${r['total_pnl']:>8.2f}")
                print(f"    MaxDD:  ${r['max_dd']:>8.2f}")
                print(f"    Calmar: {r['calmar']:>8.2f}")
                print(f"    Open:   {r['positions_remaining']} positions")

    # ============================================================
    # PART 3: Leader equity curve for context
    # ============================================================
    print("\n" + "=" * 80)
    print("PART 3: LEADER EQUITY CURVES (what the leader actually experienced)")
    print("=" * 80)

    for short, full in WALLETS.items():
        trades = load_trades(full)
        if not trades:
            continue

        # Build leader equity curve from raw trades
        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        positions = defaultdict(list)
        mark_prices = {}

        for t in trades:
            px = float(t['px'])
            sz = float(t['sz'])
            coin = t['coin']
            side = t['side']

            if px <= 0 or sz <= 0:
                continue

            mark_prices[coin] = px
            notional = abs(px * sz)

            pos_list = positions[coin]
            if side == 'BUY':
                if not pos_list or pos_list[-1][2] == 'BUY':
                    positions[coin].append((px, sz, 'BUY'))
                else:
                    # Close shorts
                    to_close = sz
                    while to_close > 1e-12 and pos_list:
                        ep, es, eside = pos_list[0]
                        if eside != 'SELL':
                            break
                        frac = min(es, to_close) / es if es > 0 else 0
                        equity += (ep - px) * es * frac
                        rem = es - es * frac
                        if rem < 1e-12:
                            pos_list.pop(0)
                        else:
                            pos_list[0] = (ep, rem, eside)
                        to_close -= es * frac
                    if to_close > 1e-12:
                        positions[coin].append((px, to_close, 'BUY'))
            else:
                if not pos_list or pos_list[-1][2] == 'SELL':
                    positions[coin].append((px, sz, 'SELL'))
                else:
                    to_close = sz
                    while to_close > 1e-12 and pos_list:
                        ep, es, eside = pos_list[0]
                        if eside != 'BUY':
                            break
                        frac = min(es, to_close) / es if es > 0 else 0
                        equity += (px - ep) * es * frac
                        rem = es - es * frac
                        if rem < 1e-12:
                            pos_list.pop(0)
                        else:
                            pos_list[0] = (ep, rem, eside)
                        to_close -= es * frac
                    if to_close > 1e-12:
                        positions[coin].append((px, to_close, 'SELL'))

            # Unrealised from open positions
            unrealised = 0.0
            for c, plist in positions.items():
                mp = mark_prices.get(c, 0.0)
                for ep, es, eside in plist:
                    if eside == 'BUY':
                        unrealised += (mp - ep) * es
                    else:
                        unrealised += (ep - mp) * es

            total_eq = equity + unrealised
            peak = max(peak, total_eq)
            dd = peak - total_eq
            max_dd = max(max_dd, dd)

        print(f"  {short}: Leader MaxDD (unrealised-aware): ${max_dd:,.2f}")
        print(f"           Leader total closed PnL:     ${sum(float(t['closedPnl']) for t in trades):,.2f}")
        print(f"           Open positions at end:        {sum(len(v) for v in positions.values())}")


if __name__ == '__main__':
    main()
