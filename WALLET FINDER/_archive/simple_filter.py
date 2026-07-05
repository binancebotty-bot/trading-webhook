"""
Simple wallet filter — two conditions only:
  1. Max drawdown < 40% of account peak (accountValueHistory from HL portfolio API)
  2. Traded in last 7 days (last_seen timestamp from hl_wallets_filtered.csv)

Applies to the FULL 121K wallet set — not just Stage 1 survivors.
Uses cached portfolio JSONs for DD check; needs API fetch for the rest.
"""
import csv
import json
import os
import time

DATA = 'data'
PORT_DIR = f'{DATA}/wallet_portfolios'
DD_PCT_THRESHOLD = 0.40  # 40%
SEVEN_DAYS = 7 * 86400
NOW = time.time()
CUTOFF = NOW - SEVEN_DAYS

# Load FULL wallet universe (121K)
with open(f'{DATA}/hl_wallets_filtered.csv') as f:
    all_rows = {row['wallet'].strip().lower(): row for row in csv.DictReader(f)}
print(f"Full wallet universe: {len(all_rows)} wallets")

# Load current Stage 1.5 pass for comparison
with open(f'{DATA}/hl_stage1_5_mtm_pass.csv') as f:
    s15_wallets = {row['wallet'].strip().lower() for row in csv.DictReader(f)}
print(f"Previous S1.5 pass: {len(s15_wallets)} wallets")

# Load Stage 1 pass for comparison
with open(f'{DATA}/hl_stage1_pass.csv') as f:
    s1_wallets = {row['wallet'].strip().lower() for row in csv.DictReader(f)}
print(f"Previous Stage 1 pass: {len(s1_wallets)} wallets")

passed = []
no_recent = 0
no_cache = 0
high_dd = 0
insufficient = 0

for wallet, row in all_rows.items():
    # Condition 2: traded in last 7 days
    last_seen = float(row.get('last_seen', 0) or 0)
    if last_seen < CUTOFF:
        no_recent += 1
        continue
    
    # Condition 1: max drawdown < 40% of peak
    cache_path = os.path.join(PORT_DIR, f'{wallet}.json')
    if not os.path.exists(cache_path):
        no_cache += 1
        continue
    
    with open(cache_path) as f:
        data = json.load(f)
    
    if not data or len(data) < 1:
        insufficient += 1
        continue
    
    # Portfolio cache is a list of [period, {accountValueHistory: [[ts, val], ...]}]
    acct_vals = []
    for item in data:
        if isinstance(item, list) and len(item) >= 2 and item[0] in ('day', 'week'):
            hist = item[1].get('accountValueHistory', [])
            for point in hist:
                if isinstance(point, list) and len(point) >= 2:
                    try:
                        acct_vals.append(float(point[1]))
                    except (ValueError, TypeError):
                        pass
            if acct_vals:
                break
    
    if not acct_vals or len(acct_vals) < 2:
        insufficient += 1
        continue
    
    peak = max(acct_vals)
    if peak <= 0:
        high_dd += 1
        continue
    
    # Calculate max drawdown from peak
    max_dd_pct = 0.0
    running_peak = acct_vals[0]
    for v in acct_vals:
        if v > running_peak:
            running_peak = v
        dd_pct = (running_peak - v) / running_peak if running_peak > 0 else 0
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct
    
    if max_dd_pct >= DD_PCT_THRESHOLD:
        high_dd += 1
        continue
    
    # Passed both conditions
    passed.append({
        'wallet': wallet,
        'max_dd_pct': round(max_dd_pct * 100, 2),
        'peak_value': round(peak, 2),
        'end_value': round(acct_vals[-1], 2),
        'in_cache': True,
        'was_in_s15': wallet in s15_wallets,
        'was_in_s1': wallet in s1_wallets,
    })

print(f"\n=== SIMPLE FILTER RESULTS (from {len(all_rows)} wallets) ===")
print(f"Condition 1: Max DD < {DD_PCT_THRESHOLD*100:.0f}% of peak")
print(f"Condition 2: Traded in last 7 days")
print(f"")
print(f"Passed BOTH:              {len(passed)}")
print(f"  - was in previous S1.5: {sum(1 for p in passed if p['was_in_s15'])}")
print(f"  - was in Stage 1 only:  {sum(1 for p in passed if p['was_in_s1'] and not p['was_in_s15'])}")
print(f"  - NEW (never seen):     {sum(1 for p in passed if not p['was_in_s1'])}")
print(f"")
print(f"Eliminated breakdown:")
print(f"  No recent trades:       {no_recent:>6} (of {len(all_rows)})")
print(f"  No portfolio cache:     {no_cache:>6} (need API fetch for DD check)")
print(f"  DD >= {DD_PCT_THRESHOLD*100:.0f}%:              {high_dd:>6}")
print(f"  Insufficient data:      {insufficient:>6}")
print(f"  Note: 'No cache' wallets may PASS DD check if fetched — they need API data")

# Show distribution of DD% among passed wallets
passed.sort(key=lambda x: x['max_dd_pct'])
print(f"\n=== DD% DISTRIBUTION (passed wallets) ===")
brackets = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40)]
for lo, hi in brackets:
    count = sum(1 for p in passed if lo <= p['max_dd_pct'] < hi)
    print(f"  {lo:>2}-{hi:>2}%: {count}")

# Show peak account value distribution
print(f"\n=== PEAK ACCOUNT VALUE DISTRIBUTION ===")
val_brackets = [(0, 1000), (1000, 5000), (5000, 10000), (10000, 50000), (50000, 100000), (100000, 1e9)]
for lo, hi in val_brackets:
    count = sum(1 for p in passed if lo <= p['peak_value'] < hi)
    label = f"${lo:,.0f}-${hi:,.0f}" if hi < 1e6 else f">${lo:,.0f}"
    print(f"  {label}: {count}")

# Write output
output_file = f'{DATA}/simple_filter_pass.csv'
fields = ['wallet', 'max_dd_pct', 'peak_value', 'end_value', 'in_cache', 'was_in_s15', 'was_in_s1']
with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(passed)
print(f"\nWrote {len(passed)} wallets to {output_file}")
