"""Simulate Stage 1.5 gates against summary.csv to find rejection patterns."""
import csv
import json
import os

DATA = 'data'

# Load all data sources
with open(f'{DATA}/hl_stage1_pass.csv') as f:
    s1 = {row['wallet'].strip().lower(): row for row in csv.DictReader(f)}

with open(f'{DATA}/hl_stage1_5_mtm_pass.csv') as f:
    s15 = {row['wallet'].strip().lower(): True for row in csv.DictReader(f)}

with open(f'{DATA}/summary.csv') as f:
    summary = {row['wallet'].strip().lower(): row for row in csv.DictReader(f)}

killed = set(s1.keys()) - set(s15.keys())
passed = set(s1.keys()) & set(s15.keys())

print(f"Stage 1 pass: {len(s1)}")
print(f"Stage 1.5 passed: {len(passed)}")
print(f"Stage 1.5 killed: {len(killed)}")
print()

# For killed wallets, check portfolio cache to simulate gate_mtm
PORT_DIR = f'{DATA}/wallet_portfolios'

reasons = {}
no_cache = 0
summary_missing = 0

for w in killed:
    cache_path = os.path.join(PORT_DIR, f'{w}.json')
    if not os.path.exists(cache_path):
        no_cache += 1
        reason = 'no_cache'
        reasons[reason] = reasons.get(reason, 0) + 1
        continue
    
    with open(cache_path) as f:
        data = json.load(f)
    
    if not data or len(data) < 2:
        reason = 'insufficient_cache_data'
        reasons[reason] = reasons.get(reason, 0) + 1
        continue
    
    # Simulate _summarise from hl_mtm_lookup
    # The portfolio API returns accountValueHistory entries
    acct_vals = [e.get('accountValue', 0) for e in data if e.get('accountValue')]
    if not acct_vals:
        reason = 'no_account_values'
        reasons[reason] = reasons.get(reason, 0) + 1
        continue
    
    end_val = acct_vals[-1]
    peak = max(acct_vals)
    pnl_chg = acct_vals[-1] - acct_vals[0]
    
    # Compute drawdown
    max_dd = 0
    running_peak = acct_vals[0]
    for v in acct_vals:
        if v > running_peak:
            running_peak = v
        dd = v - running_peak
        if dd < max_dd:
            max_dd = dd
    
    calmar = abs(pnl_chg / max_dd) if max_dd < -1.0 else 999.0
    negative = pnl_chg < 0
    collapse = peak > 0 and (peak - min(acct_vals)) / peak > 0.5
    
    # Determine rejection reason
    if end_val < 5000:
        reason = f'low_account_value_{end_val:.0f}'
    elif collapse:
        reason = 'equity_collapse'
    elif negative:
        reason = 'negative_month'
    elif calmar < 1.0:
        reason = f'low_calmar_{calmar:.2f}'
    else:
        reason = 'pnl_dd_ratio'  # The pnl/dd ratio gate killed it
    
    reasons[reason] = reasons.get(reason, 0) + 1

print("=== REJECTION REASON BREAKDOWN (5,182 killed wallets) ===")
for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
    pct = count / len(killed) * 100
    print(f"  {count:>5} ({pct:5.1f}%) {reason}")

print(f"\n  {no_cache:>5} wallets had NO portfolio cache")

# Also check: what % of Stage 1 wallets have low account values?
print("\n=== ACCOUNT VALUE DISTRIBUTION (all Stage 1 wallets with cache) ===")
low_acct = 0
has_data = 0
for w in s1:
    cache_path = os.path.join(PORT_DIR, f'{w}.json')
    if not os.path.exists(cache_path):
        continue
    with open(cache_path) as f:
        data = json.load(f)
    if data and len(data) >= 2:
        has_data += 1
        acct_vals = [e.get('accountValue', 0) for e in data if e.get('accountValue')]
        if acct_vals:
            end_val = acct_vals[-1]
            if end_val < 5000:
                low_acct += 1

print(f"  Stage 1 wallets with cache: {has_data}")
print(f"  Ending account < $5000: {low_acct} ({low_acct/has_data*100:.1f}%)")
print(f"  Ending account >= $5000: {has_data - low_acct} ({(has_data-low_acct)/has_data*100:.1f}%)")

# Check: of the killed wallets that HAD >= $5000 account, what killed them?
print("\n=== KILLED WALLETS WITH >= $5000 ACCOUNT ===")
big_killed_reasons = {}
for w in killed:
    cache_path = os.path.join(PORT_DIR, f'{w}.json')
    if not os.path.exists(cache_path):
        continue
    with open(cache_path) as f:
        data = json.load(f)
    if not data or len(data) < 2:
        continue
    acct_vals = [e.get('accountValue', 0) for e in data if e.get('accountValue')]
    if not acct_vals:
        continue
    end_val = acct_vals[-1]
    if end_val < 5000:
        continue
    
    peak = max(acct_vals)
    pnl_chg = acct_vals[-1] - acct_vals[0]
    max_dd = 0
    running_peak = acct_vals[0]
    for v in acct_vals:
        if v > running_peak:
            running_peak = v
        dd = v - running_peak
        if dd < max_dd:
            max_dd = dd
    
    calmar = abs(pnl_chg / max_dd) if max_dd < -1.0 else 999.0
    negative = pnl_chg < 0
    collapse = peak > 0 and (peak - min(acct_vals)) / peak > 0.5
    
    if collapse:
        reason = 'equity_collapse'
    elif negative:
        reason = 'negative_month'
    elif calmar < 1.0:
        reason = f'low_calmar'
    else:
        reason = 'pnl_dd_ratio_or_other'
    
    big_killed_reasons[reason] = big_killed_reasons.get(reason, 0) + 1

for reason, count in sorted(big_killed_reasons.items(), key=lambda x: -x[1]):
    print(f"  {count:>5} {reason}")
