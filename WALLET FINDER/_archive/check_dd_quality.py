"""Check DD quality for universe wallets"""
import csv

with open('data/summary.csv', 'r') as f:
    reader = csv.DictReader(f)
    summary = {r['wallet'].lower(): r for r in reader}

with open('data/wallet_universe.csv', 'r') as f:
    reader = csv.DictReader(f)
    universe = {r['wallet'].lower(): r for r in reader}

# For each universe wallet, check DD quality
results = []
for w, urow in universe.items():
    srow = summary.get(w, {})
    
    pnl = float(urow.get('realised_pnl', 0) or 0)
    dd_realised = float(srow.get('max_drawdown', 0) or 0)
    dd_mtm = float(srow.get('max_drawdown_mtm', 0) or 0)
    dd_mtm_alltime = float(srow.get('allTime_max_drawdown_mtm', 0) or 0)
    mtm_calmar = float(srow.get('mtm_calmar', 0) or 0)
    eff = float(urow.get('efficiency', 0) or 0)
    score = float(urow.get('score', 0) or 0)
    trades = int(float(urow.get('trades', 0) or 0))
    
    # PnL/DD ratios
    ratio_realised = pnl / abs(dd_realised) if abs(dd_realised) > 0.01 else 999
    ratio_mtm = pnl / abs(dd_mtm_alltime) if abs(dd_mtm_alltime) > 0.01 else 999
    
    results.append({
        'wallet': w,
        'pnl': pnl,
        'dd_realised': dd_realised,
        'dd_mtm': dd_mtm,
        'dd_mtm_alltime': dd_mtm_alltime,
        'mtm_calmar': mtm_calmar,
        'ratio_realised': ratio_realised,
        'ratio_mtm': ratio_mtm,
        'efficiency': eff,
        'score': score,
        'trades': trades,
    })

# How many have bad MTM DD?
print("=== DRAWDOWN ANALYSIS ===")
print(f"Total universe: {len(results)}")
print()

# Classify by MTM DD quality
good_mtm = [r for r in results if r['ratio_mtm'] >= 1.0]
ok_mtm = [r for r in results if 0.5 <= r['ratio_mtm'] < 1.0]
bad_mtm = [r for r in results if 0.2 <= r['ratio_mtm'] < 0.5]
terrible_mtm = [r for r in results if r['ratio_mtm'] < 0.2]
no_dd_mtm = [r for r in results if r['ratio_mtm'] >= 999]

print("PnL / allTime MTM DD ratio:")
print(f"  >= 1.0 (good):     {len(good_mtm)}")
print(f"  0.5-1.0 (ok):      {len(ok_mtm)}")
print(f"  0.2-0.5 (bad):     {len(bad_mtm)}")
print(f"  < 0.2 (terrible):  {len(terrible_mtm)}")
print(f"  no DD (999):       {len(no_dd_mtm)}")
print()

# Classify by realised DD quality
good_r = [r for r in results if r['ratio_realised'] >= 1.0]
ok_r = [r for r in results if 0.5 <= r['ratio_realised'] < 1.0]
bad_r = [r for r in results if 0.2 <= r['ratio_realised'] < 0.5]
terrible_r = [r for r in results if r['ratio_realised'] < 0.2]
no_dd_r = [r for r in results if r['ratio_realised'] >= 999]

print("PnL / Realised DD ratio:")
print(f"  >= 1.0 (good):     {len(good_r)}")
print(f"  0.5-1.0 (ok):      {len(ok_r)}")
print(f"  0.2-0.5 (bad):     {len(bad_r)}")
print(f"  < 0.2 (terrible):  {len(terrible_r)}")
print(f"  no DD (999):       {len(no_dd_r)}")
print()

# Show the worst MTM wallets
print("=== WORST 15 by MTM DD (PnL / allTime MTM DD) ===")
bad_sorted = sorted(results, key=lambda r: r['ratio_mtm'])
for r in bad_sorted[:15]:
    print(f"  {r['wallet'][:12]}... pnl=${r['pnl']:>8,.2f}  mtm_dd=${r['dd_mtm_alltime']:>8,.2f}  "
          f"ratio={r['ratio_mtm']:.3f}  eff={r['efficiency']:.4f}  score={r['score']:.1f}  trades={r['trades']}")

print()
print("=== TOP 15 by MTM DD (PnL / allTime MTM DD) ===")
for r in bad_sorted[-15:]:
    dd_str = f"${r['dd_mtm_alltime']:>8,.2f}" if r['dd_mtm_alltime'] != 0 else "       0"
    print(f"  {r['wallet'][:12]}... pnl=${r['pnl']:>8,.2f}  mtm_dd={dd_str}  "
          f"ratio={r['ratio_mtm']:.3f}  eff={r['efficiency']:.4f}  score={r['score']:.1f}  trades={r['trades']}")

# Also check: what percentage of wallets have zero DD in summary?
zero_dd = sum(1 for r in results if abs(r['dd_mtm_alltime']) < 0.01)
print(f"\nWallets with near-zero MTM DD: {zero_dd}")
zero_dd_r = sum(1 for r in results if abs(r['dd_realised']) < 0.01)
print(f"Wallets with near-zero Realised DD: {zero_dd_r}")
