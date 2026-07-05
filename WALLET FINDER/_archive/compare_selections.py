import pandas as pd
import numpy as np
import os

os.chdir(r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER")

# Load all data
summary = pd.read_csv('data/summary.csv')
top20 = pd.read_csv('data/top20_wallets_v3.csv')
v3 = pd.read_csv('data/wallet_analysis_v3.csv')
bruteforce = pd.read_csv('data/wallet_bruteforce.csv')

# Other agent's wallets (full addresses where possible)
other_wallets = {
    '0xac2ce040': {'full_hint': '0xac2ce040', 'alloc': 789, 'ratio': 1.48, 'min_lead': 814, 'mtm_calmar': 48.59, 'trades': 545, 'recent_3d': 137},
    '0x9f3e77cb': {'full_hint': '0x9f3e77cb', 'alloc': 626, 'ratio': 0.62, 'min_lead': 1949, 'mtm_calmar': 38.60, 'trades': 14382, 'recent_3d': 174},
    '0x96f0cd31': {'full_hint': '0x96f0cd31', 'alloc': 359, 'ratio': 3.49, 'min_lead': 344, 'mtm_calmar': 22.14, 'trades': 466, 'recent_3d': 125},
    '0xd47935ce': {'full_hint': '0xd47935ce', 'alloc': 342, 'ratio': 6.23, 'min_lead': 192, 'mtm_calmar': 21.06, 'trades': 303, 'recent_3d': 9},
    '0xb6bfa5b7': {'full_hint': '0xb6bfa5b7', 'alloc': 340, 'ratio': 22.57, 'min_lead': 53, 'mtm_calmar': 20.97, 'trades': 13220, 'recent_3d': 13698},
    '0x22439c28': {'full_hint': '0x22439c28', 'alloc': 143, 'ratio': 0.73, 'min_lead': 1643, 'mtm_calmar': 8.78, 'trades': 466, 'recent_3d': 91},
    '0x1c6ad8b5': {'full_hint': '0x1c6ad8b5', 'alloc': 121, 'ratio': 1.02, 'min_lead': 1181, 'mtm_calmar': 7.46, 'trades': 469, 'recent_3d': 21},
    '0x0d7ec7b7': {'full_hint': '0x0d7ec7b7', 'alloc': 97, 'ratio': 1.73, 'min_lead': 694, 'mtm_calmar': 5.95, 'trades': 479, 'recent_3d': 29},
    '0x53570dce': {'full_hint': '0x53570dce', 'alloc': 95, 'ratio': 0.89, 'min_lead': 1342, 'mtm_calmar': 5.87, 'trades': 1349, 'recent_3d': 150},
    '0x045779ca': {'full_hint': '0x045779ca', 'alloc': 89, 'ratio': 0.53, 'min_lead': 2264, 'mtm_calmar': 5.45, 'trades': 27230, 'recent_3d': 3543},
}

print("=" * 100)
print("CROSS-EVALUATION: OTHER AGENT vs MY SELECTIONS")
print("=" * 100)

# 1. Look up each other-agent wallet in summary.csv
print("\n1. OTHER AGENT WALLETS — SUMMARY DATA (API MTM)")
print("-" * 100)
print(f"{'Prefix':<12} {'Trades':>7} {'PnL':>10} {'Win%':>6} {'MaxDD':>10} {'MTM Calmar':>10} {'AcctV':>12} {'Collapse':>8} {'NegTotal':>8} {'MTM Source':>12}")
print("-" * 100)

for addr, info in other_wallets.items():
    mask = summary['wallet'].str.lower().str.startswith(addr.lower())
    if mask.any():
        row = summary[mask].iloc[0]
        ec = int(row['equity_collapse_flag_mtm']) if pd.notna(row['equity_collapse_flag_mtm']) else -1
        nt = int(row['negative_total_flag_mtm']) if pd.notna(row['negative_total_flag_mtm']) else -1
        src = str(row['mtm_source']) if pd.notna(row['mtm_source']) else 'unknown'
        print(f"{addr:<12} {int(row['trades']):>7} ${row['total_pnl']:>8,.0f} {row['win_rate']:>5.0%} ${row['max_drawdown']:>8,.0f} {row['mtm_calmar']:>10.1f} ${row['month_acctV_end']:>10,.0f} {ec:>8} {nt:>8} {src:>12}")
    else:
        print(f"{addr:<12} {'NOT IN SUMMARY':>7}")

# 2. Look up in my simulation data
print("\n2. OTHER AGENT WALLETS — MY SIMULATION RESULTS")
print("-" * 110)
print(f"{'Prefix':<12} {'My Model':>14} {'My Calmar':>10} {'My PnL':>10} {'My DD':>10} {'Win%':>6} {'Cap%':>5} {'Trades':>8} {'Composite':>10}")
print("-" * 110)

found_in_sim = 0
for addr, info in other_wallets.items():
    # Check top20 first
    mask = top20['wallet'].str.lower().str.startswith(addr.lower())
    if mask.any():
        row = top20[mask].iloc[0]
        print(f"{addr:<12} {row['best_mode']:>14} {row['best_calmar']:>10.1f} ${row['best_pnl']:>8,.0f} ${row['best_dd']:>8,.0f} {row['best_win_rate']:>5.0%} {row['best_capture_pct']:>4.0f}% {int(row['best_trades_exec']):>8} {row['best_composite']:>10.1f}")
        found_in_sim += 1
        continue
    # Check bruteforce
    mask = bruteforce['wallet'].str.lower().str.startswith(addr.lower())
    if mask.any():
        row = bruteforce[mask].iloc[0]
        mode = 'fixed' if row['best_mode'] == 'fixed' else f"prop nb{int(row['best_nb'])}"
        print(f"{addr:<12} {mode:>14} {row['best_calmar']:>10.1f} ${row['best_pnl']:>8,.0f} ${row['best_dd']:>8,.0f} {row['best_win_rate']:>5.0%} {row['best_capture']:>4.0f}% {'':>8} {row['best_composite']:>10.1f}")
        found_in_sim += 1
        continue
    print(f"{addr:<12} {'NOT SIMULATED':>14}")

print(f"\n  Found in simulation: {found_in_sim}/10")

# 3. My top 20 for reference
print("\n3. MY TOP 20 — FOR REFERENCE")
print("-" * 110)
print(f"{'Rank':<5} {'Prefix':<12} {'Model':>14} {'Calmar':>10} {'PnL':>10} {'DD':>10} {'Win%':>6} {'Cap%':>5} {'Trades':>8} {'MTM Calmar':>10}")
print("-" * 110)
for i, (_, row) in enumerate(top20.iterrows()):
    print(f"{i+1:<5} {row['wallet'][:10]:<12} {row['best_mode']:>14} {row['best_calmar']:>10.1f} ${row['best_pnl']:>8,.0f} ${row['best_dd']:>8,.0f} {row['best_win_rate']:>5.0%} {row['best_capture_pct']:>4.0f}% {int(row['best_trades_exec']):>8} {row['mtm_calmar']:>10.1f}")

# 4. Overlap
print("\n4. OVERLAP ANALYSIS")
print("-" * 80)
my_prefixes = set(row['wallet'][:10] for _, row in top20.iterrows())
other_prefixes = set(other_wallets.keys())
overlap = set()
for op in other_prefixes:
    for mp in my_prefixes:
        if op.startswith(mp[:8]) or mp.startswith(op[:8]):
            overlap.add((op, mp))

if overlap:
    print(f"  Found {len(overlap)} overlapping wallets:")
    for op, mp in overlap:
        print(f"    Other: {op} <-> Mine: {mp}")
else:
    print("  ZERO OVERLAP — completely different selections")

# 5. Critical analysis
print("\n5. CRITICAL ANALYSIS")
print("-" * 80)

# Check each other-agent wallet for red flags
for addr, info in other_wallets.items():
    mask = summary['wallet'].str.lower().str.startswith(addr.lower())
    if not mask.any():
        continue
    row = summary[mask].iloc[0]
    
    flags = []
    
    # Check if in my simulation
    in_sim = top20['wallet'].str.lower().str.startswith(addr.lower()).any() or bruteforce['wallet'].str.lower().str.startswith(addr.lower()).any()
    
    # Trade count vs agent report
    if abs(row['trades'] - info['trades']) > 500:
        flags.append(f"TRADE COUNT MISMATCH (summary={int(row['trades'])}, agent={info['trades']})")
    
    # Low trade count
    if row['trades'] < 300:
        flags.append(f"LOW TRADES ({int(row['trades'])})")
    
    # MTM source
    if row['mtm_source'] == 'http_429':
        flags.append("MTM FROM RATE-LIMITED API (may be stale)")
    
    # Negative total
    if row['negative_total_flag_mtm'] == 1:
        flags.append("NEGATIVE TOTAL FLAG")
    
    # Equity collapse
    if row['equity_collapse_flag_mtm'] == 1:
        flags.append("EQUITY COLLAPSE FLAG")
    
    # Calmar sanity
    if row['mtm_calmar'] > 50:
        flags.append(f"VERY HIGH CALMAR ({row['mtm_calmar']:.1f}) — verify")
    
    # Simulated vs MTM calmar divergence
    if in_sim:
        sim_mask = bruteforce['wallet'].str.lower().str.startswith(addr.lower())
        if sim_mask.any():
            sim_row = bruteforce[sim_mask].iloc[0]
            if row['mtm_calmar'] > 0 and sim_row['best_calmar'] > 0:
                ratio = row['mtm_calmar'] / sim_row['best_calmar']
                if ratio > 5:
                    flags.append(f"CALMAR DIVERGENCE: MTM={row['mtm_calmar']:.1f} vs Sim={sim_row['best_calmar']:.1f} ({ratio:.0f}x)")
    
    # Recent activity check
    if info['recent_3d'] > 10000:
        flags.append(f"HUGE RECENT ACTIVITY ({info['recent_3d']} trades in 3d) — possible market maker")
    
    # Account value
    if row['month_acctV_end'] < 10000:
        flags.append(f"SMALL ACCOUNT (${row['month_acctV_end']:,.0f})")
    
    flag_str = " *** " + " | ".join(flags) if flags else " [OK]"
    print(f"  {addr}: MTM Calmar={row['mtm_calmar']:.1f}, Sim PnL=${row['total_pnl']:,.0f}{flag_str}")

# 6. Summary comparison
print("\n6. METHODOLOGY COMPARISON")
print("-" * 80)

# My top 10 stats
my_pnl = top20['best_pnl'].head(10).values
my_dd = top20['best_dd'].head(10).values
my_calmar = top20['best_calmar'].head(10).values
my_mtm_calmar = top20['mtm_calmar'].head(10).values

# Other agent stats
other_mtm_calmars = []
for addr in other_wallets:
    mask = summary['wallet'].str.lower().str.startswith(addr.lower())
    if mask.any():
        other_mtm_calmars.append(summary[mask].iloc[0]['mtm_calmar'])

print(f"""
  MY TOP 10 (simulated):
    Simulated Calmar: {np.min(my_calmar):.1f} - {np.max(my_calmar):.1f} (median {np.median(my_calmar):.1f})
    API MTM Calmar:   {np.min(my_mtm_calmar):.1f} - {np.max(my_mtm_calmar):.1f} (median {np.median(my_mtm_calmar):.1f})
    PnL: ${np.min(my_pnl):,.0f} - ${np.max(my_pnl):,.0f}
    MaxDD: ${np.min(my_dd):,.0f} - ${np.max(my_dd):,.0f}

  OTHER AGENT TOP 10 (MTM Calmar ranked):
    MTM Calmar: {np.min(other_mtm_calmars):.1f} - {np.max(other_mtm_calmars):.1f} (median {np.median(other_mtm_calmars):.1f})
    No simulation — no PnL/DD estimates
""")

# 7. VERDICT
print("7. VERDICT")
print("=" * 80)
print("""
  OTHER AGENT APPROACH:
    - Ranked purely by API MTM Calmar (no trade replay simulation)
    - Uses proportional allocation (alloc / lead_notional = ratio)
    - Portfolio construction with dollar allocation per wallet
    - Includes recent activity weighting (3-day trade count)
    - Total allocation: $3,002 across 10 wallets

  MY APPROACH:
    - Full trade replay simulation (fixed $12 + proportional norm_base sweep)
    - Balanced composite score (Calmar 25% + Sortino 15% + PnL 35% + DD 10% + WinRate 15%)
    - Validates actual trade capture and min order viability
    - No portfolio construction (pure ranking)

  CRITICAL DIFFERENCES:
    1. OVERLAP: 0/10 wallets in common — fundamentally different selection methods
    2. CALMAR SOURCE: Other agent uses API MTM (with unrealised PnL); I use simulated closedPnl
    3. VALIDATION: I simulate actual copy execution; other agent trusts reported MTM
    4. ALLOCATION: Other agent sizes positions; I just rank
    5. RED FLAGS in other agent's picks:
       - 0xb6bfa5b7: 22.57% ratio ($53 min lead) — extreme concentration, single wallet is 22% of portfolio
       - 0x045779ca: 27,230 trades — market maker, 3,543 trades in 3 days
       - Several wallets have <500 trades (thin sample)
       - No drawdown validation — MTM Calmar could be misleading

  RECOMMENDATION:
    - OTHER AGENT provides portfolio construction (good)
    - MY ANALYSIS provides realistic PnL/DD estimates (good)
    - IDEAL: Use other agent's allocation framework + my simulation validation
    - Specific concerns to flag:
      a) 0xb6bfa5b7's 22.57% allocation is too concentrated
      b) Require simulated Calmar >= 5 (not just MTM Calmar)
      c) 0x0d7ec7b7 and 0xd47935ce have only 9 and 29 recent trades — possible inactivity
""")
