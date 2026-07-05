import pandas as pd
import numpy as np
import os

os.chdir(r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER")

summary = pd.read_csv('data/summary.csv')
top20 = pd.read_csv('data/top20_wallets_v3.csv')
v3 = pd.read_csv('data/wallet_analysis_v3.csv')
bf = pd.read_csv('data/wallet_bruteforce.csv')

# User's wallets (partial addresses)
user_wallets = ['0xd405f0', '0x9db82c', '0x82d7eb', '0xf83858', '0x811e8f']

# Other agent's list for cross-ref
other_wallets = ['0xac2ce040', '0x9f3e77cb', '0x96f0cd31', '0xd47935ce', '0xb6bfa5b7', 
                 '0x22439c28', '0x1c6ad8b5', '0x0d7ec7b7', '0x53570dce', '0x045779ca']

print("=" * 110)
print("USER WALLETS — FULL LOOKUP")
print("=" * 110)

all_user_data = []

for prefix in user_wallets:
    print(f"\n--- {prefix}... ---")
    found_summary = False
    full_addr = prefix
    
    # 1. Summary data
    mask = summary['wallet'].str.lower().str.startswith(prefix.lower())
    if mask.any():
        row = summary[mask].iloc[0]
        full_addr = row['wallet']
        found_summary = True
        ec = int(row['equity_collapse_flag_mtm']) if pd.notna(row['equity_collapse_flag_mtm']) else -1
        nt = int(row['negative_total_flag_mtm']) if pd.notna(row['negative_total_flag_mtm']) else -1
        src = str(row['mtm_source']) if pd.notna(row['mtm_source']) else 'unknown'
        trades_7d = int(row['trades_7d']) if 'trades_7d' in row and pd.notna(row['trades_7d']) else 0
        print(f"  Full addr: {full_addr}")
        print(f"  Summary: trades={int(row['trades'])}, PnL=${row['total_pnl']:,.0f}, win={row['win_rate']:.0%}, DD=${row['max_drawdown']:,.0f}")
        print(f"  MTM: calmar={row['mtm_calmar']:.1f}, month_chg=${row['month_pnl_chg_mtm']:,.0f}, acctV=${row['month_acctV_end']:,.0f}")
        print(f"  Flags: collapse={ec}, neg_total={nt}, source={src}, timespan={row['timespan_hours']:.0f}h, symbols={int(row['symbol_count'])}")
    else:
        print(f"  NOT IN summary.csv")
    
    # 2. Simulation results (top20)
    sim_found = False
    mask = top20['wallet'].str.lower().str.startswith(prefix.lower())
    if mask.any():
        row = top20[mask].iloc[0]
        print(f"  My sim (top20): model={row['best_mode']}, calmar={row['best_calmar']:.1f}, PnL=${row['best_pnl']:,.0f}, DD=${row['best_dd']:,.0f}, win={row['best_win_rate']:.0%}, cap={row['best_capture_pct']:.0f}%, composite={row['best_composite']:.1f}")
        sim_found = True
    
    # Check bruteforce
    mask = bf['wallet'].str.lower().str.startswith(prefix.lower())
    if mask.any():
        row = bf[mask].iloc[0]
        mode = 'fixed' if row['best_mode'] == 'fixed' else f"prop nb{int(row['best_nb'])}"
        print(f"  My sim (brute): model={mode}, calmar={row['best_calmar']:.1f}, PnL=${row['best_pnl']:,.0f}, DD=${row['best_dd']:,.0f}, win={row['best_win_rate']:.0%}, cap={row['best_capture']:.0f}%")
        sim_found = True
    
    if not sim_found:
        print(f"  NOT IN simulation results")
    
    # 3. Rank in my top 20
    mask = top20['wallet'].str.lower().str.startswith(prefix.lower())
    if mask.any():
        rank = top20[mask].index[0] + 1
        print(f"  My top 20 rank: #{rank}")
    else:
        print(f"  My top 20 rank: Not ranked")
    
    # 4. Other agent check
    in_other = any(prefix.lower()[:8] == ow[:8].lower() for ow in other_wallets)
    print(f"  In other agent top 10: {'YES' if in_other else 'No'}")
    
    # 5. Copyable wallet check
    try:
        cw = pd.read_csv('data/copyable_wallets.csv')
        in_copyable = cw['wallet'].str.lower().str.startswith(prefix.lower()).any()
        print(f"  In copyable_wallets.csv: {'Yes' if in_copyable else 'No'}")
    except:
        pass

# Check for duplicates with my top 10
print("\n" + "=" * 110)
print("OVERLAP CHECK")
print("=" * 110)
my_top10_prefixes = [w[:10].lower() for w in top20['wallet'].values[:10]]
for prefix in user_wallets:
    in_my = any(prefix.lower()[:8] == mp[:8] for mp in my_top10_prefixes)
    in_other = any(prefix.lower()[:8] == ow[:8].lower() for ow in other_wallets)
    overlaps = []
    if in_my:
        overlaps.append("MY TOP 10")
    if in_other:
        overlaps.append("OTHER AGENT TOP 10")
    if overlaps:
        print(f"  {prefix}: DUPLICATE with {' + '.join(overlaps)}")
    else:
        print(f"  {prefix}: NEW — not in either list")
