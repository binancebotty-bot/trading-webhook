import pandas as pd, numpy as np

DIR = r'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\data'
br = pd.read_csv(DIR + r'\wallet_bankroll_requirements.csv')

print('=' * 120)
print('PRACTICAL MINIMUM BANKROLL TO COPY TOP 10 WALLETS')
print('=' * 120)
print()
print('Two components of bankroll:')
print('  1. DRAWDOWN BUFFER  = Max historical DD x 1.25 (absorbs worst-case loss)')
print('  2. WORKING CAPITAL  = Margin locked in open positions (returned on close)')
print()
print('For HIGH-FREQUENCY wallets, working capital dominates.')
print('For LOW-FREQUENCY wallets, drawdown buffer dominates.')
print()

print(f"{'#':<3} {'Wallet':<20} {'Model':<16} {'MaxDD':>8} {'DD+25%':>8} {'PnL':>10} {'PnL/DD':>8} {'Freq':>8}")
print('-' * 120)

for i, (_, r) in enumerate(br.head(10).iterrows()):
    nb = r.get('norm_base')
    model_str = f'prop nb{int(nb)}' if pd.notna(nb) and r['model'] == 'proportional' else 'fixed'
    dd_buffer = r['max_dd'] * 1.25
    pnl_dd = r['sim_pnl'] / max(r['max_dd'], 1)
    # Estimate daily trade count
    trades = r.get('sim_trades', 0)
    daily = trades / 90  # ~3 month window
    freq = f'{daily:.0f}/day' if daily > 1 else 'low'
    print(f"#{i+1:<2} {r['wallet'][:18]:<20} {model_str:<16} ${r['max_dd']:>6,.0f} ${dd_buffer:>6,.0f} ${r['sim_pnl']:>8,.0f} {pnl_dd:>6.1f}x {freq:>8}")

print()
t1 = br.head(2)
t2 = br.head(5)
t3 = br.head(10)

print('=' * 120)
print('TIERED RECOMMENDATIONS')
print('=' * 120)
print()
print('  TIER 1 - Micro (2 best wallets):')
print(f'    Wallets: 0xfd81... (prop nb620) + 0xae7a... (fixed)')
print(f'    DD buffer:  ${(t1["max_dd"].sum() * 1.25):>6,.0f}')
print(f'    Expected PnL: ${t1["sim_pnl"].sum():>6,.0f}')
print(f'    ABSOLUTE MINIMUM:     ${t1["max_dd"].sum() * 1.25:>6,.0f}')
print(f'    COMFORTABLE (2x):     ${t1["max_dd"].sum() * 2.5:>6,.0f}')
print()
print('  TIER 2 - Small (5 wallets):')
print(f'    DD buffer:  ${(t2["max_dd"].sum() * 1.25):>6,.0f}')
print(f'    Expected PnL: ${t2["sim_pnl"].sum():>6,.0f}')
print(f'    ABSOLUTE MINIMUM:     ${t2["max_dd"].sum() * 1.25:>6,.0f}')
print(f'    COMFORTABLE (2x):     ${t2["max_dd"].sum() * 2.5:>6,.0f}')
print()
print('  TIER 3 - Full (10 wallets):')
print(f'    DD buffer:  ${(t3["max_dd"].sum() * 1.25):>6,.0f}')
print(f'    Expected PnL: ${t3["sim_pnl"].sum():>6,.0f}')
print(f'    ABSOLUTE MINIMUM:     ${t3["max_dd"].sum() * 1.25:>6,.0f}')
print(f'    COMFORTABLE (2x):     ${t3["max_dd"].sum() * 2.5:>6,.0f}')
print()
print('=' * 120)
print('CRITICAL CAVEATS')
print('=' * 120)
print('  1. MaxDD is from 3-month trade replay. Real drawdowns can exceed this.')
print('  2. High-freq wallets (0xae58: 315 trades/day) need extra working')
print('     capital for margin. The DD buffer does NOT cover margin requirements.')
print('  3. On Hyperliquid cross-margin, you need account value > total margin')
print('     for all open positions + unrealised losses.')
print('  4. Conservative rule: bankroll = MaxDD x 3 + 50% for margin buffer.')
print()
print('  FINAL RECOMMENDATION:')
print(f'    For a 2-wallet portfolio: start with ${t1["max_dd"].sum() * 1.25:,.0f}, scale to ${t1["max_dd"].sum() * 3:,.0f}')
print(f'    For a 5-wallet portfolio: start with ${t2["max_dd"].sum() * 1.25:,.0f}, scale to ${t2["max_dd"].sum() * 3:,.0f}')
print(f'    For all 10:               start with ${t3["max_dd"].sum() * 1.25:,.0f}, scale to ${t3["max_dd"].sum() * 3:,.0f}')
