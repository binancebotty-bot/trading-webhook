import csv, os

# Check the new summary
with open(r'data\summary.csv', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))
print(f"New summary.csv: {len(rows)} wallets")
if rows:
    print(f"Columns: {list(rows[0].keys())}")

# Check universe and copyable
with open(r'data\wallet_universe.csv', encoding='utf-8') as f:
    u = list(csv.DictReader(f))
print(f"wallet_universe.csv: {len(u)} wallets")

with open(r'data\copyable_wallets.csv', encoding='utf-8') as f:
    c = list(csv.DictReader(f))
print(f"copyable_wallets.csv: {len(c)} wallets")

# Check sit-out list
sp = r'data\sit_out_list.csv'
if os.path.exists(sp):
    with open(sp, encoding='utf-8') as f:
        s = list(csv.DictReader(f))
    print(f"sit_out_list.csv: {len(s)} wallets")
else:
    print("sit_out_list.csv: NOT FOUND")

# Verify the problematic wallet is gone
target = '0x7cb42a9f'
found = any(target in r.get('wallet', '') for r in rows)
status = "YES - STILL PRESENT" if found else "NO - correctly removed"
print(f"\nWallet 0x7cb42a9f in new summary: {status}")

# Verify catastrophic wallets are gone
catastrophic = ['0x352d88fcc7', '0xe1b8ebfc91', '0xcb8436264d', '0xf517639a8872', '0x795cfd1b03ea']
for w in catastrophic:
    found = any(w in r.get('wallet', '') for r in rows)
    st = "STILL PRESENT" if found else "removed"
    print(f"  {w}: {st}")

# Show all survivors with key metrics
print(f"\n=== ALL {len(rows)} SURVIVORS ===")
print(f"{'Wallet':<46} {'PnL':>12} {'DD ($)':>12} {'DD%':>6} {'Ratio':>7} {'Trades':>7} {'7d':>4}")
for r in sorted(rows, key=lambda x: -float(x.get('_pnl_dd_ratio', 0) or 0)):
    w = r['wallet'][:44]
    pnl = float(r.get('total_pnl', 0))
    dd = float(r.get('_dd_usd', 0))
    dd_pct = float(r.get('_dd_pct', 0) or 0)
    ratio = float(r.get('_pnl_dd_ratio', 0) or 0)
    trades = r.get('trades', '?')
    t7d = r.get('trades_7d', '?')
    print(f"  {w:<44} {pnl:>12,.0f} {dd:>12,.0f} {dd_pct:>5.1f}% {ratio:>7.2f} {trades:>7} {t7d:>4}")
