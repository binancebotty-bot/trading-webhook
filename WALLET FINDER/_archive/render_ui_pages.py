"""Render both UI pages to HTML files and open in browser."""
import sys, os, json, webbrowser

base = r'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER'
sys.path.insert(0, base)

# Load model state
ms_path = os.path.join(base, 'hl_copy_output', 'app_model_state.json')
print('Loading model state...')
with open(ms_path, 'r') as f:
    state = json.load(f)
print(f'Loaded. {len(state.get("wallet_rows", []))} wallets.')

# Import the app module for render functions
import HL_Copy_App_SSOT as app_mod

# Render home page
print('Rendering home page...')
html_home = app_mod.render_home(state)
home_path = os.path.join(base, 'hl_copy_output', 'ui_home.html')
with open(home_path, 'w', encoding='utf-8') as f:
    f.write(html_home)
print(f'Home page: {home_path} ({len(html_home):,} bytes)')

# Find the top wallet for detail page
wallets = state.get('wallet_rows', [])
wallets_sorted = sorted(wallets, key=lambda w: w.get('copy_total_pnl', 0), reverse=True)
top_wallet = wallets_sorted[0]
wallet_addr = top_wallet.get('wallet', '')
print(f'Top wallet for detail: {wallet_addr}')

# Inject state into the module cache so wallet_detail() can find it
import time
app_mod._MODEL_CACHE['state'] = state
app_mod._MODEL_CACHE['built_at'] = time.time()

# Render wallet detail page by calling the route function directly
print('Rendering wallet detail page...')
html_wallet = app_mod.wallet_detail(wallet_addr)
wallet_path = os.path.join(base, 'hl_copy_output', 'ui_wallet_detail.html')
with open(wallet_path, 'w', encoding='utf-8') as f:
    f.write(html_wallet)
print(f'Wallet detail: {wallet_path} ({len(html_wallet):,} bytes)')

# Open both in browser
print('\nOpening in browser...')
webbrowser.open(f'file:///{home_path}')
webbrowser.open(f'file:///{wallet_path}')
print('Done! Both pages opened.')
