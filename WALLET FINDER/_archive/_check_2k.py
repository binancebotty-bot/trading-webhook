"""Check for 2000-trade cap in wallet_universe.csv and all_trades.csv"""
import csv, collections

# Check wallet_universe.csv
print("=== wallet_universe.csv ===")
with open("data/wallet_universe.csv", "r") as f:
    r = csv.DictReader(f)
    rows = list(r)
    trades = [int(float(row.get("trades", 0) or 0)) for row in rows]
    exact_2000 = sum(1 for t in trades if t == 2000)
    at_2000 = sum(1 for t in trades if t >= 2000)
    print(f"Total wallets: {len(rows)}")
    print(f"Exact 2000: {exact_2000}")
    print(f">= 2000: {at_2000}")
    print(f"Top 20: {sorted(trades, reverse=True)[:20]}")

# Check all_trades.csv
print("\n=== all_trades.csv ===")
counts = collections.Counter()
with open("data/all_trades.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        w = row.get("wallet", "")
        counts[w] += 1

total = len(counts)
exact_2000 = sum(1 for c in counts.values() if c == 2000)
at_2000 = sum(1 for c in counts.values() if c >= 2000)
print(f"Wallets: {total}, Total rows: {sum(counts.values())}")
print(f"Exact 2000: {exact_2000}, >= 2000: {at_2000}")
print(f"Max: {max(counts.values()) if counts else 0}")

# Check how many wallets in hl_stage1_pass have 2000 trades
print("\n=== hl_stage1_pass.csv ===")
try:
    with open("data/hl_stage1_pass.csv", "r") as f:
        r = csv.DictReader(f)
        rows = list(r)
        trades_col = "trades"
        vals = [int(float(row.get(trades_col, 0) or 0)) for row in rows if row.get(trades_col)]
        exact_2000 = sum(1 for v in vals if v == 2000)
        at_2000 = sum(1 for v in vals if v >= 2000)
        print(f"Wallets: {len(rows)}, with trades data: {len(vals)}")
        print(f"Exact 2000: {exact_2000}, >= 2000: {at_2000}")
        print(f"Top 20: {sorted(vals, reverse=True)[:20]}")
except Exception as e:
    print(f"Error: {e}")

# Check the cached portfolios for the 2000-cap wallets
print("\n=== Checking Hyperliquid API 'userFills' cap ===")
import json, os
cache_dir = "data/wallet_portfolios"
if os.path.exists(cache_dir):
    files = os.listdir(cache_dir)
    print(f"Cached portfolios: {len(files)}")
