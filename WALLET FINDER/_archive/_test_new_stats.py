"""Quick test of the new extract_wallet_stats with MTM allTime DD."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# First, test _summarise directly
from hl_mtm_lookup import _summarise

cache_dir = Path(__file__).resolve().parent / "data" / "wallet_portfolios"
files = list(cache_dir.glob("*.json"))[:5]

print("=== _summarise test ===")
for f in files:
    data = json.loads(f.read_text(encoding="utf-8"))
    mtm = _summarise(data)
    dd = mtm.get("allTime_max_drawdown_mtm")
    peak = mtm.get("allTime_acctV_peak")
    print(f"  {f.stem[:16]}: dd={dd}, peak={peak}")

print("\n=== extract_wallet_stats test ===")
from stage1_simple import extract_wallet_stats

for f in files:
    data = json.loads(f.read_text(encoding="utf-8"))
    try:
        result = extract_wallet_stats(data)
        if result:
            print(f"  {f.stem[:16]}: PnL={result['realised_pnl']:.2f}, dd={result['abs_dd']:.2f}, ratio={result['pnl_dd_ratio']:.2f}")
        else:
            print(f"  {f.stem[:16]}: None")
    except Exception as e:
        print(f"  {f.stem[:16]}: ERROR: {e}")
