"""Debug why 4336 wallets got calmar_nil rejection."""
import json, os, sys, csv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hl_mtm_lookup import _summarise

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Load stage1 pass
stage1_wallets = []
with open(os.path.join(DATA_DIR, "hl_stage1_pass.csv"), newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        stage1_wallets.append(row["wallet"].lower())

# Load stage1.5 passed wallets
passed = set()
with open(os.path.join(DATA_DIR, "hl_stage1_5_mtm_pass.csv"), newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        passed.add(row["wallet"].lower())

cache_dir = os.path.join(DATA_DIR, "wallet_portfolios")
no_cache = 0
unavail = 0
calmar_nil = 0
calmar_zero = 0
calmar_neg = 0
calmar_real = 0
equity_collapse = 0
negative = 0
low_acctv = 0
low_pnl_dd = 0
examples_nil = []
examples_zero = []
examples_unavail = []

for w in stage1_wallets:
    if w in passed:
        continue
    cache_path = os.path.join(cache_dir, f"{w}.json")
    if not os.path.exists(cache_path):
        no_cache += 1
        continue
    try:
        with open(cache_path) as f:
            data = json.load(f)
        result = _summarise(data)
        source = result.get("mtm_source", "unavailable")
        calmar = result.get("mtm_calmar")
        mdd = result.get("max_drawdown_mtm")

        if source == "unavailable":
            unavail += 1
            if len(examples_unavail) < 3:
                examples_unavail.append(w[:12])
            continue

        # Data was available - check why gate rejected
        if calmar is None:
            calmar_nil += 1
            if len(examples_nil) < 5:
                examples_nil.append({
                    "w": w[:12],
                    "mdd": mdd,
                    "month_chg": result.get("month_pnl_chg_mtm"),
                    "peak": result.get("month_acctV_peak"),
                    "end": result.get("month_acctV_end"),
                    "at_mdd": result.get("allTime_max_drawdown_mtm"),
                    "at_pnl": result.get("allTime_pnl_chg_mtm"),
                    "ec": result.get("equity_collapse_flag_mtm"),
                    "neg": result.get("negative_total_flag_mtm"),
                })
        elif calmar < 0:
            calmar_neg += 1
        elif calmar == 0:
            calmar_zero += 1
            if len(examples_zero) < 3:
                examples_zero.append({
                    "w": w[:12],
                    "mdd": mdd,
                    "month_chg": result.get("month_pnl_chg_mtm"),
                    "peak": result.get("month_acctV_peak"),
                    "end": result.get("month_acctV_end"),
                })
        else:
            calmar_real += 1
            # These SHOULD have passed calmar - check other gates
            if result.get("equity_collapse_flag_mtm"):
                equity_collapse += 1
            if result.get("negative_total_flag_mtm"):
                negative += 1
            end_val = result.get("month_acctV_end") or 0
            if end_val < 5000:
                low_acctv += 1
            at_pnl = result.get("allTime_pnl_chg_mtm")
            at_dd = result.get("allTime_max_drawdown_mtm")
            if at_pnl is not None and at_dd is not None and abs(at_dd) > 1:
                ratio = at_pnl / abs(at_dd)
                if ratio < 1.5:
                    low_pnl_dd += 1
    except Exception as e:
        pass

print(f"=== STAGE 1.5 REJECTION BREAKDOWN (from cached data) ===")
print(f"Total stage1 wallets: {len(stage1_wallets)}")
print(f"Passed stage1.5: {len(passed)}")
print(f"Rejected (analyzed):")
print(f"  No cache file:          {no_cache}")
print(f"  API unavailable:        {unavail}")
print(f"  calmar = None (nil):    {calmar_nil}")
print(f"  calmar = 0:             {calmar_zero}")
print(f"  calmar < 0:             {calmar_neg}")
print(f"  calmar > 0 (real):      {calmar_real}")
print(f"    - equity collapse:    {equity_collapse}")
print(f"    - negative month:     {negative}")
print(f"    - acctV < 5000:       {low_acctv}")
print(f"    - PnL/DD < 1.5:       {low_pnl_dd}")
print(f"    - other (passed all): {calmar_real - equity_collapse - negative - low_acctv - low_pnl_dd}")

print(f"\n=== calmar_nil EXAMPLES (mdd < $1 causing None) ===")
for ex in examples_nil:
    print(f"  {ex['w']} | mdd={ex['mdd']} | chg={ex['month_chg']} | peak={ex['peak']} | end={ex['end']} | at_mdd={ex['at_mdd']} | ec={ex['ec']} | neg={ex['neg']}")

print(f"\n=== calmar_zero EXAMPLES ===")
for ex in examples_zero:
    print(f"  {ex['w']} | mdd={ex['mdd']} | chg={ex['month_chg']} | peak={ex['peak']} | end={ex['end']}")

print(f"\n=== unavailable examples (no cache) ===")
for ex in examples_unavail:
    print(f"  {ex}")
