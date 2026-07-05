"""Check MTM coverage across the full trade universe"""
import pandas as pd, numpy as np
from pathlib import Path

DATA = Path(__file__).parent / "data"
best = pd.read_csv(DATA / "full_universe_best.csv")
summary = pd.read_csv(DATA / "summary.csv")
all_trades = pd.read_csv(DATA / "all_trades.csv")

# Qualifying wallets
qual_w = all_trades.groupby("wallet").size()
qual_w = qual_w[qual_w >= 100].index.tolist()
in_sum = summary[summary["wallet"].isin(qual_w)].copy()
no_mtm = [w for w in qual_w if w not in set(summary["wallet"])]

print(f"Qualifying wallets: {len(qual_w)}")
print(f"In summary.csv: {len(in_sum)}")
print(f"NOT in summary.csv: {len(no_mtm)}")

# MTM source distribution
print(f"\n=== MTM SOURCE BREAKDOWN ===")
print(in_sum["mtm_source"].value_counts())

# MTM filter breakdown
mtm_fail_reasons = {"calmar_low": 0, "collapse": 0, "negative": 0, "acctv_low": 0, "pass": 0}
for _, s in in_sum.iterrows():
    fails = False
    if s.get("mtm_calmar", 0) < 1.5:
        mtm_fail_reasons["calmar_low"] += 1; fails = True
    if s.get("equity_collapse_flag_mtm", 0) == 1:
        mtm_fail_reasons["collapse"] += 1; fails = True
    if s.get("negative_total_flag_mtm", 0) == 1:
        mtm_fail_reasons["negative"] += 1; fails = True
    if s.get("month_acctV_end", 0) < 5000:
        mtm_fail_reasons["acctv_low"] += 1; fails = True
    if not fails:
        mtm_fail_reasons["pass"] += 1

print(f"\n=== MTM FILTER FAILURE COUNTS (wallets can fail multiple) ===")
for k, v in sorted(mtm_fail_reasons.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v}")

# Check which summary wallets have ZERO mtm data vs unavailable
unavail = in_sum[in_sum["mtm_source"] == "unavailable"]
print(f"\nmtm_source=unavailable: {len(unavail)}")
print(f"mtm_source != unavailable: {len(in_sum) - len(unavail)}")

# Distribution of mtm_calmar for in-summary wallets
has_mtm = in_sum[in_sum["mtm_calmar"].notna() & (in_sum["mtm_source"] != "unavailable")]
print(f"\n=== MTM CALMAR DISTRIBUTION (n={len(has_mtm)}) ===")
print(f"  >= 1.5: {(has_mtm['mtm_calmar'] >= 1.5).sum()}")
print(f"  >= 1.0: {(has_mtm['mtm_calmar'] >= 1.0).sum()}")
print(f"  >= 0.5: {(has_mtm['mtm_calmar'] >= 0.5).sum()}")
print(f"  >= 0.0: {(has_mtm['mtm_calmar'] >= 0).sum()}")
print(f"  < 0.0: {(has_mtm['mtm_calmar'] < 0).sum()}")
print(f"  NaN: {in_sum['mtm_calmar'].isna().sum()}")

# Top 30 wallets - what's their actual MTM status?
print(f"\n=== TOP 30 WALLETS - MTM DETAIL ===")
for _, r in best.head(30).iterrows():
    w = r["wallet"]
    s = summary[summary["wallet"] == w]
    if len(s) == 0:
        print(f"  {w[:20]}... NO SUMMARY DATA")
        continue
    s = s.iloc[0]
    mc = s.get("mtm_calmar", "NaN")
    ed = s.get("equity_collapse_flag_mtm", "NaN")
    nt = s.get("negative_total_flag_mtm", "NaN")
    aev = s.get("month_acctV_end", "NaN")
    src = s.get("mtm_source", "NaN")
    peak = s.get("month_acctV_peak", "NaN")
    chg = s.get("month_pnl_chg_mtm", "NaN")
    print(f"  {w[:20]}... src={src} calmar={mc} collapse={ed} neg={nt} acctV_end={aev} peak={peak} chg={chg}")
