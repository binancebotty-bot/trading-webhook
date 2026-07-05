"""V2 funnel: candidates filtered by HL portfolio API MTM data, not summary.csv."""
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent

cand = pd.read_csv(OUT / "candidates.csv")  # 849 from V1 funnel (already safety-pruned)
mtm = pd.read_csv(ROOT / "summary_mtm.csv")

# Wallet lowercase match
cand["wallet"] = cand["wallet"].str.lower()
mtm["wallet"] = mtm["wallet"].str.lower()
joined = cand.merge(mtm, on="wallet", how="left")
print(f"joined: {len(joined)} candidates, {joined.status.eq('ok').sum()} with MTM data")

# Hard MTM filter: positive month, meaningful drawdown signal, real account
before = len(joined)
joined = joined[
    (joined.status == "ok")
    & (joined.month_acctV_chg > 200)              # +$200 MTM in the month (material)
    & (joined.month_acctV_mdd_mtm < -50)          # at least $50 of real MTM DD (not artifact)
    & (joined.month_acctV_end > 100)              # account not nuked
    & (joined.month_pts >= 20)                    # enough history
].copy()
print(f"after MTM filter: {len(joined)} (dropped {before - len(joined)})")

joined["mtm_calmar"] = joined.month_acctV_chg / joined.month_acctV_mdd_mtm.abs()
joined = joined.sort_values("mtm_calmar", ascending=False).reset_index(drop=True)

joined.to_csv(OUT / "candidates_v2.csv", index=False)
print(f"top 20 by MTM Calmar:")
cols = ["wallet","month_acctV_chg","month_acctV_mdd_mtm","mtm_calmar","month_acctV_end"]
print(joined[cols].head(20).to_string(index=False))
