"""Compose the final deliverable CSV that's ready to paste into the live UI."""
import pandas as pd
import numpy as np
from pathlib import Path

OUT = Path(__file__).resolve().parent
final = pd.read_csv(OUT / "final_portfolio.csv")
oos = pd.read_csv(OUT / "oos_per_wallet.csv")

# Parse out the actual params used (pre-scaling) and apply the scale_factor to derive
# the recommended live-UI values.
def parse_params(row):
    mode = row["chosen_mode"]
    params = row["chosen_params"]
    factor = row["scale_factor"]
    if mode == "proportional":
        nb = float(params.split("norm_base=")[1].split(",")[0])
        leb = float(params.split("leader_equity_base=")[1])
        return pd.Series({
            "live_copy_mode": "proportional",
            "live_norm_base": round(nb * factor, 1),
            "live_leader_equity_base": leb,
            "live_fixed_notional": None,
        })
    else:
        fn = float(params.split("fixed_notional=")[1])
        return pd.Series({
            "live_copy_mode": "fixed",
            "live_norm_base": None,
            "live_leader_equity_base": None,
            "live_fixed_notional": round(fn * factor, 2),
        })

live_params = final.apply(parse_params, axis=1)
final = pd.concat([final, live_params], axis=1)
final = final.merge(oos[["wallet","OOS_total","OOS_mdd","OOS_peak_margin"]], on="wallet", how="left")

cols = [
    "pick_order","wallet","live_copy_mode","live_norm_base","live_leader_equity_base","live_fixed_notional",
    "scaled_total","scaled_mdd","scaled_peak_margin","scaled_calmar",
    "OOS_total","OOS_mdd","OOS_peak_margin","n_fills",
]
final[cols].sort_values("pick_order").to_csv(OUT / "FINAL_10_WALLETS.csv", index=False)
print(f"saved {OUT / 'FINAL_10_WALLETS.csv'}")
print()
print(final[cols].sort_values("pick_order").to_string(index=False))
print()
# Joint summary
is_total = float(final["scaled_total"].sum())
oos_total = float(final["OOS_total"].sum())
peak_marg = 3157.0  # from 05_portfolio joint
oos_peak = 3157.0
print(f"--- HEADLINE ---")
print(f"$5,000 account, 90-day window (72d IS / 18d OOS), 10-wallet portfolio")
print(f"IS:  total return ${is_total:,.0f} ({is_total/5000*100:.0f}%), joint MDD ${-9.0:.2f}, peak margin ${peak_marg:,.0f}")
print(f"OOS: total return ${oos_total:,.0f} ({oos_total/5000*100:.0f}%) in 18 days, joint MDD ${-8.67:.2f}")
