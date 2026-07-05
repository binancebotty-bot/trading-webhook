"""V2 final deliverable CSV: 10 wallets with live-UI-ready params + MTM-truth stats."""
import pandas as pd, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent
PORTF = ROOT / "wallet_portfolios"

final = pd.read_csv(OUT / "final_portfolio_v2.csv")

# Parse live-UI fields
def parse_params(row):
    nb = float(row.chosen_params.split("norm_base=")[1].split(",")[0])
    leb = float(row.chosen_params.split("leader_equity_base=")[1])
    return pd.Series({
        "live_copy_mode": "proportional",
        "live_norm_base": round(nb, 1),
        "live_leader_equity_base": leb,
        "live_fixed_notional": None,
    })
live = final.apply(parse_params, axis=1)
final = pd.concat([final, live], axis=1)

# Get HL allTime stats per wallet for context
def alltime_stats(w):
    p = PORTF / f"{w}.json"
    if not p.exists(): return {}
    data = json.loads(p.read_text())
    periods = {x[0]: x[1] for x in data}
    avh = periods.get("allTime", {}).get("accountValueHistory") or []
    pn = periods.get("allTime", {}).get("pnlHistory") or []
    vlm = periods.get("allTime", {}).get("vlm")
    if not avh: return {}
    vals = [float(p[1]) for p in avh]
    peak = vals[0]; mdd = 0
    for v in vals:
        if v > peak: peak = v
        if v - peak < mdd: mdd = v - peak
    return {
        "allTime_pnl_chg": round(float(pn[-1][1]) - float(pn[0][1]), 2) if pn else 0.0,
        "allTime_acctV_mdd_mtm": round(mdd, 2),
        "allTime_vlm": vlm,
    }

extras = [alltime_stats(w) for w in final.wallet]
ext_df = pd.DataFrame(extras)
final = pd.concat([final.reset_index(drop=True), ext_df], axis=1)

cols = [
    "pick_order", "wallet",
    "live_copy_mode", "live_norm_base", "live_leader_equity_base",
    "chosen_total", "chosen_mdd", "chosen_calmar",  # this wallet, scaled to $5k slot
    "month_acctV_chg", "month_acctV_mdd_mtm", "month_acctV_end",  # leader's MTM truth
    "allTime_pnl_chg", "allTime_acctV_mdd_mtm", "allTime_vlm",
    "n_fills",
]
final[cols].sort_values("pick_order").to_csv(OUT / "FINAL_10_WALLETS_V2.csv", index=False)
print(f"saved FINAL_10_WALLETS_V2.csv")
print(final[cols].sort_values("pick_order").to_string(index=False))
print()
joint_is = final.chosen_total.sum()
joint_mdd_is = -324  # from v2_portfolio output
print(f"\n--- HEADLINE V2 (MTM truth) ---")
print(f"$5,000 account, 30-day HL MTM window")
print(f"IS total: ${joint_is:,.0f} ({joint_is/5000*100:.0f}%)")
print(f"IS joint MDD: ${joint_mdd_is:,.0f}")
print(f"IS Calmar: {joint_is/-joint_mdd_is:.1f}")
print(f"OOS (last 7 days): +$9,236  MDD -$274  (see v2_oos_validation output)")
