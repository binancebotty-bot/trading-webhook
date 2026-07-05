"""Walk-forward OOS validation:
  - First 72 days = in-sample (already used)
  - Last 18 days = OOS
  Score the chosen 10 on the OOS slice.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

OUT = Path(__file__).resolve().parent
CURVES = OUT / "best_curves"

WINDOW_END_MS = 1778747921863
WINDOW_START_MS = WINDOW_END_MS - 90 * 86400 * 1000
SPLIT_MS = WINDOW_START_MS + int(0.8 * (WINDOW_END_MS - WINDOW_START_MS))

final = pd.read_csv(OUT / "final_portfolio.csv")
print(f"OOS window: {(WINDOW_END_MS - SPLIT_MS)/86400000:.1f} days")

rows = []
for _, r in final.iterrows():
    w = r["wallet"]
    df = pd.read_parquet(CURVES / f"{w}.parquet")
    in_sample = df[df["time"] < SPLIT_MS]
    oos = df[df["time"] >= SPLIT_MS]
    def stats(d):
        if not len(d): return 0.0, 0.0
        eq = d["copy_pnl"].cumsum().to_numpy()
        rm = np.maximum.accumulate(eq)
        mdd = (eq - rm).min()
        return float(eq[-1]), float(mdd)
    is_t, is_mdd = stats(in_sample)
    oo_t, oo_mdd = stats(oos)
    rows.append({
        "wallet": w, "mode": r["chosen_mode"],
        "IS_total": is_t, "IS_mdd": is_mdd,
        "OOS_total": oo_t, "OOS_mdd": oo_mdd,
        "OOS_peak_margin": float(oos["margin_used"].max() if len(oos) else 0.0),
    })

df = pd.DataFrame(rows)
print("\nper-wallet IS vs OOS:")
print(df.to_string(index=False))

# Joint OOS
all_pnl = []
all_marg = []
for _, r in final.iterrows():
    d = pd.read_parquet(CURVES / f"{r['wallet']}.parquet")
    d = d[d["time"] >= SPLIT_MS]
    all_pnl.append(d.set_index("time")["copy_pnl"])
    all_marg.append(d.set_index("time")["margin_used"])

bars = np.arange(SPLIT_MS, WINDOW_END_MS + 300000, 300000)
joint_pnl = np.zeros(len(bars))
joint_marg = np.zeros(len(bars))
for _, r in final.iterrows():
    d = pd.read_parquet(CURVES / f"{r['wallet']}.parquet")
    d = d.sort_values("time")
    t = d["time"].to_numpy()
    pnl_cum = d["copy_pnl"].cumsum().to_numpy()
    marg = d["margin_used"].to_numpy()
    is_end_idx = np.searchsorted(t, SPLIT_MS, side="right") - 1
    base_pnl = pnl_cum[is_end_idx] if is_end_idx >= 0 else 0.0
    idx = np.searchsorted(t, bars, side="right") - 1
    pnl_at = np.where(idx >= 0, pnl_cum[np.clip(idx, 0, len(pnl_cum)-1)], 0.0)
    marg_at = np.where(idx >= 0, marg[np.clip(idx, 0, len(marg)-1)], 0.0)
    joint_pnl += (pnl_at - base_pnl)
    joint_marg += marg_at

rm = np.maximum.accumulate(joint_pnl)
mdd = (joint_pnl - rm).min()
print(f"\n=== JOINT OOS (last 18 days) ===")
print(f"total: ${joint_pnl[-1]:,.2f}   mdd: ${mdd:,.2f}   peak_margin: ${joint_marg.max():,.0f}")
print(f"in-sample (joint, 72d): see 05_portfolio output -- $17,840 total, -$9 mdd")

oos_summary = {
    "OOS_days": (WINDOW_END_MS - SPLIT_MS)/86400000,
    "OOS_joint_total": float(joint_pnl[-1]),
    "OOS_joint_mdd": float(mdd),
    "OOS_peak_margin": float(joint_marg.max()),
}
import json
with open(OUT / "oos_summary.json", "w") as f:
    json.dump(oos_summary, f, indent=2)
df.to_csv(OUT / "oos_per_wallet.csv", index=False)
