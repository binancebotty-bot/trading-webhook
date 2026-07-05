"""Portfolio selection: pick 10 wallets that maximise joint Calmar subject to
peak cross-margin <= MARGIN_BUDGET on a 5-minute joint timeline."""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

OUT = Path(__file__).resolve().parent
CURVES = OUT / "best_curves"

MARGIN_BUDGET = 4500.0
ACCOUNT_USD = 5000.0
PORTFOLIO_N = 10

# 90-day window
WINDOW_END_MS = 1778747921863
WINDOW_START_MS = WINDOW_END_MS - 90 * 86400 * 1000
BAR_MS = 5 * 60 * 1000  # 5 min
bars = np.arange(WINDOW_START_MS, WINDOW_END_MS + BAR_MS, BAR_MS)
print(f"timeline bars: {len(bars)}")


def load_wallet(wallet: str):
    df = pd.read_parquet(CURVES / f"{wallet}.parquet")
    # cumsum copy_pnl
    df = df.sort_values("time").reset_index(drop=True)
    df["pnl_cum"] = df["copy_pnl"].cumsum()
    # binsearch each bar's last-known value (forward-fill semantics)
    t = df["time"].to_numpy()
    pnl = df["pnl_cum"].to_numpy()
    marg = df["margin_used"].to_numpy()
    idx = np.searchsorted(t, bars, side="right") - 1  # last fill at-or-before bar
    pnl_at = np.where(idx >= 0, pnl[np.clip(idx, 0, len(pnl)-1)], 0.0)
    pnl_at = np.where(idx < 0, 0.0, pnl_at)
    marg_at = np.where(idx >= 0, marg[np.clip(idx, 0, len(marg)-1)], 0.0)
    marg_at = np.where(idx < 0, 0.0, marg_at)
    return pnl_at, marg_at


shortlist = pd.read_csv(OUT / "shortlist.csv")
wallets = shortlist["wallet"].tolist()
print(f"loading {len(wallets)} wallet curves...")
PNL = np.zeros((len(wallets), len(bars)))
MARG = np.zeros((len(wallets), len(bars)))
for i, w in enumerate(wallets):
    p, m = load_wallet(w)
    PNL[i] = p
    MARG[i] = m
print(f"loaded matrices: {PNL.shape}")


def joint_metrics(idx: list[int]):
    """For a portfolio = list of wallet indices, return (total_pnl, mdd, calmar, peak_margin, feasible)."""
    p = PNL[idx].sum(axis=0)
    m = MARG[idx].sum(axis=0)
    running_max = np.maximum.accumulate(p)
    dd = p - running_max
    mdd = dd.min() if len(dd) else 0.0
    total = p[-1] if len(p) else 0.0
    peak_margin = m.max() if len(m) else 0.0
    feasible = peak_margin <= MARGIN_BUDGET
    calmar = total / (-mdd) if mdd < -1e-6 else (total if total > 0 else 0.0)
    return total, mdd, calmar, peak_margin, feasible


# Greedy build
selected: list[int] = []
remaining = set(range(len(wallets)))
print("\n--- greedy build ---")
while len(selected) < PORTFOLIO_N:
    best_cand = None
    best_score = -np.inf
    for c in remaining:
        trial = selected + [c]
        total, mdd, calmar, peak_margin, feasible = joint_metrics(trial)
        if not feasible:
            continue
        # Score: regularised Calmar to avoid weirdness at small DD; tie-break on total
        score = total / (abs(mdd) + 100.0)
        if score > best_score:
            best_score = score
            best_cand = (c, total, mdd, calmar, peak_margin)
    if best_cand is None:
        print("no feasible addition; stopping early")
        break
    c, total, mdd, calmar, peak_margin = best_cand
    selected.append(c)
    remaining.discard(c)
    print(f"  added {wallets[c][:10]}... -> n={len(selected)} total=${total:,.0f} mdd=${mdd:,.0f} calmar={calmar:.1f} peak_margin=${peak_margin:,.0f}")

print("\n--- swap-improvement ---")
improved = True
passes = 0
while improved and passes < 20:
    improved = False
    passes += 1
    base_total, base_mdd, base_calmar, base_pm, _ = joint_metrics(selected)
    base_score = base_total / (abs(base_mdd) + 100.0)
    for i, s in enumerate(selected):
        for c in list(remaining):
            trial = selected.copy()
            trial[i] = c
            total, mdd, calmar, peak_margin, feasible = joint_metrics(trial)
            if not feasible:
                continue
            score = total / (abs(mdd) + 100.0)
            if score > base_score + 1e-6:
                selected[i] = c
                remaining.discard(c)
                remaining.add(s)
                base_score = score
                base_total, base_mdd, base_calmar, base_pm = total, mdd, calmar, peak_margin
                improved = True
                print(f"  pass {passes}: swap {wallets[s][:10]} -> {wallets[c][:10]}  score={score:.2f} total=${total:,.0f} mdd=${mdd:,.0f}")
                break
        if improved:
            break
print(f"\nswap passes: {passes}")

# Emit final
final = [wallets[i] for i in selected]
print(f"\n=== FINAL PORTFOLIO ({len(final)}) ===")
final_df = shortlist[shortlist["wallet"].isin(final)].copy()
# Reorder by selection order
order = {w: i for i, w in enumerate(final)}
final_df["pick_order"] = final_df["wallet"].map(order)
final_df = final_df.sort_values("pick_order")
total, mdd, calmar, peak_margin, feasible = joint_metrics(selected)
print(f"joint: total=${total:,.0f}  MDD=${mdd:,.0f}  Calmar={calmar:.2f}  peak_margin=${peak_margin:,.0f}/$ {MARGIN_BUDGET:.0f}")
print(final_df[["pick_order","wallet","chosen_mode","chosen_params","chosen_total","chosen_mdd","chosen_calmar","n_fills"]].to_string(index=False))
final_df.to_csv(OUT / "final_portfolio.csv", index=False)

# Save joint timeseries for OOS validation
p = PNL[selected].sum(axis=0)
m = MARG[selected].sum(axis=0)
joint_ts = pd.DataFrame({"time": bars, "joint_pnl": p, "joint_margin": m})
joint_ts.to_parquet(OUT / "joint_timeseries.parquet", index=False)
print(f"\nsaved final_portfolio.csv + joint_timeseries.parquet")
