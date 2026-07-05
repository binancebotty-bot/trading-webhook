"""V2 portfolio selection on MTM truth.

For each shortlisted wallet, load HL accountValueHistory[month] from
wallet_portfolios/<wallet>.json. Resample to a daily grid. Compute leader's
incremental PnL series, scale by chosen_scale (norm_base/leb), and join into
a portfolio-level cumulative-PnL timeline. Joint MDD computed on that sum.

Margin constraint: each wallet contributes <= PER_SLOT_MARGIN ($450) by
construction (sizing was capped on that). Joint margin sum is therefore
bounded by 10 * $450 = $4500. No further runtime cap needed.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent
PORTF = ROOT / "wallet_portfolios"

PORTFOLIO_N = 10
SHORTLIST_N = 80
MARGIN_BUDGET = 4500.0
PER_SLOT_MARGIN = 450.0

best = pd.read_csv(OUT / "per_wallet_best_v2.csv")
# Shortlist: proportional only (fixed lacks MTM DD), feasible, ranked by chosen_score.
# Also: require leader account >= $5k (small leaders amplify noise via large scale_factor).
short = best[
    (best.chosen_mode == "proportional")
    & (best.month_acctV_end >= 5000.0)
].head(SHORTLIST_N).reset_index(drop=True)
print(f"shortlist: {len(short)} proportional-feasible wallets (leader_account >= $5k)")

# --- Build a daily timeline from HL month accountValueHistory ---
# Find all unique timestamps across short.
def load_month_avh(w: str) -> list[tuple[int, float]]:
    p = PORTF / f"{w}.json"
    if not p.exists():
        return []
    data = json.loads(p.read_text())
    periods = {x[0]: x[1] for x in data}
    avh = periods.get("month", {}).get("accountValueHistory") or []
    return [(int(t), float(v)) for t, v in avh]

curves = {}
all_ts = set()
for w in short.wallet:
    avh = load_month_avh(w)
    if not avh:
        continue
    avh.sort()
    curves[w] = avh
    for t, _ in avh:
        all_ts.add(t)
ts_sorted = sorted(all_ts)
print(f"timeline pts: {len(ts_sorted)} (unique)")

# Resample each wallet to ts_sorted via forward-fill on [start,end] window;
# outside window contribute 0 to incremental PnL.
ts_arr = np.array(ts_sorted, dtype=np.int64)

def deltas_aligned(avh: list[tuple[int, float]]) -> np.ndarray:
    """Scaled-to-leader incremental PnL on ts_arr grid (zero outside coverage)."""
    if not avh:
        return np.zeros(len(ts_arr))
    a_t = np.array([t for t, _ in avh])
    a_v = np.array([v for _, v in avh], dtype=np.float64)
    # For each ts in ts_arr, find the leader account value (forward-fill within coverage)
    idx = np.searchsorted(a_t, ts_arr, side="right") - 1
    in_cov = (ts_arr >= a_t[0]) & (ts_arr <= a_t[-1])
    vals = np.where(idx >= 0, a_v[np.clip(idx, 0, len(a_v)-1)], 0.0)
    vals = np.where(in_cov, vals, np.nan)
    # Incremental: zero outside coverage, leader_value - leader_value_prev within
    pnl_cum = np.where(np.isnan(vals), 0.0, vals - np.nan_to_num(vals[np.argmax(~np.isnan(vals))], nan=0.0))
    # Simpler: cum scaled to start-of-coverage = 0
    first_idx = np.argmax(~np.isnan(vals)) if (~np.isnan(vals)).any() else 0
    base = vals[first_idx] if not np.isnan(vals[first_idx]) else 0.0
    pnl_cum = np.where(np.isnan(vals), 0.0, vals - base)
    # Forward fill across post-coverage gap (account value freezes at last known)
    last_known = 0.0
    out = np.zeros(len(ts_arr))
    seen = False
    for i, v in enumerate(vals):
        if not np.isnan(v):
            seen = True
            last_known = v - base
        if seen:
            out[i] = last_known
    return out


# Build leader-cumPnL arrays for every shortlisted wallet on ts_arr grid
short = short[short.wallet.isin(curves)].reset_index(drop=True)
print(f"with month avh: {len(short)}")

LEADER_CUM = np.zeros((len(short), len(ts_arr)))
for i, w in enumerate(short.wallet):
    LEADER_CUM[i] = deltas_aligned(curves[w])

# Scaled copy-account cumulative PnL = leader_cum × scale
# scale = norm_base / leader_equity_base (parsed from chosen_params)
def parse_scale(params: str) -> float:
    nb = float(params.split("norm_base=")[1].split(",")[0])
    leb = float(params.split("leader_equity_base=")[1])
    return nb / leb

scales = short.chosen_params.apply(parse_scale).to_numpy()
COPY_CUM = LEADER_CUM * scales[:, None]


def joint(idx: list[int]):
    p = COPY_CUM[idx].sum(axis=0)
    rm = np.maximum.accumulate(p)
    mdd = (p - rm).min() if len(p) else 0.0
    total = p[-1] if len(p) else 0.0
    calmar = total / -mdd if mdd < -1 else (total if total > 0 else 0)
    return total, mdd, calmar


# Greedy + swap
print("\n--- greedy ---")
selected: list[int] = []
remaining = set(range(len(short)))
while len(selected) < PORTFOLIO_N:
    best_c = None
    best_score = -np.inf
    for c in remaining:
        trial = selected + [c]
        total, mdd, calmar = joint(trial)
        score = total / (abs(mdd) + 100.0)
        if score > best_score:
            best_score, best_c = score, (c, total, mdd, calmar)
    if best_c is None:
        break
    c, total, mdd, calmar = best_c
    selected.append(c); remaining.discard(c)
    print(f"  +{short.wallet.iloc[c][:10]}  total=${total:,.0f} mdd=${mdd:,.0f} calmar={calmar:.2f}")

print("\n--- swap ---")
improved = True; passes = 0
while improved and passes < 20:
    improved = False; passes += 1
    base = joint(selected)
    base_score = base[0] / (abs(base[1]) + 100.0)
    for i, s in enumerate(selected):
        for c in list(remaining):
            trial = selected.copy(); trial[i] = c
            total, mdd, calmar = joint(trial)
            score = total / (abs(mdd) + 100.0)
            if score > base_score + 1e-6:
                selected[i] = c; remaining.discard(c); remaining.add(s)
                base_score = score
                improved = True
                print(f"  swap {short.wallet.iloc[s][:10]} -> {short.wallet.iloc[c][:10]}  score={score:.2f} total=${total:,.0f} mdd=${mdd:,.0f}")
                break
        if improved: break

total, mdd, calmar = joint(selected)
final = short.iloc[selected].copy().reset_index(drop=True)
final["pick_order"] = range(len(final))
print(f"\n=== JOINT V2 ===")
print(f"total=${total:,.0f}  MDD=${mdd:,.0f}  Calmar={calmar:.2f}")
print(f"peak joint margin (by construction): ≤ ${PER_SLOT_MARGIN*len(final):.0f}")
cols = ["pick_order","wallet","chosen_params","chosen_total","chosen_mdd","chosen_calmar","month_acctV_chg","month_acctV_mdd_mtm","month_acctV_end","n_fills"]
print(final[cols].to_string(index=False))
final.to_csv(OUT / "final_portfolio_v2.csv", index=False)

# Save joint cum PnL
joint_cum = COPY_CUM[selected].sum(axis=0)
pd.DataFrame({"ts": ts_arr, "joint_pnl_cum": joint_cum}).to_parquet(OUT / "joint_timeseries_v2.parquet", index=False)
print("\nsaved final_portfolio_v2.csv + joint_timeseries_v2.parquet")
