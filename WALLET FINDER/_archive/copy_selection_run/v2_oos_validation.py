"""V2 OOS: split the HL month timeline 80/20, re-optimise on first 80%, score on last 20%.

Optimisation = greedy+swap on top-80 shortlist using only IS data; then evaluate the
chosen 10 (and the V1-style 'pick on all data' set) on OOS slice.
"""
from __future__ import annotations
import pandas as pd, numpy as np, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent
PORTF = ROOT / "wallet_portfolios"
PORTFOLIO_N = 10

best = pd.read_csv(OUT / "per_wallet_best_v2.csv")
short = best[(best.chosen_mode == "proportional") & (best.month_acctV_end >= 5000.0)].head(80).reset_index(drop=True)

def load_avh(w):
    p = PORTF / f"{w}.json"
    if not p.exists(): return []
    data = json.loads(p.read_text())
    periods = {x[0]: x[1] for x in data}
    avh = periods.get("month", {}).get("accountValueHistory") or []
    return sorted([(int(t), float(v)) for t, v in avh])

curves = {w: load_avh(w) for w in short.wallet if load_avh(w)}
short = short[short.wallet.isin(curves)].reset_index(drop=True)
all_ts = sorted({t for c in curves.values() for t, _ in c})
ts_arr = np.array(all_ts, dtype=np.int64)
print(f"timeline: {len(ts_arr)} pts, span {(ts_arr.max()-ts_arr.min())/86400000:.1f} days")

# Time-based split: last 7 days OOS (HL "month" data clusters recent points, so
# index-based split gives tiny OOS window).
OOS_DAYS = 7
split_ts = int(ts_arr.max() - OOS_DAYS * 86400000)
split_idx = int(np.searchsorted(ts_arr, split_ts))
print(f"time-based split: last {OOS_DAYS} days OOS (idx {split_idx}/{len(ts_arr)})")

def deltas_aligned(avh):
    a_t = np.array([t for t, _ in avh])
    a_v = np.array([v for _, v in avh], dtype=np.float64)
    idx = np.searchsorted(a_t, ts_arr, side="right") - 1
    in_cov = (ts_arr >= a_t[0]) & (ts_arr <= a_t[-1])
    vals = np.where(idx >= 0, a_v[np.clip(idx, 0, len(a_v)-1)], 0.0)
    vals = np.where(in_cov, vals, np.nan)
    first_i = np.argmax(~np.isnan(vals)) if (~np.isnan(vals)).any() else 0
    base = vals[first_i] if not np.isnan(vals[first_i]) else 0.0
    out = np.zeros(len(ts_arr))
    seen = False; last = 0.0
    for i, v in enumerate(vals):
        if not np.isnan(v):
            seen = True; last = v - base
        if seen: out[i] = last
    return out

LEADER_CUM = np.zeros((len(short), len(ts_arr)))
for i, w in enumerate(short.wallet):
    LEADER_CUM[i] = deltas_aligned(curves[w])

def parse_scale(p):
    nb = float(p.split("norm_base=")[1].split(",")[0])
    leb = float(p.split("leader_equity_base=")[1])
    return nb / leb
scales = short.chosen_params.apply(parse_scale).to_numpy()
COPY = LEADER_CUM * scales[:, None]

def metrics(cumcurve):
    rm = np.maximum.accumulate(cumcurve)
    mdd = (cumcurve - rm).min() if len(cumcurve) else 0.0
    total = cumcurve[-1] - cumcurve[0] if len(cumcurve) else 0.0
    return total, mdd

def joint_metrics(idx, slice_):
    cum = COPY[idx][:, slice_].sum(axis=0)
    return metrics(cum)

# Greedy on IS only
IS = slice(0, split_idx + 1)
OOS = slice(split_idx, len(ts_arr))

selected = []; remaining = set(range(len(short)))
while len(selected) < PORTFOLIO_N:
    best_c, best_score = None, -np.inf
    for c in remaining:
        total, mdd = joint_metrics(selected + [c], IS)
        score = total / (abs(mdd) + 100)
        if score > best_score: best_score, best_c = score, c
    if best_c is None: break
    selected.append(best_c); remaining.discard(best_c)

# swap
improved = True; passes = 0
while improved and passes < 20:
    improved = False; passes += 1
    base_total, base_mdd = joint_metrics(selected, IS)
    base_score = base_total / (abs(base_mdd) + 100)
    for i, s in enumerate(selected):
        for c in list(remaining):
            trial = selected.copy(); trial[i] = c
            total, mdd = joint_metrics(trial, IS)
            score = total / (abs(mdd) + 100)
            if score > base_score + 1e-6:
                selected[i] = c; remaining.discard(c); remaining.add(s)
                base_score = score; improved = True; break
        if improved: break

is_total, is_mdd = joint_metrics(selected, IS)
oos_total, oos_mdd = joint_metrics(selected, OOS)
print(f"\n=== IS (first 80%) ===")
print(f"  total=${is_total:,.0f}  MDD=${is_mdd:,.0f}  Calmar={is_total/-is_mdd if is_mdd<-1 else 'inf':.2f}")
print(f"=== OOS (last 20%, ~{(ts_arr.max()-split_ts)/86400000:.1f} days) ===")
print(f"  total=${oos_total:,.0f}  MDD=${oos_mdd:,.0f}")

# Also evaluate the FULL-DATA-OPTIMISED v2 portfolio (from v2_portfolio.py) on OOS
full_final = pd.read_csv(OUT / "final_portfolio_v2.csv")
short_map = {w: i for i, w in enumerate(short.wallet)}
full_idx = [short_map[w] for w in full_final.wallet if w in short_map]
oos_total_full, oos_mdd_full = joint_metrics(full_idx, OOS)
print(f"\n=== v2_portfolio's choice (in-sample-optimised on FULL month, then scored on OOS slice) ===")
print(f"  OOS total=${oos_total_full:,.0f}  MDD=${oos_mdd_full:,.0f}  (overfit risk indicator)")

# Per-wallet OOS for the v2_portfolio finalists
print(f"\nper-wallet OOS contribution (v2_portfolio choices):")
for w in full_final.wallet:
    if w not in short_map: continue
    i = short_map[w]
    cum = COPY[i, OOS]
    t, m = metrics(cum)
    print(f"  {w[:10]}  OOS_total=${t:>8,.0f}  OOS_mdd=${m:>8,.0f}")

# Save IS-optimised selection
is_selected_df = short.iloc[selected].copy().reset_index(drop=True)
is_selected_df["pick_order"] = range(len(is_selected_df))
is_selected_df.to_csv(OUT / "final_portfolio_v2_IS_only.csv", index=False)
overlap = set(is_selected_df.wallet) & set(full_final.wallet)
print(f"\nIS-only vs full-data picks overlap: {len(overlap)}/{PORTFOLIO_N}")
