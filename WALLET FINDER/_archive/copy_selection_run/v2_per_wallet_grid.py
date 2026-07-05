"""V2 per-wallet grid: $12 min copy notional + MTM MDD from HL API.

For each candidate:
  - Load trade tape (already cached).
  - Compute leader stats from tape: median fill notional, peak open notional, peak margin.
  - PROPORTIONAL path (preferred, MTM-truthful):
      min_scale  = $12 / median_fill_notional   (else most fills below HL min)
      max_scale  = PER_SLOT_MARGIN / peak_margin (else breaches slot budget)
      feasible iff min_scale <= max_scale
      copy_total  = month_acctV_chg     * chosen_scale    (FROM HL API)
      copy_mdd    = month_acctV_mdd_mtm * chosen_scale    (FROM HL API)
      Calmar invariant; pick chosen_scale = max_scale (maximises return)
  - FIXED path (fallback / cross-check):
      for fn in {25, 50, 100}: trade-replay; track realised PnL, realised MDD, peak margin.
      Calmar uses REALISED MDD only (no per-coin price tape → no MTM here).
      Mark output with dd_source="realised" so it's not mixed with proportional.
  - Choose per-wallet mode by: prefer proportional if feasible AND its Calmar (MTM) is
    competitive; otherwise fixed.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
import time
from leverage_table import lev

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent
SHARDS = OUT / "wallet_tapes"

MIN_COPY_NOTIONAL = 12.0
PER_SLOT_MARGIN = 450.0    # 1/10 of $4500 budget
FIXED_GRID = [25, 50, 100]  # $5 and $10 dropped — below live minimum


def replay_fixed(df: pd.DataFrame, fn: float):
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    side = df["side"].to_numpy()
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    coin = df["coin"].to_numpy()
    leader_notional = np.abs(px * sz)
    leader_notional = np.where(leader_notional > 0, leader_notional, 1e-9)

    size_ratio = fn / leader_notional
    copy_pnl = closed * size_ratio
    copy_size = np.abs(sz) * size_ratio
    signed_size = np.where(side == "B", copy_size, -copy_size)

    positions: dict[str, float] = {}
    total_margin = 0.0
    coin_lev = {c: lev(c) for c in set(coin.tolist())}
    peak_margin = 0.0
    for i in range(len(df)):
        c = coin[i]
        prev = positions.get(c, 0.0)
        new = prev + signed_size[i]
        delta_n = (abs(new) - abs(prev)) * px[i]
        total_margin += delta_n / coin_lev[c]
        positions[c] = new
        if total_margin > peak_margin:
            peak_margin = total_margin
    equity = np.cumsum(copy_pnl)
    rm = np.maximum.accumulate(equity) if len(equity) else np.array([0.0])
    mdd_realised = (equity - rm).min() if len(equity) else 0.0
    return float(equity[-1] if len(equity) else 0), float(mdd_realised), float(peak_margin)


def replay_leader_margin(df: pd.DataFrame):
    """Compute peak open notional/margin for leader-sized fills (scale=1).
    Used to derive proportional max_scale."""
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    side = df["side"].to_numpy()
    coin = df["coin"].to_numpy()
    signed_size = np.where(side == "B", np.abs(sz), -np.abs(sz))
    positions: dict[str, float] = {}
    total_margin = 0.0
    coin_lev = {c: lev(c) for c in set(coin.tolist())}
    peak_margin = 0.0
    peak_notional = 0.0
    total_notional = 0.0
    for i in range(len(df)):
        c = coin[i]
        prev = positions.get(c, 0.0)
        new = prev + signed_size[i]
        delta_n = (abs(new) - abs(prev)) * px[i]
        total_notional += delta_n
        total_margin += delta_n / coin_lev[c]
        positions[c] = new
        if total_margin > peak_margin: peak_margin = total_margin
        if total_notional > peak_notional: peak_notional = total_notional
    return float(peak_margin), float(peak_notional)


cand = pd.read_csv(OUT / "candidates_v2.csv")
print(f"V2 candidates: {len(cand)}")

rows = []
t0 = time.time()
skipped = 0
for n, cw in enumerate(cand.itertuples()):
    w = cw.wallet
    p = SHARDS / f"{w}.parquet"
    if not p.exists():
        skipped += 1
        continue
    df = pd.read_parquet(p).sort_values("time").reset_index(drop=True)
    leader_n = (df["px"].abs() * df["sz"].abs())
    df = df[leader_n >= 5.0].reset_index(drop=True)
    if len(df) < 30:
        skipped += 1
        continue
    leader_notional = (df["px"].abs() * df["sz"].abs()).to_numpy()
    median_fill = float(np.median(leader_notional))
    leader_peak_margin, leader_peak_notional = replay_leader_margin(df)

    # ---- PROPORTIONAL feasibility ----
    min_scale_for_min_notional = MIN_COPY_NOTIONAL / median_fill if median_fill > 0 else np.inf
    max_scale_for_slot_margin = (PER_SLOT_MARGIN / leader_peak_margin) if leader_peak_margin > 0 else np.inf
    prop_feasible = min_scale_for_min_notional <= max_scale_for_slot_margin
    if prop_feasible:
        chosen_scale = max_scale_for_slot_margin
        prop_total_mtm = cw.month_acctV_chg * chosen_scale
        prop_mdd_mtm = cw.month_acctV_mdd_mtm * chosen_scale
        prop_calmar_mtm = cw.mtm_calmar  # invariant
        prop_peak_margin = PER_SLOT_MARGIN
        # Equivalent (norm_base, leb): want notional ratio = chosen_scale; default leb=10000 → nb = scale*leb
        prop_norm_base = round(chosen_scale * 10000.0, 1)
        prop_leb = 10000.0
    else:
        prop_total_mtm = prop_mdd_mtm = 0.0
        prop_calmar_mtm = 0.0
        prop_peak_margin = 0.0
        prop_norm_base = None
        prop_leb = None

    # ---- FIXED grid ----
    best_fixed = None
    for fn in FIXED_GRID:
        total_f, mdd_f, peak_m = replay_fixed(df, fn)
        # peak margin under fixed mode — must fit slot
        if peak_m > PER_SLOT_MARGIN:
            # scale down: factor = PER_SLOT_MARGIN / peak_m -- but reducing fixed_notional below 25 is illegal.
            # so fixed wallets with too-big peak margin at fn=25 are infeasible
            if fn == FIXED_GRID[0]:
                # try only if it's the smallest -- mark infeasible
                pass
            continue
        calmar_f = total_f / abs(mdd_f) if mdd_f < -1 else (total_f if total_f > 0 else 0)
        score = total_f / (abs(mdd_f) + 50.0)
        if best_fixed is None or score > best_fixed["score"]:
            best_fixed = {"fn": fn, "total": total_f, "mdd_realised": mdd_f,
                          "peak_margin": peak_m, "calmar_realised": calmar_f, "score": score}

    # ---- Choose mode ----
    if prop_feasible:
        chosen_mode = "proportional"
        chosen_calmar = prop_calmar_mtm
        chosen_total = prop_total_mtm
        chosen_mdd = prop_mdd_mtm
        chosen_mdd_source = "mtm"
        chosen_peak_margin = prop_peak_margin
        chosen_params = f"norm_base={prop_norm_base},leader_equity_base={prop_leb}"
    elif best_fixed:
        chosen_mode = "fixed"
        chosen_calmar = best_fixed["calmar_realised"]
        chosen_total = best_fixed["total"]
        chosen_mdd = best_fixed["mdd_realised"]
        chosen_mdd_source = "realised"
        chosen_peak_margin = best_fixed["peak_margin"]
        chosen_params = f"fixed_notional={best_fixed['fn']}"
    else:
        chosen_mode = "INFEASIBLE"
        chosen_calmar = chosen_total = chosen_mdd = chosen_peak_margin = 0.0
        chosen_mdd_source = "n/a"
        chosen_params = ""

    rows.append({
        "wallet": w,
        "n_fills": len(df),
        "median_fill_usd": median_fill,
        "leader_peak_margin": leader_peak_margin,
        "min_scale_for_min_notional": min_scale_for_min_notional,
        "max_scale_for_slot": max_scale_for_slot_margin,
        "prop_feasible": prop_feasible,
        "prop_total_mtm": prop_total_mtm,
        "prop_mdd_mtm": prop_mdd_mtm,
        "prop_calmar_mtm": prop_calmar_mtm,
        "prop_norm_base": prop_norm_base,
        "prop_leb": prop_leb,
        "fixed_best_fn": best_fixed["fn"] if best_fixed else None,
        "fixed_total_realised": best_fixed["total"] if best_fixed else 0.0,
        "fixed_mdd_realised": best_fixed["mdd_realised"] if best_fixed else 0.0,
        "fixed_calmar_realised": best_fixed["calmar_realised"] if best_fixed else 0.0,
        "fixed_peak_margin": best_fixed["peak_margin"] if best_fixed else 0.0,
        "chosen_mode": chosen_mode,
        "chosen_params": chosen_params,
        "chosen_total": chosen_total,
        "chosen_mdd": chosen_mdd,
        "chosen_calmar": chosen_calmar,
        "chosen_mdd_source": chosen_mdd_source,
        "chosen_peak_margin": chosen_peak_margin,
        # carry MTM API stats for reporting
        "month_acctV_chg": cw.month_acctV_chg,
        "month_acctV_mdd_mtm": cw.month_acctV_mdd_mtm,
        "month_acctV_end": cw.month_acctV_end,
    })
    if (n + 1) % 50 == 0:
        print(f"  {n+1}/{len(cand)}  dt={time.time()-t0:.1f}s")

out = pd.DataFrame(rows)
out["chosen_score"] = out.chosen_total / (out.chosen_mdd.abs() + 50)
out = out.sort_values("chosen_score", ascending=False).reset_index(drop=True)
out.to_csv(OUT / "per_wallet_best_v2.csv", index=False)
print(f"\ndone in {time.time()-t0:.1f}s, {len(out)} wallets, skipped {skipped}")
print(f"prop_feasible: {out.prop_feasible.sum()}/{len(out)}")
print(f"chosen_mode breakdown: {out.chosen_mode.value_counts().to_dict()}")
print(f"\ntop 20 by chosen_score:")
cols = ["wallet","chosen_mode","chosen_params","chosen_total","chosen_mdd","chosen_calmar","chosen_mdd_source","chosen_score"]
print(out[cols].head(20).to_string(index=False))
