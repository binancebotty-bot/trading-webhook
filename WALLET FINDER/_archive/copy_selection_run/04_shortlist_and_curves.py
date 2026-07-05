"""Shortlist top-N candidates and emit per-wallet timeseries.

Ranking uses *regularised* Calmar: total / (abs(mdd) + floor) where floor=$50.
Filters:
  - chosen_total >= 200  (>=4% return on $5k -- material)
  - abs(chosen_mdd) >= 5  (some real drawdown signal, not artifact)
  - chosen_peak_margin <= 4500 (already-margin-feasible per wallet)

Emits one parquet per shortlisted wallet:
  best_curves/<wallet>.parquet  with columns: time, copy_pnl, copy_notional_signed (per-coin notional later)
We store fill-level copy_pnl plus rolling open_notional_total and margin_used_total.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
from leverage_table import lev

OUT = Path(__file__).resolve().parent
SHARDS = OUT / "wallet_tapes"
CURVES = OUT / "best_curves"
CURVES.mkdir(exist_ok=True)

SHORTLIST_N = 80
MDD_FLOOR = 50.0
MIN_TOTAL = 200.0
MIN_MDD_ABS = 5.0
MARGIN_BUDGET = 4500.0
PER_SLOT_MARGIN = 450.0  # 4500 / 10 -- target a 10-wallet portfolio

best = pd.read_csv(OUT / "per_wallet_best.csv")
print(f"per_wallet_best rows: {len(best)}")

# Regularise Calmar
best["reg_calmar"] = best["chosen_total"] / (best["chosen_mdd"].abs() + MDD_FLOOR)

filt = best[
    (best["chosen_total"] >= MIN_TOTAL)
    & (best["chosen_mdd"].abs() >= MIN_MDD_ABS)
    & (best["chosen_peak_margin"] <= MARGIN_BUDGET)
].copy()
print(f"after filter: {len(filt)}")

filt = filt.sort_values("reg_calmar", ascending=False).head(SHORTLIST_N).reset_index(drop=True)
filt.to_csv(OUT / "shortlist.csv", index=False)
print(f"shortlist top {len(filt)}:")
print(filt[["wallet","chosen_mode","chosen_params","chosen_total","chosen_mdd","chosen_calmar","reg_calmar","n_fills"]].head(20).to_string(index=False))


def emit_curve(wallet: str, mode: str, params: str, target_margin: float):
    """Rescale params so that this wallet's peak margin == target_margin.
       Returns the (possibly rescaled) params actually used."""
    df = pd.read_parquet(SHARDS / f"{wallet}.parquet")
    df = df.sort_values("time").reset_index(drop=True)
    leader_notional_full = (df["px"].abs() * df["sz"].abs())
    df = df[leader_notional_full >= 5.0].reset_index(drop=True)

    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    side = df["side"].to_numpy()
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    coin = df["coin"].to_numpy()
    leader_notional = np.abs(px * sz)
    leader_notional = np.where(leader_notional > 0, leader_notional, 1e-9)

    if mode == "proportional":
        nb = float(params.split("norm_base=")[1].split(",")[0])
        leb = float(params.split("leader_equity_base=")[1])
        copy_notional_per_fill = leader_notional * (nb / leb)
    else:
        fn = float(params.split("fixed_notional=")[1])
        copy_notional_per_fill = np.full_like(leader_notional, fn)

    size_ratio = copy_notional_per_fill / leader_notional
    copy_pnl = closed * size_ratio
    copy_size = np.abs(sz) * size_ratio
    signed_size = np.where(side == "B", copy_size, -copy_size)

    positions: dict[str, float] = {}
    open_notional_total = np.zeros(len(df))
    margin_used = np.zeros(len(df))
    coin_lev = {c: lev(c) for c in set(coin.tolist())}
    total_n = 0.0
    total_m = 0.0
    coin_n: dict[str, float] = {}
    for i in range(len(df)):
        c = coin[i]
        prev = positions.get(c, 0.0)
        new = prev + signed_size[i]
        prev_n = abs(prev) * px[i]
        new_n = abs(new) * px[i]
        total_n += (new_n - prev_n)
        total_m += (new_n - prev_n) / coin_lev[c]
        positions[c] = new
        coin_n[c] = new_n
        open_notional_total[i] = total_n
        margin_used[i] = total_m

    # Rescale to target margin: find peak_margin at current scale, then apply factor
    peak = margin_used.max() if len(margin_used) else 0.0
    factor = (target_margin / peak) if peak > 1e-9 else 1.0
    # Cap at factor=1.0 (don't scale UP beyond initial sizing -- our initial choice
    # was already the wallet's max-Calmar config; only scale DOWN if it eats too much margin)
    factor = min(factor, 1.0)
    copy_pnl_s = copy_pnl * factor
    open_notional_s = open_notional_total * factor
    margin_used_s = margin_used * factor

    out = pd.DataFrame({
        "time": df["time"].to_numpy(),
        "copy_pnl": copy_pnl_s,
        "open_notional": open_notional_s,
        "margin_used": margin_used_s,
    })
    out.to_parquet(CURVES / f"{wallet}.parquet", index=False)

    # Rescaled summary
    eq = np.cumsum(copy_pnl_s)
    rm = np.maximum.accumulate(eq) if len(eq) else np.array([0.0])
    mdd = (eq - rm).min() if len(eq) else 0.0
    return {
        "scaled_total": float(eq[-1]) if len(eq) else 0.0,
        "scaled_mdd": float(mdd),
        "scaled_peak_margin": float(margin_used_s.max()) if len(margin_used_s) else 0.0,
        "scale_factor": float(factor),
    }


scaled_stats = []
for i, row in filt.iterrows():
    s = emit_curve(row["wallet"], row["chosen_mode"], row["chosen_params"], PER_SLOT_MARGIN)
    s["wallet"] = row["wallet"]
    scaled_stats.append(s)
sdf = pd.DataFrame(scaled_stats)
filt = filt.merge(sdf, on="wallet", how="left")
filt["scaled_calmar"] = filt["scaled_total"] / (filt["scaled_mdd"].abs() + MDD_FLOOR)
filt = filt.sort_values("scaled_calmar", ascending=False).reset_index(drop=True)
filt.to_csv(OUT / "shortlist.csv", index=False)
print(f"\nrescaled shortlist top 20:")
print(filt[["wallet","chosen_mode","chosen_params","scaled_total","scaled_mdd","scaled_peak_margin","scale_factor","scaled_calmar"]].head(20).to_string(index=False))
print(f"emitted {len(filt)} per-wallet curves at target margin ${PER_SLOT_MARGIN:.0f}")
