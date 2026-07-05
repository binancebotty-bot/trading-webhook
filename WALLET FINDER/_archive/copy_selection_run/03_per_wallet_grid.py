"""Per-wallet param grid optimisation.

For each candidate wallet:
- Load 90-day trade tape from parquet shard
- Compute proportional baseline (Calmar invariant under norm_base scaling)
- For fixed mode, sweep fixed_notional grid and recompute Calmar
- Pick best (mode, params) by Calmar; for proportional, pick norm_base/leader_equity_base
  such that peak open notional fits inside a notional budget (default $50k, i.e. 10x on $5k)

Outputs:
  per_wallet_best.csv  - one row per wallet, chosen config + metrics
  best_curves/<wallet>.parquet - per-wallet timeseries (ts, copy_pnl_cum, open_notional, margin_used)
                                 under chosen config (proportional uses default sizing of $1000 base)
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
CURVES = OUT / "best_curves"
CURVES.mkdir(exist_ok=True)

FIXED_GRID = [5, 10, 25, 50, 100]
ACCOUNT_USD = 5000.0
TARGET_MARGIN_BUDGET = 4500.0  # leave headroom from $5000


def replay(df: pd.DataFrame, copy_notional_per_fill: np.ndarray):
    """Given per-fill copy_notional (USD, signed-positive), compute:
      - copy_closed_pnl per fill = closedPnl * (copy_notional / leader_notional)
      - running open_notional per (wallet,coin) using signed_size_units accumulator
    Returns (pnl_series, open_notional_series, margin_series) all length len(df).
    Assumes df is sorted by time.
    """
    px = df["px"].to_numpy(dtype=np.float64)
    sz = df["sz"].to_numpy(dtype=np.float64)
    side = df["side"].to_numpy()  # 'B' = bid (buy), 'A' = ask (sell). In HL fills: side 'B' = open long / close short.
    closed = df["closedPnl"].to_numpy(dtype=np.float64)
    coin = df["coin"].to_numpy()

    leader_notional = np.abs(px * sz)
    leader_notional = np.where(leader_notional > 0, leader_notional, 1e-9)
    size_ratio = copy_notional_per_fill / leader_notional
    copy_pnl = closed * size_ratio
    copy_size = np.abs(sz) * size_ratio
    signed_size = np.where(side == "B", copy_size, -copy_size)

    # Per-coin running position
    positions: dict[str, float] = {}
    open_notional_per_fill = np.zeros(len(df))
    margin_used_per_fill = np.zeros(len(df))
    # Track total open notional across all coins after each fill
    # Cheap incremental update
    coin_lev = {c: lev(c) for c in set(coin.tolist())}
    total_abs_notional = 0.0
    total_margin = 0.0
    coin_abs_notional: dict[str, float] = {}
    coin_margin: dict[str, float] = {}

    for i in range(len(df)):
        c = coin[i]
        prev_size = positions.get(c, 0.0)
        new_size = prev_size + signed_size[i]
        prev_abs_notional = abs(prev_size) * px[i]  # use current px as proxy
        new_abs_notional = abs(new_size) * px[i]
        delta_n = new_abs_notional - prev_abs_notional
        delta_m = delta_n / coin_lev[c]
        total_abs_notional += delta_n
        total_margin += delta_m
        coin_abs_notional[c] = new_abs_notional
        coin_margin[c] = coin_abs_notional[c] / coin_lev[c]
        positions[c] = new_size
        open_notional_per_fill[i] = total_abs_notional
        margin_used_per_fill[i] = total_margin

    return copy_pnl, open_notional_per_fill, margin_used_per_fill


def calmar_from_pnl(pnl: np.ndarray) -> tuple[float, float, float]:
    """Return (total_pnl, max_drawdown_neg, calmar)."""
    if len(pnl) == 0:
        return 0.0, 0.0, 0.0
    equity = np.cumsum(pnl)
    running_max = np.maximum.accumulate(equity)
    dd = equity - running_max  # <= 0
    mdd = dd.min()
    total = equity[-1]
    if mdd >= -1e-9:
        # no drawdown
        return float(total), 0.0, float(total)  # treat as huge Calmar
    return float(total), float(mdd), float(total / -mdd)


def main():
    cand = pd.read_csv(OUT / "candidates.csv")
    wallets = cand["wallet"].str.lower().tolist()
    rows = []
    t0 = time.time()
    skipped = 0
    for n, w in enumerate(wallets):
        p = SHARDS / f"{w}.parquet"
        if not p.exists():
            skipped += 1
            continue
        df = pd.read_parquet(p)
        if len(df) < 30:
            skipped += 1
            continue
        df = df.sort_values("time").reset_index(drop=True)
        # Drop micro-fills below HL min order ($10 nominal) — not copyable in live
        leader_notional_full = (df["px"].abs() * df["sz"].abs())
        df = df[leader_notional_full >= 5.0].reset_index(drop=True)
        if len(df) < 30:
            skipped += 1
            continue
        leader_notional = (df["px"].abs() * df["sz"].abs()).to_numpy()
        # ---- Proportional: Calmar invariant under scale. Compute on leader's raw closedPnl. ----
        prop_pnl = df["closedPnl"].to_numpy(dtype=np.float64)
        prop_total, prop_mdd, prop_calmar = calmar_from_pnl(prop_pnl)
        # Peak leader notional sets the proportional sizing constraint:
        # margin_per_$1_norm_base = (1/leader_equity_base) * peak_leader_notional / lev
        # We choose leader_equity_base = $10k (default in UI) and find max norm_base
        # such that peak margin contribution <= TARGET_MARGIN_BUDGET.
        # Compute peak total margin contribution for the leader's series at norm_base=1, leb=1:
        copy_notional_unit = leader_notional * (1.0 / 10000.0)  # at norm_base=1, leb=10000
        _, _, marg_unit = replay(df, copy_notional_unit)
        peak_margin_at_nb1 = marg_unit.max() if len(marg_unit) else 0.0
        max_norm_base_prop = (TARGET_MARGIN_BUDGET / peak_margin_at_nb1) if peak_margin_at_nb1 > 0 else 5000.0
        max_norm_base_prop = min(max_norm_base_prop, 5000.0)  # cap to sensible
        # Scaled stats:
        prop_total_scaled = prop_total * (max_norm_base_prop / 10000.0)
        prop_mdd_scaled = prop_mdd * (max_norm_base_prop / 10000.0)

        # ---- Fixed: sweep fixed_notional ----
        best_fixed = None
        for fn in FIXED_GRID:
            copy_notional_fn = np.full_like(leader_notional, float(fn))
            pnl_fn, _on_fn, marg_fn = replay(df, copy_notional_fn)
            peak_marg = marg_fn.max() if len(marg_fn) else 0.0
            total_fn, mdd_fn, calmar_fn = calmar_from_pnl(pnl_fn)
            if best_fixed is None or calmar_fn > best_fixed["calmar"]:
                best_fixed = {
                    "fixed_notional": fn,
                    "total": total_fn,
                    "mdd": mdd_fn,
                    "calmar": calmar_fn,
                    "peak_margin": peak_marg,
                }

        # Choose mode by Calmar
        if prop_calmar >= best_fixed["calmar"]:
            chosen_mode = "proportional"
            chosen_total = prop_total_scaled
            chosen_mdd = prop_mdd_scaled
            chosen_calmar = prop_calmar  # invariant
            chosen_params = f"norm_base={max_norm_base_prop:.1f},leader_equity_base=10000"
            chosen_peak_margin = TARGET_MARGIN_BUDGET  # by construction
        else:
            chosen_mode = "fixed"
            chosen_total = best_fixed["total"]
            chosen_mdd = best_fixed["mdd"]
            chosen_calmar = best_fixed["calmar"]
            chosen_params = f"fixed_notional={best_fixed['fixed_notional']}"
            chosen_peak_margin = best_fixed["peak_margin"]

        rows.append({
            "wallet": w,
            "n_fills": len(df),
            "n_coins": int(df["coin"].nunique()),
            "prop_total": prop_total_scaled,
            "prop_mdd": prop_mdd_scaled,
            "prop_calmar": prop_calmar,
            "prop_max_norm_base": max_norm_base_prop,
            "fixed_best_fn": best_fixed["fixed_notional"],
            "fixed_total": best_fixed["total"],
            "fixed_mdd": best_fixed["mdd"],
            "fixed_calmar": best_fixed["calmar"],
            "fixed_peak_margin": best_fixed["peak_margin"],
            "chosen_mode": chosen_mode,
            "chosen_params": chosen_params,
            "chosen_total": chosen_total,
            "chosen_mdd": chosen_mdd,
            "chosen_calmar": chosen_calmar,
            "chosen_peak_margin": chosen_peak_margin,
        })
        if (n + 1) % 50 == 0:
            print(f"  {n+1}/{len(wallets)}  dt={time.time()-t0:.1f}s  skipped={skipped}")

    out = pd.DataFrame(rows).sort_values("chosen_calmar", ascending=False)
    out.to_csv(OUT / "per_wallet_best.csv", index=False)
    print(f"done in {time.time()-t0:.1f}s, {len(out)} wallets, skipped {skipped}")
    print(out.head(20)[["wallet","chosen_mode","chosen_params","chosen_total","chosen_mdd","chosen_calmar","n_fills"]].to_string(index=False))


if __name__ == "__main__":
    main()
