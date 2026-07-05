"""V2B full pipeline — re-runs candidate funnel + grid + portfolio + OOS on the
post-MTM-gate copyable_wallets.csv (664 wallets). The new pool surfaces 577
wallets the legacy biased gate had been rejecting.

Outputs in copy_selection_run/:
  candidates_v2b.csv
  per_wallet_best_v2b.csv
  final_portfolio_v2b.csv
  FINAL_10_WALLETS_V2B.csv
  oos_v2b.json
"""
from __future__ import annotations
import csv
import json
import sys
import time
import numpy as np
import pandas as pd
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SCANNER = ROOT.parent
SHARDS = HERE / "wallet_tapes"
PORTF = ROOT / "wallet_portfolios"

sys.path.insert(0, str(SCANNER))
from hl_mtm_lookup import get_mtm_stats  # noqa
# leverage table is local to copy_selection_run
sys.path.insert(0, str(HERE))
from leverage_table import lev  # noqa

# Config (same as V2)
MIN_COPY_NOTIONAL = 12.0
PER_SLOT_MARGIN = 450.0
PORTFOLIO_N = 10
SHORTLIST_N = 80
MARGIN_BUDGET = 4500.0
FIXED_GRID = [25, 50, 100]
LEADER_MIN_ACCT = 5000.0


# ─── STEP 1: candidates from copyable_wallets.csv + MTM stats ────────────
def build_candidates() -> pd.DataFrame:
    # copyable_wallets.csv already carries MTM columns from the patched gate.
    cand = pd.read_csv(ROOT / "copyable_wallets.csv")
    cand["wallet"] = cand["wallet"].str.lower()
    # Rename to v2-grid naming convention
    cand = cand.rename(columns={
        "month_pnl_chg_mtm": "month_acctV_chg",
        "max_drawdown_mtm": "month_acctV_mdd_mtm",
    })
    # Coerce numerics (CSV reads them as strings if "")
    for c in ("month_acctV_chg", "month_acctV_mdd_mtm", "month_acctV_end",
              "mtm_calmar", "equity_collapse_flag_mtm"):
        cand[c] = pd.to_numeric(cand[c], errors="coerce")
    before = len(cand)
    cand = cand[
        cand.month_acctV_chg.notna()
        & (cand.month_acctV_chg > 200)
        & (cand.month_acctV_mdd_mtm < -50)
        & (cand.month_acctV_end >= LEADER_MIN_ACCT)
        & (cand.equity_collapse_flag_mtm == 0)
    ].copy()
    print(f"build_candidates: {before} -> {len(cand)} (after leader_account>=${LEADER_MIN_ACCT}, etc.)")
    cand.to_csv(HERE / "candidates_v2b.csv", index=False)
    return cand


# ─── STEP 2: per-wallet grid (proportional + fixed) ──────────────────────
def replay_leader_margin(df):
    px = df.px.to_numpy(np.float64); sz = df.sz.to_numpy(np.float64)
    side = df.side.to_numpy(); coin = df.coin.to_numpy()
    signed = np.where(side == "B", np.abs(sz), -np.abs(sz))
    positions: dict[str, float] = {}
    total_m = 0.0; peak_m = 0.0; total_n = 0.0; peak_n = 0.0
    coin_lev = {c: lev(c) for c in set(coin.tolist())}
    for i in range(len(df)):
        c = coin[i]; prev = positions.get(c, 0.0); new = prev + signed[i]
        delta_n = (abs(new) - abs(prev)) * px[i]
        total_n += delta_n; total_m += delta_n / coin_lev[c]
        positions[c] = new
        if total_m > peak_m: peak_m = total_m
        if total_n > peak_n: peak_n = total_n
    return float(peak_m), float(peak_n)


def replay_fixed(df, fn: float):
    px = df.px.to_numpy(np.float64); sz = df.sz.to_numpy(np.float64)
    side = df.side.to_numpy(); coin = df.coin.to_numpy()
    closed = df.closedPnl.to_numpy(np.float64)
    leader_n = np.abs(px * sz); leader_n = np.where(leader_n > 0, leader_n, 1e-9)
    ratio = fn / leader_n
    pnl = closed * ratio
    copy_size = np.abs(sz) * ratio
    signed = np.where(side == "B", copy_size, -copy_size)
    positions: dict[str, float] = {}
    total_m = 0.0; peak_m = 0.0
    coin_lev = {c: lev(c) for c in set(coin.tolist())}
    for i in range(len(df)):
        c = coin[i]; prev = positions.get(c, 0.0); new = prev + signed[i]
        delta_n = (abs(new) - abs(prev)) * px[i]
        total_m += delta_n / coin_lev[c]; positions[c] = new
        if total_m > peak_m: peak_m = total_m
    eq = np.cumsum(pnl)
    rm = np.maximum.accumulate(eq) if len(eq) else np.array([0.0])
    mdd = (eq - rm).min() if len(eq) else 0.0
    return float(eq[-1] if len(eq) else 0), float(mdd), float(peak_m)


def per_wallet_grid(cand: pd.DataFrame) -> pd.DataFrame:
    rows = []
    t0 = time.time(); skipped = 0
    for n, cw in enumerate(cand.itertuples()):
        w = cw.wallet
        p = SHARDS / f"{w}.parquet"
        if not p.exists():
            skipped += 1; continue
        df = pd.read_parquet(p).sort_values("time").reset_index(drop=True)
        lf = (df.px.abs() * df.sz.abs())
        df = df[lf >= 5.0].reset_index(drop=True)
        if len(df) < 30:
            skipped += 1; continue
        leader_notional = (df.px.abs() * df.sz.abs()).to_numpy()
        median_fill = float(np.median(leader_notional))
        peak_m, _ = replay_leader_margin(df)

        # Proportional
        min_scale = MIN_COPY_NOTIONAL / median_fill if median_fill > 0 else np.inf
        max_scale = (PER_SLOT_MARGIN / peak_m) if peak_m > 0 else np.inf
        prop_feas = min_scale <= max_scale
        if prop_feas:
            scale = max_scale
            prop_total_mtm = cw.month_acctV_chg * scale
            prop_mdd_mtm = cw.month_acctV_mdd_mtm * scale
            prop_norm_base = round(scale * 10000.0, 1)
        else:
            prop_total_mtm = prop_mdd_mtm = prop_norm_base = 0.0

        # Fixed
        best_fixed = None
        for fn in FIXED_GRID:
            tot, mdd, pm = replay_fixed(df, fn)
            if pm > PER_SLOT_MARGIN: continue
            cal = tot / abs(mdd) if mdd < -1 else (tot if tot > 0 else 0)
            score = tot / (abs(mdd) + 50.0)
            if best_fixed is None or score > best_fixed["score"]:
                best_fixed = {"fn": fn, "total": tot, "mdd": mdd, "peak_m": pm, "calmar": cal, "score": score}

        if prop_feas:
            mode = "proportional"
            chosen_params = f"norm_base={prop_norm_base},leader_equity_base=10000.0"
            chosen_total = prop_total_mtm; chosen_mdd = prop_mdd_mtm
            chosen_calmar = cw.mtm_calmar; chosen_pm = PER_SLOT_MARGIN
            mdd_source = "mtm"
        elif best_fixed:
            mode = "fixed"
            chosen_params = f"fixed_notional={best_fixed['fn']}"
            chosen_total = best_fixed["total"]; chosen_mdd = best_fixed["mdd"]
            chosen_calmar = best_fixed["calmar"]; chosen_pm = best_fixed["peak_m"]
            mdd_source = "realised"
        else:
            mode = "INFEASIBLE"
            chosen_params = ""
            chosen_total = chosen_mdd = chosen_calmar = chosen_pm = 0.0
            mdd_source = "n/a"

        rows.append({
            "wallet": w, "n_fills": len(df), "median_fill_usd": median_fill,
            "leader_peak_margin": peak_m, "prop_feasible": prop_feas,
            "chosen_mode": mode, "chosen_params": chosen_params,
            "chosen_total": chosen_total, "chosen_mdd": chosen_mdd,
            "chosen_calmar": chosen_calmar, "chosen_peak_margin": chosen_pm,
            "chosen_mdd_source": mdd_source,
            "month_acctV_chg": cw.month_acctV_chg,
            "month_acctV_mdd_mtm": cw.month_acctV_mdd_mtm,
            "month_acctV_end": cw.month_acctV_end,
            "mtm_calmar": cw.mtm_calmar,
        })
        if (n + 1) % 100 == 0:
            print(f"  grid {n+1}/{len(cand)}  dt={time.time()-t0:.1f}s skipped={skipped}")

    out = pd.DataFrame(rows)
    out["chosen_score"] = out.chosen_total / (out.chosen_mdd.abs() + 50)
    out = out.sort_values("chosen_score", ascending=False).reset_index(drop=True)
    out.to_csv(HERE / "per_wallet_best_v2b.csv", index=False)
    print(f"grid done in {time.time()-t0:.1f}s  candidates={len(out)} skipped={skipped}")
    print(f"  prop_feasible: {out.prop_feasible.sum()}/{len(out)}")
    print(f"  chosen modes: {out.chosen_mode.value_counts().to_dict()}")
    return out


# ─── STEP 3: portfolio selection on joint MTM timeline ───────────────────
def load_avh(w):
    p = PORTF / f"{w}.json"
    if not p.exists(): return []
    data = json.loads(p.read_text())
    periods = {x[0]: x[1] for x in data}
    avh = periods.get("month", {}).get("accountValueHistory") or []
    return sorted([(int(t), float(v)) for t, v in avh])


def select_portfolio(best: pd.DataFrame):
    short = best[best.chosen_mode == "proportional"].head(SHORTLIST_N).reset_index(drop=True)
    print(f"shortlist (proportional): {len(short)}")
    curves = {w: load_avh(w) for w in short.wallet if load_avh(w)}
    short = short[short.wallet.isin(curves)].reset_index(drop=True)
    all_ts = sorted({t for c in curves.values() for t, _ in c})
    ts_arr = np.array(all_ts, dtype=np.int64)
    print(f"timeline: {len(ts_arr)} pts, span {(ts_arr.max()-ts_arr.min())/86400000:.1f} days")

    def deltas_aligned(avh):
        a_t = np.array([t for t, _ in avh])
        a_v = np.array([v for _, v in avh], dtype=np.float64)
        idx = np.searchsorted(a_t, ts_arr, side="right") - 1
        in_cov = (ts_arr >= a_t[0]) & (ts_arr <= a_t[-1])
        vals = np.where(idx >= 0, a_v[np.clip(idx, 0, len(a_v)-1)], 0.0)
        vals = np.where(in_cov, vals, np.nan)
        first_i = np.argmax(~np.isnan(vals)) if (~np.isnan(vals)).any() else 0
        base = vals[first_i] if not np.isnan(vals[first_i]) else 0.0
        out = np.zeros(len(ts_arr)); seen = False; last = 0.0
        for i, v in enumerate(vals):
            if not np.isnan(v): seen = True; last = v - base
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

    def joint(idx, slice_=None):
        """For OOS-style sub-slices, total = cum[end] - cum[start] (slice delta).
        MDD is the worst peak-to-trough WITHIN the slice (running max reset)."""
        s = slice(None) if slice_ is None else slice_
        cum = COPY[idx][:, s].sum(axis=0)
        if not len(cum): return 0.0, 0.0
        # Rebase to start of slice so it represents PnL accrued within the window.
        base = cum[0]
        rebased = cum - base
        rm = np.maximum.accumulate(rebased); mdd = (rebased - rm).min()
        total = rebased[-1]
        return float(total), float(mdd)

    # Greedy
    selected: list[int] = []; remaining = set(range(len(short)))
    print("\ngreedy build:")
    while len(selected) < PORTFOLIO_N:
        best_c, best_score = None, -np.inf
        for c in remaining:
            tot, mdd = joint(selected + [c])
            score = tot / (abs(mdd) + 100.0)
            if score > best_score: best_score, best_c = score, c
        if best_c is None: break
        selected.append(best_c); remaining.discard(best_c)
        tot, mdd = joint(selected)
        cal = tot / -mdd if mdd < -1 else 0
        print(f"  +{short.wallet.iloc[best_c][:10]}  n={len(selected)} total=${tot:,.0f} mdd=${mdd:,.0f} calmar={cal:.1f}")

    # Swap improve
    improved = True; passes = 0
    while improved and passes < 20:
        improved = False; passes += 1
        base_t, base_m = joint(selected); base_score = base_t / (abs(base_m) + 100.0)
        for i, s in enumerate(selected):
            for c in list(remaining):
                trial = selected.copy(); trial[i] = c
                t, m = joint(trial); score = t / (abs(m) + 100.0)
                if score > base_score + 1e-6:
                    selected[i] = c; remaining.discard(c); remaining.add(s)
                    base_score = score; improved = True
                    print(f"  swap {short.wallet.iloc[s][:10]} -> {short.wallet.iloc[c][:10]}  score={score:.2f}")
                    break
            if improved: break

    tot, mdd = joint(selected)
    print(f"\nJOINT V2B: total=${tot:,.0f}  MDD=${mdd:,.0f}  Calmar={tot/-mdd if mdd<-1 else 0:.2f}")

    final = short.iloc[selected].copy().reset_index(drop=True)
    final["pick_order"] = range(len(final))
    final.to_csv(HERE / "final_portfolio_v2b.csv", index=False)

    # OOS: last 7 days
    OOS_DAYS = 7
    split_ts = int(ts_arr.max() - OOS_DAYS * 86400000)
    split_idx = int(np.searchsorted(ts_arr, split_ts))
    IS = slice(0, split_idx + 1); OOS = slice(split_idx, len(ts_arr))
    is_t, is_m = joint(selected, IS); oos_t, oos_m = joint(selected, OOS)
    print(f"\nIS  (~{split_idx/len(ts_arr)*100:.0f}% of timeline): total=${is_t:,.0f}  MDD=${is_m:,.0f}")
    print(f"OOS (last {OOS_DAYS} days):                total=${oos_t:,.0f}  MDD=${oos_m:,.0f}")

    oos_summary = {
        "candidates": int(len(short)),
        "joint_total_full": float(tot),
        "joint_mdd_full": float(mdd),
        "joint_calmar_full": float(tot / -mdd) if mdd < -1 else None,
        "IS_total": float(is_t), "IS_mdd": float(is_m),
        "OOS_total": float(oos_t), "OOS_mdd": float(oos_m),
        "OOS_days": OOS_DAYS,
    }
    (HERE / "oos_v2b.json").write_text(json.dumps(oos_summary, indent=2))
    return final, oos_summary


# ─── STEP 4: final deliverable CSV ────────────────────────────────────────
def emit_final(final: pd.DataFrame):
    def parse(row):
        nb = float(row.chosen_params.split("norm_base=")[1].split(",")[0])
        leb = float(row.chosen_params.split("leader_equity_base=")[1])
        return pd.Series({
            "live_copy_mode": "proportional",
            "live_norm_base": round(nb, 1),
            "live_leader_equity_base": leb,
        })
    final = final.copy()
    final[["live_copy_mode", "live_norm_base", "live_leader_equity_base"]] = final.apply(parse, axis=1)
    cols = [
        "pick_order", "wallet", "live_copy_mode", "live_norm_base", "live_leader_equity_base",
        "chosen_total", "chosen_mdd", "chosen_calmar",
        "month_acctV_chg", "month_acctV_mdd_mtm", "month_acctV_end",
        "n_fills",
    ]
    final[cols].sort_values("pick_order").to_csv(HERE / "FINAL_10_WALLETS_V2B.csv", index=False)
    print(f"\nFinal V2B portfolio written:")
    print(final[cols].sort_values("pick_order").to_string(index=False))


if __name__ == "__main__":
    cand = build_candidates()
    best = per_wallet_grid(cand)
    final, oos = select_portfolio(best)
    emit_final(final)
