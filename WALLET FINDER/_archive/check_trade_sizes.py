"""Quick check: trade size distributions for wallets that couldn't reach 50% capture."""
import pandas as pd, numpy as np
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data"
all_trades = pd.read_csv(DATA / "all_trades.csv", usecols=["wallet", "px", "sz", "closedPnl"])
all_trades["wallet"] = all_trades["wallet"].str.lower()
all_trades["notional"] = np.abs(all_trades["px"] * all_trades["sz"])

# All qualifying wallets
cand = pd.read_csv(DATA / "copyable_wallets.csv")
cand["wallet"] = cand["wallet"].str.lower()
for c in ("mtm_calmar", "equity_collapse_flag_mtm", "negative_total_flag_mtm",
          "month_acctV_end", "trades"):
    cand[c] = pd.to_numeric(cand[c], errors="coerce")
mtm_pass = cand[
    (cand.mtm_calmar >= 1.5) &
    (cand.equity_collapse_flag_mtm == 0) &
    (cand.negative_total_flag_mtm == 0) &
    (cand.month_acctV_end >= 5000) &
    (cand.mtm_source != "unavailable") &
    (cand.trades >= 300)
].copy()

print(f"{'Wallet':<14} {'Trades':>7} {'MedNot':>8} {'P25':>7} {'P75':>8} {'P90':>8}")
print(f"{'':14} {'<12$':>7} {'<24$':>7} {'<50$':>7} {'<100$':>7} {'<200$':>7} {'<500$':>7} {'>1k$':>7}")
print("-" * 100)

rows = []
for cw in mtm_pass.itertuples():
    w = cw.wallet
    wt = all_trades[all_trades.wallet == w]
    if len(wt) < 300:
        continue
    n_arr = wt["notional"].values
    n = len(n_arr)
    med = np.median(n_arr)
    p25 = np.percentile(n_arr, 25)
    p75 = np.percentile(n_arr, 75)
    p90 = np.percentile(n_arr, 90)

    # At what nb does a trade with notional=X hit $12 copy?
    # copy = X * (nb / 10000) >= 12  =>  nb >= 120000 / X
    # So for the median trade: nb_min = 120000 / med
    nb_for_med = 120000 / med if med > 0 else 99999

    # Percentage of trades capturable at various nb values
    caps = {}
    for nb in [50, 100, 200, 500, 1000, 2000, 5000]:
        scale = nb / 10000
        capturable = (n_arr * scale >= 12).sum() / n * 100
        caps[nb] = capturable

    pct_lt12 = (n_arr < 12).sum() / n * 100
    pct_lt24 = (n_arr < 24).sum() / n * 100
    pct_lt50 = (n_arr < 50).sum() / n * 100
    pct_lt100 = (n_arr < 100).sum() / n * 100
    pct_lt200 = (n_arr < 200).sum() / n * 100
    pct_lt500 = (n_arr < 500).sum() / n * 100
    pct_gt1k = (n_arr > 1000).sum() / n * 100

    rows.append({
        "wallet": w[:14], "trades": n, "med": med, "p25": p25, "p75": p75, "p90": p90,
        "nb_for_med": nb_for_med,
        "lt12": pct_lt12, "lt24": pct_lt24, "lt50": pct_lt50,
        "lt100": pct_lt100, "lt200": pct_lt200, "lt500": pct_lt500, "gt1k": pct_gt1k,
        **{f"cap_nb{nb}": caps[nb] for nb in caps},
    })

    print(f"{w[:14]} {n:>7} ${med:>7.0f} ${p25:>6.0f} ${p75:>7.0f} ${p90:>7.0f}  nb4med={nb_for_med:>6.0f}")
    print(f"{'':14} {pct_lt12:>6.1f}% {pct_lt24:>6.1f}% {pct_lt50:>6.1f}% {pct_lt100:>6.1f}% {pct_lt200:>6.1f}% {pct_lt500:>6.1f}% {pct_gt1k:>6.1f}%")

print("\n\nCAPTURE TABLE: % of trades capturable at each norm_base")
print(f"{'Wallet':<14} {'MedNot':>8}", end="")
for nb in [50, 100, 200, 500, 1000, 2000, 5000]:
    print(f"  nb={nb:<5}", end="")
print()
print("-" * 100)

df = pd.DataFrame(rows)
for _, r in df.iterrows():
    print(f"{r.wallet:<14} ${r.med:>7.0f}", end="")
    for nb in [50, 100, 200, 500, 1000, 2000, 5000]:
        v = r[f"cap_nb{nb}"]
        marker = " *" if v >= 50 else "  "
        print(f"  {v:>5.1f}%{marker}", end="")
    print()

# Summary: how many wallets achieve 50% capture at each nb?
print(f"\n\nWALLETS ACHIEVING >=50% CAPTURE:")
for nb in [50, 100, 200, 500, 1000, 2000, 5000]:
    count = (df[f"cap_nb{nb}"] >= 50).sum()
    print(f"  nb={nb:>5}: {count:>3}/{len(df)} wallets")
