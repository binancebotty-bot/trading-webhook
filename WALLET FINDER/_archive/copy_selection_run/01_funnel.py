"""Stage 1 of wallet-selection pipeline: safety-prune summary.csv -> candidate list."""
from __future__ import annotations
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent

summary = pd.read_csv(ROOT / "summary.csv")
print(f"summary.csv loaded: {len(summary)} wallets")

flag_cols = [
    "martingale_flag", "one_big_trade_flag", "equity_collapse_flag",
    "suspected_truncated", "short_span_flag", "negative_total_flag",
]
for c in flag_cols:
    if c not in summary.columns:
        summary[c] = 0

before = len(summary)
# NOTE: martingale_flag is set to 1 on 2694/2704 wallets — clearly a broken
# default in the upstream scanner. Excluded from the filter. equity_collapse_flag
# is discriminating (~40% positive) so it stays as a hard drop.
mask = (
    (summary["one_big_trade_flag"] == 0)
    & (summary["equity_collapse_flag"] == 0)
    & (summary["suspected_truncated"] == 0)
    & (summary["short_span_flag"] == 0)
    & (summary["negative_total_flag"] == 0)
    & (summary["trades"] >= 200)
    & (summary["trades_7d"] >= 50)
    & (summary["timespan_hours"] >= 72)
    & (summary["total_pnl"] > 0)
)
cand = summary[mask].copy()
print(f"after safety+activity prune: {len(cand)} (dropped {before - len(cand)})")

# Tag whether wallet appears in the live-copyable set (closer-to-live signal)
copyable = pd.read_csv(ROOT / "copyable_wallets.csv")
cand["in_copyable"] = cand["wallet"].isin(copyable["wallet"]).astype(int)
print(f"  of which in copyable_wallets.csv: {int(cand['in_copyable'].sum())}")

# Light pre-rank for downstream report (NOT used to drop — replay decides)
cand["calmar_proxy"] = cand["total_pnl"] / cand["max_drawdown"].abs().clip(lower=1.0)
cand = cand.sort_values("calmar_proxy", ascending=False)

cand.to_csv(OUT / "candidates.csv", index=False)
print(f"wrote {OUT / 'candidates.csv'}")
print(cand[["wallet", "total_pnl", "max_drawdown", "trades", "trades_7d", "in_copyable", "calmar_proxy"]].head(15).to_string(index=False))
