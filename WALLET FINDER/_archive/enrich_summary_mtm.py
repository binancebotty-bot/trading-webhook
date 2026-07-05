"""
enrich_summary_mtm.py — WALLET FINDER EDITION

Take an existing realised-only data/summary.csv and produce a
schema-current data/summary_enriched.csv by joining MTM truth
from cached wallet_portfolios/<wallet>.json.
"""
from __future__ import annotations
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from hl_mtm_lookup import get_mtm_stats, MTM_OUTPUT_COLUMNS

SUMMARY_IN = HERE / "data" / "summary.csv"
SUMMARY_OUT = HERE / "data" / "summary_enriched.csv"
ALL_TRADES = HERE / "data" / "all_trades.csv"

HL_TAKER_FEE_PER_SIDE = 0.00035
ROUND_TRIP_FEE = HL_TAKER_FEE_PER_SIDE * 2.0


def load_total_notional_by_wallet() -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    if not ALL_TRADES.exists():
        print(f"WARN: {ALL_TRADES} missing; fee columns will be blank.")
        return {}
    with ALL_TRADES.open(encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for i, row in enumerate(rdr):
            try:
                w = (row.get("wallet") or "").strip().lower()
                px = float(row.get("px") or 0); sz = float(row.get("sz") or 0)
                out[w] += abs(px * sz)
            except Exception:
                continue
            if (i + 1) % 2_000_000 == 0:
                print(f"  scanned {i+1:,} trades...")
    return dict(out)


def main():
    if not SUMMARY_IN.exists():
        print(f"ERR: {SUMMARY_IN} missing")
        sys.exit(1)

    print(f"loading {SUMMARY_IN}...")
    with SUMMARY_IN.open(encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        rows = list(rdr)
        in_cols = list(rdr.fieldnames or [])
    print(f"  {len(rows)} rows, {len(in_cols)} existing columns")

    print("scanning all_trades.csv for fee/notional estimates...")
    notional_by_wallet = load_total_notional_by_wallet()
    print(f"  {len(notional_by_wallet):,} wallets with trade history")

    fee_cols = ["total_notional", "est_fee_drag", "total_pnl_net_fees_est"]
    out_cols = list(in_cols)
    for c in fee_cols + MTM_OUTPUT_COLUMNS:
        if c not in out_cols:
            out_cols.append(c)

    print(f"writing enriched -> {SUMMARY_OUT}  ({len(out_cols)} cols)")
    n_mtm_ok = 0; n_mtm_missing = 0
    with SUMMARY_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_cols)
        w.writeheader()
        for r in rows:
            wallet = (r.get("wallet") or "").strip().lower()
            tn = notional_by_wallet.get(wallet)
            if tn is not None:
                fees = round(tn * ROUND_TRIP_FEE, 4)
                total_pnl = float(r.get("total_pnl") or 0)
                r["total_notional"] = round(tn, 2)
                r["est_fee_drag"] = fees
                r["total_pnl_net_fees_est"] = round(total_pnl - fees, 4)
            mtm = get_mtm_stats(wallet)
            for k in MTM_OUTPUT_COLUMNS:
                v = mtm.get(k)
                r[k] = "" if v is None else v
            src = mtm.get("mtm_source")
            if src == "hl_portfolio_api":
                n_mtm_ok += 1
            else:
                n_mtm_missing += 1
            w.writerow(r)

    print(f"\ndone. MTM coverage:")
    print(f"  with MTM data:   {n_mtm_ok:,}/{len(rows):,}")
    print(f"  missing MTM:     {n_mtm_missing:,}/{len(rows):,}")
    print(f"output: {SUMMARY_OUT}")


if __name__ == "__main__":
    main()
