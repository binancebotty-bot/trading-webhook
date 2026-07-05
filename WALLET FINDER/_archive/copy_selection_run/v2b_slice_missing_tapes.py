"""V2B: slice all_trades.csv for the wallets in the post-MTM-gate copyable list
that don't already have a cached parquet tape from the earlier V2 pass."""
from __future__ import annotations
import csv
import pandas as pd
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent.parent          # hl_stage2/
OUT = Path(__file__).resolve().parent                  # copy_selection_run/
SHARDS = OUT / "wallet_tapes"
SHARDS.mkdir(exist_ok=True)

COPYABLE = ROOT / "copyable_wallets.csv"
import sys; sys.path.insert(0, str(ROOT))
from trades_path import trades_csv
ALL_TRADES = trades_csv()
WINDOW_END_MS = 1778747921863
WINDOW_START_MS = WINDOW_END_MS - 90 * 86400 * 1000

with COPYABLE.open() as f:
    candidates = {r["wallet"].strip().lower() for r in csv.DictReader(f)}
have = {p.stem.lower() for p in SHARDS.glob("*.parquet")}
missing = candidates - have
print(f"copyable: {len(candidates)}  have_tape: {len(candidates & have)}  missing: {len(missing)}")
if not missing:
    print("nothing to slice"); raise SystemExit(0)

bufs: dict[str, list] = {}
rows_kept = 0; rows_seen = 0; t0 = time.time()
CHUNK = 1_000_000
for i, chunk in enumerate(pd.read_csv(
    ALL_TRADES, chunksize=CHUNK,
    dtype={"wallet": str, "coin": str, "side": str},
)):
    rows_seen += len(chunk)
    chunk["wallet"] = chunk["wallet"].str.lower()
    sub = chunk[chunk["wallet"].isin(missing) & (chunk["time"] >= WINDOW_START_MS)]
    if len(sub):
        rows_kept += len(sub)
        for w, g in sub.groupby("wallet"):
            bufs.setdefault(w, []).append(g)
    if (i + 1) % 5 == 0:
        print(f"  chunk {i+1}: seen={rows_seen:,} kept={rows_kept:,} dt={time.time()-t0:.1f}s")

print(f"writing {len(bufs)} shards...")
for w, parts in bufs.items():
    df = pd.concat(parts, ignore_index=True).sort_values("time")
    df.to_parquet(SHARDS / f"{w}.parquet", index=False)
print(f"done in {time.time()-t0:.1f}s ({rows_kept:,} rows / {len(bufs)} wallets)")
