"""Slice all_trades.csv into per-wallet parquet shards (90-day window)."""
from __future__ import annotations
import pandas as pd
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent
SHARDS = OUT / "wallet_tapes"
SHARDS.mkdir(exist_ok=True)

cand = pd.read_csv(OUT / "candidates.csv", usecols=["wallet"])
candidate_set = set(cand.wallet.str.lower())
print(f"candidates: {len(candidate_set)}")

# 90-day window relative to tape end (last_ts observed: 2026-05-14)
WINDOW_END_MS = 1778747921863
WINDOW_START_MS = WINDOW_END_MS - 90 * 86400 * 1000
print(f"window: {WINDOW_START_MS} .. {WINDOW_END_MS}")

bufs: dict[str, list] = {}
rows_kept = 0
rows_seen = 0
t0 = time.time()

CHUNK = 1_000_000
import sys; sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from trades_path import trades_csv
for i, chunk in enumerate(pd.read_csv(
    trades_csv(),
    chunksize=CHUNK,
    dtype={"wallet": str, "coin": str, "side": str},
)):
    rows_seen += len(chunk)
    chunk["wallet"] = chunk["wallet"].str.lower()
    sub = chunk[
        chunk["wallet"].isin(candidate_set)
        & (chunk["time"] >= WINDOW_START_MS)
    ]
    if len(sub):
        rows_kept += len(sub)
        for w, g in sub.groupby("wallet"):
            bufs.setdefault(w, []).append(g)
    print(f"  chunk {i}: seen={rows_seen:,} kept_total={rows_kept:,} dt={time.time()-t0:.1f}s")

print(f"writing {len(bufs)} per-wallet shards...")
for w, parts in bufs.items():
    df = pd.concat(parts, ignore_index=True).sort_values("time")
    df.to_parquet(SHARDS / f"{w}.parquet", index=False)

print(f"done in {time.time()-t0:.1f}s, {rows_kept:,} rows across {len(bufs)} wallets")
