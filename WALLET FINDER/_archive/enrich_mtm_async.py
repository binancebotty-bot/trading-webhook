"""
enrich_mtm_async.py — WALLET FINDER EDITION

Adds real MTM drawdown columns (from Hyperliquid accountValueHistory) to
data/summary.csv using async concurrent requests. One API call per wallet
at 50 concurrent = ~30s for 2700 wallets vs ~22min synchronous.
"""
import asyncio
import aiohttp
import csv
import os
import socket
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from hl_mtm_lookup import get_mtm_stats_async, MTM_OUTPUT_COLUMNS

SUMMARY_PATH = HERE / "data" / "summary.csv"
CONCURRENCY = 50
API_URL = "https://api.hyperliquid.xyz/info"


async def main():
    if not SUMMARY_PATH.exists():
        print(f"ERR: {SUMMARY_PATH} not found")
        return

    # Load existing rows
    print(f"Loading {SUMMARY_PATH}...")
    with SUMMARY_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_cols = list(reader.fieldnames or [])
        rows = list(reader)
    print(f"  {len(rows)} rows, {len(in_cols)} columns")

    # Build output column list — add MTM columns if missing
    out_cols = list(in_cols)
    for c in MTM_OUTPUT_COLUMNS:
        if c not in out_cols:
            out_cols.append(c)

    # Normalise wallet keys
    wallet_list = []
    for r in rows:
        w = (r.get("wallet") or "").strip().lower()
        wallet_list.append(w)

    # Async MTM fetch with semaphore
    sem = asyncio.Semaphore(CONCURRENCY)
    resolver = aiohttp.resolver.ThreadedResolver()
    connector = aiohttp.TCPConnector(
        resolver=resolver, limit=CONCURRENCY, limit_per_host=CONCURRENCY,
        ttl_dns_cache=300, family=socket.AF_INET,
    )

    mtm_results = {}
    done = 0
    errors = 0
    start = time.time()

    async def fetch_one(session, wallet):
        nonlocal done, errors
        async with sem:
            try:
                mtm = await get_mtm_stats_async(session, wallet)
            except Exception:
                mtm = {k: None for k in MTM_OUTPUT_COLUMNS}
                mtm["mtm_source"] = "error"
                errors += 1
        done += 1
        if done % 100 == 0 or done == len(rows):
            elapsed = time.time() - start
            rate = done / elapsed if elapsed > 0 else 0
            print(f"  {done}/{len(rows)} ({rate:.0f}/s) errors={errors}", flush=True)
        return wallet, mtm

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_one(session, w) for w in wallet_list]
        all_results = await asyncio.gather(*tasks)
        for wallet, mtm in all_results:
            mtm_results[wallet] = mtm

    elapsed = time.time() - start
    print(f"\nFetched MTM for {len(mtm_results)} wallets in {elapsed:.1f}s")

    # Write enriched CSV
    n_mtm_ok = 0
    n_mtm_missing = 0
    tmp_path = str(SUMMARY_PATH) + ".tmp"

    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_cols)
        writer.writeheader()
        for r, w in zip(rows, wallet_list):
            mtm = mtm_results.get(w, {})
            for k in MTM_OUTPUT_COLUMNS:
                v = mtm.get(k)
                r[k] = "" if v is None else v
            src = mtm.get("mtm_source", "unknown")
            if src == "hl_portfolio_api":
                n_mtm_ok += 1
            else:
                n_mtm_missing += 1
            writer.writerow(r)

    os.replace(tmp_path, str(SUMMARY_PATH))

    print(f"\nDone. Wrote {len(rows)} rows ({len(out_cols)} cols)")
    print(f"  MTM from API:  {n_mtm_ok}")
    print(f"  No MTM data:   {n_mtm_missing}")
    print(f"Output: {SUMMARY_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
