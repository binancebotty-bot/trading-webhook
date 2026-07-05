"""
build_equity_curves.py — One-shot script: fetch fills + generate equity curves for filtered wallets.

Reads wallets from wallet_universe.csv (produced by Stage 1+1.5 pipeline).
Fetches ALL fills via paginated userFillsByTime (sliding 24h windows to avoid 2000 cap).
Computes equity curves using identical logic to universe_builder.py:
  - FRICTION_BPS = 9.5 applied to every trade's notional
  - Cumulative adjusted PnL = equity
  - Efficiency, score, activity_ratio computed from full trade history

Outputs:
  - data/equity_curves/{wallet}.csv  (ts, equity)
  - data/all_trades.csv              (wallet, time, coin, side, px, sz, closedPnl)
  - data/wallet_universe.csv         (re-written with scores)

This is a ONE-SHOT complement to universe_builder.py, not a replacement.
universe_builder.py remains the continuous incremental engine.
"""
import asyncio
import aiohttp
import csv
import json
import math
import os
import random
import socket
import sys
import time
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent / "data"
UNIVERSE_FILE = DATA_DIR / "wallet_universe.csv"
TRADES_FILE = DATA_DIR / "all_trades.csv"
CURVES_DIR = DATA_DIR / "equity_curves"
PROGRESS_FILE = DATA_DIR / "equity_curves_progress.json"

API_URL = "https://api.hyperliquid.xyz/info"
FRICTION_BPS = 9.5
MS_7D = 7 * 24 * 3_600_000
POLL_WINDOW_MS = 24 * 60 * 60 * 1000  # 24h sliding window
OVERLAP_MS = 5 * 60 * 1000  # 5min overlap to catch edge fills
DEFAULT_LOOKBACK_MS = 365 * 24 * 60 * 60 * 1000  # 1 year (full history)

CONCURRENCY = 15
MAX_RETRIES = 5
RETRY_BASE_WAIT = 1.5
EXTENDED_FAILURE_RETRIES = 3
EXTENDED_FAILURE_COOLDOWN_S = 30
MAX_PAGES = 200  # safety: 200 pages × 2000 fills = 400K fills per wallet

OUTPUT_FIELDS = [
    "wallet", "realised_pnl", "total_notional", "efficiency",
    "trades", "trades_7d", "last_trade_time", "score", "status",
]


def trade_key(fill, wallet=""):
    """Full canonical key matching universe_builder.py."""
    return (
        str(wallet).strip().lower(),
        int(fill.get("time", 0)),
        str(fill.get("coin", "")).strip().upper(),
        str(fill.get("side", "")).strip().lower(),
        str(fill.get("px")),
        str(fill.get("sz")),
        str(fill.get("closedPnl", 0)),
    )


def load_wallets():
    """Load wallets from wallet_universe.csv."""
    wallets = []
    with open(UNIVERSE_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            wallets.append(row["wallet"].strip().lower())
    return wallets


async def post_with_retry(session, payload, wallet, stats):
    """POST with exponential backoff. Returns data or None on API failure."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    wait = RETRY_BASE_WAIT * (2 ** attempt) + random.uniform(0, 1.0)
                    stats["retries"] += 1
                    await asyncio.sleep(wait)
                    continue
                if resp.status in (500, 502, 503, 504):
                    wait = RETRY_BASE_WAIT * (2 ** attempt)
                    stats["retries"] += 1
                    await asyncio.sleep(wait)
                    continue
                stats["http_errors"] += 1
                return None
        except (asyncio.TimeoutError, OSError, ConnectionError):
            wait = RETRY_BASE_WAIT * (2 ** attempt)
            stats["retries"] += 1
            await asyncio.sleep(wait)
            continue
        except Exception:
            stats["http_errors"] += 1
            return None
    return None


async def post_with_extended_recovery(session, payload, wallet, stats):
    """Return API data, [] for a true empty window, or None for API failure."""
    data = await post_with_retry(session, payload, wallet, stats)
    if data is not None:
        return data
    for attempt in range(EXTENDED_FAILURE_RETRIES):
        wait = EXTENDED_FAILURE_COOLDOWN_S * (attempt + 1)
        stats["extended_retries"] = stats.get("extended_retries", 0) + 1
        print(f"[{wallet[:10]}] API failure; retrying same window after {wait}s", flush=True)
        await asyncio.sleep(wait)
        data = await post_with_retry(session, payload, wallet, stats)
        if data is not None:
            return data
    return None


async def fetch_all_fills(session, wallet, sem, stats):
    """Fetch ALL fills using sliding 24h windows (matching universe_builder.py logic).

    This avoids the 2000 fill cap by polling in 24h windows from oldest to now.
    Each window returns at most 2000 fills. Wallets with >2000 fills/day are
    extremely rare (would need >46 fills/minute sustained).
    """
    async with sem:
        seen_keys = set()
        all_fills = []
        now_ms = int(time.time() * 1000)
        window_start = now_ms - DEFAULT_LOOKBACK_MS
        pages = 0

        while window_start < now_ms:
            window_end = min(window_start + POLL_WINDOW_MS, now_ms)
            payload = {
                "type": "userFillsByTime",
                "user": wallet,
                "startTime": window_start,
                "endTime": window_end,
            }
            data = await post_with_extended_recovery(session, payload, wallet, stats)

            if data is None:
                stats["suspected_truncated"] = stats.get("suspected_truncated", 0) + 1
                if pages > 0:
                    stats["fetched"] += 1
                    stats["total_pages"] = stats.get("total_pages", 0) + pages
                else:
                    stats["failed"] += 1
                return {
                    "fills": all_fills,
                    "suspected_truncated": 1,
                    "termination_reason": "api_failure",
                }

            if isinstance(data, list) and data:
                pages += 1
                for fill in data:
                    if not isinstance(fill, dict):
                        continue
                    key = trade_key(fill, wallet)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        all_fills.append(fill)

            window_start = window_end
            if window_start < now_ms:
                await asyncio.sleep(0.15)

        if pages > 0:
            stats["fetched"] += 1
            stats["total_pages"] = stats.get("total_pages", 0) + pages
        else:
            stats["failed"] += 1

        return {
            "fills": all_fills,
            "suspected_truncated": 0,
            "termination_reason": "ok",
        } if all_fills else None


def compute_wallet_metrics(wallet, fills):
    """Compute equity curve and metrics using identical logic to universe_builder.py."""
    if not fills:
        return None

    # Sort by time
    fills.sort(key=lambda f: int(f.get("time", 0)))

    friction_rate = FRICTION_BPS / 10_000.0
    max_ts = max(int(f.get("time", 0)) for f in fills)
    cutoff_7d = max_ts - MS_7D

    realised_pnl = 0.0
    total_notional = 0.0
    close_count = 0
    trades_7d = 0
    equity_curve = []

    for f in fills:
        try:
            px = float(f.get("px", 0) or 0)
            sz = float(f.get("sz", 0) or 0)
            raw_pnl = float(f.get("closedPnl", 0) or 0)
            ts = int(f.get("time", 0))
        except (ValueError, TypeError):
            continue

        notional = px * sz
        friction_cost = notional * friction_rate
        total_notional += notional

        if raw_pnl != 0.0:
            adj_pnl = raw_pnl - friction_cost
            realised_pnl += adj_pnl
            close_count += 1
            equity_curve.append((ts, round(realised_pnl, 6)))
        else:
            realised_pnl -= friction_cost

        if ts >= cutoff_7d:
            trades_7d += 1

    if close_count == 0 or total_notional <= 0:
        return None

    efficiency = realised_pnl / total_notional
    activity_ratio = min(trades_7d / max(close_count, 1), 1.0)
    score = efficiency * activity_ratio * math.log(max(close_count, 2))
    status = "active" if trades_7d > 0 else "inactive"

    return {
        "wallet": wallet,
        "realised_pnl": round(realised_pnl, 4),
        "total_notional": round(total_notional, 2),
        "efficiency": round(efficiency, 8),
        "trades": close_count,
        "trades_7d": trades_7d,
        "last_trade_time": max_ts,
        "score": round(score, 8),
        "status": status,
        "_equity_curve": equity_curve,
    }


def write_equity_curve(wallet, curve):
    """Write equity curve CSV."""
    if not curve:
        return
    path = CURVES_DIR / f"{wallet}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "equity"])
        writer.writerows(curve)


def save_progress(stats, done, total, passed):
    """Save intermediate progress."""
    PROGRESS_FILE.write_text(json.dumps({
        "timestamp": time.time(),
        "stats": stats,
        "done": done,
        "total": total,
        "passed": passed,
    }, indent=2), encoding="utf-8")


async def main():
    print("=" * 70)
    print("BUILD EQUITY CURVES (one-shot for filtered wallets)")
    print("=" * 70)

    # Load wallets
    print("\nLoading wallets from wallet_universe.csv...")
    wallets = load_wallets()
    print(f"  {len(wallets)} wallets to process")

    # Create equity_curves directory
    os.makedirs(CURVES_DIR, exist_ok=True)

    # Stats
    stats = {"fetched": 0, "retries": 0, "http_errors": 0, "failed": 0}
    results = []
    errors = []

    # Fetch fills
    print(f"\nFetching fills (concurrency={CONCURRENCY}, window={POLL_WINDOW_MS // 3_600_000}h)...")
    sem = asyncio.Semaphore(CONCURRENCY)
    resolver = aiohttp.resolver.ThreadedResolver()
    connector = aiohttp.TCPConnector(
        resolver=resolver,
        family=socket.AF_INET,
        limit=CONCURRENCY,
        limit_per_host=CONCURRENCY,
        ttl_dns_cache=300,
    )

    BATCH = 200
    fills_data = {}  # wallet -> list of fills
    suspected_truncated = []

    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(wallets), BATCH):
            batch = wallets[i : i + BATCH]
            tasks = [fetch_all_fills(session, w, sem, stats) for w in batch]
            fills_results = await asyncio.gather(*tasks)

            for w, fetch_result in zip(batch, fills_results):
                if fetch_result and fetch_result.get("fills"):
                    fills_data[w] = fetch_result["fills"]
                    if fetch_result.get("suspected_truncated"):
                        suspected_truncated.append({
                            "wallet": w,
                            "reason": fetch_result.get("termination_reason", "api_failure"),
                            "fills_kept": len(fetch_result["fills"]),
                        })

            done = min(i + BATCH, len(wallets))
            pct = done / len(wallets) * 100
            print(
                f"  Fetched {done}/{len(wallets)} ({pct:.0f}%) "
                f"| ok={stats['fetched']} fail={stats['failed']} "
                f"retries={stats['retries']} pages={stats.get('total_pages', 0)} "
                f"truncated={len(suspected_truncated)}",
                flush=True,
            )

            save_progress(stats, done, len(wallets), len(fills_data))

    # Compute metrics and equity curves
    print(f"\nComputing metrics + equity curves for {len(fills_data)} wallets...")
    skipped = 0
    for wallet, fills in fills_data.items():
        metrics = compute_wallet_metrics(wallet, fills)
        if metrics is None:
            skipped += 1
            continue
        # Filter out losing wallets
        if metrics["realised_pnl"] <= 0:
            skipped += 1
            continue
        # Write equity curve
        write_equity_curve(wallet, metrics["_equity_curve"])
        # Remove internal field before storing
        del metrics["_equity_curve"]
        results.append(metrics)

    print(f"  Metrics computed: {len(results)}, skipped (losing/no data): {skipped}")

    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)

    # Write all_trades.csv
    print(f"\nWriting all_trades.csv...")
    all_rows = []
    for wallet, fills in fills_data.items():
        for fill in fills:
            all_rows.append({
                "wallet": wallet,
                "time": fill.get("time"),
                "coin": fill.get("coin", ""),
                "side": fill.get("side", ""),
                "px": fill.get("px"),
                "sz": fill.get("sz"),
                "closedPnl": fill.get("closedPnl", 0),
            })
    all_rows.sort(key=lambda r: (r["wallet"], int(r["time"] or 0)))

    with open(TRADES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["wallet", "time", "coin", "side", "px", "sz", "closedPnl"])
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"  Wrote {len(all_rows)} fills to {TRADES_FILE}")

    # Rewrite wallet_universe.csv with scores
    print(f"\nRewriting wallet_universe.csv with scores...")
    with open(UNIVERSE_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)
    print(f"  Wrote {len(results)} wallets to {UNIVERSE_FILE}")

    # Summary
    print(f"\n{'=' * 70}")
    print(f"RESULTS")
    print(f"{'=' * 70}")
    print(f"  Input wallets: {len(wallets)}")
    print(f"  Wallets with fills: {len(fills_data)}")
    print(f"  Wallets with metrics: {len(results)}")
    print(f"  Wallets skipped: {skipped}")
    print(f"  Equity curves written: {len(results)}")
    print(f"  Total fills in all_trades.csv: {len(all_rows)}")
    if suspected_truncated:
        print(f"  Suspected truncated wallets: {len(suspected_truncated)}")
        for item in suspected_truncated[:10]:
            print(f"    {item['wallet']}: {item['reason']} after {item['fills_kept']} fills")

    if results:
        scores = [r["score"] for r in results]
        pnls = [r["realised_pnl"] for r in results]
        trades = [r["trades"] for r in results]
        print(f"\n  Score distribution:")
        print(f"    Median: {sorted(scores)[len(scores)//2]:.8f}")
        print(f"    Top 5:")
        for i, r in enumerate(results[:5], 1):
            print(
                f"      {i}. {r['wallet'][:15]}... "
                f"score={r['score']:.6f} pnl=${r['realised_pnl']:,.2f} "
                f"trades={r['trades']} 7d={r['trades_7d']}"
            )

    save_progress(stats, len(wallets), len(wallets), len(results))
    print(f"\nDone. Equity curves: {CURVES_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
