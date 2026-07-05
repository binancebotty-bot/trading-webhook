"""
stage1_simple.py — The ONLY filter stage before the deep-dive trade count check.

Conditions (all from portfolio cache, zero new API calls for cached wallets):
  1. Max drawdown < 40% of account peak (MTM allTime — same source as UI)
  2. Traded in the last 7 days
  3. Profitable: perpAllTime realised PnL > 0
  4. Account age > 30 days (existed long enough to prove itself)
  5. Profit >= drawdown: realised PnL covers the worst MTM allTime drawdown

DD is computed via hl_mtm_lookup._summarise() which reads
allTime.accountValueHistory (accountValueHistory includes unrealised PnL,
funding, fees — the SAME source the 8012 UI uses for its DD display).
This ensures wallets that fail PnL/Real DD >= 1.0 in the UI are
rejected here, not after wasting compute.

Wallets failing condition 2 are skipped without API calls.
Wallets failing other conditions are rejected after reading the cache.

After this stage, surviving wallets go to stage1.5_trade_count.py
which fetches fills to enforce the 500-trade minimum.

Outputs: data/simple_filter_pass.csv
"""
import asyncio
import aiohttp
import csv
import json
import os
import socket
import sys
import time
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent / "data"
PORT_DIR = DATA_DIR / "wallet_portfolios"
SOURCE_FILE = DATA_DIR / "hl_wallets_filtered.csv"
OUTPUT_FILE = DATA_DIR / "simple_filter_pass.csv"
PROGRESS_FILE = DATA_DIR / "stage1_simple_progress.json"

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
DD_THRESHOLD = 0.40          # 40% max drawdown
RECENT_DAYS = 7              # must have traded in last 7 days
MIN_ACCOUNT_AGE_DAYS = 30    # account must be at least this old
CONCURRENCY = 15             # concurrent API requests
MAX_RETRIES = 5              # per-wallet retries
RETRY_BASE_WAIT = 1.0        # seconds, doubles each retry up to 8s

MS_7D = RECENT_DAYS * 86400 * 1000  # 7 days in milliseconds


def load_source_wallets():
    """Load all wallets from hl_wallets_filtered.csv."""
    wallets = {}
    with open(SOURCE_FILE, newline="") as f:
        for row in csv.DictReader(f):
            w = row["wallet"].strip().lower()
            wallets[w] = {
                "last_seen": float(row.get("last_seen", 0) or 0),
                "trade_count": int(row.get("trade_count", 0) or 0),
            }
    return wallets


def read_cache(wallet):
    """Read cached portfolio JSON. Returns list or None."""
    p = PORT_DIR / f"{wallet}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_cache(wallet, data):
    """Write portfolio data to cache."""
    PORT_DIR.mkdir(parents=True, exist_ok=True)
    p = PORT_DIR / f"{wallet}.json"
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    tmp.replace(p)


def extract_wallet_stats(portfolio_data):
    """Extract all filtering stats from portfolio cache.

    DD is computed from allTime.accountValueHistory via hl_mtm_lookup._summarise()
    — the SAME source the 8012 UI uses. This ensures consistency:
    wallets that would show PnL/DD < 1.0 in the UI are rejected here.

    PnL is perpAllTime realised PnL (perp-only, matching what the UI displays).

    Returns dict with:
      max_dd_pct:    float (0.0-1.0) — max drawdown as fraction of allTime peak
      peak_value:    float — allTime peak account value (MTM)
      end_value:     float — latest account value (MTM)
      realised_pnl:  float — total realised PnL (perpAllTime final value)
      account_age_days: float — days since first data point
      abs_dd:        float — absolute dollar drawdown (MTM allTime)
      pnl_dd_ratio:  float — realised_pnl / abs_dd (>1 means profit covers DD)

    Returns None if insufficient data for any filter.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from hl_mtm_lookup import _summarise

    now = time.time()

    # ── MTM allTime DD — the authoritative drawdown source ──
    # This is the same computation the 8012 UI uses via _overlay_cached_mtm()
    mtm = _summarise(portfolio_data)
    alltime_dd = mtm.get("allTime_max_drawdown_mtm")  # negative value
    alltime_peak = mtm.get("allTime_acctV_peak")  # peak account value

    if alltime_dd is None or alltime_peak is None or alltime_peak <= 0:
        return None

    # alltime_dd is negative (peak-to-trough drop)
    if alltime_dd >= 0:
        # No drawdown or invalid — treat as 0 DD
        abs_dd = 0.0
        max_dd_pct = 0.0
    else:
        abs_dd = abs(alltime_dd)
        max_dd_pct = abs_dd / alltime_peak

    # ── Realised PnL from perpAllTime (perp-only, matching UI display) ──
    realised_pnl = None
    account_age_days = None
    for item in portfolio_data:
        if isinstance(item, list) and len(item) >= 2 and item[0] == "perpAllTime":
            inner = item[1]
            # PnL: last entry in pnlHistory
            pnl_hist = inner.get("pnlHistory", [])
            if pnl_hist:
                last_point = pnl_hist[-1]
                if isinstance(last_point, list) and len(last_point) >= 2:
                    try:
                        realised_pnl = float(last_point[1])
                    except (ValueError, TypeError):
                        pass
            # Account age: first entry timestamp
            acct_hist = inner.get("accountValueHistory", [])
            if acct_hist:
                first_point = acct_hist[0]
                if isinstance(first_point, list) and len(first_point) >= 2:
                    try:
                        first_ts = float(first_point[0]) / 1000  # ms → seconds
                        account_age_days = (now - first_ts) / 86400
                    except (ValueError, TypeError):
                        pass
            break

    if realised_pnl is None or account_age_days is None:
        return None

    pnl_dd_ratio = realised_pnl / abs_dd if abs_dd > 0 else float("inf")

    return {
        "max_dd_pct": max_dd_pct,
        "peak_value": alltime_peak,
        "end_value": mtm.get("allTime_pnl_chg_mtm", 0) + alltime_peak if alltime_peak else 0,
        "realised_pnl": realised_pnl,
        "account_age_days": account_age_days,
        "abs_dd": abs_dd,
        "pnl_dd_ratio": pnl_dd_ratio,
    }


async def fetch_portfolio(session, wallet, sem, stats):
    """Fetch portfolio data for one wallet with retries. Returns dict or None."""
    async with sem:
        for attempt in range(MAX_RETRIES):
            try:
                async with session.post(
                    HL_INFO_URL,
                    json={"type": "portfolio", "user": wallet},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        write_cache(wallet, data)
                        stats["fetched"] += 1
                        return data
                    if resp.status in (429, 500, 502, 503, 504):
                        wait = min(2 ** attempt, 8)
                        stats["retries"] += 1
                        await asyncio.sleep(wait)
                        continue
                    # Non-transient error
                    stats["http_errors"] += 1
                    return None
            except (asyncio.TimeoutError, OSError, ConnectionError):
                wait = min(2 ** attempt, 8)
                stats["retries"] += 1
                await asyncio.sleep(wait)
                continue
            except Exception:
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                stats["http_errors"] += 1
                return None

        stats["failed"] += 1
        return None


def save_progress(results, stats):
    """Save intermediate progress so we can resume."""
    progress = {
        "timestamp": time.time(),
        "stats": stats,
        "passed_count": len(results),
    }
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2), encoding="utf-8")


async def main():
    now = time.time()
    cutoff_ts = now - (RECENT_DAYS * 86400)

    # ── Load all 121K wallets ──
    print("Loading source wallets...", flush=True)
    all_wallets = load_source_wallets()
    print(f"  Total wallets: {len(all_wallets)}", flush=True)

    # ── Condition 2 first: filter to recent traders (zero API cost) ──
    recent_wallets = {
        w: info for w, info in all_wallets.items()
        if info["last_seen"] >= cutoff_ts
    }
    not_recent = len(all_wallets) - len(recent_wallets)
    print(f"  Traded in last {RECENT_DAYS} days: {len(recent_wallets)}", flush=True)
    print(f"  Not recent (skipped): {not_recent}", flush=True)

    # ── Split recent wallets: cached vs need fetch ──
    cached_wallets = []
    need_fetch = []
    for w in recent_wallets:
        cached = read_cache(w)
        if cached is not None:
            cached_wallets.append((w, cached))
        else:
            need_fetch.append(w)

    print(f"  Already cached: {len(cached_wallets)}", flush=True)
    print(f"  Need API fetch: {len(need_fetch)}", flush=True)

    # ── Fetch missing portfolios ──
    stats = {
        "fetched": 0,
        "retries": 0,
        "http_errors": 0,
        "failed": 0,
    }

    if need_fetch:
        sem = asyncio.Semaphore(CONCURRENCY)
        resolver = aiohttp.resolver.ThreadedResolver()
        connector = aiohttp.TCPConnector(
            resolver=resolver, family=socket.AF_INET,
            limit=CONCURRENCY, limit_per_host=CONCURRENCY,
        )
        async with aiohttp.ClientSession(connector=connector) as session:
            # Process in batches to show progress
            BATCH = 500
            fetched_data = {}
            for i in range(0, len(need_fetch), BATCH):
                batch = need_fetch[i : i + BATCH]
                tasks = {
                    w: asyncio.create_task(fetch_portfolio(session, w, sem, stats))
                    for w in batch
                }
                results = await asyncio.gather(*tasks.values())
                for w, data in zip(batch, results):
                    if data is not None:
                        fetched_data[w] = data

                done = min(i + BATCH, len(need_fetch))
                pct = done / len(need_fetch) * 100
                print(
                    f"  Fetched {done}/{len(need_fetch)} ({pct:.0f}%) "
                    f"| ok={stats['fetched']} err={stats['http_errors']} "
                    f"fail={stats['failed']} retry={stats['retries']}",
                    flush=True,
                )

                # Save progress periodically
                if done % 2000 == 0:
                    save_progress([], stats)

        # Merge fetched into cached
        cached_wallets.extend((w, d) for w, d in fetched_data.items())
        print(f"  Total wallets with portfolio data: {len(cached_wallets)}", flush=True)

    # ── Apply all cache-based filters ──
    print(f"\nApplying filters...", flush=True)
    passed = []
    dd_killed = 0
    data_killed = 0
    not_profitable = 0
    too_young = 0
    profit_lt_dd = 0

    for wallet, portfolio_data in cached_wallets:
        result = extract_wallet_stats(portfolio_data)
        if result is None:
            data_killed += 1
            continue

        max_dd_pct, peak, end_val = result["max_dd_pct"], result["peak_value"], result["end_value"]

        # Condition 1: DD < 40%
        if max_dd_pct >= DD_THRESHOLD:
            dd_killed += 1
            continue

        # Condition 3: Must be profitable (perp PnL > 0)
        if result["realised_pnl"] <= 0:
            not_profitable += 1
            continue

        # Condition 4: Account must be > 30 days old
        if result["account_age_days"] < MIN_ACCOUNT_AGE_DAYS:
            too_young += 1
            continue

        # Condition 5: Profit must cover the drawdown
        if result["pnl_dd_ratio"] < 1.0:
            profit_lt_dd += 1
            continue

        passed.append({
            "wallet": wallet,
            "max_dd_pct": round(max_dd_pct * 100, 2),
            "peak_value": round(peak, 2),
            "end_value": round(end_val, 2),
            "realised_pnl": round(result["realised_pnl"], 2),
            "account_age_days": round(result["account_age_days"], 1),
            "abs_dd": round(result["abs_dd"], 2),
            "pnl_dd_ratio": round(result["pnl_dd_ratio"], 2),
        })

    # ── Results ──
    print(f"\n{'='*60}")
    print(f"RESULTS FROM {len(all_wallets)} WALLETS")
    print(f"{'='*60}")
    print(f"Condition 2 (traded last {RECENT_DAYS}d): {len(recent_wallets)} passed, {not_recent} eliminated")
    print(f"Portfolio data: {len(cached_wallets)} fetched/cached, {data_killed} insufficient")
    print(f"Condition 1 (DD < {DD_THRESHOLD*100:.0f}%): {len(cached_wallets) - data_killed - dd_killed} passed, {dd_killed} eliminated")
    print(f"Condition 3 (profitable): {len(cached_wallets) - data_killed - dd_killed - not_profitable} passed, {not_profitable} eliminated")
    print(f"Condition 4 (age > {MIN_ACCOUNT_AGE_DAYS}d): {len(cached_wallets) - data_killed - dd_killed - not_profitable - too_young} passed, {too_young} eliminated")
    print(f"Condition 5 (profit >= DD): {len(passed)} passed, {profit_lt_dd} eliminated")
    print(f"")
    print(f"FINAL: {len(passed)} wallets passed all 5 conditions")
    print(f"  (from {len(all_wallets)} total — {len(passed)/len(all_wallets)*100:.1f}% pass rate)")
    print(f"")

    # DD distribution
    passed.sort(key=lambda x: x["max_dd_pct"])
    print(f"DD% distribution of passers:")
    brackets = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40)]
    for lo, hi in brackets:
        c = sum(1 for p in passed if lo <= p["max_dd_pct"] < hi)
        bar = "#" * (c // 10)
        print(f"  {lo:>2}-{hi:>2}%: {c:>5} {bar}")

    # PnL distribution
    print(f"\nRealised PnL distribution:")
    pnl_brackets = [(-1e12, 0), (0, 1000), (1000, 5000), (5000, 10000), (10000, 50000), (50000, 1e12)]
    labels = ["< $0", "$0-$1K", "$1K-$5K", "$5K-$10K", "$10K-$50K", "> $50K"]
    for (lo, hi), label in zip(pnl_brackets, labels):
        c = sum(1 for p in passed if lo <= p["realised_pnl"] < hi)
        print(f"  {label:>12}: {c:>5}")

    # Account age distribution
    print(f"\nAccount age distribution:")
    age_brackets = [(30, 60), (60, 90), (90, 180), (180, 365), (365, 1e6)]
    for lo, hi in age_brackets:
        c = sum(1 for p in passed if lo <= p["account_age_days"] < hi)
        print(f"  {lo:>3}-{hi:>5.0f}d: {c:>5}")

    # PnL/DD ratio distribution
    print(f"\nPnL/DD ratio distribution:")
    ratio_brackets = [(1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, 5.0), (5.0, 1e6)]
    for lo, hi in ratio_brackets:
        c = sum(1 for p in passed if lo <= p["pnl_dd_ratio"] < hi)
        print(f"  {lo:.1f}x-{hi:.1f}x: {c:>5}")

    # Account value distribution
    print(f"\nPeak account value distribution:")
    val_brackets = [(0, 1000), (1000, 5000), (5000, 10000), (10000, 50000), (50000, 100000), (100000, 1e12)]
    for lo, hi in val_brackets:
        c = sum(1 for p in passed if lo <= p["peak_value"] < hi)
        print(f"  ${lo:>8,.0f}-${hi:>12,.0f}: {c:>5}")

    # ── Write output ──
    fieldnames = [
        "wallet", "max_dd_pct", "peak_value", "end_value",
        "realised_pnl", "account_age_days", "abs_dd", "pnl_dd_ratio",
    ]
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(passed)
    print(f"\nWrote {len(passed)} wallets to {OUTPUT_FILE}")

    save_progress(passed, stats)


if __name__ == "__main__":
    asyncio.run(main())
