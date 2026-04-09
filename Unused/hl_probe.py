import asyncio
import aiohttp
import random
import time

API_URL = "https://api.hyperliquid.xyz/info"
MAX_CONCURRENCY = 50
SAMPLE_SIZE = 3000
SATURATION_WINDOW = 7
NEW_WALLET_THRESH = 50
ACTIVE_WINDOW_DAYS = 30
FILLS_LIMIT = 100
MIN_TRADE_COUNT = 20
MIN_TRADES_7D = 2


async def post(session, payload):
    try:
        async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                return await r.json()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# PASS 1 — DISCOVERY
# ---------------------------------------------------------------------------

async def discover_wallets():
    now_ms = time.time() * 1000
    cutoff_ms = now_ms - ACTIVE_WINDOW_DAYS * 86400 * 1000

    wallet_last_seen = {}
    low_yield_streak = 0
    call_count = 0

    async with aiohttp.ClientSession() as session:
        while True:
            data = await post(session, {"type": "recentTrades"})
            call_count += 1

            if data and isinstance(data, list):
                new_this_call = 0
                for trade in data:
                    addr = trade.get("user") or trade.get("buyer") or trade.get("seller")
                    if not addr:
                        continue
                    t = trade.get("time", 0)
                    if addr not in wallet_last_seen or t > wallet_last_seen[addr]:
                        if addr not in wallet_last_seen:
                            new_this_call += 1
                        wallet_last_seen[addr] = t

                if new_this_call < NEW_WALLET_THRESH:
                    low_yield_streak += 1
                else:
                    low_yield_streak = 0

                print(f"  [discovery] call {call_count} | new={new_this_call} | total={len(wallet_last_seen)} | streak={low_yield_streak}", flush=True)
            else:
                low_yield_streak += 1
                print(f"  [discovery] call {call_count} | no data | streak={low_yield_streak}", flush=True)

            if low_yield_streak >= SATURATION_WINDOW:
                print(f"  [discovery] saturation reached after {call_count} calls", flush=True)
                break

            await asyncio.sleep(0.2)

    active = {addr for addr, t in wallet_last_seen.items() if t >= cutoff_ms}
    print(f"  [discovery] {len(wallet_last_seen)} total seen | {len(active)} active (last {ACTIVE_WINDOW_DAYS}d)", flush=True)
    return active


# ---------------------------------------------------------------------------
# PASS 2 — SAMPLE ANALYSIS
# ---------------------------------------------------------------------------

async def analyse_wallet(session, sem, addr):
    async with sem:
        data = await post(session, {"type": "userFills", "user": addr})

    if not data or not isinstance(data, list):
        return None

    fills = data[-FILLS_LIMIT:]
    if not fills:
        return None

    now_ms = time.time() * 1000
    cutoff_7d = now_ms - 7 * 86400 * 1000

    trade_count = len(fills)
    trades_7d = sum(1 for f in fills if f.get("time", 0) >= cutoff_7d)
    net_pnl = sum(float(f.get("closedPnl", 0)) for f in fills)

    if trade_count < MIN_TRADE_COUNT or trades_7d < MIN_TRADES_7D:
        return None

    avg_pnl = net_pnl / trade_count
    return {
        "wallet": addr,
        "trade_count": trade_count,
        "trades_7d": trades_7d,
        "net_pnl": net_pnl,
        "avg_pnl": avg_pnl
    }


async def analyse_sample(wallets):
    sample = random.sample(list(wallets), min(SAMPLE_SIZE, len(wallets)))
    print(f"  [analysis] sampling {len(sample)} wallets", flush=True)

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results = []
    batch_size = 100
    total_done = 0

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(sample), batch_size):
            batch = sample[i : i + batch_size]
            batch_results = await asyncio.gather(
                *[analyse_wallet(session, sem, addr) for addr in batch]
            )
            valid = [r for r in batch_results if r is not None]
            results.extend(valid)
            total_done += len(batch)
            print(f"  [analysis] {total_done}/{len(sample)} done | qualified so far: {len(results)}", flush=True)
            await asyncio.sleep(0.1)

    return results


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def main():
    print("=== HYPERLIQUID WALLET PROBE ===\n", flush=True)

    print("[PASS 1] Discovering active wallets...", flush=True)
    active_wallets = await discover_wallets()
    total_active = len(active_wallets)

    print(f"\n[PASS 2] Analysing sample...", flush=True)
    results = await analyse_sample(active_wallets)

    profitable = sum(1 for r in results if r["net_pnl"] > 0)
    neutral    = sum(1 for r in results if r["net_pnl"] == 0)
    losing     = sum(1 for r in results if r["net_pnl"] < 0)

    import csv

    with open("hl_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "trade_count", "trades_7d", "net_pnl", "avg_pnl"])

        for r in results:
            writer.writerow([
                r["wallet"],
                r["trade_count"],
                r["trades_7d"],
                r["net_pnl"],
                r["avg_pnl"]
            ])

    print("\n=== RESULTS ===")
    print(f"TOTAL_ACTIVE_WALLETS: {total_active}")
    print(f"SAMPLED_WALLETS: {len(results)}")
    print()
    print(f"PROFITABLE: {profitable}")
    print(f"NEUTRAL:    {neutral}")
    print(f"LOSING:     {losing}")


if __name__ == "__main__":
    asyncio.run(main())