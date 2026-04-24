import asyncio
import aiohttp
import csv
import time
import os  # ✅ ADDED

API_URL = "https://api.hyperliquid.xyz/info"

INPUT_FILE = "hl_wallets_filtered.csv"
OUTPUT_FILE = "hl_stage1_pass.csv"

CONCURRENCY = 5
FILLS_LIMIT = 100


# ---------- HTTP ----------
async def post(session, payload):
    try:
        async with session.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:

            if r.status == 200:
                return await r.json()

            elif r.status == 429:
                await asyncio.sleep(1)
                return None

    except:
        return None

    return None


# ---------- LOAD ----------
def load_wallets():
    wallets = []

    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallets.append(row["wallet"])

    return wallets


# ---------- LOAD EXISTING (DEDUP) ----------
def load_existing_wallets():
    if not os.path.exists(OUTPUT_FILE):
        return set()

    existing = set()

    with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing.add(row["wallet"])

    return existing


# ---------- SAVE ----------
def append_result(row):
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


# ---------- ANALYSIS ----------
def analyse_fills(fills):
    if not fills:
        return None

    pnl = 0.0
    volume = 0.0
    trades = 0
    wins = 0

    now = int(time.time() * 1000)
    trades_7d = 0

    for f in fills:
        closed_pnl = float(f.get("closedPnl", 0) or 0)
        px = float(f.get("px", 0) or 0)
        sz = float(f.get("sz", 0) or 0)
        t = int(f.get("time", 0) or 0)

        pnl += closed_pnl
        volume += abs(px * sz)
        trades += 1

        if closed_pnl > 0:
            wins += 1

        if now - t <= 7 * 24 * 60 * 60 * 1000:
            trades_7d += 1

    if trades == 0 or volume == 0:
        return None

    pnl_pct = pnl
    avg_pnl = pnl / trades
    win_rate = wins / trades

    return trades, pnl_pct, avg_pnl, win_rate, trades_7d, volume


# ---------- WORKER ----------
async def process_wallet(session, wallet):
    data = await post(session, {
        "type": "userFills",
        "user": wallet
    })

    if not data:
        return None

    fills = data[:FILLS_LIMIT]

    result = analyse_fills(fills)
    if not result:
        return None

    trades, pnl_pct, avg_pnl, win_rate, trades_7d, volume = result

    if (
        trades >= 30 and
        pnl_pct > 1 and
        avg_pnl > 0 and
        win_rate > 0.15 and
        trades_7d >= 1 and
        volume > 20000
    ):
        return (
            wallet,
            trades,
            round(pnl_pct, 2),
            round(avg_pnl, 4),
            round(win_rate, 2),
            trades_7d,
            int(volume)
        )

    return None


# ---------- MAIN ----------
async def main():
    print("=== STAGE 1 FILTER (STRICT) ===\n", flush=True)

    wallets = load_wallets()
    print(f"Loaded {len(wallets)} wallets\n", flush=True)

    # ✅ ONLY CREATE FILE IF IT DOES NOT EXIST
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "wallet",
                "trades",
                "pnl_pct",
                "avg_pnl",
                "win_rate",
                "trades_7d",
                "volume"
            ])

    # ✅ LOAD EXISTING FOR DEDUPE
    existing_wallets = load_existing_wallets()
    print(f"Existing stored wallets: {len(existing_wallets)}\n", flush=True)

    resolver = aiohttp.resolver.ThreadedResolver()
    connector = aiohttp.TCPConnector(resolver=resolver)

    sem = asyncio.Semaphore(CONCURRENCY)

    pass_count = [0]

    async with aiohttp.ClientSession(connector=connector) as session:

        async def bound_process(wallet, i):
            async with sem:
                result = await process_wallet(session, wallet)

                if i % 50 == 0:
                    print(f"[PROGRESS] {i}/{len(wallets)} | passes={pass_count[0]}", flush=True)

                if result:
                    wallet_id = result[0]

                    # ✅ DEDUPE CHECK
                    if wallet_id not in existing_wallets:
                        append_result(result)
                        existing_wallets.add(wallet_id)
                        pass_count[0] += 1
                        print(f"[NEW PASS] {result}", flush=True)

        tasks = [bound_process(w, i) for i, w in enumerate(wallets)]

        await asyncio.gather(*tasks)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())