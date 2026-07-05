import asyncio
import aiohttp
import websockets
import csv
import os
import time
import json
from pathlib import Path

API_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CSV_FILE = DATA_DIR / "hl_wallets_filtered.csv"

# Leaderboard wallet injection
LEADERBOARD_FILE = Path(__file__).resolve().parent / "_archive" / "Leaderboard Wallets" / "profitable_missing_from_current_universe.txt"

SEEN_WALLETS = set()

BASE_DELAY = 0.5
delay = BASE_DELAY

TOP_COINS = []  # now dynamic


# ---------- CSV ----------
def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["wallet", "first_seen", "last_seen", "trade_count", "net_pnl_pct", "status"])

def load_existing_wallets():
    if not os.path.exists(CSV_FILE):
        return set()

    wallets = set()

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallets.add(row["wallet"])

    return wallets

def append_csv(wallet, now, trade_count="", net_pnl_pct="", status=""):
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([wallet, now, now, trade_count, net_pnl_pct, status])

def inject_leaderboard_wallets():
    """Inject leaderboard wallets with current timestamps so they flow through pipeline."""
    if not LEADERBOARD_FILE.exists():
        print(f"[LEADERBOARD] File not found: {LEADERBOARD_FILE}", flush=True)
        return 0
    
    now = int(time.time())
    injected = 0
    
    with open(LEADERBOARD_FILE, "r", encoding="utf-8") as f:
        for line in f:
            wallet = line.strip().lower()
            if not wallet or wallet in SEEN_WALLETS:
                continue
            if not wallet.startswith("0x") or len(wallet) != 42:
                continue
            
            SEEN_WALLETS.add(wallet)
            append_csv(wallet, now, trade_count="", net_pnl_pct="", status="LEADERBOARD")
            injected += 1
    
    if injected:
        print(f"[LEADERBOARD] Injected {injected} wallets with current timestamps", flush=True)
    return injected


# ---------- HTTP ----------
async def post(session, payload):
    global delay

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
                delay = min(delay * 1.5, 3.0)
                print(f"[RATE LIMIT] delay → {round(delay,2)}", flush=True)
                return None

            else:
                print(f"[WARN] HTTP {r.status}", flush=True)

    except Exception as e:
        print(f"[ERR] {e}", flush=True)

    return None


# ---------- COIN DISCOVERY (UPDATED) ----------
async def get_coins(session):
    meta = await post(session, {"type": "meta"})

    if not meta or "universe" not in meta:
        return ["BTC", "ETH", "SOL"]

    coins = []

    for c in meta["universe"]:
        name = c.get("name")
        vol = c.get("dayNtlVlm", 0) or 0  # safe fallback
        coins.append((name, vol))

    coins.sort(key=lambda x: x[1], reverse=True)

    sorted_coins = [c[0] for c in coins]

    return sorted_coins


# ---------- STREAM ----------
async def stream_worker():
    global TOP_COINS

    while True:
        try:
            async with websockets.connect(WS_URL) as ws:

                for coin in TOP_COINS:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {
                            "type": "trades",
                            "coin": coin
                        }
                    }))

                print(f"[STREAM] connected → {TOP_COINS}", flush=True)

                while True:
                    msg = await ws.recv()

                    try:
                        data = json.loads(msg)
                    except:
                        continue

                    if not isinstance(data, dict):
                        continue

                    if "data" not in data:
                        continue

                    if not isinstance(data["data"], list):
                        continue

                    new = 0

                    for trade in data["data"]:
                        if not isinstance(trade, dict):
                            continue

                        users = trade.get("users", [])

                        for wallet in users:
                            if wallet in SEEN_WALLETS:
                                continue

                            SEEN_WALLETS.add(wallet)
                            new += 1

                            now = int(time.time())
                            append_csv(wallet, now, trade_count="", net_pnl_pct="", status="LIVE")

                    if new > 0:
                        print(f"[STREAM] +{new} | total={len(SEEN_WALLETS)}", flush=True)

        except Exception as e:
            print(f"[STREAM ERR] reconnecting... {e}", flush=True)
            await asyncio.sleep(2)


# ---------- POLLING ----------
async def poll_worker(session, coins):
    global delay

    idx = 0

    while True:
        coin = coins[idx]
        idx = (idx + 1) % len(coins)

        if coin in TOP_COINS:
            continue

        data = await post(session, {
            "type": "recentTrades",
            "coin": coin
        })

        new = 0

        if data and isinstance(data, list):
            for trade in data:
                users = trade.get("users", [])

                for wallet in users:
                    if wallet in SEEN_WALLETS:
                        continue

                    SEEN_WALLETS.add(wallet)
                    new += 1

                    now = int(time.time())
                    append_csv(wallet, now, trade_count="", net_pnl_pct="", status="LIVE")

            delay = max(BASE_DELAY, delay * 0.98)

        print(
            f"[POLL] {coin} | total={len(SEEN_WALLETS)} | new={new} | delay={round(delay,2)}",
            flush=True
        )

        await asyncio.sleep(delay)


# ---------- MAIN ----------
async def main():
    global TOP_COINS

    print("=== HL WALLET ENGINE (DYNAMIC STREAM + POLL) ===\n", flush=True)

    init_csv()

    global SEEN_WALLETS
    SEEN_WALLETS = load_existing_wallets()

    print(f"[INIT] loaded {len(SEEN_WALLETS)} existing wallets", flush=True)

    # Inject leaderboard wallets with current timestamps
    inject_leaderboard_wallets()

    resolver = aiohttp.resolver.ThreadedResolver()
    connector = aiohttp.TCPConnector(resolver=resolver)

    async with aiohttp.ClientSession(connector=connector) as session:

        coins = await get_coins(session)

        # 🔥 AUTO SELECT TOP VOLUME COINS
        TOP_COINS = coins[:12]

        print(f"Loaded {len(coins)} coins")
        print(f"Top stream coins: {TOP_COINS}\n", flush=True)

        await asyncio.gather(
            stream_worker(),
            poll_worker(session, coins)
        )


if __name__ == "__main__":
    asyncio.run(main())