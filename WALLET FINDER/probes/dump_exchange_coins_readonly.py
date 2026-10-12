"""READ-ONLY: dump every coin the exchange reports for the 2 wallets, and probe
spot (@NNN) coins via spotClearinghouseState. Establishes coin-mapping vs flat."""
import json, os, urllib.request

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
API = "https://api.hyperliquid.xyz/info"

def post(body):
    req = urllib.request.Request(API, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

wallets = ["0x35519de6193f0e6581032d87236988629e35a281",
           "0x4e994d450c7c710ba4baeda5e249acc71f80aa1b"]

dexs = [d.get("name") for d in post({"type": "perpDexs"}) if isinstance(d, dict) and d.get("name")]

for w in wallets:
    print("="*70)
    print("WALLET", w)
    # perp positions across native + all dexs
    allcoins = {}
    for dn in ["<native>"] + dexs:
        body = {"type": "clearinghouseState", "user": w}
        if dn != "<native>":
            body["dex"] = dn
        try:
            snap = post(body)
        except Exception as e:
            print(f"  {dn}: FAILED {e}"); continue
        aps = (snap or {}).get("assetPositions") or []
        coins = {}
        for ap in aps:
            p = ap.get("position") or {}
            coins[p.get("coin")] = p.get("szi")
        if coins:
            print(f"  perp[{dn}]: {coins}")
        for c in coins:
            allcoins.setdefault(c, {})[dn] = coins[c]
    # spot
    try:
        spot = post({"type": "spotClearinghouseState", "user": w})
        sb = (spot or {}).get("balances") or []
        nonzero = {b.get("coin"): b.get("total") for b in sb if b.get("coin")}
        print(f"  spot balances: {nonzero}")
    except Exception as e:
        print("  spot FAILED", e)
    print("  --- perp coins seen across all dexes:", list(allcoins.keys()))
