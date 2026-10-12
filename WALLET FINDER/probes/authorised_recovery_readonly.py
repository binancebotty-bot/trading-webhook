"""AUTHORISED recovery (ruling #6129895528): bounded, rate-guarded historical fill
requests for the 2 affected wallets + 1 metadata request to resolve @188.

Budget: <=6 userFillsByTime requests, 1 metadata request. Read-only. Writes evidence
to hl_copy_output/recon_evidence/ only; NEVER touches production data.
"""
import json, os, time, urllib.request, datetime

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
OUT = os.path.join(WF, "hl_copy_output", "recon_evidence")
API = "https://api.hyperliquid.xyz/info"
os.makedirs(OUT, exist_ok=True)

def post(body):
    req = urllib.request.Request(API, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())

def iso(ms):
    try: return datetime.datetime.fromtimestamp(int(ms)/1000, datetime.UTC).isoformat()
    except Exception: return str(ms)

def guard():
    time.sleep(1.5)  # rate-guard pacing between requests

# --- 1 metadata request: resolve @188 in the SPOT MARKET index space ---
meta = post({"type": "spotMeta"})
spot_universe = (meta or {}).get("universe") or []
spot_tokens = {t.get("index"): t.get("name") for t in ((meta or {}).get("tokens") or [])}
m188 = next((m for m in spot_universe if m.get("index") == 188), None)
print("METADATA spotMeta: universe[188] =", json.dumps(m188, default=str))
print("  (token-space tokens[188] =", spot_tokens.get(188), "— different index space)")
json.dump({"spot_universe_188": m188, "token_188": spot_tokens.get(188)},
          open(os.path.join(OUT, "spotmeta_188.json"), "w"), indent=1, default=str)

et = json.load(open(os.path.join(WF, "hl_copy_output", "engine_truth.json"), encoding="utf-8-sig"))
pe = et.get("proof_epochs", {})

budget = 6
wallets = ["0x35519de6193f0e6581032d87236988629e35a281",   # SOFTBANK, UNITREE
           "0x4e994d450c7c710ba4baeda5e249acc71f80aa1b"]   # @188
for w in wallets:
    fence = int((pe.get(w) or {}).get("baseline_ts_ms") or 0)
    lo = max(0, fence - 86400000)      # include a day before the fence for boundary proof
    hi = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
    guard(); budget -= 1
    rows = post({"type": "userFillsByTime", "user": w, "startTime": lo, "endTime": hi,
                 "aggregateByTime": False})
    print(f"\nFILLS {w[:14]} window {iso(lo)}..{iso(hi)} -> {len(rows)} rows (budget left {budget})")
    # keep only the target coins + summarize
    keep = [r for r in rows if str(r.get("coin")) in ("XYZ:SOFTBANK", "XYZ:UNITREE", "@188")]
    summary = {}
    for r in keep:
        c = r.get("coin")
        summary.setdefault(c, {"n": 0, "first": None, "last": None, "sum_sz": 0.0})
        s = summary[c]
        s["n"] += 1
        try: s["sum_sz"] += float(r.get("sz") or 0) * (1 if r.get("side") == "B" else -1)
        except Exception: pass
        t = int(r.get("time") or 0)
        s["first"] = min(s["first"] or t, t); s["last"] = max(s["last"] or t, t)
    for c, s in summary.items():
        s["first_iso"] = iso(s["first"]); s["last_iso"] = iso(s["last"])
    print("  target-coin summary:", json.dumps(summary, default=str))
    json.dump(rows, open(os.path.join(OUT, f"userFillsByTime_{w}.json"), "w"), default=str)
print(f"\nbudget remaining: {budget}")
