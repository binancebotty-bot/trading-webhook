"""READ-ONLY: resolve spot index @188 and check whether XYZ:SOFTBANK exists as a coin."""
import json, urllib.request

API = "https://api.hyperliquid.xyz/info"

def post(body):
    req = urllib.request.Request(API, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

# spot metadata: index -> token
try:
    sm = post({"type": "spotMeta"})
    toks = {t.get("index"): t.get("name") for t in (sm.get("tokens") or [])}
    print("@188 ->", toks.get(188), "| @188 in tokens:", 188 in toks)
except Exception as e:
    print("spotMeta FAILED", e)

# is SOFTBANK tradeable anywhere? scan perp meta across dexes
dexs = [d.get("name") for d in post({"type": "perpDexs"}) if isinstance(d, dict) and d.get("name")]
found = {}
for dn in ["<native>"] + dexs:
    body = {"type": "meta"}
    if dn != "<native>":
        body = {"type": "meta", "dex": dn}
    try:
        m = post(body)
        names = [u.get("name") for u in (m.get("universe") or [])]
        hits = [n for n in names if n and "SOFTBANK" in n.upper()]
        if hits:
            found[dn] = hits
    except Exception as e:
        pass
print("SOFTBANK universe hits:", found if found else "NONE — coin not tradeable on any dex")
