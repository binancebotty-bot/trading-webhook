"""READ-ONLY offline reconstruction probe for the 3 blocked positions.

Controller ruling #6129572665 authorises ONE targeted offline reconstruction to
establish whether the cause is (a) missing exchange evidence, (b) incorrect
accounting, or (c) coin mapping. This script:
  - reads the engine's persisted epoch baseline + internal position (from engine_truth.json)
  - fetches the LIVE exchange truth for the same wallet/dex (read-only /info)
  - reports the delta and classifies the cause
It NEVER writes to engine state, NEVER trades, and NEVER mutates anything.
"""
import json, os, sys, urllib.request, datetime

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
API = "https://api.hyperliquid.xyz/info"

def post(body):
    req = urllib.request.Request(API, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

def iso(ms):
    try: return datetime.datetime.fromtimestamp(int(ms)/1000, datetime.UTC).isoformat()
    except Exception: return str(ms)

def main():
    et = json.load(open(os.path.join(WF, "hl_copy_output", "engine_truth.json"), encoding="utf-8-sig"))
    pe = et.get("proof_epochs", {})
    pos = et.get("positions", {})
    drift = et.get("drift_state", {})

    targets = {
        "0x35519de6193f0e6581032d87236988629e35a281": ["XYZ:SOFTBANK", "XYZ:UNITREE"],
        "0x4e994d450c7c710ba4baeda5e249acc71f80aa1b": ["@188"],
    }

    # enumerate perp dexs (to fetch HIP-3 builder positions)
    try:
        dexs = post({"type": "perpDexs"})
        dex_names = [d.get("name") for d in dexs if isinstance(d, dict)]
    except Exception as e:
        dex_names = []
        print("PERPDEXS_FAILED", e)

    out = {}
    for wallet, coins in targets.items():
        ep = pe.get(wallet, {})
        baseline = ep.get("baseline_position") or {}
        out[wallet] = {"epoch_status": ep.get("epoch_status"),
                       "baseline_ts_ms": ep.get("baseline_ts_ms"),
                       "baseline_ts_iso": iso(ep.get("baseline_ts_ms")),
                       "coins": {}}
        # native + each perp dex
        snaps = {}
        try:
            native = post({"type": "clearinghouseState", "user": wallet})
            snaps["native"] = native
        except Exception as e:
            print("NATIVE_FAILED", wallet, e)
        for dn in dex_names:
            if not dn: continue
            try:
                snaps[dn] = post({"type": "clearinghouseState", "user": wallet, "dex": dn})
            except Exception as e:
                print("DEX_FAILED", wallet, dn, e)

        # collect exchange sizes for the target coins across all dexs
        exch_by_coin = {}
        for src, snap in snaps.items():
            if not isinstance(snap, dict): continue
            for ap in (snap.get("assetPositions") or []):
                p = ap.get("position") or {}
                c = p.get("coin")
                szi = p.get("szi")
                try: szi = float(szi)
                except Exception: pass
                if c:
                    exch_by_coin.setdefault(c, {})[src] = szi
            # also account value / margin summary presence = evidence the snapshot returned
        out[wallet]["_snapshot_sources"] = list(snaps.keys())
        out[wallet]["_snapshot_has_margin"] = {k: bool((v or {}).get("marginSummary")) for k, v in snaps.items()}

        for c in coins:
            internal = None
            cur = (pos.get(wallet) or {}).get(c)
            if isinstance(cur, dict): internal = cur.get("signed_size")
            b = (baseline.get(c) or {}).get("signed_size") if isinstance(baseline.get(c), dict) else None
            out[wallet]["coins"][c] = {
                "internal": internal,
                "epoch_baseline": b,
                "exchange_by_source": exch_by_coin.get(c, {}),
                "in_drift_state": (drift.get(wallet) or {}).get(c, {}).get("status") if isinstance(drift.get(wallet), dict) else None,
            }
    print(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
