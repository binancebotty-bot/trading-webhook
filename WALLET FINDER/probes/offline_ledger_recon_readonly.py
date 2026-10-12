"""OFFLINE ledger reconstruction of the 3 blocked positions.

Strictly offline: reads only persisted fills (hl_copy_output/raw_live_fills.csv)
and persisted engine state (engine_truth.json). NO network calls.

Question (ruling #6129799800): is the discrepancy from INCORRECT LEDGER ACCOUNTING
(a proven defect) or INCOMPLETE HISTORICAL EVIDENCE (missing fills)?

Method: for each wallet/coin, replay fills in time order, compute the cumulative
signed size from the epoch baseline, and compare with the engine's internal size.
"""
import csv, json, os, sys, datetime

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
FILLS = os.path.join(WF, "hl_copy_output", "raw_live_fills.csv")
TRUTH = os.path.join(WF, "hl_copy_output", "engine_truth.json")

TARGETS = {
    "0x35519de6193f0e6581032d87236988629e35a281": ["XYZ:SOFTBANK", "XYZ:UNITREE"],
    "0x4e994d450c7c710ba4baeda5e249acc71f80aa1b": ["@188"],
}

def iso(ms):
    try: return datetime.datetime.fromtimestamp(int(ms)/1000, datetime.UTC).isoformat()
    except Exception: return str(ms)

def main():
    et = json.load(open(TRUTH, encoding="utf-8-sig"))
    pe = et.get("proof_epochs", {})
    pos = et.get("positions", {})

    # gather fills for the target wallet/coin pairs
    want = {(w, c) for w, cs in TARGETS.items() for c in cs}
    fills = {k: [] for k in want}
    total = 0
    with open(FILLS, "r", encoding="utf-8", errors="replace", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            total += 1
            key = (row.get("wallet", "").lower(), row.get("coin"))
            if key in fills:
                fills[key].append({
                    "ts": int(row.get("timestamp_ms") or 0),
                    "signed": float(row.get("signed_size_delta") or 0),
                    "start": float(row.get("start_position") or 0),
                    "end": float(row.get("end_position") or 0),
                    "method": row.get("recording_method"),
                    "contrib": row.get("contributes_to_execution_delta"),
                })
    print(f"scanned rows: {total}")

    out = {}
    for w, cs in TARGETS.items():
        ep = pe.get(w, {})
        base = ep.get("baseline_position") or {}
        fence = int(ep.get("baseline_ts_ms") or 0)
        out[w] = {"epoch_status": ep.get("epoch_status"), "baseline_ts_ms": fence,
                  "baseline_ts_iso": iso(fence), "coins": {}}
        for c in cs:
            fl = sorted(fills[(w, c)], key=lambda x: x["ts"])
            b = (base.get(c) or {}).get("signed_size") if isinstance(base.get(c), dict) else None
            cur = (pos.get(w) or {}).get(c)
            internal = cur.get("signed_size") if isinstance(cur, dict) else None
            # replay: baseline + sum(signed deltas strictly after fence)
            post = [x for x in fl if x["ts"] > fence]
            pre = [x for x in fl if x["ts"] <= fence]
            replayed = (b or 0.0) + sum(x["signed"] for x in post)
            out[w]["coins"][c] = {
                "epoch_baseline": b,
                "engine_internal": internal,
                "fills_total": len(fl),
                "fills_pre_fence": len(pre),
                "fills_post_fence": len(post),
                "replayed_from_fills": round(replayed, 6),
                "internal_minus_replayed": (round(internal - replayed, 6) if internal is not None else None),
                "first_fill_iso": iso(fl[0]["ts"]) if fl else None,
                "last_fill_iso": iso(fl[-1]["ts"]) if fl else None,
                "methods": sorted({x["method"] for x in fl}),
            }
    print(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
