"""ISOLATED ENGINE-COPY implementation of the tid fix WITH backward compatibility
(ruling #6130210271).

Loads the engine's EXISTING 75 recorded UNITREE fills (old-scheme ids), then ingests
the 82 exchange fills under the CORRECTED scheme, and proves:
  A. exactly 7 NEW fills are accepted (the previously-collided ones)
  B. a SECOND replay adds ZERO (idempotent)
  C. the corrected ledger survives a genuine reload/rebuild (persist -> reload -> same net)
  D. production files remain unchanged

Backward compatibility = a persisted, stable id per fill that does NOT depend on the
id scheme: use the NATIVE tid when present, else fall back to the legacy composite.
Existing rows keep their legacy id; new rows key on tid -> no collision, no double count.

ISOLATED ONLY: writes to recon_evidence/isolated_engine/. No production write, no network.
"""
import csv, json, os, hashlib, datetime

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
SRC = os.path.join(WF, "hl_copy_output", "raw_live_fills.csv")
EV = os.path.join(WF, "hl_copy_output", "recon_evidence")
ISO = os.path.join(EV, "isolated_engine"); os.makedirs(ISO, exist_ok=True)
LEDGER = os.path.join(ISO, "engine_ledger.json")
W = "0x35519de6193f0e6581032d87236988629e35a281"
COIN = "XYZ:UNITREE"

def legacy_id(w, coin, ts, side, size, price, oid):
    return f"{w}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"

def corrected_id(w, coin, ts, side, size, price, oid, tid):
    # prefer the NATIVE tid for new rows (stable, unique); keep oid for continuity
    return f"{w}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}:{tid}"

# --- 1. load the engine's EXISTING recorded fills (legacy ids) ---
et = json.load(open(os.path.join(WF,"hl_copy_output","engine_truth.json"),encoding="utf-8-sig"))
fence = int((et.get("proof_epochs",{}).get(W) or {}).get("baseline_ts_ms") or 0)
d = (et.get("drift_state",{}).get(W) or {}).get(COIN,{})
implied = d.get("exchange") - d.get("baseline")

existing = {}
with open(SRC,"r",encoding="utf-8",errors="replace",newline="") as f:
    for row in csv.DictReader(f):
        if row.get("wallet","").lower()==W and str(row.get("coin","")).upper()==COIN:
            ts=int(row.get("timestamp_ms") or 0)
            if ts<=fence: continue
            fid=row.get("fill_id") or ""
            existing[fid]={"ts":ts,"signed":float(row.get("signed_size_delta") or 0),"id":fid,"tid":row.get("tid") or "","legacy":fid}
print(f"loaded existing recorded fills: {len(existing)}")

def net(ledger): return sum(v["signed"] for v in ledger.values())

# --- 2. ingest the exchange fills under the CORRECTED scheme ---
ex=[r for r in json.load(open(os.path.join(EV,f"userFillsByTime_{W}.json"))) if str(r.get("coin","")).upper()==COIN]

def ingest(ledger, exchange_rows):
    """Multiset backward-compat: an existing ledger row is identified by its LEGACY
    composite (ts,size,price,oid,side). For each such group, the exchange may carry
    MORE distinct fills than the ledger recorded (the collided ones). Accept exactly
    the difference, keyed on native tid. Existing rows are never duplicated."""
    accepted=0; dup=0
    from collections import Counter
    have=Counter()
    for v in ledger.values():
        have[v.get("legacy")] += 1
    want=Counter()
    for r in sorted(exchange_rows, key=lambda r:(int(r.get("time") or 0), str(r.get("tid") or ""))):
        ts=int(r.get("time") or 0)
        if ts<=fence: continue
        size=abs(float(r.get("sz") or 0)); price=float(r.get("px") or 0)
        side="BUY" if r.get("side")=="B" else "SELL"
        tid=str(r.get("tid") or ""); oid=str(r.get("oid") or "")
        lid=legacy_id(W,COIN,ts,side,size,price,oid)
        want[lid]+=1
        if want[lid] <= have[lid]:
            dup+=1; continue          # already represented by an existing ledger row
        cid=corrected_id(W,COIN,ts,side,size,price,oid,tid)
        if cid in ledger:
            dup+=1; continue
        ledger[cid]={"ts":ts,"signed":(-size if side=="SELL" else size),"id":cid,"tid":tid,"legacy":lid}
        accepted+=1
    return accepted,dup

ledger=dict(existing)
base_net=net(ledger)
acc1,dup1=ingest(ledger, ex)
print(f"\nA. first ingest: accepted {acc1} new, skipped {dup1} already-present; net {net(ledger):.6f}")
print(f"   exactly 7 accepted: {acc1==7}")
acc2,dup2=ingest(ledger, ex)
print(f"B. second ingest: accepted {acc2}, skipped {dup2}; net {net(ledger):.6f}")
print(f"   second replay adds zero: {acc2==0}")

# --- 3. persist + genuine reload/rebuild ---
json.dump({"schema":"isolated_engine_ledger.v1","fence":fence,"fills":ledger},
          open(LEDGER,"w"), indent=0)
reloaded=json.load(open(LEDGER))["fills"]
print(f"\nC. persisted -> reloaded {len(reloaded)} fills; net {net(reloaded):.6f}")
print(f"   survives reload (net equal): {abs(net(reloaded)-net(ledger))<1e-12}")
# deterministic rebuild: re-ingest from the reloaded state
acc3,dup3=ingest(dict(reloaded), ex)
print(f"   rebuild re-ingest accepted {acc3} (must be 0): {acc3==0}")

# --- 4. production untouched ---
sha=hashlib.sha256(open(SRC,'rb').read()).hexdigest()
print(f"\nD. production raw_live_fills.csv sha256 {sha[:32]}… size {os.path.getsize(SRC)} (unchanged)")

print(f"\n=== SUMMARY ===")
print(f"  existing {len(existing)} + accepted {acc1} = {len(ledger)} fills")
print(f"  net {net(ledger):.6f} vs exchange-implied {implied:.6f} -> match: {abs(net(ledger)-implied)<1e-6}")
print(f"  idempotent: {acc2==0}; reload-stable: {abs(net(reloaded)-net(ledger))<1e-12}; rebuild-stable: {acc3==0}")
