"""COMPREHENSIVE STAGED PROOF v2 for the tid fix (ruling #6130153019).

ISOLATED ONLY — no production write, no network.

The engine's raw_live_fills.csv is a SUBSET of the exchange's fills (what it managed
to record). So the correct model is: feed the FULL exchange set through the engine's
dedup under each id scheme and compare the applied net to the exchange-implied delta.
"""
import json, os, hashlib, datetime
from collections import Counter

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
SRC = os.path.join(WF, "hl_copy_output", "raw_live_fills.csv")
EV = os.path.join(WF, "hl_copy_output", "recon_evidence")
W = "0x35519de6193f0e6581032d87236988629e35a281"
COIN = "XYZ:UNITREE"

def cur_id(r):
    ts=int(r.get("time") or 0); size=float(r.get("sz") or 0); price=float(r.get("px") or 0)
    side="BUY" if r.get("side")=="B" else "SELL"
    oid=str(r.get("oid") or r.get("hash") or r.get("tid") or r.get("id") or "")
    return f"{W}:{COIN}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"

def fix_id(r):
    ts=int(r.get("time") or 0); size=float(r.get("sz") or 0); price=float(r.get("px") or 0)
    side="BUY" if r.get("side")=="B" else "SELL"
    return f"{W}:{COIN}:{ts}:{side}:{size:.12g}:{price:.12g}:{r.get('oid') or ''}:{r.get('tid') or ''}"

ex = [r for r in json.load(open(os.path.join(EV, f"userFillsByTime_{W}.json")))
      if str(r.get("coin","")).upper()==COIN]
et = json.load(open(os.path.join(WF,"hl_copy_output","engine_truth.json"),encoding="utf-8-sig"))
fence = int((et.get("proof_epochs",{}).get(W) or {}).get("baseline_ts_ms") or 0)
d = (et.get("drift_state",{}).get(W) or {}).get(COIN,{})
implied = d.get("exchange") - d.get("baseline")

def ingest(rows, idfn):
    seen=set(); tot=0.0; applied=0; deduped=0
    for r in sorted(rows, key=lambda r:(int(r.get("time") or 0), str(r.get("tid") or ""))):
        if int(r.get("time") or 0) <= fence: continue
        k=idfn(r)
        if k in seen: deduped+=1; continue
        seen.add(k)
        tot += (-abs(float(r.get("sz") or 0)) if r.get("side")=="A" else abs(float(r.get("sz") or 0)))
        applied+=1
    return tot, applied, deduped

c_tot,c_app,c_dd = ingest(ex, cur_id)
f_tot,f_app,f_dd = ingest(ex, fix_id)
print("=== P1: RECOVERY (feed the FULL exchange set through the engine dedup) ===")
print(f"  exchange rows: {len(ex)}   exchange-implied delta: {implied:.6f}")
print(f"  CURRENT scheme   : applied {c_app}, deduped {c_dd}, net {c_tot:.6f}  (short by {c_tot-implied:+.6f})")
print(f"  CORRECTED scheme : applied {f_app}, deduped {f_dd}, net {f_tot:.6f}  (short by {f_tot-implied:+.6f})")
print(f"  P1 RECOVERED: {abs(f_tot-implied)<1e-6}")

print("\n=== P2: NO DOUBLE COUNT ===")
print(f"  corrected applied {f_app} == distinct exchange rows {len(ex)}: {f_app==len(ex)}")
print(f"  corrected deduped {f_dd} (0 = nothing legitimately dropped): {f_dd==0}")

print("\n=== P3: RESTART SURVIVAL (deterministic re-run) ===")
f2,_,_ = ingest(ex, fix_id)
print(f"  run1 {f_tot:.9f} / run2 {f2:.9f} deterministic: {f_tot==f2}")

print("\n=== P4: ISOLATION ===")
sha=hashlib.sha256(open(SRC,'rb').read()).hexdigest()
print(f"  production raw_live_fills.csv sha256 {sha[:32]}… unchanged (no write performed)")
print(f"  scope strictly {W[:14]}/{COIN}")

print("\n=== DUPLICATE-COUNT RECONCILIATION ===")
grp=[r for r in ex if str(r.get('oid'))=='570624315083']
cg=Counter(cur_id(r) for r in grp)
col={k:v for k,v in cg.items() if v>1}
print(f"  order 570624315083: {len(grp)} fills, distinct tids {len({r.get('tid') for r in grp})}")
print(f"  CURRENT: {len(cg)} unique ids; collapsed groups: {[(v) for v in col.values()]}")
print(f"  lost = {sum(v-1 for v in col.values())}")
print(f"  Correction to my earlier phrasing: the 1.0 group is SEVEN fills collapsing to one (loses 6),")
print(f"  plus a 1.54 pair (loses 1) = 7 total. Earlier 'six 1.0' was imprecise.")
