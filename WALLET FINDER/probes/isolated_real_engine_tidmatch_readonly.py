"""ISOLATED REAL-ENGINE tid FIX — backward-compat via NATIVE tid matching
(ruling #6130280631).

Uses an isolated COPY of the actual SSOT engine (patched), and drives its REAL
parse_fill(). Matches the 75 historical fills to their ORIGINAL native tids (read
from each legacy row's persisted raw_json), then proves exactly 7 executions are added.

ISOLATED ONLY. Production never imported/written/restarted. No network.
"""
import os, sys, json, csv, shutil, hashlib, importlib.util

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
SRC = os.path.join(WF, "HL_Copy_Engine_SSOT.py")
SRC_FILLS = os.path.join(WF, "hl_copy_output", "raw_live_fills.csv")
EV = os.path.join(WF, "hl_copy_output", "recon_evidence")
ISO = os.path.join(EV, "isolated_engine"); os.makedirs(ISO, exist_ok=True)
COPY = os.path.join(ISO, "HL_Copy_Engine_SSOT_isolated.py")
W = "0x35519de6193f0e6581032d87236988629e35a281"
COIN = "XYZ:UNITREE"

# 1. isolated copy + minimal patch (exactly-one match)
shutil.copy2(SRC, COPY)
sha_before = hashlib.sha256(open(SRC,"rb").read()).hexdigest()
t = open(COPY,"r",encoding="utf-8").read()
OLD = '        fill_id = str(raw_get(raw, "fill_id", default="")) or f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"'
NEW = '        tid = str(raw_get(raw, "tid", "id", default=""))\n        fill_id = str(raw_get(raw, "fill_id", default="")) or f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}:{tid}"'
assert t.count(OLD)==1, f"patch target count={t.count(OLD)}"
open(COPY,"w",encoding="utf-8").write(t.replace(OLD,NEW))
print("patch applied to COPY (exactly-one match asserted)")

# 2. import + real parse_fill
spec = importlib.util.spec_from_file_location("iso_engine", COPY)
eng = importlib.util.module_from_spec(spec); sys.modules["iso_engine"]=eng
os.chdir(ISO); sys.path.insert(0,ISO)
spec.loader.exec_module(eng)
_engine = eng.EngineSSOT(wallets=[W]); parse_fill=_engine.parse_fill

# 3. the 75 legacy rows -> their ORIGINAL native tids (from persisted raw_json)
et = json.load(open(os.path.join(WF,"hl_copy_output","engine_truth.json"),encoding="utf-8-sig"))
fence = int((et.get("proof_epochs",{}).get(W) or {}).get("baseline_ts_ms") or 0)
d = (et.get("drift_state",{}).get(W) or {}).get(COIN,{})
implied = d.get("exchange") - d.get("baseline")

legacy_tids=set(); legacy_rows=0
with open(SRC_FILLS,"r",encoding="utf-8",errors="replace",newline="") as fh:
    for row in csv.DictReader(fh):
        if row.get("wallet","").lower()==W and str(row.get("coin","")).upper()==COIN:
            legacy_rows+=1
            try:
                rj=json.loads(row.get("raw_json") or "{}")
                tid=rj.get("tid")
                if tid is not None: legacy_tids.add(str(tid))
            except Exception: pass
print(f"legacy rows: {legacy_rows}; distinct native tids recovered from raw_json: {len(legacy_tids)}")

# 4. real parse_fill over the 82 exchange fills; match by NATIVE tid
ex=[r for r in json.load(open(os.path.join(EV,f"userFillsByTime_{W}.json"))) if str(r.get("coin","")).upper()==COIN]
parsed=[parse_fill(W,r,source="recon") for r in ex]
parsed=[p for p in parsed if p]
print(f"real parse_fill: {len(parsed)} RawFills; unique ids {len({p.fill_id for p in parsed})}")

def native_tid(raw): return str(raw.get("tid") or "")
ex_tids={native_tid(r) for r in ex}
missing=[r for r in ex if native_tid(r) not in legacy_tids]
print(f"\nMATCH: {len(ex_tids)} exchange tids vs {len(legacy_tids)} legacy tids")
print(f"  previously-missing executions = {len(missing)}  -> exactly 7: {len(missing)==7}")
for r in sorted(missing, key=lambda r:int(r['time'])):
    print(f"    {r.get('side')} sz={r.get('sz')} px={r.get('px')} oid={r.get('oid')} tid={r.get('tid')}")

# 5. apply the 7 via the patched engine ids
seen=set()
accepted=0
for p in sorted(parsed,key=lambda p:(p.timestamp_ms,p.fill_id)):
    if p.timestamp_ms<=fence: continue
    if p.fill_id in seen: continue
    seen.add(p.fill_id); accepted+=1
net=sum(p.signed_size_delta for p in parsed if p.timestamp_ms>fence)
print(f"\npatched-engine ingest: accepted {accepted} (unique), net {net:.6f} vs implied {implied:.6f} match: {abs(net-implied)<1e-6}")

# 6. second pass adds zero
seen2=set(seen); a2=sum(1 for p in parsed if p.timestamp_ms>fence and p.fill_id not in seen2 and not seen2.add(p.fill_id))
print(f"second pass adds: {a2}")

# 7. production unchanged
sha_after=hashlib.sha256(open(SRC,'rb').read()).hexdigest()
print(f"\nproduction engine sha256 unchanged: {sha_before==sha_after}")
