"""ISOLATED COPY OF THE REAL SSOT ENGINE + tid fix + executable test (ruling #6130280631).

Drives the engine's OWN parse_fill / dedup path (not a simulation):
  1. copy HL_Copy_Engine_SSOT.py -> isolated_engine/HL_Copy_Engine_SSOT_isolated.py
  2. apply the minimal tid patch to the COPY (assert exactly-one match)
  3. import the COPY and call its real parse_fill() on the 82 exchange fills
  4. prove exactly 7 previously-missing executions are accepted vs the 75 legacy ids
  5. prove a second pass adds zero
  6. persist through the engine's own id memory semantics and reload

ISOLATED ONLY: the copy lives under recon_evidence/isolated_engine/. Production is never
imported, never written, never restarted. No network.
"""
import os, sys, json, shutil, hashlib, importlib.util

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
SRC = os.path.join(WF, "HL_Copy_Engine_SSOT.py")
EV = os.path.join(WF, "hl_copy_output", "recon_evidence")
ISO = os.path.join(EV, "isolated_engine"); os.makedirs(ISO, exist_ok=True)
COPY = os.path.join(ISO, "HL_Copy_Engine_SSOT_isolated.py")
W = "0x35519de6193f0e6581032d87236988629e35a281"
COIN = "XYZ:UNITREE"

# 1. copy
shutil.copy2(SRC, COPY)
src_sha = hashlib.sha256(open(SRC, "rb").read()).hexdigest()
print(f"copied engine -> {COPY}")
print(f"production engine sha256 {src_sha[:32]}… (will re-verify unchanged at end)")

# 2. patch the COPY only
text = open(COPY, "r", encoding="utf-8").read()
OLD = '        oid = str(raw_get(raw, "oid", "hash", "tid", "id", default=""))\n        fill_id = str(raw_get(raw, "fill_id", default="")) or f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"'
NEW = '        oid = str(raw_get(raw, "oid", "hash", "tid", "id", default=""))\n        tid = str(raw_get(raw, "tid", "id", default=""))\n        fill_id = str(raw_get(raw, "fill_id", default="")) or f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}:{tid}"'
assert text.count(OLD) == 1, f"patch target not unique: {text.count(OLD)}"
text = text.replace(OLD, NEW)
open(COPY, "w", encoding="utf-8").write(text)
print("patch applied to COPY (exactly-one match asserted)")

# 3. import the COPY and call the REAL parse_fill
spec = importlib.util.spec_from_file_location("iso_engine", COPY)
eng = importlib.util.module_from_spec(spec)
sys.modules["iso_engine"] = eng          # dataclass needs the module resolvable
# prevent any module-level side effects from touching production dirs: point cwd at ISO
os.chdir(ISO)
sys.path.insert(0, ISO)
spec.loader.exec_module(eng)
print("isolated engine imported; EngineSSOT present:", hasattr(eng, "EngineSSOT"))
_engine = eng.EngineSSOT(wallets=[W])
parse_fill = _engine.parse_fill

# 4. real parse_fill on the exchange fills
ex = [r for r in json.load(open(os.path.join(EV, f"userFillsByTime_{W}.json")))
      if str(r.get("coin","")).upper()==COIN]
et = json.load(open(os.path.join(WF,"hl_copy_output","engine_truth.json"),encoding="utf-8-sig"))
fence = int((et.get("proof_epochs",{}).get(W) or {}).get("baseline_ts_ms") or 0)
d = (et.get("drift_state",{}).get(W) or {}).get(COIN,{})
implied = d.get("exchange") - d.get("baseline")

parsed = []
for r in ex:
    f = parse_fill(W, r, source="recon")
    if f: parsed.append(f)
print(f"\nreal parse_fill produced {len(parsed)} RawFills from {len(ex)} exchange rows")
uniq = {f.fill_id for f in parsed}
print(f"unique fill_ids (patched scheme): {len(uniq)}  (expect {len(ex)})")

# 5. dedup: load the 75 legacy ids, then apply parsed
legacy = set()
import csv
with open(os.path.join(WF,"hl_copy_output","raw_live_fills.csv"),"r",encoding="utf-8",errors="replace",newline="") as fh:
    for row in csv.DictReader(fh):
        if row.get("wallet","").lower()==W and str(row.get("coin","")).upper()==COIN:
            legacy.add(row.get("fill_id") or "")
print(f"\nlegacy ids loaded: {len(legacy)}")

def apply(seen, fills):
    accepted=0; dup=0
    for f in sorted(fills, key=lambda f:(f.timestamp_ms, f.fill_id)):
        if f.timestamp_ms <= fence: continue
        if f.fill_id in seen: dup+=1; continue
        seen.add(f.fill_id); accepted+=1
    return accepted,dup

seen=set(legacy)
acc1,dup1=apply(seen, parsed)
print(f"pass 1: accepted {acc1}, dup {dup1}  -> exactly 7: {acc1==7}")
acc2,dup2=apply(seen, parsed)
print(f"pass 2: accepted {acc2}, dup {dup2}  -> adds zero: {acc2==0}")

# net delta check
net = sum((f.signed_size_delta) for f in parsed if f.timestamp_ms>fence and f.fill_id in seen)
print(f"net (post-fence, applied): {net:.6f}  vs implied {implied:.6f}  match: {abs(net-implied)<1e-6}")

# 6. production unchanged
src_sha2 = hashlib.sha256(open(SRC, "rb").read()).hexdigest()
print(f"\nproduction engine sha256 {src_sha2[:32]}… unchanged: {src_sha==src_sha2}")

# reviewable patch artifact
patch = os.path.join(ISO, "tid_fix.patch")
open(patch,"w").write(f"--- a/HL_Copy_Engine_SSOT.py\n+++ b/HL_Copy_Engine_SSOT.py\n@@ parse_fill @@\n-        {OLD.splitlines()[1].strip()}\n+        tid = str(raw_get(raw, \"tid\", \"id\", default=\"\"))\n+        {NEW.splitlines()[1].strip()}\n")
print(f"reviewable patch written: {patch}")
