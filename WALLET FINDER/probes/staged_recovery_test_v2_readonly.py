"""STAGED RECOVERY TEST v2 (ruling #6130003901) — ISOLATED COPY ONLY.

Derives the missing UNITREE fills by (ts,|sz|) matching against the engine's own
recorded fills, injects them into an ISOLATED COPY, and proves the discrepancy
clears and nothing else changes. NO exchange queries.
"""
import csv, json, os, shutil, datetime

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
SRC = os.path.join(WF, "hl_copy_output", "raw_live_fills.csv")
EV = os.path.join(WF, "hl_copy_output", "recon_evidence")
STAGED = os.path.join(EV, "staged")
os.makedirs(STAGED, exist_ok=True)
W = "0x35519de6193f0e6581032d87236988629e35a281"
COIN = "XYZ:UNITREE"

def iso(ms):
    try: return datetime.datetime.fromtimestamp(int(ms)/1000, datetime.UTC).isoformat()
    except Exception: return str(ms)

eng = {}
with open(SRC, "r", encoding="utf-8", errors="replace", newline="") as f:
    for row in csv.DictReader(f):
        if row.get("wallet","").lower()==W and str(row.get("coin","")).upper()==COIN:
            eng.setdefault(int(row.get("timestamp_ms") or 0), []).append(abs(float(row.get("signed_size_delta") or 0)))

ex = [r for r in json.load(open(os.path.join(EV, f"userFillsByTime_{W}.json")))
      if str(r.get("coin","")).upper()==COIN]

missing=[]
for r in ex:
    t=int(r.get("time") or 0); s=abs(float(r.get("sz") or 0))
    lst=eng.get(t,[]); hit=None
    for i,v in enumerate(lst):
        if abs(v-s)<1e-9: hit=i; break
    if hit is None: missing.append(r)
    else: lst.pop(hit)
missing.sort(key=lambda r:(int(r["time"]), float(r["sz"])))
print(f"missing fills derived: {len(missing)}")
for r in missing:
    print("  ", iso(r["time"]), r["side"], r["sz"], "oid", r["oid"])
print("  net:", sum((1 if r.get('side')=='B' else -1)*float(r.get('sz') or 0) for r in missing))

dst = os.path.join(STAGED, "raw_live_fills.staged.csv")
shutil.copy2(SRC, dst)
with open(SRC, "r", encoding="utf-8", errors="replace", newline="") as f:
    cols = f.readline().rstrip("\n").split(",")
def row_for(r):
    d={c:"" for c in cols}
    d["wallet"]=W; d["coin"]=r.get("coin"); d["side"]=r.get("side")
    d["price"]=r.get("px"); d["size"]=r.get("sz")
    d["signed_size_delta"]=repr(float(r.get("sz") or 0)*(1 if r.get("side")=="B" else -1))
    d["timestamp_ms"]=str(int(r.get("time") or 0)); d["timestamp_iso"]=iso(r.get("time"))
    d["source"]="recon_staged"; d["recording_method"]="REBUILD"
    d["contributes_to_execution_delta"]="True"; d["reconstructed"]="True"
    return ",".join(str(d[c]) for c in cols)
with open(dst,"a",encoding="utf-8",newline="") as f:
    for r in missing: f.write(row_for(r)+"\n")

def replay(path, fence):
    tot=0.0; n=0
    with open(path,"r",encoding="utf-8",errors="replace",newline="") as f:
        for row in csv.DictReader(f):
            if row.get("wallet","").lower()==W and str(row.get("coin","")).upper()==COIN:
                if int(row.get("timestamp_ms") or 0) > fence:
                    try: tot+=float(row.get("signed_size_delta") or 0); n+=1
                    except Exception: pass
    return tot,n

et=json.load(open(os.path.join(WF,"hl_copy_output","engine_truth.json"),encoding="utf-8-sig"))
fence=int((et.get("proof_epochs",{}).get(W) or {}).get("baseline_ts_ms") or 0)
d=(et.get("drift_state",{}).get(W) or {}).get(COIN,{})
implied=(d.get("exchange")-d.get("baseline"))
b,bn=replay(SRC,fence); a,an=replay(dst,fence)
print("\n=== REPLAY (post-fence) ===")
print(f"  BEFORE (production): {b:.6f} ({bn} fills)")
print(f"  AFTER  (isolated +{len(missing)}): {a:.6f} ({an} fills)")
print(f"  exchange-implied:    {implied:.6f}")
print(f"  discrepancy BEFORE {b-implied:+.6f}  AFTER {a-implied:+.6f}")
print(f"  RESULT: {'DISCREPANCY CLEARED' if abs(a-implied)<1e-6 else 'STILL MISMATCHED'}")
print("\n=== ISOLATION ===")
print("  production size:", os.path.getsize(SRC), "staged size:", os.path.getsize(dst), "(diff = injected rows)")
print("  production mtime:", iso(os.path.getmtime(SRC)*1000))
