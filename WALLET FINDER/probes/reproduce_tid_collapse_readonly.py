"""REPRODUCE the tid-collapse defect + test the correction (ruling #6130091873).

ISOLATED ONLY. No production change, no network.

Defect: HL_Copy_Engine_SSOT.py L1991-1992 builds the internal fill_id as
  oid = raw_get(raw, "oid","hash","tid","id")     -> picks OID first
  fill_id = f"{wallet}:{coin}:{ts}:{side}:{size}:{price}:{oid}"   -> NO tid
So N genuinely-distinct fills of one order (same oid/ts/size/price/side) collide.
"""
import json, os

WF = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER"
EV = os.path.join(WF, "hl_copy_output", "recon_evidence")
W = "0x35519de6193f0e6581032d87236988629e35a281"
COIN = "XYZ:UNITREE"

ex = [r for r in json.load(open(os.path.join(EV, f"userFillsByTime_{W}.json")))
      if str(r.get("coin","")).upper() == COIN]

def current_fill_id(raw):
    """Reproduces L1991-1992 exactly."""
    ts = int(raw.get("time") or 0)
    size = float(raw.get("sz") or 0)
    price = float(raw.get("px") or 0)
    side = "BUY" if raw.get("side") == "B" else "SELL"
    oid = str(raw.get("oid") or raw.get("hash") or raw.get("tid") or raw.get("id") or "")
    return f"{W}:{COIN}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"

def corrected_fill_id(raw):
    """Include the native tid (trade id)."""
    ts = int(raw.get("time") or 0)
    size = float(raw.get("sz") or 0)
    price = float(raw.get("px") or 0)
    side = "BUY" if raw.get("side") == "B" else "SELL"
    oid = str(raw.get("oid") or "")
    tid = str(raw.get("tid") or "")
    return f"{W}:{COIN}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}:{tid}"

# focus on the affected order
grp = [r for r in ex if str(r.get("oid")) == "570624315083"]
print(f"fills for oid 570624315083: {len(grp)}")
print("tid values:", [r.get("tid") for r in grp])

cur = {}
cor = {}
for r in grp:
    cur.setdefault(current_fill_id(r), []).append(r)
    cor.setdefault(corrected_fill_id(r), []).append(r)

print(f"\nCURRENT scheme: {len(grp)} fills -> {len(cur)} unique ids")
for k, v in cur.items():
    print(f"   {len(v)} fills collapse to one id  (sizes {[x['sz'] for x in v]})")
print(f"CORRECTED scheme: {len(grp)} fills -> {len(cor)} unique ids")

lost_cur = len(grp) - len(cur)
print(f"\n=> CURRENT loses {lost_cur} fills to collision; CORRECTED loses 0")

# full-pair effect (all UNITREE fills)
all_cur = len({current_fill_id(r) for r in ex})
all_cor = len({corrected_fill_id(r) for r in ex})
print(f"\nALL UNITREE fills: {len(ex)} exchange rows")
print(f"  unique ids under CURRENT   : {all_cur}  (collisions lose {len(ex)-all_cur})")
print(f"  unique ids under CORRECTED : {all_cor}  (collisions lose {len(ex)-all_cor})")

# the 7th fill (1.54 @ .465) — does it collide too?
r7 = [r for r in grp if float(r["sz"]) == 1.54]
print(f"\n1.54 fills in this order: {len(r7)}")
for r in r7:
    print("   tid", r.get("tid"), "-> current id unique?", current_fill_id(r) not in [current_fill_id(x) for x in grp if x is not r])
