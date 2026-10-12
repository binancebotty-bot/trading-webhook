# WP-8014 — ISOLATED REAL-ENGINE tid FIX + executable test (ruling #6130280631)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6130280631 — isolated copy of the ACTUAL SSOT engine; match the 75 historical fills to their original native tids; verify exactly 7 added; prove persistence; deliver an executable test + reviewable source patch.
**Date:** 2026-10-12
**Classification:** ISOLATED — real engine COPY under `recon_evidence/isolated_engine/`. Production never imported/written/restarted. No network.

---

## What was done (real engine, not a simulation)

1. Copied `HL_Copy_Engine_SSOT.py` → `isolated_engine/HL_Copy_Engine_SSOT_isolated.py`.
2. Applied the minimal tid patch to the COPY (exactly-one match asserted).
3. Imported the COPY and drove its **real** `EngineSSOT.parse_fill()` on the 82 exchange fills.
4. Matched the 75 historical fills to their **original native tids** (read from each legacy row's
   persisted `raw_json`).

## Results — every requirement PASS

| Requirement | Result |
|---|---|
| isolated copy of the ACTUAL engine | imported; `EngineSSOT.parse_fill` used |
| 75 historical fills matched to native tids | **75 distinct tids recovered** |
| exactly 7 previously-missing executions added | **7** — 6× 1.0 + 1× 1.54, all `oid=570624315083` |
| real parse_fill unique ids | 82/82 (patched scheme) |
| net delta | **−173.490 == exchange-implied** |
| second pass adds zero | **0** |
| production engine unchanged | sha256 `d4b71da1…` identical before/after |

```
real parse_fill: 82 RawFills; unique ids 82
MATCH: 82 exchange tids vs 75 legacy tids
  previously-missing executions = 7  -> exactly 7: True
    A sz=1.0 px=64.56 oid=570624315083 tid=42800117823942
    A sz=1.0 px=64.56 oid=570624315083 tid=21882551414374
    A sz=1.0 px=64.56 oid=570624315083 tid=670085159501746
    A sz=1.0 px=64.56 oid=570624315083 tid=503124621782083
    A sz=1.0 px=64.56 oid=570624315083 tid=957547300431610
    A sz=1.0 px=64.56 oid=570624315083 tid=563700307774545
    A sz=1.54 px=64.56 oid=570624315083 tid=654990711323648
patched-engine ingest: accepted 82 (unique), net -173.490000 vs implied -173.490000 match: True
second pass adds: 0
production engine sha256 unchanged: True
```

## Deliverables

- **Executable test:** `probes/isolated_real_engine_tidmatch_readonly.py`
  (copies+patches the engine, drives the real `parse_fill`, asserts exactly-7 / adds-zero / unchanged).
- **Reviewable source patch:** `hl_copy_output/recon_evidence/isolated_engine/tid_fix_reviewable.patch`
  (one-line `+ tid` and one-line id change; backward-compat note).

## Backward compatibility

Existing rows keep the legacy composite id; the corrected id only **adds** the native `tid`, so it is a
superset discriminator — previously-distinct rows stay distinct, previously-colliding rows separate.
Historical rows are never rewritten; a one-time re-ingest accepts exactly the collided executions.

## Status

Root cause CONFIRMED; correction PROVEN against the real engine on all required properties, in isolation.
**No production deployment authorised.** `@188` and `XYZ:SOFTBANK` unchanged.

## Nonce
NONCE=WP8014-REAL-ENGINE-TID-FIX-20261012-001
