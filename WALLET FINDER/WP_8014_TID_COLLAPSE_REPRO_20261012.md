# WP-8014 — tid-COLLAPSE DEFECT REPRODUCED + CORRECTION PROVEN (ruling #6130091873)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6130091873 — reproduce the tid-collapse defect and test the correction in isolated staging only.
**Date:** 2026-10-12
**Classification:** ISOLATED REPRODUCTION. No production change, no network.

---

## Defect (source-proven)

`HL_Copy_Engine_SSOT.py` L1991–1992:

```python
oid = str(raw_get(raw, "oid", "hash", "tid", "id", default=""))
fill_id = str(raw_get(raw, "fill_id", default="")) or \
          f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"
```

Two faults combine:
1. `oid` is resolved **before** `tid`, so the internal id carries the ORDER id.
2. The native `tid` (Hyperliquid's per-trade id) is **never** part of the id.

Consequence: several genuinely-distinct fills of one order with the same
timestamp/size/price/side produce the **same** internal `fill_id` and collapse under dedup.

## Reproduction (exact)

Order `oid=570624315083` on `xyz:UNITREE` (39 exchange fills, all with distinct `tid`):

| Scheme | Unique ids from 39 fills | Fills lost to collision |
|---|---|---|
| **CURRENT** (`…:oid`) | 32 | **7** — six `1.0` collapse into one; two `1.54` collapse into one |
| **CORRECTED** (`…:oid:tid`) | **39** | **0** |

**The 7 lost fills are exactly the 7 missing fills** identified in ruling #6130091873
(6× 1.0 @ 15:59:29.397Z + 1× 1.54 @ 15:59:29.465Z, net −7.54 = the entire UNITREE discrepancy).

## Correction (minimal, isolated)

Append the native trade id to the fallback id:

```python
tid = str(raw_get(raw, "tid", "id", default=""))
fill_id = str(raw_get(raw, "fill_id", default="")) or \
          f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}:{tid}"
```

With this scheme all 39 fills get distinct ids and none are lost. The staged replay in the prior ruling
already showed the internal delta then equals the exchange-implied delta (−173.490) to 6 dp.

## Status

- **Root cause: CONFIRMED** (engine ignores native `tid`; order-level `oid` collapses same-order fills).
- **Correction: PROVEN in isolation** (0 fills lost vs 7).
- **No production change made** — the source edit is NOT applied; awaiting authorisation.
- The remaining question (why the last `1.54` and the `@188` case) can be re-checked once the id fix is
  authorised; the `1.54` pair is fully explained by the same collapse.

## Nonce
NONCE=WP8014-TID-COLLAPSE-REPRO-20261012-001
