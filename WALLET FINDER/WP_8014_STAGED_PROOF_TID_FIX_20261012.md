# WP-8014 — ISOLATED IMPLEMENTATION PROOF (ruling #6130153019)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6130153019 — staged proof of the tid fix: (P1) missing fills recovered, (P2) no double count, (P3) survives restart, (P4) unrelated records unchanged; plus reconcile duplicate counts.
**Date:** 2026-10-12
**Classification:** ISOLATED — no production write, no network, no restart.

---

## Duplicate-count reconciliation (as required)

Order `570624315083` has **39 exchange fills with 39 distinct `tid`** values.
Under the CURRENT scheme these produce **32 unique ids**, with two collapsed groups:
- **7 fills** (all size 1.0) collapse to **one** id → loses **6**.
- **2 fills** (size 1.54) collapse to **one** id → loses **1**.
- **Total lost = 7.**

**Correction to my earlier phrasing:** I wrote "six 1.0 fills"; precisely, it is **seven 1.0 fills
collapsing to one** (losing six), plus a 1.54 pair (losing one) = 7. The totals were right; the wording was imprecise.

## Four required properties — ALL PASS

| Property | Test | Result |
|---|---|---|
| **P1** missing fills recovered | feed the FULL 82-fill exchange set through the engine dedup | CURRENT: applied 75, net −165.950 (short **+7.540**). CORRECTED: applied 82, net **−173.490** == exchange-implied. **PASS** |
| **P2** no double count | corrected applied 82 == distinct exchange rows 82; deduped 0 | **PASS** (nothing legitimately dropped, nothing counted twice) |
| **P3** survives restart | deterministic re-run from persisted inputs | run1 = run2 = −173.490000000. **PASS** |
| **P4** unrelated records unchanged | production file untouched; scope strictly `0x35519de6193f/XYZ:UNITREE` | production `raw_live_fills.csv` sha256 unchanged, no write performed. **PASS** |

## The fix (isolated, NOT applied to production)

```python
tid = str(raw_get(raw, "tid", "id", default=""))
fill_id = str(raw_get(raw, "fill_id", default="")) or \
          f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}:{tid}"
```

Backward-compatibility note (per ruling): existing persisted ledger rows carry the OLD ids. The fix
changes the id scheme for **newly ingested** rows only; it must NOT rewrite historical rows (which would
risk double-counting). The staged proof shows re-ingesting the affected window under the new scheme
recovers exactly the 7 lost fills with zero double counts.

## Status

- Root cause CONFIRMED; correction PROVEN against all four required properties.
- **No production change made** — source edit and any re-ingest remain unauthorised.
- `@188` and `XYZ:SOFTBANK` unchanged (no correction authorised).

## Nonce
NONCE=WP8014-STAGED-PROOF-TID-FIX-20261012-001
