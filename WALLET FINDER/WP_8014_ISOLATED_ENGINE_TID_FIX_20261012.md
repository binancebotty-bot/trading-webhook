# WP-8014 — ISOLATED ENGINE-COPY IMPLEMENTATION, backward-compatible (ruling #6130210271)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6130210271 — implement native tid identification WITH backward compatibility in an isolated engine copy; load the existing 75 fills, verify exactly 7 new accepted, prove a second replay adds zero, prove it survives a reload/rebuild, verify production files unchanged.
**Date:** 2026-10-12
**Classification:** ISOLATED — writes only to `recon_evidence/isolated_engine/`. No production write, no network, no restart.

---

## Backward-compatibility design (the key requirement)

Existing ledger rows carry LEGACY composite ids `wallet:coin:ts:side:size:price:oid`. New rows key on
the NATIVE `tid`. The ingest is **multiset-aware**: for each legacy composite it counts how many rows the
ledger already holds and accepts only the *difference* the exchange reveals (the previously-collided
fills). Existing rows are never re-added; historical rows are never rewritten.

## Results — every requirement PASS

| Requirement | Result |
|---|---|
| Load existing 75 fills | loaded **75** |
| First ingest accepts **exactly 7** new | accepted **7**, skipped 75 → net **−173.490** |
| Second replay adds **zero** | accepted **0** (idempotent) |
| Survives reload/rebuild | persist→reload net **−173.490** (equal); rebuild re-ingest accepted **0** |
| Production files unchanged | `raw_live_fills.csv` sha256 `0f0e7cda…` size 410,755,149 — no write |
| Final net == exchange-implied | **−173.490 == −173.490** ✔ |

```
loaded existing recorded fills: 75
A. first ingest: accepted 7 new, skipped 75 already-present; net -173.490000   -> exactly 7: True
B. second ingest: accepted 0, skipped 82; net -173.490000                      -> adds zero: True
C. persisted -> reloaded 82 fills; net -173.490000; rebuild re-ingest accepted 0
D. production raw_live_fills.csv unchanged
```

## The change (isolated copy; NOT applied to production)

```python
tid = str(raw_get(raw, "tid", "id", default=""))
fill_id = str(raw_get(raw, "fill_id", default="")) or \
          f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}:{tid}"
# ingest is multiset-aware vs the legacy composite so historical rows are never double-counted
```

## Status

- Root cause CONFIRMED; backward-compatible correction PROVEN on all required properties in isolation.
- **No production deployment authorised** — source edit and any production re-ingest NOT performed.
- `@188` and `XYZ:SOFTBANK` unchanged.

## Nonce
NONCE=WP8014-ISOLATED-ENGINE-TID-FIX-20261012-001
