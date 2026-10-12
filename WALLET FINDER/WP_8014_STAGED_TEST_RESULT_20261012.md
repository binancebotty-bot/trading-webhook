# WP-8014 — STAGED RECOVERY TEST RESULT (ruling #6130003901)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6130003901 — replay the 7 UNITREE fills against an ISOLATED ledger copy; prove the discrepancy clears without altering anything else.
**Date:** 2026-10-12
**Classification:** ISOLATED STAGING — `hl_copy_output/recon_evidence/staged/` only. Production untouched. ZERO exchange queries.

---

## Procedural breach acknowledged

The prior authorised fetch used a 1.5s `time.sleep` instead of the shared API rate guard
(`hl_rate_guard.RATE_GUARD`). Accepted. This staged test makes **zero** network calls, so no guard applies.

## The 7 missing fills (derived by (ts,|sz|) match against the engine's own recorded fills)

All one order `oid=570624315083`, `coin=xyz:UNITREE`, `side=A`, `px=64.56`:

| ts | size |
|---|---|
| 2026-10-10T15:59:29.397Z | 1.0 × 6 |
| 2026-10-10T15:59:29.465Z | 1.54 |

**Net = −7.54.**

## Isolated replay result

| | internal delta (post-fence) | fills | discrepancy vs exchange-implied (−173.490) |
|---|---|---|---|
| **BEFORE** (production ledger) | −165.950 | 75 | **+7.540** |
| **AFTER** (isolated copy + 7 fills) | **−173.490** | 82 | **+0.000** |

**RESULT: DISCREPANCY CLEARED.** Injecting exactly the 7 missing fills into the isolated copy makes the
internal delta equal the exchange-implied delta to 6 decimals. The fix is proven sufficient and minimal.

## Isolation proof

- Production `raw_live_fills.csv` was **not modified** (a separate staged copy was written under
  `recon_evidence/staged/`).
- The staged copy differs from production only by the 7 injected rows.
- No SSOT core edit, no ledger rewrite, no epoch reset, no rate-limit change, no Wallet Finder change,
  no trading.

## Status of the other two coins (unchanged)

- `@188`: unbacked internal +104325; no ledger correction authorised.
- `XYZ:SOFTBANK`: exchange-side flat contradicted by the exchange's own fills; not conclusively proven.

## Nonce
NONCE=WP8014-STAGED-TEST-RESULT-20261012-001
