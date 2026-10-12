# WP-8014 — AUTHORISED RECOVERY RESULT (ruling #6129895528)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6129895528 — ≤6 rate-guarded historical fill requests (2 wallets) + 1 metadata request; correct dedup; evidence stored separately.
**Date:** 2026-10-12
**Classification:** READ-ONLY recovery. Evidence written to `hl_copy_output/recon_evidence/` only. No production data touched.

---

## Budget used

- 1 metadata request: `spotMeta` → **`@188` resolves in the SPOT MARKET space** as
  `{"tokens":[299,0],"name":"@188","index":188,"isCanonical":false}` (my earlier "SSS" read the
  TOKEN space `tokens[188]` — a different index — so the controller was right that it was unproven).
- 2 `userFillsByTime` requests (one per affected wallet). Budget left: 4.

## Correction applied

The engine's `drift_state.internal` is a **DELTA**, not an absolute position. Re-computed delta-vs-delta.

## RESULTS

### XYZ:UNITREE — PROVEN DEFECT (Outcome A)

| Quantity | Value |
|---|---|
| Exchange post-fence fills | 82 fills, net **−173.490** (= E−B = −490.75 − (−317.26)) |
| Engine recorded post-fence fills | 75 fills, net **−165.950** |
| **Missing** | **7 fills, net −7.540** |

The 7 missing fills are all one order: `oid=570624315083`, `coin=xyz:UNITREE`, `side=A`, `px=64.56`,
`time=2026-10-10T15:59:29.397–.465Z`, sizes 1.0×6 + 1.54. **Sum = −7.54 = the entire discrepancy.**

**Specific corrective action:** ingest these 7 exchange fills (re-fetch that window) so the internal
delta becomes −173.490 and the pair reconciles. This is a real missed-fill defect.

### @188 — PROVEN DEFECT (Outcome A)

| Quantity | Value |
|---|---|
| Exchange fills in window | 16 fills, net **−2134.802** |
| Engine recorded post-fence | 14 fills, net **+104325.0** |
| Engine's own fill chain (last) | `start=661294.8 → end=0.802` |
| Drift exchange / baseline | 0.0 / 0.0 |

The engine's internal **+104325 is unbacked**: it does not match the exchange fills (−2134.802) and it
does not match the engine's OWN recorded fill chain (which ends at 0.802). The internal position is
inconsistent even internally.

**Specific corrective action:** reconcile `@188` from the exchange fills — the +104325 must be
reconstructed/replaced from the recorded fill chain (which ends ~0.8). Requires an authorised
ledger-side correction.

### XYZ:SOFTBANK — DISTINCT CLASS (fills agree; exchange POSITION differs)

| Quantity | Value |
|---|---|
| Exchange fills (all) net | **+1421.546** |
| Engine modelled position (baseline + internal) | 1444.766 + (−23.22) = **+1421.546** |
| Exchange REPORTED position | **0.0** (flat) |

Here the **exchange fills and the engine AGREE exactly (+1421.546)** — but the **exchange reports the
position as 0.0**. So this is NOT an engine accounting error: the exchange's own fills imply a
+1421.546 position while its reported position is flat. That is an **exchange-side flat not explained
by any fill** (consistent with a HIP-3 dex settlement / ADL / delisting close, which produces no
ordinary fill).

**Specific corrective action:** needs an exchange-side explanation for the flat (not an engine fix).

## Honest summary

- **2 of 3 coins are PROVEN engine defects** with specific corrective actions (UNITREE: 7 missed
  fills; @188: unbacked internal position).
- **1 coin (SOFTBANK)** is an exchange-side flat that the exchange's own fills contradict — not an
  engine accounting error.
- No SSOT core change, no ledger rewrite, no epoch reset, no rate-limit change, no Wallet Finder
  change, no trading.

## Nonce
NONCE=WP8014-AUTH-RECOVERY-RESULT-20261012-001
