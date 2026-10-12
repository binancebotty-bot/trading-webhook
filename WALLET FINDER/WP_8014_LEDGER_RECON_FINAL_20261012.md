# WP-8014 — FINAL Offline Ledger Reconstruction (ruling #6129799800)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6129799800 — one final offline ledger reconstruction; outcome = (A) proven defect + specific corrective action, or (B) insufficient evidence + precise recovery action requiring authorisation.
**Date:** 2026-10-12
**Classification:** STRICTLY OFFLINE — persisted fills + persisted state only. ZERO network calls.

---

## OUTCOME: (B) INSUFFICIENT EVIDENCE — precise recovery action identified

The recorded ledger cannot reproduce the exchange position for any of the three coins. The ledger's
own arithmetic is self-consistent, so this is NOT an accounting-arithmetic defect — it is **missing
fill coverage** (historical evidence).

## Method

`probes/offline_ledger_recon_readonly.py` replays `raw_live_fills.csv` (529,147 rows scanned) for the
three wallet/coin pairs, comparing the recorded fills against the epoch baseline and the engine's
published internal.

## Evidence

| Wallet | Coin | epoch_baseline | internal Δ (Σ recorded post-fence fills) | exchange | exchange−baseline | **UNRECORDED movement** |
|---|---|---|---|---|---|---|
| `0x35519de6193f…` | `XYZ:SOFTBANK` | 1444.766 | −23.22 | 0.0 | −1444.766 | **−1421.546** |
| `0x35519de6193f…` | `XYZ:UNITREE` | −317.26 | −165.95 | −490.75 | −173.490 | **−7.54** |
| `0x4e994d450c7c…` | `@188` | 0.0 | +104325.0 | 0.0 | 0.0 | **−104325.0** |

**Ledger arithmetic is self-consistent:** for every pair, `internal Δ == Σ(recorded post-fence
signed deltas)` exactly (SOFTBANK: 42 fills → −23.22; UNITREE: 75 fills → −165.95; @188: 99 fills →
+104325). The engine is not mis-summing what it recorded.

**Coverage is incomplete:**
- SOFTBANK: 42 fills, all post-fence, **stop at 2026-10-09T06:00:31**; the last recorded fill ends at
  **0.0** while the baseline was 1444.766 — a −1444.766 move of which only −23.22 is recorded.
- UNITREE: 75 fills, stop at **2026-10-11T18:00:09**; the last fill ends at −490.75 (== current
  exchange) but the recorded delta (−165.95) ≠ exchange-implied (−173.49).
- @188: 99 fills; 85 pre-fence, 14 post-fence; recorded Δ +104325 vs exchange-implied 0.0.

**Data-integrity signal:** the recorded fills' `start_position` frequently ≠ the previous fill's
`end_position` (SOFTBANK 28/42 breaks, UNITREE 37/75, @188 86/99) — consistent with fills captured
across gapped poll windows.

## PRECISE RECOVERY ACTION (requires authorisation)

For each of the three pairs, obtain the **missing fills** over the uncovered windows:

1. `(baseline_ts, first_recorded_fill_ts]` for SOFTBANK and UNITREE — the movement between the epoch
   fence and the first recorded fill.
2. The full `(last_recorded_fill_ts, now]` window where the recorded chain ends short of exchange truth
   (@188 especially).

This is a **bounded historical `userFillsByTime` fetch** for exactly these wallet/coin pairs. It
requires **exchange access (network)**, which the offline-only ruling forbids — hence the authorisation
request. No SSOT core change, no Wallet Finder change, no trading.

## Mapping status (unchanged)

- `XYZ:SOFTBANK`, `XYZ:UNITREE`: mapping PROVEN offline (names verbatim in persisted state).
- `@188`: mapping UNPROVEN (spot-MARKET index space; no offline source).

## Nonce
NONCE=WP8014-LEDGER-RECON-FINAL-20261012-001
