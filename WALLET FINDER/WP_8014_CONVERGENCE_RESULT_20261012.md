# WP-8014 — Bounded Convergence Check RESULT: NOT CONVERGED (precise blocker)

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Ruling consumed:** #6128817080 (bounded 45-min convergence check)
**T0:** 2026-10-12T16:59:45Z  **DEADLINE:** 2026-10-12T17:44:45Z
**Result:** NOT CONVERGED — 4 blockers remain. Precise cause identified below (not a repeat investigation).

---

## Headline

The dashboard pill is still **CONVERGING**. The writer is healthy and producing fresh truth
(engine_truth mtime <5s, 211 distinct wallets polled, 38/56 watermarks advanced past the restart).
Convergence failed for a **specific, named reason per wallet** — it is NOT a stall and NOT a core defect.

## CLASS A — 2 wallets with NO watermark (blocks inputs_current)

`0x1003d8c532e5178673668e5f3aa48dc399ee940c`, `0xf9705511f4d0ae48aa128f73518c776fbda27ba7`

- Both are now `ready=True`, `epoch_status=OPEN` (bootstrapped 17:19 / 17:22).
- Yet neither has ANY `proof_watermark` entry, so `MIN(watermark over 58 required)=0` → `inputs_current=False`.
- Precise cause: `_advance_trusted_through()` only writes a watermark when a **complete** interval
  succeeds. These wallets' intervals keep landing on `rate_budget_deferred`
  (e.g. `BOOTSTRAP_UNION_INCOHERENT … reason=rate_budget_deferred action=fail_closed_no_epoch_no_ready`),
  so the interval is incomplete → watermark never advances. All 56 other required wallets DO have a
  watermark (min 2026-10-10T13:23Z; 38 advanced past the restart).

## CLASS B — 2 wallets whose epoch cannot promote (blocks state_trustworthy)

Promotion needs `pending_epoch_can_promote()`: a complete post-fence interval AND per-coin
`exchange − epoch_baseline == internal` within POSITION_EPSILON. Both fail on specific coins:

### `0x35519de6193f0e6581032d87236988629e35a281` (PENDING)
- `XYZ:SOFTBANK`: internal **-23.22** vs exchange **0.0** (baseline 1444.766) — engine models a
  nonzero position the exchange reports FLAT.
- `XYZ:UNITREE`: internal **-165.95** vs exchange **-490.75** (baseline -317.26).
- Real fills were re-ingested (128 applied) and still `DRIFT_UNRESOLVED` → the modelled delta cannot
  be closed from the ledger; a genuine external/flat divergence on a HIP-3 coin.

### `0x4e994d450c7c710ba4baeda5e249acc71f80aa1b` (PENDING)
- `@188` (spot-index coin): internal **+104325.0** vs exchange **0.0** — `UNRESOLVED_ESCALATED`.
- Every drift-recovery ingest for this coin fails on rate budget
  (`REAL_FILL_INGEST_FAILED … fetch_reason=rate_budget_deferred`), so `applied=0`, `complete=False`.

## Audit snapshot (deadline)

`bootstrap_snapshot_deferred_rate_budget=43`, `poll_cycle_deferred_rate_budget=17`,
`rate_budget_deferrals_resolved=40`, `drift_unresolved=38`, `drift_recovered=12`,
`hip3_fence_proof_unavailable=4`.

## Precise conclusion (what a fix would need — NOT proposed as authorised work)

1. **CLASS A:** the 2 wallets need ONE complete (un-deferred) interval to earn a watermark. Under the
   shared 250 wu/min ceiling this is a scheduling/latency effect, not a defect.
2. **CLASS B:** the 2 wallets need their per-coin drift closed. `XYZ:SOFTBANK` and `@188` are modelled
   nonzero vs exchange FLAT — closing them requires either real fills that reconcile the divergence or
   an authorised adjudication (they are the `hip3_fence_proof_unavailable` class). **No code change is
   implied by this analysis.**

## Constraints honoured

No SSOT core change; no cohort change; 5 historical non-cohort PENDING records untouched;
no Wallet Finder work; one writer; no unnecessary restarts.

## Nonce
NONCE=WP8014-CONVERGENCE-RESULT-20261012-001
