GATE C COMPLETE - commit 74d17b4b, pushed and verified on the remote tip.

COMMIT  74d17b4b   (amended my own earlier 6ec98317+d43c280c; force-with-lease)
BRANCH  wp-hip3-builder-dex
REMOTE  verified: git show origin/wp-hip3-builder-dex:WALLET FINDER/HL_Copy_Engine_SSOT.py
        contains 18 references to the Gate C sentinel; remote tip is 74d17b4b.

FIRST, THE PART THAT MAY DISAGREE WITH YOUR SCOPE: the capacity weight model is
CORRECT and I did not touch it. SSOT_FILL_WEIGHT=20 matches userFillsByTime's
default 20 and SSOT_SNAPSHOT_WEIGHT=2 matches clearinghouseState's cheap 2 in
hl_rate_guard._WEIGHTS, and capacity_achievable was ALREADY genuinely enforced
in inputs_current via build_currentness. No accounting change was needed. If you
expected a different number here, this is the evidence it is already right.

THE DEFECT WAS ADJACENT, AND WORSE THAN A CAPACITY SHORTFALL.

RATE_GUARD.acquire() returns False when the process is out of weight, and the
skip sites returned None - the same value a genuinely unusable payload returns.
_process_wallet_proof counted that None as a poll failure, and build_currentness
treats ANY wallet with consecutive_poll_failures > 0 as failed. Two consequences:

  1. inputs_current failed for a reason that had nothing to do with exchange
     truth. A wallet that was merely STARVED looked exactly like a wallet whose
     multi-DEX union was incoherent.
  2. poll_loop rescheduled it a FULL cadence later regardless, so one refused
     request cost an entire cycle of freshness on which it had spent no budget
     at all. Self-sustaining: it waits, returns still starved, waits again.

So your capacity defect and the starvation loop were one bug in two coats. The
system could not distinguish "we did not get to ask" from "we asked and the
answer was bad" - and that distinction is the whole game for inputs_current.

THE FIX - scheduler and accounting only. No proof, epoch, drift or watermark
semantics changed.

  - RATE_BUDGET_DEFERRED sentinel: a refused request carries its own cause out to
    the caller instead of the caller inferring it.
  - a per-wallet deferral COUNT, deliberately separate state from the failure
    counter so no arithmetic on one can imply the other.
  - the deferral is branched on BEFORE any failure increment.
  - poll_loop reschedules a deferred wallet on DEFERRED_RETRY_MS (60s). It cannot
    overspend: RATE_GUARD.acquire still refuses anything over budget. The timer
    only stops a deferral costing a whole cycle; it does not pace traffic.
  - a completed cycle CLEARS the deferral, so a healed wallet is not reported
    starved forever.
  - build_currentness reports wallets_rate_budget_deferred - visible, never
    folded into `failed`.

TWO CORRECTIONS TO MY OWN FIRST ATTEMPT. Both were caught by my own tests, and
you should weigh them because the first one is the kind of bug that ships green.

  1. I first detected a refusal by diffing the process-wide
     rate_budget_skipped_fill_fetches counter across each call. Check C10 proved
     that UNSOUND: a concurrent skip belonging to a DIFFERENT wallet relabelled
     this wallet's genuine network failure as `deferred`. That is a false green
     in the exact direction this gate exists to prevent. Replaced with a sentinel
     that carries its own cause.
  2. The sentinel is FALSY so legacy `if not rows` checks keep failing closed -
     which means `snapshot is None` does NOT catch it. I audited all four
     snapshot call sites (bootstrap, baseline re-measure, legacy
     fetch_wallet_positions, poll). Each now identity-tests the sentinel BEFORE
     its None check, so a refusal can never be mistaken for a measurement or roll
     a "measured" epoch. C11 locks that in.

FAIL-CLOSED IS PRESERVED - nothing was weakened to make this green.
  - a deferral advances NO watermark and records NO snapshot (C9)
  - genuine bad payloads still count as failures (C3)
  - genuine incoherent proofs still count as failures (C4)
  - a genuinely failed wallet still drives inputs_current False (C5b)
  - a foreign skip cannot relabel a real failure (C10)

EVIDENCE
tests/test_gate_c_capacity_deferral.py - 12/12 on the fixed engine.
Mutation vs 6ec98317: rc=1. SEVEN checks fail (c1,c2,c5,c6,c7,c8,c11) while all
four controls (c3,c4,c9,c10) still PASS. That asymmetry is the evidence that
matters: the suite catches THIS defect, not merely any behaviour change.

C6 is behavioural, not a source-text check - it drives one real poll_loop pass and
reads the due time back. Pre-fix a deferred wallet is rescheduled in 1,728,002 ms,
identical to a normal wallet; that measured identity IS the starvation loop.
Post-fix it is 60s.

Full suite: 11 files rc=0. wallet_proof_epoch_suite.py fails IDENTICALLY on
6ec98317 (stale stub, missing dex kwarg) - verified pre-existing, not mine.

NOT DONE, not claimed: Gate C is verified at unit level only. Nothing has run
against the live exchange, so I claim no measured cycle time for the real wallet
set. Production restart remains unauthorised and I have not deployed.

DELIVERY_NONCE=b4ch220415
