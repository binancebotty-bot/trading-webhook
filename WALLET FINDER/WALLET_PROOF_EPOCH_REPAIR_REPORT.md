# Wallet Proof Engine — Epoch Repair Report

**Engine:** `HL_Copy_Engine_SSOT.py` (Hyperliquid Wallet Proof / 8014)
**Repo:** `binancebotty-bot/trading-webhook` · **Base commit:** `509d3580` · **Branch:** `wallet-proof-epoch-repair`
**Date:** 2026-10-07 · **Scope:** bounded proof-engine repair only (no sender, no Build-4, no UI rewrite)

---

## 1. What was wrong (root causes, proven)

| # | Defect | Evidence |
|---|---|---|
| D1 | **Fail-open pagination.** Page considered "complete" when rows `< 500`; on page-cap trip the code logged *"admitting a data gap"* and **advanced the cursor past the unread region**. | live API probe: hard cap is **2000**, not 500 |
| D2 | **Silent fill loss at tied timestamps.** Cursor advanced to `max_ts + 1`, dropping any fill sharing the boundary timestamp. | live probe: **336 timestamps carry >1 fill (max 15)** on one wallet |
| D3 | **Baseline absorption.** `baseline = exchange − internal` makes `delta ≡ 0` by construction, so any pair could be forced **CLEAN** without ever reconciling. | old truth: **1435 CLEAN / 934 UNRESOLVED**; the 1435 were unverifiable |
| D4 | **Epoch-mixed accumulation.** `monitor_start_ms` resolves to 0 → internal position = Σ(**all** ledger fills), i.e. the model assumes every wallet was flat at the start of the whole ledger, not at its baseline. | 18/1435 CLEAN pairs include pre-baseline fills |
| D5 | **Provenance mislabelling.** REST/poll fills recorded as `WS_CAPTURED`. | `normalise_recording_method` |
| D6 | **Non-convergent recovery.** Barren recovery retried forever (or quiesced) while the pair stayed known-wrong; no escalation, no durable gate. | audit counters |
| D7 | **No currentness contract.** No way to state whether output is current vs trustworthy. | absence of any watermark |

---

## 2. What changed (all in `HL_Copy_Engine_SSOT.py`, +526 / −126)

**Provenance (D5)**
- New factual recording methods: `WS_CAPTURED` (genuine WS), `POLL` (REST), `REBUILD` (backfill).
- `normalise_recording_method(value, source, is_snapshot)` derives the label from the **actual source**, never inferred upward.
- `recording_contributes_to_execution_delta` = {`WS_CAPTURED`, `POLL`} only — `REBUILD` can never contribute measured delta.

**Fetch completeness — fail closed (D1, D2)**
- `API_FILLS_PAGE_CAP = 2000` (live-proven).
- A page is complete **only** when `rows < 2000`.
- Compound cursor: advance from the **inclusive** boundary, dedupe by stable `fill_id`; **no `max_ts + 1`**.
- Saturated boundary (a full page that cannot advance) → `COMPLETENESS = UNKNOWN`, reason `saturated_boundary`, **fail closed**. The old "admit a data gap" path is gone.
- `fetch_fills_since` now returns `{ok, complete, reason, rows}`.

**Proof epoch (D3, D4)**
- New authoritative file `hl_copy_output/proof_epoch.json`:
  `epoch_id`, `baseline_ts_ms`, measured `baseline_position`, `baseline_provenance`, `epoch_status`, `epoch_history`.
- Opening a fresh epoch: snapshot **measured** now, record `baseline_ts`, set `internal_delta = 0`, consume only fills **strictly after** the boundary.
- `derive_exchange_baseline_from_current_state` **no longer absorbs drift** — it may only open an epoch and mark the pair; a fresh snapshot **never** makes an old unresolved epoch CLEAN.
- `rebuild_from_ledger` is epoch-aware: a fill contributes to internal position **only if strictly after the epoch boundary**; pre-epoch fills are skipped and counted.
- Closing an unresolved epoch archives it as `UNRESOLVED` + reason (visible permanently).

**Recovery convergence (D6)**
- Barren-attempt gate now **durable** (`hl_copy_output/recovery_gate.json`, survives restart — a restart is not new evidence).
- After `HL_DRIFT_ESCALATE_AFTER_ATTEMPTS` (default 3) barren attempts a still-wrong pair becomes **`UNRESOLVED_ESCALATED`** — a *louder* signal, never a quiet acceptance of a known-wrong state.

**Currentness (D7)** — smallest explicit contract, in `engine_truth.json → currentness`
- `per_wallet_trusted_through_ms` = end timestamp of an interval whose completeness was **proven** (not the last fill's ts — a quiet wallet still advances on a complete empty poll).
- `global_trusted_through_ms = MIN(required wallets)` — a fast wallet can never make a lagging one look current.
- `inputs_current` and `state_trustworthy` reported **separately** (current input ≠ correct accounting).

**Drift status machine** — `CLEAN` / `RECOVERING` / `DRIFT_UNRESOLVED` / `UNRESOLVED_ESCALATED`, each with ts + reason.

---

## 3. Regression suite — 26/26 PASS

Run: `python WALLET FINDER/tests/wallet_proof_epoch_suite.py`

| Area | Tests |
|---|---|
| Provenance | poll→POLL, ws→WS_CAPTURED, snapshot→REBUILD, REBUILD ineligible for delta |
| Fetch completeness | >2000 rows proven complete; tied timestamps not dropped; saturated boundary fails closed; transient failure fails closed |
| Epoch model | no epoch → UNRESOLVED; epoch match → CLEAN; **snapshot change never CLEAN**; closed epoch retained UNRESOLVED |
| Trusted-through | complete empty poll advances; incomplete poll does **not** |
| Currentness | global = MIN; lagging wallet → not current; fast wallet cannot mask; inputs_current True while state_trustworthy False |
| Recovery | ≥3 barren attempts → UNRESOLVED_ESCALATED (still visibly unresolved) |
| Restart | epoch, watermark, attempt-count all survive restart |
| Deterministic rebuild | pre-epoch fill excluded; identical across instances |

## 4. Cold-restart test on real persisted data

Copied real `exchange_baselines.json` + `engine_truth.json` (184 wallets, 2375 pairs) into scratch; replayed the **exact absorption case** (exchange snapshot == stored baseline):

| Engine | CLEAN | UNRESOLVED |
|---|---|---|
| OLD (absorption) | 1435 | 934 |
| CORRECTED | **0** | **662 / 662** |

→ The absorption pathway is closed. A baseline-equal snapshot no longer yields CLEAN.

## 5. Live API evidence (`userFillsByTime`)

- max rows = **2000** (wide window returns exactly 2000, oldest-first)
- ascending by time; `startTime` **inclusive** (boundary fill returned)
- **tied timestamps exist** (336 timestamps with >1 fill, max 15 on one wallet)
- ⇒ `HISTORICAL_FILL_COMPLETENESS = UNPROVABLE` for arbitrary historical gaps;
  correctness primitive is **fresh measured re-baselining**, not historical backfill.

## 6. Evidence preserved before repair

`hl_copy_output/pre_epoch_archive/20261007_164230/` (+ SHA256 `_MANIFEST.json`):
`engine_truth.json`, `live_state.json`, `exchange_baselines.json`, `historical_drift_and_baselines.json`, `engine_source.py`.

---

## 7. Status

- Working tree contains **only** this one engine file's change plus this report and the test suite.
- The running engine process (started pre-edit) keeps the OLD code in memory; the repair takes effect on the **next restart**, which is operator-controlled.
- New runtime files (`proof_epoch.json`, `recovery_gate.json`, `proof_watermark.json`) are created on first run.

**GO / NO-GO: GO** — model is sound; defects were implementation-level and are now located, fixed, and tested. Success is *not* "the new epoch begins CLEAN"; it is the engine staying correct while processing new fills and across restart/recovery.

---

## 8. GPT review response — four gates corrected (revision 2)

GPT review ruling `CHANGES_REQUIRED` identified four correctness blockers plus a
secondary trust-contract defect. All are now fixed and covered by dedicated tests.

| Gate | Defect | Fix |
|---|---|---|
| **G1** | `fetch_exchange_positions()` returning `None` was converted to `{}`, then used to open a fake zero "measured" epoch. | `fetch_exchange_positions` now returns `(positions, fence_ms)`; on failure `(None, 0)`. `bootstrap_wallet_from_exchange` **aborts**: no epoch, no READY. |
| **G2** | Epoch timestamp was a pre-request local clock (`now`), captured possibly at the start of the whole sweep — a fill between that timestamp and the snapshot could be double-counted. | Epoch is fenced at the **server** `clearinghouseState.time` (the same clock that timestamps fills). A snapshot with no `time` fails closed. Rebuild excludes fills `<= fence`, includes `> fence`. |
| **G3** | `OPEN_UNVERIFIED_LEGACY` had no promotion path → permanent unresolved. | Replaced with **`PENDING`**. `_maybe_promote_epoch()` promotes PENDING → `OPEN` **only** after (a) a complete post-fence interval (`post_epoch_complete_interval`, set by `_advance_trusted_through`) **and** (b) an independent reconciliation for every coin. Legacy persisted status maps to PENDING on load. |
| **G4** | Incomplete fetch still advanced `last_poll_ts_by_wallet` to `end_ms`; a later complete poll could leap the hole and report `inputs_current=True`. Restart resumed from a possibly-partial ledger tail. | Cursor advances **only** when `complete`. Restart resume point = `min(last_ledger_ts, trusted_boundary)` (trusted boundary = watermark, else epoch fence) — never past an unresolved gap. |
| **G5** | `state_trustworthy` was `total > 0 and unresolved == 0` — an unaudited/flat wallet could vanish from the gate. | Now also requires **every required wallet to have a proven epoch (`OPEN`)**; `wallets_without_proven_epoch` is reported. |

**Revision-2 test suite: 41/41 PASS** (was 26). New gates G1–G5 covered explicitly:
snapshot-failure rejection; server-fence timestamp + at-fence fill exclusion;
PENDING→OPEN convergence (positive and mismatch-negative); incomplete-interval
no-leap across restart; trustworthy requires a proven epoch per wallet.

### Honest note on CI
GitHub shows **no CI checks / workflow runs** on the review branch — the 41/41
result is author-executed locally, not independently run by GitHub. Reviewers who
want independent execution should run
`python "WALLET FINDER/tests/wallet_proof_epoch_suite.py"` (self-contained, mocked
network, scratch IO; never touches live state).
