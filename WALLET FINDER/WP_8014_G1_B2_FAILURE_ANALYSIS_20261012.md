# G1-B2 Failed Patches — Read-Only Recovery Analysis

## Action ID: WP-8014-SUCCESSOR-RESUME-20261012-1
## Controller: 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
## Date: 2026-10-12
## Classification: READ-ONLY ANALYSIS — NO CODE CHANGES

---

## 1. Problem Statement

G1-B2 requires removing app writes to engine-owned `exchange_baselines.json` in BOTH staged application modules:
- `wallet_proof_engine_8014.py` (~lines 3732, 3746)
- `HL_Copy_App_SSOT.py` (~lines 2890, 2904)

The function `_build_account_reconciliation()` currently writes shared engine-owned `exchange_baselines.json`. This violates SSOT ownership separation. The helper `_account_reconciliation_baseline_timestamp()` also reads the old baseline.

## 2. Failed Attempts

### 2.1 G1-B2 First Patch (comment 6103230323)

**Approach:** Added `APP_EXCHANGE_BASELINES_JSON` under app `DATA_DIR`; replaced two `exchange_baselines.json` writes with writes to app-owned file.

**Defects identified by controller:**
1. `_build_account_reconciliation()` still loads ONLY `EXCHANGE_BASELINES_JSON` on every call — never loads `APP_EXCHANGE_BASELINES_JSON`.
2. For an account absent from legacy engine-owned baselines, every refresh constructs a new baseline from today's account value and overwrites the app-owned value → false zero change.
3. `_account_reconciliation_baseline_timestamp()` reads only legacy engine file, ignoring persisted app baseline.
4. Silenced IO errors conceal failure to persist.

**Verdict:** CHANGES_REQUIRED — directionally correct (app-owned file) but incomplete.

### 2.2 G1-B2-R1 First Patch (comment 6103301135)

**Defects (source review):**
1. `baselines={**app_baselines, **engine_baselines}` is a SHALLOW root merge. If `engine_baselines` contains `account_reconciliation` (as it normally does), that whole subtree masks `app_baselines.account_reconciliation`, even when the wallet/account key is absent in engine.
2. Merged dict written as whole to `app_exchange_baselines.json`, copying engine-owned data instead of app-only reconciliation records.
3. Bare `except Exception:pass` converts failure to persist into apparent successful modelled reconciliation/zero delta.
4. `_account_reconciliation_baseline_timestamp()` remains engine-only reader.
5. Diff envelope concatenated, not directly git-applicable.

**Verdict:** CODE REVIEW FAIL.

### 2.3 G1-B2-R1 Second Patch (comment 6103317033)

**Defects:**
- `wallet_proof_engine_8014.py.r1(2).diff` SHA256 `8ab9d1cc...` is byte-identical to previously rejected r1(1).diff.
- `HL_Copy_App_SSOT.py.r1(1).diff` SHA256 `32fd22e0...` uses SAME shallow top-level merge.
- Both still merge `baselines={**app_baselines, **engine_baselines}` → engine-owned root `account_reconciliation` dict entirely masks app-owned subtree.
- Both diffs copy engine-owned root keys into app-owned baseline file.
- Silently swallow write errors.
- Omit updating baseline-timestamp readers.
- Test `g1b2_r1_test.py` only asserts one reconciliation call and presence of app file — does NOT reload baseline, change account value to 5200 and assert net change +200, or simulate restart.
- Test fixture puts `exchange_baselines.json` at TEST_DIR root while app `DATA_DIR` is `TEST_DIR/hl_copy_output`.

**Verdict:** FAIL CONFIRMED — same definitive bug in both modules. Green test checks a weaker property. 2/2 permitted implementation attempts exhausted.

## 3. Root Cause

The core defect is a **shallow merge of two SSOT root keys**:

```python
baselines = {**app_baselines, **engine_baselines}
```

When `engine_baselines` contains `account_reconciliation` (a nested dict keyed by wallet/account), the engine's entire subtree replaces the app's subtree — even for accounts that exist ONLY in the app. After the first app-only baseline is persisted, the next refresh loses that record and initializes at the latest value, falsely showing delta $0 rather than +$200.

## 4. Controller's Acceptance Invariant (from comment 6103301135)

> choose account record by `engine account_reconciliation[account]` if valid else `app account_reconciliation[account]` if valid else initialize once to app-owned store; never shallow merge subtrees, never duplicate SSOT keys into app file, do not reset baseline if write fails, return unavailable status, update timestamp lookup, assert correct deltas across repeat loads

## 5. Smallest Safe Corrective Plan

A future separately-authorised solution must:

### 5.1 Data Selection (per-account, not subtree merge)

- For each account key, check engine `account_reconciliation[account]` first; if valid, use it.
- If absent/invalid in engine, check app `account_reconciliation[account]`; if valid, use it.
- If neither has a valid baseline, return UNAVAILABLE (do NOT initialize from current value).
- NEVER shallow-merge the two root dicts.

### 5.2 Per-Account Persistence

- Write ONLY the app-owned account's reconciliation record to `APP_EXCHANGE_BASELINES_JSON`.
- Never copy engine-owned root keys into the app file.
- Use atomic persistence (write temp, rename).
- On persist failure: return UNAVAILABLE status, never fabricate $0.

### 5.3 Timestamp Lookup

- `_account_reconciliation_baseline_timestamp()` must check BOTH engine and app sources.
- Prefer engine timestamp for engine-sourced baselines, app timestamp for app-sourced.

### 5.4 Error Handling

- No bare `except Exception:pass`.
- IO errors must propagate or be explicitly handled with UNAVAILABLE status.

### 5.5 Diff Envelope

- Use standard `git diff` format: `--- a/path` / `+++ b/path` / `@@ -l,c +l,c @@`.
- No concatenated headers.

## 6. Acceptance Tests (for future authorised implementation)

1. **Per-account selection:** Engine has account A, app has account B → A uses engine baseline, B uses app baseline, no cross-contamination.
2. **Repeat load:** Load baseline, change account value from 5000 to 5200, reload → net change = +200 (not $0).
3. **Restart persistence:** Load baseline, simulate restart (clear memory), reload → same baseline, correct delta.
4. **Write failure:** Make app file read-only → function returns UNAVAILABLE, does not fabricate $0.
5. **No SSOT duplication:** App file contains ONLY app-owned account records, no engine root keys.
6. **Timestamp correctness:** `_account_reconciliation_baseline_timestamp()` returns correct source timestamp for both engine and app baselines.
7. **Both modules:** All tests pass for BOTH `wallet_proof_engine_8014.py` AND `HL_Copy_App_SSOT.py`.
8. **G1-B1 preservation:** Purge endpoints still return HTTP 403; direct purge functions remain non-destructive.

## 7. Constraints

- G1-B1 purge guard MUST remain intact (staged PASS).
- No third patch authorised — this is analysis only.
- No deployment, no SSOT writer start, no live-copy changes.
- No production or trading authority.
- All safety holds remain.

## 8. References

- G1-B2 first patch review: #6103230323
- G1-B2-R1 code review fail: #6103301135
- G1-B2-R1 final ruling: #6103317033
- G1-B2-R1 green test rejection: #6103323797
- G1-B1-V staging PASS: #6103077769
- Controller governance correction (GitHub transport): #6107750664
