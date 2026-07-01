# FOLDER ISOLATION AUDIT — WALLET FINDER vs LIVE WALLET TRADING

**Audit Timestamp:** 2026-07-01 11:02:02 UTC
**Proof Folder:** FOLDER_ISOLATION_AUDIT_20260701_110202
**Auditor:** Freebuff (read-only, no patches, no commits)
**Reference Accounting Task:** PE_ACCOUNTING_SSOT_FIX_20260701_095449

## RESULT: PASS ✅

All 8 pass-gates cleared. WALLET FINDER and LIVE WALLET TRADING remain properly isolated. No cross-contamination detected.

---

## 1. REPOSITORY LAYOUT

| Item | Detail |
|------|--------|
| Parent repo root | C:\Users\wigmore\trading_stack\Hyperliquid scanner |
| Parent repo branch | live-plumbing-from-dashboard-fixed |
| WALLET FINDER git top-level | Same as parent repo |
| WALLET FINDER tracking | Entirely UNTRACKED (??) in parent repo |
| LIVE WALLET TRADING git | Independent repo (own .git), branch module-4-targets |
| LIVE WALLET TRADING remote | None configured |
| LIVE WALLET TRADING in parent | Tracked as single entry: M "LIVE WALLET TRADING" (directory ref) |
| Submodule? | No .gitmodules exists — not a formal submodule |

### Parent repo dirty files (WALLET FINDER scope):
- M .gitignore
- M 3hl_stage2_useful_wallet_scanner.py
- M "LIVE WALLET TRADING" (directory reference)
- M hl_stage2/HL_Copy_App_SSOT.py
- M hl_stage2/HL_Copy_Engine_SSOT.py
- M hl_stage2/HL_Live_Copy_Service_Core.py
- M hl_stage2/test_harness_ssot.py
- M wallet_copyability_test.py
- D Unused/hl_probe.py (deleted)
- D Unused/wallet_copyability_debug.py (deleted)
- D dashboard_app.py (deleted)
- D hl_stage2/HL_Copy_App.py (deleted)
- D hl_stage2/HL_Copy_Engine.py (deleted)
- D hl_stage2/HL_Live_Copy_Service.py (deleted)
- D hl_stage2/test_harness.py (deleted)
- ?? WALLET FINDER/ (entire directory untracked)

### LIVE WALLET TRADING independent repo dirty files:
The module-4-targets branch has extensive pre-existing changes:
- Deleted: Most documentation, test files, configs, launchers, proofs — large-scale cleanup
- Modified (source): proof_core/audit_log.py, config.py, convergence_sender.py, run_shadow_runtime_readonly.py, ui_server.py, dashboard.html
- Modified (generated): configs/shadow_runtime.json, graphify-out/ files
- Untracked: configs/product_config.live.json, launchers/launch-live-full-stack.ps1, proof_core/account_history.py, new proof directories

---

## 2. TIMESTAMP ISOLATION

Reference timestamp: 2026-07-01 09:54:49 (accounting task proof folder creation)

### LIVE WALLET TRADING files modified AFTER reference:
ALL 13 files are generated runtime output in reports/. Zero source, config, or launcher files were modified.

| File | Timestamp | Size | Classification |
|------|-----------|------|----------------|
| reports/product_audit.jsonl | Jul 1 10:53 | 74,657,710 | Generated — runtime audit log |
| reports/raw_shadow_snapshots.jsonl | Jul 1 10:53 | 84,440,059 | Generated — shadow snapshots |
| reports/shadow_runtime_history.jsonl | Jul 1 10:53 | 52,351,667 | Generated — history |
| reports/would_place.jsonl | Jul 1 10:53 | 9,920,209 | Generated — order ledger |
| reports/account_history.jsonl | Jul 1 10:53 | 5,867,902 | Generated — account history |
| reports/runtime_stdout.log | Jul 1 10:53 | 1,559,320 | Generated — runtime log |
| reports/runtime_stderr.log | Jul 1 10:48 | 185,862 | Generated — error log |
| reports/live_runtime_state.json | Jul 1 10:53 | 23,602 | Generated — state snapshot |
| reports/shadow_runtime_report.html | Jul 1 10:53 | 10,447 | Generated — report |
| reports/equity_curve.jsonl | Jul 1 10:53 | 8,445 | Generated — equity data |
| reports/validation_summary.json | Jul 1 10:53 | 6,193 | Generated — validation |
| reports/validation_summary.md | Jul 1 10:53 | 2,627 | Generated — validation report |
| reports/account_history_proof.json | Jul 1 10:53 | 1,831 | Generated — proof artifact |

Assessment: These are LIVE WALLET TRADING's OWN ongoing runtime activity. The WALLET FINDER accounting task did NOT write into LIVE WALLET TRADING.

### WALLET FINDER files modified AFTER reference:
- data/hl_wallets_filtered.csv (Jul 1 10:53) — wallet filter output
- engine_ssot_stdout.log (Jul 1 10:53) — engine log
- graphify-out/ cache files (Jul 1 10:26-10:27) — graph analysis cache

Assessment: Data/log/cache files only. No source code modifications.

---

## 3. CROSS-PATH REFERENCE AUDIT

### WALLET FINDER → LIVE WALLET TRADING

| Search Term | Hits | Classification |
|-------------|------|----------------|
| LIVE WALLET TRADING | 20 hits, ALL in proof files | SAFE — Historical git status records |
| Live Copy Control Centre | 0 | N/A |
| launch-live-full-stack | 0 | N/A |
| product_config.live | 0 | N/A |
| proof_core | 0 (in Python source) | N/A |
| convergence_sender | 0 | N/A |
| HL_PRIVATE_KEY_PATH | 0 | N/A |
| HL_ENABLE_LIVE_SEND | 0 | N/A |
| Global Live Copy Controls | 1 (HL_Copy_App_SSOT.py:10731) | SAFE — UI label string |
| transport | 1 (wh.transport_stale_ms at line 11707) | SAFE — Read-only UI indicator |

Verdict: No source code imports, launches, or writes to LIVE WALLET TRADING.

### LIVE WALLET TRADING → WALLET FINDER

| Search Term | Hits | Classification |
|-------------|------|----------------|
| WALLET FINDER | 0 | N/A |
| Wallet Proof Engine | 0 | N/A |
| HL_Copy_App_SSOT | 0 | N/A |
| 8014 | 0 | N/A |

Verdict: Zero references. Complete isolation from the LIVE side.

---

## 4. RUNTIME / PORT ISOLATION

| Check | Result |
|-------|--------|
| Port 8014 listening? | Yes |
| PID | 57448 |
| Executable | python.exe |
| Command line | HL_Copy_App_SSOT.py |
| Start time | 2026-07-01 10:23:45 |
| Working directory | WALLET FINDER |
| Imports live sender? | No |
| Launches live senders? | No |
| Port 8014 = Wallet Proof Engine? | Yes |

---

## 5. GRAPHIFY TIMEOUT AUDIT

| Check | Finding |
|-------|---------|
| WALLET FINDER graphify-out? | Yes — July 1 cache files (10:26-10:27) |
| LIVE WALLET TRADING graphify July 1? | No — last run June 29 |
| Graphify touched LIVE WALLET TRADING? | No |
| Partial write evidence? | None detected |
| Timeout indication? | .graphify_root empty, but otherwise normal |

Verdict: Graphify ran from WALLET FINDER only. Did NOT mutate LIVE WALLET TRADING.

---

## 6. ACCOUNTING PROOF SANITY

| Metric | Expected | Actual | Match? |
|--------|----------|--------|--------|
| Selected wallets | 84 | 84 | ✅ |
| Row accounting mismatches | 0 | 0 | ✅ |
| Portfolio lead alloc | 8400.0 | 8400.0 | ✅ |
| Portfolio copy alloc | 8400.0 | 8400.0 | ✅ |
| Portfolio delta | -1019.65825039 | -1019.65825039 | ✅ |
| Cache health | OK_RAW_CURRENT_SOURCE_MOVED | OK_RAW_CURRENT_SOURCE_MOVED | ✅ |
| Source hash | — | E684A82A... | Confirmed |
| Contract warnings | 5 | 5 | ✅ |

All values verified. 5 contract warnings are pre-existing win-rate/zero-exit fee validation.

---

## 7. COMMIT SAFETY PLAN

### Safe commit strategy:
- Stage: git add "WALLET FINDER/"
- Exclude: git reset -- "LIVE WALLET TRADING"
- Verify: git diff --cached --stat shows zero LIVE WALLET TRADING files
- NEVER use git add -A or git add .

---

## 8. PASS GATE SUMMARY

| # | Gate | Status |
|---|------|--------|
| 1 | No LIVE WALLET TRADING source/config/runtime modified by WF task | ✅ PASS |
| 2 | No WALLET FINDER runtime writes into LIVE WALLET TRADING | ✅ PASS |
| 3 | No WALLET FINDER imports/launches live sender modules | ✅ PASS |
| 4 | Port 8014 = Wallet Proof Engine only | ✅ PASS |
| 5 | Graphify timeout did NOT mutate LIVE WALLET TRADING | ✅ PASS |
| 6 | Commit plan excludes LIVE WALLET TRADING | ✅ PASS |
| 7 | No old PE_FIX_V1 scripts were run | ✅ PASS |
| 8 | No new patches made during audit | ✅ PASS |

---

## 9. CONCLUSION

FOLDER ISOLATION: CONFIRMED & INTACT

- WALLET FINDER and LIVE WALLET TRADING are properly isolated
- LIVE WALLET TRADING dirty state is pre-existing and unrelated
- WALLET FINDER accounting/cache work did NOT touch LIVE WALLET TRADING
- No cross-contamination in either direction
- Port 8014 remains pure Wallet Proof Engine
- Accounting proof internally consistent
- Safe to commit WALLET FINDER using explicit pathspecs

Blocker Report:
