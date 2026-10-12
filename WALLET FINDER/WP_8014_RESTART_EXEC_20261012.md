# WP-8014 Controlled Restart — Execution + Evidence

**Action ID:** WP-8014-SUCCESSOR-RESUME-20261012-1
**Controller:** 6accb9f5-f2b8-83eb-8f77-a1aa17937c64
**Controller ruling consumed:** #6128595296 (one controlled restart, subject to 3 safety checks)
**Date:** 2026-10-12
**Classification:** CONTROLLED RESTART — read-only harvester, no code change, no cohort alteration

---

## Pre-start safety checks (all PASS — any fail would have aborted)

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | Single-instance protection | PASS | `acquire_lock()` at L3303: reads `engine_ssot.lock`, `os.kill(old,0)` liveness test, refuses if a live instance holds it. Lock acquired this run → PID 30984. |
| 2 | Source identity | PASS | HEAD `1aa8ffd76fd7f63452c702ccba9e287ad0fd980a`; `HL_Copy_Engine_SSOT.py` working sha256 `d4b71da1…` == committed sha256 (working tree clean for the engine file). |
| 3 | Trading isolation | PASS | Only POST target is `https://api.hyperliquid.xyz/info` (read-only). No `/exchange`, no order/cancel/modify, no private key, no signing, no `eth_account`. Writer is a read-only data harvester. |

## Restart execution

- Shape: `pythonw.exe -u HL_Copy_Engine_SSOT.py` (leaf — the engine spawns no children, so pythonw is the correct headless form; no focus stealing).
- Env: scrubbed `PYTHON*`; `HL_IP_BUDGET_PATH` + `HL_IP_BUDGET_WEIGHT_PER_MIN=250` set (matches launcher).
- cwd: `WALLET FINDER`.

## Post-start evidence

| Field | Value |
|---|---|
| Engine PID | 30984 (pythonw.exe) |
| Wrapper | bash 35696 → pythonw 30984 = ONE loop |
| Lock PID | 30984 (matches process) |
| Instance count | 1 |
| Startup log | `2026-10-12T16:47:37 CAPACITY_INVARIANT wallets=58 … achievable=True` |
| engine_truth.json | FRESH (age ~17s) |
| BASELINE_SET / WALLET_READY | resuming (wallets bootstrapping) |

## Ruling compliance

- 5 non-cohort wallets: records PRESERVED (no deletion/reset) — as ruled.
- 2 rate-starved wallets: existing rate-limited collection (no budget change) — as ruled.
- No SSOT code change; no cohort alteration; no Wallet Finder work.

## Nonce
NONCE=WP8014-RESTART-EXEC-20261012-001
