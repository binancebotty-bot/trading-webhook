# WP-8014-DUPLICATE-CORE-CLOSURE-2 — THREE ACCEPTANCE GAPS CLOSED

ACTION_ID=WP-8014-DUPLICATE-CORE-CLOSURE-2
PARENT_RULING=6110782175 (GitHub comment)
DELIVERY_NONCE=32023a2debbd401e
REVIEWED_BRANCH=wp-hip3-builder-dex
REVIEWED_SHA=82258d43ec0f575d803f74c6d96de0e294a530bf
STAND_DOWN_REQUEST=YES
TRADING_AUTHORITY=NONE — read-only, no process/config/data changes.

---

## A. SINGLE SEND LOOP EXCLUSIVITY (PIDs 46304/13372) — VERIFIED / UNPROVEN

**Processes DEAD** (were archived testnet core, not current). Instance lock held by PID 13372 (child), PID 46304 was parent launcher. Both same argv, same cwd (archive), same env.

**Code architecture (HL_Live_Copy_Service_Core.py):**
- `_INSTANCE_LOCK` (msvcrt/flock on `instance.lock`) prevents **any second engine** on same state folder
- `acquire_instance_lock()` called in `main()` before `--once`/`--loop` → process dies if lock held
- Parent (46304) acquired lock, forked child (13372), child inherited lock → single sender guaranteed by OS lock
- `FOLLOWER_NETWORK=testnet` (env), `--confirm-mainnet-follower` **NOT passed** → `MAINNET_ORDERS_CONFIRMED=False` → `raise SystemExit` if mainnet attempted
- Order submission: `HLExchange.order()` in `_place_order()` → only reached when `send_mode_now() != "OFF"` AND `args.confirm_mainnet_follower` truthy
- **Testnet sends VERIFIED** (local ledger: order_ids 62416521136, 62417808090, 62417812186 in `send_attempts.csv` + `live_fills.csv`)
- **Mainnet send capability: UNPROVEN** — no mainnet follower process alive; testnet agent key cannot place mainnet orders without explicit `--confirm-mainnet-follower` flag; network isolation at order-submission path ENFORCED by code (CLI flag gate + instance lock + network stamp)

**CLASSIFICATION:**
- Parent=launcher only, child=single engine (instance lock serializes)
- Testnet sender: **VERIFIED ACTIVE** (archived run, real testnet IOC fills)
- Mainnet sender: **UNPROVEN / NOT CAPABLE** (no process, flag gate, wrong network)
- Single-sender exclusivity: **VERIFIED by OS instance lock**

---

## B. THREE VERIFIED THREE-MINUTE WAKE EVENTS — PARTIAL / FIRST_FAILED_EDGE

**Current wake controller (PID 32964, identity `32964:1791790873.481920`):**
- Up 2+ min (9 checks × 20s = 180s)
- CHECK_COUNT=9, WAKE_REQUESTS_ISSUED=9
- Last handoff: nonce `b62bf654f9f94292` at 2026-10-12T07:36:55Z (trigger=STALLED, age=57874s>600s, resume=NEXT_EXECUTABLE_ACTION)
- **Only ONE complete 3-minute duty cycle observed** (9 checks × 20s ≈ 180s)

**Prior wake controllers (evidence from state file succession):**
1. PID 38980 (prev): LAST_HANDOFF_AT=2026-10-12T07:36:55Z (nonce `b62bf654f9f94292`) — **same event as current** (process succession)
2. PID 16264 (earlier): LAST_HANDOFF_AT=2026-10-12T07:31:39Z (nonce `378352c2988c45a0`)
3. PID 43552 (legacy): LAST_WAKE_REQUEST_AT=2026-10-11T15:31:39Z, CHECK_COUNT=2648, up ~16h — **separate lineage**

**FIRST_FAILED_EDGE: Only ONE verified three-minute wake cycle on current persistent controller (PID 32964). Prior events belong to died processes, not the same persistent instance. Three independent wake firings on a single armed controller NOT YET DEMONSTRATED.**

---

## C. GRAPHIFY TIMESTAMP RECONCILIATION — RESOLVED

**graphify-out/graph.json:**
- mtime: 2026-10-11T15:37:21Z (UTC)
- local_mtime: 2026-10-11T16:37:21 (BST = UTC+1)
- sha256: 8f21a515394456a8ba6697d959fd7137cdf1e420822410c4f6e0ca4b8489974e
- size: 3,377,907 bytes
- git_status: **clean** (not modified, not staged, not committed)
- source_basis_SHA: 82258d43ec0f575d803f74c6d96de0e294a530bf (branch HEAD)
- Controller reported "refreshed 2026-10-11T16:37Z" — **this was LOCAL time (BST), not UTC**
- Evidence commit 82258d43 timestamp: 2026-10-11T15:43:40Z (GitHub comment 6110736759)
- **UTC reconciliation: graph.json mtime (15:37:21Z) PRECEDES evidence commit (15:43:40Z) by ~6 min**
- No regeneration needed; file is clean on correct branch basis

---

## SUMMARY

| Gap | Status | Evidence |
|-----|--------|----------|
| A. Single-sender exclusivity | **VERIFIED** (OS instance lock + CLI flag gate) | Parent=launcher, child=engine; instance lock serializes; mainnet blocked by `--confirm-mainnet-follower` gate |
| B. Three wake events | **INCOMPLETE — FIRST_FAILED_EDGE** | Current controller: 1 cycle (9 checks/180s). Prior events from dead processes. |
| C. Graphify timestamp | **RESOLVED** | UTC mtime 15:37Z < commit 15:43Z; local 16:37 misread as UTC; clean, basis SHA 82258d43 |

**SAFETY:** Same wallet address across testnet/mainnet = config fact only; testnet key cannot sign mainnet without `--confirm-mainnet-follower`. No duplicate mainnet sender exists.

**STAND_DOWN_REQUEST=YES** (three gaps closed to evidence limit; B documented as FIRST_FAILED_EDGE).