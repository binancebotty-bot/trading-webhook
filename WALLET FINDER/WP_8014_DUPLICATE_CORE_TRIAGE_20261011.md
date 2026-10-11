# WP-8014 DUPLICATE-CORE READ-ONLY SAFETY TRIAGE — REDACTED EVIDENCE

ACTION_ID=WP-8014-DUPLICATE-CORE-TRIAGE-1
PARENT_DIRECTIVE=6110641191  SUCCESSOR_ALIGN=6110698563
DELIVERY_NONCE=563b4cb43d0c433f
REVIEWED_REMOTE_BRANCH=wp-hip3-builder-dex  REVIEWED_REMOTE_SHA=de38010e68a183d17d4ffdca263e89d221ff6ce2
MODE=READ-ONLY. No process started/stopped, no config/data changed, no exchange API, no secrets.

## 1) DUPLICATE CORE — PIDs 13372 / 46304
- ONE LOGICAL ENGINE, not duplicates. 46304 = PARENT, 13372 = its CHILD.
  core_start_record.json: pid=13372, parent_pid=46304, both started 2026-10-11T15:19:13Z,
  same cwd, same argv.
- argv: `HL_Live_Copy_Service_Core.py --ws --loop --interval 10 --poll-live --poll-copy`
- script realpath: C:/Users/wigmore/trading_stack/hl-build3-testnet-cert/_archive/hl_stage2/HL_Live_Copy_Service_Core.py
- script sha256: 62a621b9f2e9f2e71a49d057b00be4cce87ea0b8d3bc017c1761c1ca23b837ec
- cwd: C:/Users/wigmore/trading_stack/hl-build3-testnet-cert/_archive/hl_stage2  (ARCHIVE)
- RUN FLAGS (from _archive/run_env.sh, redacted): HL_LEADER_NETWORK=mainnet (read-only),
  HL_FOLLOWER_NETWORK=testnet, HL_LIVE_ENV_FILE=/nonexistent/no_env_file.env, HL_APP_PORT=8031.
- ACCOUNT (fingerprint only, never key): master 0x…5e9205, sha256(lower)[:12]=dd1282723681.
  Key is read from .env.testnet at launch; HL_LIVE_HL_PRIVATE_KEY is the TESTNET agent key
  (signing-only). No secret printed.
- SEND CAPABILITY: live_config.json -> auto_send_enabled=true => _send_mode_of()=ON
  (global_controls.send_mode absent) => master_real_orders_enabled=True. 15 wallets: 8 mode=LIVE, 7 OFF.
- ACTUAL SENDS (bounded local append-only evidence, NOT an exchange call):
  send_attempts.csv shows REAL_IOC attempts with exchange status ok and order ids
  oid=62416521136 (WLD, 2026-10-11T11:04Z), oid=62417808090 + 62417812186 (PONS, 2026-10-11T11:27Z);
  live_fills.csv matches by exchange_order_id. So this TESTNET engine has genuinely placed TESTNET orders.
- NET: one engine (parent->child wrapper), TESTNET, real order capability, real testnet sends seen,
  last 2026-10-11T11:27Z. NOT the production live-copy runtime. Nothing touched.

## 2) SEPARATE AUTHORISED LIVE-COPY RUNTIME — checked independently
- production root: C:/Users/Public/HyperliquidProduction/runtime/active
- product_config.safe.json: global_state=OFF, dry_run=False, config_generation=103,
  follower_account_id=endurance, follower_address 0x…5e9205 (sha256[:12]=dd1282723681),
  max_order_notional_usd=10000, leaders=12.
- operator_lifecycle.lock holder pid=37432 (at 2026-10-09T14:30:59Z): NOT running.
- No live_copy_operator / live_copy_supervisor / product-runtime process alive (psutil scan).
- NET: production live-copy runtime is NOT running; global_state OFF.

## 3) ACCOUNT OVERLAP — CRITICAL
- SAME follower address in BOTH: testnet engine account AND mainnet product_config follower_address
  (0x…5e9205). Same literal address => same account under the Hybrid API, on BOTH networks.
- Consequence: the two addresses are the SAME account; "duplicate same-account send" across networks
  is therefore real in the sense that both target one account. Since the production runtime is OFF
  and the archived engine is TESTNET, there is no concurrent same-network send today.
- The testnet engine carries the TESTNET agent key (signing-only) and is testnet-scoped, so it cannot
  place mainnet orders.
- MARKED: CRITICAL_BLOCKER = SAME_ACCOUNT_ACROSS_NETWORKS (production OFF). Awaiting controller/operator
  ruling. No intervention taken.

## 4) FIVE-FILE SSOT INVENTORY (UTC mtime / size / sha256) @ effective output path
C:/Users/wigmore/trading_stack/Hyperliquid scanner/WALLET FINDER/
- hl_copy_output/engine_truth.json      2026-10-10T21:59:57Z  707703  a9e4ed82be6c1310c92642486dca0b4033c7de564e6dcd6289f1f4e6fdbfb98e
- hl_copy_output/live_state.json        2026-10-10T21:59:57Z  707703  a9e4ed82be6c1310c92642486dca0b4033c7de564e6dcd6289f1f4e6fdbfb98e (identical mirror)
- hl_copy_output/raw_live_fills.csv     2026-10-10T16:55:58Z 406974158 c972b3303d72d5d8c8942c4d039c65799dfb5f0fe2c2cc34d0c3eff56140405d
- hl_copy_output/live_wallet_metrics.csv 2026-10-10T21:59:57Z    22010 7cfd213703c0b1413bdf62bfe8e90db3087ba04e12acbe0c28b6844f8fe3f2ab
- hl_copy_output/exchange_baselines.json 2026-10-10T22:00:44Z   136028 8d30bbe3c683fc7071ec66fbb186fccc3210b240d9cb6c26c3524d5a95507a3b
- hl_copy_output/proof_epoch.json        2026-10-10T08:47:36Z   178010 e858d43de7800ec5358a696762eca3a5614519ab94c465da7b676093492a56e1
- hl_copy_output/proof_watermark.json    2026-10-10T14:55:24Z    12132 2f192fce1ee69ac1f6358a53a99441a06ce03509b592c53dd4b836362076e555
- hl_copy_output/recovery_gate.json      2026-10-10T14:55:26Z    20414 12c9574ce72cf02271d56a5bf715f188759386eb9ad1672b06ea7d5ec98e05f7
- wallet_gate.json ABSENT at repo root (Engine SSOT path BASE_DIR/wallet_gate.json).
- Writer HL_Copy_Engine_SSOT.py NOT running. All evidence stale.

## 5) THREE-MINUTE WAKE — VERIFIED
- b4_wake_arm.py --status: WAKE_CONTROLLERS=1, pid=43552, STATUS=ALIVE, STAND_DOWN_AUTHORISED=NO.
- build4_wake_controller.json: WAKE_CONTROLLER_STATE=ARMED, PID=43552,
  PROCESS_START_IDENTITY=43552:1791674629.748795, CHECK_COUNT=2648,
  WAKE_CONTROLLER_LAST_HEARTBEAT=2026-10-11T15:41:55Z, LAST_WAKE_REQUEST_AT=2026-10-11T15:31:39Z,
  MISSION_STATE=ACTIVE. CHECK_SECONDS=20, DUTY cycle ~180s.
- Independent ticks (WAKE_CONTROLLER_LAST_HEARTBEAT advancing, ~180s duty print):
  t1 2026-10-11T15:31:39Z (LAST_WAKE_REQUEST_AT), t2 2026-10-11T15:41:55Z (heartbeat).
- GitHub watcher pid 47168 alive (20s poll).

## 6) GRAPHIFY
- graphify update . run at source C:/Users/wigmore/trading_stack/Hyperliquid scanner/WALLET FINDER
  (Graphify root = Hyperliquid scanner, per .graphify_root).
- graphify-out/graph.json refreshed 2026-10-11T16:37Z; branch HEAD de38010e (unchanged basis).

## SUMMARY
- Duplicate Core = ONE testnet engine (parent+child); real testnet sends seen, testnet-scoped.
- Production live-copy runtime NOT running; global_state OFF.
- CRITICAL_BLOCKER: same follower account address on BOTH networks (production currently OFF).
- SSOT writer DOWN; five-file evidence stale; wake verified alive.
- No code/data/process changes. STAND_DOWN_REQUEST=YES (see wake).
