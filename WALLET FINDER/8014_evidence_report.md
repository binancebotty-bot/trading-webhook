# 8014 Process Identity & Proof Status Evidence

**Date**: 2026-10-11
**Commit**: b35f5c957442143e4fc88f6038fba90c7ac49fcb (wp-hip3-builder-dex)

## Process Serving Port 8014
- **PID**: 34100
- **Running since**: 2026-10-07T19:52:53Z (>4 days uptime)
- **Command**: `uvicorn wallet_proof_engine_8014:app --host 127.0.0.1 --port 8014 --log-level warning`
- **Working directory**: `C:/Users/wigmore/trading_stack/Hyperliquid scanner/WALLET FINDER`
- **Git branch**: wp-hip3-builder-dex
- **Git commit**: b35f5c957442143e4fc88f6038fba90c7ac49fcb

## Source Match to GitHub
- Local `wallet_proof_engine_8014.py` SHA256: `2d1d1209c2899575c1d40dbf259e057b13d4e4d3b4940dc122e1e3f1265339d0`
- Git blob SHA1: `336b440d76d316bbcb3d97071abe2c9252f47eb8`
- **CONFIRMED**: Matches `origin/wp-hip3-builder-dex` exactly (controller noted this SHA is identical on both `wp-8014-ui-repair` and `wp-hip3-builder-dex`)

## SSOT Data Being Read
- Primary: `hl_copy_output/engine_truth.json` (confirmed via `data-proof-source="engine_truth.json"` in proof-status banner)
- File updated: `2026-10-10T21:59:56.366279+00:00` (matches `data-truth-updated` in banner)
- Contains 203 wallets, proof_epochs for tracked wallets, currentness metrics

## Current Proof Status (from live 8014 page)
- **Status**: **CONVERGING** (yellow border `#d29922`) — NOT RED (`proof-unresolved` = red `#ff4d4f`)
- `inputs_current: false`
- `state_trustworthy: false` (implied)
- `OPEN wallets: 196`
- `PENDING wallets: 7`
- `wallets without proven epoch: 7`
- `unresolved pairs: 0`
- `escalated pairs: 0`
- `global trusted-through: UNKNOWN` (`global_trusted_through_ms: 0`)
- `trusted-through lag: UNKNOWN`

## Root Cause from engine_truth.json currentness
- `global_trusted_through_ms: 0` — no global trusted-through established
- `global_trusted_through_iso: ""` — empty
- `bootstrap_complete: false`
- `audited_pairs: 0`
- `inputs_current: false`
- **7 wallets have `per_wallet_trusted_through_ms: 0`** (e.g., 0x061c9d98dca634ec51a38a79c2c9c734f8a5bf79, 0x1003d8c532e5178673668e5f3aa48dc399ee940c, 0x1082b74e86e666ff9c163c6a976f4be5cb98de4b)

## Conclusion
The running 8014 process is correctly identified and matches GitHub. The proof status is CONVERGING (yellow), not RED. The blocker to TRUSTED (green) is: **no global trusted-through has been established** (`global_trusted_through_ms = 0`), bootstrap is incomplete, and 7 wallets have zero trusted-through timestamps. The `audited_pairs: 0` confirms no audit cycle has completed to establish the trusted boundary.

The process has been running >4 days without a model refresh that would establish the trusted-through boundary.