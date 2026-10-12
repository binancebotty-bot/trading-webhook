# SSOT Writer Recovery Evidence — 2026-10-12

## Summary

SSOT writer successfully restarted and running. Fresh evidence being produced. proof_epoch.json still bootstrapping (expected — updates only after all wallets complete epoch promotion).

## Safeguard Verification (Pre-Start)

| Check | Result |
|-------|--------|
| Single-instance lock | PASS — stale lock (PID 42296) detected and cleaned |
| Duplicate processes | PASS — no duplicate HL_Copy_Engine_SSOT.py running |
| API-rate safeguards | PASS — rate budget deferrals active in log |
| Trading disabled | PASS — writer is read-only data harvester |

## Writer Status

| Field | Value |
|-------|-------|
| PID | 39504 |
| Status | running |
| Uptime | 1942 seconds (~32 minutes) |
| Start time | 2026-10-12T13:49:00Z |

## Fresh Evidence Files

| File | Size | Last Modified (UTC) | Age (seconds) | Status |
|------|------|---------------------|---------------|--------|
| engine_truth.json | 780,749 bytes | 2026-10-12T14:21:29Z | 2 | FRESH |
| proof_watermark.json | 12,522 bytes | 2026-10-12T14:21:13Z | 18 | FRESH |
| recovery_gate.json | 20,979 bytes | 2026-10-12T14:21:11Z | 20 | FRESH |
| raw_live_fills.csv | 409,621,648 bytes | 2026-10-12T14:21:10Z | 20 | FRESH |
| proof_epoch.json | 178,010 bytes | 2026-10-10T08:47:36Z | 192,835 | STALE (bootstrapping) |

## Bootstrap Progress

- Wallets tracked: 58
- Builder snapshots: 36/58 complete
- WS shards: 12/12 active
- Rate budget deferrals: active (normal)

## Epoch Status (from proof_epoch.json — stale)

| Status | Count |
|--------|-------|
| OPEN | 196 |
| PENDING | 7 |
| UNRESOLVED | 0 |

## Recovery Gate Status (fresh)

| Status | Count |
|--------|-------|
| Complete | 19 |
| Incomplete | 53 |
| Total | 72 |

## Key Findings

1. **Writer IS running** — PID 39504, uptime 1942s, producing fresh evidence
2. **Fresh data IS being produced** — engine_truth, proof_watermark, recovery_gate, raw_live_fills all updated within last 20 seconds
3. **proof_epoch.json is stale** — expected behavior; updates only after all 58 wallets complete epoch promotion
4. **7 PENDING epochs remain** — these are the 7 wallets the dashboard shows as unresolved; they require epoch promotion to complete
5. **Writer is making progress** — 36/58 wallets bootstrapped, 12 WS shards active, fills being ingested
6. **Rate budget constraints** — writer is respecting API rate limits, causing slower bootstrap but preventing lockout

## Next Steps

1. Continue monitoring writer until all 58 wallets complete bootstrap
2. proof_epoch.json will refresh automatically after all wallets complete epoch promotion
3. Dashboard banner should clear once proof_epoch.json shows 0 PENDING wallets
4. No manual intervention required — writer is self-sustaining

## Nonce

NONCE=SSOT-RECOVERY-EVIDENCE-001
