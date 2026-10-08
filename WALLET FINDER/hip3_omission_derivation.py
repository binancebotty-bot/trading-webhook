"""HIP-3 baseline-omission derivation: pair-level causal proof.

For each candidate pair (builder coin the wallet HOLDS but which is ABSENT from its
pre-fix epoch baseline), this module proves the omission from a provably COMPLETE
post-fence interval:

    omitted_at_fence = exchange - sum(signed fill deltas strictly after old_fence)

where the sum is computed from the engine's fetch_fills_since() which returns
{"ok": bool, "complete": bool, "rows": [...]} and handles the 2000-row cap,
inclusive boundaries, and tied timestamps fail-closed.

The snapshot fence is the actual fence returned by fetch_wallet_positions(), stored
as snapshot_fence_ms in the drift state.

This module is called by the derivation script and is also importable for testing.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

FIX_RESTART_MS = 1791446594000  # 2026-10-08T08:03:14Z
EPS = 1e-9


def is_builder_coin(coin: str) -> bool:
    """A builder coin has format 'DEX:SYMBOL' (e.g. 'XYZ:MRNA')."""
    return ":" in str(coin) and not str(coin).startswith("@")


def fetch_fills_in_interval(
    wallet: str,
    old_fence_ms: int,
    snapshot_fence_ms: int,
    fetch_fn,
) -> Tuple[Optional[float], bool]:
    """Fetch and sum signed fill deltas in (old_fence, snapshot_fence].

    Uses the engine's fetch_fills_since() contract:
      fetch_fn(wallet, start_ms, end_ms) -> {"ok": bool, "complete": bool, "rows": [...]}

    Completeness is proven ONLY when ok == true AND complete == true.
    The 2000-row cap, inclusive boundaries, and tied timestamps are handled
    by the engine's fetch_fills_since() -- this module does NOT re-implement
    pagination or use any time-based heuristic.

    Args:
        wallet: the wallet address
        old_fence_ms: the old epoch baseline fence (exclusive)
        snapshot_fence_ms: the actual snapshot fence (inclusive)
        fetch_fn: callable matching the fetch_fills_since() contract

    Returns:
        (sum_of_deltas, is_complete)
        If the fetch fails or returns incomplete data, returns (None, False).
    """
    if not old_fence_ms or not snapshot_fence_ms or snapshot_fence_ms < old_fence_ms:
        return None, False

    try:
        result = fetch_fn(wallet, old_fence_ms, snapshot_fence_ms)
    except Exception as e:
        log.warning("FETCH_FILLS_FAILED wallet=%s err=%s", wallet, e)
        return None, False

    if result is None:
        return None, False

    # Must use the engine's ok/complete contract
    ok = bool(result.get("ok"))
    complete = bool(result.get("complete"))
    rows = result.get("rows")

    if not ok or not complete or rows is None:
        log.warning(
            "INCOMPLETE_INTERVAL wallet=%s ok=%s complete=%s reason=%s",
            wallet, ok, complete, result.get("reason", "unknown"),
        )
        return None, False

    # Sum signed deltas from the returned rows
    total = 0.0
    for r in rows:
        if not isinstance(r, dict):
            continue
        delta = float(r.get("signed_delta") or r.get("size") or 0)
        total += delta

    return total, True


def derive_omission(
    wallet: str,
    coin: str,
    exchange_size: float,
    baseline_size: float,
    old_fence_ms: int,
    snapshot_fence_ms: int,
    epoch_pre_fix: bool,
    fetch_fn,
) -> Tuple[bool, Dict[str, Any]]:
    """Derive whether a pair is a proven baseline omission.

    Returns:
        (is_proven, evidence_dict)
    """
    evidence = {
        "exchange_at_snapshot": exchange_size,
        "baseline": baseline_size,
        "old_fence_ms": old_fence_ms,
        "snapshot_fence_ms": snapshot_fence_ms,
        "epoch_pre_fix": epoch_pre_fix,
    }

    # Must be pre-fix epoch
    if not epoch_pre_fix:
        evidence["proven"] = False
        evidence["reason"] = "epoch_not_pre_fix"
        return False, evidence

    # Must be absent from baseline
    if abs(baseline_size) > EPS:
        evidence["proven"] = False
        evidence["reason"] = "baseline_nonzero"
        return False, evidence

    # Must be held on exchange
    if abs(exchange_size) <= EPS:
        evidence["proven"] = False
        evidence["reason"] = "not_held_on_exchange"
        return False, evidence

    # Must have a valid snapshot fence
    if not snapshot_fence_ms or snapshot_fence_ms < old_fence_ms:
        evidence["proven"] = False
        evidence["reason"] = "invalid_snapshot_fence"
        return False, evidence

    # Fetch and prove the post-fence interval is complete using the engine's
    # fetch_fills_since() ok/complete/rows contract
    post_fence_delta, is_complete = fetch_fills_in_interval(
        wallet, old_fence_ms, snapshot_fence_ms, fetch_fn,
    )

    if not is_complete or post_fence_delta is None:
        evidence["proven"] = False
        evidence["reason"] = "interval_incomplete"
        return False, evidence

    omitted = exchange_size - post_fence_delta
    evidence["post_fence_delta_sum"] = post_fence_delta
    evidence["omitted_position_at_old_fence"] = omitted
    evidence["interval_complete"] = True

    if abs(omitted) <= EPS:
        evidence["proven"] = False
        evidence["reason"] = "omitted_position_zero"
        return False, evidence

    evidence["proven"] = True
    return True, evidence


def derive_cohort_from_truth(
    truth: Dict[str, Any],
    fetch_fn,
) -> Dict[str, Any]:
    """Derive the full cohort from engine_truth.json data.

    Args:
        truth: the parsed engine_truth.json dict
        fetch_fn: callable matching the fetch_fills_since() contract

    Returns:
        dict with 'targets', 'wallets', 'proven_wallet_count', 'proven_pair_count'
    """
    ds = truth.get("drift_state") or {}
    ep = truth.get("proof_epochs") or {}

    wallets: Dict[str, Any] = {}
    for w, m in ds.items():
        if not isinstance(m, dict):
            continue
        e = ep.get(w) or {}
        fence = int(e.get("baseline_ts_ms") or 0)
        eid = e.get("epoch_id")
        pre_fix = bool(fence) and fence < FIX_RESTART_MS

        coins: Dict[str, Any] = {}
        for c, s in m.items():
            if not isinstance(s, dict) or not is_builder_coin(c):
                continue
            baseline = float(s.get("baseline") or 0)
            exchange = float(s.get("exchange") or 0)
            snap_fence = int(s.get("snapshot_fence_ms") or 0)

            if abs(baseline) > EPS or abs(exchange) <= EPS:
                continue

            proven, evidence = derive_omission(
                w, c, exchange, baseline, fence, snap_fence, pre_fix, fetch_fn,
            )
            coins[str(c).upper()] = evidence

        if coins:
            proven_coins = sorted(c for c, d in coins.items() if d.get("proven"))
            wallets[w] = {
                "epoch_id": eid,
                "epoch_baseline_ts_ms": fence,
                "epoch_pre_fix": pre_fix,
                "builder_coins": coins,
                "proven_coins": proven_coins,
                "proven": bool(pre_fix and any(d.get("proven") for d in coins.values())),
            }

    targets: Dict[str, Any] = {}
    total_pairs = 0
    for w, v in wallets.items():
        if v["proven"]:
            targets[w] = {
                "epoch_id": v["epoch_id"],
                "proven_coins": {
                    c: round(v["builder_coins"][c]["omitted_position_at_old_fence"], 8)
                    for c in v["proven_coins"]
                },
            }
            total_pairs += len(v["proven_coins"])

    return {
        "schema": "hip3_baseline_omission_derivation.v5",
        "fix_restart_ms": FIX_RESTART_MS,
        "criteria": "pre-fix epoch AND baseline==0 AND exchange!=0 AND fetch_fills_since(old_fence, snapshot_fence) ok==true AND complete==true AND (exchange - post_fence_delta) != 0",
        "proven_wallet_count": len(targets),
        "proven_pair_count": total_pairs,
        "targets": targets,
        "wallets": wallets,
    }
