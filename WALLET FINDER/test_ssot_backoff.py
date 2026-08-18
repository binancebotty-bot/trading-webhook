"""The tracker must not re-ask the exchange a question it already answered.

This engine spent three days in a permanent drift-recovery loop.  Its own audit
counters recorded the shape of it: 20,800 recovery attempts, 220 recoveries,
and 11,020,509 duplicate fill rows fetched and discarded.  Each of those fetches
was a 20-weight userFillsByTime against a 1200 weight/min allowance shared with
a live execution path in another process.

The cause was not a bad request.  It was an unfalsifiable one: a drift the fill
history cannot explain -- a pre-baseline transfer, a manual move, a liquidation,
dust below the exchange's reporting threshold -- stays unexplained however many
times the same window is re-fetched.  The engine treated "still unexplained" as
a reason to try again immediately, forever.

These tests pin the fix: unexplained drift is still reported, but it stops being
re-fetched until something actually changes.

Run:  python -m pytest test_ssot_backoff.py -q
"""

from __future__ import annotations

import time
import types

import pytest

import HL_Copy_Engine_SSOT as ssot
import hl_rate_guard


WALLET = "0x1111111111111111111111111111111111111111"
COIN = "PUMP"


class _Recorder:
    """A stand-in engine carrying only what the drift audit actually touches."""

    def __init__(self):
        self.ingest_calls = []
        self.audit = __import__("collections").Counter()
        self.drift_state = {}
        self.drift_recovery_gate_by_wallet = {}
        self.last_ledger_ts_by_wallet = {WALLET: 1_000}
        self.exchange_baseline_by_wallet = {WALLET: {COIN: {"signed_size": 0.0, "baseline_ts_ms": 500}}}
        self.positions = {}
        self.wallet_ready = True

    # --- the surface audit_position_drift_only calls into ---
    def _is_wallet_ready(self, wallet):
        return self.wallet_ready

    def baseline_size(self, wallet, coin):
        return 0.0

    def _set_drift_state(self, wallet, coin, status, detail):
        self.drift_state[(wallet.lower(), coin.upper())] = {"status": status, **detail}

    def _wallet_state(self, wallet):
        return {}

    def ingest_real_fills_window(self, wallet, start_ms, end_ms, reason, *, advance_cursor=False):
        self.ingest_calls.append((reason, start_ms, end_ms))
        # Barren: the exchange has nothing new to say about this drift.
        return {"ok": True, "parsed": 0, "applied": 0, "deduped": 7, "max_ts": start_ms}

    def rebuild_from_ledger(self):  # pragma: no cover - only on a productive ingest
        raise AssertionError("a barren recovery must not trigger a ledger rebuild")


def _engine():
    e = _Recorder()
    # Borrow the real method under test rather than reimplementing it.
    e.audit_position_drift_only = types.MethodType(
        ssot.EngineSSOT.audit_position_drift_only, e
    )
    return e


def _drifting_snapshot():
    # Exchange says we hold PUMP; the ledger says we do not.  No fill explains it.
    return {COIN: {"signed_size": -0.437, "entry_price": 0.0, "unrealized_pnl": 0.0}}


def test_the_first_unexplained_drift_is_investigated():
    e = _engine()
    e.audit_position_drift_only(WALLET, _drifting_snapshot())

    assert [c[0] for c in e.ingest_calls] == ["drift_recovery"]
    assert e.drift_state[(WALLET, COIN)]["status"] == "DRIFT_UNRESOLVED"


def test_the_same_unexplained_drift_is_not_investigated_twice():
    """The whole incident in one assertion."""
    e = _engine()
    for _ in range(20):
        e.audit_position_drift_only(WALLET, _drifting_snapshot())

    assert len(e.ingest_calls) == 1, (
        f"{len(e.ingest_calls)} exchange fetches for one unchanged, unexplainable "
        "drift -- this is the loop that consumed the IP budget"
    )
    assert e.audit["drift_recovery_quiesced"] == 19


def test_a_quiesced_drift_is_still_reported_truthfully():
    """Backing off is not the same as hiding it."""
    e = _engine()
    e.audit_position_drift_only(WALLET, _drifting_snapshot())
    e.audit_position_drift_only(WALLET, _drifting_snapshot())

    state = e.drift_state[(WALLET, COIN)]
    assert state["status"] == "DRIFT_UNRESOLVED", "the drift must not be marked clean"
    assert state["recovery"] == "QUIESCED"
    assert state["recovery_quiesced_reason"] == "no_new_fills_since_last_barren_recovery"
    assert state["recovery_retry_after_sec"] == ssot.DRIFT_RECOVERY_RETRY_SEC
    assert abs(state["delta"]) > 0, "the actual delta must still be visible"


def test_new_fills_reopen_the_investigation_immediately():
    """Backoff is about absence of evidence, not about elapsed time."""
    e = _engine()
    e.audit_position_drift_only(WALLET, _drifting_snapshot())
    e.audit_position_drift_only(WALLET, _drifting_snapshot())
    assert len(e.ingest_calls) == 1

    # A normal poll ingests a real fill: the ledger moves.
    e.last_ledger_ts_by_wallet[WALLET] = 2_000
    e.audit_position_drift_only(WALLET, _drifting_snapshot())

    assert len(e.ingest_calls) == 2, (
        "new fill evidence must reopen the investigation without waiting"
    )


def test_the_retry_interval_eventually_reopens_it():
    e = _engine()
    e.audit_position_drift_only(WALLET, _drifting_snapshot())
    e.audit_position_drift_only(WALLET, _drifting_snapshot())
    assert len(e.ingest_calls) == 1

    e.drift_recovery_gate_by_wallet[WALLET]["at"] = (
        time.time() - ssot.DRIFT_RECOVERY_RETRY_SEC - 1
    )
    e.audit_position_drift_only(WALLET, _drifting_snapshot())

    assert len(e.ingest_calls) == 2


def test_a_clean_wallet_is_never_investigated_at_all():
    e = _engine()
    e.audit_position_drift_only(WALLET, {})
    assert e.ingest_calls == []


# ------------------------------------------------------------------ pacing

def test_the_process_ceiling_binds_without_any_coordination():
    """The fixed slice is the property that holds when everything else fails."""
    hl_rate_guard.reset_for_tests("t")
    g = hl_rate_guard.guard("t", 100.0)
    granted = sum(1 for _ in range(20) if g.acquire("userFillsByTime"))
    assert granted == 5, "100 weight must buy exactly five 20-weight requests"


def test_a_refused_read_is_never_reported_as_an_empty_window():
    """The dangerous confusion: 'I did not look' vs 'there was nothing there'.

    fetch_fills_range returns [] for a proven-empty window and None for a
    failure, and only None stops the caller advancing its cursor.  A budget
    refusal must take the None path, or a skipped fetch silently becomes a
    permanent hole in the fill ledger.
    """
    hl_rate_guard.reset_for_tests("ssot")
    ssot.RATE_GUARD = hl_rate_guard.guard("ssot", 0.0)  # nothing may pass
    try:
        e = types.SimpleNamespace(audit=__import__("collections").Counter())
        result = ssot.EngineSSOT.fetch_fills_range(e, WALLET, 0, 1)
        assert result is None, "a refused fetch must not look like an empty window"
        assert e.audit["rate_budget_skipped_fill_fetches"] == 1
    finally:
        hl_rate_guard.reset_for_tests("ssot")
        ssot.RATE_GUARD = hl_rate_guard.guard("ssot", ssot.SSOT_WEIGHT_PER_MIN)


def test_the_tracker_polls_at_a_bounded_low_cadence():
    assert ssot.POLL_SECONDS >= 120.0
    assert ssot.DRIFT_RECOVERY_RETRY_SEC >= 300.0
    assert ssot.SSOT_WEIGHT_PER_MIN <= 200.0


def test_the_sweep_rotates_so_no_wallet_is_permanently_unseen():
    """Under a ceiling a sweep may not finish; it must not always give up at the same place."""
    source = open(ssot.__file__, encoding="utf-8").read()
    assert "_poll_rotation_offset" in source
    assert "order = order[offset:] + order[:offset]" in source
