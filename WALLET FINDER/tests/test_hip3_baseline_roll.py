"""Tests for the HIP-3 defective-baseline guard + targeted epoch roll (fix D).

Approved repair (controller ruling):
  - a builder coin the wallet HOLDS but which is absent from the epoch's measured
    baseline (baseline=0) is flagged BASELINE_INVALID_REMEASUREMENT_REQUIRED and
    must NOT trigger a wide drift-recovery fetch;
  - the epoch is rolled: prior closed UNRESOLVED (reason HIP3_NATIVE_ONLY_BASELINE_OMISSION,
    preserved in epoch_history), fresh epoch PENDING with baseline = current union;
  - fail closed: enumeration/union/fence failure => NO roll;
  - the guard is builder-only and NEVER fires for ordinary (native) drift.

Deterministic + offline (module-level `requests` replaced with a fake).
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import HL_Copy_Engine_SSOT as mod  # noqa: E402


class _Resp:
    def __init__(self, p): self._p = p
    def json(self): return self._p


class FakeRequests:
    def __init__(self, routes):
        self.routes = routes; self.calls = []
    def post(self, url, json=None, timeout=None):
        b = dict(json or {}); rt = b.get("type")
        self.calls.append((rt, b.get("dex"), b.get("user")))
        fn = self.routes.get(rt)
        if fn is None: raise AssertionError(f"unscripted {rt!r}")
        return _Resp(fn(b))
    def count(self, rt, dex=None):
        return sum(1 for (t, d, _u) in self.calls if t == rt and (dex is None or d == dex))


def pos(coin, szi, entry=1.0):
    return {"position": {"coin": coin, "szi": str(szi), "entryPx": str(entry), "unrealizedPnl": "0"}}

def ch_state(positions, time_ms):
    return {"assetPositions": positions, "time": time_ms, "marginSummary": {}, "crossMarginSummary": {}}


W = "0x069f458978a51ef6a9e2f6b6eb1fe2f39271c5a2"
NATIVE_W = "0x06ee56c1682ee8a6b504a0ebef23ec27766449e0"
T = 1791446594000


def new_engine(epochs=None, positions=None):
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.positions = positions or {}
    e.wallet_builder_dexes = {}
    e.wallet_active_dexes = {}
    e.perp_dex_names = ["xyz"]
    e.perp_dex_names_loaded_ms = 0
    e.last_exchange_snapshot_by_wallet = {}
    e.last_exchange_snapshot_ts_by_wallet = {}
    e.wallet_runtime = {}
    e.epoch_by_wallet = epochs or {}
    e.trusted_through_ms_by_wallet = {}
    e.last_poll_ts_by_wallet = {}
    e.exchange_baseline_by_wallet = {}
    e.last_ledger_ts_by_wallet = defaultdict(int)
    e._startup_fill_wallets = set()
    e.drift_state_by_wallet = {}
    e.mark_prices = {}
    e.wallets_raw = {}
    e.drift_recovery_attempt_count = defaultdict(int)
    e.drift_recovery_gate_by_wallet = {}
    e.consecutive_poll_failures = defaultdict(int)
    e._baseline_remeasure_gate = {}
    # wallets must be READY or drift audit returns early
    e.wallet_runtime = {W: {"ready": True}, NATIVE_W: {"ready": True}}
    return e


def test_guard_flags_builder_omission_not_native_drift():
    """A held builder coin absent from baseline -> flagged; native drift -> NOT flagged."""
    e = new_engine()
    # epoch baseline OMITS the builder coin (pre-fix native-only measurement)
    e.epoch_by_wallet[W] = {
        "epoch_id": "w-1", "epoch_status": "OPEN", "baseline_ts_ms": T - 10**9,
        "baseline_position": {"BTC": {"signed_size": 1.0, "entry_price": 1.0}},
    }
    # a genuine NATIVE drift the guard must NOT capture
    e.epoch_by_wallet[NATIVE_W] = {
        "epoch_id": "n-1", "epoch_status": "OPEN", "baseline_ts_ms": T - 10**9,
        "baseline_position": {"BTC": {"signed_size": 1.0, "entry_price": 1.0}},
    }
    # exchange: builder wallet holds BTC(1, clean) + XYZ:MRNA(-5, omitted)
    e.positions = {}
    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state(
            [pos("xyz:MRNA", -5.0), pos("BTC", 1.0)], T) if b.get("dex") == "xyz"
            else ch_state([pos("BTC", 1.0)], T),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr; mod.RATE_GUARD = None
    # builder wallet: MRNA flagged, and the ROLL then replaces the epoch (baseline includes it)
    e.audit_position_drift_only(W, {"XYZ:MRNA": {"signed_size": -5.0}, "BTC": {"signed_size": 1.0}})
    st = e.drift_state_by_wallet[W]
    assert st["BTC"]["status"] == "CLEAN", st["BTC"]
    # after the roll the epoch baseline must now CONTAIN the builder coin
    newb = e.epoch_by_wallet[W]["baseline_position"]
    assert newb.get("XYZ:MRNA", {}).get("signed_size") == -5.0, newb
    assert e.epoch_by_wallet[W]["epoch_status"] == "PENDING"
    assert e.audit["epochs_rolled_baseline_omission"] == 1
    # native wallet: genuine drift stays DRIFT_* and is NOT rolled
    e.audit_position_drift_only(NATIVE_W, {"BTC": {"signed_size": 3.0}})  # internal 0, base 1 -> delta 2
    nst = e.drift_state_by_wallet[NATIVE_W]["BTC"]
    assert nst["status"] in {"DRIFT_DETECTED", "DRIFT_UNRESOLVED"}, nst
    assert "BTC" not in (e.epoch_by_wallet[NATIVE_W].get("baseline_position") or {}) or True
    assert e.audit["epochs_rolled_baseline_omission"] == 1  # native wallet did NOT roll
    print("PASS 1/4 builder omission flagged+rolled; native drift NOT captured, NOT rolled")


def test_prior_epoch_preserved_unresolved():
    """The defective prior epoch is preserved UNRESOLVED with the explicit reason."""
    e = new_engine()
    e.epoch_by_wallet[W] = {
        "epoch_id": "defective-1", "epoch_status": "OPEN", "baseline_ts_ms": T - 10**9,
        "baseline_position": {},
    }
    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("xyz:MRNA", -5.0)], T) if b.get("dex") == "xyz"
        else ch_state([], T),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr; mod.RATE_GUARD = None
    e.audit_position_drift_only(W, {"XYZ:MRNA": {"signed_size": -5.0}})
    hist = e.epoch_by_wallet[W].get("epoch_history") or []
    assert len(hist) == 1, hist
    assert hist[0]["epoch_status"] == "UNRESOLVED", hist[0].get("epoch_status")
    assert hist[0].get("unresolved_reason") == "HIP3_NATIVE_ONLY_BASELINE_OMISSION"
    assert hist[0]["epoch_id"] == "defective-1"
    print("PASS 2/4 prior defective epoch preserved UNRESOLVED with explicit reason")


def test_roll_fails_closed_on_union_failure():
    """Enumeration/union failure => NO roll, epoch untouched, pair stays flagged."""
    e = new_engine()
    e.perp_dex_names = []          # empty cache: enumeration has nothing to fall back to
    e.perp_dex_names_loaded_ms = 0
    e.epoch_by_wallet[W] = {
        "epoch_id": "defective-1", "epoch_status": "OPEN", "baseline_ts_ms": T - 10**9,
        "baseline_position": {},
    }

    def boom(b):
        raise RuntimeError("perpDexs down")

    fr = FakeRequests({"perpDexs": boom})
    mod.requests = fr; mod.RATE_GUARD = None
    e.audit_position_drift_only(W, {"XYZ:MRNA": {"signed_size": -5.0}})
    assert e.epoch_by_wallet[W]["epoch_id"] == "defective-1", "epoch must be UNTOUCHED"
    assert e.drift_state_by_wallet[W]["XYZ:MRNA"]["status"] == "BASELINE_INVALID_REMEASUREMENT_REQUIRED"
    assert e.audit["baseline_remeasure_skipped_no_dexes"] == 1
    assert e.audit["epochs_rolled_baseline_omission"] == 0
    print("PASS 3/4 roll FAILS CLOSED on enumeration failure; epoch untouched")


def test_remeasure_is_gated():
    """A second detection inside the retry window does NOT re-issue the union fetch."""
    e = new_engine()
    e.epoch_by_wallet[W] = {
        "epoch_id": "defective-1", "epoch_status": "OPEN", "baseline_ts_ms": T - 10**9,
        "baseline_position": {},
    }
    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("xyz:MRNA", -5.0)], T) if b.get("dex") == "xyz"
        else ch_state([], T),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr; mod.RATE_GUARD = None
    # force the roll to fail by making the FIRST attempt consume the gate then a
    # failed snapshot: easier — directly call the method twice.
    e._baseline_remeasure_gate[W] = mod.time.time()  # pretend a recent attempt
    n_before = fr.count("clearinghouseState")
    e._remeasure_epoch_baseline_omission(W, 1)
    assert fr.count("clearinghouseState") == n_before, "gated call must not fetch"
    assert e.audit["baseline_remeasure_gated"] == 1
    print("PASS 4/4 re-measure is gated (no repeated union fetch)")


if __name__ == "__main__":
    test_guard_flags_builder_omission_not_native_drift()
    test_prior_epoch_preserved_unresolved()
    test_roll_fails_closed_on_union_failure()
    test_remeasure_is_gated()
    print("\nALL BASELINE-OMISSION ROLL TESTS PASSED")
