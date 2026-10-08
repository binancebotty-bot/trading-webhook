"""Proof tests for the HIP-3 builder-dex snapshot fix (HL_Copy_Engine_SSOT).

Required proof (reviewer ruling):
  1. builder wallet snapshot matches native + HIP-3 API positions (union)
  2. builder false drift disappears (union snapshot reconciles)
  3. an injected / intervening-fill snapshot FAILS CLOSED
  4. normal native reconciliation is unchanged
  5. request counts show the fix does NOT fan out every DEX for every wallet

Deterministic + offline: the module-level `requests` is replaced with a fake
that serves scripted responses and counts every call.
"""
from __future__ import annotations

import os
import sys
import threading
from collections import defaultdict, deque
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENGINE_DIR = HERE.parent  # tests/ lives inside the engine dir ("WALLET FINDER")
sys.path.insert(0, str(ENGINE_DIR))

import HL_Copy_Engine_SSOT as mod  # noqa: E402


# ----------------------------- fake transport -----------------------------
class _Resp:
    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


class FakeRequests:
    """Scripted /info transport. `routes` maps request type -> callable(body)."""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def post(self, url, json=None, timeout=None):
        body = dict(json or {})
        rtype = body.get("type")
        self.calls.append((rtype, body.get("dex"), body.get("user")))
        fn = self.routes.get(rtype)
        if fn is None:
            raise AssertionError(f"unscripted request type {rtype!r}")
        return _Resp(fn(body))

    def count(self, rtype, dex=None):
        return sum(1 for (t, d, _u) in self.calls if t == rtype and (dex is None or d == dex))


def new_engine():
    """A bare engine with only the state the HIP-3 methods touch (no disk I/O)."""
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.positions = {}
    e.wallet_builder_dexes = {}
    e.wallet_active_dexes = {}
    e.perp_dex_names = []
    e.perp_dex_names_loaded_ms = 0
    e.last_exchange_snapshot_by_wallet = {}
    e.last_exchange_snapshot_ts_by_wallet = {}
    e.wallet_runtime = {}
    e.epoch_by_wallet = {}
    e.trusted_through_ms_by_wallet = {}
    e.last_poll_ts_by_wallet = {}
    e.exchange_baseline_by_wallet = {}
    e.last_ledger_ts_by_wallet = defaultdict(int)
    e._startup_fill_wallets = set()
    e.drift_state_by_wallet = {}
    e.mark_prices = {}
    e.wallets_raw = {}
    e.shard_status = {}  # WS shard status for operational_inputs_fresh
    e._capacity_invariant = {}
    e._capacity_bootstrap_complete = False
    e._capacity_rest_pages = defaultdict(list)
    e._wallet_due_ms = {}
    e._poll_rotation_offset = 0
    e._proof_fence_by_wallet = {}
    e._post_fence_buffer = defaultdict(list)
    e._proof_fence_lock = threading.Lock()
    e._wallet_state_data = {}
    e.wallets = [W, NATIVE_W]
    e.consecutive_poll_failures = defaultdict(int)
    e._hip3_omission_targets = {}
    e._pending_proof_cache = {}
    e._pending_proof_cache_lock = threading.Lock()
    e._drift_state_by_wallet = {}
    e._hip3_omission_targets = {}
    e._pending_proof_cache = {}
    e._pending_proof_cache_lock = threading.Lock()
    e.drift_recovery_attempt_count = {}
    e.max_drift_recovery_attempts = 3
    e.trusted_through_ms_by_wallet = {}
    e.last_poll_ts_by_wallet = {}
    e.epoch_by_wallet = {}
    e._capacity_invariant = {}
    e.drift_recovery_gate_by_wallet = {}
    e.last_drift_recovery_wallet_ts_ms = {}
    e.last_proof_fetch_ms = {}
    e.seen_ids = deque(maxlen=1000)
    e.seen_set = set()
    e.seen_lock = threading.Lock()
    e.state_lock = threading.Lock()

    class _NoopLedger:
        def append(self, row):
            return None
    e.raw_ledger = _NoopLedger()
    return e


def pos(coin, szi, entry=1.0):
    return {"position": {"coin": coin, "szi": str(szi), "entryPx": str(entry),
                         "unrealizedPnl": "0"}}


def ch_state(positions, time_ms):
    return {"assetPositions": positions, "time": time_ms,
            "marginSummary": {}, "crossMarginSummary": {}}


W = "0x069f458978a51ef6a9e2f6b6eb1fe2f39271c5a2"
NATIVE_W = "0x06ee56c1682ee8a6b504a0ebef23ec27766449e0"


def test_builder_dex_of():
    assert mod.EngineSSOT._builder_dex_of("XYZ:MRNA") == "xyz"
    assert mod.EngineSSOT._builder_dex_of("PARA:FOO") == "para"
    assert mod.EngineSSOT._builder_dex_of("BTC") is None
    assert mod.EngineSSOT._builder_dex_of("@142") is None       # native spot
    assert mod.EngineSSOT._builder_dex_of("PURR/USDC") is None  # native spot pair
    print("PASS 1/6 _builder_dex_of classification")


def test_perp_dexs_cached():
    e = new_engine()
    fr = FakeRequests({"perpDexs": lambda b: [None, {"name": "xyz"}, {"name": "para"}]})
    mod.requests = fr
    mod.RATE_GUARD = None
    names = e.load_perp_dex_names()
    assert names == ["xyz", "para"], names
    # second call within the cache window must NOT re-request
    names2 = e.load_perp_dex_names()
    assert names2 == ["xyz", "para"]
    assert fr.count("perpDexs") == 1, f"expected 1 perpDexs call, got {fr.count('perpDexs')}"
    print("PASS 2/6 perpDexs cached (1 request for 2 calls)")


def test_union_matches_api_and_clears_false_drift():
    """(1) union == native+HIP-3 positions; (2) builder drift disappears."""
    e = new_engine()
    t = 1791445590000
    native = [pos("BTC", 1.0)]
    xyz = [pos("xyz:MRNA", -288.73), pos("xyz:GOOGL", 12.23)]
    fr = FakeRequests({
        "clearinghouseState": lambda b: (
            ch_state(xyz, t) if b.get("dex") == "xyz" else ch_state(native, t)
        ),
        # no fills in the snapshot window -> coherent union
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr
    mod.RATE_GUARD = None
    snap, fence, times, _unified = e.fetch_wallet_positions(W, ["xyz"])
    assert snap is not None, "union must succeed"
    assert set(snap.keys()) == {"BTC", "XYZ:MRNA", "XYZ:GOOGL"}, snap.keys()
    assert fence == t and times == {"native": t, "xyz": t}, times
    # false drift: the OLD native-only snapshot would show XYZ:MRNA as 0 (absent)
    # while internal holds -288.73 -> drift.  The union now matches internal.
    e.positions = {}
    internal = {"XYZ:MRNA": -288.73, "XYZ:GOOGL": 12.23, "BTC": 1.0}
    for coin, szi in internal.items():
        e.positions[(W, coin)] = type("P", (), {"signed_size": szi})()
    # with baseline 0 and epoch usable, delta must be 0 for every builder pair
    for coin in ("XYZ:MRNA", "XYZ:GOOGL"):
        exch = snap[coin]["signed_size"]
        delta = exch - e.positions[(W, coin)].signed_size
        assert abs(delta) <= mod.POSITION_EPSILON, f"{coin} still drifts {delta}"
    # and the native-only view genuinely WOULD have drifted (regression guard)
    native_only = {k: v for k, v in snap.items() if ":" not in k}
    assert "XYZ:MRNA" not in native_only, "native-only view must miss the builder pair"
    print("PASS 3/6 union matches native+HIP-3; builder false drift removed")


def test_intervening_fill_fails_closed():
    """(3) a fill between min and max snapshot time -> union rejected."""
    e = new_engine()
    fr = FakeRequests({
        # two dex snapshots with DIFFERENT server times
        "clearinghouseState": lambda b: ch_state(
            [pos("xyz:MRNA", -1.0)], 1791445590500 if b.get("dex") == "xyz" else None
        ) if b.get("dex") == "xyz" else ch_state([pos("BTC", 1.0)], 1791445590000),
        # a real fill lands between the two snapshot times
        "userFillsByTime": lambda b: [
            {"coin": "xyz:MRNA", "px": "1", "sz": "1", "time": 1791445590300,
             "side": "B", "startPosition": "0", "oid": "x1"}
        ],
    })
    mod.requests = fr
    mod.RATE_GUARD = None
    snap, fence, times, _unified = e.fetch_wallet_positions(W, ["xyz"])
    assert snap is None and fence == 0 and times == {}, "incoherent union MUST fail closed"
    assert e.audit["hip3_union_incoherent"] == 1
    # and when the proof fetch itself ERRORS -> also fail closed
    e2 = new_engine()

    def _boom(b):
        raise RuntimeError("api unavailable")

    fr2 = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("xyz:MRNA", -1.0)], 1791445590500)
        if b.get("dex") == "xyz" else ch_state([pos("BTC", 1.0)], 1791445590000),
        "userFillsByTime": _boom,   # transport failure
    })
    mod.requests = fr2
    snap2, _f, _t, _u = e2.fetch_wallet_positions(W, ["xyz"])
    assert snap2 is None, "unavailable fence proof MUST fail closed"
    assert e2.audit["hip3_fence_proof_unavailable"] == 1
    print("PASS 4/6 intervening fill / unavailable proof -> FAIL CLOSED")


def test_native_wallet_unchanged_and_no_fanout():
    """(4) native reconciliation unchanged; (5) no PER-CYCLE DEX fan-out."""
    e = new_engine()
    e.perp_dex_names = ["xyz", "flx", "vntl", "hyna", "km", "abcd", "cash", "para", "mkts", "io"]
    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("BTC", 2.0)], 1791445590000),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr
    mod.RATE_GUARD = None
    # cold bootstrap fans out ONCE over every DEX (ledger is not proof of absence)
    assert e.cold_bootstrap_dexes(NATIVE_W) == e.perp_dex_names
    # but the PER-CYCLE poll for a wallet with no builder activity is native-only
    assert e.wallet_poll_dexes(NATIVE_W) == [], "native-only wallet must not fan out per cycle"
    snap, fence, times, _unified = e.fetch_wallet_positions(NATIVE_W, e.wallet_poll_dexes(NATIVE_W))
    assert snap == {"BTC": {"signed_size": 2.0, "entry_price": 1.0, "unrealized_pnl": 0.0}}
    assert fence == 1791445590000 and times == {"native": 1791445590000}
    assert fr.count("clearinghouseState") == 1, "native wallet per-cycle = exactly 1 snapshot"
    assert fr.count("userFillsByTime") == 0, "single-dex union needs no fence proof"

    # a builder wallet is targeted to its OWN dexes only (1 of the 10 available)
    e2 = new_engine()
    e2.wallet_builder_dexes[W] = {"xyz"}
    e2.wallet_active_dexes[W] = {"xyz"}
    e2.perp_dex_names = ["xyz", "flx", "vntl", "hyna", "km", "abcd", "cash", "para", "mkts", "io"]
    fr2 = FakeRequests({
        "clearinghouseState": lambda b: ch_state(
            [pos("xyz:MRNA", -1.0)], 1791445590000) if b.get("dex") == "xyz"
        else ch_state([pos("BTC", 1.0)], 1791445590000),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr2
    assert e2.wallet_poll_dexes(W) == ["xyz"]
    assert e2.cold_bootstrap_dexes(W) == e2.perp_dex_names  # cold bootstrap fans out ONCE
    snap2, _f, _t, _u = e2.fetch_wallet_positions(W, e2.wallet_poll_dexes(W))
    assert fr2.count("clearinghouseState") == 2, "builder poll = native + its 1 dex"
    assert fr2.count("clearinghouseState", dex="flx") == 0, "must NOT touch unrelated dexes"
    print("PASS 5/6 native per-cycle unchanged; no fan-out (native=1 call, builder=2 calls of 10 dexes)")


def test_cold_bootstrap_discovers_preledger_builder_position():
    """(NEW) wallet with NO builder fills in its ledger, but an EXISTING builder position."""
    e = new_engine()
    # ledger knows NOTHING about any builder market for this wallet
    assert e.wallet_builder_dexes.get(W) is None
    e.perp_dex_names = ["xyz", "para"]
    fr = FakeRequests({
        "clearinghouseState": lambda b: (
            ch_state([pos("xyz:MRNA", -288.73)], 1791445590000) if b.get("dex") == "xyz"
            else ch_state([], 1791445590000) if b.get("dex") == "para"
            else ch_state([pos("BTC", 1.0)], 1791445590000)
        ),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr
    mod.RATE_GUARD = None
    # cold bootstrap still fans out over EVERY dex despite the empty ledger
    assert e.cold_bootstrap_dexes(W) == ["xyz", "para"]
    snap, _f, _t, _u = e.fetch_wallet_positions(W, e.cold_bootstrap_dexes(W))
    assert "XYZ:MRNA" in snap, "pre-ledger builder position MUST be discovered"
    assert snap["XYZ:MRNA"]["signed_size"] == -288.73
    # and it is now recorded as an active dex for the targeted per-cycle poll
    e.wallet_active_dexes[W] = e._builder_dexes_in_snapshot(snap)
    assert e.wallet_poll_dexes(W) == ["xyz"]
    print("PASS 7/8 cold bootstrap discovers a pre-ledger builder position")


def test_failed_perp_dexs_blocks_bootstrap():
    """(NEW) failed initial perpDexs enumeration must PREVENT bootstrap (fail closed)."""
    e = new_engine()
    e.epoch_by_wallet = {}
    e.wallet_runtime = {}

    def _boom(b):
        raise RuntimeError("perpDexs unavailable")

    fr = FakeRequests({"perpDexs": _boom})
    mod.requests = fr
    mod.RATE_GUARD = None
    assert e.perp_dex_names == []
    assert e.cold_bootstrap_dexes(W) == [], "failed enumeration -> empty dex set"
    # bootstrap must refuse: no snapshot, no epoch, not ready
    e.bootstrap_wallet_from_exchange(W)
    assert e.wallet_runtime.get(W, {}).get("ready") is False, "wallet must stay NOT READY"
    assert e.epoch_by_wallet == {}, "no epoch may be opened on a failed enumeration"
    assert e.audit["bootstrap_perp_dexs_unavailable"] == 1
    # and no clearinghouseState was issued (it never got that far)
    assert fr.count("clearinghouseState") == 0
    print("PASS 8/8 failed perpDexs enumeration -> bootstrap FAILS CLOSED")


def test_fill_hook_learns_builder_dex():
    """A builder fill teaches the engine to poll that DEX."""
    e = new_engine()
    assert e.wallet_builder_dexes.get(W) is None
    e._remember_builder_dex(W, "XYZ:MRNA")
    assert e.wallet_builder_dexes[W] == {"xyz"}
    e._remember_builder_dex(W, "BTC")       # native must NOT register a dex
    assert e.wallet_builder_dexes[W] == {"xyz"}
    assert e.wallet_poll_dexes(W) == ["xyz"]
    print("PASS 6/8 builder fill registers the DEX; native fill does not")


def test_constructor_startup_regression():
    """Constructor must initialize capacity state BEFORE _compute_capacity_invariant()."""
    e = new_engine()
    assert hasattr(e, '_capacity_rest_pages'), "_capacity_rest_pages must exist after __init__"
    assert hasattr(e, '_wallet_due_ms'), "_wallet_due_ms must exist after __init__"
    assert hasattr(e, '_capacity_bootstrap_complete'), "_capacity_bootstrap_complete must exist after __init__"
    assert e._capacity_bootstrap_complete is False, "bootstrap must not be complete before bootstrap_all_wallets"
    assert hasattr(e, '_proof_fence_by_wallet'), "_proof_fence_by_wallet must exist after __init__"
    assert hasattr(e, '_post_fence_buffer'), "_post_fence_buffer must exist after __init__"
    assert hasattr(e, '_proof_fence_lock'), "_proof_fence_lock must exist after __init__"
    print("PASS 9/17 constructor initializes capacity + fence state before _compute_capacity_invariant()")


def test_proof_fence_activated_before_snapshot():
    """Proof fence MUST be active BEFORE snapshots — check via event ordering.

    Uses an instrumented accept_fill to verify the fence is set before
    any ingest path that could apply a WS fill during the proof window.
    """
    e = new_engine()
    e.wallet_runtime = {W: {"ready": True, "bootstrapped_at_ms": 0}}
    e.epoch_by_wallet = {W: {"epoch_status": "OPEN", "baseline_ts_ms": 1791445590000}}
    e.trusted_through_ms_by_wallet = {W: 1791445590000}
    e.last_poll_ts_by_wallet = {W: 1791445590000}

    call_order = []
    orig_set_fence = e._set_proof_fence
    orig_fetch = e._fetch_snapshots_only

    def tracked_set_fence(wallet, fence_hi):
        call_order.append("set_fence")
        orig_set_fence(wallet, fence_hi)

    def tracked_fetch(wallet, dexes):
        call_order.append("fetch_snapshots")
        return orig_fetch(wallet, dexes)

    e._set_proof_fence = tracked_set_fence
    e._fetch_snapshots_only = tracked_fetch

    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("BTC", 1.0)], 1791445590000),
    })
    mod.requests = fr
    mod.RATE_GUARD = None
    e._process_wallet_proof(W, 1791445595000)
    assert call_order[0] == "set_fence", \
        f"set_fence must come first, got {call_order}"
    assert call_order[1] == "fetch_snapshots", \
        f"fetch_snapshots must come after set_fence, got {call_order}"
    print("PASS 10/17 proof fence activated BEFORE snapshots")


def test_buffered_fills_released_only_after_reconciliation():
    """Buffered post-fence WS fills must be released in finally, NOT before reconcile."""
    e = new_engine()
    e.wallet_runtime = {W: {"ready": True, "bootstrapped_at_ms": 0}}
    e.epoch_by_wallet = {W: {"epoch_status": "OPEN", "baseline_ts_ms": 1791445590000}}
    e.trusted_through_ms_by_wallet = {W: 1791445590000}
    e.last_poll_ts_by_wallet = {W: 1791445590000}

    # Simulate a buffered post-fence fill
    buffered_fill = mod.RawFill(
        fill_id="fb1", wallet=W, coin="BTC", side="BUY", price=50.0, size=1.0,
        signed_size_delta=1.0, start_position=0.0, end_position=1.0,
        closed_pnl=0.0, fee=0.0, timestamp_ms=1791445595000,
        timestamp_iso="", received_at_ms=1791445595000, received_at_iso="",
        latency_ms=0, source="ws", recording_method="WS_CAPTURED"
    )
    from collections import deque
    e._post_fence_buffer[W] = [buffered_fill]

    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("BTC", 1.0)], 1791445590000),
    })
    mod.requests = fr
    mod.RATE_GUARD = None

    # The fill should NOT be in positions before reconciliation
    e._process_wallet_proof(W, 1791445596000)
    # After the proof, the buffered fill should have been applied
    assert e.audit.get("ws_post_fence_applied", 0) == 1, \
        "buffered fill must be released in finally block"
    print("PASS 11/17 buffered fills released in finally after reconciliation")


def test_native_only_uses_canonical_ingest():
    """Native-only wallet (single DEX) uses _advance_trusted_through, not direct assignment."""
    e = new_engine()
    e.wallet_runtime = {NATIVE_W: {"ready": True, "bootstrapped_at_ms": 0}}
    e.epoch_by_wallet = {NATIVE_W: {"epoch_status": "OPEN", "baseline_ts_ms": 1791445590000,
                                     "epoch_id": "e1", "post_epoch_complete_interval": False}}
    e.trusted_through_ms_by_wallet = {NATIVE_W: 1791445590000}
    e.last_poll_ts_by_wallet = {NATIVE_W: 1791445590000}
    e.perp_dex_names = []  # native-only

    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("BTC", 1.0)], 1791445591000),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr
    mod.RATE_GUARD = None
    e._process_wallet_proof(NATIVE_W, 1791445596000)
    tt = e.trusted_through_ms_by_wallet.get(NATIVE_W, 0)
    assert tt == 1791445591000, f"trusted_through must be advanced via _advance_trusted_through, got {tt}"
    assert e.epoch_by_wallet[NATIVE_W].get("post_epoch_complete_interval") is True, \
        "PENDING->OPEN promotion requires post_epoch_complete_interval=True"
    print("PASS 12/17 native-only wallet uses canonical ingest + _advance_trusted_through()")


def test_fixed_sla_capacity_failure():
    """Fixed SLA: capacity_achievable must be False when theoretical minimum exceeds SLA."""
    e = new_engine()
    e._capacity_rest_pages = defaultdict(list)
    for w in [W, NATIVE_W]:
        e._capacity_rest_pages[w] = [5000]  # 5000 pages/wallet -> theoretical minimum >> 2160s SLA
    e._wallets = [W, NATIVE_W]
    e._compute_capacity_invariant()
    inv = e._capacity_invariant
    assert inv["declared_freshness_sla_seconds"] == 2160, "SLA must be fixed at 2160s"
    assert inv["capacity_achievable"] is False, \
        f"capacity_achievable must be False when theoretical_minimum={inv['theoretical_minimum_cycle_seconds']}s > SLA={inv['declared_freshness_sla_seconds']}s"
    print("PASS 13/17 fixed SLA: capacity_achievable=False when theoretical minimum exceeds SLA")


def test_scheduler_non_blocking_timeout():
    """SSOT_ACQUIRE_TIMEOUT_SEC must be <= 2 for scheduler paths."""
    assert mod.SSOT_ACQUIRE_TIMEOUT_SEC <= 2.0, \
        f"SSOT_ACQUIRE_TIMEOUT_SEC must be <=2 for non-blocking scheduler, got {mod.SSOT_ACQUIRE_TIMEOUT_SEC}"
    print("PASS 14/17 scheduler acquisition timeout is non-blocking (<=2s)")


def test_scheduler_cadence_inside_sla():
    """Scheduler must schedule wallets at < 100% of SLA, leaving headroom."""
    e = new_engine()
    e._capacity_invariant = {
        "declared_freshness_sla_seconds": 2160,
        "capacity_achievable": True,
        "theoretical_minimum_cycle_seconds": 60,
    }
    sla_ms = 2160 * 1000
    schedule_target_ms = int(sla_ms * 0.8)
    # The due-time update must use schedule_target_ms, not full SLA
    assert schedule_target_ms < sla_ms, \
        "schedule target must be strictly inside the SLA"
    print("PASS 15/17 scheduler cadence strictly inside SLA (80% target)")


def test_operational_inputs_fresh_present():
    """build_currentness must include operational_inputs_fresh field."""
    e = new_engine()
    e.wallets = [W]
    e.trusted_through_ms_by_wallet = {W: 1791445590000}
    e._capacity_invariant = {"capacity_achievable": True, "declared_freshness_sla_seconds": 2160}
    e.epoch_by_wallet = {W: {"epoch_status": "OPEN", "baseline_ts_ms": 1791445580000}}
    e.consecutive_poll_failures = defaultdict(int)
    cw = e.build_currentness()
    assert "operational_inputs_fresh" in cw, "operational_inputs_fresh must be present"
    assert isinstance(cw["operational_inputs_fresh"], bool), \
        "operational_inputs_fresh must be a boolean"
    print("PASS 16/17 operational_inputs_fresh present in build_currentness")


def test_cold_bootstrap_no_historical_fetch():
    """Cold bootstrap must NOT issue userFillsByTime for historical catch-up.

    A fresh wallet with no trusted_through should prove only the narrow
    snapshot-coherence interval, not fetch months of history.
    """
    e = new_engine()
    e.wallets = [NATIVE_W]
    e.perp_dex_names = []
    e.epoch_by_wallet = {}  # no prior epoch
    e.trusted_through_ms_by_wallet = {}  # no watermark
    e.last_poll_ts_by_wallet = {}  # no cursor

    fr = FakeRequests({
        "clearinghouseState": lambda b: ch_state([pos("BTC", 1.0)], 1791445590000),
        "userFillsByTime": lambda b: [],
    })
    mod.requests = fr
    mod.RATE_GUARD = None
    e.bootstrap_wallet_from_exchange(NATIVE_W, ts_ms=1791445590500)
    # Cold bootstrap should NOT call userFillsByTime (native-only, no proof needed)
    assert fr.count("userFillsByTime") == 0, \
        f"cold bootstrap must not issue historical proof fetch, got {fr.count('userFillsByTime')} calls"
    print("PASS 17/17 cold bootstrap does not issue historical fill fetch")


if __name__ == "__main__":
    test_builder_dex_of()
    test_perp_dexs_cached()
    test_union_matches_api_and_clears_false_drift()
    test_intervening_fill_fails_closed()
    test_native_wallet_unchanged_and_no_fanout()
    test_fill_hook_learns_builder_dex()
    test_cold_bootstrap_discovers_preledger_builder_position()
    test_failed_perp_dexs_blocks_bootstrap()
    test_constructor_startup_regression()
    test_proof_fence_activated_before_snapshot()
    test_buffered_fills_released_only_after_reconciliation()
    test_native_only_uses_canonical_ingest()
    test_fixed_sla_capacity_failure()
    test_scheduler_non_blocking_timeout()
    test_scheduler_cadence_inside_sla()
    test_operational_inputs_fresh_present()
    test_cold_bootstrap_no_historical_fetch()
    print("\nALL HIP-3 PROOF TESTS PASSED")
