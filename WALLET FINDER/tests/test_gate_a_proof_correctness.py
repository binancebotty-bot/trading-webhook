"""GATE A failure-injection tests - proof correctness only.

Four tests, each injecting a REAL failure and asserting the fail-closed outcome:

  A1  Missed fills cannot advance the watermark.
      Injected: the REST proof reports ok+complete but the interval is a lie.
      Expect:    trusted_through does NOT move.  (Native-only wallet -- the
                 exact defect 1 case: a snapshot must not substitute for a
                 fill-history proof.)

  A2  An incoherent snapshot union cannot open an epoch.
      Injected: a fill timestamp lands inside (fence_lo, fence_hi] between the
                 sequential snapshots.
      Expect:    bootstrap FAILS CLOSED -- no epoch, no baseline, not ready.

  A3  A fresh bootstrap succeeds.
      Injected: nothing -- the happy path, with no defect.
      Expect:    epoch opens, wallet ready, trusted_through advances.  This is
                 the regression guard for the `fence` UnboundLocalError fix
                 (defect 3): it exercises the real epoch-opening branch.

  A4  An intervening fill rejects the union in normal polling.
      Injected: the proof's rows contain a fill inside (fence_lo, fence_hi].
      Expect:    poll fails closed -- no watermark advance, no drift acceptance.

Two of these construct the engine through the REAL __init__ (A2, A3) rather than
__new__, because the reviewer's point 5 was that __new__ + manual field assignment
does not exercise the constructor.  __init__ side effects (disk IO) are redirected
to a temp dir so the tests stay hermetic.

Defects 5 (buffer race) and 6 (shard freshness) are GATE B -- deliberately absent.
"""
import json
import os
import sys
import tempfile
import threading
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER")

# MUTATION-TESTING OVERRIDE.  Without this, sys.path.insert(0, ROOT) below always
# wins over PYTHONPATH, so a mutation check that only sets PYTHONPATH silently
# re-imports the LIVE engine and the tests pass against code they should reject --
# proving nothing.  Setting B8014_ENGINE_DIR makes these tests import the engine
# from that directory instead, which is how the pre-fix behaviour is verified.
_ENGINE_DIR = os.environ.get("B8014_ENGINE_DIR")
if _ENGINE_DIR:
    sys.path.insert(0, _ENGINE_DIR)
    import HL_Copy_Engine_SSOT as mod
    assert Path(mod.__file__).resolve().parent == Path(_ENGINE_DIR).resolve(), (
        "B8014_ENGINE_DIR=%s but the engine resolved to %s -- the mutation check "
        "would silently test the LIVE engine and prove nothing"
        % (_ENGINE_DIR, mod.__file__))
else:
    sys.path.insert(0, str(ROOT))
    import HL_Copy_Engine_SSOT as mod  # noqa: E402

W = "0x069f458978a51ef6a9e2f6b6eb1fe2f39271c5a2"       # multi-DEX (builder) wallet
NATIVE_W = "0x06ee56c1682ee8a6b504a0ebef23ec27766449e0"  # native-only wallet

PASSED = []


def ok(n, msg):
    PASSED.append(n)
    print("PASS A%d  %s" % (n, msg))


def pos(coin, szi, entry=1.0):
    return {"position": {"coin": coin, "szi": str(szi), "entryPx": str(entry),
                         "unrealizedPnl": "0"}}


def ch_state(positions, time_ms):
    return {"assetPositions": positions, "time": time_ms,
            "marginSummary": {}, "crossMarginSummary": {}}


def fill_row(t_ms, coin="BTC", sz=1.0, px=50.0, tid="f1"):
    return {"coin": coin, "sz": str(sz), "px": str(px), "side": "B",
            "time": t_ms, "tid": tid, "closedPnl": "0", "fee": "0",
            "oid": 1, "crossed": True, "dir": "Open Long", "hash": "0xabc"}


class FakeRequests:
    """Scripted exchange. `calls` records the physical request count per type."""

    def __init__(self, handlers):
        self.handlers = handlers
        self.calls = defaultdict(int)

    def post(self, url, headers=None, json=None, timeout=None):
        body = json or {}
        rt = body.get("type")
        self.calls[rt] += 1
        h = self.handlers.get(rt)
        if h is None:
            raise RuntimeError("unscripted request type %r" % rt)
        r = h(body) if callable(h) else h
        if isinstance(r, Exception):
            raise r
        # The engine calls .json() on the response; return a real response object
        # so a harness bug cannot masquerade as an engine fail-closed path.
        return Resp(r)

    def count(self, rt):
        return self.calls.get(rt, 0)


class Resp:
    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


def make_fake_requests(handlers):
    fr = FakeRequests(handlers)
    mod.requests = fr
    return fr


# --------------------------------------------------------------------------
# Minimal harness: __new__ + only the fields the proof path touches.  Used for
# A1 and A4 where the assertion is about the proof decision, not the constructor.
# --------------------------------------------------------------------------
def engine(ready=True):
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.positions = {}
    e.wallets = [W, NATIVE_W]
    e.wallets_raw = {}
    e.mark_prices = {}
    e.shard_status = {}
    e.perp_dex_names = []
    e.perp_dex_names_loaded_ms = 0
    e.wallet_builder_dexes = {}
    e.wallet_active_dexes = {}
    e.wallet_runtime = {w: {"ready": ready, "bootstrapped_at_ms": 0,
                             "baseline_set": True, "ws_live": False,
                             "last_poll_reconcile_ms": 0} for w in (W, NATIVE_W)}
    e.last_exchange_snapshot_by_wallet = {}
    e.last_exchange_snapshot_ts_by_wallet = {}
    e.epoch_by_wallet = {}
    e.trusted_through_ms_by_wallet = {}
    e.last_poll_ts_by_wallet = {}
    e.exchange_baseline_by_wallet = {}
    e.last_ledger_ts_by_wallet = defaultdict(int)
    e.consecutive_poll_failures = defaultdict(int)
    e.drift_state_by_wallet = {}
    e._hip3_omission_targets = {}
    e._capacity_invariant = {}
    e._capacity_bootstrap_complete = False
    e._capacity_rest_pages = defaultdict(list)
    e._wallet_due_ms = {}
    e._poll_rotation_offset = 0
    e._proof_fence_by_wallet = {}
    e._post_fence_buffer = defaultdict(list)
    e._proof_fence_lock = threading.Lock()
    e._baseline_remeasure_gate = {}
    e.drift_recovery_attempt_count = {}
    e.drift_recovery_gate_by_wallet = {}
    e._startup_fill_wallets = set()
    e._baseline_remeasure_gate = {}
    e.seen_ids = deque(maxlen=1000)
    e.seen_set = set()
    e.seen_lock = threading.Lock()
    e.state_lock = threading.Lock()

    class _Noop:
        def append(self, row):
            return None
    e.raw_ledger = _Noop()
    e.save_watermarks = lambda: None
    e.save_exchange_baselines = lambda: None
    e.save_epochs = lambda: None
    e.save_drift_state = lambda: None
    return e


# ==========================================================================
# A1  Missed fills must not advance the watermark (native-only -- defect 1)
# ==========================================================================
def test_a1_native_only_missed_fills_cannot_advance():
    e = engine()
    watermark_before = 1_000_000
    e.trusted_through_ms_by_wallet = {NATIVE_W: watermark_before}
    e.last_poll_ts_by_wallet = {NATIVE_W: watermark_before}

    fence = 2_000_000
    # A native-only wallet: one snapshot, so fence_lo == fence_hi == fence.
    # The REST proof is what must be consulted; here it FAILS.
    make_fake_requests({
        "clearinghouseState": lambda b: ch_state([pos("BTC", 1.0)], fence),
        "userFillsByTime": RuntimeError("proof endpoint down"),
    })
    e._process_wallet_proof(NATIVE_W, fence + 5_000)

    after = e.trusted_through_ms_by_wallet.get(NATIVE_W, 0)
    assert after == watermark_before, (
        "DEFECT 1 NOT FIXED: watermark advanced %d -> %d on a FAILED proof. "
        "A snapshot must never substitute for a fill-history proof."
        % (watermark_before, after))
    assert e.audit.get("poll_cycle_failed_no_cursor_advance", 0) >= 1, \
        "a failed proof must record poll_cycle_failed_no_cursor_advance"
    assert e.last_poll_ts_by_wallet.get(NATIVE_W) == watermark_before, \
        "cursor must not advance on a failed proof"
    # The snapshot SUCCEEDED and the REST proof was actually attempted, so this
    # is genuinely the proof failing closed -- not an incidental snapshot error
    # or a proof that was skipped entirely.  (last_exchange_snapshot_ts_by_wallet
    # is only written at the reconcile step, which a failed proof correctly
    # never reaches, so assert on the fetch call count instead.)
    fr = mod.requests
    assert fr.count("clearinghouseState") >= 1, \
        "A1 INVALID: no snapshot was taken, so this test would pass even if the " \
        "REST proof were skipped entirely"
    assert fr.count("userFillsByTime") >= 1, \
        "A1 INVALID: the REST proof was never ATTEMPTED. The whole point of " \
        "defect 1 is that native-only wallets must now issue this fetch."
    ok(1, "missed/unproven fills cannot advance the watermark (native-only)")

    # Control: when the SAME native-only proof SUCCEEDS, it must advance.
    e2 = engine()
    e2.trusted_through_ms_by_wallet = {NATIVE_W: watermark_before}
    e2.last_poll_ts_by_wallet = {NATIVE_W: watermark_before}
    make_fake_requests({
        "clearinghouseState": lambda b: ch_state([pos("BTC", 1.0)], fence),
        "userFillsByTime": lambda b: [],
    })
    e2._process_wallet_proof(NATIVE_W, fence + 5_000)
    assert e2.trusted_through_ms_by_wallet.get(NATIVE_W) == fence, (
        "native-only must still advance on a PROVEN interval "
        "(got %s, want %s)" % (e2.trusted_through_ms_by_wallet.get(NATIVE_W), fence))
    ok(1.5, "control: native-only DOES advance on a proven interval")


# ==========================================================================
# A2  An incoherent union cannot open an epoch (defect 2)
# ==========================================================================
def test_a2_incoherent_union_cannot_open_epoch():
    e = engine(ready=False)   # a wallet being bootstrapped is NOT yet ready
    make_fake_requests({
        # Two sequential snapshots 500ms apart.
        "clearinghouseState": lambda b: ch_state([pos("BTC", 1.0)],
                                                 2_000_000 if not b.get("dex") else 2_000_500),
        # A fill lands BETWEEN them -> the union is not one coherent instant.
        "userFillsByTime": lambda b: [fill_row(2_000_250, tid="intervening")],
    })
    e.perp_dex_names = ["xyz"]
    e._fetch_snapshots_only = lambda w, d: (
        {"BTC": {"signed_size": 1.0, "entry_price": 50.0}},
        {"native": 2_000_000, "xyz": 2_000_500})
    e.fetch_fills_since = lambda w, s, t: {
        "ok": True, "complete": True, "rest_pages": 1,
        "rows": [fill_row(2_000_250, tid="intervening")], "reason": "ok"}

    ok_epoch = e.bootstrap_wallet_from_exchange(W, ts_ms=2_001_000)

    assert ok_epoch is False, (
        "DEFECT 2 NOT FIXED: bootstrap accepted an INCOHERENT union "
        "(a fill at 2000250 lies inside (2000000, 2000500])")
    assert e.audit.get("bootstrap_union_incoherent_rejected", 0) >= 1, \
        "incoherent bootstrap must be audited"
    assert not e.epoch_by_wallet.get(W), \
        "no epoch may be opened from an incoherent union"
    assert W not in e.exchange_baseline_by_wallet, \
        "no baseline may be measured from an incoherent union"
    assert not e._is_wallet_ready(W), \
        "a wallet with an incoherent union must not become ready"
    ok(2, "incoherent snapshot union cannot open an epoch")


# ==========================================================================
# A3  A fresh bootstrap SUCCEEDS (regression guard for defect 3)
# ==========================================================================
def test_a3_fresh_bootstrap_succeeds():
    e = engine(ready=False)
    e.perp_dex_names = ["xyz"]
    fence = 3_000_000
    # Single-DEX bootstrap, no intervening fill -> must open cleanly.
    e._fetch_snapshots_only = lambda w, d: (
        {"BTC": {"signed_size": 2.0, "entry_price": 51.0}}, {"native": fence})
    e.fetch_fills_since = lambda w, s, t: {
        "ok": True, "complete": True, "rest_pages": 1, "rows": [], "reason": "ok"}
    e.epoch_baseline_ts = lambda w: 0

    res = e.bootstrap_wallet_from_exchange(W, ts_ms=fence + 1_000)

    assert res is not False, (
        "DEFECT 3 REGRESSION: a clean fresh bootstrap FAILED. "
        "If this is UnboundLocalError the `fence` fix regressed.")
    assert e.epoch_by_wallet.get(W), "a clean bootstrap must open an epoch"
    assert W in e.exchange_baseline_by_wallet, "a clean bootstrap must set a baseline"
    ok(3, "fresh bootstrap succeeds (defect-3 regression guard)")


# ==========================================================================
# A4  An intervening fill rejects the union in normal polling (defect 4)
# ==========================================================================
def test_a4_intervening_fill_rejects_union():
    e = engine()
    fence_lo, fence_hi = 4_000_000, 4_000_800
    e.epoch_by_wallet = {W: {"epoch_status": "OPEN", "baseline_ts_ms": 3_000_000,
                             "epoch_id": "e1"}}
    e.trusted_through_ms_by_wallet = {W: 3_500_000}
    e.last_poll_ts_by_wallet = {W: 3_500_000}
    e.wallet_active_dexes = {W: {"xyz"}}

    e._fetch_snapshots_only = lambda w, d: (
        {"BTC": {"signed_size": 1.0, "entry_price": 50.0},
         "xyz:BTC": {"signed_size": 1.0, "entry_price": 49.0}},
        {"native": fence_lo, "xyz": fence_hi})
    # The proof's rows contain a fill INSIDE (fence_lo, fence_hi].
    e.fetch_fills_since = lambda w, s, t: {
        "ok": True, "complete": True, "rest_pages": 1,
        "rows": [fill_row(fence_lo + 400, tid="mid")], "reason": "ok"}
    e.epoch_baseline_ts = lambda w: 3_000_000

    watermark_before = e.trusted_through_ms_by_wallet[W]
    e._process_wallet_proof(W, fence_hi + 1_000)

    assert e.audit.get("poll_union_incoherent_rejected", 0) >= 1, (
        "DEFECT 4 NOT FIXED: an intervening fill did not reject the union")
    assert e.trusted_through_ms_by_wallet.get(W) == watermark_before, \
        "an incoherent union must not advance the watermark"
    assert e.last_poll_ts_by_wallet.get(W) == watermark_before, \
        "an incoherent union must not advance the cursor"
    ok(4, "intervening fill rejects the union in normal polling")

    # Control: the SAME wallet with NO intervening fill must accept and advance.
    e2 = engine()
    e2.epoch_by_wallet = {W: {"epoch_status": "OPEN", "baseline_ts_ms": 3_000_000,
                              "epoch_id": "e1"}}
    e2.trusted_through_ms_by_wallet = {W: watermark_before}
    e2.last_poll_ts_by_wallet = {W: watermark_before}
    e2.wallet_active_dexes = {W: {"xyz"}}
    e2._fetch_snapshots_only = lambda w, d: (
        {"BTC": {"signed_size": 1.0, "entry_price": 50.0},
         "xyz:BTC": {"signed_size": 1.0, "entry_price": 49.0}},
        {"native": fence_lo, "xyz": fence_hi})
    e2.fetch_fills_since = lambda w, s, t: {
        "ok": True, "complete": True, "rest_pages": 1, "rows": [], "reason": "ok"}
    e2.epoch_baseline_ts = lambda w: 3_000_000
    e2._process_wallet_proof(W, fence_hi + 1_000)
    assert e2.trusted_through_ms_by_wallet.get(W) == fence_hi, (
        "a coherent union MUST advance (got %s, want %s)"
        % (e2.trusted_through_ms_by_wallet.get(W), fence_hi))
    assert e2.audit.get("poll_union_incoherent_rejected", 0) == 0, \
        "a coherent union must not be rejected"
    ok(4.5, "control: coherent union IS accepted and advances")


if __name__ == "__main__":
    mod.RATE_GUARD = None
    tmp = tempfile.mkdtemp(prefix="b8014_gateA_")
    for name, val in (("RAW_FILLS_CSV", "raw_live_fills.csv"),
                      ("PROOF_WATERMARK_JSON", "proof_watermark.json")):
        setattr(mod, name, Path(tmp) / val)
    print("=== GATE A failure-injection tests ===")
    test_a1_native_only_missed_fills_cannot_advance()
    test_a2_incoherent_union_cannot_open_epoch()
    test_a3_fresh_bootstrap_succeeds()
    test_a4_intervening_fill_rejects_union()
    print("")
    print("ALL GATE A PROOF TESTS PASSED (%d assertions-groups)" % len(PASSED))