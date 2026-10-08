"""GATE B2 tests - the two deployment gates from the B1 review.

  B2.1  A shard that never reported a connection status (None / empty) must NOT
        report fresh.  B1 accepted it; the reviewer requires an EXPLICIT
        status == "OPEN".

  B2.2  At proof start, the fence was set to `now`.  A WS fill arriving DURING
        snapshot capture whose timestamp is below now (the exchange clock lags
        real time) is NOT yet proven, and a `now` sentinel fails the
        fill > fence test -- so it was queued INLINE and contaminated
        reconciliation before the real fence_hi was known.  Proof start now sets
        the fence to the TRUSTED boundary: anything above the watermark is
        buffered, and only already-proven fills are applied.
"""
import os
import sys
import threading
import json
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(r"C:\Users\wigma\trading_stack\Hyperliquid scanner\WALLET FINDER")

_ENGINE_DIR = os.environ.get("B8014_ENGINE_DIR")
if _ENGINE_DIR:
    sys.path.insert(0, _ENGINE_DIR)
    import HL_Copy_Engine_SSOT as mod
    assert Path(mod.__file__).resolve().parent == Path(_ENGINE_DIR).resolve(), (
        "B8014_ENGINE_DIR=%s but engine resolved to %s" % (_ENGINE_DIR, mod.__file__))
else:
    sys.path.insert(0, str(ROOT))
    import HL_Copy_Engine_SSOT as mod  # noqa: E402

W = "0x069f458978a51ef6a9e2f6b6eb1fe2f39271c5a2"
PASSED = []


def ok(n, msg):
    PASSED.append(n)
    print("PASS B%-5s %s" % (n, msg))


def shard_engine(statuses):
    """statuses: {shard_id: (status, age_ms, data_age_ms)}"""
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.shard_status = {}
    e._expected_shard_ids = lambda: sorted(statuses)
    now = mod.utc_now_ms()
    for sid, (st, age, dage) in statuses.items():
        e.shard_status[sid] = {
            "status": st,
            "last_msg_ms": now - age,
            "last_data_ms": now - dage,
        }
    return e


class _Queue:
    def __init__(self):
        self.items = []
        self._lock = threading.Lock()

    def put_nowait(self, item):
        with self._lock:
            self.items.append(item)

    def qsize(self):
        return len(self.items)


def ws_engine(fence_ms):
    """Engine whose wallet is READY, with a fence set and a no-op ledger."""
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e._proof_fence_by_wallet = {W: fence_ms}
    e._post_fence_buffer = defaultdict(list)
    e._proof_fence_lock = threading.Lock()
    e.event_queue = _Queue()
    e.seen_ids = deque(maxlen=200000)
    e.seen_set = set()
    e.seen_lock = threading.Lock()
    e.state_lock = threading.Lock()
    e.wallet_runtime = {W: {"ready": True, "bootstrapped_at_ms": 0, "ws_live": False}}
    e._startup_fill_wallets = set()
    e.last_ledger_ts_by_wallet = defaultdict(int)
    e.shard_status = defaultdict(dict)

    class _Noop:
        def append(self, row):
            return None
    e.raw_ledger = _Noop()
    return e


def ws_msg(fill_ts):
    return json.dumps({"channel": "userFills", "data": {"isSnapshot": False,
                                                        "user": W,
                                                        "fills": [
                                                            {"tid": "x", "coin": "BTC",
                                                             "side": "B", "px": "50",
                                                             "sz": "1", "time": fill_ts,
                                                             "startPosition": "0",
                                                             "dir": "Open Long",
                                                             "closedPnl": "0", "fee": "0",
                                                             "hash": "0xabc"}]}})


# ====================================================================== B2.1
def test_b21_no_status_is_not_fresh():
    """A shard that never reported a connection state must NOT keep trust alive."""
    e = shard_engine({0: (None, 1000, 1000), 1: ("OPEN", 1000, 1000)})
    assert e._ws_data_fresh() is False, (
        "B2.1 NOT FIXED: a shard with no status still reported fresh. The "
        "reviewer requires an EXPLICIT status == 'OPEN'; a shard that never "
        "connected must not keep WS trust alive.")
    ok("2.1", "a shard with no status does not report fresh")


def test_b21_empty_string_status_is_not_fresh():
    e = shard_engine({0: ("", 1000, 1000), 1: ("OPEN", 1000, 1000)})
    assert e._ws_data_fresh() is False, (
        "B2.1 NOT FIXED: a shard with an empty status string reported fresh")
    ok("2.1b", "a shard with an empty status string does not report fresh")


def test_b21_control_all_open_is_fresh():
    e = shard_engine({0: ("OPEN", 1000, 1000), 1: ("OPEN", 1000, 1000)})
    assert e._ws_data_fresh() is True, "control: all OPEN+recent must be fresh"
    ok("2.1c", "control: all shards OPEN reports fresh")


# ====================================================================== B2.2
def test_b22_unproven_fill_is_buffered_during_capture():
    """At proof start the fence is the TRUSTED boundary, not `now`.

    A WS fill arriving during snapshot capture whose timestamp is BELOW the
    local clock (exchange-time lag) but ABOVE the watermark is not yet proven,
    so it must be BUFFERED -- a `now` sentinel would fail the > test and queue
    it inline while the baseline is measured.
    """
    trusted = 5_000_000
    e = ws_engine(trusted)          # fence == trusted boundary, as at step 1
    e.trusted_through_ms_by_wallet = {W: trusted}

    now = mod.utc_now_ms()
    fill_ts = min(now - 5000, trusted + 1000)   # below now, above the watermark
    e._on_ws_message(0, None, ws_msg(fill_ts))

    assert len(e.event_queue.items) == 0, (
        "B2.2 NOT FIXED: an UNPROVEN fill was applied INLINE (queue=%r). During "
        "snapshot capture the fence is the TRUSTED boundary, so any fill above "
        "the watermark must be buffered; else it contaminates reconciliation "
        "before the baseline is measured."
        % ([f.timestamp_ms for f in e.event_queue.items],))
    assert fill_ts in [f.timestamp_ms for f in e._post_fence_buffer[W]], (
        "B2.2 NOT FIXED: the fill was neither queued nor buffered (queue=%d "
        "buffered=%r) -- it was silently DROPPED."
        % (len(e.event_queue.items),
           [f.timestamp_ms for f in e._post_fence_buffer[W]]))
    ok("2.2", "an unproven fill is buffered during proof-start capture")


def test_b22_control_proven_fill_is_queued():
    """A fill at or below the trusted watermark is already proven: safe inline."""
    trusted = 5_000_000
    e = ws_engine(trusted)
    e._on_ws_message(0, None, ws_msg(trusted - 1000))
    assert not e._post_fence_buffer.get(W), (
        "control failed: a fill BELOW the watermark must not be buffered")
    assert len(e.event_queue.items) == 1, (
        "control failed: a fill at/below the watermark must be queued; queue=%r"
        % ([f.timestamp_ms for f in e.event_queue.items],))
    ok("2.2c2", "a proven fill at/below the watermark is queued, not buffered")


def test_b22_proof_start_fence_is_the_trusted_boundary():
    """BEHAVIOURAL: run the real _process_wallet_proof and observe the fence.

    Earlier versions of this test set the fence by hand, which meant it passed
    even against the buggy engine -- a test that cannot fail on the defect it
    claims to cover proves nothing.  This drives the actual proof job (wallet
    ready, snapshot stubbed) and captures the fence value installed at step 1,
    so it fails on the `now` sentinel.
    """
    trusted = 5_000_000
    e = ws_engine(0)
    e._is_wallet_ready = lambda w: True            # skip the bootstrap branch
    e.trusted_through_ms_by_wallet = {W: trusted}
    e.last_poll_ts_by_wallet = {}
    e.consecutive_poll_failures = defaultdict(int)
    e.wallet_poll_dexes = lambda w: ["dex-a"]

    seen = {}
    real_set = e._set_proof_fence

    def spy(wallet, fence_ms):
        seen.setdefault("fences", []).append(int(fence_ms))
        return real_set(wallet, fence_ms)

    e._set_proof_fence = spy

    # Snapshot capture records the fence in force WHILE it is running, then
    # returns a valid snapshot so the job proceeds to the real fence in step 3.
    def snapshots(wallet, dexes):
        seen["during_capture"] = int(e._proof_fence_by_wallet.get(wallet, -1))
        return {"BTC": {"szi": "1.0", "px": "50.0"}}, {"dex-a": 9_000_000}

    e._fetch_snapshots_only = snapshots

    # Everything after the snapshot is irrelevant to this assertion; make the
    # proof path harmless so the job cannot touch state we did not stub.
    e.prove_interval_coherent = lambda *a, **kw: {
        "ok": True, "complete": True, "hip3_coherent": True,
        "rows": [], "rest_pages": 1, "reason": "proven"}
    e.ingest_fetched_rows = lambda *a, **kw: None
    e._advance_trusted_through = lambda *a, **kw: None
    e.set_exchange_baseline = lambda *a, **kw: None
    e.reconcile_wallet = lambda *a, **kw: None
    e.write_state = lambda *a, **kw: None

    try:
        e._process_wallet_proof(W, now=9_500_000)
    except Exception:
        pass  # the post-snapshot path is stubbed; the fence is what we assert

    during = seen.get("during_capture")
    assert during is not None, (
        "B2.2 test harness failed: snapshot capture never ran, so no fence was "
        "observed. The stubs must let _process_wallet_proof reach step 2.")

    assert during == trusted, (
        "B2.2 NOT FIXED: during snapshot capture the fence was %s, expected the "
        "TRUSTED boundary %s. With a `now` sentinel (%s) an unproven WS fill "
        "arriving during capture fails the fill > fence test and is applied "
        "INLINE, contaminating reconciliation before the real fence_hi is known."
        % (during, trusted, 9_500_000))
    ok("2.2d", "proof-start fence is the trusted boundary (observed at runtime)")

    # And the real fence from the snapshot must be installed afterwards.
    assert seen.get("fences") and seen["fences"][-1] == 9_000_000, (
        "B2.2: the real snapshot fence must be installed after capture; saw %r"
        % (seen.get("fences"),))
    ok("2.2e", "the real snapshot fence replaces the sentinel after capture")


def test_b22_control_above_fence_buffered_and_below_queued():
    """Control: with a FINITE fence, above-fence buffers and below-fence queues."""
    e = ws_engine(5_000_000)
    e._on_ws_message(0, None, ws_msg(5_000_001))     # above fence -> buffered
    e._on_ws_message(0, None, ws_msg(4_999_999))     # below fence -> queued
    assert [f.timestamp_ms for f in e._post_fence_buffer[W]] == [5_000_001], (
        "control failed: a fill ABOVE the fence must be buffered; got %r"
        % ([f.timestamp_ms for f in e._post_fence_buffer[W]],))
    assert [f.timestamp_ms for f in e.event_queue.items] == [4_999_999], (
        "control failed: a fill below the real fence must reach the queue; got %r"
        % ([f.timestamp_ms for f in e.event_queue.items],))
    ok("2.2c", "control: above-fence buffered, below-fence queued")



if __name__ == "__main__":
    print("=== GATE B2 tests ===")
    test_b21_no_status_is_not_fresh()
    test_b21_empty_string_status_is_not_fresh()
    test_b21_control_all_open_is_fresh()
    test_b22_unproven_fill_is_buffered_during_capture()
    test_b22_control_proven_fill_is_queued()
    test_b22_proof_start_fence_is_the_trusted_boundary()
    test_b22_control_above_fence_buffered_and_below_queued()
    print("")
    print("ALL GATE B2 TESTS PASSED (%d assertion-groups)" % len(PASSED))