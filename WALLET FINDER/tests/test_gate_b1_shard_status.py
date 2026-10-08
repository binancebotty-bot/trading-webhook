"""GATE B1 failure-injection tests - WS receive race and shard connection status.

  B1.7  The RECEIVE-path check-then-act gap.  _on_ws_message() read the fence
        under the lock, released it, then re-acquired via
        _buffer_post_fence_fill() to append.  A drain in that window stranded
        the fill.  As with Gate B defect 5 the probabilistic version passes on
        buggy code, so this forces the interleaving deterministically: the
        receive path must take ONE acquisition, and the fill must be accounted
        for exactly once -- buffered, or queued.

  B1.8  A fill must NEVER be applied INLINE while a fence is active.  That is
        the fence's entire purpose: the baseline is being measured, so applying
        it inline would fold an unproven fill into a measured epoch.

  B1.9  Every expected shard must be OPEN as well as fresh.  A shard marked
        ERROR or CLOSED with a RECENT last_data_ms must NOT report fresh --
        that is stale residue keeping WS trust alive for a dead shard.
  B1.9c control: an OPEN, recent shard reports fresh.

Honours B8014_ENGINE_DIR exactly like the Gate A suite, so the mutation check
can run it against a pre-fix engine.
"""
import os
import sys
import threading
import time
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER")

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


def engine():
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.wallets = [W]
    e.positions = {}
    e.wallets_raw = {}
    e.mark_prices = {}
    e.shard_status = {}
    e.seen_ids = deque(maxlen=200000)
    e.seen_set = set()
    e.seen_lock = threading.Lock()
    e.state_lock = threading.Lock()
    e.last_ledger_ts_by_wallet = defaultdict(int)
    e._proof_fence_by_wallet = {}
    e._post_fence_buffer = defaultdict(list)
    e._proof_fence_lock = threading.Lock()
    e.event_queue = _Queue()
    e._startup_fill_wallets = set()
    e.wallet_runtime = {}
    e.stop_event = threading.Event()

    class _Noop:
        def append(self, row):
            return None
    e.raw_ledger = _Noop()
    return e


class _Queue:
    """Minimal stand-in for queue.Queue with the same put_nowait contract."""

    def __init__(self):
        self.items = []
        self._lock = threading.Lock()

    def put_nowait(self, item):
        with self._lock:
            self.items.append(item)

    def get_nowait(self):
        with self._lock:
            return self.items.pop(0)

    def qsize(self):
        return len(self.items)


def ws_fill(fid, ts_ms):
    return {
        "tid": fid, "coin": "BTC", "side": "B", "px": "50.0", "sz": "1.0",
        "time": int(ts_ms), "startPosition": "0.0", "dir": "Open Long",
        "closedPnl": "0.0", "fee": "0.0", "hash": "0xdead",
    }


def ready_engine(fence_ms):
    """An engine whose wallet is READY, so _on_ws_message does not skip it."""
    e = engine()
    e.wallet_runtime = {W: {"ready": True, "bootstrapped_at_ms": 0, "ws_live": False}}
    if fence_ms is not None:
        e._set_proof_fence(W, fence_ms)
    return e


# ==========================================================================
# B1.7  the receive-path race, forced deterministically
# ==========================================================================
def test_b17_receive_path_takes_one_acquisition():
    e = ready_engine(None)
    FENCE = 5_000_000
    hook = {"armed": False, "drained": None}

    class InterleavingLock:
        def __init__(self):
            self._real = threading.Lock()
            self.releases = 0
            self.acquisitions = 0

        def __enter__(self):
            self._real.acquire()
            if not hook.get("firing"):
                self.acquisitions += 1
            return self

        def __exit__(self, *exc):
            self._real.release()
            if hook["armed"] and not exc:
                self.releases += 1
                if self.releases == 1:
                    # THE race window: fence read, lock dropped, append pending.
                    hook["armed"] = False
                    hook["firing"] = True
                    try:
                        hook["drained"] = e._clear_proof_fence(W)
                    finally:
                        hook["firing"] = False
            return False

    lock = InterleavingLock()
    e._proof_fence_lock = lock
    e._proof_fence_by_wallet = {}
    e._set_proof_fence(W, FENCE)
    hook["armed"] = True
    lock.acquisitions = 0            # count only the receive path
    lock.releases = 0

    import json
    msg = json.dumps({"channel": "userFills", "data": {"isSnapshot": False,
                                                        "user": W,
                                                        "fills": [ws_fill(7001, FENCE + 10)]}})
    e._on_ws_message(0, None, msg)

    accept_acquisitions = lock.acquisitions
    hook["armed"] = True
    drained = e._clear_proof_fence(W)
    queued = list(e.event_queue.items)
    buffered = ((hook["drained"] or []) + drained)

    # The engine builds a COMPOSITE fill_id (wallet:coin:ts:side:...), not the
    # raw tid, so assert on the identity that survives: the fill timestamp.
    buffered_ts = [int(f.timestamp_ms) for f in buffered]
    FILL_TS = FENCE + 10

    assert accept_acquisitions == 1, (
        "B1.7 NOT FIXED: the WS receive path took %d lock acquisitions (the old "
        "check-then-append took 2). The fence read and the buffer append must "
        "happen inside ONE acquisition." % accept_acquisitions)
    assert not queued, (
        "B1.8 FAILURE: the fill was applied INLINE (%r) while a proof fence was "
        "active. The fence exists so a fill arriving during a measured baseline "
        "is buffered, not folded into the epoch." % (queued,))
    assert FILL_TS in buffered_ts, (
        "B1.7 FAILURE: the WS fill was STRANDED. released_ts=%s queued=%s -- a "
        "real fill silently lost from the wallet" % (buffered_ts, queued))
    ok("1.7", "receive path takes one acquisition and strands no fill")


def test_b17_control_no_fence_queues_the_fill():
    """Control: with no fence the fill must be QUEUED, not buffered."""
    e = ready_engine(None)
    import json
    msg = json.dumps({"data": {"isSnapshot": False, "user": W,
                               "fills": [ws_fill(7101, 6_000_000)]}})
    e._on_ws_message(0, None, msg)

    assert not e._post_fence_buffer.get(W), (
        "control failed: with no fence nothing may be buffered, got %r"
        % (e._post_fence_buffer.get(W),))
    assert len(e.event_queue.items) == 1, (
        "control failed: with no fence the fill must be queued, queue=%r"
        % (e.event_queue.items,))
    ok("1.7c", "no fence means the fill is queued, not buffered")


def test_b18_fill_older_than_fence_is_queued():
    """A fill at or before the fence is already covered by the proof: queue it."""
    e = ready_engine(5_000_000)
    import json
    msg = json.dumps({"data": {"isSnapshot": False, "user": W,
                               "fills": [ws_fill(7201, 4_000_000)]}})
    e._on_ws_message(0, None, msg)

    assert not e._post_fence_buffer.get(W), (
        "a fill BEFORE the fence is covered by the proof and must not be "
        "re-buffered; got %r" % (e._post_fence_buffer.get(W),))
    assert len(e.event_queue.items) == 1, (
        "a fill before the fence must still be queued; queue=%r"
        % (e.event_queue.items,))
    ok("1.8", "a fill at or before the fence is queued, not buffered")


# ==========================================================================
# B1.9  shard connection status
# ==========================================================================
def shard_engine(statuses):
    """statuses: {shard_id: (status, age_ms)}"""
    e = engine()
    e.wallet_runtime = {}
    e.expected = sorted(statuses)
    e._expected_shard_ids = lambda: e.expected
    now = mod.utc_now_ms()
    e.shard_status = {
        sid: {"status": st, "last_data_ms": now - age, "last_msg_ms": now - age}
        for sid, (st, age) in statuses.items()}
    return e


def test_b19_closed_shard_with_recent_data_is_not_fresh():
    e = shard_engine({0: ("CLOSED", 1000), 1: ("OPEN", 1000)})
    fresh = e._ws_data_fresh()
    assert fresh is False, (
        "B1.9 NOT FIXED: a CLOSED shard with a RECENT last_data_ms still reported "
        "fresh. A dead shard's last message is stale residue, not liveness, and "
        "it must not keep WS trust alive.")
    ok("1.9", "a CLOSED shard with recent data does not report fresh")


def test_b19_error_shard_with_recent_data_is_not_fresh():
    e = shard_engine({0: ("ERROR", 500), 1: ("OPEN", 500)})
    assert e._ws_data_fresh() is False, (
        "B1.9 NOT FIXED: an ERROR shard with a recent last_data_ms reported fresh")
    ok("1.9b", "an ERROR shard with recent data does not report fresh")


def test_b19_connecting_shard_is_not_fresh():
    e = shard_engine({0: ("CONNECTING", 100), 1: ("OPEN", 100)})
    assert e._ws_data_fresh() is False, (
        "B1.9 NOT FIXED: a CONNECTING shard reported fresh -- it is not "
        "receiving fills yet")
    ok("1.9c", "a CONNECTING shard does not report fresh")


def test_b19_control_all_open_and_recent_is_fresh():
    e = shard_engine({0: ("OPEN", 1000), 1: ("OPEN", 1000)})
    assert e._ws_data_fresh() is True, (
        "control failed: two OPEN, recently-active shards must report fresh")
    ok("1.9d", "control: all shards OPEN and recent reports fresh")


def test_b19_control_one_old_shard_still_fails():
    """The original freshness fix must survive the status change."""
    e = shard_engine({0: ("OPEN", 10_000_000), 1: ("OPEN", 1000)})
    assert e._ws_data_fresh() is False, (
        "control failed: one ancient shard must still fail closed")
    ok("1.9e", "control: one stale shard still fails closed")


if __name__ == "__main__":
    print("=== GATE B1 failure-injection tests ===")
    test_b17_receive_path_takes_one_acquisition()
    test_b17_control_no_fence_queues_the_fill()
    test_b18_fill_older_than_fence_is_queued()
    test_b19_closed_shard_with_recent_data_is_not_fresh()
    test_b19_error_shard_with_recent_data_is_not_fresh()
    test_b19_connecting_shard_is_not_fresh()
    test_b19_control_all_open_and_recent_is_fresh()
    test_b19_control_one_old_shard_still_fails()
    print("")
    print("ALL GATE B1 TESTS PASSED (%d assertion-groups)" % len(PASSED))