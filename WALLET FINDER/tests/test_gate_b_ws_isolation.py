"""GATE B failure-injection tests - WS isolation and observability.

  B1  The buffer release race (defect 5).
      A real CONCURRENT test: one thread streams fills into accept_fill() while
      another closes the fence, repeated over many rounds.  The old two-step
      check-then-append could interleave so the drain popped an empty buffer and
      the fill was stranded.  Asserts every fill is accounted for exactly once --
      applied inline OR present in the returned buffer.  No fill may vanish.

  B2  Queued-fill concurrency: many threads, same wallet, one fence.
      Asserts no fill is lost or double-applied under contention.

  B3  Freshness must fail closed (defect 6).
      One fresh shard out of four required shards must NOT report fresh.

  B4  REST fallback must be age-aware.
      A watermark that is months old must NOT report fresh.

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
    print("PASS B%-4s %s" % (n, msg))


def make_fill(fid, ts_ms, coin="BTC"):
    return mod.RawFill(
        fill_id=fid, wallet=W, coin=coin, side="BUY", price=50.0, size=1.0,
        signed_size_delta=1.0, start_position=0.0, end_position=1.0,
        closed_pnl=0.0, fee=0.0, timestamp_ms=ts_ms,
        timestamp_iso="", received_at_ms=ts_ms, received_at_iso="",
        latency_ms=0, source="ws", recording_method=mod.RECORDING_WS_CAPTURED)


def engine(wallets=None):
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.wallets = wallets if wallets is not None else [W]
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

    class _Noop:
        def append(self, row):
            return None
    e.raw_ledger = _Noop()
    return e


# ==========================================================================
# B1  The release race, under real concurrency
# ==========================================================================
def test_b1_no_fill_is_stranded_by_the_drain():
    e = engine()
    FENCE = 1_000_000
    ROUNDS = 300
    PER_ROUND = 8
    stranded = []
    total_accounted = 0

    for r in range(ROUNDS):
        produced = []
        holder = {}                      # drainer's return value, captured safely
        start = threading.Event()

        def producer():
            start.wait()
            for i in range(PER_ROUND):
                f = make_fill("r%d-f%d" % (r, i), FENCE + 1000 + i)
                produced.append(f)
                # Every fill is newer than the fence.  accept_fill returns False
                # when it was BUFFERED, True when the fence was already gone and
                # it was applied inline (also correct -- the proof window closed).
                if e.accept_fill(f, persist=False):
                    holder.setdefault("inline", []).append(f.fill_id)

        def drainer():
            start.wait()
            holder["drained"] = e._clear_proof_fence(W)

        t1 = threading.Thread(target=producer)
        t2 = threading.Thread(target=drainer)
        t1.start()
        t2.start()
        start.set()
        t1.join()
        t2.join()

        # Anything still in the buffer after the drain is also ours.
        with e._proof_fence_lock:
            leftover = [f.fill_id for f in e._post_fence_buffer.get(W, [])]
            e._post_fence_buffer.pop(W, None)

        accounted = set(holder.get("drained") and [f.fill_id for f in holder["drained"]] or [])
        accounted |= set(leftover)
        accounted |= set(holder.get("inline", []))
        for f in produced:
            if f.fill_id not in accounted:
                stranded.append((r, f.fill_id))
        total_accounted += len(accounted)

    assert not stranded, (
        "DEFECT 5 NOT FIXED: %d fill(s) STRANDED -- neither buffered-then-released "
        "nor applied inline. e.g. %s" % (len(stranded), stranded[:5]))
    assert total_accounted == ROUNDS * PER_ROUND, (
        "expected %d fills accounted for, got %d"
        % (ROUNDS * PER_ROUND, total_accounted))
    ok(1, "no fill is stranded by the drain under %d concurrent rounds" % ROUNDS)

    # ---- DETERMINISTIC interleaving -------------------------------------
    # The probabilistic test above MISSES defect 5: the window is a couple of
    # bytecodes and the GIL rarely schedules the drain exactly there.  A test
    # that passes on buggy code proves nothing, so force the interleaving.
    #
    # The buggy path takes TWO lock acquisitions inside accept_fill:
    #     acquire -> read fence -> RELEASE -> acquire -> append -> release
    # The stranding window is between that first release and the second acquire.
    # This wrapper counts releases but only becomes active AFTER the fence is
    # installed (_set_proof_fence takes a release of its own), so release #1
    # inside accept_fill is the race window.  The buggy path then re-acquires and
    # appends into a buffer that is now empty and whose wallet has no fence, so
    # the fill is stranded.  The fixed path takes ONE acquisition and appends
    # inside it, so the drain lands after the append and nothing is lost.
    e3 = engine()
    FENCE3 = 7_000_000
    hook = {"armed": False, "drained": None}

    class InterleavingLock:
        def __init__(self):
            self._real = threading.Lock()
            self.releases = 0
            self.acquisitions = 0

        def __enter__(self):
            self._real.acquire()
            if not hook.get("firing"):
                self.acquisitions += 1        # the hook's own drain is not counted
            return self

        def __exit__(self, *exc):
            self._real.release()
            if hook["armed"] and not exc:
                self.releases += 1
                if self.releases == 1:
                    # THE race window: fence read, lock dropped, append pending.
                    hook["armed"] = False          # fire exactly once
                    hook["firing"] = True
                    try:
                        hook["drained"] = e3._clear_proof_fence(W)
                    finally:
                        hook["firing"] = False
            return False

    lock = InterleavingLock()
    e3._proof_fence_lock = lock
    e3._proof_fence_by_wallet = {}
    e3._set_proof_fence(W, FENCE3)
    hook["armed"] = True                  # arm AFTER the fence install
    lock.acquisitions = 0                 # baseline: count only accept_fill
    lock.releases = 0

    fill3 = make_fill("raced", FENCE3 + 10)
    applied = e3.accept_fill(fill3, persist=False)
    # snapshot NOW: the explicit drain below takes the lock again, which must not
    # be counted as accept_fill's own usage.
    accept_acquisitions = lock.acquisitions
    hook["armed"] = True
    drained3 = e3._clear_proof_fence(W)
    accounted = ([f.fill_id for f in (hook["drained"] or [])]
                 + [f.fill_id for f in drained3])

    assert accept_acquisitions == 1, (
        "EXPECTED the fixed single-acquisition path, saw %d acquisitions in "
        "accept_fill -- accept_fill is still taking the lock twice"
        % accept_acquisitions)
    assert fill3.fill_id in accounted or applied, (
        "DEFECT 5 NOT FIXED: a fill was STRANDED in the drain/re-acquire window. "
        "released=%s applied_inline=%s -- the fill exists nowhere and the "
        "position silently lost it" % (accounted, applied))
    ok(1.7, "deterministic drain/re-acquire interleaving strands no fill")

    # Control: with NO concurrency the fence still buffers correctly.
    e2 = engine()
    e2._set_proof_fence(W, FENCE)
    f = make_fill("solo", FENCE + 5)
    assert e2.accept_fill(f, persist=False) is False, "a post-fence fill must be buffered"
    got = e2._clear_proof_fence(W)
    assert [x.fill_id for x in got] == ["solo"], "the drain must return the buffered fill"
    ok(1.5, "control: sequential fence buffers and drains exactly one fill")


# ==========================================================================
# B2  Queued-fill concurrency: no loss, no double-apply
# ==========================================================================
def test_b2_concurrent_fills_applied_exactly_once():
    e = engine()
    N_THREADS = 8
    N_EACH = 200
    base_ts = 500_000

    def worker(tid):
        for i in range(N_EACH):
            f = make_fill("t%d-%d" % (tid, i), base_ts + i)
            e.accept_fill(f, persist=False)

    ts = [threading.Thread(target=worker, args=(t,)) for t in range(N_THREADS)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    total = N_THREADS * N_EACH
    assert e.audit.get("fills_applied", 0) == total, (
        "expected %d applied, got %s (duplicates=%s)"
        % (total, e.audit.get("fills_applied", 0), e.audit.get("duplicates", 0)))
    assert len(e.seen_set) == total, (
        "seen-set should hold %d ids, holds %d" % (total, len(e.seen_set)))
    ok(2, "%d concurrent fills applied exactly once, none lost or doubled"
       % total)


# ==========================================================================
# B3  Freshness fails closed when shards are missing (defect 6)
# ==========================================================================
def test_b3_freshness_requires_every_shard():
    wallets = ["0x%040x" % (i + 1) for i in range(8)]
    e = engine(wallets=wallets)
    per_shard = int(os.getenv("HL_WALLETS_PER_SHARD", "0") or 0) or mod.WALLETS_PER_SHARD
    mod.WALLETS_PER_SHARD = per_shard
    n_shards = len(mod.chunked(wallets, per_shard))
    assert n_shards > 1, ("need multiple shards for this test; "
                          "WALLETS_PER_SHARD=%s over %d wallets gave %d"
                          % (per_shard, len(wallets), n_shards))
    now = mod.utc_now_ms()

    # Three shards stale, ONE fresh -- must NOT report fresh.
    e.shard_status = {0: {"status": "OPEN", "last_data_ms": now},
                      1: {"status": "OPEN", "last_data_ms": 0},
                      2: {"status": "OPEN", "last_data_ms": now - 10**7},
                      3: {"status": "ERROR", "last_data_ms": 0}}
    assert e._expected_shard_ids() == set(range(n_shards)), "expected-shard derivation wrong"
    assert e._ws_data_fresh() is False, (
        "DEFECT 6 NOT FIXED: 1 fresh shard out of %d reported FRESH. Partial "
        "coverage must fail closed." % n_shards)
    ok(3, "freshness fails closed with %d/%d shards healthy" % (1, n_shards))

    # Control: ALL shards fresh -> fresh.
    e.shard_status = {i: {"status": "OPEN", "last_data_ms": now} for i in range(n_shards)}
    assert e._ws_data_fresh() is True, "all shards fresh must report fresh"
    ok(3.5, "control: all %d shards healthy reports fresh" % n_shards)


# ==========================================================================
# B4  REST fallback must be age-aware
# ==========================================================================
def test_b4_rest_freshness_is_age_aware():
    e = engine()
    now = mod.utc_now_ms()
    # A watermark set at the Unix epoch is non-zero but ancient.
    e.trusted_through_ms_by_wallet = {W: 1_000_000_000_000}
    assert e._rest_fresh() is False, (
        "DEFECT 6 NOT FIXED: an ancient watermark reported FRESH. Age must matter.")
    ok(4, "an ancient watermark does not report fresh")

    e.trusted_through_ms_by_wallet = {W: now - 5_000}
    assert e._rest_fresh() is True, "a recent watermark must report fresh"
    ok(4.5, "control: a recent watermark reports fresh")

    e.trusted_through_ms_by_wallet = {}
    assert e._rest_fresh() is False, "no watermarks must not report fresh"


if __name__ == "__main__":
    print("=== GATE B failure-injection tests ===")
    test_b1_no_fill_is_stranded_by_the_drain()
    test_b2_concurrent_fills_applied_exactly_once()
    test_b3_freshness_requires_every_shard()
    test_b4_rest_freshness_is_age_aware()
    print("")
    print("ALL GATE B TESTS PASSED (%d assertion-groups)" % len(PASSED))