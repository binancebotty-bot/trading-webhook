"""GATE A1 regression tests - the three cases the controller specified.

  A1.1  A fill BEFORE start_ms but INSIDE the snapshot span (fence_lo, fence_hi]
        must make the union INCOHERENT.  Pre-fix the fetch began at start_ms, so
        that fill was never returned, the coherence scan never saw it, and an
        incoherent union was accepted.

  A1.2  A STALE snapshot (fence_hi earlier than the already-proven watermark) must
        be rejected outright rather than moving trust backwards.

  A1.3  A REJECTED bootstrap must leave NO cached snapshot state behind --
        no active-dexes entry, no snapshot, no timestamp.

Each is paired with a control that passes on the fixed code for the right reason.
"""

import os
import sys
import threading
import collections

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Allow the mutation harness to point the import at a different engine build.
ENGINE_DIR = os.environ.get("B8014_ENGINE_DIR", ROOT)
if ENGINE_DIR not in sys.path:
    sys.path.insert(0, ENGINE_DIR)

import HL_Copy_Engine_SSOT as engine_mod


def _isolate_proof_state():
    """Redirect every persisted proof path into a per-run temp directory.

    The engine writes epochs, watermarks and the recovery gate to real files at
    import time.  Tests must never create, truncate or delete those.  After
    rebuilding the DATA_DIR-based paths we chdir into the temp tree as well, so
    any relative path the engine resolves also lands there.
    """
    import shutil
    import tempfile
    from pathlib import Path as _Path

    tmp = tempfile.mkdtemp(prefix="b8014_gate_a_")
    for name in ("PROOF_EPOCH_JSON", "PROOF_WATERMARK_JSON", "RECOVERY_GATE_JSON",
                 "RAW_FILLS_CSV"):
        cur = getattr(engine_mod, name, None)
        if not cur:
            continue
        # keep the ORIGINAL type: CsvLedger and friends call .parent on these.
        setattr(engine_mod, name, _Path(tmp) / _Path(str(cur)).name)
    os.chdir(tmp)
    return tmp


_ISOLATED = _isolate_proof_state()
print("proof state isolated to: %s" % _ISOLATED)

assert os.path.dirname(os.path.abspath(engine_mod.__file__)) == \
    os.path.abspath(ENGINE_DIR), (
        "imported engine from %s but ENGINE_DIR is %s" % (engine_mod.__file__, ENGINE_DIR))

_raw_get = engine_mod.raw_get
_inum = engine_mod.inum

W = "0x069f458978a51ef6a9e2f6b6eb1fe2f39271c5a2"

PASSES = []


def ok(tag, msg):
    PASSES.append(tag)
    print("PASS %-5s %s" % (tag, msg))


def engine():
    """A bare engine with exactly the state these two paths touch."""
    e = engine_mod.EngineSSOT.__new__(engine_mod.EngineSSOT)
    e.audit = collections.defaultdict(int)
    e.trusted_through_ms_by_wallet = {}
    e.wallet_active_dexes = {}
    e.last_exchange_snapshot_by_wallet = {}
    e.last_exchange_snapshot_ts_by_wallet = {}
    e.wallet_runtime = {}
    e._startup_fill_wallets = set()
    e.fetch_log = []
    return e


def install_fetch(e, rows):
    """Stub the single REST fetch, recording the window it was asked for."""

    def fake_fetch(wallet, start_ms, end_ms, *a, **kw):
        e.fetch_log.append((start_ms, end_ms))
        return {"ok": True, "complete": True, "rows": rows, "rest_pages": 1}

    e.fetch_fills_since = fake_fetch
    e._update_capacity_with_rest_pages = lambda *a, **kw: None
    return e


def row(ts_ms, tid):
    return {"time": ts_ms, "tid": tid, "coin": "BTC", "side": "BUY",
            "px": "50.0", "sz": "1.0", "startPosition": "0.0",
            "dir": "Open Long", "closedPnl": "0.0", "fee": "0.0", "hash": "0xdead"}


# ============================================================ A1.1
def test_a11_fill_before_start_ms_inside_span_is_caught():
    e = engine()
    e.trusted_through_ms_by_wallet[W] = 1200

    # The controller's own example: fences 1000-1500, watermark 1200, a fill at
    # 1100 -- BEFORE start_ms=1200 but INSIDE the snapshot span.
    fill_in_span = row(1100, 9001)
    install_fetch(e, [fill_in_span])

    proof = e.prove_interval_coherent(W, 1200, 1000, 1500)

    assert len(e.fetch_log) == 1, "expected exactly ONE fetch, got %d" % len(e.fetch_log)
    scan_start, scan_end = e.fetch_log[0]

    assert scan_start <= 1000, (
        "DEFECT 1 NOT FIXED: the fetch started at %d, so a fill at 1100 (before "
        "start_ms=1200 but inside the span) was never requested and the union "
        "was wrongly accepted. The scan must start at min(start_ms, fence_lo)=1000."
        % scan_start)
    assert scan_end == 1500, "fetch end %d != fence_hi 1500" % scan_end
    assert proof.get("hip3_coherent") is False, (
        "DEFECT 1 NOT FIXED: an incoherent union was ACCEPTED (reason=%r)"
        % proof.get("reason"))
    assert proof.get("complete") is False, (
        "a caller seeing complete=True could still advance the watermark")
    assert fill_in_span in proof.get("rows", []), (
        "the fill was detected but not returned to the caller, so it cannot be "
        "ingested -- it would be silently lost")
    ok("A1.1", "fill before start_ms but inside the span is rejected as incoherent")


def test_a11_control_clean_span_is_coherent():
    """Control: no fill in the span -> coherent, and the fetch still starts early."""
    e = engine()
    e.trusted_through_ms_by_wallet[W] = 1200
    # Both fills sit OUTSIDE the coherence span (fence_lo, fence_hi] =
    # (1000, 1500]: one at/below fence_lo, one after fence_hi.  A widened scan
    # must therefore still report the span clean.
    install_fetch(e, [row(1000, 9002), row(1501, 9003), row(1600, 9004)])

    proof = e.prove_interval_coherent(W, 1200, 1000, 1500)
    scan_start, _ = e.fetch_log[0]

    assert proof.get("hip3_coherent") is True, (
        "control failed: a clean span must stay coherent (reason=%r)"
        % proof.get("reason"))
    assert proof.get("complete") is True, "a clean span must still prove completeness"
    assert scan_start == 1000, (
        "the widened scan must persist for clean spans too, or the defect "
        "returns for a wallet whose next fill lands earlier; got %d" % scan_start)
    ok("A1.1c", "clean span stays coherent and still scans from fence_lo")


# ============================================================ A1.2
def test_a12_stale_snapshot_is_rejected():
    e = engine()
    e.trusted_through_ms_by_wallet[W] = 5000        # already proven to 5000

    # An older snapshot arrives: its fence_hi (3000) is BEHIND the watermark.
    install_fetch(e, [])

    proof = e.prove_interval_coherent(W, 1000, 2000, 3000)

    assert proof.get("hip3_coherent") is False, (
        "DEFECT 2 NOT FIXED: a snapshot older than the proven watermark was "
        "accepted (reason=%r)" % proof.get("reason"))
    assert proof.get("reason") == "stale_snapshot", (
        "expected reason='stale_snapshot', got %r" % proof.get("reason"))
    assert e.fetch_log == [], (
        "a stale snapshot should be rejected BEFORE spending a REST call; "
        "fetch windows seen: %s" % (e.fetch_log,))
    ok("A1.2", "stale snapshot is rejected and costs no REST call")


def test_a12_control_fresh_snapshot_accepted():
    e = engine()
    e.trusted_through_ms_by_wallet[W] = 5000
    install_fetch(e, [])
    proof = e.prove_interval_coherent(W, 5000, 5000, 6000)   # fence_hi ahead
    assert proof.get("hip3_coherent") is True, (
        "control failed: a snapshot AHEAD of the watermark must be accepted "
        "(reason=%r)" % proof.get("reason"))
    ok("A1.2c", "snapshot ahead of the watermark is accepted")


def test_a12_control_equal_boundary_accepted():
    """fence_hi == watermark is not stale -- it proves nothing new but is not a regression."""
    e = engine()
    e.trusted_through_ms_by_wallet[W] = 5000
    install_fetch(e, [])
    proof = e.prove_interval_coherent(W, 5000, 5000, 5000)
    assert proof.get("reason") != "stale_snapshot", (
        "equal boundary must not be treated as stale; got %r" % proof.get("reason"))
    ok("A1.2c2", "equal boundary is not rejected as stale")


# ============================================================ A1.3
def test_a13_rejected_bootstrap_leaves_no_cached_state():
    e = engine()

    # dex-a at 1000 and dex-b at 1500 -> fence_lo=1000, fence_hi=1500, a
    # genuinely non-empty span, so the coherence check actually RUNS.  (A
    # single-DEX wallet gives fence_lo == fence_hi, which is vacuously coherent
    # and would skip the very branch under test.)
    e.cold_bootstrap_dexes = lambda w: ["dex-a", "dex-b"]
    e._fetch_snapshots_only = lambda w, d: (
        {"BTC": {"szi": "1.0", "px": "50.0"}}, {"dex-a": 1000, "dex-b": 1500})

    # force the incoherent branch by returning a proof that fails
    e.prove_interval_coherent = lambda *a, **kw: {
        "ok": True, "complete": False, "hip3_coherent": False,
        "rows": [], "rest_pages": 1, "reason": "hip3_union_incoherent"}

    result = e.bootstrap_wallet_from_exchange(W, ts_ms=1500)

    assert e.wallet_active_dexes.get(W) in (None, {}), (
        "DEFECT 3 NOT FIXED: a rejected bootstrap still published "
        "wallet_active_dexes=%r -- downstream reads would treat an incoherent "
        "union as the wallet's active DEX set" % (e.wallet_active_dexes.get(W),))
    assert W not in e.last_exchange_snapshot_by_wallet, (
        "DEFECT 3 NOT FIXED: a rejected bootstrap still cached a snapshot for "
        "the wallet (%r)" % (e.last_exchange_snapshot_by_wallet.get(W),))
    assert W not in e.last_exchange_snapshot_ts_by_wallet, (
        "DEFECT 3 NOT FIXED: a rejected bootstrap still published a snapshot "
        "timestamp (%r)" % (e.last_exchange_snapshot_ts_by_wallet.get(W),))
    ok("A1.3", "rejected bootstrap leaves no authoritative-looking snapshot state")


def test_a13_control_proven_bootstrap_publishes():
    """Control: the staged state IS published once the proof succeeds.

    This one uses the REAL constructor (unlike the defect tests above), so the
    staging fix is exercised against a fully initialised engine rather than a
    hand-built bag of attributes -- otherwise a missing attribute could make
    the control fail for the wrong reason, or worse, pass vacuously.
    """
    # NOTE: do NOT unlink the engine's proof files to isolate this test.  They are
    # production proof history.  suite_tmpdir() (installed at import) already
    # redirects every one of those paths into a per-run temp directory, so the
    # real files are never opened, let alone deleted.
    e = engine_mod.EngineSSOT(wallets=[W])
    e.cold_bootstrap_dexes = lambda w: ["dex-a"]
    e._fetch_snapshots_only = lambda w, d: (
        {"BTC": {"szi": "2.0", "px": "50.0"}}, {"d": 1000})
    e.prove_interval_coherent = lambda *a, **kw: {
        "ok": True, "complete": True, "hip3_coherent": True,
        "rows": [], "rest_pages": 1, "reason": "proven"}
    e.set_exchange_baseline = lambda *a, **kw: None
    e.epoch_is_usable = lambda w: True
    e._open_epoch = lambda *a, **kw: None

    e.bootstrap_wallet_from_exchange(W, ts_ms=1500)

    assert W in e.last_exchange_snapshot_by_wallet, (
        "control failed: a PROVEN bootstrap must still publish its snapshot -- "
        "the staging fix must not swallow the success path")
    assert W in e.last_exchange_snapshot_ts_by_wallet, (
        "control failed: a proven bootstrap must publish its snapshot timestamp")
    assert e.last_exchange_snapshot_ts_by_wallet[W] == 1000, (
        "snapshot timestamp must be the SERVER fence, got %r"
        % e.last_exchange_snapshot_ts_by_wallet.get(W))
    ok("A1.3c", "proven bootstrap still publishes snapshot state (real constructor)")


def test_a12_realistic_stale_watermark_caller_path():
    """The REAL caller path, as named in the A1 REVIEW.

    The reviewer showed the guard was bypassable because the method clamped
    fence_hi_ms with max(start_ms, fence_hi_ms) BEFORE the stale check, and the
    real caller passes the trusted watermark as start_ms:

        trusted=5000, start=5000, actual server fence_hi=3000
             -> clamp moved the fence to 5000
             -> guard compared 5000 < 5000 and PASSED
             -> an obsolete snapshot was accepted

    This asserts the exact scenario: rejected, no REST spent, and no cursor or
    snapshot state touched.
    """
    e = engine()
    e.trusted_through_ms_by_wallet[W] = 5000
    install_fetch(e, [])

    # Mirror the caller: start_ms == the trusted watermark.
    proof = e.prove_interval_coherent(W, 5000, 2000, 3000)

    assert proof.get("reason") == "stale_snapshot", (
        "DEFECT 1 NOT FIXED: the stale guard was bypassed on the real caller "
        "path -- trusted=5000 start=5000 server fence_hi=3000 returned "
        "reason=%r. The fence must be validated BEFORE any adjustment, never "
        "manufactured forward with max(start_ms, fence_hi_ms)."
        % proof.get("reason"))
    assert proof.get("complete") is False and proof.get("hip3_coherent") is False, (
        "a bypassed stale snapshot must fail closed on BOTH flags, got %r"
        % proof)
    assert e.fetch_log == [], (
        "no REST request may be spent on an obsolete snapshot; windows seen: %s"
        % (e.fetch_log,))

    # No cursor / watermark / snapshot mutation.
    assert e.trusted_through_ms_by_wallet[W] == 5000, (
        "the watermark moved on a rejected stale snapshot: %r"
        % e.trusted_through_ms_by_wallet[W])
    assert W not in e.last_exchange_snapshot_by_wallet, (
        "a rejected stale snapshot published snapshot state")
    assert W not in e.last_exchange_snapshot_ts_by_wallet, (
        "a rejected stale snapshot published a snapshot timestamp")
    assert W not in e.wallet_active_dexes, (
        "a rejected stale snapshot published active DEX state")
    ok("A2.1", "stale watermark on the real caller path is rejected with no REST and no state change")


if __name__ == "__main__":
    print("=== GATE A1 regression tests ===")
    test_a11_fill_before_start_ms_inside_span_is_caught()
    test_a11_control_clean_span_is_coherent()
    test_a12_stale_snapshot_is_rejected()
    test_a12_control_fresh_snapshot_accepted()
    test_a12_control_equal_boundary_accepted()
    test_a13_rejected_bootstrap_leaves_no_cached_state()
    test_a13_control_proven_bootstrap_publishes()
    test_a12_realistic_stale_watermark_caller_path()
    print("")
    print("ALL GATE A1 TESTS PASSED (%d assertion-groups)" % len(PASSES))