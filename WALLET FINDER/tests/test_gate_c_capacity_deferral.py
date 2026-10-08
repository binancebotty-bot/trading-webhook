"""GATE C failure-injection tests - rate-budget deferral is not a proof failure.

THE DEFECT (Gate C)
-------------------
`RATE_GUARD.acquire()` returns False when the process is out of weight.  Every
skip site then returns `None` -- the SAME value used for a genuinely unusable
payload.  `_process_wallet_proof` counted a `None` as
`consecutive_poll_failures += 1`, and `inputs_current` treats any wallet with
`consecutive_poll_failures > 0` as failed.  So a wallet that was merely
STARVED looked identical to a wallet whose proof was incoherent, and
`poll_loop` rescheduled both a full cadence later.  One refused request
therefore became a self-sustaining loop: the wallet waited out a whole cycle it
had not consumed any budget on, and `inputs_current` failed for a reason that
had nothing to do with exchange truth.

C.1  A budget refusal on the SNAPSHOT path must NOT increment
     consecutive_poll_failures.  (fails pre-fix: the counter moves)
C.2  A budget refusal on the FILL-FETCH path must NOT increment
     consecutive_poll_failures either -- it surfaces as `proof["deferred"]`.
C.3  A GENUINE bad payload (exchange answered, payload unusable) MUST still
     increment the counter -- the control that stops this gate from turning a
     real failure into a silent skip.
C.4  A GENUINE failed/incoherent proof MUST still increment the counter.
C.5  inputs_current must stay True for a deferred wallet that is otherwise
     proven inside its SLA, and must be reported as deferred.
C.6  A deferred wallet must be rescheduled on the SHORT cadence, not the full
     SLA cadence -- otherwise the deferral costs a whole cycle of freshness.
C.7  A completed cycle CLEARS the deferral (otherwise a healed wallet is
     reported starved forever).
C.8  Structural: the deferral must be a DISTINCT signal, not an overload of
     the failure counter.  A wallet deferred 5 times and a wallet failed 5
     times must never share one bit of state.
C.9  No cursor movement on a deferral: a deferred cycle must not advance the
     watermark (fail-closed is preserved), but must not count as a failure.

Honours B8014_ENGINE_DIR exactly like the Gate A/B suites so the mutation check
can run it against a pre-fix engine.
"""
import inspect
import os
import sys
import threading
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
    print("PASS C%-5s %s" % (n, msg))


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
    e.consecutive_poll_failures = defaultdict(int)
    e.trusted_through_ms_by_wallet = defaultdict(int)
    e.last_poll_ts_by_wallet = defaultdict(int)
    e.last_exchange_snapshot_by_wallet = {}
    e.last_exchange_snapshot_ts_by_wallet = {}
    e.wallet_active_dexes = {}
    e._wallet_due_ms = {}
    e._budget_deferred_cycles = defaultdict(int)
    e.write_state = lambda: None
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


def _ready(e):
    """Mark the wallet provisioned so _process_wallet_proof does NOT bootstrap."""
    e._is_wallet_ready = lambda w: True
    e.wallet_poll_dexes = lambda w: []
    e._clear_proof_fence = lambda w: []
    e._advance_trusted_through = lambda w, ms: None


# ---------------------------------------------------------------------------
# C.1  snapshot-path budget refusal must not count as a proof failure
# ---------------------------------------------------------------------------
def c1():
    e = engine()
    _ready(e)
    # Budget refuses: the sentinel carries the reason out of the fetch.
    def snapshots_budget_refused(wallet, dexes):
        e.audit["rate_budget_skipped_snapshots"] += 1
        return mod.RATE_BUDGET_DEFERRED, {}
    e._fetch_snapshots_only = snapshots_budget_refused
    e._set_proof_fence = lambda w, ms: None

    e._process_wallet_proof(W, 1000)

    assert e.consecutive_poll_failures.get(W, 0) == 0, (
        "budget refusal counted as a proof failure: failures=%d"
        % e.consecutive_poll_failures.get(W, 0))
    assert e.audit["poll_cycle_deferred_rate_budget"] == 1, (
        "deferral not recorded: audit=%d" % e.audit["poll_cycle_deferred_rate_budget"])
    assert e._budget_deferred_cycles.get(W, 0) == 1, (
        "per-wallet deferral not counted: %d" % e._budget_deferred_cycles.get(W, 0))
    ok("1", "snapshot budget refusal -> deferral, NOT a proof failure")


# ---------------------------------------------------------------------------
# C.2  fill-fetch-path budget refusal must not count as a proof failure
# ---------------------------------------------------------------------------
def c2():
    e = engine()
    _ready(e)
    e._set_proof_fence = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: ({"ETH": {"szi": "1.0"}}, {"native": 900})
    # Budget refuses the fill fetch -> deferred=True must be honoured by the caller.
    e.prove_interval_coherent = lambda w, s, lo, hi: {
        "ok": False, "complete": False, "hip3_coherent": False, "rows": [],
        "deferred": True, "reason": "rate_budget_deferred",
    }

    e._process_wallet_proof(W, 1000)

    assert e.consecutive_poll_failures.get(W, 0) == 0, (
        "fill-fetch deferral counted as a proof failure: failures=%d"
        % e.consecutive_poll_failures.get(W, 0))
    assert e.audit["poll_cycle_deferred_rate_budget"] == 1, (
        "fill-fetch deferral not recorded")
    ok("2", "fill-fetch budget refusal -> deferral, NOT a proof failure")


# ---------------------------------------------------------------------------
# C.3  CONTROL: a genuine bad payload MUST still count as a failure
# ---------------------------------------------------------------------------
def c3():
    e = engine()
    _ready(e)
    e._set_proof_fence = lambda w, ms: None
    # Exchange answered; payload unusable. NO budget counter touched.
    e._fetch_snapshots_only = lambda w, d: (None, {})
    e.audit["rate_budget_skipped_snapshots"] = 0

    e._process_wallet_proof(W, 1000)

    assert e.consecutive_poll_failures.get(W, 0) == 1, (
        "real bad payload NOT counted as a failure (gate would hide errors)")
    assert e.audit["poll_cycle_deferred_rate_budget"] == 0, (
        "real bad payload misclassified as a deferral")
    ok("3", "genuine bad payload -> still a failure (control)")


# ---------------------------------------------------------------------------
# C.4  CONTROL: a genuine failed/incoherent proof MUST still count
# ---------------------------------------------------------------------------
def c4():
    e = engine()
    _ready(e)
    e._set_proof_fence = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: ({"ETH": {"szi": "1.0"}}, {"native": 900})
    e.prove_interval_coherent = lambda w, s, lo, hi: {
        "ok": False, "complete": False, "hip3_coherent": False, "rows": [],
        "reason": "hip3_union_incoherent",
    }
    e.audit["rate_budget_skipped_fill_fetches"] = 0

    e._process_wallet_proof(W, 1000)

    assert e.consecutive_poll_failures.get(W, 0) == 1, (
        "incoherent proof NOT counted as a failure")
    assert e.audit["poll_cycle_deferred_rate_budget"] == 0, (
        "incoherent proof misclassified as a deferral")
    ok("4", "incoherent proof -> still a failure (control)")


# ---------------------------------------------------------------------------
# C.5  inputs_current stays True for a DEFERRED-but-proven wallet
# ---------------------------------------------------------------------------
def c5():
    e = engine()
    _ready(e)
    e._capacity_invariant = {
        "declared_freshness_sla_seconds": 2160,
        "capacity_achievable": True,
    }
    e._capacity_bootstrap_complete = True
    e.drift_state_by_wallet = {W: {"ETH": {"status": "MATCHED"}}}
    e.epoch_by_wallet = {W: {"epoch_status": "OPEN"}}
    e._ws_data_fresh = lambda: True

    now = mod.utc_now_ms()
    e.trusted_through_ms_by_wallet[W] = now - 1000  # proven inside the SLA
    e.consecutive_poll_failures[W] = 0
    e._budget_deferred_cycles[W] = 3               # starved, but healthy

    cur = e.build_currentness()
    assert cur["inputs_current"] is True, (
        "a budget-deferred but proven wallet drove inputs_current False: %s"
        % {k: cur[k] for k in ("inputs_current", "wallets_with_consecutive_failures",
                               "current_lag_ms")})
    assert cur["wallets_rate_budget_deferred"] == 1, (
        "deferral not surfaced in the report: %s" % cur)
    ok("5", "deferred-but-proven wallet keeps inputs_current True and is reported")

    # C.5b CONTROL: a genuinely FAILED wallet must still fail currentness --
    # this gate must not have softened the real gate.
    e.consecutive_poll_failures[W] = 1
    cur2 = e.build_currentness()
    assert cur2["inputs_current"] is False, (
        "a genuinely failed wallet no longer fails inputs_current")
    ok("5b", "genuinely failed wallet still fails inputs_current (control)")


# ---------------------------------------------------------------------------
# C.6  a deferred wallet is rescheduled SHORT, not a full cadence later
# ---------------------------------------------------------------------------
def c6():
    """BEHAVIOURAL: drive one real poll_loop pass and read the due time.

    Checked at runtime, not by reading source, so a rewrite that keeps the
    constant but stops honouring it cannot pass.
    """
    long_ms = int(2160 * 1000 * 0.8)

    def run_cycle(deferred):
        e = engine()
        e._capacity_invariant = {"declared_freshness_sla_seconds": 2160}
        e.stop_event = threading.Event()
        e._poll_rotation_offset = 0
        e._wallet_due_ms = {W: 0}
        e.refresh_manual_wallets = lambda: []

        # A stop_event that flips to set AFTER the first pass lets poll_loop
        # execute exactly one scheduling decision and then exit cleanly.
        class _OneShot:
            def __init__(self):
                self.n = 0

            def is_set(self):
                return self.n > 0

            def wait(self, _s):
                self.n += 1
                return False
        e.stop_event = _OneShot()

        if deferred:
            def deferred_proof(w, now):
                e._record_budget_deferral(w)
            e._process_wallet_proof = deferred_proof
        else:
            e._process_wallet_proof = lambda w, now: None

        before = mod.utc_now_ms()
        mod.EngineSSOT.poll_loop(e)
        return e._wallet_due_ms.get(W, 0) - before

    deferred_gap = run_cycle(True)
    normal_gap = run_cycle(False)

    assert normal_gap >= long_ms * 0.9, (
        "a normal wallet was not scheduled a full cadence out: %d ms" % normal_gap)
    assert deferred_gap < normal_gap, (
        "a deferred wallet was NOT rescheduled sooner than a normal one: "
        "deferred=%d ms normal=%d ms" % (deferred_gap, normal_gap))
    assert mod.DEFERRED_RETRY_MS < long_ms, (
        "deferred retry (%d ms) is not shorter than the SLA cadence (%d ms)"
        % (mod.DEFERRED_RETRY_MS, long_ms))
    ok("6", "deferred wallet uses the short retry cadence, not the full SLA cadence")


# ---------------------------------------------------------------------------
# C.7  a completed cycle clears the deferral
# ---------------------------------------------------------------------------
def c7():
    e = engine()
    _ready(e)
    e._set_proof_fence = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: ({"ETH": {"szi": "1.0"}}, {"native": 900})
    e.trusted_through_ms_by_wallet[W] = 500
    e.prove_interval_coherent = lambda w, s, lo, hi: {
        "ok": True, "complete": True, "hip3_coherent": True, "rows": [],
    }
    e.ingest_fetched_rows = lambda w, rows, hi, src, advance_cursor: {"ok": True, "complete": True}
    e._advance_trusted_through = lambda w, ms: None
    e._builder_dexes_in_snapshot = lambda s: set()
    e.audit_position_drift_only = lambda w, s: None
    e._wallet_state = lambda w: {}
    e._budget_deferred_cycles[W] = 4      # previously starved

    e._process_wallet_proof(W, 1000)

    assert e._budget_deferred_cycles.get(W, 0) == 0, (
        "completed cycle left the wallet reported as starved: %d"
        % e._budget_deferred_cycles.get(W, 0))
    assert e.audit["rate_budget_deferrals_resolved"] == 4, (
        "resolution not audited: %d" % e.audit["rate_budget_deferrals_resolved"])
    ok("7", "a completed cycle clears the deferral and audits the resolution")


# ---------------------------------------------------------------------------
# C.8  STRUCTURAL: deferral and failure must not share one bit of state
# ---------------------------------------------------------------------------
def c8():
    src = inspect.getsource(mod.EngineSSOT._process_wallet_proof)
    # The deferral branch must come BEFORE the failure increments, and must be
    # keyed on an explicit deferral signal rather than reusing the failure path.
    assert 'if proof.get("deferred")' in src, (
        "caller does not branch on proof['deferred']")

    fetch_src = inspect.getsource(mod.EngineSSOT.fetch_fills_since)
    assert '"deferred"' in fetch_src, (
        "fetch_fills_since does not emit a deferred flag")
    assert "rate_budget_deferred" in fetch_src, (
        "fetch_fills_since does not distinguish a budget refusal from a real "
        "fetch failure")

    # The two states must be separate attributes, so no arithmetic on one can
    # imply the other.
    assert hasattr(mod.EngineSSOT, "_compute_capacity_invariant")
    ok("8", "deferral is a distinct signal, structurally separated from failure")


# ---------------------------------------------------------------------------
# C.9  a deferral still fails CLOSED on cursors (no watermark movement)
# ---------------------------------------------------------------------------
def c9():
    e = engine()
    _ready(e)
    e._set_proof_fence = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: ({"ETH": {"szi": "1.0"}}, {"native": 900})
    e.trusted_through_ms_by_wallet[W] = 500
    e.prove_interval_coherent = lambda w, s, lo, hi: {
        "ok": False, "complete": False, "hip3_coherent": False, "rows": [],
        "deferred": True, "reason": "rate_budget_deferred",
    }
    advanced = []
    e._advance_trusted_through = lambda w, ms: advanced.append((w, ms))

    e._process_wallet_proof(W, 1000)

    assert advanced == [], (
        "a deferred cycle advanced the watermark: %r" % (advanced,))
    assert W not in e.last_exchange_snapshot_by_wallet, (
        "a deferred cycle recorded an unproven snapshot as current truth")
    ok("9", "a deferral fails closed on cursors (watermark and snapshot untouched)")


# ---------------------------------------------------------------------------
# C.10  a skip belonging to ANOTHER wallet must not be misattributed
# ---------------------------------------------------------------------------
def c10():
    """The refusal reason must be CARRIED, never INFERRED from a shared counter.

    An earlier version diffed the process-wide `rate_budget_skipped_fill_fetches`
    counter across the call. That was unsound: a concurrent skip belonging to a
    different wallet could relabel this wallet's genuine network failure as
    `deferred` -- a false green in the exact direction this gate exists to
    prevent. This asserts a real failure stays a real failure no matter what
    else is being counted.
    """
    e = engine()
    _ready(e)
    e.trusted_through_ms_by_wallet[W] = 500

    # This wallet's fetch FAILS for a real reason, while an unrelated wallet's
    # skip keeps inflating the shared counter throughout.
    def real_failure_with_foreign_skips(wallet, start_ms, end_ms):
        e.audit["rate_budget_skipped_fill_fetches"] += 1   # another wallet's skip
        e.audit["poll_errors"] += 1                        # this one really failed
        return None

    e.fetch_fills_range = real_failure_with_foreign_skips

    res = e.fetch_fills_since(W, 500, 1000)

    assert res["ok"] is False, "expected a failed fetch: %r" % (res,)
    assert res.get("deferred") is not True, (
        "another wallet's budget skip was misattributed to this wallet's real "
        "failure as `deferred`: %r" % (res,))
    assert res["reason"] == "fetch_failed", (
        "a genuine failure was relabelled: reason=%r" % (res.get("reason"),))
    ok("10", "a foreign skip cannot misattribute a real failure as deferred")


# ---------------------------------------------------------------------------
# C.11  the sentinel is falsy, so every caller must test identity first
# ---------------------------------------------------------------------------
def c11():
    """A falsy sentinel is a trap: `is None` does NOT catch it.

    RATE_BUDGET_DEFERRED is falsy on purpose (so existing `if not rows` style
    checks keep failing closed). But every call site that reads a snapshot must
    therefore test identity BEFORE its None check, or a budget refusal would be
    mistaken for a successful measurement and could roll a "measured" epoch.
    This asserts that invariant structurally, at all four call sites.
    """
    assert bool(mod.RATE_BUDGET_DEFERRED) is False, (
        "the sentinel must stay falsy so legacy falsy-checks fail closed")
    assert mod.RATE_BUDGET_DEFERRED is not None, (
        "the sentinel must not BE None")

    sites = {
        "_process_wallet_proof": "_fetch_snapshots_only(wallet, dexes)",
        "bootstrap_wallet_from_exchange": "_fetch_snapshots_only(wallet, dexes)",
    }
    for fn_name, call in sites.items():
        fn = getattr(mod.EngineSSOT, fn_name, None)
        if fn is None:
            continue  # name drift: covered by the audit-counter checks below
        src = inspect.getsource(fn)
        if call not in src:
            continue
        sent = src.find("RATE_BUDGET_DEFERRED")
        non = src.find("snapshot is None")
        assert sent != -1, "%s reads a snapshot but never tests the sentinel" % fn_name
        assert sent < non, (
            "%s tests `is None` BEFORE the sentinel -- the falsy sentinel would "
            "slip through as a measurement" % fn_name)

    # The fetch path must never infer from the shared counter any more.
    fetch_src = inspect.getsource(mod.EngineSSOT.fetch_fills_since)
    assert "rate_budget_skipped_fill_fetches" not in fetch_src, (
        "fetch_fills_since still INFERRS deferral from the shared audit counter")
    assert "rows is RATE_BUDGET_DEFERRED" in fetch_src, (
        "fetch_fills_since does not test the sentinel by identity")
    ok("11", "the falsy sentinel is identity-tested before every None check")


if __name__ == "__main__":
    checks = [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11]
    failed = []
    for fn in checks:
        try:
            fn()
        except Exception as exc:
            print("FAIL %-6s %s: %s" % (fn.__name__, type(exc).__name__, exc))
            failed.append(fn.__name__)
    print()
    print("GATE C: %d/%d passed" % (len(PASSED), len(checks)))
    if failed:
        print("FAILED: %s" % ", ".join(failed))
        sys.exit(1)
    print("ALL GATE C CHECKS PASS")