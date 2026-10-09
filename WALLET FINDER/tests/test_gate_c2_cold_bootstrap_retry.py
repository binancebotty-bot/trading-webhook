"""GATE C2 - the cold-bootstrap deferral must actually retry at 60s.

THE DEFECT THIS CAUGHT
----------------------
Gate C added a per-wallet deferral counter so poll_loop could reschedule a
deferred wallet on DEFERRED_RETRY_MS instead of the full SLA cadence.  But the
counter was only bumped inside `_process_wallet_proof`.  An UNREADY wallet
reaches its FIRST snapshot fetch through `bootstrap_wallet_from_exchange`, which
bumped its own audit counter and never called `_record_budget_deferral`.  So for
every cold-start wallet the counter stayed 0, poll_loop's `deferred_now` test
read False, and the wallet was rescheduled a FULL cadence (1,728,000 ms) later
instead of 60s.  The starvation loop survived cold bootstrap -- precisely the
population that needs the retry most, since a cold start has no watermark to fall
back on.

This is a BEHAVIOURAL one-shot poll_loop test: it drives a real poll_loop pass
and reads the due time back.  Checked at runtime, not by reading source, so a
rewrite that keeps `_record_budget_deferral` but stops honouring it cannot pass.

Honours B8014_ENGINE_DIR exactly like the Gate A/B/C suites.
"""
import os, sys, threading
from collections import defaultdict, deque
from pathlib import Path

_ROOT = Path(r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER")
_ENGINE_DIR = os.environ.get("B8014_ENGINE_DIR")
if _ENGINE_DIR:
    sys.path.insert(0, _ENGINE_DIR)
    import HL_Copy_Engine_SSOT as mod
    assert Path(mod.__file__).resolve().parent == Path(_ENGINE_DIR).resolve(), (
        "B8014_ENGINE_DIR=%s but engine resolved to %s" % (_ENGINE_DIR, mod.__file__))
else:
    sys.path.insert(0, str(_ROOT))
    import HL_Copy_Engine_SSOT as mod

W = "0x069f458978a51ef6a9e2f6b6eb1fe2f39271c5a2"
PASSED = []


def ok(n, msg):
    PASSED.append(n)
    print("PASS C2-%-4s %s" % (n, msg))


class _Queue:
    def __init__(self):
        self.items = []; self._lock = threading.Lock()
    def put_nowait(self, item):
        with self._lock: self.items.append(item)
    def get_nowait(self):
        with self._lock: return self.items.pop(0)
    def qsize(self):
        return len(self.items)


class _OneShot:
    """Flips to set only AFTER the first wait(), so poll_loop performs exactly
    one scheduling decision and then exits cleanly."""
    def __init__(self):
        self.n = 0
    def is_set(self):
        return self.n > 0
    def wait(self, _s):
        self.n += 1
        return False


def engine():
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.wallets = [W]
    e.positions = {}; e.wallets_raw = {}; e.mark_prices = {}
    e.shard_status = {}; e.seen_ids = deque(maxlen=200000); e.seen_set = set()
    e.seen_lock = threading.Lock(); e.state_lock = threading.Lock()
    e.event_queue = _Queue(); e._startup_fill_wallets = set()
    e.wallet_runtime = {}; e.stop_event = threading.Event()
    e.consecutive_poll_failures = defaultdict(int)
    e.trusted_through_ms_by_wallet = defaultdict(int)
    e.last_poll_ts_by_wallet = defaultdict(int)
    e.last_exchange_snapshot_by_wallet = {}; e.last_exchange_snapshot_ts_by_wallet = {}
    e.wallet_active_dexes = {}; e._wallet_due_ms = {}
    e._budget_deferred_cycles = defaultdict(int)
    e.wallet_builder_dexes = {W: set()}
    e._capacity_bootstrap_complete = True
    e._capacity_invariant = {"declared_freshness_sla_seconds": mod.SSOT_PROOF_SLA_SECONDS}
    e.write_state = lambda: None
    e._poll_rotation_offset = 0
    e.refresh_manual_wallets = lambda: []
    return e


def _cold_bootstrap_engine():
    """An UNREADY wallet whose first snapshot fetch is refused by the budget."""
    e = engine()
    e._is_wallet_ready = lambda w: False          # cold: needs bootstrap
    e.cold_bootstrap_dexes = lambda w: ["ETH"]
    e.wallet_poll_dexes = lambda w: []
    e._clear_proof_fence = lambda w: []
    e._set_proof_fence = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: (mod.RATE_BUDGET_DEFERRED, {})
    e._wallet_due_ms = {W: 0}                      # due immediately
    return e


def _run_one_poll_pass(e):
    e.stop_event = _OneShot()
    before = mod.utc_now_ms()
    mod.EngineSSOT.poll_loop(e)
    return e._wallet_due_ms.get(W, 0) - before


def c2_1_cold_bootstrap_deferral_retries_at_60s():
    """The defect itself: an unready wallet refused at cold bootstrap must be
    rescheduled at DEFERRED_RETRY_MS, NOT a full SLA cadence later."""
    e = _cold_bootstrap_engine()
    gap = _run_one_poll_pass(e)

    assert e.consecutive_poll_failures.get(W, 0) == 0, (
        "cold-bootstrap deferral counted as a proof failure: %d"
        % e.consecutive_poll_failures.get(W, 0))
    assert e.wallet_runtime.get(W, {}).get("ready") is False, (
        "cold-bootstrap deferral opened a fake measured epoch (READY=True)")
    assert e._budget_deferred_cycles.get(W, 0) == 1, (
        "cold-bootstrap deferral never reached the per-wallet counter: %d"
        % e._budget_deferred_cycles.get(W, 0))

    sla_ms = int(mod.SSOT_PROOF_SLA_SECONDS * 1000)
    assert gap < sla_ms * 0.5, (
        "a cold-bootstrap deferral waited a near-full SLA cadence: %d ms "
        "(SLA=%d ms). The starvation loop survives cold bootstrap." % (gap, sla_ms))
    assert mod.DEFERRED_RETRY_MS <= gap <= mod.DEFERRED_RETRY_MS + 5000, (
        "expected a ~%d ms retry, measured %d ms" % (mod.DEFERRED_RETRY_MS, gap))
    ok("1", "cold-bootstrap deferral retries at ~%ds, not a full SLA cadence"
       % (mod.DEFERRED_RETRY_MS // 1000))


def c2_2_control_real_cold_bootstrap_failure_uses_full_cadence():
    """CONTROL: a genuine cold-bootstrap failure is NOT a deferral, so the wallet
    keeps the normal full cadence. If this were rescheduled at 60s we would be
    hammering the exchange on a real error."""
    e = engine()
    e._is_wallet_ready = lambda w: False
    e.cold_bootstrap_dexes = lambda w: ["ETH"]
    e.wallet_poll_dexes = lambda w: []
    e._clear_proof_fence = lambda w: []
    e._set_proof_fence = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: (None, {})     # real failure, not the sentinel
    e._wallet_due_ms = {W: 0}

    gap = _run_one_poll_pass(e)

    assert e._budget_deferred_cycles.get(W, 0) == 0, (
        "a real failure was recorded as a budget deferral: %d"
        % e._budget_deferred_cycles.get(W, 0))
    assert gap > mod.DEFERRED_RETRY_MS * 10, (
        "a real failure was rescheduled on the short deferral cadence (%d ms): "
        "that would hammer the exchange on a genuine error" % gap)
    ok("2", "genuine cold-bootstrap failure keeps the full cadence (control)")


def c2_3_ready_wallet_deferral_still_short():
    """REGRESSION GUARD: the original Gate C behaviour for an ALREADY-READY wallet
    must be unchanged by the C2 fix."""
    e = engine()
    e._is_wallet_ready = lambda w: True
    e._clear_proof_fence = lambda w: []
    e._advance_trusted_through = lambda w, ms: None
    e._set_proof_fence = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: (mod.RATE_BUDGET_DEFERRED, {})
    e._wallet_due_ms = {W: 0}

    gap = _run_one_poll_pass(e)

    assert e._budget_deferred_cycles.get(W, 0) == 1, (
        "ready-wallet deferral not counted: %d" % e._budget_deferred_cycles.get(W, 0))
    assert mod.DEFERRED_RETRY_MS <= gap <= mod.DEFERRED_RETRY_MS + 5000, (
        "ready-wallet deferral no longer retries at %d ms (measured %d)"
        % (mod.DEFERRED_RETRY_MS, gap))
    ok("3", "already-ready wallet deferral still retries at ~%ds (no regression)"
       % (mod.DEFERRED_RETRY_MS // 1000))


if __name__ == "__main__":
    checks = [c2_1_cold_bootstrap_deferral_retries_at_60s,
              c2_2_control_real_cold_bootstrap_failure_uses_full_cadence,
              c2_3_ready_wallet_deferral_still_short]
    failed = []
    for fn in checks:
        try:
            fn()
        except Exception as exc:
            print("FAIL %-58s %s: %s" % (fn.__name__, type(exc).__name__, exc))
            failed.append(fn.__name__)
    print()
    print("GATE C2: %d/%d passed" % (len(PASSED), len(checks)))
    if failed:
        print("FAILED: %s" % ", ".join(failed))
        sys.exit(1)
    print("ALL GATE C2 CHECKS PASS")
