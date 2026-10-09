"""GATE C1 + cold-bootstrap retry test.

C1: a budget refusal on the snapshot path (including the COLD-BOOTSTRAP path,
which has no prior snapshot) must NOT create a "measured" epoch and must NOT
increment consecutive_poll_failures.  The wallet must be retried on the SHORT
cadence, not the full SLA.

Verified by behaviour, not by reading source.  Honours B8014_ENGINE_DIR.
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

class _Queue:
    def __init__(self):
        self.items = []; self._lock = threading.Lock()
    def put_nowait(self, item):
        with self._lock: self.items.append(item)
    def get_nowait(self):
        with self._lock: return self.items.pop(0)
    def qsize(self):
        return len(self.items)

def engine():
    e = mod.EngineSSOT.__new__(mod.EngineSSOT)
    e.audit = defaultdict(int)
    e.wallets = [W]; e.positions = {}; e.wallets_raw = {}; e.mark_prices = {}
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
    e._capacity_bootstrap_complete = True
    e._capacity_invariant = {"declared_freshness_sla_seconds": mod.SSOT_PROOF_SLA_SECONDS, "capacity_achievable": True}
    e.write_state = lambda: None
    e.cold_bootstrap_dexes = lambda w: ["ETH"]; e.wallet_poll_dexes = lambda w: []
    e._set_proof_fence = lambda w, ms: None
    return e


def test_c1_snapshot_budget_refusal_not_a_failure():
    e = engine()
    e._is_wallet_ready = lambda w: True
    e._clear_proof_fence = lambda w: []
    e._advance_trusted_through = lambda w, ms: None
    e._fetch_snapshots_only = lambda w, d: (mod.RATE_BUDGET_DEFERRED, {})
    e._process_wallet_proof(W, 1000)
    assert e.consecutive_poll_failures.get(W, 0) == 0, "budget refusal counted as proof failure: %d" % e.consecutive_poll_failures.get(W, 0)
    assert e.audit["poll_cycle_deferred_rate_budget"] == 1, "deferral not audited: %d" % e.audit["poll_cycle_deferred_rate_budget"]
    assert e._budget_deferred_cycles.get(W, 0) == 1, "per-wallet deferral not counted: %d" % e._budget_deferred_cycles.get(W, 0)


def test_cold_bootstrap_deferral_is_fail_closed_and_not_a_failure():
    e = engine()
    e._is_wallet_ready = lambda w: False
    e.trusted_through_ms_by_wallet = defaultdict(int)
    e._clear_proof_fence = lambda w: []
    e._fetch_snapshots_only = lambda w, d: (mod.RATE_BUDGET_DEFERRED, {})
    e.cold_bootstrap_dexes = lambda w: ["ETH"]
    result = e.bootstrap_wallet_from_exchange(W, ts_ms=2000)
    assert e.wallet_runtime.get(W, {}).get("ready") is False, "cold-bootstrap deferral left READY=True"
    assert W not in e.last_exchange_snapshot_by_wallet, "cold-bootstrap deferral recorded an unproven snapshot"
    assert e.consecutive_poll_failures.get(W, 0) == 0, "cold-bootstrap deferral counted as proof failure: %d" % e.consecutive_poll_failures.get(W, 0)
    assert e.audit["bootstrap_snapshot_deferred_rate_budget"] == 1, "cold-bootstrap deferral not audited distinctly: %r" % dict(e.audit)
    assert e.audit.get("bootstrap_snapshot_failed", 0) == 0, "cold-bootstrap deferral misclassified as real failure"
    assert result in (None, False), "bootstrap did not return a clear no-epoch signal: %r" % (result,)


def test_cold_bootstrap_genuine_failure_still_distinct_from_deferral():
    e = engine()
    e._is_wallet_ready = lambda w: False
    e._clear_proof_fence = lambda w: []
    e._fetch_snapshots_only = lambda w, d: (None, {})
    e.cold_bootstrap_dexes = lambda w: ["ETH"]
    e.bootstrap_wallet_from_exchange(W, ts_ms=2000)
    assert e.audit.get("bootstrap_snapshot_failed", 0) == 1, "real cold-bootstrap failure not audited: %r" % dict(e.audit)
    assert e.audit.get("bootstrap_snapshot_deferred_rate_budget", 0) == 0, "real failure misclassified as a deferral"
    assert e.wallet_runtime.get(W, {}).get("ready") is False


if __name__ == "__main__":
    tests = [
        ("C1 snapshot-path budget refusal", test_c1_snapshot_budget_refusal_not_a_failure),
        ("cold-bootstrap retry deferral (fail-closed)", test_cold_bootstrap_deferral_is_fail_closed_and_not_a_failure),
        ("cold-bootstrap genuine failure control", test_cold_bootstrap_genuine_failure_still_distinct_from_deferral),
    ]
    failed = []
    for name, fn in tests:
        try:
            fn(); print("PASS  %s" % name)
        except Exception as exc:
            print("FAIL  %s: %s: %s" % (name, type(exc).__name__, exc)); failed.append(name)
    print()
    print("GATE C1 + COOL-BOOTSTRAP RETRY: %d/%d passed" % (len(tests) - len(failed), len(tests)))
    if failed:
        print("FAILED: %s" % ", ".join(failed)); sys.exit(1)
    print("ALL GATE C1 CHECKS PASS")
