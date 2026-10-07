"""Regression suite for the Wallet Proof epoch repair (26 tests).

Runs against a SCRATCH copy of the output dir with mocked network calls.
Never touches live hl_copy_output or the running engine.
"""
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

ENGINE_DIR = Path(__file__).resolve().parents[1]  # .../WALLET FINDER
SCRATCH = Path(__file__).resolve().parent / "_scratch_data"
if SCRATCH.exists():
    shutil.rmtree(SCRATCH, ignore_errors=True)
SCRATCH.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(ENGINE_DIR))
spec = importlib.util.spec_from_file_location("hl_engine", ENGINE_DIR / "HL_Copy_Engine_SSOT.py")
eng = importlib.util.module_from_spec(spec)
sys.modules["hl_engine"] = eng  # required for @dataclass to resolve cls.__module__
spec.loader.exec_module(eng)

# --- redirect all engine IO into scratch ---------------------------------
OUT = SCRATCH / "hl_copy_output"
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "snapshots").mkdir(exist_ok=True)
eng.OUTPUT_DIR = OUT
eng.SNAP_DIR = OUT / "snapshots"
eng.RAW_FILLS_CSV = OUT / "raw_live_fills.csv"
eng.ENGINE_TRUTH_JSON = OUT / "engine_truth.json"
eng.LIVE_STATE_JSON = OUT / "live_state.json"
eng.LIVE_METRICS_CSV = OUT / "live_wallet_metrics.csv"
eng.LOG_FILE = OUT / "test.log"
eng.BASELINE_JSON = OUT / "exchange_baselines.json"
eng.PROOF_EPOCH_JSON = OUT / "proof_epoch.json"
eng.RECOVERY_GATE_JSON = OUT / "recovery_gate.json"
eng.PROOF_WATERMARK_JSON = OUT / "proof_watermark.json"
eng.MANUAL_WALLETS_FILE = SCRATCH / "manual_wallets.txt"
eng.LOCK_FILE = SCRATCH / "engine_ssot.lock"
eng.RATE_GUARD = None  # no rate limiting in tests
eng.MONITOR_START_MS = 0

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail else ""))


W = "0x" + "a" * 40
W2 = "0x" + "b" * 40

# =====================================================================
# TEST 1: provenance labelling
# =====================================================================
check("T1a poll -> POLL", eng.normalise_recording_method(None, source="poll") == "POLL")
check("T1b ws -> WS_CAPTURED", eng.normalise_recording_method(None, source="ws") == "WS_CAPTURED")
check("T1c snapshot -> REBUILD", eng.normalise_recording_method(None, source="poll", is_snapshot=True) == "REBUILD")
check("T1d poll eligible, rebuild not, for execution delta",
      eng.recording_contributes_to_execution_delta("POLL") is True
      and eng.recording_contributes_to_execution_delta("REBUILD") is False)

# =====================================================================
# TEST 2: fetch completeness - mocked API
# =====================================================================
class FakeAPI:
    """Mock userFillsByTime with a hard 2000 cap, ascending, inclusive, ties."""
    def __init__(self, fills):
        self.fills = sorted(fills, key=lambda f: f["time"])
        self.calls = []
        self.cap = eng.API_FILLS_PAGE_CAP
        self.snapshots = {}

    def fetch(self, wallet, start_ms, end_ms):
        self.calls.append((start_ms, end_ms))
        rows = [f for f in self.fills if start_ms <= f["time"] <= end_ms]
        return rows[: self.cap]


def make_engine(wallets, api=None):
    e = eng.EngineSSOT(wallets=list(wallets))
    e.fetch_exchange_positions = lambda w: (api.snapshots.get(w, {}) if api else {})
    if api is not None:
        e.fetch_fills_range = lambda w, s, en: api.fetch(w, s, en)
    return e


def fill(wallet, coin, ts, sz, tid):
    return {"coin": coin, "time": ts, "sz": str(abs(sz)), "px": "10.0",
            "side": "B" if sz > 0 else "A", "startPosition": "0", "dir": "Open Long",
            "closedPnl": "0", "fee": "0.01", "tid": tid, "oid": tid, "hash": "h" + str(tid)}


now = eng.utc_now_ms()
base = now - 4000

# T2a: >2000 rows in a window (2 pages) must be PROVEN COMPLETE
big = [fill(W, "BTC", base + i, 1.0, 100000 + i) for i in range(2500)]
api = FakeAPI(big)
api.snapshots = {W: {"BTC": {"signed_size": 2500.0, "entry_price": 10.0}}}
e = make_engine([W], api)
e.epoch_by_wallet[W] = {"epoch_id": "t", "baseline_ts_ms": base - 1, "baseline_position": {},
                        "baseline_provenance": "test", "epoch_status": "OPEN"}
res = e.fetch_fills_since(W, base - 1, now)
check("T2a >2000 rows proven complete", res["complete"] and res["reason"] == "complete",
      f"rows={len(res['rows'])} reason={res['reason']}")
check("T2a all 2500 unique rows returned", len({r['tid'] for r in res['rows']}) == 2500,
      f"unique={len({r['tid'] for r in res['rows']})}")

# T2b: tied timestamps at the cap boundary must NOT be dropped
ties = [fill(W2, "ETH", base + (i // 10), 1.0, 200000 + i) for i in range(2500)]  # 10 fills/ts
api2 = FakeAPI(ties)
api2.snapshots = {W2: {"ETH": {"signed_size": 2500.0, "entry_price": 10.0}}}
e2 = make_engine([W2], api2)
e2.epoch_by_wallet[W2] = {"epoch_id": "t2", "baseline_ts_ms": base - 1, "baseline_position": {},
                          "baseline_provenance": "test", "epoch_status": "OPEN"}
res2 = e2.fetch_fills_since(W2, base - 1, now)
check("T2b tied-timestamp fills not dropped", len({r['tid'] for r in res2['rows']}) == 2500,
      f"unique={len({r['tid'] for r in res2['rows']})} complete={res2['complete']}")

# T2c: saturated boundary -> completeness UNKNOWN (fail closed)
class SaturatedAPI(FakeAPI):
    def fetch(self, wallet, start_ms, end_ms):
        return [fill(wallet, "SOL", start_ms, 1.0, 300000 + i) for i in range(self.cap)]

sat = SaturatedAPI([])
e3 = make_engine([W], sat)
res3 = e3.fetch_fills_since(W, base, now)
check("T2c saturated boundary fails closed",
      res3["complete"] is False and res3["reason"] == "saturated_boundary",
      f"reason={res3['reason']}")

# T2d: transient failure -> ok=False
class FailAPI(FakeAPI):
    def fetch(self, wallet, start_ms, end_ms):
        return None

fapi = FailAPI([])
e4 = make_engine([W], fapi)
res4 = e4.fetch_fills_since(W, base, now)
check("T2d transient failure fails closed", res4["ok"] is False and res4["reason"] == "fetch_failed")

# =====================================================================
# TEST 3: epoch model
# =====================================================================
apiX = FakeAPI([])
apiX.snapshots = {W: {"BTC": {"signed_size": 5.0, "entry_price": 10.0}}}
e5 = make_engine([W], apiX)
e5._mark_wallet_ready(W, "test", now)
e5.audit_position_drift_only(W, apiX.snapshots[W])
st = e5.drift_state_by_wallet.get(W, {}).get("BTC", {}).get("status")
check("T3a no open epoch -> DRIFT_UNRESOLVED", st == "DRIFT_UNRESOLVED", f"status={st}")

e5._open_epoch(W, {"BTC": {"signed_size": 5.0, "entry_price": 10.0}}, ts_ms=now, reason="test")
e5.audit_position_drift_only(W, apiX.snapshots[W])
st = e5.drift_state_by_wallet.get(W, {}).get("BTC", {}).get("status")
check("T3b epoch baseline matches -> CLEAN", st == "CLEAN", f"status={st}")

apiY = FakeAPI([])
apiY.snapshots = {W: {"BTC": {"signed_size": 0.0, "entry_price": 10.0}}}
e6 = make_engine([W], apiY)
e6._mark_wallet_ready(W, "test", now)
e6._open_epoch(W, {"BTC": {"signed_size": 5.0, "entry_price": 10.0}}, ts_ms=now, reason="test")
e6.audit_position_drift_only(W, apiY.snapshots[W])
st = e6.drift_state_by_wallet.get(W, {}).get("BTC", {}).get("status")
check("T3c snapshot change -> not CLEAN", st in {"DRIFT_UNRESOLVED", "UNRESOLVED_ESCALATED", "DRIFT_DETECTED"},
      f"status={st}")

e6._close_epoch_unresolved(W, "test_incomplete")
e6._open_epoch(W, {"BTC": {"signed_size": 0.0, "entry_price": 10.0}}, ts_ms=now + 1, reason="fresh")
hist = e6.epoch_by_wallet[W].get("epoch_history", [])
check("T3d closed epoch retained as UNRESOLVED",
      any(h.get("epoch_status") == "UNRESOLVED" for h in hist),
      f"history_statuses={[h.get('epoch_status') for h in hist]}")

# =====================================================================
# TEST 4: trusted-through semantics
# =====================================================================
apiQ = FakeAPI([])
e7 = make_engine([W], apiQ)
e7._mark_wallet_ready(W, "test", now)
e7._open_epoch(W, {}, ts_ms=now - 10_000, reason="test")
t_end = now
r = e7.ingest_real_fills_window(W, now - 5000, t_end, "poll", advance_cursor=True)
check("T4a complete empty poll advances trusted-through",
      r["complete"] and e7.trusted_through_ms_by_wallet.get(W) == t_end,
      f"tt={e7.trusted_through_ms_by_wallet.get(W)} end={t_end}")

class IncompleteAPI(FakeAPI):
    def fetch(self, wallet, start_ms, end_ms):
        return [fill(wallet, "X", start_ms, 1.0, 400000 + i) for i in range(self.cap)]

iapi = IncompleteAPI([])
e8 = make_engine([W], iapi)
e8._mark_wallet_ready(W, "test", now)
e8._open_epoch(W, {}, ts_ms=now - 10_000, reason="test")
before = e8.trusted_through_ms_by_wallet.get(W, 0)
r8 = e8.ingest_real_fills_window(W, now - 5000, now, "poll", advance_cursor=True)
check("T4b incomplete poll does NOT advance trusted-through",
      (not r8["complete"]) and e8.trusted_through_ms_by_wallet.get(W, 0) == before,
      f"complete={r8['complete']} tt={e8.trusted_through_ms_by_wallet.get(W,0)}")

# =====================================================================
# TEST 5: currentness
# =====================================================================
e9 = make_engine([W, W2], apiQ)
e9._mark_wallet_ready(W, "t", now)
e9._mark_wallet_ready(W2, "t", now)
e9.trusted_through_ms_by_wallet = {W: now - 1000, W2: now - 900000}
cn = e9.build_currentness()
check("T5a global = MIN (lagging wallet governs)",
      cn["global_trusted_through_ms"] == now - 900000,
      f"global={cn['global_trusted_through_ms']} expected={now-900000}")
check("T5b lagging wallet -> inputs_current False", cn["inputs_current"] is False)
check("T5c fast wallet does not mask lagging one", cn["global_trusted_through_ms"] != now - 1000)

e9.drift_state_by_wallet = {W: {"BTC": {"status": "DRIFT_UNRESOLVED"}}}
e9.trusted_through_ms_by_wallet = {W: now, W2: now}
cn2 = e9.build_currentness()
check("T5d inputs_current True while state_trustworthy False",
      cn2["inputs_current"] is True and cn2["state_trustworthy"] is False,
      f"inputs={cn2['inputs_current']} trustworthy={cn2['state_trustworthy']}")

# =====================================================================
# TEST 6: recovery escalation
# =====================================================================
apiR = FakeAPI([])
apiR.snapshots = {W: {"BTC": {"signed_size": 0.0}}}
e10 = make_engine([W], apiR)
e10._mark_wallet_ready(W, "t", now)
e10._open_epoch(W, {"BTC": {"signed_size": 9.0, "entry_price": 1.0}}, ts_ms=now - 10_000, reason="test")
for i in range(5):
    e10.drift_recovery_gate_by_wallet.pop(W, None)
    e10.audit_position_drift_only(W, apiR.snapshots[W])
st = e10.drift_state_by_wallet.get(W, {}).get("BTC", {}).get("status")
check("T6a after >=3 barren attempts -> UNRESOLVED_ESCALATED",
      st == "UNRESOLVED_ESCALATED", f"status={st} attempts={e10.drift_recovery_attempt_count.get(W)}")
check("T6b escalated is still visibly unresolved", st in {"UNRESOLVED_ESCALATED", "DRIFT_UNRESOLVED"})

# =====================================================================
# TEST 7: restart persistence
# =====================================================================
e10.save_epochs(); e10.save_recovery_gates(); e10.save_watermarks()
e11 = make_engine([W], apiR)
check("T7a epoch survives restart",
      e11.epoch_by_wallet.get(W, {}).get("epoch_id") == e10.epoch_by_wallet.get(W, {}).get("epoch_id"))
check("T7b watermark survives restart",
      e11.trusted_through_ms_by_wallet == e10.trusted_through_ms_by_wallet)
check("T7c attempt count survives restart",
      e11.drift_recovery_attempt_count.get(W) == e10.drift_recovery_attempt_count.get(W),
      f"{e11.drift_recovery_attempt_count.get(W)} vs {e10.drift_recovery_attempt_count.get(W)}")

# =====================================================================
# TEST 8: deterministic epoch-aware rebuild
# =====================================================================
import csv as _csv
e12 = make_engine([W], FakeAPI([]))
e12._startup_fill_wallets = {W}
e12.epoch_by_wallet[W] = {"epoch_id": "d", "baseline_ts_ms": base - 1,
                          "baseline_position": {}, "baseline_provenance": "t", "epoch_status": "OPEN"}
with open(eng.RAW_FILLS_CSV, "w", newline="", encoding="utf-8") as f:
    w = _csv.DictWriter(f, fieldnames=eng.RAW_FILL_FIELDS)
    w.writeheader()
    for i, ts in enumerate([base - 5, base + 1, base + 2]):
        r = eng.RawFill(fill_id=f"f{i}", wallet=W, coin="BTC", side="BUY", price=10.0, size=1.0,
                        signed_size_delta=1.0, start_position=0.0, end_position=1.0, closed_pnl=0.0,
                        fee=0.0, timestamp_ms=ts, timestamp_iso="", received_at_ms=0, received_at_iso="",
                        latency_ms=0, source="poll", recording_method="POLL")
        w.writerow(r.to_csv_row())
e12.rebuild_from_ledger()
pos = e12.positions.get((W, "BTC"))
check("T8a epoch-aware rebuild excludes pre-epoch fill",
      pos is not None and abs(pos.signed_size - 2.0) < 1e-9,
      f"signed={pos.signed_size if pos else None} (expected 2.0)")

e13 = make_engine([W], FakeAPI([]))
e13._startup_fill_wallets = {W}
e13.epoch_by_wallet[W] = dict(e12.epoch_by_wallet[W])
e13.rebuild_from_ledger()
pos2 = e13.positions.get((W, "BTC"))
check("T8b rebuild deterministic across instances",
      abs((pos2.signed_size if pos2 else 0) - 2.0) < 1e-9)

# =====================================================================
print("\n==== SUMMARY ====")
passed = sum(1 for _, ok, _ in RESULTS if ok)
print(f"{passed}/{len(RESULTS)} passed")
for name, ok, detail in RESULTS:
    if not ok:
        print("  FAILED:", name, detail)
sys.exit(0 if passed == len(RESULTS) else 1)
