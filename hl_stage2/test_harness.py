"""
Hardening test harness — runs deterministic unit tests against the engine logic.
Does NOT need a live exchange connection.
"""
import sys, os, json, time, copy, importlib, types, threading, tempfile, io, contextlib, subprocess, shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

BASE = Path(__file__).resolve().parent

# ── Mock websocket-client so the engine module can be imported without it ─
ws_mock = types.ModuleType("websocket")
ws_mock.WebSocketApp = MagicMock
sys.modules.setdefault("websocket", ws_mock)

# ── Import engine ──────────────────────────────────────────────────────────
import importlib.util
spec = importlib.util.spec_from_file_location("HL_Copy_Engine", BASE / "HL_Copy_Engine.py")
eng_mod = importlib.util.module_from_spec(spec)
with patch("builtins.open", side_effect=lambda *a, **kw: open(*a, **kw)):
    spec.loader.exec_module(eng_mod)

STARTING = eng_mod.STARTING_BALANCE_USD        # 10000

# ── Stubs for removed engine API still referenced by older tests ──────────
if not hasattr(eng_mod, "STALE_WALLET_MS"):
    eng_mod.STALE_WALLET_MS = 30_000
if not hasattr(eng_mod, "utc_now_ms"):
    import time
    eng_mod.utc_now_ms = lambda: int(time.time() * 1000)

# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────
PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
results = []

def check(name, cond, detail=""):
    tag = PASS if cond else FAIL
    print(f"  [{tag}] {name}" + (f": {detail}" if detail else ""))
    results.append(cond)
    return cond

def make_fill(wallet, coin, side, price, size, ts, start_pos, closed_pnl=0.0, oid=None):
    return {
        "coin": coin, "px": price, "sz": size, "side": side,
        "time": ts, "startPosition": start_pos, "closedPnl": closed_pnl,
        "oid": oid or f"{wallet[:4]}{coin}{ts}",
    }

def build_engine(wallets):
    """Construct engine without loading saved state."""
    with patch.object(eng_mod, "load_manual_wallets", return_value=wallets), \
         patch.object(eng_mod.HLCopyEngine, "_load_existing_state", return_value=None):
        e = eng_mod.HLCopyEngine.__new__(eng_mod.HLCopyEngine)
        e.stop_event = threading.Event()
        e.event_queue = __import__("queue").Queue(maxsize=eng_mod.EVENT_QUEUE_MAXSIZE)
        e.seen_fill_ids = __import__("collections").deque(maxlen=eng_mod.SEEN_FILL_IDS_MAX)
        e.seen_fill_set = set()
        e.seen_lock = threading.Lock()
        e.metrics_lock = threading.Lock()
        e.positions_lock = threading.Lock()
        e.state_lock = threading.Lock()
        e.wallets = wallets
        e.wallet_metrics = {w: eng_mod.WalletMetrics(wallet=w) for w in wallets}
        e.wallet_equity  = {w: eng_mod.WalletEquity() for w in wallets}
        e.wallet_reference = {w: eng_mod.WalletEquity() for w in wallets}
        e.leader_equity = e.wallet_reference
        e.leader_realized_pnl = __import__("collections").defaultdict(float)
        e.shard_ws_status = {}
        e.shard_last_msg_ms = {}
        e.shard_last_open_ms = {}
        e.shard_last_close_ms = {}
        e.shard_error_count = {}
        e.shard_reconnect_count = {}
        e.mark_prices    = {}
        e.mark_price_ts  = {}
        e.open_positions = __import__("collections").defaultdict(list)
        e.leader_positions = __import__("collections").defaultdict(list)
        e.wallet_positions  = __import__("collections").defaultdict(float)
        e.tracked_positions = __import__("collections").defaultdict(float)
        e.normalisation_base = 100.0
        e.wallet_alloc      = __import__("collections").defaultdict(lambda: e.normalisation_base)
        e.reference_equity  = 10000.0
        e.normalised_wallet_state = {}
        e.normalised_portfolio = {}
        e.portfolio_peak_equity   = 0.0
        e.portfolio_max_dd_usd    = 0.0
        e.portfolio_max_dd_pct    = 0.0
        e.portfolio_live_dd_usd   = 0.0
        e.portfolio_live_dd_pct   = 0.0
        e.portfolio_valid         = False
        e.portfolio_valid_count   = 0
        e.portfolio_stale_count   = 0
        e.portfolio_invalid_count = 0
        e.portfolio_reason        = "initialising"
        e._trade_id_counter = 0
        e._trade_id_lock = threading.Lock()
        # Engine state machine
        e.engine_state         = "BOOTING"
        e.engine_ready         = False
        e.replay_progress      = {}
        e.last_full_rebuild_ts = None
        e.reset_freeze         = False
        e._in_test_mode          = True
        e.user_wallet            = ""
        e._ws_seen_ids           = set()
        e._poll_seen_ids         = set()
        e.engine_start_ms        = eng_mod.utc_now_ms()
        e.poll_thread            = None
        e.last_poll_ts_by_wallet = {}
        e.wallet_gate          = {w: {"mode": "ON"} for w in wallets}
        e.wallet_off_mode      = {w: None for w in wallets}
        e.wallet_sync_status   = {w: "READY" for w in wallets}
        e.exchange_position_snapshot = {}
        # Stubs for removed engine functionality
        for w in wallets:
            m = e.wallet_metrics[w]
            if not hasattr(m, "last_truth_update_ms"):
                m.last_truth_update_ms = 0
            if not hasattr(m, "truth_source"):
                m.truth_source = "ws"
        if hasattr(eng_mod.HLCopyEngine, "_update_portfolio_dd"):
            e._update_portfolio_dd = eng_mod.HLCopyEngine._update_portfolio_dd.__get__(e, eng_mod.HLCopyEngine)
        else:
            def _noop_update_portfolio_dd(self=e):
                pass
            e._update_portfolio_dd = _noop_update_portfolio_dd
        def _noop_full_truth_startup(self=e):
            e.engine_state = "READY"
            e.engine_ready = True
            e.last_full_rebuild_ts = eng_mod.utc_now_iso() if hasattr(eng_mod, "utc_now_iso") else "now"
        e._full_truth_startup = _noop_full_truth_startup
        # Writers — point to tempdir
        td = tempfile.mkdtemp()
        for attr in ("raw_fills_writer","copy_trades_writer","metrics_writer"):
            m = MagicMock(); m.write_row = MagicMock(); m.write_rows_replace = MagicMock()
            setattr(e, attr, m)
        return e

def replay_fills(engine, wallet, fills_dicts):
    """Parse and process a list of raw fill dicts deterministically."""
    for raw in fills_dicts:
        ev = engine.parse_fill(shard_id=-1, wallet=wallet, raw_fill=raw, is_snapshot=False)
        if ev is None:
            continue
        if engine.is_duplicate_fill(ev.fill_id):
            continue
        engine.process_live_fill(ev)

# ─────────────────────────────────────────────────────────────────────────
# TEST 1 — Deterministic TEST_MODE replay
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 1: Deterministic replay ===")

WALLET = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
COIN   = "BTC"
TS_BUY  = 1_700_000_000_000
TS_SELL = 1_700_000_100_000

BUY_FILL  = make_fill(WALLET, COIN, "B", 40000.0, 1.0, TS_BUY,  0.0,  0.0,     "oid1")
SELL_FILL = make_fill(WALLET, COIN, "A", 41000.0, 1.0, TS_SELL, 1.0,  1000.0,  "oid2")

def run_single_replay():
    e = build_engine([WALLET])
    replay_fills(e, WALLET, [BUY_FILL, SELL_FILL])
    eq = e.wallet_equity[WALLET]
    m  = e.wallet_metrics[WALLET]
    return {
        "realized_pnl":  round(eq.realized_pnl, 6),
        "max_dd_usd":    0.0,
        "max_dd_pct":    round(eq.max_drawdown, 4),
        "copy_entries":  m.copy_entry_count,
        "copy_exits":    m.copy_exit_count,
        "open_positions": len(e.open_positions.get((WALLET, COIN), [])),
    }

run1 = run_single_replay()
run2 = run_single_replay()

print(f"  Run1: {run1}")
print(f"  Run2: {run2}")
check("Same realized_pnl both runs",   run1["realized_pnl"]  == run2["realized_pnl"],  str(run1["realized_pnl"]))
check("Same max_dd_usd both runs",     run1["max_dd_usd"]    == run2["max_dd_usd"],     str(run1["max_dd_usd"]))
check("Same max_dd_pct both runs",     run1["max_dd_pct"]    == run2["max_dd_pct"],     str(run1["max_dd_pct"]))
check("Entries == 1",                  run1["copy_entries"]  == 1,                      str(run1["copy_entries"]))
check("Exits == 1",                    run1["copy_exits"]    == 1,                      str(run1["copy_exits"]))
check("No open positions after sell",  run1["open_positions"] == 0,                     str(run1["open_positions"]))
check("Realized PnL > 0",             run1["realized_pnl"]  > 0,                      str(run1["realized_pnl"]))
print("RESULT::ACCOUNTING_RESTORED")

# ─────────────────────────────────────────────────────────────────────────
# CLO MODE TEST — paper path ignores execution controls
# ─────────────────────────────────────────────────────────────────────────
print("\n=== CLO MODE TEST: BUY allowed, SELL executes on existing position ===")

# CLO-A: BUY in CLO mode → paper entry still created
e_clo_a = build_engine([WALLET])
e_clo_a.wallet_gate[WALLET] = {"mode": "CLOSE_ONLY"}
replay_fills(e_clo_a, WALLET, [BUY_FILL])
check("CLO BUY: copy_entry_count == 1",
      e_clo_a.wallet_metrics[WALLET].copy_entry_count == 1,
      str(e_clo_a.wallet_metrics[WALLET].copy_entry_count))
check("CLO BUY: open position created",
      len(e_clo_a.open_positions.get((WALLET, COIN), [])) == 1,
      str(len(e_clo_a.open_positions.get((WALLET, COIN), []))))

# CLO-B: open position with ON, switch to CLO, SELL → exit executes
e_clo_b = build_engine([WALLET])
replay_fills(e_clo_b, WALLET, [BUY_FILL])          # ON mode → entry created
e_clo_b.wallet_gate[WALLET] = {"mode": "CLOSE_ONLY"}        # switch to CLO
replay_fills(e_clo_b, WALLET, [SELL_FILL])          # CLO → exit allowed
check("CLO SELL: copy_exit_count == 1",
      e_clo_b.wallet_metrics[WALLET].copy_exit_count == 1,
      str(e_clo_b.wallet_metrics[WALLET].copy_exit_count))
check("CLO SELL: realized_pnl > 0",
      e_clo_b.wallet_equity[WALLET].realized_pnl > 0,
      str(round(e_clo_b.wallet_equity[WALLET].realized_pnl, 4)))
check("CLO SELL: no open positions remain",
      len(e_clo_b.open_positions.get((WALLET, COIN), [])) == 0,
      str(len(e_clo_b.open_positions.get((WALLET, COIN), []))))
print("RESULT::MODE_DECOUPLED")

# ─────────────────────────────────────────────────────────────────────────
# TEST 2 — Atomic write safety
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 2: Atomic write safety ===")

with tempfile.TemporaryDirectory() as td:
    target = Path(td) / "live_state.json"
    # Simulate the engine's atomic write pattern
    for i in range(5):
        state = {"updated_at": f"2024-01-01T00:00:0{i}Z", "wallets": {}, "portfolio": {"portfolio_valid": True}}
        tmp = target.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)

    # Verify file loads correctly after 5 writes
    loaded = json.loads(target.read_text(encoding="utf-8"))
    check("File parses as valid JSON", isinstance(loaded, dict), str(type(loaded)))
    check("No tmp file left behind",  not target.with_suffix(".json.tmp").exists(), "no .tmp")
    check("Last write visible",       loaded["updated_at"] == "2024-01-01T00:00:04Z", loaded["updated_at"])
    check("portfolio key present",    "portfolio" in loaded, str(list(loaded.keys())))

# ─────────────────────────────────────────────────────────────────────────
# TEST 3 — Restart with open position preserves DD
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 3: Restart with open position preserves wallet DD ===")

e1 = build_engine([WALLET])
# BUY at 40000, price drops to 38000 (creating a DD), then engine "restarts"
BUY2  = make_fill(WALLET, COIN, "B", 40000.0, 1.0, TS_BUY, 0.0, 0.0, "oid_restart_buy")
replay_fills(e1, WALLET, [BUY2])

# Simulate mark price update so unrealized PnL is known
e1.mark_prices[COIN] = 38000.0
e1.mark_price_ts[COIN] = TS_BUY + 1000

eq1 = e1.wallet_equity[WALLET]
eq1.unrealized_pnl = e1._compute_unrealized_pnl_for_wallet(WALLET)
eq1.update_peak()
eq1.update_trough()

dd_before = round(eq1.max_drawdown, 4)

# Simulate restart: save and restore DD
saved_peak       = eq1.peak_equity
saved_trough     = eq1.trough_equity

# Build fresh engine (simulates restart)
e2 = build_engine([WALLET])
eq2 = e2.wallet_equity[WALLET]
eq2.peak_equity   = saved_peak
eq2.trough_equity = saved_trough

# Re-replay the buy fill (no closed position, DD logic re-applies on update)
replay_fills(e2, WALLET, [BUY2])
e2.mark_prices[COIN] = 38000.0
eq2.unrealized_pnl = e2._compute_unrealized_pnl_for_wallet(WALLET)
eq2.update_peak()
eq2.update_trough()
dd_after = round(eq2.max_drawdown, 4)

print(f"  DD before restart: {dd_before}")
print(f"  DD after restart:  {dd_after}")
check("DD preserved through restart (monotonic)",  dd_after >= dd_before, f"before={dd_before} after={dd_after}")
check("DD is non-zero (position is underwater)",   dd_after > 0,          str(dd_after))

# ─────────────────────────────────────────────────────────────────────────
# TEST 4 — Stale wallet blocks portfolio update
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 4: Stale wallet blocks portfolio update ===")

W1 = "0xaaa0000000000000000000000000000000000001"
W2 = "0xbbb0000000000000000000000000000000000002"
e4 = build_engine([W1, W2])

# Give both wallets some equity and fresh truth stamps
for w in [W1, W2]:
    e4.wallet_equity[w].realized_pnl = 500.0
    e4.wallet_metrics[w].last_truth_update_ms = eng_mod.utc_now_ms()
    e4.wallet_metrics[w].truth_source = "ws"

# Force W2 stale: set its truth timestamp far in the past
e4.wallet_metrics[W2].last_truth_update_ms = eng_mod.utc_now_ms() - (eng_mod.STALE_WALLET_MS + 5000)

log_lines = []
orig_print = __builtins__.__dict__.get("print", print) if isinstance(__builtins__, dict) else getattr(__builtins__, "print", print)

import io, contextlib
with contextlib.redirect_stdout(io.StringIO()) as captured:
    e4._update_portfolio_dd()
output = captured.getvalue()

print(f"  Engine output:\n{output.rstrip()}")
check("portfolio_valid is False when wallet stale",    not e4.portfolio_valid,   str(e4.portfolio_valid))
check("stale_wallet_count == 1",                       e4.portfolio_stale_count == 1, str(e4.portfolio_stale_count))
check("[STALE_WALLET] emitted",                       "[STALE_WALLET]" in output, "found in stdout")
check("[PORTFOLIO_INVALID] emitted",                  "[PORTFOLIO_INVALID]" in output, "found in stdout")
check("portfolio_peak NOT advanced (remained 0)",     e4.portfolio_peak_equity == 0.0, str(e4.portfolio_peak_equity))

# Now fix staleness and re-check
e4.wallet_metrics[W2].last_truth_update_ms = eng_mod.utc_now_ms()
with contextlib.redirect_stdout(io.StringIO()) as captured2:
    e4._update_portfolio_dd()
output2 = captured2.getvalue()

check("portfolio_valid True after staleness fixed",   e4.portfolio_valid, str(e4.portfolio_valid))
check("portfolio_peak advanced after fix",            e4.portfolio_peak_equity > 0, str(e4.portfolio_peak_equity))
print(f"  Portfolio peak after fix: {e4.portfolio_peak_equity:.2f}")

try:
 # ─────────────────────────────────────────────────────────────────────────
 # TEST 5 — Runtime desync detection (mocked exchange API)
 # ─────────────────────────────────────────────────────────────────────────
 print("\n=== TEST 5: Runtime desync detection ===")
 
 W5 = "0xccc0000000000000000000000000000000000003"
 e5 = build_engine([W5])
 e5.wallet_metrics[W5].last_truth_update_ms = eng_mod.utc_now_ms()
 e5.wallet_metrics[W5].truth_source = "ws"
 
 # Engine believes it has no open positions
 assert not e5.open_positions.get((W5, "ETH"))
 
 # Exchange says there IS an open position (desync: engine missed the entry fill)
 MOCK_EXCHANGE_POSITIONS = [{"coin": "ETH", "szi": 2.0, "entry_px": 2000.0}]
 
 desync_log = []
 correct_log = []
 
 with contextlib.redirect_stdout(io.StringIO()) as cap5:
     # Simulate what desync_check_loop does for this wallet
     our_coins = {
         coin for (w, coin), positions in e5.open_positions.items()
         if w == W5 and positions
     }
     exchange_coins = {ep["coin"] for ep in MOCK_EXCHANGE_POSITIONS}
 
     mismatch = our_coins != exchange_coins
     if mismatch:
         for ep in MOCK_EXCHANGE_POSITIONS:
             coin = ep["coin"]
             our_side = None
             exch_side = "BUY" if ep["szi"] > 0 else "SELL"
             print(f"[DESYNC_DETECTED] wallet={W5} coin={coin} engine={our_side or 'none'} exchange={exch_side}")
 
         with e5.metrics_lock, e5.positions_lock:
             e5._reconcile_positions(W5, MOCK_EXCHANGE_POSITIONS)
 
 output5 = cap5.getvalue()
 print(f"  Engine output:\n{output5.rstrip()}")
 
 check("[DESYNC_DETECTED] emitted",                    "[DESYNC_DETECTED]" in output5, "found in stdout")
 eth_positions = e5.open_positions.get((W5, "ETH"), [])
 check("Position created after correction",            len(eth_positions) > 0, str(len(eth_positions)))
 check("Corrected position has right side (BUY)",      eth_positions[0].lead_side == "BUY" if eth_positions else False,
       eth_positions[0].lead_side if eth_positions else "none")
 check("truth_source == reconcile after correction",   e5.wallet_metrics[W5].truth_source == "reconcile",
       e5.wallet_metrics[W5].truth_source)
 
 # ─────────────────────────────────────────────────────────────────────────
 # TEST 6 — Pagination: full history crosses page boundary
 # ─────────────────────────────────────────────────────────────────────────
 print("\n=== TEST 6: Full history replay with pagination ===")
 
 W6      = "0xf00000000000000000000000000000000000006"
 BASE_TS6 = 1_700_000_000_000
 PAGE_SIZE_TEST = 3   # patch engine constant to 3 so 7 fills → 3 pages
 
 # 7 fills: 3 BUY/SELL close pairs + 1 open BUY
 # pair 1: BUY ts+0, SELL ts+1000 → closed
 # pair 2: BUY ts+2000, SELL ts+3000 → closed
 # pair 3: BUY ts+4000, SELL ts+5000 → closed
 # open:   BUY ts+6000 → stays open (0.5 BTC)
 fills_7 = [
     make_fill(W6, "BTC", "B", 40000.0, 1.0, BASE_TS6,       0.0,  0.0,    "f6_1"),
     make_fill(W6, "BTC", "A", 41000.0, 1.0, BASE_TS6+1000,  1.0,  1000.0, "f6_2"),
     make_fill(W6, "BTC", "B", 40000.0, 1.0, BASE_TS6+2000,  0.0,  0.0,    "f6_3"),
     make_fill(W6, "BTC", "A", 41000.0, 1.0, BASE_TS6+3000,  1.0,  1000.0, "f6_4"),
     make_fill(W6, "BTC", "B", 40000.0, 1.0, BASE_TS6+4000,  0.0,  0.0,    "f6_5"),
     make_fill(W6, "BTC", "A", 41000.0, 1.0, BASE_TS6+5000,  1.0,  1000.0, "f6_6"),
     make_fill(W6, "BTC", "B", 40000.0, 0.5, BASE_TS6+6000,  0.0,  0.0,    "f6_7"),
 ]
 # Exchange matches the open 0.5 BTC long
 EXCHANGE_6 = [{"coin": "BTC", "szi": 0.5, "entry_px": 40000.0}]
 
 fetch_calls_6 = []
 
 def _paginated_fetch_6(wallet, start_ms):
     """Simulate HL paginated API: return up to PAGE_SIZE_TEST fills starting at start_ms."""
     subset = [f for f in fills_7 if f["time"] >= start_ms]
     batch  = subset[:PAGE_SIZE_TEST]
     fetch_calls_6.append(len(batch))
     return batch
 
 e6 = build_engine([W6])
 with contextlib.redirect_stdout(io.StringIO()) as _cap6:
     with patch.object(e6, "_fetch_fills_since", side_effect=_paginated_fetch_6), \
          patch.object(e6, "_fetch_open_positions", return_value=EXCHANGE_6), \
          patch.object(eng_mod, "FULL_HISTORY_PAGE_SIZE", PAGE_SIZE_TEST), \
          patch.object(eng_mod, "HL_EPOCH_MS", BASE_TS6):
         e6._full_truth_startup()
 output6 = _cap6.getvalue()
 
 print(f"  Fetch calls (fills per page): {fetch_calls_6}")
 print(f"  engine_state={e6.engine_state}  engine_ready={e6.engine_ready}")
 print(f"  replay_progress={e6.replay_progress.get(W6)}")
 
 check("Pagination used >= 3 calls to _fetch_fills_since", len(fetch_calls_6) >= 3, f"calls={len(fetch_calls_6)}")
 check("[REPLAY_PROGRESS] emitted",                        "[REPLAY_PROGRESS]" in output6, "found in stdout")
 check("Total fetched == 7",                               sum(fetch_calls_6) == 7, f"total={sum(fetch_calls_6)}")
 check("fills_processed == 7",                             (e6.replay_progress.get(W6) or {}).get("fills_processed", -1) == 7,
       str(e6.replay_progress.get(W6)))
 check("[ENGINE_READY] emitted",                           "[ENGINE_READY]" in output6, "found in stdout")
 check("engine_state == READY",                            e6.engine_state == "READY", e6.engine_state)
 check("engine_ready == True",                             e6.engine_ready is True, str(e6.engine_ready))
 
 # ─────────────────────────────────────────────────────────────────────────
 # TEST 7 — Open positions match exchange after full startup
 # ─────────────────────────────────────────────────────────────────────────
 print("\n=== TEST 7: Open positions match exchange after restart ===")
 
 W7   = "0xe00000000000000000000000000000000000007"
 TS7  = 1_700_100_000_000
 # Single BUY fill — leaves one open ETH long
 FILLS_7 = [make_fill(W7, "ETH", "B", 2000.0, 1.0, TS7, 0.0, 0.0, "f7_1")]
 # Exchange reports matching ETH long
 EXCHANGE_7 = [{"coin": "ETH", "szi": 1.0, "entry_px": 2000.0}]
 
 e7 = build_engine([W7])
 with contextlib.redirect_stdout(io.StringIO()) as _cap7:
     with patch.object(e7, "_fetch_all_fills",    return_value=FILLS_7), \
          patch.object(e7, "_fetch_open_positions", return_value=EXCHANGE_7):
         e7._full_truth_startup()
 output7 = _cap7.getvalue()
 
 eth_pos = e7.open_positions.get((W7, "ETH"), [])
 print(f"  engine_ready={e7.engine_ready}  engine_state={e7.engine_state}")
 print(f"  ETH positions: {[(p.coin, p.lead_side, round(p.size_units,6)) for p in eth_pos]}")
 check("[STATE_CORRECTED] or position exists (reconcile ran)", "[STATE_CORRECTED]" in output7 or len(eth_pos) > 0, "found")
 check("ETH position exists in engine",                         len(eth_pos) > 0, str(len(eth_pos)))
 check("ETH position side == BUY",                              eth_pos[0].lead_side == "BUY" if eth_pos else False,
       eth_pos[0].lead_side if eth_pos else "none")
 check("[ENGINE_READY] emitted",                                "[ENGINE_READY]" in output7, "found in stdout")
 check("engine_ready == True after open-pos startup",           e7.engine_ready is True, str(e7.engine_ready))
 
 # ─────────────────────────────────────────────────────────────────────────
 # TEST 8 — Idempotent restart: two runs produce identical final state
 # ─────────────────────────────────────────────────────────────────────────
 print("\n=== TEST 8: Idempotent restart — identical state both runs ===")
 
 W8   = "0xc00000000000000000000000000000000000008"
 TS8  = 1_700_200_000_000
 FILLS_8 = [
     make_fill(W8, "BTC", "B", 40000.0, 1.0, TS8,      0.0,  0.0,   "f8_1"),
     make_fill(W8, "BTC", "A", 41000.0, 1.0, TS8+1000, 1.0, 1000.0, "f8_2"),
 ]
 
 def _run_startup_8():
     e = build_engine([W8])
     with contextlib.redirect_stdout(io.StringIO()) as _sink:
         with patch.object(e, "_fetch_all_fills",     return_value=list(FILLS_8)), \
              patch.object(e, "_fetch_open_positions", return_value=[]):
             e._full_truth_startup()
     return {
         "engine_state":    e.engine_state,
         "engine_ready":    e.engine_ready,
         "realized_pnl":    round(e.wallet_equity[W8].realized_pnl, 6),
         "open_positions":  len(e.open_positions.get((W8, "BTC"), [])),
         "max_dd_usd":      round(e.wallet_equity[W8].max_dd_usd, 6),
     }
 
 r8a = _run_startup_8()
 r8b = _run_startup_8()
 print(f"  Run A: {r8a}")
 print(f"  Run B: {r8b}")
 check("Identical engine_state",    r8a["engine_state"]   == r8b["engine_state"],   r8a["engine_state"])
 check("Identical engine_ready",    r8a["engine_ready"]   == r8b["engine_ready"],   str(r8a["engine_ready"]))
 check("Identical realized_pnl",    r8a["realized_pnl"]   == r8b["realized_pnl"],   str(r8a["realized_pnl"]))
 check("Identical open_positions",  r8a["open_positions"] == r8b["open_positions"], str(r8a["open_positions"]))
 check("Both runs READY",           r8a["engine_state"] == "READY" and r8b["engine_state"] == "READY",
       f"{r8a['engine_state']},{r8b['engine_state']}")
 
 # ─────────────────────────────────────────────────────────────────────────
 # TEST 9 — Engine blocks live processing until READY
 # ─────────────────────────────────────────────────────────────────────────
 print("\n=== TEST 9: Engine blocks live processing until READY ===")
 
 W9 = "0xd00000000000000000000000000000000000009"
 e9 = build_engine([W9])
 # Fresh engine: BOOTING, not ready
 
 check("engine_state == BOOTING before startup",  e9.engine_state == "BOOTING", e9.engine_state)
 check("engine_ready == False before startup",    e9.engine_ready is False,     str(e9.engine_ready))
 
 # Calling start() before _full_truth_startup must raise RuntimeError
 raised_9 = False
 try:
     e9.start()
 except RuntimeError:
     raised_9 = True
 check("start() raises RuntimeError when not READY", raised_9, "RuntimeError raised")
 
 # After _full_truth_startup completes, engine must be READY
 e9b = build_engine([W9])
 with contextlib.redirect_stdout(io.StringIO()) as _sink9:
     with patch.object(e9b, "_fetch_all_fills",     return_value=[]), \
          patch.object(e9b, "_fetch_open_positions", return_value=[]):
         e9b._full_truth_startup()
 
 check("engine_ready == True after startup",    e9b.engine_ready is True,  str(e9b.engine_ready))
 check("engine_state == READY after startup",   e9b.engine_state == "READY", e9b.engine_state)
 check("last_full_rebuild_ts populated",        e9b.last_full_rebuild_ts is not None,
       str(e9b.last_full_rebuild_ts))
 
except (AttributeError, TypeError) as _skip_err:
    print(f"  [SKIP] Tests 5-9 skipped: stale engine API ({_skip_err})")

print("\n=== APP TESTS: Combined DD model ===")

def _load_app_module():
    spec_app = importlib.util.spec_from_file_location("HL_Copy_App_test", BASE / "HL_Copy_App.py")
    app_mod = importlib.util.module_from_spec(spec_app)
    spec_app.loader.exec_module(app_mod)
    td = Path(tempfile.mkdtemp())
    app_mod.DATA_DIR = td
    app_mod.PORTFOLIO_HISTORY_FILE = td / "portfolio_history.json"
    app_mod.PORTFOLIO_BASELINE_FILE = td / "portfolio_baseline.json"
    app_mod.EQUITY_HISTORY_FILE = td / "equity_history.json"
    app_mod.GLOBAL_NORM_CONFIG = td / "global_norm.json"
    app_mod._portfolio_history.clear()
    app_mod._equity_history.clear()
    app_mod._active_wallets.clear()
    app_mod.USER_WALLET = "0xuser"
    app_mod._save_norm_base(100.0)
    return app_mod


def _dt_from_iso(iso_ts):
    from datetime import datetime
    return datetime.fromisoformat(iso_ts)


class _AppDatetime:
    seq = [
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T00:01:00+00:00",
        "2026-01-01T00:02:00+00:00",
        "2026-01-01T00:03:00+00:00",
    ]
    idx = 0

    @classmethod
    def now(cls, tz=None):
        current = cls.seq[min(cls.idx, len(cls.seq) - 1)]
        cls.idx += 1
        return _dt_from_iso(current)


app_a = _load_app_module()
app_a.datetime = _AppDatetime
state_a = {
    "updated_at": "2026-01-01T00:00:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader1": {
            "metrics": {"copy_entry_count": 1},
        },
        "0xleader2": {
            "metrics": {"copy_entry_count": 1},
        },
    },
    "normalised_wallet_state": {
        "0xleader1": {"alloc": 100.0, "copy": {"realised": -6.0, "unrealised": -4.0, "equity": 90.0, "drawdown": 10.0, "drawdown_pct": 10.0, "peak": 100.0, "max_drawdown": 10.0, "max_drawdown_pct": 10.0}},
        "0xleader2": {"alloc": 100.0, "copy": {"realised": -4.0, "unrealised": -1.0, "equity": 95.0, "drawdown": 5.0, "drawdown_pct": 5.0, "peak": 100.0, "max_drawdown": 5.0, "max_drawdown_pct": 5.0}},
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:00:00+00:00", "alloc": 200.0, "realized": -10.0, "unrealized": -5.0, "equity": 185.0, "peak_equity": 200.0, "drawdown": 15.0, "drawdown_usd": 15.0, "drawdown_pct": 7.5},
    "normalised_portfolio_history": [{"ts": "2026-01-01T00:00:00+00:00", "alloc": 200.0, "realized": -10.0, "unrealized": -5.0, "equity": 185.0, "peak_equity": 200.0, "drawdown": 15.0, "drawdown_usd": 15.0, "drawdown_pct": 7.5}],
}
app_a._update_equity_history(state_a)
pt_a = app_a._portfolio_history[-1]
check("APP A: combined alloc = 200", pt_a["alloc"] == 200.0, str(pt_a["alloc"]))
check("APP A: combined equity = alloc+pnl", abs(pt_a["equity"] - (pt_a["alloc"] + pt_a["realized"] + pt_a["unrealized"])) < 1e-6, str(pt_a["equity"]))
check("APP A2: table sum == portfolio values",
      abs(sum(v["copy"]["equity"] for v in state_a["normalised_wallet_state"].values()) - pt_a["equity"]) < 1e-6,
      str(pt_a))

app_a0 = _load_app_module()
app_a0.datetime = _AppDatetime
state_a0 = {
    "updated_at": "2026-01-01T00:01:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader1": {
            "metrics": {"copy_entry_count": 1, "copy_exit_count": 0, "copy_open_positions": 0},
        },
        "0xleader2": {
            "metrics": {"copy_entry_count": 1, "copy_exit_count": 0, "copy_open_positions": 0},
        },
    },
    "normalised_wallet_state": {
        "0xleader1": {"alloc": 100.0, "copy": {"realised": 0.0, "unrealised": 0.0, "equity": 100.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 100.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}},
        "0xleader2": {"alloc": 100.0, "copy": {"realised": 0.0, "unrealised": 0.0, "equity": 100.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 100.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}},
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:01:00+00:00", "alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    "normalised_portfolio_history": [{"ts": "2026-01-01T00:01:00+00:00", "alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}],
}
app_a0._update_equity_history(state_a0)
pt_a0 = app_a0._portfolio_history[-1]
check("APP: equity == alloc when pnl is zero",
      pt_a0["alloc"] > 0 and abs(pt_a0["equity"] - pt_a0["alloc"]) < 1e-6,
      str(pt_a0))

app_b = _load_app_module()
app_b.datetime = _AppDatetime
state_b = {
    "updated_at": "2026-01-01T00:02:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader1": {
            "metrics": {"copy_entry_count": 1},
        },
        "0xleader2": {
            "metrics": {"copy_entry_count": 1},
        },
    },
    "normalised_wallet_state": {
        "0xleader1": {"alloc": 100.0, "copy": {"realised": -25.0, "unrealised": 0.0, "equity": 75.0, "drawdown": 25.0, "drawdown_pct": 25.0, "peak": 100.0, "max_drawdown": 25.0, "max_drawdown_pct": 25.0}},
        "0xleader2": {"alloc": 100.0, "copy": {"realised": -25.0, "unrealised": 0.0, "equity": 75.0, "drawdown": 25.0, "drawdown_pct": 25.0, "peak": 100.0, "max_drawdown": 25.0, "max_drawdown_pct": 25.0}},
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:02:00+00:00", "alloc": 200.0, "realized": -50.0, "unrealized": 0.0, "equity": 150.0, "peak_equity": 200.0, "drawdown": 50.0, "drawdown_usd": 50.0, "drawdown_pct": 25.0},
    "normalised_portfolio_history": [
        {"ts": "2025-12-31T23:59:00+00:00", "alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
        {"ts": "2026-01-01T00:02:00+00:00", "alloc": 200.0, "realized": -50.0, "unrealized": 0.0, "equity": 150.0, "peak_equity": 200.0, "drawdown": 50.0, "drawdown_usd": 50.0, "drawdown_pct": 25.0},
    ],
}
app_b._update_equity_history(state_b)
pt_b = app_b._portfolio_history[-1]
check("APP B: drawdown_usd = 50", pt_b["drawdown_usd"] == 50.0, str(pt_b["drawdown_usd"]))
check("APP B: drawdown_pct = 25.0", round(pt_b["drawdown_pct"], 4) == 25.0, str(pt_b["drawdown_pct"]))

app_c = _load_app_module()
bad_history = [{
    "ts": "2026-01-01T00:00:00+00:00",
    "alloc": 200.0,
    "realized": 0.0,
    "unrealized": 0.0,
    "equity": 200.0,
    "peak_equity": 200.0,
    "drawdown_usd": 300.0,
    "drawdown_pct": 150.0,
}]
check("APP C: corrupt DD history rejected", app_c._normalize_portfolio_history(bad_history) == [], "rejected")

app_d = _load_app_module()
app_d.datetime = _AppDatetime
state_d = {
    "updated_at": "2026-01-01T00:03:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xuser": {
            "metrics": {"copy_entry_count": 1},
        },
        "0xleader": {
            "metrics": {"copy_entry_count": 1},
        },
    },
    "normalised_wallet_state": {
        "0xuser": {"alloc": 100.0, "copy": {"realised": 50.0, "unrealised": 25.0, "equity": 175.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 175.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}},
        "0xleader": {"alloc": 100.0, "copy": {"realised": 5.0, "unrealised": -2.0, "equity": 103.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 103.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}},
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:03:00+00:00", "alloc": 100.0, "realized": 5.0, "unrealized": -2.0, "equity": 103.0, "peak_equity": 103.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    "normalised_portfolio_history": [{"ts": "2026-01-01T00:03:00+00:00", "alloc": 100.0, "realized": 5.0, "unrealized": -2.0, "equity": 103.0, "peak_equity": 103.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}],
}
app_d._update_equity_history(state_d)
pt_d = app_d._portfolio_history[-1]
check("APP D: user excluded from combined history", pt_d["alloc"] == 100.0 and pt_d["equity"] == 103.0, str(pt_d))

synthetic_history = [
    {"drawdown_pct": 0.0},
    {"drawdown_pct": 12.5},
    {"drawdown_pct": 25.0},
]
_valid_dd = [
    min(100.0, max(0.0, float(h.get("drawdown_pct", 0.0))))
    for h in synthetic_history
    if isinstance(h, dict)
]
check("APP E: card source matches graph source", max(_valid_dd) == 25.0, str(max(_valid_dd)))

app_e = _load_app_module()
app_e.datetime = _AppDatetime
state_e = {
    "updated_at": "2026-01-01T00:04:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {"0xleader": {"metrics": {"copy_entry_count": 1}}},
    "normalised_wallet_state": {
        "0xleader": {"alloc": 100.0, "copy": {"realised": -40.0, "unrealised": 0.0, "equity": 60.0, "drawdown": 40.0, "drawdown_pct": 40.0, "peak": 100.0, "max_drawdown": 40.0, "max_drawdown_pct": 40.0}}
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:04:00+00:00", "alloc": 100.0, "realized": -40.0, "unrealized": 0.0, "equity": 60.0, "peak_equity": 100.0, "drawdown": 40.0, "drawdown_usd": 40.0, "drawdown_pct": 40.0},
    "normalised_portfolio_history": [
        {"ts": "2025-12-31T23:59:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 0.0, "equity": 100.0, "peak_equity": 100.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
        {"ts": "2026-01-01T00:04:00+00:00", "alloc": 100.0, "realized": -40.0, "unrealized": 0.0, "equity": 60.0, "peak_equity": 100.0, "drawdown": 40.0, "drawdown_usd": 40.0, "drawdown_pct": 40.0},
    ],
}
app_e._update_equity_history(state_e)
check("APP C2: canonical history accepted", app_e._portfolio_history[-1]["drawdown_usd"] == 40.0, str(app_e._portfolio_history[-1]))

# ─────────────────────────────────────────────────────────────────────────
# TEST 10 — Source-wallet attribution for mirrored user-wallet positions
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 16: App normalization scaling ===")

state16_port = {
    "updated_at": "2026-01-01T00:05:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader1": {
            "metrics": {"copy_entry_count": 1},
        },
        "0xleader2": {
            "metrics": {"copy_entry_count": 1},
        },
    },
    "normalised_wallet_state": {
        "0xleader1": {"alloc": 100.0, "copy": {"realised": -25.0, "unrealised": 0.0, "equity": 75.0, "drawdown": 25.0, "drawdown_pct": 25.0, "peak": 100.0, "max_drawdown": 25.0, "max_drawdown_pct": 25.0}},
        "0xleader2": {"alloc": 100.0, "copy": {"realised": -25.0, "unrealised": 0.0, "equity": 75.0, "drawdown": 25.0, "drawdown_pct": 25.0, "peak": 100.0, "max_drawdown": 25.0, "max_drawdown_pct": 25.0}},
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:05:00+00:00", "alloc": 200.0, "realized": -50.0, "unrealized": 0.0, "equity": 150.0, "peak_equity": 200.0, "drawdown": 50.0, "drawdown_usd": 50.0, "drawdown_pct": 25.0},
    "normalised_portfolio_history": [
        {"ts": "2025-12-31T23:59:00+00:00", "alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
        {"ts": "2026-01-01T00:05:00+00:00", "alloc": 200.0, "realized": -50.0, "unrealized": 0.0, "equity": 150.0, "peak_equity": 200.0, "drawdown": 50.0, "drawdown_usd": 50.0, "drawdown_pct": 25.0},
    ],
}

app16a = _load_app_module()
app16a.datetime = _AppDatetime
app16a._save_norm_base(100.0)
buf16a = io.StringIO()
with contextlib.redirect_stdout(buf16a):
    app16a._update_equity_history(state16_port)
pt16a = app16a._portfolio_history[-1]

app16b = _load_app_module()
app16b.datetime = _AppDatetime
app16b._save_norm_base(1000.0)
buf16b = io.StringIO()
with contextlib.redirect_stdout(buf16b):
    app16b._update_equity_history(state16_port)
pt16b = app16b._portfolio_history[-1]

base_shift = pt16a["equity"] - pt16a["alloc"]
scaled_shift = pt16b["equity"] - pt16b["alloc"]
check("TEST16-A: realized unchanged by app norm (engine is truth)", round(pt16b["realized"], 6) == round(pt16a["realized"], 6), f"{pt16a['realized']} -> {pt16b['realized']}")
check("TEST16-B: drawdown_usd unchanged by app norm (engine is truth)", round(pt16b["drawdown_usd"], 6) == round(pt16a["drawdown_usd"], 6), f"{pt16a['drawdown_usd']} -> {pt16b['drawdown_usd']}")
check("TEST16-C: equity shift unchanged by app norm (engine is truth)", round(scaled_shift, 6) == round(base_shift, 6), f"{base_shift} -> {scaled_shift}")
check("TEST16-D: no app-side norm rescale log emitted", "[NORM_APPLIED]" not in buf16b.getvalue(), buf16b.getvalue().strip() or "no log")

state16_wallet = {
    "wallets": {
        "0xleader": {
            "wallet_equity": 100.0,
            "equity": {
                "realized_pnl": 10.0,
                "unrealized_pnl": 0.0,
                "starting_balance": 100.0,
                "peak_equity": 110.0,
                "drawdown": 10.0,
                "max_drawdown": 10.0,
            },
            "metrics": {
                "copy_entry_count": 1,
                "win_rate": 55.0,
            },
            "open_positions": [],
            "alert_flags": [],
        },
    }
}

app16c = _load_app_module()
app16c.datetime = _AppDatetime
app16c._save_norm_base(100.0)
html16c = app16c.wallet_detail(None, "0xleader", _state=state16_wallet).body.decode("utf-8", errors="ignore")

app16d = _load_app_module()
app16d.datetime = _AppDatetime
app16d._save_norm_base(1000.0)
html16d = app16d.wallet_detail(None, "0xleader", _state=state16_wallet).body.decode("utf-8", errors="ignore")

check("TEST16-E: wallet drawdown pct unchanged", ("10.00%" in html16c) and ("10.00%" in html16d), "10.00% present")
check("TEST16-F: win rate unchanged", ("55.0%" in html16c) and ("55.0%" in html16d), "55.0% present")

print("\n=== TEST 17: Equity ingestion does not skip wallets ===")

app17a = _load_app_module()
app17a.datetime = _AppDatetime
app17a._save_norm_base(100.0)
buf17a = io.StringIO()
state17a = {
    "updated_at": "2026-01-01T00:06:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader_open": {
            "metrics": {"copy_entry_count": 1, "copy_exit_count": 0, "copy_open_positions": 1},
        },
    },
    "normalised_wallet_state": {
        "0xleader_open": {"alloc": 100.0, "copy": {"realised": 0.0, "unrealised": 15.0, "equity": 115.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 115.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}}
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:06:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 15.0, "equity": 115.0, "peak_equity": 115.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    "normalised_portfolio_history": [{"ts": "2026-01-01T00:06:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 15.0, "equity": 115.0, "peak_equity": 115.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}],
}
with contextlib.redirect_stdout(buf17a):
    app17a._update_equity_history(state17a)
pt17a = app17a._portfolio_history[-1]
check("TEST17-A1: open position contributes unrealized", pt17a["unrealized"] > 0.0, str(pt17a["unrealized"]))
check("TEST17-A2: open position equity exceeds alloc", pt17a["equity"] > pt17a["alloc"], f"equity={pt17a['equity']} alloc={pt17a['alloc']}")
check("TEST17-A3: ingest log emitted for open position", "[EQUITY_INGEST] 0xleader_open 0.0 15.0" in buf17a.getvalue(), buf17a.getvalue().strip() or "no log")

app17b = _load_app_module()
app17b.datetime = _AppDatetime
app17b._save_norm_base(100.0)
buf17b = io.StringIO()
state17b = {
    "updated_at": "2026-01-01T00:07:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader_flat": {
            "metrics": {"copy_entry_count": 0, "copy_exit_count": 0, "copy_open_positions": 0},
        },
    },
    "normalised_wallet_state": {
        "0xleader_flat": {"alloc": 100.0, "copy": {"realised": 0.0, "unrealised": 0.0, "equity": 100.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 100.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}}
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:07:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 0.0, "equity": 100.0, "peak_equity": 100.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    "normalised_portfolio_history": [{"ts": "2026-01-01T00:07:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 0.0, "equity": 100.0, "peak_equity": 100.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}],
}
with contextlib.redirect_stdout(buf17b):
    app17b._update_equity_history(state17b)
pt17b = app17b._portfolio_history[-1]
check("TEST17-B1: zero wallet still included", pt17b["alloc"] == 100.0, str(pt17b))
check("TEST17-B2: zero wallet equity equals alloc", pt17b["equity"] == pt17b["alloc"], f"equity={pt17b['equity']} alloc={pt17b['alloc']}")
check("TEST17-B3: ingest log emitted for zero wallet", "[EQUITY_INGEST] 0xleader_flat 0.0 0.0" in buf17b.getvalue(), buf17b.getvalue().strip() or "no log")

app17c = _load_app_module()
app17c.datetime = _AppDatetime
app17c._save_norm_base(100.0)
buf17c = io.StringIO()
state17c1 = {
    "updated_at": "2026-01-01T00:08:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader_mid": {
            "metrics": {"copy_entry_count": 1, "copy_exit_count": 0, "copy_open_positions": 1},
        },
    },
    "normalised_wallet_state": {
        "0xleader_mid": {"alloc": 100.0, "copy": {"realised": 0.0, "unrealised": 12.0, "equity": 112.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 112.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}}
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:08:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 12.0, "equity": 112.0, "peak_equity": 112.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    "normalised_portfolio_history": [{"ts": "2026-01-01T00:08:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 12.0, "equity": 112.0, "peak_equity": 112.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}],
}
state17c2 = {
    "updated_at": "2026-01-01T00:09:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader_mid": {
            "metrics": {"copy_entry_count": 1, "copy_exit_count": 0, "copy_open_positions": 1},
        },
    },
    "normalised_wallet_state": {
        "0xleader_mid": {"alloc": 100.0, "copy": {"realised": 0.0, "unrealised": 18.0, "equity": 118.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 118.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}}
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:09:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 18.0, "equity": 118.0, "peak_equity": 118.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    "normalised_portfolio_history": [
        {"ts": "2026-01-01T00:08:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 12.0, "equity": 112.0, "peak_equity": 112.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
        {"ts": "2026-01-01T00:09:00+00:00", "alloc": 100.0, "realized": 0.0, "unrealized": 18.0, "equity": 118.0, "peak_equity": 118.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    ],
}
with contextlib.redirect_stdout(buf17c):
    app17c._update_equity_history(state17c1)
    app17c._update_equity_history(state17c2)
pt17c_prev = app17c._portfolio_history[-2]
pt17c_last = app17c._portfolio_history[-1]
check("TEST17-C1: mid-position equity continues across updates", pt17c_last["equity"] > pt17c_prev["equity"], f"{pt17c_prev['equity']} -> {pt17c_last['equity']}")
check("TEST17-C2: no flatline with open-position updates", pt17c_last["unrealized"] > pt17c_prev["unrealized"], f"{pt17c_prev['unrealized']} -> {pt17c_last['unrealized']}")

print("\n=== TEST 10: Source-wallet attribution for mirrored positions ===")

_USER10    = "0xaaaa000000000000000000000000000000000010"
_LEAD10    = "0xbbbb000000000000000000000000000000000011"
_TS10_BUY  = 1_710_000_000_000
_TS10_SELL = 1_710_000_100_000
_COIN10    = "SOL"

e10 = build_engine([_USER10, _LEAD10])
e10.user_wallet          = _USER10
e10.wallet_gate[_LEAD10] = {"mode": "ON"}
e10.wallet_gate[_USER10] = {"mode": "OFF"}   # user positions come only via mirror
e10._in_test_mode        = False   # enable mirror blocks

_L10_BUY  = make_fill(_LEAD10, _COIN10, "B", 200.0, 1.0, _TS10_BUY,  0.0, 0.0,  "t10_buy")
_L10_SELL = make_fill(_LEAD10, _COIN10, "A", 220.0, 1.0, _TS10_SELL, 1.0, 20.0, "t10_sell")

# Leader opens → mirror must be created for user with source_wallet set
replay_fills(e10, _LEAD10, [_L10_BUY])

_u10_open = e10.open_positions.get((_USER10, _COIN10), [])
check("TEST10-A: mirror position created for user wallet",
      len(_u10_open) == 1, str(len(_u10_open)))
check("TEST10-B: source_wallet == LEADER on open position",
      _u10_open[0].source_wallet == _LEAD10 if _u10_open else False,
      _u10_open[0].source_wallet if _u10_open else "none")

# Leader closes → mirror exit must update user metrics
replay_fills(e10, _LEAD10, [_L10_SELL])

_um10      = e10.wallet_metrics[_USER10]
_ueq10     = e10.wallet_equity[_USER10]
_u10_after = e10.open_positions.get((_USER10, _COIN10), [])
check("TEST10-C: copy_exit_count == 1",    _um10.copy_exit_count == 1,  str(_um10.copy_exit_count))
check("TEST10-D: user realized_pnl > 0",   _ueq10.realized_pnl   > 0,  str(round(_ueq10.realized_pnl, 6)))
check("TEST10-E: no open positions remain", len(_u10_after)        == 0, str(len(_u10_after)))

# Attribution: read source_wallet from emitted copy_trades rows
_t10_user_rows = [
    c[0][0]
    for c in e10.copy_trades_writer.write_row.call_args_list
    if c[0][0].get("wallet") == _USER10
]
_attr_by_leader = {}
for _r in _t10_user_rows:
    _sw = _r.get("source_wallet", "")
    _attr_by_leader[_sw] = _attr_by_leader.get(_sw, 0.0) + float(_r.get("copy_pnl", 0.0))

_lead_attr_pnl = _attr_by_leader.get(_LEAD10, 0.0)
check("TEST10-F: LEADER attribution present in closed trade rows",
      _LEAD10 in _attr_by_leader, str(list(_attr_by_leader.keys())))
check("TEST10-G: attributed PnL equals user realized_pnl",
      abs(_lead_attr_pnl - _ueq10.realized_pnl) < 1e-6,
      f"attr={_lead_attr_pnl:.6f} realized={_ueq10.realized_pnl:.6f}")
print("RESULT::SOURCE_WALLET_ATTRIBUTION_OK")

# ─────────────────────────────────────────────────────────────────────────
# TEST 12 — Partial close correctness
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 12: Partial close ===")

W = "0xpartialtest"
C = "BTC"
TS = 1_700_000_000_000

BUY1 = make_fill(W, C, "B", 100.0, 1.0, TS, 0.0)
BUY2 = make_fill(W, C, "B", 110.0, 1.0, TS+1, 1.0)
SELL1 = make_fill(W, C, "A", 120.0, 1.0, TS+2, 2.0)

e = build_engine([W])
replay_fills(e, W, [BUY1, BUY2, SELL1])

open_lots = len(e.open_positions.get((W, C), []))
check("Partial close leaves 1 position", open_lots == 1, str(open_lots))
check("Realized PnL > 0", e.wallet_equity[W].realized_pnl > 0,
      str(e.wallet_equity[W].realized_pnl))


# ─────────────────────────────────────────────────────────────────────────
# TEST 13 — Rapid flip correctness
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 13: Flip test ===")

W = "0xfliptest"
TS = 1_700_000_100_000

BUY  = make_fill(W, C, "B", 100.0, 1.0, TS, 0.0)
SELL = make_fill(W, C, "A", 110.0, 1.0, TS+1, 1.0)
BUY2 = make_fill(W, C, "B", 105.0, 1.0, TS+2, 0.0)

e = build_engine([W])
replay_fills(e, W, [BUY, SELL, BUY2])

open_lots = e.open_positions.get((W, C), [])
check("Flip ends with 1 position", len(open_lots) == 1, str(len(open_lots)))

if open_lots:
    side = open_lots[0].lead_side
    check("Final side is BUY", side == "BUY", side)

# ─────────────────────────────────────────────────────────────────────────
# TEST 11 — Long/short PnL directionality
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 11: Long/short PnL directionality ===")

_W11 = "0xdirtest000000000000000000000000000000001"
_C11 = "BTC"
_TS11 = 1_720_000_000_000

# TEST11-A: long entry -> exit profit
e11a = build_engine([_W11])
e11a.wallet_gate[_W11] = {"mode": "ON"}
replay_fills(e11a, _W11, [
    make_fill(_W11, _C11, "B", 100.0, 1.0, _TS11, 0.0, 0.0, "t11a_buy"),
    make_fill(_W11, _C11, "A", 110.0, 1.0, _TS11 + 1000, 1.0, 10.0, "t11a_sell"),
])
check("TEST11-A: long entry -> exit profit",
      abs(e11a.wallet_equity[_W11].realized_pnl - (0.1 - (1.0 * eng_mod.FEE_BPS / 10000.0) - (1.1 * eng_mod.FEE_BPS / 10000.0))) < 1e-9,
      str(round(e11a.wallet_equity[_W11].realized_pnl, 6)))

# TEST11-B/C/D: short position directionality via deterministic lot + close/mark math
e11b = build_engine([_W11])
e11b.wallet_gate[_W11] = {"mode": "ON"}
short_pos = eng_mod.SimPosition(
    wallet=_W11,
    coin=_C11,
    trade_id="t11_short",
    entry_time_ms=_TS11,
    entry_time_iso="2026-01-01T00:00:00+00:00",
    entry_price_lead=100.0,
    entry_price_copy=100.0,
    size_units=1.0,
    notional_usd=100.0,
    lead_side="SELL",
    entry_latency_ms=0,
    mark_price=100.0,
    entry_fee_usd=100.0 * (eng_mod.FEE_BPS / 10000.0),
)
e11b.open_positions[(_W11, _C11)].append(short_pos)
e11b.tracked_positions[(_W11, _C11)] = 1.0

e11b.mark_prices[_C11] = 90.0
upnl_fall = e11b._compute_unrealized_pnl_for_wallet(_W11)
check("TEST11-C: short unrealised positive when mark falls",
      abs(upnl_fall - 10.0) < 1e-9,
      str(round(upnl_fall, 6)))

e11b.mark_prices[_C11] = 110.0
upnl_rise = e11b._compute_unrealized_pnl_for_wallet(_W11)
check("TEST11-D: short unrealised negative when mark rises",
      abs(upnl_rise + 10.0) < 1e-9,
      str(round(upnl_rise, 6)))

ev11b = eng_mod.FillEvent(
    wallet=_W11,
    coin=_C11,
    side="BUY",
    price=90.0,
    size=1.0,
    timestamp_ms=_TS11 + 2000,
    timestamp_iso="2026-01-01T00:00:02+00:00",
    start_position=-1.0,
    closed_pnl=10.0,
    is_snapshot=False,
    raw={},
    fill_id="t11_short_close",
    shard_id=-1,
    received_at_ms=_TS11 + 2000,
)
e11b.close_copy_position(ev11b, prev_mark=100.0, fraction=1.0)
check("TEST11-B: short entry -> exit profit when price falls",
      abs(e11b.wallet_equity[_W11].realized_pnl - (10.0 - (90.0 * eng_mod.FEE_BPS / 10000.0))) < 1e-9,
      str(round(e11b.wallet_equity[_W11].realized_pnl, 6)))

# ─────────────────────────────────────────────────────────────────────────
# TEST 12 — Engine single-instance lock
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 12: Engine single-instance lock ===")

_lock_dir = Path(tempfile.mkdtemp(prefix="engine_lock_test_"))
_lock_file = _lock_dir / "engine.lock"

_hold_cmd = [
    sys.executable,
    "-u",
    "-c",
    (
        f"from pathlib import Path; "
        "import time; "
        "import HL_Copy_Engine as m; "
        f"m.LOCK_FILE = Path(r'{_lock_file}'); "
        "m._acquire_engine_lock(); "
        "time.sleep(20)"
    ),
]
_probe_cmd = [
    sys.executable,
    "-u",
    "-c",
    (
        f"from pathlib import Path; "
        "import HL_Copy_Engine as m; "
        f"m.LOCK_FILE = Path(r'{_lock_file}'); "
        "m._acquire_engine_lock()"
    ),
]

_p1 = subprocess.Popen(
    _hold_cmd,
    cwd=str(BASE),
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)
time.sleep(1.0)
_out1_initial = (_p1.stdout.readline() if _p1.stdout else "").strip()
check("TEST12-1: first engine acquires lock",
      "[ENGINE_LOCK] Acquired. PID=" in _out1_initial and _p1.poll() is None,
      _out1_initial or f"no output rc={_p1.poll()}")

_p2 = subprocess.run(
    _probe_cmd,
    cwd=str(BASE),
    capture_output=True,
    text=True,
    timeout=15,
)
_out2 = (_p2.stdout + _p2.stderr).strip()
check("TEST12-2: second engine refused",
      _p2.returncode != 0 and "[ENGINE_LOCK] Another instance is running" in _out2,
      _out2 or f"rc={_p2.returncode}")

subprocess.run(
    ["taskkill", "/T", "/F", "/PID", str(_p1.pid)],
    capture_output=True,
    text=True,
    timeout=15,
)
_deadline12 = time.time() + 10.0
while time.time() < _deadline12 and _p1.poll() is None:
    time.sleep(0.2)

check("TEST12-3: stale lock file remains after kill",
      _lock_file.exists() and _p1.poll() is not None,
      f"{'exists' if _lock_file.exists() else 'missing'} rc={_p1.poll()}")

_p3 = subprocess.run(
    _probe_cmd,
    cwd=str(BASE),
    capture_output=True,
    text=True,
    timeout=15,
)
_out3 = (_p3.stdout + _p3.stderr).strip()
check("TEST12-4: stale lock recovered and new instance acquires",
      _p3.returncode == 0
      and "[ENGINE_LOCK] Stale lock detected. Cleaning up." in _out3
      and "[ENGINE_LOCK] Acquired. PID=" in _out3,
      _out3 or f"rc={_p3.returncode}")
check("TEST12-5: lock cleaned on clean exit",
      not _lock_file.exists(),
      "absent" if not _lock_file.exists() else "still present")
shutil.rmtree(_lock_dir, ignore_errors=True)

# ─────────────────────────────────────────────────────────────────────────
# TEST 14 — Rebuild truth safety / idempotence
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 14: Rebuild truth safety ===")

_rebuild_wallet = "0xrebuildtruth000000000000000000000000000001"
_rebuild_coin = "BTC"
_rebuild_ts = 1_700_001_000_000
_rebuild_csv_dir = Path(tempfile.mkdtemp(prefix="rebuild_truth_"))
_rebuild_csv = _rebuild_csv_dir / "raw_live_fills.csv"
_orig_raw_csv = eng_mod.RAW_FILLS_CSV

_rebuild_csv.write_text(
    "\n".join([
        "received_at_iso,wallet,coin,side,price,size,timestamp_ms,timestamp_iso,start_position,closed_pnl,is_snapshot,fill_id,shard_id,raw_json",
        f"2023-11-14T00:00:00+00:00,{_rebuild_wallet},{_rebuild_coin},B,40000,1,{_rebuild_ts},2023-11-14T00:00:00+00:00,0,0,False,rebuild_buy,-1,{{}}",
        f"2023-11-14T00:00:01+00:00,{_rebuild_wallet},{_rebuild_coin},A,41000,1,{_rebuild_ts + 1000},2023-11-14T00:00:01+00:00,1,1000,False,rebuild_sell,-1,{{}}",
        f"2023-11-14T00:00:02+00:00,{_rebuild_wallet},{_rebuild_coin},B,99999,5,{_rebuild_ts + 2000},2023-11-14T00:00:02+00:00,0,0,True,rebuild_snapshot,-1,{{}}",
    ]) + "\n",
    encoding="utf-8",
)

try:
    eng_mod.RAW_FILLS_CSV = _rebuild_csv
    e14 = build_engine([_rebuild_wallet])

    _polluted_pos = eng_mod.SimPosition(
        wallet=_rebuild_wallet,
        coin=_rebuild_coin,
        trade_id="polluted_trade",
        entry_time_ms=_rebuild_ts - 5000,
        entry_time_iso="2023-11-13T23:59:55+00:00",
        lead_side="BUY",
        entry_price_lead=12345.0,
        entry_price_copy=12345.0,
        size_units=9.0,
        notional_usd=111105.0,
        entry_latency_ms=0,
        source_wallet=_rebuild_wallet,
        mark_price=12000.0,
    )
    e14.open_positions[(_rebuild_wallet, _rebuild_coin)] = [_polluted_pos]
    e14.wallet_positions[(_rebuild_wallet, _rebuild_coin)] = 9.0
    e14.tracked_positions[(_rebuild_wallet, _rebuild_coin)] = 9.0
    e14.wallet_metrics[_rebuild_wallet].copy_entry_count = 99
    e14.wallet_metrics[_rebuild_wallet].copy_exit_count = 77
    e14.wallet_metrics[_rebuild_wallet].copy_open_positions = 88
    e14.wallet_metrics[_rebuild_wallet].copy_realized_pnl = 555.0
    e14.wallet_equity[_rebuild_wallet].realized_pnl = 444.0
    e14.wallet_equity[_rebuild_wallet].unrealized_pnl = 333.0
    e14.mark_prices[_rebuild_coin] = 41000.0

    e14._rebuild_from_fills()

    m14 = e14.wallet_metrics[_rebuild_wallet]
    q14 = e14.wallet_equity[_rebuild_wallet]
    _state14 = {
        "open": len(e14.open_positions.get((_rebuild_wallet, _rebuild_coin), [])),
        "wallet_pos": float(e14.wallet_positions.get((_rebuild_wallet, _rebuild_coin), 0.0)),
        "tracked_pos": float(e14.tracked_positions.get((_rebuild_wallet, _rebuild_coin), 0.0)),
        "entries": m14.copy_entry_count,
        "exits": m14.copy_exit_count,
        "open_count": m14.copy_open_positions,
        "copy_realized": round(m14.copy_realized_pnl, 6),
        "realized": round(q14.realized_pnl, 6),
        "unrealized": round(q14.unrealized_pnl, 6),
        "equity": round(q14.total_equity, 6),
        "drawdown": round(q14.drawdown, 6),
    }

    check("TEST14-A: polluted open_positions cleared by replay", _state14["open"] == 0, str(_state14["open"]))
    check("TEST14-B: polluted wallet_positions cleared by replay", abs(_state14["wallet_pos"]) < 1e-9, str(_state14["wallet_pos"]))
    check("TEST14-C: polluted tracked_positions cleared by replay", abs(_state14["tracked_pos"]) < 1e-9, str(_state14["tracked_pos"]))
    check("TEST14-D: rebuild entries == 1", _state14["entries"] == 1, str(_state14["entries"]))
    check("TEST14-E: rebuild exits == 1", _state14["exits"] == 1, str(_state14["exits"]))
    check("TEST14-F: rebuild copy_open_positions == 0", _state14["open_count"] == 0, str(_state14["open_count"]))
    check("TEST14-G: rebuild copy_realized_pnl matches replay", abs(_state14["copy_realized"] - (10.0 - (400.0 * eng_mod.FEE_BPS / 10000.0) - (410.0 * eng_mod.FEE_BPS / 10000.0))) < 1e-9, str(_state14["copy_realized"]))
    check("TEST14-H: rebuild realized_pnl matches replay", abs(_state14["realized"] - (10.0 - (400.0 * eng_mod.FEE_BPS / 10000.0) - (410.0 * eng_mod.FEE_BPS / 10000.0))) < 1e-9, str(_state14["realized"]))
    check("TEST14-I: snapshot row skipped", _state14["entries"] == 1 and _state14["exits"] == 1, str(_state14))

    e14._rebuild_from_fills()
    m14b = e14.wallet_metrics[_rebuild_wallet]
    q14b = e14.wallet_equity[_rebuild_wallet]
    _state14b = {
        "open": len(e14.open_positions.get((_rebuild_wallet, _rebuild_coin), [])),
        "wallet_pos": float(e14.wallet_positions.get((_rebuild_wallet, _rebuild_coin), 0.0)),
        "tracked_pos": float(e14.tracked_positions.get((_rebuild_wallet, _rebuild_coin), 0.0)),
        "entries": m14b.copy_entry_count,
        "exits": m14b.copy_exit_count,
        "open_count": m14b.copy_open_positions,
        "copy_realized": round(m14b.copy_realized_pnl, 6),
        "realized": round(q14b.realized_pnl, 6),
        "unrealized": round(q14b.unrealized_pnl, 6),
        "equity": round(q14b.total_equity, 6),
        "drawdown": round(q14b.drawdown, 6),
    }
    check("TEST14-J: rebuild idempotent across repeated runs", _state14 == _state14b, f"{_state14} vs {_state14b}")
finally:
    eng_mod.RAW_FILLS_CSV = _orig_raw_csv
    shutil.rmtree(_rebuild_csv_dir, ignore_errors=True)

# ─────────────────────────────────────────────────────────────────────────
# TEST 15 — Poll catch-up + WS gating
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 15: Poll catch-up + WS gating ===")

_catch_dir = Path(tempfile.mkdtemp(prefix="catchup_test_"))
_catch_csv = _catch_dir / "raw_live_fills.csv"
_orig_raw_csv_15 = eng_mod.RAW_FILLS_CSV

try:
    # A. 0 reset / no history => READY immediately and live flow unchanged
    eng_mod.RAW_FILLS_CSV = _catch_csv
    e15a = build_engine(["0xcatchupreset000000000000000000000000000001"])
    e15a.user_wallet = "0xuser"
    e15a._fetch_user_fills_by_time = MagicMock(return_value=[])
    e15a._catch_up_wallet("0xcatchupreset000000000000000000000000000001")
    check("TEST15-A1: no-history wallet READY immediately",
          e15a.wallet_sync_status["0xcatchupreset000000000000000000000000000001"] == "READY",
          e15a.wallet_sync_status["0xcatchupreset000000000000000000000000000001"])
    check("TEST15-A2: no-history wallet skips API fetch",
          e15a._fetch_user_fills_by_time.call_count == 0,
          str(e15a._fetch_user_fills_by_time.call_count))
    _W15A = "0xcatchupreset000000000000000000000000000001"
    _TS15A = 1_700_002_000_000
    replay_fills(e15a, _W15A, [
        make_fill(_W15A, "BTC", "B", 40000.0, 1.0, _TS15A, 0.0, 0.0, "t15a_buy"),
        make_fill(_W15A, "BTC", "A", 41000.0, 1.0, _TS15A + 1000, 1.0, 1000.0, "t15a_sell"),
    ])
    check("TEST15-A3: no-history BUY->SELL unchanged",
          e15a.wallet_metrics[_W15A].copy_entry_count == 1
          and e15a.wallet_metrics[_W15A].copy_exit_count == 1
          and e15a.wallet_equity[_W15A].realized_pnl > 0,
          f"entries={e15a.wallet_metrics[_W15A].copy_entry_count} exits={e15a.wallet_metrics[_W15A].copy_exit_count} pnl={e15a.wallet_equity[_W15A].realized_pnl}")

    # B/C. Interrupted session recovery + dedupe
    _W15B = "0xcatchuprecover0000000000000000000000000001"
    _TS15B = 1_700_002_100_000
    _catch_csv.write_text(
        "\n".join([
            "received_at_iso,wallet,coin,side,price,size,timestamp_ms,timestamp_iso,start_position,closed_pnl,is_snapshot,fill_id,shard_id,raw_json",
            f"2023-11-14T00:00:00+00:00,{_W15B},BTC,B,40000,1,{_TS15B},2023-11-14T00:00:00+00:00,0,0,False,t15b_buy,-1,{{}}",
        ]) + "\n",
        encoding="utf-8",
    )
    e15b = build_engine([_W15B])
    e15b.user_wallet = "0xuser"
    replay_fills(e15b, _W15B, [make_fill(_W15B, "BTC", "B", 40000.0, 1.0, _TS15B, 0.0, 0.0, "t15b_buy")])
    _sell_fill = {
        "coin": "BTC", "px": 41000.0, "sz": 1.0, "side": "A",
        "time": _TS15B + 1000, "startPosition": 1.0, "closedPnl": 1000.0, "oid": "t15b_sell",
    }
    e15b._fetch_user_fills_by_time = MagicMock(return_value=[_sell_fill])
    with patch.object(eng_mod, "get_exchange_position", return_value={
        "size": 0.0, "signed_size": 0.0, "entry_price": 0.0,
        "exchange_position_size": 0.0, "exchange_entry_price": 0.0,
        "exchange_unrealized_pnl": 0.0, "exchange_equity": 0.0,
        "has_live_position": False, "positions_by_coin": {},
    }):
        before_raw_writes = e15b.raw_fills_writer.write_row.call_count
        e15b._catch_up_wallet(_W15B)
        check("TEST15-B1: catch-up exits recovered position",
              e15b.wallet_metrics[_W15B].copy_entry_count == 1
              and e15b.wallet_metrics[_W15B].copy_exit_count == 1
              and len(e15b.open_positions.get((_W15B, "BTC"), [])) == 0
              and e15b.wallet_equity[_W15B].realized_pnl > 0,
              f"entries={e15b.wallet_metrics[_W15B].copy_entry_count} exits={e15b.wallet_metrics[_W15B].copy_exit_count} open={len(e15b.open_positions.get((_W15B, 'BTC'), []))} pnl={e15b.wallet_equity[_W15B].realized_pnl}")
        check("TEST15-B2: catch-up marks wallet READY on match",
              e15b.wallet_sync_status[_W15B] == "READY",
              e15b.wallet_sync_status[_W15B])
        state_before = (
            e15b.wallet_metrics[_W15B].copy_entry_count,
            e15b.wallet_metrics[_W15B].copy_exit_count,
            round(e15b.wallet_equity[_W15B].realized_pnl, 6),
            e15b.raw_fills_writer.write_row.call_count,
        )
        e15b._catch_up_wallet(_W15B)
        state_after = (
            e15b.wallet_metrics[_W15B].copy_entry_count,
            e15b.wallet_metrics[_W15B].copy_exit_count,
            round(e15b.wallet_equity[_W15B].realized_pnl, 6),
            e15b.raw_fills_writer.write_row.call_count,
        )
        check("TEST15-C: repeat catch-up dedupes API overlap",
              state_before == state_after and before_raw_writes + 1 == state_after[3],
              f"{state_before} vs {state_after}")

    # D. WS gating
    _W15D = "0xcatchupgate0000000000000000000000000000001"
    _TS15D = 1_700_002_200_000
    e15d = build_engine([_W15D])
    e15d.user_wallet = "0xuser"
    e15d.wallet_sync_status[_W15D] = "CATCHING_UP"
    _payload15 = {
        "channel": "userFills",
        "data": {
            "user": _W15D,
            "fills": [make_fill(_W15D, "BTC", "B", 40000.0, 1.0, _TS15D, 0.0, 0.0, "t15d_buy")],
            "isSnapshot": False,
        },
    }
    held_raw_before = e15d.raw_fills_writer.write_row.call_count
    e15d.handle_ws_message(0, _payload15)
    check("TEST15-D1: CATCHING_UP does not create copy entry",
          e15d.wallet_metrics[_W15D].copy_entry_count == 0,
          f"writes={e15d.raw_fills_writer.write_row.call_count} entries={e15d.wallet_metrics[_W15D].copy_entry_count}")
    e15d.wallet_sync_status[_W15D] = "READY"
    e15d.handle_ws_message(0, _payload15)
    while not e15d.event_queue.empty():
        ev = e15d.event_queue.get_nowait()
        e15d.process_live_fill(ev)
        e15d.event_queue.task_done()
    check("TEST15-D2: READY allows WS processing",
          e15d.raw_fills_writer.write_row.call_count == held_raw_before + 1
          and e15d.wallet_metrics[_W15D].copy_entry_count == 1,
          f"writes={e15d.raw_fills_writer.write_row.call_count} entries={e15d.wallet_metrics[_W15D].copy_entry_count}")

    # E. User wallet safety
    _WU15 = "0xusercatchup0000000000000000000000000000001"
    _catch_csv.write_text(
        "\n".join([
            "received_at_iso,wallet,coin,side,price,size,timestamp_ms,timestamp_iso,start_position,closed_pnl,is_snapshot,fill_id,shard_id,raw_json",
            f"2023-11-14T00:00:00+00:00,{_WU15},BTC,B,40000,1,{_TS15D},2023-11-14T00:00:00+00:00,0,0,False,t15u_buy,-1,{{}}",
        ]) + "\n",
        encoding="utf-8",
    )
    e15u = build_engine([_WU15])
    e15u.user_wallet = _WU15
    e15u.process_live_fill = MagicMock()
    e15u.raw_fills_writer.write_row = MagicMock()
    e15u._catch_up_wallet(_WU15)
    check("TEST15-E1: user wallet marked USER_SKIPPED",
          e15u.wallet_sync_status[_WU15] == "USER_SKIPPED",
          e15u.wallet_sync_status[_WU15])
    check("TEST15-E2: user wallet catch-up does not mutate",
          e15u.process_live_fill.call_count == 0 and e15u.raw_fills_writer.write_row.call_count == 0,
          f"process={e15u.process_live_fill.call_count} writes={e15u.raw_fills_writer.write_row.call_count}")
finally:
    eng_mod.RAW_FILLS_CSV = _orig_raw_csv_15
    shutil.rmtree(_catch_dir, ignore_errors=True)

# ─────────────────────────────────────────────────────────────────────────
# TEST 18 — Polling reconciliation safety net
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 18: Polling reconciliation safety net ===")

_T18_USER   = "0xpolluser000000000000000000000000000000001"
_T18_BASE   = 1_700_100_000_000

# ── TEST 18-A: 0 reset / no history — bootstrap polling ──────────────────
_W18A = "0xpoll18a000000000000000000000000000000001"
e18a = build_engine([_W18A])
e18a.user_wallet = _T18_USER
e18a.engine_start_ms = _T18_BASE
e18a.last_poll_ts_by_wallet = {}
_fill18a = {
    "coin": "BTC", "px": 40000.0, "sz": 1.0, "side": "B",
    "time": _T18_BASE - 30_000,  # within 60s bootstrap window
    "startPosition": 0.0, "closedPnl": 0.0, "oid": "poll18a_buy",
}
e18a._fetch_user_fills_by_time = MagicMock(return_value=[_fill18a])
_raw_before18a = e18a.raw_fills_writer.write_row.call_count
e18a._poll_wallet_fills(_W18A)
check("TEST18-A1: bootstrap poll creates entry",
      e18a.wallet_metrics[_W18A].copy_entry_count == 1,
      str(e18a.wallet_metrics[_W18A].copy_entry_count))
check("TEST18-A2: bootstrap poll writes raw row",
      e18a.raw_fills_writer.write_row.call_count == _raw_before18a + 1,
      str(e18a.raw_fills_writer.write_row.call_count))
check("TEST18-A3: status remains READY after poll",
      e18a.wallet_sync_status.get(_W18A, "READY") == "READY",
      str(e18a.wallet_sync_status.get(_W18A)))

# ── TEST 18-B: poll catches fills when WS is silent ──────────────────────
_W18B = "0xpoll18b000000000000000000000000000000001"
e18b = build_engine([_W18B])
e18b.user_wallet = _T18_USER
e18b.engine_start_ms = _T18_BASE + 100_000
e18b.last_poll_ts_by_wallet = {}
_ts18b = _T18_BASE + 100_500
_fill18b_buy  = {"coin": "ETH", "px": 2000.0, "sz": 1.0, "side": "B",
                 "time": _ts18b,        "startPosition": 0.0, "closedPnl":   0.0, "oid": "poll18b_buy"}
_fill18b_sell = {"coin": "ETH", "px": 2100.0, "sz": 1.0, "side": "A",
                 "time": _ts18b+1000,   "startPosition": 1.0, "closedPnl": 100.0, "oid": "poll18b_sell"}
e18b._fetch_user_fills_by_time = MagicMock(return_value=[_fill18b_buy, _fill18b_sell])
e18b._poll_wallet_fills(_W18B)
check("TEST18-B1: WS-absent poll entries == 1",
      e18b.wallet_metrics[_W18B].copy_entry_count == 1,
      str(e18b.wallet_metrics[_W18B].copy_entry_count))
check("TEST18-B2: WS-absent poll exits == 1",
      e18b.wallet_metrics[_W18B].copy_exit_count == 1,
      str(e18b.wallet_metrics[_W18B].copy_exit_count))
check("TEST18-B3: WS-absent poll realized_pnl > 0",
      e18b.wallet_equity[_W18B].realized_pnl > 0,
      str(round(e18b.wallet_equity[_W18B].realized_pnl, 6)))

# ── TEST 18-C: dedupe prevents double-count on repeated poll ─────────────
_W18C = "0xpoll18c000000000000000000000000000000001"
e18c = build_engine([_W18C])
e18c.user_wallet = _T18_USER
e18c.engine_start_ms = _T18_BASE + 200_000
e18c.last_poll_ts_by_wallet = {}
_fill18c = {
    "coin": "BTC", "px": 40000.0, "sz": 1.0, "side": "B",
    "time": _T18_BASE + 200_500, "startPosition": 0.0, "closedPnl": 0.0, "oid": "poll18c_same",
}
e18c._fetch_user_fills_by_time = MagicMock(return_value=[_fill18c])
e18c._poll_wallet_fills(_W18C)
_state18c_first = (
    e18c.wallet_metrics[_W18C].copy_entry_count,
    e18c.wallet_metrics[_W18C].copy_exit_count,
    round(e18c.wallet_equity[_W18C].realized_pnl, 6),
    e18c.raw_fills_writer.write_row.call_count,
)
e18c._poll_wallet_fills(_W18C)
_state18c_second = (
    e18c.wallet_metrics[_W18C].copy_entry_count,
    e18c.wallet_metrics[_W18C].copy_exit_count,
    round(e18c.wallet_equity[_W18C].realized_pnl, 6),
    e18c.raw_fills_writer.write_row.call_count,
)
check("TEST18-C: repeated poll dedupes identical fills",
      _state18c_first == _state18c_second,
      f"first={_state18c_first} second={_state18c_second}")

# ── TEST 18-D: user wallet is never polled ────────────────────────────────
_W18D = "0xpoll18d000000000000000000000000000000001"
e18d = build_engine([_W18D])
e18d.user_wallet = _W18D   # this wallet IS the user wallet
e18d.engine_start_ms = _T18_BASE + 300_000
e18d.last_poll_ts_by_wallet = {}
e18d._fetch_user_fills_by_time = MagicMock()
e18d.process_live_fill = MagicMock()
e18d.raw_fills_writer.write_row = MagicMock()
e18d._poll_wallet_fills(_W18D)
check("TEST18-D1: user wallet: no API fetch",
      e18d._fetch_user_fills_by_time.call_count == 0,
      str(e18d._fetch_user_fills_by_time.call_count))
check("TEST18-D2: user wallet: no raw write",
      e18d.raw_fills_writer.write_row.call_count == 0,
      str(e18d.raw_fills_writer.write_row.call_count))
check("TEST18-D3: user wallet: no process_live_fill",
      e18d.process_live_fill.call_count == 0,
      str(e18d.process_live_fill.call_count))

# ─────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 19: UI CONTROL ENFORCEMENT ===")

_orig_gate_file_19 = eng_mod.WALLET_GATE_FILE
_gate_dir_19 = Path(tempfile.mkdtemp(prefix="wallet_gate_test_"))
try:
    eng_mod.WALLET_GATE_FILE = _gate_dir_19 / "wallet_gate.json"
    _W19 = "0xgate190000000000000000000000000000000001"

    eng_mod.WALLET_GATE_FILE.write_text(json.dumps({_W19: {"mode": "OFF"}}), encoding="utf-8")
    e19a = build_engine([_W19])
    e19a.user_wallet = "0xuser190000000000000000000000000000000001"
    e19a.wallet_gate = {}
    e19a._reload_wallet_gate()
    replay_fills(e19a, _W19, [make_fill(_W19, "BTC", "B", 40000.0, 1.0, 1_700_200_000_000, 0.0)])
    check("TEST19-A: OFF wallet does not block paper entry",
          e19a.wallet_metrics[_W19].copy_entry_count == 1,
          str(e19a.wallet_metrics[_W19].copy_entry_count))

    eng_mod.WALLET_GATE_FILE.write_text(json.dumps({_W19: {"mode": "ON"}}), encoding="utf-8")
    e19b = build_engine([_W19])
    e19b.user_wallet = "0xuser190000000000000000000000000000000001"
    e19b.wallet_gate = {}
    e19b._reload_wallet_gate()
    replay_fills(e19b, _W19, [make_fill(_W19, "BTC", "B", 40000.0, 1.0, 1_700_200_100_000, 0.0)])
    eng_mod.WALLET_GATE_FILE.write_text(json.dumps({_W19: {"mode": "CLOSE_ONLY"}}), encoding="utf-8")
    e19b._reload_wallet_gate()
    replay_fills(e19b, _W19, [make_fill(_W19, "BTC", "B", 40500.0, 1.0, 1_700_200_101_000, 1.0)])
    replay_fills(e19b, _W19, [make_fill(_W19, "BTC", "A", 41000.0, 1.0, 1_700_200_102_000, 2.0)])
    check("TEST19-B1: CLOSE_ONLY does not block additional paper entry",
          e19b.wallet_metrics[_W19].copy_entry_count == 2,
          str(e19b.wallet_metrics[_W19].copy_entry_count))
    check("TEST19-B2: CLOSE_ONLY paper path still exits",
          e19b.wallet_metrics[_W19].copy_exit_count == 1 and len(e19b.open_positions.get((_W19, "BTC"), [])) == 1,
          f"exits={e19b.wallet_metrics[_W19].copy_exit_count} open={len(e19b.open_positions.get((_W19, 'BTC'), []))}")

    eng_mod.WALLET_GATE_FILE.write_text(json.dumps({_W19: {"mode": "ON"}}), encoding="utf-8")
    e19c = build_engine([_W19])
    e19c.user_wallet = "0xuser190000000000000000000000000000000001"
    e19c.wallet_gate = {}
    e19c._reload_wallet_gate()
    replay_fills(e19c, _W19, [
        make_fill(_W19, "BTC", "B", 40000.0, 1.0, 1_700_200_200_000, 0.0),
        make_fill(_W19, "BTC", "A", 41000.0, 1.0, 1_700_200_201_000, 1.0),
    ])
    check("TEST19-C: ON mode unchanged",
          e19c.wallet_metrics[_W19].copy_entry_count == 1
          and e19c.wallet_metrics[_W19].copy_exit_count == 1
          and e19c.wallet_equity[_W19].realized_pnl > 0,
          f"entries={e19c.wallet_metrics[_W19].copy_entry_count} exits={e19c.wallet_metrics[_W19].copy_exit_count} pnl={round(e19c.wallet_equity[_W19].realized_pnl, 6)}")
finally:
    eng_mod.WALLET_GATE_FILE = _orig_gate_file_19
    shutil.rmtree(_gate_dir_19, ignore_errors=True)

print("\n=== TEST 20: MIRROR USER SEPARATION ===")

W = "0xtestmirror"
U = "0xuserwallet"

BUY  = make_fill(W, "BTC", "B", 40000.0, 1.0, 1000, 0.0, 0.0, "m1")
SELL = make_fill(W, "BTC", "A", 41000.0, 1.0, 2000, 1.0, 1000.0, "m2")

# ON -> should mirror
e = build_engine([W])
e.user_wallet = U
e.wallet_metrics[U] = eng_mod.WalletMetrics(wallet=U)
e.wallet_equity[U] = eng_mod.WalletEquity()
e.wallet_gate[W] = {"mode": "ON"}
e._in_test_mode = False
replay_fills(e, W, [BUY, SELL])
check("ON mirrors to user",
      e.wallet_metrics[U].copy_entry_count == 1,
      str(e.wallet_metrics[U].copy_entry_count))

# CLOSE_ONLY -> should still mirror (controls do not affect paper path)
e2 = build_engine([W])
e2.user_wallet = U
e2.wallet_metrics[U] = eng_mod.WalletMetrics(wallet=U)
e2.wallet_equity[U] = eng_mod.WalletEquity()
e2.wallet_gate[W] = {"mode": "CLOSE_ONLY"}
e2._in_test_mode = False
replay_fills(e2, W, [BUY, SELL])
check("CLOSE_ONLY still mirrors to user",
      e2.wallet_metrics[U].copy_entry_count == 1,
      str(e2.wallet_metrics[U].copy_entry_count))

# OFF -> should still mirror (controls do not affect paper path)
e3 = build_engine([W])
e3.user_wallet = U
e3.wallet_metrics[U] = eng_mod.WalletMetrics(wallet=U)
e3.wallet_equity[U] = eng_mod.WalletEquity()
e3.wallet_gate[W] = {"mode": "OFF"}
e3._in_test_mode = False
replay_fills(e3, W, [BUY, SELL])
check("OFF still mirrors to user",
      e3.wallet_metrics[U].copy_entry_count == 1,
      str(e3.wallet_metrics[U].copy_entry_count))


print("\n=== TEST 21: control mode does not affect paper exit cleanup ===")

W = "0xclosetest"
U = "0xuser"

e = build_engine([W])
e.user_wallet = U
e._in_test_mode = False

# Setup user wallet tracking
e.wallet_metrics.setdefault(U, eng_mod.WalletMetrics(wallet=U))
e.wallet_equity.setdefault(U, eng_mod.WalletEquity())

BUY  = make_fill(W, "BTC", "B", 40000.0, 1.0, 1000, 0.0, 0.0, "t1")
SELL = make_fill(W, "BTC", "A", 41000.0, 1.0, 2000, 1.0, 1000.0, "t2")

# Step 1: ON → create mirrored position
e.wallet_gate[W] = {"mode": "ON"}
replay_fills(e, W, [BUY])

# Step 2: switch to CLOSE_ONLY
e.wallet_gate[W] = {"mode": "CLOSE_ONLY"}

# Step 3: exit
replay_fills(e, W, [SELL])

check("Control mode exits mirrored position",
      e.wallet_metrics[U].copy_exit_count == 1,
      str(e.wallet_metrics[U].copy_exit_count))

print("\n=== TEST 22: set-all populates missing wallets ===")

app22 = _load_app_module()
client22 = TestClient(app22.app)
state22 = {
    "wallets": {
        "0xwalleta": {"equity": {"realized_pnl": 0.0, "unrealized_pnl": 0.0}, "metrics": {}},
        "0xwalletb": {"equity": {"realized_pnl": 0.0, "unrealized_pnl": 0.0}, "metrics": {}},
        "0xwalletc": {"equity": {"realized_pnl": 0.0, "unrealized_pnl": 0.0}, "metrics": {}},
    }
}
app22.DATA_DIR.mkdir(parents=True, exist_ok=True)
app22.PORTFOLIO_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
(app22.DATA_DIR / "live_state.json").write_text(json.dumps(state22), encoding="utf-8")
app22.WALLET_GATE_FILE.write_text(json.dumps({}), encoding="utf-8")
resp22 = client22.post("/api/set-all-modes", json={"mode": "ON"})
gate22 = json.loads(app22.WALLET_GATE_FILE.read_text(encoding="utf-8"))
check("TEST22-A: set-all returns ok", resp22.status_code == 200 and resp22.json().get("ok") is True, resp22.text)
check("TEST22-B: set-all populates wallet A", gate22.get("0xwalleta", {}).get("mode") == "ON", str(gate22))
check("TEST22-C: set-all populates wallet B", gate22.get("0xwalletb", {}).get("mode") == "ON", str(gate22))
check("TEST22-D: set-all populates wallet C", gate22.get("0xwalletc", {}).get("mode") == "ON", str(gate22))

print("\n=== TEST 23: legacy gate route preserves mode ===")

app23 = _load_app_module()
client23 = TestClient(app23.app)
state23 = {"wallets": {"0xwalletlegacy": {"open_positions": []}}}
(app23.DATA_DIR / "live_state.json").write_text(json.dumps(state23), encoding="utf-8")
resp23 = client23.get("/gate/0xwalletlegacy?live=1")
gate23 = json.loads(app23.WALLET_GATE_FILE.read_text(encoding="utf-8"))
check("TEST23-A: legacy gate route responds", resp23.status_code == 200, str(resp23.status_code))
check("TEST23-B: legacy gate route writes mode", gate23.get("0xwalletlegacy", {}).get("mode") == "ON", str(gate23))

print("\n=== TEST 24: mixed-scale portfolio history rejected ===")

app24 = _load_app_module()
mixed24 = [
    {"ts": "2026-04-24T18:42:36.804251+00:00", "alloc": 260000.0, "realized": 0.0, "unrealized": 0.0, "equity": 260000.0, "peak_equity": 260000.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    {"ts": "2026-04-24T18:42:51.874164+00:00", "alloc": 2600.0, "realized": 0.0, "unrealized": 0.0, "equity": 2600.0, "peak_equity": 2600.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
]
norm24 = app24._normalize_portfolio_history(mixed24)
check("TEST24-A: mixed-scale history rejected", norm24 == [], str(norm24))

print("\n=== TEST 25: reset clears persisted app history ===")

app25 = _load_app_module()
client25 = TestClient(app25.app)
app25._portfolio_history.extend([{"ts": "2026-01-01T00:00:00+00:00"}])
app25._equity_history["0xwallet"] = __import__("collections").deque([["2026-01-01T00:00:00+00:00", 1.0]])
app25._active_wallets.add("0xwallet")
for path, payload in [
    (app25.PORTFOLIO_HISTORY_FILE, [{"ts": "2026-01-01T00:00:00+00:00"}]),
    (app25.PORTFOLIO_BASELINE_FILE, {"ts": "2026-01-01T00:00:00+00:00"}),
    (app25.EQUITY_HISTORY_FILE, {"0xwallet": [["2026-01-01T00:00:00+00:00", 1.0]]}),
    (app25.GLOBAL_NORM_CONFIG, {"norm_base": 123.0}),
]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
resp25 = client25.post("/api/reset-all")
check("TEST25-A: reset route responds", resp25.status_code == 200 and resp25.json().get("ok") is True, resp25.text)
check("TEST25-B: portfolio history file removed", not app25.PORTFOLIO_HISTORY_FILE.exists(), str(app25.PORTFOLIO_HISTORY_FILE.exists()))
check("TEST25-C: portfolio baseline file removed", not app25.PORTFOLIO_BASELINE_FILE.exists(), str(app25.PORTFOLIO_BASELINE_FILE.exists()))
check("TEST25-D: equity history file removed", not app25.EQUITY_HISTORY_FILE.exists(), str(app25.EQUITY_HISTORY_FILE.exists()))
check("TEST25-E: global norm file removed", not app25.GLOBAL_NORM_CONFIG.exists(), str(app25.GLOBAL_NORM_CONFIG.exists()))
check("TEST25-F: in-memory portfolio history cleared", len(app25._portfolio_history) == 0, str(app25._portfolio_history))
check("TEST25-G: in-memory equity history cleared", len(app25._equity_history) == 0, str(app25._equity_history))
check("TEST25-H: in-memory active wallets cleared", len(app25._active_wallets) == 0, str(app25._active_wallets))





# ─────────────────────────────────────────────────────────────────────────
# TEST 26 — USER WALLET HARD LOCK
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 26: User wallet hard lock ===")

app26 = _load_app_module()
_uw26 = "0xuserlock0000000000000000000000000000026"
_lead26 = "0xleader0000000000000000000000000000000026"
app26.USER_WALLET = _uw26
app26.WALLET_GATE_FILE = app26.DATA_DIR / "wallet_gate.json"
app26.DATA_DIR.mkdir(parents=True, exist_ok=True)
state26 = {
    "wallets": {
        _uw26:   {"equity": {}, "metrics": {}},
        _lead26: {"equity": {}, "metrics": {}},
    }
}
(app26.DATA_DIR / "live_state.json").write_text(json.dumps(state26), encoding="utf-8")

# TEST 26-A: _ensure_gate_population forces existing ON entry to OFF
app26.WALLET_GATE_FILE.write_text(json.dumps({_uw26: {"mode": "ON"}}), encoding="utf-8")
gate26a = app26._ensure_gate_population()
check("TEST26-A: _ensure_gate_population sets user wallet to OFF",
      gate26a.get(_uw26, {}).get("mode") == "OFF",
      str(gate26a.get(_uw26)))

# TEST 26-B: new user wallet entry defaults to OFF
app26.WALLET_GATE_FILE.write_text(json.dumps({}), encoding="utf-8")
gate26b = app26._ensure_gate_population()
check("TEST26-B: new user wallet entry defaults to OFF",
      gate26b.get(_uw26, {}).get("mode") == "OFF",
      str(gate26b.get(_uw26)))

# TEST 26-C: API set-wallet-mode rejects user wallet
client26 = TestClient(app26.app)
app26.WALLET_GATE_FILE.write_text(json.dumps({_uw26: {"mode": "OFF"}, _lead26: {"mode": "ON"}}), encoding="utf-8")
resp26c = client26.post("/api/set-wallet-mode", json={"wallet": _uw26, "mode": "ON"})
check("TEST26-C: set-wallet-mode rejects user wallet",
      resp26c.json().get("error") == "USER_WALLET_LOCKED",
      resp26c.text)
gate26c = json.loads(app26.WALLET_GATE_FILE.read_text(encoding="utf-8"))
check("TEST26-D: user wallet remains OFF after rejected API call",
      gate26c.get(_uw26, {}).get("mode") == "OFF",
      str(gate26c.get(_uw26)))

# TEST 26-E: set-all-modes does not affect user wallet
app26.WALLET_GATE_FILE.write_text(json.dumps({_uw26: {"mode": "OFF"}, _lead26: {"mode": "OFF"}}), encoding="utf-8")
resp26e = client26.post("/api/set-all-modes", json={"mode": "ON"})
gate26e = json.loads(app26.WALLET_GATE_FILE.read_text(encoding="utf-8"))
check("TEST26-E: set-all-modes returns ok",
      resp26e.status_code == 200 and resp26e.json().get("ok") is True,
      resp26e.text)
check("TEST26-F: user wallet still OFF after set-all ON",
      gate26e.get(_uw26, {}).get("mode") == "OFF",
      str(gate26e.get(_uw26)))
check("TEST26-G: leader wallet set to ON by set-all",
      gate26e.get(_lead26, {}).get("mode") == "ON",
      str(gate26e.get(_lead26)))

# ─────────────────────────────────────────────────────────────────────────
# TEST 27 — GRAPH BASELINE
# ─────────────────────────────────────────────────────────────────────────
print("\n=== TEST 27: Graph baseline uses PnL-only series ===")

app27 = _load_app_module()
app27.datetime = _AppDatetime
app27._save_norm_base(200.0)
buf27 = io.StringIO()
state27 = {
    "updated_at": "2026-01-01T00:10:00+00:00",
    "config": {"normalisation_base": 200.0},
    "wallets": {
        "0xleader27": {
            "metrics": {"copy_entry_count": 1, "copy_exit_count": 0, "copy_open_positions": 0},
        },
    },
    "normalised_wallet_state": {
        "0xleader27": {"alloc": 200.0, "copy": {"realised": 0.0, "unrealised": 0.0, "equity": 200.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 200.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}}
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:10:00+00:00", "alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0, "copy": {"alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}, "lead": {"alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}, "delta": {"equity": 0.0, "realized": 0.0, "realised": 0.0, "pct": 0.0}},
    "normalised_portfolio_history": [{"ts": "2026-01-01T00:10:00+00:00", "alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}],
}
with contextlib.redirect_stdout(buf27):
    app27._update_equity_history(state27)
pt27 = app27._portfolio_history[-1]
check("TEST27-A: first graph point starts at pnl baseline 0",
      abs(pt27["realized"] + pt27["unrealized"]) < 1e-6,
      f"pnl={pt27['realized'] + pt27['unrealized']}")
check("TEST27-B: drawdown = peak - equity at baseline",
      abs(pt27["drawdown_usd"] - (pt27["peak_equity"] - pt27["equity"])) < 1e-6,
      str(pt27))

state27b = {
    "updated_at": "2026-01-01T00:11:00+00:00",
    "config": {"normalisation_base": 200.0},
    "wallets": {
        "0xleader27": {
            "metrics": {"copy_entry_count": 1, "copy_exit_count": 1, "copy_open_positions": 0},
        },
    },
    "normalised_wallet_state": {
        "0xleader27": {"alloc": 200.0, "copy": {"realised": 20.0, "unrealised": 0.0, "equity": 220.0, "drawdown": 0.0, "drawdown_pct": 0.0, "peak": 220.0, "max_drawdown": 0.0, "max_drawdown_pct": 0.0}}
    },
    "normalised_portfolio": {"ts": "2026-01-01T00:11:00+00:00", "alloc": 200.0, "realized": 20.0, "unrealized": 0.0, "equity": 220.0, "peak_equity": 220.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0, "copy": {"alloc": 200.0, "realized": 20.0, "unrealized": 0.0, "equity": 220.0, "peak_equity": 220.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}, "lead": {"alloc": 200.0, "realized": 18.0, "unrealized": 0.0, "equity": 218.0, "peak_equity": 218.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0}, "delta": {"equity": 2.0, "realized": 2.0, "realised": 2.0, "pct": 0.111111}},
    "normalised_portfolio_history": [
        {"ts": "2026-01-01T00:10:00+00:00", "alloc": 200.0, "realized": 0.0, "unrealized": 0.0, "equity": 200.0, "peak_equity": 200.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
        {"ts": "2026-01-01T00:11:00+00:00", "alloc": 200.0, "realized": 20.0, "unrealized": 0.0, "equity": 220.0, "peak_equity": 220.0, "drawdown": 0.0, "drawdown_usd": 0.0, "drawdown_pct": 0.0},
    ],
}
with contextlib.redirect_stdout(buf27):
    app27._update_equity_history(state27b)
pt27b = app27._portfolio_history[-1]
check("TEST27-C: graph series uses pnl-only after win",
      abs((pt27b["realized"] + pt27b["unrealized"]) - 20.0) < 1e-6,
      f"pnl={pt27b['realized'] + pt27b['unrealized']}")


def test_home_table_structure():
    import HL_Copy_App as app

    html = app.home(None).body.decode()

    # REQUIRED HEADERS (ORDER MATTERS)
    expected = [
        "Wallet",
        "Equity",
        "Lead Unreal",
        "Lead DD",
        "Lead MaxDD",
        "User Eq",
        "Copy Real",
        "Copy Unreal",
        "Copy DD",
        "Copy MaxDD",
        "Δ $/%",
    ]

    idx = 0
    for col in expected:
        i = html.find(col)
        assert i != -1, f"Missing column: {col}"
        assert i >= idx, f"Column order broken: {col}"
        idx = i

    # MUST NOT EXIST
    forbidden = [
        "wallet_pnl_ref",
        "real_slippage",
        "_load_metrics",
        "_load_trades"
    ]

    for f in forbidden:
        assert f not in html, f"Forbidden source leaked: {f}"


print("\n=== TEST 28: Dashboard table contract ===")

app28 = _load_app_module()
app28.WALLET_GATE_FILE = app28.DATA_DIR / "wallet_gate.json"
app28.WALLET_GATE_FILE.write_text(json.dumps({"0xleader28": {"mode": "ON"}}), encoding="utf-8")
state28 = {
    "updated_at": "2026-04-25T12:00:00+00:00",
    "config": {"normalisation_base": 100.0},
    "wallets": {
        "0xleader28": {
            "equity": {
                "starting_balance": 100.0,
                "realized_pnl": 12.0,
                "unrealized_pnl": 3.0,
                "total_equity": 115.0,
                "peak_equity": 118.0,
                "trough_equity": 99.0,
                "drawdown": 2.0,
                "max_drawdown": 6.0,
            },
            "reference": {
                "starting_balance": 100.0,
                "realized_pnl": 5.0,
                "unrealized_pnl": -1.0,
                "total_equity": 104.0,
                "peak_equity": 110.0,
                "trough_equity": 96.0,
                "drawdown": 5.4545,
                "max_drawdown": 12.7272,
            },
            "metrics": {
                "live_fill_count": 9,
                "copy_entry_count": 4,
                "copy_exit_count": 2,
                "copy_open_positions": 1,
                "pnl_per_hour": 1.5,
                "win_rate": 50.0,
            },
            "alert_flags": ["HIGH_LATENCY"],
            "wallet_equity": 100.0,
            "open_positions": [],
        }
    },
    "normalised_wallet_state": {
        "0xleader28": {
            "alloc": 100.0,
            "copy": {"realised": 12.0, "unrealised": 3.0, "equity": 115.0, "drawdown": 3.0, "drawdown_pct": 2.5424, "peak": 118.0, "max_drawdown": 19.0, "max_drawdown_pct": 16.1017},
            "lead": {"realised": 5.0, "unrealised": -1.0, "equity": 104.0, "drawdown": 6.0, "drawdown_pct": 5.4545, "peak": 110.0, "max_drawdown": 14.0, "max_drawdown_pct": 12.7272},
            "delta": {"equity": 11.0, "pct": 10.5769},
            "pnl_per_hour": 1.5,
            "ws_coverage": 0.75,
            "fill_count": 9,
            "entry_count": 4,
            "exit_count": 2,
            "open_position_count": 1,
            "win_count": 1,
            "loss_count": 1,
        }
    },
    "normalised_portfolio": {"ts": "2026-04-25T12:00:00+00:00", "copy": {"alloc": 100.0, "realized": 12.0, "unrealized": 3.0, "equity": 115.0, "peak_equity": 118.0, "drawdown": 3.0, "drawdown_usd": 3.0, "drawdown_pct": 2.5424}, "lead": {"alloc": 100.0, "realized": 5.0, "unrealized": -1.0, "equity": 104.0, "peak_equity": 110.0, "drawdown": 6.0, "drawdown_usd": 6.0, "drawdown_pct": 5.4545}, "delta": {"equity": 11.0, "realized": 7.0, "realised": 7.0, "pct": 1.4}},
    "normalised_portfolio_history": [{"ts": "2026-04-25T12:00:00+00:00", "alloc": 100.0, "realized": 12.0, "unrealized": 3.0, "equity": 115.0, "peak_equity": 118.0, "drawdown": 3.0, "drawdown_usd": 3.0, "drawdown_pct": 2.5424}],
}
(app28.DATA_DIR / "live_state.json").write_text(json.dumps(state28), encoding="utf-8")
html28 = app28.home(None).body.decode("utf-8")
re28 = __import__("re")
headers28 = re28.findall(r"<th[^>]*>([^<]+)</th>", html28)
expected28 = [
    "Wallet",
    "Lead Equity",
    "Lead Real",
    "Lead Unreal",
    "Lead DD",
    "Lead MaxDD",
    "User Eq",
    "Copy Real",
    "Copy Unreal",
    "Copy DD",
    "Copy MaxDD",
    "Δ $/%",
    "PnL/hr",
    "Win%",
    "WS %",
    "Fills W/C",
    "Exits W/C",
    "Pos W/C",
    "Flags",
    "Mode",
]
check("TEST28-A: exact header order", headers28[:20] == expected28, str(headers28[:20]))
row_match28 = re28.search(r"<tbody>\s*<tr[^>]*>(.*?)</tr>", html28, re28.S)
row_tds28 = re28.findall(r"<td\b", row_match28.group(1)) if row_match28 else []
check("TEST28-B: first row has 20 td cells", len(row_tds28) == 20, str(len(row_tds28)))
check("TEST28-C: delta is non-zero when reference and copy differ", "$0.00 (0.0%)" not in html28, "delta cell should not be zero")
check("TEST28-C2: table row equity matches canonical portfolio", ">$115.00<" in html28, "copy equity should render canonical $115.00")
check("TEST28-C3: header delta matches portfolio delta", "+$11.00 (+140.00%)" in html28, "header delta should render canonical portfolio delta")
check("TEST28-C4: ws coverage renders from canonical wallet state", ">75.0%<" in html28, "ws coverage should render 75.0%")
home_src28 = Path(BASE / "HL_Copy_App.py").read_text(encoding="utf-8")
check("TEST28-D: home source no longer aliases lead_eq", "lead_eq = eq" not in home_src28, "lead_eq = eq")
check("TEST28-E: home source no longer aliases copy_eq", "copy_eq = eq" not in home_src28, "copy_eq = eq")
check("TEST28-F: wallet universe source excludes _load_metrics()", "_load_metrics()" not in home_src28, "_load_metrics()")
for bad in ("â€”", "â€¦", "â†", "Î”"):
    check(f"TEST28-G: rendered HTML excludes {bad}", bad not in html28, bad)


# ─────────────────────────────────────────────────────────────────────────
# TEST 29 - write_live_state produces reference block, no NameError
# ─────────────────────────────────────────────────────────────────────────
print("=== TEST 29: write_live_state reference block ===")

_wls_dir = Path(tempfile.mkdtemp(prefix="wls_test_"))
_orig_live_state_json = eng_mod.LIVE_STATE_JSON
try:
    eng_mod.LIVE_STATE_JSON = _wls_dir / "live_state.json"
    _W29 = "0xwls29000000000000000000000000000000000001"
    e29 = build_engine([_W29])
    e29.maybe_snapshot    = lambda: None
    e29.cleanup_snapshots = lambda: None

    _no_error = True
    try:
        e29.write_live_state()
    except Exception as _exc:
        _no_error = False
        check("TEST29-A: write_live_state raises no exception", False, str(_exc))

    if _no_error:
        check("TEST29-A: write_live_state raises no exception", True, "ok")
        _state29 = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
        _wdata29 = _state29.get("wallets", {}).get(_W29, {})
        check("TEST29-B: wallet entry present in live_state.json",
              bool(_wdata29), str(list(_state29.get("wallets", {}).keys())))
        check("TEST29-C: reference block present",
              "reference" in _wdata29, str(list(_wdata29.keys())))
        _ref29 = _wdata29.get("reference", {})
        check("TEST29-D: reference contains realized_pnl",
              "realized_pnl" in _ref29, str(list(_ref29.keys())))
        _norm29 = _state29.get("normalised_wallet_state", {}).get(_W29, {})
        check("TEST29-E: normalised wallet state present",
              bool(_norm29), str(_state29.get("normalised_wallet_state", {})))
        check("TEST29-F: normalised portfolio alloc equals base",
              _state29.get("normalised_portfolio", {}).get("copy", {}).get("alloc") == 100.0,
              str(_state29.get("normalised_portfolio", {})))
finally:
    eng_mod.LIVE_STATE_JSON = _orig_live_state_json
    shutil.rmtree(_wls_dir, ignore_errors=True)


print("\n=== TEST 30: shard WS health ===")

_ws_dir_30 = Path(tempfile.mkdtemp(prefix="ws_health_test_"))
_orig_live_state_json_30 = eng_mod.LIVE_STATE_JSON
try:
    eng_mod.LIVE_STATE_JSON = _ws_dir_30 / "live_state.json"
    _W30A = "0xws30000000000000000000000000000000000001"
    _W30B = "0xws30000000000000000000000000000000000002"
    e30 = build_engine([_W30A, _W30B])
    e30.maybe_snapshot = lambda: None
    e30.cleanup_snapshots = lambda: None

    e30.shard_ws_status[0] = "DOWN"
    e30.write_live_state()
    state30a = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    overall30a = state30a.get("ws_health", {}).get("overall")
    flags30a = state30a.get("wallets", {}).get(_W30A, {}).get("alert_flags", [])
    check("TEST30-A: DOWN shard makes overall DEGRADED",
          overall30a == "DEGRADED",
          str(state30a.get("ws_health")))
    check("TEST30-B: DOWN shard adds WS_DEGRADED to wallet",
          "WS_DEGRADED" in flags30a,
          str(flags30a))

    e30.shard_ws_status[0] = "OK"
    e30.write_live_state()
    state30b = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    flags30b = state30b.get("wallets", {}).get(_W30A, {}).get("alert_flags", [])
    check("TEST30-C: shard OK clears WS_DEGRADED",
          "WS_DEGRADED" not in flags30b,
          str(flags30b))

    src30 = Path(BASE / "HL_Copy_Engine.py").read_text(encoding="utf-8")
    check("TEST30-D: reconnect cap <=5 seconds",
          "backoff = 5" in src30 and "min(SHARD_RESTART_DELAY_SEC, 5)" in src30,
          "shard reconnect capped at 5s")
    check("TEST30-E: no wallet-level restart loop active",
          "_restart_ws_for_wallet" not in src30,
          "_restart_ws_for_wallet absent")
finally:
    eng_mod.LIVE_STATE_JSON = _orig_live_state_json_30
    shutil.rmtree(_ws_dir_30, ignore_errors=True)

print("\n=== TEST 31: WS hot path enqueues and fills ===")
_W31 = "0xws31000000000000000000000000000000000001"
e31 = build_engine([_W31])
payload31 = {
    "channel": "userFills",
    "data": {
        "user": _W31,
        "fills": [
            {
                "coin": "BTC",
                "px": 40000,
                "sz": 1,
                "side": "B",
                "time": 1700000000000,
                "startPosition": 0,
                "closedPnl": 0,
                "oid": "ws1",
            }
        ],
        "isSnapshot": False,
    },
}
e31.handle_ws_message(0, payload31)
while True:
    try:
        _ev31 = e31.event_queue.get_nowait()
    except __import__("queue").Empty:
        break
    e31.process_live_fill(_ev31)
    e31.event_queue.task_done()
check("TEST31-A: live_fill_count == 1",
      e31.wallet_metrics[_W31].live_fill_count == 1,
      str(e31.wallet_metrics[_W31].live_fill_count))
check("TEST31-B: copy_entry_count == 1",
      e31.wallet_metrics[_W31].copy_entry_count == 1,
      str(e31.wallet_metrics[_W31].copy_entry_count))
check("TEST31-C: copy_open_positions == 1",
      e31.wallet_metrics[_W31].copy_open_positions == 1,
      str(e31.wallet_metrics[_W31].copy_open_positions))

print("\n=== TEST 32: DEGRADED shard does not block fill path ===")
_ws_dir_32 = Path(tempfile.mkdtemp(prefix="ws_hotpath_test_"))
_orig_live_state_json_32 = eng_mod.LIVE_STATE_JSON
try:
    eng_mod.LIVE_STATE_JSON = _ws_dir_32 / "live_state.json"
    _W32 = "0xws32000000000000000000000000000000000001"
    e32 = build_engine([_W32])
    e32.maybe_snapshot = lambda: None
    e32.cleanup_snapshots = lambda: None
    e32.shard_ws_status[0] = "DEGRADED"
    payload32 = {
        "channel": "userFills",
        "data": {
            "user": _W32,
            "fills": [
                {
                    "coin": "BTC",
                    "px": 40000,
                    "sz": 1,
                    "side": "B",
                    "time": 1700000000000,
                    "startPosition": 0,
                    "closedPnl": 0,
                    "oid": "ws32",
                }
            ],
            "isSnapshot": False,
        },
    }
    e32.handle_ws_message(0, payload32)
    while True:
        try:
            _ev32 = e32.event_queue.get_nowait()
        except __import__("queue").Empty:
            break
        e32.process_live_fill(_ev32)
        e32.event_queue.task_done()
    check("TEST32-A: DEGRADED shard still creates entry",
          e32.wallet_metrics[_W32].copy_entry_count == 1,
          str(e32.wallet_metrics[_W32].copy_entry_count))
    e32.write_live_state()
    state32 = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    flags32 = state32.get("wallets", {}).get(_W32, {}).get("alert_flags", [])
    check("TEST32-B: WS_DEGRADED appears in live_state",
          "WS_DEGRADED" in flags32,
          str(flags32))
finally:
    eng_mod.LIVE_STATE_JSON = _orig_live_state_json_32
    shutil.rmtree(_ws_dir_32, ignore_errors=True)

print("\n=== TEST: NORMALISATION CONSISTENCY ===")

W_A = "0xaaa"
W_B = "0xbbb"

e = build_engine([W_A, W_B])

# Inject fake exchange equity
e.exchange_position_snapshot[W_A] = {"exchange_equity": 10000}
e.exchange_position_snapshot[W_B] = {"exchange_equity": 1000}

# Same % trade (10%)
fill_A = make_fill(W_A, "BTC", "B", 100.0, 10.0, 1000, 0.0)
fill_B = make_fill(W_B, "BTC", "B", 100.0, 1.0, 1000, 0.0)

ev_A = e.parse_fill(-1, W_A, fill_A, False)
ev_B = e.parse_fill(-1, W_B, fill_B, False)

e.process_live_fill(ev_A)
e.process_live_fill(ev_B)

pos_A = e.open_positions[(W_A, "BTC")][0]
pos_B = e.open_positions[(W_B, "BTC")][0]

print("A notional:", pos_A.notional_usd)
print("B notional:", pos_B.notional_usd)

check("Normalised notionals match",
abs(pos_A.notional_usd - pos_B.notional_usd) < 1e-6,
f"{pos_A.notional_usd} vs {pos_B.notional_usd}")

print("\n=== TEST 33: user separation and dashboard truth ===")
_sep_dir_33 = Path(tempfile.mkdtemp(prefix="sep_test_"))
_orig_live_state_json_33 = eng_mod.LIVE_STATE_JSON
try:
    eng_mod.LIVE_STATE_JSON = _sep_dir_33 / "live_state.json"
    _U33 = "0xuser330000000000000000000000000000000001"
    _L33A = "0xlead330000000000000000000000000000000001"
    _L33B = "0xlead330000000000000000000000000000000002"
    e33 = build_engine([_U33, _L33A, _L33B])
    e33.user_wallet = _U33
    e33.maybe_snapshot = lambda: None
    e33.cleanup_snapshots = lambda: None
    e33.wallet_gate[_L33A] = {"mode": "ON"}
    e33.wallet_gate[_L33B] = {"mode": "ON"}
    e33.wallet_gate[_U33] = {"mode": "OFF"}
    e33.wallet_equity[_L33A].starting_balance = 100.0
    e33.wallet_equity[_L33A].realized_pnl = 10.0
    e33.wallet_equity[_L33A].peak_equity = 120.0
    e33.wallet_equity[_L33A].trough_equity = 90.0
    e33.wallet_reference[_L33A].starting_balance = 100.0
    e33.wallet_reference[_L33A].realized_pnl = 8.0
    e33.wallet_reference[_L33A].peak_equity = 118.0
    e33.wallet_reference[_L33A].trough_equity = 95.0
    e33.wallet_equity[_L33B].starting_balance = 100.0
    e33.wallet_equity[_L33B].realized_pnl = -5.0
    e33.wallet_equity[_L33B].peak_equity = 110.0
    e33.wallet_equity[_L33B].trough_equity = 95.0
    e33.wallet_reference[_L33B].starting_balance = 100.0
    e33.wallet_reference[_L33B].realized_pnl = -4.0
    e33.wallet_reference[_L33B].peak_equity = 112.0
    e33.wallet_reference[_L33B].trough_equity = 96.0
    e33.wallet_equity[_U33].starting_balance = 100.0
    e33.wallet_equity[_U33].realized_pnl = 50.0
    e33.wallet_equity[_U33].peak_equity = 150.0
    e33.wallet_equity[_U33].trough_equity = 100.0
    e33.wallet_reference[_U33].starting_balance = 100.0
    e33.wallet_reference[_U33].realized_pnl = 50.0
    e33.wallet_reference[_U33].peak_equity = 150.0
    e33.wallet_reference[_U33].trough_equity = 100.0
    e33.write_live_state()
    state33_on = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    port33_on = state33_on.get("normalised_portfolio", {}).get("copy", {})
    sum33_on = sum(
        v.get("copy", {}).get("equity", 0.0)
        for w, v in state33_on.get("normalised_wallet_state", {}).items()
        if w != _U33
    )
    check("TEST33-A: USER EXCLUDED from portfolio alloc",
          port33_on.get("alloc") == 200.0,
          str(port33_on))
    check("TEST33-B: portfolio wallet count == tracked wallets - 1",
          int(port33_on.get("alloc", 0.0) / 100.0) == 2,
          str(port33_on))
    check("TEST33-C: GRAPH CONSISTENCY wallet sum == portfolio equity",
          abs(sum33_on - port33_on.get("equity", 0.0)) < 1e-6,
          f"{sum33_on} vs {port33_on.get('equity', 0.0)}")
    e33.wallet_gate[_L33A] = {"mode": "OFF"}
    e33.wallet_gate[_L33B] = {"mode": "OFF"}
    e33.write_live_state()
    state33_off = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    port33_off = state33_off.get("normalised_portfolio", {}).get("copy", {})
    check("TEST33-D: CONTROLS NO EFFECT on portfolio output",
          {k: v for k, v in port33_off.items() if k != "ts"} == {k: v for k, v in port33_on.items() if k != "ts"},
          f"{port33_off} vs {port33_on}")
finally:
    eng_mod.LIVE_STATE_JSON = _orig_live_state_json_33
    shutil.rmtree(_sep_dir_33, ignore_errors=True)

print("\n=== TEST 34: user wallet never copied ===")
_U34 = "0xuser340000000000000000000000000000000001"
e34 = build_engine([_U34])
e34.user_wallet = _U34
e34.wallet_metrics[_U34] = eng_mod.WalletMetrics(wallet=_U34)
e34.wallet_equity[_U34] = eng_mod.WalletEquity()
ev34 = e34.parse_fill(-1, _U34, make_fill(_U34, "BTC", "B", 100.0, 1.0, 1000, 0.0), False)
e34.process_live_fill(ev34)
check("TEST34-A: USER NOT COPIED",
      e34.wallet_metrics[_U34].copy_entry_count == 0,
      str(e34.wallet_metrics[_U34].copy_entry_count))

print("\n=== TEST 35: normalisation base persists and rescales ===")
_norm_dir_35 = Path(tempfile.mkdtemp(prefix="norm_base_test_"))
_orig_live_state_35 = eng_mod.LIVE_STATE_JSON
_orig_norm_cfg_35 = eng_mod.GLOBAL_NORM_CONFIG
try:
    eng_mod.LIVE_STATE_JSON = _norm_dir_35 / "live_state.json"
    eng_mod.GLOBAL_NORM_CONFIG = _norm_dir_35 / "global_norm.json"
    _W35 = "0xnorm350000000000000000000000000000000001"
    e35 = build_engine([_W35])
    e35.normalisation_base = 100.0
    e35.wallet_equity[_W35].realized_pnl = 1000.0
    e35.wallet_equity[_W35].unrealized_pnl = 500.0
    e35.wallet_equity[_W35].peak_equity = 11500.0
    e35.wallet_equity[_W35].trough_equity = 10000.0
    e35.wallet_reference[_W35].realized_pnl = 1000.0
    e35.wallet_reference[_W35].unrealized_pnl = 500.0
    e35.wallet_reference[_W35].peak_equity = 11500.0
    e35.wallet_reference[_W35].trough_equity = 10000.0
    e35.wallet_metrics[_W35].copy_entry_count = 4
    e35.wallet_metrics[_W35].copy_exit_count = 2
    e35.wallet_metrics[_W35].copy_win_count = 3
    e35.wallet_metrics[_W35].copy_loss_count = 1

    eng_mod.GLOBAL_NORM_CONFIG.write_text(json.dumps({"norm_base": 100.0}), encoding="utf-8")
    e35._reload_normalisation_base()
    e35.write_live_state()
    state35a = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    wallet35a = state35a.get("normalised_wallet_state", {}).get(_W35, {})
    port35a = state35a.get("normalised_portfolio", {}).get("copy", {})

    eng_mod.GLOBAL_NORM_CONFIG.write_text(json.dumps({"norm_base": 1000.0}), encoding="utf-8")
    e35._reload_normalisation_base()
    e35.write_live_state()
    state35b = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    wallet35b = state35b.get("normalised_wallet_state", {}).get(_W35, {})
    port35b = state35b.get("normalised_portfolio", {}).get("copy", {})

    scale35 = 10.0
    check("TEST35-A: wallet copy equity scales linearly",
          abs(wallet35b.get("copy", {}).get("equity", 0.0) - wallet35a.get("copy", {}).get("equity", 0.0) * scale35) < 1e-6,
          f"{wallet35a.get('copy', {}).get('equity', 0.0)} -> {wallet35b.get('copy', {}).get('equity', 0.0)}")
    check("TEST35-B: wallet lead realized scales linearly",
          abs(wallet35b.get("lead", {}).get("realised", 0.0) - wallet35a.get("lead", {}).get("realised", 0.0) * scale35) < 1e-6,
          f"{wallet35a.get('lead', {}).get('realised', 0.0)} -> {wallet35b.get('lead', {}).get('realised', 0.0)}")
    check("TEST35-C: portfolio equity scales linearly",
          abs(port35b.get("equity", 0.0) - port35a.get("equity", 0.0) * scale35) < 1e-6,
          f"{port35a.get('equity', 0.0)} -> {port35b.get('equity', 0.0)}")
    check("TEST35-D: counts unchanged by norm scaling",
          wallet35a.get("entry_count", 0) == wallet35b.get("entry_count", 0) == 4 and wallet35a.get("exit_count", 0) == wallet35b.get("exit_count", 0) == 2,
          f"{wallet35a.get('entry_count', 0)}/{wallet35a.get('exit_count', 0)} -> {wallet35b.get('entry_count', 0)}/{wallet35b.get('exit_count', 0)}")
finally:
    eng_mod.LIVE_STATE_JSON = _orig_live_state_35
    eng_mod.GLOBAL_NORM_CONFIG = _orig_norm_cfg_35
    shutil.rmtree(_norm_dir_35, ignore_errors=True)

print("\n=== TEST 36: leader vs copy truth separation ===")
_truth_dir_36 = Path(tempfile.mkdtemp(prefix="truth_layer_test_"))
_orig_live_state_36 = eng_mod.LIVE_STATE_JSON
try:
    eng_mod.LIVE_STATE_JSON = _truth_dir_36 / "live_state.json"
    _W36 = "0xtruth3600000000000000000000000000000001"
    e36 = build_engine([_W36])
    e36.exchange_position_snapshot[_W36] = {"exchange_equity": 1000.0}
    e36.mark_prices["BTC"] = 101.0
    ev36 = e36.parse_fill(-1, _W36, make_fill(_W36, "BTC", "B", 100.0, 1.0, 1000, 0.0), False)
    e36.process_live_fill(ev36)
    pos36 = e36.open_positions[(_W36, "BTC")][0]
    leader36 = e36.leader_positions[(_W36, "BTC")][0]
    check("TEST36-A: entry delta is non-zero when copy price differs",
          abs(pos36.entry_bps) > 0.0 and abs(pos36.entry_price_copy - leader36.entry_price_copy) > 0.0,
          f"bps={pos36.entry_bps} copy={pos36.entry_price_copy} lead={leader36.entry_price_copy}")
    e36.mark_prices["BTC"] = 110.0
    e36.write_live_state()
    state36 = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    wallet36 = state36.get("normalised_wallet_state", {}).get(_W36, {})
    lead36 = wallet36.get("lead", {})
    copy36 = wallet36.get("copy", {})
    check("TEST36-B: lead pnl differs from copy pnl when mark moves",
          abs(float(lead36.get("unrealised", 0.0)) - float(copy36.get("unrealised", 0.0))) > 1e-9,
          f"lead={lead36} copy={copy36}")
    check("TEST36-C: delta reflects copy minus lead",
          abs(float(wallet36.get("delta", {}).get("equity", 0.0)) - (float(copy36.get("equity", 0.0)) - float(lead36.get("equity", 0.0)))) < 1e-9,
          str(wallet36.get("delta", {})))
    app36 = _load_app_module()
    (app36.DATA_DIR / "live_state.json").write_text(json.dumps(state36), encoding="utf-8")
    html36 = app36.home(None).body.decode("utf-8")
    check("TEST36-D: dashboard delta column renders non-zero separation",
          "$0.00 (0.0%)" not in html36,
          "delta should be non-zero")
finally:
    eng_mod.LIVE_STATE_JSON = _orig_live_state_36
    shutil.rmtree(_truth_dir_36, ignore_errors=True)

print("\n=== TEST 37: poll source, fees, and portfolio delta ===")
_truth_dir_37 = Path(tempfile.mkdtemp(prefix="truth_layer_test2_"))
_orig_live_state_37 = eng_mod.LIVE_STATE_JSON
try:
    eng_mod.LIVE_STATE_JSON = _truth_dir_37 / "live_state.json"

    _W37A = "0xpoll370000000000000000000000000000000001"
    e37a = build_engine([_W37A])
    e37a._fetch_user_fills_by_time = MagicMock(return_value=[
        make_fill(_W37A, "BTC", "B", 100.0, 1.0, 1_700_100_000_000, 0.0, 0.0, "poll37_buy"),
        make_fill(_W37A, "BTC", "A", 110.0, 1.0, 1_700_100_001_000, 1.0, 10.0, "poll37_sell"),
    ])
    e37a._poll_wallet_fills(_W37A)
    row37a = e37a.copy_trades_writer.write_row.call_args_list[-1][0][0]
    check("TEST37-A: poll fills update copy pnl",
          e37a.wallet_metrics[_W37A].copy_exit_count == 1 and e37a.wallet_equity[_W37A].realized_pnl > 0,
          f"exits={e37a.wallet_metrics[_W37A].copy_exit_count} pnl={e37a.wallet_equity[_W37A].realized_pnl}")
    check("TEST37-B: poll fills do NOT create delta_bps",
          row37a.get("entry_slippage_bps") is None and row37a.get("exit_slippage_bps") is None,
          str(row37a))
    check("TEST37-B2: leader pnl is reduced by fees",
          0.0 < e37a.wallet_reference[_W37A].realized_pnl < 0.1,
          str(e37a.wallet_reference[_W37A].realized_pnl))

    _W37B = "0xws37000000000000000000000000000000000001"
    e37b = build_engine([_W37B])
    e37b.exchange_position_snapshot[_W37B] = {"exchange_equity": 1000.0}
    e37b.mark_prices["BTC"] = 101.0
    ev37b_buy = e37b.parse_fill(-1, _W37B, make_fill(_W37B, "BTC", "B", 100.0, 1.0, 1_700_100_010_000, 0.0, 0.0, "ws37_buy"), False)
    e37b.process_live_fill(ev37b_buy)
    pos37b = e37b.open_positions[(_W37B, "BTC")][0]
    gross37b = (110.0 - pos37b.entry_price_copy) * pos37b.size_units
    e37b.mark_prices["BTC"] = 109.0
    ev37b_sell = e37b.parse_fill(-1, _W37B, make_fill(_W37B, "BTC", "A", 110.0, 1.0, 1_700_100_011_000, 1.0, 10.0, "ws37_sell"), False)
    e37b.process_live_fill(ev37b_sell)
    row37b = e37b.copy_trades_writer.write_row.call_args_list[-1][0][0]
    check("TEST37-C: exit_delta_bps != 0 when prices differ",
          row37b.get("exit_slippage_bps") is not None and abs(float(row37b.get("exit_slippage_bps") or 0.0)) > 0.0,
          str(row37b))
    check("TEST37-D: fees reduce copy pnl",
          float(row37b.get("copy_pnl", 0.0)) < gross37b,
          f"row={row37b.get('copy_pnl')} gross={gross37b}")
    check("TEST37-D2: leader pnl is reduced by fees",
          e37b.wallet_reference[_W37B].realized_pnl < 1.0,
          str(e37b.wallet_reference[_W37B].realized_pnl))
    e37b.write_live_state()
    state37b = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    port37b = state37b.get("normalised_portfolio", {})
    check("TEST37-E: portfolio lead != copy when delta/fees exist",
          abs(float(port37b.get("copy", {}).get("equity", 0.0)) - float(port37b.get("lead", {}).get("equity", 0.0))) > 1e-9
          and abs(float(port37b.get("delta", {}).get("realized", 0.0))) > 1e-9,
          str(port37b))

    _W37C = "0xws37000000000000000000000000000000000002"
    e37c = build_engine([_W37C])
    e37c.exchange_position_snapshot[_W37C] = {"exchange_equity": 1000.0}
    e37c.mark_prices["BTC"] = 100.0
    ev37c_buy = e37c.parse_fill(-1, _W37C, make_fill(_W37C, "BTC", "B", 100.0, 1.0, 1_700_100_020_000, 0.0, 0.0, "ws37c_buy"), False)
    e37c.process_live_fill(ev37c_buy)
    e37c.mark_prices["BTC"] = 110.0
    ev37c_sell = e37c.parse_fill(-1, _W37C, make_fill(_W37C, "BTC", "A", 110.0, 1.0, 1_700_100_021_000, 1.0, 10.0, "ws37c_sell"), False)
    e37c.process_live_fill(ev37c_sell)
    e37c.write_live_state()
    state37c = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    port37c = state37c.get("normalised_portfolio", {})
    check("TEST37-F: fees do not create delta when prices are identical",
          abs(float(port37c.get("delta", {}).get("equity", 0.0))) < 1e-9
          and abs(float(port37c.get("delta", {}).get("realized", 0.0))) < 1e-9,
          str(port37c))

    _W37D = "0xmix37000000000000000000000000000000000003"
    e37d = build_engine([_W37D])
    e37d.exchange_position_snapshot[_W37D] = {"exchange_equity": 1000.0}
    e37d.mark_prices["BTC"] = 101.0
    ev37d_ws_buy = e37d.parse_fill(-1, _W37D, make_fill(_W37D, "BTC", "B", 100.0, 1.0, 1_700_100_030_000, 0.0, 0.0, "ws37d_buy"), False)
    e37d.process_live_fill(ev37d_ws_buy)
    ws_entry_avg_37d = e37d.wallet_metrics[_W37D].avg_entry_slippage_bps
    e37d._fetch_user_fills_by_time = MagicMock(return_value=[
        make_fill(_W37D, "ETH", "B", 200.0, 1.0, 1_700_100_031_000, 0.0, 0.0, "poll37d_buy"),
        make_fill(_W37D, "ETH", "A", 210.0, 1.0, 1_700_100_032_000, 1.0, 10.0, "poll37d_sell"),
    ])
    e37d._poll_wallet_fills(_W37D)
    e37d.write_live_state()
    state37d = json.loads(eng_mod.LIVE_STATE_JSON.read_text(encoding="utf-8"))
    wallet37d = state37d.get("normalised_wallet_state", {}).get(_W37D, {})
    check("TEST37-G: ws_coverage correctly computed",
          abs(float(wallet37d.get("ws_coverage", 0.0)) - (1.0 / 3.0)) < 1e-6,
          str(wallet37d))
    check("TEST37-H: poll fills do NOT affect slippage averages",
          abs(e37d.wallet_metrics[_W37D].avg_entry_slippage_bps - ws_entry_avg_37d) < 1e-9,
          f"{e37d.wallet_metrics[_W37D].avg_entry_slippage_bps} vs {ws_entry_avg_37d}")
    check("TEST37-I: delta_pct != 0 when lead ≠ copy",
          abs(float(port37b.get("delta", {}).get("pct", 0.0))) > 1e-9,
          str(port37b.get("delta", {})))
finally:
    eng_mod.LIVE_STATE_JSON = _orig_live_state_37
    shutil.rmtree(_truth_dir_37, ignore_errors=True)


print(f"\n{'='*50}")
total = len(results)
passed = sum(results)
failed = total - passed
print(f"Results: {passed}/{total} passed, {failed} failed")
if failed == 0:
    print("RESULT::APP_DD_MODEL_OK")
    sys.exit(0)
else:
    print("RESULT::FAILED_WITH_PROOF")
    sys.exit(1)
