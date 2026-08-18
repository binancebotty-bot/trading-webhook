"""
HL_Copy_Engine_SSOT.py

Pure Hyperliquid data harvester / truth manager.

Role boundary:
- Engine owns ONLY factual ingestion, dedupe, ordering, raw position balancing,
  poll health, atomic persistence, deterministic rebuild, and live-capture
  provenance (poll/WS captured vs REBUILD).
- Engine does NOT own copy sizing, normalisation, portfolio modelling, ranking,
  or any user-selected presentation model.

Outputs:
- hl_copy_output/raw_live_fills.csv      canonical append-only real userFillsByTime ledger
- hl_copy_output/engine_truth.json       canonical SSOT snapshot
- hl_copy_output/live_state.json         compatibility mirror of raw truth only
- hl_copy_output/live_wallet_metrics.csv raw health/intake metrics only
- hl_copy_output/exchange_baselines.json baseline offsets for cold-start/new wallets

Run:
    python HL_Copy_Engine_SSOT.py

Environment:
    HL_USER_WALLET=0x...       optional user wallet marker
    HL_POLL_SECONDS=30         optional polling interval
    HL_CLEAN_REBUILD=1         rebuild truth from raw_live_fills.csv on start
"""
from __future__ import annotations

import csv
import json
import os
import queue
import signal
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover
    websocket = None

BASE_DIR = Path(__file__).resolve().parent
MANUAL_WALLETS_FILE = BASE_DIR / "manual_wallets.txt"
OUTPUT_DIR = BASE_DIR / "hl_copy_output"
LIVE_CONFIG_FILE = BASE_DIR / "hl_live_copy_audit" / "live_config.json"
SNAP_DIR = OUTPUT_DIR / "snapshots"
RAW_FILLS_CSV = OUTPUT_DIR / "raw_live_fills.csv"
ENGINE_TRUTH_JSON = OUTPUT_DIR / "engine_truth.json"
LIVE_STATE_JSON = OUTPUT_DIR / "live_state.json"  # compatibility mirror, raw truth only
LIVE_METRICS_CSV = OUTPUT_DIR / "live_wallet_metrics.csv"
LOG_FILE = OUTPUT_DIR / "hl_copy_engine_ssot.log"
LOCK_FILE = BASE_DIR / "engine_ssot.lock"
BASELINE_JSON = OUTPUT_DIR / "exchange_baselines.json"  # baseline offsets; not trades/PnL
WALLET_GATE_FILE = BASE_DIR / "wallet_gate.json"  # visibility only; not modelling

WS_URL = "wss://api.hyperliquid.xyz/ws"
MAX_WALLETS = int(os.getenv("HL_MAX_WALLETS", "0"))  # 0 = unlimited; poll-only mode has no legacy WS wallet cap
WALLETS_PER_SHARD = int(os.getenv("HL_WALLETS_PER_SHARD", "5"))
EVENT_QUEUE_MAXSIZE = int(os.getenv("HL_EVENT_QUEUE_MAXSIZE", "50000"))
SEEN_FILL_IDS_MAX = int(os.getenv("HL_SEEN_FILL_IDS_MAX", "250000"))
POLL_SECONDS = float(os.getenv("HL_POLL_SECONDS", "120"))
STATE_WRITE_INTERVAL_SEC = float(os.getenv("HL_STATE_WRITE_INTERVAL_SEC", "5"))
# userFills can be application-message quiet for long periods. websocket-client
# handles protocol ping/pong; do not churn healthy idle sockets by default.
WS_STALE_MS = int(os.getenv("HL_WS_STALE_MS", "0"))
WS_DATA_STALE_MS = int(os.getenv("HL_WS_DATA_STALE_MS", "0"))
WS_WATCHDOG_INTERVAL_SEC = float(os.getenv("HL_WS_WATCHDOG_INTERVAL_SEC", "5"))
WS_RESTART_COOLDOWN_SEC = float(os.getenv("HL_WS_RESTART_COOLDOWN_SEC", "15"))
POLL_OVERLAP_MS = int(os.getenv("HL_POLL_OVERLAP_MS", "300000"))  # 5m overlap, matching proven universe_builder pattern
POLL_WINDOW_MS = int(os.getenv("HL_POLL_WINDOW_MS", str(24 * 60 * 60 * 1000)))
POLL_MAX_PAGE_ROWS = int(os.getenv("HL_POLL_MAX_PAGE_ROWS", "500"))
POSITION_EPSILON = float(os.getenv("HL_POSITION_EPSILON", "1e-9"))
RECONNECT_BACKOFF_CAP_SEC = float(os.getenv("HL_RECONNECT_BACKOFF_CAP_SEC", "30"))

# Drift recovery must not become a polling loop.
#
# A drift the fill history cannot explain -- a pre-baseline transfer, a
# manual move, a liquidation, dust below the exchange's own reporting -- stays
# unexplained no matter how many times the same window is re-fetched.  This
# engine's own audit counters recorded the consequence: 20,800 recovery
# attempts, 220 recoveries, and 11,020,509 duplicate fill rows fetched and
# discarded.  Every one of those rows cost 20 weight against a 1200/min IP
# allowance shared with two other products.
#
# So a recovery attempt that changes nothing quiesces.  The drift is still
# reported -- truthfully, and with the reason it stopped re-fetching -- but the
# same barren window is not requested again until either new fills actually
# arrive for that wallet or this interval elapses.
DRIFT_RECOVERY_RETRY_SEC = float(os.getenv("HL_DRIFT_RECOVERY_RETRY_SEC", "900"))

# This tracker is an independent product.  It shares no state, no cursor and
# no business logic with the copy runtime or the :8014 proof engine -- only
# the IP-level weight budget, because the exchange meters the IP and not the
# process.  This ceiling binds even with the shared coordinator switched off.
SSOT_WEIGHT_PER_MIN = float(os.getenv("HL_SSOT_WEIGHT_PER_MIN", "150"))

# How long a background read may wait for budget before giving up on this
# cycle.  Waiting is what turns a fixed ceiling into pacing: without it the
# sweep would sprint through the wallet list, be refused for most of it, and
# serve only whichever wallets happen to sit at the front.  This wait happens
# on the tracker's own poll thread and blocks nothing else -- least of all
# another process's execution path, whose reserve this product never claims.
SSOT_ACQUIRE_TIMEOUT_SEC = float(os.getenv("HL_SSOT_ACQUIRE_TIMEOUT_SEC", "30"))

try:
    from hl_rate_guard import guard as _rate_guard
    RATE_GUARD = _rate_guard("ssot", SSOT_WEIGHT_PER_MIN)
except Exception:  # pragma: no cover - never let the guard break the tracker
    RATE_GUARD = None
CLEAN_REBUILD = os.getenv("HL_CLEAN_REBUILD", "0") == "1"
USER_WALLET = os.getenv("HL_USER_WALLET", "").lower()
ENABLE_WS = False

RECORDING_WS_CAPTURED = "WS_CAPTURED"
RECORDING_REBUILD = "REBUILD"

RAW_FILL_FIELDS = [
    "fill_id", "wallet", "coin", "side", "price", "size", "signed_size_delta",
    "start_position", "end_position", "closed_pnl", "fee", "timestamp_ms",
    "timestamp_iso", "received_at_ms", "received_at_iso", "latency_ms",
    "source", "recording_method", "rebuild_reason", "contributes_to_execution_delta",
    "reconstructed", "shard_id", "is_snapshot", "raw_json",
]

METRIC_FIELDS = [
    "wallet", "fill_count", "ws_fill_count", "rebuild_fill_count", "poll_fill_count",
    "snapshot_fill_count", "duplicate_fill_count", "last_fill_ts", "last_source",
    "position_count", "open_notional_raw", "realized_pnl_raw", "fees_raw",
    "ws_status", "ws_stale_ms", "ws_data_stale_ms",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def iso_to_ms(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except Exception:
        return 0


def monitor_start_ms() -> int:
    env_value = os.getenv("HL_MONITOR_START_MS", "")
    if str(env_value).strip():
        return inum(env_value)
    try:
        data = json.loads(LIVE_CONFIG_FILE.read_text(encoding="utf-8-sig"))
    except Exception:
        return 0
    if not isinstance(data, dict):
        return 0
    explicit = inum(data.get("monitor_start_ms"))
    if explicit:
        return explicit
    return iso_to_ms(data.get("updated_at"))


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SNAP_DIR.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        # Stream the encoder directly to disk.  Building the complete JSON text
        # with json.dumps temporarily duplicates the truth snapshot in memory
        # and can kill both the poll and state-writer threads with MemoryError.
        with tmp.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def log(level: str, message: str) -> None:
    ensure_dirs()
    line = f"{utc_now_iso()} | {level.upper()} | {message}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def inum(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def bval(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def load_manual_wallets(path: Path = MANUAL_WALLETS_FILE) -> List[str]:
    wallets: List[str] = []
    seen: Set[str] = set()
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            w = line.strip().lower()
            if not w or w.startswith("#"):
                continue
            if w.startswith("0x") and len(w) == 42 and w not in seen:
                wallets.append(w)
                seen.add(w)
    if USER_WALLET and USER_WALLET not in seen:
        wallets.insert(0, USER_WALLET)
    if not wallets:
        raise SystemExit(f"No wallets found. Create {path} or set HL_USER_WALLET.")
    return wallets if MAX_WALLETS <= 0 else wallets[:MAX_WALLETS]


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def normalize_side(raw_side: Any, signed_size: float = 0.0, start_position: float = 0.0) -> str:
    s = str(raw_side or "").strip().lower()
    if s in {"b", "buy", "long", "bid"}:
        return "BUY"
    if s in {"a", "s", "sell", "short", "ask"}:
        return "SELL"
    if signed_size > 0:
        return "BUY"
    if signed_size < 0:
        return "SELL"
    return "SELL" if start_position > 0 else "BUY"


def raw_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d:
            return d[k]
    return default


def normalise_recording_method(value: Any, source: str = "", is_snapshot: bool = False) -> str:
    """Provenance contract: post-baseline live captures vs rebuild accounting.

    In this Wallet Finder deployment the live feeder is poll-only, so successful
    post-baseline poll fills are live captured evidence too.
    REBUILD fills are accounting/recovery fills only and must not be treated as
    measured execution-delta evidence.
    """
    v = str(value or "").strip().upper()
    if v in {RECORDING_WS_CAPTURED, RECORDING_REBUILD}:
        return v
    src = str(source or "").strip().lower()
    if src in {"ws", "poll"} and not is_snapshot:
        return RECORDING_WS_CAPTURED
    return RECORDING_REBUILD


def recording_contributes_to_execution_delta(recording_method: str) -> bool:
    # Absolute safety rail: REBUILD can never contribute measured execution delta.
    return str(recording_method or "").strip().upper() == RECORDING_WS_CAPTURED


def normalise_execution_delta_flag(value: Any, recording_method: str) -> bool:
    if not recording_contributes_to_execution_delta(recording_method):
        return False
    # Legacy rows may not have the bool column yet; infer True for WS_CAPTURED.
    return bval(value, default=True)


@dataclass(frozen=True)
class RawFill:
    fill_id: str
    wallet: str
    coin: str
    side: str
    price: float
    size: float
    signed_size_delta: float
    start_position: float
    end_position: float
    closed_pnl: float
    fee: float
    timestamp_ms: int
    timestamp_iso: str
    received_at_ms: int
    received_at_iso: str
    latency_ms: int
    source: str
    recording_method: str = RECORDING_REBUILD
    rebuild_reason: str = ""
    contributes_to_execution_delta: bool = False
    reconstructed: bool = False
    shard_id: int = -1
    is_snapshot: bool = False
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_csv_row(self) -> Dict[str, Any]:
        d = asdict(self)
        raw = d.pop("raw", {})
        d["raw_json"] = json.dumps(raw, sort_keys=True)
        return d

    @staticmethod
    def from_csv_row(row: Dict[str, Any]) -> "RawFill":
        raw_text = row.get("raw_json") or "{}"
        try:
            raw = json.loads(raw_text)
        except Exception:
            raw = {}
        is_snapshot = bval(row.get("is_snapshot"), default=False)
        source = str(row.get("source", "unknown")).strip().lower() or "unknown"
        recording_method = normalise_recording_method(row.get("recording_method"), source, is_snapshot)
        return RawFill(
            fill_id=str(row.get("fill_id", "")),
            wallet=str(row.get("wallet", "")).lower(),
            coin=str(row.get("coin", "")).upper(),
            side=str(row.get("side", "BUY")).upper(),
            price=fnum(row.get("price")),
            size=abs(fnum(row.get("size"))),
            signed_size_delta=fnum(row.get("signed_size_delta")),
            start_position=fnum(row.get("start_position")),
            end_position=fnum(row.get("end_position")),
            closed_pnl=fnum(row.get("closed_pnl")),
            fee=fnum(row.get("fee")),
            timestamp_ms=inum(row.get("timestamp_ms")),
            timestamp_iso=str(row.get("timestamp_iso", "")),
            received_at_ms=inum(row.get("received_at_ms")),
            received_at_iso=str(row.get("received_at_iso", "")),
            latency_ms=inum(row.get("latency_ms")),
            source=source,
            recording_method=recording_method,
            rebuild_reason=str(row.get("rebuild_reason", "")),
            contributes_to_execution_delta=normalise_execution_delta_flag(row.get("contributes_to_execution_delta"), recording_method),
            reconstructed=bval(row.get("reconstructed"), default=False),
            shard_id=inum(row.get("shard_id"), -1),
            is_snapshot=is_snapshot,
            raw=raw,
        )


@dataclass
class RawPosition:
    wallet: str
    coin: str
    signed_size: float = 0.0
    avg_entry_price: float = 0.0
    last_price: float = 0.0
    last_timestamp_ms: int = 0

    @property
    def side(self) -> str:
        if self.signed_size > 0:
            return "LONG"
        if self.signed_size < 0:
            return "SHORT"
        return "FLAT"

    @property
    def notional(self) -> float:
        return abs(self.signed_size) * (self.last_price or self.avg_entry_price)

    def apply_fill(self, fill: RawFill) -> None:
        prev = self.signed_size
        delta = fill.signed_size_delta
        new = prev + delta
        # Entry/scale same direction: weighted entry. Reductions keep old entry.
        if prev == 0 or (prev > 0 and delta > 0) or (prev < 0 and delta < 0):
            old_abs = abs(prev)
            add_abs = abs(delta)
            if old_abs + add_abs > 0:
                self.avg_entry_price = ((self.avg_entry_price * old_abs) + (fill.price * add_abs)) / (old_abs + add_abs)
        elif new == 0:
            self.avg_entry_price = 0.0
        elif (prev > 0 > new) or (prev < 0 < new):
            # Flip: remaining position starts at flip fill price.
            self.avg_entry_price = fill.price
        self.signed_size = new
        if abs(self.signed_size) < 1e-12:
            self.signed_size = 0.0
            self.avg_entry_price = 0.0
        self.last_price = fill.price
        self.last_timestamp_ms = fill.timestamp_ms


@dataclass
class RawWallet:
    wallet: str
    realized_pnl_raw: float = 0.0
    fees_raw: float = 0.0
    fill_count: int = 0
    ws_fill_count: int = 0
    poll_fill_count: int = 0
    rebuild_fill_count: int = 0
    snapshot_fill_count: int = 0
    duplicate_fill_count: int = 0
    last_fill_ts: str = ""
    last_source: str = ""


class CsvLedger:
    def __init__(self, path: Path, fields: List[str]) -> None:
        self.path = path
        self.fields = fields
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self.fields).writeheader()

    def append(self, row: Dict[str, Any]) -> None:
        clean = {k: row.get(k, "") for k in self.fields}
        with self.lock, self.path.open("a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self.fields).writerow(clean)

    def replace_all(self, rows: Iterable[Dict[str, Any]]) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with self.lock, tmp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self.fields)
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in self.fields})
        os.replace(tmp, self.path)


class EngineSSOT:
    def __init__(self, wallets: Optional[List[str]] = None) -> None:
        ensure_dirs()
        self.wallets = wallets or load_manual_wallets()
        self.user_wallet = USER_WALLET
        self.stop_event = threading.Event()
        self.event_queue: "queue.Queue[RawFill]" = queue.Queue(maxsize=EVENT_QUEUE_MAXSIZE)
        self.raw_ledger = CsvLedger(RAW_FILLS_CSV, RAW_FILL_FIELDS)
        self.metrics_ledger = CsvLedger(LIVE_METRICS_CSV, METRIC_FIELDS)
        self.seen_ids: Deque[str] = deque(maxlen=SEEN_FILL_IDS_MAX)
        self.seen_set: Set[str] = set()
        self.seen_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.wallets_raw: Dict[str, RawWallet] = {w: RawWallet(w) for w in self.wallets}
        self.positions: Dict[Tuple[str, str], RawPosition] = {}
        self.mark_prices: Dict[str, float] = {}
        self.shard_status: Dict[int, Dict[str, Any]] = {}
        self.ws_apps: Dict[int, Any] = {}
        self.ws_apps_lock = threading.Lock()
        self.audit: Dict[str, int] = defaultdict(int)
        self.last_poll_ts_by_wallet: Dict[str, int] = {}
        self.last_exchange_snapshot_by_wallet: Dict[str, Dict[str, float]] = {}
        self.exchange_baseline_by_wallet: Dict[str, Dict[str, Dict[str, float]]] = self.load_exchange_baselines()
        self.wallet_runtime: Dict[str, Dict[str, Any]] = {}
        self.drift_state_by_wallet: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.threads: List[threading.Thread] = []
        self.engine_start_ms = utc_now_ms()

        startup_fills = self.load_fills_from_ledger()
        self._startup_fill_wallets: Set[str] = {f.wallet for f in startup_fills}
        self.last_ledger_ts_by_wallet: Dict[str, int] = defaultdict(int)
        # Per-wallet memory of the last drift-recovery attempt, so a barren
        # recovery is not repeated every cycle.  Deliberately in-memory only:
        # a restart is new evidence and should re-attempt once.
        self.drift_recovery_gate_by_wallet: Dict[str, Dict[str, Any]] = {}
        self._poll_rotation_offset: int = 0
        for f in startup_fills:
            self.last_ledger_ts_by_wallet[f.wallet] = max(self.last_ledger_ts_by_wallet[f.wallet], f.timestamp_ms)
        for w in self.wallets:
            self.wallet_runtime[w] = {
                "baseline_set": w in self.exchange_baseline_by_wallet,
                "ready": False,
                "ws_live": False,
                "bootstrapped_at_ms": 0,
                "last_poll_reconcile_ms": 0,
            }

        self._load_ledger_seen_ids()
        if CLEAN_REBUILD:
            self.rebuild_from_ledger()

    def refresh_manual_wallets(self) -> List[str]:
        """Append newly added manual wallets without dropping active runtime state."""
        try:
            current = load_manual_wallets()
        except SystemExit:
            return []
        added: List[str] = []
        existing = set(self.wallets)
        for wallet in current:
            wallet = wallet.lower()
            if wallet in existing:
                continue
            self.wallets.append(wallet)
            existing.add(wallet)
            self.wallets_raw.setdefault(wallet, RawWallet(wallet))
            self.wallet_runtime.setdefault(wallet, {
                "baseline_set": wallet in self.exchange_baseline_by_wallet,
                "ready": False,
                "ws_live": False,
                "bootstrapped_at_ms": 0,
                "last_poll_reconcile_ms": 0,
            })
            added.append(wallet)
        if added:
            self.audit["manual_wallets_reloaded"] += len(added)
            log("INFO", f"MANUAL_WALLETS_RELOADED added={len(added)} total={len(self.wallets)}")
        return added

    # ------------------------- baseline / bootstrap -------------------------
    def load_exchange_baselines(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        if not BASELINE_JSON.exists():
            return {}
        try:
            data = json.loads(BASELINE_JSON.read_text(encoding="utf-8"))
            wallets = data.get("wallets", data) if isinstance(data, dict) else {}
            out: Dict[str, Dict[str, Dict[str, float]]] = {}
            if not isinstance(wallets, dict):
                return {}
            for wallet, coins in wallets.items():
                if not isinstance(coins, dict):
                    continue
                w = str(wallet).lower()
                out[w] = {}
                for coin, snap in coins.items():
                    if isinstance(snap, dict):
                        out[w][str(coin).upper()] = {
                            "signed_size": fnum(snap.get("signed_size")),
                            "entry_price": fnum(snap.get("entry_price")),
                            "baseline_ts_ms": inum(snap.get("baseline_ts_ms")),
                        }
                    else:
                        out[w][str(coin).upper()] = {
                            "signed_size": fnum(snap),
                            "entry_price": 0.0,
                            "baseline_ts_ms": 0,
                        }
            return out
        except Exception as e:
            log("WARNING", f"BASELINE_LOAD_FAILED err={e}")
            return {}

    def save_exchange_baselines(self) -> None:
        atomic_write_json(BASELINE_JSON, {
            "schema": "exchange_baselines.v1",
            "updated_at": utc_now_iso(),
            "wallets": self.exchange_baseline_by_wallet,
            "important_boundary": "Baseline offsets are not trades, not PnL, and not engine positions.",
        })

    def _wallet_state(self, wallet: str) -> Dict[str, Any]:
        wallet = str(wallet).lower()
        if wallet not in self.wallet_runtime:
            self.wallet_runtime[wallet] = {
                "baseline_set": wallet in self.exchange_baseline_by_wallet,
                "ready": False,
                "ws_live": False,
                "bootstrapped_at_ms": 0,
                "last_poll_reconcile_ms": 0,
            }
        return self.wallet_runtime[wallet]

    def _is_wallet_ready(self, wallet: str) -> bool:
        return bool(self._wallet_state(wallet).get("ready"))

    def _mark_wallet_ready(self, wallet: str, reason: str, ts_ms: Optional[int] = None) -> None:
        st = self._wallet_state(wallet)
        now = int(ts_ms or utc_now_ms())
        st["baseline_set"] = wallet in self.exchange_baseline_by_wallet
        st["ready"] = True
        st["bootstrapped_at_ms"] = st.get("bootstrapped_at_ms") or now
        log("INFO", f"WALLET_READY wallet={wallet} reason={reason}")

    def set_exchange_baseline(self, wallet: str, snapshot: Dict[str, Dict[str, float]], ts_ms: Optional[int] = None) -> None:
        wallet = wallet.lower()
        ts = int(ts_ms or utc_now_ms())
        base: Dict[str, Dict[str, float]] = {}
        for coin, snap in snapshot.items():
            size = fnum(snap.get("signed_size"))
            entry = fnum(snap.get("entry_price"))
            if abs(size) <= POSITION_EPSILON:
                continue
            c = str(coin).upper()
            base[c] = {"signed_size": size, "entry_price": entry, "baseline_ts_ms": ts}
            if entry > 0:
                self.mark_prices[c] = entry
        self.exchange_baseline_by_wallet[wallet] = base
        self.save_exchange_baselines()
        self.audit["baseline_wallets"] += 1
        self.audit["baseline_positions"] += len(base)
        log("INFO", f"BASELINE_SET wallet={wallet} positions={len(base)}")

    def derive_exchange_baseline_from_current_state(self, wallet: str, snapshot: Dict[str, Dict[str, float]], ts_ms: Optional[int] = None) -> None:
        wallet = wallet.lower()
        ts = int(ts_ms or utc_now_ms())
        base: Dict[str, Dict[str, float]] = {}
        coins = set(snapshot.keys()) | {coin for (w, coin) in self.positions.keys() if w == wallet}
        for coin in sorted(coins):
            exch_size = fnum((snapshot.get(coin) or {}).get("signed_size"))
            exch_entry = fnum((snapshot.get(coin) or {}).get("entry_price"))
            current = self.positions.get((wallet, coin))
            internal_size = current.signed_size if current else 0.0
            baseline_size = exch_size - internal_size
            if abs(baseline_size) <= POSITION_EPSILON:
                continue
            base[str(coin).upper()] = {
                "signed_size": baseline_size,
                "entry_price": exch_entry or (current.avg_entry_price if current else 0.0),
                "baseline_ts_ms": ts,
            }
        self.exchange_baseline_by_wallet[wallet] = base
        self.save_exchange_baselines()
        self.audit["baseline_derived_wallets"] += 1
        self.audit["baseline_derived_positions"] += len(base)
        log("WARNING", f"BASELINE_DERIVED wallet={wallet} positions={len(base)} reason=ledger_without_saved_baseline")

    def baseline_size(self, wallet: str, coin: str) -> float:
        return fnum((self.exchange_baseline_by_wallet.get(wallet.lower(), {}).get(str(coin).upper(), {}) or {}).get("signed_size"))

    def fetch_exchange_positions(self, wallet: str) -> Optional[Dict[str, Dict[str, float]]]:
        if requests is None:
            return None
        if RATE_GUARD is not None and not RATE_GUARD.acquire(
            "clearinghouseState", timeout_s=SSOT_ACQUIRE_TIMEOUT_SEC
        ):
            # Background analysis yields rather than queues.  A skipped snapshot
            # costs one cycle of freshness; a queued one costs the execution
            # path in another process its headroom.
            self.audit["rate_budget_skipped_snapshots"] += 1
            log("INFO", f"RATE_BUDGET_SKIP wallet={wallet} request=clearinghouseState")
            return None
        try:
            r = requests.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "clearinghouseState", "user": wallet},
                timeout=10,
            )
            data = r.json()
            if not isinstance(data, dict) or "assetPositions" not in data:
                self.audit["position_snapshot_invalid"] += 1
                log("WARNING", f"POSITION_SNAPSHOT_INVALID wallet={wallet} response={str(data)[:160]}")
                return None
            out: Dict[str, Dict[str, float]] = {}
            for p in data.get("assetPositions", []):
                pos = p.get("position", {}) if isinstance(p, dict) else {}
                coin = str(pos.get("coin") or p.get("coin") or "").upper()
                signed = fnum(pos.get("szi"))
                if not coin or abs(signed) <= POSITION_EPSILON:
                    continue
                out[coin] = {
                    "signed_size": signed,
                    "entry_price": fnum(pos.get("entryPx")),
                    "unrealized_pnl": fnum(pos.get("unrealizedPnl")),
                }
            self.audit["position_snapshot_requests"] += 1
            return out
        except Exception as e:
            self.audit["position_snapshot_errors"] += 1
            log("ERROR", f"POSITION_SNAPSHOT_ERROR wallet={wallet} err={e}")
            return None

    def bootstrap_wallet_from_exchange(self, wallet: str, ts_ms: Optional[int] = None) -> None:
        wallet = wallet.lower()
        now = int(ts_ms or utc_now_ms())
        snapshot = self.fetch_exchange_positions(wallet)
        if snapshot is None:
            log("WARNING", f"BOOTSTRAP_SNAPSHOT_FAILED wallet={wallet}")
            snapshot = {}
        self.last_exchange_snapshot_by_wallet[wallet] = {k: dict(v) for k, v in snapshot.items()}

        if wallet not in self._startup_fill_wallets:
            self.set_exchange_baseline(wallet, snapshot, ts_ms=now)
            self.last_poll_ts_by_wallet[wallet] = now
            self._mark_wallet_ready(wallet, "cold_start_baseline", now)
            return

        if wallet not in self.exchange_baseline_by_wallet:
            self.derive_exchange_baseline_from_current_state(wallet, snapshot, ts_ms=now)

        last_ledger_ts = int(self.last_ledger_ts_by_wallet.get(wallet, 0) or 0)
        # Poll-only DB hard rule: after restart, resume from the last ledger fill.
        # Never jump to near-now, or downtime fills can be missed.
        self.last_poll_ts_by_wallet[wallet] = max(0, last_ledger_ts)
        self._mark_wallet_ready(wallet, "ledger_rebuild_ready", now)

    def bootstrap_all_wallets(self) -> None:
        log("INFO", f"BOOTSTRAP_START wallets={len(self.wallets)}")
        self.rebuild_from_ledger()
        for wallet in self.wallets:
            self.bootstrap_wallet_from_exchange(wallet, ts_ms=utc_now_ms())
        self.write_state()
        log("INFO", "BOOTSTRAP_COMPLETE")

    # ------------------------- parsing -------------------------
    def parse_fill(self, wallet: str, raw: Dict[str, Any], source: str, shard_id: int = -1, is_snapshot: bool = False) -> Optional[RawFill]:
        source = str(source or "unknown").strip().lower() or "unknown"
        wallet = str(wallet or raw.get("user") or raw.get("wallet") or "").lower()
        if not wallet:
            return None
        coin = str(raw_get(raw, "coin", "symbol", "asset", default="")).upper().strip()
        price = fnum(raw_get(raw, "px", "price", "avgPx"))
        size = abs(fnum(raw_get(raw, "sz", "size", "qty")))
        ts = inum(raw_get(raw, "time", "timestamp", "ts"), utc_now_ms())
        if not coin or price <= 0 or size <= 0 or ts <= 0:
            return None
        start_pos = fnum(raw_get(raw, "startPosition", "start_pos", "start_position"), 0.0)
        side = normalize_side(raw_get(raw, "side", "dir", "direction"), size, start_pos)
        delta = size if side == "BUY" else -size
        end_pos = fnum(raw_get(raw, "endPosition", "end_pos", "end_position"), start_pos + delta)
        closed_pnl = fnum(raw_get(raw, "closedPnl", "closed_pnl", "realizedPnl"), 0.0)
        fee = abs(fnum(raw_get(raw, "fee", "feeUsd", "builderFee"), 0.0))
        oid = str(raw_get(raw, "oid", "hash", "tid", "id", default=""))
        fill_id = str(raw_get(raw, "fill_id", default="")) or f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"
        recv_ms = utc_now_ms()
        recording_method = normalise_recording_method(raw_get(raw, "recording_method", default=None), source=source, is_snapshot=is_snapshot)
        rebuild_reason = "" if recording_method == RECORDING_WS_CAPTURED else str(raw_get(raw, "rebuild_reason", default=source or "backfill"))
        return RawFill(
            fill_id=fill_id,
            wallet=wallet,
            coin=coin,
            side=side,
            price=price,
            size=size,
            signed_size_delta=delta,
            start_position=start_pos,
            end_position=end_pos,
            closed_pnl=closed_pnl,
            fee=fee,
            timestamp_ms=ts,
            timestamp_iso=datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
            received_at_ms=recv_ms,
            received_at_iso=datetime.fromtimestamp(recv_ms / 1000, tz=timezone.utc).isoformat(),
            latency_ms=max(0, recv_ms - ts),
            source=source,
            recording_method=recording_method,
            rebuild_reason=rebuild_reason,
            contributes_to_execution_delta=recording_contributes_to_execution_delta(recording_method),
            reconstructed=False,
            shard_id=shard_id,
            is_snapshot=is_snapshot,
            raw=raw,
        )

    def _remember_id(self, fill_id: str) -> bool:
        with self.seen_lock:
            if fill_id in self.seen_set:
                return False
            self.seen_set.add(fill_id)
            self.seen_ids.append(fill_id)
            while len(self.seen_set) > len(self.seen_ids):
                self.seen_set = set(self.seen_ids)
            return True

    def _load_ledger_seen_ids(self) -> None:
        for fill in self.load_fills_from_ledger():
            self._remember_id(fill.fill_id)

    # ------------------------- truth application -------------------------
    def accept_fill(self, fill: RawFill, persist: bool = True) -> bool:
        if not self._remember_id(fill.fill_id):
            self.audit["duplicates"] += 1
            self.wallets_raw.setdefault(fill.wallet, RawWallet(fill.wallet)).duplicate_fill_count += 1
            return False
        with self.state_lock:
            self._apply_fill_unlocked(fill)
            self.last_ledger_ts_by_wallet[fill.wallet] = max(
                int(self.last_ledger_ts_by_wallet.get(fill.wallet, 0) or 0),
                int(fill.timestamp_ms or 0),
            )
            if persist:
                self.raw_ledger.append(fill.to_csv_row())
        return True

    def _apply_fill_unlocked(self, fill: RawFill) -> None:
        self.audit["fills_applied"] += 1
        if fill.recording_method == RECORDING_WS_CAPTURED:
            self.audit["ws_captured_applied"] += 1
        else:
            self.audit["rebuild_applied"] += 1
        w = self.wallets_raw.setdefault(fill.wallet, RawWallet(fill.wallet))
        w.fill_count += 1
        if fill.recording_method == RECORDING_WS_CAPTURED:
            w.ws_fill_count += 1
        else:
            w.rebuild_fill_count += 1
        if fill.source == "poll":
            w.poll_fill_count += 1
        if fill.is_snapshot:
            w.snapshot_fill_count += 1
        w.realized_pnl_raw += fill.closed_pnl
        w.fees_raw += fill.fee
        w.last_fill_ts = fill.timestamp_iso
        w.last_source = fill.source
        self.mark_prices[fill.coin] = fill.price
        pos = self.positions.setdefault((fill.wallet, fill.coin), RawPosition(fill.wallet, fill.coin))
        pos.apply_fill(fill)

    def rebuild_from_ledger(self) -> None:
        fills = self.load_fills_from_ledger()
        with self.state_lock:
            self.wallets_raw = {w: RawWallet(w) for w in self.wallets}
            self.positions = {}
            self.mark_prices = {}
            self.audit["fills_applied"] = 0
            for fill in fills:
                self._apply_fill_unlocked(fill)
        log("INFO", f"REBUILD_OK fills={len(fills)}")
        self.write_state()

    def load_fills_from_ledger(self) -> List[RawFill]:
        if not RAW_FILLS_CSV.exists():
            return []
        out: List[RawFill] = []
        seen: Set[str] = set()
        min_ts = monitor_start_ms()
        with RAW_FILLS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    fill = RawFill.from_csv_row(row)
                except Exception:
                    continue
                if min_ts and fill.timestamp_ms < min_ts:
                    continue
                if not fill.fill_id or fill.fill_id in seen:
                    continue
                seen.add(fill.fill_id)
                out.append(fill)
        out.sort(key=lambda x: (x.timestamp_ms, x.wallet, x.coin, x.fill_id))
        return out

    def reconcile_positions_from_snapshot(self, wallet: str, snapshot: Dict[str, Dict[str, float]], ts_ms: Optional[int] = None) -> int:
        """Deprecated compatibility wrapper: snapshots do not create fills.

        The current poll-only proof engine has one append path only:
        ingest_real_fills_window(). This wrapper is kept so older harnesses or
        callers do not crash, but it deliberately delegates to drift audit and
        returns without writing REBUILD/position_diff rows.
        """
        self.audit["snapshot_reconcile_deprecated_noop"] += 1
        log("WARNING", f"SNAPSHOT_RECONCILE_NOOP wallet={wallet} action=use_real_fill_ingest_only")
        return self.audit_position_drift_only(wallet, snapshot)

    # ------------------------- polling -------------------------
    def fetch_fills_range(self, wallet: str, start_ms: int, end_ms: int) -> Optional[List[Dict[str, Any]]]:
        """Fetch one real userFillsByTime range.

        Returns None on request/API failure so callers do NOT advance the cursor.
        Returns [] on a successful empty range.
        """
        if requests is None:
            return None
        if RATE_GUARD is not None and not RATE_GUARD.acquire(
            "userFillsByTime", timeout_s=SSOT_ACQUIRE_TIMEOUT_SEC
        ):
            # None (not []) so the caller does NOT advance its cursor: a skipped
            # fetch must never be mistaken for a proven-empty window.
            self.audit["rate_budget_skipped_fill_fetches"] += 1
            log("INFO", f"RATE_BUDGET_SKIP wallet={wallet} request=userFillsByTime")
            return None
        try:
            payload: Dict[str, Any] = {
                "type": "userFillsByTime",
                "user": wallet,
                "startTime": int(start_ms),
                "endTime": int(end_ms),
                "aggregateByTime": False,
            }
            r = requests.post(
                "https://api.hyperliquid.xyz/info",
                json=payload,
                timeout=8,
            )
            data = r.json()
            if data is None:
                # Hyperliquid returns JSON null for some quiet windows.
                self.audit["poll_empty_response"] += 1
                return []
            if not isinstance(data, list):
                self.audit["poll_invalid_response"] += 1
                log("WARNING", f"POLL_INVALID_RESPONSE wallet={wallet} start={start_ms} end={end_ms} response={str(data)[:160]}")
                return None
            return data
        except Exception as e:
            self.audit["poll_errors"] += 1
            log("ERROR", f"POLL_ERROR wallet={wallet} start={start_ms} end={end_ms} err={e}")
            return None

    def fetch_fills_since(self, wallet: str, start_ms: int, end_ms: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """Fetch all real fills from start_ms to end_ms using bounded windows.

        This mirrors the proven universe_builder pattern:
        last stored fill - overlap -> now, split into windows, dedupe downstream.

        If a window returns a full page, continue from the max returned timestamp
        + 1 within that same window so long outage catch-up cannot silently drop
        later fills from the same broad range.
        """
        now_ms = int(end_ms or utc_now_ms())
        cursor = max(0, int(start_ms))
        all_rows: List[Dict[str, Any]] = []

        while cursor < now_ms:
            window_end = min(cursor + POLL_WINDOW_MS, now_ms)
            page_cursor = cursor
            page_count = 0
            max_pages_per_window = 200  # safety: 500*200=100k fills per window, ~8 wallets/day max
            while page_cursor < window_end and page_count < max_pages_per_window:
                rows = self.fetch_fills_range(wallet, page_cursor, window_end)
                if rows is None:
                    return None
                if not rows:
                    break

                all_rows.extend(rows)
                max_row_ts = max(inum(raw_get(r, "time", "timestamp", "ts"), page_cursor) for r in rows if isinstance(r, dict))

                # If the API returns a full capped page, page forward inside the same window.
                # Otherwise this window is complete.
                if len(rows) >= POLL_MAX_PAGE_ROWS and max_row_ts >= page_cursor:
                    next_cursor = max_row_ts + 1
                    if next_cursor <= page_cursor:
                        next_cursor = page_cursor + 1
                    page_cursor = min(next_cursor, window_end)
                    page_count += 1
                    self.audit["poll_paginated_pages"] += 1
                    continue
                break

            if page_count >= max_pages_per_window:
                log("WARNING", f"POLL_PAGINATION_SAFEGUARD wallet={wallet} window_start={cursor} window_end={window_end} pages={page_count} — admitting data gap to prevent infinite loop")
                self.audit["poll_pagination_safeguard_hit"] += 1

            cursor = window_end

        return all_rows

    def ingest_real_fills_window(
        self,
        wallet: str,
        start_ms: int,
        end_ms: int,
        reason: str,
        *,
        advance_cursor: bool = False,
    ) -> Dict[str, Any]:
        """Canonical real-fill ingestion path.

        This is the only active poll-only path that is allowed to fetch
        userFillsByTime and append rows to raw_live_fills.csv. It is used by
        normal polling, restart catch-up and drift recovery. Snapshot drift may
        trigger this function with a wider window, but snapshots never create
        synthetic rows.
        """
        wallet = wallet.lower()
        start_ms = max(0, int(start_ms))
        end_ms = max(start_ms, int(end_ms))
        self.audit[f"{reason}_ingest_attempts"] += 1
        log("INFO", f"REAL_FILL_INGEST_START wallet={wallet} reason={reason} start={start_ms} end={end_ms}")

        raw_fills = self.fetch_fills_since(wallet, start_ms, end_ms)
        if raw_fills is None:
            self.audit[f"{reason}_ingest_failed"] += 1
            log("WARNING", f"REAL_FILL_INGEST_FAILED wallet={wallet} reason={reason} start={start_ms} end={end_ms}")
            return {"ok": False, "parsed": 0, "applied": 0, "deduped": 0, "max_ts": start_ms}

        parsed: List[RawFill] = []
        min_ts = monitor_start_ms()
        for raw in raw_fills:
            fill = self.parse_fill(wallet, raw, source="poll")
            if fill is not None and min_ts and fill.timestamp_ms < min_ts:
                self.audit["poll_pre_monitor_skipped"] += 1
                continue
            if fill is not None:
                parsed.append(fill)
        parsed.sort(key=lambda f: (f.timestamp_ms, f.wallet, f.coin, f.fill_id))

        boot_ms = int(self._wallet_state(wallet).get("bootstrapped_at_ms") or 0)
        cold_start_wallet = wallet not in self._startup_fill_wallets
        max_ts = start_ms
        applied = 0
        deduped = 0
        skipped_prebaseline = 0
        for fill in parsed:
            if cold_start_wallet and fill.timestamp_ms <= boot_ms:
                skipped_prebaseline += 1
                self.audit["poll_prebaseline_skipped"] += 1
                continue
            max_ts = max(max_ts, fill.timestamp_ms)
            if self.accept_fill(fill, persist=True):
                applied += 1
                self.audit[f"{reason}_applied"] += 1
                if reason != "poll":
                    self.audit["poll_applied"] += 1
            else:
                deduped += 1
                self.audit[f"{reason}_deduped"] += 1
                if reason != "poll":
                    self.audit["poll_deduped"] += 1

        if advance_cursor:
            # Successful API cycle: advance to end, with next cycle protected by POLL_OVERLAP_MS.
            self.last_poll_ts_by_wallet[wallet] = max(max_ts, end_ms)

        log(
            "INFO",
            f"REAL_FILL_INGEST_DONE wallet={wallet} reason={reason} parsed={len(parsed)} "
            f"applied={applied} deduped={deduped} prebaseline_skipped={skipped_prebaseline} max_ts={max_ts}"
        )
        return {"ok": True, "parsed": len(parsed), "applied": applied, "deduped": deduped, "max_ts": max_ts}

    def _set_drift_state(self, wallet: str, coin: str, status: str, detail: Dict[str, Any]) -> None:
        wallet = wallet.lower()
        coin = coin.upper()
        self.drift_state_by_wallet.setdefault(wallet, {})[coin] = {
            "status": status,
            "updated_at": utc_now_iso(),
            **detail,
        }

    def audit_position_drift_only(self, wallet: str, snapshot: Dict[str, Dict[str, float]]) -> int:
        """Compare snapshot to ledger state and recover using real fills only.

        Snapshots prove drift, but they must never create synthetic fills. If
        drift is detected, widen the userFillsByTime window through the same
        canonical ingest_real_fills_window() path used by normal polling, then
        recheck. Unresolved drift is surfaced in engine_truth; it does not lock
        the wallet and it does not fabricate rows.
        """
        wallet = wallet.lower()
        now_ms = utc_now_ms()
        if not self._is_wallet_ready(wallet):
            log("INFO", f"POLL_LOCKED wallet={wallet}")
            return 0
        if wallet not in self.exchange_baseline_by_wallet:
            self.set_exchange_baseline(wallet, snapshot, ts_ms=now_ms)
            self._mark_wallet_ready(wallet, "baseline_missing_created", now_ms)
            return 0

        coins = (
            set(snapshot.keys())
            | {coin for (w, coin) in self.positions.keys() if w == wallet}
            | set(self.exchange_baseline_by_wallet.get(wallet, {}).keys())
        )

        drift_rows: List[Dict[str, Any]] = []
        for coin in sorted(coins):
            exch_size = fnum((snapshot.get(coin) or {}).get("signed_size"))
            baseline = self.baseline_size(wallet, coin)
            tracked_exchange_size = exch_size - baseline
            current = self.positions.get((wallet, coin))
            internal_size = current.signed_size if current else 0.0
            delta = tracked_exchange_size - internal_size
            if abs(delta) <= POSITION_EPSILON:
                self._set_drift_state(wallet, coin, "CLEAN", {
                    "delta": delta, "internal": internal_size, "exchange": exch_size,
                    "baseline": baseline, "tracked_exchange": tracked_exchange_size,
                })
                continue
            drift_rows.append({
                "coin": coin,
                "delta": delta,
                "internal": internal_size,
                "exchange": exch_size,
                "baseline": baseline,
                "tracked_exchange": tracked_exchange_size,
            })
            self.audit["poll_drift_detected"] += 1
            self._set_drift_state(wallet, coin, "DRIFT_DETECTED", drift_rows[-1])
            log(
                "WARNING",
                f"POLL_DRIFT wallet={wallet} coin={coin} delta={delta} "
                f"internal={internal_size} exchange={exch_size} baseline={baseline} "
                f"tracked_exchange={tracked_exchange_size} action=recover_from_real_fills"
            )

        if not drift_rows:
            return 0

        # One recovery call per wallet, not one fake row per coin. Start from
        # the earliest relevant baseline timestamp or last ledger overlap.
        baseline_ts_values: List[int] = []
        for row in drift_rows:
            base = self.exchange_baseline_by_wallet.get(wallet, {}).get(str(row["coin"]).upper(), {})
            ts = inum((base or {}).get("baseline_ts_ms"), 0)
            if ts > 0:
                baseline_ts_values.append(ts)
        last_ledger_ts = int(self.last_ledger_ts_by_wallet.get(wallet, 0) or 0)
        candidates = [now_ms - POLL_WINDOW_MS]
        if last_ledger_ts > 0:
            candidates.append(max(0, last_ledger_ts - POLL_OVERLAP_MS))
        candidates.extend(baseline_ts_values)
        recovery_start = max(0, min(candidates))

        # Has anything changed since the last barren attempt over this window?
        # New fills advance last_ledger_ts; an earlier recovery_start would cover
        # ground the previous attempt did not.  Absent both, re-fetching asks the
        # exchange a question it has already answered.
        gate = self.drift_recovery_gate_by_wallet.get(wallet) or {}
        no_new_evidence = (
            bool(gate.get("barren"))
            and int(gate.get("ledger_ts", -1)) == last_ledger_ts
            and int(gate.get("recovery_start", -1)) <= recovery_start
        )
        if no_new_evidence and (time.time() - float(gate.get("at", 0.0))) < DRIFT_RECOVERY_RETRY_SEC:
            self.audit["drift_recovery_quiesced"] += 1
            for row in drift_rows:
                self._set_drift_state(wallet, str(row["coin"]).upper(), "DRIFT_UNRESOLVED", {
                    **row,
                    "recovery": "QUIESCED",
                    "recovery_quiesced_reason": "no_new_fills_since_last_barren_recovery",
                    "recovery_retry_after_sec": DRIFT_RECOVERY_RETRY_SEC,
                    "recovery_last_attempt_epoch": float(gate.get("at", 0.0)),
                })
            log(
                "WARNING",
                f"DRIFT_UNRESOLVED_QUIESCED wallet={wallet} coins={len(drift_rows)} "
                f"ledger_ts={last_ledger_ts} recovery_start={recovery_start} "
                f"retry_after_sec={DRIFT_RECOVERY_RETRY_SEC} "
                "reason=no_new_fills_since_last_barren_recovery"
            )
            return len(drift_rows)

        self.audit["drift_recovery_attempts"] += 1
        ingest = self.ingest_real_fills_window(wallet, recovery_start, now_ms, "drift_recovery", advance_cursor=False)
        self.drift_recovery_gate_by_wallet[wallet] = {
            "at": time.time(),
            "ledger_ts": last_ledger_ts,
            "recovery_start": recovery_start,
            "barren": not int(ingest.get("applied", 0) or 0),
        }
        if ingest.get("applied", 0):
            # Replay ensures in-memory truth exactly matches the append-only ledger.
            self.rebuild_from_ledger()

        recovered = 0
        unresolved = 0
        for row in drift_rows:
            coin = str(row["coin"]).upper()
            exch_size = fnum((snapshot.get(coin) or {}).get("signed_size"))
            baseline = self.baseline_size(wallet, coin)
            tracked_exchange_size = exch_size - baseline
            current = self.positions.get((wallet, coin))
            internal_size = current.signed_size if current else 0.0
            delta = tracked_exchange_size - internal_size
            detail = {
                "delta": delta,
                "internal": internal_size,
                "exchange": exch_size,
                "baseline": baseline,
                "tracked_exchange": tracked_exchange_size,
                "recovery_start_ms": recovery_start,
                "recovery_end_ms": now_ms,
                "recovery_applied": int(ingest.get("applied", 0) or 0),
                "recovery_deduped": int(ingest.get("deduped", 0) or 0),
            }
            if abs(delta) <= POSITION_EPSILON:
                recovered += 1
                self.audit["drift_recovered"] += 1
                self._set_drift_state(wallet, coin, "RECOVERED", detail)
                log("INFO", f"DRIFT_RECOVERED wallet={wallet} coin={coin} applied={ingest.get('applied', 0)}")
            else:
                unresolved += 1
                self.audit["drift_unresolved"] += 1
                self._set_drift_state(wallet, coin, "DRIFT_UNRESOLVED", detail)
                log(
                    "WARNING",
                    f"DRIFT_UNRESOLVED wallet={wallet} coin={coin} delta={delta} "
                    f"internal={internal_size} exchange={exch_size} baseline={baseline} "
                    f"tracked_exchange={tracked_exchange_size} applied={ingest.get('applied', 0)}"
                )
        return recovered + unresolved

    def poll_once(self) -> None:
        now = utc_now_ms()
        self.refresh_manual_wallets()
        # Start each sweep where the last one stopped.  Under a weight ceiling a
        # sweep may not reach every wallet, and a sweep that always starts at
        # index 0 would poll the front of the list forever and never once look
        # at the back of it.  Rotating makes the shortfall show up as latency
        # spread evenly across wallets instead of a permanently blind tail.
        order = list(self.wallets)
        if order:
            offset = self._poll_rotation_offset % len(order)
            order = order[offset:] + order[:offset]
            self._poll_rotation_offset = (offset + 1) % len(order)
        for wallet in order:
            wallet = wallet.lower()
            if not self._is_wallet_ready(wallet):
                log("INFO", f"POLL_BOOTSTRAP wallet={wallet}")
                self.bootstrap_wallet_from_exchange(wallet, ts_ms=now)
                continue

            start = self.last_poll_ts_by_wallet.get(wallet, now)
            ingest = self.ingest_real_fills_window(
                wallet,
                max(0, start - POLL_OVERLAP_MS),
                now,
                "poll",
                advance_cursor=True,
            )
            if not ingest.get("ok"):
                self.audit["poll_cycle_failed_no_cursor_advance"] += 1
                continue

            snapshot = self.fetch_exchange_positions(wallet)
            if snapshot is None:
                log("WARNING", f"POLL_SNAPSHOT_SKIPPED wallet={wallet} reason=invalid_or_failed")
                continue
            self.last_exchange_snapshot_by_wallet[wallet] = {k: dict(v) for k, v in snapshot.items()}
            self.audit_position_drift_only(wallet, snapshot)
            self._wallet_state(wallet)["last_poll_reconcile_ms"] = now

        self.write_state()

    def poll_loop(self) -> None:
        while not self.stop_event.is_set():
            self.poll_once()
            self.stop_event.wait(POLL_SECONDS)

    # ------------------------- websocket -------------------------
    def _on_ws_message(self, shard_id: int, _ws: Any, message: str) -> None:
        now_ms = utc_now_ms()
        self.audit["ws_messages"] += 1
        self.shard_status.setdefault(shard_id, {})["last_msg_ms"] = now_ms
        try:
            msg = json.loads(message)
        except Exception:
            self.audit["ws_parse_errors"] += 1
            return
        data = msg.get("data", msg)
        fills_payload: List[Tuple[str, Dict[str, Any]]] = []
        if isinstance(data, dict):
            if bool(data.get("isSnapshot")):
                self.audit["ws_snapshot_messages_ignored"] += 1
                return
            wallet = str(data.get("user") or data.get("wallet") or "").lower()
            fills = data.get("fills") or data.get("userFills") or []
            if isinstance(fills, list):
                fills_payload.extend((wallet, f) for f in fills if isinstance(f, dict))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    wallet = str(item.get("user") or item.get("wallet") or "").lower()
                    fills_payload.append((wallet, item))
        if fills_payload:
            st = self.shard_status.setdefault(shard_id, {})
            st["last_data_ms"] = now_ms
            self.audit["ws_data_messages"] += 1
        for wallet, raw in fills_payload:
            wallet = str(wallet or raw.get("user") or raw.get("wallet") or "").lower()
            if not self._is_wallet_ready(wallet):
                self.audit["ws_ignored_not_ready"] += 1
                log("INFO", f"WS_IGNORED_NOT_READY wallet={wallet} shard={shard_id}")
                continue
            self._wallet_state(wallet)["ws_live"] = True
            fill = self.parse_fill(wallet, raw, source="ws", shard_id=shard_id, is_snapshot=False)
            if fill is not None:
                boot_ms = int(self._wallet_state(wallet).get("bootstrapped_at_ms") or 0)
                if wallet not in self._startup_fill_wallets and fill.timestamp_ms <= boot_ms:
                    self.audit["ws_prebaseline_skipped"] += 1
                    continue
                try:
                    self.event_queue.put_nowait(fill)
                    self.audit["ws_queued"] += 1
                except queue.Full:
                    self.audit["ws_queue_full"] += 1

    def _ws_thread(self, shard_id: int, wallets: List[str]) -> None:
        if websocket is None:
            log("ERROR", "websocket-client missing; WS disabled")
            return
        backoff = 1.0
        while not self.stop_event.is_set():
            self.shard_status[shard_id] = {"status": "CONNECTING", "wallets": wallets, "last_msg_ms": 0, "last_data_ms": 0, "error_count": 0}
            try:
                def on_open(ws: Any) -> None:
                    now_ms = utc_now_ms()
                    self.shard_status[shard_id]["status"] = "OPEN"
                    self.shard_status[shard_id]["last_open_ms"] = now_ms
                    self.shard_status[shard_id]["last_msg_ms"] = now_ms
                    self.shard_status[shard_id].setdefault("last_data_ms", 0)
                    with self.ws_apps_lock:
                        self.ws_apps[shard_id] = ws
                    for w in wallets:
                        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": w}}))
                    log("INFO", f"WS_OPEN shard={shard_id} wallets={len(wallets)}")

                def on_message(ws: Any, message: str) -> None:
                    self._on_ws_message(shard_id, ws, message)

                def on_error(_ws: Any, err: Any) -> None:
                    self.shard_status[shard_id]["status"] = "ERROR"
                    self.shard_status[shard_id]["error_count"] = int(self.shard_status[shard_id].get("error_count", 0)) + 1
                    log("ERROR", f"WS_ERROR shard={shard_id} err={err}")

                def on_close(_ws: Any, *_args: Any) -> None:
                    self.shard_status[shard_id]["status"] = "CLOSED"
                    self.shard_status[shard_id]["last_close_ms"] = utc_now_ms()
                    with self.ws_apps_lock:
                        self.ws_apps.pop(shard_id, None)
                    log("WARN", f"WS_CLOSED shard={shard_id}")

                ws_app = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
                ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log("ERROR", f"WS_THREAD_EXCEPTION shard={shard_id} err={e}")
            if self.stop_event.wait(backoff):
                break
            backoff = min(RECONNECT_BACKOFF_CAP_SEC, backoff * 2)

    def force_restart_shard(self, shard_id: int, reason: str) -> None:
        now = utc_now_ms()
        st = self.shard_status.setdefault(shard_id, {})
        last_forced = int(st.get("last_forced_restart_ms") or 0)
        if last_forced and (now - last_forced) < int(WS_RESTART_COOLDOWN_SEC * 1000):
            return
        st["status"] = "RESTARTING"
        st["restart_reason"] = reason
        st["restart_count"] = int(st.get("restart_count", 0)) + 1
        st["last_forced_restart_ms"] = now
        self.audit["ws_forced_restarts"] += 1
        log("WARNING", f"WS_WATCHDOG_RESTART shard={shard_id} reason={reason}")
        with self.ws_apps_lock:
            ws_app = self.ws_apps.get(shard_id)
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception as e:
                log("WARNING", f"WS_WATCHDOG_CLOSE_FAILED shard={shard_id} err={e}")

    def ws_watchdog_loop(self) -> None:
        while not self.stop_event.is_set():
            if WS_STALE_MS <= 0 and WS_DATA_STALE_MS <= 0:
                self.stop_event.wait(WS_WATCHDOG_INTERVAL_SEC)
                continue
            now = utc_now_ms()
            for shard_id, st in list(self.shard_status.items()):
                if st.get("status") != "OPEN":
                    continue
                last_msg = int(st.get("last_msg_ms") or 0)
                stale = (now - last_msg) if last_msg else 10**12
                if WS_STALE_MS > 0 and stale > WS_STALE_MS:
                    self.force_restart_shard(shard_id, f"NO_MESSAGES_{stale}ms")
                    continue
                last_data = int(st.get("last_data_ms") or 0)
                data_stale = (now - last_data) if last_data else 10**12
                if WS_DATA_STALE_MS > 0 and data_stale > WS_DATA_STALE_MS:
                    self.force_restart_shard(shard_id, f"NO_USER_FILL_DATA_{data_stale}ms")
            self.stop_event.wait(WS_WATCHDOG_INTERVAL_SEC)

    def worker_loop(self) -> None:
        if not ENABLE_WS:
            return
        while not self.stop_event.is_set():
            try:
                fill = self.event_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            self.accept_fill(fill, persist=True)
            self.event_queue.task_done()

    def state_loop(self) -> None:
        while not self.stop_event.is_set():
            self.write_state()
            self.stop_event.wait(STATE_WRITE_INTERVAL_SEC)

    # ------------------------- state output -------------------------
    def _raw_positions_by_wallet(self) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for (wallet, coin), pos in self.positions.items():
            result[wallet][coin] = {
                "signed_size": pos.signed_size,
                "side": pos.side,
                "avg_entry_price": pos.avg_entry_price,
                "last_price": pos.last_price,
                "notional": pos.notional,
                "last_timestamp_ms": pos.last_timestamp_ms,
            }
        return dict(result)

    def _wallet_raw_state(self) -> Dict[str, Any]:
        positions_by_wallet = self._raw_positions_by_wallet()
        out: Dict[str, Any] = {}
        for wallet in sorted(set(self.wallets) | set(self.wallets_raw.keys()) | set(positions_by_wallet.keys())):
            w = self.wallets_raw.setdefault(wallet, RawWallet(wallet))
            pos = positions_by_wallet.get(wallet, {})
            out[wallet] = {
                "wallet": wallet,
                "is_user_wallet": wallet == self.user_wallet,
                "realized_pnl_raw": w.realized_pnl_raw,
                "fees_raw": w.fees_raw,
                "fill_count": w.fill_count,
                "ws_fill_count": w.ws_fill_count,
                "rebuild_fill_count": w.rebuild_fill_count,
                "poll_fill_count": w.poll_fill_count,
                "snapshot_fill_count": w.snapshot_fill_count,
                "duplicate_fill_count": w.duplicate_fill_count,
                "last_fill_ts": w.last_fill_ts,
                "last_source": w.last_source,
                "positions": pos,
                "position_count": sum(1 for p in pos.values() if abs(p.get("signed_size", 0.0)) > 1e-12),
                "open_notional_raw": sum(float(p.get("notional", 0.0)) for p in pos.values()),
            }
        return out

    def _ws_health(self) -> Dict[str, Any]:
        now = utc_now_ms()
        shards = {}
        overall = "OK"
        for sid, st in self.shard_status.items():
            last = int(st.get("last_msg_ms") or 0)
            stale = (now - last) if last else 10**12
            last_data = int(st.get("last_data_ms") or 0)
            data_stale = (now - last_data) if last_data else 10**12
            status = st.get("status", "UNKNOWN")
            if WS_STALE_MS > 0 and stale > WS_STALE_MS and status == "OPEN":
                status = "STALE"
            if WS_DATA_STALE_MS > 0 and data_stale > WS_DATA_STALE_MS and status == "OPEN":
                status = "DATA_STALE"
            if status not in {"OPEN", "OK"}:
                overall = "DEGRADED"
            shards[str(sid)] = {**st, "effective_status": status, "stale_ms": stale, "data_stale_ms": data_stale}
        if not shards:
            overall = "NO_WS"
        return {
            "overall": overall,
            "shards": shards,
            "ws_stale_ms": WS_STALE_MS,
            "ws_data_stale_ms": WS_DATA_STALE_MS,
            "watchdog_interval_sec": WS_WATCHDOG_INTERVAL_SEC,
            "restart_cooldown_sec": WS_RESTART_COOLDOWN_SEC,
        }

    def build_truth(self) -> Dict[str, Any]:
        with self.state_lock:
            wallets = self._wallet_raw_state()
            fills = self.load_fills_from_ledger()
            truth = {
                "schema": "engine_truth.v1.raw_only",
                "updated_at": utc_now_iso(),
                "engine_start_ms": self.engine_start_ms,
                "wallets_tracked": self.wallets,
                "user_wallet": self.user_wallet,
                "wallets": wallets,
                "positions": self._raw_positions_by_wallet(),
                "mark_prices": dict(self.mark_prices),
                "ws_health": self._ws_health(),
                "audit": dict(self.audit),
                "wallet_runtime": {k: dict(v) for k, v in self.wallet_runtime.items()},
                "drift_state": self.drift_state_by_wallet,
                "exchange_baselines": self.exchange_baseline_by_wallet,
                "ledger": {
                    "path": str(RAW_FILLS_CSV),
                    "fill_count": len(fills),
                    "first_ts": fills[0].timestamp_iso if fills else "",
                    "last_ts": fills[-1].timestamp_iso if fills else "",
                    "ws_captured_count": sum(1 for f in fills if f.recording_method == RECORDING_WS_CAPTURED),
                    "rebuild_count": sum(1 for f in fills if f.recording_method == RECORDING_REBUILD),
                    "execution_delta_eligible_count": sum(1 for f in fills if f.contributes_to_execution_delta),
                },
                "important_boundary": "raw truth only: WS_CAPTURED vs REBUILD provenance; no copy sizing, no normalisation, no portfolio modelling",
            }
            return truth

    def write_state(self) -> None:
        truth = self.build_truth()
        atomic_write_json(ENGINE_TRUTH_JSON, truth)
        atomic_write_json(LIVE_STATE_JSON, truth)  # compatibility mirror
        rows = []
        ws = truth.get("ws_health", {})
        stale_ms = 0
        data_stale_ms = 0
        for shard in ws.get("shards", {}).values():
            stale_ms = max(stale_ms, int(shard.get("stale_ms", 0)))
            data_stale_ms = max(data_stale_ms, int(shard.get("data_stale_ms", 0)))
        for wallet, w in truth["wallets"].items():
            rows.append({
                "wallet": wallet,
                "fill_count": w["fill_count"],
                "ws_fill_count": w["ws_fill_count"],
                "rebuild_fill_count": w["rebuild_fill_count"],
                "poll_fill_count": w["poll_fill_count"],
                "snapshot_fill_count": w["snapshot_fill_count"],
                "duplicate_fill_count": w["duplicate_fill_count"],
                "last_fill_ts": w["last_fill_ts"],
                "last_source": w["last_source"],
                "position_count": w["position_count"],
                "open_notional_raw": w["open_notional_raw"],
                "realized_pnl_raw": w["realized_pnl_raw"],
                "fees_raw": w["fees_raw"],
                "ws_status": ws.get("overall", "UNKNOWN"),
                "ws_stale_ms": stale_ms,
                "ws_data_stale_ms": data_stale_ms,
            })
        self.metrics_ledger.replace_all(rows)

    def save_snapshot(self) -> Path:
        truth = self.build_truth()
        p = SNAP_DIR / datetime.now().strftime("%Y-%m-%d_%H-%M-%S_engine_truth.json")
        atomic_write_json(p, truth)
        return p

    # ------------------------- lifecycle -------------------------
    def start(self) -> None:
        log("INFO", "ENGINE_SSOT_START raw_truth_only=1")
        # Startup barrier: establish/load baseline offsets before WS opens.
        # Cold-start baselines are not positions and not ledger rows.
        self.bootstrap_all_wallets()
        self.threads.append(threading.Thread(target=self.worker_loop, name="worker", daemon=True))
        self.threads.append(threading.Thread(target=self.poll_loop, name="poll", daemon=True))
        self.threads.append(threading.Thread(target=self.state_loop, name="state", daemon=True))
        if ENABLE_WS:
            self.threads.append(threading.Thread(target=self.ws_watchdog_loop, name="ws-watchdog", daemon=True))
            for sid, wallets in enumerate(chunked(self.wallets, WALLETS_PER_SHARD)):
                self.threads.append(threading.Thread(target=self._ws_thread, args=(sid, wallets), name=f"ws-{sid}", daemon=True))
        for t in self.threads:
            t.start()
        while not self.stop_event.is_set():
            time.sleep(0.5)

    def stop(self) -> None:
        self.stop_event.set()
        self.write_state()
        self.save_snapshot()
        log("INFO", "ENGINE_SSOT_STOP")


def acquire_lock() -> Optional[int]:
    try:
        if LOCK_FILE.exists():
            old = int(LOCK_FILE.read_text().strip() or "0")
            if old:
                try:
                    os.kill(old, 0)
                    print(f"[ENGINE_LOCK] Another instance is running (PID={old}). Exiting.")
                    return None
                except Exception:
                    print("[ENGINE_LOCK] Stale lock detected. Cleaning up.")
        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
        print(f"[ENGINE_LOCK] Acquired. PID={os.getpid()}")
        return os.getpid()
    except Exception:
        return os.getpid()


def release_lock() -> None:
    try:
        if LOCK_FILE.exists() and LOCK_FILE.read_text().strip() == str(os.getpid()):
            LOCK_FILE.unlink()
    except Exception:
        pass


def main() -> None:
    ensure_dirs()
    if acquire_lock() is None:
        return
    engine = EngineSSOT()
    def _handler(_sig: int, _frame: Any) -> None:
        engine.stop()
        release_lock()
        raise SystemExit(0)
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    try:
        engine.start()
    finally:
        engine.stop()
        release_lock()


if __name__ == "__main__":
    main()
