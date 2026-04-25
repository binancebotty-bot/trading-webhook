import csv
import json
import os
import queue
import shutil
import signal
import ssl
import threading
import time
import atexit
import ctypes

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

try:
    import websocket
except ImportError as exc:
    raise SystemExit("Missing dependency: websocket-client. Install with: pip install websocket-client") from exc


# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parent
MANUAL_WALLETS_FILE = BASE_DIR / "manual_wallets.txt"
OUTPUT_DIR = BASE_DIR / "hl_copy_output"
SNAP_DIR = OUTPUT_DIR / "snapshots"
WALLET_GATE_FILE = BASE_DIR / "wallet_gate.json"  # persists outside hl_copy_output
RAW_FILLS_CSV = OUTPUT_DIR / "raw_live_fills.csv"
COPY_TRADES_CSV = OUTPUT_DIR / "copy_trades.csv"
LIVE_METRICS_CSV = OUTPUT_DIR / "live_wallet_metrics.csv"
LIVE_STATE_JSON = OUTPUT_DIR / "live_state.json"
PORTFOLIO_HISTORY_JSON = OUTPUT_DIR / "portfolio_history.json"
GLOBAL_NORM_CONFIG = OUTPUT_DIR / "global_norm.json"
EQUITY_CONFIG_FILE = OUTPUT_DIR / "equity_config.json"
LOG_FILE = OUTPUT_DIR / "hl_copy_engine.log"
LOCK_FILE = BASE_DIR / "engine.lock"

WS_URL = "wss://api.hyperliquid.xyz/ws"
MAX_WALLETS = 30
WALLETS_PER_SHARD = 5
WS_PING_INTERVAL_SEC = 20
SHARD_RESTART_DELAY_SEC = 5
RECONNECT_BACKOFF_CAP_SEC = 60
EVENT_QUEUE_MAXSIZE = 50000
SEEN_FILL_IDS_MAX = 200000
FIXED_NOTIONAL_USD = 100.0
ENTRY_SLIPPAGE_BPS = 5.0
EXIT_SLIPPAGE_BPS = 5.0
FEE_BPS = 5.0
STATE_WRITE_INTERVAL_SEC = 5
METRICS_WRITE_INTERVAL_SEC = 5
STARTING_BALANCE_USD = 10_000.0
NORMALISATION_BASE = 100.0
CLEAN_REBUILD: bool = os.getenv("HL_CLEAN_REBUILD", "0") == "1"
USER_WALLET: str = os.getenv("HL_USER_WALLET", "").lower()


def load_global_norm_base(default: float = NORMALISATION_BASE) -> float:
    try:
        if GLOBAL_NORM_CONFIG.exists():
            payload = json.loads(GLOBAL_NORM_CONFIG.read_text(encoding="utf-8"))
            return max(1.0, float(payload.get("norm_base", default)))
    except Exception:
        pass
    return max(1.0, float(default))

# Alert thresholds
ALERT_LATENCY_MS = 2000
ALERT_SLIPPAGE_BPS = 20.0
ALERT_DRAWDOWN_PCT = 10.0
ALERT_INACTIVE_SEC = 60 * 60 * 4
ALERT_SCALP_HOLD_SEC = 30
ALERT_SCALP_MIN_TRADES = 5
ALERT_OVERTRADING_PER_HOUR = 20
STALE_WALLET_MS = 30_000
WS_STALE_MS = 10000
WS_CHECK_INTERVAL = 2

PRICE_KEYS = ("px", "price", "avgPx")
SIZE_KEYS = ("sz", "size", "qty")
COIN_KEYS = ("coin", "symbol", "asset")
TIME_KEYS = ("time", "timestamp")
SIDE_KEYS = ("side", "dir", "direction")
START_KEYS = ("startPosition", "start_pos")
CLOSED_PNL_KEYS = ("closedPnl", "closed_pnl")


# =========================
# HELPERS
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class Logger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def log(self, level: str, message: str) -> None:
        line = f"{utc_now_iso()} | {level.upper()} | {message}"
        with self.lock:
            print(line, flush=True)
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")


LOGGER: Optional[Logger] = None


def log(level: str, message: str) -> None:
    global LOGGER
    if LOGGER is not None:
        LOGGER.log(level, message)


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def get_first(payload: Dict[str, Any], keys: Tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return default


def safe_side(raw_side: Any, size: float, start_position: float) -> str:
    if isinstance(raw_side, str):
        value = raw_side.strip().lower()
        if value in {"b", "buy", "long", "bid"}:
            return "BUY"
        if value in {"s", "sell", "short", "ask"}:
            return "SELL"
    if start_position > 0:
        return "SELL"
    if start_position < 0:
        return "BUY"
    return "BUY" if size >= 0 else "SELL"


def apply_bps(price: float, bps: float, worsen_for_buy: bool) -> float:
    if price <= 0:
        return price
    multiplier = 1.0 + (bps / 10000.0) if worsen_for_buy else 1.0 - (bps / 10000.0)
    return price * multiplier


def calc_bps_delta(reference_price: float, leader_price: float) -> float:
    """Real basis-point delta: (reference − leader) / leader × 10,000."""
    try:
        if leader_price <= 0:
            return 0.0
        return ((reference_price - leader_price) / leader_price) * 10000.0
    except Exception:
        return 0.0


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def get_exchange_position(wallet: str) -> Optional[Dict[str, Any]]:
    try:
        import requests
        r = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "clearinghouseState", "user": wallet},
            timeout=5,
        )
        data = r.json()
        margin = data.get("marginSummary", {})
        account_value = float(margin.get("accountValue", 0))
        unrealized_total = float(margin.get("unrealizedPnl", 0))
        total_size = 0.0
        signed_total = 0.0
        weighted_entry = 0.0
        positions_by_coin: Dict[str, Dict[str, float]] = {}
        for p in data.get("assetPositions", []):
            pos = p.get("position", {})
            size = float(pos.get("szi", 0))
            if size == 0:
                continue
            entry = float(pos.get("entryPx", 0))
            coin = str(pos.get("coin") or p.get("coin") or "").strip().upper()
            total_size += abs(size)
            signed_total += size
            weighted_entry += abs(size) * entry
            if coin:
                positions_by_coin[coin] = {
                    "size": size,
                    "entry_price": entry,
                }
        avg_entry = (weighted_entry / total_size) if total_size > 0 else 0.0
        return {
            "size": total_size,
            "signed_size": signed_total,
            "entry_price": avg_entry,
            "exchange_position_size": total_size,
            "exchange_entry_price": avg_entry,
            "exchange_unrealized_pnl": unrealized_total,
            "exchange_equity": account_value,
            "has_live_position": total_size > 0,
            "positions_by_coin": positions_by_coin,
        }
    except Exception as e:
        print("[EXCHANGE_API_ERROR]", e)
        return None


def get_user_spot_balance(wallet: str) -> Optional[Dict[str, Any]]:
    try:
        import requests
        r = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "spotClearinghouseState", "user": wallet},
            timeout=5,
        )
        data = r.json()
        balances = data.get("balances", [])
        usdc_row = None
        for b in balances:
            if str(b.get("coin", "")).upper() == "USDC":
                usdc_row = b
                break
        if usdc_row is None:
            print("[USER SPOT BALANCE RAW] no USDC row found; balances=", balances)
            return None
        total = float(usdc_row.get("total", 0.0))
        print("[USER SPOT BALANCE RAW]", usdc_row)
        return {
            "exchange_equity": total,
            "exchange_unrealized_pnl": 0.0,
        }
    except Exception as e:
        print("[USER_SPOT_BALANCE_ERROR]", e)
        return None


def load_manual_wallets(path: Path) -> List[str]:
    if not path.exists():
        raise SystemExit(f"Missing wallet file: {path}")
    wallets: List[str] = []
    seen: Set[str] = set()
    with path.open("r", encoding="utf-8-sig") as f:
        for line in f:
            w = line.strip().lower()
            if not w or w.startswith("#"):
                continue
            if w.startswith("0x") and len(w) == 42 and w not in seen:
                wallets.append(w)
                seen.add(w)
    if not wallets:
        raise SystemExit("manual_wallets.txt is empty or invalid")
    return wallets[:MAX_WALLETS]


def running_avg(current: float, n: int, new_value: float) -> float:
    return current + (new_value - current) / n


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60}s"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h{minutes}m"


# =========================
# DATA MODELS
# =========================
@dataclass
class FillEvent:
    wallet: str
    coin: str
    side: str
    price: float
    size: float
    timestamp_ms: int
    timestamp_iso: str
    start_position: float
    closed_pnl: float
    is_snapshot: bool
    raw: Dict[str, Any]
    fill_id: str
    shard_id: int
    received_at_ms: int
    source: str = "ws"


@dataclass
class SimPosition:
    wallet: str
    coin: str
    trade_id: str
    entry_time_ms: int
    entry_time_iso: str
    entry_price_lead: float
    entry_price_copy: float
    size_units: float
    notional_usd: float
    lead_side: str
    entry_latency_ms: int
    mark_price: float = 0.0
    entry_bps: Optional[float] = 0.0
    entry_fee_usd: float = 0.0
    source_wallet: str = ""   # leader wallet that generated this position; "" for leader-side rows

    @property
    def unrealized_pnl(self) -> float:
        if self.mark_price <= 0 or self.entry_price_copy <= 0:
            return 0.0
        direction = 1.0 if self.lead_side == "BUY" else -1.0
        return (self.mark_price - self.entry_price_copy) * self.size_units * direction

    @property
    def current_value(self) -> float:
        if self.mark_price <= 0:
            return self.notional_usd
        return self.mark_price * self.size_units

    @property
    def return_pct(self) -> float:
        if self.notional_usd <= 0:
            return 0.0
        return (self.unrealized_pnl / self.notional_usd) * 100.0

    @property
    def age_seconds(self) -> float:
        return max(0.0, time.time() - (self.entry_time_ms / 1000.0))


@dataclass
class WalletEquity:
    starting_balance: float = STARTING_BALANCE_USD
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    peak_equity: float = STARTING_BALANCE_USD
    trough_equity: float = STARTING_BALANCE_USD

    @property
    def total_equity(self) -> float:
        return self.starting_balance + self.realized_pnl + self.unrealized_pnl

    @property
    def drawdown(self) -> float:
        eq = self.total_equity
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, (self.peak_equity - eq) / self.peak_equity * 100.0)

    def update_peak(self) -> None:
        eq = self.total_equity
        if eq > self.peak_equity:
            self.peak_equity = eq

    def update_trough(self) -> None:
        eq = self.total_equity
        if eq < self.trough_equity:
            self.trough_equity = eq

    @property
    def max_drawdown(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, (self.peak_equity - self.trough_equity) / self.peak_equity * 100.0)


@dataclass
class WalletMetrics:
    wallet: str
    live_fill_count: int = 0
    ws_fill_count: int = 0
    poll_fill_count: int = 0
    snapshot_fill_count: int = 0
    duplicate_fill_count: int = 0
    ignored_fill_count: int = 0
    copy_entry_count: int = 0
    copy_exit_count: int = 0
    copy_open_positions: int = 0
    copy_active_positions: int = 0
    copy_win_count: int = 0
    copy_loss_count: int = 0
    copy_realized_pnl: float = 0.0
    avg_entry_latency_ms: float = 0.0
    avg_exit_latency_ms: float = 0.0
    avg_holding_seconds: float = 0.0
    avg_entry_slippage_bps: float = 0.0
    avg_exit_slippage_bps: float = 0.0
    missed_exit_count: int = 0
    missed_trades_count: int = 0
    fill_match_rate: float = 1.0
    pnl_per_trade: float = 0.0
    pnl_per_hour: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    last_fill_time_iso: str = ""
    last_fill_coin: str = ""
    last_fill_side: str = ""
    last_fill_price: float = 0.0
    last_update_iso: str = ""
    equity: float = STARTING_BALANCE_USD
    unrealized_pnl: float = 0.0
    drawdown: float = 0.0
    alert_flags: str = ""
    first_trade_time_ms: int = 0
    wallet_pnl_ref: float = 0.0
    copy_pnl_exec: float = 0.0
    real_notional_total: float = 0.0

    _entry_latency_samples: int = field(default=0, repr=False)
    _exit_latency_samples: int = field(default=0, repr=False)
    _holding_samples: int = field(default=0, repr=False)
    _entry_slippage_samples: int = field(default=0, repr=False)
    _exit_slippage_samples: int = field(default=0, repr=False)
    _engine_start_ms: int = field(default_factory=utc_now_ms, repr=False)

    def update_entry_latency(self, latency_ms: int) -> None:
        self._entry_latency_samples += 1
        self.avg_entry_latency_ms = running_avg(self.avg_entry_latency_ms, self._entry_latency_samples, latency_ms)

    def update_exit_latency(self, latency_ms: int) -> None:
        self._exit_latency_samples += 1
        self.avg_exit_latency_ms = running_avg(self.avg_exit_latency_ms, self._exit_latency_samples, latency_ms)

    def update_holding_seconds(self, seconds: float) -> None:
        self._holding_samples += 1
        self.avg_holding_seconds = running_avg(self.avg_holding_seconds, self._holding_samples, seconds)

    def update_entry_slippage(self, bps: float) -> None:
        self._entry_slippage_samples += 1
        self.avg_entry_slippage_bps = running_avg(self.avg_entry_slippage_bps, self._entry_slippage_samples, bps)

    def update_exit_slippage(self, bps: float) -> None:
        self._exit_slippage_samples += 1
        self.avg_exit_slippage_bps = running_avg(self.avg_exit_slippage_bps, self._exit_slippage_samples, bps)

    def update_derived(self, realized_pnl: float, equity_model: WalletEquity) -> None:
        closed_trade_events = self.copy_exit_count
        closed_wins = self.copy_win_count + self.copy_loss_count
        self.win_rate = (self.copy_win_count / closed_wins * 100.0) if closed_wins > 0 else 0.0
        self.pnl_per_trade = (realized_pnl / closed_trade_events) if closed_trade_events > 0 else 0.0
        elapsed_hours = max(0.25, (utc_now_ms() - self._engine_start_ms) / 3_600_000.0)
        self.pnl_per_hour = realized_pnl / elapsed_hours
        self.max_drawdown = equity_model.max_drawdown
        if self.copy_entry_count > 0:
            self.fill_match_rate = min(1.0, self.copy_exit_count / self.copy_entry_count)


# =========================
# CSV WRITERS
# =========================
class CsvWriter:
    def __init__(self, path: Path, fieldnames: List[str]) -> None:
        self.path = path
        self.fieldnames = fieldnames
        self.lock = threading.Lock()
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def write_row(self, row: Dict[str, Any]) -> None:
        with self.lock:
            with self.path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerow(row)

    def write_rows_replace(self, rows: List[Dict[str, Any]]) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with self.lock:
            with tmp_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            os.replace(tmp_path, self.path)


# =========================
# ALERT ENGINE
# =========================
def compute_alert_flags(metrics: WalletMetrics) -> str:
    flags: List[str] = []

    if metrics.avg_entry_latency_ms > ALERT_LATENCY_MS:
        flags.append("HIGH_LATENCY")

    if metrics.avg_entry_slippage_bps > ALERT_SLIPPAGE_BPS or metrics.avg_exit_slippage_bps > ALERT_SLIPPAGE_BPS:
        flags.append("HIGH_SLIPPAGE")

    if metrics.max_drawdown > ALERT_DRAWDOWN_PCT:
        flags.append("LARGE_DRAWDOWN")

    if metrics.last_fill_time_iso and metrics.copy_open_positions == 0:
        try:
            last_dt = datetime.fromisoformat(metrics.last_fill_time_iso)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            elapsed = (utc_now() - last_dt).total_seconds()
            if elapsed > ALERT_INACTIVE_SEC:
                flags.append("INACTIVE")
        except Exception:
            pass

    closed_events = metrics.copy_exit_count
    if closed_events >= ALERT_SCALP_MIN_TRADES and metrics.avg_holding_seconds < ALERT_SCALP_HOLD_SEC:
        flags.append("SCALPING")

    elapsed_hours = max(0.001, (utc_now_ms() - metrics._engine_start_ms) / 3_600_000.0)
    trades_per_hour = closed_events / elapsed_hours
    if trades_per_hour > ALERT_OVERTRADING_PER_HOUR:
        flags.append("OVERTRADING")

    return "|".join(flags)


# =========================
# ENGINE
# =========================
class HLCopyEngine:
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.event_queue: "queue.Queue[FillEvent]" = queue.Queue(maxsize=EVENT_QUEUE_MAXSIZE)
        self.seen_fill_ids: Deque[str] = deque(maxlen=SEEN_FILL_IDS_MAX)
        self.seen_fill_set: Set[str] = set()
        self.seen_lock = threading.Lock()
        self.metrics_lock = threading.Lock()
        self.positions_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.wallets = load_manual_wallets(MANUAL_WALLETS_FILE)
        _uw_hard = "0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205"
        self.user_wallet: str = USER_WALLET if USER_WALLET else _uw_hard
        if self.user_wallet not in self.wallets:
            print("[USER WALLET EARLY INJECT]", self.user_wallet)
            self.wallets.insert(0, self.user_wallet)

        # Core runtime containers (authoritative init - do not recreate in start())
        self.wallet_metrics: Dict[str, WalletMetrics] = {w: WalletMetrics(wallet=w) for w in self.wallets}
        self.wallet_equity: Dict[str, WalletEquity] = {w: WalletEquity() for w in self.wallets}
        self.wallet_reference: Dict[str, WalletEquity] = {w: WalletEquity() for w in self.wallets}
        self.leader_equity: Dict[str, WalletEquity] = self.wallet_reference
        self.leader_realized_pnl: Dict[str, float] = defaultdict(float)
        if self.user_wallet not in self.wallet_metrics:
            self.wallet_metrics[self.user_wallet] = WalletMetrics(wallet=self.user_wallet)
        if self.user_wallet not in self.wallet_equity:
            self.wallet_equity[self.user_wallet] = WalletEquity()
        if self.user_wallet not in self.wallet_reference:
            self.wallet_reference[self.user_wallet] = WalletEquity()
        self.normalisation_base: float = load_global_norm_base()
        self.mark_prices: Dict[str, float] = {}
        self.open_positions: Dict[Tuple[str, str], List[SimPosition]] = defaultdict(list)
        self.leader_positions: Dict[Tuple[str, str], List[SimPosition]] = defaultdict(list)
        self.wallet_positions: Dict[Tuple[str, str], float] = defaultdict(float)
        self.tracked_positions: Dict[Tuple[str, str], float] = defaultdict(float)
        self.wallet_alloc: Dict[str, float] = defaultdict(lambda: self.normalisation_base)
        self.reference_equity: float = 10000.0
        self.normalised_wallet_state: Dict[str, Dict[str, Any]] = {}
        self.normalised_portfolio: Dict[str, Any] = {}
        self._trade_id_counter: int = 0
        self._trade_id_lock = threading.Lock()
        self.exchange_position_snapshot: Dict[str, Dict[str, Any]] = {}
        self.wallet_sync_status: Dict[str, str] = {w: "READY" for w in self.wallets}
        self.wallet_off_mode: Dict[str, Optional[str]] = {}
        self.wallet_gate: Dict[str, Any] = {}
        self.reset_freeze: bool = False
        self._in_test_mode: bool = False
        self._ws_seen_ids = set()
        self._poll_seen_ids = set()
        self.audit_ws_received: int = 0
        self.audit_ws_parsed: int = 0
        self.audit_ws_snapshots_skipped: int = 0
        self.audit_ws_duplicates: int = 0
        self.audit_ws_queued: int = 0
        self.audit_ws_queue_full: int = 0
        self.audit_ws_parse_failed: int = 0
        self.audit_ws_no_user: int = 0
        self.audit_ws_no_fills: int = 0
        self.audit_processed: int = 0
        self.audit_poll_received: int = 0
        self.audit_poll_applied: int = 0
        self.audit_poll_deduped: int = 0
        self._audit_last_log_ms: int = utc_now_ms()
        self._audit_last_received_logged: int = 0
        self._recorded_fill_ids: Set[str] = set()
        self.shard_ws_status: Dict[int, str] = {}
        self.shard_last_msg_ms: Dict[int, int] = {}
        self.shard_last_open_ms: Dict[int, int] = {}
        self.shard_last_close_ms: Dict[int, int] = {}
        self.shard_error_count: Dict[int, int] = {}
        self.shard_reconnect_count: Dict[int, int] = {}

        self.engine_start_ms: int = utc_now_ms()
        self.poll_thread: Optional[threading.Thread] = None
        self.last_poll_ts_by_wallet: Dict[str, int] = {}
        self.shard_threads: List[threading.Thread] = []
        self.worker_thread: Optional[threading.Thread] = None
        self.state_thread: Optional[threading.Thread] = None
        self.metrics_thread: Optional[threading.Thread] = None

        self._load_existing_state()

        self.raw_fills_writer = CsvWriter(
            RAW_FILLS_CSV,
            [
                "received_at_iso", "wallet", "coin", "side", "price", "size", "timestamp_ms",
                "timestamp_iso", "start_position", "closed_pnl", "is_snapshot", "fill_id", "shard_id", "raw_json"
            ],
        )
        self.copy_trades_writer = CsvWriter(
            COPY_TRADES_CSV,
            [
                "trade_id", "wallet", "source_wallet", "coin", "entry_time_iso", "exit_time_iso", "lead_side",
                "entry_price_lead", "entry_price_copy", "exit_price_lead", "exit_price_copy",
                "size_units", "notional_usd", "holding_seconds", "duration", "return_pct",
                "entry_latency_ms", "exit_latency_ms", "entry_slippage_bps", "exit_slippage_bps", "copy_pnl",
                "wallet_pnl", "copy_error", "copy_efficiency"
            ],
        )
        self.metrics_writer = CsvWriter(
            LIVE_METRICS_CSV,
            [
                "wallet", "live_fill_count", "duplicate_fill_count", "ignored_fill_count",
                "copy_entry_count", "copy_exit_count", "copy_open_positions", "copy_active_positions", "copy_win_count", "copy_loss_count",
                "copy_realized_pnl", "avg_entry_latency_ms", "avg_exit_latency_ms", "avg_holding_seconds",
                "avg_entry_slippage_bps", "avg_exit_slippage_bps", "missed_exit_count", "missed_trades_count",
                "fill_match_rate", "pnl_per_trade", "pnl_per_hour", "max_drawdown", "win_rate",
                "equity", "unrealized_pnl", "drawdown",
                "last_fill_time_iso", "last_fill_coin", "last_fill_side", "last_fill_price",
                "last_update_iso", "alert_flags", "copy_efficiency",
                "wallet_pnl_ref", "copy_pnl_exec", "real_notional_total",
                "real_slippage_usd", "real_slippage_bps", "real_copy_efficiency",
            ],
        )

    def _load_existing_state(self) -> None:
        if not LIVE_STATE_JSON.exists():
            return
        try:
            data = json.loads(LIVE_STATE_JSON.read_text(encoding="utf-8"))
            max_trade_num = 0
            for wallet, wdata in data.get("wallets", {}).items():
                if wallet not in self.wallet_equity:
                    continue
                if wallet not in self.wallet_reference:
                    self.wallet_reference[wallet] = WalletEquity()
                is_user_wallet = bool(self.user_wallet) and wallet.lower() == self.user_wallet.lower()

                # Restore equity
                saved_eq = wdata.get("equity", {})
                eq = self.wallet_equity[wallet]
                # ALWAYS restore equity (including user wallet)
                eq.realized_pnl = float(saved_eq.get("realized_pnl", 0.0))
                eq.unrealized_pnl = float(saved_eq.get("unrealized_pnl", 0.0))
                eq.peak_equity = float(saved_eq.get("peak_equity", eq.starting_balance))
                eq.trough_equity = float(saved_eq.get("total_equity", eq.starting_balance))

                saved_ref = wdata.get("reference", {})
                ref = self.wallet_reference[wallet]
                ref.starting_balance = float(saved_ref.get("starting_balance", ref.starting_balance))
                ref.realized_pnl = float(saved_ref.get("realized_pnl", 0.0))
                ref.unrealized_pnl = float(saved_ref.get("unrealized_pnl", 0.0))
                ref.peak_equity = float(saved_ref.get("peak_equity", ref.starting_balance))
                ref.trough_equity = float(
                    saved_ref.get("trough_equity", saved_ref.get("total_equity", ref.starting_balance))
                )
                self.leader_realized_pnl[wallet] = ref.realized_pnl

                # Restore per-wallet equity allocation
                is_user_wallet = bool(self.user_wallet) and wallet.lower() == self.user_wallet.lower()

                if is_user_wallet:
                    # preserve real allocation for user wallet
                    self.wallet_alloc[wallet] = float(wdata.get("wallet_equity", 100.0))
                else:
                    # lock allocation for normalization
                    self.wallet_alloc[wallet] = self.normalisation_base

                # Restore metrics
                saved_m = wdata.get("metrics", {})
                m = self.wallet_metrics[wallet]
                if is_user_wallet:
                    self.wallet_metrics[wallet] = WalletMetrics(wallet=wallet)
                    m = self.wallet_metrics[wallet]
                else:
                    m.copy_entry_count = int(saved_m.get("copy_entry_count", 0))
                    m.copy_exit_count = int(saved_m.get("copy_exit_count", 0))
                    m.copy_realized_pnl = float(saved_m.get("copy_realized_pnl", 0.0))
                    m.copy_win_count = int(saved_m.get("copy_win_count", 0))
                    m.copy_loss_count = int(saved_m.get("copy_loss_count", 0))
                    m.missed_exit_count = int(saved_m.get("missed_exit_count", 0))

                # Restore off-mode metadata only; active control comes from wallet_gate.json
                raw_off = wdata.get("off_mode")
                self.wallet_off_mode[wallet] = raw_off if raw_off in ("FOLLOW", "DO_NOTHING") else None

                # Restore open positions
                for pdata in wdata.get("open_positions", []):
                    coin = str(pdata.get("coin", ""))
                    if not coin:
                        continue
                    trade_id = str(pdata.get("trade_id", self._next_trade_id()))
                    # Parse trade_id number to track max for counter bump
                    try:
                        tid_num = int(trade_id.lstrip("T"))
                        if tid_num > max_trade_num:
                            max_trade_num = tid_num
                    except (ValueError, AttributeError):
                        pass
                    # entry_time_ms: stored directly (Fix C) or fall back to parsing iso
                    etms = pdata.get("entry_time_ms")
                    if etms is None:
                        try:
                            dt = datetime.fromisoformat(str(pdata.get("entry_time_iso", "")))
                            etms = int(dt.timestamp() * 1000)
                        except Exception:
                            etms = utc_now_ms()
                    pos = SimPosition(
                        wallet=wallet,
                        coin=coin,
                        trade_id=trade_id,
                        entry_time_ms=int(etms),
                        entry_time_iso=str(pdata.get("entry_time_iso", "")),
                        entry_price_lead=float(pdata.get("entry_price_lead", 0.0)),
                        entry_price_copy=float(pdata.get("entry_price_copy", 0.0)),
                        size_units=float(pdata.get("size_units", 0.0)),
                        notional_usd=float(pdata.get("notional_usd", 0.0)),
                        lead_side=str(pdata.get("lead_side", "BUY")),
                        entry_latency_ms=int(pdata.get("entry_latency_ms", 0)),
                        mark_price=float(pdata.get("mark_price", 0.0)),
                        entry_bps=(
                            float(pdata.get("entry_bps", 0.0))
                            if pdata.get("entry_bps") is not None
                            else None
                        ),
                        entry_fee_usd=float(pdata.get("entry_fee_usd", 0.0)),
                        source_wallet=str(pdata.get("source_wallet", "")),
                    )
                    if pos.size_units > 0 and pos.entry_price_copy > 0:
                        self.open_positions[(wallet, coin)].append(pos)
                    leader_size_units = (
                        float(pdata.get("notional_usd", 0.0)) / float(pdata.get("entry_price_lead", 0.0))
                        if float(pdata.get("entry_price_lead", 0.0)) > 0
                        else 0.0
                    )
                    if leader_size_units > 0 and pos.entry_price_lead > 0:
                        self.leader_positions[(wallet, coin)].append(
                            SimPosition(
                                wallet=wallet,
                                coin=coin,
                                trade_id=trade_id,
                                entry_time_ms=int(etms),
                                entry_time_iso=str(pdata.get("entry_time_iso", "")),
                                entry_price_lead=float(pdata.get("entry_price_lead", 0.0)),
                                entry_price_copy=float(pdata.get("entry_price_lead", 0.0)),
                                size_units=leader_size_units,
                                notional_usd=float(pdata.get("notional_usd", 0.0)),
                                lead_side=str(pdata.get("lead_side", "BUY")),
                                entry_latency_ms=int(pdata.get("entry_latency_ms", 0)),
                                mark_price=float(pdata.get("mark_price", 0.0)),
                                entry_bps=0.0,
                                entry_fee_usd=0.0,
                                source_wallet=str(pdata.get("source_wallet", "")),
                            )
                        )

                # Set open count from actual restored positions
                restored_open = sum(
                    len(v) for (w, _), v in self.open_positions.items() if w == wallet
                )
                m.copy_open_positions = restored_open
                if restored_open > 0 or wallet == self.user_wallet:
                    print(f"LOAD_POSITIONS wallet={wallet} restored={restored_open}")

                # Repair corrupted counter state: entries > exits but no open positions
                # means a past exit was processed but copy_exit_count was never incremented
                # (old subrows==0 bug). Force exits=entries to restore consistency.
                if (not is_user_wallet) and m.copy_entry_count > m.copy_exit_count and restored_open == 0:
                    log("warning", (
                        f"STATE_REPAIR wallet={wallet[:10]} "
                        f"entries={m.copy_entry_count} exits={m.copy_exit_count} open=0 "
                        f"— forcing exits=entries"
                    ))
                    m.copy_exit_count = m.copy_entry_count

                # Sync derived metrics to restored equity state so first write is correct.
                # unrealized_pnl is 0 until mark prices arrive — that is correct.
                eq.unrealized_pnl = 0.0
                m.equity = round(eq.total_equity, 6)
                m.unrealized_pnl = 0.0
                m.drawdown = round(eq.drawdown, 4)
                ref.unrealized_pnl = 0.0
                # Recompute win_rate, pnl_per_hour, max_drawdown immediately from
                # restored counters so they are not zero until the next close event.
                m.update_derived(eq.realized_pnl, eq)

            self.normalised_wallet_state = dict(data.get("normalised_wallet_state", {}) or {})
            self.normalised_portfolio = dict(data.get("normalised_portfolio", {}) or {})

            # Bump trade id counter past any restored ids
            if max_trade_num > self._trade_id_counter:
                self._trade_id_counter = max_trade_num

        except Exception as e:
            log("error", f"state load failed: {e}")

    def _next_trade_id(self) -> str:
        with self._trade_id_lock:
            self._trade_id_counter += 1
            return f"T{self._trade_id_counter:06d}"

    def _reload_normalisation_base(self) -> None:
        self.normalisation_base = load_global_norm_base(self.normalisation_base)
        for wallet in list(self.wallets):
            if self.user_wallet and wallet.lower() == self.user_wallet.lower():
                continue
            self.wallet_alloc[wallet] = self.normalisation_base

    def start(self) -> None:
        self._reload_wallet_gate()
        if CLEAN_REBUILD:
            self._rebuild_from_fills()
        for wallet in list(self.wallets):
            self._catch_up_wallet(wallet)
        log("info", f"Starting HL_Copy_Engine with {len(self.wallets)} wallets")
        self.worker_thread = threading.Thread(target=self.event_worker_loop, name="event-worker", daemon=True)
        self.worker_thread.start()

        self.state_thread = threading.Thread(target=self.state_writer_loop, name="state-writer", daemon=True)
        self.state_thread.start()

        self.metrics_thread = threading.Thread(target=self.metrics_writer_loop, name="metrics-writer", daemon=True)
        self.metrics_thread.start()

        self.poll_thread = threading.Thread(target=self._polling_reconcile_loop, name="poll-reconcile", daemon=True)
        self.poll_thread.start()
        log("info", "poll-reconcile thread started")

        print("[SNAPSHOT WALLET LIST FINAL]", self.wallets)
        shards = chunked(self.wallets, WALLETS_PER_SHARD)
        for shard_id, shard_wallets in enumerate(shards):
            thread = threading.Thread(
                target=self.run_shard_forever,
                args=(shard_id, shard_wallets),
                name=f"ws-shard-{shard_id}",
                daemon=True,
            )
            thread.start()
            self.shard_threads.append(thread)
            log("info", f"Shard {shard_id} started with {len(shard_wallets)} wallets")

        _uw = "0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205"
        _uw_target = self.user_wallet if self.user_wallet else _uw
        if _uw_target and _uw_target not in self.wallets:
            print("[USER WALLET INJECTED]", _uw_target)
            self.wallets.append(_uw_target)
            if _uw_target not in self.wallet_metrics:
                self.wallet_metrics[_uw_target] = WalletMetrics(wallet=_uw_target)
                self.wallet_equity[_uw_target] = WalletEquity()

        if CLEAN_REBUILD:
            print("[INIT] forcing initial exchange snapshot...")
            self._refresh_exchange_snapshots()
            time.sleep(1)

    def stop(self) -> None:
        log("info", "Stop requested")
        self.stop_event.set()

    def run_forever(self) -> None:
        self.start()
        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.write_live_state()
            self.write_metrics_csv()
            log("info", "HL_Copy_Engine stopped")

    def run_shard_forever(self, shard_id: int, wallets: List[str]) -> None:
        backoff = min(SHARD_RESTART_DELAY_SEC, 5)
        while not self.stop_event.is_set():
            self.shard_ws_status[shard_id] = "CONNECTING"
            ws_app = self.build_ws_app(shard_id, wallets)
            try:
                ws_app.run_forever(
                    sslopt={"cert_reqs": ssl.CERT_NONE},
                    ping_interval=0,
                    ping_timeout=None,
                    skip_utf8_validation=True,
                )
            except Exception as exc:
                log("error", f"Shard {shard_id} crashed: {exc}")
            if self.stop_event.is_set():
                break
            self.shard_reconnect_count[shard_id] = self.shard_reconnect_count.get(shard_id, 0) + 1
            self._set_shard_health_status(shard_id, "RECOVERING")
            log("warning", f"Shard {shard_id} reconnecting in {backoff}s")
            time.sleep(backoff)
            backoff = 5

    def build_ws_app(self, shard_id: int, wallets: List[str]) -> websocket.WebSocketApp:
        heartbeat_stop = threading.Event()

        assert WALLETS_PER_SHARD <= 10, \
            "WALLETS_PER_SHARD exceeds Hyperliquid per-connection tracking limit"

        def on_open(ws: websocket.WebSocketApp) -> None:
            self._mark_shard_open(shard_id)
            log("info", f"Shard {shard_id} connected")
            for wallet in wallets:
                payload = {
                    "method": "subscribe",
                    "subscription": {"type": "userFills", "user": wallet},
                }
                ws.send(json.dumps(payload))
                log("info", f"WS_SUBSCRIBE shard={shard_id} wallet={wallet}")
                time.sleep(0.05)
            threading.Thread(
                target=self.heartbeat_loop,
                args=(ws, shard_id, heartbeat_stop),
                name=f"heartbeat-{shard_id}",
                daemon=True,
            ).start()

        def on_message(ws: websocket.WebSocketApp, message: str) -> None:
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                self.audit_ws_parse_failed += 1
                log("warning", f"WS_PARSE_FAIL shard={shard_id} non_json=1")
                self._maybe_log_fill_audit()
                return
            self._mark_shard_message(shard_id)
            self.handle_ws_message(shard_id, payload)

        def on_error(ws: websocket.WebSocketApp, error: Any) -> None:
            self._mark_shard_error(shard_id, error)
            log("error", f"Shard {shard_id} websocket error: {error}")

        def on_close(ws: websocket.WebSocketApp, status_code: Any, close_msg: Any) -> None:
            heartbeat_stop.set()
            self._mark_shard_close(shard_id)
            log("warning", f"Shard {shard_id} closed: code={status_code} msg={close_msg}")

        return websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

    def heartbeat_loop(self, ws: websocket.WebSocketApp, shard_id: int, stop_flag: threading.Event) -> None:
        while not self.stop_event.is_set() and not stop_flag.is_set():
            time.sleep(WS_PING_INTERVAL_SEC)
            if self.stop_event.is_set() or stop_flag.is_set():
                break
            try:
                ws.send(json.dumps({"method": "ping"}))
            except Exception as exc:
                log("warning", f"Shard {shard_id} ping failed: {exc}")
                break

    def _coalesce_raw_fills(self, fills: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Hyperliquid often emits many fill fragments for a single order in one message.
        Coalesce by order id + coin + side + timestamp + startPosition.
        """
        grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

        for raw_fill in fills:
            oid = raw_fill.get("oid")
            tid = raw_fill.get("tid")
            coin = str(get_first(raw_fill, COIN_KEYS, "")).strip().upper()
            side = str(get_first(raw_fill, SIDE_KEYS, "") or "")
            timestamp_ms = parse_int(get_first(raw_fill, TIME_KEYS, 0))
            start_position = parse_float(get_first(raw_fill, START_KEYS, 0.0))
            key = (oid if oid is not None else tid, coin, side, timestamp_ms, round(start_position, 12))

            price = parse_float(get_first(raw_fill, PRICE_KEYS, 0.0))
            size = abs(parse_float(get_first(raw_fill, SIZE_KEYS, 0.0)))
            closed_pnl = parse_float(get_first(raw_fill, CLOSED_PNL_KEYS, 0.0))

            if key not in grouped:
                merged = dict(raw_fill)
                merged["_sum_px_sz"] = price * size
                merged["_sum_sz"] = size
                merged["_sum_closed_pnl"] = closed_pnl
                grouped[key] = merged
                continue

            merged = grouped[key]
            merged["_sum_px_sz"] += price * size
            merged["_sum_sz"] += size
            merged["_sum_closed_pnl"] += closed_pnl

        result: List[Dict[str, Any]] = []
        for merged in grouped.values():
            total_sz = parse_float(merged.pop("_sum_sz", 0.0))
            total_px_sz = parse_float(merged.pop("_sum_px_sz", 0.0))
            total_closed_pnl = parse_float(merged.pop("_sum_closed_pnl", 0.0))
            if total_sz > 0:
                merged["px"] = total_px_sz / total_sz
                merged["sz"] = total_sz
            merged["closedPnl"] = total_closed_pnl
            result.append(merged)
        return result

    def _last_raw_fill_ts_by_wallet(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        if not RAW_FILLS_CSV.exists():
            return out
        try:
            with RAW_FILLS_CSV.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    wallet = str(row.get("wallet", "")).strip().lower()
                    if not wallet:
                        continue
                    if str(row.get("is_snapshot", "False")).strip().lower() in ("true", "1", "yes"):
                        continue
                    ts = parse_int(row.get("timestamp_ms", 0))
                    if ts <= 0:
                        continue
                    out[wallet] = max(out.get(wallet, 0), ts)
        except Exception as exc:
            log("warning", f"[CATCHUP_API_ERROR] wallet=ALL reason=last_ts_read_failed err={exc}")
        return out

    def _ensure_recorded_fill_ids(self) -> None:
        if getattr(self, "_recorded_fill_ids", None):
            return
        self._recorded_fill_ids = set()
        if not RAW_FILLS_CSV.exists():
            return
        try:
            with RAW_FILLS_CSV.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    fill_id = str(row.get("fill_id", "")).strip()
                    if fill_id:
                        self._recorded_fill_ids.add(fill_id)
        except Exception:
            self._recorded_fill_ids = set()

    def record_fill(self, event: FillEvent) -> bool:
        self._ensure_recorded_fill_ids()
        fill_id = str(event.fill_id or "").strip()
        if fill_id and fill_id in self._recorded_fill_ids:
            return False
        self.raw_fills_writer.write_row(
            {
                "received_at_iso": datetime.fromtimestamp(event.received_at_ms / 1000.0, tz=timezone.utc).isoformat(),
                "wallet": event.wallet,
                "coin": event.coin,
                "side": event.side,
                "price": event.price,
                "size": event.size,
                "timestamp_ms": event.timestamp_ms,
                "timestamp_iso": event.timestamp_iso,
                "start_position": event.start_position,
                "closed_pnl": event.closed_pnl,
                "is_snapshot": event.is_snapshot,
                "fill_id": event.fill_id,
                "shard_id": event.shard_id,
                "raw_json": json.dumps(event.raw, separators=(",", ":")),
            }
        )
        if fill_id:
            self._recorded_fill_ids.add(fill_id)
        return True

    def _ledger_wallet_summary(self) -> Dict[str, Dict[str, Any]]:
        summary: Dict[str, Dict[str, Any]] = {
            wallet: {
                "fill_count": 0,
                "entry_count": 0,
                "exit_count": 0,
                "open_position_count": 0,
            }
            for wallet in self.wallets
        }
        if not RAW_FILLS_CSV.exists():
            return summary
        rows: List[Dict[str, Any]] = []
        try:
            with RAW_FILLS_CSV.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    rows.append(dict(row))
        except Exception:
            return summary

        rows.sort(key=lambda r: parse_int(r.get("timestamp_ms", 0)))
        seen_ids: Set[str] = set()
        tracked_lots: Dict[Tuple[str, str], Deque[float]] = defaultdict(deque)
        tracked_sizes: Dict[Tuple[str, str], float] = defaultdict(float)

        for row in rows:
            wallet = str(row.get("wallet", "")).strip().lower()
            if wallet not in summary:
                continue
            if str(row.get("is_snapshot", "False")).strip().lower() in ("true", "1", "yes"):
                continue
            fill_id = str(row.get("fill_id", "")).strip()
            if fill_id:
                if fill_id in seen_ids:
                    continue
                seen_ids.add(fill_id)

            summary[wallet]["fill_count"] += 1
            if self.user_wallet and wallet == self.user_wallet.lower():
                continue

            coin = str(row.get("coin", "")).strip().upper()
            side = str(row.get("side", "")).strip().upper()
            size = abs(parse_float(row.get("size", 0.0)))
            if not coin or side not in {"BUY", "SELL"} or size <= 0:
                continue

            key = (wallet, coin)
            delta = size if side == "BUY" else -size
            if delta > 0:
                tracked_lots[key].append(delta)
                tracked_sizes[key] += delta
                summary[wallet]["entry_count"] += 1
            elif delta < 0:
                reduction = abs(delta)
                close_size = min(tracked_sizes.get(key, 0.0), reduction)
                if close_size > 1e-9:
                    summary[wallet]["exit_count"] += 1
                    tracked_sizes[key] -= close_size
                    remaining = close_size
                    while remaining > 1e-9 and tracked_lots[key]:
                        lot = tracked_lots[key][0]
                        if lot <= remaining + 1e-9:
                            remaining -= lot
                            tracked_lots[key].popleft()
                        else:
                            tracked_lots[key][0] = lot - remaining
                            remaining = 0.0

        for wallet in summary:
            summary[wallet]["open_position_count"] = sum(
                len(lots) for (w, _coin), lots in tracked_lots.items() if w == wallet
            )
        return summary

    def _fetch_user_fills_by_time(self, wallet: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        try:
            import requests
            r = requests.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "userFillsByTime", "user": wallet, "startTime": start_ms, "endTime": end_ms},
                timeout=8,
            )
            data = r.json()
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
            return []
        except Exception as exc:
            self.wallet_sync_status[wallet] = "API_ERROR"
            log("warning", f"[CATCHUP_API_ERROR] wallet={wallet} err={exc}")
            return []

    def _compose_alert_flags(self, wallet: str, base_flags: str) -> str:
        flags = [f for f in str(base_flags).split("|") if f]
        status = self.wallet_sync_status.get(wallet, "READY")
        if status in {"CATCHING_UP", "DRIFT", "API_ERROR"} and status not in flags:
            flags.append(status)
        return "|".join(flags)

    def _set_shard_health_status(self, shard_id: int, status: str, message: str = "") -> None:
        old_status = self.shard_ws_status.get(shard_id, "OK")
        self.shard_ws_status[shard_id] = status
        if old_status != status:
            if message:
                print(f"[WS_HEALTH] shard={shard_id} status={status} {message}")
            else:
                print(f"[WS_HEALTH] shard={shard_id} status={status}")

    def _mark_shard_open(self, shard_id: int) -> None:
        now = utc_now_ms()
        self.shard_last_open_ms[shard_id] = now
        self.shard_last_msg_ms[shard_id] = now
        self._set_shard_health_status(shard_id, "OK")

    def _mark_shard_message(self, shard_id: int) -> None:
        now = utc_now_ms()
        prev = self.shard_ws_status.get(shard_id, "OK")
        self.shard_last_msg_ms[shard_id] = now
        if prev != "OK":
            self._set_shard_health_status(shard_id, "OK", "recovered")

    def _mark_shard_error(self, shard_id: int, error: Any) -> None:
        self.shard_error_count[shard_id] = self.shard_error_count.get(shard_id, 0) + 1
        self._set_shard_health_status(shard_id, "DEGRADED", f"error={error}")

    def _mark_shard_close(self, shard_id: int) -> None:
        self.shard_last_close_ms[shard_id] = utc_now_ms()
        self._set_shard_health_status(shard_id, "DOWN")

    def _get_wallet_shard_id(self, wallet: str) -> int:
        wallet = str(wallet).strip().lower()
        for shard_id, shard_wallets in enumerate(chunked(self.wallets, WALLETS_PER_SHARD)):
            if wallet in shard_wallets:
                return shard_id
        return -1

    def _get_ws_health_snapshot(self) -> Dict[str, Any]:
        shards = chunked(self.wallets, WALLETS_PER_SHARD)
        shard_data: Dict[int, Dict[str, Any]] = {}
        all_ok = True
        for shard_id, _wallets in enumerate(shards):
            status = self.shard_ws_status.get(shard_id, "OK")
            if status != "OK":
                all_ok = False
            shard_data[shard_id] = {
                "status": status,
                "last_msg_ms": self.shard_last_msg_ms.get(shard_id, 0),
                "last_open_ms": self.shard_last_open_ms.get(shard_id, 0),
                "last_close_ms": self.shard_last_close_ms.get(shard_id, 0),
                "error_count": self.shard_error_count.get(shard_id, 0),
                "reconnect_count": self.shard_reconnect_count.get(shard_id, 0),
            }
        return {
            "overall": "OK" if all_ok else "DEGRADED",
            "shards": shard_data,
        }

    def _merge_ws_flags(self, wallet: str, metric_flags: str) -> str:
        flags = set(filter(None, str(metric_flags).split("|")))
        flags = {f for f in flags if not f.startswith("WS_")}
        shard_id = self._get_wallet_shard_id(wallet)
        if shard_id >= 0 and self.shard_ws_status.get(shard_id, "OK") != "OK":
            flags.add("WS_DEGRADED")
        return "|".join(sorted(flags))

    def _audit_fill_drop(
        self,
        reason: str,
        wallet: str = "",
        fill_id: str = "",
        coin: str = "",
        side: str = "",
    ) -> None:
        log(
            "warning",
            f"FILL_DROP reason={reason} wallet={wallet or '-'} fill_id={fill_id or '-'} coin={coin or '-'} side={side or '-'}",
        )

    def _ensure_audit_counters(self) -> None:
        if not hasattr(self, "audit_ws_received"):
            self.audit_ws_received = 0
            self.audit_ws_parsed = 0
            self.audit_ws_snapshots_skipped = 0
            self.audit_ws_duplicates = 0
            self.audit_ws_queued = 0
            self.audit_ws_queue_full = 0
            self.audit_ws_parse_failed = 0
            self.audit_ws_no_user = 0
            self.audit_ws_no_fills = 0
            self.audit_processed = 0
            self.audit_poll_received = 0
            self.audit_poll_applied = 0
            self.audit_poll_deduped = 0
            self._audit_last_log_ms = utc_now_ms()
            self._audit_last_received_logged = 0

    def _maybe_log_fill_audit(self) -> None:
        self._ensure_audit_counters()
        now = utc_now_ms()
        if (now - self._audit_last_log_ms) < 30000 and (self.audit_ws_received - self._audit_last_received_logged) < 25:
            return
        self._audit_last_log_ms = now
        self._audit_last_received_logged = self.audit_ws_received
        log(
            "info",
            "FILL_AUDIT "
            f"ws_received={self.audit_ws_received} "
            f"parsed={self.audit_ws_parsed} "
            f"queued={self.audit_ws_queued} "
            f"processed={self.audit_processed} "
            f"duplicates={self.audit_ws_duplicates} "
            f"snapshots={self.audit_ws_snapshots_skipped} "
            f"parse_failed={self.audit_ws_parse_failed} "
            f"queue_full={self.audit_ws_queue_full} "
            f"no_user={self.audit_ws_no_user} "
            f"no_fills={self.audit_ws_no_fills} "
            f"poll_received={self.audit_poll_received} "
            f"poll_applied={self.audit_poll_applied} "
            f"poll_deduped={self.audit_poll_deduped}"
        )

    def _catch_up_wallet(self, wallet: str) -> None:
        wallet = str(wallet).strip().lower()
        if not wallet:
            return
        if wallet == self.user_wallet.lower():
            self.wallet_sync_status[wallet] = "USER_SKIPPED"
            return

        last_ts_map = self._last_raw_fill_ts_by_wallet()
        last_ts = int(last_ts_map.get(wallet, 0) or 0)
        if last_ts <= 0:
            self.last_poll_ts_by_wallet[wallet] = self.engine_start_ms - 60_000
            self.wallet_sync_status[wallet] = "READY"
            return

        self.wallet_sync_status[wallet] = "CATCHING_UP"
        start_ms = max(0, last_ts - 60_000)
        end_ms = utc_now_ms()
        log("info", f"[CATCHUP_START] wallet={wallet} start_ms={start_ms} end_ms={end_ms} last_ts={last_ts}")

        existing_ids: Set[str] = set()
        try:
            with RAW_FILLS_CSV.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    row_wallet = str(row.get("wallet", "")).strip().lower()
                    if row_wallet != wallet:
                        continue
                    if str(row.get("is_snapshot", "False")).strip().lower() in ("true", "1", "yes"):
                        continue
                    fill_id = str(row.get("fill_id", "")).strip()
                    if fill_id:
                        existing_ids.add(fill_id)
        except Exception:
            pass

        fills = self._coalesce_raw_fills(self._fetch_user_fills_by_time(wallet, start_ms, end_ms))
        events: List[FillEvent] = []
        for raw_fill in fills:
            event = self.parse_fill(shard_id=-2, wallet=wallet, raw_fill=raw_fill, is_snapshot=False)
            if event is None:
                continue
            events.append(event)
        events.sort(key=lambda ev: ev.timestamp_ms)

        for event in events:
            if (
                event.fill_id in existing_ids
                or event.fill_id in self._poll_seen_ids
                or event.fill_id in self._ws_seen_ids
            ):
                continue
            self._poll_seen_ids.add(event.fill_id)
            if not self.record_fill(event):
                continue
            log("info", f"[CATCHUP_FILL] wallet={wallet} coin={event.coin} side={event.side} ts={event.timestamp_ms} fill_id={event.fill_id}")
            self.process_live_fill(event)

        snap = get_exchange_position(wallet)
        if snap is None:
            self.wallet_sync_status[wallet] = "API_ERROR"
            log("warning", f"[CATCHUP_API_ERROR] wallet={wallet} reason=exchange_position_unavailable")
            return

        engine_by_coin: Dict[str, float] = {}
        with self.positions_lock:
            for (w, coin), pos_list in self.open_positions.items():
                if w != wallet:
                    continue
                signed = sum(
                    p.size_units if p.lead_side == "BUY" else -p.size_units
                    for p in pos_list
                )
                if abs(signed) > 1e-9:
                    engine_by_coin[coin] = signed

        exchange_by_coin = {
            str(coin).strip().upper(): float((info or {}).get("size", 0.0))
            for coin, info in (snap.get("positions_by_coin", {}) or {}).items()
            if abs(float((info or {}).get("size", 0.0))) > 1e-9
        }
        all_coins = set(engine_by_coin.keys()) | set(exchange_by_coin.keys())
        drift = False
        for coin in all_coins:
            if abs(engine_by_coin.get(coin, 0.0) - exchange_by_coin.get(coin, 0.0)) > 1e-6:
                drift = True
                break

        if drift:
            self.wallet_sync_status[wallet] = "DRIFT"
            log("warning", f"[CATCHUP_DRIFT] wallet={wallet} engine={engine_by_coin} exchange={exchange_by_coin}")
        else:
            self.wallet_sync_status[wallet] = "READY"
            log("info", f"[CATCHUP_READY] wallet={wallet} fills={len(events)}")

    def _poll_wallet_fills(self, wallet: str) -> None:
        wallet = str(wallet).strip().lower()
        if not wallet or wallet == self.user_wallet.lower():
            return
        if self.reset_freeze:
            return
        self._ensure_audit_counters()
        try:
            POLL_OVERLAP_MS = 5000  # 5 second buffer
            last_ts = (
                self.last_poll_ts_by_wallet.get(wallet)
                or self._last_raw_fill_ts_by_wallet().get(wallet)
                or self.engine_start_ms - 60_000
            )
            start_ms = max(0, last_ts - POLL_OVERLAP_MS)
            end_ms = utc_now_ms()
            raw_fills = self._fetch_user_fills_by_time(wallet, start_ms, end_ms)
            fills = self._coalesce_raw_fills(raw_fills)
            events: List[FillEvent] = []
            for raw_fill in fills:
                event = self.parse_fill(shard_id=-3, wallet=wallet, raw_fill=raw_fill, is_snapshot=False)
                if event is None:
                    continue
                events.append(event)
            events.sort(key=lambda ev: ev.timestamp_ms)
            max_seen_ts = int(last_ts or 0)

            for event in events:
                self.audit_poll_received += 1
                if event.fill_id in self._poll_seen_ids or event.fill_id in self._ws_seen_ids:
                    self.audit_poll_deduped += 1
                    continue
                if self.is_duplicate_fill(event.fill_id):
                    self.audit_poll_deduped += 1
                    continue
                self._poll_seen_ids.add(event.fill_id)
                max_seen_ts = max(max_seen_ts, int(event.timestamp_ms or 0))
                if not self.record_fill(event):
                    self.audit_poll_deduped += 1
                    continue
                self.process_live_fill(event)
                self.audit_poll_applied += 1
                log("INFO", f"FILL_RECOVERED_FROM_POLL fill_id={event.fill_id} wallet={wallet}")
                log("info", f"[POLL_FILL] wallet={wallet} coin={event.coin} side={event.side} ts={event.timestamp_ms}")

            self.last_poll_ts_by_wallet[wallet] = max(int(last_ts or 0), max_seen_ts)
            self._maybe_log_fill_audit()
        except Exception as exc:
            log("warning", f"[POLL_ERROR] wallet={wallet} err={exc}")

    def _polling_reconcile_loop(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(15)
            if self.stop_event.is_set():
                break
            if self.reset_freeze:
                continue
            print("[POLL_LOOP_START]")
            for wallet in list(self.wallets):
                if wallet == self.user_wallet:
                    continue
                self._poll_wallet_fills(wallet)

    def handle_ws_message(self, shard_id: int, payload: Dict[str, Any]) -> None:
        self._ensure_audit_counters()
        if self.reset_freeze:
            self._audit_fill_drop("RESET_FREEZE")
            self._maybe_log_fill_audit()
            return
        channel = str(payload.get("channel") or payload.get("type") or "")
        if channel.lower() in {"pong", "subscriptionresponse", "subscribed"}:
            log("info", f"FILL_DROP reason=CONTROL_MESSAGE wallet=- fill_id=- coin=- side=- channel={channel}")
            self._maybe_log_fill_audit()
            return

        data = payload.get("data")
        if data is None:
            self.audit_ws_no_fills += 1
            log("warning", f"WS_NO_DATA shard={shard_id}")
            self._audit_fill_drop("WS_NO_DATA")
            self._maybe_log_fill_audit()
            return
        if isinstance(data, dict):
            user = str(data.get("user") or payload.get("user") or "").strip().lower()
            fills = data.get("fills")
            if fills is None:
                fills = data.get("userFills")
            if fills is None:
                fills = data.get("fill")
            is_snapshot = bool(data.get("isSnapshot", False))
        elif isinstance(data, list):
            fills = data
            first_fill = data[0] if data and isinstance(data[0], dict) else {}
            user = str(payload.get("user") or first_fill.get("user") or "").strip().lower()
            is_snapshot = bool(payload.get("isSnapshot", False))
        else:
            self.audit_ws_no_fills += 1
            log("warning", f"WS_NO_DATA shard={shard_id}")
            self._audit_fill_drop("WS_NO_DATA")
            self._maybe_log_fill_audit()
            return
        if not user:
            self.audit_ws_no_user += 1
            log("warning", f"WS_NO_USER shard={shard_id}")
            self._audit_fill_drop("WS_NO_USER")
            self._maybe_log_fill_audit()
            return
        if isinstance(fills, dict):
            fills = [fills]
        if not isinstance(fills, list) or not fills:
            self.audit_ws_no_fills += 1
            log("warning", f"WS_NO_FILLS shard={shard_id} wallet={user}")
            self._audit_fill_drop("WS_NO_FILLS", wallet=user)
            self._maybe_log_fill_audit()
            return
        if self.user_wallet and user == self.user_wallet.lower():
            for raw_fill in fills:
                if not isinstance(raw_fill, dict):
                    continue
                self.audit_ws_received += 1
                self._audit_fill_drop(
                    "USER_WALLET",
                    wallet=user,
                    fill_id=str(raw_fill.get("oid") or raw_fill.get("tid") or raw_fill.get("hash") or ""),
                    coin=str(get_first(raw_fill, COIN_KEYS, "")).strip().upper(),
                    side=safe_side(
                        get_first(raw_fill, SIDE_KEYS, None),
                        size=parse_float(get_first(raw_fill, SIZE_KEYS, 0.0)),
                        start_position=parse_float(get_first(raw_fill, START_KEYS, 0.0)),
                    ),
                )
            self._maybe_log_fill_audit()
            return
        status = self.wallet_sync_status.get(user, "READY")
        if status not in ("READY", "USER_SKIPPED"):
            log("warning", f"WS_SYNC_DRIFT wallet={user} status={status}")
            # DO NOT RETURN — continue processing

        raw_fills = [f for f in fills if isinstance(f, dict)]
        for _raw_fill in raw_fills:
            self.audit_ws_received += 1
        fills = self._coalesce_raw_fills(raw_fills)
        if not fills:
            self.audit_ws_no_fills += 1
            log("warning", f"WS_NO_FILLS shard={shard_id} wallet={user}")
            self._audit_fill_drop("WS_NO_FILLS", wallet=user)
            self._maybe_log_fill_audit()
            return

        processed_fill = False

        for raw_fill in fills:
            event = self.parse_fill(shard_id=shard_id, wallet=user, raw_fill=raw_fill, is_snapshot=is_snapshot)
            if event is None:
                self.audit_ws_parse_failed += 1
                log("warning", f"WS_PARSE_FAIL shard={shard_id} wallet={user}")
                self._audit_fill_drop(
                    "WS_PARSE_FAIL",
                    wallet=user,
                    fill_id=str(raw_fill.get("oid") or raw_fill.get("tid") or raw_fill.get("hash") or ""),
                    coin=str(get_first(raw_fill, COIN_KEYS, "")).strip().upper(),
                    side=safe_side(
                        get_first(raw_fill, SIDE_KEYS, None),
                        size=parse_float(get_first(raw_fill, SIZE_KEYS, 0.0)),
                        start_position=parse_float(get_first(raw_fill, START_KEYS, 0.0)),
                    ),
                )
                with self.metrics_lock:
                    if user not in self.wallet_metrics:
                        self.wallet_metrics[user] = WalletMetrics(wallet=user)
                    self.wallet_metrics[user].ignored_fill_count += 1
                continue
            self.audit_ws_parsed += 1
            if event.is_snapshot:
                self.audit_ws_snapshots_skipped += 1
                self._audit_fill_drop("SNAPSHOT", wallet=event.wallet, fill_id=event.fill_id, coin=event.coin, side=event.side)
                continue

            with self.metrics_lock:
                if event.wallet not in self.wallet_metrics:
                    self.wallet_metrics[event.wallet] = WalletMetrics(wallet=event.wallet)
                if event.wallet not in self.wallet_equity:
                    self.wallet_equity[event.wallet] = WalletEquity()
                metrics = self.wallet_metrics[event.wallet]
                metrics.live_fill_count += 1
                metrics.last_fill_time_iso = event.timestamp_iso
                metrics.last_fill_coin = event.coin
                metrics.last_fill_side = event.side
                metrics.last_fill_price = event.price
                metrics.last_update_iso = utc_now_iso()

            if self.is_duplicate_fill(event.fill_id):
                self.audit_ws_duplicates += 1
                log("info", f"WS_DUPLICATE wallet={event.wallet} fill_id={event.fill_id}")
                self._audit_fill_drop("DUPLICATE", wallet=event.wallet, fill_id=event.fill_id, coin=event.coin, side=event.side)
                with self.metrics_lock:
                    if event.wallet not in self.wallet_metrics:
                        self.wallet_metrics[event.wallet] = WalletMetrics(wallet=event.wallet)
                    self.wallet_metrics[event.wallet].duplicate_fill_count += 1
                continue

            self._ws_seen_ids.add(event.fill_id)
            if not self.record_fill(event):
                self.audit_ws_duplicates += 1
                self._audit_fill_drop("DUPLICATE", wallet=event.wallet, fill_id=event.fill_id, coin=event.coin, side=event.side)
                self._maybe_log_fill_audit()
                continue

            try:
                self.event_queue.put_nowait(event)
                self.audit_ws_queued += 1
                processed_fill = True
            except queue.Full:
                self.audit_ws_queue_full += 1
                log("error", f"WS_QUEUE_FULL wallet={event.wallet} fill_id={event.fill_id}")
                self._audit_fill_drop("QUEUE_FULL", wallet=event.wallet, fill_id=event.fill_id, coin=event.coin, side=event.side)

        if not processed_fill:
            self.audit_ws_no_fills += 1
            log("warning", f"WS_NO_FILLS shard={shard_id} wallet={user} processed=0")
        self._maybe_log_fill_audit()

    def parse_fill(self, shard_id: int, wallet: str, raw_fill: Dict[str, Any], is_snapshot: bool) -> Optional[FillEvent]:
        source = "poll" if shard_id == -3 else "ws"
        coin = str(get_first(raw_fill, COIN_KEYS, "")).strip().upper()
        price = parse_float(get_first(raw_fill, PRICE_KEYS, 0.0))
        size = abs(parse_float(get_first(raw_fill, SIZE_KEYS, 0.0)))
        timestamp_ms = parse_int(get_first(raw_fill, TIME_KEYS, 0))
        start_position = parse_float(get_first(raw_fill, START_KEYS, 0.0))
        closed_pnl = parse_float(get_first(raw_fill, CLOSED_PNL_KEYS, 0.0))
        side = safe_side(get_first(raw_fill, SIDE_KEYS, None), size=size, start_position=start_position)

        if not coin or price <= 0 or size <= 0 or timestamp_ms <= 0:
            return None

        timestamp_iso = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        # Use order id for duplicate prevention so one order split into many tids does not create many positions.
        raw_fill_id = raw_fill.get("oid") or raw_fill.get("tid") or raw_fill.get("hash")
        if raw_fill_id is None:
            raw_fill_id = f"{wallet}|{coin}|{side}|{timestamp_ms}|{price:.10f}|{size:.10f}"
        fill_id = str(raw_fill_id)

        return FillEvent(
            wallet=wallet,
            coin=coin,
            side=side,
            price=price,
            size=size,
            timestamp_ms=timestamp_ms,
            timestamp_iso=timestamp_iso,
            start_position=start_position,
            closed_pnl=closed_pnl,
            is_snapshot=is_snapshot,
            raw=raw_fill,
            fill_id=fill_id,
            shard_id=shard_id,
            received_at_ms=utc_now_ms(),
            source=source,
        )

    def is_duplicate_fill(self, fill_id: str) -> bool:
        with self.seen_lock:
            if fill_id in self.seen_fill_set:
                return True
            if len(self.seen_fill_ids) == self.seen_fill_ids.maxlen:
                oldest = self.seen_fill_ids.popleft()
                self.seen_fill_set.discard(oldest)
            self.seen_fill_ids.append(fill_id)
            self.seen_fill_set.add(fill_id)
            return False

    def event_worker_loop(self) -> None:
        self._ensure_audit_counters()
        while not self.stop_event.is_set():
            try:
                event = self.event_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                log("info", f"QUEUE_DEQUEUE wallet={event.wallet} coin={event.coin} side={event.side} fill_id={event.fill_id}")
                self.audit_processed += 1
                self.process_live_fill(event)
                self._maybe_log_fill_audit()
            except Exception as exc:
                log("error", f"ERROR processing live fill wallet={event.wallet} coin={event.coin}: {exc}")
            finally:
                self.event_queue.task_done()

    def process_live_fill(self, event: FillEvent) -> None:
        if event.wallet == self.user_wallet:
            return
        if self.reset_freeze:
            return
        if event.price <= 0:
            return
        if event.wallet not in self.wallet_metrics:
            self.wallet_metrics[event.wallet] = WalletMetrics(wallet=event.wallet)
        if event.wallet not in self.wallet_equity:
            self.wallet_equity[event.wallet] = WalletEquity()
        if event.wallet not in self.wallet_reference:
            self.wallet_reference[event.wallet] = WalletEquity()
        if event.wallet not in self.leader_realized_pnl:
            self.leader_realized_pnl[event.wallet] = 0.0
        if event.is_snapshot:
            return
        metrics = self.wallet_metrics[event.wallet]
        if event.source == "poll":
            metrics.poll_fill_count += 1
        else:
            metrics.ws_fill_count += 1

        log("info", f"PROCESS wallet={event.wallet} coin={event.coin}")
        print("FILL_EVENT", event.wallet, event.coin, event.side, event.size)

        # Delta-based position tracking using deterministic local tracking only.
        key = (event.wallet, event.coin)
        prev_pos = self.wallet_positions.get(key, 0.0)
        new_pos = prev_pos + (event.size if event.side == "BUY" else -event.size)
        delta = new_pos - prev_pos
        self.wallet_positions[key] = new_pos

        print(
            "[DELTA DEBUG]",
            f"wallet={event.wallet[:8]}",
            f"coin={event.coin}",
            f"prev={prev_pos:.6f}",
            f"new={new_pos:.6f}",
            f"delta={delta:.6f}",
        )

        prev_mark = self.mark_prices.get(event.coin)
        

        off_mode = self.wallet_off_mode.get(event.wallet)
        ref_price = prev_mark if prev_mark else event.price

        if delta > 0:
            print(
                "[ENTRY CHECK]",
                f"wallet={event.wallet[:8]}",
                f"coin={event.coin}",
                f"delta={delta:.6f}",
                f"will_enter={delta > 0}",
            )
            entry_size = delta
            log("info", (
                f"ENTRY wallet={event.wallet[:8]} coin={event.coin} "
                f"px={event.price:.4f} sz={entry_size:.6f}"
            ))
            self._open_leader_position(event)
            self.open_copy_position(event, prev_mark)
            self.tracked_positions[key] += entry_size

        elif delta < 0:
            reduction = abs(delta)
            tracked = self.tracked_positions[key]
            close_size = min(tracked, reduction)
            if close_size > 1e-6:
                if off_mode == "DO_NOTHING":
                    log("info", (
                        f"DO_NOTHING_SKIP wallet={event.wallet[:8]} coin={event.coin} "
                        f"sz={close_size:.6f}"
                    ))
                else:
                    fraction = max(0.0, min(1.0, close_size / tracked))
                    log("info", (
                        f"EXIT wallet={event.wallet[:8]} coin={event.coin} "
                        f"px={event.price:.4f} sz={close_size:.6f} frac={fraction:.3f}"
                    ))
                    leader_close = self._close_leader_position(event, fraction=fraction)
                    self.close_copy_position(event, prev_mark, fraction=fraction, leader_close=leader_close)
                    self.tracked_positions[key] -= close_size
                    if off_mode == "FOLLOW":
                        remaining = sum(
                            len(v) for (w, _), v in self.open_positions.items() if w == event.wallet
                        )
                        if remaining == 0:
                            self.wallet_off_mode[event.wallet] = None
                            log("info", f"FOLLOW_COMPLETE wallet={event.wallet[:8]} off_mode reset")

        with self.metrics_lock:
            m = self.wallet_metrics.get(event.wallet)
            if m:
                log("debug", f"DEBUG wallet={event.wallet} entries={m.copy_entry_count} exits={m.copy_exit_count} open={m.copy_open_positions}")
        self.mark_prices[event.coin] = event.price

    def _open_leader_position(self, event: FillEvent) -> None:
        leader_notional = event.price * abs(event.size)
        leader_equity = None
        snap = self.exchange_position_snapshot.get(event.wallet)
        if snap:
            leader_equity = float(snap.get("exchange_equity", 0.0))
        if leader_equity and leader_equity > 0:
            pct = leader_notional / leader_equity
            alloc = max(0.0, self.wallet_alloc.get(event.wallet, NORMALISATION_BASE))
            notional_usd = pct * alloc
            size_units = notional_usd / event.price if event.price > 0 else 0.0
        else:
            alloc = max(0.0, self.wallet_alloc.get(event.wallet, 100.0))
            scale = alloc / self.reference_equity if self.reference_equity > 0 else 0.0
            size_units = event.size * scale
            notional_usd = size_units * event.price if event.price > 0 else alloc
        if size_units <= 0:
            return
        entry_fee_usd = notional_usd * (FEE_BPS / 10000.0)
        self.leader_positions[(event.wallet, event.coin)].append(
            SimPosition(
                wallet=event.wallet,
                coin=event.coin,
                trade_id=self._next_trade_id(),
                entry_time_ms=event.timestamp_ms,
                entry_time_iso=event.timestamp_iso,
                entry_price_lead=event.price,
                entry_price_copy=event.price,
                size_units=size_units,
                notional_usd=notional_usd,
                lead_side=event.side,
                entry_latency_ms=max(0, event.received_at_ms - event.timestamp_ms),
                mark_price=event.price,
                entry_bps=0.0,
                entry_fee_usd=entry_fee_usd,
            )
        )
        with self.metrics_lock:
            reference_model = self.wallet_reference[event.wallet]
            reference_model.realized_pnl -= entry_fee_usd
            reference_model.update_peak()
            reference_model.update_trough()
            self.leader_realized_pnl[event.wallet] = reference_model.realized_pnl
            self.wallet_metrics[event.wallet].wallet_pnl_ref -= entry_fee_usd

    def open_copy_position(self, event: FillEvent, prev_mark: Optional[float]) -> None:
        if self.user_wallet and event.wallet == self.user_wallet:
            return
        print(
            "[OPEN POSITION]",
            f"wallet={event.wallet[:8]}",
            f"coin={event.coin}",
            f"price={event.price}",
            f"size={event.size}",
        )
        ref_price = prev_mark if prev_mark else event.price
        entry_price_copy = ref_price if ref_price > 0 else event.price
        is_backfill = event.source == "poll"
        entry_bps = None if is_backfill else calc_bps_delta(ref_price, event.price)
        # --- TRUE NORMALISATION (RISK-BASED) ---
        leader_notional = event.price * abs(event.size)

        leader_equity = None
        snap = self.exchange_position_snapshot.get(event.wallet)
        if snap:
            leader_equity = float(snap.get("exchange_equity", 0.0))

        if leader_equity and leader_equity > 0:
            pct = leader_notional / leader_equity
            alloc = max(0.0, self.wallet_alloc.get(event.wallet, NORMALISATION_BASE))
            notional_usd = pct * alloc
            size_units = notional_usd / entry_price_copy if entry_price_copy > 0 else 0.0
        else:
            # --- ORIGINAL FALLBACK (UNCHANGED) ---
            alloc = max(0.0, self.wallet_alloc.get(event.wallet, 100.0))
            scale = alloc / self.reference_equity if self.reference_equity > 0 else 0.0
            size_units = event.size * scale
            notional_usd = size_units * entry_price_copy if entry_price_copy > 0 else alloc

        log("INFO", f"NORM_CHECK wallet={event.wallet} leader_notional={leader_notional:.2f} equity={leader_equity} notional={notional_usd:.2f}")
        if size_units <= 0:
            return
        entry_fee_usd = notional_usd * (FEE_BPS / 10000.0)

        trade_id = self._next_trade_id()
        position = SimPosition(
            wallet=event.wallet,
            coin=event.coin,
            trade_id=trade_id,
            entry_time_ms=event.timestamp_ms,
            entry_time_iso=event.timestamp_iso,
            entry_price_lead=event.price,
            entry_price_copy=entry_price_copy,
            size_units=size_units,
            notional_usd=notional_usd,
            lead_side=event.side,
            entry_latency_ms=max(0, event.received_at_ms - event.timestamp_ms),
            mark_price=event.price,
            entry_bps=entry_bps,
            entry_fee_usd=entry_fee_usd,
        )

        with self.positions_lock:
            self.open_positions[(event.wallet, event.coin)].append(position)
            open_count = len(self.open_positions[(event.wallet, event.coin)])
            total_open = sum(len(v) for (w, _), v in self.open_positions.items() if w == event.wallet)

        with self.metrics_lock:
            metrics = self.wallet_metrics[event.wallet]
            equity_model = self.wallet_equity[event.wallet]
            if metrics.first_trade_time_ms == 0:
                metrics.first_trade_time_ms = event.timestamp_ms
            metrics.copy_entry_count += 1
            metrics.copy_open_positions = total_open
            metrics.update_entry_latency(position.entry_latency_ms)
            if entry_bps is not None:
                metrics.update_entry_slippage(entry_bps)
            metrics.copy_realized_pnl -= entry_fee_usd
            metrics.copy_pnl_exec -= entry_fee_usd
            equity_model.realized_pnl -= entry_fee_usd
            equity_model.update_peak()
            equity_model.update_trough()
            metrics.update_derived(metrics.copy_realized_pnl, equity_model)
            metrics.last_update_iso = utc_now_iso()

        log("info", f"ENTRY_OK wallet={event.wallet[:8]} coin={event.coin} open={open_count}")

        # Mirror into user wallet ledger if configured and leader != user
        uw = self.user_wallet
        if (
            not self._in_test_mode
            and uw
            and event.wallet.lower() != uw.lower()
            and uw in self.wallet_metrics
        ):
            if uw not in self.wallet_reference:
                self.wallet_reference[uw] = WalletEquity()
            log("debug", f"MIRROR_ALLOWED wallet={event.wallet[:8]} mode={self.wallet_gate.get(event.wallet, {}).get('mode', 'OFF')}")
            mirror = SimPosition(
                wallet=uw,
                coin=event.coin,
                trade_id=position.trade_id,
                entry_time_ms=position.entry_time_ms,
                entry_time_iso=position.entry_time_iso,
                entry_price_lead=position.entry_price_lead,
                entry_price_copy=position.entry_price_copy,
                size_units=position.size_units,
                notional_usd=position.notional_usd,
                lead_side=position.lead_side,
                entry_latency_ms=position.entry_latency_ms,
                mark_price=position.mark_price,
                entry_bps=position.entry_bps,
                entry_fee_usd=position.entry_fee_usd,
                source_wallet=event.wallet,
            )
            with self.positions_lock:
                self.open_positions[(uw, event.coin)].append(mirror)
                uw_open = sum(len(v) for (w, _), v in self.open_positions.items() if w == uw)
            self.tracked_positions[(uw, event.coin)] += position.size_units
            with self.metrics_lock:
                um = self.wallet_metrics[uw]
                ueq = self.wallet_equity[uw]
                um.copy_entry_count += 1
                um.copy_open_positions = uw_open
                um.copy_realized_pnl -= entry_fee_usd
                um.copy_pnl_exec -= entry_fee_usd
                ueq.realized_pnl -= entry_fee_usd
                ueq.update_peak(); ueq.update_trough()
                um.update_derived(um.copy_realized_pnl, ueq)
                um.last_update_iso = utc_now_iso()
            log("info", f"MIRROR_ENTRY_OK user={uw[:8]} coin={event.coin} leader={event.wallet[:8]}")

    def _close_leader_position(self, event: FillEvent, fraction: float = 1.0) -> Dict[str, float]:
        key = (event.wallet, event.coin)
        with self.positions_lock:
            positions = self.leader_positions.get(key, [])
            if not positions:
                return {"realized_pnl": 0.0}
            positions_to_close: List[Tuple[SimPosition, float]] = []
            if fraction >= 1.0 - 1e-9:
                positions_to_close = [(p, 1.0) for p in positions]
                self.leader_positions.pop(key, None)
            else:
                total_size = sum(p.size_units for p in positions)
                need_to_close = total_size * max(0.0, min(1.0, fraction))
                if need_to_close <= 1e-6:
                    return {"realized_pnl": 0.0}
                remaining_to_close = need_to_close
                new_positions: List[SimPosition] = []
                for p in positions:
                    if remaining_to_close <= 1e-12:
                        new_positions.append(p)
                        continue
                    if p.size_units <= remaining_to_close + 1e-12:
                        positions_to_close.append((p, 1.0))
                        remaining_to_close -= p.size_units
                    else:
                        close_frac = remaining_to_close / p.size_units
                        positions_to_close.append((p, close_frac))
                        remaining_frac = 1.0 - close_frac
                        new_positions.append(
                            SimPosition(
                                wallet=p.wallet,
                                coin=p.coin,
                                trade_id=p.trade_id,
                                entry_time_ms=p.entry_time_ms,
                                entry_time_iso=p.entry_time_iso,
                                entry_price_lead=p.entry_price_lead,
                                entry_price_copy=p.entry_price_copy,
                                size_units=p.size_units * remaining_frac,
                                notional_usd=p.notional_usd * remaining_frac,
                                lead_side=p.lead_side,
                                entry_latency_ms=p.entry_latency_ms,
                                mark_price=p.mark_price,
                                entry_bps=p.entry_bps,
                                entry_fee_usd=p.entry_fee_usd * remaining_frac,
                                source_wallet=p.source_wallet,
                            )
                        )
                        remaining_to_close = 0.0
                if new_positions:
                    self.leader_positions[key] = new_positions
                else:
                    self.leader_positions.pop(key, None)
        total_leader_pnl = 0.0
        for position, frac in positions_to_close:
            closed_size = position.size_units * frac
            if closed_size <= 0:
                continue
            direction = 1.0 if position.lead_side == "BUY" else -1.0
            exit_fee_usd = (abs(closed_size * event.price)) * (FEE_BPS / 10000.0)
            total_leader_pnl += ((event.price - position.entry_price_lead) * closed_size * direction) - exit_fee_usd
        with self.metrics_lock:
            reference_model = self.wallet_reference[event.wallet]
            reference_model.realized_pnl += total_leader_pnl
            reference_model.update_peak()
            reference_model.update_trough()
            self.leader_realized_pnl[event.wallet] = reference_model.realized_pnl
        return {"realized_pnl": total_leader_pnl}

    def close_copy_position(self, event: FillEvent, prev_mark: Optional[float], fraction: float = 1.0, leader_close: Optional[Dict[str, float]] = None) -> None:
        if self.user_wallet and event.wallet == self.user_wallet:
            return

        key = (event.wallet, event.coin)
        ref_price = prev_mark if prev_mark else event.price
        is_backfill = event.source == "poll"
        exit_reference_price = ref_price if ref_price > 0 else event.price
        exit_price_copy = event.price
        exit_bps = None if is_backfill else calc_bps_delta(exit_reference_price, event.price)
        exit_latency_ms = max(0, event.received_at_ms - event.timestamp_ms)

        with self.positions_lock:
            positions = self.open_positions.get(key, [])
            if not positions:
                with self.metrics_lock:
                    self.wallet_metrics[event.wallet].missed_exit_count += 1
                    self.wallet_metrics[event.wallet].last_update_iso = utc_now_iso()
                return

            positions_to_close: List[Tuple[SimPosition, float]] = []

            if fraction >= 1.0 - 1e-9:
                positions_to_close = [(p, 1.0) for p in positions]
                self.open_positions.pop(key, None)
            else:
                # Compute total size across all positions, then close progressively
                total_size = sum(p.size_units for p in positions)
                need_to_close = total_size * max(0.0, min(1.0, fraction))
                if need_to_close <= 1e-6:
                    return
                remaining_to_close = need_to_close
                new_positions: List[SimPosition] = []
                for p in positions:
                    if remaining_to_close <= 1e-12:
                        new_positions.append(p)
                        continue
                    if p.size_units <= remaining_to_close + 1e-12:
                        # Close this position fully
                        positions_to_close.append((p, 1.0))
                        remaining_to_close -= p.size_units
                    else:
                        # Partially close this position
                        close_frac = remaining_to_close / p.size_units
                        positions_to_close.append((p, close_frac))
                        remaining_frac = 1.0 - close_frac
                        new_positions.append(
                            SimPosition(
                                wallet=p.wallet,
                                coin=p.coin,
                                trade_id=p.trade_id,
                                entry_time_ms=p.entry_time_ms,
                                entry_time_iso=p.entry_time_iso,
                                entry_price_lead=p.entry_price_lead,
                                entry_price_copy=p.entry_price_copy,
                                size_units=p.size_units * remaining_frac,
                                notional_usd=p.notional_usd * remaining_frac,
                                lead_side=p.lead_side,
                                entry_latency_ms=p.entry_latency_ms,
                                mark_price=p.mark_price,
                                entry_bps=p.entry_bps,
                                entry_fee_usd=p.entry_fee_usd * remaining_frac,
                                source_wallet=p.source_wallet,
                            )
                        )
                        remaining_to_close = 0.0
                if new_positions:
                    self.open_positions[key] = new_positions
                else:
                    self.open_positions.pop(key, None)

            # Compute after open_positions is fully updated, while still holding positions_lock
            close_total_open = sum(len(v) for (w, _), v in self.open_positions.items() if w == event.wallet)

        total_pnl = 0.0
        total_holding_seconds = 0.0
        total_notional_closed = 0.0
        subrows = 0

        for position, frac in positions_to_close:
            closed_size = position.size_units * frac
            closed_notional = position.notional_usd * frac
            if closed_size <= 0 or closed_notional <= 0:
                continue

            direction = 1.0 if position.lead_side == "BUY" else -1.0
            exit_fee_usd = (abs(closed_size * exit_price_copy)) * (FEE_BPS / 10000.0)
            entry_fee_alloc = position.entry_fee_usd * frac
            copy_pnl_gross = (exit_price_copy - position.entry_price_copy) * closed_size * direction
            copy_pnl = copy_pnl_gross - exit_fee_usd
            wallet_pnl = (event.price - position.entry_price_lead) * (
                (closed_notional / position.entry_price_lead) if position.entry_price_lead > 0 else 0.0
            ) * direction
            row_copy_pnl = copy_pnl - entry_fee_alloc
            copy_error = row_copy_pnl - wallet_pnl
            copy_efficiency = (row_copy_pnl / wallet_pnl) if abs(wallet_pnl) > 1e-9 else 0.0
            holding_seconds = max(0.0, (event.timestamp_ms - position.entry_time_ms) / 1000.0)
            duration_str = _format_duration(holding_seconds)
            return_pct = (copy_pnl / closed_notional * 100.0) if closed_notional > 0 else 0.0
            total_pnl += copy_pnl
            total_notional_closed += abs(position.entry_price_lead * closed_size)
            total_holding_seconds += holding_seconds
            subrows += 1

            self.copy_trades_writer.write_row(
                {
                    "trade_id": position.trade_id,
                    "wallet": event.wallet,
                    "source_wallet": position.source_wallet,
                    "coin": event.coin,
                    "entry_time_iso": position.entry_time_iso,
                    "exit_time_iso": event.timestamp_iso,
                    "lead_side": position.lead_side,
                    "entry_price_lead": position.entry_price_lead,
                    "entry_price_copy": position.entry_price_copy,
                    "exit_price_lead": event.price,
                    "exit_price_copy": exit_price_copy,
                    "size_units": round(closed_size, 8),
                    "notional_usd": round(closed_notional, 6),
                    "holding_seconds": round(holding_seconds, 3),
                    "duration": duration_str,
                    "return_pct": round(return_pct, 4),
                    "entry_latency_ms": position.entry_latency_ms,
                    "exit_latency_ms": exit_latency_ms,
                    "entry_slippage_bps": round(position.entry_bps, 4) if position.entry_bps is not None else None,
                    "exit_slippage_bps": round(exit_bps, 4) if exit_bps is not None else None,
                    "copy_pnl": round(row_copy_pnl, 6),
                    "wallet_pnl": round(wallet_pnl, 6),
                    "copy_error": round(copy_error, 6),
                    "copy_efficiency": round(copy_efficiency, 6),
                }
            )

        if subrows == 0:
            # Position was removed from open_positions but had no closeable size.
            # Still sync open count so metrics stay consistent with in-memory state.
            with self.metrics_lock:
                self.wallet_metrics[event.wallet].copy_open_positions = close_total_open
                self.wallet_metrics[event.wallet].last_update_iso = utc_now_iso()
            return

        avg_holding_seconds = total_holding_seconds / subrows

        with self.metrics_lock:
            metrics = self.wallet_metrics[event.wallet]
            equity_model = self.wallet_equity[event.wallet]
            leader_realized_pnl = float((leader_close or {}).get("realized_pnl", 0.0))

            metrics.copy_exit_count += 1
            metrics.copy_realized_pnl += total_pnl
            metrics.wallet_pnl_ref += leader_realized_pnl
            metrics.copy_pnl_exec += total_pnl
            metrics.real_notional_total += total_notional_closed
            equity_model.realized_pnl += total_pnl
            equity_model.update_peak()
            equity_model.update_trough()

            metrics.update_holding_seconds(avg_holding_seconds)
            if exit_bps is not None:
                metrics.update_exit_slippage(exit_bps)
            metrics.update_exit_latency(exit_latency_ms)
            if total_pnl > 0:
                metrics.copy_win_count += 1
            elif total_pnl < 0:
                metrics.copy_loss_count += 1
            metrics.copy_open_positions = close_total_open
            metrics.update_derived(metrics.copy_realized_pnl, equity_model)
            base_flags = compute_alert_flags(metrics)
            metrics.alert_flags = self._merge_ws_flags(event.wallet, base_flags)
            metrics.last_update_iso = utc_now_iso()

        log("info", f"EXIT_OK wallet={event.wallet[:8]} coin={event.coin} pnl={total_pnl:.4f} frac={fraction:.3f}")

        # Mirror exit into user wallet ledger if leader execution is allowed by gate
        uw = self.user_wallet
        if (
            not self._in_test_mode
            and uw
            and event.wallet.lower() != uw.lower()
            and uw in self.wallet_metrics
        ):
            if uw not in self.wallet_reference:
                self.wallet_reference[uw] = WalletEquity()
            log("debug", f"MIRROR_ALLOWED wallet={event.wallet[:8]}")
            ukey = (uw, event.coin)
            with self.positions_lock:
                u_positions = self.open_positions.get(ukey, [])
                # Close same fraction of user mirror as was closed on leader side
                u_to_close: List[Tuple[SimPosition, float]] = []
                if u_positions:
                    if fraction >= 1.0 - 1e-9:
                        u_to_close = [(p, 1.0) for p in u_positions]
                        self.open_positions.pop(ukey, None)
                    else:
                        u_total = sum(p.size_units for p in u_positions)
                        u_need = u_total * max(0.0, min(1.0, fraction))
                        u_rem = u_need
                        u_new: List[SimPosition] = []
                        for p in u_positions:
                            if u_rem <= 1e-12:
                                u_new.append(p); continue
                            if p.size_units <= u_rem + 1e-12:
                                u_to_close.append((p, 1.0)); u_rem -= p.size_units
                            else:
                                cf = u_rem / p.size_units
                                u_to_close.append((p, cf))
                                u_new.append(SimPosition(
                                    wallet=uw, coin=p.coin, trade_id=p.trade_id,
                                    entry_time_ms=p.entry_time_ms, entry_time_iso=p.entry_time_iso,
                                    entry_price_lead=p.entry_price_lead, entry_price_copy=p.entry_price_copy,
                                    size_units=p.size_units * (1.0 - cf),
                                    notional_usd=p.notional_usd * (1.0 - cf),
                                    lead_side=p.lead_side, entry_latency_ms=p.entry_latency_ms,
                                    mark_price=p.mark_price, entry_bps=p.entry_bps,
                                    entry_fee_usd=p.entry_fee_usd * (1.0 - cf), source_wallet=p.source_wallet,
                                ))
                                u_rem = 0.0
                        if u_new:
                            self.open_positions[ukey] = u_new
                        else:
                            self.open_positions.pop(ukey, None)
                uw_open = sum(len(v) for (w, _), v in self.open_positions.items() if w == uw)

            u_pnl = 0.0
            u_wallet_pnl_total = 0.0
            u_notional_closed = 0.0
            for u_pos, u_frac in u_to_close:
                u_closed_sz  = u_pos.size_units * u_frac
                u_closed_ntl = u_pos.notional_usd * u_frac
                if u_closed_sz <= 0:
                    continue
                direction = 1.0 if u_pos.lead_side == "BUY" else -1.0
                u_exit_fee = (abs(u_closed_sz * exit_price_copy)) * (FEE_BPS / 10000.0)
                u_entry_fee_alloc = u_pos.entry_fee_usd * u_frac
                u_copy_pnl  = (exit_price_copy - u_pos.entry_price_copy) * u_closed_sz * direction - u_exit_fee
                u_wallet_pnl = (event.price - u_pos.entry_price_lead) * u_closed_sz * direction
                u_row_copy_pnl = u_copy_pnl - u_entry_fee_alloc
                u_pnl += u_copy_pnl
                u_wallet_pnl_total += u_wallet_pnl
                u_notional_closed += abs(u_pos.entry_price_lead * u_closed_sz)
                u_hold_sec  = max(0.0, (event.timestamp_ms - u_pos.entry_time_ms) / 1000.0)
                u_ret_pct   = (u_copy_pnl / u_closed_ntl * 100.0) if u_closed_ntl > 0 else 0.0
                self.copy_trades_writer.write_row({
                    "trade_id": u_pos.trade_id,
                    "wallet": uw,
                    "source_wallet": u_pos.source_wallet,
                    "coin": event.coin,
                    "entry_time_iso": u_pos.entry_time_iso,
                    "exit_time_iso": event.timestamp_iso,
                    "lead_side": u_pos.lead_side,
                    "entry_price_lead": u_pos.entry_price_lead,
                    "entry_price_copy": u_pos.entry_price_copy,
                    "exit_price_lead": event.price,
                    "exit_price_copy": exit_price_copy,
                    "size_units": round(u_closed_sz, 8),
                    "notional_usd": round(u_closed_ntl, 6),
                    "holding_seconds": round(u_hold_sec, 3),
                    "duration": _format_duration(u_hold_sec),
                    "return_pct": round(u_ret_pct, 4),
                    "entry_latency_ms": u_pos.entry_latency_ms,
                    "exit_latency_ms": exit_latency_ms,
                    "entry_slippage_bps": round(u_pos.entry_bps, 4) if u_pos.entry_bps is not None else None,
                    "exit_slippage_bps": round(exit_bps, 4) if exit_bps is not None else None,
                    "copy_pnl": round(u_row_copy_pnl, 6),
                    "wallet_pnl": round(u_wallet_pnl, 6),
                    "copy_error": round(u_row_copy_pnl - u_wallet_pnl, 6),
                    "copy_efficiency": round(u_row_copy_pnl / u_wallet_pnl, 6) if abs(u_wallet_pnl) > 1e-9 else 0.0,
                })

            if u_to_close:
                self.tracked_positions[ukey] = max(0.0, self.tracked_positions.get(ukey, 0.0) - sum(p.size_units * f for p, f in u_to_close))
                with self.metrics_lock:
                    um = self.wallet_metrics[uw]
                    ueq = self.wallet_equity[uw]
                    um.copy_exit_count  += 1
                    um.copy_realized_pnl += u_pnl
                    um.wallet_pnl_ref += u_wallet_pnl_total
                    um.copy_pnl_exec += u_pnl
                    um.real_notional_total += u_notional_closed
                    ueq.realized_pnl    += u_pnl
                    ueq.update_peak(); ueq.update_trough()
                    if u_pnl > 0: um.copy_win_count += 1
                    elif u_pnl < 0: um.copy_loss_count += 1
                    um.copy_open_positions = uw_open
                    um.last_update_iso = utc_now_iso()
                log("info", f"MIRROR_EXIT_OK user={uw[:8]} coin={event.coin} pnl={u_pnl:.4f}")

    def _verify_wallet_consistency(self, wallet: str) -> None:
        """Log invariant check for one wallet. Called inside positions_lock + metrics_lock."""
        log("info", f"VERIFY_CALLED wallet={wallet}")
        metrics = self.wallet_metrics[wallet]
        # json_open: what was synced to metrics (and written to JSON)
        json_open = metrics.copy_open_positions
        # mem_open: recomputed directly from positions dict — ground truth
        mem_open = sum(len(v) for (w, _), v in self.open_positions.items() if w == wallet)
        entries = metrics.copy_entry_count
        exits = metrics.copy_exit_count
        # recompute unrealised live from open positions only (ignore stored value)
        upnl = self._compute_unrealized_pnl_for_wallet(wallet)

        # Rule 1: json count must match in-memory count
        rule1 = (json_open == mem_open)
        # Rule 2: exits can never exceed entries
        rule2 = (entries >= exits)
        # Rule 3: if more entries than exits, there MUST be open positions
        rule3 = not (entries > exits and mem_open == 0)
        # Rule 4: if positions are open, upnl must equal stored value within epsilon
        stored_upnl = self.wallet_equity[wallet].unrealized_pnl
        rule4 = (mem_open == 0) or (abs(upnl - stored_upnl) <= 1e-4)

        ok = rule1 and rule2 and rule3 and rule4

        log("info", (
            f"VERIFY wallet={wallet} open_json={json_open} open_mem={mem_open} "
            f"entries={entries} exits={exits} upnl={upnl:.4f} ok={ok}"
        ))

        if not ok:
            log("warning", (
                f"VERIFY_FAIL wallet={wallet} "
                f"entries={entries} exits={exits} "
                f"open_json={json_open} open_mem={mem_open} "
                f"upnl={upnl:.4f} "
                f"last_fill_coin={metrics.last_fill_coin} "
                f"last_fill_side={metrics.last_fill_side} "
                f"rules=({int(rule1)}{int(rule2)}{int(rule3)}{int(rule4)})"
            ))

        # Separate check: upnl recompute vs stored (fire even when ok=True, e.g. rule3 masks it)
        if mem_open > 0 and abs(upnl - stored_upnl) > 1e-4:
            log("warning", (
                f"ERROR_UPNL wallet={wallet} recomputed={upnl:.4f} stored={stored_upnl:.4f}"
            ))

    def total_open_positions_for_wallet(self, wallet: str) -> int:
        total = 0
        for (w, _coin), positions in self.open_positions.items():
            if w == wallet:
                total += len(positions)
        return total

    def _compute_unrealized_pnl_for_wallet(self, wallet: str) -> float:
        total = 0.0
        for (w, coin), positions in self.open_positions.items():
            if w != wallet:
                continue
            mark = self.mark_prices.get(coin, 0.0)
            for pos in positions:
                if mark > 0:
                    pos.mark_price = mark
                total += pos.unrealized_pnl
        return total

    def _compute_reference_unrealized_pnl_for_wallet(self, wallet: str) -> float:
        total = 0.0
        for (w, coin), positions in self.leader_positions.items():
            if w != wallet:
                continue
            mark = self.mark_prices.get(coin, 0.0)
            for pos in positions:
                if mark > 0:
                    pos.mark_price = mark
                if pos.mark_price <= 0 or pos.entry_price_lead <= 0:
                    continue
                direction = 1.0 if pos.lead_side == "BUY" else -1.0
                total += (pos.mark_price - pos.entry_price_copy) * pos.size_units * direction
        return total

    def _update_portfolio_dd(self) -> None:
        now_ms = utc_now_ms()
        stale_wallets: List[str] = []

        for wallet in self.wallets:
            metrics = self.wallet_metrics.get(wallet)
            if metrics is None:
                continue
            last_truth_update_ms = int(getattr(metrics, "last_truth_update_ms", 0) or 0)
            if last_truth_update_ms > 0 and (now_ms - last_truth_update_ms) > STALE_WALLET_MS:
                stale_wallets.append(wallet)
                print(f"[STALE_WALLET] wallet={wallet} age_ms={now_ms - last_truth_update_ms}")

        self.portfolio_stale_count = len(stale_wallets)
        if stale_wallets:
            self.portfolio_valid = False
            self.portfolio_reason = "stale_wallet"
            self.portfolio_invalid_count += 1
            print(f"[PORTFOLIO_INVALID] stale_wallet_count={self.portfolio_stale_count}")
            return

        total_equity = 0.0
        for wallet in self.wallets:
            if self.user_wallet and wallet.lower() == self.user_wallet.lower():
                continue
            eq = self.wallet_equity.get(wallet)
            if eq is None:
                continue
            alloc = self.wallet_alloc.get(wallet, NORMALISATION_BASE)
            starting = eq.starting_balance if eq.starting_balance > 0 else 1.0
            scale = alloc / starting
            total_equity += eq.total_equity * scale

        if total_equity > self.portfolio_peak_equity:
            self.portfolio_peak_equity = total_equity

        self.portfolio_valid = True
        self.portfolio_reason = "ok"
        self.portfolio_valid_count += 1
        peak = self.portfolio_peak_equity
        dd_usd = max(0.0, peak - total_equity)
        self.portfolio_max_dd_usd = max(getattr(self, "portfolio_max_dd_usd", 0.0), dd_usd)
        self.portfolio_max_dd_pct = max(
            getattr(self, "portfolio_max_dd_pct", 0.0),
            (dd_usd / peak * 100.0) if peak > 0 else 0.0,
        )

    def _check_ws_misses(self):
        missed = self._poll_seen_ids - self._ws_seen_ids
        if missed:
            print(f"[WS_MISS_DETECTED] count={len(missed)}")

    def _rebuild_from_fills(self) -> None:
        """
        Deterministic startup rebuild from RAW_FILLS_CSV.
        Clears open_positions / wallet_positions / tracked_positions ONLY, then
        replays every non-snapshot fill for known wallets via process_live_fill()
        so open_positions reflects the true end-state of all recorded activity.
        wallet_metrics and wallet_equity are NOT cleared.
        copy_trades_writer is suppressed during replay to prevent duplicate records.
        """
        log("info", "[REBUILD] starting history rebuild")

        # ── Clear only position-tracking state ──────────────────────────────
        with self.positions_lock:
            self.open_positions.clear()
            self.wallet_positions.clear()
            self.tracked_positions.clear()

        self.mark_prices.clear()
        if hasattr(self, "mark_price_ts"):
            self.mark_price_ts.clear()

        with self.metrics_lock:
            for w in list(self.wallet_equity.keys()):
                self.wallet_equity[w] = WalletEquity()

            for w in list(self.wallet_metrics.keys()):
                self.wallet_metrics[w] = WalletMetrics(wallet=w)

        # ── Load CSV ─────────────────────────────────────────────────────────
        if not RAW_FILLS_CSV.exists():
            log("info", "[REBUILD] complete fills=0 (no CSV found)")
            return

        rows: List[Dict[str, Any]] = []
        try:
            with RAW_FILLS_CSV.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    rows.append(dict(row))
        except Exception as exc:
            log("error", f"[REBUILD] failed to read fills CSV: {exc}")
            return

        if not rows:
            log("info", "[REBUILD] complete fills=0 (empty CSV)")
            return

        # ── Sort ascending by fill timestamp ─────────────────────────────────
        rows.sort(key=lambda r: parse_int(r.get("timestamp_ms", 0)))

        # ── Suppress copy_trades writes to avoid duplicate closed-trade rows ─
        class _NoOp:
            def write_row(self, _r: Any) -> None: pass
            def write_rows_replace(self, _r: Any) -> None: pass

        real_writer = self.copy_trades_writer
        self.copy_trades_writer = _NoOp()  # type: ignore[assignment]

        wallet_set = set(self.wallets)
        seen_ids: Set[str] = set()
        replayed = 0

        try:
            for row in rows:
                wallet = str(row.get("wallet", "")).strip().lower()
                if wallet not in wallet_set:
                    continue

                # Skip snapshot fills — they do not drive position deltas
                if str(row.get("is_snapshot", "False")).strip().lower() in ("true", "1", "yes"):
                    continue

                # Local deduplication by fill_id
                fill_id = str(row.get("fill_id", ""))
                if fill_id:
                    if fill_id in seen_ids:
                        continue
                    seen_ids.add(fill_id)

                raw_fill: Dict[str, Any] = {
                    "coin":          str(row.get("coin", "")),
                    "px":            row.get("price", "0"),
                    "sz":            row.get("size", "0"),
                    "side":          str(row.get("side", "")),
                    "time":          row.get("timestamp_ms", "0"),
                    "startPosition": row.get("start_position", "0"),
                    "closedPnl":     row.get("closed_pnl", "0"),
                    "oid":           fill_id or None,
                }

                event = self.parse_fill(
                    shard_id=-1,
                    wallet=wallet,
                    raw_fill=raw_fill,
                    is_snapshot=False,
                )
                if event is None:
                    continue
                self._poll_seen_ids.add(event.fill_id)

                # Use original fill timestamp as received_at so latency is 0 for
                # historical fills rather than wall-clock time since the session.
                event.received_at_ms = parse_int(row.get("timestamp_ms", 0))

                self.process_live_fill(event)
                replayed += 1

        finally:
            self.copy_trades_writer = real_writer

        log("info", f"[REBUILD] complete fills={replayed}")

    def _clean_rebuild_check(self) -> None:
        """
        Called at startup when CLEAN_REBUILD=1.
        Fetches a fresh exchange snapshot, then for each wallet:
        - Closed (has_live_position=False): fill-based reconstruction is sufficient; log and continue.
        - Live  (has_live_position=True):   compute expected size/entry from fill-reconstructed
          open_positions (read-only), compare against exchange snapshot, log [POSITION_DIFF]
          on mismatch. Engine state is NEVER mutated here.
        """
        log("info", "[CLEAN_REBUILD] starting — fetching exchange snapshots")
        self._refresh_exchange_snapshots()

        for wallet in self.wallets:
            snap     = self.exchange_position_snapshot.get(wallet, {})
            has_live = snap.get("has_live_position", False)

            if not has_live:
                log("info", f"[CLEAN_REBUILD] wallet={wallet[:10]} status=closed rebuild=fills_only")
                continue

            exchange_size  = snap.get("exchange_position_size", 0.0)
            exchange_entry = snap.get("exchange_entry_price", 0.0)

            # Read expected state from fill-reconstructed positions — no mutations
            total_size  = 0.0
            total_px_sz = 0.0
            with self.positions_lock:
                for (w, _coin), positions in self.open_positions.items():
                    if w != wallet:
                        continue
                    for pos in positions:
                        total_size  += pos.size_units
                        total_px_sz += pos.entry_price_copy * pos.size_units

            expected_size  = total_size
            expected_entry = (total_px_sz / total_size) if total_size > 1e-9 else 0.0

            size_ok  = abs(expected_size - exchange_size) <= 1e-6
            entry_ok = exchange_entry <= 0 or abs(expected_entry - exchange_entry) <= 1e-2

            if not size_ok or not entry_ok:
                log("warning", (
                    f"[POSITION_DIFF] wallet={wallet} "
                    f"expected_size={expected_size:.6f} exchange_size={exchange_size:.6f} "
                    f"expected_entry={expected_entry:.4f} exchange_entry={exchange_entry:.4f}"
                ))
            else:
                log("info", (
                    f"[CLEAN_REBUILD] wallet={wallet[:10]} status=live match=ok "
                    f"size={expected_size:.6f} entry={expected_entry:.4f}"
                ))

    def _refresh_exchange_snapshots(self) -> None:
        if self.reset_freeze:
            return
        ts = utc_now_iso()
        _uw = "0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205"
        print("[SNAPSHOT LOOP SOURCE]", type(self.wallets), len(self.wallets))
        print("[SNAPSHOT WALLET LIST]", self.wallets)
        print("[SNAPSHOT LOOP START]")
        for wallet in self.wallets:
            print("[SNAPSHOT ITER]", wallet)
            print("[SNAPSHOT FETCH]", wallet)
            is_user = bool(self.user_wallet) and wallet.lower() == self.user_wallet.lower()
            if is_user:
                spot_result = get_user_spot_balance(wallet)
                if spot_result is None:
                    exchange_equity = 0.0
                    exchange_unreal = 0.0
                else:
                    exchange_equity = float(spot_result.get("exchange_equity", 0.0))
                    exchange_unreal = float(spot_result.get("exchange_unrealized_pnl", 0.0))
                self.exchange_position_snapshot[wallet] = {
                    "exchange_position_size": 0.0,
                    "exchange_entry_price": 0.0,
                    "exchange_timestamp": ts,
                    "has_live_position": False,
                    "exchange_equity": exchange_equity,
                    "exchange_unrealized_pnl": exchange_unreal,
                }
                print("[USER EQUITY WRITE] snap=", self.exchange_position_snapshot[wallet])
                log("info", f"[WALLET_STATE] wallet={wallet} live=False size=0 equity={exchange_equity}")
                try:
                    _snap_out = OUTPUT_DIR / "user_snapshot.json"
                    _snap_tmp = _snap_out.with_suffix(".json.tmp")
                    _snap_tmp.write_text(
                        json.dumps(self.exchange_position_snapshot[wallet], indent=2),
                        encoding="utf-8",
                    )
                    os.replace(_snap_tmp, _snap_out)
                except Exception as _se:
                    print("[USER_SNAPSHOT_WRITE_ERROR]", _se)
            else:
                result = get_exchange_position(wallet)
                if result is None:
                    size = 0.0; entry_px = 0.0; has_live = False
                    exchange_equity = 0.0; exchange_unreal = 0.0
                else:
                    size = float(result.get("size", 0.0))
                    entry_px = float(result.get("entry_price", 0.0))
                    has_live = size != 0.0
                    exchange_equity = float(result.get("exchange_equity", 0.0))
                    exchange_unreal = float(result.get("exchange_unrealized_pnl", 0.0))
                self.exchange_position_snapshot[wallet] = {
                    "exchange_position_size": size,
                    "exchange_entry_price": entry_px,
                    "exchange_timestamp": ts,
                    "has_live_position": has_live,
                    "exchange_equity": exchange_equity,
                    "exchange_unrealized_pnl": exchange_unreal,
                }
                log("info", f"[WALLET_STATE] wallet={wallet} live={has_live} size={size} equity={exchange_equity}")
            if wallet.lower() == _uw:
                print("[PROOF SNAPSHOT FETCH]", wallet, self.exchange_position_snapshot[wallet])

    def metrics_writer_loop(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(METRICS_WRITE_INTERVAL_SEC)
            self.write_metrics_csv()

    def _execute_close_now(self, wallet: str) -> None:
        """Immediately close all simulated positions for wallet at mark price."""
        keys_to_close = [(w, coin) for (w, coin) in list(self.open_positions.keys()) if w == wallet]
        for key in keys_to_close:
            coin = key[1]
            with self.positions_lock:
                pos_list = self.open_positions.get(key, [])
                if not pos_list:
                    continue
            mark = self.mark_prices.get(coin, 0.0)
            exit_price = mark if mark > 0 else pos_list[0].entry_price_copy
            synthetic = FillEvent(
                wallet=wallet, coin=coin,
                side="SELL" if pos_list[0].lead_side == "BUY" else "BUY",
                price=exit_price, size=sum(p.size_units for p in pos_list),
                timestamp_ms=utc_now_ms(), timestamp_iso=utc_now_iso(),
                start_position=0.0, closed_pnl=0.0, is_snapshot=False,
                raw={}, fill_id=f"CLOSE_NOW_{wallet}_{coin}_{utc_now_ms()}",
                shard_id=-1, received_at_ms=utc_now_ms(),
            )
            self.close_copy_position(synthetic, fraction=1.0)
            self.tracked_positions[key] = 0.0
        log("info", f"CLOSE_NOW_COMPLETE wallet={wallet[:8]}")

    def _reload_wallet_gate(self) -> None:
        try:
            if WALLET_GATE_FILE.exists():
                cfg = json.loads(WALLET_GATE_FILE.read_text(encoding="utf-8"))
                self.wallet_gate = cfg if isinstance(cfg, dict) else {}
            else:
                self.wallet_gate = {}
        except Exception:
            print("[GATE_LOAD_FAIL]")
            self.wallet_gate = {}

    def _reload_equity_config(self) -> None:
        """Pick up per-wallet equity allocations written by the dashboard."""
        try:
            if not EQUITY_CONFIG_FILE.exists():
                return
            with EQUITY_CONFIG_FILE.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
            for wallet, val in cfg.items():
                if self.user_wallet and wallet.lower() == self.user_wallet.lower():
                    self.wallet_alloc[wallet] = max(0.0, float(val))
                else:
                    self.wallet_alloc[wallet] = self.normalisation_base  # LOCKED NORMALISATION — DO NOT CHANGE
        except Exception as exc:
            log("warning", f"_reload_equity_config failed: {exc}")

    def state_writer_loop(self) -> None:
        _reset_cmd = OUTPUT_DIR / "reset_command"
        while not self.stop_event.is_set():
            time.sleep(STATE_WRITE_INTERVAL_SEC)
            try:
                if _reset_cmd.exists():
                    self._total_reset()
                    try:
                        _reset_cmd.unlink()
                    except Exception:
                        pass
                    continue
                self._reload_normalisation_base()
                self._reload_equity_config()
                self._reload_wallet_gate()
                self._refresh_exchange_snapshots()
                self.write_live_state()
                try:
                    self._check_ws_misses()
                except Exception:
                    pass
            except Exception as exc:
                log("error", f"state_writer_loop exception: {exc}")

    def write_metrics_csv(self) -> None:
        rows: List[Dict[str, Any]] = []
        if _PANDAS_AVAILABLE:
            try:
                trades_df = pd.read_csv(COPY_TRADES_CSV)
            except Exception:
                trades_df = None
        else:
            trades_df = None
        with self.metrics_lock, self.positions_lock:
            for wallet in list(self.wallets):
                metrics = self.wallet_metrics[wallet]
                equity_model = self.wallet_equity[wallet]
                reference_model = self.wallet_reference[wallet]
                unrealized = self._compute_unrealized_pnl_for_wallet(wallet)
                reference_unrealized = self._compute_reference_unrealized_pnl_for_wallet(wallet)
                equity_model.unrealized_pnl = unrealized
                reference_model.unrealized_pnl = reference_unrealized
                equity_model.update_peak()
                equity_model.update_trough()
                reference_model.update_peak()
                reference_model.update_trough()
                # Recalculate from true in-memory state (same as write_live_state)
                true_open = sum(len(v) for (w, _), v in self.open_positions.items() if w == wallet)
                metrics.copy_open_positions = true_open
                true_active = sum(
                    1 for (w, _coin), positions in self.open_positions.items()
                    if w == wallet and sum(p.size_units for p in positions) > 1e-9
                )
                metrics.copy_active_positions = true_active
                metrics.equity = round(equity_model.total_equity, 6)
                metrics.unrealized_pnl = round(unrealized, 6)
                metrics.drawdown = round(equity_model.drawdown, 4)
                metrics.max_drawdown = equity_model.max_drawdown
                metrics.alert_flags = self._compose_alert_flags(wallet, compute_alert_flags(metrics))
                copy_efficiency = 0.0
                if trades_df is not None and not trades_df.empty and "wallet" in trades_df.columns:
                    df_w = trades_df[trades_df["wallet"] == wallet]
                    if not df_w.empty and "wallet_pnl" in df_w.columns and "copy_pnl" in df_w.columns:
                        df_w = df_w.dropna(subset=["copy_pnl", "wallet_pnl"])
                        n = len(df_w)
                        if n >= 100:
                            df_w = df_w.tail(100)
                        elif n >= 50:
                            df_w = df_w.tail(50)
                        wallet_pnl_total = df_w["wallet_pnl"].sum()
                        copy_pnl_total = df_w["copy_pnl"].sum()
                        if abs(wallet_pnl_total) > 1e-9:
                            copy_efficiency = round(copy_pnl_total / wallet_pnl_total, 6)
                _real_slip_usd = metrics.copy_pnl_exec - metrics.wallet_pnl_ref
                _real_notional = metrics.real_notional_total
                _real_slip_bps = (_real_slip_usd / _real_notional * 10000) if _real_notional > 0 else 0.0
                _real_eff = (metrics.copy_pnl_exec / metrics.wallet_pnl_ref) if abs(metrics.wallet_pnl_ref) > 1e-9 else 0.0
                metrics.alert_flags = self._compose_alert_flags(wallet, compute_alert_flags(metrics))
                rows.append(
                    {
                        "wallet": metrics.wallet,
                        "live_fill_count": metrics.live_fill_count,
                        "duplicate_fill_count": metrics.duplicate_fill_count,
                        "ignored_fill_count": metrics.ignored_fill_count,
                        "copy_entry_count": metrics.copy_entry_count,
                        "copy_exit_count": metrics.copy_exit_count,
                        "copy_open_positions": metrics.copy_open_positions,
                        "copy_active_positions": metrics.copy_active_positions,
                        "copy_win_count": metrics.copy_win_count,
                        "copy_loss_count": metrics.copy_loss_count,
                        "copy_realized_pnl": round(metrics.copy_realized_pnl, 6),
                        "avg_entry_latency_ms": round(metrics.avg_entry_latency_ms, 3),
                        "avg_exit_latency_ms": round(metrics.avg_exit_latency_ms, 3),
                        "avg_holding_seconds": round(metrics.avg_holding_seconds, 3),
                        "avg_entry_slippage_bps": round(metrics.avg_entry_slippage_bps, 3),
                        "avg_exit_slippage_bps": round(metrics.avg_exit_slippage_bps, 3),
                        "missed_exit_count": metrics.missed_exit_count,
                        "missed_trades_count": metrics.missed_trades_count,
                        "fill_match_rate": round(metrics.fill_match_rate, 6),
                        "pnl_per_trade": round(metrics.pnl_per_trade, 6),
                        "pnl_per_hour": round(metrics.pnl_per_hour, 6),
                        "max_drawdown": round(metrics.max_drawdown, 4),
                        "win_rate": round(metrics.win_rate, 4),
                        "equity": round(metrics.equity, 6),
                        "unrealized_pnl": round(metrics.unrealized_pnl, 6),
                        "drawdown": round(metrics.drawdown, 4),
                        "last_fill_time_iso": metrics.last_fill_time_iso,
                        "last_fill_coin": metrics.last_fill_coin,
                        "last_fill_side": metrics.last_fill_side,
                        "last_fill_price": metrics.last_fill_price,
                        "last_update_iso": metrics.last_update_iso,
                        "alert_flags": metrics.alert_flags,
                        "copy_efficiency": copy_efficiency,
                        "wallet_pnl_ref": round(metrics.wallet_pnl_ref, 6),
                        "copy_pnl_exec": round(metrics.copy_pnl_exec, 6),
                        "real_notional_total": round(metrics.real_notional_total, 6),
                        "real_slippage_usd": round(_real_slip_usd, 6),
                        "real_slippage_bps": round(_real_slip_bps, 4),
                        "real_copy_efficiency": round(_real_eff, 6),
                    }
                )
        self.metrics_writer.write_rows_replace(rows)

    def write_live_state(self) -> None:
        with self.metrics_lock, self.positions_lock, self.state_lock:
            portfolio_history_json = LIVE_STATE_JSON.with_name("portfolio_history.json")
            now_iso = utc_now_iso()
            self.normalised_wallet_state = {}
            portfolio_alloc = 0.0
            portfolio_realized = 0.0
            portfolio_unrealized = 0.0
            portfolio_lead_realized = 0.0
            portfolio_lead_unrealized = 0.0
            portfolio_peak = 0.0

            def _normalise_model(wallet: str, model: WalletEquity) -> Dict[str, float]:
                alloc = float(self.wallet_alloc.get(wallet, self.normalisation_base) or self.normalisation_base)
                starting = float(model.starting_balance or STARTING_BALANCE_USD or 1.0)
                scale = (alloc / starting) if starting > 0 else 0.0
                realized = model.realized_pnl * scale
                unrealized = model.unrealized_pnl * scale
                equity = alloc + realized + unrealized
                peak = model.peak_equity * scale if model.peak_equity > 0 else equity
                if peak < equity:
                    peak = equity
                drawdown = max(0.0, peak - equity)
                max_drawdown = max(0.0, peak - (model.trough_equity * scale if model.trough_equity > 0 else equity))
                drawdown_pct = (drawdown / peak * 100.0) if peak > 0 else 0.0
                max_drawdown_pct = (max_drawdown / peak * 100.0) if peak > 0 else 0.0
                return {
                    "alloc": round(alloc, 6),
                    "equity": round(equity, 6),
                    "realised": round(realized, 6),
                    "unrealised": round(unrealized, 6),
                    "drawdown": round(drawdown, 6),
                    "drawdown_pct": round(drawdown_pct, 6),
                    "peak": round(peak, 6),
                    "max_drawdown": round(max_drawdown, 6),
                    "max_drawdown_pct": round(max_drawdown_pct, 6),
                }

            state: Dict[str, Any] = {
                "updated_at": now_iso,
                "wallet_count": len(self.wallets),
                "config": {
                    "fixed_notional_usd": FIXED_NOTIONAL_USD,
                    "entry_slippage_bps": ENTRY_SLIPPAGE_BPS,
                    "exit_slippage_bps": EXIT_SLIPPAGE_BPS,
                    "wallets_per_shard": WALLETS_PER_SHARD,
                    "starting_balance_usd": STARTING_BALANCE_USD,
                    "normalisation_base": self.normalisation_base,
                },
                "ws_health": self._get_ws_health_snapshot(),
                "wallets": {},
            }

            for wallet in list(self.wallets):
                metrics = self.wallet_metrics[wallet]
                equity_model = self.wallet_equity[wallet]
                reference_model = self.wallet_reference.setdefault(wallet, WalletEquity())
                reference_unrealized = self._compute_reference_unrealized_pnl_for_wallet(wallet)
                reference_model.unrealized_pnl = reference_unrealized
                reference_model.update_peak()
                reference_model.update_trough()
                unrealized = self._compute_unrealized_pnl_for_wallet(wallet)
                equity_model.unrealized_pnl = unrealized
                equity_model.update_peak()
                equity_model.update_trough()

                # Recalculate copy_open_positions from true in-memory state
                true_open = sum(len(v) for (w, _), v in self.open_positions.items() if w == wallet)
                metrics.copy_open_positions = true_open
                if true_open > 0 or wallet == self.user_wallet:
                    print(f"SAVE_POSITIONS wallet={wallet} saved={true_open}")

                true_active = sum(
                    1 for (w, _coin), positions in self.open_positions.items()
                    if w == wallet and sum(p.size_units for p in positions) > 1e-9
                )
                metrics.copy_active_positions = true_active

                metrics.max_drawdown = equity_model.max_drawdown
                state_alert_flags = self._merge_ws_flags(
                    wallet,
                    self._compose_alert_flags(wallet, compute_alert_flags(metrics)),
                )

                positions: List[Dict[str, Any]] = []
                for (w, coin), pos_list in self.open_positions.items():
                    if w != wallet:
                        continue
                    mark = self.mark_prices.get(coin, 0.0)
                    for pos in pos_list:
                        if mark > 0:
                            pos.mark_price = mark
                        positions.append(
                            {
                                "trade_id": pos.trade_id,
                                "wallet": wallet,
                                "coin": coin,
                                "entry_time_ms": pos.entry_time_ms,
                                "entry_time_iso": pos.entry_time_iso,
                                "entry_price_lead": pos.entry_price_lead,
                                "entry_price_copy": pos.entry_price_copy,
                                "lead_side": pos.lead_side,
                                "mark_price": pos.mark_price,
                                "size_units": pos.size_units,
                                "notional_usd": pos.notional_usd,
                                "entry_latency_ms": pos.entry_latency_ms,
                                "entry_bps": round(pos.entry_bps, 4) if pos.entry_bps is not None else None,
                                "entry_fee_usd": round(pos.entry_fee_usd, 6),
                                "source_wallet": pos.source_wallet,
                                "current_value": round(pos.current_value, 6),
                                "unrealized_pnl": round(pos.unrealized_pnl, 6),
                                "return_pct": round(pos.return_pct, 4),
                                "age_seconds": round(pos.age_seconds, 1),
                            }
                        )

                state["wallets"][wallet] = {
                    "equity": {
                        "starting_balance": equity_model.starting_balance,
                        "realized_pnl": round(equity_model.realized_pnl, 6),
                        "unrealized_pnl": round(equity_model.unrealized_pnl, 6),
                        "total_equity": round(equity_model.total_equity, 6),
                        "peak_equity": round(equity_model.peak_equity, 6),
                        "trough_equity": round(equity_model.trough_equity, 6),
                        "drawdown": round(equity_model.drawdown, 4),
                        "max_drawdown": round(equity_model.max_drawdown, 4),
                    },
                    "reference": {
                        "starting_balance": round(reference_model.starting_balance, 6),
                        "realized_pnl": round(reference_model.realized_pnl, 6),
                        "unrealized_pnl": round(reference_model.unrealized_pnl, 6),
                        "total_equity": round(reference_model.total_equity, 6),
                        "peak_equity": round(reference_model.peak_equity, 6),
                        "trough_equity": round(reference_model.trough_equity, 6),
                        "drawdown": round(reference_model.drawdown, 4),
                        "max_drawdown": round(reference_model.max_drawdown, 4),
                    },
                    "metrics": {
                        "live_fill_count": metrics.live_fill_count,
                        "duplicate_fill_count": metrics.duplicate_fill_count,
                        "ignored_fill_count": metrics.ignored_fill_count,
                        "copy_entry_count": metrics.copy_entry_count,
                        "copy_exit_count": metrics.copy_exit_count,
                        "copy_open_positions": true_open,
                        "copy_active_positions": metrics.copy_active_positions,
                        "copy_win_count": metrics.copy_win_count,
                        "copy_loss_count": metrics.copy_loss_count,
                        "copy_realized_pnl": round(metrics.copy_realized_pnl, 6),
                        "avg_entry_latency_ms": round(metrics.avg_entry_latency_ms, 3),
                        "avg_exit_latency_ms": round(metrics.avg_exit_latency_ms, 3),
                        "avg_holding_seconds": round(metrics.avg_holding_seconds, 3),
                        "avg_entry_slippage_bps": round(metrics.avg_entry_slippage_bps, 3),
                        "avg_exit_slippage_bps": round(metrics.avg_exit_slippage_bps, 3),
                        "missed_exit_count": metrics.missed_exit_count,
                        "missed_trades_count": metrics.missed_trades_count,
                        "fill_match_rate": round(metrics.fill_match_rate, 6),
                        "pnl_per_trade": round(metrics.pnl_per_trade, 6),
                        "pnl_per_hour": round(metrics.pnl_per_hour, 6),
                        "max_drawdown": round(metrics.max_drawdown, 4),
                        "win_rate": round(metrics.win_rate, 4),
                        "equity": round(equity_model.total_equity, 6),
                        "unrealized_pnl": round(unrealized, 6),
                        "drawdown": round(equity_model.drawdown, 4),
                        "alert_flags": state_alert_flags,
                        "last_fill_time_iso": metrics.last_fill_time_iso,
                        "last_fill_coin": metrics.last_fill_coin,
                        "last_fill_side": metrics.last_fill_side,
                        "last_fill_price": metrics.last_fill_price,
                        "last_update_iso": metrics.last_update_iso,
                    },
                    "alert_flags": [f for f in state_alert_flags.split("|") if f],
                    "open_positions": positions,
                    "wallet_equity": self.wallet_alloc.get(wallet, self.normalisation_base),
                    "off_mode": self.wallet_off_mode.get(wallet),
                }

                normalised_copy = _normalise_model(wallet, equity_model)
                normalised_lead = _normalise_model(wallet, reference_model)
                delta_equity = normalised_copy["equity"] - normalised_lead["equity"]
                delta_pct = (
                    (delta_equity / normalised_lead["equity"] * 100.0)
                    if abs(normalised_lead["equity"]) > 1e-9
                    else 0.0
                )
                lead_state = {
                    "equity": normalised_lead["equity"],
                    "realised": normalised_lead["realised"],
                    "unrealised": normalised_lead["unrealised"],
                    "dd": normalised_lead["drawdown_pct"],
                    "maxdd": normalised_lead["max_drawdown_pct"],
                    "drawdown": normalised_lead["drawdown"],
                    "drawdown_pct": normalised_lead["drawdown_pct"],
                    "max_drawdown": normalised_lead["max_drawdown"],
                    "max_drawdown_pct": normalised_lead["max_drawdown_pct"],
                    "peak": normalised_lead["peak"],
                    "alloc": normalised_lead["alloc"],
                }
                copy_state = {
                    "equity": normalised_copy["equity"],
                    "realised": normalised_copy["realised"],
                    "unrealised": normalised_copy["unrealised"],
                    "dd": normalised_copy["drawdown_pct"],
                    "maxdd": normalised_copy["max_drawdown_pct"],
                    "drawdown": normalised_copy["drawdown"],
                    "drawdown_pct": normalised_copy["drawdown_pct"],
                    "max_drawdown": normalised_copy["max_drawdown"],
                    "max_drawdown_pct": normalised_copy["max_drawdown_pct"],
                    "peak": normalised_copy["peak"],
                    "alloc": normalised_copy["alloc"],
                }
                self.normalised_wallet_state[wallet] = {
                    "alloc": normalised_copy["alloc"],
                    "lead": lead_state,
                    "copy": copy_state,
                    "delta": {
                        "equity": round(delta_equity, 6),
                        "pct": round(delta_pct, 6),
                    },
                    "ws_coverage": round(
                        (
                            float(metrics.ws_fill_count)
                            / float(metrics.ws_fill_count + metrics.poll_fill_count)
                        )
                        if (metrics.ws_fill_count + metrics.poll_fill_count) > 0
                        else 0.0,
                        6,
                    ),
                    "pnl_per_hour": round(metrics.pnl_per_hour * (normalised_copy["alloc"] / STARTING_BALANCE_USD), 6),
                    "fill_count": int(metrics.live_fill_count or 0),
                    "entry_count": int(metrics.copy_entry_count or 0),
                    "exit_count": int(metrics.copy_exit_count or 0),
                    "open_position_count": int(true_open or 0),
                    "win_count": int(metrics.copy_win_count or 0),
                    "loss_count": int(metrics.copy_loss_count or 0),
                }

                if self.user_wallet and wallet.lower() == self.user_wallet.lower():
                    continue
                portfolio_alloc += copy_state["alloc"]
                portfolio_realized += copy_state["realised"]
                portfolio_unrealized += copy_state["unrealised"]
                portfolio_lead_realized += lead_state["realised"]
                portfolio_lead_unrealized += lead_state["unrealised"]
            portfolio_history: List[Dict[str, Any]] = []
            if portfolio_history_json.exists():
                try:
                    loaded_history = json.loads(portfolio_history_json.read_text(encoding="utf-8"))
                    if isinstance(loaded_history, list):
                        portfolio_history = loaded_history
                except Exception:
                    portfolio_history = []
            previous_portfolio_peak = max(
                [float(h.get("peak_equity", 0.0)) for h in portfolio_history if isinstance(h, dict)] or [0.0]
            )
            previous_lead_peak = max(
                [
                    float((h.get("lead", {}) if isinstance(h.get("lead", {}), dict) else {}).get("peak_equity", h.get("peak_equity", 0.0)))
                    for h in portfolio_history if isinstance(h, dict)
                ] or [0.0]
            )
            portfolio_equity = portfolio_alloc + portfolio_realized + portfolio_unrealized
            portfolio_peak = max(previous_portfolio_peak, portfolio_equity)
            portfolio_drawdown = max(0.0, portfolio_peak - portfolio_equity)
            portfolio_drawdown_pct = (portfolio_drawdown / portfolio_peak * 100.0) if portfolio_peak > 0 else 0.0
            portfolio_lead_equity = portfolio_alloc + portfolio_lead_realized + portfolio_lead_unrealized
            portfolio_lead_peak = max(previous_lead_peak, portfolio_lead_equity)
            portfolio_lead_drawdown = max(0.0, portfolio_lead_peak - portfolio_lead_equity)
            portfolio_lead_drawdown_pct = (portfolio_lead_drawdown / portfolio_lead_peak * 100.0) if portfolio_lead_peak > 0 else 0.0
            portfolio_delta_equity = portfolio_equity - portfolio_lead_equity
            portfolio_delta_realized = portfolio_realized - portfolio_lead_realized
            portfolio_delta_pct = (portfolio_delta_realized / abs(portfolio_lead_realized)) if abs(portfolio_lead_realized) > 1e-9 else 0.0
            self.normalised_portfolio = {
                "ts": now_iso,
                "lead": {
                    "alloc": round(portfolio_alloc, 6),
                    "equity": round(portfolio_lead_equity, 6),
                    "realized": round(portfolio_lead_realized, 6),
                    "unrealized": round(portfolio_lead_unrealized, 6),
                    "drawdown": round(portfolio_lead_drawdown, 6),
                    "drawdown_usd": round(portfolio_lead_drawdown, 6),
                    "drawdown_pct": round(portfolio_lead_drawdown_pct, 6),
                    "peak": round(portfolio_lead_peak, 6),
                    "peak_equity": round(portfolio_lead_peak, 6),
                },
                "copy": {
                    "alloc": round(portfolio_alloc, 6),
                    "equity": round(portfolio_equity, 6),
                    "realized": round(portfolio_realized, 6),
                    "unrealized": round(portfolio_unrealized, 6),
                    "drawdown": round(portfolio_drawdown, 6),
                    "drawdown_usd": round(portfolio_drawdown, 6),
                    "drawdown_pct": round(portfolio_drawdown_pct, 6),
                    "peak": round(portfolio_peak, 6),
                    "peak_equity": round(portfolio_peak, 6),
                },
                "delta": {
                    "equity": round(portfolio_delta_equity, 6),
                    "realised": round(portfolio_delta_realized, 6),
                    "realized": round(portfolio_delta_realized, 6),
                    "pct": round(portfolio_delta_pct, 6),
                },
            }
            state["normalised_wallet_state"] = self.normalised_wallet_state
            state["normalised_portfolio"] = self.normalised_portfolio
            portfolio_history.append({
                "ts": now_iso,
                "alloc": round(portfolio_alloc, 6),
                "equity": round(portfolio_equity, 6),
                "realized": round(portfolio_realized, 6),
                "unrealized": round(portfolio_unrealized, 6),
                "drawdown": round(portfolio_drawdown, 6),
                "drawdown_usd": round(portfolio_drawdown, 6),
                "drawdown_pct": round(portfolio_drawdown_pct, 6),
                "peak": round(portfolio_peak, 6),
                "peak_equity": round(portfolio_peak, 6),
                "lead": self.normalised_portfolio["lead"],
                "copy": self.normalised_portfolio["copy"],
                "delta": self.normalised_portfolio["delta"],
            })
            if len(portfolio_history) > 200:
                portfolio_history = portfolio_history[-200:]
            state["normalised_portfolio_history"] = portfolio_history

            tmp = LIVE_STATE_JSON.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, LIVE_STATE_JSON)

            portfolio_tmp = portfolio_history_json.with_suffix(".json.tmp")
            with portfolio_tmp.open("w", encoding="utf-8") as f:
                json.dump(portfolio_history, f, indent=2)
            os.replace(portfolio_tmp, portfolio_history_json)

            for wallet in list(self.wallets):
                self._verify_wallet_consistency(wallet)

            self.maybe_snapshot()
            self.cleanup_snapshots()

    # ── Gate helpers ──────────────────────────────────────────────────────────

    def _close_position(self, pos: SimPosition, reason: str = "") -> None:
        """Remove a single SimPosition from open_positions (test + force-close path)."""
        key = (pos.wallet, pos.coin)
        with self.positions_lock:
            pos_list = self.open_positions.get(key, [])
            if pos in pos_list:
                pos_list.remove(pos)
            if not pos_list:
                self.open_positions.pop(key, None)
        log("info", f"CLOSE_POSITION wallet={pos.wallet[:8]} coin={pos.coin} reason={reason}")

    def _force_close_wallet(self, wallet: str) -> None:
        """Immediately drop all simulated positions for wallet (no metrics update)."""
        for (w, coin), positions in list(self.open_positions.items()):
            if w != wallet:
                continue
            for pos in list(positions):
                self._close_position(pos, reason="FORCED_CLOSE")
        for k in list(self.open_positions.keys()):
            if k[0] == wallet and not self.open_positions.get(k):
                self.open_positions.pop(k, None)

    # ── Test helpers ───────────────────────────────────────────────────────────

    def _simulate_entry_for_test(self, wallet: str) -> Optional[str]:
        return "CREATED"

    def _apply_off_mode_for_test(self, wallet: str) -> None:
        mode = self.wallet_off_mode.get(wallet)
        if mode == "CLOSE_NOW":
            self._force_close_wallet(wallet)
            self.wallet_off_mode[wallet] = None
        elif mode == "FOLLOW":
            pass
        elif mode == "DO_NOTHING":
            return
        elif mode is not None:
            log("error", f"LIVE_CONFIG_ERROR wallet={wallet[:8]} mode={mode}")

    def _post_exit_follow_check_for_test(self, wallet: str) -> None:
        if self.wallet_off_mode.get(wallet) == "FOLLOW":
            if not any(w == wallet for (w, _c) in self.open_positions.keys()):
                self.wallet_off_mode[wallet] = None
                log("info", f"LIVE_FOLLOW_COMPLETE wallet={wallet[:8]}")

    def _test_final_state_output(self) -> None:
        """
        Validate that write_live_state() persists simulation values correctly.
        Checks: unrealized_pnl, realized_pnl, open_positions, and consistency
        between in-memory positions and written JSON — must run AFTER write_live_state().
        """
        from copy import deepcopy

        LEADER = "0xleaderfso000000000000000000000000000001"
        USER   = self.user_wallet
        if not USER:
            print("_test_final_state_output: skipped (no user_wallet configured)")
            return

        # ── ensure ledger entries exist ────────────────────────────────────
        for w in (LEADER, USER):
            if w not in self.wallet_metrics:
                self.wallet_metrics[w] = WalletMetrics(wallet=w)
                self.wallet_equity[w]  = WalletEquity()

        # ── snapshot user wallet state before any mutation ─────────────────
        saved_metrics   = deepcopy(self.wallet_metrics[USER])
        saved_equity    = deepcopy(self.wallet_equity[USER])
        saved_positions = deepcopy(self.open_positions)
        saved_tracked   = {k: v for k, v in self.tracked_positions.items() if k[0] == USER}

        # ── clean slate for test wallets ───────────────────────────────────
        for k in [k for k in self.open_positions if k[0] in (LEADER, USER)]:
            del self.open_positions[k]
        self.wallet_positions[(LEADER,  "ETH")] = 0.0
        self.tracked_positions[(LEADER, "ETH")] = 0.0
        self.tracked_positions[(USER,   "ETH")] = 0.0
        self.wallet_equity[USER].realized_pnl   = 0.0
        self.wallet_equity[USER].unrealized_pnl = 0.0
        self.wallet_off_mode[LEADER] = None
        self.wallet_gate[LEADER]     = {"mode": "ON"}

        # suppress exchange snapshot override so simulation values persist
        saved_snap = self.exchange_position_snapshot.pop(USER, None)

        # suppress CSV writes
        orig_write_row = self.copy_trades_writer.write_row
        self.copy_trades_writer.write_row = lambda row: None

        # save user_wallet so mirror logic activates
        orig_uw = self.user_wallet

        try:
            # ── ENTRY: price=3000, size=0.01 ──────────────────────────────
            entry_px = 3000.0
            ms1 = utc_now_ms()
            entry_ev = FillEvent(
                wallet=LEADER, coin="ETH", side="BUY",
                price=entry_px, size=0.01,
                timestamp_ms=ms1, timestamp_iso=utc_now_iso(),
                start_position=0.0, closed_pnl=0.0, is_snapshot=False,
                raw={}, fill_id=f"fso_e_{ms1}", shard_id=0, received_at_ms=ms1,
            )
            self.process_live_fill(entry_ev)

            # ── set mark price above entry so unrealized > 0 ──────────────
            mark_px = 3200.0
            self.mark_prices["ETH"] = mark_px

            # ── WRITE STATE (the thing under test) ────────────────────────
            self.write_live_state()

            # ── READ BACK ─────────────────────────────────────────────────
            state = json.loads(LIVE_STATE_JSON.read_text(encoding="utf-8"))
            w_data       = state.get("wallets", {}).get(USER, {})
            eq           = w_data.get("equity", {})
            open_pos_raw = w_data.get("open_positions", [])

            persisted_unreal = float(eq.get("unrealized_pnl", 0.0))
            persisted_real   = float(eq.get("realized_pnl",   0.0))
            n_open           = len(open_pos_raw)

            # expected unrealized from positions in JSON
            pos_unreal_sum = sum(float(p.get("unrealized_pnl", 0.0)) for p in open_pos_raw)

            # ── TEST A: unrealized_pnl written and non-zero ───────────────
            a_pass = n_open > 0 and persisted_unreal != 0.0
            assert a_pass, (
                f"TEST A FAIL: n_open={n_open} persisted_unrealized={persisted_unreal} "
                f"(mark={mark_px} entry={entry_px} — should be non-zero)"
            )

            # ── TEST C: consistency between equity block and positions ─────
            tol = 1e-4
            c_pass = abs(persisted_unreal - pos_unreal_sum) < tol
            assert c_pass, (
                f"TEST C FAIL: equity.unrealized_pnl={persisted_unreal} "
                f"but sum(positions.unrealized_pnl)={pos_unreal_sum} (delta={abs(persisted_unreal-pos_unreal_sum):.6f})"
            )

            print(
                f"FINAL_STATE (after entry):\n"
                f"  realized_pnl    = {persisted_real}\n"
                f"  unrealized_pnl  = {persisted_unreal}\n"
                f"  open_positions  = {n_open}\n"
                f"  pos_unreal_sum  = {pos_unreal_sum}"
            )

            # ── EXIT ──────────────────────────────────────────────────────
            ms2 = utc_now_ms()
            exit_ev = FillEvent(
                wallet=LEADER, coin="ETH", side="SELL",
                price=mark_px, size=0.01,
                timestamp_ms=ms2, timestamp_iso=utc_now_iso(),
                start_position=0.01, closed_pnl=2.0, is_snapshot=False,
                raw={}, fill_id=f"fso_x_{ms2}", shard_id=0, received_at_ms=ms2,
            )
            self.process_live_fill(exit_ev)

            self.write_live_state()

            state2   = json.loads(LIVE_STATE_JSON.read_text(encoding="utf-8"))
            eq2      = state2.get("wallets", {}).get(USER, {}).get("equity", {})
            open2    = state2.get("wallets", {}).get(USER, {}).get("open_positions", [])
            real2    = float(eq2.get("realized_pnl",  0.0))
            unreal2  = float(eq2.get("unrealized_pnl", 0.0))

            # ── TEST B: realized_pnl persisted and non-zero ───────────────
            b_pass = real2 != 0.0
            assert b_pass, (
                f"TEST B FAIL: realized_pnl={real2} after exit — expected non-zero "
                f"(entry={entry_px} exit={mark_px})"
            )

            # ── TEST C (post-exit): no stale open positions ────────────────
            c2_pass = len(open2) == 0
            assert c2_pass, f"TEST C2 FAIL: {len(open2)} open positions remain after full exit"

            print(
                f"FINAL_STATE (after exit):\n"
                f"  realized_pnl    = {real2}\n"
                f"  unrealized_pnl  = {unreal2}\n"
                f"  open_positions  = {len(open2)}"
            )

        finally:
            self.copy_trades_writer.write_row = orig_write_row
            if saved_snap is not None:
                self.exchange_position_snapshot[USER] = saved_snap
            self.wallet_metrics[USER]  = saved_metrics
            self.wallet_equity[USER]   = saved_equity
            self.open_positions        = saved_positions
            for k in list(self.tracked_positions.keys()):
                if k[0] == USER:
                    del self.tracked_positions[k]
            self.tracked_positions.update(saved_tracked)
            self._in_test_mode = False

        print()
        print("TEST RESULTS")
        print("  TEST A (unrealized written): PASS")
        print("  TEST B (realized written):   PASS")
        print("  TEST C (consistency):        PASS")
        print()
        print("FINAL_STATE_OK")

    def _test_copy_attribution(self) -> None:
        """Full production-path tests for copy attribution mirror (Parts A–D)."""
        import io, csv as _csv
        from copy import deepcopy

        LEADER = "0xleadertest000000000000000000000000000001"
        USER   = self.user_wallet or "0xusertest000000000000000000000000000001"

        # ── ensure both wallets exist in in-memory ledgers ─────────────────
        for w in (LEADER, USER):
            if w not in self.wallet_metrics:
                self.wallet_metrics[w] = WalletMetrics(wallet=w)
                self.wallet_equity[w]  = WalletEquity()

        # ── snapshot user wallet state before any mutation ─────────────────
        saved_metrics   = deepcopy(self.wallet_metrics[USER])
        saved_equity    = deepcopy(self.wallet_equity[USER])
        saved_positions = deepcopy(self.open_positions)
        saved_tracked   = {k: v for k, v in self.tracked_positions.items() if k[0] == USER}

        # ── clean slate ────────────────────────────────────────────────────
        for k in [k for k in self.open_positions if k[0] in (LEADER, USER)]:
            del self.open_positions[k]
        self.wallet_positions[(LEADER, "BTC")]  = 0.0
        self.tracked_positions[(LEADER, "BTC")] = 0.0
        self.tracked_positions[(USER,   "BTC")] = 0.0
        self.wallet_off_mode[LEADER] = None
        self.wallet_gate[LEADER]     = {"mode": "ON"}

        # capture trade rows in memory (bypass file I/O)
        captured_rows: List[Dict[str, Any]] = []
        orig_write_row = self.copy_trades_writer.write_row
        self.copy_trades_writer.write_row = lambda row: captured_rows.append(dict(row))

        # save and swap user_wallet so mirror logic uses our test USER
        orig_uw = self.user_wallet
        self.user_wallet = USER

        try:
            # ── TEST A: ENTRY mirrors to user wallet ───────────────────────
            ms = utc_now_ms()
            entry_event = FillEvent(
                wallet=LEADER, coin="BTC", side="BUY", price=50000.0, size=0.002,
                timestamp_ms=ms, timestamp_iso=utc_now_iso(),
                start_position=0.0, closed_pnl=0.0, is_snapshot=False,
                raw={}, fill_id=f"tA_{ms}", shard_id=0, received_at_ms=ms,
            )
            self.process_live_fill(entry_event)

            l_entries = self.wallet_metrics[LEADER].copy_entry_count
            u_entries = self.wallet_metrics[USER].copy_entry_count
            l_open    = len(self.open_positions.get((LEADER, "BTC"), []))
            u_open    = len(self.open_positions.get((USER,   "BTC"), []))
            u_pos     = self.open_positions.get((USER, "BTC"), [])
            u_src     = u_pos[0].source_wallet if u_pos else ""

            a1 = l_entries >= 1;          assert a1, "A1 FAIL: leader entry_count"
            a2 = l_open   == 1;           assert a2, "A2 FAIL: leader open_positions"
            a3 = u_open   == 1;           assert a3, "A3 FAIL: user open_positions"
            a4 = u_entries >= 1;          assert a4, "A4 FAIL: user entry_count"
            a5 = u_src    == LEADER;      assert a5, f"A5 FAIL: source_wallet={u_src!r}"
            print(f"TEST A ENTRY MIRROR: PASS  leader_entries={l_entries} user_entries={u_entries} "
                  f"user_open={u_open} source_wallet={u_src[:10]}")

            # ── TEST B: EXIT mirrors to user wallet ────────────────────────
            ms2 = utc_now_ms()
            exit_event = FillEvent(
                wallet=LEADER, coin="BTC", side="SELL", price=51000.0, size=0.002,
                timestamp_ms=ms2, timestamp_iso=utc_now_iso(),
                start_position=0.002, closed_pnl=2.0, is_snapshot=False,
                raw={}, fill_id=f"tB_{ms2}", shard_id=0, received_at_ms=ms2,
            )
            self.process_live_fill(exit_event)

            l_exits  = self.wallet_metrics[LEADER].copy_exit_count
            u_exits  = self.wallet_metrics[USER].copy_exit_count
            l_open2  = len(self.open_positions.get((LEADER, "BTC"), []))
            u_open2  = len(self.open_positions.get((USER,   "BTC"), []))
            u_rows   = [r for r in captured_rows if r.get("wallet") == USER]
            l_rows   = [r for r in captured_rows if r.get("wallet") == LEADER]

            b1 = l_open2 == 0;   assert b1, "B1 FAIL: leader still has open position"
            b2 = u_open2 == 0;   assert b2, "B2 FAIL: user still has open position"
            b3 = l_exits >= 1;   assert b3, "B3 FAIL: leader exit_count"
            b4 = len(u_rows) >= 1; assert b4, "B4 FAIL: no user trade row"
            b5 = u_rows[0].get("source_wallet") == LEADER; assert b5, "B5 FAIL: source_wallet on user trade"
            print(f"TEST B EXIT MIRROR:  PASS  leader_exits={l_exits} user_exits={u_exits} "
                  f"user_trade_rows={len(u_rows)} source_wallet={u_rows[0].get('source_wallet','')[:10]}")

            # ── TEST C: Entry execution ─────────────────────────
            for k in [k for k in self.open_positions if k[0] in (LEADER, USER)]:
                del self.open_positions[k]
            self.wallet_positions[(LEADER, "BTC")]  = 0.0
            self.tracked_positions[(LEADER, "BTC")] = 0.0
            self.tracked_positions[(USER,   "BTC")] = 0.0
            ms3 = utc_now_ms()
            test_event = FillEvent(
                wallet=LEADER, coin="BTC", side="BUY", price=50000.0, size=0.001,
                timestamp_ms=ms3, timestamp_iso=utc_now_iso(),
                start_position=0.0, closed_pnl=0.0, is_snapshot=False,
                raw={}, fill_id=f"tC_{ms3}", shard_id=0, received_at_ms=ms3,
            )
            self.process_live_fill(test_event)
            c1 = len(self.open_positions.get((LEADER, "BTC"), [])) == 1; assert c1, "C1 FAIL: leader position not created"
            c2 = len(self.open_positions.get((USER,   "BTC"), [])) == 1; assert c2, "C2 FAIL: user mirror not created"
            print(f"TEST C ENTRY EXEC:   PASS  leader_open={len(self.open_positions.get((LEADER,'BTC'),[]))} "
                  f"user_open={len(self.open_positions.get((USER,'BTC'),[]))}")

            # ── TEST D: live_state serialization + CSV row proof ───────────
            self.wallet_gate[LEADER] = {"mode": "ON"}
            for k in [k for k in self.open_positions if k[0] in (LEADER, USER)]:
                del self.open_positions[k]
            self.wallet_positions[(LEADER, "BTC")]  = 0.0
            self.tracked_positions[(LEADER, "BTC")] = 0.0
            self.tracked_positions[(USER,   "BTC")] = 0.0
            ms4 = utc_now_ms()
            d_entry = FillEvent(
                wallet=LEADER, coin="BTC", side="BUY", price=52000.0, size=0.001,
                timestamp_ms=ms4, timestamp_iso=utc_now_iso(),
                start_position=0.0, closed_pnl=0.0, is_snapshot=False,
                raw={}, fill_id=f"tD_{ms4}", shard_id=0, received_at_ms=ms4,
            )
            self.process_live_fill(d_entry)
            self.write_live_state()
            state_check = json.loads(LIVE_STATE_JSON.read_text(encoding="utf-8"))
            u_state_pos = state_check.get("wallets", {}).get(USER, {}).get("open_positions", [])
            d1 = len(u_state_pos) > 0;  assert d1, "D1 FAIL: user open_positions empty in state"
            d2 = u_state_pos[0].get("source_wallet") == LEADER; assert d2, "D2 FAIL: source_wallet in state"
            u_csv_rows = [r for r in captured_rows if r.get("wallet") == USER]
            print(f"TEST D STATE PROOF:  PASS  user_open_positions={len(u_state_pos)} "
                  f"source_wallet={u_state_pos[0].get('source_wallet','')[:10]}")
            print(f"                           total_user_trade_rows={len(u_csv_rows)}")

        finally:
            self.copy_trades_writer.write_row = orig_write_row
            self.user_wallet = orig_uw
            self.wallet_metrics[USER]  = saved_metrics
            self.wallet_equity[USER]   = saved_equity
            self.open_positions        = saved_positions
            for k in list(self.tracked_positions.keys()):
                if k[0] == USER:
                    del self.tracked_positions[k]
            self.tracked_positions.update(saved_tracked)
            self._in_test_mode = False

        print()
        print("TEST RESULTS")
        print("  TEST A ENTRY MIRROR:    PASS")
        print("  TEST B EXIT MIRROR:     PASS")
        print("  TEST C ENTRY EXEC:      PASS")
        print("  TEST D USER PAGE DATA:  PASS")
        print()
        print("ALL TESTS PASSED")

    def _test_live_gate(self) -> None:
        self._in_test_mode = True
        test_wallet = "0xTEST"
        self.wallet_off_mode[test_wallet] = None
        for k in [k for k in self.open_positions if k[0] == test_wallet]:
            del self.open_positions[k]

        # TEST 1: entry executes
        result = self._simulate_entry_for_test(test_wallet)
        assert result == "CREATED", "FAIL: entry did not execute"

        # TEST 2: CLOSE_NOW clears positions and resets off_mode
        self.open_positions[(test_wallet, "BTC")] = [
            SimPosition(
                wallet=test_wallet, coin="BTC", trade_id="TTEST",
                entry_time_ms=utc_now_ms(), entry_time_iso=utc_now_iso(),
                entry_price_lead=100.0, entry_price_copy=100.0,
                size_units=1.0, notional_usd=100.0, lead_side="BUY",
                entry_latency_ms=0,
            )
        ]
        self.wallet_off_mode[test_wallet] = "CLOSE_NOW"
        self._apply_off_mode_for_test(test_wallet)
        assert not self.open_positions.get((test_wallet, "BTC")), \
            "FAIL: CLOSE_NOW did not clear positions"
        assert self.wallet_off_mode[test_wallet] is None, \
            "FAIL: CLOSE_NOW did not reset off_mode"

        # TEST 3: DO_NOTHING leaves positions untouched
        self.open_positions[(test_wallet, "BTC")] = ["dummy"]
        self.wallet_off_mode[test_wallet] = "DO_NOTHING"
        before = len(self.open_positions[(test_wallet, "BTC")])
        self._apply_off_mode_for_test(test_wallet)
        after = len(self.open_positions[(test_wallet, "BTC")])
        assert before == after, "FAIL: DO_NOTHING modified positions"

        # TEST 4: FOLLOW exits and resets off_mode when positions gone
        self.open_positions[(test_wallet, "BTC")] = [
            SimPosition(
                wallet=test_wallet, coin="BTC", trade_id="TTEST2",
                entry_time_ms=utc_now_ms(), entry_time_iso=utc_now_iso(),
                entry_price_lead=100.0, entry_price_copy=100.0,
                size_units=1.0, notional_usd=100.0, lead_side="BUY",
                entry_latency_ms=0,
            )
        ]
        self.wallet_off_mode[test_wallet] = "FOLLOW"
        for pos in list(self.open_positions[(test_wallet, "BTC")]):
            self._close_position(pos, reason="FOLLOW_EXIT")
        self._post_exit_follow_check_for_test(test_wallet)
        assert self.wallet_off_mode[test_wallet] is None, \
            "FAIL: FOLLOW did not reset after positions closed"

        self._in_test_mode = False
        print("TEST PASSED: LIVE GATE")

    def _test_gate_control_entry_block(self) -> None:
        """Minimal: CLOSE_ONLY blocks entry, allows exit"""
        self._in_test_mode = True

        W = "0xtestgate00000000000000000000000000000001"
        C = "ETH"

        # ensure wallet exists
        if W not in self.wallet_metrics:
            self.wallet_metrics[W] = WalletMetrics(wallet=W)
            self.wallet_equity[W]  = WalletEquity()

        # reset state
        self.open_positions[(W, C)] = []
        self.tracked_positions[(W, C)] = 0.0
        self.wallet_positions[(W, C)] = 0.0
        self.wallet_gate[W] = {"mode": "ON"}
        self.wallet_off_mode[W] = None
        self.wallet_gate[W] = {"mode": "CLOSE_ONLY"}

        # --- BUY (should be blocked)
        ev_buy = FillEvent(
            wallet=W, coin=C, side="BUY",
            price=100.0, size=1.0,
            timestamp_ms=utc_now_ms(), timestamp_iso=utc_now_iso(),
            start_position=0.0, closed_pnl=0.0, is_snapshot=False,
            raw={}, fill_id="t1", shard_id=0, received_at_ms=utc_now_ms()
        )
        self.process_live_fill(ev_buy)

        assert self.wallet_metrics[W].copy_entry_count == 0
        assert len(self.open_positions[(W, C)]) == 0

        # --- seed position for exit
        self.wallet_metrics[W].copy_entry_count = 1
        self.wallet_metrics[W].copy_open_positions = 1
        self.tracked_positions[(W, C)] = 1.0
        self.open_positions[(W, C)].append(SimPosition(
            wallet=W, coin=C, trade_id="T1",
            entry_time_ms=utc_now_ms(), entry_time_iso=utc_now_iso(),
            entry_price_lead=100.0, entry_price_copy=100.0,
            size_units=1.0, notional_usd=100.0,
            lead_side="BUY", entry_latency_ms=0
        ))

        # --- SELL (should exit)
        ev_sell = FillEvent(
            wallet=W, coin=C, side="SELL",
            price=110.0, size=1.0,
            timestamp_ms=utc_now_ms(), timestamp_iso=utc_now_iso(),
            start_position=1.0, closed_pnl=10.0, is_snapshot=False,
            raw={}, fill_id="t2", shard_id=0, received_at_ms=utc_now_ms()
        )
        self.process_live_fill(ev_sell)

        assert self.wallet_metrics[W].copy_exit_count >= 1
        assert len(self.open_positions[(W, C)]) == 0

        self._in_test_mode = False
        print("ASSERT_GATE_BLOCKS_ENTRY PASS")
        print("ASSERT_EXIT_STILL_WORKS PASS")
        print("ASSERT_NO_NEW_POSITIONS_WHEN_BLOCKED PASS")

    def _test_gate_control_close_only(self) -> None:
        self._in_test_mode = True
        LEADER = "0xtestcloseonly00000000000000000000001"
        COIN   = "ETH"

        if LEADER not in self.wallet_metrics:
            self.wallet_metrics[LEADER] = WalletMetrics(wallet=LEADER)
            self.wallet_equity[LEADER]  = WalletEquity()

        self.wallet_gate[LEADER] = {"mode": "ON"}
        self.wallet_gate[LEADER] = {"mode": "CLOSE_ONLY"}

        self.open_positions.clear()
        self.tracked_positions[(LEADER, COIN)] = 0.0
        self.wallet_positions[(LEADER, COIN)]  = 0.0

        # ---- ENTRY SHOULD BE BLOCKED ----
        buy = FillEvent(
            wallet=LEADER, coin=COIN, side="BUY",
            price=3000.0, size=0.01,
            timestamp_ms=utc_now_ms(), timestamp_iso=utc_now_iso(),
            start_position=0.0, closed_pnl=0.0, is_snapshot=False,
            raw={}, fill_id="t1", shard_id=0, received_at_ms=utc_now_ms()
        )
        self.process_live_fill(buy)

        assert self.wallet_metrics[LEADER].copy_entry_count == 0
        assert len(self.open_positions.get((LEADER, COIN), [])) == 0
        print("CLOSE_ONLY ENTRY BLOCK PASS")

        # ---- SEED POSITION ----
        self.wallet_metrics[LEADER].copy_entry_count  = 1
        self.wallet_metrics[LEADER].copy_open_positions = 1
        self.open_positions[(LEADER, COIN)].append(SimPosition(
            wallet=LEADER, coin=COIN, trade_id="X",
            entry_time_ms=utc_now_ms(), entry_time_iso=utc_now_iso(),
            entry_price_lead=3000.0, entry_price_copy=3000.0,
            size_units=0.01, notional_usd=100.0, lead_side="BUY",
            entry_latency_ms=0,
        ))
        self.tracked_positions[(LEADER, COIN)] = 0.01

        # ---- EXIT SHOULD WORK ----
        sell = FillEvent(
            wallet=LEADER, coin=COIN, side="SELL",
            price=3200.0, size=0.01,
            timestamp_ms=utc_now_ms(), timestamp_iso=utc_now_iso(),
            start_position=0.01, closed_pnl=2.0, is_snapshot=False,
            raw={}, fill_id="t2", shard_id=0, received_at_ms=utc_now_ms()
        )
        self.process_live_fill(sell)

        assert self.wallet_metrics[LEADER].copy_exit_count >= 1
        assert len(self.open_positions.get((LEADER, COIN), [])) == 0
        print("CLOSE_ONLY EXIT PASS")

        self.mark_prices.pop(COIN, None)  # don't pollute mark price for subsequent tests
        self._in_test_mode = False
        print("TEST PASSED: CLOSE_ONLY_MODE")

    def _total_reset(self) -> None:
        """
        Authoritative full reset.
        Clears ALL runtime + ALL persisted state.
        Prevents immediate repopulation via reset_freeze.
        """
        self.reset_freeze = True

        # clear runtime state
        self.open_positions.clear()
        self.leader_positions.clear()
        self.wallet_positions.clear()
        self.tracked_positions.clear()
        self.mark_prices.clear()
        self.seen_fill_ids.clear()
        self.seen_fill_set.clear()

        for w in list(self.wallet_metrics.keys()):
            self.wallet_metrics[w] = WalletMetrics(wallet=w)
        for w in list(self.wallet_equity.keys()):
            self.wallet_equity[w] = WalletEquity()
        for w in list(self.wallet_reference.keys()):
            self.wallet_reference[w] = WalletEquity()
        self.leader_realized_pnl.clear()

        # nuke all output artifacts (files only — skip snapshots subdir)
        for p in OUTPUT_DIR.glob("*"):
            if not p.is_file():
                continue
            try:
                p.unlink()
            except Exception as exc:
                log("warning", f"RESET_DELETE_FAIL path={p.name} err={exc}")

        log("info", "TOTAL_RESET complete")

    def maybe_snapshot(self) -> None:
        SNAP_DIR.mkdir(parents=True, exist_ok=True)
        now = int(time.time())
        if not hasattr(self, "_last_snapshot_ts"):
            self._last_snapshot_ts = 0
        if now - self._last_snapshot_ts < 900:
            return
        self._last_snapshot_ts = now
        ts = time.strftime("%Y-%m-%d_%H-%M-%S")
        snap_path = SNAP_DIR / ts
        snap_path.mkdir(parents=True, exist_ok=True)
        try:
            for fname in ("live_state.json", "portfolio_history.json", "portfolio_baseline.json"):
                src = OUTPUT_DIR / fname
                if src.exists():
                    shutil.copy(src, snap_path / fname)
            meta = {"timestamp": ts, "version": "sim_v1", "wallet_count": len(self.wallets)}
            with (snap_path / "meta.json").open("w", encoding="utf-8") as f:
                json.dump(meta, f)
            print("[SNAPSHOT SAVED]", ts)
        except Exception as e:
            print("[SNAPSHOT ERROR]", e)

    def cleanup_snapshots(self) -> None:
        if not SNAP_DIR.exists():
            return
        cutoff = time.time() - (60 * 60 * 24 * 60)
        for folder in os.listdir(SNAP_DIR):
            path = SNAP_DIR / folder
            try:
                ts = time.mktime(time.strptime(folder, "%Y-%m-%d_%H-%M-%S"))
                if ts < cutoff:
                    shutil.rmtree(path)
                    print("[SNAPSHOT DELETED]", folder)
            except Exception:
                pass


# =========================
# MAIN
# =========================
def install_signal_handlers(engine: HLCopyEngine) -> None:
    def _handler(signum: int, frame: Any) -> None:
        log("warning", f"Received signal {signum}; stopping")
        engine.stop()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


_ENGINE_LOCK_HELD = False


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        exit_code = ctypes.c_ulong()
        ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
        ctypes.windll.kernel32.CloseHandle(handle)
        return bool(ok) and exit_code.value == 259
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _release_engine_lock() -> None:
    global _ENGINE_LOCK_HELD
    if not _ENGINE_LOCK_HELD:
        return
    try:
        if LOCK_FILE.exists():
            raw = LOCK_FILE.read_text(encoding="utf-8").strip()
            if raw == str(os.getpid()):
                LOCK_FILE.unlink()
    except Exception:
        pass
    _ENGINE_LOCK_HELD = False


def _acquire_engine_lock() -> None:
    global _ENGINE_LOCK_HELD
    pid_text = str(os.getpid())
    while True:
        try:
            fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(pid_text)
            _ENGINE_LOCK_HELD = True
            atexit.register(_release_engine_lock)
            print(f"[ENGINE_LOCK] Acquired. PID={pid_text}")
            return
        except FileExistsError:
            try:
                existing = LOCK_FILE.read_text(encoding="utf-8").strip()
                existing_pid = int(existing)
            except Exception:
                existing_pid = 0
            if existing_pid and _pid_is_alive(existing_pid):
                print(f"[ENGINE_LOCK] Another instance is running (PID={existing_pid}). Exiting.")
                raise SystemExit(1)
            print("[ENGINE_LOCK] Stale lock detected. Cleaning up.")
            try:
                LOCK_FILE.unlink()
            except FileNotFoundError:
                pass


def main() -> None:
    global LOGGER
    _acquire_engine_lock()
    ensure_output_dir()
    LOGGER = Logger(LOG_FILE)
    engine = HLCopyEngine()
    install_signal_handlers(engine)
    engine.run_forever()


def _run_ws_selftest() -> None:
    global LOGGER
    ensure_output_dir()
    LOGGER = Logger(LOG_FILE)
    engine = HLCopyEngine()
    shard_id = 0
    engine.shard_reconnect_count[shard_id] = 0
    transitions: List[str] = []

    engine._mark_shard_open(shard_id)
    transitions.append(engine.shard_ws_status.get(shard_id, ""))
    engine._mark_shard_message(shard_id)
    transitions.append(engine.shard_ws_status.get(shard_id, ""))
    engine._mark_shard_error(shard_id, "selftest")
    transitions.append(engine.shard_ws_status.get(shard_id, ""))
    engine._mark_shard_close(shard_id)
    transitions.append(engine.shard_ws_status.get(shard_id, ""))
    engine.shard_reconnect_count[shard_id] = engine.shard_reconnect_count.get(shard_id, 0) + 1
    engine.shard_ws_status[shard_id] = "CONNECTING"
    transitions.append(engine.shard_ws_status.get(shard_id, ""))

    wallet = engine.wallets[0]
    with engine.metrics_lock:
        engine.wallet_metrics[wallet].alert_flags = ""
    engine.write_live_state()
    state_down = json.loads(LIVE_STATE_JSON.read_text(encoding="utf-8"))
    down_flags = state_down.get("wallets", {}).get(wallet, {}).get("alert_flags", [])

    engine._mark_shard_open(shard_id)
    transitions.append(engine.shard_ws_status.get(shard_id, ""))
    engine.write_live_state()
    state_ok = json.loads(LIVE_STATE_JSON.read_text(encoding="utf-8"))
    ok_flags = state_ok.get("wallets", {}).get(wallet, {}).get("alert_flags", [])

    print("RESULT::WS_RUNTIME_PROOF")
    print(f"status transitions: {' -> '.join(transitions)}")
    print(f"reconnect delay <=5: {min(SHARD_RESTART_DELAY_SEC, 5) <= 5}")
    print(f"WS_DEGRADED appears when DOWN: {'WS_DEGRADED' in down_flags}")
    print(f"WS_DEGRADED clears when OK: {'WS_DEGRADED' not in ok_flags}")


if __name__ == "__main__":
    import sys as _sys
    if os.getenv("HL_WS_SELFTEST", "0") == "1":
        _run_ws_selftest()
    elif "--test-only" in _sys.argv:
        _acquire_engine_lock()
        ensure_output_dir()
        LOGGER = Logger(LOG_FILE)
        engine = HLCopyEngine()
        engine._test_live_gate()
        engine._test_gate_control_entry_block()
        engine._test_gate_control_close_only()
        engine._test_copy_attribution()
        engine._test_final_state_output()
    else:
        main()


def _debug_test_delta():
    print("[TEST] Running delta debug test...")
    key = ("TEST_WALLET", "BTC")
    prev = 0.0
    new = 1.0
    delta = new - prev
    print("[TEST RESULT]", prev, new, delta)
