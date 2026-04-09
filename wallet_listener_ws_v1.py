#!/usr/bin/env python3
# wallet_listener_ws_v1.py

from __future__ import annotations

import csv
import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import websocket  # websocket-client
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: websocket-client\n"
        "Install with:\n"
        "  pip install websocket-client"
    ) from exc


# ============================================================
# CONFIG
# ============================================================

getcontext().prec = 28

WS_URL = "wss://api.hyperliquid.xyz/ws"
INFO_URL = "https://api.hyperliquid.xyz/info"
WALLET_SOURCE_CSV = Path("hl_stage2") / "copyable_wallets.csv"

OUTPUT_DIR = Path("hyperliquid_live_output")
RAW_TRADES_CSV = OUTPUT_DIR / "raw_trades.csv"
LIVE_METRICS_CSV = OUTPUT_DIR / "live_wallet_metrics.csv"
LOG_FILE = OUTPUT_DIR / "wallet_listener_ws_v1.log"
EQUITY_CURVES_DIR = OUTPUT_DIR / "equity_curves"
CURSOR_FILE = OUTPUT_DIR / "poll_cursors.json"

EXECUTION_PROFILES = {
    "ideal": {"slippage_bps": 2, "latency_penalty_bps": 0},
    "good": {"slippage_bps": 5, "latency_penalty_bps": 2},
    "realistic": {"slippage_bps": 10, "latency_penalty_bps": 5},
    "bad": {"slippage_bps": 20, "latency_penalty_bps": 15},
}

COPY_SLIPPAGE_BPS = Decimal("5")
COPY_FEE_BPS = Decimal("4.5")
COPY_SIZE_MULTIPLIER = Decimal("1")

RECV_TIMEOUT_SECONDS = 15
PING_INTERVAL_SECONDS = 20
RECONNECT_INITIAL_SECONDS = 2
RECONNECT_MAX_SECONDS = 30
METRICS_FLUSH_INTERVAL_SECONDS = 5
POLL_INTERVAL_SECONDS = 3600
RANKING_REFRESH_INTERVAL_SECONDS = 60
UNIVERSE_RELOAD_INTERVAL_SECONDS = 3600
POLL_OVERLAP_MS = 120_000
INITIAL_POLL_LOOKBACK_MS = 86_400_000  # 24 hours
POLL_HTTP_TIMEOUT_SECONDS = 20
POLL_MAX_WORKERS = 32
VERBOSE_LOG_EVERY_N_MESSAGES = 100
TOP_WS_WALLET_LIMIT = 10
TOP_WS_USERS_PER_CONNECTION = 9

PROCESS_SNAPSHOT_FILLS = False


# ============================================================
# LOGGING
# ============================================================

def setup_logging() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
    )

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


# ============================================================
# HELPERS
# ============================================================

def d(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_iso_from_ms(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000.0)) + f".{ms % 1000:03d}Z"


def is_valid_wallet(addr: str) -> bool:
    if not isinstance(addr, str):
        return False
    addr = addr.strip()
    return len(addr) == 42 and addr.startswith("0x")


def normalize_wallets(wallets: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    seen = set()
    for w in wallets:
        w2 = str(w).strip().lower()
        if not w2:
            continue
        if not is_valid_wallet(w2):
            raise ValueError(f"Invalid wallet address: {w}")
        if w2 not in seen:
            cleaned.append(w2)
            seen.add(w2)
    return cleaned


def normalize_side(side: Any) -> str:
    s = str(side).strip().lower()
    if s in {"b", "buy", "bid", "long"}:
        return "BUY"
    if s in {"a", "sell", "ask", "short"}:
        return "SELL"
    return str(side).strip().upper()


def signed_qty_for_side(side: str, qty: Decimal) -> Decimal:
    s = normalize_side(side)
    if s == "BUY":
        return qty
    if s == "SELL":
        return -qty
    raise ValueError(f"Unknown side: {side}")


def build_event_id(wallet: str, fill: Dict[str, Any]) -> str:
    wallet_l = wallet.lower()
    tid = str(fill.get("tid", ""))
    tx_hash = str(fill.get("hash", ""))
    oid = str(fill.get("oid", ""))
    coin = str(fill.get("coin", ""))
    fill_time = str(fill.get("time", ""))
    side = str(fill.get("side", ""))
    px = str(fill.get("px", ""))
    sz = str(fill.get("sz", ""))
    return "|".join([wallet_l, tid, tx_hash, oid, coin, fill_time, side, px, sz])


def chunk_wallets(wallets: List[str], size: int) -> List[List[str]]:
    return [wallets[i:i + size] for i in range(0, len(wallets), size)]


def source_from_tag(tag: str) -> str:
    if tag == "poll" or tag.startswith("poll"):
        return "poll"
    if tag.startswith("ws-"):
        return "websocket"
    return "unknown"


def load_wallets_from_csv(path: Path) -> Tuple[List[str], Dict[str, Decimal]]:
    if not path.exists():
        raise FileNotFoundError(f"Wallet source CSV not found: {path}")

    wallets: List[str] = []
    seed_scores: Dict[str, Decimal] = {}

    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "wallet" not in reader.fieldnames:
            raise ValueError(f"CSV missing required 'wallet' column: {path}")

        for row in reader:
            wallet = str(row.get("wallet", "")).strip().lower()
            if not wallet:
                continue
            if not is_valid_wallet(wallet):
                raise ValueError(f"Invalid wallet address in {path}: {wallet}")
            if wallet in seed_scores:
                continue
            wallets.append(wallet)
            seed_scores[wallet] = d(row.get("score"), "0")

    return normalize_wallets(wallets), seed_scores


def post_info(payload: Dict[str, Any]) -> Any:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        INFO_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=POLL_HTTP_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


@dataclass
class PositionState:
    qty: Decimal = Decimal("0")
    avg_entry_px: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    trade_count: int = 0
    wins: int = 0
    losses: int = 0
    last_fill_ts_ms: int = 0
    last_recv_ts_ms: int = 0
    last_exec_px: Decimal = Decimal("0")


@dataclass
class WalletStats:
    wallet: str
    trade_count: int = 0
    unique_event_count: int = 0
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    total_pnl: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    avg_latency_ms: Decimal = Decimal("0")
    max_latency_ms: int = 0
    latency_sum_ms: int = 0
    latency_count: int = 0
    last_fill_ts_ms: int = 0
    last_recv_ts_ms: int = 0
    wins: int = 0
    losses: int = 0
    open_position_count: int = 0
    gross_open_notional: Decimal = Decimal("0")
    net_open_notional: Decimal = Decimal("0")
    slippage_cost_total: Decimal = Decimal("0")
    latency_bucket_lt_100ms: int = 0
    latency_bucket_100_300ms: int = 0
    latency_bucket_300_1000ms: int = 0
    latency_bucket_1_3s: int = 0
    latency_bucket_gt_3s: int = 0
    polled_fill_count: int = 0
    websocket_fill_count: int = 0
    polling_realized_pnl: Decimal = Decimal("0")
    websocket_realized_pnl: Decimal = Decimal("0")
    last_fill_source: str = ""
    profile_realized_pnl: Dict[str, Decimal] = field(
        default_factory=lambda: {profile: Decimal("0") for profile in EXECUTION_PROFILES}
    )
    profile_trade_count: Dict[str, int] = field(
        default_factory=lambda: {profile: 0 for profile in EXECUTION_PROFILES}
    )
    total_notional: Decimal = Decimal("0")


class WalletListenerEngine:

    def _load_poll_cursors(self) -> Dict[str, int]:
        if CURSOR_FILE.exists():
            try:
                with open(CURSOR_FILE, "r") as f:
                    data = json.load(f)
                    return {k: int(v) for k, v in data.items()}
            except Exception:
                logging.warning("Failed to load poll cursors — starting fresh")

        return {
            wallet: max(0, now_ms() - INITIAL_POLL_LOOKBACK_MS)
            for wallet in self.wallets
        }

    def _save_poll_cursors(self) -> None:
        try:
            tmp_path = CURSOR_FILE.with_suffix(".tmp")
            with open(tmp_path, "w") as f:
                json.dump(self.poll_cursor_end_ms, f)
            os.replace(tmp_path, CURSOR_FILE)
        except Exception as e:
            logging.warning(f"Failed to save poll cursors: {e}")

    RAW_HEADERS = [
        "event_id",
        "wallet",
        "coin",
        "side",
        "dir",
        "px",
        "sz",
        "observed_ts_ms",
        "observed_ts_iso",
        "received_ts_ms",
        "received_ts_iso",
        "latency_ms",
        "hash",
        "oid",
        "tid",
        "crossed",
        "wallet_fee",
        "fee_token",
        "builder_fee",
        "closed_pnl",
        "start_position",
        "is_snapshot",
        "fill_source",
        "copy_side",
        "copy_exec_px",
        "copy_sz",
        "copy_notional",
        "copy_fee",
        "copy_realized_pnl_delta",
    ]

    METRIC_HEADERS = [
        "wallet",
        "trade_count",
        "unique_event_count",
        "realized_pnl",
        "slippage_cost_total",
        "unrealized_pnl",
        "total_pnl",
        "total_fees",
        "avg_latency_ms",
        "max_latency_ms",
        "last_fill_ts_ms",
        "last_fill_ts_iso",
        "last_recv_ts_ms",
        "last_recv_ts_iso",
        "open_position_count",
        "gross_open_notional",
        "net_open_notional",
        "wins",
        "losses",
        "latency_bucket_lt_100ms",
        "latency_bucket_100_300ms",
        "latency_bucket_300_1000ms",
        "latency_bucket_1_3s",
        "latency_bucket_gt_3s",
        "polled_fill_count",
        "websocket_fill_count",
        "last_fill_source",
        "polling_realized_pnl",
        "websocket_realized_pnl",
        "ideal_pnl",
        "realistic_pnl",
        "bad_pnl",
        "total_notional",
        "efficiency",
        "positions_json",
    ]

    def __init__(self, wallets: List[str], seed_scores: Dict[str, Decimal]) -> None:
        self.wallets = normalize_wallets(wallets)
        if not self.wallets:
            raise ValueError("No wallets loaded from hl_stage2/copyable_wallets.csv")

        self.seed_scores = {w.lower(): d(v, "0") for w, v in seed_scores.items()}

        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self._raw_csv_lock = threading.Lock()
        self._metrics_csv_lock = threading.Lock()
        self._equity_curve_lock = threading.Lock()
        self._top_ws_rotate_event = threading.Event()
        self._active_top_wallets_by_worker: Dict[str, List[str]] = {}

        self.processed_event_ids: set[str] = set()
        self.positions: Dict[str, Dict[str, PositionState]] = defaultdict(lambda: defaultdict(PositionState))
        self.profile_positions: Dict[str, Dict[str, Dict[str, PositionState]]] = defaultdict(
            lambda: {profile: defaultdict(PositionState) for profile in EXECUTION_PROFILES}
        )
        self.profile_equity_curves: Dict[str, Dict[str, List[Tuple[int, Decimal]]]] = defaultdict(
            lambda: {profile: [] for profile in EXECUTION_PROFILES}
        )
        self.wallet_stats: Dict[str, WalletStats] = {w: WalletStats(wallet=w) for w in self.wallets}
        self.latest_mids: Dict[str, Decimal] = {}
        self.rankings: List[str] = list(self.wallets)
        self.desired_top_wallets: List[str] = []
        self.active_top_wallets: List[str] = []
        self.poll_cursor_end_ms: Dict[str, int] = {}
        self.poll_cursor_end_ms = self._load_poll_cursors()

        self._raw_file_handle = None
        self._raw_writer: Optional[csv.DictWriter] = None
        self._metrics_file_handle = None
        self._threads: List[threading.Thread] = []

        self.total_ignored_snapshots: int = 0
        self.total_duplicates_skipped: int = 0
        self.total_reconnects: int = 0
        self.total_poll_cycles: int = 0
        self.total_poll_requests: int = 0

    def start(self) -> None:
        logging.info("Starting wallet listener")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        EQUITY_CURVES_DIR.mkdir(parents=True, exist_ok=True)

        self._open_raw_csv()
        self._open_metrics_csv()
        self._load_existing_raw_state()

        logging.info("Bootstrapping positions from historical trades...")
        self._bootstrap_positions_from_history()

        with self.lock:
            self._refresh_rankings_locked(force_log=True)

        self._start_thread(self._metrics_flush_loop, "metrics-flush")
        self._start_thread(self._poll_loop, "poll-loop")
        self._start_thread(self._ranking_loop, "ranking-loop")
        self._start_thread(self._universe_reload_loop, "universe-reload")
        self._start_thread(self._mids_ws_loop, "ws-mids")
        for chunk_idx in range(max(1, (TOP_WS_WALLET_LIMIT + TOP_WS_USERS_PER_CONNECTION - 1) // TOP_WS_USERS_PER_CONNECTION)):
            self._start_thread(
                lambda idx=chunk_idx: self._top_wallet_ws_loop(idx),
                f"ws-top-wallets-{chunk_idx}",
            )

        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt in main thread -> stopping")
            self.stop()

        logging.info("Stop event set; waiting for threads...")
        for t in self._threads:
            t.join(timeout=10)

        self._flush_metrics()
        self._close_metrics_csv()
        self._close_raw_csv()
        logging.info("Stopped wallet listener")
        logging.shutdown()

    def _start_thread(self, target: Any, name: str) -> None:
        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        self._threads.append(thread)

    def stop(self) -> None:
        self.stop_event.set()
        self._top_ws_rotate_event.set()

    def _open_raw_csv(self) -> None:
        self._upgrade_raw_csv_if_needed()
        file_exists = RAW_TRADES_CSV.exists()
        self._raw_file_handle = open(RAW_TRADES_CSV, "a", newline="", encoding="utf-8")
        self._raw_writer = csv.DictWriter(self._raw_file_handle, fieldnames=self.RAW_HEADERS)
        if not file_exists or RAW_TRADES_CSV.stat().st_size == 0:
            self._raw_writer.writeheader()
            self._raw_file_handle.flush()
            os.fsync(self._raw_file_handle.fileno())

    def _upgrade_raw_csv_if_needed(self) -> None:
        if not RAW_TRADES_CSV.exists() or RAW_TRADES_CSV.stat().st_size == 0:
            return

        with open(RAW_TRADES_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            existing_headers = next(reader, None)

        if not existing_headers:
            return
        if "fill_source" in existing_headers:
            return

        logging.info("Upgrading raw_trades.csv schema to include fill_source")
        with open(RAW_TRADES_CSV, "r", newline="", encoding="utf-8") as src:
            reader = csv.DictReader(src)
            rows = list(reader)

        with open(RAW_TRADES_CSV, "w", newline="", encoding="utf-8") as dst:
            writer = csv.DictWriter(dst, fieldnames=self.RAW_HEADERS)
            writer.writeheader()
            for row in rows:
                upgraded_row = {header: row.get(header, "") for header in self.RAW_HEADERS}
                upgraded_row["fill_source"] = row.get("fill_source", "")
                writer.writerow(upgraded_row)
            dst.flush()
            os.fsync(dst.fileno())

    def _close_raw_csv(self) -> None:
        if self._raw_file_handle:
            try:
                self._raw_file_handle.flush()
                os.fsync(self._raw_file_handle.fileno())
            except OSError:
                pass
            self._raw_file_handle.close()
            self._raw_file_handle = None
            self._raw_writer = None

    def _open_metrics_csv(self) -> None:
        LIVE_METRICS_CSV.parent.mkdir(parents=True, exist_ok=True)
        self._metrics_file_handle = open(LIVE_METRICS_CSV, "w+", newline="", encoding="utf-8")

    def _close_metrics_csv(self) -> None:
        if self._metrics_file_handle:
            try:
                self._metrics_file_handle.flush()
                os.fsync(self._metrics_file_handle.fileno())
            except OSError:
                pass
            self._metrics_file_handle.close()
            self._metrics_file_handle = None

    def _load_existing_raw_state(self) -> None:
        if not RAW_TRADES_CSV.exists() or RAW_TRADES_CSV.stat().st_size == 0:
            logging.info("No existing raw_trades.csv found; starting clean")
            return

        logging.info("Loading existing raw_trades.csv for dedupe + state replay")
        loaded = 0
        replayed = 0

        with open(RAW_TRADES_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                event_id = (row.get("event_id") or "").strip()
                wallet = (row.get("wallet") or "").strip().lower()
                if not event_id or wallet not in self.wallet_stats:
                    continue

                self.processed_event_ids.add(event_id)
                loaded += 1

                latency_ms = int(d(row.get("latency_ms"), "0"))
                observed_ts_ms = int(d(row.get("observed_ts_ms"), "0"))
                received_ts_ms = int(d(row.get("received_ts_ms"), "0"))

                stats = self.wallet_stats[wallet]
                stats.unique_event_count += 1
                stats.trade_count += 1
                stats.latency_sum_ms += latency_ms
                stats.latency_count += 1
                stats.max_latency_ms = max(stats.max_latency_ms, latency_ms)
                stats.avg_latency_ms = (
                    Decimal(stats.latency_sum_ms) / Decimal(stats.latency_count)
                    if stats.latency_count > 0 else Decimal("0")
                )
                stats.last_fill_ts_ms = max(stats.last_fill_ts_ms, observed_ts_ms)
                stats.last_recv_ts_ms = max(stats.last_recv_ts_ms, received_ts_ms)

                coin = row.get("coin", "")
                copy_side = row.get("copy_side", "")
                copy_exec_px = d(row.get("copy_exec_px"), "0")
                copy_sz = d(row.get("copy_sz"), "0")
                copy_fee = d(row.get("copy_fee"), "0")
                fill_source = (row.get("fill_source") or "poll").strip().lower()
                wallet_px = d(row.get("px"), "0")
                wallet_sz = d(row.get("sz"), "0")
                slippage_cost = Decimal("0")
                if wallet_px > 0 and wallet_sz > 0 and copy_exec_px > 0:
                    slippage_cost = (copy_exec_px - wallet_px) * signed_qty_for_side(copy_side, wallet_sz)

                if coin and copy_side and copy_exec_px > 0 and copy_sz > 0:
                    realized_delta = self._apply_copy_fill(
                        wallet=wallet,
                        coin=coin,
                        side=copy_side,
                        exec_px=copy_exec_px,
                        qty=copy_sz,
                        fee=copy_fee,
                        observed_ts_ms=observed_ts_ms,
                        recv_ts_ms=received_ts_ms,
                    )
                    if realized_delta > 0:
                        self.wallet_stats[wallet].wins += 1
                    elif realized_delta < 0:
                        self.wallet_stats[wallet].losses += 1
                    if fill_source == "poll":
                        self.wallet_stats[wallet].polled_fill_count += 1
                        self.wallet_stats[wallet].polling_realized_pnl += realized_delta
                    elif fill_source == "websocket":
                        self.wallet_stats[wallet].websocket_fill_count += 1
                        self.wallet_stats[wallet].websocket_realized_pnl += realized_delta
                    self.wallet_stats[wallet].last_fill_source = fill_source
                    self.wallet_stats[wallet].slippage_cost_total += slippage_cost
                    for profile_name in EXECUTION_PROFILES:
                        profile_exec_px = self._profile_exec_px(
                            side=copy_side,
                            wallet_fill_px=wallet_px,
                            latency_ms=latency_ms,
                            fill_source=fill_source,
                            profile_name=profile_name,
                        )
                        profile_fee = (profile_exec_px * copy_sz) * (COPY_FEE_BPS / Decimal("10000"))
                        self._apply_profile_copy_fill(
                            wallet=wallet,
                            profile_name=profile_name,
                            coin=coin,
                            side=copy_side,
                            exec_px=profile_exec_px,
                            qty=copy_sz,
                            fee=profile_fee,
                            observed_ts_ms=observed_ts_ms,
                            recv_ts_ms=received_ts_ms,
                        )
                        self._record_profile_equity_point(
                            wallet=wallet,
                            profile_name=profile_name,
                            ts_ms=observed_ts_ms or received_ts_ms,
                            equity=self.wallet_stats[wallet].profile_realized_pnl[profile_name],
                            persist=True,
                        )
                    replayed += 1

        with self.lock:
            self._recompute_unrealized_locked()
            self._recompute_wallet_aggregates_locked()
            self._refresh_rankings_locked(force_log=True)
        self._rewrite_equity_curve_files()

        logging.info(
            "Replay complete | loaded_events=%s | replayed_copy_fills=%s | dedupe_set=%s",
            loaded,
            replayed,
            len(self.processed_event_ids),
        )

    def _bootstrap_positions_from_history(self) -> None:
        history_file = Path("hl_stage2") / "all_trades.csv"
        if not history_file.exists():
            logging.warning("Bootstrap: history file not found at %s — skipping", history_file)
            return

        logging.info("Bootstrap: loading %s", history_file)
        wallet_set = set(self.wallets)
        position_count = 0
        row_count = 0

        try:
            with open(history_file, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for raw_row in reader:
                    try:
                        wallet = str(raw_row.get("wallet", "")).strip().lower()
                        if not wallet or wallet not in wallet_set:
                            continue

                        coin = str(raw_row.get("coin", "")).strip()
                        if not coin:
                            continue

                        side = normalize_side(raw_row.get("side", ""))
                        if side not in {"BUY", "SELL"}:
                            continue

                        px = d(raw_row.get("px"), "0")
                        sz = d(raw_row.get("sz"), "0")
                        if px <= 0 or sz <= 0:
                            continue

                        qty = signed_qty_for_side(side, sz)
                        pos = self.positions[wallet][coin]

                        if pos.qty == 0:
                            pos.qty = qty
                            pos.avg_entry_px = px
                        elif (pos.qty > 0 and qty > 0) or (pos.qty < 0 and qty < 0):
                            new_qty = pos.qty + qty
                            pos.avg_entry_px = (
                                (pos.avg_entry_px * abs(pos.qty) + px * abs(qty))
                                / abs(new_qty)
                            )
                            pos.qty = new_qty
                        else:
                            closing_qty = min(abs(qty), abs(pos.qty))
                            realized = (px - pos.avg_entry_px) * closing_qty * (Decimal("1") if pos.qty > 0 else Decimal("-1"))
                            pos.realized_pnl += realized
                            pos.qty += qty
                            if pos.qty == 0:
                                pos.avg_entry_px = Decimal("0")

                        row_count += 1
                    except Exception as exc:
                        logging.debug("Bootstrap: skipping bad row: %s | error=%s", raw_row, exc)
                        continue
        except Exception as exc:
            logging.warning("Bootstrap: failed to read history file: %s", exc)
            return

        for wallet in wallet_set:
            for coin, pos in self.positions[wallet].items():
                if pos.qty != 0:
                    position_count += 1

        logging.info(
            "Bootstrap complete | rows_processed=%s | open_positions=%s",
            row_count,
            position_count,
        )

    def _poll_loop(self) -> None:
        logging.info(
            "Polling loop started | wallets=%s interval=%ss max_workers=%s",
            len(self.wallets),
            POLL_INTERVAL_SECONDS,
            min(POLL_MAX_WORKERS, len(self.wallets)),
        )

        while not self.stop_event.is_set():
            cycle_started_at = time.time()
            cycle_end_ms = now_ms()

            with self.lock:
                poll_windows = {
                    wallet: max(
                        0,
                        self.poll_cursor_end_ms.get(wallet, cycle_end_ms - INITIAL_POLL_LOOKBACK_MS)
                        - POLL_OVERLAP_MS
                    )
                    for wallet in self.wallets
                }

            futures = {}
            max_workers = max(1, min(POLL_MAX_WORKERS, len(self.wallets)))
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="poll-wallet") as executor:
                for wallet, start_ms in poll_windows.items():
                    futures[executor.submit(self._poll_wallet_window, wallet, start_ms, cycle_end_ms)] = wallet

                for future in as_completed(futures):
                    wallet = futures[future]
                    try:
                        request_count, fill_count = future.result()
                        if request_count > 0 or fill_count > 0:
                            logging.info(
                                "[poll] wallet=%s requests=%s fills=%s start=%s end=%s",
                                wallet,
                                request_count,
                                fill_count,
                                poll_windows[wallet],
                                cycle_end_ms,
                            )
                    except Exception as exc:
                        logging.exception("[poll] wallet=%s failed: %s", wallet, exc)

            with self.lock:
                self.total_poll_cycles += 1

            elapsed = time.time() - cycle_started_at
            sleep_for = max(0.5, POLL_INTERVAL_SECONDS - elapsed)
            self.stop_event.wait(sleep_for)

        logging.info("Polling loop stopped")

    def _ranking_loop(self) -> None:
        logging.info("Ranking loop started | interval=%ss", RANKING_REFRESH_INTERVAL_SECONDS)
        while not self.stop_event.is_set():
            with self.lock:
                self._recompute_unrealized_locked()
                self._recompute_wallet_aggregates_locked(refresh_rankings=False)
                self._refresh_rankings_locked()
                self._log_top_rankings_locked()
            self.stop_event.wait(RANKING_REFRESH_INTERVAL_SECONDS)
        logging.info("Ranking loop stopped")

    def _universe_reload_loop(self) -> None:
        logging.info("Universe reload loop started | interval=%ss", UNIVERSE_RELOAD_INTERVAL_SECONDS)
        while not self.stop_event.is_set():
            self.stop_event.wait(UNIVERSE_RELOAD_INTERVAL_SECONDS)
            if self.stop_event.is_set():
                break
            try:
                added = self._reload_wallet_universe()
                logging.info("Universe reload complete | added_wallets=%s total_wallets=%s", added, len(self.wallets))
            except Exception as exc:
                logging.exception("Universe reload error: %s", exc)
        logging.info("Universe reload loop stopped")

    def _poll_wallet_window(self, wallet: str, start_ms: int, end_ms: int) -> Tuple[int, int]:
        request_count = 0
        fill_count = 0
        page_start_ms = start_ms
        last_page_signature: Optional[Tuple[int, str]] = None

        while not self.stop_event.is_set() and page_start_ms <= end_ms:
            fills = self._fetch_user_fills_by_time(wallet, page_start_ms, end_ms)
            request_count += 1
            with self.lock:
                self.total_poll_requests += 1

            if not fills:
                break

            fills = sorted(
                fills,
                key=lambda item: (
                    int(d(item.get("time"), "0")),
                    str(item.get("oid", "")),
                    str(item.get("tid", "")),
                ),
            )

            for fill in fills:
                self._process_fill(
                    wallet=wallet,
                    fill=fill,
                    recv_ts_ms=now_ms(),
                    is_snapshot=False,
                    tag="poll",
                )
                fill_count += 1

            if len(fills) < 2000:
                break

            last_fill_time = int(d(fills[-1].get("time"), "0"))
            last_fill_event_id = build_event_id(wallet, fills[-1])
            page_signature = (last_fill_time, last_fill_event_id)
            if page_signature == last_page_signature:
                logging.warning(
                    "[poll] pagination stalled | wallet=%s start=%s end=%s last_fill_time=%s",
                    wallet,
                    page_start_ms,
                    end_ms,
                    last_fill_time,
                )
                break
            last_page_signature = page_signature
            page_start_ms = max(page_start_ms, last_fill_time)

        with self.lock:
            self.poll_cursor_end_ms[wallet] = max(self.poll_cursor_end_ms.get(wallet, 0), end_ms)
        self._save_poll_cursors()

        return request_count, fill_count

    def _fetch_user_fills_by_time(self, wallet: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        payload = {
            "type": "userFillsByTime",
            "user": wallet,
            "startTime": int(start_ms),
            "endTime": int(end_ms),
            "aggregateByTime": False,
        }
        try:
            data = post_info(payload)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error: {exc}") from exc

        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict) and "response" in data and isinstance(data["response"], list):
            return [item for item in data["response"] if isinstance(item, dict)]

        raise RuntimeError(f"Unexpected poll response for wallet={wallet}: {data}")

    def _mids_ws_loop(self) -> None:
        self._generic_ws_loop(
            worker_name="mids",
            subscribe_fn=self._subscribe_mids,
            rotate_event=None,
            wallets_provider=None,
        )

    def _top_wallet_ws_loop(self, chunk_idx: int) -> None:
        self._generic_ws_loop(
            worker_name=f"top-{chunk_idx}",
            subscribe_fn=self._subscribe_top_wallets,
            rotate_event=self._top_ws_rotate_event,
            wallets_provider=lambda: self._get_desired_top_wallet_chunk(chunk_idx),
        )

    def _generic_ws_loop(
        self,
        worker_name: str,
        subscribe_fn: Any,
        rotate_event: Optional[threading.Event],
        wallets_provider: Optional[Any],
    ) -> None:
        reconnect_delay = RECONNECT_INITIAL_SECONDS
        reconnect_count = 0

        while not self.stop_event.is_set():
            wallets = wallets_provider() if wallets_provider is not None else None
            if wallets_provider is not None and not wallets:
                self.stop_event.wait(1)
                continue

            try:
                self._run_ws_session(
                    worker_name=worker_name,
                    subscribe_fn=subscribe_fn,
                    wallets=wallets,
                    rotate_event=rotate_event,
                    wallets_provider=wallets_provider,
                )
                reconnect_delay = RECONNECT_INITIAL_SECONDS
            except Exception as exc:
                if self.stop_event.is_set():
                    break
                reconnect_count += 1
                with self.lock:
                    self.total_reconnects += 1
                logging.exception(
                    "[ws-%s] Worker exception: %s | reconnect_count=%s | retry_in=%ss",
                    worker_name,
                    exc,
                    reconnect_count,
                    reconnect_delay,
                )
                self.stop_event.wait(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_SECONDS)

        logging.info("[ws-%s] Worker stopped", worker_name)

    def _run_ws_session(
        self,
        worker_name: str,
        subscribe_fn: Any,
        wallets: Optional[List[str]],
        rotate_event: Optional[threading.Event],
        wallets_provider: Optional[Any],
    ) -> None:
        tag = f"ws-{worker_name}"
        logging.info("[%s] Connecting: %s", tag, WS_URL)

        ws = websocket.create_connection(
            WS_URL,
            timeout=RECV_TIMEOUT_SECONDS,
            enable_multithread=True,
        )

        try:
            subscribe_fn(ws, wallets)
            last_ping_ts = time.time()
            msg_count = 0

            if worker_name.startswith("top-"):
                with self.lock:
                    self._set_active_top_wallets_for_worker(worker_name, wallets or [])
                logging.info("[%s] Active top-wallet subscriptions: %s", tag, wallets)

            while not self.stop_event.is_set():
                if rotate_event is not None and rotate_event.is_set():
                    new_wallets = wallets_provider() if wallets_provider is not None else wallets
                    if list(new_wallets or []) != list(wallets or []):
                        logging.info("[%s] Top-wallet rotation requested -> reconnecting", tag)
                        rotate_event.clear()
                        return
                    rotate_event.clear()

                try:
                    raw_msg = ws.recv()
                except websocket.WebSocketTimeoutException:
                    if (time.time() - last_ping_ts) >= PING_INTERVAL_SECONDS:
                        logging.info("[%s] Recv timeout -> ping", tag)
                        try:
                            ws.ping()
                            last_ping_ts = time.time()
                        except Exception as ping_exc:
                            raise RuntimeError(f"[{tag}] Ping failed: {ping_exc}") from ping_exc
                    continue

                if raw_msg is None:
                    raise RuntimeError(f"[{tag}] Websocket returned None message")
                if raw_msg == "":
                    continue

                msg_count += 1
                self._handle_ws_message(raw_msg, tag=tag)

                if msg_count % VERBOSE_LOG_EVERY_N_MESSAGES == 0:
                    logging.info(
                        "[%s] messages=%s | unique_events=%s | duplicates_skipped=%s | reconnects=%s",
                        tag,
                        msg_count,
                        len(self.processed_event_ids),
                        self.total_duplicates_skipped,
                        self.total_reconnects,
                    )
        finally:
            if worker_name.startswith("top-"):
                with self.lock:
                    self._set_active_top_wallets_for_worker(worker_name, [])
            try:
                ws.close()
            except Exception:
                pass

    def _subscribe_mids(self, ws: websocket.WebSocket, wallets: Optional[List[str]]) -> None:
        del wallets
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
        logging.info("[ws-mids] Subscribed allMids")

    def _subscribe_top_wallets(self, ws: websocket.WebSocket, wallets: Optional[List[str]]) -> None:
        if not wallets:
            return

        if len(wallets) > TOP_WS_USERS_PER_CONNECTION:
            raise ValueError(
                f"Top-wallet websocket cannot subscribe more than {TOP_WS_USERS_PER_CONNECTION} wallets"
            )

        for wallet in wallets:
            ws.send(
                json.dumps(
                    {
                        "method": "subscribe",
                        "subscription": {
                            "type": "userFills",
                            "user": wallet,
                            "aggregateByTime": False,
                        },
                    }
                )
            )
            logging.info("[ws-top] Subscribed userFills | wallet=%s", wallet)

    def _handle_ws_message(self, raw_msg: str, tag: str = "unknown") -> None:
        recv_ts_ms = now_ms()

        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            logging.warning("[%s] Skipping non-JSON message: %s", tag, raw_msg[:500])
            return

        channel = msg.get("channel")
        data = msg.get("data")

        if channel == "subscriptionResponse":
            logging.info("[%s] Subscription ack: %s", tag, msg)
            return

        if channel == "allMids":
            self._handle_all_mids(data)
            return

        if channel == "userFills":
            self._handle_user_fills(data, recv_ts_ms=recv_ts_ms, tag=tag)
            return

        if channel == "error":
            logging.error("[%s] Websocket error channel received: %s", tag, msg)
            return

        if channel in {"pong", "ping"}:
            return

        logging.info("[%s] Unhandled websocket channel=%s msg=%s", tag, channel, msg)

    def _handle_all_mids(self, data: Any) -> None:
        if not isinstance(data, dict):
            return

        mids = data.get("mids", {})
        if not isinstance(mids, dict):
            return

        with self.lock:
            for coin, mid in mids.items():
                px = d(mid, "0")
                if px > 0:
                    self.latest_mids[str(coin)] = px
            self._recompute_unrealized_locked()
            self._recompute_wallet_aggregates_locked()

    def _handle_user_fills(self, data: Any, recv_ts_ms: int, tag: str = "unknown") -> None:
        if not isinstance(data, dict):
            return

        wallet = str(data.get("user", "")).strip().lower()
        if wallet not in self.wallet_stats:
            return

        is_snapshot = bool(data.get("isSnapshot", False))
        if is_snapshot and not PROCESS_SNAPSHOT_FILLS:
            fills_count = len(data.get("fills", []))
            with self.lock:
                self.total_ignored_snapshots += 1
                ignored_total = self.total_ignored_snapshots
            logging.info(
                "[%s] Snapshot ignored | wallet=%s fills_in_snapshot=%s total_ignored=%s",
                tag,
                wallet,
                fills_count,
                ignored_total,
            )
            return

        fills = data.get("fills", [])
        if not isinstance(fills, list):
            return

        for fill in fills:
            if not isinstance(fill, dict):
                continue
            self._process_fill(wallet=wallet, fill=fill, recv_ts_ms=recv_ts_ms, is_snapshot=is_snapshot, tag=tag)

    def _process_fill(
        self,
        wallet: str,
        fill: Dict[str, Any],
        recv_ts_ms: int,
        is_snapshot: bool,
        tag: str = "unknown",
    ) -> None:
        event_id = build_event_id(wallet, fill)
        fill_source = source_from_tag(tag)

        with self.lock:
            if event_id in self.processed_event_ids:
                self.total_duplicates_skipped += 1
                return

            coin = str(fill.get("coin", "")).strip()
            side = normalize_side(fill.get("side", ""))
            px = d(fill.get("px"), "0")
            sz = d(fill.get("sz"), "0")
            observed_ts_ms = int(d(fill.get("time"), "0"))
            latency_ms = max(0, recv_ts_ms - observed_ts_ms) if observed_ts_ms > 0 else 0

            if not coin or side not in {"BUY", "SELL"} or px <= 0 or sz <= 0:
                logging.warning("[%s] Skipping invalid fill event_id=%s fill=%s", tag, event_id, fill)
                self.processed_event_ids.add(event_id)
                return

            start_position = d(fill.get("startPosition"), "0")
            local_copy_qty = self.positions[wallet][coin].qty
            if local_copy_qty == 0 and start_position != 0:
                logging.info(
                    "[%s] Skipping fill — unsynced carry position | wallet=%s coin=%s "
                    "startPosition=%s event_id=%s",
                    tag, wallet, coin, start_position, event_id,
                )
                self.processed_event_ids.add(event_id)
                return

            copy_exec_px = self._simulate_exec_px(side=side, wallet_fill_px=px)
            if fill_source == "poll":
                if latency_ms < 2000:
                    latency_bps = Decimal("2")
                elif latency_ms < 10000:
                    latency_bps = Decimal("5")
                elif latency_ms < 60000:
                    latency_bps = Decimal("10")
                else:
                    latency_bps = Decimal("25")

                latency_penalty = px * (latency_bps / Decimal("10000"))
                if side == "BUY":
                    copy_exec_px += latency_penalty
                else:
                    copy_exec_px -= latency_penalty
            signed_qty = signed_qty_for_side(side, sz)
            slippage_cost = (copy_exec_px - px) * signed_qty
            copy_sz = sz * COPY_SIZE_MULTIPLIER
            copy_notional = copy_exec_px * copy_sz
            copy_fee = (copy_exec_px * copy_sz) * (COPY_FEE_BPS / Decimal("10000"))

            realized_delta = self._apply_copy_fill(
                wallet=wallet,
                coin=coin,
                side=side,
                exec_px=copy_exec_px,
                qty=copy_sz,
                fee=copy_fee,
                observed_ts_ms=observed_ts_ms,
                recv_ts_ms=recv_ts_ms,
            )

            if realized_delta > 0:
                self.wallet_stats[wallet].wins += 1
            elif realized_delta < 0:
                self.wallet_stats[wallet].losses += 1

            row = {
                "event_id": event_id,
                "wallet": wallet,
                "coin": coin,
                "side": side,
                "dir": side,
                "px": self._fmt(px),
                "sz": self._fmt(sz),
                "observed_ts_ms": observed_ts_ms,
                "observed_ts_iso": utc_iso_from_ms(observed_ts_ms) if observed_ts_ms else "",
                "received_ts_ms": recv_ts_ms,
                "received_ts_iso": utc_iso_from_ms(recv_ts_ms),
                "latency_ms": latency_ms,
                "hash": str(fill.get("hash", "")),
                "oid": str(fill.get("oid", "")),
                "tid": str(fill.get("tid", "")),
                "crossed": str(fill.get("crossed", "")),
                "wallet_fee": str(fill.get("fee", "")),
                "fee_token": str(fill.get("feeToken", "")),
                "builder_fee": str(fill.get("builderFee", "")),
                "closed_pnl": str(fill.get("closedPnl", "")),
                "start_position": str(fill.get("startPosition", "")),
                "is_snapshot": is_snapshot,
                "fill_source": fill_source,
                "copy_side": side,
                "copy_exec_px": self._fmt(copy_exec_px),
                "copy_sz": self._fmt(copy_sz),
                "copy_notional": self._fmt(copy_notional),
                "copy_fee": self._fmt(copy_fee),
                "copy_realized_pnl_delta": self._fmt(realized_delta),
            }

            self._append_raw_row(row)
            self.processed_event_ids.add(event_id)

            stats = self.wallet_stats[wallet]
            stats.unique_event_count += 1
            stats.trade_count += 1
            stats.slippage_cost_total += slippage_cost
            if fill_source == "poll":
                stats.polled_fill_count += 1
                stats.polling_realized_pnl += realized_delta
            elif fill_source == "websocket":
                stats.websocket_fill_count += 1
                stats.websocket_realized_pnl += realized_delta
            stats.last_fill_source = fill_source
            for profile_name in EXECUTION_PROFILES:
                profile_exec_px = self._profile_exec_px(
                    side=side,
                    wallet_fill_px=px,
                    latency_ms=latency_ms,
                    fill_source=fill_source,
                    profile_name=profile_name,
                )
                profile_fee = (profile_exec_px * copy_sz) * (COPY_FEE_BPS / Decimal("10000"))
                self._apply_profile_copy_fill(
                    wallet=wallet,
                    profile_name=profile_name,
                    coin=coin,
                    side=side,
                    exec_px=profile_exec_px,
                    qty=copy_sz,
                    fee=profile_fee,
                    observed_ts_ms=observed_ts_ms,
                    recv_ts_ms=recv_ts_ms,
                )
                self._record_profile_equity_point(
                    wallet=wallet,
                    profile_name=profile_name,
                    ts_ms=observed_ts_ms or recv_ts_ms,
                    equity=stats.profile_realized_pnl[profile_name],
                    persist=True,
                )
            stats.latency_sum_ms += latency_ms
            stats.latency_count += 1
            stats.max_latency_ms = max(stats.max_latency_ms, latency_ms)
            stats.avg_latency_ms = (
                Decimal(stats.latency_sum_ms) / Decimal(stats.latency_count)
                if stats.latency_count > 0 else Decimal("0")
            )
            if latency_ms < 100:
                stats.latency_bucket_lt_100ms += 1
            elif latency_ms < 300:
                stats.latency_bucket_100_300ms += 1
            elif latency_ms < 1000:
                stats.latency_bucket_300_1000ms += 1
            elif latency_ms <= 3000:
                stats.latency_bucket_1_3s += 1
            else:
                stats.latency_bucket_gt_3s += 1
            stats.last_fill_ts_ms = max(stats.last_fill_ts_ms, observed_ts_ms)
            stats.last_recv_ts_ms = max(stats.last_recv_ts_ms, recv_ts_ms)

            self._recompute_unrealized_locked()
            self._recompute_wallet_aggregates_locked()

            logging.info(
                "[%s] Fill processed | wallet=%s coin=%s side=%s px=%s sz=%s copy_px=%s realized_delta=%s snapshot=%s latency_ms=%s",
                tag,
                wallet,
                coin,
                side,
                self._fmt(px),
                self._fmt(sz),
                self._fmt(copy_exec_px),
                self._fmt(realized_delta),
                is_snapshot,
                latency_ms,
            )

    def _simulate_exec_px(self, side: str, wallet_fill_px: Decimal) -> Decimal:
        slip = COPY_SLIPPAGE_BPS / Decimal("10000")
        if side == "BUY":
            return wallet_fill_px * (Decimal("1") + slip)
        if side == "SELL":
            return wallet_fill_px * (Decimal("1") - slip)
        raise ValueError(f"Unknown side for slippage: {side}")

    def _profile_exec_px(
        self,
        side: str,
        wallet_fill_px: Decimal,
        latency_ms: int,
        fill_source: str,
        profile_name: str,
    ) -> Decimal:
        del latency_ms
        config = EXECUTION_PROFILES[profile_name]
        slippage_bps = Decimal(str(config["slippage_bps"]))
        latency_penalty_bps = Decimal(str(config["latency_penalty_bps"]))
        slippage = slippage_bps / Decimal("10000")

        if side == "BUY":
            exec_px = wallet_fill_px * (Decimal("1") + slippage)
        elif side == "SELL":
            exec_px = wallet_fill_px * (Decimal("1") - slippage)
        else:
            raise ValueError(f"Unknown side for profile pricing: {side}")

        if fill_source == "poll" and latency_penalty_bps > 0:
            latency_penalty = wallet_fill_px * (latency_penalty_bps / Decimal("10000"))
            if side == "BUY":
                exec_px += latency_penalty
            else:
                exec_px -= latency_penalty

        return exec_px

    def _apply_copy_fill(
        self,
        wallet: str,
        coin: str,
        side: str,
        exec_px: Decimal,
        qty: Decimal,
        fee: Decimal,
        observed_ts_ms: int,
        recv_ts_ms: int,
    ) -> Decimal:
        pos = self.positions[wallet][coin]
        signed_fill_qty = signed_qty_for_side(side, qty)

        realized_delta = Decimal("0")
        old_qty = pos.qty
        old_avg = pos.avg_entry_px

        if old_qty == 0 or (old_qty > 0 and signed_fill_qty > 0) or (old_qty < 0 and signed_fill_qty < 0):
            new_qty = old_qty + signed_fill_qty
            if old_qty == 0:
                pos.avg_entry_px = exec_px
            else:
                total_abs = abs(old_qty) + abs(signed_fill_qty)
                if total_abs > 0:
                    pos.avg_entry_px = (
                        (old_avg * abs(old_qty)) + (exec_px * abs(signed_fill_qty))
                    ) / total_abs
            pos.qty = new_qty
            realized_delta -= fee
        else:
            closing_qty = min(abs(old_qty), abs(signed_fill_qty))

            if old_qty > 0 and signed_fill_qty < 0:
                gross_pnl = (exec_px - old_avg) * closing_qty
            elif old_qty < 0 and signed_fill_qty > 0:
                gross_pnl = (old_avg - exec_px) * closing_qty
            else:
                gross_pnl = Decimal("0")

            realized_delta += gross_pnl
            realized_delta -= fee

            remaining_qty = old_qty + signed_fill_qty

            if remaining_qty == 0:
                pos.qty = Decimal("0")
                pos.avg_entry_px = Decimal("0")
            elif (old_qty > 0 and remaining_qty > 0) or (old_qty < 0 and remaining_qty < 0):
                pos.qty = remaining_qty
            else:
                pos.qty = remaining_qty
                pos.avg_entry_px = exec_px

        pos.realized_pnl += realized_delta
        pos.total_fees += fee
        pos.trade_count += 1
        pos.last_fill_ts_ms = observed_ts_ms
        pos.last_recv_ts_ms = recv_ts_ms
        pos.last_exec_px = exec_px

        wallet_stats = self.wallet_stats[wallet]
        wallet_stats.realized_pnl += realized_delta
        wallet_stats.total_fees += fee
        wallet_stats.total_notional += abs(exec_px * qty)

        return realized_delta

    def _apply_profile_copy_fill(
        self,
        wallet: str,
        profile_name: str,
        coin: str,
        side: str,
        exec_px: Decimal,
        qty: Decimal,
        fee: Decimal,
        observed_ts_ms: int,
        recv_ts_ms: int,
    ) -> Decimal:
        pos = self.profile_positions[wallet][profile_name][coin]
        signed_fill_qty = signed_qty_for_side(side, qty)

        realized_delta = Decimal("0")
        old_qty = pos.qty
        old_avg = pos.avg_entry_px

        if old_qty == 0 or (old_qty > 0 and signed_fill_qty > 0) or (old_qty < 0 and signed_fill_qty < 0):
            new_qty = old_qty + signed_fill_qty
            if old_qty == 0:
                pos.avg_entry_px = exec_px
            else:
                total_abs = abs(old_qty) + abs(signed_fill_qty)
                if total_abs > 0:
                    pos.avg_entry_px = (
                        (old_avg * abs(old_qty)) + (exec_px * abs(signed_fill_qty))
                    ) / total_abs
            pos.qty = new_qty
            realized_delta -= fee
        else:
            closing_qty = min(abs(old_qty), abs(signed_fill_qty))

            if old_qty > 0 and signed_fill_qty < 0:
                gross_pnl = (exec_px - old_avg) * closing_qty
            elif old_qty < 0 and signed_fill_qty > 0:
                gross_pnl = (old_avg - exec_px) * closing_qty
            else:
                gross_pnl = Decimal("0")

            realized_delta += gross_pnl
            realized_delta -= fee

            remaining_qty = old_qty + signed_fill_qty

            if remaining_qty == 0:
                pos.qty = Decimal("0")
                pos.avg_entry_px = Decimal("0")
            elif (old_qty > 0 and remaining_qty > 0) or (old_qty < 0 and remaining_qty < 0):
                pos.qty = remaining_qty
            else:
                pos.qty = remaining_qty
                pos.avg_entry_px = exec_px

        pos.realized_pnl += realized_delta
        pos.total_fees += fee
        pos.trade_count += 1
        pos.last_fill_ts_ms = observed_ts_ms
        pos.last_recv_ts_ms = recv_ts_ms
        pos.last_exec_px = exec_px

        wallet_stats = self.wallet_stats[wallet]
        wallet_stats.profile_realized_pnl[profile_name] += realized_delta
        wallet_stats.profile_trade_count[profile_name] += 1

        return realized_delta

    def _recompute_unrealized_locked(self) -> None:
        for wallet in self.wallets:
            wallet_unrealized = Decimal("0")
            for coin, pos in self.positions[wallet].items():
                if pos.qty == 0:
                    continue
                mark_px = self.latest_mids.get(coin)
                if mark_px is None or mark_px <= 0:
                    continue

                if pos.qty > 0:
                    wallet_unrealized += (mark_px - pos.avg_entry_px) * pos.qty
                else:
                    wallet_unrealized += (pos.avg_entry_px - mark_px) * abs(pos.qty)

            self.wallet_stats[wallet].unrealized_pnl = wallet_unrealized
            self.wallet_stats[wallet].total_pnl = (
                self.wallet_stats[wallet].realized_pnl + self.wallet_stats[wallet].unrealized_pnl
            )

    def _recompute_wallet_aggregates_locked(self, refresh_rankings: bool = True) -> None:
        for wallet in self.wallets:
            open_count = 0
            gross_notional = Decimal("0")
            net_notional = Decimal("0")

            for coin, pos in self.positions[wallet].items():
                if pos.qty == 0:
                    continue
                open_count += 1

                ref_px = self.latest_mids.get(coin, pos.last_exec_px if pos.last_exec_px > 0 else pos.avg_entry_px)
                if ref_px <= 0:
                    ref_px = pos.avg_entry_px

                gross_notional += abs(pos.qty) * ref_px
                net_notional += pos.qty * ref_px

            stats = self.wallet_stats[wallet]
            stats.open_position_count = open_count
            stats.gross_open_notional = gross_notional
            stats.net_open_notional = net_notional
            stats.total_pnl = stats.realized_pnl + stats.unrealized_pnl

        if refresh_rankings:
            self._refresh_rankings_locked()

    def _refresh_rankings_locked(self, force_log: bool = False) -> None:
        ranked = sorted(
            self.wallets,
            key=self._ranking_sort_key,
            reverse=True,
        )
        desired_top = ranked[:TOP_WS_WALLET_LIMIT]
        changed = desired_top != self.desired_top_wallets

        self.rankings = ranked
        if changed:
            previous = list(self.desired_top_wallets)
            self.desired_top_wallets = desired_top
            self._top_ws_rotate_event.set()
            logging.info(
                "Top-wallet rotation | previous=%s | next=%s",
                previous,
                desired_top,
            )
        elif force_log:
            logging.info("Top-wallet ranking initialized: %s", desired_top)

    def _ranking_sort_key(self, wallet: str) -> Tuple[Decimal, Decimal, int, Decimal, int]:
        stats = self.wallet_stats[wallet]
        ranking_pnl = stats.profile_realized_pnl["realistic"]
        total_decisions = stats.wins + stats.losses
        win_rate = (
            Decimal(stats.wins) / Decimal(total_decisions)
            if total_decisions > 0 else Decimal("0")
        )
        return (
            ranking_pnl,
            win_rate,
            stats.trade_count,
            stats.profile_realized_pnl["realistic"] + stats.unrealized_pnl,
            stats.last_fill_ts_ms,
        )

    def _get_desired_top_wallets(self) -> List[str]:
        with self.lock:
            return list(self.desired_top_wallets)

    def _log_top_rankings_locked(self) -> None:
        for rank, wallet in enumerate(self.rankings, start=1):
            stats = self.wallet_stats[wallet]
            ideal = stats.profile_realized_pnl["ideal"]
            realistic = stats.profile_realized_pnl["realistic"]
            bad = stats.profile_realized_pnl["bad"]
            degradation_pct = Decimal("0")
            if ideal != 0:
                degradation_pct = ((ideal - bad) / abs(ideal)) * Decimal("100")
            logging.info(
                "[ranking] rank=%s wallet=%s ideal=%s realistic=%s bad=%s degradation_pct=%s",
                rank,
                wallet,
                self._fmt(ideal),
                self._fmt(realistic),
                self._fmt(bad),
                self._fmt(degradation_pct),
            
            )

    def _set_active_top_wallets_for_worker(self, worker_name: str, wallets: List[str]) -> None:
        if wallets:
            self._active_top_wallets_by_worker[worker_name] = list(wallets)
        else:
            self._active_top_wallets_by_worker.pop(worker_name, None)

        merged: List[str] = []
        seen = set()
        for chunk_wallets_list in self._active_top_wallets_by_worker.values():
            for wallet in chunk_wallets_list:
                if wallet not in seen:
                    seen.add(wallet)
                    merged.append(wallet)
        self.active_top_wallets = merged

    def _reload_wallet_universe(self) -> int:
        wallets, seed_scores = load_wallets_from_csv(WALLET_SOURCE_CSV)

        with self.lock:
            added = 0
            existing_wallets = list(self.wallets)
            for wallet in wallets:
                if wallet not in self.wallet_stats:
                    self.wallet_stats[wallet] = WalletStats(wallet=wallet)
                    self.poll_cursor_end_ms[wallet] = max(0, now_ms() - INITIAL_POLL_LOOKBACK_MS)
                    added += 1
                    existing_wallets.append(wallet)
            self.wallets = normalize_wallets(existing_wallets)
            for wallet, score in seed_scores.items():
                self.seed_scores[wallet.lower()] = d(score, "0")
            self._recompute_unrealized_locked()
            self._recompute_wallet_aggregates_locked(refresh_rankings=False)
            self._refresh_rankings_locked(force_log=True)

        return added

    def _get_desired_top_wallet_chunk(self, chunk_idx: int) -> List[str]:
        with self.lock:
            chunks = chunk_wallets(self.desired_top_wallets, TOP_WS_USERS_PER_CONNECTION)
            if chunk_idx >= len(chunks):
                return []
            return list(chunks[chunk_idx])

    def _metrics_flush_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._flush_metrics()
            except Exception as exc:
                logging.exception("Metrics flush error: %s", exc)
            self.stop_event.wait(METRICS_FLUSH_INTERVAL_SECONDS)

        logging.info("Metrics flush loop stopped")

    def _compute_profile_unrealized_and_aggregates(
        self,
        profile_positions: Dict[str, PositionState],
    ) -> tuple:
        """
        Compute unrealized PnL and open-position aggregates from a supplied
        position map (e.g. profile_positions[wallet]["realistic"]).
        Returns: (unrealized_pnl, open_position_count, gross_open_notional, net_open_notional)
        All values use self.latest_mids for mark prices.
        """
        unrealized_pnl = Decimal("0")
        open_position_count = 0
        gross_open_notional = Decimal("0")
        net_open_notional = Decimal("0")

        for coin, pos in profile_positions.items():
            if pos.qty == 0:
                continue
            open_position_count += 1
            mark_px = self.latest_mids.get(coin)

            if mark_px is not None and mark_px > 0:
                if pos.qty > 0:
                    unrealized_pnl += (mark_px - pos.avg_entry_px) * pos.qty
                else:
                    unrealized_pnl += (pos.avg_entry_px - mark_px) * abs(pos.qty)
                ref_px = mark_px
            else:
                ref_px = pos.last_exec_px if pos.last_exec_px > 0 else pos.avg_entry_px

            if ref_px > 0:
                gross_open_notional += abs(pos.qty) * ref_px
                net_open_notional += pos.qty * ref_px

        return unrealized_pnl, open_position_count, gross_open_notional, net_open_notional

    def _flush_metrics(self) -> None:
        with self.lock:
            self._recompute_unrealized_locked()
            self._recompute_wallet_aggregates_locked()

            rows: List[Dict[str, Any]] = []
            for wallet in self.wallets:
                stats = self.wallet_stats[wallet]
                positions_json = self._positions_json(wallet)

                realistic_realized_pnl = stats.profile_realized_pnl["realistic"]

                (
                    realistic_unrealized_pnl,
                    realistic_open_position_count,
                    realistic_gross_open_notional,
                    realistic_net_open_notional,
                ) = self._compute_profile_unrealized_and_aggregates(
                    self.profile_positions[wallet]["realistic"]
                )

                realistic_total_pnl = realistic_realized_pnl + realistic_unrealized_pnl

                efficiency = (
                    realistic_realized_pnl / stats.total_notional
                    if stats.total_notional > 0 else Decimal("0")
                )

                rows.append(
                    {
                        "wallet": wallet,
                        "trade_count": stats.trade_count,
                        "unique_event_count": stats.unique_event_count,
                        "realized_pnl": self._fmt(realistic_realized_pnl),
                        "slippage_cost_total": self._fmt(stats.slippage_cost_total),
                        "unrealized_pnl": self._fmt(realistic_unrealized_pnl),
                        "total_pnl": self._fmt(realistic_total_pnl),
                        "total_fees": self._fmt(stats.total_fees),
                        "avg_latency_ms": self._fmt(stats.avg_latency_ms),
                        "max_latency_ms": stats.max_latency_ms,
                        "last_fill_ts_ms": stats.last_fill_ts_ms,
                        "last_fill_ts_iso": utc_iso_from_ms(stats.last_fill_ts_ms) if stats.last_fill_ts_ms else "",
                        "last_recv_ts_ms": stats.last_recv_ts_ms,
                        "last_recv_ts_iso": utc_iso_from_ms(stats.last_recv_ts_ms) if stats.last_recv_ts_ms else "",
                        "open_position_count": realistic_open_position_count,
                        "gross_open_notional": self._fmt(realistic_gross_open_notional),
                        "net_open_notional": self._fmt(realistic_net_open_notional),
                        "wins": stats.wins,
                        "losses": stats.losses,
                        "latency_bucket_lt_100ms": stats.latency_bucket_lt_100ms,
                        "latency_bucket_100_300ms": stats.latency_bucket_100_300ms,
                        "latency_bucket_300_1000ms": stats.latency_bucket_300_1000ms,
                        "latency_bucket_1_3s": stats.latency_bucket_1_3s,
                        "latency_bucket_gt_3s": stats.latency_bucket_gt_3s,
                        "polled_fill_count": stats.polled_fill_count,
                        "websocket_fill_count": stats.websocket_fill_count,
                        "last_fill_source": stats.last_fill_source,
                        "polling_realized_pnl": self._fmt(stats.polling_realized_pnl),
                        "websocket_realized_pnl": self._fmt(stats.websocket_realized_pnl),
                        "ideal_pnl": self._fmt(stats.profile_realized_pnl["ideal"]),
                        "realistic_pnl": self._fmt(realistic_realized_pnl),
                        "bad_pnl": self._fmt(stats.profile_realized_pnl["bad"]),
                        "total_notional": self._fmt(stats.total_notional),
                        "efficiency": self._fmt(efficiency),
                        "positions_json": positions_json,
                    }
                )

        self._rewrite_metrics_csv(rows)

    def _rewrite_metrics_csv(self, rows: List[Dict[str, Any]]) -> None:
        with self._metrics_csv_lock:
            if self._metrics_file_handle is None:
                raise RuntimeError("Metrics CSV handle is not initialized")

            self._metrics_file_handle.seek(0)
            self._metrics_file_handle.truncate(0)
            writer = csv.DictWriter(self._metrics_file_handle, fieldnames=self.METRIC_HEADERS)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            self._metrics_file_handle.flush()
            os.fsync(self._metrics_file_handle.fileno())

    def _positions_json(self, wallet: str) -> str:
        payload = {}
        for coin, pos in self.positions[wallet].items():
            if pos.qty == 0:
                continue
            payload[coin] = {
                "qty": self._fmt(pos.qty),
                "avg_entry_px": self._fmt(pos.avg_entry_px),
                "realized_pnl": self._fmt(pos.realized_pnl),
                "total_fees": self._fmt(pos.total_fees),
                "trade_count": pos.trade_count,
                "last_fill_ts_ms": pos.last_fill_ts_ms,
            }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)

    def _append_raw_row(self, row: Dict[str, Any]) -> None:
        with self._raw_csv_lock:
            if self._raw_writer is None or self._raw_file_handle is None:
                raise RuntimeError("Raw CSV writer is not initialized")
            self._raw_writer.writerow(row)
            self._raw_file_handle.flush()
            os.fsync(self._raw_file_handle.fileno())

    def _equity_curve_path(self, wallet: str) -> Path:
        return EQUITY_CURVES_DIR / f"{wallet}.csv"

    def _record_profile_equity_point(
        self,
        wallet: str,
        profile_name: str,
        ts_ms: int,
        equity: Decimal,
        persist: bool,
    ) -> None:
        self.profile_equity_curves[wallet][profile_name].append((ts_ms, equity))
        if persist:
            self._append_equity_curve_row(wallet, ts_ms, profile_name, equity)

    def _append_equity_curve_row(self, wallet: str, ts_ms: int, profile_name: str, equity: Decimal) -> None:
        path = self._equity_curve_path(wallet)
        with self._equity_curve_lock:
            EQUITY_CURVES_DIR.mkdir(parents=True, exist_ok=True)
            file_exists = path.exists()
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["ts", "profile", "equity"])
                if not file_exists or path.stat().st_size == 0:
                    writer.writeheader()
                writer.writerow(
                    {
                        "ts": ts_ms,
                        "profile": profile_name,
                        "equity": self._fmt(equity),
                    }
                )
                f.flush()
                os.fsync(f.fileno())

    def _rewrite_equity_curve_files(self) -> None:
        with self._equity_curve_lock:
            EQUITY_CURVES_DIR.mkdir(parents=True, exist_ok=True)
            for wallet in self.wallets:
                path = self._equity_curve_path(wallet)
                with open(path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["ts", "profile", "equity"])
                    writer.writeheader()
                    for profile_name in EXECUTION_PROFILES:
                        for ts_ms, equity in self.profile_equity_curves[wallet][profile_name]:
                            writer.writerow(
                                {
                                    "ts": ts_ms,
                                    "profile": profile_name,
                                    "equity": self._fmt(equity),
                                }
                            )
                    f.flush()
                    os.fsync(f.fileno())

    @staticmethod
    def _fmt(value: Decimal) -> str:
        normalized = value.normalize()
        text = format(normalized, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text if text else "0"


ENGINE: Optional[WalletListenerEngine] = None


def handle_signal(signum: int, frame: Any) -> None:
    del frame
    logging.info("Received signal %s -> stopping", signum)
    if ENGINE is not None:
        ENGINE.stop()


def main() -> None:
    setup_logging()

    wallets, seed_scores = load_wallets_from_csv(WALLET_SOURCE_CSV)

    global ENGINE
    ENGINE = WalletListenerEngine(wallets, seed_scores)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logging.info("Configuration")
    logging.info("  wallet_source_csv=%s", WALLET_SOURCE_CSV.resolve())
    logging.info("  wallets=%s", len(wallets))
    logging.info("  top_ws_wallet_limit=%s", TOP_WS_WALLET_LIMIT)
    logging.info("  poll_interval_seconds=%s", POLL_INTERVAL_SECONDS)
    logging.info("  poll_overlap_ms=%s", POLL_OVERLAP_MS)
    logging.info("  output_dir=%s", OUTPUT_DIR.resolve())
    logging.info("  raw_trades_csv=%s", RAW_TRADES_CSV.resolve())
    logging.info("  live_metrics_csv=%s", LIVE_METRICS_CSV.resolve())
    logging.info("  slippage_bps=%s", COPY_SLIPPAGE_BPS)
    logging.info("  fee_bps=%s", COPY_FEE_BPS)
    logging.info("  size_multiplier=%s", COPY_SIZE_MULTIPLIER)
    logging.info("  process_snapshot_fills=%s", PROCESS_SNAPSHOT_FILLS)

    ENGINE.start()


if __name__ == "__main__":
    main()
