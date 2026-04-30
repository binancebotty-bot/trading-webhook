"""
HL_Live_Copy_Service.py

Phase 2 dry-run live copy service for the Hyperliquid Copy Engine stack.

Boundary:
- NEW FILE ONLY.
- Reads app live config from hl_live_copy_audit/live_config.json.
- Reads leader fills from hl_copy_output/raw_live_fills.csv for dry-run replay/proof.
- Writes only to permanent audit files outside hl_copy_output.
- Does NOT place exchange orders.
- Does NOT use private keys.
- Does NOT open websockets.
- Does NOT mutate engine truth or app-derived dashboard files.

Run:
    python HL_Live_Copy_Service.py --once
    python HL_Live_Copy_Service.py --loop --interval 5
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except Exception:
    requests = None

try:
    import websocket
except Exception:
    websocket = None

BASE_DIR = Path(__file__).resolve().parent
ENGINE_OUTPUT_DIR = BASE_DIR / "hl_copy_output"
RAW_LEADER_FILLS_CSV = Path(os.environ.get("HL_LIVE_SOURCE_FILLS", ENGINE_OUTPUT_DIR / "raw_live_fills.csv"))

DEFAULT_AUDIT_DIR = BASE_DIR / "hl_live_copy_audit"
REPLAY_AUDIT_DIR = BASE_DIR / "hl_live_copy_audit_replay"
AUDIT_DIR_OVERRIDE = os.environ.get("HL_LIVE_AUDIT_DIR")
AUDIT_DIR = Path(AUDIT_DIR_OVERRIDE or DEFAULT_AUDIT_DIR)
APP_CONFIG_FILE = AUDIT_DIR / "live_config.json"
APPEND_ONLY_DIR = AUDIT_DIR / "append_only"

SERVICE_STATE_FILE = AUDIT_DIR / "live_service_state.json"
LIVE_POSITIONS_FILE = AUDIT_DIR / "live_positions.json"
LIVE_EQUITY_HISTORY_FILE = AUDIT_DIR / "live_equity_history.json"
LIVE_WS_HEALTH_FILE = AUDIT_DIR / "live_ws_health.json"

ORDER_INTENTS_CSV = APPEND_ONLY_DIR / "order_intents.csv"
LIVE_FILLS_CSV = APPEND_ONLY_DIR / "live_fills.csv"
RECONCILIATION_CSV = APPEND_ONLY_DIR / "reconciliation.csv"
ERRORS_CSV = APPEND_ONLY_DIR / "errors.csv"

DRY_RUN = True
MAX_LIVE_WALLETS = 10
DEFAULT_LEADER_EQUITY_BASE = 10_000.0
DEFAULT_NORM_BASE = 100.0
DEFAULT_FIXED_NOTIONAL = 10.0
MIN_ORDER_NOTIONAL = 10.0
LIVE_POLL_ENABLED = os.getenv("HL_LIVE_POLL_ENABLED", "1") == "1"
LIVE_POLL_OVERLAP_MS = int(os.getenv("HL_LIVE_POLL_OVERLAP_MS", "300000"))
LIVE_POLL_WINDOW_MS = int(os.getenv("HL_LIVE_POLL_WINDOW_MS", str(24 * 60 * 60 * 1000)))
LIVE_POLL_MAX_PAGE_ROWS = int(os.getenv("HL_LIVE_POLL_MAX_PAGE_ROWS", "500"))
LIVE_WS_ENABLED = os.getenv("HL_LIVE_WS_ENABLED", "0") == "1"
LIVE_WS_URL = os.getenv("HL_LIVE_WS_URL", "wss://api.hyperliquid.xyz/ws")
LIVE_WS_MAX_WALLETS = 10
LIVE_WS_STALE_MS = int(os.getenv("HL_LIVE_WS_STALE_MS", "30000"))
LIVE_WS_PING_INTERVAL_SEC = int(os.getenv("HL_LIVE_WS_PING_INTERVAL_SEC", "20"))
LIVE_WS_PING_TIMEOUT_SEC = int(os.getenv("HL_LIVE_WS_PING_TIMEOUT_SEC", "10"))
LIVE_WS_RECONNECT_BACKOFF_CAP_SEC = float(os.getenv("HL_LIVE_WS_RECONNECT_BACKOFF_CAP_SEC", "30"))
LIVE_WS_HEALTH_WRITE_SEC = float(os.getenv("HL_LIVE_WS_HEALTH_WRITE_SEC", "2"))
LIVE_WS_INITIAL_BACKOFF_SEC = float(os.getenv("HL_LIVE_WS_INITIAL_BACKOFF_SEC", "1"))
LIVE_WS_BACKOFF_MULTIPLIER = float(os.getenv("HL_LIVE_WS_BACKOFF_MULTIPLIER", "1.5"))

ORDER_INTENT_FIELDS = [
    "created_at", "intent_id", "dry_run", "leader_wallet", "leader_fill_id",
    "linked_leader_fill_ids", "mode", "copy_mode", "coin", "side",
    "intent_type", "reason", "status", "leader_price", "target_price",
    "leader_size", "leader_notional", "copy_notional", "copy_size",
    "min_notional_policy", "diff_pct", "max_diff_pct", "daily_loss_limit", "notes",
]

LIVE_FILL_FIELDS = [
    "created_at", "dry_run", "intent_id", "leader_wallet", "leader_fill_id",
    "coin", "side", "fill_status", "fill_price", "fill_size", "fill_notional",
    "fee", "fee_policy", "realized_pnl", "unrealized_after",
    "position_signed_size_after", "position_avg_entry_after", "notes",
]

RECONCILIATION_FIELDS = [
    "created_at", "leader_wallet", "leader_fill_id", "coin", "event", "status",
    "mode", "leader_side", "copy_side_before", "copy_side_after",
    "copy_signed_size_before", "copy_signed_size_after", "copy_notional", "action", "notes",
]

ERROR_FIELDS = ["created_at", "context", "error_type", "message", "traceback"]


def configure_paths(audit_dir: Path, source_fills: Path) -> None:
    global AUDIT_DIR, APP_CONFIG_FILE, APPEND_ONLY_DIR, SERVICE_STATE_FILE
    global LIVE_POSITIONS_FILE, LIVE_EQUITY_HISTORY_FILE, LIVE_WS_HEALTH_FILE, ORDER_INTENTS_CSV
    global LIVE_FILLS_CSV, RECONCILIATION_CSV, ERRORS_CSV, RAW_LEADER_FILLS_CSV

    AUDIT_DIR = Path(audit_dir)
    RAW_LEADER_FILLS_CSV = Path(source_fills)
    APP_CONFIG_FILE = AUDIT_DIR / "live_config.json"
    APPEND_ONLY_DIR = AUDIT_DIR / "append_only"
    SERVICE_STATE_FILE = AUDIT_DIR / "live_service_state.json"
    LIVE_POSITIONS_FILE = AUDIT_DIR / "live_positions.json"
    LIVE_EQUITY_HISTORY_FILE = AUDIT_DIR / "live_equity_history.json"
    LIVE_WS_HEALTH_FILE = AUDIT_DIR / "live_ws_health.json"
    ORDER_INTENTS_CSV = APPEND_ONLY_DIR / "order_intents.csv"
    LIVE_FILLS_CSV = APPEND_ONLY_DIR / "live_fills.csv"
    RECONCILIATION_CSV = APPEND_ONLY_DIR / "reconciliation.csv"
    ERRORS_CSV = APPEND_ONLY_DIR / "errors.csv"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        x = float(value)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def inum(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def ensure_dirs() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    APPEND_ONLY_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        pass
    return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}_{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def ensure_csv_header(path: Path, fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def append_csv(path: Path, fieldnames: List[str], row: Dict[str, Any]) -> None:
    ensure_csv_header(path, fieldnames)
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writerow({k: row.get(k, "") for k in fieldnames})


def side_from_signed(value: float) -> str:
    if value > 1e-12:
        return "LONG"
    if value < -1e-12:
        return "SHORT"
    return "FLAT"


def trade_delta_from_side(side: str, size: float) -> float:
    return abs(size) if str(side).upper() == "BUY" else -abs(size)


def is_reducing_position(current_signed: float, incoming_delta: float) -> bool:
    return current_signed != 0 and current_signed * incoming_delta < 0


def weighted_entry_price(old_size: float, old_entry: float, add_size: float, add_price: float) -> float:
    old_abs = abs(old_size)
    add_abs = abs(add_size)
    if old_abs + add_abs <= 0:
        return 0.0
    return ((old_entry * old_abs) + (add_price * add_abs)) / (old_abs + add_abs)


@dataclass(frozen=True)
class LiveWalletConfig:
    wallet: str
    mode: str = "OFF"
    copy_mode: str = "proportional"
    norm_base: float = DEFAULT_NORM_BASE
    fixed_notional: float = DEFAULT_FIXED_NOTIONAL
    leader_equity_base: float = DEFAULT_LEADER_EQUITY_BASE
    max_diff_pct: float = 0.10
    daily_loss_limit: float = 0.0
    enabled: bool = True

    @staticmethod
    def from_raw(wallet: str, raw: Any) -> "LiveWalletConfig":
        raw = raw if isinstance(raw, dict) else {}
        mode = str(raw.get("mode", "OFF")).upper()
        if mode not in {"LIVE", "CLO", "OFF"}:
            mode = "OFF"
        copy_mode = str(raw.get("copy_mode", "proportional")).lower()
        if copy_mode not in {"proportional", "fixed"}:
            copy_mode = "proportional"
        return LiveWalletConfig(
            wallet=str(wallet).lower().strip(),
            mode=mode,
            copy_mode=copy_mode,
            norm_base=max(1.0, fnum(raw.get("norm_base"), DEFAULT_NORM_BASE)),
            fixed_notional=max(0.01, fnum(raw.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL)),
            leader_equity_base=max(1.0, fnum(raw.get("leader_equity_base"), DEFAULT_LEADER_EQUITY_BASE)),
            max_diff_pct=max(0.0, fnum(raw.get("max_diff_pct"), 0.10)),
            daily_loss_limit=max(0.0, fnum(raw.get("daily_loss_limit"), 0.0)),
            enabled=str(raw.get("enabled", "true")).lower() not in {"0", "false", "no", "off"},
        )


@dataclass(frozen=True)
class LeaderFill:
    fill_id: str
    wallet: str
    coin: str
    side: str
    price: float
    size: float
    signed_size_delta: float
    timestamp_ms: int
    timestamp_iso: str
    source: str
    recording_method: str
    raw: Dict[str, Any]

    @property
    def notional(self) -> float:
        return abs(self.price * self.size)


def parse_raw_json(text: str) -> Dict[str, Any]:
    try:
        data = json.loads(text or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def raw_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(d, dict) and key in d:
            return d[key]
    return default


def normalise_side(raw_side: Any, signed_size: float = 0.0, start_position: float = 0.0) -> str:
    side = str(raw_side or "").strip().lower()
    if side in {"b", "buy", "long", "bid"}:
        return "BUY"
    if side in {"a", "s", "sell", "short", "ask"}:
        return "SELL"
    if signed_size > 0:
        return "BUY"
    if signed_size < 0:
        return "SELL"
    return "SELL" if start_position > 0 else "BUY"


def parse_leader_fill_row(row: Dict[str, Any]) -> Optional[LeaderFill]:
    wallet = str(row.get("wallet", "")).lower().strip()
    coin = str(row.get("coin", "")).upper().strip()
    side = str(row.get("side", "")).upper().strip()
    price = fnum(row.get("price"))
    size = abs(fnum(row.get("size")))
    ts = inum(row.get("timestamp_ms"))
    if not wallet or not coin or side not in {"BUY", "SELL"} or price <= 0 or size <= 0 or ts <= 0:
        return None
    delta = fnum(row.get("signed_size_delta"), trade_delta_from_side(side, size))
    return LeaderFill(
        fill_id=str(row.get("fill_id", f"{wallet}:{coin}:{ts}:{side}:{size}:{price}")),
        wallet=wallet,
        coin=coin,
        side=side,
        price=price,
        size=size,
        signed_size_delta=delta if delta != 0 else trade_delta_from_side(side, size),
        timestamp_ms=ts,
        timestamp_iso=str(row.get("timestamp_iso", "")),
        source=str(row.get("source", "")),
        recording_method=str(row.get("recording_method", "")),
        raw=parse_raw_json(str(row.get("raw_json", "{}"))),
    )


def parse_api_leader_fill(wallet: str, raw: Dict[str, Any]) -> Optional[LeaderFill]:
    wallet = str(wallet or raw_get(raw, "user", "wallet", default="")).lower()
    if not wallet:
        return None

    coin = str(raw_get(raw, "coin", "symbol", "asset", default="")).upper().strip()
    price = fnum(raw_get(raw, "px", "price", "avgPx"))
    size = abs(fnum(raw_get(raw, "sz", "size", "qty")))
    ts = inum(raw_get(raw, "time", "timestamp", "ts"), utc_now_ms())
    if not coin or price <= 0 or size <= 0 or ts <= 0:
        return None

    start_pos = fnum(raw_get(raw, "startPosition", "start_pos", "start_position"), 0.0)
    side = normalise_side(raw_get(raw, "side", "dir", "direction"), size, start_pos)
    delta = size if side == "BUY" else -size
    oid = str(raw_get(raw, "oid", "hash", "tid", "id", default=""))
    fill_id = str(raw_get(raw, "fill_id", default="")) or f"{wallet}:{coin}:{ts}:{side}:{size:.12g}:{price:.12g}:{oid}"

    return LeaderFill(
        fill_id=fill_id,
        wallet=wallet,
        coin=coin,
        side=side,
        price=price,
        size=size,
        signed_size_delta=delta,
        timestamp_ms=ts,
        timestamp_iso=datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
        source="live_poll",
        recording_method="REBUILD",
        raw=raw,
    )


def extract_ws_fills(message: str) -> List[Tuple[str, Dict[str, Any]]]:
    try:
        msg = json.loads(message)
    except Exception:
        return []

    data = msg.get("data", msg) if isinstance(msg, dict) else msg
    out: List[Tuple[str, Dict[str, Any]]] = []

    if isinstance(data, dict):
        if bool(data.get("isSnapshot")):
            return []
        wallet = str(data.get("user") or data.get("wallet") or "").lower()
        fills = data.get("fills") or data.get("userFills") or []
        if isinstance(fills, list):
            for item in fills:
                if isinstance(item, dict):
                    out.append((wallet or str(item.get("user") or item.get("wallet") or "").lower(), item))
        return out

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                wallet = str(item.get("user") or item.get("wallet") or "").lower()
                out.append((wallet, item))
    return out


def is_benign_ws_error(err: Any) -> bool:
    name = type(err).__name__
    module = type(err).__module__
    text = repr(err)
    if "websocket._abnf" in module:
        return True
    if name == "ABNF":
        return True
    if text.startswith("<websocket._abnf.ABNF object"):
        return True
    return False


def parse_ws_leader_fill(wallet: str, raw: Dict[str, Any]) -> Optional[LeaderFill]:
    fill = parse_api_leader_fill(wallet, raw)
    if fill is None:
        return None
    return LeaderFill(
        fill_id=fill.fill_id,
        wallet=fill.wallet,
        coin=fill.coin,
        side=fill.side,
        price=fill.price,
        size=fill.size,
        signed_size_delta=fill.signed_size_delta,
        timestamp_ms=fill.timestamp_ms,
        timestamp_iso=fill.timestamp_iso,
        source="live_ws",
        recording_method="WS_CAPTURED",
        raw=fill.raw,
    )


def audit_reason_for_fill(fill: LeaderFill, replay_history: bool = False) -> str:
    if replay_history:
        return "REPLAY_HISTORY"
    source = str(getattr(fill, "source", "") or "").lower()
    if source == "live_poll":
        return "LIVE_POLL_DETECTED"
    if source in {"ws", "live_ws"}:
        return "LIVE_WS_DETECTED"
    return "SOURCE_CSV_FORWARD"


def audit_notes_for_fill(fill: LeaderFill, replay_history: bool = False) -> str:
    if replay_history:
        return "dry-run replay; source=replay_history; no exchange order placed"
    source = str(getattr(fill, "source", "") or "").lower()
    if source == "live_poll":
        return "dry-run simulated fill; source=live_poll; no exchange order placed"
    if source in {"ws", "live_ws"}:
        return "dry-run simulated fill; source=live_ws; no exchange order placed"
    return "dry-run simulated fill; source=source_csv_forward; no exchange order placed"


def load_leader_fills(path: Optional[Path] = None) -> List[LeaderFill]:
    if path is None:
        path = RAW_LEADER_FILLS_CSV
    if not path.exists():
        return []
    fills: List[LeaderFill] = []
    seen = set()
    with path.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fill = parse_leader_fill_row(row)
            if fill is None or fill.fill_id in seen:
                continue
            seen.add(fill.fill_id)
            fills.append(fill)
    fills.sort(key=lambda x: (x.timestamp_ms, x.wallet, x.coin, x.fill_id))
    return fills


def fetch_live_fills_range(wallet: str, start_ms: int, end_ms: int) -> Optional[List[Dict[str, Any]]]:
    if requests is None:
        append_csv(ERRORS_CSV, ERROR_FIELDS, {
            "created_at": utc_now_iso(),
            "context": "LIVE_POLL_FETCH",
            "error_type": "RequestsUnavailable",
            "message": "requests module unavailable",
            "traceback": "",
        })
        return None

    try:
        payload = {
            "type": "userFillsByTime",
            "user": wallet,
            "startTime": int(start_ms),
            "endTime": int(end_ms),
            "aggregateByTime": False,
        }
        response = requests.post("https://api.hyperliquid.xyz/info", json=payload, timeout=8)
        data = response.json()
        if not isinstance(data, list):
            append_csv(ERRORS_CSV, ERROR_FIELDS, {
                "created_at": utc_now_iso(),
                "context": "LIVE_POLL_FETCH",
                "error_type": "InvalidResponse",
                "message": str(data)[:500],
                "traceback": "",
            })
            return None
        return data
    except Exception as exc:
        append_csv(ERRORS_CSV, ERROR_FIELDS, {
            "created_at": utc_now_iso(),
            "context": "LIVE_POLL_FETCH",
            "error_type": type(exc).__name__,
            "message": f"wallet={wallet} start={start_ms} end={end_ms} err={exc}",
            "traceback": traceback.format_exc(),
        })
        return None


def fetch_live_fills_since(wallet: str, start_ms: int, end_ms: int) -> Optional[List[Dict[str, Any]]]:
    cursor = max(0, int(start_ms))
    end_ms = max(cursor, int(end_ms))
    all_rows: List[Dict[str, Any]] = []

    while cursor < end_ms:
        window_end = min(cursor + LIVE_POLL_WINDOW_MS, end_ms)
        page_cursor = cursor

        while page_cursor < window_end:
            rows = fetch_live_fills_range(wallet, page_cursor, window_end)
            if rows is None:
                return None
            if not rows:
                break

            all_rows.extend(rows)
            row_times = [
                inum(raw_get(row, "time", "timestamp", "ts"), page_cursor)
                for row in rows
                if isinstance(row, dict)
            ]
            max_row_ts = max(row_times) if row_times else page_cursor

            if len(rows) >= LIVE_POLL_MAX_PAGE_ROWS and max_row_ts >= page_cursor:
                next_cursor = max_row_ts + 1
                if next_cursor <= page_cursor:
                    next_cursor = page_cursor + 1
                page_cursor = min(next_cursor, window_end)
                continue

            break

        cursor = window_end

    return all_rows


class DryRunLiveCopyService:
    def __init__(self) -> None:
        ensure_dirs()
        self.state = load_json(SERVICE_STATE_FILE, {})
        if not isinstance(self.state, dict):
            self.state = {}
        self.positions = load_json(LIVE_POSITIONS_FILE, {})
        if not isinstance(self.positions, dict):
            self.positions = {}
        self.equity_history = load_json(LIVE_EQUITY_HISTORY_FILE, {})
        if not isinstance(self.equity_history, dict):
            self.equity_history = {"wallets": {}}

        self.state.setdefault("schema", "live_copy_service_state.v1.dry_run")
        self.state.setdefault("dry_run", True)
        self.state.setdefault("created_at", utc_now_iso())
        self.state.setdefault("processed_leader_fill_ids", [])
        self.state.setdefault("accumulators", {})
        self.state.setdefault("counters", {})
        self.processed_id_order: List[str] = []
        seen_processed = set()
        for item in self.state.get("processed_leader_fill_ids", []):
            fill_id = str(item)
            if fill_id and fill_id not in seen_processed:
                self.processed_id_order.append(fill_id)
                seen_processed.add(fill_id)
        self.processed_ids = set(self.processed_id_order)

        for path, fields in [
            (ORDER_INTENTS_CSV, ORDER_INTENT_FIELDS),
            (LIVE_FILLS_CSV, LIVE_FILL_FIELDS),
            (RECONCILIATION_CSV, RECONCILIATION_FIELDS),
            (ERRORS_CSV, ERROR_FIELDS),
        ]:
            ensure_csv_header(path, fields)

    def bump(self, key: str, amount: int = 1) -> None:
        counters = self.state.setdefault("counters", {})
        counters[key] = int(counters.get(key, 0)) + amount

    def load_config(self) -> Dict[str, LiveWalletConfig]:
        config_file = APP_CONFIG_FILE
        if not config_file.exists() and AUDIT_DIR == REPLAY_AUDIT_DIR and not AUDIT_DIR_OVERRIDE:
            config_file = DEFAULT_AUDIT_DIR / "live_config.json"
        raw = load_json(config_file, {})
        wallets = raw.get("wallets", {}) if isinstance(raw, dict) else {}
        if not isinstance(wallets, dict):
            wallets = {}
        out: Dict[str, LiveWalletConfig] = {}
        live_count = 0
        for wallet, cfg in wallets.items():
            item = LiveWalletConfig.from_raw(wallet, cfg)
            if not item.wallet:
                continue
            if item.mode == "LIVE":
                live_count += 1
                if live_count > MAX_LIVE_WALLETS:
                    item = LiveWalletConfig(**{**asdict(item), "mode": "OFF"})
            out[item.wallet] = item
        return out

    def persist(self) -> None:
        self.state["updated_at"] = utc_now_iso()
        missing = [x for x in sorted(self.processed_ids) if x not in set(self.processed_id_order)]
        self.processed_id_order.extend(missing)
        self.processed_id_order = [x for x in self.processed_id_order if x in self.processed_ids][-250_000:]
        self.processed_ids = set(self.processed_id_order)
        self.state["processed_leader_fill_ids"] = list(self.processed_id_order)
        self.state["audit_dir"] = str(AUDIT_DIR)
        atomic_write_json(SERVICE_STATE_FILE, self.state)
        atomic_write_json(LIVE_POSITIONS_FILE, self.positions)
        atomic_write_json(LIVE_EQUITY_HISTORY_FILE, self.equity_history)

    def log_error(self, context: str, exc: BaseException) -> None:
        append_csv(ERRORS_CSV, ERROR_FIELDS, {
            "created_at": utc_now_iso(),
            "context": context,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        })
        self.bump("errors")

    def position_key(self, wallet: str, coin: str) -> str:
        return f"{wallet.lower()}::{coin.upper()}"

    def get_position(self, wallet: str, coin: str) -> Dict[str, Any]:
        key = self.position_key(wallet, coin)
        return self.positions.setdefault(key, {
            "leader_wallet": wallet.lower(), "coin": coin.upper(), "signed_size": 0.0,
            "side": "FLAT", "avg_entry_price": 0.0, "last_price": 0.0,
            "realized_pnl": 0.0, "unrealized_pnl": 0.0, "notional": 0.0,
            "updated_at": utc_now_iso(), "source": "dry_run",
        })

    def update_equity(self, cfg: LiveWalletConfig, fill: LeaderFill) -> None:
        wallet_positions = [p for p in self.positions.values() if isinstance(p, dict) and str(p.get("leader_wallet", "")).lower() == cfg.wallet]
        realized = sum(fnum(p.get("realized_pnl")) for p in wallet_positions)
        unrealized = sum(fnum(p.get("unrealized_pnl")) for p in wallet_positions)
        exposure = sum(abs(fnum(p.get("notional"))) for p in wallet_positions)
        alloc = cfg.norm_base if cfg.copy_mode == "proportional" else max(cfg.norm_base, cfg.fixed_notional)
        equity = alloc + realized + unrealized
        wallets = self.equity_history.setdefault("wallets", {})
        record = wallets.setdefault(cfg.wallet, {"curve": [], "peak_equity": alloc, "max_drawdown": 0.0})
        peak = max(fnum(record.get("peak_equity"), alloc), equity)
        drawdown = max(0.0, peak - equity)
        max_dd = max(fnum(record.get("max_drawdown")), drawdown)
        record["peak_equity"] = peak
        record["max_drawdown"] = max_dd
        record["updated_at"] = utc_now_iso()
        record["curve"].append({
            "ts": fill.timestamp_iso or utc_now_iso(), "timestamp_ms": fill.timestamp_ms,
            "leader_wallet": cfg.wallet, "equity": round(equity, 8), "alloc": round(alloc, 8),
            "realized": round(realized, 8), "unrealized": round(unrealized, 8),
            "exposure": round(exposure, 8), "drawdown": round(drawdown, 8),
            "max_drawdown": round(max_dd, 8), "dry_run": True,
        })
        record["curve"] = record["curve"][-20_000:]

    def model_copy_notional(self, cfg: LiveWalletConfig, fill: LeaderFill) -> float:
        if cfg.copy_mode == "fixed":
            return cfg.fixed_notional
        return fill.notional * (cfg.norm_base / cfg.leader_equity_base)

    def accumulator_key(self, cfg: LiveWalletConfig, fill: LeaderFill) -> str:
        return f"{cfg.wallet}::{fill.coin}::{fill.side}"

    def handle_min_notional_for_entry(self, cfg: LiveWalletConfig, fill: LeaderFill, copy_notional: float) -> Tuple[bool, float, List[str], str]:
        if copy_notional >= MIN_ORDER_NOTIONAL:
            return True, copy_notional, [fill.fill_id], "DIRECT_EXECUTABLE"
        key = self.accumulator_key(cfg, fill)
        acc = self.state.setdefault("accumulators", {}).setdefault(key, {
            "leader_wallet": cfg.wallet, "coin": fill.coin, "side": fill.side,
            "notional": 0.0, "source_fill_ids": [], "updated_at": utc_now_iso(),
        })
        acc["notional"] = fnum(acc.get("notional")) + copy_notional
        acc.setdefault("source_fill_ids", []).append(fill.fill_id)
        acc["updated_at"] = utc_now_iso()
        total = fnum(acc.get("notional"))
        ids = list(acc.get("source_fill_ids", []))
        if total >= MIN_ORDER_NOTIONAL:
            acc["notional"] = 0.0
            acc["source_fill_ids"] = []
            acc["released_at"] = utc_now_iso()
            return True, total, ids, "ACCUMULATED_UNTIL_EXECUTABLE"
        return False, total, ids, "ACCUMULATED_NO_ORDER"

    def append_reconciliation(self, cfg: LiveWalletConfig, fill: LeaderFill, event: str, status: str, action: str, before: float, after: float, copy_notional: float, notes: str = "") -> None:
        append_csv(RECONCILIATION_CSV, RECONCILIATION_FIELDS, {
            "created_at": utc_now_iso(), "leader_wallet": cfg.wallet, "leader_fill_id": fill.fill_id,
            "coin": fill.coin, "event": event, "status": status, "mode": cfg.mode,
            "leader_side": fill.side, "copy_side_before": side_from_signed(before),
            "copy_side_after": side_from_signed(after), "copy_signed_size_before": round(before, 12),
            "copy_signed_size_after": round(after, 12), "copy_notional": round(copy_notional, 8),
            "action": action, "notes": notes,
        })

    def append_order_intent(self, cfg: LiveWalletConfig, fill: LeaderFill, intent_id: str, linked_ids: List[str], intent_type: str, reason: str, status: str, copy_notional: float, copy_size: float, policy: str, notes: str = "") -> None:
        append_csv(ORDER_INTENTS_CSV, ORDER_INTENT_FIELDS, {
            "created_at": utc_now_iso(), "intent_id": intent_id, "dry_run": True,
            "leader_wallet": cfg.wallet, "leader_fill_id": fill.fill_id,
            "linked_leader_fill_ids": "|".join(linked_ids), "mode": cfg.mode,
            "copy_mode": cfg.copy_mode, "coin": fill.coin, "side": fill.side,
            "intent_type": intent_type, "reason": reason, "status": status,
            "leader_price": round(fill.price, 8), "target_price": round(fill.price, 8),
            "leader_size": round(fill.size, 12), "leader_notional": round(fill.notional, 8),
            "copy_notional": round(copy_notional, 8), "copy_size": round(copy_size, 12),
            "min_notional_policy": policy, "diff_pct": 0.0, "max_diff_pct": cfg.max_diff_pct,
            "daily_loss_limit": cfg.daily_loss_limit, "notes": notes,
        })

    def append_live_fill(self, cfg: LiveWalletConfig, fill: LeaderFill, intent_id: str, status: str, copy_notional: float, copy_size: float, pos: Dict[str, Any], realized_pnl: float, notes: str = "") -> None:
        append_csv(LIVE_FILLS_CSV, LIVE_FILL_FIELDS, {
            "created_at": utc_now_iso(), "dry_run": True, "intent_id": intent_id,
            "leader_wallet": cfg.wallet, "leader_fill_id": fill.fill_id, "coin": fill.coin,
            "side": fill.side, "fill_status": status, "fill_price": round(fill.price, 8),
            "fill_size": round(copy_size, 12), "fill_notional": round(copy_notional, 8),
            "fee": 0.0, "fee_policy": "DRY_RUN_NO_EXCHANGE_FEE", "realized_pnl": round(realized_pnl, 8),
            "unrealized_after": round(fnum(pos.get("unrealized_pnl")), 8),
            "position_signed_size_after": round(fnum(pos.get("signed_size")), 12),
            "position_avg_entry_after": round(fnum(pos.get("avg_entry_price")), 8), "notes": notes,
        })

    def apply_dry_run_fill_to_position(self, cfg: LiveWalletConfig, fill: LeaderFill, copy_notional: float) -> Tuple[Dict[str, Any], float, float]:
        pos = self.get_position(cfg.wallet, fill.coin)
        before = fnum(pos.get("signed_size"))
        incoming_delta = trade_delta_from_side(fill.side, copy_notional / fill.price if fill.price > 0 else 0.0)
        realized_pnl = 0.0
        old_entry = fnum(pos.get("avg_entry_price"))
        new_signed = before + incoming_delta
        if before == 0 or before * incoming_delta > 0:
            new_entry = weighted_entry_price(before, old_entry, incoming_delta, fill.price)
        elif abs(incoming_delta) < abs(before):
            direction = 1.0 if before > 0 else -1.0
            realized_pnl = (fill.price - old_entry) * abs(incoming_delta) * direction
            new_entry = old_entry
        elif abs(incoming_delta) == abs(before):
            direction = 1.0 if before > 0 else -1.0
            realized_pnl = (fill.price - old_entry) * abs(before) * direction
            new_entry = 0.0
        else:
            direction = 1.0 if before > 0 else -1.0
            realized_pnl = (fill.price - old_entry) * abs(before) * direction
            new_entry = fill.price
        if abs(new_signed) < 1e-12:
            new_signed = 0.0
            new_entry = 0.0
        unrealized = 0.0
        if new_signed != 0 and new_entry > 0:
            direction = 1.0 if new_signed > 0 else -1.0
            unrealized = (fill.price - new_entry) * abs(new_signed) * direction
        pos.update({
            "signed_size": new_signed, "side": side_from_signed(new_signed), "avg_entry_price": new_entry,
            "last_price": fill.price, "realized_pnl": fnum(pos.get("realized_pnl")) + realized_pnl,
            "unrealized_pnl": unrealized, "notional": abs(new_signed * fill.price),
            "updated_at": utc_now_iso(), "last_leader_fill_id": fill.fill_id,
        })
        return pos, before, realized_pnl

    def process_fill(self, fill: LeaderFill, cfg: LiveWalletConfig, replay_history: bool = False) -> None:
        if fill.fill_id in self.processed_ids:
            self.bump("fills_already_processed")
            return
        pos = self.get_position(cfg.wallet, fill.coin)
        before_signed = fnum(pos.get("signed_size"))
        incoming_delta_sign = trade_delta_from_side(fill.side, 1.0)
        reducing = is_reducing_position(before_signed, incoming_delta_sign)
        copy_notional_raw = self.model_copy_notional(cfg, fill)

        if cfg.mode == "OFF" or not cfg.enabled:
            self.append_reconciliation(cfg, fill, "SKIP", "OFF", "BLOCKED_OFF", before_signed, before_signed, copy_notional_raw, "wallet mode OFF")
            self.processed_ids.add(fill.fill_id)
            self.bump("fills_skipped_off")
            return
        if cfg.mode == "CLO" and not reducing:
            self.append_reconciliation(cfg, fill, "SKIP", "CLO_ENTRY_BLOCKED", "BLOCKED_CLO_ENTRY", before_signed, before_signed, copy_notional_raw, "CLO allows exits/reductions only")
            self.processed_ids.add(fill.fill_id)
            self.bump("fills_skipped_clo_entry")
            return

        intent_type = "EXIT" if reducing else "ENTRY"
        policy = "DIRECT_EXECUTABLE"
        linked_ids = [fill.fill_id]
        copy_notional = copy_notional_raw
        if intent_type == "ENTRY":
            can_order, copy_notional, linked_ids, policy = self.handle_min_notional_for_entry(cfg, fill, copy_notional_raw)
            if not can_order:
                intent_id = f"DRYRUN-{utc_now_ms()}-{len(self.processed_ids)+1}"
                copy_size = copy_notional / fill.price if fill.price > 0 else 0.0
                self.append_order_intent(cfg, fill, intent_id, linked_ids, intent_type, "MIN_NOTIONAL_ACCUMULATOR", "ACCUMULATED_NO_ORDER", copy_notional, copy_size, policy, "below $10; accumulated, no simulated order/fill")
                self.append_reconciliation(cfg, fill, "ACCUMULATE", "NO_ORDER", "ACCUMULATE", before_signed, before_signed, copy_notional, "below minimum notional")
                self.processed_ids.add(fill.fill_id)
                self.bump("fills_accumulated_no_order")
                return
        elif copy_notional < MIN_ORDER_NOTIONAL:
            intent_id = f"DRYRUN-{utc_now_ms()}-{len(self.processed_ids)+1}"
            copy_size = copy_notional / fill.price if fill.price > 0 else 0.0
            self.append_order_intent(cfg, fill, intent_id, linked_ids, intent_type, "EXIT_BELOW_MIN_NOTIONAL", "MANUAL_REVIEW", copy_notional, copy_size, "EXIT_BELOW_MIN_REVIEW", "exit/reduce notional below $10; manual reconcile in live mode")
            self.append_reconciliation(cfg, fill, "REVIEW", "EXIT_BELOW_MIN_NOTIONAL", "MANUAL_REVIEW", before_signed, before_signed, copy_notional, "exit/reduce below minimum")
            self.processed_ids.add(fill.fill_id)
            self.bump("exits_below_min_review")
            return

        copy_size = copy_notional / fill.price if fill.price > 0 else 0.0
        intent_id = f"DRYRUN-{utc_now_ms()}-{len(self.processed_ids)+1}"
        audit_notes = audit_notes_for_fill(fill, replay_history=replay_history)
        self.append_order_intent(cfg, fill, intent_id, linked_ids, intent_type, audit_reason_for_fill(fill, replay_history=replay_history), "DRY_RUN_FILLED", copy_notional, copy_size, policy, audit_notes)
        updated_pos, before, realized_pnl = self.apply_dry_run_fill_to_position(cfg, fill, copy_notional)
        after = fnum(updated_pos.get("signed_size"))
        self.append_live_fill(cfg, fill, intent_id, "DRY_RUN_FILLED", copy_notional, copy_size, updated_pos, realized_pnl, audit_notes)
        self.append_reconciliation(cfg, fill, "DRY_RUN_FILL", "MATCHED_SIMULATED", "DRY_RUN_FILL", before, after, copy_notional, "dry-run position updated")
        self.update_equity(cfg, fill)
        self.processed_ids.add(fill.fill_id)
        self.state["last_processed_fill_ts"] = max(inum(self.state.get("last_processed_fill_ts")), fill.timestamp_ms)
        self.bump("dry_run_fills_processed")

    def process_ws_fill(self, fill: LeaderFill) -> Dict[str, Any]:
        config = self.load_config()
        cfg = config.get(fill.wallet)
        if cfg is None:
            return {"processed": False, "reason": "WALLET_NOT_CONFIGURED"}
        if cfg.mode not in {"LIVE", "CLO"} or not cfg.enabled:
            return {"processed": False, "reason": "WALLET_NOT_ACTIVE"}

        baselines = self.state.setdefault("baselines", {})
        if not isinstance(baselines, dict):
            baselines = {}
            self.state["baselines"] = baselines

        baseline = baselines.get(fill.wallet)
        if not isinstance(baseline, dict):
            baselines[fill.wallet] = {
                "wallet": fill.wallet,
                "baseline_created_at": utc_now_iso(),
                "baseline_ts_ms": fill.timestamp_ms,
                "baseline_fill_id": fill.fill_id,
                "baseline_source_fill_count": 0,
                "mode": "LIVE_COPY_BASELINE",
            }
            self.persist()
            return {"processed": False, "reason": "BASELINE_CREATED_FROM_WS"}

        baseline_ts_ms = inum(baseline.get("baseline_ts_ms"))
        if fill.fill_id in self.processed_ids:
            return {"processed": False, "reason": "DUPLICATE"}
        if fill.timestamp_ms <= baseline_ts_ms:
            return {"processed": False, "reason": "WS_BEFORE_BASELINE"}

        self.process_fill(fill, cfg)
        if fill.fill_id in self.processed_ids and fill.fill_id not in self.processed_id_order:
            self.processed_id_order.append(fill.fill_id)
        baseline["baseline_ts_ms"] = fill.timestamp_ms
        baseline["baseline_fill_id"] = fill.fill_id
        self.persist()
        return {"processed": True, "reason": "LIVE_WS_DETECTED"}

    def run_once(self, replay_history: bool = False) -> Dict[str, Any]:
        config = self.load_config()
        fills = load_leader_fills()
        active_wallets = {w for w, cfg in config.items() if cfg.mode in {"LIVE", "CLO"} and cfg.enabled}
        fills_by_wallet: Dict[str, List[LeaderFill]] = {w: [] for w in active_wallets}
        for fill in fills:
            if fill.wallet in active_wallets:
                fills_by_wallet.setdefault(fill.wallet, []).append(fill)

        selected: List[LeaderFill] = []
        baseline_created = False
        baseline_wallet_count = 0
        live_poll = bool(LIVE_POLL_ENABLED and not replay_history)
        poll_attempted_wallets = 0
        poll_failed_wallets = 0
        poll_rows_fetched = 0
        poll_rows_selected = 0
        if replay_history:
            selected = [f for f in fills if f.wallet in active_wallets]
        else:
            baselines = self.state.setdefault("baselines", {})
            if not isinstance(baselines, dict):
                baselines = {}
                self.state["baselines"] = baselines
            poll_cursors = self.state.setdefault("poll_cursors", {})
            if not isinstance(poll_cursors, dict):
                poll_cursors = {}
                self.state["poll_cursors"] = poll_cursors
            for wallet in sorted(active_wallets):
                wallet_fills = fills_by_wallet.get(wallet, [])
                baseline = baselines.get(wallet)
                if not isinstance(baseline, dict):
                    latest = wallet_fills[-1] if wallet_fills else None
                    baselines[wallet] = {
                        "wallet": wallet,
                        "baseline_created_at": utc_now_iso(),
                        "baseline_ts_ms": latest.timestamp_ms if latest else 0,
                        "baseline_fill_id": latest.fill_id if latest else "",
                        "baseline_source_fill_count": len(wallet_fills),
                        "mode": "LIVE_COPY_BASELINE",
                    }
                    baseline_created = True
                    baseline_wallet_count += 1
                    continue
                baseline_ts_ms = inum(baseline.get("baseline_ts_ms"))
                if not live_poll:
                    continue

                poll_start = max(0, baseline_ts_ms - LIVE_POLL_OVERLAP_MS)
                poll_end = utc_now_ms()
                poll_attempted_wallets += 1
                raw_rows = fetch_live_fills_since(wallet, poll_start, poll_end)
                if raw_rows is None:
                    poll_failed_wallets += 1
                    cursor = poll_cursors.get(wallet)
                    if isinstance(cursor, dict):
                        cursor["last_poll_failed"] = True
                        cursor["last_poll_at"] = utc_now_iso()
                    else:
                        poll_cursors[wallet] = {"last_poll_failed": True, "last_poll_at": utc_now_iso()}
                    continue

                selected_count_for_wallet = 0
                poll_rows_fetched += len(raw_rows)
                for raw in raw_rows:
                    if not isinstance(raw, dict):
                        continue
                    fill = parse_api_leader_fill(wallet, raw)
                    if fill is None or fill.timestamp_ms <= baseline_ts_ms:
                        continue
                    selected.append(fill)
                    selected_count_for_wallet += 1
                poll_rows_selected += selected_count_for_wallet
                poll_cursors[wallet] = {
                    "last_poll_at": utc_now_iso(),
                    "last_poll_start_ms": poll_start,
                    "last_poll_end_ms": poll_end,
                    "last_poll_rows": len(raw_rows),
                    "last_poll_selected": selected_count_for_wallet,
                    "last_poll_failed": False,
                }
        self.state["last_run_started_at"] = utc_now_iso()
        self.state["configured_wallets"] = {w: asdict(c) for w, c in config.items()}
        self.state["source_ledger"] = str(RAW_LEADER_FILLS_CSV)
        self.state["source_fill_count"] = len(fills)
        self.state["selected_fill_count"] = len(selected)
        self.state["replay_history"] = bool(replay_history)
        processed_this_run = 0
        skipped_duplicate_fills = 0
        latest_processed_by_wallet: Dict[str, LeaderFill] = {}
        for fill in selected:
            cfg = config.get(fill.wallet)
            if cfg is None:
                continue
            if fill.fill_id in self.processed_ids:
                skipped_duplicate_fills += 1
                continue
            try:
                self.process_fill(fill, cfg, replay_history=replay_history)
                processed_this_run += 1
                if fill.fill_id in self.processed_ids and fill.fill_id not in self.processed_id_order:
                    self.processed_id_order.append(fill.fill_id)
                latest = latest_processed_by_wallet.get(fill.wallet)
                if latest is None or fill.timestamp_ms > latest.timestamp_ms:
                    latest_processed_by_wallet[fill.wallet] = fill
            except Exception as exc:
                self.log_error(f"process_fill leader_fill_id={fill.fill_id}", exc)
        if not replay_history:
            baselines = self.state.setdefault("baselines", {})
            for wallet, fill in latest_processed_by_wallet.items():
                baseline = baselines.get(wallet)
                if isinstance(baseline, dict):
                    baseline["baseline_ts_ms"] = fill.timestamp_ms
                    baseline["baseline_fill_id"] = fill.fill_id
        self.state["last_run_finished_at"] = utc_now_iso()
        self.state["last_run_processed"] = processed_this_run
        self.state["processed_count"] = len(self.processed_ids)
        self.state["skipped_duplicate_fills"] = skipped_duplicate_fills
        self.persist()
        return {
            "ok": True, "dry_run": True, "configured_wallets": len(config),
            "active_wallets": len(active_wallets), "source_fill_count": len(fills),
            "selected_fill_count": len(selected), "processed_this_run": processed_this_run,
            "skipped_duplicate_fills": skipped_duplicate_fills,
            "processed_id_count": len(self.processed_ids),
            "new_fill_count": max(0, len(selected) - skipped_duplicate_fills),
            "baseline_created": baseline_created,
            "baseline_wallets": baseline_wallet_count, "replay_history": bool(replay_history),
            "live_poll": live_poll,
            "poll_attempted_wallets": poll_attempted_wallets,
            "poll_failed_wallets": poll_failed_wallets,
            "poll_rows_fetched": poll_rows_fetched,
            "poll_rows_selected": poll_rows_selected,
            "audit_dir": str(AUDIT_DIR),
        }


def build_ws_health_snapshot(manager: Any) -> Dict[str, Any]:
    now = utc_now_ms()
    wallets = {}
    overall = "DISABLED"
    if manager is not None:
        overall = "OK"
        with manager.status_lock:
            items = list(manager.wallet_status.items())
        for wallet, status_item in items:
            status = dict(status_item)
            transport_ref = max(
                int(status.get("last_msg_ms") or 0),
                int(status.get("last_pong_ms") or 0),
                int(status.get("last_ping_ms") or 0),
                int(status.get("last_open_ms") or 0),
            )
            transport_stale_ms = (now - transport_ref) if transport_ref else 10**12
            last_data_ms = int(status.get("last_data_ms") or 0)
            data_stale_ms = (now - last_data_ms) if last_data_ms else 0
            next_reconnect_ms = int(status.get("next_reconnect_ms") or 0)
            last_open_ms = int(status.get("last_open_ms") or 0)
            last_close_ms = int(status.get("last_close_ms") or 0)
            status_name = str(status.get("status", "UNKNOWN"))
            effective_status = status_name
            if status_name == "OPEN" and (LIVE_WS_STALE_MS <= 0 or transport_stale_ms <= LIVE_WS_STALE_MS):
                effective_status = "OPEN"
            elif status_name == "OPEN":
                effective_status = "STALE"
            elif status_name in {"CONNECTING", "RECONNECTING"}:
                effective_status = status_name
            if last_data_ms == 0:
                data_status = "IDLE_NO_FILLS"
            elif LIVE_WS_STALE_MS > 0 and data_stale_ms > LIVE_WS_STALE_MS:
                data_status = "IDLE"
            else:
                data_status = "ACTIVE"
            if effective_status != "OPEN":
                overall = "DEGRADED"
            reconnect_in_ms = max(0, next_reconnect_ms - now) if next_reconnect_ms else 0
            uptime_ms = (now - last_open_ms) if status_name == "OPEN" and last_open_ms else int(status.get("uptime_ms") or 0)
            downtime_ms = (now - last_close_ms) if status_name in {"CLOSED", "RECONNECTING"} and last_close_ms else 0
            wallets[wallet] = {
                **status,
                "effective_status": effective_status,
                "data_status": data_status,
                "stale_ms": transport_stale_ms,
                "transport_stale_ms": transport_stale_ms,
                "data_stale_ms": data_stale_ms,
                "reconnect_in_ms": reconnect_in_ms,
                "uptime_ms": uptime_ms,
                "downtime_ms": downtime_ms,
            }
    return {
        "enabled": bool(manager is not None),
        "updated_at": utc_now_iso(),
        "overall": overall,
        "wallets": wallets,
        "max_wallets": LIVE_WS_MAX_WALLETS,
        "stale_ms_threshold": LIVE_WS_STALE_MS,
        "mode": "DEDICATED_SOCKET_PER_WALLET",
        "dry_run": True,
    }


def write_ws_health(manager: Any) -> None:
    atomic_write_json(LIVE_WS_HEALTH_FILE, build_ws_health_snapshot(manager))


class DedicatedLiveWSManager:
    def __init__(self, service: DryRunLiveCopyService, wallets: List[str]) -> None:
        self.service = service
        self.wallets = [str(wallet).lower() for wallet in wallets[:LIVE_WS_MAX_WALLETS]]
        self.stop_event = threading.Event()
        self.threads: List[threading.Thread] = []
        self.apps: Dict[str, Any] = {}
        self.wallet_status = {
            wallet: {
                "wallet": wallet,
                "status": "INIT",
                "last_open_ms": 0,
                "last_close_ms": 0,
                "last_msg_ms": 0,
                "last_data_ms": 0,
                "last_ping_ms": 0,
                "last_pong_ms": 0,
                "last_heartbeat_ms": 0,
                "last_close_status_code": "",
                "last_close_msg": "",
                "last_reconnect_attempt_ms": 0,
                "next_reconnect_ms": 0,
                "reconnect_backoff_sec": 0.0,
                "close_count": 0,
                "uptime_ms": 0,
                "downtime_ms": 0,
                "last_run_forever_return_ms": 0,
                "processed_count": 0,
                "duplicate_count": 0,
                "ignored_count": 0,
                "error_count": 0,
                "reconnect_count": 0,
                "last_error": "",
                "last_error_repr": "",
                "benign_event_count": 0,
                "last_benign_event_repr": "",
                "data_status": "IDLE_NO_FILLS",
            }
            for wallet in self.wallets
        }
        self.status_lock = threading.Lock()
        self.service_lock = threading.Lock()

    def update_status(self, wallet: str, **patch: Any) -> None:
        wallet = str(wallet).lower()
        with self.status_lock:
            status = self.wallet_status.setdefault(wallet, {"wallet": wallet})
            status.update(patch)

    def health_loop(self) -> None:
        write_ws_health(self)
        while not self.stop_event.wait(max(0.2, LIVE_WS_HEALTH_WRITE_SEC)):
            write_ws_health(self)
        write_ws_health(self)

    def start(self) -> None:
        write_ws_health(self)
        health_thread = threading.Thread(target=self.health_loop, name="live-ws-health", daemon=True)
        health_thread.start()
        self.threads.append(health_thread)
        for wallet in self.wallets:
            thread = threading.Thread(target=self.wallet_loop, args=(wallet,), name=f"live-ws-{wallet[:10]}", daemon=True)
            thread.start()
            self.threads.append(thread)

    def stop(self) -> None:
        self.stop_event.set()
        for app in list(self.apps.values()):
            try:
                app.close()
            except Exception:
                pass
        write_ws_health(self)

    def wait(self) -> None:
        try:
            while not self.stop_event.is_set():
                alive = any(thread.is_alive() for thread in self.threads)
                if not alive:
                    break
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.stop()
        finally:
            for thread in self.threads:
                thread.join(timeout=2.0)

    def wallet_loop(self, wallet: str) -> None:
        if websocket is None:
            self.update_status(wallet, status="ERROR", error_count=1, last_error="websocket-client unavailable")
            write_ws_health(self)
            return

        attempt = 0
        backoff = max(0.1, LIVE_WS_INITIAL_BACKOFF_SEC)
        while not self.stop_event.is_set():
            self.update_status(wallet, status="CONNECTING", last_error="", last_reconnect_attempt_ms=utc_now_ms())

            def on_open(app: Any) -> None:
                nonlocal backoff
                now = utc_now_ms()
                backoff = max(0.1, LIVE_WS_INITIAL_BACKOFF_SEC)
                patch = {
                    "status": "OPEN",
                    "last_open_ms": now,
                    "last_msg_ms": now,
                    "last_heartbeat_ms": now,
                    "reconnect_backoff_sec": backoff,
                    "next_reconnect_ms": 0,
                }
                self.update_status(wallet, **patch)
                app.send(json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": wallet}}))

            def on_ping(_app: Any, _message: Any) -> None:
                now = utc_now_ms()
                self.update_status(wallet, last_ping_ms=now, last_heartbeat_ms=now)

            def on_pong(_app: Any, _message: Any) -> None:
                now = utc_now_ms()
                self.update_status(wallet, last_pong_ms=now, last_heartbeat_ms=now)

            def on_message(_app: Any, message: str) -> None:
                now = utc_now_ms()
                self.update_status(wallet, last_msg_ms=now, last_heartbeat_ms=now)
                extracted = extract_ws_fills(message)
                if extracted:
                    self.update_status(wallet, last_data_ms=now)
                for msg_wallet, raw in extracted:
                    fill = parse_ws_leader_fill(msg_wallet or wallet, raw)
                    if fill is None:
                        with self.status_lock:
                            self.wallet_status[wallet]["ignored_count"] += 1
                        continue
                    with self.service_lock:
                        result = self.service.process_ws_fill(fill)
                    with self.status_lock:
                        status = self.wallet_status[wallet]
                        if result.get("processed"):
                            status["processed_count"] += 1
                        elif result.get("reason") == "DUPLICATE":
                            status["duplicate_count"] += 1
                        else:
                            status["ignored_count"] += 1

            def on_error(_app: Any, error: Any) -> None:
                with self.status_lock:
                    status = self.wallet_status[wallet]
                    if is_benign_ws_error(error):
                        status["benign_event_count"] += 1
                        status["last_benign_event_repr"] = repr(error)
                        status["last_heartbeat_ms"] = utc_now_ms()
                        return
                    status["status"] = "ERROR"
                    status["error_count"] += 1
                    status["last_error"] = str(error)
                    status["last_error_repr"] = repr(error)

            def on_close(_app: Any, close_status_code: Any, close_msg: Any) -> None:
                message = str(close_msg or "")
                if close_status_code is None and not message:
                    message = "run_forever returned/connection closed without close frame"
                with self.status_lock:
                    status = self.wallet_status[wallet]
                    status["status"] = "CLOSED"
                    status["close_count"] += 1
                    status["last_close_ms"] = utc_now_ms()
                    status["last_close_status_code"] = close_status_code
                    status["last_close_msg"] = message

            try:
                app = websocket.WebSocketApp(
                    LIVE_WS_URL,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_ping=on_ping,
                    on_pong=on_pong,
                )
                self.apps[wallet] = app
                app.run_forever(
                    ping_interval=LIVE_WS_PING_INTERVAL_SEC,
                    ping_timeout=LIVE_WS_PING_TIMEOUT_SEC,
                )
            except Exception as exc:
                with self.status_lock:
                    status = self.wallet_status[wallet]
                    status["status"] = "ERROR"
                    status["error_count"] += 1
                    status["last_error"] = str(exc)
                    status["last_error_repr"] = repr(exc)
            finally:
                self.apps.pop(wallet, None)

            if self.stop_event.is_set():
                break
            now = utc_now_ms()
            next_reconnect_ms = now + int(backoff * 1000)
            with self.status_lock:
                status = self.wallet_status[wallet]
                status["status"] = "RECONNECTING"
                status["last_run_forever_return_ms"] = now
                status["reconnect_count"] += 1
                status["reconnect_backoff_sec"] = backoff
                status["next_reconnect_ms"] = next_reconnect_ms
                if not status.get("last_close_msg"):
                    status["last_close_msg"] = "run_forever returned/connection closed without close frame"
            write_ws_health(self)
            while not self.stop_event.is_set() and utc_now_ms() < next_reconnect_ms:
                self.stop_event.wait(min(0.5, max(0.0, (next_reconnect_ms - utc_now_ms()) / 1000)))
            attempt += 1
            backoff = min(LIVE_WS_RECONNECT_BACKOFF_CAP_SEC, max(0.1, backoff * LIVE_WS_BACKOFF_MULTIPLIER))


def write_self_test_fills(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "fill_id", "wallet", "coin", "side", "price", "size", "signed_size_delta",
        "timestamp_ms", "timestamp_iso", "source", "recording_method", "raw_json",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def count_csv_data_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8") as f:
        return max(0, sum(1 for _ in csv.reader(f)) - 1)


def last_csv_row(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[-1] if rows else {}


def self_test() -> bool:
    global LIVE_POLL_ENABLED, fetch_live_fills_since

    old_paths = (
        AUDIT_DIR, RAW_LEADER_FILLS_CSV, APP_CONFIG_FILE, APPEND_ONLY_DIR,
        SERVICE_STATE_FILE, LIVE_POSITIONS_FILE, LIVE_EQUITY_HISTORY_FILE,
        LIVE_WS_HEALTH_FILE, ORDER_INTENTS_CSV, LIVE_FILLS_CSV, RECONCILIATION_CSV, ERRORS_CSV,
    )
    old_poll_enabled = LIVE_POLL_ENABLED
    old_fetch_live_fills_since = fetch_live_fills_since
    wallet = "0xabc0000000000000000000000000000000000001"
    rows = [
        {"fill_id": "hist-1", "wallet": wallet, "coin": "BTC", "side": "BUY", "price": 100, "size": 1, "timestamp_ms": 1000, "raw_json": "{}"},
        {"fill_id": "hist-2", "wallet": wallet, "coin": "BTC", "side": "SELL", "price": 101, "size": 1, "timestamp_ms": 2000, "raw_json": "{}"},
    ]
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_dir = root / "hl_live_copy_audit"
            source_fills = root / "raw_live_fills.csv"
            configure_paths(audit_dir, source_fills)
            atomic_write_json(APP_CONFIG_FILE, {
                "wallets": {
                    wallet: {
                        "mode": "LIVE",
                        "enabled": True,
                        "copy_mode": "fixed",
                        "fixed_notional": 20.0,
                        "norm_base": 100.0,
                    }
                }
            })
            write_self_test_fills(source_fills, rows)
            LIVE_POLL_ENABLED = True

            def fake_fetch(_wallet: str, start_ms: int, end_ms: int) -> Optional[List[Dict[str, Any]]]:
                out = []
                for row in rows:
                    ts = inum(row.get("timestamp_ms"))
                    if row.get("wallet") == _wallet and start_ms <= ts <= end_ms:
                        out.append({
                            "fill_id": row.get("fill_id"),
                            "coin": row.get("coin"),
                            "side": row.get("side"),
                            "price": row.get("price"),
                            "size": row.get("size"),
                            "time": ts,
                        })
                return out

            fetch_live_fills_since = fake_fetch

            first = DryRunLiveCopyService().run_once()
            if not first.get("baseline_created") or first.get("processed_this_run") != 0 or first.get("selected_fill_count") != 0:
                raise AssertionError(f"first run was not baseline-only: {first}")

            rows.append({"fill_id": "live-1", "wallet": wallet, "coin": "BTC", "side": "BUY", "price": 102, "size": 1, "timestamp_ms": 3000, "raw_json": "{}"})
            write_self_test_fills(source_fills, rows)
            second = DryRunLiveCopyService().run_once()
            if second.get("processed_this_run") != 1 or second.get("new_fill_count") != 1:
                raise AssertionError(f"second run did not process exactly one new fill: {second}")

            third = DryRunLiveCopyService().run_once()
            if third.get("processed_this_run") != 0:
                raise AssertionError(f"third run processed a duplicate fill: {third}")
            if third.get("selected_fill_count") != 0 and third.get("skipped_duplicate_fills") <= 0:
                raise AssertionError(f"third run neither had zero selection nor duplicate skip: {third}")

            before_ws_rows = count_csv_data_rows(ORDER_INTENTS_CSV)
            ws_message = json.dumps({
                "channel": "userFills",
                "data": {
                    "user": wallet,
                    "isSnapshot": False,
                    "fills": [{
                        "fill_id": "ws-1",
                        "coin": "BTC",
                        "side": "BUY",
                        "px": "103",
                        "sz": "1",
                        "time": 4000,
                    }],
                },
            })
            extracted = extract_ws_fills(ws_message)
            if len(extracted) != 1:
                raise AssertionError(f"expected one extracted WS fill, got {extracted}")
            ws_wallet, ws_raw = extracted[0]
            ws_fill = parse_ws_leader_fill(ws_wallet, ws_raw)
            if ws_fill is None:
                raise AssertionError("failed to parse WS fill")
            service = DryRunLiveCopyService()
            ws_result = service.process_ws_fill(ws_fill)
            if not ws_result.get("processed"):
                raise AssertionError(f"WS fill was not processed: {ws_result}")
            duplicate_result = service.process_ws_fill(ws_fill)
            if duplicate_result.get("processed") or duplicate_result.get("reason") != "DUPLICATE":
                raise AssertionError(f"WS duplicate was not blocked: {duplicate_result}")
            after_ws_rows = count_csv_data_rows(ORDER_INTENTS_CSV)
            if after_ws_rows - before_ws_rows != 1:
                raise AssertionError(f"expected one WS order intent row, before={before_ws_rows} after={after_ws_rows}")
            ws_intent = last_csv_row(ORDER_INTENTS_CSV)
            if ws_intent.get("reason") != "LIVE_WS_DETECTED":
                raise AssertionError(f"WS order intent reason mismatch: {ws_intent}")
            if "source=live_ws" not in ws_intent.get("notes", ""):
                raise AssertionError(f"WS order intent notes missing source label: {ws_intent}")

            class ABNF:
                __module__ = "websocket._abnf"

            if not is_benign_ws_error(ABNF()):
                raise AssertionError("ABNF websocket callback object was not classified as benign")

            manager = DedicatedLiveWSManager(service, [wallet])
            manager.update_status(wallet, status="OPEN", last_open_ms=utc_now_ms(), last_data_ms=0, benign_event_count=2)
            health_snapshot = build_ws_health_snapshot(manager)
            wallet_health = health_snapshot.get("wallets", {}).get(wallet, {})
            if wallet_health.get("effective_status") != "OPEN":
                raise AssertionError(f"quiet open WS should remain OPEN: {health_snapshot}")
            if wallet_health.get("data_status") != "IDLE_NO_FILLS":
                raise AssertionError(f"quiet open WS data status mismatch: {health_snapshot}")
            if health_snapshot.get("overall") != "OK":
                raise AssertionError(f"quiet open WS should not degrade overall: {health_snapshot}")
            if wallet_health.get("benign_event_count") != 2:
                raise AssertionError(f"WS benign event count missing from health: {health_snapshot}")
            reconnect_wallet = "0xreconnect"
            manager.update_status(
                reconnect_wallet,
                status="RECONNECTING",
                last_close_ms=utc_now_ms(),
                next_reconnect_ms=utc_now_ms() + 5000,
                reconnect_backoff_sec=5.0,
            )
            reconnect_snapshot = build_ws_health_snapshot(manager)
            reconnect_health = reconnect_snapshot.get("wallets", {}).get(reconnect_wallet, {})
            if reconnect_health.get("effective_status") != "RECONNECTING":
                raise AssertionError(f"reconnecting wallet status mismatch: {reconnect_snapshot}")
            if reconnect_health.get("reconnect_in_ms", 0) <= 0:
                raise AssertionError(f"reconnect countdown missing: {reconnect_snapshot}")
            if reconnect_snapshot.get("overall") != "DEGRADED":
                raise AssertionError(f"reconnecting wallet should degrade overall: {reconnect_snapshot}")
            write_ws_health(manager)
            health = load_json(LIVE_WS_HEALTH_FILE, {})
            if health.get("mode") != "DEDICATED_SOCKET_PER_WALLET":
                raise AssertionError(f"WS health mode mismatch: {health}")
            if wallet not in health.get("wallets", {}):
                raise AssertionError(f"WS health missing wallet: {health}")
        print("RESULT::LIVE_COPY_SERVICE_SELF_TEST_PASS")
        return True
    except Exception as exc:
        print(f"RESULT::LIVE_COPY_SERVICE_SELF_TEST_FAIL {exc}")
        return False
    finally:
        LIVE_POLL_ENABLED = old_poll_enabled
        fetch_live_fills_since = old_fetch_live_fills_since
        configure_paths(old_paths[0], old_paths[1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2 dry-run live copy service. No exchange orders.")
    parser.add_argument("--once", action="store_true", help="Run one dry-run replay cycle and exit.")
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--interval", type=float, default=5.0, help="Loop interval seconds.")
    parser.add_argument("--replay-history", action="store_true", help="Replay historical source fills instead of creating/using live-copy baselines.")
    parser.add_argument("--self-test", action="store_true", help="Run a compact temp-file live-forward self-test.")
    parser.add_argument("--ws", action="store_true", help="Run dedicated dry-run live websocket sockets for active wallets.")
    args = parser.parse_args()
    if args.self_test:
        raise SystemExit(0 if self_test() else 1)
    run_ws = bool(args.ws or LIVE_WS_ENABLED)
    if args.replay_history and run_ws:
        print(json.dumps({"ok": False, "error": "--ws cannot be combined with --replay-history"}, sort_keys=True))
        raise SystemExit(2)
    if not args.once and not args.loop and not run_ws:
        args.once = True
    if args.replay_history and not AUDIT_DIR_OVERRIDE:
        configure_paths(REPLAY_AUDIT_DIR, RAW_LEADER_FILLS_CSV)
    service = DryRunLiveCopyService()
    if run_ws:
        config = service.load_config()
        active_wallets = [
            wallet for wallet, cfg in sorted(config.items())
            if cfg.enabled and cfg.mode in {"LIVE", "CLO"}
        ][:LIVE_WS_MAX_WALLETS]
        service.run_once(replay_history=False)
        manager = DedicatedLiveWSManager(service, active_wallets)

        def handle_stop(_signum: int, _frame: Any) -> None:
            manager.stop()

        signal.signal(signal.SIGINT, handle_stop)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, handle_stop)
        print(json.dumps({
            "ok": True,
            "dry_run": True,
            "ws": True,
            "wallets": active_wallets,
            "wallet_count": len(active_wallets),
            "max_wallets": LIVE_WS_MAX_WALLETS,
            "audit_dir": str(AUDIT_DIR),
        }, indent=2, sort_keys=True), flush=True)
        manager.start()
        try:
            manager.wait()
        finally:
            manager.stop()
            service.persist()
        return
    if args.once:
        print(json.dumps(service.run_once(replay_history=args.replay_history), indent=2, sort_keys=True))
        return
    while True:
        print(json.dumps(service.run_once(replay_history=args.replay_history), sort_keys=True), flush=True)
        time.sleep(max(0.5, args.interval))


if __name__ == "__main__":
    main()
