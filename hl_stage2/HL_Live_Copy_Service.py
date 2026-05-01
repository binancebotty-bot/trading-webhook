"""
HL_Live_Copy_Service.py

Phase 2 dry-run live copy service for the Hyperliquid Copy Engine stack.

Boundary:
- NEW FILE ONLY.
- Reads app live config from hl_live_copy_audit/live_config.json.
- Reads leader fills from hl_copy_output/raw_live_fills.csv for dry-run replay/proof.
- Writes only to permanent audit files outside hl_copy_output.
- Default WS/REST paths remain dry-run only and do not place exchange orders.
- Private key use is limited to explicit manual --send-one-intent --confirm-send.
- Opens websockets only when explicitly requested with --ws / HL_LIVE_WS_ENABLED.
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
from decimal import Decimal, ROUND_DOWN
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
MANUAL_LIVE_POSITIONS_FILE = AUDIT_DIR / "manual_live_positions.json"
LIVE_EQUITY_HISTORY_FILE = AUDIT_DIR / "live_equity_history.json"
LIVE_WS_HEALTH_FILE = AUDIT_DIR / "live_ws_health.json"

ORDER_INTENTS_CSV = APPEND_ONLY_DIR / "order_intents.csv"
WOULD_SEND_ORDERS_CSV = APPEND_ONLY_DIR / "would_send_orders.csv"
SEND_ATTEMPTS_CSV = APPEND_ONLY_DIR / "send_attempts.csv"
LIVE_FILLS_CSV = APPEND_ONLY_DIR / "live_fills.csv"
RECONCILIATION_CSV = APPEND_ONLY_DIR / "reconciliation.csv"
ERRORS_CSV = APPEND_ONLY_DIR / "errors.csv"


def load_local_env_file(path: Optional[Path] = None) -> None:
    """Load KEY=VALUE pairs from a local .env file into os.environ.

    Skips blank lines, comment lines (#), and malformed lines.
    Never overwrites a key that is already present in os.environ.
    Never prints loaded values.
    """
    if path is None:
        path = BASE_DIR.parent / "hl_stage2.env"
    try:
        env_path = Path(path)
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if not key:
                continue
            if key not in os.environ:
                os.environ[key] = value.strip()
    except Exception:
        pass


load_local_env_file()

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
LIVE_WS_RECONNECT_GRACE_MS = int(os.getenv("HL_LIVE_WS_RECONNECT_GRACE_MS", "10000"))
LIVE_WS_THREAD_WATCHDOG_SEC = float(os.getenv("HL_LIVE_WS_THREAD_WATCHDOG_SEC", "5"))
LIVE_WS_PROACTIVE_RECYCLE_SEC = float(os.getenv("HL_LIVE_WS_PROACTIVE_RECYCLE_SEC", "50"))
LIVE_WS_PROACTIVE_RECYCLE_ENABLED = os.getenv("HL_LIVE_WS_PROACTIVE_RECYCLE_ENABLED", "1") == "1"
LIVE_WS_SNAPSHOT_RECOVERY_GRACE_MS = int(os.getenv("HL_LIVE_WS_SNAPSHOT_RECOVERY_GRACE_MS", "120000"))
LIVE_QUOTE_CACHE_TTL_MS = int(os.getenv("HL_LIVE_QUOTE_CACHE_TTL_MS", "1000"))
LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD = float(os.getenv("HL_LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD", "25"))
LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT = float(os.getenv("HL_LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT", "0.25"))
LIVE_ORDER_ENDPOINT = os.getenv("HL_LIVE_ORDER_ENDPOINT", "https://api.hyperliquid.xyz/exchange")
LIVE_JSON_WRITE_LOCK = threading.RLock()
HL_PERP_META_BY_COIN_CACHE: Optional[Dict[str, Dict[str, Any]]] = None

ORDER_INTENT_FIELDS = [
    "created_at", "intent_id", "dry_run", "leader_wallet", "leader_fill_id",
    "linked_leader_fill_ids", "mode", "copy_mode", "coin", "side",
    "intent_type", "reason", "status", "leader_price", "target_price",
    "leader_size", "leader_notional", "copy_notional", "copy_size",
    "min_notional_policy", "diff_pct", "max_diff_pct", "daily_loss_limit", "notes",
    "execution_decision", "decision_reason", "executable_price", "adverse_diff_pct",
    "suggested_order_type", "suggested_limit_price", "manual_reconcile_required",
    "market_data_source", "market_data_error",
]

WOULD_SEND_ORDER_FIELDS = [
    "created_at", "dry_run", "intent_id", "leader_wallet", "leader_fill_id",
    "source_reason", "execution_decision", "decision_reason", "coin", "side",
    "copy_size", "copy_notional", "order_type", "limit_price", "reduce_only",
    "manual_reconcile_required", "status", "notes",
]

SEND_ATTEMPT_FIELDS = [
    "created_at", "intent_id", "leader_wallet", "coin", "side", "order_type",
    "limit_price", "copy_size", "copy_notional", "reduce_only", "confirmed",
    "status", "response", "error", "notes",
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
    global LIVE_POSITIONS_FILE, MANUAL_LIVE_POSITIONS_FILE, LIVE_EQUITY_HISTORY_FILE, LIVE_WS_HEALTH_FILE, ORDER_INTENTS_CSV, WOULD_SEND_ORDERS_CSV, SEND_ATTEMPTS_CSV
    global LIVE_FILLS_CSV, RECONCILIATION_CSV, ERRORS_CSV, RAW_LEADER_FILLS_CSV

    AUDIT_DIR = Path(audit_dir)
    RAW_LEADER_FILLS_CSV = Path(source_fills)
    APP_CONFIG_FILE = AUDIT_DIR / "live_config.json"
    APPEND_ONLY_DIR = AUDIT_DIR / "append_only"
    SERVICE_STATE_FILE = AUDIT_DIR / "live_service_state.json"
    LIVE_POSITIONS_FILE = AUDIT_DIR / "live_positions.json"
    MANUAL_LIVE_POSITIONS_FILE = AUDIT_DIR / "manual_live_positions.json"
    LIVE_EQUITY_HISTORY_FILE = AUDIT_DIR / "live_equity_history.json"
    LIVE_WS_HEALTH_FILE = AUDIT_DIR / "live_ws_health.json"
    ORDER_INTENTS_CSV = APPEND_ONLY_DIR / "order_intents.csv"
    WOULD_SEND_ORDERS_CSV = APPEND_ONLY_DIR / "would_send_orders.csv"
    SEND_ATTEMPTS_CSV = APPEND_ONLY_DIR / "send_attempts.csv"
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


def safe_atomic_write_json(path: Path, payload: Any, context: str = "JSON_WRITE") -> bool:
    delays = [0.05, 0.15, 0.35]
    with LIVE_JSON_WRITE_LOCK:
        for attempt in range(4):
            try:
                atomic_write_json(path, payload)
                return True
            except (PermissionError, OSError):
                if attempt >= 3:
                    return False
                time.sleep(delays[attempt])
            except Exception:
                return False
    return False


def ensure_csv_header(path: Path, fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def ensure_csv_schema(path: Path, fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if not path.exists() or path.stat().st_size <= 0:
            with path.open("w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=fieldnames).writeheader()
            return
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            current = list(reader.fieldnames or [])
            if all(name in current for name in fieldnames):
                return
            rows = list(reader)
        backup = path.with_name(f"{path.name}.schema_backup_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}")
        backup.write_bytes(path.read_bytes())
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({name: row.get(name, "") for name in fieldnames})
    except Exception as exc:
        if path != ERRORS_CSV:
            try:
                ensure_csv_header(ERRORS_CSV, ERROR_FIELDS)
                with ERRORS_CSV.open("a", newline="", encoding="utf-8") as f:
                    csv.DictWriter(f, fieldnames=ERROR_FIELDS).writerow({
                        "created_at": utc_now_iso(),
                        "context": "CSV_SCHEMA_UPGRADE",
                        "error_type": type(exc).__name__,
                        "message": f"path={path} err={exc}",
                        "traceback": traceback.format_exc(),
                    })
            except Exception:
                pass


def append_csv(path: Path, fieldnames: List[str], row: Dict[str, Any]) -> None:
    ensure_csv_schema(path, fieldnames)
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writerow({k: row.get(k, "") for k in fieldnames})


def load_csv_ids(path: Path, id_field: str) -> set:
    if not path.exists() or path.stat().st_size <= 0:
        return set()
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            return {str(row.get(id_field, "")).strip() for row in csv.DictReader(f) if str(row.get(id_field, "")).strip()}
    except Exception:
        return set()


def should_write_would_send(decision: Dict[str, Any], row: Dict[str, Any]) -> bool:
    execution_decision = str((decision or {}).get("execution_decision") or row.get("execution_decision") or "").strip()
    return execution_decision in {"WOULD_PLACE_IOC_LIMIT", "WOULD_LATE_COPY", "WOULD_REDUCE_OR_EXIT"}


def load_would_send_order(intent_id: str) -> Optional[Dict[str, Any]]:
    wanted = str(intent_id or "").strip()
    if not wanted or not WOULD_SEND_ORDERS_CSV.exists():
        return None
    try:
        with WOULD_SEND_ORDERS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if str(row.get("intent_id", "")).strip() == wanted:
                    return dict(row)
    except Exception:
        return None
    return None


def truthy_csv(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def append_send_attempt(intent_row: Dict[str, Any], status: str, confirmed: bool = False, response: Any = "", error: str = "", notes: str = "") -> None:
    append_csv(SEND_ATTEMPTS_CSV, SEND_ATTEMPT_FIELDS, {
        "created_at": utc_now_iso(),
        "intent_id": intent_row.get("intent_id", ""),
        "leader_wallet": intent_row.get("leader_wallet", ""),
        "coin": intent_row.get("coin", ""),
        "side": intent_row.get("side", ""),
        "order_type": intent_row.get("order_type", ""),
        "limit_price": intent_row.get("limit_price", ""),
        "copy_size": intent_row.get("copy_size", ""),
        "copy_notional": intent_row.get("copy_notional", ""),
        "reduce_only": intent_row.get("reduce_only", ""),
        "confirmed": bool(confirmed),
        "status": status,
        "response": json.dumps(response, sort_keys=True) if isinstance(response, (dict, list)) else response,
        "error": error,
        "notes": notes,
    })


def load_manual_live_positions() -> Dict[str, Any]:
    data = load_json(MANUAL_LIVE_POSITIONS_FILE, {})
    return data if isinstance(data, dict) else {}


def save_manual_live_positions(positions: Dict[str, Any]) -> bool:
    return safe_atomic_write_json(MANUAL_LIVE_POSITIONS_FILE, positions, "MANUAL_LIVE_POSITIONS_WRITE")


def update_manual_live_position_from_fill(coin: str, side: str, fill_size: Any, oid: Any, intent_id: str) -> Dict[str, Any]:
    positions = load_manual_live_positions()
    key = str(coin or "").upper().strip()
    current = positions.get(key, {}) if isinstance(positions.get(key), dict) else {}
    before = fnum(current.get("signed_size"))
    delta = fnum(fill_size) if str(side).upper() == "BUY" else -fnum(fill_size)
    after = before + delta
    if abs(after) < 1e-9:
        after = 0.0
    positions[key] = {
        "signed_size": after,
        "last_updated_at": utc_now_iso(),
        "last_oid": str(oid or ""),
        "last_intent_id": str(intent_id or ""),
    }
    save_manual_live_positions(positions)
    return {"coin": key, "position_before": before, "position_after": after}


def wire_safe_float(value: Any, kind: str) -> Tuple[bool, float, str]:
    try:
        from hyperliquid.utils.signing import float_to_wire  # type: ignore
        original = Decimal(str(value))
        if original <= 0:
            return False, 0.0, f"{kind} must be positive"
        try:
            float_to_wire(float(original))
            return True, float(original), ""
        except ValueError:
            pass
        for places in range(8, -1, -1):
            quantum = Decimal("1").scaleb(-places)
            candidate = original.quantize(quantum, rounding=ROUND_DOWN)
            if candidate <= 0 or candidate > original:
                continue
            try:
                float_to_wire(float(candidate))
                return True, float(candidate), ""
            except ValueError:
                continue
        return False, 0.0, f"{kind} is not wire-safe after rounding down"
    except Exception as exc:
        return False, 0.0, f"{kind} wire conversion failed: {exc!r}"


def get_hl_perp_meta_by_coin() -> Dict[str, Dict[str, Any]]:
    global HL_PERP_META_BY_COIN_CACHE
    if HL_PERP_META_BY_COIN_CACHE is not None:
        return HL_PERP_META_BY_COIN_CACHE
    if requests is None:
        HL_PERP_META_BY_COIN_CACHE = {}
        return HL_PERP_META_BY_COIN_CACHE
    try:
        response = requests.post("https://api.hyperliquid.xyz/info", json={"type": "meta"}, timeout=8)
        data = response.json()
        universe = data.get("universe") if isinstance(data, dict) else None
        out: Dict[str, Dict[str, Any]] = {}
        if isinstance(universe, list):
            for item in universe:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).upper().strip()
                if not name:
                    continue
                out[name] = {"szDecimals": inum(item.get("szDecimals")), "name": name}
        HL_PERP_META_BY_COIN_CACHE = out
        return out
    except Exception:
        HL_PERP_META_BY_COIN_CACHE = {}
        return HL_PERP_META_BY_COIN_CACHE


def floor_decimal_to_places(value: Any, places: int) -> Decimal:
    quant = Decimal("1").scaleb(-max(0, int(places)))
    return Decimal(str(value)).quantize(quant, rounding=ROUND_DOWN)


def round_price_hl_perp(price: Any, sz_decimals: int) -> Decimal:
    max_decimals = max(0, 6 - int(sz_decimals))
    candidate = floor_decimal_to_places(price, max_decimals)
    if candidate <= 0:
        return candidate
    sig_quant = Decimal("1e{}".format(candidate.adjusted() - 4))
    candidate = candidate.quantize(sig_quant, rounding=ROUND_DOWN)
    return floor_decimal_to_places(candidate, max_decimals)


def response_has_order_error(response: Any) -> bool:
    return not parse_hl_order_status(response).get("ok", False)


def parse_hl_order_status(response: Any) -> Dict[str, Any]:
    if not isinstance(response, dict):
        return {"ok": False, "status": "ORDER_UNKNOWN"}
    statuses = (((response.get("response") or {}).get("data") or {}).get("statuses"))
    if isinstance(statuses, list):
        for item in statuses:
            if isinstance(item, dict) and item.get("error"):
                return {"ok": False, "status": "ORDER_REJECTED", "error": str(item.get("error"))}
        for item in statuses:
            filled = item.get("filled") if isinstance(item, dict) else None
            if isinstance(filled, dict):
                return {
                    "ok": True,
                    "status": "ORDER_FILLED",
                    "fill_avg_px": filled.get("avgPx", ""),
                    "fill_size": filled.get("totalSz", ""),
                    "oid": filled.get("oid", ""),
                }
        for item in statuses:
            resting = item.get("resting") if isinstance(item, dict) else None
            if isinstance(resting, dict):
                return {"ok": True, "status": "ORDER_RESTING", "oid": resting.get("oid", "")}
    if response.get("status") == "err":
        return {"ok": False, "status": "ORDER_REJECTED", "error": str(response.get("response") or response)}
    return {"ok": False, "status": "ORDER_UNKNOWN"}


def prepare_hl_order_numbers(coin: str, size: Any, limit_px: Any, max_notional: float) -> Dict[str, Any]:
    meta = get_hl_perp_meta_by_coin()
    item = meta.get(str(coin or "").upper().strip())
    if not item:
        return {"ok": False, "status": "META_UNAVAILABLE", "error": f"missing perp meta for {coin}"}
    try:
        from hyperliquid.utils.signing import float_to_wire  # type: ignore
        sz_decimals = inum(item.get("szDecimals"))
        original_size = Decimal(str(size))
        original_limit_price = Decimal(str(limit_px))
        wire_size_dec = floor_decimal_to_places(original_size, sz_decimals)
        wire_limit_dec = round_price_hl_perp(original_limit_price, sz_decimals)
        if wire_size_dec <= 0 or wire_limit_dec <= 0:
            return {
                "ok": False,
                "status": "WIRE_NUMBER_UNSAFE",
                "error": "size/price rounded to non-positive value",
                "szDecimals": sz_decimals,
                "original_size": float(original_size),
                "original_limit_price": float(original_limit_price),
            }
        wire_notional_dec = wire_size_dec * wire_limit_dec
        if wire_notional_dec < Decimal("10.0"):
            return {
                "ok": False,
                "status": "MIN_NOTIONAL_AFTER_ROUNDING",
                "szDecimals": sz_decimals,
                "original_size": float(original_size),
                "wire_size": float(wire_size_dec),
                "original_limit_price": float(original_limit_price),
                "wire_limit_price": float(wire_limit_dec),
                "wire_notional": float(wire_notional_dec),
            }
        if wire_notional_dec > Decimal(str(max_notional)):
            return {
                "ok": False,
                "status": "MAX_NOTIONAL_EXCEEDED",
                "cap": max_notional,
                "copy_notional": float(wire_notional_dec),
                "szDecimals": sz_decimals,
                "original_size": float(original_size),
                "wire_size": float(wire_size_dec),
                "original_limit_price": float(original_limit_price),
                "wire_limit_price": float(wire_limit_dec),
                "wire_notional": float(wire_notional_dec),
            }
        float_to_wire(float(wire_size_dec))
        float_to_wire(float(wire_limit_dec))
        return {
            "ok": True,
            "status": "OK",
            "szDecimals": sz_decimals,
            "original_size": float(original_size),
            "wire_size": float(wire_size_dec),
            "original_limit_price": float(original_limit_price),
            "wire_limit_price": float(wire_limit_dec),
            "wire_notional": float(wire_notional_dec),
        }
    except Exception as exc:
        return {"ok": False, "status": "WIRE_NUMBER_UNSAFE", "error": repr(exc)}


def send_hyperliquid_order(payload: Dict[str, Any], marketable_bps: float = 0.0, use_current_quote: bool = False, max_notional: Optional[float] = None) -> Dict[str, Any]:
    coin = str(payload.get("coin", "")).upper().strip()
    side = str(payload.get("side", "")).upper().strip()
    size = fnum(payload.get("copy_size"))
    limit_px = fnum(payload.get("limit_price"))
    reduce_only = bool(payload.get("reduce_only"))
    marketable_bps = fnum(marketable_bps)
    if marketable_bps < 0:
        return {"ok": False, "status": "MARKETABLE_BPS_INVALID", "marketable_bps": marketable_bps}
    if marketable_bps > 20:
        return {"ok": False, "status": "MARKETABLE_BPS_TOO_HIGH", "marketable_bps": marketable_bps}
    adjusted_limit_px = limit_px
    if marketable_bps > 0:
        multiplier = 1 + (marketable_bps / 10000.0) if side == "BUY" else 1 - (marketable_bps / 10000.0)
        adjusted_limit_px = limit_px * multiplier
    private_key = os.getenv("HL_LIVE_HL_PRIVATE_KEY", "").strip()
    if not private_key:
        return {"ok": False, "status": "CREDENTIALS_MISSING"}
    try:
        from eth_account import Account  # type: ignore
        from hyperliquid.exchange import Exchange  # type: ignore
    except Exception as exc:
        return {"ok": False, "status": "SDK_UNAVAILABLE", "error": repr(exc)}

    try:
        _effective_max_notional = max_notional if max_notional is not None else LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD
        prepared = prepare_hl_order_numbers(coin, size, adjusted_limit_px, _effective_max_notional)
        if not prepared.get("ok"):
            _out = {"ok": False, "marketable_bps": marketable_bps, "adjusted_limit_price": adjusted_limit_px, **prepared}
            if prepared.get("status") == "MAX_NOTIONAL_EXCEEDED":
                _out["notional_cap_reason"] = "manual_open_cap"
            return _out
        wire_size = fnum(prepared.get("wire_size"))
        wire_limit_px = fnum(prepared.get("wire_limit_price"))
        account = Account.from_key(private_key)
        base_url = str(LIVE_ORDER_ENDPOINT or "").strip() or "https://api.hyperliquid.xyz/exchange"
        if base_url.endswith("/exchange"):
            base_url = base_url[: -len("/exchange")]
        account_address = os.getenv("HL_LIVE_HL_ACCOUNT_ADDRESS", "").strip() or None
        exchange = Exchange(account, base_url=base_url, account_address=account_address)
        response = exchange.order(
            coin,
            side == "BUY",
            wire_size,
            wire_limit_px,
            {"limit": {"tif": "Ioc"}},
            reduce_only=reduce_only,
        )
        parsed_status = parse_hl_order_status(response)
        return {
            "ok": bool(parsed_status.get("ok")),
            "status": parsed_status.get("status", "ORDER_UNKNOWN"),
            "response": response,
            "fill_avg_px": parsed_status.get("fill_avg_px", ""),
            "fill_size": parsed_status.get("fill_size", ""),
            "oid": parsed_status.get("oid", ""),
            "error": parsed_status.get("error", ""),
            "coin": coin,
            "side": side,
            "size": wire_size,
            "limit_price": wire_limit_px,
            "marketable_bps": marketable_bps,
            "adjusted_limit_price": adjusted_limit_px,
            "use_current_quote": bool(use_current_quote),
            "price_source": payload.get("price_source", "intent_price"),
            "current_quote_price": payload.get("current_quote_price", ""),
            "szDecimals": prepared.get("szDecimals"),
            "original_size": prepared.get("original_size"),
            "wire_size": prepared.get("wire_size"),
            "original_limit_price": limit_px,
            "wire_limit_price": prepared.get("wire_limit_price"),
            "wire_notional": prepared.get("wire_notional"),
            "reduce_only": reduce_only,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": "ORDER_REJECTED",
            "error": repr(exc),
            "coin": coin,
            "side": side,
            "size": size,
            "limit_price": limit_px,
            "marketable_bps": marketable_bps,
            "adjusted_limit_price": adjusted_limit_px,
            "use_current_quote": bool(use_current_quote),
            "price_source": payload.get("price_source", "intent_price"),
            "current_quote_price": payload.get("current_quote_price", ""),
            "original_size": size,
            "original_limit_price": limit_px,
            "reduce_only": reduce_only,
        }


def manual_send_one_intent(intent_id: str, confirm_send: bool, marketable_bps: float = 0.0, close_position: bool = False, use_current_quote: bool = False) -> Dict[str, Any]:
    ensure_csv_schema(SEND_ATTEMPTS_CSV, SEND_ATTEMPT_FIELDS)
    row = load_would_send_order(intent_id)
    if row is None:
        missing = {"intent_id": intent_id}
        append_send_attempt(missing, "INTENT_NOT_FOUND", confirmed=confirm_send, notes="would-send intent_id not found")
        return {"ok": False, "status": "INTENT_NOT_FOUND", "intent_id": intent_id}

    allowed = {"WOULD_PLACE_IOC_LIMIT", "WOULD_LATE_COPY", "WOULD_REDUCE_OR_EXIT"}
    errors = []
    if str(row.get("execution_decision", "")).strip() not in allowed:
        errors.append("execution_decision")
    if str(row.get("status", "")).strip() != "WOULD_SEND_DRY_RUN":
        errors.append("status")
    if fnum(row.get("limit_price")) <= 0:
        errors.append("limit_price")
    if fnum(row.get("copy_size")) <= 0:
        errors.append("copy_size")
    if str(row.get("order_type", "")).strip() != "IOC_LIMIT":
        errors.append("order_type")
    if truthy_csv(row.get("manual_reconcile_required")):
        errors.append("manual_reconcile_required")
    if errors:
        error = "REFUSED_INVALID_WOULD_SEND_ROW:" + ",".join(errors)
        append_send_attempt(row, "INTENT_REFUSED", confirmed=confirm_send, error=error, notes="manual one-shot send refused by payload validation")
        return {"ok": False, "status": "INTENT_REFUSED", "intent_id": intent_id, "error": error, "row": row}

    payload = {
        "coin": row.get("coin", ""),
        "side": row.get("side", ""),
        "order_type": row.get("order_type", ""),
        "limit_price": fnum(row.get("limit_price")),
        "copy_size": fnum(row.get("copy_size")),
        "copy_notional": fnum(row.get("copy_notional")),
        "reduce_only": truthy_csv(row.get("reduce_only")),
    }
    positions = load_manual_live_positions()
    coin_key = str(payload.get("coin", "")).upper().strip()
    position_before = fnum((positions.get(coin_key) or {}).get("signed_size")) if isinstance(positions.get(coin_key), dict) else 0.0
    size_source = "payload"
    if close_position:
        if position_before == 0:
            append_send_attempt(row, "NO_MANUAL_POSITION_TO_CLOSE", confirmed=confirm_send, error="manual live position is flat")
            return {"ok": False, "status": "NO_MANUAL_POSITION_TO_CLOSE", "intent_id": intent_id, "position_before": position_before, "close_position": True}
        payload["side"] = "SELL" if position_before > 0 else "BUY"
        payload["copy_size"] = abs(position_before)
        payload["reduce_only"] = True
        size_source = "manual_position_close"
        if use_current_quote:
            quote = fetch_public_executable_quote(str(payload.get("coin", "")), str(payload.get("side", "")))
            if not quote.get("ok") or fnum(quote.get("executable_price")) <= 0:
                append_send_attempt(row, "CURRENT_QUOTE_UNAVAILABLE", confirmed=confirm_send, error=str(quote.get("error") or "quote unavailable"))
                return {"ok": False, "status": "CURRENT_QUOTE_UNAVAILABLE", "intent_id": intent_id, "quote": quote, "position_before": position_before, "close_position": True}
            payload["limit_price"] = fnum(quote.get("executable_price"))
            payload["current_quote_price"] = fnum(quote.get("executable_price"))
            payload["price_source"] = "current_quote"
    elif payload.get("reduce_only") and position_before != 0:
        current_abs = abs(position_before)
        if fnum(payload.get("copy_size")) > current_abs:
            payload["copy_size"] = current_abs
            size_source = "manual_position_cap"
        payload["reduce_only"] = True
    elif payload.get("reduce_only"):
        payload["position_note"] = "no tracked manual live position; using payload size"
    payload.setdefault("price_source", "intent_price")
    payload["use_current_quote"] = bool(use_current_quote and close_position)
    payload.setdefault("current_quote_price", "")
    payload["copy_notional"] = fnum(payload.get("copy_size")) * fnum(payload.get("limit_price"))
    payload["position_before"] = position_before
    payload["close_position"] = bool(close_position)
    payload["size_source"] = size_source
    is_close_guard_path = (
        bool(close_position)
        and bool(payload.get("reduce_only"))
        and size_source == "manual_position_close"
    )
    marketable_bps = fnum(marketable_bps)
    if marketable_bps < 0:
        append_send_attempt(row, "MARKETABLE_BPS_INVALID", confirmed=confirm_send, error="marketable_bps must be >= 0")
        return {"ok": False, "status": "MARKETABLE_BPS_INVALID", "intent_id": intent_id, "marketable_bps": marketable_bps}
    if marketable_bps > 20:
        append_send_attempt(row, "MARKETABLE_BPS_TOO_HIGH", confirmed=confirm_send, error="marketable_bps must be <= 20")
        return {"ok": False, "status": "MARKETABLE_BPS_TOO_HIGH", "intent_id": intent_id, "marketable_bps": marketable_bps}
    payload["marketable_bps"] = marketable_bps
    adjusted_limit_price = fnum(payload.get("limit_price"))
    if marketable_bps > 0:
        side = str(payload.get("side", "")).upper()
        multiplier = 1 + (marketable_bps / 10000.0) if side == "BUY" else 1 - (marketable_bps / 10000.0)
        adjusted_limit_price *= multiplier
    payload["adjusted_limit_price"] = adjusted_limit_price
    if is_close_guard_path:
        payload["close_notional_cap_bypassed"] = True
        payload["notional_cap_reason"] = "manual_close_diff_guard"
        _prep = prepare_hl_order_numbers(
            str(payload.get("coin", "")).upper().strip(),
            fnum(payload.get("copy_size")),
            adjusted_limit_price,
            1e12,
        )
        if _prep.get("ok"):
            _side = str(payload.get("side", "")).upper()
            _ref = (
                fnum(payload.get("current_quote_price"))
                if str(payload.get("price_source", "")) == "current_quote"
                else fnum(payload.get("limit_price"))
            )
            _wire = fnum(_prep.get("wire_limit_price"))
            if _side == "BUY":
                _adv = max(0.0, (_wire - _ref) / _ref * 100.0) if _ref > 0 else 0.0
            else:
                _adv = max(0.0, (_ref - _wire) / _ref * 100.0) if _ref > 0 else 0.0
            payload["close_adverse_diff_pct"] = _adv
            payload["close_adverse_diff_limit_pct"] = LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT
            if _adv > LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT:
                append_send_attempt(
                    row, "CLOSE_ADVERSE_DIFF_TOO_LARGE", confirmed=confirm_send,
                    error=f"close_adverse_diff_pct={_adv:.4f} > limit={LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT}",
                    notes="close adverse diff guard rejected",
                )
                return {
                    "ok": False,
                    "status": "CLOSE_ADVERSE_DIFF_TOO_LARGE",
                    "intent_id": intent_id,
                    "close_adverse_diff_pct": _adv,
                    "close_adverse_diff_limit_pct": LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT,
                    "close_notional_cap_bypassed": True,
                    "notional_cap_reason": "manual_close_diff_guard",
                    "payload": payload,
                }
        # META_UNAVAILABLE / MIN_NOTIONAL / WIRE_UNSAFE: skip adverse diff guard;
        # send_hyperliquid_order will apply its own checks at send time.
    if not confirm_send:
        append_send_attempt(row, "CONFIRM_REQUIRED", confirmed=False, response={"payload": payload}, notes="pass --confirm-send to reach disabled sender placeholder")
        return {"ok": False, "status": "CONFIRM_REQUIRED", "intent_id": intent_id, "payload": payload}

    result = send_hyperliquid_order(
        payload,
        marketable_bps=marketable_bps,
        use_current_quote=bool(use_current_quote and close_position),
        max_notional=1e12 if is_close_guard_path else None,
    )
    status = str(result.get("status") or "ORDER_SENDER_NOT_IMPLEMENTED")
    position_after = position_before
    if status == "ORDER_FILLED":
        updated = update_manual_live_position_from_fill(
            str(payload.get("coin", "")),
            str(payload.get("side", "")),
            result.get("fill_size") or result.get("size"),
            result.get("oid"),
            intent_id,
        )
        position_after = fnum(updated.get("position_after"), position_before)
    result.update({"position_before": position_before, "position_after": position_after, "close_position": bool(close_position), "size_source": size_source})
    append_send_attempt(row, status, confirmed=True, response=result, notes="manual one-shot sender result")
    return {"ok": bool(result.get("ok")), "status": status, "intent_id": intent_id, "payload": payload, "response": result}


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


def extract_ws_fills_with_meta(message: str) -> List[Tuple[str, Dict[str, Any], bool]]:
    try:
        msg = json.loads(message)
    except Exception:
        return []

    data = msg.get("data", msg) if isinstance(msg, dict) else msg
    out: List[Tuple[str, Dict[str, Any], bool]] = []

    if isinstance(data, dict):
        is_snapshot = bool(data.get("isSnapshot"))
        wallet = str(data.get("user") or data.get("wallet") or "").lower()
        fills = data.get("fills") or data.get("userFills") or []
        if isinstance(fills, list):
            for item in fills:
                if isinstance(item, dict):
                    out.append((wallet or str(item.get("user") or item.get("wallet") or "").lower(), item, is_snapshot))
        return out

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                wallet = str(item.get("user") or item.get("wallet") or "").lower()
                out.append((wallet, item, False))
    return out


def extract_ws_fills(message: str) -> List[Tuple[str, Dict[str, Any]]]:
    return [(wallet, raw) for wallet, raw, _is_snapshot in extract_ws_fills_with_meta(message)]


def classify_ignored_ws_message(message: str) -> str:
    try:
        msg = json.loads(message)
    except Exception:
        return "JSON_ERROR"
    data = msg.get("data", msg) if isinstance(msg, dict) else msg
    if isinstance(data, dict):
        if bool(data.get("isSnapshot")):
            return "SNAPSHOT"
        if any(k in data for k in ("fills", "userFills")):
            fills = data.get("fills") if "fills" in data else data.get("userFills")
            if isinstance(fills, list) and not fills:
                return "EMPTY_FILLS"
        channel = str(msg.get("channel") or data.get("channel") or "").lower() if isinstance(msg, dict) else ""
        if "subscription" in msg or "subscription" in data or "userfills" in channel:
            return "SUBSCRIPTION_ACK"
        return "PARSE_EMPTY"
    if isinstance(data, list) and not data:
        return "EMPTY_FILLS"
    return "PARSE_EMPTY"


def ws_message_preview(message: Any, limit: int = 300) -> str:
    try:
        return str(message)[:limit]
    except Exception:
        return "<unprintable>"


def is_ws_abnf_frame(obj: Any) -> bool:
    name = type(obj).__name__
    module = type(obj).__module__
    text = repr(obj)
    if "websocket._abnf" in module:
        return True
    if name == "ABNF":
        return True
    if text.startswith("<websocket._abnf.ABNF object"):
        return True
    return False


def ws_abnf_details(frame: Any) -> Dict[str, Any]:
    data = getattr(frame, "data", b"")
    opcode = getattr(frame, "opcode", "")
    details: Dict[str, Any] = {
        "type_name": type(frame).__name__,
        "module": type(frame).__module__,
        "repr": repr(frame),
        "opcode": opcode,
        "fin": getattr(frame, "fin", ""),
        "data_len": "",
        "close_code": "",
        "close_reason": "",
    }
    if isinstance(data, (bytes, bytearray, str)):
        details["data_len"] = len(data)
    try:
        if opcode == 8 or (isinstance(data, (bytes, bytearray)) and len(data) >= 2):
            if isinstance(data, (bytes, bytearray)) and len(data) >= 2:
                details["close_code"] = int.from_bytes(data[:2], "big")
                details["close_reason"] = bytes(data[2:]).decode("utf-8", errors="replace")
            elif isinstance(data, str):
                details["close_reason"] = data
    except Exception:
        pass
    return details


def is_benign_ws_error(err: Any) -> bool:
    return is_ws_abnf_frame(err)


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


def parse_ws_snapshot_leader_fill(wallet: str, raw: Dict[str, Any]) -> Optional[LeaderFill]:
    fill = parse_ws_leader_fill(wallet, raw)
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
        source="live_ws_snapshot",
        recording_method="WS_SNAPSHOT_RECOVERY",
        raw=fill.raw,
    )


def should_recover_ws_snapshot_fill(service: Any, wallet_status: Dict[str, Any], wallet: str, fill: LeaderFill) -> str:
    wallet = str(wallet or fill.wallet or "").lower()
    baselines = getattr(service, "state", {}).get("baselines", {})
    baseline = baselines.get(wallet) if isinstance(baselines, dict) else None
    if not isinstance(baseline, dict):
        return "PREBASELINE"
    if fill.timestamp_ms <= inum(baseline.get("baseline_ts_ms")):
        return "PREBASELINE"
    if fill.fill_id in getattr(service, "processed_ids", set()):
        return "DUPLICATE"
    last_open_ms = inum((wallet_status or {}).get("last_open_ms"))
    if last_open_ms and fill.timestamp_ms < last_open_ms - LIVE_WS_SNAPSHOT_RECOVERY_GRACE_MS:
        return "OLD"
    return "RECOVER"


def audit_reason_for_fill(fill: LeaderFill, replay_history: bool = False) -> str:
    if replay_history:
        return "REPLAY_HISTORY"
    source = str(getattr(fill, "source", "") or "").lower()
    if source == "live_poll":
        return "LIVE_POLL_DETECTED"
    if source == "live_ws_snapshot":
        return "LIVE_WS_SNAPSHOT_RECOVERY"
    if source in {"ws", "live_ws"}:
        return "LIVE_WS_DETECTED"
    return "SOURCE_CSV_FORWARD"


def audit_notes_for_fill(fill: LeaderFill, replay_history: bool = False) -> str:
    if replay_history:
        return "dry-run replay; source=replay_history; no exchange order placed"
    source = str(getattr(fill, "source", "") or "").lower()
    if source == "live_poll":
        return f"dry-run simulated fill; source=live_poll; no exchange order placed; ws_coverage={ws_coverage_for_fill(fill)}"
    if source == "live_ws_snapshot":
        return "dry-run simulated fill; source=live_ws_snapshot; guarded snapshot recovery; no exchange order placed"
    if source in {"ws", "live_ws"}:
        return "dry-run simulated fill; source=live_ws; no exchange order placed"
    return "dry-run simulated fill; source=source_csv_forward; no exchange order placed"


def executable_price_from_fill_payload(fill: LeaderFill) -> float:
    raw = fill.raw if isinstance(fill.raw, dict) else {}
    if fill.side == "BUY":
        return fnum(raw_get(raw, "ask", "bestAsk", "best_ask", "executable_ask", "current_ask"), 0.0)
    return fnum(raw_get(raw, "bid", "bestBid", "best_bid", "executable_bid", "current_bid"), 0.0)


def adverse_diff_pct(fill: LeaderFill, executable_price: float) -> float:
    if fill.price <= 0 or executable_price <= 0:
        return 0.0
    if fill.side == "BUY":
        return max(0.0, (executable_price - fill.price) / fill.price * 100.0)
    return max(0.0, (fill.price - executable_price) / fill.price * 100.0)


def dry_run_intent_audit_decision(cfg: LiveWalletConfig, fill: LeaderFill, intent_type: str, reducing: bool, replay_history: bool = False, quote_getter: Optional[Any] = None) -> Dict[str, Any]:
    source = str(getattr(fill, "source", "") or "").lower()
    target_price = fill.price
    executable_price: Any = fill.price
    diff_pct: Any = 0.0
    reason = audit_reason_for_fill(fill, replay_history=replay_history)
    status = "DRY_RUN_FILLED"
    policy = "DIRECT_EXECUTABLE"
    extra_note = ""
    execution_decision = status
    decision_reason = reason
    suggested_order_type = ""
    suggested_limit_price: Any = fill.price
    manual_reconcile_required = False
    market_data_source = ""
    market_data_error = ""
    if reducing:
        status = "WOULD_REDUCE" if intent_type == "ENTRY" else "WOULD_EXIT"
        execution_decision = "WOULD_REDUCE_OR_EXIT"
        decision_reason = "RISK_REDUCING_EXIT"
        suggested_order_type = "IOC_LIMIT"
        market_data_source = "LEADER_FILL_PRICE"
        extra_note = "; reducing/exit path; risk-reducing dry-run"
    elif source in {"ws", "live_ws", "live_ws_snapshot"}:
        status = "WOULD_PLACE_IOC_LIMIT"
        policy = "IOC_LIMIT_AT_LEADER_OR_ADJUSTED_COPY_PRICE"
        execution_decision = "WOULD_PLACE_IOC_LIMIT"
        decision_reason = "LIVE_WS_SNAPSHOT_RECOVERY" if source == "live_ws_snapshot" else "LIVE_WS_FAST_PATH"
        executable_price = fill.price
        suggested_order_type = "IOC_LIMIT"
        suggested_limit_price = fill.price
        market_data_source = "LEADER_FILL_PRICE"
        extra_note = "; target limit price=leader fill price"
    elif source == "live_poll":
        executable_price = executable_price_from_fill_payload(fill)
        quote_source = "FILL_PAYLOAD_QUOTE"
        quote_error = ""
        if executable_price <= 0 and quote_getter is not None:
            quote = quote_getter(fill.coin, fill.side)
            if isinstance(quote, dict) and quote.get("ok"):
                executable_price = fnum(quote.get("executable_price"), 0.0)
                quote_source = str(quote.get("source") or "hyperliquid_l2book")
            else:
                quote_error = str((quote or {}).get("error") or "EXECUTABLE_QUOTE_UNAVAILABLE") if isinstance(quote, dict) else "EXECUTABLE_QUOTE_UNAVAILABLE"
        if executable_price > 0:
            target_price = executable_price
            diff_pct = adverse_diff_pct(fill, executable_price)
            if diff_pct <= cfg.max_diff_pct:
                status = "WOULD_LATE_COPY"
                reason = "RECOVERED_WITHIN_DIFF_TOLERANCE"
                execution_decision = "WOULD_LATE_COPY"
                decision_reason = "RECOVERED_WITHIN_DIFF_TOLERANCE"
                policy = "LATE_COPY_WITHIN_DIFF_TOLERANCE"
                suggested_order_type = "IOC_LIMIT"
                suggested_limit_price = executable_price
                market_data_source = quote_source
                extra_note = f"; executable_price={executable_price:.8g}; adverse_diff_pct={diff_pct:.6g}"
            else:
                status = "DO_NOT_MARKET_COPY"
                reason = "MISSED_FILL_DIFF_TOO_LARGE"
                execution_decision = "DO_NOT_MARKET_COPY"
                decision_reason = "MISSED_FILL_DIFF_TOO_LARGE"
                target_price = fill.price
                policy = "MANUAL_RECONCILE_OR_ORIGINAL_LIMIT"
                suggested_order_type = "LIMIT_AT_ORIGINAL_COPY_PRICE"
                suggested_limit_price = fill.price
                manual_reconcile_required = True
                market_data_source = quote_source
                extra_note = f"; executable_price={executable_price:.8g}; adverse_diff_pct={diff_pct:.6g}; suggested_limit_price={fill.price:.8g}; manual reconcile"
        else:
            status = "MANUAL_REVIEW"
            reason = "RECOVERY_QUOTE_UNAVAILABLE"
            execution_decision = "MANUAL_REVIEW"
            decision_reason = "RECOVERY_QUOTE_UNAVAILABLE"
            policy = "NO_EXECUTABLE_BID_ASK_AVAILABLE"
            executable_price = ""
            diff_pct = ""
            suggested_order_type = "LIMIT_AT_ORIGINAL_COPY_PRICE"
            suggested_limit_price = fill.price
            manual_reconcile_required = True
            market_data_source = ""
            market_data_error = quote_error or "EXECUTABLE_QUOTE_UNAVAILABLE"
            extra_note = "; executable bid/ask unavailable; suggested_limit_price=original copy price; manual reconcile"
    return {
        "reason": reason,
        "status": status,
        "policy": policy,
        "target_price": target_price,
        "diff_pct": diff_pct,
        "extra_note": extra_note,
        "execution_decision": execution_decision,
        "decision_reason": decision_reason,
        "executable_price": executable_price,
        "adverse_diff_pct": diff_pct,
        "suggested_order_type": suggested_order_type,
        "suggested_limit_price": suggested_limit_price,
        "manual_reconcile_required": manual_reconcile_required,
        "market_data_source": market_data_source,
        "market_data_error": market_data_error,
    }


def ws_coverage_for_fill(fill: LeaderFill) -> str:
    health = load_json(LIVE_WS_HEALTH_FILE, {})
    wallets = health.get("wallets") if isinstance(health, dict) else {}
    wallet_health = wallets.get(fill.wallet) if isinstance(wallets, dict) else None
    windows = wallet_health.get("ws_session_open_windows") if isinstance(wallet_health, dict) else None
    if not isinstance(windows, list):
        return "UNKNOWN"
    for window in windows:
        if not isinstance(window, dict):
            continue
        open_ms = inum(window.get("open_ms"))
        close_ms = inum(window.get("close_ms"))
        if open_ms <= 0:
            continue
        if fill.timestamp_ms >= open_ms and (close_ms <= 0 or fill.timestamp_ms <= close_ms):
            return "INSIDE_WS_SESSION"
    return "OUTSIDE_WS_SESSION"


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


def parse_l2_level_price(level: Any) -> float:
    if isinstance(level, dict):
        return fnum(raw_get(level, "px", "price", "p"), 0.0)
    if isinstance(level, (list, tuple)) and level:
        return fnum(level[0], 0.0)
    return 0.0


def fetch_public_executable_quote(coin: str, side: str) -> Dict[str, Any]:
    coin = str(coin or "").upper().strip()
    side = str(side or "").upper().strip()
    quote = {
        "ok": False,
        "coin": coin,
        "side": side,
        "bid": 0.0,
        "ask": 0.0,
        "executable_price": 0.0,
        "source": "hyperliquid_l2book",
        "error": "",
    }
    if not coin:
        quote["error"] = "missing coin"
        return quote
    if requests is None:
        quote["error"] = "requests module unavailable"
        return quote
    try:
        response = requests.post("https://api.hyperliquid.xyz/info", json={"type": "l2Book", "coin": coin}, timeout=5)
        data = response.json()
        levels = data.get("levels") if isinstance(data, dict) else None
        if not isinstance(levels, list) or len(levels) < 2:
            quote["error"] = f"invalid l2Book response: {str(data)[:200]}"
            return quote
        bids = levels[0] if isinstance(levels[0], list) else []
        asks = levels[1] if isinstance(levels[1], list) else []
        bid = parse_l2_level_price(bids[0]) if bids else 0.0
        ask = parse_l2_level_price(asks[0]) if asks else 0.0
        executable = ask if side == "BUY" else bid if side == "SELL" else 0.0
        quote.update({"bid": bid, "ask": ask, "executable_price": executable})
        if bid <= 0 or ask <= 0 or executable <= 0:
            quote["error"] = f"missing executable quote bid={bid} ask={ask} side={side}"
            return quote
        quote["ok"] = True
        return quote
    except Exception as exc:
        quote["error"] = f"{type(exc).__name__}: {exc}"
        return quote


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
        self.quote_cache: Dict[str, Dict[str, Any]] = {}

        for path, fields in [
            (ORDER_INTENTS_CSV, ORDER_INTENT_FIELDS),
            (WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS),
            (SEND_ATTEMPTS_CSV, SEND_ATTEMPT_FIELDS),
            (LIVE_FILLS_CSV, LIVE_FILL_FIELDS),
            (RECONCILIATION_CSV, RECONCILIATION_FIELDS),
            (ERRORS_CSV, ERROR_FIELDS),
        ]:
            ensure_csv_schema(path, fields)
        self.would_send_intent_ids = load_csv_ids(WOULD_SEND_ORDERS_CSV, "intent_id")

    def bump(self, key: str, amount: int = 1) -> None:
        counters = self.state.setdefault("counters", {})
        counters[key] = int(counters.get(key, 0)) + amount

    def get_public_quote(self, coin: str, side: str) -> Dict[str, Any]:
        coin_key = str(coin or "").upper().strip()
        now = utc_now_ms()
        cached = self.quote_cache.get(coin_key)
        if isinstance(cached, dict) and now - inum(cached.get("ts_ms")) <= LIVE_QUOTE_CACHE_TTL_MS:
            quote = cached.get("quote")
            if isinstance(quote, dict):
                return dict(quote)
        quote = fetch_public_executable_quote(coin_key, side)
        self.quote_cache[coin_key] = {"ts_ms": now, "quote": dict(quote)}
        return quote

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
        failures = []
        if not safe_atomic_write_json(SERVICE_STATE_FILE, self.state, "SERVICE_STATE_WRITE"):
            failures.append(str(SERVICE_STATE_FILE))
        if not safe_atomic_write_json(LIVE_POSITIONS_FILE, self.positions, "LIVE_POSITIONS_WRITE"):
            failures.append(str(LIVE_POSITIONS_FILE))
        if not safe_atomic_write_json(LIVE_EQUITY_HISTORY_FILE, self.equity_history, "LIVE_EQUITY_HISTORY_WRITE"):
            failures.append(str(LIVE_EQUITY_HISTORY_FILE))
        if failures:
            self.bump("json_write_errors")
            try:
                append_csv(ERRORS_CSV, ERROR_FIELDS, {
                    "created_at": utc_now_iso(),
                    "context": "PERSIST_JSON_WRITE",
                    "error_type": "SafeAtomicWriteFailed",
                    "message": "; ".join(failures),
                    "traceback": "",
                })
            except Exception:
                pass

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

    def append_order_intent(self, cfg: LiveWalletConfig, fill: LeaderFill, intent_id: str, linked_ids: List[str], intent_type: str, reason: str, status: str, copy_notional: float, copy_size: float, policy: str, notes: str = "", target_price: Optional[float] = None, diff_pct: Any = 0.0, decision: Optional[Dict[str, Any]] = None) -> None:
        decision = decision or {}
        row = {
            "created_at": utc_now_iso(), "intent_id": intent_id, "dry_run": True,
            "leader_wallet": cfg.wallet, "leader_fill_id": fill.fill_id,
            "linked_leader_fill_ids": "|".join(linked_ids), "mode": cfg.mode,
            "copy_mode": cfg.copy_mode, "coin": fill.coin, "side": fill.side,
            "intent_type": intent_type, "reason": reason, "status": status,
            "leader_price": round(fill.price, 8), "target_price": round(fnum(target_price, fill.price), 8),
            "leader_size": round(fill.size, 12), "leader_notional": round(fill.notional, 8),
            "copy_notional": round(copy_notional, 8), "copy_size": round(copy_size, 12),
            "min_notional_policy": policy, "diff_pct": round(fnum(diff_pct), 8) if diff_pct != "" else "", "max_diff_pct": cfg.max_diff_pct,
            "daily_loss_limit": cfg.daily_loss_limit, "notes": notes,
            "execution_decision": decision.get("execution_decision", ""),
            "decision_reason": decision.get("decision_reason", ""),
            "executable_price": decision.get("executable_price", ""),
            "adverse_diff_pct": decision.get("adverse_diff_pct", ""),
            "suggested_order_type": decision.get("suggested_order_type", ""),
            "suggested_limit_price": decision.get("suggested_limit_price", ""),
            "manual_reconcile_required": decision.get("manual_reconcile_required", ""),
            "market_data_source": decision.get("market_data_source", ""),
            "market_data_error": decision.get("market_data_error", ""),
        }
        append_csv(ORDER_INTENTS_CSV, ORDER_INTENT_FIELDS, row)
        if should_write_would_send(decision, row):
            self.append_would_send_order(row)

    def append_would_send_order(self, intent_row: Dict[str, Any]) -> None:
        if not should_write_would_send(intent_row, intent_row):
            return
        intent_id = str(intent_row.get("intent_id", "")).strip()
        if not intent_id or intent_id in self.would_send_intent_ids:
            return
        execution_decision = str(intent_row.get("execution_decision", ""))
        row = {
            "created_at": utc_now_iso(),
            "dry_run": True,
            "intent_id": intent_id,
            "leader_wallet": intent_row.get("leader_wallet", ""),
            "leader_fill_id": intent_row.get("leader_fill_id", ""),
            "source_reason": intent_row.get("reason", ""),
            "execution_decision": execution_decision,
            "decision_reason": intent_row.get("decision_reason", ""),
            "coin": intent_row.get("coin", ""),
            "side": intent_row.get("side", ""),
            "copy_size": intent_row.get("copy_size", ""),
            "copy_notional": intent_row.get("copy_notional", ""),
            "order_type": intent_row.get("suggested_order_type") or "IOC_LIMIT",
            "limit_price": intent_row.get("suggested_limit_price") or intent_row.get("target_price", ""),
            "reduce_only": execution_decision == "WOULD_REDUCE_OR_EXIT",
            "manual_reconcile_required": intent_row.get("manual_reconcile_required", ""),
            "status": "WOULD_SEND_DRY_RUN",
            "notes": "dry-run would-send payload only; no order placed",
        }
        append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, row)
        self.would_send_intent_ids.add(intent_id)

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
        decision = dry_run_intent_audit_decision(cfg, fill, intent_type, reducing, replay_history=replay_history, quote_getter=self.get_public_quote)
        policy = decision.get("policy") or policy
        audit_notes = audit_notes_for_fill(fill, replay_history=replay_history) + str(decision.get("extra_note") or "")
        self.append_order_intent(
            cfg, fill, intent_id, linked_ids, intent_type,
            str(decision.get("reason") or audit_reason_for_fill(fill, replay_history=replay_history)),
            str(decision.get("status") or "DRY_RUN_FILLED"),
            copy_notional, copy_size, policy, audit_notes,
            target_price=fnum(decision.get("target_price"), fill.price),
            diff_pct=decision.get("diff_pct", ""),
            decision=decision,
        )
        if decision.get("status") in {"DO_NOT_MARKET_COPY", "MANUAL_REVIEW"}:
            self.append_reconciliation(cfg, fill, "REVIEW", str(decision.get("reason") or "MANUAL_REVIEW"), "MANUAL_REVIEW", before_signed, before_signed, copy_notional, "manual reconcile required" + str(decision.get("extra_note") or ""))
            self.processed_ids.add(fill.fill_id)
            self.bump("late_copy_manual_review")
            return
        updated_pos, before, realized_pnl = self.apply_dry_run_fill_to_position(cfg, fill, copy_notional)
        after = fnum(updated_pos.get("signed_size"))
        self.append_live_fill(cfg, fill, intent_id, "DRY_RUN_FILLED", copy_notional, copy_size, updated_pos, realized_pnl, audit_notes)
        recon_action = "MANUAL_REVIEW" if decision.get("status") in {"DO_NOT_MARKET_COPY", "MANUAL_REVIEW"} else "DRY_RUN_FILL"
        self.append_reconciliation(cfg, fill, "DRY_RUN_FILL", str(decision.get("status") or "MATCHED_SIMULATED"), recon_action, before, after, copy_notional, "dry-run position updated" + str(decision.get("extra_note") or ""))
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
    summary = {
        "wallet_count": 0,
        "open_count": 0,
        "reconnecting_count": 0,
        "closed_count": 0,
        "stale_count": 0,
        "total_processed_count": 0,
        "total_reconnect_count": 0,
        "avg_reconnects_per_min": 0.0,
        "worst_health_grade": "GOOD",
    }
    overall = "DISABLED"
    if manager is not None:
        overall = "OK"
        with manager.status_lock:
            items = list(manager.wallet_status.items())
        total_reconnects_per_min = 0.0
        worst_grade_score = 0
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
            next_proactive_recycle_ms = int(status.get("next_proactive_recycle_ms") or 0)
            last_open_ms = int(status.get("last_open_ms") or 0)
            last_close_ms = int(status.get("last_close_ms") or 0)
            first_seen_ms = int(status.get("first_seen_ms") or now)
            thread_alive = bool(status.get("worker_thread_alive"))
            reconnect_overdue = bool(
                status.get("status") == "RECONNECTING"
                and next_reconnect_ms > 0
                and now > next_reconnect_ms + LIVE_WS_RECONNECT_GRACE_MS
            )
            status_name = str(status.get("status", "UNKNOWN"))
            effective_status = status_name
            if reconnect_overdue:
                effective_status = "RECONNECT_OVERDUE"
            elif status_name == "OPEN" and (LIVE_WS_STALE_MS <= 0 or transport_stale_ms <= LIVE_WS_STALE_MS):
                effective_status = "OPEN"
            elif status_name == "OPEN":
                effective_status = "STALE"
            elif status_name in {"CONNECTING", "RECONNECTING", "THREAD_ERROR", "THREAD_EXITED", "RESTARTING"}:
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
            proactive_recycle_due_ms = max(0, next_proactive_recycle_ms - now) if next_proactive_recycle_ms else 0
            uptime_ms = (now - last_open_ms) if status_name == "OPEN" and last_open_ms else int(status.get("uptime_ms") or 0)
            downtime_ms = (now - last_close_ms) if status_name in {"CLOSED", "RECONNECTING"} and last_close_ms else 0
            observed_ms = max(0, now - first_seen_ms)
            observed_min = max(observed_ms / 60000, 1)
            reconnect_count = int(status.get("reconnect_count") or 0)
            close_count = int(status.get("close_count") or 0)
            error_count = int(status.get("error_count") or 0)
            processed_count = int(status.get("processed_count") or 0)
            reconnects_per_min = reconnect_count / observed_min
            closes_per_min = close_count / observed_min
            if effective_status == "OPEN" and reconnects_per_min <= 2 and error_count == 0:
                health_grade = "GOOD"
            elif effective_status == "OPEN" and reconnects_per_min <= 5 and error_count == 0:
                health_grade = "WATCH"
            else:
                health_grade = "DEGRADED"
            grade_score = {"GOOD": 0, "WATCH": 1, "DEGRADED": 2}[health_grade]
            worst_grade_score = max(worst_grade_score, grade_score)
            total_reconnects_per_min += reconnects_per_min
            summary["wallet_count"] += 1
            if effective_status == "OPEN":
                summary["open_count"] += 1
            elif effective_status == "RECONNECTING":
                summary["reconnecting_count"] += 1
            elif effective_status == "CLOSED":
                summary["closed_count"] += 1
            elif effective_status == "STALE":
                summary["stale_count"] += 1
            summary["total_processed_count"] += processed_count
            summary["total_reconnect_count"] += reconnect_count
            wallets[wallet] = {
                **status,
                "effective_status": effective_status,
                "data_status": data_status,
                "stale_ms": transport_stale_ms,
                "transport_stale_ms": transport_stale_ms,
                "data_stale_ms": data_stale_ms,
                "reconnect_in_ms": reconnect_in_ms,
                "proactive_recycle_due_ms": proactive_recycle_due_ms,
                "uptime_ms": uptime_ms,
                "downtime_ms": downtime_ms,
                "observed_ms": observed_ms,
                "reconnects_per_min": reconnects_per_min,
                "closes_per_min": closes_per_min,
                "health_grade": health_grade,
                "thread_alive": thread_alive,
                "reconnect_overdue": reconnect_overdue,
            }
        if summary["wallet_count"]:
            summary["avg_reconnects_per_min"] = total_reconnects_per_min / summary["wallet_count"]
        summary["worst_health_grade"] = {0: "GOOD", 1: "WATCH", 2: "DEGRADED"}[worst_grade_score]
    return {
        "enabled": bool(manager is not None),
        "updated_at": utc_now_iso(),
        "overall": overall,
        "ws_summary": summary,
        "wallets": wallets,
        "health_write_error_count": int(getattr(manager, "health_write_error_count", 0) or 0) if manager is not None else 0,
        "last_health_write_error": str(getattr(manager, "last_health_write_error", "") or "") if manager is not None else "",
        "last_health_write_error_at": str(getattr(manager, "last_health_write_error_at", "") or "") if manager is not None else "",
        "max_wallets": LIVE_WS_MAX_WALLETS,
        "stale_ms_threshold": LIVE_WS_STALE_MS,
        "mode": "DEDICATED_SOCKET_PER_WALLET",
        "dry_run": True,
    }


def write_ws_health(manager: Any) -> None:
    try:
        payload = build_ws_health_snapshot(manager)
        ok = safe_atomic_write_json(LIVE_WS_HEALTH_FILE, payload, "WS_HEALTH_WRITE")
    except Exception:
        ok = False
    if not ok and manager is not None:
        try:
            manager.health_write_error_count += 1
            manager.last_health_write_error = "safe_atomic_write_json failed for live_ws_health.json"
            manager.last_health_write_error_at = utc_now_iso()
        except Exception:
            pass


class DedicatedLiveWSManager:
    def __init__(self, service: DryRunLiveCopyService, wallets: List[str]) -> None:
        self.service = service
        self.wallets = [str(wallet).lower() for wallet in wallets[:LIVE_WS_MAX_WALLETS]]
        self.stop_event = threading.Event()
        self.threads: List[threading.Thread] = []
        self.apps: Dict[str, Any] = {}
        self.health_write_error_count = 0
        self.last_health_write_error = ""
        self.last_health_write_error_at = ""
        self.wallet_status = {
            wallet: {
                "wallet": wallet,
                "status": "INIT",
                "first_seen_ms": utc_now_ms(),
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
                "last_run_forever_result": "",
                "processed_count": 0,
                "duplicate_count": 0,
                "ignored_count": 0,
                "error_count": 0,
                "reconnect_count": 0,
                "last_error": "",
                "last_error_repr": "",
                "benign_event_count": 0,
                "last_benign_event_repr": "",
                "control_frame_count": 0,
                "close_frame_count": 0,
                "last_control_frame": {},
                "last_close_frame_code": "",
                "last_close_frame_reason": "",
                "last_close_frame_opcode": "",
                "data_status": "IDLE_NO_FILLS",
                "worker_thread_alive": False,
                "worker_thread_name": "",
                "worker_last_seen_ms": 0,
                "worker_restart_count": 0,
                "fatal_error_count": 0,
                "last_fatal_error": "",
                "last_fatal_error_repr": "",
                "proactive_recycle_count": 0,
                "last_proactive_recycle_ms": 0,
                "next_proactive_recycle_ms": 0,
                "proactive_recycle_enabled": bool(LIVE_WS_PROACTIVE_RECYCLE_ENABLED),
                "raw_message_count": 0,
                "parsed_fill_message_count": 0,
                "ignored_message_count": 0,
                "ignored_snapshot_count": 0,
                "ignored_empty_message_count": 0,
                "ignored_parse_error_count": 0,
                "last_raw_message_at_ms": 0,
                "last_raw_message_preview": "",
                "last_ignored_message_reason": "",
                "last_ignored_message_preview": "",
                "last_parsed_fill_ids": [],
                "ws_session_open_windows": [],
                "snapshot_fill_seen_count": 0,
                "snapshot_fill_recovered_count": 0,
                "snapshot_fill_skipped_old_count": 0,
                "snapshot_fill_skipped_duplicate_count": 0,
                "snapshot_fill_skipped_prebaseline_count": 0,
            }
            for wallet in self.wallets
        }
        self.wallet_threads: Dict[str, threading.Thread] = {}
        self.status_lock = threading.Lock()
        self.service_lock = threading.Lock()

    def update_status(self, wallet: str, **patch: Any) -> None:
        wallet = str(wallet).lower()
        with self.status_lock:
            status = self.wallet_status.setdefault(wallet, {"wallet": wallet})
            status.update(patch)

    def record_session_open(self, wallet: str, open_ms: int) -> None:
        wallet = str(wallet).lower()
        with self.status_lock:
            status = self.wallet_status.setdefault(wallet, {"wallet": wallet})
            windows = status.setdefault("ws_session_open_windows", [])
            if not isinstance(windows, list):
                windows = []
                status["ws_session_open_windows"] = windows
            windows.append({"open_ms": int(open_ms), "close_ms": 0})
            del windows[:-50]

    def record_session_close(self, wallet: str, close_ms: int) -> None:
        wallet = str(wallet).lower()
        with self.status_lock:
            status = self.wallet_status.setdefault(wallet, {"wallet": wallet})
            windows = status.setdefault("ws_session_open_windows", [])
            if not isinstance(windows, list):
                windows = []
                status["ws_session_open_windows"] = windows
            for window in reversed(windows):
                if isinstance(window, dict) and inum(window.get("close_ms")) <= 0:
                    window["close_ms"] = int(close_ms)
                    break
            del windows[:-50]

    def health_loop(self) -> None:
        write_ws_health(self)
        while not self.stop_event.wait(max(0.2, LIVE_WS_HEALTH_WRITE_SEC)):
            write_ws_health(self)
        write_ws_health(self)

    def start_wallet_thread(self, wallet: str) -> None:
        wallet = str(wallet).lower()
        existing = self.wallet_threads.get(wallet)
        if existing is not None and existing.is_alive():
            return
        thread = threading.Thread(target=self.wallet_loop, args=(wallet,), name=f"live-ws-{wallet[-6:]}", daemon=True)
        self.wallet_threads[wallet] = thread
        self.threads.append(thread)
        thread.start()

    def watchdog_loop(self) -> None:
        while not self.stop_event.wait(max(0.5, LIVE_WS_THREAD_WATCHDOG_SEC)):
            now = utc_now_ms()
            for wallet in self.wallets:
                thread = self.wallet_threads.get(wallet)
                thread_alive = bool(thread and thread.is_alive())
                should_restart = False
                with self.status_lock:
                    status = self.wallet_status[wallet]
                    status["worker_thread_alive"] = thread_alive
                    next_reconnect_ms = int(status.get("next_reconnect_ms") or 0)
                    if not thread_alive:
                        should_restart = True
                    elif (
                        status.get("status") == "RECONNECTING"
                        and next_reconnect_ms > 0
                        and now > next_reconnect_ms + LIVE_WS_RECONNECT_GRACE_MS
                        and not thread_alive
                    ):
                        should_restart = True
                    if should_restart and not self.stop_event.is_set():
                        status["worker_restart_count"] += 1
                        status["status"] = "RESTARTING"
                if should_restart and not self.stop_event.is_set():
                    self.start_wallet_thread(wallet)
            write_ws_health(self)

    def start(self) -> None:
        write_ws_health(self)
        health_thread = threading.Thread(target=self.health_loop, name="live-ws-health", daemon=True)
        health_thread.start()
        self.threads.append(health_thread)
        watchdog_thread = threading.Thread(target=self.watchdog_loop, name="live-ws-watchdog", daemon=True)
        watchdog_thread.start()
        self.threads.append(watchdog_thread)
        for wallet in self.wallets:
            self.start_wallet_thread(wallet)

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
        thread_name = threading.current_thread().name
        self.update_status(
            wallet,
            worker_thread_alive=True,
            worker_thread_name=thread_name,
            worker_last_seen_ms=utc_now_ms(),
        )
        try:
            self.wallet_loop_body(wallet)
        except Exception as exc:
            with self.status_lock:
                status = self.wallet_status[wallet]
                status["status"] = "THREAD_ERROR"
                status["fatal_error_count"] += 1
                status["last_fatal_error"] = str(exc)
                status["last_fatal_error_repr"] = repr(exc)
                status["worker_thread_alive"] = False
            write_ws_health(self)
            return
        finally:
            with self.status_lock:
                status = self.wallet_status[wallet]
                status["worker_thread_alive"] = False
                status["worker_last_seen_ms"] = utc_now_ms()
                if not self.stop_event.is_set() and status.get("status") not in {"THREAD_ERROR", "ERROR"}:
                    status["status"] = "THREAD_EXITED"
            write_ws_health(self)

    def wallet_loop_body(self, wallet: str) -> None:
        if websocket is None:
            self.update_status(wallet, status="ERROR", error_count=1, last_error="websocket-client unavailable")
            write_ws_health(self)
            return

        attempt = 0
        backoff = max(0.1, LIVE_WS_INITIAL_BACKOFF_SEC)
        while not self.stop_event.is_set():
            self.update_status(
                wallet,
                status="CONNECTING",
                last_error="",
                last_reconnect_attempt_ms=utc_now_ms(),
                worker_thread_alive=True,
                worker_thread_name=threading.current_thread().name,
                worker_last_seen_ms=utc_now_ms(),
            )

            def schedule_proactive_recycle(app: Any, open_ms: int) -> None:
                if not LIVE_WS_PROACTIVE_RECYCLE_ENABLED or LIVE_WS_PROACTIVE_RECYCLE_SEC <= 0:
                    return

                def recycle_after_delay() -> None:
                    if self.stop_event.wait(LIVE_WS_PROACTIVE_RECYCLE_SEC):
                        return
                    with self.status_lock:
                        status = self.wallet_status[wallet]
                        if int(status.get("last_open_ms") or 0) != open_ms:
                            return
                        if self.apps.get(wallet) is not app:
                            return
                        status["status"] = "PROACTIVE_RECYCLING"
                        status["proactive_recycle_count"] += 1
                        status["last_proactive_recycle_ms"] = utc_now_ms()
                    try:
                        app.close()
                    except Exception as exc:
                        with self.status_lock:
                            status = self.wallet_status[wallet]
                            status["last_error"] = str(exc)
                            status["last_error_repr"] = repr(exc)

                thread = threading.Thread(target=recycle_after_delay, name=f"live-ws-recycle-{wallet[-6:]}", daemon=True)
                thread.start()

            def on_open(app: Any) -> None:
                nonlocal backoff
                now = utc_now_ms()
                backoff = max(0.1, LIVE_WS_INITIAL_BACKOFF_SEC)
                next_proactive_recycle_ms = (
                    now + int(LIVE_WS_PROACTIVE_RECYCLE_SEC * 1000)
                    if LIVE_WS_PROACTIVE_RECYCLE_ENABLED and LIVE_WS_PROACTIVE_RECYCLE_SEC > 0
                    else 0
                )
                patch = {
                    "status": "OPEN",
                    "last_open_ms": now,
                    "last_msg_ms": now,
                    "last_heartbeat_ms": now,
                    "reconnect_backoff_sec": backoff,
                    "next_reconnect_ms": 0,
                    "next_proactive_recycle_ms": next_proactive_recycle_ms,
                    "proactive_recycle_enabled": bool(LIVE_WS_PROACTIVE_RECYCLE_ENABLED),
                }
                self.update_status(wallet, **patch)
                self.record_session_open(wallet, now)
                schedule_proactive_recycle(app, now)
                app.send(json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": wallet}}))

            def on_ping(_app: Any, _message: Any) -> None:
                now = utc_now_ms()
                self.update_status(wallet, last_ping_ms=now, last_heartbeat_ms=now)

            def on_pong(_app: Any, _message: Any) -> None:
                now = utc_now_ms()
                self.update_status(wallet, last_pong_ms=now, last_heartbeat_ms=now)

            def on_message(_app: Any, message: str) -> None:
                now = utc_now_ms()
                preview = ws_message_preview(message)
                self.update_status(
                    wallet,
                    last_msg_ms=now,
                    last_heartbeat_ms=now,
                    last_raw_message_at_ms=now,
                    last_raw_message_preview=preview,
                )
                with self.status_lock:
                    self.wallet_status[wallet]["raw_message_count"] += 1
                extracted = extract_ws_fills_with_meta(message)
                if extracted:
                    with self.status_lock:
                        status = self.wallet_status[wallet]
                        status["last_data_ms"] = now
                else:
                    reason = classify_ignored_ws_message(message)
                    with self.status_lock:
                        status = self.wallet_status[wallet]
                        status["ignored_message_count"] += 1
                        status["last_ignored_message_reason"] = reason
                        status["last_ignored_message_preview"] = preview
                        if reason == "SNAPSHOT":
                            status["ignored_snapshot_count"] += 1
                        elif reason == "EMPTY_FILLS":
                            status["ignored_empty_message_count"] += 1
                        elif reason == "JSON_ERROR":
                            status["ignored_parse_error_count"] += 1
                    return
                recovered_or_live_count = 0
                snapshot_seen = 0
                for msg_wallet, raw, is_snapshot in extracted:
                    fill = parse_ws_snapshot_leader_fill(msg_wallet or wallet, raw) if is_snapshot else parse_ws_leader_fill(msg_wallet or wallet, raw)
                    if fill is None:
                        with self.status_lock:
                            self.wallet_status[wallet]["ignored_count"] += 1
                        continue
                    if is_snapshot:
                        snapshot_seen += 1
                        with self.status_lock:
                            snapshot_status = dict(self.wallet_status[wallet])
                        guard = should_recover_ws_snapshot_fill(self.service, snapshot_status, fill.wallet, fill)
                        with self.status_lock:
                            status = self.wallet_status[wallet]
                            status["snapshot_fill_seen_count"] += 1
                            if guard == "DUPLICATE":
                                status["snapshot_fill_skipped_duplicate_count"] += 1
                            elif guard == "PREBASELINE":
                                status["snapshot_fill_skipped_prebaseline_count"] += 1
                            elif guard == "OLD":
                                status["snapshot_fill_skipped_old_count"] += 1
                        if guard != "RECOVER":
                            continue
                    with self.service_lock:
                        result = self.service.process_ws_fill(fill)
                    with self.status_lock:
                        status = self.wallet_status[wallet]
                        ids = status.setdefault("last_parsed_fill_ids", [])
                        if isinstance(ids, list):
                            ids.append(fill.fill_id)
                            del ids[:-20]
                        if result.get("processed"):
                            status["processed_count"] += 1
                        elif result.get("reason") == "DUPLICATE":
                            status["duplicate_count"] += 1
                        else:
                            status["ignored_count"] += 1
                        if is_snapshot and result.get("processed"):
                            status["snapshot_fill_recovered_count"] += 1
                        if (not is_snapshot) or result.get("processed"):
                            recovered_or_live_count += 1
                with self.status_lock:
                    status = self.wallet_status[wallet]
                    if recovered_or_live_count:
                        status["parsed_fill_message_count"] += 1
                    elif snapshot_seen:
                        status["ignored_message_count"] += 1
                        status["ignored_snapshot_count"] += 1
                        status["last_ignored_message_reason"] = "SNAPSHOT_NO_RECOVERABLE_FILLS"
                        status["last_ignored_message_preview"] = preview

            def on_error(_app: Any, error: Any) -> None:
                with self.status_lock:
                    status = self.wallet_status[wallet]
                    if is_ws_abnf_frame(error):
                        details = ws_abnf_details(error)
                        status["benign_event_count"] += 1
                        status["last_benign_event_repr"] = repr(error)
                        status["control_frame_count"] += 1
                        status["last_control_frame"] = details
                        status["last_heartbeat_ms"] = utc_now_ms()
                        if details.get("opcode") == 8 or details.get("close_code") != "":
                            status["close_frame_count"] += 1
                            status["last_close_frame_code"] = details.get("close_code", "")
                            status["last_close_frame_reason"] = details.get("close_reason", "")
                            status["last_close_frame_opcode"] = details.get("opcode", "")
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
                    close_ms = utc_now_ms()
                    status["last_close_ms"] = close_ms
                    status["last_close_status_code"] = close_status_code
                    status["last_close_msg"] = message
                self.record_session_close(wallet, close_ms)

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
                run_result = app.run_forever(
                    ping_interval=LIVE_WS_PING_INTERVAL_SEC,
                    ping_timeout=LIVE_WS_PING_TIMEOUT_SEC,
                )
                self.update_status(wallet, last_run_forever_result=repr(run_result))
                self.record_session_close(wallet, utc_now_ms())
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
                self.update_status(wallet, worker_thread_alive=True, worker_last_seen_ms=utc_now_ms())
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
    global LIVE_POLL_ENABLED, LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD, LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT
    global HL_PERP_META_BY_COIN_CACHE, fetch_live_fills_since, fetch_public_executable_quote, send_hyperliquid_order

    old_paths = (
        AUDIT_DIR, RAW_LEADER_FILLS_CSV, APP_CONFIG_FILE, APPEND_ONLY_DIR,
        SERVICE_STATE_FILE, LIVE_POSITIONS_FILE, MANUAL_LIVE_POSITIONS_FILE, LIVE_EQUITY_HISTORY_FILE,
        LIVE_WS_HEALTH_FILE, ORDER_INTENTS_CSV, WOULD_SEND_ORDERS_CSV,
        SEND_ATTEMPTS_CSV, LIVE_FILLS_CSV, RECONCILIATION_CSV, ERRORS_CSV,
    )
    old_poll_enabled = LIVE_POLL_ENABLED
    old_max_manual_order_notional = LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD
    old_max_close_adverse_diff = LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT
    old_meta_cache = HL_PERP_META_BY_COIN_CACHE
    old_private_key = os.environ.get("HL_LIVE_HL_PRIVATE_KEY")
    old_account_address = os.environ.get("HL_LIVE_HL_ACCOUNT_ADDRESS")
    old_fetch_live_fills_since = fetch_live_fills_since
    old_fetch_public_executable_quote = fetch_public_executable_quote
    old_send_hyperliquid_order = send_hyperliquid_order
    wallet = "0xabc0000000000000000000000000000000000001"
    rows = [
        {"fill_id": "hist-1", "wallet": wallet, "coin": "BTC", "side": "BUY", "price": 100, "size": 1, "timestamp_ms": 1000, "raw_json": "{}"},
        {"fill_id": "hist-2", "wallet": wallet, "coin": "BTC", "side": "SELL", "price": 101, "size": 1, "timestamp_ms": 2000, "raw_json": "{}"},
    ]
    try:
        # .env loader tests (no secrets involved; test keys only)
        os.environ["HL_SELFTEST_EXISTING"] = "original"
        _env_content = "\n".join([
            "HL_SELFTEST_MISSING=loaded",
            "HL_SELFTEST_EXISTING=overwrite_attempt",
            "# comment line",
            "MALFORMED_NO_EQUALS",
            "",
        ])
        _env_path = Path(tempfile.mktemp(suffix=".env"))
        try:
            _env_path.write_text(_env_content, encoding="utf-8")
            load_local_env_file(_env_path)
            if os.environ.get("HL_SELFTEST_MISSING") != "loaded":
                raise AssertionError("load_local_env_file did not load missing key")
            if os.environ.get("HL_SELFTEST_EXISTING") != "original":
                raise AssertionError("load_local_env_file overwrote existing env key")
        finally:
            os.environ.pop("HL_SELFTEST_MISSING", None)
            os.environ.pop("HL_SELFTEST_EXISTING", None)
            try:
                _env_path.unlink()
            except Exception:
                pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_dir = root / "hl_live_copy_audit"
            source_fills = root / "raw_live_fills.csv"
            configure_paths(audit_dir, source_fills)
            safe_probe = root / "safe_write_probe.json"
            if not safe_atomic_write_json(safe_probe, {"ok": True}, "SELF_TEST_SAFE_WRITE"):
                raise AssertionError("safe_atomic_write_json returned false for temp JSON")
            if load_json(safe_probe, {}).get("ok") is not True:
                raise AssertionError("safe_atomic_write_json temp JSON did not round-trip")
            old_order_fields = ORDER_INTENT_FIELDS[:ORDER_INTENT_FIELDS.index("execution_decision")]
            ORDER_INTENTS_CSV.parent.mkdir(parents=True, exist_ok=True)
            with ORDER_INTENTS_CSV.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=old_order_fields)
                writer.writeheader()
                writer.writerow({"created_at": "old", "intent_id": "old-1", "leader_fill_id": "old-fill", "notes": "preserve me"})
            ensure_csv_schema(ORDER_INTENTS_CSV, ORDER_INTENT_FIELDS)
            with ORDER_INTENTS_CSV.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                schema_header = list(reader.fieldnames or [])
                schema_rows = list(reader)
            if "execution_decision" not in schema_header or "decision_reason" not in schema_header:
                raise AssertionError(f"order intent schema upgrade missing decision columns: {schema_header}")
            if not schema_rows or schema_rows[0].get("leader_fill_id") != "old-fill" or schema_rows[0].get("notes") != "preserve me":
                raise AssertionError(f"order intent schema upgrade did not preserve old row: {schema_rows}")
            if not safe_atomic_write_json(APP_CONFIG_FILE, {
                "wallets": {
                    wallet: {
                        "mode": "LIVE",
                        "enabled": True,
                        "copy_mode": "fixed",
                        "fixed_notional": 20.0,
                        "norm_base": 100.0,
                    }
                }
            }, "SELF_TEST_CONFIG_WRITE"):
                raise AssertionError("failed to write self-test config")
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
            cfg_for_quote = LiveWalletConfig(wallet=wallet, mode="LIVE", enabled=True, copy_mode="fixed", fixed_notional=20.0, max_diff_pct=0.5)
            poll_buy = LeaderFill("quote-buy", wallet, "BTC", "BUY", 100.0, 1.0, 1.0, 3000, utc_now_iso(), "live_poll", "REST", {})
            poll_sell = LeaderFill("quote-sell", wallet, "BTC", "SELL", 100.0, 1.0, -1.0, 3001, utc_now_iso(), "live_poll", "REST", {})

            def quote_within(coin: str, side: str) -> Dict[str, Any]:
                return {"ok": True, "coin": coin, "side": side, "bid": 99.8, "ask": 100.2, "executable_price": 100.2 if side == "BUY" else 99.8, "source": "hyperliquid_l2book", "error": ""}

            def quote_too_adverse(coin: str, side: str) -> Dict[str, Any]:
                return {"ok": True, "coin": coin, "side": side, "bid": 98.0, "ask": 102.0, "executable_price": 102.0 if side == "BUY" else 98.0, "source": "hyperliquid_l2book", "error": ""}

            def quote_unavailable(_coin: str, _side: str) -> Dict[str, Any]:
                return {"ok": False, "coin": _coin, "side": _side, "bid": 0.0, "ask": 0.0, "executable_price": 0.0, "source": "hyperliquid_l2book", "error": "test unavailable"}

            buy_within_decision = dry_run_intent_audit_decision(cfg_for_quote, poll_buy, "ENTRY", False, quote_getter=quote_within)
            if buy_within_decision.get("execution_decision") != "WOULD_LATE_COPY" or buy_within_decision.get("decision_reason") != "RECOVERED_WITHIN_DIFF_TOLERANCE":
                raise AssertionError(f"BUY within quote decision mismatch: {buy_within_decision}")
            buy_adverse_decision = dry_run_intent_audit_decision(cfg_for_quote, poll_buy, "ENTRY", False, quote_getter=quote_too_adverse)
            if buy_adverse_decision.get("execution_decision") != "DO_NOT_MARKET_COPY" or buy_adverse_decision.get("decision_reason") != "MISSED_FILL_DIFF_TOO_LARGE":
                raise AssertionError(f"BUY adverse quote decision mismatch: {buy_adverse_decision}")
            sell_within_decision = dry_run_intent_audit_decision(cfg_for_quote, poll_sell, "ENTRY", False, quote_getter=quote_within)
            if sell_within_decision.get("execution_decision") != "WOULD_LATE_COPY":
                raise AssertionError(f"SELL within quote decision mismatch: {sell_within_decision}")
            unavailable_decision = dry_run_intent_audit_decision(cfg_for_quote, poll_buy, "ENTRY", False, quote_getter=quote_unavailable)
            if unavailable_decision.get("execution_decision") != "MANUAL_REVIEW" or unavailable_decision.get("market_data_error") != "test unavailable":
                raise AssertionError(f"unavailable quote decision mismatch: {unavailable_decision}")

            quote_call_count = {"n": 0}

            def fake_public_quote(coin: str, side: str) -> Dict[str, Any]:
                quote_call_count["n"] += 1
                return quote_within(coin, side)

            fetch_public_executable_quote = fake_public_quote
            quote_service = DryRunLiveCopyService()
            q1 = quote_service.get_public_quote("BTC", "BUY")
            q2 = quote_service.get_public_quote("BTC", "BUY")
            if not q1.get("ok") or not q2.get("ok") or quote_call_count["n"] != 1:
                raise AssertionError(f"quote cache did not reuse cached quote: calls={quote_call_count} q1={q1} q2={q2}")

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
            before_would_send_rows = count_csv_data_rows(WOULD_SEND_ORDERS_CSV)
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
            if ws_intent.get("execution_decision") != "WOULD_PLACE_IOC_LIMIT" or ws_intent.get("decision_reason") != "LIVE_WS_FAST_PATH":
                raise AssertionError(f"WS order intent decision columns mismatch: {ws_intent}")
            if "source=live_ws" not in ws_intent.get("notes", ""):
                raise AssertionError(f"WS order intent notes missing source label: {ws_intent}")
            after_would_send_rows = count_csv_data_rows(WOULD_SEND_ORDERS_CSV)
            if after_would_send_rows - before_would_send_rows != 1:
                raise AssertionError(f"expected one would-send row, before={before_would_send_rows} after={after_would_send_rows}")
            would_send_row = last_csv_row(WOULD_SEND_ORDERS_CSV)
            if (
                would_send_row.get("coin") != "BTC"
                or would_send_row.get("side") != "BUY"
                or would_send_row.get("order_type") != "IOC_LIMIT"
                or not would_send_row.get("copy_size")
                or not would_send_row.get("limit_price")
            ):
                raise AssertionError(f"would-send row missing order payload fields: {would_send_row}")
            service.append_would_send_order(ws_intent)
            if count_csv_data_rows(WOULD_SEND_ORDERS_CSV) != after_would_send_rows:
                raise AssertionError("duplicate would-send intent_id was appended")
            manual_row = dict(ws_intent)
            manual_row["intent_id"] = "manual-review-intent"
            manual_row["execution_decision"] = "MANUAL_REVIEW"
            manual_row["decision_reason"] = "RECOVERY_QUOTE_UNAVAILABLE"
            service.append_would_send_order(manual_row)
            if count_csv_data_rows(WOULD_SEND_ORDERS_CSV) != after_would_send_rows:
                raise AssertionError("MANUAL_REVIEW should not append would-send row")
            confirm_required = manual_send_one_intent(str(ws_intent.get("intent_id")), False)
            if confirm_required.get("status") != "CONFIRM_REQUIRED" or confirm_required.get("payload", {}).get("order_type") != "IOC_LIMIT":
                raise AssertionError(f"manual send should require confirmation and show payload: {confirm_required}")
            buy_offset = manual_send_one_intent(str(ws_intent.get("intent_id")), False, marketable_bps=5)
            if buy_offset.get("payload", {}).get("adjusted_limit_price", 0) <= buy_offset.get("payload", {}).get("limit_price", 0):
                raise AssertionError(f"BUY marketable offset did not increase price: {buy_offset}")
            sell_payload = dict(would_send_row)
            sell_payload["intent_id"] = "sell-offset-intent"
            sell_payload["side"] = "SELL"
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, sell_payload)
            sell_offset = manual_send_one_intent("sell-offset-intent", False, marketable_bps=5)
            if sell_offset.get("payload", {}).get("adjusted_limit_price", 0) >= sell_offset.get("payload", {}).get("limit_price", 0):
                raise AssertionError(f"SELL marketable offset did not decrease price: {sell_offset}")
            too_high_bps = manual_send_one_intent(str(ws_intent.get("intent_id")), False, marketable_bps=20.1)
            if too_high_bps.get("status") != "MARKETABLE_BPS_TOO_HIGH":
                raise AssertionError(f"marketable bps >20 should be rejected: {too_high_bps}")
            default_offset = manual_send_one_intent(str(ws_intent.get("intent_id")), False)
            if default_offset.get("payload", {}).get("adjusted_limit_price") != default_offset.get("payload", {}).get("limit_price"):
                raise AssertionError(f"default marketable offset should be unchanged: {default_offset}")
            invalid_would_send = dict(would_send_row)
            invalid_would_send["intent_id"] = "manual-review-intent"
            invalid_would_send["execution_decision"] = "MANUAL_REVIEW"
            invalid_would_send["manual_reconcile_required"] = True
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, invalid_would_send)
            refused = manual_send_one_intent("manual-review-intent", False)
            if refused.get("status") != "INTENT_REFUSED":
                raise AssertionError(f"manual review would-send row should be refused: {refused}")
            os.environ.pop("HL_LIVE_HL_PRIVATE_KEY", None)
            missing_creds = manual_send_one_intent(str(ws_intent.get("intent_id")), True)
            if missing_creds.get("status") != "CREDENTIALS_MISSING":
                raise AssertionError(f"confirmed manual send should require credentials: {missing_creds}")
            LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD = 1.0
            os.environ["HL_LIVE_HL_PRIVATE_KEY"] = "0x" + ("1" * 64)
            max_refused = manual_send_one_intent(str(ws_intent.get("intent_id")), True)
            if max_refused.get("status") != "MAX_NOTIONAL_EXCEEDED":
                raise AssertionError(f"manual send should enforce max notional: {max_refused}")
            LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD = 25.0
            safe_ok, safe_value, safe_error = wire_safe_float(0.038284839204, "size")
            if not safe_ok or safe_value <= 0 or safe_value > 0.038284839204:
                raise AssertionError(f"wire_safe_float did not round size safely: ok={safe_ok} value={safe_value} error={safe_error}")
            zero_ok, _zero_value, _zero_error = wire_safe_float(0, "size")
            negative_ok, _negative_value, _negative_error = wire_safe_float(-0.1, "size")
            if zero_ok or negative_ok:
                raise AssertionError("wire_safe_float should reject zero/negative values")
            HL_PERP_META_BY_COIN_CACHE = {"TAO": {"szDecimals": 3, "name": "TAO"}}
            tao_too_small = prepare_hl_order_numbers("TAO", 0.038284839204, 261.2, 25.0)
            if tao_too_small.get("status") != "MIN_NOTIONAL_AFTER_ROUNDING" or tao_too_small.get("wire_size") != 0.038:
                raise AssertionError(f"TAO lot preflight should floor size and reject min notional: {tao_too_small}")
            tao_valid = prepare_hl_order_numbers("TAO", 0.04, 261.2, 25.0)
            if not tao_valid.get("ok") or tao_valid.get("wire_size") != 0.04 or tao_valid.get("wire_limit_price") != 261.2:
                raise AssertionError(f"TAO lot preflight valid example failed: {tao_valid}")
            rejected_response = {"status": "ok", "response": {"data": {"statuses": [{"error": "Order has invalid size."}]}}}
            if not response_has_order_error(rejected_response):
                raise AssertionError(f"SDK error response was not detected: {rejected_response}")
            rejected_parsed = parse_hl_order_status(rejected_response)
            if rejected_parsed.get("status") != "ORDER_REJECTED" or rejected_parsed.get("ok"):
                raise AssertionError(f"SDK error response was not labelled rejected: {rejected_parsed}")
            filled_response = {"status": "ok", "response": {"data": {"statuses": [{"filled": {"avgPx": "0.20233", "oid": 406136339652, "totalSz": "51.8"}}]}}}
            filled_parsed = parse_hl_order_status(filled_response)
            if filled_parsed.get("status") != "ORDER_FILLED" or not filled_parsed.get("ok") or filled_parsed.get("fill_avg_px") != "0.20233" or filled_parsed.get("fill_size") != "51.8" or filled_parsed.get("oid") != 406136339652:
                raise AssertionError(f"filled SDK response was not labelled filled: {filled_parsed}")
            save_manual_live_positions({})
            entry_intent = {
                "created_at": utc_now_iso(), "dry_run": True, "intent_id": "manual-entry-fill",
                "leader_wallet": wallet, "leader_fill_id": "manual-entry-fill", "source_reason": "LIVE_MANUAL_TEST",
                "execution_decision": "WOULD_PLACE_IOC_LIMIT", "decision_reason": "LIVE_WS_FAST_PATH",
                "coin": "FARTCOIN", "side": "BUY", "copy_size": 51.8, "copy_notional": 10.477,
                "order_type": "IOC_LIMIT", "limit_price": 0.20233, "reduce_only": False,
                "manual_reconcile_required": False, "status": "WOULD_SEND_DRY_RUN", "notes": "self-test",
            }
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, entry_intent)
            exit_intent = dict(entry_intent)
            exit_intent.update({"intent_id": "manual-exit-fill", "side": "SELL", "copy_size": 51.5, "copy_notional": 10.46377, "limit_price": 0.20318, "reduce_only": True})
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, exit_intent)

            def fake_sender(payload: Dict[str, Any], marketable_bps: float = 0.0, use_current_quote: bool = False, max_notional: Optional[float] = None) -> Dict[str, Any]:
                return {
                    "ok": True, "status": "ORDER_FILLED", "fill_avg_px": str(payload.get("limit_price")),
                    "fill_size": str(payload.get("copy_size")), "oid": "selftest-oid",
                    "coin": payload.get("coin"), "side": payload.get("side"), "size": payload.get("copy_size"),
                    "limit_price": payload.get("limit_price"), "reduce_only": payload.get("reduce_only"),
                }

            send_hyperliquid_order = fake_sender
            entry_result = manual_send_one_intent("manual-entry-fill", True)
            if entry_result.get("response", {}).get("position_after") != 51.8:
                raise AssertionError(f"manual BUY fill did not update position: {entry_result}")
            exit_result = manual_send_one_intent("manual-exit-fill", True)
            if round(fnum(exit_result.get("response", {}).get("position_after")), 8) != 0.3:
                raise AssertionError(f"manual SELL reduce fill did not leave expected dust: {exit_result}")
            close_intent = dict(exit_intent)
            close_intent.update({"intent_id": "manual-close-dust", "copy_size": 99, "copy_notional": 20})
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, close_intent)
            close_preview = manual_send_one_intent("manual-close-dust", False, close_position=True)
            if close_preview.get("payload", {}).get("side") != "SELL" or not close_preview.get("payload", {}).get("reduce_only") or round(fnum(close_preview.get("payload", {}).get("copy_size")), 8) != 0.3:
                raise AssertionError(f"close-position preview did not use tracked dust: {close_preview}")
            save_manual_live_positions({"AVAX": {"signed_size": -1.15, "last_updated_at": utc_now_iso(), "last_oid": "short", "last_intent_id": "short"}})
            avax_intent = dict(close_intent)
            avax_intent.update({"intent_id": "manual-close-short-quote", "coin": "AVAX", "side": "SELL", "limit_price": 20.0})
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, avax_intent)

            def fake_close_quote(coin: str, side: str) -> Dict[str, Any]:
                if coin == "AVAX" and side == "BUY":
                    return {"ok": True, "coin": coin, "side": side, "bid": 21.0, "ask": 21.1, "executable_price": 21.1, "source": "test", "error": ""}
                if coin == "FARTCOIN" and side == "SELL":
                    return {"ok": True, "coin": coin, "side": side, "bid": 0.2, "ask": 0.21, "executable_price": 0.2, "source": "test", "error": ""}
                return {"ok": False, "coin": coin, "side": side, "bid": 0.0, "ask": 0.0, "executable_price": 0.0, "source": "test", "error": "missing"}

            fetch_public_executable_quote = fake_close_quote
            short_quote_preview = manual_send_one_intent("manual-close-short-quote", False, close_position=True, use_current_quote=True)
            if short_quote_preview.get("payload", {}).get("side") != "BUY" or short_quote_preview.get("payload", {}).get("limit_price") != 21.1 or short_quote_preview.get("payload", {}).get("price_source") != "current_quote":
                raise AssertionError(f"close short current quote did not use ask: {short_quote_preview}")
            save_manual_live_positions({"FARTCOIN": {"signed_size": 0.3, "last_updated_at": utc_now_iso(), "last_oid": "long", "last_intent_id": "long"}})
            long_quote_preview = manual_send_one_intent("manual-close-dust", False, close_position=True, use_current_quote=True)
            if long_quote_preview.get("payload", {}).get("side") != "SELL" or long_quote_preview.get("payload", {}).get("limit_price") != 0.2 or long_quote_preview.get("payload", {}).get("price_source") != "current_quote":
                raise AssertionError(f"close long current quote did not use bid: {long_quote_preview}")
            save_manual_live_positions({"AVAX": {"signed_size": -1.15, "last_updated_at": utc_now_iso(), "last_oid": "short", "last_intent_id": "short"}})
            fetch_public_executable_quote = lambda coin, side: {"ok": False, "coin": coin, "side": side, "error": "forced unavailable"}
            unavailable_quote = manual_send_one_intent("manual-close-short-quote", False, close_position=True, use_current_quote=True)
            if unavailable_quote.get("status") != "CURRENT_QUOTE_UNAVAILABLE":
                raise AssertionError(f"unavailable current quote should refuse close: {unavailable_quote}")
            fetch_public_executable_quote = old_fetch_public_executable_quote
            save_manual_live_positions({})
            no_position = manual_send_one_intent("manual-close-dust", False, close_position=True)
            if no_position.get("status") != "NO_MANUAL_POSITION_TO_CLOSE":
                raise AssertionError(f"close-position should refuse without tracked position: {no_position}")

            # --- close adverse diff guard tests ---
            # Uses TAO meta (szDecimals=3) already in cache.
            HL_PERP_META_BY_COIN_CACHE = {"TAO": {"szDecimals": 3, "name": "TAO"}}
            tao_close_row = {
                "created_at": utc_now_iso(), "dry_run": True, "intent_id": "tao-close-long",
                "leader_wallet": wallet, "leader_fill_id": "tao-close-long", "source_reason": "LIVE_MANUAL_TEST",
                "execution_decision": "WOULD_PLACE_IOC_LIMIT", "decision_reason": "LIVE_WS_FAST_PATH",
                "coin": "TAO", "side": "SELL", "copy_size": 0.05, "copy_notional": 13.06,
                "order_type": "IOC_LIMIT", "limit_price": 261.2, "reduce_only": True,
                "manual_reconcile_required": False, "status": "WOULD_SEND_DRY_RUN", "notes": "self-test",
            }
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, tao_close_row)
            tao_short_row = dict(tao_close_row)
            tao_short_row.update({"intent_id": "tao-close-short", "side": "BUY"})
            append_csv(WOULD_SEND_ORDERS_CSV, WOULD_SEND_ORDER_FIELDS, tao_short_row)

            # test: close over open cap (notional ~$13 > cap $10) with fine adverse diff → CONFIRM_REQUIRED
            # open order over cap is already asserted above (MAX_NOTIONAL_EXCEEDED at line ~2820).
            save_manual_live_positions({"TAO": {"signed_size": 0.05, "last_updated_at": utc_now_iso(), "last_oid": "long", "last_intent_id": "long"}})
            LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD = 10.0
            tao_close_over_cap = manual_send_one_intent("tao-close-long", False, close_position=True)
            LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD = 25.0
            if tao_close_over_cap.get("status") != "CONFIRM_REQUIRED":
                raise AssertionError(f"close over open cap with fine adverse diff should give CONFIRM_REQUIRED: {tao_close_over_cap}")
            if tao_close_over_cap.get("payload", {}).get("close_notional_cap_bypassed") is not True:
                raise AssertionError(f"close_notional_cap_bypassed missing from close payload: {tao_close_over_cap}")
            if tao_close_over_cap.get("payload", {}).get("notional_cap_reason") != "manual_close_diff_guard":
                raise AssertionError(f"notional_cap_reason mismatch for close: {tao_close_over_cap}")

            # test: SELL close adverse diff over tiny limit → CLOSE_ADVERSE_DIFF_TOO_LARGE
            # price 261.7654321 rounds DOWN to 261.76 (TAO szDecimals=3, 5-sig-fig rule);
            # SELL adverse = (ref - wire)/ref*100 ≈ 0.00207 % which exceeds 0.001 % limit.
            LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT = 0.001

            def fake_tao_close_quote(coin: str, side: str) -> Dict[str, Any]:
                return {"ok": True, "coin": coin, "side": side, "bid": 261.7654321, "ask": 261.7654321,
                        "executable_price": 261.7654321, "source": "test", "error": ""}

            fetch_public_executable_quote = fake_tao_close_quote
            tao_sell_adverse = manual_send_one_intent("tao-close-long", False, close_position=True, use_current_quote=True)
            if tao_sell_adverse.get("status") != "CLOSE_ADVERSE_DIFF_TOO_LARGE":
                raise AssertionError(f"SELL close adverse diff over tiny limit should be rejected: {tao_sell_adverse}")
            if not (fnum(tao_sell_adverse.get("close_adverse_diff_pct")) > 0):
                raise AssertionError(f"SELL close adverse diff should be positive: {tao_sell_adverse}")

            # test: BUY close (closing short) at same price is FAVORABLE (wire rounds DOWN for BUY)
            # → adverse=0 → passes even with tiny limit.
            save_manual_live_positions({"TAO": {"signed_size": -0.05, "last_updated_at": utc_now_iso(), "last_oid": "short", "last_intent_id": "short"}})
            tao_buy_favorable = manual_send_one_intent("tao-close-short", False, close_position=True, use_current_quote=True)
            if tao_buy_favorable.get("status") != "CONFIRM_REQUIRED":
                raise AssertionError(f"BUY close with favorable wire price should pass adverse diff guard: {tao_buy_favorable}")
            if fnum(tao_buy_favorable.get("payload", {}).get("close_adverse_diff_pct")) != 0.0:
                raise AssertionError(f"BUY close adverse diff should be 0 for favorable fill: {tao_buy_favorable}")

            LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT = 0.25
            fetch_public_executable_quote = old_fetch_public_executable_quote
            save_manual_live_positions({})
            # --- end close adverse diff guard tests ---

            send_hyperliquid_order = old_send_hyperliquid_order
            import builtins
            real_import = builtins.__import__

            def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
                if name.startswith("hyperliquid") or name.startswith("eth_account"):
                    raise ImportError("forced self-test SDK unavailable")
                return real_import(name, *args, **kwargs)

            try:
                builtins.__import__ = fake_import
                sdk_result = send_hyperliquid_order({"coin": "BTC", "side": "BUY", "order_type": "IOC_LIMIT", "limit_price": 100.0, "copy_size": 0.01, "copy_notional": 1.0, "reduce_only": False})
            finally:
                builtins.__import__ = real_import
            if sdk_result.get("status") != "SDK_UNAVAILABLE":
                raise AssertionError(f"forced SDK unavailable path returned unexpected status: {sdk_result}")
            if count_csv_data_rows(SEND_ATTEMPTS_CSV) < 3:
                raise AssertionError("send attempts ledger did not record manual send attempts")

            class ABNF:
                __module__ = "websocket._abnf"
                opcode = 8
                fin = 1
                data = (1000).to_bytes(2, "big") + b"test close"

            fake_abnf = ABNF()
            if not is_benign_ws_error(fake_abnf) or not is_ws_abnf_frame(fake_abnf):
                raise AssertionError("ABNF websocket callback object was not classified as benign")
            abnf_details = ws_abnf_details(fake_abnf)
            if abnf_details.get("close_code") != 1000 or abnf_details.get("close_reason") != "test close":
                raise AssertionError(f"ABNF close frame details mismatch: {abnf_details}")
            snapshot_reason = classify_ignored_ws_message(json.dumps({"channel": "userFills", "data": {"isSnapshot": True, "fills": []}}))
            if snapshot_reason != "SNAPSHOT":
                raise AssertionError(f"snapshot WS message classification mismatch: {snapshot_reason}")
            empty_reason = classify_ignored_ws_message(json.dumps({"channel": "userFills", "data": {"user": wallet, "fills": []}}))
            if empty_reason != "EMPTY_FILLS":
                raise AssertionError(f"empty WS message classification mismatch: {empty_reason}")
            snapshot_message = json.dumps({
                "channel": "userFills",
                "data": {
                    "isSnapshot": True,
                    "user": wallet,
                    "fills": [{
                        "fill_id": "snap-1",
                        "coin": "BTC",
                        "side": "BUY",
                        "px": "104",
                        "sz": "1",
                        "time": 2000,
                    }],
                },
            })
            snapshot_extracted = extract_ws_fills_with_meta(snapshot_message)
            if len(snapshot_extracted) != 1 or snapshot_extracted[0][2] is not True:
                raise AssertionError(f"snapshot fill was not extracted with metadata: {snapshot_extracted}")
            snapshot_fill = parse_ws_snapshot_leader_fill(snapshot_extracted[0][0], snapshot_extracted[0][1])
            if snapshot_fill is None or snapshot_fill.source != "live_ws_snapshot":
                raise AssertionError(f"snapshot fill parse/source mismatch: {snapshot_fill}")
            if audit_reason_for_fill(snapshot_fill) != "LIVE_WS_SNAPSHOT_RECOVERY":
                raise AssertionError(f"snapshot audit reason mismatch: {audit_reason_for_fill(snapshot_fill)}")
            if "guarded snapshot recovery" not in audit_notes_for_fill(snapshot_fill):
                raise AssertionError(f"snapshot audit notes mismatch: {audit_notes_for_fill(snapshot_fill)}")
            guard_service = DryRunLiveCopyService()
            guard_service.state["baselines"] = {wallet: {"baseline_ts_ms": 1000, "baseline_fill_id": "base"}}
            guard_service.processed_ids = set()
            guard_status = {"last_open_ms": 3000}
            if should_recover_ws_snapshot_fill(guard_service, guard_status, wallet, snapshot_fill) != "RECOVER":
                raise AssertionError("snapshot recovery guard rejected valid forward fill")
            old_snapshot = parse_ws_snapshot_leader_fill(wallet, {"fill_id": "snap-old", "coin": "BTC", "side": "BUY", "px": "104", "sz": "1", "time": 500})
            if old_snapshot is None or should_recover_ws_snapshot_fill(guard_service, guard_status, wallet, old_snapshot) != "PREBASELINE":
                raise AssertionError("snapshot recovery guard did not block prebaseline fill")
            duplicate_snapshot = parse_ws_snapshot_leader_fill(wallet, {"fill_id": "snap-dup", "coin": "BTC", "side": "BUY", "px": "104", "sz": "1", "time": 2500})
            guard_service.processed_ids.add("snap-dup")
            if duplicate_snapshot is None or should_recover_ws_snapshot_fill(guard_service, guard_status, wallet, duplicate_snapshot) != "DUPLICATE":
                raise AssertionError("snapshot recovery guard did not block duplicate fill")
            too_old_snapshot = parse_ws_snapshot_leader_fill(wallet, {"fill_id": "snap-too-old", "coin": "BTC", "side": "BUY", "px": "104", "sz": "1", "time": 2001})
            if too_old_snapshot is None or should_recover_ws_snapshot_fill(guard_service, {"last_open_ms": utc_now_ms() + LIVE_WS_SNAPSHOT_RECOVERY_GRACE_MS + 10000}, wallet, too_old_snapshot) != "OLD":
                raise AssertionError("snapshot recovery guard did not block outside-grace fill")

            manager = DedicatedLiveWSManager(service, [wallet])
            manager.update_status(
                wallet,
                status="OPEN",
                last_open_ms=utc_now_ms(),
                last_data_ms=0,
                benign_event_count=2,
                raw_message_count=3,
                ignored_empty_message_count=1,
                last_parsed_fill_ids=["ws-1"],
                ws_session_open_windows=[{"open_ms": 3500, "close_ms": 4500}],
                snapshot_fill_seen_count=3,
                snapshot_fill_recovered_count=1,
                snapshot_fill_skipped_duplicate_count=1,
                snapshot_fill_skipped_prebaseline_count=1,
                next_proactive_recycle_ms=utc_now_ms() + 50000,
                proactive_recycle_enabled=True,
            )
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
            if "ws_summary" not in health_snapshot:
                raise AssertionError(f"WS summary missing from health: {health_snapshot}")
            if "reconnects_per_min" not in wallet_health or "health_grade" not in wallet_health:
                raise AssertionError(f"WS stability metrics missing from wallet health: {health_snapshot}")
            if wallet_health.get("proactive_recycle_due_ms", 0) <= 0 or not wallet_health.get("proactive_recycle_enabled"):
                raise AssertionError(f"WS proactive recycle health fields missing: {health_snapshot}")
            if wallet_health.get("raw_message_count") != 3 or wallet_health.get("ignored_empty_message_count") != 1:
                raise AssertionError(f"WS completeness counters missing from health: {health_snapshot}")
            if wallet_health.get("last_parsed_fill_ids") != ["ws-1"]:
                raise AssertionError(f"WS parsed fill id window missing from health: {health_snapshot}")
            if wallet_health.get("snapshot_fill_seen_count") != 3 or wallet_health.get("snapshot_fill_recovered_count") != 1:
                raise AssertionError(f"WS snapshot recovery counters missing from health: {health_snapshot}")
            if ws_coverage_for_fill(ws_fill) != "UNKNOWN":
                raise AssertionError("WS coverage should be unknown before health file is written")
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
            overdue_wallet = "0xoverdue"
            manager.update_status(
                overdue_wallet,
                status="RECONNECTING",
                next_reconnect_ms=utc_now_ms() - LIVE_WS_RECONNECT_GRACE_MS - 1000,
                worker_thread_alive=False,
            )
            overdue_snapshot = build_ws_health_snapshot(manager)
            overdue_health = overdue_snapshot.get("wallets", {}).get(overdue_wallet, {})
            if overdue_health.get("effective_status") not in {"RECONNECT_OVERDUE", "RESTARTING", "THREAD_EXITED"}:
                raise AssertionError(f"overdue reconnect status mismatch: {overdue_snapshot}")
            if "ws_summary" not in overdue_snapshot:
                raise AssertionError(f"WS summary missing from overdue snapshot: {overdue_snapshot}")
            write_ws_health(manager)
            health = load_json(LIVE_WS_HEALTH_FILE, {})
            if health.get("mode") != "DEDICATED_SOCKET_PER_WALLET":
                raise AssertionError(f"WS health mode mismatch: {health}")
            if wallet not in health.get("wallets", {}):
                raise AssertionError(f"WS health missing wallet: {health}")
            if ws_coverage_for_fill(ws_fill) != "INSIDE_WS_SESSION":
                raise AssertionError(f"WS coverage did not detect session window: {health}")
        print("RESULT::LIVE_COPY_SERVICE_SELF_TEST_PASS")
        return True
    except Exception as exc:
        print(f"RESULT::LIVE_COPY_SERVICE_SELF_TEST_FAIL {exc}")
        return False
    finally:
        LIVE_POLL_ENABLED = old_poll_enabled
        LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD = old_max_manual_order_notional
        LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT = old_max_close_adverse_diff
        HL_PERP_META_BY_COIN_CACHE = old_meta_cache
        for key, value in {
            "HL_LIVE_HL_PRIVATE_KEY": old_private_key,
            "HL_LIVE_HL_ACCOUNT_ADDRESS": old_account_address,
        }.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        fetch_live_fills_since = old_fetch_live_fills_since
        fetch_public_executable_quote = old_fetch_public_executable_quote
        send_hyperliquid_order = old_send_hyperliquid_order
        configure_paths(old_paths[0], old_paths[1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run live copy service with explicit manual one-shot sender.")
    parser.add_argument("--once", action="store_true", help="Run one dry-run replay cycle and exit.")
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--interval", type=float, default=5.0, help="Loop interval seconds.")
    parser.add_argument("--replay-history", action="store_true", help="Replay historical source fills instead of creating/using live-copy baselines.")
    parser.add_argument("--self-test", action="store_true", help="Run a compact temp-file live-forward self-test.")
    parser.add_argument("--ws", action="store_true", help="Run dedicated dry-run live websocket sockets for active wallets.")
    parser.add_argument("--send-one-intent", help="Inspect or manually send one would-send intent_id through the disabled sender skeleton.")
    parser.add_argument("--confirm-send", action="store_true", help="Confirm the manual one-shot sender path. Sender remains disabled in this skeleton.")
    parser.add_argument("--marketable-bps", type=float, default=0.0, help="Optional manual IOC marketability offset in basis points, capped at 20.")
    parser.add_argument("--close-position", action="store_true", help="Manual sender only: size reduce-only order from tracked manual live position.")
    parser.add_argument("--use-current-quote", action="store_true", help="Manual close-position only: use current executable bid/ask instead of intent price.")
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
    if args.send_one_intent:
        print(json.dumps(manual_send_one_intent(args.send_one_intent, bool(args.confirm_send), marketable_bps=args.marketable_bps, close_position=bool(args.close_position), use_current_quote=bool(args.use_current_quote)), indent=2, sort_keys=True))
        return
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
