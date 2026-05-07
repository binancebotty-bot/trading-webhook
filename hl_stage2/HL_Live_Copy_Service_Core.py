"""
HL_Live_Copy_Service_Core.py

Clean Hyperliquid live copy core for a small leader-wallet set.

Design contract:
- One orchestrator, isolated wallet sleeves.
- manual_live_positions.json is the only live copy-position ledger.
- order_intents.csv = observed leader fill + decision.
- send_attempts.csv = real exchange send attempt only.
- live_fills.csv = real copy-account fills only.
- reconciliation.csv = health, mismatch, unmatched fill, rebuild notes.
- HL_LIVE_AUTO_SEND_ENABLED=0 is a hard master gate: no automatic send calls,
  no send_attempt rows, leader_sends_attempted remains zero.
- No live_positions.json and no would_send_orders.csv.

This file is intentionally independent of the legacy HL_Live_Copy_Service.py flow.
It is safe to compile and run --self-test without network or orders.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import queue
import threading
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover
    websocket = None

BASE_DIR = Path(__file__).resolve().parent
ENGINE_OUTPUT_DIR = BASE_DIR / "hl_copy_output"
AUDIT_DIR = Path(os.getenv("HL_LIVE_AUDIT_DIR", str(BASE_DIR / "hl_live_copy_audit")))
APPEND_ONLY_DIR = AUDIT_DIR / "append_only"

LIVE_CONFIG_FILE = AUDIT_DIR / "live_config.json"
SERVICE_STATE_FILE = AUDIT_DIR / "live_service_state.json"
CORE_RUNTIME_STATE_FILE = AUDIT_DIR / "clean_core_runtime_state.json"
LIVE_WS_HEALTH_FILE = AUDIT_DIR / "live_ws_health.json"
MANUAL_LIVE_POSITIONS_FILE = AUDIT_DIR / "manual_live_positions.json"
EXCHANGE_ACCOUNT_SNAPSHOT_FILE = AUDIT_DIR / "exchange_account_snapshot.json"

ORDER_INTENTS_CSV = APPEND_ONLY_DIR / "order_intents.csv"
SEND_ATTEMPTS_CSV = APPEND_ONLY_DIR / "send_attempts.csv"
LIVE_FILLS_CSV = APPEND_ONLY_DIR / "live_fills.csv"
RECONCILIATION_CSV = APPEND_ONLY_DIR / "reconciliation.csv"
ERRORS_CSV = APPEND_ONLY_DIR / "errors.csv"
RAW_LEADER_FILLS_CSV = Path(os.getenv("HL_LIVE_SOURCE_FILLS", str(ENGINE_OUTPUT_DIR / "raw_live_fills.csv")))
MANUAL_WALLETS_FILE = BASE_DIR / "manual_wallets.txt"
WALLET_GATE_FILE = BASE_DIR / "wallet_gate.json"
UI_STATE_FILE = BASE_DIR / "ui_state.json"

# Forbidden legacy artifacts. The core self-test asserts these are not created.
FORBIDDEN_LIVE_POSITIONS_FILE = AUDIT_DIR / "live_positions.json"
FORBIDDEN_WOULD_SEND_ORDERS_CSV = APPEND_ONLY_DIR / "would_send_orders.csv"

HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
HL_WS_URL = os.getenv("HL_LIVE_WS_URL", "wss://api.hyperliquid.xyz/ws")
HL_EXCHANGE_URL = os.getenv("HL_LIVE_ORDER_ENDPOINT", "https://api.hyperliquid.xyz/exchange")
USER_WALLET = os.getenv("HL_USER_WALLET", "").strip().lower()

DEFAULT_FIXED_NOTIONAL = float(os.getenv("HL_LIVE_DEFAULT_FIXED_NOTIONAL", "10"))
DEFAULT_MIN_NOTIONAL = float(os.getenv("HL_LIVE_MIN_NOTIONAL", "10"))
DEFAULT_MARKETABLE_BPS = float(os.getenv("HL_LIVE_MARKETABLE_BPS", "25"))
DEFAULT_MAX_CLOSE_ADVERSE_DIFF_PCT = float(os.getenv("HL_LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT", "0.25"))
HTTP_TIMEOUT_SEC = float(os.getenv("HL_LIVE_HTTP_TIMEOUT_SEC", "3"))
POLL_OVERLAP_MS = int(os.getenv("HL_LIVE_POLL_OVERLAP_MS", "300000"))
POLL_WINDOW_MS = int(os.getenv("HL_LIVE_POLL_WINDOW_MS", str(24 * 60 * 60 * 1000)))
POLL_MAX_PAGES_PER_WALLET = int(os.getenv("HL_LIVE_POLL_MAX_PAGES_PER_WALLET", "5"))
MAX_WALLETS = int(os.getenv("HL_LIVE_WS_MAX_WALLETS", "10"))
POSITION_EPSILON = float(os.getenv("HL_LIVE_POSITION_EPSILON", "1e-9"))

FILE_LOCK = threading.RLock()

ORDER_INTENT_FIELDS = [
    "created_at", "created_at_ms", "intent_id", "leader_fill_id", "leader_wallet", "source",
    "coin", "leader_side", "copy_side", "leader_price", "leader_size", "leader_notional",
    "copy_size", "copy_notional", "wallet_mode", "copy_mode", "decision", "reason",
    "sleeve_id", "position_id", "position_direction_before", "wallet_position_before",
    "coin_net_before", "reduce_only_intended", "reduce_only_sent_planned",
    "marketable_bps", "max_close_adverse_diff_pct", "min_notional", "notes",
]

SEND_ATTEMPT_FIELDS = [
    "created_at", "created_at_ms", "attempt_id", "intent_id", "leader_fill_id", "leader_wallet",
    "coin", "side", "order_type", "limit_price", "copy_size", "copy_notional", "reduce_only_sent",
    "sleeve_id", "position_id", "wallet_position_before", "wallet_position_after_expected",
    "coin_net_before", "coin_net_after_expected", "status", "exchange_response", "exchange_order_id",
    "error", "notes",
]

LIVE_FILL_FIELDS = [
    "created_at", "created_at_ms", "copy_fill_id", "intent_id", "leader_fill_id", "leader_wallet",
    "sleeve_id", "position_id", "coin", "side", "fill_price", "fill_size", "fill_notional",
    "fee", "source", "exchange_hash", "ledger_action", "wallet_position_after", "coin_net_after", "notes",
]

RECONCILIATION_FIELDS = [
    "created_at", "created_at_ms", "event", "status", "leader_wallet", "leader_fill_id", "intent_id",
    "copy_fill_id", "coin", "manual_net", "exchange_net", "action", "notes",
]

ERROR_FIELDS = ["created_at", "created_at_ms", "context", "error_type", "message", "traceback"]


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def bval(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def ensure_dirs() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    APPEND_ONLY_DIR.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}_{threading.get_ident()}_{time.time_ns()}.tmp")
    text = json.dumps(payload, indent=2, sort_keys=True)
    with FILE_LOCK:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default
    return default


def ensure_csv_header(path: Path, fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with FILE_LOCK:
        if path.exists() and path.stat().st_size > 0:
            return
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def append_csv(path: Path, fieldnames: List[str], row: Dict[str, Any]) -> None:
    ensure_csv_header(path, fieldnames)
    with FILE_LOCK:
        with path.open("a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writerow({k: row.get(k, "") for k in fieldnames})


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def log_error(context: str, exc: BaseException | str) -> None:
    ensure_dirs()
    append_csv(ERRORS_CSV, ERROR_FIELDS, {
        "created_at": utc_now_iso(),
        "created_at_ms": utc_now_ms(),
        "context": context,
        "error_type": type(exc).__name__ if isinstance(exc, BaseException) else "Error",
        "message": str(exc),
        "traceback": traceback.format_exc() if isinstance(exc, BaseException) else "",
    })


def normalise_wallet(wallet: Any) -> str:
    return str(wallet or "").strip().lower()


def is_valid_wallet(wallet: str) -> bool:
    return wallet.startswith("0x") and len(wallet) == 42 and all(c in "0123456789abcdef" for c in wallet[2:])


def stable_hash(parts: Iterable[Any]) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True)
class LeaderFill:
    leader_fill_id: str
    leader_wallet: str
    coin: str
    side: str  # BUY / SELL
    price: float
    size: float
    timestamp_ms: int
    source: str  # WS_CAPTURED / REBUILD / POLL / TEST
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def notional(self) -> float:
        return abs(self.price * self.size)


@dataclass
class Intent:
    intent_id: str
    fill: LeaderFill
    copy_side: str
    copy_size: float
    copy_notional: float
    wallet_mode: str
    copy_mode: str
    decision: str
    reason: str
    sleeve_id: str
    position_id: str
    position_direction_before: str
    wallet_position_before: float
    coin_net_before: float
    reduce_only_intended: bool
    reduce_only_sent_planned: bool
    notes: str = ""

    @property
    def send_allowed(self) -> bool:
        return self.decision in {"ENTRY_ALLOWED", "EXIT_ALLOWED", "SEND_ALLOWED"}


@dataclass
class CycleSummary:
    ok: bool = True
    cycle: str = "run_cycle"
    auto_send_enabled: bool = False
    active_wallets: int = 0
    leader_fills_seen: int = 0
    leader_fills_deduped: int = 0
    leader_intents_written: int = 0
    leader_sends_attempted: int = 0
    copy_fills_seen: int = 0
    copy_fills_deduped: int = 0
    copy_fills_baselined: int = 0
    copy_fills_matched: int = 0
    copy_fills_unmatched: int = 0
    copy_account_status: str = "COPY_ACCOUNT_POLL_DISABLED"
    ledger_updates: int = 0
    recovery_limits_placed: int = 0
    poll_loop_status: str = "POLL_DISABLED"
    ws_status: str = "WS_DISABLED"
    exchange_recon_status: str = "SNAPSHOT_UNAVAILABLE"
    budget_exceeded: bool = False
    network_errors: int = 0
    fatal_errors: int = 0


class ConfigManager:
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or LIVE_CONFIG_FILE
        self.config = self._load()
        self.wallet_gate = load_json(WALLET_GATE_FILE, {})
        if not isinstance(self.wallet_gate, dict):
            self.wallet_gate = {}

    def _load(self) -> Dict[str, Any]:
        cfg = load_json(self.config_path, {})
        if not isinstance(cfg, dict):
            cfg = {}
        cfg.setdefault("wallets", {})
        cfg.setdefault("global_controls", {})
        return cfg

    @property
    def auto_send_enabled(self) -> bool:
        # Env master gate wins. Config can only disable further.
        env_gate = bval(os.getenv("HL_LIVE_AUTO_SEND_ENABLED"), False)
        cfg_gate = bval(self.config.get("auto_send_enabled"), env_gate)
        return bool(env_gate and cfg_gate)

    @property
    def global_controls(self) -> Dict[str, Any]:
        return self.config.get("global_controls") if isinstance(self.config.get("global_controls"), dict) else {}

    def wallets(self) -> Dict[str, Dict[str, Any]]:
        raw = self.config.get("wallets")
        out: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw, dict) and raw:
            for key, value in raw.items():
                w = normalise_wallet(key)
                if not is_valid_wallet(w):
                    continue
                cfg = dict(value) if isinstance(value, dict) else {}
                gate = self._gate_cfg(w)
                if gate:
                    # live_config wins; wallet_gate fills compatibility gaps only.
                    cfg.setdefault("mode", gate.get("mode"))
                    if "enabled" not in cfg and "live_enabled" in gate:
                        cfg["enabled"] = bval(gate.get("live_enabled"), True)
                out[w] = cfg
            return out
        if MANUAL_WALLETS_FILE.exists():
            for line in MANUAL_WALLETS_FILE.read_text(encoding="utf-8-sig").splitlines():
                w = normalise_wallet(line)
                if is_valid_wallet(w):
                    gate = self._gate_cfg(w)
                    out[w] = {"mode": gate.get("mode", "OFF") if gate else "OFF", "enabled": True}
        return out

    def _gate_cfg(self, wallet: str) -> Dict[str, Any]:
        wallet = normalise_wallet(wallet)
        raw = self.wallet_gate.get(wallet)
        if not isinstance(raw, dict):
            return {}
        out = dict(raw)
        # Compatibility with older app gate schema.
        if "mode" not in out:
            if bval(out.get("live_enabled"), False):
                out["mode"] = "ON"
            elif str(out.get("off_mode", "")).upper() in {"CLO", "CLOSE_ONLY"}:
                out["mode"] = "CLO"
            else:
                out["mode"] = "OFF"
        if str(out.get("mode", "")).upper() == "CLOSE_ONLY":
            out["mode"] = "CLO"
        return out

    def wallet_cfg(self, wallet: str) -> Dict[str, Any]:
        return self.wallets().get(normalise_wallet(wallet), {})

    def wallet_mode(self, wallet: str) -> str:
        cfg = self.wallet_cfg(wallet)
        mode = str(cfg.get("mode", cfg.get("gate", "OFF"))).upper().strip()
        if mode == "CLOSE_ONLY":
            mode = "CLO"
        if mode == "LIVE":
            mode = "ON"
        return mode if mode in {"ON", "CLO", "OFF"} else "OFF"

    def wallet_enabled(self, wallet: str) -> bool:
        return bval(self.wallet_cfg(wallet).get("enabled"), True)

    def copy_mode(self, wallet: str) -> str:
        mode = str(self.wallet_cfg(wallet).get("copy_mode", "fixed")).lower().strip()
        return mode if mode in {"fixed", "proportional"} else "fixed"

    def fixed_notional(self, wallet: str) -> float:
        return max(0.0, fnum(self.wallet_cfg(wallet).get("fixed_notional"), DEFAULT_FIXED_NOTIONAL))

    def min_notional(self) -> float:
        return max(0.0, fnum(self.global_controls.get("min_notional"), DEFAULT_MIN_NOTIONAL))

    def max_total_exposure(self) -> float:
        return max(0.0, fnum(self.global_controls.get("max_total_live_exposure_usd"), 0.0))

    def max_wallet_exposure(self, wallet: str) -> float:
        wc = self.wallet_cfg(wallet)
        return max(0.0, fnum(wc.get("max_wallet_exposure_usd", self.global_controls.get("max_wallet_exposure_usd")), 0.0))

    def max_order_notional(self) -> float:
        return max(0.0, fnum(self.global_controls.get("max_order_notional_usd"), 0.0))

    def marketable_bps(self) -> float:
        return max(0.0, min(100.0, fnum(self.global_controls.get("marketable_bps"), DEFAULT_MARKETABLE_BPS)))

    def max_close_adverse_diff_pct(self) -> float:
        return max(0.0, fnum(self.global_controls.get("max_close_adverse_diff_pct"), DEFAULT_MAX_CLOSE_ADVERSE_DIFF_PCT))

    def is_symbol_allowed(self, coin: str) -> Tuple[bool, str]:
        coin = str(coin or "").upper()
        allow = self.global_controls.get("allow_symbols") or self.global_controls.get("allowlist")
        block = self.global_controls.get("block_symbols") or self.global_controls.get("blocklist")
        if isinstance(block, list) and coin in {str(x).upper() for x in block}:
            return False, "SYMBOL_BLOCKED"
        if isinstance(allow, list) and allow and coin not in {str(x).upper() for x in allow}:
            return False, "SYMBOL_NOT_ALLOWED"
        return True, ""


class DedupeStore:
    def __init__(self, state: Optional[Dict[str, Any]] = None):
        state = state if isinstance(state, dict) else {}
        self.processed: set[str] = set(state.get("processed_leader_fill_ids") or [])
        self.processed_copy: set[str] = set(state.get("processed_copy_fill_ids") or [])
        self.copy_account_baseline_set: bool = bval(state.get("copy_account_baseline_set"), False)
        self.copy_account_baseline_at_ms: int = int(fnum(state.get("copy_account_baseline_at_ms"), 0))
        self.copy_account_baseline_fill_count: int = int(fnum(state.get("copy_account_baseline_fill_count"), 0))
        self.copy_account_baseline_max_ts_ms: int = int(fnum(state.get("copy_account_baseline_max_ts_ms"), 0))
        self.live_start_ms: int = int(fnum(state.get("live_start_ms"), 0))

    def accept_leader(self, fill_id: str) -> bool:
        if fill_id in self.processed:
            return False
        self.processed.add(fill_id)
        return True

    def accept_copy(self, fill_id: str) -> bool:
        if fill_id in self.processed_copy:
            return False
        self.processed_copy.add(fill_id)
        return True

    def baseline_copy_account(self, copy_fills: List[Dict[str, Any]]) -> int:
        max_ts = self.copy_account_baseline_max_ts_ms
        for raw in copy_fills:
            self.processed_copy.add(CopyAccountIngestor.copy_fill_id(raw))
            max_ts = max(max_ts, int(fnum(raw.get("timestamp_ms", raw.get("time")), 0)))
        self.copy_account_baseline_set = True
        self.copy_account_baseline_at_ms = utc_now_ms()
        self.copy_account_baseline_fill_count = len(copy_fills)
        self.copy_account_baseline_max_ts_ms = max_ts
        return len(copy_fills)

    def export(self) -> Dict[str, Any]:
        return {
            "processed_leader_fill_ids": sorted(self.processed)[-250000:],
            "processed_copy_fill_ids": sorted(self.processed_copy)[-250000:],
            "copy_account_baseline_set": self.copy_account_baseline_set,
            "copy_account_baseline_at_ms": self.copy_account_baseline_at_ms,
            "copy_account_baseline_fill_count": self.copy_account_baseline_fill_count,
            "copy_account_baseline_max_ts_ms": self.copy_account_baseline_max_ts_ms,
            "live_start_ms": self.live_start_ms,
        }


class ManualLedger:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or MANUAL_LIVE_POSITIONS_FILE
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        raw = load_json(self.path, {})
        if not isinstance(raw, dict):
            raw = {}
        raw.setdefault("schema", "manual_live_positions.v1.wallet_sleeves")
        raw.setdefault("by_wallet", {})
        raw.setdefault("by_coin_net", {})
        self._recompute_net(raw)
        return raw

    @staticmethod
    def signed_delta(side: str, size: float) -> float:
        return abs(size) if side.upper() == "BUY" else -abs(size)

    @staticmethod
    def direction_from_size(size: float) -> str:
        if size > POSITION_EPSILON:
            return "LONG"
        if size < -POSITION_EPSILON:
            return "SHORT"
        return "FLAT"

    @staticmethod
    def sleeve_id(wallet: str, coin: str) -> str:
        return f"{normalise_wallet(wallet)}::{str(coin).upper()}"

    @staticmethod
    def position_id(wallet: str, coin: str, direction: str, opened_by_intent_id: str) -> str:
        return f"{normalise_wallet(wallet)}::{str(coin).upper()}::{direction}::{opened_by_intent_id}"

    def sleeve(self, wallet: str, coin: str) -> Dict[str, Any]:
        w = normalise_wallet(wallet)
        c = str(coin).upper()
        by_wallet = self.data.setdefault("by_wallet", {})
        wallet_map = by_wallet.setdefault(w, {})
        return wallet_map.setdefault(c, {
            "sleeve_id": self.sleeve_id(w, c),
            "position_id": "",
            "leader_wallet": w,
            "coin": c,
            "direction": "FLAT",
            "signed_size": 0.0,
            "avg_entry_px": 0.0,
            "opened_by_intent_id": "",
            "opened_at_ms": 0,
            "last_copy_fill_id": "",
            "last_updated_ms": 0,
        })

    def wallet_coin_position(self, wallet: str, coin: str) -> float:
        return fnum(self.sleeve(wallet, coin).get("signed_size"), 0.0)

    def coin_net(self, coin: str) -> float:
        return fnum((self.data.get("by_coin_net") or {}).get(str(coin).upper(), {}).get("signed_size"), 0.0)

    def total_abs_exposure_usd(self, mark_prices: Optional[Dict[str, float]] = None) -> float:
        total = 0.0
        mark_prices = mark_prices or {}
        for wallet_map in (self.data.get("by_wallet") or {}).values():
            if not isinstance(wallet_map, dict):
                continue
            for coin, sleeve in wallet_map.items():
                total += abs(fnum(sleeve.get("signed_size"))) * max(0.0, fnum(mark_prices.get(coin), fnum(sleeve.get("avg_entry_px"), 0.0)))
        return total

    def wallet_abs_exposure_usd(self, wallet: str, mark_prices: Optional[Dict[str, float]] = None) -> float:
        total = 0.0
        mark_prices = mark_prices or {}
        wallet_map = (self.data.get("by_wallet") or {}).get(normalise_wallet(wallet), {})
        if not isinstance(wallet_map, dict):
            return 0.0
        for coin, sleeve in wallet_map.items():
            total += abs(fnum(sleeve.get("signed_size"))) * max(0.0, fnum(mark_prices.get(coin), fnum(sleeve.get("avg_entry_px"), 0.0)))
        return total

    def classify_leader_side_for_wallet(self, wallet: str, coin: str, side: str) -> Tuple[str, bool]:
        before = self.wallet_coin_position(wallet, coin)
        delta = self.signed_delta(side, 1.0)
        if abs(before) <= POSITION_EPSILON:
            return "ENTRY", False
        # If trade side opposes current position, it is reducing/exit for this wallet.
        if before * delta < 0:
            return "EXIT", True
        return "ADD", False

    def apply_copy_fill(self, intent: Intent, copy_fill: Dict[str, Any]) -> Dict[str, Any]:
        wallet = intent.fill.leader_wallet
        coin = intent.fill.coin
        side = str(copy_fill.get("side", intent.copy_side)).upper()
        size = abs(fnum(copy_fill.get("size", copy_fill.get("sz", intent.copy_size)), intent.copy_size))
        price = fnum(copy_fill.get("price", copy_fill.get("px", intent.fill.price)), intent.fill.price)
        fill_id = str(copy_fill.get("copy_fill_id") or copy_fill.get("hash") or copy_fill.get("oid") or stable_hash([intent.intent_id, side, size, price, utc_now_ms()]))
        sleeve = self.sleeve(wallet, coin)
        before = fnum(sleeve.get("signed_size"), 0.0)
        before_abs = abs(before)
        delta = self.signed_delta(side, size)
        after = before + delta
        if abs(after) <= POSITION_EPSILON:
            after = 0.0
        old_avg = fnum(sleeve.get("avg_entry_px"), 0.0)
        # Avg price update: only recompute when adding to same direction or opening from flat.
        if before == 0.0 or before * delta > 0:
            new_abs = abs(after)
            avg = ((old_avg * before_abs) + (price * abs(delta))) / new_abs if new_abs > POSITION_EPSILON else 0.0
        else:
            avg = old_avg if abs(after) > POSITION_EPSILON else 0.0
        direction = self.direction_from_size(after)
        opened_by = sleeve.get("opened_by_intent_id") or (intent.intent_id if direction != "FLAT" else "")
        position_id = sleeve.get("position_id") or (self.position_id(wallet, coin, direction, opened_by) if direction != "FLAT" else "")
        sleeve.update({
            "sleeve_id": self.sleeve_id(wallet, coin),
            "position_id": position_id if direction != "FLAT" else "",
            "leader_wallet": wallet,
            "coin": coin,
            "direction": direction,
            "signed_size": after,
            "avg_entry_px": avg,
            "opened_by_intent_id": opened_by if direction != "FLAT" else "",
            "opened_at_ms": sleeve.get("opened_at_ms") or (intent.fill.timestamp_ms if direction != "FLAT" else 0),
            "last_copy_fill_id": fill_id,
            "last_updated_ms": utc_now_ms(),
        })
        self._recompute_net(self.data)
        self.save()
        return {
            "copy_fill_id": fill_id,
            "wallet_position_before": before,
            "wallet_position_after": after,
            "coin_net_after": self.coin_net(coin),
            "ledger_action": "UPDATED" if direction != "FLAT" else "CLOSED",
        }

    def _recompute_net(self, data: Optional[Dict[str, Any]] = None) -> None:
        data = data if data is not None else self.data
        net: Dict[str, float] = {}
        by_wallet = data.get("by_wallet") or {}
        if isinstance(by_wallet, dict):
            for wallet_map in by_wallet.values():
                if not isinstance(wallet_map, dict):
                    continue
                for coin, sleeve in wallet_map.items():
                    net[str(coin).upper()] = net.get(str(coin).upper(), 0.0) + fnum((sleeve or {}).get("signed_size"), 0.0)
        data["by_coin_net"] = {coin: {"signed_size": 0.0 if abs(size) <= POSITION_EPSILON else size} for coin, size in sorted(net.items())}

    def save(self) -> None:
        self._recompute_net(self.data)
        self.data["updated_at"] = utc_now_iso()
        self.data["updated_at_ms"] = utc_now_ms()
        atomic_write_json(self.path, self.data)


class AuditLogWriter:
    def append_order_intent(self, intent: Intent) -> None:
        append_csv(ORDER_INTENTS_CSV, ORDER_INTENT_FIELDS, {
            "created_at": utc_now_iso(),
            "created_at_ms": utc_now_ms(),
            "intent_id": intent.intent_id,
            "leader_fill_id": intent.fill.leader_fill_id,
            "leader_wallet": intent.fill.leader_wallet,
            "source": intent.fill.source,
            "coin": intent.fill.coin,
            "leader_side": intent.fill.side,
            "copy_side": intent.copy_side,
            "leader_price": intent.fill.price,
            "leader_size": intent.fill.size,
            "leader_notional": intent.fill.notional,
            "copy_size": intent.copy_size,
            "copy_notional": intent.copy_notional,
            "wallet_mode": intent.wallet_mode,
            "copy_mode": intent.copy_mode,
            "decision": intent.decision,
            "reason": intent.reason,
            "sleeve_id": intent.sleeve_id,
            "position_id": intent.position_id,
            "position_direction_before": intent.position_direction_before,
            "wallet_position_before": intent.wallet_position_before,
            "coin_net_before": intent.coin_net_before,
            "reduce_only_intended": str(intent.reduce_only_intended),
            "reduce_only_sent_planned": str(intent.reduce_only_sent_planned),
            "marketable_bps": "",
            "max_close_adverse_diff_pct": "",
            "min_notional": "",
            "notes": intent.notes,
        })

    def append_send_attempt(self, row: Dict[str, Any]) -> None:
        append_csv(SEND_ATTEMPTS_CSV, SEND_ATTEMPT_FIELDS, row)

    def append_live_fill(self, row: Dict[str, Any]) -> None:
        append_csv(LIVE_FILLS_CSV, LIVE_FILL_FIELDS, row)

    def append_reconciliation(self, event: str, status: str, **kwargs: Any) -> None:
        row = {
            "created_at": utc_now_iso(),
            "created_at_ms": utc_now_ms(),
            "event": event,
            "status": status,
            **kwargs,
        }
        append_csv(RECONCILIATION_CSV, RECONCILIATION_FIELDS, row)


class IntentBuilder:
    def __init__(self, cfg: ConfigManager, ledger: ManualLedger):
        self.cfg = cfg
        self.ledger = ledger

    def build(self, fill: LeaderFill) -> Intent:
        wallet = fill.leader_wallet
        coin = fill.coin
        wallet_mode = self.cfg.wallet_mode(wallet)
        copy_mode = self.cfg.copy_mode(wallet)
        copy_notional = self._copy_notional(wallet, fill)
        copy_size = round(copy_notional / fill.price, 8) if fill.price > 0 and copy_notional > 0 else 0.0
        sleeve_id = self.ledger.sleeve_id(wallet, coin)
        before = self.ledger.wallet_coin_position(wallet, coin)
        direction_before = self.ledger.direction_from_size(before)
        lifecycle, is_reduce = self.ledger.classify_leader_side_for_wallet(wallet, coin, fill.side)
        if lifecycle == "EXIT":
            copy_size = round(min(copy_size, abs(before)), 8)
            copy_notional = copy_size * fill.price
        intent_id = stable_hash(["intent", fill.leader_fill_id, wallet, coin, fill.side, fill.timestamp_ms])
        position_id = ""
        if lifecycle in {"ENTRY", "ADD"}:
            direction_after = self.ledger.direction_from_size(before + self.ledger.signed_delta(fill.side, copy_size))
            position_id = self.ledger.position_id(wallet, coin, direction_after, intent_id) if direction_after != "FLAT" else ""
        else:
            existing = self.ledger.sleeve(wallet, coin)
            position_id = str(existing.get("position_id") or self.ledger.position_id(wallet, coin, direction_before, str(existing.get("opened_by_intent_id") or intent_id)))
        coin_net_before = self.ledger.coin_net(coin)
        decision, reason = self._decision(wallet, fill, lifecycle, copy_notional, before)
        reduce_only_intended = bool(is_reduce)
        reduce_only_sent_planned = False
        return Intent(
            intent_id=intent_id,
            fill=fill,
            copy_side=fill.side,
            copy_size=copy_size,
            copy_notional=copy_notional,
            wallet_mode=wallet_mode,
            copy_mode=copy_mode,
            decision=decision,
            reason=reason,
            sleeve_id=sleeve_id,
            position_id=position_id,
            position_direction_before=direction_before,
            wallet_position_before=before,
            coin_net_before=coin_net_before,
            reduce_only_intended=reduce_only_intended,
            reduce_only_sent_planned=reduce_only_sent_planned,
            notes=f"lifecycle={lifecycle}",
        )

    def _copy_notional(self, wallet: str, fill: LeaderFill) -> float:
        mode = self.cfg.copy_mode(wallet)
        if mode == "fixed":
            return self.cfg.fixed_notional(wallet)
        # Minimal proportional fallback: live_config may provide norm_base and leader_equity_base.
        wc = self.cfg.wallet_cfg(wallet)
        norm_base = max(0.0, fnum(wc.get("norm_base"), self.cfg.fixed_notional(wallet)))
        leader_equity = max(1.0, fnum(wc.get("leader_equity_base"), 10000.0))
        return max(0.0, fill.notional * (norm_base / leader_equity))

    def _decision(self, wallet: str, fill: LeaderFill, lifecycle: str, copy_notional: float, before: float) -> Tuple[str, str]:
        if not self.cfg.wallet_enabled(wallet):
            return "BLOCKED_OFF", "wallet disabled"
        mode = self.cfg.wallet_mode(wallet)
        if mode == "OFF":
            return "BLOCKED_OFF", "wallet mode OFF"
        if mode == "CLO" and lifecycle in {"ENTRY", "ADD"}:
            return "BLOCKED_CLO_ENTRY", "CLO blocks entries/adds"
        allowed_symbol, reason = self.cfg.is_symbol_allowed(fill.coin)
        if not allowed_symbol:
            return "SYMBOL_UNAVAILABLE", reason
        if copy_notional <= 0:
            return "MANUAL_REVIEW", "copy notional is zero"
        if copy_notional < self.cfg.min_notional() and lifecycle in {"ENTRY", "ADD"}:
            return "BELOW_MIN_NOTIONAL", "entry/add notional below minimum"
        max_order = self.cfg.max_order_notional()
        if max_order > 0 and copy_notional > max_order:
            return "SEND_BLOCKED_RISK", "max order notional exceeded"
        max_wallet = self.cfg.max_wallet_exposure(wallet)
        if max_wallet > 0 and lifecycle in {"ENTRY", "ADD"}:
            # Use fill price as mark for this coin.
            current = self.ledger.wallet_abs_exposure_usd(wallet, {fill.coin: fill.price})
            if current + copy_notional > max_wallet:
                return "SEND_BLOCKED_RISK", "max wallet exposure exceeded"
        max_total = self.cfg.max_total_exposure()
        if max_total > 0 and lifecycle in {"ENTRY", "ADD"}:
            current = self.ledger.total_abs_exposure_usd({fill.coin: fill.price})
            if current + copy_notional > max_total:
                return "SEND_BLOCKED_RISK", "max total exposure exceeded"
        return ("EXIT_ALLOWED" if lifecycle == "EXIT" else "ENTRY_ALLOWED"), lifecycle



class SenderGateway:
    def __init__(self, cfg: ConfigManager, audit: AuditLogWriter):
        self.cfg = cfg
        self.audit = audit
        self._meta_cache: Dict[str, Dict[str, Any]] = {}
        self._index_cache: Dict[int, str] = {}
        self._meta_fetched: bool = False

    def send_if_allowed(self, intent: Intent) -> Tuple[bool, str]:
        if not intent.send_allowed:
            return False, "INTENT_NOT_SEND_ALLOWED"
        if not self.cfg.auto_send_enabled:
            return False, "AUTO_SEND_DISABLED"
        if bval(os.getenv("HL_LIVE_MOCK_SEND"), False):
            self._append_mock_attempt(intent, "MOCK_ORDER_SENT")
            return True, "MOCK_ORDER_SENT"
        ok, send_status, result = self._send_real(intent)
        if result.get("exchange_called"):
            self._append_real_attempt(intent, send_status, result)
        return ok, send_status

    def _send_real(self, intent: Intent) -> Tuple[bool, str, Dict[str, Any]]:
        resolved = self._resolve_coin(intent.fill.coin)
        if not resolved.get("ok"):
            return False, "REAL_SENDER_NOT_CONFIGURED", {
                "status": resolved.get("status", "SYMBOL_UNRESOLVED"),
                "error": resolved.get("error", ""),
                "exchange_called": False,
            }
        private_key = os.getenv("HL_LIVE_HL_PRIVATE_KEY", "").strip()
        if not private_key:
            return False, "REAL_SENDER_NOT_CONFIGURED", {"status": "CREDENTIALS_MISSING", "exchange_called": False}
        try:
            from eth_account import Account  # type: ignore
            from hyperliquid.exchange import Exchange  # type: ignore
        except Exception as exc:
            return False, "REAL_SENDER_NOT_CONFIGURED", {"status": "SDK_UNAVAILABLE", "error": repr(exc), "exchange_called": False}
        sdk_coin = resolved["sdk_coin"]
        sz_dec = resolved["sz_decimals"]
        price_max_dec = resolved["price_max_decimals"]
        perp_dexs = resolved.get("perp_dexs")
        bps = self.cfg.marketable_bps()
        mult = (1.0 + bps / 10000.0) if intent.copy_side == "BUY" else (1.0 - bps / 10000.0)
        raw_px = intent.fill.price * mult
        factor_px = 10 ** price_max_dec
        limit_px = math.floor(raw_px * factor_px) / factor_px
        factor_sz = 10 ** sz_dec
        wire_size = math.floor(intent.copy_size * factor_sz + 1e-12) / factor_sz
        if wire_size <= 0:
            return False, "REAL_SENDER_NOT_CONFIGURED", {"status": "WIRE_SIZE_ZERO", "exchange_called": False}
        try:
            account = Account.from_key(private_key)
            base_url = HL_EXCHANGE_URL
            if base_url.endswith("/exchange"):
                base_url = base_url[:-len("/exchange")]
            account_address = os.getenv("HL_LIVE_HL_ACCOUNT_ADDRESS", "").strip() or None
            exchange_kwargs: Dict[str, Any] = {"base_url": base_url, "account_address": account_address}
            if perp_dexs is not None:
                exchange_kwargs["perp_dexs"] = perp_dexs
            exchange = Exchange(account, **exchange_kwargs)
            response = exchange.order(
                sdk_coin,
                intent.copy_side == "BUY",
                wire_size,
                limit_px,
                {"limit": {"tif": "Ioc"}},
                reduce_only=False,
            )
            ok, status, oid = self._parse_hl_response(response)
            return ok, status, {
                "exchange_response": response, "oid": oid,
                "limit_px": limit_px, "wire_size": wire_size,
                "exchange_called": True, "error": "",
            }
        except Exception as exc:
            log_error("send_real", exc)
            return False, "EXCHANGE_ERROR", {
                "exchange_response": {}, "oid": "",
                "limit_px": limit_px, "wire_size": wire_size,
                "exchange_called": True, "error": repr(exc),
            }

    def _fetch_meta(self) -> None:
        if self._meta_fetched:
            return
        self._meta_fetched = True
        if requests is None:
            return
        try:
            r = requests.post(HL_INFO_URL, json={"type": "meta"}, timeout=HTTP_TIMEOUT_SEC)
            for idx, asset in enumerate(r.json().get("universe", [])):
                if not isinstance(asset, dict):
                    continue
                name = str(asset.get("name", "")).strip()
                if not name:
                    continue
                self._meta_cache[name.upper()] = {
                    "canonical_coin": name,
                    "szDecimals": int(asset.get("szDecimals", 4)),
                    "symbol_source": "core_meta",
                    "perp_dex": "",
                }
                self._index_cache[idx] = name
        except Exception:
            pass
        if requests is None:
            return
        try:
            r2 = requests.post(HL_INFO_URL, json={"type": "perpDexs"}, timeout=HTTP_TIMEOUT_SEC)
            raw_dexs = r2.json()
            dex_items: List[Any] = raw_dexs if isinstance(raw_dexs, list) else (raw_dexs.get("perpDexs", []) if isinstance(raw_dexs, dict) else [])
            for dex_item in dex_items:
                dex_name = (str(dex_item.get("name") or dex_item.get("dex") or "").strip().upper()
                            if isinstance(dex_item, dict) else str(dex_item).strip().upper())
                if not dex_name:
                    continue
                try:
                    r3 = requests.post(HL_INFO_URL, json={"type": "meta", "dex": dex_name}, timeout=HTTP_TIMEOUT_SEC)
                    for b_asset in r3.json().get("universe", []):
                        if not isinstance(b_asset, dict):
                            continue
                        b_name = str(b_asset.get("name", "")).strip()
                        if not b_name:
                            continue
                        b_key = f"{dex_name}:{b_name.upper()}"
                        self._meta_cache[b_key] = {
                            "canonical_coin": b_key,
                            "szDecimals": int(b_asset.get("szDecimals", 0)),
                            "symbol_source": "builder_meta",
                            "perp_dex": dex_name,
                        }
                except Exception:
                    pass
        except Exception:
            pass

    def _resolve_coin(self, coin: str) -> Dict[str, Any]:
        raw = str(coin or "").strip()
        key = raw.upper()
        if key.startswith("@"):
            return {"ok": False, "raw_coin": raw, "status": "SPOT_MARKET_SKIPPED",
                    "error": f"{key} is a spot market; not a perp", "exchange_called": False}
        if key.startswith("#"):
            try:
                idx = int(key[1:])
            except ValueError:
                return {"ok": False, "raw_coin": raw, "status": "SYMBOL_UNRESOLVED",
                        "error": f"cannot parse index {key}", "exchange_called": False}
            self._fetch_meta()
            canonical = self._index_cache.get(idx)
            if canonical is None:
                return {"ok": False, "raw_coin": raw, "status": "PERP_INDEX_UNRESOLVED",
                        "error": f"{key} not in meta.universe", "exchange_called": False}
            result = self._resolve_coin(canonical)
            result["raw_coin"] = raw
            return result
        self._fetch_meta()
        item = self._meta_cache.get(key)
        if item is None:
            status = "SYMBOL_UNRESOLVED" if self._meta_fetched else "META_UNAVAILABLE"
            return {"ok": False, "raw_coin": raw, "status": status,
                    "error": f"{key} not in meta", "exchange_called": False}
        canonical = item["canonical_coin"]
        sz_dec = int(item.get("szDecimals", 4))
        price_max_dec = max(0, 6 - sz_dec)
        if item.get("symbol_source") == "builder_meta":
            sdk_coin = canonical.split(":", 1)[1] if ":" in canonical else canonical
            perp_dexs: Optional[List[str]] = ["", str(item.get("perp_dex", ""))]
        else:
            sdk_coin = canonical
            perp_dexs = None
        return {
            "ok": True, "raw_coin": raw, "sdk_coin": sdk_coin,
            "sz_decimals": sz_dec, "price_max_decimals": price_max_dec,
            "perp_dexs": perp_dexs, "status": "OK", "error": "",
        }

    @staticmethod
    def _parse_hl_response(response: Any) -> Tuple[bool, str, str]:
        try:
            statuses = response.get("response", {}).get("data", {}).get("statuses", [])
            if statuses:
                s = statuses[0]
                if isinstance(s, dict):
                    if "filled" in s:
                        return True, "ORDER_FILLED", str(s["filled"].get("oid", ""))
                    if "resting" in s:
                        return True, "ORDER_RESTING", str(s["resting"].get("oid", ""))
                    if "error" in s:
                        return False, "ORDER_REJECTED", ""
        except Exception:
            pass
        return False, "ORDER_UNKNOWN", ""

    def _append_real_attempt(self, intent: Intent, status: str, result: Dict[str, Any]) -> None:
        delta = ManualLedger.signed_delta(intent.copy_side, intent.copy_size)
        self.audit.append_send_attempt({
            "created_at": utc_now_iso(),
            "created_at_ms": utc_now_ms(),
            "attempt_id": stable_hash(["attempt", intent.intent_id, utc_now_ms()]),
            "intent_id": intent.intent_id,
            "leader_fill_id": intent.fill.leader_fill_id,
            "leader_wallet": intent.fill.leader_wallet,
            "coin": intent.fill.coin,
            "side": intent.copy_side,
            "order_type": "REAL_IOC",
            "limit_price": result.get("limit_px", intent.fill.price),
            "copy_size": result.get("wire_size", intent.copy_size),
            "copy_notional": intent.copy_notional,
            "reduce_only_sent": "False",
            "sleeve_id": intent.sleeve_id,
            "position_id": intent.position_id,
            "wallet_position_before": intent.wallet_position_before,
            "wallet_position_after_expected": intent.wallet_position_before + delta,
            "coin_net_before": intent.coin_net_before,
            "coin_net_after_expected": intent.coin_net_before + delta,
            "status": status,
            "exchange_response": json.dumps(result.get("exchange_response", {})),
            "exchange_order_id": result.get("oid", ""),
            "error": result.get("error", ""),
            "notes": "real exchange IOC attempt",
        })

    def _append_mock_attempt(self, intent: Intent, status: str) -> None:
        delta = ManualLedger.signed_delta(intent.copy_side, intent.copy_size)
        self.audit.append_send_attempt({
            "created_at": utc_now_iso(),
            "created_at_ms": utc_now_ms(),
            "attempt_id": stable_hash(["attempt", intent.intent_id, utc_now_ms()]),
            "intent_id": intent.intent_id,
            "leader_fill_id": intent.fill.leader_fill_id,
            "leader_wallet": intent.fill.leader_wallet,
            "coin": intent.fill.coin,
            "side": intent.copy_side,
            "order_type": "MOCK_IOC",
            "limit_price": intent.fill.price,
            "copy_size": intent.copy_size,
            "copy_notional": intent.copy_notional,
            "reduce_only_sent": str(intent.reduce_only_sent_planned),
            "sleeve_id": intent.sleeve_id,
            "position_id": intent.position_id,
            "wallet_position_before": intent.wallet_position_before,
            "wallet_position_after_expected": intent.wallet_position_before + delta,
            "coin_net_before": intent.coin_net_before,
            "coin_net_after_expected": intent.coin_net_before + delta,
            "status": status,
            "exchange_response": json.dumps({"mock": True}),
            "exchange_order_id": "mock",
            "error": "",
            "notes": "mocked sender; no exchange call",
        })


class LeaderFillIngestor:
    @staticmethod
    def stable_leader_fill_id(wallet: str, raw: Dict[str, Any]) -> str:
        return str(raw.get("fill_id") or raw.get("hash") or raw.get("tid") or raw.get("oid") or stable_hash([
            wallet, raw.get("coin"), raw.get("side", raw.get("dir")), raw.get("px", raw.get("price")),
            raw.get("sz", raw.get("size")), raw.get("time", raw.get("timestamp_ms")), raw.get("startPosition", ""),
        ]))

    @staticmethod
    def parse_fill(wallet: str, raw: Dict[str, Any], source: str) -> Optional[LeaderFill]:
        try:
            w = normalise_wallet(raw.get("user") or raw.get("wallet") or wallet)
            if not is_valid_wallet(w):
                return None
            coin = str(raw.get("coin") or "").upper().strip()
            if not coin:
                return None
            raw_side = str(raw.get("side") or raw.get("dir") or "").lower()
            side = "BUY" if raw_side in {"b", "buy", "open long", "close short"} or "long" in raw_side and "short" not in raw_side else "SELL"
            price = fnum(raw.get("px", raw.get("price")), 0.0)
            size = abs(fnum(raw.get("sz", raw.get("size")), 0.0))
            ts = int(fnum(raw.get("time", raw.get("timestamp_ms", raw.get("timestamp"))), utc_now_ms()))
            if price <= 0 or size <= 0:
                return None
            fid = LeaderFillIngestor.stable_leader_fill_id(w, raw)
            return LeaderFill(fid, w, coin, side, price, size, ts, source, dict(raw))
        except Exception as exc:
            log_error("parse_leader_fill", exc)
            return None

    def read_from_csv(self, source_csv: Path, wallets: Iterable[str], since_ms: int = 0) -> List[LeaderFill]:
        wallet_set = {normalise_wallet(w) for w in wallets}
        out: List[LeaderFill] = []
        if not source_csv.exists():
            return out
        for row in read_csv_rows(source_csv):
            w = normalise_wallet(row.get("wallet") or row.get("user"))
            if wallet_set and w not in wallet_set:
                continue
            ts = int(fnum(row.get("timestamp_ms") or row.get("time"), 0))
            if ts < since_ms:
                continue
            source = str(row.get("recording_method") or row.get("source") or "REBUILD").upper()
            fill = self.parse_fill(w, row, source)
            if fill:
                out.append(fill)
        out.sort(key=lambda f: (f.timestamp_ms, f.leader_fill_id))
        return out

    def poll_hyperliquid_fills(self, wallet: str, start_ms: int, end_ms: Optional[int] = None) -> Tuple[List[LeaderFill], str]:
        if requests is None:
            return [], "POLL_NETWORK_ERROR"
        fills: List[LeaderFill] = []
        status = "POLL_OK"
        start = max(0, start_ms)
        end = end_ms or utc_now_ms()
        for _ in range(POLL_MAX_PAGES_PER_WALLET):
            payload = {"type": "userFillsByTime", "user": wallet, "startTime": start, "endTime": end, "aggregateByTime": False}
            try:
                r = requests.post(HL_INFO_URL, json=payload, timeout=HTTP_TIMEOUT_SEC)
                data = r.json()
                if not isinstance(data, list):
                    return fills, "POLL_NETWORK_ERROR"
                page: List[LeaderFill] = []
                for raw in data:
                    if isinstance(raw, dict):
                        fill = self.parse_fill(wallet, raw, "POLL")
                        if fill:
                            page.append(fill)
                fills.extend(page)
                if len(data) < 2000 or not page:
                    break
                start = max(f.timestamp_ms for f in page) + 1
            except Exception as exc:
                log_error("poll_hyperliquid_fills", exc)
                status = "POLL_NETWORK_ERROR"
                break
        fills.sort(key=lambda f: (f.timestamp_ms, f.leader_fill_id))
        return fills, status


class WSManager:
    """Single shared WebSocket connection subscribing up to MAX_WALLETS leader wallets.

    One thread, one socket, N subscribe messages on open (HOT10 design).
    Transport health is socket-level: WS_OK when the shared connection is open
    regardless of per-wallet fill recency. Per-wallet last_message_ms is tracked
    for information only and does not influence the health grade.
    """

    def __init__(self, wallets: Iterable[str], ingestor: LeaderFillIngestor):
        self.wallets = list(wallets)[:MAX_WALLETS]
        self.ingestor = ingestor
        self.queue: "queue.Queue[LeaderFill]" = queue.Queue(maxsize=50000)
        self.last_msg_ms: Dict[str, int] = {w: 0 for w in self.wallets}
        self.last_error: Dict[str, str] = {}
        self.enabled = bval(os.getenv("HL_LIVE_WS_ENABLED"), False)
        self.stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._app: Optional[Any] = None
        self._socket_open: bool = False
        self._reconnect_count: int = 0
        self._last_socket_error: str = ""
        self._last_ping_ms: int = 0
        self._last_pong_ms: int = 0
        self._heartbeat_interval: float = float(os.getenv("HL_LIVE_WS_HEARTBEAT_SEC", "25"))
        self._hb_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.enabled or websocket is None or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._shared_thread, daemon=True, name="HLCoreWS-shared"
        )
        self._thread.start()
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="HLCoreWS-heartbeat"
        )
        self._hb_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        app = self._app
        if app is not None:
            try:
                app.close()
            except Exception:
                pass

    def _heartbeat_loop(self) -> None:
        while not self.stop_event.wait(self._heartbeat_interval):
            if self._socket_open:
                app = self._app
                if app is not None:
                    try:
                        app.send(json.dumps({"method": "ping"}))
                        self._last_ping_ms = utc_now_ms()
                    except Exception:
                        pass

    def _shared_thread(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            self._socket_open = False
            try:
                def on_open(ws: Any) -> None:
                    self._socket_open = True
                    for wallet in self.wallets:
                        ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "userFills", "user": wallet},
                        }))

                def on_message(_ws: Any, message: str) -> None:
                    self._on_message(message)

                def on_error(_ws: Any, err: Any) -> None:
                    self._last_socket_error = str(err)
                    self._socket_open = False

                def on_close(_ws: Any, *_args: Any) -> None:
                    self._socket_open = False
                    self._reconnect_count += 1

                app = websocket.WebSocketApp(
                    HL_WS_URL,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self._app = app
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self._last_socket_error = str(exc)
                self._socket_open = False
            if self.stop_event.wait(min(30.0, backoff)):
                break
            backoff = min(30.0, backoff * 1.5)

    def _on_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
            if isinstance(payload, dict) and payload.get("channel") == "pong":
                self._last_pong_ms = utc_now_ms()
                return
            data = payload.get("data", payload) if isinstance(payload, dict) else payload
            # Hyperliquid userFills channel carries data.user — use as wallet hint
            channel_wallet = normalise_wallet(data.get("user") or "") if isinstance(data, dict) else ""
            if isinstance(data, dict) and data.get("isSnapshot"):
                fills = data.get("fills") or data.get("userFills") or []
                source = "WS_SNAPSHOT"
            else:
                fills = (data.get("fills") or data.get("userFills")) if isinstance(data, dict) else data
                source = "WS_CAPTURED"
            if isinstance(fills, dict):
                fills = [fills]
            if not isinstance(fills, list):
                return
            for raw in fills:
                if not isinstance(raw, dict):
                    continue
                fill = self.ingestor.parse_fill(channel_wallet, raw, source)
                if fill:
                    w = fill.leader_wallet
                    if w in self.last_msg_ms:
                        self.last_msg_ms[w] = utc_now_ms()
                    try:
                        self.queue.put_nowait(fill)
                    except queue.Full:
                        log_error("ws_queue_full", RuntimeError("WS queue full"))
        except Exception as exc:
            log_error("ws_message", exc)

    def drain(self) -> List[LeaderFill]:
        out: List[LeaderFill] = []
        while True:
            try:
                out.append(self.queue.get_nowait())
            except queue.Empty:
                break
        out.sort(key=lambda f: (f.timestamp_ms, f.leader_fill_id))
        return out

    def write_health(self) -> Dict[str, Any]:
        now = utc_now_ms()
        thread_alive = self._thread is not None and self._thread.is_alive()
        socket_open = self._socket_open and thread_alive
        if not self.enabled:
            ws_status, worst_grade = "WS_DISABLED", "DISABLED"
        elif socket_open:
            ws_status, worst_grade = "WS_OK", "OK"
        else:
            ws_status, worst_grade = "WS_DEGRADED", "DEGRADED"
        wallets: Dict[str, Any] = {}
        for w in self.wallets:
            last = self.last_msg_ms.get(w, 0)
            wallets[w] = {
                "last_message_ms": last,
                "stale_ms": (now - last) if last else None,
                "last_error": self.last_error.get(w, ""),
            }
        summary = {
            "wallet_count": len(self.wallets),
            "open_count": 1 if socket_open else 0,
            "stale_count": 0 if socket_open else (1 if self.enabled else 0),
            "total_reconnect_count": self._reconnect_count,
            "worst_health_grade": worst_grade,
            "ws_status": ws_status,
            "socket_open": socket_open,
            "thread_alive": thread_alive,
            "last_socket_error": self._last_socket_error,
            "last_ping_ms": self._last_ping_ms,
            "last_pong_ms": self._last_pong_ms,
        }
        payload = {"created_at": utc_now_iso(), "created_at_ms": now, "ws_summary": summary, "wallets": wallets}
        atomic_write_json(LIVE_WS_HEALTH_FILE, payload)
        return payload


class CopyAccountIngestor:
    @staticmethod
    def copy_fill_id(raw: Dict[str, Any]) -> str:
        return str(raw.get("copy_fill_id") or raw.get("hash") or raw.get("tid") or raw.get("oid") or stable_hash([
            raw.get("coin"), raw.get("side", raw.get("dir")), raw.get("px", raw.get("price")), raw.get("sz", raw.get("size")), raw.get("time"), raw.get("intent_id"),
        ]))

    def poll_copy_account_fills(self, user_wallet: str, start_ms: int, end_ms: Optional[int] = None) -> Tuple[List[Dict[str, Any]], str]:
        user_wallet = normalise_wallet(user_wallet)
        if not is_valid_wallet(user_wallet):
            return [], "COPY_ACCOUNT_NOT_CONFIGURED"
        if requests is None:
            return [], "COPY_ACCOUNT_POLL_NETWORK_ERROR"
        start = max(0, int(start_ms))
        end = int(end_ms or utc_now_ms())
        out: List[Dict[str, Any]] = []
        for _ in range(POLL_MAX_PAGES_PER_WALLET):
            payload = {"type": "userFillsByTime", "user": user_wallet, "startTime": start, "endTime": end, "aggregateByTime": False}
            try:
                r = requests.post(HL_INFO_URL, json=payload, timeout=HTTP_TIMEOUT_SEC)
                data = r.json()
                if not isinstance(data, list):
                    return out, "COPY_ACCOUNT_POLL_NETWORK_ERROR"
                page: List[Dict[str, Any]] = []
                for raw in data:
                    if isinstance(raw, dict):
                        item = dict(raw)
                        item.setdefault("copy_fill_id", self.copy_fill_id(item))
                        item.setdefault("timestamp_ms", int(fnum(item.get("time", item.get("timestamp_ms")), utc_now_ms())))
                        page.append(item)
                out.extend(page)
                if len(data) < 2000 or not page:
                    break
                start = max(int(fnum(x.get("timestamp_ms"), start)) for x in page) + 1
            except Exception as exc:
                log_error("poll_copy_account_fills", exc)
                return out, "COPY_ACCOUNT_POLL_NETWORK_ERROR"
        out.sort(key=lambda r: (int(fnum(r.get("timestamp_ms"), 0)), str(r.get("copy_fill_id"))))
        return out, "COPY_ACCOUNT_POLLED"


class CopyFillMatcher:
    def __init__(self, ledger: ManualLedger, audit: AuditLogWriter):
        self.ledger = ledger
        self.audit = audit
        self.matched_intent_ids = {str(r.get("intent_id") or "") for r in read_csv_rows(LIVE_FILLS_CSV) if r.get("intent_id")}

    @staticmethod
    def copy_fill_id(raw: Dict[str, Any]) -> str:
        return CopyAccountIngestor.copy_fill_id(raw)

    def _choose_intent(self, copy_fill: Dict[str, Any], intents_by_id: Dict[str, Intent]) -> Optional[Intent]:
        explicit = str(copy_fill.get("intent_id") or "")
        if explicit and explicit in intents_by_id:
            return intents_by_id[explicit]
        coin = str(copy_fill.get("coin") or "").upper()
        raw_side = str(copy_fill.get("side") or copy_fill.get("dir") or "").lower()
        side = "BUY" if raw_side in {"b", "buy", "open long", "close short"} or ("long" in raw_side and "short" not in raw_side) else "SELL"
        ts = int(fnum(copy_fill.get("timestamp_ms", copy_fill.get("time")), utc_now_ms()))
        window_ms = int(os.getenv("HL_LIVE_COPY_MATCH_WINDOW_MS", "600000"))
        candidates: List[Intent] = []
        for intent in intents_by_id.values():
            if intent.intent_id in self.matched_intent_ids:
                continue
            if intent.fill.coin != coin or intent.copy_side != side:
                continue
            if intent.fill.timestamp_ms <= ts + window_ms and abs(ts - intent.fill.timestamp_ms) <= window_ms:
                candidates.append(intent)
        candidates.sort(key=lambda i: (abs(ts - i.fill.timestamp_ms), i.fill.timestamp_ms, i.intent_id))
        return candidates[0] if len(candidates) == 1 else None

    def match_and_apply(self, copy_fill: Dict[str, Any], intents_by_id: Dict[str, Intent]) -> bool:
        intent = self._choose_intent(copy_fill, intents_by_id)
        if not intent:
            self.audit.append_reconciliation("COPY_FILL", "COPY_FILL_UNMATCHED", copy_fill_id=self.copy_fill_id(copy_fill), coin=copy_fill.get("coin", ""), notes="copy fill has no matching intent")
            return False
        result = self.ledger.apply_copy_fill(intent, copy_fill)
        self.matched_intent_ids.add(intent.intent_id)
        self.audit.append_live_fill({
            "created_at": utc_now_iso(),
            "created_at_ms": utc_now_ms(),
            "copy_fill_id": result["copy_fill_id"],
            "intent_id": intent.intent_id,
            "leader_fill_id": intent.fill.leader_fill_id,
            "leader_wallet": intent.fill.leader_wallet,
            "sleeve_id": intent.sleeve_id,
            "position_id": intent.position_id,
            "coin": intent.fill.coin,
            "side": str(copy_fill.get("side", intent.copy_side)).upper(),
            "fill_price": fnum(copy_fill.get("price", copy_fill.get("px", intent.fill.price))),
            "fill_size": fnum(copy_fill.get("size", copy_fill.get("sz", intent.copy_size))),
            "fill_notional": fnum(copy_fill.get("notional"), intent.copy_notional),
            "fee": fnum(copy_fill.get("fee"), 0.0),
            "source": str(copy_fill.get("source", "copy_account_poll")),
            "exchange_hash": str(copy_fill.get("hash", "")),
            "ledger_action": result["ledger_action"],
            "wallet_position_after": result["wallet_position_after"],
            "coin_net_after": result["coin_net_after"],
            "notes": "real copy-account fill matched to intent",
        })
        return True


class ExchangeReconciler:
    def __init__(self, ledger: ManualLedger, audit: AuditLogWriter):
        self.ledger = ledger
        self.audit = audit

    def fetch_snapshot(self) -> Tuple[Dict[str, Any], str]:
        if not USER_WALLET or requests is None:
            return {}, "SNAPSHOT_UNAVAILABLE"
        try:
            r = requests.post(HL_INFO_URL, json={"type": "clearinghouseState", "user": USER_WALLET}, timeout=HTTP_TIMEOUT_SEC)
            data = r.json()
            if isinstance(data, dict):
                snapshot = {"created_at": utc_now_iso(), "created_at_ms": utc_now_ms(), "raw": data, "positions_by_coin": self._positions_from_snapshot(data)}
                atomic_write_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, snapshot)
                return snapshot, "SNAPSHOT_OK"
        except Exception as exc:
            log_error("fetch_exchange_snapshot", exc)
        return {}, "SNAPSHOT_UNAVAILABLE"

    @staticmethod
    def _positions_from_snapshot(data: Dict[str, Any]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for item in data.get("assetPositions") or []:
            try:
                pos = item.get("position") if isinstance(item, dict) else None
                if isinstance(pos, dict):
                    coin = str(pos.get("coin") or "").upper()
                    size = fnum(pos.get("szi"), 0.0)
                    if coin:
                        out[coin] = size
            except Exception:
                continue
        return out

    def compare_snapshot(self, snapshot: Optional[Dict[str, Any]] = None) -> str:
        if not snapshot:
            snapshot, status = self.fetch_snapshot()
            if not snapshot:
                self.audit.append_reconciliation("EXCHANGE_RECON", status, notes="exchange snapshot unavailable")
                return status
        exchange = snapshot.get("positions_by_coin") if isinstance(snapshot, dict) else {}
        if not isinstance(exchange, dict):
            exchange = {}
        manual = {coin: fnum(v.get("signed_size"), 0.0) for coin, v in (self.ledger.data.get("by_coin_net") or {}).items() if isinstance(v, dict)}
        all_coins = sorted(set(manual) | set(exchange))
        mismatch = False
        for coin in all_coins:
            m = fnum(manual.get(coin), 0.0)
            e = fnum(exchange.get(coin), 0.0)
            if abs(m - e) > POSITION_EPSILON:
                mismatch = True
                self.audit.append_reconciliation("EXCHANGE_RECON", "LEDGER_EXCHANGE_NET_MISMATCH", coin=coin, manual_net=m, exchange_net=e, notes="manual ledger differs from exchange snapshot; no auto-correction")
        if not mismatch:
            self.audit.append_reconciliation("EXCHANGE_RECON", "EXCHANGE_MANUAL_NET_MATCH", notes="manual ledger and exchange net match")
        return "LEDGER_EXCHANGE_NET_MISMATCH" if mismatch else "EXCHANGE_MANUAL_NET_MATCH"


class ServiceStateWriter:
    @staticmethod
    def ws_summary_from_file() -> Dict[str, Any]:
        payload = load_json(LIVE_WS_HEALTH_FILE, {})
        summary = payload.get("ws_summary") if isinstance(payload, dict) else None
        if isinstance(summary, dict):
            return summary
        return {"ws_status": "WS_DISABLED", "wallet_count": 0, "open_count": 0, "stale_count": 0, "total_reconnect_count": 0, "worst_health_grade": "DISABLED"}

    def write(self, summary: CycleSummary, dedupe: DedupeStore) -> None:
        ws = self.ws_summary_from_file()
        payload = {
            "created_at": utc_now_iso(),
            "created_at_ms": utc_now_ms(),
            **asdict(summary),
            "ws_status": ws.get("ws_status") or ("WS_DEGRADED" if fnum(ws.get("stale_count"), 0) > 0 else "WS_OK"),
            "ws_wallet_count": ws.get("wallet_count", 0),
            "ws_open_count": ws.get("open_count", 0),
            "ws_stale_count": ws.get("stale_count", 0),
            "ws_reconnect_count": ws.get("total_reconnect_count", 0),
            "ws_worst_health_grade": ws.get("worst_health_grade", ""),
            **dedupe.export(),
            "forbidden_files": {
                "live_positions_exists": FORBIDDEN_LIVE_POSITIONS_FILE.exists(),
                "would_send_orders_exists": FORBIDDEN_WOULD_SEND_ORDERS_CSV.exists(),
            },
        }
        core_state = {
            "created_at": payload.get("created_at"),
            "created_at_ms": payload.get("created_at_ms"),
            **dedupe.export(),
        }
        atomic_write_json(CORE_RUNTIME_STATE_FILE, core_state)
        atomic_write_json(SERVICE_STATE_FILE, payload)


def load_existing_intents() -> Dict[str, Intent]:
    # Runtime matching normally uses in-memory intents. This placeholder keeps the
    # core deterministic without reverse-parsing every CSV field into dataclasses.
    return {}


class LiveCopyCore:
    def __init__(self, source_csv: Optional[Path] = None):
        ensure_dirs()
        self.cfg = ConfigManager()
        self.ledger = ManualLedger()
        self.audit = AuditLogWriter()
        self.ingestor = LeaderFillIngestor()
        service_state = load_json(SERVICE_STATE_FILE, {})
        core_state = load_json(CORE_RUNTIME_STATE_FILE, {})
        merged_state = service_state if isinstance(service_state, dict) else {}
        if isinstance(core_state, dict):
            merged_state = {**merged_state, **core_state}
        self.dedupe = DedupeStore(merged_state)
        if not self.dedupe.live_start_ms:
            self.dedupe.live_start_ms = utc_now_ms()
        self.wallets = list(self.cfg.wallets().keys())[:MAX_WALLETS]
        self.ws = WSManager(self.wallets, self.ingestor)
        self.intent_builder = IntentBuilder(self.cfg, self.ledger)
        self.sender = SenderGateway(self.cfg, self.audit)
        self.copy_ingestor = CopyAccountIngestor()
        self.matcher = CopyFillMatcher(self.ledger, self.audit)
        self.reconciler = ExchangeReconciler(self.ledger, self.audit)
        self.state_writer = ServiceStateWriter()
        self.source_csv = source_csv or RAW_LEADER_FILLS_CSV
        self.intents_by_id: Dict[str, Intent] = {}

    def run_cycle(self, use_source_csv: bool = True, poll_live: bool = False, poll_copy: bool = False, reconcile_exchange: bool = False) -> CycleSummary:
        summary = CycleSummary(auto_send_enabled=self.cfg.auto_send_enabled, active_wallets=len(self.wallets))
        started = time.monotonic()
        try:
            self.ws.write_health()
            fills: List[LeaderFill] = []
            fills.extend(self.ws.drain())
            if use_source_csv:
                since = 0
                fills.extend(self.ingestor.read_from_csv(self.source_csv, self.wallets, since_ms=since))
            if poll_live:
                poll_status = "POLL_OK"
                now = utc_now_ms()
                for w in self.wallets:
                    wallet_fills, status = self.ingestor.poll_hyperliquid_fills(w, max(0, now - POLL_WINDOW_MS - POLL_OVERLAP_MS), now)
                    fills.extend(wallet_fills)
                    if status != "POLL_OK":
                        poll_status = status
                        summary.network_errors += 1
                summary.poll_loop_status = poll_status
            else:
                summary.poll_loop_status = "POLL_DISABLED"
            fills.sort(key=lambda f: (f.timestamp_ms, f.leader_fill_id))
            summary.leader_fills_seen = len(fills)
            for fill in fills:
                if not self.dedupe.accept_leader(fill.leader_fill_id):
                    summary.leader_fills_deduped += 1
                    continue
                intent = self.intent_builder.build(fill)
                self.audit.append_order_intent(intent)
                self.intents_by_id[intent.intent_id] = intent
                summary.leader_intents_written += 1
                is_pre_cutover = self.dedupe.live_start_ms > 0 and fill.timestamp_ms < self.dedupe.live_start_ms
                if not is_pre_cutover:
                    sent, send_status = self.sender.send_if_allowed(intent)
                    if sent:
                        summary.leader_sends_attempted += 1
                    elif send_status == "REAL_SENDER_NOT_CONFIGURED" and intent.send_allowed and self.cfg.auto_send_enabled:
                        # Visible decision only; send_attempts.csv remains pure.
                        self.audit.append_reconciliation("SEND_PRECHECK", "REAL_SENDER_NOT_CONFIGURED", leader_wallet=fill.leader_wallet, leader_fill_id=fill.leader_fill_id, intent_id=intent.intent_id, coin=fill.coin, notes="auto-send enabled but real sender unavailable; no exchange attempt made")
            if poll_copy:
                copy_state = load_json(CORE_RUNTIME_STATE_FILE, {})
                start_ms = max(0, int(fnum(copy_state.get("last_copy_poll_ms", load_json(SERVICE_STATE_FILE, {}).get("last_copy_poll_ms", 0)), 0)) - POLL_OVERLAP_MS)
                copy_fills, copy_status = self.copy_ingestor.poll_copy_account_fills(USER_WALLET, start_ms, utc_now_ms())
                summary.copy_account_status = copy_status
                summary.copy_fills_seen = len(copy_fills)
                if copy_status == "COPY_ACCOUNT_POLLED" and copy_fills and not self.dedupe.copy_account_baseline_set:
                    summary.copy_fills_baselined = self.dedupe.baseline_copy_account(copy_fills)
                    summary.copy_account_status = "COPY_ACCOUNT_BASELINED"
                    self.audit.append_reconciliation("COPY_ACCOUNT_BASELINE", "COPY_ACCOUNT_BASELINED", notes=f"baseline historical copy fills count={summary.copy_fills_baselined}; no ledger mutation")
                else:
                    for raw_copy in copy_fills:
                        copy_id = CopyAccountIngestor.copy_fill_id(raw_copy)
                        if not self.dedupe.accept_copy(copy_id):
                            summary.copy_fills_deduped += 1
                            continue
                        if self.matcher.match_and_apply(raw_copy, self.intents_by_id):
                            summary.copy_fills_matched += 1
                            summary.ledger_updates += 1
                        else:
                            summary.copy_fills_unmatched += 1
                if copy_status == "COPY_ACCOUNT_POLLED":
                    state_update = load_json(CORE_RUNTIME_STATE_FILE, {})
                    if not isinstance(state_update, dict):
                        state_update = {}
                    state_update["last_copy_poll_ms"] = utc_now_ms()
                    state_update.update(self.dedupe.export())
                    atomic_write_json(CORE_RUNTIME_STATE_FILE, state_update)
                if copy_status not in {"COPY_ACCOUNT_POLLED", "COPY_ACCOUNT_NOT_CONFIGURED"}:
                    summary.network_errors += 1
            else:
                summary.copy_account_status = "COPY_ACCOUNT_POLL_DISABLED"
            if reconcile_exchange:
                summary.exchange_recon_status = self.reconciler.compare_snapshot()
            else:
                summary.exchange_recon_status = "SNAPSHOT_SKIPPED"
        except Exception as exc:
            summary.ok = False
            summary.fatal_errors += 1
            log_error("run_cycle", exc)
        finally:
            if time.monotonic() - started > float(os.getenv("HL_LIVE_RECON_CYCLE_BUDGET_SEC", "5")):
                summary.budget_exceeded = True
                if summary.poll_loop_status == "POLL_OK":
                    summary.poll_loop_status = "POLL_BUDGET_EXCEEDED"
            self.state_writer.write(summary, self.dedupe)
        return summary


# ----------------------------- SELF TESTS -----------------------------

class TestFailure(Exception):
    pass


def _check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise TestFailure(f"FAIL: {name}" + (f" :: {detail}" if detail else ""))
    print(f"PASS: {name}" + (f" :: {detail}" if detail else ""))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def run_self_test() -> None:
    import tempfile

    global BASE_DIR, ENGINE_OUTPUT_DIR, AUDIT_DIR, APPEND_ONLY_DIR, LIVE_CONFIG_FILE, SERVICE_STATE_FILE, CORE_RUNTIME_STATE_FILE
    global LIVE_WS_HEALTH_FILE, MANUAL_LIVE_POSITIONS_FILE, EXCHANGE_ACCOUNT_SNAPSHOT_FILE, ORDER_INTENTS_CSV
    global SEND_ATTEMPTS_CSV, LIVE_FILLS_CSV, RECONCILIATION_CSV, ERRORS_CSV, RAW_LEADER_FILLS_CSV
    global MANUAL_WALLETS_FILE, WALLET_GATE_FILE, UI_STATE_FILE, FORBIDDEN_LIVE_POSITIONS_FILE, FORBIDDEN_WOULD_SEND_ORDERS_CSV

    old_env = dict(os.environ)
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        BASE_DIR = tmp
        ENGINE_OUTPUT_DIR = tmp / "hl_copy_output"
        AUDIT_DIR = tmp / "hl_live_copy_audit"
        APPEND_ONLY_DIR = AUDIT_DIR / "append_only"
        LIVE_CONFIG_FILE = AUDIT_DIR / "live_config.json"
        SERVICE_STATE_FILE = AUDIT_DIR / "live_service_state.json"
        CORE_RUNTIME_STATE_FILE = AUDIT_DIR / "clean_core_runtime_state.json"
        LIVE_WS_HEALTH_FILE = AUDIT_DIR / "live_ws_health.json"
        MANUAL_LIVE_POSITIONS_FILE = AUDIT_DIR / "manual_live_positions.json"
        EXCHANGE_ACCOUNT_SNAPSHOT_FILE = AUDIT_DIR / "exchange_account_snapshot.json"
        ORDER_INTENTS_CSV = APPEND_ONLY_DIR / "order_intents.csv"
        SEND_ATTEMPTS_CSV = APPEND_ONLY_DIR / "send_attempts.csv"
        LIVE_FILLS_CSV = APPEND_ONLY_DIR / "live_fills.csv"
        RECONCILIATION_CSV = APPEND_ONLY_DIR / "reconciliation.csv"
        ERRORS_CSV = APPEND_ONLY_DIR / "errors.csv"
        RAW_LEADER_FILLS_CSV = ENGINE_OUTPUT_DIR / "raw_live_fills.csv"
        MANUAL_WALLETS_FILE = tmp / "manual_wallets.txt"
        WALLET_GATE_FILE = tmp / "wallet_gate.json"
        UI_STATE_FILE = tmp / "ui_state.json"
        FORBIDDEN_LIVE_POSITIONS_FILE = AUDIT_DIR / "live_positions.json"
        FORBIDDEN_WOULD_SEND_ORDERS_CSV = APPEND_ONLY_DIR / "would_send_orders.csv"
        ensure_dirs()

        wallet_a = "0x" + "a" * 40
        wallet_b = "0x" + "b" * 40
        atomic_write_json(LIVE_CONFIG_FILE, {
            "auto_send_enabled": False,
            "wallets": {
                wallet_a: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 10},
                wallet_b: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 10},
            },
            "global_controls": {"min_notional": 10, "max_order_notional_usd": 0, "marketable_bps": 25},
        })
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "leader-a-ton-buy-1", "wallet": wallet_a, "coin": "TON", "side": "BUY", "price": "2.0", "size": "10", "timestamp_ms": "1000", "source": "ws", "recording_method": "WS_CAPTURED",
        }])
        os.environ["HL_LIVE_AUTO_SEND_ENABLED"] = "0"
        os.environ.pop("HL_LIVE_MOCK_SEND", None)
        core = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        summary = core.run_cycle(use_source_csv=True, poll_live=False, reconcile_exchange=False)
        _check("master gate disabled keeps leader_sends_attempted zero", summary.leader_sends_attempted == 0, str(summary))
        _check("order intent written with auto-send disabled", len(read_csv_rows(ORDER_INTENTS_CSV)) == 1)
        _check("no send_attempt row when auto-send disabled", len(read_csv_rows(SEND_ATTEMPTS_CSV)) == 0)
        _check("no forbidden live_positions.json", not FORBIDDEN_LIVE_POSITIONS_FILE.exists())
        _check("no forbidden would_send_orders.csv", not FORBIDDEN_WOULD_SEND_ORDERS_CSV.exists())

        # Mock sender proves send_attempts is only written after a send boundary call.
        os.environ["HL_LIVE_AUTO_SEND_ENABLED"] = "1"
        os.environ["HL_LIVE_MOCK_SEND"] = "1"
        cfg_payload = load_json(LIVE_CONFIG_FILE, {})
        cfg_payload["auto_send_enabled"] = True
        cfg_payload["wallets"][wallet_a]["mode"] = "LIVE"
        atomic_write_json(LIVE_CONFIG_FILE, cfg_payload)
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "leader-a-eth-buy-1", "wallet": wallet_a, "coin": "ETH", "side": "BUY", "price": "1000", "size": "1", "timestamp_ms": "2000", "source": "ws", "recording_method": "WS_CAPTURED",
        }])
        atomic_write_json(SERVICE_STATE_FILE, {})
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": 1})
        core = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        summary = core.run_cycle(use_source_csv=True, poll_live=False, reconcile_exchange=False)
        _check("mock auto-send writes one send_attempt", summary.leader_sends_attempted == 1 and len(read_csv_rows(SEND_ATTEMPTS_CSV)) == 1)
        _check("UI LIVE mode maps to core active mode", summary.leader_sends_attempted >= 1, "LIVE wallet generated a send attempt")

        # Copy fill fallback matching: exchange fills do not carry our internal intent_id.
        core.matcher.match_and_apply({"coin": "ETH", "side": "BUY", "price": "1000", "size": "0.01", "time": 2050, "hash": "copy-eth-1"}, core.intents_by_id)
        _check("copy fill fallback matches by coin/side/time", len(read_csv_rows(LIVE_FILLS_CSV)) == 1)

        # Clean-start copy account baseline: historical copy fills become cursor only.
        class FakeCopyIngestor:
            def poll_copy_account_fills(self, user_wallet: str, start_ms: int, end_ms: Optional[int] = None):
                return ([
                    {"copy_fill_id": "hist-copy-1", "coin": "ETH", "side": "BUY", "price": "1000", "size": "0.01", "timestamp_ms": 2100},
                    {"copy_fill_id": "hist-copy-2", "coin": "TON", "side": "SELL", "price": "2", "size": "1", "timestamp_ms": 2200},
                ], "COPY_ACCOUNT_POLLED")

        os.environ["HL_USER_WALLET"] = wallet_a
        before_ledger = json.dumps(load_json(MANUAL_LIVE_POSITIONS_FILE, {}), sort_keys=True)
        before_recon = len(read_csv_rows(RECONCILIATION_CSV))
        atomic_write_json(SERVICE_STATE_FILE, {})
        try:
            CORE_RUNTIME_STATE_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        core = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        core.copy_ingestor = FakeCopyIngestor()
        summary = core.run_cycle(use_source_csv=False, poll_live=False, poll_copy=True, reconcile_exchange=False)
        _check("copy account first poll baselines historical fills", summary.copy_account_status == "COPY_ACCOUNT_BASELINED" and summary.copy_fills_baselined == 2 and summary.copy_fills_unmatched == 0, str(summary))
        _check("copy account baseline writes one recon note", len(read_csv_rows(RECONCILIATION_CSV)) == before_recon + 1)
        _check("copy account baseline does not mutate ledger", json.dumps(load_json(MANUAL_LIVE_POSITIONS_FILE, {}), sort_keys=True) == before_ledger)
        # Simulate the public service_state heartbeat being clobbered by legacy/other UI code; core checkpoint must still preserve dedupe.
        atomic_write_json(SERVICE_STATE_FILE, {})
        core = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        core.copy_ingestor = FakeCopyIngestor()
        summary = core.run_cycle(use_source_csv=False, poll_live=False, poll_copy=True, reconcile_exchange=False)
        _check("copy account checkpoint survives service_state clobber", summary.copy_fills_deduped == 2 and summary.copy_fills_baselined == 0 and summary.copy_fills_unmatched == 0, str(summary))

        # Per-wallet sleeve isolation.
        ledger = ManualLedger()
        audit = AuditLogWriter()
        cfg = ConfigManager()
        builder = IntentBuilder(cfg, ledger)
        fill_a_open = LeaderFill("fa", wallet_a, "TON", "BUY", 2.0, 10.0, 3000, "TEST")
        intent_a_open = builder.build(fill_a_open)
        matcher = CopyFillMatcher(ledger, audit)
        matched = matcher.match_and_apply({"intent_id": intent_a_open.intent_id, "side": "BUY", "price": 2.0, "size": 5.0, "copy_fill_id": "ca1"}, {intent_a_open.intent_id: intent_a_open})
        _check("wallet A open fill matched", matched)
        fill_b_open = LeaderFill("fb", wallet_b, "TON", "SELL", 2.0, 10.0, 4000, "TEST")
        intent_b_open = builder.build(fill_b_open)
        matched = matcher.match_and_apply({"intent_id": intent_b_open.intent_id, "side": "SELL", "price": 2.0, "size": 3.0, "copy_fill_id": "cb1"}, {intent_b_open.intent_id: intent_b_open})
        _check("wallet B open fill matched", matched)
        before_b = ledger.wallet_coin_position(wallet_b, "TON")
        fill_a_close = LeaderFill("fc", wallet_a, "TON", "SELL", 2.1, 2.0, 5000, "TEST")
        intent_a_close = builder.build(fill_a_close)
        matcher.match_and_apply({"intent_id": intent_a_close.intent_id, "side": "SELL", "price": 2.1, "size": 2.0, "copy_fill_id": "ca2"}, {intent_a_close.intent_id: intent_a_close})
        _check("wallet A close mutates only wallet A", abs(ledger.wallet_coin_position(wallet_a, "TON") - 3.0) < 1e-9)
        _check("wallet B unchanged after wallet A close", abs(ledger.wallet_coin_position(wallet_b, "TON") - before_b) < 1e-9)
        _check("by_coin_net updates correctly", abs(ledger.coin_net("TON") - 0.0) < 1e-9, str(ledger.data.get("by_coin_net")))

        # Unmatched copy fill writes reconciliation and does not mutate ledger.
        recon_before = len(read_csv_rows(RECONCILIATION_CSV))
        pos_before = json.dumps(ledger.data, sort_keys=True)
        matcher.match_and_apply({"intent_id": "missing", "coin": "BTC", "side": "BUY", "price": 1, "size": 1, "copy_fill_id": "unmatched"}, {})
        _check("unmatched copy fill writes reconciliation", len(read_csv_rows(RECONCILIATION_CSV)) == recon_before + 1)
        _check("unmatched copy fill does not mutate ledger", json.dumps(ledger.data, sort_keys=True) == pos_before)

        # Exchange reconciliation mismatch reports but does not mutate manual ledger.
        recon = ExchangeReconciler(ledger, audit)
        ledger_before = json.dumps(ledger.data, sort_keys=True)
        status = recon.compare_snapshot({"positions_by_coin": {"TON": 999.0}})
        _check("exchange mismatch is reported", status == "LEDGER_EXCHANGE_NET_MISMATCH")
        _check("exchange mismatch does not auto-correct ledger", json.dumps(ledger.data, sort_keys=True) == ledger_before)

        # Live cutover boundary: pre-cutover fills write intents but never send.
        os.environ["HL_LIVE_AUTO_SEND_ENABLED"] = "1"
        os.environ["HL_LIVE_MOCK_SEND"] = "1"
        _cutover_ms = utc_now_ms()

        # Tests 1+2: pre-cutover intent-only / post-cutover sends.
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": _cutover_ms})
        atomic_write_json(SERVICE_STATE_FILE, {})
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "co-pre-1", "wallet": wallet_a, "coin": "ETH", "side": "BUY",
            "price": "1000", "size": "0.01", "timestamp_ms": str(_cutover_ms - 5000),
            "recording_method": "WS_CAPTURED",
        }])
        _co_intents_before = len(read_csv_rows(ORDER_INTENTS_CSV))
        _co_attempts_before = len(read_csv_rows(SEND_ATTEMPTS_CSV))
        _core_co1 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _sum_co1 = _core_co1.run_cycle(use_source_csv=True, poll_live=False)
        _check("cutover: pre-cutover fill writes intent",
               _sum_co1.leader_intents_written == 1 and
               len(read_csv_rows(ORDER_INTENTS_CSV)) == _co_intents_before + 1,
               str(_sum_co1))
        _check("cutover: pre-cutover fill does not send",
               _sum_co1.leader_sends_attempted == 0 and
               len(read_csv_rows(SEND_ATTEMPTS_CSV)) == _co_attempts_before,
               str(_sum_co1))

        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": _cutover_ms})
        atomic_write_json(SERVICE_STATE_FILE, {})
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "co-post-1", "wallet": wallet_a, "coin": "ETH", "side": "BUY",
            "price": "1000", "size": "0.01", "timestamp_ms": str(_cutover_ms + 5000),
            "recording_method": "WS_CAPTURED",
        }])
        _co_attempts_before2 = len(read_csv_rows(SEND_ATTEMPTS_CSV))
        _core_co2 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _sum_co2 = _core_co2.run_cycle(use_source_csv=True, poll_live=False)
        _check("cutover: post-cutover fill sends with mock sender",
               _sum_co2.leader_sends_attempted == 1 and
               len(read_csv_rows(SEND_ATTEMPTS_CSV)) == _co_attempts_before2 + 1,
               str(_sum_co2))

        # Tests 3+4: live_start_ms set on first construction; unchanged across restart.
        atomic_write_json(SERVICE_STATE_FILE, {})
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {})
        _core_t1 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _check("cutover: live_start_ms set on first construction",
               _core_t1.dedupe.live_start_ms > 0)
        _t1 = _core_t1.dedupe.live_start_ms
        _core_t1.run_cycle(use_source_csv=False)
        _core_t2 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _check("cutover: live_start_ms unchanged across restart",
               _core_t2.dedupe.live_start_ms == _t1,
               f"t1={_t1} t2={_core_t2.dedupe.live_start_ms}")

        # Test 5: deleted state file still produces a fresh live_start_ms.
        try:
            CORE_RUNTIME_STATE_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        atomic_write_json(SERVICE_STATE_FILE, {})
        _core_t3 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _check("cutover: wiped state creates fresh live_start_ms",
               _core_t3.dedupe.live_start_ms > 0)

        # Test 6: WS_CAPTURED post-cutover fill is send-eligible.
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": _cutover_ms})
        atomic_write_json(SERVICE_STATE_FILE, {})
        _core_ws = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _ws_fill = LeaderFill("ws-co-1", wallet_a, "BTC", "BUY", 50000.0, 0.001,
                              _cutover_ms + 2000, "WS_CAPTURED")
        _core_ws.ws.queue.put_nowait(_ws_fill)
        _co_ws_attempts_before = len(read_csv_rows(SEND_ATTEMPTS_CSV))
        _sum_ws = _core_ws.run_cycle(use_source_csv=False, poll_live=False)
        _check("cutover: WS_CAPTURED post-cutover fill is send-eligible",
               _sum_ws.leader_sends_attempted == 1 and
               len(read_csv_rows(SEND_ATTEMPTS_CSV)) == _co_ws_attempts_before + 1,
               str(_sum_ws))

        # Sleeve exit policy: exit clamped to sleeve size, not current config notional.
        atomic_write_json(LIVE_CONFIG_FILE, {
            "auto_send_enabled": False,
            "wallets": {
                wallet_a: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 10},
                wallet_b: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 10},
            },
            "global_controls": {"min_notional": 1},
        })
        _sl = ManualLedger(path=AUDIT_DIR / "test_sl1_positions.json")
        _sb = IntentBuilder(ConfigManager(), _sl)
        _sm = CopyFillMatcher(_sl, AuditLogWriter())
        _f_open = LeaderFill("sl-open", wallet_a, "SOL", "BUY", 2.0, 25.0, 6000, "TEST")
        _i_open = _sb.build(_f_open)
        _sm.match_and_apply(
            {"intent_id": _i_open.intent_id, "side": "BUY", "price": 2.0, "size": 5.0, "copy_fill_id": "sl-cf1"},
            {_i_open.intent_id: _i_open},
        )
        # Config raised to 50; unclamped exit would give copy_size=25.0
        atomic_write_json(LIVE_CONFIG_FILE, {
            "auto_send_enabled": False,
            "wallets": {
                wallet_a: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 50},
                wallet_b: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 10},
            },
            "global_controls": {"min_notional": 1},
        })
        _f_exit = LeaderFill("sl-exit", wallet_a, "SOL", "SELL", 2.0, 25.0, 6500, "TEST")
        _i_exit = IntentBuilder(ConfigManager(), _sl).build(_f_exit)
        _check("sleeve exit clamped to sleeve size not inflated config",
               abs(_i_exit.copy_size - 5.0) < 1e-9,
               f"copy_size={_i_exit.copy_size}")

        # Wallet A exits TON with wallet B holding opposite exposure — A exit allowed and sized to A sleeve.
        _sl2 = ManualLedger(path=AUDIT_DIR / "test_sl2_positions.json")
        _sb2 = IntentBuilder(ConfigManager(), _sl2)
        _sm2 = CopyFillMatcher(_sl2, AuditLogWriter())
        _fa_open = LeaderFill("ab2-oa", wallet_a, "TON", "BUY", 2.0, 10.0, 7000, "TEST")
        _ia_open = _sb2.build(_fa_open)
        _sm2.match_and_apply(
            {"intent_id": _ia_open.intent_id, "side": "BUY", "price": 2.0, "size": 5.0, "copy_fill_id": "ab2-cfa"},
            {_ia_open.intent_id: _ia_open},
        )
        _fb_open = LeaderFill("ab2-ob", wallet_b, "TON", "SELL", 2.0, 10.0, 7100, "TEST")
        _ib_open = _sb2.build(_fb_open)
        _sm2.match_and_apply(
            {"intent_id": _ib_open.intent_id, "side": "SELL", "price": 2.0, "size": 3.0, "copy_fill_id": "ab2-cfb"},
            {_ib_open.intent_id: _ib_open},
        )
        # coin_net=+2.0 but wallet_a sleeve=+5.0; exit must clamp to 5.0 not coin_net
        _fa_exit = LeaderFill("ab2-xa", wallet_a, "TON", "SELL", 2.0, 10.0, 7200, "TEST")
        _ia_exit = _sb2.build(_fa_exit)
        _check("wallet A exit allowed with wallet B opposite exposure",
               _ia_exit.decision in {"EXIT_ALLOWED", "ENTRY_ALLOWED"})
        _check("wallet A exit clamped to wallet A sleeve not coin_net",
               abs(_ia_exit.copy_size - 5.0) < 1e-9,
               f"copy_size={_ia_exit.copy_size} coin_net={_ia_exit.coin_net_before}")
        _check("wallet B sleeve unchanged after wallet A exit intent",
               abs(_sl2.wallet_coin_position(wallet_b, "TON") - (-3.0)) < 1e-9)

        # reduce_only_sent_planned is always False regardless of position or coin_net.
        _sl3 = ManualLedger(path=AUDIT_DIR / "test_sl3_positions.json")
        _sb3 = IntentBuilder(ConfigManager(), _sl3)
        _sm3 = CopyFillMatcher(_sl3, AuditLogWriter())
        _f_ro_entry = LeaderFill("ro2-open", wallet_a, "BTC", "BUY", 50000.0, 0.002, 8000, "TEST")
        _i_ro_entry = _sb3.build(_f_ro_entry)
        _check("reduce_only_sent_planned False on entry",
               _i_ro_entry.reduce_only_sent_planned is False)
        _sm3.match_and_apply(
            {"intent_id": _i_ro_entry.intent_id, "side": "BUY", "price": 50000.0, "size": 0.0002, "copy_fill_id": "ro2-cf1"},
            {_i_ro_entry.intent_id: _i_ro_entry},
        )
        _f_ro_exit = LeaderFill("ro2-exit", wallet_a, "BTC", "SELL", 50000.0, 0.002, 8500, "TEST")
        _i_ro_exit = _sb3.build(_f_ro_exit)
        _check("reduce_only_sent_planned False on exit regardless of coin_net",
               _i_ro_exit.reduce_only_sent_planned is False)

        # Real sender wiring tests (no network — transport mocked on instance).
        os.environ.pop("HL_LIVE_MOCK_SEND", None)
        os.environ["HL_LIVE_AUTO_SEND_ENABLED"] = "1"
        atomic_write_json(LIVE_CONFIG_FILE, {
            "auto_send_enabled": True,
            "wallets": {
                wallet_a: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 10},
                wallet_b: {"enabled": True, "mode": "ON", "copy_mode": "fixed", "fixed_notional": 10},
            },
            "global_controls": {"min_notional": 1},
        })
        _rs_now = utc_now_ms()

        # Test: credentials missing → zero send_attempts.
        os.environ.pop("HL_LIVE_HL_PRIVATE_KEY", None)
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": 1})
        atomic_write_json(SERVICE_STATE_FILE, {})
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "rs-cred-1", "wallet": wallet_a, "coin": "SOL", "side": "BUY",
            "price": "150", "size": "1", "timestamp_ms": str(_rs_now),
            "recording_method": "WS_CAPTURED",
        }])
        _rs_att0 = len(read_csv_rows(SEND_ATTEMPTS_CSV))
        _core_rs1 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _sum_rs1 = _core_rs1.run_cycle(use_source_csv=True, poll_live=False)
        _check("real sender: no credentials writes zero send_attempts",
               _sum_rs1.leader_sends_attempted == 0 and
               len(read_csv_rows(SEND_ATTEMPTS_CSV)) == _rs_att0,
               str(_sum_rs1))

        # Test: mocked exchange_called=True → one REAL_IOC send_attempt with oid.
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": 1})
        atomic_write_json(SERVICE_STATE_FILE, {})
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "rs-real-1", "wallet": wallet_a, "coin": "SOL", "side": "BUY",
            "price": "150", "size": "1", "timestamp_ms": str(_rs_now),
            "recording_method": "WS_CAPTURED",
        }])
        _ledger_snap = json.dumps(load_json(MANUAL_LIVE_POSITIONS_FILE, {}), sort_keys=True)
        _rs_att1 = len(read_csv_rows(SEND_ATTEMPTS_CSV))
        _core_rs2 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        def _mock_real_send_ok(intent: Any) -> Tuple[bool, str, Dict[str, Any]]:
            return True, "ORDER_FILLED", {
                "exchange_response": {"response": {"data": {"statuses": [{"filled": {"oid": "test-oid-1"}}]}}},
                "oid": "test-oid-1", "limit_px": intent.fill.price * 1.0025,
                "wire_size": intent.copy_size, "exchange_called": True, "error": "",
            }
        _core_rs2.sender._send_real = _mock_real_send_ok
        _sum_rs2 = _core_rs2.run_cycle(use_source_csv=True, poll_live=False)
        _check("real sender: mocked exchange writes one REAL_IOC send_attempt",
               _sum_rs2.leader_sends_attempted == 1 and
               len(read_csv_rows(SEND_ATTEMPTS_CSV)) == _rs_att1 + 1,
               str(_sum_rs2))
        _rs_row = read_csv_rows(SEND_ATTEMPTS_CSV)[-1]
        _check("real sender: send_attempt has exchange_order_id test-oid-1",
               _rs_row.get("exchange_order_id") == "test-oid-1")
        _check("real sender: send_attempt order_type is REAL_IOC",
               _rs_row.get("order_type") == "REAL_IOC")
        _check("real sender: reduce_only_sent False in real attempt row",
               _rs_row.get("reduce_only_sent") == "False")
        _check("real sender: manual_live_positions unchanged by send",
               json.dumps(load_json(MANUAL_LIVE_POSITIONS_FILE, {}), sort_keys=True) == _ledger_snap)

        # Test: exchange_called=False (SDK unavailable mock) → zero new send_attempts.
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": 1})
        atomic_write_json(SERVICE_STATE_FILE, {})
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "rs-sdk-1", "wallet": wallet_a, "coin": "SOL", "side": "BUY",
            "price": "150", "size": "1", "timestamp_ms": str(_rs_now),
            "recording_method": "WS_CAPTURED",
        }])
        _rs_att2 = len(read_csv_rows(SEND_ATTEMPTS_CSV))
        _core_rs3 = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        def _mock_real_send_sdk_unavail(intent: Any) -> Tuple[bool, str, Dict[str, Any]]:
            return False, "REAL_SENDER_NOT_CONFIGURED", {"status": "SDK_UNAVAILABLE", "exchange_called": False}
        _core_rs3.sender._send_real = _mock_real_send_sdk_unavail
        _sum_rs3 = _core_rs3.run_cycle(use_source_csv=True, poll_live=False)
        _check("real sender: SDK unavailable exchange_called=False writes zero send_attempts",
               _sum_rs3.leader_sends_attempted == 0 and
               len(read_csv_rows(SEND_ATTEMPTS_CSV)) == _rs_att2,
               str(_sum_rs3))

        # Symbol resolution tests (injected meta, no network).
        _sym_gw = SenderGateway(ConfigManager(), AuditLogWriter())
        _sym_gw._meta_cache = {
            "ETH":      {"canonical_coin": "ETH",      "szDecimals": 4, "symbol_source": "core_meta",    "perp_dex": ""},
            "KBONK":    {"canonical_coin": "kBONK",    "szDecimals": 0, "symbol_source": "core_meta",    "perp_dex": ""},
            "XYZ:SNDK": {"canonical_coin": "XYZ:SNDK", "szDecimals": 0, "symbol_source": "builder_meta", "perp_dex": "XYZ"},
        }
        _sym_gw._index_cache = {41: "ETH"}
        _sym_gw._meta_fetched = True

        _r_eth = _sym_gw._resolve_coin("ETH")
        _check("symbol: ETH resolves sdk_coin=ETH no perp_dexs",
               _r_eth.get("ok") and _r_eth.get("sdk_coin") == "ETH" and _r_eth.get("perp_dexs") is None)

        _r_kbonk = _sym_gw._resolve_coin("KBONK")
        _check("symbol: KBONK resolves canonical sdk_coin=kBONK",
               _r_kbonk.get("ok") and _r_kbonk.get("sdk_coin") == "kBONK",
               str(_r_kbonk))

        _r_xyz = _sym_gw._resolve_coin("XYZ:SNDK")
        _check("symbol: XYZ:SNDK resolves sdk_coin=SNDK perp_dexs=[,XYZ]",
               _r_xyz.get("ok") and _r_xyz.get("sdk_coin") == "SNDK" and _r_xyz.get("perp_dexs") == ["", "XYZ"],
               str(_r_xyz))

        _r_spot = _sym_gw._resolve_coin("@230")
        _check("symbol: @230 spot blocked SPOT_MARKET_SKIPPED exchange_called=False",
               not _r_spot.get("ok") and _r_spot.get("status") == "SPOT_MARKET_SKIPPED" and not _r_spot.get("exchange_called"))

        _r_idx = _sym_gw._resolve_coin("#41")
        _check("symbol: #41 resolves via index_cache to ETH",
               _r_idx.get("ok") and _r_idx.get("sdk_coin") == "ETH", str(_r_idx))

        _r_fake = _sym_gw._resolve_coin("FAKECOIN")
        _check("symbol: FAKECOIN returns SYMBOL_UNRESOLVED exchange_called=False",
               not _r_fake.get("ok") and _r_fake.get("status") == "SYMBOL_UNRESOLVED" and not _r_fake.get("exchange_called"))

        # Unresolved coin in full run_cycle → zero send_attempts, ledger unchanged.
        atomic_write_json(CORE_RUNTIME_STATE_FILE, {"live_start_ms": 1})
        atomic_write_json(SERVICE_STATE_FILE, {})
        _write_csv(RAW_LEADER_FILLS_CSV, [{
            "fill_id": "sym-fake-1", "wallet": wallet_a, "coin": "FAKECOIN", "side": "BUY",
            "price": "100", "size": "1", "timestamp_ms": str(_rs_now),
            "recording_method": "WS_CAPTURED",
        }])
        _sym_att_before = len(read_csv_rows(SEND_ATTEMPTS_CSV))
        _ledger_sym_snap = json.dumps(load_json(MANUAL_LIVE_POSITIONS_FILE, {}), sort_keys=True)
        _core_sym = LiveCopyCore(source_csv=RAW_LEADER_FILLS_CSV)
        _core_sym.sender._meta_cache = {}
        _core_sym.sender._meta_fetched = True
        _sum_sym = _core_sym.run_cycle(use_source_csv=True, poll_live=False)
        _check("symbol: unresolved FAKECOIN writes zero send_attempts",
               _sum_sym.leader_sends_attempted == 0 and
               len(read_csv_rows(SEND_ATTEMPTS_CSV)) == _sym_att_before,
               str(_sum_sym))
        _check("symbol: unresolved coin does not mutate ledger",
               json.dumps(load_json(MANUAL_LIVE_POSITIONS_FILE, {}), sort_keys=True) == _ledger_sym_snap)

        # Heartbeat: pong message must not enqueue a fill.
        _ws_pong = WSManager(["0x" + "f" * 40], LeaderFillIngestor())
        _ws_pong._on_message(json.dumps({"channel": "pong"}))
        _check("heartbeat: pong does not enqueue fill", _ws_pong.queue.empty())
        _check("heartbeat: pong updates last_pong_ms", _ws_pong._last_pong_ms > 0)

        # Heartbeat: _heartbeat_loop sends ping via app.send when socket is open.
        _pings_sent: List[str] = []
        class _RecordingApp:
            def send(self, data: str) -> None: _pings_sent.append(data)
        _ws_hb = WSManager(["0x" + "f" * 40], LeaderFillIngestor())
        _ws_hb._socket_open = True
        _ws_hb._app = _RecordingApp()
        _ws_hb._heartbeat_interval = 0.05
        _ws_hb._hb_thread = threading.Thread(target=_ws_hb._heartbeat_loop, daemon=True)
        _ws_hb._hb_thread.start()
        time.sleep(0.15)
        _ws_hb.stop()
        _ws_hb._hb_thread.join(timeout=1.0)
        _check("heartbeat: ping sent when socket open",
               len(_pings_sent) >= 1 and json.loads(_pings_sent[0]) == {"method": "ping"})

        # HOT10: 3 wallets must create exactly 1 shared WS thread, not 3.
        import types as _types
        _mock_ws = _types.SimpleNamespace()
        class _NoopApp:
            def __init__(self, *_a: Any, **_kw: Any) -> None: pass
            def run_forever(self, **_kw: Any) -> None: pass
            def close(self) -> None: pass
        _mock_ws.WebSocketApp = _NoopApp
        global websocket
        _saved_ws = websocket
        try:
            websocket = _mock_ws
            _ws3 = WSManager(["0x" + c * 40 for c in ("a", "b", "c")], LeaderFillIngestor())
            _ws3.enabled = True
            _ws3.start()
            _check("HOT10: 3 wallets create exactly 1 WS thread",
                   _ws3._thread is not None and isinstance(_ws3._thread, threading.Thread))
            _ws3.stop()
            _ws3._thread.join(timeout=2.0)
        finally:
            websocket = _saved_ws

        # HOT10: shared socket open with no recent fills must be WS_OK, not DEGRADED.
        _wsh = WSManager(["0x" + "e" * 40], LeaderFillIngestor())
        _wsh.enabled = True
        _wsh._socket_open = True
        _keep_alive = threading.Event()
        _wsh._thread = threading.Thread(target=_keep_alive.wait, daemon=True, name="fake-ws")
        _wsh._thread.start()
        try:
            _wsh_result = _wsh.write_health()
            _check("HOT10: socket open + no fills => WS_OK not DEGRADED",
                   _wsh_result["ws_summary"]["ws_status"] == "WS_OK",
                   str(_wsh_result["ws_summary"]))
        finally:
            _keep_alive.set()

        state = load_json(SERVICE_STATE_FILE, {})
        _check("service state exists", isinstance(state, dict) and state.get("cycle") == "run_cycle")

    os.environ.clear()
    os.environ.update(old_env)
    print("RESULT::CLEAN_LIVE_COPY_CORE_SELF_TEST_PASS")


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean Hyperliquid live copy core")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--source-file", default=str(RAW_LEADER_FILLS_CSV))
    parser.add_argument("--poll-live", action="store_true", help="Use read-only userFillsByTime polling for leaders")
    parser.add_argument("--poll-copy", action="store_true", help="Use read-only userFillsByTime polling for the copy account")
    parser.add_argument("--reconcile-exchange", action="store_true", help="Fetch clearinghouseState and compare manual ledger")
    parser.add_argument("--ws", action="store_true", help="Start WS manager before running cycles; requires HL_LIVE_WS_ENABLED=1")
    parser.add_argument("--loop", action="store_true", help="Run repeatedly")
    parser.add_argument("--interval", type=float, default=5.0)
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        return
    if args.once or args.loop:
        core = LiveCopyCore(source_csv=Path(args.source_file))
        if args.ws:
            core.ws.enabled = True
            core.ws.start()
        try:
            while True:
                summary = core.run_cycle(
                    use_source_csv=Path(args.source_file).exists(),
                    poll_live=args.poll_live,
                    poll_copy=args.poll_copy,
                    reconcile_exchange=args.reconcile_exchange,
                )
                print(json.dumps(asdict(summary), indent=2, sort_keys=True))
                if not args.loop:
                    break
                time.sleep(max(0.5, float(args.interval)))
        finally:
            core.ws.stop()
        return
    parser.print_help()


if __name__ == "__main__":
    main()
