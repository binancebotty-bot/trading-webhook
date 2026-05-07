"""
HL_Copy_App_SSOT.py

Pure app/model/presentation layer for the Hyperliquid copy-truth stack.

Role boundary:
- Reads engine SSOT only: hl_copy_output/engine_truth.json + raw_live_fills.csv.
- Performs ALL post-fact modelling: proportional/fixed copy models, normalisation,
  portfolio aggregation, equity curves, ranking, derived copy trades.
- Never mutates engine truth. All generated files are explicitly app-derived.

Outputs:
- hl_copy_output/app_model_state.json   derived model snapshot
- hl_copy_output/copy_trades.csv        derived closed copy trades
- hl_copy_output/portfolio_history.json derived portfolio curve
- hl_copy_output/equity_history.json    derived per-wallet curves

Run:
    python HL_Copy_App_SSOT.py
or:
    uvicorn HL_Copy_App_SSOT:app --host 127.0.0.1 --port 8000
"""
from __future__ import annotations

import csv
import json
import math
import os
import html
import shutil
import time
import threading
import urllib.request
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    import uvicorn  # type: ignore
except Exception:  # pragma: no cover
    uvicorn = None

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "hl_copy_output"
ENGINE_TRUTH_JSON = DATA_DIR / "engine_truth.json"
LEGACY_LIVE_STATE_JSON = DATA_DIR / "live_state.json"
RAW_FILLS_CSV = DATA_DIR / "raw_live_fills.csv"
APP_MODEL_STATE_JSON = DATA_DIR / "app_model_state.json"
COPY_TRADES_CSV = DATA_DIR / "copy_trades.csv"
EXPECTED_COPY_FILLS_CSV = DATA_DIR / "expected_copy_fills.csv"
LIVE_WALLET_METRICS_CSV = DATA_DIR / "live_wallet_metrics.csv"
EXCHANGE_BASELINES_JSON = DATA_DIR / "exchange_baselines.json"
PORTFOLIO_HISTORY_FILE = DATA_DIR / "portfolio_history.json"
EQUITY_HISTORY_FILE = DATA_DIR / "equity_history.json"
UI_STATE_FILE = BASE_DIR / "ui_state.json"
WALLET_GATE_FILE = BASE_DIR / "wallet_gate.json"
MANUAL_WALLETS_FILE = BASE_DIR / "manual_wallets.txt"
PURGED_WALLETS_FILE = BASE_DIR / "purged_wallets.txt"
LIVE_COPY_AUDIT_DIR = BASE_DIR / "hl_live_copy_audit"
LIVE_COPY_CONFIG_FILE = LIVE_COPY_AUDIT_DIR / "live_config.json"
LIVE_COPY_WS_HEALTH_FILE = LIVE_COPY_AUDIT_DIR / "live_ws_health.json"
LIVE_COPY_SERVICE_STATE_FILE = LIVE_COPY_AUDIT_DIR / "live_service_state.json"
LIVE_COPY_CORE_STATE_FILE = LIVE_COPY_AUDIT_DIR / "clean_core_runtime_state.json"
LIVE_COPY_RECONCILIATION_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "reconciliation.csv"
LIVE_COPY_ORDER_INTENTS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "order_intents.csv"
MANUAL_POSITIONS_FILE = LIVE_COPY_AUDIT_DIR / "manual_live_positions.json"
SEND_ATTEMPTS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "send_attempts.csv"
WOULD_SEND_ORDERS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "would_send_orders.csv"
EXCHANGE_ACCOUNT_SNAPSHOT_FILE = LIVE_COPY_AUDIT_DIR / "exchange_account_snapshot.json"
EXCHANGE_ACCOUNT_HISTORY_FILE = LIVE_COPY_AUDIT_DIR / "exchange_account_history.json"
LIVE_FILLS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "live_fills.csv"
MANUAL_RECON_BACKUP_DIR = LIVE_COPY_AUDIT_DIR / "reconciliation_backups"
MANUAL_RECON_ACTIONS_FILE = LIVE_COPY_AUDIT_DIR / "manual_reconciliation_actions.json"
LIVE_CONFIG_DIR = BASE_DIR / "hl_live_copy_audit"
LIVE_CONFIG_FILE = LIVE_CONFIG_DIR / "live_config.json"
SNAP_DIR = DATA_DIR / "snapshots"

USER_WALLET = (os.getenv("HL_USER_WALLET") or "0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205").lower()
DEFAULT_NORM_BASE = 100.0
DEFAULT_LEADER_EQUITY = 10_000.0
DEFAULT_FIXED_NOTIONAL = 100.0
DEFAULT_FEE_BPS = 5.0
DEFAULT_COPY_FRICTION_BPS = 0.0
FEE_BPS = 5.0
COPY_FRICTION_BPS = DEFAULT_COPY_FRICTION_BPS
EQUITY_HISTORY_MAX = 20000
CHART_POINT_MAX = 1200
WS_CAPTURED = "WS_CAPTURED"
REBUILD = "REBUILD"
_LOCAL_NOOP_STATUSES: frozenset = frozenset({
    "NO_MANUAL_POSITION_TO_CLOSE",
    "ALREADY_FLAT",
    "LEDGER_FLAT",
    "NO_POSITION_TO_CLOSE",
})

app = FastAPI(title="HL Copy Dashboard SSOT")


def load_local_env_file(path: Optional[Path] = None) -> None:
    if path is None:
        path = BASE_DIR.parent / "hl_stage2.env"
    try:
        env_path = Path(path)
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip()
    except Exception:
        pass


load_local_env_file()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        x = float(value)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def money2(value: Any) -> float:
    return round(fnum(value), 2)


def contract_money_equal(a: Any, b: Any, tolerance: float = 0.05) -> bool:
    return abs(money2(a) - money2(b)) <= tolerance


def inum(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


_FILE_WRITE_LOCK = threading.RLock()
_MODEL_BUILD_LOCK = threading.RLock()
_MODEL_CACHE: Dict[str, Any] = {"state": None, "built_at": 0.0}
APP_HEALTH: Dict[str, Any] = {
    "last_build_started_at": "",
    "last_build_finished_at": "",
    "last_build_seconds": 0.0,
    "last_persist_at": "",
    "last_error": "",
    "cache_hits": 0,
    "build_count": 0,
}


def _unique_tmp_path(path: Path) -> Path:
    stamp = f"{os.getpid()}_{threading.get_ident()}_{time.time_ns()}"
    return path.with_name(f"{path.name}.{stamp}.tmp")


def _replace_with_retries(tmp: Path, path: Path, attempts: int = 30, delay: float = 0.075) -> None:
    last_err: Optional[Exception] = None
    for _ in range(attempts):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_err = exc
            time.sleep(delay)
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass
    raise last_err if last_err else PermissionError(f"Could not replace {path}")


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _unique_tmp_path(path)
    with _FILE_WRITE_LOCK:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        _replace_with_retries(tmp, path)


def atomic_write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _unique_tmp_path(path)
    with _FILE_WRITE_LOCK:
        with tmp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fieldnames})
        _replace_with_retries(tmp, path)


def normalise_wallet_for_purge(wallet: Any) -> str:
    value = str(wallet or "").strip().lower()
    if not value.startswith("0x") or len(value) != 42:
        raise ValueError("BAD_WALLET")
    if any(c not in "0123456789abcdef" for c in value[2:]):
        raise ValueError("BAD_WALLET")
    return value


def load_purged_wallets() -> set[str]:
    if not PURGED_WALLETS_FILE.exists():
        return set()
    wallets: set[str] = set()
    for line in PURGED_WALLETS_FILE.read_text(encoding="utf-8").splitlines():
        try:
            wallets.add(normalise_wallet_for_purge(line))
        except ValueError:
            continue
    return wallets


def save_purged_wallets(wallets: set[str]) -> None:
    PURGED_WALLETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _unique_tmp_path(PURGED_WALLETS_FILE)
    valid = sorted(normalise_wallet_for_purge(w) for w in wallets)
    with _FILE_WRITE_LOCK:
        tmp.write_text("".join(f"{w}\n" for w in valid), encoding="utf-8")
        _replace_with_retries(tmp, PURGED_WALLETS_FILE)


def backup_purge_files(wallet: str, files: List[Path]) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = DATA_DIR / "purge_backups" / f"{stamp}_{wallet[2:10]}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    seen: set[Path] = set()
    for path in files:
        p = Path(path)
        if p in seen or not p.exists() or not p.is_file():
            continue
        seen.add(p)
        try:
            rel = p.resolve().relative_to(BASE_DIR.resolve())
            dest = backup_dir / rel
        except Exception:
            dest = backup_dir / p.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dest)
    return backup_dir


def remove_wallet_from_text_file(path: Path, wallet: str) -> int:
    wallet = normalise_wallet_for_purge(wallet)
    if not path.exists():
        return 0
    lines = path.read_text(encoding="utf-8").splitlines()
    kept = [line for line in lines if line.strip().lower() != wallet]
    removed = len(lines) - len(kept)
    if removed:
        tmp = _unique_tmp_path(path)
        with _FILE_WRITE_LOCK:
            tmp.write_text("".join(f"{line}\n" for line in kept), encoding="utf-8")
            _replace_with_retries(tmp, path)
    return removed


def remove_wallet_rows_from_csv(path: Path, wallet: str) -> int:
    wallet = normalise_wallet_for_purge(wallet)
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    if "wallet" not in fieldnames:
        return 0
    kept = [row for row in rows if str(row.get("wallet", "")).strip().lower() != wallet]
    removed = len(rows) - len(kept)
    if removed:
        atomic_write_csv(path, fieldnames, kept)
    return removed


def _remove_wallet_from_json_obj(obj: Any, wallet: str) -> Tuple[Any, int]:
    removed = 0
    if isinstance(obj, dict):
        if wallet in obj:
            obj.pop(wallet, None)
            removed += 1
        for key in list(obj.keys()):
            child, child_removed = _remove_wallet_from_json_obj(obj[key], wallet)
            obj[key] = child
            removed += child_removed
        return obj, removed
    if isinstance(obj, list):
        kept: List[Any] = []
        for item in obj:
            if isinstance(item, dict) and str(item.get("wallet", "")).strip().lower() == wallet:
                removed += 1
                continue
            child, child_removed = _remove_wallet_from_json_obj(item, wallet)
            kept.append(child)
            removed += child_removed
        return kept, removed
    return obj, 0


def remove_wallet_from_json(path: Path, wallet: str) -> int:
    wallet = normalise_wallet_for_purge(wallet)
    if not path.exists():
        return 0
    payload = load_json(path, None)
    payload, removed = _remove_wallet_from_json_obj(payload, wallet)
    if removed:
        atomic_write_json(path, payload)
    return removed


def purge_wallet_everywhere(wallet: str) -> Dict[str, Any]:
    """ADMIN MAINTENANCE ONLY: purge a wallet from active app/engine-loaded files."""
    wallet = normalise_wallet_for_purge(wallet)
    if wallet == USER_WALLET:
        raise ValueError("CANNOT_PURGE_USER_WALLET")

    csv_files = [RAW_FILLS_CSV, EXPECTED_COPY_FILLS_CSV, COPY_TRADES_CSV, LIVE_WALLET_METRICS_CSV]
    json_files = [
        UI_STATE_FILE,
        WALLET_GATE_FILE,
        LIVE_COPY_CONFIG_FILE,
        EXCHANGE_BASELINES_JSON,
        ENGINE_TRUTH_JSON,
        LEGACY_LIVE_STATE_JSON,
    ]
    text_files = [PURGED_WALLETS_FILE, MANUAL_WALLETS_FILE]
    derived_files = [APP_MODEL_STATE_JSON, PORTFOLIO_HISTORY_FILE, EQUITY_HISTORY_FILE]
    backup_dir = backup_purge_files(wallet, text_files + json_files + csv_files + derived_files)

    purged = load_purged_wallets()
    purged.add(wallet)
    save_purged_wallets(purged)

    text_removed = {
        "manual_wallets.txt": remove_wallet_from_text_file(MANUAL_WALLETS_FILE, wallet),
    }

    json_removed: Dict[str, int] = {}
    ui = load_json(UI_STATE_FILE, {})
    ui_removed = 0
    if isinstance(ui, dict):
        for key in ("wallet_config", "wallet_include"):
            section = ui.get(key)
            if isinstance(section, dict) and wallet in section:
                section.pop(wallet, None)
                ui_removed += 1
        if ui_removed:
            atomic_write_json(UI_STATE_FILE, ui)
    json_removed["ui_state.json"] = ui_removed

    wallet_gate = load_json(WALLET_GATE_FILE, {})
    gate_removed = 0
    if isinstance(wallet_gate, dict) and wallet in wallet_gate:
        wallet_gate.pop(wallet, None)
        gate_removed = 1
        atomic_write_json(WALLET_GATE_FILE, wallet_gate)
    json_removed["wallet_gate.json"] = gate_removed

    live_config = load_json(LIVE_COPY_CONFIG_FILE, {})
    live_removed = 0
    if isinstance(live_config, dict):
        wallets = live_config.get("wallets")
        if isinstance(wallets, dict) and wallet in wallets:
            wallets.pop(wallet, None)
            live_removed = 1
            atomic_write_json(LIVE_COPY_CONFIG_FILE, live_config)
    json_removed["hl_live_copy_audit/live_config.json"] = live_removed

    for path in (EXCHANGE_BASELINES_JSON, ENGINE_TRUTH_JSON, LEGACY_LIVE_STATE_JSON):
        json_removed[str(path.relative_to(BASE_DIR) if path.is_relative_to(BASE_DIR) else path)] = remove_wallet_from_json(path, wallet)

    csv_rows_removed = {str(path.relative_to(DATA_DIR) if path.is_relative_to(DATA_DIR) else path): remove_wallet_rows_from_csv(path, wallet) for path in csv_files}

    deleted: List[str] = []
    for path in derived_files:
        if path.exists():
            path.unlink()
            deleted.append(str(path.relative_to(BASE_DIR) if path.is_relative_to(BASE_DIR) else path))

    invalidate_model_cache()
    return {
        "ok": True,
        "wallet": wallet,
        "backup_dir": str(backup_dir),
        "text_removed": text_removed,
        "csv_rows_removed": csv_rows_removed,
        "json_removed": json_removed,
        "derived_deleted": deleted,
    }

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
    received_at_ms: int = 0
    received_at_iso: str = ""
    latency_ms: int = 0
    source: str = "unknown"
    shard_id: int = -1
    is_snapshot: bool = False
    raw_json: str = "{}"
    recording_method: str = REBUILD
    rebuild_reason: str = ""
    contributes_to_execution_delta: bool = False
    reconstructed: bool = False
    expected_copy_price: float = 0.0
    expected_copy_price_source: str = ""


@dataclass
class ModelPosition:
    trade_id: str
    wallet: str
    coin: str
    side: str
    entry_time_ms: int
    entry_time_iso: str
    entry_price_lead: float
    entry_price_copy: float
    leader_size_units: float
    copy_size_units: float
    leader_notional: float
    copy_notional: float
    entry_fee_lead: float
    entry_fee_copy: float
    source: str
    latency_ms: int
    recording_method: str
    entry_contributes_to_execution_delta: bool
    entry_disadvantage_bps: Optional[float] = None
    entry_price_source: str = ""


@dataclass
class WalletModel:
    wallet: str
    alloc: float
    lead_realized: float = 0.0
    lead_unrealized: float = 0.0
    lead_equity: float = 0.0
    lead_peak: float = 0.0
    lead_max_drawdown: float = 0.0
    copy_realized: float = 0.0
    copy_unrealized: float = 0.0
    copy_equity: float = 0.0
    copy_peak: float = 0.0
    copy_max_drawdown: float = 0.0
    entry_count: int = 0
    exit_count: int = 0
    open_position_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    ws_fill_count: int = 0
    poll_fill_count: int = 0
    rebuild_fill_count: int = 0
    measured_delta_fill_count: int = 0
    measured_delta_entry_count: int = 0
    measured_delta_exit_count: int = 0
    total_entry_disadvantage_bps: float = 0.0
    total_exit_disadvantage_bps: float = 0.0
    fill_count: int = 0
    current_position_usd: float = 0.0
    max_position_usd: float = 0.0
    position_exposure_sum: float = 0.0
    position_exposure_samples: int = 0
    entry_notional_sum: float = 0.0
    entry_notional_count: int = 0
    entry_notional_ge10_count: int = 0
    max_entry_notional_usd: float = 0.0
    # Latency diagnostics are websocket-capture quality only.
    # REBUILD/poll rows can carry synthetic/REST latency and must never trigger
    # HIGH_LATENCY or contaminate execution-quality diagnostics.
    ws_latency_count: int = 0
    avg_ws_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0  # legacy alias; equals avg_ws_latency_ms
    last_ts: str = ""
    flags: List[str] = field(default_factory=list)
    curve: List[Dict[str, Any]] = field(default_factory=list)

    def sync_equity(self) -> None:
        self.lead_equity = self.alloc + self.lead_realized + self.lead_unrealized
        self.copy_equity = self.alloc + self.copy_realized + self.copy_unrealized
        self.lead_peak = max(self.lead_peak or self.alloc, self.lead_equity)
        self.copy_peak = max(self.copy_peak or self.alloc, self.copy_equity)
        self.lead_max_drawdown = max(self.lead_max_drawdown, max(0.0, self.lead_peak - self.lead_equity))
        self.copy_max_drawdown = max(self.copy_max_drawdown, max(0.0, self.copy_peak - self.copy_equity))


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def load_engine_truth() -> Dict[str, Any]:
    truth = load_json(ENGINE_TRUTH_JSON, None)
    if not isinstance(truth, dict):
        legacy = load_json(LEGACY_LIVE_STATE_JSON, {})
        truth = legacy if isinstance(legacy, dict) else {}
    # ADMIN MAINTENANCE ONLY: locally blacklisted wallets stay hidden from
    # dashboard/model reads even if an active engine-loaded file is repopulated.
    for wallet in load_purged_wallets():
        truth, _ = _remove_wallet_from_json_obj(truth, wallet)
    return truth


def _clean_wallet_config(raw_cfg: Any) -> Dict[str, Dict[str, Any]]:
    """Sanitise optional per-wallet model overrides.

    Empty/missing wallet config means: inherit the global header settings.
    Supported per-wallet keys: copy_mode, norm_base, fixed_notional.
    """
    if not isinstance(raw_cfg, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for wallet, cfg in raw_cfg.items():
        w = str(wallet).strip().lower()
        if not w or not isinstance(cfg, dict):
            continue
        item: Dict[str, Any] = {}
        mode = str(cfg.get("copy_mode", cfg.get("mode", ""))).strip().lower()
        if mode in {"proportional", "fixed"}:
            item["copy_mode"] = mode
        if "norm_base" in cfg and str(cfg.get("norm_base", "")).strip() != "":
            item["norm_base"] = max(1.0, fnum(cfg.get("norm_base"), DEFAULT_NORM_BASE))
        if "fixed_notional" in cfg and str(cfg.get("fixed_notional", "")).strip() != "":
            item["fixed_notional"] = max(0.01, fnum(cfg.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL))
        if item:
            out[w] = item
    return out


def _clean_wallet_include(raw_inc: Any) -> Dict[str, bool]:
    """Sanitise display-only portfolio include/exclude map.

    Missing wallets default to included. This affects combined headers/graph
    only; it never changes raw fills, wallet gate mode, or per-wallet rows.
    """
    if not isinstance(raw_inc, dict):
        return {}
    out: Dict[str, bool] = {}
    for wallet, enabled in raw_inc.items():
        w = str(wallet).strip().lower()
        if not w:
            continue
        out[w] = parse_bool(enabled)
    return out


WALLET_META_TAGS = {"none", "watch", "scale", "risk", "remove", "blocked"}
WALLET_META_COLORS = {"none", "blue", "green", "yellow", "red", "purple"}


def sanitize_wallet_meta(meta: Any) -> Dict[str, Dict[str, str]]:
    if not isinstance(meta, dict):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for wallet, raw_item in meta.items():
        w = str(wallet).strip().lower()
        if not w or not isinstance(raw_item, dict):
            continue
        tag = str(raw_item.get("tag", "none")).strip().lower()
        color = str(raw_item.get("color", "none")).strip().lower()
        note = str(raw_item.get("note", "")).strip()
        if tag not in WALLET_META_TAGS:
            tag = "none"
        if color not in WALLET_META_COLORS:
            color = "none"
        out[w] = {"tag": tag, "note": note[:120], "color": color}
    return out


def get_wallet_meta(ui: Dict[str, Any], wallet: str) -> Dict[str, str]:
    meta = (ui.get("wallet_meta") or {}).get(str(wallet).strip().lower(), {})
    if not isinstance(meta, dict):
        return {"tag": "none", "note": "", "color": "none"}
    clean = sanitize_wallet_meta({"_": meta}).get("_", {})
    return clean or {"tag": "none", "note": "", "color": "none"}


def set_wallet_meta(ui: Dict[str, Any], wallet: str, tag: Any, note: Any, color: Any) -> Dict[str, Dict[str, str]]:
    wallet_key = str(wallet).strip().lower()
    meta = sanitize_wallet_meta(ui.get("wallet_meta", {}))
    if not wallet_key:
        return meta
    item = sanitize_wallet_meta({wallet_key: {"tag": tag, "note": note, "color": color}}).get(wallet_key, {"tag": "none", "note": "", "color": "none"})
    meta[wallet_key] = item
    return meta


def wallet_included(wallet: str, ui: Dict[str, Any]) -> bool:
    return bool((ui.get("wallet_include") or {}).get(str(wallet).lower(), True))


def load_ui_state() -> Dict[str, Any]:
    raw = load_json(UI_STATE_FILE, {})
    if not isinstance(raw, dict):
        raw = {"norm_base": fnum(raw, DEFAULT_NORM_BASE)}
    mode = str(raw.get("copy_mode", "proportional")).lower()
    if mode not in {"proportional", "fixed"}:
        mode = "proportional"
    norm_mode = str(raw.get("normalisation_mode", "equity")).lower()
    if norm_mode not in {"equity", "pnl"}:
        norm_mode = "equity"
    ranking = raw.get("ranking") if isinstance(raw.get("ranking"), dict) else {}
    ranking_col = ranking.get("column")
    ranking_dir = str(ranking.get("direction", "desc")).lower()
    ranking = {"column": ranking_col, "direction": ranking_dir if ranking_dir in {"asc", "desc"} else "desc"}
    norm_base = max(1.0, fnum(raw.get("norm_base"), DEFAULT_NORM_BASE))
    return {
        "norm_base": norm_base,
        "user_norm_base": max(1.0, fnum(raw.get("user_norm_base"), norm_base)),
        "copy_mode": mode,
        "normalisation_mode": norm_mode,
        "fixed_notional": max(0.01, fnum(raw.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL)),
        "leader_equity_base": max(1.0, fnum(raw.get("leader_equity_base"), DEFAULT_LEADER_EQUITY)),
        "fee_bps": max(0.0, fnum(raw.get("fee_bps"), DEFAULT_FEE_BPS)),
        "copy_friction_bps": max(0.0, fnum(raw.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS)),
        "wallet_config": _clean_wallet_config(raw.get("wallet_config", {})),
        "wallet_include": _clean_wallet_include(raw.get("wallet_include", {})),
        "wallet_meta": sanitize_wallet_meta(raw.get("wallet_meta", {})),
        "ranking": ranking,
    }


def save_ui_state(patch: Dict[str, Any]) -> Dict[str, Any]:
    raw_existing = load_json(UI_STATE_FILE, {})
    raw_has_user_base = isinstance(raw_existing, dict) and "user_norm_base" in raw_existing
    existing = load_ui_state()
    merged = {**existing, **patch}
    mode = str(merged.get("copy_mode", "proportional")).lower()
    merged["copy_mode"] = mode if mode in {"proportional", "fixed"} else "proportional"
    merged["norm_base"] = max(1.0, fnum(merged.get("norm_base"), DEFAULT_NORM_BASE))
    if "user_norm_base" in patch or raw_has_user_base:
        merged["user_norm_base"] = max(1.0, fnum(merged.get("user_norm_base"), merged.get("norm_base", DEFAULT_NORM_BASE)))
    else:
        merged["user_norm_base"] = merged["norm_base"]
    merged["fee_bps"] = max(0.0, fnum(merged.get("fee_bps"), DEFAULT_FEE_BPS))
    merged["copy_friction_bps"] = max(0.0, fnum(merged.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS))
    merged["wallet_config"] = _clean_wallet_config(merged.get("wallet_config", {}))
    merged["wallet_include"] = _clean_wallet_include(merged.get("wallet_include", {}))
    merged["wallet_meta"] = sanitize_wallet_meta(merged.get("wallet_meta", {}))
    ranking = merged.get("ranking") if isinstance(merged.get("ranking"), dict) else {}
    ranking_dir = str(ranking.get("direction", "desc")).lower()
    merged["ranking"] = {"column": ranking.get("column"), "direction": ranking_dir if ranking_dir in {"asc", "desc"} else "desc"}
    atomic_write_json(UI_STATE_FILE, merged)
    invalidate_model_cache()
    return merged


def load_wallet_gate() -> Dict[str, Any]:
    g = load_json(WALLET_GATE_FILE, {})
    return g if isinstance(g, dict) else {}


_GLOBAL_CONTROLS_DEFAULTS: Dict[str, Any] = {
    "max_total_live_exposure_usd": 0.0,
    "max_asset_directional_exposure_usd": 0.0,
    "max_wallet_exposure_usd": 0.0,
    "max_order_notional_usd": 0.0,
    "marketable_bps": 0.0,
    "max_close_adverse_diff_pct": 0.0,
    "symbol_allowlist": [],
    "symbol_blocklist": [],
}


def _normalise_global_controls(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    out = dict(_GLOBAL_CONTROLS_DEFAULTS)
    for k, default in _GLOBAL_CONTROLS_DEFAULTS.items():
        if k in raw:
            if isinstance(default, list):
                val = raw[k]
                out[k] = [str(s).strip().upper() for s in val] if isinstance(val, list) else []
            else:
                out[k] = max(0.0, fnum(raw[k]))
    if "marketable_slippage_pct" in raw:
        out["marketable_bps"] = max(0.0, fnum(raw.get("marketable_slippage_pct")) * 100.0)
    return out


def _global_controls_for_ui(raw: Any) -> Dict[str, Any]:
    out = _normalise_global_controls(raw)
    out["marketable_slippage_pct"] = round(fnum(out.get("marketable_bps")) / 100.0, 6)
    out["marketable_slippage_off"] = fnum(out.get("marketable_bps")) <= 0
    out["close_adverse_diff_off"] = fnum(out.get("max_close_adverse_diff_pct")) <= 0
    return out


def _load_live_copy_config() -> Dict[str, Any]:
    try:
        if LIVE_COPY_CONFIG_FILE.exists():
            cfg = json.loads(LIVE_COPY_CONFIG_FILE.read_text(encoding="utf-8-sig"))
        else:
            cfg = {}
    except Exception:
        cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}
    wallets = cfg.get("wallets")
    archived = cfg.get("archived_wallets")
    cfg["wallets"] = wallets if isinstance(wallets, dict) else {}
    cfg["archived_wallets"] = archived if isinstance(archived, dict) else {}
    gc = cfg.get("global_controls")
    cfg["global_controls"] = _normalise_global_controls(gc if isinstance(gc, dict) else {})
    return cfg


def _atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_json(path, payload)


def _save_live_copy_config(config: Dict[str, Any]) -> None:
    LIVE_COPY_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(LIVE_COPY_CONFIG_FILE, config)


def _normalise_wallet_address(wallet: Any) -> str:
    value = str(wallet or "").strip().lower()
    if not value.startswith("0x") or len(value) != 42:
        raise ValueError("BAD_WALLET")
    return value


def normalize_live_wallet_config(wallet: Any, cfg: Any, repair: bool = False) -> Dict[str, Any]:
    raw = cfg if isinstance(cfg, dict) else {}
    mode = str(raw.get("mode", "OFF")).upper()
    if mode not in {"LIVE", "CLO", "OFF"}:
        mode = "OFF"
    explicit_disabled = "enabled" in raw and parse_bool(raw.get("enabled")) is False
    if mode in {"LIVE", "CLO"}:
        if explicit_disabled and not repair:
            enabled = False
            reason = "CONFIG_CONFLICT_MODE_ENABLED_FALSE"
        else:
            enabled = True
            reason = "ENABLED_LIVE" if mode == "LIVE" else "ENABLED_CLO"
    else:
        enabled = False
        reason = "MODE_OFF"
    return {
        "wallet": str(wallet or "").lower().strip(),
        "mode": mode,
        "enabled": enabled,
        "service_eligible": bool(mode in {"LIVE", "CLO"} and enabled),
        "service_eligibility_reason": reason,
    }


def repair_live_config_consistency(config: Dict[str, Any]) -> bool:
    wallets = config.get("wallets", {}) if isinstance(config, dict) else {}
    if not isinstance(wallets, dict):
        return False
    changed = False
    for wallet, cfg in wallets.items():
        if not isinstance(cfg, dict):
            continue
        normal = normalize_live_wallet_config(wallet, cfg, repair=True)
        if cfg.get("mode") != normal["mode"]:
            cfg["mode"] = normal["mode"]
            changed = True
        if cfg.get("enabled") is not normal["enabled"]:
            cfg["enabled"] = normal["enabled"]
            changed = True
    return changed


def _normalise_live_wallet_payload(payload: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = dict(existing or {})
    mode_default = base.get("mode", "OFF") if existing is not None else "OFF"
    mode = str(payload.get("mode", mode_default)).upper()
    if mode not in {"LIVE", "CLO", "OFF"}:
        raise ValueError("BAD_MODE")
    copy_mode = str(payload.get("copy_mode", base.get("copy_mode", "proportional"))).lower()
    if copy_mode not in {"proportional", "fixed"}:
        raise ValueError("BAD_COPY_MODE")
    enabled = normalize_live_wallet_config("", {"mode": mode}, repair=True)["enabled"]
    out = {
        "mode": mode,
        "copy_mode": copy_mode,
        "norm_base": max(1.0, fnum(payload.get("norm_base", base.get("norm_base", 100)), 100)),
        "fixed_notional": max(0.01, fnum(payload.get("fixed_notional", base.get("fixed_notional", 10)), 10)),
        "leader_equity_base": max(1.0, fnum(payload.get("leader_equity_base", base.get("leader_equity_base", 10000)), 10000)),
        "max_diff_pct": max(0.0, fnum(payload.get("max_diff_pct", base.get("max_diff_pct", 0.1)), 0.1)),
        "daily_loss_limit": max(0.0, fnum(payload.get("daily_loss_limit", base.get("daily_loss_limit", 0)), 0)),
        "enabled": enabled,
    }
    for key, value in base.items():
        if key not in out:
            out[key] = value
    return out


def _active_live_copy_wallet_count(config: Dict[str, Any]) -> int:
    wallets = config.get("wallets", {}) if isinstance(config, dict) else {}
    return sum(
        1 for cfg in wallets.values()
        if isinstance(cfg, dict) and normalize_live_wallet_config("", cfg).get("service_eligible")
    )


def _live_copy_config_response(config: Dict[str, Any]) -> Dict[str, Any]:
    wallets = config.get("wallets", {}) if isinstance(config.get("wallets"), dict) else {}
    return {
        "ok": True,
        "config": config,
        "wallet_count": len(wallets),
        "active_wallets": _active_live_copy_wallet_count(config),
        "max_wallets": 10,
    }


def _live_config_error(error: str, status_code: int = 400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": error}, status_code=status_code)


def _enforce_live_copy_cap(config: Dict[str, Any]) -> None:
    if _active_live_copy_wallet_count(config) > 10:
        raise ValueError("MAX_WALLETS_EXCEEDED")


def _load_live_ws_health() -> Dict[str, Any]:
    data = load_json(LIVE_COPY_WS_HEALTH_FILE, None)
    if not isinstance(data, dict):
        return {"enabled": False, "overall": "OFFLINE", "wallets": {}}
    if "ws_summary" in data and "overall" not in data:
        ws_summary = data.get("ws_summary") or {}
        ws_status = str(ws_summary.get("ws_status", "OFFLINE"))
        data = {**data, "overall": ws_status, "enabled": ws_status not in {"WS_DISABLED", "OFFLINE", ""}}
    return data


def _load_clean_core_status() -> Dict[str, Any]:
    service_state = load_json(LIVE_COPY_SERVICE_STATE_FILE, {})
    core_state = load_json(LIVE_COPY_CORE_STATE_FILE, {})
    if not isinstance(service_state, dict):
        service_state = {}
    if not isinstance(core_state, dict):
        core_state = {}
    return {
        "service_state": service_state,
        "core_state": core_state,
        "active_state": core_state if core_state else service_state,
        "available": bool(service_state or core_state),
        "checkpoint_available": bool(core_state),
    }


def _live_order_intents_path() -> Path:
    if LIVE_COPY_ORDER_INTENTS_CSV.exists():
        return LIVE_COPY_ORDER_INTENTS_CSV
    return LIVE_COPY_AUDIT_DIR / "order_intents.csv"


def _live_audit_summary() -> Dict[str, Any]:
    path = _live_order_intents_path()
    reason_counts: Dict[str, int] = {}
    status_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    execution_decision_counts: Dict[str, int] = {}
    decision_reason_counts: Dict[str, int] = {}
    manual_reconcile_required_counts: Dict[str, int] = {}
    market_data_error_counts: Dict[str, int] = {}
    last_rows: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []
    rows = 0
    if path.exists():
        try:
            with path.open("r", newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    rows += 1
                    reason = str(row.get("reason", "") or "UNKNOWN")
                    status = str(row.get("status", "") or "UNKNOWN")
                    notes = str(row.get("notes", "") or "")
                    source = str(row.get("source", "") or "")
                    if not source and "source=" in notes:
                        source = notes.split("source=", 1)[1].split(";", 1)[0].split()[0]
                    execution_decision = str(row.get("execution_decision", "") or "UNKNOWN")
                    decision_reason = str(row.get("decision_reason", "") or "UNKNOWN")
                    manual_required = str(row.get("manual_reconcile_required", "") or "UNKNOWN")
                    market_data_error = str(row.get("market_data_error", "") or "")
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                    status_counts[status] = status_counts.get(status, 0) + 1
                    execution_decision_counts[execution_decision] = execution_decision_counts.get(execution_decision, 0) + 1
                    decision_reason_counts[decision_reason] = decision_reason_counts.get(decision_reason, 0) + 1
                    manual_reconcile_required_counts[manual_required] = manual_reconcile_required_counts.get(manual_required, 0) + 1
                    if market_data_error:
                        market_data_error_counts[market_data_error] = market_data_error_counts.get(market_data_error, 0) + 1
                    if source:
                        source_counts[source] = source_counts.get(source, 0) + 1
                    audit_rows.append(row)
                    if len(audit_rows) > 5000:
                        audit_rows.pop(0)
                    last_rows.append(row)
                    if len(last_rows) > 20:
                        last_rows.pop(0)
        except Exception:
            pass
    manual_positions = _load_manual_live_positions()
    recent_send_attempts = _load_recent_send_attempts(100)
    metric_send_attempts = _load_recent_send_attempts(5000)
    send_attempt_counts = _load_send_attempt_counts()
    exchange_snapshot = _fetch_exchange_account_snapshot()
    manual_live_summary = _manual_live_summary(manual_positions, recent_send_attempts, exchange_snapshot)
    _append_exchange_history(exchange_snapshot, manual_live_summary)
    ws_health = _load_live_ws_health()
    clean_core_status = _load_clean_core_status()

    live_config = _load_live_copy_config()
    _auto_send_enabled = os.getenv("HL_LIVE_AUTO_SEND_ENABLED", "0") == "1"
    _auto_send_filter = os.getenv("HL_LIVE_AUTO_SEND_WALLET", "").lower().strip()
    _cfg_wallets = live_config.get("wallets", {})
    _auto_live_eligible: List[str] = [
        w.lower() for w, cfg in (_cfg_wallets.items() if isinstance(_cfg_wallets, dict) else [])
        if isinstance(cfg, dict)
        and normalize_live_wallet_config(w, cfg).get("service_eligible")
        and (not _auto_send_filter or w.lower() == _auto_send_filter)
    ] if _auto_send_enabled else []
    model_portfolio: Dict[str, Any] = {}
    live_wallet_derived = {}
    live_leader_performance = _build_live_leader_performance(metric_send_attempts, manual_positions, exchange_snapshot, live_config, audit_rows)
    matched_exchange_fills = sum(inum(v.get("matched_exchange_fill_count")) for v in live_leader_performance.values() if isinstance(v, dict))
    filled_attempt_count = sum(inum(v.get("filled_count")) for v in live_leader_performance.values() if isinstance(v, dict))
    exit_attempt_count = sum(inum(v.get("exits_count")) for v in live_leader_performance.values() if isinstance(v, dict))
    exchange_snapshot["realized_pnl_matched_send_attempt_count"] = matched_exchange_fills
    exchange_snapshot["realized_pnl_unmatched_send_attempt_count"] = max(0, filled_attempt_count - matched_exchange_fills)
    exchange_snapshot["realized_pnl_exit_attempt_count"] = exit_attempt_count
    if exit_attempt_count and abs(fnum(exchange_snapshot.get("realized_pnl_today"))) <= 1e-12 and matched_exchange_fills == 0:
        exchange_snapshot["realized_pnl_diagnostic"] = "Exchange closedPnl not found for matched exits"
    else:
        exchange_snapshot["realized_pnl_diagnostic"] = ""
    account_reconciliation = _build_account_reconciliation(exchange_snapshot, live_leader_performance)
    _live_fills_data = _load_recent_live_fills(500)
    live_wallet_rows = _build_live_wallet_rows(live_config, audit_rows, metric_send_attempts, manual_positions, manual_live_summary, ws_health, live_leader_performance, live_fills=_live_fills_data)
    real_copy_positions = _build_real_copy_positions(manual_positions, exchange_snapshot)
    execution_quality_rows = _build_execution_quality_rows(metric_send_attempts, audit_rows, live_fills=_live_fills_data)
    execution_quality_summary = _build_execution_quality_summary(execution_quality_rows)
    manual_reconciliation_rows = _build_manual_reconciliation_rows(manual_positions, exchange_snapshot, recent_send_attempts)
    recent_send_warning_groups = _build_recent_send_warning_groups(recent_send_attempts)

    portfolio_history: List[Dict[str, Any]] = []
    exchange_history = load_json(EXCHANGE_ACCOUNT_HISTORY_FILE, [])
    fills_recent = exchange_snapshot.get("actual_user_fills_recent", [])
    rpnl_points: List[Dict[str, Any]] = []
    for _fill in (fills_recent if isinstance(fills_recent, list) else []):
        if not isinstance(_fill, dict):
            continue
        _ts = inum(_fill.get("time") or _fill.get("timestamp") or _fill.get("ts"))
        if _ts <= 0:
            continue
        rpnl_points.append({"ts": _ts, "pnl": fnum(_fill.get("closedPnl") or _fill.get("closed_pnl") or 0)})
    rpnl_points.sort(key=lambda x: x["ts"])

    return {
        "ok": True,
        "rows": rows,
        "reason_counts": reason_counts,
        "status_counts": status_counts,
        "source_counts": source_counts,
        "execution_decision_counts": execution_decision_counts,
        "decision_reason_counts": decision_reason_counts,
        "manual_reconcile_required_counts": manual_reconcile_required_counts,
        "market_data_error_counts": market_data_error_counts,
        "last_rows": last_rows,
        "audit_rows": audit_rows[-200:],
        "manual_positions": manual_positions,
        "recent_send_attempts": recent_send_attempts,
        "send_attempt_counts": send_attempt_counts,
        "manual_live_summary": manual_live_summary,
        "manual_reconciliation_rows": manual_reconciliation_rows,
        "recent_send_warning_groups": recent_send_warning_groups,
        "recent_send_warning_group_count": len(recent_send_warning_groups),
        "exchange_account_snapshot": exchange_snapshot,
        "account_reconciliation": account_reconciliation,
        "model_portfolio": model_portfolio,
        "portfolio_history": portfolio_history,
        "exchange_history": exchange_history,
        "exchange_account_history": exchange_history,
        "live_graph": {
            "default_mode": "account",
            "timescales": ["1d", "7d", "all"],
            "modes": ["account", "wallet_pnl", "exposure"],
            "account_points": exchange_history,
            "realized_pnl_points": rpnl_points,
        },
        "live_wallet_derived": live_wallet_derived,
        "live_wallet_rows": live_wallet_rows,
        "live_leader_performance": live_leader_performance,
        "real_copy_positions": real_copy_positions,
        "execution_quality_rows": execution_quality_rows,
        "execution_quality_summary": execution_quality_summary,
        "auto_live_eligible_wallets": _auto_live_eligible,
        "tracked_wallet_count": len(_cfg_wallets) if isinstance(_cfg_wallets, dict) else 0,
        "auto_live_wallet_count": len(_auto_live_eligible),
        "clean_core_status": clean_core_status,
        "core_service_state": clean_core_status.get("service_state", {}),
    }


def _load_manual_live_positions() -> Dict[str, Any]:
    try:
        if MANUAL_POSITIONS_FILE.exists():
            data = json.loads(MANUAL_POSITIONS_FILE.read_text(encoding="utf-8-sig"))
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def iter_manual_wallet_positions(manual_positions: Dict[str, Any]):
    if not isinstance(manual_positions, dict):
        return
    _schema = str(manual_positions.get("schema", ""))
    # Handle v2 and v1.wallet_sleeves schemas — both use the by_wallet structure
    if _schema == "manual_live_positions.v2" or _schema.startswith("manual_live_positions.v1"):
        by_wallet = manual_positions.get("by_wallet", {})
        if not isinstance(by_wallet, dict):
            return
        for wallet, coins in by_wallet.items():
            if not isinstance(coins, dict):
                continue
            wallet_key = str(wallet or "").lower().strip()
            for coin, pos in coins.items():
                if isinstance(pos, dict):
                    yield wallet_key, str(coin or "").upper().strip(), pos
        return
    for coin, pos in manual_positions.items():
        if not isinstance(pos, dict):
            continue
        wallet = str(pos.get("leader_wallet") or pos.get("wallet") or "").lower().strip()
        yield wallet, str(coin or "").upper().strip(), pos


def _manual_position_sleeves(manual_positions: Dict[str, Any]) -> List[Tuple[str, str, Dict[str, Any]]]:
    sleeves: List[Tuple[str, str, Dict[str, Any]]] = []
    for wallet, coin, pos in iter_manual_wallet_positions(manual_positions):
        if not coin or not isinstance(pos, dict):
            continue
        if abs(fnum(pos.get("signed_size"))) <= 1e-12:
            continue
        sleeves.append((wallet, coin, pos))
    return sleeves


def _shared_manual_coin_nets(sleeves: List[Tuple[str, str, Dict[str, Any]]]) -> Tuple[Dict[str, int], Dict[str, float]]:
    counts: Dict[str, int] = {}
    nets: Dict[str, float] = {}
    wallets_by_coin: Dict[str, set[str]] = {}
    for wallet, coin, pos in sleeves:
        wallets_by_coin.setdefault(coin, set()).add(str(wallet or ""))
        nets[coin] = nets.get(coin, 0.0) + fnum(pos.get("signed_size"))
    for coin, wallets in wallets_by_coin.items():
        counts[coin] = len(wallets)
    return counts, nets


def _load_recent_send_attempts(limit: int = 20) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not SEND_ATTEMPTS_CSV.exists():
        return out
    try:
        with SEND_ATTEMPTS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                # Support both old "response" column and new-core "exchange_response" column
                resp_text = str(row.get("exchange_response") or row.get("response") or "")
                parsed: Dict[str, Any] = {}
                if resp_text:
                    try:
                        resp = json.loads(resp_text)
                        if not isinstance(resp, dict):
                            resp = {}
                    except Exception:
                        resp = {}
                    payload = resp.get("payload") or {}
                    for key in (
                        "fill_avg_px", "fill_size", "oid",
                        "position_before", "position_after",
                        "price_source", "size_source",
                        "close_adverse_diff_pct", "close_adverse_diff_limit_pct",
                        "notional_cap_reason", "error", "auto_live", "auto_send_wallet",
                    ):
                        val = resp.get(key)
                        if val is None:
                            val = payload.get(key)
                        if val is not None:
                            parsed[key] = val
                    # New-core Hyperliquid exchange_response format:
                    # {"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.02","avgPx":"559.92","oid":415169026806}}]}}}
                    if "fill_avg_px" not in parsed or "fill_size" not in parsed:
                        order_r = resp.get("response") if isinstance(resp.get("response"), dict) else {}
                        order_d = order_r.get("data") if isinstance(order_r, dict) else {}
                        statuses = order_d.get("statuses") if isinstance(order_d, dict) else None
                        if isinstance(statuses, list) and statuses:
                            filled = statuses[0].get("filled") if isinstance(statuses[0], dict) else None
                            if isinstance(filled, dict):
                                if "fill_avg_px" not in parsed and filled.get("avgPx"):
                                    try:
                                        parsed["fill_avg_px"] = float(filled["avgPx"])
                                    except Exception:
                                        pass
                                if "fill_size" not in parsed and filled.get("totalSz"):
                                    try:
                                        parsed["fill_size"] = float(filled["totalSz"])
                                    except Exception:
                                        pass
                    # actual_side: prefer executed side from response/payload over CSV intent side
                    actual_side = resp.get("side") or payload.get("side") or row.get("side")
                    if actual_side:
                        parsed["actual_side"] = actual_side
                # exchange_order_id column (new-core) → oid fallback
                if "oid" not in parsed and row.get("exchange_order_id"):
                    parsed["oid"] = str(row["exchange_order_id"])
                out.append({**row, **parsed})
        return out[-limit:]
    except Exception:
        return out[-limit:] if len(out) >= limit else out


def _load_recent_live_fills(limit: int = 500) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not LIVE_FILLS_CSV.exists():
        return out
    try:
        with LIVE_FILLS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                out.append(dict(row))
        return out[-limit:]
    except Exception:
        return out[-limit:] if len(out) >= limit else out


def _local_env_value(key: str) -> str:
    value = os.getenv(key, "").strip()
    if value:
        return value
    env_file = BASE_DIR.parent / "hl_stage2.env"
    try:
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                if k.strip() == key:
                    return v.strip()
    except Exception:
        pass
    return ""


def _execution_guards_info() -> Dict[str, Any]:
    def ev(key: str, default: str = "") -> str:
        return _local_env_value(key) or os.getenv(key, default)
    return {
        "auto_send_enabled": ev("HL_LIVE_AUTO_SEND_ENABLED", "0") == "1",
        "auto_send_wallet": ev("HL_LIVE_AUTO_SEND_WALLET", ""),
        "max_per_run": ev("HL_LIVE_AUTO_SEND_MAX_PER_RUN", "1"),
        "marketable_bps": ev("HL_LIVE_AUTO_SEND_MARKETABLE_BPS", "5"),
        "close_adverse_diff_pct": ev("HL_LIVE_MAX_CLOSE_ADVERSE_DIFF_PCT", "0.25"),
        "legacy_notional_cap": ev("HL_LIVE_MAX_MANUAL_ORDER_NOTIONAL_USD", "25"),
    }


def _public_account_address() -> str:
    account = _local_env_value("HL_LIVE_HL_ACCOUNT_ADDRESS") or os.getenv("HL_USER_WALLET", "").strip() or _local_env_value("HL_USER_WALLET")
    account = account.lower().strip()
    if account.startswith("0x") and len(account) == 42:
        return account
    return ""


def _iso_to_ms(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _account_reconciliation_baseline_timestamp(account: str) -> str:
    baselines = load_json(EXCHANGE_BASELINES_JSON, {})
    if not isinstance(baselines, dict):
        return ""
    all_baselines = baselines.get("account_reconciliation")
    if not isinstance(all_baselines, dict):
        return ""
    baseline = all_baselines.get(str(account or "").lower())
    if not isinstance(baseline, dict):
        return ""
    return str(baseline.get("baseline_timestamp") or "")


def _fetch_user_fills_by_time(account: str, start_ms: int, end_ms: int, timeout: float = 8.0) -> Optional[List[Dict[str, Any]]]:
    payload = {
        "type": "userFillsByTime",
        "user": account,
        "startTime": int(start_ms),
        "endTime": int(end_ms),
        "aggregateByTime": False,
    }
    try:
        req = urllib.request.Request(
            "https://api.hyperliquid.xyz/info",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        return raw if isinstance(raw, list) else None
    except Exception:
        return None


def _today_start_ms() -> int:
    try:
        tz = ZoneInfo("Europe/London")
    except Exception:
        tz = timezone.utc
    now_local = datetime.now(tz)
    start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp() * 1000)


def _earliest_real_order_filled_ms() -> int:
    earliest = 0
    if not SEND_ATTEMPTS_CSV.exists():
        return earliest
    try:
        with SEND_ATTEMPTS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if str(row.get("status") or "").upper() != "ORDER_FILLED":
                    continue
                ts = _iso_to_ms(row.get("created_at") or row.get("timestamp") or row.get("time"))
                if ts > 0 and (earliest <= 0 or ts < earliest):
                    earliest = ts
    except Exception:
        return 0
    return earliest


def _summarize_exchange_closed_pnl_rows(rows: List[Dict[str, Any]], start_ms: int, end_ms: int) -> Dict[str, Any]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts = inum(row.get("time") or row.get("timestamp") or row.get("ts"))
        if ts < start_ms or ts > end_ms:
            continue
        key = str(row.get("hash") or row.get("tid") or row.get("oid") or row.get("fill_id") or f"{ts}:{row.get('coin')}:{row.get('px')}:{row.get('sz')}:{row.get('side')}")
        deduped[key] = row
    real_rows = list(deduped.values())
    closed_pnl_sum = round(sum(fnum(row.get("closedPnl") if "closedPnl" in row else row.get("closed_pnl")) for row in real_rows), 8)
    fee_sum = round(sum(fnum(row.get("fee") or row.get("feeUsd") or row.get("builderFee")) for row in real_rows), 8)
    nonzero_count = sum(1 for row in real_rows if abs(fnum(row.get("closedPnl") if "closedPnl" in row else row.get("closed_pnl"))) > 1e-12)
    return {
        "closed_pnl_sum": closed_pnl_sum,
        "fee_sum": fee_sum,
        "fill_count": len(real_rows),
        "nonzero_closed_pnl_count": nonzero_count,
        "rows": sorted(real_rows, key=lambda r: inum(r.get("time") or r.get("timestamp") or r.get("ts"))),
    }


def _fetch_user_realized_pnl_snapshot(account: Optional[str] = None, baseline_timestamp: str = "") -> Dict[str, Any]:
    account = (account or _public_account_address()).lower().strip()
    now_ms = int(time.time() * 1000)
    today_start_ms = _today_start_ms()
    last_24h_start_ms = now_ms - 24 * 60 * 60 * 1000
    seven_day_start_ms = now_ms - 7 * 24 * 60 * 60 * 1000
    first_live_order_ms = _earliest_real_order_filled_ms()
    if not account:
        return {
            "ok": False,
            "status": "UNAVAILABLE",
            "reason": "ACCOUNT_ADDRESS_UNAVAILABLE",
            "realized_pnl_total": None,
            "realized_pnl_today": None,
            "realized_pnl_24h": None,
            "realized_pnl_7d": None,
            "realized_pnl_since_first_live_order": None,
            "realized_pnl_selected": None,
            "realized_pnl_selected_label": "",
            "realized_pnl_since_baseline": None,
            "realized_pnl_total_source": "",
            "realized_pnl_since_baseline_source": "",
            "realized_pnl_window_start": "",
            "realized_pnl_window_end": utc_now_iso(),
            "realized_pnl_fill_count": 0,
        }
    baseline_timestamp = baseline_timestamp or _account_reconciliation_baseline_timestamp(account)
    baseline_ms = _iso_to_ms(baseline_timestamp)
    fetch_candidates = [today_start_ms, last_24h_start_ms, seven_day_start_ms]
    if baseline_ms > 0:
        fetch_candidates.append(baseline_ms)
    if first_live_order_ms > 0:
        fetch_candidates.append(first_live_order_ms)
    fetch_start_ms = min(x for x in fetch_candidates if x > 0)
    rows = _fetch_user_fills_by_time(account, fetch_start_ms, now_ms)
    if rows is None:
        return {
            "ok": False,
            "status": "UNAVAILABLE",
            "reason": "userFillsByTime request failed or returned non-list",
            "account_address": account,
            "realized_pnl_total": None,
            "realized_pnl_total_source": "",
            "realized_pnl_today": None,
            "realized_pnl_today_source": "",
            "realized_pnl_24h": None,
            "realized_pnl_24h_source": "",
            "realized_pnl_7d": None,
            "realized_pnl_7d_source": "",
            "realized_pnl_since_first_live_order": None,
            "realized_pnl_since_first_live_order_source": "",
            "realized_pnl_selected": None,
            "realized_pnl_selected_source": "",
            "realized_pnl_selected_label": "",
            "realized_pnl_selected_start": "",
            "realized_pnl_selected_end": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
            "realized_pnl_selected_fill_count": 0,
            "realized_pnl_since_baseline": None,
            "realized_pnl_since_baseline_source": "",
            "realized_pnl_all_available_window": None,
            "realized_pnl_all_available_window_source": "",
            "realized_pnl_window_start": datetime.fromtimestamp(fetch_start_ms / 1000, tz=timezone.utc).isoformat(),
            "realized_pnl_window_end": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
            "realized_pnl_fill_count": 0,
            "closed_pnl_sum": None,
            "fee_sum": None,
            "fee_policy": "exchange_closedPnl_as_reported",
        }
    today = _summarize_exchange_closed_pnl_rows(rows, today_start_ms, now_ms)
    last_24h = _summarize_exchange_closed_pnl_rows(rows, last_24h_start_ms, now_ms)
    last_7d = _summarize_exchange_closed_pnl_rows(rows, seven_day_start_ms, now_ms)
    since_first_live_order = (
        _summarize_exchange_closed_pnl_rows(rows, first_live_order_ms, now_ms)
        if first_live_order_ms > 0
        else {"closed_pnl_sum": None, "fee_sum": None, "fill_count": 0, "nonzero_closed_pnl_count": 0, "rows": []}
    )
    since_baseline = _summarize_exchange_closed_pnl_rows(rows, baseline_ms, now_ms) if baseline_ms > 0 else {"closed_pnl_sum": 0.0, "fee_sum": 0.0, "fill_count": 0, "nonzero_closed_pnl_count": 0, "rows": []}
    all_available = _summarize_exchange_closed_pnl_rows(rows, fetch_start_ms, now_ms)
    latest_rows = all_available["rows"][-250:] if isinstance(all_available.get("rows"), list) else []
    source = "hyperliquid.info.userFillsByTime.closedPnl"
    selected = since_first_live_order if first_live_order_ms > 0 and since_first_live_order.get("fill_count", 0) else last_24h
    selected_label = "since first live order" if selected is since_first_live_order else "last 24h"
    selected_start_ms = first_live_order_ms if selected is since_first_live_order else last_24h_start_ms
    if not selected.get("fill_count", 0):
        selected = today
        selected_label = "today"
        selected_start_ms = today_start_ms
    return {
        "ok": True,
        "status": "OK",
        "account_address": account,
        "realized_pnl_total": selected["closed_pnl_sum"],
        "realized_pnl_total_source": f"{source}; selected={selected_label}",
        "realized_pnl_today": today["closed_pnl_sum"],
        "realized_pnl_today_source": f"{source}; today Europe/London",
        "realized_pnl_today_fill_count": today["fill_count"],
        "realized_pnl_today_nonzero_count": today["nonzero_closed_pnl_count"],
        "realized_pnl_24h": last_24h["closed_pnl_sum"],
        "realized_pnl_24h_source": f"{source}; last 24h",
        "realized_pnl_24h_fill_count": last_24h["fill_count"],
        "realized_pnl_24h_nonzero_count": last_24h["nonzero_closed_pnl_count"],
        "realized_pnl_7d": last_7d["closed_pnl_sum"],
        "realized_pnl_7d_source": f"{source}; last 7d",
        "realized_pnl_7d_fill_count": last_7d["fill_count"],
        "realized_pnl_7d_nonzero_count": last_7d["nonzero_closed_pnl_count"],
        "realized_pnl_since_first_live_order": since_first_live_order["closed_pnl_sum"],
        "realized_pnl_since_first_live_order_source": f"{source}; start=earliest send_attempts ORDER_FILLED",
        "realized_pnl_since_first_live_order_start": datetime.fromtimestamp(first_live_order_ms / 1000, tz=timezone.utc).isoformat() if first_live_order_ms > 0 else "",
        "realized_pnl_since_first_live_order_fill_count": since_first_live_order["fill_count"],
        "realized_pnl_since_first_live_order_nonzero_count": since_first_live_order["nonzero_closed_pnl_count"],
        "realized_pnl_selected": selected["closed_pnl_sum"],
        "realized_pnl_selected_source": f"{source}; selected={selected_label}",
        "realized_pnl_selected_label": selected_label,
        "realized_pnl_selected_start": datetime.fromtimestamp(selected_start_ms / 1000, tz=timezone.utc).isoformat() if selected_start_ms > 0 else "",
        "realized_pnl_selected_end": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
        "realized_pnl_selected_fill_count": selected["fill_count"],
        "realized_pnl_since_baseline": since_baseline["closed_pnl_sum"],
        "realized_pnl_since_baseline_source": f"{source}; start=account_reconciliation.baseline_timestamp",
        "realized_pnl_since_baseline_fill_count": since_baseline["fill_count"],
        "realized_pnl_all_available_window": all_available["closed_pnl_sum"],
        "realized_pnl_all_available_window_source": f"{source}; fetched 7d/today/baseline window",
        "realized_pnl_all_available_fill_count": all_available["fill_count"],
        "realized_pnl_window_start": datetime.fromtimestamp(today_start_ms / 1000, tz=timezone.utc).isoformat(),
        "realized_pnl_window_end": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
        "realized_pnl_fill_count": today["fill_count"],
        "closed_pnl_sum": today["closed_pnl_sum"],
        "fee_sum": today["fee_sum"],
        "fee_sum_today": today["fee_sum"],
        "fee_sum_24h": last_24h["fee_sum"],
        "fee_sum_7d": last_7d["fee_sum"],
        "fee_sum_since_first_live_order": since_first_live_order["fee_sum"],
        "fee_sum_selected": selected["fee_sum"],
        "fee_sum_since_baseline": since_baseline["fee_sum"],
        "fee_sum_all_available_window": all_available["fee_sum"],
        "fee_policy": "exchange_closedPnl_as_reported",
        "actual_user_fills_recent": latest_rows,
        "actual_user_fills_source": "hyperliquid.info.userFillsByTime",
    }


def _append_exchange_history(snapshot: Dict[str, Any], manual_summary: Dict[str, Any]) -> None:
    if not snapshot.get("ok") or not snapshot.get("available"):
        return
    history = load_json(EXCHANGE_ACCOUNT_HISTORY_FILE, [])
    if not isinstance(history, list):
        history = []
    entry = {
        "timestamp": snapshot.get("updated_at"),
        "account_value": fnum(snapshot.get("account_value")),
        "unified_portfolio_value": fnum(snapshot.get("unified_portfolio_value")) if is_present_num(snapshot.get("unified_portfolio_value")) else None,
        "realized_pnl_since_baseline": fnum(snapshot.get("realized_pnl_since_baseline")) if is_present_num(snapshot.get("realized_pnl_since_baseline")) else None,
        "realized_pnl_today": fnum(snapshot.get("realized_pnl_today")) if is_present_num(snapshot.get("realized_pnl_today")) else None,
        "realized_pnl_selected": fnum(snapshot.get("realized_pnl_selected")) if is_present_num(snapshot.get("realized_pnl_selected")) else None,
        "realized_pnl_selected_label": snapshot.get("realized_pnl_selected_label", ""),
        "realized_pnl_24h": fnum(snapshot.get("realized_pnl_24h")) if is_present_num(snapshot.get("realized_pnl_24h")) else None,
        "realized_pnl_7d": fnum(snapshot.get("realized_pnl_7d")) if is_present_num(snapshot.get("realized_pnl_7d")) else None,
        "realized_pnl_since_first_live_order": fnum(snapshot.get("realized_pnl_since_first_live_order")) if is_present_num(snapshot.get("realized_pnl_since_first_live_order")) else None,
        "withdrawable": fnum(snapshot.get("withdrawable")),
        "manual_live_exposure": fnum(manual_summary.get("manual_live_exposure_estimate")),
        "open_position_count": inum(snapshot.get("open_position_count") or len(snapshot.get("open_positions", []))),
    }
    if history and history[-1].get("timestamp") == entry["timestamp"]:
        return
    history.append(entry)
    if len(history) > 2000:
        history = history[-2000:]
    atomic_write_json(EXCHANGE_ACCOUNT_HISTORY_FILE, history)


def _fetch_exchange_account_snapshot(max_age_sec: float = 15.0) -> Dict[str, Any]:
    cached = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, {})
    now_ms = int(time.time() * 1000)
    if (
        isinstance(cached, dict)
        and cached.get("ok")
        and cached.get("account_value_source")
        and cached.get("raw_total_usd_source")
        and cached.get("spot_account_source")
        and cached.get("realized_pnl_since_baseline_source") is not None
        and cached.get("realized_pnl_selected_source") is not None
        and now_ms - inum(cached.get("fetched_at_ms")) <= int(max_age_sec * 1000)
    ):
        return cached
    account = _public_account_address()
    if not account:
        unavailable = {
            "ok": False, "available": False, "status": "UNAVAILABLE", "reason": "ACCOUNT_ADDRESS_UNAVAILABLE",
            "updated_at": utc_now_iso(), "fetched_at_ms": now_ms,
        }
        try:
            atomic_write_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, unavailable)
        except Exception:
            pass
        return unavailable
    try:
        payload = json.dumps({"type": "clearinghouseState", "user": account}).encode("utf-8")
        req = urllib.request.Request(
            "https://api.hyperliquid.xyz/info",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        spot_raw: Dict[str, Any] = {}
        spot_parse_status = "NOT_FETCHED"
        try:
            spot_payload = json.dumps({"type": "spotClearinghouseState", "user": account}).encode("utf-8")
            spot_req = urllib.request.Request(
                "https://api.hyperliquid.xyz/info",
                data=spot_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(spot_req, timeout=5) as spot_resp:
                spot_data = json.loads(spot_resp.read().decode("utf-8"))
            spot_raw = spot_data if isinstance(spot_data, dict) else {}
            spot_parse_status = "OK"
        except Exception as spot_exc:
            spot_parse_status = f"UNAVAILABLE: {type(spot_exc).__name__}: {spot_exc}"
        margin = raw.get("marginSummary") if isinstance(raw, dict) else {}
        cross = raw.get("crossMarginSummary") if isinstance(raw, dict) else {}
        withdrawable = raw.get("withdrawable") if isinstance(raw, dict) else ""
        margin_raw_total_usd = fnum(margin.get("totalRawUsd")) if isinstance(margin, dict) and "totalRawUsd" in margin else None
        cross_raw_total_usd = fnum(cross.get("totalRawUsd")) if isinstance(cross, dict) and "totalRawUsd" in cross else None
        raw_total_usd = margin_raw_total_usd if margin_raw_total_usd is not None else cross_raw_total_usd
        raw_total_usd_source = (
            "clearinghouseState.marginSummary.totalRawUsd"
            if margin_raw_total_usd is not None
            else "clearinghouseState.crossMarginSummary.totalRawUsd"
            if cross_raw_total_usd is not None
            else ""
        )
        spot_balances = spot_raw.get("balances", []) if isinstance(spot_raw.get("balances"), list) else []
        raw_spot_balances = spot_balances
        usdc_balance = next((b for b in spot_balances if isinstance(b, dict) and str(b.get("coin", "")).upper() == "USDC"), None)
        unified_portfolio_value: Optional[float] = None
        unified_portfolio_value_source = ""
        available_to_trade: Optional[float] = None
        available_to_trade_source = ""
        available_candidates: Dict[str, Any] = {}
        if isinstance(usdc_balance, dict) and is_present_num(usdc_balance.get("total")):
            unified_portfolio_value = fnum(usdc_balance.get("total"))
            unified_portfolio_value_source = "spotClearinghouseState.balances[coin=USDC].total"
            spot_parse_status = "OK: unified portfolio value matched spot USDC total"
            if is_present_num(usdc_balance.get("hold")):
                available_candidates["spotClearinghouseState.balances[coin=USDC].total_minus_hold"] = round(fnum(usdc_balance.get("total")) - fnum(usdc_balance.get("hold")), 8)
        token_available = spot_raw.get("tokenToAvailableAfterMaintenance", [])
        if isinstance(token_available, list):
            for item in token_available:
                if isinstance(item, list) and len(item) >= 2:
                    candidate_key = f"spotClearinghouseState.tokenToAvailableAfterMaintenance[token={item[0]}]"
                    available_candidates[candidate_key] = item[1]
                    if str(item[0]) == "0":
                        spot_parse_status += "; available-to-trade candidate present but not promoted without frontend match"
        positions = []
        position_map: Dict[str, Dict[str, Any]] = {}
        unrealized = 0.0
        for item in raw.get("assetPositions", []) if isinstance(raw, dict) else []:
            pos = item.get("position", item) if isinstance(item, dict) else {}
            if not isinstance(pos, dict):
                continue
            size = fnum(pos.get("szi"))
            if abs(size) <= 1e-12:
                continue
            upnl = fnum(pos.get("unrealizedPnl"))
            unrealized += upnl
            mark_px = fnum(pos.get("positionValue")) / abs(size) if abs(size) > 1e-12 else 0.0
            item_out = {
                "coin": str(pos.get("coin", "")).upper(),
                "signed_size": size,
                "position_value": fnum(pos.get("positionValue")),
                "entry_px": fnum(pos.get("entryPx")),
                "mark_px": mark_px,
                "unrealized_pnl": upnl,
                "margin_used": fnum(pos.get("marginUsed")),
            }
            positions.append(item_out)
            position_map[item_out["coin"]] = item_out
        realized_snapshot = _fetch_user_realized_pnl_snapshot(account)
        snapshot = {
            "ok": True,
            "available": True,
            "status": "OK",
            "account_address": account,
            "updated_at": utc_now_iso(),
            "fetched_at_ms": now_ms,
            "account_value": fnum(margin.get("accountValue"), fnum(cross.get("accountValue"))),
            "account_value_label": "Clearinghouse account value",
            "account_value_source": "clearinghouseState.marginSummary.accountValue",
            "total_account_value": None,
            "total_account_value_status": "unavailable from clearinghouseState",
            "raw_total_usd": raw_total_usd,
            "raw_total_usd_source": raw_total_usd_source,
            "raw_total_usd_label": "Raw USD / collateral value",
            "margin_raw_total_usd": margin_raw_total_usd,
            "cross_raw_total_usd": cross_raw_total_usd,
            "unified_portfolio_value": unified_portfolio_value,
            "unified_portfolio_value_source": unified_portfolio_value_source,
            "available_to_trade": available_to_trade,
            "available_to_trade_source": available_to_trade_source,
            "available_to_trade_candidates": available_candidates,
            "raw_spot_balances": raw_spot_balances,
            "spot_account_source": "spotClearinghouseState",
            "spot_parse_status": spot_parse_status,
            "withdrawable": fnum(withdrawable),
            "withdrawable_source": "clearinghouseState.withdrawable",
            "margin_used": fnum(margin.get("totalMarginUsed"), fnum(cross.get("totalMarginUsed"))),
            "margin_used_source": "clearinghouseState.marginSummary.totalMarginUsed",
            "total_notional_position": fnum(margin.get("totalNtlPos"), fnum(cross.get("totalNtlPos"))),
            "unrealized_pnl": unrealized,
            "unrealized_pnl_source": "sum(assetPositions.position.unrealizedPnl)",
            "realized_pnl_total": realized_snapshot.get("realized_pnl_total"),
            "realized_pnl_total_source": realized_snapshot.get("realized_pnl_total_source", ""),
            "realized_pnl_today": realized_snapshot.get("realized_pnl_today"),
            "realized_pnl_today_source": realized_snapshot.get("realized_pnl_today_source", ""),
            "realized_pnl_today_fill_count": realized_snapshot.get("realized_pnl_today_fill_count", 0),
            "realized_pnl_today_nonzero_count": realized_snapshot.get("realized_pnl_today_nonzero_count", 0),
            "realized_pnl_24h": realized_snapshot.get("realized_pnl_24h"),
            "realized_pnl_24h_source": realized_snapshot.get("realized_pnl_24h_source", ""),
            "realized_pnl_24h_fill_count": realized_snapshot.get("realized_pnl_24h_fill_count", 0),
            "realized_pnl_24h_nonzero_count": realized_snapshot.get("realized_pnl_24h_nonzero_count", 0),
            "realized_pnl_7d": realized_snapshot.get("realized_pnl_7d"),
            "realized_pnl_7d_source": realized_snapshot.get("realized_pnl_7d_source", ""),
            "realized_pnl_7d_fill_count": realized_snapshot.get("realized_pnl_7d_fill_count", 0),
            "realized_pnl_7d_nonzero_count": realized_snapshot.get("realized_pnl_7d_nonzero_count", 0),
            "realized_pnl_since_first_live_order": realized_snapshot.get("realized_pnl_since_first_live_order"),
            "realized_pnl_since_first_live_order_source": realized_snapshot.get("realized_pnl_since_first_live_order_source", ""),
            "realized_pnl_since_first_live_order_start": realized_snapshot.get("realized_pnl_since_first_live_order_start", ""),
            "realized_pnl_since_first_live_order_fill_count": realized_snapshot.get("realized_pnl_since_first_live_order_fill_count", 0),
            "realized_pnl_since_first_live_order_nonzero_count": realized_snapshot.get("realized_pnl_since_first_live_order_nonzero_count", 0),
            "realized_pnl_selected": realized_snapshot.get("realized_pnl_selected"),
            "realized_pnl_selected_source": realized_snapshot.get("realized_pnl_selected_source", ""),
            "realized_pnl_selected_label": realized_snapshot.get("realized_pnl_selected_label", ""),
            "realized_pnl_selected_start": realized_snapshot.get("realized_pnl_selected_start", ""),
            "realized_pnl_selected_end": realized_snapshot.get("realized_pnl_selected_end", ""),
            "realized_pnl_selected_fill_count": realized_snapshot.get("realized_pnl_selected_fill_count", 0),
            "realized_pnl_since_baseline": realized_snapshot.get("realized_pnl_since_baseline"),
            "realized_pnl_since_baseline_source": realized_snapshot.get("realized_pnl_since_baseline_source", ""),
            "realized_pnl_since_baseline_fill_count": realized_snapshot.get("realized_pnl_since_baseline_fill_count", 0),
            "realized_pnl_all_available_window": realized_snapshot.get("realized_pnl_all_available_window"),
            "realized_pnl_all_available_window_source": realized_snapshot.get("realized_pnl_all_available_window_source", ""),
            "realized_pnl_all_available_fill_count": realized_snapshot.get("realized_pnl_all_available_fill_count", 0),
            "realized_pnl_window_start": realized_snapshot.get("realized_pnl_window_start", ""),
            "realized_pnl_window_end": realized_snapshot.get("realized_pnl_window_end", ""),
            "realized_pnl_fill_count": realized_snapshot.get("realized_pnl_fill_count", 0),
            "closed_pnl_sum": realized_snapshot.get("closed_pnl_sum"),
            "fee_sum": realized_snapshot.get("fee_sum"),
            "fee_sum_today": realized_snapshot.get("fee_sum_today"),
            "fee_sum_24h": realized_snapshot.get("fee_sum_24h"),
            "fee_sum_7d": realized_snapshot.get("fee_sum_7d"),
            "fee_sum_since_first_live_order": realized_snapshot.get("fee_sum_since_first_live_order"),
            "fee_sum_selected": realized_snapshot.get("fee_sum_selected"),
            "fee_sum_since_baseline": realized_snapshot.get("fee_sum_since_baseline"),
            "fee_sum_all_available_window": realized_snapshot.get("fee_sum_all_available_window"),
            "fee_policy": realized_snapshot.get("fee_policy", "exchange_closedPnl_as_reported"),
            "realized_pnl_snapshot_status": realized_snapshot.get("status", ""),
            "actual_user_fills_recent": realized_snapshot.get("actual_user_fills_recent", []),
            "actual_user_fills_source": realized_snapshot.get("actual_user_fills_source", ""),
            "open_positions": positions,
            "open_positions_count": len(positions),
            "open_position_count": len(positions),
            "positions_by_coin": position_map,
            "raw_margin_summary": margin if isinstance(margin, dict) else {},
            "raw_cross_margin_summary": cross if isinstance(cross, dict) else {},
            "raw_withdrawable": withdrawable,
            "source": "hyperliquid_clearinghouse_state",
        }
        atomic_write_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, snapshot)
        return snapshot
    except Exception as exc:
        fallback = cached if isinstance(cached, dict) else {}
        return {
            **fallback,
            "ok": False,
            "available": False,
            "status": "UNAVAILABLE",
            "reason": f"{type(exc).__name__}: {exc}",
            "updated_at": utc_now_iso(),
            "fetched_at_ms": now_ms,
        }


def _build_account_reconciliation(exchange_snapshot: Dict[str, Any], live_leader_performance: Dict[str, Any]) -> Dict[str, Any]:
    raw_total_usd = exchange_snapshot.get("raw_total_usd")
    has_raw_total = is_present_num(raw_total_usd)
    unified_value = exchange_snapshot.get("unified_portfolio_value")
    has_unified = is_present_num(unified_value)
    baselines = load_json(EXCHANGE_BASELINES_JSON, {})
    if not isinstance(baselines, dict):
        baselines = {}
    acct = str(exchange_snapshot.get("account_address") or _public_account_address() or "").lower()
    all_baselines = baselines.get("account_reconciliation")
    if not isinstance(all_baselines, dict):
        all_baselines = {}
    baseline = all_baselines.get(acct) if acct else None
    if not isinstance(baseline, dict):
        baseline = None
    if baseline is None and acct and (has_unified or has_raw_total):
        baseline = {
            "account_address": acct,
            "baseline_timestamp": exchange_snapshot.get("updated_at") or utc_now_iso(),
            "baseline_initialized_after_bug_discovery": True,
            "baseline_note": "Initialized from current account snapshot; deltas start from this point.",
        }
        if has_unified:
            baseline["baseline_unified_portfolio_value"] = fnum(unified_value)
            baseline["baseline_source"] = exchange_snapshot.get("unified_portfolio_value_source") or ""
        if has_raw_total:
            baseline["baseline_raw_total_usd"] = fnum(raw_total_usd)
            baseline.setdefault("baseline_raw_total_usd_source", exchange_snapshot.get("raw_total_usd_source") or "")
        all_baselines[acct] = baseline
        baselines.setdefault("schema", "exchange_baselines.v1")
        baselines.setdefault("important_boundary", "Baseline offsets are not trades, not PnL, and not engine positions.")
        baselines["account_reconciliation"] = all_baselines
        try:
            atomic_write_json(EXCHANGE_BASELINES_JSON, baselines)
        except Exception:
            pass
    elif isinstance(baseline, dict) and acct and has_unified and not is_present_num(baseline.get("baseline_unified_portfolio_value")):
        baseline["baseline_unified_portfolio_value"] = fnum(unified_value)
        baseline["baseline_source"] = exchange_snapshot.get("unified_portfolio_value_source") or ""
        baseline["baseline_initialized_after_bug_discovery"] = True
        baseline["baseline_note"] = "Unified portfolio baseline initialized from current account snapshot; deltas start from this point."
        baseline.setdefault("baseline_timestamp", exchange_snapshot.get("updated_at") or utc_now_iso())
        all_baselines[acct] = baseline
        baselines.setdefault("schema", "exchange_baselines.v1")
        baselines.setdefault("important_boundary", "Baseline offsets are not trades, not PnL, and not engine positions.")
        baselines["account_reconciliation"] = all_baselines
        try:
            atomic_write_json(EXCHANGE_BASELINES_JSON, baselines)
        except Exception:
            pass

    baseline_raw = fnum(baseline.get("baseline_raw_total_usd")) if isinstance(baseline, dict) and is_present_num(baseline.get("baseline_raw_total_usd")) else None
    net_raw_change = round(fnum(raw_total_usd) - baseline_raw, 6) if has_raw_total and baseline_raw is not None else None
    baseline_unified = fnum(baseline.get("baseline_unified_portfolio_value")) if isinstance(baseline, dict) and is_present_num(baseline.get("baseline_unified_portfolio_value")) else None
    net_account_change = round(fnum(unified_value) - baseline_unified, 6) if has_unified and baseline_unified is not None else net_raw_change
    account_value_source = exchange_snapshot.get("unified_portfolio_value_source") if has_unified else exchange_snapshot.get("raw_total_usd_source") or ""
    attributed_open_pnl = round(sum(fnum(p.get("live_net_pnl")) for p in live_leader_performance.values() if isinstance(p, dict) and is_present_num(p.get("live_net_pnl"))), 6)
    return {
        "account_address": acct,
        "baseline_unified_portfolio_value": baseline_unified,
        "baseline_raw_total_usd": baseline_raw,
        "baseline_timestamp": baseline.get("baseline_timestamp", "") if isinstance(baseline, dict) else "",
        "baseline_initialized_after_bug_discovery": bool(baseline.get("baseline_initialized_after_bug_discovery")) if isinstance(baseline, dict) else False,
        "baseline_note": baseline.get("baseline_note", "") if isinstance(baseline, dict) else "",
        "current_account_value": fnum(unified_value) if has_unified else (fnum(raw_total_usd) if has_raw_total else None),
        "current_account_value_source": account_value_source,
        "current_unified_portfolio_value": fnum(unified_value) if has_unified else None,
        "current_raw_total_usd": fnum(raw_total_usd) if has_raw_total else None,
        "raw_total_usd_source": exchange_snapshot.get("raw_total_usd_source") or "",
        "net_account_value_change_unadjusted": net_account_change,
        "net_raw_usd_change_unadjusted": net_raw_change,
        "clearinghouse_account_value": fnum(exchange_snapshot.get("account_value")) if exchange_snapshot.get("available") else None,
        "exchange_unrealized_pnl": fnum(exchange_snapshot.get("unrealized_pnl")) if exchange_snapshot.get("available") else None,
        "exchange_realized_pnl_since_baseline": fnum(exchange_snapshot.get("realized_pnl_since_baseline")) if is_present_num(exchange_snapshot.get("realized_pnl_since_baseline")) else None,
        "exchange_realized_pnl_today": fnum(exchange_snapshot.get("realized_pnl_today")) if is_present_num(exchange_snapshot.get("realized_pnl_today")) else None,
        "exchange_realized_pnl_all_available_window": fnum(exchange_snapshot.get("realized_pnl_all_available_window")) if is_present_num(exchange_snapshot.get("realized_pnl_all_available_window")) else None,
        "exchange_realized_pnl_source": exchange_snapshot.get("realized_pnl_since_baseline_source", ""),
        "exchange_realized_pnl_fill_count": inum(exchange_snapshot.get("realized_pnl_fill_count")),
        "exchange_realized_pnl_window_start": exchange_snapshot.get("realized_pnl_window_start", ""),
        "exchange_realized_pnl_window_end": exchange_snapshot.get("realized_pnl_window_end", ""),
        "open_notional": fnum(exchange_snapshot.get("total_notional_position")) if exchange_snapshot.get("available") else None,
        "margin_used": fnum(exchange_snapshot.get("margin_used")) if exchange_snapshot.get("available") else None,
        "attributed_open_pnl": attributed_open_pnl,
        "estimated_copy_pnl": attributed_open_pnl,
        "reconciliation_gap": round(net_account_change - attributed_open_pnl, 6) if net_account_change is not None else None,
        "updated_at": exchange_snapshot.get("updated_at") or utc_now_iso(),
    }


def _manual_live_summary(manual_positions: Dict[str, Any], recent_send_attempts: List[Dict[str, Any]], exchange_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    price_by_coin: Dict[str, float] = {}
    exchange_snapshot = exchange_snapshot if isinstance(exchange_snapshot, dict) else {}
    exchange_positions = exchange_snapshot.get("positions_by_coin", {}) if isinstance(exchange_snapshot.get("positions_by_coin"), dict) else {}
    for coin, pos in exchange_positions.items():
        if isinstance(pos, dict) and fnum(pos.get("mark_px")) > 0:
            price_by_coin[str(coin).upper()] = fnum(pos.get("mark_px"))
    last_filled: Dict[str, Any] = {}
    last_rejected: Dict[str, Any] = {}
    last_local_block: Dict[str, Any] = {}
    last_auto_fill_at = ""
    auto_wallet = ""
    auto_wallets: List[str] = []
    for row in recent_send_attempts:
        coin = str(row.get("coin", "")).upper()
        px = fnum(row.get("fill_avg_px"))
        if coin and px > 0 and coin not in price_by_coin:
            price_by_coin[coin] = px
        status = str(row.get("status", ""))
        if status == "ORDER_FILLED":
            last_filled = row
            if "AUTO_LIVE_WS_FAST_PATH" in str(row.get("notes", "")) or row.get("auto_live"):
                last_auto_fill_at = str(row.get("created_at", ""))
                w = str(row.get("leader_wallet") or row.get("auto_send_wallet") or "").lower()
                auto_wallet = w
                if w and w not in auto_wallets:
                    auto_wallets.append(w)
        elif status in _LOCAL_NOOP_STATUSES:
            last_local_block = row
        elif status and status not in {"CONFIRM_REQUIRED"}:
            last_rejected = row
    signed_by_coin: Dict[str, float] = {}
    exposure_by_coin: Dict[str, float] = {}
    for _wallet, coin, pos in iter_manual_wallet_positions(manual_positions):
        signed = fnum(pos.get("signed_size"))
        coin_upper = str(coin).upper()
        signed_by_coin[coin_upper] = signed_by_coin.get(coin_upper, 0.0) + signed
        exposure_by_coin[coin_upper] = exposure_by_coin.get(coin_upper, 0.0) + abs(signed) * fnum(price_by_coin.get(coin_upper))
    open_count = sum(1 for v in signed_by_coin.values() if abs(v) > 1e-12)
    return {
        "open_manual_position_count": open_count,
        "signed_size_by_coin": signed_by_coin,
        "exposure_by_coin": exposure_by_coin,
        "manual_live_exposure_estimate": sum(exposure_by_coin.values()),
        "last_filled_manual_order": last_filled,
        "last_rejected_manual_order": last_rejected,
        "last_local_block_manual_order": last_local_block,
        "current_auto_live_wallet": auto_wallet,
        "auto_live_wallets": auto_wallets,
        "last_auto_live_fill_at": last_auto_fill_at,
    }


def _build_manual_reconciliation_rows(manual_positions: Dict[str, Any], exchange_snapshot: Dict[str, Any], recent_send_attempts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    exchange_positions = exchange_snapshot.get("positions_by_coin", {}) if isinstance(exchange_snapshot.get("positions_by_coin"), dict) else {}
    exchange_available = bool(exchange_snapshot.get("available"))
    seen_on_exchange: set[str] = set()
    sleeves = _manual_position_sleeves(manual_positions)
    coin_counts, coin_nets = _shared_manual_coin_nets(sleeves)
    shared_coins = {coin for coin, count in coin_counts.items() if count > 1}
    for wallet, coin_upper, pos in sorted(sleeves, key=lambda item: (item[1], item[0])):
        signed = fnum(pos.get("signed_size"))
        if abs(signed) <= 1e-12:
            continue
        ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
        exchange_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
        if ex:
            seen_on_exchange.add(coin_upper)
        if coin_upper in shared_coins:
            rows.append({
                "severity": "OK" if exchange_available else "INFO",
                "wallet": wallet,
                "coin": coin_upper,
                "side": "LONG" if signed > 0 else "SHORT",
                "issue": "SHARED_SYMBOL_SLEEVE_TRACKED" if exchange_available else "EXCHANGE_UNAVAILABLE",
                "manual_signed_size": signed,
                "exchange_signed_size": exchange_signed if exchange_available else "n/a",
                "last_intent_id": pos.get("last_intent_id", ""),
                "last_oid": pos.get("last_oid", ""),
                "last_updated_at": pos.get("last_updated_at", ""),
                "latest_error": "shared symbol: sleeve tracked; exchange is netted at account level",
                "count": 1,
                "action_available": False,
            })
            continue
        if exchange_available:
            diff = signed - exchange_signed
            if abs(diff) <= 1e-8:
                severity = "OK"
                issue = "MATCH"
            elif exchange_signed == 0:
                severity = "CRITICAL"
                issue = "MISSING_EXCHANGE"
            else:
                severity = "CRITICAL"
                issue = f"SIZE_DIFF: {diff:+.6f}"
            exchange_label = exchange_signed
        else:
            severity = "INFO"
            issue = "EXCHANGE_UNAVAILABLE"
            exchange_label = "n/a"
        rows.append({
            "severity": severity,
            "wallet": wallet,
            "coin": coin_upper,
            "side": "LONG" if signed > 0 else "SHORT",
            "issue": issue,
            "manual_signed_size": signed,
            "exchange_signed_size": exchange_label,
            "last_intent_id": pos.get("last_intent_id", ""),
            "last_oid": pos.get("last_oid", ""),
            "last_updated_at": pos.get("last_updated_at", ""),
            "count": 1,
            "action_available": bool(issue == "MISSING_EXCHANGE" and abs(exchange_signed) <= 1e-12),
            "action_label": "Archive ledger row" if issue == "MISSING_EXCHANGE" and abs(exchange_signed) <= 1e-12 else "",
            "action_note": "Ledger cleanup only; does not place an exchange order." if issue == "MISSING_EXCHANGE" and abs(exchange_signed) <= 1e-12 else "",
        })
    if exchange_available:
        for coin_upper in sorted(shared_coins):
            ledger_net = coin_nets.get(coin_upper, 0.0)
            ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
            exchange_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
            diff = ledger_net - exchange_signed
            if ex:
                seen_on_exchange.add(coin_upper)
            if abs(diff) <= 1e-8:
                severity = "OK"
                issue = "SHARED_SYMBOL_NET_MATCH"
            elif exchange_signed == 0.0:
                severity = "CRITICAL"
                issue = "MISSING_EXCHANGE"
            else:
                severity = "CRITICAL"
                issue = f"SHARED_SYMBOL_NET_DIFF: {diff:+.6f}"
            rows.append({
                "severity": severity,
                "wallet": "aggregate",
                "coin": coin_upper,
                "side": "NET",
                "issue": issue,
                "manual_signed_size": round(ledger_net, 12),
                "exchange_signed_size": exchange_signed,
                "last_intent_id": "aggregate",
                "last_oid": "aggregate",
                "last_updated_at": "—",
                "latest_error": "aggregate net matches exchange" if severity == "OK" else "aggregate ledger net differs from exchange net",
                "count": coin_counts.get(coin_upper, 0),
                "action_available": False,
            })
    if exchange_available:
        for coin, ex in sorted(exchange_positions.items()):
            if coin not in seen_on_exchange:
                rows.append({
                    "severity": "CRITICAL",
                    "wallet": "unknown",
                    "coin": str(coin).upper(),
                    "issue": "MISSING_LEDGER",
                    "manual_signed_size": 0.0,
                    "exchange_signed_size": fnum(ex.get("signed_size")),
                    "last_intent_id": "—",
                    "last_oid": "—",
                    "last_updated_at": "—",
                    "count": 1,
                    "action_available": False,
                })
    return rows[:50]


def _build_recent_send_warning_groups(recent_send_attempts: List[Dict[str, Any]], limit: int = 50) -> List[Dict[str, Any]]:
    grouped_warnings: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for attempt in recent_send_attempts[-50:]:
        status = str(attempt.get("status", ""))
        if status and status not in {"ORDER_FILLED", "CONFIRM_REQUIRED"}:
            wallet = str(attempt.get("leader_wallet") or attempt.get("auto_send_wallet") or "")
            coin = str(attempt.get("coin", "")).upper()
            error = str(attempt.get("error") or "")
            key = (wallet.lower(), coin, status, error)
            grouped = grouped_warnings.get(key)
            if not grouped:
                grouped = {
                    "severity": "WARN",
                    "wallet": wallet,
                    "coin": coin,
                    "issue": f"recent send attempt {status}" + (f": {error}" if error else ""),
                    "status": status,
                    "error": error,
                    "manual_signed_size": "n/a",
                    "exchange_signed_size": "n/a",
                    "last_intent_id": "",
                    "last_oid": "",
                    "last_updated_at": "",
                    "latest_error": "",
                    "count": 0,
                    "action_available": False,
                }
                grouped_warnings[key] = grouped
            grouped["count"] = inum(grouped.get("count")) + 1
            grouped["last_intent_id"] = attempt.get("intent_id", "") or grouped.get("last_intent_id", "")
            grouped["last_oid"] = attempt.get("oid", "") or grouped.get("last_oid", "")
            grouped["last_updated_at"] = attempt.get("created_at", "") or grouped.get("last_updated_at", "")
            grouped["latest_error"] = error or grouped.get("latest_error", "")
    return sorted(
        grouped_warnings.values(),
        key=lambda r: str(r.get("last_updated_at", "")),
        reverse=True,
    )[:limit]


def _archive_manual_reconciliation_ledger_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    coin_req = str(payload.get("coin", "")).upper().strip()
    issue_req = str(payload.get("issue", "")).upper().strip()
    wallet_req = str(payload.get("wallet", "")).lower().strip()
    manual_req_raw = payload.get("manual_signed_size")
    if not coin_req or issue_req != "MISSING_EXCHANGE" or not is_present_num(manual_req_raw):
        return {"ok": False, "error": "BAD_REQUEST"}

    manual_positions = _load_manual_live_positions()
    matched_key = next((k for k in manual_positions.keys() if str(k).upper() == coin_req), None)
    if matched_key is None:
        return {"ok": False, "error": "MANUAL_LEDGER_ROW_NOT_FOUND"}
    old_row = manual_positions.get(matched_key)
    if not isinstance(old_row, dict):
        return {"ok": False, "error": "BAD_MANUAL_LEDGER_ROW"}

    old_signed = fnum(old_row.get("signed_size"))
    if abs(old_signed - fnum(manual_req_raw)) > 1e-12:
        return {"ok": False, "error": "MANUAL_SIGNED_SIZE_MISMATCH"}
    old_wallet = str(old_row.get("leader_wallet") or old_row.get("wallet") or "").lower().strip()
    if wallet_req and old_wallet and wallet_req != old_wallet:
        return {"ok": False, "error": "WALLET_MISMATCH"}
    if abs(old_signed) <= 1e-12:
        return {"ok": False, "error": "MANUAL_LEDGER_ROW_ALREADY_ZERO"}

    # Force a live fetch so archive decisions are never based on stale cached state.
    cached_snapshot = _fetch_exchange_account_snapshot(max_age_sec=0)
    if not isinstance(cached_snapshot, dict) or not cached_snapshot.get("ok") or not cached_snapshot.get("available"):
        return {"ok": False, "error": "EXCHANGE_SNAPSHOT_UNAVAILABLE",
                "status": cached_snapshot.get("status", "UNAVAILABLE") if isinstance(cached_snapshot, dict) else "UNAVAILABLE"}
    exchange_positions = cached_snapshot.get("positions_by_coin", {}) if isinstance(cached_snapshot.get("positions_by_coin"), dict) else {}
    ex = exchange_positions.get(coin_req, {})
    exchange_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
    if abs(exchange_signed) > 1e-12:
        return {"ok": False, "error": "EXCHANGE_SIZE_NOT_ZERO", "exchange_signed_size": exchange_signed}

    if not MANUAL_POSITIONS_FILE.exists():
        return {"ok": False, "error": "MANUAL_POSITIONS_FILE_MISSING"}
    MANUAL_RECON_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = MANUAL_RECON_BACKUP_DIR / f"manual_live_positions_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    shutil.copy2(MANUAL_POSITIONS_FILE, backup_path)

    removed = manual_positions.pop(matched_key)
    atomic_write_json(MANUAL_POSITIONS_FILE, manual_positions)

    actions = load_json(MANUAL_RECON_ACTIONS_FILE, [])
    if not isinstance(actions, list):
        actions = []
    action_record = {
        "timestamp": utc_now_iso(),
        "action": "ARCHIVE_MANUAL_LEDGER_ROW",
        "coin": coin_req,
        "wallet": old_wallet or wallet_req,
        "old_row": removed,
        "issue": "MISSING_EXCHANGE",
        "manual_signed_size": old_signed,
        "exchange_signed_size": exchange_signed,
        "backup_path": str(backup_path),
    }
    actions.append(action_record)
    atomic_write_json(MANUAL_RECON_ACTIONS_FILE, actions)
    return {"ok": True, "archived": action_record}


def _build_live_wallet_derived(model_state: Dict[str, Any], live_config: Dict[str, Any], last_rows: List[Dict[str, Any]], recent_send_attempts: List[Dict[str, Any]], manual_positions: Dict[str, Any], manual_summary: Dict[str, Any]) -> Dict[str, Any]:
    model_rows = {
        str(row.get("wallet", "")).lower(): row
        for row in model_state.get("wallet_rows", []) if isinstance(row, dict)
    } if isinstance(model_state, dict) else {}
    wallets = live_config.get("wallets", {}) if isinstance(live_config.get("wallets"), dict) else {}
    derived: Dict[str, Any] = {}
    for wallet in wallets:
        w = str(wallet).lower()
        model = model_rows.get(w, {})
        lead = model.get("lead", {}) if isinstance(model.get("lead"), dict) else {}
        copy = model.get("copy", {}) if isinstance(model.get("copy"), dict) else {}
        delta = model.get("delta", {}) if isinstance(model.get("delta"), dict) else {}
        derived[w] = {
            "wallet": w,
            "lead_equity": lead.get("equity"),
            "copy_equity": copy.get("equity"),
            "lead_realized": lead.get("realized"),
            "copy_realized": copy.get("realized"),
            "lead_unrealized": lead.get("unrealized"),
            "copy_unrealized": copy.get("unrealized"),
            "lead_drawdown": lead.get("drawdown"),
            "copy_drawdown": copy.get("drawdown"),
            "lead_max_drawdown": lead.get("max_drawdown"),
            "copy_max_drawdown": copy.get("max_drawdown"),
            "real_diff": delta.get("equity", model.get("delta_equity")),
            "ws_intent_count": 0,
            "lead_activity_count": inum(model.get("fill_count")),
            "copy_activity_count": inum(model.get("fill_count")),
            "last_ws_time": "",
            "last_coin": "",
            "last_side": "",
            "auto_filled_count": 0,
            "auto_rejected_count": 0,
            "manual_open_exposure": 0.0,
        }
    for row in last_rows:
        w = str(row.get("leader_wallet") or row.get("wallet") or "").lower()
        if w not in derived:
            continue
        if str(row.get("reason", "")) == "LIVE_WS_DETECTED":
            derived[w]["ws_intent_count"] += 1
            derived[w]["last_ws_time"] = row.get("created_at", derived[w].get("last_ws_time", ""))
            derived[w]["last_coin"] = row.get("coin", "")
            derived[w]["last_side"] = row.get("side", "")
            derived[w]["copy_activity_count"] += 1
    for attempt in recent_send_attempts:
        w = str(attempt.get("leader_wallet") or attempt.get("auto_send_wallet") or "").lower()
        if w not in derived:
            continue
        if "AUTO_LIVE_WS_FAST_PATH" not in str(attempt.get("notes", "")) and not attempt.get("auto_live"):
            continue
        if str(attempt.get("status", "")) == "ORDER_FILLED":
            derived[w]["auto_filled_count"] += 1
        elif str(attempt.get("status", "")):
            derived[w]["auto_rejected_count"] += 1
    exposure_by_coin = manual_summary.get("exposure_by_coin", {}) if isinstance(manual_summary.get("exposure_by_coin"), dict) else {}
    for w, coin, pos in iter_manual_wallet_positions(manual_positions):
        if abs(fnum(pos.get("signed_size"))) <= 1e-12:
            continue
        if w in derived:
            derived[w]["manual_open_exposure"] += fnum(exposure_by_coin.get(str(coin).upper()))
    return derived


def _build_live_wallet_rows(
    live_config: Dict[str, Any],
    last_rows: List[Dict[str, Any]],
    recent_send_attempts: List[Dict[str, Any]],
    manual_positions: Dict[str, Any],
    manual_summary: Dict[str, Any],
    ws_health: Dict[str, Any],
    live_leader_performance: Optional[Dict[str, Any]] = None,
    live_fills: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    wallets = live_config.get("wallets", {})
    if not isinstance(wallets, dict):
        wallets = {}
    health_wallets = ws_health.get("wallets", {}) if isinstance(ws_health.get("wallets"), dict) else {}
    exposure_by_coin = manual_summary.get("exposure_by_coin", {}) if isinstance(manual_summary.get("exposure_by_coin"), dict) else {}
    # Detect shared HOT10 WS health from ws_summary (per-wallet rows absent under shared socket)
    _ws_summary = ws_health.get("ws_summary", {}) if isinstance(ws_health.get("ws_summary"), dict) else {}
    _shared_ws_ok = (
        str(_ws_summary.get("ws_status", "")).upper() == "WS_OK"
        and bool(_ws_summary.get("socket_open"))
        and bool(_ws_summary.get("thread_alive"))
    )
    # Pre-build per-wallet manual ledger aggregates from by_wallet sleeves
    _by_wallet_pos: Dict[str, Any] = {}
    if isinstance(manual_positions, dict):
        _bwp = manual_positions.get("by_wallet", {})
        if isinstance(_bwp, dict):
            _by_wallet_pos = _bwp
    # Pre-build most recent live fill per leader wallet (for last_fill field)
    if live_fills is None:
        live_fills = _load_recent_live_fills(500)
    _last_lf_by_wallet: Dict[str, Dict[str, Any]] = {}
    for _lf in (live_fills or []):
        _lfw = str(_lf.get("leader_wallet") or "").lower()
        if _lfw:
            _ex_lf = _last_lf_by_wallet.get(_lfw)
            if _ex_lf is None or str(_lf.get("created_at", "")) > str(_ex_lf.get("created_at", "")):
                _last_lf_by_wallet[_lfw] = _lf
    # Pre-count ORDER_FILLED send_attempts per wallet
    _filled_cnt_by_wallet: Dict[str, int] = {}
    for _sa in recent_send_attempts:
        _saw = str(_sa.get("leader_wallet") or _sa.get("auto_send_wallet") or "").lower()
        if _saw and str(_sa.get("status", "")) == "ORDER_FILLED":
            _filled_cnt_by_wallet[_saw] = _filled_cnt_by_wallet.get(_saw, 0) + 1
    rows: List[Dict[str, Any]] = []
    for wallet, cfg in sorted(wallets.items()):
        if not isinstance(cfg, dict):
            cfg = {}
        w = wallet.lower()
        wh: Dict[str, Any] = health_wallets.get(wallet) or health_wallets.get(w) or {}
        in_health_file: bool = (wallet in health_wallets) or (w in health_wallets)
        normal = normalize_live_wallet_config(wallet, cfg)
        mode = str(normal.get("mode", "OFF")).upper()
        service_eligible = bool(normal.get("service_eligible"))
        service_reason = str(normal.get("service_eligibility_reason", "MODE_OFF"))
        perf = (live_leader_performance or {}).get(w, {})

        # Per-wallet manual ledger: open sleeves + exposure
        _w_sleeves = _by_wallet_pos.get(wallet) or _by_wallet_pos.get(w) or {}
        _w_open_coins: List[str] = []
        _w_exposure = 0.0
        for _coin_k, _pos_v in (_w_sleeves.items() if isinstance(_w_sleeves, dict) else []):
            _sz_v = float((_pos_v or {}).get("signed_size", 0) or 0)
            if abs(_sz_v) <= 1e-12:
                continue
            _w_open_coins.append(str(_coin_k).upper())
            _w_exposure += abs(_sz_v) * float((_pos_v or {}).get("avg_entry_px", 0) or 0)
        _w_filled_count = _filled_cnt_by_wallet.get(w, 0)
        _w_last_fill: Dict[str, Any] = _last_lf_by_wallet.get(w, {})

        # WS intent activity
        ws_intent_count = 0
        last_intent_time = ""
        last_intent_coin = ""
        last_intent_side = ""
        for row in last_rows:
            rw = str(row.get("leader_wallet") or row.get("wallet") or "").lower()
            if rw != w:
                continue
            if str(row.get("reason", "")) == "LIVE_WS_DETECTED":
                ws_intent_count += 1
                t = str(row.get("created_at", ""))
                if t > last_intent_time:
                    last_intent_time = t
                    last_intent_coin = str(row.get("coin", ""))
                    last_intent_side = str(row.get("side", ""))

        if service_reason == "CONFIG_CONFLICT_MODE_ENABLED_FALSE":
            eligibility = "CONFIG CONFLICT"
        elif service_eligible and mode == "LIVE":
            eligibility = "COPYING"
        elif service_eligible and mode == "CLO":
            eligibility = "CLOSE ONLY"
        else:
            eligibility = "DISABLED"

        # Connection: OFF wallets get "copy disabled"; LIVE/CLO use real ws health.
        if mode == "OFF" or not service_eligible:
            conn_status = "COPY DISABLED"
            conn_detail = service_reason if service_reason != "MODE_OFF" else ""
        elif not in_health_file:
            # Shared HOT10 socket: per-wallet rows absent but socket is OK
            if _shared_ws_ok and mode in {"LIVE", "CLO"}:
                conn_status = "SHARED_WS_OK"
                conn_detail = "HOT10 shared socket active"
            else:
                conn_status = "NO WS HEALTH"
                conn_detail = "service not reporting this wallet"
        else:
            raw_status = str(wh.get("current_status") or wh.get("effective_status") or wh.get("status") or "UNKNOWN").upper()
            conn_status = "OFFLINE" if raw_status in {"OFFLINE", "DISCONNECTED", "CLOSED"} else raw_status
            conn_detail = wh.get("current_health_grade") or wh.get("health_grade") or wh.get("last_error") or "n/a"

        open_positions = perf.get("current_open_positions", [])
        open_exposure = perf.get("current_exposure", 0.0)
        last_fill_d = perf.get("last_actual_fill", {})

        rows.append({
            "wallet": wallet,
            "mode": mode,
            "eligibility": eligibility,
            "enabled": bool(normal.get("enabled")),
            "service_eligible": service_eligible,
            "service_eligibility_reason": service_reason,
            "copy_mode": str(cfg.get("copy_mode", "proportional")),
            "fixed_notional": cfg.get("fixed_notional"),
            "norm_base": cfg.get("norm_base"),
            "leader_equity_base": cfg.get("leader_equity_base"),
            "max_diff_pct": cfg.get("max_diff_pct"),
            "daily_loss_limit": cfg.get("daily_loss_limit"),
            "conn_status": conn_status,
            "conn_detail": conn_detail,
            "ws_state": conn_status,
            "ws_reason": conn_detail,
            "ws_intent_count": ws_intent_count,
            "last_intent_time": last_intent_time,
            "last_ws_intent_at": last_intent_time,
            "last_intent_coin": last_intent_coin,
            "last_intent_side": last_intent_side,
            "last_intent_coin_side": f"{last_intent_coin} {last_intent_side}".strip(),
            "filled_count": _w_filled_count if _w_filled_count else perf.get("filled_count", 0),
            "exits_count": perf.get("exits_count", 0),
            "exchange_rejected_count": perf.get("exchange_rejected_count", 0),
            "local_blocked_count": perf.get("local_blocked_count", 0),
            "recent_reject_count": perf.get("recent_reject_count", 0),
            "recent_block_count": perf.get("recent_block_count", 0),
            "preview_count": perf.get("preview_count", 0),
            "preview_label": perf.get("preview_label", "Queued previews / would-send records"),
            "avg_fill_bps": perf.get("avg_fill_vs_limit_bps"),
            "worst_fill_bps": perf.get("worst_fill_vs_limit_bps"),
            "avg_leader_bps": perf.get("avg_leader_vs_user_bps"),
            "realized_pnl": perf.get("realized_pnl_estimate"),
            "unrealized_pnl": perf.get("unrealized_pnl_estimate"),
            "net_pnl": perf.get("net_pnl_estimate"),
            "live_realized_pnl": perf.get("live_realized_pnl"),
            "confirmed_realized_pnl": perf.get("confirmed_realized_pnl"),
            "realized_match_status": perf.get("realized_match_status", "N/A"),
            "matched_exchange_fill_count": perf.get("matched_exchange_fill_count", 0),
            "matched_closed_pnl_fill_count": perf.get("matched_closed_pnl_fill_count", 0),
            "live_unrealized_pnl": perf.get("live_unrealized_pnl"),
            "live_net_pnl": perf.get("live_net_pnl"),
            "live_equity_effect": perf.get("live_equity_effect"),
            "pnl_status": perf.get("pnl_status", "N/A"),
            "pnl_status_label": perf.get("pnl_status_label", "No live PnL"),
            "attribution_quality": perf.get("attribution_quality", "N/A"),
            "dry_run_fill_count": perf.get("dry_run_fill_count", 0),
            "dry_run_realized_pnl": perf.get("dry_run_realized_pnl", 0.0),
            "data_quality": perf.get("data_quality_notes", ""),
            "data_quality_notes": perf.get("data_quality_notes", ""),
            "lead_reference_price": perf.get("lead_reference_price"),
            "copy_fill_price": perf.get("copy_fill_price"),
            "total_diff_usd": perf.get("total_diff_usd"),
            "avg_diff_usd": perf.get("avg_diff_usd"),
            "avg_diff_bps": perf.get("avg_diff_bps"),
            "worst_diff_bps": perf.get("worst_diff_bps"),
            "fill_vs_limit_avg_bps": perf.get("avg_fill_vs_limit_bps"),
            "fill_vs_limit_worst_bps": perf.get("worst_fill_vs_limit_bps"),
            "open_position_count": len(_w_open_coins) if _w_open_coins else len(open_positions),
            "open_positions": _w_open_coins if _w_open_coins else open_positions,
            "open_coins": _w_open_coins if _w_open_coins else perf.get("open_coins", []),
            "open_exposure": _w_exposure if _w_exposure > 0 else open_exposure,
            "current_exposure": _w_exposure if _w_exposure > 0 else open_exposure,
            "max_exposure": perf.get("max_exposure"),
            "drawdown": perf.get("drawdown"),
            "live_dd": perf.get("drawdown"),
            "max_drawdown": perf.get("max_drawdown"),
            "last_fill": _w_last_fill if _w_last_fill else last_fill_d,
        })
    return rows


def _build_real_copy_positions(
    manual_positions: Dict[str, Any],
    exchange_snapshot: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    exchange_positions = exchange_snapshot.get("positions_by_coin", {}) if isinstance(exchange_snapshot.get("positions_by_coin"), dict) else {}
    exchange_available = bool(exchange_snapshot.get("available"))
    seen: set = set()
    sleeves = _manual_position_sleeves(manual_positions)
    coin_counts, coin_nets = _shared_manual_coin_nets(sleeves)
    shared_coins = {coin for coin, count in coin_counts.items() if count > 1}
    for wallet, coin_upper, pos in sorted(sleeves, key=lambda item: (item[1], item[0])):
        signed = fnum(pos.get("signed_size"))
        if abs(signed) <= 1e-12:
            continue
        seen.add(coin_upper)
        ex: Dict[str, Any] = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
        ex_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
        ex_mark = fnum(ex.get("mark_px")) if isinstance(ex, dict) else 0.0
        ex_entry = fnum(ex.get("entry_px")) if isinstance(ex, dict) else 0.0
        ex_upnl = fnum(ex.get("unrealized_pnl")) if isinstance(ex, dict) else 0.0
        ex_pos_value = fnum(ex.get("position_value")) if isinstance(ex, dict) else 0.0
        is_shared = coin_upper in shared_coins
        if is_shared and exchange_available:
            status = "SHARED_SYMBOL_SLEEVE_TRACKED"
        elif exchange_available:
            diff = signed - ex_signed
            if abs(diff) <= 1e-8:
                status = "MATCH"
            elif ex_signed == 0.0:
                status = "MISSING_EXCHANGE"
            else:
                status = f"SIZE_DIFF {diff:+.6f}"
        else:
            status = "EXCHANGE_UNAVAILABLE"
        rows.append({
            "row_type": "OWNED_COPY",
            "coin": coin_upper,
            "signed_size": signed,
            "side": "LONG" if signed > 0 else "SHORT",
            "leader_wallet": wallet,
            "avg_entry_px": fnum(pos.get("avg_entry_px")) or None,
            "last_copy_fill_id": pos.get("last_copy_fill_id", ""),
            "last_updated_ms": pos.get("last_updated_ms"),
            "last_intent_id": pos.get("last_intent_id", ""),
            "last_oid": pos.get("last_oid", ""),
            "last_updated_at": pos.get("last_updated_at", ""),
            "exchange_signed_size": ex_signed if exchange_available else None,
            "entry_px": ex_entry if ex_entry > 0 else None,
            "mark_px": ex_mark if ex_mark > 0 else None,
            "position_value": None if is_shared else (ex_pos_value if ex_pos_value > 0 else None),
            "unrealized_pnl": None if is_shared else (ex_upnl if exchange_available else None),
            "ledger_vs_exchange": status,
            "reconciliation_note": "shared symbol: sleeve tracked; exchange is netted at account level" if is_shared else "",
        })
    if exchange_available:
        for coin_upper in sorted(shared_coins):
            ledger_net = coin_nets.get(coin_upper, 0.0)
            ex: Dict[str, Any] = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
            ex_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
            diff = ledger_net - ex_signed
            status = "SHARED_SYMBOL_NET_MATCH" if abs(diff) <= 1e-8 else f"SHARED_SYMBOL_NET_DIFF {diff:+.6f}"
            rows.append({
                "coin": coin_upper,
                "signed_size": round(ledger_net, 12),
                "side": "NET LONG" if ledger_net > 0 else "NET SHORT" if ledger_net < 0 else "NET FLAT",
                "leader_wallet": "aggregate",
                "last_intent_id": "aggregate",
                "last_oid": "aggregate",
                "last_updated_at": "—",
                "exchange_signed_size": ex_signed,
                "entry_px": fnum(ex.get("entry_px")) or None,
                "mark_px": fnum(ex.get("mark_px")) or None,
                "position_value": fnum(ex.get("position_value")) or None,
                "unrealized_pnl": fnum(ex.get("unrealized_pnl")) if isinstance(ex, dict) else None,
                "ledger_vs_exchange": status,
                "reconciliation_note": "aggregate net matches exchange" if abs(diff) <= 1e-8 else "aggregate ledger net differs from exchange net",
            })
    if exchange_available:
        for coin, ex in sorted(exchange_positions.items()):
            if coin in seen:
                continue
            ex_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
            if abs(ex_signed) <= 1e-12:
                continue
            rows.append({
                "row_type": "ACCOUNT_LEVEL_ONLY",
                "coin": coin.upper(),
                "signed_size": None,
                "side": "LONG" if ex_signed > 0 else "SHORT",
                "leader_wallet": "—",
                "avg_entry_px": None,
                "last_copy_fill_id": "",
                "last_updated_ms": None,
                "last_intent_id": "—",
                "last_oid": "—",
                "last_updated_at": "—",
                "exchange_signed_size": ex_signed,
                "entry_px": fnum(ex.get("entry_px")) or None,
                "mark_px": fnum(ex.get("mark_px")) or None,
                "position_value": fnum(ex.get("position_value")) or None,
                "unrealized_pnl": fnum(ex.get("unrealized_pnl")) or None,
                "ledger_vs_exchange": "ORPHAN_EXCHANGE",
                "reconciliation_note": "pre-existing or non-copied exchange position; not adopted into ledger",
            })
    return rows


def _build_execution_quality_rows(
    recent_send_attempts: List[Dict[str, Any]],
    last_rows: List[Dict[str, Any]],
    live_fills: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    if live_fills is None:
        live_fills = _load_recent_live_fills(500)
    intent_by_id: Dict[str, Dict[str, Any]] = {}
    for row in last_rows:
        iid = str(row.get("intent_id", "")).strip()
        if iid:
            intent_by_id[iid] = row
    # Build live_fill lookup by intent_id first, then by leader_fill_id as fallback
    _lf_by_intent: Dict[str, Dict[str, Any]] = {}
    _lf_by_leader_fill: Dict[str, Dict[str, Any]] = {}
    for _lf in (live_fills or []):
        _iid = str(_lf.get("intent_id", "")).strip()
        if _iid and _iid not in _lf_by_intent:
            _lf_by_intent[_iid] = _lf
        _lfid = str(_lf.get("leader_fill_id", "")).strip()
        if _lfid and _lfid not in _lf_by_leader_fill:
            _lf_by_leader_fill[_lfid] = _lf
    rows: List[Dict[str, Any]] = []
    for attempt in reversed(recent_send_attempts):
        intent_id = str(attempt.get("intent_id", "")).strip()
        leader_fill_id = str(attempt.get("leader_fill_id", "")).strip()
        intent = intent_by_id.get(intent_id, {})
        # Resolve live_fill: intent_id first, then leader_fill_id
        live_fill = _lf_by_intent.get(intent_id) or _lf_by_leader_fill.get(leader_fill_id) or {}
        # fill_avg_px: parsed from exchange_response, else live_fill fill_price
        fill_px = fnum(attempt.get("fill_avg_px"))
        if fill_px == 0 and live_fill:
            fill_px = fnum(live_fill.get("fill_price"))
        # fill_size: parsed from exchange_response, else live_fill fill_size
        fill_size_val = attempt.get("fill_size")
        if (fill_size_val is None or fnum(fill_size_val) == 0) and live_fill:
            fill_size_val = live_fill.get("fill_size")
        # oid: exchange_order_id column (already mapped to oid in _load_recent_send_attempts)
        oid_val = attempt.get("oid") or attempt.get("exchange_order_id")
        limit_px = fnum(attempt.get("limit_price") or attempt.get("limit_px") or 0)
        if limit_px == 0 and intent:
            limit_px = fnum(intent.get("suggested_limit_price") or intent.get("target_price") or 0)
        leader_px = fnum(intent.get("leader_price") or intent.get("target_price") or 0) if intent else 0.0
        side = str(attempt.get("actual_side") or attempt.get("side") or "").upper()
        fill_bps: Optional[float] = None
        if fill_px > 0 and limit_px > 0:
            raw = (fill_px - limit_px) / limit_px * 10000 if side == "BUY" else (limit_px - fill_px) / limit_px * 10000
            fill_bps = round(raw, 2)
        leader_bps: Optional[float] = None
        if fill_px > 0 and leader_px > 0:
            raw2 = (fill_px - leader_px) / leader_px * 10000 if side == "BUY" else (leader_px - fill_px) / leader_px * 10000
            leader_bps = round(raw2, 2)
        rows.append({
            "time": attempt.get("created_at", ""),
            "leader_wallet": str(attempt.get("leader_wallet") or attempt.get("auto_send_wallet") or ""),
            "coin": attempt.get("coin", ""),
            "side": side,
            "status": attempt.get("status", ""),
            "limit_px": limit_px if limit_px > 0 else None,
            "fill_avg_px": fill_px if fill_px > 0 else None,
            "fill_size": fill_size_val,
            "oid": oid_val,
            "fill_bps": fill_bps,
            "leader_bps": leader_bps,
            "marketable_bps": fnum(attempt.get("marketable_bps")) or None,
            "error": attempt.get("error", ""),
        })
    return rows


_EXCHANGE_REJECT_STATUSES = {"ORDER_REJECTED"}
_PREVIEW_STATUSES = {"CONFIRM_REQUIRED"}
_TERMINAL_IGNORE = {"", "ORDER_FILLED", "ORDER_REJECTED", "CONFIRM_REQUIRED"}


def _is_dry_run_live_fill(row: Dict[str, Any]) -> bool:
    return str(row.get("fill_status", "")).upper() == "DRY_RUN_FILLED" or parse_bool(row.get("dry_run"))


def _send_attempt_time_ms(row: Dict[str, Any]) -> int:
    return _iso_to_ms(row.get("created_at"))


def _exchange_fill_side(row: Dict[str, Any]) -> str:
    raw = str(row.get("side") or "").upper()
    if raw.startswith("B"):
        return "BUY"
    if raw.startswith("A") or raw.startswith("S"):
        return "SELL"
    return raw


def _exchange_fill_match_id(row: Dict[str, Any]) -> str:
    return str(row.get("hash") or row.get("tid") or row.get("oid") or f"{row.get('time')}:{row.get('coin')}:{row.get('side')}:{row.get('sz')}:{row.get('px')}")


def _row_client_id(row: Dict[str, Any]) -> str:
    return str(
        row.get("cloid")
        or row.get("client_order_id")
        or row.get("clientOrderId")
        or row.get("client_id")
        or row.get("cid")
        or ""
    ).strip()


def _match_exchange_fill_to_attempt(attempt: Dict[str, Any], fills: List[Dict[str, Any]], used: set[str]) -> Optional[Dict[str, Any]]:
    attempt_oid = str(attempt.get("oid") or "").strip()
    attempt_client_id = _row_client_id(attempt)
    if not attempt_oid and not attempt_client_id:
        return None
    for fill in fills:
        mid = _exchange_fill_match_id(fill)
        if mid in used:
            continue
        fill_oid = str(fill.get("oid") or "").strip()
        fill_client_id = _row_client_id(fill)
        if attempt_oid and fill_oid and fill_oid == attempt_oid:
            used.add(mid)
            return fill
        if attempt_client_id and fill_client_id and fill_client_id == attempt_client_id:
            used.add(mid)
            return fill
    return None


def _build_live_leader_performance(
    recent_send_attempts: List[Dict[str, Any]],
    manual_positions: Dict[str, Any],
    exchange_snapshot: Dict[str, Any],
    live_config: Dict[str, Any],
    last_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    intent_by_id: Dict[str, Dict[str, Any]] = {}
    for row in last_rows:
        iid = str(row.get("intent_id", "")).strip()
        if iid:
            intent_by_id[iid] = row

    would_send_by_id: Dict[str, Dict[str, Any]] = {}
    if WOULD_SEND_ORDERS_CSV.exists():
        try:
            with WOULD_SEND_ORDERS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    iid = str(row.get("intent_id", "")).strip()
                    if iid:
                        would_send_by_id[iid] = row
        except Exception:
            pass

    exchange_positions = exchange_snapshot.get("positions_by_coin", {}) if isinstance(exchange_snapshot.get("positions_by_coin"), dict) else {}
    exchange_available = bool(exchange_snapshot.get("available"))
    coin_wallets: Dict[str, set[str]] = {}
    for pw, coin, pos in iter_manual_wallet_positions(manual_positions):
        if abs(fnum(pos.get("signed_size"))) <= 1e-12:
            continue
        if pw:
            coin_wallets.setdefault(str(coin).upper(), set()).add(pw)

    live_fills_by_wallet: Dict[str, List[Dict[str, Any]]] = {}
    if LIVE_FILLS_CSV.exists():
        try:
            with LIVE_FILLS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    lw = str(row.get("leader_wallet", "")).lower()
                    if lw:
                        live_fills_by_wallet.setdefault(lw, []).append(row)
        except Exception:
            pass
    actual_user_fills = exchange_snapshot.get("actual_user_fills_recent", [])
    if not isinstance(actual_user_fills, list):
        actual_user_fills = []

    wallets = live_config.get("wallets", {})
    if not isinstance(wallets, dict):
        wallets = {}

    wallet_keys: set = set(wallets.keys())
    for attempt in recent_send_attempts:
        aw = str(attempt.get("leader_wallet") or attempt.get("auto_send_wallet") or "").lower()
        if aw:
            wallet_keys.add(aw)

    perf: Dict[str, Any] = {}
    for wallet_key in sorted(wallet_keys):
        w = wallet_key.lower()
        filled_attempts: List[Dict[str, Any]] = []
        exch_rejected: List[Dict[str, Any]] = []
        local_blocked: List[Dict[str, Any]] = []
        previews: List[Dict[str, Any]] = []
        gross_buy = 0.0
        gross_sell = 0.0
        fill_bps_vals: List[float] = []
        leader_bps_vals: List[float] = []
        diff_usd_vals: List[float] = []
        friction_rows: List[Dict[str, Any]] = []
        pnl_points: List[Dict[str, Any]] = []
        exits_count = 0
        recent_cutoff_ms = int(time.time() * 1000) - 60 * 60 * 1000
        last_actual_fill: Dict[str, Any] = {}
        last_exch_reject: Dict[str, Any] = {}
        last_local_block: Dict[str, Any] = {}

        for attempt in recent_send_attempts:
            aw = str(attempt.get("leader_wallet") or attempt.get("auto_send_wallet") or "").lower()
            if aw != w:
                continue
            st = str(attempt.get("status", ""))
            if st == "ORDER_FILLED":
                filled_attempts.append(attempt)
                last_actual_fill = attempt
                fill_px = fnum(attempt.get("fill_avg_px"))
                fill_sz = fnum(attempt.get("fill_size"))
                side = str(attempt.get("actual_side") or attempt.get("side") or "").upper()
                if fill_px > 0 and fill_sz > 0:
                    notional = fill_px * abs(fill_sz)
                    if side == "BUY":
                        gross_buy += notional
                    else:
                        gross_sell += notional
                if str(attempt.get("reduce_only", "")).lower() in {"1", "true", "yes"}:
                    exits_count += 1
                elif abs(fnum(attempt.get("position_after"))) < abs(fnum(attempt.get("position_before"))):
                    exits_count += 1
                limit_px = fnum(attempt.get("limit_price") or attempt.get("limit_px") or 0)
                iid = str(attempt.get("intent_id", "")).strip()
                if limit_px == 0 and iid:
                    intent = intent_by_id.get(iid, {})
                    limit_px = fnum(intent.get("suggested_limit_price") or intent.get("target_price") or 0)
                if fill_px > 0 and limit_px > 0:
                    raw = (fill_px - limit_px) / limit_px * 10000 if side == "BUY" else (limit_px - fill_px) / limit_px * 10000
                    fill_bps_vals.append(round(raw, 2))
                intent = intent_by_id.get(iid, {}) if iid else {}
                would = would_send_by_id.get(iid, {}) if iid else {}
                leader_px = fnum(
                    intent.get("leader_price")
                    or intent.get("target_price")
                    or intent.get("suggested_limit_price")
                    or would.get("limit_price")
                    or 0
                )
                diff_usd: Optional[float] = None
                diff_bps: Optional[float] = None
                if fill_px > 0 and leader_px > 0:
                    lb = (fill_px - leader_px) / leader_px * 10000 if side == "BUY" else (leader_px - fill_px) / leader_px * 10000
                    diff_bps = round(lb, 2)
                    leader_bps_vals.append(diff_bps)
                    diff_usd = round((fill_px - leader_px) * abs(fill_sz) if side == "BUY" else (leader_px - fill_px) * abs(fill_sz), 6)
                    diff_usd_vals.append(diff_usd)
                friction_rows.append({
                    "time": attempt.get("created_at", ""),
                    "coin": attempt.get("coin", ""),
                    "side": side,
                    "leader_reference_px": leader_px if leader_px > 0 else None,
                    "copy_fill_px": fill_px if fill_px > 0 else None,
                    "size": fill_sz if fill_sz > 0 else None,
                    "diff_usd": diff_usd,
                    "diff_bps": diff_bps,
                    "limit_px": limit_px if limit_px > 0 else None,
                    "fill_vs_limit_bps": fill_bps_vals[-1] if fill_bps_vals else None,
                    "oid": attempt.get("oid", ""),
                    "intent_id": iid,
                })
            elif st == "ORDER_REJECTED":
                exch_rejected.append(attempt)
                last_exch_reject = attempt
            elif st == "CONFIRM_REQUIRED":
                previews.append(attempt)
            elif st:
                local_blocked.append(attempt)
                last_local_block = attempt
        recent_reject_count = sum(1 for row in exch_rejected if _send_attempt_time_ms(row) >= recent_cutoff_ms)
        recent_block_count = sum(1 for row in local_blocked if _send_attempt_time_ms(row) >= recent_cutoff_ms)

        avg_fill_bps: Optional[float] = round(sum(fill_bps_vals) / len(fill_bps_vals), 1) if fill_bps_vals else None
        worst_fill_bps: Optional[float] = round(max(fill_bps_vals), 1) if fill_bps_vals else None
        avg_leader_bps: Optional[float] = round(sum(leader_bps_vals) / len(leader_bps_vals), 1) if leader_bps_vals else None
        total_diff_usd: Optional[float] = round(sum(diff_usd_vals), 4) if diff_usd_vals else None
        avg_diff_usd: Optional[float] = round(sum(diff_usd_vals) / len(diff_usd_vals), 4) if diff_usd_vals else None
        worst_diff_bps: Optional[float] = round(max(leader_bps_vals), 1) if leader_bps_vals else None

        realized_pnl: Optional[float] = None
        pnl_status = "N/A"
        attribution_quality = "N/A"
        data_quality_notes: List[str] = []
        wfills = live_fills_by_wallet.get(w, [])
        dry_run_wfills = [f for f in wfills if _is_dry_run_live_fill(f)]
        real_wfills = [f for f in wfills if not _is_dry_run_live_fill(f)]
        dry_run_realized_pnl = round(sum(fnum(f.get("realized_pnl", 0)) for f in dry_run_wfills), 4) if dry_run_wfills else 0.0
        if real_wfills:
            total_fees = sum(fnum(f.get("fee", 0)) for f in real_wfills)
            pnl_status = "ACCOUNT_LEVEL_ONLY"
            attribution_quality = "WALLET_REALIZED_REQUIRES_EXACT_EXCHANGE_FILL_ID"
            data_quality_notes.append("account realised PnL is shown in header; wallet attribution unavailable")
            if abs(total_fees) > 1e-8:
                data_quality_notes.append(f"live_fills fees diagnostic={round(total_fees, 4)}")
            if dry_run_wfills:
                data_quality_notes.append(f"dry-run fills excluded={len(dry_run_wfills)}")
        elif not filled_attempts:
            data_quality_notes.append("no fills yet")
        else:
            pnl_status = "ACCOUNT_LEVEL_ONLY"
            attribution_quality = "WALLET_REALIZED_REQUIRES_EXACT_EXCHANGE_FILL_ID"
            data_quality_notes.append("account realised PnL is shown in header; wallet attribution unavailable")
        if wfills and not real_wfills and dry_run_wfills:
            data_quality_notes.append("dry-run live_fills excluded from live PnL")

        used_exchange_fills: set[str] = set()
        matched_exchange_fills: List[Dict[str, Any]] = []
        for attempt in filled_attempts:
            match = _match_exchange_fill_to_attempt(attempt, actual_user_fills, used_exchange_fills)
            if match is not None:
                matched_exchange_fills.append(match)
        confirmed_realized_pnl: Optional[float] = None
        realized_match_status = "NO_MATCHED_EXCHANGE_CLOSED_PNL"
        matched_closed_count = sum(1 for fill in matched_exchange_fills if abs(fnum(fill.get("closedPnl") if "closedPnl" in fill else fill.get("closed_pnl"))) > 1e-12)
        if matched_exchange_fills:
            confirmed_realized_pnl = round(sum(fnum(fill.get("closedPnl") if "closedPnl" in fill else fill.get("closed_pnl")) for fill in matched_exchange_fills), 8)
            realized_match_status = "EXACT_ID_MATCHED_EXCHANGE_USER_FILLS" if matched_closed_count else "EXACT_ID_MATCHED_USER_FILLS_CLOSEDPNL_ZERO"
            if realized_pnl is None and abs(confirmed_realized_pnl) > 1e-12:
                realized_pnl = confirmed_realized_pnl
                pnl_status = "EXACT"
                attribution_quality = "EXACT_ID_EXCHANGE_CLOSED_PNL"
        elif filled_attempts:
            realized_match_status = "ACCOUNT_LEVEL_ONLY"

        open_positions: List[Dict[str, Any]] = []
        current_exposure = 0.0
        total_unrealized = 0.0
        has_unrealized = False
        has_ambiguous_coin = False

        for pw, coin, pos in iter_manual_wallet_positions(manual_positions):
            if pw != w:
                continue
            signed = fnum(pos.get("signed_size"))
            if abs(signed) <= 1e-12:
                continue
            coin_upper = coin.upper()
            coin_shared = len(coin_wallets.get(coin_upper, set())) > 1
            ex: Dict[str, Any] = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
            mark = fnum(ex.get("mark_px")) if isinstance(ex, dict) else 0.0
            entry = fnum(ex.get("entry_px")) if isinstance(ex, dict) else 0.0
            ex_upnl = fnum(ex.get("unrealized_pnl")) if isinstance(ex, dict) else 0.0
            pos_value = fnum(ex.get("position_value")) if isinstance(ex, dict) else 0.0
            exp_est = abs(signed) * mark if mark > 0 else (pos_value if pos_value > 0 else 0.0)
            current_exposure += exp_est
            if coin_shared:
                match = "AMBIGUOUS_COIN_SHARED"
                has_ambiguous_coin = True
            elif exchange_available and isinstance(ex, dict) and ex:
                ex_signed = fnum(ex.get("signed_size"))
                match = "MATCH" if abs(ex_signed - signed) <= 1e-8 else "MISMATCH"
                total_unrealized += ex_upnl
                has_unrealized = True
            else:
                match = "EXCHANGE_UNAVAILABLE"
            open_positions.append({
                "coin": coin_upper,
                "signed_size": signed,
                "side": "LONG" if signed > 0 else "SHORT",
                "entry_px": entry if entry > 0 else None,
                "mark_px": mark if mark > 0 else None,
                "unrealized_pnl": ex_upnl if exchange_available and not coin_shared else None,
                "exposure": round(exp_est, 4) if exp_est > 0 else None,
                "exchange_match": match,
            })

        unrealized_pnl: Optional[float] = round(total_unrealized, 4) if has_unrealized else None

        if has_ambiguous_coin:
            pnl_status = "AMBIGUOUS_COIN_SHARED"
            attribution_quality = "UNSAFE_SHARED_COIN_ATTRIBUTION"
            data_quality_notes.append("shared coin across leaders; exchange open PnL not attributed")
        elif pnl_status == "EXACT":
            if unrealized_pnl is not None and open_positions:
                pass  # already exact realized; unrealized is a bonus
        elif pnl_status == "ACCOUNT_LEVEL_ONLY":
            if unrealized_pnl is not None and open_positions:
                attribution_quality = "ACCOUNT_LEVEL_REALIZED_OPEN_PNL_SAFE"
        elif unrealized_pnl is not None and open_positions:
            if realized_pnl is None:
                pnl_status = "OPEN_ONLY"
                attribution_quality = "EXCHANGE_OPEN_UNREALIZED_ONLY"
            elif attribution_quality == "N/A":
                attribution_quality = "EXCHANGE_OPEN_UNREALIZED_ONLY"
        elif pnl_status == "ESTIMATED_FROM_REAL_ORDER_FILLS":
            pass
        elif not has_unrealized and not real_wfills and filled_attempts:
            pnl_status = "ACCOUNT_LEVEL_ONLY"
            attribution_quality = "WALLET_REALIZED_REQUIRES_EXACT_EXCHANGE_FILL_ID"

        net_pnl: Optional[float] = None
        if realized_pnl is not None or unrealized_pnl is not None:
            net_pnl = round((realized_pnl or 0.0) + (unrealized_pnl or 0.0), 4)

        drawdown: Optional[float] = None
        max_drawdown: Optional[float] = None
        if pnl_points:
            peak = pnl_points[0]["value"]
            max_dd = 0.0
            for p in pnl_points:
                peak = max(peak, fnum(p.get("value")))
                dd = fnum(p.get("value")) - peak
                max_dd = min(max_dd, dd)
            drawdown = round(fnum(pnl_points[-1].get("value")) - peak, 4)
            max_drawdown = round(max_dd, 4)

        exposure_series = [{
            "timestamp": exchange_snapshot.get("updated_at") or utc_now_iso(),
            "value": round(current_exposure, 4),
        }] if current_exposure else []
        pnl_status_labels = {
            "EXACT": "Exact closed PnL",
            "OPEN_ONLY": "Open PnL",
            "ESTIMATED_FROM_REAL_ORDER_FILLS": "Real fills",
            "ACCOUNT_LEVEL_ONLY": "Account-level only",
            "AMBIGUOUS_COIN_SHARED": "Shared coin",
            "N/A": "No PnL yet",
        }
        pnl_status_label = pnl_status_labels.get(pnl_status, pnl_status or "No live PnL")

        perf[w] = {
            "wallet": wallet_key,
            "filled_count": len(filled_attempts),
            "exits_count": exits_count,
            "exchange_rejected_count": len(exch_rejected),
            "local_blocked_count": len(local_blocked),
            "recent_reject_count": recent_reject_count,
            "recent_block_count": recent_block_count,
            "preview_count": len(previews),
            "preview_label": "Queued previews / would-send records",
            "gross_buy_notional": round(gross_buy, 4),
            "gross_sell_notional": round(gross_sell, 4),
            "current_open_positions": open_positions,
            "current_exposure": round(current_exposure, 4),
            "open_position_count": len(open_positions),
            "open_coins": [p.get("coin") for p in open_positions if p.get("coin")],
            "max_exposure": None,
            "drawdown": drawdown,
            "max_drawdown": max_drawdown,
            "realized_pnl_estimate": realized_pnl,
            "unrealized_pnl_estimate": unrealized_pnl,
            "net_pnl_estimate": net_pnl,
            "live_realized_pnl": realized_pnl,
            "confirmed_realized_pnl": confirmed_realized_pnl,
            "realized_match_status": realized_match_status,
            "matched_exchange_fill_count": len(matched_exchange_fills),
            "matched_closed_pnl_fill_count": matched_closed_count,
            "live_unrealized_pnl": unrealized_pnl,
            "live_net_pnl": net_pnl,
            "live_equity_effect": net_pnl,
            "pnl_status": pnl_status,
            "pnl_status_label": pnl_status_label,
            "attribution_quality": attribution_quality,
            "dry_run_fill_count": len(dry_run_wfills),
            "dry_run_realized_pnl": dry_run_realized_pnl,
            "lead_reference_price": friction_rows[-1].get("leader_reference_px") if friction_rows else None,
            "copy_fill_price": friction_rows[-1].get("copy_fill_px") if friction_rows else None,
            "total_diff_usd": total_diff_usd,
            "avg_diff_usd": avg_diff_usd,
            "avg_diff_bps": avg_leader_bps,
            "worst_diff_bps": worst_diff_bps,
            "friction_fills": friction_rows[-50:],
            "pnl_series": pnl_points[-500:],
            "exposure_series": exposure_series,
            "avg_fill_vs_limit_bps": avg_fill_bps,
            "worst_fill_vs_limit_bps": worst_fill_bps,
            "avg_leader_vs_user_bps": avg_leader_bps,
            "last_actual_fill": {
                "coin": last_actual_fill.get("coin", ""),
                "side": last_actual_fill.get("actual_side") or last_actual_fill.get("side", ""),
                "size": last_actual_fill.get("fill_size", ""),
                "avg_px": last_actual_fill.get("fill_avg_px", ""),
                "oid": last_actual_fill.get("oid", ""),
                "time": last_actual_fill.get("created_at", ""),
            } if last_actual_fill else {},
            "last_exchange_reject": {
                "coin": last_exch_reject.get("coin", ""),
                "side": last_exch_reject.get("actual_side") or last_exch_reject.get("side", ""),
                "time": last_exch_reject.get("created_at", ""),
                "error": last_exch_reject.get("error", ""),
            } if last_exch_reject else {},
            "last_local_block": {
                "status": last_local_block.get("status", ""),
                "coin": last_local_block.get("coin", ""),
                "time": last_local_block.get("created_at", ""),
            } if last_local_block else {},
            "data_quality_notes": "; ".join(data_quality_notes) if data_quality_notes else "",
        }
    return perf


def _build_execution_quality_summary(execution_quality_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    filled = [r for r in execution_quality_rows if r.get("status") == "ORDER_FILLED"]
    exch_rej = [r for r in execution_quality_rows if r.get("status") == "ORDER_REJECTED"]
    local_blk = [r for r in execution_quality_rows if r.get("status") and r.get("status") not in _TERMINAL_IGNORE]
    previews = [r for r in execution_quality_rows if r.get("status") == "CONFIRM_REQUIRED"]
    bps_vals = [r["fill_bps"] for r in filled if r.get("fill_bps") is not None]
    lbps_vals = [r["leader_bps"] for r in filled if r.get("leader_bps") is not None]
    avg_bps: Optional[float] = round(sum(bps_vals) / len(bps_vals), 2) if bps_vals else None
    worst_bps: Optional[float] = round(max(bps_vals), 2) if bps_vals else None
    avg_lbps: Optional[float] = round(sum(lbps_vals) / len(lbps_vals), 2) if lbps_vals else None
    last_fill = filled[0] if filled else {}
    return {
        "filled_count": len(filled),
        "exchange_rejected_count": len(exch_rej),
        "local_blocked_count": len(local_blk),
        "preview_count": len(previews),
        "avg_fill_vs_limit_bps": avg_bps,
        "worst_fill_vs_limit_bps": worst_bps,
        "avg_leader_vs_user_bps": avg_lbps,
        "last_fill": {"coin": last_fill.get("coin", ""), "side": last_fill.get("side", ""), "time": last_fill.get("time", "")} if last_fill else {},
    }


def _load_send_attempt_counts() -> Dict[str, int]:
    counts: Dict[str, int] = {}
    if not SEND_ATTEMPTS_CSV.exists():
        return counts
    try:
        with SEND_ATTEMPTS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                s = str(row.get("status") or "UNKNOWN")
                counts[s] = counts.get(s, 0) + 1
    except Exception:
        pass
    return counts


def load_live_config() -> Dict[str, Any]:
    return _load_live_copy_config()


def save_live_config(cfg: Dict[str, Any]) -> None:
    _save_live_copy_config(cfg)


def enforce_live_wallet_limit(cfg: Dict[str, Any], max_live: int = 10) -> Dict[str, Any]:
    wallets = cfg.get("wallets", {})
    live = [w for w, v in wallets.items() if v.get("mode") == "LIVE"]

    if len(live) <= max_live:
        return cfg

    for w in live[max_live:]:
        wallets[w]["mode"] = "OFF"

    cfg["wallets"] = wallets
    return cfg


def save_wallet_gate(gate: Dict[str, Any]) -> None:
    atomic_write_json(WALLET_GATE_FILE, gate)


def parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_raw_json(text: str) -> Dict[str, Any]:
    try:
        data = json.loads(text or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def normalise_recording_method(row: Dict[str, Any], source: str, is_snapshot: bool) -> str:
    raw = str(row.get("recording_method", "")).strip().upper()
    if raw in {WS_CAPTURED, REBUILD}:
        return raw
    return WS_CAPTURED if source == "ws" and not is_snapshot else REBUILD


def expected_copy_price_from_row(row: Dict[str, Any], raw: Dict[str, Any], leader_price: float) -> Tuple[float, str]:
    keys = (
        "expected_copy_price", "copy_exec_price", "copy_execution_price", "copy_price",
        "expected_fill_price", "mark_price_at_expected_execution", "mark_price_at_received",
        "mark_price", "markPx", "oraclePx",
    )
    for k in keys:
        if k in row:
            v = fnum(row.get(k))
            if v > 0:
                return v, k
        if k in raw:
            v = fnum(raw.get(k))
            if v > 0:
                return v, f"raw.{k}"
    return leader_price, "leader_price_fallback"


def disadvantage_bps(side: str, leader_px: float, copy_px: float) -> Optional[float]:
    if leader_px <= 0 or copy_px <= 0:
        return None
    raw_bps = ((copy_px - leader_px) / leader_px) * 10000.0
    return raw_bps if side.upper() == "BUY" else -raw_bps


def fill_can_measure_execution_delta(fill: RawFill) -> bool:
    return (
        fill.recording_method == WS_CAPTURED
        and fill.contributes_to_execution_delta
        and fill.expected_copy_price > 0
        and fill.expected_copy_price_source != "leader_price_fallback"
    )


def copy_price_for_fill(fill: RawFill) -> float:
    return fill.expected_copy_price if fill.expected_copy_price > 0 else fill.price


def parse_raw_fill_row(row: Dict[str, Any]) -> Optional[RawFill]:
    try:
        wallet = str(row.get("wallet", "")).lower()
        coin = str(row.get("coin", "")).upper()
        price = fnum(row.get("price"))
        size = abs(fnum(row.get("size")))
        ts = inum(row.get("timestamp_ms"))
        if not wallet or not coin or price <= 0 or size <= 0 or ts <= 0:
            return None
        side = str(row.get("side", "BUY")).upper()
        if side not in {"BUY", "SELL"}:
            side = "BUY" if fnum(row.get("signed_size_delta")) >= 0 else "SELL"
        source = str(row.get("source", "unknown")).lower()
        is_snapshot = parse_bool(row.get("is_snapshot", "False"))
        raw_json = str(row.get("raw_json", "{}"))
        raw = load_raw_json(raw_json)
        recording_method = normalise_recording_method(row, source, is_snapshot)
        rebuild_reason = str(row.get("rebuild_reason", ""))
        expected_copy_price, expected_source = expected_copy_price_from_row(row, raw, price)
        explicit_contrib = row.get("contributes_to_execution_delta", "")
        if explicit_contrib == "":
            contributes = recording_method == WS_CAPTURED and expected_source != "leader_price_fallback"
        else:
            contributes = parse_bool(explicit_contrib) and recording_method == WS_CAPTURED and expected_source != "leader_price_fallback"
        return RawFill(
            fill_id=str(row.get("fill_id", f"{wallet}:{coin}:{ts}:{side}:{size}:{price}")),
            wallet=wallet,
            coin=coin,
            side=side,
            price=price,
            size=size,
            signed_size_delta=fnum(row.get("signed_size_delta"), size if side == "BUY" else -size),
            start_position=fnum(row.get("start_position")),
            end_position=fnum(row.get("end_position")),
            closed_pnl=fnum(row.get("closed_pnl")),
            fee=abs(fnum(row.get("fee"))),
            timestamp_ms=ts,
            timestamp_iso=str(row.get("timestamp_iso") or datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()),
            received_at_ms=inum(row.get("received_at_ms")),
            received_at_iso=str(row.get("received_at_iso", "")),
            latency_ms=inum(row.get("latency_ms")),
            source=source,
            shard_id=inum(row.get("shard_id"), -1),
            is_snapshot=is_snapshot,
            raw_json=raw_json,
            recording_method=recording_method,
            rebuild_reason=rebuild_reason,
            contributes_to_execution_delta=contributes,
            reconstructed=parse_bool(row.get("reconstructed", "False")),
            expected_copy_price=expected_copy_price,
            expected_copy_price_source=expected_source,
        )
    except Exception:
        return None

def load_raw_fills() -> List[RawFill]:
    if not RAW_FILLS_CSV.exists():
        return []
    fills: List[RawFill] = []
    seen = set()
    purged = load_purged_wallets()
    with RAW_FILLS_CSV.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fill = parse_raw_fill_row(row)
            if fill is None or fill.fill_id in seen or fill.wallet in purged:
                continue
            seen.add(fill.fill_id)
            fills.append(fill)
    fills.sort(key=lambda x: (x.timestamp_ms, x.wallet, x.coin, x.fill_id))
    return fills


def sign_from_side(side: str) -> float:
    return 1.0 if side.upper() == "BUY" else -1.0


def bps_fee(notional: float, fee_bps: float) -> float:
    return abs(notional) * fee_bps / 10000.0


def calc_unrealized(side: str, entry_px: float, mark_px: float, size_units: float) -> float:
    if entry_px <= 0 or mark_px <= 0 or size_units <= 0:
        return 0.0
    direction = 1.0 if side == "BUY" else -1.0
    return (mark_px - entry_px) * size_units * direction


def close_positions_fifo(positions: List[ModelPosition], raw_size_to_close: float) -> Tuple[List[Tuple[ModelPosition, float]], float]:
    remaining = abs(raw_size_to_close)
    closed: List[Tuple[ModelPosition, float]] = []
    kept: List[ModelPosition] = []
    for p in positions:
        if remaining <= 1e-12:
            kept.append(p)
            continue
        take_leader = min(p.leader_size_units, remaining)
        frac = take_leader / p.leader_size_units if p.leader_size_units > 0 else 0.0
        if frac > 0:
            closed.append((p, frac))
        remaining -= take_leader
        leftover_leader = p.leader_size_units - take_leader
        if leftover_leader > 1e-12:
            kept.append(ModelPosition(
                trade_id=p.trade_id, wallet=p.wallet, coin=p.coin, side=p.side,
                entry_time_ms=p.entry_time_ms, entry_time_iso=p.entry_time_iso,
                entry_price_lead=p.entry_price_lead, entry_price_copy=p.entry_price_copy,
                leader_size_units=leftover_leader,
                copy_size_units=p.copy_size_units * (leftover_leader / p.leader_size_units),
                leader_notional=p.leader_notional * (leftover_leader / p.leader_size_units),
                copy_notional=p.copy_notional * (leftover_leader / p.leader_size_units),
                entry_fee_lead=p.entry_fee_lead * (leftover_leader / p.leader_size_units),
                entry_fee_copy=p.entry_fee_copy * (leftover_leader / p.leader_size_units),
                source=p.source,
                latency_ms=p.latency_ms,
                recording_method=p.recording_method,
                entry_contributes_to_execution_delta=p.entry_contributes_to_execution_delta,
                entry_disadvantage_bps=p.entry_disadvantage_bps,
                entry_price_source=p.entry_price_source,
            ))
    positions[:] = kept
    return closed, remaining


def effective_wallet_ui(wallet: str, ui: Dict[str, Any]) -> Dict[str, Any]:
    """Return copy sizing settings for one wallet, with global header fallback."""
    cfg = (ui.get("wallet_config") or {}).get(str(wallet).lower(), {})
    mode = str(cfg.get("copy_mode", ui.get("copy_mode", "proportional"))).lower()
    if mode not in {"proportional", "fixed"}:
        mode = str(ui.get("copy_mode", "proportional")).lower()
    return {
        **ui,
        "copy_mode": mode if mode in {"proportional", "fixed"} else "proportional",
        "norm_base": max(1.0, fnum(cfg.get("norm_base", ui.get("norm_base")), DEFAULT_NORM_BASE)),
        "fixed_notional": max(0.01, fnum(cfg.get("fixed_notional", ui.get("fixed_notional")), DEFAULT_FIXED_NOTIONAL)),
        "wallet_override": bool(cfg),
    }


def wallet_alloc(wallet: str, ui: Dict[str, Any]) -> float:
    return max(1.0, fnum(effective_wallet_ui(wallet, ui).get("norm_base"), DEFAULT_NORM_BASE))


def model_copy_notional(fill: RawFill, ui: Dict[str, Any]) -> float:
    cfg = effective_wallet_ui(fill.wallet, ui)
    if cfg["copy_mode"] == "fixed":
        return cfg["fixed_notional"]
    leader_notional = abs(fill.price * fill.size)
    return leader_notional * (cfg["norm_base"] / cfg["leader_equity_base"])


def model_copy_notional_for_size(fill: RawFill, ui: Dict[str, Any], leader_size_units: float) -> float:
    cfg = effective_wallet_ui(fill.wallet, ui)
    full_size = abs(fill.size) if fill.size else abs(leader_size_units)
    frac = abs(leader_size_units) / full_size if full_size > 0 else 1.0
    if cfg["copy_mode"] == "fixed":
        return cfg["fixed_notional"] * frac
    leader_notional = abs(fill.price * leader_size_units)
    return leader_notional * (cfg["norm_base"] / cfg["leader_equity_base"])


def signed_position_from_model_positions(positions: List[ModelPosition], use_copy_size: bool = False) -> float:
    signed = 0.0
    for p in positions:
        qty = p.copy_size_units if use_copy_size else p.leader_size_units
        signed += qty * sign_from_side(p.side)
    return signed


def side_label_from_signed(value: float, eps: float = 1e-10) -> str:
    if value > eps:
        return "LONG"
    if value < -eps:
        return "SHORT"
    return "FLAT"


def alignment_status_for_key(
    leader_positions: Dict[Tuple[str, str], List[ModelPosition]],
    copy_positions: Dict[Tuple[str, str], List[ModelPosition]],
    key: Tuple[str, str],
) -> Dict[str, Any]:
    lead_signed = signed_position_from_model_positions(leader_positions.get(key, []), use_copy_size=False)
    copy_signed = signed_position_from_model_positions(copy_positions.get(key, []), use_copy_size=True)
    lead_side = side_label_from_signed(lead_signed)
    copy_side = side_label_from_signed(copy_signed)
    side_alignment_ok = lead_side == copy_side
    flat_alignment_ok = (lead_side == "FLAT") == (copy_side == "FLAT")
    return {
        "leader_position_side_after": lead_side,
        "copy_position_side_after": copy_side,
        "leader_signed_size_after": round(lead_signed, 12),
        "copy_signed_size_after": round(copy_signed, 12),
        "side_alignment_ok": bool(side_alignment_ok),
        "flat_alignment_ok": bool(flat_alignment_ok),
        "position_alignment_ok": bool(side_alignment_ok and flat_alignment_ok),
    }



def _apply_price_model(price: float, side: str, is_copy: bool) -> float:
    # Poll-only tracked mode: both lead and copy use the same real leader fill price.
    # The fixed model separation is explicit cost only, not an execution-price diff.
    return price


def _lead_cost_bps(ui: Optional[Dict[str, Any]] = None) -> float:
    # Lead and copy share the same base exchange fee model. Raw exchange
    # closed_pnl/fee fields are factual data but are not used for the
    # normalised dashboard model, so fees are applied exactly once here.
    return max(0.0, fnum((ui or {}).get("fee_bps"), DEFAULT_FEE_BPS))


def _copy_friction_bps(ui: Optional[Dict[str, Any]] = None) -> float:
    # User-controlled extra friction applied to COPY ONLY. Default is zero so
    # lead and copy are identical except where the user explicitly adds drag.
    return max(0.0, fnum((ui or {}).get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS))


def _copy_cost_bps(ui: Optional[Dict[str, Any]] = None) -> float:
    return _lead_cost_bps(ui) + _copy_friction_bps(ui)


def _copyable_model_fill(fill: RawFill, has_model_position: bool) -> Optional[RawFill]:
    """Return only the portion of a ledger fill that should be modelled as copied.

    Poll-only tracking can start while the leader already has exchange exposure.
    That baseline exposure was not copied. So the app must not treat a first
    seen baseline reduction/close as a new copied entry.

    Rules:
    - synthetic old position-diff rebuild rows are ignored;
    - if a model position already exists, normal FIFO handles the fill;
    - with no model position and nonzero start_position:
      * reduction/close of baseline exposure is ignored for model PnL;
      * add to baseline exposure models only the incremental added size;
      * flip through flat models only the residual new exposure.
    """
    if fill.reconstructed or fill.rebuild_reason == "position_diff" or fill.source == "rebuild":
        return None

    if has_model_position:
        return fill

    eps = 1e-10
    start = fnum(fill.start_position)
    end = fnum(fill.end_position)

    if abs(start) <= eps:
        return fill

    if start * end >= 0:
        # Same side or flat after fill. If absolute exposure did not increase,
        # this was only reducing/closing baseline exposure.
        if abs(end) <= abs(start) + eps:
            return None
        model_size = abs(end) - abs(start)
        model_side = "BUY" if end > 0 else "SELL"
    else:
        # Crossed through flat. Ignore baseline-close component and model only
        # the residual exposure on the new side.
        model_size = abs(end)
        if model_size <= eps:
            return None
        model_side = "BUY" if end > 0 else "SELL"

    if model_size <= eps:
        return None

    model_delta = model_size if model_side == "BUY" else -model_size
    return replace(
        fill,
        side=model_side,
        size=model_size,
        signed_size_delta=model_delta,
        start_position=0.0,
        end_position=model_delta,
    )


def build_model_state(ui: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ui = ui or load_ui_state()
    truth = load_engine_truth()
    fills = load_raw_fills()
    model_asof = max(fills, key=lambda f: f.timestamp_ms).timestamp_iso if fills else "1970-01-01T00:00:00+00:00"
    wallets = sorted(set([f.wallet for f in fills]) | set((truth.get("wallets") or {}).keys()))
    if USER_WALLET and USER_WALLET not in wallets:
        wallets.insert(0, USER_WALLET)

    gate = load_wallet_gate()
    norm_base = ui["norm_base"]
    user_base = max(1.0, fnum(ui.get("user_norm_base"), norm_base))
    fee_bps = ui["fee_bps"]
    mark_prices: Dict[str, float] = {str(k).upper(): fnum(v) for k, v in (truth.get("mark_prices") or {}).items()} if isinstance(truth.get("mark_prices"), dict) else {}

    models: Dict[str, WalletModel] = {
        w: WalletModel(wallet=w, alloc=wallet_alloc(w, ui), lead_peak=wallet_alloc(w, ui), copy_peak=wallet_alloc(w, ui))
        for w in wallets
    }
    copy_positions: Dict[Tuple[str, str], List[ModelPosition]] = {}
    leader_positions: Dict[Tuple[str, str], List[ModelPosition]] = {}
    trades: List[Dict[str, Any]] = []
    expected_copy_fills: List[Dict[str, Any]] = []
    position_alignment_errors: List[Dict[str, Any]] = []
    trade_seq = 0

    def wallet_model(wallet: str) -> WalletModel:
        alloc_for_wallet = wallet_alloc(wallet, ui)
        return models.setdefault(wallet, WalletModel(wallet=wallet, alloc=alloc_for_wallet, lead_peak=alloc_for_wallet, copy_peak=alloc_for_wallet))

    def copy_wallet_exposure(wallet: str) -> float:
        exposure = 0.0
        for (w, coin), pos_list in copy_positions.items():
            if w != wallet:
                continue
            mark = mark_prices.get(coin, pos_list[-1].entry_price_copy if pos_list else 0.0)
            for p in pos_list:
                exposure += abs(mark * p.copy_size_units) if mark > 0 else abs(p.copy_notional)
        return exposure

    def record_copy_exposure(wallet: str) -> float:
        m = wallet_model(wallet)
        exposure = copy_wallet_exposure(wallet)
        m.current_position_usd = exposure
        m.max_position_usd = max(m.max_position_usd, exposure)
        m.position_exposure_sum += exposure
        m.position_exposure_samples += 1
        return exposure

    def record_entry_notional(wallet: str, notional: float) -> None:
        m = wallet_model(wallet)
        n = abs(fnum(notional))
        if n <= 0:
            return
        m.entry_notional_sum += n
        m.entry_notional_count += 1
        if n >= 10.0:
            m.entry_notional_ge10_count += 1
        m.max_entry_notional_usd = max(m.max_entry_notional_usd, n)

    def append_expected_fill(fill: RawFill, model_action: str, copy_price: float, copy_notional: float, copy_size: float, fee: float, contributes: bool, extra: Optional[Dict[str, Any]] = None) -> None:
        disadv = disadvantage_bps(fill.side, fill.price, copy_price) if contributes else None
        row = {
            "leader_fill_id": fill.fill_id,
            "wallet": fill.wallet,
            "coin": fill.coin,
            "side": fill.side,
            "model_action": model_action,
            "recording_method": fill.recording_method,
            "rebuild_reason": fill.rebuild_reason,
            "leader_price": round(fill.price, 8),
            "copy_price": round(copy_price, 8),
            "copy_price_source": fill.expected_copy_price_source,
            "price_diff": round(copy_price - fill.price, 8),
            "disadvantage_bps": round(disadv, 8) if disadv is not None else None,
            "contributes_to_execution_delta": bool(contributes),
            "timestamp_ms": fill.timestamp_ms,
            "timestamp_iso": fill.timestamp_iso,
            "received_at_ms": fill.received_at_ms,
            "expected_arrival_ms": inum(load_raw_json(fill.raw_json).get("expected_arrival_ms"), 0),
            "latency_ms": fill.latency_ms,
            "copy_notional": round(copy_notional, 8),
            "copy_size": round(copy_size, 12),
            "fee_bps": _copy_cost_bps(ui),
            "base_fee_bps": _lead_cost_bps(ui),
            "copy_friction_bps": _copy_friction_bps(ui),
            "fee": round(fee, 8),
            "reconstructed": fill.reconstructed,
        }
        if extra:
            row.update(extra)
        expected_copy_fills.append(row)

    for fill in fills:
        m = wallet_model(fill.wallet)
        # Raw ledger fills may be baseline closes/reductions that were never
        # copyable events. They can update mark prices, but dashboard counters
        # must count only modelled copy events.
        mark_prices[fill.coin] = fill.price
        key = (fill.wallet, fill.coin)
        copy_positions.setdefault(key, [])
        leader_positions.setdefault(key, [])

        model_fill = _copyable_model_fill(fill, bool(leader_positions[key]))
        if model_fill is None:
            continue
        fill = model_fill

        m.fill_count += 1
        m.ws_fill_count += 1 if fill.recording_method == WS_CAPTURED else 0
        m.poll_fill_count += 1 if fill.source == "poll" else 0
        m.rebuild_fill_count += 1 if fill.recording_method == REBUILD else 0
        m.measured_delta_fill_count += 1 if fill_can_measure_execution_delta(fill) else 0
        if fill.recording_method == WS_CAPTURED:
            m.ws_latency_count += 1
            m.avg_ws_latency_ms = ((m.avg_ws_latency_ms * (m.ws_latency_count - 1)) + fill.latency_ms) / max(1, m.ws_latency_count)
            m.avg_latency_ms = m.avg_ws_latency_ms
        m.last_ts = fill.timestamp_iso

        expected_start_idx = len(expected_copy_fills)
        delta = fill.signed_size_delta if fill.signed_size_delta != 0 else fill.size * sign_from_side(fill.side)
        is_entry = (delta > 0 and not leader_positions[key]) or (delta < 0 and not leader_positions[key]) or (leader_positions[key] and sign_from_side(leader_positions[key][0].side) == sign_from_side(fill.side))

        # Leader PnL is derived from the same selected notional basis as copy PnL.
        # Raw exchange closed_pnl is factual input, but not directly comparable to user-selected fixed/proportional copy sizing.

        if is_entry:
            trade_seq += 1
            copy_notional = model_copy_notional_for_size(fill, ui, fill.size)
            entry_lead_price = fill.price
            entry_copy_price = fill.price
            entry_contributes_delta = False
            entry_disadvantage = None
            copy_size = copy_notional / entry_copy_price if entry_copy_price > 0 else 0.0
            copy_fee = bps_fee(copy_notional, _copy_cost_bps(ui))
            leader_notional_norm = copy_notional
            lead_fee = bps_fee(leader_notional_norm, _lead_cost_bps(ui))
            pos = ModelPosition(
                trade_id=f"T{trade_seq:08d}", wallet=fill.wallet, coin=fill.coin, side=fill.side,
                entry_time_ms=fill.timestamp_ms, entry_time_iso=fill.timestamp_iso,
                entry_price_lead=entry_lead_price, entry_price_copy=entry_copy_price,
                leader_size_units=fill.size, copy_size_units=copy_size,
                leader_notional=leader_notional_norm, copy_notional=copy_notional,
                entry_fee_lead=lead_fee, entry_fee_copy=copy_fee, source=fill.source, latency_ms=fill.latency_ms,
                recording_method=fill.recording_method,
                entry_contributes_to_execution_delta=entry_contributes_delta,
                entry_disadvantage_bps=entry_disadvantage,
                entry_price_source="fixed_poll_model",
            )
            copy_positions[key].append(pos)
            leader_positions[key].append(pos)
            m.entry_count += 1
            record_entry_notional(fill.wallet, copy_notional)
            m.lead_realized -= lead_fee
            m.copy_realized -= copy_fee
            append_expected_fill(fill, "ENTRY", entry_copy_price, copy_notional, copy_size, copy_fee, entry_contributes_delta, {"trade_id": pos.trade_id, "lead_fee": round(lead_fee, 8)})
        else:
            closed_copy, _miss = close_positions_fifo(copy_positions[key], abs(delta))
            closed_leader, _miss_l = close_positions_fifo(leader_positions[key], abs(delta))
            for p, frac in closed_copy:
                exit_copy_price = fill.price
                exit_contributes_delta = False
                exit_disadvantage = None
                exit_fee = bps_fee(p.copy_notional * frac, _copy_cost_bps(ui))
                gross_copy_pnl = calc_unrealized(p.side, p.entry_price_copy, exit_copy_price, p.copy_size_units * frac)
                pnl = gross_copy_pnl - (p.entry_fee_copy * frac) - exit_fee
                m.copy_realized += gross_copy_pnl - exit_fee
                m.exit_count += 1
                if exit_contributes_delta:
                    m.measured_delta_exit_count += 1
                    m.total_exit_disadvantage_bps += fnum(exit_disadvantage)
                if pnl >= 0:
                    m.win_count += 1
                else:
                    m.loss_count += 1
                hold = max(0.0, (fill.timestamp_ms - p.entry_time_ms) / 1000.0)
                leader_norm_size = (p.leader_notional / p.entry_price_lead) if p.entry_price_lead > 0 else 0.0
                wallet_pnl_norm = calc_unrealized(p.side, p.entry_price_lead, fill.price, leader_norm_size * frac) - (p.entry_fee_lead * frac) - bps_fee(p.leader_notional * frac, _lead_cost_bps(ui))
                trade_copy_error = pnl - wallet_pnl_norm
                trades.append({
                    "trade_id": p.trade_id,
                    "wallet": fill.wallet,
                    "coin": fill.coin,
                    "entry_time_iso": p.entry_time_iso,
                    "exit_time_iso": fill.timestamp_iso,
                    "lead_side": p.side,
                    "entry_recording_method": p.recording_method,
                    "exit_recording_method": fill.recording_method,
                    "entry_contributes_to_execution_delta": p.entry_contributes_to_execution_delta,
                    "exit_contributes_to_execution_delta": exit_contributes_delta,
                    "entry_price_source": p.entry_price_source,
                    "exit_price_source": fill.expected_copy_price_source,
                    "entry_price_lead": round(p.entry_price_lead, 8),
                    "entry_price_copy": round(p.entry_price_copy, 8),
                    "exit_price_lead": round(_apply_price_model(fill.price, fill.side, False), 8),
                    "exit_price_copy": round(exit_copy_price, 8),
                    "entry_price_diff": round(p.entry_price_copy - p.entry_price_lead, 8),
                    "exit_price_diff": round(exit_copy_price - fill.price, 8),
                    "size_units": round(p.copy_size_units * frac, 12),
                    "notional_usd": round(p.copy_notional * frac, 8),
                    "holding_seconds": round(hold, 3),
                    "duration": format_duration(hold),
                    "return_pct": round((pnl / (p.copy_notional * frac) * 100.0) if p.copy_notional * frac else 0.0, 6),
                    "entry_latency_ms": p.latency_ms,
                    "exit_latency_ms": fill.latency_ms,
                    "entry_slippage_bps": round(p.entry_disadvantage_bps, 8) if p.entry_disadvantage_bps is not None else None,
                    "exit_slippage_bps": round(exit_disadvantage, 8) if exit_disadvantage is not None else None,
                    "entry_disadvantage_bps": round(p.entry_disadvantage_bps, 8) if p.entry_disadvantage_bps is not None else None,
                    "exit_disadvantage_bps": round(exit_disadvantage, 8) if exit_disadvantage is not None else None,
                    "copy_pnl": round(pnl, 8),
                    "wallet_pnl": round(wallet_pnl_norm, 8),
                    "copy_error": round(trade_copy_error, 8),
                    "copy_efficiency": round((pnl / wallet_pnl_norm) if wallet_pnl_norm else 0.0, 8),
                    "model_mode": effective_wallet_ui(fill.wallet, ui)["copy_mode"],
                    "norm_base": wallet_alloc(fill.wallet, ui),
                })
                append_expected_fill(fill, "EXIT", exit_copy_price, p.copy_notional * frac, p.copy_size_units * frac, exit_fee, exit_contributes_delta, {"trade_id": p.trade_id})
            # Derive leader PnL on the same selected notional basis used for the copy model.
            for p, frac in closed_leader:
                leader_norm_size = (p.leader_notional / p.entry_price_lead) if p.entry_price_lead > 0 else 0.0
                lead_exit_fee = bps_fee(p.leader_notional * frac, _lead_cost_bps(ui))
                m.lead_realized += calc_unrealized(p.side, p.entry_price_lead, fill.price, leader_norm_size * frac) - lead_exit_fee

            if _miss > 1e-10:
                # Same-fill flip support. The model must never remain flat while
                # the leader has flipped, nor invert side against the leader.
                trade_seq += 1
                flip_lead_price = fill.price
                flip_copy_price = fill.price
                flip_contributes_delta = False
                flip_disadvantage = None
                flip_notional = model_copy_notional_for_size(fill, ui, _miss)
                flip_copy_size = flip_notional / flip_copy_price if flip_copy_price > 0 else 0.0
                flip_fee = bps_fee(flip_notional, _copy_cost_bps(ui))
                flip_lead_fee = bps_fee(flip_notional, _lead_cost_bps(ui))
                flip_pos = ModelPosition(
                    trade_id=f"T{trade_seq:08d}", wallet=fill.wallet, coin=fill.coin, side=fill.side,
                    entry_time_ms=fill.timestamp_ms, entry_time_iso=fill.timestamp_iso,
                    entry_price_lead=flip_lead_price, entry_price_copy=flip_copy_price,
                    leader_size_units=_miss, copy_size_units=flip_copy_size,
                    leader_notional=flip_notional, copy_notional=flip_notional,
                    entry_fee_lead=flip_lead_fee, entry_fee_copy=flip_fee, source=fill.source, latency_ms=fill.latency_ms,
                    recording_method=fill.recording_method,
                    entry_contributes_to_execution_delta=flip_contributes_delta,
                    entry_disadvantage_bps=flip_disadvantage,
                    entry_price_source="fixed_poll_model",
                )
                copy_positions[key].append(flip_pos)
                leader_positions[key].append(flip_pos)
                m.entry_count += 1
                record_entry_notional(fill.wallet, flip_notional)
                m.lead_realized -= flip_lead_fee
                m.copy_realized -= flip_fee
                append_expected_fill(fill, "FLIP_ENTRY", flip_copy_price, flip_notional, flip_copy_size, flip_fee, flip_contributes_delta, {"trade_id": flip_pos.trade_id, "lead_fee": round(flip_lead_fee, 8)})

        status = alignment_status_for_key(leader_positions, copy_positions, key)
        for row in expected_copy_fills[expected_start_idx:]:
            row.update(status)
        if not status["position_alignment_ok"]:
            err = {"wallet": fill.wallet, "coin": fill.coin, "fill_id": fill.fill_id, "timestamp_iso": fill.timestamp_iso, **status}
            position_alignment_errors.append(err)
            if "POSITION_ALIGNMENT_ERROR" not in m.flags:
                m.flags.append("POSITION_ALIGNMENT_ERROR")

        # Recompute wallet unrealized after each fill for exact model curve.
        m.lead_unrealized = 0.0
        m.copy_unrealized = 0.0
        for (w, coin), pos_list in leader_positions.items():
            if w != fill.wallet:
                continue
            mark = mark_prices.get(coin, pos_list[-1].entry_price_lead if pos_list else 0.0)
            for p in pos_list:
                leader_norm_size = (p.leader_notional / p.entry_price_lead) if p.entry_price_lead > 0 else 0.0
                m.lead_unrealized += calc_unrealized(p.side, p.entry_price_lead, mark, leader_norm_size)
        for (w, coin), pos_list in copy_positions.items():
            if w != fill.wallet:
                continue
            mark = mark_prices.get(coin, pos_list[-1].entry_price_copy if pos_list else 0.0)
            for p in pos_list:
                m.copy_unrealized += calc_unrealized(p.side, p.entry_price_copy, mark, p.copy_size_units)
        m.open_position_count = sum(len(v) for (w, _c), v in copy_positions.items() if w == fill.wallet)
        current_exposure = record_copy_exposure(fill.wallet)
        m.sync_equity()
        m.curve.append({
            "ts": fill.timestamp_iso,
            "lead_pnl": round(m.lead_realized + m.lead_unrealized, 8),
            "lead_realized": round(m.lead_realized, 8),
            "lead_equity": round(m.lead_equity, 8),
            "lead_drawdown": round(max(0.0, m.lead_peak - m.lead_equity), 8),
            "copy_pnl": round(m.copy_realized + m.copy_unrealized, 8),
            "copy_realized": round(m.copy_realized, 8),
            "copy_equity": round(m.copy_equity, 8),
            "copy_drawdown": round(max(0.0, m.copy_peak - m.copy_equity), 8),
            "open_notional_usd": round(current_exposure, 8),
            # Legacy aliases remain copy-based for existing UI callers.
            "pnl": round(m.copy_realized + m.copy_unrealized, 8),
            "equity": round(m.copy_equity, 8),
            "drawdown": round(max(0.0, m.copy_peak - m.copy_equity), 8),
        })

    for wallet, m in models.items():
        # Ensure quiet wallets still have a valid point.
        m.open_position_count = sum(len(v) for (w, _c), v in copy_positions.items() if w == wallet)
        if m.position_exposure_samples == 0:
            record_copy_exposure(wallet)
        m.sync_equity()
        if not m.curve:
            m.curve.append({
                "ts": model_asof,
                "lead_pnl": 0.0, "lead_realized": 0.0, "lead_equity": m.alloc, "lead_drawdown": 0.0,
                "copy_pnl": 0.0, "copy_realized": 0.0, "copy_equity": m.alloc, "copy_drawdown": 0.0,
                "open_notional_usd": 0.0,
                "pnl": 0.0, "equity": m.alloc, "drawdown": 0.0,
            })
        if m.ws_latency_count > 0 and m.avg_ws_latency_ms > 2000:
            m.flags.append("HIGH_LATENCY")
        if wallet == USER_WALLET:
            gate.setdefault(wallet, {"mode": "OFF", "off_mode": None})

    rows = []
    keys_by_wallet: Dict[str, List[Tuple[str, str]]] = {}
    for k in set(leader_positions.keys()) | set(copy_positions.keys()):
        keys_by_wallet.setdefault(k[0], []).append(k)
    errors_by_wallet: Dict[str, List[Dict[str, Any]]] = {}
    for err in position_alignment_errors:
        errors_by_wallet.setdefault(str(err.get("wallet", "")).lower(), []).append(err)

    for wallet, m in sorted(models.items()):
        lead_dd = max(0.0, m.lead_peak - m.lead_equity)
        copy_dd = max(0.0, m.copy_peak - m.copy_equity)
        delta_eq = m.copy_equity - m.lead_equity
        ws_pct = (m.ws_fill_count / m.fill_count * 100.0) if m.fill_count else 0.0
        wallet_fills = [f for f in fills if f.wallet == wallet]
        ws_without_expected = sum(1 for f in wallet_fills if f.recording_method == WS_CAPTURED and f.expected_copy_price_source == "leader_price_fallback")
        ws_expected_total = sum(1 for f in wallet_fills if f.recording_method == WS_CAPTURED)
        expected_price_coverage_pct = ((ws_expected_total - ws_without_expected) / ws_expected_total * 100.0) if ws_expected_total else 0.0
        wallet_errors = errors_by_wallet.get(wallet, [])
        final_alignment = [alignment_status_for_key(leader_positions, copy_positions, k) for k in keys_by_wallet.get(wallet, [])]
        final_alignment_ok = (not wallet_errors) and all(a.get("position_alignment_ok") for a in final_alignment)
        row_flags = list(m.flags)
        if ws_without_expected and "EXPECTED_COPY_PRICE_MISSING" not in row_flags:
            row_flags.append("EXPECTED_COPY_PRICE_MISSING")
        pnl_hr = m.copy_realized
        first_ts = min((f.timestamp_ms for f in wallet_fills), default=0)
        last_ts = max((f.timestamp_ms for f in wallet_fills), default=0)
        active_hours = 0.0
        if first_ts and last_ts > first_ts:
            active_hours = max(1 / 60, (last_ts - first_ts) / 3_600_000.0)
            pnl_hr = m.copy_realized / active_hours
        avg_position_usd = (m.position_exposure_sum / m.position_exposure_samples) if m.position_exposure_samples else m.current_position_usd
        avg_entry_notional_usd = (m.entry_notional_sum / m.entry_notional_count) if m.entry_notional_count else 0.0
        pct_entries_ge10 = (m.entry_notional_ge10_count / m.entry_notional_count * 100.0) if m.entry_notional_count else 0.0
        required_leverage = (m.max_position_usd / m.alloc) if m.alloc else 0.0
        rows.append({
            "wallet": wallet,
            "is_user_wallet": wallet == USER_WALLET,
            "alloc": m.alloc,
            "lead": block(m.lead_equity, m.lead_realized, m.lead_unrealized, lead_dd, m.lead_max_drawdown, m.lead_peak, m.alloc),
            "copy": block(m.copy_equity, m.copy_realized, m.copy_unrealized, copy_dd, m.copy_max_drawdown, m.copy_peak, m.alloc),
            "delta": {"equity": round(delta_eq, 8), "pct": round((delta_eq / m.alloc * 100.0) if m.alloc else 0.0, 8)},
            "lead_total_pnl": round(m.lead_realized + m.lead_unrealized, 8),
            "copy_total_pnl": round(m.copy_realized + m.copy_unrealized, 8),
            "copy_error": round((m.copy_realized + m.copy_unrealized) - (m.lead_realized + m.lead_unrealized), 8),
            "copy_efficiency": round(((m.copy_realized + m.copy_unrealized) / (m.lead_realized + m.lead_unrealized)) if abs(m.lead_realized + m.lead_unrealized) > 1e-12 else 0.0, 8),
            "position_alignment_ok": bool(final_alignment_ok),
            "position_alignment_errors": wallet_errors,
            "final_position_alignment": final_alignment,
            "ws_captured_without_expected_price_count": ws_without_expected,
            "expected_price_coverage_pct": round(expected_price_coverage_pct, 8),
            "ws_latency_count": m.ws_latency_count,
            "avg_ws_latency_ms": round(m.avg_ws_latency_ms, 3),
            "avg_latency_ms": round(m.avg_ws_latency_ms, 3),
            "entry_count": m.entry_count,
            "exit_count": m.exit_count,
            "open_position_count": m.open_position_count,
            "fill_count": m.fill_count,
            "ws_fill_count": m.ws_fill_count,
            "poll_fill_count": m.poll_fill_count,
            "ws_coverage": round(ws_pct, 6),
            "rebuild_fill_count": m.rebuild_fill_count,
            "measured_delta_fill_count": m.measured_delta_fill_count,
            "measured_delta_entry_count": m.measured_delta_entry_count,
            "measured_delta_exit_count": m.measured_delta_exit_count,
            "avg_entry_disadvantage_bps": round((m.total_entry_disadvantage_bps / m.measured_delta_entry_count) if m.measured_delta_entry_count else 0.0, 8),
            "avg_exit_disadvantage_bps": round((m.total_exit_disadvantage_bps / m.measured_delta_exit_count) if m.measured_delta_exit_count else 0.0, 8),
            "win_rate": round((m.win_count / max(1, m.win_count + m.loss_count) * 100.0) if (m.win_count + m.loss_count) else 0.0, 6),
            "pnl_per_hour": round(pnl_hr, 8),
            "active_hours": round(active_hours, 8),
            "pnl_per_trade": round((m.copy_realized / m.exit_count) if m.exit_count else 0.0, 8),
            "current_position_usd": round(m.current_position_usd, 8),
            "avg_position_usd": round(avg_position_usd, 8),
            "max_position_usd": round(m.max_position_usd, 8),
            "position_exposure_sum": round(m.position_exposure_sum, 8),
            "position_exposure_samples": m.position_exposure_samples,
            "avg_entry_notional_usd": round(avg_entry_notional_usd, 8),
            "max_entry_notional_usd": round(m.max_entry_notional_usd, 8),
            "entry_notional_sum": round(m.entry_notional_sum, 8),
            "entry_notional_count": m.entry_notional_count,
            "entry_notional_ge10_count": m.entry_notional_ge10_count,
            "pct_entries_ge10": round(pct_entries_ge10, 8),
            "required_leverage": round(required_leverage, 8),
            "wallet_config": (ui.get("wallet_config") or {}).get(wallet, {}),
            "include_in_portfolio": wallet_included(wallet, ui),
            "effective_copy_mode": effective_wallet_ui(wallet, ui).get("copy_mode"),
            "effective_norm_base": effective_wallet_ui(wallet, ui).get("norm_base"),
            "effective_fixed_notional": effective_wallet_ui(wallet, ui).get("fixed_notional"),
            "flags": row_flags,
            "gate": gate.get(wallet, {"mode": "OFF", "off_mode": None}),
            "curve": m.curve[-EQUITY_HISTORY_MAX:],
        })

    trade_returns_by_wallet: Dict[str, List[float]] = {}
    for t in trades:
        w = str(t.get("wallet", "")).lower()
        if not w:
            continue
        trade_returns_by_wallet.setdefault(w, []).append(fnum(t.get("return_pct")))
    for r in rows:
        rs = trade_returns_by_wallet.get(str(r.get("wallet", "")).lower(), [])
        r["avg_trade_pct"] = round(avg(rs), 8) if rs else 0.0

    # User wallet excluded from combined portfolio. Optional INC toggle excludes
    # a wallet from combined graph/header aggregation only, not from tracking.
    portfolio_wallets = [r for r in rows if not r["is_user_wallet"] and r.get("include_in_portfolio", True)]
    # Single canonical aggregate — header, USER row, and validator all read from here.
    sa = selected_aggregate(portfolio_wallets, trades, user_base)
    alloc = sum(fnum(r["alloc"]) for r in portfolio_wallets)
    lead_equity = sa["lead_real"] + sa["lead_unreal"] + alloc
    copy_equity = sa["copy_real"] + sa["copy_unreal"] + alloc
    lead_real  = sa["lead_real"];  lead_unreal  = sa["lead_unreal"]
    copy_real  = sa["copy_real"];  copy_unreal  = sa["copy_unreal"]
    open_notional_usd = sa["current_position_usd"]
    # Current DD: sum of live wallet block DD (not curve tail or peak-equity derivation).
    lead_live_dd = sum(fnum((r.get("lead") or {}).get("drawdown")) for r in portfolio_wallets)
    copy_live_dd = sum(fnum((r.get("copy") or {}).get("drawdown")) for r in portfolio_wallets)
    # Rebuild combined graph by timestamp using latest wallet lead/copy equity at each event.
    portfolio_history = build_portfolio_history(rows, ui)
    # Append live snapshot as tail so graph endpoint reconciles with header current DD.
    _live_snap = {
        "ts": model_asof,
        "alloc": round(alloc, 8),
        "equity": round(copy_equity, 8),
        "realized": round(copy_real, 8),
        "unrealized": round(copy_unreal, 8),
        "peak_equity": round(copy_equity + copy_live_dd, 8),
        "drawdown_usd": round(copy_live_dd, 8),
        "drawdown_pct": round((copy_live_dd / alloc * 100.0) if alloc else 0.0, 8),
        "open_notional_usd": round(open_notional_usd, 8),
        "lead": {
            "alloc": round(alloc, 8), "equity": round(lead_equity, 8),
            "realized": round(lead_real, 8), "unrealized": round(lead_unreal, 8),
            "drawdown": round(lead_live_dd, 8),
            "drawdown_usd": round(lead_live_dd, 8),
            "drawdown_pct": round((lead_live_dd / alloc * 100.0) if alloc else 0.0, 8),
            "peak_equity": round(lead_equity + lead_live_dd, 8),
        },
        "copy": {
            "alloc": round(alloc, 8), "equity": round(copy_equity, 8),
            "realized": round(copy_real, 8), "unrealized": round(copy_unreal, 8),
            "drawdown": round(copy_live_dd, 8),
            "drawdown_usd": round(copy_live_dd, 8),
            "drawdown_pct": round((copy_live_dd / alloc * 100.0) if alloc else 0.0, 8),
            "peak_equity": round(copy_equity + copy_live_dd, 8),
        },
        "delta": {
            "equity": round(copy_equity - lead_equity, 8),
            "pct": round(((copy_equity - lead_equity) / alloc * 100.0) if alloc else 0.0, 8),
        },
    }
    if portfolio_history and portfolio_history[-1].get("ts") == model_asof:
        portfolio_history[-1] = _live_snap
    else:
        portfolio_history = portfolio_history + [_live_snap]
    # Max DD and max exposure: max timestamped sum across full history including live tail.
    max_open_notional_usd = max((fnum(p.get("open_notional_usd")) for p in portfolio_history), default=open_notional_usd)
    lead_maxdd = max((fnum((p.get("lead") or {}).get("drawdown")) for p in portfolio_history), default=lead_live_dd)
    copy_maxdd = max((fnum((p.get("copy") or {}).get("drawdown")) for p in portfolio_history), default=copy_live_dd)
    portfolio = {
        "ts": model_asof,
        "lead": block(lead_equity, lead_real, lead_unreal, lead_live_dd, lead_maxdd, lead_equity + lead_live_dd, alloc),
        "copy": block(copy_equity, copy_real, copy_unreal, copy_live_dd, copy_maxdd, copy_equity + copy_live_dd, alloc),
        "delta": {"equity": round(copy_equity - lead_equity, 8), "realized": round(copy_real - lead_real, 8), "pct": round(((copy_equity - lead_equity) / alloc * 100.0) if alloc else 0.0, 8)},
        "open_notional_usd": round(open_notional_usd, 8),
        "max_open_notional_usd": round(max_open_notional_usd, 8),
        "avg_position_usd": round(sa["avg_position_usd"], 8),
        # max_required_leverage uses same formula as USER row so header matches.
        "max_required_leverage": round(sa["required_leverage"], 8),
        "avg_trade_pct": round(sa["avg_trade_pct"], 8),
        "win_rate": round(sa["win_rate"], 6),
        "avg_entry_notional_usd": round(sa["avg_entry_notional_usd"], 8),
        "pct_entries_ge10": round(sa["pct_entries_ge10"], 8),
        "position_alignment_errors": position_alignment_errors,
        "position_alignment_ok": not position_alignment_errors,
    }


    # USER row is a display-only aggregate of all tracked non-user copy models.
    # It uses user_norm_base only for the aggregate/header display denominator.
    user_row = next((r for r in rows if r.get("is_user_wallet")), None)
    if user_row is not None:
        total_lead_pnl = lead_real + lead_unreal
        total_copy_pnl = copy_real + copy_unreal
        total_exits = sa["exit_count"]

        user_curve: List[Dict[str, Any]] = []
        for point in portfolio_history:
            p_alloc = fnum(point.get("alloc"), alloc)
            lead_point = point.get("lead", {}) if isinstance(point.get("lead"), dict) else {}
            copy_point = point.get("copy", {}) if isinstance(point.get("copy"), dict) else {}
            lead_pnl_point = fnum(lead_point.get("equity")) - p_alloc
            copy_pnl_point = fnum(copy_point.get("equity")) - p_alloc
            u_lead_eq = user_base + lead_pnl_point
            u_copy_eq = user_base + copy_pnl_point
            # Use stored DD from portfolio_history — it tracks rolling peak across
            # ALL events (including same-timestamp intermediates) so it is accurate.
            # Re-deriving from compacted equity values would miss intra-timestamp peaks.
            u_lead_dd = max(0.0, fnum(lead_point.get("drawdown", lead_point.get("drawdown_usd", 0.0))))
            u_copy_dd = max(0.0, fnum(copy_point.get("drawdown", copy_point.get("drawdown_usd", 0.0))))
            user_curve.append({
                "ts": point.get("ts", model_asof),
                "lead_pnl": round(lead_pnl_point, 8),
                "lead_equity": round(u_lead_eq, 8),
                "lead_drawdown": round(u_lead_dd, 8),
                "copy_pnl": round(copy_pnl_point, 8),
                "copy_equity": round(u_copy_eq, 8),
                "copy_drawdown": round(u_copy_dd, 8),
                "pnl": round(copy_pnl_point, 8),
                "equity": round(u_copy_eq, 8),
                "drawdown": round(u_copy_dd, 8),
            })
        if not user_curve:
            user_curve.append({
                "ts": model_asof,
                "lead_pnl": round(total_lead_pnl, 8),
                "lead_equity": round(user_base + total_lead_pnl, 8),
                "lead_drawdown": max(0.0, -total_lead_pnl),
                "copy_pnl": round(total_copy_pnl, 8),
                "copy_equity": round(user_base + total_copy_pnl, 8),
                "copy_drawdown": max(0.0, -total_copy_pnl),
                "pnl": round(total_copy_pnl, 8),
                "equity": round(user_base + total_copy_pnl, 8),
                "drawdown": max(0.0, -total_copy_pnl),
            })

        u_lead_equity = user_base + total_lead_pnl
        u_copy_equity = user_base + total_copy_pnl
        # Current DD: use the same lead_live_dd / copy_live_dd that the portfolio
        # header uses, so header and USER row always match on current DD.
        u_lead_dd = lead_live_dd
        u_copy_dd = copy_live_dd
        u_lead_peak = u_lead_equity + u_lead_dd
        u_copy_peak = u_copy_equity + u_copy_dd
        # MaxDD: max stored DD across all user_curve points (uses stored DD from
        # build_portfolio_history which tracks the full event-level rolling peak).
        u_lead_maxdd = max((fnum(p.get("lead_drawdown")) for p in user_curve), default=u_lead_dd)
        u_copy_maxdd = max((fnum(p.get("copy_drawdown")) for p in user_curve), default=u_copy_dd)

        user_row.update({
            "alloc": user_base,
            "lead": block(u_lead_equity, lead_real, lead_unreal, u_lead_dd, u_lead_maxdd, u_lead_peak, user_base),
            "copy": block(u_copy_equity, copy_real, copy_unreal, u_copy_dd, u_copy_maxdd, u_copy_peak, user_base),
            "delta": {"equity": round(total_copy_pnl - total_lead_pnl, 8), "pct": round(((total_copy_pnl - total_lead_pnl) / user_base * 100.0) if user_base else 0.0, 8)},
            "lead_total_pnl": round(total_lead_pnl, 8),
            "copy_total_pnl": round(total_copy_pnl, 8),
            "copy_error": round(total_copy_pnl - total_lead_pnl, 8),
            "copy_efficiency": round((total_copy_pnl / total_lead_pnl) if abs(total_lead_pnl) > 1e-12 else 0.0, 8),
            "position_alignment_ok": bool(portfolio.get("position_alignment_ok", True)),
            "position_alignment_errors": portfolio.get("position_alignment_errors", []),
            "final_position_alignment": [],
            "ws_captured_without_expected_price_count": 0,
            "expected_price_coverage_pct": 0.0,
            "ws_latency_count": 0,
            "avg_ws_latency_ms": 0.0,
            "avg_latency_ms": 0.0,
            "entry_count": sa["entry_count"],
            "exit_count": sa["exit_count"],
            "open_position_count": sa["open_position_count"],
            "fill_count": sa["fill_count"],
            "ws_fill_count": 0,
            "poll_fill_count": sum(int(r.get("poll_fill_count") or 0) for r in portfolio_wallets),
            "ws_coverage": 0.0,
            "rebuild_fill_count": sum(int(r.get("rebuild_fill_count") or 0) for r in portfolio_wallets),
            "measured_delta_fill_count": 0,
            "measured_delta_entry_count": 0,
            "measured_delta_exit_count": 0,
            "avg_entry_disadvantage_bps": 0.0,
            "avg_exit_disadvantage_bps": 0.0,
            "win_rate": sa["win_rate"],
            "pnl_per_hour": round(sum(fnum(r.get("pnl_per_hour")) for r in portfolio_wallets), 8),
            "active_hours": round(sum(fnum(r.get("active_hours")) for r in portfolio_wallets), 8),
            "pnl_per_trade": sa["pnl_per_trade"],
            "avg_trade_pct": sa["avg_trade_pct"],
            "current_position_usd": sa["current_position_usd"],
            "avg_position_usd": sa["avg_position_usd"],
            "max_position_usd": sa["max_position_usd"],
            "avg_entry_notional_usd": sa["avg_entry_notional_usd"],
            "entry_notional_sum": sa["entry_notional_sum"],
            "entry_notional_count": sa["entry_notional_count"],
            "entry_notional_ge10_count": sa["entry_notional_ge10_count"],
            "pct_entries_ge10": sa["pct_entries_ge10"],
            "required_leverage": sa["required_leverage"],
            "flags": [] if portfolio.get("position_alignment_ok", True) else ["POSITION_ALIGNMENT_ERROR"],
            "gate": {"mode": "USER_AGGREGATE", "off_mode": None},
            "curve": user_curve[-EQUITY_HISTORY_MAX:],
        })

    state = {
        "live_config": load_live_config(),
        "schema": "app_model_state.v1.derived_only",
        "updated_at": utc_now_iso(),
        "model_asof": model_asof,
        "source_schema": truth.get("schema", "unknown"),
        "ui_state": ui,
        "user_wallet": USER_WALLET,
        "wallets": {r["wallet"]: r for r in rows},
        "wallet_rows": rows,
        "portfolio": portfolio,
        "portfolio_history": portfolio_history,
        "copy_trades": trades,
        "expected_copy_fills": expected_copy_fills,
        "position_alignment_errors": position_alignment_errors,
        "position_alignment_ok": not position_alignment_errors,
        "engine_truth_boundary": "app-derived only; engine truth is not mutated",
    }
    persist_model_state(state)
    return state


def block(equity: float, realized: float, unrealized: float, dd: float, maxdd: float, peak: float, alloc: float) -> Dict[str, float]:
    return {
        "alloc": round(alloc, 8),
        "equity": round(equity, 8),
        "realized": round(realized, 8),
        "realised": round(realized, 8),
        "unrealized": round(unrealized, 8),
        "unrealised": round(unrealized, 8),
        "drawdown": round(dd, 8),
        "drawdown_usd": round(dd, 8),
        "drawdown_pct": round((dd / alloc * 100.0) if alloc else 0.0, 8),
        "max_drawdown": round(maxdd, 8),
        "maxdd": round(maxdd, 8),
        "max_drawdown_pct": round((maxdd / alloc * 100.0) if alloc else 0.0, 8),
        "peak": round(peak, 8),
        "peak_equity": round(peak, 8),
    }


def selected_aggregate(portfolio_wallets: List[Dict[str, Any]], trades: List[Dict[str, Any]], norm_base: float) -> Dict[str, Any]:
    """One canonical aggregate of all selected (included, non-user) wallets.

    Header cards, USER row, and validate_render_contract must all read from
    the same aggregate so they are guaranteed to match.
    """
    included_wallets = {str(r.get("wallet", "")).lower() for r in portfolio_wallets}
    sel_trades = [t for t in trades if str(t.get("wallet", "")).lower() in included_wallets]

    lead_real   = sum(fnum((r.get("lead") or {}).get("realized"))   for r in portfolio_wallets)
    lead_unreal = sum(fnum((r.get("lead") or {}).get("unrealized")) for r in portfolio_wallets)
    copy_real   = sum(fnum((r.get("copy") or {}).get("realized"))   for r in portfolio_wallets)
    copy_unreal = sum(fnum((r.get("copy") or {}).get("unrealized")) for r in portfolio_wallets)

    fill_count          = sum(inum(r.get("fill_count"))          for r in portfolio_wallets)
    exit_count          = sum(inum(r.get("exit_count"))          for r in portfolio_wallets)
    entry_count         = sum(inum(r.get("entry_count"))         for r in portfolio_wallets)
    open_position_count = sum(inum(r.get("open_position_count")) for r in portfolio_wallets)
    curve_stats = selected_combined_curve_stats(portfolio_wallets)
    current_position_usd = curve_stats["current_exposure"] if curve_stats["has_points"] else sum(fnum(r.get("current_position_usd")) for r in portfolio_wallets)
    max_position_usd = curve_stats["max_exposure"] if curve_stats["has_points"] else current_position_usd

    pos_exp_sum     = sum(fnum(r.get("position_exposure_sum", 0)) for r in portfolio_wallets)
    pos_exp_samples = sum(inum(r.get("position_exposure_samples", 0)) for r in portfolio_wallets)
    ent_not_sum     = sum(fnum(r.get("entry_notional_sum", 0)) for r in portfolio_wallets)
    ent_not_count   = sum(inum(r.get("entry_notional_count", 0)) for r in portfolio_wallets)
    ent_not_ge10    = sum(inum(r.get("entry_notional_ge10_count", 0)) for r in portfolio_wallets)

    avg_position_usd     = (pos_exp_sum / pos_exp_samples) if pos_exp_samples > 0 else sum(
        fnum(r.get("avg_position_usd")) for r in portfolio_wallets if fnum(r.get("avg_position_usd")) > 0
    ) / max(1, sum(1 for r in portfolio_wallets if fnum(r.get("avg_position_usd")) > 0)) if any(
        fnum(r.get("avg_position_usd")) > 0 for r in portfolio_wallets
    ) else 0.0
    avg_entry_notional_usd = (ent_not_sum / ent_not_count) if ent_not_count > 0 else 0.0
    pct_entries_ge10       = (ent_not_ge10 / ent_not_count * 100.0) if ent_not_count > 0 else 0.0

    wins     = sum(1 for t in sel_trades if fnum(t.get("copy_pnl")) >= 0)
    win_rate = (wins / len(sel_trades) * 100.0) if sel_trades else 0.0

    returns       = [fnum(t.get("return_pct")) for t in sel_trades]
    avg_trade_pct = (sum(returns) / len(returns)) if returns else 0.0

    pnl_per_trade    = (copy_real / exit_count) if exit_count > 0 else 0.0
    required_leverage = (max_position_usd / norm_base) if norm_base > 0 else 0.0

    return {
        "lead_real": round(lead_real, 8), "lead_unreal": round(lead_unreal, 8),
        "copy_real": round(copy_real, 8), "copy_unreal": round(copy_unreal, 8),
        "fill_count": fill_count, "exit_count": exit_count,
        "entry_count": entry_count, "open_position_count": open_position_count,
        "current_position_usd": round(current_position_usd, 8),
        "max_position_usd": round(max_position_usd, 8),
        "position_exposure_sum": round(pos_exp_sum, 8),
        "position_exposure_samples": pos_exp_samples,
        "entry_notional_sum": round(ent_not_sum, 8),
        "entry_notional_count": ent_not_count,
        "entry_notional_ge10_count": ent_not_ge10,
        "avg_position_usd": round(avg_position_usd, 8),
        "avg_entry_notional_usd": round(avg_entry_notional_usd, 8),
        "pct_entries_ge10": round(pct_entries_ge10, 8),
        "win_rate": round(win_rate, 6),
        "wins": wins, "total_trades": len(sel_trades),
        "avg_trade_pct": round(avg_trade_pct, 8),
        "pnl_per_trade": round(pnl_per_trade, 8),
        "required_leverage": round(required_leverage, 8),
        "selected_trades": sel_trades,
    }


def selected_combined_curve_stats(portfolio_rows: List[Dict[str, Any]]) -> Dict[str, float]:
    events: List[Tuple[str, str, Dict[str, Any]]] = []
    for r in portfolio_rows:
        wallet = str(r.get("wallet", ""))
        curve = r.get("curve", [])
        for p in curve if isinstance(curve, list) else []:
            ts = str(p.get("ts", ""))
            if ts:
                events.append((ts, wallet, p))
    events.sort(key=lambda x: x[0])
    latest: Dict[str, Dict[str, Any]] = {}
    current_lead_dd = current_copy_dd = current_exposure = 0.0
    max_lead_dd = max_copy_dd = max_exposure = 0.0
    for _ts, wallet, point in events:
        latest[wallet] = point
        current_lead_dd = sum(fnum(v.get("lead_drawdown")) for v in latest.values())
        current_copy_dd = sum(fnum(v.get("copy_drawdown")) for v in latest.values())
        current_exposure = sum(fnum(v.get("open_notional_usd")) for v in latest.values())
        max_lead_dd = max(max_lead_dd, current_lead_dd)
        max_copy_dd = max(max_copy_dd, current_copy_dd)
        max_exposure = max(max_exposure, current_exposure)
    return {
        "has_points": bool(events),
        "current_lead_dd": round(current_lead_dd, 8),
        "current_copy_dd": round(current_copy_dd, 8),
        "max_lead_dd": round(max_lead_dd, 8),
        "max_copy_dd": round(max_copy_dd, 8),
        "current_exposure": round(current_exposure, 8),
        "max_exposure": round(max_exposure, 8),
    }


def build_portfolio_history(rows: List[Dict[str, Any]], ui: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    events: List[Tuple[str, str, Dict[str, Any]]] = []
    ui = ui or {}
    portfolio_rows = [
        r for r in rows
        if not r.get("is_user_wallet") and bool(r.get("include_in_portfolio", wallet_included(str(r.get("wallet", "")), ui)))
    ]
    wallet_allocs = {str(r.get("wallet")): fnum(r.get("alloc"), DEFAULT_NORM_BASE) for r in portfolio_rows}
    for r in portfolio_rows:
        for p in r.get("curve", []):
            events.append((str(p.get("ts", "")), str(r["wallet"]), p))
    events.sort(key=lambda x: x[0])
    latest: Dict[str, Dict[str, Any]] = {}
    out: List[Dict[str, Any]] = []
    alloc = sum(wallet_allocs.values())
    lead_peak = alloc
    copy_peak = alloc
    for ts, wallet, point in events:
        if not ts:
            continue
        latest[wallet] = point
        lead_pnl = sum(fnum(v.get("lead_pnl", v.get("pnl"))) for v in latest.values())
        copy_pnl = sum(fnum(v.get("copy_pnl", v.get("pnl"))) for v in latest.values())
        lead_realized = sum(fnum(v.get("lead_realized", v.get("lead_pnl", v.get("pnl")))) for v in latest.values())
        copy_realized = sum(fnum(v.get("copy_realized", v.get("copy_pnl", v.get("pnl")))) for v in latest.values())
        open_notional_usd = sum(fnum(v.get("open_notional_usd")) for v in latest.values())
        lead_equity = alloc + lead_pnl
        copy_equity = alloc + copy_pnl
        lead_dd = sum(fnum(v.get("lead_drawdown")) for v in latest.values())
        copy_dd = sum(fnum(v.get("copy_drawdown")) for v in latest.values())
        lead_peak = lead_equity + lead_dd
        copy_peak = copy_equity + copy_dd
        delta_eq = copy_equity - lead_equity
        out.append({
            "ts": ts,
            "alloc": round(alloc, 8),
            # Legacy top-level remains copy-based.
            "equity": round(copy_equity, 8),
            "realized": round(copy_realized, 8),
            "unrealized": round(copy_pnl - copy_realized, 8),
            "peak_equity": round(copy_peak, 8),
            "drawdown_usd": round(copy_dd, 8),
            "drawdown_pct": round((copy_dd / alloc * 100.0) if alloc else 0.0, 8),
            "open_notional_usd": round(open_notional_usd, 8),
            "lead": {
                "alloc": round(alloc, 8), "equity": round(lead_equity, 8),
                "realized": round(lead_realized, 8), "unrealized": round(lead_pnl - lead_realized, 8),
                "drawdown": round(lead_dd, 8),
                "drawdown_usd": round(lead_dd, 8),
                "drawdown_pct": round((lead_dd / alloc * 100.0) if alloc else 0.0, 8),
                "peak_equity": round(lead_peak, 8),
            },
            "copy": {
                "alloc": round(alloc, 8), "equity": round(copy_equity, 8),
                "realized": round(copy_realized, 8), "unrealized": round(copy_pnl - copy_realized, 8),
                "drawdown": round(copy_dd, 8),
                "drawdown_usd": round(copy_dd, 8),
                "drawdown_pct": round((copy_dd / alloc * 100.0) if alloc else 0.0, 8),
                "peak_equity": round(copy_peak, 8),
            },
            "delta": {
                "equity": round(delta_eq, 8),
                "pct": round((delta_eq / alloc * 100.0) if alloc else 0.0, 8),
            },
        })
    return compact_history(out)

def compact_history(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    last_ts = None
    for p in points:
        ts = p.get("ts")
        if ts == last_ts and out:
            out[-1] = p
        else:
            out.append(p)
            last_ts = ts
    return out[-EQUITY_HISTORY_MAX:]


def persist_model_state(state: Dict[str, Any]) -> None:
    atomic_write_json(APP_MODEL_STATE_JSON, state)
    atomic_write_json(PORTFOLIO_HISTORY_FILE, state.get("portfolio_history", []))
    equity = {w: r.get("curve", []) for w, r in (state.get("wallets") or {}).items()}
    atomic_write_json(EQUITY_HISTORY_FILE, equity)
    expected_fields = [
        "leader_fill_id", "wallet", "coin", "side", "model_action",
        "recording_method", "rebuild_reason", "leader_price", "copy_price",
        "copy_price_source", "price_diff", "disadvantage_bps",
        "contributes_to_execution_delta", "timestamp_ms", "timestamp_iso",
        "received_at_ms", "expected_arrival_ms", "latency_ms", "copy_notional",
        "copy_size", "fee_bps", "base_fee_bps", "copy_friction_bps", "fee", "reconstructed", "trade_id",
        "leader_position_side_after", "copy_position_side_after",
        "leader_signed_size_after", "copy_signed_size_after",
        "side_alignment_ok", "flat_alignment_ok", "position_alignment_ok",
    ]
    atomic_write_csv(EXPECTED_COPY_FILLS_CSV, expected_fields, state.get("expected_copy_fills", []))
    fields = [
        "trade_id", "wallet", "coin", "entry_time_iso", "exit_time_iso", "lead_side",
        "entry_recording_method", "exit_recording_method",
        "entry_contributes_to_execution_delta", "exit_contributes_to_execution_delta",
        "entry_price_source", "exit_price_source",
        "entry_price_lead", "entry_price_copy", "exit_price_lead", "exit_price_copy",
        "entry_price_diff", "exit_price_diff",
        "size_units", "notional_usd", "holding_seconds", "duration", "return_pct",
        "entry_latency_ms", "exit_latency_ms",
        "entry_slippage_bps", "exit_slippage_bps",
        "entry_disadvantage_bps", "exit_disadvantage_bps",
        "copy_pnl", "wallet_pnl", "copy_error", "copy_efficiency", "model_mode", "norm_base",
    ]
    atomic_write_csv(COPY_TRADES_CSV, fields, state.get("copy_trades", []))
    APP_HEALTH["last_persist_at"] = utc_now_iso()


def invalidate_model_cache() -> None:
    with _MODEL_BUILD_LOCK:
        _MODEL_CACHE["state"] = None
        _MODEL_CACHE["built_at"] = 0.0


def get_model_state_cached(max_age_sec: float = 5.0, force: bool = False) -> Dict[str, Any]:
    now = time.time()
    with _MODEL_BUILD_LOCK:
        cached = _MODEL_CACHE.get("state")
        built_at = fnum(_MODEL_CACHE.get("built_at"))
        if cached is not None and not force and (now - built_at) <= max_age_sec:
            APP_HEALTH["cache_hits"] = int(APP_HEALTH.get("cache_hits", 0)) + 1
            cached = dict(cached)
            cached["ui_state"] = load_ui_state()
            cached["wallet_rows"] = sorted_rows(cached)
            return cached
        started = time.time()
        APP_HEALTH["last_build_started_at"] = utc_now_iso()
        try:
            state = build_model_state()
            _MODEL_CACHE["state"] = state
            _MODEL_CACHE["built_at"] = time.time()
            APP_HEALTH["last_build_finished_at"] = utc_now_iso()
            APP_HEALTH["last_build_seconds"] = round(time.time() - started, 4)
            APP_HEALTH["last_error"] = ""
            APP_HEALTH["build_count"] = int(APP_HEALTH.get("build_count", 0)) + 1
            fresh = dict(state)
            fresh["ui_state"] = load_ui_state()
            fresh["wallet_rows"] = sorted_rows(fresh)
            return fresh
        except Exception as exc:
            APP_HEALTH["last_error"] = f"{type(exc).__name__}: {exc}"
            APP_HEALTH["last_build_seconds"] = round(time.time() - started, 4)
            if cached is not None:
                stale = dict(cached)
                stale["ui_state"] = load_ui_state()
                stale["wallet_rows"] = sorted_rows(stale)
                stale["app_health_stale"] = True
                return stale
            raise


def health_status_label() -> Tuple[str, str]:
    err = str(APP_HEALTH.get("last_error") or "")
    build_s = fnum(APP_HEALTH.get("last_build_seconds"))
    if err:
        return "🔴 ERROR", err[:80]
    if build_s > 4.0:
        return "🟡 SLOW", f"build {build_s:.2f}s"
    return "🟢 OK", f"build {build_s:.2f}s"


def format_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60}s"
    return f"{s // 3600}h{(s % 3600) // 60}m"


def money(v: Any, decimals: int = 2) -> str:
    x = fnum(v)
    sign = "+" if x > 0 else "-" if x < 0 else ""
    return f"{sign}${abs(x):,.{decimals}f}"


def pct(v: Any, decimals: int = 1) -> str:
    return f"{fnum(v):.{decimals}f}%"


def dual(v: float, base: float, decimals: int = 2) -> str:
    return f"{money(v, decimals)} ({(v / base * 100.0) if base else 0.0:+.2f}%)"


def has_num(v: Any) -> bool:
    if v is None or v == "" or isinstance(v, bool):
        return False
    try:
        return math.isfinite(float(v))
    except Exception:
        return False


def money_or_dash(v: Any, decimals: int = 2) -> str:
    return money(v, decimals) if has_num(v) else "—"


def pct_or_dash(v: Any, decimals: int = 1) -> str:
    return pct(v, decimals) if has_num(v) else "—"


def dual_or_dash(v: Any, base: float, decimals: int = 2) -> str:
    return dual(fnum(v), base, decimals) if has_num(v) else "—"


def x_or_dash(v: Any, decimals: int = 2) -> str:
    return f"{fnum(v):.{decimals}f}x" if has_num(v) else "—"


def is_present_num(v: Any) -> bool:
    return has_num(v)


def fmt_money_or_dash(v: Any, decimals: int = 2) -> str:
    return money(v, decimals) if is_present_num(v) else '<span class="missing">—</span>'


def fmt_pct_or_dash(v: Any, decimals: int = 1) -> str:
    return pct(v, decimals) if is_present_num(v) else '<span class="missing">—</span>'


def fmt_dual_or_dash(value: Any, alloc: float, decimals: int = 2) -> str:
    return dual(fnum(value), alloc, decimals) if is_present_num(value) else '<span class="missing">—</span>'


def fmt_x_or_dash(v: Any, decimals: int = 2) -> str:
    return f"{fnum(v):.{decimals}f}x" if is_present_num(v) else '<span class="missing">—</span>'


def td_num(sort_value: Any, content: str, cls: str = "") -> str:
    if not has_num(sort_value):
        cell_cls = f"{cls} missing muted".strip()
        return f'<td class="{cell_cls}" data-sort="-999999999">—</td>'
    return f'<td class="{cls}" data-sort="{fnum(sort_value):.12g}">{content}</td>'


def active_wallet_row(row: Dict[str, Any]) -> bool:
    return any(inum(row.get(k), 0) > 0 for k in ("fill_count", "entry_count", "exit_count", "open_position_count"))


def active_wallet(row: Dict[str, Any]) -> bool:
    return active_wallet_row(row)


def core_missing(row: Dict[str, Any], field: str) -> str:
    flags = row.setdefault("flags", [])
    labelled = f"DATA_FIELD_MISSING:{field}" if field else "DATA_FIELD_MISSING"
    if isinstance(flags, list):
        if "DATA_FIELD_MISSING" not in flags:
            flags.append("DATA_FIELD_MISSING")
        if labelled not in flags:
            flags.append(labelled)
    safe = html.escape(field)
    return f'<span class="badge neg" title="Missing core data field: {safe}">WIRE_ERR</span>'


def required_num_or_wire_err(row: Dict[str, Any], label: str, value: Any) -> Tuple[bool, float, str]:
    if has_num(value):
        return True, fnum(value), ""
    if active_wallet(row):
        return False, 0.0, core_missing(row, label)
    return True, 0.0, ""


def core_td(row: Dict[str, Any], present: bool, sort_value: Any, content: str, cls: str = "", field: str = "") -> str:
    if active_wallet(row) and (not present or not has_num(sort_value)):
        return f'<td class="{cls} neg" data-sort="-999999999">{core_missing(row, field)}</td>'
    return f'<td class="{cls}" data-sort="{fnum(sort_value):.12g}">{content}</td>'


def dash_td(cls: str = "", field: str = "") -> str:
    attr = f' data-field="{html.escape(field)}"' if field else ""
    return f'<td class="{cls} missing muted" data-sort="-999999999"{attr}>—</td>'


def money_cell_core(row: Dict[str, Any], label: str, value: Any, cls: str = "") -> str:
    ok, num, err = required_num_or_wire_err(row, label, value)
    return f'<td class="{cls} {"neg" if not ok else ""}" data-sort="{num:.12g}">{err if not ok else money(num)}</td>'


def pct_cell_core(row: Dict[str, Any], label: str, value: Any, cls: str = "", decimals: int = 1) -> str:
    ok, num, err = required_num_or_wire_err(row, label, value)
    return f'<td class="{cls} {"neg" if not ok else ""}" data-sort="{num:.12g}">{err if not ok else pct(num, decimals)}</td>'


def dual_cell_core(row: Dict[str, Any], label: str, value: Any, base: float, cls: str = "", decimals: int = 2) -> str:
    ok, num, err = required_num_or_wire_err(row, label, value)
    return f'<td class="{cls} {"neg" if not ok else ""}" data-sort="{num:.12g}">{err if not ok else dual(num, base, decimals)}</td>'


def css_class(v: Any) -> str:
    if not has_num(v):
        return "missing muted"
    x = fnum(v)
    return "pos" if x > 0 else "neg" if x < 0 else "zero"


def block_num(block: Dict[str, Any], *keys: str, default: float = 0.0) -> float:
    """Read numeric metric fields with legacy alias fallback."""
    if not isinstance(block, dict):
        return default
    for key in keys:
        if key in block and block.get(key) is not None:
            return fnum(block.get(key), default)
    return default


def latest_history_block_dd(history: List[Dict[str, Any]], side: str, which: str = "drawdown") -> Optional[float]:
    """Return latest combined curve DD when the rendered block lacks it.

    Used only as a display fallback for header/user rows; it does not change
    the model or persist anything.
    """
    for point in reversed(history or []):
        block = point.get(side) if isinstance(point, dict) else None
        if isinstance(block, dict):
            if which == "max_drawdown":
                # Per-point history carries live drawdown; max is computed by caller.
                val = block_num(block, "max_drawdown", "maxdd", "drawdown", "drawdown_usd", default=0.0)
            else:
                val = block_num(block, "drawdown", "drawdown_usd", default=0.0)
            return max(0.0, val)
        elif side == "copy" and isinstance(point, dict):
            val = block_num(point, "drawdown", "drawdown_usd", default=0.0)
            return max(0.0, val)
        elif isinstance(point, dict):
            val = block_num(point, f"{side}_drawdown", default=0.0)
            return max(0.0, val)
    return None


def max_history_block_dd(history: List[Dict[str, Any]], side: str) -> Optional[float]:
    vals: List[float] = []
    for point in history or []:
        block = point.get(side) if isinstance(point, dict) else None
        if isinstance(block, dict):
            vals.append(max(0.0, block_num(block, "drawdown", "drawdown_usd", default=0.0)))
        elif side == "copy" and isinstance(point, dict):
            vals.append(max(0.0, block_num(point, "drawdown", "drawdown_usd", default=0.0)))
        elif isinstance(point, dict):
            vals.append(max(0.0, block_num(point, f"{side}_drawdown", default=0.0)))
    return max(vals) if vals else None


def dd_current(block: Dict[str, Any], history: Optional[List[Dict[str, Any]]] = None, side: Optional[str] = None) -> float:
    if isinstance(block, dict) and any(has_num(block.get(k)) for k in ("drawdown", "drawdown_usd")):
        return max(0.0, block_num(block, "drawdown", "drawdown_usd", default=0.0))
    if isinstance(block, dict) and has_num(block.get("equity")) and has_num(block.get("peak")):
        return max(0.0, fnum(block.get("peak")) - fnum(block.get("equity")))
    if isinstance(block, dict) and has_num(block.get("equity")) and has_num(block.get("peak_equity")):
        return max(0.0, fnum(block.get("peak_equity")) - fnum(block.get("equity")))
    hist = latest_history_block_dd(history or [], side or "copy") if history is not None and side else None
    if hist is not None:
        return max(0.0, hist)
    return 0.0


def dd_max(block: Dict[str, Any], history: Optional[List[Dict[str, Any]]] = None, side: Optional[str] = None) -> float:
    block_max = 0.0
    if isinstance(block, dict) and any(has_num(block.get(k)) for k in ("max_drawdown", "maxdd")):
        block_max = max(0.0, block_num(block, "max_drawdown", "maxdd", default=0.0))
    hist_max = max_history_block_dd(history or [], side or "copy") if history is not None and side else None
    return max(block_max, hist_max or 0.0, dd_current(block, history, side))


def get_current_dd(block: Dict[str, Any], curve: Optional[List[Dict[str, Any]]] = None, side_prefix: Optional[str] = None) -> float:
    return dd_current(block, curve, side_prefix)


def get_max_dd(block: Dict[str, Any], curve: Optional[List[Dict[str, Any]]] = None, side_prefix: Optional[str] = None) -> float:
    return dd_max(block, curve, side_prefix)


def format_dd(value: Any, alloc: float) -> str:
    return dual(-max(0.0, fnum(value)), alloc)


DASHBOARD_CELL_CONTRACT = {
    "header_pnl": "portfolio lead/copy total pnl = equity - alloc; delta = copy - lead",
    "header_realised": "portfolio lead/copy realised = sum included non-user row lead/copy realised",
    "header_unrealised": "portfolio lead/copy unrealised = sum included non-user row lead/copy unrealised",
    "header_drawdown": "portfolio current DD = current peak - current equity; copy max = max history/current",
    "header_maxdd": "portfolio maxDD = max historical drawdown/current DD",
    "header_exposure": "open = sum included current_position_usd; max = max portfolio history open_notional_usd; base = open/norm_base",
    "header_copyability": "avg_trade_pct from included copy_trades.return_pct; avg_trade_usd = included copy realised/exits; avg_pos from included avg_position_usd; req_lev = max required leverage",
    "header_activity": "fills/exits/open_pos/win from included non-user rows only",
    "wallet": "wallet address + user/aggregate badge only",
    "lead_equity": "row['lead']['equity'] = alloc + lead.realized + lead.unrealized",
    "copy_equity": "row['copy']['equity'] = alloc + copy.realized + copy.unrealized",
    "lead_real": "row['lead']['realized']",
    "copy_real": "row['copy']['realized']",
    "lead_unreal": "row['lead']['unrealized']",
    "copy_unreal": "row['copy']['unrealized']",
    "lead_dd": "get_current_dd(row['lead'])",
    "copy_dd": "get_current_dd(row['copy'])",
    "lead_maxdd": "get_max_dd(row['lead'], row['curve'], 'lead')",
    "copy_maxdd": "get_max_dd(row['copy'], row['curve'], 'copy')",
    "delta": "row['copy']['equity'] - row['lead']['equity']; pct = delta / row['alloc'] * 100",
    "pnl_per_hour": "row['pnl_per_hour']",
    "avg_trade_pct": "average copy_trades.return_pct for wallet; user aggregate uses selected included trades",
    "win_rate": "wins / closed copy trades * 100; win = copy_pnl >= 0",
    "avg_position_usd": "row['avg_position_usd'] = position_exposure_sum / position_exposure_samples",
    "max_position_usd": "row['max_position_usd']",
    "avg_entry_notional_usd": "entry_notional_sum / entry_notional_count",
    "pct_entries_ge10": "entry_notional_ge10_count / entry_notional_count * 100",
    "required_leverage": "max_position_usd / alloc",
    "fills_lc": "lead=row['fill_count']; copy=distinct expected_copy_fills.leader_fill_id for wallet",
    "exits_lc": "lead=row['exit_count']; copy=count copy_trades rows for wallet",
    "pos_lc": "lead/copy non-flat counts from final_position_alignment; fallback open_position_count",
    "include_control": "row['include_in_portfolio'] affects combined header/graph/user aggregate only",
    "wallet_model": "global/proportional/fixed wallet config controls",
}


def validate_render_contract(state: Dict[str, Any]) -> List[str]:
    """Validate dashboard cell contract. Returns list of error strings; never throws."""
    errors: List[str] = []
    try:
        ui = state.get("ui_state") or {}
        norm_base = fnum(ui.get("norm_base"), DEFAULT_NORM_BASE)
        user_base = max(1.0, fnum(ui.get("user_norm_base"), norm_base))
        rows = state.get("wallet_rows") or []
        copy_trades = state.get("copy_trades") or []

        trades_by_wallet: Dict[str, List[Dict[str, Any]]] = {}
        for t in copy_trades:
            w = str(t.get("wallet", "")).lower()
            trades_by_wallet.setdefault(w, []).append(t)

        selected_rows = [
            r for r in rows
            if not r.get("is_user_wallet") and wallet_included(str(r.get("wallet", "")), ui)
        ]

        # A. Normal active wallet rows
        for r in rows:
            if r.get("is_user_wallet"):
                continue
            if not active_wallet(r):
                continue
            wallet = str(r.get("wallet", "")).lower()
            wlabel = wallet[:10]
            alloc = fnum(r.get("alloc"), norm_base)
            lead = r.get("lead") if isinstance(r.get("lead"), dict) else {}
            copy = r.get("copy") if isinstance(r.get("copy"), dict) else {}

            lead_real = fnum(lead.get("realized"))
            lead_unreal = fnum(lead.get("unrealized"))
            lead_eq = fnum(lead.get("equity"))
            if abs(lead_eq - (alloc + lead_real + lead_unreal)) > 0.01:
                errors.append(f"{wlabel}: lead equity {lead_eq:.4f} != alloc({alloc:.2f})+real+unreal {alloc+lead_real+lead_unreal:.4f}")

            copy_real = fnum(copy.get("realized"))
            copy_unreal = fnum(copy.get("unrealized"))
            copy_eq = fnum(copy.get("equity"))
            if abs(copy_eq - (alloc + copy_real + copy_unreal)) > 0.01:
                errors.append(f"{wlabel}: copy equity {copy_eq:.4f} != alloc({alloc:.2f})+real+unreal {alloc+copy_real+copy_unreal:.4f}")

            lead_dd = get_current_dd(lead)
            copy_dd = get_current_dd(copy)
            if lead_dd < -0.01:
                errors.append(f"{wlabel}: lead DD {lead_dd:.4f} is negative")
            if copy_dd < -0.01:
                errors.append(f"{wlabel}: copy DD {copy_dd:.4f} is negative")

            lead_maxdd = get_max_dd(lead)
            copy_maxdd = get_max_dd(copy)
            if lead_maxdd < lead_dd - 0.01:
                errors.append(f"{wlabel}: lead maxDD {lead_maxdd:.4f} < currentDD {lead_dd:.4f}")
            if copy_maxdd < copy_dd - 0.01:
                errors.append(f"{wlabel}: copy maxDD {copy_maxdd:.4f} < currentDD {copy_dd:.4f}")

            delta = r.get("delta") if isinstance(r.get("delta"), dict) else {}
            delta_eq = fnum(delta.get("equity"))
            if abs(delta_eq - (copy_eq - lead_eq)) > 0.01:
                errors.append(f"{wlabel}: delta.equity {delta_eq:.4f} != copy_eq-lead_eq {copy_eq-lead_eq:.4f}")

            wallet_trades = trades_by_wallet.get(wallet, [])
            exit_count = inum(r.get("exit_count"), 0)
            if exit_count == 0 and inum(r.get("entry_notional_count"), 0) > 0:
                entry_notional = fnum(r.get("entry_notional_sum"))
                fee_bps = fnum(ui.get("fee_bps"), DEFAULT_FEE_BPS)
                copy_bps = fee_bps + fnum(ui.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS)
                expected_lead_real = -entry_notional * fee_bps / 10000.0
                expected_copy_real = -entry_notional * copy_bps / 10000.0
                if abs(lead_real - expected_lead_real) > 0.01:
                    errors.append(f"{wlabel}: zero-exit lead realized {lead_real:.4f} != entry cost {expected_lead_real:.4f}")
                if abs(copy_real - expected_copy_real) > 0.01:
                    errors.append(f"{wlabel}: zero-exit copy realized {copy_real:.4f} != entry cost {expected_copy_real:.4f}")

            if exit_count == 0:
                rendered = render_row(r, norm_base, ui, state)
                for field in ("win_rate", "avg_trade_pct"):
                    marker = f'data-field="{field}"'
                    if marker in rendered:
                        cell = rendered[rendered.rfind("<td", 0, rendered.find(marker)):rendered.find("</td>", rendered.find(marker)) + 5]
                        if "—" not in cell:
                            errors.append(f"{wlabel}: zero-exit {field} rendered with denominator-free value instead of N/A")
                active_hours = fnum(r.get("active_hours"))
                pnl_hr = fnum(r.get("pnl_per_hour"))
                if active_hours > 0:
                    expected_pnl_hr = copy_real / active_hours
                    if abs(pnl_hr - expected_pnl_hr) > 0.01:
                        errors.append(f"{wlabel}: pnl_per_hour {pnl_hr:.4f} != copy_realized/active_hours {expected_pnl_hr:.4f}")
                else:
                    marker = 'data-field="pnl_per_hour"'
                    if marker in rendered:
                        cell = rendered[rendered.rfind("<td", 0, rendered.find(marker)):rendered.find("</td>", rendered.find(marker)) + 5]
                        if "—" not in cell:
                            errors.append(f"{wlabel}: pnl_per_hour {pnl_hr:.4f} has no active_hours denominator")
            if exit_count > 0 and wallet_trades and len(wallet_trades) == exit_count:
                returns = [fnum(t.get("return_pct")) for t in wallet_trades]
                expected_atp = sum(returns) / len(returns)
                if abs(fnum(r.get("avg_trade_pct")) - expected_atp) > 0.1:
                    errors.append(f"{wlabel}: avg_trade_pct {fnum(r.get('avg_trade_pct')):.4f} != avg(return_pct) {expected_atp:.4f}")
                wins = sum(1 for t in wallet_trades if fnum(t.get("copy_pnl")) >= 0)
                expected_wr = wins / len(wallet_trades) * 100.0
                if abs(fnum(r.get("win_rate")) - expected_wr) > 1.0:
                    errors.append(f"{wlabel}: win_rate {fnum(r.get('win_rate')):.4f} != {expected_wr:.4f}")

            max_pos = fnum(r.get("max_position_usd"))
            cur_pos = fnum(r.get("current_position_usd"))
            req_lev = fnum(r.get("required_leverage"))
            if alloc > 0 and max_pos > 0:
                expected_lev = max_pos / alloc
                if abs(req_lev - expected_lev) > 0.01:
                    errors.append(f"{wlabel}: required_leverage {req_lev:.4f} != max_pos/alloc {expected_lev:.4f}")
            if max_pos < cur_pos - 0.01:
                errors.append(f"{wlabel}: max_position_usd {max_pos:.4f} < current_position_usd {cur_pos:.4f}")

            lc = wallet_lead_copy_counts(r, state)
            if lc["fills"][0] != lc["fills"][1]:
                errors.append(f"{wlabel}: fills L/C {lc['fills'][0]}/{lc['fills'][1]}")
            if lc["exits"][0] != lc["exits"][1]:
                errors.append(f"{wlabel}: exits L/C {lc['exits'][0]}/{lc['exits'][1]}")
            if lc["pos"][0] != lc["pos"][1]:
                errors.append(f"{wlabel}: pos L/C {lc['pos'][0]}/{lc['pos'][1]}")

        # B. User aggregate row
        user_rows = [r for r in rows if r.get("is_user_wallet")]
        if user_rows:
            u = user_rows[0]
            u_lead = u.get("lead") if isinstance(u.get("lead"), dict) else {}
            u_copy = u.get("copy") if isinstance(u.get("copy"), dict) else {}
            u_alloc = fnum(u.get("alloc"), user_base)
            if abs(u_alloc - user_base) > 0.01:
                errors.append(f"user_aggregate alloc {u_alloc:.4f} != user_norm_base {user_base:.4f}")
            sel_lr = sum(fnum((r.get("lead") or {}).get("realized")) for r in selected_rows)
            sel_cr = sum(fnum((r.get("copy") or {}).get("realized")) for r in selected_rows)
            sel_lu = sum(fnum((r.get("lead") or {}).get("unrealized")) for r in selected_rows)
            sel_cu = sum(fnum((r.get("copy") or {}).get("unrealized")) for r in selected_rows)
            if abs(fnum(u_lead.get("realized")) - sel_lr) > 0.01:
                errors.append(f"user_aggregate lead.realized {fnum(u_lead.get('realized')):.4f} != sum_selected {sel_lr:.4f}")
            if abs(fnum(u_copy.get("realized")) - sel_cr) > 0.01:
                errors.append(f"user_aggregate copy.realized {fnum(u_copy.get('realized')):.4f} != sum_selected {sel_cr:.4f}")
            if abs(fnum(u_lead.get("unrealized")) - sel_lu) > 0.01:
                errors.append(f"user_aggregate lead.unrealized {fnum(u_lead.get('unrealized')):.4f} != sum_selected {sel_lu:.4f}")
            if abs(fnum(u_copy.get("unrealized")) - sel_cu) > 0.01:
                errors.append(f"user_aggregate copy.unrealized {fnum(u_copy.get('unrealized')):.4f} != sum_selected {sel_cu:.4f}")
            exp_u_lead_eq = user_base + sel_lr + sel_lu
            exp_u_copy_eq = user_base + sel_cr + sel_cu
            if abs(fnum(u_lead.get("equity")) - exp_u_lead_eq) > 0.01:
                errors.append(f"user_aggregate lead.equity {fnum(u_lead.get('equity')):.4f} != user_norm_base+pnl {exp_u_lead_eq:.4f}")
            if abs(fnum(u_copy.get("equity")) - exp_u_copy_eq) > 0.01:
                errors.append(f"user_aggregate copy.equity {fnum(u_copy.get('equity')):.4f} != user_norm_base+pnl {exp_u_copy_eq:.4f}")
            sel_fills = sum(inum(r.get("fill_count")) for r in selected_rows)
            sel_exits = sum(inum(r.get("exit_count")) for r in selected_rows)
            sel_open = sum(inum(r.get("open_position_count")) for r in selected_rows)
            if inum(u.get("fill_count")) != sel_fills:
                errors.append(f"user_aggregate fill_count {inum(u.get('fill_count'))} != sum_selected {sel_fills}")
            if inum(u.get("exit_count")) != sel_exits:
                errors.append(f"user_aggregate exit_count {inum(u.get('exit_count'))} != sum_selected {sel_exits}")
            if inum(u.get("open_position_count")) != sel_open:
                errors.append(f"user_aggregate open_position_count {inum(u.get('open_position_count'))} != sum_selected {sel_open}")

        # C. Portfolio/header
        port = state.get("portfolio") or {}
        port_lead = port.get("lead") if isinstance(port.get("lead"), dict) else {}
        port_copy = port.get("copy") if isinstance(port.get("copy"), dict) else {}
        sel_lr2 = sum(fnum((r.get("lead") or {}).get("realized")) for r in selected_rows)
        sel_cr2 = sum(fnum((r.get("copy") or {}).get("realized")) for r in selected_rows)
        sel_notional = sum(fnum(r.get("current_position_usd")) for r in selected_rows)
        if abs(fnum(port_lead.get("realized")) - sel_lr2) > 0.01:
            errors.append(f"portfolio lead.realized {fnum(port_lead.get('realized')):.4f} != sum_selected {sel_lr2:.4f}")
        if abs(fnum(port_copy.get("realized")) - sel_cr2) > 0.01:
            errors.append(f"portfolio copy.realized {fnum(port_copy.get('realized')):.4f} != sum_selected {sel_cr2:.4f}")
        if abs(fnum(port.get("open_notional_usd")) - sel_notional) > 0.01:
            errors.append(f"portfolio open_notional_usd {fnum(port.get('open_notional_usd')):.4f} != sum_selected {sel_notional:.4f}")
        # Live current stats: portfolio current DD must equal sum of live wallet block DD.
        live_lead_dd = sum(fnum((r.get("lead") or {}).get("drawdown")) for r in selected_rows)
        live_copy_dd = sum(fnum((r.get("copy") or {}).get("drawdown")) for r in selected_rows)
        if abs(fnum(port_lead.get("drawdown")) - live_lead_dd) > 0.01:
            errors.append(f"portfolio lead DD {fnum(port_lead.get('drawdown')):.4f} != sum live wallet blocks {live_lead_dd:.4f}")
        if abs(fnum(port_copy.get("drawdown")) - live_copy_dd) > 0.01:
            errors.append(f"portfolio copy DD {fnum(port_copy.get('drawdown')):.4f} != sum live wallet blocks {live_copy_dd:.4f}")
        # Historical max stats: portfolio maxDD and max exposure equal max timestamped sum (live snapshot is tail).
        curve_stats = selected_combined_curve_stats(selected_rows)
        if curve_stats["has_points"]:
            expected_max_lead_dd = max(curve_stats["max_lead_dd"], live_lead_dd)
            expected_max_copy_dd = max(curve_stats["max_copy_dd"], live_copy_dd)
            expected_max_exposure = max(curve_stats["max_exposure"], sel_notional)
            if not contract_money_equal(port_lead.get("max_drawdown"), expected_max_lead_dd):
                errors.append(f"portfolio lead MaxDD {fnum(port_lead.get('max_drawdown')):.4f} != max timestamped summed DD {expected_max_lead_dd:.4f}")
            if not contract_money_equal(port_copy.get("max_drawdown"), expected_max_copy_dd):
                errors.append(f"portfolio copy MaxDD {fnum(port_copy.get('max_drawdown')):.4f} != max timestamped summed DD {expected_max_copy_dd:.4f}")
            if abs(fnum(port.get("max_open_notional_usd")) - expected_max_exposure) > 0.01:
                errors.append(f"portfolio max exposure {fnum(port.get('max_open_notional_usd')):.4f} != max timestamped summed exposure {expected_max_exposure:.4f}")
            expected_req_lev = expected_max_exposure / user_base if user_base else 0.0
            if abs(fnum(port.get("max_required_leverage")) - expected_req_lev) > 0.001:
                errors.append(f"portfolio req_lev {fnum(port.get('max_required_leverage')):.4f} != max summed exposure/user_norm_base {expected_req_lev:.4f}")

        # D. Header / USER row must match (both derived from selected_aggregate).
        user_rows = [r for r in rows if r.get("is_user_wallet")]
        if user_rows and selected_rows:
            u = user_rows[0]
            u_copy = u.get("copy") if isinstance(u.get("copy"), dict) else {}
            u_lead = u.get("lead") if isinstance(u.get("lead"), dict) else {}
            port_copy2 = port.get("copy") if isinstance(port.get("copy"), dict) else {}
            port_lead2 = port.get("lead") if isinstance(port.get("lead"), dict) else {}

            # DD: both in absolute dollars so must match.
            hdr_copy_dd = fnum(port_copy2.get("drawdown"))
            usr_copy_dd = get_current_dd(u_copy)
            if abs(hdr_copy_dd - usr_copy_dd) > 0.01:
                errors.append(f"header/user copy DD mismatch: header={hdr_copy_dd:.4f} user={usr_copy_dd:.4f}")
            hdr_lead_dd = fnum(port_lead2.get("drawdown"))
            usr_lead_dd = get_current_dd(u_lead)
            if abs(hdr_lead_dd - usr_lead_dd) > 0.01:
                errors.append(f"header/user lead DD mismatch: header={hdr_lead_dd:.4f} user={usr_lead_dd:.4f}")

            # MaxDD
            hdr_copy_maxdd = fnum(port_copy2.get("max_drawdown"))
            usr_copy_maxdd = get_max_dd(u_copy)
            if not contract_money_equal(hdr_copy_maxdd, usr_copy_maxdd):
                errors.append(f"header/user copy maxDD mismatch: header={hdr_copy_maxdd:.4f} user={usr_copy_maxdd:.4f}")
            hdr_lead_maxdd = fnum(port_lead2.get("max_drawdown"))
            usr_lead_maxdd = get_max_dd(u_lead)
            if not contract_money_equal(hdr_lead_maxdd, usr_lead_maxdd):
                errors.append(f"header/user lead maxDD mismatch: header={hdr_lead_maxdd:.4f} user={usr_lead_maxdd:.4f}")

            for metric in ("current_position_usd", "max_position_usd", "avg_trade_pct", "avg_entry_notional_usd", "pct_entries_ge10"):
                header_key = "open_notional_usd" if metric == "current_position_usd" else "max_open_notional_usd" if metric == "max_position_usd" else metric
                hv = fnum(port.get(header_key))
                uv = fnum(u.get(metric))
                if abs(hv - uv) > 0.01:
                    errors.append(f"header/user {metric} mismatch: header={hv:.4f} user={uv:.4f}")

            # Req Lev (both from selected_aggregate.required_leverage)
            hdr_req_lev = fnum(port.get("max_required_leverage"))
            usr_req_lev = fnum(u.get("required_leverage"))
            if abs(hdr_req_lev - usr_req_lev) > 0.001:
                errors.append(f"header/user req_lev mismatch: header={hdr_req_lev:.4f} user={usr_req_lev:.4f}")

            # Win%
            hdr_win = fnum(port.get("win_rate"))
            usr_win = fnum(u.get("win_rate"))
            if abs(hdr_win - usr_win) > 0.01:
                errors.append(f"header/user win_rate mismatch: header={hdr_win:.4f} user={usr_win:.4f}")

            # Avg Notional: user must not be 0 when selected entries exist
            sel_entry_count = sum(inum(r.get("entry_notional_count", 0)) for r in selected_rows)
            u_avg_not = fnum(u.get("avg_entry_notional_usd"))
            if sel_entry_count > 0 and u_avg_not <= 0:
                errors.append(f"user avg_entry_notional_usd is 0 but selected entry_notional_count={sel_entry_count}")

            # % ≥ $10: user must not be 0 when selected notional entries exist
            u_pct_ge10 = fnum(u.get("pct_entries_ge10"))
            sel_ge10   = sum(inum(r.get("entry_notional_ge10_count", 0)) for r in selected_rows)
            if sel_entry_count > 0 and sel_ge10 > 0 and u_pct_ge10 <= 0:
                errors.append(f"user pct_entries_ge10 is 0 but selected ge10={sel_ge10}/{sel_entry_count}")

    except Exception as exc:
        errors.append(f"validate_render_contract exception: {type(exc).__name__}: {exc}")
    return errors


def render_chart(history: List[Dict[str, Any]]) -> str:
    """Render copy PnL, realised PnL and drawdown against real elapsed time."""
    if not history:
        return '<div class="chart-empty">Awaiting data…</div>'

    raw_points: List[Dict[str, Any]] = []
    for p in history:
        copy_block = p.get("copy") if isinstance(p.get("copy"), dict) else {}
        alloc = fnum(p.get("alloc"), fnum(copy_block.get("alloc"), 0.0))
        if copy_block:
            equity = fnum(copy_block.get("equity"))
            pnl = equity - alloc
            realized = fnum(copy_block.get("realized", p.get("realized", pnl)))
            dd = fnum(copy_block.get("drawdown", copy_block.get("drawdown_usd")))
        else:
            equity = fnum(p.get("copy_equity", p.get("equity")))
            pnl = fnum(p.get("copy_pnl", p.get("pnl", equity - alloc)))
            realized = fnum(p.get("copy_realized", p.get("realized", pnl)))
            dd = fnum(p.get("copy_drawdown", p.get("drawdown", p.get("drawdown_usd"))))
        ts = str(p.get("ts", ""))
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except Exception:
            t = float(len(raw_points))
        raw_points.append({"ts": ts, "t": t, "equity": equity, "pnl": pnl, "realized": realized, "dd": max(0.0, dd)})

    def compress_points(points: List[Dict[str, Any]], max_points: int = CHART_POINT_MAX) -> List[Dict[str, Any]]:
        """Pixel-safe full-history rendering.

        Keeps the complete persisted history on disk, but compresses the SVG
        representation by time buckets while preserving first/last plus PnL,
        realised-PnL and DD extremes. This prevents barcode rendering without
        losing drawdown spikes.
        """
        if len(points) <= max_points:
            return points
        bucket_count = max(1, max_points // 4)
        bucket_size = len(points) / bucket_count
        out: List[Dict[str, Any]] = []
        i = 0.0
        while int(i) < len(points):
            start = int(i)
            end = min(len(points), max(start + 1, int(i + bucket_size)))
            chunk = points[start:end]
            keep = [
                chunk[0],
                min(chunk, key=lambda x: fnum(x.get("pnl"))),
                max(chunk, key=lambda x: fnum(x.get("pnl"))),
                min(chunk, key=lambda x: fnum(x.get("realized"))),
                max(chunk, key=lambda x: fnum(x.get("realized"))),
                max(chunk, key=lambda x: fnum(x.get("dd"))),
                chunk[-1],
            ]
            seen_ids = set()
            ordered: List[Dict[str, Any]] = []
            for point in sorted(keep, key=lambda x: fnum(x.get("t"))):
                ident = id(point)
                if ident not in seen_ids:
                    seen_ids.add(ident)
                    ordered.append(point)
            out.extend(ordered)
            i += bucket_size
        if out and out[-1] is not points[-1]:
            out.append(points[-1])
        return out

    pts_data = compress_points(raw_points, CHART_POINT_MAX)

    if len(pts_data) == 1:
        first = dict(pts_data[0])
        pts_data = [dict(first, t=first["t"] - 1.0, pnl=0.0, realized=0.0, dd=0.0, equity=first["equity"] - first["pnl"]), first]

    pnl_vals = [fnum(p["pnl"]) for p in pts_data]
    realized_vals = [fnum(p["realized"]) for p in pts_data]
    dd_vals = [-fnum(p["dd"]) for p in pts_data]
    all_vals = pnl_vals + realized_vals + dd_vals + [0.0]
    mn, mx = min(all_vals), max(all_vals)
    if abs(mx - mn) < 1e-9:
        mx += 1.0
        mn -= 1.0

    w, h = 1200, 260
    pad_l, pad_r, pad_t, pad_b = 62, 18, 18, 34
    plot_w, plot_h = w - pad_l - pad_r, h - pad_t - pad_b
    ts_min = min(fnum(p["t"]) for p in pts_data)
    ts_max = max(fnum(p["t"]) for p in pts_data)
    if abs(ts_max - ts_min) < 1e-9:
        ts_max = ts_min + 1.0

    def x_for_t(t: float) -> float:
        return pad_l + plot_w * ((t - ts_min) / max(1e-9, ts_max - ts_min))

    def x_at(i: int) -> float:
        return x_for_t(fnum(pts_data[i]["t"]))

    def y_at(v: float) -> float:
        return pad_t + plot_h * (1 - ((v - mn) / (mx - mn)))

    def poly(vals: List[float]) -> str:
        return " ".join(f"{x_at(i):.1f},{y_at(v):.1f}" for i, v in enumerate(vals))

    chart_span = ts_max - ts_min

    def label_time(t: float) -> str:
        try:
            dt = datetime.fromtimestamp(t, tz=timezone.utc)
            if chart_span > 48 * 3600:
                return dt.strftime("%d %b")
            if chart_span > 24 * 3600:
                return dt.strftime("%d %b %H:%M")
            return dt.strftime("%H:%M")
        except Exception:
            return ""

    grid_bits: List[str] = []
    for i in range(5):
        val = mn + (mx - mn) * (i / 4)
        y = y_at(val)
        grid_bits.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-pad_r}" y2="{y:.1f}" class="grid-line"/>')
        grid_bits.append(f'<text x="{pad_l-8}" y="{y+4:.1f}" class="axis-label" text-anchor="end">{val:+.2f}</text>')
    for i in range(5):
        t = ts_min + (ts_max - ts_min) * (i / 4)
        x = x_for_t(t)
        grid_bits.append(f'<line x1="{x:.1f}" y1="{pad_t}" x2="{x:.1f}" y2="{h-pad_b}" class="grid-vert"/>')
        grid_bits.append(f'<text x="{x:.1f}" y="{h-8}" class="axis-label" text-anchor="middle">{label_time(t)}</text>')
    grid = "".join(grid_bits)

    hit_bits: List[str] = []
    xs = [x_at(i) for i in range(len(pts_data))]
    for i, point in enumerate(pts_data):
        x = xs[i]
        left = ((xs[i - 1] + x) / 2) if i > 0 else pad_l
        right = ((x + xs[i + 1]) / 2) if i < len(xs) - 1 else w - pad_r
        label = f"{point['ts']} | PnL {point['pnl']:+.4f} | Real {point['realized']:+.4f} | DD {-point['dd']:.4f} | Eq {point['equity']:.4f}"
        safe = html.escape(label, quote=True)
        hit_bits.append(f'<rect class="hit" x="{left:.1f}" y="{pad_t}" width="{max(1.0, right-left):.1f}" height="{plot_h}" data-x="{x:.1f}" data-y="{y_at(point["pnl"]):.1f}" data-label="{safe}"/>')
    hits = "".join(hit_bits)
    zero_y = y_at(0.0)

    return f"""
    <div class="chart-wrap">
      <svg class="chart" viewBox="0 0 {w} {h}" preserveAspectRatio="none">
        {grid}
        <line x1="{pad_l}" y1="{zero_y:.1f}" x2="{w-pad_r}" y2="{zero_y:.1f}" class="zero-line"/>
        <polyline points="{poly(pnl_vals)}" class="pnl-line"/>
        <polyline points="{poly(realized_vals)}" class="realized-line"/>
        <polyline points="{poly(dd_vals)}" class="dd-line"/>
        <line id="chartCrossX" x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{h-pad_b}" class="crosshair" style="display:none"/>
        <circle id="chartDot" cx="{pad_l}" cy="{zero_y:.1f}" r="4" class="chart-dot" style="display:none"/>
        {hits}
      </svg>
      <div class="chart-legend"><span class="legend-pnl">Copy PnL</span><span class="legend-realized">Realised</span><span class="legend-dd">Drawdown</span><span class="muted">Hover for values · click chart to expand</span></div>
      <div id="chartTip" class="chart-tip" style="display:none"></div>
    </div>"""

def sorted_rows(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = list(state.get("wallet_rows") or [])
    ranking = state.get("ui_state", {}).get("ranking", {}) or {}
    col = ranking.get("column") or "lead_equity"
    direction = str(ranking.get("direction") or "desc").lower()
    def key(row: Dict[str, Any]) -> Any:
        lead = row.get("lead", {}) if isinstance(row.get("lead"), dict) else {}
        copy = row.get("copy", {}) if isinstance(row.get("copy"), dict) else {}
        delta = row.get("delta", {}) if isinstance(row.get("delta"), dict) else {}
        lead_pnl = fnum(lead.get("realized")) + fnum(lead.get("unrealized"))
        copy_pnl = fnum(copy.get("realized")) + fnum(copy.get("unrealized"))
        mapping = {
            "wallet": row.get("wallet", ""),
            "lead_equity": lead_pnl,
            "lead_real": fnum(lead.get("realized")),
            "lead_realized": fnum(lead.get("realized")),
            "lead_unreal": fnum(lead.get("unrealized")),
            "lead_unrealized": fnum(lead.get("unrealized")),
            "lead_dd": -block_num(lead, "drawdown", "drawdown_usd", default=0.0),
            "lead_drawdown": -block_num(lead, "drawdown", "drawdown_usd", default=0.0),
            "lead_maxdd": -block_num(lead, "max_drawdown", "maxdd", default=0.0),
            "lead_max_drawdown": -block_num(lead, "max_drawdown", "maxdd", default=0.0),
            "copy_equity": copy_pnl,
            "copy_real": fnum(copy.get("realized")),
            "copy_realized": fnum(copy.get("realized")),
            "copy_unreal": fnum(copy.get("unrealized")),
            "copy_unrealized": fnum(copy.get("unrealized")),
            "copy_dd": -block_num(copy, "drawdown", "drawdown_usd", default=0.0),
            "copy_drawdown": -block_num(copy, "drawdown", "drawdown_usd", default=0.0),
            "copy_maxdd": -block_num(copy, "max_drawdown", "maxdd", default=0.0),
            "copy_max_drawdown": -block_num(copy, "max_drawdown", "maxdd", default=0.0),
            "delta": fnum(delta.get("equity")),
            "delta_pct": fnum(delta.get("pct")),
            "copy_total_pnl": fnum(row.get("copy_total_pnl")),
            "lead_total_pnl": fnum(row.get("lead_total_pnl")),
            "copy_efficiency": fnum(row.get("copy_efficiency")),
            "pnl_per_hour": fnum(row.get("pnl_per_hour")),
            "pnl_per_trade": fnum(row.get("pnl_per_trade")),
            "avg_trade_pct": fnum(row.get("avg_trade_pct")),
            "win_rate": fnum(row.get("win_rate")),
            "fill_count": fnum(row.get("fill_count")),
            "exit_count": fnum(row.get("exit_count")),
            "ws": fnum(row.get("ws_coverage")),
            "ws_fill_count": fnum(row.get("ws_fill_count")),
            "avg_ws_latency_ms": fnum(row.get("avg_ws_latency_ms")),
            "expected_price_coverage_pct": fnum(row.get("expected_price_coverage_pct")),
            "rebuild_fill_count": fnum(row.get("rebuild_fill_count")),
            "measured_delta_fill_count": fnum(row.get("measured_delta_fill_count")),
            "avg_entry_disadvantage_bps": fnum(row.get("avg_entry_disadvantage_bps")),
            "avg_exit_disadvantage_bps": fnum(row.get("avg_exit_disadvantage_bps")),
            "open_position_count": fnum(row.get("open_position_count")),
            "avg_position_usd": fnum(row.get("avg_position_usd")),
            "max_position_usd": fnum(row.get("max_position_usd")),
            "avg_entry_notional_usd": fnum(row.get("avg_entry_notional_usd")),
            "max_entry_notional_usd": fnum(row.get("max_entry_notional_usd")),
            "pct_entries_ge10": fnum(row.get("pct_entries_ge10")),
            "required_leverage": fnum(row.get("required_leverage")),
        }
        return mapping.get(str(col), row.get("wallet", ""))

    reverse = direction == "desc"
    ui_for_grp = state.get("ui_state") or {}
    user_rows = [r for r in rows if r.get("is_user_wallet")]
    included_rows = [r for r in rows if not r.get("is_user_wallet") and wallet_included(str(r.get("wallet", "")), ui_for_grp)]
    other_rows = [r for r in rows if not r.get("is_user_wallet") and not wallet_included(str(r.get("wallet", "")), ui_for_grp)]
    included_rows.sort(key=key, reverse=reverse)
    other_rows.sort(key=key, reverse=reverse)
    return user_rows + included_rows + other_rows


def render_home(state: Dict[str, Any]) -> str:
    state = dict(state)
    ui = load_ui_state()
    state["ui_state"] = ui
    port = state.get("portfolio", {})
    copy = port.get("copy", {})
    lead = port.get("lead", {})
    delta = port.get("delta", {})
    base = fnum(ui.get("norm_base"), DEFAULT_NORM_BASE)
    user_base = max(1.0, fnum(ui.get("user_norm_base"), base))
    rows = sorted_rows(state)
    metric_rows = [r for r in rows if not r.get("is_user_wallet")]
    included_rows = [r for r in metric_rows if r.get("include_in_portfolio", True)]
    lead_total = fnum(lead.get("realized")) + fnum(lead.get("unrealized"))
    copy_total = fnum(copy.get("realized")) + fnum(copy.get("unrealized"))
    delta_total = fnum(delta.get("equity"))
    hist = state.get("portfolio_history", []) if isinstance(state.get("portfolio_history", []), list) else []
    lead_dd = get_current_dd(lead, hist, "lead")
    copy_dd = get_current_dd(copy, hist, "copy")
    lead_maxdd = get_max_dd(lead, hist, "lead")
    copy_maxdd = get_max_dd(copy, hist, "copy")
    open_notional = fnum(port.get("open_notional_usd"), sum(fnum(r.get("current_position_usd")) for r in included_rows))
    max_open_notional = fnum(port.get("max_open_notional_usd"), open_notional)
    notional_x = open_notional / user_base if user_base else 0.0
    max_notional_x = max_open_notional / user_base if user_base else 0.0
    avg_pos_size = fnum(port.get("avg_position_usd"), avg([fnum(r.get("avg_position_usd")) for r in included_rows if fnum(r.get("avg_position_usd")) > 0]))
    # Read from portfolio dict (which is populated from selected_aggregate) so
    # header and USER row always derive from the same canonical source.
    max_req_lev = fnum(port.get("max_required_leverage"))
    win_avg = fnum(port.get("win_rate", avg([fnum(r.get("win_rate")) for r in included_rows if r.get("exit_count")])))
    avg_trade = avg([fnum(r.get("pnl_per_trade")) for r in included_rows if r.get("exit_count")])
    avg_trade_pct = fnum(port.get("avg_trade_pct"))
    fill_count = sum(int(r.get("fill_count") or 0) for r in included_rows)
    exit_count = sum(int(r.get("exit_count") or 0) for r in included_rows)
    open_positions = sum(int(r.get("open_position_count") or 0) for r in included_rows)
    health_label, health_detail = health_status_label()
    def small_metric(label: str, value: str, raw: Any = 0.0) -> str:
        return f'<div class="metric-line"><span>{label}</span><b class="{css_class(raw)}">{value}</b></div>'
    def group_card(title: str, lines: List[str], extra_cls: str = "") -> str:
        return f'<div class="card group-card {extra_cls}"><div class="label">{title}</div>' + "".join(lines) + '</div>'
    cards = "".join([
        group_card("PNL", [small_metric("LEAD", dual(lead_total, user_base), lead_total), small_metric("COPY", dual(copy_total, user_base), copy_total), small_metric("Δ", dual(delta_total, user_base), delta_total)]),
        group_card("REALISED", [small_metric("LEAD", dual(fnum(lead.get("realized")), user_base), fnum(lead.get("realized"))), small_metric("COPY", dual(fnum(copy.get("realized")), user_base), fnum(copy.get("realized")))]),
        group_card("UNREALISED", [small_metric("LEAD", dual(fnum(lead.get("unrealized")), user_base), fnum(lead.get("unrealized"))), small_metric("COPY", dual(fnum(copy.get("unrealized")), user_base), fnum(copy.get("unrealized")))]),
        group_card("DRAWDOWN", [small_metric("LEAD DD", format_dd(lead_dd, user_base), -lead_dd), small_metric("COPY DD", format_dd(copy_dd, user_base), -copy_dd), small_metric("COPY MAX", format_dd(copy_maxdd, user_base), -copy_maxdd)]),
        group_card("MAX DD", [small_metric("LEAD", format_dd(lead_maxdd, user_base), -lead_maxdd), small_metric("COPY", format_dd(copy_maxdd, user_base), -copy_maxdd)]),
        group_card("EXPOSURE", [small_metric("OPEN", money(open_notional), open_notional), small_metric("MAX", money(max_open_notional), max_open_notional), small_metric("BASE", f"{notional_x:.2f}x / {max_notional_x:.2f}x", notional_x)]),
        group_card("COPYABILITY", [small_metric("AVG TRADE %", pct(avg_trade_pct, 3), avg_trade_pct), small_metric("AVG TRADE $", money(avg_trade), avg_trade), small_metric("AVG POS", money(avg_pos_size), avg_pos_size), small_metric("REQ LEV", f"{max_req_lev:.2f}x", max_req_lev)]),
        group_card("ACTIVITY", [small_metric("FILLS", str(fill_count), fill_count), small_metric("EXITS", str(exit_count), exit_count), small_metric("OPEN POS", str(open_positions), open_positions), small_metric("WIN", pct(win_avg), win_avg)]),
        group_card("DB HEALTH", [small_metric(health_label, html.escape(health_detail), -1 if "ERROR" in health_label else 0), small_metric("CACHE", str(APP_HEALTH.get("cache_hits", 0)), 0), small_metric("BUILDS", str(APP_HEALTH.get("build_count", 0)), 0)], "health-card"),
    ])
    ranking = ui.get("ranking", {}) if isinstance(ui.get("ranking"), dict) else {}
    active_col = str(ranking.get("column") or "lead_equity"); active_dir = str(ranking.get("direction") or "desc")
    def th(col: str, label: str, cls: str = "") -> str:
        active = col == active_col; arrow = " ▲" if active and active_dir == "asc" else " ▼" if active else ""
        return f'<th class="{cls} {"sort-active" if active else ""}"><a href="/sort/{col}">{label}{arrow}</a></th>'
    table_head = """<tr><th class="sticky-wallet">WALLET</th>""" + "".join([
        th("lead_equity", "LEAD EQ", "pair-lead"), th("copy_equity", "COPY EQ", "pair-copy group-divider"),
        th("lead_real", "LEAD REAL", "pair-lead"), th("copy_real", "COPY REAL", "pair-copy group-divider"),
        th("lead_unreal", "LEAD UNREAL", "pair-lead"), th("copy_unreal", "COPY UNREAL", "pair-copy group-divider"),
        th("lead_dd", "LEAD DD", "pair-lead"), th("copy_dd", "COPY DD", "pair-copy group-divider"),
        th("lead_maxdd", "LEAD MAXDD", "pair-lead"), th("copy_maxdd", "COPY MAXDD", "pair-copy group-divider"),
        th("delta", "Δ $/%", "group-divider"), th("pnl_per_hour", "PNL/HR"), th("avg_trade_pct", "AVG TRADE %"), th("win_rate", "WIN%"),
        th("avg_position_usd", "AVG POS $"), th("max_position_usd", "MAX POS $"), th("avg_entry_notional_usd", "AVG NOTIONAL"), th("pct_entries_ge10", "% ≥ $10"), th("required_leverage", "REQ LEV"),
        th("fill_count", "FILLS L/C", "ops-group"), th("exit_count", "EXITS L/C", "ops-group"), th("open_position_count", "POS L/C", "ops-group group-divider"),
    ]) + "<th>INC / WALLET MODEL</th></tr>"
    body_parts: List[str] = []
    inserted_divider = False
    has_included_group = any(not r.get("is_user_wallet") and wallet_included(str(r.get("wallet", "")), ui) for r in rows)
    has_other_group = any(not r.get("is_user_wallet") and not wallet_included(str(r.get("wallet", "")), ui) for r in rows)
    for r in rows:
        if has_included_group and has_other_group and not inserted_divider and not r.get("is_user_wallet") and not wallet_included(str(r.get("wallet", "")), ui):
            body_parts.append('<tr class="selected-divider"><td colspan="99">Other tracked wallets</td></tr>')
            inserted_divider = True
        body_parts.append(render_row(r, base, ui, state))
    body_rows = "\n".join(body_parts)
    contract_errs = validate_render_contract(state)
    banner = ""
    if contract_errs:
        msgs = " | ".join(html.escape(e) for e in contract_errs[:10])
        banner = f'<div style="background:#3d1515;border:1px solid #f44;color:#f88;padding:8px 14px;font-size:11px;position:sticky;top:48px;z-index:3"><b>&#9888; CONTRACT WARNING ({len(contract_errs)} errors):</b> {msgs}</div>'
    return banner + HTML_TEMPLATE.format(updated=state.get("updated_at", ""), norm=base, mode=str(ui.get("copy_mode", "proportional")).upper(), fixed=fnum(ui.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL), fee=fnum(ui.get("fee_bps"), DEFAULT_FEE_BPS), friction=fnum(ui.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS), cards=cards, chart=render_chart(state.get("portfolio_history", [])), wallet_count=len(metric_rows), table_head=table_head, table_rows=body_rows, raw_boundary=state.get("engine_truth_boundary", ""))


def extract_num(s: str) -> float:
    try:
        t = str(s).replace("$", "").replace(",", "").replace("%", "").split()[0]
        return float(t)
    except Exception:
        return 0.0


def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def wallet_lead_copy_counts(row: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Tuple[int, int]]:
    wallet = str(row.get("wallet", "")).lower()
    lead_fills = inum(row.get("fill_count"), 0)
    wallet_actions = [f for f in state.get("expected_copy_fills", []) if str(f.get("wallet", "")).lower() == wallet]
    copy_fills = len({str(f.get("leader_fill_id", "")) for f in wallet_actions if str(f.get("leader_fill_id", "")).strip()}) or lead_fills
    lead_exits = inum(row.get("exit_count"), 0)
    copy_exits = sum(1 for t in state.get("copy_trades", []) if str(t.get("wallet", "")).lower() == wallet) or lead_exits
    align = row.get("final_position_alignment", [])
    if isinstance(align, list) and align:
        lead_pos = sum(1 for a in align if str((a or {}).get("leader_position_side_after", "FLAT")).upper() != "FLAT")
        copy_pos = sum(1 for a in align if str((a or {}).get("copy_position_side_after", "FLAT")).upper() != "FLAT")
    else:
        lead_pos = copy_pos = inum(row.get("open_position_count"), 0)
    return {"fills": (lead_fills, copy_fills), "exits": (lead_exits, copy_exits), "pos": (lead_pos, copy_pos)}


def lc_cell(row: Dict[str, Any], pair: Tuple[int, int], cls: str = "") -> str:
    left, right = pair
    mismatch = left != right
    if mismatch:
        flags = row.setdefault("flags", [])
        if isinstance(flags, list) and "LEAD_COPY_COUNT_MISMATCH" not in flags:
            flags.append("LEAD_COPY_COUNT_MISMATCH")
    content = f"{left} / {right}" + (" <span class='badge neg'>DIFF</span>" if mismatch else "")
    return f'<td class="{cls} {"neg" if mismatch else ""}" data-sort="{left}">{content}</td>'


def render_row(r: Dict[str, Any], base: float, ui: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> str:
    state = state or {}
    lead = r.get("lead", {}); copy = r.get("copy", {}); delta = r.get("delta", {})
    wallet = str(r.get("wallet", "")); badge = " <span class='badge'>USER</span>" if r.get("is_user_wallet") else ""
    alloc = fnum(r.get('alloc'), base)
    lead_pnl = fnum(lead.get('realized')) + fnum(lead.get('unrealized'))
    copy_pnl = fnum(copy.get('realized')) + fnum(copy.get('unrealized'))
    curve = r.get("curve", []) if isinstance(r.get("curve", []), list) else []
    hist = curve if r.get("is_user_wallet") else None
    lead_dd_raw = get_current_dd(lead, hist, "lead") if hist is not None else get_current_dd(lead)
    copy_dd_raw = get_current_dd(copy, hist, "copy") if hist is not None else get_current_dd(copy)
    lead_maxdd_raw = get_max_dd(lead, hist, "lead") if hist is not None else get_max_dd(lead)
    copy_maxdd_raw = get_max_dd(copy, hist, "copy") if hist is not None else get_max_dd(copy)
    lead_dd_val = -lead_dd_raw; lead_maxdd_val = -lead_maxdd_raw
    copy_dd_val = -copy_dd_raw; copy_maxdd_val = -copy_maxdd_raw
    eff_mode = str(r.get("effective_copy_mode") or ui.get("copy_mode", "proportional"))
    eff_base = fnum(r.get("effective_norm_base"), fnum(r.get("alloc"), base)); eff_fixed = fnum(r.get("effective_fixed_notional"), fnum(ui.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL))
    override_badge = " *" if r.get("wallet_config") else ""; included = bool(r.get("include_in_portfolio", True))
    lc_counts = wallet_lead_copy_counts(r, state)
    no_closed_trades = inum(r.get("exit_count"), 0) == 0
    pnl_hr_cell = core_td(r, 'pnl_per_hour' in r, r.get('pnl_per_hour'), dual(fnum(r.get('pnl_per_hour')), alloc), css_class(r.get('pnl_per_hour')), "pnl_per_hour")
    if no_closed_trades and fnum(r.get("active_hours")) <= 0:
        pnl_hr_cell = dash_td(css_class(r.get('pnl_per_hour')), "pnl_per_hour")
    avg_trade_pct_cell = dash_td(css_class(r.get('avg_trade_pct')), "avg_trade_pct") if no_closed_trades else core_td(r, 'avg_trade_pct' in r, r.get('avg_trade_pct'), pct(r.get('avg_trade_pct'), 3), css_class(r.get('avg_trade_pct')), "avg_trade_pct")
    win_rate_cell = dash_td(css_class(r.get('win_rate')), "win_rate") if no_closed_trades else core_td(r, 'win_rate' in r, r.get('win_rate'), pct(r.get('win_rate')), css_class(r.get('win_rate')), "win_rate")
    inc_cell = ""
    cfg_cell = ""
    purge_cell = ""
    meta_link = ""
    meta_badge = ""
    wallet_color_cls = ""
    wallet_title = ""
    if r.get("is_user_wallet"):
        user_base = max(1.0, fnum(ui.get("user_norm_base"), fnum(ui.get("norm_base"), DEFAULT_NORM_BASE)))
        cfg_cell = f'<form action="/api/ui-state" method="post" class="ajax-form user-base-form"><span class="small muted">aggregate base</span><input name="user_norm_base" value="{user_base:g}" size="5" title="aggregate normalisation base"><button title="set aggregate base">Set</button></form>'
    else:
        meta = get_wallet_meta(ui, wallet)
        tag = meta.get("tag", "none")
        color = meta.get("color", "none")
        note = meta.get("note", "")
        if tag != "none" or note:
            wallet_title = f' title="{html.escape(f"tag={tag}; note={note}")}"'
        if tag != "none":
            meta_badge = f' <span class="wallet-tag">{html.escape(tag.upper())}</span>'
        if color != "none":
            wallet_color_cls = f" wallet-color-{html.escape(color)}"
        inc_cell = f"""<form action="/api/wallet-include" method="post" class="inc-form" title="Include/exclude this wallet from combined graph and header cards only"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><input type="checkbox" name="included" value="1" {'checked' if included else ''} onchange="this.form.requestSubmit()"><span class="small">INC</span></form>"""
        cfg_cell = f"""<form action="/api/wallet-config" method="post" class="wallet-cfg"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><select name="copy_mode"><option value="" {'selected' if not r.get('wallet_config') else ''}>global</option><option value="proportional" {'selected' if eff_mode == 'proportional' and r.get('wallet_config') else ''}>prop</option><option value="fixed" {'selected' if eff_mode == 'fixed' and r.get('wallet_config') else ''}>fixed</option></select><input name="norm_base" value="{eff_base:g}" size="5" title="wallet normalisation base"><input name="fixed_notional" value="{eff_fixed:g}" size="4" title="wallet fixed $"><button title="save wallet override">Set</button></form>"""
        purge_cell = f"""<form action="/api/admin/purge-wallet" method="post" class="purge-form" title="ADMIN MAINTENANCE ONLY: permanently purge wallet"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><button class="purge-btn" title="purge wallet">PURGE</button></form>"""
        meta_link = f'<a href="/wallet-meta/{html.escape(wallet)}" class="meta-edit-link" title="Edit wallet tag / color / note">Meta</a>'
    row_cls = 'user' if r.get('is_user_wallet') else ''
    if not r.get('is_user_wallet') and not included: row_cls += ' excluded-row'
    wallet_sort = html.escape(wallet)
    return f"""
    <tr class="{row_cls}">
      <td class="sticky-wallet{wallet_color_cls}" data-sort="{wallet_sort}"{wallet_title}><a href="/wallet/{wallet}">{wallet[:8]}…{wallet[-6:]}</a>{badge}{meta_badge}</td>
      {core_td(r, isinstance(lead, dict) and 'equity' in lead, lead_pnl, money(lead.get('equity')), f"pair-lead {css_class(lead_pnl)}", "lead.equity")}{core_td(r, isinstance(copy, dict) and 'equity' in copy, copy_pnl, money(copy.get('equity')), f"pair-copy group-divider {css_class(copy_pnl)}", "copy.equity")}
      {core_td(r, isinstance(lead, dict) and 'realized' in lead, lead.get('realized'), dual(fnum(lead.get('realized')), alloc), f"pair-lead {css_class(lead.get('realized'))}", "lead.realized")}{core_td(r, isinstance(copy, dict) and 'realized' in copy, copy.get('realized'), dual(fnum(copy.get('realized')), alloc), f"pair-copy group-divider {css_class(copy.get('realized'))}", "copy.realized")}
      {core_td(r, isinstance(lead, dict) and 'unrealized' in lead, lead.get('unrealized'), dual(fnum(lead.get('unrealized')), alloc), f"pair-lead {css_class(lead.get('unrealized'))}", "lead.unrealized")}{core_td(r, isinstance(copy, dict) and 'unrealized' in copy, copy.get('unrealized'), dual(fnum(copy.get('unrealized')), alloc), f"pair-copy group-divider {css_class(copy.get('unrealized'))}", "copy.unrealized")}
      {core_td(r, isinstance(lead, dict) and ('drawdown' in lead or 'drawdown_usd' in lead), lead_dd_val, format_dd(lead_dd_raw, alloc), f"pair-lead {css_class(lead_dd_val)}", "lead.drawdown")}{core_td(r, isinstance(copy, dict) and ('drawdown' in copy or 'drawdown_usd' in copy), copy_dd_val, format_dd(copy_dd_raw, alloc), f"pair-copy group-divider {css_class(copy_dd_val)}", "copy.drawdown")}
      {core_td(r, isinstance(lead, dict) and ('max_drawdown' in lead or 'maxdd' in lead), lead_maxdd_val, format_dd(lead_maxdd_raw, alloc), f"pair-lead {css_class(lead_maxdd_val)}", "lead.maxdd")}{core_td(r, isinstance(copy, dict) and ('max_drawdown' in copy or 'maxdd' in copy), copy_maxdd_val, format_dd(copy_maxdd_raw, alloc), f"pair-copy group-divider {css_class(copy_maxdd_val)}", "copy.maxdd")}
      {core_td(r, isinstance(delta, dict) and 'equity' in delta, delta.get('equity'), dual(fnum(delta.get('equity')), alloc), f"group-divider {css_class(delta.get('equity'))}", "delta.equity")}{pnl_hr_cell}
      {avg_trade_pct_cell}{win_rate_cell}
      {core_td(r, 'avg_position_usd' in r, r.get('avg_position_usd'), money(r.get('avg_position_usd')), css_class(r.get('avg_position_usd')), "avg_position_usd")}{core_td(r, 'max_position_usd' in r, r.get('max_position_usd'), money(r.get('max_position_usd')), css_class(r.get('max_position_usd')), "max_position_usd")}
      {core_td(r, 'avg_entry_notional_usd' in r, r.get('avg_entry_notional_usd'), money(r.get('avg_entry_notional_usd')), css_class(r.get('avg_entry_notional_usd')), "avg_entry_notional_usd")}{core_td(r, 'pct_entries_ge10' in r, r.get('pct_entries_ge10'), pct(r.get('pct_entries_ge10')), css_class(r.get('pct_entries_ge10')), "pct_entries_ge10")}{core_td(r, 'required_leverage' in r, r.get('required_leverage'), f"{fnum(r.get('required_leverage')):.2f}x", css_class(r.get('required_leverage')), "required_leverage")}
      {lc_cell(r, lc_counts['fills'], "ops-group")}{lc_cell(r, lc_counts['exits'], "ops-group")}{lc_cell(r, lc_counts['pos'], "ops-group group-divider")}
      <td class="controls-cell {'inc-off' if (not r.get('is_user_wallet') and not included) else ''}">{inc_cell}{cfg_cell}{purge_cell}{meta_link}</td>
    </tr>"""

HTML_TEMPLATE = """
<!doctype html><html><head><meta charset="utf-8"><title>HL Copy Engine SSOT</title>
<style>
body{{margin:0;background:#0d1117;color:#c9d1d9;font:12px Arial,Helvetica,sans-serif}} a{{color:#58a6ff;text-decoration:none}} .top{{display:flex;align-items:center;gap:12px;padding:8px 14px;border-bottom:1px solid #222;background:#090d12;position:sticky;top:0;z-index:4;box-shadow:0 2px 8px rgba(0,0,0,.25)}} .live{{background:#003d1f;color:#2ea043;border:1px solid #2ea043;border-radius:12px;padding:2px 8px;font-size:10px}} .muted{{color:#8b949e}} input,select,button{{background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:4px;padding:3px 8px}} button{{cursor:pointer}}
.cards{{display:grid;grid-template-columns:repeat(9,minmax(130px,1fr));gap:8px;padding:10px 14px}} .card{{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:9px;min-height:72px}} .group-card .label{{font-size:10px;color:#8b949e;margin-bottom:6px;border-bottom:1px solid #21262d;padding-bottom:4px}} .metric-line{{display:flex;justify-content:space-between;gap:8px;line-height:1.55}} .metric-line span{{color:#8b949e}} .metric-line b{{font-weight:700}} .pos{{color:#2ea043}} .neg{{color:#ff4d4f}} .zero{{color:#c9d1d9}}
.section{{padding:0 14px 10px}} .panel{{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px;margin-bottom:12px}} .chart-wrap{{position:relative;cursor:zoom-in}} .chart-wrap.expanded{{position:relative;z-index:20}} .chart-wrap.expanded .chart{{height:76vh}} .chart{{width:100%;height:260px;background:#151a21}} .chart *{{vector-effect:non-scaling-stroke}} .pnl-line{{fill:none;stroke:#2ea043;stroke-width:1.6;stroke-linejoin:round;stroke-linecap:round}} .realized-line{{fill:none;stroke:#58a6ff;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .dd-line{{fill:none;stroke:#ff4d4f;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .zero-line{{stroke:#8b949e;stroke-width:1}} .grid-line,.grid-vert{{stroke:#21262d;stroke-width:1}} .axis-label{{fill:#8b949e;font-size:10px}} .hit{{fill:transparent;stroke:none;pointer-events:all}} .crosshair{{stroke:#8b949e;stroke-width:1;stroke-dasharray:3 3;pointer-events:none}} .chart-dot{{fill:#c9d1d9;stroke:#0d1117;stroke-width:1.2;pointer-events:none}} .chart-tip{{position:absolute;left:10px;top:10px;background:#0d1117;border:1px solid #30363d;border-radius:4px;padding:5px 7px;color:#c9d1d9;font-size:11px;pointer-events:none;box-shadow:0 4px 12px rgba(0,0,0,.35)}} .chart-legend{{display:flex;gap:10px;align-items:center;margin-top:6px}} .legend-pnl{{color:#2ea043}} .legend-realized{{color:#58a6ff}} .legend-dd{{color:#ff4d4f}}
.table-wrap{{border:1px solid #21262d;border-radius:6px;background:#0d1117}} table{{width:100%;border-collapse:separate;border-spacing:0;font-size:11px}} th{{position:sticky;top:0;background:#21262d;color:#8b949e;text-align:right;padding:7px;border-bottom:1px solid #30363d;z-index:2}} th:first-child,td:first-child{{text-align:left}} th.sort-active{{background:#303a49;color:#fff;box-shadow:inset 0 -2px 0 #58a6ff}} th.sort-active a{{color:#fff}} th a{{display:block;color:#8b949e}} td{{padding:6px;border-bottom:1px solid #21262d;text-align:right;white-space:nowrap}} tr:nth-child(even){{background:#111820}} tr.user{{background:#071527}} tr:hover{{background:#1b2330}} tr.excluded-row{{}}
.sticky-wallet{{position:sticky;left:0;z-index:3;background:inherit;min-width:132px;border-right:1px solid #30363d}} th.sticky-wallet{{z-index:4;background:#21262d}} .sticky-wallet.wallet-color-blue{{background:linear-gradient(90deg,rgba(88,166,255,.34),rgba(13,17,23,.96))!important;border-left:4px solid #58a6ff}} .sticky-wallet.wallet-color-green{{background:linear-gradient(90deg,rgba(46,160,67,.34),rgba(13,17,23,.96))!important;border-left:4px solid #2ea043}} .sticky-wallet.wallet-color-yellow{{background:linear-gradient(90deg,rgba(210,153,34,.36),rgba(13,17,23,.96))!important;border-left:4px solid #d29922}} .sticky-wallet.wallet-color-red{{background:linear-gradient(90deg,rgba(255,77,79,.34),rgba(13,17,23,.96))!important;border-left:4px solid #ff4d4f}} .sticky-wallet.wallet-color-purple{{background:linear-gradient(90deg,rgba(163,113,247,.34),rgba(13,17,23,.96))!important;border-left:4px solid #a371f7}} .pair-lead{{background:rgba(88,166,255,.045)}} .pair-copy{{background:rgba(46,160,67,.045)}} .ops-group{{background:rgba(210,153,34,.05)}} .group-divider{{border-right:2px solid #30363d!important}} .badge{{background:#0d419d;color:#fff;border-radius:3px;padding:1px 4px;font-size:9px}} .wallet-tag{{background:#30363d;color:#c9d1d9;border-radius:3px;padding:1px 4px;font-size:9px}} .mode{{background:#063d1f;color:#2ea043;border-radius:3px;padding:2px 6px}} .wallet-cfg{{display:inline-flex;gap:3px;margin-left:4px;align-items:center}} .wallet-cfg input{{width:48px;padding:1px 3px}} .wallet-cfg select{{width:70px;padding:1px 3px}} .wallet-cfg button{{padding:1px 4px}} .inc-form{{display:inline-flex;align-items:center;gap:2px;margin-right:6px}} .inc-form input{{padding:0;width:14px;height:14px}} .purge-form{{display:inline-flex;margin-left:4px;align-items:center}} .purge-btn{{border-color:#8b1d1d;background:#3a1111;color:#ff7b72;padding:1px 5px;font-size:10px}} .inc-off{{opacity:1}} .controls-cell{{min-width:315px;text-align:left}} .small{{font-size:11px;color:#8b949e}} .missing{{color:#6e7681!important}} .selected-divider td{{background:#0d1117;border-top:2px solid #58a6ff;border-bottom:1px solid #30363d;color:#8b949e;text-align:left;font-size:10px;letter-spacing:.04em;text-transform:uppercase;padding:6px 8px}} .saving{{opacity:.65}} .saved-flash{{color:#2ea043}} .chart-empty{{height:230px;display:flex;align-items:center;justify-content:center;color:#8b949e}} @media(max-width:1300px){{.cards{{grid-template-columns:repeat(3,minmax(150px,1fr))}}}}
</style></head><body><div class="top"><b>⚡ HL Copy Engine</b><span class="live">POLL</span><span class="muted">Updated: {updated}</span><form action="/api/ui-state" method="post" class="ajax-form" style="display:flex;gap:6px;align-items:center;margin:0"><span class="muted">Normalisation Base:</span><input name="norm_base" value="{norm}" size="8"><span class="muted">Mode:</span><select name="copy_mode"><option>proportional</option><option>fixed</option></select><span class="muted">Fixed $:</span><input name="fixed_notional" value="{fixed}" size="6"><span class="muted">Fee bps:</span><input name="fee_bps" value="{fee}" size="5"><span class="muted">Copy friction bps:</span><input name="copy_friction_bps" value="{friction}" size="5"><button>Set</button></form><span class="muted">Current: {mode}</span><span id="save-status" class="muted"></span><span style="margin-left:auto" class="muted">Auto refresh off</span></div><div class="cards">{cards}</div><div class="section"><b>COMBINED PORTFOLIO — NON-USER WALLETS</b><div class="panel">{chart}</div><div class="small">TRACKED WALLETS ({wallet_count}) — model derived in app from engine SSOT only. {raw_boundary}</div><div class="table-wrap"><table><thead>{table_head}</thead><tbody>{table_rows}</tbody></table></div></div>
<script>(function(){{
let busyUntil=0;
const status=document.getElementById('save-status');
function saveViewState(){{
  const wrap=document.querySelector('.table-wrap');
  try{{sessionStorage.setItem('hlViewState',JSON.stringify({{x:window.scrollX||0,y:window.scrollY||0,tableLeft:wrap?wrap.scrollLeft:0}}));}}catch(e){{}}
}}
function restoreViewState(){{
  try{{
    const raw=sessionStorage.getItem('hlViewState'); if(!raw)return;
    const s=JSON.parse(raw); const wrap=document.querySelector('.table-wrap');
    if(wrap&&typeof s.tableLeft==='number')wrap.scrollLeft=s.tableLeft;
    if(typeof s.x==='number'&&typeof s.y==='number')window.scrollTo(s.x,s.y);
  }}catch(e){{}}
}}
restoreViewState();
function markBusy(ms){{busyUntil=Date.now()+(ms||12000);}}
function isBusy(){{return Date.now()<busyUntil||document.activeElement&&['INPUT','SELECT','TEXTAREA'].includes(document.activeElement.tagName);}}
function flash(msg,cls){{if(!status)return;status.textContent=msg;status.className=cls||'muted';setTimeout(()=>{{status.textContent='';status.className='muted';}},3500);}}
function numericText(txt){{
  const s=String(txt||'').replace(/[,+$%x]/g,' ').replace(/−/g,'-');
  const m=s.match(/-?\\d+(?:\\.\\d+)?/);
  return m?parseFloat(m[0]):0;
}}
function cellSortValue(row, idx, isWallet){{
  const cell=row.cells[idx]; if(!cell)return isWallet?'':0;
  const raw=(cell.dataset&&cell.dataset.sort!==undefined)?cell.dataset.sort:cell.innerText;
  return isWallet?String(raw||'').toLowerCase():numericText(raw);
}}
function sortTableByHeader(link, direction){{
  const th=link.closest('th'); const table=link.closest('table'); if(!th||!table)return;
  const idx=Array.from(th.parentElement.children).indexOf(th);
  const tbody=table.tBodies[0]; const rows=Array.from(tbody.rows);
  const userRows=rows.filter(r=>r.classList.contains('user'));
  const dividerRows=rows.filter(r=>r.classList.contains('selected-divider'));
  const includedRows=rows.filter(r=>!r.classList.contains('user')&&!r.classList.contains('selected-divider')&&!r.classList.contains('excluded-row'));
  const otherRows=rows.filter(r=>!r.classList.contains('user')&&!r.classList.contains('selected-divider')&&r.classList.contains('excluded-row'));
  const isWallet=(link.getAttribute('href')||'').endsWith('/wallet');
  const sorter=(a,b)=>{{
    const av=cellSortValue(a,idx,isWallet), bv=cellSortValue(b,idx,isWallet);
    if(isWallet){{return direction==='asc'?av.localeCompare(bv):bv.localeCompare(av);}}
    return direction==='asc'?av-bv:bv-av;
  }};
  includedRows.sort(sorter);
  otherRows.sort(sorter);
  tbody.replaceChildren(...userRows,...includedRows,...dividerRows,...otherRows);
  table.querySelectorAll('th').forEach(h=>{{h.classList.remove('sort-active'); const a=h.querySelector('a'); if(a&&a.dataset.baseLabel){{a.textContent=a.dataset.baseLabel;}}}});
  th.classList.add('sort-active');
  if(!link.dataset.baseLabel) link.dataset.baseLabel=link.textContent.replace(/\\s*[▲▼]$/,'');
  link.textContent=link.dataset.baseLabel+(direction==='asc'?' ▲':' ▼');
}}

document.addEventListener('focusin',e=>{{if(e.target.matches('input,select,textarea'))markBusy(30000);}});
document.addEventListener('input',e=>{{if(e.target.matches('input,select,textarea'))markBusy(30000);}});
document.addEventListener('change',e=>{{if(e.target.matches('input,select,textarea'))markBusy(15000);}});
document.addEventListener('submit',async e=>{{
  const form=e.target;if(!form.matches('.ajax-form,.wallet-cfg,.inc-form,.purge-form'))return;
  if(form.matches('.purge-form')){{
    const wallet=(new FormData(form).get('wallet')||'').toString();
    const typed=prompt('Type full wallet address to permanently purge');
    if(typed!==wallet){{e.preventDefault();flash('purge cancelled','muted');return;}}
  }}
  e.preventDefault();markBusy(5000);form.classList.add('saving');
  try{{const r=await fetch(form.action,{{method:'POST',body:new FormData(form),headers:{{'X-Requested-With':'fetch'}}}});if(!r.ok)throw new Error('HTTP '+r.status);saveViewState();setTimeout(()=>location.reload(),150);}}
  catch(err){{console.warn('save failed',err);flash('save failed','neg');}}
  finally{{form.classList.remove('saving');restoreViewState();}}
}});

document.addEventListener('click',async e=>{{
  const sortLink=e.target.closest('th a[href^="/sort/"]');
  if(sortLink){{
    e.preventDefault(); markBusy(2500);
    const th=sortLink.closest('th');
    const current=th.classList.contains('sort-active') && /▼\\s*$/.test(sortLink.textContent) ? 'desc' : th.classList.contains('sort-active') ? 'asc' : '';
    const next=current==='desc'?'asc':'desc';
    saveViewState();sortTableByHeader(sortLink,next);restoreViewState();
    try{{await fetch(sortLink.getAttribute('href')+'?direction='+encodeURIComponent(next),{{headers:{{'X-Requested-With':'fetch'}}}});}}catch(err){{console.warn('sort save failed',err);}}
    return;
  }}
  const wrap=e.target.closest('.chart-wrap');
  if(wrap && !e.target.closest('.hit')){{wrap.classList.toggle('expanded');}}
}});

document.addEventListener('mousemove',e=>{{
  const hit=e.target.closest('.hit'); const wrap=e.target.closest('.chart-wrap');
  if(!wrap)return;
  const tip=wrap.querySelector('.chart-tip'); const cross=wrap.querySelector('#chartCrossX'); const dot=wrap.querySelector('#chartDot');
  if(!hit){{if(tip)tip.style.display='none'; if(cross)cross.style.display='none'; if(dot)dot.style.display='none'; return;}}
  if(tip){{tip.textContent=hit.dataset.label||''; tip.style.display='block'; tip.style.left=Math.min(e.offsetX+14, wrap.clientWidth-260)+'px'; tip.style.top='10px';}}
  if(cross){{cross.setAttribute('x1',hit.dataset.x); cross.setAttribute('x2',hit.dataset.x); cross.style.display='block';}}
  if(dot){{dot.setAttribute('cx',hit.dataset.x); dot.setAttribute('cy',hit.dataset.y); dot.style.display='block';}}
}});

// Auto refresh off: use browser refresh when needed.
}})();</script></body></html>
"""


def wants_json_response(request: Request) -> bool:
    return "fetch" in str(request.headers.get("x-requested-with", "")).lower() or "application/json" in str(request.headers.get("content-type", "")).lower()


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return render_home(get_model_state_cached(max_age_sec=5.0))


@app.get("/api/state")
def api_state() -> JSONResponse:
    return JSONResponse(get_model_state_cached(max_age_sec=2.0))


@app.get("/api/metrics")
def api_metrics() -> JSONResponse:
    state = get_model_state_cached(max_age_sec=2.0)
    return JSONResponse({"wallets": state.get("wallet_rows", []), "portfolio": state.get("portfolio", {})})


@app.get("/api/trades/{wallet}")
def api_trades(wallet: str) -> JSONResponse:
    wallet = wallet.lower()
    state = get_model_state_cached(max_age_sec=10.0)
    return JSONResponse([t for t in state.get("copy_trades", []) if str(t.get("wallet", "")).lower() == wallet])


@app.get("/api/equity/{wallet}")
def api_equity(wallet: str) -> JSONResponse:
    wallet = wallet.lower()
    state = get_model_state_cached(max_age_sec=10.0)
    row = (state.get("wallets") or {}).get(wallet, {})
    return JSONResponse(row.get("curve", []))


@app.post("/api/wallet-include", response_class=HTMLResponse)
async def set_wallet_include(request: Request):
    form = await request.form()
    wallet = str(form.get("wallet", "")).strip().lower()
    if wallet:
        ui = load_ui_state()
        inc = dict(ui.get("wallet_include") or {})
        inc[wallet] = parse_bool(form.get("included", ""))
        save_ui_state({"wallet_include": inc})
    if wants_json_response(request):
        return JSONResponse({"ok": True, "ui_state": load_ui_state()})
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')


@app.post("/api/wallet-config", response_class=HTMLResponse)
async def set_wallet_config(request: Request):
    form = await request.form()
    wallet = str(form.get("wallet", "")).strip().lower()
    if wallet:
        ui = load_ui_state()
        cfg = dict(ui.get("wallet_config") or {})
        mode = str(form.get("copy_mode", "")).strip().lower()
        norm_raw = str(form.get("norm_base", "")).strip()
        fixed_raw = str(form.get("fixed_notional", "")).strip()
        if not mode:
            cfg.pop(wallet, None)
        else:
            item: Dict[str, Any] = {}
            if mode in {"proportional", "fixed"}:
                item["copy_mode"] = mode
            if norm_raw:
                item["norm_base"] = max(1.0, fnum(norm_raw, ui.get("norm_base", DEFAULT_NORM_BASE)))
            if fixed_raw:
                item["fixed_notional"] = max(0.01, fnum(fixed_raw, ui.get("fixed_notional", DEFAULT_FIXED_NOTIONAL)))
            if item:
                cfg[wallet] = item
            else:
                cfg.pop(wallet, None)
        save_ui_state({"wallet_config": cfg})
    if wants_json_response(request):
        return JSONResponse({"ok": True, "ui_state": load_ui_state()})
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')


def render_wallet_meta_page(wallet: str, meta: Dict[str, str], saved: bool = False) -> str:
    def opt(name: str, val: str, current: str) -> str:
        sel = ' selected' if val == current else ''
        return f'<option value="{html.escape(val)}"{sel}>{html.escape(val)}</option>'
    tag_opts = "".join(opt("tag", v, meta.get("tag", "none")) for v in sorted(WALLET_META_TAGS))
    color_opts = "".join(opt("color", v, meta.get("color", "none")) for v in sorted(WALLET_META_COLORS))
    saved_banner = '<p style="color:#2ea043;margin:0 0 10px">Saved.</p>' if saved else ""
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>Wallet Meta — {html.escape(wallet)}</title>
<style>
body{{margin:0;background:#0d1117;color:#c9d1d9;font:13px Arial,Helvetica,sans-serif}}
a{{color:#58a6ff;text-decoration:none}}
.top{{display:flex;align-items:center;gap:12px;padding:8px 14px;border-bottom:1px solid #222;background:#090d12}}
.box{{max-width:480px;margin:32px auto;background:#161b22;border:1px solid #21262d;border-radius:8px;padding:20px 24px}}
h2{{margin:0 0 16px;font-size:15px;color:#c9d1d9}}
label{{display:block;margin-bottom:12px;color:#8b949e;font-size:12px}}
label span{{display:block;margin-bottom:4px}}
input,select,textarea{{width:100%;background:#0d1117;color:#c9d1d9;border:1px solid #30363d;border-radius:4px;padding:5px 8px;font-size:13px;box-sizing:border-box}}
textarea{{height:60px;resize:vertical}}
button{{background:#238636;color:#fff;border:none;border-radius:6px;padding:7px 18px;cursor:pointer;font-size:13px;margin-top:6px}}
button:hover{{background:#2ea043}}
.addr{{font-size:11px;color:#8b949e;font-family:monospace;word-break:break-all;margin-bottom:16px}}
</style></head><body>
<div class="top"><b>HL Copy Engine</b><a href="/">← Dashboard</a></div>
<div class="box">
<h2>Wallet meta</h2>
{saved_banner}
<div class="addr">{html.escape(wallet)}</div>
<form action="/api/wallet-meta" method="post">
  <input type="hidden" name="wallet" value="{html.escape(wallet)}">
  <label><span>Tag</span><select name="tag">{tag_opts}</select></label>
  <label><span>Color</span><select name="color">{color_opts}</select></label>
  <label><span>Note</span><textarea name="note" maxlength="120">{html.escape(meta.get("note", ""))}</textarea></label>
  <button type="submit">Save</button>
</form>
</div></body></html>"""


@app.get("/wallet-meta/{wallet}", response_class=HTMLResponse)
def wallet_meta_page(wallet: str, saved: str = "") -> HTMLResponse:
    wallet = wallet.strip().lower()
    ui = load_ui_state()
    meta = get_wallet_meta(ui, wallet)
    return HTMLResponse(render_wallet_meta_page(wallet, meta, saved == "1"))


@app.post("/api/wallet-meta", response_class=HTMLResponse)
async def api_wallet_meta(request: Request):
    form = await request.form()
    wallet = str(form.get("wallet", "")).strip().lower()
    if wallet:
        ui = load_ui_state()
        meta = set_wallet_meta(ui, wallet, form.get("tag", "none"), form.get("note", ""), form.get("color", "none"))
        save_ui_state({"wallet_meta": meta})
    if wants_json_response(request):
        return JSONResponse({"ok": True, "ui_state": load_ui_state()})
    if wallet:
        return HTMLResponse(f'<meta http-equiv="refresh" content="0; url=/wallet-meta/{html.escape(wallet)}?saved=1">')
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')


@app.post("/api/admin/purge-wallet")
async def admin_purge_wallet(request: Request):
    """ADMIN MAINTENANCE ONLY: purge a wallet from active dashboard data files."""
    form = await request.form()
    try:
        proof = purge_wallet_everywhere(str(form.get("wallet", "")))
        if wants_json_response(request):
            return JSONResponse(proof)
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')
    except ValueError as exc:
        payload = {"ok": False, "error": str(exc)}
        if wants_json_response(request):
            return JSONResponse(payload, status_code=400)
        return HTMLResponse(f"<h1>400</h1><pre>{html.escape(str(exc))}</pre>", status_code=400)


def render_live_copy_control_panel() -> str:
    return """
<div class="section live-copy-centre" id="liveCopyPanel">
  <header class="lc-header">
    <div class="lc-title">Live Copy Command Centre</div>
    <div class="lc-header-pills">
      <span class="lc-pill">Wallets: <b id="lcWalletCount">0 / 10</b></span>
      <span id="lcModeCounts" style="display:contents"></span>
      <span class="lc-pill" id="lcAutoSend">Real order sending: n/a</span>
      <span class="lc-pill">WS: <b id="lcWsOverall">OFFLINE</b></span>
    </div>
    <div class="lc-header-actions">
      <button type="button" id="lcRefresh">Refresh</button>
      <button type="button" id="lcGcToggle" class="lc-soft" aria-expanded="false">Global Controls</button>
      <button type="button" data-lc-modal="lcWalletModal">Add wallet</button>
      <div class="lc-gc-popover" id="lcGlobalControlsPanel" aria-hidden="true">
        <div class="lc-popover-head">
          <h3>Global Live Copy Controls</h3>
          <button type="button" id="lcGcClose">close</button>
        </div>
        <p>These override all wallet settings. 0/blank = OFF. Slippage is shown as %. Internally converted where needed.</p>
        <div class="lc-form-grid" id="lcGcForm">
          <label>Max total live exposure ($) <input id="gcMaxTotal" type="number" min="0" step="1" placeholder="0 = disabled"></label>
          <label>Max per-asset directional exposure ($) <input id="gcMaxDir" type="number" min="0" step="1" placeholder="0 = disabled"></label>
          <label>Max per-wallet exposure ($) <input id="gcMaxWallet" type="number" min="0" step="1" placeholder="0 = disabled"></label>
          <label>Max per-order notional ($) <input id="gcMaxOrder" type="number" min="0" step="0.01" placeholder="0 = disabled"></label>
          <label>Marketable slippage % <input id="gcMktPct" type="number" min="0" max="0.50" step="0.01" placeholder="0 = OFF"><span id="gcMktPctState" class="lc-muted"></span></label>
          <label>Adverse close diff % <input id="gcCloseAdv" type="number" min="0" step="0.01" placeholder="0 = OFF"><span id="gcCloseAdvState" class="lc-muted"></span></label>
          <label class="wide">Symbol allowlist (empty = all allowed) <input id="gcAllowlist" type="text" placeholder="BTC,ETH"></label>
          <label class="wide">Symbol blocklist <input id="gcBlocklist" type="text" placeholder="DOGE,SHIB"></label>
        </div>
        <div class="lc-modal-actions">
          <span id="lcGcStatus" class="lc-status"></span>
          <button type="button" id="lcGcSave">Save Global Controls</button>
        </div>
      </div>
    </div>
  </header>
  <div class="lc-safety-strip">
    <span class="lc-pill lc-red" id="lcRealOrders">REAL ORDERS: n/a</span>
    <span id="lcStatus" class="lc-status"></span>
  </div>

  <section class="lc-panel">
    <h3>Real Account</h3>
    <div class="lc-stat-strip" id="lcRealCards" style="grid-template-columns:repeat(auto-fit,minmax(130px,1fr))"></div>
  </section>

  <section class="lc-panel lc-graph-panel">
    <div class="lc-graph-top">
      <div>
        <h3>Live Account / Wallet PnL</h3>
        <p id="lcGraphSubtitle">Portfolio value change from exchange snapshots; realised PnL from actual user closedPnl.</p>
      </div>
      <div class="lc-chart-controls">
        <button type="button" class="active" data-lc-graph-mode="account">Account</button>
        <button type="button" data-lc-graph-mode="wallet_pnl">Selected Wallet PnL</button>
        <button type="button" data-lc-graph-mode="exposure">Exposure</button>
        <button type="button" data-lc-graph-scale="1d">1D</button>
        <button type="button" data-lc-graph-scale="7d">7D</button>
        <button type="button" class="active" data-lc-graph-scale="all">ALL</button>
        <input id="lcGraphStart" type="datetime-local" title="Start datetime">
        <input id="lcGraphEnd" type="datetime-local" title="End datetime">
        <button type="button" id="lcGraphApplyRange">Apply</button>
        <button type="button" id="lcGraphResetRange">Reset</button>
      </div>
    </div>
    <div class="lc-chart-legend" aria-hidden="true">
      <span><i class="lc-line-green"></i>Total PnL</span>
      <span><i class="lc-line-blue"></i>Realized PnL</span>
      <span><i class="lc-line-red"></i>Drawdown</span>
    </div>
    <div class="lc-chart-wrap">
      <svg id="lcEquityChart" viewBox="0 0 1000 330" preserveAspectRatio="none" aria-label="Portfolio value change">
        <line id="lcZeroLine" x1="58" y1="280" x2="976" y2="280" stroke="rgba(148,163,184,.6)" stroke-width="1.2" stroke-dasharray="4 4"/>
        <path id="lcExchangeFill" fill="rgba(33,193,107,.08)" d=""/>
        <polyline id="lcExchangePath" points="" fill="none" stroke="#3fb950" stroke-width="1.5"/>
        <polyline id="lcRealizedPath" points="" fill="none" stroke="#58a6ff" stroke-width="1.4"/>
        <polyline id="lcDrawdownPath" points="" fill="none" stroke="#ff5263" stroke-width="1.4"/>
        <text x="8" y="28" class="lc-axis" id="lcYMax"></text><text x="8" y="155" class="lc-axis" id="lcYMid"></text><text x="8" y="282" class="lc-axis" id="lcYMin"></text>
        <g id="lcXAxisTicks"></g>
      </svg>
    </div>
  </section>

  <section class="lc-panel lc-wallet-panel">
    <h3>Live Copy Wallets</h3>
    <p>Configured wallets from live_config.json. Click a row for real copy details for that leader.</p>
    <div class="lc-table-wrap">
      <table class="lc-wallet-table" style="min-width:1380px">
        <thead><tr><th>Wallet</th><th>Mode / Health</th><th>Live PnL</th><th>Lead↔Copy Diff</th><th>Execution</th><th>Risk / Exposure</th><th>Last Fill</th><th>Controls</th></tr></thead>
        <tbody id="lcWalletRows"><tr><td colspan="12">Loading live-copy config...</td></tr></tbody>
      </table>
    </div>
  </section>

  <section class="lc-tabs">
    <div class="lc-tabbar">
      <button type="button" class="active" data-lc-tab="positions">Real Copy Positions</button>
      <button type="button" data-lc-tab="execqual">Execution Quality</button>
      <button type="button" data-lc-tab="audit">Order Intents / Audit</button>
      <button type="button" data-lc-tab="recon">Reconciliation</button>
      <button type="button" data-lc-tab="ws">WS Health</button>
    </div>
    <div class="lc-tab-panel active" data-lc-panel="positions">
      <section class="lc-panel">
        <h3>Real User Copy Positions</h3>
        <p>Manual ledger vs real exchange account. All sizes signed (positive = long, negative = short). Exchange data requires HL_LIVE_HL_ACCOUNT_ADDRESS configured.</p>
        <div class="lc-table-wrap">
          <table style="min-width:1200px"><thead><tr><th>Coin</th><th>Side</th><th>Ledger size</th><th>Exchange size</th><th>Entry px</th><th>Mark px</th><th>Pos value</th><th>Unrealized PnL</th><th>Leader wallet</th><th>Status</th><th>Last OID</th><th>Last updated</th></tr></thead><tbody id="lcPositionRows"><tr><td colspan="12">Loading...</td></tr></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel" data-lc-panel="execqual">
      <section class="lc-panel">
        <h3>Execution Quality</h3>
        <p>Real fill quality from send_attempts.csv joined to order_intents.csv. Leader-vs-user bps requires reference price from intent.</p>
        <div class="lc-source-strip" id="lcExecQualChips"></div>
        <div class="lc-table-wrap">
          <table style="min-width:1440px"><thead><tr><th>Time</th><th>Wallet</th><th>Coin</th><th>Side</th><th>Status</th><th>Limit px</th><th>Fill avg px</th><th>Size</th><th>OID</th><th>Fill-vs-limit %</th><th>Leader-vs-user %</th><th>Market slip %</th><th>Error</th></tr></thead><tbody id="lcExecQualRows"><tr><td colspan="13">Loading...</td></tr></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel" data-lc-panel="audit">
      <section class="lc-panel">
        <h3>Order Intents / Audit</h3>
        <div class="lc-source-strip" id="lcSourceChips"></div>
        <div class="lc-table-wrap">
          <table><thead><tr><th>Time</th><th>Wallet</th><th>Coin</th><th>Side</th><th>Source/Reason</th><th>Status</th><th>Decision</th><th>Decision reason</th><th>Suggested limit</th><th>Diff %</th><th>Manual?</th><th>Intent note</th><th>Real order result</th></tr></thead><tbody id="lcAuditRows"><tr><td colspan="13">Loading audit summary...</td></tr></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel" data-lc-panel="recon">
      <section class="lc-panel">
        <h3>Ledger vs Exchange</h3>
        <div class="lc-table-wrap">
          <table><thead><tr><th>Severity</th><th>Wallet</th><th>Coin</th><th>Issue</th><th>Count</th><th>Manual ledger</th><th>Exchange</th><th>Last intent</th><th>Last oid</th><th>Last updated</th><th>Action</th></tr></thead><tbody id="lcReconRows"></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel" data-lc-panel="ws">
      <section class="lc-panel">
        <h3>WS Health Detail</h3>
        <div class="lc-table-wrap">
          <table><thead><tr><th>Wallet</th><th>Current</th><th>Grade</th><th>Thread</th><th>Reconnect/min</th><th>Processed</th><th>Raw msg</th><th>Parsed</th><th>Snap seen</th><th>Snap recovered</th><th>Ignored</th><th>Recent errors</th><th>Lifetime errors</th><th>Stale ms</th><th>Data status</th><th>Last close/error</th></tr></thead><tbody id="lcHealthRows"></tbody></table>
        </div>
      </section>
    </div>
  </section>

  <div class="lc-modal-backdrop" id="lcWalletModal" aria-hidden="true">
    <form class="lc-modal" id="lcAddForm">
      <div class="lc-modal-head"><h3>Add Wallet</h3><button type="button" data-lc-close>close</button></div>
      <div class="lc-form-grid">
        <label class="wide">Wallet address<input name="wallet" placeholder="0x wallet address" autocomplete="off"></label>
        <label>Initial mode<select name="mode"><option>LIVE</option><option>CLO</option><option>OFF</option></select></label>
        <label>Copy model<select name="copy_mode"><option value="proportional">proportional</option><option value="fixed">fixed</option></select></label>
        <label>Norm base<input name="norm_base" value="100"></label>
        <label>Fixed notional<input name="fixed_notional" value="10"></label>
        <label>Leader equity base<input name="leader_equity_base" value="10000"></label>
        <label>Max diff %<input name="max_diff_pct" value="0.1"></label>
        <label>Daily loss<input name="daily_loss_limit" value="0"></label>
      </div>
      <div class="lc-modal-actions"><button type="button" data-lc-close>Cancel</button><button type="submit">Add wallet</button></div>
    </form>
  </div>
</div>
<style>
.live-copy-centre{--lc-bg:#070c11;--lc-panel:#0f171f;--lc-line:#223342;--lc-text:#e6edf5;--lc-muted:#8fa3b7;--lc-green:#21c16b;--lc-red:#ff5263;--lc-amber:#f5b84b;--lc-blue:#58a6ff;border-top:1px solid #30363d;margin-top:12px;color:var(--lc-text);font-size:12px}.live-copy-centre *{box-sizing:border-box}.lc-header{display:grid;grid-template-columns:auto 1fr auto;gap:12px;align-items:center;padding:10px 12px;border:1px solid var(--lc-line);background:#0c131a;border-radius:8px;margin-bottom:10px}.lc-title{font-size:20px;font-weight:760}.lc-header-pills,.lc-header-actions,.lc-safety-strip,.lc-source-strip{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.lc-header-pills{justify-content:flex-end}.lc-header-actions{justify-content:flex-end;position:relative}.lc-pill{display:inline-flex;align-items:center;min-height:24px;padding:0 8px;border:1px solid var(--lc-line);border-radius:999px;background:#131f2b;color:var(--lc-muted);font-weight:720;white-space:nowrap}.lc-blue{color:var(--lc-blue);border-color:rgba(88,166,255,.45)}.lc-green{color:var(--lc-green);border-color:rgba(33,193,107,.45)}.lc-red{color:var(--lc-red);border-color:rgba(255,82,99,.45)}.lc-amber,.lc-mode-CLO{color:var(--lc-amber);border-color:rgba(245,184,75,.45)}.live-copy-centre button{min-height:28px;border:1px solid var(--lc-line);border-radius:6px;background:#172437;color:var(--lc-text);padding:0 8px;font-weight:700}.live-copy-centre button.lc-soft{color:var(--lc-amber);border-color:rgba(245,184,75,.55)}.live-copy-centre button.lc-danger{color:var(--lc-red);border-color:rgba(255,82,99,.6);background:#2a1218}.live-copy-centre button[disabled]{opacity:.5}.lc-safety-strip{margin-bottom:10px}.lc-status{font-weight:720}.lc-ok{color:var(--lc-green)}.lc-bad{color:var(--lc-red)}.lc-panel{border:1px solid var(--lc-line);border-radius:8px;background:var(--lc-panel);padding:12px;margin-bottom:10px;min-width:0}.lc-panel h3{margin:0 0 5px 0;font-size:15px}.lc-panel p,.lc-modal p,.lc-gc-popover p{margin:0 0 10px 0;color:var(--lc-muted);font-size:12px}.lc-popover-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px}.lc-popover-head h3{margin:0;font-size:15px}.lc-gc-popover{display:none;position:absolute;right:0;top:36px;width:min(640px,calc(100vw - 36px));max-height:calc(100vh - 90px);overflow:auto;z-index:40;border:1px solid var(--lc-line);border-radius:8px;background:#0d161f;padding:12px;box-shadow:0 18px 48px rgba(0,0,0,.55)}.lc-gc-popover.active{display:block}.lc-graph-panel{min-height:430px}.lc-graph-top{display:grid;grid-template-columns:1fr auto;gap:12px;align-items:start}.lc-chart-controls{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}.lc-chart-controls button,.lc-chart-controls input{min-height:26px;border:1px solid var(--lc-line);border-radius:6px;background:#0a1118;color:var(--lc-muted);padding:0 7px}.lc-chart-controls .active{color:var(--lc-text);border-color:rgba(88,166,255,.55);background:#142337}.lc-stat-strip{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin:8px 0 10px}.lc-stat{border:1px solid var(--lc-line);background:#0a1118;border-radius:7px;padding:8px;min-height:55px}.lc-stat .label{color:var(--lc-muted);font-size:10px;font-weight:720;text-transform:uppercase}.lc-stat .value{margin-top:6px;font-size:16px;font-weight:780}.lc-chart-legend{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin:4px 0 8px;color:var(--lc-muted);font-weight:720}.lc-chart-legend span{display:inline-flex;gap:6px;align-items:center}.lc-chart-legend i{width:18px;height:3px;border-radius:3px;display:inline-block}.lc-line-green{background:#3fb950}.lc-line-blue{background:#58a6ff}.lc-line-red{background:#ff5263}.lc-chart-wrap{position:relative;min-height:330px;border:1px solid var(--lc-line);border-radius:8px;background:linear-gradient(rgba(255,255,255,.035) 1px,transparent 1px) 0 0/100% 20%,linear-gradient(90deg,rgba(255,255,255,.028) 1px,transparent 1px) 0 0/10% 100%,#091017;overflow:hidden}.lc-chart-wrap svg{display:block;width:100%;height:100%;min-height:330px}.lc-axis{fill:var(--lc-muted);font-size:11px}.lc-table-wrap{overflow-x:auto;border:1px solid var(--lc-line);border-radius:8px}.live-copy-centre table{width:100%;border-collapse:collapse;min-width:1320px}.live-copy-centre th,.live-copy-centre td{border-bottom:1px solid var(--lc-line);padding:6px 7px;text-align:left;vertical-align:middle;white-space:nowrap}.live-copy-centre th{color:var(--lc-muted);font-size:10px;font-weight:780;text-transform:uppercase;background:#0a1118}.lc-wallet{font-family:Consolas,Monaco,monospace;color:#d9ebff}.lc-cell-stack{display:grid;gap:4px}.lc-pair{display:grid;grid-template-columns:34px minmax(52px,auto);gap:5px;align-items:baseline}.lc-pair span:first-child{color:var(--lc-muted);font-size:10px;font-weight:780}.lc-pos{color:var(--lc-green);font-weight:760}.lc-neg{color:var(--lc-red);font-weight:760}.lc-muted{color:var(--lc-muted)}.lc-row-OFF{opacity:.58}.lc-mini-actions,.lc-inline-controls{display:flex;gap:4px;align-items:center;flex-wrap:wrap}.lc-wallet-table input,.lc-wallet-table select{width:76px;min-height:26px;background:#0a1118;color:var(--lc-text);border:1px solid var(--lc-line);border-radius:5px;padding:0 6px}.lc-wallet-table select{width:92px}.lc-graph-toggle{display:inline-flex;gap:5px;align-items:center}.lc-graph-toggle input{width:14px;min-height:14px}.lc-tabs{display:grid;gap:8px}.lc-tabbar{display:flex;gap:6px;border-bottom:1px solid var(--lc-line)}.lc-tabbar button{border-bottom:0;border-radius:7px 7px 0 0;color:var(--lc-muted)}.lc-tabbar button.active{color:var(--lc-text);background:var(--lc-panel)}.lc-tab-panel{display:none}.lc-tab-panel.active{display:block}.lc-source-strip{margin-bottom:10px}.lc-modal-backdrop{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,.62);z-index:2000;padding:18px}.lc-modal-backdrop.active{display:flex}.lc-modal{width:min(660px,100%);border:1px solid var(--lc-line);border-radius:8px;background:var(--lc-panel);padding:14px;box-shadow:0 20px 60px rgba(0,0,0,.45)}.lc-modal-head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:10px}.lc-modal-head h3{margin:0}.lc-form-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.lc-form-grid .wide{grid-column:1/-1}.lc-form-grid label{display:grid;gap:5px;color:var(--lc-muted);font-size:10px;font-weight:760;text-transform:uppercase}.lc-form-grid input,.lc-form-grid select{width:100%;min-height:32px;border:1px solid var(--lc-line);border-radius:6px;color:var(--lc-text);background:#0a1118;padding:0 8px}.lc-modal-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:12px;flex-wrap:wrap}@media(max-width:1300px){.lc-header,.lc-graph-top{grid-template-columns:1fr}.lc-header-pills,.lc-header-actions,.lc-chart-controls{justify-content:flex-start}.lc-gc-popover{left:0;right:auto}.lc-stat-strip{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:800px){.lc-stat-strip,.lc-form-grid{grid-template-columns:1fr}.lc-gc-popover{position:static;width:100%;max-height:none;margin-top:8px}}
</style>
<script>
(function(){
const root=document.getElementById('liveCopyPanel'); if(!root) return;
const status=root.querySelector('#lcStatus');
let lcConfig={wallets:{},archived_wallets:{}}, lcHealth={wallets:{}}, lcAudit={last_rows:[]};
let lcGraphMode='account', lcGraphScale='all', lcSelectedWallet='', lcGraphStartMs=0, lcGraphEndMs=0;
function h(v){return String(v==null?'':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function msg(t,bad){if(status){status.textContent=t||'';status.className='lc-status '+(bad?'lc-bad':'lc-ok');}}
async function jget(url){const r=await fetch(url,{headers:{'x-requested-with':'fetch'}});return await r.json();}
async function jpost(url,payload){const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json','x-requested-with':'fetch'},body:JSON.stringify(payload)});const j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.error||r.statusText);return j;}
// intentional blank — helpers below
function num(v,d){const n=parseFloat(v);return Number.isFinite(n)?n:d;}
function isNum(v){const n=parseFloat(v);return Number.isFinite(n);}
function moneyVal(v){return isNum(v)?'$'+Number(v).toLocaleString(undefined,{maximumFractionDigits:2}):'n/a';}
function plainVal(v,digits=2){return isNum(v)?Number(v).toLocaleString(undefined,{maximumFractionDigits:digits}):'n/a';}
function first(row,keys){for(const k of keys){if(row&&row[k]!=null&&row[k]!=='')return row[k];}return '';}
function shortWallet(w){return String(w||'').length>18?String(w).slice(0,10)+'...'+String(w).slice(-6):String(w||'');}
function pill(text,kind){const t=String(text||'n/a');const token=String(kind||t).split(' ')[0].toUpperCase();const cls=['OPEN','LIVE','OK','GOOD','DRY_RUN_FILLED','ALLOWED','ACTIVE'].includes(token)?'lc-green':['STALE','DEGRADED','WARN','CLO','WATCH','QUEUED','CONNECTING','RECONNECTING','PENDING'].includes(token)?'lc-amber':['OFF','OFFLINE','CLOSED','MISSING','DISABLED','ERROR','RECONNECT_OVERDUE'].includes(token)?'lc-red':'';
 return `<span class="lc-pill ${cls}">${h(t)}</span>`;}
function decisionPill(text){const t=String(text||'—');const u=t.toUpperCase();const cls=['WOULD_PLACE_IOC_LIMIT','WOULD_LATE_COPY','WOULD_REDUCE_OR_EXIT','WOULD_EXIT','WOULD_REDUCE'].includes(u)?'lc-green':u==='DO_NOT_MARKET_COPY'?'lc-red':u==='MANUAL_REVIEW'?'lc-amber':'';return `<span class="lc-pill ${cls}">${h(t)}</span>`;}
function tiny(v,n){const s=String(v==null?'':v);return h(s.length>n?s.slice(0,n-1)+'…':s);}
function pair(a,b,cls){return `<div class="lc-pair"><span>${h(a)}</span><b class="${cls||''}">${h(b||'—')}</b></div>`;}
function signed(v){const s=String(v||'—');return s.trim().startsWith('-')?'lc-neg':(s.trim().startsWith('+')?'lc-pos':'');}
function count(obj,key){return Number((obj||{})[key]||0);}
function sourceOf(r){return first(r,['source','fill_source'])||String(first(r,['reason'])).replace('LIVE_','').replace('_DETECTED','')||'—';}
function rowPayload(tr){return {wallet:tr.dataset.wallet,mode:tr.querySelector('[name=mode]').value,copy_mode:tr.querySelector('[name=copy_mode]').value,norm_base:num(tr.querySelector('[name=norm_base]').value,100),fixed_notional:num(tr.querySelector('[name=fixed_notional]').value,10),leader_equity_base:num(tr.querySelector('[name=leader_equity_base]').value,10000),max_diff_pct:num(tr.querySelector('[name=max_diff_pct]').value,0.1),daily_loss_limit:num(tr.querySelector('[name=daily_loss_limit]').value,0)};}
function age(ms){const n=Number(ms||0);if(!n)return '—';const d=Math.max(0,Date.now()-n);return d<60000?Math.round(d/1000)+'s':Math.round(d/60000)+'m';}
function time(ms){const n=Number(ms||0);return n?new Date(n).toLocaleTimeString():'—';}
function tsOf(p){const raw=p.fetched_at_ms||p.timestamp_ms||p.ts||p.time_ms; if(Number(raw)>0)return Number(raw); const s=p.timestamp||p.updated_at||p.created_at||p.time||''; const t=Date.parse(s); return Number.isFinite(t)?t:0;}
function localInputMs(v){const t=Date.parse(v||'');return Number.isFinite(t)?t:0;}
function graphRangeLabel(){if(lcGraphScale==='custom'){const s=lcGraphStartMs?new Date(lcGraphStartMs).toLocaleString():'start';const e=lcGraphEndMs?new Date(lcGraphEndMs).toLocaleString():'now';return `${s} to ${e}`;}return lcGraphScale.toUpperCase();}
function filterScale(points){let start=0,end=0;if(lcGraphScale==='custom'){start=lcGraphStartMs;end=lcGraphEndMs;}else if(lcGraphScale!=='all'){const days=lcGraphScale==='7d'?7:1;start=Date.now()-days*86400000;}return points.filter(p=>{const t=tsOf(p);if(!t)return false;if(start&&t<start)return false;if(end&&t>end)return false;return true;});}
function linePts(series,xOf,yOf){return series.map(p=>`${xOf(p)},${yOf(p.value)}`).join(' ');}
function fmtAxisMoney(v){const n=Number(v);const sign=n<0?'-':'';const a=Math.abs(n);return sign+'$'+(a>=1000?Math.round(a).toLocaleString():a.toLocaleString(undefined,{maximumFractionDigits:a<10?2:1}));}
function tickLabel(ms,minTs,maxTs){const span=maxTs-minTs;const d=new Date(ms);if(span<=2*86400000)return d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});if(span<=10*86400000)return d.toLocaleDateString([], {weekday:'short',day:'numeric'});return d.toLocaleDateString([], {month:'short',day:'numeric'});}
function renderXTicks(minTs,maxTs,getX){
 const g=root.querySelector('#lcXAxisTicks');if(!g)return;const span=maxTs-minTs||1;const ticks=[];for(let i=0;i<5;i++)ticks.push(minTs+(span*i/4));
 g.innerHTML=ticks.map(t=>{const x=getX({timestamp_ms:t});return `<line x1="${x}" y1="280" x2="${x}" y2="286" stroke="rgba(148,163,184,.45)" stroke-width="1"/><text x="${x}" y="306" text-anchor="middle" class="lc-axis">${h(tickLabel(t,minTs,maxTs))}</text>`;}).join('');
}
function clearGraphText(message,label){
 const realizedPath=root.querySelector('#lcRealizedPath'), drawdownPath=root.querySelector('#lcDrawdownPath');
 if(message&&root.querySelector('#lcGraphSubtitle')) root.querySelector('#lcGraphSubtitle').textContent=message;
 root.querySelector('#lcExchangePath').setAttribute('points','');
 root.querySelector('#lcExchangeFill').setAttribute('d','');
 if(realizedPath) realizedPath.setAttribute('points','');
 if(drawdownPath) drawdownPath.setAttribute('points','');
 ['#lcYMax','#lcYMid','#lcYMin'].forEach(id=>{const el=root.querySelector(id);if(el)el.textContent='';});
 const ticks=root.querySelector('#lcXAxisTicks');if(ticks)ticks.innerHTML='';
 const legend=root.querySelector('#lcGraphLegend'), legend2=root.querySelector('#lcGraphLegend2'), legend3=root.querySelector('#lcGraphLegend3');
 if(legend) legend.textContent=label||'Total account PnL';
 if(legend2) legend2.textContent='';
 if(legend3) legend3.textContent='';
}
function renderGraph(){
 const sub=root.querySelector('#lcGraphSubtitle');
 const legend=root.querySelector('#lcGraphLegend');
 const legend2=root.querySelector('#lcGraphLegend2');
 const realizedPath=root.querySelector('#lcRealizedPath');
 const drawdownPath=root.querySelector('#lcDrawdownPath');
 const zeroLine=root.querySelector('#lcZeroLine');
 let points=[], realizedPoints=[], drawdownPoints=[], fillsInRange=[], label='Total account PnL';
 if(lcGraphMode==='account'){
  const hist=lcAudit.exchange_account_history||lcAudit.exchange_history||[];
  const hasUnified=hist.some(p=>isNum(p.unified_portfolio_value));
  label=hasUnified?'Total account PnL / portfolio value change':'Clearinghouse value change (fallback)';
  const visible=filterScale(hist.map(p=>({timestamp:p.timestamp||p.updated_at||'', timestamp_ms:tsOf(p), value:hasUnified?num(p.unified_portfolio_value,NaN):num(p.account_value,NaN)}))).filter(p=>isNum(p.value)).sort((a,b)=>a.timestamp_ms-b.timestamp_ms);
  if(visible.length<2){clearGraphText(`Not enough points in selected range (${graphRangeLabel()}).`,label);return;}
  const selectedStart=visible[0].timestamp_ms, selectedEnd=visible[visible.length-1].timestamp_ms;
  const base=visible[0].value;
  let peak=0;
  points=visible.map(p=>({timestamp:p.timestamp,timestamp_ms:p.timestamp_ms,value:p.value-base}));
  const rawFillPts=(lcAudit.live_graph||{}).realized_pnl_points||[];
  const allRealizedFills=rawFillPts.map(p=>({timestamp_ms:tsOf(p), timestamp:p.timestamp||new Date(tsOf(p)||0).toISOString(), pnl:num(p.pnl,0)})).filter(p=>p.timestamp_ms>0&&isNum(p.pnl)).sort((a,b)=>a.timestamp_ms-b.timestamp_ms);
  const baseRealized=allRealizedFills.filter(p=>p.timestamp_ms<=selectedStart).reduce((a,p)=>a+p.pnl,0);
  let runningRealized=baseRealized;
  fillsInRange=allRealizedFills.filter(p=>p.timestamp_ms>selectedStart&&p.timestamp_ms<=selectedEnd);
  realizedPoints=[{timestamp_ms:selectedStart,timestamp:visible[0].timestamp,value:0}];
  for(const p of fillsInRange){runningRealized+=p.pnl;realizedPoints.push({timestamp_ms:p.timestamp_ms,timestamp:p.timestamp,value:runningRealized-baseRealized});}
  realizedPoints.push({timestamp_ms:selectedEnd,timestamp:visible[visible.length-1].timestamp,value:runningRealized-baseRealized});
  drawdownPoints=points.map(p=>{peak=Math.max(peak,p.value);return {timestamp:p.timestamp,timestamp_ms:p.timestamp_ms,value:p.value-peak};});
 } else {
  const perf=(lcAudit.live_leader_performance||{})[String(lcSelectedWallet||'').toLowerCase()]||{};
  const src=lcGraphMode==='wallet_pnl'?(perf.pnl_series||[]):(perf.exposure_series||[]);
  label=lcGraphMode==='wallet_pnl'?'Selected wallet live PnL':'Selected wallet exposure';
  points=src.map(p=>({timestamp:p.timestamp||p.updated_at||'', timestamp_ms:tsOf(p), value:num(p.value,0)}));
  if(!lcSelectedWallet || !points.length){
   clearGraphText(lcGraphMode==='wallet_pnl'?'wallet PnL history unavailable; click a wallet with live_fills history':'wallet exposure history unavailable; click a wallet with open live exposure',label);
   return;
  }
 }
 if(lcGraphMode!=='account') points=filterScale(points).filter(p=>isNum(p.value)).sort((a,b)=>a.timestamp_ms-b.timestamp_ms);
 if(points.length<2){
  clearGraphText(`Not enough points in selected range (${graphRangeLabel()}).`,label);
  return;
 }
 const allSeries=points.concat(realizedPoints).concat(drawdownPoints);
 const allVals=allSeries.map(p=>num(p.value,0)).concat([0]);
 const mn=Math.min(...allVals), mx=Math.max(...allVals), pad=Math.max(Math.abs(mx-mn)*0.12, 1);
 const ymin=mn-pad, ymax=mx+pad, rng=ymax-ymin||1;
 const plot={left:58,right:976,top:24,bottom:280};
 const getY2=v=>plot.bottom-((num(v)-ymin)/rng)*(plot.bottom-plot.top);
 const minTs=Math.min(...points.map(p=>p.timestamp_ms).filter(Boolean)), maxTs=Math.max(...points.map(p=>p.timestamp_ms).filter(Boolean)), trng=maxTs-minTs||1;
 const getX=p=>plot.left+(((p.timestamp_ms||minTs)-minTs)/trng)*(plot.right-plot.left);
 const pts=linePts(points,getX,getY2);
 root.querySelector('#lcExchangePath').setAttribute('points',pts);
 const fx=getX(points[0]),lx=getX(points[points.length-1]);
 root.querySelector('#lcExchangeFill').setAttribute('d',`M${fx} ${plot.bottom} L${pts} L${lx} ${plot.bottom} Z`);
 if(realizedPath) realizedPath.setAttribute('points',realizedPoints.length?linePts(realizedPoints,getX,getY2):'');
 if(drawdownPath) drawdownPath.setAttribute('points',drawdownPoints.length?linePts(drawdownPoints,getX,getY2):'');
 if(zeroLine){const zy=getY2(0);zeroLine.setAttribute('x1',plot.left);zeroLine.setAttribute('x2',plot.right);zeroLine.setAttribute('y1',zy);zeroLine.setAttribute('y2',zy);}
 renderXTicks(minTs,maxTs,getX);
 root.querySelector('#lcYMax').textContent=fmtAxisMoney(ymax);
 root.querySelector('#lcYMid').textContent=fmtAxisMoney(ymin+rng/2);
 root.querySelector('#lcYMin').textContent=fmtAxisMoney(ymin);
 if(legend) legend.textContent=label;
 if(legend2) legend2.textContent=lcGraphMode==='account'?'Realized PnL':'';
 const legend3=root.querySelector('#lcGraphLegend3'); if(legend3) legend3.textContent=lcGraphMode==='account'?'Drawdown':'';
 if(sub) sub.textContent=lcGraphMode==='account'?`Exchange account graph. ${graphRangeLabel()} range — ${points.length} portfolio pts, ${fillsInRange.length} realized fills.${((lcAudit.live_graph||{}).realized_pnl_points||[]).length>=250?' realized fill series limited to recent fetched fills.':''}`:`${label}. ${graphRangeLabel()} range, ${points.length} live data points.`;
}
function renderCards(){
 const snap=lcAudit.exchange_account_snapshot||{}, manual=lcAudit.manual_live_summary||{};
 const lf=manual.last_filled_manual_order||{}, lr=manual.last_rejected_manual_order||{};
 const unified=snap.available&&snap.unified_portfolio_value!=null?('$'+Number(snap.unified_portfolio_value||0).toLocaleString(undefined,{maximumFractionDigits:2})):'Unavailable';
 const upnl=snap.available&&snap.unrealized_pnl!=null?('$'+Number(snap.unrealized_pnl).toLocaleString(undefined,{maximumFractionDigits:2})):'n/a';
 const rpnl=snap.realized_pnl_selected!=null?('$'+Number(snap.realized_pnl_selected||0).toLocaleString(undefined,{maximumFractionDigits:2})+' '+h(snap.realized_pnl_selected_label||'')):'Unavailable';
 const upnlCls=signCls(snap.unrealized_pnl);
 const rpnlCls=signCls(snap.realized_pnl_selected);
 const exPos=snap.available?((snap.open_positions||[]).length):'n/a';
 const expVal=manual.manual_live_exposure_estimate||0;
 const exp='$'+Number(expVal).toLocaleString(undefined,{maximumFractionDigits:2});
 const expCls=expVal>0?'lc-amber':'';
 const lfStr=lf.coin?(h(lf.coin)+' '+h(lf.actual_side||lf.side||'?')+' '+h(lf.fill_size||'?')+' @ '+h(lf.fill_avg_px||'?')):'none yet';
 const lrErr=lr.error||lr.response||lr.notes||'';
 const lrStatusRaw=String(lr.status||'?');
 const lrStatusLabel={'SYMBOL_UNAVAILABLE':'symbol not found after fresh universe refresh','META_UNAVAILABLE':'could not fetch Hyperliquid universe','SDK_SYMBOL_MAP_UNAVAILABLE':'symbol in meta but SDK map unavailable'}[lrStatusRaw]||lrStatusRaw;
 const lrStr=lr.coin?(h(lr.coin)+' '+h(lr.actual_side||lr.side||'?')+' '+h(lrStatusLabel)+(lrErr?'<br><span class="lc-muted" title="'+h(lrErr)+'">'+tiny(lrErr,90)+'</span>':'')):'none';
 const lrCls=lr.coin?'lc-neg':'';
 const cards=[['Portfolio Value',unified,''],['Unrealized PnL',upnl,upnlCls],['Realized PnL',rpnl,rpnlCls],['Open Positions',exPos,''],['Live Exposure',exp,expCls],['Last Fill',lfStr,''],['Last Reject',lrStr,lrCls]];
 root.querySelector('#lcRealCards').innerHTML=cards.map(([l,v,cls])=>`<div class="lc-stat"><div class="label">${h(l)}</div><div class="value ${cls||''}" style="font-size:12px;word-break:break-all">${v}</div></div>`).join('');
 renderGraph();
}
function moneyFmt(v){return v!=null?('$'+Number(v).toLocaleString(undefined,{maximumFractionDigits:2})):null;}
function signCls(v){return v!=null?(v>0?'lc-pos':v<0?'lc-neg':''):''}
function pnlLabel(st,label){const m={'EXACT':'lc-green','ESTIMATED_FROM_REAL_ORDER_FILLS':'lc-amber','OPEN_ONLY':'lc-blue','ACCOUNT_LEVEL_ONLY':'lc-amber','AMBIGUOUS_COIN_SHARED':'lc-red','N/A':''}; const text=label||({'OPEN_ONLY':'Open PnL','ESTIMATED_FROM_REAL_ORDER_FILLS':'Real fills','ACCOUNT_LEVEL_ONLY':'Account-level only','AMBIGUOUS_COIN_SHARED':'Shared coin','N/A':'No PnL yet','EXACT':'Exact closed PnL'}[st]||st||'No PnL yet'); return `<span class="lc-pill ${m[st]||''}">${h(text)}</span>`;}
function renderWallets(){
 const wrows=lcAudit.live_wallet_rows||[], autoLiveWallets=(lcAudit.auto_live_eligible_wallets||[]).map(w=>String(w).toLowerCase());
 const rows=wrows.map(d=>{
  const mode=String(d.mode||'OFF').toUpperCase(), isLive=autoLiveWallets.includes(String(d.wallet).toLowerCase());
  const hasHistory=(d.filled_count>0||d.exchange_rejected_count>0||d.local_blocked_count>0);
  const typeLabel=isLive?pill('LIVE COPY','LIVE'):pill('TRACKED','INFO');
  const histLabel=(!isLive&&hasHistory&&mode==='OFF')?pill('HISTORICAL','INFO'):'';
  const eligibility=d.eligibility|| (mode==='LIVE'?'COPYING':mode==='CLO'?'CLOSE ONLY':'DISABLED');
  const model=String(d.copy_mode||'')==='fixed'?'fixed':'prop';
  const connPill=mode==='OFF'?`<span class="lc-pill lc-red">COPY DISABLED</span>`:(pill(d.ws_state||d.conn_status||'NO WS HEALTH',d.ws_state||d.conn_status||''));
  const connDetail=mode==='OFF'?'':`<span class="lc-muted" style="font-size:10px">${h(d.ws_reason||d.conn_detail||'')} ${d.last_ws_intent_at?'intent:'+tiny(d.last_ws_intent_at,19):''}</span>`;
  const expStr=moneyFmt(d.current_exposure||0)||'$0';
  const real=d.realized_pnl, unreal=d.unrealized_pnl, net=d.net_pnl, st=d.pnl_status||'N/A';
  const pnlHtml=`<div class="lc-cell-stack"><span class="${signCls(real)}" style="font-size:11px">R: ${moneyFmt(real)||'n/a'}</span><span class="${signCls(unreal)}" style="font-size:11px">U: ${moneyFmt(unreal)||'n/a'}</span><span class="${signCls(net)}" style="font-weight:780">Net: ${moneyFmt(net)||'n/a'}</span>${pnlLabel(st,d.pnl_status_label)}</div>`;
  const bpsCls=d.avg_fill_bps!=null&&d.avg_fill_bps>10?'lc-neg':d.avg_fill_bps!=null&&d.avg_fill_bps<=0?'lc-pos':'';
  const wbpsCls=d.worst_fill_bps!=null&&d.worst_fill_bps>15?'lc-neg':'';
  const diffHtml=`<div class="lc-cell-stack"><span class="${signCls(d.total_diff_usd)}">Total: ${moneyFmt(d.total_diff_usd)||'n/a'}</span><span>Avg: ${moneyFmt(d.avg_diff_usd)||'n/a'}</span><span class="${bpsCls}">Avg bps: ${d.avg_diff_bps!=null?h(d.avg_diff_bps):'n/a'}</span><span class="${wbpsCls}">Worst bps: ${d.worst_diff_bps!=null?h(d.worst_diff_bps):'n/a'}</span></div>`;
  const execHtml=`<div class="lc-cell-stack"><span class="lc-pos">${h(d.filled_count)} filled / ${h(d.exits_count||0)} exits</span><span class="${(d.recent_reject_count||0)>0?'lc-neg':'lc-muted'}">${h(d.recent_reject_count||0)} recent rejects</span><span class="${(d.recent_block_count||0)>0?'lc-amber':'lc-muted'}">${h(d.recent_block_count||0)} recent blocks</span></div>`;
  const riskHtml=`<div class="lc-cell-stack"><span>Exposure: <b>${expStr}</b></span><span>Open pos: <b>${h(d.open_position_count||0)}</b></span><span>DD: ${moneyFmt(d.drawdown)||'n/a'}</span><span>MaxDD: ${moneyFmt(d.max_drawdown)||'n/a'}</span></div>`;
  const lf=d.last_fill||{};
  const lfStr=lf.coin?`${h(lf.coin)} ${h(lf.side)} ${h(lf.size)} @ ${h(lf.avg_px)}<br><span class="lc-muted" style="font-size:10px">${tiny(lf.time||'',22)}</span>`:'<span class="lc-muted">none yet</span>';
  return `<tr class="lc-wallet-row lc-row-${h(mode)}" data-wallet="${h(d.wallet)}">
    <td style="cursor:pointer" title="Click to expand detail"><div class="lc-cell-stack"><span class="lc-wallet">${h(shortWallet(d.wallet))}</span><span>${typeLabel} ${histLabel}</span></div></td>
    <td><div class="lc-cell-stack">${pill(mode,mode)}<span class="lc-muted" style="font-size:10px">${h(eligibility)}</span>${connPill}${connDetail}</div></td>
    <td>${pnlHtml}</td>
    <td>${diffHtml}</td>
    <td>${execHtml}</td>
    <td>${riskHtml}</td>
    <td style="font-size:11px">${lfStr}</td>
    <td><div class="lc-cell-stack"><div class="lc-inline-controls"><select name="mode"><option ${mode==='LIVE'?'selected':''}>LIVE</option><option ${mode==='CLO'?'selected':''}>CLO</option><option ${mode==='OFF'?'selected':''}>OFF</option></select><select name="copy_mode"><option value="proportional" ${model!=='fixed'?'selected':''}>prop</option><option value="fixed" ${model==='fixed'?'selected':''}>fixed</option></select></div><div class="lc-inline-controls"><span class="lc-muted">F</span><input name="fixed_notional" value="${h(d.fixed_notional??10)}" style="width:58px"><span class="lc-muted">N</span><input name="norm_base" value="${h(d.norm_base??100)}" style="width:52px"><span class="lc-muted">B</span><input name="leader_equity_base" value="${h(d.leader_equity_base??10000)}" style="width:70px"><input name="max_diff_pct" type="hidden" value="${h(d.max_diff_pct??0.1)}"><input name="daily_loss_limit" type="hidden" value="${h(d.daily_loss_limit??0)}"></div><div class="lc-mini-actions"><button data-act="save" type="button">Save</button><button data-act="clo" type="button">CLO</button><button data-act="off" type="button">OFF</button><button data-act="archive" class="lc-danger" type="button">Archive</button></div></div></td>
  </tr>`;
 }).join('');
 root.querySelector('#lcWalletRows').innerHTML=rows||'<tr><td colspan="8">No live-copy wallets configured.</td></tr>';
}
function walletDetailHtml(wallet){
 const w=String(wallet).toLowerCase();
 const perf=(lcAudit.live_leader_performance||{})[w]||{};
 const allAttempts=(lcAudit.recent_send_attempts||[]).filter(a=>(a.leader_wallet||a.auto_send_wallet||'').toLowerCase()===w);
 const fills=allAttempts.filter(a=>a.status==='ORDER_FILLED').slice().reverse();
 const exchRej=allAttempts.filter(a=>a.status==='ORDER_REJECTED').slice().reverse();
 const localBlk=allAttempts.filter(a=>a.status&&a.status!=='ORDER_FILLED'&&a.status!=='ORDER_REJECTED'&&a.status!=='CONFIRM_REQUIRED').slice().reverse();
 const intents=(lcAudit.last_rows||[]).filter(r=>(r.leader_wallet||r.wallet||'').toLowerCase()===w).slice(-10).reverse();
 const openPos=perf.current_open_positions||[];
 const wCfg=(lcConfig.wallets||{})[wallet]||{}, modeNow=String(wCfg.mode||'OFF').toUpperCase();
 const isOff=modeNow==='OFF';
 const hasActivity=(fills.length>0||exchRej.length>0||localBlk.length>0);
 let out='<div style="padding:6px;background:#070c11;border-top:2px solid #58a6ff;display:grid;gap:0">';
 if(isOff&&hasActivity) out+=`<div style="padding:6px 8px;color:#f5b84b;font-size:12px;font-weight:780">Copy disabled now. Historical live-copy audit retained below.</div>`;

 // A) Live Performance
 out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">A — Live Performance</b>`;
 out+=`<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:4px">`;
 out+=`<span>Realized: <b class="${signCls(perf.live_realized_pnl)}">${moneyFmt(perf.live_realized_pnl)||'n/a'}</b></span>`;
 out+=`<span>Unrealized: <b class="${signCls(perf.live_unrealized_pnl)}">${moneyFmt(perf.live_unrealized_pnl)||'n/a'}</b></span>`;
 out+=`<span>Net: <b class="${signCls(perf.live_net_pnl)}">${moneyFmt(perf.live_net_pnl)||'n/a'}</b></span>`;
 out+=`<span>Exposure: <b>${moneyFmt(perf.current_exposure)||'n/a'}</b></span>`;
 out+=`<span>DD: <b>${moneyFmt(perf.drawdown)||'n/a'}</b></span>`;
 out+=`<span>MaxDD: <b>${moneyFmt(perf.max_drawdown)||'n/a'}</b></span>`;
 out+=`<span>PnL status: ${pnlLabel(perf.pnl_status||'N/A',perf.pnl_status_label)}</span>`;
 out+=`<span>Attribution: <b>${h(perf.attribution_quality||'N/A')}</b></span>`;
 out+=`<span>Confirmed realized match: <b class="${signCls(perf.confirmed_realized_pnl)}">${moneyFmt(perf.confirmed_realized_pnl)||'n/a'}</b> (${h(perf.realized_match_status||'N/A')})</span>`;
 out+=`<span>Cumulative rejects: <b>${h(perf.exchange_rejected_count||0)}</b>; cumulative local blocks: <b>${h(perf.local_blocked_count||0)}</b>; queued previews / would-send records: <b>${h(perf.preview_count||0)}</b></span>`;
 if(perf.data_quality_notes) out+=`<span class="lc-muted" style="font-size:10px">${h(perf.data_quality_notes)}</span>`;
 out+='</div></div>';

 // B) Open Positions
 out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">B — Open Positions (${openPos.length})</b>`;
 if(openPos.length){
  out+='<table style="margin-top:4px;min-width:auto"><thead><tr><th>Coin</th><th>Side</th><th>Size</th><th>Entry px</th><th>Mark px</th><th>Unrealized PnL</th><th>Exposure</th><th>Exchange match</th></tr></thead><tbody>';
  out+=openPos.map(p=>{const stCls=p.exchange_match==='MATCH'?'lc-green':'lc-red';return `<tr><td><b>${h(p.coin)}</b></td><td>${pill(p.side||'—',p.side||'')}</td><td class="${p.signed_size>0?'lc-pos':'lc-neg'}">${h(p.signed_size)}</td><td>${p.entry_px!=null?h(p.entry_px):'n/a'}</td><td>${p.mark_px!=null?h(p.mark_px):'n/a'}</td><td class="${signCls(p.unrealized_pnl)}">${moneyFmt(p.unrealized_pnl)||'n/a'}</td><td>${moneyFmt(p.exposure)||'n/a'}</td><td><span class="lc-pill ${stCls}">${h(p.exchange_match||'?')}</span></td></tr>`;}).join('');
  out+='</tbody></table>';
 } else { out+=' <span class="lc-muted">no open positions</span>'; }
 out+='</div>';

 // C) Lead vs Copy / Friction
 const fr=perf.friction_fills||[];
 out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">C — Lead vs Copy / Friction</b>`;
 out+=`<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:4px">`;
 out+=`<span class="lc-pill">Total diff: ${moneyFmt(perf.total_diff_usd)||'n/a'}</span>`;
 out+=`<span class="lc-pill">Avg diff: ${moneyFmt(perf.avg_diff_usd)||'n/a'}</span>`;
 out+=perf.avg_diff_bps!=null?`<span class="lc-pill">Avg: ${h(perf.avg_diff_bps)}bps</span>`:'';
 out+=perf.worst_diff_bps!=null?`<span class="lc-pill">Worst: ${h(perf.worst_diff_bps)}bps</span>`:'';
 out+=perf.avg_fill_vs_limit_bps!=null?`<span class="lc-pill">Fill-vs-limit avg: ${h(perf.avg_fill_vs_limit_bps)}bps</span>`:'';
 out+='</div>';
 if(fr.length){
  out+='<table style="margin-top:4px;min-width:auto"><thead><tr><th>Time</th><th>Coin</th><th>Side</th><th>Leader/ref px</th><th>Our fill px</th><th>Size</th><th>Diff $</th><th>Diff bps</th><th>Limit px</th><th>Fill-vs-limit bps</th><th>OID</th></tr></thead><tbody>';
  out+=fr.slice(-20).reverse().map(r=>`<tr><td>${tiny(r.time||'—',22)}</td><td>${h(r.coin||'—')}</td><td>${h(r.side||'—')}</td><td>${r.leader_reference_px!=null?h(r.leader_reference_px):'n/a'}</td><td>${r.copy_fill_px!=null?h(r.copy_fill_px):'n/a'}</td><td>${r.size!=null?h(r.size):'n/a'}</td><td class="${signCls(r.diff_usd)}">${moneyFmt(r.diff_usd)||'n/a'}</td><td>${r.diff_bps!=null?h(r.diff_bps):'n/a'}</td><td>${r.limit_px!=null?h(r.limit_px):'n/a'}</td><td>${r.fill_vs_limit_bps!=null?h(r.fill_vs_limit_bps):'n/a'}</td><td class="lc-wallet">${r.oid?tiny(String(r.oid),18):'n/a'}</td></tr>`).join('');
  out+='</tbody></table>';
 } else { out+=' <span class="lc-muted">reference/fill comparison unavailable</span>'; }
 out+='</div>';

 // D) Execution Audit
 out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">D — Execution Audit</b>`;
  out+=`<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:4px"><span class="lc-pill lc-green">Filled: ${h(perf.filled_count||0)}</span><span class="lc-pill">Exits: ${h(perf.exits_count||0)}</span><span class="lc-pill ${(perf.recent_reject_count||0)>0?'lc-red':''}">Recent rejects: ${h(perf.recent_reject_count||0)}</span><span class="lc-pill ${(perf.recent_block_count||0)>0?'lc-amber':''}">Recent blocks: ${h(perf.recent_block_count||0)}</span></div>`;
 if(fills.length){
  out+='<table style="margin-top:4px;min-width:auto"><thead><tr><th>Time</th><th>Coin</th><th>Side</th><th>Size @ Px</th><th>Pos before→after</th><th>OID</th></tr></thead><tbody>';
  out+=fills.slice(0,10).map(a=>{const fill=a.fill_avg_px?`${h(a.fill_size||'?')} @ ${h(a.fill_avg_px)}`:'—';const pm=(a.position_before!=null&&a.position_after!=null)?`${h(a.position_before)}→${h(a.position_after)}`:'—';return `<tr><td>${tiny(a.created_at||'—',22)}</td><td>${h(a.coin||'—')}</td><td>${h(a.actual_side||a.side||'—')}</td><td>${fill}</td><td>${pm}</td><td class="lc-wallet">${a.oid?tiny(String(a.oid),18):'n/a'}</td></tr>`;}).join('');
  out+='</tbody></table>';
 } else { out+=' <span class="lc-muted">no real copied fills yet</span>'; }
 out+='</div>';

 // E) Rejects / Blocks
 out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">E — Rejects &amp; Blocks</b>`;
 if(exchRej.length){
  out+=`<div style="margin-top:4px"><span class="lc-pill lc-red">Exchange rejected (${exchRej.length})</span>`;
  out+='<table style="margin-top:4px;min-width:auto"><thead><tr><th>Time</th><th>Coin</th><th>Side</th><th>Error</th></tr></thead><tbody>';
  out+=exchRej.slice(0,5).map(a=>`<tr><td>${tiny(a.created_at||'—',22)}</td><td>${h(a.coin||'—')}</td><td>${h(a.actual_side||a.side||'—')}</td><td title="${h(a.error||'')}">${tiny(a.error||'',50)}</td></tr>`).join('');
  out+='</tbody></table></div>';
 }
 if(localBlk.length){
  out+=`<div style="margin-top:4px"><span class="lc-pill lc-amber">Local blocked (${localBlk.length})</span>`;
  out+='<table style="margin-top:4px;min-width:auto"><thead><tr><th>Time</th><th>Coin</th><th>Status (reason)</th></tr></thead><tbody>';
  out+=localBlk.slice(0,5).map(a=>`<tr><td>${tiny(a.created_at||'—',22)}</td><td>${h(a.coin||'—')}</td><td>${h(a.status||'—')}</td></tr>`).join('');
  out+='</tbody></table></div>';
 }
 if(!exchRej.length&&!localBlk.length) out+=' <span class="lc-muted">none</span>';
 out+='</div>';

 // F) Recent Intents
 out+=`<div style="padding:6px 8px"><b style="color:#58a6ff">F — Recent Order Intents (${intents.length})</b>`;
 if(intents.length){
  out+=' ';
  out+=intents.slice(0,10).map(r=>`<span class="lc-muted" style="font-size:10px">${h(r.coin||'?')} ${h(r.side||'?')} ${decisionPill(r.execution_decision||'—')} ${tiny(r.created_at||'',20)}</span>`).join(' · ');
 } else { out+=' <span class="lc-muted">none in recent window</span>'; }
 out+='</div>';
 out+='</div>';
 return out;
}
function renderPositions(){
 const rows=lcAudit.real_copy_positions||[];
 root.querySelector('#lcPositionRows').innerHTML=rows.map(r=>{
  const szCls=r.signed_size!=null?(r.signed_size>0?'lc-pos':'lc-neg'):'';
  const exCls=r.exchange_signed_size!=null?(r.exchange_signed_size>0?'lc-pos':'lc-neg'):'';
  const stRaw=r.ledger_vs_exchange||'—';
  const stCls=(stRaw==='MATCH'||String(stRaw).startsWith('SHARED_SYMBOL_NET_MATCH')||String(stRaw).startsWith('SHARED_SYMBOL_SLEEVE_TRACKED'))?'lc-green':stRaw==='EXCHANGE_UNAVAILABLE'?'':'lc-red';
  const upnlCls=r.unrealized_pnl!=null?(r.unrealized_pnl>0?'lc-pos':r.unrealized_pnl<0?'lc-neg':''):'';
  const pv=r.position_value!=null?('$'+Number(r.position_value).toLocaleString(undefined,{maximumFractionDigits:2})):'n/a';
  const upnlStr=r.unrealized_pnl!=null?('$'+Number(r.unrealized_pnl).toLocaleString(undefined,{maximumFractionDigits:2})):'n/a';
  return `<tr><td><b>${h(r.coin||'—')}</b></td><td>${pill(r.side||'—',r.side||'')}</td><td class="${szCls}">${r.signed_size!=null?h(r.signed_size):'n/a'}</td><td class="${exCls}">${r.exchange_signed_size!=null?h(r.exchange_signed_size):'n/a'}</td><td>${r.entry_px!=null?h(r.entry_px):'n/a'}</td><td>${r.mark_px!=null?h(r.mark_px):'n/a'}</td><td>${pv}</td><td class="${upnlCls}">${upnlStr}</td><td class="lc-wallet">${h(r.leader_wallet?shortWallet(r.leader_wallet):'—')}</td><td title="${h(r.reconciliation_note||'')}"><span class="lc-pill ${stCls}">${h(stRaw)}</span></td><td class="lc-wallet">${tiny(String(r.last_oid||'—'),24)}</td><td>${tiny(r.last_updated_at||'—',22)}</td></tr>`;
 }).join('')||'<tr><td colspan="12">No real copy positions tracked. Positions appear here after the copy service places live orders.</td></tr>';
}
function renderExecQuality(){
 const rows=lcAudit.execution_quality_rows||[];
 const qs=lcAudit.execution_quality_summary||{};
 const filled=rows.filter(r=>r.status==='ORDER_FILLED');
 const exchRej=rows.filter(r=>r.status==='ORDER_REJECTED');
 const localBlk=rows.filter(r=>r.status&&r.status!=='ORDER_FILLED'&&r.status!=='ORDER_REJECTED'&&r.status!=='CONFIRM_REQUIRED');
 const previews=rows.filter(r=>r.status==='CONFIRM_REQUIRED');
 const bpsVals=filled.map(r=>r.fill_bps).filter(v=>v!=null);
 const avgBps=bpsVals.length?Math.round(bpsVals.reduce((a,b)=>a+b,0)/bpsVals.length*10)/10:null;
 const worstBps=bpsVals.length?Math.round(Math.max(...bpsVals)*10)/10:null;
 const lastFill=filled[0]||null;
 root.querySelector('#lcExecQualChips').innerHTML=[
  ['Filled',filled.length,'lc-green'],
  ['Exchange rejected',exchRej.length,exchRej.length?'lc-red':''],
  ['Local blocked',localBlk.length,localBlk.length?'lc-amber':''],
  ['Queued previews / would-send records',previews.length,''],
  ['Avg fill-vs-limit',avgBps!=null?avgBps+'bps':'n/a',avgBps!=null&&avgBps>5?'lc-amber':''],
  ['Worst fill-vs-limit',worstBps!=null?worstBps+'bps':'n/a',worstBps!=null&&worstBps>10?'lc-red':''],
  ['Last fill',lastFill?(lastFill.coin+' '+lastFill.side):'none',''],
 ].map(([k,v,cls])=>`<span class="lc-pill ${cls}">${h(k)}: <b>${h(v)}</b></span>`).join('');
 root.querySelector('#lcExecQualRows').innerHTML=rows.map(r=>{
  const st=String(r.status||'—');
  const stCls=st==='ORDER_FILLED'?'lc-green':st==='ORDER_REJECTED'?'lc-red':st==='CONFIRM_REQUIRED'?'lc-blue':st?'lc-amber':'';
  const bCls=r.fill_bps!=null?(r.fill_bps>10?'lc-neg':r.fill_bps<=0?'lc-pos':''):'';
  const lbCls=r.leader_bps!=null?(r.leader_bps>10?'lc-neg':r.leader_bps<=0?'lc-pos':''):'';
  const pct=v=>v!=null?(Number(v)/100).toFixed(4)+'%':'n/a';
  const mkt=r.marketable_bps!=null?(Number(r.marketable_bps)<=0?'OFF':(Number(r.marketable_bps)/100).toFixed(2)+'%'):'n/a';
  return `<tr><td>${tiny(r.time||'—',22)}</td><td class="lc-wallet">${h(r.leader_wallet?shortWallet(r.leader_wallet):'—')}</td><td>${h(r.coin||'—')}</td><td>${h(r.side||'—')}</td><td><span class="lc-pill ${stCls}">${h(st)}</span></td><td>${r.limit_px!=null?h(r.limit_px):'n/a'}</td><td>${r.fill_avg_px!=null?h(r.fill_avg_px):'n/a'}</td><td>${r.fill_size!=null?h(r.fill_size):'n/a'}</td><td class="lc-wallet">${r.oid?tiny(String(r.oid),18):'n/a'}</td><td class="${bCls}">${pct(r.fill_bps)}</td><td class="${lbCls}">${pct(r.leader_bps)}</td><td>${mkt}</td><td title="${h(r.error||'')}">${tiny(r.error||'',28)}</td></tr>`;
 }).join('')||'<tr><td colspan="13">No execution data. Real fills appear here after send_attempts.csv is populated.</td></tr>';
}
function renderAudit(){
 const rows=(lcAudit.last_rows||[]).slice(-10).reverse();
 const reason=lcAudit.reason_counts||{}, decisionCounts=lcAudit.execution_decision_counts||{}, decisionReasonCounts=lcAudit.decision_reason_counts||{}, manualCounts=lcAudit.manual_reconcile_required_counts||{}, errorCounts=lcAudit.market_data_error_counts||{};
 root.querySelector('#lcSourceChips').innerHTML=[
  ['WS fast path',count(decisionCounts,'WOULD_PLACE_IOC_LIMIT'),'lc-green'],
  ['Exits/reduce',count(decisionCounts,'WOULD_REDUCE_OR_EXIT'),'lc-green'],
  ['Late copy',count(decisionCounts,'WOULD_LATE_COPY'),''],
  ['Do not copy',count(decisionCounts,'DO_NOT_MARKET_COPY'),'lc-red'],
  ['Manual review',count(decisionCounts,'MANUAL_REVIEW')+count(manualCounts,'True')+count(manualCounts,'true'),'lc-amber'],
  ['Quote unavail',count(errorCounts,'EXECUTABLE_QUOTE_UNAVAILABLE')+count(decisionReasonCounts,'RECOVERY_QUOTE_UNAVAILABLE'),'lc-amber'],
  ['Recent order warnings',lcAudit.recent_send_warning_group_count||0,(lcAudit.recent_send_warning_group_count||0)>0?'lc-amber':''],
 ].map(([k,v,cls])=>`<span class="lc-pill ${cls}">${h(k)}: <b>${h(v)}</b></span>`).join('');
 const sendByIntent={};for(const a of(lcAudit.recent_send_attempts||[])){const iid=String(a.intent_id||'');if(iid)sendByIntent[iid]=a;}
 root.querySelector('#lcAuditRows').innerHTML=rows.map(r=>{const iid=String(r.intent_id||'');const attempt=iid?sendByIntent[iid]:null;const execDec=String(r.execution_decision||'');let realRes='NO_SEND_ATTEMPT';if(attempt){const st=String(attempt.status||'');realRes=st==='ORDER_FILLED'?'REAL_ORDER_FILLED':st==='ORDER_REJECTED'?'EXCHANGE_REJECTED':st||'NO_SEND_ATTEMPT';}else if(execDec==='WOULD_PLACE_IOC_LIMIT'){realRes='WOULD_SEND_ONLY';}const rawNote=first(r,['notes','message']);const displayNote=rawNote&&rawNote.indexOf('dry-run simulated fill')!==-1?'legacy intent note: leader WS fill detected; check real order result':rawNote;const rrCls=realRes==='REAL_ORDER_FILLED'?'lc-green':realRes==='EXCHANGE_REJECTED'?'lc-red':realRes==='WOULD_SEND_ONLY'?'lc-amber':'lc-muted';return `<tr><td>${h(first(r,['created_at','timestamp_iso','time']))}</td><td class="lc-wallet">${h(shortWallet(first(r,['leader_wallet','wallet'])))}</td><td>${h(first(r,['coin','asset']))}</td><td>${h(first(r,['side']))}</td><td><div class="lc-cell-stack"><span>${h(sourceOf(r))}</span><span class="lc-muted">${tiny(first(r,['reason']),32)}</span></div></td><td>${pill(first(r,['status'])||'—')}</td><td>${decisionPill(first(r,['execution_decision'])||'—')}</td><td>${tiny(first(r,['decision_reason']),34)}</td><td>${h(first(r,['suggested_limit_price','target_price']))}</td><td>${h(first(r,['adverse_diff_pct','diff_pct','real_diff_pct','price_diff_pct']))}</td><td>${h(first(r,['manual_reconcile_required']))}</td><td title="${h(displayNote)}">${tiny(displayNote,60)}</td><td><span class="lc-pill ${rrCls}">${h(realRes)}</span></td></tr>`;}).join('')||'<tr><td colspan="13">No audit rows found.</td></tr>';
 const recon=lcAudit.manual_reconciliation_rows||[];
 root.querySelector('#lcReconRows').innerHTML=recon.map(r=>{const sideCls=r.side==='LONG'?'lc-pos':r.side==='SHORT'?'lc-neg':'';const cnt=Number(r.count||1);const action=r.action_available?`<button type="button" data-recon-act="archive-ledger-row" data-wallet="${h(r.wallet||'')}" data-coin="${h(r.coin||'')}" data-issue="${h(r.issue||'')}" data-manual-size="${h(r.manual_signed_size??'')}">Archive ledger row</button><div class="lc-muted">Ledger cleanup only. No exchange order.</div>`:'';return `<tr><td>${pill(r.severity||'INFO',r.severity||'INFO')}</td><td class="lc-wallet" title="${h(r.wallet||'unknown')}">${h(r.wallet?shortWallet(r.wallet):'unknown')}</td><td>${h(r.coin||'—')}</td><td title="${h(r.latest_error||r.error||'')}">${h(r.issue||'n/a')}</td><td>${h(cnt>1?cnt:1)}</td><td class="${sideCls}">${h(r.side||'')} ${h(r.manual_signed_size??'n/a')}</td><td>${h(r.exchange_signed_size??'n/a')}</td><td class="lc-wallet">${tiny(String(r.last_intent_id||'—'),36)}</td><td class="lc-wallet">${tiny(String(r.last_oid||'—'),24)}</td><td>${h(r.last_updated_at||'—')}</td><td>${action}</td></tr>`;}).join('')||'<tr><td colspan="11">No reconciliation issues.</td></tr>';
}
function renderHealth(){
 const rows=Object.entries(lcHealth.wallets||{}).sort().map(([wallet,wh])=>{const current=String(wh.current_status||wh.effective_status||wh.status||'OFFLINE');const effective=String(wh.effective_status||wh.status||current);const grade=String(wh.current_health_grade||wh.health_grade||'—');const alive=!!wh.thread_alive||!!wh.worker_thread_alive;const fatal=Number(wh.fatal_error_count||0);const label=(current==='STALE'&&alive&&fatal===0)?'STALE / current idle':current;const lastErr=wh.last_error_repr||wh.last_error||wh.last_close_msg||'';const recentErr=Number(wh.recent_error_count||0);const lifeErr=Number(wh.lifetime_error_count??wh.error_count??0);return `<tr><td class="lc-wallet">${h(shortWallet(wallet))}</td><td title="effective: ${h(effective)}">${pill(label,current)}</td><td>${pill(grade,grade)}</td><td>${h(alive?'alive':'down')}</td><td>${Number(wh.reconnects_per_min||0).toFixed(2)}</td><td>${h(wh.processed_count??0)}</td><td>${h(wh.raw_message_count??0)}</td><td>${h(wh.parsed_fill_message_count??0)}</td><td>${h(wh.snapshot_fill_seen_count??0)}</td><td>${h(wh.snapshot_fill_recovered_count??0)}</td><td>${h(wh.ignored_snapshot_count??0)}</td><td>${h(recentErr)}</td><td>${h(lifeErr)}</td><td>${h(wh.transport_stale_ms??wh.stale_ms??'')}</td><td>${pill(wh.data_status||'—',wh.data_status||'')}</td><td title="${h(lastErr)}">${tiny(lastErr,70)}</td></tr>`;}).join('');
 root.querySelector('#lcHealthRows').innerHTML=rows||'<tr><td colspan="16">WS service not running or no wallet health file found.</td></tr>';
}
function render(){
 const wallets=lcConfig.wallets||{}, walletEntries=Object.values(wallets);
 const tracked=walletEntries.length;
 const liveCount=walletEntries.filter(w=>w&&String(w.mode||'').toUpperCase()==='LIVE').length;
 const cloCount=walletEntries.filter(w=>w&&String(w.mode||'').toUpperCase()==='CLO').length;
 const offCount=walletEntries.filter(w=>w&&String(w.mode||'').toUpperCase()==='OFF').length;
 const autoLiveCount=lcAudit.auto_live_wallet_count||0;
 root.querySelector('#lcWalletCount').textContent=`${tracked} / 10`;
 const mc=root.querySelector('#lcModeCounts');
 if(mc) mc.innerHTML=pill('LIVE '+liveCount,'LIVE')+' '+pill('CLO '+cloCount,'CLO')+' '+pill('OFF '+offCount,'OFF');
 const as=root.querySelector('#lcAutoSend');
 if(as) as.innerHTML='Real order sending: '+(autoLiveCount>0?pill('ON','LIVE'):pill('OFF','OFF'));
 const wsOverall=String(lcHealth.overall||'OFFLINE').toUpperCase();
 root.querySelector('#lcWsOverall').textContent=(['CLOSED','DEGRADED','DISABLED','OFFLINE'].includes(wsOverall))?'OFFLINE':wsOverall;
 const ro=root.querySelector('#lcRealOrders');
 if(ro){const hasRealFills=(lcAudit.execution_quality_rows||[]).some(r=>r.status==='ORDER_FILLED');ro.className='lc-pill '+(hasRealFills?'lc-green':'lc-red');ro.textContent=hasRealFills?'REAL ORDERS: SERVICE ACTIVE':'REAL ORDERS: APP DISABLED';}
 renderCards(); renderWallets(); renderAudit(); renderHealth(); renderPositions(); renderExecQuality();
}
async function refresh(quiet){try{if(!quiet)msg('Loading...');const [cfg,health,audit,gcr]=await Promise.all([jget('/api/live-config'),jget('/api/live-ws-health'),jget('/api/live-audit-summary'),jget('/api/global-controls')]);lcConfig=cfg.config||{wallets:{}};lcHealth=health.health||{};lcAudit=audit||{};render();loadGcForm(gcr.global_controls||{});if(!quiet)msg('Loaded');}catch(e){msg(e.message||String(e),true);}}
function loadGcForm(gc){
  const f=(id,v)=>{const el=root.querySelector('#'+id);if(el&&v!=null)el.value=v;};
  const st=(id,v)=>{const el=root.querySelector('#'+id);if(el)el.textContent=Number(v||0)<=0?'OFF':'';};
  f('gcMaxTotal',gc.max_total_live_exposure_usd||0);f('gcMaxDir',gc.max_asset_directional_exposure_usd||0);
  f('gcMaxWallet',gc.max_wallet_exposure_usd||0);f('gcMaxOrder',gc.max_order_notional_usd||0);
  const mktPct=gc.marketable_slippage_pct!=null?gc.marketable_slippage_pct:(Number(gc.marketable_bps||0)/100);
  const closePct=gc.max_close_adverse_diff_pct!=null?gc.max_close_adverse_diff_pct:0;
  f('gcMktPct',mktPct);f('gcCloseAdv',closePct);st('gcMktPctState',mktPct);st('gcCloseAdvState',closePct);
  f('gcAllowlist',(gc.symbol_allowlist||[]).join(','));f('gcBlocklist',(gc.symbol_blocklist||[]).join(','));
}
root.querySelector('#lcRefresh').addEventListener('click',()=>refresh());
const gcPanel=root.querySelector('#lcGlobalControlsPanel'), gcToggle=root.querySelector('#lcGcToggle'), gcClose=root.querySelector('#lcGcClose');
function setGcOpen(open){if(!gcPanel||!gcToggle)return;gcPanel.classList.toggle('active',!!open);gcPanel.setAttribute('aria-hidden',open?'false':'true');gcToggle.setAttribute('aria-expanded',open?'true':'false');}
if(gcToggle) gcToggle.addEventListener('click',e=>{e.stopPropagation();setGcOpen(!(gcPanel&&gcPanel.classList.contains('active')));});
if(gcClose) gcClose.addEventListener('click',()=>setGcOpen(false));
document.addEventListener('click',e=>{if(gcPanel&&gcPanel.classList.contains('active')&&!gcPanel.contains(e.target)&&e.target!==gcToggle)setGcOpen(false);});
root.querySelector('#lcGcSave').addEventListener('click',async()=>{
  const gs=root.querySelector('#lcGcStatus');
  const g=id=>parseFloat(root.querySelector('#'+id).value)||0;
  const gl=id=>(root.querySelector('#'+id).value||'').split(',').map(s=>s.trim().toUpperCase()).filter(Boolean);
  try{gs.textContent='Saving...';gs.className='lc-status';
    await jpost('/api/global-controls',{max_total_live_exposure_usd:g('gcMaxTotal'),max_asset_directional_exposure_usd:g('gcMaxDir'),max_wallet_exposure_usd:g('gcMaxWallet'),max_order_notional_usd:g('gcMaxOrder'),marketable_slippage_pct:g('gcMktPct'),max_close_adverse_diff_pct:g('gcCloseAdv'),symbol_allowlist:gl('gcAllowlist'),symbol_blocklist:gl('gcBlocklist')});
    gs.textContent='Saved';gs.className='lc-status lc-ok';}catch(e){gs.textContent=e.message||String(e);gs.className='lc-status lc-bad';}
});
root.querySelectorAll('[data-lc-modal]').forEach(btn=>btn.addEventListener('click',()=>{const m=root.querySelector('#'+btn.dataset.lcModal);if(m){m.classList.add('active');m.setAttribute('aria-hidden','false');}}));
root.querySelectorAll('[data-lc-close]').forEach(btn=>btn.addEventListener('click',()=>{const m=btn.closest('.lc-modal-backdrop');if(m){m.classList.remove('active');m.setAttribute('aria-hidden','true');}}));
root.querySelectorAll('[data-lc-tab]').forEach(btn=>btn.addEventListener('click',()=>{root.querySelectorAll('[data-lc-tab]').forEach(b=>b.classList.remove('active'));root.querySelectorAll('[data-lc-panel]').forEach(p=>p.classList.remove('active'));btn.classList.add('active');root.querySelector(`[data-lc-panel="${btn.dataset.lcTab}"]`).classList.add('active');}));
root.querySelectorAll('[data-lc-graph-mode]').forEach(btn=>btn.addEventListener('click',()=>{lcGraphMode=btn.dataset.lcGraphMode;root.querySelectorAll('[data-lc-graph-mode]').forEach(b=>b.classList.remove('active'));btn.classList.add('active');renderGraph();}));
root.querySelectorAll('[data-lc-graph-scale]').forEach(btn=>btn.addEventListener('click',()=>{lcGraphScale=btn.dataset.lcGraphScale;lcGraphStartMs=0;lcGraphEndMs=0;const s=root.querySelector('#lcGraphStart'),e=root.querySelector('#lcGraphEnd');if(s)s.value='';if(e)e.value='';root.querySelectorAll('[data-lc-graph-scale]').forEach(b=>b.classList.remove('active'));btn.classList.add('active');renderGraph();}));
const applyRange=root.querySelector('#lcGraphApplyRange');
if(applyRange) applyRange.addEventListener('click',()=>{const s=root.querySelector('#lcGraphStart'),e=root.querySelector('#lcGraphEnd');lcGraphStartMs=localInputMs(s&&s.value);lcGraphEndMs=localInputMs(e&&e.value);lcGraphScale='custom';root.querySelectorAll('[data-lc-graph-scale]').forEach(b=>b.classList.remove('active'));renderGraph();});
const resetRange=root.querySelector('#lcGraphResetRange');
if(resetRange) resetRange.addEventListener('click',()=>{lcGraphScale='all';lcGraphStartMs=0;lcGraphEndMs=0;const s=root.querySelector('#lcGraphStart'),e=root.querySelector('#lcGraphEnd');if(s)s.value='';if(e)e.value='';root.querySelectorAll('[data-lc-graph-scale]').forEach(b=>b.classList.toggle('active',b.dataset.lcGraphScale==='all'));renderGraph();});
root.querySelector('#lcAddForm').addEventListener('submit',async e=>{e.preventDefault();const fd=new FormData(e.currentTarget);const payload=Object.fromEntries(fd.entries());try{msg('Updating...');await jpost('/api/live-config/add-wallet',payload);e.currentTarget.reset();const m=e.currentTarget.closest('.lc-modal-backdrop');if(m)m.classList.remove('active');await refresh(true);msg('Updated: '+shortWallet(payload.wallet)+' -> SAVED');}catch(err){msg(err.message,true);}});
root.querySelector('#lcReconRows').addEventListener('click',async e=>{const btn=e.target.closest('button[data-recon-act="archive-ledger-row"]');if(!btn)return;const payload={wallet:btn.dataset.wallet||'',coin:btn.dataset.coin||'',issue:btn.dataset.issue||'',manual_signed_size:btn.dataset.manualSize||''};if(!window.confirm('Archive this stale app ledger row only? This will not place an exchange order.'))return;try{msg('Archiving ledger row...');await jpost('/api/manual-reconciliation/archive-ledger-row',payload);await refresh(true);msg('Archived ledger row for '+payload.coin+'. No exchange order was placed.');}catch(err){msg(err.message||String(err),true);}});
root.querySelector('#lcWalletRows').addEventListener('click',async e=>{
 const btn=e.target.closest('button[data-act]');
 if(btn){const tr=btn.closest('tr');const wallet=tr.dataset.wallet;const short=shortWallet(wallet);try{if(btn.dataset.act==='archive'&&!window.confirm('Archive removes from config only. Audit/history preserved.')){msg('Cancelled');return;}msg('Updating...');if(btn.dataset.act==='save'){await jpost('/api/live-config/set-wallet',rowPayload(tr));await refresh(true);msg('Updated: '+short+' -> SAVED');}if(btn.dataset.act==='clo'){await jpost('/api/live-config/set-mode',{wallet,mode:'CLO'});await refresh(true);msg('Updated: '+short+' -> CLO');}if(btn.dataset.act==='off'){await jpost('/api/live-config/set-mode',{wallet,mode:'OFF'});await refresh(true);msg('Updated: '+short+' -> OFF');}if(btn.dataset.act==='archive'){await jpost('/api/live-config/remove-wallet',{wallet,archive:true});await refresh(true);msg('Updated: '+short+' -> ARCHIVED');}}catch(err){msg(err.message,true);}return;}
 const tr=e.target.closest('tr.lc-wallet-row');
 if(!tr||!tr.dataset.wallet) return;
 const wallet=tr.dataset.wallet;
 lcSelectedWallet=wallet;
 renderGraph();
 const detailId='lcdet-'+wallet.replace(/[^a-z0-9]/gi,'');
 const existing=root.querySelector('#'+detailId);
 if(existing){existing.remove();return;}
 const detRow=document.createElement('tr');
 detRow.id=detailId;
 detRow.innerHTML=`<td colspan="12" style="padding:0">${walletDetailHtml(wallet)}</td>`;
 tr.insertAdjacentElement('afterend',detRow);
});
refresh();
})();
</script>
"""


@app.get("/wallet/{wallet}", response_class=HTMLResponse)
def wallet_detail(wallet: str) -> str:
    wallet = wallet.lower()
    state = get_model_state_cached(max_age_sec=10.0)
    row = (state.get("wallets") or {}).get(wallet)
    if not row:
        return HTMLResponse(f"<h3>Wallet not found: {wallet}</h3>", status_code=404)
    if wallet == USER_WALLET:
        updated = html.escape(str(state.get("updated_at", "")))
        return f"""<!doctype html><html><head><meta charset="utf-8"><title>Live Copy Control Centre</title>
<style>
body{{margin:0;background:#0d1117;color:#c9d1d9;font:12px Arial,Helvetica,sans-serif}}
a{{color:#58a6ff;text-decoration:none}}
.top{{display:flex;align-items:center;gap:12px;padding:8px 14px;border-bottom:1px solid #222;background:#090d12;position:sticky;top:0;z-index:4;box-shadow:0 2px 8px rgba(0,0,0,.25)}}
.top .muted{{color:#8b949e}} .top-spacer{{margin-left:auto}}
.section{{padding:10px 10px 12px}}
</style></head><body>
<div class="top"><b>HL Copy Engine</b><a href="/">Main dashboard</a><span class="muted">Updated: {updated}</span><span class="top-spacer muted">Live copy command centre</span></div>
{render_live_copy_control_panel()}
</body></html>"""
    trades = [t for t in state.get("copy_trades", []) if str(t.get("wallet", "")).lower() == wallet][-100:]
    expected_fills = [f for f in state.get("expected_copy_fills", []) if str(f.get("wallet", "")).lower() == wallet][-100:]
    def esc(v: Any) -> str:
        return html.escape(str(v if v is not None else ""))
    def px(v: Any) -> str:
        return f"{fnum(v):,.6f}" if is_present_num(v) else '<span class="missing">—</span>'
    def ms(v: Any) -> str:
        return f"{int(fnum(v))}ms" if is_present_num(v) else '<span class="missing">—</span>'
    trade_rows = "".join(
        "<tr>"
        f"<td>{esc(t.get('trade_id'))}</td><td>{esc(t.get('coin'))}</td><td>{esc(t.get('lead_side'))}</td>"
        f"<td>{esc(t.get('entry_time_iso'))}</td><td>{esc(t.get('exit_time_iso'))}</td>"
        f"<td>{px(t.get('entry_price_lead'))}</td><td>{px(t.get('entry_price_copy'))}</td>"
        f"<td>{px(t.get('entry_price_diff'))}</td>"
        f"<td>{px(t.get('exit_price_lead'))}</td><td>{px(t.get('exit_price_copy'))}</td><td>{px(t.get('exit_price_diff'))}</td>"
        f"<td>{fmt_money_or_dash(t.get('wallet_pnl'))}</td><td>{fmt_money_or_dash(t.get('copy_pnl'))}</td><td>{fmt_money_or_dash(t.get('copy_error'))}</td>"
        f"<td>{fmt_pct_or_dash(t.get('return_pct'), 3)}</td><td>{esc(t.get('duration', ''))}</td>"
        "</tr>"
        for t in trades
    )
    fill_rows = "".join(
        "<tr>"
        f"<td>{esc(f.get('timestamp_iso'))}</td><td>{esc(f.get('coin'))}</td><td>{esc(f.get('side'))}</td><td>{esc(f.get('model_action'))}</td><td>{esc(f.get('recording_method'))}</td>"
        f"<td>{px(f.get('leader_price'))}</td><td>{px(f.get('copy_price'))}</td><td>{px(f.get('price_diff'))}</td>"
        f"<td>{esc(f.get('leader_position_side_after'))}</td><td>{esc(f.get('copy_position_side_after'))}</td>"
        f"<td>{esc(f.get('position_alignment_ok', f.get('side_alignment_ok', '')))}</td>"
        "</tr>"
        for f in expected_fills
    )
    lead = row.get("lead", {}) if isinstance(row.get("lead"), dict) else {}
    copy = row.get("copy", {}) if isinstance(row.get("copy"), dict) else {}
    curve = row.get("curve", []) if isinstance(row.get("curve", []), list) else []
    lc_counts = wallet_lead_copy_counts(row, state)
    summary = "".join([
        f"<div class='metric-line'><span>FILLS L/C</span><b>{lc_counts['fills'][0]} / {lc_counts['fills'][1]}</b></div>",
        f"<div class='metric-line'><span>EXITS L/C</span><b>{lc_counts['exits'][0]} / {lc_counts['exits'][1]}</b></div>",
        f"<div class='metric-line'><span>POS L/C</span><b>{lc_counts['pos'][0]} / {lc_counts['pos'][1]}</b></div>",
        f"<div class='metric-line'><span>LEAD EQ</span><b>{fmt_money_or_dash(lead.get('equity'))}</b></div>",
        f"<div class='metric-line'><span>COPY EQ</span><b>{fmt_money_or_dash(copy.get('equity'))}</b></div>",
        f"<div class='metric-line'><span>LEAD DD</span><b class='neg'>{format_dd(get_current_dd(lead, curve, 'lead'), fnum(row.get('alloc'), DEFAULT_NORM_BASE))}</b></div>",
        f"<div class='metric-line'><span>COPY DD</span><b class='neg'>{format_dd(get_current_dd(copy, curve, 'copy'), fnum(row.get('alloc'), DEFAULT_NORM_BASE))}</b></div>",
        f"<div class='metric-line'><span>LEAD MAXDD</span><b class='neg'>{format_dd(get_max_dd(lead, curve, 'lead'), fnum(row.get('alloc'), DEFAULT_NORM_BASE))}</b></div>",
        f"<div class='metric-line'><span>COPY MAXDD</span><b class='neg'>{format_dd(get_max_dd(copy, curve, 'copy'), fnum(row.get('alloc'), DEFAULT_NORM_BASE))}</b></div>",
        f"<div class='metric-line'><span>ALIGNMENT</span><b>{esc(row.get('position_alignment_ok', True))}</b></div>",
    ])
    page_html = render_home({**state, "wallet_rows": [row], "portfolio_history": row.get("curve", [])})
    live_copy_panel = render_live_copy_control_panel() if wallet == USER_WALLET else ""
    detail_html = f"""
    <div class='section'>
      <h3>{esc(wallet)}</h3>
      <div class='panel'><div class='cards' style='padding:0;grid-template-columns:repeat(5,minmax(130px,1fr))'>{summary}</div></div>
      <div class='table-wrap'><table><tr><th>TRADE</th><th>COIN</th><th>SIDE</th><th>ENTRY TIME</th><th>EXIT TIME</th><th>LEAD ENTRY</th><th>COPY ENTRY</th><th>ENTRY DIFF</th><th>LEAD EXIT</th><th>COPY EXIT</th><th>EXIT DIFF</th><th>LEAD PNL</th><th>COPY PNL</th><th>COPY ERROR</th><th>RET %</th><th>HOLD</th></tr>{trade_rows}</table></div>
      <h3>Expected copy fills</h3>
      <div class='table-wrap'><table><tr><th>TIME</th><th>COIN</th><th>SIDE</th><th>ACTION</th><th>LEADER PX</th><th>COPY PX</th><th>PRICE DIFF</th><th>LEAD POS AFTER</th><th>COPY POS AFTER</th><th>ALIGNMENT</th></tr>{fill_rows}</table></div>
    </div>"""
    if "</body></html>" in page_html:
        return page_html.replace("</body></html>", live_copy_panel + detail_html + "</body></html>")
    return page_html + live_copy_panel + detail_html


@app.get("/api/ui-state")
def api_get_ui_state() -> JSONResponse:
    return JSONResponse(load_ui_state())


@app.post("/api/ui-state", response_model=None)
async def api_set_ui_state(request: Request):
    data: Dict[str, Any]
    ctype = request.headers.get("content-type", "")
    if "application/json" in ctype:
        data = await request.json()
    else:
        form = await request.form()
        data = dict(form)
    patch = {}
    for k in ("norm_base", "user_norm_base", "copy_mode", "normalisation_mode", "fixed_notional", "leader_equity_base", "fee_bps", "copy_friction_bps"):
        if k in data:
            patch[k] = data[k]
    if "ranking_column" in data or "ranking_direction" in data:
        cur = load_ui_state().get("ranking", {})
        patch["ranking"] = {"column": data.get("ranking_column", cur.get("column")), "direction": data.get("ranking_direction", cur.get("direction", "desc"))}
    save_ui_state(patch)
    if wants_json_response(request):
        return JSONResponse({"ok": True, "ui_state": load_ui_state()})
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')


@app.post("/api/norm")
async def api_norm(request: Request) -> JSONResponse:
    data = await request.json()
    save_ui_state({"norm_base": data.get("norm_base", DEFAULT_NORM_BASE)})
    return JSONResponse({"ok": True, "ui_state": load_ui_state()})


@app.post("/api/set-wallet-mode")
async def set_wallet_mode(request: Request) -> JSONResponse:
    data = await request.json()
    wallet = str(data.get("wallet", "")).lower()
    mode = str(data.get("mode", "OFF")).upper()
    if wallet == USER_WALLET:
        mode = "OFF"
    if mode not in {"ON", "OFF", "CLOSE_ONLY"}:
        return JSONResponse({"ok": False, "error": "BAD_MODE"}, status_code=400)
    gate = load_wallet_gate()
    gate[wallet] = {"mode": mode, "off_mode": None}
    save_wallet_gate(gate)
    return JSONResponse({"ok": True, "wallet": wallet, "mode": mode})


@app.post("/api/set-all-modes")
async def set_all_modes(request: Request) -> JSONResponse:
    data = await request.json()
    mode = str(data.get("mode", "OFF")).upper()
    if mode not in {"ON", "OFF", "CLOSE_ONLY"}:
        return JSONResponse({"ok": False, "error": "BAD_MODE"}, status_code=400)
    state = get_model_state_cached(max_age_sec=10.0)
    gate = load_wallet_gate()
    for wallet in (state.get("wallets") or {}).keys():
        gate[wallet] = {"mode": "OFF" if wallet == USER_WALLET else mode, "off_mode": None}
    save_wallet_gate(gate)
    return JSONResponse({"ok": True, "mode": mode, "wallets_updated": len(gate)})


@app.post("/api/reset-app-history")
def reset_app_history() -> JSONResponse:
    for p in (APP_MODEL_STATE_JSON, COPY_TRADES_CSV, EXPECTED_COPY_FILLS_CSV, PORTFOLIO_HISTORY_FILE, EQUITY_HISTORY_FILE):
        try:
            if p.exists(): p.unlink()
        except Exception:
            pass
    return JSONResponse({"ok": True, "note": "derived app files removed; engine truth untouched"})


@app.post("/api/snapshot")
def snapshot() -> JSONResponse:
    state = get_model_state_cached(max_age_sec=0.0, force=True)
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    p = SNAP_DIR / datetime.now().strftime("%Y-%m-%d_%H-%M-%S_app_model_state.json")
    atomic_write_json(p, state)
    return JSONResponse({"ok": True, "path": str(p)})

@app.get("/sort/{column}", response_class=HTMLResponse)
def sort_column(column: str, request: Request, direction: str = ""):
    allowed = {
        "wallet",
        "lead_equity", "lead_real", "lead_realized", "lead_unreal", "lead_unrealized",
        "lead_dd", "lead_drawdown", "lead_maxdd", "lead_max_drawdown",
        "copy_equity", "copy_real", "copy_realized", "copy_unreal", "copy_unrealized",
        "copy_dd", "copy_drawdown", "copy_maxdd", "copy_max_drawdown",
        "delta", "delta_pct",
        "pnl_per_hour", "avg_trade_pct", "win_rate",
        "fill_count", "exit_count", "open_position_count",
        "avg_position_usd", "max_position_usd",
        "avg_entry_notional_usd", "pct_entries_ge10", "required_leverage",
    }
    if column not in allowed:
        if wants_json_response(request):
            return JSONResponse({"ok": False, "error": "BAD_SORT"}, status_code=400)
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')
    cur = load_ui_state().get("ranking", {})
    requested_direction = str(direction or "").lower()
    if requested_direction not in {"asc", "desc"}:
        requested_direction = "asc" if cur.get("column") == column and cur.get("direction") == "desc" else "desc"
    save_ui_state({"ranking": {"column": column, "direction": requested_direction}})
    direction = requested_direction
    if wants_json_response(request):
        return JSONResponse({"ok": True, "column": column, "direction": direction})
    state = get_model_state_cached(max_age_sec=30.0)
    state = dict(state)
    state["ui_state"] = load_ui_state()
    return HTMLResponse(render_home(state))


@app.get("/api/live-config")
def get_live_config():
    return JSONResponse(_live_copy_config_response(_load_live_copy_config()))


@app.get("/api/global-controls")
def get_global_controls():
    cfg = _load_live_copy_config()
    return JSONResponse({"ok": True, "global_controls": _global_controls_for_ui(cfg.get("global_controls", _GLOBAL_CONTROLS_DEFAULTS))})


@app.post("/api/global-controls")
async def set_global_controls(req: Request):
    try:
        body = await req.json()
        config = _load_live_copy_config()
        config["global_controls"] = _normalise_global_controls(body)
        _save_live_copy_config(config)
        return JSONResponse({"ok": True, "global_controls": _global_controls_for_ui(config["global_controls"])})
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


@app.get("/api/live-ws-health")
def get_live_ws_health():
    return JSONResponse({"ok": True, "health": _load_live_ws_health()})


@app.get("/api/live-audit-summary")
def get_live_audit_summary():
    return JSONResponse(_live_audit_summary())


@app.post("/api/live-config/add-wallet")
async def add_live_config_wallet(req: Request):
    try:
        body = await req.json()
        wallet = _normalise_wallet_address(body.get("wallet"))
        config = _load_live_copy_config()
        archived = config["archived_wallets"]
        existing = config["wallets"].get(wallet) or archived.pop(wallet, {})
        config["wallets"][wallet] = _normalise_live_wallet_payload(body, existing)
        repair_live_config_consistency(config)
        _enforce_live_copy_cap(config)
        _save_live_copy_config(config)
        return JSONResponse(_live_copy_config_response(config))
    except ValueError as exc:
        return _live_config_error(str(exc))
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


@app.post("/api/live-config/set-wallet")
async def set_live_config_wallet(req: Request):
    try:
        body = await req.json()
        wallet = _normalise_wallet_address(body.get("wallet"))
        config = _load_live_copy_config()
        wallets = config["wallets"]
        archived = config["archived_wallets"]
        if wallet in wallets:
            wallets[wallet] = _normalise_live_wallet_payload(body, wallets[wallet])
        elif wallet in archived:
            wallets[wallet] = _normalise_live_wallet_payload(body, archived.pop(wallet))
        else:
            raise ValueError("WALLET_NOT_FOUND")
        repair_live_config_consistency(config)
        _enforce_live_copy_cap(config)
        _save_live_copy_config(config)
        return JSONResponse(_live_copy_config_response(config))
    except ValueError as exc:
        return _live_config_error(str(exc))
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


@app.post("/api/live-config/remove-wallet")
async def remove_live_config_wallet(req: Request):
    try:
        body = await req.json()
        wallet = _normalise_wallet_address(body.get("wallet"))
        archive = parse_bool(body.get("archive", True))
        config = _load_live_copy_config()
        wallets = config["wallets"]
        archived = config["archived_wallets"]
        existing = wallets.pop(wallet, archived.get(wallet, {}))
        if not isinstance(existing, dict):
            existing = {}
        existing = _normalise_live_wallet_payload({"mode": "OFF", "enabled": False}, existing)
        if archive:
            existing["archived_at"] = utc_now_iso()
            archived[wallet] = existing
        else:
            wallets[wallet] = existing
        repair_live_config_consistency(config)
        _save_live_copy_config(config)
        return JSONResponse(_live_copy_config_response(config))
    except ValueError as exc:
        return _live_config_error(str(exc))
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


@app.post("/api/live-config/set-mode")
async def set_live_config_mode(req: Request):
    try:
        body = await req.json()
        wallet = _normalise_wallet_address(body.get("wallet"))
        mode = str(body.get("mode", "OFF")).upper()
        if mode not in {"LIVE", "CLO", "OFF"}:
            raise ValueError("BAD_MODE")
        config = _load_live_copy_config()
        wallets = config["wallets"]
        archived = config["archived_wallets"]
        if wallet in wallets:
            existing = wallets[wallet]
        elif wallet in archived:
            existing = archived.pop(wallet)
            wallets[wallet] = existing
        else:
            raise ValueError("WALLET_NOT_FOUND")
        wallets[wallet] = _normalise_live_wallet_payload({"mode": mode, "enabled": mode != "OFF"}, existing)
        repair_live_config_consistency(config)
        _enforce_live_copy_cap(config)
        _save_live_copy_config(config)
        return JSONResponse(_live_copy_config_response(config))
    except ValueError as exc:
        return _live_config_error(str(exc))
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


@app.post("/api/manual-reconciliation/archive-ledger-row")
async def archive_manual_reconciliation_ledger_row(req: Request):
    try:
        body = await req.json()
        result = _archive_manual_reconciliation_ledger_row(body if isinstance(body, dict) else {})
        return JSONResponse(result, status_code=200 if result.get("ok") else 400)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": type(exc).__name__}, status_code=400)


if __name__ == "__main__":
    if uvicorn is None:
        raise SystemExit("Missing uvicorn. Install with: pip install uvicorn fastapi")
    uvicorn.run("HL_Copy_App_SSOT:app", host="127.0.0.1", port=8000, reload=False)
