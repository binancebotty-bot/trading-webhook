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
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
LIVE_COPY_ORDER_INTENTS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "order_intents.csv"
MANUAL_POSITIONS_FILE = LIVE_COPY_AUDIT_DIR / "manual_live_positions.json"
SEND_ATTEMPTS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "send_attempts.csv"
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

app = FastAPI(title="HL Copy Dashboard SSOT")


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
    ranking = merged.get("ranking") if isinstance(merged.get("ranking"), dict) else {}
    ranking_dir = str(ranking.get("direction", "desc")).lower()
    merged["ranking"] = {"column": ranking.get("column"), "direction": ranking_dir if ranking_dir in {"asc", "desc"} else "desc"}
    atomic_write_json(UI_STATE_FILE, merged)
    invalidate_model_cache()
    return merged


def load_wallet_gate() -> Dict[str, Any]:
    g = load_json(WALLET_GATE_FILE, {})
    return g if isinstance(g, dict) else {}


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


def _normalise_live_wallet_payload(payload: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = dict(existing or {})
    mode = str(payload.get("mode", base.get("mode", "OFF"))).upper()
    if mode not in {"LIVE", "CLO", "OFF"}:
        raise ValueError("BAD_MODE")
    copy_mode = str(payload.get("copy_mode", base.get("copy_mode", "proportional"))).lower()
    if copy_mode not in {"proportional", "fixed"}:
        raise ValueError("BAD_COPY_MODE")
    enabled_default = bool(base.get("enabled", True))
    enabled = parse_bool(payload["enabled"]) if "enabled" in payload else enabled_default
    if mode == "OFF":
        enabled = False
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
        if isinstance(cfg, dict) and cfg.get("enabled", True) and str(cfg.get("mode", "")).upper() in {"LIVE", "CLO"}
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
    if isinstance(data, dict):
        return data
    return {"enabled": False, "overall": "OFFLINE", "wallets": {}}


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
                    last_rows.append(row)
                    if len(last_rows) > 20:
                        last_rows.pop(0)
        except Exception:
            pass
    manual_positions = _load_manual_live_positions()
    recent_send_attempts = _load_recent_send_attempts(20)
    send_attempt_counts = _load_send_attempt_counts()
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
        "manual_positions": manual_positions,
        "recent_send_attempts": recent_send_attempts,
        "send_attempt_counts": send_attempt_counts,
    }


def _load_manual_live_positions() -> Dict[str, Any]:
    try:
        if MANUAL_POSITIONS_FILE.exists():
            data = json.loads(MANUAL_POSITIONS_FILE.read_text(encoding="utf-8-sig"))
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def _load_recent_send_attempts(limit: int = 20) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not SEND_ATTEMPTS_CSV.exists():
        return out
    try:
        with SEND_ATTEMPTS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                resp_text = str(row.get("response") or "")
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
                        "notional_cap_reason", "error",
                    ):
                        val = resp.get(key)
                        if val is None:
                            val = payload.get(key)
                        if val is not None:
                            parsed[key] = val
                    # actual_side: prefer executed side from response/payload over CSV intent side
                    actual_side = resp.get("side") or payload.get("side") or row.get("side")
                    if actual_side:
                        parsed["actual_side"] = actual_side
                out.append({**row, **parsed})
        return out[-limit:]
    except Exception:
        return out[-limit:] if len(out) >= limit else out


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
            if abs(fnum(port_lead.get("max_drawdown")) - expected_max_lead_dd) > 0.01:
                errors.append(f"portfolio lead MaxDD {fnum(port_lead.get('max_drawdown')):.4f} != max timestamped summed DD {expected_max_lead_dd:.4f}")
            if abs(fnum(port_copy.get("max_drawdown")) - expected_max_copy_dd) > 0.01:
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
            if abs(hdr_copy_maxdd - usr_copy_maxdd) > 0.01:
                errors.append(f"header/user copy maxDD mismatch: header={hdr_copy_maxdd:.4f} user={usr_copy_maxdd:.4f}")
            hdr_lead_maxdd = fnum(port_lead2.get("max_drawdown"))
            usr_lead_maxdd = get_max_dd(u_lead)
            if abs(hdr_lead_maxdd - usr_lead_maxdd) > 0.01:
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
    if r.get("is_user_wallet"):
        user_base = max(1.0, fnum(ui.get("user_norm_base"), fnum(ui.get("norm_base"), DEFAULT_NORM_BASE)))
        cfg_cell = f'<form action="/api/ui-state" method="post" class="ajax-form user-base-form"><span class="small muted">aggregate base</span><input name="user_norm_base" value="{user_base:g}" size="5" title="aggregate normalisation base"><button title="set aggregate base">Set</button></form>'
    else:
        inc_cell = f"""<form action="/api/wallet-include" method="post" class="inc-form" title="Include/exclude this wallet from combined graph and header cards only"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><input type="checkbox" name="included" value="1" {'checked' if included else ''} onchange="this.form.requestSubmit()"><span class="small">INC</span></form>"""
        cfg_cell = f"""<form action="/api/wallet-config" method="post" class="wallet-cfg"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><select name="copy_mode"><option value="" {'selected' if not r.get('wallet_config') else ''}>global</option><option value="proportional" {'selected' if eff_mode == 'proportional' and r.get('wallet_config') else ''}>prop</option><option value="fixed" {'selected' if eff_mode == 'fixed' and r.get('wallet_config') else ''}>fixed</option></select><input name="norm_base" value="{eff_base:g}" size="5" title="wallet normalisation base"><input name="fixed_notional" value="{eff_fixed:g}" size="4" title="wallet fixed $"><button title="save wallet override">Set</button></form>"""
        purge_cell = f"""<form action="/api/admin/purge-wallet" method="post" class="purge-form" title="ADMIN MAINTENANCE ONLY: permanently purge wallet"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><button class="purge-btn" title="purge wallet">PURGE</button></form>"""
    row_cls = 'user' if r.get('is_user_wallet') else ''
    if not r.get('is_user_wallet') and not included: row_cls += ' excluded-row'
    wallet_sort = html.escape(wallet)
    return f"""
    <tr class="{row_cls}">
      <td class="sticky-wallet" data-sort="{wallet_sort}"><a href="/wallet/{wallet}">{wallet[:8]}…{wallet[-6:]}</a>{badge}</td>
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
      <td class="controls-cell {'inc-off' if (not r.get('is_user_wallet') and not included) else ''}">{inc_cell}{cfg_cell}{purge_cell}</td>
    </tr>"""

HTML_TEMPLATE = """
<!doctype html><html><head><meta charset="utf-8"><title>HL Copy Engine SSOT</title>
<style>
body{{margin:0;background:#0d1117;color:#c9d1d9;font:12px Arial,Helvetica,sans-serif}} a{{color:#58a6ff;text-decoration:none}} .top{{display:flex;align-items:center;gap:12px;padding:8px 14px;border-bottom:1px solid #222;background:#090d12;position:sticky;top:0;z-index:4;box-shadow:0 2px 8px rgba(0,0,0,.25)}} .live{{background:#003d1f;color:#2ea043;border:1px solid #2ea043;border-radius:12px;padding:2px 8px;font-size:10px}} .muted{{color:#8b949e}} input,select,button{{background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:4px;padding:3px 8px}} button{{cursor:pointer}}
.cards{{display:grid;grid-template-columns:repeat(9,minmax(130px,1fr));gap:8px;padding:10px 14px}} .card{{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:9px;min-height:72px}} .group-card .label{{font-size:10px;color:#8b949e;margin-bottom:6px;border-bottom:1px solid #21262d;padding-bottom:4px}} .metric-line{{display:flex;justify-content:space-between;gap:8px;line-height:1.55}} .metric-line span{{color:#8b949e}} .metric-line b{{font-weight:700}} .pos{{color:#2ea043}} .neg{{color:#ff4d4f}} .zero{{color:#c9d1d9}}
.section{{padding:0 14px 10px}} .panel{{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px;margin-bottom:12px}} .chart-wrap{{position:relative;cursor:zoom-in}} .chart-wrap.expanded{{position:relative;z-index:20}} .chart-wrap.expanded .chart{{height:76vh}} .chart{{width:100%;height:260px;background:#151a21}} .chart *{{vector-effect:non-scaling-stroke}} .pnl-line{{fill:none;stroke:#2ea043;stroke-width:1.6;stroke-linejoin:round;stroke-linecap:round}} .realized-line{{fill:none;stroke:#58a6ff;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .dd-line{{fill:none;stroke:#ff4d4f;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .zero-line{{stroke:#8b949e;stroke-width:1}} .grid-line,.grid-vert{{stroke:#21262d;stroke-width:1}} .axis-label{{fill:#8b949e;font-size:10px}} .hit{{fill:transparent;stroke:none;pointer-events:all}} .crosshair{{stroke:#8b949e;stroke-width:1;stroke-dasharray:3 3;pointer-events:none}} .chart-dot{{fill:#c9d1d9;stroke:#0d1117;stroke-width:1.2;pointer-events:none}} .chart-tip{{position:absolute;left:10px;top:10px;background:#0d1117;border:1px solid #30363d;border-radius:4px;padding:5px 7px;color:#c9d1d9;font-size:11px;pointer-events:none;box-shadow:0 4px 12px rgba(0,0,0,.35)}} .chart-legend{{display:flex;gap:10px;align-items:center;margin-top:6px}} .legend-pnl{{color:#2ea043}} .legend-realized{{color:#58a6ff}} .legend-dd{{color:#ff4d4f}}
.table-wrap{{border:1px solid #21262d;border-radius:6px;background:#0d1117}} table{{width:100%;border-collapse:separate;border-spacing:0;font-size:11px}} th{{position:sticky;top:0;background:#21262d;color:#8b949e;text-align:right;padding:7px;border-bottom:1px solid #30363d;z-index:2}} th:first-child,td:first-child{{text-align:left}} th.sort-active{{background:#303a49;color:#fff;box-shadow:inset 0 -2px 0 #58a6ff}} th.sort-active a{{color:#fff}} th a{{display:block;color:#8b949e}} td{{padding:6px;border-bottom:1px solid #21262d;text-align:right;white-space:nowrap}} tr:nth-child(even){{background:#111820}} tr.user{{background:#071527}} tr:hover{{background:#1b2330}} tr.excluded-row{{}}
.sticky-wallet{{position:sticky;left:0;z-index:3;background:inherit;min-width:132px;border-right:1px solid #30363d}} th.sticky-wallet{{z-index:4;background:#21262d}} .pair-lead{{background:rgba(88,166,255,.045)}} .pair-copy{{background:rgba(46,160,67,.045)}} .ops-group{{background:rgba(210,153,34,.05)}} .group-divider{{border-right:2px solid #30363d!important}} .badge{{background:#0d419d;color:#fff;border-radius:3px;padding:1px 4px;font-size:9px}} .mode{{background:#063d1f;color:#2ea043;border-radius:3px;padding:2px 6px}} .wallet-cfg{{display:inline-flex;gap:3px;margin-left:4px;align-items:center}} .wallet-cfg input{{width:48px;padding:1px 3px}} .wallet-cfg select{{width:70px;padding:1px 3px}} .wallet-cfg button{{padding:1px 4px}} .inc-form{{display:inline-flex;align-items:center;gap:2px;margin-right:6px}} .inc-form input{{padding:0;width:14px;height:14px}} .purge-form{{display:inline-flex;margin-left:4px;align-items:center}} .purge-btn{{border-color:#8b1d1d;background:#3a1111;color:#ff7b72;padding:1px 5px;font-size:10px}} .inc-off{{opacity:1}} .controls-cell{{min-width:315px;text-align:left}} .small{{font-size:11px;color:#8b949e}} .missing{{color:#6e7681!important}} .selected-divider td{{background:#0d1117;border-top:2px solid #58a6ff;border-bottom:1px solid #30363d;color:#8b949e;text-align:left;font-size:10px;letter-spacing:.04em;text-transform:uppercase;padding:6px 8px}} .saving{{opacity:.65}} .saved-flash{{color:#2ea043}} .chart-empty{{height:230px;display:flex;align-items:center;justify-content:center;color:#8b949e}} @media(max-width:1300px){{.cards{{grid-template-columns:repeat(3,minmax(150px,1fr))}}}}
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
    <div class="lc-title">Live Copy Control Centre</div>
    <div class="lc-header-pills">
      <span class="lc-pill lc-blue">Mode: DRY RUN</span>
      <span class="lc-pill lc-green">WS Fast Path</span>
      <span class="lc-pill lc-amber">REST Fallback</span>
      <span class="lc-pill">Active LIVE/CLO: <b id="lcWalletCount">0 / 10</b></span>
      <span class="lc-pill">WS: <b id="lcWsOverall">OFFLINE</b></span>
    </div>
    <div class="lc-header-actions">
      <button type="button" id="lcRefresh">Refresh</button>
      <button type="button" data-lc-modal="lcWalletModal">Add wallet</button>
      <button type="button" data-lc-modal="lcGlobalModal" class="lc-soft">Global controls</button>
    </div>
  </header>
  <div class="lc-safety-strip">
    <span class="lc-pill lc-blue">COPY STATE: DRY RUN</span>
    <span class="lc-pill lc-green">NEW ENTRIES: ALLOWED</span>
    <span class="lc-pill lc-green">EXITS: ALLOWED</span>
    <span class="lc-pill lc-red">REAL ORDERS: DISABLED</span>
    <span class="lc-pill lc-green">AUDIT: ON</span>
    <span id="lcStatus" class="lc-status"></span>
  </div>

  <section class="lc-panel lc-graph-panel">
    <div class="lc-graph-top">
      <div>
        <h3>Exchange-backed user equity</h3>
        <p>Dry-run preview values shown. Live mode will use exchange-backed equity with real friction measured, not modelled.</p>
      </div>
      <div class="lc-chart-controls">
        <button type="button" class="active">Equity</button><button type="button">PnL</button><button type="button">Exposure</button>
        <button type="button">Drawdown</button><button type="button">Fees</button><button type="button">Realised</button>
        <input type="date" value="2026-04-30"><input type="time" value="09:00"><input type="time" value="16:30">
        <button type="button" class="active">1D</button><button type="button">7D</button><button type="button">All</button>
      </div>
    </div>
    <div class="lc-stat-strip" id="lcEquityCards"></div>
    <div class="lc-chart-wrap">
      <svg viewBox="0 0 1000 320" preserveAspectRatio="none" aria-label="Exchange-backed user equity graph">
        <polyline points="40,232 140,220 240,205 340,214 440,182 540,166 640,146 740,124 840,132 960,96" fill="none" stroke="#21c16b" stroke-width="4"/>
        <polyline points="40,256 140,248 240,236 340,240 440,226 540,214 640,218 740,202 840,196 960,188" fill="none" stroke="#58a6ff" stroke-width="2" opacity=".72"/>
        <path d="M40 232 L140 220 L240 205 L340 214 L440 182 L540 166 L640 146 L740 124 L840 132 L960 96 L960 300 L40 300 Z" fill="rgba(33,193,107,.12)"/>
        <text x="42" y="34" class="lc-axis">$12.5k</text><text x="42" y="155" class="lc-axis">$12.2k</text><text x="42" y="296" class="lc-axis">$11.9k</text>
        <text x="760" y="52" class="lc-axis">exchange-backed user equity</text><text x="810" y="78" class="lc-axis">dry-run preview values</text>
      </svg>
    </div>
  </section>

  <section class="lc-panel lc-wallet-panel">
    <h3>Live Wallets</h3>
    <p>Command surface mirrors the main dashboard where practical. PnL/equity fields show pending until exchange-backed live data is wired.</p>
    <div class="lc-table-wrap">
      <table class="lc-wallet-table">
        <thead><tr><th>Wallet</th><th>Equity L/C</th><th>Real L/C</th><th>Unreal L/C</th><th>DD L/C</th><th>MaxDD L/C</th><th>Real diff</th><th>Efficiency</th><th>Sizing</th><th>BL</th><th>Stream</th><th title="Include this wallet in the live performance graph only. Does not affect copy execution.">Graph</th><th>Activity L/C</th><th>Controls</th></tr></thead>
        <tbody id="lcWalletRows"><tr><td colspan="14">Loading live-copy config...</td></tr></tbody>
      </table>
    </div>
  </section>

  <section class="lc-tabs">
    <div class="lc-tabbar">
      <button type="button" class="active" data-lc-tab="audit">Health & Audit</button>
      <button type="button" data-lc-tab="recon">Manual Reconciliation</button>
      <button type="button" data-lc-tab="ws">WS Detail</button>
      <button type="button" data-lc-tab="exec">Manual Live Execution</button>
    </div>
    <div class="lc-tab-panel active" data-lc-panel="audit">
      <section class="lc-panel">
        <h3>Execution / Audit</h3>
        <div class="lc-source-strip" id="lcSourceChips"></div>
        <div class="lc-table-wrap">
          <table><thead><tr><th>Time</th><th>Wallet</th><th>Coin</th><th>Side</th><th>Source / Reason</th><th>Status</th><th>Decision</th><th>Decision reason</th><th>Suggested order</th><th>Suggested limit</th><th>Diff %</th><th>Manual?</th><th>Notes</th></tr></thead><tbody id="lcAuditRows"><tr><td colspan="13">Loading audit summary...</td></tr></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel" data-lc-panel="recon">
      <section class="lc-panel">
        <h3>Manual Reconciliation Queue</h3>
        <div class="lc-table-wrap">
          <table><thead><tr><th>Severity</th><th>Wallet</th><th>Coin</th><th>Issue</th><th>Leader</th><th>Copy</th><th>Action</th><th>Buttons</th></tr></thead><tbody id="lcReconRows"></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel" data-lc-panel="ws">
      <section class="lc-panel">
        <h3>WS Health Detail</h3>
        <div class="lc-table-wrap">
          <table><thead><tr><th>Wallet</th><th>Effective</th><th>Grade</th><th>Thread</th><th>Reconnect/min</th><th>Processed</th><th>Raw msg</th><th>Parsed msg</th><th>Snapshot seen</th><th>Snapshot recovered</th><th>Ignored snapshots</th><th>Errors</th><th>Transport stale</th><th>Data status</th><th>Last close/error</th></tr></thead><tbody id="lcHealthRows"></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel" data-lc-panel="exec">
      <section class="lc-panel">
        <h3>Manual Live Execution</h3>
        <p>Read-only. Tracked manual positions and recent send attempts from HL_Live_Copy_Service.</p>
        <div class="lc-source-strip" id="lcExecChips"></div>
        <h4 style="margin:8px 0 4px;font-size:13px">Manual Positions</h4>
        <div class="lc-table-wrap">
          <table style="min-width:680px"><thead><tr><th>Coin</th><th>Signed Size</th><th>Last OID</th><th>Last Intent ID</th><th>Last Updated</th></tr></thead><tbody id="lcManualPosRows"><tr><td colspan="5">Loading…</td></tr></tbody></table>
        </div>
        <h4 style="margin:12px 0 4px;font-size:13px">Recent Send Attempts</h4>
        <div class="lc-table-wrap">
          <table style="min-width:900px"><thead><tr><th>Time</th><th>Coin</th><th>Side</th><th>Status</th><th>Fill</th><th>Pos before→after</th><th>Price src</th><th>Size src</th><th>Error</th></tr></thead><tbody id="lcSendAttemptRows"><tr><td colspan="9">Loading…</td></tr></tbody></table>
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
      <p>Add wallet only introduces it to the live table. Row controls manage sizing, safety limits and graph inclusion.</p>
      <div class="lc-modal-actions"><button type="button" data-lc-close>Cancel</button><button type="submit">Add wallet to live table</button></div>
    </form>
  </div>

  <div class="lc-modal-backdrop" id="lcGlobalModal" aria-hidden="true">
    <section class="lc-modal">
      <div class="lc-modal-head"><h3>Global Controls</h3><button type="button" data-lc-close>close</button></div>
      <div class="lc-form-grid">
        <label>Mode<select><option>DRY RUN</option><option disabled>LIVE gated</option></select></label>
        <label>Global exposure %<input value="22"></label>
        <label>Daily loss limit<input value="50"></label>
        <label>WS fast path<select><option>armed</option><option>off</option></select></label>
        <label>REST fallback<select><option>armed</option><option>off</option></select></label>
        <label>Confirmation guard<select><option>required</option><option>two-person future</option></select></label>
      </div>
      <p>Preview only. No backend effect and no order path is connected.</p>
      <div class="lc-modal-actions"><button type="button" class="lc-soft">Soft kill preview</button><button type="button" class="lc-danger" disabled>Hard kill disabled</button><button type="button" data-lc-close>Close</button></div>
    </section>
  </div>
</div>
<style>
.live-copy-centre{--lc-bg:#070c11;--lc-panel:#0f171f;--lc-line:#223342;--lc-text:#e6edf5;--lc-muted:#8fa3b7;--lc-green:#21c16b;--lc-red:#ff5263;--lc-amber:#f5b84b;--lc-blue:#58a6ff;border-top:1px solid #30363d;margin-top:12px;color:var(--lc-text);font-size:12px}.live-copy-centre *{box-sizing:border-box}.lc-header{display:grid;grid-template-columns:auto 1fr auto;gap:12px;align-items:center;padding:10px 12px;border:1px solid var(--lc-line);background:#0c131a;border-radius:8px;margin-bottom:10px}.lc-title{font-size:20px;font-weight:760}.lc-header-pills,.lc-header-actions,.lc-safety-strip,.lc-source-strip{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.lc-header-pills{justify-content:flex-end}.lc-header-actions{justify-content:flex-end}.lc-pill{display:inline-flex;align-items:center;min-height:24px;padding:0 8px;border:1px solid var(--lc-line);border-radius:999px;background:#131f2b;color:var(--lc-muted);font-weight:720;white-space:nowrap}.lc-blue{color:var(--lc-blue);border-color:rgba(88,166,255,.45)}.lc-green{color:var(--lc-green);border-color:rgba(33,193,107,.45)}.lc-red{color:var(--lc-red);border-color:rgba(255,82,99,.45)}.lc-amber,.lc-mode-CLO{color:var(--lc-amber);border-color:rgba(245,184,75,.45)}.live-copy-centre button{min-height:28px;border:1px solid var(--lc-line);border-radius:6px;background:#172437;color:var(--lc-text);padding:0 8px;font-weight:700}.live-copy-centre button.lc-soft{color:var(--lc-amber);border-color:rgba(245,184,75,.55)}.live-copy-centre button.lc-danger{color:var(--lc-red);border-color:rgba(255,82,99,.6);background:#2a1218}.live-copy-centre button[disabled]{opacity:.5}.lc-safety-strip{margin-bottom:10px}.lc-status{font-weight:720}.lc-ok{color:var(--lc-green)}.lc-bad{color:var(--lc-red)}.lc-panel{border:1px solid var(--lc-line);border-radius:8px;background:var(--lc-panel);padding:12px;margin-bottom:10px;min-width:0}.lc-panel h3{margin:0 0 5px 0;font-size:15px}.lc-panel p,.lc-modal p{margin:0 0 10px 0;color:var(--lc-muted);font-size:12px}.lc-graph-panel{min-height:430px}.lc-graph-top{display:grid;grid-template-columns:1fr auto;gap:12px;align-items:start}.lc-chart-controls{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}.lc-chart-controls button,.lc-chart-controls input{min-height:26px;border:1px solid var(--lc-line);border-radius:6px;background:#0a1118;color:var(--lc-muted);padding:0 7px}.lc-chart-controls .active{color:var(--lc-text);border-color:rgba(88,166,255,.55);background:#142337}.lc-stat-strip{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin:8px 0 10px}.lc-stat{border:1px solid var(--lc-line);background:#0a1118;border-radius:7px;padding:8px;min-height:55px}.lc-stat .label{color:var(--lc-muted);font-size:10px;font-weight:720;text-transform:uppercase}.lc-stat .value{margin-top:6px;font-size:16px;font-weight:780}.lc-chart-wrap{position:relative;min-height:300px;border:1px solid var(--lc-line);border-radius:8px;background:linear-gradient(rgba(255,255,255,.035) 1px,transparent 1px) 0 0/100% 20%,linear-gradient(90deg,rgba(255,255,255,.028) 1px,transparent 1px) 0 0/10% 100%,#091017;overflow:hidden}.lc-chart-wrap svg{display:block;width:100%;height:100%;min-height:300px}.lc-axis{fill:var(--lc-muted);font-size:11px}.lc-table-wrap{overflow-x:auto;border:1px solid var(--lc-line);border-radius:8px}.live-copy-centre table{width:100%;border-collapse:collapse;min-width:1320px}.live-copy-centre th,.live-copy-centre td{border-bottom:1px solid var(--lc-line);padding:6px 7px;text-align:left;vertical-align:middle;white-space:nowrap}.live-copy-centre th{color:var(--lc-muted);font-size:10px;font-weight:780;text-transform:uppercase;background:#0a1118}.lc-wallet{font-family:Consolas,Monaco,monospace;color:#d9ebff}.lc-cell-stack{display:grid;gap:4px}.lc-pair{display:grid;grid-template-columns:34px minmax(52px,auto);gap:5px;align-items:baseline}.lc-pair span:first-child{color:var(--lc-muted);font-size:10px;font-weight:780}.lc-pos{color:var(--lc-green);font-weight:760}.lc-neg{color:var(--lc-red);font-weight:760}.lc-muted{color:var(--lc-muted)}.lc-row-OFF{opacity:.58}.lc-mini-actions,.lc-inline-controls{display:flex;gap:4px;align-items:center;flex-wrap:wrap}.lc-wallet-table input,.lc-wallet-table select{width:76px;min-height:26px;background:#0a1118;color:var(--lc-text);border:1px solid var(--lc-line);border-radius:5px;padding:0 6px}.lc-wallet-table select{width:92px}.lc-graph-toggle{display:inline-flex;gap:5px;align-items:center}.lc-graph-toggle input{width:14px;min-height:14px}.lc-tabs{display:grid;gap:8px}.lc-tabbar{display:flex;gap:6px;border-bottom:1px solid var(--lc-line)}.lc-tabbar button{border-bottom:0;border-radius:7px 7px 0 0;color:var(--lc-muted)}.lc-tabbar button.active{color:var(--lc-text);background:var(--lc-panel)}.lc-tab-panel{display:none}.lc-tab-panel.active{display:block}.lc-source-strip{margin-bottom:10px}.lc-modal-backdrop{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,.62);z-index:2000;padding:18px}.lc-modal-backdrop.active{display:flex}.lc-modal{width:min(660px,100%);border:1px solid var(--lc-line);border-radius:8px;background:var(--lc-panel);padding:14px;box-shadow:0 20px 60px rgba(0,0,0,.45)}.lc-modal-head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:10px}.lc-modal-head h3{margin:0}.lc-form-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.lc-form-grid .wide{grid-column:1/-1}.lc-form-grid label{display:grid;gap:5px;color:var(--lc-muted);font-size:10px;font-weight:760;text-transform:uppercase}.lc-form-grid input,.lc-form-grid select{width:100%;min-height:32px;border:1px solid var(--lc-line);border-radius:6px;color:var(--lc-text);background:#0a1118;padding:0 8px}.lc-modal-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:12px;flex-wrap:wrap}@media(max-width:1300px){.lc-header,.lc-graph-top{grid-template-columns:1fr}.lc-header-pills,.lc-header-actions,.lc-chart-controls{justify-content:flex-start}.lc-stat-strip{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:800px){.lc-stat-strip,.lc-form-grid{grid-template-columns:1fr}}
</style>
<script>
(function(){
const root=document.getElementById('liveCopyPanel'); if(!root) return;
const status=root.querySelector('#lcStatus');
let lcConfig={wallets:{},archived_wallets:{}}, lcHealth={wallets:{}}, lcAudit={last_rows:[]};
function h(v){return String(v==null?'':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function msg(t,bad){if(status){status.textContent=t||'';status.className='lc-status '+(bad?'lc-bad':'lc-ok');}}
async function jget(url){const r=await fetch(url,{headers:{'x-requested-with':'fetch'}});return await r.json();}
async function jpost(url,payload){const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json','x-requested-with':'fetch'},body:JSON.stringify(payload)});const j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.error||r.statusText);return j;}
function num(v,d){const n=parseFloat(v);return Number.isFinite(n)?n:d;}
function first(row,keys){for(const k of keys){if(row&&row[k]!=null&&row[k]!=='')return row[k];}return '';}
function shortWallet(w){return String(w||'').length>18?String(w).slice(0,10)+'...'+String(w).slice(-6):String(w||'');}
function pill(text,kind){const t=String(text||'pending');const token=String(kind||t).split(' ')[0].toUpperCase();const cls=['OPEN','LIVE','OK','GOOD','DRY_RUN_FILLED','ALLOWED','ACTIVE'].includes(token)?'lc-green':['STALE','DEGRADED','CLO','WATCH','QUEUED','RECONNECTING','PENDING'].includes(token)?'lc-amber':['OFF','OFFLINE','CLOSED','MISSING','DISABLED','ERROR','RECONNECT_OVERDUE'].includes(token)?'lc-red':'';
 return `<span class="lc-pill ${cls}">${h(t)}</span>`;}
function decisionPill(text){const t=String(text||'—');const u=t.toUpperCase();const cls=['WOULD_PLACE_IOC_LIMIT','WOULD_LATE_COPY','WOULD_REDUCE_OR_EXIT','WOULD_EXIT','WOULD_REDUCE'].includes(u)?'lc-green':u==='DO_NOT_MARKET_COPY'?'lc-red':u==='MANUAL_REVIEW'?'lc-amber':'';return `<span class="lc-pill ${cls}">${h(t)}</span>`;}
function tiny(v,n){const s=String(v==null?'':v);return h(s.length>n?s.slice(0,n-1)+'…':s);}
function pair(a,b,cls){return `<div class="lc-pair"><span>${h(a)}</span><b class="${cls||''}">${h(b||'—')}</b></div>`;}
function signed(v){const s=String(v||'—');return s.trim().startsWith('-')?'lc-neg':(s.trim().startsWith('+')?'lc-pos':'');}
function count(obj,key){return Number((obj||{})[key]||0);}
function sourceOf(r){return first(r,['source','fill_source'])||String(first(r,['reason'])).replace('LIVE_','').replace('_DETECTED','')||'—';}
function rowPayload(tr){return {wallet:tr.dataset.wallet,mode:tr.querySelector('[name=mode]').value,copy_mode:tr.querySelector('[name=copy_mode]').value,norm_base:num(tr.querySelector('[name=norm_base]').value,100),fixed_notional:num(tr.querySelector('[name=fixed_notional]').value,10),leader_equity_base:num(tr.querySelector('[name=leader_equity_base]').value,10000),max_diff_pct:num(tr.querySelector('[name=max_diff_pct]').value,0.1),daily_loss_limit:num(tr.querySelector('[name=daily_loss_limit]').value,0)};}
function renderEquity(){
 const cards=[['real equity','pending exchange wiring'],['realised PnL','dry-run'],['unrealised PnL','dry-run'],['fees paid','pending'],['open exposure','pending'],['drawdown','pending']];
 root.querySelector('#lcEquityCards').innerHTML=cards.map(([label,value])=>`<div class="lc-stat"><div class="label">${h(label)}</div><div class="value">${h(value)}</div></div>`).join('');
}
function renderWallets(){
 const wallets=lcConfig.wallets||{}, healthWallets=lcHealth.wallets||{};
 const rows=Object.entries(wallets).sort().map(([wallet,cfg])=>{
  const wh=healthWallets[wallet]||{}, mode=String(cfg.mode||'OFF').toUpperCase(), model=cfg.copy_mode==='fixed'?'fixed':'proportional';
  const stream=wh.effective_status||wh.status||'OFFLINE', grade=wh.health_grade||'pending';
  const bl=wh.last_data_ms||wh.last_msg_ms||wh.last_open_ms?'OK':'pending';
  return `<tr class="lc-row-${h(mode)}" data-wallet="${h(wallet)}">
    <td><div class="lc-cell-stack"><span class="lc-wallet">${h(shortWallet(wallet))}</span><span>${pill(mode,mode)} ${h(model)}</span></div></td>
    <td>${pair('L','pending','lc-muted')}${pair('C','pending','lc-muted')}</td>
    <td>${pair('L','pending','lc-muted')}${pair('C','pending','lc-muted')}</td>
    <td>${pair('L','pending','lc-muted')}${pair('C','pending','lc-muted')}</td>
    <td>${pair('L','—')}${pair('C','—')}</td>
    <td>${pair('L','—')}${pair('C','—')}</td>
    <td>pending</td>
    <td>${pair('hr','—')}${pair('avg','—')}${pair('win','—')}</td>
    <td>${pair('norm',cfg.norm_base??100)}${pair('fixed',cfg.fixed_notional??10)}${pair('base',cfg.leader_equity_base??10000)}${pair('diff',cfg.max_diff_pct??0.1)}${pair('loss',cfg.daily_loss_limit??0)}</td>
    <td>${pill(bl,bl)}</td>
    <td><div class="lc-cell-stack">${pill(stream,stream)}<span class="lc-muted">grade ${h(grade)}</span><span class="lc-muted">poll fallback ready</span></div></td>
    <td><label class="lc-graph-toggle" title="Include this wallet in the live performance graph only. Does not affect copy execution."><input type="checkbox" checked>SHOW</label></td>
    <td>${pair('fills',wh.processed_count??0)}${pair('dup',wh.duplicate_count??0)}${pair('recon/min',Number(wh.reconnects_per_min||0).toFixed(2))}</td>
    <td><div class="lc-cell-stack"><div class="lc-inline-controls"><select name="mode"><option ${mode==='LIVE'?'selected':''}>LIVE</option><option ${mode==='CLO'?'selected':''}>CLO</option><option ${mode==='OFF'?'selected':''}>OFF</option></select><select name="copy_mode"><option value="proportional" ${model!=='fixed'?'selected':''}>proportion</option><option value="fixed" ${model==='fixed'?'selected':''}>fixed</option></select></div><div class="lc-inline-controls"><span class="lc-muted">N</span><input name="norm_base" value="${h(cfg.norm_base??100)}"><span class="lc-muted">F</span><input name="fixed_notional" value="${h(cfg.fixed_notional??10)}"><span class="lc-muted">B</span><input name="leader_equity_base" value="${h(cfg.leader_equity_base??10000)}"></div><div class="lc-inline-controls"><span class="lc-muted">diff</span><input name="max_diff_pct" value="${h(cfg.max_diff_pct??0.1)}"><span class="lc-muted">loss</span><input name="daily_loss_limit" value="${h(cfg.daily_loss_limit??0)}"></div><div class="lc-mini-actions"><button data-act="save" type="button">Save</button><button data-act="clo" type="button">CLO</button><button data-act="off" type="button">OFF</button><button data-act="archive" class="lc-danger" type="button" title="Removes wallet from active config only; does not delete audit/history.">Archive config</button></div></div></td>
  </tr>`;
 }).join('');
 root.querySelector('#lcWalletRows').innerHTML=rows||'<tr><td colspan="14">No live-copy wallets configured.</td></tr>';
}
function renderAudit(){
 const rows=(lcAudit.last_rows||[]).slice(-10).reverse();
 const reason=lcAudit.reason_counts||{}, statusCounts=lcAudit.status_counts||{}, sourceCounts=lcAudit.source_counts||{}, decisionCounts=lcAudit.execution_decision_counts||{}, decisionReasonCounts=lcAudit.decision_reason_counts||{}, manualCounts=lcAudit.manual_reconcile_required_counts||{}, errorCounts=lcAudit.market_data_error_counts||{};
 root.querySelector('#lcSourceChips').innerHTML=[
  ['WS fast path',count(decisionCounts,'WOULD_PLACE_IOC_LIMIT'),'lc-green'],
  ['Late copy',count(decisionCounts,'WOULD_LATE_COPY'),'lc-green'],
  ['Do not market copy',count(decisionCounts,'DO_NOT_MARKET_COPY'),'lc-red'],
  ['Manual review',count(decisionCounts,'MANUAL_REVIEW')+count(manualCounts,'True')+count(manualCounts,'true'),'lc-amber'],
  ['Quote unavailable',count(errorCounts,'EXECUTABLE_QUOTE_UNAVAILABLE')+count(decisionReasonCounts,'RECOVERY_QUOTE_UNAVAILABLE'),'lc-amber'],
  ['Snapshot recovery',count(reason,'LIVE_WS_SNAPSHOT_RECOVERY')+count(decisionReasonCounts,'LIVE_WS_SNAPSHOT_RECOVERY'),'lc-blue']
 ].map(([k,v,cls])=>`<span class="lc-pill ${cls}">${h(k)}: <b>${h(v)}</b></span>`).join('');
 root.querySelector('#lcAuditRows').innerHTML=rows.map(r=>`<tr><td>${h(first(r,['created_at','timestamp_iso','time']))}</td><td class="lc-wallet">${h(shortWallet(first(r,['leader_wallet','wallet'])))}</td><td>${h(first(r,['coin','asset']))}</td><td>${h(first(r,['side']))}</td><td><div class="lc-cell-stack"><span>${h(sourceOf(r))}</span><span class="lc-muted">${tiny(first(r,['reason']),32)}</span></div></td><td>${pill(first(r,['status'])||'—')}</td><td>${decisionPill(first(r,['execution_decision'])||'—')}</td><td>${tiny(first(r,['decision_reason']),34)}</td><td>${tiny(first(r,['suggested_order_type']),28)}</td><td>${h(first(r,['suggested_limit_price','target_price']))}</td><td>${h(first(r,['adverse_diff_pct','diff_pct','real_diff_pct','price_diff_pct']))}</td><td>${h(first(r,['manual_reconcile_required']))}</td><td title="${h(first(r,['notes','message']))}">${tiny(first(r,['notes','message']),60)}</td></tr>`).join('')||'<tr><td colspan="13">No audit rows found.</td></tr>';
 const manual=rows.filter(r=>String(first(r,['reason','status'])).toUpperCase().includes('MANUAL')).slice(0,5);
 root.querySelector('#lcReconRows').innerHTML=(manual.length?manual:[{}]).map((r,i)=>{const sev=i===0&&manual.length?'WARN':'INFO';return `<tr><td>${pill(sev,sev)}</td><td class="lc-wallet">${h(shortWallet(first(r,['leader_wallet','wallet'])||'pending'))}</td><td>${h(first(r,['coin'])||'—')}</td><td>${h(first(r,['reason'])||'pending manual review feed')}</td><td>pending</td><td>pending</td><td>${h(first(r,['action'])||'review')}</td><td><div class="lc-mini-actions"><button type="button">review</button><button type="button">ignore</button><button type="button">close</button><button type="button">align</button></div></td></tr>`;}).join('');
}
function renderManualExec(){
 const positions=lcAudit.manual_positions||{};
 const attempts=(lcAudit.recent_send_attempts||[]).slice().reverse();
 const sCounts=lcAudit.send_attempt_counts||{};
 const openPos=Object.values(positions).filter(p=>p&&Math.abs(parseFloat(p.signed_size||0))>1e-12).length;
 root.querySelector('#lcExecChips').innerHTML=[
  ['Filled',count(sCounts,'ORDER_FILLED'),'lc-green'],
  ['Rejected',count(sCounts,'ORDER_REJECTED'),'lc-red'],
  ['Over cap',count(sCounts,'MAX_NOTIONAL_EXCEEDED'),'lc-amber'],
  ['Adverse diff',count(sCounts,'CLOSE_ADVERSE_DIFF_TOO_LARGE'),'lc-amber'],
  ['No position',count(sCounts,'NO_MANUAL_POSITION_TO_CLOSE'),''],
  ['Open positions',openPos,openPos>0?'lc-blue':''],
 ].map(([k,v,cls])=>`<span class="lc-pill ${cls}">${h(k)}: <b>${h(v)}</b></span>`).join('');
 root.querySelector('#lcManualPosRows').innerHTML=Object.entries(positions).sort().map(([coin,p])=>{const sz=parseFloat(p.signed_size||0);const cls=sz>0?'lc-pos':sz<0?'lc-neg':'lc-muted';return `<tr><td>${h(coin)}</td><td class="${cls}">${h(sz)}</td><td class="lc-wallet">${tiny(String(p.last_oid||'—'),32)}</td><td>${tiny(String(p.last_intent_id||'—'),36)}</td><td>${h(p.last_updated_at||'—')}</td></tr>`;}).join('')||'<tr><td colspan="5">No manual positions tracked.</td></tr>';
 root.querySelector('#lcSendAttemptRows').innerHTML=attempts.map(a=>{const st=String(a.status||'—');const stCls=st==='ORDER_FILLED'?'lc-green':['ORDER_REJECTED','MAX_NOTIONAL_EXCEEDED','CLOSE_ADVERSE_DIFF_TOO_LARGE','NO_MANUAL_POSITION_TO_CLOSE'].includes(st)?'lc-red':'lc-amber';const fill=a.fill_avg_px?`${h(a.fill_size||'?')} @ ${h(a.fill_avg_px)}`:'—';const posMov=(a.position_before!=null&&a.position_after!=null)?`${h(a.position_before)}→${h(a.position_after)}`:'—';const err=String(a.error||'');return `<tr><td>${h(first(a,['created_at']))}</td><td>${h(a.coin||'—')}</td><td>${h(a.actual_side||a.side||'—')}</td><td><span class="lc-pill ${stCls}">${h(st)}</span></td><td>${fill}</td><td>${posMov}</td><td>${tiny(String(a.price_source||'—'),22)}</td><td>${tiny(String(a.size_source||'—'),22)}</td><td title="${h(err)}">${tiny(err,40)}</td></tr>`;}).join('')||'<tr><td colspan="9">No send attempts recorded.</td></tr>';
}
function age(ms){const n=Number(ms||0);if(!n)return '—';const d=Math.max(0,Date.now()-n);return d<60000?Math.round(d/1000)+'s':Math.round(d/60000)+'m';}
function time(ms){const n=Number(ms||0);return n?new Date(n).toLocaleTimeString():'—';}
function renderHealth(){
 const rows=Object.entries(lcHealth.wallets||{}).sort().map(([wallet,wh])=>{const effective=String(wh.effective_status||wh.status||'OFFLINE');const alive=!!wh.thread_alive||!!wh.worker_thread_alive;const fatal=Number(wh.fatal_error_count||0);const label=(effective==='STALE'&&alive&&fatal===0)?'STALE / non-fatal; worker alive':effective;const lastErr=wh.last_error_repr||wh.last_error||wh.last_close_msg||'';return `<tr><td class="lc-wallet">${h(shortWallet(wallet))}</td><td>${pill(label,effective)}</td><td>${pill(wh.health_grade||'—',wh.health_grade||'')}</td><td>${h(alive?'alive':'down')}</td><td>${Number(wh.reconnects_per_min||0).toFixed(2)}</td><td>${h(wh.processed_count??0)}</td><td>${h(wh.raw_message_count??0)}</td><td>${h(wh.parsed_fill_message_count??0)}</td><td>${h(wh.snapshot_fill_seen_count??0)}</td><td>${h(wh.snapshot_fill_recovered_count??0)}</td><td>${h(wh.ignored_snapshot_count??0)}</td><td>${h(wh.error_count??0)}</td><td>${h(wh.transport_stale_ms??wh.stale_ms??'')}</td><td>${pill(wh.data_status||'—',wh.data_status||'')}</td><td title="${h(lastErr)}">${tiny(lastErr,70)}</td></tr>`;}).join('');
 root.querySelector('#lcHealthRows').innerHTML=rows||'<tr><td colspan="15">WS service not running or no wallet health file found.</td></tr>';
}
function render(){
 const wallets=lcConfig.wallets||{}, active=Object.values(wallets).filter(w=>w&&w.enabled!==false&&['LIVE','CLO'].includes(String(w.mode||'').toUpperCase())).length;
 root.querySelector('#lcWalletCount').textContent=active+' / 10';
 const wsOverall=String(lcHealth.overall||'OFFLINE').toUpperCase();
 root.querySelector('#lcWsOverall').textContent=(['CLOSED','DEGRADED','DISABLED','OFFLINE'].includes(wsOverall))?'OFFLINE / service not running':wsOverall;
 renderEquity(); renderWallets(); renderAudit(); renderHealth(); renderManualExec();
}
async function refresh(quiet){try{if(!quiet)msg('Loading...');const [cfg,health,audit]=await Promise.all([jget('/api/live-config'),jget('/api/live-ws-health'),jget('/api/live-audit-summary')]);lcConfig=cfg.config||{wallets:{}};lcHealth=health.health||{};lcAudit=audit||{};render();if(!quiet)msg('Loaded');}catch(e){msg(e.message||String(e),true);}}
root.querySelector('#lcRefresh').addEventListener('click',()=>refresh());
root.querySelectorAll('[data-lc-modal]').forEach(btn=>btn.addEventListener('click',()=>{const m=root.querySelector('#'+btn.dataset.lcModal);if(m){m.classList.add('active');m.setAttribute('aria-hidden','false');}}));
root.querySelectorAll('[data-lc-close]').forEach(btn=>btn.addEventListener('click',()=>{const m=btn.closest('.lc-modal-backdrop');if(m){m.classList.remove('active');m.setAttribute('aria-hidden','true');}}));
root.querySelectorAll('[data-lc-tab]').forEach(btn=>btn.addEventListener('click',()=>{root.querySelectorAll('[data-lc-tab]').forEach(b=>b.classList.remove('active'));root.querySelectorAll('[data-lc-panel]').forEach(p=>p.classList.remove('active'));btn.classList.add('active');root.querySelector(`[data-lc-panel="${btn.dataset.lcTab}"]`).classList.add('active');}));
root.querySelector('#lcAddForm').addEventListener('submit',async e=>{e.preventDefault();const fd=new FormData(e.currentTarget);const payload=Object.fromEntries(fd.entries());try{msg('Updating...');await jpost('/api/live-config/add-wallet',payload);e.currentTarget.reset();const m=e.currentTarget.closest('.lc-modal-backdrop');if(m)m.classList.remove('active');await refresh(true);msg('Updated: '+shortWallet(payload.wallet)+' -> SAVED');}catch(err){msg(err.message,true);}});
root.querySelector('#lcWalletRows').addEventListener('click',async e=>{const btn=e.target.closest('button[data-act]');if(!btn)return;const tr=btn.closest('tr');const wallet=tr.dataset.wallet;const short=shortWallet(wallet);try{if(btn.dataset.act==='archive'&&!window.confirm('Archive disables/removes config only. Permanent audit/history is preserved.')){msg('Archive cancelled');return;}msg('Updating...');if(btn.dataset.act==='save'){await jpost('/api/live-config/set-wallet',rowPayload(tr));await refresh(true);msg('Updated: '+short+' -> SAVED');}if(btn.dataset.act==='clo'){await jpost('/api/live-config/set-mode',{wallet,mode:'CLO'});await refresh(true);msg('Updated: '+short+' -> CLO');}if(btn.dataset.act==='off'){await jpost('/api/live-config/set-mode',{wallet,mode:'OFF'});await refresh(true);msg('Updated: '+short+' -> OFF');}if(btn.dataset.act==='archive'){await jpost('/api/live-config/remove-wallet',{wallet,archive:true});await refresh(true);msg('Updated: '+short+' -> ARCHIVED');}}catch(err){msg(err.message,true);}});
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
        _enforce_live_copy_cap(config)
        _save_live_copy_config(config)
        return JSONResponse(_live_copy_config_response(config))
    except ValueError as exc:
        return _live_config_error(str(exc))
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


if __name__ == "__main__":
    if uvicorn is None:
        raise SystemExit("Missing uvicorn. Install with: pip install uvicorn fastapi")
    uvicorn.run("HL_Copy_App_SSOT:app", host="127.0.0.1", port=8000, reload=False)
