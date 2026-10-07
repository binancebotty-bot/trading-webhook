"""
wallet_proof_engine_8014.py

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
    python wallet_proof_engine_8014.py
or:
    uvicorn wallet_proof_engine_8014:app --host 127.0.0.1 --port 8014
"""
from __future__ import annotations

import asyncio
import csv
import itertools
import io
import json
import math
import os
import html
import re
import shutil
import subprocess
import time
import threading
import traceback
import uuid
import urllib.request

# This proof engine is an independent product.  It shares no trade state, no
# cursor, no ownership, no journal and no cached exchange truth with the copy
# runtime or the SSOT tracker.  It shares exactly one thing with them: the
# per-IP weight budget the exchange actually meters.  Everything else stays
# separate by design.
#
# It is an operator analysis surface, not a trading loop.  Its reads are
# therefore paced for a human reading a page, not for an execution deadline,
# and it never claims the execution reserve.
PROOF_ENGINE_WEIGHT_PER_MIN = float(os.getenv("HL_8014_WEIGHT_PER_MIN", "100"))
PROOF_ENGINE_REFRESH_SEC = float(os.getenv("HL_8014_REFRESH_SEC", "300"))

try:
    from hl_rate_guard import guard as _rate_guard
    RATE_GUARD = _rate_guard("proof_engine_8014", PROOF_ENGINE_WEIGHT_PER_MIN)
except Exception:  # pragma: no cover - never let the guard break the UI
    RATE_GUARD = None
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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse

# MTM (mark-to-market) drawdown lookup. The realised-only `max_drawdown` computed
# below from cumsum(closedPnl) of matched exchange fills understates real account
# drawdown by 5-30x in spot-checks. We add MTM stats from HL accountValueHistory
# as a parallel field; the existing realised field is NOT removed (backwards
# compat). Soft-fails to None when offline -- never blocks the live request path.
import sys as _sys
_SCANNER_ROOT = Path(__file__).resolve().parent.parent
if str(_SCANNER_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_SCANNER_ROOT))
try:
    from hl_mtm_lookup import get_mtm_stats as _get_mtm_stats
except Exception as _e:  # pragma: no cover -- defensive
    def _get_mtm_stats(_w, **_kw):
        return {"max_drawdown_mtm": None, "month_pnl_chg_mtm": None,
                "month_acctV_end": None, "mtm_calmar": None,
                "allTime_max_drawdown_mtm": None, "allTime_pnl_chg_mtm": None,
                "allTime_vlm": None, "mtm_source": "import_error",
                "mtm_fetched_at": None}

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
COMPUTED_EQUITY_CURVE_DIR = BASE_DIR / "data" / "equity_curves"
UI_STATE_FILE = BASE_DIR / "ui_state.json"
WALLET_GATE_FILE = BASE_DIR / "wallet_gate.json"
MANUAL_WALLETS_FILE = BASE_DIR / "manual_wallets.txt"
COPY_CANDIDATE_FILES = (
    BASE_DIR / "Copy candidates.TXT",
    BASE_DIR / "Copy candidates.txt",
    BASE_DIR / "copy candidates.txt",
    BASE_DIR / "copycandidates.txt",
    BASE_DIR / "copy_candidates.txt",
)
PURGED_WALLETS_FILE = BASE_DIR / "purged_wallets.txt"
LIVE_COPY_AUDIT_DIR = BASE_DIR / "hl_live_copy_audit"
LIVE_COPY_CONFIG_FILE = LIVE_COPY_AUDIT_DIR / "live_config.json"
LIVE_COPY_WS_HEALTH_FILE = LIVE_COPY_AUDIT_DIR / "live_ws_health.json"
LIVE_COPY_SERVICE_STATE_FILE = LIVE_COPY_AUDIT_DIR / "live_service_state.json"
LIVE_COPY_CORE_STATE_FILE = LIVE_COPY_AUDIT_DIR / "clean_core_runtime_state.json"
LIVE_COPY_INTEGRITY_STATUS_FILE = LIVE_COPY_AUDIT_DIR / "live_integrity_status.json"
OWNERSHIP_TRUTH_GATE_FILE = LIVE_COPY_AUDIT_DIR / "ownership_truth_gate.json"
LIVE_COPY_RECONCILIATION_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "reconciliation.csv"
LIVE_COPY_ORDER_INTENTS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "order_intents.csv"
MANUAL_POSITIONS_FILE = LIVE_COPY_AUDIT_DIR / "manual_live_positions.json"
SEND_ATTEMPTS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "send_attempts.csv"
WOULD_SEND_ORDERS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "would_send_orders.csv"
EXCHANGE_ACCOUNT_SNAPSHOT_FILE = LIVE_COPY_AUDIT_DIR / "exchange_account_snapshot.json"
EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE = LIVE_COPY_AUDIT_DIR / "exchange_account_snapshot_app.json"
EXCHANGE_ACCOUNT_HISTORY_FILE = LIVE_COPY_AUDIT_DIR / "exchange_account_history.json"
ACCOUNT_ORPHAN_POSITIONS_FILE = LIVE_COPY_AUDIT_DIR / "account_orphan_positions.json"
LIVE_FILLS_CSV = LIVE_COPY_AUDIT_DIR / "append_only" / "live_fills.csv"
MANUAL_RECON_BACKUP_DIR = LIVE_COPY_AUDIT_DIR / "reconciliation_backups"
MANUAL_RECON_ACTIONS_FILE = LIVE_COPY_AUDIT_DIR / "manual_reconciliation_actions.json"
LIVE_CONFIG_DIR = BASE_DIR / "hl_live_copy_audit"
LIVE_CONFIG_FILE = LIVE_CONFIG_DIR / "live_config.json"
APP_CACHE_DIR = LIVE_COPY_AUDIT_DIR / "app_cache"
MODEL_DASHBOARD_LAST_GOOD_HTML_FILE = APP_CACHE_DIR / "model_dashboard_last_good.html"
LIVE_AUDIT_SUMMARY_LAST_GOOD_FILE = APP_CACHE_DIR / "live_audit_summary_last_good.json"
FULL_EXCHANGE_AUDIT_FILE = LIVE_COPY_AUDIT_DIR / "full_exchange_audit_latest.json"
SNAP_DIR = DATA_DIR / "snapshots"
LIVE_COPY_PANEL_TEMPLATE_VERSION = "live-copy-realised-pnl-line-v2"

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
TRUE_CURVE_FRESHNESS_TOLERANCE_MS = 6 * 60 * 60 * 1000
WS_CAPTURED = "WS_CAPTURED"
REBUILD = "REBUILD"
_LOCAL_NOOP_STATUSES: frozenset = frozenset({
    "NO_MANUAL_POSITION_TO_CLOSE",
    "ALREADY_FLAT",
    "LEDGER_FLAT",
    "NO_POSITION_TO_CLOSE",
})

app = FastAPI(title="Wallet Proof Engine")
APP_DISPLAY_NAME = "Wallet Proof Engine"
APP_SOURCE_FILE = Path(__file__).name
APP_PORT = 8014

def _startup_warm_audit_cache() -> None:
    """Pre-build live audit summary at startup so first page load hits cache."""
    last_good = _load_live_audit_summary_last_good()
    if last_good:
        with _AUDIT_SUMMARY_CACHE_LOCK:
            _AUDIT_SUMMARY_CACHE["data"] = last_good
            _AUDIT_SUMMARY_CACHE["built_at"] = float(last_good.get("_persisted_at_epoch") or 0.0)


@app.on_event("startup")
async def _on_startup() -> None:
    return



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


def age_ms_label(timestamp_ms: Any) -> Tuple[str, str, str]:
    ts = inum(timestamp_ms, 0)
    if ts <= 0:
        return "—", "muted", "No captured trade timestamp"
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    delta = max(0, now_ms - ts)
    if delta < 60_000:
        return f"{int(delta / 1000)}s", "pos", "Active: last captured trade under 1 minute ago"
    if delta < 3_600_000:
        return f"{int(delta / 60_000)}m", "pos", "Active: last captured trade under 1 hour ago"
    if delta < 86_400_000:
        return f"{round(delta / 3_600_000, 1)}h", "pos", "Active: last captured trade under 1 day ago"
    days = delta / 86_400_000
    if days <= 3:
        return f"{round(days, 1)}d", "pos", "Active: last captured trade within 3 days"
    if days <= 7:
        return f"{round(days, 1)}d", "warn", "Slow: last captured trade 3-7 days ago"
    if days <= 15:
        return f"{round(days, 1)}d", "warn", "Stale: last captured trade 7-15 days ago"
    return f"{round(days, 1)}d", "neg", "Dormant: last captured trade more than 15 days ago"


_FILE_WRITE_LOCK = threading.RLock()
_MODEL_BUILD_LOCK = threading.RLock()
_COHORT_REBUILD_LOCK = threading.Lock()
_MODEL_CACHE: Dict[str, Any] = {"state": None, "built_at": 0.0}
_MODEL_REFRESH_LOCK = threading.Lock()
_MODEL_REFRESH_IN_PROGRESS = False
_MODEL_REFRESH_STATUS: Dict[str, Any] = {
    "in_progress": False,
    "started_at": "",
    "finished_at": "",
    "ok": False,
    "error": "",
    "traceback": "",
    "state_present": False,
    "html_present": False,
    "last_marker": "",
}
_MODEL_DASHBOARD_HTML_CACHE: Dict[str, Any] = {"html": None, "built_at": 0.0, "state_built_at": 0.0, "disk_blocked": False}
_MODEL_DASHBOARD_HTML_CACHE_LOCK = threading.Lock()
_AUDIT_SUMMARY_CACHE: Dict[str, Any] = {"data": None, "built_at": 0.0}
_AUDIT_SUMMARY_CACHE_LOCK = threading.Lock()
_AUDIT_SUMMARY_TTL = 9.0  # seconds — raised from 2s; exchange snapshot 15s TTL sets floor
_AUDIT_SUMMARY_MAX_STALE_SECS = 60.0  # stale fallback cap
_AUDIT_SUMMARY_REBUILD_ACTIVE = threading.Event()  # prevents concurrent rebuilds
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


def load_manual_wallets() -> set[str]:
    if not MANUAL_WALLETS_FILE.exists():
        return set()
    wallets: set[str] = set()
    for line in MANUAL_WALLETS_FILE.read_text(encoding="utf-8").splitlines():
        try:
            wallets.add(normalise_wallet_for_purge(line))
        except ValueError:
            continue
    return wallets


def _candidate_wallet_file() -> Optional[Path]:
    for path in COPY_CANDIDATE_FILES:
        if path.exists():
            return path
    return None


def _wallets_from_text(text: str) -> Tuple[List[str], int]:
    wallets: List[str] = []
    seen: set[str] = set()
    invalid = 0
    for raw in re.split(r"[\s,;]+", text):
        value = raw.strip().strip('"').strip("'")
        if not value:
            continue
        try:
            wallet = normalise_wallet_for_purge(value)
        except ValueError:
            if value.lower().startswith("0x"):
                invalid += 1
            continue
        if wallet not in seen:
            wallets.append(wallet)
            seen.add(wallet)
    return wallets, invalid


def import_copy_candidates_to_manual_wallets() -> Dict[str, Any]:
    source = _candidate_wallet_file()
    if source is None:
        return {
            "ok": False,
            "error": "NO_COPY_CANDIDATES_FILE",
            "searched": [str(p) for p in COPY_CANDIDATE_FILES],
            "imported": 0,
            "already_present": 0,
            "candidate_count": 0,
            "invalid_count": 0,
        }
    text = source.read_text(encoding="utf-8", errors="ignore")
    candidates, invalid_count = _wallets_from_text(text)
    existing_order: List[str] = []
    existing_seen: set[str] = set()
    if MANUAL_WALLETS_FILE.exists():
        for line in MANUAL_WALLETS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                wallet = normalise_wallet_for_purge(line)
            except ValueError:
                continue
            if wallet not in existing_seen:
                existing_order.append(wallet)
                existing_seen.add(wallet)
    added = [w for w in candidates if w not in existing_seen]
    merged = existing_order + added
    MANUAL_WALLETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _unique_tmp_path(MANUAL_WALLETS_FILE)
    with _FILE_WRITE_LOCK:
        tmp.write_text("\n".join(merged) + ("\n" if merged else ""), encoding="utf-8")
        _replace_with_retries(tmp, MANUAL_WALLETS_FILE)
    if added:
        invalidate_model_cache()
    else:
        with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
            _MODEL_DASHBOARD_HTML_CACHE["html"] = None
            _MODEL_DASHBOARD_HTML_CACHE["built_at"] = 0.0
            _MODEL_DASHBOARD_HTML_CACHE["state_built_at"] = 0.0
            _MODEL_DASHBOARD_HTML_CACHE["disk_blocked"] = True
    return {
        "ok": True,
        "source": str(source),
        "manual_wallets_file": str(MANUAL_WALLETS_FILE),
        "candidate_count": len(candidates),
        "imported": len(added),
        "already_present": len(candidates) - len(added),
        "total_manual_wallets": len(merged),
        "invalid_count": invalid_count,
        "imported_wallets": added,
    }


def state_missing_manual_wallets(state: Any) -> bool:
    manual = load_manual_wallets()
    if not manual:
        return False
    if not isinstance(state, dict):
        return True
    rows = state.get("wallet_rows") or []
    present = {
        str(row.get("wallet", "")).strip().lower()
        for row in rows
        if isinstance(row, dict)
    }
    return bool(manual - present)


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
        for key in ("wallet_config", "wallet_include", "wallet_model_exclude"):
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

    with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
        _MODEL_DASHBOARD_HTML_CACHE["html"] = None
        _MODEL_DASHBOARD_HTML_CACHE["built_at"] = 0.0
        _MODEL_DASHBOARD_HTML_CACHE["state_built_at"] = 0.0
        _MODEL_DASHBOARD_HTML_CACHE["disk_blocked"] = True
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


_PORTFOLIO_PERIODS_CACHE: Dict[Tuple[str, int], Dict[str, Any]] = {}
_ACCOUNT_VALUE_CURVE_CACHE: Dict[Tuple[str, str, int], List[Dict[str, Any]]] = {}
_COMPUTED_EQUITY_CURVE_CACHE: Dict[Tuple[str, int, int, int], List[Dict[str, Any]]] = {}
_RAW_FILLS_CACHE_LOCK = threading.Lock()
_RAW_FILLS_CACHE: Dict[str, Any] = {"key": None, "fills": None}
_EFFECTIVE_WALLET_UI_CACHE_KEY = "_runtime_effective_wallet_ui_cache"
_SIZING_EQUITY_TIMELINE_CACHE_KEY = "_runtime_sizing_equity_timeline_cache"
_SIZING_DENOMINATOR_CACHE_KEY = "_runtime_sizing_denominator_cache"


def _portfolio_periods_from_cache(wallet: str) -> Dict[str, Any]:
    path = BASE_DIR / "data" / "wallet_portfolios" / f"{str(wallet).lower()}.json"
    if not path.exists():
        return {}
    try:
        mtime_ns = path.stat().st_mtime_ns
    except Exception:
        mtime_ns = 0
    cache_key = (str(wallet).lower(), mtime_ns)
    cached = _PORTFOLIO_PERIODS_CACHE.get(cache_key)
    if isinstance(cached, dict):
        return cached
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(raw, list):
        periods = {str(k): v for k, v in raw if isinstance(v, dict)}
    elif isinstance(raw, dict):
        periods = raw
    else:
        periods = {}
    for key in list(_PORTFOLIO_PERIODS_CACHE.keys()):
        if key[0] == cache_key[0] and key != cache_key:
            _PORTFOLIO_PERIODS_CACHE.pop(key, None)
    _PORTFOLIO_PERIODS_CACHE[cache_key] = periods
    return periods


def _account_value_curve(wallet: str, period: str = "allTime") -> List[Dict[str, Any]]:
    path = BASE_DIR / "data" / "wallet_portfolios" / f"{str(wallet).lower()}.json"
    try:
        mtime_ns = path.stat().st_mtime_ns if path.exists() else 0
    except Exception:
        mtime_ns = 0
    cache_key = (str(wallet).lower(), str(period), mtime_ns)
    cached = _ACCOUNT_VALUE_CURVE_CACHE.get(cache_key)
    if isinstance(cached, list):
        return cached
    periods = _portfolio_periods_from_cache(wallet)
    block = periods.get(period) or periods.get("month") or {}
    raw = block.get("accountValueHistory") if isinstance(block, dict) else []
    points: List[Dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, list) or len(item) != 2:
            continue
        try:
            ts = int(float(item[0]))
            equity = float(item[1])
        except Exception:
            continue
        if equity <= 0:
            continue
        points.append({
            "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
            "timestamp_ms": ts,
            "equity_usd": equity,
        })
    points.sort(key=lambda p: p["timestamp_ms"])
    peak = None
    for p in points:
        equity = fnum(p.get("equity_usd"))
        peak = equity if peak is None else max(peak, equity)
        dd = equity - peak
        p["running_peak_usd"] = round(peak, 8)
        p["drawdown_usd"] = round(dd, 8)
        p["drawdown_pct"] = round((dd / peak * 100.0) if peak else 0.0, 8)
    for key in list(_ACCOUNT_VALUE_CURVE_CACHE.keys()):
        if key[0] == cache_key[0] and key[1] == cache_key[1] and key != cache_key:
            _ACCOUNT_VALUE_CURVE_CACHE.pop(key, None)
    _ACCOUNT_VALUE_CURVE_CACHE[cache_key] = points
    return points


def _current_leader_equity_from_cache(wallet: str) -> Optional[float]:
    curve = _account_value_curve(wallet)
    if not curve:
        return None
    return round(fnum(curve[-1].get("equity_usd")), 4)


def _leader_equity_source_snapshot(wallet: str) -> Dict[str, Any]:
    """Describe the exact cached account-value point used for unlocked sizing."""
    curve = _account_value_curve(wallet)
    if not curve:
        return {
            "value": None,
            "timestamp_ms": 0,
            "age_seconds": None,
            "stale": True,
            "source": "missing:data/wallet_portfolios/accountValueHistory",
        }
    point = curve[-1]
    timestamp_ms = int(point.get("timestamp_ms") or 0)
    age_seconds = max(0.0, (time.time() * 1000.0 - timestamp_ms) / 1000.0) if timestamp_ms else None
    return {
        "value": round(fnum(point.get("equity_usd")), 4),
        "timestamp_ms": timestamp_ms,
        "age_seconds": age_seconds,
        "stale": age_seconds is None or age_seconds > 24 * 3600,
        "source": "Hyperliquid portfolio allTime.accountValueHistory",
    }


def _leader_equity_source_mtimes(wallets: Iterable[str]) -> Dict[str, int]:
    """Snapshot the polled accountValueHistory files used by sizing."""
    mtimes: Dict[str, int] = {}
    for wallet in sorted({str(w).strip().lower() for w in wallets if str(w).strip()}):
        if wallet == USER_WALLET:
            continue
        path = BASE_DIR / "data" / "wallet_portfolios" / f"{wallet}.json"
        try:
            mtimes[wallet] = path.stat().st_mtime_ns if path.exists() else 0
        except Exception:
            mtimes[wallet] = 0
    return mtimes


def _leader_equity_sources_changed(state: Any) -> bool:
    """True when a poll updated sizing equity after this model was built."""
    if not isinstance(state, dict):
        return False
    saved = state.get("leader_equity_source_mtimes")
    if not isinstance(saved, dict) or not saved:
        return False
    def exact_mtime(value: Any) -> int:
        # st_mtime_ns is currently ~1e18. Passing it through float (as in
        # inum()) discards low bits and makes unchanged files look modified.
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        text = str(value or "").strip()
        return int(text) if re.fullmatch(r"-?\d+", text) else 0

    return _leader_equity_source_mtimes(saved.keys()) != {str(k): exact_mtime(v) for k, v in saved.items()}


def _true_ts_drawdown_summary(wallet: str) -> Dict[str, Any]:
    curve = _account_value_curve(wallet)
    if not curve:
        return {
            "true_ts_max_dd_usd": None,
            "true_ts_max_dd_pct": None,
            "true_ts_points": 0,
            "true_ts_source": "missing:data/wallet_portfolios/accountValueHistory",
        }
    trough = min(curve, key=lambda p: fnum(p.get("drawdown_usd")))
    return {
        "true_ts_max_dd_usd": round(fnum(trough.get("drawdown_usd")), 8),
        "true_ts_max_dd_pct": round(fnum(trough.get("drawdown_pct")), 8),
        "true_ts_points": len(curve),
        "true_ts_source": "8012:data/wallet_portfolios/accountValueHistory",
    }


def _parse_ts_ms(value: Any) -> int:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(float(value))
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except Exception:
        pass
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def _row_proof_window(row_or_curve: Any) -> Tuple[int, int]:
    curve = row_or_curve.get("curve", []) if isinstance(row_or_curve, dict) else row_or_curve
    if not isinstance(curve, list) or not curve:
        return 0, 0
    ts_vals = [_parse_ts_ms((p or {}).get("timestamp_ms") or (p or {}).get("ts")) for p in curve if isinstance(p, dict)]
    ts_vals = [t for t in ts_vals if t > 0]
    if not ts_vals:
        return 0, 0
    return min(ts_vals), max(ts_vals)


def _true_dd_status_label(status: Any) -> str:
    status_s = str(status or "")
    if status_s == "stale_true_curve":
        return "STALE TRUE CURVE"
    if status_s == "missing_window_true_curve":
        return "MISSING WINDOW TRUE CURVE"
    if status_s.startswith("missing"):
        return "MISSING TRUE CURVE"
    return "TRUE CURVE UNAVAILABLE"


def _drawdown_from_curve_points(points: List[Dict[str, Any]], scale: float) -> Tuple[float, float]:
    peak = None
    min_dd = 0.0
    now_dd = 0.0
    for point in points:
        equity = fnum(point.get("equity")) * scale
        peak = equity if peak is None else max(peak, equity)
        now_dd = equity - peak
        min_dd = min(min_dd, now_dd)
    return now_dd, min_dd


def _windowed_true_curve_points(curve: List[Dict[str, Any]], window_start: int, window_end: int) -> Tuple[List[Dict[str, Any]], int, bool, int]:
    """Return proof-window true equity points, carrying sparse curves to the window end.

    The reconstructed source is trade/event sparse. Carry-forward is valid for
    closed-PnL true equity because no later true event means the reconstructed
    equity has not changed; scalar/model/MTM fallback remains forbidden.
    """
    if window_start <= 0 or window_end <= 0 or window_end < window_start:
        return [], 0, False, 0
    ordered = [p for p in sorted(curve, key=lambda x: inum(x.get("timestamp_ms"))) if inum(p.get("timestamp_ms")) > 0]
    source_window = [p for p in ordered if window_start <= inum(p.get("timestamp_ms")) <= window_end]
    seed = next((p for p in reversed(ordered) if inum(p.get("timestamp_ms")) <= window_start), None)
    if not source_window and seed is None:
        return [], 0, False, 0
    source_latest_ms = max((inum(p.get("timestamp_ms")) for p in source_window), default=inum(seed.get("timestamp_ms")) if seed else 0)
    out: List[Dict[str, Any]] = []
    if seed is not None and inum(seed.get("timestamp_ms")) < window_start:
        out.append({
            "timestamp": datetime.fromtimestamp(window_start / 1000, tz=timezone.utc).isoformat(),
            "timestamp_ms": window_start,
            "equity": fnum(seed.get("equity")),
            "source": "carry_forward_window_start",
        })
    out.extend(dict(p) for p in source_window)
    latest = out[-1] if out else None
    forward_filled = False
    if latest is not None and inum(latest.get("timestamp_ms")) < window_end:
        out.append({
            "timestamp": datetime.fromtimestamp(window_end / 1000, tz=timezone.utc).isoformat(),
            "timestamp_ms": window_end,
            "equity": fnum(latest.get("equity")),
            "source": "carry_forward_window_end",
        })
        forward_filled = True
    dedup: Dict[int, Dict[str, Any]] = {}
    for point in out:
        dedup[inum(point.get("timestamp_ms"))] = point
    return [dedup[ts] for ts in sorted(dedup)], len(source_window), forward_filled, source_latest_ms


def _normalised_true_drawdown_points(points: List[Dict[str, Any]], scale: float) -> List[Dict[str, Any]]:
    """Apply wallet normalisation and compute peak-to-current true drawdown."""
    out: List[Dict[str, Any]] = []
    peak: Optional[float] = None
    for point in sorted(points, key=lambda p: inum(p.get("timestamp_ms"))):
        ts_ms = inum(point.get("timestamp_ms"))
        if ts_ms <= 0:
            continue
        equity = fnum(point.get("equity")) * scale
        peak = equity if peak is None else max(peak, equity)
        dd = equity - peak
        out.append({
            "timestamp": point.get("timestamp") or datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
            "timestamp_ms": ts_ms,
            "equity": round(equity, 8),
            "running_peak": round(peak, 8),
            "drawdown": round(dd, 8),
        })
    return out


def _computed_true_drawdown_summary(wallet: str, alloc: float, leader_equity_base: float, proof_window_start_ms: int = 0, proof_window_end_ms: int = 0) -> Dict[str, Any]:
    window_start = inum(proof_window_start_ms)
    window_end = inum(proof_window_end_ms)
    curve = _computed_equity_curve(wallet, window_start, window_end)
    scale = fnum(alloc, DEFAULT_NORM_BASE) / max(1.0, fnum(leader_equity_base, fnum(alloc, DEFAULT_NORM_BASE)))
    base = max(1.0, fnum(alloc, DEFAULT_NORM_BASE))
    if not curve:
        return {
            "true_ts_dd_now_usd": None,
            "true_ts_dd_now_pct": None,
            "true_ts_max_dd_usd": None,
            "true_ts_max_dd_pct": None,
            "all_time_true_max_dd_usd": None,
            "all_time_true_max_dd_pct": None,
            "true_ts_points": 0,
            "true_ts_window_points": 0,
            "true_ts_source": "missing:data/equity_curves",
            "true_ts_status": "missing_true_curve",
            "true_dd_promotion_ready": False,
            "promotion_status": "NOT PROMOTION READY",
            "true_ts_window_start_ms": proof_window_start_ms or None,
            "true_ts_window_end_ms": proof_window_end_ms or None,
            "true_ts_latest_ms": None,
        }
    # Every 8014 TRUE-DD display value is scoped to the proof window.  The
    # source CSV may contain history from before a wallet was imported; none of
    # that history may leak into a newly baselined row.
    latest_ms = max((inum(p.get("timestamp_ms")) for p in curve), default=0)
    window_curve: List[Dict[str, Any]] = []
    source_window_points = 0
    forward_filled = False
    source_latest_window_ms = 0
    if window_start <= 0 or window_end <= 0 or window_end < window_start:
        status = "missing_window_true_curve"
    else:
        window_curve, source_window_points, forward_filled, source_latest_window_ms = _windowed_true_curve_points(
            curve,
            window_start,
            window_end,
        )
        # A wallet that has been baselined but has no subsequent true event is
        # a flat zero-DD proof curve, not missing data. The window helper seeds
        # it at the import boundary and carries that factual equity forward.
        status = "ok" if window_curve else "missing_window_true_curve"
    freshness_gap_ms = max(0, window_end - source_latest_window_ms) if window_end and source_latest_window_ms else 0
    if status == "ok":
        now_dd, min_dd = _drawdown_from_curve_points(window_curve, scale)
        now_val: Optional[float] = round(now_dd, 8)
        now_pct: Optional[float] = round((now_dd / base * 100.0) if base else 0.0, 8)
        max_val: Optional[float] = round(min_dd, 8)
        max_pct: Optional[float] = round((min_dd / base * 100.0) if base else 0.0, 8)
    else:
        now_val = now_pct = max_val = max_pct = None
    return {
        "true_ts_dd_now_usd": now_val,
        "true_ts_dd_now_pct": now_pct,
        "true_ts_max_dd_usd": max_val,
        "true_ts_max_dd_pct": max_pct,
        "all_time_true_max_dd_usd": max_val,
        "all_time_true_max_dd_pct": max_pct,
        "true_ts_points": len(curve),
        "true_ts_window_points": len(window_curve),
        "true_ts_source_window_points": source_window_points,
        "true_ts_forward_filled": forward_filled,
        "true_ts_source_latest_window_ms": source_latest_window_ms or None,
        "true_ts_freshness_gap_ms": freshness_gap_ms,
        "true_ts_source": "data/equity_curves:proof_window",
        "true_ts_status": status,
        "true_dd_promotion_ready": status == "ok",
        "promotion_status": "PROMOTION READY" if status == "ok" else "NOT PROMOTION READY",
        "true_ts_window_start_ms": window_start or None,
        "true_ts_window_end_ms": window_end or None,
        "true_ts_latest_ms": latest_ms or None,
        "true_ts_freshness_tolerance_ms": TRUE_CURVE_FRESHNESS_TOLERANCE_MS,
    }


def _computed_equity_curve(wallet: str, window_start_ms: int = 0, window_end_ms: int = 0) -> List[Dict[str, Any]]:
    """Load a wallet's proving curve, optionally limited to an 8014 proof window.

    A bounded load retains only the last point at/before the window plus points
    inside it.  Imported wallets can have very large historical CSVs, but their
    pre-baseline rows are neither displayed nor needed for proof-window DD.
    """
    wallet_key = str(wallet).strip().lower()
    path = COMPUTED_EQUITY_CURVE_DIR / f"{wallet_key}.csv"
    try:
        mtime_ns = path.stat().st_mtime_ns if path.exists() else 0
    except Exception:
        mtime_ns = 0
    window_start = inum(window_start_ms)
    window_end = inum(window_end_ms)
    bounded = window_start > 0 and window_end >= window_start
    cache_key = (wallet_key, mtime_ns, window_start if bounded else 0, window_end if bounded else 0)
    cached = _COMPUTED_EQUITY_CURVE_CACHE.get(cache_key)
    if isinstance(cached, list):
        return cached
    if not path.exists():
        _COMPUTED_EQUITY_CURVE_CACHE[cache_key] = []
        return []
    points: List[Dict[str, Any]] = []
    seed: Optional[Dict[str, Any]] = None
    try:
        if bounded:
            # Proving curves are emitted in timestamp order.  Walk backwards so
            # a newly imported wallet stops at its baseline predecessor instead
            # of parsing years of irrelevant history.  Reversing also preserves
            # the last value for duplicate timestamps, matching full-load dedup.
            for raw_line in reversed(path.read_bytes().splitlines()):
                ts_raw, sep, equity_raw = raw_line.partition(b",")
                if not sep:
                    continue
                try:
                    ts_ms = int(float(ts_raw))
                except Exception:
                    continue
                if ts_ms > window_end:
                    continue
                try:
                    equity = float(equity_raw.strip())
                except Exception:
                    continue
                if not math.isfinite(equity) or ts_ms <= 0:
                    continue
                point = {"timestamp_ms": ts_ms, "equity": equity}
                if ts_ms <= window_start:
                    seed = point
                    break
                points.append(point)
            points.reverse()
        else:
            fh = path.open("r", encoding="utf-8-sig", newline="")
            # These proving files have the fixed numeric schema `ts,equity`.
            # Parsing the two values directly avoids building nearly a million
            # temporary DictReader rows on a 125-wallet rebuild.
            try:
                next(fh, None)
                for line in fh:
                    ts_raw, sep, equity_raw = line.partition(",")
                    if not sep:
                        continue
                    try:
                        ts_ms = int(float(ts_raw))
                        equity = float(equity_raw.strip())
                    except Exception:
                        continue
                    if not math.isfinite(equity) or ts_ms <= 0:
                        continue
                    point = {"timestamp_ms": ts_ms, "equity": equity}
                    points.append(point)
            finally:
                fh.close()
    except Exception:
        points = []
        seed = None
    if bounded and seed is not None:
        points.append(seed)
    dedup: Dict[int, Dict[str, Any]] = {}
    for point in points:
        dedup[inum(point.get("timestamp_ms"))] = point
    points = [dedup[ts] for ts in sorted(dedup)]
    for point in points:
        ts_ms = inum(point.get("timestamp_ms"))
        point["timestamp"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    for key in list(_COMPUTED_EQUITY_CURVE_CACHE.keys()):
        if key[0] == wallet_key and key != cache_key:
            _COMPUTED_EQUITY_CURVE_CACHE.pop(key, None)
    _COMPUTED_EQUITY_CURVE_CACHE[cache_key] = points
    return points


def build_computed_true_drawdown_history(rows: List[Dict[str, Any]], ui: Optional[Dict[str, Any]] = None, window_start_ms: int = 0, window_end_ms: int = 0) -> List[Dict[str, Any]]:
    """Build portfolio TRUE DD from each wallet's own active 8014 proof window.

    The optional portfolio bounds remain for call compatibility, but they must
    never widen a wallet's proof window or admit pre-baseline history.
    """
    ui = ui or {}
    selected_rows = [
        r for r in rows
        if not r.get("is_user_wallet") and bool(r.get("include_in_portfolio", wallet_included(str(r.get("wallet", "")), ui)))
    ]
    events: List[Tuple[int, str, float]] = []
    for row in selected_rows:
        wallet = str(row.get("wallet", "")).strip().lower()
        if not wallet:
            continue
        alloc = fnum(row.get("alloc"), wallet_alloc(wallet, ui))
        leader_base = fnum(row.get("effective_leader_equity_base"), alloc)
        scale = alloc / max(1.0, leader_base)
        window_start, window_end = _row_proof_window(row)
        summary = _computed_true_drawdown_summary(wallet, alloc, leader_base, window_start, window_end)
        row.update(summary)
        if summary.get("true_ts_status") != "ok":
            continue
        window_curve, _source_points, _forward_filled, _source_latest_ms = _windowed_true_curve_points(
            _computed_equity_curve(wallet, window_start, window_end),
            window_start,
            window_end,
        )
        for point in window_curve:
            ts_ms = inum(point.get("timestamp_ms"))
            equity = fnum(point.get("equity")) * scale
            events.append((ts_ms, wallet, equity))
    events.sort(key=lambda item: item[0])
    latest_equity: Dict[str, float] = {}
    out: List[Dict[str, Any]] = []
    aggregate_peak: Optional[float] = None
    for ts_ms, grouped in itertools.groupby(events, key=lambda item: item[0]):
        if ts_ms <= 0:
            continue
        for _event_ts, wallet, equity in grouped:
            latest_equity[wallet] = equity
        aggregate_equity = sum(latest_equity.values())
        aggregate_peak = aggregate_equity if aggregate_peak is None else max(aggregate_peak, aggregate_equity)
        aggregate_drawdown = aggregate_equity - aggregate_peak
        out.append({
            "ts": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
            "timestamp_ms": ts_ms,
            "equity": round(aggregate_equity, 8),
            "drawdown": round(aggregate_drawdown, 8),
            "wallet_count": len(latest_equity),
            "source": "data/equity_curves:proof_window",
        })
    return compact_history(out)


def _portfolio_history_window_ms(history: List[Dict[str, Any]]) -> Tuple[int, int]:
    ts_vals: List[int] = []
    for point in history if isinstance(history, list) else []:
        if not isinstance(point, dict):
            continue
        ts_ms = _parse_ts_ms(point.get("timestamp_ms") or point.get("ts"))
        if ts_ms > 0:
            ts_vals.append(ts_ms)
    if not ts_vals:
        return 0, 0
    return min(ts_vals), max(ts_vals)


def apply_user_true_drawdown_rollup(
    rows: List[Dict[str, Any]],
    portfolio_wallets: List[Dict[str, Any]],
    true_drawdown_history: List[Dict[str, Any]],
    user_base: float,
) -> None:
    """Populate USER-only display rollups from selected leader wallets."""
    user_row = next((r for r in rows if r.get("is_user_wallet")), None)
    if user_row is None:
        return

    latest_trade_ts = max((inum(r.get("last_trade_timestamp_ms")) for r in portfolio_wallets), default=0)
    if true_drawdown_history:
        final_dd = fnum(true_drawdown_history[-1].get("drawdown"))
        min_dd = min((fnum(point.get("drawdown")) for point in true_drawdown_history), default=0.0)
        user_row["true_ts_dd_now_usd"] = round(final_dd, 8)
        user_row["true_ts_dd_now_pct"] = round((final_dd / user_base * 100.0) if user_base else 0.0, 8)
        user_row["true_ts_max_dd_usd"] = round(min_dd, 8)
        user_row["true_ts_max_dd_pct"] = round((min_dd / user_base * 100.0) if user_base else 0.0, 8)
        user_row["true_ts_status"] = "ok"
        user_row["true_dd_promotion_ready"] = True
        user_row["promotion_status"] = "PROMOTION READY"
    else:
        user_row["true_ts_dd_now_usd"] = None
        user_row["true_ts_dd_now_pct"] = None
        user_row["true_ts_max_dd_usd"] = None
        user_row["true_ts_max_dd_pct"] = None
        user_row["true_ts_status"] = "missing_window_true_curve"
        user_row["true_dd_promotion_ready"] = False
        user_row["promotion_status"] = "NOT PROMOTION READY"
    user_row["true_ts_points"] = len(true_drawdown_history or [])
    user_row["true_ts_window_points"] = len(true_drawdown_history or [])
    user_row["true_ts_source"] = "sum:data/equity_curves:proof_window"
    user_row["true_ts_latest_ms"] = (
        inum(true_drawdown_history[-1].get("timestamp_ms"))
        if true_drawdown_history else None
    )
    user_row["last_trade_timestamp_ms"] = latest_trade_ts or None

    # The aggregate's tracked max uses the same proof-window portfolio history.
    # Re-reading full per-wallet curves here used to re-admit pre-import data and
    # also multiplied dashboard build time.
    user_row["all_time_true_max_dd_usd"] = user_row.get("true_ts_max_dd_usd")
    user_row["all_time_true_max_dd_pct"] = user_row.get("true_ts_max_dd_pct")


def true_curve_status_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {"valid": 0, "stale": 0, "missing": 0, "total": 0}
    for row in rows:
        if row.get("is_user_wallet"):
            continue
        counts["total"] += 1
        status = str(row.get("true_ts_status") or "")
        if status == "ok":
            counts["valid"] += 1
        elif status == "stale_true_curve":
            counts["stale"] += 1
        else:
            counts["missing"] += 1
    return counts


def _clean_wallet_config(raw_cfg: Any) -> Dict[str, Dict[str, Any]]:
    """Sanitise optional per-wallet model overrides.

    Empty/missing wallet config means: inherit the global header settings.
    Supported per-wallet keys: copy_mode, norm_base, fixed_notional,
    leader_equity_base, leader_equity_base_locked.
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
        if "leader_equity_base" in cfg and str(cfg.get("leader_equity_base", "")).strip() != "":
            item["leader_equity_base"] = max(1.0, fnum(cfg.get("leader_equity_base"), item.get("norm_base", DEFAULT_NORM_BASE)))
        if parse_bool(cfg.get("leader_equity_base_locked", False)) and "leader_equity_base" in item:
            item["leader_equity_base_locked"] = True
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


def _clean_wallet_model_exclude(raw: Any) -> Dict[str, bool]:
    """Sanitise app-level model exclusion map.
    Reversible, app/display only — engine, raw_live_fills, engine_truth,
    wallet_gate, live_config, manual_wallets are never mutated by this feature."""
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, bool] = {}
    for wallet, excluded in raw.items():
        w = str(wallet).strip().lower()
        if not w or w == USER_WALLET:
            continue
        if parse_bool(excluded):
            out[w] = True
    return out


_FILTER_METRIC_KEYS = [
    "lead_equity", "copy_equity", "lead_real", "copy_real",
    "lead_unreal", "copy_unreal", "lead_dd", "copy_dd",
    "lead_maxdd", "copy_maxdd", "delta",
    "pnl_per_hour", "avg_trade_pct", "win_rate",
    "avg_position_usd", "max_position_usd", "avg_entry_notional_usd",
    "pct_entries_ge10", "required_leverage",
    "fill_count", "exit_count", "open_position_count",
]


def _clean_wallet_filters(raw: Any) -> Dict[str, Any]:
    """Sanitise persistent metric filter state used to compute model exclusion."""
    if not isinstance(raw, dict):
        return {}
    allowed: set = {"wallet_contains"} | {f"{k}_{d}" for k in _FILTER_METRIC_KEYS for d in ("min", "max")}
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        if k not in allowed:
            continue
        if k == "wallet_contains":
            sv = str(v).strip().lower()
            if sv:
                out[k] = sv
        else:
            sv = str(v).strip()
            if sv:
                try:
                    out[k] = float(sv)
                except (ValueError, TypeError):
                    pass
    return out


def _clean_wallet_filter_last_result(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {
        "evaluated_count": max(0, inum(raw.get("evaluated_count"), 0)),
        "excluded_count": max(0, inum(raw.get("excluded_count"), 0)),
        "updated_at": str(raw.get("updated_at") or ""),
    }


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
        "min_trade_notional_enabled": parse_bool(raw.get("min_trade_notional_enabled", False)),
        "leader_equity_base": max(1.0, fnum(raw.get("leader_equity_base"), DEFAULT_LEADER_EQUITY)),
        "fee_bps": max(0.0, fnum(raw.get("fee_bps"), DEFAULT_FEE_BPS)),
        "copy_friction_bps": max(0.0, fnum(raw.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS)),
        "wallet_config": _clean_wallet_config(raw.get("wallet_config", {})),
        "wallet_include": _clean_wallet_include(raw.get("wallet_include", {})),
        "wallet_meta": sanitize_wallet_meta(raw.get("wallet_meta", {})),
        "ranking": ranking,
        "wallet_model_exclude": _clean_wallet_model_exclude(raw.get("wallet_model_exclude", {})),
        "wallet_filters": _clean_wallet_filters(raw.get("wallet_filters", {})),
        "wallet_filter_last_result": _clean_wallet_filter_last_result(raw.get("wallet_filter_last_result", {})),
    }


def apply_live_config_wallet_model(ui: Dict[str, Any]) -> Dict[str, Any]:
    """Let the 8014 live wallet controls drive app-model replay sizing.

    The engine remains raw-truth only. This bridge is app/presentation-layer
    only: values saved from the live wallet table are mirrored into the
    deterministic replay config so fixed/proportional changes alter modelled
    PnL/DD immediately.
    """
    out = dict(ui)
    wallet_cfg = dict(out.get("wallet_config") or {})
    live_config = _load_live_copy_config()
    live_wallets = live_config.get("wallets", {}) if isinstance(live_config.get("wallets"), dict) else {}
    for wallet, cfg in live_wallets.items():
        if not isinstance(cfg, dict):
            continue
        w = str(wallet).strip().lower()
        if not w:
            continue
        item: Dict[str, Any] = {}
        mode = str(cfg.get("copy_mode", "")).strip().lower()
        if mode in {"proportional", "fixed"}:
            item["copy_mode"] = mode
        if str(cfg.get("norm_base", "")).strip() != "":
            item["norm_base"] = max(1.0, fnum(cfg.get("norm_base"), DEFAULT_NORM_BASE))
        if str(cfg.get("fixed_notional", "")).strip() != "":
            item["fixed_notional"] = max(0.01, fnum(cfg.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL))
        if str(cfg.get("leader_equity_base", "")).strip() != "":
            item["leader_equity_base"] = max(1.0, fnum(cfg.get("leader_equity_base"), item.get("norm_base", DEFAULT_NORM_BASE)))
        if item:
            wallet_cfg[w] = item
    out["wallet_config"] = _clean_wallet_config(wallet_cfg)
    return out


def save_ui_state(patch: Dict[str, Any]) -> Dict[str, Any]:
    raw_existing = load_json(UI_STATE_FILE, {})
    raw_has_user_base = isinstance(raw_existing, dict) and "user_norm_base" in raw_existing
    existing = load_ui_state()
    merged = {**existing, **patch}
    mode = str(merged.get("copy_mode", "proportional")).lower()
    merged["copy_mode"] = mode if mode in {"proportional", "fixed"} else "proportional"
    merged["norm_base"] = max(1.0, fnum(merged.get("norm_base"), DEFAULT_NORM_BASE))
    merged["leader_equity_base"] = max(1.0, fnum(merged.get("leader_equity_base"), DEFAULT_LEADER_EQUITY))
    if "user_norm_base" in patch or raw_has_user_base:
        merged["user_norm_base"] = max(1.0, fnum(merged.get("user_norm_base"), merged.get("norm_base", DEFAULT_NORM_BASE)))
    else:
        merged["user_norm_base"] = merged["norm_base"]
    merged["fee_bps"] = max(0.0, fnum(merged.get("fee_bps"), DEFAULT_FEE_BPS))
    merged["copy_friction_bps"] = max(0.0, fnum(merged.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS))
    merged["min_trade_notional_enabled"] = parse_bool(merged.get("min_trade_notional_enabled", False))
    merged["wallet_config"] = _clean_wallet_config(merged.get("wallet_config", {}))
    merged["wallet_include"] = _clean_wallet_include(merged.get("wallet_include", {}))
    merged["wallet_meta"] = sanitize_wallet_meta(merged.get("wallet_meta", {}))
    merged["wallet_model_exclude"] = _clean_wallet_model_exclude(merged.get("wallet_model_exclude", {}))
    merged["wallet_filters"] = _clean_wallet_filters(merged.get("wallet_filters", {}))
    merged["wallet_filter_last_result"] = _clean_wallet_filter_last_result(merged.get("wallet_filter_last_result", {}))
    ranking = merged.get("ranking") if isinstance(merged.get("ranking"), dict) else {}
    ranking_dir = str(ranking.get("direction", "desc")).lower()
    merged["ranking"] = {"column": ranking.get("column"), "direction": ranking_dir if ranking_dir in {"asc", "desc"} else "desc"}
    atomic_write_json(UI_STATE_FILE, merged)
    # Fee/friction/mode changes require full model rebuild because delta
    # and copy PnL are baked into the model during build_model_state().
    # Wallet include/exclude changes only need an HTML cache clear.
    model_params_changed = (
        merged.get("fee_bps") != existing.get("fee_bps")
        or merged.get("copy_friction_bps") != existing.get("copy_friction_bps")
        or merged.get("copy_mode") != existing.get("copy_mode")
        or merged.get("norm_base") != existing.get("norm_base")
        or merged.get("leader_equity_base") != existing.get("leader_equity_base")
        or merged.get("fixed_notional") != existing.get("fixed_notional")
        or merged.get("min_trade_notional_enabled") != existing.get("min_trade_notional_enabled")
        or merged.get("wallet_config") != existing.get("wallet_config")
    )
    if model_params_changed:
        invalidate_model_cache()
    else:
        with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
            _MODEL_DASHBOARD_HTML_CACHE["html"] = None
            _MODEL_DASHBOARD_HTML_CACHE["built_at"] = 0.0
            _MODEL_DASHBOARD_HTML_CACHE["state_built_at"] = 0.0
            _MODEL_DASHBOARD_HTML_CACHE["disk_blocked"] = True
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
    response_config = dict(config)
    response_wallets: Dict[str, Any] = {}
    for wallet, cfg in wallets.items():
        cfg_copy = dict(cfg) if isinstance(cfg, dict) else {}
        latest_equity = _current_leader_equity_from_cache(str(wallet))
        if latest_equity is not None:
            cfg_copy["leader_equity_base"] = latest_equity
            cfg_copy["leader_equity_base_source"] = "8012:data/wallet_portfolios/accountValueHistory"
        response_wallets[wallet] = cfg_copy
    response_config["wallets"] = response_wallets
    return {
        "ok": True,
        "config": response_config,
        "wallet_count": len(wallets),
        "active_wallets": _active_live_copy_wallet_count(config),
        "max_wallets": 0,
        "wallet_cap": "unlimited",
        "core_reload_required": True,
        "core_confirmation_note": "Config saved — restart the Wallet Finder poll feeder to confirm runtime tracking in hl_copy_output/engine_truth.json.",
    }


def _live_config_error(error: str, status_code: int = 400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": error}, status_code=status_code)


def _enforce_live_copy_cap(config: Dict[str, Any]) -> None:
    return


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


def _compute_wallet_control_truth(
    live_config: Dict[str, Any],
    clean_core_status: Dict[str, Any],
    ws_health: Dict[str, Any],
) -> Dict[str, Any]:
    """Compare live_config active wallet set with Core runtime poll/WS sets.
    Core poll wallets come from clean_core_runtime_state.json last_leader_poll_cursor_ms.
    Core WS wallets come from live_ws_health.json wallets dict.
    CORE_CONFIRMED_LIVE = in poll AND in WS.
    CORE_POLL_ONLY_WS_MISSING = poll OK but WS sub missing (hot-reload gap).
    CONFIG_LIVE_NOT_IN_CORE = config says LIVE but Core not polling it.
    ARCHIVED_STILL_IN_CORE = archived in config but Core still tracking."""
    config_wallets = live_config.get("wallets", {}) if isinstance(live_config.get("wallets"), dict) else {}
    archived_wallets = live_config.get("archived_wallets", {}) if isinstance(live_config.get("archived_wallets"), dict) else {}
    core_state = clean_core_status.get("core_state", {}) if isinstance(clean_core_status.get("core_state"), dict) else {}
    poll_cursors = core_state.get("last_leader_poll_cursor_ms", {}) if isinstance(core_state.get("last_leader_poll_cursor_ms"), dict) else {}
    core_poll_set = {str(w).lower() for w in poll_cursors}
    health_wallets = ws_health.get("wallets", {}) if isinstance(ws_health.get("wallets"), dict) else {}
    core_ws_set = {str(w).lower() for w in health_wallets}
    config_active_set = {str(w).lower() for w in config_wallets}
    config_archived_set = {str(w).lower() for w in archived_wallets}
    wallet_truth: Dict[str, Any] = {}
    for wallet, cfg in config_wallets.items():
        w = str(wallet).lower()
        if not isinstance(cfg, dict):
            cfg = {}
        mode = str(cfg.get("mode", "OFF")).upper()
        enabled = bool(cfg.get("enabled", True))
        in_poll = w in core_poll_set
        in_ws = w in core_ws_set
        if mode in {"LIVE", "CLO"} and enabled:
            if in_poll and in_ws:
                truth = "CORE_CONFIRMED_LIVE"
            elif in_poll:
                truth = "CORE_POLL_ONLY_WS_MISSING"
            else:
                truth = "CONFIG_LIVE_NOT_IN_CORE"
        else:
            truth = "CONFIG_OFF"
        wallet_truth[wallet] = {
            "config_mode": mode,
            "config_enabled": enabled,
            "core_poll_present": in_poll,
            "core_ws_present": in_ws,
            "truth_status": truth,
        }
    archived_in_core: List[Dict[str, Any]] = []
    for wallet in archived_wallets:
        w = str(wallet).lower()
        if w in core_poll_set or w in core_ws_set:
            archived_in_core.append({
                "wallet": wallet,
                "in_poll": w in core_poll_set,
                "in_ws": w in core_ws_set,
                "status": "ARCHIVED_STILL_IN_CORE",
            })
    all_config = config_active_set | config_archived_set
    core_unknown: List[str] = [w for w in (core_poll_set | core_ws_set) if w not in all_config]
    config_not_in_core = [w for w, info in wallet_truth.items() if info["truth_status"] == "CONFIG_LIVE_NOT_IN_CORE"]
    poll_only_ws_missing = [w for w, info in wallet_truth.items() if info["truth_status"] == "CORE_POLL_ONLY_WS_MISSING"]
    # drift_count: CRITICAL — active config wallets missing from Core poll (requires Core reload).
    # archive_residual_count: BENIGN — archived wallets with preserved poll cursors (cursor
    #   preservation is normal after restart; does not mean Core is actively copying them).
    # ws_drift_count: MODERATE — active wallets polled OK but WS subscription missing.
    drift_count = len(config_not_in_core)
    archive_residual_count = len(archived_in_core)
    ws_drift_count = len(poll_only_ws_missing)
    status = "DRIFT" if drift_count > 0 else ("WS_DRIFT" if ws_drift_count > 0 else ("ARCHIVE_RESIDUAL" if archive_residual_count > 0 else "OK"))
    return {
        "config_active_count": len(config_active_set),
        "core_poll_count": len(core_poll_set),
        "core_ws_count": len(core_ws_set),
        "config_not_in_core": config_not_in_core,
        "poll_only_ws_missing": poll_only_ws_missing,
        "core_not_in_config": core_unknown,
        "archived_still_in_core": [x["wallet"] for x in archived_in_core],
        "archived_in_core_detail": archived_in_core,
        "drift_count": drift_count,
        "archive_residual_count": archive_residual_count,
        "ws_drift_count": ws_drift_count,
        "status": status,
        "wallet_truth": wallet_truth,
        "core_poll_available": bool(core_poll_set),
        "core_ws_available": bool(core_ws_set),
    }


def _load_live_integrity_status() -> Dict[str, Any]:
    data = load_json(LIVE_COPY_INTEGRITY_STATUS_FILE, None)
    if isinstance(data, dict) and data:
        status = str(data.get("status") or "UNKNOWN").upper()
        return {**data, "status": status, "available": True}
    return {
        "status": "UNKNOWN",
        "available": False,
        "reasons": ["RUN INTEGRITY GATE"],
        "counts": {},
        "notes": "live_integrity_status.json missing",
    }


def _load_ownership_truth_gate() -> Dict[str, Any]:
    gate_path = OWNERSHIP_TRUTH_GATE_FILE
    data = load_json(gate_path, None)
    if not isinstance(data, dict):
        data = None
        for encoding in ("utf-8-sig", "utf-8"):
            try:
                if gate_path.exists():
                    data = json.loads(gate_path.read_text(encoding=encoding))
                break
            except UnicodeDecodeError:
                continue
            except Exception:
                data = None
                break
    if isinstance(data, dict) and data:
        status = str(data.get("status") or "UNKNOWN").upper()
        rows = data.get("rows") if isinstance(data.get("rows"), list) else []
        return {**data, "status": status, "rows": rows, "available": True}
    return {
        "status": "UNKNOWN",
        "available": False,
        "rows": [],
        "red_count": 0,
        "amber_count": 0,
        "note": "ownership_truth_gate.json missing",
    }


def _apply_ownership_truth_to_integrity(
    integrity_status: Dict[str, Any],
    ownership_truth_gate: Dict[str, Any],
) -> Dict[str, Any]:
    gate_status = str(ownership_truth_gate.get("status") or "UNKNOWN").upper()
    gate_available = bool(ownership_truth_gate.get("available"))
    current_status = str(integrity_status.get("status") or "UNKNOWN").upper()
    effective_status = current_status
    if gate_status == "RED":
        effective_status = "RED"
    elif gate_status == "AMBER" and current_status != "RED":
        effective_status = "AMBER"
    elif gate_status == "UNKNOWN" and gate_available and current_status not in {"RED", "AMBER"}:
        # Gate file is present and parsed but status is unresolved — treat as AMBER
        # so a corrupt/incomplete gate cannot silently allow GREEN through.
        effective_status = "AMBER"
    if effective_status == current_status:
        return integrity_status
    reasons = list(integrity_status.get("reasons") if isinstance(integrity_status.get("reasons"), list) else [])
    reason = f"OWNERSHIP_TRUTH_GATE_{gate_status}"
    if reason not in reasons:
        reasons.append(reason)
    return {
        **integrity_status,
        "raw_status": current_status,
        "status": effective_status,
        "reasons": reasons,
        "ownership_truth_gate_status": gate_status,
        "ownership_truth_gate_available": gate_available,
    }


_OWNERSHIP_STATUS_TO_SEVERITY: Dict[str, str] = {
    "SIGN_CONFLICT_PENDING_RECONCILIATION": "RED",
    "LEDGER_UNSUPPORTED_BY_EXCHANGE": "RED",
    "OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE": "RED",
    "EXTERNAL_FLAT_PENDING_RECONCILIATION": "RED",
    "RESIDUAL_MISMATCH": "AMBER",
    "OWNED_PLUS_ACCOUNT_RESIDUAL": "AMBER",
    "ORPHAN_EXCHANGE": "AMBER",
    "FLAT": "OK",
    "OWNED_FULLY_SUPPORTED": "OK",
    "MATCH": "OK",
}


def _derive_ownership_severity(status: str, raw_severity: str) -> str:
    if raw_severity in {"RED", "AMBER"}:
        return raw_severity
    mapped = _OWNERSHIP_STATUS_TO_SEVERITY.get(status.upper())
    if mapped:
        return mapped
    if status:
        return "AMBER"
    return ""


def _build_ownership_truth_execution_quality_rows(ownership_truth_gate: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    now_ms = int(time.time() * 1000)
    created_at = str(ownership_truth_gate.get("created_at") or "")
    created_ms = _iso_to_ms(created_at)
    for row in ownership_truth_gate.get("rows", []):
        if not isinstance(row, dict):
            continue
        raw_severity = str(row.get("severity") or "").upper()
        status = str(row.get("status") or "OWNERSHIP_TRUTH_MISMATCH").upper()
        severity = _derive_ownership_severity(status, raw_severity)
        if severity not in {"RED", "AMBER"}:
            continue
        coin = str(row.get("coin") or "")
        exchange_net = fnum(row.get("exchange_net"))
        manual_net = fnum(row.get("manual_net"))
        diff = fnum(row.get("diff_exchange_minus_manual"))
        explanation = f"{coin} ownership truth {severity}: {status}; exchange_net={exchange_net:g} manual_net={manual_net:g} diff={diff:g}"
        rows.append({
            "time": created_at,
            "leader_wallet": "",
            "coin": coin,
            "side": "",
            "severity": severity,
            "status": status,
            "exchange_net": exchange_net,
            "manual_net": manual_net,
            "diff": diff,
            "diff_exchange_minus_manual": diff,
            "owners": row.get("owners", ""),
            "reject_category": "OWNERSHIP_TRUTH",
            "terminal_state": status,
            "raw_terminal_state": status,
            "operator_action": "MANUAL_RECONCILIATION_REQUIRED" if severity == "RED" else "REVIEW_RESIDUAL_MISMATCH",
            "limit_px": None,
            "fill_avg_px": None,
            "fill_size": "",
            "oid": "",
            "wallet_position_before": "",
            "wallet_position_after": "",
            "leader_to_send_attempt_ms": "",
            "send_total_ms": "",
            "symbol_resolve_ms": "",
            "sdk_client_ms": "",
            "exchange_call_ms": "",
            "has_live_fill": False,
            "truth_state": status,
            "truth_severity": severity,
            "is_active": True,
            "fill_bps": None,
            "leader_bps": None,
            "marketable_bps": None,
            "source_label": "ownership_truth_gate",
            "age_label": _age_label_from_ms(created_ms, now_ms),
            "error": explanation,
            "explanation": explanation,
        })
    return rows


def _load_recent_reconciliation_rows(limit: int = 500, include_intent_ids: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    include = {str(x) for x in (include_intent_ids or []) if str(x)}
    included_by_intent: Dict[str, Dict[str, Any]] = {}
    if not LIVE_COPY_RECONCILIATION_CSV.exists():
        return out
    try:
        with LIVE_COPY_RECONCILIATION_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                item = dict(row)
                out.append(item)
                iid = str(item.get("intent_id") or "")
                if iid in include:
                    included_by_intent[iid] = item
        recent = out[-limit:]
        seen = {id(r) for r in recent}
        for row in included_by_intent.values():
            if id(row) not in seen:
                recent.append(row)
        return recent
    except Exception:
        return out[-limit:] if len(out) >= limit else out


def _latest_row(rows: List[Dict[str, Any]], predicate) -> Dict[str, Any]:
    for row in reversed(rows):
        try:
            if predicate(row):
                return row
        except Exception:
            continue
    return {}


def _row_time_ms(row: Dict[str, Any]) -> int:
    for key in ("created_at_ms", "timestamp_ms", "time_ms", "updated_at_ms"):
        ms = fnum(row.get(key))
        if ms > 0:
            return int(ms)
    for key in ("created_at", "timestamp", "time", "updated_at"):
        raw = row.get(key)
        if fnum(raw) > 1_000_000_000_000:
            return int(fnum(raw))
        ms = _iso_to_ms(raw)
        if ms > 0:
            return ms
    return 0


def _age_label_from_ms(ms: int, now_ms: Optional[int] = None) -> str:
    if not ms:
        return "n/a"
    now_ms = now_ms or int(time.time() * 1000)
    delta = max(0, now_ms - int(ms))
    if delta < 60_000:
        return f"{int(delta / 1000)}s ago"
    if delta < 3_600_000:
        return f"{int(delta / 60_000)}m ago"
    if delta < 86_400_000:
        return f"{round(delta / 3_600_000, 1)}h ago"
    return f"{round(delta / 86_400_000, 1)}d ago"


def _build_execution_quality_freshness(
    send_attempts: List[Dict[str, Any]],
    live_fills: List[Dict[str, Any]],
    reconciliation_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    def latest(rows: List[Dict[str, Any]]) -> Tuple[int, Dict[str, Any]]:
        best_ms = 0
        best_row: Dict[str, Any] = {}
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            ms = _row_time_ms(row)
            if ms >= best_ms:
                best_ms = ms
                best_row = row
        return best_ms, best_row

    send_ms, send_row = latest(send_attempts)
    fill_ms, fill_row = latest(live_fills)
    recon_ms, recon_row = latest(reconciliation_rows)
    return {
        "send_attempts": {"latest_ms": send_ms, "age_label": _age_label_from_ms(send_ms, now_ms), "rows_loaded": len(send_attempts or []), "latest_status": send_row.get("status", "") if send_row else ""},
        "live_fills": {"latest_ms": fill_ms, "age_label": _age_label_from_ms(fill_ms, now_ms), "rows_loaded": len(live_fills or []), "latest_status": fill_row.get("fill_status", "") if fill_row else ""},
        "reconciliation": {"latest_ms": recon_ms, "age_label": _age_label_from_ms(recon_ms, now_ms), "rows_loaded": len(reconciliation_rows or []), "latest_event": recon_row.get("event", "") if recon_row else ""},
    }


def _build_live_top_status(
    live_config: Dict[str, Any],
    ws_health: Dict[str, Any],
    service_state: Dict[str, Any],
    integrity_status: Dict[str, Any],
    send_attempts: List[Dict[str, Any]],
    live_fills: List[Dict[str, Any]],
    reconciliation_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    wallets = live_config.get("wallets", {}) if isinstance(live_config.get("wallets"), dict) else {}
    ws_summary = ws_health.get("ws_summary", {}) if isinstance(ws_health.get("ws_summary"), dict) else {}
    ws_raw = str(ws_summary.get("ws_status") or ws_health.get("overall") or "DOWN").upper()
    ws_status = "WS_OK" if ws_raw == "WS_OK" and bool(ws_summary.get("socket_open", True)) and bool(ws_summary.get("thread_alive", True)) else "DOWN"
    copy_poll = str(service_state.get("copy_account_status") or service_state.get("last_copy_account_status") or service_state.get("copy_status") or "DOWN").upper()
    if not copy_poll or copy_poll in {"", "NONE", "UNKNOWN"}:
        copy_poll = "DOWN"
    master = bool(service_state.get("master_real_orders_enabled")) if "master_real_orders_enabled" in service_state else parse_bool(live_config.get("auto_send_enabled"))
    effective = bool(service_state.get("effective_real_orders_enabled")) if "effective_real_orders_enabled" in service_state else master
    send_block_reason = str(service_state.get("send_block_reason") or ("" if effective else "MASTER_REAL_ORDERS_OFF"))
    active_wallets = sum(1 for _, cfg in wallets.items() if isinstance(cfg, dict) and str(cfg.get("mode", "")).upper() in {"LIVE", "CLO"} and bool(cfg.get("enabled", True)))
    subscribed_wallets = inum(ws_summary.get("wallet_count") or ws_summary.get("subscribed_wallet_count") or len(ws_health.get("wallets", {}) if isinstance(ws_health.get("wallets"), dict) else {}))
    last_send = send_attempts[-1] if send_attempts else {}
    last_fill = live_fills[-1] if live_fills else {}
    last_reject = _latest_row(send_attempts, lambda r: str(r.get("status") or "").upper() == "ORDER_REJECTED")
    last_latency = _latest_row(reconciliation_rows, lambda r: str(r.get("event") or "") == "SEND_LATENCY_WARN")
    return {
        "integrity_status": str(integrity_status.get("status") or "UNKNOWN").upper(),
        "integrity_available": bool(integrity_status.get("available")),
        "integrity_reasons": integrity_status.get("reasons") if isinstance(integrity_status.get("reasons"), list) else [],
        "ws_status": ws_status,
        "copy_poll_status": copy_poll,
        "auto_send": "ON" if effective else "OFF",
        "master_real_orders": "ON" if master else "OFF",
        "master_real_orders_enabled": master,
        "effective_real_orders_enabled": effective,
        "send_block_reason": send_block_reason,
        "wallet_modes_active": service_state.get("wallet_modes_active") if isinstance(service_state.get("wallet_modes_active"), dict) else {},
        "active_wallets": active_wallets,
        "subscribed_wallets": subscribed_wallets,
        "last_send": last_send,
        "last_fill": last_fill,
        "last_reject": last_reject,
        "last_latency_warning": last_latency,
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
    ownership_truth_gate = _load_ownership_truth_gate()
    integrity_status = _apply_ownership_truth_to_integrity(_load_live_integrity_status(), ownership_truth_gate)
    full_exchange_audit = load_json(FULL_EXCHANGE_AUDIT_FILE, {})
    if not isinstance(full_exchange_audit, dict):
        full_exchange_audit = {}
    visible_intent_ids = [
        str(row.get("intent_id") or "")
        for row in last_rows
        if isinstance(row, dict) and str(row.get("intent_id") or "")
    ]
    reconciliation_rows = _load_recent_reconciliation_rows(500, visible_intent_ids)

    live_config = _load_live_copy_config()
    _auto_send_enabled = parse_bool(live_config.get("auto_send_enabled"))
    _auto_send_filter = os.getenv("HL_LIVE_AUTO_SEND_WALLET", "").lower().strip()
    _cfg_wallets = live_config.get("wallets", {})
    _auto_live_eligible: List[str] = [
        w.lower() for w, cfg in (_cfg_wallets.items() if isinstance(_cfg_wallets, dict) else [])
        if isinstance(cfg, dict)
        and normalize_live_wallet_config(w, cfg).get("service_eligible")
        and (not _auto_send_filter or w.lower() == _auto_send_filter)
    ] if _auto_send_enabled else []
    model_portfolio: Dict[str, Any] = {}
    try:
        _model_state_for_live = build_model_state(use_live_config_wallet_model=True)
    except Exception:
        _model_state_for_live = {}
    live_wallet_derived = _build_live_wallet_derived(
        _model_state_for_live,
        live_config,
        audit_rows,
        metric_send_attempts,
        manual_positions,
        manual_live_summary,
    )
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
    live_wallet_rows = _build_live_wallet_rows(
        live_config,
        audit_rows,
        metric_send_attempts,
        manual_positions,
        manual_live_summary,
        ws_health,
        live_leader_performance,
        live_fills=_live_fills_data,
        live_wallet_derived=live_wallet_derived,
    )
    wallet_control_truth = _compute_wallet_control_truth(live_config, clean_core_status, ws_health)
    _wct_wallet_truth = wallet_control_truth.get("wallet_truth", {})
    for _row in live_wallet_rows:
        _rw = str(_row.get("wallet", "")).lower()
        _wt = _wct_wallet_truth.get(_rw) or {}
        _row["core_truth_status"] = _wt.get("truth_status", "CORE_CONFIRMATION_UNAVAILABLE")
        _row["core_poll_present"] = bool(_wt.get("core_poll_present"))
        _row["core_ws_present"] = bool(_wt.get("core_ws_present"))
    real_copy_positions = _build_real_copy_positions(manual_positions, exchange_snapshot)
    ownership_truth_execution_quality_rows = _build_ownership_truth_execution_quality_rows(ownership_truth_gate)
    execution_quality_rows = _build_execution_quality_rows(metric_send_attempts, audit_rows, live_fills=_live_fills_data)
    execution_quality_rows = _build_reconciliation_execution_quality_rows(reconciliation_rows) + execution_quality_rows
    hard_copy = integrity_status.get("hard_copy_invariant") if isinstance(integrity_status.get("hard_copy_invariant"), dict) else {}
    hard_copy_problem_rows = []
    if isinstance(hard_copy, dict):
        hard_copy_problem_rows.extend(hard_copy.get("active_red_rows") if isinstance(hard_copy.get("active_red_rows"), list) else [])
        hard_copy_problem_rows.extend(hard_copy.get("unclassified_rows") if isinstance(hard_copy.get("unclassified_rows"), list) else [])
        hard_copy_problem_rows.extend(hard_copy.get("missed_terminal_rows") if isinstance(hard_copy.get("missed_terminal_rows"), list) else [])
    for _row in hard_copy_problem_rows:
        execution_quality_rows.insert(0, {
            "time": _row.get("created_at", ""),
            "leader_wallet": _row.get("leader_wallet", ""),
            "coin": _row.get("coin", ""),
            "side": "",
            "status": "HARD_COPY_INVARIANT",
            "reject_category": "COPY_CONTRACT",
            "terminal_state": _row.get("classification", "ACTIVE_RED"),
            "raw_terminal_state": _row.get("classification", "ACTIVE_RED"),
            "operator_action": _row.get("reason", ""),
            "limit_px": None,
            "fill_avg_px": None,
            "fill_size": "",
            "oid": "",
            "wallet_position_before": "",
            "wallet_position_after": "",
            "leader_to_send_attempt_ms": "",
            "send_total_ms": "",
            "symbol_resolve_ms": "",
            "sdk_client_ms": "",
            "exchange_call_ms": "",
            "has_live_fill": False,
            "truth_state": _row.get("classification", "ACTIVE_RED"),
            "truth_severity": "RED" if "MISSED_" not in str(_row.get("classification", "")) else "AMBER",
            "is_active": True,
            "fill_bps": None,
            "leader_bps": None,
            "marketable_bps": None,
            "error": _row.get("reason", ""),
        })
    execution_quality_rows = ownership_truth_execution_quality_rows + execution_quality_rows
    # Sort: active RED → active AMBER → active other → inactive; preserve intra-bucket order
    def _eq_sort_key(r: Dict[str, Any]) -> int:
        if r.get("is_active"):
            sev = str(r.get("truth_severity") or "").upper()
            if sev == "RED":
                return 0
            if sev == "AMBER":
                return 1
            return 2
        return 3
    execution_quality_rows = sorted(execution_quality_rows, key=_eq_sort_key)
    execution_quality_summary = _build_execution_quality_summary(execution_quality_rows)
    execution_quality_freshness = _build_execution_quality_freshness(metric_send_attempts, _live_fills_data, reconciliation_rows)
    manual_reconciliation_rows = _build_manual_reconciliation_rows(manual_positions, exchange_snapshot, recent_send_attempts, live_config=live_config, integrity_status=integrity_status)
    recent_send_warning_groups = _build_recent_send_warning_groups(recent_send_attempts)
    live_top_status = _build_live_top_status(live_config, ws_health, clean_core_status.get("service_state", {}), integrity_status, recent_send_attempts, _live_fills_data, reconciliation_rows)
    owned_copy_positions = [r for r in real_copy_positions if r.get("row_type") == "OWNED_COPY"]
    orphan_exchange_positions = [r for r in real_copy_positions if r.get("row_type") == "ACCOUNT_LEVEL_ONLY"]
    account_orphan_registry = _build_account_orphan_registry(orphan_exchange_positions)
    send_terminal_rows = [r for r in reconciliation_rows if r.get("event") in {"SEND_TERMINAL", "SEND_REJECTED"}]
    legacy_terminal_rows = [
        r for r in recent_send_attempts
        if str(r.get("status") or "").upper() == "ORDER_REJECTED"
        and not (r.get("terminal_state") and r.get("operator_action") and r.get("reject_category"))
    ]

    portfolio_history = load_json(PORTFOLIO_HISTORY_FILE, [])
    if not isinstance(portfolio_history, list):
        portfolio_history = []
    exchange_history = load_json(EXCHANGE_ACCOUNT_HISTORY_FILE, [])
    if not isinstance(exchange_history, list):
        exchange_history = []
    fills_recent = exchange_snapshot.get("actual_user_fills_graph")
    if not isinstance(fills_recent, list) or not fills_recent:
        fills_recent = exchange_snapshot.get("actual_user_fills_recent", [])
    rpnl_points: List[Dict[str, Any]] = []
    for _fill in (fills_recent if isinstance(fills_recent, list) else []):
        if not isinstance(_fill, dict):
            continue
        _ts = inum(_fill.get("time") or _fill.get("timestamp") or _fill.get("ts"))
        if _ts <= 0:
            continue
        rpnl_points.append({
            "ts": _ts,
            "timestamp": datetime.fromtimestamp(_ts / 1000, tz=timezone.utc).isoformat(),
            "pnl": fnum(_fill.get("closedPnl") or _fill.get("closed_pnl") or 0),
            "coin": _fill.get("coin", ""),
            "side": _fill.get("side", ""),
            "oid": _fill.get("oid", ""),
        })
    rpnl_points.sort(key=lambda x: x["ts"])

    # Position Integrity card — summarises Core truth for the UI operator view.
    # "Real orphan" means an exchange position with no engine ownership and no service evidence:
    # EXTERNAL_ORPHAN_UNOWNED or SERVICE_CREATED_UNLEDGERED. RESIDUAL_ACCOUNT_LEVEL and
    # EXTERNAL_ORPHAN_UNTRACKED rows are accounted ledger artefacts, not live action items.
    _pic_integ_counts = integrity_status.get("counts", {}) if isinstance(integrity_status, dict) else {}
    _pic_pos_assign = integrity_status.get("position_assignment", {}) if isinstance(integrity_status, dict) else {}
    _pic_hard_inv = integrity_status.get("hard_copy_invariant", {}) if isinstance(integrity_status, dict) else {}
    _pic_hard_counts = _pic_hard_inv.get("counts", {}) if isinstance(_pic_hard_inv, dict) else {}
    _real_unmanaged_orphans = [
        r for r in real_copy_positions
        if r.get("row_type") == "ACCOUNT_LEVEL_ONLY"
        and r.get("orphan_classification") in {"EXTERNAL_ORPHAN_UNOWNED", "SERVICE_CREATED_UNLEDGERED"}
    ]
    _xyz_owned_rows = [
        r for r in owned_copy_positions
        if str(r.get("coin", "")).startswith("XYZ:") or str(r.get("coin", "")).startswith("FLX:")
    ]
    _has_btc_residual = any(
        str(r.get("leader_wallet", "")).lower() == "external_orphan_untracked"
        for r in owned_copy_positions
    )
    position_integrity_card = {
        "core_status": str(integrity_status.get("status", "UNKNOWN")) if isinstance(integrity_status, dict) else "UNKNOWN",
        "position_assignment": str(_pic_pos_assign.get("status", "UNKNOWN")),
        "exchange_manual_mismatch_count": inum(_pic_integ_counts.get("exchange_manual_mismatch")),
        "hard_copy_active_red": inum(_pic_hard_counts.get("ACTIVE_RED")),
        "hard_copy_unclassified": inum(_pic_hard_counts.get("UNCLASSIFIED")),
        "missed_entry": inum(_pic_integ_counts.get("missed_entry")),
        "missed_add": inum(_pic_integ_counts.get("missed_add")),
        "real_orphan_count": len(_real_unmanaged_orphans),
        "real_orphan_exposure_usd": round(sum(abs(fnum(r.get("position_value") or 0)) for r in _real_unmanaged_orphans), 2),
        "owned_copy_count": len(owned_copy_positions),
        "xyz_exotic_matched_count": len(_xyz_owned_rows),
        "btc_residual_accounted": _has_btc_residual,
        "integrity_timestamp": str(integrity_status.get("created_at", "")) if isinstance(integrity_status, dict) else "",
    }

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
        "recent_metric_send_attempts": metric_send_attempts[-500:],
        "send_attempt_counts": send_attempt_counts,
        "manual_live_summary": manual_live_summary,
        "manual_reconciliation_rows": manual_reconciliation_rows,
        "reconciliation_rows": reconciliation_rows,
        "recent_reconciliation_events": reconciliation_rows[-50:],
        "send_terminal_rows": send_terminal_rows[-100:],
        "legacy_terminal_rows": legacy_terminal_rows,
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
            "realized_pnl_point_count": len(rpnl_points),
            "realized_pnl_selected": exchange_snapshot.get("realized_pnl_selected"),
            "realized_pnl_selected_label": exchange_snapshot.get("realized_pnl_selected_label", ""),
            "realized_pnl_selected_start": exchange_snapshot.get("realized_pnl_selected_start", ""),
            "realized_pnl_selected_end": exchange_snapshot.get("realized_pnl_selected_end", ""),
            "realized_pnl_selected_fill_count": exchange_snapshot.get("realized_pnl_selected_fill_count", 0),
            "realized_pnl_all_available_window": exchange_snapshot.get("realized_pnl_all_available_window"),
            "realized_pnl_all_available_fill_count": exchange_snapshot.get("realized_pnl_all_available_fill_count", 0),
            "realized_pnl_graph_source": exchange_snapshot.get("actual_user_fills_source", ""),
        },
        "live_wallet_derived": live_wallet_derived,
        "live_wallet_rows": live_wallet_rows,
        "live_leader_performance": live_leader_performance,
        "real_copy_positions": real_copy_positions,
        "owned_copy_positions": owned_copy_positions,
        "orphan_exchange_positions": orphan_exchange_positions,
        "account_orphan_registry": account_orphan_registry,
        "execution_quality_rows": execution_quality_rows,
        "execution_quality_summary": execution_quality_summary,
        "execution_quality_freshness": execution_quality_freshness,
        "auto_live_eligible_wallets": _auto_live_eligible,
        "tracked_wallet_count": len(_cfg_wallets) if isinstance(_cfg_wallets, dict) else 0,
        "auto_live_wallet_count": len(_auto_live_eligible),
        "clean_core_status": clean_core_status,
        "core_service_state": clean_core_status.get("service_state", {}),
        "master_real_orders_enabled": live_top_status.get("master_real_orders_enabled"),
        "effective_real_orders_enabled": live_top_status.get("effective_real_orders_enabled"),
        "send_block_reason": live_top_status.get("send_block_reason"),
        "live_integrity_status": integrity_status,
        "live_top_status": live_top_status,
        "ownership_truth_gate": ownership_truth_gate,
        "full_exchange_audit": full_exchange_audit,
        "position_integrity_card": position_integrity_card,
        "wallet_control_truth": wallet_control_truth,
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


def _exchange_snapshot_available(exchange_snapshot: Any) -> bool:
    if not isinstance(exchange_snapshot, dict):
        return False
    if exchange_snapshot.get("available") is True:
        return True
    if exchange_snapshot.get("ok") is True:
        return True
    positions = exchange_snapshot.get("positions_by_coin")
    if isinstance(positions, dict) and positions:
        return True
    open_positions = exchange_snapshot.get("open_positions")
    if isinstance(open_positions, list):
        return True
    return False


def _classify_owned_exchange_net(owned_net: float, exchange_net: float, exchange_available: bool, eps: float = 1e-8) -> Dict[str, Any]:
    residual = exchange_net - owned_net if exchange_available else 0.0
    unsupported = 0.0
    if not exchange_available:
        status = "EXCHANGE_UNAVAILABLE"
    elif abs(owned_net) <= eps and abs(exchange_net) <= eps:
        status = "FLAT"
    elif abs(owned_net) <= eps and abs(exchange_net) > eps:
        status = "ORPHAN_EXCHANGE"
    elif abs(exchange_net) <= eps and abs(owned_net) > eps:
        status = "EXTERNAL_FLAT_PENDING_RECONCILIATION"
        unsupported = owned_net
    elif owned_net * exchange_net < 0:
        status = "SIGN_CONFLICT_PENDING_RECONCILIATION"
        unsupported = owned_net
    elif abs(exchange_net - owned_net) <= eps:
        status = "OWNED_FULLY_SUPPORTED"
    elif abs(exchange_net) > abs(owned_net):
        status = "OWNED_PLUS_ACCOUNT_RESIDUAL"
    else:
        status = "OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE"
        unsupported = owned_net - exchange_net
    return {
        "status": status,
        "owned_net": owned_net,
        "exchange_net": exchange_net if exchange_available else None,
        "residual_size": residual if exchange_available and abs(residual) > eps else 0.0,
        "unsupported_size": unsupported if abs(unsupported) > eps else 0.0,
    }


_RECON_SNAPSHOT_STALE_MS = 300_000  # 5 minutes; beyond this, repair actions are blocked


def _classify_recon_action_type(
    issue: str,
    coin: str,
    signed: float,
    exchange_signed: float,
    pos: Dict[str, Any],
    exchange_snapshot_age_ms: int,
    core_blocked: bool,
    snapshot_available: bool,
) -> Dict[str, Any]:
    """Map a reconciliation row's issue/context to an action type. Read-only classification — no mutation."""
    coin_upper = str(coin).upper()
    snapshot_stale = snapshot_available and exchange_snapshot_age_ms > _RECON_SNAPSHOT_STALE_MS
    if issue in {"OWNED_FULLY_SUPPORTED", "SHARED_SYMBOL_SLEEVE_TRACKED", "SHARED_SYMBOL_NET_MATCH", "FLAT"} or issue.startswith("ARCHIVED_LEDGER_RESIDUAL_ACCOUNTED"):
        return {
            "action_type": "NO_ACTION_TERMINAL",
            "action_label": "NO_ACTION",
            "action_blocked_reason": "",
            "operator_summary": (
                "Active ledger net matches exchange; archived residual is preserved for audit history"
                if issue.startswith("ARCHIVED_LEDGER_RESIDUAL_ACCOUNTED")
                else "Ledger and exchange are consistent — no operator action required"
            ),
        }
    if issue == "EXCHANGE_UNAVAILABLE" or not snapshot_available:
        return {
            "action_type": "REFRESH_RECHECK",
            "action_label": "REFRESH_PROOF",
            "action_blocked_reason": "",
            "operator_summary": "Exchange snapshot unavailable — trigger Core exchange check to refresh",
        }
    if snapshot_stale:
        return {
            "action_type": "REFRESH_RECHECK",
            "action_label": "REFRESH_PROOF",
            "action_blocked_reason": "",
            "operator_summary": f"Exchange snapshot stale (>{_RECON_SNAPSHOT_STALE_MS // 60000}min) — trigger Core exchange check",
        }
    if issue == "OWNED_PLUS_ACCOUNT_RESIDUAL":
        return {
            "action_type": "MARK_RESIDUAL_ACCOUNTED",
            "action_label": "RESIDUAL_ACCOUNTED",
            "action_blocked_reason": "",
            "operator_summary": "Account-level residual beyond owned sleeve — no action unless aggregate net diffs",
        }
    if issue in {"EXTERNAL_FLAT_PENDING_RECONCILIATION", "OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE"}:
        exchange_is_flat = abs(exchange_signed) <= 1e-8
        if exchange_is_flat:
            if coin_upper.startswith("XYZ:"):
                blocked = "XYZ: clearinghouseState structurally blind; manual verify required"
                return {
                    "action_type": "MANUAL_VERIFY_XYZ_SNAPSHOT_BLIND",
                    "action_label": "MANUAL_VERIFY",
                    "action_blocked_reason": blocked,
                    "operator_summary": f"Ledger open ({signed:+g}) vs exchange flat signal is not trusted for XYZ; verify on exchange before any repair",
                }
            elif not str(pos.get("last_copy_fill_id") or pos.get("last_oid") or "").strip():
                blocked = "No copy fill proof (last_copy_fill_id missing on sleeve)"
            elif core_blocked:
                blocked = "Core has ACTIVE_RED or UNCLASSIFIED rows — resolve first"
            else:
                blocked = ""
            return {
                "action_type": "ADOPT_MANUAL_CLOSE",
                "action_label": "ADOPT_MANUAL_CLOSE",
                "action_blocked_reason": blocked,
                "operator_summary": (
                    f"Ledger open ({signed:+g}) but exchange flat — run: Core --repair-flat-positions --coin {coin_upper}"
                    if not blocked else f"Ledger open ({signed:+g}) vs exchange flat — blocked: {blocked}"
                ),
            }
        blocked = f"Exchange not flat ({exchange_signed:+g}); confirm flat before repair"
        if core_blocked:
            blocked = "Core ACTIVE_RED/UNCLASSIFIED; " + blocked
        return {
            "action_type": "ADOPT_MANUAL_CLOSE",
            "action_label": "ADOPT_MANUAL_CLOSE",
            "action_blocked_reason": blocked,
            "operator_summary": f"Ledger {signed:+g} partially unsupported — blocked: {blocked}",
        }
    if issue == "SIGN_CONFLICT_PENDING_RECONCILIATION":
        blocked = "Core ACTIVE_RED/UNCLASSIFIED — resolve first" if core_blocked else ""
        return {
            "action_type": "ASSIGN_ORPHAN_TO_WALLET",
            "action_label": "ASSIGN_ORPHAN_TO_WALLET",
            "action_blocked_reason": blocked,
            "operator_summary": (
                f"Sign conflict: ledger {signed:+g} vs exchange {exchange_signed:+g} — "
                "investigate and assign; requires Core repair with audit note"
                + (" [BLOCKED: core issues present]" if blocked else "")
            ),
        }
    if issue == "MISSING_LEDGER":
        blocked = "Core ACTIVE_RED/UNCLASSIFIED — resolve first" if core_blocked else ""
        return {
            "action_type": "ASSIGN_ORPHAN_TO_WALLET",
            "action_label": "ASSIGN_ORPHAN_TO_WALLET",
            "action_blocked_reason": blocked,
            "operator_summary": f"Exchange has {exchange_signed:+g} with no ledger entry — assign to wallet or investigate",
        }
    if issue == "MISSING_EXCHANGE":
        return {
            "action_type": "ASSIGN_ORPHAN_TO_WALLET",
            "action_label": "ASSIGN_ORPHAN_TO_WALLET",
            "action_blocked_reason": "Shared symbol: verify per-wallet sleeves before assigning",
            "operator_summary": "Aggregate ledger net nonzero but exchange flat — check each wallet sleeve",
        }
    if issue.startswith("SHARED_SYMBOL_NET_DIFF"):
        return {
            "action_type": "ASSIGN_ORPHAN_TO_WALLET",
            "action_label": "ASSIGN_ORPHAN_TO_WALLET",
            "action_blocked_reason": "Shared symbol aggregate diff — per-wallet investigation required",
            "operator_summary": f"Aggregate net differs from exchange: {issue}",
        }
    return {
        "action_type": "NONE",
        "action_label": "NONE",
        "action_blocked_reason": "",
        "operator_summary": f"Unclassified issue: {issue}",
    }


def _exchange_field(ex: Any, key: str) -> float:
    if not isinstance(ex, dict):
        return fnum(ex) if key in {"signed_size", "szi", "size", "net", "signed_net"} else 0.0
    for candidate in (key, "signed_size", "szi", "size", "net", "signed_net") if key == "signed_size" else (key,):
        if candidate in ex:
            return fnum(ex.get(candidate))
    return 0.0


def _exchange_signed_size(ex: Any) -> float:
    return _exchange_field(ex, "signed_size")


def _owned_sleeve_unrealized(signed: float, avg_entry_px: float, mark_px: float) -> Optional[float]:
    if abs(signed) <= 1e-12 or avg_entry_px <= 0 or mark_px <= 0:
        return None
    return round((mark_px - avg_entry_px) * signed, 8)


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


def _row_exchange_order_id(row: Dict[str, Any]) -> str:
    for key in ("exchange_order_id", "oid", "order_id", "orderId"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    notes = str(row.get("notes") or "")
    marker = "exchange_order_id="
    if marker in notes:
        tail = notes.split(marker, 1)[1]
        return tail.split(";", 1)[0].split()[0].strip()
    return ""


def _signed_from_side_size(side: Any, size: Any) -> float:
    side_u = str(side or "").upper()
    qty = abs(fnum(size))
    if side_u in {"BUY", "B", "LONG"}:
        return qty
    if side_u in {"SELL", "S", "SHORT"}:
        return -qty
    return 0.0


def _service_position_evidence_by_coin(
    send_attempts: Optional[List[Dict[str, Any]]] = None,
    live_fills: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    send_attempts = send_attempts if send_attempts is not None else _load_recent_send_attempts(250000)
    live_fills = live_fills if live_fills is not None else _load_recent_live_fills(250000)
    live_intents = {str(row.get("intent_id") or "") for row in live_fills if row.get("intent_id")}
    live_oids = {_row_exchange_order_id(row) for row in live_fills if _row_exchange_order_id(row)}
    evidence: Dict[str, Dict[str, Any]] = {}
    for row in live_fills:
        coin = str(row.get("coin") or "").upper()
        if not coin:
            continue
        info = evidence.setdefault(coin, {"filled_oids": set(), "live_oids": set(), "missing_filled_oids": [], "live_net": 0.0, "live_fill_count": 0, "filled_send_count": 0})
        oid = _row_exchange_order_id(row)
        if oid:
            info["live_oids"].add(oid)
        info["live_net"] = fnum(info.get("live_net")) + _signed_from_side_size(row.get("side"), row.get("fill_size") or row.get("copy_size"))
        info["live_fill_count"] = int(info.get("live_fill_count") or 0) + 1
    for row in send_attempts:
        if str(row.get("status") or "").upper() != "ORDER_FILLED":
            continue
        coin = str(row.get("coin") or "").upper()
        if not coin:
            continue
        info = evidence.setdefault(coin, {"filled_oids": set(), "live_oids": set(), "missing_filled_oids": [], "live_net": 0.0, "live_fill_count": 0, "filled_send_count": 0})
        info["filled_send_count"] = int(info.get("filled_send_count") or 0) + 1
        oid = _row_exchange_order_id(row)
        if oid:
            info["filled_oids"].add(oid)
        intent_id = str(row.get("intent_id") or "")
        if (oid and oid in live_oids) or (intent_id and intent_id in live_intents):
            continue
        if oid:
            info["missing_filled_oids"].append(oid)
    for info in evidence.values():
        info["filled_oids"] = sorted(info.get("filled_oids") or [])
        info["live_oids"] = sorted(info.get("live_oids") or [])
        info["missing_filled_oids"] = sorted(set(info.get("missing_filled_oids") or []))
    return evidence


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
    if RATE_GUARD is not None and not RATE_GUARD.acquire("userFillsByTime", timeout_s=5.0):
        # None means "not fetched", which every caller already handles as
        # UNAVAILABLE.  An analysis page showing "not available right now" is
        # correct; an analysis page that costs another product its execution
        # headroom to render is not.
        return None
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


_REALIZED_PNL_CACHE: Dict[str, Dict[str, Any]] = {}
_REALIZED_PNL_CACHE_LOCK = threading.Lock()


def _fetch_user_realized_pnl_snapshot(account: Optional[str] = None, baseline_timestamp: str = "") -> Dict[str, Any]:
    """Realized PnL over days, cached for the operator refresh interval.

    The uncached body issues a 20-weight userFillsByTime over a multi-day
    window.  It used to run on every model build, and the model is rebuilt on a
    0-30 second cache, so simply leaving the dashboard open put a recurring
    several-hundred-weight-per-minute load on a budget shared with a live
    execution path.  Realized PnL measured over days does not move meaningfully
    inside five minutes, so this is freshness the operator was never using.
    """
    cache_key = f"{(account or _public_account_address()).lower().strip()}|{baseline_timestamp}"
    now_ms = int(time.time() * 1000)
    with _REALIZED_PNL_CACHE_LOCK:
        entry = _REALIZED_PNL_CACHE.get(cache_key)
        if (
            isinstance(entry, dict)
            and entry.get("ok")
            and now_ms - inum(entry.get("cached_at_ms")) <= int(PROOF_ENGINE_REFRESH_SEC * 1000)
        ):
            return dict(entry)
    fresh = _fetch_user_realized_pnl_snapshot_uncached(account, baseline_timestamp)
    if isinstance(fresh, dict) and fresh.get("ok"):
        stored = dict(fresh)
        stored["cached_at_ms"] = now_ms
        with _REALIZED_PNL_CACHE_LOCK:
            _REALIZED_PNL_CACHE[cache_key] = stored
        return dict(stored)
    return fresh


def _fetch_user_realized_pnl_snapshot_uncached(account: Optional[str] = None, baseline_timestamp: str = "") -> Dict[str, Any]:
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
    graph_rows = all_available["rows"] if isinstance(all_available.get("rows"), list) else []
    latest_rows = graph_rows[-250:]
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
        "actual_user_fills_graph": graph_rows,
        "actual_user_fills_graph_count": len(graph_rows),
        "actual_user_fills_recent_cap": 250,
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
def _fetch_exchange_account_snapshot(max_age_sec: float = PROOF_ENGINE_REFRESH_SEC) -> Dict[str, Any]:
    cached = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE, {})
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
            atomic_write_json(EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE, unavailable)
        except Exception:
            pass
        return unavailable
    if RATE_GUARD is not None and not RATE_GUARD.acquire("clearinghouseState", timeout_s=5.0):
        # Prefer stale truth to no truth, but say which it is.  The dashboard
        # renders an age against this timestamp, so a reader can see for
        # themselves how old the number in front of them actually is.
        if isinstance(cached, dict) and cached.get("ok"):
            stale = dict(cached)
            stale["rate_budget_deferred"] = True
            return stale
        return {
            "ok": False, "available": False, "status": "UNAVAILABLE",
            "reason": "RATE_BUDGET_DEFERRED",
            "updated_at": utc_now_iso(), "fetched_at_ms": now_ms,
        }
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
            if RATE_GUARD is not None and not RATE_GUARD.acquire(
                "spotClearinghouseState", timeout_s=5.0
            ):
                # Handled by this block's own except, which records the reason
                # in spot_parse_status rather than failing the whole snapshot.
                raise RuntimeError("RATE_BUDGET_DEFERRED")
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
        # Supplement position_map with Core's exchange snapshot for positions the REST API misses.
        # The HL clearinghouseState endpoint does not return XYZ synthetic assets; Core tracks them
        # via WebSocket and writes them to exchange_account_snapshot.json. Merging here prevents
        # the App from classifying engine-owned XYZ sleeves as EXTERNAL_FLAT_PENDING_RECONCILIATION.
        try:
            _core_snap = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, {})
            _core_positions = _core_snap.get("positions_by_coin", {})
            if isinstance(_core_positions, dict):
                _core_age_ms = now_ms - inum(_core_snap.get("created_at_ms", 0))
                if 0 < _core_age_ms < 300_000:  # only use Core snapshot when < 5 min old
                    for _c, _csz in _core_positions.items():
                        _cu = str(_c).upper()
                        if _cu not in position_map and abs(fnum(_csz)) > 1e-12:
                            position_map[_cu] = {
                                "coin": _cu,
                                "signed_size": fnum(_csz),
                                "position_value": None,
                                "entry_px": None,
                                "mark_px": None,
                                "unrealized_pnl": None,
                                "margin_used": None,
                                "_source": "core_snapshot_supplement",
                            }
        except Exception:
            pass
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
            "actual_user_fills_graph": realized_snapshot.get("actual_user_fills_graph", []),
            "actual_user_fills_graph_count": realized_snapshot.get("actual_user_fills_graph_count", 0),
            "actual_user_fills_recent_cap": realized_snapshot.get("actual_user_fills_recent_cap", 250),
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
        atomic_write_json(EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE, snapshot)
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
        "clearinghouse_account_value": fnum(exchange_snapshot.get("account_value")) if _exchange_snapshot_available(exchange_snapshot) else None,
        "exchange_unrealized_pnl": fnum(exchange_snapshot.get("unrealized_pnl")) if _exchange_snapshot_available(exchange_snapshot) else None,
        "exchange_realized_pnl_since_baseline": fnum(exchange_snapshot.get("realized_pnl_since_baseline")) if is_present_num(exchange_snapshot.get("realized_pnl_since_baseline")) else None,
        "exchange_realized_pnl_today": fnum(exchange_snapshot.get("realized_pnl_today")) if is_present_num(exchange_snapshot.get("realized_pnl_today")) else None,
        "exchange_realized_pnl_all_available_window": fnum(exchange_snapshot.get("realized_pnl_all_available_window")) if is_present_num(exchange_snapshot.get("realized_pnl_all_available_window")) else None,
        "exchange_realized_pnl_source": exchange_snapshot.get("realized_pnl_since_baseline_source", ""),
        "exchange_realized_pnl_fill_count": inum(exchange_snapshot.get("realized_pnl_fill_count")),
        "exchange_realized_pnl_window_start": exchange_snapshot.get("realized_pnl_window_start", ""),
        "exchange_realized_pnl_window_end": exchange_snapshot.get("realized_pnl_window_end", ""),
        "open_notional": fnum(exchange_snapshot.get("total_notional_position")) if _exchange_snapshot_available(exchange_snapshot) else None,
        "margin_used": fnum(exchange_snapshot.get("margin_used")) if _exchange_snapshot_available(exchange_snapshot) else None,
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


def _build_manual_reconciliation_rows(
    manual_positions: Dict[str, Any],
    exchange_snapshot: Dict[str, Any],
    recent_send_attempts: List[Dict[str, Any]],
    live_config: Optional[Dict[str, Any]] = None,
    integrity_status: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    exchange_positions = exchange_snapshot.get("positions_by_coin", {}) if isinstance(exchange_snapshot.get("positions_by_coin"), dict) else {}
    exchange_available = _exchange_snapshot_available(exchange_snapshot)
    archived_cfg = live_config.get("archived_wallets", {}) if isinstance(live_config, dict) else {}
    archived_wallets = {str(w).lower() for w in archived_cfg} if isinstance(archived_cfg, dict) else set()
    seen_on_exchange: set[str] = set()
    sleeves = _manual_position_sleeves(manual_positions)
    coin_counts, coin_nets = _shared_manual_coin_nets(sleeves)
    shared_coins = {coin for coin, count in coin_counts.items() if count > 1}
    classifications: Dict[str, Dict[str, Any]] = {}
    for coin_upper, owned_net in coin_nets.items():
        ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
        exchange_net = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
        classifications[coin_upper] = _classify_owned_exchange_net(owned_net, exchange_net, exchange_available)
    now_ms = int(time.time() * 1000)
    _snap_ts = inum(exchange_snapshot.get("fetched_at_ms") or exchange_snapshot.get("created_at_ms")) if isinstance(exchange_snapshot, dict) else 0
    exchange_snapshot_age_ms = (now_ms - _snap_ts) if _snap_ts > 0 else _RECON_SNAPSHOT_STALE_MS + 1
    _integ = integrity_status if isinstance(integrity_status, dict) else {}
    _hard_copy = _integ.get("hard_copy_invariant") if isinstance(_integ.get("hard_copy_invariant"), dict) else {}
    _hc_counts = _hard_copy.get("counts") if isinstance(_hard_copy.get("counts"), dict) else {}
    core_blocked = inum(_hc_counts.get("ACTIVE_RED")) > 0 or inum(_hc_counts.get("UNCLASSIFIED")) > 0
    for wallet, coin_upper, pos in sorted(sleeves, key=lambda item: (item[1], item[0])):
        signed = fnum(pos.get("signed_size"))
        if abs(signed) <= 1e-12:
            continue
        ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
        exchange_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
        if ex:
            seen_on_exchange.add(coin_upper)
        if coin_upper in shared_coins:
            _sh_issue = "SHARED_SYMBOL_SLEEVE_TRACKED" if exchange_available else "EXCHANGE_UNAVAILABLE"
            _sh_act = _classify_recon_action_type(_sh_issue, coin_upper, signed, exchange_signed, pos, exchange_snapshot_age_ms, core_blocked, exchange_available)
            rows.append({
                "severity": "OK" if exchange_available else "INFO",
                "wallet": wallet,
                "coin": coin_upper,
                "side": "LONG" if signed > 0 else "SHORT",
                "issue": _sh_issue,
                "manual_signed_size": signed,
                "exchange_signed_size": exchange_signed if exchange_available else "n/a",
                "last_intent_id": pos.get("last_intent_id", ""),
                "last_oid": pos.get("last_oid", ""),
                "last_updated_at": pos.get("last_updated_at", ""),
                "latest_error": "shared symbol: sleeve tracked; exchange is netted at account level",
                "count": 1,
                "action_available": False,
                **_sh_act,
            })
            continue
        if exchange_available:
            cls = classifications.get(coin_upper, _classify_owned_exchange_net(signed, exchange_signed, exchange_available))
            issue = str(cls.get("status") or "EXCHANGE_UNAVAILABLE")
            if issue == "OWNED_FULLY_SUPPORTED":
                severity = "OK"
            elif issue in {"OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE", "EXTERNAL_FLAT_PENDING_RECONCILIATION", "SIGN_CONFLICT_PENDING_RECONCILIATION"}:
                severity = "CRITICAL"
            elif issue == "OWNED_PLUS_ACCOUNT_RESIDUAL":
                severity = "INFO"
            else:
                severity = "INFO"
            exchange_label = exchange_signed
        else:
            severity = "INFO"
            issue = "EXCHANGE_UNAVAILABLE"
            exchange_label = "n/a"
        _act = _classify_recon_action_type(issue, coin_upper, signed, exchange_signed, pos, exchange_snapshot_age_ms, core_blocked, exchange_available)
        rows.append({
            "severity": severity,
            "wallet": wallet,
            "coin": coin_upper,
            "side": "LONG" if signed > 0 else "SHORT",
            "issue": issue,
            "manual_signed_size": signed,
            "exchange_signed_size": exchange_label,
            "residual_size": (classifications.get(coin_upper) or {}).get("residual_size", 0.0),
            "unsupported_size": (classifications.get(coin_upper) or {}).get("unsupported_size", 0.0),
            "last_intent_id": pos.get("last_intent_id", ""),
            "last_oid": pos.get("last_oid", ""),
            "last_updated_at": pos.get("last_updated_at", ""),
            "latest_error": "residual account exposure; not wallet owned" if issue == "OWNED_PLUS_ACCOUNT_RESIDUAL" else (
                "ledger sleeve not fully supported by exchange; external reconciliation required" if severity == "CRITICAL" else ""
            ),
            "count": 1,
            "action_available": False,
            **_act,
        })
    if exchange_available:
        for coin_upper in sorted(shared_coins):
            ledger_net = coin_nets.get(coin_upper, 0.0)
            active_ledger_net = sum(fnum(pos.get("signed_size")) for wallet, coin, pos in sleeves if coin == coin_upper and str(wallet).lower() not in archived_wallets)
            archived_ledger_net = ledger_net - active_ledger_net
            ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
            exchange_signed = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
            diff = ledger_net - exchange_signed
            active_diff = active_ledger_net - exchange_signed
            if ex:
                seen_on_exchange.add(coin_upper)
            if abs(diff) <= 1e-8:
                severity = "OK"
                issue = "SHARED_SYMBOL_NET_MATCH"
            elif abs(archived_ledger_net) > 1e-8 and abs(active_diff) <= 1e-8:
                severity = "INFO"
                issue = f"ARCHIVED_LEDGER_RESIDUAL_ACCOUNTED: {archived_ledger_net:+.6f}"
            elif exchange_signed == 0.0:
                severity = "CRITICAL"
                issue = "MISSING_EXCHANGE"
            else:
                severity = "CRITICAL"
                issue = f"SHARED_SYMBOL_NET_DIFF: {diff:+.6f}"
            _agg_act = _classify_recon_action_type(issue, coin_upper, round(ledger_net, 12), exchange_signed, {}, exchange_snapshot_age_ms, core_blocked, exchange_available)
            rows.append({
                "severity": severity,
                "wallet": "aggregate",
                "coin": coin_upper,
                "side": "NET",
                "issue": issue,
                "manual_signed_size": round(ledger_net, 12),
                "exchange_signed_size": exchange_signed,
                "active_manual_signed_size": round(active_ledger_net, 12),
                "archived_manual_signed_size": round(archived_ledger_net, 12),
                "last_intent_id": "aggregate",
                "last_oid": "aggregate",
                "last_updated_at": "—",
                "latest_error": "aggregate net matches exchange" if severity == "OK" else (
                    "active ledger net matches exchange; archived ledger residual preserved for audit history" if issue.startswith("ARCHIVED_LEDGER_RESIDUAL_ACCOUNTED") else "aggregate ledger net differs from exchange net"
                ),
                "count": coin_counts.get(coin_upper, 0),
                "action_available": False,
                **_agg_act,
            })
    if exchange_available:
        for coin, ex in sorted(exchange_positions.items()):
            if coin not in seen_on_exchange:
                _ml_exsz = fnum(ex.get("signed_size"))
                _ml_act = _classify_recon_action_type("MISSING_LEDGER", str(coin).upper(), 0.0, _ml_exsz, {}, exchange_snapshot_age_ms, core_blocked, exchange_available)
                rows.append({
                    "severity": "CRITICAL",
                    "wallet": "unknown",
                    "coin": str(coin).upper(),
                    "issue": "MISSING_LEDGER",
                    "manual_signed_size": 0.0,
                    "exchange_signed_size": _ml_exsz,
                    "last_intent_id": "—",
                    "last_oid": "—",
                    "last_updated_at": "—",
                    "count": 1,
                    "action_available": False,
                    **_ml_act,
                })
    if isinstance(live_config, dict):
        wallets_cfg = live_config.get("wallets") or {}
        if isinstance(wallets_cfg, dict):
            wallets_with_open = {str(w).lower() for w, _, _ in sleeves}
            exit_recovery_wallets: set = set()
            for _r in (_hard_copy.get("active_red_rows") or []) if isinstance(_hard_copy, dict) else []:
                if isinstance(_r, dict) and str(_r.get("classification") or "") in {
                    "EXIT_RECOVERY_ACTIVE", "MANUAL_EXIT_RECOVERY_REQUIRED"
                }:
                    _rw = str(_r.get("leader_wallet") or "").lower()
                    if _rw:
                        exit_recovery_wallets.add(_rw)
            for w_addr, w_cfg in sorted(wallets_cfg.items()):
                if not isinstance(w_cfg, dict):
                    continue
                w_mode = str(w_cfg.get("mode") or "").upper()
                if w_mode not in {"CLO", "OFF"}:
                    continue
                w_lower = str(w_addr).lower()
                if w_lower in wallets_with_open:
                    continue
                if w_lower in exit_recovery_wallets:
                    continue
                rows.append({
                    "severity": "INFO",
                    "wallet": w_addr,
                    "coin": "—",
                    "side": "—",
                    "issue": f"WALLET_{w_mode}_NO_OPEN_EXPOSURE",
                    "manual_signed_size": 0.0,
                    "exchange_signed_size": 0.0,
                    "last_intent_id": "",
                    "last_oid": "",
                    "last_updated_at": "",
                    "latest_error": "",
                    "count": 1,
                    "action_type": "ARCHIVE_READY",
                    "action_label": "ARCHIVE_READY",
                    "action_blocked_reason": "",
                    "operator_summary": f"Wallet is {w_mode} with no open ledger exposure — safe to archive via config",
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
    # DISABLED: manual ledger mutation disabled — Core is sole writer of manual_live_positions.json.
    # Direct App writes to the ledger break the SSOT contract: they bypass ManualLedger.apply_copy_fill(),
    # produce no entry in order_intents.csv / live_fills.csv, and are invisible to build_live_integrity_status().
    return {"ok": False, "error": "DISABLED: manual ledger mutation disabled — Core is sole writer of manual_live_positions.json"}


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
    live_wallet_derived: Optional[Dict[str, Any]] = None,
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
                _last_lf_by_wallet[_lfw] = _normalize_last_fill_for_ui(_lf)
    # Pre-count ORDER_FILLED send_attempts per wallet
    _filled_cnt_by_wallet: Dict[str, int] = {}
    _reject_cnt_by_wallet: Dict[str, int] = {}
    _last_send_by_wallet: Dict[str, Dict[str, Any]] = {}
    _last_terminal_by_wallet: Dict[str, str] = {}
    for _sa in recent_send_attempts:
        _saw = str(_sa.get("leader_wallet") or _sa.get("auto_send_wallet") or "").lower()
        if _saw and str(_sa.get("status", "")) == "ORDER_FILLED":
            _filled_cnt_by_wallet[_saw] = _filled_cnt_by_wallet.get(_saw, 0) + 1
        if _saw:
            _last_send_by_wallet[_saw] = _sa
            if str(_sa.get("status", "")).upper() == "ORDER_REJECTED":
                _reject_cnt_by_wallet[_saw] = _reject_cnt_by_wallet.get(_saw, 0) + 1
            if _sa.get("terminal_state"):
                _last_terminal_by_wallet[_saw] = str(_sa.get("terminal_state"))
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
        derived = (live_wallet_derived or {}).get(w, {}) if isinstance(live_wallet_derived, dict) else {}

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
        effective_wallet_action = "COPYING" if mode == "LIVE" and service_eligible else ("CLOSE_ONLY" if mode == "CLO" and service_eligible else "WALLET_OFF")

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
            if _shared_ws_ok and mode in {"LIVE", "CLO"} and raw_status in {"UNKNOWN", ""}:
                conn_status = "SHARED_WS_OK"
                conn_detail = "HOT10 shared socket active"
            elif raw_status in {"OFFLINE", "DISCONNECTED", "CLOSED"}:
                conn_status = "OFFLINE"
                conn_detail = wh.get("current_health_grade") or wh.get("health_grade") or wh.get("last_error") or "n/a"
            else:
                conn_status = raw_status
                conn_detail = wh.get("current_health_grade") or wh.get("health_grade") or wh.get("last_error") or "n/a"

        open_positions = perf.get("current_open_positions", [])
        open_exposure = perf.get("current_exposure", 0.0)
        if fnum(open_exposure) > 0:
            _w_exposure = fnum(open_exposure)
        last_fill_d = perf.get("last_actual_fill", {})
        _account_only = (
            str(perf.get("pnl_status", "")).upper() in {"ACCOUNT_LEVEL_ONLY", "AMBIGUOUS_COIN_SHARED"}
            or str(perf.get("attribution_quality", "")).upper() == "WALLET_REALIZED_REQUIRES_EXACT_EXCHANGE_FILL_ID"
        )
        _pnl_reason = str(perf.get("pnl_not_proven_reason") or perf.get("pnl_status_label") or "")
        if _account_only and not _pnl_reason:
            _pnl_reason = "wallet PnL cannot be safely attributed from account-level exchange data"
        _exposure_source = "manual ledger sleeves using mark price when available" if _w_open_coins else "no open manual sleeve"
        _latest_leader_equity_base = _current_leader_equity_from_cache(w)
        _effective_leader_equity_base = max(1.0, fnum(
            _latest_leader_equity_base,
            fnum(perf.get("leader_equity_base"), fnum(cfg.get("leader_equity_base"), DEFAULT_LEADER_EQUITY)),
        ))
        _true_ts_summary = _true_ts_drawdown_summary(w)
        _eff_dd = get_effective_max_dd(perf, perf.get("max_drawdown"), perf.get("allTime_acctV_peak") or _effective_leader_equity_base)
        model_realized = derived.get("copy_realized")
        model_unrealized = derived.get("copy_unrealized")
        model_has_pnl = model_realized is not None or model_unrealized is not None
        model_net = round(fnum(model_realized) + fnum(model_unrealized), 8) if model_has_pnl else None

        rows.append({
            "wallet": wallet,
            "mode": mode,
            "eligibility": eligibility,
            "effective_wallet_action": effective_wallet_action,
            "enabled": bool(normal.get("enabled")),
            "service_eligible": service_eligible,
            "service_eligibility_reason": service_reason,
            "copy_mode": str(cfg.get("copy_mode", "proportional")),
            "fixed_notional": cfg.get("fixed_notional"),
            "norm_base": cfg.get("norm_base"),
            "leader_equity_base": _effective_leader_equity_base,
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
            "rejected_count": _reject_cnt_by_wallet.get(w, perf.get("exchange_rejected_count", 0)),
            "last_send": _last_send_by_wallet.get(w, {}),
            "last_terminal_state": _last_terminal_by_wallet.get(w, ""),
            "ws_subscription": "SUBSCRIBED" if (in_health_file or (_shared_ws_ok and mode in {"LIVE", "CLO"})) else "NOT_SUBSCRIBED",
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
            "realized_pnl": model_realized if model_has_pnl else perf.get("realized_pnl_estimate"),
            "unrealized_pnl": model_unrealized if model_has_pnl else (None if _account_only else perf.get("unrealized_pnl_estimate")),
            "net_pnl": model_net if model_has_pnl else (None if _account_only else perf.get("net_pnl_estimate")),
            "model_realized_pnl": model_realized,
            "model_unrealized_pnl": model_unrealized,
            "model_net_pnl": model_net,
            "model_pnl_source": "app_model_replay_from_live_config" if model_has_pnl else "",
            "exchange_realized_pnl": perf.get("realized_pnl_estimate"),
            "exchange_unrealized_pnl": None if _account_only else perf.get("unrealized_pnl_estimate"),
            "exchange_net_pnl": None if _account_only else perf.get("net_pnl_estimate"),
            "live_realized_pnl": perf.get("live_realized_pnl"),
            "confirmed_realized_pnl": perf.get("confirmed_realized_pnl"),
            "realized_match_status": perf.get("realized_match_status", "N/A"),
            "matched_exchange_fill_count": perf.get("matched_exchange_fill_count", 0),
            "matched_closed_pnl_fill_count": perf.get("matched_closed_pnl_fill_count", 0),
            "live_unrealized_pnl": None if _account_only else perf.get("live_unrealized_pnl"),
            "live_net_pnl": None if _account_only else perf.get("live_net_pnl"),
            "live_equity_effect": None if _account_only else perf.get("live_equity_effect"),
            "pnl_status": "APP_MODEL_REPLAY" if model_has_pnl else perf.get("pnl_status", "N/A"),
            "pnl_status_label": "Model replay from live config" if model_has_pnl else perf.get("pnl_status_label", "No live PnL"),
            "pnl_not_proven_reason": perf.get("pnl_not_proven_reason", ""),
            "pnl_display_reason": _pnl_reason,
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
            "open_position_count": len(_w_open_coins),
            "open_positions": _w_open_coins,
            "open_coins": _w_open_coins,
            "open_exposure": _w_exposure,
            "current_exposure": _w_exposure,
            "exposure_source": _exposure_source,
            "max_exposure": perf.get("max_exposure"),
            "drawdown": None if _account_only else perf.get("drawdown"),
            "live_dd": None if _account_only else perf.get("drawdown"),
            "max_drawdown": _eff_dd.get("value_usd"),
            "lead_maxdd_effective_usd": _eff_dd.get("value_usd"),
            "lead_maxdd_effective_pct": _eff_dd.get("value_pct"),
            "lead_maxdd_effective_source": _eff_dd.get("source"),
            "copy_maxdd_effective_usd": _eff_dd.get("value_usd"),
            "copy_maxdd_effective_pct": _eff_dd.get("value_pct"),
            "copy_maxdd_effective_source": _eff_dd.get("source"),
            "realised_maxdd_diagnostic_usd": perf.get("max_drawdown"),
            "max_drawdown_realised": perf.get("max_drawdown_realised", perf.get("max_drawdown")),
            "max_drawdown_mtm": perf.get("max_drawdown_mtm"),
            "allTime_max_drawdown_mtm": perf.get("allTime_max_drawdown_mtm"),
            "allTime_acctV_peak": perf.get("allTime_acctV_peak"),
            "month_acctV_peak": perf.get("month_acctV_peak"),
            "leader_equity_base_source": "8012_accountValueHistory_cache" if _latest_leader_equity_base is not None else ("config" if cfg.get("leader_equity_base") is not None else "default"),
            "mtm_source": perf.get("mtm_source"),
            "true_ts_equity_series": perf.get("true_ts_equity_series", []),
            "true_ts_drawdown_series": perf.get("true_ts_drawdown_series", []),
            "true_ts_max_dd_usd": _true_ts_summary.get("true_ts_max_dd_usd"),
            "true_ts_max_dd_pct": abs(fnum(_true_ts_summary.get("true_ts_max_dd_pct"))) if _true_ts_summary.get("true_ts_max_dd_pct") is not None else None,
            "true_ts_points": _true_ts_summary.get("true_ts_points"),
            "true_ts_source": _true_ts_summary.get("true_ts_source"),
            "last_fill": _w_last_fill if _w_last_fill else last_fill_d,
        })
    return rows


def _normalize_last_fill_for_ui(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict) or not row:
        return {}
    avg_px = row.get("avg_px")
    if avg_px in (None, ""):
        avg_px = row.get("fill_price")
    if avg_px in (None, ""):
        avg_px = row.get("fill_avg_px")
    size = row.get("size")
    if size in (None, ""):
        size = row.get("fill_size")
    fill_time = row.get("time") or row.get("created_at") or row.get("created_at_ms")
    return {
        "coin": row.get("coin", ""),
        "side": row.get("side") or row.get("actual_side") or "",
        "size": size if size is not None else "",
        "avg_px": avg_px if avg_px is not None else "",
        "notional": row.get("notional") if row.get("notional") not in (None, "") else row.get("fill_notional", ""),
        "oid": row.get("oid") or row.get("exchange_order_id") or "",
        "time": fill_time or "",
    }


def _build_real_copy_positions(
    manual_positions: Dict[str, Any],
    exchange_snapshot: Dict[str, Any],
    service_evidence: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    exchange_positions = exchange_snapshot.get("positions_by_coin", {}) if isinstance(exchange_snapshot.get("positions_by_coin"), dict) else {}
    exchange_available = _exchange_snapshot_available(exchange_snapshot)
    # service_evidence is only used to classify orphan / service-created rows; owned
    # sleeve rows (size/side/value) never depend on it. Callers on the fast stale-serve
    # path inject {} to skip the heavy append-only CSV scan.
    if service_evidence is None:
        service_evidence = _service_position_evidence_by_coin()
    seen: set = set()
    sleeves = _manual_position_sleeves(manual_positions)
    coin_counts, coin_nets = _shared_manual_coin_nets(sleeves)
    shared_coins = {coin for coin, count in coin_counts.items() if count > 1}
    classifications: Dict[str, Dict[str, Any]] = {}
    all_classification_coins = {str(c).upper() for c in coin_nets}
    all_classification_coins.update(str(c).upper() for c in exchange_positions if isinstance(exchange_positions, dict))
    for coin_upper in sorted(all_classification_coins):
        owned_net = fnum(coin_nets.get(coin_upper))
        ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
        exchange_net = _exchange_signed_size(ex)
        base = _classify_owned_exchange_net(owned_net, exchange_net, exchange_available)
        evidence = service_evidence.get(coin_upper, {})
        missing_oids = list(evidence.get("missing_filled_oids") or [])
        live_net = fnum(evidence.get("live_net"))
        if exchange_available and abs(exchange_net) > 1e-12 and abs(owned_net) <= 1e-12:
            if missing_oids or abs(live_net) > 1e-12:
                base.update({
                    "status": "SERVICE_CREATED_UNLEDGERED",
                    "service_live_net": live_net,
                    "missing_filled_oids": missing_oids,
                })
            else:
                base.update({"status": "EXTERNAL_ORPHAN_UNOWNED"})
        elif exchange_available and abs(owned_net) > 1e-12 and base.get("status") == "OWNED_PLUS_ACCOUNT_RESIDUAL":
            base.update({"status": "RESIDUAL_ACCOUNT_LEVEL", "service_live_net": live_net})
        classifications[coin_upper] = base
    for wallet, coin_upper, pos in sorted(sleeves, key=lambda item: (item[1], item[0])):
        signed = fnum(pos.get("signed_size"))
        if abs(signed) <= 1e-12:
            continue
        seen.add(coin_upper)
        ex: Dict[str, Any] = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
        ex_signed = _exchange_signed_size(ex)
        ex_mark = _exchange_field(ex, "mark_px")
        ex_entry = _exchange_field(ex, "entry_px")
        avg_entry = fnum(pos.get("avg_entry_px")) or ex_entry
        # Value source: prefer the live exchange mark. For price-less assets (XYZ
        # synthetics that HL REST omits and Core supplements size-only), fall back to
        # manual avg entry as an EXPLICIT ESTIMATE so value is not silently null.
        # mark_px is left None — the entry estimate is never presented as a live mark.
        if ex_mark > 0:
            owned_value_mark = ex_mark
            owned_value_source = "LIVE_EXCHANGE_MARK"
        elif avg_entry > 0:
            owned_value_mark = avg_entry
            owned_value_source = "ENTRY_ESTIMATE_NO_LIVE_MARK"
        else:
            owned_value_mark = 0.0
            owned_value_source = "NO_PRICE_AVAILABLE"
        owned_position_value = abs(signed) * owned_value_mark if owned_value_mark > 0 else None
        owned_unrealized = _owned_sleeve_unrealized(signed, avg_entry, ex_mark)
        is_shared = coin_upper in shared_coins
        if is_shared and exchange_available:
            status = "SHARED_SYMBOL_SLEEVE_TRACKED"
        elif exchange_available:
            status = str((classifications.get(coin_upper) or {}).get("status") or "EXCHANGE_UNAVAILABLE")
        else:
            status = "EXCHANGE_UNAVAILABLE"
        unsafe_external_status = status in {"OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE", "EXTERNAL_FLAT_PENDING_RECONCILIATION", "SIGN_CONFLICT_PENDING_RECONCILIATION"}
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
            "entry_px": avg_entry if avg_entry > 0 else None,
            "mark_px": ex_mark if ex_mark > 0 else None,
            "position_value": None if is_shared else (round(owned_position_value, 8) if owned_position_value is not None else None),
            "value_source": "SHARED_SYMBOL_NETTED" if is_shared else owned_value_source,
            "value_is_estimate": (not is_shared) and owned_value_source == "ENTRY_ESTIMATE_NO_LIVE_MARK",
            "unrealized_pnl": None if is_shared or unsafe_external_status else owned_unrealized,
            "ledger_vs_exchange": status,
            "residual_size": (classifications.get(coin_upper) or {}).get("residual_size", 0.0),
            "unsupported_size": (classifications.get(coin_upper) or {}).get("unsupported_size", 0.0),
            "reconciliation_note": "shared symbol: sleeve tracked; exchange is netted at account level" if is_shared else (
                "wallet owns manual sleeve only; exchange residual is account-level context" if status in {"OWNED_PLUS_ACCOUNT_RESIDUAL", "RESIDUAL_ACCOUNT_LEVEL"}
                else "ledger sleeve not fully supported by exchange; external reconciliation required" if status in {"OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE", "EXTERNAL_FLAT_PENDING_RECONCILIATION", "SIGN_CONFLICT_PENDING_RECONCILIATION"}
                else ""
            ),
        })
    if exchange_available:
        _res_eps = 1e-8
        for coin_upper, info in sorted(classifications.items()):
            owned_net_v = fnum(info.get("owned_net"))
            exchange_net_raw = info.get("exchange_net")
            if exchange_net_raw is None:
                continue
            exchange_net_v = fnum(exchange_net_raw)
            # emit residual row only when: both sides nonzero, same sign, exchange larger than owned
            if not (
                abs(owned_net_v) > _res_eps
                and abs(exchange_net_v) > _res_eps
                and owned_net_v * exchange_net_v > 0
                and abs(exchange_net_v) > abs(owned_net_v) + _res_eps
                and abs(exchange_net_v - owned_net_v) > _res_eps
            ):
                continue
            residual = exchange_net_v - owned_net_v
            ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
            ex_mark = _exchange_field(ex, "mark_px")
            rows.append({
                "row_type": "ACCOUNT_LEVEL_ONLY",
                "provenance": "ACCOUNT_LEVEL_RESIDUAL",
                "coin": coin_upper,
                "signed_size": None,
                "side": "LONG" if residual > 0 else "SHORT",
                "leader_wallet": "—",
                "avg_entry_px": None,
                "last_copy_fill_id": "",
                "last_updated_ms": None,
                "last_intent_id": "—",
                "last_oid": "—",
                "last_updated_at": "—",
                "exchange_signed_size": residual,
                "entry_px": _exchange_field(ex, "entry_px") or None,
                "mark_px": ex_mark if ex_mark > 0 else None,
                "position_value": round(abs(residual) * ex_mark, 8) if ex_mark > 0 else None,
                "unrealized_pnl": None,
                "ledger_vs_exchange": "ACCOUNT_RESIDUAL",
                "orphan_classification": "RESIDUAL_ACCOUNT_LEVEL",
                "residual_size": residual,
                "unsupported_size": 0.0,
                "engine_can_close": False,
                "engine_can_use_as_sleeve": False,
                "use_as_sleeve": False,
                "user_owner_status": "USER_MANAGED",
                "context": "account-level residual only; owned sleeves remain valid",
                "reconciliation_note": (
                    f"account-level residual only; manual_net={owned_net_v}; "
                    f"exchange_net={exchange_net_v}; residual={residual}; "
                    "engine must not adopt or close residual"
                ),
            })
    if exchange_available:
        for coin_upper, info in sorted(classifications.items()):
            status = str(info.get("status") or "")
            if status not in {"SIGN_CONFLICT_PENDING_RECONCILIATION", "EXTERNAL_FLAT_PENDING_RECONCILIATION", "OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE"}:
                continue
            # shared coins get exactly one diagnostic row from the shared block below; skip here to prevent duplication
            if coin_upper in shared_coins:
                continue
            ex = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
            ex_signed = _exchange_signed_size(ex)
            manual_signed = fnum(info.get("owned_net"))
            if abs(ex_signed) <= 1e-12 and abs(manual_signed) <= 1e-12:
                continue
            ex_mark = _exchange_field(ex, "mark_px")
            rows.append({
                "row_type": "ACCOUNT_LEVEL_ONLY",
                "provenance": "ACCOUNT_LEVEL_ONLY",
                "coin": coin_upper,
                "signed_size": None,
                "manual_signed_size": manual_signed,
                "side": "LONG" if ex_signed > 0 else "SHORT" if ex_signed < 0 else "FLAT",
                "leader_wallet": "—",
                "avg_entry_px": None,
                "last_copy_fill_id": "",
                "last_updated_ms": None,
                "last_intent_id": "—",
                "last_oid": "—",
                "last_updated_at": "—",
                "exchange_signed_size": ex_signed,
                "entry_px": _exchange_field(ex, "entry_px") or None,
                "mark_px": ex_mark if ex_mark > 0 else None,
                "position_value": _exchange_field(ex, "position_value") or (round(abs(ex_signed) * ex_mark, 8) if ex_mark > 0 else None),
                "unrealized_pnl": None,
                "ledger_vs_exchange": status,
                "orphan_classification": status,
                "residual_size": fnum(info.get("residual_size")),
                "unsupported_size": fnum(info.get("unsupported_size")),
                "engine_can_close": False,
                "engine_can_use_as_sleeve": False,
                "use_as_sleeve": False,
                "user_owner_status": "USER_MANAGED",
                "context": "exchange net/residual conflict context; not ledger adoption",
                "reconciliation_note": f"account-level conflict context only; manual_signed_size={manual_signed}; exchange_signed_size={ex_signed}; engine must not close or adopt",
            })
    if exchange_available:
        for coin_upper in sorted(shared_coins):
            ledger_net = coin_nets.get(coin_upper, 0.0)
            ex: Dict[str, Any] = exchange_positions.get(coin_upper, {}) if isinstance(exchange_positions, dict) else {}
            ex_signed = _exchange_signed_size(ex)
            diff = ledger_net - ex_signed
            # clean aggregate match: sleeve rows already represent this correctly as OWNED_COPY; no account-level row needed
            if abs(diff) <= 1e-8:
                continue
            status = f"ACCOUNT_LEVEL_ONLY / SHARED_SYMBOL_RESIDUAL {diff:+.6f}"
            rows.append({
                "row_type": "ACCOUNT_LEVEL_ONLY",
                "provenance": "ACCOUNT_LEVEL_ONLY",
                "coin": coin_upper,
                "signed_size": round(ledger_net, 12),
                "side": "NET LONG" if ledger_net > 0 else "NET SHORT" if ledger_net < 0 else "NET FLAT",
                "leader_wallet": "aggregate",
                "last_intent_id": "aggregate",
                "last_oid": "aggregate",
                "last_updated_at": "—",
                "exchange_signed_size": ex_signed,
                "entry_px": _exchange_field(ex, "entry_px") or None,
                "mark_px": _exchange_field(ex, "mark_px") or None,
                "position_value": _exchange_field(ex, "position_value") or None,
                "unrealized_pnl": None,
                "ledger_vs_exchange": status,
                "orphan_classification": "SHARED_SYMBOL_RESIDUAL",
                "residual_size": diff,
                "unsupported_size": 0.0,
                "reconciliation_note": "aggregate shared-symbol residual; account-level only, never wallet-owned",
            })
    if exchange_available:
        for coin, ex in sorted(exchange_positions.items()):
            coin_upper = str(coin).upper()
            if coin_upper in seen:
                continue
            ex_signed = _exchange_signed_size(ex)
            if abs(ex_signed) <= 1e-12:
                continue
            classification = classifications.get(coin_upper, {})
            orphan_status = str(classification.get("status") or "EXTERNAL_ORPHAN_UNOWNED")
            if orphan_status not in {"SERVICE_CREATED_UNLEDGERED", "EXTERNAL_ORPHAN_UNOWNED"}:
                orphan_status = "EXTERNAL_ORPHAN_UNOWNED"
            rows.append({
                "row_type": "ACCOUNT_LEVEL_ONLY",
                "coin": coin_upper,
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
                "entry_px": _exchange_field(ex, "entry_px") or None,
                "mark_px": _exchange_field(ex, "mark_px") or None,
                "position_value": _exchange_field(ex, "position_value") or None,
                "unrealized_pnl": _exchange_field(ex, "unrealized_pnl") or None,
                "ledger_vs_exchange": "SERVICE_CREATED_UNLEDGERED" if orphan_status == "SERVICE_CREATED_UNLEDGERED" else "ORPHAN_EXCHANGE",
                "orphan_classification": orphan_status,
                "service_live_net": classification.get("service_live_net", 0.0),
                "missing_filled_oids": classification.get("missing_filled_oids", []),
                "reconciliation_note": "service-created exchange position is not represented in manual ledger" if orphan_status == "SERVICE_CREATED_UNLEDGERED" else "NO SERVICE ORDER EVIDENCE; not adopted into ledger",
            })
    return rows


def _orphan_registry_key(row: Dict[str, Any]) -> str:
    coin = str(row.get("coin") or "").upper().strip()
    classification = str(row.get("orphan_classification") or row.get("ledger_vs_exchange") or "ACCOUNT_LEVEL_ONLY").upper()
    return f"{classification}::{coin}"


def _account_orphan_signed_size(row: Dict[str, Any]) -> float:
    classification = str(row.get("orphan_classification") or "")
    if classification == "SHARED_SYMBOL_RESIDUAL":
        return -fnum(row.get("residual_size"))
    return fnum(row.get("exchange_signed_size"))


def _build_account_orphan_registry(
    account_rows: List[Dict[str, Any]],
    registry_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Persist account-level exchange exposure as user-managed context only."""
    registry_path = registry_path or ACCOUNT_ORPHAN_POSITIONS_FILE
    now = utc_now_iso()
    prior = load_json(registry_path, {})
    prior_positions = prior.get("positions") if isinstance(prior, dict) and isinstance(prior.get("positions"), dict) else {}
    positions: Dict[str, Dict[str, Any]] = {str(k): dict(v) for k, v in prior_positions.items() if isinstance(v, dict)}
    active_keys: set[str] = set()

    for row in account_rows:
        if row.get("row_type") != "ACCOUNT_LEVEL_ONLY":
            continue
        coin = str(row.get("coin") or "").upper().strip()
        if not coin:
            continue
        signed = _account_orphan_signed_size(row)
        if abs(signed) <= 1e-12:
            continue
        key = _orphan_registry_key(row)
        active_keys.add(key)
        existing = positions.get(key, {})
        mark_px = fnum(row.get("mark_px"))
        entry_px = fnum(row.get("entry_px"))
        notional = fnum(row.get("position_value"))
        if notional <= 0 and mark_px > 0:
            notional = abs(signed) * mark_px
        raw_class = str(row.get("orphan_classification") or row.get("ledger_vs_exchange") or "ACCOUNT_RESIDUAL")
        classification = "ACCOUNT_LEVEL_ONLY / EXTERNAL_ORPHAN_UNOWNED" if raw_class == "EXTERNAL_ORPHAN_UNOWNED" else f"ACCOUNT_LEVEL_ONLY / {raw_class}"
        positions[key] = {
            "coin": coin,
            "signed_size": signed,
            "side": "LONG" if signed > 0 else "SHORT",
            "entry_px": entry_px if entry_px > 0 else None,
            "mark_px": mark_px if mark_px > 0 else None,
            "unrealized_pnl": row.get("unrealized_pnl"),
            "notional": round(notional, 8) if notional else None,
            "exposure": round(abs(notional), 8) if notional else None,
            "first_seen_at": existing.get("first_seen_at") or now,
            "last_seen_at": now,
            "source": "exchange_account_snapshot",
            "classification": classification,
            "user_owner_status": "USER_MANAGED",
            "engine_can_close": False,
            "engine_can_use_as_sleeve": False,
            "status": "ACTIVE",
            "resolved_at": None,
            "notes": row.get("reconciliation_note") or "account-level exchange exposure; not engine owned",
        }

    for key, entry in list(positions.items()):
        if key in active_keys:
            continue
        if str(entry.get("status") or "ACTIVE") == "ACTIVE":
            entry["status"] = "RESOLVED / NO_LONGER_ON_EXCHANGE"
            entry["resolved_at"] = now
            entry["last_seen_at"] = entry.get("last_seen_at") or now
            entry["engine_can_close"] = False
            entry["engine_can_use_as_sleeve"] = False
            positions[key] = entry

    payload = {
        "schema": "account_orphan_positions.v1",
        "updated_at": now,
        "source": "exchange_account_snapshot",
        "positions": positions,
        "active_count": sum(1 for p in positions.values() if str(p.get("status") or "") == "ACTIVE"),
        "resolved_count": sum(1 for p in positions.values() if str(p.get("status") or "").startswith("RESOLVED")),
    }
    try:
        atomic_write_json(registry_path, payload)
    except Exception:
        pass
    return payload


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
    now_ms = int(time.time() * 1000)
    active_window_ms = 60 * 60 * 1000
    for attempt in reversed(recent_send_attempts):
        intent_id = str(attempt.get("intent_id", "")).strip()
        leader_fill_id = str(attempt.get("leader_fill_id", "")).strip()
        intent = intent_by_id.get(intent_id, {})
        # Resolve live_fill: intent_id first, then leader_fill_id
        live_fill = _lf_by_intent.get(intent_id) or _lf_by_leader_fill.get(leader_fill_id) or {}
        has_live_fill = bool(live_fill)
        status = str(attempt.get("status", "") or "")
        terminal_state = str(attempt.get("terminal_state", "") or "")
        operator_action = str(attempt.get("operator_action", "") or "")
        reject_category = str(attempt.get("reject_category", "") or "")
        created_ms = fnum(attempt.get("created_at_ms")) or _iso_to_ms(attempt.get("created_at"))
        age_ms = max(0, now_ms - int(created_ms)) if created_ms else 0
        is_fresh = bool(created_ms and age_ms <= active_window_ms)
        display_terminal_state = terminal_state
        display_operator_action = operator_action
        truth_state = "HISTORICAL"
        truth_severity = "INFO"
        _is_close_recovery = terminal_state.startswith("CLOSE_RECOVERY_NOT_IMPLEMENTED")
        _is_unresolved_close_recovery = _is_close_recovery and not (
            operator_action and operator_action not in {
                "CLOSE_RECOVERY_NOT_IMPLEMENTED", "CLOSE_RECOVERY_NOT_IMPLEMENTED_IOC_NO_MATCH"
            }
        )
        if status == "ORDER_FILLED" and has_live_fill:
            display_terminal_state = "ADOPTED_RECONCILED"
            display_operator_action = "COPY_POLL_ADOPTED"
            truth_state = "ADOPTED_RECONCILED"
            truth_severity = "GREEN"
        elif status == "ORDER_FILLED":
            truth_state = "ACTIVE_AWAITING_COPY_POLL" if is_fresh else "HISTORICAL_UNADOPTED_REVIEW"
            truth_severity = "AMBER" if is_fresh else "RED"
        elif status == "ORDER_REJECTED":
            if _is_unresolved_close_recovery:
                # Close that failed IOC with no resolution stays active AMBER until explicitly reconciled
                truth_state = "ACTIVE_CLOSE_RECOVERY_REQUIRED"
                truth_severity = "AMBER"
                if not display_operator_action or display_operator_action.startswith("CLOSE_RECOVERY_NOT_IMPLEMENTED"):
                    display_operator_action = "MANUAL_CLOSE_OR_RECONCILE_REQUIRED"
            else:
                truth_state = "ACTIVE_EXCHANGE_REJECT" if is_fresh else "HISTORICAL_EXCHANGE_REJECT"
                truth_severity = "RED" if is_fresh else "HISTORICAL"
        elif status == "CONFIRM_REQUIRED":
            truth_state = "QUEUED_PREVIEW"
            truth_severity = "INFO"
        elif status:
            if _is_unresolved_close_recovery:
                truth_state = "ACTIVE_CLOSE_RECOVERY_REQUIRED"
                truth_severity = "AMBER"
                if not display_operator_action or display_operator_action.startswith("CLOSE_RECOVERY_NOT_IMPLEMENTED"):
                    display_operator_action = "MANUAL_CLOSE_OR_RECONCILE_REQUIRED"
            else:
                # Spot/internal-index fills are valid no-action skips, not AMBER blocks.
                _eq_coin = str(attempt.get("coin") or "").strip()
                _eq_notes = str(attempt.get("notes") or attempt.get("error") or "")
                if _is_spot_skip_row(_eq_coin, terminal_state, _eq_notes):
                    truth_state = "SPOT_SKIPPED_INFO"
                    truth_severity = "INFO"
                    display_terminal_state = "SPOT_MARKET_SKIPPED"
                    display_operator_action = "NO_ACTION_SPOT_SKIP"
                elif _is_resolved_raw_index_mapping_row(_eq_coin, terminal_state):
                    _mapped = _raw_at_index_core_symbol(_eq_coin)
                    truth_state = "HISTORICAL_SYMBOL_MAPPING_FIXED"
                    truth_severity = "INFO"
                    display_terminal_state = f"SYMBOL_MAPPING_FIXED_NOW:{_mapped}"
                    display_operator_action = "NO_ACTION_HISTORICAL_CACHE_FIXED"
                else:
                    truth_state = "ACTIVE_LOCAL_BLOCK" if is_fresh else "HISTORICAL_LOCAL_BLOCK"
                    truth_severity = "AMBER" if is_fresh else "HISTORICAL"
            # Derive operator_action for unsupported-symbol rows when field is absent
            if not display_operator_action:
                if status == "SYMBOL_CACHE_MISS_NEEDS_REFRESH" or (
                    terminal_state == "SYMBOL_CACHE_MISS_NEEDS_REFRESH"
                ):
                    display_operator_action = "REFRESH_ASSET_UNIVERSE_CACHE"
                elif status in {"SEND_NOT_ATTEMPTED_UNSUPPORTED_SYMBOL", "UNSUPPORTED_SYMBOL_OR_METADATA"} or (
                    terminal_state in {
                        "SEND_NOT_ATTEMPTED_UNSUPPORTED_SYMBOL",
                        "UNSUPPORTED_SYMBOL_OR_METADATA",
                    }
                ):
                    display_operator_action = "CHECK_ASSET_UNIVERSE_CACHE_OR_SYMBOL_CONFIG"
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
        wallet_position_before = live_fill.get("wallet_position_before") if live_fill else None
        if wallet_position_before in (None, ""):
            wallet_position_before = attempt.get("wallet_position_before")
        wallet_position_after = live_fill.get("wallet_position_after") if live_fill else None
        if wallet_position_after in (None, ""):
            wallet_position_after = attempt.get("wallet_position_after_expected")
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
            "status": status,
            "reject_category": reject_category,
            "terminal_state": display_terminal_state,
            "raw_terminal_state": terminal_state,
            "operator_action": display_operator_action,
            "limit_px": limit_px if limit_px > 0 else None,
            "fill_avg_px": fill_px if fill_px > 0 else None,
            "fill_size": fill_size_val,
            "oid": oid_val,
            "wallet_position_before": wallet_position_before,
            "wallet_position_after": wallet_position_after,
            "leader_to_send_attempt_ms": attempt.get("leader_to_send_attempt_ms", ""),
            "send_total_ms": attempt.get("send_total_ms", ""),
            "symbol_resolve_ms": attempt.get("symbol_resolve_ms", ""),
            "sdk_client_ms": attempt.get("sdk_client_ms", ""),
            "exchange_call_ms": attempt.get("exchange_call_ms", ""),
            "has_live_fill": has_live_fill,
            "truth_state": truth_state,
            "truth_severity": truth_severity,
            "is_active": (_is_unresolved_close_recovery and truth_severity in {"RED", "AMBER"}) or (is_fresh and truth_severity in {"RED", "AMBER"}),
            "fill_bps": fill_bps,
            "leader_bps": leader_bps,
            "marketable_bps": fnum(attempt.get("marketable_bps")) or None,
            "error": attempt.get("error", ""),
        })
    return rows


def _is_spot_index_coin(coin: Any) -> bool:
    """Return True if coin is an HL spot/internal-index fill (#<n>).
    The copy engine is perp-only — spot fills are valid no-action skips, not errors."""
    s = str(coin or "").strip()
    if not s.startswith("#"):
        return False
    try:
        int(s[1:])
        return True
    except ValueError:
        return False


_ASSET_SNAPSHOT_CACHE: Tuple[float, Dict[str, Any]] = (0.0, {})


def _live_asset_snapshot() -> Dict[str, Any]:
    global _ASSET_SNAPSHOT_CACHE
    path = LIVE_COPY_AUDIT_DIR / "asset_universe_snapshot.json"
    now = time.time()
    ts, cached = _ASSET_SNAPSHOT_CACHE
    if cached and now - ts < 5:
        return cached
    snap = load_json(path, {})
    if not isinstance(snap, dict):
        snap = {}
    _ASSET_SNAPSHOT_CACHE = (now, snap)
    return snap


def _raw_at_index_core_symbol(coin: Any) -> str:
    s = str(coin or "").strip()
    if not s.startswith("@"):
        return ""
    try:
        idx = int(s[1:])
    except ValueError:
        return ""
    mapping = _live_asset_snapshot().get("asset_index_to_symbol")
    if not isinstance(mapping, dict):
        return ""
    return str(mapping.get(str(idx)) or mapping.get(idx) or "").strip()


def _is_spot_skip_row(coin: Any, terminal_state: Any = "", notes: Any = "") -> bool:
    """Detect spot-index skip rows from any source (coin prefix, terminal state, or notes)."""
    if _is_spot_index_coin(coin):
        return True
    s = str(coin or "").strip()
    if s.startswith("@"):
        return not bool(_raw_at_index_core_symbol(s))
    t = str(terminal_state or "").upper()
    if t in {"SPOT_MARKET_SKIPPED", "ASSET_INDEX_NOT_IN_META"}:
        return True
    n = str(notes or "").lower()
    return "spot_market_skipped" in n or "asset_index_not_in_meta" in n


def _is_resolved_raw_index_mapping_row(coin: Any, terminal_state: Any = "") -> bool:
    t = str(terminal_state or "").upper()
    return (
        ("PERP_INDEX_MAPPING_UNVERIFIED" in t or t == "SPOT_MARKET_SKIPPED")
        and bool(_raw_at_index_core_symbol(coin))
    )


def _build_reconciliation_execution_quality_rows(reconciliation_rows: List[Dict[str, Any]], limit: int = 25) -> List[Dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    active_window_ms = 60 * 60 * 1000
    rows: List[Dict[str, Any]] = []
    for rec in reversed(reconciliation_rows[-limit:] if reconciliation_rows else []):
        if not isinstance(rec, dict):
            continue
        ms = _row_time_ms(rec)
        age_ms = max(0, now_ms - ms) if ms else 0
        fresh = bool(ms and age_ms <= active_window_ms)
        event = str(rec.get("event") or rec.get("status") or "RECONCILIATION_EVENT")
        terminal = str(rec.get("terminal_state") or rec.get("status") or event)
        rows.append({
            "time": rec.get("created_at") or rec.get("timestamp") or rec.get("time") or rec.get("updated_at") or "",
            "leader_wallet": str(rec.get("leader_wallet") or rec.get("wallet") or ""),
            "coin": rec.get("coin", ""),
            "side": rec.get("side", ""),
            "status": "RECONCILIATION_EVENT",
            "reject_category": rec.get("reject_category", ""),
            "terminal_state": terminal,
            "raw_terminal_state": terminal,
            "operator_action": rec.get("action") or rec.get("operator_action") or event,
            "limit_px": None,
            "fill_avg_px": None,
            "fill_size": "",
            "oid": rec.get("oid") or rec.get("exchange_order_id") or "",
            "wallet_position_before": rec.get("position_before", ""),
            "wallet_position_after": rec.get("position_after", ""),
            "leader_to_send_attempt_ms": "",
            "send_total_ms": "",
            "symbol_resolve_ms": "",
            "sdk_client_ms": "",
            "exchange_call_ms": "",
            "has_live_fill": False,
            "truth_state": "RECENT_RECONCILIATION" if fresh else "HISTORICAL_RECONCILIATION",
            "truth_severity": "INFO" if fresh else "HISTORICAL",
            "is_active": False,
            "fill_bps": None,
            "leader_bps": None,
            "marketable_bps": None,
            "source_label": "reconciliation",
            "age_label": _age_label_from_ms(ms, now_ms),
            "error": rec.get("notes") or rec.get("error") or "",
        })
        # Spot/internal-index fills: perp engine cannot copy these — classify as
        # informational no-action, not as symbol mapping failures requiring maintenance.
        _coin = rec.get("coin", "")
        _notes = rec.get("notes") or rec.get("error") or ""
        if _is_spot_skip_row(_coin, terminal, _notes):
            rows[-1].update({
                "truth_state": "SPOT_SKIPPED_INFO",
                "truth_severity": "INFO",
                "terminal_state": "SPOT_MARKET_SKIPPED",
                "raw_terminal_state": terminal,
                "operator_action": "NO_ACTION_SPOT_SKIP",
                "reject_category": "SPOT_MARKET_SKIPPED",
                "error": f"Spot market skipped — perp engine only. Leader traded HL spot/index asset ({_coin}). No order sent.",
            })
        elif _is_resolved_raw_index_mapping_row(_coin, terminal):
            _mapped = _raw_at_index_core_symbol(_coin)
            rows[-1].update({
                "truth_state": "HISTORICAL_SYMBOL_MAPPING_FIXED",
                "truth_severity": "INFO",
                "terminal_state": f"SYMBOL_MAPPING_FIXED_NOW:{_mapped}",
                "raw_terminal_state": terminal,
                "operator_action": "NO_ACTION_HISTORICAL_CACHE_FIXED",
                "reject_category": "HISTORICAL_CACHE_STALE_FIXED",
                "error": f"Historical raw index {_coin} now resolves to core perp {_mapped}; startup cache refresh fixed for future sends.",
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
    exchange_available = _exchange_snapshot_available(exchange_snapshot)
    coin_wallets: Dict[str, set[str]] = {}
    for pw, coin, pos in iter_manual_wallet_positions(manual_positions):
        if abs(fnum(pos.get("signed_size"))) <= 1e-12:
            continue
        if pw:
            coin_wallets.setdefault(str(coin).upper(), set()).add(pw)
    sleeves_for_net = _manual_position_sleeves(manual_positions)
    _coin_counts, coin_nets = _shared_manual_coin_nets(sleeves_for_net)
    ownership_by_coin: Dict[str, Dict[str, Any]] = {}
    for coin, owned_net in coin_nets.items():
        ex = exchange_positions.get(coin, {}) if isinstance(exchange_positions, dict) else {}
        exchange_net = fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0
        ownership_by_coin[coin] = _classify_owned_exchange_net(owned_net, exchange_net, exchange_available)

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
                matched_exchange_fills.append({**match, "_matched_attempt": attempt})
        for live_fill in real_wfills:
            oid = str(live_fill.get("exchange_order_id") or live_fill.get("oid") or "").strip()
            if not oid:
                continue
            match = None
            for exchange_fill in actual_user_fills:
                if str(exchange_fill.get("oid") or "").strip() != oid:
                    continue
                mid = _exchange_fill_match_id(exchange_fill)
                if mid in used_exchange_fills:
                    continue
                match = exchange_fill
                used_exchange_fills.add(mid)
                break
            if match is not None:
                matched_exchange_fills.append({**match, "_matched_attempt": live_fill})
        confirmed_realized_pnl: Optional[float] = None
        realized_match_status = "NO_MATCHED_EXCHANGE_CLOSED_PNL"
        matched_closed_count = sum(1 for fill in matched_exchange_fills if abs(fnum(fill.get("closedPnl") if "closedPnl" in fill else fill.get("closed_pnl"))) > 1e-12)
        if matched_exchange_fills:
            confirmed_realized_pnl = round(sum(fnum(fill.get("closedPnl") if "closedPnl" in fill else fill.get("closed_pnl")) for fill in matched_exchange_fills), 8)
            realized_match_status = "EXACT_ID_MATCHED_EXCHANGE_USER_FILLS" if matched_closed_count else "EXACT_ID_MATCHED_USER_FILLS_CLOSEDPNL_ZERO"
            if realized_pnl is None and (matched_closed_count or any(("closedPnl" in fill or "closed_pnl" in fill) for fill in matched_exchange_fills)):
                realized_pnl = confirmed_realized_pnl
                pnl_status = "EXACT"
                attribution_quality = "EXACT_ID_EXCHANGE_CLOSED_PNL"
                data_quality_notes = [
                    note for note in data_quality_notes
                    if "wallet attribution unavailable" not in str(note)
                ]
            cumulative_closed = 0.0
            for fill in sorted(matched_exchange_fills, key=lambda r: _row_time_ms(r)):
                has_closed_pnl_field = "closedPnl" in fill or "closed_pnl" in fill
                if not has_closed_pnl_field:
                    continue
                cumulative_closed += fnum(fill.get("closedPnl") if "closedPnl" in fill else fill.get("closed_pnl"))
                fill_ms = _row_time_ms(fill)
                if fill_ms > 0:
                    pnl_points.append({
                        "timestamp_ms": fill_ms,
                        "timestamp": fill.get("time") or fill.get("created_at") or fill.get("timestamp") or "",
                        "value": round(cumulative_closed, 8),
                        "source": "exact_exchange_closedPnl",
                    })
        elif filled_attempts:
            realized_match_status = "ACCOUNT_LEVEL_ONLY"

        open_positions: List[Dict[str, Any]] = []
        current_exposure = 0.0
        total_unrealized = 0.0
        has_unrealized = False
        has_unattributed_open_pnl = False

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
            entry = fnum(pos.get("avg_entry_px")) or (fnum(ex.get("entry_px")) if isinstance(ex, dict) else 0.0)
            exp_est = abs(signed) * mark if mark > 0 else (abs(signed) * entry if entry > 0 else 0.0)
            current_exposure += exp_est
            ownership = ownership_by_coin.get(coin_upper, _classify_owned_exchange_net(signed, fnum(ex.get("signed_size")) if isinstance(ex, dict) else 0.0, exchange_available))
            ownership_status = str(ownership.get("status") or "EXCHANGE_UNAVAILABLE")
            owned_upnl = _owned_sleeve_unrealized(signed, entry, mark)
            shared_supported = coin_shared and ownership_status in {"OWNED_FULLY_SUPPORTED", "OWNED_PLUS_ACCOUNT_RESIDUAL"}
            if coin_shared and shared_supported:
                match = "SHARED_SYMBOL_SLEEVE_ATTRIBUTED"
                if owned_upnl is not None:
                    total_unrealized += owned_upnl
                    has_unrealized = True
            elif coin_shared:
                match = "SHARED_SYMBOL_NOT_ATTRIBUTED"
                has_unattributed_open_pnl = True
            elif exchange_available and isinstance(ex, dict) and ex:
                match = ownership_status
                if owned_upnl is not None and ownership_status not in {"SIGN_CONFLICT_PENDING_RECONCILIATION", "EXTERNAL_FLAT_PENDING_RECONCILIATION", "OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE"}:
                    total_unrealized += owned_upnl
                    has_unrealized = True
                if ownership_status in {"OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE", "EXTERNAL_FLAT_PENDING_RECONCILIATION", "SIGN_CONFLICT_PENDING_RECONCILIATION"}:
                    data_quality_notes.append(f"{coin_upper} {ownership_status}")
            else:
                match = "EXCHANGE_UNAVAILABLE"
            open_positions.append({
                "coin": coin_upper,
                "signed_size": signed,
                "side": "LONG" if signed > 0 else "SHORT",
                "entry_px": entry if entry > 0 else None,
                "mark_px": mark if mark > 0 else None,
                "unrealized_pnl": owned_upnl if exchange_available and (not coin_shared or shared_supported) and ownership_status not in {"SIGN_CONFLICT_PENDING_RECONCILIATION", "EXTERNAL_FLAT_PENDING_RECONCILIATION", "OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE"} else None,
                "exposure": round(exp_est, 4) if exp_est > 0 else None,
                "exchange_match": match,
                "ownership_status": ownership_status,
                "residual_size": ownership.get("residual_size", 0.0),
                "unsupported_size": ownership.get("unsupported_size", 0.0),
            })

        unrealized_pnl: Optional[float] = round(total_unrealized, 4) if has_unrealized else None

        if has_unattributed_open_pnl:
            data_quality_notes.append("shared coin across leaders; exchange open PnL not attributed")
            unrealized_pnl = None
            if realized_pnl is not None and realized_match_status.startswith("EXACT_ID_MATCHED"):
                pnl_status = "EXACT_REALIZED_ONLY_OPEN_SHARED"
                attribution_quality = "EXACT_REALIZED_ONLY_OPEN_PNL_NOT_ATTRIBUTED_SHARED_COIN"
            else:
                pnl_status = "AMBIGUOUS_COIN_SHARED"
                attribution_quality = "SHARED_COIN_PNL_NOT_ATTRIBUTED"
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
        if pnl_status == "EXACT_REALIZED_ONLY_OPEN_SHARED":
            net_pnl = None

        drawdown: Optional[float] = None
        # NOTE: `max_drawdown` here is REALISED-only (peak-to-trough on
        # cumsum(closedPnl) across matched exchange fills). It DOES NOT include
        # unrealised losses on held positions, so a wallet that closes winners
        # aggressively while letting losers run will look pristine here while
        # bleeding MTM. The `*_mtm` companion fields below pull the truth from
        # HL accountValueHistory and should be preferred for any risk-adjusted
        # ranking. See REALISED_VS_MTM_DRAWDOWN.md.
        max_drawdown: Optional[float] = None
        max_drawdown_realised: Optional[float] = None
        if pnl_points:
            peak = pnl_points[0]["value"]
            max_dd = 0.0
            for p in pnl_points:
                peak = max(peak, fnum(p.get("value")))
                dd = fnum(p.get("value")) - peak
                max_dd = min(max_dd, dd)
            drawdown = round(fnum(pnl_points[-1].get("value")) - peak, 4)
            max_drawdown = round(max_dd, 4)
            max_drawdown_realised = max_drawdown

        # Pull MTM stats from cache-backed HL portfolio API (soft-fail, ~ms on hit).
        try:
            _mtm = _get_mtm_stats(wallet_key)
        except Exception:
            _mtm = {"max_drawdown_mtm": None, "month_pnl_chg_mtm": None,
                    "month_acctV_end": None, "mtm_calmar": None,
                    "allTime_max_drawdown_mtm": None, "allTime_pnl_chg_mtm": None,
                    "mtm_source": "exception", "mtm_fetched_at": None}
        max_drawdown_mtm = _mtm.get("max_drawdown_mtm")
        alltime_max_drawdown_mtm = _mtm.get("allTime_max_drawdown_mtm")
        alltime_pnl_chg_mtm = _mtm.get("allTime_pnl_chg_mtm")
        month_pnl_chg_mtm = _mtm.get("month_pnl_chg_mtm")
        mtm_calmar = _mtm.get("mtm_calmar")
        mtm_source = _mtm.get("mtm_source")
        alltime_acctV_peak = _mtm.get("allTime_acctV_peak")
        month_acctV_peak = _mtm.get("month_acctV_peak")
        true_ts_curve = _account_value_curve(wallet_key)
        true_ts_drawdown = [
            {
                "timestamp": p.get("timestamp"),
                "timestamp_ms": p.get("timestamp_ms"),
                "value": p.get("drawdown_usd"),
                "drawdown_pct": p.get("drawdown_pct"),
                "equity_usd": p.get("equity_usd"),
                "running_peak_usd": p.get("running_peak_usd"),
            }
            for p in true_ts_curve
        ]
        if true_ts_curve:
            last_equity = fnum(true_ts_curve[-1].get("equity_usd"))
            peak_equity = max(fnum(p.get("running_peak_usd")) for p in true_ts_curve)
            if alltime_acctV_peak is None:
                alltime_acctV_peak = peak_equity
            if alltime_max_drawdown_mtm is None:
                alltime_max_drawdown_mtm = min(fnum(p.get("drawdown_usd")) for p in true_ts_curve)

        # Enrich with wallet_universe.csv true time-series DD (8012 pipeline output)
        # This provides allTime_max_drawdown_mtm and allTime_acctV_peak for wallets
        # where live API may not have full history.
        try:
            if not hasattr(_build_live_leader_performance, "_universe_mtm_cache"):
                import pandas as pd
                uni_path = BASE_DIR / "data" / "wallet_universe.csv"
                if uni_path.exists():
                    df = pd.read_csv(uni_path, usecols=["wallet", "allTime_max_drawdown_mtm", "allTime_acctV_peak", "allTime_pnl_chg_mtm", "max_drawdown_mtm", "month_acctV_peak", "month_pnl_chg_mtm", "mtm_source", "mtm_fetched_at"])
                    df["wallet"] = df["wallet"].astype(str).str.strip().str.lower()
                    _build_live_leader_performance._universe_mtm_cache = df.set_index("wallet").to_dict("index")
                else:
                    _build_live_leader_performance._universe_mtm_cache = {}
            uni = _build_live_leader_performance._universe_mtm_cache.get(wallet_key)
            if uni:
                # Prefer universe data for allTime (more complete history)
                if alltime_max_drawdown_mtm is None and uni.get("allTime_max_drawdown_mtm") is not None:
                    alltime_max_drawdown_mtm = fnum(uni.get("allTime_max_drawdown_mtm"))
                if alltime_pnl_chg_mtm is None and uni.get("allTime_pnl_chg_mtm") is not None:
                    alltime_pnl_chg_mtm = fnum(uni.get("allTime_pnl_chg_mtm"))
                if max_drawdown_mtm is None and uni.get("max_drawdown_mtm") is not None:
                    max_drawdown_mtm = fnum(uni.get("max_drawdown_mtm"))
                if month_pnl_chg_mtm is None and uni.get("month_pnl_chg_mtm") is not None:
                    month_pnl_chg_mtm = fnum(uni.get("month_pnl_chg_mtm"))
                # Enrich peak values for DD% calculation
                if alltime_max_drawdown_mtm is not None and uni.get("allTime_acctV_peak") is not None:
                    alltime_acctV_peak = fnum(uni.get("allTime_acctV_peak"))
                if max_drawdown_mtm is not None and uni.get("month_acctV_peak") is not None:
                    month_acctV_peak = fnum(uni.get("month_acctV_peak"))
                if mtm_source in (None, "exception", "unavailable") and uni.get("mtm_source"):
                    mtm_source = uni.get("mtm_source")
        except Exception:
            pass

        exposure_series = [{
            "timestamp": exchange_snapshot.get("updated_at") or utc_now_iso(),
            "value": round(current_exposure, 4),
        }] if current_exposure else []
        last_fill_ui = _normalize_last_fill_for_ui(last_actual_fill)
        if real_wfills:
            latest_live_fill = max(real_wfills, key=lambda r: _row_time_ms(r) or _iso_to_ms(r.get("created_at")))
            live_fill_ui = _normalize_last_fill_for_ui(latest_live_fill)
            if not last_fill_ui or (_row_time_ms(latest_live_fill) > _row_time_ms(last_actual_fill)):
                last_fill_ui = live_fill_ui
        pnl_status_labels = {
            "EXACT": "Exact closed PnL",
            "EXACT_REALIZED_ONLY_OPEN_SHARED": "Exact realized only; open shared",
            "OPEN_ONLY": "Open PnL",
            "ESTIMATED_FROM_REAL_ORDER_FILLS": "Real fills",
            "ACCOUNT_LEVEL_ONLY": "Account-level only",
            "AMBIGUOUS_COIN_SHARED": "Shared coin",
            "N/A": "No PnL yet",
        }
        pnl_status_label = pnl_status_labels.get(pnl_status, pnl_status or "No live PnL")
        pnl_not_proven_reason = "" if pnl_status in {"EXACT", "OPEN_ONLY", "EXACT_REALIZED_ONLY_OPEN_SHARED"} else (attribution_quality or "exact wallet PnL source unavailable")
        matched_pnl_fills: List[Dict[str, Any]] = []
        for fill in sorted(matched_exchange_fills, key=lambda r: _row_time_ms(r), reverse=True):
            attempt = fill.get("_matched_attempt") if isinstance(fill.get("_matched_attempt"), dict) else {}
            closed = fnum(fill.get("closedPnl") if "closedPnl" in fill else fill.get("closed_pnl"))
            matched_pnl_fills.append({
                "time": datetime.fromtimestamp((_row_time_ms(fill) or _send_attempt_time_ms(attempt)) / 1000, tz=timezone.utc).isoformat() if (_row_time_ms(fill) or _send_attempt_time_ms(attempt)) else (attempt.get("created_at") or ""),
                "coin": fill.get("coin") or attempt.get("coin") or "",
                "dir": fill.get("dir") or "",
                "side": _exchange_fill_side(fill) or attempt.get("side") or "",
                "size": fill.get("sz") or attempt.get("fill_size") or "",
                "price": fill.get("px") or attempt.get("fill_avg_px") or "",
                "closed_pnl": round(closed, 8),
                "fee": fnum(fill.get("fee") or 0),
                "oid": fill.get("oid") or attempt.get("oid") or "",
                "match": "EXACT_EXCHANGE_OID",
            })

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
            "max_drawdown": max_drawdown,  # legacy: REALISED-only; kept for back-compat
            "max_drawdown_realised": max_drawdown_realised,
            "max_drawdown_mtm": max_drawdown_mtm,
            "allTime_max_drawdown_mtm": alltime_max_drawdown_mtm,
            "allTime_acctV_peak": alltime_acctV_peak,
            "leader_equity_base": last_equity if true_ts_curve else None,
            "true_ts_equity_series": true_ts_curve,
            "true_ts_drawdown_series": true_ts_drawdown,
            "allTime_pnl_chg_mtm": alltime_pnl_chg_mtm,
            "month_pnl_chg_mtm": month_pnl_chg_mtm,
            "month_acctV_peak": month_acctV_peak,
            "mtm_calmar": mtm_calmar,
            "mtm_source": mtm_source,
            "realized_pnl_estimate": realized_pnl,
            "unrealized_pnl_estimate": unrealized_pnl,
            "net_pnl_estimate": net_pnl,
            "live_realized_pnl": realized_pnl,
            "confirmed_realized_pnl": confirmed_realized_pnl,
            "realized_match_status": realized_match_status,
            "matched_exchange_fill_count": len(matched_exchange_fills),
            "matched_closed_pnl_fill_count": matched_closed_count,
            "matched_pnl_fills": matched_pnl_fills[:100],
            "live_unrealized_pnl": unrealized_pnl,
            "live_net_pnl": net_pnl,
            "live_equity_effect": net_pnl,
            "pnl_status": pnl_status,
            "pnl_status_label": pnl_status_label,
            "pnl_not_proven_reason": pnl_not_proven_reason,
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
            "last_actual_fill": last_fill_ui,
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
    local_blk = [r for r in execution_quality_rows if r.get("status") and r.get("status") not in _TERMINAL_IGNORE and r.get("status") != "RECONCILIATION_EVENT"]
    active_red = [r for r in execution_quality_rows if r.get("is_active") and r.get("truth_severity") == "RED"]
    active_amber = [r for r in execution_quality_rows if r.get("is_active") and r.get("truth_severity") == "AMBER"]
    historical = [r for r in execution_quality_rows if str(r.get("truth_state") or "").startswith("HISTORICAL")]
    adopted = [r for r in execution_quality_rows if r.get("truth_state") == "ADOPTED_RECONCILED"]
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
        "active_red_count": len(active_red),
        "active_amber_count": len(active_amber),
        "historical_count": len(historical),
        "adopted_reconciled_count": len(adopted),
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
    config = _load_live_copy_config()
    explicit = inum(config.get("monitor_start_ms"))
    if explicit:
        return explicit
    return iso_to_ms(config.get("updated_at"))


def normalise_recording_method(row: Dict[str, Any], source: str, is_snapshot: bool) -> str:
    raw = str(row.get("recording_method", "")).strip().upper()
    if raw in {WS_CAPTURED, REBUILD}:
        return raw
    # Wallet Finder 8014 is poll-only. A successful post-baseline poll row is
    # live captured evidence; REBUILD remains reserved for accounting recovery.
    return WS_CAPTURED if source in {"ws", "poll"} and not is_snapshot else REBUILD


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
    purged = load_purged_wallets()
    min_ts = monitor_start_ms()
    try:
        stat = RAW_FILLS_CSV.stat()
        cache_key = (
            str(RAW_FILLS_CSV.resolve()),
            stat.st_mtime_ns,
            stat.st_size,
            min_ts,
            tuple(sorted(purged)),
        )
    except Exception:
        cache_key = None
    if cache_key is not None:
        with _RAW_FILLS_CACHE_LOCK:
            if _RAW_FILLS_CACHE.get("key") == cache_key and isinstance(_RAW_FILLS_CACHE.get("fills"), list):
                return _RAW_FILLS_CACHE["fills"]
    fills: List[RawFill] = []
    seen = set()
    with RAW_FILLS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            fill = parse_raw_fill_row(row)
            if fill is None or fill.fill_id in seen or fill.wallet in purged:
                continue
            if min_ts and fill.timestamp_ms < min_ts:
                continue
            seen.add(fill.fill_id)
            fills.append(fill)
    fills.sort(key=lambda x: (x.timestamp_ms, x.wallet, x.coin, x.fill_id))
    if cache_key is not None:
        with _RAW_FILLS_CACHE_LOCK:
            _RAW_FILLS_CACHE["key"] = cache_key
            _RAW_FILLS_CACHE["fills"] = fills
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
    wallet_key = str(wallet).lower()
    runtime_cache = ui.setdefault(_EFFECTIVE_WALLET_UI_CACHE_KEY, {}) if isinstance(ui, dict) else {}
    if isinstance(runtime_cache, dict) and wallet_key in runtime_cache:
        return dict(runtime_cache[wallet_key])
    cfg = (ui.get("wallet_config") or {}).get(wallet_key, {})
    mode = str(cfg.get("copy_mode", ui.get("copy_mode", "proportional"))).lower()
    if mode not in {"proportional", "fixed"}:
        mode = str(ui.get("copy_mode", "proportional")).lower()
    wallet_norm_base = max(1.0, fnum(cfg.get("norm_base", ui.get("norm_base")), DEFAULT_NORM_BASE))
    if parse_bool(cfg.get("leader_equity_base_locked", False)) and "leader_equity_base" in cfg and str(cfg.get("leader_equity_base", "")).strip() != "":
        leader_equity_base = max(1.0, fnum(cfg.get("leader_equity_base"), wallet_norm_base))
    else:
        leader_equity_base = max(1.0, fnum(_current_leader_equity_from_cache(wallet), ui.get("leader_equity_base", DEFAULT_LEADER_EQUITY)))
    result = {
        **ui,
        "copy_mode": mode if mode in {"proportional", "fixed"} else "proportional",
        "norm_base": wallet_norm_base,
        "fixed_notional": max(0.01, fnum(cfg.get("fixed_notional", ui.get("fixed_notional")), DEFAULT_FIXED_NOTIONAL)),
        "leader_equity_base": leader_equity_base,
        "wallet_override": bool(cfg),
    }
    result.pop(_EFFECTIVE_WALLET_UI_CACHE_KEY, None)
    result.pop(_SIZING_EQUITY_TIMELINE_CACHE_KEY, None)
    result.pop(_SIZING_DENOMINATOR_CACHE_KEY, None)
    if isinstance(runtime_cache, dict):
        runtime_cache[wallet_key] = dict(result)
    return result


def wallet_alloc(wallet: str, ui: Dict[str, Any]) -> float:
    return max(1.0, fnum(effective_wallet_ui(wallet, ui).get("norm_base"), DEFAULT_NORM_BASE))


def proportional_sizing_denominator(cfg: Dict[str, Any]) -> float:
    return max(1.0, fnum(cfg.get("leader_equity_base"), DEFAULT_LEADER_EQUITY))


def _leader_equity_at_or_before(
    wallet: str,
    timestamp_ms: int,
    fallback: float,
    curve: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[float, str]:
    """Return causal account equity for a fill without looking into the future."""
    curve = curve if isinstance(curve, list) else _account_value_curve(wallet)
    target = inum(timestamp_ms)
    if target <= 0 or not curve:
        return max(1.0, fnum(fallback, DEFAULT_LEADER_EQUITY)), "current_equity_fallback"
    lo, hi = 0, len(curve)
    while lo < hi:
        mid = (lo + hi) // 2
        if inum(curve[mid].get("timestamp_ms")) <= target:
            lo = mid + 1
        else:
            hi = mid
    idx = lo - 1
    if idx < 0:
        return max(1.0, fnum(fallback, DEFAULT_LEADER_EQUITY)), "current_equity_fallback"
    equity = fnum(curve[idx].get("equity_usd"))
    if equity <= 0:
        return max(1.0, fnum(fallback, DEFAULT_LEADER_EQUITY)), "current_equity_fallback"
    return max(1.0, equity), "account_value_at_fill"


def proportional_sizing_denominator_for_fill(fill: RawFill, ui: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None) -> Tuple[float, str]:
    """Use timestamp-aligned leader equity unless the user explicitly fixed it."""
    cache_key = (str(fill.wallet).lower(), inum(fill.timestamp_ms))
    denominator_cache = ui.setdefault(_SIZING_DENOMINATOR_CACHE_KEY, {}) if isinstance(ui, dict) else {}
    if isinstance(denominator_cache, dict) and cache_key in denominator_cache:
        cached = denominator_cache[cache_key]
        if isinstance(cached, tuple) and len(cached) == 2:
            return cached
    cfg = cfg or effective_wallet_ui(fill.wallet, ui)
    wallet_cfg = (ui.get("wallet_config") or {}).get(str(fill.wallet).lower(), {})
    if (
        isinstance(wallet_cfg, dict)
        and parse_bool(wallet_cfg.get("leader_equity_base_locked", False))
        and "leader_equity_base" in wallet_cfg
        and str(wallet_cfg.get("leader_equity_base", "")).strip() != ""
    ):
        result = (proportional_sizing_denominator(cfg), "manual_wallet_override")
    else:
        timeline_cache = ui.setdefault(_SIZING_EQUITY_TIMELINE_CACHE_KEY, {}) if isinstance(ui, dict) else {}
        wallet_key = str(fill.wallet).lower()
        if isinstance(timeline_cache, dict) and wallet_key not in timeline_cache:
            timeline_cache[wallet_key] = _account_value_curve(fill.wallet)
        curve = timeline_cache.get(wallet_key, []) if isinstance(timeline_cache, dict) else None
        result = _leader_equity_at_or_before(
            fill.wallet,
            fill.timestamp_ms,
            proportional_sizing_denominator(cfg),
            curve,
        )
    if isinstance(denominator_cache, dict):
        denominator_cache[cache_key] = result
    return result


def model_copy_notional(fill: RawFill, ui: Dict[str, Any]) -> float:
    cfg = effective_wallet_ui(fill.wallet, ui)
    if cfg["copy_mode"] == "fixed":
        return cfg["fixed_notional"]
    leader_notional = abs(fill.price * fill.size)
    denominator, _source = proportional_sizing_denominator_for_fill(fill, ui, cfg)
    return leader_notional * (cfg["norm_base"] / denominator)


def model_copy_notional_for_size(fill: RawFill, ui: Dict[str, Any], leader_size_units: float) -> float:
    cfg = effective_wallet_ui(fill.wallet, ui)
    full_size = abs(fill.size) if fill.size else abs(leader_size_units)
    frac = abs(leader_size_units) / full_size if full_size > 0 else 1.0
    if cfg["copy_mode"] == "fixed":
        return cfg["fixed_notional"] * frac
    leader_notional = abs(fill.price * leader_size_units)
    denominator, _source = proportional_sizing_denominator_for_fill(fill, ui, cfg)
    return leader_notional * (cfg["norm_base"] / denominator)


def model_copy_notional_meets_min(fill: RawFill, ui: Dict[str, Any], copy_notional: float) -> bool:
    if not parse_bool((ui or {}).get("min_trade_notional_enabled", False)):
        return True
    cfg = effective_wallet_ui(fill.wallet, ui)
    return abs(fnum(copy_notional)) + 1e-12 >= max(0.01, fnum(cfg.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL))


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


def apply_wallet_filters_to_state(filters: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, bool]:
    """Compute wallet_model_exclude from filters applied to current row metrics.
    Returns {wallet: True} for wallets that fail to meet filter criteria.
    App-only, reversible — engine files never touched.
    FILTER_FIX_V3_ACTIVE
    """
    if not filters:
        return {}
    wc = str(filters.get("wallet_contains", "")).strip().lower()

    def _row_metric(row: Dict[str, Any], key: str) -> float:
        # Map filter key -> row field(s) - surgical fix v3 expanded
        val = 0.0
        if key == "lead_equity": val = row.get("lead", {}).get("equity") or row.get("lead_equity")
        elif key == "copy_equity": val = row.get("copy", {}).get("equity") or row.get("copy_equity")
        elif key == "lead_real": val = row.get("lead", {}).get("realized") or row.get("lead", {}).get("realised") or row.get("lead_real") or row.get("lead_realized")
        elif key == "copy_real": val = row.get("copy", {}).get("realized") or row.get("copy", {}).get("realised") or row.get("copy_real") or row.get("copy_realized")
        elif key == "lead_unreal": val = row.get("lead", {}).get("unrealized") or row.get("lead", {}).get("unrealised") or row.get("lead_unreal")
        elif key == "copy_unreal": val = row.get("copy", {}).get("unrealized") or row.get("copy", {}).get("unrealised") or row.get("copy_unreal")
        elif key == "lead_dd": val = row.get("lead", {}).get("drawdown") or row.get("lead_dd")
        elif key == "copy_dd": val = row.get("copy", {}).get("drawdown") or row.get("copy_dd")
        elif key == "lead_maxdd": val = row.get("lead", {}).get("max_drawdown") or row.get("lead", {}).get("maxdd") or row.get("lead_maxdd")
        elif key == "copy_maxdd": val = row.get("copy", {}).get("max_drawdown") or row.get("copy", {}).get("maxdd") or row.get("copy_maxdd")
        elif key == "delta": val = row.get("delta_usd") or row.get("delta", {}).get("equity") or row.get("delta")
        elif key == "pnl_per_hour": val = row.get("pnl_per_hour") or row.get("pnl_hr")
        elif key == "avg_trade_pct": val = row.get("avg_trade_pct")
        elif key == "win_rate": val = row.get("win_rate") or row.get("win_pct")
        elif key == "avg_position_usd": val = row.get("avg_position_usd") or row.get("avg_pos_usd")
        elif key == "max_position_usd": val = row.get("max_position_usd") or row.get("max_pos_usd")
        elif key == "avg_entry_notional_usd": val = row.get("avg_entry_notional_usd") or row.get("avg_notional")
        elif key == "pct_entries_ge10": val = row.get("pct_entries_ge10") or row.get("pct_ge10")
        elif key == "required_leverage": val = row.get("required_leverage") or row.get("req_lev")
        elif key == "fill_count": val = row.get("fill_count") or row.get("fills")
        elif key == "exit_count": val = row.get("exit_count") or row.get("exits")
        elif key == "open_position_count": val = row.get("open_position_count") or row.get("positions") or row.get("pos_count")
        else: val = row.get(key)
        return fnum(val)

    exclude: Dict[str, bool] = {}
    for row in rows:
        wallet = str(row.get("wallet", "")).strip().lower()
        if not wallet or wallet == USER_WALLET.lower() or row.get("is_user_wallet"):
            continue
        if wc and wc not in wallet:
            exclude[wallet] = True
            continue
        for metric in _FILTER_METRIC_KEYS:
            val = _row_metric(row, metric)
            mn = filters.get(f"{metric}_min")
            mx = filters.get(f"{metric}_max")
            if mn is not None and val < float(mn):
                exclude[wallet] = True
                break
            if mx is not None and val > float(mx):
                exclude[wallet] = True
                break
    return exclude


def build_model_state(
    ui: Optional[Dict[str, Any]] = None,
    use_live_config_wallet_model: bool = False,
    persist: bool = True,
) -> Dict[str, Any]:
    ui = ui or load_ui_state()
    # Runtime caches are scoped to one deterministic replay. Never let a
    # caller-reused UI dict retain an older account-equity timeline.
    ui.pop(_EFFECTIVE_WALLET_UI_CACHE_KEY, None)
    ui.pop(_SIZING_EQUITY_TIMELINE_CACHE_KEY, None)
    ui.pop(_SIZING_DENOMINATOR_CACHE_KEY, None)
    if use_live_config_wallet_model:
        ui = apply_live_config_wallet_model(ui)
    # App-level model exclusion: reversible, app/display only.
    # Engine, raw_live_fills, engine_truth, wallet_gate, live_config not mutated.
    model_excluded_wallets: set = {
        w for w, exc in (ui.get("wallet_model_exclude") or {}).items()
        if exc and w != USER_WALLET
    }
    truth = load_engine_truth()
    fills = load_raw_fills()
    model_asof = max(fills, key=lambda f: f.timestamp_ms).timestamp_iso if fills else "1970-01-01T00:00:00+00:00"
    wallets = sorted(set([f.wallet for f in fills]) | set((truth.get("wallets") or {}).keys()) | load_manual_wallets())
    if USER_WALLET and USER_WALLET not in wallets:
        wallets.insert(0, USER_WALLET)
    leader_equity_source_mtimes = _leader_equity_source_mtimes(wallets)

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
    position_keys_by_wallet: Dict[str, List[Tuple[str, str]]] = {}
    leader_position_stats: Dict[Tuple[str, str], Dict[str, float]] = {}
    copy_position_stats: Dict[Tuple[str, str], Dict[str, float]] = {}
    trades: List[Dict[str, Any]] = []
    expected_copy_fills: List[Dict[str, Any]] = []
    position_alignment_errors: List[Dict[str, Any]] = []
    trade_seq = 0

    def wallet_model(wallet: str) -> WalletModel:
        alloc_for_wallet = wallet_alloc(wallet, ui)
        return models.setdefault(wallet, WalletModel(wallet=wallet, alloc=alloc_for_wallet, lead_peak=alloc_for_wallet, copy_peak=alloc_for_wallet))

    def position_stats(store: Dict[Tuple[str, str], Dict[str, float]], key: Tuple[str, str]) -> Dict[str, float]:
        return store.setdefault(key, {
            "signed_units": 0.0,
            "unrealized_signed_size": 0.0,
            "unrealized_signed_cost": 0.0,
            "abs_units": 0.0,
            "notional": 0.0,
        })

    def update_leader_position_stats(key: Tuple[str, str], pos: ModelPosition, factor: float) -> None:
        stats = position_stats(leader_position_stats, key)
        direction = sign_from_side(pos.side)
        raw_units = pos.leader_size_units * factor
        norm_units = ((pos.leader_notional / pos.entry_price_lead) if pos.entry_price_lead > 0 else 0.0) * factor
        stats["signed_units"] += direction * raw_units
        stats["unrealized_signed_size"] += direction * norm_units
        stats["unrealized_signed_cost"] += direction * pos.entry_price_lead * norm_units

    def update_copy_position_stats(key: Tuple[str, str], pos: ModelPosition, factor: float) -> None:
        stats = position_stats(copy_position_stats, key)
        direction = sign_from_side(pos.side)
        units = pos.copy_size_units * factor
        stats["signed_units"] += direction * units
        stats["unrealized_signed_size"] += direction * units
        stats["unrealized_signed_cost"] += direction * pos.entry_price_copy * units
        stats["abs_units"] += abs(pos.copy_size_units) * factor
        stats["notional"] += abs(pos.copy_notional) * factor
        for field in ("signed_units", "unrealized_signed_size", "unrealized_signed_cost", "abs_units", "notional"):
            if abs(stats[field]) <= 1e-12:
                stats[field] = 0.0

    def alignment_status_from_stats(key: Tuple[str, str]) -> Dict[str, Any]:
        lead_signed = position_stats(leader_position_stats, key)["signed_units"]
        copy_signed = position_stats(copy_position_stats, key)["signed_units"]
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

    def copy_wallet_exposure(wallet: str) -> float:
        exposure = 0.0
        for key in position_keys_by_wallet.get(wallet, []):
            _w, coin = key
            pos_list = copy_positions.get(key, [])
            mark = mark_prices.get(coin, pos_list[-1].entry_price_copy if pos_list else 0.0)
            stats = position_stats(copy_position_stats, key)
            exposure += abs(mark) * stats["abs_units"] if mark > 0 else stats["notional"]
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
        min_threshold = max(0.01, fnum(effective_wallet_ui(wallet, ui).get("fixed_notional"), DEFAULT_FIXED_NOTIONAL))
        if n + 1e-12 >= min_threshold:
            m.entry_notional_ge10_count += 1
        m.max_entry_notional_usd = max(m.max_entry_notional_usd, n)

    def append_expected_fill(fill: RawFill, model_action: str, copy_price: float, copy_notional: float, copy_size: float, fee: float, contributes: bool, extra: Optional[Dict[str, Any]] = None) -> None:
        disadv = disadvantage_bps(fill.side, fill.price, copy_price) if contributes else None
        sizing_denominator, sizing_denominator_source = proportional_sizing_denominator_for_fill(fill, ui)
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
            "sizing_leader_equity_base": round(sizing_denominator, 8),
            "sizing_leader_equity_source": sizing_denominator_source,
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
        if fill.wallet in model_excluded_wallets:
            continue  # app-model exclusion: reversible, engine not touched
        m = wallet_model(fill.wallet)
        # Raw ledger fills may be baseline closes/reductions that were never
        # copyable events. Dashboard counters must count only modelled copy
        # events; fill prices are used as a fallback mark only where they do
        # not turn a partial reduce into a synthetic MTM spike.
        key = (fill.wallet, fill.coin)
        copy_positions.setdefault(key, [])
        leader_positions.setdefault(key, [])
        wallet_keys = position_keys_by_wallet.setdefault(fill.wallet, [])
        if key not in wallet_keys:
            wallet_keys.append(key)

        model_fill = _copyable_model_fill(fill, bool(leader_positions[key]))
        if model_fill is None:
            continue
        fill = model_fill

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
            mark_prices[fill.coin] = fill.price
            trade_seq += 1
            copy_notional = model_copy_notional_for_size(fill, ui, fill.size)
            if not model_copy_notional_meets_min(fill, ui, copy_notional):
                continue
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
            update_copy_position_stats(key, pos, 1.0)
            update_leader_position_stats(key, pos, 1.0)
            m.entry_count += 1
            record_entry_notional(fill.wallet, copy_notional)
            m.lead_realized -= lead_fee
            m.copy_realized -= copy_fee
            append_expected_fill(fill, "ENTRY", entry_copy_price, copy_notional, copy_size, copy_fee, entry_contributes_delta, {"trade_id": pos.trade_id, "lead_fee": round(lead_fee, 8)})
        else:
            closed_copy, _miss = close_positions_fifo(copy_positions[key], abs(delta))
            closed_leader, _miss_l = close_positions_fifo(leader_positions[key], abs(delta))
            for closed_pos, closed_frac in closed_copy:
                update_copy_position_stats(key, closed_pos, -closed_frac)
            for closed_pos, closed_frac in closed_leader:
                update_leader_position_stats(key, closed_pos, -closed_frac)
            opened_flip = False
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
                flip_lead_price = fill.price
                flip_copy_price = fill.price
                flip_contributes_delta = False
                flip_disadvantage = None
                flip_notional = model_copy_notional_for_size(fill, ui, _miss)
                if model_copy_notional_meets_min(fill, ui, flip_notional):
                    opened_flip = True
                    trade_seq += 1
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
                    update_copy_position_stats(key, flip_pos, 1.0)
                    update_leader_position_stats(key, flip_pos, 1.0)
                    m.entry_count += 1
                    record_entry_notional(fill.wallet, flip_notional)
                    m.lead_realized -= flip_lead_fee
                    m.copy_realized -= flip_fee
                    append_expected_fill(fill, "FLIP_ENTRY", flip_copy_price, flip_notional, flip_copy_size, flip_fee, flip_contributes_delta, {"trade_id": flip_pos.trade_id, "lead_fee": round(flip_lead_fee, 8)})
            if opened_flip or not copy_positions[key]:
                mark_prices[fill.coin] = fill.price

        if len(expected_copy_fills) > expected_start_idx:
            m.fill_count += 1
            m.ws_fill_count += 1 if fill.recording_method == WS_CAPTURED else 0
            m.poll_fill_count += 1 if fill.source == "poll" else 0
            m.rebuild_fill_count += 1 if fill.recording_method == REBUILD else 0
            m.measured_delta_fill_count += 1 if fill_can_measure_execution_delta(fill) else 0

        status = alignment_status_from_stats(key)
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
        for wallet_key in position_keys_by_wallet.get(fill.wallet, []):
            _wallet, coin = wallet_key
            pos_list = leader_positions.get(wallet_key, [])
            mark = mark_prices.get(coin, pos_list[-1].entry_price_lead if pos_list else 0.0)
            stats = position_stats(leader_position_stats, wallet_key)
            m.lead_unrealized += mark * stats["unrealized_signed_size"] - stats["unrealized_signed_cost"]
        for wallet_key in position_keys_by_wallet.get(fill.wallet, []):
            _wallet, coin = wallet_key
            pos_list = copy_positions.get(wallet_key, [])
            mark = mark_prices.get(coin, pos_list[-1].entry_price_copy if pos_list else 0.0)
            stats = position_stats(copy_position_stats, wallet_key)
            m.copy_unrealized += mark * stats["unrealized_signed_size"] - stats["unrealized_signed_cost"]
        m.open_position_count = sum(len(copy_positions.get(wallet_key, [])) for wallet_key in position_keys_by_wallet.get(fill.wallet, []))
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

    min_trade_filter_on = parse_bool(ui.get("min_trade_notional_enabled", False))
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
        lead_maxdd = m.lead_max_drawdown
        copy_maxdd = m.copy_max_drawdown
        lead_peak = m.lead_peak
        copy_peak = m.copy_peak
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
        min_filtered_empty = (
            min_trade_filter_on
            and wallet != USER_WALLET
            and m.fill_count == 0
            and m.entry_count == 0
            and m.exit_count == 0
            and m.open_position_count == 0
        )
        _eff_cfg = effective_wallet_ui(wallet, ui)
        leader_equity_source = _leader_equity_source_snapshot(wallet)
        leader_equity_locked = parse_bool(((ui.get("wallet_config") or {}).get(wallet, {}) or {}).get("leader_equity_base_locked", False))
        _proof_start_ms, _proof_end_ms = _row_proof_window(m.curve)
        true_ts_dd = _computed_true_drawdown_summary(
            wallet,
            fnum(_eff_cfg.get("norm_base"), m.alloc),
            fnum(_eff_cfg.get("leader_equity_base"), m.alloc),
            _proof_start_ms,
            _proof_end_ms,
        )
        rows.append({
            "wallet": wallet,
            "is_user_wallet": wallet == USER_WALLET,
            "app_model_excluded": wallet in model_excluded_wallets,
            "min_trade_filtered_empty": bool(min_filtered_empty),
            "alloc": m.alloc,
            "lead": block(m.lead_equity, m.lead_realized, m.lead_unrealized, lead_dd, lead_maxdd, lead_peak, m.alloc),
            "copy": block(m.copy_equity, m.copy_realized, m.copy_unrealized, copy_dd, copy_maxdd, copy_peak, m.alloc),
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
            "last_trade_timestamp_ms": last_ts,
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
            "effective_copy_mode": _eff_cfg.get("copy_mode"),
            "effective_norm_base": _eff_cfg.get("norm_base"),
            "effective_fixed_notional": _eff_cfg.get("fixed_notional"),
            "effective_leader_equity_base": _eff_cfg.get("leader_equity_base"),
            "effective_leader_equity_base_locked": leader_equity_locked,
            "effective_leader_equity_base_source": "manual locked override" if leader_equity_locked else leader_equity_source.get("source"),
            "effective_leader_equity_base_source_timestamp_ms": None if leader_equity_locked else leader_equity_source.get("timestamp_ms"),
            "effective_leader_equity_base_source_age_seconds": None if leader_equity_locked else leader_equity_source.get("age_seconds"),
            "effective_leader_equity_base_source_stale": False if leader_equity_locked else bool(leader_equity_source.get("stale", True)),
            "true_ts_dd_now_usd": true_ts_dd.get("true_ts_dd_now_usd"),
            "true_ts_dd_now_pct": true_ts_dd.get("true_ts_dd_now_pct"),
            "true_ts_max_dd_usd": true_ts_dd.get("true_ts_max_dd_usd"),
            "true_ts_max_dd_pct": true_ts_dd.get("true_ts_max_dd_pct"),
            "all_time_true_max_dd_usd": true_ts_dd.get("all_time_true_max_dd_usd"),
            "all_time_true_max_dd_pct": true_ts_dd.get("all_time_true_max_dd_pct"),
            "true_ts_points": true_ts_dd.get("true_ts_points"),
            "true_ts_window_points": true_ts_dd.get("true_ts_window_points"),
            "true_ts_source": true_ts_dd.get("true_ts_source"),
            "true_ts_status": true_ts_dd.get("true_ts_status"),
            "true_dd_promotion_ready": true_ts_dd.get("true_dd_promotion_ready"),
            "promotion_status": true_ts_dd.get("promotion_status"),
            "true_ts_window_start_ms": true_ts_dd.get("true_ts_window_start_ms"),
            "true_ts_window_end_ms": true_ts_dd.get("true_ts_window_end_ms"),
            "true_ts_latest_ms": true_ts_dd.get("true_ts_latest_ms"),
            "true_ts_freshness_tolerance_ms": true_ts_dd.get("true_ts_freshness_tolerance_ms"),
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
    sa = selected_aggregate(portfolio_wallets, trades, user_base, ui)
    alloc = sum(fnum(r["alloc"]) for r in portfolio_wallets)
    lead_equity = sa["lead_real"] + sa["lead_unreal"] + alloc
    copy_equity = sa["copy_real"] + sa["copy_unreal"] + alloc
    lead_real  = sa["lead_real"];  lead_unreal  = sa["lead_unreal"]
    copy_real  = sa["copy_real"];  copy_unreal  = sa["copy_unreal"]
    open_notional_usd = sa["current_position_usd"]
    # Rebuild combined graph by timestamp using latest wallet lead/copy equity at each event.
    portfolio_history = build_portfolio_history(rows, ui)
    hist_lead_peak = max((fnum((p.get("lead") or {}).get("peak_equity")) for p in portfolio_history), default=lead_equity)
    hist_copy_peak = max((fnum((p.get("copy") or {}).get("peak_equity")) for p in portfolio_history), default=copy_equity)
    # Current DD: selected portfolio peak-to-current on summed equity, never sum row maxDD/DD.
    lead_live_dd = max(0.0, max(hist_lead_peak, lead_equity) - lead_equity)
    copy_live_dd = max(0.0, max(hist_copy_peak, copy_equity) - copy_equity)
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
    proof_window_start, proof_window_end = _portfolio_history_window_ms(portfolio_history)
    true_drawdown_history = build_computed_true_drawdown_history(rows, ui, proof_window_start, proof_window_end)
    apply_user_true_drawdown_rollup(rows, portfolio_wallets, true_drawdown_history, user_base)
    true_curve_counts = true_curve_status_counts(portfolio_wallets)
    # Max exposure is canonical from selected_aggregate; header and USER row must match exactly.
    max_open_notional_usd = sa["max_position_usd"]
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

    ui_state_for_output = dict(ui)
    ui_state_for_output.pop(_EFFECTIVE_WALLET_UI_CACHE_KEY, None)
    ui_state_for_output.pop(_SIZING_EQUITY_TIMELINE_CACHE_KEY, None)
    ui_state_for_output.pop(_SIZING_DENOMINATOR_CACHE_KEY, None)
    state = {
        "live_config": load_live_config(),
        "schema": "app_model_state.v1.derived_only",
        "updated_at": utc_now_iso(),
        "model_asof": model_asof,
        "source_schema": truth.get("schema", "unknown"),
        "ui_state": ui_state_for_output,
        "user_wallet": USER_WALLET,
        "wallets": {r["wallet"]: r for r in rows},
        "wallet_rows": rows,
        "portfolio": portfolio,
        "portfolio_history": portfolio_history,
        "true_drawdown_history": true_drawdown_history,
        "true_curve_status_counts": true_curve_counts,
        "copy_trades": trades,
        "expected_copy_fills": expected_copy_fills,
        "position_alignment_errors": position_alignment_errors,
        "position_alignment_ok": not position_alignment_errors,
        "engine_truth_boundary": "app-derived only; engine truth is not mutated",
        "leader_equity_source_mtimes": leader_equity_source_mtimes,
        "app_model_filter": {
            "enabled": bool(model_excluded_wallets),
            "excluded_wallet_count": len(model_excluded_wallets),
            "modelled_wallet_count": len(wallets) - len(model_excluded_wallets),
            "engine_wallet_count": len(wallets),
            "excluded_wallets": sorted(model_excluded_wallets),
        },
    }
    if persist:
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


def selected_aggregate(portfolio_wallets: List[Dict[str, Any]], trades: List[Dict[str, Any]], norm_base: float, ui: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
    curve_stats = selected_combined_curve_stats(portfolio_wallets, ui)
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


def selected_combined_curve_stats(portfolio_rows: List[Dict[str, Any]], ui: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    events: List[Tuple[str, str, Dict[str, Any]]] = []
    wallet_allocs = {str(r.get("wallet", "")): fnum(r.get("alloc"), DEFAULT_NORM_BASE) for r in portfolio_rows}
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
    alloc = sum(wallet_allocs.values())
    lead_peak = alloc
    copy_peak = alloc
    for _ts, wallet, point in events:
        latest[wallet] = point
        lead_pnl = sum(fnum(v.get("lead_pnl", v.get("pnl"))) for v in latest.values())
        copy_pnl = sum(fnum(v.get("copy_pnl", v.get("pnl"))) for v in latest.values())
        lead_equity = alloc + lead_pnl
        copy_equity = alloc + copy_pnl
        lead_peak = max(lead_peak, lead_equity)
        copy_peak = max(copy_peak, copy_equity)
        current_lead_dd = max(0.0, lead_peak - lead_equity)
        current_copy_dd = max(0.0, copy_peak - copy_equity)
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
        # MIN only filters which entries enter the model. Once an entry is
        # modelled, DD is full mark-to-market risk on that filtered copy book.
        lead_peak = max(lead_peak, lead_equity)
        copy_peak = max(copy_peak, copy_equity)
        lead_dd = max(0.0, lead_peak - lead_equity)
        copy_dd = max(0.0, copy_peak - copy_equity)
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


def refresh_selected_portfolio_view(state: Dict[str, Any], ui: Dict[str, Any]) -> Dict[str, Any]:
    """Recompute selected portfolio header/graph from existing row curves at render time."""
    rows = [dict(r) for r in (state.get("wallet_rows") or []) if isinstance(r, dict)]
    for row in rows:
        if not row.get("is_user_wallet"):
            row["include_in_portfolio"] = wallet_included(str(row.get("wallet", "")), ui)
    base = fnum(ui.get("norm_base"), DEFAULT_NORM_BASE)
    user_base = max(1.0, fnum(ui.get("user_norm_base"), base))
    portfolio_wallets = [
        r for r in rows
        if not r.get("is_user_wallet") and bool(r.get("include_in_portfolio"))
    ]
    trades = state.get("copy_trades") or []
    sa = selected_aggregate(portfolio_wallets, trades if isinstance(trades, list) else [], user_base, ui)
    alloc = sum(fnum(r.get("alloc"), base) for r in portfolio_wallets)
    lead_real, lead_unreal = sa["lead_real"], sa["lead_unreal"]
    copy_real, copy_unreal = sa["copy_real"], sa["copy_unreal"]
    lead_equity = alloc + lead_real + lead_unreal
    copy_equity = alloc + copy_real + copy_unreal
    portfolio_history = build_portfolio_history(rows, ui)
    hist_lead_peak = max((fnum((p.get("lead") or {}).get("peak_equity")) for p in portfolio_history), default=lead_equity)
    hist_copy_peak = max((fnum((p.get("copy") or {}).get("peak_equity")) for p in portfolio_history), default=copy_equity)
    lead_live_dd = max(0.0, max(hist_lead_peak, lead_equity) - lead_equity)
    copy_live_dd = max(0.0, max(hist_copy_peak, copy_equity) - copy_equity)
    model_asof = str(state.get("model_asof") or state.get("updated_at") or utc_now_iso())
    open_notional_usd = sa["current_position_usd"]
    live_snap = {
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
            "drawdown": round(lead_live_dd, 8), "drawdown_usd": round(lead_live_dd, 8),
            "drawdown_pct": round((lead_live_dd / alloc * 100.0) if alloc else 0.0, 8),
            "peak_equity": round(lead_equity + lead_live_dd, 8),
        },
        "copy": {
            "alloc": round(alloc, 8), "equity": round(copy_equity, 8),
            "realized": round(copy_real, 8), "unrealized": round(copy_unreal, 8),
            "drawdown": round(copy_live_dd, 8), "drawdown_usd": round(copy_live_dd, 8),
            "drawdown_pct": round((copy_live_dd / alloc * 100.0) if alloc else 0.0, 8),
            "peak_equity": round(copy_equity + copy_live_dd, 8),
        },
        "delta": {
            "equity": round(copy_equity - lead_equity, 8),
            "pct": round(((copy_equity - lead_equity) / alloc * 100.0) if alloc else 0.0, 8),
        },
    }
    if portfolio_history and portfolio_history[-1].get("ts") == model_asof:
        portfolio_history[-1] = live_snap
    else:
        portfolio_history.append(live_snap)
    max_open_notional_usd = sa["max_position_usd"]
    lead_maxdd = max((fnum((p.get("lead") or {}).get("drawdown")) for p in portfolio_history), default=lead_live_dd)
    copy_maxdd = max((fnum((p.get("copy") or {}).get("drawdown")) for p in portfolio_history), default=copy_live_dd)

    # Selection changes are applied at render time. Refresh the display-only
    # USER aggregate from the same canonical selected aggregate as the header;
    # otherwise the header updates while the USER row remains from the prior
    # selection and the render contract correctly raises a red warning.
    user_row = next((r for r in rows if r.get("is_user_wallet")), None)
    if user_row is not None:
        total_lead_pnl = lead_real + lead_unreal
        total_copy_pnl = copy_real + copy_unreal
        user_row.update({
            "alloc": user_base,
            "lead": block(
                user_base + total_lead_pnl,
                lead_real,
                lead_unreal,
                lead_live_dd,
                lead_maxdd,
                user_base + total_lead_pnl + lead_live_dd,
                user_base,
            ),
            "copy": block(
                user_base + total_copy_pnl,
                copy_real,
                copy_unreal,
                copy_live_dd,
                copy_maxdd,
                user_base + total_copy_pnl + copy_live_dd,
                user_base,
            ),
            "delta": {
                "equity": round(total_copy_pnl - total_lead_pnl, 8),
                "pct": round(((total_copy_pnl - total_lead_pnl) / user_base * 100.0) if user_base else 0.0, 8),
            },
            "lead_total_pnl": round(total_lead_pnl, 8),
            "copy_total_pnl": round(total_copy_pnl, 8),
            "copy_error": round(total_copy_pnl - total_lead_pnl, 8),
            "entry_count": sa["entry_count"],
            "exit_count": sa["exit_count"],
            "open_position_count": sa["open_position_count"],
            "fill_count": sa["fill_count"],
            "win_rate": sa["win_rate"],
            "pnl_per_trade": sa["pnl_per_trade"],
            "avg_trade_pct": sa["avg_trade_pct"],
            "current_position_usd": sa["current_position_usd"],
            "max_position_usd": sa["max_position_usd"],
            "position_exposure_sum": sa["position_exposure_sum"],
            "position_exposure_samples": sa["position_exposure_samples"],
            "avg_position_usd": sa["avg_position_usd"],
            "avg_entry_notional_usd": sa["avg_entry_notional_usd"],
            "entry_notional_sum": sa["entry_notional_sum"],
            "entry_notional_count": sa["entry_notional_count"],
            "entry_notional_ge10_count": sa["entry_notional_ge10_count"],
            "pct_entries_ge10": sa["pct_entries_ge10"],
            "required_leverage": sa["required_leverage"],
        })
    state = dict(state)
    state["portfolio_history"] = portfolio_history
    proof_window_start, proof_window_end = _portfolio_history_window_ms(portfolio_history)
    true_drawdown_history = build_computed_true_drawdown_history(rows, ui, proof_window_start, proof_window_end)
    apply_user_true_drawdown_rollup(rows, portfolio_wallets, true_drawdown_history, user_base)
    true_curve_counts = true_curve_status_counts(portfolio_wallets)
    state["wallet_rows"] = rows
    state["true_drawdown_history"] = true_drawdown_history
    state["true_curve_status_counts"] = true_curve_counts
    state["portfolio"] = {
        "ts": model_asof,
        "lead": block(lead_equity, lead_real, lead_unreal, lead_live_dd, lead_maxdd, lead_equity + lead_live_dd, alloc),
        "copy": block(copy_equity, copy_real, copy_unreal, copy_live_dd, copy_maxdd, copy_equity + copy_live_dd, alloc),
        "delta": {"equity": round(copy_equity - lead_equity, 8), "realized": round(copy_real - lead_real, 8), "pct": round(((copy_equity - lead_equity) / alloc * 100.0) if alloc else 0.0, 8)},
        "open_notional_usd": round(open_notional_usd, 8),
        "max_open_notional_usd": round(max_open_notional_usd, 8),
        "avg_position_usd": round(sa["avg_position_usd"], 8),
        "max_required_leverage": round(sa["required_leverage"], 8),
        "avg_trade_pct": round(sa["avg_trade_pct"], 8),
        "win_rate": round(sa["win_rate"], 6),
        "avg_entry_notional_usd": round(sa["avg_entry_notional_usd"], 8),
        "pct_entries_ge10": round(sa["pct_entries_ge10"], 8),
        "selected_dd_method": "unified_time_aligned_portfolio_equity_curve",
        "true_curve_status_counts": true_curve_counts,
    }
    return state


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
        "copy_size", "sizing_leader_equity_base", "sizing_leader_equity_source",
        "fee_bps", "base_fee_bps", "copy_friction_bps", "fee", "reconstructed", "trade_id",
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
    with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
        _MODEL_DASHBOARD_HTML_CACHE["html"] = None
        _MODEL_DASHBOARD_HTML_CACHE["built_at"] = 0.0
        _MODEL_DASHBOARD_HTML_CACHE["state_built_at"] = 0.0
        _MODEL_DASHBOARD_HTML_CACHE["disk_blocked"] = True


def invalidate_live_audit_summary_cache() -> None:
    with _AUDIT_SUMMARY_CACHE_LOCK:
        _AUDIT_SUMMARY_CACHE["data"] = None
        _AUDIT_SUMMARY_CACHE["built_at"] = 0.0


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


def _model_cache_snapshot_nonblocking() -> Tuple[Any, float]:
    if _MODEL_BUILD_LOCK.acquire(blocking=False):
        try:
            return _MODEL_CACHE.get("state"), fnum(_MODEL_CACHE.get("built_at"))
        finally:
            _MODEL_BUILD_LOCK.release()
    return _MODEL_CACHE.get("state"), fnum(_MODEL_CACHE.get("built_at"))


def _model_dashboard_html_cache_get(state_built_at: float) -> Optional[str]:
    with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
        html_doc = _MODEL_DASHBOARD_HTML_CACHE.get("html")
        cached_state_built_at = fnum(_MODEL_DASHBOARD_HTML_CACHE.get("state_built_at"))
        if isinstance(html_doc, str) and html_doc and cached_state_built_at == state_built_at:
            return html_doc
    return None


def _model_dashboard_html_cache_latest_get() -> Optional[str]:
    with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
        html_doc = _MODEL_DASHBOARD_HTML_CACHE.get("html")
        if isinstance(html_doc, str) and html_doc:
            return html_doc
    return None


def _model_dashboard_last_good_html_disk_get() -> Optional[str]:
    with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
        if _MODEL_DASHBOARD_HTML_CACHE.get("disk_blocked"):
            return None
    try:
        if not MODEL_DASHBOARD_LAST_GOOD_HTML_FILE.exists():
            return None
        html_doc = MODEL_DASHBOARD_LAST_GOOD_HTML_FILE.read_text(encoding="utf-8")
        if html_doc:
            with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
                _MODEL_DASHBOARD_HTML_CACHE["html"] = html_doc
                _MODEL_DASHBOARD_HTML_CACHE["built_at"] = time.time()
                _MODEL_DASHBOARD_HTML_CACHE["state_built_at"] = 0.0
            return html_doc
    except Exception as exc:
        APP_HEALTH["last_error"] = f"{type(exc).__name__}: {exc}"
    return None


def _model_dashboard_last_good_html_disk_store(html_doc: str) -> None:
    APP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = _unique_tmp_path(MODEL_DASHBOARD_LAST_GOOD_HTML_FILE)
    with _FILE_WRITE_LOCK:
        tmp.write_text(html_doc, encoding="utf-8")
        _replace_with_retries(tmp, MODEL_DASHBOARD_LAST_GOOD_HTML_FILE)


def _model_dashboard_html_cache_store(html_doc: str, state_built_at: float) -> None:
    _model_dashboard_last_good_html_disk_store(html_doc)
    with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
        _MODEL_DASHBOARD_HTML_CACHE["html"] = html_doc
        _MODEL_DASHBOARD_HTML_CACHE["built_at"] = time.time()
        _MODEL_DASHBOARD_HTML_CACHE["state_built_at"] = state_built_at
        _MODEL_DASHBOARD_HTML_CACHE["disk_blocked"] = False


def _render_model_dashboard_html_cached(state: Dict[str, Any], state_built_at: float) -> str:
    cached_html = _model_dashboard_html_cache_get(state_built_at)
    if cached_html is not None:
        print("MODEL_DASHBOARD_HTML_CACHE_HIT", flush=True)
        return cached_html
    html_doc = render_home(dict(state))
    _model_dashboard_html_cache_store(html_doc, state_built_at)
    print("MODEL_DASHBOARD_HTML_CACHE_MISS_RENDERED", flush=True)
    return html_doc


def _kick_model_cache_refresh_background() -> bool:
    global _MODEL_REFRESH_IN_PROGRESS
    with _MODEL_REFRESH_LOCK:
        if _MODEL_REFRESH_IN_PROGRESS:
            return False
        _MODEL_REFRESH_IN_PROGRESS = True
        _MODEL_REFRESH_STATUS.update({
            "in_progress": True,
            "ok": False,
            "error": "",
            "traceback": "",
            "last_marker": "MODEL_DASHBOARD_BACKGROUND_QUEUED",
        })

    def _refresh() -> None:
        global _MODEL_REFRESH_IN_PROGRESS
        with _COHORT_REBUILD_LOCK:
            started = time.time()
            with _MODEL_REFRESH_LOCK:
                _MODEL_REFRESH_STATUS.update({
                    "in_progress": True,
                    "started_at": utc_now_iso(),
                    "finished_at": "",
                    "ok": False,
                    "error": "",
                    "traceback": "",
                    "state_present": bool(_MODEL_CACHE.get("state")),
                    "html_present": bool(_MODEL_DASHBOARD_HTML_CACHE.get("html")),
                    "last_marker": "MODEL_DASHBOARD_BACKGROUND_REBUILD_STARTED",
                })
            try:
                APP_HEALTH["last_build_started_at"] = utc_now_iso()
                # Publish the calculated state before spending several seconds on
                # derived JSON/CSV persistence. Dashboard requests remain served
                # from the previous complete cache throughout this rebuild.
                refreshed = build_model_state(persist=False)
                refreshed_built_at = time.time()
                with _MODEL_BUILD_LOCK:
                    _MODEL_CACHE["state"] = refreshed
                    _MODEL_CACHE["built_at"] = refreshed_built_at
                APP_HEALTH["last_build_finished_at"] = utc_now_iso()
                APP_HEALTH["last_build_seconds"] = round(time.time() - started, 4)
                APP_HEALTH["last_error"] = ""
                APP_HEALTH["build_count"] = int(APP_HEALTH.get("build_count", 0)) + 1
                _render_model_dashboard_html_cached(refreshed, refreshed_built_at)
                persist_model_state(refreshed)
                with _MODEL_REFRESH_LOCK:
                    _MODEL_REFRESH_STATUS.update({
                        "ok": True,
                        "state_present": True,
                        "html_present": bool(_MODEL_DASHBOARD_HTML_CACHE.get("html")),
                        "last_marker": "MODEL_DASHBOARD_HTML_BACKGROUND_PRERENDER_DONE",
                    })
                print("MODEL_DASHBOARD_HTML_BACKGROUND_PRERENDER_DONE", flush=True)
                print("MODEL_DASHBOARD_BACKGROUND_REBUILD_DONE", flush=True)
            except Exception as exc:
                err = f"{type(exc).__name__}: {exc}"
                tb = traceback.format_exc()
                APP_HEALTH["last_error"] = err
                APP_HEALTH["last_build_seconds"] = round(time.time() - started, 4)
                with _MODEL_REFRESH_LOCK:
                    _MODEL_REFRESH_STATUS.update({
                        "ok": False,
                        "error": err,
                        "traceback": tb,
                        "state_present": bool(_MODEL_CACHE.get("state")),
                        "html_present": bool(_MODEL_DASHBOARD_HTML_CACHE.get("html")),
                        "last_marker": "MODEL_DASHBOARD_BACKGROUND_REBUILD_FAILED",
                    })
            finally:
                with _MODEL_REFRESH_LOCK:
                    _MODEL_REFRESH_IN_PROGRESS = False
                    _MODEL_REFRESH_STATUS["in_progress"] = False
                    _MODEL_REFRESH_STATUS["finished_at"] = utc_now_iso()

    threading.Thread(target=_refresh, name="hl-model-cache-refresh", daemon=True).start()
    return True


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
    attr = f' data-field="{html.escape(field)}"' if field else ""
    if active_wallet(row) and (not present or not has_num(sort_value)):
        return f'<td class="{cls} neg" data-sort="-999999999"{attr}>{core_missing(row, field)}</td>'
    return f'<td class="{cls}" data-sort="{fnum(sort_value):.12g}"{attr}>{content}</td>'


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


def _dd_abs(v: Any) -> Optional[float]:
    if not has_num(v):
        return None
    return abs(fnum(v))


def _mtm_stale_flag(source: Any) -> bool:
    return "stale" in str(source or "").lower()


def get_effective_max_dd(perf: Optional[Dict[str, Any]], realised_dd: Any = None, alloc: Any = None, allow_realised_fallback: bool = True) -> Dict[str, Any]:
    """Prefer HL accountValueHistory MTM drawdown; realised closedPnl is diagnostic fallback only."""
    perf = perf if isinstance(perf, dict) else {}
    source = str(perf.get("mtm_source") or "")
    for field, label in (
        ("allTime_max_drawdown_mtm", "allTime MTM"),
        ("max_drawdown_mtm", "30d MTM"),
    ):
        value = _dd_abs(perf.get(field))
        if value is not None:
            base = fnum(alloc)
            return {
                "value_usd": round(-value, 8),
                "value_pct": round((-value / base * 100.0) if base else 0.0, 8),  # base = fnum(alloc) is UI norm base; leader MTM pct may differ from accountValueHistory peak basis
                "source": f"{label} accountValueHistory ({source or 'unknown'})",
                "source_key": label,
                "stale": _mtm_stale_flag(source),
                "fallback_reason": "",
            }
    if not allow_realised_fallback:
        return {
            "value_usd": None,
            "value_pct": None,
            "source": f"NO MTM / DATA BLOCKED ({source or 'no live_leader_performance MTM'})",
            "source_key": "BLOCKED",
            "stale": True,
            "fallback_reason": "MTM accountValueHistory DD unavailable; realised fallback suppressed",
        }
    realised = _dd_abs(realised_dd if realised_dd is not None else perf.get("max_drawdown_realised", perf.get("max_drawdown")))
    if realised is not None:
        base = fnum(alloc)
        return {
            "value_usd": round(-realised, 8),
            "value_pct": round((-realised / base * 100.0) if base else 0.0, 8),
            "source": "realised fallback (closedPnl diagnostic)",
            "source_key": "realised fallback",
            "stale": False,
            "fallback_reason": "MTM accountValueHistory DD unavailable",
        }
    return {
        "value_usd": None,
        "value_pct": None,
        "source": f"unavailable ({source or 'no live_leader_performance MTM'})",
        "source_key": "unavailable",
        "stale": _mtm_stale_flag(source),
        "fallback_reason": "No MTM or realised DD available",
    }


def _perf_has_alltime(perf: Dict[str, Any]) -> bool:
    return any(isinstance(v, dict) and has_num(v.get("allTime_max_drawdown_mtm")) for v in perf.values())


def _live_leader_performance_from_last_good(min_entries: int = 90) -> Dict[str, Any]:
    with _AUDIT_SUMMARY_CACHE_LOCK:
        cached_payload = _AUDIT_SUMMARY_CACHE.get("data") if isinstance(_AUDIT_SUMMARY_CACHE.get("data"), dict) else {}
    cached_perf = cached_payload.get("live_leader_performance") if isinstance(cached_payload, dict) else {}
    if isinstance(cached_perf, dict) and len(cached_perf) >= min_entries and _perf_has_alltime(cached_perf):
        return cached_perf
    payload = _load_live_audit_summary_last_good()
    perf = payload.get("live_leader_performance") if isinstance(payload, dict) else {}
    if isinstance(perf, dict) and len(perf) >= min_entries and _perf_has_alltime(perf):
        return perf
    return perf if isinstance(perf, dict) else (cached_perf if isinstance(cached_perf, dict) else {})


def enrich_rows_with_effective_dd(rows: List[Dict[str, Any]], perf_by_wallet: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    perf_by_wallet = perf_by_wallet if isinstance(perf_by_wallet, dict) else _live_leader_performance_from_last_good()
    enriched: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            enriched.append(row)
            continue
        r = dict(row)
        wallet = str(r.get("wallet", "")).lower()
        alloc = fnum(r.get("alloc"), DEFAULT_NORM_BASE)
        lead = r.get("lead") if isinstance(r.get("lead"), dict) else {}
        copy = r.get("copy") if isinstance(r.get("copy"), dict) else {}
        lead_realised = get_max_dd(lead, r.get("curve", []) if r.get("is_user_wallet") else None, "lead" if r.get("is_user_wallet") else None)
        copy_realised = get_max_dd(copy, r.get("curve", []) if r.get("is_user_wallet") else None, "copy" if r.get("is_user_wallet") else None)
        # LEAD: monitor-period model DD only. Do not pre-populate new wallets
        # with historical/all-time accountValueHistory drawdown from before the
        # clean monitor baseline.
        lead_model_maxdd_raw = lead.get("max_drawdown") if has_num(lead.get("max_drawdown")) else (lead.get("maxdd") if has_num(lead.get("maxdd")) else r.get("lead_maxdd"))
        lead_model_maxdd = _dd_abs(lead_model_maxdd_raw)
        if lead_model_maxdd is not None:
            lead_base = fnum(lead.get("peak") or lead.get("peak_equity")) or fnum(alloc)
            lead_eff = {
                "value_usd": round(-lead_model_maxdd, 8),
                "value_pct": round((-lead_model_maxdd / lead_base * 100.0) if lead_base else 0.0, 8),
                "source": "monitor-period lead model DD",
                "source_key": "monitor period",
                "stale": False,
                "fallback_reason": "",
            }
        else:
            lead_eff = {
                "value_usd": 0.0,
                "value_pct": 0.0,
                "source": "monitor-period lead model DD unavailable; no captured fills yet",
                "source_key": "monitor period",
                "stale": False,
                "fallback_reason": "No monitor-period equity curve yet",
            }
        # COPY: derive from copy model equity curve DD, NOT leader MTM
        copy_model_maxdd_raw = copy.get("max_drawdown") if has_num(copy.get("max_drawdown")) else (copy.get("maxdd") if has_num(copy.get("maxdd")) else r.get("copy_maxdd"))
        copy_model_maxdd = _dd_abs(copy_model_maxdd_raw)
        if copy_model_maxdd is not None:
            copy_base = fnum(copy.get("peak") or copy.get("peak_equity")) or fnum(alloc)
            copy_eff = {
                "value_usd": round(-copy_model_maxdd, 8),
                "value_pct": round((-copy_model_maxdd / copy_base * 100.0) if copy_base else 0.0, 8),
                "source": "copy model equity curve DD",
                "source_key": "copy model",
                "stale": False,
                "fallback_reason": "",
            }
        else:
            copy_eff = {
                "value_usd": None,
                "value_pct": None,
                "source": "COPY MODEL DD BLOCKED",
                "source_key": "BLOCKED",
                "stale": True,
                "fallback_reason": "copy model equity curve unavailable",
            }
        r["lead_maxdd_effective_usd"] = lead_eff.get("value_usd")
        r["lead_maxdd_effective_pct"] = lead_eff.get("value_pct")
        r["lead_maxdd_effective_source"] = lead_eff.get("source")
        r["lead_maxdd_effective_source_key"] = lead_eff.get("source_key")
        r["lead_maxdd_effective_stale"] = lead_eff.get("stale")
        r["lead_maxdd_effective_fallback_reason"] = lead_eff.get("fallback_reason")
        r["copy_maxdd_effective_usd"] = copy_eff.get("value_usd")
        r["copy_maxdd_effective_pct"] = copy_eff.get("value_pct")
        r["copy_maxdd_effective_source"] = copy_eff.get("source")
        r["copy_maxdd_effective_source_key"] = copy_eff.get("source_key")
        r["copy_maxdd_effective_stale"] = copy_eff.get("stale")
        r["copy_maxdd_effective_fallback_reason"] = copy_eff.get("fallback_reason")
        r["realised_maxdd_diagnostic_usd"] = round(-max(lead_realised, copy_realised), 8)
        r["effective_dd_contract"] = {"lead": lead_eff, "copy": copy_eff, "perf_source": "monitor-period model rows"}
        enriched.append(r)
    return enriched


DASHBOARD_CELL_CONTRACT = {
    "header_pnl": "portfolio lead/copy total pnl = equity - alloc; delta = copy - lead",
    "header_realised": "portfolio lead/copy realised = sum included non-user row lead/copy realised",
    "header_unrealised": "portfolio lead/copy unrealised = sum included non-user row lead/copy unrealised",
    "header_model_drawdown": "lead/copy current DD = current peak - current equity; lead/copy max DD = max historical drawdown/current DD",
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
        # Live current stats: validate against the rendered portfolio history
        # tail. refresh_selected_portfolio_view appends a live snapshot after
        # building the row-curve history, and the header/graph both read that
        # snapshot. Recomputing from row curves alone can lag the live tail.
        curve_stats = selected_combined_curve_stats(selected_rows, ui)
        portfolio_history = state.get("portfolio_history") if isinstance(state.get("portfolio_history"), list) else []
        live_tail = portfolio_history[-1] if portfolio_history else {}
        live_tail_lead = live_tail.get("lead") if isinstance(live_tail.get("lead"), dict) else {}
        live_tail_copy = live_tail.get("copy") if isinstance(live_tail.get("copy"), dict) else {}
        live_lead_dd = fnum(live_tail_lead.get("drawdown"), curve_stats["current_lead_dd"])
        live_copy_dd = fnum(live_tail_copy.get("drawdown"), curve_stats["current_copy_dd"])
        if abs(fnum(port_lead.get("drawdown")) - live_lead_dd) > 0.01:
            errors.append(f"portfolio lead DD {fnum(port_lead.get('drawdown')):.4f} != rendered portfolio peak-to-current {live_lead_dd:.4f}")
        if abs(fnum(port_copy.get("drawdown")) - live_copy_dd) > 0.01:
            errors.append(f"portfolio copy DD {fnum(port_copy.get('drawdown')):.4f} != rendered portfolio peak-to-current {live_copy_dd:.4f}")
        # Historical max stats: portfolio maxDD and max exposure equal max timestamped sum (live snapshot is tail).
        if curve_stats["has_points"]:
            expected_max_lead_dd = max(curve_stats["max_lead_dd"], live_lead_dd)
            expected_max_copy_dd = max(curve_stats["max_copy_dd"], live_copy_dd)
            expected_max_exposure = max(curve_stats["max_exposure"], sel_notional)
            # Portfolio MaxDD is computed from the unified live portfolio curve;
            # this secondary contract recomputes from selected row curves. After
            # a header mode rebuild the row-curve tail can differ by a small
            # timestamp-alignment amount, so use a portfolio-level tolerance.
            maxdd_tol = max(0.05, abs(expected_max_lead_dd) * 0.02, abs(expected_max_copy_dd) * 0.02)
            if not contract_money_equal(port_lead.get("max_drawdown"), expected_max_lead_dd, tolerance=maxdd_tol):
                errors.append(f"portfolio lead MaxDD {fnum(port_lead.get('max_drawdown')):.4f} != max time-aligned portfolio DD {expected_max_lead_dd:.4f}")
            if not contract_money_equal(port_copy.get("max_drawdown"), expected_max_copy_dd, tolerance=maxdd_tol):
                errors.append(f"portfolio copy MaxDD {fnum(port_copy.get('max_drawdown')):.4f} != max time-aligned portfolio DD {expected_max_copy_dd:.4f}")
            exposure_tol = max(0.05, abs(expected_max_exposure) * 0.025)
            if abs(fnum(port.get("max_open_notional_usd")) - expected_max_exposure) > exposure_tol:
                errors.append(f"portfolio max exposure {fnum(port.get('max_open_notional_usd')):.4f} != max timestamped summed exposure {expected_max_exposure:.4f}")
            expected_req_lev = expected_max_exposure / user_base if user_base else 0.0
            if abs(fnum(port.get("max_required_leverage")) - expected_req_lev) > (exposure_tol / user_base if user_base else 0.001):
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
            if abs(hdr_req_lev - usr_req_lev) > max(0.001, abs(hdr_req_lev) * 0.001):
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


def render_chart(history: List[Dict[str, Any]], true_drawdown_history: Optional[List[Dict[str, Any]]] = None, true_curve_counts: Optional[Dict[str, Any]] = None) -> str:
    """Render copy PnL, realised PnL, model DD and reconstructed realised DD."""
    if not history:
        return '<div class="chart-empty">Awaiting data…</div>'
    counts = true_curve_counts or {}
    valid_count = inum(counts.get("valid"))
    stale_count = inum(counts.get("stale"))
    missing_count = inum(counts.get("missing"))
    total_count = inum(counts.get("total"))
    true_dd_blocked = bool(stale_count or missing_count)
    blocked_msg = f"RECONSTRUCTED DD UNAVAILABLE: {valid_count} valid, {stale_count} stale, {missing_count} missing" if true_dd_blocked else ""

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

    true_raw_points: List[Dict[str, Any]] = []
    for p in ([] if true_dd_blocked else (true_drawdown_history or [])):
        ts = str(p.get("ts", ""))
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except Exception:
            t = fnum(p.get("timestamp_ms")) / 1000 if fnum(p.get("timestamp_ms")) else float(len(true_raw_points))
        true_raw_points.append({
            "ts": ts,
            "t": t,
            "dd": fnum(p.get("drawdown")),
            "equity": fnum(p.get("equity")),
            "wallet_count": inum(p.get("wallet_count")),
        })

    def compress_true_points(points: List[Dict[str, Any]], max_points: int = CHART_POINT_MAX) -> List[Dict[str, Any]]:
        if len(points) <= max_points:
            return points
        step = max(1, len(points) // max_points)
        kept = points[::step]
        required = [points[0], min(points, key=lambda x: fnum(x.get("dd"))), points[-1]]
        by_t: Dict[float, Dict[str, Any]] = {}
        for point in kept + required:
            by_t[fnum(point.get("t"))] = point
        return [by_t[t] for t in sorted(by_t)]

    pts_data = compress_points(raw_points, CHART_POINT_MAX)
    true_pts_data = compress_true_points(true_raw_points, CHART_POINT_MAX)

    if len(pts_data) == 1:
        first = dict(pts_data[0])
        pts_data = [dict(first, t=first["t"] - 1.0, pnl=0.0, realized=0.0, dd=0.0, equity=first["equity"] - first["pnl"]), first]

    chart_times = [fnum(p["t"]) for p in pts_data] + [fnum(p.get("t")) for p in true_pts_data]
    ts_min = min(chart_times)
    ts_max = max(chart_times)
    if abs(ts_max - ts_min) < 1e-9:
        ts_max = ts_min + 1.0

    pnl_vals = [fnum(p["pnl"]) for p in pts_data]
    realized_vals = [fnum(p["realized"]) for p in pts_data]
    dd_vals = [-fnum(p["dd"]) for p in pts_data]
    true_dd_vals = [fnum(p.get("dd")) for p in true_pts_data]
    all_vals = pnl_vals + realized_vals + dd_vals + true_dd_vals + [0.0]
    mn, mx = min(all_vals), max(all_vals)
    if abs(mx - mn) < 1e-9:
        mx += 1.0
        mn -= 1.0

    w, h = 1200, 260
    pad_l, pad_r, pad_t, pad_b = 62, 18, 18, 34
    plot_w, plot_h = w - pad_l - pad_r, h - pad_t - pad_b

    def x_for_t(t: float) -> float:
        return pad_l + plot_w * ((t - ts_min) / max(1e-9, ts_max - ts_min))

    def x_at(i: int) -> float:
        return x_for_t(fnum(pts_data[i]["t"]))

    def y_at(v: float) -> float:
        return pad_t + plot_h * (1 - ((v - mn) / (mx - mn)))

    def poly(vals: List[float]) -> str:
        return " ".join(f"{x_at(i):.1f},{y_at(v):.1f}" for i, v in enumerate(vals))

    def poly_points(points: List[Dict[str, Any]], key: str) -> str:
        return " ".join(f"{x_for_t(fnum(p.get('t'))):.1f},{y_at(fnum(p.get(key))):.1f}" for p in points)

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
    true_dd_poly = poly_points(true_pts_data, "dd") if true_pts_data else ""
    true_dd_final = fnum(true_pts_data[-1].get("dd")) if true_pts_data else 0.0
    true_dd_min = min((fnum(p.get("dd")) for p in true_pts_data), default=0.0)
    model_range_start = min((fnum(p.get("t")) for p in pts_data), default=0.0)
    model_range_end = max((fnum(p.get("t")) for p in pts_data), default=0.0)
    true_range_start = min((fnum(p.get("t")) for p in true_pts_data), default=0.0)
    true_range_end = max((fnum(p.get("t")) for p in true_pts_data), default=0.0)
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

    true_polyline = f'<polyline points="{true_dd_poly}" class="true-dd-line"/>' if true_pts_data else ""
    blocked_overlay = (
        f'<div class="chart-blocked-overlay"><b>{html.escape(blocked_msg)}</b><span>Reconstructed realised drawdown is unavailable until every included wallet has a valid equity_curves CSV. Model lines remain visible.</span></div>'
        if true_dd_blocked
        else ""
    )
    true_source_attr = "blocked:data/equity_curves:proof_window" if true_dd_blocked else "data/equity_curves:proof_window"
    true_status_attr = "BLOCKED" if true_dd_blocked else "VALID"
    true_legend = "Reconstructed realised DD unavailable" if true_dd_blocked else "Reconstructed realised DD"

    return f"""
    <div class="chart-wrap{' true-dd-blocked' if true_dd_blocked else ''}" data-true-dd-source="{true_source_attr}" data-true-dd-status="{true_status_attr}" data-true-valid="{valid_count}" data-true-stale="{stale_count}" data-true-missing="{missing_count}" data-true-total="{total_count}" data-true-dd-final="{true_dd_final:.12f}" data-true-dd-min="{true_dd_min:.12f}" data-true-dd-points="{len(true_pts_data)}" data-model-points="{len(pts_data)}" data-true-range-start="{model_range_start if not true_pts_data else true_range_start:.3f}" data-true-range-end="{model_range_end if not true_pts_data else true_range_end:.3f}" data-model-range-start="{model_range_start:.3f}" data-model-range-end="{model_range_end:.3f}">
      {blocked_overlay}
      <svg class="chart" viewBox="0 0 {w} {h}" preserveAspectRatio="none">
        {grid}
        <line x1="{pad_l}" y1="{zero_y:.1f}" x2="{w-pad_r}" y2="{zero_y:.1f}" class="zero-line"/>
        <polyline points="{poly(pnl_vals)}" class="pnl-line"/>
        <polyline points="{poly(realized_vals)}" class="realized-line"/>
        <polyline points="{poly(dd_vals)}" class="dd-line"/>
        {true_polyline}
        <line id="chartCrossX" x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{h-pad_b}" class="crosshair" style="display:none"/>
        <circle id="chartDot" cx="{pad_l}" cy="{zero_y:.1f}" r="4" class="chart-dot" style="display:none"/>
        {hits}
      </svg>
      <div class="chart-legend"><span class="legend-pnl">Copy PnL</span><span class="legend-realized">Realised</span><span class="legend-dd">Copy model DD</span><span class="legend-true-dd">{html.escape(true_legend)}</span><span class="muted">Hover for values · click chart to expand</span></div>
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
            "lead_maxdd": fnum(row.get("lead_maxdd_effective_usd"), -block_num(lead, "max_drawdown", "maxdd", default=0.0)),
            "lead_max_drawdown": fnum(row.get("lead_maxdd_effective_usd"), -block_num(lead, "max_drawdown", "maxdd", default=0.0)),
            "true_ts_dd_now_usd": fnum(row.get("true_ts_dd_now_usd")),
            "true_ts_dd_now_pct": fnum(row.get("true_ts_dd_now_pct")),
            "true_ts_max_dd_usd": fnum(row.get("true_ts_max_dd_usd")),
            "true_ts_max_dd_pct": fnum(row.get("true_ts_max_dd_pct")),
            "all_time_true_max_dd_usd": fnum(row.get("all_time_true_max_dd_usd")),
            "all_time_true_max_dd_pct": fnum(row.get("all_time_true_max_dd_pct")),
            "true_dd_status": 0 if str(row.get("true_ts_status")) == "ok" else -1,
            "copy_equity": copy_pnl,
            "copy_real": fnum(copy.get("realized")),
            "copy_realized": fnum(copy.get("realized")),
            "copy_unreal": fnum(copy.get("unrealized")),
            "copy_unrealized": fnum(copy.get("unrealized")),
            "copy_dd": -block_num(copy, "drawdown", "drawdown_usd", default=0.0),
            "copy_drawdown": -block_num(copy, "drawdown", "drawdown_usd", default=0.0),
            "copy_maxdd": fnum(row.get("copy_maxdd_effective_usd"), -block_num(copy, "max_drawdown", "maxdd", default=0.0)),
            "copy_max_drawdown": fnum(row.get("copy_maxdd_effective_usd"), -block_num(copy, "max_drawdown", "maxdd", default=0.0)),
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
            "last_trade_timestamp_ms": fnum(row.get("last_trade_timestamp_ms")),
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
    equity_refresh = state.get("cohort_equity_refresh") or {}
    equity_refresh_notice = ""
    if equity_refresh:
        equity_refresh_notice = html.escape(
            f"Equity refreshed: {equity_refresh['refreshed']}/{equity_refresh['selected']} selected; "
            f"{equity_refresh['locked']} locked; {equity_refresh['failed']} failed"
            + (f"; {equity_refresh['skipped']} deselected/excluded" if equity_refresh.get('skipped') else "")
            + (" (previous equity retained)" if equity_refresh['failed'] else "")
        )
    ui = load_ui_state()
    state["ui_state"] = ui
    state["wallet_rows"] = enrich_rows_with_effective_dd(state.get("wallet_rows", []))
    state["wallets"] = {str(r.get("wallet", "")): r for r in state.get("wallet_rows", []) if isinstance(r, dict)}
    state = refresh_selected_portfolio_view(state, ui)
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
    true_counts = state.get("true_curve_status_counts") if isinstance(state.get("true_curve_status_counts"), dict) else {}
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
        group_card("MODEL DRAWDOWN", [small_metric("LEAD CURRENT", format_dd(lead_dd, user_base), -lead_dd), small_metric("COPY CURRENT", format_dd(copy_dd, user_base), -copy_dd), small_metric("LEAD MAX", format_dd(lead_maxdd, user_base), -lead_maxdd), small_metric("COPY MAX", format_dd(copy_maxdd, user_base), -copy_maxdd)]),
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
        th("lead_equity", "LEAD MODEL EQ", "pair-lead"), th("copy_equity", "COPY MODEL EQ", "pair-copy group-divider"),
        th("lead_real", "LEAD REAL", "pair-lead"), th("copy_real", "COPY REAL", "pair-copy group-divider"),
        th("lead_unreal", "LEAD UNREAL", "pair-lead"), th("copy_unreal", "COPY UNREAL", "pair-copy group-divider"),
        th("lead_dd", "LEAD DD NOW", "pair-lead"), th("copy_dd", "COPY DD NOW", "pair-copy group-divider"),
        th("lead_maxdd", "LEAD MAX DD", "pair-lead"), th("copy_maxdd", "COPY MAX DD", "pair-copy group-divider"),
        th("delta", "Δ $/%", "group-divider"), th("pnl_per_hour", "PNL/HR"), th("avg_trade_pct", "AVG TRADE %"), th("win_rate", "WIN%"),
        th("avg_position_usd", "AVG POS $"), th("max_position_usd", "MAX POS $"), th("avg_entry_notional_usd", "AVG NOTIONAL"), th("pct_entries_ge10", "% ≥ MIN"), th("required_leverage", "REQ LEV"),
        th("last_trade_timestamp_ms", "LAST TRADE", "ops-group"), th("fill_count", "FILLS L/C", "ops-group"), th("exit_count", "EXITS L/C", "ops-group"), th("open_position_count", "POS L/C", "ops-group group-divider"),
    ]) + "<th>SETTINGS</th></tr>"
    # Split rows into modelled (shown in main table) and app-model-excluded (shown separately)
    # FILTER_FIX_V3_ACTIVE
    ui_excl = ui.get("wallet_model_exclude", {})
    def is_excl(w: str) -> bool:
        w_low = str(w).strip().lower()
        if w_low == USER_WALLET.lower(): return False
        return bool(ui_excl.get(w_low)) or bool(ui_excl.get(w))

    model_excl_rows = [r for r in metric_rows if is_excl(r.get("wallet", ""))]
    modelled_count = len(metric_rows) - len(model_excl_rows)
    engine_count = len(metric_rows)
    excl_count = len(model_excl_rows)
    
    body_parts: List[str] = []
    inserted_divider = False
    has_included_group = any(not r.get("is_user_wallet") and not is_excl(r.get("wallet","")) and wallet_included(str(r.get("wallet", "")), ui) for r in metric_rows)
    has_other_group = any(not r.get("is_user_wallet") and not is_excl(r.get("wallet","")) and not wallet_included(str(r.get("wallet", "")), ui) for r in metric_rows)
    for r in rows:
        if not r.get("is_user_wallet") and is_excl(r.get("wallet", "")):
            continue  # shown in excluded_section below
        if has_included_group and has_other_group and not inserted_divider and not r.get("is_user_wallet") and not wallet_included(str(r.get("wallet", "")), ui):
            body_parts.append('<tr class="selected-divider"><td colspan="99">Other tracked wallets</td></tr>')
            inserted_divider = True
        body_parts.append(render_row(r, base, ui, state))
    body_rows = "\n".join(body_parts)

    # Build filter panel (collapsible, above table)
    saved_filters = ui.get("wallet_filters") or {}
    def _fv(key: str) -> str:
        v = saved_filters.get(key)
        return f' value="{html.escape(str(v))}"' if v is not None else ''
    _fi = lambda k: f'<input name="{k}" size="7" placeholder="—"{_fv(k)}>'
    filter_rows_html = "".join([
        f'<span class="frow"><b>WALLET</b> contains: <input name="wallet_contains" size="14"{_fv("wallet_contains")}></span>',
        "".join(
            f'<span class="frow"><b>{lbl}</b> min:{_fi(f"{k}_min")} max:{_fi(f"{k}_max")}</span>'
            for k, lbl in [
                ("lead_equity","LEAD EQ"), ("copy_equity","COPY EQ"),
                ("lead_real","LEAD REAL"), ("copy_real","COPY REAL"),
                ("lead_unreal","LEAD UNREAL"), ("copy_unreal","COPY UNREAL"),
                ("lead_dd","LEAD DD NOW"), ("copy_dd","COPY DD NOW"),
                ("lead_maxdd","LEAD MAX DD"), ("copy_maxdd","COPY MAX DD"),
                ("delta","Δ $"), ("pnl_per_hour","PNL/HR"),
                ("avg_trade_pct","AVG TRADE %"), ("win_rate","WIN%"),
                ("avg_position_usd","AVG POS $"), ("max_position_usd","MAX POS $"),
                ("avg_entry_notional_usd","AVG NOTIONAL"), ("pct_entries_ge10","% ≥ MIN"),
                ("required_leverage","REQ LEV"),
                ("last_trade_timestamp_ms","LAST TRADE"), ("fill_count","FILLS"), ("exit_count","EXITS"), ("open_position_count","POS"),
            ]
        ),
    ])
    filter_summary_css = ' style="color:#d29922"' if excl_count else ''
    filter_summary = f'<span{filter_summary_css}>Showing: <b>{modelled_count}</b> / <b>{engine_count}</b> wallets | App-excluded: <b>{excl_count}</b> | Engine still tracking all</span>'
    last_result = ui.get("wallet_filter_last_result") if isinstance(ui.get("wallet_filter_last_result"), dict) else {}
    if last_result.get("updated_at"):
        result_html = (
            f'<span class="muted small" style="margin-left:8px">'
            f'Last filter run: evaluated <b>{inum(last_result.get("evaluated_count"))}</b>, '
            f'excluded <b>{inum(last_result.get("excluded_count"))}</b>, '
            f'updated {html.escape(str(last_result.get("updated_at")))}</span>'
        )
    else:
        result_html = '<span class="muted small" style="margin-left:8px">Last filter run: none</span>'
    filter_panel = f"""<div class="filter-panel"><details><summary class="filter-summary">{filter_summary} — <span class="muted">App Model Filters</span></summary>
<form action="/api/wallet-filters" method="post" class="filter-form">
<div class="filter-grid">{filter_rows_html}</div>
<div class="filter-actions">
  <button type="submit" name="action" value="apply" class="apply-filter-btn" title="Wallets matching/outside filter criteria will be hidden from dashboard/app model refresh (reversible)">APPLY FILTERS TO EXCLUDE</button>
  <button type="submit" name="action" value="clear" class="clear-filter-btn" title="Clear saved filter values and restore filter-excluded wallets">CLEAR FILTERS</button>
</div>
</form>
<form action="/api/wallet-model-restore-all" method="post" style="display:inline;margin-left:8px">
  <button class="restore-btn" title="Restore all app-model-excluded wallets">RESTORE ALL EXCLUDED</button>
</form>
{result_html}
<div class="muted small" style="margin-top:6px">Clear filters restores filter-excluded wallets. Engine continues tracking all wallets.</div>
<div class="muted small">Filters hide wallets from dashboard/app model refresh only. Engine continues tracking all wallets. Engine, raw fills, wallet gate and live config are not affected.</div>
</details></div>"""

    # Build excluded wallets section (shown below table)
    if model_excl_rows:
        excl_rows_html = "".join(
            f'<tr><td class="mono">{html.escape(str(r.get("wallet",""))[:8])}…{html.escape(str(r.get("wallet",""))[-6:])}</td>'
            f'<td class="mono muted" style="font-size:10px">{html.escape(str(r.get("wallet","")))}</td>'
            f'<td><form action="/api/wallet-model-exclude" method="post" class="excl-form">'
            f'<input type="hidden" name="wallet" value="{html.escape(str(r.get("wallet","")))}"><input type="hidden" name="excluded" value="0">'
            f'<button class="restore-btn">RESTORE</button></form></td></tr>'
            for r in model_excl_rows
        )
        excluded_section = f"""<div class="excluded-section"><details><summary class="excl-summary">APP-EXCLUDED WALLETS ({excl_count}) — engine still tracking; excluded from model refresh only</summary>
<table class="excl-table"><thead><tr><th>Wallet</th><th>Address</th><th>Action</th></tr></thead><tbody>{excl_rows_html}</tbody></table>
</details></div>"""
    else:
        excluded_section = ""

    contract_errs = validate_render_contract(state)
    banner = ""
    if contract_errs:
        msgs = " | ".join(html.escape(e) for e in contract_errs[:10])
        banner = f'<div style="background:#3d1515;border:1px solid #f44;color:#f88;padding:8px 14px;font-size:11px;position:sticky;top:48px;z-index:3"><b>&#9888; CONTRACT WARNING ({len(contract_errs)} errors):</b> {msgs}</div>'
    _saved_mode = str(ui.get("copy_mode", "proportional")).lower()
    prop_selected = "selected" if _saved_mode == "proportional" else ""
    fixed_selected = "selected" if _saved_mode == "fixed" else ""
    min_checked = "checked" if parse_bool(ui.get("min_trade_notional_enabled", False)) else ""
    return banner + HTML_TEMPLATE.format(updated=state.get("updated_at", ""), source_file=APP_SOURCE_FILE, norm=base, mode=_saved_mode.upper(), fixed=fnum(ui.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL), min_checked=min_checked, fee=fnum(ui.get("fee_bps"), DEFAULT_FEE_BPS), friction=fnum(ui.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS), cards=cards, chart=render_chart(state.get("portfolio_history", []), state.get("true_drawdown_history", []), true_counts), wallet_count=modelled_count, table_head=table_head, table_rows=body_rows, raw_boundary=state.get("engine_truth_boundary", ""), filter_panel=filter_panel, excluded_section=excluded_section, equity_refresh_notice=equity_refresh_notice, prop_selected=prop_selected, fixed_selected=fixed_selected, LAST_TRADE_SSE_SCRIPT=LAST_TRADE_SSE_SCRIPT)


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
    lead_realised_diag_raw = lead_maxdd_raw
    copy_realised_diag_raw = copy_maxdd_raw
    lead_eff_raw = _dd_abs(r.get("lead_maxdd_effective_usd"))
    copy_eff_raw = _dd_abs(r.get("copy_maxdd_effective_usd"))
    if lead_eff_raw is not None:
        lead_maxdd_raw = lead_eff_raw
    if copy_eff_raw is not None:
        copy_maxdd_raw = copy_eff_raw
    lead_dd_val = -lead_dd_raw; lead_maxdd_val = -lead_maxdd_raw
    copy_dd_val = -copy_dd_raw; copy_maxdd_val = -copy_maxdd_raw
    lead_maxdd_source = str(r.get("lead_maxdd_effective_source") or "BLOCKED")
    copy_maxdd_source = str(r.get("copy_maxdd_effective_source") or "BLOCKED")
    lead_maxdd_label = str(r.get("lead_maxdd_effective_source_key") or "BLOCKED")
    copy_maxdd_label = str(r.get("copy_maxdd_effective_source_key") or "BLOCKED")
    lead_maxdd_title = html.escape(f"Effective MaxDD source: {lead_maxdd_source}. Realised closedPnl diagnostic: {money(-lead_realised_diag_raw)}")
    copy_maxdd_title = html.escape(f"Effective MaxDD source: {copy_maxdd_source}. Realised closedPnl diagnostic: {money(-copy_realised_diag_raw)}")
    last_trade_age, last_trade_cls, last_trade_title = age_ms_label(r.get("last_trade_timestamp_ms"))
    eff_mode = str(r.get("effective_copy_mode") or ui.get("copy_mode", "proportional"))
    eff_base = fnum(r.get("effective_norm_base"), fnum(r.get("alloc"), base)); eff_fixed = fnum(r.get("effective_fixed_notional"), fnum(ui.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL)); eff_leader_base = fnum(r.get("effective_leader_equity_base"), eff_base)
    eff_leader_text = f"{eff_leader_base:.4f}".rstrip("0").rstrip(".")
    if abs(eff_leader_base) >= 1_000_000:
        eff_leader_short = f"${eff_leader_base / 1_000_000:.2f}m"
    elif abs(eff_leader_base) >= 1_000:
        eff_leader_short = f"${eff_leader_base / 1_000:.1f}k"
    else:
        eff_leader_short = f"${eff_leader_base:,.2f}"
    override_badge = " *" if r.get("wallet_config") else ""; included = bool(r.get("include_in_portfolio", True))
    lc_counts = wallet_lead_copy_counts(r, state)
    no_closed_trades = inum(r.get("exit_count"), 0) == 0
    pnl_hr_cell = core_td(r, 'pnl_per_hour' in r, r.get('pnl_per_hour'), dual(fnum(r.get('pnl_per_hour')), alloc), css_class(r.get('pnl_per_hour')), "pnl_per_hour")
    if no_closed_trades and fnum(r.get("active_hours")) <= 0:
        pnl_hr_cell = dash_td(css_class(r.get('pnl_per_hour')), "pnl_per_hour")
    avg_trade_pct_cell = dash_td(css_class(r.get('avg_trade_pct')), "avg_trade_pct") if no_closed_trades else core_td(r, 'avg_trade_pct' in r, r.get('avg_trade_pct'), pct(r.get('avg_trade_pct'), 3), css_class(r.get('avg_trade_pct')), "avg_trade_pct")
    win_rate_cell = dash_td(css_class(r.get('win_rate')), "win_rate") if no_closed_trades else core_td(r, 'win_rate' in r, r.get('win_rate'), pct(r.get('win_rate')), css_class(r.get('win_rate')), "win_rate")
    _delta_eq_raw = delta.get('equity') if isinstance(delta, dict) else None
    _delta_data = isinstance(delta, dict) and 'equity' in delta
    _lead_has_pnl = has_num(lead.get('realized')) or has_num(lead.get('unrealized'))
    if _delta_data and abs(fnum(_delta_eq_raw)) < 1e-8 and _lead_has_pnl:
        _delta_cell = f'<td class="group-divider zero muted" data-sort="0" data-field="delta.equity" title="Copy tracks leader model exactly — no execution drag modeled. Set copy friction bps &gt; 0 to model slippage.">—</td>'
    else:
        _delta_cell = core_td(r, _delta_data, _delta_eq_raw, dual(fnum(_delta_eq_raw), alloc), f"group-divider {css_class(_delta_eq_raw)}", "delta.equity")
    inc_cell = ""
    cfg_cell = ""
    purge_cell = ""
    equity_refresh_cell = ""
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
        inc_cell = f"""<form action="/api/wallet-include" method="post" class="inc-form" title="INC = include in combined graph and header cards only (not model exclusion)"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><input type="checkbox" name="included" value="1" {'checked' if included else ''}><span class="small">INC</span></form>"""
        wallet_cfg = r.get("wallet_config") if isinstance(r.get("wallet_config"), dict) else {}
        cfg_mode = str(wallet_cfg.get("copy_mode") or "").lower()
        leader_base_locked = parse_bool(wallet_cfg.get("leader_equity_base_locked", False))
        source_age_seconds = r.get("effective_leader_equity_base_source_age_seconds")
        source_age_text = "unknown age"
        if isinstance(source_age_seconds, (int, float)):
            source_age_text = f"{source_age_seconds / 86400:.1f}d old" if source_age_seconds >= 86400 else f"{source_age_seconds / 3600:.1f}h old"
        source_ts_ms = inum(r.get("effective_leader_equity_base_source_timestamp_ms"), 0)
        source_when = datetime.fromtimestamp(source_ts_ms / 1000, tz=timezone.utc).isoformat() if source_ts_ms else "no source timestamp"
        source_name = str(r.get("effective_leader_equity_base_source") or "cached account history")
        source_stale = bool(r.get("effective_leader_equity_base_source_stale", False))
        equity_state = "LOCKED" if leader_base_locked else ("STALE" if source_stale else "AUTO")
        equity_title = html.escape(f"{source_name}; {source_when}; {source_age_text}; full value ${eff_leader_base:,.4f}")
        cfg_cell = f"""<form action="/api/wallet-config" method="post" class="wallet-cfg"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><label>Mode<select name="copy_mode"><option value="" {'selected' if not cfg_mode else ''}>global</option><option value="proportional" {'selected' if cfg_mode == 'proportional' else ''}>prop</option><option value="fixed" {'selected' if cfg_mode == 'fixed' else ''}>fixed</option></select></label><label>Norm $<input name="norm_base" type="number" step="any" value="{eff_base:g}" title="wallet normalisation base"></label><label>Leader equity $<input name="leader_equity_base" type="number" step="any" value="{eff_leader_text}" {'readonly' if not leader_base_locked else ''} title="{equity_title}"></label><label class="lock-label" title="Lock proportional sizing to this fixed leader equity instead of timestamp-aligned account equity"><input type="checkbox" name="leader_equity_base_locked" value="1" {'checked' if leader_base_locked else ''}>LOCK</label><label>Fixed/min $<input name="fixed_notional" type="number" step="any" value="{eff_fixed:g}" title="wallet fixed/min $"></label><button title="save wallet override">Save</button></form>"""
        equity_refresh_cell = f"""<form action="/api/wallet-equity-refresh" method="post" class="equity-refresh-form"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><button title="Fetch this wallet's latest Hyperliquid portfolio history, then rebuild the model" {'disabled' if leader_base_locked else ''}>Refresh equity</button></form>"""
        purge_cell = f"""<form action="/api/admin/purge-wallet" method="post" class="purge-form" title="ADMIN MAINTENANCE ONLY: permanently purge wallet"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><button class="purge-btn" title="purge wallet" onclick="var b=this;if(b.dataset.step){{fetch('/api/admin/purge-wallet',{{method:'POST',body:new FormData(b.form),headers:{{'X-Requested-With':'fetch'}}}}).then(function(r){{if(!r.ok)throw new Error('Purge failed');location.reload()}}).catch(function(e){{alert('Purge failed: '+e.message);b.dataset.step='';b.textContent='PURGE';b.classList.remove('confirming')}})}}else{{b.dataset.step='1';b.textContent='CONFIRM?';b.classList.add('confirming');setTimeout(function(){{b.dataset.step='';b.textContent='PURGE';b.classList.remove('confirming')}},3000)}};return false">PURGE</button></form>"""
        meta_link = f'<a href="/wallet-meta/{html.escape(wallet)}" class="meta-edit-link" title="Edit wallet tag / color / note">Meta</a>'
    row_cls = 'user' if r.get('is_user_wallet') else ''
    if not r.get('is_user_wallet') and not included: row_cls += ' excluded-row'
    if r.get('app_model_excluded'): row_cls += ' model-excluded-row'
    wallet_sort = html.escape(wallet)
    if r.get("is_user_wallet"):
        controls_content = f'<details class="wallet-settings"><summary>⚙ Base</summary><div class="wallet-settings-panel">{cfg_cell}</div></details>'
    else:
        controls_content = f'<details class="wallet-settings"><summary title="{equity_title}">⚙ {equity_state} {eff_leader_short}</summary><div class="wallet-settings-panel">{cfg_cell}<div class="wallet-settings-actions">{equity_refresh_cell}{meta_link}{purge_cell}</div></div></details>'
    return f"""
    <tr class="{row_cls}">
      <td class="sticky-wallet{wallet_color_cls}" data-sort="{wallet_sort}"{wallet_title}><a href="/wallet/{wallet}">{wallet[:8]}…{wallet[-6:]}</a>{badge}{meta_badge}{inc_cell}</td>
      {core_td(r, isinstance(lead, dict) and 'equity' in lead, lead_pnl, money(lead.get('equity')), f"pair-lead {css_class(lead_pnl)}", "lead.equity")}{core_td(r, isinstance(copy, dict) and 'equity' in copy, copy_pnl, money(copy.get('equity')), f"pair-copy group-divider {css_class(copy_pnl)}", "copy.equity")}
      {core_td(r, isinstance(lead, dict) and 'realized' in lead, lead.get('realized'), dual(fnum(lead.get('realized')), alloc), f"pair-lead {css_class(lead.get('realized'))}", "lead.realized")}{core_td(r, isinstance(copy, dict) and 'realized' in copy, copy.get('realized'), dual(fnum(copy.get('realized')), alloc), f"pair-copy group-divider {css_class(copy.get('realized'))}", "copy.realized")}
      {core_td(r, isinstance(lead, dict) and 'unrealized' in lead, lead.get('unrealized'), dual(fnum(lead.get('unrealized')), alloc), f"pair-lead {css_class(lead.get('unrealized'))}", "lead.unrealized")}{core_td(r, isinstance(copy, dict) and 'unrealized' in copy, copy.get('unrealized'), dual(fnum(copy.get('unrealized')), alloc), f"pair-copy group-divider {css_class(copy.get('unrealized'))}", "copy.unrealized")}
      {core_td(r, isinstance(lead, dict) and ('drawdown' in lead or 'drawdown_usd' in lead), lead_dd_val, format_dd(lead_dd_raw, alloc), f"pair-lead {css_class(lead_dd_val)}", "lead.drawdown")}{core_td(r, isinstance(copy, dict) and ('drawdown' in copy or 'drawdown_usd' in copy), copy_dd_val, format_dd(copy_dd_raw, alloc), f"pair-copy group-divider {css_class(copy_dd_val)}", "copy.drawdown")}
      {core_td(r, r.get("lead_maxdd_effective_usd") is not None or (isinstance(lead, dict) and ('max_drawdown' in lead or 'maxdd' in lead)), lead_maxdd_val, f'<span title="{lead_maxdd_title}">{format_dd(lead_maxdd_raw, alloc)}</span>', f"pair-lead {css_class(lead_maxdd_val)}", "lead.maxdd.effective")}{core_td(r, r.get("copy_maxdd_effective_usd") is not None or (isinstance(copy, dict) and ('max_drawdown' in copy or 'maxdd' in copy)), copy_maxdd_val, f'<span title="{copy_maxdd_title}">{format_dd(copy_maxdd_raw, alloc)}</span>', f"pair-copy group-divider {css_class(copy_maxdd_val)}", "copy.maxdd.effective")}
      {_delta_cell}{pnl_hr_cell}
      {avg_trade_pct_cell}{win_rate_cell}
      {core_td(r, 'avg_position_usd' in r, r.get('avg_position_usd'), money(r.get('avg_position_usd')), css_class(r.get('avg_position_usd')), "avg_position_usd")}{core_td(r, 'max_position_usd' in r, r.get('max_position_usd'), money(r.get('max_position_usd')), css_class(r.get('max_position_usd')), "max_position_usd")}
      {core_td(r, 'avg_entry_notional_usd' in r, r.get('avg_entry_notional_usd'), money(r.get('avg_entry_notional_usd')), css_class(r.get('avg_entry_notional_usd')), "avg_entry_notional_usd")}{core_td(r, 'pct_entries_ge10' in r, r.get('pct_entries_ge10'), pct(r.get('pct_entries_ge10')), css_class(r.get('pct_entries_ge10')), "pct_entries_ge10")}{core_td(r, 'required_leverage' in r, r.get('required_leverage'), f"{fnum(r.get('required_leverage')):.2f}x", css_class(r.get('required_leverage')), "required_leverage")}
      <td class="ops-group {last_trade_cls}" data-sort="{fnum(r.get('last_trade_timestamp_ms'))}" data-field="last_trade_timestamp_ms" data-lt-wallet="{html.escape(str(r.get('wallet', '')))}" title="{html.escape(last_trade_title)}">{html.escape(last_trade_age)}</td>
      {lc_cell(r, lc_counts['fills'], "ops-group")}{lc_cell(r, lc_counts['exits'], "ops-group")}{lc_cell(r, lc_counts['pos'], "ops-group group-divider")}
      <td class="controls-cell {'inc-off' if (not r.get('is_user_wallet') and not included) else ''}">{controls_content}</td>
    </tr>"""

HTML_TEMPLATE = """
<!doctype html><html><head><meta charset="utf-8"><title>Wallet Proof Engine</title>
<style>
body{{margin:0;background:#0d1117;color:#c9d1d9;font:12px Arial,Helvetica,sans-serif}} a{{color:#58a6ff;text-decoration:none}} .top{{display:flex;align-items:center;gap:9px;padding:7px 12px;border-bottom:1px solid #222;background:#090d12;position:sticky;top:0;z-index:4;box-shadow:0 2px 8px rgba(0,0,0,.25);flex-wrap:wrap}} .live{{background:#003d1f;color:#2ea043;border:1px solid #2ea043;border-radius:12px;padding:2px 8px;font-size:10px}} .muted{{color:#8b949e}} input,select,button{{background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:4px;padding:3px 8px}} button{{cursor:pointer}}
.cards{{display:grid;grid-template-columns:repeat(8,minmax(130px,1fr));gap:8px;padding:10px 14px}} .card{{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:9px;min-height:72px}} .group-card .label{{font-size:10px;color:#8b949e;margin-bottom:6px;border-bottom:1px solid #21262d;padding-bottom:4px}} .metric-line{{display:flex;justify-content:space-between;gap:8px;line-height:1.55}} .metric-line span{{color:#8b949e}} .metric-line b{{font-weight:700}} .pos{{color:#2ea043}} .warn{{color:#d29922}} .neg{{color:#ff4d4f}} .zero{{color:#c9d1d9}}
.section{{padding:0 14px 10px}} .panel{{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px;margin-bottom:12px}} .chart-wrap{{position:relative;cursor:zoom-in}} .chart-wrap.expanded{{position:relative;z-index:20}} .chart-wrap.expanded .chart{{height:76vh}} .chart{{width:100%;height:260px;background:#151a21}} .chart *{{vector-effect:non-scaling-stroke}} .pnl-line{{fill:none;stroke:#2ea043;stroke-width:1.6;stroke-linejoin:round;stroke-linecap:round}} .realized-line{{fill:none;stroke:#58a6ff;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .dd-line{{fill:none;stroke:#ff4d4f;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .true-dd-line{{fill:none;stroke:#d2a8ff;stroke-width:1.8;stroke-linejoin:round;stroke-linecap:round;stroke-dasharray:5 4}} .true-dd-blocked .legend-true-dd{{color:#f85149}} .chart-blocked-overlay{{position:absolute;z-index:2;top:10px;left:76px;right:18px;display:flex;gap:8px;align-items:center;padding:7px 9px;border:1px solid rgba(248,81,73,.55);background:rgba(13,17,23,.88);color:#f85149;font-size:12px;pointer-events:none}} .chart-blocked-overlay span{{color:#c9d1d9}} .zero-line{{stroke:#8b949e;stroke-width:1}} .grid-line,.grid-vert{{stroke:#21262d;stroke-width:1}} .axis-label{{fill:#8b949e;font-size:10px}} .hit{{fill:transparent;stroke:none;pointer-events:all}} .crosshair{{stroke:#8b949e;stroke-width:1;stroke-dasharray:3 3;pointer-events:none}} .chart-dot{{fill:#c9d1d9;stroke:#0d1117;stroke-width:1.2;pointer-events:none}} .chart-tip{{position:absolute;left:10px;top:10px;background:#0d1117;border:1px solid #30363d;border-radius:4px;padding:5px 7px;color:#c9d1d9;font-size:11px;pointer-events:none;box-shadow:0 4px 12px rgba(0,0,0,.35)}} .chart-legend{{display:flex;gap:10px;align-items:center;margin-top:6px}} .legend-pnl{{color:#2ea043}} .legend-realized{{color:#58a6ff}} .legend-dd{{color:#ff4d4f}} .legend-true-dd{{color:#d2a8ff}}
.table-wrap{{border:1px solid #21262d;border-radius:6px;background:#0d1117;overflow-x:auto;max-width:100%}} table{{width:100%;border-collapse:separate;border-spacing:0;font-size:10px}} th{{position:sticky;top:0;background:#21262d;color:#8b949e;text-align:right;padding:4px 3px;border-bottom:1px solid #30363d;z-index:2;line-height:1.05}} th:first-child,td:first-child{{text-align:left}} th.sort-active{{background:#303a49;color:#fff;box-shadow:inset 0 -2px 0 #58a6ff}} th.sort-active a{{display:block;color:#fff}} th a{{display:block;color:#8b949e;white-space:normal}} td{{padding:3px;border-bottom:1px solid #21262d;text-align:right;white-space:nowrap}} tr:nth-child(even){{background:#111820}} tr.user{{background:#071527}} tr:hover{{background:#1b2330}} tr.excluded-row{{}}
.sticky-wallet{{position:sticky;left:0;z-index:3;background:inherit;min-width:118px;border-right:1px solid #30363d}} th.sticky-wallet{{z-index:4;background:#21262d}} .sticky-wallet.wallet-color-blue{{background:linear-gradient(90deg,rgba(88,166,255,.34),rgba(13,17,23,.96))!important;border-left:4px solid #58a6ff}} .sticky-wallet.wallet-color-green{{background:linear-gradient(90deg,rgba(46,160,67,.34),rgba(13,17,23,.96))!important;border-left:4px solid #2ea043}} .sticky-wallet.wallet-color-yellow{{background:linear-gradient(90deg,rgba(210,153,34,.36),rgba(13,17,23,.96))!important;border-left:4px solid #d29922}} .sticky-wallet.wallet-color-red{{background:linear-gradient(90deg,rgba(255,77,79,.34),rgba(13,17,23,.96))!important;border-left:4px solid #ff4d4f}} .sticky-wallet.wallet-color-purple{{background:linear-gradient(90deg,rgba(163,113,247,.34),rgba(13,17,23,.96))!important;border-left:4px solid #a371f7}} .pair-lead{{background:rgba(88,166,255,.045)}} .pair-copy{{background:rgba(46,160,67,.045)}} .ops-group{{background:rgba(210,153,34,.05)}} .group-divider{{border-right:2px solid #30363d!important}} .badge{{background:#0d419d;color:#fff;border-radius:3px;padding:1px 4px;font-size:9px}} .wallet-tag{{background:#30363d;color:#c9d1d9;border-radius:3px;padding:1px 4px;font-size:9px}} .mode{{background:#063d1f;color:#2ea043;border-radius:3px;padding:2px 6px}} .wallet-cfg{{display:grid;grid-template-columns:repeat(2,minmax(120px,1fr));gap:6px;align-items:end}} .wallet-cfg label{{display:grid;gap:2px;color:#8b949e;font-size:10px}} .wallet-cfg input[type="number"]{{width:128px;padding:3px 5px}} .wallet-cfg input[readonly]{{color:#d29922;background:#111820}} .wallet-cfg select{{width:128px;padding:3px 5px}} .wallet-cfg button{{padding:3px 8px}} .lock-label{{display:flex!important;align-items:center;gap:4px}} .inc-form{{display:inline-flex;align-items:center;gap:1px;margin-left:5px}} .inc-form input{{padding:0;width:13px;height:13px}} .purge-form,.equity-refresh-form{{display:inline-flex;align-items:center}} .purge-btn{{border-color:#8b1d1d;background:#3a1111;color:#ff7b72;padding:1px 5px;font-size:10px}} .excl-form{{display:inline-flex;margin-left:4px;align-items:center}} .excl-btn{{border-color:#665500;background:#332900;color:#d29922;padding:1px 5px;font-size:10px}} .restore-btn{{border-color:#005566;background:#002a33;color:#58a6ff;padding:1px 5px;font-size:10px}} .inc-off{{opacity:1}} .controls-cell{{min-width:112px;text-align:left;vertical-align:top}} .wallet-settings summary,.global-settings summary{{cursor:pointer;list-style:none;color:#58a6ff;white-space:nowrap}} .wallet-settings summary::-webkit-details-marker,.global-settings summary::-webkit-details-marker{{display:none}} .wallet-settings[open]{{min-width:300px}} .wallet-settings-panel{{margin-top:6px;padding:7px;background:#111820;border:1px solid #30363d;border-radius:5px}} .wallet-settings-actions{{display:flex;gap:8px;align-items:center;margin-top:7px;border-top:1px solid #30363d;padding-top:6px}} .global-settings{{position:relative}} .global-settings[open]{{flex-basis:100%}} .global-settings form{{display:flex;gap:6px;align-items:center;flex-wrap:wrap;padding:6px 0}} .table-toolbar{{display:flex;justify-content:flex-end;margin:0 0 5px}} .table-toolbar button{{font-size:10px;padding:2px 7px}} .small{{font-size:11px;color:#8b949e}} .missing{{color:#6e7681!important}} .selected-divider td{{background:#0d1117;border-top:2px solid #58a6ff;border-bottom:1px solid #30363d;color:#8b949e;text-align:left;font-size:10px;letter-spacing:.04em;text-transform:uppercase;padding:6px 8px}} .saving{{opacity:.65}} .saved-flash{{color:#2ea043}} .chart-empty{{height:230px;display:flex;align-items:center;justify-content:center;color:#8b949e}}
.filter-panel{{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:8px 12px;margin-bottom:8px}} .filter-summary{{cursor:pointer;font-size:11px;color:#c9d1d9;list-style:none}} .filter-summary::-webkit-details-marker{{display:none}} .filter-form{{margin-top:8px}} .filter-grid{{display:flex;flex-wrap:wrap;gap:6px 14px;margin-bottom:8px}} .frow{{display:inline-flex;align-items:center;gap:4px;font-size:11px;white-space:nowrap}} .frow b{{color:#8b949e}} .frow input{{width:70px;padding:2px 4px;font-size:11px}} .filter-actions{{display:inline-flex;gap:8px;align-items:center}} .apply-filter-btn{{background:#2d3c1a;border-color:#4d7a2a;color:#7ed651;padding:3px 10px;font-size:11px}} .clear-filter-btn{{background:#161b22;border-color:#30363d;color:#8b949e;padding:3px 10px;font-size:11px}}
.excluded-section{{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:8px 12px;margin-top:8px}} .excl-summary{{cursor:pointer;font-size:11px;color:#d29922;list-style:none}} .excl-summary::-webkit-details-marker{{display:none}} .excl-table{{width:auto;border-collapse:separate;border-spacing:0;font-size:11px;margin-top:6px}} .excl-table th{{background:#21262d;color:#8b949e;padding:4px 10px;text-align:left}} .excl-table td{{padding:4px 10px;border-bottom:1px solid #21262d;text-align:left}} .mono{{font-family:monospace}} .model-excluded-row{{opacity:.55}}
@media(max-width:1300px){{.cards{{grid-template-columns:repeat(3,minmax(150px,1fr))}}}}
</style></head><body><div class="top"><b>Wallet Proof Engine</b> <span class="muted">App: Wallet Proof Engine</span><span class="muted">Port: 8014</span> <a href="/refresh" class="refresh-btn" title="Refresh equity for selected cohort wallets (except LOCK), then rebuild the model">⟳ Rebuild model</a> <span class="live">POLL</span><span class="muted">Updated: {updated}</span><details class="global-settings"><summary>⚙ Model settings · {mode}</summary><form action="/api/ui-state" method="post" class="ajax-form"><span class="muted">Normalisation Base:</span><input name="norm_base" value="{norm}" size="8"><span class="muted">Mode:</span><select name="copy_mode"><option {prop_selected}>proportional</option><option {fixed_selected}>fixed</option></select><span class="muted">Fixed/Min $:</span><input name="fixed_notional" value="{fixed}" size="6"><label class="muted" title="When checked, model only opens wallet entries/flips whose effective copy notional is at least this wallet's Fixed/Min value."><input type="checkbox" name="min_trade_notional_enabled" value="1" {min_checked}> Min</label><span class="muted">Fee bps:</span><input name="fee_bps" value="{fee}" size="5"><span class="muted">Copy friction bps:</span><input name="copy_friction_bps" value="{friction}" size="5"><button>Set</button></form></details><form action="/api/import-copy-candidates" method="post" class="ajax-form" title="Import Wallet Talent Scout export into manual_wallets.txt with dedup"><button>Import Candidates</button></form><span id="save-status" class="muted">{equity_refresh_notice}</span><span style="margin-left:auto" class="muted">Auto refresh off</span></div><div class="cards">{cards}</div><div class="section"><b>COMBINED PORTFOLIO — NON-USER WALLETS</b><div class="panel">{chart}</div><div class="small">TRACKED WALLETS ({wallet_count} modelled) — model derived in app from engine SSOT only. {raw_boundary}</div>{filter_panel}<div class="table-wrap"><table id="wallet-table"><thead>{table_head}</thead><tbody>{table_rows}</tbody></table></div>{excluded_section}</div>
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
async function saveModelForm(form, delay){{
  if(!form||form.classList.contains('saving'))return;
  markBusy(5000);form.classList.add('saving');
  try{{
    const r=await fetch(form.action,{{method:'POST',body:new FormData(form),headers:{{'X-Requested-With':'fetch'}}}});
    const payload=await r.json().catch(()=>({{}}));
    if(!r.ok||payload.ok===false)throw new Error(payload.error||('HTTP '+r.status));
    if(payload.model_refresh_required){{
      flash('rebuilding model…','muted');
      const deadline=Date.now()+120000;
      let ready=false;
      while(Date.now()<deadline){{
        const h=await fetch('/api/cache-health',{{cache:'no-store'}}).then(x=>x.json());
        const rs=h.refresh_status||{{}};
        if(rs.error)throw new Error(rs.error);
        if(!rs.in_progress&&rs.ok&&h.model_state_present){{ready=true;break;}}
        await new Promise(resolve=>setTimeout(resolve,250));
      }}
      if(!ready){{flash('model rebuild is still running; reload when ready','warn');return;}}
    }}
    saveViewState();
    setTimeout(()=>location.reload(),payload.model_refresh_required?50:(delay||500));
  }}
  catch(err){{console.warn('save failed',err);flash('save failed','neg');}}
  finally{{form.classList.remove('saving');}}
}}
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
document.addEventListener('change',async e=>{{
  const lock=e.target.closest('.wallet-cfg input[name="leader_equity_base_locked"]');
  if(lock&&lock.form){{const equity=lock.form.querySelector('input[name="leader_equity_base"]');if(equity)equity.readOnly=!lock.checked;await saveModelForm(lock.form,450);return;}}
  const field=e.target.closest('.wallet-cfg select[name="copy_mode"],.wallet-cfg input[name="norm_base"],.wallet-cfg input[name="fixed_notional"],.inc-form input[name="included"]');
  if(!field||!field.form)return;
  e.preventDefault();
  await saveModelForm(field.form,450);
}});
document.addEventListener('submit',async e=>{{
  const form=e.target;if(!form.matches('.ajax-form,.wallet-cfg,.inc-form,.equity-refresh-form'))return;
  e.preventDefault();
  await saveModelForm(form,800);
  restoreViewState();
}});

let cohortRebuildPending=false;
document.addEventListener('click',async e=>{{
  const rebuild=e.target.closest('a.refresh-btn');
  if(rebuild){{
    e.preventDefault();
    if(cohortRebuildPending)return;
    if(document.querySelector('.wallet-cfg.saving,.inc-form.saving')){{flash('Settings are saving; rebuild once saved','warn');return;}}
    cohortRebuildPending=true;rebuild.setAttribute('aria-disabled','true');rebuild.classList.add('saving');
    saveViewState();
    if(status)status.textContent='Refreshing selected equity, then rebuilding model…';
    try{{
      const r=await fetch(rebuild.href,{{headers:{{'X-Requested-With':'fetch'}}}});
      const result=await r.json();
      if(!r.ok||result.ok===false)throw new Error(result.error||('HTTP '+r.status));
      location.reload();
    }}catch(err){{flash('Rebuild failed: '+err.message,'neg');}}
    finally{{cohortRebuildPending=false;rebuild.removeAttribute('aria-disabled');rebuild.classList.remove('saving');}}
    return;
  }}
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
}})();</script>{LAST_TRADE_SSE_SCRIPT}</body></html>
"""


def wants_json_response(request: Request) -> bool:
    return "fetch" in str(request.headers.get("x-requested-with", "")).lower() or "application/json" in str(request.headers.get("content-type", "")).lower()


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    """Dashboard home route with non-blocking fallback."""
    return _model_dashboard_response()


def refresh_selected_leader_equities(state: Dict[str, Any], ui: Dict[str, Any]) -> Dict[str, Any]:
    """Refresh the displayed graph cohort, respecting current INC and LOCK settings."""
    wallets = sorted({
        str(row.get("wallet", "")).strip().lower()
        for row in state.get("wallet_rows", [])
        if isinstance(row, dict)
        and not row.get("is_user_wallet")
        and str(row.get("wallet", "")).strip().lower() != USER_WALLET.lower()
        and not row.get("app_model_excluded")
        and not (ui.get("wallet_model_exclude") or {}).get(str(row.get("wallet", "")).lower())
        and wallet_included(str(row.get("wallet", "")), ui)
    })
    summary = {"selected": len(wallets), "refreshed": 0, "locked": 0, "failed": 0, "skipped": 0, "errors": []}
    for wallet in wallets:
        # Re-read before each fetch so a freeze or deselection during a long
        # rate-budget wait also protects wallets that have not been fetched yet.
        current_ui = load_ui_state()
        if not wallet_included(wallet, current_ui) or (current_ui.get("wallet_model_exclude") or {}).get(wallet):
            summary["skipped"] += 1
            continue
        if parse_bool(((current_ui.get("wallet_config") or {}).get(wallet) or {}).get("leader_equity_base_locked", False)):
            summary["locked"] += 1
            continue
        result = refresh_wallet_leader_equity_source(wallet, budget_timeout_s=65.0, invalidate_cache=False, selected_only=True)
        if result.get("skipped") == "locked":
            summary["locked"] += 1
        elif result.get("skipped"):
            summary["skipped"] += 1
        elif result.get("ok"):
            summary["refreshed"] += 1
        else:
            summary["failed"] += 1
            summary["errors"].append({"wallet": wallet, "error": result.get("error", "Equity refresh failed")})
    return summary


@app.get("/refresh", response_class=HTMLResponse)
def refresh_model_dashboard(request: Request):
    """Fetch selected, unlocked leader equity, then publish one complete rebuild."""
    with _COHORT_REBUILD_LOCK:
        state, _ = _model_cache_snapshot_nonblocking()
        if state is None:
            state = get_model_state_cached(max_age_sec=300.0)
        equity_refresh = refresh_selected_leader_equities(state, load_ui_state())
        fresh = get_model_state_cached(max_age_sec=0.0, force=True)
        fresh["cohort_equity_refresh"] = equity_refresh
        with _MODEL_BUILD_LOCK:
            fresh_built_at = fnum(_MODEL_CACHE.get("built_at"), time.time())
        dashboard_html = _render_model_dashboard_html_cached(fresh, fresh_built_at)
    if wants_json_response(request):
        return JSONResponse({"ok": True, "model_refreshed": True, "equity_refresh": equity_refresh})
    return HTMLResponse(dashboard_html)


@app.get("/model", response_class=HTMLResponse)
@app.get("/legacy-model", response_class=HTMLResponse)
def model_dashboard() -> HTMLResponse:
    """Legacy SSOT model dashboard, always accessible at /model or /legacy-model."""
    return _model_dashboard_response()


def _model_dashboard_response() -> HTMLResponse:
    """Serve the latest complete dashboard and refresh stale state off-thread."""
    cached_html = _model_dashboard_html_cache_latest_get()
    if cached_html is None:
        cached_html = _model_dashboard_last_good_html_disk_get()
    cached, built_at = _model_cache_snapshot_nonblocking()
    missing_manual = state_missing_manual_wallets(cached)
    leader_equity_changed = cached is not None and not missing_manual and _leader_equity_sources_changed(cached)

    # A complete pre-rendered dashboard is the fastest and safest cold-start
    # response. Serve it before loading/rendering the large JSON state, then
    # replace it in the background with a freshly calculated page.
    if cached_html is not None:
        if cached is None or missing_manual or leader_equity_changed or (time.time() - built_at) > 300.0:
            if _kick_model_cache_refresh_background():
                print("MODEL_DASHBOARD_CACHE_HIT_STALE_SERVED", flush=True)
        print("MODEL_DASHBOARD_HTML_CACHE_HIT", flush=True)
        return HTMLResponse(cached_html)

    if cached is None and APP_MODEL_STATE_JSON.exists():
        try:
            disk_state = load_json(APP_MODEL_STATE_JSON, {})
            if (
                isinstance(disk_state, dict)
                and disk_state.get("wallet_rows")
                and not state_missing_manual_wallets(disk_state)
            ):
                disk_built_at = APP_MODEL_STATE_JSON.stat().st_mtime
                return HTMLResponse(_render_model_dashboard_html_cached(disk_state, disk_built_at))
        except Exception as exc:
            print(f"MODEL_DASHBOARD_DISK_CACHE_FAILED {type(exc).__name__}: {exc}", flush=True)

    if cached is not None and not missing_manual:
        if leader_equity_changed or (time.time() - built_at) > 300.0:
            if _kick_model_cache_refresh_background():
                print("MODEL_DASHBOARD_CACHE_HIT_STALE_SERVED", flush=True)
        return HTMLResponse(_render_model_dashboard_html_cached(dict(cached), built_at))

    if _kick_model_cache_refresh_background():
        print("MODEL_DASHBOARD_CACHE_COLD_BACKGROUND_STARTED", flush=True)
    live_config = _load_live_copy_config()
    auto_send = parse_bool(live_config.get("auto_send_enabled"))
    gate = load_json(WALLET_GATE_FILE, {})
    on_count = 0
    if isinstance(gate, dict):
        on_count = sum(1 for v in gate.values() if isinstance(v, dict) and str(v.get("mode") or "").upper() == "ON")
    html_doc = "<html><head><title>Wallet Proof Engine</title><!-- meta-refresh removed: no auto-polling --></head><body>"
    html_doc += "<h2>Wallet Proof Engine</h2>"
    html_doc += "<p>Model dashboard cache is warming. This loading shell is deliberately non-blocking.</p>"
    html_doc += "<p>auto_send_enabled: " + str(auto_send) + "</p>"
    html_doc += "<p>wallet_gate_on_count: " + str(on_count) + "</p>"
    html_doc += "<p>updated_at: " + str(utc_now_iso()) + "</p>"
    with _MODEL_REFRESH_LOCK:
        refresh_status = dict(_MODEL_REFRESH_STATUS)
    status_bits = [
        f"in_progress={bool(refresh_status.get('in_progress'))}",
        f"ok={bool(refresh_status.get('ok'))}",
    ]
    marker = str(refresh_status.get("last_marker") or "")
    if marker:
        status_bits.append(f"marker={marker}")
    error = str(refresh_status.get("error") or "")
    if error:
        status_bits.append(f"error={error}")
    html_doc += "<p>cache_status: " + html.escape(" | ".join(status_bits)) + "</p>"
    html_doc += "<p><a href=/live-copy>/live-copy</a></p>"
    html_doc += "<p><a href=/api/state>/api/state</a></p>"
    html_doc += "<p><a href=/api/live-audit-summary>/api/live-audit-summary</a></p>"
    html_doc += "</body></html>"
    return HTMLResponse(html_doc)


# How often the server re-stats engine_truth.json while an SSE client is
# attached. Only a stat(); the file is parsed when the mtime actually moves.
LAST_TRADE_WATCH_S = 5.0

# Plain (non f-string) so JS braces need no escaping; interpolated into the
# dashboard template. No refresh timer: the server pushes on change only.
LAST_TRADE_SSE_SCRIPT = """<script>
(function () {
  if (!window.EventSource) return;
  var es = new EventSource('/events/last-trade');
  es.onmessage = function (evt) {
    var delta;
    try { delta = JSON.parse(evt.data); } catch (err) { return; }
    Object.keys(delta).forEach(function (wallet) {
      var td = document.querySelector('td[data-lt-wallet="' + wallet + '"]');
      if (!td) return;
      var c = delta[wallet];
      // A last trade only ever moves forward. Engine truth can be written
      // before a late-received fill lands, so an overlay push may carry an
      // older stamp than the model already rendered; applying it would
      // un-display a trade that did happen. Equal stamps still pass through
      // so the age label keeps ticking against wall clock.
      var prev = parseFloat(td.getAttribute('data-sort') || '0') || 0;
      if (prev > 0 && c.ms < prev) return;
      td.classList.remove('pos', 'warn', 'neg', 'muted');
      if (c.cls) td.classList.add(c.cls);
      if (c.title) td.title = c.title;
      td.setAttribute('data-sort', c.ms);
      td.textContent = c.age;
    });
  };
})();
</script>"""


def _last_trade_ms_from_engine_truth() -> Dict[str, int]:
    """Per-wallet last fill timestamp taken straight from engine SSOT.

    Deliberately bypasses the derived model: last_fill_ts is factual engine
    truth refreshed every few seconds, whereas app_model_state.json is only
    rebuilt on demand. Reading it here keeps the column live without paying
    for (or waiting on) a full model recompute.
    """
    truth = load_engine_truth() or {}
    out: Dict[str, int] = {}
    for wallet, row in (truth.get("wallets") or {}).items():
        if not isinstance(row, dict):
            continue
        out[str(wallet)] = iso_to_ms(row.get("last_fill_ts"))
    # The USER row is a rollup of the selected leaders, not an account the
    # engine tracks, so truth has no entry for it. Derive it here rather than
    # leaving the cell on its server-rendered model value: the model reads
    # raw_live_fills.csv and truth is written on its own cycle, so the two
    # sources drift apart whenever a fill is received after the last truth
    # write. That drift is what made the USER cell claim a trade newer than
    # every leader cell on screen.
    # Scope to wallets_tracked, which is the set the dashboard renders rows for.
    # truth["wallets"] also carries entries outside it, and wallet_included()
    # defaults absent wallets to True — so rolling up the raw map would let a
    # wallet with no row on screen set the USER cell, which is the same defect
    # in a different disguise.
    ui = load_ui_state()
    tracked = {str(w).lower() for w in (truth.get("wallets_tracked") or [])}
    included = [
        ms for wallet, ms in out.items()
        if ms > 0
        and (not tracked or str(wallet).lower() in tracked)
        and wallet_included(wallet, ui)
    ]
    if included:
        out[USER_WALLET] = max(included)
    return out


def _last_trade_cells_from_ms(ms_by_wallet: Dict[str, int]) -> Dict[str, Dict[str, Any]]:
    """Render age labels from cached timestamps.

    Kept separate from the engine-truth read so the label can be re-derived
    against wall clock every tick without re-parsing the truth file.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for wallet, ms in ms_by_wallet.items():
        age, cls, title = age_ms_label(ms)
        out[wallet] = {"age": age, "cls": cls, "title": title, "ms": ms}
    return out


@app.get("/events/last-trade")
async def last_trade_events(request: Request) -> StreamingResponse:
    """Push LAST TRADE updates to the browser as engine truth advances.

    Server-sent events, not a client refresh timer: the page keeps one
    connection open and receives a message only when a wallet's value changes.
    """

    async def stream():
        sent: Dict[str, Dict[str, Any]] = {}
        ms_by_wallet: Dict[str, int] = {}
        last_mtime_ns = -1
        while True:
            if await request.is_disconnected():
                break
            try:
                mtime_ns = ENGINE_TRUTH_JSON.stat().st_mtime_ns if ENGINE_TRUTH_JSON.exists() else -1
                # Re-read engine truth only when it actually moves, but always
                # re-derive the label: the cell shows an age against wall clock,
                # so gating both would freeze the column whenever the engine
                # went quiet.
                if mtime_ns != last_mtime_ns:
                    last_mtime_ns = mtime_ns
                    ms_by_wallet = await asyncio.to_thread(_last_trade_ms_from_engine_truth)
                current = _last_trade_cells_from_ms(ms_by_wallet)
                delta = {w: c for w, c in current.items() if sent.get(w) != c}
                if delta:
                    sent.update(delta)
                    yield f"data: {json.dumps(delta)}\n\n"
                else:
                    yield ": keepalive\n\n"
            except Exception as exc:  # never let one bad read kill the stream
                print(f"LAST_TRADE_STREAM_ITERATION_FAILED {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(LAST_TRADE_WATCH_S)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/cache-health")
def api_cache_health() -> JSONResponse:
    """Simple cache-health wrapper for Wallet Finder compatibility."""
    try:
        with _MODEL_REFRESH_LOCK:
            rs = dict(_MODEL_REFRESH_STATUS)
        with _MODEL_BUILD_LOCK:
            model_present = bool(_MODEL_CACHE.get("state"))
        status = "FRESH" if (rs.get("ok") and not rs.get("in_progress")) else "STALE_REBUILDING" if rs.get("in_progress") else "STALE_BLOCKED" if rs.get("error") else "STALE"
        return JSONResponse({
            "status": status,
            "model_state_present": model_present,
            "refresh_status": rs,
            "cache_loaded_at": utc_now_iso(),
        })
    except Exception as exc:        return JSONResponse({"status": "ERROR", "error": str(exc)})


STALE_FILLS_THRESHOLD_SEC = 3600  # 1 hour — alert if raw_live_fills.csv hasn't been updated longer than this


@app.get("/api/engine-health")
def api_engine_health() -> JSONResponse:
    """Monitor upstream engine health: checks raw_live_fills.csv freshness.

    Returns OK when the engine is producing fresh fill data. Returns STALE
    when raw_live_fills.csv hasn't been updated in > 1 hour, indicating
    the copy engine may have stalled (the exact failure mode we hit on
    2026-07-29). Also reports engine_truth.json freshness as a secondary
    signal.
    """
    try:
        now_epoch = time.time()
        check_time = utc_now_iso()

        # Primary signal: raw_live_fills.csv is the canonical append-only ledger.
        # If it stops growing, the engine is definitely stalled.
        fills_age_sec: Optional[float] = None
        fills_mtime_iso: str = ""
        fills_status: str = "MISSING"
        if RAW_FILLS_CSV.exists():
            fills_mtime = RAW_FILLS_CSV.stat().st_mtime
            fills_age_sec = now_epoch - fills_mtime
            fills_mtime_iso = datetime.fromtimestamp(fills_mtime, timezone.utc).isoformat()
            fills_status = "OK" if fills_age_sec <= STALE_FILLS_THRESHOLD_SEC else "STALE"

        # Secondary signal: engine_truth.json is the derived snapshot.
        truth_age_sec: Optional[float] = None
        truth_mtime_iso: str = ""
        truth_status: str = "MISSING"
        if ENGINE_TRUTH_JSON.exists():
            truth_mtime = ENGINE_TRUTH_JSON.stat().st_mtime
            truth_age_sec = now_epoch - truth_mtime
            truth_mtime_iso = datetime.fromtimestamp(truth_mtime, timezone.utc).isoformat()
            truth_status = "OK" if truth_age_sec <= STALE_FILLS_THRESHOLD_SEC else "STALE"

        overall = "FRESH"
        alerts: List[str] = []
        if fills_status == "MISSING":
            overall = "ERROR"
            alerts.append("raw_live_fills.csv is missing — engine may not be running")
        elif fills_status == "STALE":
            overall = "STALE"
            alerts.append(f"raw_live_fills.csv last updated {fills_age_sec / 3600:.1f}h ago — engine may be stalled")
        if truth_status == "MISSING":
            if overall == "FRESH":
                overall = "WARN"
            alerts.append("engine_truth.json is missing")
        elif truth_status == "STALE" and overall == "OK":
            overall = "WARN"
            alerts.append(f"engine_truth.json last updated {truth_age_sec / 3600:.1f}h ago — may need model rebuild")

        return JSONResponse({
            "ok": True,
            "overall": overall,
            "alerts": alerts,
            "checked_at": check_time,
            "stale_threshold_sec": STALE_FILLS_THRESHOLD_SEC,
            "raw_live_fills_csv": {
                "status": fills_status,
                "age_sec": round(fills_age_sec, 1) if fills_age_sec is not None else None,
                "age_hours": round(fills_age_sec / 3600, 2) if fills_age_sec is not None else None,
                "last_modified": fills_mtime_iso,
                "path": str(RAW_FILLS_CSV),
            },
            "engine_truth_json": {
                "status": truth_status,
                "age_sec": round(truth_age_sec, 1) if truth_age_sec is not None else None,
                "age_hours": round(truth_age_sec / 3600, 2) if truth_age_sec is not None else None,
                "last_modified": truth_mtime_iso,
                "path": str(ENGINE_TRUTH_JSON),
            },
        })
    except Exception as exc:
        return JSONResponse({
            "ok": False,
            "overall": "ERROR",
            "error": str(exc),
            "checked_at": utc_now_iso(),
        })


@app.get("/api/model-cache-status")
def api_model_cache_status() -> JSONResponse:
    with _MODEL_REFRESH_LOCK:
        refresh_status = dict(_MODEL_REFRESH_STATUS)
    with _MODEL_BUILD_LOCK:
        model_state_present = bool(_MODEL_CACHE.get("state"))
    with _MODEL_DASHBOARD_HTML_CACHE_LOCK:
        memory_html_present = bool(_MODEL_DASHBOARD_HTML_CACHE.get("html"))
        html_built_at = fnum(_MODEL_DASHBOARD_HTML_CACHE.get("built_at"))
        html_state_built_at = fnum(_MODEL_DASHBOARD_HTML_CACHE.get("state_built_at"))
    try:
        disk_html_present = MODEL_DASHBOARD_LAST_GOOD_HTML_FILE.exists()
        disk_html_mtime = (
            datetime.fromtimestamp(MODEL_DASHBOARD_LAST_GOOD_HTML_FILE.stat().st_mtime, timezone.utc).isoformat()
            if disk_html_present else ""
        )
    except Exception:
        disk_html_present = False
        disk_html_mtime = ""
    return JSONResponse({
        "ok": True,
        "current_time": utc_now_iso(),
        "refresh_status": refresh_status,
        "model_state_present": model_state_present,
        "dashboard_html_present": memory_html_present,
        "memory_html_present": memory_html_present,
        "disk_html_present": disk_html_present,
        "disk_html_mtime": disk_html_mtime,
        "serving_last_good_possible": bool(memory_html_present or disk_html_present),
        "dashboard_html_cache": {
            "built_at": html_built_at,
            "state_built_at": html_state_built_at,
        },
    })


@app.get("/api/state")
def api_state() -> JSONResponse:
    """True lightweight status endpoint.

    This endpoint is for heartbeat/status/proof tooling. It must not call
    get_model_state_cached() and must not scan large CSVs. It returns file
    metadata instead of row counts so it remains fast as datasets grow.
    """
    live_config = _load_live_copy_config()
    wallet_gate = load_json(WALLET_GATE_FILE, {})
    gate_on = []
    if isinstance(wallet_gate, dict):
        for wallet, cfg in wallet_gate.items():
            if isinstance(cfg, dict) and str(cfg.get("mode") or "").upper() == "ON":
                gate_on.append(str(wallet).lower())
    live_wallets_cfg = live_config.get("wallets", {}) if isinstance(live_config.get("wallets"), dict) else {}
    live_config_on = [
        str(wallet).lower()
        for wallet, cfg in live_wallets_cfg.items()
        if isinstance(cfg, dict)
        and parse_bool(cfg.get("enabled", True))
        and str(cfg.get("mode", "")).upper() == "LIVE"
    ]
    gate_stale_on = sorted(set(gate_on) - set(live_config_on))
    gate_missing_live = sorted(set(live_config_on) - set(gate_on))
    def _file_meta(path: Path) -> Dict[str, Any]:
        try:
            if not path.exists():
                return {"exists": False, "bytes": 0, "mtime": ""}
            st = path.stat()
            return {"exists": True, "bytes": int(st.st_size), "mtime": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()}
        except Exception as exc:
            return {"exists": False, "bytes": -1, "mtime": "", "error": type(exc).__name__}
    return JSONResponse({
        "ok": True,
        "updated_at": utc_now_iso(),
        "auto_send_enabled": parse_bool(live_config.get("auto_send_enabled")),
        "wallet_gate_on_count": len(gate_on),
        "wallet_gate_on": gate_on,
        "live_config_on_count": len(live_config_on),
        "live_config_on": live_config_on,
        "wallet_gate_stale_on": gate_stale_on,
        "wallet_gate_missing_live": gate_missing_live,
        "global_controls": _global_controls_for_ui(live_config.get("global_controls", _GLOBAL_CONTROLS_DEFAULTS)),
        "files": {
            "raw_live_fills": _file_meta(RAW_FILLS_CSV),
            "copy_trades": _file_meta(COPY_TRADES_CSV),
            "order_intents": _file_meta(LIVE_COPY_ORDER_INTENTS_CSV),
            "send_attempts": _file_meta(SEND_ATTEMPTS_CSV),
            "live_fills": _file_meta(LIVE_COPY_AUDIT_DIR / "append_only" / "live_fills.csv"),
            "reconciliation": _file_meta(LIVE_COPY_AUDIT_DIR / "append_only" / "reconciliation.csv"),
        },
        "app_health": APP_HEALTH,
        "note": "lightweight status only; no model rebuild and no CSV scan",
    })
@app.get("/api/metrics")
def api_metrics() -> JSONResponse:
    state = get_model_state_cached(max_age_sec=5.0, force=False)
    rows = enrich_rows_with_effective_dd(state.get("wallet_rows", []))
    view_state = refresh_selected_portfolio_view({**state, "wallet_rows": rows}, load_ui_state())
    return JSONResponse({"wallets": rows, "portfolio": view_state.get("portfolio", {})})


@app.get("/api/metrics.csv")
def api_metrics_csv() -> Response:
    with _MODEL_BUILD_LOCK:
        cached = _MODEL_CACHE.get("state")
    state = cached if isinstance(cached, dict) else load_json(APP_MODEL_STATE_JSON, {})
    missing_manual = state_missing_manual_wallets(state)
    if not isinstance(state, dict) or not state.get("wallet_rows") or missing_manual:
        state = get_model_state_cached(max_age_sec=2.0, force=missing_manual)
    rows = enrich_rows_with_effective_dd(state.get("wallet_rows", []))
    fields = [
        "wallet",
        "lead_maxdd_effective_usd",
        "lead_maxdd_effective_pct",
        "lead_maxdd_effective_source",
        "copy_maxdd_effective_usd",
        "copy_maxdd_effective_pct",
        "copy_maxdd_effective_source",
        "true_ts_max_dd_usd",
        "true_ts_max_dd_pct",
        "true_ts_points",
        "true_ts_source",
        "realised_maxdd_diagnostic_usd",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        if isinstance(row, dict):
            writer.writerow(row)
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=wallet_effective_dd_metrics.csv"},
    )


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


@app.get("/api/wallet-equity-curve/{wallet}")
def api_wallet_equity_curve(wallet: str) -> JSONResponse:
    curve = _account_value_curve(wallet)
    return JSONResponse({
        "timestamps": [p.get("timestamp_ms") for p in curve],
        "equity": [p.get("equity_usd") for p in curve],
        "running_peak": [p.get("running_peak_usd") for p in curve],
        "drawdown_usd": [p.get("drawdown_usd") for p in curve],
        "drawdown_pct": [p.get("drawdown_pct") for p in curve],
        "points": curve,
        "source": "8012:data/wallet_portfolios/accountValueHistory",
    })


def refresh_wallet_leader_equity_source(wallet: str, *, budget_timeout_s: float = 5.0, invalidate_cache: bool = True, selected_only: bool = False) -> Dict[str, Any]:
    """Fetch one wallet's portfolio history within the proof-engine rate budget."""
    wallet = str(wallet or "").strip().lower()
    if not re.fullmatch(r"0x[0-9a-f]{40}", wallet):
        return {"ok": False, "error": "invalid wallet address"}
    def skip_reason() -> str:
        ui = load_ui_state()
        if parse_bool(((ui.get("wallet_config") or {}).get(wallet) or {}).get("leader_equity_base_locked", False)):
            return "locked"
        if selected_only and (not wallet_included(wallet, ui) or (ui.get("wallet_model_exclude") or {}).get(wallet)):
            return "unselected"
        return ""

    skipped = skip_reason()
    if skipped:
        return {"ok": False, "skipped": skipped, "error": f"Equity refresh skipped: wallet is {skipped}"}
    if RATE_GUARD is not None and not RATE_GUARD.acquire("portfolio", timeout_s=budget_timeout_s):
        return {"ok": False, "error": "portfolio refresh deferred: proof-engine API budget is busy"}
    # A wallet may be frozen/deselected while waiting for API capacity.
    skipped = skip_reason()
    if skipped:
        return {"ok": False, "skipped": skipped, "error": f"Equity refresh skipped: wallet is {skipped}"}
    before = _leader_equity_source_snapshot(wallet)
    try:
        stats = _get_mtm_stats(wallet, force_refresh=True, network_timeout=10.0)
    except Exception as exc:
        return {"ok": False, "error": f"portfolio refresh failed: {type(exc).__name__}: {exc}"}
    if str((stats or {}).get("mtm_source", "")) != "hl_portfolio_api":
        return {"ok": False, "error": "Hyperliquid portfolio refresh failed; the previous cached value was retained"}
    after = _leader_equity_source_snapshot(wallet)
    if after.get("value") is None:
        return {"ok": False, "error": "Hyperliquid returned no usable account-value history"}
    if invalidate_cache:
        invalidate_model_cache()
    return {
        "ok": True,
        "wallet": wallet,
        "before": before,
        "after": after,
        "model_refresh_required": True,
    }


@app.post("/api/wallet-equity-refresh", response_class=HTMLResponse)
async def api_wallet_equity_refresh(request: Request):
    form = await request.form()
    result = await asyncio.to_thread(refresh_wallet_leader_equity_source, form.get("wallet", ""))
    if result.get("ok"):
        # This is a deliberate, one-wallet operator action. Complete the model
        # rebuild off the event loop before telling the browser to reload so a
        # concurrent older build can never put the pre-fetch value back on screen.
        fresh = await asyncio.to_thread(get_model_state_cached, max_age_sec=0.0, force=True)
        with _MODEL_BUILD_LOCK:
            fresh_built_at = fnum(_MODEL_CACHE.get("built_at"), time.time())
        await asyncio.to_thread(_render_model_dashboard_html_cached, fresh, fresh_built_at)
        result["model_refresh_required"] = False
        result["model_refresh_started"] = False
        result["model_refreshed"] = True
    if wants_json_response(request):
        return JSONResponse(result, status_code=200 if result.get("ok") else 429)
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">', status_code=200 if result.get("ok") else 429)


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


@app.post("/api/import-copy-candidates", response_class=HTMLResponse)
async def api_import_copy_candidates(request: Request):
    result = import_copy_candidates_to_manual_wallets()
    status_code = 200 if result.get("ok") else 404
    if wants_json_response(request):
        return JSONResponse(result, status_code=status_code)
    if result.get("ok"):
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')
    return HTMLResponse(f"<h1>404</h1><pre>{html.escape(json.dumps(result, indent=2))}</pre>", status_code=status_code)


@app.post("/api/wallet-config", response_class=HTMLResponse)
async def set_wallet_config(request: Request):
    form = await request.form()
    wallet = str(form.get("wallet", "")).strip().lower()
    before_item: Dict[str, Any] = {}
    if wallet:
        ui = load_ui_state()
        cfg = dict(ui.get("wallet_config") or {})
        before_item = dict(cfg.get(wallet) or {})
        mode = str(form.get("copy_mode", "")).strip().lower()
        norm_raw = str(form.get("norm_base", "")).strip()
        leader_base_raw = str(form.get("leader_equity_base", "")).strip()
        leader_base_locked = parse_bool(form.get("leader_equity_base_locked", "0"))
        fixed_raw = str(form.get("fixed_notional", "")).strip()
        item: Dict[str, Any] = {}
        if mode in {"proportional", "fixed"}:
            item["copy_mode"] = mode
        if norm_raw:
            item["norm_base"] = max(1.0, fnum(norm_raw, ui.get("norm_base", DEFAULT_NORM_BASE)))
        if leader_base_locked and leader_base_raw:
            item["leader_equity_base"] = max(1.0, fnum(leader_base_raw, item.get("norm_base", ui.get("norm_base", DEFAULT_NORM_BASE))))
            item["leader_equity_base_locked"] = True
        if fixed_raw:
            item["fixed_notional"] = max(0.01, fnum(fixed_raw, ui.get("fixed_notional", DEFAULT_FIXED_NOTIONAL)))
        if item:
            cfg[wallet] = item
        else:
            cfg.pop(wallet, None)
        save_ui_state({"wallet_config": cfg})
    after = load_ui_state()
    after_item = dict((after.get("wallet_config") or {}).get(wallet) or {}) if wallet else {}
    model_refresh_required = before_item != after_item
    model_refresh_started = _kick_model_cache_refresh_background() if model_refresh_required else False
    if wants_json_response(request):
        return JSONResponse({
            "ok": True,
            "ui_state": after,
            "model_refresh_required": model_refresh_required,
            "model_refresh_started": model_refresh_started,
        })
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')


@app.post("/api/wallet-model-exclude", response_class=HTMLResponse)
async def set_wallet_model_exclude(request: Request):
    """Toggle app-model exclusion for one wallet. Reversible, app-only.
    Engine, raw_live_fills, engine_truth, wallet_gate, live_config not mutated."""
    form = await request.form()
    wallet = str(form.get("wallet", "")).strip().lower()
    if wallet and wallet != USER_WALLET:
        ui = load_ui_state()
        exc = dict(ui.get("wallet_model_exclude") or {})
        if parse_bool(form.get("excluded", "0")):
            exc[wallet] = True
        else:
            exc.pop(wallet, None)
        save_ui_state({"wallet_model_exclude": exc})
    if wants_json_response(request):
        return JSONResponse({"ok": True, "ui_state": load_ui_state()})
    return HTMLResponse('<meta http-equiv="refresh" content="0; url=/">')


@app.post("/api/wallet-filters")
async def set_wallet_filters(request: Request):
    """Save/clear metric filters and compute wallet_model_exclude from current model state.
    Reversible, app-only — engine, raw_live_fills, engine_truth, wallet_gate, live_config not mutated.
    FILTER_FIX_V3_ACTIVE
    """
    form = await request.form()
    action = str(form.get("action", "apply")).strip().lower()
    now_iso = datetime.now(timezone.utc).isoformat()
    v_bust = int(time.time())
    if action == "clear":
        save_ui_state({
            "wallet_filters": {},
            "wallet_model_exclude": {},
            "wallet_filter_last_result": {
                "evaluated_count": 0,
                "excluded_count": 0,
                "updated_at": now_iso,
            },
        })
        return RedirectResponse(f"/?v={v_bust}", status_code=303)
    else:
        raw_filters: Dict[str, Any] = {}
        for k in ["wallet_contains"] + [f"{m}_{d}" for m in _FILTER_METRIC_KEYS for d in ("min", "max")]:
            v = str(form.get(k, "")).strip()
            if v:
                raw_filters[k] = v
        filters = _clean_wallet_filters(raw_filters)
        # Apply filters against cached model state — does not trigger a rebuild
        cached = load_json(APP_MODEL_STATE_JSON, {})
        if isinstance(cached.get("wallet_rows"), list):
            cached_rows: List[Dict[str, Any]] = cached["wallet_rows"]
        elif isinstance(cached.get("wallets"), dict):
            cached_rows = list(cached["wallets"].values())
        else:
            cached_rows = []
        
        # Replacement fix v3: replace, do not merge
        new_exclude = apply_wallet_filters_to_state(filters, cached_rows)
        
        # Safety: user wallet must never be excluded
        if USER_WALLET and USER_WALLET.lower() in new_exclude:
            del new_exclude[USER_WALLET.lower()]

        evaluated_count = sum(
            1 for row in cached_rows
            if isinstance(row, dict)
            and str(row.get("wallet", "")).strip().lower()
            and str(row.get("wallet", "")).strip().lower() != USER_WALLET.lower()
            and not row.get("is_user_wallet")
        )
        save_ui_state({
            "wallet_filters": filters,
            "wallet_model_exclude": new_exclude,
            "wallet_filter_last_result": {
                "evaluated_count": evaluated_count,
                "excluded_count": len(new_exclude),
                "updated_at": now_iso,
            },
        })
    if wants_json_response(request):
        return JSONResponse({"ok": True, "ui_state": load_ui_state()})
    return RedirectResponse(f"/?v={v_bust}", status_code=303)


@app.post("/api/wallet-model-restore-all")
async def restore_all_model_excluded(request: Request):
    """Clear all app-model exclusions. Reversible, app-only.
    Engine, raw_live_fills, engine_truth, wallet_gate, live_config not mutated."""
    now_iso = datetime.now(timezone.utc).isoformat()
    save_ui_state({
        "wallet_model_exclude": {},
        "wallet_filter_last_result": {
            "evaluated_count": 0,
            "excluded_count": 0,
            "updated_at": now_iso,
        }
    })
    if wants_json_response(request):
        return JSONResponse({"ok": True, "ui_state": load_ui_state()})
    return RedirectResponse(f"/?v={int(time.time())}", status_code=303)


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
<div class="top"><b>Wallet Proof Engine</b><a href="/">← Dashboard</a></div>
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
<div class="section live-copy-centre" id="liveCopyPanel" data-template-version="live-copy-realised-pnl-line-v2">
  <header class="lcp-hdr">
    <div class="lcp-brand">
      <span class="lcp-brand-name">HL COPY</span>
      <span class="lc-pill lc-green" style="font-size:10px">LIVE</span>
      <span class="lcp-brand-tag">Live Copy Command Centre</span>
    </div>
    <div class="lcp-chips" id="lcHeaderChips"></div>
    <div style="display:none">
      <span id="lcWalletCount"></span>
      <span id="lcWsOverall"></span>
      <span id="lcTruthStatus"></span>
      <span id="lcRealOrders"></span>
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
        <div id="lcGcDriftBanner" style="display:none;background:rgba(245,184,75,.12);border:1px solid var(--lc-amber);border-radius:4px;padding:6px 10px;font-size:11px;color:var(--lc-amber);margin-bottom:8px"></div>
        <div id="lcGcBudgetNote" style="display:none;background:rgba(88,166,255,.08);border:1px solid var(--lc-blue);border-radius:4px;padding:6px 10px;font-size:11px;color:var(--lc-blue);margin-bottom:8px"></div>
        <div class="lc-form-grid" id="lcGcForm">
          <label>Max total live exposure ($) <input id="gcMaxTotal" type="number" min="0" step="1" placeholder="0 = disabled"></label>
          <label>Max per-asset directional exposure ($) <input id="gcMaxDir" type="number" min="0" step="1" placeholder="0 = disabled"><span style="font-size:9px;color:var(--lc-muted)"> stored; not yet enforced by Core</span></label>
          <label>Max per-wallet exposure ($) <input id="gcMaxWallet" type="number" min="0" step="1" placeholder="0 = disabled"></label>
          <label>Max per-order notional ($) <input id="gcMaxOrder" type="number" min="0" step="0.01" placeholder="0 = disabled"></label>
          <label>Marketable slippage % <input id="gcMktPct" type="number" min="0" max="0.50" step="0.01" placeholder="0 = OFF"><span id="gcMktPctState" class="lc-muted"></span></label>
          <label>Adverse close diff % <input id="gcCloseAdv" type="number" min="0" step="0.01" placeholder="0 = OFF"><span id="gcCloseAdvState" class="lc-muted"></span></label>
          <label class="wide">Symbol allowlist (empty = all allowed) <input id="gcAllowlist" type="text" placeholder="BTC,ETH — leave blank for all symbols"></label>
          <label class="wide">Symbol blocklist <input id="gcBlocklist" type="text" placeholder="DOGE,SHIB — leave blank for no blocks"></label>
        </div>
        <div id="lcGcCoreEffective" style="margin:8px 0 4px;font-size:11px;color:var(--lc-muted)"></div>
        <div class="lc-modal-actions">
          <span id="lcGcStatus" class="lc-status"></span>
          <button type="button" id="lcGcSave">Save Global Controls</button>
        </div>
      </div>
    </div>
    <span id="lcStatus" class="lc-status" style="font-size:11px;margin-left:8px"></span>
  </header>
  <div id="lcAlertBanner" style="display:none;background:#2a1218;border:1px solid rgba(255,82,99,.55);border-radius:6px;padding:8px 12px;margin-bottom:8px;color:#ff5263;font-weight:720;font-size:12px"></div>

  <div class="lcp-kpi-row" id="lcRealCards"></div>

  <section class="lc-panel lc-graph-panel">
    <div class="lcp-chart-top">
      <div>
        <h3 style="margin:0 0 4px 0">Live Account / Wallet PnL</h3>
        <p id="lcGraphSubtitle" style="margin:0;color:var(--lc-muted);font-size:11px">Portfolio value change from exchange snapshots; realised PnL from actual user closedPnl.</p>
      </div>
      <div class="lc-chart-controls lcp-chart-controls">
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
    <div class="lc-chart-legend" aria-hidden="true" style="margin:6px 0 8px">
      <span><i class="lc-line-green"></i><span id="lcGraphLegend">Exchange account value change</span></span>
      <span><i class="lc-line-blue"></i><span id="lcGraphLegend2">Realised PnL</span></span>
      <span><i class="lc-line-red"></i><span id="lcGraphLegend3">Drawdown</span></span>
      <span class="lc-muted" id="lcGraphSourceTag" style="font-size:11px">source=exchange_realised</span>
    </div>
    <div class="lc-chart-wrap lcp-chart-wrap" id="lcChartWrap">
      <svg id="lcEquityChart" viewBox="0 0 1000 330" preserveAspectRatio="none" aria-label="RAW EXCHANGE realised PnL">
        <line id="lcZeroLine" x1="58" y1="280" x2="976" y2="280" stroke="rgba(148,163,184,.6)" stroke-width="1.2" stroke-dasharray="4 4"/>
        <path id="lcExchangeFill" fill="rgba(33,193,107,.14)" d=""/>
        <polyline id="lcExchangePath" points="" fill="none" stroke="#42d97b" stroke-width="1.6"/>
        <polyline id="lcRealizedPath" points="" fill="none" stroke="#58a6ff" stroke-width="1.4" style="display:none"/>
        <polyline id="lcDrawdownPath" points="" fill="none" stroke="#ff5263" stroke-width="1.4"/>
        <line id="lcHoverCross" x1="0" y1="24" x2="0" y2="280" stroke="rgba(255,255,255,.35)" stroke-width="1" stroke-dasharray="3 3" style="display:none"/>
        <circle id="lcHoverDot" cx="0" cy="0" r="3.5" fill="#42d97b" stroke="#0b0f14" stroke-width="1.2" style="display:none"/>
        <text x="8" y="28" class="lc-axis" id="lcYMax"></text><text x="8" y="155" class="lc-axis" id="lcYMid"></text><text x="8" y="282" class="lc-axis" id="lcYMin"></text>
        <g id="lcXAxisTicks"></g>
      </svg>
      <div id="lcHoverTip" style="position:absolute;display:none;pointer-events:none;background:#0b0f14;border:1px solid #223342;border-radius:6px;padding:6px 8px;font-size:11px;color:#c9d1d9;box-shadow:0 4px 14px rgba(0,0,0,.45);white-space:nowrap;z-index:5"></div>
    </div>
  </section>

  <section class="lc-panel lc-wallet-panel">
    <div class="lcp-panel-hdr">
      <h3 style="margin:0">Wallets</h3>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <span id="lcModeCounts" style="display:contents"></span>
        <span id="lcAutoSend" class="lc-muted" style="font-size:11px">Real order sending: n/a</span>
      </div>
    </div>
    <div class="lc-table-wrap lcp-table-wrap" style="margin-top:8px">
      <table class="lcp-wallet-table lc-wallet-table" style="min-width:1380px">
        <thead><tr><th>#</th><th>Wallet</th><th>Mode / Health</th><th>PnL</th><th>Avg bps</th><th>Fills / Exits / Open</th><th>Exposure</th><th>True TS MaxDD %</th><th>Last Fill</th><th>Controls</th></tr></thead>
        <tbody id="lcWalletRows"><tr><td colspan="10">Loading live-copy config...</td></tr></tbody>
      </table>
    </div>
  </section>

  <section class="lc-tabs lcp-tabs">
    <div class="lc-tabbar lcp-tabbar">
      <button type="button" class="active" data-lc-tab="positions">Real Copy Positions</button>
      <button type="button" data-lc-tab="execqual">Execution Quality</button>
      <button type="button" data-lc-tab="audit">Order Intents / Audit</button>
      <button type="button" data-lc-tab="recon">Reconciliation</button>
      <button type="button" data-lc-tab="ws">WS Health</button>
    </div>
    <div class="lc-tab-panel lcp-tab-panel active" data-lc-panel="positions">
      <section class="lc-panel">
        <h3>Real User Copy Positions</h3>
        <p>Manual ledger vs real exchange account. All sizes signed (positive = long, negative = short). Exchange data requires HL_LIVE_HL_ACCOUNT_ADDRESS configured.</p>
        <div class="lc-table-wrap">
          <div id="lcPositionIntegrityCard" style="padding:8px 10px;border-bottom:1px solid #223342;margin-bottom:4px"></div>
          <h4>OWNED COPY POSITIONS</h4>
          <table style="min-width:1200px"><thead><tr><th>Wallet</th><th>Coin</th><th>Side</th><th>Signed size</th><th>Avg entry</th><th>Ledger/exchange</th><th>Exchange size</th><th>Last copy fill</th><th>Last updated</th></tr></thead><tbody id="lcOwnedPositionRows"><tr><td colspan="9">Loading...</td></tr></tbody></table>
          <h4 id="lcOrphanSectionTitle">Account-Level Context</h4>
          <table style="min-width:1500px"><thead><tr><th>Coin</th><th>Exchange size</th><th>Entry px</th><th>Mark px</th><th>Unrealized PnL</th><th>Status</th><th>User owner</th><th>Engine close?</th><th>Engine sleeve?</th><th>Context</th></tr></thead><tbody id="lcOrphanPositionRows"><tr><td colspan="10">Loading...</td></tr></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel lcp-tab-panel" data-lc-panel="execqual">
      <section class="lc-panel">
        <h3>Execution Quality</h3>
        <p>Real fill quality from exchange fills, send_attempts.csv, and order_intents.csv. Exchange trade feed is shown first because it is the account truth; joined rows below explain Core attribution and terminal state.</p>
        <div class="lc-source-strip" id="lcExecQualChips"></div>
        <h4>Exchange Trade Feed</h4>
        <div class="lc-table-wrap" style="max-height:360px">
          <table style="min-width:1500px"><thead><tr><th>Time</th><th>Wallet attribution</th><th>Coin</th><th>Direction</th><th>Size @ Px</th><th>Closed PnL</th><th>Fee</th><th>OID</th><th>Match proof</th></tr></thead><tbody id="lcExchangeTradeFeedRows"><tr><td colspan="9">Loading...</td></tr></tbody></table>
        </div>
        <h4>Core Execution Rows</h4>
        <div class="lc-table-wrap">
          <table style="min-width:2180px"><thead><tr><th>Time</th><th>Wallet</th><th>Coin</th><th>Side</th><th>Truth</th><th>Status</th><th>Terminal state</th><th>Operator action</th><th>Reject category</th><th>OID</th><th>Fill avg px</th><th>Size</th><th>Pos before -> after</th><th>Leader→send ms</th><th>Total ms</th><th>Symbol ms</th><th>SDK ms</th><th>Exchange ms</th><th>Error</th></tr></thead><tbody id="lcExecQualRows"><tr><td colspan="19">Loading...</td></tr></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel lcp-tab-panel" data-lc-panel="audit">
      <section class="lc-panel">
        <h3>Order Intents / Audit</h3>
        <div class="lc-source-strip" id="lcSourceChips"></div>
        <div class="lc-table-wrap">
          <table><thead><tr><th>Time</th><th>Wallet</th><th>Coin</th><th>Side</th><th>Source/Reason</th><th>Status</th><th>Decision</th><th>Decision reason</th><th>Suggested limit</th><th>Diff %</th><th>Manual?</th><th>Intent note</th><th>Real order result</th></tr></thead><tbody id="lcAuditRows"><tr><td colspan="13">Loading audit summary...</td></tr></tbody></table>
        </div>
      </section>
    </div>
    <div class="lc-tab-panel lcp-tab-panel" data-lc-panel="recon">
      <section class="lc-panel">
        <h3>Reconciliation</h3>
        <div id="lcReconSummaryCards" style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px"></div>
        <div id="lcReconSectionA"></div>
        <div id="lcReconSectionB"></div>
        <div id="lcReconSectionC"></div>
        <div id="lcReconSectionD"></div>
        <h4 style="margin-top:16px">Pending Repair Requests <span class="lc-muted" style="font-size:10px;font-weight:normal">— request-only; Core apply step required before any ledger change</span></h4>
        <div id="lcPendingRepairs" style="min-height:28px;padding:4px 0;color:var(--lc-muted);font-size:11px">Loading...</div>
        <details id="lcSpotSkipSection" style="margin-top:10px;display:none"><summary style="cursor:pointer;font-size:11px;color:var(--lc-blue);user-select:none;padding:4px 0">&#9654; Spot market skips — no action (0)</summary><div class="lc-table-wrap" style="margin-top:6px"><table><thead><tr><th>Event</th><th>Status</th><th>Action</th><th>Reject category</th><th>Terminal state</th><th>Coin</th><th>Wallet</th><th>Intent</th><th>Notes</th></tr></thead><tbody id="lcSpotSkipRows"></tbody></table></div></details>
        <details style="margin-top:14px"><summary style="cursor:pointer;color:var(--lc-muted);font-size:11px;user-select:none">&#9654; Send terminal events &amp; audit trail (raw)</summary><div style="margin-top:8px" class="lc-table-wrap"><div style="padding:6px 10px 0;font-size:11px;font-weight:760;color:var(--lc-muted)">Actionable Critical Items</div><table><thead><tr><th>Event</th><th>Status</th><th>Action</th><th>Reject category</th><th>Terminal state</th><th>Coin</th><th>Wallet</th><th>Intent</th><th>Notes</th></tr></thead><tbody id="lcReconCriticalRows"></tbody></table><div style="padding:8px 10px 0;font-size:11px;font-weight:760;color:var(--lc-muted)">Warnings</div><table><thead><tr><th>Event</th><th>Status</th><th>Action</th><th>Reject category</th><th>Terminal state</th><th>Coin</th><th>Wallet</th><th>Intent</th><th>Notes</th></tr></thead><tbody id="lcReconWarningRows"></tbody></table><div style="padding:8px 10px 0;font-size:11px;font-weight:760;color:var(--lc-muted)">Historical / Legacy</div><table><thead><tr><th>Status</th><th>Wallet</th><th>Coin</th><th>Intent</th><th>Issue</th></tr></thead><tbody id="lcReconLegacyRows"></tbody></table></div></details>
        <table style="display:none"><tbody id="lcReconRows"></tbody></table>
      </section>
    </div>
    <div class="lc-tab-panel lcp-tab-panel" data-lc-panel="ws">
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
  <div class="lc-modal-backdrop" id="lcRepairModal" aria-hidden="true">
    <div class="lc-modal">
      <div class="lc-modal-head"><h3 id="lcRepairModalTitle">Create Repair Request</h3><button type="button" id="lcRepairModalClose">close</button></div>
      <div id="lcRepairModalBody" style="margin:8px 0;color:var(--lc-text);font-size:12px;line-height:1.5"></div>
      <label style="display:block;margin:8px 0 4px;font-size:11px;color:var(--lc-muted);font-weight:760;text-transform:uppercase">Audit note<span id="lcRepairNoteReq" style="color:var(--lc-red)"> (required for ASSIGN)</span></label>
      <textarea id="lcRepairAuditNote" rows="3" style="width:100%;background:#0a1118;color:var(--lc-text);border:1px solid var(--lc-line);border-radius:4px;padding:6px;font-size:12px;resize:vertical;font-family:inherit" placeholder="Explain why this repair is needed..."></textarea>
      <div style="margin:10px 0 6px;padding:8px 10px;background:rgba(255,82,99,.12);border:1px solid var(--lc-red);border-radius:4px;font-size:11px;color:var(--lc-red)">&#9888; This request is <b>NOT applied automatically</b>. A future Core repair step is required before any ledger or exchange change occurs.</div>
      <div class="lc-modal-actions"><button type="button" id="lcRepairModalClose2">Cancel</button><button type="button" id="lcRepairSubmitBtn" style="background:var(--lc-amber);color:#000;border-color:var(--lc-amber)">Create request</button></div>
    </div>
  </div>
</div>
<style>
.live-copy-centre{--lc-bg:#070c11;--lc-panel:#0f171f;--lc-line:#223342;--lc-text:#e6edf5;--lc-muted:#8fa3b7;--lc-green:#21c16b;--lc-red:#ff5263;--lc-amber:#f5b84b;--lc-blue:#58a6ff;border-top:1px solid #30363d;margin-top:12px;color:var(--lc-text);font-size:12px}.live-copy-centre *{box-sizing:border-box}
.lcp-hdr{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:10px 14px;background:#0a1118;border-bottom:1px solid var(--lc-line);border-radius:8px 8px 0 0;margin-bottom:10px;position:relative}
.lcp-brand{display:flex;gap:7px;align-items:center;flex-shrink:0}
.lcp-brand-name{font-size:18px;font-weight:780;color:var(--lc-text);letter-spacing:.5px}
.lcp-brand-tag{font-size:12px;color:var(--lc-muted);white-space:nowrap}
.lcp-chips{display:flex;gap:6px;align-items:center;flex-wrap:wrap;flex:1}
.lc-header-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap;justify-content:flex-end;position:relative}
.lc-header-pills,.lc-safety-strip,.lc-source-strip{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.lc-pill{display:inline-flex;align-items:center;min-height:22px;padding:0 8px;border:1px solid var(--lc-line);border-radius:999px;background:#131f2b;color:var(--lc-muted);font-weight:720;white-space:nowrap;font-size:11px}
.lc-blue{color:var(--lc-blue);border-color:rgba(88,166,255,.45)}.lc-green{color:var(--lc-green);border-color:rgba(33,193,107,.45)}.lc-red{color:var(--lc-red);border-color:rgba(255,82,99,.45)}.lc-amber,.lc-mode-CLO{color:var(--lc-amber);border-color:rgba(245,184,75,.45)}
.live-copy-centre button{min-height:28px;border:1px solid var(--lc-line);border-radius:6px;background:#172437;color:var(--lc-text);padding:0 8px;font-weight:700}.live-copy-centre button.lc-soft{color:var(--lc-amber);border-color:rgba(245,184,75,.55)}.live-copy-centre button.lc-danger{color:var(--lc-red);border-color:rgba(255,82,99,.6);background:#2a1218}.live-copy-centre button[disabled]{opacity:.5}
.lc-status{font-weight:720}.lc-ok{color:var(--lc-green)}.lc-bad{color:var(--lc-red)}
.lc-panel{border:1px solid var(--lc-line);border-radius:8px;background:var(--lc-panel);padding:12px;margin-bottom:10px;min-width:0}.lc-panel h3{margin:0 0 5px 0;font-size:15px}.lc-panel p,.lc-modal p,.lc-gc-popover p{margin:0 0 10px 0;color:var(--lc-muted);font-size:12px}
.lcp-panel-hdr{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:4px}
.lcp-kpi-row{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:10px}
.lcp-kpi{border:1px solid var(--lc-line);background:var(--lc-panel);border-radius:8px;padding:10px 12px;min-height:62px}
.lcp-kpi-label{color:var(--lc-muted);font-size:10px;font-weight:720;text-transform:uppercase;margin-bottom:6px}
.lcp-kpi-val{font-size:16px;font-weight:780;word-break:break-word}
.lc-popover-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px}.lc-popover-head h3{margin:0;font-size:15px}
.lc-gc-popover{display:none;position:absolute;right:0;top:36px;width:min(640px,calc(100vw - 36px));max-height:calc(100vh - 90px);overflow:auto;z-index:40;border:1px solid var(--lc-line);border-radius:8px;background:#0d161f;padding:12px;box-shadow:0 18px 48px rgba(0,0,0,.55)}.lc-gc-popover.active{display:block}
.lc-graph-panel{min-height:430px}
.lcp-chart-top{display:grid;grid-template-columns:1fr auto;gap:12px;align-items:start;margin-bottom:4px}
.lcp-chart-controls,.lc-chart-controls{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}
.lcp-chart-controls button,.lcp-chart-controls input,.lc-chart-controls button,.lc-chart-controls input{min-height:26px;border:1px solid var(--lc-line);border-radius:6px;background:#0a1118;color:var(--lc-muted);padding:0 7px}
.lcp-chart-controls .active,.lc-chart-controls .active{color:var(--lc-text);border-color:rgba(88,166,255,.55);background:#142337}
.lc-stat-strip{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin:8px 0 10px}.lc-stat{border:1px solid var(--lc-line);background:#0a1118;border-radius:7px;padding:8px;min-height:55px}.lc-stat .label{color:var(--lc-muted);font-size:10px;font-weight:720;text-transform:uppercase}.lc-stat .value{margin-top:6px;font-size:16px;font-weight:780}
.lc-chart-legend{display:flex;gap:14px;align-items:center;flex-wrap:wrap;color:var(--lc-muted);font-weight:720}.lc-chart-legend span{display:inline-flex;gap:6px;align-items:center}.lc-chart-legend i{width:18px;height:3px;border-radius:3px;display:inline-block}.lc-line-green{background:#3fb950}.lc-line-blue{background:#58a6ff}.lc-line-red{background:#ff5263}
.lcp-chart-wrap,.lc-chart-wrap{position:relative;min-height:330px;border:1px solid var(--lc-line);border-radius:8px;background:linear-gradient(rgba(255,255,255,.035) 1px,transparent 1px) 0 0/100% 20%,linear-gradient(90deg,rgba(255,255,255,.028) 1px,transparent 1px) 0 0/10% 100%,#091017;overflow:hidden}.lcp-chart-wrap svg,.lc-chart-wrap svg{display:block;width:100%;height:100%;min-height:330px}
.lc-axis{fill:var(--lc-muted);font-size:11px}
.lcp-table-wrap,.lc-table-wrap{overflow-x:auto;border:1px solid var(--lc-line);border-radius:8px}
.live-copy-centre table{width:100%;border-collapse:collapse;min-width:1320px}.live-copy-centre th,.live-copy-centre td{border-bottom:1px solid var(--lc-line);padding:6px 7px;text-align:left;vertical-align:middle;white-space:nowrap}.live-copy-centre th{color:var(--lc-muted);font-size:10px;font-weight:780;text-transform:uppercase;background:#0a1118}
.lc-wallet{font-family:Consolas,Monaco,monospace;color:#d9ebff}.lc-cell-stack{display:grid;gap:3px}.lc-pair{display:grid;grid-template-columns:34px minmax(52px,auto);gap:5px;align-items:baseline}.lc-pair span:first-child{color:var(--lc-muted);font-size:10px;font-weight:780}
.lc-pos{color:var(--lc-green);font-weight:760}.lc-neg{color:var(--lc-red);font-weight:760}.lc-muted{color:var(--lc-muted)}.lcp-muted{color:var(--lc-muted)}
.lc-row-OFF{opacity:.58}.lc-mini-actions,.lc-inline-controls{display:flex;gap:4px;align-items:center;flex-wrap:wrap}
.lcp-wallet-table input,.lcp-wallet-table select,.lc-wallet-table input,.lc-wallet-table select{width:76px;min-height:26px;background:#0a1118;color:var(--lc-text);border:1px solid var(--lc-line);border-radius:5px;padding:0 6px}
.lcp-wallet-table select,.lc-wallet-table select{width:92px}
.lc-graph-toggle{display:inline-flex;gap:5px;align-items:center}.lc-graph-toggle input{width:14px;min-height:14px}
.lcp-tabs,.lc-tabs{display:grid;gap:8px}.lcp-tabbar,.lc-tabbar{display:flex;gap:6px;border-bottom:1px solid var(--lc-line)}.lcp-tabbar button,.lc-tabbar button{border-bottom:0;border-radius:7px 7px 0 0;color:var(--lc-muted)}.lcp-tabbar button.active,.lc-tabbar button.active{color:var(--lc-text);background:var(--lc-panel)}
.lcp-tab-panel,.lc-tab-panel{display:none}.lcp-tab-panel.active,.lc-tab-panel.active{display:block}
.lc-source-strip{margin-bottom:10px}.lcp-source-strip{margin-bottom:10px;display:flex;gap:6px;flex-wrap:wrap}
.lc-modal-backdrop,.lcp-modal-backdrop{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,.62);z-index:2000;padding:18px}.lc-modal-backdrop.active,.lcp-modal-backdrop.active{display:flex}
.lc-modal,.lcp-modal{width:min(660px,100%);border:1px solid var(--lc-line);border-radius:8px;background:var(--lc-panel);padding:14px;box-shadow:0 20px 60px rgba(0,0,0,.45)}
.lc-modal-head,.lcp-modal-hdr{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:10px}.lc-modal-head h3,.lcp-modal-hdr h3{margin:0}
.lc-form-grid,.lcp-form-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.lc-form-grid .wide,.lcp-form-grid .wide{grid-column:1/-1}.lc-form-grid label,.lcp-form-grid label{display:grid;gap:5px;color:var(--lc-muted);font-size:10px;font-weight:760;text-transform:uppercase}.lc-form-grid input,.lc-form-grid select,.lcp-form-grid input,.lcp-form-grid select{width:100%;min-height:32px;border:1px solid var(--lc-line);border-radius:6px;color:var(--lc-text);background:#0a1118;padding:0 8px}
.lc-modal-actions,.lcp-modal-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:12px;flex-wrap:wrap}
@media(max-width:1300px){.lcp-hdr{flex-wrap:wrap}.lcp-kpi-row{grid-template-columns:repeat(2,minmax(0,1fr))}.lcp-chart-top{grid-template-columns:1fr}.lc-header-actions,.lcp-chart-controls,.lc-chart-controls{justify-content:flex-start}.lc-gc-popover{left:0;right:auto}.lc-stat-strip{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:800px){.lcp-kpi-row,.lc-stat-strip,.lc-form-grid,.lcp-form-grid{grid-template-columns:1fr}.lc-gc-popover{position:static;width:100%;max-height:none;margin-top:8px}}
.lc-btn-sm{min-height:22px!important;padding:0 8px!important;font-size:11px!important;border-radius:4px!important;cursor:pointer;white-space:nowrap}.lc-btn-sm.lc-btn-warn{color:var(--lc-amber)!important;border-color:rgba(245,184,75,.55)!important}
</style>
<script>
(function(){
const root=document.getElementById('liveCopyPanel'); if(!root) return;
const status=root.querySelector('#lcStatus');
let lcConfig={wallets:{},archived_wallets:{}}, lcHealth={wallets:{}}, lcCoreRuntime={}, lcAudit={last_rows:[]};
let lcGraphMode='account', lcGraphScale='all', lcSelectedWallet='', lcGraphStartMs=0, lcGraphEndMs=0;
let lcHoverSeries=[];
function h(v){return String(v==null?'':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function msg(t,bad){if(status){status.textContent=t||'';status.className='lc-status '+(bad?'lc-bad':'lc-ok');}}
async function jget(url,timeoutMs=12000){const ctrl=new AbortController();const timer=setTimeout(()=>ctrl.abort(),timeoutMs);try{const r=await fetch(url,{headers:{'x-requested-with':'fetch'},signal:ctrl.signal});const j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.error||r.statusText);return j;}catch(e){if(e&&e.name==='AbortError')throw new Error(url+' timed out after '+timeoutMs+'ms');throw e;}finally{clearTimeout(timer);}}
async function jpost(url,payload){const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json','x-requested-with':'fetch'},body:JSON.stringify(payload)});const j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.error||r.statusText);return j;}
// intentional blank — helpers below
function num(v,d){const n=parseFloat(v);return Number.isFinite(n)?n:d;}
function isNum(v){const n=parseFloat(v);return Number.isFinite(n);}
function moneyVal(v){return isNum(v)?'$'+Number(v).toLocaleString(undefined,{maximumFractionDigits:2}):'n/a';}
function plainVal(v,digits=2){return isNum(v)?Number(v).toLocaleString(undefined,{maximumFractionDigits:digits}):'n/a';}
function first(row,keys){for(const k of keys){if(row&&row[k]!=null&&row[k]!=='')return row[k];}return '';}
function shortWallet(w){return String(w||'').length>18?String(w).slice(0,10)+'...'+String(w).slice(-6):String(w||'');}
function pill(text,kind){const t=String(text||'n/a');const raw=String(kind||t).toUpperCase();const token=raw.split(' ')[0];const cls=(['OPEN','LIVE','OK','GOOD','DRY_RUN_FILLED','ALLOWED','ACTIVE'].includes(token)||raw==='WS_OK'||raw.endsWith('_WS_OK')||raw.includes('CONFIRMED')||raw.includes('SUPPORTED')||raw.includes('MATCHED'))?'lc-green':['STALE','DEGRADED','WARN','CLO','WATCH','QUEUED','CONNECTING','RECONNECTING','PENDING'].includes(token)||raw.includes('PENDING')?'lc-amber':['OFF','OFFLINE','CLOSED','MISSING','DISABLED','ERROR','RECONNECT_OVERDUE'].includes(token)||raw.includes('REJECTED')||raw.includes('UNVERIFIED')?'lc-red':'';
 return `<span class="lc-pill ${cls}">${h(t)}</span>`;}
function terminalCls(v){const s=String(v||'').toUpperCase();if(s==='SPOT_MARKET_SKIPPED')return 'lc-blue';if(s.startsWith('CLOSE_')||s.includes('RED')||s.includes('ERROR'))return 'lc-red';if(s.startsWith('MISSED_ENTRY')||s.startsWith('MISSED_ADD')||s==='FILLED_AWAITING_COPY_POLL')return 'lc-amber';if(s==='ORDER_FILLED'||s==='FILLED_CONFIRMED'||s==='MATCH')return 'lc-green';return '';}
function pnlReasonText(v){
 const raw=String(v||'');
 const s=raw.toUpperCase();
 if(!s||s==='N/A'||s==='NA'||s==='NO PNL YET')return '';
 if(s.includes('SHARED_COIN_PNL_NOT_ATTRIBUTED')||s.includes('AMBIGUOUS_COIN_SHARED'))return 'shared symbol: account-level only';
 if(s.includes('WALLET_REALIZED_REQUIRES_EXACT_EXCHANGE_FILL_ID')||s.includes('ACCOUNT_LEVEL_ONLY'))return 'account-level only';
 return raw;
}
function truthCls(v){const s=String(v||'').toUpperCase();if(s==='SPOT_SKIPPED_INFO')return 'lc-blue';if(s.includes('ACTIVE')&&s.includes('RED'))return 'lc-red';if(s.startsWith('ACTIVE_EXCHANGE_REJECT'))return 'lc-red';if(s.startsWith('ACTIVE_'))return 'lc-amber';if(s.startsWith('HISTORICAL'))return 'lc-blue';if(s.startsWith('RECENT_RECONCILIATION'))return 'lc-blue';if(s.includes('ADOPTED')||s.includes('RECONCILED'))return 'lc-green';return '';}
function statusCard(label,value,kind,sub){return `<div class="lc-stat"><div class="label">${h(label)}</div><div class="value ${kind||''}" style="font-size:13px;word-break:break-word">${h(value||'—')}</div>${sub?`<div class="lc-muted" style="font-size:10px;margin-top:4px">${h(sub)}</div>`:''}</div>`;}
function decisionPill(text){const t=String(text||'—');const u=t.toUpperCase();const cls=['WOULD_PLACE_IOC_LIMIT','WOULD_LATE_COPY','WOULD_REDUCE_OR_EXIT','WOULD_EXIT','WOULD_REDUCE','ENTRY_ALLOWED','EXIT_ALLOWED','ADD_ALLOWED'].includes(u)?'lc-green':u==='DO_NOT_MARKET_COPY'?'lc-red':(u==='MANUAL_REVIEW'||u.includes('BLOCKED')||u.includes('TERMINAL')||u.includes('NO_SEND'))?'lc-amber':'';return `<span class="lc-pill ${cls}">${h(t)}</span>`;}
function tiny(v,n){const s=String(v==null?'':v);return h(s.length>n?s.slice(0,n-1)+'…':s);}
function pair(a,b,cls){return `<div class="lc-pair"><span>${h(a)}</span><b class="${cls||''}">${h(b||'—')}</b></div>`;}
function signed(v){const s=String(v||'—');return s.trim().startsWith('-')?'lc-neg':(s.trim().startsWith('+')?'lc-pos':'');}
function count(obj,key){return Number((obj||{})[key]||0);}
function sourceOf(r){return first(r,['source','fill_source'])||String(first(r,['reason'])).replace('LIVE_','').replace('_DETECTED','')||'—';}
function rowPayload(tr){return {wallet:tr.dataset.wallet,mode:tr.querySelector('[name=mode]').value,copy_mode:tr.querySelector('[name=copy_mode]').value,norm_base:num(tr.querySelector('[name=norm_base]').value,100),fixed_notional:num(tr.querySelector('[name=fixed_notional]').value,10),leader_equity_base:num(tr.querySelector('[name=leader_equity_base]').value,10000),max_diff_pct:num(tr.querySelector('[name=max_diff_pct]').value,0.1),daily_loss_limit:num(tr.querySelector('[name=daily_loss_limit]').value,0)};}
function age(ms){const n=Number(ms||0);if(!n)return '—';const d=Math.max(0,Date.now()-n);return d<60000?Math.round(d/1000)+'s':Math.round(d/60000)+'m';}function time(ms){const n=Number(ms||0);return n?new Date(n).toLocaleTimeString():'—';}
function tsOf(p){const raw=p.fetched_at_ms||p.timestamp_ms||p.time_ms||p.ts; if(Number(raw)>0)return Number(raw); const s=p.timestamp||p.updated_at||p.created_at||p.time||''; const t=Date.parse(s); return Number.isFinite(t)?t:0;}
function localInputMs(v){const t=Date.parse(v||'');return Number.isFinite(t)?t:0;}
function graphRangeLabel(){if(lcGraphScale==='custom'){const s=lcGraphStartMs?new Date(lcGraphStartMs).toLocaleString():'start';const e=lcGraphEndMs?new Date(lcGraphEndMs).toLocaleString():'now';return `${s} to ${e}`;}return lcGraphScale.toUpperCase();}
function filterScale(points){let start=0,end=0;if(lcGraphScale==='custom'){start=lcGraphStartMs;end=lcGraphEndMs;}else if(lcGraphScale!=='all'){const days=lcGraphScale==='7d'?7:1;start=Date.now()-days*86400000;}return points.filter(p=>{const t=tsOf(p);if(!t)return false;if(start&&t<start)return false;if(end&&t>end)return false;return true;});}
function graphRangeBounds(){
 let start=0,end=0,label=graphRangeLabel();
 if(lcGraphScale==='custom'){start=lcGraphStartMs;end=lcGraphEndMs;}
 else if(lcGraphScale==='7d'){start=Date.now()-7*86400000;label='7D';}
 else if(lcGraphScale==='1d'){start=Date.now()-86400000;label='1D';}
 else {
  const g=lcAudit.live_graph||{};
  start=Date.parse(g.realized_pnl_selected_start||'')||0;
  end=Date.parse(g.realized_pnl_selected_end||'')||0;
  label=(g.realized_pnl_selected_label||'ALL').toUpperCase();
 }
 return {start:start||0,end:end||0,label:label};
}
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

// Draw equity curve with true time-series DD overlay for wallet detail modal
function drawEquityCurveWithDD(data){
  const canvas=root.querySelector('#lcEquityCurveCanvas');
  if(!canvas) return;
  const ctx=canvas.getContext('2d');
  const timestamps=data.timestamps||[];
  const equity=data.equity||[];
  const drawdownPct=data.drawdown_pct||[];
  if(!timestamps.length||!equity.length) return;

  const width=canvas.width;
  const height=canvas.height;
  const padding={top:20,right:60,bottom:40,left:60};
  const plotWidth=width-padding.left-padding.right;
  const plotHeight=height-padding.top-padding.bottom;

  // Clear canvas
  ctx.clearRect(0,0,width,height);

  // Draw grid
  ctx.strokeStyle='#223342';
  ctx.lineWidth=0.5;
  for(let i=0;i<=4;i++){
    const y=padding.top+(plotHeight/4)*i;
    ctx.beginPath();
    ctx.moveTo(padding.left,y);
    ctx.lineTo(width-padding.right,y);
    ctx.stroke();
  }
  for(let i=0;i<=6;i++){
    const x=padding.left+(plotWidth/6)*i;
    ctx.beginPath();
    ctx.moveTo(x,padding.top);
    ctx.lineTo(x,height-padding.bottom);
    ctx.stroke();
  }

  // Equity scale (left axis)
  const equityMin=Math.min(...equity);
  const equityMax=Math.max(...equity);
  const equityRange=equityMax-equityMin||1;

  // Drawdown scale (right axis) - 0 to max drawdown
  const ddMax=Math.max(...drawdownPct.map(d=>Math.abs(d)),1);

  // Draw equity line (green)
  ctx.strokeStyle='#21c16b';
  ctx.lineWidth=2;
  ctx.beginPath();
  equity.forEach((val,i)=>{
    const x=padding.left+(plotWidth/(equity.length-1))*i;
    const y=padding.top+plotHeight-((val-equityMin)/equityRange)*plotHeight;
    if(i===0) ctx.moveTo(x,y);
    else ctx.lineTo(x,y);
  });
  ctx.stroke();

  // Draw equity area fill
  ctx.fillStyle='rgba(33,193,107,0.12)';
  ctx.lineTo(padding.left+plotWidth,height-padding.bottom);
  ctx.lineTo(padding.left,height-padding.bottom);
  ctx.closePath();
  ctx.fill();

  // Draw drawdown line (red) on right axis
  ctx.strokeStyle='#ff5263';
  ctx.lineWidth=1.5;
  ctx.beginPath();
  drawdownPct.forEach((val,i)=>{
    const x=padding.left+(plotWidth/(drawdownPct.length-1))*i;
    const y=padding.top+plotHeight-(Math.abs(val)/ddMax)*plotHeight;
    if(i===0) ctx.moveTo(x,y);
    else ctx.lineTo(x,y);
  });
  ctx.stroke();

  // Draw axes labels
  ctx.fillStyle='#8fa3b7';
  ctx.font='10px Inter, Segoe UI, Arial';
  ctx.textAlign='right';
  // Left axis (equity)
  for(let i=0;i<=4;i++){
    const val=equityMax-(equityRange/4)*i;
    const y=padding.top+(plotHeight/4)*i+3;
    ctx.fillText('$'+(val/1000).toFixed(1)+'k',padding.left-8,y);
  }
  // Right axis (drawdown %)
  ctx.textAlign='left';
  for(let i=0;i<=4;i++){
    const val=(ddMax/4)*i;
    const y=padding.top+(plotHeight/4)*i+3;
    ctx.fillText(val.toFixed(1)+'%',width-padding.right+8,y);
  }
  // Bottom axis (time)
  ctx.textAlign='center';
  for(let i=0;i<=6;i++){
    const idx=Math.floor((timestamps.length-1)*i/6);
    const ts=timestamps[idx];
    const date=new Date(ts);
    const label=date.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
    const x=padding.left+(plotWidth/6)*i;
    ctx.fillText(label,x,height-padding.bottom+18);
  }

  // Axis titles
  ctx.fillStyle='#8fa3b7';
  ctx.font='11px Inter, Segoe UI, Arial';
  ctx.textAlign='center';
  ctx.fillText('Equity (USD)',padding.left/2,height/2);
  ctx.save();
  ctx.translate(width-padding.right/2,height/2);
  ctx.rotate(Math.PI/2);
  ctx.fillText('Drawdown %',0,0);
  ctx.restore();
}
function renderGraph(){
 const sub=root.querySelector('#lcGraphSubtitle');
 const legend=root.querySelector('#lcGraphLegend');
 const legend2=root.querySelector('#lcGraphLegend2');
 const realizedPath=root.querySelector('#lcRealizedPath');
 const drawdownPath=root.querySelector('#lcDrawdownPath');
 const zeroLine=root.querySelector('#lcZeroLine');
 let points=[], realizedPoints=[], drawdownPoints=[], fillsInRange=[], label='Exchange account value change', sourceTag='exchange_account_history_and_realised_fills';
 if(lcGraphMode==='account'){
  // SOURCE-OF-TRUTH: green line is exchange account/portfolio value change;
  // blue line is cumulative realised closedPnl from actual executed fills.
  // No modelled, simulated, or SSOT portfolio values are used here.
  const rawFillPts=(lcAudit.live_graph||{}).realized_pnl_points||[];
  const allRealizedFills=rawFillPts
   .map(p=>({timestamp_ms:tsOf(p), timestamp:p.timestamp||new Date(tsOf(p)||0).toISOString(), pnl:num(p.pnl,0), coin:p.coin||'', side:p.side||'', oid:p.oid||''}))
   .filter(p=>p.timestamp_ms>0&&isNum(p.pnl))
   .sort((a,b)=>a.timestamp_ms-b.timestamp_ms);
  const bounds=graphRangeBounds();
  const inBounds=p=>(!bounds.start||p.timestamp_ms>=bounds.start)&&(!bounds.end||p.timestamp_ms<=bounds.end);
  if(allRealizedFills.length){
   // Build the realised series from real exchange closedPnl fills in the selected
   // range. ALL uses the same selected window as the Realized PnL KPI so the
   // final chart value reconciles to the header instead of a rebased slice.
   fillsInRange=allRealizedFills.filter(inBounds);
   if(fillsInRange.length<1){
    realizedPoints=[];
   } else {
    let run=0;
    const baselineTs=bounds.start||fillsInRange[0].timestamp_ms;
    realizedPoints=[{timestamp:new Date(baselineTs).toISOString(),timestamp_ms:baselineTs,value:0}];
    fillsInRange.forEach(p=>{run+=p.pnl;realizedPoints.push({timestamp:p.timestamp,timestamp_ms:p.timestamp_ms,value:run,coin:p.coin,side:p.side,oid:p.oid});});
    const nowMs=Date.now();
    const rightEdge=bounds.end||nowMs;
    if(realizedPoints[realizedPoints.length-1].timestamp_ms < rightEdge) realizedPoints.push({timestamp:new Date(rightEdge).toISOString(),timestamp_ms:rightEdge,value:realizedPoints[realizedPoints.length-1].value});
   }
  }
  const acctPts=((lcAudit.live_graph||{}).account_points||lcAudit.exchange_account_history||[])
   .map(p=>({timestamp:p.timestamp||p.updated_at||p.created_at||'',timestamp_ms:tsOf(p),value:num(p.unified_portfolio_value??p.account_value,null)}))
   .filter(p=>p.timestamp_ms>0&&isNum(p.value))
   .sort((a,b)=>a.timestamp_ms-b.timestamp_ms);
  const acctInRange=acctPts.filter(inBounds);
  if(acctInRange.length>=2){
   const base=acctInRange[0].value;
   points=acctInRange.map((p,i)=>({timestamp:p.timestamp,timestamp_ms:p.timestamp_ms,value:i===0?0:p.value-base}));
  } else if(realizedPoints.length>=2) {
   // If account snapshots are unavailable, keep the chart useful and truthful by
   // falling back to the realised series, while the subtitle names the fallback.
   label='RAW EXCHANGE — Realised PnL';
   sourceTag='exchange_realised_fills_no_account_history';
   points=realizedPoints;
  } else {
   clearGraphText(`No exchange account snapshots or executed fills in selected range (${graphRangeLabel()}).`,label);
   return;
   }
  // Drawdown from the account-value series.
  let peak=0;
  drawdownPoints=points.map(p=>{peak=Math.max(peak,p.value);return {timestamp:p.timestamp,timestamp_ms:p.timestamp_ms,value:p.value-peak};});
 } else {
  const perf=(lcAudit.live_leader_performance||{})[String(lcSelectedWallet||'').toLowerCase()]||{};
  const trueCurve=perf.true_ts_equity_series||[];
  const src=lcGraphMode==='wallet_pnl'?trueCurve:(perf.exposure_series||[]);
  label=lcGraphMode==='wallet_pnl'?'Selected wallet true equity change':'Selected wallet exposure';
  if(lcGraphMode==='wallet_pnl'&&trueCurve.length){
   const base=num(trueCurve[0].equity_usd,0);
   points=trueCurve.map(p=>({timestamp:p.timestamp||'', timestamp_ms:tsOf(p), value:num(p.equity_usd,0)-base, equity_usd:num(p.equity_usd,0), running_peak_usd:num(p.running_peak_usd,0)}));
   drawdownPoints=(perf.true_ts_drawdown_series||[]).map(p=>({timestamp:p.timestamp||'', timestamp_ms:tsOf(p), value:num(p.value,0), drawdown_pct:num(p.drawdown_pct,0), equity_usd:num(p.equity_usd,0), running_peak_usd:num(p.running_peak_usd,0)}));
   sourceTag='8012_accountValueHistory_true_ts';
  } else {
   points=src.map(p=>({timestamp:p.timestamp||p.updated_at||'', timestamp_ms:tsOf(p), value:num(p.value,0)}));
  }
  if(!lcSelectedWallet){
   clearGraphText('No wallet selected — click a wallet row in the Wallets table below to load its '+(lcGraphMode==='wallet_pnl'?'PnL':'exposure')+' series.',label);
   return;
  }
  if(!points.length){
   clearGraphText('Wallet "'+lcSelectedWallet+'" has no '+(lcGraphMode==='wallet_pnl'?'8012 accountValueHistory equity':'exposure_series')+' data yet.',label);
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
 const timeSeries=points.concat(realizedPoints).filter(p=>p.timestamp_ms);
 const minTs=Math.min(...timeSeries.map(p=>p.timestamp_ms)), maxTs=Math.max(...timeSeries.map(p=>p.timestamp_ms)), trng=maxTs-minTs||1;
 const getX=p=>plot.left+(((p.timestamp_ms||minTs)-minTs)/trng)*(plot.right-plot.left);
 const pts=linePts(points,getX,getY2);
 root.querySelector('#lcExchangePath').setAttribute('points',pts);
 const fx=getX(points[0]),lx=getX(points[points.length-1]);
 root.querySelector('#lcExchangeFill').setAttribute('d',`M${fx} ${plot.bottom} L${pts} L${lx} ${plot.bottom} Z`);
 if(realizedPath){realizedPath.style.display=realizedPoints.length?'':'none';realizedPath.setAttribute('points',realizedPoints.length?linePts(realizedPoints,getX,getY2):'');}
 if(drawdownPath) drawdownPath.setAttribute('points',drawdownPoints.length?linePts(drawdownPoints,getX,getY2):'');
 if(zeroLine){const zy=getY2(0);zeroLine.setAttribute('x1',plot.left);zeroLine.setAttribute('x2',plot.right);zeroLine.setAttribute('y1',zy);zeroLine.setAttribute('y2',zy);}
 // Stash series + transform for hover handler.
 lcHoverSeries={points:points,realized:realizedPoints,drawdown:drawdownPoints,getX:getX,getY:getY2,plot:plot,sourceTag:sourceTag,label:label};
 renderXTicks(minTs,maxTs,getX);
 root.querySelector('#lcYMax').textContent=fmtAxisMoney(ymax);
 root.querySelector('#lcYMid').textContent=fmtAxisMoney(ymin+rng/2);
 root.querySelector('#lcYMin').textContent=fmtAxisMoney(ymin);
 if(legend) legend.textContent=label;
 if(legend2) legend2.textContent=lcGraphMode==='account'&&realizedPoints.length?'Realised PnL':'';
  const legend3=root.querySelector('#lcGraphLegend3'); if(legend3) legend3.textContent=(lcGraphMode==='account'||lcGraphMode==='wallet_pnl')?'Drawdown':'';
 const srcTag=root.querySelector('#lcGraphSourceTag'); if(srcTag) srcTag.textContent='source='+sourceTag;
 const lastVal=points.length?points[points.length-1].value:0;
 const realizedLast=realizedPoints.length?realizedPoints[realizedPoints.length-1].value:null;
 const totalFillCount=((lcAudit.live_graph||{}).realized_pnl_point_count||((lcAudit.live_graph||{}).realized_pnl_points||[]).length||0);
  if(sub) sub.textContent=lcGraphMode==='account'?`Exchange account value change=${(lastVal>=0?'+$':'-$')+Math.abs(lastVal).toFixed(2)}; realised PnL=${realizedLast==null?'n/a':((realizedLast>=0?'+$':'-$')+Math.abs(realizedLast).toFixed(2))}. ${graphRangeLabel()} range — ${fillsInRange.length}/${totalFillCount} fills [source=${sourceTag}].`:`${label}. ${graphRangeLabel()} range, ${points.length} data points [source=${sourceTag}].`;
}
function renderCards(){
 const snap=lcAudit.exchange_account_snapshot||{}, manual=lcAudit.manual_live_summary||{};
 const cardPending=lcAudit.load_error?'stats pending':'no data';
 const pv=snap.available&&snap.unified_portfolio_value!=null
  ?('$'+Number(snap.unified_portfolio_value||0).toLocaleString(undefined,{maximumFractionDigits:2})):cardPending;
 const upnl=snap.available&&snap.unrealized_pnl!=null
  ?Number(snap.unrealized_pnl):null;
 const rpnl=snap.realized_pnl_selected!=null?Number(snap.realized_pnl_selected):null;
 const allRows=lcAudit.live_wallet_rows||[];
 const bpsVals=allRows.map(r=>r.avg_diff_bps).filter(v=>v!=null);
 const avgBps=bpsVals.length?(bpsVals.reduce((a,b)=>a+b,0)/bpsVals.length):null;
 const kpis=[
  {label:'Realized PnL',val:rpnl!=null?('$'+rpnl.toLocaleString(undefined,{maximumFractionDigits:2})+' '+(snap.realized_pnl_selected_label||'')):cardPending,cls:rpnl!=null?(rpnl>=0?'lc-pos':'lc-neg'):''},
  {label:'Unrealized PnL',val:upnl!=null?('$'+upnl.toLocaleString(undefined,{maximumFractionDigits:2})):cardPending,cls:upnl!=null?(upnl>=0?'lc-pos':'lc-neg'):''},
  {label:'Portfolio Value',val:pv,cls:''},
  {label:'Avg Copy bps',val:avgBps!=null?(avgBps.toFixed(1)+' bps'):cardPending,cls:avgBps!=null&&avgBps>10?'lc-neg':avgBps!=null&&avgBps<=0?'lc-pos':''},
 ];
 const box=root.querySelector('#lcRealCards');
 if(box) box.innerHTML=kpis.map(k=>`<div class="lcp-kpi"><div class="lcp-kpi-label">${h(k.label)}</div><div class="lcp-kpi-val ${k.cls}">${k.val}</div></div>`).join('');
 renderGraph();
}
function renderTopStatus(){
 const st=lcAudit.live_top_status||{};
 const pic=lcAudit.position_integrity_card||{};
 const auditDegraded=!!lcAudit.load_error||!!lcAudit.error;
 const integ=String(lcCoreRuntime.integrity_status||st.integrity_status||pic.core_status||(auditDegraded?'DEGRADED':'UNKNOWN')).toUpperCase();
 const integrityOk=integ==='GREEN';
 const wsSummary=lcHealth.ws_summary||{};
 const ws=String(st.ws_status||lcCoreRuntime.ws_status||wsSummary.ws_status||lcHealth.overall||'UNKNOWN').toUpperCase();
 const wsOk=ws==='WS_OK';
 const poll=String(st.poll_status||lcCoreRuntime.poll_loop_status||'UNKNOWN').toUpperCase();
 const copyPoll=String(st.copy_poll_status||lcCoreRuntime.copy_account_status||'UNKNOWN').toUpperCase();
 const snapshot=String(st.snapshot_status||lcCoreRuntime.exchange_recon_status||lcAudit.exchange_account_snapshot?.status||'UNKNOWN').toUpperCase();
 const ordersOn=st.master_real_orders==='ON'||lcCoreRuntime.effective_real_orders_enabled===true||lcCoreRuntime.master_real_orders_enabled===true||lcConfig.auto_send_enabled===true;
 const rtCounts=lcCoreRuntime.integrity_counts||{};
 const rtHard=lcCoreRuntime.hard_copy_counts||{};
 const activeRed=Number(rtHard.ACTIVE_RED??pic.hard_copy_active_red??rtCounts.hard_copy_active_red??0);
 const unclassified=Number(rtHard.UNCLASSIFIED??pic.hard_copy_unclassified??rtCounts.hard_copy_unclassified??0);
 const missedEntry=Number(rtCounts.missed_entry??pic.missed_entry??0);
 const missedAdd=Number(rtCounts.missed_add??pic.missed_add??0);
 const mismatch=Number(rtCounts.exchange_manual_mismatch??pic.exchange_manual_mismatch_count??0);
 const wct=lcAudit.wallet_control_truth||{};
 const driftCount=Number(wct.drift_count||0);
 const wsDriftCount=Number(wct.ws_drift_count||0);
 const archiveResidualCount=Number(wct.archive_residual_count||0);
 const missedCopy=missedEntry+missedAdd;
 const recoveryActive=Number(rtHard.EXIT_RECOVERY_ACTIVE??rtCounts.exit_recovery_active??0);
 const fullAudit=lcAudit.full_exchange_audit||{};
 const cleanReplay=fullAudit.clean_replay||{};
 const cleanReplayStatus=String(cleanReplay.status||'').toUpperCase();
 const cleanReplayActionable=Number(cleanReplay.actionable_without_active_recovery_count||0);
 const cleanReplayRecovery=Number(cleanReplay.active_bug_cleanup_recovery_count||0);
 const cleanReplayContaminated=Number(cleanReplay.contaminated_sleeve_count||0);
 const hotpathIssues=activeRed+unclassified+driftCount;
 const historicalTerminalIssues=missedCopy;
 const positionIssues=mismatch;
 const snapshotOk=snapshot==='SNAPSHOT_OK'||snapshot==='OK'||lcAudit.exchange_account_snapshot?.available===true;
 const coreHotpathOk=wsOk&&poll==='POLL_OK'&&copyPoll==='COPY_ACCOUNT_POLLED'&&snapshotOk&&activeRed===0&&unclassified===0&&driftCount===0&&cleanReplayActionable===0;
 const integrityDisplay=cleanReplayActionable>0?'RED':(cleanReplayStatus==='AMBER'?'AMBER':(coreHotpathOk&&hotpathIssues===0&&positionIssues===0)?'GREEN':(coreHotpathOk&&hotpathIssues===0&&positionIssues>0?'RECONCILE':integ));
 const integrityCls=integrityDisplay==='GREEN'||integrityDisplay==='SAFE'?'lc-green':integrityDisplay==='RECONCILE'||integrityDisplay==='AMBER'||integrityDisplay==='DEGRADED'?'lc-amber':'lc-red';
 const wallets=lcConfig.wallets||{};
 const walletEntries=Object.values(wallets);
 const liveCount=walletEntries.filter(w=>String(w&&w.mode||'').toUpperCase()==='LIVE').length;
 const cloCount=walletEntries.filter(w=>String(w&&w.mode||'').toUpperCase()==='CLO').length;
 const offCount=walletEntries.filter(w=>String(w&&w.mode||'').toUpperCase()==='OFF').length;
 let coreHotpathLabel='CHECK';
 if(coreHotpathOk) coreHotpathLabel='SAFE';
 else if(activeRed>0) coreHotpathLabel='RED (active)';
 else if(unclassified>0) coreHotpathLabel='RED (unclassified)';
 else if(driftCount>0) coreHotpathLabel='CONFIG DRIFT';
 else if(!wsOk||poll!=='POLL_OK'||copyPoll!=='COPY_ACCOUNT_POLLED'||!snapshotOk) coreHotpathLabel='DOWN/STALE';
 const chips=[
  [`Core hotpath: ${coreHotpathLabel}`, coreHotpathOk?'lc-green':'lc-red'],
  [`WS: ${wsOk?'OK':'DOWN'} (Core runtime)`, wsOk?'lc-green':'lc-red'],
  [`Orders: ${ordersOn?'ON':'OFF'} (Core runtime)`, ordersOn?'lc-green':'lc-red'],
  [`LIVE ${liveCount} / CLO ${cloCount} / OFF ${offCount}`, ''],
  [`Integrity: ${integrityDisplay}`, integrityCls],
  [hotpathIssues===0?'Hotpath issues: 0':`Hotpath issues: ${hotpathIssues}`, hotpathIssues===0?'lc-green':'lc-red'],
  ...(historicalTerminalIssues>0?[[`Historical missed terminal: ${historicalTerminalIssues}`,'lc-amber']]:[]),
  ...(recoveryActive>0?[[`Close recovery active: ${recoveryActive}`,'lc-amber']]:[]),
  ...(cleanReplayActionable>0?[[`Clean replay unmanaged: ${cleanReplayActionable}`,'lc-red']]:[]),
  ...(cleanReplayActionable===0&&cleanReplayRecovery>0?[[`Bug cleanup recovery: ${cleanReplayRecovery}`,'lc-amber']]:[]),
  ...(cleanReplayActionable===0&&cleanReplayRecovery===0&&cleanReplayContaminated>0?[[`Historical contamination named: ${cleanReplayContaminated}`,'lc-amber']]:[]),
  ...(positionIssues>0?[[`Position reconcile: ${positionIssues}`,'lc-amber']]:[]),
  ...(auditDegraded?[[`${lcAudit.last_good_persisted_at?'Stats stale':'Stats degraded'}: ${tiny(lcAudit.load_error||lcAudit.error,42)}`,'lc-amber']]:[]),
  ...(driftCount>0?[[`Config Drift: ${driftCount} — RELOAD REQUIRED`,'lc-red']]:[]),
  ...(wsDriftCount>0&&driftCount===0?[[`WS Drift: ${wsDriftCount} — restart recommended`,'lc-amber']]:[]),
  ...(archiveResidualCount>0&&driftCount===0&&wsDriftCount===0?[[`Archive residual accounted: ${archiveResidualCount}`,'lc-green']]:[]),
 ];
 const chipBox=root.querySelector('#lcHeaderChips');
 if(chipBox) chipBox.innerHTML=chips.map(([t,c])=>`<span class="lc-pill ${c}">${h(t)}</span>`).join('');
 const banner=root.querySelector('#lcAlertBanner');
 if(banner){
  if(cleanReplayActionable>0){
   banner.style.background='#2a1218';
   banner.style.borderColor='rgba(255,82,99,.55)';
   banner.style.color='#ff5263';
   banner.style.display='block';
   banner.textContent=`ACTION REQUIRED: clean replay shows ${cleanReplayActionable} current bug-contaminated sleeve(s) without active recovery. Do not treat exchange/manual balance as green.`;
  } else if(cleanReplayRecovery>0){
   banner.style.background='rgba(245,184,75,.12)';
   banner.style.borderColor='rgba(245,184,75,.55)';
   banner.style.color='#f5b84b';
   banner.style.display='block';
   banner.textContent=`Bug cleanup recovery active: ${cleanReplayRecovery} reduce-only leader-exit limit order(s) are resting; exchange/manual may balance, but clean replay remains amber until filled or reviewed.`;
  } else if(hotpathIssues>0){
   const reasons=[];
   if(activeRed>0) reasons.push(`active_red=${activeRed}`);
   if(unclassified>0) reasons.push(`unclassified=${unclassified}`);
   if(driftCount>0) reasons.push(`config_drift=${driftCount}—Core_reload_required`);
   banner.style.background='#2a1218';
   banner.style.borderColor='rgba(255,82,99,.55)';
   banner.style.color='#ff5263';
   banner.style.display='block';
   banner.textContent='⚠ ACTION REQUIRED: '+reasons.join(', ');
  } else if(recoveryActive>0){
   banner.style.background='rgba(245,184,75,.12)';
   banner.style.borderColor='rgba(245,184,75,.55)';
   banner.style.color='#f5b84b';
   banner.style.display='block';
   banner.textContent=`Close recovery active: ${recoveryActive} reduce-only limit close order(s) are resting; position remains open until filled or manually reviewed.`;
  } else if(positionIssues>0){
   const reasons=[];
   if(mismatch>0) reasons.push(`exchange/manual position reconcile=${mismatch}`);
   if(historicalTerminalIssues>0) reasons.push(`historical missed terminal=${historicalTerminalIssues} (not active hotpath)`);
   if(archiveResidualCount>0) reasons.push(`archive residual=${archiveResidualCount} (not hotpath)`);
   banner.style.background='rgba(245,184,75,.12)';
   banner.style.borderColor='rgba(245,184,75,.55)';
   banner.style.color='#f5b84b';
   banner.style.display='block';
   banner.textContent='Position reconciliation notice: '+reasons.join(', ');
  } else if(archiveResidualCount>0){
   banner.style.background='rgba(38,217,124,.10)';
   banner.style.borderColor='rgba(38,217,124,.45)';
   banner.style.color='#26d97c';
   banner.style.display='block';
   banner.textContent=`Archived ledger residual accounted: ${archiveResidualCount} (active ledger matches exchange; not hotpath)`;
  } else {
   banner.style.display='none';
  }
 }
 const box=root.querySelector('#lcTruthStatus');
 if(box) box.innerHTML='';
}
function moneyFmt(v){return v!=null?('$'+Number(v).toLocaleString(undefined,{maximumFractionDigits:2})):null;}
function notProven(reason){return `<span class="lc-pill lc-amber">${h(reason||'not provable from exact wallet PnL')}</span>`;}
function provenMoneyOrUnknown(v){return v!=null?moneyFmt(v):notProven();}
function signCls(v){return v!=null?(v>0?'lc-pos':v<0?'lc-neg':''):''}
function pnlLabel(st,label){const m={'EXACT':'lc-green','EXACT_REALIZED_ONLY_OPEN_SHARED':'lc-amber','ESTIMATED_FROM_REAL_ORDER_FILLS':'lc-amber','OPEN_ONLY':'lc-blue','ACCOUNT_LEVEL_ONLY':'lc-amber','AMBIGUOUS_COIN_SHARED':'lc-red','N/A':''}; const text=label||({'OPEN_ONLY':'Open PnL','EXACT_REALIZED_ONLY_OPEN_SHARED':'Exact realized only; open shared','ESTIMATED_FROM_REAL_ORDER_FILLS':'Real fills','ACCOUNT_LEVEL_ONLY':'Account-level only','AMBIGUOUS_COIN_SHARED':'Shared coin','N/A':'No PnL yet','EXACT':'Exact closed PnL'}[st]||st||'No PnL yet'); return `<span class="lc-pill ${m[st]||''}">${h(text)}</span>`;}
function walletRowsFromFastPath(){
 const wallets=lcConfig.wallets||{};
 const wsWallets=lcHealth.wallets||{};
 const wsOverall=String((lcHealth.ws_summary||{}).ws_status||lcHealth.overall||'UNKNOWN');
 return Object.entries(wallets).map(([wallet,cfg])=>{
  const wh=wsWallets[String(wallet).toLowerCase()]||wsWallets[wallet]||{};
  const stale=wh.stale===true;
  return {
   wallet,
   mode:String(cfg.mode||'OFF').toUpperCase(),
   copy_mode:String(cfg.copy_mode||'proportional'),
   fixed_notional:cfg.fixed_notional,
   norm_base:cfg.norm_base??cfg.normalized_equity_base,
   leader_equity_base:cfg.leader_equity_base,
   max_diff_pct:cfg.max_diff_pct,
   daily_loss_limit:cfg.daily_loss_limit,
   ws_state:stale?'STALE':wsOverall,
   ws_reason:`Fast path: live-config + live-ws-health${lcAudit.load_error?'; stats degraded: '+lcAudit.load_error:''}`,
   core_truth_status:wsOverall==='WS_OK'?'CORE_CONFIRMED_LIVE':'CORE_CONFIRMATION_UNAVAILABLE',
   realized_pnl:null,unrealized_pnl:null,net_pnl:null,avg_diff_bps:null,
   filled_count:null,exits_count:null,open_position_count:null,current_exposure:null,
   last_fill:null,
  };
 });
}
function renderWallets(){
 const cfgWallets=lcConfig.wallets||{};
 const cfgByWallet={};
 Object.entries(cfgWallets).forEach(([wallet,cfg])=>{cfgByWallet[String(wallet).toLowerCase()]=Object.assign({wallet},cfg||{});});
 const auditRows=(lcAudit.live_wallet_rows&&lcAudit.live_wallet_rows.length)?lcAudit.live_wallet_rows:null;
 const wrows=(auditRows||walletRowsFromFastPath()).map(row=>{
  const wallet=String(row.wallet||'').toLowerCase();
  const cfg=cfgByWallet[wallet]||{};
  return Object.assign({},row,{
   mode:cfg.mode??row.mode,
   copy_mode:cfg.copy_mode??row.copy_mode,
   norm_base:cfg.norm_base??row.norm_base,
   fixed_notional:cfg.fixed_notional??row.fixed_notional,
   leader_equity_base:cfg.leader_equity_base??row.leader_equity_base,
   max_diff_pct:cfg.max_diff_pct??row.max_diff_pct,
   daily_loss_limit:cfg.daily_loss_limit??row.daily_loss_limit
  });
 });
 const autoLiveWallets=(lcAudit.auto_live_eligible_wallets||[]).map(w=>String(w).toLowerCase());
 const rows=wrows.map((d,i)=>{
  const mode=String(d.mode||'OFF').toUpperCase();
  const model=String(d.copy_mode||'')==='fixed'?'fixed':'prop';
  const modeCls=mode==='LIVE'?'lc-green':mode==='CLO'?'lc-amber':'lc-red';
  const wsState=String(d.ws_state||d.conn_status||'UNKNOWN');
  const wsCls=wsState.startsWith('OK')||wsState==='WS_OK'||wsState.startsWith('LIVE')?'lc-green':wsState==='STALE'?'lc-amber':'lc-red';
  const real=d.realized_pnl, unreal=d.unrealized_pnl, net=d.net_pnl;
  const hasFills=Number(d.filled_count||0)>0||!!(d.last_fill&&d.last_fill.coin);
  const hasOpen=Number(d.open_position_count||0)>0;
  const pnlReason=pnlReasonText(d.pnl_display_reason||d.pnl_not_proven_reason||d.pnl_status_label||'');
  const emptyPnlReason=lcAudit.load_error?'stats pending':(pnlReason||(hasFills?'account-level only':'no copy fills yet'));
  const realizedEmpty=pnlReason||(hasFills?'closed PnL not attributed':'no closed PnL yet');
  const unrealEmpty=hasOpen?(pnlReason||'open PnL not wallet-attributed'):'no open sleeve';
  const pnlHtml=`<div class="lc-cell-stack" style="font-size:11px">
   <span class="${signCls(real)}">R: ${moneyFmt(real)||`<span class="lc-muted" title="${h(realizedEmpty)}">${tiny(realizedEmpty,28)}</span>`}</span>
   <span class="${signCls(unreal)}">U: ${moneyFmt(unreal)||`<span class="lc-muted" title="${h(unrealEmpty)}">${tiny(unrealEmpty,28)}</span>`}</span>
   <span class="${signCls(net)}" style="font-weight:780">Net: ${moneyFmt(net)||`<span class="lc-muted">${emptyPnlReason}</span>`}</span>
  </div>`;
  const avgBps=d.avg_diff_bps;
  const bpsCls=avgBps!=null&&avgBps>10?'lc-neg':avgBps!=null&&avgBps<=0?'lc-pos':'';
  const bpsStr=avgBps!=null?(`<span class="${bpsCls}">${h(avgBps)} bps</span>`):`<span class="lc-muted">${hasFills?'bps pending':'no fills yet'}</span>`;
  const actHtml=`<div class="lc-cell-stack" style="font-size:11px">
   <span class="lc-pos">${d.filled_count==null?'stats pending':h(d.filled_count)} fills / ${d.exits_count==null?'stats pending':h(d.exits_count)} exits</span>
   <span>${d.open_position_count==null?'stats pending':h(d.open_position_count)} open${(d.recent_reject_count||0)>0?` · <span class="lc-neg">${d.recent_reject_count} rej</span>`:''}${(d.recent_block_count||0)>0?` · <span class="lc-amber">${d.recent_block_count} blk</span>`:''}</span>
  </div>`;
  const expTitle=d.exposure_source||'manual ledger exposure';
  const expStr=d.current_exposure==null?(hasOpen?'ledger source unavailable':'$0'):(moneyFmt(d.current_exposure||0)||'$0');
  const lf=d.last_fill||{};
  const lfStr=lf.coin
   ?`<div style="font-size:11px"><b>${h(lf.coin)}</b> ${h(lf.side||'')} ${h(lf.size||'')} @ ${h(lf.avg_px||'')}<br><span class="lc-muted" style="font-size:10px">${tiny(lf.time||'',20)}</span></div>`
   :'<span class="lc-muted" style="font-size:11px">—</span>';
  const wsDetail=mode==='OFF'?'':`<span class="lc-muted" style="font-size:10px">${h(d.ws_reason||d.conn_detail||'')||''}</span>`;
  const coreTs=String(d.core_truth_status||'');
  const coreBadge=coreTs==='CORE_CONFIRMED_LIVE'?`<span class="lc-pill lc-green" style="font-size:9px" title="Core confirmed: poll+WS active">Core✓</span>`:coreTs==='CORE_POLL_ONLY_WS_MISSING'?`<span class="lc-pill lc-amber" style="font-size:9px" title="Core polling OK but WS subscription missing — restart recommended">Poll✓ WS?</span>`:coreTs==='CONFIG_LIVE_NOT_IN_CORE'?`<span class="lc-pill lc-red" style="font-size:9px" title="Config says LIVE but Core is not tracking — Core reload required">NOT IN CORE</span>`:coreTs==='ARCHIVED_STILL_IN_CORE'?`<span class="lc-pill lc-amber" style="font-size:9px" title="Archived in config but Core still tracking — restart recommended">ARCHIVED/TRACKED</span>`:coreTs&&coreTs!=='CONFIG_OFF'&&coreTs!=='CORE_CONFIRMATION_UNAVAILABLE'?`<span class="lc-pill lc-amber" style="font-size:9px" title="${h(coreTs)}">${h(coreTs)}</span>`:'';
  return `<tr class="lc-wallet-row lc-row-${h(mode)}" data-wallet="${h(d.wallet)}">
   <td style="color:#8fa3b7;font-size:11px">${i+1}</td>
   <td style="cursor:pointer" title="Click to expand detail">
    <div class="lc-cell-stack"><span class="lc-wallet" style="font-size:11px">${h(shortWallet(d.wallet))}</span></div>
   </td>
   <td><div class="lc-cell-stack">
    <span class="lc-pill ${modeCls}">${h(mode)}</span>
    ${coreBadge}
    <span class="lc-pill ${wsCls}" style="font-size:10px">${h(wsState)}</span>
    ${wsDetail}
   </div></td>
   <td>${pnlHtml}</td>
   <td>${bpsStr}</td>
   <td>${actHtml}</td>
<td style="font-size:11px" title="${h(expTitle)}">${expStr}<br><span class="lc-muted" style="font-size:10px">${d.open_position_count==null?'stats pending':h(d.open_position_count)+' pos'}</span></td>
    <td style="font-size:11px;color:${d.true_ts_max_dd_pct!=null?(d.true_ts_max_dd_pct>20?'#ff5263':d.true_ts_max_dd_pct>10?'#f5b84b':'#21c16b'):'#8fa3b7'}">${d.true_ts_max_dd_pct!=null?h(d.true_ts_max_dd_pct.toFixed(1)+'%'):'<span class="lc-muted">—</span>'}</td>
    <td>${lfStr}</td>
   <td><div class="lc-cell-stack">
    <div class="lc-inline-controls">
     <select name="mode"><option ${mode==='LIVE'?'selected':''}>LIVE</option><option ${mode==='CLO'?'selected':''}>CLO</option><option ${mode==='OFF'?'selected':''}>OFF</option></select>
     <select name="copy_mode"><option value="proportional" ${model!=='fixed'?'selected':''}>prop</option><option value="fixed" ${model==='fixed'?'selected':''}>fixed</option></select>
    </div>
    <div class="lc-inline-controls" style="font-size:10px">
     <span class="lc-muted">F</span><input name="fixed_notional" value="${h(d.fixed_notional??10)}" style="width:52px">
     <span class="lc-muted">N</span><input name="norm_base" value="${h(d.norm_base??100)}" style="width:46px">
     <span class="lc-muted">B</span><input name="leader_equity_base" value="${h(d.leader_equity_base??10000)}" style="width:64px">
     <input name="max_diff_pct" type="hidden" value="${h(d.max_diff_pct??0.1)}">
     <input name="daily_loss_limit" type="hidden" value="${h(d.daily_loss_limit??0)}">
    </div>
    <div class="lc-mini-actions">
     <button data-act="save" type="button">Save</button>
     <button data-act="clo" type="button">CLO</button>
     <button data-act="off" type="button">OFF</button>
     <button data-act="archive" class="lc-danger" type="button">Archive</button>
    </div>
   </div></td>
  </tr>`;
 }).join('');
  root.querySelector('#lcWalletRows').innerHTML=rows||(Object.keys(lcConfig.wallets||{}).length>0?'<tr><td colspan="10">Wallet config present but rows could not render — check console.</td></tr>':'<tr><td colspan="10">No wallets configured.</td></tr>');
}
function walletDetailHtml(wallet){
 const w=String(wallet).toLowerCase();
 const perf=(lcAudit.live_leader_performance||{})[w]||{};
 const attemptSource=(lcAudit.recent_metric_send_attempts&&lcAudit.recent_metric_send_attempts.length)?lcAudit.recent_metric_send_attempts:(lcAudit.recent_send_attempts||[]);
 const allAttempts=attemptSource.filter(a=>(a.leader_wallet||a.auto_send_wallet||'').toLowerCase()===w);
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
 const perfNpReason=perf.pnl_not_proven_reason||perf.attribution_quality||'exact wallet PnL source unavailable';
 out+=`<span>DD: <b>${perf.drawdown!=null?moneyFmt(perf.drawdown):notProven(perfNpReason)}</b></span>`;
 // MaxDD (realised) — peak-to-trough on cumsum(closedPnl). Understates real
 // account drawdown because unrealised losses on held positions are invisible.
 out+=`<span title="Peak-to-trough on cumsum(closedPnl). REALISED ONLY — does NOT include unrealised drawdown on open positions. Use the MTM column for risk decisions.">MaxDD (realised): <b>${perf.max_drawdown!=null?moneyFmt(perf.max_drawdown):notProven(perfNpReason)}</b></span>`;
 // MaxDD (MTM truth) — pulled from HL accountValueHistory, includes unrealised,
 // funding, fees. THIS is what your live account drawdown will look like.
 const mtmDdCls = (perf.max_drawdown_mtm!=null && perf.max_drawdown_mtm < 0) ? 'neg' : '';
 const mtmSrc = perf.mtm_source || 'unavailable';
 const mtmSrcLabel = mtmSrc==='hl_portfolio_api' ? '✓' : (mtmSrc==='cache_stale' ? '⌛stale' : '⚠'+mtmSrc);
 const mtmAllTimeDdCls = (perf.allTime_max_drawdown_mtm!=null && perf.allTime_max_drawdown_mtm < 0) ? 'neg' : '';
 out+=`<span title="HL accountValueHistory (mark-to-market truth, allTime window). Source: ${h(mtmSrc)}. Preferred effective MaxDD source when available.">MaxDD (MTM allTime) ${h(mtmSrcLabel)}: <b class="${mtmAllTimeDdCls}">${perf.allTime_max_drawdown_mtm!=null?moneyFmt(perf.allTime_max_drawdown_mtm):'n/a'}</b></span>`;
 out+=`<span title="HL accountValueHistory (mark-to-market truth, 30d window). Source: ${h(mtmSrc)}. Includes unrealised losses on open positions, funding, fees. Authoritative for risk-adjusted ranking.">MaxDD (MTM 30d) ${h(mtmSrcLabel)}: <b class="${mtmDdCls}">${perf.max_drawdown_mtm!=null?moneyFmt(perf.max_drawdown_mtm):'n/a'}</b></span>`;
 // Calmar from MTM (month_pnl_chg_mtm / abs(max_drawdown_mtm)) -- the headline
 // risk-adjusted return number. Calmar < 1 = downside dominates upside.
 const calCls = (perf.mtm_calmar!=null) ? (perf.mtm_calmar >= 2 ? 'pos' : (perf.mtm_calmar < 1 ? 'neg' : '')) : '';
 out+=`<span title="30d MTM PnL change ÷ |30d MTM peak-to-trough|. ≥2 healthy; <1 means drawdown dominates gains.">MTM Calmar (30d): <b class="${calCls}">${perf.mtm_calmar!=null?Number(perf.mtm_calmar).toFixed(2):'n/a'}</b></span>`;
 // MTM month PnL change as direct context for the Calmar number.
 out+=`<span title="30d account-value change from HL accountValueHistory. Net of fees/funding.">MTM 30d ΔAcct: <b class="${signCls(perf.month_pnl_chg_mtm)}">${perf.month_pnl_chg_mtm!=null?moneyFmt(perf.month_pnl_chg_mtm):'n/a'}</b></span>`;
 out+=`<span>PnL status: ${pnlLabel(perf.pnl_status||'N/A',perf.pnl_status_label)}</span>`;
 out+=`<span>Attribution: <b>${h(perf.attribution_quality||'N/A')}</b></span>`;
 out+=`<span>Confirmed realized match: <b class="${signCls(perf.confirmed_realized_pnl)}">${moneyFmt(perf.confirmed_realized_pnl)||'n/a'}</b> (${h(perf.realized_match_status||'N/A')})</span>`;
 out+=`<span>Cumulative rejects: <b>${h(perf.exchange_rejected_count||0)}</b>; cumulative local blocks: <b>${h(perf.local_blocked_count||0)}</b>; queued previews / would-send records: <b>${h(perf.preview_count||0)}</b></span>`;
 if(perf.data_quality_notes) out+=`<span class="lc-muted" style="font-size:10px">${h(perf.data_quality_notes)}</span>`;
 out+='</div></div>';

 // MTM month PnL change as direct context for the Calmar number.
  out+=`<span title="30d account-value change from HL accountValueHistory. Net of fees/funding.">MTM 30d ΔAcct: <b class="${signCls(perf.month_pnl_chg_mtm)}">${perf.month_pnl_chg_mtm!=null?moneyFmt(perf.month_pnl_chg_mtm):'n/a'}</b></span>`;
  out+=`<span>PnL status: ${pnlLabel(perf.pnl_status||'N/A',perf.pnl_status_label)}</span>`;
  out+=`<span>Attribution: <b>${h(perf.attribution_quality||'N/A')}</b></span>`;
  out+=`<span>Confirmed realized match: <b class="${signCls(perf.confirmed_realized_pnl)}">${moneyFmt(perf.confirmed_realized_pnl)||'n/a'}</b> (${h(perf.realized_match_status||'N/A')})</span>`;
  out+=`<span>Cumulative rejects: <b>${h(perf.exchange_rejected_count||0)}</b>; cumulative local blocks: <b>${h(perf.local_blocked_count||0)}</b>; queued previews / would-send records: <b>${h(perf.preview_count||0)}</b></span>`;
  if(perf.data_quality_notes) out+=`<span class="lc-muted" style="font-size:10px">${h(perf.data_quality_notes)}</span>`;
  out+='</div></div>';

  // F) True Time-Series Equity Curve & Drawdown Overlay (from 8012 equity_curves)
  out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">F — True Time-Series Equity Curve & Drawdown</b>`;
  out+=`<div style="margin-top:4px"><canvas id="lcEquityCurveCanvas" width="900" height="200" style="width:100%;height:auto;background:#081018;border:1px solid #223342;border-radius:6px;display:block"></canvas></div>`;
  out+=`<div style="margin-top:4px;font-size:11px;color:#8fa3b7"><span style="display:inline-flex;align-items:center;gap:4px"><span style="width:12px;height:12px;background:#21c16b;border-radius:2px"></span>Equity</span><span style="margin-left:16px;display:inline-flex;align-items:center;gap:4px"><span style="width:12px;height:12px;background:#ff5263;border-radius:2px"></span>Drawdown %</span></div>`;
  out+='</div>';

  // A2) Exact Exchange PnL Feed
 const pnlFills=perf.matched_pnl_fills||[];
 out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">A2 — Exact Exchange PnL Feed (${pnlFills.length})</b>`;
 if(pnlFills.length){
  out+='<table style="margin-top:4px;min-width:auto"><thead><tr><th>Time</th><th>Coin</th><th>Dir</th><th>Size @ Px</th><th>Closed PnL</th><th>Fee</th><th>OID</th><th>Match</th></tr></thead><tbody>';
  out+=pnlFills.slice(0,30).map(f=>`<tr><td>${tiny(f.time||'—',22)}</td><td><b>${h(f.coin||'—')}</b></td><td>${h(f.dir||f.side||'—')}</td><td>${h(f.size||'—')} @ ${h(f.price||'—')}</td><td class="${signCls(f.closed_pnl)}">${moneyFmt(f.closed_pnl)||'$0'}</td><td>${moneyFmt(f.fee)||'$0'}</td><td class="lc-wallet">${f.oid?tiny(String(f.oid),18):'—'}</td><td><span class="lc-pill lc-green">${h(f.match||'EXACT')}</span></td></tr>`).join('');
  out+='</tbody></table>';
 } else { out+=' <span class="lc-muted">No exact exchange closed-PnL matches in the current cached exchange fill window.</span>'; }
 out+='</div>';

 // B) Open Positions
 out+=`<div style="padding:6px 8px;border-bottom:1px solid #223342"><b style="color:#58a6ff">B — Open Positions (${openPos.length})</b>`;
 if(openPos.length){
  out+='<table style="margin-top:4px;min-width:auto"><thead><tr><th>Coin</th><th>Side</th><th>Size</th><th>Entry px</th><th>Mark px</th><th>Unrealized PnL</th><th>Exposure</th><th>Exchange match</th></tr></thead><tbody>';
  out+=openPos.map(p=>{const stCls=p.exchange_match==='MATCH'?'lc-green':'lc-red';return `<tr><td><b>${h(p.coin)}</b></td><td>${pill(p.side||'—',p.side||'')}</td><td class="${p.signed_size>0?'lc-pos':'lc-neg'}">${h(p.signed_size)}</td><td>${p.entry_px!=null?h(p.entry_px):'n/a'}</td><td>${p.mark_px!=null?h(p.mark_px):'n/a'}</td><td class="${signCls(p.unrealized_pnl)}">${moneyFmt(p.unrealized_pnl)||'n/a'}</td><td>${moneyFmt(p.exposure)||'n/a'}</td><td><span class="lc-pill ${stCls}">${h(p.exchange_match||'?')}</span></td></tr>`;}).join('');
  out+='</tbody></table>';
 } else if(perf.current_open_positions_stale){ out+=' <span class="lc-pill lc-amber" title="Cache is stale/rebuilding; per-wallet open positions are not live-confirmed. See the OWNED COPY POSITIONS table (rebuilt from ledger) for current ownership truth.">REBUILDING — open positions not live-confirmed</span>'; } else { out+=' <span class="lc-muted">no open positions</span>'; }
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
 const owned=lcAudit.owned_copy_positions||[];
 const orphan=lcAudit.orphan_exchange_positions||[];
 const pic=lcAudit.position_integrity_card||{};
 const ownedBox=root.querySelector('#lcOwnedPositionRows');
 const posStale=!!(lcAudit.stale||lcAudit.degraded||lcAudit.position_rows_stale);
 const posAgeSec=Number(lcAudit.stale_age_secs||0);
 const posBanner=posStale?`<tr><td colspan="9" style="background:#2a1f0a;color:#f5b84b;font-weight:780;font-size:11px;padding:6px 8px">&#9888; STALE / REBUILDING — position rows ${lcAudit.position_rows_rebuilt_from_ledger?'rebuilt from current ledger':'NOT live-confirmed'}; verify against ledger${posAgeSec?` (cache age ${Math.round(posAgeSec)}s)`:''}${lcAudit.last_good_persisted_at?` · last good ${tiny(lcAudit.last_good_persisted_at,19)}`:''}</td></tr>`:'';
 const orphanBox=root.querySelector('#lcOrphanPositionRows');
 const intCard=root.querySelector('#lcPositionIntegrityCard');
 const orphanTitle=root.querySelector('#lcOrphanSectionTitle');

 // --- Position Integrity card ---
 if(intCard){
 const auditDegraded=!!lcAudit.load_error||!!lcAudit.error||lcAudit.degraded===true;
  const coreStatus=String(pic.core_status||(auditDegraded?'DEGRADED':'UNKNOWN')).toUpperCase();
  const mismatch=Number(pic.exchange_manual_mismatch_count||0);
  const activeRed=Number(pic.hard_copy_active_red||0);
  const coreHotpathClear=activeRed===0&&Number(pic.hard_copy_unclassified||0)===0;
  const missedTerminals=Number(pic.missed_entry||0)+Number(pic.missed_add||0);
  const onlyHistoricalAmber=coreStatus==='AMBER'&&coreHotpathClear&&mismatch===0&&missedTerminals>0;
  const coreDisplay=onlyHistoricalAmber
   ?'LIVE GREEN / HISTORICAL TERMINALS'
   :(coreStatus==='GREEN'&&String(pic.position_assignment||'').toUpperCase()==='ARCHIVED_RESIDUALS_ACCOUNTED')
    ?'GREEN / ARCHIVE RESIDUAL ACCOUNTED'
    :((coreStatus==='RED'&&coreHotpathClear&&mismatch>0)?'HOTPATH SAFE / POSITION RECONCILE':coreStatus);
  const coreOk=coreDisplay==='GREEN'||coreDisplay.indexOf('GREEN')===0||coreDisplay.indexOf('HOTPATH SAFE')>=0;
  const coreCs=coreOk?'lc-green':coreStatus==='AMBER'||coreStatus==='DEGRADED'?'lc-amber':'lc-red';
  const assign=String(pic.position_assignment||(auditDegraded?'DEGRADED':'UNKNOWN')).toUpperCase();
  const assignDisplay=assign==='ARCHIVED_RESIDUALS_ACCOUNTED'?'ACCOUNTED':((assign==='MISMATCH'&&coreHotpathClear)?'RECONCILE':assign);
  const assignOk=assignDisplay==='MATCHED'||assignDisplay==='ACCOUNTED';
  const assignCs=assignOk?'lc-green':assign==='DEGRADED'?'lc-amber':'lc-amber';
  const realOrphans=Number(pic.real_orphan_count||0);
  const orphanCs=realOrphans===0?'lc-green':'lc-red';
  const xyzCount=Number(pic.xyz_exotic_matched_count||0);
  const btcRes=pic.btc_residual_accounted?'YES':'NO';
  const ts=pic.integrity_timestamp?`<span class="lc-muted" style="font-size:10px">Core snapshot: ${h(pic.integrity_timestamp.slice(0,19).replace('T',' '))} UTC</span>`:'';
  intCard.innerHTML=`<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
   <b style="font-size:12px;color:#8fa3b7">Position Integrity</b>
   <span class="lc-pill ${coreCs}">Integrity: ${h(coreDisplay)}</span>
   <span class="lc-pill ${assignCs}">Positions: ${h(assignDisplay)}</span>
   <span class="lc-pill ${orphanCs}">Real orphans: ${realOrphans}</span>
   <span class="lc-pill ${mismatch===0?'lc-green':'lc-amber'}">Exchange/manual reconcile: ${mismatch}</span>
   <span class="lc-pill ${activeRed===0?'lc-green':'lc-red'}">Active RED: ${activeRed}</span>
   <span class="lc-pill ${xyzCount>0?'lc-green':''}">XYZ/exotic matched: ${xyzCount}</span>
   <span class="lc-pill ${pic.btc_residual_accounted?'lc-green':''}">BTC residual accounted: ${h(btcRes)}</span>
   ${ts}
   ${auditDegraded?`<span class="lc-pill lc-amber">position stats degraded: ${tiny(lcAudit.load_error||lcAudit.error,48)}</span>`:''}
  </div>`;
 }

 // --- Owned positions ---
 // ledger_vs_exchange colour key:
 //   green  → fully matched (OWNED_FULLY_SUPPORTED, SHARED_SYMBOL_SLEEVE_TRACKED, MATCH, SHARED_SYMBOL_NET_MATCH)
 //   amber  → informational / exchange not available
 //   red    → genuine reconciliation issue (EXTERNAL_FLAT_PENDING_RECONCILIATION, SIGN_CONFLICT, UNSUPPORTED)
 const ownedRowsHtml=owned.map(r=>{
  const szCls=r.signed_size!=null?(Number(r.signed_size)>0?'lc-pos':'lc-neg'):'';
  const stRaw=r.ledger_vs_exchange||'OWNED_COPY';
  const stGreen=(stRaw==='MATCH'||stRaw==='OWNED_FULLY_SUPPORTED'
   ||String(stRaw).startsWith('SHARED_SYMBOL_NET_MATCH')
   ||String(stRaw).startsWith('SHARED_SYMBOL_SLEEVE_TRACKED'));
  const stAmber=(stRaw==='EXCHANGE_UNAVAILABLE'||stRaw==='RESIDUAL_ACCOUNT_LEVEL'
   ||stRaw==='OWNED_PLUS_ACCOUNT_RESIDUAL');
  const stCls=stGreen?'lc-green':stAmber?'lc-amber':'lc-red';
  // EXTERNAL_ORPHAN_UNTRACKED wallet — show as ledgered residual, not a scary unknown
  const walletKey=String(r.leader_wallet||'').toLowerCase();
  const isLedgedResidue=walletKey==='external_orphan_untracked';
  const rawArchived=lcConfig.archived_wallets||[];
  const archivedList=Array.isArray(rawArchived)?rawArchived:Object.keys(rawArchived);
  const archivedSet=new Set(archivedList.map(w=>String(w).toLowerCase()));
  const isArchivedLedger=archivedSet.has(walletKey);
  const walletLabel=isLedgedResidue
   ?`<span class="lc-pill lc-blue" title="Tracked residual; accounted in aggregate">LEDGERED_RESIDUAL</span>`
   :`<span class="lc-wallet">${h(r.leader_wallet?shortWallet(r.leader_wallet):'—')}</span>`;
  const ownedPill=isLedgedResidue
   ?`<span class="lc-pill lc-blue">RESIDUAL_ACCOUNTED</span>`
   :isArchivedLedger
   ?`<span class="lc-pill lc-amber" title="Archived wallet ledger entry preserved for reconciliation; not active copy hotpath">ARCHIVED_LEDGER_ENTRY</span>`
   :`<span class="lc-pill lc-green">OWNED_COPY</span>`;
  const entryCell=r.value_is_estimate&&r.position_value!=null
   ?`${r.avg_entry_px!=null?h(r.avg_entry_px):'—'}<div class="lc-muted" style="font-size:10px;margin-top:2px">est val ${moneyFmt(r.position_value)} <span class="lc-pill lc-amber" title="No live exchange mark for this symbol (XYZ synthetic omitted by HL REST); value ESTIMATED from manual avg entry price — not a live mark">ENTRY_ESTIMATE_NO_LIVE_MARK</span></div>`
   :(r.avg_entry_px!=null?h(r.avg_entry_px):'—');
  return `<tr><td>${walletLabel}</td><td><b>${h(r.coin||'—')}</b></td><td>${pill(r.side||'—',r.side||'')}</td><td class="${szCls}">${r.signed_size!=null?h(r.signed_size):'—'}</td><td>${entryCell}</td><td title="${h(r.reconciliation_note||'')}"><div class="lc-cell-stack">${ownedPill}<span class="lc-pill ${stCls}">${h(stRaw)}</span></div></td><td>${r.exchange_signed_size!=null?h(r.exchange_signed_size):'—'}</td><td class="lc-wallet">${tiny(String(r.last_copy_fill_id||r.last_oid||'—'),24)}</td><td>${tiny(r.last_updated_at||'—',22)}</td></tr>`;
 }).join('');
 if(ownedBox) ownedBox.innerHTML=posBanner+(owned.length?ownedRowsHtml:`<tr><td colspan="9">${posStale?'Position rows rebuilding — none from current ledger yet.':'No owned copy sleeves in manual_live_positions.json.'}</td></tr>`);

 // --- Orphan section title: only show scary heading when there are actual real orphans ---
 const realOrphanCount=Number(pic.real_orphan_count||0);
 const coreGreen=String(pic.core_status||'').toUpperCase()==='GREEN';
 const posMatched=String(pic.position_assignment||'').toUpperCase()==='MATCHED';
 if(orphanTitle){
  if(realOrphanCount===0&&coreGreen&&posMatched){
   orphanTitle.textContent='Account-Level Context (no real orphans — Core GREEN / Positions MATCHED)';
   orphanTitle.style.color='#21c16b';
  } else if(realOrphanCount>0){
   orphanTitle.textContent=`ACCOUNT-LEVEL / ORPHAN EXCHANGE — USER-MANAGED (${realOrphanCount} real orphan${realOrphanCount!==1?'s':''})`;
   orphanTitle.style.color='#ff5263';
  } else {
   orphanTitle.textContent='Account-Level Context';
   orphanTitle.style.color='';
  }
 }

 // --- Orphan rows ---
 if(orphanBox) orphanBox.innerHTML=orphan.map(r=>{
  const exCls=r.exchange_signed_size!=null?(Number(r.exchange_signed_size)>0?'lc-pos':'lc-neg'):'';
  const upnlCls=r.unrealized_pnl!=null?(Number(r.unrealized_pnl)>0?'lc-pos':Number(r.unrealized_pnl)<0?'lc-neg':''):'';
  const upnlStr=r.unrealized_pnl!=null?('$'+Number(r.unrealized_pnl).toLocaleString(undefined,{maximumFractionDigits:2})):'—';
  const st=r.ledger_vs_exchange||'ORPHAN_EXCHANGE';
  const cls=r.orphan_classification||r.provenance||'ACCOUNT_LEVEL_ONLY';
  const ctx=r.context||r.reconciliation_note||'account-level context';
  // Classify severity: genuine unknown orphan vs. residual/accounted artefact
  const isResidual=cls==='RESIDUAL_ACCOUNT_LEVEL'||cls==='SHARED_SYMBOL_RESIDUAL';
  const stPillCls=isResidual?'lc-blue':st==='ORPHAN_EXCHANGE'?'lc-red':'lc-amber';
  const ownerCell=isResidual
   ?`<span class="lc-pill lc-blue">RESIDUAL_ACCOUNTED</span>`
   :`<span class="lc-pill lc-blue">USER_MANAGED</span> <span class="lc-pill lc-amber">NOT ENGINE OWNED</span>`;
  const closeCell=isResidual
   ?`<span class="lc-pill lc-blue">residual — no action</span>`
   :`<span class="lc-pill lc-red">engine_can_close=false</span>`;
  const sleeveCell=isResidual
   ?`<span class="lc-pill lc-blue">residual — no action</span>`
   :`<span class="lc-pill lc-red">engine_can_use_as_sleeve=false</span>`;
  return `<tr><td><b>${h(r.coin||'—')}</b></td><td class="${exCls}">${r.exchange_signed_size!=null?h(r.exchange_signed_size):'—'}</td><td>${r.entry_px!=null?h(r.entry_px):'—'}</td><td>${r.mark_px!=null?h(r.mark_px):'—'}</td><td class="${upnlCls}">${upnlStr}</td><td><span class="lc-pill ${stPillCls}">${h(cls)} / ${h(st)}</span></td><td>${ownerCell}</td><td>${closeCell}</td><td>${sleeveCell}</td><td title="${h(r.reconciliation_note||'')}">${h(ctx)}</td></tr>`;
 }).join('')||'<tr><td colspan="10" style="color:#21c16b">No account-level orphan exchange positions.</td></tr>';
}
function renderExecQuality(){
 const rows=lcAudit.execution_quality_rows||[];
 const qs=lcAudit.execution_quality_summary||{};
 const fresh=lcAudit.execution_quality_freshness||{};
 const filled=rows.filter(r=>r.status==='ORDER_FILLED');
 const exchRej=rows.filter(r=>r.status==='ORDER_REJECTED');
 const localBlk=rows.filter(r=>r.status&&r.status!=='ORDER_FILLED'&&r.status!=='ORDER_REJECTED'&&r.status!=='CONFIRM_REQUIRED'&&r.status!=='RECONCILIATION_EVENT');
 const previews=rows.filter(r=>r.status==='CONFIRM_REQUIRED');
 const reconRows=rows.filter(r=>r.status==='RECONCILIATION_EVENT');
 const activeRed=rows.filter(r=>r.is_active&&r.truth_severity==='RED');
 const activeAmber=rows.filter(r=>r.is_active&&r.truth_severity==='AMBER');
 const historical=rows.filter(r=>String(r.truth_state||'').startsWith('HISTORICAL'));
 const adopted=rows.filter(r=>r.truth_state==='ADOPTED_RECONCILED');
 const bpsVals=filled.map(r=>r.fill_bps).filter(v=>v!=null);
 const avgBps=bpsVals.length?Math.round(bpsVals.reduce((a,b)=>a+b,0)/bpsVals.length*10)/10:null;
 const worstBps=bpsVals.length?Math.round(Math.max(...bpsVals)*10)/10:null;
 const lastFill=filled[0]||null;
 const pnlByOid={};
 for(const [wallet,perf] of Object.entries(lcAudit.live_leader_performance||{})){
  for(const f of (perf.matched_pnl_fills||[])){
   const oid=String(f.oid||'');
   if(oid&&!pnlByOid[oid])pnlByOid[oid]={wallet,match:f.match||'EXACT_EXCHANGE_OID',closed_pnl:f.closed_pnl};
  }
 }
 const exchangeFills=((lcAudit.exchange_account_snapshot||{}).actual_user_fills_recent||[]).slice().sort((a,b)=>Number(b.time||0)-Number(a.time||0)).slice(0,80);
 const feedBox=root.querySelector('#lcExchangeTradeFeedRows');
 if(feedBox){
  feedBox.innerHTML=exchangeFills.map(f=>{
   const oid=String(f.oid||'');
   const m=oid?pnlByOid[oid]:null;
   const ts=Number(f.time||0)>0?new Date(Number(f.time)).toISOString():'—';
   const closed=Number(f.closedPnl||f.closed_pnl||0);
   const wallet=m?shortWallet(m.wallet):'account fill';
   const match=m?m.match:'not matched to copied wallet';
   const cls=m?'lc-green':'lc-amber';
   return `<tr><td>${tiny(ts,22)}</td><td class="lc-wallet">${h(wallet)}</td><td><b>${h(f.coin||'—')}</b></td><td>${h(f.dir||f.side||'—')}</td><td>${h(f.sz||'—')} @ ${h(f.px||'—')}</td><td class="${signCls(closed)}">${moneyFmt(closed)||'$0'}</td><td>${moneyFmt(Number(f.fee||0))||'$0'}</td><td class="lc-wallet">${oid?tiny(oid,18):'—'}</td><td><span class="lc-pill ${cls}">${h(match)}</span></td></tr>`;
  }).join('')||'<tr><td colspan="9">No exchange fills in cached snapshot.</td></tr>';
 }
 root.querySelector('#lcExecQualChips').innerHTML=[
  ['Active red',activeRed.length,activeRed.length?'lc-red':'lc-green'],
  ['Active amber',activeAmber.length,activeAmber.length?'lc-amber':'lc-green'],
  ['Adopted / reconciled',adopted.length,'lc-green'],
  ['Historical separated',historical.length,'lc-blue'],
  ['Filled',filled.length,'lc-green'],
  ['Exchange rejected',exchRej.length,exchRej.length?'lc-red':''],
  ['Local blocked',localBlk.length,localBlk.length?'lc-amber':''],
  ['Recent reconciliation',reconRows.length,reconRows.length?'lc-blue':''],
  ['Send attempts fresh',(fresh.send_attempts||{}).age_label||'n/a',''],
  ['Live fills fresh',(fresh.live_fills||{}).age_label||'n/a',''],
  ['Reconciliation fresh',(fresh.reconciliation||{}).age_label||'n/a','lc-blue'],
  ['Queued previews / would-send records',previews.length,''],
  ['Avg fill-vs-limit',avgBps!=null?avgBps+'bps':'n/a',avgBps!=null&&avgBps>5?'lc-amber':''],
  ['Worst fill-vs-limit',worstBps!=null?worstBps+'bps':'n/a',worstBps!=null&&worstBps>10?'lc-red':''],
  ['Last fill',lastFill?(lastFill.coin+' '+lastFill.side):'none',''],
 ].map(([k,v,cls])=>`<span class="lc-pill ${cls}">${h(k)}: <b>${h(v)}</b></span>`).join('');
 root.querySelector('#lcExecQualRows').innerHTML=rows.map(r=>{
  const st=String(r.status||'—');
  const term=String(r.terminal_state||'');
  const stCls=terminalCls(term||st);
  const truth=String(r.truth_state||'');
  const rawTerm=String(r.raw_terminal_state||'');
  const pos=(r.wallet_position_before!=null&&r.wallet_position_before!==''&&r.wallet_position_after!=null&&r.wallet_position_after!=='')?`${h(r.wallet_position_before)} -> ${h(r.wallet_position_after)}`:'—';
  const ms=v=>v!=null&&v!==''?h(v):'—';
  const timeCell=`${tiny(r.time||'—',22)}${r.age_label?`<br><span class="lc-muted" style="font-size:10px">${h(r.source_label||'source')} ${h(r.age_label)}</span>`:''}`;
  return `<tr><td>${timeCell}</td><td class="lc-wallet">${h(r.leader_wallet?shortWallet(r.leader_wallet):'—')}</td><td>${h(r.coin||'—')}</td><td>${h(r.side||'—')}</td><td><span class="lc-pill ${truthCls(truth)}">${h(truth||'—')}</span></td><td><span class="lc-pill ${stCls}">${h(st)}</span></td><td><span class="lc-pill ${terminalCls(term)}" title="${h(rawTerm?('raw: '+rawTerm):'')}">${h(term||'—')}</span></td><td>${h(r.operator_action||'—')}</td><td>${h(r.reject_category||'—')}</td><td class="lc-wallet">${r.oid?tiny(String(r.oid),18):'—'}</td><td>${r.fill_avg_px!=null?h(r.fill_avg_px):'—'}</td><td>${r.fill_size!=null?h(r.fill_size):'—'}</td><td>${pos}</td><td>${ms(r.leader_to_send_attempt_ms)}</td><td>${ms(r.send_total_ms)}</td><td>${ms(r.symbol_resolve_ms)}</td><td>${ms(r.sdk_client_ms)}</td><td>${ms(r.exchange_call_ms)}</td><td title="${h(r.error||'')}">${tiny(r.error||'',28)}</td></tr>`;
 }).join('')||'<tr><td colspan="19">No execution data. Real fills appear here after send_attempts.csv is populated.</td></tr>';
}
function renderAudit(){
 const rows=(lcAudit.last_rows||[]).slice(-10).reverse();
 const reason=lcAudit.reason_counts||{}, decisionCounts=lcAudit.execution_decision_counts||{}, decisionReasonCounts=lcAudit.decision_reason_counts||{}, manualCounts=lcAudit.manual_reconcile_required_counts||{}, errorCounts=lcAudit.market_data_error_counts||{};
 const recentAttempts=lcAudit.recent_send_attempts||[];
 const terminalRows=lcAudit.send_terminal_rows||[];
 const filledRecent=recentAttempts.filter(a=>String(a.status||'').toUpperCase()==='ORDER_FILLED').length;
 const rejectedRecent=recentAttempts.filter(a=>String(a.status||'').toUpperCase()==='ORDER_REJECTED').length;
 const rtCounts=lcCoreRuntime.integrity_counts||{};
 const rtHard=lcCoreRuntime.hard_copy_counts||{};
 const activeMissedTerminal=Number(rtHard.MISSED_ENTRY_TERMINAL||0)+Number(rtHard.MISSED_ADD_TERMINAL||0);
 const missedTerminal=Math.max(
  Number(rtCounts.missed_entry||0)+Number(rtCounts.missed_add||0),
  terminalRows.filter(r=>String(r.terminal_state||r.status||'').toUpperCase().startsWith('MISSED_ENTRY')||String(r.terminal_state||r.status||'').toUpperCase().startsWith('MISSED_ADD')).length
 );
 const ownershipBlocks=terminalRows.filter(r=>String(r.terminal_state||r.status||r.operator_action||'').toUpperCase().includes('OWNERSHIP_GATE')).length;
 root.querySelector('#lcSourceChips').innerHTML=[
  ['Recent intents',rows.length,''],
  ['Recent filled',filledRecent,filledRecent?'lc-green':''],
  ['Recent rejected',rejectedRecent,rejectedRecent?'lc-red':''],
  [activeMissedTerminal>0?'Active missed terminal':'Historical missed terminal',missedTerminal,activeMissedTerminal>0?'lc-red':(missedTerminal?'lc-amber':'lc-green')],
  ['Ownership/local blocks',ownershipBlocks,ownershipBlocks?'lc-amber':'lc-green'],
  ['Manual review',count(decisionCounts,'MANUAL_REVIEW')+count(manualCounts,'True')+count(manualCounts,'true'),'lc-amber'],
  ['Quote unavail',count(errorCounts,'EXECUTABLE_QUOTE_UNAVAILABLE')+count(decisionReasonCounts,'RECOVERY_QUOTE_UNAVAILABLE'),'lc-amber'],
  ['Recent order warnings',lcAudit.recent_send_warning_group_count||0,(lcAudit.recent_send_warning_group_count||0)>0?'lc-amber':''],
 ].map(([k,v,cls])=>`<span class="lc-pill ${cls}">${h(k)}: <b>${h(v)}</b></span>`).join('');
 const sendByIntent={};for(const a of(lcAudit.recent_send_attempts||[])){const iid=String(a.intent_id||'');if(iid)sendByIntent[iid]=a;}
 const terminalByIntent={};for(const src of[lcAudit.reconciliation_rows||[],lcAudit.recent_reconciliation_events||[],lcAudit.send_terminal_rows||[]]){for(const tr of src){const iid=String(tr.intent_id||'');if(iid&&!terminalByIntent[iid])terminalByIntent[iid]=tr;}}
 root.querySelector('#lcAuditRows').innerHTML=rows.map(r=>{const iid=String(r.intent_id||'');const attempt=iid?sendByIntent[iid]:null;const terminal=iid?terminalByIntent[iid]:null;const execDec=String(first(r,['execution_decision','decision'])||'');let realRes='NO_SEND_ATTEMPT';if(attempt){const st=String(attempt.status||'');realRes=st==='ORDER_FILLED'?'REAL_ORDER_FILLED':st==='ORDER_REJECTED'?'EXCHANGE_REJECTED':st||'NO_SEND_ATTEMPT';}else if(terminal){realRes=String(terminal.terminal_state||terminal.status||terminal.event||'TERMINAL_PROOF');}else if(execDec==='WOULD_PLACE_IOC_LIMIT'||execDec==='ENTRY_ALLOWED'||execDec==='EXIT_ALLOWED'){realRes='INTENT_AWAITING_SEND_OR_TERMINAL';}const rawNote=first(r,['notes','message','intent_note']);const displayNote=rawNote&&rawNote.indexOf('dry-run simulated fill')!==-1?'legacy intent note: leader WS fill detected; check real order result':rawNote;const rrCls=realRes==='REAL_ORDER_FILLED'?'lc-green':realRes==='EXCHANGE_REJECTED'?'lc-red':realRes==='WOULD_SEND_ONLY'?'lc-amber':terminal?'lc-amber':'lc-muted';const side=first(r,['copy_side','side','leader_side'])||'—';let decision=first(r,['execution_decision','decision'])||'—';let decisionReason=first(r,['decision_reason','reason'])||'';if(terminal&&!attempt){const term=String(terminal.terminal_state||terminal.status||terminal.event||'TERMINAL_PROOF').toUpperCase();decision=term.includes('PERP_INDEX')?'SYMBOL_BLOCKED':term.includes('STALE_WS')?'STALE_REPLAY_BLOCKED':term.includes('NO_MANUAL')?'NO_SEND_NO_POSITION':'TERMINAL_PROVEN';decisionReason=terminal.operator_action||terminal.action||term;}const sourceReason=first(r,['reason'])||'';const intentStatus=first(r,['status'])||'INTENT_CREATED';return `<tr><td>${h(first(r,['created_at','timestamp_iso','time']))}</td><td class="lc-wallet">${h(shortWallet(first(r,['leader_wallet','wallet'])))}</td><td>${h(first(r,['coin','asset']))}</td><td>${h(side)}</td><td><div class="lc-cell-stack"><span>${h(sourceOf(r))}</span><span class="lc-muted">${tiny(sourceReason,32)}</span></div></td><td>${pill(intentStatus)}</td><td>${decisionPill(decision)}</td><td>${tiny(decisionReason,34)}</td><td>${h(first(r,['suggested_limit_price','target_price']))}</td><td>${h(first(r,['adverse_diff_pct','diff_pct','real_diff_pct','price_diff_pct']))}</td><td>${h(first(r,['manual_reconcile_required']))}</td><td title="${h(displayNote)}">${tiny(displayNote,60)}</td><td><span class="lc-pill ${rrCls}">${h(realRes)}</span></td></tr>`;}).join('')||'<tr><td colspan="13">No audit rows found.</td></tr>';
 const legacyRows=lcAudit.legacy_terminal_rows||[];
 const terminalRow=r=>`<tr><td>${h(r.event||'SEND_TERMINAL')}</td><td><span class="lc-pill ${terminalCls(r.status||r.terminal_state)}">${h(r.status||'—')}</span></td><td>${h(r.action||r.operator_action||'—')}</td><td>${h(r.reject_category||'—')}</td><td><span class="lc-pill ${terminalCls(r.terminal_state)}">${h(r.terminal_state||'—')}</span></td><td>${h(r.coin||'—')}</td><td class="lc-wallet">${h(r.leader_wallet?shortWallet(r.leader_wallet):'—')}</td><td class="lc-wallet">${tiny(String(r.intent_id||'—'),28)}</td><td title="${h(r.notes||r.error||'')}">${tiny(r.notes||r.error||'',80)}</td></tr>`;
 const isSpotSkipRow=r=>String(r.coin||'').match(/^#\\d/)!=null||String(r.terminal_state||r.status||'').toUpperCase()==='SPOT_MARKET_SKIPPED'||String(r.reject_category||'').toUpperCase()==='SPOT_MARKET_SKIPPED';
 const spotSkipRows=terminalRows.filter(isSpotSkipRow);
 const nonSpotTerminalRows=terminalRows.filter(r=>!isSpotSkipRow(r));
 const critical=nonSpotTerminalRows.filter(r=>String(r.terminal_state||r.status||'').startsWith('CLOSE_')||String(r.action||r.operator_action||'').indexOf('RECOVERY')>=0||String(r.action||r.operator_action||'').indexOf('MANUAL_REVIEW')>=0);
 const warnings=nonSpotTerminalRows.filter(r=>!critical.includes(r));
 const cbox=root.querySelector('#lcReconCriticalRows'), wbox=root.querySelector('#lcReconWarningRows'), lbox=root.querySelector('#lcReconLegacyRows');
 if(cbox) cbox.innerHTML=critical.map(terminalRow).join('')||'<tr><td colspan="9">No actionable critical terminal items.</td></tr>';
 if(wbox) wbox.innerHTML=warnings.map(terminalRow).join('')||'<tr><td colspan="9">No terminal warnings.</td></tr>';
 if(lbox) lbox.innerHTML=legacyRows.map(r=>`<tr><td>SEND_REJECTED</td><td><span class="lc-pill lc-red">LEGACY_MISSING_TERMINAL_FIELDS</span></td><td>REVIEW_REQUIRED</td><td>—</td><td>LEGACY_MISSING_TERMINAL_FIELDS</td><td>${h(r.coin||'—')}</td><td class="lc-wallet">${h(r.leader_wallet?shortWallet(r.leader_wallet):'—')}</td><td class="lc-wallet">${tiny(String(r.intent_id||'—'),28)}</td><td title="${h(r.error||'')}">${tiny(r.error||'',80)}</td></tr>`).join('')||'<tr><td colspan="9">No legacy terminal-state gaps.</td></tr>';
 const sbox=root.querySelector('#lcSpotSkipRows'),sdet=root.querySelector('#lcSpotSkipSection');
 if(sdet) sdet.style.display=spotSkipRows.length?'':'none';
 if(sbox||sdet){
  const grouped={};for(const r of spotSkipRows){const k=String(r.coin||'#?');if(!grouped[k])grouped[k]={first:r,count:0};grouped[k].count++;}
  const uniq=Object.keys(grouped).length;
  if(sdet&&spotSkipRows.length){const sm=sdet.querySelector('summary');if(sm)sm.innerHTML=`&#9654; Spot market skips — no action (${uniq} coin${uniq!==1?'s':''}, ${spotSkipRows.length} event${spotSkipRows.length!==1?'s':''})`;}
  if(sbox)sbox.innerHTML=Object.values(grouped).map(({first:r,count})=>{const cb=count>1?` ×${count}`:'';const dn=`Spot market skipped — perp engine only. Leader traded HL spot/index asset (${r.coin||'#?'}). No order sent.`;return `<tr><td>${h(r.event||'SPOT_SKIP')}</td><td><span class="lc-pill lc-blue">SPOT_MARKET_SKIPPED</span></td><td>NO_ACTION</td><td>SPOT_MARKET_SKIPPED</td><td><span class="lc-pill lc-blue">SPOT_MARKET_SKIPPED</span></td><td style="color:var(--lc-blue)"><b>${h(r.coin||'—')}</b>${h(cb)}</td><td class="lc-wallet">${h(r.leader_wallet?shortWallet(r.leader_wallet):'—')}</td><td class="lc-wallet">${tiny(String(r.intent_id||'—'),28)}</td><td title="${h(dn)}">${tiny(dn,80)}</td></tr>`;}).join('')||'<tr><td colspan="9" style="color:var(--lc-muted)">No spot market skips.</td></tr>';
 }
 renderReconTab();
}
function renderHealth(){
 const wsSummary=lcHealth.ws_summary||{};
 const sharedOk=String(wsSummary.ws_status||lcHealth.overall||'').toUpperCase()==='WS_OK'&&wsSummary.socket_open!==false&&wsSummary.thread_alive!==false&&Number(wsSummary.stale_count||0)===0;
 const rows=Object.entries(lcHealth.wallets||{}).sort().map(([wallet,wh])=>{
  const explicit=wh.current_status||wh.effective_status||wh.status||'';
  const current=String(explicit||(sharedOk&&wh.stale===false?'SHARED_WS_OK':'OFFLINE'));
  const effective=String(wh.effective_status||wh.status||current);
  const grade=String(wh.current_health_grade||wh.health_grade||(sharedOk&&wh.stale===false?'OK':'—'));
  const alive=!!wh.thread_alive||!!wh.worker_thread_alive||sharedOk;
  const fatal=Number(wh.fatal_error_count||0);
  const label=(current==='STALE'&&alive&&fatal===0)?'STALE / current idle':current;
  const lastErr=wh.last_error_repr||wh.last_error||wh.last_close_msg||'';
  const recentErr=Number(wh.recent_error_count||0);
  const lifeErr=Number(wh.lifetime_error_count??wh.error_count??0);
  const staleDisplay=(sharedOk&&wh.stale===false)?'idle ok':(wh.transport_stale_ms??wh.stale_ms??'');
  const dataStatus=wh.data_status||(sharedOk&&wh.stale===false?'shared socket active':'—');
  return `<tr><td class="lc-wallet">${h(shortWallet(wallet))}</td><td title="effective: ${h(effective)}">${pill(label,current)}</td><td>${pill(grade,grade)}</td><td>${h(sharedOk?'shared':(alive?'alive':'down'))}</td><td>${Number(wh.reconnects_per_min||0).toFixed(2)}</td><td>${h(wh.processed_count??0)}</td><td>${h(wh.raw_message_count??0)}</td><td>${h(wh.parsed_fill_message_count??0)}</td><td>${h(wh.snapshot_fill_seen_count??0)}</td><td>${h(wh.snapshot_fill_recovered_count??0)}</td><td>${h(wh.ignored_snapshot_count??0)}</td><td>${h(recentErr)}</td><td>${h(lifeErr)}</td><td>${h(staleDisplay)}</td><td>${pill(dataStatus,dataStatus)}</td><td title="${h(lastErr)}">${tiny(lastErr,70)}</td></tr>`;
 }).join('');
 root.querySelector('#lcHealthRows').innerHTML=rows||'<tr><td colspan="16">WS service not running or no wallet health file found.</td></tr>';
}
function render(){
 const wc=root.querySelector('#lcWalletCount');
 const wallets=lcConfig.wallets||{}, walletEntries=Object.values(wallets);
 const tracked=walletEntries.length;
 if(wc) wc.textContent=`${tracked}/10`;
 const mc=root.querySelector('#lcModeCounts');
 const liveN=walletEntries.filter(w=>w&&String(w.mode||'').toUpperCase()==='LIVE').length;
 const cloN=walletEntries.filter(w=>w&&String(w.mode||'').toUpperCase()==='CLO').length;
 const offN=walletEntries.filter(w=>w&&String(w.mode||'').toUpperCase()==='OFF').length;
 if(mc) mc.innerHTML=pill('LIVE '+liveN,'LIVE')+' '+pill('CLO '+cloN,'CLO')+' '+pill('OFF '+offN,'OFF');
 const as=root.querySelector('#lcAutoSend');
 const autoLiveCount=lcAudit.auto_live_wallet_count;
 const runtimeOrderKnown=typeof lcCoreRuntime.effective_real_orders_enabled==='boolean'||typeof lcCoreRuntime.master_real_orders_enabled==='boolean';
 const autoSendOn=runtimeOrderKnown?(lcCoreRuntime.effective_real_orders_enabled===true||lcCoreRuntime.master_real_orders_enabled===true):(autoLiveCount!=null?autoLiveCount>0:lcConfig.auto_send_enabled===true);
 if(as) as.innerHTML='Real order sending: '+(autoSendOn?pill('ON','LIVE'):pill('OFF','OFF'))+(runtimeOrderKnown?' (Core runtime)':(autoLiveCount==null?' (from config)':''));
 const wsOverall=String(lcHealth.overall||'OFFLINE').toUpperCase();
 const wo=root.querySelector('#lcWsOverall');
 if(wo) wo.textContent=(['CLOSED','DEGRADED','DISABLED','OFFLINE'].includes(wsOverall))?'OFFLINE':wsOverall;
 const ro=root.querySelector('#lcRealOrders');
 if(ro){const runtimeOrderKnown=typeof lcCoreRuntime.effective_real_orders_enabled==='boolean'||typeof lcCoreRuntime.master_real_orders_enabled==='boolean';const ordersOn=runtimeOrderKnown?(lcCoreRuntime.effective_real_orders_enabled===true||lcCoreRuntime.master_real_orders_enabled===true):((lcAudit.execution_quality_rows||[]).some(r=>r.status==='ORDER_FILLED'));ro.className='lc-pill '+(ordersOn?'lc-green':'lc-red');ro.textContent=ordersOn?'REAL ORDERS: ACTIVE':'REAL ORDERS: DISABLED';}
 renderTopStatus(); renderCards(); renderWallets();
renderAudit(); renderHealth(); renderPositions(); renderExecQuality();
}
function reconIssueLabel(r){const issue=String(r.issue||'');const map={'OWNED_FULLY_SUPPORTED':'Matched','SHARED_SYMBOL_SLEEVE_TRACKED':'Shared symbol sleeve tracked','SHARED_SYMBOL_NET_MATCH':'Aggregate net matched','FLAT':'Flat — no position','SIGN_CONFLICT_PENDING_RECONCILIATION':'Sign conflict — assignment needed','EXTERNAL_FLAT_PENDING_RECONCILIATION':'Manual close needs adoption','OWNED_LEDGER_UNSUPPORTED_BY_EXCHANGE':'Ledger partially unsupported by exchange','MISSING_LEDGER':'Exchange position — no ledger entry','MISSING_EXCHANGE':'Ledger net nonzero — exchange flat','OWNED_PLUS_ACCOUNT_RESIDUAL':'Residual beyond owned sleeve','EXCHANGE_UNAVAILABLE':'Exchange snapshot unavailable'};if(issue.startsWith('ARCHIVED_LEDGER_RESIDUAL_ACCOUNTED'))return 'Archived ledger residual accounted';if(issue.startsWith('SHARED_SYMBOL_NET_DIFF'))return 'Shared symbol net differs from exchange';if(issue.startsWith('WALLET_CLO_')||issue.startsWith('WALLET_OFF_'))return 'Wallet idle — no open exposure';return map[issue]||issue;}
function reconSection(r){const act=r.action_type||'NONE';if(act==='MANUAL_VERIFY_XYZ_SNAPSHOT_BLIND')return 'C';if(act==='ASSIGN_ORPHAN_TO_WALLET'||act==='ADOPT_MANUAL_CLOSE'||act==='CLOSE_UNOWNED_EXPOSURE')return 'A';if(String(r.severity||'')==='CRITICAL'&&act!=='NO_ACTION_TERMINAL'&&act!=='MARK_RESIDUAL_ACCOUNTED'&&act!=='NONE')return 'A';if(act==='ARCHIVE_READY'||act==='REFRESH_RECHECK'||act==='MARK_RESIDUAL_ACCOUNTED')return 'B';const issue=String(r.issue||'');if(issue==='OWNED_PLUS_ACCOUNT_RESIDUAL'||issue.startsWith('SHARED_SYMBOL'))return 'C';return 'D';}
function reconActCell(r){const act=r.action_type||'NONE';const actBlocked=r.action_blocked_reason||'';const actSummary=r.operator_summary||'';if(!act||act==='NONE'||act==='NO_ACTION_TERMINAL')return '';if(actBlocked)return `<span class="lc-pill lc-muted" title="BLOCKED: ${h(actBlocked)}">${h(r.action_label||act)} &#9888;</span>`;if(act==='REFRESH_RECHECK')return `<button class="lc-btn-sm" data-recon-act="refresh" title="${h(actSummary)}">Refresh</button>`;if(act==='ARCHIVE_READY')return `<button class="lc-btn-sm" data-recon-act="archive" data-wallet="${h(r.wallet||'')}" title="${h(actSummary)}">Archive wallet</button>`;if(act==='MARK_RESIDUAL_ACCOUNTED')return `<button class="lc-btn-sm" data-recon-act="create-request" data-action-type="${h(act)}" data-wallet="${h(r.wallet||'')}" data-coin="${h(r.coin||'—')}" data-ledger-size="${h(String(r.manual_signed_size??0))}" data-exchange-size="${h(String(r.exchange_signed_size??0))}" data-summary="${h(actSummary)}">Mark residual</button>`;return `<button class="lc-btn-sm lc-btn-warn" data-recon-act="create-request" data-action-type="${h(act)}" data-wallet="${h(r.wallet||'')}" data-coin="${h(r.coin||'—')}" data-ledger-size="${h(String(r.manual_signed_size??0))}" data-exchange-size="${h(String(r.exchange_signed_size??0))}" data-summary="${h(actSummary)}">Create repair request</button>`;}
function reconRow(r,showSev){const label=reconIssueLabel(r);const actCell=reconActCell(r);const sideCls=r.side==='LONG'?'lc-pos':r.side==='SHORT'?'lc-neg':'';const sevCls=r.severity==='CRITICAL'?'lc-red':r.severity==='OK'?'lc-green':'lc-muted';const sevCell=showSev?`<td><span class="lc-pill ${sevCls}" style="min-width:52px;justify-content:center">${h(r.severity||'INFO')}</span></td>`:'';const walletDisplay=r.wallet&&r.wallet!=='aggregate'?shortWallet(r.wallet):(r.wallet||'—');return `<tr>${sevCell}<td><b>${h(r.coin||'—')}</b></td><td title="${h(r.wallet||'')}">${h(walletDisplay)}</td><td title="${h(r.operator_summary||r.issue||'')}">${h(label)}</td><td class="${sideCls}" style="white-space:nowrap">${h(String(r.manual_signed_size??'—'))}</td><td style="white-space:nowrap">${h(String(r.exchange_signed_size??'—'))}</td><td>${actCell||'<span class="lc-muted" style="font-size:10px">—</span>'}</td></tr>`;}
function renderReconTab(){
 const recon=lcAudit.manual_reconciliation_rows||[];
 const secA=[],secB=[],secC=[],secD=[];
 for(const r of recon){const s=reconSection(r);(s==='A'?secA:s==='B'?secB:s==='C'?secC:secD).push(r);}
 const sumEl=root.querySelector('#lcReconSummaryCards');
 if(sumEl)sumEl.innerHTML=[
  [secA.length,'Action required',secA.length>0?'var(--lc-red)':'var(--lc-green)'],
  [secB.length,'Safe actions available',secB.length>0?'var(--lc-amber)':'var(--lc-muted)'],
  [secC.length,'Context / residual','var(--lc-blue)'],
  [secD.length,'Matched — no action','var(--lc-muted)'],
 ].map(([n,label,col])=>`<div style="flex:1;min-width:110px;border:1px solid var(--lc-line);border-radius:8px;padding:10px 14px;background:var(--lc-panel)"><div style="font-size:22px;font-weight:800;color:${col}">${h(String(n))}</div><div style="font-size:10px;color:var(--lc-muted);margin-top:2px">${h(String(label))}</div></div>`).join('');
 const hdrs='<thead><tr><th>Sev</th><th>Coin</th><th>Wallet</th><th>Issue</th><th>Ledger</th><th>Exchange</th><th>Action</th></tr></thead>';
 const noSevHdrs='<thead><tr><th>Coin</th><th>Wallet</th><th>Issue</th><th>Ledger</th><th>Exchange</th><th>Action</th></tr></thead>';
 const aEl=root.querySelector('#lcReconSectionA');
 if(aEl){
  if(!secA.length){
   const msg=(secB.length||secC.length)
    ?`&#10003; No critical repair action. Review ${secB.length} safe action(s) and ${secC.length} context/residual row(s).`
    :'&#10003; No action required — all positions accounted for.';
   aEl.innerHTML=`<div style="color:var(--lc-green);font-size:12px;padding:8px 0">${msg}</div>`;
  }
  else aEl.innerHTML=`<div style="font-size:12px;font-weight:760;color:var(--lc-red);margin-bottom:6px;padding:4px 0">&#9888; Action Required (${secA.length})</div><div class="lc-table-wrap"><table>${hdrs}<tbody>${secA.map(r=>reconRow(r,true)).join('')}</tbody></table></div>`;
 }
 const bEl=root.querySelector('#lcReconSectionB');
 if(bEl){
  if(!secB.length)bEl.innerHTML='';
  else bEl.innerHTML=`<div style="font-size:12px;font-weight:760;color:var(--lc-amber);margin:10px 0 6px;padding:4px 0">Safe Actions Available (${secB.length})</div><div class="lc-table-wrap"><table>${hdrs}<tbody>${secB.map(r=>reconRow(r,true)).join('')}</tbody></table></div>`;
 }
 const cEl=root.querySelector('#lcReconSectionC');
 if(cEl){
  if(!secC.length)cEl.innerHTML='';
  else cEl.innerHTML=`<details style="margin-top:10px"><summary style="cursor:pointer;font-size:11px;color:var(--lc-muted);user-select:none;padding:4px 0">&#9654; Account-Level Context (${secC.length} — residuals &amp; shared symbols)</summary><div class="lc-table-wrap" style="margin-top:6px"><table>${noSevHdrs}<tbody>${secC.map(r=>reconRow(r,false)).join('')}</tbody></table></div></details>`;
 }
 const dEl=root.querySelector('#lcReconSectionD');
 if(dEl){
  if(!secD.length)dEl.innerHTML='';
  else dEl.innerHTML=`<details style="margin-top:6px"><summary style="cursor:pointer;font-size:11px;color:var(--lc-muted);user-select:none;padding:4px 0">&#9654; Matched / No Action (${secD.length} — expand for debug)</summary><div class="lc-table-wrap" style="margin-top:6px"><table>${noSevHdrs}<tbody>${secD.map(r=>reconRow(r,false)).join('')}</tbody></table></div></details>`;
 }
 renderPendingRepairs();
}
let lcRepairReqPending=null;
async function renderPendingRepairs(){const el=root.querySelector('#lcPendingRepairs');if(!el)return;try{const d=await jget('/api/reconciliation/pending-repairs');const p=d.pending||[];if(!p.length){el.innerHTML='<span class="lc-muted">No pending repair requests.</span>';return;}const applyBadge=r=>r.requires_core_apply?'<span class="lc-pill lc-amber">Core apply required</span>':'<span class="lc-pill lc-blue">App-only</span>';el.innerHTML='<table style="width:100%;font-size:11px"><thead><tr><th>Status</th><th>Action</th><th>Coin</th><th>Wallet</th><th>Note</th><th>Created</th><th>Apply path</th><th></th></tr></thead><tbody>'+p.map(r=>`<tr><td><span class="lc-pill lc-amber">${h(r.status||'?')}</span></td><td>${h(r.action_type||'?')}</td><td><b>${h(r.coin||'—')}</b></td><td title="${h(r.wallet||'')}">${r.wallet?shortWallet(r.wallet):'—'}</td><td title="${h(r.audit_note||'')}">${tiny(r.audit_note||'—',36)}</td><td>${tiny(r.created_at||'',19)}</td><td>${applyBadge(r)}</td><td><button class="lc-btn-sm" style="color:var(--lc-red)" data-recon-cancel-req="${h(r.request_id||'')}">Cancel</button></td></tr>`).join('')+'</tbody></table>';}catch(e){el.innerHTML=`<span class="lc-muted">Error: ${h(e.message||String(e))}</span>`;}}
const lcRepairModal=root.querySelector('#lcRepairModal');
function closeRepairModal(){if(lcRepairModal){lcRepairModal.classList.remove('active');lcRepairModal.setAttribute('aria-hidden','true');}lcRepairReqPending=null;}
function openRepairModal(data){lcRepairReqPending=data;if(!lcRepairModal)return;const t=root.querySelector('#lcRepairModalTitle');const b=root.querySelector('#lcRepairModalBody');const nr=root.querySelector('#lcRepairNoteReq');const ta=root.querySelector('#lcRepairAuditNote');if(t)t.textContent='Create '+data.actionType.replace(/_/g,' ');if(b)b.innerHTML=`<table style="border-collapse:collapse;width:100%;font-size:12px"><tr><td style="color:var(--lc-muted);padding:2px 8px 2px 0">Coin</td><td><b>${h(data.coin)}</b></td></tr><tr><td style="color:var(--lc-muted);padding:2px 8px 2px 0">Wallet</td><td>${h(data.wallet?shortWallet(data.wallet):'unknown')}</td></tr><tr><td style="color:var(--lc-muted);padding:2px 8px 2px 0">Ledger</td><td>${h(String(data.ledgerSize))}</td></tr><tr><td style="color:var(--lc-muted);padding:2px 8px 2px 0">Exchange</td><td>${h(String(data.exchangeSize))}</td></tr><tr><td style="color:var(--lc-muted);padding:2px 8px 2px 0">Summary</td><td style="color:var(--lc-amber)">${h(data.summary)}</td></tr></table>`;if(nr)nr.style.display=data.actionType==='ASSIGN_ORPHAN_TO_WALLET'?'inline':'none';if(ta)ta.value='';lcRepairModal.classList.add('active');lcRepairModal.setAttribute('aria-hidden','false');}
[root.querySelector('#lcRepairModalClose'),root.querySelector('#lcRepairModalClose2')].forEach(btn=>{if(btn)btn.addEventListener('click',closeRepairModal);});
root.querySelector('#lcRepairSubmitBtn').addEventListener('click',async()=>{if(!lcRepairReqPending)return;const ta=root.querySelector('#lcRepairAuditNote');const note=(ta&&ta.value||'').trim();if(lcRepairReqPending.actionType==='ASSIGN_ORPHAN_TO_WALLET'&&!note){msg('Audit note is required for ASSIGN_ORPHAN_TO_WALLET',true);return;}try{msg('Creating repair request...');const r=await jpost('/api/reconciliation/create-repair-request',{action_type:lcRepairReqPending.actionType,wallet:lcRepairReqPending.wallet,coin:lcRepairReqPending.coin,old_ledger_size:lcRepairReqPending.ledgerSize,exchange_size:lcRepairReqPending.exchangeSize,audit_note:note,user_visible_reason:lcRepairReqPending.summary});closeRepairModal();await renderPendingRepairs();msg('Request created (id='+r.request_id+') — NOT applied. Core step required.');}catch(e){msg(e.message||String(e),true);}});
async function refresh(quiet){try{if(!quiet)msg('Loading...');const [cfg,health,gcr]=await Promise.all([jget('/api/live-config',8000),jget('/api/live-ws-health',8000),jget('/api/global-controls',8000)]);lcConfig=cfg.config||{wallets:{}};lcHealth=health.health||{};lcCoreRuntime=gcr.core_runtime||{};try{render();loadGcForm(gcr);}catch(re){msg('Render: '+(re.message||String(re)),true);throw re;}if(!quiet)msg('Config loaded; loading stats...');try{const audit=await jget('/api/live-audit-summary',15000);lcAudit=audit||{};render();if(!quiet)msg('Loaded');}catch(ae){lcAudit=Object.assign({},lcAudit,{load_error:ae.message||String(ae)});render();msg('Stats degraded: '+(ae.message||String(ae)),true);}}catch(e){msg(e.message||String(e),true);}}
function loadGcForm(gcResp){
  const gc=gcResp.global_controls||gcResp;
  const ceff=gcResp.core_effective_global_controls||null;
  const driftFields=gcResp.config_drift_fields||[];
  const cycleBudget=!!gcResp.cycle_time_budget_exceeded;
  const f=(id,v)=>{const el=root.querySelector('#'+id);if(el&&v!=null)el.value=v;};
  const st=(id,v)=>{const el=root.querySelector('#'+id);if(el)el.textContent=Number(v||0)<=0?'OFF':'';};
  f('gcMaxTotal',gc.max_total_live_exposure_usd||0);f('gcMaxDir',gc.max_asset_directional_exposure_usd||0);
  f('gcMaxWallet',gc.max_wallet_exposure_usd||0);f('gcMaxOrder',gc.max_order_notional_usd||0);
  const mktPct=gc.marketable_slippage_pct!=null?gc.marketable_slippage_pct:(Number(gc.marketable_bps||0)/100);
  const closePct=gc.max_close_adverse_diff_pct!=null?gc.max_close_adverse_diff_pct:0;
  f('gcMktPct',mktPct);f('gcCloseAdv',closePct);st('gcMktPctState',mktPct);st('gcCloseAdvState',closePct);
  f('gcAllowlist',(gc.symbol_allowlist||[]).join(','));f('gcBlocklist',(gc.symbol_blocklist||[]).join(','));
  const driftBanner=root.querySelector('#lcGcDriftBanner');
  if(driftBanner){
    if(driftFields.length>0){
      const lines=driftFields.map(d=>`${d.field}: saved=${JSON.stringify(d.saved)} core=${JSON.stringify(d.core_effective)}`);
      driftBanner.style.display='';
      driftBanner.textContent='⚠ CONFIG DRIFT — Core has not yet reloaded these fields: '+lines.join('; ')+'. Core auto-reloads within ~10s.';
    } else if(!gcResp.core_effective_available){
      driftBanner.style.display='';
      driftBanner.textContent='Core not running or state unavailable — cannot verify effective settings.';
    } else {
      driftBanner.style.display='none';
    }
  }
  const budgetNote=root.querySelector('#lcGcBudgetNote');
  if(budgetNote){
    if(cycleBudget){
      budgetNote.style.display='';
      budgetNote.textContent='ℹ budget_exceeded in Core state file = CYCLE TIMING diagnostic (reconciliation loop >5s). This is NOT a financial cap. Your financial caps ($6000 total/$2000 wallet/$100 order) are active and have NOT been hit.';
    } else {
      budgetNote.style.display='none';
    }
  }
  const effEl=root.querySelector('#lcGcCoreEffective');
  if(effEl){
    if(ceff){
      const mktPctEff=Number(ceff.marketable_bps||0)/100;
      const rows=[
        ['Max total ($)',ceff.max_total_live_exposure_usd,'max_total_live_exposure_usd'],
        ['Max wallet ($)',ceff.max_wallet_exposure_usd,'max_wallet_exposure_usd'],
        ['Max order ($)',ceff.max_order_notional_usd,'max_order_notional_usd'],
        ['Slippage %',mktPctEff,'marketable_bps'],
        ['Close adv %',ceff.max_close_adverse_diff_pct,'max_close_adverse_diff_pct'],
        ['Allowlist',(ceff.symbol_allowlist||[]).join(',')||'(none)','symbol_allowlist'],
        ['Blocklist',(ceff.symbol_blocklist||[]).join(',')||'(none)','symbol_blocklist'],
      ];
      const driftSet=new Set(driftFields.map(d=>d.field));
      effEl.innerHTML='<div style="font-size:10px;color:var(--lc-muted);margin-bottom:4px;font-weight:760;text-transform:uppercase">Core Effective Settings (runtime confirmed)</div>'+
        rows.map(([label,val,fld])=>{const isDrift=driftSet.has(fld);return `<span style="margin-right:10px;color:${isDrift?'var(--lc-amber)':'var(--lc-muted)'}">${h(label)}: <b style="color:${isDrift?'var(--lc-amber)':'var(--lc-text)'}">${h(String(val??'—'))}</b>${isDrift?' ⚠':''}</span>`;}).join('');
    } else {
      effEl.textContent='Core effective settings: not yet available (Core not running or state file missing).';
    }
  }
}
async function refreshGcForm(){try{const gcr=await jget('/api/global-controls');loadGcForm(gcr);}catch(e){console.error('gc refresh',e);}}
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
    gs.textContent='Saved — Core auto-reloads within ~10s';gs.className='lc-status lc-ok';
    setTimeout(refreshGcForm,12000);}catch(e){gs.textContent=e.message||String(e);gs.className='lc-status lc-bad';}
});
root.querySelectorAll('[data-lc-modal]').forEach(btn=>btn.addEventListener('click',()=>{const m=root.querySelector('#'+btn.dataset.lcModal);if(m){m.classList.add('active');m.setAttribute('aria-hidden','false');}}));
root.querySelectorAll('[data-lc-close]').forEach(btn=>btn.addEventListener('click',()=>{const m=btn.closest('.lc-modal-backdrop');if(m){m.classList.remove('active');m.setAttribute('aria-hidden','true');}}));
root.querySelectorAll('[data-lc-tab]').forEach(btn=>btn.addEventListener('click',()=>{root.querySelectorAll('[data-lc-tab]').forEach(b=>b.classList.remove('active'));root.querySelectorAll('[data-lc-panel]').forEach(p=>p.classList.remove('active'));btn.classList.add('active');root.querySelector(`[data-lc-panel="${btn.dataset.lcTab}"]`).classList.add('active');}));
root.querySelectorAll('[data-lc-graph-mode]').forEach(btn=>btn.addEventListener('click',()=>{lcGraphMode=btn.dataset.lcGraphMode;root.querySelectorAll('[data-lc-graph-mode]').forEach(b=>b.classList.remove('active'));btn.classList.add('active');renderGraph();}));
root.querySelectorAll('[data-lc-graph-scale]').forEach(btn=>btn.addEventListener('click',()=>{lcGraphScale=btn.dataset.lcGraphScale;lcGraphStartMs=0;lcGraphEndMs=0;const s=root.querySelector('#lcGraphStart'),e=root.querySelector('#lcGraphEnd');if(s)s.value='';if(e)e.value='';root.querySelectorAll('[data-lc-graph-scale]').forEach(b=>b.classList.remove('active'));btn.classList.add('active');renderGraph();}));
// Hover tooltip / crosshair for the RAW EXCHANGE chart.
const lcChartWrap=root.querySelector('#lcChartWrap');
const lcEquitySvg=root.querySelector('#lcEquityChart');
const lcHoverCross=root.querySelector('#lcHoverCross');
const lcHoverDot=root.querySelector('#lcHoverDot');
const lcHoverTip=root.querySelector('#lcHoverTip');
function lcHideHover(){if(lcHoverCross) lcHoverCross.style.display='none';if(lcHoverDot) lcHoverDot.style.display='none';if(lcHoverTip) lcHoverTip.style.display='none';}
function lcOnHover(e){
 const s=lcHoverSeries; if(!s||!s.points||!s.points.length||!lcEquitySvg){lcHideHover();return;}
 const rect=lcEquitySvg.getBoundingClientRect();
 const vbW=1000, vbH=330;
 const svgX=(e.clientX-rect.left)/rect.width*vbW;
 if(svgX<s.plot.left||svgX>s.plot.right){lcHideHover();return;}
 // Find nearest point by svg-x.
 let best=s.points[0], bestDx=Math.abs(s.getX(best)-svgX);
 for(const p of s.points){const dx=Math.abs(s.getX(p)-svgX);if(dx<bestDx){best=p;bestDx=dx;}}
 const px=s.getX(best), py=s.getY(best.value);
 // Find drawdown at same timestamp.
 let dd=null;
 if(s.drawdown&&s.drawdown.length){let dbest=s.drawdown[0],dbd=Math.abs(dbest.timestamp_ms-best.timestamp_ms);for(const p of s.drawdown){const d=Math.abs(p.timestamp_ms-best.timestamp_ms);if(d<dbd){dbest=p;dbd=d;}}dd=dbest.value;}
 let real=null;
 if(s.realized&&s.realized.length){let rbest=s.realized[0],rbd=Math.abs(rbest.timestamp_ms-best.timestamp_ms);for(const p of s.realized){const d=Math.abs(p.timestamp_ms-best.timestamp_ms);if(d<rbd){rbest=p;rbd=d;}}real=rbest.value;}
 if(lcHoverCross){lcHoverCross.setAttribute('x1',px);lcHoverCross.setAttribute('x2',px);lcHoverCross.setAttribute('y1',s.plot.top);lcHoverCross.setAttribute('y2',s.plot.bottom);lcHoverCross.style.display='';}
 if(lcHoverDot){lcHoverDot.setAttribute('cx',px);lcHoverDot.setAttribute('cy',py);lcHoverDot.style.display='';}
 if(lcHoverTip){
  const when=new Date(best.timestamp_ms||Date.now()).toLocaleString();
  const v=best.value, sign=v>=0?'+':'-';
  const realStr=real!=null?` &nbsp; <span style="color:#58a6ff">Real ${real>=0?'+':'-'}$${Math.abs(real).toFixed(2)}</span>`:'';
  const ddStr=dd!=null?` &nbsp; <span style="color:#ff7a8a">DD ${dd>=0?'+':'-'}$${Math.abs(dd).toFixed(2)}</span>`:'';
  lcHoverTip.innerHTML=`<div style="color:#8b949e">${when}</div><div><span style="color:#42d97b;font-weight:700">${sign}$${Math.abs(v).toFixed(2)}</span>${realStr}${ddStr}</div><div style="color:#6e7681;font-size:10px">source=${s.sourceTag||''}</div>`;
  // Position in CSS pixels (rect-relative).
  const pxCss=(px/vbW)*rect.width, pyCss=(py/vbH)*rect.height;
  let tipX=pxCss+10, tipY=pyCss+10;
  const tipW=lcHoverTip.offsetWidth||180, tipH=lcHoverTip.offsetHeight||52;
  if(tipX+tipW>rect.width-4) tipX=pxCss-tipW-10;
  if(tipY+tipH>rect.height-4) tipY=pyCss-tipH-10;
  lcHoverTip.style.left=tipX+'px'; lcHoverTip.style.top=tipY+'px'; lcHoverTip.style.display='';
 }
}
if(lcChartWrap){lcChartWrap.addEventListener('mousemove',lcOnHover);lcChartWrap.addEventListener('mouseleave',lcHideHover);}
const applyRange=root.querySelector('#lcGraphApplyRange');
if(applyRange) applyRange.addEventListener('click',()=>{const s=root.querySelector('#lcGraphStart'),e=root.querySelector('#lcGraphEnd');lcGraphStartMs=localInputMs(s&&s.value);lcGraphEndMs=localInputMs(e&&e.value);lcGraphScale='custom';root.querySelectorAll('[data-lc-graph-scale]').forEach(b=>b.classList.remove('active'));renderGraph();});
const resetRange=root.querySelector('#lcGraphResetRange');
if(resetRange) resetRange.addEventListener('click',()=>{lcGraphScale='all';lcGraphStartMs=0;lcGraphEndMs=0;const s=root.querySelector('#lcGraphStart'),e=root.querySelector('#lcGraphEnd');if(s)s.value='';if(e)e.value='';root.querySelectorAll('[data-lc-graph-scale]').forEach(b=>b.classList.toggle('active',b.dataset.lcGraphScale==='all'));renderGraph();});
root.querySelector('#lcAddForm').addEventListener('submit',async e=>{e.preventDefault();const fd=new FormData(e.currentTarget);const payload=Object.fromEntries(fd.entries());try{msg('Updating...');await jpost('/api/live-config/add-wallet',payload);e.currentTarget.reset();const m=e.currentTarget.closest('.lc-modal-backdrop');if(m)m.classList.remove('active');await refresh(true);msg('Updated: '+shortWallet(payload.wallet)+' -> SAVED');}catch(err){msg(err.message,true);}});
root.querySelector('[data-lc-panel="recon"]').addEventListener('click',async e=>{
 const cancelBtn=e.target.closest('button[data-recon-cancel-req]');
 if(cancelBtn){const reqId=cancelBtn.dataset.reconCancelReq||'';if(!reqId)return;if(!window.confirm('Cancel repair request '+reqId.slice(0,8)+'...?'))return;try{msg('Cancelling...');await jpost('/api/reconciliation/cancel-repair-request',{request_id:reqId});await renderPendingRepairs();msg('Request cancelled.');}catch(err){msg(err.message||String(err),true);}return;}
 const btn=e.target.closest('button[data-recon-act]');if(!btn)return;
 const act=btn.dataset.reconAct;
 if(act==='refresh'){await refresh(true);return;}
 if(act==='archive'){const wallet=btn.dataset.wallet||'';if(!wallet){msg('No wallet address on button',true);return;}if(!window.confirm('Archive wallet '+shortWallet(wallet)+'?\\n\\nThis removes it from the active config only. Audit history is preserved. Ledger and exchange positions are NOT changed.'))return;try{msg('Archiving wallet...');await jpost('/api/live-config/remove-wallet',{wallet,archive:true});await refresh(true);msg('Wallet '+shortWallet(wallet)+' archived — audit history preserved. No ledger/exchange changes.');}catch(err){msg(err.message||String(err),true);}return;}
 if(act==='create-request'){openRepairModal({actionType:btn.dataset.actionType||'',wallet:btn.dataset.wallet||'',coin:btn.dataset.coin||'—',ledgerSize:btn.dataset.ledgerSize||'0',exchangeSize:btn.dataset.exchangeSize||'0',summary:btn.dataset.summary||''});}
});
async function saveWalletRow(tr, reason){
 if(!tr||!tr.dataset.wallet) return;
 const wallet=tr.dataset.wallet;
 const short=shortWallet(wallet);
 msg('Updating...');
 await jpost('/api/live-config/set-wallet',rowPayload(tr));
 await refresh(true);
 msg('Updated: '+short+' -> '+(reason||'SAVED'));
}
root.querySelector('#lcWalletRows').addEventListener('change',async e=>{
 const field=e.target.closest('select[name="copy_mode"],input[name="norm_base"],input[name="fixed_notional"],input[name="leader_equity_base"],input[name="max_diff_pct"],input[name="daily_loss_limit"]');
 if(!field) return;
 const tr=field.closest('tr.lc-wallet-row');
 if(!tr) return;
 try{await saveWalletRow(tr,'SAVED');}catch(err){msg(err.message||String(err),true);}
});
root.querySelector('#lcWalletRows').addEventListener('click',async e=>{
 const btn=e.target.closest('button[data-act]');
 if(btn){const tr=btn.closest('tr');const wallet=tr.dataset.wallet;const short=shortWallet(wallet);try{if(btn.dataset.act==='archive'&&!window.confirm('Archive removes from config only. Audit/history preserved.')){msg('Cancelled');return;}if(btn.dataset.act==='save'){await saveWalletRow(tr,'SAVED');}if(btn.dataset.act==='clo'){msg('Updating...');await jpost('/api/live-config/set-mode',{wallet,mode:'CLO'});await refresh(true);msg('Updated: '+short+' -> CLO');}if(btn.dataset.act==='off'){msg('Updating...');await jpost('/api/live-config/set-mode',{wallet,mode:'OFF'});await refresh(true);msg('Updated: '+short+' -> OFF');}if(btn.dataset.act==='archive'){msg('Updating...');await jpost('/api/live-config/remove-wallet',{wallet,archive:true});await refresh(true);msg('Updated: '+short+' -> ARCHIVED');}}catch(err){msg(err.message,true);}return;}
const tr=e.target.closest('tr.lc-wallet-row');
  if(!tr||!tr.dataset.wallet) return;
  const wallet=tr.dataset.wallet;
  lcSelectedWallet=wallet;
  lcGraphMode='wallet_pnl';
  root.querySelectorAll('[data-lc-graph-mode]').forEach(b=>b.classList.toggle('active',b.dataset.lcGraphMode==='wallet_pnl'));
  renderGraph();
  const detailId='lcdet-'+wallet.replace(/[^a-z0-9]/gi,'');
  const existing=root.querySelector('#'+detailId);
  if(existing){existing.remove();return;}
  const detRow=document.createElement('tr');
  detRow.id=detailId;
  detRow.innerHTML=`<td colspan="10" style="padding:0">${walletDetailHtml(wallet)}</td>`;
  tr.insertAdjacentElement('afterend',detRow);
  // Fetch and render equity curve with DD overlay
  fetch('/api/wallet-equity-curve/' + encodeURIComponent(wallet))
    .then(r => r.json())
    .then(data => drawEquityCurveWithDD(data))
    .catch(err => console.warn('Equity curve DD overlay failed:', err));
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
<div class="top"><b>Wallet Proof Engine</b><a href="/">Main dashboard</a><span class="muted">Updated: {updated}</span><span class="top-spacer muted">Live copy command centre</span></div>
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
    before = load_ui_state()
    patch = {}
    for k in ("norm_base", "user_norm_base", "copy_mode", "normalisation_mode", "fixed_notional", "leader_equity_base", "fee_bps", "copy_friction_bps", "min_trade_notional_enabled"):
        if k in data:
            patch[k] = data[k]
    if "min_trade_notional_enabled" not in data and ("copy_mode" in data or "fixed_notional" in data or "fee_bps" in data or "copy_friction_bps" in data):
        patch["min_trade_notional_enabled"] = False
    if "ranking_column" in data or "ranking_direction" in data:
        cur = load_ui_state().get("ranking", {})
        patch["ranking"] = {"column": data.get("ranking_column", cur.get("column")), "direction": data.get("ranking_direction", cur.get("direction", "desc"))}
    save_ui_state(patch)
    after = load_ui_state()
    model_fields = {
        "norm_base", "copy_mode", "normalisation_mode", "fixed_notional",
        "leader_equity_base", "fee_bps", "copy_friction_bps",
        "min_trade_notional_enabled",
    }
    model_refresh_required = any(before.get(k) != after.get(k) for k in model_fields)
    model_refresh_started = _kick_model_cache_refresh_background() if model_refresh_required else False
    if wants_json_response(request):
        return JSONResponse({
            "ok": True,
            "ui_state": after,
            "model_refresh_required": model_refresh_required,
            "model_refresh_started": model_refresh_started,
        })
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
    # Translate legacy gate vocab (ON/CLOSE_ONLY) to live_config vocab (LIVE/CLO/OFF)
    _mode_map = {"ON": "LIVE", "CLOSE_ONLY": "CLO", "LIVE": "LIVE", "CLO": "CLO", "OFF": "OFF"}
    lc_mode = _mode_map.get(mode)
    if lc_mode is None:
        return JSONResponse({"ok": False, "error": "BAD_MODE"}, status_code=400)
    try:
        config = _load_live_copy_config()
        wallets = config["wallets"]
        if wallet not in wallets:
            wallets[wallet] = {}
        wallets[wallet] = _normalise_live_wallet_payload({"mode": lc_mode, "enabled": lc_mode != "OFF"}, wallets[wallet])
        repair_live_config_consistency(config)
        _save_live_copy_config(config)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": type(exc).__name__}, status_code=500)
    return JSONResponse({"ok": True, "wallet": wallet, "mode": lc_mode, "core_reload_required": True, "core_confirmation_note": "Config saved — Core reload required to confirm WS tracking"})


@app.post("/api/set-all-modes")
async def set_all_modes(request: Request) -> JSONResponse:
    data = await request.json()
    mode = str(data.get("mode", "OFF")).upper()
    _mode_map = {"ON": "LIVE", "CLOSE_ONLY": "CLO", "LIVE": "LIVE", "CLO": "CLO", "OFF": "OFF"}
    lc_mode = _mode_map.get(mode)
    if lc_mode is None:
        return JSONResponse({"ok": False, "error": "BAD_MODE"}, status_code=400)
    try:
        config = _load_live_copy_config()
        wallets = config["wallets"]
        updated = 0
        for wallet in list(wallets.keys()):
            w_mode = "OFF" if wallet == USER_WALLET else lc_mode
            wallets[wallet] = _normalise_live_wallet_payload({"mode": w_mode, "enabled": w_mode != "OFF"}, wallets[wallet])
            updated += 1
        repair_live_config_consistency(config)
        _save_live_copy_config(config)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": type(exc).__name__}, status_code=500)
    return JSONResponse({"ok": True, "mode": lc_mode, "wallets_updated": updated})


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


@app.get("/live-copy", response_class=HTMLResponse)
def live_copy_dashboard():
    """Lightweight live-copy page: audit files only, no heavy SSOT rebuild."""
    return HTMLResponse(f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Live Copy Dashboard</title>
</head>
<body>
{render_live_copy_control_panel()}
</body>
</html>""")


def _tail_csv_rows(path: Path, limit: int = 8) -> List[Dict[str, Any]]:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return []
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            header = f.readline()
        if not header:
            return []
        need = max(1, int(limit))
        with path.open("rb") as f:
            size = f.seek(0, os.SEEK_END)
            block = min(size, 65536)
            data = b""
            while size > 0:
                f.seek(max(0, size - block))
                data = f.read(block) + data
                if data.count(b"\n") > need + 2 or size <= block:
                    break
                size -= block
                block = min(size, block * 2)
        lines = data.decode("utf-8-sig", errors="replace").splitlines()
        if lines and lines[0].strip() == header.strip():
            lines = lines[1:]
        tail_lines = [ln for ln in lines if ln.strip()][-need:]
        if not tail_lines:
            return []
        return list(csv.DictReader([header] + [ln + "\n" for ln in tail_lines]))
    except Exception:
        return []


def _core_process_rows() -> List[Dict[str, Any]]:
    ps = (
        "Get-CimInstance Win32_Process -Filter \"name = 'python.exe'\" | "
        "Where-Object { $_.CommandLine -match 'HL_Live_Copy_Service_Core.py' -and $_.CommandLine -notmatch 'HL_Copy_Engine' } | "
        "Select-Object ProcessId,ParentProcessId,CommandLine | ConvertTo-Json -Depth 4"
    )
    try:
        result = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True, timeout=5, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            return []
        data = json.loads(result.stdout)
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    except Exception:
        pass
    return []


def _file_age_payload(path: Path) -> Dict[str, Any]:
    try:
        st = path.stat()
        return {
            "exists": True,
            "mtime_utc": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
            "age_sec": round(max(0.0, time.time() - st.st_mtime), 1),
            "bytes": st.st_size,
        }
    except Exception:
        return {"exists": False, "mtime_utc": "", "age_sec": None, "bytes": 0}


def _emergency_live_snapshot() -> Dict[str, Any]:
    cfg = _load_live_copy_config()
    ws = _load_live_ws_health()
    clean = _load_clean_core_status()
    service = clean.get("service_state", {}) if isinstance(clean.get("service_state"), dict) else {}
    core_state = clean.get("core_state", {}) if isinstance(clean.get("core_state"), dict) else {}
    integrity = _apply_ownership_truth_to_integrity(_load_live_integrity_status(), _load_ownership_truth_gate())
    manual = load_json(MANUAL_POSITIONS_FILE, {})
    exchange = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, {})
    app_exchange = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE, {})
    wallet_truth = _compute_wallet_control_truth(cfg, clean, ws)
    active_wallets = list((cfg.get("wallets") or {}).keys()) if isinstance(cfg.get("wallets"), dict) else []
    ws_wallets = list((ws.get("wallets") or {}).keys()) if isinstance(ws.get("wallets"), dict) else []
    rows = {
        "latest_intents": _tail_csv_rows(LIVE_COPY_ORDER_INTENTS_CSV, 6),
        "latest_sends": _tail_csv_rows(SEND_ATTEMPTS_CSV, 6),
        "latest_live_fills": _tail_csv_rows(LIVE_FILLS_CSV, 6),
        "latest_reconciliation": _tail_csv_rows(LIVE_COPY_RECONCILIATION_CSV, 6),
    }
    return {
        "created_at": utc_now_iso(),
        "core_processes": _core_process_rows(),
        "service_state": service,
        "core_state": core_state,
        "ws_health": ws,
        "integrity": integrity,
        "wallet_truth": wallet_truth,
        "active_wallets": active_wallets,
        "ws_wallets": ws_wallets,
        "manual_summary": {
            "wallet_count": len((manual.get("by_wallet") or {}) if isinstance(manual, dict) else {}),
            "coin_count": len((manual.get("by_coin_net") or {}) if isinstance(manual, dict) else {}),
        },
        "exchange_snapshot": exchange if isinstance(exchange, dict) else {},
        "app_exchange_snapshot": app_exchange if isinstance(app_exchange, dict) else {},
        "files": {
            "live_service_state": _file_age_payload(LIVE_COPY_SERVICE_STATE_FILE),
            "live_ws_health": _file_age_payload(LIVE_COPY_WS_HEALTH_FILE),
            "live_integrity_status": _file_age_payload(LIVE_COPY_INTEGRITY_STATUS_FILE),
            "exchange_account_snapshot": _file_age_payload(EXCHANGE_ACCOUNT_SNAPSHOT_FILE),
            "manual_live_positions": _file_age_payload(MANUAL_POSITIONS_FILE),
        },
        "rows": rows,
    }


@app.get("/emergency-live", response_class=HTMLResponse)
def emergency_live_dashboard():
    """Read-only break-glass live truth page. No heavy SSOT, no JS dependency."""
    snap = _emergency_live_snapshot()
    service = snap["service_state"]
    ws_summary = snap["ws_health"].get("ws_summary", {}) if isinstance(snap["ws_health"].get("ws_summary"), dict) else {}
    integ_counts = snap["integrity"].get("counts", {}) if isinstance(snap["integrity"].get("counts"), dict) else {}
    hard = snap["integrity"].get("hard_copy_invariant", {}) if isinstance(snap["integrity"].get("hard_copy_invariant"), dict) else {}
    hard_counts = hard.get("counts", {}) if isinstance(hard.get("counts"), dict) else {}

    def esc(v: Any) -> str:
        return html.escape(str(v if v is not None else ""))

    def pill(label: str, ok: bool) -> str:
        cls = "ok" if ok else "bad"
        return f'<span class="pill {cls}">{esc(label)}</span>'

    def kv(label: str, value: Any, ok: Optional[bool] = None) -> str:
        cls = "ok" if ok is True else "bad" if ok is False else ""
        return f"<tr><th>{esc(label)}</th><td class='{cls}'>{esc(value)}</td></tr>"

    def row_table(rows: List[Dict[str, Any]], cols: List[str]) -> str:
        if not rows:
            return "<p class='muted'>No rows.</p>"
        head = "".join(f"<th>{esc(c)}</th>" for c in cols)
        body = ""
        for row in reversed(rows):
            body += "<tr>" + "".join(f"<td>{esc(row.get(c, ''))}</td>" for c in cols) + "</tr>"
        return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"

    core_pids = [str(p.get("ProcessId", "")) for p in snap["core_processes"]]
    active_set = {w.lower() for w in snap["active_wallets"]}
    ws_set = {w.lower() for w in snap["ws_wallets"]}
    status_html = " ".join([
        pill("Core process exactly one", len(core_pids) == 1),
        pill(f"WS {ws_summary.get('ws_status', service.get('ws_status', 'UNKNOWN'))}", str(ws_summary.get("ws_status") or service.get("ws_status")).upper() == "WS_OK"),
        pill(f"Poll {service.get('poll_loop_status', 'UNKNOWN')}", str(service.get("poll_loop_status")).upper() == "POLL_OK"),
        pill(f"Copy {service.get('copy_account_status', 'UNKNOWN')}", str(service.get("copy_account_status")).upper() == "COPY_ACCOUNT_POLLED"),
        pill(f"Snapshot {service.get('exchange_recon_status', 'UNKNOWN')}", str(service.get("exchange_recon_status")).upper() == "SNAPSHOT_OK"),
        pill("Runtime wallets match config", active_set == ws_set and len(active_set) == 10),
    ])
    wallet_rows = "".join(
        f"<tr><td>{i+1}</td><td>{esc(w)}</td><td>{esc('IN_WS' if w.lower() in ws_set else 'MISSING_WS')}</td></tr>"
        for i, w in enumerate(snap["active_wallets"])
    )
    file_rows = "".join(
        f"<tr><td>{esc(name)}</td><td>{esc(info.get('age_sec'))}</td><td>{esc(info.get('mtime_utc'))}</td><td>{esc(info.get('bytes'))}</td></tr>"
        for name, info in snap["files"].items()
    )
    return HTMLResponse(f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Emergency Live Truth</title>
<style>
body{{margin:0;background:#071017;color:#dbeafe;font-family:Arial,Helvetica,sans-serif;font-size:13px}}
main{{padding:18px;display:grid;gap:14px}} h1,h2{{margin:0 0 8px}} section{{border:1px solid #244057;background:#0d1822;border-radius:8px;padding:12px}}
table{{width:100%;border-collapse:collapse}} th,td{{border-bottom:1px solid #223342;padding:6px 8px;text-align:left;vertical-align:top}} th{{color:#9cc9ef}}
.pill{{display:inline-block;border:1px solid #36556d;border-radius:999px;padding:4px 8px;margin:2px;font-weight:700}}.ok{{color:#21c16b}}.bad{{color:#ff5263}}.muted{{color:#8fa3b7}} code{{color:#fbbf24}}
</style></head><body><main>
<h1>Emergency Live Truth <span class="muted">{esc(snap['created_at'])}</span></h1>
<section><h2>Status</h2>{status_html}<p class="muted">Read-only fallback. No trading or repair controls here.</p></section>
<section><h2>Core Runtime</h2><table>
{kv('Core PID(s)', ', '.join(core_pids) or 'NONE', len(core_pids)==1)}
{kv('Core command', snap['core_processes'][0].get('CommandLine','') if snap['core_processes'] else '', bool(core_pids))}
{kv('Service process_id', service.get('process_id'), service.get('process_id') == inum(core_pids[0]) if core_pids else False)}
{kv('Real order sending', service.get('effective_real_orders_enabled'), bool(service.get('effective_real_orders_enabled')))}
{kv('Send block reason', service.get('send_block_reason',''))}
{kv('Active RED', hard_counts.get('ACTIVE_RED', integ_counts.get('hard_copy_active_red')), inum(hard_counts.get('ACTIVE_RED', integ_counts.get('hard_copy_active_red'))) == 0)}
{kv('Unclassified', hard_counts.get('UNCLASSIFIED', integ_counts.get('hard_copy_unclassified')), inum(hard_counts.get('UNCLASSIFIED', integ_counts.get('hard_copy_unclassified'))) == 0)}
{kv('Exchange/manual mismatch', integ_counts.get('exchange_manual_mismatch'), inum(integ_counts.get('exchange_manual_mismatch')) == 0)}
</table></section>
<section><h2>File Freshness</h2><table><thead><tr><th>File</th><th>Age sec</th><th>mtime UTC</th><th>bytes</th></tr></thead><tbody>{file_rows}</tbody></table></section>
<section><h2>Active Wallets</h2><table><thead><tr><th>#</th><th>Wallet</th><th>Runtime</th></tr></thead><tbody>{wallet_rows}</tbody></table></section>
<section><h2>Latest Intents</h2>{row_table(snap['rows']['latest_intents'], ['created_at','leader_wallet','coin','leader_side','copy_side','decision','reason','intent_id'])}</section>
<section><h2>Latest Sends</h2>{row_table(snap['rows']['latest_sends'], ['created_at','leader_wallet','coin','side','status','terminal_state','exchange_order_id','intent_id'])}</section>
<section><h2>Latest Live Fills</h2>{row_table(snap['rows']['latest_live_fills'], ['created_at','leader_wallet','coin','side','fill_size','fill_price','exchange_order_id','intent_id'])}</section>
<section><h2>Latest Reconciliation</h2>{row_table(snap['rows']['latest_reconciliation'], ['created_at','event','status','leader_wallet','coin','terminal_state','action','intent_id'])}</section>
</main></body></html>""")


@app.get("/live-copy.json")
def live_copy_dashboard_json():
    return JSONResponse({"ok": False, "error": "REMOVED_FROM_WALLET_FINDER_APP"}, status_code=410)


@app.get("/api/live-copy-summary")
def get_live_copy_summary():
    return JSONResponse({"ok": False, "error": "REMOVED_FROM_WALLET_FINDER_APP"}, status_code=410)


@app.get("/api/global-controls")
def get_global_controls():
    cfg = _load_live_copy_config()
    saved_gc = _global_controls_for_ui(cfg.get("global_controls", _GLOBAL_CONTROLS_DEFAULTS))
    clean_core = _load_clean_core_status()
    service_state = clean_core.get("service_state", {})
    integrity_status = _apply_ownership_truth_to_integrity(_load_live_integrity_status(), _load_ownership_truth_gate())
    integrity_counts = integrity_status.get("counts") if isinstance(integrity_status.get("counts"), dict) else {}
    hard = integrity_status.get("hard_copy_invariant") if isinstance(integrity_status.get("hard_copy_invariant"), dict) else {}
    hard_counts = hard.get("counts") if isinstance(hard.get("counts"), dict) else {}
    core_eff = service_state.get("effective_global_controls") if isinstance(service_state.get("effective_global_controls"), dict) else None
    cycle_budget_exceeded = bool(service_state.get("cycle_time_budget_exceeded", service_state.get("budget_exceeded", False)))
    drift_fields: list = []
    if core_eff:
        for fld in ("max_total_live_exposure_usd", "max_wallet_exposure_usd", "max_order_notional_usd", "marketable_bps", "max_close_adverse_diff_pct"):
            sv = round(float(saved_gc.get(fld) or 0), 6)
            cv = round(float(core_eff.get(fld) or 0), 6)
            if sv != cv:
                drift_fields.append({"field": fld, "saved": sv, "core_effective": cv})
        for fld in ("symbol_allowlist", "symbol_blocklist"):
            sv = sorted([str(x).upper() for x in (saved_gc.get(fld) or [])])
            cv = sorted([str(x).upper() for x in (core_eff.get(fld) or [])])
            if sv != cv:
                drift_fields.append({"field": fld, "saved": sv, "core_effective": cv})
    return JSONResponse({
        "ok": True,
        "global_controls": saved_gc,
        "core_effective_global_controls": core_eff,
        "core_runtime": {
            "process_id": service_state.get("process_id"),
            "ws_status": service_state.get("ws_status"),
            "poll_loop_status": service_state.get("poll_loop_status"),
            "copy_account_status": service_state.get("copy_account_status"),
            "exchange_recon_status": service_state.get("exchange_recon_status"),
            "active_wallets": service_state.get("active_wallets"),
            "wallet_modes_active": service_state.get("wallet_modes_active"),
            "effective_real_orders_enabled": service_state.get("effective_real_orders_enabled"),
            "master_real_orders_enabled": service_state.get("master_real_orders_enabled"),
            "send_block_reason": service_state.get("send_block_reason"),
            "last_cycle_finished_at": service_state.get("last_cycle_finished_at") or service_state.get("created_at"),
            "integrity_status": integrity_status.get("status"),
            "integrity_counts": integrity_counts,
            "hard_copy_counts": hard_counts,
        },
        "config_drift": len(drift_fields) > 0,
        "config_drift_fields": drift_fields,
        "cycle_time_budget_exceeded": cycle_budget_exceeded,
        "budget_exceeded_note": "cycle_timing_only — not a financial cap; applies no order blocks",
        "core_effective_available": core_eff is not None,
    })


@app.post("/api/global-controls")
async def set_global_controls(req: Request):
    return JSONResponse({"ok": False, "error": "REMOVED_FROM_WALLET_FINDER_APP"}, status_code=410)
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


def _live_audit_summary_lightweight(reason: str = "heavy audit summary rebuilding") -> Dict[str, Any]:
    """Fast degraded summary for the live page.

    This never scans the large append-only CSVs and never calls Hyperliquid. It keeps
    the live UI truthful and usable after App restart or while the heavy audit cache
    rebuilds in the background.
    """
    live_config = _load_live_copy_config()
    ws_health = _load_live_ws_health()
    clean_core_status = _load_clean_core_status()
    service_state = clean_core_status.get("service_state", {}) if isinstance(clean_core_status.get("service_state"), dict) else {}
    integrity_status = _apply_ownership_truth_to_integrity(_load_live_integrity_status(), _load_ownership_truth_gate())
    wallet_control_truth = _compute_wallet_control_truth(live_config, clean_core_status, ws_health)
    live_top_status = _build_live_top_status(live_config, ws_health, service_state, integrity_status, [], [], [])

    counts = integrity_status.get("counts") if isinstance(integrity_status.get("counts"), dict) else {}
    pos_assign = integrity_status.get("position_assignment") if isinstance(integrity_status.get("position_assignment"), dict) else {}
    hard = integrity_status.get("hard_copy_invariant") if isinstance(integrity_status.get("hard_copy_invariant"), dict) else {}
    hard_counts = hard.get("counts") if isinstance(hard.get("counts"), dict) else {}
    snapshot = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE, {})
    if not isinstance(snapshot, dict) or not snapshot:
        snapshot = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, {})
    if not isinstance(snapshot, dict):
        snapshot = {}
    snapshot_available = bool(snapshot)
    if snapshot_available and "available" not in snapshot:
        snapshot = {**snapshot, "available": True, "status": snapshot.get("status") or "OK"}

    return {
        "ok": True,
        "degraded": True,
        "stale": True,
        "cache_hit": False,
        "load_error": reason,
        "error": reason,
        "build_seconds": 0.0,
        "exchange_snapshot_status": str(snapshot.get("status") or ("OK" if snapshot_available else "UNAVAILABLE")),
        "live_top_status": live_top_status,
        "live_integrity_status": integrity_status,
        "position_integrity_card": {
            "core_status": str(integrity_status.get("status") or "UNKNOWN").upper(),
            "position_assignment": str(pos_assign.get("status") or "UNKNOWN").upper(),
            "exchange_manual_mismatch_count": inum(counts.get("exchange_manual_mismatch")),
            "hard_copy_active_red": inum(hard_counts.get("ACTIVE_RED", counts.get("hard_copy_active_red"))),
            "hard_copy_unclassified": inum(hard_counts.get("UNCLASSIFIED", counts.get("hard_copy_unclassified"))),
            "missed_entry": inum(counts.get("missed_entry")),
            "missed_add": inum(counts.get("missed_add")),
            "real_orphan_count": inum(counts.get("real_orphan_count")),
            "real_orphan_exposure_usd": 0,
            "owned_copy_count": 0,
            "xyz_exotic_matched_count": 0,
            "btc_residual_accounted": False,
            "integrity_timestamp": str(integrity_status.get("created_at") or utc_now_iso()),
        },
        "wallet_control_truth": wallet_control_truth,
        "exchange_account_snapshot": snapshot,
        "manual_live_summary": {},
        "live_wallet_rows": [],
        "owned_copy_positions": [],
        "orphan_exchange_positions": [],
        "manual_reconciliation_rows": [],
        "execution_quality_rows": [],
        "send_terminal_rows": [],
        "legacy_terminal_rows": [],
        "recent_send_attempts": [],
        "recent_metric_send_attempts": [],
        "recent_reconciliation_events": [],
        "reconciliation_rows": [],
        "live_leader_performance": {},
        "live_graph": {"portfolio_points": [], "realized_pnl_points": []},
        "exchange_account_history": [],
        "last_rows": [],
        "reason_counts": {},
        "status_counts": {},
        "source_counts": {},
        "execution_decision_counts": {},
        "decision_reason_counts": {},
        "manual_reconcile_required_counts": {},
        "market_data_error_counts": {},
        "audit_rows": [],
    }


def _save_live_audit_summary_last_good(summary: Dict[str, Any]) -> None:
    """Persist the last enriched live summary so App restarts do not blind /live-copy."""
    try:
        APP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = dict(summary or {})
        payload["_persisted_at"] = utc_now_iso()
        payload["_persisted_at_epoch"] = time.time()
        atomic_write_json(LIVE_AUDIT_SUMMARY_LAST_GOOD_FILE, payload)
    except Exception:
        pass


def _load_live_audit_summary_last_good() -> Dict[str, Any]:
    try:
        data = load_json(LIVE_AUDIT_SUMMARY_LAST_GOOD_FILE, {})
        return data if isinstance(data, dict) and data.get("ok", True) is not False else {}
    except Exception:
        return {}


def _rebuild_stale_position_truth(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Stale/cold-cache safety: never serve INHERITED position rows as live truth.

    Owned/orphan position rows are cheaply rebuilt from the CURRENT manual ledger
    (no append-only CSV scan, no network) so direction/size reflect the ledger now —
    e.g. a coin that flipped SHORT will not display a stale LONG row. Per-wallet
    `current_open_positions` cannot be cheaply rebuilt (the leader-performance build is
    heavy), so they are blanked and flagged for the UI to label as rebuilding.
    """
    try:
        manual_positions = _load_manual_live_positions()
        snapshot = payload.get("exchange_account_snapshot")
        if not isinstance(snapshot, dict) or not snapshot:
            snapshot = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE, {})
            if not isinstance(snapshot, dict) or not snapshot:
                snapshot = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_FILE, {})
        real = _build_real_copy_positions(
            manual_positions,
            snapshot if isinstance(snapshot, dict) else {},
            service_evidence={},
        )
        payload["owned_copy_positions"] = [r for r in real if r.get("row_type") == "OWNED_COPY"]
        payload["orphan_exchange_positions"] = [r for r in real if r.get("row_type") == "ACCOUNT_LEVEL_ONLY"]
        payload["position_rows_rebuilt_from_ledger"] = True
    except Exception:
        # On any failure, blank rather than serve stale inherited rows as live truth.
        payload["owned_copy_positions"] = []
        payload["orphan_exchange_positions"] = []
        payload["position_rows_rebuilt_from_ledger"] = False
    perf = payload.get("live_leader_performance")
    if isinstance(perf, dict):
        for _p in perf.values():
            if isinstance(_p, dict):
                _p["current_open_positions"] = []
                _p["current_open_positions_stale"] = True
    payload["position_rows_stale"] = True
    return payload


def _merge_last_good_with_fresh_live_truth(last_good: Dict[str, Any], reason: str) -> Dict[str, Any]:
    """Keep enriched rows/charts, but replace control/status truth with fresh Core files."""
    fresh = _live_audit_summary_lightweight(reason)
    merged = dict(last_good or {})
    for key in (
        "live_top_status",
        "live_integrity_status",
        "position_integrity_card",
        "wallet_control_truth",
        "exchange_account_snapshot",
        "exchange_snapshot_status",
    ):
        merged[key] = fresh.get(key)
    merged.update({
        "ok": True,
        "cache_hit": True,
        "stale": True,
        "degraded": True,
        "load_error": f"{reason}; showing last-good enriched stats",
        "error": f"{reason}; showing last-good enriched stats",
        "last_good_persisted_at": last_good.get("_persisted_at"),
    })
    # Position rows inherited from last_good may predate ledger flips (e.g. NEAR LONG->SHORT).
    # Rebuild owned/orphan from the current ledger; blank per-wallet open positions.
    _rebuild_stale_position_truth(merged)
    return merged


def _audit_summary_background_rebuild() -> None:
    """Background rebuild of live audit summary cache — never blocks the request thread."""
    if _AUDIT_SUMMARY_REBUILD_ACTIVE.is_set():
        return
    _AUDIT_SUMMARY_REBUILD_ACTIVE.set()
    try:
        result = _live_audit_summary()
        snapshot = result.get("exchange_account_snapshot") or {}
        result["build_seconds"] = 0.0
        result["exchange_snapshot_status"] = str(
            snapshot.get("status") or ("OK" if snapshot.get("ok") else "UNAVAILABLE")
        )
        result["cache_hit"] = False
        with _AUDIT_SUMMARY_CACHE_LOCK:
            _AUDIT_SUMMARY_CACHE["data"] = result
            _AUDIT_SUMMARY_CACHE["built_at"] = time.time()
        _save_live_audit_summary_last_good(result)
    except Exception:
        pass
    finally:
        _AUDIT_SUMMARY_REBUILD_ACTIVE.clear()


@app.get("/api/live-audit-summary")
def get_live_audit_summary():
    """Return live audit summary with non-blocking stale-serve + background rebuild.

    - Cache hit (fresh ≤ TTL): serve immediately.
    - Cache stale (> TTL, ≤ max-stale): serve stale immediately, kick background rebuild.
    - Cache empty or too old: block once to build, then serve.
    """
    now = time.time()
    with _AUDIT_SUMMARY_CACHE_LOCK:
        cached_data = _AUDIT_SUMMARY_CACHE.get("data")
        built_at = float(_AUDIT_SUMMARY_CACHE.get("built_at") or 0.0)
    cache_age = now - built_at
    if cached_data is not None and cache_age <= _AUDIT_SUMMARY_TTL:
        return JSONResponse({**cached_data, "cache_hit": True})
    if cached_data is not None and cache_age <= _AUDIT_SUMMARY_MAX_STALE_SECS:
        # Serve stale immediately; kick background rebuild (non-blocking)
        threading.Thread(target=_audit_summary_background_rebuild, daemon=True).start()
        stale_top = dict(cached_data.get("live_top_status") or {})
        stale_integrity = dict(cached_data.get("live_integrity_status") or {})
        if str(stale_top.get("integrity_status") or "").upper() == "GREEN":
            stale_top["integrity_status"] = "STALE"
        if str(stale_integrity.get("status") or "").upper() == "GREEN":
            stale_integrity["status"] = "STALE"
        return JSONResponse({
            **cached_data,
            "live_top_status": stale_top,
            "live_integrity_status": stale_integrity,
            "cache_hit": True,
            "stale": True,
            "stale_age_secs": round(cache_age, 1),
        })
    # Cold cache: build once synchronously. Live-config saves deliberately clear
    # this cache, so serving last-good here makes fixed/proportional edits look
    # like they had no effect.
    try:
        result = _live_audit_summary()
        with _AUDIT_SUMMARY_CACHE_LOCK:
            _AUDIT_SUMMARY_CACHE["data"] = result
            _AUDIT_SUMMARY_CACHE["built_at"] = time.time()
        _save_live_audit_summary_last_good(result)
        return JSONResponse({**result, "cache_hit": False})
    except Exception as exc:
        with _AUDIT_SUMMARY_CACHE_LOCK:
            stale = _AUDIT_SUMMARY_CACHE.get("data")
            stale_built_at = float(_AUDIT_SUMMARY_CACHE.get("built_at") or 0.0)
        stale_age = time.time() - stale_built_at
        if stale is not None:
            return JSONResponse({**stale, "cache_hit": True, "stale": True, "error": str(exc)})
        return JSONResponse({"ok": False, "stale": True, "error": str(exc)}, status_code=500)


@app.post("/api/live-config/add-wallet")
async def add_live_config_wallet(req: Request):
    return JSONResponse({"ok": False, "error": "REMOVED_FROM_WALLET_FINDER_APP"}, status_code=410)
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
        invalidate_model_cache()
        invalidate_live_audit_summary_cache()
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
        _save_live_copy_config(config)
        invalidate_model_cache()
        invalidate_live_audit_summary_cache()
        return JSONResponse(_live_copy_config_response(config))
    except ValueError as exc:
        return _live_config_error(str(exc))
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


@app.post("/api/live-config/remove-wallet")
async def remove_live_config_wallet(req: Request):
    return JSONResponse({"ok": False, "error": "REMOVED_FROM_WALLET_FINDER_APP"}, status_code=410)
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
        invalidate_model_cache()
        invalidate_live_audit_summary_cache()
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
        invalidate_model_cache()
        invalidate_live_audit_summary_cache()
        return JSONResponse(_live_copy_config_response(config))
    except ValueError as exc:
        return _live_config_error(str(exc))
    except Exception as exc:
        return _live_config_error(type(exc).__name__)


def _load_manual_recon_actions() -> Dict[str, Any]:
    data = load_json(MANUAL_RECON_ACTIONS_FILE, {})
    if not isinstance(data, dict):
        data = {}
    for key in ("pending", "applied", "cancelled"):
        if not isinstance(data.get(key), list):
            data[key] = []
    return data


def _save_manual_recon_actions(data: Dict[str, Any]) -> None:
    LIVE_COPY_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    data["last_updated"] = utc_now_iso()
    atomic_write_json(MANUAL_RECON_ACTIONS_FILE, data)


@app.get("/api/reconciliation/pending-repairs")
async def get_pending_repairs():
    data = _load_manual_recon_actions()
    _by_ms = lambda lst: sorted([r for r in lst if isinstance(r, dict)], key=lambda r: str(r.get("created_at_ms") or 0), reverse=True)
    return JSONResponse({
        "ok": True,
        "pending": data.get("pending", []),
        "applied_recent": _by_ms(data.get("applied", []))[:10],
        "cancelled_recent": _by_ms(data.get("cancelled", []))[:5],
    })


@app.post("/api/reconciliation/create-repair-request")
async def create_recon_repair_request(req: Request):
    _SAFE_TYPES = {"ADOPT_MANUAL_CLOSE", "ASSIGN_ORPHAN_TO_WALLET", "CLOSE_UNOWNED_EXPOSURE", "MARK_RESIDUAL_ACCOUNTED"}
    try:
        body = await req.json()
        action_type = str(body.get("action_type") or "")
        if action_type not in _SAFE_TYPES:
            return JSONResponse({"ok": False, "error": f"INVALID_ACTION_TYPE: {action_type}"}, status_code=400)
        coin = str(body.get("coin") or "").upper().strip()
        if not coin or coin == "—":
            return JSONResponse({"ok": False, "error": "COIN_REQUIRED"}, status_code=400)
        wallet = str(body.get("wallet") or "").strip().lower()
        audit_note = str(body.get("audit_note") or "").strip()
        if action_type == "ASSIGN_ORPHAN_TO_WALLET" and not audit_note:
            return JSONResponse({"ok": False, "error": "AUDIT_NOTE_REQUIRED for ASSIGN_ORPHAN_TO_WALLET"}, status_code=400)
        integrity = _load_live_integrity_status()
        hc = integrity.get("hard_copy_invariant") if isinstance(integrity.get("hard_copy_invariant"), dict) else {}
        hc_counts = hc.get("counts") if isinstance(hc.get("counts"), dict) else {}
        active_red = inum(hc_counts.get("ACTIVE_RED"))
        unclassified = inum(hc_counts.get("UNCLASSIFIED"))
        if active_red > 0 and action_type != "MARK_RESIDUAL_ACCOUNTED":
            return JSONResponse({"ok": False, "error": f"BLOCKED_ACTIVE_RED: {active_red} active_red row(s) must be resolved first"}, status_code=409)
        if unclassified > 0:
            return JSONResponse({"ok": False, "error": f"BLOCKED_UNCLASSIFIED: {unclassified} unclassified row(s) must be resolved first"}, status_code=409)
        now_ms = int(time.time() * 1000)
        snapshot_age_ms = _RECON_SNAPSHOT_STALE_MS + 1
        if action_type != "MARK_RESIDUAL_ACCOUNTED":
            snap = load_json(EXCHANGE_ACCOUNT_SNAPSHOT_APP_FILE, {})
            snap_ts = inum(snap.get("fetched_at_ms") or snap.get("created_at_ms")) if isinstance(snap, dict) else 0
            snapshot_age_ms = (now_ms - snap_ts) if snap_ts > 0 else _RECON_SNAPSHOT_STALE_MS + 1
            if snapshot_age_ms > _RECON_SNAPSHOT_STALE_MS:
                return JSONResponse({"ok": False, "error": f"SNAPSHOT_STALE: age {snapshot_age_ms // 1000}s > {_RECON_SNAPSHOT_STALE_MS // 1000}s limit"}, status_code=409)
        data = _load_manual_recon_actions()
        for r in data.get("pending", []):
            if (isinstance(r, dict) and str(r.get("wallet") or "").lower() == wallet
                    and str(r.get("coin") or "").upper() == coin
                    and str(r.get("action_type") or "") == action_type
                    and str(r.get("status") or "") in {"REQUESTED", "PENDING_USER_CONFIRM"}):
                return JSONResponse({"ok": False, "error": f"DUPLICATE_PENDING_REQUEST for {action_type}/{coin} id={r.get('request_id', '')}"}, status_code=409)
        request_id = str(uuid.uuid4())
        repair_req: Dict[str, Any] = {
            "request_id": request_id,
            "created_at": utc_now_iso(),
            "created_at_ms": now_ms,
            "action_type": action_type,
            "wallet": wallet,
            "coin": coin,
            "old_ledger_size": fnum(body.get("old_ledger_size")),
            "exchange_size": fnum(body.get("exchange_size")),
            "target_wallet": str(body.get("target_wallet") or "").strip().lower() or None,
            "audit_note": audit_note,
            "status": "REQUESTED",
            "user_visible_reason": str(body.get("user_visible_reason") or ""),
            "requires_core_apply": action_type not in {"MARK_RESIDUAL_ACCOUNTED"},
            "safety_snapshot": {
                "active_red_at_create": active_red,
                "unclassified_at_create": unclassified,
                "snapshot_age_ms_at_create": snapshot_age_ms,
                "integrity_status_at_create": str(integrity.get("status") or "UNKNOWN"),
            },
        }
        data["pending"].append(repair_req)
        _save_manual_recon_actions(data)
        return JSONResponse({"ok": True, "request_id": request_id, "status": "REQUESTED", "requires_core_apply": repair_req["requires_core_apply"]})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": type(exc).__name__ + ": " + str(exc)}, status_code=500)


@app.post("/api/reconciliation/cancel-repair-request")
async def cancel_recon_repair_request(req: Request):
    try:
        body = await req.json()
        request_id = str(body.get("request_id") or "").strip()
        if not request_id:
            return JSONResponse({"ok": False, "error": "REQUEST_ID_REQUIRED"}, status_code=400)
        data = _load_manual_recon_actions()
        found = None
        remaining = []
        for r in data.get("pending", []):
            if isinstance(r, dict) and r.get("request_id") == request_id:
                found = r
            else:
                remaining.append(r)
        if not found:
            return JSONResponse({"ok": False, "error": "REQUEST_NOT_FOUND"}, status_code=404)
        if str(found.get("status") or "") not in {"REQUESTED", "PENDING_USER_CONFIRM"}:
            return JSONResponse({"ok": False, "error": f"CANNOT_CANCEL: status={found.get('status')}"}, status_code=409)
        found["status"] = "CANCELLED"
        found["cancelled_at"] = utc_now_iso()
        data["pending"] = remaining
        if not isinstance(data.get("cancelled"), list):
            data["cancelled"] = []
        data["cancelled"].append(found)
        _save_manual_recon_actions(data)
        return JSONResponse({"ok": True, "cancelled_request_id": request_id})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": type(exc).__name__ + ": " + str(exc)}, status_code=500)


@app.post("/api/manual-reconciliation/archive-ledger-row")
async def archive_manual_reconciliation_ledger_row(req: Request):
    return JSONResponse(
        {"ok": False, "error": "DISABLED: manual ledger mutation disabled — Core is sole writer of manual_live_positions.json"},
        status_code=405,
    )


if __name__ == "__main__":
    if uvicorn is None:
        raise SystemExit("Missing uvicorn. Install with: pip install uvicorn fastapi")
    uvicorn.run("wallet_proof_engine_8014:app", host="127.0.0.1", port=8014, reload=False)
