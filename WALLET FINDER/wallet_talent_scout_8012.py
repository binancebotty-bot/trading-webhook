"""
app.py â€” WALLET FINDER EDITION
Hyperliquid Proving Engine (Pre-Live Forward-Testing Dashboard)

FastAPI + HTMX dashboard running on port 8012.
Reads from data/wallet_universe.csv, data/summary.csv, and data/equity_curves/.
Retains all SSOT mechanics of the proof dash before live copy module.

Compound score replaces the old simplistic copy_score:
  - MTM Calmar (40%): Risk-adjusted return from accountValueHistory
  - PnL stability (15%): Chunk-level win rate consistency
  - Fee resilience (15%): Net PnL / Gross PnL â€” does profit survive fees?
  - Recency (10%): Fraction of trades in last 7 days
  - Hard penalty flags: martingale -25pts, one_big_trade -50pts,
    equity_collapse -30pts, equity_collapse_mtm -30pts
"""
import asyncio
import atexit
import json
import logging
import math
import os
import pickle
import sys
import ctypes
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape
from last_trade_index import DEFAULT_INITIAL_TAIL_BYTES, update_last_trade_times
from markupsafe import Markup

log = logging.getLogger("ui_state")

# â”€â”€ Paths (WALLET FINDER edition) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
BASE_DIR = Path(__file__).parent
# Separate state file for Wallet Finder — avoids race condition with HL_Copy_App_SSOT.py
# which strips unknown keys from ui_state.json on every save cycle.
STATE_FILE = BASE_DIR / "wallet_finder_state.json"
# Copy app's state file — read wallet_include from here (copy app is source of truth)
COPY_STATE_FILE = BASE_DIR / "ui_state.json"
STATE_BACKUP_DIR = BASE_DIR / "proofs" / "ui_state_backups"
UNIVERSE_PATH = BASE_DIR / "data" / "wallet_universe.csv"
# Compound scoring and MTM DD need KPI columns from the scanner's summary output.
SUMMARY_CANDIDATES = [
    BASE_DIR / "data" / "summary.csv",
    BASE_DIR.parent / "hl_stage2" / "summary.csv",
]
SUMMARY_PATH = next((p for p in SUMMARY_CANDIDATES if p.exists()), SUMMARY_CANDIDATES[0])
CURVE_PATH = BASE_DIR / "data" / "equity_curves"
MTM_CACHE_DIR = BASE_DIR / "data" / "wallet_portfolios"
TRADE_REPLAY_CACHE_DIR = BASE_DIR / "data" / "trade_replay_cache"
TEMPLATES_DIR = BASE_DIR / "templates"
PURGED_WALLETS_FILE = BASE_DIR / "purged_wallets.txt"
DATA_PURGED_WALLETS_FILE = BASE_DIR / "data" / "purged_wallets.txt"

ALL_TRADES_CANDIDATES = [
    BASE_DIR / "data" / "all_trades.csv",
    BASE_DIR / "data" / "all_trades.zip",
]
ENABLE_ALL_TRADES_TABLE_ENRICH = False
ENABLE_LAST_TRADE_TIME_OVERLAY = True

# â”€â”€ WALLET FINDER port â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
WALLET_FINDER_PORT = 8012

SLIPPAGE_RATES = {
    "pnl_slip_05": 0.0005,
    "pnl_slip_10": 0.0010,
    "pnl_slip_30": 0.0030,
}

_VALID_MODES = ("raw", "slip_05", "slip_10", "slip_30")
_MODE_COL = {
    "raw": "realised_pnl",
    "slip_05": "pnl_slip_05",
    "slip_10": "pnl_slip_10",
    "slip_30": "pnl_slip_30",
}
_MODE_RATE = {
    "raw": 0.0,
    "slip_05": 0.0005,
    "slip_10": 0.0010,
    "slip_30": 0.0030,
}
MIN_STABLE_DD_PNL_FRACTION = 0.02
MAX_RELIABLE_MARTINGALE_FLAG_RATE = 0.80

SORTABLE_COLUMNS = {
    "rank": "rank", "wallet": "wallet", "score": "score",
    "realised_pnl": "realised_eval_pnl", "pnl_slip_05": "pnl_slip_05",
    "pnl_slip_10": "pnl_slip_10", "pnl_slip_30": "pnl_slip_30",
    "risk_pnl": "risk_pnl", "slip_deg_03": "slip_deg_03",
    "efficiency": "efficiency", "max_dd": "max_dd_pct",
    "trades": "trades", "trades_7d": "trades_7d",
    "total_notional": "total_notional",
    "compound_score": "compound_score", "mtm_calmar": "mtm_calmar",
    "last_trade_time": "last_trade_time",
}
SORTABLE_COLUMN_VALUES = set(SORTABLE_COLUMNS.values())

# â”€â”€ Jinja2 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_jinja = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html"]),
)


def render(template_name: str, **ctx) -> str:
    return _jinja.get_template(template_name).render(**ctx)


def _js_num(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return str(round(float(v), 6))


_jinja.filters["js_num"] = _js_num


# â”€â”€ State â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_state: dict = {
    "universe": pd.DataFrame(),
    "selected": {},
    "trade_stats": {},
    "trade_stats_key": None,
    "days": "30",
    "filters": {},
    "mode": "raw",
    "sort": "last_trade_time",
    "dir": "desc",
    "risk_per_wallet": 100.0,
    "normalize": True,
    "wallet_order": [],
    "wallet_weights": {},
    "start_date": None,
    "end_date": None,
    "hide_dormant": False,
}
_STATE_SAVE_ENABLED = False
_martingale_flag_reliable_cache: bool | None = None


def _selected_true_count(state_obj: dict) -> int:
    selected = state_obj.get("selected", {}) if isinstance(state_obj, dict) else {}
    return sum(1 for v in selected.values() if bool(v)) if isinstance(selected, dict) else 0


def _martingale_flag_reliable() -> bool:
    """The current scanner flag is unusable if it marks nearly the whole universe."""
    global _martingale_flag_reliable_cache
    if _martingale_flag_reliable_cache is not None:
        return _martingale_flag_reliable_cache
    try:
        summary = _state.get("summary")
        if summary is None or summary.empty or "martingale_flag" not in summary.columns:
            _martingale_flag_reliable_cache = False
            return _martingale_flag_reliable_cache
        flags = pd.to_numeric(summary["martingale_flag"], errors="coerce").dropna()
        if flags.empty:
            _martingale_flag_reliable_cache = False
            return _martingale_flag_reliable_cache
        _martingale_flag_reliable_cache = bool((flags == 1).mean() <= MAX_RELIABLE_MARTINGALE_FLAG_RATE)
        return _martingale_flag_reliable_cache
    except Exception:
        _martingale_flag_reliable_cache = False
        return _martingale_flag_reliable_cache


def _latest_state_with_selection() -> dict:
    """Recover Talent Scout-owned selection keys from the latest rotating backup."""
    try:
        candidates = sorted(
            STATE_BACKUP_DIR.glob("ui_state_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        candidates = []
    for path in candidates:
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        if isinstance(data, dict) and _selected_true_count(data) > 0:
            log.warning("Recovered Talent Scout selection from %s", path.name)
            return data
    return {}


def _save_state():
    """Atomic save: write to temp file, then os.replace() — no partial writes possible.
    Writes only Wallet Finder keys to wallet_finder_state.json (separate from copy app)."""
    if not _STATE_SAVE_ENABLED:
        log.warning("Skipping wallet_finder_state save before state load completed")
        return
    _state.pop("_selection_dirty", None)
    _state.pop("_allow_empty_selection_save", None)
    selected_payload = _state.get("selected", {})
    order_payload = _state.get("wallet_order", [])
    weights_payload = _state.get("wallet_weights", {})

    # Write only keys this app owns
    payload_data = {
        "sort": _state.get("sort"),
        "dir": _state.get("dir"),
        "selected": selected_payload,
        "wallet_order": order_payload,
        "wallet_weights": weights_payload,
        "mode": _state.get("mode"),
        "filters": _state.get("filters"),
        "risk_per_wallet": _state.get("risk_per_wallet"),
        "normalize": _state.get("normalize"),
        "days": _state.get("days"),
        "start_date": _state.get("start_date"),
        "end_date": _state.get("end_date"),
        "hide_dormant": _state.get("hide_dormant", False),
        "page": _state.get("page", 1),
        "per_page": _state.get("per_page", 100),
    }
    # wallet_include is managed by copy app; only persist if we have it
    if "wallet_include" in _state:
        payload_data["wallet_include"] = _state.get("wallet_include", {})

    payload = json.dumps(payload_data, indent=2)
    try:
        # Write to temp file in SAME directory (same filesystem = atomic rename)
        fd, tmp_path = tempfile.mkstemp(
            dir=str(BASE_DIR), prefix=".wf_state_", suffix=".tmp"
        )
        try:
            os.write(fd, payload.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        # Backup previous good copy before overwriting
        if STATE_FILE.exists():
            try:
                BACKUP_FILE = STATE_FILE.with_suffix(".json.bak")
                current_bytes = STATE_FILE.read_bytes()
                BACKUP_FILE.write_bytes(current_bytes)
                STATE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
                stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
                (STATE_BACKUP_DIR / f"wf_state_{stamp}.json").write_bytes(current_bytes)
            except Exception:
                pass
        # Atomic rename — no reader will ever see a half-written file
        os.replace(tmp_path, str(STATE_FILE))
        log.debug("wallet_finder_state saved (%d selected wallets)", _selected_true_count(payload_data))
    except Exception as exc:
        log.error("FAILED to save wallet_finder_state: %s", exc)
        # Last resort: direct write so at least something persists
        try:
            STATE_FILE.write_text(payload, encoding="utf-8")
            log.warning("wallet_finder_state saved via fallback direct write")
        except Exception as exc2:
            log.error("CRITICAL: wallet_finder_state fallback write also failed: %s", exc2)


def _load_state():
    """Load state from disk. Falls back to .bak if main file is corrupt.

    Migration: if wallet_finder_state.json doesn't exist but ui_state.json
    does, migrate Wallet Finder keys from ui_state.json (the old shared file).
    Always overlay wallet_include from ui_state.json (copy app is source of truth).
    """
    global _STATE_SAVE_ENABLED

    # --- Step 1: Load from our own state file (or migrate from old shared file) ---
    backup_file = STATE_FILE.with_suffix(".json.bak")
    loaded = False
    for attempt_path in (STATE_FILE, backup_file):
        if not attempt_path.exists():
            continue
        try:
            raw = attempt_path.read_text(encoding="utf-8-sig")
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError(f"Expected dict, got {type(data).__name__}")
            recovered_selection = False
            if "selected" not in data:
                recovered = _latest_state_with_selection()
                if recovered:
                    data["selected"] = recovered.get("selected", {})
                    data["wallet_order"] = recovered.get("wallet_order", [])
                    data["wallet_weights"] = recovered.get("wallet_weights", {})
                    recovered_selection = True
            _state.update(data)
            selected_count = sum(1 for v in _state.get("selected", {}).values() if v)
            log.info(
                "Loaded wallet_finder_state from %s (%d total wallets, %d selected)",
                attempt_path.name,
                len(_state.get("selected", {})),
                selected_count,
            )
            loaded = True
            _STATE_SAVE_ENABLED = True
            if recovered_selection and _selected_true_count(_state) > 0:
                _state["_selection_dirty"] = True
                _save_state()
            break
        except Exception as exc:
            log.warning("Failed to load %s: %s", attempt_path.name, exc)

    # --- Step 1b: Migration — if no wallet_finder_state.json, try ui_state.json ---
    if not loaded and COPY_STATE_FILE.exists():
        try:
            raw = COPY_STATE_FILE.read_text(encoding="utf-8-sig")
            copy_data = json.loads(raw)
            if isinstance(copy_data, dict):
                wf_keys = {"sort", "dir", "selected", "wallet_order", "wallet_weights",
                           "mode", "filters", "risk_per_wallet", "normalize", "days",
                           "start_date", "end_date", "hide_dormant", "page", "per_page"}
                migrated = {k: v for k, v in copy_data.items() if k in wf_keys}
                if migrated:
                    _state.update(migrated)
                    log.info("Migrated %d Wallet Finder keys from ui_state.json", len(migrated))
                    loaded = True
        except Exception as exc:
            log.warning("Migration read from ui_state.json failed: %s", exc)

    # --- Step 2: Always overlay wallet_include from copy app's state (source of truth) ---
    if COPY_STATE_FILE.exists():
        try:
            raw = COPY_STATE_FILE.read_text(encoding="utf-8-sig")
            copy_data = json.loads(raw)
            if isinstance(copy_data, dict) and "wallet_include" in copy_data:
                _state["wallet_include"] = copy_data["wallet_include"]
                log.debug("Overlayed wallet_include from ui_state.json (%d entries)",
                          len(copy_data.get("wallet_include", {})))
        except Exception:
            pass

    if not loaded:
        log.warning("No valid state file found — using defaults")
    _STATE_SAVE_ENABLED = True


atexit.register(_save_state)  # Guarantee save on ANY process exit


# â”€â”€ Data helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _load_curve(wallet: str) -> pd.Series:
    wallet = str(wallet).strip().lower()
    if wallet in _curve_cache:
        return _curve_cache[wallet]
    p = CURVE_PATH / f"{wallet}.csv"
    if not p.exists():
        _curve_cache[wallet] = pd.Series(dtype=float)
        return _curve_cache[wallet]
    try:
        df = pd.read_csv(p)
        df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
        df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
        df = df.dropna()
        if df.empty:
            _curve_cache[wallet] = pd.Series(dtype=float)
            return _curve_cache[wallet]
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        _curve_cache[wallet] = df.set_index("ts")["equity"].sort_index()
        return _curve_cache[wallet]
    except Exception:
        _curve_cache[wallet] = pd.Series(dtype=float)
        return _curve_cache[wallet]


def _mtm_period_for_days(days: str) -> str:
    if str(days) == "7":
        return "week"
    if str(days) == "30":
        return "month"
    return "allTime"


def _mtm_period_candidates(days: str) -> list[str]:
    period = _mtm_period_for_days(days)
    if period == "week":
        return ["week"]
    if period == "month":
        return ["month", "week"]
    return ["allTime", "month", "week"]


def _load_mtm_account_value_series(wallet: str, days: str) -> pd.Series:
    wallet = str(wallet).strip().lower()
    period = _mtm_period_for_days(days)
    period_candidates = tuple(_mtm_period_candidates(days))
    key = (wallet, period, period_candidates)
    if key in _mtm_curve_cache:
        return _mtm_curve_cache[key]
    p = MTM_CACHE_DIR / f"{wallet}.json"
    if not p.exists():
        _mtm_curve_cache[key] = pd.Series(dtype=float)
        return _mtm_curve_cache[key]
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        periods = {x[0]: x[1] for x in data if isinstance(x, list) and len(x) == 2}
        rows = []
        for candidate in period_candidates:
            blob = periods.get(candidate, {}) or {}
            points = blob.get("accountValueHistory") or []
            for point in points:
                if isinstance(point, list) and len(point) >= 2:
                    ts = pd.to_datetime(float(point[0]), unit="ms", utc=True)
                    rows.append((ts, float(point[1])))
        if not rows:
            _mtm_curve_cache[key] = pd.Series(dtype=float)
            return _mtm_curve_cache[key]
        s = pd.Series({ts: val for ts, val in rows}).sort_index()
        _mtm_curve_cache[key] = s
        return s
    except Exception:
        _mtm_curve_cache[key] = pd.Series(dtype=float)
        return _mtm_curve_cache[key]


def _curve_metrics(s: pd.Series) -> dict[str, float]:
    nan = float("nan")
    if s.empty:
        return {"pnl": nan, "dd": nan, "peak": nan}
    running_max = s.cummax()
    return {
        "pnl": float(s.iloc[-1]),
        "dd": float((s - running_max).min()),
        "peak": float(running_max.max()),
    }


def _period_dd_pct(dd: float, peak: float, pnl: float, normalize: bool, risk_per_wallet: float) -> float:
    if dd is None or pd.isna(dd):
        return float("nan")
    dd_abs = abs(float(dd))
    if dd_abs < 1e-12:
        return 0.0
    if normalize and risk_per_wallet > 1e-12:
        return dd_abs / risk_per_wallet * 100.0
    denom = abs(float(peak)) if peak is not None and pd.notna(peak) else float("nan")
    if pd.isna(denom) or denom <= 1e-12:
        pnl_abs = abs(float(pnl)) if pnl is not None and pd.notna(pnl) else 0.0
        denom = pnl_abs + dd_abs
    return dd_abs / denom * 100.0 if denom > 1e-12 else float("nan")


def _slice_rebased_series(s: pd.Series, start_ts: "pd.Timestamp | None", end_ts: "pd.Timestamp | None") -> pd.Series:
    if s.empty:
        return s
    baseline = 0.0
    if start_ts is not None:
        prior = s[s.index < start_ts].tail(1)
        if not prior.empty:
            baseline = float(prior.iloc[-1])
        s = s[s.index >= start_ts]
    if end_ts is not None:
        s = s[s.index <= end_ts]
    if s.empty and start_ts is not None:
        return pd.Series([0.0], index=pd.DatetimeIndex([start_ts]))
    out = s - baseline
    if start_ts is not None and start_ts not in out.index:
        out = pd.concat([pd.Series([0.0], index=pd.DatetimeIndex([start_ts])), out]).sort_index()
    return out


def _rebase_series_for_days(s: pd.Series, days: str) -> pd.Series:
    if s.empty:
        return s
    if days not in ("all", "none"):
        try:
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=int(days))
        except (ValueError, TypeError):
            cutoff = None
        if cutoff is not None:
            prior = s[s.index < cutoff].tail(1)
            in_window = s[s.index >= cutoff]
            baseline = float(prior.iloc[-1]) if not prior.empty else 0.0
            if in_window.empty:
                return pd.Series([0.0], index=pd.DatetimeIndex([cutoff]))
            out = in_window - baseline
            if cutoff not in out.index:
                out = pd.concat([pd.Series([0.0], index=pd.DatetimeIndex([cutoff])), out]).sort_index()
            return out
    base_ts = s.index[0] - pd.Timedelta(milliseconds=1)
    if base_ts not in s.index:
        s = pd.concat([pd.Series([0.0], index=pd.DatetimeIndex([base_ts])), s]).sort_index()
    return s


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator is None or pd.isna(denominator) or abs(float(denominator)) < 1e-12:
        return float("nan")
    return float(numerator) / float(denominator)


def _num(value, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _find_all_trades_path() -> Path | None:
    for path in ALL_TRADES_CANDIDATES:
        if path.exists() and path.stat().st_size > 0:
            return path
    return None


def _load_trade_stats() -> dict[str, dict[str, float]]:
    path = _find_all_trades_path()
    if path is None:
        _state["trade_stats"] = {}
        _state["trade_stats_key"] = None
        return {}
    cache_key = (str(path), path.stat().st_mtime_ns, path.stat().st_size)
    if _state.get("trade_stats_key") == cache_key:
        return _state.get("trade_stats", {})
    agg: dict[str, dict[str, float]] = {}
    read_kwargs = {
        "usecols": ["wallet", "time", "px", "sz", "closedPnl"],
        "dtype": {"wallet": "string"},
        "chunksize": 250_000,
        "compression": "zip" if path.suffix.lower() == ".zip" else None,
        "low_memory": False,
    }
    for chunk in pd.read_csv(path, **read_kwargs):
        chunk["wallet"] = chunk["wallet"].astype("string").str.strip().str.lower()
        chunk["time"] = pd.to_numeric(chunk["time"], errors="coerce")
        chunk["px"] = pd.to_numeric(chunk["px"], errors="coerce")
        chunk["sz"] = pd.to_numeric(chunk["sz"], errors="coerce")
        chunk["closedPnl"] = pd.to_numeric(chunk["closedPnl"], errors="coerce")
        chunk = chunk.dropna(subset=["wallet", "px", "sz"])
        if chunk.empty:
            continue
        chunk["notional"] = chunk["px"].abs() * chunk["sz"].abs()
        grouped = chunk.groupby("wallet", dropna=False).agg(
            total_notional=("notional", "sum"),
            trades=("wallet", "size"),
            realised_pnl_trades=("closedPnl", "sum"),
            last_trade_time=("time", "max"),
        )
        for wallet, row in grouped.iterrows():
            wallet = str(wallet)
            entry = agg.setdefault(wallet, {"total_notional": 0.0, "trades": 0.0, "realised_pnl_trades": 0.0, "last_trade_time": float("nan")})
            entry["total_notional"] += float(row["total_notional"])
            entry["trades"] += float(row["trades"])
            entry["realised_pnl_trades"] += float(row["realised_pnl_trades"])
            if pd.notna(row["last_trade_time"]):
                prior = entry.get("last_trade_time", float("nan"))
                entry["last_trade_time"] = max(float(prior), float(row["last_trade_time"])) if pd.notna(prior) else float(row["last_trade_time"])
    _state["trade_stats"] = agg
    _state["trade_stats_key"] = cache_key
    return agg


def _load_trade_stats_for_wallets(wallets: list[str], start_ts: "pd.Timestamp | None", end_ts: "pd.Timestamp | None") -> dict[str, dict[str, float]]:
    path = _find_all_trades_path()
    wanted = {str(w).strip().lower() for w in wallets if str(w).strip()}
    if path is None or not wanted:
        return {}
    start_ms = int(start_ts.timestamp() * 1000) if start_ts is not None else None
    end_ms = int(end_ts.timestamp() * 1000) if end_ts is not None else None
    cache_key = (str(path), path.stat().st_mtime_ns, path.stat().st_size, tuple(sorted(wanted)), start_ms, end_ms)
    period_cache = _state.setdefault("period_trade_stats_cache", {})
    if cache_key in period_cache:
        return period_cache[cache_key]

    agg: dict[str, dict[str, float]] = {w: {"trades": 0.0, "total_notional": 0.0, "realised_pnl": 0.0} for w in wanted}
    read_kwargs = {
        "usecols": ["wallet", "time", "px", "sz", "closedPnl"],
        "dtype": {"wallet": "string"},
        "chunksize": 250_000,
        "compression": "zip" if path.suffix.lower() == ".zip" else None,
        "low_memory": False,
    }
    for chunk in pd.read_csv(path, **read_kwargs):
        chunk["wallet"] = chunk["wallet"].astype("string").str.strip().str.lower()
        chunk = chunk[chunk["wallet"].isin(wanted)]
        if chunk.empty:
            continue
        chunk["time"] = pd.to_numeric(chunk["time"], errors="coerce")
        if start_ms is not None:
            chunk = chunk[chunk["time"] >= start_ms]
        if end_ms is not None:
            chunk = chunk[chunk["time"] <= end_ms]
        if chunk.empty:
            continue
        chunk["px"] = pd.to_numeric(chunk["px"], errors="coerce")
        chunk["sz"] = pd.to_numeric(chunk["sz"], errors="coerce")
        chunk["closedPnl"] = pd.to_numeric(chunk["closedPnl"], errors="coerce").fillna(0.0)
        chunk = chunk.dropna(subset=["wallet", "px", "sz"])
        if chunk.empty:
            continue
        chunk["notional"] = chunk["px"].abs() * chunk["sz"].abs()
        grouped = chunk.groupby("wallet", dropna=False).agg(
            trades=("wallet", "size"),
            total_notional=("notional", "sum"),
            realised_pnl=("closedPnl", "sum"),
        )
        for wallet, row in grouped.iterrows():
            entry = agg.setdefault(str(wallet), {"trades": 0.0, "total_notional": 0.0, "realised_pnl": 0.0})
            entry["trades"] += float(row["trades"])
            entry["total_notional"] += float(row["total_notional"])
            entry["realised_pnl"] += float(row["realised_pnl"])
    if len(period_cache) > 24:
        period_cache.clear()
    period_cache[cache_key] = agg
    return agg


def _trade_replay_file_identity(path: Path) -> tuple[str, int, int]:
    stat_result = path.stat()
    return (
        str(path.resolve()),
        int(getattr(stat_result, "st_dev", 0)),
        int(getattr(stat_result, "st_ino", 0)),
    )


def _replay_snapshot_coordinates(path: Path) -> tuple[int, int] | None:
    parts = path.stem.split("_")
    if len(parts) < 3 or parts[0] != "all":
        return None
    try:
        return int(parts[-2]), int(parts[-1])
    except ValueError:
        return None


def _install_replay_payload(payload: dict) -> None:
    for wallet, item in payload.items():
        try:
            idx = pd.to_datetime(item["ts"], unit="ms", utc=True)
            _trade_replay_series_cache[str(wallet)] = {
                "raw": pd.Series(item["raw"], index=idx).sort_index(),
                "notional": pd.Series(item["notional"], index=idx).sort_index(),
            }
        except Exception:
            continue


def _load_best_replay_snapshot(path: Path, wanted: set[str]) -> bool:
    """Load the newest completed exact snapshot covering the requested wallets."""
    current_size = path.stat().st_size
    candidates: list[tuple[int, int, int, Path]] = []
    for candidate in TRADE_REPLAY_CACHE_DIR.glob("all_*.pkl"):
        coordinates = _replay_snapshot_coordinates(candidate)
        physical_size = candidate.stat().st_size
        if coordinates is None or physical_size <= 0:
            continue
        source_mtime_ns, source_size = coordinates
        if source_size <= current_size:
            candidates.append((physical_size, source_size, source_mtime_ns, candidate))
    large_candidates = [item for item in candidates if item[0] >= 1_000_000]
    candidates = large_candidates or candidates
    candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)

    best: tuple[int, int, Path, dict] | None = None
    best_coverage = -1
    for _, source_size, source_mtime_ns, candidate in candidates[:24]:
        try:
            with candidate.open("rb") as fh:
                payload = pickle.load(fh)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        coverage = len(wanted.intersection(str(wallet) for wallet in payload))
        if coverage > best_coverage:
            best = (source_size, source_mtime_ns, candidate, payload)
            best_coverage = coverage
        if coverage == len(wanted):
            break
    if best is None:
        return False

    source_size, source_mtime_ns, candidate, payload = best
    _install_replay_payload(payload)
    covered = len(wanted.intersection(_trade_replay_series_cache))
    _state["trade_replay_snapshot"] = {
        "path": str(candidate),
        "source_size": source_size,
        "source_mtime_ns": source_mtime_ns,
        "wallets": len(_trade_replay_series_cache),
        "requested_wallets": len(wanted),
        "covered_wallets": covered,
        "exact": covered == len(wanted),
    }
    log.info(
        "Loaded replay snapshot %s (%d/%d requested wallets, lag %d bytes)",
        candidate.name,
        covered,
        len(wanted),
        max(0, current_size - source_size),
    )
    return True


def _ensure_trade_replay_series(wallets: "set[str] | list[str] | tuple[str, ...]") -> None:
    """Load a completed exact replay checkpoint without rebuilding in a request."""
    global _trade_replay_series_key
    path = _find_all_trades_path()
    wanted = {str(w).strip().lower() for w in wallets if str(w).strip()}
    if path is None or not wanted:
        return
    cache_key = _trade_replay_file_identity(path)
    if _trade_replay_series_key != cache_key:
        _trade_replay_series_cache.clear()
        _trade_replay_series_key = cache_key
        _state.pop("trade_replay_missing_wallets", None)
    missing = {w for w in wanted if w not in _trade_replay_series_cache}
    known_missing = set(_state.get("trade_replay_missing_wallets", []))
    if missing and missing.issubset(known_missing):
        return
    if not missing:
        return
    if missing:
        TRADE_REPLAY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _load_best_replay_snapshot(path, wanted)
        missing = {w for w in wanted if w not in _trade_replay_series_cache}
    if not missing:
        remaining_known_missing = known_missing.difference(wanted)
        if remaining_known_missing:
            _state["trade_replay_missing_wallets"] = sorted(remaining_known_missing)
        return

    _state["trade_replay_missing_wallets"] = sorted(known_missing.union(missing))
    snapshot = _state.setdefault("trade_replay_snapshot", {})
    snapshot["exact"] = False
    snapshot["requested_wallets"] = len(wanted)
    snapshot["covered_wallets"] = len(wanted) - len(missing)
    log.warning(
        "Replay snapshot missing %d/%d requested wallets; synchronous rebuild suppressed",
        len(missing),
        len(wanted),
    )


def _load_trade_replay_pnl_series(wallet: str, mode: str) -> pd.Series:
    wallet = str(wallet).strip().lower()
    if mode == "raw":
        return pd.Series(dtype=float)
    _ensure_trade_replay_series({wallet})
    replay = _trade_replay_series_cache.get(wallet)
    if not replay:
        return pd.Series(dtype=float)
    raw = replay.get("raw", pd.Series(dtype=float))
    notional = replay.get("notional", pd.Series(dtype=float))
    if raw.empty or notional.empty:
        return pd.Series(dtype=float)
    rate = float(_MODE_RATE.get(mode, 0.0))
    return (raw - notional.reindex(raw.index).ffill().fillna(0.0) * rate).sort_index()


def _load_trade_replay_raw_series(wallet: str) -> pd.Series:
    wallet = str(wallet).strip().lower()
    _ensure_trade_replay_series({wallet})
    replay = _trade_replay_series_cache.get(wallet)
    if not replay:
        return pd.Series(dtype=float)
    return replay.get("raw", pd.Series(dtype=float))


def _load_last_trade_times(baseline: dict[str, float] | None = None) -> dict[str, float]:
    """Return latest wallet timestamps from all_trades.csv, plus a resumable cursor.

    On the first call (no cursor) the entire file is scanned via pandas to
    establish a complete baseline.  Subsequent calls use the fast incremental
    tail-read in last_trade_index.update_last_trade_times.
    """
    baseline = dict(baseline or {})
    path = _find_all_trades_path()
    if path is None:
        _state.pop("last_trade_cursor", None)
        _state["last_trade_times"] = baseline
        return baseline
    if path.suffix.lower() != ".csv":
        log.warning("Last-trade overlay skipped for non-CSV ledger: %s", path)
        return baseline

    # ── Decide: full scan (no cursor) or incremental tail-read ──────
    cursor = _state.get("last_trade_cursor")
    can_resume = False
    if cursor and isinstance(cursor, dict) and isinstance(cursor.get("latest"), dict):
        try:
            stat = path.stat()
            file_id = (
                str(path.resolve()),
                int(getattr(stat, "st_dev", 0)),
                int(getattr(stat, "st_ino", 0)),
            )
            prev_id = tuple(cursor.get("identity", ()))
            prev_offset = int(cursor.get("offset", -1))
            can_resume = prev_id == file_id and 0 <= prev_offset <= stat.st_size
        except (OSError, TypeError, ValueError):
            can_resume = False

    if not can_resume:
        # No valid cursor — full pandas scan (same as pre-8012 refactor)
        latest = dict(baseline)
        read_kwargs = {
            "usecols": ["wallet", "time"],
            "dtype": {"wallet": "string"},
            "chunksize": 500_000,
            "low_memory": False,
        }
        for chunk in pd.read_csv(path, **read_kwargs):
            chunk["wallet"] = chunk["wallet"].astype("string").str.strip().str.lower()
            chunk["time"] = pd.to_numeric(chunk["time"], errors="coerce")
            chunk = chunk.dropna(subset=["wallet", "time"])
            if chunk.empty:
                continue
            grouped = chunk.groupby("wallet", dropna=False)["time"].max()
            for wallet, ts in grouped.items():
                wallet = str(wallet)
                ts = float(ts)
                latest[wallet] = max(ts, latest.get(wallet, -1.0))
        try:
            stat = path.stat()
            cursor = {
                "identity": (
                    str(path.resolve()),
                    int(getattr(stat, "st_dev", 0)),
                    int(getattr(stat, "st_ino", 0)),
                ),
                "offset": stat.st_size,
                "latest": latest,
            }
        except OSError:
            cursor = None
    else:
        # Valid cursor — fast incremental tail-read
        try:
            latest, cursor = update_last_trade_times(
                path,
                baseline,
                cursor,
                initial_tail_bytes=DEFAULT_INITIAL_TAIL_BYTES,
            )
        except (OSError, ValueError) as exc:
            log.warning("Last-trade overlay refresh failed: %s", exc)
            return baseline

    _state["last_trade_times"] = latest
    _state["last_trade_cursor"] = cursor
    return latest


def _overlay_last_trade_times(df: pd.DataFrame) -> pd.DataFrame:
    if not ENABLE_LAST_TRADE_TIME_OVERLAY or df.empty or "wallet" not in df.columns:
        return df
    baseline = {
        str(wallet).strip().lower(): float(timestamp)
        for wallet, timestamp in zip(df["wallet"], df["last_trade_time"])
        if pd.notna(timestamp)
    }
    latest = _load_last_trade_times(baseline)
    if not latest:
        return df
    result = df.copy()
    overlay = result["wallet"].map(latest)
    lhs = pd.to_numeric(result["last_trade_time"], errors="coerce")
    rhs = pd.to_numeric(overlay, errors="coerce")
    result["last_trade_time"] = pd.concat([lhs, rhs], axis=1).max(axis=1, skipna=True)
    return result


def _load_summary_kpis() -> pd.DataFrame:
    """Load KPI columns from summary.csv for compound scoring.

    These columns come from the Stage 2+3 scanner and contain the detailed
    trade-by-trade statistics and MTM truth that the compound score needs.
    Soft-fails to empty DataFrame if summary.csv is missing or corrupted.
    """
    needed = {
        "wallet",
        # MTM truth (40% weight)
        "mtm_calmar", "max_drawdown_mtm", "max_drawdown",
        "month_acctV_start", "month_acctV_end", "month_acctV_peak",
        "month_pnl_chg_mtm", "allTime_max_drawdown_mtm", "allTime_pnl_chg_mtm", "allTime_acctV_peak",
        # PnL stability (15% weight)
        "pnl_stability_score", "consistency_score",
        # Fee resilience (15% weight)
        "total_pnl", "total_pnl_net_fees_est", "est_fee_drag",
        # Recency (10% weight)
        "trades_7d", "trades",
        # Hard penalty flags
        "martingale_flag", "one_big_trade_flag", "equity_collapse_flag",
        "equity_collapse_flag_mtm", "negative_total_flag", "negative_total_flag_mtm",
        "suspected_truncated", "termination_reason",
        # Display extras
        "win_rate", "avg_pnl", "profit_factor", "largest_win_ratio",
        "edge_score_raw", "timespan_hours", "symbol_count",
    }
    if not SUMMARY_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(SUMMARY_PATH, usecols=lambda c: c in needed, dtype={"wallet": str}, low_memory=False)
        if df.empty:
            return pd.DataFrame()
        df["wallet"] = df["wallet"].fillna("").astype(str).str.strip().str.lower()
        df = df[df["wallet"] != ""]
        # Convert numeric columns (excluding wallet) to float
        for col in (set(needed) - {"wallet"}) & set(df.columns):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def _overlay_cached_mtm(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing MTM columns from cached Hyperliquid portfolio snapshots only."""
    if df.empty or "wallet" not in df.columns or not MTM_CACHE_DIR.exists():
        return df
    try:
        from hl_mtm_lookup import MTM_OUTPUT_COLUMNS, _summarise
    except Exception:
        return df

    for col in MTM_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col == "mtm_source" else float("nan")

    needs_mtm = (
        pd.to_numeric(df.get("max_drawdown_mtm"), errors="coerce").isna()
        & pd.to_numeric(df.get("allTime_max_drawdown_mtm"), errors="coerce").isna()
    )
    for idx, wallet in df.loc[needs_mtm, "wallet"].items():
        w = str(wallet).strip().lower()
        if not (len(w) == 42 and w.startswith("0x")):
            continue
        cache_path = MTM_CACHE_DIR / f"{w}.json"
        if not cache_path.exists():
            continue
        try:
            mtm = _summarise(json.loads(cache_path.read_text(encoding="utf-8")))
        except Exception:
            continue
        if mtm.get("max_drawdown_mtm") is None and mtm.get("allTime_max_drawdown_mtm") is None:
            continue
        for col in MTM_OUTPUT_COLUMNS:
            val = mtm.get(col)
            cur = df.at[idx, col]
            is_blank = pd.isna(cur) if col != "mtm_source" else not str(cur or "").strip()
            if is_blank and val is not None:
                if col != "mtm_source":
                    val = _num(val, float("nan"))
                df.at[idx, col] = val
    return df


def _compute_compound_score(r) -> float:
    """Compute compound risk-adjusted copy score (0-100 scale).

    Weights are empirically chosen to surface elite steady earners:
      - MTM Calmar (40%): Truth from accountValueHistory. >=2 is healthy.
      - PnL stability (15%): Consistency of chunk-level win rates over time.
      - Fee resilience (15%): Net PnL / Gross PnL â€” does profit survive fees?
      - Recency (10%): Fraction of trades in last 7 days.
      - Penalty flags subtract directly: martingale -25, one_big_trade -50,
        equity_collapse -30, equity_collapse_mtm -30.
    """
    weights = [0.40, 0.15, 0.15, 0.10]
    penalties = 0.0

    # â”€â”€ 1. MTM Calmar (40%) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    calmar = _num(r.get("mtm_calmar"))
    if calmar > 0 and not math.isnan(calmar):
        # Sigmoid-like: 0â†’0, 1â†’0.50, 2â†’0.75, 3â†’0.88, 5â†’0.97
        component = 1.0 - math.exp(-calmar * 0.7)
    else:
        # Fallback: use MTM DD if available, otherwise full-history scanner DD.
        # The UI curve can be windowed/resampled and is not safe for scoring.
        pnl = _num(r.get("realised_pnl"))
        mtm_dd = _num(r.get("max_dd_mtm"))
        scanner_dd = abs(_num(r.get("max_dd")))
        dd = abs(mtm_dd) if mtm_dd < 0 and abs(mtm_dd) > 1.0 else scanner_dd
        if dd > 1.0 and pnl > 0:
            component = min(1.0, (pnl / dd) / 5.0)
        else:
            component = 0.0

    # â”€â”€ 2. PnL stability (15%) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    cons = _num(r.get("consistency_score"))
    pnl_stab = _num(r.get("pnl_stability_score"))
    if not math.isnan(cons) and cons > 0:
        stab_component = cons  # already [0,1]
    elif not math.isnan(pnl_stab) and pnl_stab > 0:
        stab_component = min(1.0, pnl_stab / 5.0)
    else:
        stab_component = 0.0

    # â”€â”€ 3. Fee resilience (15%) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    gross_pnl = _num(r.get("total_pnl"))
    net_pnl = _num(r.get("total_pnl_net_fees_est"))
    est_fee = _num(r.get("est_fee_drag"))
    if gross_pnl > 1.0 and not math.isnan(net_pnl):
        ratio = max(0.0, min(1.0, net_pnl / gross_pnl))
        fee_component = ratio
    elif est_fee > 0 and gross_pnl > 0:
        fee_component = max(0.0, min(1.0, (gross_pnl - est_fee) / gross_pnl))
    else:
        fee_component = 0.5  # neutral â€” no fee data

    # â”€â”€ 4. Recency (10%) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    trades = max(1, int(_num(r.get("trades"))))
    trades_7d = min(trades, int(_num(r.get("trades_7d"))))
    recency_raw = trades_7d / trades
    recency_component = min(1.0, recency_raw * 2.0)

    # â”€â”€ Weighted sum â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    scores = [component, stab_component, fee_component, recency_component]
    total_weight = sum(weights)
    weighted = sum(s * w for s, w in zip(scores, weights)) / total_weight if total_weight > 0 else 0.0

    # â”€â”€ Hard penalty flags â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if _martingale_flag_reliable() and int(_num(r.get("martingale_flag"))) == 1:
        penalties -= 0.25
    if int(_num(r.get("one_big_trade_flag"))) == 1:
        penalties -= 0.50
    if int(_num(r.get("equity_collapse_flag"))) == 1:
        penalties -= 0.30
    if int(_num(r.get("equity_collapse_flag_mtm"))) == 1:
        penalties -= 0.30

    final = weighted + penalties
    return round(max(0.0, final) * 100.0, 2)


_series_cache: dict = {}
_curve_cache: dict = {}
_mtm_curve_cache: dict = {}
_portfolio_cache: dict = {}
_trade_replay_series_cache: dict[str, dict[str, pd.Series]] = {}
_trade_replay_series_key: tuple | None = None


def _valid_universe_wallets() -> set[str]:
    uni = _state.get("universe")
    if uni is None or not isinstance(uni, pd.DataFrame) or uni.empty or "wallet" not in uni.columns:
        return set()
    return set(uni["wallet"].dropna().astype(str).str.strip().str.lower())


def _prune_selection_to_universe() -> bool:
    valid = _valid_universe_wallets()
    if not valid:
        return False
    selected = _state.get("selected", {}) or {}
    old_true = {str(w).strip().lower() for w, v in selected.items() if v}
    pruned_selected = {w: bool(selected.get(w, False)) for w in valid}
    pruned_order = [str(w).strip().lower() for w in _state.get("wallet_order", []) if str(w).strip().lower() in valid and pruned_selected.get(str(w).strip().lower(), False)]
    weights = _state.get("wallet_weights", {}) or {}
    pruned_weights = {w: float(weights.get(w, 1.0)) for w in pruned_order}
    changed = old_true != {w for w, v in pruned_selected.items() if v} or _state.get("wallet_order", []) != pruned_order
    _state["selected"] = pruned_selected
    _state["wallet_order"] = pruned_order
    _state["wallet_weights"] = pruned_weights
    if changed:
        _state["_selection_dirty"] = True
    return changed


def _load_purged_wallets() -> set:
    """Return set of purged wallet addresses (lowercase) from purged_wallets.txt."""
    purged = set()
    for path in (PURGED_WALLETS_FILE, DATA_PURGED_WALLETS_FILE):
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                w = line.strip().lower()
                if w and len(w) == 42 and w.startswith("0x"):
                    purged.add(w)
    return purged


def _save_purged_wallets(wallets: set[str]) -> None:
    valid = sorted(w for w in wallets if isinstance(w, str) and len(w) == 42 and w.startswith("0x"))
    body = "".join(f"{w}\n" for w in valid)
    for path in (PURGED_WALLETS_FILE, DATA_PURGED_WALLETS_FILE):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
        tmp.write_text(body, encoding="utf-8")
        os.replace(tmp, path)


def _purge_losing_wallets_from_df(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Count losing proof rows without mutating purge files or hiding wallets."""
    if df.empty or "wallet" not in df.columns:
        return df, 0
    losing = pd.Series(False, index=df.index)
    if "total_pnl" in df.columns:
        total_pnl = pd.to_numeric(df["total_pnl"], errors="coerce")
        losing |= total_pnl.notna() & (total_pnl <= 0)
    if "allTime_pnl_chg_mtm" in df.columns:
        alltime_mtm = pd.to_numeric(df["allTime_pnl_chg_mtm"], errors="coerce")
        losing |= alltime_mtm.notna() & (alltime_mtm <= 0)
    for col in ("negative_total_flag", "negative_total_flag_mtm"):
        if col in df.columns:
            losing |= pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int).eq(1)
    wallets = set(df.loc[losing, "wallet"].dropna().astype(str).str.strip().str.lower())
    wallets = {w for w in wallets if len(w) == 42 and w.startswith("0x")}
    return df, len(wallets)


def load_universe(preserve_selection: bool = True) -> None:
    global _martingale_flag_reliable_cache
    _martingale_flag_reliable_cache = None
    _series_cache.clear()
    _curve_cache.clear()
    _mtm_curve_cache.clear()
    _portfolio_cache.clear()
    if not UNIVERSE_PATH.exists() or UNIVERSE_PATH.stat().st_size == 0:
        _state["universe"] = pd.DataFrame()
        return
    df = pd.read_csv(UNIVERSE_PATH)
    if df.empty:
        _state["universe"] = pd.DataFrame()
        return
    df["wallet"] = df["wallet"].astype(str).str.strip().str.lower()
    # Drop purged wallets (safety net â€” universe_builder should already exclude them)
    purged = _load_purged_wallets()
    if purged:
        df = df[~df["wallet"].isin(purged)]
    # Drop losing wallets (negative or zero realised PnL)
    if "realised_pnl" in df.columns:
        df = df[df["realised_pnl"] > 0]
    numeric_cols = ("realised_pnl", "total_notional", "efficiency", "trades", "trades_7d", "last_trade_time", "score")
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = float("nan")

    # Merge summary KPI columns for compound scoring
    summary_kpis = _load_summary_kpis()
    _state["summary"] = summary_kpis
    if not summary_kpis.empty:
        df = df.merge(summary_kpis, on="wallet", how="left", suffixes=("", "_sum"))
        for dup_col in ["trades_7d", "trades"]:
            if f"{dup_col}_sum" in df.columns:
                df[dup_col] = df[f"{dup_col}_sum"].combine_first(df[dup_col])
                df = df.drop(columns=[f"{dup_col}_sum"])
        df, losing_proof_rows = _purge_losing_wallets_from_df(df)
        _state["last_losing_proof_rows"] = losing_proof_rows
    df = _overlay_cached_mtm(df)

    # Enrich from all_trades. wallet_universe.csv can lag behind a long
    # universe_builder cycle, so last_trade_time is overlaid from this fresher
    # append-only source whenever available.
    needs_trade_stats = (
        ENABLE_ALL_TRADES_TABLE_ENRICH
        and (
            "total_notional" not in df.columns
            or "trades" not in df.columns
            or df["total_notional"].isna().any()
            or df["trades"].isna().any()
            or "last_trade_time" not in df.columns
            or df["last_trade_time"].isna().any()
            or (
                _find_all_trades_path() is not None
                and _find_all_trades_path().stat().st_mtime_ns > UNIVERSE_PATH.stat().st_mtime_ns
            )
        )
    )
    trade_stats = _load_trade_stats() if needs_trade_stats else {}
    if trade_stats:
        trade_df = pd.DataFrame.from_dict(trade_stats, orient="index")
        trade_df.index.name = "wallet"
        trade_df = trade_df.reset_index()
        df = df.merge(trade_df, on="wallet", how="left", suffixes=("", "_trade"))
        if "total_notional_trade" in df.columns:
            df["total_notional"] = df["total_notional_trade"].combine_first(df["total_notional"])
            df = df.drop(columns=["total_notional_trade"])
        if "trades_trade" in df.columns:
            df["trades"] = df["trades_trade"].combine_first(df["trades"])
            df = df.drop(columns=["trades_trade"])
        if "last_trade_time_trade" in df.columns:
            lhs = pd.to_numeric(df["last_trade_time"], errors="coerce")
            rhs = pd.to_numeric(df["last_trade_time_trade"], errors="coerce")
            df["last_trade_time"] = pd.concat([lhs, rhs], axis=1).max(axis=1, skipna=True)
            df = df.drop(columns=["last_trade_time_trade"])

    df = _overlay_last_trade_times(df)

    # Compute curve-based DD stats only as a last-resort display fallback. It is
    # not safe for wallet selection because resampling/windowing can turn real
    # drawdowns into tiny or zero values.
    def _curve_dd_stats(wallet: str) -> tuple[float, float]:
        s = _load_curve(wallet)
        m = _curve_metrics(s)
        pct = (m["dd"] / abs(m["peak"]) * 100) if abs(m["peak"]) > 1e-12 else float("nan")
        return m["dd"], pct

    dd_stats = df["wallet"].apply(_curve_dd_stats)
    curve_max_dd = dd_stats.apply(lambda t: t[0])
    curve_max_dd_pct = dd_stats.apply(lambda t: t[1])

    # Drawdown truth hierarchy:
    # 1. MTM accountValueHistory DD, including unrealised PnL.
    # 2. Full-history scanner replay DD from summary.csv max_drawdown.
    # 3. UI curve DD only if no stronger source exists, with unknown DD%.
    has_mtm_dd = "max_drawdown_mtm" in df.columns
    has_mtm_start = "month_acctV_start" in df.columns
    if "max_drawdown" in df.columns:
        df["max_dd_scanner"] = pd.to_numeric(df["max_drawdown"], errors="coerce")
    else:
        df["max_dd_scanner"] = float("nan")
    scanner_valid = df["max_dd_scanner"].notna() & (df["max_dd_scanner"] < 0)
    scanner_pnl_ref = (
        pd.to_numeric(df["total_pnl"], errors="coerce")
        if "total_pnl" in df.columns else pd.to_numeric(df["realised_pnl"], errors="coerce")
    )
    scanner_ref = scanner_pnl_ref.abs() + df["max_dd_scanner"].abs()
    df["max_dd_scanner_pct"] = (df["max_dd_scanner"].abs() / scanner_ref * 100).where(
        scanner_valid & scanner_ref.notna() & (scanner_ref > 1),
        float("nan"),
    )

    if has_mtm_dd:
        # max_drawdown_mtm is negative account-value drawdown from Hyperliquid's
        # accountValueHistory. Use the exported account peak for a real DD% when
        # available; falling back to current_value - DD is only an approximation.
        df["max_dd_mtm"] = pd.to_numeric(df["max_drawdown_mtm"], errors="coerce")
        mtm_valid = df["max_dd_mtm"].notna() & (df["max_dd_mtm"] < 0)

        if "month_acctV_peak" in df.columns:
            df["mtm_peak_ref"] = pd.to_numeric(df["month_acctV_peak"], errors="coerce")
            df["max_dd_mtm_pct"] = df.apply(
                lambda r: min(100.0, (abs(r["max_dd_mtm"]) / r["mtm_peak_ref"] * 100))
                if pd.notna(r.get("max_dd_mtm")) and pd.notna(r.get("mtm_peak_ref")) and r.get("mtm_peak_ref", 0) > 1
                else float("nan"),
                axis=1,
            )
        elif "month_acctV_end" in df.columns:
            df["mtm_acctV_end"] = pd.to_numeric(df["month_acctV_end"], errors="coerce")
            # Approx peak = current value - DD (where DD is negative)
            df["mtm_approx_peak"] = df["mtm_acctV_end"] - df["max_dd_mtm"]
            df["max_dd_mtm_pct"] = df.apply(
                lambda r: min(100.0, (abs(r["max_dd_mtm"]) / r["mtm_approx_peak"] * 100))
                if pd.notna(r.get("max_dd_mtm")) and pd.notna(r.get("mtm_approx_peak")) and r.get("mtm_approx_peak", 0) > 1
                else float("nan"),
                axis=1,
            )
        elif has_mtm_start:
            # Fall back to month-start (less accurate but better than nothing)
            df["mtm_acctV_ref"] = pd.to_numeric(df["month_acctV_start"], errors="coerce")
            df["max_dd_mtm_pct"] = df.apply(
                lambda r: min(100.0, (abs(r["max_dd_mtm"]) / r["mtm_acctV_ref"] * 100))
                if pd.notna(r.get("max_dd_mtm")) and pd.notna(r.get("mtm_acctV_ref")) and r.get("mtm_acctV_ref", 0) > 1
                else float("nan"),
                axis=1,
            )
        else:
            df["max_dd_mtm_pct"] = float("nan")

        if "allTime_max_drawdown_mtm" in df.columns:
            df["max_dd_alltime_mtm"] = pd.to_numeric(df["allTime_max_drawdown_mtm"], errors="coerce")
        else:
            df["max_dd_alltime_mtm"] = float("nan")
        if "allTime_acctV_peak" in df.columns:
            df["alltime_mtm_peak_ref"] = pd.to_numeric(df["allTime_acctV_peak"], errors="coerce")
            df["max_dd_alltime_mtm_pct"] = df.apply(
                lambda r: min(100.0, (abs(r["max_dd_alltime_mtm"]) / r["alltime_mtm_peak_ref"] * 100))
                if pd.notna(r.get("max_dd_alltime_mtm")) and pd.notna(r.get("alltime_mtm_peak_ref")) and r.get("alltime_mtm_peak_ref", 0) > 1
                else float("nan"),
                axis=1,
            )
        else:
            df["max_dd_alltime_mtm_pct"] = float("nan")

        # ── DD priority: MTM > scanner > curve ────────────────────
        # Use allTime MTM DD as the authoritative risk metric when available.
        # Fall back to monthly MTM, then scanner, then curve.  This ensures
        # the dashboard NEVER shows closedPnL-only DD as the primary risk
        # figure when accountValueHistory data exists.
        mtm_alltime_valid = df["max_dd_alltime_mtm"].notna() & (df["max_dd_alltime_mtm"] < 0)
        df["max_dd"] = float("nan")
        df["max_dd_pct"] = float("nan")
        df["max_dd_source"] = "curve_untrusted"
        # Tier 1: allTime MTM (most complete history)
        mask_at = mtm_alltime_valid
        df.loc[mask_at, "max_dd"] = df.loc[mask_at, "max_dd_alltime_mtm"]
        df.loc[mask_at, "max_dd_pct"] = df.loc[mask_at, "max_dd_alltime_mtm_pct"] if "max_dd_alltime_mtm_pct" in df.columns else float("nan")
        df.loc[mask_at, "max_dd_source"] = "mtm_alltime"
        # Tier 2: monthly MTM (fills gaps where allTime unavailable)
        mask_m = (~mask_at) & mtm_valid
        df.loc[mask_m, "max_dd"] = df.loc[mask_m, "max_dd_mtm"]
        df.loc[mask_m, "max_dd_pct"] = df.loc[mask_m, "max_dd_mtm_pct"]
        df.loc[mask_m, "max_dd_source"] = "mtm_monthly"
        # Tier 3: scanner (realised-only fallback)
        mask_s = (~mask_at) & (~mask_m) & scanner_valid
        df.loc[mask_s, "max_dd"] = df.loc[mask_s, "max_dd_scanner"]
        df.loc[mask_s, "max_dd_pct"] = df.loc[mask_s, "max_dd_scanner_pct"]
        df.loc[mask_s, "max_dd_source"] = "scanner_realised"
        # Tier 4: curve (last resort, untrusted)
        mask_c = (~mask_at) & (~mask_m) & (~mask_s)
        df.loc[mask_c, "max_dd"] = curve_max_dd[mask_c]
        df.loc[mask_c, "max_dd_source"] = "curve_untrusted"
    else:
        df["max_dd"] = df["max_dd_scanner"].where(scanner_valid, curve_max_dd)
        df["max_dd_pct"] = df["max_dd_scanner_pct"].where(scanner_valid, float("nan"))
        df["max_dd_source"] = "curve_untrusted"
        df.loc[scanner_valid, "max_dd_source"] = "scanner_realised"
        df["max_dd_mtm"] = float("nan")
        df["max_dd_mtm_pct"] = float("nan")
    realised_eval_pnl = (
        pd.to_numeric(df["total_pnl"], errors="coerce")
        if "total_pnl" in df.columns else pd.to_numeric(df["realised_pnl"], errors="coerce")
    )
    realised_eval_pnl = realised_eval_pnl.combine_first(pd.to_numeric(df["realised_pnl"], errors="coerce"))
    df["realised_eval_pnl"] = realised_eval_pnl
    df["realised_eval_dd"] = df["max_dd_scanner"].where(scanner_valid, curve_max_dd)
    df["realised_eval_source"] = "curve_untrusted"
    df.loc[scanner_valid, "realised_eval_source"] = "scanner"
    realised_eval_ref = df["realised_eval_pnl"].abs() + df["realised_eval_dd"].abs()
    df["realised_eval_dd_pct"] = (df["realised_eval_dd"].abs() / realised_eval_ref * 100).where(
        df["realised_eval_dd"].notna() & (df["realised_eval_dd"] < 0) & realised_eval_ref.notna() & (realised_eval_ref > 1),
        float("nan"),
    )
    df["realised_pnl_dd"] = df.apply(
        lambda r: _safe_div(r.get("realised_eval_pnl"), abs(r.get("realised_eval_dd")))
        if r.get("realised_eval_source") == "scanner"
        and pd.notna(r.get("realised_eval_pnl")) and pd.notna(r.get("realised_eval_dd")) and abs(_num(r.get("realised_eval_dd"))) > 1.0
        and abs(_num(r.get("realised_eval_dd"))) >= abs(_num(r.get("realised_eval_pnl"))) * MIN_STABLE_DD_PNL_FRACTION
        else float("nan"),
        axis=1,
    )
    df["total_notional"] = pd.to_numeric(df["total_notional"], errors="coerce").fillna(0.0)
    df["realised_pnl"] = pd.to_numeric(df["realised_pnl"], errors="coerce")
    for col, rate in SLIPPAGE_RATES.items():
        df[col] = df["realised_pnl"] - (df["total_notional"] * rate)
    df["risk_pnl_basis"] = df["realised_eval_pnl"]
    df["risk_pnl"] = df.apply(
        lambda r: _safe_div(r.get("realised_eval_pnl"), abs(r.get("max_dd")))
        if pd.notna(r.get("realised_eval_pnl")) and pd.notna(r.get("max_dd")) and _num(r.get("max_dd")) < 0
        and abs(_num(r.get("max_dd"))) > 1.0
        and abs(_num(r.get("max_dd"))) >= abs(_num(r.get("realised_eval_pnl"))) * MIN_STABLE_DD_PNL_FRACTION
        else float("nan"),
        axis=1,
    )

    def _slip_deg_03(r):
        pnl = r.get("realised_pnl")
        slip = r.get("pnl_slip_30")
        if pnl is None or slip is None or pd.isna(pnl) or pd.isna(slip) or abs(float(pnl)) < 1e-12:
            return float("nan")
        return ((float(pnl) - float(slip)) / abs(float(pnl))) * 100

    df["slip_deg_03"] = df.apply(_slip_deg_03, axis=1)

    # Compound score replaces old copy_score
    df["compound_score"] = df.apply(_compute_compound_score, axis=1)

    # Sort by compound_score descending, then realised_pnl
    df = df.sort_values(["compound_score", "realised_pnl"], ascending=[False, False], na_position="last").reset_index(drop=True)
    df["rank"] = df.index + 1

    _state["universe"] = df
    prev = _state["selected"] if preserve_selection else {}
    if not prev:
        _state["selected"] = {w: False for w in df["wallet"]}
    else:
        new_selected = {str(w): bool(v) for w, v in prev.items()}
        for w in df["wallet"]:
            new_selected.setdefault(w, False)
        _state["selected"] = new_selected
    selected_set = {w for w, v in _state["selected"].items() if v}
    existing_order = _state.get("wallet_order", [])
    new_order = [w for w in existing_order if w in selected_set]
    order_set = set(new_order)
    for w in df["wallet"]:
        w = str(w)
        if w in selected_set and w not in order_set:
            new_order.append(w)
            order_set.add(w)
    _state["wallet_order"] = new_order
    weights = _state.setdefault("wallet_weights", {})
    for w in new_order:
        weights.setdefault(w, 1.0)
    for w in list(weights.keys()):
        if w not in selected_set:
            weights.pop(w, None)


# â”€â”€ Context builders â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def refresh_last_trade_overlay() -> None:
    """Update Last Trade without invalidating replay, curve, or portfolio caches."""
    universe = _state.get("universe")
    if universe is None or universe.empty:
        return
    _state["universe"] = _overlay_last_trade_times(universe)


def _fmt(val, spec: str, na: str = "\u2014") -> str:
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return na
        return spec.format(val)
    except Exception:
        return na


def _get_sort(sort: str | None, direction: str | None) -> tuple[str, str]:
    raw_sort = (sort or "").strip()
    if raw_sort in SORTABLE_COLUMNS:
        sort_key = SORTABLE_COLUMNS[raw_sort]
    elif raw_sort in SORTABLE_COLUMN_VALUES:
        sort_key = raw_sort
    else:
        sort_key = "compound_score"
    sort_dir = "asc" if (direction or "").strip().lower() == "asc" else "desc"
    _state["sort"] = raw_sort if raw_sort in SORTABLE_COLUMNS else "compound_score"  # Save user-facing key for template matching
    _state["dir"] = sort_dir
    return sort_key, sort_dir


def _sort_universe(df: pd.DataFrame, sort: str, direction: str) -> pd.DataFrame:
    sort_key = sort  # Already mapped — do NOT re-run _get_sort (aliases like table_pnl aren't in SORTABLE_COLUMNS)
    sort_dir = "asc" if (direction or "").strip().lower() == "asc" else "desc"
    if df.empty:
        return df
    ascending = sort_dir == "asc"
    if sort_key == "wallet":
        ordered = df.sort_values(["wallet", "compound_score"], ascending=[ascending, False], na_position="last", kind="mergesort")
    else:
        ordered = df.sort_values([sort_key, "compound_score", "wallet"], ascending=[ascending, False, True], na_position="last", kind="mergesort")
    ordered = ordered.reset_index(drop=True).copy()
    ordered["display_rank"] = ordered.index + 1
    return ordered


def _sort_universe_pinned(df: pd.DataFrame, sort: str, direction: str) -> pd.DataFrame:
    sort_key = sort  # Already mapped — do NOT re-run _get_sort (aliases like table_pnl aren't in SORTABLE_COLUMNS)
    sort_dir = "asc" if (direction or "").strip().lower() == "asc" else "desc"
    if df.empty:
        return df
    ascending = sort_dir == "asc"
    if sort_key == "wallet":
        ordered = df.sort_values(["_is_selected", "wallet", "compound_score"], ascending=[False, ascending, False], na_position="last", kind="mergesort")
    else:
        ordered = df.sort_values(["_is_selected", sort_key, "compound_score", "wallet"], ascending=[False, ascending, False, True], na_position="last", kind="mergesort")
    ordered = ordered.reset_index(drop=True).copy()
    ordered["display_rank"] = ordered.index + 1
    return ordered


def _build_sparkline(wallet: str) -> str:
    s = _load_curve(wallet)
    if s.empty:
        return ""
    pts = s.iloc[-50:]
    mn, mx = float(pts.min()), float(pts.max())
    rng = mx - mn
    if rng < 1e-12:
        return " ".join(f"{round(i * 160 / max(len(pts) - 1, 1), 2)},15" for i in range(len(pts)))
    result = []
    for i, v in enumerate(pts):
        x = round(i * 160 / max(len(pts) - 1, 1), 2)
        y = round(30 - ((float(v) - mn) / rng) * 28 - 1, 2)
        result.append(f"{x},{y}")
    return " ".join(result)


MS_DAY = 24 * 60 * 60 * 1000
ACTIVE_MAX_MS = 3 * MS_DAY
SLOW_MAX_MS = 7 * MS_DAY
STALE_MAX_MS = 15 * MS_DAY


def _last_trade_age_ms(last_trade_time) -> float:
    if last_trade_time is None or (isinstance(last_trade_time, float) and math.isnan(last_trade_time)):
        return float("inf")
    try:
        ts = float(last_trade_time)
        if ts < 1e9:
            return float("inf")
        if ts < 1e12:
            ts *= 1000
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return max(0.0, float(now_ms - int(ts)))
    except Exception:
        return float("inf")


def _activity_bucket(last_trade_time) -> str:
    age_ms = _last_trade_age_ms(last_trade_time)
    if age_ms <= ACTIVE_MAX_MS:
        return "active"
    if age_ms <= SLOW_MAX_MS:
        return "slow"
    if age_ms <= STALE_MAX_MS:
        return "stale"
    return "dormant"


def _is_dormant(last_trade_time) -> bool:
    """True if wallet has no trade for more than 15 days."""
    return _activity_bucket(last_trade_time) == "dormant"


def _is_slow(last_trade_time) -> bool:
    """True if wallet last traded 3-7 days ago."""
    return _activity_bucket(last_trade_time) == "slow"


def _is_stale(last_trade_time) -> bool:
    """True if wallet last traded 7-15 days ago."""
    return _activity_bucket(last_trade_time) == "stale"


def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    f = _state.get("filters", {})
    if not f and not _state.get("hide_dormant"):
        return df
    mask = pd.Series(True, index=df.index)
    if f.get("min_trades") is not None:
        trades_col = pd.to_numeric(df.get("trades"), errors="coerce")
        mask &= trades_col.fillna(0) >= f["min_trades"]
    if f.get("max_dd_pct") is not None:
        # Match the table's DD% column after mode/normalisation/risk are applied.
        dd_pct_col = df["table_dd_pct"] if "table_dd_pct" in df.columns else df.get("max_dd_pct", df.get("realised_eval_dd_pct"))
        dd_pct_col = pd.to_numeric(dd_pct_col, errors="coerce")
        mask &= dd_pct_col.notna() & (dd_pct_col.abs() <= f["max_dd_pct"])
    if f.get("max_slip_deg_03") is not None:
        slip_col = df["table_slip_pct"] if "table_slip_pct" in df.columns else df["slip_deg_03"]
        slip_col = pd.to_numeric(slip_col, errors="coerce")
        mask &= slip_col.fillna(float("inf")) <= f["max_slip_deg_03"]
    if f.get("min_score") is not None:
        score_col = df["table_compound_score"] if "table_compound_score" in df.columns else df["compound_score"]
        score_col = pd.to_numeric(score_col, errors="coerce")
        mask &= score_col.fillna(float("-inf")) >= f["min_score"]
    if f.get("min_pnl") is not None:
        pnl_col = df["table_pnl"] if "table_pnl" in df.columns else (df["realised_eval_pnl"] if "realised_eval_pnl" in df.columns else df["realised_pnl"])
        pnl_col = pd.to_numeric(pnl_col, errors="coerce")
        mask &= pnl_col.fillna(float("-inf")) >= f["min_pnl"]
    if f.get("min_risk_pnl") is not None:
        risk_col = df["table_risk_pnl"] if "table_risk_pnl" in df.columns else (df["risk_pnl"] if "risk_pnl" in df.columns else df["realised_pnl_dd"])
        risk_col = pd.to_numeric(risk_col, errors="coerce")
        mask &= risk_col.fillna(float("-inf")) >= f["min_risk_pnl"]
    # wallet_include filter: exclude wallets marked false in ui_state.json
    wallet_include = _state.get("wallet_include", {})
    if wallet_include:
        excluded = {w for w, v in wallet_include.items() if v is False}
        if excluded:
            mask &= ~df["wallet"].isin(excluded)
    # Dormancy filter: hide wallets with no trade for more than 15 days.
    if _state.get("hide_dormant"):
        dormant_mask = df["last_trade_time"].apply(_is_dormant)
        mask &= ~dormant_mask
    return df[mask]


def _table_execution_required(sort: str) -> bool:
    f = _state.get("filters", {}) or {}
    return any(f.get(k) is not None for k in ("max_dd_pct", "max_slip_deg_03", "min_score", "min_pnl", "min_risk_pnl"))


def _get_visible_universe(sort: str = "compound_score", direction: str = "desc") -> pd.DataFrame:
    uni = _state["universe"]
    if uni.empty:
        return uni
    sort_alias = {
        "realised_eval_pnl": "table_pnl",
        "realised_pnl": "table_pnl",
        "risk_pnl": "table_risk_pnl",
        "slip_deg_03": "table_slip_pct",
        "compound_score": "table_compound_score",
    }
    needs_execution = _table_execution_required(sort)
    if needs_execution:
        uni = _table_execution_frame(uni)
        sort = sort_alias.get(sort, sort)
    selected_set = {w for w, v in _state["selected"].items() if v}
    filtered = _apply_filters(uni)
    # Selected wallets belong to the active portfolio and must remain visible
    # even when candidate-discovery filters no longer match them.  Filters
    # continue to narrow the unselected candidate rows.
    selected_rows = uni[uni["wallet"].isin(selected_set)]
    visible = pd.concat([selected_rows, filtered], ignore_index=False)
    visible = visible[~visible["wallet"].duplicated(keep="first")]
    if visible.empty:
        return visible
    visible = visible.copy()
    visible["_is_selected"] = visible["wallet"].isin(selected_set).astype(int)
    return _sort_universe_pinned(visible, sort, direction)


def _active_filter_count() -> int:
    count = len(_state.get("filters", {}))
    if _state.get("hide_dormant"):
        count += 1
    return count


def _activity_counts() -> dict[str, int]:
    """Return active (<=3d), slow (3d-1w), stale (1w-15d), and dormant (>15d) counts."""
    uni = _state.get("universe")
    if uni is None or uni.empty or "last_trade_time" not in uni.columns:
        return {"active": 0, "slow": 0, "stale": 0, "dormant": 0}
    counts = uni["last_trade_time"].apply(_activity_bucket).value_counts().to_dict()
    return {
        "active": int(counts.get("active", 0)),
        "slow": int(counts.get("slow", 0)),
        "stale": int(counts.get("stale", 0)),
        "dormant": int(counts.get("dormant", 0)),
    }


def _current_table_days() -> str:
    if _state.get("start_date") or _state.get("end_date"):
        return "none"
    return str(_state.get("days", "30"))


def _parse_date_ts(s: str | None) -> "pd.Timestamp | None":
    if not s:
        return None
    try:
        return pd.Timestamp(s, tz="UTC")
    except Exception:
        return None


def _get_wallet_pnl_series(wallet: str, mode: str, normalize: bool, risk_per_wallet: float, days: str) -> pd.Series:
    key = (wallet, mode, normalize, risk_per_wallet, days)
    if key in _series_cache:
        return _series_cache[key]
    replay_mode = False
    s_full = pd.Series(dtype=float)
    if mode != "raw":
        s_full = _load_trade_replay_pnl_series(wallet, mode)
        replay_mode = not s_full.empty
    if s_full.empty:
        s_full = _load_curve(wallet)
    if s_full.empty:
        _series_cache[key] = pd.Series(dtype=float)
        return _series_cache[key]
    cutoff = None
    baseline = 0.0
    has_baseline_point = False
    if days not in ("all", "none"):
        try:
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=int(days))
            prior = s_full[s_full.index < cutoff].tail(1)
            in_window = s_full[s_full.index >= cutoff]
            baseline = float(prior.iloc[-1]) if not prior.empty else 0.0
            has_baseline_point = not prior.empty
            if in_window.empty:
                result = pd.Series([0.0], index=pd.DatetimeIndex([cutoff]))
                _series_cache[key] = result
                return result
            s = pd.concat([prior, in_window]) if not prior.empty else in_window
        except (ValueError, TypeError):
            s = s_full
            baseline = 0.0
    else:
        s = s_full
    if s.empty:
        _series_cache[key] = pd.Series(dtype=float)
        return _series_cache[key]
    if not has_baseline_point:
        base_ts = cutoff if cutoff is not None else s.index[0] - pd.Timedelta(milliseconds=1)
        if base_ts not in s.index:
            s = pd.concat([pd.Series([baseline], index=pd.DatetimeIndex([base_ts])), s]).sort_index()
    slip_scale = 1.0
    if mode != "raw" and not replay_mode:
        mode_col = _MODE_COL.get(mode, "realised_pnl")
        uni = _state["universe"]
        rows = uni[uni["wallet"] == wallet]
        if not rows.empty:
            row = rows.iloc[0]
            w_raw_total = _num(row.get("realised_pnl"))
            w_slip_total = _num(row.get(mode_col))
            if abs(w_raw_total) > 1e-6:
                slip_scale = w_slip_total / w_raw_total
    s_start = baseline
    w_period_dd = abs(_curve_metrics(s)["dd"])
    if normalize:
        risk_ref_dd = w_period_dd
        uni = _state.get("universe")
        if uni is not None and not uni.empty:
            rows = uni[uni["wallet"] == wallet]
            if not rows.empty:
                real_dd = abs(_num(rows.iloc[0].get("max_dd")))
                if pd.notna(real_dd) and real_dd > 1e-6:
                    risk_ref_dd = max(risk_ref_dd, real_dd)
        # Treat Risk/wallet as a cap against the MTM-primary real DD when
        # present. A tiny curve dip must not lever a wallet into absurd PnL.
        risk_scale = min(1.0, risk_per_wallet / risk_ref_dd) if risk_ref_dd > 1e-6 else 1.0
    else:
        risk_scale = 1.0
    s_final = s_start + (s - s_start) * (slip_scale * risk_scale)
    result = (s_final - s_start).resample("1h").last().sort_index().ffill().fillna(0)
    if cutoff is not None:
        if cutoff not in result.index:
            result = pd.concat([pd.Series([0.0], index=pd.DatetimeIndex([cutoff])), result]).sort_index()
        result = result[result.index >= cutoff]
    elif not result.empty:
        start_anchor = result.index[0] - pd.Timedelta(milliseconds=1)
        result = pd.concat([pd.Series([0.0], index=pd.DatetimeIndex([start_anchor])), result]).sort_index()
    _series_cache[key] = result
    return result


def _apply_date_range_to_series(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    start_ts = _parse_date_ts(_state.get("start_date"))
    end_raw = _state.get("end_date")
    end_ts = _parse_date_ts(end_raw)
    if end_ts is not None and end_raw and len(str(end_raw)) <= 10:
        end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
    return _slice_rebased_series(s, start_ts, end_ts)


def _mode_slip_pct(raw_pnl, exec_pnl) -> float:
    if raw_pnl is None or exec_pnl is None or pd.isna(raw_pnl) or pd.isna(exec_pnl) or abs(float(raw_pnl)) < 1e-12:
        return float("nan")
    return ((float(raw_pnl) - float(exec_pnl)) / abs(float(raw_pnl))) * 100


def _display_ssot_dd(row, normalize: bool, risk_per_wallet: float) -> tuple[float, float]:
    dd = row.get("max_dd")
    if dd is None or pd.isna(dd) or float(dd) >= 0:
        return float("nan"), float("nan")
    adjusted_dd = float(dd)
    if normalize and risk_per_wallet > 1e-12:
        scale = min(1.0, risk_per_wallet / abs(adjusted_dd)) if abs(adjusted_dd) > 1e-12 else 1.0
        adjusted_dd *= scale
        return adjusted_dd, abs(adjusted_dd) / risk_per_wallet * 100.0
    base_pct = row.get("max_dd_pct")
    if base_pct is not None and pd.notna(base_pct):
        return adjusted_dd, float(base_pct)
    ref = abs(_num(row.get("realised_eval_pnl"), _num(row.get("realised_pnl")))) + abs(adjusted_dd)
    return adjusted_dd, abs(adjusted_dd) / ref * 100.0 if ref > 1e-12 else float("nan")


def _table_execution_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mode = _state.get("mode", "raw")
    normalize = bool(_state.get("normalize", True))
    risk_per_wallet = float(_state.get("risk_per_wallet", 100.0))
    days = _current_table_days()
    out = df.copy()
    exec_pnls = []
    ssot_dds = []
    ssot_dd_pcts = []
    raw_pnls = []
    slip_pcts = []
    risk_pnls = []
    scores = []
    dd_sources = []
    if mode != "raw" and "wallet" in out.columns:
        _ensure_trade_replay_series(set(out["wallet"].dropna().astype(str).str.strip().str.lower()))
    for _, r in out.iterrows():
        wallet = str(r.get("wallet", "")).strip().lower()
        s_exec = _apply_date_range_to_series(_get_wallet_pnl_series(wallet, mode, normalize, risk_per_wallet, days))
        s_raw = _apply_date_range_to_series(_get_wallet_pnl_series(wallet, "raw", normalize, risk_per_wallet, days))
        s_exec_for_slip = _apply_date_range_to_series(_get_wallet_pnl_series(wallet, mode, False, 1.0, days))
        replay_raw_for_slip = _load_trade_replay_raw_series(wallet) if mode != "raw" else pd.Series(dtype=float)
        s_raw_for_slip = _apply_date_range_to_series(
            _rebase_series_for_days(replay_raw_for_slip, days)
            if not replay_raw_for_slip.empty
            else _get_wallet_pnl_series(wallet, "raw", False, 1.0, days)
        )
        m_exec = _curve_metrics(s_exec)
        m_raw = _curve_metrics(s_raw)
        m_exec_for_slip = _curve_metrics(s_exec_for_slip)
        m_raw_for_slip = _curve_metrics(s_raw_for_slip)
        exec_pnl = m_exec["pnl"]
        raw_pnl = m_raw["pnl"]
        raw_pnl_for_slip = m_raw_for_slip["pnl"]
        exec_pnl_for_slip = m_exec_for_slip["pnl"]
        if pd.isna(exec_pnl):
            exec_pnl = r.get("realised_eval_pnl", r.get("realised_pnl"))
        ssot_dd, ssot_dd_pct = _display_ssot_dd(r, normalize, risk_per_wallet)
        ssot_dd_source = str(r.get("max_dd_source", "curve_untrusted"))
        exec_dd = m_exec["dd"]
        if pd.notna(exec_dd) and float(exec_dd) < 0 and (
            pd.isna(ssot_dd) or float(exec_dd) < float(ssot_dd)
        ):
            ssot_dd = float(exec_dd)
            ssot_dd_pct = _period_dd_pct(
                ssot_dd,
                m_exec.get("peak"),
                m_exec.get("pnl"),
                normalize,
                risk_per_wallet,
            )
            ssot_dd_source = "execution_curve"
        risk_pnl = (
            _safe_div(exec_pnl, abs(ssot_dd))
            if pd.notna(exec_pnl) and pd.notna(ssot_dd) and float(ssot_dd) < 0 and abs(float(ssot_dd)) > 1.0
            else float("nan")
        )
        score = min(100.0, max(0.0, float(risk_pnl) * 20.0)) if pd.notna(risk_pnl) else float("nan")
        exec_pnls.append(exec_pnl)
        ssot_dds.append(ssot_dd)
        ssot_dd_pcts.append(ssot_dd_pct)
        raw_pnls.append(raw_pnl)
        slip_pcts.append(_mode_slip_pct(raw_pnl_for_slip, exec_pnl_for_slip))
        risk_pnls.append(risk_pnl)
        scores.append(score)
        dd_sources.append(ssot_dd_source)
    out["table_pnl"] = exec_pnls
    out["table_dd"] = ssot_dds
    out["table_dd_pct"] = ssot_dd_pcts
    out["table_raw_pnl"] = raw_pnls
    out["table_slip_pct"] = slip_pcts
    out["table_risk_pnl"] = risk_pnls
    out["table_compound_score"] = scores
    out["table_dd_source"] = dd_sources
    return out


def _last_trade_age_fmt(last_trade_time) -> str:
    """Format last trade time as a human-readable age string."""
    if last_trade_time is None or (isinstance(last_trade_time, float) and math.isnan(last_trade_time)):
        return "\u2014"
    try:
        ts = float(last_trade_time)
        if ts < 1e9:
            return "\u2014"
        if ts < 1e12:
            ts *= 1000
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        delta_ms = max(0, now_ms - int(ts))
        if delta_ms < 60_000:
            return f"{int(delta_ms / 1000)}s ago"
        if delta_ms < 3_600_000:
            return f"{int(delta_ms / 60_000)}m ago"
        if delta_ms < 86_400_000:
            return f"{round(delta_ms / 3_600_000, 1)}h ago"
        return f"{round(delta_ms / 86_400_000, 1)}d ago"
    except Exception:
        return "\u2014"


# Colour/label rules for the Last Trade cell, shared by the Jinja template and
# the SSE push so a live-updated cell is styled identically to a rendered one.
LAST_TRADE_STYLES = {
    "dormant": ("#f85149", "Dormant: last trade >15d ago", True),
    "stale": ("#f0883e", "Stale: last trade 1w-15d ago", False),
    "slow": ("#d29922", "Slow: last trade 3d-1w ago", False),
    "active": ("#3fb950", "Active: last trade <=3d ago", False),
}


def last_trade_cell(last_trade_time) -> dict:
    """Presentation payload for one Last Trade cell."""
    age = _last_trade_age_fmt(last_trade_time)
    if age == "—":
        return {"age": age, "color": "#6e7681", "title": "", "warn": False}
    color, title, warn = LAST_TRADE_STYLES.get(
        _activity_bucket(last_trade_time), LAST_TRADE_STYLES["active"]
    )
    return {"age": age, "color": color, "title": title, "warn": warn}


def get_table_rows(sort: str = "compound_score", direction: str = "desc", offset: int = 0, limit: int | None = None) -> list[dict]:
    uni = _get_visible_universe(sort, direction)
    if uni.empty:
        return []
    defer_slice = False
    if limit is not None and not defer_slice:
        offset = max(0, int(offset or 0))
        limit = max(1, int(limit or 1))
        uni = uni.iloc[offset:offset + limit]
    elif limit is not None:
        offset = max(0, int(offset or 0))
        limit = max(1, int(limit or 1))
    if "table_pnl" not in uni.columns:
        uni = _table_execution_frame(uni)
    rows: list[dict] = []
    tbl_mode = _state.get("mode", "raw")
    tbl_normalize = bool(_state.get("normalize", True))
    tbl_risk = float(_state.get("risk_per_wallet", 100.0))
    for _, r in uni.iterrows():
        w = r["wallet"]

        mtm_dd_val = _num(r.get("max_dd_mtm"))
        mtm_dd_pct_val = r.get("max_dd_mtm_pct")
        mtm_source = "monthly_mtm" if mtm_dd_val < 0 else "none"
        using_mtm_dd = bool(mtm_dd_val < 0)
        mtm_source_label = "M" if using_mtm_dd else "—"
        mtm_source_title = (
            "Monthly MTM drawdown from accountValueHistory; includes unrealised PnL"
            if using_mtm_dd
            else "No monthly accountValueHistory MTM drawdown for this wallet"
        )
        # ── Primary risk DD: prefer MTM over realised ──────────────
        # max_dd is now set to MTM DD when available (see DD priority
        # cascade above). Use it as the primary display DD.
        display_pnl = r.get("table_pnl", r.get("realised_eval_pnl"))
        display_dd = r.get("table_dd", r.get("max_dd"))  # SSOT DD, risk-scaled only when normalization is active
        display_dd_pct = r.get("table_dd_pct", r.get("max_dd_pct"))
        canonical_pnl = display_pnl
        canonical_dd = display_dd
        dd_source = str(r.get("table_dd_source", r.get("max_dd_source", "curve_untrusted")))
        realised_source = str(r.get("realised_eval_source", "curve_untrusted"))
        if dd_source.startswith("mtm"):
            dd_source_label = "M"
            dd_source_title = (
                f"AllTime MTM drawdown from accountValueHistory ({dd_source}). "
                "Includes unrealised PnL, funding, fees. Authoritative for risk."
                if dd_source == "mtm_alltime"
                else "Monthly MTM drawdown from accountValueHistory. Includes unrealised PnL."
            )
        elif dd_source == "scanner_realised":
            dd_source_label = "R"
            dd_source_title = "Realised scanner drawdown from summary.csv max_drawdown (closedPnL only)"
        elif dd_source == "execution_curve":
            dd_source_label = "E"
            dd_source_title = (
                "Follower execution drawdown after the selected slippage mode, normalization, "
                "risk cap, and date range. Used because it is deeper than leader MTM DD."
            )
        else:
            dd_source_label = "?"
            dd_source_title = "Curve/raw-fill fallback only; untrusted DD source"
        display_risk_pnl = r.get("table_risk_pnl", r.get("risk_pnl"))
        risk_pnl_title = (
            f"PnL / real drawdown: PnL { _fmt(display_pnl, '${:,.2f}') } / {dd_source_label} DD { _fmt(display_dd, '${:,.2f}') }. {dd_source_title}"
            if pd.notna(display_risk_pnl)
            else "PnL/DD unavailable: real drawdown is missing, untrusted, or below the 2% stability floor"
        )

        rows.append({
            "wallet": w,
            "wallet_label": f"{w[:10]}\u2026{w[-6:]}",
            "rank": int(r["display_rank"]),
            "base_rank": int(r.get("rank", r["display_rank"])),
            "score": r.get("score"), "score_fmt": _fmt(r.get("score"), "{:.4f}"),
            "compound_score": r.get("table_compound_score", r.get("compound_score")), "compound_score_fmt": _fmt(r.get("table_compound_score", r.get("compound_score")), "{:.1f}"),
            "mtm_calmar": r.get("mtm_calmar"), "mtm_calmar_fmt": _fmt(r.get("mtm_calmar"), "{:.2f}"),
            "pnl": display_pnl, "pnl_fmt": _fmt(display_pnl, "${:,.2f}"),
            "pnl_positive": bool(pd.notna(display_pnl) and display_pnl >= 0),
            "exec_pnl": canonical_pnl, "exec_pnl_fmt": _fmt(canonical_pnl, "${:,.2f}"),
            "pnl_title": f"Realised scanner PnL. Current execution/range PnL: {_fmt(canonical_pnl, '${:,.2f}')}",
            "pnl_slip_05": r.get("pnl_slip_05"), "pnl_slip_05_fmt": _fmt(r.get("pnl_slip_05"), "${:,.2f}"),
            "pnl_slip_10": r.get("pnl_slip_10"), "pnl_slip_10_fmt": _fmt(r.get("pnl_slip_10"), "${:,.2f}"),
            "pnl_slip_30": r.get("pnl_slip_30"), "pnl_slip_30_fmt": _fmt(r.get("pnl_slip_30"), "${:,.2f}"),
            "risk_pnl": display_risk_pnl, "risk_pnl_fmt": _fmt(display_risk_pnl, "{:.3f}"),
            "risk_pnl_basis": display_pnl, "risk_pnl_basis_fmt": _fmt(display_pnl, "${:,.2f}"),
            "risk_pnl_title": risk_pnl_title,
            "slip_deg_03": r.get("table_slip_pct", r.get("slip_deg_03")), "slip_deg_03_fmt": _fmt(r.get("table_slip_pct", r.get("slip_deg_03")), "{:.2f}%"),
            "slip_deg_03_cls": (
                "" if (lambda v: v is None or (isinstance(v, float) and math.isnan(v)))(r.get("table_slip_pct", r.get("slip_deg_03")))
                else ("pos" if r.get("table_slip_pct", r.get("slip_deg_03")) < 10 else ("warn" if r.get("table_slip_pct", r.get("slip_deg_03")) <= 30 else "neg"))
            ),
            "efficiency": r.get("efficiency"), "eff_fmt": _fmt(r.get("efficiency"), "{:.6f}"),
            "max_dd": display_dd, "max_dd_fmt": _fmt(display_dd, "${:,.2f}"),
            "max_dd_pct": display_dd_pct, "max_dd_pct_fmt": _fmt(display_dd_pct, "{:.1f}%"),
            # MTM truth DD (accountValueHistory — includes unrealised)
            "max_dd_mtm": mtm_dd_val, "max_dd_mtm_fmt": _fmt(mtm_dd_val, "${:,.2f}"),
            "max_dd_mtm_pct": mtm_dd_pct_val, "max_dd_mtm_pct_fmt": _fmt(mtm_dd_pct_val, "{:.1f}%"),
            "max_dd_source": dd_source,
            "max_dd_is_mtm": dd_source.startswith("mtm"),
            "max_dd_source_label": dd_source_label,
            "max_dd_source_title": dd_source_title,
            "mtm_source_label": mtm_source_label,
            "mtm_source_title": mtm_source_title,
            "trades": r.get("trades"), "trades_fmt": _fmt(r.get("trades"), "{:,.0f}"),
            "trades_7d": r.get("trades_7d"), "trades_7d_fmt": _fmt(r.get("trades_7d"), "{:,.0f}"),
            "last_trade_time": r.get("last_trade_time"),
            "last_trade_age_fmt": _last_trade_age_fmt(r.get("last_trade_time")),
            "last_trade_cell": last_trade_cell(r.get("last_trade_time")),
            "is_dormant": _is_dormant(r.get("last_trade_time")),
            "is_slow": _is_slow(r.get("last_trade_time")),
            "is_stale": _is_stale(r.get("last_trade_time")),
            "penalty_flags": _penalty_flags(r),
            "penalty_flags_title": _penalty_flags_title(r),
            "selected": bool(_state["selected"].get(w, False)),
            "sparkline": _build_sparkline(w),
        })
    if sort == "risk_pnl":
        reverse = direction != "asc"
        def _risk_sort_value(row: dict) -> float:
            v = row.get("risk_pnl")
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return float("-inf") if reverse else float("inf")
            return float(v)
        rows.sort(
            key=lambda row: (
                0 if row.get("selected") else 1,
                -_risk_sort_value(row) if reverse else _risk_sort_value(row),
                -_num(row.get("compound_score")),
                str(row.get("wallet", "")),
            )
        )
        for idx, row in enumerate(rows, start=1):
            row["rank"] = idx
    if limit is not None and sort == "risk_pnl" and defer_slice:
        rows = rows[offset:offset + limit]
    return rows


def _table_context(sort: str = "compound_score", dir: str = "desc", page: int = 1, per_page: int = 100) -> dict:
    mapped_sort, mapped_dir = _get_sort(sort, dir)
    visible = _get_visible_universe(mapped_sort, mapped_dir)
    total = 0 if visible.empty else len(visible)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return {
        "rows": get_table_rows(mapped_sort, mapped_dir, offset=start, limit=per_page),
        "sort": sort,  # Return the USER-FACING sort key for template column header matching
        "dir": mapped_dir,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "total_rows": total,
        "active_filter_count": _active_filter_count(),
    }


def _render_table_html(sort: str = "compound_score", dir: str = "desc", page: int = 1, per_page: int = 100) -> str:
    return render("partials/table.html", **_table_context(sort, dir, page, per_page), oob=False)


def _penalty_flags(r) -> str:
    """Return a short string showing penalty flags (empty if none)."""
    parts = []
    if _martingale_flag_reliable() and int(_num(r.get("martingale_flag"))) == 1:
        parts.append("M")
    if int(_num(r.get("one_big_trade_flag"))) == 1:
        parts.append("1B")
    if int(_num(r.get("equity_collapse_flag"))) == 1 or int(_num(r.get("equity_collapse_flag_mtm"))) == 1:
        parts.append("EC")
    if int(_num(r.get("suspected_truncated"))) == 1:
        parts.append("TR")
    return " ".join(parts)


def _penalty_flags_title(r) -> str:
    """Return a readable explanation for compact penalty flags."""
    explanations = []
    if _martingale_flag_reliable() and int(_num(r.get("martingale_flag"))) == 1:
        explanations.append("M = martingale-like sizing pattern")
    if int(_num(r.get("one_big_trade_flag"))) == 1:
        explanations.append("1B = one trade dominates realised PnL")
    if int(_num(r.get("equity_collapse_flag"))) == 1 or int(_num(r.get("equity_collapse_flag_mtm"))) == 1:
        explanations.append("EC = equity collapse risk flag")
    if int(_num(r.get("suspected_truncated"))) == 1:
        reason = str(r.get("termination_reason") or "unknown")
        explanations.append(f"TR = trade history may be truncated ({reason})")
    return "; ".join(explanations) if explanations else "No penalty flags"


def get_portfolio_data(days: str = "30") -> dict:
    uni = _state["universe"]
    valid_wallets = set(uni["wallet"].astype(str).str.strip().str.lower()) if not uni.empty and "wallet" in uni.columns else set()
    selected_set = {str(w).strip().lower() for w, v in _state["selected"].items() if v and str(w).strip().lower() in valid_wallets}
    ordered_seen: set[str] = set()
    selected = []
    for w in _state.get("wallet_order", []):
        w = str(w).strip().lower()
        if w in selected_set and w not in ordered_seen:
            selected.append(w)
            ordered_seen.add(w)
    if selected_set - ordered_seen:
        universe_order = [str(w).strip().lower() for w in uni["wallet"].tolist()] if not uni.empty else []
        for w in universe_order:
            if w in selected_set and w not in ordered_seen:
                selected.append(w)
                ordered_seen.add(w)
        for w in sorted(selected_set - ordered_seen):
            selected.append(w)
    mode = _state.get("mode", "raw")
    risk_per_wallet = float(_state.get("risk_per_wallet", 100.0))
    normalize = bool(_state.get("normalize", True))
    mode_col = _MODE_COL.get(mode, "realised_pnl")
    _empty = {
        "n_selected": 0, "total_pnl_fmt": "\u2014", "exec_loss_fmt": "\u2014",
        "max_dd_fmt": "\u2014", "trades_fmt": "\u2014", "avg_eff_fmt": "\u2014",
        "avg_compound_score_fmt": "\u2014", "labels_json": Markup("[]"), "values_json": Markup("[]"),
        "mtm_dd_values_json": Markup("[]"), "mtm_marker_values_json": Markup("[]"),
        "pnl_points_json": Markup("[]"), "mtm_dd_points_json": Markup("[]"), "mtm_marker_points_json": Markup("[]"),
        "has_data": False, "pnl_positive": True, "days": days, "mode": mode,
        "risk_per_wallet": risk_per_wallet, "normalize": normalize,
        "raw_total_pnl_fmt": "\u2014", "raw_max_dd_fmt": "\u2014",
        "leader_mtm_max_dd_fmt": "\u2014", "follower_mtm_max_dd_fmt": "\u2014", "mtm_max_dd_fmt": "\u2014",
        "mtm_has_data": False,
        "selected_wallets": [], "start_date": _state.get("start_date") or "",
        "end_date": _state.get("end_date") or "",
        "empty_message": "No wallets selected \u2014 select wallets to build portfolio.",
    }
    if not selected or uni.empty:
        return _empty
    cache_key = (
        days,
        mode,
        normalize,
        round(risk_per_wallet, 8),
        _state.get("start_date") or "",
        _state.get("end_date") or "",
        tuple(selected),
        tuple((w, float(_state.get("wallet_weights", {}).get(w, 1.0))) for w in selected),
    )
    if cache_key in _portfolio_cache:
        return _portfolio_cache[cache_key]
    portfolio_df = uni[uni["wallet"].isin(selected)]
    weights = _state.get("wallet_weights", {})
    start_ts = _parse_date_ts(_state.get("start_date"))
    _end_raw = _state.get("end_date")
    end_ts = (_parse_date_ts(_end_raw) + pd.Timedelta(hours=23, minutes=59, seconds=59)) if _end_raw else None
    if start_ts is None and end_ts is None and days not in ("all", "none"):
        start_ts_for_trades = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=int(days))
    else:
        start_ts_for_trades = start_ts
    period_trade_stats = _load_trade_stats_for_wallets(selected, start_ts_for_trades, end_ts)
    if start_ts is None and end_ts is None and days in ("all", "none"):
        for _, row in portfolio_df.iterrows():
            w = str(row.get("wallet", "")).strip().lower()
            if not w:
                continue
            entry = period_trade_stats.setdefault(w, {"trades": 0.0, "total_notional": 0.0, "realised_pnl": 0.0})
            summary_trades = _num(row.get("trades"), float("nan"))
            if pd.notna(summary_trades) and summary_trades > entry.get("trades", 0.0):
                entry["trades"] = summary_trades
            summary_notional = _num(row.get("total_notional"), float("nan"))
            if pd.notna(summary_notional) and summary_notional > entry.get("total_notional", 0.0):
                entry["total_notional"] = summary_notional
            summary_pnl = _num(row.get("realised_eval_pnl", row.get("realised_pnl")), float("nan"))
            if pd.notna(summary_pnl) and abs(summary_pnl) > abs(entry.get("realised_pnl", 0.0)):
                entry["realised_pnl"] = summary_pnl
    exec_series: dict[str, pd.Series] = {}
    exec_unscaled_series: dict[str, pd.Series] = {}
    raw_series: dict[str, pd.Series] = {}
    leader_mtm_series: dict[str, pd.Series] = {}
    mtm_series: dict[str, pd.Series] = {}
    wallet_follow_scales: dict[str, float] = {}
    wallet_exec_pnls: dict[str, float] = {}
    wallet_dds: dict[str, tuple[float, float]] = {}
    wallet_mtm_dd_pcts: dict[str, float] = {}
    use_days = "none" if (start_ts is not None or end_ts is not None) else days
    mtm_days = "all" if (start_ts is not None or end_ts is not None) else days
    if mode != "raw":
        _ensure_trade_replay_series(selected)
    for w in selected:
        s_exec_unscaled = _get_wallet_pnl_series(w, mode, False, 1.0, use_days)
        s_raw = _get_wallet_pnl_series(w, "raw", False, 1.0, use_days)
        s_replay_raw = (
            _rebase_series_for_days(_load_trade_replay_raw_series(w), use_days)
            if mode != "raw"
            else pd.Series(dtype=float)
        )
        s_mtm = _load_mtm_account_value_series(w, mtm_days)
        if start_ts is not None or end_ts is not None:
            s_exec_unscaled = _slice_rebased_series(s_exec_unscaled, start_ts, end_ts)
            s_raw = _slice_rebased_series(s_raw, start_ts, end_ts)
            s_replay_raw = _slice_rebased_series(s_replay_raw, start_ts, end_ts)
        if start_ts is not None:
            s_mtm = s_mtm.loc[start_ts:]
        if end_ts is not None:
            s_mtm = s_mtm.loc[:end_ts]

        mtm_delta = (s_mtm - s_mtm.iloc[0]) if not s_mtm.empty else pd.Series(dtype=float)
        m_exec_unscaled = _curve_metrics(s_exec_unscaled)
        if not mtm_delta.empty:
            slip_delta = pd.Series(dtype=float)
            slip_base = s_replay_raw if not s_replay_raw.empty else s_raw
            if mode != "raw" and not slip_base.empty and not s_exec_unscaled.empty:
                slip_idx = slip_base.index.union(s_exec_unscaled.index).sort_values()
                raw_aligned = slip_base.reindex(slip_idx).ffill().fillna(0)
                exec_aligned = s_exec_unscaled.reindex(slip_idx).ffill().fillna(0)
                slip_delta = (exec_aligned - raw_aligned).sort_index()
            mtm_slip_delta = (
                slip_delta.reindex(mtm_delta.index, method="ffill").fillna(0)
                if not slip_delta.empty
                else pd.Series(0.0, index=mtm_delta.index)
            )
            follower_mtm_unscaled = mtm_delta + mtm_slip_delta
        else:
            follower_mtm_unscaled = pd.Series(dtype=float)
        mtm_dd_unscaled = float((follower_mtm_unscaled - follower_mtm_unscaled.cummax()).min()) if not follower_mtm_unscaled.empty else float("nan")
        if not s_mtm.empty:
            mtm_running_peak = s_mtm.cummax()
            mtm_raw_dd = (
                float((follower_mtm_unscaled - follower_mtm_unscaled.cummax()).min())
                if not follower_mtm_unscaled.empty
                else float((s_mtm - mtm_running_peak).min())
            )
            mtm_peak_ref = float(mtm_running_peak.max())
            wallet_mtm_dd_pcts[w] = abs(mtm_raw_dd) / mtm_peak_ref * 100.0 if mtm_peak_ref > 1e-12 else float("nan")
        if normalize:
            if pd.notna(mtm_dd_unscaled) and mtm_dd_unscaled < 0 and abs(mtm_dd_unscaled) > 1e-12:
                risk_ref_dd = abs(mtm_dd_unscaled)
            else:
                risk_ref_dd = abs(m_exec_unscaled["dd"]) if pd.notna(m_exec_unscaled.get("dd")) else float("nan")
            wallet_follow_scales[w] = min(1.0, risk_per_wallet / risk_ref_dd) if pd.notna(risk_ref_dd) and risk_ref_dd > 1e-12 else 1.0
        else:
            wallet_follow_scales[w] = 1.0
        s_exec = s_exec_unscaled * wallet_follow_scales[w] if not s_exec_unscaled.empty else pd.Series(dtype=float)

        if not s_exec.empty:
            exec_series[w] = s_exec
            if not s_exec_unscaled.empty:
                exec_unscaled_series[w] = s_exec_unscaled
            m_exec = _curve_metrics(s_exec)
            wallet_exec_pnls[w] = m_exec["pnl"]
            w_dd_dollar = m_exec["dd"]
            w_dd_pct = _period_dd_pct(w_dd_dollar, m_exec.get("peak"), m_exec.get("pnl"), normalize, risk_per_wallet)
            wallet_dds[w] = (w_dd_dollar, w_dd_pct)
        if not s_raw.empty:
            raw_series[w] = s_raw
        if not mtm_delta.empty:
            leader_mtm_series[w] = mtm_delta
            mtm_series[w] = follower_mtm_unscaled * wallet_follow_scales.get(w, 1.0)

    if exec_series:
        _exec_idx = pd.concat(list(exec_series.values()), axis=1).sort_index().index
        port = pd.DataFrame({
            w: exec_series[w].reindex(_exec_idx).ffill().fillna(0) * float(weights.get(w, 1.0))
            for w in exec_series
        }).sum(axis=1)
        m_port = _curve_metrics(port)
        portfolio_pnl = m_port["pnl"]
        portfolio_dd = m_port["dd"]
    else:
        port = pd.Series(dtype=float)
        portfolio_pnl = float("nan")
        portfolio_dd = float("nan")

    if raw_series:
        _raw_idx = pd.concat(list(raw_series.values()), axis=1).sort_index().index
        raw_port = pd.DataFrame({
            w: raw_series[w].reindex(_raw_idx).ffill().fillna(0) * float(weights.get(w, 1.0))
            for w in raw_series
        }).sum(axis=1)
        m_raw = _curve_metrics(raw_port)
        raw_total_pnl = m_raw["pnl"]
        raw_max_dd = m_raw["dd"]
    else:
        raw_total_pnl = float("nan")
        raw_max_dd = float("nan")

    if exec_unscaled_series:
        _mode_idx = pd.concat(list(exec_unscaled_series.values()), axis=1).sort_index().index
        mode_unscaled_port = pd.DataFrame({
            w: exec_unscaled_series[w].reindex(_mode_idx).ffill().fillna(0) * float(weights.get(w, 1.0))
            for w in exec_unscaled_series
        }).sum(axis=1)
        mode_unscaled_pnl = _curve_metrics(mode_unscaled_port)["pnl"]
    else:
        mode_unscaled_pnl = float("nan")

    if leader_mtm_series:
        _leader_mtm_idx = pd.concat(list(leader_mtm_series.values()), axis=1).sort_index().index
        leader_mtm_port = pd.DataFrame({
            w: leader_mtm_series[w].reindex(_leader_mtm_idx).ffill().fillna(0) * float(weights.get(w, 1.0))
            for w in leader_mtm_series
        }).sum(axis=1)
        if not port.empty:
            leader_mtm_port = leader_mtm_port.loc[port.index.min():port.index.max()]
        leader_mtm_dd = leader_mtm_port - leader_mtm_port.cummax()
        leader_mtm_max_dd = float(leader_mtm_dd.min()) if not leader_mtm_dd.empty else float("nan")
    else:
        leader_mtm_dd = pd.Series(dtype=float)
        leader_mtm_max_dd = float("nan")

    if mtm_series:
        _mtm_idx = pd.concat(list(mtm_series.values()), axis=1).sort_index().index
        mtm_port = pd.DataFrame({
            w: mtm_series[w].reindex(_mtm_idx).ffill().fillna(0) * float(weights.get(w, 1.0))
            for w in mtm_series
        }).sum(axis=1)
        if not port.empty:
            mtm_port = mtm_port.loc[port.index.min():port.index.max()]
        mtm_dd = mtm_port - mtm_port.cummax()
        follower_mtm_max_dd_raw = float(mtm_dd.min()) if not mtm_dd.empty else float("nan")
        follower_mtm_max_dd = (
            min(follower_mtm_max_dd_raw, portfolio_dd)
            if pd.notna(follower_mtm_max_dd_raw) and pd.notna(portfolio_dd)
            else (portfolio_dd if pd.notna(portfolio_dd) else follower_mtm_max_dd_raw)
        )
    else:
        mtm_dd = pd.Series(dtype=float)
        follower_mtm_max_dd_raw = float("nan")
        follower_mtm_max_dd = portfolio_dd if pd.notna(portfolio_dd) else float("nan")

    exec_loss_pct = (
        (raw_total_pnl - mode_unscaled_pnl) / abs(raw_total_pnl) * 100
        if pd.notna(raw_total_pnl) and pd.notna(mode_unscaled_pnl) and abs(raw_total_pnl) > 1e-6 else 0.0
    )
    n_trades = int(sum(_num(v.get("trades")) for v in period_trade_stats.values()))
    notional_total = sum(_num(v.get("total_notional")) for v in period_trade_stats.values())
    avg_eff = (raw_total_pnl / notional_total) if notional_total > 1e-12 and pd.notna(raw_total_pnl) else float("nan")
    scored_wallets: list[float] = []
    for w in selected:
        pnl = wallet_exec_pnls.get(w, float("nan"))
        mtm_dd_for_score = float((mtm_series[w] - mtm_series[w].cummax()).min()) if w in mtm_series and not mtm_series[w].empty else float("nan")
        dd_val = abs(mtm_dd_for_score) if pd.notna(mtm_dd_for_score) and mtm_dd_for_score < 0 else abs(wallet_dds.get(w, (float("nan"), float("nan")))[0])
        trades_val = _num(period_trade_stats.get(w, {}).get("trades"))
        if pd.notna(pnl) and pd.notna(dd_val) and dd_val > 1.0 and trades_val > 0:
            scored_wallets.append(min(100.0, max(0.0, (pnl / dd_val) * 20.0)))
    avg_compound_score = float(sum(scored_wallets) / len(scored_wallets)) if scored_wallets else float("nan")

    selected_wallets = []
    for w in selected:
        w_row = portfolio_df[portfolio_df["wallet"] == w]
        if w_row.empty:
            continue
        r = w_row.iloc[0]
        w_exec_pnl_disp = wallet_exec_pnls.get(w, float("nan"))
        wt = float(weights.get(w, 1.0))
        contribution_base = portfolio_pnl
        contribution = (w_exec_pnl_disp * wt / contribution_base * 100) if abs(contribution_base) > 1e-12 else float("nan")
        curve_dd_dollar, curve_dd_pct = wallet_dds.get(w, (float("nan"), float("nan")))
        w_mtm_dd_raw = float((mtm_series[w] - mtm_series[w].cummax()).min()) if w in mtm_series and not mtm_series[w].empty else float("nan")
        dd_candidates: list[tuple[str, float, float]] = []
        if pd.notna(w_mtm_dd_raw):
            dd_candidates.append(("period_mtm", float(w_mtm_dd_raw), wallet_mtm_dd_pcts.get(w, float("nan"))))
        if pd.notna(curve_dd_dollar):
            dd_candidates.append(("period_curve", float(curve_dd_dollar), curve_dd_pct))
        if days in ("all", "none"):
            scanner_dd = r.get("max_drawdown", r.get("realised_eval_dd"))
            if scanner_dd is not None and pd.notna(scanner_dd):
                scanner_ref = abs(_num(r.get("realised_eval_pnl", r.get("realised_pnl")))) + abs(float(scanner_dd))
                scanner_pct = abs(float(scanner_dd)) / scanner_ref * 100.0 if scanner_ref > 1e-12 else float("nan")
                dd_candidates.append(("scanner_realised", float(scanner_dd), scanner_pct))
        if dd_candidates:
            w_dd_source, w_dd_dollar, w_dd_pct = min(dd_candidates, key=lambda item: item[1])
        else:
            w_dd_source, w_dd_dollar, w_dd_pct = "period_curve", float("nan"), float("nan")
        if w_dd_source == "period_mtm" and pd.notna(w_dd_dollar) and w_dd_dollar < 0:
            w_dd_pct = (
                abs(w_dd_dollar) / risk_per_wallet * 100.0
                if normalize and risk_per_wallet > 1e-12
                else w_dd_pct
            )
        elif normalize and pd.notna(w_dd_dollar) and w_dd_dollar < 0 and risk_per_wallet > 1e-12:
            w_dd_pct = abs(w_dd_dollar) / risk_per_wallet * 100.0
        w_dd_source_label = "M" if w_dd_source == "period_mtm" else ("R" if w_dd_source == "scanner_realised" else "P")
        w_dd_source_title = (
            "Period MTM drawdown from accountValueHistory. Includes unrealised PnL, funding, fees."
            if w_dd_source == "period_mtm"
            else (
                "Full-history realised drawdown from summary.csv max_drawdown."
                if w_dd_source == "scanner_realised"
                else "Period drawdown from the selected execution curve after mode, normalization, risk, and date filters."
            )
        )
        w_mtm_dd_fmt = _fmt(w_mtm_dd_raw, "${:,.2f}")
        w_trades = _num(period_trade_stats.get(w, {}).get("trades"))
        w_score = min(100.0, max(0.0, (w_exec_pnl_disp / abs(w_dd_dollar)) * 20.0)) if pd.notna(w_exec_pnl_disp) and pd.notna(w_dd_dollar) and abs(w_dd_dollar) > 1.0 else float("nan")
        w_risk_pnl = _safe_div(w_exec_pnl_disp, abs(w_dd_dollar)) if pd.notna(w_exec_pnl_disp) and pd.notna(w_dd_dollar) and abs(w_dd_dollar) > 1.0 else float("nan")
        _sd = r.get("slip_deg_03")
        _slip_cls = (
            "" if (_sd is None or (isinstance(_sd, float) and math.isnan(float(_sd))))
            else ("pos" if float(_sd) < 10 else ("warn" if float(_sd) <= 30 else "neg"))
        )
        selected_wallets.append({
            "wallet": w, "wallet_label": f"{w[:10]}\u2026{w[-6:]}",
            "pnl": w_exec_pnl_disp, "pnl_fmt": _fmt(w_exec_pnl_disp, "${:,.2f}"),
            "pnl_positive": bool(pd.notna(w_exec_pnl_disp) and w_exec_pnl_disp >= 0),
            "max_dd": w_dd_dollar, "max_dd_fmt": _fmt(w_dd_dollar, "${:,.2f}"),
            "max_dd_pct_fmt": _fmt(w_dd_pct, "{:.1f}%"),
            "max_dd_source_label": w_dd_source_label,
            "max_dd_source_title": w_dd_source_title,
            # MTM drawdown truth (accountValueHistory â€” includes unrealised)
            "max_dd_mtm": w_mtm_dd_raw, "max_dd_mtm_fmt": w_mtm_dd_fmt,
            "max_dd_mtm_pct_fmt": "",
            "max_dd_source": w_dd_source,
            "slip_deg_03": _num(r.get("slip_deg_03")), "slip_deg_03_fmt": _fmt(r.get("slip_deg_03"), "{:.2f}%"),
            "slip_deg_03_cls": _slip_cls,
            "contribution": _num(contribution), "contribution_fmt": _fmt(contribution, "{:+.1f}%"),
            "contribution_positive": bool(pd.notna(contribution) and contribution >= 0),
            "score": w_score, "score_fmt": _fmt(w_score, "{:.4f}"),
            "compound_score": w_score, "compound_score_fmt": _fmt(w_score, "{:.1f}"),
            "mtm_calmar": _num(r.get("mtm_calmar")), "mtm_calmar_fmt": _fmt(r.get("mtm_calmar"), "{:.2f}"),
            "risk_pnl": w_risk_pnl, "risk_pnl_fmt": _fmt(w_risk_pnl, "{:.3f}"),
            "trades": w_trades, "trades_fmt": _fmt(w_trades, "{:,.0f}"),
        })

    _shared = {
        "n_selected": len(selected),
        "total_pnl_fmt": _fmt(portfolio_pnl, "${:,.2f}"),
        "exec_loss_fmt": _fmt(exec_loss_pct, "{:.1f}%"),
        "trades_fmt": f"{n_trades:,}",
        "avg_eff_fmt": _fmt(avg_eff, "{:.6f}"),
        "avg_compound_score_fmt": _fmt(avg_compound_score, "{:.1f}"),
        "pnl_positive": not math.isnan(portfolio_pnl) and portfolio_pnl >= 0,
        "days": days, "mode": mode, "risk_per_wallet": risk_per_wallet, "normalize": normalize,
        "raw_total_pnl_fmt": _fmt(raw_total_pnl, "${:,.2f}"),
        "raw_max_dd_fmt": _fmt(raw_max_dd, "${:,.2f}"),
        "leader_mtm_max_dd_fmt": _fmt(leader_mtm_max_dd, "${:,.2f}"),
        "follower_mtm_max_dd_fmt": _fmt(follower_mtm_max_dd, "${:,.2f}"),
        "mtm_max_dd_fmt": _fmt(follower_mtm_max_dd, "${:,.2f}"),
        "mtm_has_data": bool(not mtm_dd.empty and pd.notna(follower_mtm_max_dd)),
        "selected_wallets": selected_wallets,
        "start_date": _state.get("start_date") or "",
        "end_date": _state.get("end_date") or "",
        "empty_message": "No equity data available for the selected wallets.",
    }

    if not exec_series:
        result = {
            **_shared,
            "max_dd_fmt": "\u2014",
            "labels_json": Markup("[]"), "values_json": Markup("[]"),
            "mtm_dd_values_json": Markup("[]"), "mtm_marker_values_json": Markup("[]"),
            "pnl_points_json": Markup("[]"), "mtm_dd_points_json": Markup("[]"), "mtm_marker_points_json": Markup("[]"),
            "has_data": False,
        }
        _portfolio_cache[cache_key] = result
        return result

    step = max(1, len(port) // 300)
    port_ds = port.iloc[::step]
    labels = [ts.strftime("%Y-%m-%d %H:%M") for ts in port_ds.index]
    values = [round(float(v), 2) for v in port_ds.tolist()]
    pnl_points = [
        {"x": int(ts.timestamp() * 1000), "y": round(float(v), 2)}
        for ts, v in port_ds.items()
    ]
    mtm_dd_values = (
        [
            None if pd.isna(v) else round(float(v), 2)
            for v in mtm_dd.reindex(port_ds.index, method="ffill").tolist()
        ]
        if not mtm_dd.empty
        else []
    )
    mtm_dd_ds = mtm_dd.iloc[::max(1, len(mtm_dd) // 300)] if not mtm_dd.empty else pd.Series(dtype=float)
    if not mtm_dd_ds.empty and pd.notna(portfolio_dd):
        mtm_dd_ds = mtm_dd_ds.apply(lambda v: min(float(v), float(portfolio_dd)) if pd.notna(v) else v)
    mtm_dd_points = [
        {"x": int(ts.timestamp() * 1000), "y": round(float(v), 2)}
        for ts, v in mtm_dd_ds.items()
        if pd.notna(v)
    ]
    if mtm_dd_values and not any(v is not None for v in mtm_dd_values):
        mtm_dd_values = []
    if mtm_dd_values and pd.notna(portfolio_dd):
        mtm_dd_values = [None if v is None else min(v, round(float(portfolio_dd), 2)) for v in mtm_dd_values]
    mtm_marker_values = (
        [round(float(follower_mtm_max_dd), 2) for _ in values]
        if values and not mtm_dd_values and pd.notna(follower_mtm_max_dd) and follower_mtm_max_dd < 0
        else []
    )
    mtm_marker_points = (
        [{"x": p["x"], "y": round(float(follower_mtm_max_dd), 2)} for p in pnl_points]
        if pnl_points and not mtm_dd_points and pd.notna(follower_mtm_max_dd) and follower_mtm_max_dd < 0
        else []
    )
    result = {
        **_shared,
        "max_dd_fmt": _fmt(portfolio_dd, "${:,.2f}"),
        "labels_json": Markup(json.dumps(labels)),
        "values_json": Markup(json.dumps(values)),
        "mtm_dd_values_json": Markup(json.dumps(mtm_dd_values)),
        "mtm_marker_values_json": Markup(json.dumps(mtm_marker_values)),
        "pnl_points_json": Markup(json.dumps(pnl_points)),
        "mtm_dd_points_json": Markup(json.dumps(mtm_dd_points)),
        "mtm_marker_points_json": Markup(json.dumps(mtm_marker_points)),
        "has_data": bool(values),
    }
    _portfolio_cache[cache_key] = result
    return result


# ── Singleton guard (Windows named mutex) ───────────────────────────────────
# Prevents the split-brain bug where multiple uvicorn workers each hold their
# own in-memory _state and overwrite wallet_finder_state.json on every save.
import ctypes
from ctypes import wintypes

_MUTEX_NAME = "Global\\WalletFinderAppSingleton"
_kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

_CreateMutexW = _kernel32.CreateMutexW
_CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
_CreateMutexW.restype = wintypes.HANDLE

_GetLastError = _kernel32.GetLastError

_mutex = _CreateMutexW(None, False, _MUTEX_NAME)
if not _mutex or _GetLastError() == 183:  # ERROR_ALREADY_EXISTS
    print("[singleton] Another Wallet Finder instance is already running on this machine. Exiting.", flush=True)
    sys.exit(1)

# ── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(title="Wallet Finding Engine")
EXPORT_PATH = BASE_DIR / "copycandidates.txt"


@app.on_event("startup")
async def startup() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    _load_state()
    load_universe(preserve_selection=True)


def _selected_wallets_ordered() -> list[str]:
    selected = {str(w).strip().lower() for w, v in _state.get("selected", {}).items() if v}
    ordered = []
    seen = set()
    for wallet in _state.get("wallet_order", []):
        wallet = str(wallet).strip().lower()
        if wallet and wallet in selected and wallet not in seen:
            ordered.append(wallet)
            seen.add(wallet)
    return ordered


def _wallet_text(wallets: list[str]) -> str:
    return "\n".join(wallets) + ("\n" if wallets else "")


@app.get("/selected_wallets.txt", response_class=PlainTextResponse)
async def selected_wallets_txt() -> PlainTextResponse:
    return PlainTextResponse(_wallet_text(_selected_wallets_ordered()))


@app.get("/selected_wallets.json")
async def selected_wallets_json() -> JSONResponse:
    wallets = _selected_wallets_ordered()
    return JSONResponse({"count": len(wallets), "wallets": wallets})


@app.get("/visible_wallets.txt", response_class=PlainTextResponse)
async def visible_wallets_txt(sort: str = "compound_score", dir: str = "desc") -> PlainTextResponse:
    visible = _get_visible_universe(sort, dir)
    wallets = [] if visible.empty else [str(w).strip().lower() for w in visible["wallet"].tolist()]
    return PlainTextResponse(_wallet_text(wallets))


@app.get("/proof_wallets.txt", response_class=PlainTextResponse)
async def proof_wallets_txt() -> PlainTextResponse:
    return PlainTextResponse(_wallet_text(_selected_wallets_ordered()))


@app.post("/export_selected")
async def export_selected():
    wallets = _selected_wallets_ordered()
    try:
        EXPORT_PATH.write_text(_wallet_text(wallets), encoding="utf-8")
        return {"ok": True, "count": len(wallets)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, sort: str | None = None, dir: str | None = None, page: int | None = None, per_page: int | None = None) -> HTMLResponse:
    sort_val = sort or _state.get("sort", "compound_score")
    dir_val = dir or _state.get("dir", "desc")
    page_val = page or _state.get("page", 1)
    per_page_val = per_page or _state.get("per_page", 100)
    table_ctx = await asyncio.to_thread(_table_context, sort_val, dir_val, page_val, per_page_val)
    portfolio = await _get_portfolio_data_async(_state.get("days", "30"))
    html = render(
        "index.html",
        **table_ctx,
        portfolio=portfolio,
        wallet_order=_state.get("wallet_order", []),
        wallet_weights=_state.get("wallet_weights", {}),
        filters=_state.get("filters", {}),
        hide_dormant=_state.get("hide_dormant", False),
        activity_counts=_activity_counts(),
    )
    return HTMLResponse(html)


@app.get("/table", response_class=HTMLResponse)
async def table_partial(request: Request, sort: str | None = None, dir: str | None = None, page: int | None = None, per_page: int | None = None) -> HTMLResponse:
    sort_val = sort or _state.get("sort", "compound_score")
    dir_val = dir or _state.get("dir", "desc")
    page_val = page or _state.get("page", 1)
    per_page_val = per_page or _state.get("per_page", 100)
    _state["page"] = page_val
    _state["per_page"] = per_page_val
    html = _render_table_html(sort_val, dir_val, page_val, per_page_val)
    _save_state()
    return HTMLResponse(html)


_portfolio_compute_lock: asyncio.Lock | None = None
_last_trade_refresh_lock: asyncio.Lock | None = None


async def _get_portfolio_data_async(days: str) -> dict:
    """Keep the multi-gigabyte period scan off the web server's event loop."""
    global _portfolio_compute_lock
    if _portfolio_compute_lock is None:
        _portfolio_compute_lock = asyncio.Lock()
    async with _portfolio_compute_lock:
        return await asyncio.to_thread(get_portfolio_data, days)


def _get_last_trade_lock() -> asyncio.Lock:
    """Shared by /refresh and the SSE stream: both rebind _state['universe'],
    so without this a push could write a stale frame over a fresh reload."""
    global _last_trade_refresh_lock
    if _last_trade_refresh_lock is None:
        _last_trade_refresh_lock = asyncio.Lock()
    return _last_trade_refresh_lock


@app.get("/portfolio-panel", response_class=HTMLResponse)
async def portfolio_panel_initial() -> HTMLResponse:
    days = _state.get("days", "30")
    portfolio = await _get_portfolio_data_async(days)
    return HTMLResponse(_render_portfolio_data(portfolio, oob=False))


@app.get("/portfolio", response_class=HTMLResponse)
async def portfolio_panel(request: Request, days: str = "30") -> HTMLResponse:
    if days not in ("7", "30", "90", "all"):
        days = "30"
    _state["days"] = days
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(_state.get("sort", "compound_score"), _state.get("dir", "desc")))


@app.post("/select_wallet", response_class=HTMLResponse)
async def select_wallet(request: Request, wallet: str = Form(), chk: str = Form(default="")) -> HTMLResponse:
    wallet = wallet.strip().lower()
    selected = (chk == "on")
    _state["selected"][wallet] = selected
    _state["_selection_dirty"] = True
    if selected:
        if wallet not in _state["wallet_order"]:
            _state["wallet_order"].append(wallet)
        _state["wallet_weights"].setdefault(wallet, 1.0)
    else:
        _state["wallet_order"] = [w for w in _state["wallet_order"] if w != wallet]
        _state["wallet_weights"].pop(wallet, None)
    portfolio_html = _render_portfolio_html(oob=False)
    _save_state()
    return HTMLResponse(portfolio_html)


@app.post("/select_all", response_class=HTMLResponse)
async def select_all(request: Request, sort: str | None = Form(None), dir: str | None = Form(None)) -> HTMLResponse:
    sort, dir = _get_sort(sort or _state.get("sort", "compound_score"), dir or _state.get("dir", "desc"))
    visible = _get_visible_universe(sort, dir)
    for w in visible["wallet"] if not visible.empty else []:
        _state["selected"][w] = True
        if w not in _state["wallet_order"]:
            _state["wallet_order"].append(w)
        _state["wallet_weights"].setdefault(w, 1.0)
    _state["_selection_dirty"] = True
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(sort, dir))


@app.post("/clear_all", response_class=HTMLResponse)
async def clear_all(request: Request, sort: str | None = Form(None), dir: str | None = Form(None)) -> HTMLResponse:
    sort, dir = _get_sort(sort or _state.get("sort", "compound_score"), dir or _state.get("dir", "desc"))
    for w in _state["universe"]["wallet"]:
        _state["selected"][w] = False
    _state["wallet_order"] = []
    _state["wallet_weights"] = {}
    _state["_allow_empty_selection_save"] = True
    _state["_selection_dirty"] = True
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(sort, dir))


@app.post("/refresh", response_class=HTMLResponse)
async def refresh(request: Request, sort: str | None = Form(None), dir: str | None = Form(None)) -> HTMLResponse:
    async with _get_last_trade_lock():
        await asyncio.to_thread(load_universe, True)
    html = await asyncio.to_thread(
        _table_plus_portfolio_oob,
        sort or _state.get("sort", "compound_score"),
        dir or _state.get("dir", "desc"),
    )
    return HTMLResponse(html)


# How often the server re-stats the trade ledger while an SSE client is
# attached. This is a stat() on one file, not a data read: the incremental
# index only runs when the mtime actually moves.
LAST_TRADE_WATCH_S = 5.0


def _last_trade_cells_snapshot() -> dict[str, dict]:
    """Current Last Trade payload for every wallet in the loaded universe."""
    universe = _state.get("universe")
    if universe is None or universe.empty or "last_trade_time" not in universe.columns:
        return {}
    return {
        str(wallet): last_trade_cell(ts)
        for wallet, ts in zip(universe["wallet"], universe["last_trade_time"])
    }


@app.get("/events/last-trade")
async def last_trade_events(request: Request) -> StreamingResponse:
    """Push Last Trade updates to the browser as the ledger grows.

    Server-sent events rather than a client refresh timer: the page holds one
    connection open and receives a message only when a value actually changes.
    Each update runs the incremental tail index (resumed from a byte cursor),
    not the full universe reload behind the Refresh Data button.
    """

    async def stream():
        sent: dict[str, dict] = {}
        last_mtime_ns = -1
        while True:
            if await request.is_disconnected():
                break
            try:
                path = _find_all_trades_path()
                mtime_ns = path.stat().st_mtime_ns if path is not None else -1
                ledger_grew = mtime_ns != last_mtime_ns
                # The cell renders an age against wall clock, so it has to be
                # re-derived every tick even when no new fills landed. Only the
                # ledger re-index is gated on the file actually changing --
                # gating both is what previously froze the column whenever
                # universe_builder went quiet (e.g. its whole cold start).
                async with _get_last_trade_lock():
                    if ledger_grew:
                        if last_mtime_ns != -1:
                            await asyncio.to_thread(refresh_last_trade_overlay)
                        last_mtime_ns = mtime_ns
                    current = await asyncio.to_thread(_last_trade_cells_snapshot)
                delta = {w: c for w, c in current.items() if sent.get(w) != c}
                if delta:
                    sent.update(delta)
                    yield f"data: {json.dumps(delta)}\n\n"
                else:
                    yield ": keepalive\n\n"
            except Exception as exc:  # never let one bad read kill the stream
                log.warning("last-trade stream iteration failed: %s", exc)
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


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"ok": True, "port": WALLET_FINDER_PORT})


@app.get("/readyz")
async def readyz() -> JSONResponse:
    universe = _state.get("universe")
    ready = universe is not None and not universe.empty
    return JSONResponse(
        {"ok": ready, "wallets": 0 if not ready else len(universe)},
        status_code=200 if ready else 503,
    )


def _parse_filter(v: str, cast=float):
    v = (v or "").strip()
    if not v:
        return None
    try:
        return cast(v)
    except (ValueError, TypeError):
        return None


def _reset_table_page() -> None:
    _state["page"] = 1


@app.post("/filter", response_class=HTMLResponse)
async def apply_filter(
    request: Request, sort: str | None = Form(None), dir: str | None = Form(None),
    min_trades: str = Form(""), max_dd_pct: str = Form(""), max_slip_deg_03: str = Form(""),
    min_score: str = Form(""), min_pnl: str = Form(""), min_risk_pnl: str = Form(""),
) -> HTMLResponse:
    _state["filters"] = {
        k: v for k, v in {
            "min_trades": _parse_filter(min_trades), "max_dd_pct": _parse_filter(max_dd_pct),
            "max_slip_deg_03": _parse_filter(max_slip_deg_03), "min_score": _parse_filter(min_score),
            "min_pnl": _parse_filter(min_pnl), "min_risk_pnl": _parse_filter(min_risk_pnl),
        }.items() if v is not None
    }
    _reset_table_page()
    sort, dir = _get_sort(sort or _state.get("sort", "compound_score"), dir or _state.get("dir", "desc"))
    _save_state()
    return HTMLResponse(_table_plus_filter_oob(sort, dir))


@app.post("/filter_clear", response_class=HTMLResponse)
async def clear_filter(request: Request) -> HTMLResponse:
    _state["filters"] = {}
    _reset_table_page()
    _save_state()
    return HTMLResponse(_table_plus_filter_oob(_state.get("sort", "compound_score"), _state.get("dir", "desc")))


@app.get("/normalize", response_class=HTMLResponse)
async def toggle_normalize(request: Request, v: str = "1") -> HTMLResponse:
    _state["normalize"] = (v == "1")
    _reset_table_page()
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(_state.get("sort", "compound_score"), _state.get("dir", "desc")))


@app.get("/risk", response_class=HTMLResponse)
async def set_risk_get(request: Request, r: str = "100") -> HTMLResponse:
    try:
        v = float(r)
        if v > 0:
            _state["risk_per_wallet"] = v
    except (ValueError, TypeError):
        pass
    _reset_table_page()
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(_state.get("sort", "compound_score"), _state.get("dir", "desc")))


@app.get("/mode", response_class=HTMLResponse)
async def set_mode(request: Request, m: str = "raw") -> HTMLResponse:
    if m not in _VALID_MODES:
        m = "raw"
    _state["mode"] = m
    _reset_table_page()
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(_state.get("sort", "compound_score"), _state.get("dir", "desc")))


@app.get("/set_date_range", response_class=HTMLResponse)
async def set_date_range(request: Request, start: str = "", end: str = "", start_date: str = "", end_date: str = "") -> HTMLResponse:
    _state["start_date"] = (start_date or start).strip() or None
    _state["end_date"] = (end_date or end).strip() or None
    _reset_table_page()
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(_state.get("sort", "compound_score"), _state.get("dir", "desc")))


@app.get("/toggle_dormant", response_class=HTMLResponse)
async def toggle_dormant(request: Request) -> HTMLResponse:
    _state["hide_dormant"] = not _state.get("hide_dormant", False)
    _reset_table_page()
    _save_state()
    sort = _state.get("sort", "compound_score")
    dir = _state.get("dir", "desc")
    return HTMLResponse(_table_plus_filter_oob(sort, dir))


@app.get("/unselect_wallet", response_class=HTMLResponse)
async def unselect_wallet(request: Request, wallet: str = "") -> HTMLResponse:
    wallet = wallet.strip().lower()
    _state["selected"][wallet] = False
    _state["_selection_dirty"] = True
    _state["wallet_order"] = [w for w in _state["wallet_order"] if w != wallet]
    _state["wallet_weights"].pop(wallet, None)
    _save_state()
    return HTMLResponse(_table_plus_portfolio_oob(_state.get("sort", "compound_score"), _state.get("dir", "desc")))


@app.get("/data-status")
async def data_status() -> JSONResponse:
    """Diagnostic endpoint showing data directory, file sizes, row counts, and MTM coverage."""
    import os, csv, glob, time as _time
    data_dir = str(BASE_DIR / "data")
    result = {
        "ok": True,
        "dashboard": "PROVING ENGINE",
        "port": WALLET_FINDER_PORT,
        "data_directory": data_dir,
        "data_directory_exists": os.path.isdir(data_dir),
        "files": {},
        "summary": {},
        "timestamp_utc": int(_time.time()),
    }
    # Core data files
    for name, path in [
        ("summary.csv", SUMMARY_PATH),
        ("wallet_universe.csv", UNIVERSE_PATH),
        ("wallet_gate.json", BASE_DIR / "data" / "wallet_gate.json"),
        ("wallet_finder_state.json", STATE_FILE),
        ("ui_state.json (copy app)", COPY_STATE_FILE),
        ("all_trades.csv", BASE_DIR / "data" / "all_trades.csv"),
        ("all_trades.zip", BASE_DIR / "data" / "all_trades.zip"),
    ]:
        info = {"exists": path.exists()}
        if path.exists():
            info["size_bytes"] = path.stat().st_size
            info["size_mb"] = round(path.stat().st_size / 1_048_576, 2)
            info["modified_utc"] = int(path.stat().st_mtime)
            # Row counts for CSV files
            if path.suffix == ".csv":
                try:
                    with open(path, "r", encoding="utf-8-sig") as f:
                        info["rows"] = sum(1 for _ in f) - 1  # exclude header
                except Exception:
                    info["rows"] = -1
        result["files"][name] = info
    # Equity curves
    curves_dir = CURVE_PATH
    curve_files = glob.glob(str(curves_dir / "*.csv")) if curves_dir.exists() else []
    result["files"]["equity_curves/*.csv"] = {"exists": len(curve_files) > 0, "count": len(curve_files)}
    # MTM coverage
    if SUMMARY_PATH.exists():
        try:
            with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
                r = csv.DictReader(f)
                cols = r.fieldnames or []
                rows = list(r)
            mtm_real = sum(1 for row in rows if row.get("mtm_source", "") == "hl_portfolio_api")
            mtm_stale = sum(1 for row in rows if row.get("mtm_source", "") == "cache_stale")
            mtm_none = sum(1 for row in rows if not row.get("mtm_source", "").strip() or row.get("mtm_source") == "unavailable")
            has_mtm_col = "max_drawdown_mtm" in cols
            result["summary"] = {
                "total_wallets": len(rows),
                "total_columns": len(cols),
                "mtm_column_present": has_mtm_col,
                "mtm_real": mtm_real,
                "mtm_stale": mtm_stale,
                "mtm_unavailable": mtm_none,
                "mtm_coverage_pct": round(mtm_real / max(len(rows), 1) * 100, 1),
            }
        except Exception as e:
            result["summary"] = {"error": str(e)}
    # Universe stats
    if UNIVERSE_PATH.exists() and UNIVERSE_PATH.stat().st_size > 0:
        try:
            with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
                ur = csv.DictReader(f)
                urows = list(ur)
            result["universe"] = {
                "total_wallets": len(urows),
                "columns": ur.fieldnames if ur.fieldnames else [],
            }
        except Exception as e:
            result["universe"] = {"error": str(e)}
    # Loaded state
    result["loaded_state"] = {
        "universe_rows_loaded": len(_state.get("universe", pd.DataFrame())),
        "selected_wallets": len([w for w, v in _state.get("selected", {}).items() if v]),
        "mode": _state.get("mode", "raw"),
        "sort": _state.get("sort", "compound_score"),
        "filters_active": len(_state.get("filters", {})),
    }
    return JSONResponse(result)



@app.get("/data-status-card", response_class=HTMLResponse)
async def data_status_card() -> HTMLResponse:
    """HTMX card showing MTM coverage, file counts, last-modified times."""
    import csv as _csv, os as _os, time as _time
    summary_path = SUMMARY_PATH
    universe_path = BASE_DIR / "data" / "wallet_universe.csv"
    curves_dir = BASE_DIR / "data" / "equity_curves"

    # Gather stats
    summary_rows = 0
    mtm_real = 0
    summary_modified = ""
    if summary_path.exists():
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                r = _csv.DictReader(f)
                rows = list(r)
            summary_rows = len(rows)
            mtm_real = sum(1 for row in rows if row.get("mtm_source","") == "hl_portfolio_api")
            summary_modified = _time.strftime("%Y-%m-%d %H:%M UTC", _time.gmtime(summary_path.stat().st_mtime))
        except Exception:
            pass

    universe_rows = 0
    universe_modified = ""
    if universe_path.exists():
        universe_rows = sum(1 for _ in open(universe_path, "r", encoding="utf-8-sig")) - 1
        universe_modified = _time.strftime("%Y-%m-%d %H:%M UTC", _time.gmtime(universe_path.stat().st_mtime))

    curve_count = len([f for f in _os.listdir(str(curves_dir)) if f.endswith(".csv")]) if curves_dir.exists() else 0

    mtm_pct = round(mtm_real / max(summary_rows, 1) * 100, 1)
    mtm_color = "#3fb950" if mtm_pct >= 30 else ("#d29922" if mtm_pct >= 10 else "#f85149")
    replay_meta = _state.get("trade_replay_snapshot", {}) or {}
    replay_exact = bool(replay_meta.get("exact"))
    replay_source_ns = int(replay_meta.get("source_mtime_ns") or 0)
    replay_as_of = (
        _time.strftime("%Y-%m-%d %H:%M:%S UTC", _time.gmtime(replay_source_ns / 1_000_000_000))
        if replay_source_ns > 0 else "not loaded"
    )
    replay_path = _find_all_trades_path()
    replay_lag_bytes = (
        max(0, replay_path.stat().st_size - int(replay_meta.get("source_size") or 0))
        if replay_path is not None and replay_meta else 0
    )
    replay_label = "Exact checkpoint" if replay_exact else "Checkpoint pending"
    replay_color = "#3fb950" if replay_exact else "#d29922"

    html = f"""<div id="data-health-card" style="background:#161b22; border:1px solid #30363d; border-radius:6px; padding:10px 16px; margin-bottom:12px; font-size:12px; display:flex; gap:24px; flex-wrap:wrap; align-items:center;">
  <div style="display:flex;align-items:center;gap:6px;">
    <span style="color:#6e7681;text-transform:uppercase;font-size:10px;">Data Health</span>
    <span style="color:#3fb950;font-size:10px;">â—</span>
  </div>
  <div style="display:flex;align-items:center;gap:4px;">
    <span style="color:#8b949e;">MTM:</span>
    <span style="color:{mtm_color};font-weight:600;">{mtm_pct}%</span>
    <span style="color:#6e7681;">({mtm_real}/{summary_rows})</span>
  </div>
  <div style="display:flex;align-items:center;gap:4px;">
    <span style="color:#8b949e;">Summary:</span>
    <span style="color:#c9d1d9;">{summary_rows:,} rows</span>
    <span style="color:#6e7681;font-size:10px;">{summary_modified}</span>
  </div>
  <div style="display:flex;align-items:center;gap:4px;">
    <span style="color:#8b949e;">Universe:</span>
    <span style="color:#c9d1d9;">{universe_rows:,} wallets</span>
  </div>
  <div style="display:flex;align-items:center;gap:4px;">
    <span style="color:#8b949e;">Curves:</span>
    <span style="color:#c9d1d9;">{curve_count:,}</span>
  </div>
  <div style="display:flex;align-items:center;gap:4px;" title="Execution-mode PnL/filter checkpoint; Last Trade refreshes independently from the live ledger.">
    <span style="color:#8b949e;">Replay:</span>
    <span style="color:{replay_color};font-weight:600;">{replay_label}</span>
    <span style="color:#6e7681;font-size:10px;">{replay_as_of} · lag {replay_lag_bytes / 1_048_576:.2f} MB</span>
  </div>
  <div style="margin-left:auto;">
    <a href="/data-status" style="color:#58a6ff;font-size:10px;text-decoration:none;" target="_blank">JSON â†’</a>
  </div>
</div>"""
    return HTMLResponse(html)


@app.get("/set_weight")
async def set_weight(wallet: str = "", weight: float = 1.0) -> JSONResponse:
    wallet = wallet.strip().lower()
    if wallet in _state["wallet_weights"]:
        _state["wallet_weights"][wallet] = max(0.0, weight)
    _save_state()
    return JSONResponse({"ok": True})


@app.post("/reorder_portfolio")
async def reorder_portfolio(request: Request) -> JSONResponse:
    data = await request.json()
    new_order = data.get("order", [])
    valid = set(_state["wallet_weights"].keys())
    _state["wallet_order"] = [w for w in new_order if w in valid]
    _save_state()
    return JSONResponse({"ok": True})


# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _render_portfolio_data(portfolio: dict, oob: bool) -> str:
    return render("partials/portfolio.html", portfolio=portfolio,
                  wallet_order=_state.get("wallet_order", []), wallet_weights=_state.get("wallet_weights", {}), oob=oob)


def _render_portfolio_html(oob: bool) -> str:
    return _render_portfolio_data(get_portfolio_data(_state.get("days", "30")), oob=oob)


def _table_plus_portfolio_oob(sort: str = "compound_score", dir: str = "desc") -> str:
    sort, dir = _get_sort(sort, dir)
    table_html = _render_table_html(sort, dir)
    portfolio_html = _render_portfolio_html(oob=True)
    filter_html = _render_filter_bar(oob=True)
    return table_html + "\n" + portfolio_html + "\n" + filter_html


def _table_plus_filter_oob(sort: str = "compound_score", dir: str = "desc") -> str:
    sort, dir = _get_sort(sort, dir)
    table_html = _render_table_html(sort, dir)
    filter_html = _render_filter_bar(oob=True)
    return table_html + "\n" + filter_html


def _render_filter_bar(oob: bool = False) -> str:
    filters = _state.get("filters", {})
    active_filter_count = _active_filter_count()
    return _jinja.from_string("""
<div id="filter-bar-container"{% if oob %} hx-swap-oob="outerHTML"{% endif %}>
  <form id="filter-form" class="filter-bar">
    <span class="filter-label">Filters</span>
    <label class="filter-field">Min Trades
      <input type="number" name="min_trades" class="filter-input{% if filters.get('min_trades') is not none %} active-filter{% endif %}"
             value="{{ filters.get('min_trades', '') }}" placeholder="any" min="0" step="1">
    </label>
    <label class="filter-field">Max DD %
      <input type="number" name="max_dd_pct" class="filter-input{% if filters.get('max_dd_pct') is not none %} active-filter{% endif %}"
             value="{{ filters.get('max_dd_pct', '') }}" placeholder="e.g. 30" min="0" step="0.1">
    </label>
    <label class="filter-field">Max Slip%
      <input type="number" name="max_slip_deg_03" class="filter-input{% if filters.get('max_slip_deg_03') is not none %} active-filter{% endif %}"
             value="{{ filters.get('max_slip_deg_03', '') }}" placeholder="any" min="0" step="0.1">
    </label>
    <label class="filter-field">Min CScore
      <input type="number" name="min_score" class="filter-input{% if filters.get('min_score') is not none %} active-filter{% endif %}"
             value="{{ filters.get('min_score', '') }}" placeholder="any" step="0.1">
    </label>
    <label class="filter-field">Min PnL ($)
      <input type="number" name="min_pnl" class="filter-input{% if filters.get('min_pnl') is not none %} active-filter{% endif %}"
             value="{{ filters.get('min_pnl', '') }}" placeholder="any">
    </label>
    <label class="filter-field">Min PnL/DD
      <input type="number" name="min_risk_pnl" class="filter-input{% if filters.get('min_risk_pnl') is not none %} active-filter{% endif %}"
             value="{{ filters.get('min_risk_pnl', '') }}" placeholder="any" step="0.001">
    </label>
    <button type="button" class="btn btn-sm btn-primary"
            hx-post="/filter" hx-include="#filter-form" hx-target="#wallet-table" hx-swap="outerHTML">Apply</button>
    <button type="button" class="btn btn-sm"
            hx-post="/filter_clear" hx-target="#wallet-table" hx-swap="outerHTML">Clear</button>
    {% if active_filter_count %}
    <span class="filter-active-badge">{{ active_filter_count }} filter{{ "s" if active_filter_count != 1 else "" }} active</span>
    {% endif %}
    <button type="button" class="btn btn-sm{% if hide_dormant %} btn-active{% endif %}"
            hx-get="/toggle_dormant" hx-target="#wallet-table" hx-swap="outerHTML"
            style="margin-left:8px"
            title="{% if hide_dormant %}Hiding dormant wallets (last trade >15d ago){% else %}Hide dormant wallets (last trade >15d ago){% endif %}">
      {% if hide_dormant %}&#9679; Dormant Hidden{% else %}&#9675; Hide Dormant{% endif %}
    </button>
    {% if activity_counts.dormant or activity_counts.stale or activity_counts.slow %}<span class="filter-active-badge" style="color:rgba(201,209,217,0.9);background:rgba(22,27,34,0.8);border-color:rgba(48,54,61,0.6);font-size:11px"><span style="color:#3fb950">{{ activity_counts.active }} active</span> &middot; <span style="color:#d29922">{{ activity_counts.slow }} slow</span> &middot; <span style="color:#f0883e">{{ activity_counts.stale }} stale</span> &middot; <span style="color:#f85149">{{ activity_counts.dormant }} dormant</span></span>{% endif %}
  </form>
</div>
    """).render(filters=filters, active_filter_count=active_filter_count, oob=oob, hide_dormant=_state.get("hide_dormant", False), activity_counts=_activity_counts())
