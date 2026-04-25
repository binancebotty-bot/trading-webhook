"""
HL_Copy_App.py — Hyperliquid Copy Trading Live Dashboard
Reads: live_state.json, live_wallet_metrics.csv, copy_trades.csv
Serves: sortable equity table, wallet detail, equity curves, slippage/latency buckets.
"""

import json
import math
import os
import shutil
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "hl_copy_output"
EQUITY_CONFIG = DATA_DIR / "equity_config.json"
WALLET_GATE_FILE = BASE_DIR / "wallet_gate.json"  # persists outside hl_copy_output
PORTFOLIO_HISTORY_FILE = DATA_DIR / "portfolio_history.json"
PORTFOLIO_BASELINE_FILE = DATA_DIR / "portfolio_baseline.json"
EQUITY_HISTORY_FILE = DATA_DIR / "equity_history.json"
GLOBAL_NORM_CONFIG = DATA_DIR / "global_norm.json"
GLOBAL_NORM_DEFAULT = 100.0

EQUITY_HISTORY_MAX = 200
_UW_HARD = "0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205"
USER_WALLET: str = os.getenv("HL_USER_WALLET", "").lower() or _UW_HARD

# In-memory equity history per wallet: deque of [iso_ts, pnl]
_equity_history: dict[str, deque] = {}
_active_wallets: set[str] = set()
_portfolio_history: list = []
print("APP USER WALLET =", USER_WALLET)


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    return num if math.isfinite(num) else default


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, path)


def _purge_app_history_files() -> None:
    for p in (
        PORTFOLIO_HISTORY_FILE,
        PORTFOLIO_BASELINE_FILE,
        EQUITY_HISTORY_FILE,
        GLOBAL_NORM_CONFIG,
    ):
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def _normalize_portfolio_history(history: Any) -> list[dict[str, float | str]]:
    if not isinstance(history, list):
        return []
    normalized: list[dict[str, float | str]] = []
    prev_ts = ""
    prev_peak = None
    prev_alloc = None
    for item in history:
        if not isinstance(item, dict):
            return []
        ts = str(item.get("ts", "") or "")
        if not ts:
            return []
        alloc = _finite_float(item.get("alloc", 0.0))
        realized = _finite_float(item.get("realized"))
        unrealized = _finite_float(item.get("unrealized"))
        equity = _finite_float(item.get("equity"))
        peak_equity = _finite_float(item.get("peak_equity"))
        drawdown_usd = _finite_float(item.get("drawdown_usd"))
        drawdown_pct = _finite_float(item.get("drawdown_pct"))
        if prev_ts and ts <= prev_ts:
            return []
        if prev_peak is not None and peak_equity < prev_peak:
            return []
        if prev_alloc is not None and prev_alloc > 0.0 and alloc > 0.0:
            ratio = max(prev_alloc, alloc) / min(prev_alloc, alloc)
            if ratio > 5.0:
                return []
        if alloc < 0.0:
            return []
        if abs(equity - (alloc + realized + unrealized)) > 1e-6:
            return []
        if drawdown_usd < 0.0 or drawdown_pct < 0.0 or drawdown_pct > 100.0 or peak_equity < equity:
            return []
        if peak_equity > 0.0 and abs(drawdown_usd - max(0.0, peak_equity - equity)) > 1e-6:
            return []
        prev_ts = ts
        prev_peak = peak_equity
        prev_alloc = alloc
        normalized.append({
            "ts": ts,
            "alloc": alloc,
            "realized": realized,
            "unrealized": unrealized,
            "equity": equity,
            "peak_equity": peak_equity,
            "drawdown_usd": drawdown_usd,
            "drawdown_pct": drawdown_pct,
        })
    return normalized


if PORTFOLIO_HISTORY_FILE.exists():
    try:
        _portfolio_history = _normalize_portfolio_history(
            json.loads(PORTFOLIO_HISTORY_FILE.read_text(encoding="utf-8"))
        )
        _portfolio_base = {}
        if not _portfolio_history:
            print("[PORTFOLIO_HISTORY_RESET] reason=invalid_history")
            _purge_app_history_files()
    except Exception:
        print("[PORTFOLIO_HISTORY_RESET] reason=load_exception")
        _portfolio_history = []
        _portfolio_base = {}
        _purge_app_history_files()

if EQUITY_HISTORY_FILE.exists():
    try:
        _loaded_hist = json.loads(EQUITY_HISTORY_FILE.read_text(encoding="utf-8"))
        for _w, _pts in _loaded_hist.items():
            _equity_history[_w] = deque(_pts, maxlen=EQUITY_HISTORY_MAX)
    except Exception:
        pass

_equity_lock = threading.Lock()
app = FastAPI(title="HL Copy Dashboard")


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Global normalisation config
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _load_norm_base() -> float:
    try:
        if GLOBAL_NORM_CONFIG.exists():
            return float(json.loads(GLOBAL_NORM_CONFIG.read_text(encoding="utf-8"))["norm_base"])
    except Exception:
        pass
    return GLOBAL_NORM_DEFAULT


def _save_norm_base(value: float) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = GLOBAL_NORM_CONFIG.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"norm_base": round(value, 2)}), encoding="utf-8")
        os.replace(str(tmp), str(GLOBAL_NORM_CONFIG))
    except Exception:
        pass


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Wallet gate helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _load_wallet_gate() -> dict:
    try:
        if WALLET_GATE_FILE.exists():
            return json.loads(WALLET_GATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_wallet_gate(gate: dict) -> None:
    try:
        tmp = WALLET_GATE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(gate, indent=2), encoding="utf-8")
        os.replace(str(tmp), str(WALLET_GATE_FILE))
    except Exception:
        pass


def _ensure_gate_population() -> dict:
    gate = _load_wallet_gate()
    universe = _wallet_universe_from_state()
    uw = USER_WALLET.lower() if USER_WALLET else None
    changed = False
    for w in universe:
        if w not in gate:
            gate[w] = {"mode": "OFF", "off_mode": None}
            changed = True
        if uw and w == uw:
            if gate[w].get("mode") != "OFF":
                gate[w] = {"mode": "OFF", "off_mode": None}
                changed = True
    if changed:
        _save_wallet_gate(gate)
    return gate


def _wallet_universe_from_state() -> list[str]:
    wallets: set[str] = set()
    state = _load_state()
    for wallet in (state.get("wallets", {}) or {}).keys():
        wallet = str(wallet).strip().lower()
        if wallet:
            wallets.add(wallet)
    for wallet in _load_wallet_gate().keys():
        wallet = str(wallet).strip().lower()
        if wallet:
            wallets.add(wallet)
    return sorted(wallets)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Data loaders
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _load_state() -> dict:
    p = DATA_DIR / "live_state.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_user_snapshot() -> dict:
    p = DATA_DIR / "user_snapshot.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _update_equity_history(state: dict) -> None:
    now_iso = str(state.get("updated_at") or datetime.now(timezone.utc).isoformat())
    with _equity_lock:
        wallet_state = state.get("normalised_wallet_state") or {}
        for wallet, nstate in wallet_state.items():
            copy_state = nstate.get("copy", {}) if isinstance(nstate, dict) else {}
            realized = _finite_float(copy_state.get("realised"))
            unrealized = _finite_float(copy_state.get("unrealised"))
            print("[EQUITY_INGEST]", wallet, realized, unrealized)
            is_active = (
                int(nstate.get("entry_count") or 0) > 0
                or int(nstate.get("exit_count") or 0) > 0
                or int(nstate.get("open_position_count") or 0) > 0
            )
            if is_active:
                _active_wallets.add(wallet)
                if wallet not in _equity_history:
                    _equity_history[wallet] = deque(maxlen=EQUITY_HISTORY_MAX)
                _equity_history[wallet].append([now_iso, realized + unrealized])

        history = state.get("normalised_portfolio_history")
        if isinstance(history, list):
            _portfolio_history[:] = _normalize_portfolio_history(history)
        elif PORTFOLIO_HISTORY_FILE.exists():
            try:
                _portfolio_history[:] = _normalize_portfolio_history(
                    json.loads(PORTFOLIO_HISTORY_FILE.read_text(encoding="utf-8"))
                )
            except Exception:
                _portfolio_history[:] = []
        else:
            point = state.get("normalised_portfolio") or {}
            if isinstance(point, dict) and point:
                _portfolio_history[:] = _normalize_portfolio_history([point])
            else:
                _portfolio_history[:] = []

        portfolio_state = state.get("normalised_portfolio") or {}
        if isinstance(portfolio_state, dict) and isinstance(portfolio_state.get("copy"), dict):
            point = dict(portfolio_state.get("copy") or {})
            point.setdefault("ts", portfolio_state.get("ts", now_iso))
        else:
            point = dict(portfolio_state or {})
        if point:
            point.setdefault("ts", now_iso)
            if not _portfolio_history:
                _portfolio_history.append(point)
            elif str(_portfolio_history[-1].get("ts", "")) != str(point.get("ts", "")):
                _portfolio_history.append(point)
            else:
                _portfolio_history[-1] = point
            _portfolio_history[:] = _normalize_portfolio_history(_portfolio_history)

        if _portfolio_history:
            latest = _portfolio_history[-1]
            print(
                "[PORTFOLIO_DEBUG]",
                {
                    "wallets": [w for w in wallet_state if w.lower() != USER_WALLET.lower()],
                    "combined_alloc": latest.get("alloc", 0.0),
                    "combined_realized": latest.get("realized", 0.0),
                    "combined_unrealized": latest.get("unrealized", 0.0),
                    "combined_equity": latest.get("equity", 0.0),
                    "peak_equity": latest.get("peak_equity", 0.0),
                },
            )

        try:
            _atomic_write_json(PORTFOLIO_HISTORY_FILE, _portfolio_history)
        except Exception:
            pass
        try:
            hist_snap = {w: list(v) for w, v in _equity_history.items()}
            _atomic_write_json(EQUITY_HISTORY_FILE, hist_snap)
        except Exception:
            pass


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Formatting helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _fv(v, decimals: int = 2, prefix: str = "") -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    try:
        return f"{prefix}{float(v):,.{decimals}f}"
    except Exception:
        return "—"


def _fp(v, decimals: int = 1) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    try:
        return f"{float(v):.{decimals}f}%"
    except Exception:
        return "—"


def _cls(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "neu"
    return "pos" if float(v) >= 0 else "neg"


def _dv(v) -> str:
    """data-val attribute string for numeric sort."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    try:
        return str(float(v))
    except Exception:
        return ""


def _fdual_pnl(dollar: float, base: float) -> str:
    """$D (P%) — dollar first, then percentage of base."""
    try:
        sign = "+" if dollar >= 0 else ""
        pct = (dollar / base * 100) if base and abs(base) > 1e-9 else 0.0
        pct_sign = "+" if pct >= 0 else ""
        return f"{sign}${dollar:,.2f} ({pct_sign}{pct:.2f}%)"
    except Exception:
        return "—"


def _fdual_dd(pct: float, peak: float) -> str:
    """P% (-$D) — percent first, then peak-relative dollar amount."""
    try:
        dd_dollars = peak * pct / 100.0 if peak and abs(peak) > 1e-9 else 0.0
        return f"{pct:.2f}% (-${dd_dollars:,.2f})"
    except Exception:
        return "—"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Analytics helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _slip_buckets(df_w: pd.DataFrame) -> dict:
    b = {"0â€“5 bps": 0, "5â€“10 bps": 0, "10â€“20 bps": 0, "20+ bps": 0}
    if df_w.empty:
        return b
    for col in ["entry_slippage_bps", "exit_slippage_bps"]:
        if col not in df_w.columns:
            continue
        for v in df_w[col].dropna():
            v = float(v)
            if v <= 5:
                b["0â€“5 bps"] += 1
            elif v <= 10:
                b["5â€“10 bps"] += 1
            elif v <= 20:
                b["10â€“20 bps"] += 1
            else:
                b["20+ bps"] += 1
    return b


def _lat_buckets(df_w: pd.DataFrame) -> dict:
    b = {"<500 ms": 0, "500â€“1000 ms": 0, "1000â€“2000 ms": 0, "2000+ ms": 0}
    if df_w.empty or "entry_latency_ms" not in df_w.columns:
        return b
    for v in df_w["entry_latency_ms"].dropna():
        v = float(v)
        if v < 500:
            b["<500 ms"] += 1
        elif v < 1000:
            b["500â€“1000 ms"] += 1
        elif v < 2000:
            b["1000â€“2000 ms"] += 1
        else:
            b["2000+ ms"] += 1
    return b


def _bucket_bars_html(buckets: dict, color_class: str) -> str:
    total = max(sum(buckets.values()), 1)
    rows = []
    for label, count in buckets.items():
        pct = count / total * 100
        rows.append(
            f'<div class="bucket-row">'
            f'<span class="bucket-label">{label}</span>'
            f'<div class="bucket-track"><div class="bucket-fill {color_class}" style="width:{pct:.1f}%"></div></div>'
            f'<span class="bucket-count">{count}</span>'
            f'</div>'
        )
    return "\n".join(rows)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Shared CSS + JS
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0d1117;color:#e6edf3;font-size:13px}
a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}
h1,h2,h3{color:#f0f6fc}
.container{width:100%;max-width:100%;margin:0;padding:8px 12px}
.header{display:flex;align-items:center;gap:14px;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid #21262d}
.header h1{font-size:17px;font-weight:600}
.badge{background:#21262d;border:1px solid #30363d;border-radius:12px;padding:2px 10px;font-size:11px;color:#8b949e}
.badge.live{background:#0d2b1e;border-color:#2ea043;color:#3fb950}
.ml-auto{margin-left:auto}
table{width:100%;border-collapse:collapse;background:#161b22;border:1px solid #21262d;border-radius:6px;overflow:hidden}
th{background:#21262d;color:#8b949e;text-align:left;padding:8px 10px;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.4px;cursor:pointer;user-select:none;white-space:nowrap}
th:hover{color:#e6edf3;background:#2d333b}
th.sorted-asc::after{content:""}
th.sorted-desc::after{content:""}
td{padding:5px 8px;border-top:1px solid #21262d;color:#c9d1d9;white-space:nowrap;font-size:12px}
tr:hover td{background:#1e2530}
tr.row-pos{background:#0a1a10}
tr.row-neg{background:#1a0a0a}
tr.row-user td{background:#0a0f1f!important}
tr.row-user td:first-child{border-left:3px solid #58a6ff!important}
.badge-user{display:inline-block;background:#1a3a6e;color:#58a6ff;border:1px solid #2d5fa6;border-radius:3px;padding:1px 5px;font-size:10px;margin-left:4px;vertical-align:middle}
.group-divider{border-left:2px solid #30363d!important}
td.pnl-col{font-weight:500}
tr.row-pos td.pnl-col{color:#3fb950}
tr.row-neg td.pnl-col{color:#f85149}
.pos{color:#3fb950}.neg{color:#f85149}.neu{color:#8b949e}
.flag{display:inline-block;background:#2d1f0e;color:#e3b341;border:1px solid #4a3219;border-radius:3px;padding:1px 5px;font-size:10px;margin:1px}
.flag.red{background:#2d0e0e;color:#f85149;border-color:#4a1919}
.stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:10px;margin-bottom:16px}
.stat{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 12px}
.stat-label{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.stat-value{font-size:19px;font-weight:600;color:#e6edf3}
.stat-value.pos{color:#3fb950}.stat-value.neg{color:#f85149}
.section{margin-bottom:22px}
.section-title{font-size:11px;font-weight:600;color:#8b949e;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.chart-box{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:12px;margin-bottom:16px}
.chart-title{font-size:11px;color:#8b949e;margin-bottom:8px}
canvas{display:block;width:100%!important}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.bucket-row{display:flex;align-items:center;gap:8px;margin-bottom:5px}
.bucket-label{width:95px;font-size:11px;color:#8b949e;flex-shrink:0}
.bucket-track{flex:1;background:#21262d;border-radius:2px;height:14px;overflow:hidden}
.bucket-fill{height:100%;background:#1f6feb;border-radius:2px;transition:width .3s}
.bucket-fill.lat{background:#6e40c9}
.bucket-count{font-size:11px;color:#c9d1d9;width:28px;text-align:right}
.positions-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px;margin-bottom:16px}
.pos-card{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 12px}
.pos-coin{font-size:14px;font-weight:600;color:#f0f6fc;margin-bottom:6px}
.pos-row{display:flex;justify-content:space-between;margin-bottom:3px;font-size:11px}
.pos-key{color:#8b949e}.pos-val{color:#c9d1d9}
.back-link{display:block;margin-bottom:12px;font-size:12px}
@media(max-width:900px){.two-col{grid-template-columns:1fr}.stat-grid{grid-template-columns:repeat(2,1fr)}}
"""

_SORT_JS = """
function savePref(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch(e) {}
}
function loadPref(key, fallback) {
  try { const v = localStorage.getItem(key); return v !== null ? JSON.parse(v) : fallback; } catch(e) { return fallback; }
}
function openUserWallet(wallet) { window.open('/user-wallet/' + wallet, '_blank'); }
function sortTable(tableId, colIdx, numeric) {
  const tbl = document.getElementById(tableId);
  const tbody = tbl.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  const ths = tbl.querySelectorAll('th');
  let asc = ths[colIdx].dataset.dir !== 'asc';
  ths.forEach(t => { t.classList.remove('sorted-asc','sorted-desc'); delete t.dataset.dir; });
  ths[colIdx].classList.add(asc ? 'sorted-asc' : 'sorted-desc');
  ths[colIdx].dataset.dir = asc ? 'asc' : 'desc';
  rows.sort((a, b) => {
    const ca = a.cells[colIdx], cb = b.cells[colIdx];
    let va = ca ? (ca.dataset.val ?? ca.textContent.trim()) : '';
    let vb = cb ? (cb.dataset.val ?? cb.textContent.trim()) : '';
    if (numeric) { va = parseFloat(va); vb = parseFloat(vb); va = isNaN(va) ? -Infinity : va; vb = isNaN(vb) ? -Infinity : vb; }
    return asc ? (va < vb ? -1 : va > vb ? 1 : 0) : (va > vb ? -1 : va < vb ? 1 : 0);
  });
  rows.forEach(r => tbody.appendChild(r));
  if (tableId === 'wallets') {
    savePref('hlcopy.wallets.sort.col', colIdx);
    savePref('hlcopy.wallets.sort.dir', asc ? 'asc' : 'desc');
    savePref('hlcopy.wallets.sort.numeric', numeric);
  }
}
function restoreSort(tableId) {
  if (tableId !== 'wallets') return;
  const col     = loadPref('hlcopy.wallets.sort.col', null);
  const dir     = loadPref('hlcopy.wallets.sort.dir', null);
  const numeric = loadPref('hlcopy.wallets.sort.numeric', true);
  if (col === null || dir === null) return;
  const tbl = document.getElementById(tableId);
  if (!tbl) return;
  const ths = tbl.querySelectorAll('th');
  if (col >= ths.length) return;
  ths[col].dataset.dir = dir === 'asc' ? 'desc' : 'asc';
  sortTable(tableId, col, numeric);
}
const _MODE_STYLES = {
  ON:         {bg:'#0d2b1e', border:'#2ea043', color:'#3fb950'},
  CLOSE_ONLY: {bg:'#2d1f0e', border:'#e3b341', color:'#e3b341'},
  OFF:        {bg:'#2d0e0e', border:'#4a1919', color:'#f85149'},
};
function _applyModeStyle(sel, mode) {
  const s = _MODE_STYLES[mode] || _MODE_STYLES['OFF'];
  sel.style.background  = s.bg;
  sel.style.borderColor = s.border;
  sel.style.color       = s.color;
}
function setWalletMode(wallet, sel) {
  const mode = sel.value;
  _applyModeStyle(sel, mode);
  fetch('/api/set-wallet-mode', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({wallet: wallet, mode: mode})
  }).then(r => r.json()).then(d => {
    if (!d.ok) { alert('Mode error: ' + d.error); }
  }).catch(() => alert('Failed to set mode'));
}
function setAllModes(mode) {
  fetch('/api/set-all-modes', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({mode: mode})
  }).then(r => r.json()).then(d => {
    if (d.ok) {
      document.querySelectorAll('select[onchange^="setWalletMode"]').forEach(sel => {
        sel.value = mode; _applyModeStyle(sel, mode);
      });
    } else { alert('Bulk mode error: ' + d.error); }
  }).catch(() => alert('Failed to set all modes'));
}
"""

_CHART_JS = """
function drawLine(id, labels, values, color) {
  const c = document.getElementById(id); if (!c) return;
  const ctx = c.getContext('2d');
  const W = c.offsetWidth || c.width; c.width = W;
  const H = c.height;
  ctx.clearRect(0,0,W,H);
  if (!values || values.length < 2) {
    ctx.fillStyle='#8b949e'; ctx.font='12px sans-serif'; ctx.textAlign='center';
      ctx.fillText('Awaiting data…', W/2, H/2); return;
  }
  const pad = {t:8,r:8,b:20,l:52};
  const cw = W-pad.l-pad.r, ch = H-pad.t-pad.b;
  const mn = Math.min(...values), mx = Math.max(...values);
  const rng = mx-mn || 1;
  const tx = i => pad.l + (i/(values.length-1))*cw;
  const ty = v => pad.t + ch - ((v-mn)/rng)*ch;
  ctx.strokeStyle='#21262d'; ctx.lineWidth=1;
  for (let i=0;i<=4;i++) {
    const y=pad.t+(i/4)*ch;
    ctx.beginPath(); ctx.moveTo(pad.l,y); ctx.lineTo(pad.l+cw,y); ctx.stroke();
    ctx.fillStyle='#8b949e'; ctx.font='10px sans-serif'; ctx.textAlign='right';
    ctx.fillText((mn+((4-i)/4)*rng).toFixed(0), pad.l-3, y+3);
  }
  if (mn<0 && mx>0) {
    const zy=ty(0); ctx.strokeStyle='#30363d'; ctx.setLineDash([3,3]);
    ctx.beginPath(); ctx.moveTo(pad.l,zy); ctx.lineTo(pad.l+cw,zy); ctx.stroke();
    ctx.setLineDash([]);
  }
  const g=ctx.createLinearGradient(0,pad.t,0,pad.t+ch);
  g.addColorStop(0,color+'44'); g.addColorStop(1,color+'00');
  ctx.beginPath(); ctx.moveTo(tx(0),ty(values[0]));
  values.forEach((v,i)=>ctx.lineTo(tx(i),ty(v)));
  ctx.lineTo(tx(values.length-1),pad.t+ch); ctx.lineTo(tx(0),pad.t+ch);
  ctx.closePath(); ctx.fillStyle=g; ctx.fill();
  ctx.beginPath(); ctx.moveTo(tx(0),ty(values[0]));
  values.forEach((v,i)=>ctx.lineTo(tx(i),ty(v)));
  ctx.strokeStyle=color; ctx.lineWidth=1.5; ctx.setLineDash([]); ctx.stroke();
  if (labels && labels.length) {
    const step = Math.max(1, Math.floor(values.length/5));
    ctx.fillStyle='#8b949e'; ctx.font='9px sans-serif'; ctx.textAlign='center';
    for (let i=0;i<values.length;i+=step) {
      const lbl = labels[i]; if (!lbl) continue;
      const short = lbl.length>10 ? lbl.slice(11,16) : lbl;
      ctx.fillText(short, tx(i), pad.t+ch+14);
    }
  }
}

function drawMultiLine(id, labels, series) {
  const c = document.getElementById(id); if (!c) return;
  const ctx = c.getContext('2d');
  const W = c.offsetWidth || c.width; c.width = W;
  const H = c.height;
  ctx.clearRect(0,0,W,H);
  const validSeries = (series || []).filter(s => s && s.values && s.values.length >= 2);
  if (!validSeries.length) {
    ctx.fillStyle='#8b949e'; ctx.font='12px sans-serif'; ctx.textAlign='center';
      ctx.fillText('Awaiting data…', W/2, H/2); return;
  }
  const pad = {t:8,r:8,b:20,l:52};
  const cw = W-pad.l-pad.r, ch = H-pad.t-pad.b;
  const allValues = validSeries.flatMap(s => s.values);
  const mn = Math.min(...allValues), mx = Math.max(...allValues);
  const rng = mx-mn || 1;
  const pointCount = Math.max(...validSeries.map(s => s.values.length));
  const tx = i => pad.l + (i/Math.max(1, pointCount-1))*cw;
  const ty = v => pad.t + ch - ((v-mn)/rng)*ch;
  ctx.strokeStyle='#21262d'; ctx.lineWidth=1;
  for (let i=0;i<=4;i++) {
    const y=pad.t+(i/4)*ch;
    ctx.beginPath(); ctx.moveTo(pad.l,y); ctx.lineTo(pad.l+cw,y); ctx.stroke();
    ctx.fillStyle='#8b949e'; ctx.font='10px sans-serif'; ctx.textAlign='right';
    ctx.fillText((mn+((4-i)/4)*rng).toFixed(0), pad.l-3, y+3);
  }
  if (mn<0 && mx>0) {
    const zy=ty(0); ctx.strokeStyle='#30363d'; ctx.setLineDash([3,3]);
    ctx.beginPath(); ctx.moveTo(pad.l,zy); ctx.lineTo(pad.l+cw,zy); ctx.stroke();
    ctx.setLineDash([]);
  }
  validSeries.forEach((s) => {
    ctx.beginPath(); ctx.moveTo(tx(0),ty(s.values[0]));
    s.values.forEach((v,i)=>ctx.lineTo(tx(i),ty(v)));
    ctx.strokeStyle=s.color; ctx.lineWidth=1.8; ctx.setLineDash([]); ctx.stroke();
  });
  if (labels && labels.length) {
    const step = Math.max(1, Math.floor(pointCount/5));
    ctx.fillStyle='#8b949e'; ctx.font='9px sans-serif'; ctx.textAlign='center';
    for (let i=0;i<pointCount;i+=step) {
      const lbl = labels[i]; if (!lbl) continue;
      const short = lbl.length>10 ? lbl.slice(11,16) : lbl;
      ctx.fillText(short, tx(i), pad.t+ch+14);
    }
  }
}
"""


def _page(title: str, body: str, extra_js: str = "") -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>{_CSS}</style>
</head>
<body>
<div class="container">
{body}
</div>
<script>{_SORT_JS}</script>
<script>{_CHART_JS}</script>
{extra_js}
</body>
</html>"""


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Home page — sortable wallet table
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    print("[RUNNING_LIVE_FILE]", __file__)
    state = _load_state()

    updated_at = state.get("updated_at", "—")

    _norm_param = request.query_params.get("norm") if request is not None else None
    if _norm_param is not None:
        try:
            _save_norm_base(max(1.0, float(_norm_param)))
        except (ValueError, TypeError):
            pass
    saved_norm_base = _load_norm_base()
    norm_base = saved_norm_base if GLOBAL_NORM_CONFIG.exists() else _finite_float(
        (state.get("config", {}) or {}).get("normalisation_base"),
        saved_norm_base,
    )
    _update_equity_history(state)

    snap_dir = DATA_DIR / "snapshots"
    snapshots = sorted(snap_dir.iterdir(), reverse=True)[:20] if snap_dir.exists() else []
    snap_links = ""
    for s in snapshots:
        label = s.name
        meta_path = s / "meta.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                label = f"{s.name} ({meta.get('wallet_count', '?')}w)"
            except Exception:
                pass
        snap_links += f'<a href="/rollback/{s.name}" style="margin-right:8px;font-size:11px;color:#58a6ff">{label}</a>'
    snap_html = (
        f'<div style="margin:6px 0 12px;color:#8b949e;font-size:11px">'
        f'<strong style="color:#c9d1d9">Snapshots:</strong> '
        + (snap_links if snap_links else "none yet")
        + "</div>"
    )

    gate = _ensure_gate_population()
    wallets_state = state.get("wallets", {})
    wallets_norm = state.get("normalised_wallet_state", {})
    portfolio_current = state.get("normalised_portfolio", {})
    portfolio_lead = portfolio_current.get("lead", {}) if isinstance(portfolio_current, dict) else {}
    portfolio_copy = portfolio_current.get("copy", {}) if isinstance(portfolio_current, dict) else {}
    portfolio_delta = portfolio_current.get("delta", {}) if isinstance(portfolio_current, dict) else {}
    table_rows = ""
    wallets = list(wallets_state.keys())
    wallets.sort(key=lambda w: (w != USER_WALLET, w))
    sorted_wallets = [(wallet, wallets_state[wallet]) for wallet in wallets]

    for wallet, wdata in sorted_wallets:
        m = wdata.get("metrics", {})
        nstate = wallets_norm.get(wallet, {})
        copy_state = nstate.get("copy", {}) if isinstance(nstate, dict) else {}
        lead_state = nstate.get("lead", {}) if isinstance(nstate, dict) else {}
        delta_state = nstate.get("delta", {}) if isinstance(nstate, dict) else {}
        alloc = _finite_float(nstate.get("alloc"), norm_base if wallet.lower() != USER_WALLET.lower() else _finite_float(portfolio_copy.get("alloc"), norm_base))
        lead_real = _finite_float(lead_state.get("realised"))
        lead_unreal = _finite_float(lead_state.get("unrealised"))
        lead_eq = _finite_float(lead_state.get("equity"), alloc)
        lead_dd = _finite_float(lead_state.get("drawdown_pct"))
        lead_max_dd = _finite_float(lead_state.get("max_drawdown_pct"))
        copy_real = _finite_float(copy_state.get("realised"))
        copy_unreal = _finite_float(copy_state.get("unrealised"))
        copy_eq = _finite_float(copy_state.get("equity"), alloc)
        copy_dd = _finite_float(copy_state.get("drawdown_pct"))
        copy_max_dd = _finite_float(copy_state.get("max_drawdown_pct"))
        pph = _finite_float(nstate.get("pnl_per_hour"), 0.0)
        entries = int(nstate.get("entry_count", 0) or 0)
        copy_exits = int(nstate.get("exit_count", 0) or 0)
        copy_positions = int(nstate.get("open_position_count", 0) or 0)
        wallet_fills = int(nstate.get("fill_count", 0) or 0)
        ws_coverage = _finite_float(nstate.get("ws_coverage"), 0.0)
        wins = int(nstate.get("win_count", 0) or 0)
        losses = int(nstate.get("loss_count", 0) or 0)
        wr = (wins / (wins + losses) * 100.0) if (wins + losses) > 0 else 0.0
        alert_flags_list = wdata.get("alert_flags", [])
        flags_html = ""
        for f in alert_flags_list:
            f = str(f).strip()
            if f:
                css = "flag red" if f in ("LARGE_DRAWDOWN", "HIGH_LATENCY", "HIGH_SLIPPAGE") else "flag"
                flags_html += f'<span class="{css}">{f}</span>'

        lead_unreal_pct = (lead_unreal / alloc * 100) if alloc > 0 else 0.0
        copy_unreal_pct = (copy_unreal / alloc * 100) if alloc > 0 else 0.0
        total_pnl = copy_real + copy_unreal
        is_user = bool(USER_WALLET) and wallet.lower() == USER_WALLET
        row_cls = ("row-user " if is_user else "") + ("row-pos" if total_pnl > 0 else ("row-neg" if total_pnl < 0 else ""))
        short = f"{wallet[:8]}…{wallet[-6:]}" if len(wallet) > 16 else wallet
        user_badge = '<span class="badge-user">USER</span>' if is_user else ""
        print("[STATE_TABLE]", wallet, copy_real, copy_unreal, entries, copy_exits, copy_positions)

        wallet_link = (
            f'<a href="#" onclick="openUserWallet(\'{wallet}\'); return false;">{short}</a>{user_badge}'
            if is_user else
            f'<a href="/wallet/{wallet}">{short}</a>'
        )

        w_gate = gate.get(wallet, {})
        w_mode = w_gate.get("mode", "OFF")
        w_off = w_gate.get("off_mode") or ""
        _mode_styles = {
            "ON": "background:#0d2b1e;border:1px solid #2ea043;color:#3fb950",
            "CLOSE_ONLY": "background:#2d1f0e;border:1px solid #e3b341;color:#e3b341",
            "OFF": "background:#2d0e0e;border:1px solid #4a1919;color:#f85149",
        }
        _sel_style = _mode_styles.get(w_mode, _mode_styles["ON"])
        _w_esc = wallet.replace("'", "\\'")
        if is_user:
            mode_selector = (
                '<span style="background:#2d0e0e;border:1px solid #4a1919;color:#f85149;'
                'border-radius:3px;padding:2px 6px;font-size:10px;font-weight:600">'
                'USER (LOCKED)</span>'
            )
        else:
            mode_selector = (
                f'<select onchange="setWalletMode(\'{_w_esc}\', this)" '
                f'style="{_sel_style};padding:2px 4px;border-radius:3px;font-size:10px;'
                f'cursor:pointer;border-width:1px;border-style:solid;outline:none">'
                f'<option value="ON"{"         selected" if w_mode == "ON" else ""}>ON</option>'
                f'<option value="CLOSE_ONLY"{"  selected" if w_mode == "CLOSE_ONLY" else ""}>CLO</option>'
                f'<option value="OFF"{"        selected" if w_mode == "OFF" else ""}>OFF</option>'
                f'</select>'
            )
        off_badge = (
            f' <span style="background:#2d1f0e;color:#e3b341;border:1px solid #4a3219;'
            f'border-radius:3px;padding:1px 4px;font-size:9px">{w_off}</span>'
            if w_off else ""
        )

        diff_val = _finite_float(delta_state.get("equity"), copy_eq - lead_eq)
        diff_pct = _finite_float(delta_state.get("pct"), (diff_val / lead_eq * 100) if lead_eq != 0 else 0.0)
        table_rows += f'''<tr class="{row_cls}">
  <td>{wallet_link}</td>
  <td data-val="{_dv(lead_eq)}" class="pnl-col group-divider">{_fv(lead_eq, 2, "$")}</td>
  <td data-val="{_dv(lead_real)}" class="{_cls(lead_real)}">{_fv(lead_real, 2, "$")}</td>
  <td data-val="{_dv(lead_unreal)}" class="{_cls(lead_unreal)}">{_fv(lead_unreal, 2, "$")} ({_fp(lead_unreal_pct)})</td>
  <td data-val="{_dv(-lead_dd)}" class="neg">{lead_dd:.2f}%</td>
  <td data-val="{_dv(-lead_max_dd)}" class="neg">{lead_max_dd:.2f}%</td>
  <td data-val="{_dv(copy_eq)}" class="pnl-col user-eq group-divider">{_fv(copy_eq, 2, "$")}</td>
  <td data-val="{_dv(copy_real)}" class="{_cls(copy_real)}">{_fv(copy_real, 2, "$")}</td>
  <td data-val="{_dv(copy_unreal)}" class="{_cls(copy_unreal)}">{_fv(copy_unreal, 2, "$")} ({_fp(copy_unreal_pct)})</td>
  <td data-val="{_dv(-copy_dd)}" class="neg">{copy_dd:.2f}%</td>
  <td data-val="{_dv(-copy_max_dd)}" class="neg">{copy_max_dd:.2f}%</td>
  <td data-val="{_dv(diff_val)}" class="{_cls(diff_val)}">{_fv(diff_val, 2, "$")} ({_fp(diff_pct)})</td>
  <td data-val="{_dv(pph)}" class="{_cls(pph)} group-divider">{_fv(pph, 2, "$")}</td>
  <td data-val="{_dv(wr)}">{_fp(wr)}</td>
  <td data-val="{_dv(ws_coverage)}">{ws_coverage * 100.0:.1f}%</td>
  <td>{wallet_fills} / {entries}</td>
  <td>— / {copy_exits}</td>
  <td>— / {copy_positions}</td>
  <td>{flags_html}</td>
  <td style="white-space:nowrap">{mode_selector}{off_badge}</td>
</tr>'''

    tracked_wallets = [w for w in wallets if w != USER_WALLET]
    tracked_norm = [wallets_norm.get(w, {}) for w in tracked_wallets]
    _s_tracked_realized = _finite_float(portfolio_copy.get("realized"))
    _s_tracked_unrealized = _finite_float(portfolio_copy.get("unrealized"))
    _s_tracked_total = _finite_float(portfolio_copy.get("equity")) - _finite_float(portfolio_copy.get("alloc"))
    _s_tracked_max_dd = max(
        [float(h.get("drawdown_pct", 0.0)) for h in _portfolio_history if isinstance(h, dict)] or [0.0]
    )
    _s_live_realized = _s_tracked_realized
    _s_live_unrealized = _s_tracked_unrealized
    _s_live_total = _s_tracked_total
    _s_live_max_dd = _s_tracked_max_dd
    _s_total_open = sum(int(n.get("open_position_count", 0) or 0) for n in tracked_norm)
    _s_wins = sum(int(n.get("win_count", 0) or 0) for n in tracked_norm)
    _s_losses = sum(int(n.get("loss_count", 0) or 0) for n in tracked_norm)
    _s_total_trades = _s_wins + _s_losses
    _s_win_rate = _s_wins / _s_total_trades * 100 if _s_total_trades > 0 else 0.0
    _s_avg_trade = _s_tracked_realized / _s_total_trades if _s_total_trades > 0 else 0.0
    _s_total_alloc = _finite_float(portfolio_copy.get("alloc"))
    _s_capital_in_use = sum(
        _finite_float(wallets_norm.get(w, {}).get("alloc"))
        for w in tracked_wallets
        if int(wallets_state.get(w, {}).get("metrics", {}).get("copy_open_positions", 0) or 0) > 0
    )
    _s_utilisation = (_s_capital_in_use / _s_total_alloc) * 100 if _s_total_alloc > 0 else 0.0
    _s_peak_equity = _finite_float(portfolio_copy.get("peak_equity"), _finite_float(portfolio_copy.get("peak")))
    _s_return_on_alloc = _s_tracked_realized / _s_total_alloc if _s_total_alloc > 0 else 0.0
    _s_return_on_active = _s_tracked_realized / _s_capital_in_use if _s_capital_in_use > 0 else 0.0
    _s_lead_eq = _finite_float(portfolio_lead.get("equity"))
    _s_copy_eq = _finite_float(portfolio_copy.get("equity"))
    _s_delta_eq = _finite_float(portfolio_delta.get("equity"))
    _s_delta_pct = _finite_float(portfolio_delta.get("pct"))
    _s_exec_drag_pct = _s_delta_pct * 100.0

    def _sc2_pnl(val: float, label: str, base: float) -> str:
        sign = "+" if val >= 0 else ""
        pct = (val / base * 100) if base and abs(base) > 1e-9 else 0.0
        pct_sign = "+" if pct >= 0 else ""
        disp = f"{sign}${val:,.2f} ({pct_sign}{pct:.2f}%)"
        cls = "pos" if val >= 0 else "neg"
        return (
            f'<div class="stat" style="flex:1;min-width:110px;max-width:180px">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value {cls}" style="font-size:14px;line-height:1.3">{disp}</div>'
            f'</div>'
        )

    def _sc2_dd(pct: float, label: str, peak: float) -> str:
        if _portfolio_history:
            dd_dollars = max(
                float(h.get("drawdown_usd", 0.0))
                for h in _portfolio_history
                if isinstance(h, dict)
            )
        else:
            dd_dollars = peak * pct / 100.0 if peak and abs(peak) > 1e-9 else 0.0
        disp = f"{pct:.2f}% (-${dd_dollars:,.2f})"
        cls = "neg" if pct and pct > 0 else "neu"
        return (
            f'<div class="stat" style="flex:1;min-width:110px;max-width:180px">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value {cls}" style="font-size:14px;line-height:1.3">{disp}</div>'
            f'</div>'
        )

    def _sc2_delta(val: float, ratio: float, label: str) -> str:
        cls = "pos" if val >= 0 else "neg"
        sign = "+" if val >= 0 else ""
        pct = ratio * 100.0
        pct_sign = "+" if pct >= 0 else ""
        disp = f"{sign}${val:,.2f} ({pct_sign}{pct:.2f}%)"
        return (
            f'<div class="stat" style="flex:1;min-width:110px;max-width:180px">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value {cls}" style="font-size:14px;line-height:1.3">{disp}</div>'
            f'</div>'
        )

    def _sc(val: float, label: str, is_pct: bool = False, is_int: bool = False) -> str:
        if is_int:
            disp = str(int(val))
            cls = "neu"
        elif is_pct:
            disp = f"{val:.1f}%"
            cls = "pos" if val >= 0 else "neg"
        else:
            disp = f"${val:+,.2f}" if val != 0 else "$0.00"
            cls = "pos" if val >= 0 else "neg"
        return (
            f'<div class="stat" style="flex:1;min-width:110px;max-width:180px">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value {cls}" style="font-size:14px;line-height:1.3">{disp}</div>'
            f'</div>'
        )

    summary_html = (
        '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px">'
        + _sc(_s_lead_eq, "LEAD EQ")
        + _sc(_s_copy_eq, "COPY EQ")
        + _sc2_delta(_s_delta_eq, _s_delta_pct, "Δ EQ")
        + _sc(_s_exec_drag_pct, "EXECUTION DRAG %", is_pct=True)
        + _sc2_pnl(_s_tracked_total, "TRACKED PnL", _s_total_alloc)
        + _sc2_pnl(_s_tracked_realized, "TRACKED REALIZED", _s_total_alloc)
        + _sc2_pnl(_s_tracked_unrealized, "TRACKED UNREALIZED", _s_total_alloc)
        + _sc2_dd(_s_tracked_max_dd, "TRACKED MAX DD", _s_peak_equity)
        + _sc(_s_total_alloc, "ALLOCATED CAPITAL")
        + _sc(_s_capital_in_use, "CAPITAL IN USE")
        + _sc(_s_utilisation, "UTILISATION", is_pct=True)
        + _sc(_s_return_on_alloc, "RETURN / ALLOC")
        + _sc(_s_return_on_active, "RETURN / ACTIVE")
        + _sc(_s_live_total, "LIVE PnL")
        + _sc(_s_live_max_dd, "LIVE MAX DD", is_pct=True)
        + _sc(_s_total_open, "OPEN POSITIONS", is_int=True)
        + _sc(_s_win_rate, "WIN RATE", is_pct=True)
        + _sc(_s_avg_trade, "AVG TRADE")
        + '</div>'
    )

    col_note = (
        '<p style="font-size:0.75rem;color:#888;margin:0.25rem 0 0.5rem;">'
        'Lead = leader reference · Copy = copy execution · $ values normalised to global base · '
        'leader-side exits/positions render as — when no live-state field exists.'
        '</p>'
    )

    headers = [
        ("Wallet", 0, False, ""), ("Lead Equity", 1, True, "group-divider"),
        ("Lead Real", 2, True, ""), ("Lead Unreal", 3, True, ""), ("Lead DD", 4, True, ""), ("Lead MaxDD", 5, True, ""),
        ("User Eq", 6, True, "group-divider"),
        ("Copy Real", 7, True, ""), ("Copy Unreal", 8, True, ""), ("Copy DD", 9, True, ""), ("Copy MaxDD", 10, True, ""),
        ("Δ $/%", 11, True, ""),
        ("PnL/hr", 12, True, "group-divider"), ("Win%", 13, True, ""), ("WS %", 14, True, ""),
        ("Fills W/C", 15, True, ""), ("Exits W/C", 16, True, ""), ("Pos W/C", 17, True, ""),
        ("Flags", 18, False, ""), ("Mode", 19, False, ""),
    ]
    th_html = "".join(
        f'<th class="{cls}" onclick="sortTable(\'wallets\',{i},{"true" if num else "false"})">{lbl}</th>'
        for lbl, i, num, cls in headers
    )

    body = f"""
<div class="header">
  <h1>⚡ HL Copy Engine</h1>
  <span class="badge live">LIVE</span>
  <span class="badge">Updated: {updated_at[:19].replace("T", " ")} UTC</span>
  <form method="get" id="normForm" style="margin-left:16px;display:flex;align-items:center;gap:6px;">
    <label style="font-size:11px;color:#8b949e;white-space:nowrap">Normalisation Base:</label>
    <input type="number" name="norm" id="normInput" value="{norm_base:.0f}" min="1" step="1"
      style="width:90px;background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:4px;padding:2px 6px;font-size:11px;">
    <button type="submit"
      style="background:#21262d;border:1px solid #30363d;color:#c9d1d9;border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;">
      Set
    </button>
  </form>
  <span class="ml-auto" style="font-size:11px;color:#8b949e">Auto-refresh 15s</span>
</div>
{summary_html}
<div class="section">
  <div class="section-title" style="display:flex;align-items:center;gap:10px;">
    Combined Portfolio — Non-User Wallets
    <button id="portExpandBtn" onclick="togglePortChart()"
      style="background:#21262d;border:1px solid #30363d;color:#c9d1d9;border-radius:4px;padding:2px 10px;font-size:11px;cursor:pointer;">
      Expand
    </button>
  </div>
  <div class="chart-box">
    <div class="chart-title">Green = PnL, Blue = realized, Red = drawdown</div>
    <canvas id="portChart" height="300"></canvas>
  </div>
</div>
{snap_html}
<div class="section">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;flex-wrap:wrap;">
    <div class="section-title" style="margin:0">Tracked Wallets ({len(wallets_state)}) — Norm Base: ${norm_base:,.0f}</div>
    <button onclick="setAllModes('ON')"
      style="padding:3px 10px;background:#0d2b1e;border:1px solid #2ea043;color:#3fb950;border-radius:3px;font-size:11px;cursor:pointer;">
      ALL ON
    </button>
    <button onclick="setAllModes('OFF')"
      style="padding:3px 10px;background:#2d0e0e;border:1px solid #4a1919;color:#f85149;border-radius:3px;font-size:11px;cursor:pointer;">
      ALL OFF
    </button>
  </div>
  {col_note}
  <div style="width:100%;overflow-x:auto;">
  <table id="wallets">
    <thead><tr>{th_html}</tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  </div>
</div>
<div class="section" style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
  <span style="font-size:12px;color:#8b949e;font-weight:600;">ENGINE CONTROLS</span>
  <button onclick="resetAll()"
    style="padding:6px 12px;background:#2d0e0e;border:1px solid #4a1919;color:#f85149;border-radius:4px;cursor:pointer;font-size:12px;">
    RESET ALL
  </button>
  <button onclick="startEngine()"
    style="margin-top:0;padding:6px 12px;background:#0d2b1e;border:1px solid #2ea043;color:#3fb950;border-radius:4px;cursor:pointer;font-size:12px;">
    START ENGINE
  </button>
</div>
"""

    with _equity_lock:
        port_snap = list(_portfolio_history)
    if port_snap:
        port_labels = [h["ts"] for h in port_snap]
        port_equity = [_finite_float(h.get("realized")) + _finite_float(h.get("unrealized")) for h in port_snap]
        port_realized = [h["realized"] for h in port_snap]
        port_drawdown = [-h["drawdown_usd"] for h in port_snap]
    else:
        port_labels = []
        port_equity = []
        port_realized = []
        port_drawdown = []

    extra_js = f"""
<script>
const portLabels = {json.dumps(port_labels)};
const portEquity = {json.dumps(port_equity)};
const portRealized = {json.dumps(port_realized)};
const portDrawdown = {json.dumps(port_drawdown)};
const portSeries = [
  {{ label: 'Equity', values: portEquity, color: '#3fb950' }},
  {{ label: 'Realized', values: portRealized, color: '#58a6ff' }},
  {{ label: 'Drawdown', values: portDrawdown, color: '#f85149' }},
];
function togglePortChart() {{
  const c = document.getElementById('portChart');
  const btn = document.getElementById('portExpandBtn');
  if (c.height <= 300) {{
    c.height = 600; btn.textContent = 'Collapse';
    savePref('hlcopy.portfolio.expanded', true);
  }} else {{
    c.height = 300; btn.textContent = 'Expand';
    savePref('hlcopy.portfolio.expanded', false);
  }}
  drawMultiLine('portChart', portLabels, portSeries);
}}
window.addEventListener('load', () => {{
  if (loadPref('hlcopy.portfolio.expanded', false)) {{
    const c = document.getElementById('portChart');
    const btn = document.getElementById('portExpandBtn');
    if (c && btn) {{ c.height = 600; btn.textContent = 'Collapse'; }}
  }}
  drawMultiLine('portChart', portLabels, portSeries);
  restoreSort('wallets');
}});
function startEngine() {{
  fetch('/api/start', {{ method: 'POST' }})
    .then(r => r.json())
    .then(d => {{
      if (d.ok) {{
        alert('Engine starting...');
        setTimeout(() => location.reload(), 2000);
      }} else {{
        alert('Start failed: ' + d.error);
      }}
    }})
    .catch(() => alert('Start request failed'));
}}
function resetAll() {{
  if (!confirm('Reset ALL engine state? This cannot be undone.')) return;
  fetch('/api/reset-all', {{ method: 'POST' }})
    .then(r => r.json())
    .then(d => {{ if (d.ok) {{ alert('Reset queued.'); setTimeout(() => location.reload(), 2000); }} }})
    .catch(() => alert('Reset request failed'));
}}
let _rt = setTimeout(() => location.reload(), 15000);
const _ni = document.getElementById('normInput');
const _nf = document.getElementById('normForm');
if (_ni) {{
  _ni.addEventListener('focus', () => clearTimeout(_rt));
  _ni.addEventListener('blur', () => {{ _rt = setTimeout(() => location.reload(), 15000); }});
}}
if (_nf && _ni) {{
  _nf.addEventListener('submit', async (ev) => {{
    ev.preventDefault();
    clearTimeout(_rt);
    const normBase = Math.max(1, Number(_ni.value || 0) || 1);
    try {{
      await fetch('/api/norm', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ norm_base: normBase }})
      }});
    }} catch (_err) {{
      window.location = '/?norm=' + encodeURIComponent(normBase);
      return;
    }}
    let synced = false;
    for (let i = 0; i < 20; i++) {{
      await new Promise(r => setTimeout(r, 500));
      try {{
        const resp = await fetch('/api/state');
        const data = await resp.json();
        const current = Number((((data || {{}}).config || {{}}).normalisation_base) || 0);
        if (Math.abs(current - normBase) < 1e-9) {{
          synced = true;
          break;
        }}
      }} catch (_err) {{}}
    }}
    if (!synced) {{
      await new Promise(r => setTimeout(r, 500));
    }}
    location.reload();
  }});
}}
</script>"""

    return HTMLResponse(_page("HL Copy Dashboard", body, extra_js))

@app.get("/rollback/{ts}", response_class=HTMLResponse)
def rollback(ts: str) -> HTMLResponse:
    snap_path = DATA_DIR / "snapshots" / ts
    if not snap_path.exists():
        return HTMLResponse(f"Snapshot {ts} not found", status_code=404)
    try:
        for fname in ("live_state.json", "portfolio_history.json", "portfolio_baseline.json"):
            src = snap_path / fname
            if src.exists():
                shutil.copy(src, DATA_DIR / fname)
        return HTMLResponse(f"Rollback SUCCESS â†’ {ts} (restart engine to apply)", status_code=200)
    except Exception as e:
        return HTMLResponse(f"Rollback ERROR: {e}", status_code=500)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Wallet gate control
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/gate/{wallet}", response_class=HTMLResponse)
def gate_wallet(wallet: str, live: int = 0, off_mode: str = "") -> HTMLResponse:
    wallet = wallet.strip().lower()
    state = _load_state()
    gate = _ensure_gate_population()

    live_enabled = bool(live)
    valid_off_modes = ("CLOSE_NOW", "FOLLOW", "DO_NOTHING")
    existing = gate.get(wallet, {})
    if not isinstance(existing, dict):
        existing = {}

    if live_enabled:
        existing["mode"] = "ON"
        existing["off_mode"] = None
        gate[wallet] = existing
        _save_wallet_gate(gate)
        return HTMLResponse(
            f'<meta http-equiv="refresh" content="0;url=/">Enabled LIVE for {wallet[:10]}…'
        )

    # Toggling OFF — check for open positions
    open_pos = len(state.get("wallets", {}).get(wallet, {}).get("open_positions", []))
    if open_pos == 0:
        existing["mode"] = "OFF"
        existing["off_mode"] = None
        gate[wallet] = existing
        _save_wallet_gate(gate)
        return HTMLResponse(
            f'<meta http-equiv="refresh" content="0;url=/">Disabled LIVE for {wallet[:10]}…'
        )

    # Positions exist — need off_mode
    if off_mode in valid_off_modes:
        existing["mode"] = "OFF"
        existing["off_mode"] = off_mode
        gate[wallet] = existing
        _save_wallet_gate(gate)
        return HTMLResponse(
            f'<meta http-equiv="refresh" content="0;url=/">Set off_mode={off_mode} for {wallet[:10]}…'
        )

    # Show off-mode selection page
    short = f"{wallet[:8]}…{wallet[-6:]}"
    body = f"""
<div class="header"><h1>Disable LIVE — {short}</h1></div>
<p style="color:#8b949e;margin-bottom:16px">{open_pos} open position(s) exist. Choose how to handle them:</p>
<div style="display:flex;flex-direction:column;gap:10px;max-width:300px">
  <a href="/gate/{wallet}?live=0&off_mode=CLOSE_NOW"
     style="background:#2d0e0e;border:1px solid #4a1919;color:#f85149;padding:10px 16px;border-radius:6px;text-align:center">
     CLOSE NOW — close all sim positions immediately</a>
  <a href="/gate/{wallet}?live=0&off_mode=FOLLOW"
     style="background:#0d2b1e;border:1px solid #2ea043;color:#3fb950;padding:10px 16px;border-radius:6px;text-align:center">
     FOLLOW TRADER — close when trader closes</a>
  <a href="/gate/{wallet}?live=0&off_mode=DO_NOTHING"
     style="background:#21262d;border:1px solid #30363d;color:#8b949e;padding:10px 16px;border-radius:6px;text-align:center">
     DO NOTHING — keep sim positions frozen</a>
</div>
<a href="/" style="display:block;margin-top:16px;font-size:12px">← Cancel</a>
"""
    return HTMLResponse(_page(f"Gate — {short}", body))


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Wallet detail page
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/wallet/{wallet}", response_class=HTMLResponse)
def wallet_detail(request: Request, wallet: str, _state: dict | None = None) -> HTMLResponse:
    wallet = wallet.strip().lower()
    state = _state if _state is not None else _load_state()
    trades = pd.DataFrame()
    _update_equity_history(state)

    norm_base = _load_norm_base()

    wdata = state.get("wallets", {}).get(wallet, {})
    metrics = wdata.get("metrics", {})
    equity_data = wdata.get("equity", {})
    open_positions = wdata.get("open_positions", [])
    alert_flags = wdata.get("alert_flags", [])

    if not trades.empty and "wallet" in trades.columns:
        wt = trades[trades["wallet"] == wallet].copy()
        if "exit_time_iso" in wt.columns:
            wt = wt.sort_values("exit_time_iso", ascending=False)
        wt_last20 = wt.head(20)
    else:
        wt = pd.DataFrame()
        wt_last20 = pd.DataFrame()

    _exits = int(metrics.get("copy_exit_count", 0) or 0)
    _trade_rows = len(wt) if not wt.empty else 0
    print(f"TRADE_DEBUG wallet={wallet} rows={_trade_rows}")
    if _exits > 0 and _trade_rows == 0:
        print("ERROR::TRADE_TABLE_EMPTY_WITH_EXITS")

    with _equity_lock:
        hist = list(_equity_history.get(wallet, []))
    eq_labels = [h[0] for h in hist]
    eq_values = [h[1] for h in hist]
    eq_color = "#3fb950" if (eq_values and eq_values[-1] >= eq_values[0]) else "#f85149"

    _pnl_latest = eq_values[-1] if eq_values else 0.0
    print(f"EQUITY_DEBUG wallet={wallet} hist_len={len(hist)} pnl={_pnl_latest}")
    _entries = int(metrics.get("copy_entry_count", 0) or 0)
    if _entries > 0 and len(hist) < 2:
        print("ERROR::EQUITY_HISTORY_NOT_ADVANCING")

    slip_b = _slip_buckets(wt)
    lat_b = _lat_buckets(wt)
    slip_bars = _bucket_bars_html(slip_b, "slip")
    lat_bars = _bucket_bars_html(lat_b, "lat")

    scale = norm_base / 100.0
    rpnl = float(equity_data.get("realized_pnl", 0.0) or 0.0) * scale
    upnl = float(equity_data.get("unrealized_pnl", 0.0) or 0.0) * scale
    _uw_hard = "0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205"
    _is_user_wallet = wdata.get("is_user_wallet") or (bool(USER_WALLET) and wallet.lower() == USER_WALLET) or wallet.lower() == _uw_hard
    # Paper values are the primary source for all wallets including user wallet
    total_eq_disp = norm_base + rpnl + upnl
    raw_starting  = float(equity_data.get("starting_balance", 100.0) or 100.0)
    raw_peak      = float(equity_data.get("peak_equity", raw_starting) or raw_starting)
    peak_eq_disp  = norm_base + (raw_peak - raw_starting) * scale
    dd_pct        = float(equity_data.get("drawdown", 0.0) or 0.0)
    max_dd_pct    = float(equity_data.get("max_drawdown", 0.0) or 0.0)

    win_r = float(metrics.get("win_rate", 0.0) or 0.0)
    pph   = float(metrics.get("pnl_per_hour", 0.0) or 0.0)
    ppt   = float(metrics.get("pnl_per_trade", 0.0) or 0.0)
    avg_lat = metrics.get("avg_entry_latency_ms", 0.0)
    avg_slip = metrics.get("avg_entry_slippage_bps", 0.0)
    match_r = metrics.get("fill_match_rate", 1.0)
    avg_hold = metrics.get("avg_holding_seconds", 0.0)

    def hold_fmt(s: float) -> str:
        if not s:
            return "—"
        s = int(s)
        if s < 60:
            return f"{s}s"
        if s < 3600:
            return f"{s//60}m{s%60}s"
        return f"{s//3600}h{(s%3600)//60}m"

    stats = [
        ("Total Equity", _fv(total_eq_disp, 2, "$"), _cls(rpnl + upnl)),
        ("Realized PnL", _fdual_pnl(rpnl, norm_base), _cls(rpnl)),
        ("Unrealized PnL", _fdual_pnl(upnl, norm_base), _cls(upnl)),
        ("Peak Equity", _fv(peak_eq_disp, 2, "$"), "neu"),
        ("Drawdown", _fdual_dd(dd_pct, peak_eq_disp), "neg" if dd_pct and dd_pct > 0 else "neu"),
        ("Max Drawdown", _fdual_dd(max_dd_pct, peak_eq_disp), "neg" if max_dd_pct and max_dd_pct > 0 else "neu"),
        ("Win Rate", _fp(win_r), _cls(win_r - 50)),
        ("PnL / Hour", _fv(pph, 2, "$"), _cls(pph)),
        ("PnL / Trade", _fv(ppt, 2, "$"), _cls(ppt)),
        ("Avg Latency", f"{_fv(avg_lat, 0)} ms", "neu"),
        ("Avg Δ (bps)", _fv(avg_slip, 2), "neu"),
        ("Fill Match", _fp(match_r * 100), _cls(match_r - 0.9)),
        ("Avg Hold", hold_fmt(avg_hold), "neu"),
        ("Entries", str(metrics.get("copy_entry_count", 0)), "neu"),
        ("Exits", str(metrics.get("copy_exit_count", 0)), "neu"),
        ("Open Pos", str(metrics.get("copy_open_positions", 0)), "neu"),
    ]
    stat_html = "".join(
        f'<div class="stat"><div class="stat-label">{lbl}</div>'
        f'<div class="stat-value {cls}">{val}</div></div>'
        for lbl, val, cls in stats
    )

    # â”€â”€ user wallet diagnostics + exchange panel â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    exchange_panel = ""
    if _is_user_wallet:
        pos_unreal_sum = sum(float(p.get("unrealized_pnl", 0.0)) for p in open_positions)
        trades_count   = len(wt) if not wt.empty else 0
        if open_positions and upnl == 0.0:
            print(f"ERROR::PAPER_PNL_MISMATCH wallet={wallet} open_pos={len(open_positions)} unrealized=0")
        print(f"USER_UI_CHECK: summary_unreal={upnl} sum(open_positions.unreal)={pos_unreal_sum} trades_count={trades_count}")

        ex_snap = wdata.get("_exchange_snap", {})
        if ex_snap:
            ex_eq  = float(ex_snap.get("exchange_equity", 0.0))
            ex_sz  = float(ex_snap.get("exchange_position_size", 0.0))
            ex_px  = float(ex_snap.get("exchange_entry_price", 0.0))
            ex_ts  = str(ex_snap.get("exchange_timestamp", "—"))[:19].replace("T", " ")
            exchange_panel = f"""
<div class="section">
  <div class="section-title">Exchange Snapshot <span style="font-size:10px;color:#8b949e;font-weight:400">(as of {ex_ts} UTC)</span></div>
  <div class="stat-grid">
    <div class="stat"><div class="stat-label">Exchange Equity</div><div class="stat-value neu">{_fv(ex_eq, 2, "$")}</div></div>
    <div class="stat"><div class="stat-label">Position Size</div><div class="stat-value neu">{_fv(ex_sz, 6) if ex_sz else "—"}</div></div>
    <div class="stat"><div class="stat-label">Entry Price</div><div class="stat-value neu">{_fv(ex_px, 4, "$") if ex_px else "—"}</div></div>
  </div>
</div>"""

    flags_html = ""
    for f in alert_flags:
        css = "flag red" if f in ("LARGE_DRAWDOWN", "HIGH_LATENCY", "HIGH_SLIPPAGE") else "flag"
        flags_html += f'<span class="{css}">{f}</span>'

    pos_cards = ""
    for pos in open_positions:
        coin = pos.get("coin", "?")
        entry_px = _fv(pos.get("entry_price_copy"), 6)
        mark_px = _fv(pos.get("mark_price"), 6)
        upnl_pos = pos.get("unrealized_pnl", 0.0)
        ret_pct = pos.get("return_pct", 0.0)
        age = pos.get("age_seconds", 0)
        notional = _fv(pos.get("notional_usd"), 2, "$")
        tid = pos.get("trade_id", "")
        pos_cards += f"""<div class="pos-card">
  <div class="pos-coin">{coin} <span style="font-size:10px;color:#8b949e">{tid}</span></div>
  <div class="pos-row"><span class="pos-key">Entry px</span><span class="pos-val">{entry_px}</span></div>
  <div class="pos-row"><span class="pos-key">Mark px</span><span class="pos-val">{mark_px}</span></div>
  <div class="pos-row"><span class="pos-key">Notional</span><span class="pos-val">{notional}</span></div>
  <div class="pos-row"><span class="pos-key">Unreal PnL</span><span class="pos-val {_cls(upnl_pos)}">{_fv(upnl_pos, 4, "$")}</span></div>
  <div class="pos-row"><span class="pos-key">Return</span><span class="pos-val {_cls(ret_pct)}">{_fp(ret_pct, 2)}</span></div>
  <div class="pos-row"><span class="pos-key">Age</span><span class="pos-val">{hold_fmt(age)}</span></div>
</div>"""
    if not pos_cards:
        pos_cards = '<p style="color:#8b949e;font-size:12px">No open positions</p>'

    trade_rows = ""
    if not wt_last20.empty:
        for _, t in wt_last20.iterrows():
            pnl = t.get("copy_pnl", 0.0)
            ret = t.get("return_pct")
            dur = str(t.get("duration", t.get("holding_seconds", "")))
            lat_ms = t.get("entry_latency_ms")
            slip_e = t.get("entry_slippage_bps")
            slip_x = t.get("exit_slippage_bps")
            tid = str(t.get("trade_id", ""))
            entry_t = str(t.get("entry_time_iso", ""))[:19].replace("T", " ")
            exit_t = str(t.get("exit_time_iso", ""))[:19].replace("T", " ")
            trade_rows += f"""<tr class="{'row-pos' if (pnl or 0) > 0 else 'row-neg' if (pnl or 0) < 0 else ''}">
  <td style="font-size:10px;color:#8b949e">{tid}</td>
  <td>{str(t.get("coin",""))}</td>
  <td style="font-size:11px">{entry_t}</td>
  <td style="font-size:11px">{exit_t}</td>
  <td>{dur}</td>
  <td data-val="{_dv(pnl)}" class="pnl-col">{_fv(pnl, 4, "$")}</td>
  <td data-val="{_dv(ret)}" class="{_cls(ret)}">{_fp(ret, 2) if ret is not None else "—"}</td>
  <td>{_fv(lat_ms, 0)} ms</td>
  <td>{_fv(slip_e, 1)} / {_fv(slip_x, 1)} bps</td>
</tr>"""
    else:
        trade_rows = '<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:16px">No trades yet</td></tr>'

    short_w = f"{wallet[:10]}…{wallet[-6:]}" if len(wallet) > 18 else wallet
    body = f"""
<a class="back-link" href="/">← Back to Dashboard</a>
<div class="header">
  <h1>Wallet: {short_w}</h1>
  {''.join(f'<span class="{"flag red" if f in ("LARGE_DRAWDOWN","HIGH_LATENCY","HIGH_SLIPPAGE") else "flag"}">{f}</span>' for f in alert_flags)}
</div>

<div class="section">
  <div class="section-title">Paper Trading — Equity &amp; PnL <span style="font-size:10px;color:#8b949e;font-weight:400">(norm base: ${norm_base:,.0f})</span></div>
  <div class="stat-grid">{stat_html}</div>
</div>
{exchange_panel}
<div class="section">
  <div class="section-title">Equity Curve</div>
  <div class="chart-box">
    <div class="chart-title">PnL Over Time — realized + unrealized (in-session, last {EQUITY_HISTORY_MAX} points)</div>
    <canvas id="eqChart" height="180"></canvas>
  </div>
</div>

<div class="section">
  <div class="section-title">Open Positions ({len(open_positions)})</div>
  <div class="positions-grid">{pos_cards}</div>
</div>

<div class="two-col section">
  <div>
    <div class="section-title">Slippage Distribution</div>
    <div class="chart-box">{slip_bars}</div>
  </div>
  <div>
    <div class="section-title">Latency Distribution</div>
    <div class="chart-box">{lat_bars}</div>
  </div>
</div>

<div class="section">
  <div class="section-title">Last 20 Trades</div>
  <table id="tradesTbl">
    <thead><tr>
      <th onclick="sortTable('tradesTbl',0,false)">ID</th>
      <th onclick="sortTable('tradesTbl',1,false)">Coin</th>
      <th onclick="sortTable('tradesTbl',2,false)">Entry</th>
      <th onclick="sortTable('tradesTbl',3,false)">Exit</th>
      <th onclick="sortTable('tradesTbl',4,false)">Duration</th>
      <th onclick="sortTable('tradesTbl',5,true)">PnL</th>
      <th onclick="sortTable('tradesTbl',6,true)">Return %</th>
      <th onclick="sortTable('tradesTbl',7,true)">Latency</th>
      <th>Entry/Exit Slip</th>
    </tr></thead>
    <tbody>{trade_rows}</tbody>
  </table>
</div>
"""

    extra_js = f"""
<script>
const eqLabels = {json.dumps(eq_labels)};
const eqVals = {json.dumps(eq_values)};
window.addEventListener('load', () => drawLine('eqChart', eqLabels, eqVals, '{eq_color}'));
</script>"""

    return HTMLResponse(_page(f"Wallet {short_w}", body, extra_js))


@app.get("/user-wallet/{wallet}", response_class=HTMLResponse)
def user_wallet_detail(request: Request, wallet: str) -> HTMLResponse:
    snap = _load_user_snapshot()
    w = wallet.strip().lower()
    state = _load_state()
    if w in state.get("wallets", {}):
        state["wallets"][w]["is_user_wallet"] = True
        if snap and snap.get("exchange_equity", 0) > 0:
            state["wallets"][w]["_exchange_snap"] = snap
    return wallet_detail(request, w, _state=state)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# JSON API endpoints
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/api/state")
def api_state() -> JSONResponse:
    state = _load_state()
    _update_equity_history(state)
    with _equity_lock:
        for w in state.get("wallets", {}):
            hist = _equity_history.get(w, [])
            state["wallets"][w]["equity_history_len"] = len(hist)
    return JSONResponse(state)


@app.get("/api/equity/{wallet}")
def api_equity(wallet: str) -> JSONResponse:
    wallet = wallet.strip().lower()
    with _equity_lock:
        hist = list(_equity_history.get(wallet, []))
    return JSONResponse({"wallet": wallet, "history": hist})


@app.get("/api/metrics")
def api_metrics() -> JSONResponse:
    state = _load_state()
    rows = []
    for wallet, wdata in (state.get("wallets", {}) or {}).items():
        row = {"wallet": wallet}
        row.update(wdata.get("metrics", {}) or {})
        rows.append(row)
    return JSONResponse(rows)


@app.get("/api/trades/{wallet}")
def api_trades(wallet: str, limit: int = 50) -> JSONResponse:
    wallet = wallet.strip().lower()
    trades = pd.DataFrame()
    if trades.empty or "wallet" not in trades.columns:
        return JSONResponse([])
    wt = trades[trades["wallet"] == wallet]
    if "exit_time_iso" in wt.columns:
        wt = wt.sort_values("exit_time_iso", ascending=False)
    return JSONResponse(wt.head(limit).fillna("").astype(str).to_dict(orient="records"))


@app.get("/api/norm")
def api_norm() -> JSONResponse:
    return JSONResponse({"norm_base": _load_norm_base()})


@app.post("/api/norm")
async def api_set_norm(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        norm_base = max(1.0, float((body or {}).get("norm_base", GLOBAL_NORM_DEFAULT)))
    except Exception:
        return JSONResponse({"ok": False, "error": "INVALID_NORM_BASE"}, status_code=400)
    _save_norm_base(norm_base)
    return JSONResponse({"ok": True, "norm_base": norm_base})


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Legacy alerts route (preserved)
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/alerts", response_class=HTMLResponse)
def alerts(request: Request) -> HTMLResponse:
    path = DATA_DIR / "alerts.csv"
    rows = ""
    if path.exists():
        try:
            df = pd.read_csv(path).fillna("").astype(str)
            for _, r in df.iterrows():
                cells = "".join(f"<td>{v}</td>" for v in r.values)
                rows += f"<tr>{cells}</tr>"
            headers = "".join(f"<th>{c}</th>" for c in df.columns)
        except Exception:
            headers = "<th>Error loading alerts</th>"
    else:
        headers = "<th>No alerts file</th>"

    body = f"""
<div class="header">
  <h1>Alert Log</h1>
  <a href="/" class="badge">← Dashboard</a>
</div>
<table><thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table>
"""
    return HTMLResponse(_page("Alerts", body))


@app.post("/api/reset-all")
def reset_all() -> JSONResponse:
    """Signal engine to total-reset via command file, then clear app memory."""
    global _equity_history, _active_wallets, _portfolio_history

    # signal engine (separate process) via command file
    try:
        (DATA_DIR / "reset_command").touch()
    except Exception:
        pass

    # clear app in-memory state immediately
    _equity_history.clear()
    _active_wallets.clear()
    _portfolio_history.clear()

    # delete app-side persisted files
    _purge_app_history_files()

    return JSONResponse({"ok": True, "pending": "engine_reset_within_5s"})


@app.post("/api/start")
def api_start():
    import subprocess, sys, os
    try:
        subprocess.Popen(
            [sys.executable, "HL_Copy_Engine.py"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return {"ok": True, "status": "engine_started"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/set-wallet-mode")
async def set_wallet_mode(request: Request) -> JSONResponse:
    """Set wallet copy mode: ON | CLOSE_ONLY | OFF."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid JSON"}, status_code=400)
    wallet = str(body.get("wallet", "")).strip().lower()
    mode   = str(body.get("mode", "")).strip().upper()
    if not wallet:
        return JSONResponse({"ok": False, "error": "missing wallet"}, status_code=400)
    if mode not in ("ON", "CLOSE_ONLY", "OFF"):
        return JSONResponse({"ok": False, "error": f"invalid mode '{mode}'"}, status_code=400)
    if USER_WALLET and wallet.lower() == USER_WALLET.lower():
        return JSONResponse({"ok": False, "error": "USER_WALLET_LOCKED"})
    gate = _ensure_gate_population()
    entry = gate.get(wallet, {})
    if not isinstance(entry, dict):
        entry = {}
    entry["mode"] = mode
    gate[wallet] = entry
    _save_wallet_gate(gate)
    return JSONResponse({"ok": True, "wallet": wallet, "mode": mode})


@app.post("/api/set-all-modes")
async def set_all_modes(request: Request) -> JSONResponse:
    """Set every wallet in the gate to the same mode: ON | CLOSE_ONLY | OFF."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid JSON"}, status_code=400)
    mode = str(body.get("mode", "")).strip().upper()
    if mode not in ("ON", "CLOSE_ONLY", "OFF"):
        return JSONResponse({"ok": False, "error": f"invalid mode '{mode}'"}, status_code=400)
    gate = _ensure_gate_population()
    for wallet in _wallet_universe_from_state():
        if USER_WALLET and wallet.lower() == USER_WALLET.lower():
            continue
        entry = gate.get(wallet, {})
        if not isinstance(entry, dict):
            entry = {}
        entry["mode"] = mode
        gate[wallet] = entry
    _save_wallet_gate(gate)
    return JSONResponse({"ok": True, "mode": mode, "wallets_updated": len(gate)})


@app.get("/api/wallet-gate-debug")
def wallet_gate_debug() -> JSONResponse:
    gate = _ensure_gate_population()
    universe = _wallet_universe_from_state()
    missing = [wallet for wallet in universe if wallet not in gate]
    first_items = []
    for wallet in sorted(gate.keys())[:5]:
        first_items.append({"wallet": wallet, "entry": gate.get(wallet)})
    return JSONResponse({
        "wallet_gate_file": str(WALLET_GATE_FILE),
        "exists": WALLET_GATE_FILE.exists(),
        "gate_count": len(gate),
        "universe_count": len(universe),
        "missing_wallets": missing,
        "first_items": first_items,
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("HL_Copy_App:app", host="127.0.0.1", port=8000, reload=False)

