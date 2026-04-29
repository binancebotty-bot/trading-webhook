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
PORTFOLIO_HISTORY_FILE = DATA_DIR / "portfolio_history.json"
EQUITY_HISTORY_FILE = DATA_DIR / "equity_history.json"
UI_STATE_FILE = BASE_DIR / "ui_state.json"
WALLET_GATE_FILE = BASE_DIR / "wallet_gate.json"
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
    if isinstance(truth, dict):
        return truth
    legacy = load_json(LEGACY_LIVE_STATE_JSON, {})
    return legacy if isinstance(legacy, dict) else {}


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
    return {
        "norm_base": max(1.0, fnum(raw.get("norm_base"), DEFAULT_NORM_BASE)),
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
    existing = load_ui_state()
    merged = {**existing, **patch}
    mode = str(merged.get("copy_mode", "proportional")).lower()
    merged["copy_mode"] = mode if mode in {"proportional", "fixed"} else "proportional"
    merged["norm_base"] = max(1.0, fnum(merged.get("norm_base"), DEFAULT_NORM_BASE))
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
    with RAW_FILLS_CSV.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fill = parse_raw_fill_row(row)
            if fill is None or fill.fill_id in seen:
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
        if first_ts and last_ts > first_ts:
            pnl_hr = m.copy_realized / max(1 / 60, (last_ts - first_ts) / 3_600_000.0)
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
            "pnl_per_trade": round((m.copy_realized / m.exit_count) if m.exit_count else 0.0, 8),
            "current_position_usd": round(m.current_position_usd, 8),
            "avg_position_usd": round(avg_position_usd, 8),
            "max_position_usd": round(m.max_position_usd, 8),
            "avg_entry_notional_usd": round(avg_entry_notional_usd, 8),
            "max_entry_notional_usd": round(m.max_entry_notional_usd, 8),
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
    included_wallets = {str(r.get("wallet", "")).lower() for r in portfolio_wallets}
    included_trade_returns = [fnum(t.get("return_pct")) for t in trades if str(t.get("wallet", "")).lower() in included_wallets]
    portfolio_avg_trade_pct = avg(included_trade_returns)
    alloc = sum(fnum(r["alloc"]) for r in portfolio_wallets)
    lead_equity = sum(fnum(r["lead"]["equity"]) for r in portfolio_wallets)
    copy_equity = sum(fnum(r["copy"]["equity"]) for r in portfolio_wallets)
    lead_real = sum(fnum(r["lead"]["realized"]) for r in portfolio_wallets)
    lead_unreal = sum(fnum(r["lead"]["unrealized"]) for r in portfolio_wallets)
    copy_real = sum(fnum(r["copy"]["realized"]) for r in portfolio_wallets)
    copy_unreal = sum(fnum(r["copy"]["unrealized"]) for r in portfolio_wallets)
    lead_peak = max(alloc, lead_equity)
    copy_peak = max(alloc, copy_equity)
    open_notional_usd = sum(fnum(r.get("current_position_usd")) for r in portfolio_wallets)
    avg_position_usd = avg([fnum(r.get("avg_position_usd")) for r in portfolio_wallets if fnum(r.get("avg_position_usd")) > 0])
    max_required_leverage = max([fnum(r.get("required_leverage")) for r in portfolio_wallets] or [0.0])
    # Rebuild combined graph by timestamp using latest wallet lead/copy equity at each event.
    portfolio_history = build_portfolio_history(rows, ui)
    if portfolio_history:
        lead_peak = max(fnum(p.get("lead", {}).get("equity")) for p in portfolio_history)
        copy_peak = max(fnum(p.get("copy", {}).get("equity")) for p in portfolio_history)
    lead_live_dd = max(0.0, lead_peak - lead_equity)
    copy_live_dd = max(0.0, copy_peak - copy_equity)
    max_open_notional_usd = max([fnum(p.get("open_notional_usd")) for p in portfolio_history] or [open_notional_usd])
    portfolio = {
        "ts": model_asof,
        "lead": block(lead_equity, lead_real, lead_unreal, lead_live_dd, max((fnum(p.get("lead", {}).get("drawdown")) for p in portfolio_history), default=lead_live_dd), lead_peak, alloc),
        "copy": block(copy_equity, copy_real, copy_unreal, copy_live_dd, max((fnum(p.get("copy", {}).get("drawdown")) for p in portfolio_history), default=copy_live_dd), copy_peak, alloc),
        "delta": {"equity": round(copy_equity - lead_equity, 8), "realized": round(copy_real - lead_real, 8), "pct": round(((copy_equity - lead_equity) / alloc * 100.0) if alloc else 0.0, 8)},
        "open_notional_usd": round(open_notional_usd, 8),
        "max_open_notional_usd": round(max_open_notional_usd, 8),
        "avg_position_usd": round(avg_position_usd, 8),
        "max_required_leverage": round(max_required_leverage, 8),
        "avg_trade_pct": round(portfolio_avg_trade_pct, 8),
        "position_alignment_errors": position_alignment_errors,
        "position_alignment_ok": not position_alignment_errors,
    }


    # USER row is a display-only aggregate of all tracked non-user copy models.
    # It keeps the same normalisation base as every other row; it does not use
    # the full portfolio allocation as its displayed starting equity.
    user_row = next((r for r in rows if r.get("is_user_wallet")), None)
    if user_row is not None:
        total_lead_pnl = lead_real + lead_unreal
        total_copy_pnl = copy_real + copy_unreal
        total_exits = sum(int(r.get("exit_count") or 0) for r in portfolio_wallets)
        total_wins = sum(int(round(fnum(r.get("win_rate")) * max(0, int(r.get("exit_count") or 0)) / 100.0)) for r in portfolio_wallets)

        user_curve: List[Dict[str, Any]] = []
        u_lead_peak = norm_base
        u_copy_peak = norm_base
        for point in portfolio_history:
            p_alloc = fnum(point.get("alloc"), alloc)
            lead_point = point.get("lead", {}) if isinstance(point.get("lead"), dict) else {}
            copy_point = point.get("copy", {}) if isinstance(point.get("copy"), dict) else {}
            lead_pnl_point = fnum(lead_point.get("equity")) - p_alloc
            copy_pnl_point = fnum(copy_point.get("equity")) - p_alloc
            u_lead_eq = norm_base + lead_pnl_point
            u_copy_eq = norm_base + copy_pnl_point
            u_lead_peak = max(u_lead_peak, u_lead_eq)
            u_copy_peak = max(u_copy_peak, u_copy_eq)
            u_lead_dd = max(0.0, u_lead_peak - u_lead_eq)
            u_copy_dd = max(0.0, u_copy_peak - u_copy_eq)
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
                "lead_equity": round(norm_base + total_lead_pnl, 8),
                "lead_drawdown": max(0.0, -total_lead_pnl),
                "copy_pnl": round(total_copy_pnl, 8),
                "copy_equity": round(norm_base + total_copy_pnl, 8),
                "copy_drawdown": max(0.0, -total_copy_pnl),
                "pnl": round(total_copy_pnl, 8),
                "equity": round(norm_base + total_copy_pnl, 8),
                "drawdown": max(0.0, -total_copy_pnl),
            })

        u_lead_equity = norm_base + total_lead_pnl
        u_copy_equity = norm_base + total_copy_pnl
        u_lead_peak = max(norm_base, max((fnum(p.get("lead_equity")) for p in user_curve), default=u_lead_equity))
        u_copy_peak = max(norm_base, max((fnum(p.get("copy_equity")) for p in user_curve), default=u_copy_equity))
        u_lead_dd = max(0.0, u_lead_peak - u_lead_equity)
        u_copy_dd = max(0.0, u_copy_peak - u_copy_equity)
        u_lead_maxdd = max((fnum(p.get("lead_drawdown")) for p in user_curve), default=u_lead_dd)
        u_copy_maxdd = max((fnum(p.get("copy_drawdown")) for p in user_curve), default=u_copy_dd)

        user_row.update({
            "alloc": norm_base,
            "lead": block(u_lead_equity, lead_real, lead_unreal, u_lead_dd, u_lead_maxdd, u_lead_peak, norm_base),
            "copy": block(u_copy_equity, copy_real, copy_unreal, u_copy_dd, u_copy_maxdd, u_copy_peak, norm_base),
            "delta": {"equity": round(total_copy_pnl - total_lead_pnl, 8), "pct": round(((total_copy_pnl - total_lead_pnl) / norm_base * 100.0) if norm_base else 0.0, 8)},
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
            "entry_count": sum(int(r.get("entry_count") or 0) for r in portfolio_wallets),
            "exit_count": total_exits,
            "open_position_count": sum(int(r.get("open_position_count") or 0) for r in portfolio_wallets),
            "fill_count": sum(int(r.get("fill_count") or 0) for r in portfolio_wallets),
            "ws_fill_count": 0,
            "poll_fill_count": sum(int(r.get("poll_fill_count") or 0) for r in portfolio_wallets),
            "ws_coverage": 0.0,
            "rebuild_fill_count": sum(int(r.get("rebuild_fill_count") or 0) for r in portfolio_wallets),
            "measured_delta_fill_count": 0,
            "measured_delta_entry_count": 0,
            "measured_delta_exit_count": 0,
            "avg_entry_disadvantage_bps": 0.0,
            "avg_exit_disadvantage_bps": 0.0,
            "win_rate": round((total_wins / total_exits * 100.0) if total_exits else 0.0, 6),
            "pnl_per_hour": round(sum(fnum(r.get("pnl_per_hour")) for r in portfolio_wallets), 8),
            "pnl_per_trade": round((copy_real / total_exits) if total_exits else 0.0, 8),
            "avg_trade_pct": round(portfolio_avg_trade_pct, 8),
            "current_position_usd": round(open_notional_usd, 8),
            "avg_position_usd": round(avg_position_usd, 8),
            "max_position_usd": round(max_open_notional_usd, 8),
            "required_leverage": round((max_open_notional_usd / norm_base) if norm_base else 0.0, 8),
            "flags": [] if portfolio.get("position_alignment_ok", True) else ["POSITION_ALIGNMENT_ERROR"],
            "gate": {"mode": "USER_AGGREGATE", "off_mode": None},
            "curve": user_curve[-EQUITY_HISTORY_MAX:],
        })

    state = {
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
        lead_peak = max(lead_peak or alloc, lead_equity)
        copy_peak = max(copy_peak or alloc, copy_equity)
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


def css_class(v: Any) -> str:
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
    return None


def max_history_block_dd(history: List[Dict[str, Any]], side: str) -> Optional[float]:
    vals: List[float] = []
    for point in history or []:
        block = point.get(side) if isinstance(point, dict) else None
        if isinstance(block, dict):
            vals.append(max(0.0, block_num(block, "drawdown", "drawdown_usd", default=0.0)))
        elif side == "copy" and isinstance(point, dict):
            vals.append(max(0.0, block_num(point, "drawdown", "drawdown_usd", default=0.0)))
    return max(vals) if vals else None


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

    def short_time(t: float) -> str:
        try:
            return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%H:%M")
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
        grid_bits.append(f'<text x="{x:.1f}" y="{h-8}" class="axis-label" text-anchor="middle">{short_time(t)}</text>')
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
    col = ranking.get("column")
    direction = str(ranking.get("direction", "desc")).lower()
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

    reverse = direction == "desc" and col is not None
    user_rows = [r for r in rows if r.get("is_user_wallet")]
    included_rows = [r for r in rows if not r.get("is_user_wallet") and bool(r.get("include_in_portfolio", True))]
    other_rows = [r for r in rows if not r.get("is_user_wallet") and not bool(r.get("include_in_portfolio", True))]
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
    rows = sorted_rows(state)
    metric_rows = [r for r in rows if not r.get("is_user_wallet")]
    included_rows = [r for r in metric_rows if r.get("include_in_portfolio", True)]
    user_base = max(1.0, base)
    lead_total = fnum(lead.get("realized")) + fnum(lead.get("unrealized"))
    copy_total = fnum(copy.get("realized")) + fnum(copy.get("unrealized"))
    delta_total = fnum(delta.get("equity"))
    hist = state.get("portfolio_history", []) if isinstance(state.get("portfolio_history", []), list) else []
    lead_dd = block_num(lead, "drawdown", "drawdown_usd", default=0.0)
    copy_dd = block_num(copy, "drawdown", "drawdown_usd", default=0.0)
    lead_maxdd = block_num(lead, "max_drawdown", "maxdd", default=0.0)
    copy_maxdd = block_num(copy, "max_drawdown", "maxdd", default=0.0)
    if lead_dd == 0.0:
        lead_dd = latest_history_block_dd(hist, "lead") if latest_history_block_dd(hist, "lead") is not None else lead_dd
    if copy_dd == 0.0:
        copy_dd = latest_history_block_dd(hist, "copy") if latest_history_block_dd(hist, "copy") is not None else copy_dd
    hist_lead_maxdd = max_history_block_dd(hist, "lead")
    hist_copy_maxdd = max_history_block_dd(hist, "copy")
    lead_maxdd = max(lead_maxdd, hist_lead_maxdd or 0.0)
    copy_maxdd = max(copy_maxdd, hist_copy_maxdd or 0.0)
    open_notional = fnum(port.get("open_notional_usd"), sum(fnum(r.get("current_position_usd")) for r in included_rows))
    max_open_notional = fnum(port.get("max_open_notional_usd"), open_notional)
    notional_x = open_notional / user_base if user_base else 0.0
    max_notional_x = max_open_notional / user_base if user_base else 0.0
    avg_pos_size = fnum(port.get("avg_position_usd"), avg([fnum(r.get("avg_position_usd")) for r in included_rows if fnum(r.get("avg_position_usd")) > 0]))
    max_req_lev = fnum(port.get("max_required_leverage"), max([fnum(r.get("required_leverage")) for r in included_rows] or [0.0]))
    win_avg = avg([fnum(r.get("win_rate")) for r in included_rows if r.get("exit_count")])
    avg_trade = avg([fnum(r.get("pnl_per_trade")) for r in included_rows if r.get("exit_count")])
    avg_trade_pct = fnum(port.get("avg_trade_pct"), avg([fnum(r.get("avg_trade_pct")) for r in included_rows if r.get("exit_count")]))
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
        group_card("DRAWDOWN", [small_metric("LEAD DD", dual(-lead_dd, user_base), -lead_dd), small_metric("COPY DD", dual(-copy_dd, user_base), -copy_dd), small_metric("COPY MAX", dual(-copy_maxdd, user_base), -copy_maxdd)]),
        group_card("MAX DD", [small_metric("LEAD", dual(-lead_maxdd, user_base), -lead_maxdd), small_metric("COPY", dual(-copy_maxdd, user_base), -copy_maxdd)]),
        group_card("EXPOSURE", [small_metric("OPEN", money(open_notional), open_notional), small_metric("MAX", money(max_open_notional), max_open_notional), small_metric("BASE", f"{notional_x:.2f}x / {max_notional_x:.2f}x", notional_x)]),
        group_card("COPYABILITY", [small_metric("AVG TRADE %", pct(avg_trade_pct, 3), avg_trade_pct), small_metric("AVG TRADE $", money(avg_trade), avg_trade), small_metric("AVG POS", money(avg_pos_size), avg_pos_size), small_metric("REQ LEV", f"{max_req_lev:.2f}x", max_req_lev)]),
        group_card("ACTIVITY", [small_metric("FILLS", str(fill_count), fill_count), small_metric("EXITS", str(exit_count), exit_count), small_metric("OPEN POS", str(open_positions), open_positions), small_metric("WIN", pct(win_avg), win_avg)]),
        group_card("DB HEALTH", [small_metric(health_label, html.escape(health_detail), -1 if "ERROR" in health_label else 0), small_metric("CACHE", str(APP_HEALTH.get("cache_hits", 0)), 0), small_metric("BUILDS", str(APP_HEALTH.get("build_count", 0)), 0)], "health-card"),
    ])
    ranking = ui.get("ranking", {}) if isinstance(ui.get("ranking"), dict) else {}
    active_col = str(ranking.get("column") or ""); active_dir = str(ranking.get("direction") or "desc")
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
        th("fill_count", "FILLS", "ops-group"), th("exit_count", "EXITS", "ops-group"), th("open_position_count", "POS", "ops-group group-divider"),
    ]) + "<th>INC / MODE / WALLET MODEL</th></tr>"
    body_rows = "\n".join(render_row(r, base, ui) for r in rows)
    return HTML_TEMPLATE.format(updated=state.get("updated_at", ""), norm=base, mode=str(ui.get("copy_mode", "proportional")).upper(), fixed=fnum(ui.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL), fee=fnum(ui.get("fee_bps"), DEFAULT_FEE_BPS), friction=fnum(ui.get("copy_friction_bps"), DEFAULT_COPY_FRICTION_BPS), cards=cards, chart=render_chart(state.get("portfolio_history", [])), wallet_count=len(metric_rows), table_head=table_head, table_rows=body_rows, raw_boundary=state.get("engine_truth_boundary", ""))


def extract_num(s: str) -> float:
    try:
        t = str(s).replace("$", "").replace(",", "").replace("%", "").split()[0]
        return float(t)
    except Exception:
        return 0.0


def avg(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def render_row(r: Dict[str, Any], base: float, ui: Dict[str, Any]) -> str:
    lead = r.get("lead", {}); copy = r.get("copy", {}); delta = r.get("delta", {})
    wallet = str(r.get("wallet", "")); badge = " <span class='badge'>USER</span>" if r.get("is_user_wallet") else ""
    mode = str((r.get("gate") or {}).get("mode") or "OFF")
    alloc = fnum(r.get('alloc'), base)
    lead_pnl = fnum(lead.get('realized')) + fnum(lead.get('unrealized'))
    copy_pnl = fnum(copy.get('realized')) + fnum(copy.get('unrealized'))
    lead_dd_raw = block_num(lead, "drawdown", "drawdown_usd", default=0.0)
    copy_dd_raw = block_num(copy, "drawdown", "drawdown_usd", default=0.0)
    lead_maxdd_raw = block_num(lead, "max_drawdown", "maxdd", default=0.0)
    copy_maxdd_raw = block_num(copy, "max_drawdown", "maxdd", default=0.0)
    # User aggregate row carries a curve; use it as a display fallback so DD
    # cannot appear unplumbed if block aliases are missing/stale.
    if r.get("is_user_wallet"):
        curve = r.get("curve", []) if isinstance(r.get("curve", []), list) else []
        lead_curve_dd = latest_history_block_dd(curve, "lead")
        copy_curve_dd = latest_history_block_dd(curve, "copy")
        lead_curve_maxdd = max_history_block_dd(curve, "lead")
        copy_curve_maxdd = max_history_block_dd(curve, "copy")
        if lead_dd_raw == 0.0 and lead_curve_dd is not None:
            lead_dd_raw = lead_curve_dd
        if copy_dd_raw == 0.0 and copy_curve_dd is not None:
            copy_dd_raw = copy_curve_dd
        lead_maxdd_raw = max(lead_maxdd_raw, lead_curve_maxdd or 0.0)
        copy_maxdd_raw = max(copy_maxdd_raw, copy_curve_maxdd or 0.0)
    lead_dd_val = -lead_dd_raw; lead_maxdd_val = -lead_maxdd_raw
    copy_dd_val = -copy_dd_raw; copy_maxdd_val = -copy_maxdd_raw
    eff_mode = str(r.get("effective_copy_mode") or ui.get("copy_mode", "proportional"))
    eff_base = fnum(r.get("effective_norm_base"), fnum(r.get("alloc"), base)); eff_fixed = fnum(r.get("effective_fixed_notional"), fnum(ui.get("fixed_notional"), DEFAULT_FIXED_NOTIONAL))
    override_badge = " *" if r.get("wallet_config") else ""; included = bool(r.get("include_in_portfolio", True))
    inc_cell = ""
    if not r.get("is_user_wallet"):
        inc_cell = f"""<form action="/api/wallet-include" method="post" class="inc-form" title="Include/exclude this wallet from combined graph and header cards only"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><input type="checkbox" name="included" value="1" {'checked' if included else ''} onchange="this.form.requestSubmit()"><span class="small">INC</span></form>"""
    cfg_cell = f"""<form action="/api/wallet-config" method="post" class="wallet-cfg"><input type="hidden" name="wallet" value="{html.escape(wallet)}"><select name="copy_mode"><option value="" {'selected' if not r.get('wallet_config') else ''}>global</option><option value="proportional" {'selected' if eff_mode == 'proportional' and r.get('wallet_config') else ''}>prop</option><option value="fixed" {'selected' if eff_mode == 'fixed' and r.get('wallet_config') else ''}>fixed</option></select><input name="norm_base" value="{eff_base:g}" size="5" title="wallet normalisation base"><input name="fixed_notional" value="{eff_fixed:g}" size="4" title="wallet fixed $"><button title="save wallet override">Set</button></form>"""
    row_cls = 'user' if r.get('is_user_wallet') else ''
    if not r.get('is_user_wallet') and not included: row_cls += ' excluded-row'
    def td(sort_value: Any, content: str, cls: str = "") -> str:
        return f'<td class="{cls}" data-sort="{fnum(sort_value):.12g}">{content}</td>'
    wallet_sort = html.escape(wallet)
    return f"""
    <tr class="{row_cls}">
      <td class="sticky-wallet" data-sort="{wallet_sort}"><a href="/wallet/{wallet}">{wallet[:8]}…{wallet[-6:]}</a>{badge}</td>
      {td(lead_pnl, money(lead.get('equity')), f"pair-lead {css_class(lead_pnl)}")}{td(copy_pnl, money(copy.get('equity')), f"pair-copy group-divider {css_class(copy_pnl)}")}
      {td(lead.get('realized'), dual(fnum(lead.get('realized')), alloc), f"pair-lead {css_class(lead.get('realized'))}")}{td(copy.get('realized'), dual(fnum(copy.get('realized')), alloc), f"pair-copy group-divider {css_class(copy.get('realized'))}")}
      {td(lead.get('unrealized'), dual(fnum(lead.get('unrealized')), alloc), f"pair-lead {css_class(lead.get('unrealized'))}")}{td(copy.get('unrealized'), dual(fnum(copy.get('unrealized')), alloc), f"pair-copy group-divider {css_class(copy.get('unrealized'))}")}
      {td(lead_dd_val, dual(lead_dd_val, alloc), f"pair-lead {css_class(lead_dd_val)}")}{td(copy_dd_val, dual(copy_dd_val, alloc), f"pair-copy group-divider {css_class(copy_dd_val)}")}
      {td(lead_maxdd_val, dual(lead_maxdd_val, alloc), f"pair-lead {css_class(lead_maxdd_val)}")}{td(copy_maxdd_val, dual(copy_maxdd_val, alloc), f"pair-copy group-divider {css_class(copy_maxdd_val)}")}
      {td(delta.get('equity'), dual(fnum(delta.get('equity')), alloc), f"group-divider {css_class(delta.get('equity'))}")}{td(r.get('pnl_per_hour'), dual(fnum(r.get('pnl_per_hour')), alloc), css_class(r.get('pnl_per_hour')))}
      {td(r.get('avg_trade_pct'), pct(r.get('avg_trade_pct'), 3), css_class(r.get('avg_trade_pct')))}{td(r.get('win_rate'), pct(r.get('win_rate')), css_class(r.get('win_rate')))}
      {td(r.get('avg_position_usd'), money(r.get('avg_position_usd')), css_class(r.get('avg_position_usd')))}{td(r.get('max_position_usd'), money(r.get('max_position_usd')), css_class(r.get('max_position_usd')))}
      {td(r.get('avg_entry_notional_usd'), money(r.get('avg_entry_notional_usd')), css_class(r.get('avg_entry_notional_usd')))}{td(r.get('pct_entries_ge10'), pct(r.get('pct_entries_ge10')), css_class(r.get('pct_entries_ge10')))}{td(r.get('required_leverage'), f"{fnum(r.get('required_leverage')):.2f}x", css_class(r.get('required_leverage')))}
      {td(r.get('fill_count'), str(int(r.get('fill_count') or 0)), "ops-group")}{td(r.get('exit_count'), str(int(r.get('exit_count') or 0)), "ops-group")}{td(r.get('open_position_count'), str(int(r.get('open_position_count') or 0)), "ops-group group-divider")}
      <td class="controls-cell {'inc-off' if (not r.get('is_user_wallet') and not included) else ''}">{inc_cell}<span class="mode">{html.escape(mode)}{override_badge}</span>{cfg_cell}</td>
    </tr>"""

HTML_TEMPLATE = """
<!doctype html><html><head><meta charset="utf-8"><title>HL Copy Engine SSOT</title>
<style>
body{{margin:0;background:#0d1117;color:#c9d1d9;font:12px Arial,Helvetica,sans-serif}} a{{color:#58a6ff;text-decoration:none}} .top{{display:flex;align-items:center;gap:12px;padding:8px 14px;border-bottom:1px solid #222;background:#090d12;position:sticky;top:0;z-index:4;box-shadow:0 2px 8px rgba(0,0,0,.25)}} .live{{background:#003d1f;color:#2ea043;border:1px solid #2ea043;border-radius:12px;padding:2px 8px;font-size:10px}} .muted{{color:#8b949e}} input,select,button{{background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:4px;padding:3px 8px}} button{{cursor:pointer}}
.cards{{display:grid;grid-template-columns:repeat(9,minmax(130px,1fr));gap:8px;padding:10px 14px}} .card{{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:9px;min-height:72px}} .group-card .label{{font-size:10px;color:#8b949e;margin-bottom:6px;border-bottom:1px solid #21262d;padding-bottom:4px}} .metric-line{{display:flex;justify-content:space-between;gap:8px;line-height:1.55}} .metric-line span{{color:#8b949e}} .metric-line b{{font-weight:700}} .pos{{color:#2ea043}} .neg{{color:#ff4d4f}} .zero{{color:#c9d1d9}}
.section{{padding:0 14px 10px}} .panel{{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px;margin-bottom:12px}} .chart-wrap{{position:relative;cursor:zoom-in}} .chart-wrap.expanded{{position:relative;z-index:20}} .chart-wrap.expanded .chart{{height:76vh}} .chart{{width:100%;height:260px;background:#151a21}} .chart *{{vector-effect:non-scaling-stroke}} .pnl-line{{fill:none;stroke:#2ea043;stroke-width:1.6;stroke-linejoin:round;stroke-linecap:round}} .realized-line{{fill:none;stroke:#58a6ff;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .dd-line{{fill:none;stroke:#ff4d4f;stroke-width:1.4;stroke-linejoin:round;stroke-linecap:round}} .zero-line{{stroke:#8b949e;stroke-width:1}} .grid-line,.grid-vert{{stroke:#21262d;stroke-width:1}} .axis-label{{fill:#8b949e;font-size:10px}} .hit{{fill:transparent;stroke:none;pointer-events:all}} .crosshair{{stroke:#8b949e;stroke-width:1;stroke-dasharray:3 3;pointer-events:none}} .chart-dot{{fill:#c9d1d9;stroke:#0d1117;stroke-width:1.2;pointer-events:none}} .chart-tip{{position:absolute;left:10px;top:10px;background:#0d1117;border:1px solid #30363d;border-radius:4px;padding:5px 7px;color:#c9d1d9;font-size:11px;pointer-events:none;box-shadow:0 4px 12px rgba(0,0,0,.35)}} .chart-legend{{display:flex;gap:10px;align-items:center;margin-top:6px}} .legend-pnl{{color:#2ea043}} .legend-realized{{color:#58a6ff}} .legend-dd{{color:#ff4d4f}}
.table-wrap{{overflow:auto;border:1px solid #21262d;border-radius:6px;background:#0d1117;max-height:72vh}} table{{width:100%;border-collapse:separate;border-spacing:0;font-size:11px}} th{{position:sticky;top:0;background:#21262d;color:#8b949e;text-align:right;padding:7px;border-bottom:1px solid #30363d;z-index:2}} th:first-child,td:first-child{{text-align:left}} th.sort-active{{background:#303a49;color:#fff;box-shadow:inset 0 -2px 0 #58a6ff}} th.sort-active a{{color:#fff}} th a{{display:block;color:#8b949e}} td{{padding:6px;border-bottom:1px solid #21262d;text-align:right;white-space:nowrap}} tr:nth-child(even){{background:#111820}} tr.user{{background:#071527}} tr:hover{{background:#1b2330}} tr.excluded-row{{opacity:.55}}
.sticky-wallet{{position:sticky;left:0;z-index:3;background:inherit;min-width:132px;border-right:1px solid #30363d}} th.sticky-wallet{{z-index:4;background:#21262d}} .pair-lead{{background:rgba(88,166,255,.045)}} .pair-copy{{background:rgba(46,160,67,.045)}} .ops-group{{background:rgba(210,153,34,.05)}} .group-divider{{border-right:2px solid #30363d!important}} .badge{{background:#0d419d;color:#fff;border-radius:3px;padding:1px 4px;font-size:9px}} .mode{{background:#063d1f;color:#2ea043;border-radius:3px;padding:2px 6px}} .wallet-cfg{{display:inline-flex;gap:3px;margin-left:4px;align-items:center}} .wallet-cfg input{{width:48px;padding:1px 3px}} .wallet-cfg select{{width:70px;padding:1px 3px}} .wallet-cfg button{{padding:1px 4px}} .inc-form{{display:inline-flex;align-items:center;gap:2px;margin-right:6px}} .inc-form input{{padding:0;width:14px;height:14px}} .inc-off{{opacity:.45}} .controls-cell{{min-width:270px;text-align:left}} .small{{font-size:11px;color:#8b949e}} .saving{{opacity:.65}} .saved-flash{{color:#2ea043}} .chart-empty{{height:230px;display:flex;align-items:center;justify-content:center;color:#8b949e}} @media(max-width:1300px){{.cards{{grid-template-columns:repeat(3,minmax(150px,1fr))}}}}
</style></head><body><div class="top"><b>⚡ HL Copy Engine</b><span class="live">POLL</span><span class="muted">Updated: {updated}</span><form action="/api/ui-state" method="post" class="ajax-form" style="display:flex;gap:6px;align-items:center;margin:0"><span class="muted">Normalisation Base:</span><input name="norm_base" value="{norm}" size="8"><span class="muted">Mode:</span><select name="copy_mode"><option>proportional</option><option>fixed</option></select><span class="muted">Fixed $:</span><input name="fixed_notional" value="{fixed}" size="6"><span class="muted">Fee bps:</span><input name="fee_bps" value="{fee}" size="5"><span class="muted">Copy friction bps:</span><input name="copy_friction_bps" value="{friction}" size="5"><button>Set</button></form><span class="muted">Current: {mode}</span><span id="save-status" class="muted"></span><span style="margin-left:auto" class="muted">Idle refresh 15s</span></div><div class="cards">{cards}</div><div class="section"><b>COMBINED PORTFOLIO — NON-USER WALLETS</b><div class="panel">{chart}</div><div class="small">TRACKED WALLETS ({wallet_count}) — model derived in app from engine SSOT only. {raw_boundary}</div><div class="table-wrap"><table><thead>{table_head}</thead><tbody>{table_rows}</tbody></table></div></div>
<script>(function(){{
let busyUntil=0;
const status=document.getElementById('save-status');
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
  const includedRows=rows.filter(r=>!r.classList.contains('user')&&!r.classList.contains('excluded-row'));
  const otherRows=rows.filter(r=>!r.classList.contains('user')&&r.classList.contains('excluded-row'));
  const isWallet=(link.getAttribute('href')||'').endsWith('/wallet');
  const sorter=(a,b)=>{{
    const av=cellSortValue(a,idx,isWallet), bv=cellSortValue(b,idx,isWallet);
    if(isWallet){{return direction==='asc'?av.localeCompare(bv):bv.localeCompare(av);}}
    return direction==='asc'?av-bv:bv-av;
  }};
  includedRows.sort(sorter);
  otherRows.sort(sorter);
  tbody.replaceChildren(...userRows,...includedRows,...otherRows);
  table.querySelectorAll('th').forEach(h=>{{h.classList.remove('sort-active'); const a=h.querySelector('a'); if(a&&a.dataset.baseLabel){{a.textContent=a.dataset.baseLabel;}}}});
  th.classList.add('sort-active');
  if(!link.dataset.baseLabel) link.dataset.baseLabel=link.textContent.replace(/\\s*[▲▼]$/,'');
  link.textContent=link.dataset.baseLabel+(direction==='asc'?' ▲':' ▼');
}}

document.addEventListener('focusin',e=>{{if(e.target.matches('input,select,textarea'))markBusy(30000);}});
document.addEventListener('input',e=>{{if(e.target.matches('input,select,textarea'))markBusy(30000);}});
document.addEventListener('change',e=>{{if(e.target.matches('input,select,textarea'))markBusy(15000);}});
document.addEventListener('submit',async e=>{{
  const form=e.target;if(!form.matches('.ajax-form,.wallet-cfg,.inc-form'))return;
  e.preventDefault();markBusy(5000);form.classList.add('saving');
  try{{const r=await fetch(form.action,{{method:'POST',body:new FormData(form),headers:{{'X-Requested-With':'fetch'}}}});if(!r.ok)throw new Error('HTTP '+r.status);flash('saved','saved-flash');}}
  catch(err){{console.warn('save failed',err);flash('save failed','neg');}}
  finally{{form.classList.remove('saving');}}
}});

document.addEventListener('click',async e=>{{
  const sortLink=e.target.closest('th a[href^="/sort/"]');
  if(sortLink){{
    e.preventDefault(); markBusy(2500);
    const th=sortLink.closest('th');
    const current=th.classList.contains('sort-active') && /▼\\s*$/.test(sortLink.textContent) ? 'desc' : th.classList.contains('sort-active') ? 'asc' : '';
    const next=current==='desc'?'asc':'desc';
    sortTableByHeader(sortLink,next);
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

setInterval(()=>{{if(!isBusy())location.reload();}},15000);
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


@app.get("/wallet/{wallet}", response_class=HTMLResponse)
def wallet_detail(wallet: str) -> str:
    wallet = wallet.lower()
    state = get_model_state_cached(max_age_sec=10.0)
    row = (state.get("wallets") or {}).get(wallet)
    if not row:
        return HTMLResponse(f"<h3>Wallet not found: {wallet}</h3>", status_code=404)
    trades = [t for t in state.get("copy_trades", []) if str(t.get("wallet", "")).lower() == wallet][-100:]
    trade_rows = "".join(f"<tr><td>{t['trade_id']}</td><td>{t['coin']}</td><td>{t['entry_time_iso']}</td><td>{t['exit_time_iso']}</td><td>{money(t['copy_pnl'])}</td><td>{t['return_pct']}%</td></tr>" for t in trades)
    html = render_home({**state, "wallet_rows": [row], "portfolio_history": row.get("curve", [])})
    return html + f"<div class='section'><h3>{wallet}</h3><table><tr><th>TRADE</th><th>COIN</th><th>ENTRY</th><th>EXIT</th><th>PNL</th><th>RET%</th></tr>{trade_rows}</table></div>"


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
    for k in ("norm_base", "copy_mode", "normalisation_mode", "fixed_notional", "leader_equity_base", "fee_bps", "copy_friction_bps"):
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
        "wallet", "lead_equity", "lead_real", "lead_realized", "lead_unreal", "lead_unrealized",
        "lead_dd", "lead_drawdown", "lead_maxdd", "lead_max_drawdown",
        "copy_equity", "copy_real", "copy_realized", "copy_unreal", "copy_unrealized",
        "copy_dd", "copy_drawdown", "copy_maxdd", "copy_max_drawdown",
        "delta", "delta_pct", "copy_total_pnl", "lead_total_pnl", "copy_efficiency",
        "pnl_per_hour", "pnl_per_trade", "avg_trade_pct", "win_rate", "ws", "ws_fill_count",
        "fill_count", "exit_count", "rebuild_fill_count", "measured_delta_fill_count",
        "avg_ws_latency_ms", "expected_price_coverage_pct",
        "avg_entry_disadvantage_bps", "avg_exit_disadvantage_bps",
        "open_position_count", "avg_position_usd", "max_position_usd",
        "avg_entry_notional_usd", "max_entry_notional_usd", "pct_entries_ge10", "required_leverage",
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


if __name__ == "__main__":
    if uvicorn is None:
        raise SystemExit("Missing uvicorn. Install with: pip install uvicorn fastapi")
    uvicorn.run("HL_Copy_App_SSOT:app", host="127.0.0.1", port=8000, reload=False)
