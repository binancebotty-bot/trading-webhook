"""
real_dd_filter.py — Shared real-drawdown upstream filter.

Provides deterministic functions for extracting and gating real mark-to-market
drawdown from Hyperliquid's accountValueHistory (portfolio API). Replaces the
invalid closedPnl-only drawdown which understates risk by 1,000-3,600x.

Closes the gap between:
  - `hl_stage1_5_mtm_filter.py` (Stage Pipeline)
  - `full_universe_scan.py` (Full Universe Scan)

Both call into this module for consistent real-DD gating.

Usage:
    from real_dd_filter import (
        parse_max_real_dd_pct,
        real_dd_gate,
        load_real_dd_for_wallet,
        wallet_passes_real_dd,
    )

Sign rule:
    HL's accountValueHistory max drawdown (`allTime_max_drawdown_mtm`) is
    stored as a NEGATIVE number (peak-to-trough).  We use abs() throughout.

Forbidden:
    CLOSED_PNL_ONLY_DRAWDOWN as a risk source.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

__all__ = [
    "parse_max_real_dd_pct",
    "real_dd_gate",
    "load_real_dd_for_wallet",
    "wallet_passes_real_dd",
]

_HERE = Path(__file__).resolve().parent
_DEFAULT_CACHE_DIRS: list[Path] = [
    _HERE / "data" / "wallet_portfolios",
    _HERE / "copy_selection_run" / "wallet_portfolios",
]

# ---------------------------------------------------------------------------
# Source label helpers
# ---------------------------------------------------------------------------

_SOURCE_MAP: dict[str, str] = {
    "hl_portfolio_api": "FETCHED_ACCOUNT_VALUE_HISTORY",
    "cache_stale": "LOCAL_ACCOUNT_VALUE_HISTORY",
}

_DD_SOURCE_FETCHED = "FETCHED_ACCOUNT_VALUE_HISTORY"
_DD_SOURCE_LOCAL = "LOCAL_ACCOUNT_VALUE_HISTORY"
_DD_SOURCE_BLOCKED = "DATA_FETCH_BLOCKED"


def _dd_source_from_mtm(source: str | None) -> str:
    """Map mtm_source to dd_source."""
    if source in _SOURCE_MAP:
        return _SOURCE_MAP[source]
    return _DD_SOURCE_BLOCKED


# ---------------------------------------------------------------------------
# 1. parse_max_real_dd_pct
# ---------------------------------------------------------------------------

def parse_max_real_dd_pct(stats: dict) -> tuple[Optional[float], str]:
    """Extract real max drawdown percentage from an MTM stats dict.

    Parameters
    ----------
    stats : dict
        Must contain keys produced by ``hl_mtm_lookup.get_mtm_stats`` or
        ``hl_mtm_lookup._summarise``:
          - ``allTime_max_drawdown_mtm`` (float, **negative** peak-to-trough)
          - ``allTime_acctV_peak``      (float, positive peak account value)
          - ``mtm_source``              (str)

    Returns
    -------
    (dd_pct, dd_source)
        ``dd_pct`` is ``abs(allTime_max_drawdown_mtm) / allTime_acctV_peak * 100``
        or ``None`` when the data is missing.
        ``dd_source`` is one of:
          - ``FETCHED_ACCOUNT_VALUE_HISTORY``
          - ``LOCAL_ACCOUNT_VALUE_HISTORY``
          - ``DATA_FETCH_BLOCKED``

    Examples
    --------
    >>> parse_max_real_dd_pct({
    ...     "allTime_max_drawdown_mtm": -100,
    ...     "allTime_acctV_peak": 1000,
    ...     "mtm_source": "hl_portfolio_api",
    ... })
    (10.0, 'FETCHED_ACCOUNT_VALUE_HISTORY')

    >>> parse_max_real_dd_pct({"mtm_source": "unavailable"})
    (None, 'DATA_FETCH_BLOCKED')
    """
    mtm_source = stats.get("mtm_source", "unavailable")
    dd_source = _dd_source_from_mtm(mtm_source)

    # If source is blocked, short-circuit
    if dd_source == _DD_SOURCE_BLOCKED:
        return None, _DD_SOURCE_BLOCKED

    real_dd = stats.get("allTime_max_drawdown_mtm")
    peak = stats.get("allTime_acctV_peak")

    # Missing core data → blocked
    if real_dd is None or peak is None or peak <= 0:
        return None, _DD_SOURCE_BLOCKED

    # HL convention: drawdown is negative.  Use abs().
    dd_pct = abs(float(real_dd)) / float(peak) * 100.0
    return round(dd_pct, 4), dd_source


# ---------------------------------------------------------------------------
# 2. real_dd_gate
# ---------------------------------------------------------------------------

def real_dd_gate(stats: dict, max_real_dd_pct: Optional[float]) -> dict:
    """Evaluate the real-DD gate.

    Parameters
    ----------
    stats : dict
        MTM stats dict (same shape as ``parse_max_real_dd_pct`` input).
    max_real_dd_pct : float or None
        Threshold percentage.  ``None`` disables the gate (all wallets pass).

    Returns
    -------
    dict with keys:
        real_max_dd_usd, real_max_dd_pct, dd_source,
        dd_gate_pass, dd_gate_reason
    """
    # Gate disabled → pass
    if max_real_dd_pct is None:
        return {
            "real_max_dd_usd": None,
            "real_max_dd_pct": None,
            "dd_source": _DD_SOURCE_BLOCKED,
            "dd_gate_pass": True,
            "dd_gate_reason": "gate_disabled",
        }

    dd_pct, dd_source = parse_max_real_dd_pct(stats)
    real_dd_raw = stats.get("allTime_max_drawdown_mtm")
    real_max_dd_usd = abs(float(real_dd_raw)) if real_dd_raw is not None else None

    # Missing data → FAIL (never silently pass)
    if dd_pct is None:
        return {
            "real_max_dd_usd": real_max_dd_usd,
            "real_max_dd_pct": None,
            "dd_source": dd_source,
            "dd_gate_pass": False,
            "dd_gate_reason": "DATA_FETCH_BLOCKED",
        }

    # DD exceeds threshold → FAIL
    if dd_pct > max_real_dd_pct:
        return {
            "real_max_dd_usd": real_max_dd_usd,
            "real_max_dd_pct": dd_pct,
            "dd_source": dd_source,
            "dd_gate_pass": False,
            "dd_gate_reason": f"REAL_DD_TOO_HIGH_{dd_pct:.1f}pct",
        }

    # DD within threshold → PASS
    return {
        "real_max_dd_usd": real_max_dd_usd,
        "real_max_dd_pct": dd_pct,
        "dd_source": dd_source,
        "dd_gate_pass": True,
        "dd_gate_reason": "ok",
    }


# ---------------------------------------------------------------------------
# 3. load_real_dd_for_wallet
# ---------------------------------------------------------------------------

def _extract_dd_from_json(data: dict, wallet: str) -> dict:
    """Extract real DD from a raw HL portfolio API response.

    The response is a list of [period_label, metrics_dict] pairs.
    We prefer 'allTime' for the broadest drawdown picture.
    """
    if not isinstance(data, list):
        return {
            "wallet": wallet,
            "real_max_dd_usd": None,
            "real_max_dd_pct": None,
            "peak_account_value": None,
            "dd_source": _DD_SOURCE_BLOCKED,
            "fetch_error": "invalid_response_format",
        }

    periods = {}
    for entry in data:
        if isinstance(entry, list) and len(entry) == 2:
            periods[entry[0]] = entry[1]

    # Prefer allTime, fall back to month, then week, then day
    for period in ("allTime", "month", "week", "day"):
        blob = periods.get(period) or {}
        avh = blob.get("accountValueHistory") or []
        if avh:
            break

    if not avh:
        return {
            "wallet": wallet,
            "real_max_dd_usd": None,
            "real_max_dd_pct": None,
            "peak_account_value": None,
            "dd_source": _DD_SOURCE_BLOCKED,
            "fetch_error": "no_account_value_history",
        }

    # Compute peak-to-trough from the equity curve
    vals = [float(p[1]) for p in avh if isinstance(p, list) and len(p) >= 2]
    if not vals:
        return {
            "wallet": wallet,
            "real_max_dd_usd": None,
            "real_max_dd_pct": None,
            "peak_account_value": None,
            "dd_source": _DD_SOURCE_BLOCKED,
            "fetch_error": "empty_account_value_history",
        }

    peak = vals[0]
    running_max = vals[0]
    max_dd = 0.0
    for v in vals:
        if v > running_max:
            running_max = v
        dd = v - running_max
        if dd < max_dd:
            max_dd = dd

    peak_global = max(vals)
    real_max_dd_usd = abs(max_dd)
    real_max_dd_pct = (real_max_dd_usd / peak_global * 100.0) if peak_global > 0 else None

    return {
        "wallet": wallet,
        "real_max_dd_usd": round(real_max_dd_usd, 2) if real_max_dd_usd > 0 else 0.0,
        "real_max_dd_pct": round(real_max_dd_pct, 4) if real_max_dd_pct is not None else None,
        "peak_account_value": round(peak_global, 2),
        "dd_source": _DD_SOURCE_LOCAL,  # loaded from local cache
        "fetch_error": None,
    }


def load_real_dd_for_wallet(
    wallet: str,
    cache_dirs: list[Path] | None = None,
) -> dict:
    """Load real drawdown for a single wallet from cached portfolio JSONs.

    Parameters
    ----------
    wallet : str
        Wallet address (case-insensitive).
    cache_dirs : list of Path, optional
        Directories to search for ``{wallet}.json`` files.
        Defaults to ``data/wallet_portfolios/`` and
        ``copy_selection_run/wallet_portfolios/``.

    Returns
    -------
    dict with keys:
        wallet, real_max_dd_usd, real_max_dd_pct, peak_account_value,
        dd_source, fetch_error
    """
    if cache_dirs is None:
        cache_dirs = _DEFAULT_CACHE_DIRS

    wallet_lower = wallet.lower()

    for d in cache_dirs:
        path = d / f"{wallet_lower}.json"
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            return {
                "wallet": wallet,
                "real_max_dd_usd": None,
                "real_max_dd_pct": None,
                "peak_account_value": None,
                "dd_source": _DD_SOURCE_BLOCKED,
                "fetch_error": f"json_read_error: {exc}",
            }
        return _extract_dd_from_json(data, wallet)

    # Not found in any cache dir — report blocked (caller decides whether to fetch)
    return {
        "wallet": wallet,
        "real_max_dd_usd": None,
        "real_max_dd_pct": None,
        "peak_account_value": None,
        "dd_source": _DD_SOURCE_BLOCKED,
        "fetch_error": "portfolio_json_not_found",
    }


# ---------------------------------------------------------------------------
# 4. wallet_passes_real_dd
# ---------------------------------------------------------------------------

def wallet_passes_real_dd(
    wallet: str,
    max_real_dd_pct: Optional[float],
    cache_dirs: list[Path] | None = None,
) -> dict:
    """Load real DD for a wallet and evaluate the gate.

    Parameters
    ----------
    wallet : str
        Wallet address.
    max_real_dd_pct : float or None
        Threshold.  ``None`` disables the gate.
    cache_dirs : list of Path, optional
        Override cache directories.

    Returns
    -------
    dict with ALL output columns:
        wallet, real_max_dd_usd, real_max_dd_pct,
        current_dd_usd, current_dd_pct,
        dd_source, dd_gate_pass, dd_gate_reason, fetch_error

    ``current_dd_usd`` and ``current_dd_pct`` are always None — they require
    live position data not available from portfolio JSONs alone.
    """
    loaded = load_real_dd_for_wallet(wallet, cache_dirs)

    # Build a stats-shaped dict for real_dd_gate
    fake_stats: dict = {
        "allTime_max_drawdown_mtm": (
            -loaded["real_max_dd_usd"]
            if loaded["real_max_dd_usd"] is not None
            else None
        ),
        "allTime_acctV_peak": loaded.get("peak_account_value"),
        "mtm_source": (
            "cache_stale"
            if loaded["dd_source"] == _DD_SOURCE_LOCAL
            else "hl_portfolio_api"
            if loaded["dd_source"] == _DD_SOURCE_FETCHED
            else "unavailable"
        ),
    }

    gate = real_dd_gate(fake_stats, max_real_dd_pct)

    # If fetch failed and gate says blocked, propagate the fetch_error
    if loaded.get("fetch_error") and not gate["dd_gate_pass"]:
        gate["dd_gate_reason"] = f"DATA_FETCH_BLOCKED:{loaded['fetch_error']}"

    return {
        "wallet": wallet,
        "real_max_dd_usd": gate.get("real_max_dd_usd"),
        "real_max_dd_pct": gate.get("real_max_dd_pct"),
        "current_dd_usd": None,
        "current_dd_pct": None,
        "dd_source": gate["dd_source"],
        "dd_gate_pass": gate["dd_gate_pass"],
        "dd_gate_reason": gate["dd_gate_reason"],
        "fetch_error": loaded.get("fetch_error"),
    }
