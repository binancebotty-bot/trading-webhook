"""hl_mtm_lookup.py — Shared HL portfolio (MTM) lookup utility.
WALLET FINDER edition — cache dir is `data/wallet_portfolios/`.

The local pipeline historically computed drawdown from cumsum(closedPnl), which
is REALISED-only and systematically understates true mark-to-market drawdown
(5-30x in spot-checks). This module provides a single canonical source of MTM
truth: Hyperliquid's `info/portfolio` API.

Usage (sync):
    from hl_mtm_lookup import get_mtm_stats
    stats = get_mtm_stats("0xabc...")
    # -> {"max_drawdown_mtm": -2573.50, "month_pnl_chg_mtm": 115770.34, ...}

Usage (async):
    from hl_mtm_lookup import get_mtm_stats_async
    stats = await get_mtm_stats_async(session, "0xabc...")

Cache:
    JSON responses are written to data/wallet_portfolios/<wallet>.json.
    A stat-time check enforces a default 24h TTL so stale prices aren't silently used.
    Pass force_refresh=True to bust.

Naming convention (NEVER use unqualified "max_drawdown" downstream):
    *_mdd_realised : derived from cumsum(closedPnl). Understates DD; legacy only.
    *_mdd_mtm      : derived from accountValueHistory. Truth.
    mtm_source     : "hl_portfolio_api", "cache_stale", or "unavailable".
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

# Cache dir relative to this file: WALLET FINDER/data/wallet_portfolios/
_HERE = Path(__file__).resolve().parent
DEFAULT_CACHE_DIR = _HERE / "data" / "wallet_portfolios"
DEFAULT_TTL_SEC = 24 * 3600  # 24h


def _empty_stats(source: str = "unavailable") -> dict:
    return {
        "max_drawdown_mtm": None,
        "month_pnl_chg_mtm": None,
        "month_acctV_end": None,
        "month_acctV_peak": None,
        "mtm_calmar": None,
        "allTime_max_drawdown_mtm": None,
        "allTime_pnl_chg_mtm": None,
        "allTime_acctV_peak": None,
        "allTime_vlm": None,
        "equity_collapse_flag_mtm": None,
        "negative_total_flag_mtm": None,
        "mtm_source": source,
        "mtm_fetched_at": None,
    }


def _summarise(data: list) -> dict:
    """Reduce raw HL `portfolio` API response to flat MTM stats."""
    if not isinstance(data, list):
        return _empty_stats("invalid_response")
    periods = {x[0]: x[1] for x in data if isinstance(x, list) and len(x) == 2}

    def _stats(series: list) -> tuple[float, float, float, float]:
        """Return (start, end, mdd_neg, peak)."""
        if not series:
            return 0.0, 0.0, 0.0, 0.0
        vals = [float(p[1]) for p in series if isinstance(p, list) and len(p) == 2]
        if not vals:
            return 0.0, 0.0, 0.0, 0.0
        peak = vals[0]; mdd = 0.0; peak_global = vals[0]
        for v in vals:
            if v > peak: peak = v
            if v > peak_global: peak_global = v
            if v - peak < mdd: mdd = v - peak
        return vals[0], vals[-1], mdd, peak_global

    month = periods.get("month", {}) or {}
    avh_m = month.get("accountValueHistory") or []
    pnh_m = month.get("pnlHistory") or []
    m_start, m_end, m_mdd, m_peak = _stats(avh_m)
    _, m_pnl_end, _, _ = _stats(pnh_m)
    month_chg = m_end - m_start

    allt = periods.get("allTime", {}) or {}
    avh_a = allt.get("accountValueHistory") or []
    pnh_a = allt.get("pnlHistory") or []
    a_start, a_end, a_mdd, a_peak = _stats(avh_a)
    _, a_pnl_end, _, _ = _stats(pnh_a)

    calmar = round(month_chg / abs(m_mdd), 3) if abs(m_mdd) > 1.0 else None

    equity_collapse_flag_mtm = int(m_peak > 0 and m_end < 0.5 * m_peak)
    negative_total_flag_mtm = int(month_chg < 0)

    return {
        "max_drawdown_mtm": round(m_mdd, 4),
        "month_pnl_chg_mtm": round(month_chg, 4),
        "month_acctV_end": round(m_end, 4),
        "month_acctV_peak": round(m_peak, 4),
        "mtm_calmar": calmar,
        "allTime_max_drawdown_mtm": round(a_mdd, 4),
        "allTime_pnl_chg_mtm": round(a_pnl_end, 4),
        "allTime_acctV_peak": round(a_peak, 4),
        "allTime_vlm": allt.get("vlm"),
        "equity_collapse_flag_mtm": equity_collapse_flag_mtm,
        "negative_total_flag_mtm": negative_total_flag_mtm,
        "mtm_source": "hl_portfolio_api",
        "mtm_fetched_at": int(time.time()),
    }


def _cache_path(wallet: str, cache_dir: Path) -> Path:
    return cache_dir / f"{wallet.lower()}.json"


def _read_cache(wallet: str, cache_dir: Path, ttl_sec: int) -> Optional[list]:
    p = _cache_path(wallet, cache_dir)
    if not p.exists():
        return None
    age = time.time() - p.stat().st_mtime
    if age > ttl_sec:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_cache_any(wallet: str, cache_dir: Path) -> Optional[list]:
    """Read cache without TTL check; used as soft fallback when network is down."""
    p = _cache_path(wallet, cache_dir)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_cache(wallet: str, data: list, cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = _cache_path(wallet, cache_dir)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    os.replace(tmp, p)


def get_mtm_stats(
    wallet: str,
    cache_dir: Optional[Path] = None,
    ttl_sec: int = DEFAULT_TTL_SEC,
    force_refresh: bool = False,
    network_timeout: float = 10.0,
    on_error: str = "soft",
) -> dict:
    """Return MTM stats for `wallet`, hitting HL API only if cache is missing/stale."""
    wallet = wallet.lower()
    cdir = cache_dir or DEFAULT_CACHE_DIR

    if not force_refresh:
        cached = _read_cache(wallet, cdir, ttl_sec)
        if cached is not None:
            return _summarise(cached)

    req = urllib.request.Request(
        HL_INFO_URL,
        data=json.dumps({"type": "portfolio", "user": wallet}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "hl-mtm-lookup/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=network_timeout) as r:
            data = json.loads(r.read())
        _write_cache(wallet, data, cdir)
        return _summarise(data)
    except Exception as e:
        if on_error == "raise":
            raise
        stale = _read_cache_any(wallet, cdir)
        if stale is not None:
            out = _summarise(stale)
            out["mtm_source"] = "cache_stale"
            return out
        return _empty_stats("unavailable")


async def get_mtm_stats_async(
    session,
    wallet: str,
    cache_dir: Optional[Path] = None,
    ttl_sec: int = DEFAULT_TTL_SEC,
    force_refresh: bool = False,
    network_timeout: float = 10.0,
    max_retries: int = 5,
) -> dict:
    """Async variant for aiohttp callers.

    Retries transient failures (429 rate-limit, 5xx, timeouts) with
    exponential backoff before giving up.  A wallet is only marked
    "unavailable" after ALL retries are exhausted.
    """
    import asyncio as _aio

    wallet = wallet.lower()
    cdir = cache_dir or DEFAULT_CACHE_DIR

    if not force_refresh:
        cached = _read_cache(wallet, cdir, ttl_sec)
        if cached is not None:
            return _summarise(cached)

    last_err = None
    for attempt in range(max_retries):
        try:
            async with session.post(
                HL_INFO_URL,
                json={"type": "portfolio", "user": wallet},
                timeout=network_timeout,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _write_cache(wallet, data, cdir)
                    return _summarise(data)

                # Transient errors: retry with backoff
                if resp.status in (429, 500, 502, 503, 504):
                    wait = min(2 ** attempt, 8)  # 1, 2, 4, 8, 8
                    await _aio.sleep(wait)
                    continue

                # Non-transient HTTP error (400, 403, etc.) — don't retry
                stale = _read_cache_any(wallet, cdir)
                if stale is not None:
                    out = _summarise(stale)
                    out["mtm_source"] = "cache_stale"
                    return out
                return _empty_stats(f"http_{resp.status}")
        except (_aio.TimeoutError, OSError, ConnectionError) as e:
            last_err = e
            wait = min(2 ** attempt, 8)
            await _aio.sleep(wait)
            continue
        except Exception as e:
            last_err = e
            # Unknown error — one retry then give up
            if attempt == 0:
                await _aio.sleep(1)
                continue
            break

    # All retries exhausted — fall back to stale cache if available
    stale = _read_cache_any(wallet, cdir)
    if stale is not None:
        out = _summarise(stale)
        out["mtm_source"] = "cache_stale"
        return out
    return _empty_stats("unavailable")


MTM_OUTPUT_COLUMNS = [
    "max_drawdown_mtm",
    "month_pnl_chg_mtm",
    "month_acctV_end",
    "month_acctV_peak",
    "mtm_calmar",
    "allTime_max_drawdown_mtm",
    "allTime_pnl_chg_mtm",
    "allTime_acctV_peak",
    "allTime_vlm",
    "equity_collapse_flag_mtm",
    "negative_total_flag_mtm",
    "mtm_source",
    "mtm_fetched_at",
]


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("usage: python hl_mtm_lookup.py <wallet> [<wallet>...]", file=sys.stderr)
        sys.exit(1)
    for w in sys.argv[1:]:
        out = get_mtm_stats(w)
        print(f"{w}: {json.dumps(out, indent=2)}")
