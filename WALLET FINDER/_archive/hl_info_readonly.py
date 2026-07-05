#!/usr/bin/env python3
"""
hl_info_readonly.py — WALLET FINDER EDITION
READ-ONLY Hyperliquid info client. NO TRADING CAPABILITY.
"""
from __future__ import annotations

import json
import os
import time
import urllib.request
from typing import Any, Dict

HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
_HTTP_TIMEOUT = float(os.getenv("HL_SHADOW_INFO_TIMEOUT_SEC", "8"))
_RATE_GAP_SEC = float(os.getenv("HL_SHADOW_INFO_RATE_GAP_SEC", "0.15"))
_CACHE_TTL_SEC = float(os.getenv("HL_SHADOW_INFO_CACHE_TTL_SEC", "120"))

_last_call = [0.0]
_cache: Dict[str, Any] = {}

assert HL_INFO_URL.rstrip("/").endswith("/info"), "read-only client must target the /info endpoint only"


def _rate_limit() -> None:
    dt = time.time() - _last_call[0]
    if dt < _RATE_GAP_SEC:
        time.sleep(_RATE_GAP_SEC - dt)
    _last_call[0] = time.time()


def fetch_clearinghouse_state(address: str, timeout: float = _HTTP_TIMEOUT, dex: str = "") -> Dict[str, Any]:
    payload: Dict[str, Any] = {"type": "clearinghouseState", "user": str(address)}
    if dex:
        payload["dex"] = dex
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        HL_INFO_URL, data=body,
        headers={"Content-Type": "application/json", "User-Agent": "shadow-readonly/1.0"},
        method="POST",
    )
    _rate_limit()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _positions_and_marks(st: Dict[str, Any]):
    pos: Dict[str, float] = {}
    marks: Dict[str, float] = {}
    for ap in (st.get("assetPositions") or []):
        p = ap.get("position", {}) if isinstance(ap, dict) else {}
        coin = str(p.get("coin", "")).upper()
        if not coin:
            continue
        try:
            szi = float(p.get("szi") or 0.0)
        except Exception:
            szi = 0.0
        pos[coin] = szi
        try:
            pv = abs(float(p.get("positionValue") or 0.0))
            if abs(szi) > 1e-12 and pv > 0:
                marks[coin] = pv / abs(szi)
        except Exception:
            pass
    return pos, marks


def current_leader_positions(address: str, *, use_cache: bool = True, dexes=("", "xyz")) -> Dict[str, Any]:
    addr = str(address)
    now = time.time()
    if use_cache and addr in _cache and (now - _cache[addr][0]) <= _CACHE_TTL_SEC:
        ts, payload = _cache[addr]
        return {**payload, "age_sec": round(now - ts, 1)}
    pos: Dict[str, float] = {}
    marks: Dict[str, float] = {}
    main_ok = xyz_ok = False
    err = ""
    for dex in dexes:
        try:
            p, m = _positions_and_marks(fetch_clearinghouse_state(addr, dex=dex))
            pos.update(p)
            marks.update(m)
            if dex == "xyz":
                xyz_ok = True
            else:
                main_ok = True
        except Exception as exc:
            err = (err + "; " if err else "") + f"dex={dex or 'main'}:{repr(exc)[:120]}"
    payload = {"ok": main_ok, "xyz_ok": xyz_ok, "positions": pos, "marks": marks, "age_sec": 0.0, "error": err}
    if main_ok or xyz_ok:
        _cache[addr] = (now, payload)
    return payload
