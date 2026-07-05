"""
build_hl_asset_universe_cache.py — WALLET FINDER EDITION
Read-only asset universe cache builder. Output goes to data/.
"""
from __future__ import annotations
import argparse, csv, json, math, os, re, tempfile, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_FILE = DATA_DIR / "asset_universe_snapshot.json"
HL_INFO_URL = "https://api.hyperliquid.xyz/info"
DEFAULT_MIN_ORDER_VALUE_USD = 10.0

REQUIRED_SYMBOLS = {"@150", "XYZ:BRENTOIL", "XYZ:SILVER", "XYZ:GOLD", "XYZ:SNDK", "BTC", "ETH", "SOL", "HYPE", "JUP", "PENGU", "WIF", "KBONK"}

def utc_now_iso() -> str: return datetime.now(timezone.utc).isoformat()
def fnum(value: Any, default: float = 0.0) -> float:
    try: out = float(value); return out if math.isfinite(out) else default
    except Exception: return default

def post_info(payload: Dict[str, Any], timeout: float = 10.0) -> Any:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(HL_INFO_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp: return json.loads(resp.read().decode("utf-8"))

def price_decimals(sz_decimals: int) -> int: return max(0, 6 - int(sz_decimals))

def symbol_row(original_symbol, canonical_symbol, sdk_coin, source, perp_dex, perp_dexs, sz_decimals, fetched_at, verified_from, tradable=True, reason="", asset_id="", confidence="HIGH", min_order_value_usd=DEFAULT_MIN_ORDER_VALUE_USD, min_size=""):
    max_dec = price_decimals(sz_decimals)
    return {"original_symbol": original_symbol.upper(), "canonical_symbol": canonical_symbol, "sdk_coin": sdk_coin, "asset_id": asset_id, "index": asset_id, "source": source, "perp_dex": perp_dex, "perp_dexs": perp_dexs, "sz_decimals": sz_decimals, "price_decimals": max_dec, "price_max_decimals": max_dec, "max_decimals": max_dec, "tick_size": "", "min_order_value_usd": min_order_value_usd, "min_size": min_size, "tradable_by_sender": bool(tradable), "sdk_order_compatible": bool(tradable and sdk_coin), "reason": "" if tradable else reason, "fetched_at": fetched_at, "verified_from": verified_from, "confidence": confidence}

def discover_perp_dexs(seen_symbols, endpoints_used, errors):
    dexs = {s.split(":",1)[0].lower(): s.split(":",1)[0].upper() for s in seen_symbols if ":" in s}
    try:
        raw = post_info({"type": "perpDexs"})
        endpoints_used.append("info:perpDexs")
        items = raw if isinstance(raw, list) else raw.get("perpDexs",[]) if isinstance(raw, dict) else []
        for item in items:
            name = str(item.get("name") or item.get("dex") or "").strip() if isinstance(item, dict) else str(item or "").strip()
            if name: dexs[name.lower()] = name.upper()
    except Exception as exc: errors.append(f"perpDexs failed: {repr(exc)[:180]}")
    return dexs

def build_snapshot(include_seen=True):
    fetched_at = utc_now_iso()
    seen_symbols = set(REQUIRED_SYMBOLS)
    symbols: Dict[str, Dict[str, Any]] = {}
    index_to_symbol: Dict[str, str] = {}
    symbol_to_index: Dict[str, int] = {}
    endpoints_used: List[str] = []
    errors: List[str] = []
    try:
        meta = post_info({"type": "meta"})
        endpoints_used.append("info:meta")
        for idx, asset in enumerate(meta.get("universe",[]) if isinstance(meta, dict) else []):
            if not isinstance(asset, dict): continue
            name = str(asset.get("name") or "").strip()
            if not name: continue
            key = name.upper()
            sz = int(fnum(asset.get("szDecimals"),4))
            row = symbol_row(key, name, name, "core_meta", "", None, sz, fetched_at, "info:meta", asset_id=idx)
            symbols[key] = row
            index_to_symbol[str(idx)] = name
            symbol_to_index[key] = idx
            alias = f"@{idx}"
            if alias in seen_symbols or alias in REQUIRED_SYMBOLS:
                symbols[alias] = symbol_row(alias, name, name, "index", "", None, sz, fetched_at, f"info:meta.universe[{idx}]", asset_id=idx)
    except Exception as exc: errors.append(f"meta failed: {repr(exc)[:180]}")
    try:
        spot_meta = post_info({"type": "spotMeta"})
        endpoints_used.append("info:spotMeta")
        if isinstance(spot_meta, dict):
            for idx, asset in enumerate(spot_meta.get("universe",[]) if isinstance(spot_meta.get("universe",[]), list) else []):
                name = str(asset.get("name") or "").strip().upper() if isinstance(asset, dict) else ""
                if not name: continue
                alias = f"@{idx}"
                if alias in seen_symbols and alias not in symbols:
                    symbols[alias] = symbol_row(alias, name, "", "spot_meta", "", None, 0, fetched_at, f"info:spotMeta.universe[{idx}]", tradable=False, reason="SPOT_MARKET_NOT_PERP_COPYABLE", asset_id=idx, confidence="MEDIUM")
    except Exception as exc: errors.append(f"spotMeta failed: {repr(exc)[:180]}")
    dexs = discover_perp_dexs(seen_symbols, endpoints_used, errors)
    for dex, dex_display in sorted(dexs.items()):
        try:
            builder = post_info({"type": "meta", "dex": dex})
            endpoints_used.append(f"info:meta:dex={dex}")
            for idx, asset in enumerate(builder.get("universe",[]) if isinstance(builder, dict) else []):
                if not isinstance(asset, dict): continue
                name = str(asset.get("name") or "").strip()
                if not name: continue
                asset_suffix = name.split(":",1)[1] if ":" in name else name
                key = f"{dex_display}:{asset_suffix.upper()}"
                sdk_coin = f"{dex}:{asset_suffix}"
                sz = int(fnum(asset.get("szDecimals"),0))
                symbols[key] = symbol_row(key, key, sdk_coin, "builder_meta", dex, ["",dex], sz, fetched_at, f"info:meta dex={dex}", asset_id=idx)
        except Exception as exc: errors.append(f"builder meta {dex} failed: {repr(exc)[:180]}")
    for symbol in sorted(seen_symbols | REQUIRED_SYMBOLS):
        key = symbol.upper()
        if key in symbols: continue
        symbols[key] = symbol_row(key, key, "", "failed", "", None, 0, fetched_at, "seen_symbols+probe", tradable=False, reason="NOT_FOUND_IN_META", confidence="LOW")
    snapshot = {"fetched_at": fetched_at, "fetched_at_ms": int(time.time()*1000), "source": "build_hl_asset_universe_cache.py", "read_only_endpoints_used": endpoints_used, "counts": {"seen_symbols": len(seen_symbols), "symbols": len(symbols), "tradable": sum(1 for r in symbols.values() if r.get("tradable_by_sender")), "failed": sum(1 for r in symbols.values() if not r.get("tradable_by_sender")), "index_entries": len(index_to_symbol)}, "symbols": dict(sorted(symbols.items())), "asset_index_to_symbol": index_to_symbol, "symbol_to_asset_index": symbol_to_index, "seen_symbols": sorted(seen_symbols), "learned_min_order_values": {}, "errors": errors}
    proof = {"seen_symbol_count": len(seen_symbols), "total_symbols_mapped": len(symbols), "endpoints_used": endpoints_used, "errors": errors}
    return snapshot, proof

def atomic_write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as fh:
        json.dump(payload, fh, indent=2, sort_keys=True); fh.write("\n"); tmp = Path(fh.name)
    os.replace(tmp, path)

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true"); parser.add_argument("--write-cache", action="store_true")
    args = parser.parse_args()
    if not args.dry_run and not args.write_cache: args.dry_run = True
    snapshot, proof = build_snapshot()
    if args.write_cache: atomic_write_json(CACHE_FILE, snapshot)
    focus = {s: snapshot["symbols"].get(s) for s in sorted(REQUIRED_SYMBOLS)}
    print("RESULT::HL_ASSET_UNIVERSE_CACHE_BUILD_BEGIN")
    print(f"MODE::{'WRITE_CACHE' if args.write_cache else 'DRY_RUN'}")
    print(f"CACHE_FILE::{CACHE_FILE}")
    print(f"READ_ONLY_ENDPOINTS_USED::{','.join(proof['endpoints_used'])}")
    print(f"TOTAL_SYMBOLS_MAPPED::{proof['total_symbols_mapped']}")
    print("FOCUS_SYMBOLS::" + json.dumps(focus, sort_keys=True))
    print("ERRORS::" + json.dumps(proof["errors"], sort_keys=True))
    print("RESULT::HL_ASSET_UNIVERSE_CACHE_BUILD_END")
    return 0

if __name__ == "__main__": raise SystemExit(main())
