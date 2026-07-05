"""
fetch_hl_portfolios.py — WALLET FINDER EDITION

Pulls Hyperliquid's authoritative MTM data via the portfolio API.
All output goes to data/.
"""
from __future__ import annotations
import argparse
import csv
import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
DEFAULT_RATE_LIMIT_S = 0.4

HERE = Path(__file__).resolve().parent
CACHE_DIR = HERE / "data" / "wallet_portfolios"
SUMMARY_PATH = HERE / "data" / "summary_mtm.csv"


def fetch_one(wallet: str, timeout: float = 15.0) -> dict | None:
    req = urllib.request.Request(
        HL_INFO_URL,
        data=json.dumps({"type": "portfolio", "user": wallet}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "hl-scanner/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            time.sleep(5)
            return fetch_one(wallet, timeout)
        return {"_error": f"HTTP {e.code}", "_wallet": wallet}
    except Exception as e:
        return {"_error": str(e), "_wallet": wallet}


def mdd_of(series: list[list]) -> tuple[float, float, float]:
    if not series:
        return 0.0, 0.0, 0.0
    vals = [float(p[1]) for p in series]
    peak = vals[0]
    mdd = 0.0
    for v in vals:
        if v > peak:
            peak = v
        if v - peak < mdd:
            mdd = v - peak
    return vals[0], vals[-1], mdd


def summarise(wallet: str, data: list) -> dict:
    if isinstance(data, dict) and data.get("_error"):
        return {"wallet": wallet, "status": "ERR:" + data["_error"]}
    if not isinstance(data, list):
        return {"wallet": wallet, "status": "ERR:unknown_response"}
    periods = {x[0]: x[1] for x in data}
    out: dict = {"wallet": wallet, "status": "ok"}
    for period in ("day", "week", "month", "allTime"):
        blob = periods.get(period, {}) or {}
        avh = blob.get("accountValueHistory") or []
        pnh = blob.get("pnlHistory") or []
        vlm = blob.get("vlm")
        s0, s1, av_mdd = mdd_of(avh)
        _, p1, pnl_mdd = mdd_of(pnh)
        out.update({
            f"{period}_pts": len(avh),
            f"{period}_acctV_start": round(s0, 2),
            f"{period}_acctV_end": round(s1, 2),
            f"{period}_acctV_chg": round(s1 - s0, 2),
            f"{period}_acctV_mdd_mtm": round(av_mdd, 2),
            f"{period}_pnl_chg": round(p1, 2),
            f"{period}_pnl_mdd_mtm": round(pnl_mdd, 2),
            f"{period}_vlm": vlm,
        })
    chg = out.get("month_acctV_chg", 0.0) or 0.0
    mdd = abs(out.get("month_acctV_mdd_mtm", 0.0) or 0.0)
    out["month_calmar_mtm"] = round(chg / mdd, 3) if mdd > 1.0 else None
    return out


def load_wallets(input_path: Path, col: str | None) -> list[str]:
    text = input_path.read_text()
    if input_path.suffix.lower() == ".csv":
        rdr = csv.DictReader(text.splitlines())
        key = col or "wallet"
        return [row[key].strip().lower() for row in rdr if row.get(key)]
    return [ln.strip().lower() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True, help="CSV (with wallet col) or txt (one per line)")
    ap.add_argument("--col", default="wallet")
    ap.add_argument("--rate", type=float, default=DEFAULT_RATE_LIMIT_S)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--summary-only", action="store_true")
    args = ap.parse_args()

    CACHE_DIR.mkdir(exist_ok=True)
    wallets = load_wallets(args.input, args.col)
    print(f"loaded {len(wallets)} wallets")

    rows: list[dict] = []
    t0 = time.time()
    for i, w in enumerate(wallets):
        cache_path = CACHE_DIR / f"{w}.json"
        if args.summary_only or (args.resume and cache_path.exists()):
            if cache_path.exists():
                try:
                    data = json.loads(cache_path.read_text())
                except Exception:
                    data = {"_error": "cache_unreadable", "_wallet": w}
            else:
                continue
        else:
            data = fetch_one(w)
            if data is not None and not (isinstance(data, dict) and data.get("_error")):
                cache_path.write_text(json.dumps(data))
            time.sleep(args.rate)
        rows.append(summarise(w, data))
        if (i + 1) % 25 == 0:
            print(f"  {i+1}/{len(wallets)}  dt={time.time()-t0:.1f}s")

    all_keys: list[str] = ["wallet", "status"]
    for period in ("day", "week", "month", "allTime"):
        for sfx in ("pts", "acctV_start", "acctV_end", "acctV_chg", "acctV_mdd_mtm",
                    "pnl_chg", "pnl_mdd_mtm", "vlm"):
            all_keys.append(f"{period}_{sfx}")
    all_keys.append("month_calmar_mtm")

    with SUMMARY_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in all_keys})
    print(f"wrote {SUMMARY_PATH}  rows={len(rows)}  total={time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
