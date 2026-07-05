"""
build_universe_from_stage1.py — Bridge between stage1_simple.py and the 8012 dashboard.

PASSES THROUGH Stage 1 decisions: Stage 1 already filtered using MTM allTime DD
(spot+perp combined). This script does NOT re-filter by DD. It only extracts
perp PnL/volume/trades for CSV columns. app.py handles MTM overlay at runtime.

Reads:
  - data/simple_filter_pass.csv  (wallet, max_dd_pct, peak_value, end_value,
    realised_pnl, account_age_days, abs_dd, pnl_dd_ratio, lifetime_fills)
  - data/wallet_portfolios/*.json (portfolio cache: perpAllTime, etc.)
  - data/summary.csv (optional — scanner KPIs)
  - data/all_trades.csv (optional — individual fills for trade stats)

Writes:
  - data/wallet_universe.csv — format expected by app.py (8012 dashboard)
"""
import csv
import json
import time
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
PASS_FILE = DATA_DIR / "simple_filter_pass.csv"
CACHE_DIR = DATA_DIR / "wallet_portfolios"
OUTPUT_FILE = DATA_DIR / "wallet_universe.csv"
SUMMARY_FILE = DATA_DIR / "summary.csv"
ALL_TRADES_FILE = DATA_DIR / "all_trades.csv"


def _safe_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _iso_to_ms(value):
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except (TypeError, ValueError):
        return None


def build_from_stage2_summary():
    """Build wallet_universe.csv directly from the Stage 2 deep scanner output."""
    if not SUMMARY_FILE.exists() or SUMMARY_FILE.stat().st_size == 0:
        return None

    output_cols = [
        "wallet", "realised_pnl", "total_notional", "efficiency",
        "trades", "trades_7d", "last_trade_time", "score", "status",
    ]
    rows = []
    now_ms = int(time.time() * 1000)
    stale_cutoff = now_ms - 24 * 60 * 60 * 1000

    with open(SUMMARY_FILE, newline="", encoding="utf-8-sig") as f:
        for src in csv.DictReader(f):
            wallet = str(src.get("wallet", "")).strip().lower()
            if not wallet:
                continue
            pnl = _safe_float(src.get("total_pnl"), 0.0)
            notional = _safe_float(src.get("total_notional"), 0.0)
            trades = _safe_int(src.get("trades"), 0)
            trades_7d = _safe_int(src.get("trades_7d"), 0)
            last_trade_time = _iso_to_ms(src.get("last_timestamp"))
            efficiency = (pnl / notional) if notional and abs(notional) > 1e-12 else None
            score = (efficiency or 0.0) * max(trades_7d, 1)
            status = "active" if last_trade_time and last_trade_time >= stale_cutoff else "inactive"
            rows.append({
                "wallet": wallet,
                "realised_pnl": round(pnl, 4),
                "total_notional": round(notional, 4) if notional is not None else "",
                "efficiency": round(efficiency, 8) if efficiency is not None else "",
                "trades": trades,
                "trades_7d": trades_7d,
                "last_trade_time": last_trade_time or "",
                "score": round(score, 8),
                "status": status,
            })

    if not rows:
        return None
    rows.sort(key=lambda r: float(r["realised_pnl"] or 0), reverse=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_cols)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def load_pass_list():
    """Load stage1 pass list — ALL wallets here already passed MTM allTime DD filter."""
    wallets = {}
    with open(PASS_FILE, newline="") as f:
        for row in csv.DictReader(f):
            wallets[row["wallet"].lower()] = {
                "max_dd_pct": float(row["max_dd_pct"]),
                "peak_value": float(row.get("peak_value", 0)),
                "end_value": float(row.get("end_value", 0)),
                "realised_pnl": float(row.get("realised_pnl", 0)),
                "lifetime_fills": int(row.get("lifetime_fills", 0)),
            }
    return wallets


def extract_perp_stats(portfolio_data):
    """Extract PnL, volume, timestamps from PERP-ONLY blocks."""
    result = {}

    for granularity in ["perpAllTime", "perpMonth", "perpWeek"]:
        block = None
        for item in portfolio_data:
            if isinstance(item, list) and len(item) >= 2 and item[0] == granularity:
                block = item[1]
                break

        if block is None:
            continue

        # PnL from pnlHistory (last value = cumulative PnL)
        pnl_hist = block.get("pnlHistory", [])
        if pnl_hist and len(pnl_hist) > 0:
            last_pnl = float(pnl_hist[-1][1])
            if "realised_pnl" not in result:
                result["realised_pnl"] = last_pnl

        # Volume from vlm
        vlm = block.get("vlm")
        if vlm is not None:
            try:
                v = float(vlm)
                if "total_notional" not in result:
                    result["total_notional"] = v
            except (ValueError, TypeError):
                pass

        # Last trade time from accountValueHistory (most recent timestamp)
        acct_hist = block.get("accountValueHistory", [])
        if acct_hist:
            last_ts = max(int(pt[0]) for pt in acct_hist)
            if "last_trade_time" not in result or last_ts > result.get("last_trade_time", 0):
                result["last_trade_time"] = last_ts

    return result


def load_portfolio(wallet):
    """Load portfolio cache for a wallet."""
    cache_path = CACHE_DIR / f"{wallet}.json"
    if not cache_path.exists():
        return None
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_trade_stats_from_all_trades():
    """Load trade count stats from all_trades.csv if available."""
    if not ALL_TRADES_FILE.exists():
        return {}

    stats = {}
    with open(ALL_TRADES_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = row["wallet"].lower()
            if w not in stats:
                stats[w] = {"trades": 0, "first_time": float("inf"), "last_time": 0}
            stats[w]["trades"] += 1
            t = int(row["time"])
            stats[w]["first_time"] = min(stats[w]["first_time"], t)
            stats[w]["last_time"] = max(stats[w]["last_time"], t)
    return stats


def main():
    summary_rows = build_from_stage2_summary()
    if summary_rows is not None:
        print("=" * 60)
        print("BUILD UNIVERSE FROM STAGE2 SUMMARY")
        print("=" * 60)
        print(f"  Wallets written: {len(summary_rows)}")
        print(f"  Output: {OUTPUT_FILE}")
        return

    print("=" * 60)
    print("BUILD UNIVERSE FROM STAGE1 (pass-through, no re-filter)")
    print("=" * 60)

    # Step 1: Load stage1 pass list — ALL wallets already passed MTM allTime DD filter
    print("\nLoading stage1 pass list...")
    pass_list = load_pass_list()
    print(f"  {len(pass_list)} wallets in stage1 pass list (all passed MTM DD filter)")

    # Step 2: Extract perp stats from portfolio cache (no re-filtering)
    print("\nExtracting perp stats from portfolio cache...")
    enriched = []
    no_cache = 0

    for wallet, s1_data in pass_list.items():
        portfolio = load_portfolio(wallet)
        if portfolio is None:
            no_cache += 1
            # Still include the wallet — use Stage 1 data directly
            enriched.append({
                "wallet": wallet,
                "realised_pnl": s1_data["realised_pnl"],
                "total_notional": None,
                "last_trade_time": None,
                "lifetime_fills": s1_data["lifetime_fills"],
                "max_dd_pct": s1_data["max_dd_pct"],
            })
            continue

        # Extract perp PnL/volume from portfolio cache
        perp_stats = extract_perp_stats(portfolio)

        enriched.append({
            "wallet": wallet,
            "realised_pnl": perp_stats.get("realised_pnl", s1_data["realised_pnl"]),
            "total_notional": perp_stats.get("total_notional"),
            "last_trade_time": perp_stats.get("last_trade_time"),
            "lifetime_fills": s1_data["lifetime_fills"],
            "max_dd_pct": s1_data["max_dd_pct"],
        })

    print(f"  Enriched: {len(enriched)}, no cache (used Stage 1 data): {no_cache}")

    # Step 3: Build output — ALL wallets pass through (no DD re-filtering)
    now_ms = int(time.time() * 1000)
    cutoff_7d = now_ms - 7 * 24 * 60 * 60 * 1000

    output_cols = [
        "wallet", "realised_pnl", "total_notional", "efficiency",
        "trades", "trades_7d", "last_trade_time", "score", "status",
    ]

    rows = []
    for row in sorted(enriched, key=lambda r: r["wallet"]):
        out = {col: "" for col in output_cols}
        out["wallet"] = row["wallet"]

        pnl = row.get("realised_pnl")
        if pnl is not None:
            out["realised_pnl"] = round(pnl, 4)

        vlm = row.get("total_notional")
        if vlm is not None:
            out["total_notional"] = round(vlm, 4)

        # Efficiency = PnL / volume
        if pnl is not None and vlm is not None and vlm > 0:
            out["efficiency"] = round(pnl / vlm, 8)

        # Use lifetime_fills from Stage 1.5 as the trades count
        lifetime = row.get("lifetime_fills", 0)
        if lifetime:
            out["trades"] = lifetime

        # trades_7d: approximate from last_trade_time
        ltt = row.get("last_trade_time")
        if ltt is not None:
            out["last_trade_time"] = int(ltt)
            if ltt > cutoff_7d and lifetime:
                out["trades_7d"] = lifetime  # all trades are recent
            else:
                out["trades_7d"] = 0
        else:
            out["trades_7d"] = 0

        out["score"] = ""
        out["status"] = "active"

        rows.append(out)

    # Write output
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_cols)
        writer.writeheader()
        writer.writerows(rows)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"OUTPUT: {OUTPUT_FILE}")
    print(f"{'=' * 60}")
    print(f"  Wallets written: {len(rows)} (all passed Stage 1 MTM DD filter)")
    print(f"  Column coverage:")
    for col in output_cols[1:]:
        filled = sum(1 for r in rows if r[col] != "")
        pct = filled / len(rows) * 100 if rows else 0
        print(f"    {col}: {filled}/{len(rows)} ({pct:.0f}%)")

    # DD distribution (from Stage 1 MTM allTime data, not re-computed)
    if rows:
        print(f"\n  MTM allTime DD% distribution (from Stage 1):")
        brackets = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40)]
        for lo, hi in brackets:
            c = sum(1 for r in enriched if lo <= r["max_dd_pct"] < hi)
            bar = "#" * (c // 10)
            print(f"    {lo:>2}-{hi:>2}%: {c:>5} {bar}")

    # PnL distribution
    if rows:
        print(f"\n  PnL distribution:")
        pnl_brackets = [
            ("< -$10K", lambda p: p < -10000),
            ("-$10K to -$1K", lambda p: -10000 <= p < -1000),
            ("-$1K to $0", lambda p: -1000 <= p < 0),
            ("$0 to $1K", lambda p: 0 <= p < 1000),
            ("$1K to $10K", lambda p: 1000 <= p < 10000),
            ("> $10K", lambda p: p >= 10000),
        ]
        for label, fn in pnl_brackets:
            c = sum(1 for r in enriched if r.get("realised_pnl") is not None and fn(r["realised_pnl"]))
            print(f"    {label:>20}: {c:>5}")


if __name__ == "__main__":
    main()
