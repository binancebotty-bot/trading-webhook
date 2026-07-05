"""
3hl_stage2_useful_wallet_scanner.py â€” WALLET FINDER EDITION

Stage 2+3 deep wallet scanner with MTM truth integration.
All data paths point to the local data/ directory.

Loads wallets from the Stage 1.5 MTM Calmar pre-filter output
(hl_stage1_5_mtm_pass.csv), fetches full trade history via userFillsByTime,
computes 30+ KPIs (PnL, drawdown, win rate, martingale flags, fee-net estimates,
MTM truth), and outputs summary.csv.

RUN ORDER: Stage 1 â†’ Stage 1.5 (MTM pre-filter) â†’ Stage 2+3 (this script)
Stage 1.5 eliminates 80%+ of expensive fill fetches by only advancing
Calmar-ranked survivors to full analysis.
"""
import asyncio
import aiohttp
import csv
import os
import socket
import json
from datetime import datetime, timezone

# WALLET FINDER edition â€” local imports and data paths
from hl_mtm_lookup import get_mtm_stats_async, MTM_OUTPUT_COLUMNS

API_URL = "https://api.hyperliquid.xyz/info"

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
INPUT_FILE = os.path.join(DATA_DIR, "hl_stage1_5_mtm_pass.csv")
# If the Stage 1.5 output doesn't exist yet, fall back to Stage 1 output.
STAGE1_FALLBACK = os.path.join(DATA_DIR, "simple_filter_pass.csv")
SUMMARY_FILE = os.path.join(DATA_DIR, "summary.csv")
ALL_TRADES_FILE = os.path.join(DATA_DIR, "all_trades.csv")
CURVES_DIR = os.path.join(DATA_DIR, "equity_curves")
os.makedirs(CURVES_DIR, exist_ok=True)

MAX_CONCURRENCY = 50
FILLS_LIMIT = 200_000  # safety cap â€” some wallets have 77K+ fills
FETCH_LIMIT = 2000     # Hyperliquid API max per response

import statistics
import time as _time_module

# Tracking
WALLET_PROGRESS = os.path.join(DATA_DIR, "stage2_progress.txt")
EXTENDED_FAILURE_RETRIES = 3
EXTENDED_FAILURE_COOLDOWN_S = 30

def load_progress():
    if os.path.exists(WALLET_PROGRESS):
        with open(WALLET_PROGRESS) as f:
            return set(line.strip().lower() for line in f if line.strip())
    return set()

def save_progress_wallet(wallet):
    with open(WALLET_PROGRESS, "a", encoding="utf-8") as f:
        f.write(wallet.lower() + "\n")

def clear_wallet_progress(wallet):
    pass

def log(wallet, msg):
    tag = wallet[:10] if wallet else "?"
    print(f"[{tag}] {msg}", flush=True)


# ---------- HTTP ----------
async def post(session, payload, wallet, retries=4):
    backoff = 1.0
    for attempt in range(retries):
        try:
            async with session.post(
                API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status == 200:
                    return await r.json()
                if r.status == 429:
                    sleep_s = backoff * (2 ** attempt) + 0.5
                    log(wallet, f"[429] sleep {sleep_s:.1f}s")
                    await asyncio.sleep(sleep_s)
                    continue
        except Exception as e:
            sleep_s = backoff * (2 ** attempt)
            log(wallet, f"[ERR] {str(e)[:60]} sleep {sleep_s:.1f}s")
            await asyncio.sleep(sleep_s)
    return None


async def post_with_extended_recovery(session, payload, wallet):
    """Return API data, [] for a true empty page, or None for API failure."""
    data = await post(session, payload, wallet)
    if data is not None:
        return data

    for attempt in range(EXTENDED_FAILURE_RETRIES):
        sleep_s = EXTENDED_FAILURE_COOLDOWN_S * (attempt + 1)
        log(wallet, f"api failure - retrying same page after {sleep_s:.0f}s")
        await asyncio.sleep(sleep_s)
        data = await post(session, payload, wallet)
        if data is not None:
            return data
    return None


# ---------- FETCH ALL FILLS ----------
async def fetch_all_fills(session, wallet):
    lo = 0
    all_fills = []
    fetch_cycles = 0
    termination_reason = "ok"
    suspected_truncated = 0

    while True:
        payload = {
            "type": "userFillsByTime",
            "user": wallet,
            "startTime": lo,
        }
        batch = await post_with_extended_recovery(session, payload, wallet)
        if batch is None:
            termination_reason = "api_failure"
            suspected_truncated = 1
            break
        if not isinstance(batch, list):
            log(wallet, f"unexpected response type: {type(batch).__name__}")
            termination_reason = "api_failure"
            suspected_truncated = 1
            break
        if len(batch) == 0:
            break
        fetch_cycles += 1
        all_fills.extend(batch)
        if len(batch) < FETCH_LIMIT:
            break
        new_lo = max(int(f.get("time", 0)) for f in batch) + 1
        if new_lo <= lo:
            break
        lo = new_lo
        if len(all_fills) >= FILLS_LIMIT:
            termination_reason = "fills_limit"
            suspected_truncated = 1
            break
        await asyncio.sleep(0.15)

    return {
        "fills": all_fills[:FILLS_LIMIT],
        "fetch_cycles": fetch_cycles,
        "termination_reason": termination_reason,
        "suspected_truncated": suspected_truncated,
    }


# ---------- MAGIC DETECTION ----------
MAGIC_7D = 7 * 24 * 3600 * 1000

def ms_to_iso(ms):
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    except:
        return ""


def compute_kpis(fills):
    if not fills:
        return None

    fills_sorted = sorted(fills, key=lambda f: int(f.get("time", 0)))
    pnl = 0.0
    peak = 0.0
    max_dd = 0.0
    max_dd_start = 0
    max_dd_end = 0
    dd_start = 0
    pnl_list = []
    pnl_curve = []
    wins = []
    losses = []

    HL_TAKER_FEE_PER_SIDE = 0.00035
    total_notional = 0.0

    trade_times = []
    symbols = set()

    wins_streak = 0
    losses_streak = 0
    max_consec_win = 0
    max_consec_loss = 0

    now_ms = int(_time_module.time() * 1000)
    cutoff_7d = now_ms - MAGIC_7D
    trades_7d = 0

    for f in fills_sorted:
        p = float(f.get("closedPnl", 0) or 0)
        pnl += p
        pnl_curve.append(pnl)
        pnl_list.append(p)

        try:
            _px = float(f.get("px", 0) or 0); _sz = float(f.get("sz", 0) or 0)
            total_notional += abs(_px * _sz)
        except Exception:
            pass

        t = int(f["time"])
        trade_times.append(t)
        coin = (f.get("coin") or "").strip()
        if coin:
            symbols.add(coin)

        if p > 0:
            wins.append(p)
            wins_streak += 1
            losses_streak = 0
            if wins_streak > max_consec_win:
                max_consec_win = wins_streak
        elif p < 0:
            losses.append(abs(p))
            losses_streak += 1
            wins_streak = 0
            if losses_streak > max_consec_loss:
                max_consec_loss = losses_streak

        if pnl > peak:
            peak = pnl
        dd = pnl - peak
        if dd < max_dd:
            max_dd = dd
            if max_dd_start == 0:
                max_dd_start = t

        if t >= cutoff_7d:
            trades_7d += 1

    trades = len(pnl_list)
    win_rate = len(wins) / trades if trades else 0
    avg_pnl = sum(pnl_list) / trades if trades else 0
    pnl_std = statistics.stdev(pnl_list) if len(pnl_list) > 1 else 0

    max_dd_duration = 0
    recovery_time = max_dd_duration

    _ts_span_h = (max(trade_times) - min(trade_times)) / 3_600_000 if len(trade_times) > 1 else 0
    slope = (pnl_curve[-1] - pnl_curve[0]) / _ts_span_h if _ts_span_h > 0 else 0
    pnl_stability = (avg_pnl / pnl_std) if pnl_std > 0 else 0

    chunk = max(20, trades // 5) if trades else 20
    chunks = [pnl_list[i:i + chunk] for i in range(0, trades, chunk)]
    chunk_wr = [sum(1 for x in c if x > 0) / len(c) for c in chunks if c]
    consistency_score = max(0.0, min(1.0, 1 - statistics.stdev(chunk_wr))) if len(chunk_wr) > 1 else 0

    gaps = [(trade_times[i] - trade_times[i - 1]) for i in range(1, len(trade_times))]
    trade_freq_std = statistics.stdev(gaps) if len(gaps) > 1 else 0
    inactivity_score = (max(gaps) / 3_600_000) if gaps else 0

    skew = (max(wins) / sum(wins)) if wins else 0

    size_seq = [abs(float(f.get("sz", 0) or 0)) for f in fills_sorted]

    def _is_martingale(seq):
        streak = 0
        for i in range(1, len(seq)):
            if seq[i - 1] > 0 and seq[i] > seq[i - 1] * 1.5:
                streak += 1
                if streak >= 2:
                    return True
            else:
                streak = 0
        return False

    martingale_flag = int(_is_martingale(size_seq))

    one_big_trade_flag = int(max(wins) > pnl * 0.5) if wins and pnl > 0 else 0
    equity_collapse_flag = int(pnl_curve[-1] < max(pnl_curve) * 0.5) if pnl_curve else 0

    first_ts = min(trade_times)
    last_ts = max(trade_times)
    timespan_hours = (last_ts - first_ts) / (1000 * 60 * 60) if len(trade_times) > 1 else 0

    est_fee_drag = round(total_notional * HL_TAKER_FEE_PER_SIDE * 2.0, 4)
    total_pnl_net_fees_est = round(pnl - est_fee_drag, 4)

    edge_score_raw = avg_pnl / (pnl_std + 0.0001) if pnl_std > 0 else 0
    skew_ratio = (skew / (1 - skew + 0.01)) if 0 < skew < 1 else 0

    return (
        trades, round(pnl, 2), round(win_rate, 3),
        round(avg_pnl, 4), round(pnl_std, 4),
        round(max_dd, 2),
        round(max(wins) if wins else 0, 2),
        round(max(losses) if losses else 0, 2),
        round((sum(wins) / sum(losses)) if losses else 999.0, 2),
        round(max(wins) / (max(wins) + max(losses)) if wins and losses else (1.0 if wins else 0.0), 3),
        max_consec_win, max_consec_loss, trades_7d,
        round(slope, 6), round(pnl_stability, 4), round(consistency_score, 4),
        max_dd_duration, recovery_time, martingale_flag,
        round(trade_freq_std, 2), round(inactivity_score, 2),
        round(edge_score_raw, 6), round(skew_ratio, 6),
        one_big_trade_flag, equity_collapse_flag,
        ms_to_iso(first_ts), ms_to_iso(last_ts),
        round(timespan_hours, 2),
        len(symbols),
        round(total_notional, 2), est_fee_drag, total_pnl_net_fees_est,
    )


# ---------- SUMMARY HELPERS ----------
CSV_FIELDS = ["wallet", "time", "coin", "side", "px", "sz", "closedPnl"]


def _trade_key(row):
    return (
        str(row.get("wallet", "")).strip().lower(),
        str(row.get("time", "")).strip(),
        str(row.get("coin", "")).strip().upper(),
        str(row.get("side", "")).strip().lower(),
        str(row.get("px", "")).strip(),
        str(row.get("sz", "")).strip(),
        str(row.get("closedPnl", 0)).strip(),
    )


def remove_all_trades_wallet(wallet):
    wallet = wallet.strip().lower()
    if not os.path.exists(ALL_TRADES_FILE) or os.path.getsize(ALL_TRADES_FILE) == 0:
        return
    with open(ALL_TRADES_FILE, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if str(r.get("wallet", "")).strip().lower() != wallet]
    tmp = ALL_TRADES_FILE + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, ALL_TRADES_FILE)


def append_all_trades(wallet, fills):
    file_exists = os.path.exists(ALL_TRADES_FILE) and os.path.getsize(ALL_TRADES_FILE) > 0
    rows = []
    for f in sorted(fills, key=lambda x: int(x.get("time", 0) or 0)):
        row = {
            "wallet": wallet,
            "time": f.get("time"),
            "coin": f.get("coin", ""),
            "side": f.get("side", ""),
            "px": f.get("px"),
            "sz": f.get("sz"),
            "closedPnl": f.get("closedPnl", 0),
        }
        rows.append(row)
    with open(ALL_TRADES_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def write_equity_curve(wallet, fills):
    equity = 0.0
    curve = []
    for f in sorted(fills, key=lambda x: int(x.get("time", 0) or 0)):
        try:
            equity += float(f.get("closedPnl", 0) or 0)
            ts = int(f.get("time", 0) or 0)
        except (TypeError, ValueError):
            continue
        if ts > 0:
            curve.append((ts, round(equity, 6)))
    path = os.path.join(CURVES_DIR, f"{wallet}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "equity"])
        writer.writerows(curve)


def remove_summary_wallet(wallet):
    if not os.path.exists(SUMMARY_FILE):
        return

    with open(SUMMARY_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        existing_keys = set(rows[0].keys()) if rows else set()
        canonical = [
            "wallet", "trades", "total_pnl", "win_rate", "avg_pnl", "pnl_std",
            "max_drawdown", "max_win", "max_loss", "profit_factor",
            "largest_win_ratio", "max_consec_wins", "max_consec_losses", "trades_7d",
            "equity_slope", "pnl_stability_score", "consistency_score",
            "drawdown_duration", "recovery_time", "martingale_flag",
            "trade_freq_std", "inactivity_score",
            "edge_score_raw", "skew_ratio",
            "one_big_trade_flag", "equity_collapse_flag",
            "first_timestamp", "last_timestamp",
            "timespan_hours", "symbol_count",
            "total_notional", "est_fee_drag", "total_pnl_net_fees_est",
            "fetch_cycles", "termination_reason",
            "suspected_truncated", "short_span_flag", "negative_total_flag",
            *MTM_OUTPUT_COLUMNS,
        ]
        for k in (existing_keys - set(canonical)):
            canonical.append(k)
        fieldnames = canonical

    filtered = [row for row in rows if row.get("wallet") != wallet]

    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered)


def append_summary(row):
    file_exists = os.path.exists(SUMMARY_FILE) and os.path.getsize(SUMMARY_FILE) > 0

    with open(SUMMARY_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "wallet", "trades", "total_pnl", "win_rate", "avg_pnl", "pnl_std",
                "max_drawdown", "max_win", "max_loss", "profit_factor",
                "largest_win_ratio", "max_consec_wins", "max_consec_losses", "trades_7d",
                "equity_slope", "pnl_stability_score", "consistency_score",
                "drawdown_duration", "recovery_time", "martingale_flag",
                "trade_freq_std", "inactivity_score",
                "edge_score_raw", "skew_ratio",
                "one_big_trade_flag", "equity_collapse_flag",
                "first_timestamp", "last_timestamp",
                "timespan_hours", "symbol_count",
                "total_notional", "est_fee_drag", "total_pnl_net_fees_est",
                "fetch_cycles", "termination_reason",
                "suspected_truncated", "short_span_flag", "negative_total_flag",
                *MTM_OUTPUT_COLUMNS,
            ])

        writer.writerow(row)


# ---------- MAIN ----------
async def main():
    print("=== STAGE 2+3 DEEP SCANNER (WALLET FINDER EDITION) ===\n", flush=True)

    # Prefer Stage 1.5 output (MTM-filtered) but fall back to raw Stage 1
    source_file = INPUT_FILE if os.path.exists(INPUT_FILE) else STAGE1_FALLBACK
    using_mtm_filter = source_file == INPUT_FILE
    if not os.path.exists(source_file):
        print(f"[!] Missing input: {INPUT_FILE} (and fallback {STAGE1_FALLBACK} not found)")
        print(f"[!] Run Stage 1 filter first: python 2hl_Stage1_Filter.py")
        print(f"[!] Then run Stage 1.5 MTM pre-filter: python hl_stage1_5_mtm_filter.py")
        return

    with open(source_file, newline="", encoding="utf-8") as f:
        wallets = [row["wallet"].strip().lower() for row in csv.DictReader(f) if row.get("wallet")]

    source_label = "Stage 1.5 (MTM-filtered)" if using_mtm_filter else "Stage 1 (unfiltered â€” Stage 1.5 not yet run)"
    print(f"Loaded {len(wallets)} wallets from {source_label}\n", flush=True)
    if not using_mtm_filter:
        print(f"[!] TIP: Run Stage 1.5 first to reduce API calls by ~80%: python hl_stage1_5_mtm_filter.py\n", flush=True)

    completed = load_progress()
    remaining = [w for w in wallets if w not in completed]
    print(f"Completed: {len(completed)} | Remaining: {len(remaining)}\n", flush=True)

    resolver = aiohttp.resolver.ThreadedResolver()
    connector = aiohttp.TCPConnector(resolver=resolver)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:
        summary_lock = asyncio.Lock()
        ledger_lock = asyncio.Lock()
        progress_lock = asyncio.Lock()

        async def process(wallet):
            async with sem:
                log(wallet, "fetching fills")
                fetch_result = await fetch_all_fills(session, wallet)
                fills = fetch_result["fills"]
                fetch_truncated = int(fetch_result["suspected_truncated"])
                termination_reason = fetch_result["termination_reason"]
                if not fills:
                    if fetch_truncated:
                        log(wallet, f"api failed before first page - leaving wallet pending ({termination_reason})")
                    else:
                        log(wallet, "no fills - skipping")
                        save_progress_wallet(wallet)
                    return

                kpis = compute_kpis(fills)
                if kpis is None:
                    log(wallet, "kpi failed")
                    save_progress_wallet(wallet)
                    return

                total_pnl = kpis[1]
                trades = kpis[0]
                span = kpis[27]

                cycles = fetch_result["fetch_cycles"]
                reason = termination_reason
                suspected_truncated = int(fetch_truncated or len(fills) >= FILLS_LIMIT)
                short_span_flag = int(span < 24)
                negative_total_flag = int(total_pnl < 0)

                # MTM lookup: if Stage 1.5 ran first, this is a cache hit (instant).
                # If using Stage 1 fallback, this may be a real API call.
                try:
                    mtm = await get_mtm_stats_async(session, wallet)
                except Exception as _mtm_err:
                    log(wallet, f"mtm lookup failed: {_mtm_err}")
                    mtm = {k: None for k in MTM_OUTPUT_COLUMNS}
                    mtm["mtm_source"] = "error"
                mtm_source = mtm.get("mtm_source", "unknown")
                if using_mtm_filter and mtm_source == "unavailable":
                    # Stage 1.5 already verified this wallet; if MTM is unavailable now
                    # it's a transient issue â€” proceed with available data
                    log(wallet, f"mtm unavailable post-s1.5 (was verified before)")
                mtm_values = tuple(mtm.get(k) for k in MTM_OUTPUT_COLUMNS)

                async with ledger_lock:
                    appended_fills = append_all_trades(wallet, fills)
                write_equity_curve(wallet, fills)
                async with summary_lock:
                    append_summary((
                        wallet, *kpis,
                        cycles, reason,
                        suspected_truncated, short_span_flag, negative_total_flag,
                        *mtm_values,
                    ))

                async with progress_lock:
                    save_progress_wallet(wallet)
                log(wallet, f"done | trades={trades} | ledger={appended_fills} | pnl={total_pnl}")

        for i in range(0, len(remaining), 10):
            batch = remaining[i:i+10]
            await asyncio.gather(*[process(w) for w in batch])
            print(f"[PROGRESS] {i+len(batch)}/{len(remaining)}", flush=True)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
