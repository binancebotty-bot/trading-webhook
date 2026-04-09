import asyncio
import aiohttp
import csv
import os
import time
import statistics
import random
import socket
import json
from datetime import datetime, timezone

API_URL = "https://api.hyperliquid.xyz/info"

INPUT_FILE = "hl_stage1_pass.csv"

OUTPUT_DIR = "hl_stage2"
ALL_TRADES_FILE = os.path.join(OUTPUT_DIR, "all_trades.csv")
SUMMARY_FILE = os.path.join(OUTPUT_DIR, "summary.csv")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "wallet_progress.json")

CONCURRENCY = 2
INITIAL_WINDOW_MS = 24 * 60 * 60 * 1000   # 1 day
MIN_WINDOW_MS = 60 * 60 * 1000            # 1 hour
CAP_SHRINK_THRESHOLD = 1900


# ---------- HELPERS ----------
def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def ms_to_iso(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return ""


def log(wallet, msg):
    print(f"[{wallet}] {msg}", flush=True)


def trade_key(fill):
    return (
        int(fill["time"]),
        str(fill.get("px")),
        str(fill.get("sz")),
        str(fill.get("side")),
    )


def atomic_write_json(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


# ---------- PROGRESS ----------
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {}

    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_progress(progress):
    ensure_output_dir()
    atomic_write_json(PROGRESS_FILE, progress)


def update_wallet_progress(wallet, **fields):
    progress = load_progress()
    current = progress.get(wallet, {})
    current.update(fields)
    progress[wallet] = current
    save_progress(progress)


def clear_wallet_progress(wallet):
    progress = load_progress()
    if wallet in progress:
        del progress[wallet]
        save_progress(progress)


# ---------- HTTP ----------
async def post(session, payload, wallet, retries=6):
    backoff = 1.5

    for attempt in range(retries):
        try:
            async with session.post(
                API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=20)
            ) as r:

                if r.status == 200:
                    return await r.json()

                if r.status == 429:
                    sleep_time = backoff * (2 ** attempt) + random.uniform(0, 1.0)
                    log(wallet, f"[429] backoff {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    continue

                log(wallet, f"[HTTP {r.status}]")

        except Exception as e:
            sleep_time = backoff * (2 ** attempt)
            log(wallet, f"[ERR] {str(e)[:80]} | sleep {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)

    return "ERROR"


# ---------- LOAD ----------
def load_wallets():
    wallets = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallets.append(row["wallet"])
    return wallets


def load_summary_wallets():
    if not os.path.exists(SUMMARY_FILE):
        return set()

    s = set()
    with open(SUMMARY_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = row.get("wallet")
            if wallet:
                s.add(wallet)
    return s


def load_existing_trade_keys(wallet):
    seen = set()

    if not os.path.exists(ALL_TRADES_FILE):
        return seen

    with open(ALL_TRADES_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("wallet") != wallet:
                continue

            try:
                seen.add((
                    int(row["time"]),
                    str(row.get("px")),
                    str(row.get("sz")),
                    str(row.get("side")),
                ))
            except Exception:
                continue

    return seen


def load_wallet_trades(wallet):
    fills = []

    if not os.path.exists(ALL_TRADES_FILE):
        return fills

    with open(ALL_TRADES_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("wallet") != wallet:
                continue

            try:
                fills.append({
                    "time": int(row["time"]),
                    "coin": row.get("coin"),
                    "side": row.get("side"),
                    "px": row.get("px"),
                    "sz": row.get("sz"),
                    "closedPnl": row.get("closedPnl"),
                })
            except Exception:
                continue

    return fills


# ---------- WRITE ----------
def append_all_trades(wallet, rows):
    ensure_output_dir()
    file_exists = os.path.exists(ALL_TRADES_FILE)

    to_write = rows

    if not to_write:
        return 0

    with open(ALL_TRADES_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(["wallet", "time", "coin", "side", "px", "sz", "closedPnl"])

        for r in to_write:
            writer.writerow([
                wallet,
                r["time"],
                r.get("coin"),
                r.get("side"),
                r.get("px"),
                r.get("sz"),
                r.get("closedPnl"),
            ])

    return len(to_write)


def remove_summary_wallet(wallet):
    if not os.path.exists(SUMMARY_FILE):
        return

    with open(SUMMARY_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else [
            "wallet", "trades", "total_pnl", "win_rate", "avg_pnl", "pnl_std",
            "max_drawdown", "max_win", "max_loss", "profit_factor",
            "largest_win_ratio", "max_consec_wins", "max_consec_losses", "trades_7d",
            "equity_slope", "pnl_stability_score", "consistency_score",
            "drawdown_duration", "recovery_time", "martingale_flag",
            "trade_freq_std", "inactivity_score",
            "edge_score_raw", "skew_ratio",
            "one_big_trade_flag", "equity_collapse_flag",
            "first_timestamp", "last_timestamp",
            "timespan_hours", "symbol_count", "fetch_cycles", "termination_reason",
            "suspected_truncated", "short_span_flag", "negative_total_flag"
        ]

    filtered = [row for row in rows if row.get("wallet") != wallet]

    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered)


def append_summary(row):
    ensure_output_dir()
    file_exists = os.path.exists(SUMMARY_FILE)

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
                "timespan_hours", "symbol_count", "fetch_cycles", "termination_reason",
                "suspected_truncated", "short_span_flag", "negative_total_flag"
            ])

        writer.writerow(row)


async def cheap_wallet_check(session, wallet):
    payload = {
        "type": "clearinghouseState",
        "user": wallet
    }

    data = await post(session, payload, wallet)

    if not data or data == "ERROR":
        return False

    margin = data.get("marginSummary", {})
    positions = data.get("assetPositions", [])

    account_value = float(margin.get("accountValue", 0) or 0)
    margin_used = float(margin.get("totalMarginUsed", 0) or 0)

    # --- OBVIOUS TRASH ONLY ---

    if account_value < 30:
        return False

    if not positions and margin_used < 2:
        return False

    if account_value > 0:
        if margin_used / account_value > 0.97:
            return False

    return True
  
  
async def probe_wallet(session, wallet):
    now = int(time.time() * 1000)
    window = 24 * 60 * 60 * 1000  # 24h

    payload = {
        "type": "userFillsByTime",
        "user": wallet,
        "startTime": now - window,
        "endTime": now
    }

    data = await post(session, payload, wallet)

    if not data or data == "ERROR":
        return False

    # --- ONLY OBVIOUS TRASH ---

    # no activity
    if len(data) < 3:
        return False

    pnl_list = [float(f.get("closedPnl", 0) or 0) for f in data]
    total_pnl = sum(pnl_list)

    # extreme loss only
    if total_pnl < -200:
        return False

    return True

    
# ---------- FETCH ----------
async def fetch_all_fills(session, wallet):
    all_fills = load_wallet_trades(wallet)
    seen = set()

    for f in all_fills:
        try:
            seen.add(trade_key(f))
        except Exception:
            continue

    progress = load_progress().get(wallet, {})

    now = int(time.time() * 1000)
    window_ms = int(progress.get("window_ms", INITIAL_WINDOW_MS) or INITIAL_WINDOW_MS)
    end_time = int(progress.get("end_time", now) or now)
    cycles = int(progress.get("cycles", 0) or 0)
    termination_reason = "completed"
    suspected_truncated = int(progress.get("suspected_truncated", 0) or 0)
    empty_windows = int(progress.get("empty_windows", 0) or 0)

    max_empty_windows = 30
    early_stop_empty_windows = 15

    if all_fills:
        log(wallet, f"resume existing_trades={len(all_fills)} next_end={ms_to_iso(end_time)}")
    else:
        log(wallet, "resume fresh")

    while True:
        start_time = max(0, end_time - window_ms)
        cycles += 1

        payload = {
            "type": "userFillsByTime",
            "user": wallet,
            "startTime": start_time,
            "endTime": end_time
        }

        data = await post(session, payload, wallet)

        if data == "ERROR":
            update_wallet_progress(
                wallet,
                status="error",
                end_time=end_time,
                window_ms=window_ms,
                cycles=cycles,
                empty_windows=empty_windows,
                suspected_truncated=suspected_truncated,
                trades=len(all_fills),
                updated_at=ms_to_iso(int(time.time() * 1000))
            )
            return None, cycles, "error", suspected_truncated

        if not data:
            empty_windows += 1
            end_time = start_time

            update_wallet_progress(
                wallet,
                status="running",
                end_time=end_time,
                window_ms=window_ms,
                cycles=cycles,
                empty_windows=empty_windows,
                suspected_truncated=suspected_truncated,
                trades=len(all_fills),
                updated_at=ms_to_iso(int(time.time() * 1000))
            )

            log(wallet, f"empty window | progress cycles={cycles} trades={len(all_fills)} next_end={ms_to_iso(end_time)}")

            if empty_windows >= early_stop_empty_windows:
                log(wallet, f"STOP — no more history (trades={len(all_fills)} empties={empty_windows})")
                termination_reason = "natural_end"
                break

            if empty_windows >= max_empty_windows:
                termination_reason = "natural_end"
                break

            if start_time <= 0:
                termination_reason = "time_floor"
                break

            continue

        empty_windows = 0

        if len(data) >= CAP_SHRINK_THRESHOLD and window_ms > MIN_WINDOW_MS:
            window_ms = max(window_ms // 2, MIN_WINDOW_MS)

            update_wallet_progress(
                wallet,
                status="running",
                end_time=end_time,
                window_ms=window_ms,
                cycles=cycles,
                empty_windows=empty_windows,
                suspected_truncated=suspected_truncated,
                trades=len(all_fills),
                updated_at=ms_to_iso(int(time.time() * 1000))
            )

            log(wallet, f"↓ shrinking window → {window_ms / 3600000:.1f}h")
            continue

        if len(data) >= CAP_SHRINK_THRESHOLD and window_ms <= MIN_WINDOW_MS:
            suspected_truncated = 1
            log(wallet, f"[WARN] response size {len(data)} at minimum window {window_ms / 3600000:.1f}h")

        new_rows = []

        for f in data:
            try:
                key = trade_key(f)
            except Exception:
                continue

            if key not in seen:
                seen.add(key)
                all_fills.append(f)
                new_rows.append(f)

        written = append_all_trades(wallet, new_rows)

        log(
            wallet,
            f"window {round(window_ms / 3600000, 1)}h | +{len(new_rows)} trades | "
            f"total={len(all_fills)} | wrote={written}"
        )
        log(wallet, f"progress cycles={cycles} trades={len(all_fills)} next_end={ms_to_iso(start_time)}")

        if start_time <= 0:
            termination_reason = "time_floor"
            break

        end_time = start_time

        update_wallet_progress(
            wallet,
            status="running",
            end_time=end_time,
            window_ms=window_ms,
            cycles=cycles,
            empty_windows=empty_windows,
            suspected_truncated=suspected_truncated,
            trades=len(all_fills),
            updated_at=ms_to_iso(int(time.time() * 1000))
        )

    update_wallet_progress(
        wallet,
        status="complete_fetch",
        end_time=end_time,
        window_ms=window_ms,
        cycles=cycles,
        empty_windows=empty_windows,
        suspected_truncated=suspected_truncated,
        trades=len(all_fills),
        termination_reason=termination_reason,
        updated_at=ms_to_iso(int(time.time() * 1000))
    )

    return all_fills, cycles, termination_reason, suspected_truncated


# ---------- KPI ----------
def compute_kpis(fills):
    if not fills:
        return None

    pnl_list = []
    pnl_curve = []
    pnl = 0.0

    wins = []
    losses = []
    times = []
    symbols = set()

    consec_win = 0
    consec_loss = 0
    max_consec_win = 0
    max_consec_loss = 0

    now = int(time.time() * 1000)
    trades_7d = 0

    fills_sorted = sorted(fills, key=lambda x: int(x["time"]))

    for f in fills_sorted:
        p = float(f.get("closedPnl", 0) or 0)

        pnl += p
        pnl_curve.append(pnl)
        pnl_list.append(p)

        t = int(f["time"])
        times.append(t)

        coin = f.get("coin")
        if coin:
            symbols.add(str(coin))

        if p > 0:
            wins.append(p)
            consec_win += 1
            consec_loss = 0
        elif p < 0:
            losses.append(abs(p))
            consec_loss += 1
            consec_win = 0
        else:
            consec_win = 0
            consec_loss = 0

        max_consec_win = max(max_consec_win, consec_win)
        max_consec_loss = max(max_consec_loss, consec_loss)

        if now - t <= 7 * 24 * 60 * 60 * 1000:
            trades_7d += 1

    trades = len(pnl_list)
    total_pnl = pnl
    avg_pnl = total_pnl / trades if trades else 0
    win_rate = len(wins) / trades if trades else 0
    pnl_std = statistics.stdev(pnl_list) if len(pnl_list) > 1 else 0

    peak = pnl_curve[0]
    max_dd = 0
    dd_duration = 0
    max_dd_duration = 0

    for x in pnl_curve:
        if x < peak:
            dd_duration += 1
        else:
            max_dd_duration = max(max_dd_duration, dd_duration)
            dd_duration = 0
        peak = max(peak, x)
        max_dd = min(max_dd, x - peak)

    recovery_time = max_dd_duration

    slope = (pnl_curve[-1] - pnl_curve[0]) / len(pnl_curve) if len(pnl_curve) > 1 else 0
    pnl_stability = (avg_pnl / pnl_std) if pnl_std > 0 else 0

    chunk = max(20, trades // 5) if trades else 20
    chunks = [pnl_list[i:i + chunk] for i in range(0, trades, chunk)]
    chunk_wr = [sum(1 for x in c if x > 0) / len(c) for c in chunks if c]
    consistency_score = 1 - statistics.stdev(chunk_wr) if len(chunk_wr) > 1 else 0

    gaps = [(times[i] - times[i - 1]) for i in range(1, len(times))]
    trade_freq_std = statistics.stdev(gaps) if len(gaps) > 1 else 0
    inactivity_score = max(gaps) if gaps else 0

    skew = (max(wins) / sum(wins)) if wins else 0

    size_seq = [abs(float(f.get("sz", 0) or 0)) for f in fills_sorted]
    martingale_flag = int(any(
        size_seq[i] > size_seq[i - 1] * 1.8
        for i in range(1, len(size_seq))
        if size_seq[i - 1] > 0
    ))

    one_big_trade_flag = int(max(wins) > total_pnl * 0.5) if wins and total_pnl > 0 else 0
    equity_collapse_flag = int(pnl_curve[-1] < max(pnl_curve) * 0.5) if pnl_curve else 0

    edge_score_raw = avg_pnl / (pnl_std + 1e-6)

    first_ts = min(times)
    last_ts = max(times)
    timespan_hours = (last_ts - first_ts) / (1000 * 60 * 60) if len(times) > 1 else 0

    return (
        trades, round(total_pnl, 2), round(win_rate, 3),
        round(avg_pnl, 4), round(pnl_std, 4),
        round(max_dd, 2),
        round(max(wins) if wins else 0, 2),
        round(max(losses) if losses else 0, 2),
        round((sum(wins) / sum(losses)) if losses else 0, 2),
        round((max(wins) / total_pnl) if total_pnl > 0 else 0, 3),
        max_consec_win, max_consec_loss, trades_7d,
        round(slope, 6), round(pnl_stability, 4), round(consistency_score, 4),
        max_dd_duration, recovery_time, martingale_flag,
        round(trade_freq_std, 2), inactivity_score,
        round(edge_score_raw, 4), round(skew, 3),
        one_big_trade_flag, equity_collapse_flag,
        ms_to_iso(first_ts), ms_to_iso(last_ts),
        round(timespan_hours, 2),
        len(symbols)
    )


# ---------- MAIN ----------
async def main():
    print("=== STAGE 2+3 WALLET SCANNER ===\n", flush=True)

    wallets = load_wallets()
    completed = load_summary_wallets()

    resolver = aiohttp.resolver.ThreadedResolver()

    connector = aiohttp.TCPConnector(
        resolver=resolver,
        limit=CONCURRENCY,
        limit_per_host=CONCURRENCY,
        ttl_dns_cache=300,
        family=socket.AF_INET
    )

    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:

        async def process(wallet, i):
            async with sem:
                log(wallet, f"[{i + 1}/{len(wallets)}] start")

                progress = load_progress().get(wallet, {})
                if wallet in completed and progress.get("status") != "running":
                    log(wallet, "skip")
                    return

                fills = None
                cycles = 0
                reason = "error"
                suspected_truncated = 0
                # --- NEW: CHEAP FILTER ---
                ok = await cheap_wallet_check(session, wallet)
                if not ok:
                    log(wallet, "SKIP — cheap filter")
                    return

                # --- NEW: PROBE FILTER ---
                ok = await probe_wallet(session, wallet)
                if not ok:
                    log(wallet, "SKIP — probe filter")
                    return

                for attempt in range(3):
                    fills, cycles, reason, suspected_truncated = await fetch_all_fills(session, wallet)
                    if fills is not None:
                        break
                    log(wallet, f"retry wallet ({attempt + 1}/3)")
                    await asyncio.sleep(2 + attempt)

                if not fills:
                    log(wallet, f"no data | end={reason}")
                    return

                kpis = compute_kpis(fills)
                if not kpis:
                    log(wallet, "kpi fail")
                    return

                trades = kpis[0]
                pnl = kpis[1]
                span = kpis[-2]
                symbols = kpis[-1]

                short_span_flag = int(span < 24)
                negative_total_flag = int(pnl < 0)

                remove_summary_wallet(wallet)
                append_summary((
                    wallet, *kpis,
                    cycles, reason,
                    suspected_truncated, short_span_flag, negative_total_flag
                ))

                clear_wallet_progress(wallet)

                log(
                    wallet,
                    f"DONE trades={trades} pnl={pnl} span={span}h "
                    f"symbols={symbols} cycles={cycles} "
                    f"end={reason} trunc={suspected_truncated}"
                )

        await asyncio.gather(*[process(w, i) for i, w in enumerate(wallets)])

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())