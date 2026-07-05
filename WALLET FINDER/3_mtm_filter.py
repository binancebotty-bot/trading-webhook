"""
hl_stage1_5_mtm_filter.py — WALLET FINDER EDITION

STAGE 1.5: MTM CALMAR PRE-FILTER

Sits between Stage 1 (cheap userFills filter) and Stage 2 (expensive deep
dive). Uses Hyperliquid's `portfolio` API (accountValueHistory) which
returns PnL curves and drawdown truth in a single call per wallet.

INPUT:   data/simple_filter_pass.csv  (from Stage 1 filter)
OUTPUT:  data/hl_stage1_5_mtm_pass.csv  (ranked survivors)
         data/hl_stage1_5_mtm_pending.csv  (wallets needing retry — data unavailable)
         data/stage1_5_progress.txt     (resume tracking)

Gates (all must pass):
  1. month_acctV_end >= 1000  —  leader has real capital in play
  2. allTime_pnl_chg_mtm > 0  —  net profitable across all data (not just one month)
  3. mtm_source is available  —  retry if not, never reject for missing data
  4. PnL/DD ratio >= 1.5  —  quality: earns ≥$1.50 per $1 of drawdown

Removed gates (already covered by Stage 1 or redundant):
  - mtm_calmar: covered by Stage 1's DD<40% + PnL/DD>=1.0
  - equity_collapse_flag: covered by Stage 1's allTime DD check
  - negative_month: too short a window; replaced by allTime profitable

Wallets with unavailable MTM data are written to pending CSV for retry,
NOT rejected — missing data is an error to fix, not a disqualifier.

Output is sorted by PnL/DD ratio descending so Stage 2 processes the
best wallets first.
"""
import asyncio
import aiohttp
import csv
import os
import socket
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
DATA_DIR = _HERE / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Add WALLET FINDER to path for hl_mtm_lookup import
import sys
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from hl_mtm_lookup import get_mtm_stats_async, MTM_OUTPUT_COLUMNS
from real_dd_filter import real_dd_gate as _real_dd_gate_fn
from typing import Dict, Any

INPUT_FILE = DATA_DIR / "simple_filter_pass.csv"
OUTPUT_FILE = DATA_DIR / "hl_stage1_5_mtm_pass.csv"
PROGRESS_FILE = DATA_DIR / "stage1_5_progress.txt"

# ── Gates (configurable) ──────────────────────────────────────
MIN_ACCTV_END = float(os.getenv("HL_S1_5_MIN_ACCTV_END", "1000.0"))
MAX_CONCURRENCY = int(os.getenv("HL_S1_5_CONCURRENCY", "10"))

# ── Real DD gate (opt-in) ────────────────────────────────────────
_val = os.getenv("HL_S1_5_MAX_REAL_DD_PCT")
MAX_REAL_DD_PCT = float(_val) if _val is not None and _val.strip() != "" else None

# ── PnL/DD ratio gate (primary quality filter) ──────────────────
# Wallets must make ≥ MIN_PNL_DD_RATIO per unit of max drawdown.
# Uses allTime MTM data: allTime_pnl_chg_mtm / abs(allTime_max_drawdown_mtm)
_MIN_RAT = os.getenv("HL_S1_5_MIN_PNL_DD_RATIO")
MIN_PNL_DD_RATIO = float(_MIN_RAT) if _MIN_RAT is not None and _MIN_RAT.strip() != "" else 1.5


# ── Output columns ────────────────────────────────────────────
# Derived from hl_mtm_lookup.MTM_OUTPUT_COLUMNS — keeps Stage 1.5 output
# in sync with the canonical MTM column set (single source of truth).
REAL_DD_COLUMNS = [
    "real_max_dd_usd", "real_max_dd_pct", "dd_source",
    "dd_gate_pass", "dd_gate_reason",
]
OUTPUT_COLUMNS = ["wallet"] + MTM_OUTPUT_COLUMNS + REAL_DD_COLUMNS
OUTPUT_PENDING = DATA_DIR / "hl_stage1_5_mtm_pending.csv"


def log(msg: str) -> None:
    print(f"[S1.5] {msg}", flush=True)


def load_progress() -> set[str]:
    if not PROGRESS_FILE.exists():
        return set()
    with open(PROGRESS_FILE, encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}


def save_progress(wallet: str) -> None:
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{wallet.lower()}\n")


def load_wallets() -> list[str]:
    if not INPUT_FILE.exists():
        return []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        return [row["wallet"].strip().lower() for row in csv.DictReader(f) if row.get("wallet")]


def write_output(rows: list[dict]) -> None:
    """Write ranked output CSV atomically."""
    tmp = OUTPUT_FILE.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, OUTPUT_FILE)


def gate_mtm(stats: dict) -> tuple[bool, str]:
    """Return (pass, reason). All gates must pass.

    Three outcomes:
      - (True, "ok")          → wallet passes all gates
      - (False, "mtm_data_missing:...") → data unavailable → RETRY, not reject
      - (False, "reason")     → genuinely failed a gate → rejected
    """
    source = stats.get("mtm_source", "unavailable")

    # ── Data availability check ──────────────────────────────────
    # Wallets with missing data are NOT rejected — they're flagged for retry.
    # Missing data is an error to fix, not a disqualifier.
    if source not in ("hl_portfolio_api", "cache_stale"):
        return False, f"mtm_data_missing:{source}"

    # ── Gate 1: Capital in play >= $1,000 ────────────────────────
    # Wallet must have meaningful capital at stake.
    end_val = stats.get("month_acctV_end") or 0.0
    if end_val < MIN_ACCTV_END:
        return False, f"skin_in_game_{end_val:.0f}_<_{MIN_ACCTV_END:.0f}"

    # ── Gate 2: Net profitable across ALL data ───────────────────
    # Not just one month — allTime PnL must be positive.
    # This ensures we only copy wallets with proven, sustained profitability.
    alltime_pnl = stats.get("allTime_pnl_chg_mtm")
    if alltime_pnl is not None and alltime_pnl <= 0:
        return False, f"allTime_not_profitable_{alltime_pnl:.0f}"

    # ── Gate 3: Real DD gate (opt-in) ────────────────────────────
    if MAX_REAL_DD_PCT is not None:
        dd_result = _real_dd_gate_fn(stats, MAX_REAL_DD_PCT)
        if not dd_result["dd_gate_pass"]:
            return False, f"real_dd:{dd_result['dd_gate_reason']}"

    # ── Gate 4: PnL/DD ratio >= 1.5 ─────────────────────────────
    # Quality filter: wallet must earn ≥ $1.50 per $1 of drawdown.
    alltime_dd = stats.get("allTime_max_drawdown_mtm")
    if alltime_pnl is not None and alltime_dd is not None and abs(alltime_dd) > 1.0:
        pnl_dd_ratio = alltime_pnl / abs(alltime_dd)
        if pnl_dd_ratio < MIN_PNL_DD_RATIO:
            return False, f"pnl_dd_{pnl_dd_ratio:.2f}_<_{MIN_PNL_DD_RATIO}"

    return True, "ok"


async def fetch_mtm(session, wallet: str, sem: asyncio.Semaphore, stats: dict) -> dict:
    """Fetch MTM stats for a wallet, updating global counters."""
    async with sem:
        try:
            mtm = await get_mtm_stats_async(session, wallet)
        except Exception as e:
            mtm: dict[str, Any] = {k: None for k in MTM_OUTPUT_COLUMNS}
            mtm["mtm_source"] = f"error_{str(e)[:60]}"
    passed, reason = gate_mtm(mtm)
    stats["total"] += 1
    if passed:
        stats["passed"] += 1
    elif reason.startswith("mtm_data_missing:"):
        stats["pending_retry"] = stats.get("pending_retry", 0) + 1
        stats["reasons"][reason] = stats["reasons"].get(reason, 0) + 1
    else:
        stats["rejected"] += 1
        stats["reasons"][reason] = stats["reasons"].get(reason, 0) + 1

    # Attach real DD columns for audit trail
    if MAX_REAL_DD_PCT is not None:
        dd_gate_result = _real_dd_gate_fn(mtm, MAX_REAL_DD_PCT)
    else:
        dd_gate_result = {
            "real_max_dd_usd": None, "real_max_dd_pct": None,
            "dd_source": "DATA_FETCH_BLOCKED", "dd_gate_pass": True,
            "dd_gate_reason": "gate_disabled",
        }

    return {**mtm, "wallet": wallet, "_passed": passed, "_reason": reason, **dd_gate_result}


async def _run_pass(
    session: aiohttp.ClientSession,
    wallets: list[str],
    sem: asyncio.Semaphore,
    stats: dict,
    t0: float,
    retry_label: str = "",
) -> tuple[list[dict], list[str]]:
    """Run a pass over a list of wallets, returning (passed_results, data_missing_wallets)."""
    results: list[dict] = []
    data_missing: list[str] = []
    batch_size = 50
    label = f" {retry_label}" if retry_label else ""
    for i in range(0, len(wallets), batch_size):
        batch = wallets[i:i + batch_size]
        tasks = [fetch_mtm(session, w, sem, stats) for w in batch]
        batch_results = await asyncio.gather(*tasks)

        for r in batch_results:
            reason = r.get("_reason", "")
            if r.pop("_passed"):
                results.append({k: r.get(k) for k in OUTPUT_COLUMNS})
            elif reason.startswith("mtm_data_missing:"):
                data_missing.append(r["wallet"])

        elapsed = time.monotonic() - t0
        rate = stats["total"] / elapsed if elapsed > 0 else 0
        log(f"Progress{label} {i+len(batch)}/{len(wallets)} | "
            f"pass={stats['passed']} reject={stats['rejected']} | "
            f"{rate:.1f} wallets/s | top_reasons: {_top_reasons(stats['reasons'])}")
    return results, data_missing


async def main() -> None:
    log("=== STAGE 1.5 — MTM FILTER (WALLET FINDER) ===")
    log(f"Gates: acctV_end>={MIN_ACCTV_END} | allTime profitable | source=OK (retry if missing) | pnl/dd>={MIN_PNL_DD_RATIO}")
    log(f"Concurrency: {MAX_CONCURRENCY}")

    wallets = load_wallets()
    if not wallets:
        log(f"[!] Missing input: {INPUT_FILE}")
        return

    completed = load_progress()
    remaining = [w for w in wallets if w not in completed]
    log(f"Total stage1 pass: {len(wallets)} | Already checked: {len(completed)} | Remaining: {len(remaining)}")

    if not remaining:
        log("All wallets already processed — loading previous results.")
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            log(f"Previously ranked: {len(rows)} wallets")
            _show_summary(rows)
        return

    stats: dict = {"total": 0, "passed": 0, "rejected": 0, "pending_retry": 0, "reasons": {}}
    results: list[dict] = []

    resolver = aiohttp.resolver.ThreadedResolver()
    connector = aiohttp.TCPConnector(resolver=resolver, family=socket.AF_INET)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    t0 = time.monotonic()

    async with aiohttp.ClientSession(connector=connector) as session:
        # ── Pass 1: process all remaining wallets ──
        all_results, data_missing_wallets = await _run_pass(session, remaining, sem, stats, t0)

        still_missing = []

        # ── Pass 2 (retry): wallets that failed due to missing API data ──
        # These get a second chance with a fresh cache-bust after a cooldown.
        if data_missing_wallets:
            log(f"\n--- RETRY PASS: {len(data_missing_wallets)} wallets with missing data (waiting 5s for rate-limit cooldown) ---")
            await asyncio.sleep(5)

            # Clear any stale cache for these wallets so they hit the API fresh
            for w in data_missing_wallets:
                cache_p = _HERE / "data" / "wallet_portfolios" / f"{w.lower()}.json"
                if cache_p.exists():
                    try:
                        cache_p.unlink()
                    except OSError:
                        pass

            # Remove these from progress so they get processed
            for w in data_missing_wallets:
                completed.discard(w.lower())

            # Remove stale results from the passed list
            retry_set = {w.lower() for w in data_missing_wallets}
            all_results = [r for r in all_results if r["wallet"].lower() not in retry_set]

            retry_stats: dict = {"total": 0, "passed": 0, "rejected": 0, "pending_retry": 0, "reasons": {}}
            retry_results, still_missing = await _run_pass(
                session, data_missing_wallets, sem, retry_stats, t0,
                retry_label="[RETRY]",
            )
            all_results.extend(retry_results)
            if still_missing:
                log(f"  {len(still_missing)} wallets still missing data after retry — written to pending CSV")

            # Merge retry stats
            stats["total"] += retry_stats["total"]
            stats["passed"] += retry_stats["passed"]
            stats["rejected"] += retry_stats["rejected"]
            stats["pending_retry"] = stats.get("pending_retry", 0) + retry_stats.get("pending_retry", 0)
            for k, v in retry_stats["reasons"].items():
                stats["reasons"][k] = stats["reasons"].get(k, 0) + v

    elapsed = time.monotonic() - t0

    # Load any previous results and merge
    prev_passed: list[dict] = []
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
            prev_passed = list(csv.DictReader(f))
    existing_wallets = {r["wallet"] for r in prev_passed}
    new_results = [r for r in all_results if r["wallet"] not in existing_wallets]
    all_results = prev_passed + new_results

    # Sort by PnL/DD ratio descending (best risk-adjusted first)
    all_results.sort(
        key=lambda r: float(r.get("allTime_pnl_chg_mtm") or 0) / max(1.0, abs(float(r.get("allTime_max_drawdown_mtm") or -1.0))),
        reverse=True,
    )
    write_output(all_results)

    # Write pending wallets (data unavailable — need retry, not rejection)
    if still_missing:
        pending_rows = [{"wallet": w} for w in still_missing]
        tmp = OUTPUT_PENDING.with_suffix(".csv.tmp")
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["wallet"])
            writer.writeheader()
            writer.writerows(pending_rows)
        os.replace(tmp, OUTPUT_PENDING)
        log(f"  Pending retry:     {len(still_missing)} wallets → {OUTPUT_PENDING}")

    # Save progress only AFTER output is safely written.
    for r in all_results:
        save_progress(r["wallet"])

    log(f"\n{'='*60}")
    log(f"STAGE 1.5 COMPLETE")
    log(f"  Wallets checked:   {stats['total']}")
    log(f"  Passed (advance):  {stats['passed']} ({stats['passed']/max(1,stats['total'])*100:.1f}%)")
    log(f"  Rejected:          {stats['rejected']} ({stats['rejected']/max(1,stats['total'])*100:.1f}%)")
    log(f"  Pending retry:     {stats.get('pending_retry', 0)}")
    log(f"  Elapsed:           {elapsed:.1f}s ({elapsed/max(1,stats['total']):.3f}s per wallet)")
    log(f"  Output:            {OUTPUT_FILE} ({len(all_results)} wallets)")
    if MAX_REAL_DD_PCT is not None:
        real_dd_rejected = sum(1 for r in all_results if not r.get("dd_gate_pass", True))
        log(f"  Real DD gate ({MAX_REAL_DD_PCT}%): rejected {real_dd_rejected} wallets")
    else:
        log(f"  Real DD gate: disabled (set HL_S1_5_MAX_REAL_DD_PCT to enable)")
    _show_summary(all_results)


def _show_summary(results: list[dict]) -> None:
    """Print top 10 survivors."""
    if not results:
        log("No survivors.")
        return
    log("\nTop 10 MTM-ranked wallets:")
    for i, r in enumerate(results[:10], 1):
        calmar = float(r.get("mtm_calmar") or 0)
        pnl = float(r.get("month_pnl_chg_mtm") or 0)
        dd = float(r.get("max_drawdown_mtm") or 0)
        acctv = float(r.get("month_acctV_end") or 0)
        log(f"  {i:2d}. {r['wallet'][:10]}…{r['wallet'][-6:]} | "
            f"Calmar={calmar:.2f} | PnL=${pnl:,.0f} | DD=${dd:,.0f} | "
            f"AcctV=${acctv:,.0f}")


def _top_reasons(reasons: dict, n: int = 3) -> str:
    sorted_reasons = sorted(reasons.items(), key=lambda x: -x[1])
    return ", ".join(f"{k}:{v}" for k, v in sorted_reasons[:n])


if __name__ == "__main__":
    asyncio.run(main())
