import csv
import os
from collections import defaultdict

# WALLET FINDER edition — local imports and data paths
from hl_mtm_lookup import get_mtm_stats, MTM_OUTPUT_COLUMNS

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
SUMMARY_FILE = os.path.join(DATA_DIR, "summary.csv")
TRADES_FILE = os.path.join(DATA_DIR, "all_trades.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "copyable_wallets.csv")

COPY_NOTIONAL = 20.0
SLIPPAGE_BPS = 5.0
FEE_BPS = 4.5

SLIPPAGE_PER_COIN_BPS = {
    "BTC": 2.0, "ETH": 3.0,
    "SOL": 5.0, "BNB": 5.0, "XRP": 5.0, "AVAX": 5.0, "LTC": 5.0, "DOGE": 5.0,
    "MATIC": 5.0, "DOT": 5.0, "LINK": 5.0, "TRX": 5.0, "BCH": 5.0, "ADA": 5.0,
    "ARB": 8.0, "OP": 8.0, "APT": 8.0, "ATOM": 8.0, "NEAR": 8.0, "FIL": 8.0,
    "INJ": 8.0, "SUI": 8.0, "TIA": 8.0, "JUP": 8.0, "AAVE": 8.0, "TON": 8.0,
    "HYPE": 15.0, "PENDLE": 15.0,
    "FARTCOIN": 40.0, "KPEPE": 40.0, "WIF": 40.0, "MOODENG": 40.0, "GOAT": 40.0,
    "PURR": 40.0, "POPCAT": 40.0, "KBONK": 40.0, "PEPE": 40.0,
}
SLIPPAGE_DEFAULT_BPS = 20.0

MIN_REALISED_SCORE = 0.01
MIN_MTM_CALMAR = 1.0
REQUIRE_MTM_POSITIVE_MONTH = True


def slippage_bps_for(coin: str) -> float:
    return SLIPPAGE_PER_COIN_BPS.get((coin or "").upper(), SLIPPAGE_DEFAULT_BPS)


def safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except:
        return default


def safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except:
        return default


def is_garbage(row):
    try:
        trades = safe_int(row.get("trades"), 0)
        span = safe_float(row.get("timespan_hours"), 0.0)
        if trades < 50:
            return True
        if span < 10:
            return True
        return False
    except:
        return True


def load_summary():
    if not os.path.exists(SUMMARY_FILE):
        raise FileNotFoundError(f"Missing summary file: {SUMMARY_FILE}")
    rows = []
    with open(SUMMARY_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    filtered = []
    by_wallet = {}
    for row in rows:
        wallet = str(row.get("wallet", "")).strip().lower()
        if not wallet:
            continue
        row["wallet"] = wallet
        if not is_garbage(row):
            filtered.append(row)
            by_wallet[wallet] = row
    return filtered, by_wallet


def load_trades_for_wallets(valid_wallets):
    if not os.path.exists(TRADES_FILE):
        raise FileNotFoundError(f"Missing trades file: {TRADES_FILE}")
    trades = defaultdict(list)
    with open(TRADES_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = str(row.get("wallet", "")).strip().lower()
            if wallet not in valid_wallets:
                continue
            coin = str(row.get("coin", "")).strip()
            side = str(row.get("side", "")).strip().lower()
            px = safe_float(row.get("px"), 0.0)
            ts = safe_int(row.get("time"), 0)
            if not wallet or not coin or side not in {"buy", "sell", "b", "a"} or px <= 0 or ts <= 0:
                continue
            trades[wallet].append({
                "wallet": wallet,
                "coin": coin,
                "side": "buy" if side in {"buy", "b"} else "sell",
                "px": px,
                "time": ts,
            })
    for wallet in trades:
        trades[wallet].sort(key=lambda x: x["time"])
    return trades


def simulate_wallet(trades):
    positions = {}
    realized_pnl = 0.0
    gross_win = 0.0
    gross_loss = 0.0
    pnl_curve = []
    symbols = set()
    times = []
    close_events = 0
    fee_rate = FEE_BPS / 10000.0

    for t in trades:
        coin = t["coin"]
        side = t["side"]
        raw_px = t["px"]
        ts = t["time"]
        symbols.add(coin)
        times.append(ts)
        slip = slippage_bps_for(coin) / 10000.0

        if side == "buy":
            exec_px = raw_px * (1.0 + slip)
            delta_qty = COPY_NOTIONAL / exec_px
        else:
            exec_px = raw_px * (1.0 - slip)
            delta_qty = -(COPY_NOTIONAL / exec_px)

        fee = abs(exec_px * delta_qty) * fee_rate

        if coin not in positions:
            positions[coin] = {"qty": 0.0, "avg_px": 0.0}

        pos = positions[coin]
        old_qty = pos["qty"]
        old_avg = pos["avg_px"]

        if old_qty == 0.0 or (old_qty > 0 and delta_qty > 0) or (old_qty < 0 and delta_qty < 0):
            new_qty = old_qty + delta_qty
            if abs(new_qty) > 1e-12:
                if old_qty == 0.0:
                    pos["avg_px"] = exec_px
                else:
                    pos["avg_px"] = (
                        (abs(old_qty) * old_avg) + (abs(delta_qty) * exec_px)
                    ) / abs(new_qty)
            pos["qty"] = new_qty
            realized_pnl -= fee
            pnl_curve.append(realized_pnl)
            continue

        close_qty = min(abs(old_qty), abs(delta_qty))
        direction = 1.0 if old_qty > 0 else -1.0
        trade_pnl = (exec_px - old_avg) * close_qty * direction
        trade_pnl -= fee
        realized_pnl += trade_pnl
        close_events += 1

        if trade_pnl >= 0:
            gross_win += trade_pnl
        else:
            gross_loss += abs(trade_pnl)

        remaining_qty = old_qty + delta_qty
        if abs(remaining_qty) < 1e-12:
            pos["qty"] = 0.0
            pos["avg_px"] = 0.0
        elif (old_qty > 0 and remaining_qty < 0) or (old_qty < 0 and remaining_qty > 0):
            pos["qty"] = remaining_qty
            pos["avg_px"] = exec_px
        else:
            pos["qty"] = remaining_qty
            pos["avg_px"] = old_avg

        pnl_curve.append(realized_pnl)

    if not times:
        return None

    peak = 0.0
    max_dd = 0.0
    for x in pnl_curve:
        if x > peak:
            peak = x
        dd = x - peak
        if dd < max_dd:
            max_dd = dd

    trades_count = len(trades)
    timespan_hours = max((max(times) - min(times)) / 3_600_000.0, 0.0)
    pnl_per_trade = realized_pnl / close_events if close_events > 0 else 0.0
    profit_factor = 999.0 if gross_loss == 0.0 and gross_win > 0.0 else (
        gross_win / gross_loss if gross_loss > 0.0 else 0.0
    )
    edge_score = 0.0
    if close_events > 0 and max_dd < 0.0:
        edge_score = pnl_per_trade / abs(max_dd)

    return {
        "total_pnl": round(realized_pnl, 6),
        "profit_factor": round(profit_factor, 6),
        "trades": close_events if close_events > 0 else trades_count,
        "timespan_hours": round(timespan_hours, 3),
        "symbol_count": len(symbols),
        "max_dd_realised": round(max_dd, 6),
        "score": round(edge_score, 6),
        "score_realised": round(edge_score, 6),
    }


def write_output(ranked_rows):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "wallet", "score", "total_pnl", "profit_factor", "trades",
            "timespan_hours", "symbol_count", "max_dd_realised", "score_realised",
            *MTM_OUTPUT_COLUMNS, "copyability_gate",
        ])
        for r in ranked_rows:
            writer.writerow([
                r["wallet"], r["score"], r["total_pnl"], r["profit_factor"],
                r["trades"], r["timespan_hours"], r["symbol_count"],
                r.get("max_dd_realised"), r.get("score_realised"),
                *[r.get(k) for k in MTM_OUTPUT_COLUMNS],
                r.get("copyability_gate", ""),
            ])


def evaluate_copyability_gate(sim_row: dict, mtm: dict) -> tuple[bool, str]:
    mtm_source = mtm.get("mtm_source")
    if mtm_source in ("hl_portfolio_api", "cache_stale"):
        cal = mtm.get("mtm_calmar")
        if cal is None or cal < MIN_MTM_CALMAR:
            return False, f"mtm_calmar<{MIN_MTM_CALMAR}"
        if REQUIRE_MTM_POSITIVE_MONTH:
            chg = mtm.get("month_pnl_chg_mtm") or 0.0
            if chg <= 0:
                return False, "month_mtm_pnl<=0"
        if mtm.get("equity_collapse_flag_mtm") == 1:
            return False, "equity_collapse_flag_mtm"
        return True, "passed_mtm"
    if sim_row.get("score_realised", 0.0) < MIN_REALISED_SCORE:
        return False, f"realised_score<{MIN_REALISED_SCORE}_NO_MTM_VET"
    return True, "passed_realised_only_NO_MTM_VET"


def main():
    summary_rows, summary_by_wallet = load_summary()
    valid_wallets = set(summary_by_wallet.keys())
    print(f"\nTotal wallets in summary: {len(summary_rows)}")
    print(f"After structural garbage filter: {len(valid_wallets)}")
    trades_by_wallet = load_trades_for_wallets(valid_wallets)
    ranked = []
    skipped_no_trades = 0
    for wallet in valid_wallets:
        trades = trades_by_wallet.get(wallet, [])
        if not trades:
            skipped_no_trades += 1
            continue
        sim = simulate_wallet(trades)
        if not sim:
            continue
        mtm = get_mtm_stats(wallet)
        passed, reason = evaluate_copyability_gate(sim, mtm)
        effective_score = mtm.get("mtm_calmar")
        if effective_score is None:
            effective_score = sim["score_realised"]
        row = {
            "wallet": wallet,
            "score": round(effective_score, 6),
            "total_pnl": sim["total_pnl"],
            "profit_factor": sim["profit_factor"],
            "trades": sim["trades"],
            "timespan_hours": sim["timespan_hours"],
            "symbol_count": sim["symbol_count"],
            "max_dd_realised": sim["max_dd_realised"],
            "score_realised": sim["score_realised"],
            "copyability_gate": "passed" if passed else f"failed:{reason}",
            **{k: mtm.get(k) for k in MTM_OUTPUT_COLUMNS},
        }
        if passed:
            ranked.append(row)
    ranked.sort(key=lambda x: float(x["score"]), reverse=True)
    print(f"Simulated wallets: {len(ranked)} passed gate")
    print(f"Skipped (no trades in all_trades.csv): {skipped_no_trades}")
    print("\nTOP 10 (ranked by MTM Calmar where available):\n")
    for i, r in enumerate(ranked[:10], 1):
        mtm_tag = r.get("mtm_source") or "no_mtm"
        print(
            f"{i}. {r['wallet']} | "
            f"score={r['score']} ({mtm_tag}) | "
            f"realised_pnl={r['total_pnl']} | "
            f"month_mtm_chg={r.get('month_pnl_chg_mtm')} | "
            f"month_mtm_mdd={r.get('max_drawdown_mtm')} | "
            f"trades={r['trades']}"
        )
    write_output(ranked)
    print(f"\nSaved {len(ranked)} wallets -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
