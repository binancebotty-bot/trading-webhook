import csv

INPUT_FILE = "hl_stage2\\summary.csv"


def analyze_wallet(row):
    reasons = []

    trades = int(row["trades"])
    pnl = float(row["total_pnl"])
    pf = float(row["profit_factor"])
    lw = float(row["largest_win_ratio"])
    span = float(row["timespan_hours"])
    symbols = int(row["symbol_count"])
    one_big = int(row["one_big_trade_flag"])
    collapse = int(row["equity_collapse_flag"])

    trades_per_hour = trades / max(span, 1)

    # ---------- HARD REJECTS ----------
    if one_big == 1:
        reasons.append("one_big_trade")

    if lw > 1:
        reasons.append("lottery_lw")

    if pf < 1.05:
        reasons.append("no_edge_pf")

    if trades < 50:
        reasons.append("too_few_trades")

    if span < 10:
        reasons.append("too_short_span")

    if collapse == 1:
        reasons.append("equity_collapse")

    # ---------- COPYABILITY FLAGS (NOT REJECT) ----------
    flags = []

    if trades_per_hour > 20:
        flags.append("HFT")

    if symbols > 40:
        flags.append("too_many_symbols")

    if pf > 20:
        flags.append("extreme_pf")

    if pnl > 200000:
        flags.append("extreme_pnl")

    return reasons, flags


def score_wallet(row):
    pf = float(row["profit_factor"])
    pnl = float(row["total_pnl"])
    lw = float(row["largest_win_ratio"])
    span = float(row["timespan_hours"])
    trades = int(row["trades"])
    symbols = int(row["symbol_count"])

    trades_per_hour = trades / max(span, 1)

    score = 0

    # base edge
    score += pf * 2
    score += min(pnl / 20000, 5)

    # distribution quality
    score -= lw * 10

    # penalties (soft)
    score -= trades_per_hour * 0.3
    score -= symbols * 0.05

    return round(score, 3)


def main():
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"\nTotal wallets: {len(rows)}\n")

    survivors = []
    rejected = 0

    for r in rows:
        reasons, flags = analyze_wallet(r)

        if reasons:
            rejected += 1
            print(f"REJECT {r['wallet']} → {', '.join(reasons)}")
            continue

        r["flags"] = ",".join(flags)
        r["score"] = score_wallet(r)
        survivors.append(r)

    print(f"\nSurvivors: {len(survivors)}")
    print(f"Rejected: {rejected}")

    ranked = sorted(survivors, key=lambda x: x["score"], reverse=True)

    print("\nTOP 15 COPYABLE CANDIDATES:\n")

    for i, r in enumerate(ranked[:15], 1):
        print(
            f"{i}. {r['wallet']} | score={r['score']} | "
            f"pnl={r['total_pnl']} | pf={r['profit_factor']} | flags={r['flags']}"
        )


if __name__ == "__main__":
    main()