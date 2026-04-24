import csv
import os

INPUT_FILE = "hl_stage2\\summary.csv"
OUTPUT_FILE = "hl_stage2\\copyable_wallets.csv"


def is_garbage(row):
    try:
        lw = float(row["largest_win_ratio"])
        pf = float(row["profit_factor"])
        trades = int(row["trades"])
        span = float(row["timespan_hours"])

        # ONLY obvious garbage
        if lw > 1:
            return True

        if pf < 1.05:
            return True

        if trades < 50:
            return True

        if span < 10:
            return True

        return False
    except:
        return True


def score_wallet(row):
    pf = float(row["profit_factor"])
    pnl = float(row["total_pnl"])
    lw = float(row["largest_win_ratio"])
    span = float(row["timespan_hours"])
    trades = int(row["trades"])

    trades_per_hour = trades / max(span, 1)

    score = 0

    # reward real edge
    score += pf * 3
    score += min(pnl / 10000, 5)

    # reward distribution
    score -= lw * 10

    # penalise HFT
    score -= trades_per_hour * 0.2

    # penalise too many symbols
    score -= int(row["symbol_count"]) * 0.05

    return round(score, 3)


def load_existing():
    if not os.path.exists(OUTPUT_FILE):
        return {}

    existing = {}
    with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing[row["wallet"]] = row
    return existing


def write_output(ranked):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    existing = load_existing()

    merged = {}

    # existing first
    for w, r in existing.items():
        merged[w] = r

    # overwrite/add new
    for r in ranked:
        merged[r["wallet"]] = {
            "wallet": r["wallet"],
            "score": r["score"],
            "total_pnl": r["total_pnl"],
            "profit_factor": r["profit_factor"],
            "trades": r["trades"],
            "timespan_hours": r["timespan_hours"],
            "symbol_count": r["symbol_count"],
        }

    final = list(merged.values())

    # sort again
    final = sorted(final, key=lambda x: float(x["score"]), reverse=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "wallet",
            "score",
            "total_pnl",
            "profit_factor",
            "trades",
            "timespan_hours",
            "symbol_count"
        ])

        for r in final:
            writer.writerow([
                r["wallet"],
                r["score"],
                r["total_pnl"],
                r["profit_factor"],
                r["trades"],
                r["timespan_hours"],
                r["symbol_count"]
            ])


def main():
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"\nTotal wallets: {len(rows)}")

    clean = []

    for r in rows:
        if not is_garbage(r):
            r["score"] = score_wallet(r)
            clean.append(r)

    print(f"After garbage removal: {len(clean)}")

    ranked = sorted(clean, key=lambda x: x["score"], reverse=True)

    print("\nTOP 10:\n")

    for i, r in enumerate(ranked[:10], 1):
        print(
            f"{i}. {r['wallet']} | score={r['score']} | pnl={r['total_pnl']} | pf={r['profit_factor']}"
        )

    # ✅ NEW: persist full ranked universe
    write_output(ranked)

    print(f"\nSaved {len(ranked)} wallets → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()