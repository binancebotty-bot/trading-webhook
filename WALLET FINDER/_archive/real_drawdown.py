"""
Compute real drawdown from portfolio JSON accountValueHistory (includes unrealised PnL).
Compare with our simulation results.
"""
import json
import os
import numpy as np

PORTF_DIR = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\copy_selection_run\wallet_portfolios"

WALLETS = {
    "0x9db82c": "0x9db82c502472d76742fdd69609dfcc6e01327401",
    "0x82d7eb": "0x82d7ebbd8106b08e91f8ac9f4ca97fbd98125c29",
    "0xf83858": "0xf83858e57d9f804f5ca1603bce82558119aeac7b",
    "0x811e8f": "0x811e8f6d80f38a2f0f8b606cb743a950638f0ad4",
}

def calc_real_drawdown(acct_values):
    """From actual account value curve, compute max drawdown."""
    vals = np.array(acct_values, dtype=np.float64)
    running_max = np.maximum.accumulate(vals)
    drawdowns = vals - running_max
    max_dd = float(np.min(drawdowns))
    peak = float(np.max(vals))
    trough = float(vals[np.argmin(drawdowns)])
    return {
        "peak_account_value": round(peak, 2),
        "trough_account_value": round(trough, 2),
        "max_drawdown_usd": round(abs(max_dd), 2),
        "max_drawdown_pct": round(abs(max_dd) / peak * 100, 2) if peak > 0 else 0,
        "final_account_value": round(float(vals[-1]), 2),
        "total_return_usd": round(float(vals[-1] - vals[0]), 2),
        "total_return_pct": round((vals[-1] - vals[0]) / vals[0] * 100, 2) if vals[0] > 0 else 0,
    }


def main():
    for short, full in WALLETS.items():
        path = os.path.join(PORTF_DIR, f"{full}.json")
        if not os.path.exists(path):
            print(f"\n=== {short} === MISSING")
            continue

        with open(path) as f:
            data = json.load(f)

        # Entry 0 is "day", 1 is "week", 2 is "month" — use "all" (last entry) for full history
        # Find the entry with the most data points
        best = None
        for entry in data:
            if isinstance(entry, list) and len(entry) == 2:
                label, metrics = entry
                avh = metrics.get("accountValueHistory", [])
                if best is None or len(avh) > len(best[1].get("accountValueHistory", [])):
                    best = entry

        if best is None:
            print(f"\n=== {short} === NO DATA")
            continue

        label, metrics = best
        avh = metrics.get("accountValueHistory", [])
        pnlh = metrics.get("pnlHistory", [])

        acct_values = [float(v) for _, v in avh]
        pnl_values = [float(v) for _, v in pnlh]

        result = calc_real_drawdown(acct_values)

        # Time span
        timestamps = [int(t) for t, _ in avh]
        hours = (timestamps[-1] - timestamps[0]) / 3600000
        days = hours / 24

        print(f"\n=== {short} ({label} data, {len(avh)} points, {days:.1f} days) ===")
        print(f"  Account Value Range: ${min(acct_values):,.2f} - ${max(acct_values):,.2f}")
        print(f"  PnL Range: ${min(pnl_values):,.2f} - ${max(pnl_values):,.2f}")
        print(f"  Peak Account Value: ${result['peak_account_value']:,.2f}")
        print(f"  Trough Account Value: ${result['trough_account_value']:,.2f}")
        print(f"  Max Drawdown (USD): ${result['max_drawdown_usd']:,.2f}")
        print(f"  Max Drawdown (%): {result['max_drawdown_pct']:.2f}%")
        print(f"  Final Account Value: ${result['final_account_value']:,.2f}")
        print(f"  Total Return (USD): ${result['total_return_usd']:,.2f}")
        print(f"  Total Return (%): {result['total_return_pct']:.2f}%")

        # Now compute Calmar from actual data
        # Monthly PnL from pnlHistory
        monthly_returns = {}
        for ts_ms, pnl_str in pnlh:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            month_key = (dt.year, dt.month)
            monthly_returns[month_key] = float(pnl_str)

        # Compute month-over-month changes
        months = sorted(monthly_returns.keys())
        month_changes = []
        for i in range(1, len(months)):
            curr = monthly_returns[months[i]]
            prev = monthly_returns[months[i - 1]]
            change = curr - prev
            month_changes.append(change)

        avg_month_change = np.mean(month_changes) if month_changes else 0
        dd_pct = result["max_drawdown_pct"]
        real_calmar = (avg_month_change / result["peak_account_value"] * 100) / dd_pct if dd_pct > 0 else 0

        print(f"  Monthly PnL changes: {[round(c, 2) for c in month_changes]}")
        print(f"  Avg Monthly Change: ${avg_month_change:,.2f}")
        print(f"  REAL Calmar (monthly_chg%/maxDD%): {real_calmar:.2f}")

        # Comparison with our sim
        print(f"\n  --- COMPARISON WITH SIM (from V2 results) ---")
        sim_data = {
            "0x9db82c": {"sim_dd": 68, "sim_pnl": 935, "sim_calmar": 13.67, "mode": "fixed $12"},
            "0x82d7eb": {"sim_dd": 66, "sim_pnl": 757, "sim_calmar": 11.44, "mode": "fixed $12"},
            "0xf83858": {"sim_dd": 44, "sim_pnl": 143, "sim_calmar": 3.23, "mode": "fixed $12"},
            "0x811e8f": {"sim_dd": 12, "sim_pnl": 181, "sim_calmar": 14.79, "mode": "fixed $12"},
        }
        if short in sim_data:
            s = sim_data[short]
            dd_ratio = result["max_drawdown_usd"] / s["sim_dd"] if s["sim_dd"] > 0 else float('inf')
            print(f"  Sim DD: ${s['sim_dd']} vs Real DD: ${result['max_drawdown_usd']} -> Ratio: {dd_ratio:.1f}x")
            print(f"  Sim PnL: ${s['sim_pnl']} vs Real Return: ${result['total_return_usd']} -> Ratio: {result['total_return_usd']/s['sim_pnl']:.1f}x")
            print(f"  Sim Calmar: {s['sim_calmar']} vs Real Calmar: {real_calmar:.2f}")


if __name__ == "__main__":
    main()