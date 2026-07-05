"""Cross-reference 8012 dashboard DD values against raw portfolio JSONs."""
import json
import os
import numpy as np
import csv

PORTF_DIR = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\copy_selection_run\wallet_portfolios"
SUMMARY = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\data\summary.csv"
UNIVERSE = r"C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\data\wallet_universe.csv"

# Dashboard values from the 10 rows shown to user
DASH = [
    {"prefix": "0x716d7ae4", "cscore": 55.0, "pnl": 14846.08, "maxdd_usd": 414011.37, "maxdd_pct": 100.0, "mtm_usd": 2080.95, "mtm_pct": 1.6, "trades": 14790, "flags": ""},
    {"prefix": "0xfbf4b6bf", "cscore": 12.0, "pnl": 86930.36, "maxdd_usd": 170748.30, "maxdd_pct": 100.0, "mtm_usd": 29162.51, "mtm_pct": 100.0, "trades": 6374, "flags": "EC"},
    {"prefix": "0x6f83ab88", "cscore": 66.0, "pnl": 113709.13, "maxdd_usd": 21351.86, "maxdd_pct": 6.1, "mtm_usd": 10852.72, "mtm_pct": 3.1, "trades": 1777, "flags": ""},
    {"prefix": "0x4d293ca7", "cscore": 61.4, "pnl": 6251.55, "maxdd_usd": 7856.75, "maxdd_pct": 46.2, "mtm_usd": 179.29, "mtm_pct": 1.8, "trades": 645, "flags": ""},
    {"prefix": "0x67ab0e9e", "cscore": 68.3, "pnl": 60167.96, "maxdd_usd": 137194.80, "maxdd_pct": 95.3, "mtm_usd": 3946.55, "mtm_pct": 4.4, "trades": 20838, "flags": ""},
    {"prefix": "0x6dfda7c3", "cscore": 66.5, "pnl": 32661.49, "maxdd_usd": 30129.52, "maxdd_pct": 85.6, "mtm_usd": 159.50, "mtm_pct": 0.5, "trades": 8163, "flags": ""},
    {"prefix": "0x1b5c2bbf", "cscore": 67.2, "pnl": 7123.71, "maxdd_usd": 5117.12, "maxdd_pct": 27.2, "mtm_usd": 2022.62, "mtm_pct": 10.8, "trades": 1154, "flags": ""},
    {"prefix": "0x5ce7b350", "cscore": 61.9, "pnl": 3440.72, "maxdd_usd": 2002.60, "maxdd_pct": 20.0, "mtm_usd": 529.72, "mtm_pct": 5.2, "trades": 444, "flags": ""},
    {"prefix": "0x373b036b", "cscore": 0.0, "pnl": 7109.18, "maxdd_usd": 31100.12, "maxdd_pct": 85.1, "mtm_usd": 898.17, "mtm_pct": 11.4, "trades": 14311, "flags": "1B EC"},
    {"prefix": "0x98d0e608", "cscore": 34.8, "pnl": 295154.65, "maxdd_usd": 1151898.15, "maxdd_pct": 95.4, "mtm_usd": 39007.94, "mtm_pct": 41.2, "trades": 18199, "flags": ""},
]


def find_full_address(prefix):
    """Search summary.csv, universe.csv, and portfolio dir for full address."""
    for csv_path in [SUMMARY, UNIVERSE]:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                w = row.get("wallet", "")
                if w.startswith(prefix):
                    return w
    # Check portfolio dir
    for fname in os.listdir(PORTF_DIR):
        if fname.startswith(prefix) and fname.endswith(".json"):
            return fname[:-5]  # strip .json
    return None


def summarise_hl(data):
    """Replicate hl_mtm_lookup._summarise() exactly."""
    if not isinstance(data, list):
        return None
    periods = {}
    for x in data:
        if isinstance(x, list) and len(x) == 2:
            periods[x[0]] = x[1]

    def _stats(series):
        if not series:
            return 0.0, 0.0, 0.0, 0.0
        vals = [float(p[1]) for p in series if isinstance(p, list) and len(p) == 2]
        if not vals:
            return 0.0, 0.0, 0.0, 0.0
        peak = vals[0]
        mdd = 0.0
        peak_global = vals[0]
        for v in vals:
            if v > peak:
                peak = v
            if v > peak_global:
                peak_global = v
            if v - peak < mdd:
                mdd = v - peak
        return vals[0], vals[-1], mdd, peak_global

    # Month period
    month = periods.get("month", {}) or {}
    avh_m = month.get("accountValueHistory") or []
    m_start, m_end, m_mdd, m_peak = _stats(avh_m)
    month_chg = m_end - m_start

    # allTime period
    allt = periods.get("allTime", {}) or {}
    avh_a = allt.get("accountValueHistory") or []
    a_start, a_end, a_mdd, a_peak = _stats(avh_a)

    return {
        "max_drawdown_mtm": round(m_mdd, 4),          # month DD (negative)
        "allTime_max_drawdown_mtm": round(a_mdd, 4),   # allTime DD (negative)
        "allTime_acctV_peak": round(a_peak, 4),
        "month_acctV_peak": round(m_peak, 4),
        "month_acctV_end": round(m_end, 4),
        "a_start": a_start, "a_end": a_end,
        "m_start": m_start, "m_end": m_end,
        "a_data_points": len(avh_a),
        "m_data_points": len(avh_m),
        "a_first_ts": int(avh_a[0][0]) if avh_a else 0,
        "a_last_ts": int(avh_a[-1][0]) if avh_a else 0,
    }


def compute_raw_avh_dd(avh):
    """Compute DD directly from accountValueHistory array (independent check)."""
    vals = np.array([float(v) for _, v in avh], dtype=np.float64)
    rm = np.maximum.accumulate(vals)
    dd_curve = vals - rm
    peak = float(np.max(vals))
    trough = float(vals[np.argmin(dd_curve)])
    max_dd = abs(float(np.min(dd_curve)))
    dd_pct = (max_dd / peak * 100) if peak > 0 else 0.0
    return max_dd, dd_pct, peak, trough, len(vals)


print("=" * 140)
print("WALLET DD SANITY CHECK — Dashboard vs Portfolio JSON vs _summarise()")
print("=" * 140)

results = []
for d in DASH:
    prefix = d["prefix"]
    full = find_full_address(prefix)
    if not full:
        print(f"\n{prefix} — FULL ADDRESS NOT FOUND")
        results.append({**d, "full": "NOT_FOUND"})
        continue

    path = os.path.join(PORTF_DIR, f"{full}.json")
    if not os.path.exists(path):
        print(f"\n{prefix} ({full}) — NO PORTFOLIO JSON")
        results.append({**d, "full": full, "json": "MISSING"})
        continue

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 1) Replicate _summarise()
    stats = summarise_hl(data)

    # 2) Raw AVH DD (independent check)
    periods = {}
    for x in data:
        if isinstance(x, list) and len(x) == 2:
            periods[x[0]] = x[1]

    # allTime raw DD
    allt_avh = periods.get("allTime", {}).get("accountValueHistory", [])
    if allt_avh:
        raw_all_dd, raw_all_pct, raw_all_peak, raw_all_trough, raw_all_n = compute_raw_avh_dd(allt_avh)
    else:
        raw_all_dd, raw_all_pct, raw_all_peak, raw_all_trough, raw_all_n = 0, 0, 0, 0, 0

    # Month raw DD
    month_avh = periods.get("month", {}).get("accountValueHistory", [])
    if month_avh:
        raw_m_dd, raw_m_pct, raw_m_peak, raw_m_trough, raw_m_n = compute_raw_avh_dd(month_avh)
    else:
        raw_m_dd, raw_m_pct, raw_m_peak, raw_m_trough, raw_m_n = 0, 0, 0, 0, 0

    # Time span
    hours = (stats["a_last_ts"] - stats["a_first_ts"]) / 3600000 if stats["a_first_ts"] else 0

    # Dashboard expects: max_dd = allTime MTM (Tier 1), mtm_dd = monthly MTM
    # Both _summarise() and raw compute should agree

    print(f"\n{'='*140}")
    print(f"  {prefix} -> {full}")
    print(f"  JSON periods: {list(periods.keys())}")
    print(f"  allTime: {stats['a_data_points']} points, {hours/24:.0f} days, peak=${stats['allTime_acctV_peak']:,.2f}")
    print(f"  month:   {stats['m_data_points']} points, end=${stats['m_end']:,.2f}")
    print(f"{'─'*140}")
    print(f"  {'Metric':<30} {'_summarise()':>18} {'Raw AVH DD':>18} {'Dashboard':>18} {'Match?':>12}")
    print(f"{'─'*140}")

    # allTime DD
    s_at_dd = stats["allTime_max_drawdown_mtm"]  # negative
    s_at_pct = abs(s_at_dd) / stats["allTime_acctV_peak"] * 100 if stats["allTime_acctV_peak"] > 0 else 0
    raw_at_pct = raw_all_pct
    dash_at_dd = d["maxdd_usd"]  # positive (displayed)
    dash_at_pct = d["maxdd_pct"]

    # Compare percentages
    if dash_at_pct > 0:
        at_diff = abs(raw_at_pct - dash_at_pct) / dash_at_pct * 100
        at_match = "OK" if at_diff < 10 else "CLOSE" if at_diff < 30 else "MISMATCH"
    else:
        at_diff = 0
        at_match = "N/A"

    print(f"  {'allTime DD (USD)':<30} {abs(s_at_dd):>17,.2f} {raw_all_dd:>17,.2f} {dash_at_dd:>17,.2f} {at_match:>12}")
    print(f"  {'allTime DD (%)':<30} {s_at_pct:>17.1f}% {raw_at_pct:>17.1f}% {dash_at_pct:>17.1f}% {at_match:>12}")
    print(f"  {'allTime Peak':<30} {stats['allTime_acctV_peak']:>17,.2f} {raw_all_peak:>17,.2f}")

    # Month DD (MTM column in dashboard)
    s_m_dd = stats["max_drawdown_mtm"]  # negative
    s_m_pct = abs(s_m_dd) / stats["month_acctV_peak"] * 100 if stats["month_acctV_peak"] > 0 else 0
    raw_m_pct_val = raw_m_pct
    dash_m_dd = d["mtm_usd"]  # positive (displayed)
    dash_m_pct = d["mtm_pct"]

    if dash_m_pct > 0:
        m_diff = abs(raw_m_pct_val - dash_m_pct) / dash_m_pct * 100
        m_match = "OK" if m_diff < 10 else "CLOSE" if m_diff < 30 else "MISMATCH"
    else:
        m_diff = 0
        m_match = "N/A"

    print(f"  {'Month DD (MTM) (USD)':<30} {abs(s_m_dd):>17,.2f} {raw_m_dd:>17,.2f} {dash_m_dd:>17,.2f} {m_match:>12}")
    print(f"  {'Month DD (MTM) (%)':<30} {s_m_pct:>17.1f}% {raw_m_pct_val:>17.1f}% {dash_m_pct:>17.1f}% {m_match:>12}")
    print(f"  {'Month Peak':<30} {stats['month_acctV_peak']:>17,.2f} {raw_m_peak:>17,.2f}")

    # Flag suspicious
    issues = []
    if at_match == "MISMATCH":
        issues.append(f"AllTime DD mismatch: JSON={raw_at_pct:.1f}% vs Dashboard={dash_at_pct:.1f}%")
    if m_match == "MISMATCH":
        issues.append(f"Month DD mismatch: JSON={raw_m_pct_val:.1f}% vs Dashboard={dash_m_pct:.1f}%")
    if dash_at_pct == 100.0 and raw_at_pct < 95.0:
        issues.append(f"SUSPICIOUS: Dashboard shows 100% DD but JSON only {raw_at_pct:.1f}%")
    if dash_at_pct < 100.0 and raw_at_pct > 95.0:
        issues.append(f"NOTE: Dashboard understates DD ({dash_at_pct:.1f}% vs JSON {raw_at_pct:.1f}%)")

    if issues:
        print(f"\n  *** ISSUES ***")
        for iss in issues:
            print(f"    - {iss}")

    results.append({
        **d,
        "full": full,
        "json_all_dd": raw_all_dd,
        "json_all_pct": raw_at_pct,
        "json_month_dd": raw_m_dd,
        "json_month_pct": raw_m_pct_val,
        "json_peak": raw_all_peak,
        "summarise_all_dd": abs(s_at_dd),
        "summarise_month_dd": abs(s_m_dd),
        "at_match": at_match,
        "m_match": m_match,
    })

print(f"\n\n{'='*140}")
print("SUMMARY TABLE")
print("=" * 140)
print(f"{'#':<3} {'Wallet':<12} {'Dash MaxDD%':>10} {'JSON AllDD%':>11} {'Match':>7} | {'Dash MTM%':>9} {'JSON MoDD%':>10} {'Match':>7} | {'Peak':>12} {'Flags':<10}")
print("-" * 140)
for i, r in enumerate(results, 1):
    json_all = r.get("json_all_pct", "?")
    json_m = r.get("json_month_pct", "?")
    at_m = r.get("at_match", "?")
    m_m = r.get("m_match", "?")
    peak = r.get("json_peak", "?")
    jstr = f"{json_all:.1f}%" if isinstance(json_all, float) else json_all
    mstr = f"{json_m:.1f}%" if isinstance(json_m, float) else json_m
    pstr = f"${peak:,.0f}" if isinstance(peak, (int, float)) and peak else str(peak)
    print(f"{i:<3} {r['prefix']:<12} {r['maxdd_pct']:>9.1f}% {jstr:>11} {at_m:>7} | {r['mtm_pct']:>8.1f}% {mstr:>10} {m_m:>7} | {pstr:>12} {r['flags']:<10}")
