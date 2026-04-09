import pandas as pd
import numpy as np
import streamlit as st
from pathlib import Path

from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide", page_title="Hyperliquid Copy Decision Engine")

DATA_PATH = Path("hyperliquid_live_output/live_wallet_metrics.csv")
CURVE_PATH = Path("hyperliquid_live_output/equity_curves")

# auto refresh every 5s
st_autorefresh(interval=5000, key="refresh")

st.title("Hyperliquid Copy Decision Engine (LIVE)")

# =========================
# LOAD DATA
# =========================
if not DATA_PATH.exists():
    st.warning("Waiting for live_wallet_metrics.csv...")
    st.stop()

try:
    df = pd.read_csv(DATA_PATH)
    numeric_cols = df.columns.drop("wallet")
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
except Exception as e:
    st.error(f"Failed to load metrics: {e}")
    st.stop()

# =========================
# DERIVED COLUMNS (computed on full df before filtering)
# =========================
df["win_rate_pct"] = df.apply(
    lambda r: (r["wins"] / max(r["trade_count"], 1)) * 100, axis=1
)

# active_hours: derive from last_fill_ts_ms span; fall back to 1 to avoid div/0
df["active_hours"] = df.apply(
    lambda r: max((r["last_fill_ts_ms"] - r.get("first_fill_ts_ms", r["last_fill_ts_ms"])) / 3_600_000, 1)
    if "first_fill_ts_ms" in df.columns else 1.0,
    axis=1,
)

df["pnl_per_trade"] = df.apply(
    lambda r: r["total_pnl"] / max(r["trade_count"], 1), axis=1
)
df["pnl_per_hour"] = df.apply(
    lambda r: r["total_pnl"] / max(r["active_hours"], 1), axis=1
)

# PF per wallet: gross_profit / gross_loss proxy using wins/losses ratio
df["PF"] = df.apply(
    lambda r: round(
        (r["wins"] * r["pnl_per_trade"]) / max(abs(r["losses"] * r["pnl_per_trade"]), 1e-9), 4
    ) if r["trade_count"] > 0 else 0.0,
    axis=1,
)

# consistency: win_rate as fraction (0–1)
df["consistency"] = df["win_rate_pct"] / 100.0

# raw copy score (before normalisation)
df["_copy_score_raw"] = df.apply(
    lambda r: (
        r["realized_pnl"] * r["win_rate_pct"] / 100.0 * r["consistency"]
    ) / (1.0 + (r["avg_latency_ms"] / 1000.0)),
    axis=1,
)

# normalise to 0–100
_cs_min = df["_copy_score_raw"].min()
_cs_max = df["_copy_score_raw"].max()
_cs_range = _cs_max - _cs_min if _cs_max != _cs_min else 1.0
df["copy_score"] = ((df["_copy_score_raw"] - _cs_min) / _cs_range * 100).round(2)

# copyability status
def _copy_status(lat):
    if lat < 200:
        return "COPYABLE"
    if lat < 1000:
        return "RISKY"
    return "DO NOT COPY"

df["copy_status"] = df["avg_latency_ms"].apply(_copy_status)

# tag helpers
top10_wallets = set(df.sort_values("realistic_pnl", ascending=False).head(10)["wallet"].tolist())
alpha_threshold = df["total_pnl"].quantile(0.8) if len(df) >= 5 else 0.0
alpha_wallets = set(
    df[(df["total_pnl"] >= alpha_threshold) & (~df["wallet"].isin(top10_wallets))]["wallet"].tolist()
)

def _tag(w):
    if w in top10_wallets:
        return "LIVE"
    if w in alpha_wallets:
        return "ALPHA"
    return ""

# =========================
# SIDEBAR FILTERS
# =========================
st.sidebar.header("Filters")

min_trades   = st.sidebar.number_input("Min trades", min_value=0, value=0, step=1)
min_pnl      = st.sidebar.number_input("Min total_pnl", value=0.0, step=0.01)
max_latency  = st.sidebar.number_input("Max avg_latency_ms (0 = no limit)", min_value=0.0, value=0.0, step=1.0)
only_profitable = st.sidebar.checkbox("Only profitable wallets", value=False)

sort_option = st.sidebar.selectbox(
    "Sort leaderboard by",
    ["copy_score", "total_pnl", "realistic_pnl", "trade_count", "wins", "efficiency", "pnl_per_trade"],
    key="sortbox",
)

normalize_equity = st.sidebar.checkbox("Normalize Equity Curve (start at 1)", value=False)

df_filtered = df.copy()
df_filtered = df_filtered[df_filtered["trade_count"] >= min_trades]
df_filtered = df_filtered[df_filtered["total_pnl"] >= min_pnl]
if max_latency > 0:
    df_filtered = df_filtered[df_filtered["avg_latency_ms"] <= max_latency]
if only_profitable:
    df_filtered = df_filtered[df_filtered["total_pnl"] > 0]

df_sorted = df_filtered.sort_values(sort_option, ascending=False)

# =========================
# WALLET TOGGLE (session_state, inline with leaderboard)
# =========================
if "selected_wallets" not in st.session_state:
    st.session_state.selected_wallets = {w: True for w in df_filtered["wallet"].tolist()}

for w in df_filtered["wallet"].tolist():
    if w not in st.session_state.selected_wallets:
        st.session_state.selected_wallets[w] = True

current_wallets = set(df_filtered["wallet"].tolist())
st.session_state.selected_wallets = {
    w: v for w, v in st.session_state.selected_wallets.items() if w in current_wallets
}

selected_mask = df_sorted["wallet"].map(lambda w: st.session_state.selected_wallets.get(w, True))
df_selected = df_sorted[selected_mask]

# =========================
# COMBINED EQUITY CURVE
# =========================
def load_combined_equity(wallets, normalise=False):
    frames = []
    for w in wallets:
        f = CURVE_PATH / f"{w}.csv"
        if not f.exists():
            continue
        try:
            cdf = pd.read_csv(f)
            if "ts" not in cdf.columns or "equity" not in cdf.columns:
                continue
            if "profile" in cdf.columns:
                cdf = cdf[cdf["profile"] == "realistic"]
            cdf = cdf[["ts", "equity"]].copy()
            cdf["ts"]     = pd.to_numeric(cdf["ts"],     errors="coerce")
            cdf["equity"] = pd.to_numeric(cdf["equity"], errors="coerce")
            cdf = cdf.dropna()
            if normalise and len(cdf) > 0:
                first = cdf["equity"].iloc[0]
                if first != 0:
                    cdf["equity"] = cdf["equity"] / first
            frames.append(cdf)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames)
    combined = combined.groupby("ts")["equity"].sum().reset_index().sort_values("ts")
    combined["peak"]     = combined["equity"].cummax()
    combined["drawdown"] = combined["equity"] - combined["peak"]
    return combined

selected_wallet_list = df_selected["wallet"].tolist()
combined_eq = load_combined_equity(selected_wallet_list, normalise=normalize_equity)

# =========================
# MAX DRAWDOWN
# =========================
max_dd = float(combined_eq["drawdown"].min()) if not combined_eq.empty else 0.0

# =========================
# TOP METRICS PANEL
# =========================
st.subheader("Portfolio Metrics (Selected Wallets)")

total_copy_pnl  = float(df_selected["total_pnl"].sum())  if not df_selected.empty else 0.0
pct_profitable  = float((df_selected["total_pnl"] > 0).mean() * 100) if not df_selected.empty else 0.0
median_pnl      = float(df_selected["total_pnl"].median()) if not df_selected.empty else 0.0
gross_profit    = float(df_selected[df_selected["total_pnl"] > 0]["total_pnl"].sum())
gross_loss      = float(df_selected[df_selected["total_pnl"] < 0]["total_pnl"].abs().sum())
profit_factor   = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")
risk_score_port = total_copy_pnl / (abs(max_dd) + 1)

# contribution analysis
n_wallets = len(df_selected)
top5_pnl  = float(df_selected.nlargest(min(5,  n_wallets), "total_pnl")["total_pnl"].sum())
top10_pnl = float(df_selected.nlargest(min(10, n_wallets), "total_pnl")["total_pnl"].sum())
total_abs_pnl = float(df_selected["total_pnl"].abs().sum()) if not df_selected.empty else 1.0
pct_top5  = (top5_pnl  / total_abs_pnl * 100) if total_abs_pnl != 0 else 0.0
pct_top10 = (top10_pnl / total_abs_pnl * 100) if total_abs_pnl != 0 else 0.0

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Total Copy PnL",   f"${total_copy_pnl:,.2f}")
m2.metric("Max Drawdown",     f"${max_dd:,.2f}")
m3.metric("% Profitable",     f"{pct_profitable:.1f}%")
m4.metric("Median PnL",       f"${median_pnl:,.4f}")
m5.metric("Profit Factor",    f"{profit_factor:.2f}" if profit_factor != float("inf") else "∞")
m6.metric("Risk Score",       f"{risk_score_port:.4f}")

c1, c2 = st.columns(2)
c1.metric("% PnL from Top 5",  f"{pct_top5:.1f}%")
c2.metric("% PnL from Top 10", f"{pct_top10:.1f}%")

st.divider()

# =========================
# SUGGESTED COPY SET
# =========================
st.subheader("Suggested Copy Set  (copy_score > 70)")

suggested = df_filtered[df_filtered["copy_score"] > 70].sort_values("copy_score", ascending=False)

if not suggested.empty:
    sug_display = suggested[["wallet", "copy_score", "copy_status", "total_pnl", "avg_latency_ms"]].copy()
    sug_display["total_pnl"] = sug_display["total_pnl"].apply(lambda v: f"${v:,.4f}")
    sug_display.insert(0, "tag", sug_display["wallet"].map(_tag))
    st.dataframe(sug_display, use_container_width=True)
else:
    st.info("No wallets currently above copy_score threshold of 70.")

st.divider()

# =========================
# COMBINED EQUITY CHART
# =========================
st.subheader("Combined Equity Curve (Selected Wallets)")

if not combined_eq.empty:
    chart_df = combined_eq.set_index("ts")[["equity", "peak"]]
    st.line_chart(chart_df)
else:
    st.info("No equity curve data available for selected wallets.")

st.divider()

# =========================
# ACTIVE COPY SET  (selected wallets)
# =========================
st.subheader("Active Copy Set")

if not df_selected.empty:
    copy_display = df_selected[[
        "wallet", "total_pnl", "trade_count", "win_rate_pct", "PF", "copy_score", "copy_status"
    ]].copy()
    copy_display["win_rate"] = copy_display["win_rate_pct"].apply(lambda v: f"{v:.1f}%")
    copy_display = copy_display.drop(columns=["win_rate_pct"])
    copy_display["total_pnl"] = copy_display["total_pnl"].apply(lambda v: f"${v:,.4f}")
    copy_display.insert(0, "tag", copy_display["wallet"].map(_tag))
    st.dataframe(copy_display, use_container_width=True)
else:
    st.info("No wallets selected.")

st.divider()

# =========================
# LEADERBOARD  (with inline include toggle)
# =========================
st.subheader("Leaderboard")

if not df_sorted.empty:
    lb = df_sorted.copy()

    # build display frame
    lb_display = lb[[
        "wallet", "copy_score", "copy_status", "total_pnl",
        "pnl_per_trade", "pnl_per_hour", "PF", "win_rate_pct", "avg_latency_ms",
    ]].copy()

    lb_display.rename(columns={"win_rate_pct": "win_rate"}, inplace=True)
    lb_display["total_pnl"]     = lb_display["total_pnl"].apply(lambda v: f"${v:,.4f}")
    lb_display["pnl_per_trade"] = lb_display["pnl_per_trade"].apply(lambda v: f"${v:,.6f}")
    lb_display["pnl_per_hour"]  = lb_display["pnl_per_hour"].apply(lambda v: f"${v:,.6f}")
    lb_display["win_rate"]      = lb_display["win_rate"].apply(lambda v: f"{v:.1f}%")
    lb_display.insert(0, "tag", lb_display["wallet"].map(_tag))
    lb_display.insert(1, "include", lb_display["wallet"].map(
        lambda w: st.session_state.selected_wallets.get(w, True)
    ))

    # render with st.data_editor so the include column is a checkbox
    edited = st.data_editor(
        lb_display,
        use_container_width=True,
        height=520,
        column_config={
            "include": st.column_config.CheckboxColumn("Include", default=True),
            "tag":     st.column_config.TextColumn("Tag"),
        },
        disabled=[c for c in lb_display.columns if c != "include"],
        key="leaderboard_editor",
    )

    # sync checkbox state back into session_state
    for _, row in edited.iterrows():
        w = row["wallet"]
        if w in st.session_state.selected_wallets:
            st.session_state.selected_wallets[w] = bool(row["include"])
else:
    st.info("No data after filters.")

st.divider()

# =========================
# WALLET DETAIL
# =========================
st.subheader("Wallet Detail")

if not df_sorted.empty:
    selected_wallet = st.selectbox(
        "Select Wallet",
        df_sorted["wallet"].head(50),
        key="walletbox",
    )

    row = df[df["wallet"] == selected_wallet]
    if not row.empty:
        row = row.iloc[0]

        tag_label = ""
        if selected_wallet in top10_wallets:
            tag_label = " [LIVE]"
        elif selected_wallet in alpha_wallets:
            tag_label = " [ALPHA]"

        st.markdown(f"**{selected_wallet}**{tag_label}")

        colA, colB, colC, colD = st.columns(4)
        colA.metric("Realistic PnL", f"${row['realistic_pnl']:,.4f}")
        colB.metric("Total PnL",     f"${row['total_pnl']:,.4f}")
        colC.metric("Trades",        int(row["trade_count"]))
        colD.metric("Win Rate",      f"{(row['wins'] / max(row['trade_count'], 1)) * 100:.1f}%")

        colE, colF = st.columns(2)
        colE.metric("Copy Score",   f"{row['copy_score']:.2f}")
        colF.metric("Copy Status",  row["copy_status"])

        st.subheader("Individual Equity Curve")
        curve_file = CURVE_PATH / f"{selected_wallet}.csv"
        if curve_file.exists():
            try:
                curve_df = pd.read_csv(curve_file)
                if "profile" in curve_df.columns:
                    curve_df = curve_df[curve_df["profile"] == "realistic"]
                if "equity" in curve_df.columns:
                    curve_df["equity"] = pd.to_numeric(curve_df["equity"], errors="coerce")
                    if normalize_equity and len(curve_df) > 0:
                        first_val = curve_df["equity"].iloc[0]
                        if first_val != 0:
                            curve_df["equity"] = curve_df["equity"] / first_val
                    curve_df["peak"] = curve_df["equity"].cummax()
                    if "ts" in curve_df.columns:
                        curve_df = curve_df.set_index("ts")
                    st.line_chart(curve_df[["equity", "peak"]])
                else:
                    st.warning("No equity column found")
            except Exception as ex:
                st.warning(f"Could not load curve: {ex}")
        else:
            st.info("No curve file yet for this wallet.")

# =========================
# HEALTH CHECK
# =========================
st.divider()
st.subheader("Top 10 Health")

top10_health = df.sort_values("realistic_pnl", ascending=False).head(10)
active = int((top10_health["last_fill_ts_ms"] > 0).sum())
dead   = 10 - active

st.metric("Top 10 Active", f"{active}/10")
if dead > 3:
    st.error("Top 10 unreliable (too many inactive)")
