"""
Turbulence / Fragility Dashboard
==================================
Run with:
    streamlit run turbulence/dashboard.py

Requires ANTHROPIC_API_KEY in environment for AI summaries (optional).
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

# Ensure project root is on path when running from subdirectory
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st

from turbulence.engine import (
    run,
    run_demo,
    latest_snapshot,
    REGIME_COLORS,
    COMPOSITE_WEIGHTS,
    ALL_TICKERS,
)
from turbulence.ai_summary import generate_summary

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Market Turbulence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
st.sidebar.title("⚙️ Controls")
start_date    = st.sidebar.date_input("Start date", value=pd.Timestamp("2018-01-01"))
use_cache     = st.sidebar.checkbox("Use data cache", value=True)
refresh_btn   = st.sidebar.button("🔄 Refresh data")
demo_mode     = st.sidebar.checkbox("Demo mode (synthetic data)", value=False,
                                    help="Use generated data when Yahoo Finance is unavailable")
show_ai       = st.sidebar.checkbox("Generate AI regime summary", value=False)
api_key_input = st.sidebar.text_input("Anthropic API key (optional)", type="password")

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Regime thresholds**\n"
    "- 🟢 GREEN  < 1.0\n"
    "- 🟡 YELLOW 1.0–2.0\n"
    "- 🟠 ORANGE 2.0–3.0\n"
    "- 🔴 RED   ≥ 3.0"
)
st.sidebar.markdown("---")
st.sidebar.caption(
    "Data: Yahoo Finance. "
    "Model: Mahalanobis turbulence + contagion signals. "
    "Not investment advice."
)

# ---------------------------------------------------------------------------
# Data load (cached)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Computing turbulence signals…")
def load_features(start: str, _bust: bool = False, _demo: bool = False) -> pd.DataFrame:
    if _demo:
        return run_demo()
    return run(start=start, use_cache=(not _bust))


features = load_features(str(start_date), _bust=refresh_btn, _demo=demo_mode)
snap     = latest_snapshot(features)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
regime      = snap.get("regime", "UNKNOWN")
composite   = snap.get("composite", 0.0)
regime_col  = REGIME_COLORS.get(regime, "#94a3b8")

col_title, col_gauge = st.columns([3, 1])
with col_title:
    st.title("📊 Market Turbulence & Fragility Dashboard")
    st.caption(f"Last update: **{snap['date']}**")

with col_gauge:
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=composite,
        number={"suffix": "", "font": {"size": 28}},
        gauge={
            "axis": {"range": [0, 4], "tickwidth": 1},
            "bar":  {"color": regime_col},
            "steps": [
                {"range": [0,   1], "color": "#dcfce7"},
                {"range": [1,   2], "color": "#fef9c3"},
                {"range": [2,   3], "color": "#ffedd5"},
                {"range": [3,   4], "color": "#fee2e2"},
            ],
            "threshold": {
                "line": {"color": regime_col, "width": 4},
                "thickness": 0.75,
                "value": composite,
            },
        },
        title={"text": f"Composite  |  <b>{regime}</b>", "font": {"size": 16}},
    ))
    fig_gauge.update_layout(height=200, margin=dict(l=10, r=10, t=30, b=0))
    st.plotly_chart(fig_gauge, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Tab layout
# ---------------------------------------------------------------------------
tab_main, tab_subs, tab_corr, tab_crisis, tab_ai = st.tabs([
    "📈 Composite Score",
    "🧩 Sub-factors",
    "🔗 Correlation Breaks",
    "⚠️ Crisis Analogs",
    "🤖 AI Memo",
])

# ── Tab 1: Composite score time-series ──────────────────────────────────────
with tab_main:
    st.subheader("Composite Fragility Score — Full History")
    feat_clean = features.dropna(subset=["composite"])

    # Colour background bands by regime
    fig = go.Figure()

    # Band fills
    band_defs = [
        (0, 1,   "#dcfce7", "Normal"),
        (1, 2,   "#fef9c3", "Caution"),
        (2, 3,   "#ffedd5", "Fragility"),
        (3, 4.5, "#fee2e2", "Turbulence"),
    ]
    for lo, hi, col, lbl in band_defs:
        fig.add_hrect(y0=lo, y1=hi, fillcolor=col, opacity=0.4, layer="below",
                      line_width=0, annotation_text=lbl,
                      annotation_position="left", annotation_font_size=10)

    # Composite line
    fig.add_trace(go.Scatter(
        x=feat_clean.index, y=feat_clean["composite"],
        mode="lines", name="Composite",
        line=dict(width=2, color="#1d4ed8"),
        hovertemplate="%{x|%Y-%m-%d}<br>Score: %{y:.2f}<extra></extra>",
    ))

    # Colour the line by regime
    for regime_val, col in REGIME_COLORS.items():
        if regime_val == "UNKNOWN":
            continue
        mask = feat_clean["regime"] == regime_val
        if mask.any():
            fig.add_trace(go.Scatter(
                x=feat_clean.index[mask],
                y=feat_clean["composite"][mask],
                mode="markers", name=regime_val,
                marker=dict(size=3, color=col),
                showlegend=False,
                hoverinfo="skip",
            ))

    fig.update_layout(
        height=380, yaxis_title="Composite z-score",
        xaxis_title="", legend_orientation="h",
        margin=dict(l=0, r=0, t=20, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Recent 30-day table
    st.subheader("Recent 30 Trading Days")
    cols_show = ["composite", "regime", "turbulence_z", "credit_stress",
                 "financial_rel", "software_rel", "corr_break"]
    cols_show = [c for c in cols_show if c in features.columns]
    recent_df = (
        features[cols_show]
        .dropna(subset=["composite"])
        .tail(30)
        .sort_index(ascending=False)
        .round(3)
    )

    def color_regime(val):
        col = REGIME_COLORS.get(val, "")
        return f"background-color: {col}20; color: {col}; font-weight: bold"

    st.dataframe(
        recent_df.style.applymap(color_regime, subset=["regime"]),
        use_container_width=True,
        height=400,
    )


# ── Tab 2: Sub-factor heatmap ────────────────────────────────────────────────
with tab_subs:
    st.subheader("Sub-factor Contributions — Trailing 60 Days")

    sub_cols = [
        "turbulence_z", "credit_stress", "financial_rel",
        "software_rel", "alt_manager_rel", "btc_rel", "corr_break",
    ]
    sub_cols = [c for c in sub_cols if c in features.columns]
    sub_df   = features[sub_cols].dropna(how="all").tail(60)

    fig_heat = px.imshow(
        sub_df.T,
        color_continuous_scale="RdYlGn_r",
        zmin=-3, zmax=3,
        aspect="auto",
        labels=dict(x="Date", y="Signal", color="z-score"),
    )
    fig_heat.update_layout(height=350, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig_heat, use_container_width=True)

    # Bar chart: today's sub-factor values
    st.subheader("Today's Sub-factor Snapshot")
    today_vals = {c: snap.get(c, np.nan) for c in sub_cols}
    today_df   = (
        pd.Series(today_vals, name="z-score")
        .dropna()
        .sort_values(ascending=False)
    )

    colors = [
        "#ef4444" if v >= 2 else "#f97316" if v >= 1 else "#22c55e" if v <= -1 else "#94a3b8"
        for v in today_df
    ]
    fig_bar = go.Figure(go.Bar(
        x=today_df.index, y=today_df.values,
        marker_color=colors,
        text=today_df.round(2).values,
        textposition="outside",
    ))
    fig_bar.add_hline(y=1, line_dash="dot", line_color="#eab308")
    fig_bar.add_hline(y=2, line_dash="dot", line_color="#f97316")
    fig_bar.update_layout(
        height=300, yaxis_title="z-score",
        margin=dict(l=0, r=0, t=20, b=0),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # Weight table
    st.caption("Composite weights")
    w_df = pd.DataFrame.from_dict(
        COMPOSITE_WEIGHTS, orient="index", columns=["weight"]
    )
    w_df["weight"] = w_df["weight"].map("{:.0%}".format)
    st.dataframe(w_df, use_container_width=False)


# ── Tab 3: Correlation breaks ────────────────────────────────────────────────
with tab_corr:
    st.subheader("Pairwise Rolling Correlations (20-day) vs 126-day baseline")

    rets = None
    try:
        from turbulence.engine import fetch_prices
        prices = fetch_prices(ALL_TICKERS, start=str(start_date), use_cache=use_cache)
        rets   = prices.pct_change().dropna()
    except Exception as e:
        st.warning(f"Could not reload prices: {e}")

    if rets is not None:
        from turbulence.engine import CORR_PAIRS, rolling_zscore

        fig_corr = make_subplots(
            rows=len(CORR_PAIRS), cols=1,
            shared_xaxes=True,
            subplot_titles=[f"{a} / {b}" for a, b in CORR_PAIRS],
            vertical_spacing=0.06,
        )
        for i, (a, b) in enumerate(CORR_PAIRS, start=1):
            if a not in rets or b not in rets:
                continue
            roll = rets[a].rolling(20).corr(rets[b])
            base = roll.rolling(126).mean()
            fig_corr.add_trace(
                go.Scatter(x=roll.index, y=roll, mode="lines", name=f"{a}/{b} 20d",
                           line=dict(width=1.5)),
                row=i, col=1,
            )
            fig_corr.add_trace(
                go.Scatter(x=base.index, y=base, mode="lines",
                           name=f"{a}/{b} 126d baseline",
                           line=dict(width=1, dash="dot", color="#94a3b8")),
                row=i, col=1,
            )

        fig_corr.update_layout(
            height=140 * len(CORR_PAIRS),
            showlegend=False,
            margin=dict(l=0, r=0, t=30, b=0),
        )
        st.plotly_chart(fig_corr, use_container_width=True)


# ── Tab 4: Crisis analogs ────────────────────────────────────────────────────
with tab_crisis:
    st.subheader("Crisis Period Analogs")

    crisis_windows = {
        "COVID crash (Feb–Apr 2020)":            ("2020-01-01", "2020-06-30"),
        "Fed tightening / growth unwind 2022":   ("2021-11-01", "2022-12-31"),
        "Regional bank stress 2023":             ("2023-02-01", "2023-06-30"),
        "AI disruption (late 2025 – 2026)":      ("2025-07-01", str(pd.Timestamp.today().date())),
    }

    feat_clean2 = features.dropna(subset=["composite"])

    fig_analogs = go.Figure()
    palette = px.colors.qualitative.Set2

    for idx, (label, (lo, hi)) in enumerate(crisis_windows.items()):
        window = feat_clean2.loc[lo:hi, "composite"]
        if window.empty:
            continue
        normed_x = np.arange(len(window))
        fig_analogs.add_trace(go.Scatter(
            x=normed_x, y=window.values,
            mode="lines", name=label,
            line=dict(color=palette[idx % len(palette)], width=2),
            hovertemplate=f"{label}<br>Day %{{x}}<br>Score: %{{y:.2f}}<extra></extra>",
        ))

    fig_analogs.update_layout(
        height=360,
        xaxis_title="Trading days into episode",
        yaxis_title="Composite score",
        margin=dict(l=0, r=0, t=20, b=0),
    )
    st.plotly_chart(fig_analogs, use_container_width=True)

    # Summary stats per window
    rows = []
    for label, (lo, hi) in crisis_windows.items():
        w = feat_clean2.loc[lo:hi, "composite"]
        if w.empty:
            continue
        rows.append({
            "Episode":       label,
            "Peak score":    round(w.max(), 2),
            "Days at RED":   int((w >= 3).sum()),
            "Days at ORANGE":int(((w >= 2) & (w < 3)).sum()),
            "Avg score":     round(w.mean(), 2),
        })
    if rows:
        st.dataframe(pd.DataFrame(rows).set_index("Episode"), use_container_width=True)


# ── Tab 5: AI memo ──────────────────────────────────────────────────────────
with tab_ai:
    st.subheader("🤖 AI-Generated Daily Regime Memo")
    st.caption(
        "Powered by Claude. Requires ANTHROPIC_API_KEY. "
        "Enable in sidebar and click the button below."
    )

    if show_ai or st.button("Generate memo now"):
        key = api_key_input or os.environ.get("ANTHROPIC_API_KEY", "")
        with st.spinner("Asking Claude for a regime summary…"):
            memo = generate_summary(features, snap, api_key=key or None)
        st.markdown(memo)

        # Show the feature values passed to the model
        with st.expander("Signal values used"):
            sub_cols2 = [
                "turbulence_z", "credit_stress", "financial_rel",
                "software_rel", "alt_manager_rel", "btc_rel", "corr_break",
            ]
            vals = {k: round(snap.get(k, float("nan")), 3) for k in sub_cols2}
            st.json(vals)
    else:
        st.info("Enable 'Generate AI regime summary' in the sidebar or click the button above.")
