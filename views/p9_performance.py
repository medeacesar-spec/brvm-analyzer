"""
Page 9 : Performance historique
Classement des titres et secteurs par performance sur différentes périodes.
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import plotly.graph_objects as go
import plotly.express as px

from config import load_tickers
from data.storage import get_cached_prices
from data.db import read_sql_df
from utils.charts import COLORS
from utils.nav import ticker_quick_picker
from utils.auth import is_admin


# ── Period definitions ──

PERIODS = {
    "3M": timedelta(days=90),
    "6M": timedelta(days=182),
    "1A": timedelta(days=365),
    "2A": timedelta(days=730),
    "3A": timedelta(days=1095),
    "Max": None,
}


def _compute_performance(df: pd.DataFrame, cutoff_date):
    """Return % change from cutoff_date to latest close."""
    if df.empty or "close" not in df.columns:
        return None
    df_sorted = df.sort_values("date")
    if cutoff_date is not None:
        df_period = df_sorted[df_sorted["date"] >= pd.Timestamp(cutoff_date)]
    else:
        df_period = df_sorted
    if len(df_period) < 2:
        return None
    start_price = df_period.iloc[0]["close"]
    end_price = df_period.iloc[-1]["close"]
    if not start_price or start_price == 0:
        return None
    return (end_price - start_price) / start_price


@st.cache_data(ttl=300, show_spinner=False)
def _load_all_performances_from_snapshot() -> pd.DataFrame:
    """Lit les performances précalculées depuis ticker_performance_snapshot.
    Retourne un DataFrame avec les mêmes colonnes que l'ancien _load_all_performances
    (3M, 6M, 1A, 2A, 3A, Max) pour compatibilité avec le reste de la page."""
    try:
        df = read_sql_df(
            "SELECT ticker, company_name AS name, sector, last_price AS price, "
            "perf_1m, perf_3m, perf_6m, perf_1a, perf_2a, perf_3a, perf_max "
            "FROM ticker_performance_snapshot"
        )
    except Exception:
        return pd.DataFrame()

    # Map vers les noms de colonnes attendus par la page (3M/6M/1A/2A/3A/Max)
    rename = {
        "perf_3m": "3M", "perf_6m": "6M", "perf_1a": "1A",
        "perf_2a": "2A", "perf_3a": "3A", "perf_max": "Max",
    }
    df = df.rename(columns=rename)
    return df


def _load_all_performances_live() -> pd.DataFrame:
    """Fallback : calcul live (lent) utilisé si le snapshot est vide."""
    tickers_data = load_tickers()
    today = datetime.now().date()
    rows = []

    for t in tickers_data:
        ticker = t["ticker"]
        name = t.get("name", ticker)
        sector = t.get("sector", "")
        df = get_cached_prices(ticker)
        if df.empty:
            continue

        last_price = df.sort_values("date").iloc[-1]["close"]
        row = {
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "price": last_price,
        }

        for label, delta in PERIODS.items():
            cutoff = (today - delta) if delta else None
            row[label] = _compute_performance(df, cutoff)

        rows.append(row)

    return pd.DataFrame(rows)


def _load_all_performances() -> tuple:
    """Retourne (df, from_snapshot: bool)."""
    df = _load_all_performances_from_snapshot()
    if df.empty:
        return _load_all_performances_live(), False
    return df, True


def _format_pct(val):
    if val is None or pd.isna(val):
        return "—"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.1%}"


def _color_pct(val):
    if val is None or pd.isna(val):
        return "color: #8F9BBA"
    return f"color: {COLORS['green']}" if val >= 0 else f"color: {COLORS['red']}"


def render():
    st.markdown(
        '<div class="main-header">📈 Performance des titres</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="sub-header">Titres et secteurs les plus/moins performants sur différentes périodes</div>',
        unsafe_allow_html=True,
    )

    # Load data — depuis snapshot si dispo, sinon fallback live (lent).
    perf_df, from_snapshot = _load_all_performances()

    if perf_df.empty:
        st.warning("Aucune donnée de prix disponible.")
        return

    if not from_snapshot and is_admin():
        st.warning(
            "⚠️ Snapshot de performance vide (calcul live en cours). Cliquez sur "
            "**📸 Regénérer snapshots** dans la sidebar pour accélérer cette page."
        )

    # ── Period selector ──
    period = st.radio(
        "Période",
        list(PERIODS.keys()),
        horizontal=True,
        index=2,  # default 1A
    )

    col_perf = period  # column name matches period key

    # Filter to tickers that have data for this period
    valid = perf_df.dropna(subset=[col_perf]).copy()
    if valid.empty:
        st.warning(f"Pas assez de données historiques pour la période {period}.")
        return

    valid_sorted = valid.sort_values(col_perf, ascending=False)

    # ── KPIs ──
    st.markdown("---")
    k1, k2, k3, k4 = st.columns(4)
    best = valid_sorted.iloc[0]
    worst = valid_sorted.iloc[-1]
    median_perf = valid[col_perf].median()
    positive_count = (valid[col_perf] >= 0).sum()

    k1.metric("Meilleure perf.", f"{best['name']}", f"{_format_pct(best[col_perf])}")
    k2.metric("Pire perf.", f"{worst['name']}", f"{_format_pct(worst[col_perf])}")
    k3.metric("Perf. médiane", _format_pct(median_perf))
    k4.metric("Titres en hausse", f"{positive_count}/{len(valid)}")

    # ── Tabs: Titres | Secteurs ──
    tab_stocks, tab_sectors, tab_chart = st.tabs(
        ["📊 Par Titre", "🏢 Par Secteur", "📈 Graphique"]
    )

    # ───────── TAB 1: By stock ─────────
    with tab_stocks:
        st.subheader(f"Classement des titres — {period}")

        n_show = st.slider("Nombre de titres", 5, len(valid_sorted), min(20, len(valid_sorted)))

        col_top, col_bottom = st.columns(2)

        with col_top:
            st.markdown("#### 🟢 Top performers")
            top = valid_sorted.head(n_show)
            _display_perf_table(top, col_perf)

        with col_bottom:
            st.markdown("#### 🔴 Pires performers")
            bottom = valid_sorted.tail(n_show).sort_values(col_perf)
            _display_perf_table(bottom, col_perf)

        # Bar chart all tickers
        st.markdown("---")
        st.subheader(f"Vue complète — {period}")
        fig = _bar_chart(valid_sorted, col_perf, f"Performance {period} — Tous les titres")
        st.plotly_chart(fig, use_container_width=True)

    # ───────── TAB 2: By sector ─────────
    with tab_sectors:
        st.subheader(f"Performance sectorielle — {period}")

        sector_perf = (
            valid.groupby("sector")[col_perf]
            .agg(["mean", "median", "count", "min", "max"])
            .rename(columns={
                "mean": "Perf. moyenne",
                "median": "Perf. médiane",
                "count": "Nb titres",
                "min": "Min",
                "max": "Max",
            })
            .sort_values("Perf. moyenne", ascending=False)
        )

        # Sector bar chart
        fig_sector = go.Figure()
        colors = [COLORS["green"] if v >= 0 else COLORS["red"] for v in sector_perf["Perf. moyenne"]]
        fig_sector.add_trace(go.Bar(
            x=sector_perf.index,
            y=sector_perf["Perf. moyenne"] * 100,
            marker_color=colors,
            text=[f"{v:+.1f}%" for v in sector_perf["Perf. moyenne"] * 100],
            textposition="outside",
        ))
        fig_sector.update_layout(
            title=f"Performance moyenne par secteur — {period}",
            yaxis_title="Performance (%)",
            xaxis_title="",
            plot_bgcolor=COLORS["bg"],
            paper_bgcolor=COLORS["bg"],
            font=dict(color=COLORS["text"]),
            height=400,
        )
        st.plotly_chart(fig_sector, use_container_width=True)

        # Sector detail table
        display_sector = sector_perf.copy()
        for c in ["Perf. moyenne", "Perf. médiane", "Min", "Max"]:
            display_sector[c] = display_sector[c].apply(_format_pct)
        display_sector["Nb titres"] = display_sector["Nb titres"].astype(int)
        st.dataframe(display_sector, use_container_width=True)

        # Drill-down by sector
        st.markdown("---")
        selected_sector = st.selectbox(
            "Détail par secteur",
            sorted(valid["sector"].unique()),
        )
        sector_stocks = valid_sorted[valid_sorted["sector"] == selected_sector]
        if not sector_stocks.empty:
            fig_drill = _bar_chart(
                sector_stocks, col_perf,
                f"Performance {period} — {selected_sector}",
            )
            st.plotly_chart(fig_drill, use_container_width=True)
        else:
            st.info(f"Aucun titre avec données dans le secteur {selected_sector}.")

    # ───────── TAB 3: Price chart ─────────
    with tab_chart:
        st.subheader("Évolution comparée des prix (base 100)")

        # Let user pick tickers to compare
        options = [f"{r['ticker']} - {r['name']}" for _, r in valid_sorted.iterrows()]
        # Default: top 3 + bottom 1
        defaults = options[:3] + options[-1:] if len(options) >= 4 else options[:3]
        selected = st.multiselect(
            "Choisir des titres à comparer",
            options,
            default=defaults,
            max_selections=10,
        )

        if selected:
            sel_tickers = [s.split(" - ")[0] for s in selected]
            today = datetime.now().date()
            delta = PERIODS[period]
            cutoff = pd.Timestamp(today - delta) if delta else None

            fig_line = go.Figure()
            for ticker in sel_tickers:
                df = get_cached_prices(ticker)
                if df.empty:
                    continue
                df = df.sort_values("date")
                if cutoff is not None:
                    df = df[df["date"] >= cutoff]
                if len(df) < 2:
                    continue
                base = df.iloc[0]["close"]
                if not base or base == 0:
                    continue
                df["indexed"] = (df["close"] / base) * 100
                name = next(
                    (t["name"] for t in load_tickers() if t["ticker"] == ticker),
                    ticker,
                )
                fig_line.add_trace(go.Scatter(
                    x=df["date"], y=df["indexed"],
                    mode="lines", name=name,
                    hovertemplate="%{x|%d %b %Y}<br>%{y:.1f}<extra></extra>",
                ))

            fig_line.add_hline(
                y=100, line_dash="dash",
                line_color=COLORS["text_secondary"],
                annotation_text="Base 100",
            )
            fig_line.update_layout(
                title=f"Performance comparée — {period} (base 100)",
                yaxis_title="Indice (base 100)",
                xaxis_title="",
                plot_bgcolor=COLORS["bg"],
                paper_bgcolor=COLORS["bg"],
                font=dict(color=COLORS["text"]),
                legend=dict(orientation="h", yanchor="bottom", y=-0.25),
                height=500,
                hovermode="x unified",
            )
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("Sélectionnez des titres pour afficher le graphique comparé.")

    # ── Multi-period summary table ──
    st.markdown("---")
    st.subheader("📋 Tableau récapitulatif multi-périodes")

    summary = perf_df[["ticker", "name", "sector", "price"] + list(PERIODS.keys())].copy()
    summary = summary.sort_values(period, ascending=False, na_position="last")
    summary["price_fmt"] = summary["price"].apply(
        lambda x: f"{x:,.0f}" if x and not pd.isna(x) else "—"
    )

    display_cols = {"ticker": "Ticker", "name": "Nom", "sector": "Secteur", "price_fmt": "Prix"}
    for p in PERIODS:
        col_name = f"perf_{p}"
        summary[col_name] = summary[p].apply(_format_pct)
        display_cols[col_name] = p

    st.dataframe(
        summary[list(display_cols.keys())].rename(columns=display_cols),
        use_container_width=True,
        hide_index=True,
        height=600,
    )

    # Quick jump to stock analysis
    picker_options = [
        (row["ticker"], f"{row['ticker']} — {row['name']}")
        for _, row in summary.iterrows()
    ]
    ticker_quick_picker(picker_options, key="perf_goto", label="🔍 Ouvrir l'analyse d'un titre")


def _display_perf_table(df: pd.DataFrame, col_perf: str):
    """Display a compact performance table."""
    display = df[["ticker", "name", col_perf]].copy()
    display["Perf."] = display[col_perf].apply(_format_pct)
    display["Prix"] = df["price"].apply(lambda x: f"{x:,.0f}" if x and not pd.isna(x) else "—")

    for _, row in display.iterrows():
        val = row[col_perf]
        if val is None or pd.isna(val):
            icon = "⬜"
        elif val >= 0.2:
            icon = "🚀"
        elif val >= 0:
            icon = "🟢"
        elif val >= -0.2:
            icon = "🔴"
        else:
            icon = "💥"

        c1, c2, c3 = st.columns([3, 1.5, 1.5])
        c1.write(f"{icon} **{row['name']}** ({row['ticker']})")
        c2.write(row["Prix"])
        c3.write(row["Perf."])


def _bar_chart(df: pd.DataFrame, col_perf: str, title: str) -> go.Figure:
    """Horizontal bar chart of performances."""
    df_plot = df.sort_values(col_perf, ascending=True).copy()
    colors = [
        COLORS["green"] if v >= 0 else COLORS["red"]
        for v in df_plot[col_perf]
    ]

    fig = go.Figure(go.Bar(
        x=df_plot[col_perf] * 100,
        y=df_plot["name"],
        orientation="h",
        marker_color=colors,
        text=[f"{v:+.1f}%" for v in df_plot[col_perf] * 100],
        textposition="outside",
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Performance (%)",
        yaxis_title="",
        plot_bgcolor=COLORS["bg"],
        paper_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=max(400, len(df_plot) * 28),
        margin=dict(l=200),
    )
    return fig
