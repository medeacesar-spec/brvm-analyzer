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
    from utils.ui_helpers import section_heading
    st.title("Performance des Titres")

    perf_df, from_snapshot = _load_all_performances()
    if perf_df.empty:
        st.warning("Aucune donnée de prix disponible.")
        return

    # ─── Ligne titre : caption gauche + period pills droite ───
    col_sub, col_period = st.columns([3, 2])

    with col_period:
        period = st.radio(
            "Période", list(PERIODS.keys()),
            horizontal=True, index=2, label_visibility="collapsed",
        )

    col_perf = period
    valid = perf_df.dropna(subset=[col_perf]).copy()
    if valid.empty:
        st.warning(f"Pas assez de données historiques pour la période {period}.")
        return
    valid_sorted = valid.sort_values(col_perf, ascending=False)

    with col_sub:
        st.caption(f"Classement sur {period.lower()} · {len(valid)} titres")

    if not from_snapshot and is_admin():
        st.caption("⚠️ Snapshot vide — clic Regénérer snapshots admin pour accélérer.")

    # ─── 4 KPI cards ──────────────────────────────────────────────────
    best = valid_sorted.iloc[0]
    worst = valid_sorted.iloc[-1]
    median_perf = valid[col_perf].median()
    positive_count = (valid[col_perf] >= 0).sum()
    total = len(valid)
    pct_market = positive_count / total * 100 if total else 0

    def _kpi(label, value, sub, arrow_tone="neutral"):
        arrow = {"up": "▲", "down": "▼"}.get(arrow_tone, "")
        sub_color = {"up": "var(--up)", "down": "var(--down)"}.get(
            arrow_tone, "var(--ink-3)"
        )
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;min-height:92px;'>"
            f"<div class='label-xs' style='margin-bottom:6px;'>{label}</div>"
            f"<div style='font-size:22px;font-weight:600;letter-spacing:-0.02em;"
            f"color:var(--ink);line-height:1.15;'>{value}</div>"
            f"<div style='font-size:11.5px;color:{sub_color};margin-top:6px;"
            f"font-weight:500;'>{arrow + ' ' if arrow else ''}{sub}</div>"
            f"</div>"
        )

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(_kpi("Meilleure perf.", best["name"],
                          _format_pct(best[col_perf]), "up"),
                     unsafe_allow_html=True)
    with k2:
        st.markdown(_kpi("Pire perf.", worst["name"],
                          _format_pct(worst[col_perf]), "down"),
                     unsafe_allow_html=True)
    with k3:
        st.markdown(_kpi("Perf. médiane", _format_pct(median_perf),
                          f"{total} titres", "neutral"),
                     unsafe_allow_html=True)
    with k4:
        st.markdown(_kpi("Titres en hausse", f"{positive_count} / {total}",
                          f"{pct_market:.0f}% du marché",
                          "up" if pct_market >= 50 else "down"),
                     unsafe_allow_html=True)

    # ─── Tabs ─────────────────────────────────────────────────────────
    tab_stocks, tab_sectors, tab_chart, tab_multi = st.tabs(
        ["Classement", "Par secteur", "Graphique", "Tableau multi-périodes"]
    )

    # ───────── TAB 1 : Classement avec bars horizontaux Top / Pires ──
    with tab_stocks:
        n_show = st.slider("Nombre de titres affichés", 5, len(valid_sorted),
                           min(8, len(valid_sorted)), key="p9_n_show",
                           label_visibility="collapsed")

        col_top, col_bot = st.columns(2)

        def _hbar_list(df_subset, tone_color, title, dot_class):
            """Liste bar horizontaux: nom + bar propotionnel + % à droite."""
            max_abs = max(abs(v) for v in df_subset[col_perf]) if not df_subset.empty else 1
            max_abs = max_abs or 1
            inner = ""
            for _, r in df_subset.iterrows():
                v = r[col_perf]
                w = abs(v) / max_abs * 100
                sign = "+" if v >= 0 else ""
                inner += (
                    f"<div style='display:flex;align-items:center;gap:10px;"
                    f"padding:7px 0;border-bottom:1px solid var(--border);font-size:13px;'>"
                    f"<div style='min-width:100px;color:var(--ink);font-weight:500;'>{r['name']}</div>"
                    f"<div style='flex:1;height:10px;background:var(--bg-sunken);"
                    f"border-radius:4px;overflow:hidden;'>"
                    f"<div style='width:{w:.0f}%;height:100%;background:{tone_color};"
                    f"border-radius:4px;'></div></div>"
                    f"<div style='min-width:65px;text-align:right;color:{tone_color};"
                    f"font-weight:600;font-variant-numeric:tabular-nums;'>"
                    f"{sign}{v*100:.1f}%</div>"
                    f"</div>"
                )
            st.markdown(
                f"<div style='font-size:14px;font-weight:600;color:var(--ink);"
                f"margin-bottom:10px;'><span class='dot {dot_class}'></span>{title}</div>"
                f"<div>{inner}</div>",
                unsafe_allow_html=True,
            )

        with col_top:
            _hbar_list(valid_sorted.head(n_show), "var(--up)", "Top performers", "up")
        with col_bot:
            _hbar_list(valid_sorted.tail(n_show).sort_values(col_perf),
                        "var(--down)", "Pires performers", "down")

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

    # ───────── TAB 4 : Tableau multi-périodes ──────────────
    with tab_multi:
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

        picker_options = [
            (row["ticker"], f"{row['ticker']} — {row['name']}")
            for _, row in summary.iterrows()
        ]
        ticker_quick_picker(picker_options, key="perf_goto",
                             label="Ouvrir l'analyse d'un titre")


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
