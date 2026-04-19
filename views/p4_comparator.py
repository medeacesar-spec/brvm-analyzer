"""
Page 4 : Comparateur sectoriel
Choisir un secteur → titres auto-proposes pour comparaison.
"""

import streamlit as st
import pandas as pd
import numpy as np

from config import load_tickers
from data.storage import (
    get_fundamentals, get_cached_prices, list_tickers_with_fundamentals,
    get_all_stocks_for_analysis, get_analyzable_tickers,
)
from analysis.fundamental import compute_ratios, format_ratio
from utils.nav import ticker_analyze_button
from utils.charts import radar_chart, performance_chart, COLORS
from utils.ui_helpers import section_heading


def render():
    # Hiérarchie v3 : Title + caption → sélecteur → tableau → charts
    st.title("Comparateur")
    st.caption("Comparer les titres d'un secteur ou une sélection libre")

    analyzable = get_analyzable_tickers()
    all_stocks = get_all_stocks_for_analysis()

    if not analyzable:
        st.warning("Aucune donnée disponible.")
        return

    mode = st.radio("Mode de comparaison", ["Par secteur", "Sélection libre"], horizontal=True)

    if mode == "Par secteur":
        sectors = sorted(set(t["sector"] for t in analyzable if t.get("sector")))
        selected_sector = st.selectbox("Secteur", sectors)
        sector_tickers = [t for t in analyzable if t["sector"] == selected_sector]
        st.caption(f"{len(sector_tickers)} titres avec données · secteur {selected_sector}")

        options = [f"{t['ticker']} · {t['name']}" for t in sector_tickers]
        selected = st.multiselect(
            "Titres à comparer", options,
            default=options[:min(5, len(options))],
        )
        tickers = [s.split(" · ")[0] for s in selected]
    else:
        all_options = [f"{t['ticker']} · {t['name']}" for t in analyzable]
        selected = st.multiselect("Sélectionnez 2 à 5 titres", all_options, default=[])
        tickers = [s.split(" · ")[0] for s in selected]

    if len(tickers) < 2:
        st.info("Sélectionnez au moins 2 titres pour comparer.")
        return

    # Boutons "Ouvrir" pour chaque ticker sélectionné
    if tickers:
        btn_cols = st.columns(len(tickers))
        for i, ticker in enumerate(tickers):
            with btn_cols[i]:
                ticker_analyze_button(
                    ticker, label=ticker,
                    key=f"cmp_goto_{ticker}", use_container_width=True,
                )

    # Load data for selected tickers — depuis all_stocks (1 requête cachée)
    # au lieu de N appels get_fundamentals (N round-trips Supabase).
    import math as _m
    stocks = {}
    if not all_stocks.empty:
        stocks_by_ticker = {r["ticker"]: r.to_dict() for _, r in all_stocks.iterrows()}
    else:
        stocks_by_ticker = {}
    for ticker in tickers:
        data = stocks_by_ticker.get(ticker)
        if not data:
            continue
        data = {k: (None if isinstance(v, float) and _m.isnan(v) else v)
                for k, v in data.items()}
        if data.get("price"):
            ratios = compute_ratios(data)
            stocks[ticker] = {"fundamentals": data, "ratios": ratios}

    if len(stocks) < 2:
        st.warning("Données insuffisantes pour au moins 2 titres. Importez des données fondamentales.")
        return

    # --- Tableau comparatif ---
    section_heading("Tableau comparatif", spacing="loose")

    metrics = [
        ("Prix (FCFA)",       "price",              "number"),
        ("ROE",               "roe",                "pct"),
        ("Marge nette",       "net_margin",         "pct"),
        ("PER",               "per",                "decimal"),
        ("Dividend Yield",    "dividend_yield",     "pct"),
        ("Payout ratio",      "payout_ratio",       "pct"),
        ("Dette/Equity",      "debt_equity",        "x"),
        ("P/B",               "pb",                 "x"),
        ("EPS (FCFA)",        "eps",                "number"),
        ("DPS (FCFA)",        "dps",                "number"),
        ("Checklist V&D",     "_checklist",         "text"),
        ("Score fondamental", "fundamental_score",  "decimal"),
    ]

    comp_data = {"Indicateur": [m[0] for m in metrics]}
    for ticker, data in stocks.items():
        name = data["fundamentals"].get("company_name") or ticker
        values = []
        for _, key, fmt in metrics:
            if key == "price":
                val = data["fundamentals"].get("price")
                values.append(format_ratio(val, fmt))
            elif key == "_checklist":
                cl = data["ratios"].get("checklist", [])
                passed = sum(1 for c in cl if c["passed"] is True)
                total = len(cl)
                values.append(f"{passed} / {total}" if total else "—")
            elif key == "fundamental_score":
                val = data["ratios"].get("fundamental_score")
                values.append(format_ratio(val, fmt))
            else:
                val = data["ratios"].get(key)
                values.append(format_ratio(val, fmt))
        comp_data[f"{name} · {ticker}"] = values

    comp_df = pd.DataFrame(comp_data)
    st.dataframe(comp_df, use_container_width=True, hide_index=True)

    # --- Bar chart horizontal monochrome (remplace le radar arc-en-ciel) ---
    # Principe design v3 #07 : dataviz monochrome. Bar horizontal groupé
    # est plus lisible que le radar pour des valeurs chiffrées.
    section_heading("Profil comparatif", spacing="loose")

    import plotly.graph_objects as go
    bar_metrics = [
        ("ROE", lambda r: min((r.get("roe") or 0) / 0.30 * 100, 100)),
        ("Marge", lambda r: min((r.get("net_margin") or 0) / 0.25 * 100, 100)),
        ("Yield", lambda r: min((r.get("dividend_yield") or 0) / 0.10 * 100, 100)),
        ("Valorisation", lambda r: max(0, min(100, (20 - (r.get("per") or 0)) / 20 * 100))
                          if (r.get("per") or 0) > 0 else 0),
        ("Croissance", lambda r: min(max(((r.get("revenue_growth") or 0)) / 0.15 * 100, 0), 100)),
        ("Score global", lambda r: (r.get("fundamental_score") or 0) / 50 * 100),
    ]
    # Palette monochrome (design v3) — deep green + accent ocre + neutre
    mono_palette = [
        COLORS["primary"], COLORS["accent"], COLORS["secondary"],
        "#4A8A5F", "#D97E4F", "#A69D8D",  # variantes
    ]

    fig = go.Figure()
    for i, (ticker, data) in enumerate(stocks.items()):
        r = data["ratios"]
        name = data["fundamentals"].get("company_name") or ticker
        values = [fn(r) for _, fn in bar_metrics]
        fig.add_trace(go.Bar(
            y=[m[0] for m in bar_metrics],
            x=values,
            name=name,
            orientation="h",
            marker_color=mono_palette[i % len(mono_palette)],
            text=[f"{v:.0f}" for v in values],
            textposition="auto",
        ))
    fig.update_layout(
        barmode="group", height=380,
        template="plotly_white",
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"], family="ui-sans-serif, -apple-system, sans-serif", size=12),
        xaxis=dict(title="Score (0-100)", range=[0, 100], gridcolor=COLORS["border"]),
        yaxis=dict(autorange="reversed"),
        margin=dict(l=10, r=10, t=10, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=11, color=COLORS["text_secondary"])),
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Performance Chart ---
    section_heading("Performance comparée des prix", spacing="loose")

    price_data = {}
    for ticker in tickers:
        prices = get_cached_prices(ticker)
        if not prices.empty and "close" in prices.columns:
            price_data[ticker] = prices.set_index("date")["close"]

    if len(price_data) >= 2:
        fig = performance_chart(price_data, "Performance normalisee (base 100)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas assez de données de prix pour comparer les performances. Chargez les prix depuis la page Analyse.")
