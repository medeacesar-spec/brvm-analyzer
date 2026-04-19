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
from utils.charts import radar_chart, performance_chart


def render():
    st.markdown('<div class="main-header">⚖️ Comparateur Sectoriel</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Choisissez un secteur pour comparer les titres automatiquement</div>', unsafe_allow_html=True)

    analyzable = get_analyzable_tickers()
    all_stocks = get_all_stocks_for_analysis()

    if not analyzable:
        st.warning("Aucune donnée disponible.")
        return

    # --- Mode de selection ---
    mode = st.radio("Mode de comparaison", ["Par secteur", "Selection libre"], horizontal=True)

    if mode == "Par secteur":
        sectors = sorted(set(t["sector"] for t in analyzable if t.get("sector")))
        selected_sector = st.selectbox("Choisir un secteur", sectors)

        sector_tickers = [t for t in analyzable if t["sector"] == selected_sector]
        st.info(f"**{len(sector_tickers)} titres** avec données dans le secteur {selected_sector}")

        options = [f"{t['ticker']} - {t['name']}" + (" 📊" if t.get("has_fundamentals") else " 📈") for t in sector_tickers]
        selected = st.multiselect(
            "Titres à comparer",
            options,
            default=options[:min(5, len(options))],
        )
        tickers = [s.split(" - ")[0] for s in selected]

    else:
        all_options = [f"{t['ticker']} - {t['name']}" + (" 📊" if t.get("has_fundamentals") else " 📈") for t in analyzable]
        selected = st.multiselect("Sélectionnez 2 à 5 titres", all_options, default=[])
        tickers = [s.split(" - ")[0] for s in selected]

    if len(tickers) < 2:
        st.info("Sélectionnez au moins 2 titres pour comparer.")
        return

    # Quick analyze buttons for selected tickers
    if tickers:
        btn_cols = st.columns(len(tickers))
        for i, ticker in enumerate(tickers):
            with btn_cols[i]:
                ticker_analyze_button(
                    ticker, label=f"🔍 {ticker}",
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
    st.subheader("Tableau comparatif")

    metrics = [
        ("Prix (FCFA)", "price", "number"),
        ("ROE", "roe", "pct"),
        ("Marge nette", "net_margin", "pct"),
        ("PER", "per", "decimal"),
        ("Dividend Yield", "dividend_yield", "pct"),
        ("Payout ratio", "payout_ratio", "pct"),
        ("Dette/Equity", "debt_equity", "x"),
        ("P/B", "pb", "x"),
        ("EPS (FCFA)", "eps", "number"),
        ("DPS (FCFA)", "dps", "number"),
        ("Score fondamental", "fundamental_score", "decimal"),
    ]

    comp_data = {"Indicateur": [m[0] for m in metrics]}
    for ticker, data in stocks.items():
        name = data["fundamentals"].get("company_name") or ticker
        values = []
        for _, key, fmt in metrics:
            if key == "price":
                val = data["fundamentals"].get("price")
            elif key == "fundamental_score":
                val = data["ratios"].get("fundamental_score")
            else:
                val = data["ratios"].get(key)
            values.append(format_ratio(val, fmt))
        comp_data[f"{name}\n({ticker})"] = values

    comp_df = pd.DataFrame(comp_data)
    st.dataframe(comp_df, use_container_width=True, hide_index=True)

    # --- Radar Chart ---
    st.markdown("---")
    st.subheader("Radar de comparaison")

    radar_data = {}
    for ticker, data in stocks.items():
        r = data["ratios"]
        name = data["fundamentals"].get("company_name") or ticker

        roe = min((r.get("roe") or 0) / 0.30 * 100, 100)
        margin = min((r.get("net_margin") or 0) / 0.25 * 100, 100)
        dy = min((r.get("dividend_yield") or 0) / 0.10 * 100, 100)

        per = r.get("per")
        per_score = max(0, min(100, (20 - per) / 20 * 100)) if per and per > 0 else 0

        growth = r.get("revenue_growth")
        growth_score = min(max((growth or 0) / 0.15 * 100, 0), 100)

        fund_score = (r.get("fundamental_score") or 0) / 50 * 100

        radar_data[name] = {
            "ROE": roe,
            "Marge": margin,
            "Yield": dy,
            "Valorisation": per_score,
            "Croissance": growth_score,
            "Score Global": fund_score,
        }

    fig = radar_chart(radar_data, "Profil comparatif")
    st.plotly_chart(fig, use_container_width=True)

    # --- Checklist comparison ---
    st.markdown("---")
    st.subheader("Checklist Value & Dividendes")

    for ticker, data in stocks.items():
        name = data["fundamentals"].get("company_name") or ticker
        checklist = data["ratios"].get("checklist", [])
        passed = sum(1 for c in checklist if c["passed"] is True)
        total = len(checklist)

        col1, col2 = st.columns([1, 4])
        col1.write(f"**{name}**")
        col2.progress(passed / total if total > 0 else 0, text=f"{passed}/{total} critères")

    # --- Performance Chart ---
    st.markdown("---")
    st.subheader("Performance comparée des prix")

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
