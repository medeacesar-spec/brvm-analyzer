"""
Page 5 : Signaux d'achat/vente
Choix par secteur ou par titre, scan automatique.
"""

import streamlit as st
import pandas as pd

from config import load_tickers
from data.storage import (
    list_tickers_with_fundamentals, get_fundamentals, get_cached_prices,
    get_publication_calendar, get_analyzable_tickers, get_all_stocks_for_analysis,
)
from analysis.scoring import compute_hybrid_score
from utils.charts import stars_display


def render():
    st.markdown('<div class="main-header">📡 Signaux d\'Achat / Vente</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Choisissez un secteur ou des titres specifiques a analyser</div>', unsafe_allow_html=True)

    analyzable = get_analyzable_tickers()
    if not analyzable:
        st.warning("Aucune donnee disponible.")
        return

    # --- Mode de selection ---
    mode = st.radio("Analyser par", ["Secteur", "Titres specifiques", "Tous les titres avec donnees"], horizontal=True)

    if mode == "Secteur":
        sectors = sorted(set(t["sector"] for t in analyzable if t.get("sector")))
        selected_sector = st.selectbox("Choisir un secteur", sectors)
        target_tickers = [t for t in analyzable if t["sector"] == selected_sector]
        if not target_tickers:
            st.warning(f"Aucun titre avec des donnees dans le secteur {selected_sector}.")
            return

    elif mode == "Titres specifiques":
        options = [f"{t['ticker']} - {t['name']}" for t in analyzable]
        selected = st.multiselect("Choisir des titres", options, default=options[:5])
        target_tickers = [
            {"ticker": s.split(" - ")[0], "name": s.split(" - ")[1] if " - " in s else ""}
            for s in selected
        ]

    else:
        target_tickers = analyzable

    # --- Scan ---
    all_signals = []
    stock_summaries = []

    all_stocks = get_all_stocks_for_analysis()

    with st.spinner(f"Analyse de {len(target_tickers)} titres..."):
        for t in target_tickers:
            ticker = t["ticker"]
            fund = get_fundamentals(ticker)
            if not fund and not all_stocks.empty:
                row = all_stocks[all_stocks["ticker"] == ticker]
                if not row.empty:
                    fund = row.iloc[0].to_dict()
            if not fund:
                continue

            price_df = get_cached_prices(ticker)
            result = compute_hybrid_score(fund, price_df)

            for sig in result.get("signals", []):
                all_signals.append({"ticker": ticker, "name": fund.get("company_name", ""), **sig})

            ratios = result["ratios"]
            checklist = ratios.get("checklist", [])
            passed = sum(1 for c in checklist if c["passed"] is True)
            total = len(checklist)

            if passed == total and total > 0:
                all_signals.append({
                    "ticker": ticker, "name": fund.get("company_name", ""),
                    "type": "achat", "signal": "Checklist complete", "strength": 5,
                    "details": f"Tous les {total} criteres Value & Dividendes valides",
                })
            elif passed >= total - 1 and total > 0:
                all_signals.append({
                    "ticker": ticker, "name": fund.get("company_name", ""),
                    "type": "achat", "signal": "Checklist quasi-complete", "strength": 3,
                    "details": f"{passed}/{total} criteres valides",
                })

            stock_summaries.append({
                "ticker": ticker, "name": fund.get("company_name", ""),
                "sector": fund.get("sector", ""),
                "price": fund.get("price", 0),
                "hybrid_score": result["hybrid_score"],
                "verdict": result["recommendation"]["verdict"],
                "stars": result["recommendation"]["stars"],
                "trend": result["trend"]["trend"],
                "nb_signals": len([s for s in result.get("signals", []) if s["type"] in ("achat", "vente")]),
            })

    # --- Buy Signals ---
    st.subheader("🟢 Signaux d'achat")
    buy_signals = sorted([s for s in all_signals if s["type"] == "achat"], key=lambda x: x["strength"], reverse=True)
    if buy_signals:
        for sig in buy_signals:
            col1, col2, col3, col4 = st.columns([2, 2.5, 1, 3])
            col1.write(f"**{sig['name']}** ({sig['ticker']})")
            col2.write(sig["signal"])
            col3.write(stars_display(sig["strength"]))
            col4.write(sig["details"])
    else:
        st.info("Aucun signal d'achat detecte")

    # --- Sell Signals ---
    st.markdown("---")
    st.subheader("🔴 Signaux de vente")
    sell_signals = sorted([s for s in all_signals if s["type"] == "vente"], key=lambda x: x["strength"], reverse=True)
    if sell_signals:
        for sig in sell_signals:
            col1, col2, col3, col4 = st.columns([2, 2.5, 1, 3])
            col1.write(f"**{sig['name']}** ({sig['ticker']})")
            col2.write(sig["signal"])
            col3.write(stars_display(sig["strength"]))
            col4.write(sig["details"])
    else:
        st.info("Aucun signal de vente detecte")

    # --- Summary Table ---
    st.markdown("---")
    st.subheader("📋 Resume")
    if stock_summaries:
        sum_df = pd.DataFrame(stock_summaries).sort_values("hybrid_score", ascending=False)
        sum_df["stars_display"] = sum_df["stars"].apply(stars_display)
        sum_df["price_fmt"] = sum_df["price"].apply(lambda x: f"{x:,.0f}" if x else "N/A")
        sum_df["score_fmt"] = sum_df["hybrid_score"].apply(lambda x: f"{x:.0f}/100")
        trend_emoji = {"haussiere": "📈", "baissiere": "📉", "neutre": "➡️", "indetermine": "❓"}
        sum_df["trend_display"] = sum_df["trend"].apply(lambda x: f"{trend_emoji.get(x, '❓')} {x}")

        st.dataframe(
            sum_df[["ticker", "name", "sector", "price_fmt", "score_fmt", "verdict", "stars_display", "trend_display", "nb_signals"]].rename(columns={
                "ticker": "Ticker", "name": "Nom", "sector": "Secteur", "price_fmt": "Prix",
                "score_fmt": "Score", "verdict": "Verdict", "stars_display": "Rating",
                "trend_display": "Tendance", "nb_signals": "Signaux",
            }),
            use_container_width=True, hide_index=True,
        )

    # --- Publication calendar ---
    st.markdown("---")
    st.subheader("📅 Calendrier des publications attendues")
    calendar = get_publication_calendar()
    if not calendar.empty:
        relevant = [t["ticker"] for t in target_tickers]
        cal = calendar[calendar["ticker"].isin(relevant)]
        if not cal.empty:
            status_emoji = {"a_venir": "🔵", "attendu_ce_mois": "🟡", "en_retard": "🔴"}
            for _, row in cal.iterrows():
                emoji = status_emoji.get(row["status"], "⚪")
                st.write(f"{emoji} **{row['company_name']}** — {row['period']} ({row['type']}) — {row['status'].replace('_', ' ')}")
        else:
            st.info("Aucune publication attendue pour les titres selectionnes")
    else:
        st.info("Importez des donnees pour voir le calendrier des publications")
