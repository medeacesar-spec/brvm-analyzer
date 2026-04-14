"""
Page 2 : Analyse individuelle d'un titre
Onglets: Fondamental | Technique | Recommandation
"""

import streamlit as st
import pandas as pd

from config import load_tickers, CURRENCY
from data.storage import (
    get_fundamentals, save_fundamentals, get_all_fundamentals,
    import_from_excel, list_tickers_with_fundamentals,
    get_cached_prices, cache_prices, get_analyzable_tickers,
    get_all_stocks_for_analysis,
)
from data.scraper import fetch_historical_prices
from analysis.fundamental import compute_ratios, format_ratio
from analysis.technical import compute_all_indicators, detect_trend, detect_support_resistance, generate_signals
from analysis.scoring import compute_hybrid_score
from utils.charts import candlestick_chart, gauge_chart, flag_badge, stars_display


def render():
    st.markdown('<div class="main-header">🔍 Analyse d\'un Titre</div>', unsafe_allow_html=True)

    # --- Selection du titre (seulement ceux avec donnees) ---
    analyzable = get_analyzable_tickers()

    if not analyzable:
        st.warning("Aucune donnee disponible. Lancez l'enrichissement depuis le Dashboard ou importez des fichiers Excel.")
        return

    col_select, col_import = st.columns([3, 1])

    with col_select:
        all_options = [f"{t['ticker']} - {t['name']}" + (" 📊" if t.get("has_fundamentals") else " 📈") for t in analyzable]
        selection = st.selectbox("Choisir un titre (📊=fondamentaux 📈=marche)", all_options, index=0)
        selected_ticker = selection.split(" - ")[0]

    with col_import:
        st.markdown("##### Importer Excel")
        uploaded = st.file_uploader("Fichier Analyse Hybride", type=["xlsx"], label_visibility="collapsed")
        if uploaded:
            import tempfile, os
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                tmp.write(uploaded.read())
                tmp_path = tmp.name
            try:
                data = import_from_excel(tmp_path)
                save_fundamentals(data)
                st.success(f"✅ {data['company_name']} importe ({data['fiscal_year']})")
                st.rerun()
            except Exception as e:
                st.error(f"Erreur d'import: {e}")
            finally:
                os.unlink(tmp_path)

    # --- Load data (fondamentaux OU donnees de marche) ---
    fundamentals = get_fundamentals(selected_ticker)

    if not fundamentals:
        # Fallback: essayer les donnees de marche auto-scrapees
        all_stocks = get_all_stocks_for_analysis()
        if not all_stocks.empty:
            row = all_stocks[all_stocks["ticker"] == selected_ticker]
            if not row.empty:
                fundamentals = row.iloc[0].to_dict()

    if not fundamentals:
        st.warning(f"Aucune donnee pour {selected_ticker}. Importez un fichier Excel ou saisissez les donnees.")
        _render_input_form(selected_ticker, analyzable)
        return

    # Load price data
    price_df = get_cached_prices(selected_ticker)
    if price_df.empty:
        with st.spinner("Chargement des prix historiques..."):
            try:
                price_df = fetch_historical_prices(selected_ticker)
                if not price_df.empty:
                    cache_prices(selected_ticker, price_df)
            except Exception:
                price_df = pd.DataFrame()

    # --- Compute scores ---
    result = compute_hybrid_score(fundamentals, price_df)
    ratios = result["ratios"]
    reco = result["recommendation"]

    # --- Header metrics ---
    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Entreprise", fundamentals.get("company_name", ""))
    c2.metric("Prix", f"{fundamentals.get('price', 0):,.0f} {CURRENCY}")
    c3.metric("Secteur", fundamentals.get("sector", ""))
    c4.metric("Exercice", str(fundamentals.get("fiscal_year", "")))
    c5.markdown(f"### {stars_display(reco['stars'])}")
    c5.markdown(f"<span style='color:{reco['verdict_color']};font-weight:bold;font-size:1.2em;'>{reco['verdict']}</span>", unsafe_allow_html=True)

    # --- Tabs ---
    tab1, tab2, tab3 = st.tabs(["📊 Fondamental", "📈 Technique", "🎯 Recommandation"])

    with tab1:
        _render_fundamental(fundamentals, ratios)

    with tab2:
        _render_technical(selected_ticker, price_df, result)

    with tab3:
        _render_recommendation(result, fundamentals)


def _render_fundamental(fundamentals, ratios):
    """Onglet analyse fondamentale."""
    st.subheader("Ratios calcules")

    # Main ratios table
    flags = ratios.get("flags", {})
    ratio_rows = [
        ("ROE", format_ratio(ratios.get("roe")), "≥ 15% (solide) ; ≥ 20% (excellent)", flags.get("roe", ("—", ""))),
        ("Marge nette", format_ratio(ratios.get("net_margin")), "≥ 10% (bon) ; ≥ 15% (tres bon)", flags.get("net_margin", ("—", ""))),
        ("Dette/Equity", format_ratio(ratios.get("debt_equity"), "x"), "≤ 1.5 (hors banques)", flags.get("debt_equity", ("—", ""))),
        ("Couverture interets", format_ratio(ratios.get("interest_coverage"), "x"), "≥ 3x (confortable)", flags.get("interest_coverage", ("—", ""))),
        ("FCF", format_ratio(ratios.get("fcf"), "number"), "Positif et stable", flags.get("fcf", ("—", ""))),
        ("FCF Margin", format_ratio(ratios.get("fcf_margin")), "≥ 5% (bon) ; ≥ 10% (tres bon)", flags.get("fcf_margin", ("—", ""))),
        ("EPS", format_ratio(ratios.get("eps"), "number"), "—", ("OK", "")),
        ("DPS", format_ratio(ratios.get("dps"), "number"), "—", ("OK", "")),
        ("Dividend Yield", format_ratio(ratios.get("dividend_yield")), "≥ 6% (cible BRVM)", flags.get("dividend_yield", ("—", ""))),
        ("Payout ratio", format_ratio(ratios.get("payout_ratio")), "40-70% (sain)", flags.get("payout_ratio", ("—", ""))),
        ("PER", format_ratio(ratios.get("per"), "decimal"), "≤ 12-15 (value)", flags.get("per", ("—", ""))),
        ("P/B", format_ratio(ratios.get("pb"), "x"), "< 2 (hors banques)", flags.get("pb", ("—", ""))),
        ("Couverture div (cash)", format_ratio(ratios.get("dividend_cash_coverage"), "x"), "≥ 1.2x", flags.get("dividend_cash_coverage", ("—", ""))),
    ]

    for name, value, rule, (flag, detail) in ratio_rows:
        c1, c2, c3, c4 = st.columns([2, 1.5, 3, 1.5])
        c1.write(f"**{name}**")
        c2.write(value)
        c3.write(rule)
        flag_color = {"OK": "🟢", "Vigilance": "🟡", "Risque": "🔴"}.get(flag, "⚪")
        c4.write(f"{flag_color} {flag} - {detail}")

    # Checklist
    st.markdown("---")
    st.subheader("Checklist Value & Dividendes")
    checklist = ratios.get("checklist", [])
    for item in checklist:
        if item["passed"] is True:
            st.write(f"✅ {item['label']} — Valeur: {format_ratio(item['value'])}")
        elif item["passed"] is False:
            st.write(f"❌ {item['label']} — Valeur: {format_ratio(item['value'])}")
        else:
            st.write(f"⚪ {item['label']} — N/A")

    passed = sum(1 for i in checklist if i["passed"] is True)
    total = len(checklist)
    st.progress(passed / total if total > 0 else 0, text=f"{passed}/{total} criteres valides")

    # Historical growth
    st.markdown("---")
    st.subheader("Historique (N-3 a N)")
    hist_data = {
        "Chiffre d'affaires": [fundamentals.get(f"revenue_{s}") for s in ("n3", "n2", "n1", "n0")],
        "Resultat net": [fundamentals.get(f"net_income_{s}") for s in ("n3", "n2", "n1", "n0")],
        "DPS": [fundamentals.get(f"dps_{s}") for s in ("n3", "n2", "n1", "n0")],
    }
    years = ["N-3", "N-2", "N-1", "N"]

    for label, values in hist_data.items():
        if any(v is not None for v in values):
            c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 1, 1])
            c1.write(f"**{label}**")
            for i, (col, val) in enumerate(zip([c2, c3, c4, c5], values)):
                if val is not None:
                    col.write(f"{val:,.0f}")
                else:
                    col.write("—")


def _render_technical(ticker, price_df, result):
    """Onglet analyse technique."""
    if price_df.empty or len(price_df) < 5:
        st.warning("Donnees de prix insuffisantes pour l'analyse technique.")
        st.info("Cliquez sur le bouton ci-dessous pour tenter de charger les prix depuis sikafinance.com")
        if st.button("📥 Charger les prix"):
            with st.spinner("Telechargement en cours..."):
                try:
                    price_df = fetch_historical_prices(ticker)
                    if not price_df.empty:
                        cache_prices(ticker, price_df)
                        st.success(f"{len(price_df)} jours de donnees charges")
                        st.rerun()
                    else:
                        st.error("Aucune donnee trouvee")
                except Exception as e:
                    st.error(f"Erreur: {e}")
        return

    # Compute indicators
    df = compute_all_indicators(price_df)

    # Chart options
    col_opt1, col_opt2, col_opt3 = st.columns(3)
    with col_opt1:
        show_bb = st.checkbox("Bandes de Bollinger", value=False)
    with col_opt2:
        show_rsi = st.checkbox("RSI", value=True)
    with col_opt3:
        show_macd = st.checkbox("MACD", value=True)

    # Candlestick chart
    fig = candlestick_chart(
        df, title=f"{ticker}", show_bollinger=show_bb, show_rsi=show_rsi, show_macd=show_macd
    )
    st.plotly_chart(fig, use_container_width=True)

    # Trend
    trend = result.get("trend", {})
    st.markdown("---")
    col_t1, col_t2, col_t3 = st.columns(3)
    trend_emoji = {"haussiere": "📈", "baissiere": "📉", "neutre": "➡️"}.get(trend.get("trend", ""), "❓")
    col_t1.metric("Tendance", f"{trend_emoji} {trend.get('trend', 'N/A').capitalize()}")
    col_t2.metric("Force", trend.get("strength", "N/A").capitalize())
    col_t3.metric("Detail", trend.get("details", ""))

    # Supports / Resistances
    supports = result.get("supports", [])
    resistances = result.get("resistances", [])
    col_sr1, col_sr2 = st.columns(2)
    with col_sr1:
        st.subheader("🟢 Supports")
        for i, s in enumerate(supports[:3]):
            st.write(f"Zone {i+1}: **{s:,.0f} FCFA**")
        if not supports:
            st.info("Aucun support detecte")
    with col_sr2:
        st.subheader("🔴 Resistances")
        for i, r in enumerate(resistances[:3]):
            st.write(f"Zone {i+1}: **{r:,.0f} FCFA**")
        if not resistances:
            st.info("Aucune resistance detectee")

    # Signals
    st.markdown("---")
    st.subheader("Signaux techniques")
    signals = result.get("signals", [])
    if signals:
        for sig in signals:
            emoji = {"achat": "🟢", "vente": "🔴", "info": "🔵"}.get(sig["type"], "⚪")
            strength = "★" * sig["strength"] + "☆" * (5 - sig["strength"])
            st.write(f"{emoji} **{sig['signal']}** ({strength}) — {sig['details']}")
    else:
        st.info("Aucun signal technique actif")


def _render_recommendation(result, fundamentals):
    """Onglet recommandation."""
    reco = result["recommendation"]

    # Score gauges
    col_g1, col_g2, col_g3 = st.columns(3)
    with col_g1:
        st.plotly_chart(gauge_chart(result["fundamental_score"], 50, "Fondamental"), use_container_width=True)
    with col_g2:
        st.plotly_chart(gauge_chart(result["technical_score"], 50, "Technique"), use_container_width=True)
    with col_g3:
        st.plotly_chart(gauge_chart(result["hybrid_score"], 100, "Score Hybride"), use_container_width=True)

    # Verdict
    st.markdown(f"### {stars_display(reco['stars'])} <span style='color:{reco['verdict_color']}'>{reco['verdict']}</span>", unsafe_allow_html=True)

    # Points forts / vigilance
    col_s, col_w = st.columns(2)
    with col_s:
        st.subheader("💪 Points forts")
        for s in reco.get("strengths", []):
            st.write(f"✅ {s}")
        if not reco.get("strengths"):
            st.info("Aucun point fort identifie")
    with col_w:
        st.subheader("⚠️ Points de vigilance")
        for w in reco.get("warnings", []):
            st.write(f"⚠️ {w}")
        if not reco.get("warnings"):
            st.info("Aucun point de vigilance")

    # Entry zones
    st.markdown("---")
    st.subheader("🎯 Zones d'entree suggerees")
    entry_zones = reco.get("entry_zones", [])
    if entry_zones:
        for zone in entry_zones:
            st.write(f"🟢 **{zone['label']}**: {zone['zone']} — Risque/Rendement: {zone['risk_reward']}")
    else:
        st.info("Pas assez de donnees pour determiner les zones d'entree")


def _render_input_form(ticker, tickers_data):
    """Formulaire de saisie manuelle des données fondamentales."""
    st.markdown("---")
    st.subheader("Saisie manuelle des donnees fondamentales")

    ticker_info = next((t for t in tickers_data if t["ticker"] == ticker), {})

    with st.form("fundamental_form"):
        st.markdown("##### Informations societe")
        col1, col2, col3 = st.columns(3)
        company_name = col1.text_input("Nom", value=ticker_info.get("name", ""))
        sector = col2.text_input("Secteur", value=ticker_info.get("sector", ""))
        fiscal_year = col3.number_input("Exercice", value=2024, min_value=2000, max_value=2030)

        col4, col5 = st.columns(2)
        price = col4.number_input("Prix actuel (FCFA)", value=0, min_value=0)
        shares = col5.number_input("Nombre d'actions", value=0, min_value=0)

        st.markdown("##### Donnees financieres")
        col6, col7 = st.columns(2)
        revenue = col6.number_input("Chiffre d'affaires", value=0)
        net_income = col7.number_input("Resultat net", value=0)

        col8, col9 = st.columns(2)
        equity = col8.number_input("Capitaux propres", value=0)
        total_debt = col9.number_input("Dette financiere totale", value=0)

        col10, col11 = st.columns(2)
        ebit = col10.number_input("EBIT", value=0)
        interest_expense = col11.number_input("Charges d'interets", value=0)

        col12, col13 = st.columns(2)
        cfo = col12.number_input("Cash-flow operationnel (CFO)", value=0)
        capex = col13.number_input("CAPEX", value=0)

        col14, col15 = st.columns(2)
        dividends_total = col14.number_input("Dividendes verses (total)", value=0)
        dps = col15.number_input("DPS (dividende par action)", value=0)

        submitted = st.form_submit_button("💾 Enregistrer")
        if submitted:
            data = {
                "ticker": ticker,
                "company_name": company_name,
                "sector": sector,
                "currency": "XOF",
                "fiscal_year": fiscal_year,
                "price": price,
                "shares": shares,
                "revenue": revenue,
                "net_income": net_income,
                "equity": equity,
                "total_debt": total_debt,
                "ebit": ebit,
                "interest_expense": interest_expense,
                "cfo": cfo if cfo != 0 else None,
                "capex": capex,
                "dividends_total": dividends_total,
                "dps": dps,
            }
            save_fundamentals(data)
            st.success("✅ Donnees enregistrees !")
            st.rerun()
