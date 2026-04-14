"""
Page 6 : Suivi Portefeuille
Gestion des positions et performance.
"""

import streamlit as st
import pandas as pd

from config import load_tickers, CURRENCY
from data.storage import (
    save_position, get_portfolio, delete_position,
    get_fundamentals, get_cached_prices,
)
from data.scraper import fetch_daily_quotes
from utils.charts import pie_chart


def render():
    st.markdown('<div class="main-header">💼 Suivi Portefeuille</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Gerez vos positions et suivez la performance</div>', unsafe_allow_html=True)

    portfolio = get_portfolio()

    # --- Import from screenshot ---
    with st.expander("📸 Importer depuis un screenshot SGI", expanded=False):
        st.markdown(
            "Uploadez une capture d'ecran de votre portefeuille SGI. "
            "L'application analysera l'image et extraira automatiquement vos positions."
        )
        screenshot = st.file_uploader(
            "Screenshot du portefeuille SGI",
            type=["png", "jpg", "jpeg"],
            key="sgi_screenshot",
        )
        if screenshot:
            st.image(screenshot, caption="Votre portefeuille SGI", use_container_width=True)
            st.markdown("---")
            st.markdown(
                "**Saisie assistee** : Renseignez les positions visibles sur le screenshot ci-dessous. "
                "Chaque ligne correspond a un titre visible dans votre portefeuille SGI."
            )
            _render_batch_input(load_tickers())

    # --- Add position ---
    with st.expander("➕ Ajouter une position", expanded=portfolio.empty):
        tickers_data = load_tickers()
        options = [f"{t['ticker']} - {t['name']}" for t in tickers_data]

        with st.form("add_position"):
            col1, col2, col3, col4 = st.columns(4)
            selection = col1.selectbox("Titre", options)
            quantity = col2.number_input("Quantite", min_value=1, value=10)
            avg_price = col3.number_input("Prix moyen d'achat (FCFA)", min_value=1, value=1000)
            purchase_date = col4.date_input("Date d'achat")

            notes = st.text_input("Notes (optionnel)")
            submitted = st.form_submit_button("💾 Ajouter")

            if submitted:
                ticker = selection.split(" - ")[0]
                name = selection.split(" - ")[1] if " - " in selection else ""
                save_position(ticker, name, quantity, avg_price, str(purchase_date), notes)
                st.success("✅ Position ajoutee !")
                st.rerun()

    if portfolio.empty:
        st.info("Aucune position en portefeuille. Ajoutez votre premiere position ci-dessus.")
        return

    # --- Portfolio summary ---
    st.markdown("---")

    # Try to get current prices
    try:
        quotes = fetch_daily_quotes()
        price_map = dict(zip(quotes["ticker"], quotes["last"]))
    except Exception:
        price_map = {}

    # Enrich portfolio with current prices
    portfolio["current_price"] = portfolio["ticker"].map(price_map)
    portfolio["invested"] = portfolio["quantity"] * portfolio["avg_price"]
    portfolio["current_value"] = portfolio.apply(
        lambda r: r["quantity"] * r["current_price"] if pd.notna(r["current_price"]) else r["invested"],
        axis=1,
    )
    portfolio["pnl"] = portfolio["current_value"] - portfolio["invested"]
    portfolio["pnl_pct"] = portfolio["pnl"] / portfolio["invested"] * 100

    # --- KPIs ---
    total_invested = portfolio["invested"].sum()
    total_value = portfolio["current_value"].sum()
    total_pnl = total_value - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Investissement total", f"{total_invested:,.0f} {CURRENCY}")
    col2.metric("Valeur actuelle", f"{total_value:,.0f} {CURRENCY}")
    col3.metric("P&L", f"{total_pnl:,.0f} {CURRENCY}", delta=f"{total_pnl_pct:.1f}%")
    col4.metric("Nb positions", len(portfolio))

    # --- Projected dividends ---
    total_div = 0
    for _, pos in portfolio.iterrows():
        fund = get_fundamentals(pos["ticker"])
        if fund and fund.get("dps"):
            total_div += fund["dps"] * pos["quantity"]

    if total_div > 0:
        div_yield_pf = (total_div / total_value * 100) if total_value > 0 else 0
        st.info(f"💰 Dividendes projetes: **{total_div:,.0f} {CURRENCY}** (rendement portefeuille: {div_yield_pf:.1f}%)")

    st.markdown("---")

    # --- Positions table ---
    st.subheader("Positions")

    for _, pos in portfolio.iterrows():
        col1, col2, col3, col4, col5, col6, col7 = st.columns([2, 1, 1, 1, 1, 1, 0.5])
        col1.write(f"**{pos['company_name']}** ({pos['ticker']})")
        col2.write(f"{pos['quantity']:.0f} titres")
        col3.write(f"PRU: {pos['avg_price']:,.0f}")
        col4.write(f"Investi: {pos['invested']:,.0f}")
        if pd.notna(pos.get("current_price")):
            col5.write(f"Actuel: {pos['current_price']:,.0f}")
            pnl_color = "#28a745" if pos["pnl"] >= 0 else "#dc3545"
            col6.markdown(f"<span style='color:{pnl_color}'>{pos['pnl']:+,.0f} ({pos['pnl_pct']:+.1f}%)</span>", unsafe_allow_html=True)
        else:
            col5.write("Prix N/A")
            col6.write("—")
        if col7.button("🗑️", key=f"del_{pos['id']}"):
            delete_position(pos["id"])
            st.rerun()

    # --- Allocation chart ---
    st.markdown("---")
    col_pie1, col_pie2 = st.columns(2)

    with col_pie1:
        st.subheader("Allocation par titre")
        labels = portfolio["company_name"].tolist()
        values = portfolio["current_value"].tolist()
        fig = pie_chart(labels, values, "Allocation par titre")
        st.plotly_chart(fig, use_container_width=True)

    with col_pie2:
        st.subheader("Allocation par secteur")
        tickers_data = load_tickers()
        ticker_to_sector = {t["ticker"]: t["sector"] for t in tickers_data}
        portfolio["sector"] = portfolio["ticker"].map(ticker_to_sector).fillna("Autre")
        sector_alloc = portfolio.groupby("sector")["current_value"].sum()
        fig = pie_chart(sector_alloc.index.tolist(), sector_alloc.values.tolist(), "Allocation sectorielle")
        st.plotly_chart(fig, use_container_width=True)


def _render_batch_input(tickers_data):
    """Formulaire de saisie en lot pour import screenshot SGI."""
    options = [""] + [f"{t['ticker']} - {t['name']}" for t in tickers_data]
    nb_lines = st.number_input("Nombre de lignes a saisir", min_value=1, max_value=20, value=3, key="batch_lines")

    with st.form("batch_import"):
        positions = []
        for i in range(nb_lines):
            col1, col2, col3 = st.columns([3, 1, 1])
            ticker_sel = col1.selectbox(f"Titre {i+1}", options, key=f"batch_ticker_{i}")
            qty = col2.number_input("Qte", min_value=0, value=0, key=f"batch_qty_{i}")
            pru = col3.number_input("PRU (FCFA)", min_value=0, value=0, key=f"batch_pru_{i}")
            if ticker_sel and qty > 0 and pru > 0:
                positions.append((ticker_sel, qty, pru))

        if st.form_submit_button("💾 Importer toutes les positions"):
            if positions:
                for sel, qty, pru in positions:
                    ticker = sel.split(" - ")[0]
                    name = sel.split(" - ")[1] if " - " in sel else ""
                    save_position(ticker, name, qty, pru)
                st.success(f"✅ {len(positions)} position(s) importee(s) !")
                st.rerun()
            else:
                st.warning("Aucune position valide a importer")
