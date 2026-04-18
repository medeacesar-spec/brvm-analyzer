"""
Page 8 : Informations Générales du Marché
Actualités du marche, panorama des profils d'entreprise, notes d'analyse.
Les infos qualitatives spécifiques à chaque titre sont dans l'onglet Profil de la page Analyse.
"""

import streamlit as st
import pandas as pd
import time

from config import load_tickers
from data.storage import (
    get_all_company_profiles, get_company_news,
    save_company_news, save_company_profile,
    get_connection, get_publication_calendar,
)


def _sync_all_profiles():
    """Scrape les profils et actus de tous les tickers."""
    from data.scraper import fetch_company_profile, fetch_company_news

    tickers = load_tickers()
    progress = st.progress(0, text="Chargement des profils et actualités...")
    ok = 0
    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        try:
            profile = fetch_company_profile(ticker)
            if profile.get("description") or profile.get("dg"):
                save_company_profile(profile)
                ok += 1
            articles = fetch_company_news(ticker, max_articles=8)
            if articles:
                save_company_news(ticker, articles)
        except Exception:
            pass
        progress.progress((i + 1) / len(tickers), text=f"{ticker}... ({i+1}/{len(tickers)})")
        time.sleep(0.3)
    progress.empty()
    return ok


def render():
    st.markdown('<div class="main-header">📅 Infos Générales Marché</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Actualités, panorama des sociétés et calendrier des publications BRVM</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs([
        "📰 Fil d'actualités",
        "🏢 Panorama des sociétés",
        "📅 Calendrier des publications",
    ])

    with tab1:
        _render_news_feed()

    with tab2:
        _render_company_overview()

    with tab3:
        _render_publication_calendar()


def _render_news_feed():
    """Fil d'actualités agrege pour tous les titres."""
    st.subheader("Actualités du marché BRVM")

    tickers_data = load_tickers()
    ticker_names = {t["ticker"]: t["name"] for t in tickers_data}

    col1, col2 = st.columns([4, 1])
    with col1:
        filter_options = ["Tous les titres"] + [f"{t['ticker']} - {t['name']}" for t in tickers_data]
        filter_sel = st.selectbox("Filtrer par titre", filter_options, key="news_filter")
    with col2:
        st.write("")
        st.write("")
        if st.button("🔄 Actualiser", key="refresh_news"):
            from data.scraper import fetch_company_news
            progress = st.progress(0)
            for i, t in enumerate(tickers_data):
                try:
                    articles = fetch_company_news(t["ticker"], max_articles=5)
                    if articles:
                        save_company_news(t["ticker"], articles)
                except Exception:
                    pass
                progress.progress((i + 1) / len(tickers_data))
                time.sleep(0.2)
            progress.empty()
            st.rerun()

    selected_ticker = None
    if filter_sel != "Tous les titres":
        selected_ticker = filter_sel.split(" - ")[0]

    news = get_company_news(selected_ticker, limit=80)
    if news.empty:
        st.info("Aucune actualité chargée. Cliquez sur 'Actualiser' ou lancez `scripts/scrape_profiles.py`.")
        return

    st.caption(f"{len(news)} article(s)")

    for _, art in news.iterrows():
        ticker = art.get("ticker", "")
        name = ticker_names.get(ticker, ticker)
        date_str = art.get("article_date", "")
        url = art.get("url", "")

        col_info, col_content = st.columns([1, 5])
        with col_info:
            if date_str:
                st.caption(date_str)
            st.caption(f"**{name}**")
        with col_content:
            if url and url.startswith("http"):
                st.markdown(f"[{art['title']}]({url})")
            else:
                st.write(art["title"])
        st.markdown("<hr style='margin:0.2rem 0;border-color:#333;'>", unsafe_allow_html=True)


def _render_company_overview():
    """Panorama des sociétés BRVM avec profils."""
    st.subheader("Panorama des sociétés BRVM")

    profiles = get_all_company_profiles()
    if profiles.empty:
        st.info("Les profils n'ont pas encore ete charges.")
        if st.button("📥 Charger les profils (~30s)", key="load_all_profiles"):
            count = _sync_all_profiles()
            st.success(f"✅ {count} profils charges !")
            st.rerun()
        return

    tickers_data = load_tickers()
    ticker_names = {t["ticker"]: t["name"] for t in tickers_data}
    ticker_sectors = {t["ticker"]: t.get("sector", "") for t in tickers_data}

    # Filter by sector
    all_sectors = sorted(set(ticker_sectors.values()) - {""})
    selected_sector = st.selectbox("Filtrer par secteur", ["Tous"] + all_sectors)

    # Get market data for all tickers
    conn = get_connection()
    md = pd.read_sql_query(
        "SELECT ticker, price, market_cap, shares, float_pct FROM market_data WHERE price > 0",
        conn,
    )
    conn.close()
    md_map = {row["ticker"]: row.to_dict() for _, row in md.iterrows()} if not md.empty else {}

    # Display profiles as expandable cards
    st.caption(f"💡 Pour l'analyse détaillée d'un titre, utilisez **Analyse d'un Titre > Onglet Profil**")
    st.markdown("---")

    displayed = 0
    for _, profile in profiles.iterrows():
        ticker = profile["ticker"]
        name = ticker_names.get(ticker, ticker)
        sector = ticker_sectors.get(ticker, "")

        if selected_sector != "Tous" and sector != selected_sector:
            continue

        mdata = md_map.get(ticker, {})
        price = mdata.get("price", 0) or 0
        mcap = mdata.get("market_cap", 0) or 0

        # Summary line
        price_str = f" | Prix: {price:,.0f} FCFA" if price else ""
        mcap_str = f" | Cap: {mcap/1e3:,.1f} Mds" if mcap else ""

        with st.expander(f"**{name}** ({ticker}) — {sector}{price_str}{mcap_str}"):
            if profile.get("description"):
                desc = profile["description"]
                for prefix in ["La société :", "La société :", "La société:", "La société:"]:
                    if desc.startswith(prefix):
                        desc = desc[len(prefix):].strip()
                st.markdown(desc[:500] + ("..." if len(desc) > 500 else ""))

            col_a, col_b = st.columns(2)
            with col_a:
                if profile.get("dg"):
                    st.markdown(f"**DG :** {profile['dg']}")
                if profile.get("president"):
                    st.markdown(f"**PCA :** {profile['president']}")
            with col_b:
                if profile.get("major_shareholder"):
                    pct = profile.get("major_shareholder_pct")
                    pct_str = f" ({pct:.1f}%)" if pct else ""
                    st.markdown(f"**Actionnaire :** {profile['major_shareholder']}{pct_str}")
                if profile.get("phone"):
                    st.caption(f"📞 {profile['phone']}")

        displayed += 1

    st.caption(f"{displayed} société(s) affichée(s)")


def _render_publication_calendar():
    """Calendrier des publications financières attendues."""
    st.subheader("Calendrier des publications attendues")

    tickers_data = load_tickers()
    all_sectors = sorted(set(t.get("sector", "") for t in tickers_data) - {""})
    selected_sector = st.selectbox("Filtrer par secteur", ["Tous les secteurs"] + all_sectors, key="cal_sector_filter")

    calendar = get_publication_calendar()
    if calendar.empty:
        st.info("Aucune publication enregistrée. Importez des données fondamentales pour alimenter le calendrier.")
        return

    if selected_sector != "Tous les secteurs":
        sector_tickers = {t["ticker"] for t in tickers_data if t.get("sector") == selected_sector}
        calendar = calendar[calendar["ticker"].isin(sector_tickers)]

    if calendar.empty:
        st.info("Aucune publication attendue pour ce secteur.")
        return

    status_emoji = {"a_venir": "🔵", "attendu_ce_mois": "🟡", "en_retard": "🔴"}

    # Group by status for better readability
    for status, label in [("en_retard", "🔴 En retard"), ("attendu_ce_mois", "🟡 Attendu ce mois"), ("a_venir", "🔵 À venir")]:
        group = calendar[calendar["status"] == status] if "status" in calendar.columns else pd.DataFrame()
        if not group.empty:
            st.markdown(f"**{label}** ({len(group)})")
            for _, row in group.iterrows():
                emoji = status_emoji.get(row.get("status", ""), "⚪")
                st.write(f"{emoji} **{row['company_name']}** — {row['period']} ({row['type']}) — {row['status'].replace('_', ' ')}")
            st.markdown("---")

    st.caption(f"{len(calendar)} publication(s) au total")
