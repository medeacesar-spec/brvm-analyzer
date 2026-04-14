"""
Page 8 : Suivi des Publications, Rapports & Info Qualitative
Rapports annuels, calendrier publications, notes qualitatives.
"""

import streamlit as st
import pandas as pd

from config import load_tickers
from data.storage import (
    get_publication_calendar, get_publications, save_publication,
    mark_publications_read, get_quarterly_data, save_quarterly_data,
    list_tickers_with_fundamentals, get_report_links, seed_known_report_links,
    get_qualitative_notes, save_qualitative_note, delete_qualitative_note,
)
from data.scraper import fetch_brvm_publications


def render():
    st.markdown('<div class="main-header">📅 Publications & Info Qualitative</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Rapports annuels, calendrier des publications, notes qualitatives</div>', unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs([
        "📄 Rapports annuels",
        "📅 Calendrier publications",
        "📝 Notes qualitatives",
        "📊 Donnees trimestrielles",
    ])

    with tab1:
        _render_reports()

    with tab2:
        _render_calendar()

    with tab3:
        _render_qualitative()

    with tab4:
        _render_quarterly()


def _render_reports():
    """Rapports annuels et etats financiers par titre."""
    st.subheader("Rapports annuels & etats financiers")

    # Seed if needed
    reports = get_report_links()
    if reports.empty:
        seed_known_report_links()
        reports = get_report_links()

    if reports.empty:
        st.warning("Aucun rapport disponible.")
        return

    st.success(f"**{len(reports)} rapports** disponibles pour **{reports['ticker'].nunique()} titres**")

    # Filter
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        years = sorted(reports["fiscal_year"].dropna().unique().tolist(), reverse=True)
        selected_year = st.selectbox("Annee", ["Toutes"] + [str(int(y)) for y in years])
    with col_filter2:
        tickers = sorted(reports["ticker"].unique().tolist())
        tickers_data = load_tickers()
        ticker_names = {t["ticker"]: t["name"] for t in tickers_data}
        ticker_options = ["Tous"] + [f"{t} - {ticker_names.get(t, '')}" for t in tickers]
        selected_ticker_opt = st.selectbox("Titre", ticker_options)

    filtered = reports.copy()
    if selected_year != "Toutes":
        filtered = filtered[filtered["fiscal_year"] == int(selected_year)]
    if selected_ticker_opt != "Tous":
        sel_ticker = selected_ticker_opt.split(" - ")[0]
        filtered = filtered[filtered["ticker"] == sel_ticker]

    # Display by ticker
    for ticker in filtered["ticker"].unique():
        ticker_reports = filtered[filtered["ticker"] == ticker]
        name = ticker_names.get(ticker, ticker)
        with st.expander(f"📁 {name} ({ticker}) — {len(ticker_reports)} document(s)", expanded=False):
            for _, row in ticker_reports.iterrows():
                type_emoji = {
                    "etats_financiers": "📊",
                    "rapport_annuel": "📘",
                    "rapport_semestriel": "📗",
                    "rapport_trimestriel": "📋",
                    "analyse": "🔬",
                }.get(row.get("report_type", ""), "📄")

                is_pdf = row["url"].endswith(".pdf")
                link_text = "📥 Telecharger PDF" if is_pdf else "🔗 Voir sur BRVM"

                col1, col2, col3 = st.columns([4, 1, 2])
                col1.write(f"{type_emoji} **{row['title']}**")
                col2.write(f"{int(row['fiscal_year'])}")
                col3.markdown(f"[{link_text}]({row['url']})")

    # Stats
    st.markdown("---")
    st.markdown("#### Couverture des rapports")
    all_tickers = load_tickers()
    covered = set(reports["ticker"].unique())
    not_covered = [t for t in all_tickers if t["ticker"] not in covered]

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        st.metric("Titres couverts", f"{len(covered)}/48")
    with col_s2:
        st.metric("Rapports 2024", f"{len(reports[reports['fiscal_year'] == 2024])}")

    if not_covered:
        with st.expander(f"⚪ {len(not_covered)} titres sans rapport disponible"):
            for t in not_covered:
                st.write(f"- {t['name']} ({t['ticker']})")


def _render_calendar():
    """Calendrier des publications attendues."""
    st.subheader("Calendrier des publications attendues")
    st.markdown(
        "Les entreprises BRVM publient generalement :\n"
        "- **Annuel** : fin mars / debut avril\n"
        "- **T1** : fin avril / debut mai\n"
        "- **S1** : fin aout / debut septembre\n"
        "- **T3** : fin octobre / debut novembre"
    )

    calendar = get_publication_calendar()
    if calendar.empty:
        st.info("Importez des donnees pour voir le calendrier.")
        return

    status_filter = st.multiselect(
        "Filtrer par statut",
        ["en_retard", "attendu_ce_mois", "a_venir"],
        default=["en_retard", "attendu_ce_mois"],
        format_func=lambda x: {"en_retard": "🔴 En retard", "attendu_ce_mois": "🟡 Ce mois", "a_venir": "🔵 A venir"}[x],
    )

    filtered = calendar[calendar["status"].isin(status_filter)]
    if filtered.empty:
        st.info("Aucune publication correspondante.")
        return

    status_emoji = {"a_venir": "🔵", "attendu_ce_mois": "🟡", "en_retard": "🔴"}

    for pub_type in ["annuel", "semestriel", "trimestriel"]:
        type_df = filtered[filtered["type"] == pub_type]
        if type_df.empty:
            continue
        st.markdown(f"#### {pub_type.capitalize()}")
        for _, row in type_df.iterrows():
            emoji = status_emoji.get(row["status"], "⚪")
            st.write(f"{emoji} **{row['company_name']}** ({row['ticker']}) — {row['period']} — {row['sector']}")


def _render_qualitative():
    """Notes qualitatives par titre."""
    st.subheader("Notes qualitatives")
    st.markdown("Ajoutez des notes d'analyse (strategie, position concurrentielle, risques, etc.) pour chaque titre.")

    tickers_data = load_tickers()
    options = [f"{t['ticker']} - {t['name']}" for t in tickers_data]
    selection = st.selectbox("Titre", options, key="quali_ticker")
    ticker = selection.split(" - ")[0]

    # Show existing notes
    notes = get_qualitative_notes(ticker)
    if not notes.empty:
        st.markdown(f"#### Notes existantes ({len(notes)})")
        for _, note in notes.iterrows():
            cat_emoji = {
                "strategie": "🎯", "concurrence": "⚔️", "risques": "⚠️",
                "gouvernance": "🏛️", "perspectives": "🔮", "dividendes": "💰",
                "general": "📝",
            }.get(note.get("category", ""), "📝")

            with st.container():
                col1, col2, col3 = st.columns([1, 5, 0.5])
                col1.write(f"{cat_emoji} **{note.get('category', 'general').capitalize()}**")
                col2.write(note["content"])
                if note.get("source"):
                    col2.caption(f"Source: {note['source']} | {note.get('note_date', '')}")
                if col3.button("🗑️", key=f"del_note_{note['id']}"):
                    delete_qualitative_note(note["id"])
                    st.rerun()
            st.markdown("---")

    # Add new note
    st.markdown("#### Ajouter une note")
    with st.form("add_note"):
        category = st.selectbox("Categorie", [
            "strategie", "concurrence", "risques", "gouvernance",
            "perspectives", "dividendes", "general",
        ])
        content = st.text_area(
            "Contenu de la note",
            placeholder="Ex: Plan strategique 2025-2028 axe sur la diversification data/fibre...",
            height=150,
        )
        col1, col2 = st.columns(2)
        source = col1.text_input("Source", placeholder="Ex: Rapport annuel 2024, Assemblee generale...")
        note_date = col2.date_input("Date")

        if st.form_submit_button("💾 Enregistrer la note"):
            if content.strip():
                save_qualitative_note(ticker, category, content.strip(), source, str(note_date))
                st.success("✅ Note enregistree")
                st.rerun()
            else:
                st.warning("Le contenu ne peut pas etre vide")


def _render_quarterly():
    """Saisie et consultation des donnees trimestrielles."""
    st.subheader("Donnees trimestrielles")

    tracked = list_tickers_with_fundamentals()
    tickers_data = load_tickers()

    if not tracked:
        options = [f"{t['ticker']} - {t['name']}" for t in tickers_data]
    else:
        options = [f"{t['ticker']} - {t['name']}" for t in tracked]

    selection = st.selectbox("Titre", options, key="quarterly_ticker")
    ticker = selection.split(" - ")[0]

    quarterly = get_quarterly_data(ticker)
    if not quarterly.empty:
        st.markdown("#### Historique trimestriel")
        display = quarterly.copy()
        display["revenue_fmt"] = display["revenue"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
        display["net_income_fmt"] = display["net_income"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
        st.dataframe(
            display[["fiscal_year", "quarter", "revenue_fmt", "net_income_fmt", "source"]].rename(columns={
                "fiscal_year": "Annee", "quarter": "Trimestre", "revenue_fmt": "CA (FCFA)",
                "net_income_fmt": "RN (FCFA)", "source": "Source",
            }),
            use_container_width=True, hide_index=True,
        )

    st.markdown("---")
    st.markdown("#### Ajouter des donnees trimestrielles")

    with st.form("quarterly_form"):
        col1, col2 = st.columns(2)
        fiscal_year = col1.number_input("Annee", value=2024, min_value=2020, max_value=2030)
        quarter = col2.selectbox("Trimestre", [1, 2, 3, 4])

        col3, col4, col5 = st.columns(3)
        revenue = col3.number_input("CA (FCFA)", value=0, min_value=0)
        net_income = col4.number_input("RN (FCFA)", value=0, min_value=0)
        ebit = col5.number_input("EBIT (FCFA)", value=0, min_value=0)

        source = st.text_input("Source", placeholder="Publication BRVM T1 2024...")
        notes = st.text_area("Notes", placeholder="Faits marquants du trimestre...")

        if st.form_submit_button("💾 Enregistrer"):
            save_quarterly_data({
                "ticker": ticker, "fiscal_year": fiscal_year, "quarter": quarter,
                "revenue": revenue if revenue > 0 else None,
                "net_income": net_income if net_income > 0 else None,
                "ebit": ebit if ebit > 0 else None,
                "source": source, "notes": notes,
            })
            st.success(f"✅ T{quarter} {fiscal_year} enregistre")
            st.rerun()
