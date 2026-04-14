"""
Page 7 : Assistant Investisseur
Questionnaire structuré qui guide l'utilisateur vers les meilleurs titres
selon son profil, budget, préférences sectorielles et objectifs.
"""

import streamlit as st
import pandas as pd

from config import load_tickers, SECTORS, CURRENCY
from data.storage import (
    get_all_fundamentals, get_fundamentals, get_cached_prices,
    save_investor_profile, get_investor_profile,
    get_all_stocks_for_analysis,
)
from analysis.fundamental import compute_ratios, format_ratio
from analysis.scoring import compute_hybrid_score, rank_stocks, recommend_for_profile
from utils.charts import radar_chart, gauge_chart, pie_chart, stars_display


def render():
    st.markdown('<div class="main-header">🤖 Assistant Investisseur</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Repondez aux questions ci-dessous pour obtenir des recommandations personnalisees</div>',
        unsafe_allow_html=True,
    )

    # Check if we have data (fundamentals OR market data)
    df_fund = get_all_fundamentals()
    all_stocks = get_all_stocks_for_analysis()

    if df_fund.empty and all_stocks.empty:
        st.warning("Aucune donnee disponible. Lancez l'enrichissement ou importez des fichiers Excel.")
        return

    # Use all_stocks if fundamentals are limited
    if len(all_stocks) > len(df_fund):
        df_fund = all_stocks

    # Load existing profile if any
    existing_profile = get_investor_profile()

    # ─── STEP MANAGEMENT ───
    if "assistant_step" not in st.session_state:
        st.session_state.assistant_step = 1
    if "assistant_profile" not in st.session_state:
        st.session_state.assistant_profile = existing_profile or {}

    step = st.session_state.assistant_step
    profile = st.session_state.assistant_profile

    # Progress bar
    total_steps = 6  # 5 questions + results
    st.progress(min(step / total_steps, 1.0), text=f"Etape {min(step, 5)}/5")

    # Reset button
    if step > 1:
        if st.button("🔄 Recommencer"):
            st.session_state.assistant_step = 1
            st.session_state.assistant_profile = {}
            st.rerun()

    st.markdown("---")

    # ─── STEP 1: RISK PROFILE ───
    if step == 1:
        st.subheader("1️⃣ Quel est votre profil de risque ?")
        st.markdown(
            "Cela determine le type de titres qui vous seront recommandes "
            "et le niveau de risque acceptable."
        )

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("### 🛡️ Prudent")
            st.markdown(
                "- Priorite a la **securite du capital**\n"
                "- Recherche de **dividendes reguliers**\n"
                "- Faible tolerance aux pertes\n"
                "- Titres solides a faible volatilite"
            )
            if st.button("Choisir Prudent", key="risk_prudent", use_container_width=True):
                profile["risk_profile"] = "prudent"
                st.session_state.assistant_step = 2
                st.rerun()

        with col2:
            st.markdown("### ⚖️ Equilibre")
            st.markdown(
                "- **Compromis** rendement / risque\n"
                "- Mix dividendes + **croissance moderate**\n"
                "- Tolerance moyenne aux fluctuations\n"
                "- Diversification sectorielle"
            )
            if st.button("Choisir Equilibre", key="risk_balanced", use_container_width=True):
                profile["risk_profile"] = "equilibre"
                st.session_state.assistant_step = 2
                st.rerun()

        with col3:
            st.markdown("### 🚀 Dynamique")
            st.markdown(
                "- Recherche de **forte croissance**\n"
                "- Accepte une **volatilite elevee**\n"
                "- Horizon long terme\n"
                "- Titres a fort potentiel d'appreciation"
            )
            if st.button("Choisir Dynamique", key="risk_dynamic", use_container_width=True):
                profile["risk_profile"] = "dynamique"
                st.session_state.assistant_step = 2
                st.rerun()

    # ─── STEP 2: INVESTMENT HORIZON ───
    elif step == 2:
        st.subheader("2️⃣ Quel est votre horizon d'investissement ?")
        _show_profile_badge(profile)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("### ⏱️ Court terme")
            st.markdown("**Moins de 6 mois**\n\nRecherche de gains rapides sur momentum")
            if st.button("Court terme", key="h_short", use_container_width=True):
                profile["horizon"] = "court"
                st.session_state.assistant_step = 3
                st.rerun()

        with col2:
            st.markdown("### 📅 Moyen terme")
            st.markdown("**6 a 18 mois**\n\nEquilibre entre timing et patience")
            if st.button("Moyen terme", key="h_medium", use_container_width=True):
                profile["horizon"] = "moyen"
                st.session_state.assistant_step = 3
                st.rerun()

        with col3:
            st.markdown("### 🏦 Long terme")
            st.markdown("**Plus de 18 mois**\n\nAccumulation et rendement compose")
            if st.button("Long terme", key="h_long", use_container_width=True):
                profile["horizon"] = "long"
                st.session_state.assistant_step = 3
                st.rerun()

    # ─── STEP 3: BUDGET ───
    elif step == 3:
        st.subheader("3️⃣ Quel montant souhaitez-vous investir ?")
        _show_profile_badge(profile)

        budget = st.number_input(
            f"Budget disponible ({CURRENCY})",
            min_value=100_000,
            max_value=1_000_000_000,
            value=int(profile.get("budget", 5_000_000)),
            step=500_000,
            format="%d",
        )
        st.markdown(f"**{budget:,.0f} {CURRENCY}**")

        if st.button("Continuer ➡️", key="budget_next"):
            profile["budget"] = budget
            st.session_state.assistant_step = 4
            st.rerun()

    # ─── STEP 4: SECTOR PREFERENCES ───
    elif step == 4:
        st.subheader("4️⃣ Quels secteurs vous interessent ?")
        _show_profile_badge(profile)

        st.markdown("Selectionnez les secteurs dans lesquels vous souhaitez investir. "
                     "Laissez vide pour considerer tous les secteurs.")

        available_sectors = sorted(df_fund["sector"].dropna().unique().tolist())

        selected_sectors = st.multiselect(
            "Secteurs preferes",
            available_sectors,
            default=profile.get("preferred_sectors", []),
        )

        # Specific tickers
        st.markdown("##### Titres specifiques (optionnel)")
        tracked_tickers = df_fund["ticker"].unique().tolist()
        tickers_data = load_tickers()
        ticker_options = [
            f"{t['ticker']} - {t['name']}"
            for t in tickers_data
            if t["ticker"] in tracked_tickers
        ]

        preferred = st.multiselect(
            "Titres a privilegier",
            ticker_options,
            default=[],
            help="Ces titres seront prioritaires dans les recommandations",
        )
        excluded = st.multiselect(
            "Titres a exclure",
            ticker_options,
            default=[],
            help="Ces titres ne seront jamais recommandes",
        )

        if st.button("Continuer ➡️", key="sector_next"):
            profile["preferred_sectors"] = selected_sectors
            profile["preferred_tickers"] = [p.split(" - ")[0] for p in preferred]
            profile["excluded_tickers"] = [e.split(" - ")[0] for e in excluded]
            st.session_state.assistant_step = 5
            st.rerun()

    # ─── STEP 5: OBJECTIVE ───
    elif step == 5:
        st.subheader("5️⃣ Quel est votre objectif principal ?")
        _show_profile_badge(profile)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("### 💰 Rendement")
            st.markdown(
                "Maximiser les **dividendes**\n\n"
                "- Dividend Yield eleve\n"
                "- Payout soutenable\n"
                "- Revenus reguliers"
            )
            if st.button("Rendement", key="obj_yield", use_container_width=True):
                profile["objective"] = "rendement"
                _finalize(profile, df_fund)

        with col2:
            st.markdown("### 📈 Croissance")
            st.markdown(
                "Maximiser l'**appreciation du capital**\n\n"
                "- Croissance du CA et RN\n"
                "- PER raisonnable\n"
                "- Momentum haussier"
            )
            if st.button("Croissance", key="obj_growth", use_container_width=True):
                profile["objective"] = "croissance"
                _finalize(profile, df_fund)

        with col3:
            st.markdown("### 🎯 Mixte")
            st.markdown(
                "**Equilibre** dividendes + croissance\n\n"
                "- Score hybride global\n"
                "- Diversification\n"
                "- Rendement + potentiel"
            )
            if st.button("Mixte", key="obj_mixed", use_container_width=True):
                profile["objective"] = "mixte"
                _finalize(profile, df_fund)

    # ─── STEP 6: RESULTS ───
    elif step >= 6:
        _show_results(profile, df_fund)


def _finalize(profile, df_fund):
    """Save profile and move to results."""
    save_investor_profile(profile)
    st.session_state.assistant_profile = profile
    st.session_state.assistant_step = 6
    st.rerun()


def _show_profile_badge(profile):
    """Affiche un résumé du profil en cours de construction."""
    badges = []
    if "risk_profile" in profile:
        emoji = {"prudent": "🛡️", "equilibre": "⚖️", "dynamique": "🚀"}.get(profile["risk_profile"], "")
        badges.append(f"{emoji} {profile['risk_profile'].capitalize()}")
    if "horizon" in profile:
        badges.append(f"📅 {profile['horizon'].capitalize()} terme")
    if "budget" in profile:
        badges.append(f"💰 {profile['budget']:,.0f} {CURRENCY}")

    if badges:
        st.markdown(" | ".join(f"**{b}**" for b in badges))


def _show_results(profile, df_fund):
    """Affiche les résultats et recommandations personnalisées."""
    st.subheader("🎯 Vos recommandations personnalisees")
    _show_profile_badge(profile)
    st.markdown("---")

    # Build ranking
    stocks_data = []
    for _, row in df_fund.iterrows():
        data = row.to_dict()
        ticker = data.get("ticker", "")
        price_df = get_cached_prices(ticker)
        stocks_data.append({
            "ticker": ticker,
            "name": data.get("company_name", ""),
            "fundamentals": data,
            "price_df": price_df,
        })

    with st.spinner("Analyse en cours..."):
        ranked = rank_stocks(stocks_data)

    if ranked.empty:
        st.error("Impossible de generer les recommandations. Verifiez les donnees importees.")
        return

    # Get personalized recommendations
    recommendations = recommend_for_profile(ranked, profile)

    if not recommendations:
        st.warning(
            "Aucun titre ne correspond a vos criteres avec le niveau de confiance requis. "
            "Essayez d'elargir vos preferences sectorielles ou d'ajuster votre profil de risque."
        )
        # Show best available anyway
        st.markdown("### Meilleurs titres disponibles (tous criteres)")
        _display_ranking_table(ranked.head(5))
        return

    # ─── TOP 3 RECOMMENDATIONS ───
    st.markdown(f"### 🏆 Top {len(recommendations)} titres pour votre profil")

    for i, reco in enumerate(recommendations):
        medal = ["🥇", "🥈", "🥉"][i] if i < 3 else f"#{i+1}"

        with st.container():
            st.markdown(f"## {medal} {reco['name']} ({reco['ticker']})")

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Score hybride", f"{reco['hybrid_score']:.0f}/100")
            col2.metric("Verdict", f"{stars_display(reco['stars'])} {reco['verdict']}")
            col3.metric("Secteur", reco["sector"])
            col4.metric("Prix actuel", f"{reco['price']:,.0f} {CURRENCY}")

            # Ratios clés
            col5, col6, col7, col8 = st.columns(4)
            col5.metric("Dividend Yield", format_ratio(reco.get("dividend_yield")))
            col6.metric("PER", format_ratio(reco.get("per"), "decimal"))
            col7.metric("ROE", format_ratio(reco.get("roe")))

            # Allocation
            weight_pct = reco["weight"] * 100
            col8.metric("Poids suggere", f"{weight_pct:.0f}%")

            st.markdown(
                f"**Allocation**: {reco['allocated_budget']:,.0f} {CURRENCY} → "
                f"**{reco['nb_shares']} actions** a {reco['price']:,.0f} {CURRENCY} = "
                f"**{reco['actual_amount']:,.0f} {CURRENCY}**"
            )
            st.markdown("---")

    # ─── ALLOCATION SUMMARY ───
    st.subheader("📊 Allocation recommandee")

    col_alloc, col_pie = st.columns([1, 1])

    with col_alloc:
        total_allocated = sum(r["actual_amount"] for r in recommendations)
        remaining = profile.get("budget", 0) - total_allocated

        for reco in recommendations:
            st.write(
                f"**{reco['name']}**: {reco['nb_shares']} actions x {reco['price']:,.0f} = "
                f"**{reco['actual_amount']:,.0f} {CURRENCY}** ({reco['weight']*100:.0f}%)"
            )

        st.markdown("---")
        st.write(f"**Total investi**: {total_allocated:,.0f} {CURRENCY}")
        st.write(f"**Solde restant**: {remaining:,.0f} {CURRENCY}")

        # Projected dividend income
        total_div = 0
        for reco in recommendations:
            fund = get_fundamentals(reco["ticker"])
            if fund and fund.get("dps"):
                total_div += fund["dps"] * reco["nb_shares"]

        if total_div > 0:
            div_yield = (total_div / total_allocated * 100) if total_allocated > 0 else 0
            st.success(
                f"💰 **Dividendes projetes**: {total_div:,.0f} {CURRENCY}/an "
                f"(rendement: {div_yield:.1f}%)"
            )

    with col_pie:
        labels = [r["name"] for r in recommendations]
        values = [r["actual_amount"] for r in recommendations]
        if remaining > 0:
            labels.append("Cash restant")
            values.append(remaining)
        fig = pie_chart(labels, values, "Repartition du budget")
        st.plotly_chart(fig, use_container_width=True)

    # ─── FULL RANKING ───
    st.markdown("---")
    with st.expander("📋 Classement complet de tous les titres"):
        _display_ranking_table(ranked)


def _display_ranking_table(ranked_df):
    """Affiche un tableau de classement."""
    if ranked_df.empty:
        st.info("Aucune donnee")
        return

    display = ranked_df.copy()
    display["score_fmt"] = display["hybrid_score"].apply(lambda x: f"{x:.0f}/100")
    display["stars_fmt"] = display["stars"].apply(lambda x: stars_display(x))
    display["yield_fmt"] = display["dividend_yield"].apply(lambda x: f"{x:.2%}" if pd.notna(x) else "N/A")
    display["per_fmt"] = display["per"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    display["roe_fmt"] = display["roe"].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
    display["price_fmt"] = display["price"].apply(lambda x: f"{x:,.0f}" if x else "N/A")

    show_cols = {
        "ticker": "Ticker",
        "name": "Nom",
        "sector": "Secteur",
        "price_fmt": "Prix",
        "score_fmt": "Score",
        "verdict": "Verdict",
        "stars_fmt": "Rating",
        "yield_fmt": "Yield",
        "per_fmt": "PER",
        "roe_fmt": "ROE",
        "trend": "Tendance",
    }

    st.dataframe(
        display[list(show_cols.keys())].rename(columns=show_cols),
        use_container_width=True,
        hide_index=True,
    )
