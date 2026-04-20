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
from utils.nav import ticker_analyze_button
from utils.ui_helpers import section_heading
from data.db import read_sql_df


@st.cache_data(ttl=300, show_spinner=False)
def _load_snapshot_ranked_df() -> pd.DataFrame:
    """Lecture rapide depuis scoring_snapshot (pre-calcule, cache 5 min).
    Remplace rank_stocks() qui recalculait les 48 scores a chaque rerun.
    Merge avec les fondamentaux pour avoir yield / PER / ROE."""
    try:
        snap = read_sql_df(
            "SELECT ticker, company_name, sector, price, hybrid_score, "
            "fundamental_score, technical_score, verdict, stars, trend "
            "FROM scoring_snapshot"
        )
    except Exception:
        snap = pd.DataFrame()
    if snap.empty:
        return pd.DataFrame()
    snap = snap.rename(columns={"company_name": "name"})

    # Merge avec fondamentaux (yield, PER, ROE) si disponibles
    try:
        from data.storage import get_all_stocks_for_analysis
        fund = get_all_stocks_for_analysis()
        if not fund.empty:
            keep = ["ticker"]
            for col in ["dividend_yield", "per", "roe", "dps"]:
                if col in fund.columns:
                    keep.append(col)
            if "dps" in fund.columns and "price" in fund.columns:
                fund = fund.copy()
                fund["dividend_yield"] = fund.apply(
                    lambda r: (r["dps"] / r["price"]) if (r.get("dps") and r.get("price")) else None,
                    axis=1,
                )
                if "dividend_yield" not in keep:
                    keep.append("dividend_yield")
            snap = snap.merge(fund[keep].drop_duplicates("ticker"), on="ticker", how="left")
    except Exception:
        pass

    # Colonnes attendues par recommend_for_profile
    for c in ("dividend_yield", "per", "roe"):
        if c not in snap.columns:
            snap[c] = None
    return snap.sort_values("hybrid_score", ascending=False).reset_index(drop=True)


def _profile_signature(profile: dict) -> str:
    """Signature stable du profil pour invalider le cache recommandations."""
    import json as _j
    keys = ["risk_profile", "horizon", "budget", "objective",
            "preferred_sectors", "excluded_tickers", "preferred_tickers"]
    return _j.dumps({k: profile.get(k) for k in keys}, sort_keys=True, default=str)


def _render_choice_card(title: str, subtitle: str, tags, selected: bool = False):
    """Carte de choix editorial v3 (bordure + titre + sous-titre + tags)."""
    border = "var(--primary)" if selected else "var(--border)"
    bg = "var(--primary-bg)" if selected else "var(--bg-elev)"
    tags_html = "".join(
        f"<span style='display:inline-block;background:var(--bg-sunken);"
        f"color:var(--ink-2);padding:3px 8px;border-radius:4px;"
        f"font-size:10.5px;font-weight:500;letter-spacing:0.02em;"
        f"text-transform:uppercase;margin:2px 4px 2px 0;'>{t}</span>"
        for t in (tags or [])
    )
    st.markdown(
        f"<div style='background:{bg};border:2px solid {border};"
        f"border-radius:10px;padding:18px 20px;min-height:170px;'>"
        f"<div style='font-size:18px;font-weight:600;color:var(--ink);"
        f"letter-spacing:-0.01em;margin-bottom:4px;'>{title}</div>"
        f"<div style='font-size:13px;color:var(--ink-3);margin-bottom:14px;'>{subtitle}</div>"
        f"<div style='margin-bottom:4px;'>{tags_html}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )


def render():
    # Data availability check
    df_fund = get_all_fundamentals()
    all_stocks = get_all_stocks_for_analysis()
    if df_fund.empty and all_stocks.empty:
        st.warning("Aucune donnée disponible. Lancez l'enrichissement ou importez des fichiers Excel.")
        return
    if len(all_stocks) > len(df_fund):
        df_fund = all_stocks

    existing_profile = get_investor_profile()
    if "assistant_step" not in st.session_state:
        st.session_state.assistant_step = 1
    if "assistant_profile" not in st.session_state:
        st.session_state.assistant_profile = existing_profile or {}

    step = st.session_state.assistant_step
    profile = st.session_state.assistant_profile
    total_steps = 5
    step_shown = min(step, total_steps)

    # ═══════════════════════════════════════════════════════════════════
    # Header : title + subtitle + "Étape X / 5" (aligné à droite)
    # ═══════════════════════════════════════════════════════════════════
    col_h, col_step = st.columns([5, 1])
    with col_h:
        st.title("Assistant Investisseur")
        st.caption(f"{total_steps} étapes · recommandations personnalisées")
    with col_step:
        st.markdown(
            f"<div style='text-align:right;padding-top:20px;color:var(--ink-3);"
            f"font-size:12.5px;'>Étape {step_shown} / {total_steps}</div>",
            unsafe_allow_html=True,
        )

    # Progress bar (fine, primary color)
    pct = step_shown / total_steps
    st.markdown(
        f"<div style='height:3px;background:var(--bg-sunken);border-radius:999px;"
        f"margin:4px 0 18px 0;overflow:hidden;'>"
        f"<div style='width:{pct*100:.0f}%;height:100%;background:var(--primary);'></div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Reset button (discrète, droite) — visible dès l'étape 2 et sur la page résultats
    if step > 1:
        col_r1, col_r2 = st.columns([5, 1])
        with col_r2:
            if st.button("Recommencer", key="assistant_reset",
                           use_container_width=True):
                st.session_state.assistant_step = 1
                st.session_state.assistant_profile = {}
                st.session_state.pop("_assistant_reco_cache", None)
                st.rerun()

    # ─── STEP 1 : RISK PROFILE — 3 cards éditoriales ──────────────────
    if step == 1:
        st.markdown(
            "<div style='font-size:17px;font-weight:600;color:var(--ink);"
            "letter-spacing:-0.01em;margin-bottom:4px;'>"
            "Quel est votre profil de risque ?</div>"
            "<div style='font-size:13px;color:var(--ink-3);margin-bottom:18px;'>"
            "Cela détermine le type de titres recommandés et le niveau de risque acceptable."
            "</div>",
            unsafe_allow_html=True,
        )

        current_choice = profile.get("risk_profile")

        def _profile_card(key_val, title, subtitle, tags, selected, btn_key):
            selected_border = "var(--primary)" if selected else "var(--border)"
            selected_bg = "var(--primary-bg)" if selected else "var(--bg-elev)"
            # Tags en ligne
            tags_html = "".join(
                f"<span style='display:inline-block;background:var(--bg-sunken);"
                f"color:var(--ink-2);padding:3px 8px;border-radius:4px;"
                f"font-size:10.5px;font-weight:500;letter-spacing:0.02em;"
                f"text-transform:uppercase;margin:2px 4px 2px 0;'>{t}</span>"
                for t in tags
            )
            st.markdown(
                f"<div style='background:{selected_bg};border:2px solid {selected_border};"
                f"border-radius:10px;padding:18px 20px;min-height:190px;'>"
                f"<div style='font-size:18px;font-weight:600;color:var(--ink);"
                f"letter-spacing:-0.01em;margin-bottom:4px;'>{title}</div>"
                f"<div style='font-size:13px;color:var(--ink-3);margin-bottom:14px;'>{subtitle}</div>"
                f"<div style='margin-bottom:14px;'>{tags_html}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

        col1, col2, col3 = st.columns(3)
        with col1:
            _profile_card(
                "prudent", "Prudent",
                "Sécurité du capital, dividendes réguliers",
                ["Faible volatilité", "Yield > 5%", "Banques / util."],
                selected=(current_choice == "prudent"),
                btn_key="risk_prudent",
            )
            label = "✓ Sélectionné" if current_choice == "prudent" else "Choisir"
            btype = "primary" if current_choice == "prudent" else "secondary"
            if st.button(label, key="risk_prudent", type=btype, use_container_width=True):
                profile["risk_profile"] = "prudent"
                st.session_state.assistant_step = 2
                st.rerun()

        with col2:
            _profile_card(
                "equilibre", "Équilibré",
                "Mix dividendes + croissance modérée",
                ["Yield 3-6%", "Diversification", "Horizon 3-5 ans"],
                selected=(current_choice == "equilibre"),
                btn_key="risk_balanced",
            )
            label = "✓ Sélectionné" if current_choice == "equilibre" else "Choisir"
            btype = "primary" if current_choice == "equilibre" else "secondary"
            if st.button(label, key="risk_balanced", type=btype, use_container_width=True):
                profile["risk_profile"] = "equilibre"
                st.session_state.assistant_step = 2
                st.rerun()

        with col3:
            _profile_card(
                "dynamique", "Dynamique",
                "Forte croissance, volatilité acceptée",
                ["Momentum", "Horizon long", "Small caps"],
                selected=(current_choice == "dynamique"),
                btn_key="risk_dynamic",
            )
            label = "✓ Sélectionné" if current_choice == "dynamique" else "Choisir"
            btype = "primary" if current_choice == "dynamique" else "secondary"
            if st.button(label, key="risk_dynamic", type=btype, use_container_width=True):
                profile["risk_profile"] = "dynamique"
                st.session_state.assistant_step = 2
                st.rerun()

    # ─── STEP 2: INVESTMENT HORIZON — 3 cards v3 ──────────────────────
    elif step == 2:
        st.markdown(
            "<div style='font-size:17px;font-weight:600;color:var(--ink);"
            "letter-spacing:-0.01em;margin-bottom:4px;'>"
            "Quel est votre horizon d'investissement ?</div>"
            "<div style='font-size:13px;color:var(--ink-3);margin-bottom:18px;'>"
            "La durée pendant laquelle vous prévoyez de détenir les titres."
            "</div>",
            unsafe_allow_html=True,
        )
        _show_profile_badge(profile)

        current = profile.get("horizon")
        col1, col2, col3 = st.columns(3)
        for col, (key, title, subtitle, tags) in zip(
            [col1, col2, col3],
            [
                ("court", "Court terme", "Moins de 6 mois",
                 ["< 6 mois", "Momentum", "Sortie rapide"]),
                ("moyen", "Moyen terme", "6 à 18 mois",
                 ["6-18 mois", "Équilibre", "Timing + patience"]),
                ("long", "Long terme", "Plus de 18 mois",
                 ["> 18 mois", "Accumulation", "Composé"]),
            ],
        ):
            with col:
                _render_choice_card(title, subtitle, tags, selected=(current == key))
                is_sel = (current == key)
                label = "✓ Sélectionné" if is_sel else "Choisir"
                btype = "primary" if is_sel else "secondary"
                if st.button(label, key=f"h_{key}", type=btype,
                              use_container_width=True):
                    profile["horizon"] = key
                    st.session_state.assistant_step = 3
                    st.rerun()

    # ─── STEP 3: BUDGET ───
    elif step == 3:
        st.markdown(
            "<div style='font-size:17px;font-weight:600;color:var(--ink);"
            "letter-spacing:-0.01em;margin-bottom:4px;'>"
            "Quel montant souhaitez-vous investir ?</div>"
            "<div style='font-size:13px;color:var(--ink-3);margin-bottom:18px;'>"
            "Montant total disponible, servira à calculer la répartition suggérée."
            "</div>",
            unsafe_allow_html=True,
        )
        _show_profile_badge(profile)

        col_input, _ = st.columns([2, 3])
        with col_input:
            budget = st.number_input(
                f"Budget ({CURRENCY})",
                min_value=100_000, max_value=1_000_000_000,
                value=int(profile.get("budget", 5_000_000)),
                step=500_000, format="%d",
                label_visibility="collapsed",
            )
            st.markdown(
                f"<div style='font-size:28px;font-weight:600;letter-spacing:-0.02em;"
                f"color:var(--ink);margin-top:8px;font-variant-numeric:tabular-nums;'>"
                f"{budget:,.0f} <span style='color:var(--ink-3);font-size:14px;font-weight:400;'>"
                f"{CURRENCY}</span></div>",
                unsafe_allow_html=True,
            )

        if st.button("Continuer", key="budget_next", type="primary"):
            profile["budget"] = budget
            st.session_state.assistant_step = 4
            st.rerun()

    # ─── STEP 4: SECTOR PREFERENCES ───
    elif step == 4:
        st.markdown(
            "<div style='font-size:17px;font-weight:600;color:var(--ink);"
            "letter-spacing:-0.01em;margin-bottom:4px;'>"
            "Quels secteurs vous intéressent ?</div>"
            "<div style='font-size:13px;color:var(--ink-3);margin-bottom:18px;'>"
            "Laissez vide pour considérer tous les secteurs."
            "</div>",
            unsafe_allow_html=True,
        )
        _show_profile_badge(profile)

        available_sectors = sorted(df_fund["sector"].dropna().unique().tolist())
        st.markdown(
            "<div class='label-xs' style='margin-bottom:4px;'>Secteurs préférés</div>",
            unsafe_allow_html=True,
        )
        selected_sectors = st.multiselect(
            "Secteurs", available_sectors,
            default=profile.get("preferred_sectors", []),
            label_visibility="collapsed",
            placeholder="Tous les secteurs",
        )

        st.markdown(
            "<div class='label-xs' style='margin:14px 0 4px 0;'>"
            "Titres à privilégier <span style='text-transform:none;"
            "font-weight:400;'>(optionnel)</span></div>",
            unsafe_allow_html=True,
        )
        tracked_tickers = df_fund["ticker"].unique().tolist()
        tickers_data = load_tickers()
        ticker_options = [
            f"{t['ticker']} - {t['name']}"
            for t in tickers_data
            if t["ticker"] in tracked_tickers
        ]
        preferred = st.multiselect(
            "Privilégier", ticker_options, default=[],
            label_visibility="collapsed",
            placeholder="Aucun titre privilégié",
        )

        st.markdown(
            "<div class='label-xs' style='margin:14px 0 4px 0;'>"
            "Titres à exclure <span style='text-transform:none;"
            "font-weight:400;'>(optionnel)</span></div>",
            unsafe_allow_html=True,
        )
        excluded = st.multiselect(
            "Exclure", ticker_options, default=[],
            label_visibility="collapsed",
            placeholder="Aucun titre exclu",
        )

        if st.button("Continuer", key="sector_next", type="primary"):
            profile["preferred_sectors"] = selected_sectors
            profile["preferred_tickers"] = [p.split(" - ")[0] for p in preferred]
            profile["excluded_tickers"] = [e.split(" - ")[0] for e in excluded]
            st.session_state.assistant_step = 5
            st.rerun()

    # ─── STEP 5: OBJECTIVE — 3 cards v3 ──────────────────────────────
    elif step == 5:
        st.markdown(
            "<div style='font-size:17px;font-weight:600;color:var(--ink);"
            "letter-spacing:-0.01em;margin-bottom:4px;'>"
            "Quel est votre objectif principal ?</div>"
            "<div style='font-size:13px;color:var(--ink-3);margin-bottom:18px;'>"
            "Cela déterminera la pondération des critères de sélection."
            "</div>",
            unsafe_allow_html=True,
        )
        _show_profile_badge(profile)

        current = profile.get("objective")
        col1, col2, col3 = st.columns(3)
        objectives = [
            ("rendement", "Rendement",
             "Maximiser les dividendes",
             ["Yield élevé", "Payout sain", "Revenus"]),
            ("croissance", "Croissance",
             "Maximiser l'appréciation du capital",
             ["Croissance CA/RN", "PER raisonnable", "Momentum"]),
            ("mixte", "Mixte",
             "Équilibre dividendes + croissance",
             ["Score hybride", "Diversifié", "Rendement + potentiel"]),
        ]
        for col, (key, title, subtitle, tags) in zip([col1, col2, col3], objectives):
            with col:
                _render_choice_card(title, subtitle, tags, selected=(current == key))
                is_sel = (current == key)
                label = "✓ Sélectionné" if is_sel else "Choisir"
                btype = "primary" if is_sel else "secondary"
                if st.button(label, key=f"obj_{key}", type=btype,
                              use_container_width=True):
                    profile["objective"] = key
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
    """Affiche le résumé du profil : 1 chip étiqueté par étape (risque / horizon /
    budget / secteurs / objectif). Palette v3 unifiée — tous en ton neutre éditorial,
    seule l'étiquette varie pour distinguer l'origine de chaque chip."""
    chips = []  # (label_etape, valeur)

    if "risk_profile" in profile:
        risk_map = {"prudent": "Prudent", "equilibre": "Équilibré", "dynamique": "Dynamique"}
        chips.append(("Risque", risk_map.get(profile["risk_profile"],
                                              profile["risk_profile"].capitalize())))

    if "horizon" in profile:
        hz_map = {"court": "Court terme", "moyen": "Moyen terme", "long": "Long terme"}
        chips.append(("Horizon", hz_map.get(profile["horizon"],
                                              f"{profile['horizon'].capitalize()} terme")))

    if "budget" in profile:
        chips.append(("Budget", f"{profile['budget']:,.0f} {CURRENCY}"))

    sectors = profile.get("preferred_sectors") or []
    if sectors:
        if len(sectors) <= 2:
            sec_val = " · ".join(sectors)
        else:
            sec_val = f"{sectors[0]} +{len(sectors) - 1}"
        chips.append(("Secteurs", sec_val))
    elif "preferred_sectors" in profile:
        chips.append(("Secteurs", "Tous"))

    if "objective" in profile:
        obj_map = {"rendement": "Rendement", "croissance": "Croissance", "mixte": "Mixte"}
        chips.append(("Objectif", obj_map.get(profile["objective"],
                                                profile["objective"].capitalize())))

    if not chips:
        return

    chips_html = "".join(
        f"<span style='display:inline-flex;align-items:center;gap:6px;"
        f"background:var(--bg-elev);border:1px solid var(--border);"
        f"padding:4px 10px 4px 8px;border-radius:6px;margin:0 6px 6px 0;"
        f"font-size:12px;'>"
        f"<span style='font-size:10px;color:var(--ink-3);text-transform:uppercase;"
        f"letter-spacing:0.06em;font-weight:600;'>{etape}</span>"
        f"<span style='color:var(--ink);font-weight:500;'>{val}</span>"
        f"</span>"
        for etape, val in chips
    )
    st.markdown(
        f"<div style='margin:6px 0 14px 0;display:flex;flex-wrap:wrap;'>{chips_html}</div>",
        unsafe_allow_html=True,
    )


def _show_results(profile, df_fund):
    """Affiche les résultats et recommandations personnalisées."""
    section_heading("Vos recommandations personnalisées", spacing="loose")
    _show_profile_badge(profile)

    # ── Lecture snapshot pre-calculé (5 min cache) au lieu de 48 recalculs ──
    ranked = _load_snapshot_ranked_df()
    if ranked.empty:
        st.error(
            "Aucun snapshot de scoring disponible. Lancez le rebuild depuis la "
            "page Admin avant d'utiliser l'Assistant."
        )
        return

    # ── Mémoïsation des recommandations par signature de profil ──
    sig = _profile_signature(profile)
    cache_key = "_assistant_reco_cache"
    cache = st.session_state.get(cache_key) or {}
    if cache.get("sig") == sig and cache.get("recos") is not None:
        recommendations = cache["recos"]
    else:
        with st.spinner("Calcul des recommandations…"):
            recommendations = recommend_for_profile(ranked, profile)
        st.session_state[cache_key] = {"sig": sig, "recos": recommendations}

    if not recommendations:
        st.warning(
            "Aucun titre ne correspond à vos critères avec le niveau de confiance requis. "
            "Essayez d'élargir vos préférences sectorielles ou d'ajuster votre profil de risque."
        )
        # Show best available anyway
        st.markdown("### Meilleurs titres disponibles (tous critères)")
        _display_ranking_table(ranked.head(5))
        return

    # ─── TOP 3 RECOMMENDATIONS ───
    section_heading(f"Top {len(recommendations)} titres pour votre profil", spacing="loose")

    for i, reco in enumerate(recommendations):
        rank = f"{i+1}"

        with st.container():
            col_title, col_btn = st.columns([6, 1])
            with col_title:
                st.markdown(
                    f"<div style='font-size:20px;font-weight:600;color:var(--ink);"
                    f"letter-spacing:-0.01em;margin-top:8px;'>"
                    f"<span style='color:var(--ink-3);margin-right:10px;'>#{rank}</span>"
                    f"{reco['name']} <span class='ticker'>{reco['ticker']}</span></div>",
                    unsafe_allow_html=True,
                )
            with col_btn:
                st.markdown('<div style="padding-top:1.5rem"></div>', unsafe_allow_html=True)
                ticker_analyze_button(
                    reco["ticker"], label="Analyser",
                    key=f"assistant_goto_{reco['ticker']}",
                    use_container_width=True,
                )

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
            col8.metric("Poids suggéré", f"{weight_pct:.0f}%")

            st.markdown(
                f"**Allocation**: {reco['allocated_budget']:,.0f} {CURRENCY} → "
                f"**{reco['nb_shares']} actions** a {reco['price']:,.0f} {CURRENCY} = "
                f"**{reco['actual_amount']:,.0f} {CURRENCY}**"
            )
            st.markdown("---")

    # ─── ALLOCATION SUMMARY ───
    section_heading("Allocation recommandée", spacing="loose")

    col_alloc, col_pie = st.columns([1, 1])

    with col_alloc:
        total_allocated = sum(r["actual_amount"] for r in recommendations)
        remaining = profile.get("budget", 0) - total_allocated

        for reco in recommendations:
            col_txt, col_btn = st.columns([5, 1])
            with col_txt:
                st.write(
                    f"**{reco['name']}**: {reco['nb_shares']} actions x {reco['price']:,.0f} = "
                    f"**{reco['actual_amount']:,.0f} {CURRENCY}** ({reco['weight']*100:.0f}%)"
                )
            with col_btn:
                ticker_analyze_button(
                    reco["ticker"], label=None,
                    key=f"assistant_alloc_{reco['ticker']}",
                )

        st.markdown("---")
        st.write(f"**Total investi**: {total_allocated:,.0f} {CURRENCY}")
        st.write(f"**Solde restant**: {remaining:,.0f} {CURRENCY}")

        # Projected dividend income (dps deja dans ranked, pas de requete DB)
        dps_map = {}
        if "dps" in ranked.columns:
            dps_map = dict(zip(ranked["ticker"], ranked["dps"]))
        total_div = 0
        for reco in recommendations:
            dps = dps_map.get(reco["ticker"]) or 0
            if dps:
                total_div += dps * reco["nb_shares"]

        if total_div > 0:
            div_yield = (total_div / total_allocated * 100) if total_allocated > 0 else 0
            st.markdown(
                f"<div style='border:1px solid var(--border);border-left:4px solid var(--primary);"
                f"border-radius:8px;padding:10px 14px;background:var(--bg-elev);margin-top:10px;"
                f"font-size:13px;'>"
                f"<b>Dividendes projetés</b> : {total_div:,.0f} {CURRENCY}/an "
                f"(rendement {div_yield:.1f}%)</div>",
                unsafe_allow_html=True,
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
    with st.expander("Classement complet de tous les titres"):
        _display_ranking_table(ranked)


def _display_ranking_table(ranked_df):
    """Affiche un tableau de classement."""
    if ranked_df.empty:
        st.info("Aucune donnée")
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
