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
    get_company_profile, get_company_news, get_connection,
    get_qualitative_notes, save_qualitative_note, delete_qualitative_note,
    save_signal_snapshots, save_recommendation_snapshot,
)
from data.db import read_sql_df
from data.scraper import fetch_historical_prices, fetch_historical_prices_page
from analysis.fundamental import (
    compute_ratios, format_ratio,
    get_sector_benchmarks, compare_to_sector,
)
from analysis.technical import compute_all_indicators, detect_trend, detect_support_resistance, generate_signals
from analysis.scoring import compute_hybrid_score
from utils.charts import candlestick_chart, gauge_chart, flag_badge, stars_display
from utils.auth import is_admin
from utils.ui_helpers import delta as _delta_html, tag as _tag_html, ticker as _ticker_html

import json as _json


def _verdict_tone(verdict: str) -> str:
    """Map verdict BRVM → tone du kit design v2."""
    v = (verdict or "").upper()
    if "ACHAT" in v:
        return "up"
    if "VENTE" in v:
        return "down"
    if "CONSERVER" in v:
        return "ocre"
    return "neutral"


@st.cache_data(ttl=300, show_spinner=False)
def _load_one_scoring_snapshot(ticker: str) -> dict:
    """Lit la ligne scoring_snapshot pour ce ticker (si existe)."""
    try:
        df = read_sql_df(
            "SELECT hybrid_score, fundamental_score, technical_score, "
            "verdict, stars, trend, signals_json, consolidated_json "
            "FROM scoring_snapshot WHERE ticker = ?",
            params=(ticker,),
        )
        if df.empty:
            return {}
        row = df.iloc[0].to_dict()
        return row
    except Exception:
        return {}


def render():
    st.markdown('<div class="main-header">🔍 Analyse d\'un Titre</div>', unsafe_allow_html=True)

    # --- Sélection du titre (seulement ceux avec données) ---
    analyzable = get_analyzable_tickers()

    if not analyzable:
        st.warning("Aucune donnée disponible. Lancez l'enrichissement depuis le Dashboard ou importez des fichiers Excel.")
        return

    col_select, col_import = st.columns([3, 1])

    with col_select:
        all_options = [f"{t['ticker']} - {t['name']}" + (" 📊" if t.get("has_fundamentals") else " 📈") for t in analyzable]

        # If another page requested a specific ticker, preselect it
        default_index = 0
        target_ticker = st.session_state.pop("target_ticker", None)
        if target_ticker:
            for i, opt in enumerate(all_options):
                if opt.split(" - ")[0] == target_ticker:
                    default_index = i
                    break

        selection = st.selectbox(
            "Choisir un titre (📊=fondamentaux 📈=marche)",
            all_options, index=default_index,
            key="p2_ticker_select",
        )
        selected_ticker = selection.split(" - ")[0]

    with col_import:
        if is_admin():
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
                    st.success(f"✅ {data['company_name']} importé ({data['fiscal_year']})")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erreur d'import: {e}")
                finally:
                    os.unlink(tmp_path)

    # --- Load data (fusion fundamentals + market_data) ---
    # Always use the joined view to get price, beta, rsi, etc. from market_data
    all_stocks = get_all_stocks_for_analysis()
    fundamentals = None
    if not all_stocks.empty:
        row = all_stocks[all_stocks["ticker"] == selected_ticker]
        if not row.empty:
            fundamentals = row.iloc[0].to_dict()
            # Replace NaN with None for cleaner handling
            import math
            for k, v in fundamentals.items():
                if isinstance(v, float) and math.isnan(v):
                    fundamentals[k] = None

    if not fundamentals:
        st.warning(f"Aucune donnée pour {selected_ticker}.")
        if is_admin():
            st.info("Importez un fichier Excel ou saisissez les données ci-dessous.")
            _render_input_form(selected_ticker, analyzable)
        else:
            st.info("L'administrateur doit intégrer les données de ce titre (import Excel ou saisie manuelle).")
        return

    # Load price data — cache session pour éviter re-downloads entre navigations
    _pdf_key = f"price_df_{selected_ticker}"
    price_df = st.session_state.get(_pdf_key)
    if price_df is None:
        price_df = get_cached_prices(selected_ticker)
        if price_df.empty:
            with st.spinner("Chargement des prix historiques..."):
                try:
                    price_df = fetch_historical_prices_page(selected_ticker, period="mensuel", years_back=5)
                    if not price_df.empty:
                        cache_prices(selected_ticker, price_df)
                except Exception:
                    price_df = pd.DataFrame()
        st.session_state[_pdf_key] = price_df

    # --- Compute scores (cache session pour éviter 7s de recalcul à chaque clic) ---
    # Clé : ticker + dernière date prix + hash des fondamentaux. Si inchangé,
    # on réutilise le résultat précédent.
    import hashlib as _h
    _pdf_sig = ""
    if not price_df.empty and "date" in price_df.columns:
        _pdf_sig = str(price_df["date"].max())
    _fund_sig = _h.md5(
        str(sorted((k, str(v)[:30]) for k, v in fundamentals.items())).encode()
    ).hexdigest()[:8]
    _score_key = f"score_{selected_ticker}_{_pdf_sig}_{_fund_sig}"

    if _score_key in st.session_state:
        result = st.session_state[_score_key]
    else:
        with st.spinner("Calcul des scores…"):
            result = compute_hybrid_score(fundamentals, price_df)
        st.session_state[_score_key] = result
    ratios = result["ratios"]
    reco = result["recommendation"]

    # --- Auto-capture for long-term calibration ---
    # Gate : 1 seule capture par ticker et par session, et seulement pour les admins.
    # Évite d'écrire ~15 round-trips Supabase à chaque render.
    _snap_key = f"snap_captured_{selected_ticker}"
    if is_admin() and not st.session_state.get(_snap_key):
        try:
            ref_price = fundamentals.get("price") or 0
            if not price_df.empty and "close" in price_df.columns:
                try:
                    ref_price = float(price_df.sort_values("date").iloc[-1]["close"]) or ref_price
                except Exception:
                    pass
            name = fundamentals.get("company_name") or selected_ticker
            sector = fundamentals.get("sector", "")
            save_signal_snapshots(
                ticker=selected_ticker,
                signals=result.get("signals", []),
                price=ref_price,
                company_name=name,
                sector=sector,
            )
            save_recommendation_snapshot(
                ticker=selected_ticker,
                recommendation=reco,
                hybrid_score=result["hybrid_score"],
                fundamental_score=result["fundamental_score"],
                technical_score=result["technical_score"],
                price=ref_price,
                trend=result["trend"]["trend"],
                company_name=name,
                sector=sector,
            )
            st.session_state[_snap_key] = True
        except Exception:
            pass

    # --- Header metrics ---
    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Entreprise", fundamentals.get("company_name", ""))
    c2.metric("Prix", f"{fundamentals.get('price', 0):,.0f} {CURRENCY}")
    c3.metric("Secteur", fundamentals.get("sector", ""))
    c4.metric("Exercice", str(fundamentals.get("fiscal_year", "")))
    c5.markdown(f"### {stars_display(reco['stars'])}")
    c5.markdown(_tag_html(reco['verdict'], _verdict_tone(reco['verdict'])), unsafe_allow_html=True)

    # --- Tabs ---
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Fondamental", "📈 Technique", "🎯 Recommandation", "🏢 Profil"])

    with tab1:
        _render_fundamental(fundamentals, ratios)

    with tab2:
        _render_technical(selected_ticker, price_df, result)

    with tab3:
        _render_recommendation(result, fundamentals)

    with tab4:
        _render_profile(selected_ticker, fundamentals)


def _render_fundamental(fundamentals, ratios):
    """Onglet analyse fondamentale."""
    st.subheader("Ratios calculés")

    sector = fundamentals.get("sector", "")
    # Load sector benchmarks for relative comparison
    benchmarks = get_sector_benchmarks(sector) if sector else {}

    # Helpers for each comparable ratio
    def _sector_cell(ratio_key, value, prefer_low=False):
        cmp = compare_to_sector(ratio_key, value, benchmarks, prefer_low=prefer_low)
        if not cmp:
            return "—"
        fmt = "decimal" if ratio_key in ("per", "pb") else "pct"
        med_str = format_ratio(cmp["median"], fmt)
        return (
            f"<span style='color:{cmp['color']};font-weight:600'>{cmp['badge']}</span>"
            f"<br><small>Médiane {cmp['scope']} : {med_str} "
            f"(N={cmp['count']})</small>"
        )

    # Main ratios table
    flags = ratios.get("flags", {})
    ratio_rows = [
        ("ROE", format_ratio(ratios.get("roe")), "≥ 15% (solide) ; ≥ 20% (excellent)",
         flags.get("roe", ("—", "")), _sector_cell("roe", ratios.get("roe"), prefer_low=False)),
        ("Marge nette", format_ratio(ratios.get("net_margin")), "≥ 10% (bon) ; ≥ 15% (très bon)",
         flags.get("net_margin", ("—", "")), _sector_cell("net_margin", ratios.get("net_margin"), prefer_low=False)),
        ("Dette/Equity", format_ratio(ratios.get("debt_equity"), "x"), "≤ 1.5 (hors banques)",
         flags.get("debt_equity", ("—", "")), "—"),
        ("Couverture intérêts", format_ratio(ratios.get("interest_coverage"), "x"), "≥ 3x (confortable)",
         flags.get("interest_coverage", ("—", "")), "—"),
        ("FCF", format_ratio(ratios.get("fcf"), "number"), "Positif et stable",
         flags.get("fcf", ("—", "")), "—"),
        ("FCF Margin", format_ratio(ratios.get("fcf_margin")), "≥ 5% (bon) ; ≥ 10% (très bon)",
         flags.get("fcf_margin", ("—", "")), "—"),
        ("EPS", format_ratio(ratios.get("eps"), "number"), "—", ("OK", ""), "—"),
        ("DPS", format_ratio(ratios.get("dps"), "number"), "—", ("OK", ""), "—"),
        ("Dividend Yield", format_ratio(ratios.get("dividend_yield")), "≥ 6% (cible BRVM)",
         flags.get("dividend_yield", ("—", "")), _sector_cell("dividend_yield", ratios.get("dividend_yield"), prefer_low=False)),
        ("Payout ratio", format_ratio(ratios.get("payout_ratio")), "40-70% (sain)",
         flags.get("payout_ratio", ("—", "")), "—"),
        ("PER", format_ratio(ratios.get("per"), "decimal"), "≤ 12-15 (value)",
         flags.get("per", ("—", "")), _sector_cell("per", ratios.get("per"), prefer_low=True)),
        ("P/B", format_ratio(ratios.get("pb"), "x"), "< 2 (hors banques)",
         flags.get("pb", ("—", "")), _sector_cell("pb", ratios.get("pb"), prefer_low=True)),
        ("Couverture div (cash)", format_ratio(ratios.get("dividend_cash_coverage"), "x"), "≥ 1.2x",
         flags.get("dividend_cash_coverage", ("—", "")), "—"),
    ]

    # Header row
    h1, h2, h3, h4, h5 = st.columns([2, 1.3, 2.7, 1.7, 2.3])
    h1.markdown("**Indicateur**")
    h2.markdown("**Valeur**")
    h3.markdown("**Seuil**")
    h4.markdown("**Drapeau**")
    h5.markdown("**Vs secteur**")

    for name, value, rule, (flag, detail), sector_cell in ratio_rows:
        c1, c2, c3, c4, c5 = st.columns([2, 1.3, 2.7, 1.7, 2.3])
        c1.write(f"**{name}**")
        c2.write(value)
        c3.write(rule)
        flag_color = {"OK": "🟢", "Vigilance": "🟡", "Risque": "🔴"}.get(flag, "⚪")
        c4.write(f"{flag_color} {flag} - {detail}")
        c5.markdown(sector_cell, unsafe_allow_html=True)

    # Sector peer box
    if benchmarks.get("sector"):
        peers = benchmarks.get("sector_peers", [])
        sector_name = benchmarks.get("sector_name", sector)
        with st.expander(
            f"📊 Positionnement vs secteur **{sector_name}** ({len(peers)} pairs)",
            expanded=False,
        ):
            st.caption(
                "Comparaison aux autres titres du secteur. "
                "Médiane = valeur centrale, pas la moyenne. "
                "Pour PER et P/B, une valeur **sous** la médiane signale souvent une opportunité. "
                "Pour ROE/Yield/Marge, une valeur **au-dessus** est préférée."
            )
            peer_rows = []
            for key, label, fmt, prefer_low in [
                ("per", "PER", "decimal", True),
                ("pb", "P/B", "x", True),
                ("roe", "ROE", "pct", False),
                ("net_margin", "Marge nette", "pct", False),
                ("dividend_yield", "Yield", "pct", False),
            ]:
                b = benchmarks["sector"].get(key)
                my_val = ratios.get(key)
                if not b:
                    continue
                peer_rows.append({
                    "Indicateur": label,
                    "Ma valeur": format_ratio(my_val, fmt) if my_val is not None else "—",
                    "Médiane secteur": format_ratio(b["median"], fmt),
                    "Min secteur": format_ratio(b["min"], fmt),
                    "Max secteur": format_ratio(b["max"], fmt),
                    "N": b["count"],
                })
            if peer_rows:
                import pandas as pd
                st.dataframe(pd.DataFrame(peer_rows), use_container_width=True, hide_index=True)

    # Checklist
    st.markdown("---")
    st.subheader("Checklist Value & Dividendes")
    checklist = ratios.get("checklist", [])
    # Map checklist labels to display format
    _check_fmt = {"PER": "decimal", "Couverture": "x", "Dette": "x"}
    for item in checklist:
        # Determine format: PER/coverage/debt → decimal/x, yield/payout/roe → pct
        fmt = "pct"
        for key, f in _check_fmt.items():
            if key in item["label"]:
                fmt = f
                break
        val_str = format_ratio(item["value"], fmt)
        if item["passed"] is True:
            st.write(f"✅ {item['label']} — Valeur: {val_str}")
        elif item["passed"] is False:
            st.write(f"❌ {item['label']} — Valeur: {val_str}")
        else:
            st.write(f"⚪ {item['label']} — N/A")

    passed = sum(1 for i in checklist if i["passed"] is True)
    total = len(checklist)
    st.progress(passed / total if total > 0 else 0, text=f"{passed}/{total} critères validés")

    # Historical growth
    st.markdown("---")
    fiscal_year = fundamentals.get("fiscal_year")
    if fiscal_year:
        fy = int(fiscal_year)
        year_labels = [str(fy - 3), str(fy - 2), str(fy - 1), str(fy)]
        st.subheader(f"Historique ({fy-3} à {fy})")
    else:
        year_labels = ["N-3", "N-2", "N-1", "N"]
        st.subheader("Historique (N-3 à N)")

    rev_series = [fundamentals.get(f"revenue_{s}") for s in ("n3", "n2", "n1", "n0")]
    ni_series = [fundamentals.get(f"net_income_{s}") for s in ("n3", "n2", "n1", "n0")]
    dps_series = [fundamentals.get(f"dps_{s}") for s in ("n3", "n2", "n1", "n0")]

    def _growth_series(values):
        """Calcule le taux de croissance année-sur-année à partir d'une série."""
        out = [None]  # pas de growth pour N-3 (première année)
        for i in range(1, len(values)):
            prev, cur = values[i-1], values[i]
            if prev and cur is not None and prev != 0:
                out.append((cur - prev) / abs(prev))
            else:
                out.append(None)
        return out

    rev_growth = _growth_series(rev_series)
    ni_growth = _growth_series(ni_series)

    hist_data = [
        ("Chiffre d'affaires", rev_series, "number"),
        ("— Croissance CA", rev_growth, "pct"),
        ("Résultat net", ni_series, "number"),
        ("— Croissance RN", ni_growth, "pct"),
        ("DPS", dps_series, "number"),
    ]

    # Header row with year labels
    c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 1, 1])
    c1.write("")
    for col, yr in zip([c2, c3, c4, c5], year_labels):
        col.write(f"**{yr}**")

    for label, values, fmt in hist_data:
        if any(v is not None for v in values):
            c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 1, 1])
            c1.write(f"**{label}**")
            for col, val in zip([c2, c3, c4, c5], values):
                if val is None:
                    col.markdown("<span class='muted'>—</span>", unsafe_allow_html=True)
                elif fmt == "pct":
                    col.markdown(_delta_html(val * 100, with_arrow=False), unsafe_allow_html=True)
                else:
                    col.markdown(
                        f"<span style='font-variant-numeric:tabular-nums'>{val:,.0f}</span>",
                        unsafe_allow_html=True,
                    )


def _render_technical(ticker, price_df, result):
    """Onglet analyse technique."""
    from datetime import datetime, timedelta
    from analysis.technical import _detect_frequency, SMA_LABELS

    if price_df.empty or len(price_df) < 5:
        st.warning("Données de prix insuffisantes pour l'analyse technique.")
        if not is_admin():
            st.info("L'administrateur doit charger les prix historiques depuis sikafinance.com.")
            return
        st.info("Cliquez sur le bouton ci-dessous pour charger les prix depuis sikafinance.com")
        if st.button("📥 Charger les prix (5 ans mensuel)"):
            with st.spinner("Téléchargement en cours..."):
                try:
                    price_df = fetch_historical_prices_page(ticker, period="mensuel", years_back=5)
                    if not price_df.empty:
                        cache_prices(ticker, price_df)
                        st.success(f"{len(price_df)} points de données chargés (mensuel 5 ans)")
                        st.rerun()
                    else:
                        st.error("Aucune donnée trouvée")
                except Exception as e:
                    st.error(f"Erreur: {e}")
        return

    # Compute indicators on full dataset
    df = compute_all_indicators(price_df)
    freq = df.attrs.get("frequency", _detect_frequency(price_df))
    sma_labels = SMA_LABELS.get(freq, SMA_LABELS["daily"])
    freq_label = "mensuelle" if freq == "monthly" else "journalière"

    # --- Period selector + chart options ---
    col_period, col_opt1, col_opt2, col_opt3 = st.columns([2, 1, 1, 1])

    with col_period:
        period_options = {"3M": 90, "6M": 180, "1A": 365, "2A": 730, "3A": 1095, "Max": 9999}
        selected_period = st.selectbox("Periode", list(period_options.keys()), index=4 if freq == "monthly" else 2)
        days_back = period_options[selected_period]

    with col_opt1:
        show_bb = st.checkbox("Bandes de Bollinger", value=False)
    with col_opt2:
        show_rsi = st.checkbox("RSI", value=True)
    with col_opt3:
        show_macd = st.checkbox("MACD", value=True)

    # Filter data to selected period
    if days_back < 9999 and not df.empty:
        cutoff = pd.Timestamp(datetime.now() - timedelta(days=days_back))
        df_display = df[df["date"] >= cutoff].copy()
        if df_display.empty:
            df_display = df
    else:
        df_display = df

    # Preserve frequency attribute after filtering
    df_display.attrs["frequency"] = freq

    # Show data range info
    if not df_display.empty:
        date_min = df_display["date"].min()
        date_max = df_display["date"].max()
        pts_label = "points mensuels" if freq == "monthly" else "séances"
        st.caption(f"Périodicité {freq_label} — {date_min.strftime('%d/%m/%Y')} au {date_max.strftime('%d/%m/%Y')} ({len(df_display)} {pts_label})")

    # Candlestick chart
    fig = candlestick_chart(
        df_display, title=f"{ticker}", show_bollinger=show_bb, show_rsi=show_rsi, show_macd=show_macd,
        sma_labels=sma_labels,
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

    # Supports / Résistances
    supports = result.get("supports", [])
    resistances = result.get("resistances", [])
    col_sr1, col_sr2 = st.columns(2)
    with col_sr1:
        st.subheader("🟢 Supports")
        for i, s in enumerate(supports[:3]):
            st.write(f"Zone {i+1}: **{s:,.0f} FCFA**")
        if not supports:
            st.info("Aucun support détecté")
    with col_sr2:
        st.subheader("🔴 Résistances")
        for i, r in enumerate(resistances[:3]):
            st.write(f"Zone {i+1}: **{r:,.0f} FCFA**")
        if not resistances:
            st.info("Aucune résistance détectée")

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

    # --- Explanations RSI & MACD ---
    st.markdown("---")
    _render_indicator_explanations(df_display, sma_labels, freq)


def _render_indicator_explanations(df: pd.DataFrame, sma_labels: dict, freq: str):
    """Affiche les explications des indicateurs RSI et MACD."""
    if df.empty:
        return

    last = df.iloc[-1]
    rsi_val = last.get("rsi")
    macd_val = last.get("macd")
    macd_sig = last.get("macd_signal")
    macd_hist = last.get("macd_histogram")

    with st.expander("📖 Comprendre les indicateurs techniques", expanded=False):
        col_rsi, col_macd = st.columns(2)

        with col_rsi:
            st.markdown("#### RSI (Relative Strength Index)")
            st.markdown(
                "Le RSI mesure la **vitesse et l'amplitude des mouvements de prix** "
                "sur une échelle de 0 à 100. Il compare les gains récents aux pertes récentes."
            )
            st.markdown(
                "- **RSI > 70** : zone de **surachat** — le titre a beaucoup monté, "
                "un repli est possible\n"
                "- **RSI < 30** : zone de **survente** — le titre a beaucoup baissé, "
                "un rebond est possible\n"
                "- **RSI entre 40-60** : zone neutre, pas de signal fort"
            )
            if rsi_val is not None and not pd.isna(rsi_val):
                if rsi_val > 70:
                    interp = "Le titre est en **surachat**. Attention à un possible retournement baissier."
                    tone = "down"
                elif rsi_val < 30:
                    interp = "Le titre est en **survente**. Opportunité d'achat potentielle si les fondamentaux sont solides."
                    tone = "up"
                elif rsi_val > 60 or rsi_val < 40:
                    interp = ("Momentum haussier, mais pas encore en surachat." if rsi_val > 60
                              else "Momentum baissier, mais pas encore en survente.")
                    tone = "ocre"
                else:
                    interp = "Zone neutre — pas de signal directionnel fort."
                    tone = "neutral"
                st.markdown(
                    "**RSI actuel :** " + _tag_html(f"{rsi_val:.1f}", tone),
                    unsafe_allow_html=True,
                )
                st.markdown(f"*{interp}*")

        with col_macd:
            st.markdown("#### MACD (Moving Average Convergence Divergence)")
            st.markdown(
                "Le MACD mesure la **convergence/divergence entre deux moyennes mobiles**. "
                "Il se compose de 3 éléments :"
            )
            st.markdown(
                "- **Ligne MACD** : différence entre MM rapide et MM lente\n"
                "- **Ligne Signal** : moyenne mobile du MACD\n"
                "- **Histogramme** : écart entre MACD et Signal"
            )
            st.markdown(
                "**Signaux clés :**\n"
                "- MACD **croise le Signal par le haut** → signal d'achat\n"
                "- MACD **croise le Signal par le bas** → signal de vente\n"
                "- Histogramme **positif et croissant** → momentum haussier\n"
                "- Histogramme **négatif et decroissant** → momentum baissier"
            )
            if macd_val is not None and not pd.isna(macd_val):
                if macd_val > 0 and macd_hist is not None and macd_hist > 0:
                    interp = "MACD positif avec histogramme croissant — **momentum haussier**."
                    tone = "up"
                elif macd_val > 0:
                    interp = "MACD positif mais histogramme en baisse — le momentum ralentit."
                    tone = "ocre"
                elif macd_hist is not None and macd_hist > 0:
                    interp = "MACD négatif mais histogramme en hausse — possible retournement haussier."
                    tone = "ocre"
                else:
                    interp = "MACD négatif avec histogramme baissier — **momentum baissier**."
                    tone = "down"
                st.markdown(
                    "**MACD actuel :** " + _tag_html(f"{macd_val:,.0f}", tone),
                    unsafe_allow_html=True,
                )
                st.markdown(f"*{interp}*")

        # Moyennes mobiles explanation
        st.markdown("---")
        col_mm, col_bb = st.columns(2)

        with col_mm:
            st.markdown("#### Moyennes Mobiles")
            if freq == "monthly":
                st.markdown(
                    f"Avec des données mensuelles, les moyennes mobiles s'adaptent :\n"
                    f"- **{sma_labels['short']}** (3 mois) : tendance court terme\n"
                    f"- **{sma_labels['medium']}** (6 mois) : tendance moyen terme\n"
                    f"- **{sma_labels['long']}** (12 mois) : tendance long terme\n\n"
                    f"Quand le prix est **au-dessus** des 3 moyennes alignées, la tendance est fortement haussière. "
                    f"Quand il est **en-dessous**, elle est fortement baissière."
                )
            else:
                st.markdown(
                    f"- **{sma_labels['short']}** (20 jours) : tendance court terme\n"
                    f"- **{sma_labels['medium']}** (50 jours) : tendance moyen terme\n"
                    f"- **{sma_labels['long']}** (200 jours) : tendance long terme\n\n"
                    f"Un **Golden Cross** (MM courte croise MM longue par le haut) est un signal d'achat. "
                    f"Un **Death Cross** (croisement par le bas) est un signal de vente."
                )

        with col_bb:
            st.markdown("#### Bandes de Bollinger")
            st.markdown(
                "Les bandes de Bollinger mesurent la **volatilité** du titre. "
                "Elles se composent de 3 lignes :"
            )
            st.markdown(
                "- **Bande supérieure** : moyenne mobile + 2 écarts-types\n"
                "- **Bande médiane** : moyenne mobile simple\n"
                "- **Bande inférieure** : moyenne mobile - 2 écarts-types"
            )
            st.markdown(
                "**Interprétation :**\n"
                "- Prix proche de la **bande supérieure** → le titre est potentiellement suracheté\n"
                "- Prix proche de la **bande inférieure** → le titre est potentiellement survendu\n"
                "- **Resserrement** des bandes → faible volatilité, mouvement important à venir\n"
                "- **Écartement** des bandes → forte volatilité en cours"
            )


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
            st.info("Aucun point fort identifié")
    with col_w:
        st.subheader("⚠️ Points de vigilance")
        for w in reco.get("warnings", []):
            st.write(f"⚠️ {w}")
        if not reco.get("warnings"):
            st.info("Aucun point de vigilance")

    # Entry zones
    st.markdown("---")
    st.subheader("🎯 Zones d'entrée suggérées")
    entry_zones = reco.get("entry_zones", [])
    if entry_zones:
        for zone in entry_zones:
            st.write(f"🟢 **{zone['label']}**: {zone['zone']} — Risque/Rendement: {zone['risk_reward']}")
    else:
        st.info("Pas assez de données pour déterminer les zones d'entree")


def _render_input_form(ticker, tickers_data):
    """Formulaire de saisie manuelle des données fondamentales."""
    st.markdown("---")
    st.subheader("Saisie manuelle des données fondamentales")

    ticker_info = next((t for t in tickers_data if t["ticker"] == ticker), {})

    with st.form("fundamental_form"):
        st.markdown("##### Informations société")
        col1, col2, col3 = st.columns(3)
        company_name = col1.text_input("Nom", value=ticker_info.get("name", ""))
        sector = col2.text_input("Secteur", value=ticker_info.get("sector", ""))
        fiscal_year = col3.number_input("Exercice", value=2024, min_value=2000, max_value=2030)

        col4, col5 = st.columns(2)
        price = col4.number_input("Prix actuel (FCFA)", value=0, min_value=0)
        shares = col5.number_input("Nombre d'actions", value=0, min_value=0)

        st.markdown("##### Données financières")
        col6, col7 = st.columns(2)
        revenue = col6.number_input("Chiffre d'affaires", value=0)
        net_income = col7.number_input("Résultat net", value=0)

        col8, col9 = st.columns(2)
        equity = col8.number_input("Capitaux propres", value=0)
        total_debt = col9.number_input("Dette financiere totale", value=0)

        col10, col11 = st.columns(2)
        ebit = col10.number_input("EBIT", value=0)
        interest_expense = col11.number_input("Charges d'intérêts", value=0)

        col12, col13 = st.columns(2)
        cfo = col12.number_input("Cash-flow opérationnel (CFO)", value=0)
        capex = col13.number_input("CAPEX", value=0)

        col14, col15 = st.columns(2)
        dividends_total = col14.number_input("Dividendes versés (total)", value=0)
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
            st.success("✅ Données enregistrées !")
            st.rerun()


def _render_profile(ticker: str, fundamentals: dict):
    """Onglet profil qualitatif de l'entreprise."""
    profile = get_company_profile(ticker)

    if not profile:
        st.info("Profil non disponible. Lancez `scripts/scrape_profiles.py` pour charger les données.")
        return

    # --- Company description ---
    if profile.get("description"):
        st.markdown("#### Présentation de l'entreprise")
        # Clean description: remove "La société :" prefix if present
        desc = profile["description"]
        for prefix in ["La société :", "La société :", "La société:", "La société:"]:
            if desc.startswith(prefix):
                desc = desc[len(prefix):].strip()
        st.markdown(desc)
        st.markdown("---")

    # --- Key info in columns ---
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 👥 Dirigeants")
        has_officers = False
        if profile.get("president"):
            st.markdown(f"**Président du Conseil :** {profile['president']}")
            has_officers = True
        if profile.get("dg"):
            st.markdown(f"**Directeur Général :** {profile['dg']}")
            has_officers = True
        if profile.get("dga"):
            st.markdown(f"**DG Adjoint :** {profile['dga']}")
            has_officers = True
        if not has_officers:
            st.caption("Non disponible")

    with col2:
        st.markdown("#### 📊 Actionnariat & Marché")
        if profile.get("major_shareholder"):
            pct = profile.get("major_shareholder_pct")
            pct_str = f" ({pct:.1f}%)" if pct else ""
            st.markdown(f"**Actionnaire principal :** {profile['major_shareholder']}{pct_str}")

        # Get shares and float from market_data
        conn = get_connection()
        md = conn.execute(
            "SELECT shares, float_pct, market_cap FROM market_data WHERE ticker = ?",
            (ticker,)
        ).fetchone()
        conn.close()

        if md:
            if md["shares"] and md["shares"] > 0:
                st.markdown(f"**Nombre de titres :** {md['shares']:,.0f}")
            if md["float_pct"] and md["float_pct"] > 0:
                st.markdown(f"**Flottant :** {md['float_pct']:.1f}%")
            if md["market_cap"] and md["market_cap"] > 0:
                st.markdown(f"**Capitalisation :** {md['market_cap']/1e3:,.1f} Mds FCFA")

    st.markdown("---")

    # --- Contact ---
    contact_parts = []
    if profile.get("address"):
        contact_parts.append(f"📍 {profile['address']}")
    if profile.get("phone"):
        contact_parts.append(f"📞 {profile['phone']}")
    if profile.get("fax"):
        contact_parts.append(f"📠 {profile['fax']}")

    if contact_parts:
        st.markdown("#### Contact")
        st.markdown(" | ".join(contact_parts))
        st.markdown("---")

    # --- Financial history ---
    conn = get_connection()
    fund = read_sql_df("""SELECT fiscal_year, revenue, net_income, dps, eps, per
           FROM fundamentals WHERE ticker = ?
           ORDER BY fiscal_year DESC LIMIT 5""", params=(ticker,),
    )
    conn.close()

    if not fund.empty:
        st.markdown("#### Historique financier (sikafinance)")
        display = fund.copy()
        display["fiscal_year"] = display["fiscal_year"].astype(int)
        display["revenue"] = display["revenue"].apply(
            lambda x: f"{x/1e9:,.1f} Mds" if pd.notna(x) and x > 0 else "—"
        )
        display["net_income"] = display["net_income"].apply(
            lambda x: f"{x/1e9:,.1f} Mds" if pd.notna(x) and abs(x) > 0 else "—"
        )
        display["dps"] = display["dps"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) and x > 0 else "—"
        )
        display["eps"] = display["eps"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) and abs(x) > 0 else "—"
        )
        display["per"] = display["per"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) and x > 0 else "—"
        )
        st.dataframe(
            display.rename(columns={
                "fiscal_year": "Année", "revenue": "Chiffre d'affaires",
                "net_income": "Résultat net", "dps": "DPS",
                "eps": "BNPA", "per": "PER",
            }),
            use_container_width=True, hide_index=True,
        )
        st.markdown("---")

    # --- Recent news ---
    news = get_company_news(ticker, limit=8)
    if not news.empty:
        st.markdown("#### Actualités récentes")
        for _, art in news.iterrows():
            date_str = f" ({art['article_date']})" if art.get("article_date") else ""
            url = art.get("url", "")
            if url and url.startswith("http"):
                st.markdown(f"- [{art['title']}]({url}){date_str}")
            else:
                st.markdown(f"- {art['title']}{date_str}")
        st.markdown("---")

    # --- Qualitative notes (user-added) ---
    st.markdown("#### Notes d'analyse")
    notes = get_qualitative_notes(ticker)
    if not notes.empty:
        for _, note in notes.iterrows():
            cat_emoji = {
                "strategie": "🎯", "concurrence": "⚔️", "risques": "⚠️",
                "gouvernance": "🏛️", "perspectives": "🔮", "dividendes": "💰",
                "general": "📝",
            }.get(note.get("category", ""), "📝")
            col_cat, col_content, col_del = st.columns([1, 5, 0.5])
            col_cat.write(f"{cat_emoji} **{note.get('category', 'general').capitalize()}**")
            col_content.write(note["content"])
            if note.get("source"):
                col_content.caption(f"Source: {note['source']} | {note.get('note_date', '')}")
            if col_del.button("🗑️", key=f"del_note_p2_{note['id']}"):
                delete_qualitative_note(note["id"])
                st.rerun()

    # Add note form
    with st.expander("Ajouter une note d'analyse"):
        with st.form(f"add_note_p2_{ticker}"):
            category = st.selectbox("Catégorie", [
                "strategie", "concurrence", "risques", "gouvernance",
                "perspectives", "dividendes", "general",
            ])
            content = st.text_area(
                "Contenu",
                placeholder="Position concurrentielle, risques identifiés, perspectives...",
                height=100,
            )
            col_s, col_d = st.columns(2)
            source = col_s.text_input("Source", placeholder="Rapport annuel 2024...")
            note_date = col_d.date_input("Date")
            if st.form_submit_button("💾 Enregistrer"):
                if content.strip():
                    save_qualitative_note(ticker, category, content.strip(), source, str(note_date))
                    st.success("✅ Note enregistrée")
                    st.rerun()
