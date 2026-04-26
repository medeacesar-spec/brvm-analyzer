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
    get_company_profile, get_connection,
    get_qualitative_notes, save_qualitative_note, delete_qualitative_note,
    save_signal_snapshots, save_recommendation_snapshot,
)
from data.db import read_sql_df
from data.scraper import fetch_historical_prices, fetch_historical_prices_page
from analysis.fundamental import (
    compute_ratios, format_ratio,
    get_sector_benchmarks, compare_to_sector,
    compute_target_price,
)
from analysis.technical import compute_all_indicators, detect_trend, detect_support_resistance, generate_signals
from analysis.scoring import compute_hybrid_score
from utils.charts import candlestick_chart, gauge_chart, flag_badge, stars_display
from utils.auth import is_admin
from utils.ui_helpers import delta as _delta_html, tag as _tag_html, ticker as _ticker_html, section_heading

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
    # Hiérarchie v3 : selecteur → Header titre+stars → KPI cards → Tabs
    analyzable = get_analyzable_tickers()
    if not analyzable:
        st.warning("Aucune donnée disponible. Lancez l'enrichissement des données de marché.")
        return

    # Sélecteur compact — ticker · nom (sans marqueur de type de données)
    all_options = [f"{t['ticker']} · {t['name']}" for t in analyzable]
    default_index = 0
    target_ticker = st.session_state.pop("target_ticker", None)
    if target_ticker:
        for i, opt in enumerate(all_options):
            if opt.split(" · ")[0] == target_ticker:
                default_index = i
                break

    selection = st.selectbox(
        "Titre",
        all_options, index=default_index,
        key="p2_ticker_select",
        label_visibility="collapsed",
    )
    selected_ticker = selection.split(" · ")[0]

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
    # ── Source unique : on calcule les ratios localement (ils dependent
    # de compute_ratios sur les fondamentaux, c'est deterministe et rapide)
    # mais on PREFERE les scores / verdict / signals du snapshot quotidien
    # quand il existe, pour rester COHERENT avec p5 Signaux / p10 Historique
    # / la requete admin. Si le snapshot est absent, fallback live compute.
    import hashlib as _h
    _pdf_sig = ""
    if not price_df.empty and "date" in price_df.columns:
        _pdf_sig = str(price_df["date"].max())
    _fund_sig = _h.md5(
        str(sorted((k, str(v)[:30]) for k, v in fundamentals.items())).encode()
    ).hexdigest()[:8]
    # v3 dans la clé = invalide les caches sessions des anciens algos
    _score_key = f"score_v3_{selected_ticker}_{_pdf_sig}_{_fund_sig}"

    if _score_key in st.session_state:
        result = st.session_state[_score_key]
    else:
        # 1. Calcul ratios en local (rapide, pur Python)
        ratios_local = compute_ratios(fundamentals)

        # 2. Charge le snapshot (ticker unique, 1 requete Supabase cachee)
        snap = _load_one_scoring_snapshot(selected_ticker)

        if snap and snap.get("hybrid_score") is not None:
            # 3a. Reconstitue result : on utilise les ratios_local comme source
            # unique pour fundamental_score (nouveau breakdown), et le snapshot
            # pour technical_score uniquement. Le hybrid_score + verdict sont
            # RECALCULÉS avec le nouvel algo pour cohérence entre les 4 onglets.
            try:
                signals = _json.loads(snap.get("signals_json") or "[]")
            except Exception:
                signals = []

            # Supports/Résistances recalculés localement si pas dans le snapshot
            from analysis.technical import detect_support_resistance
            if not price_df.empty and len(price_df) >= 8:
                sr_levels = detect_support_resistance(price_df)
            else:
                sr_levels = {"supports": [], "resistances": []}

            # Score fondamental = breakdown.total (nouveau algo, source unique)
            fund_score_local = ratios_local.get("fundamental_score") or 0
            tech_score_snap = snap.get("technical_score") or 0
            hybrid_local = fund_score_local + tech_score_snap

            # Verdict recalculé selon nouveau hybrid_score (seuils 70/52/38/25)
            if hybrid_local >= 70:
                verdict, stars = "ACHAT FORT", 5
            elif hybrid_local >= 52:
                verdict, stars = "ACHAT", 4
            elif hybrid_local >= 38:
                verdict, stars = "CONSERVER", 3
            elif hybrid_local >= 25:
                verdict, stars = "PRUDENCE", 2
            else:
                verdict, stars = "EVITER", 1

            result = {
                "ratios": ratios_local,
                "hybrid_score": hybrid_local,
                "fundamental_score": fund_score_local,
                "technical_score": tech_score_snap,
                "signals": signals,
                "trend": {
                    "trend": snap.get("trend") or "indetermine",
                    "strength": "",
                    "details": "",
                },
                "supports": sr_levels["supports"],
                "resistances": sr_levels["resistances"],
                "recommendation": {
                    "verdict": verdict,
                    "stars": stars,
                    "verdict_color": "#1F5D3A" if "ACHAT" in verdict else
                                     "#B42318" if "VENTE" in verdict or "EVITER" in verdict else "#B5730E",
                    "strengths": [],
                    "warnings": [],
                    "entry_zones": [],
                },
            }
            # Enrichit strengths/warnings/entry_zones/trend via compute_hybrid_score
            # (ces champs ne sont pas dans le snapshot)
            try:
                _full = compute_hybrid_score(fundamentals, price_df)
                result["recommendation"]["strengths"] = _full["recommendation"].get("strengths", [])
                result["recommendation"]["warnings"] = _full["recommendation"].get("warnings", [])
                result["recommendation"]["entry_zones"] = _full["recommendation"].get("entry_zones", [])
                result["trend"] = _full["trend"]
            except Exception:
                pass
        else:
            # 3b. Fallback live compute (snapshot absent = 1er lancement / cache vide)
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

    # ═══════════════════════════════════════════════════════════════════
    # Header éditorial : "TICKER · Secteur · Exercice YYYY"   + stars
    # ═══════════════════════════════════════════════════════════════════
    sector_name = fundamentals.get("sector") or "—"
    fy = fundamentals.get("fiscal_year")
    year_label = f"Exercice {int(fy)}" if fy else ""
    header_parts = [selected_ticker, sector_name]
    if year_label:
        header_parts.append(year_label)
    header_title = "  ·  ".join(header_parts)

    col_title, col_stars = st.columns([5, 1])
    with col_title:
        st.markdown(
            f"<h1 style='margin:0;padding:0;font-size:24px;font-weight:600;"
            f"letter-spacing:-0.02em;'>{header_title}</h1>"
            f"<div style='color:var(--ink-3);font-size:13px;margin-top:4px;'>"
            f"{fundamentals.get('company_name', '')}</div>",
            unsafe_allow_html=True,
        )
    with col_stars:
        # Stars + verdict tag juste en dessous (le verdict n'apparaît plus dans Recommandation)
        st.markdown(
            f"<div style='text-align:right;padding-top:4px;'>"
            f"{stars_display(reco['stars'])}"
            f"<div style='margin-top:4px;'>"
            + _tag_html(reco['verdict'], _verdict_tone(reco['verdict']))
            + "</div>"
            "</div>",
            unsafe_allow_html=True,
        )

    # ═══════════════════════════════════════════════════════════════════
    # KPI cards : Prix · Capitalisation · P/E · Dividend Yield
    # avec benchmarks secteur (flèche ▲/▼ + médiane)
    # ═══════════════════════════════════════════════════════════════════
    from analysis.fundamental import get_sector_benchmarks, compare_to_sector
    benchmarks = get_sector_benchmarks(sector_name) if sector_name != "—" else {}

    def _sector_sub(key: str, value, prefer_low: bool, fmt: str = "pct") -> str:
        """Construit le sous-texte 'Secteur X.X' avec flèche selon comparaison."""
        cmp = compare_to_sector(key, value, benchmarks, prefer_low=prefer_low)
        if not cmp:
            return ""
        med_str = (
            f"{cmp['median']:.1f}" if fmt == "decimal"
            else f"{cmp['median']*100:.1f}%" if fmt == "pct"
            else f"{cmp['median']:,.0f}"
        )
        diff = cmp["diff"]
        # Flèche : ▲ si au-dessus médiane, ▼ si en-dessous (indépendant du "bon/mauvais")
        arrow = "▲" if diff > 0 else "▼" if diff < 0 else "="
        # Couleur selon "bon/mauvais" (prefer_low inverse la logique)
        is_good = (diff < 0 and prefer_low) or (diff > 0 and not prefer_low) or abs(diff) < 0.05
        color = "var(--up)" if (is_good and abs(diff) >= 0.05) else \
                "var(--down)" if not is_good else "var(--ink-3)"
        return (
            f"<span style='color:{color};font-weight:500;'>{arrow} Secteur {med_str}</span>"
        )

    def _stat_card(label: str, value: str, sub_html: str = "", tone: str = "neutral"):
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;min-height:92px;'>"
            f"<div style='font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
            f"color:var(--ink-3);font-weight:500;margin-bottom:8px;'>{label}</div>"
            f"<div style='font-size:24px;font-weight:600;letter-spacing:-0.02em;"
            f"font-variant-numeric:tabular-nums;color:var(--ink);line-height:1.1;'>{value}</div>"
            f"{('<div style=' + chr(34) + 'font-size:11.5px;color:var(--ink-3);margin-top:6px;' + chr(34) + '>' + sub_html + '</div>') if sub_html else ''}"
            f"</div>"
        )

    price = fundamentals.get("price") or 0
    shares = fundamentals.get("shares")
    mcap = price * shares / 1e9 if (price and shares) else None
    per = ratios.get("per")
    yield_val = ratios.get("dividend_yield")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            _stat_card(
                "Prix",
                f"{price:,.0f}" if price else "—",
                f"— {CURRENCY}",
            ),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            _stat_card(
                "Capitalisation",
                f"{mcap:,.1f} Md" if mcap else "—",
                f"— {CURRENCY}",
            ),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            _stat_card(
                "P/E",
                f"{per:.1f}" if per and per > 0 else "—",
                _sector_sub("per", per, prefer_low=True, fmt="decimal") if per else "",
            ),
            unsafe_allow_html=True,
        )
    with c4:
        yield_str = f"{yield_val*100:.2f}%" if yield_val else "—"
        st.markdown(
            _stat_card(
                "Dividend Yield",
                yield_str,
                _sector_sub("dividend_yield", yield_val, prefer_low=False, fmt="pct") if yield_val else "",
            ),
            unsafe_allow_html=True,
        )

    # Tabs — labels épurés sans emoji
    tab1, tab2, tab3, tab4 = st.tabs(["Fondamentale", "Technique", "Recommandation", "Profil"])

    with tab1:
        _render_fundamental(fundamentals, ratios)

    with tab2:
        _render_technical(selected_ticker, price_df, result)

    with tab3:
        _render_recommendation(result, fundamentals)
        _render_score_evolution(selected_ticker)

    with tab4:
        _render_profile(selected_ticker, fundamentals)

    # ─── Admin : import Excel (en bas, replié par défaut) ───
    if is_admin():
        with st.expander("Import Excel (admin)", expanded=False):
            uploaded = st.file_uploader(
                "Fichier Analyse Hybride", type=["xlsx"],
                label_visibility="collapsed",
                key=f"p2_excel_{selected_ticker}",
            )
            if uploaded:
                import tempfile, os
                with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                    tmp.write(uploaded.read())
                    tmp_path = tmp.name
                try:
                    data = import_from_excel(tmp_path)
                    save_fundamentals(data)
                    st.success(f"{data['company_name']} importé ({data['fiscal_year']})")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erreur d'import: {e}")
                finally:
                    os.unlink(tmp_path)


def _render_fundamental(fundamentals, ratios):
    """Onglet analyse fondamentale v3 : score breakdown + ratios avec
    position secteur + trajectoire financiere + structure bilan."""
    from utils.ui_helpers import section_heading

    sector = fundamentals.get("sector", "")
    benchmarks = get_sector_benchmarks(sector) if sector else {}

    # ═══════════════════════════════════════════════════════════════════
    # Card "Score fondamental" avec breakdown sous-scores + mini barres
    # ═══════════════════════════════════════════════════════════════════
    # Si le breakdown n'est pas dans ratios (vieux result en cache session
    # ou code ancien), on le recalcule à la volée pour ne pas afficher des 0.
    bd = ratios.get("fundamental_breakdown")
    if not bd:
        from analysis.fundamental import _compute_fundamental_breakdown
        _sector = (fundamentals.get("sector") or "").lower()
        _is_bank = "banque" in _sector or "bank" in _sector
        bd = _compute_fundamental_breakdown(ratios, _is_bank)
        ratios["fundamental_breakdown"] = bd  # cache dans le dict pour réutilisation
    total = bd.get("total", ratios.get("fundamental_score", 0)) or 0
    profile = bd.get("profile", "")

    subs = [
        ("Rentabilité",  bd.get("rentabilite", 0),  15),
        ("Endettement",  bd.get("endettement", 0),  10),
        ("Valorisation", bd.get("valorisation", 0), 15),
        ("Dividendes",   bd.get("dividendes", 0),   10),
    ]

    def _tone_for(score, max_score):
        pct = score / max_score if max_score else 0
        if pct >= 0.66: return "var(--up)"
        if pct >= 0.40: return "var(--ocre)"
        return "var(--down)"

    # Grille : 1 col score total + 4 cols sous-scores
    c_total, c_sub = st.columns([1.6, 4])
    with c_total:
        st.markdown(
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;height:100%;'>"
            f"<div class='label-xs' style='margin-bottom:4px;'>Score fondamental</div>"
            f"<div style='font-size:28px;font-weight:600;letter-spacing:-0.02em;"
            f"color:var(--ink);font-variant-numeric:tabular-nums;'>"
            f"{total:.0f} <span style='color:var(--ink-3);font-size:16px;font-weight:400;'>/ 50</span>"
            f"</div>"
            f"<div style='height:4px;background:var(--bg-sunken);border-radius:999px;"
            f"margin:8px 0 10px 0;overflow:hidden;'>"
            f"<div style='width:{min(100, total/50*100):.0f}%;height:100%;"
            f"background:{_tone_for(total, 50)};border-radius:999px;'></div></div>"
            f"<div style='font-size:12px;color:var(--ink-3);line-height:1.4;'>Profil "
            f"<b style='color:var(--ink);'>{profile or '—'}</b></div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with c_sub:
        cells = st.columns(4)
        for (label, val, maxv), cell in zip(subs, cells):
            tone = _tone_for(val, maxv)
            pct = (val / maxv * 100) if maxv else 0
            with cell:
                st.markdown(
                    f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
                    f"border-radius:10px;padding:14px 16px;height:100%;'>"
                    f"<div class='label-xs' style='margin-bottom:4px;'>{label}</div>"
                    f"<div style='font-size:20px;font-weight:600;color:var(--ink);"
                    f"font-variant-numeric:tabular-nums;'>"
                    f"{val:.0f} <span style='color:var(--ink-3);font-size:13px;font-weight:400;'>/ {maxv}</span>"
                    f"</div>"
                    f"<div style='height:3px;background:var(--bg-sunken);border-radius:999px;"
                    f"margin-top:10px;overflow:hidden;'>"
                    f"<div style='width:{pct:.0f}%;height:100%;background:{tone};"
                    f"border-radius:999px;'></div></div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    # ═══════════════════════════════════════════════════════════════════
    # Tableau ratios calcules avec "Position vs secteur" visuelle
    # ═══════════════════════════════════════════════════════════════════
    section_heading("Ratios calculés", spacing="loose")

    def _status_cell(flag: str) -> str:
        tone = {"OK": "up", "Vigilance": "warn", "Risque": "down"}.get(flag, "neutral")
        label = {"OK": "Bon", "Vigilance": "Vigilance",
                 "Risque": "À risque", "—": "N/A"}.get(flag, flag)
        return f"<span class='dot {tone}'></span>{label}"

    def _position_bar(ratio_key: str, value, prefer_low: bool) -> str:
        """Rend une petite barre horizontale avec la position du titre vs
        min/médiane/max du secteur. Vide si pas de benchmark."""
        if value is None or not benchmarks:
            return ""
        bench = benchmarks.get("sector", {}).get(ratio_key) or benchmarks.get("global", {}).get(ratio_key)
        if not bench:
            return ""
        lo, hi, med = bench["min"], bench["max"], bench["median"]
        if hi == lo:
            return ""
        # clamp position 0-100% dans [min, max]
        v_clamped = max(lo, min(hi, value))
        pos = (v_clamped - lo) / (hi - lo) * 100
        med_pos = (med - lo) / (hi - lo) * 100
        # Couleur favorable : vert si dans la bonne moitié
        is_good = (value < med and prefer_low) or (value > med and not prefer_low)
        fill_color = "var(--up)" if is_good else "var(--down)"
        return (
            f"<div style='position:relative;width:100%;height:8px;"
            f"background:var(--bg-sunken);border-radius:999px;overflow:hidden;'>"
            # track fill up to value
            f"<div style='position:absolute;left:0;top:0;width:{pos:.0f}%;height:100%;"
            f"background:{fill_color};border-radius:999px;opacity:0.85;'></div>"
            # median marker
            f"<div style='position:absolute;left:{med_pos:.0f}%;top:-2px;width:2px;"
            f"height:12px;background:var(--ink-3);'></div>"
            f"</div>"
        )

    def _ecart_cell(ratio_key, value, prefer_low) -> str:
        """Ecart vs sectueur en valeur courte (même unité que la valeur)."""
        cmp = compare_to_sector(ratio_key, value, benchmarks, prefer_low=prefer_low)
        if not cmp or value is None:
            return "<span class='muted'>—</span>"
        med = cmp["median"]
        diff = cmp["diff"]
        if ratio_key in ("roe", "net_margin", "dividend_yield", "fcf_margin", "payout_ratio"):
            delta = (value - med) * 100
            sign = "+" if delta >= 0 else ""
            summary = f"{sign}{delta:.1f} pts vs méd."
        elif ratio_key in ("per",):
            delta = value - med
            sign = "+" if delta >= 0 else ""
            summary = f"{sign}{delta:.1f} vs méd."
        else:
            delta = value - med
            sign = "+" if delta >= 0 else ""
            summary = f"{sign}{delta:.2f}× vs méd."
        is_good = (diff < -0.05 and prefer_low) or (diff > 0.05 and not prefer_low)
        is_bad = (diff > 0.05 and prefer_low) or (diff < -0.05 and not prefer_low)
        color = "var(--up)" if is_good else "var(--down)" if is_bad else "var(--ink-3)"
        return f"<span style='color:{color};font-weight:500;font-variant-numeric:tabular-nums;'>{summary}</span>"

    flags = ratios.get("flags", {})
    # (name, key, value_fmt, seuil, prefer_low)
    ratio_rows = [
        ("Marge nette",    "net_margin",      ratios.get("net_margin"),      "pct",     "≥ 10%",       False),
        ("Dette/Equity",   "debt_equity",     ratios.get("debt_equity"),     "x",       "≤ 1.5×",      True),
        ("Dividend Yield", "dividend_yield",  ratios.get("dividend_yield"),  "pct",     "≥ 6%",        False),
        ("PER",            "per",             ratios.get("per"),             "decimal", "≤ 15",        True),
        ("Payout ratio",   "payout_ratio",    ratios.get("payout_ratio"),    "pct",     "≤ 70%",       True),
        ("EPS",            None,              ratios.get("eps"),             "number",  "—",           False),
        ("DPS",            None,              ratios.get("dps"),             "number",  "—",           False),
        ("ROE",            "roe",             ratios.get("roe"),             "pct",     "≥ 15%",       False),
        ("FCF Margin",     "fcf_margin",      ratios.get("fcf_margin"),      "pct",     "≥ 5%",        False),
    ]

    header_style = (
        "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
        "color:var(--ink-3);font-weight:500;padding:8px 10px;"
        "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
        "text-align:left;"
    )
    cell_style = "padding:9px 10px;border-bottom:1px solid var(--border);font-size:13px;"

    rows_html = (
        f"<tr>"
        f"<th style='{header_style}'>Indicateur</th>"
        f"<th style='{header_style};text-align:right;'>Valeur</th>"
        f"<th style='{header_style}'>Seuil</th>"
        f"<th style='{header_style}'>Position vs secteur</th>"
        f"<th style='{header_style}'>Statut</th>"
        f"<th style='{header_style}'>Écart</th>"
        f"</tr>"
    )
    for name, key, value, fmt, seuil, prefer_low in ratio_rows:
        flag = flags.get(key, ("OK", "")) if key else ("OK", "")
        val_str = format_ratio(value, fmt)
        bar = _position_bar(key, value, prefer_low) if key else ""
        bar_html = bar if bar else "<span class='muted'>—</span>"
        ecart = _ecart_cell(key, value, prefer_low) if key else "<span class='muted'>—</span>"
        rows_html += (
            f"<tr>"
            f"<td style='{cell_style};font-weight:500;'>{name}</td>"
            f"<td style='{cell_style};text-align:right;font-variant-numeric:tabular-nums;'>{val_str}</td>"
            f"<td style='{cell_style};color:var(--ink-3);'>{seuil}</td>"
            f"<td style='{cell_style};min-width:120px;'>{bar_html}</td>"
            f"<td style='{cell_style};'>{_status_cell(flag[0])}</td>"
            f"<td style='{cell_style};'>{ecart}</td>"
            f"</tr>"
        )

    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:10px;"
        f"overflow:hidden;background:var(--bg-elev);'>"
        f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
        unsafe_allow_html=True,
    )

    # ═══════════════════════════════════════════════════════════════════
    # Trajectoire financiere (4 ans) + Structure du bilan (side panel)
    # ═══════════════════════════════════════════════════════════════════
    fiscal_year = fundamentals.get("fiscal_year")
    if fiscal_year:
        fy = int(fiscal_year)
        year_labels = [fy - 3, fy - 2, fy - 1, fy]
    else:
        year_labels = []

    col_traj, col_bilan = st.columns([3, 2])

    with col_traj:
        if year_labels:
            section_heading(f"Trajectoire financière · 4 ans", spacing="loose")
        else:
            section_heading("Trajectoire financière", spacing="loose")

        rev = [fundamentals.get(f"revenue_{s}") for s in ("n3", "n2", "n1", "n0")]
        ni  = [fundamentals.get(f"net_income_{s}") for s in ("n3", "n2", "n1", "n0")]
        dps = [fundamentals.get(f"dps_{s}") for s in ("n3", "n2", "n1", "n0")]
        # BNPA = net_income / shares (uniquement N0 connu)
        shares = fundamentals.get("shares") or 0
        bnpa = [(v / shares) if (v and shares) else None for v in ni]
        # Yield historique : dps / price (approx — on utilise price actuel comme proxy)
        price_now = fundamentals.get("price") or 0
        yld = [(d / price_now) if (d and price_now) else None for d in dps]

        def _md(v, unit=""):
            if v is None or not v:
                return "—"
            if unit == "md":
                return f"{v/1e9:.1f}"
            if unit == "pct":
                return f"{v*100:.1f}%"
            if unit == "int":
                return f"{v:.0f}"
            return f"{v:,.0f}"

        header = "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;color:var(--ink-3);font-weight:500;padding:7px 10px;text-align:left;"
        cell = "padding:8px 10px;font-size:13px;border-bottom:1px solid var(--border);"
        years_hdr = "".join(f"<th style='{header};text-align:right;'>{y}</th>" for y in year_labels) if year_labels else ""
        rows = [
            ("CA (Md)", [_md(v, "md") for v in rev]),
            ("Rés. net (Md)", [_md(v, "md") for v in ni]),
            ("BNPA", [_md(v, "int") for v in bnpa]),
            ("DPS", [_md(v, "int") for v in dps]),
            ("Yield", [_md(v, "pct") for v in yld]),
        ]
        inner = ""
        for label, vals in rows:
            cells_html = "".join(
                f"<td style='{cell};text-align:right;font-variant-numeric:tabular-nums;'>{v}</td>"
                for v in vals
            )
            inner += f"<tr><td style='{cell};font-weight:500;'>{label}</td>{cells_html}</tr>"

        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:10px;overflow:hidden;"
            f"background:var(--bg-elev);'><table style='width:100%;border-collapse:collapse;'>"
            f"<tr><th style='{header};background:var(--bg-sunken);'>Exercice</th>{years_hdr}</tr>"
            f"{inner}</table></div>",
            unsafe_allow_html=True,
        )

    with col_bilan:
        section_heading("Structure du bilan", spacing="loose")
        eq = fundamentals.get("equity") or 0
        debt = fundamentals.get("total_debt")
        cap = fundamentals.get("market_cap") or 0
        float_pct = fundamentals.get("float_pct")

        def _big(v, unit="md", currency=True):
            if v is None or not v:
                return "—"
            if unit == "md":
                return f"{v/1e9:,.1f} Md"
            if unit == "pct":
                return f"{v:.1f}%"
            return f"{v:,.0f}"

        items = [
            ("Capitaux propres", _big(eq, "md")),
            ("Dette financière", _big(debt, "md") if debt else "0 FCFA"),
            ("Nb titres", f"{shares/1e6:,.1f} M" if shares else "—"),
            ("Flottant", _big(float_pct, "pct") if float_pct else "—"),
        ]
        rows_html = ""
        for key, val in items:
            rows_html += (
                f"<div style='display:flex;justify-content:space-between;padding:10px 14px;"
                f"border-bottom:1px solid var(--border);font-size:13px;'>"
                f"<span style='color:var(--ink-2);'>{key}</span>"
                f"<span style='font-weight:500;font-variant-numeric:tabular-nums;color:var(--ink);'>{val}</span>"
                f"</div>"
            )
        st.markdown(
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;overflow:hidden;'>{rows_html}</div>",
            unsafe_allow_html=True,
        )

    # ═══════════════════════════════════════════════════════════════════
    # Lecture synthetique (narratif)
    # ═══════════════════════════════════════════════════════════════════
    yield_val = ratios.get("dividend_yield") or 0
    per = ratios.get("per") or 0
    payout = ratios.get("payout_ratio") or 0
    margin = ratios.get("net_margin") or 0

    phrases = []
    if margin >= 0.10 and yield_val >= 0.05:
        phrases.append("Société **génératrice de cash** et rémunératrice")
    elif margin >= 0.10:
        phrases.append("Société **rentable**")
    if debt is None or not debt:
        phrases.append("**sans dette**")
    elif ratios.get("debt_equity") and ratios["debt_equity"] <= 0.5:
        phrases.append("**peu endettée**")
    if per and per > 20:
        phrases.append(f"valorisation tendue (**PER {per:.1f}**)")
    if payout and payout > 1.0:
        phrases.append(f"**payout > 100%** fragilisent la soutenabilité du dividende à rythme actuel")

    if phrases:
        narrative = ". ".join(phrases[:3]) + "."
        st.markdown(
            f"<div style='background:var(--bg-sunken);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;margin-top:18px;'>"
            f"<div class='label-xs' style='margin-bottom:6px;'>Lecture synthétique</div>"
            f"<div style='font-size:13px;line-height:1.5;color:var(--ink-2);'>{narrative}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )


def _render_technical(ticker, price_df, result):
    """Onglet analyse technique."""
    from datetime import datetime, timedelta
    from analysis.technical import _detect_frequency, SMA_LABELS

    if price_df.empty or len(price_df) < 5:
        st.warning("Données de prix insuffisantes pour l'analyse technique.")
        if not is_admin():
            st.info("L'administrateur doit charger les prix historiques.")
            return
        st.info("Cliquez sur le bouton ci-dessous pour charger les prix historiques.")
        if st.button("Charger les prix (5 ans mensuel)"):
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

    # ═══════════════════════════════════════════════════════════════════
    # 4 KPI cards : Tendance CT · RSI · MACD · Volatilité 30j
    # ═══════════════════════════════════════════════════════════════════
    last_row = df.iloc[-1] if not df.empty else None
    trend = result.get("trend", {})
    trend_name = (trend.get("trend") or "N/A").capitalize()
    trend_strength = (trend.get("strength") or "").capitalize()

    # Calcul MM20 vs MM50 pour sous-texte tendance
    mm_sub = "—"
    if last_row is not None and "sma20" in df.columns and "sma50" in df.columns:
        sma20 = last_row.get("sma20")
        sma50 = last_row.get("sma50")
        if pd.notna(sma20) and pd.notna(sma50):
            if sma20 > sma50:
                mm_sub = f"{sma_labels['short']} > {sma_labels['medium']}"
            else:
                mm_sub = f"{sma_labels['short']} < {sma_labels['medium']}"

    # Valeurs RSI / MACD / Histogramme
    rsi_val = float(last_row["rsi"]) if (last_row is not None and "rsi" in df.columns and pd.notna(last_row.get("rsi"))) else None
    macd_val = float(last_row["macd"]) if (last_row is not None and "macd" in df.columns and pd.notna(last_row.get("macd"))) else None
    macd_hist = float(last_row["macd_histogram"]) if (last_row is not None and "macd_histogram" in df.columns and pd.notna(last_row.get("macd_histogram"))) else None

    # Volatilité 30 jours (std des rendements journaliers annualisée sur 30 points)
    vol_30 = None
    if len(df) >= 30 and "close" in df.columns:
        try:
            returns = df["close"].pct_change().dropna().tail(30)
            if len(returns) >= 5:
                vol_30 = returns.std() * (252 ** 0.5) * 100  # annualisée %
        except Exception:
            vol_30 = None

    def _kpi_card(label, value, sub, arrow_tone="neutral"):
        """Card compacte avec label + value + sub ligne (petit + coloré)."""
        arrow = {"up": "▲", "down": "▼"}.get(arrow_tone, "")
        sub_color = {"up": "var(--up)", "down": "var(--down)"}.get(arrow_tone, "var(--ink-3)")
        sub_html = (
            f"<div style='font-size:11.5px;margin-top:6px;color:{sub_color};font-weight:500;'>"
            f"{arrow + ' ' if arrow else ''}{sub}</div>"
            if sub else ""
        )
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;min-height:92px;'>"
            f"<div class='label-xs' style='margin-bottom:6px;'>{label}</div>"
            f"<div style='font-size:22px;font-weight:600;letter-spacing:-0.02em;"
            f"color:var(--ink);font-variant-numeric:tabular-nums;line-height:1.15;'>"
            f"{value}</div>"
            f"{sub_html}"
            f"</div>"
        )

    # Tone tendance
    tr_tone = {"haussiere": "up", "baissiere": "down"}.get(trend.get("trend"), "neutral")
    # RSI status
    if rsi_val is None:
        rsi_value_str, rsi_sub, rsi_tone = "—", "", "neutral"
    elif rsi_val >= 70:
        rsi_value_str, rsi_sub, rsi_tone = f"{rsi_val:.1f}", "Surachat", "down"
    elif rsi_val <= 30:
        rsi_value_str, rsi_sub, rsi_tone = f"{rsi_val:.1f}", "Survente", "up"
    else:
        rsi_value_str, rsi_sub, rsi_tone = f"{rsi_val:.1f}", "Neutre", "neutral"
    # MACD status
    if macd_val is None:
        macd_value_str, macd_sub, macd_tone = "—", "", "neutral"
    else:
        sign = "+" if macd_val >= 0 else ""
        macd_value_str = f"{sign}{macd_val:,.1f}"
        if macd_hist is not None and macd_hist > 0:
            macd_sub, macd_tone = "Hist. positif", "up"
        elif macd_hist is not None and macd_hist < 0:
            macd_sub, macd_tone = "Hist. négatif", "down"
        else:
            macd_sub, macd_tone = "", "neutral"
    # Volatilité status (relatif à 15% arbitraire)
    if vol_30 is None:
        vol_value_str, vol_sub, vol_tone = "—", "", "neutral"
    else:
        vol_value_str = f"{vol_30:.1f}%"
        if vol_30 > 25:
            vol_sub, vol_tone = "Élevée", "down"
        elif vol_30 < 10:
            vol_sub, vol_tone = "Faible", "up"
        else:
            vol_sub, vol_tone = "Médiane 12-20%", "neutral"

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(
            _kpi_card("Tendance CT", trend_name, mm_sub, arrow_tone=tr_tone),
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(_kpi_card("RSI (14)", rsi_value_str, rsi_sub, arrow_tone=rsi_tone),
                    unsafe_allow_html=True)
    with k3:
        st.markdown(_kpi_card("MACD", macd_value_str, macd_sub, arrow_tone=macd_tone),
                    unsafe_allow_html=True)
    with k4:
        st.markdown(_kpi_card("Volatilité 30j", vol_value_str, vol_sub, arrow_tone=vol_tone),
                    unsafe_allow_html=True)

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

    # ═══════════════════════════════════════════════════════════════════
    # Niveaux clés (support/résistance + actuel) — tableau editorial
    # ═══════════════════════════════════════════════════════════════════
    supports = sorted(result.get("supports", []), reverse=True)[:2]
    resistances = sorted(result.get("resistances", []))[:2]
    current_price = 0
    if not df_display.empty and "close" in df_display.columns:
        current_price = float(df_display.iloc[-1]["close"])

    col_lvl, col_sig = st.columns([1, 1])
    with col_lvl:
        section_heading("Niveaux clés")

        def _ecart(px):
            if not current_price or not px:
                return "—", "var(--ink-3)"
            diff = (px - current_price) / current_price * 100
            sign = "+" if diff >= 0 else ""
            color = "var(--up)" if diff < 0 else "var(--down)"  # inverse : sous = bon pour achat
            return f"{sign}{diff:.1f}%", color

        rows_lvl = []
        # Résistances (haut en bas)
        for i, r in enumerate(sorted(resistances, reverse=True)):
            ec, col = _ecart(r)
            rows_lvl.append(("Résistance", f"R{len(resistances)-i}", r, ec, col,
                             "Plafond technique"))
        # Actuel
        rows_lvl.append(("Cours", "Actuel", current_price, "—", "var(--ink-3)",
                         "Dernière séance"))
        # Supports
        for i, s in enumerate(supports):
            ec, col = _ecart(s)
            rows_lvl.append(("Support", f"S{i+1}", s, ec, col, "Zone de rebond"))

        header_style = (
            "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
            "color:var(--ink-3);font-weight:500;padding:8px 10px;"
            "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
            "text-align:left;"
        )
        cell_style = "padding:9px 10px;border-bottom:1px solid var(--border);font-size:13px;"

        rows_html = (
            f"<tr>"
            f"<th style='{header_style}'>Type</th>"
            f"<th style='{header_style}'>Niveau</th>"
            f"<th style='{header_style};text-align:right;'>Prix</th>"
            f"<th style='{header_style};text-align:right;'>Écart</th>"
            f"<th style='{header_style}'>Commentaire</th>"
            f"</tr>"
        )
        for type_, niveau, price, ec_str, ec_color, comment in rows_lvl:
            tone = "up" if type_ == "Support" else ("down" if type_ == "Résistance" else "neutral")
            px_str = f"{price:,.0f}" if price else "—"
            rows_html += (
                f"<tr>"
                f"<td style='{cell_style};color:var(--ink-3);'>{type_}</td>"
                f"<td style='{cell_style};'><span class='dot {tone}'></span><b>{niveau}</b></td>"
                f"<td style='{cell_style};text-align:right;font-variant-numeric:tabular-nums;'>{px_str}</td>"
                f"<td style='{cell_style};text-align:right;color:{ec_color};"
                f"font-weight:500;font-variant-numeric:tabular-nums;'>{ec_str}</td>"
                f"<td style='{cell_style};color:var(--ink-3);'>{comment}</td>"
                f"</tr>"
            )
        if len(rows_lvl) <= 1:
            # Pas de supports/résistances détectés
            pass
        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:10px;"
            f"overflow:hidden;background:var(--bg-elev);'>"
            f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
            unsafe_allow_html=True,
        )

    # ═══════════════════════════════════════════════════════════════════
    # Signaux techniques — editorial list avec value a droite
    # ═══════════════════════════════════════════════════════════════════
    with col_sig:
        section_heading("Signaux techniques")
        signals = result.get("signals", [])
        if signals:
            inner = ""
            for sig in signals:
                tone = {"achat": "up", "vente": "down", "info": "neutral"}.get(sig.get("type"), "neutral")
                inner += (
                    f"<div style='display:flex;justify-content:space-between;align-items:flex-start;"
                    f"gap:12px;padding:9px 12px;border-bottom:1px solid var(--border);"
                    f"font-size:13px;'>"
                    f"<div style='flex:1;min-width:0;'>"
                    f"<span class='dot {tone}'></span><b>{sig.get('signal', '')}</b>"
                    f"</div>"
                    f"<div style='color:var(--ink-3);font-size:12.5px;text-align:right;"
                    f"max-width:60%;'>{sig.get('details', '')}</div>"
                    f"</div>"
                )
            st.markdown(
                f"<div style='border:1px solid var(--border);border-radius:10px;"
                f"overflow:hidden;background:var(--bg-elev);'>{inner}</div>",
                unsafe_allow_html=True,
            )
        else:
            st.caption("Aucun signal technique actif")

    # Explications RSI & MACD (repliable pour désencombrer)
    with st.expander("En savoir plus · RSI, MACD, Moyennes mobiles", expanded=False):
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
    """Onglet Recommandation v3 : verdict card avec composition + 3 score
    cards descriptives + Points forts/Vigilance en tables + Plan d'action
    zones de prix avec conviction."""
    reco = result["recommendation"]

    fund_s = result.get("fundamental_score") or 0
    tech_s = result.get("technical_score") or 0
    hybrid = result.get("hybrid_score") or 0
    verdict = reco.get("verdict", "N/A")
    verdict_tone = _verdict_tone(verdict)

    # ═══════════════════════════════════════════════════════════════════
    # Card "VERDICT DU MODÈLE" avec stacked bar composition
    # ═══════════════════════════════════════════════════════════════════
    # Composition : Fond (x/50) | Tech (x/50) | Manque (100-total)
    missing = max(0, 100 - hybrid)
    total = 100
    fond_pct = fund_s / total * 100
    tech_pct = tech_s / total * 100
    miss_pct = missing / total * 100

    verdict_color = {
        "up": "var(--up)", "down": "var(--down)", "ocre": "var(--ocre)",
    }.get(verdict_tone, "var(--ink-2)")

    horizon = "6-12 mois"

    st.markdown(
        f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
        f"border-radius:10px;padding:18px 20px;'>"
        f"<div style='display:flex;align-items:flex-start;gap:24px;'>"
        # Colonne gauche : verdict + score + horizon
        f"<div style='min-width:240px;'>"
        f"<div class='label-xs' style='margin-bottom:4px;'>Verdict du modèle</div>"
        f"<div style='font-size:26px;font-weight:600;letter-spacing:-0.02em;"
        f"color:{verdict_color};text-transform:capitalize;'>{verdict.capitalize()}</div>"
        f"<div style='font-size:12.5px;color:var(--ink-3);margin-top:6px;'>"
        f"Score hybride <b style='color:var(--ink);'>{hybrid:.0f} / 100</b> "
        f"<span class='muted'>·</span> horizon {horizon}"
        f"</div>"
        f"</div>"
        # Colonne droite : composition
        f"<div style='flex:1;'>"
        f"<div class='label-xs' style='margin-bottom:8px;'>Composition du score hybride</div>"
        # Stacked bar
        f"<div style='display:flex;height:22px;border-radius:4px;overflow:hidden;"
        f"border:1px solid var(--border);'>"
        f"<div style='width:{fond_pct:.1f}%;background:var(--ocre-bg);"
        f"display:flex;align-items:center;justify-content:center;"
        f"font-size:11px;font-weight:500;color:#6a4f13;'>"
        f"{'Fond. ' + str(int(fund_s)) if fond_pct >= 10 else ''}"
        f"</div>"
        f"<div style='width:{tech_pct:.1f}%;background:var(--primary-bg);"
        f"display:flex;align-items:center;justify-content:center;"
        f"font-size:11px;font-weight:500;color:var(--primary-2);'>"
        f"{'Tech. ' + str(int(tech_s)) if tech_pct >= 10 else ''}"
        f"</div>"
        f"<div style='width:{miss_pct:.1f}%;background:var(--bg-sunken);"
        f"display:flex;align-items:center;justify-content:center;"
        f"font-size:11px;font-weight:500;color:var(--ink-3);'>"
        f"{'Manque ' + str(int(missing)) if miss_pct >= 10 else ''}"
        f"</div>"
        f"</div>"
        f"<div style='display:flex;gap:16px;margin-top:8px;font-size:11.5px;"
        f"color:var(--ink-3);'>"
        f"<span><span class='dot ocre'></span>Fondamental {fund_s:.0f}/50</span>"
        f"<span><span class='dot up'></span>Technique {tech_s:.0f}/50</span>"
        f"</div>"
        f"</div>"
        f"</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ═══════════════════════════════════════════════════════════════════
    # Card "PRIX CIBLE" — modèle PER sectoriel + Yield cible
    # ═══════════════════════════════════════════════════════════════════
    ratios_src = result.get("ratios") or {}
    if "price" not in ratios_src or not ratios_src.get("price"):
        ratios_src = dict(ratios_src)
        ratios_src["price"] = fundamentals.get("price") or 0
    tgt = compute_target_price(ratios_src, sector=fundamentals.get("sector"))
    tp = tgt.get("target_price")
    cur = tgt.get("current_price") or 0
    conf = tgt.get("confidence", "moyenne")
    comps = tgt.get("components", [])

    # Détaille les prix de chaque méthode en FCFA, avec formule
    comps_rows = "".join(
        f"<tr>"
        f"<td style='padding:4px 14px 4px 0;color:var(--ink-3);font-size:11px;"
        f"text-transform:uppercase;letter-spacing:0.05em;font-weight:600;"
        f"white-space:nowrap;'>{c['method']}</td>"
        f"<td style='padding:4px 14px 4px 0;color:var(--ink-2);font-size:12px;"
        f"font-variant-numeric:tabular-nums;'>{c['formula']}</td>"
        f"<td style='padding:4px 0;color:var(--ink);font-size:13px;font-weight:600;"
        f"font-variant-numeric:tabular-nums;text-align:right;'>"
        f"{c['price']:,.0f} FCFA</td>"
        f"</tr>"
        for c in comps
    )

    # ── Cas 1 : pas de données ──
    if not tp:
        st.markdown(
            "<div style='background:var(--bg-elev);border:1px solid var(--border);"
            "border-radius:10px;padding:14px 18px;margin-top:12px;"
            "color:var(--ink-3);font-size:13px;'>"
            "Prix cible indisponible — données EPS ou DPS manquantes."
            "</div>",
            unsafe_allow_html=True,
        )
    # ── Cas 2 : confiance faible → PAS de moyenne trompeuse, on montre la fourchette ──
    elif conf == "faible" and len(comps) >= 2:
        prices_only = [c["price"] for c in comps]
        lo, hi = min(prices_only), max(prices_only)
        st.markdown(
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-left:4px solid var(--ocre);"
            f"border-radius:10px;padding:16px 20px;margin-top:12px;'>"
            f"<div style='display:flex;align-items:flex-start;gap:32px;flex-wrap:wrap;'>"
            f"<div>"
            f"<div class='label-xs' style='margin-bottom:3px;'>Prix actuel</div>"
            f"<div style='font-size:22px;font-weight:600;color:var(--ink);"
            f"letter-spacing:-0.01em;font-variant-numeric:tabular-nums;'>"
            f"{cur:,.0f} <span style='font-size:12px;color:var(--ink-3);"
            f"font-weight:400;'>FCFA</span></div>"
            f"</div>"
            f"<div>"
            f"<div class='label-xs' style='margin-bottom:3px;'>Fourchette modèle</div>"
            f"<div style='font-size:22px;font-weight:600;color:var(--ink);"
            f"letter-spacing:-0.01em;font-variant-numeric:tabular-nums;'>"
            f"{lo:,.0f} – {hi:,.0f} <span style='font-size:12px;color:var(--ink-3);"
            f"font-weight:400;'>FCFA</span></div>"
            f"</div>"
            f"<div style='flex:1;min-width:240px;'>"
            f"<div class='label-xs' style='margin-bottom:3px;'>Lecture</div>"
            f"<div style='font-size:13px;color:var(--ink);'>"
            f"Méthodes divergentes — pas de cible unique fiable.</div>"
            f"<div style='font-size:11.5px;color:var(--ink-3);margin-top:4px;'>"
            f"Confiance <b style='color:var(--ocre);'>faible</b>. "
            f"EPS ou DPS probablement non représentatifs (division d'action, "
            f"exercice exceptionnel, données manquantes). À vérifier dans Fondamentale."
            f"</div>"
            f"</div>"
            f"</div>"
            f"<table style='margin-top:12px;border-top:1px solid var(--border);"
            f"padding-top:10px;width:100%;border-collapse:collapse;'>{comps_rows}</table>"
            f"</div>",
            unsafe_allow_html=True,
        )
    # ── Cas 3 : confiance OK → affiche target + delta ──
    else:
        delta_abs = tgt.get("delta_abs") or 0
        delta_pct = tgt.get("delta_pct") or 0
        if delta_pct > 0:
            tone_color, arrow, label_sens = "var(--up)", "▲", "Potentiel haussier"
        elif delta_pct < 0:
            tone_color, arrow, label_sens = "var(--down)", "▼", "Surévalué vs modèle"
        else:
            tone_color, arrow, label_sens = "var(--ink-3)", "—", "Cours à la juste valeur"
        sign = "+" if delta_pct >= 0 else ""
        conf_color = {"élevée": "var(--up)", "moyenne": "var(--ink-2)"}.get(conf, "var(--ink-3)")

        st.markdown(
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:16px 20px;margin-top:12px;'>"
            f"<div style='display:flex;align-items:flex-start;gap:32px;flex-wrap:wrap;'>"
            f"<div>"
            f"<div class='label-xs' style='margin-bottom:3px;'>Prix actuel</div>"
            f"<div style='font-size:22px;font-weight:600;color:var(--ink);"
            f"letter-spacing:-0.01em;font-variant-numeric:tabular-nums;'>"
            f"{cur:,.0f} <span style='font-size:12px;color:var(--ink-3);"
            f"font-weight:400;'>FCFA</span></div>"
            f"</div>"
            f"<div>"
            f"<div class='label-xs' style='margin-bottom:3px;'>Prix cible (modèle)</div>"
            f"<div style='font-size:22px;font-weight:600;color:var(--ink);"
            f"letter-spacing:-0.01em;font-variant-numeric:tabular-nums;'>"
            f"{tp:,.0f} <span style='font-size:12px;color:var(--ink-3);"
            f"font-weight:400;'>FCFA</span></div>"
            f"</div>"
            f"<div>"
            f"<div class='label-xs' style='margin-bottom:3px;'>Delta</div>"
            f"<div style='font-size:22px;font-weight:600;color:{tone_color};"
            f"letter-spacing:-0.01em;font-variant-numeric:tabular-nums;'>"
            f"{arrow} {sign}{delta_pct:.1f}%</div>"
            f"<div style='font-size:11.5px;color:var(--ink-3);margin-top:2px;"
            f"font-variant-numeric:tabular-nums;'>{sign}{delta_abs:,.0f} FCFA</div>"
            f"</div>"
            f"<div style='flex:1;min-width:220px;'>"
            f"<div class='label-xs' style='margin-bottom:3px;'>Lecture</div>"
            f"<div style='font-size:13px;color:var(--ink);'>{label_sens}</div>"
            f"<div style='font-size:11.5px;color:var(--ink-3);margin-top:4px;'>"
            f"Confiance <b style='color:{conf_color};'>{conf}</b>"
            f"</div>"
            f"</div>"
            f"</div>"
            # Détail des méthodes
            f"<table style='margin-top:12px;border-top:1px solid var(--border);"
            f"padding-top:10px;width:100%;border-collapse:collapse;'>{comps_rows}</table>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # ═══════════════════════════════════════════════════════════════════
    # 3 cards : Score fondamental · Score technique · Conviction modèle
    # ═══════════════════════════════════════════════════════════════════
    bd = (fundamentals.get("fundamental_breakdown")
          or result.get("ratios", {}).get("fundamental_breakdown") or {})
    # fallback : utilise le quality/profile si présent
    fund_profile = bd.get("profile") or f"Score brut {fund_s:.0f}/50"
    # Profil technique
    if tech_s >= 35:
        tech_label, tech_sub, tech_arrow = "Fort", "Tendance haussière", "up"
    elif tech_s >= 25:
        tech_label, tech_sub, tech_arrow = "Correct", "Momentum modéré", "up"
    elif tech_s >= 15:
        tech_label, tech_sub, tech_arrow = "Neutre", "Pas de signal fort", "neutral"
    else:
        tech_label, tech_sub, tech_arrow = "Faible", "Tendance baissière", "down"
    tech_profile = f"{tech_label} — {tech_sub}"
    # Conviction : 5 dots selon hybrid_score
    conviction_pts = 5 if hybrid >= 80 else 4 if hybrid >= 65 else 3 if hybrid >= 50 else 2 if hybrid >= 35 else 1
    conviction_label = {
        5: "Très forte", 4: "Forte", 3: "Moyenne", 2: "Faible", 1: "Très faible",
    }[conviction_pts]
    conv_sub = {
        5: "Position de conviction",
        4: "Accumulation progressive",
        3: "Attendre un retracement",
        2: "Surveiller avant d'entrer",
        1: "Éviter pour l'instant",
    }[conviction_pts]

    def _score_card(label, value, max_value, profile, arrow_tone, show_dots=False):
        if show_dots:
            dots = "".join(
                f'<span class="dot {"up" if i < value else "neutral"}" '
                f'style="width:9px;height:9px;margin-right:3px;"></span>'
                for i in range(max_value)
            )
            value_html = dots
        else:
            value_html = (
                f"<span style='font-size:24px;font-weight:600;letter-spacing:-0.02em;"
                f"color:var(--ink);font-variant-numeric:tabular-nums;'>"
                f"{value:.0f}</span>"
                f"<span style='color:var(--ink-3);font-size:14px;font-weight:400;'> / {max_value}</span>"
            )
        arrow = {"up": "▲", "down": "▼"}.get(arrow_tone, "")
        sub_color = {"up": "var(--up)", "down": "var(--down)"}.get(arrow_tone, "var(--ink-3)")
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;height:100%;'>"
            f"<div class='label-xs' style='margin-bottom:6px;'>{label}</div>"
            f"<div>{value_html}</div>"
            f"<div style='font-size:12px;color:{sub_color};margin-top:6px;font-weight:500;'>"
            f"{arrow + ' ' if arrow else ''}{profile}</div>"
            f"</div>"
        )

    c1, c2, c3 = st.columns(3)
    with c1:
        fund_tone = "up" if fund_s >= 35 else "down" if fund_s <= 15 else "neutral"
        st.markdown(
            _score_card("Score fondamental", fund_s, 50, fund_profile, fund_tone),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            _score_card("Score technique", tech_s, 50, tech_profile, tech_arrow),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            _score_card("Conviction modèle", conviction_pts, 5, conv_sub,
                        "up" if conviction_pts >= 4 else "neutral", show_dots=True),
            unsafe_allow_html=True,
        )

    # ═══════════════════════════════════════════════════════════════════
    # Points forts / Points de vigilance — en tables éditoriales
    # ═══════════════════════════════════════════════════════════════════
    def _pts_card(label, items, tone):
        """Card avec titre dot + items. Chaque item = label principal
        + détail à droite (si ' — ' dans la string)."""
        rows = ""
        if items:
            for item in items:
                # Tente de splitter "Label — détail" / "Label (valeur)"
                main, side = item, ""
                if " — " in item:
                    main, side = item.split(" — ", 1)
                elif " (" in item and item.endswith(")"):
                    idx = item.rindex(" (")
                    main, side = item[:idx], item[idx + 2 : -1]
                rows += (
                    f"<div style='display:flex;justify-content:space-between;"
                    f"gap:12px;padding:8px 0;border-bottom:1px solid var(--border);'>"
                    f"<span style='color:var(--ink);font-size:13px;'>{main}</span>"
                    f"<span style='color:var(--ink-3);font-size:12.5px;text-align:right;"
                    f"font-variant-numeric:tabular-nums;'>{side}</span>"
                    f"</div>"
                )
        else:
            rows = "<div style='padding:10px 0;color:var(--ink-3);font-size:13px;'>Aucun élément</div>"
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;'>"
            f"<div style='font-size:14px;font-weight:600;color:var(--ink);"
            f"margin-bottom:6px;'><span class='dot {tone}'></span>{label}</div>"
            f"{rows}"
            f"</div>"
        )

    col_s, col_w = st.columns(2)
    with col_s:
        st.markdown(
            _pts_card("Points forts", reco.get("strengths", []), "up"),
            unsafe_allow_html=True,
        )
    with col_w:
        st.markdown(
            _pts_card("Points de vigilance", reco.get("warnings", []), "warn"),
            unsafe_allow_html=True,
        )

    # ═══════════════════════════════════════════════════════════════════
    # Plan d'action — Zones de prix avec conviction
    # ═══════════════════════════════════════════════════════════════════
    section_heading("Plan d'action · zones de prix", spacing="loose")
    entry_zones = reco.get("entry_zones", [])
    current_price = fundamentals.get("price") or 0

    if entry_zones:
        header_style = (
            "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
            "color:var(--ink-3);font-weight:500;padding:8px 10px;"
            "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
            "text-align:left;"
        )
        cell_style = "padding:10px;border-bottom:1px solid var(--border);font-size:13px;"

        rows_html = (
            f"<tr>"
            f"<th style='{header_style}'>Niveau</th>"
            f"<th style='{header_style};text-align:right;'>Prix cible</th>"
            f"<th style='{header_style};text-align:right;'>Écart vs cours</th>"
            f"<th style='{header_style}'>Risk / Reward</th>"
            f"<th style='{header_style}'>Conviction</th>"
            f"<th style='{header_style}'>Commentaire</th>"
            f"</tr>"
        )
        for zone in entry_zones:
            # Extract numeric price from zone (ex: "1,945 FCFA" → 1945)
            zone_str = str(zone.get("zone", ""))
            try:
                px = float("".join(c for c in zone_str if c.isdigit() or c == "."))
            except Exception:
                px = 0

            if current_price and px:
                diff = (px - current_price) / current_price * 100
                sign = "+" if diff >= 0 else ""
                ecart_str = f"{sign}{diff:.1f}%"
                ecart_color = "var(--up)" if diff < 0 else "var(--down)"
            else:
                ecart_str, ecart_color = "—", "var(--ink-3)"

            rr = zone.get("risk_reward", "Neutre")
            rr_tone = {"Favorable": "up", "Très favorable": "up",
                       "Défavorable": "down", "Neutre": "neutral"}.get(rr, "neutral")

            # Conviction = inférée depuis risk_reward
            conviction_n = 5 if "Très favorable" in rr else 4 if "Favorable" in rr else 2 if "Défavorable" in rr else 3
            dots = "".join(
                f'<span style="display:inline-block;width:6px;height:6px;'
                f'background:{"var(--primary)" if i < conviction_n else "var(--ink-4)"};'
                f'border-radius:50%;margin-right:2px;"></span>'
                for i in range(5)
            )

            label = zone.get("label", "")
            comment = label  # fallback description

            rows_html += (
                f"<tr>"
                f"<td style='{cell_style};font-weight:500;'>{label}</td>"
                f"<td style='{cell_style};text-align:right;font-variant-numeric:tabular-nums;'>"
                f"{zone_str}</td>"
                f"<td style='{cell_style};text-align:right;color:{ecart_color};"
                f"font-weight:500;font-variant-numeric:tabular-nums;'>{ecart_str}</td>"
                f"<td style='{cell_style};'>"
                + _tag_html(rr, rr_tone) +
                f"</td>"
                f"<td style='{cell_style};'>{dots}</td>"
                f"<td style='{cell_style};color:var(--ink-3);'>{comment}</td>"
                f"</tr>"
            )

        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:10px;"
            f"overflow:hidden;background:var(--bg-elev);'>"
            f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
            unsafe_allow_html=True,
        )
    else:
        st.caption("Pas assez de données pour déterminer les zones d'entrée")

    # Synthèse narrative
    synth = (
        f"Le verdict **{verdict}** reflète un fondamental "
        f"{'solide' if fund_s >= 35 else 'moyen' if fund_s >= 20 else 'faible'} "
        f"{'combiné à' if (fund_s >= 20 and tech_s >= 20) else 'confronté à'} "
        f"une tendance technique "
        f"{'favorable' if tech_s >= 30 else 'neutre' if tech_s >= 20 else 'défavorable'}. "
    )
    st.markdown(
        f"<div style='margin-top:16px;padding:12px 16px;background:var(--bg-sunken);"
        f"border-radius:10px;font-size:12.5px;color:var(--ink-2);line-height:1.5;'>"
        f"{synth}</div>",
        unsafe_allow_html=True,
    )


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

        submitted = st.form_submit_button("Enregistrer", type="primary")
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


def _render_publications_with_status(ticker: str):
    """Affiche les publications du ticker (depuis `publications` enrichie de
    richbourse) avec leur date, type et statut d'intégration en base.

    Statuts :
      - 🆕 nouveau   : pub.is_new = 1 (jamais vue avant)
      - ⏳ à intégrer : annuel dont fiscal_year > max(fundamentals.fiscal_year),
                        OU trimestriel/semestriel non présent dans quarterly_data
      - ✅ intégré    : sinon
    """
    pubs = read_sql_df(
        """SELECT id, title, pub_type, fiscal_year, url, pub_date, is_new
           FROM publications
           WHERE ticker = ? AND COALESCE(ignored, 0) = 0
           ORDER BY pub_date DESC NULLS LAST, created_at DESC
           LIMIT 20""",
        params=(ticker,),
        parse_dates=["pub_date"],
    )
    if pubs.empty:
        return

    # Charge les indicateurs d'intégration pour ce ticker
    fund_years = read_sql_df(
        "SELECT fiscal_year FROM fundamentals WHERE ticker = ? AND revenue IS NOT NULL",
        params=(ticker,),
    )
    fund_max = (
        int(fund_years["fiscal_year"].max())
        if not fund_years.empty and fund_years["fiscal_year"].notna().any() else None
    )
    quart_years = read_sql_df(
        "SELECT DISTINCT fiscal_year FROM quarterly_data WHERE ticker = ?",
        params=(ticker,),
    )
    quart_set = set(quart_years["fiscal_year"].dropna().astype(int).tolist())

    def _status(row):
        if row.get("is_new"):
            return ("🆕", "Nouveau", "warn")
        pt = (row.get("pub_type") or "").lower()
        fy = row.get("fiscal_year")
        try:
            fy_int = int(fy) if fy and not pd.isna(fy) else None
        except Exception:
            fy_int = None
        if pt == "annuel" and fy_int is not None:
            if fund_max is None or fy_int > fund_max:
                return ("⏳", "À intégrer", "down")
            return ("✅", "Intégré", "up")
        if pt in ("trimestriel", "semestriel") and fy_int is not None:
            if fy_int not in quart_set:
                return ("⏳", "À intégrer", "down")
            return ("✅", "Intégré", "up")
        # Types informationnels (gouvernance, dividende, autre) → pas d'intégration attendue
        return ("·", "", "neutral")

    section_heading("Publications & actualités", spacing="loose")

    # KPI ligne : combien à intégrer ?
    statuses = [_status(r) for _, r in pubs.iterrows()]
    n_pending = sum(1 for s in statuses if s[1] == "À intégrer")
    n_new = sum(1 for s in statuses if s[1] == "Nouveau")
    if n_pending or n_new:
        bits = []
        if n_pending:
            bits.append(f"⏳ **{n_pending}** à intégrer")
        if n_new:
            bits.append(f"🆕 **{n_new}** nouveau{'x' if n_new > 1 else ''}")
        st.caption(" · ".join(bits))

    for (_, art), (icon, label, _tone) in zip(pubs.iterrows(), statuses):
        date_raw = art.get("pub_date")
        if isinstance(date_raw, str):
            date_disp = date_raw[:10]
        elif date_raw is not None and not pd.isna(date_raw):
            try:
                date_disp = date_raw.strftime("%d/%m/%Y")
            except Exception:
                date_disp = str(date_raw)[:10]
        else:
            date_disp = "—"
        title = art.get("title") or ""
        url = art.get("url") or ""
        pt = art.get("pub_type") or ""
        # Badge type
        badge = (
            f"<span style='background:var(--bg-sunken);color:var(--ink-3);"
            f"padding:1px 7px;border-radius:4px;font-size:10.5px;font-weight:600;"
            f"text-transform:uppercase;letter-spacing:0.04em;margin-right:6px;'>{pt}</span>"
            if pt else ""
        )
        # Status icon
        status_html = (
            f"<span title='{label}' style='font-size:14px;margin-right:4px;'>{icon}</span>"
            if icon else ""
        )
        # Date
        date_html = (
            f"<span style='color:var(--ink-3);font-variant-numeric:tabular-nums;"
            f"font-size:12px;margin-right:8px;'>{date_disp}</span>"
        )
        if url and isinstance(url, str) and url.startswith("http"):
            title_html = (
                f"<a href='{url}' target='_blank' style='color:var(--ink);"
                f"text-decoration:none;'>{title}</a>"
            )
        else:
            title_html = title
        st.markdown(
            f"<div style='padding:6px 0;border-bottom:1px solid var(--border);"
            f"font-size:13px;line-height:1.5;'>"
            f"{status_html}{date_html}{badge}{title_html}"
            f"</div>",
            unsafe_allow_html=True,
        )


def _render_score_evolution(ticker: str):
    """Affiche l'évolution des scores (hybrid/fond/tech) sur les 90 derniers
    jours pour ce ticker, en bas de l'onglet Recommandation. Les données
    proviennent de verdict_daily (peuplé chaque jour par build_daily_snapshot).
    """
    from analysis.verdict_history import get_score_evolution, has_history
    section_heading("Évolution du score", spacing="loose")
    if not has_history():
        st.caption(
            "Collecte des verdicts quotidiens en cours. Les courbes "
            "apparaîtront après le prochain build (cron 16h UTC ou bouton admin "
            "*Regénérer snapshots*)."
        )
        return
    df = get_score_evolution(ticker, days=90)
    if df.empty:
        st.caption(
            f"Aucun historique pour {ticker} dans verdict_daily. "
            "Les données s'accumulent à partir du prochain build quotidien."
        )
        return
    import plotly.graph_objects as go
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["hybrid_score"], mode="lines+markers",
        name="Score hybride", line=dict(width=3, color="#1F5D3A"),
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["fundamental_score"], mode="lines",
        name="Fondamental", line=dict(width=1.5, color="#7A8C99", dash="dot"),
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["technical_score"], mode="lines",
        name="Technique", line=dict(width=1.5, color="#B5730E", dash="dot"),
    ))
    # Bandes de seuils verdicts (70=ACHAT FORT, 52=ACHAT, 38=CONSERVER, 25=PRUDENCE)
    fig.add_hline(y=70, line_dash="dash", line_color="rgba(31,93,58,0.4)",
                  annotation_text="ACHAT FORT", annotation_position="right")
    fig.add_hline(y=52, line_dash="dash", line_color="rgba(31,93,58,0.25)",
                  annotation_text="ACHAT", annotation_position="right")
    fig.update_layout(
        height=300, margin=dict(l=10, r=80, t=10, b=10),
        yaxis=dict(title="Score / 100", range=[0, 100]),
        xaxis=dict(title=None),
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)
    # Tableau compact des changements de verdict
    df["verdict_changed"] = df["verdict"] != df["verdict"].shift(1)
    transitions = df[df["verdict_changed"]][["date", "verdict", "hybrid_score", "price"]]
    if len(transitions) > 1:
        with st.expander(f"Changements de verdict ({len(transitions) - 1} sur 90j)",
                          expanded=False):
            transitions = transitions.copy()
            transitions["date"] = transitions["date"].dt.strftime("%d/%m/%Y")
            transitions["hybrid_score"] = transitions["hybrid_score"].round(1)
            transitions["price"] = transitions["price"].apply(
                lambda p: f"{p:,.0f} {CURRENCY}" if pd.notna(p) else "—"
            )
            st.dataframe(
                transitions.rename(columns={
                    "date": "Date", "verdict": "Verdict",
                    "hybrid_score": "Score", "price": "Prix",
                }),
                use_container_width=True, hide_index=True,
            )


def _render_profile(ticker: str, fundamentals: dict):
    """Onglet Profil v3 : en-tête entreprise + 2 colonnes éditoriales.
    Gauche (2/3) : Présentation + Historique financier table compacte.
    Droite (1/3) : Actionnariat + Dirigeants + Contact en cards."""
    profile = get_company_profile(ticker)

    if not profile:
        st.info("Profil non disponible. Lancez `scripts/scrape_profiles.py` pour charger les données.")
        return

    # ─── En-tête tab : nom entreprise (h2) + "Profil entreprise — TICKER"
    company = fundamentals.get("company_name") or ticker
    st.markdown(
        f"<div style='margin-top:6px;'>"
        f"<div style='font-size:22px;font-weight:600;color:var(--ink);"
        f"letter-spacing:-0.02em;'>{company}</div>"
        f"<div style='color:var(--ink-3);font-size:13px;margin-top:2px;"
        f"padding-bottom:12px;border-bottom:1px solid var(--border);'>"
        f"Profil entreprise · {ticker}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Helper : card key/value éditoriale
    def _info_card(label: str, items: list) -> str:
        rows_html = ""
        visible = [(k, v) for k, v in items if v]
        for i, (key, val) in enumerate(visible):
            sep = "border-bottom:1px solid var(--border);" if i < len(visible) - 1 else ""
            rows_html += (
                f"<div style='display:flex;justify-content:space-between;gap:12px;"
                f"padding:8px 0;{sep}'>"
                f"<span style='color:var(--ink-3);font-size:12.5px;'>{key}</span>"
                f"<span style='color:var(--ink);font-size:12.5px;font-weight:500;"
                f"text-align:right;font-variant-numeric:tabular-nums;'>{val}</span>"
                f"</div>"
            )
        if not rows_html:
            rows_html = ("<div style='color:var(--ink-3);font-size:12.5px;"
                         "padding:4px 0;'>Non disponible</div>")
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;margin-bottom:12px;'>"
            f"<div style='font-size:14px;font-weight:600;color:var(--ink);"
            f"margin-bottom:8px;'>{label}</div>"
            f"{rows_html}"
            f"</div>"
        )

    # ─── 2 colonnes : Présentation + Historique (2/3) · Actionnariat + Contact (1/3)
    col_left, col_right = st.columns([2, 1])

    with col_left:
        # Présentation
        if profile.get("description"):
            desc = profile["description"]
            if isinstance(desc, str):
                for prefix in ["La société :", "La société :", "La société:", "La société:"]:
                    if desc.startswith(prefix):
                        desc = desc[len(prefix):].strip()
                section_heading("Présentation", spacing="tight")
                st.markdown(
                    f"<div style='font-size:13px;line-height:1.55;color:var(--ink-2);"
                    f"margin-bottom:18px;'>{desc}</div>",
                    unsafe_allow_html=True,
                )

        # Historique financier — table compacte (5 dernières années)
        fund = read_sql_df(
            "SELECT fiscal_year, revenue, net_income, dps, eps, per FROM fundamentals "
            "WHERE ticker = ? ORDER BY fiscal_year DESC LIMIT 5",
            params=(ticker,),
        )
        if not fund.empty:
            section_heading("Historique financier", spacing="tight")

            def _money(v):
                if v is None or pd.isna(v) or not v:
                    return "—"
                av = abs(v)
                if av >= 1e9:
                    return f"{v/1e9:,.1f} Md"
                if av >= 1e6:
                    return f"{v/1e6:,.0f} M"
                return f"{v:,.0f}"

            def _int(v):
                if v is None or pd.isna(v) or not v:
                    return "—"
                return f"{v:,.0f}"

            def _dec(v):
                if v is None or pd.isna(v) or not v:
                    return "—"
                return f"{v:.1f}"

            header = (
                "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
                "color:var(--ink-3);font-weight:500;padding:8px 10px;"
                "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
                "text-align:right;"
            )
            cell = (
                "padding:9px 10px;font-size:13px;border-bottom:1px solid var(--border);"
                "text-align:right;font-variant-numeric:tabular-nums;"
            )
            rows_html = (
                f"<tr>"
                f"<th style='{header};text-align:left;'>Année</th>"
                f"<th style='{header}'>CA</th>"
                f"<th style='{header}'>Résultat net</th>"
                f"<th style='{header}'>DPS</th>"
                f"<th style='{header}'>BNPA</th>"
                f"<th style='{header}'>PER</th>"
                f"</tr>"
            )
            # Tri ascendant par année (plus récente en bas, comme dans la capture user)
            for _, row in fund.sort_values("fiscal_year", ascending=False).iterrows():
                rows_html += (
                    f"<tr>"
                    f"<td style='{cell};text-align:left;font-weight:500;'>{int(row['fiscal_year'])}</td>"
                    f"<td style='{cell}'>{_money(row['revenue'])}</td>"
                    f"<td style='{cell}'>{_money(row['net_income'])}</td>"
                    f"<td style='{cell}'>{_int(row['dps'])}</td>"
                    f"<td style='{cell}'>{_int(row['eps'])}</td>"
                    f"<td style='{cell}'>{_dec(row['per'])}</td>"
                    f"</tr>"
                )
            st.markdown(
                f"<div style='border:1px solid var(--border);border-radius:10px;"
                f"overflow:hidden;background:var(--bg-elev);'>"
                f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table>"
                f"</div>",
                unsafe_allow_html=True,
            )

    with col_right:
        # Actionnariat (Nb titres, Flottant, Secteur, Actionnaire)
        conn = get_connection()
        md = conn.execute(
            "SELECT shares, float_pct FROM market_data WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        conn.close()
        shares = md["shares"] if (md and md["shares"] and md["shares"] > 0) else None
        float_pct = md["float_pct"] if (md and md["float_pct"] and md["float_pct"] > 0) else None

        actionnariat_items = [
            ("Nombre de titres", f"{shares:,.0f}" if shares else None),
            ("Flottant", f"{float_pct:.1f}%" if float_pct else None),
            ("Secteur", fundamentals.get("sector") or None),
        ]
        # Actionnaire si dispo
        if profile.get("major_shareholder"):
            pct = profile.get("major_shareholder_pct")
            pct_str = f" · {pct:.1f}%" if pct else ""
            actionnariat_items.append(
                ("Actionnaire principal", f"{profile['major_shareholder']}{pct_str}")
            )
        st.markdown(_info_card("Actionnariat", actionnariat_items), unsafe_allow_html=True)

        # Dirigeants (compact)
        dirigeants_items = [
            ("PCA", profile.get("president")),
            ("Directeur Général", profile.get("dg")),
            ("DG Adjoint", profile.get("dga")),
        ]
        if any(v for _, v in dirigeants_items):
            st.markdown(_info_card("Dirigeants", dirigeants_items), unsafe_allow_html=True)

        # Contact — address multi-ligne, séparateur
        addr = profile.get("address") or ""
        phone = profile.get("phone") or ""
        fax = profile.get("fax") or ""
        if addr or phone or fax:
            # Pour l'adresse : on split sur les virgules pour un rendu multi-lignes
            addr_html = (
                "<br>".join(line.strip() for line in addr.split(",") if line.strip())
                if addr else ""
            )
            contact_body = ""
            if addr_html:
                contact_body += (
                    f"<div style='font-size:12.5px;color:var(--ink-2);line-height:1.55;"
                    f"padding:4px 0;'>{addr_html}</div>"
                )
            if phone:
                contact_body += (
                    f"<div style='font-size:12.5px;color:var(--ink-2);padding:4px 0;"
                    f"border-top:1px solid var(--border);margin-top:6px;'>"
                    f"<span style='color:var(--ink-3);'>Tél.</span> "
                    f"<span style='font-variant-numeric:tabular-nums;'>{phone}</span></div>"
                )
            if fax:
                contact_body += (
                    f"<div style='font-size:12.5px;color:var(--ink-2);padding:4px 0;'>"
                    f"<span style='color:var(--ink-3);'>Fax</span> "
                    f"<span style='font-variant-numeric:tabular-nums;'>{fax}</span></div>"
                )
            st.markdown(
                f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
                f"border-radius:10px;padding:14px 16px;margin-bottom:12px;'>"
                f"<div style='font-size:14px;font-weight:600;color:var(--ink);"
                f"margin-bottom:8px;'>Contact</div>"
                f"{contact_body}"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ─── Publications & actualités (pleine largeur) ───
    # Source = `publications` (richbourse) avec pub_date renseignée, plutôt
    # que `company_news` (sikafinance) où la date n'est pas extraite par le
    # scraper. On ajoute aussi un badge de statut d'intégration en base :
    #   ✅ intégré   ⏳ à intégrer   🆕 nouveau
    _render_publications_with_status(ticker)

    # --- Notes d'analyse (tag catégorie via design kit au lieu d'emojis) ---
    section_heading("Notes d'analyse", spacing="loose")
    notes = get_qualitative_notes(ticker)
    if not notes.empty:
        for _, note in notes.iterrows():
            cat = note.get("category", "general")
            cat_label = cat.capitalize()
            # Mapping catégorie → tone design : strategie/perspectives/dividendes=up,
            # concurrence/risques=warn, gouvernance=terra, general=neutral
            tone_map = {
                "strategie": "up", "perspectives": "up", "dividendes": "up",
                "concurrence": "ocre", "risques": "down",
                "gouvernance": "terra", "general": "neutral",
            }
            tone = tone_map.get(cat, "neutral")
            col_cat, col_content, col_del = st.columns([1, 5, 0.5])
            col_cat.markdown(_tag_html(cat_label, tone), unsafe_allow_html=True)
            col_content.write(note["content"])
            if note.get("source"):
                col_content.caption(f"Source · {note['source']} · {note.get('note_date', '')}")
            if col_del.button("Supprimer", key=f"del_note_p2_{note['id']}"):
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
            if st.form_submit_button("Enregistrer", type="primary"):
                if content.strip():
                    save_qualitative_note(ticker, category, content.strip(), source, str(note_date))
                    st.success("Note enregistrée")
                    st.rerun()
