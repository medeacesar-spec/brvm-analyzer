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
    bd = ratios.get("fundamental_breakdown") or {}
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

    # Tendance (densité v3, pas de divider)
    trend = result.get("trend", {})
    trend_name = trend.get("trend", "N/A")
    trend_tone = {"haussiere": "up", "baissiere": "down"}.get(trend_name, "neutral")
    section_heading("Tendance actuelle")
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        st.markdown(
            _tag_html(trend_name.capitalize(), trend_tone),
            unsafe_allow_html=True,
        )
    col_t2.markdown(
        f"<div style='font-size:13px;'><span class='muted'>Force</span> "
        f"<b>{trend.get('strength', 'N/A').capitalize()}</b></div>",
        unsafe_allow_html=True,
    )
    col_t3.markdown(
        f"<div style='font-size:13px;'><span class='muted'>Détail</span> "
        f"{trend.get('details', '—')}</div>",
        unsafe_allow_html=True,
    )

    # Supports / Résistances — dots colorés, pas d'emojis
    supports = result.get("supports", [])
    resistances = result.get("resistances", [])
    col_sr1, col_sr2 = st.columns(2)
    with col_sr1:
        st.markdown(
            '<div style="font-size:15px;font-weight:600;color:var(--ink);'
            'letter-spacing:-0.01em;margin:14px 0 8px 0;">'
            '<span class="dot up"></span>Supports</div>',
            unsafe_allow_html=True,
        )
        for i, s in enumerate(supports[:3]):
            st.markdown(
                f"<div>Zone {i+1} <span class='muted'>·</span> "
                f"<b style='font-variant-numeric:tabular-nums'>{s:,.0f} FCFA</b></div>",
                unsafe_allow_html=True,
            )
        if not supports:
            st.caption("Aucun support détecté")
    with col_sr2:
        st.markdown(
            '<div style="font-size:15px;font-weight:600;color:var(--ink);'
            'letter-spacing:-0.01em;margin:14px 0 8px 0;">'
            '<span class="dot down"></span>Résistances</div>',
            unsafe_allow_html=True,
        )
        for i, r in enumerate(resistances[:3]):
            st.markdown(
                f"<div>Zone {i+1} <span class='muted'>·</span> "
                f"<b style='font-variant-numeric:tabular-nums'>{r:,.0f} FCFA</b></div>",
                unsafe_allow_html=True,
            )
        if not resistances:
            st.caption("Aucune résistance détectée")

    # Signaux techniques — dots + stars au lieu d'emojis
    section_heading("Signaux techniques", spacing="loose")
    signals = result.get("signals", [])
    if signals:
        for sig in signals:
            tone = {"achat": "up", "vente": "down", "info": "neutral"}.get(sig["type"], "neutral")
            strength = "★" * sig["strength"] + "★" * (5 - sig["strength"])  # placeholder
            stars_html = (
                f"<span class='stars'>{'★' * sig['strength']}"
                f"<span class='off'>{'★' * (5 - sig['strength'])}</span></span>"
            )
            st.markdown(
                f"<div style='padding:4px 0;'>"
                f"<span class='dot {tone}'></span>"
                f"<b>{sig['signal']}</b> {stars_html} "
                f"<span class='muted'>· {sig['details']}</span>"
                f"</div>",
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
    """Onglet recommandation — 3 scores en metrics + synthèse.
    Le verdict+stars est affiché dans le header de la page, pas besoin
    de le répéter ici."""
    reco = result["recommendation"]

    fund_s = result.get("fundamental_score") or 0
    tech_s = result.get("technical_score") or 0
    hybrid = result.get("hybrid_score") or 0

    # 3 scores en metrics
    col_g1, col_g2, col_g3 = st.columns(3)
    col_g1.metric("Score fondamental", f"{fund_s:.0f} / 50")
    col_g2.metric("Score technique", f"{tech_s:.0f} / 50")
    col_g3.metric("Score global", f"{hybrid:.0f} / 100")

    # Points forts / vigilance — section_heading bold avec dot colorée
    _dot_section_style = (
        "font-size:15px;font-weight:600;color:var(--ink);"
        "letter-spacing:-0.01em;margin:20px 0 10px 0;"
    )
    col_s, col_w = st.columns(2)
    with col_s:
        st.markdown(
            f'<div style="{_dot_section_style}">'
            '<span class="dot up"></span>Points forts</div>',
            unsafe_allow_html=True,
        )
        for s in reco.get("strengths", []):
            st.markdown(
                f"<div style='padding:3px 0;font-size:13px;'>"
                f"<span class='dot up'></span>{s}</div>",
                unsafe_allow_html=True,
            )
        if not reco.get("strengths"):
            st.caption("Aucun point fort identifié")
    with col_w:
        st.markdown(
            f'<div style="{_dot_section_style}">'
            '<span class="dot warn"></span>Points de vigilance</div>',
            unsafe_allow_html=True,
        )
        for w in reco.get("warnings", []):
            st.markdown(
                f"<div style='padding:3px 0;font-size:13px;'>"
                f"<span class='dot warn'></span>{w}</div>",
                unsafe_allow_html=True,
            )
        if not reco.get("warnings"):
            st.caption("Aucun point de vigilance")

    # Zones d'entrée — même style que points forts/vigilance (dots + 13px)
    section_heading("Zones d'entrée suggérées", spacing="loose")
    entry_zones = reco.get("entry_zones", [])
    if entry_zones:
        for zone in entry_zones:
            st.markdown(
                f"<div style='padding:3px 0;font-size:13px;'>"
                f"<span class='dot up'></span>"
                f"<b>{zone['label']}</b> "
                f"<span class='muted'>·</span> "
                f"<span style='font-variant-numeric:tabular-nums'>{zone['zone']}</span> "
                f"<span class='muted'>·</span> "
                f"Risque/Rendement : {zone['risk_reward']}"
                f"</div>",
                unsafe_allow_html=True,
            )
    else:
        st.caption("Pas assez de données pour déterminer les zones d'entrée")


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


def _render_profile(ticker: str, fundamentals: dict):
    """Onglet profil qualitatif de l'entreprise."""
    profile = get_company_profile(ticker)

    if not profile:
        st.info("Profil non disponible. Lancez `scripts/scrape_profiles.py` pour charger les données.")
        return

    # --- Présentation ---
    if profile.get("description"):
        desc = profile["description"]
        if isinstance(desc, str):
            for prefix in ["La société :", "La société :", "La société:", "La société:"]:
                if desc.startswith(prefix):
                    desc = desc[len(prefix):].strip()
            section_heading("Présentation", spacing="tight")
            st.markdown(desc)

    # Helper : rend une carte éditoriale (label-xs uppercase + contenu structuré)
    def _info_card(label: str, items: list) -> str:
        """items = [(key_label, value)] — rendus en lignes key/value serrées."""
        rows_html = ""
        for key, val in items:
            if not val:
                continue
            rows_html += (
                f"<div style='display:flex;justify-content:space-between;gap:12px;"
                f"padding:6px 0;border-bottom:1px solid var(--border);'>"
                f"<span style='color:var(--ink-3);font-size:12.5px;'>{key}</span>"
                f"<span style='color:var(--ink);font-size:12.5px;font-weight:500;"
                f"text-align:right;'>{val}</span>"
                f"</div>"
            )
        if not rows_html:
            rows_html = ("<div style='color:var(--ink-3);font-size:12.5px;"
                         "padding:8px 0;'>Non disponible</div>")
        # Dernière row sans border-bottom
        rows_html = rows_html.rsplit(
            "border-bottom:1px solid var(--border);", 1
        )[0] + "".join(rows_html.rsplit("border-bottom:1px solid var(--border);", 1)[1:])
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:12px 16px;'>"
            f"<div style='font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
            f"color:var(--ink-3);font-weight:500;margin-bottom:8px;'>{label}</div>"
            f"{rows_html}"
            f"</div>"
        )

    # --- Dirigeants & Actionnariat (2 cards bordées) ---
    conn = get_connection()
    md = conn.execute(
        "SELECT shares, float_pct, market_cap FROM market_data WHERE ticker = ?",
        (ticker,)
    ).fetchone()
    conn.close()

    col1, col2 = st.columns(2)
    with col1:
        dirigeants_items = [
            ("Président du Conseil", profile.get("president")),
            ("Directeur Général", profile.get("dg")),
            ("DG Adjoint", profile.get("dga")),
        ]
        st.markdown(_info_card("Dirigeants", dirigeants_items), unsafe_allow_html=True)

    with col2:
        pct = profile.get("major_shareholder_pct")
        pct_str = f" · {pct:.1f}%" if pct else ""
        shareholder = (
            f"{profile['major_shareholder']}{pct_str}"
            if profile.get("major_shareholder") else None
        )
        actionnariat_items = [
            ("Actionnaire principal", shareholder),
            ("Nombre de titres",
             f"{md['shares']:,.0f}" if (md and md["shares"] and md["shares"] > 0) else None),
            ("Flottant",
             f"{md['float_pct']:.1f}%" if (md and md["float_pct"] and md["float_pct"] > 0) else None),
            ("Capitalisation",
             f"{md['market_cap']/1e3:,.1f} Md FCFA" if (md and md["market_cap"] and md["market_cap"] > 0) else None),
        ]
        st.markdown(_info_card("Actionnariat & Marché", actionnariat_items), unsafe_allow_html=True)

    # --- Contact (card bordée si au moins une info) ---
    contact_items = [
        ("Adresse", profile.get("address")),
        ("Téléphone", profile.get("phone")),
        ("Fax", profile.get("fax")),
    ]
    if any(v for _, v in contact_items):
        st.markdown(
            f"<div style='margin-top:12px;'>"
            + _info_card("Contact", contact_items)
            + "</div>",
            unsafe_allow_html=True,
        )

    # --- Financial history ---
    conn = get_connection()
    fund = read_sql_df("""SELECT fiscal_year, revenue, net_income, dps, eps, per
           FROM fundamentals WHERE ticker = ?
           ORDER BY fiscal_year DESC LIMIT 5""", params=(ticker,),
    )
    conn.close()

    if not fund.empty:
        section_heading("Historique financier", spacing="loose")
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

    # --- Actualités récentes (densité v3, pas de divider) ---
    news = get_company_news(ticker, limit=8)
    if not news.empty:
        section_heading("Actualités récentes", spacing="loose")
        for _, art in news.iterrows():
            date_str = f" <span class='muted'>({art['article_date']})</span>" if art.get("article_date") else ""
            url = art.get("url", "")
            if url and url.startswith("http"):
                st.markdown(f"- [{art['title']}]({url}){date_str}", unsafe_allow_html=True)
            else:
                st.markdown(f"- {art['title']}{date_str}", unsafe_allow_html=True)

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
