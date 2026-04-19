"""
Page 1 : Dashboard Marché BRVM
Lit uniquement depuis la base SQLite locale (pre-chargee par app.py au demarrage).
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from data.storage import (
    get_connection, get_cached_prices,
    get_pending_publications, get_data_gaps,
    ignore_publication, delete_publication, mark_publication_integrated,
    ignore_gap,
)
from data.db import read_sql_df
from utils.nav import ticker_analyze_button, ticker_quick_picker
from utils.auth import is_admin


def _load_quotes_from_db() -> pd.DataFrame:
    conn = get_connection()
    df = read_sql_df("""SELECT ticker, company_name as name, sector, price as last,
           variation, market_cap, beta, rsi, dps, updated_at
           FROM market_data WHERE price > 0 ORDER BY ticker""")

    # Compute variation from price_cache if market_data.variation is 0
    if not df.empty and (df["variation"].fillna(0).abs().sum() < 0.01):
        # Get last 2 trading days from price_cache
        dates = read_sql_df("SELECT DISTINCT date FROM price_cache ORDER BY date DESC LIMIT 2")
        if len(dates) >= 2:
            last_date = dates.iloc[0]["date"]
            prev_date = dates.iloc[1]["date"]
            prices = read_sql_df("""SELECT p1.ticker, p1.close as last_close, p2.close as prev_close
                   FROM price_cache p1
                   JOIN price_cache p2 ON p1.ticker = p2.ticker AND p2.date = ?
                   WHERE p1.date = ?""", params=(prev_date, last_date),
            )
            if not prices.empty:
                prices["var_pct"] = ((prices["last_close"] - prices["prev_close"]) / prices["prev_close"] * 100).round(2)
                var_map = dict(zip(prices["ticker"], prices["var_pct"]))
                df["variation"] = df["ticker"].map(var_map).fillna(0)
                df["_last_trading_date"] = last_date
            else:
                df["_last_trading_date"] = None
        else:
            df["_last_trading_date"] = None

    # Compute market_cap from price * shares (from fundamentals) if market_cap is 0
    if not df.empty and (df["market_cap"].fillna(0).abs().sum() < 1):
        shares_df = read_sql_df("""SELECT f.ticker, f.shares FROM fundamentals f
               INNER JOIN (
                   SELECT ticker, MAX(fiscal_year) as max_year
                   FROM fundamentals
                   WHERE shares IS NOT NULL AND shares > 0
                   GROUP BY ticker
               ) latest ON f.ticker = latest.ticker AND f.fiscal_year = latest.max_year
               WHERE f.shares IS NOT NULL AND f.shares > 0""")
        if not shares_df.empty:
            shares_map = dict(zip(shares_df["ticker"], shares_df["shares"]))
            df["market_cap"] = df.apply(
                lambda r: r["last"] * shares_map.get(r["ticker"], 0) / 1e6, axis=1  # En millions
            )

    conn.close()
    return df


def _load_indices_from_db() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = read_sql_df("SELECT name, value, variation, prev_close, ytd_variation, category FROM indices_cache")
    except Exception:
        try:
            df = read_sql_df("SELECT name, value, variation FROM indices_cache")
        except Exception:
            df = pd.DataFrame()
    conn.close()
    return df


def _compute_period_performance(quotes: pd.DataFrame) -> dict:
    """Calcule les variations jour/semaine/mois pour tous les tickers.

    Retourne également `ranges` : pour chaque période, les dates de début
    et de fin utilisées pour le calcul (affichées à l'utilisateur pour
    lever toute ambiguïté sur la fenêtre glissante).
    """
    from data.storage import get_all_cached_prices
    results = {"day": [], "week": [], "month": [], "ranges": {}}
    today = datetime.now()
    days_since_monday = today.weekday()
    last_friday = today - timedelta(days=days_since_monday + 3)
    last_monday = last_friday - timedelta(days=4)
    month_ago = today - timedelta(days=30)

    results["ranges"] = {
        "day": today.date(),
        "week_start": last_monday.date(),
        "week_end": last_friday.date(),
        "month_start": month_ago.date(),
        "month_end": today.date(),
    }

    # 1 seule requête pour tous les tickers (mise en cache 5 min)
    all_prices = get_all_cached_prices()

    for _, row in quotes.iterrows():
        ticker, name, last_price = row.get("ticker", ""), row.get("name", ""), row.get("last", 0)
        if not ticker or not last_price:
            continue
        day_var = row.get("variation", 0) or 0
        results["day"].append({"ticker": ticker, "name": name, "price": last_price, "variation": day_var})

        prices = all_prices.get(ticker, pd.DataFrame())
        if prices.empty or len(prices) < 5:
            results["week"].append({"ticker": ticker, "name": name, "price": last_price, "variation": 0})
            results["month"].append({"ticker": ticker, "name": name, "price": last_price, "variation": 0})
            continue

        prices = prices.sort_values("date")
        for period_key, start_dt, end_dt in [("week", last_monday, last_friday), ("month", month_ago, today)]:
            pdata = prices[(prices["date"] >= pd.Timestamp(start_dt)) & (prices["date"] <= pd.Timestamp(end_dt))]
            if len(pdata) >= 2:
                var = ((pdata.iloc[-1]["close"] - pdata.iloc[0]["close"]) / pdata.iloc[0]["close"] * 100) if pdata.iloc[0]["close"] > 0 else 0
            else:
                var = 0
            results[period_key].append({"ticker": ticker, "name": name, "price": last_price, "variation": var})

    # Sépare "ranges" (dict de dates, pas un DataFrame) du reste
    ranges = results.pop("ranges", {})
    out = {k: pd.DataFrame(v) for k, v in results.items()}
    out["ranges"] = ranges
    return out


def _render_top5(df: pd.DataFrame, label: str):
    """Affiche les 5 plus fortes hausses + 5 plus fortes baisses de la période.
    Le `label` est inséré dans le titre de chaque colonne pour lever toute
    ambiguïté sur la fenêtre temporelle comparée."""
    from utils.ui_helpers import delta, ticker as ticker_chip
    if df.empty or "variation" not in df.columns:
        return

    positive = df[df["variation"] > 0.01].nlargest(5, "variation")
    negative = df[df["variation"] < -0.01].nsmallest(5, "variation")

    col_up, col_dn = st.columns(2)

    def _render_list(rows: pd.DataFrame, heading: str, empty_msg: str):
        st.markdown(
            f'<div class="label-xs">{heading}</div>'
            f'<div style="font-size:11px;color:var(--ink-3);margin-bottom:8px;">'
            f'Top {min(5, max(1, len(rows)))} plus fortes variations</div>',
            unsafe_allow_html=True,
        )
        if rows.empty:
            st.caption(empty_msg)
            return
        for _, r in rows.iterrows():
            a, b, c, d = st.columns([3, 1, 1, 0.5])
            a.markdown(
                f"<div style='padding:6px 0;'><b>{r['name']}</b> "
                f"{ticker_chip(r['ticker'])}</div>",
                unsafe_allow_html=True,
            )
            b.markdown(
                f"<div style='text-align:right;font-variant-numeric:tabular-nums;padding:6px 0;'>"
                f"{r['price']:,.0f}</div>",
                unsafe_allow_html=True,
            )
            c.markdown(
                f"<div style='text-align:right;padding:6px 0;'>{delta(r['variation'])}</div>",
                unsafe_allow_html=True,
            )
            with d:
                ticker_analyze_button(
                    r["ticker"], label="🔍",
                    key=f"dash_{heading[:2]}_{label}_{r['ticker']}",
                )

    with col_up:
        _render_list(positive, f"↗ HAUSSES {label.upper()}", "Aucune hausse")
    with col_dn:
        _render_list(negative, f"↘ BAISSES {label.upper()}", "Aucune baisse")


@st.cache_data(ttl=300, show_spinner=False)
def _cached_pending_publications():
    try:
        return get_pending_publications()
    except Exception:
        return None


@st.cache_data(ttl=300, show_spinner=False)
def _cached_data_gaps():
    try:
        return get_data_gaps()
    except Exception:
        return None


def _render_pending_publications_alert():
    """Affiche une bannière si des publications récentes (états financiers annuels ou
    données trimestrielles) n'ont pas encore été intégrées en base.

    Résultats cachés 5 min (les deux requêtes font ensemble 240+ round-trips
    Supabase dans la version non optimisée).
    """
    pending = _cached_pending_publications()
    gaps = _cached_data_gaps()

    alerts = []
    if pending is not None and not pending.empty:
        n_new = int((pending["pending_reason"] == "nouveau").sum())
        n_annuel = int((pending["pending_reason"] == "annuel_non_integre").sum())
        n_trim = int((pending["pending_reason"] == "trimestriel_a_verifier").sum())
        if n_annuel:
            alerts.append(f"**{n_annuel}** rapport(s) annuel(s) scrapé(s) non intégré(s)")
        if n_trim:
            alerts.append(f"**{n_trim}** publication(s) trimestrielle(s) scrapée(s)")
        if n_new:
            alerts.append(f"**{n_new}** nouvelle(s) publication(s) récente(s)")

    if gaps is not None and not gaps.empty:
        n_missing_annual = int(gaps["missing_annual"].sum())
        n_missing_quarter = int((~gaps["missing_quarter"].isna()).sum()) if "missing_quarter" in gaps.columns else 0
        if n_missing_annual:
            alerts.append(f"**{n_missing_annual}** titre(s) sans comptes annuels à jour")
        if n_missing_quarter:
            alerts.append(f"**{n_missing_quarter}** titre(s) sans données trimestrielles récentes")

    if not alerts:
        return

    st.warning(
        "📢 **Données potentiellement manquantes (7 derniers jours)** : "
        + " · ".join(alerts)
    )

    admin = is_admin()
    detail_label = "Voir le détail et agir" if admin else "Voir le détail"
    with st.expander(detail_label, expanded=False):
        if not admin:
            st.caption(
                "🔒 Les actions (télécharger, ignorer, intégrer) sont réservées à l'administrateur."
            )

        # ─── Helpers de rendu (utilisés dans les 2 blocs pending + gaps) ───
        CELL_STYLE = (
            "font-size:0.82rem;line-height:1.2;white-space:nowrap;"
            "overflow:hidden;text-overflow:ellipsis;"
            "display:flex;align-items:center;min-height:32px;"
        )
        HEAD_STYLE = CELL_STYLE + "font-weight:700;"

        def _cell(txt, title=None):
            tip = f' title="{title}"' if title else ''
            return f'<div style="{CELL_STYLE}"{tip}>{txt}</div>'

        def _head(txt):
            return f'<div style="{HEAD_STYLE}">{txt}</div>'

        # Dictionnaire pour restaurer les accents français
        _ACCENT_REPLACEMENTS = [
            ("dactivites", "d'activités"),
            ("dactivite", "d'activité"),
            ("dinformation", "d'information"),
            ("dexercice", "d'exercice"),
            ("cote divoire", "Côte d'Ivoire"),
            ("Etats financiers", "États financiers"),
            ("etats financiers", "états financiers"),
            ("Etats ", "États "),
            ("Rapport dactivites", "Rapport d'activités"),
            ("Assemblee generale", "Assemblée générale"),
            ("assemblee generale", "assemblée générale"),
            ("Societe generale", "Société Générale"),
            ("societe generale", "société générale"),
            ("annule et remplace le precedent", "annulé et remplacé le précédent"),
            ("resultats", "résultats"),
            ("1er trimestre", "1er trimestre"),
            ("2eme trimestre", "2ème trimestre"),
            ("3eme trimestre", "3ème trimestre"),
            ("1er semestre", "1er semestre"),
            ("Extraordinaire", "Extraordinaire"),
            ("Societe ", "Société "),
            ("benin", "Bénin"),
            ("Benin", "Bénin"),
            ("Senegal", "Sénégal"),
            ("senegal", "Sénégal"),
            ("evoir", "évoir"),
            ("general ", "général "),
            ("generale ", "générale "),
        ]

        def _fr(text: str) -> str:
            if not text:
                return ""
            out = text
            for old, new in _ACCENT_REPLACEMENTS:
                out = out.replace(old, new)
            return out[:1].upper() + out[1:] if out else out

        # Scraped pending publications
        if pending is not None and not pending.empty:
            st.markdown("#### 📥 Publications des 7 derniers jours à intégrer")
            if admin:
                st.caption(
                    "**🚀 Chercher** télécharge l'état financier (PDF) et rafraîchit sika. "
                    "**🚫 Ignorer** retire la ligne."
                )

            _TYPE_LABELS = {
                "annuel": "Annuel",
                "trimestriel": "Trimestriel",
                "semestriel": "Semestriel",
                "dividende": "Dividende",
                "gouvernance": "Gouvernance",
                "corporate": "Opération capital",
                "autre": "Autre",
            }

            # Header — colonnes larges pour le titre, étroites pour les métadonnées
            if admin:
                col_widths = [0.7, 1.3, 5.0, 0.6, 1.0, 0.9, 0.6, 0.6]
                h = st.columns(col_widths, vertical_alignment="center")
                h[6].markdown(_head("🚀"), unsafe_allow_html=True)
                h[7].markdown(_head("🚫"), unsafe_allow_html=True)
            else:
                col_widths = [0.7, 1.3, 5.0, 0.6, 1.0, 0.9]
                h = st.columns(col_widths, vertical_alignment="center")
            h[0].markdown(_head("Ticker"), unsafe_allow_html=True)
            h[1].markdown(_head("Type"), unsafe_allow_html=True)
            h[2].markdown(_head("Titre"), unsafe_allow_html=True)
            h[3].markdown(_head("Ex."), unsafe_allow_html=True)
            h[4].markdown(_head("Date"), unsafe_allow_html=True)
            h[5].markdown(_head("URL"), unsafe_allow_html=True)

            reason_emoji = {
                "nouveau": "🆕 Nouveau",
                "annuel_non_integre": "📄 Annuel à intégrer",
                "trimestriel_a_verifier": "📊 Trimestriel",
            }

            from scripts.fetch_publication import _is_financial_statement

            # Reason badge compact (pas de retour ligne)
            reason_short = {
                "nouveau": "🆕",
                "annuel_non_integre": "📄",
                "trimestriel_a_verifier": "📊",
            }

            for _, row in pending.head(15).iterrows():
                rid = int(row["id"])
                is_financial = _is_financial_statement(row.get("title"), row.get("pub_type"))

                if admin:
                    c = st.columns([0.7, 1.3, 5.0, 0.6, 1.0, 0.9, 0.6, 0.6],
                                   vertical_alignment="center")
                else:
                    c = st.columns([0.7, 1.3, 5.0, 0.6, 1.0, 0.9],
                                   vertical_alignment="center")

                r_emoji = reason_short.get(row["pending_reason"], "")
                pub_type_raw = row.get("pub_type") or ""
                pub_type_label = _TYPE_LABELS.get(pub_type_raw, pub_type_raw.capitalize())

                c[0].markdown(
                    _cell(row["ticker"] or "—", title=row["ticker"] or ""),
                    unsafe_allow_html=True,
                )
                c[1].markdown(
                    _cell(f"{r_emoji} {pub_type_label}", title=row["pending_reason"] or ""),
                    unsafe_allow_html=True,
                )
                title_fr = _fr(row["title"] or "")
                c[2].markdown(_cell(title_fr, title=title_fr), unsafe_allow_html=True)
                year_txt = str(int(row["fiscal_year"])) if pd.notna(row.get("fiscal_year")) else "—"
                c[3].markdown(_cell(year_txt), unsafe_allow_html=True)
                pub_date = row.get("pub_date") or ""
                short_date = pub_date[5:] if len(pub_date) >= 10 else pub_date
                c[4].markdown(
                    _cell(short_date or "—", title=pub_date),
                    unsafe_allow_html=True,
                )
                if row.get("url"):
                    c[5].markdown(
                        f'<div style="{CELL_STYLE}"><a href="{row["url"]}" target="_blank">📄</a></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    c[5].markdown(_cell("—"), unsafe_allow_html=True)

                if admin:
                    with c[6]:
                        if is_financial:
                            if st.button("🚀", key=f"fetch_pub_{rid}", help="Télécharger l'état financier + rafraîchir sika"):
                                with st.spinner(f"Récupération {row['ticker']}…"):
                                    from scripts.fetch_publication import auto_fetch_publication
                                    res = auto_fetch_publication(rid)
                                if res.get("success"):
                                    parts = []
                                    s = res.get("sika", {})
                                    if s.get("inserted") or s.get("updated"):
                                        parts.append(f"Sika: +{s.get('inserted',0)} / ~{s.get('updated',0)}")
                                    if res.get("pdf_path"):
                                        parts.append(f"PDF → {os.path.basename(res['pdf_path'])}")
                                    st.success("✅ " + " · ".join(parts))
                                else:
                                    err = res.get("sika", {}).get("error") or res.get("pdf_error") or res.get("sika_error") or "Aucune donnée récupérée"
                                    st.warning(f"⚠️ {err}")
                                st.rerun()
                        else:
                            c[6].caption("ℹ️")
                    with c[7]:
                        if st.button("🚫", key=f"ignore_pub_{rid}", help="Ignorer"):
                            ignore_publication(rid)
                            st.rerun()

        # Gaps detected — ignore action admin only
        if gaps is not None and not gaps.empty:
            st.markdown("<div style='margin-top:14px'></div>", unsafe_allow_html=True)
            st.markdown("#### Écarts détectés par rapport au cycle de publication")
            st.caption(
                "Cycle UEMOA : rapport annuel au plus tard fin avril, trimestriels dans "
                "les 45j suivant la fin de trimestre."
                + (" Bouton **Ignorer** si le titre ne publie pas." if admin else "")
            )

            if admin:
                h = st.columns([0.7, 2.5, 1.5, 0.7, 2.5, 0.6, 0.6],
                               vertical_alignment="center")
                h[5].markdown(_head("🚫 An."), unsafe_allow_html=True)
                h[6].markdown(_head("🚫 Tr."), unsafe_allow_html=True)
            else:
                h = st.columns([0.7, 2.5, 1.5, 0.7, 2.5], vertical_alignment="center")
            h[0].markdown(_head("Ticker"), unsafe_allow_html=True)
            h[1].markdown(_head("Nom"), unsafe_allow_html=True)
            h[2].markdown(_head("Secteur"), unsafe_allow_html=True)
            h[3].markdown(_head("Dern."), unsafe_allow_html=True)
            h[4].markdown(_head("Manquant"), unsafe_allow_html=True)

            for _, row in gaps.iterrows():
                ticker = row["ticker"]
                if admin:
                    c = st.columns([0.7, 2.5, 1.5, 0.7, 2.5, 0.6, 0.6],
                                   vertical_alignment="center")
                else:
                    c = st.columns([0.7, 2.5, 1.5, 0.7, 2.5],
                                   vertical_alignment="center")
                c[0].markdown(_cell(ticker, title=ticker), unsafe_allow_html=True)
                name = _fr(row["name"] or "")
                c[1].markdown(_cell(name, title=name), unsafe_allow_html=True)
                sector = _fr(row["sector"] or "")
                c[2].markdown(_cell(sector, title=sector), unsafe_allow_html=True)
                year_txt = (
                    str(int(row["latest_year_in_db"]))
                    if pd.notna(row.get("latest_year_in_db"))
                    else "—"
                )
                c[3].markdown(_cell(year_txt), unsafe_allow_html=True)
                manquant = ", ".join(filter(None, [
                    f"📄 {int(row['expected_latest'])}" if row["missing_annual"] else None,
                    f"📊 {row['missing_quarter']}" if pd.notna(row.get("missing_quarter")) else None,
                ]))
                c[4].markdown(_cell(manquant, title=manquant), unsafe_allow_html=True)

                if admin:
                    with c[5]:
                        if row["missing_annual"]:
                            if st.button("🚫", key=f"ignore_gap_ann_{ticker}",
                                         help="Ignorer l'écart annuel"):
                                ignore_gap(ticker, "annuel", int(row["expected_latest"]),
                                           reason="Marqué non-applicable par l'utilisateur")
                                st.rerun()
                    with c[6]:
                        if pd.notna(row.get("missing_quarter")):
                            if st.button("🚫", key=f"ignore_gap_q_{ticker}",
                                         help="Ignorer les trimestriels"):
                                ignore_gap(ticker, "trimestriel",
                                           reason="Titre ne publie pas de trimestriels")
                                st.rerun()

        if admin:
            st.caption(
                "**Chercher** télécharge le PDF dans `pdfs/{ticker}/` et rafraîchit les "
                "données de marché. Pour extraire les données détaillées, lancer "
                "`python3 scripts/extract_pdfs.py`."
            )


def render():
    # Hiérarchie v3 : Title + caption → KPI row → Tabs → contenu (pas de divider)
    quotes = _load_quotes_from_db()
    if quotes.empty:
        st.title("Marché BRVM")
        st.caption("Données en cours de chargement — patientez quelques secondes puis rafraîchissez.")
        return

    JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
               "juillet", "aout", "septembre", "octobre", "novembre", "décembre"]

    last_trading_date = (
        quotes.get("_last_trading_date", pd.Series()).iloc[0]
        if "_last_trading_date" in quotes.columns and len(quotes) > 0 else None
    )
    date_caption = "Bourse régionale des valeurs mobilières · 48 titres suivis"
    if last_trading_date:
        try:
            dt = pd.to_datetime(last_trading_date)
            date_caption = (
                f"Dernier jour coté · {JOURS_FR[dt.weekday()]} "
                f"{dt.day} {MOIS_FR[dt.month-1]} {dt.year}"
            )
        except Exception:
            pass

    st.title("Marché BRVM")
    st.caption(date_caption)

    # Alerte publications non intégrées (conditionnelle, ne s'affiche que si besoin)
    _render_pending_publications_alert()

    # KPI row — densité v3 (st.metric compact via CSS)
    positive = quotes[quotes["variation"] > 0.01] if "variation" in quotes.columns else pd.DataFrame()
    negative = quotes[quotes["variation"] < -0.01] if "variation" in quotes.columns else pd.DataFrame()
    total_mcap = quotes["market_cap"].sum() if "market_cap" in quotes.columns else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Hausses", f"{len(positive)}")
    col2.metric("Baisses", f"{len(negative)}")
    col3.metric("Stables", f"{len(quotes) - len(positive) - len(negative)}")
    col4.metric(
        "Capitalisation",
        f"{total_mcap/1e3:,.0f} Mds" if total_mcap > 0 else "—",
    )

    # Tabs Jour / Semaine / Mois — labels avec date explicite dans chaque caption
    perf = _compute_period_performance(quotes)
    ranges = perf.get("ranges", {})

    def _fmt_date(d):
        try:
            return f"{d.day} {MOIS_FR[d.month-1][:4]}"
        except Exception:
            return "—"

    day_label = "Jour"
    day_caption = ""
    if last_trading_date:
        try:
            dt = pd.to_datetime(last_trading_date)
            day_label = f"Jour · {dt.day}/{dt.month:02d}"
            day_caption = f"Clôture du {JOURS_FR[dt.weekday()]} {dt.day} {MOIS_FR[dt.month-1]}"
        except Exception:
            pass

    week_caption = ""
    if ranges.get("week_start") and ranges.get("week_end"):
        week_caption = (
            f"Semaine du {_fmt_date(ranges['week_start'])} "
            f"au {_fmt_date(ranges['week_end'])}"
        )
    month_caption = ""
    if ranges.get("month_start") and ranges.get("month_end"):
        month_caption = (
            f"30 derniers jours · "
            f"{_fmt_date(ranges['month_start'])} → {_fmt_date(ranges['month_end'])}"
        )

    tab_day, tab_week, tab_month = st.tabs([day_label, "Semaine", "Mois"])
    with tab_day:
        if day_caption:
            st.caption(day_caption)
        _render_top5(perf.get("day", pd.DataFrame()), "du jour")
    with tab_week:
        if week_caption:
            st.caption(week_caption)
        week_df = perf.get("week", pd.DataFrame())
        if not week_df.empty and week_df["variation"].abs().sum() > 0:
            _render_top5(week_df, "de la semaine")
        else:
            st.info("Prix historiques en cours de chargement...")
    with tab_month:
        if month_caption:
            st.caption(month_caption)
        month_df = perf.get("month", pd.DataFrame())
        if not month_df.empty and month_df["variation"].abs().sum() > 0:
            _render_top5(month_df, "du mois")
        else:
            st.info("Prix historiques en cours de chargement...")

    # --- Tableau complet (repliable, pas de divider — densité v3) ---
    with st.expander(f"Toutes les cotations · {len(quotes)} titres", expanded=False):
        sectors = ["Tous"] + sorted(quotes["sector"].dropna().unique().tolist())
        selected_sector = st.selectbox("Filtrer par secteur", sectors)
        display_df = quotes[quotes["sector"] == selected_sector] if selected_sector != "Tous" else quotes

        # Format display columns — keep numeric for sorting
        fmt_df = display_df.copy()
        fmt_df["market_cap"] = fmt_df["market_cap"].apply(
            lambda x: round(x / 1e3, 1) if pd.notna(x) and x > 0 else None
        )
        fmt_df["variation"] = fmt_df["variation"].apply(
            lambda x: round(x, 2) if pd.notna(x) else 0.0
        )
        fmt_df["beta"] = fmt_df["beta"].apply(lambda x: round(x, 2) if pd.notna(x) and abs(x) > 0.001 else None)
        fmt_df["rsi"] = fmt_df["rsi"].apply(lambda x: round(x, 0) if pd.notna(x) and abs(x) > 0.001 else None)
        fmt_df["dps"] = fmt_df["dps"].apply(lambda x: round(x, 0) if pd.notna(x) and x > 0 else None)
        fmt_df["last"] = fmt_df["last"].apply(lambda x: round(x, 0) if pd.notna(x) and x > 0 else None)

        show_cols = {"ticker": "Ticker", "name": "Nom", "sector": "Secteur",
                     "last": "Prix (FCFA)", "variation": "Var (%)",
                     "market_cap": "Cap (Mds FCFA)", "beta": "Beta", "rsi": "RSI", "dps": "DPS"}
        available = {k: v for k, v in show_cols.items() if k in fmt_df.columns}
        st.dataframe(
            fmt_df[list(available.keys())].rename(columns=available),
            use_container_width=True, height=600,
            column_config={
                "Prix (FCFA)": st.column_config.NumberColumn(format="%.0f"),
                "Var (%)": st.column_config.NumberColumn(format="%.2f %%"),
                "Cap (Mds FCFA)": st.column_config.NumberColumn(format="%.1f"),
                "Beta": st.column_config.NumberColumn(format="%.2f"),
                "RSI": st.column_config.NumberColumn(format="%.0f"),
                "DPS": st.column_config.NumberColumn(format="%.0f"),
            }
        )

        # Quick jump to stock analysis from the full cotations table
        picker_options = [
            (row["ticker"], f"{row['ticker']} — {row.get('name', '')}")
            for _, row in fmt_df.iterrows()
            if row.get("ticker")
        ]
        ticker_quick_picker(picker_options, key="dash_goto", label="Ouvrir l'analyse d'un titre")

    # --- Indices (grille 4 colonnes fixe pour homogénéité des tailles) ---
    st.subheader("Indices BRVM")
    indices = _load_indices_from_db()
    if indices.empty:
        st.info("Indices non disponibles — lancez scripts/scrape_indices.py")
    else:
        has_category = "category" in indices.columns

        def _short_name(name: str) -> str:
            return (name or "").replace("BRVM - ", "").replace("BRVM-", "").strip()

        def _render_idx_metric(idx):
            val_str = f"{idx['value']:,.2f}" if pd.notna(idx.get("value")) else "—"
            delta_str = f"{idx['variation']:.2f}%" if pd.notna(idx.get("variation")) else None
            ytd = f" | YTD: {idx['ytd_variation']:+.2f}%" if pd.notna(idx.get("ytd_variation")) else ""
            help_txt = f"Variation depuis le 31 déc{ytd}" if ytd else None
            st.metric(_short_name(idx["name"]), val_str, delta=delta_str, help=help_txt)

        # Sélection par catégorie
        if has_category:
            principaux = indices[indices["category"] == "principal"]
            sectoriels = indices[indices["category"] == "sectoriel"]
            total_return = indices[indices["category"] == "total_return"]
        else:
            principaux = indices[indices["name"].str.contains(
                "COMPOSITE|BRVM-30|PRESTIGE|PRINCIPAL", case=False, na=False)]
            total_return = indices[indices["name"].str.contains(
                "TOTAL RETURN", case=False, na=False)]
            sectoriels = indices[~indices["name"].str.contains(
                "COMPOSITE|BRVM-30|PRESTIGE|PRINCIPAL|TOTAL RETURN", case=False, na=False)]

        # Rangée 1 : principaux + total return → toujours sur la même ligne
        # (Composite, BRVM-30, Prestige, Total Return = max 4)
        row1 = list(principaux.iterrows()) + list(total_return.iterrows())
        if row1:
            st.markdown(
                '<div class="label-xs" style="margin:6px 0 4px 2px;">'
                'Indices principaux</div>',
                unsafe_allow_html=True,
            )
            cols = st.columns(4)  # grille fixe 4 colonnes
            for i, (_, idx) in enumerate(row1[:4]):
                with cols[i]:
                    _render_idx_metric(idx)

        # Sectoriels : grille fixe 4 colonnes, padded avec empty slots
        if not sectoriels.empty:
            st.markdown(
                '<div class="label-xs" style="margin:10px 0 4px 2px;">'
                'Indices sectoriels</div>',
                unsafe_allow_html=True,
            )
            sect_list = list(sectoriels.iterrows())
            # Par rangées de 4 — toujours 4 colonnes même si la dernière en a moins
            for start in range(0, len(sect_list), 4):
                chunk = sect_list[start:start + 4]
                cols_s = st.columns(4)
                for i in range(4):
                    with cols_s[i]:
                        if i < len(chunk):
                            _render_idx_metric(chunk[i][1])
                        # else : colonne vide → largeur préservée, pas de reflow
