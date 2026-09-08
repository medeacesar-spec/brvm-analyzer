"""
Page 1 : Dashboard Marché BRVM
Lit uniquement depuis la base SQLite locale (pre-chargee par app.py au demarrage).
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from data.storage import (
    get_connection, get_cached_prices, get_portfolio,
    get_pending_publications, get_data_gaps,
    ignore_publication, delete_publication, mark_publication_integrated,
    ignore_gap,
    get_dormant_tickers, mark_ticker_inactive,
)
from data.db import read_sql_df
from utils.nav import ticker_analyze_button, ticker_quick_picker
from utils.auth import is_admin


def _load_quotes_from_db() -> pd.DataFrame:
    conn = get_connection()
    df = read_sql_df("""SELECT ticker, company_name as name, sector, price as last,
           variation, market_cap, beta, rsi, dps, updated_at
           FROM market_data WHERE price > 0 ORDER BY ticker""")
    # Un titre retire de la cote n'a plus de cours a comparer : il fausserait
    # les hausses et les baisses du jour avec sa derniere cotation connue.
    from config import tickers_retires
    _hors_cote = tickers_retires()
    if _hors_cote and not df.empty:
        df = df[~df["ticker"].isin(_hors_cote)]

    # Source unique de verite : price_cache. La date affichee dans la caption
    # correspond EXACTEMENT a la date la plus recente presente dans le cache,
    # et les variations sont calculees entre cette date et la precedente.
    # Plus de desynchro entre "Cloture du Vendredi" et "Aucune hausse" :
    # si le cache est pollue (ex. entrees Dimanche d'un ancien bug), on
    # affiche honnetement "Dimanche" et on compare Dimanche vs jour d'avant.
    dates = read_sql_df("SELECT DISTINCT date FROM price_cache ORDER BY date DESC LIMIT 2")
    if len(dates) >= 2:
        last_date = dates.iloc[0]["date"]
        prev_date = dates.iloc[1]["date"]
        prices = read_sql_df(
            """SELECT p1.ticker, p1.close as last_close, p2.close as prev_close
               FROM price_cache p1
               JOIN price_cache p2 ON p1.ticker = p2.ticker AND p2.date = ?
               WHERE p1.date = ?""",
            params=(prev_date, last_date),
        )
        if not prices.empty:
            prices["var_pct"] = (
                (prices["last_close"] - prices["prev_close"]) / prices["prev_close"] * 100
            ).round(2)
            var_map = dict(zip(prices["ticker"], prices["var_pct"]))
            df["variation"] = df["ticker"].map(var_map).fillna(df["variation"]).fillna(0)
            df["_last_trading_date"] = last_date
        else:
            # Pas de paire comparable → on utilise market_data.variation telle quelle
            df["_last_trading_date"] = last_date
    elif len(dates) == 1:
        df["_last_trading_date"] = dates.iloc[0]["date"]
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
            # market_data stocke la capitalisation en FRANCS : le repli doit
            # rester dans la meme unite, sinon la colonne melange deux echelles.
            df["market_cap"] = df.apply(
                lambda r: r["last"] * shares_map.get(r["ticker"], 0), axis=1
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
    results = {"day": [], "week": [], "month": [], "quarter": [], "ytd": [],
               "ranges": {}}
    today = datetime.now()

    # Fenêtre "Semaine" = 7 derniers jours calendaires glissants (simple).
    week_start = today - timedelta(days=7)
    month_ago = today - timedelta(days=30)
    # Trimestre et depuis le 1er janvier : uniquement pour la carte de chaleur
    # sectorielle, qui a besoin d'une tendance de fond à côté du jour.
    quarter_ago = today - timedelta(days=90)
    year_start = datetime(today.year, 1, 1)

    results["ranges"] = {
        "day": today.date(),
        "week_start": week_start.date(),
        "week_end": today.date(),
        "month_start": month_ago.date(),
        "month_end": today.date(),
        "quarter_start": quarter_ago.date(),
        "ytd_start": year_start.date(),
    }

    # 1 seule requête pour tous les tickers (mise en cache 5 min)
    all_prices = get_all_cached_prices()

    for _, row in quotes.iterrows():
        ticker, name, last_price = row.get("ticker", ""), row.get("name", ""), row.get("last", 0)
        if not ticker or not last_price:
            continue

        prices = all_prices.get(ticker, pd.DataFrame())

        # Variation "Jour" : toujours recalculée depuis price_cache (last vs
        # last-1). market_data.variation est souvent 0 sur weekend / lundi
        # pre-open (pas de trade du jour) — ne pas s'y fier.
        day_var = 0.0
        if not prices.empty and len(prices) >= 2:
            _p = prices.sort_values("date")
            last_close = _p.iloc[-1]["close"]
            prev_close = _p.iloc[-2]["close"]
            if prev_close and prev_close > 0:
                day_var = (last_close - prev_close) / prev_close * 100
        else:
            # Pas d'historique : fallback sur market_data.variation (rare)
            day_var = row.get("variation", 0) or 0
        results["day"].append({"ticker": ticker, "name": name, "price": last_price, "variation": day_var})

        if prices.empty or len(prices) < 5:
            # Sans historique, aucune fenêtre n'est calculable : on inscrit
            # None plutôt que 0, qui se lirait comme une stabilité.
            for _k in ("week", "month", "quarter", "ytd"):
                results[_k].append({"ticker": ticker, "name": name,
                                    "price": last_price,
                                    "variation": 0 if _k in ("week", "month") else None})
            continue

        prices = prices.sort_values("date")
        for period_key, start_dt, end_dt in [("week", week_start, today),
                                             ("month", month_ago, today),
                                             ("quarter", quarter_ago, today),
                                             ("ytd", year_start, today)]:
            pdata = prices[(prices["date"] >= pd.Timestamp(start_dt)) & (prices["date"] <= pd.Timestamp(end_dt))]
            if len(pdata) >= 2:
                var = ((pdata.iloc[-1]["close"] - pdata.iloc[0]["close"]) / pdata.iloc[0]["close"] * 100) if pdata.iloc[0]["close"] > 0 else 0
            elif period_key in ("week", "month"):
                var = 0
            else:
                # Fenêtres longues : pas d'historique suffisant → case vide.
                var = None
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
    from utils.ui_helpers import delta, ticker as ticker_chip, breadth_bar
    if df.empty or "variation" not in df.columns:
        return

    positive = df[df["variation"] > 0.01].nlargest(5, "variation")
    negative = df[df["variation"] < -0.01].nsmallest(5, "variation")

    # Largeur du marché sur la MÊME fenêtre que les listes ci-dessous : une
    # rangée de compteurs dit combien, la barre dit dans quelle proportion.
    _hausses = int((df["variation"] > 0.01).sum())
    _baisses = int((df["variation"] < -0.01).sum())
    breadth_bar(_hausses, len(df) - _hausses - _baisses, _baisses)

    col_up, col_dn = st.columns(2)

    def _render_list(rows: pd.DataFrame, title: str, tone: str, empty_msg: str,
                     arrow: str, key_prefix: str):
        """Rend une card bordée avec titre dot + liste compact des variations.
        tone : "up" ou "down" pour la couleur du dot et du tag variation."""
        count = len(rows)
        header_color = "var(--up)" if tone == "up" else "var(--down)"
        # Titre de section bold avec flèche + compteur en sub-ligne
        st.markdown(
            f"<div style='display:flex;align-items:baseline;gap:10px;"
            f"margin-bottom:10px;'>"
            f"<div style='font-size:17px;font-weight:600;color:var(--ink);"
            f"letter-spacing:-0.01em;'>"
            f"<span style='color:{header_color};font-size:18px;'>{arrow}</span> "
            f"{title}</div>"
            f"<div style='font-family:var(--font-mono);color:var(--ink-3);"
            f"font-size:11.5px;letter-spacing:0.04em;'>"
            f"TOP {count if count else 0}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        if rows.empty:
            st.markdown(
                f"<div style='padding:20px 14px;background:var(--bg-elev);"
                f"border:1px solid var(--border);border-radius:12px;"
                f"color:var(--ink-3);font-size:13px;text-align:center;'>"
                f"{empty_msg}</div>",
                unsafe_allow_html=True,
            )
            return

        inner = ""
        for _rang, (i, r) in enumerate(rows.iterrows(), start=1):
            # Variation + couleur
            var = r["variation"]
            var_color = header_color
            sign = "+" if var >= 0 else ""
            # Chaque ligne : nom (gauche, plus gros), prix + var (droite)
            inner += (
                f"<div style='display:flex;align-items:center;justify-content:space-between;"
                f"gap:12px;padding:11px 16px;border-bottom:1px solid var(--border-soft);'>"
                # Rang, puis nom + ticker chip
                f"<span style='font-family:var(--font-mono);font-size:11px;"
                f"color:var(--ink-4);width:14px;flex:0 0 auto;'>{_rang:02d}</span>"
                f"<div style='min-width:0;flex:1;'>"
                f"<div style='font-size:14px;font-weight:600;color:var(--ink);"
                f"white-space:nowrap;overflow:hidden;text-overflow:ellipsis;'>"
                f"{r['name']}</div>"
                f"<div style='margin-top:2px;'>{ticker_chip(r['ticker'])}</div>"
                f"</div>"
                # Droite : prix + variation
                f"<div style='text-align:right;flex-shrink:0;'>"
                f"<div style='font-size:15px;font-weight:600;color:var(--ink);"
                f"font-variant-numeric:tabular-nums;letter-spacing:-0.01em;'>"
                f"{r['price']:,.0f}</div>"
                f"<div style='font-size:13px;font-weight:600;color:{var_color};"
                f"font-variant-numeric:tabular-nums;margin-top:2px;'>"
                f"{sign}{var:.2f}%</div>"
                f"</div>"
                f"</div>"
            )
        # Card bordée, close par la bande d'accès du canevas.
        # Cinq boutons alignés sous la carte, un par titre, remplissaient la
        # largeur d'une rangée de commandes pour une action qu'on fait une
        # fois. Le canevas met une seule bande en pied de carte ; le saut vers
        # un titre PRÉCIS reste possible juste dessous, au sélecteur du
        # tableau des cotations. Rien n'est perdu, la page respire.
        st.markdown(
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:12px;overflow:hidden;'>{inner}</div>",
            unsafe_allow_html=True,
        )
        if count:
            if st.button("Ouvrir l'analyse d'un titre  →",
                         key=f"dash_{key_prefix}_{label}_ouvrir",
                         use_container_width=True):
                from utils.nav import goto_analyse
                goto_analyse()

    # Le docstring promettait depuis longtemps que `label` entrait dans le
    # titre ; il n'y entrait pas. « Hausses » seul laissait croire au jour
    # même dans l'onglet Mois.
    with col_up:
        _render_list(positive, f"Hausses {label}".strip(), tone="up", arrow="↗",
                     empty_msg="Aucune hausse sur la période",
                     key_prefix="up")
    with col_dn:
        _render_list(negative, f"Baisses {label}".strip(), tone="down", arrow="↘",
                     empty_msg="Aucune baisse sur la période",
                     key_prefix="dn")


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


@st.cache_data(ttl=600, show_spinner=False)
def _cached_dormant_tickers():
    """Seuil : 12 mois sans publication → candidat au marquage inactif."""
    try:
        return get_dormant_tickers(threshold_months=12)
    except Exception:
        return None


def _render_dormant_tickers_alert():
    """Section admin : titres n'ayant plus publié depuis 12+ mois OU sans
    aucune publication en base, avec boutons pour les marquer inactifs."""
    if not is_admin():
        return
    df = _cached_dormant_tickers()
    if df is None or df.empty:
        return

    n_stale = int((df["reason"] == "publications_arretees").sum())
    n_none = int((df["reason"] == "aucune_publication").sum())
    parts = []
    if n_stale:
        parts.append(f"{n_stale} sans pub depuis 12+ mois")
    if n_none:
        parts.append(f"{n_none} sans aucune publication en base")

    st.info(
        f"💤 **{len(df)} titre(s) potentiellement dormant(s)** — "
        + ", ".join(parts)
    )
    with st.expander("Voir la liste et agir", expanded=False):
        st.caption(
            "Pour chaque titre : **Marquer inactif** = ignore permanent "
            "(disparaît des alertes « manque annuel »). **Rester en veille** = "
            "ne rien faire, ré-alerte le mois prochain."
        )
        # En-tête
        h1, h2, h3, h4, h5, h6, h7 = st.columns(
            [1.1, 2.2, 1.4, 1.2, 1.5, 1.3, 1.3]
        )
        h1.markdown("**Ticker**")
        h2.markdown("**Nom**")
        h3.markdown("**Dernière pub.**")
        h4.markdown("**Mois écoulés**")
        h5.markdown("**Motif**")
        h6.markdown("")
        h7.markdown("")

        REASON_LABEL = {
            "publications_arretees": "Pub. arrêtées",
            "aucune_publication": "Aucune pub. en base",
        }

        for _, row in df.iterrows():
            ticker = row["ticker"]
            name = row.get("name") or "—"
            last_pub = row.get("last_pub_date")
            months = row.get("months_since")
            reason = row.get("reason")
            c1, c2, c3, c4, c5, c6, c7 = st.columns(
                [1.1, 2.2, 1.4, 1.2, 1.5, 1.3, 1.3]
            )
            c1.markdown(f"`{ticker}`")
            c2.markdown(str(name)[:35])
            c3.markdown(
                str(last_pub)[:10] if pd.notna(last_pub) else "—"
            )
            c4.markdown(
                f"{months:.0f}" if pd.notna(months) else "—"
            )
            c5.markdown(REASON_LABEL.get(reason, reason))
            reason_txt = (
                f"Dormant depuis {months:.0f} mois"
                if pd.notna(months)
                else "Aucune publication trouvée en base"
            )
            if c6.button("Marquer inactif", key=f"dormant_mark_{ticker}",
                          use_container_width=True):
                ok = mark_ticker_inactive(
                    ticker,
                    reason=f"{reason_txt} (marqué via tableau de bord)",
                )
                if ok:
                    _cached_dormant_tickers.clear()
                    _cached_data_gaps.clear()
                    st.success(f"{ticker} marqué inactif")
                    st.rerun()
                else:
                    st.error(f"Échec du marquage de {ticker}")
            c7.button("Rester en veille",
                       key=f"dormant_skip_{ticker}",
                       help="Ne change rien. Ré-alerte dans un mois.",
                       use_container_width=True)


def _render_pending_publications_alert():
    """Affiche une bannière si des publications récentes (états financiers annuels ou
    données trimestrielles) n'ont pas encore été intégrées en base.

    Réservée aux admins : c'est une info de qualité de données qui n'est
    pertinente que pour les responsables de l'intégration.

    Résultats cachés 5 min (les deux requêtes font ensemble 240+ round-trips
    Supabase dans la version non optimisée).
    """
    if not is_admin():
        return
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

        def _fr(text) -> str:
            # Garde-fou : NaN/None/non-string → "". Sans ça, `pd.NA or ""`
            # renvoie NaN (truthy) qui n'a pas de .replace() et crash.
            if text is None or not isinstance(text, str) or not text:
                return ""
            out = text
            for old, new in _ACCENT_REPLACEMENTS:
                out = out.replace(old, new)
            return out[:1].upper() + out[1:] if out else out

        # Scraped pending publications
        if pending is not None and not pending.empty:
            head_col, action_col = st.columns([5, 2], vertical_alignment="center")
            head_col.markdown("#### Publications des 7 derniers jours à intégrer")
            if admin:
                head_col.caption(
                    "Bouton **Chercher** par ligne télécharge le PDF et rafraîchit "
                    "les données. Cochez puis **Ignorer la sélection** pour retirer "
                    "les publications non pertinentes."
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
            _REASON_LABELS = {
                "nouveau": "Nouveau",
                "annuel_non_integre": "Annuel à intégrer",
                "trimestriel_a_verifier": "Trimestriel à vérifier",
            }

            # Bulk-ignore button (admin only) — invalide le cache 5 min après action
            if admin:
                with action_col:
                    if st.button(
                        "Ignorer la sélection",
                        icon=":material/delete:",
                        use_container_width=True,
                        key="bulk_ignore_pubs",
                        help="Marque les publications cochées comme ignorées",
                    ):
                        n_done = 0
                        for skey in list(st.session_state.keys()):
                            if not skey.startswith("sel_pub_"):
                                continue
                            if st.session_state.get(skey):
                                try:
                                    rid = int(skey.split("_")[-1])
                                    ignore_publication(rid)
                                    st.session_state[skey] = False
                                    n_done += 1
                                except Exception:
                                    continue
                        if n_done:
                            try:
                                _cached_pending_publications.clear()
                            except Exception:
                                pass
                            st.session_state["_pubs_flash"] = (
                                f"{n_done} publication(s) ignorée(s)"
                            )
                            st.rerun()

            # Flash de confirmation
            flash = st.session_state.pop("_pubs_flash", None)
            if flash:
                st.success(flash)

            # Header colonnes (admin a checkbox + action en plus)
            if admin:
                col_widths = [0.4, 0.7, 1.3, 5.0, 0.6, 1.0, 0.9, 0.6]
                h = st.columns(col_widths, vertical_alignment="center")
                h[0].markdown(_head("Sél."), unsafe_allow_html=True)
                h[7].markdown(_head("Action"), unsafe_allow_html=True)
                offset = 1
            else:
                col_widths = [0.7, 1.3, 5.0, 0.6, 1.0, 0.9]
                h = st.columns(col_widths, vertical_alignment="center")
                offset = 0
            h[offset].markdown(_head("Ticker"), unsafe_allow_html=True)
            h[offset + 1].markdown(_head("Type"), unsafe_allow_html=True)
            h[offset + 2].markdown(_head("Titre"), unsafe_allow_html=True)
            h[offset + 3].markdown(_head("Ex."), unsafe_allow_html=True)
            h[offset + 4].markdown(_head("Date"), unsafe_allow_html=True)
            h[offset + 5].markdown(_head("URL"), unsafe_allow_html=True)

            from scripts.fetch_publication import _is_financial_statement

            for _, row in pending.head(15).iterrows():
                rid = int(row["id"])
                is_financial = _is_financial_statement(
                    row.get("title"), row.get("pub_type"),
                )
                c = st.columns(col_widths, vertical_alignment="center")
                pub_type_raw = row.get("pub_type") or ""
                pub_type_label = _TYPE_LABELS.get(
                    pub_type_raw, pub_type_raw.capitalize() if pub_type_raw else "—"
                )
                reason_label = _REASON_LABELS.get(
                    row.get("pending_reason"), row.get("pending_reason") or "",
                )
                # Type avec libellé de raison entre parenthèses si différent
                type_display = (
                    f"{pub_type_label} · {reason_label}" if reason_label else pub_type_label
                )

                if admin:
                    c[0].checkbox(
                        f"Sél. {rid}",
                        key=f"sel_pub_{rid}",
                        label_visibility="collapsed",
                    )

                c[offset].markdown(
                    _cell(row["ticker"] or "—", title=row["ticker"] or ""),
                    unsafe_allow_html=True,
                )
                c[offset + 1].markdown(
                    _cell(type_display, title=row.get("pending_reason") or ""),
                    unsafe_allow_html=True,
                )
                title_fr = _fr(row.get("title") or "")
                c[offset + 2].markdown(_cell(title_fr, title=title_fr),
                                        unsafe_allow_html=True)
                year_txt = (
                    str(int(row["fiscal_year"]))
                    if pd.notna(row.get("fiscal_year")) else "—"
                )
                c[offset + 3].markdown(_cell(year_txt), unsafe_allow_html=True)
                pub_date = row.get("pub_date") or ""
                short_date = pub_date[5:] if len(pub_date) >= 10 else pub_date
                c[offset + 4].markdown(
                    _cell(short_date or "—", title=pub_date),
                    unsafe_allow_html=True,
                )
                if row.get("url"):
                    c[offset + 5].markdown(
                        f'<div style="{CELL_STYLE}">'
                        f'<a href="{row["url"]}" target="_blank">Voir</a></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    c[offset + 5].markdown(_cell("—"), unsafe_allow_html=True)

                if admin:
                    with c[7]:
                        if is_financial:
                            if st.button(
                                "Chercher",
                                icon=":material/download:",
                                key=f"fetch_pub_{rid}",
                                help="Télécharger l'état financier + rafraîchir sika",
                                use_container_width=True,
                            ):
                                with st.spinner(f"Récupération {row['ticker']}…"):
                                    from scripts.fetch_publication import (
                                        auto_fetch_publication,
                                    )
                                    res = auto_fetch_publication(rid)
                                if res.get("success"):
                                    parts = []
                                    s = res.get("sika", {})
                                    if s.get("inserted") or s.get("updated"):
                                        parts.append(
                                            f"Sika: +{s.get('inserted',0)} / "
                                            f"~{s.get('updated',0)}"
                                        )
                                    if res.get("pdf_path"):
                                        parts.append(
                                            f"PDF → {os.path.basename(res['pdf_path'])}"
                                        )
                                    st.session_state["_pubs_flash"] = (
                                        " · ".join(parts) or "Téléchargé"
                                    )
                                else:
                                    err = (
                                        res.get("sika", {}).get("error")
                                        or res.get("pdf_error")
                                        or res.get("sika_error")
                                        or "Aucune donnée récupérée"
                                    )
                                    st.warning(err)
                                try:
                                    _cached_pending_publications.clear()
                                except Exception:
                                    pass
                                st.rerun()
                        else:
                            c[7].caption("Info")

        # Gaps detected — ignore action admin only (cases à cocher + bulk)
        if gaps is not None and not gaps.empty:
            st.markdown("<div style='margin-top:14px'></div>", unsafe_allow_html=True)

            # En-tête + bouton bulk-ignore
            head_col, action_col = st.columns([5, 2], vertical_alignment="center")
            head_col.markdown("#### Écarts détectés par rapport au cycle de publication")
            head_col.caption(
                "Cycle UEMOA : rapport annuel au plus tard fin avril, "
                "trimestriels dans les 45j suivant la fin de trimestre."
                + (" Cochez puis Ignorer pour les titres qui ne publient pas."
                   if admin else "")
            )
            # Map clé checkbox → action ignore_gap(ticker, gap_type, fiscal_year)
            # Pour l'annuel : on stocke aussi `published_year` (annee REELLEMENT
            # manquante d'apres publications) pour l'option Completer-via-sika.
            gap_actions: dict = {}
            for _, row in gaps.iterrows():
                tk = row["ticker"]
                if row.get("missing_annual"):
                    yr = int(row["expected_latest"])
                    fetch_yr = (
                        int(row["published_year"])
                        if pd.notna(row.get("published_year")) else yr
                    )
                    gap_actions[f"sel_gap_ann_{tk}_{yr}"] = (
                        tk, "annuel", yr, fetch_yr,
                    )
                if pd.notna(row.get("missing_quarter")):
                    gap_actions[f"sel_gap_q_{tk}"] = (tk, "trimestriel", None, None)

            if admin:
                with action_col:
                    bcol1, bcol2 = st.columns(2)
                    if bcol1.button(
                        "Ignorer",
                        icon=":material/delete:",
                        use_container_width=True,
                        key="bulk_ignore_gaps",
                        help="Marque les écarts cochés comme non-applicables",
                    ):
                        n_done = 0
                        for skey, (tk, gt, yr, _fy) in gap_actions.items():
                            if st.session_state.get(skey):
                                ignore_gap(
                                    tk, gt, yr,
                                    reason="Marqué non-applicable par l'utilisateur",
                                )
                                st.session_state[skey] = False
                                n_done += 1
                        if n_done:
                            try:
                                _cached_data_gaps.clear()
                            except Exception:
                                pass
                            st.session_state["_gaps_flash"] = (
                                f"{n_done} écart(s) ignoré(s)"
                            )
                            st.rerun()
                    if bcol2.button(
                        "Compléter via sika",
                        icon=":material/cloud_download:",
                        use_container_width=True,
                        key="bulk_complete_gaps",
                        help=(
                            "Récupère les chiffres manquants (revenue, "
                            "net_income, eps, dps...) depuis sikafinance "
                            "pour les écarts annuels cochés. Ne touche que "
                            "les champs NULL — préserve les valeurs existantes."
                        ),
                    ):
                        from scripts.fetch_publication import (
                            complete_missing_fundamentals_from_sika,
                        )
                        n_filled = 0
                        n_inserted = 0
                        n_skipped_q = 0
                        errors = []
                        for skey, (tk, gt, _yr, fetch_yr) in gap_actions.items():
                            if not st.session_state.get(skey):
                                continue
                            if gt != "annuel":
                                n_skipped_q += 1
                                continue
                            try:
                                res = complete_missing_fundamentals_from_sika(
                                    tk, fetch_yr,
                                )
                                if res.get("error"):
                                    errors.append(f"{tk} {fetch_yr}: {res['error']}")
                                    continue
                                if res.get("inserted"):
                                    n_inserted += res["inserted"]
                                if res.get("updated"):
                                    n_filled += 1
                                # Décoche apres action OK
                                st.session_state[skey] = False
                            except Exception as e:
                                errors.append(f"{tk} {fetch_yr}: {e}")
                        try:
                            _cached_data_gaps.clear()
                        except Exception:
                            pass
                        bits = []
                        if n_inserted:
                            bits.append(f"{n_inserted} nouvelle(s) ligne(s)")
                        if n_filled:
                            bits.append(f"{n_filled} ligne(s) complétée(s)")
                        if n_skipped_q:
                            bits.append(
                                f"{n_skipped_q} trim. ignoré(s) "
                                "(sika ne fournit pas le détail trimestriel)"
                            )
                        if errors:
                            bits.append(f"{len(errors)} erreur(s)")
                        if bits:
                            st.session_state["_gaps_flash"] = " · ".join(bits)
                            if errors:
                                st.session_state["_gaps_errors"] = errors
                            st.rerun()

            # Flash de confirmation après bulk-ignore / completion
            flash = st.session_state.pop("_gaps_flash", None)
            if flash:
                st.success(flash)
            errors = st.session_state.pop("_gaps_errors", None)
            if errors:
                with st.expander(f"{len(errors)} erreur(s) détaillée(s)",
                                  expanded=False):
                    for e in errors:
                        st.warning(e)

            if admin:
                col_widths = [0.4, 0.4, 0.7, 2.5, 1.5, 0.7, 2.5]
                h = st.columns(col_widths, vertical_alignment="center")
                h[0].markdown(_head("An."), unsafe_allow_html=True)
                h[1].markdown(_head("Tr."), unsafe_allow_html=True)
                h[2].markdown(_head("Ticker"), unsafe_allow_html=True)
                h[3].markdown(_head("Nom"), unsafe_allow_html=True)
                h[4].markdown(_head("Secteur"), unsafe_allow_html=True)
                h[5].markdown(_head("Dern."), unsafe_allow_html=True)
                h[6].markdown(_head("Manquant"), unsafe_allow_html=True)
            else:
                col_widths = [0.7, 2.5, 1.5, 0.7, 2.5]
                h = st.columns(col_widths, vertical_alignment="center")
                h[0].markdown(_head("Ticker"), unsafe_allow_html=True)
                h[1].markdown(_head("Nom"), unsafe_allow_html=True)
                h[2].markdown(_head("Secteur"), unsafe_allow_html=True)
                h[3].markdown(_head("Dern."), unsafe_allow_html=True)
                h[4].markdown(_head("Manquant"), unsafe_allow_html=True)

            for _, row in gaps.iterrows():
                tk = row["ticker"]
                c = st.columns(col_widths, vertical_alignment="center")

                if admin:
                    # Checkbox annuel
                    if row.get("missing_annual"):
                        yr = int(row["expected_latest"])
                        c[0].checkbox(
                            "An.",
                            key=f"sel_gap_ann_{tk}_{yr}",
                            label_visibility="collapsed",
                        )
                    else:
                        c[0].markdown(_cell(""), unsafe_allow_html=True)
                    # Checkbox trimestriel
                    if pd.notna(row.get("missing_quarter")):
                        c[1].checkbox(
                            "Tr.",
                            key=f"sel_gap_q_{tk}",
                            label_visibility="collapsed",
                        )
                    else:
                        c[1].markdown(_cell(""), unsafe_allow_html=True)
                    offset = 2
                else:
                    offset = 0

                c[offset].markdown(_cell(tk, title=tk), unsafe_allow_html=True)
                name = _fr(row.get("name") or "")
                c[offset + 1].markdown(_cell(name, title=name), unsafe_allow_html=True)
                sector = _fr(row.get("sector") or "")
                c[offset + 2].markdown(_cell(sector, title=sector), unsafe_allow_html=True)
                year_txt = (
                    str(int(row["latest_year_in_db"]))
                    if pd.notna(row.get("latest_year_in_db")) else "—"
                )
                c[offset + 3].markdown(_cell(year_txt), unsafe_allow_html=True)
                # Pour l'annuel : affiche l'annee REELLEMENT manquante.
                # `published_year` est l'annee d'une publication scrapee
                # mais pas encore integree (prioritaire) ; sinon on tombe
                # sur `expected_latest` (annee attendue par cycle UEMOA).
                annual_year = (
                    int(row.get("published_year"))
                    if pd.notna(row.get("published_year")) else
                    int(row.get("expected_latest"))
                    if pd.notna(row.get("expected_latest")) else None
                )
                manquant = ", ".join(filter(None, [
                    f"Annuel {annual_year}"
                    if row.get("missing_annual") and annual_year else None,
                    f"Trim. {row['missing_quarter']}"
                    if pd.notna(row.get("missing_quarter")) else None,
                ]))
                c[offset + 4].markdown(
                    _cell(manquant, title=manquant), unsafe_allow_html=True,
                )

        if admin:
            st.caption(
                "**Chercher** télécharge le PDF dans `pdfs/{ticker}/` et rafraîchit les "
                "données de marché. Pour extraire les données détaillées, lancer "
                "`python3 scripts/extract_pdfs.py`."
            )


def _render_sector_heatmap(quotes: pd.DataFrame, perf: dict):
    """Carte de chaleur : variation par secteur sur cinq fenêtres.

    La moyenne est **pondérée par la capitalisation**, pas arithmétique : un
    secteur où Sonatel pèse dix fois les autres ne se lit pas en donnant le
    même poids à chaque ligne. Les secteurs sont classés par la fenêtre la
    plus longue disponible — la tendance de fond, pas l'agitation du jour.
    """
    from utils.ui_helpers import heatmap

    fenetres = [("Jour", "day"), ("Semaine", "week"), ("Mois", "month"),
                ("3 mois", "quarter"), ("Depuis le 1er janv.", "ytd")]
    if "sector" not in quotes.columns:
        return
    secteurs = quotes[["ticker", "sector", "market_cap"]].dropna(subset=["sector"])
    if secteurs.empty:
        return

    lignes = {}
    for _, cle in fenetres:
        cadre = perf.get(cle)
        if cadre is None or cadre.empty or "variation" not in cadre.columns:
            for nom in secteurs["sector"].unique():
                lignes.setdefault(nom, []).append(None)
            continue
        joint = secteurs.merge(cadre[["ticker", "variation"]], on="ticker", how="inner")
        joint = joint[joint["variation"].notna()]
        for nom in sorted(secteurs["sector"].unique()):
            part = joint[joint["sector"] == nom]
            poids = part["market_cap"].fillna(0)
            if part.empty or poids.sum() <= 0:
                # Sans capitalisation exploitable, la moyenne simple reste
                # honnête tant qu'elle est annoncée comme telle en pied.
                valeur = float(part["variation"].mean()) if not part.empty else None
            else:
                valeur = float((part["variation"] * poids).sum() / poids.sum())
            lignes.setdefault(nom, []).append(valeur)

    # Classement par la dernière colonne renseignée (la fenêtre la plus longue)
    def _cle_tri(item):
        vals = [v for v in item[1] if v is not None]
        return -(vals[-1] if vals else -999)

    ordonnees = sorted(lignes.items(), key=_cle_tri)
    if not ordonnees:
        return

    st.markdown(
        "<div style='display:flex;align-items:baseline;gap:10px;"
        "margin:26px 0 12px;'>"
        "<h2 style='font-size:17px;font-weight:600;margin:0;"
        "letter-spacing:-0.015em;'>Secteurs · variation par fenêtre</h2>"
        "<span style='font-family:var(--font-mono);font-size:11.5px;"
        f"color:var(--ink-3);'>{len(ordonnees)} SECTEURS</span></div>",
        unsafe_allow_html=True,
    )
    heatmap(
        ordonnees,
        [libelle for libelle, _ in fenetres],
        footer="Moyenne pondérée par la capitalisation. Le jour se lit dans "
               "la première colonne, la tendance de fond dans la dernière ; "
               "une case vide est une fenêtre sans historique suffisant, "
               "pas une stabilité.",
    )


def render():
    # Hiérarchie v3 : Title + caption → KPI row → Tabs → contenu (pas de divider)
    quotes = _load_quotes_from_db()
    if quotes.empty:
        st.title("Marché BRVM")
        st.caption("Données en cours de chargement — patientez quelques secondes puis rafraîchissez.")
        return

    MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
               "juillet", "aout", "septembre", "octobre", "novembre", "décembre"]

    last_trading_date = (
        quotes.get("_last_trading_date", pd.Series()).iloc[0]
        if "_last_trading_date" in quotes.columns and len(quotes) > 0 else None
    )

    # ── Contexte séance depuis snapshot_meta (source unique de vérité) ──
    # Le label est recalculé au render via utils.session_labels (évite le
    # 'Clôture veille' figé qui persistait après minuit).
    _meta_map = {}
    try:
        from data.db import get_connection as _gc
        _c = _gc()
        _rows = _c.execute(
            "SELECT key, value FROM snapshot_meta "
            "WHERE key IN ('last_session_date','last_session_time','last_session_is_open')"
        ).fetchall()
        _c.close()
        for _r in _rows:
            k = _r[0] if not isinstance(_r, dict) else _r.get("key")
            v = _r[1] if not isinstance(_r, dict) else _r.get("value")
            _meta_map[k] = v
    except Exception:
        pass

    from utils.session_labels import build_session_label
    session_label = build_session_label(
        session_date=_meta_map.get("last_session_date") or (
            str(last_trading_date)[:10] if last_trading_date else None
        ),
        session_time=_meta_map.get("last_session_time"),
        is_open=_meta_map.get("last_session_is_open"),
    )

    st.title("Marché BRVM")
    st.caption("Variations calculées entre la dernière clôture et la "
               "précédente, à partir du cache de cotations.")

    # Alerte publications non intégrées (conditionnelle, ne s'affiche que si besoin)
    _render_pending_publications_alert()

    # Alerte titres dormants (>12 mois sans publication) — actions Marquer inactif
    _render_dormant_tickers_alert()

    # KPI row — densité v3 (st.metric compact via CSS)
    positive = quotes[quotes["variation"] > 0.01] if "variation" in quotes.columns else pd.DataFrame()
    negative = quotes[quotes["variation"] < -0.01] if "variation" in quotes.columns else pd.DataFrame()
    total_mcap = quotes["market_cap"].sum() if "market_cap" in quotes.columns else 0

    from utils.ui_helpers import kpi_v4
    _stables = len(quotes) - len(positive) - len(negative)
    _cote = len(quotes) or 1

    # Chaque carte porte la couleur de ce qu'elle dit : la rangee se lit avant
    # meme d'avoir lu un chiffre. Le sous-titre donne la part de la cote —
    # « 18 hausses » ne veut pas dire la meme chose sur 48 titres ou sur 20.
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_v4("Hausses", f"{len(positive)}",
               f"{len(positive) / _cote * 100:.0f} % de la cote",
               accent="var(--up)", sub_color="var(--up)")
    with col2:
        kpi_v4("Baisses", f"{len(negative)}",
               f"{len(negative) / _cote * 100:.0f} % de la cote",
               accent="var(--down)", sub_color="var(--down)")
    with col3:
        kpi_v4("Stables", f"{_stables}",
               f"{_stables / _cote * 100:.0f} % de la cote",
               accent="var(--ink-4)")
    with col4:
        # La capitalisation est stockee en FRANCS : diviser par 1e3 affichait
        # 18 390 570 100 "Mds" au lieu de 18 391 Mds.
        kpi_v4("Capitalisation",
               f"{total_mcap / 1e9:,.0f}".replace(",", " ") if total_mcap > 0 else "—",
               "Mds FCFA", accent="var(--primary)")

    # ── Bulletin Officiel de la Cote ──
    # Chiffres officiels de la BRVM et, surtout, les operations a venir : le
    # BOC annonce les dividendes en BRUT avec leur date de mise en paiement et
    # le taux de retenue applicable, ce qu'aucune autre source ne donne.
    _render_boc()

    # Acces rapide a la revue de presse (l'analyse detaillee vit dans Infos Marche)
    _revue_col, _ = st.columns([1, 3])
    with _revue_col:
        if st.button("Revue de presse", key="dash_goto_revue",
                     help="Dépêches du marché croisées avec vos lignes",
                     use_container_width=True):
            from utils.nav import goto_revue
            goto_revue()

    # Tabs Jour / Semaine / Mois — labels avec date explicite dans chaque caption
    perf = _compute_period_performance(quotes)
    ranges = perf.get("ranges", {})

    def _fmt_date(d):
        try:
            return f"{d.day} {MOIS_FR[d.month-1][:4]}"
        except Exception:
            return "—"

    # Tab Jour : label uniquement (la caption principale au-dessus porte déjà
    # l'info de date/session, pas besoin de la redupliquer).
    day_label = session_label.day_tab

    week_caption = ""
    if ranges.get("week_start") and ranges.get("week_end"):
        week_caption = (
            f"7 derniers jours · "
            f"{_fmt_date(ranges['week_start'])} → {_fmt_date(ranges['week_end'])}"
        )
    month_caption = ""
    if ranges.get("month_start") and ranges.get("month_end"):
        month_caption = (
            f"30 derniers jours · "
            f"{_fmt_date(ranges['month_start'])} → {_fmt_date(ranges['month_end'])}"
        )

    tab_day, tab_week, tab_month = st.tabs([day_label, "Semaine", "Mois"])
    with tab_day:
        st.caption(f"Dernière clôture comparée à la précédente · "
                   f"{session_label.date_long.lower()}")
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

    # --- Indices (grille 4 colonnes fixe pour homogénéité des tailles) ---
    st.subheader("Indices BRVM")
    indices = _load_indices_from_db()
    if indices.empty:
        st.info("Indices non disponibles — lancez scripts/scrape_indices.py")
    else:
        has_category = "category" in indices.columns

        def _short_name(name: str) -> str:
            """« BRVM - Composite » → « Composite ». Mais « BRVM-30 » reste
            « BRVM-30 » : retirer le prefixe n'y laissait que « 30 », ce qui
            ne nomme plus rien."""
            court = (name or "").replace("BRVM - ", "").replace("BRVM-", "").strip()
            return name.strip() if court.isdigit() else court

        def _render_idx_metric(idx):
            """Carte d'indice au modèle du canevas : la variation du jour ET
            le cumul depuis le 1er janvier sur la même ligne. Le YTD vivait
            dans une infobulle — invisible tant qu'on ne survolait pas, alors
            que c'est lui qui dit la tendance de l'année."""
            from utils.ui_helpers import kpi_v4
            val = idx.get("value")
            var = idx.get("variation")
            ytd = idx.get("ytd_variation")
            val_str = f"{val:,.2f}".replace(",", " ") if pd.notna(val) else "—"
            bouts = []
            if pd.notna(var):
                bouts.append(f"{var:+.2f} %")
            if pd.notna(ytd):
                bouts.append(f"YTD {ytd:+.2f} %")
            # La couleur suit la variation du JOUR : c'est elle qui bouge.
            if pd.notna(var) and var > 0:
                accent, teinte = "var(--up)", "var(--up)"
            elif pd.notna(var) and var < 0:
                accent, teinte = "var(--down)", "var(--down)"
            else:
                accent, teinte = "var(--ink-4)", ""
            kpi_v4(_short_name(idx["name"]), val_str, " · ".join(bouts),
                   accent=accent, sub_color=teinte)

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

    _render_sector_heatmap(quotes, perf)

    # ─── Toutes les cotations ───────────────────────────────────────────
    # Le canevas la donne OUVERTE, pas repliée : c'est le dernier bloc de la
    # page, celui où l'on va vérifier une ligne précise. La replier obligeait
    # à un clic pour atteindre la seule chose qu'on venait chercher.
    _var30 = {}
    _m = perf.get("month")
    if _m is not None and not _m.empty:
        _var30 = {r["ticker"]: r["variation"] for _, r in _m.iterrows()}

    # Le bêta et le RSI du canevas sont désormais calculés
    # (`scripts/calculer_beta_rsi.py`). La corrélation au marché est lue avec
    # eux : sur cette place elle est faible, et un bêta dont la corrélation
    # tombe sous 0,30 ne décrit presque rien — il s'affiche en gris.
    try:
        from analysis.risque import toutes_les_mesures as _mesures_risque
        _risque = _mesures_risque()
    except Exception:                                           # noqa: BLE001
        _risque = {}

    st.markdown(
        "<div style='display:flex;align-items:baseline;gap:10px;"
        "margin:26px 0 12px;'>"
        "<h2 style='font-size:17px;font-weight:600;margin:0;"
        "letter-spacing:-0.015em;'>Toutes les cotations</h2>"
        "<span style='font-family:var(--font-mono);font-size:11.5px;"
        f"color:var(--ink-3);'>{len(quotes)} LIGNES</span></div>",
        unsafe_allow_html=True,
    )

    _c_sect, _c_tri = st.columns([2, 2])
    with _c_sect:
        sectors = ["Tous"] + sorted(quotes["sector"].dropna().unique().tolist())
        selected_sector = st.selectbox("Filtrer par secteur", sectors)
    with _c_tri:
        # Un tableau HTML ne se trie pas au clic comme un st.dataframe : le
        # tri redevient donc explicite, plutôt que perdu.
        _tri = st.selectbox(
            "Trier par",
            ["Capitalisation", "Variation du jour", "Variation 30 jours",
             "Ticker", "RSI", "Bêta"],
            key="dash_tri_cotations",
        )

    display_df = (quotes[quotes["sector"] == selected_sector]
                  if selected_sector != "Tous" else quotes).copy()
    display_df["var30"] = display_df["ticker"].map(_var30)

    _cles = {
        "Capitalisation": ("market_cap", False),
        "Variation du jour": ("variation", False),
        "Variation 30 jours": ("var30", False),
        "Ticker": ("ticker", True),
        "RSI": ("rsi", False),
        "Bêta": ("beta", False),
    }
    _col, _asc = _cles[_tri]
    if _col in display_df.columns:
        display_df = display_df.sort_values(_col, ascending=_asc,
                                            na_position="last")

    # Quarante-sept lignes d'un bloc noient la fin de page : on ne les lit
    # pas, on les fait defiler. Le canevas lui-meme n'en montre que huit et
    # annonce le reste en pied. Le tri s'applique AVANT la coupe — les huit
    # premieres sont donc les huit qui comptent selon le critere choisi, pas
    # les huit premieres de l'alphabet.
    APERCU = 8
    _tout = st.session_state.get("dash_cotations_tout", False)
    _total_lignes = len(display_df)
    _visible = display_df if _tout else display_df.head(APERCU)
    _restantes = _total_lignes - len(_visible)

    def _barre_signee(v):
        """Barre centrée sur un axe : à droite si positive, à gauche sinon.

        Une variation sur trente jours a un signe ; une barre qui part
        toujours de la gauche le perd. L'échelle est bornée à ±40 %, au-delà
        la barre sature — c'est l'ordre de grandeur qui compte ici, pas le
        centième de point.
        """
        if v is None or pd.isna(v):
            return "<span style='color:var(--ink-4);'>—</span>"
        borne = 40.0
        part = max(-1.0, min(1.0, v / borne)) * 50.0
        couleur = "var(--up)" if v >= 0 else "var(--down)"
        gauche = 50.0 if v >= 0 else 50.0 + part
        return (
            "<span title='" + f"{v:+.2f} % sur 30 jours" + "' "
            "style='position:relative;display:inline-block;width:100%;"
            "min-width:70px;height:8px;background:var(--bg-sunken);"
            "border-radius:999px;overflow:hidden;vertical-align:middle;'>"
            f"<span style='position:absolute;top:0;left:{gauche:.1f}%;"
            f"width:{abs(part):.1f}%;height:100%;background:{couleur};'></span>"
            "<span style='position:absolute;top:0;left:50%;width:1px;"
            "height:8px;background:var(--border-strong);'></span></span>"
        )

    def _n(v, fmt="{:,.0f}", vide="—"):
        if v is None or pd.isna(v):
            return vide
        return fmt.format(v).replace(",", " ")

    _th = ("font-size:10px;text-transform:uppercase;letter-spacing:0.09em;"
           "color:var(--ink-3);font-weight:600;padding:9px 12px;"
           "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
           "white-space:nowrap;")
    _td = "padding:10px 12px;border-bottom:1px solid var(--border-soft);font-size:13px;"
    # white-space:nowrap — sans lui, « +0,30 % » se coupait entre le nombre
    # et son signe de pourcentage dans une colonne etroite.
    _tdn = (_td + "text-align:right;font-variant-numeric:tabular-nums;"
            "white-space:nowrap;")

    _lignes = (
        "<tr>"
        f"<th style='{_th}text-align:left;'>Ticker</th>"
        f"<th style='{_th}text-align:left;'>Nom</th>"
        f"<th style='{_th}text-align:left;'>Secteur</th>"
        f"<th style='{_th}text-align:right;'>Prix</th>"
        f"<th style='{_th}text-align:right;'>Var</th>"
        f"<th style='{_th}text-align:left;'>Var. 30 j</th>"
        f"<th style='{_th}text-align:right;'>Cap (Mds)</th>"
        f"<th style='{_th}text-align:right;'>Bêta</th>"
        f"<th style='{_th}text-align:right;'>RSI</th>"
        "</tr>"
    )
    for _, r in _visible.iterrows():
        _v = r.get("variation")
        _cv = ("var(--up)" if _v and _v > 0 else
               "var(--down)" if _v and _v < 0 else "var(--ink-3)")
        _cap = r.get("market_cap")
        _mes = _risque.get(r.get("ticker")) or {}
        _corr = _mes.get("correlation_marche")
        # Un bêta dont la corrélation est faible se lit en gris : la valeur
        # est juste, mais elle n'explique presque rien du mouvement du titre.
        _c_beta = ("var(--ink-4)" if _corr is not None and abs(_corr) < 0.30
                   else "var(--ink-2)")
        _t_beta = (f"corrélation au marché {_corr:.2f}"
                   if _corr is not None else "corrélation inconnue")
        _rsi = r.get("rsi")
        _c_rsi = ("var(--warn)" if _rsi is not None and not pd.isna(_rsi)
                  and (_rsi > 70 or _rsi < 30) else "var(--ink-2)")
        _lignes += (
            "<tr>"
            f"<td style='{_td}font-family:var(--font-mono);font-size:11.5px;"
            f"font-weight:600;color:var(--ink-2);'>{r.get('ticker', '')}</td>"
            f"<td style='{_td}font-weight:500;'>{r.get('name', '')}</td>"
            f"<td style='{_td}color:var(--ink-2);font-size:12.5px;'>"
            f"{r.get('sector') or '—'}</td>"
            f"<td style='{_tdn}'>{_n(r.get('last'))}</td>"
            f"<td style='{_tdn}font-weight:600;color:{_cv};'>"
            f"{_n(_v, '{:+.2f} %')}</td>"
            f"<td style='{_td}min-width:90px;'>{_barre_signee(r.get('var30'))}</td>"
            f"<td style='{_tdn}color:var(--ink-2);'>"
            f"{_n(_cap / 1e9 if _cap and not pd.isna(_cap) else None, '{:,.1f}')}</td>"
            f"<td style='{_tdn}color:{_c_beta};' title='{_t_beta}'>"
            f"{_n(r.get('beta'), '{:.2f}')}</td>"
            f"<td style='{_tdn}color:{_c_rsi};font-weight:"
            f"{600 if _c_rsi != 'var(--ink-2)' else 400};'>"
            f"{_n(_rsi, '{:.0f}')}</td>"
            "</tr>"
        )

    st.markdown(
        "<div style='background:var(--bg-elev);border:1px solid var(--border);"
        "border-radius:12px;overflow:hidden;'>"
        "<div style='overflow-x:auto;'>"
        "<table style='width:100%;border-collapse:collapse;min-width:920px;'>"
        f"{_lignes}</table></div>"
        "<div style='padding:10px 12px;background:var(--bg-footer);"
        "font-size:11.5px;color:var(--ink-3);'>"
        + (f"{len(_visible)} lignes sur {_total_lignes}, triées par "
           f"{_tri.lower()}. " if _restantes > 0 else
           f"{_total_lignes} lignes, triées par {_tri.lower()}. ") +
        "Barre signée : variation sur 30 jours, échelle ±40 % autour de l'axe "
        "central ; au-delà, la barre sature. Une barre absente est un titre "
        "sans historique mensuel suffisant. Bêta mensuel contre le BRVM "
        "Composite, sur 24 mois minimum ; en gris quand la corrélation au "
        "marché tombe sous 0,30 — la valeur reste juste, mais elle "
        "n'explique presque rien. RSI de Wilder sur 14 cotations "
        "quotidiennes, en ocre au-delà de 70 ou en deçà de 30."
        "</div></div>",
        unsafe_allow_html=True,
    )

    # Un seul bouton, compact et centré sous la carte : « + 39 » déplie,
    # « − » replie. Une bande pleine largeur aurait pesé autant que le
    # tableau qu'elle prolonge ; le signe et le nombre suffisent à dire ce
    # qui reste.
    # `on_click` plutôt qu'un `st.rerun()` écrit à la main : le rappel s'exécute
    # AVANT le rerun, donc l'état est déjà posé quand la page se redessine.
    # Avec le rerun manuel, l'écriture se perdait — le tableau restait à huit
    # lignes alors que le clic partait bien.
    def _deplier():
        st.session_state["dash_cotations_tout"] = True

    def _replier():
        st.session_state["dash_cotations_tout"] = False

    if _restantes > 0 or (_tout and _total_lignes > APERCU):
        _g, _milieu, _d = st.columns([4, 1.4, 4])
        with _milieu:
            if _restantes > 0:
                # Sans espace apres le plus : « + 39 » est une puce de
                # liste en Markdown, et Streamlit avalait le signe.
                st.button(f"+{_restantes}", key="dash_cotations_plus",
                          help=f"Afficher les {_restantes} lignes restantes",
                          on_click=_deplier, use_container_width=True)
            else:
                st.button("−", key="dash_cotations_moins",
                          help="Revenir à l'aperçu",
                          on_click=_replier, use_container_width=True)

    # Accès direct à l'analyse depuis le tableau
    picker_options = [
        (row["ticker"], f"{row['ticker']} — {row.get('name', '')}")
        for _, row in display_df.iterrows()
        if row.get("ticker")
    ]
    ticker_quick_picker(picker_options, key="dash_goto",
                        label="Ouvrir l'analyse d'un titre")


def _render_boc():
    """Synthese du dernier Bulletin Officiel + operations a venir."""
    try:
        bul = read_sql_df(
            "SELECT date_bulletin, numero, capitalisation, per_moyen, url "
            "FROM boc_bulletins ORDER BY date_bulletin DESC LIMIT 1")
        ops = read_sql_df(
            "SELECT emetteur, ticker, type, brut, irvm_physique, irvm_morale, "
            "date_operation, detail FROM boc_operations "
            "WHERE date_bulletin = (SELECT MAX(date_bulletin) FROM boc_operations) "
            "ORDER BY date_operation")
    except Exception:
        return
    if bul is None or bul.empty:
        return

    b = bul.iloc[0]
    lignes = [f"Bulletin officiel n° {b['numero']} du {b['date_bulletin']}"]
    if pd.notna(b.get("capitalisation")):
        lignes.append(f"capitalisation officielle {b['capitalisation']/1e9:,.0f} Mds")
    if pd.notna(b.get("per_moyen")):
        lignes.append(f"PER moyen du marché {b['per_moyen']:.2f}")
    st.caption(" · ".join(lignes))

    if ops is None or ops.empty:
        return

    # Les lignes du portefeuille passent devant
    try:
        pf = get_portfolio()
        detenus = set(pf["ticker"].dropna()) if pf is not None and not pf.empty else set()
    except Exception:
        detenus = set()

    divid = ops[ops["type"] == "dividende"].copy()
    autres = ops[ops["type"] != "dividende"]
    if not divid.empty:
        divid["_mien"] = divid["ticker"].apply(lambda t: t in detenus)
        divid = divid.sort_values(["_mien", "date_operation"], ascending=[False, True])

    with st.expander(
        f"Opérations à venir · {len(divid)} dividende(s) annoncé(s)", expanded=False
    ):
        for _, o in divid.iterrows():
            marque = " ★" if o["_mien"] else ""
            retenue = ""
            if pd.notna(o.get("irvm_physique")):
                net_pp = o["brut"] * (1 - o["irvm_physique"] / 100)
                retenue = (f" — net {net_pp:,.2f} F après IRVM "
                           f"{int(o['irvm_physique'])} % (personne physique)")
            st.markdown(
                f"**{o['emetteur']}**{marque} · **{o['brut']:,.2f} F brut/action** "
                f"le {o['date_operation']}{retenue}"
            )
        for _, o in autres.iterrows():
            st.caption(f"{o['emetteur']} — {str(o['detail'])[:160]}")
