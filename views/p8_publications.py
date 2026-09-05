"""
Page 8 : Infos Marché — design v3.
Actualités, panorama des sociétés et calendrier des publications BRVM.
"""

import streamlit as st
import pandas as pd
import time
import re

from config import load_tickers
from data.storage import (
    get_all_company_profiles, get_company_news, get_portfolio,
    save_company_news, save_company_profile,
    get_connection, get_publication_calendar,
    save_fundamentals, save_quarterly_data,
    get_recent_extraction_attempts,
)
from data.db import read_sql_df
from utils.ui_helpers import section_heading
from utils.auth import is_admin


# ════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════

_TYPE_PATTERNS = [
    ("Trimestriel", re.compile(r"\btrimestr|trim\.|\bt[1234]\b|1er trim|2[eè]me trim|3[eè]me trim|4[eè]me trim", re.I)),
    ("Semestriel", re.compile(r"semestr|\bs[12]\b|1er semestre|2[eè]me semestre", re.I)),
    ("Annuel", re.compile(r"\bannuel|exercice|rapport annuel|\bfy\b", re.I)),
    ("Gouvernance", re.compile(r"\bago\b|\bage\b|convocation|assembl[ée]e|conseil|gouvernance|dividende|distribution", re.I)),
]

_TYPE_TONES = {
    "Trimestriel": "up",     # vert
    "Semestriel": "neutral", # neutre
    "Annuel": "neutral",     # neutre
    "Gouvernance": "ocre",   # ocre
}


def _classify_publication(title: str) -> str:
    """Classifie une publication par type selon son libellé."""
    if not title:
        return "Autre"
    for label, pat in _TYPE_PATTERNS:
        if pat.search(title):
            return label
    return "Autre"


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


def _refresh_news():
    """Lance scan_publications.main() (richbourse + sikafinance) pour
    actualiser la table `publications` qui sert de source au fil."""
    with st.spinner("Scan des publications (richbourse + sikafinance)…"):
        try:
            from scripts.scan_publications import main as _scan_main
            _scan_main(limit=50)
        except Exception as e:
            st.error(f"Échec du scan : {e}")


# ════════════════════════════════════════════════════════════════════
# Entry
# ════════════════════════════════════════════════════════════════════

def render():
    st.title("Infos Marché")
    st.caption("Actualités et revue de presse BRVM")

    tab0, tab1 = st.tabs([
        "Revue de presse",
        "Fil d'actualités",
    ])
    with tab0:
        _render_revue()
    with tab1:
        _render_news_feed()


# ════════════════════════════════════════════════════════════════════
# Tab 0 : Revue de presse
# ════════════════════════════════════════════════════════════════════

def _render_revue():
    """Depeches croisees avec les chiffres extraits et le portefeuille.

    On ne reformule jamais : chaque entree combine des chiffres CALCULES par
    l'app et une citation TEXTUELLE de la source. Le fil brut reste disponible
    dans l'onglet voisin, pour verifier qu'aucune publication officielle n'est
    passee a la trappe.
    """
    from analysis.revue import build_revue, RUBRIQUES

    col_j, col_p = st.columns([1, 3])
    with col_j:
        jours = st.selectbox("Période", [7, 15, 30], index=1,
                             format_func=lambda j: f"{j} derniers jours")

    try:
        df_pf = get_portfolio()
        portefeuille = (df_pf["ticker"].dropna().unique().tolist()
                        if df_pf is not None and not df_pf.empty else [])
    except Exception:
        portefeuille = []

    try:
        rubriques = build_revue(jours=jours, portefeuille=portefeuille)
    except Exception as e:
        st.info(f"Revue indisponible : {e}")
        return

    if not rubriques or not any(rubriques.values()):
        st.info(
            "Aucune dépêche collectée pour l'instant. La collecte tourne avec "
            "le job quotidien (16h UTC, jours ouvrés)."
        )
        return

    for cle, titre in RUBRIQUES:
        entrees = rubriques.get(cle) or []
        if not entrees:
            continue
        st.markdown(f"#### {titre}  ·  {len(entrees)}")
        for e in entrees:
            badges = " ".join(f"`{t}`" for t, _ in e["sujets"])
            etoile = ("  ★ " + ", ".join(n for _, n in e["portefeuille"])
                      if e["portefeuille"] else "")
            st.markdown(
                f"<div style='border-left:2px solid var(--border);padding:2px 0 10px 12px;"
                f"margin-bottom:10px;'>"
                f"<div style='font-size:11px;color:var(--ink-3);'>{e['date']}{etoile}</div>"
                f"<div style='font-size:14px;font-weight:600;margin:2px 0 4px;'>"
                f"<a href='{e['url']}' target='_blank' style='color:inherit;"
                f"text-decoration:none;'>{e['titre']}</a></div>"
                + (f"<div style='font-size:12px;color:var(--ink-2);'>{e['chiffres']}</div>"
                   if e["chiffres"] else "")
                + (f"<div style='font-size:12px;color:var(--ink-3);'>Vos lignes exposées : "
                   f"{', '.join(n for _, n in e['exposees'])}</div>"
                   if e.get("exposees") and not e["portefeuille"] else "")
                + f"<div style='font-size:12.5px;color:var(--ink-2);margin-top:4px;'>"
                f"{e['texte']}</div>"
                + (f"<div style='font-size:11px;color:var(--ink-3);margin-top:3px;'>"
                   f"{badges}</div>" if badges else "")
                + "</div>",
                unsafe_allow_html=True,
            )


# ════════════════════════════════════════════════════════════════════
# Tab 1 : Fil d'actualités
# ════════════════════════════════════════════════════════════════════

def _render_news_feed():
    tickers_data = load_tickers()
    ticker_names = {t["ticker"]: t["name"] for t in tickers_data}

    # ── Ligne filtre + actions ──
    st.markdown(
        "<div class='label-xs' style='margin-bottom:4px;'>Filtrer par titre</div>",
        unsafe_allow_html=True,
    )
    col_f, col_a1, col_a2 = st.columns([6, 1, 1])
    with col_f:
        filter_options = ["Tous les titres"] + [
            f"{t['ticker']} - {t['name']}" for t in tickers_data
        ]
        filter_sel = st.selectbox(
            "Filtrer par titre", filter_options,
            key="news_filter", label_visibility="collapsed",
        )
    with col_a1:
        refresh_clicked = st.button("Actualiser", use_container_width=True,
                                     key="news_refresh")
    with col_a2:
        export_clicked = st.button("Exporter", use_container_width=True,
                                    key="news_export")

    if refresh_clicked:
        _refresh_news()
        st.rerun()

    selected_ticker = None
    if filter_sel != "Tous les titres":
        selected_ticker = filter_sel.split(" - ")[0]

    # Source : table `publications` (richbourse) — pub_date + pub_type fiables
    # et statut d'intégration calculable. company_news (sikafinance) n'avait
    # pas de date populée, on l'a abandonné.
    from analysis.publications import get_publications_with_status
    news = get_publications_with_status(ticker=selected_ticker, limit=200)
    if news.empty:
        st.info("Aucune publication scannée. Cliquez sur Actualiser ou "
                 "lancez `python scripts/scan_publications.py`.")
        return

    # Mappe pub_type (richbourse, lowercase) → label d'affichage cohérent
    # avec les chips de filtre.
    _pubtype_to_label = {
        "trimestriel": "Trimestriel",
        "semestriel": "Semestriel",
        "annuel": "Annuel",
        "gouvernance": "Gouvernance",
        "dividende": "Gouvernance",  # dividende = info gouvernance
    }
    news = news.copy()
    news["type"] = news["pub_type"].fillna("").str.lower().map(
        _pubtype_to_label
    ).fillna("Autre")

    # ── Chips de filtre par type ──
    type_counts = news["type"].value_counts().to_dict()
    type_order = ["Trimestriel", "Semestriel", "Annuel", "Gouvernance"]
    active_types = st.session_state.get("news_type_filter", set())

    col_chips, col_count = st.columns([6, 1])
    with col_chips:
        cols = st.columns(len(type_order))
        for i, tname in enumerate(type_order):
            count = type_counts.get(tname, 0)
            active = tname in active_types
            tone = _TYPE_TONES.get(tname, "neutral")
            # Couleur d'accent selon tone v3
            border = {"up": "#1F5D3A", "ocre": "#B5730E",
                      "neutral": "#7A756C"}.get(tone, "#7A756C")
            bg_active = {"up": "#E4F0E7", "ocre": "#F4E4C2",
                          "neutral": "#EDE8DC"}.get(tone, "#EDE8DC")

            with cols[i]:
                label = f"{tname.upper()} ({count})"
                # st.button simule le chip ; style via CSS si actif
                if active:
                    if st.button(f"✓ {label}", key=f"chip_{tname}",
                                   use_container_width=True):
                        active_types.discard(tname)
                        st.session_state["news_type_filter"] = active_types
                        st.rerun()
                else:
                    if st.button(label, key=f"chip_{tname}",
                                   use_container_width=True):
                        active_types.add(tname)
                        st.session_state["news_type_filter"] = active_types
                        st.rerun()
    with col_count:
        st.markdown(
            f"<div style='text-align:right;padding-top:8px;"
            f"color:var(--ink-3);font-size:12.5px;'>{len(news)} articles</div>",
            unsafe_allow_html=True,
        )

    # ── Appliquer filtre types actifs ──
    if active_types:
        news = news[news["type"].isin(active_types)]

    # ── Export CSV ──
    if export_clicked:
        csv = news[["pub_date", "ticker", "title", "type", "status", "url"]].to_csv(index=False)
        st.download_button(
            "Télécharger CSV", data=csv,
            file_name="infos_marche.csv", mime="text/csv",
            key="news_download_csv",
        )

    if news.empty:
        st.info("Aucune publication pour ces filtres.")
        return

    # ── KPI : combien à intégrer ──
    n_pending = int((news["status"] == "À intégrer").sum())
    if n_pending:
        st.caption(f"**{n_pending}** publication{'s' if n_pending > 1 else ''}"
                    " à intégrer")

    # ── Table éditoriale ──
    header_style = (
        "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
        "color:var(--ink-3);font-weight:500;padding:9px 12px;"
        "border-bottom:1px solid var(--border);background:var(--bg-sunken);text-align:left;"
    )
    cell_style = "padding:11px 12px;font-size:13px;border-bottom:1px solid var(--border);"
    num_style = cell_style + "font-variant-numeric:tabular-nums;color:var(--ink-2);"

    tag_styles = {
        "Trimestriel": "background:#E4F0E7;color:#1F5D3A;",
        "Semestriel": "background:#EDE8DC;color:#4A453C;",
        "Annuel": "background:#EDE8DC;color:#4A453C;",
        "Gouvernance": "background:#F4E4C2;color:#8A5A15;",
        "Autre": "background:#EDE8DC;color:#7A756C;",
    }

    from utils.ui_helpers import tag as _tag_html
    rows = [
        f"<tr>"
        f"<th style='{header_style}'>Date</th>"
        f"<th style='{header_style}'>Ticker</th>"
        f"<th style='{header_style}'>Publication</th>"
        f"<th style='{header_style};text-align:center;'>Type</th>"
        f"<th style='{header_style};text-align:right;'>Statut</th>"
        f"</tr>"
    ]
    for _, art in news.iterrows():
        ticker = art.get("ticker") or ""
        date_raw = art.get("pub_date")
        # Format DD/MM depuis YYYY-MM-DD ou datetime
        if isinstance(date_raw, str) and len(date_raw) >= 10:
            date_disp = f"{date_raw[8:10]}/{date_raw[5:7]}"
        elif date_raw is not None and not pd.isna(date_raw):
            try:
                date_disp = date_raw.strftime("%d/%m")
            except Exception:
                date_disp = str(date_raw)[:10]
        else:
            date_disp = "—"
        title = art.get("title_pretty") or art.get("title") or ""
        url = art.get("url") or ""
        if url and str(url).startswith("http"):
            title_html = f"<a href='{url}' target='_blank' style='color:var(--ink);text-decoration:none;'>{title}</a>"
        else:
            title_html = title
        typ = art.get("type", "Autre")
        tag_style = tag_styles.get(typ, tag_styles["Autre"])
        status_label = art.get("status") or ""
        status_tone = art.get("status_tone") or "neutral"
        status_html = _tag_html(status_label, status_tone) if status_label else ""

        rows.append(
            f"<tr>"
            f"<td style='{num_style}'>{date_disp}</td>"
            f"<td style='{cell_style}'><span class='ticker'>{ticker}</span></td>"
            f"<td style='{cell_style}'>{title_html}</td>"
            f"<td style='{cell_style};text-align:center;'>"
            f"<span style='{tag_style}padding:3px 10px;border-radius:4px;"
            f"font-size:10.5px;font-weight:600;letter-spacing:0.04em;"
            f"text-transform:uppercase;'>{typ}</span>"
            f"</td>"
            f"<td style='{cell_style};text-align:right;'>{status_html}</td>"
            f"</tr>"
        )

    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:10px;"
        f"overflow:hidden;background:var(--bg-elev);margin-top:10px;'>"
        f"<table style='width:100%;border-collapse:collapse;'>{''.join(rows)}</table></div>",
        unsafe_allow_html=True,
    )

    # ── Section diagnostic extraction (admin uniquement) ──
    if is_admin():
        _render_extraction_diagnostics(news)


def _render_extraction_diagnostics(news_df):
    """Section admin qui montre les échecs d'extraction récents +
    formulaire de saisie manuelle pour les publications À intégrer."""
    section_heading("Diagnostic extraction (admin)", spacing="loose")

    # ── Panneau échecs récents ──
    try:
        attempts = get_recent_extraction_attempts(limit=30)
    except Exception:
        attempts = pd.DataFrame()

    with st.expander(
        f"Tentatives d'extraction échouées ({len(attempts) if not attempts.empty else 0})",
        expanded=False,
    ):
        if attempts.empty:
            st.caption(
                "Aucun échec récent enregistré. La table `extraction_attempts` "
                "se remplit à chaque run quotidien de `extract_pending_pubs`."
            )
        else:
            _STATUS_LABELS = {
                "no_pdf": "Pas de PDF",
                "parser_empty": "Parser vide",
                "error": "Erreur téléchargement/parse",
                "save_error": "Erreur sauvegarde",
                "no_quarter": "Trimestre non détecté",
                "skip_unsupported_type": "Type non géré",
            }
            for _, a in attempts.iterrows():
                status_label = _STATUS_LABELS.get(a["status"], a["status"])
                when = str(a.get("attempted_at") or "")[:16].replace("T", " ")
                st.markdown(
                    f"**{a['ticker']}** FY{a.get('fiscal_year') or '?'} "
                    f"{a.get('pub_type') or ''} · `{status_label}` · {when}"
                )
                if a.get("error_message"):
                    st.caption(f"↳ {a['error_message'][:200]}")
                if a.get("extracted_summary"):
                    st.caption(f"↳ extrait : {a['extracted_summary'][:200]}")
                if a.get("report_link_url"):
                    st.caption(f"↳ PDF : {a['report_link_url']}")
                st.markdown("---")

    # ── Formulaire de saisie manuelle ──
    pending = news_df[news_df["status"] == "À intégrer"].copy() if not news_df.empty else pd.DataFrame()

    with st.expander(
        f"Saisir manuellement les publications À intégrer ({len(pending)})",
        expanded=False,
    ):
        if pending.empty:
            st.caption("Aucune publication À intégrer actuellement.")
            return
        st.caption(
            "Sélectionnez une publication, remplissez les valeurs depuis le PDF "
            "(unités en FCFA bruts, pas en millions/milliards) puis Enregistrer."
        )
        # Selectbox pour choisir la pub
        opts = []
        pub_map = {}
        for _, p in pending.iterrows():
            fy = p.get("fiscal_year")
            try:
                fy_int = int(fy) if fy is not None and not pd.isna(fy) else None
            except (TypeError, ValueError):
                fy_int = None
            label = f"{p['ticker']} · FY{fy_int} · {p['pub_type']} · {(p.get('title_pretty') or p.get('title') or '')[:50]}"
            opts.append(label)
            pub_map[label] = {"ticker": p["ticker"], "fiscal_year": fy_int,
                                "pub_type": p["pub_type"], "id": p.get("id"),
                                "url": p.get("url")}

        chosen = st.selectbox("Publication à saisir", opts, key="manual_pub_sel")
        pub = pub_map[chosen]
        is_annual = (pub["pub_type"] or "") == "annuel"

        with st.form("manual_extract_form"):
            st.markdown(
                f"**{pub['ticker']}** · FY {pub['fiscal_year']} · "
                f"{pub['pub_type']}"
            )
            if pub.get("url"):
                st.caption(f"Lien source : {pub['url']}")

            def _parse_num(s):
                if not s or not s.strip():
                    return None
                try:
                    return float(s.strip().replace(" ", "").replace(" ", "")
                                  .replace(",", "."))
                except (ValueError, AttributeError):
                    return None

            c1, c2 = st.columns(2)
            revenue_str = c1.text_input(
                "Revenue / CA / PNB (FCFA bruts)", value="",
                help="Ex : 113000000000 pour 113 milliards. Virgule ou point acceptés.",
                key="manual_rev",
            )
            ni_str = c2.text_input(
                "Résultat net (FCFA bruts)", value="",
                key="manual_ni",
            )
            equity_str = None
            total_assets_str = None
            ebit_str = None
            div_total_str = None
            quarter_val = None
            if is_annual:
                c3, c4 = st.columns(2)
                equity_str = c3.text_input(
                    "Capitaux propres (FCFA)", value="",
                    key="manual_eq",
                )
                total_assets_str = c4.text_input(
                    "Total actif (FCFA)", value="",
                    key="manual_ta",
                )
                c5, c6 = st.columns(2)
                ebit_str = c5.text_input(
                    "EBIT / Résultat d'exploitation (FCFA)", value="",
                    key="manual_ebit",
                )
                div_total_str = c6.text_input(
                    "Dividendes totaux (FCFA, optionnel)", value="",
                    key="manual_div",
                )
            else:
                quarter_val = st.selectbox(
                    "Trimestre (S1 = 2, S2 = 4)",
                    options=[1, 2, 3, 4], index=0,
                    key="manual_quarter",
                )
                ebit_str = st.text_input(
                    "EBIT (FCFA, optionnel)", value="",
                    key="manual_ebit_q",
                )

            csave, ccancel = st.columns(2)
            saved = csave.form_submit_button(
                "Enregistrer", type="primary", use_container_width=True,
            )
            csave2 = ccancel.form_submit_button(
                "Annuler", use_container_width=True,
            )
            if saved:
                revenue = _parse_num(revenue_str)
                net_income = _parse_num(ni_str)
                if revenue is None and net_income is None:
                    st.error("Au moins revenue ou net_income doit être renseigné.")
                else:
                    data = {
                        "ticker": pub["ticker"],
                        "fiscal_year": pub["fiscal_year"],
                        "revenue": revenue,
                        "net_income": net_income,
                        "ebit": _parse_num(ebit_str),
                    }
                    try:
                        if is_annual:
                            data["equity"] = _parse_num(equity_str)
                            data["total_assets"] = _parse_num(total_assets_str)
                            data["dividends_total"] = _parse_num(div_total_str)
                            save_fundamentals(data)
                            st.success(
                                f"✓ Fundamentals {pub['ticker']} FY{pub['fiscal_year']} enregistrés"
                            )
                        else:
                            data["quarter"] = int(quarter_val)
                            data["source"] = "saisie manuelle"
                            data["notes"] = "Saisi via UI Publications"
                            save_quarterly_data(data)
                            st.success(
                                f"✓ Quarterly {pub['ticker']} Q{quarter_val} FY{pub['fiscal_year']} enregistrés"
                            )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erreur d'enregistrement : {e}")
