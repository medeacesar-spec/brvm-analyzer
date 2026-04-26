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
    get_all_company_profiles, get_company_news,
    save_company_news, save_company_profile,
    get_connection, get_publication_calendar,
)
from data.db import read_sql_df
from utils.ui_helpers import section_heading


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
    from data.scraper import fetch_company_news
    tickers = load_tickers()
    progress = st.progress(0, text="Actualisation…")
    for i, t in enumerate(tickers):
        try:
            articles = fetch_company_news(t["ticker"], max_articles=5)
            if articles:
                save_company_news(t["ticker"], articles)
        except Exception:
            pass
        progress.progress((i + 1) / len(tickers))
        time.sleep(0.2)
    progress.empty()


# ════════════════════════════════════════════════════════════════════
# Entry
# ════════════════════════════════════════════════════════════════════

def render():
    st.title("Infos Marché")
    st.caption("Actualités, panorama des sociétés et calendrier des publications BRVM")

    tab1, tab2, tab3 = st.tabs([
        "Fil d'actualités",
        "Panorama des sociétés",
        "Calendrier des publications",
    ])
    with tab1:
        _render_news_feed()
    with tab2:
        _render_company_overview()
    with tab3:
        _render_publication_calendar()


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

    news = get_company_news(selected_ticker, limit=200)
    if news.empty:
        st.info("Aucune actualité chargée. Cliquez sur Actualiser ou "
                 "lancez `scripts/scrape_profiles.py`.")
        return

    # ── Classification par type ──
    news = news.copy()
    news["type"] = news["title"].apply(_classify_publication)

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
        csv = news[["article_date", "ticker", "title", "type", "url"]].to_csv(index=False)
        st.download_button(
            "Télécharger CSV", data=csv,
            file_name="infos_marche.csv", mime="text/csv",
            key="news_download_csv",
        )

    if news.empty:
        st.info("Aucune publication pour ces filtres.")
        return

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

    rows = [
        f"<tr>"
        f"<th style='{header_style}'>Date</th>"
        f"<th style='{header_style}'>Ticker</th>"
        f"<th style='{header_style}'>Publication</th>"
        f"<th style='{header_style};text-align:right;'>Type</th>"
        f"</tr>"
    ]
    for _, art in news.iterrows():
        ticker = art.get("ticker") or ""
        date_raw = art.get("article_date") or ""
        # Format DD/MM depuis YYYY-MM-DD si possible
        if isinstance(date_raw, str) and len(date_raw) >= 10:
            try:
                date_disp = f"{date_raw[8:10]}/{date_raw[5:7]}"
            except Exception:
                date_disp = date_raw
        else:
            date_disp = str(date_raw) if date_raw else "—"
        title = art.get("title") or ""
        url = art.get("url") or ""
        if url and str(url).startswith("http"):
            title_html = f"<a href='{url}' target='_blank' style='color:var(--ink);text-decoration:none;'>{title}</a>"
        else:
            title_html = title
        typ = art.get("type", "Autre")
        tag_style = tag_styles.get(typ, tag_styles["Autre"])

        rows.append(
            f"<tr>"
            f"<td style='{num_style}'>{date_disp}</td>"
            f"<td style='{cell_style}'><span class='ticker'>{ticker}</span></td>"
            f"<td style='{cell_style}'>{title_html}</td>"
            f"<td style='{cell_style};text-align:right;'>"
            f"<span style='{tag_style}padding:3px 10px;border-radius:4px;"
            f"font-size:10.5px;font-weight:600;letter-spacing:0.04em;"
            f"text-transform:uppercase;'>{typ}</span>"
            f"</td>"
            f"</tr>"
        )

    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:10px;"
        f"overflow:hidden;background:var(--bg-elev);margin-top:10px;'>"
        f"<table style='width:100%;border-collapse:collapse;'>{''.join(rows)}</table></div>",
        unsafe_allow_html=True,
    )


# ════════════════════════════════════════════════════════════════════
# Tab 2 : Panorama des sociétés
# ════════════════════════════════════════════════════════════════════

def _render_company_overview():
    profiles = get_all_company_profiles()
    if profiles.empty:
        st.info("Les profils n'ont pas encore été chargés.")
        if st.button("Charger les profils (~30s)", key="load_all_profiles",
                       type="primary"):
            count = _sync_all_profiles()
            st.success(f"{count} profils chargés")
            st.rerun()
        return

    tickers_data = load_tickers()
    ticker_names = {t["ticker"]: t["name"] for t in tickers_data}
    ticker_sectors = {t["ticker"]: t.get("sector", "") for t in tickers_data}

    # ── Filtre secteur ──
    all_sectors = sorted(set(ticker_sectors.values()) - {""})
    st.markdown(
        "<div class='label-xs' style='margin-bottom:4px;'>Filtrer par secteur</div>",
        unsafe_allow_html=True,
    )
    col_f, col_count = st.columns([6, 1])
    with col_f:
        selected_sector = st.selectbox(
            "Filtrer par secteur", ["Tous"] + all_sectors,
            label_visibility="collapsed", key="profile_sector_filter",
        )

    # ── Market data ──
    md = read_sql_df(
        "SELECT ticker, price, market_cap, shares, float_pct "
        "FROM market_data WHERE price > 0"
    )
    md_map = {r["ticker"]: r.to_dict() for _, r in md.iterrows()} if not md.empty else {}

    # ── Filtrer & compter ──
    filtered = []
    for _, p in profiles.iterrows():
        sec = ticker_sectors.get(p["ticker"], "")
        if selected_sector != "Tous" and sec != selected_sector:
            continue
        filtered.append(p)

    with col_count:
        st.markdown(
            f"<div style='text-align:right;padding-top:8px;color:var(--ink-3);"
            f"font-size:12.5px;'>{len(filtered)} sociétés</div>",
            unsafe_allow_html=True,
        )

    st.caption(
        "Pour l'analyse détaillée d'un titre, ouvrez Analyse d'un Titre → onglet Profil."
    )

    # ── Cartes éditoriales ──
    if not filtered:
        st.info("Aucune société pour ce filtre.")
        return

    for profile in filtered:
        ticker = profile["ticker"]
        name = ticker_names.get(ticker, ticker)
        sector = ticker_sectors.get(ticker, "")
        mdata = md_map.get(ticker, {})
        price = mdata.get("price", 0) or 0
        mcap = mdata.get("market_cap", 0) or 0

        price_str = f"{price:,.0f} FCFA" if price else "—"
        mcap_str = f"{mcap/1e3:,.1f} Mds" if mcap else "—"

        # Label de l'expander : société + ticker + secteur visibles quand
        # collapsed (auparavant label="" → l'utilisateur ne savait pas
        # quelle société sans cliquer pour expand).
        expander_label = (
            f"{name} · {ticker}"
            + (f" · {sector}" if sector else "")
            + (f" · {price_str}" if price else "")
        )
        with st.expander(expander_label):
            # Header carte (sans le nom : déjà dans le label de l'expander)
            st.markdown(
                f"<div style='margin-top:-4px;'>"
                f"<span style='float:right;color:var(--ink-3);font-size:12px;'>"
                f"{sector}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='margin-top:6px;color:var(--ink-3);font-size:12px;"
                f"font-variant-numeric:tabular-nums;'>"
                f"<span class='label-xs' style='color:var(--ink-3);'>Prix</span> "
                f"<span style='color:var(--ink-2);margin-right:18px;'>{price_str}</span> "
                f"<span class='label-xs' style='color:var(--ink-3);'>Capitalisation</span> "
                f"<span style='color:var(--ink-2);'>{mcap_str}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

            # Description
            desc = profile.get("description")
            if desc and isinstance(desc, str) and desc.strip():
                for prefix in ["La société :", "La société:"]:
                    if desc.startswith(prefix):
                        desc = desc[len(prefix):].strip()
                st.markdown(
                    f"<div style='margin-top:10px;font-size:13px;color:var(--ink);"
                    f"line-height:1.55;'>{desc[:500]}"
                    f"{'…' if len(desc) > 500 else ''}</div>",
                    unsafe_allow_html=True,
                )

            # Dirigeants & actionnaires
            rows_info = []
            if profile.get("dg"):
                rows_info.append(("DG", profile["dg"]))
            if profile.get("president"):
                rows_info.append(("PCA", profile["president"]))
            if profile.get("major_shareholder"):
                pct = profile.get("major_shareholder_pct")
                pct_str = f" ({pct:.1f}%)" if pct else ""
                rows_info.append(("Actionnaire", f"{profile['major_shareholder']}{pct_str}"))
            if profile.get("phone"):
                rows_info.append(("Téléphone", profile["phone"]))

            if rows_info:
                info_html = "".join(
                    f"<tr>"
                    f"<td style='padding:6px 10px 6px 0;color:var(--ink-3);"
                    f"font-size:10.5px;text-transform:uppercase;letter-spacing:0.06em;"
                    f"font-weight:600;width:110px;'>{k}</td>"
                    f"<td style='padding:6px 0;color:var(--ink);font-size:13px;'>{v}</td>"
                    f"</tr>"
                    for k, v in rows_info
                )
                st.markdown(
                    f"<table style='margin-top:10px;border-collapse:collapse;'>"
                    f"{info_html}</table>",
                    unsafe_allow_html=True,
                )


# ════════════════════════════════════════════════════════════════════
# Tab 3 : Calendrier des publications
# ════════════════════════════════════════════════════════════════════

def _render_publication_calendar():
    tickers_data = load_tickers()
    all_sectors = sorted(set(t.get("sector", "") for t in tickers_data) - {""})

    st.markdown(
        "<div class='label-xs' style='margin-bottom:4px;'>Filtrer par secteur</div>",
        unsafe_allow_html=True,
    )
    col_f, col_count = st.columns([6, 1])
    with col_f:
        selected_sector = st.selectbox(
            "Filtrer par secteur", ["Tous les secteurs"] + all_sectors,
            key="cal_sector_filter", label_visibility="collapsed",
        )

    calendar = get_publication_calendar()
    if calendar.empty:
        st.info("Aucune publication enregistrée. Importez des données "
                 "fondamentales pour alimenter le calendrier.")
        return

    if selected_sector != "Tous les secteurs":
        sector_tickers = {t["ticker"] for t in tickers_data
                          if t.get("sector") == selected_sector}
        calendar = calendar[calendar["ticker"].isin(sector_tickers)]

    with col_count:
        st.markdown(
            f"<div style='text-align:right;padding-top:8px;color:var(--ink-3);"
            f"font-size:12.5px;'>{len(calendar)} publication(s)</div>",
            unsafe_allow_html=True,
        )

    if calendar.empty:
        st.info("Aucune publication attendue pour ce secteur.")
        return

    status_order = [
        ("en_retard", "En retard", "down"),
        ("attendu_ce_mois", "Attendu ce mois", "ocre"),
        ("a_venir", "À venir", "neutral"),
    ]

    header_style = (
        "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
        "color:var(--ink-3);font-weight:500;padding:9px 12px;"
        "border-bottom:1px solid var(--border);background:var(--bg-sunken);text-align:left;"
    )
    cell_style = "padding:11px 12px;font-size:13px;border-bottom:1px solid var(--border);"

    for status, label, tone in status_order:
        group = calendar[calendar["status"] == status] if "status" in calendar.columns else pd.DataFrame()
        if group.empty:
            continue

        section_heading(f"{label} · {len(group)}", spacing="loose")

        rows = [
            f"<tr>"
            f"<th style='{header_style}'>Ticker</th>"
            f"<th style='{header_style}'>Société</th>"
            f"<th style='{header_style}'>Période</th>"
            f"<th style='{header_style}'>Type</th>"
            f"<th style='{header_style};text-align:right;'>Statut</th>"
            f"</tr>"
        ]
        for _, row in group.iterrows():
            ticker = row.get("ticker") or ""
            name = row.get("company_name") or ticker
            period = row.get("period") or "—"
            ptype = row.get("type") or "—"
            rows.append(
                f"<tr>"
                f"<td style='{cell_style}'><span class='ticker'>{ticker}</span></td>"
                f"<td style='{cell_style};font-weight:500;'>{name}</td>"
                f"<td style='{cell_style};color:var(--ink-2);'>{period}</td>"
                f"<td style='{cell_style};color:var(--ink-2);'>{ptype}</td>"
                f"<td style='{cell_style};text-align:right;'>"
                f"<span class='dot {tone}'></span>"
                f"<span style='color:var(--ink-2);'>{label}</span>"
                f"</td>"
                f"</tr>"
            )

        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:10px;"
            f"overflow:hidden;background:var(--bg-elev);margin-bottom:8px;'>"
            f"<table style='width:100%;border-collapse:collapse;'>{''.join(rows)}</table></div>",
            unsafe_allow_html=True,
        )
