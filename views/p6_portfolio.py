"""
Page 6 : Suivi Portefeuille
Gestion des positions et performance.
"""

import streamlit as st
import pandas as pd

from config import load_tickers, CURRENCY
from data.storage import (
    save_position, get_portfolio, delete_position,
    get_fundamentals, get_cached_prices, get_all_stocks_for_analysis,
    get_portfolio_cash, set_portfolio_cash,
)
from data.db import read_sql_df
from data.scraper import fetch_daily_quotes
from analysis.scoring import compute_hybrid_score, compute_consolidated_verdict
from utils.charts import pie_chart
from utils.nav import ticker_analyze_button
from utils.ui_helpers import section_heading

import json as _json


@st.cache_data(ttl=300, show_spinner=False)
def _load_scoring_dict() -> dict:
    """Retourne {ticker: row dict} depuis scoring_snapshot. Les signals/consolidated
    sont parses en JSON. 1 seule requete Supabase cachee 5 min."""
    try:
        df = read_sql_df(
            "SELECT ticker, company_name, sector, price, hybrid_score, "
            "fundamental_score, technical_score, verdict, stars, trend, "
            "nb_signals, signals_json, consolidated_json FROM scoring_snapshot"
        )
    except Exception:
        return {}
    if df.empty:
        return {}
    out = {}
    for _, r in df.iterrows():
        d = r.to_dict()
        try:
            d["_signals"] = _json.loads(d.get("signals_json") or "[]")
        except Exception:
            d["_signals"] = []
        try:
            d["_consolidated"] = _json.loads(d.get("consolidated_json") or "{}")
        except Exception:
            d["_consolidated"] = {}
        out[r["ticker"]] = d
    return out


@st.cache_data(ttl=300, show_spinner=False)
def _load_all_stocks_dict() -> dict:
    """Retourne {ticker: row dict} depuis get_all_stocks_for_analysis.
    Permet d'eviter N appels get_fundamentals(ticker) dans les boucles."""
    all_stocks = get_all_stocks_for_analysis()
    if all_stocks.empty:
        return {}
    import math as _m
    out = {}
    for _, r in all_stocks.iterrows():
        d = {k: (None if isinstance(v, float) and _m.isnan(v) else v)
             for k, v in r.to_dict().items()}
        out[r["ticker"]] = d
    return out


def render():
    portfolio = get_portfolio()

    # Load cash from DB once per session
    if "portfolio_cash" not in st.session_state:
        st.session_state.portfolio_cash = get_portfolio_cash()

    # ═══════════════════════════════════════════════════════════════════
    # Header row : Title + subtitle (gauche) · Actions (droite)
    # ═══════════════════════════════════════════════════════════════════
    n_pos = len(portfolio)
    # Date range : min(purchase_date) → aujourd'hui
    from datetime import datetime as _dtm
    today_str = _dtm.now().strftime("%d/%m/%Y")
    if n_pos > 0 and "purchase_date" in portfolio.columns:
        try:
            _min_d = pd.to_datetime(portfolio["purchase_date"]).min()
            subtitle = (
                f"{n_pos} position{'s' if n_pos > 1 else ''} · "
                f"relevé du {_min_d.strftime('%d/%m/%Y')} au {today_str}"
            )
        except Exception:
            subtitle = f"{n_pos} position{'s' if n_pos > 1 else ''} · relevé au {today_str}"
    else:
        subtitle = "Aucune position · commencez par ajouter votre première"

    col_head, col_actions = st.columns([3, 3])
    with col_head:
        st.title("Portefeuille")
        st.caption(subtitle)
    with col_actions:
        st.markdown("<div style='padding-top:8px;'></div>", unsafe_allow_html=True)
        col_a1, col_a2, col_a3 = st.columns(3)
        with col_a1:
            show_import = st.button("Importer relevé", use_container_width=True,
                                      key="pf_btn_import")
        with col_a2:
            show_cash = st.button("Ajouter cash", use_container_width=True,
                                    key="pf_btn_cash")
        with col_a3:
            show_add = st.button("Ajouter position", type="primary",
                                   use_container_width=True, key="pf_btn_add")

    # ─── Panneau Ajouter (s'ouvre quand bouton cliqué ou portfolio vide) ──
    if show_add or st.session_state.get("pf_add_open"):
        st.session_state["pf_add_open"] = True
        with st.container():
            st.markdown(
                "<div style='background:var(--bg-elev);border:1px solid var(--border);"
                "border-radius:10px;padding:14px 16px;margin:10px 0;'>",
                unsafe_allow_html=True,
            )
            tickers_data = load_tickers()
            options = [f"{t['ticker']} - {t['name']}" for t in tickers_data]
            with st.form("add_position"):
                col1, col2, col3, col4 = st.columns(4)
                selection = col1.selectbox("Titre", options)
                quantity = col2.number_input("Quantité", min_value=1, value=10)
                avg_price = col3.number_input(f"PRU ({CURRENCY})", min_value=1, value=1000)
                purchase_date = col4.date_input("Date d'achat")
                notes = st.text_input("Notes (optionnel)")
                col_s, col_c = st.columns(2)
                submitted = col_s.form_submit_button("Enregistrer", type="primary",
                                                       use_container_width=True)
                if col_c.form_submit_button("Annuler", use_container_width=True):
                    st.session_state["pf_add_open"] = False
                    st.rerun()
                if submitted:
                    ticker = selection.split(" - ")[0]
                    name = selection.split(" - ")[1] if " - " in selection else ""
                    save_position(ticker, name, quantity, avg_price, str(purchase_date), notes)
                    st.session_state["pf_add_open"] = False
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    # ─── Panneau Cash (ajouter / ajuster liquidités) ──
    if show_cash or st.session_state.get("pf_cash_open"):
        st.session_state["pf_cash_open"] = True
        current_cash = st.session_state.get("portfolio_cash", 0) or 0
        st.markdown(
            "<div style='background:var(--bg-elev);border:1px solid var(--border);"
            "border-radius:10px;padding:14px 16px;margin:10px 0;'>"
            "<div style='font-size:14px;font-weight:600;margin-bottom:4px;'>Cash disponible</div>"
            "<div style='font-size:12.5px;color:var(--ink-3);margin-bottom:10px;'>"
            f"Solde courant : {current_cash:,.0f} {CURRENCY}. Ajoutez un montant (positif pour "
            "dépôt, négatif pour retrait) ou saisissez un solde total."
            "</div>",
            unsafe_allow_html=True,
        )
        with st.form("cash_form"):
            col1, col2, col3 = st.columns([2, 2, 2])
            mode = col1.radio(
                "Mode", ["Ajouter / retirer", "Définir le solde"],
                horizontal=False, label_visibility="collapsed",
                key="pf_cash_mode",
            )
            if mode == "Ajouter / retirer":
                delta = col2.number_input(
                    f"Montant ({CURRENCY})", value=0, step=1000, key="pf_cash_delta",
                )
                new_total = float(current_cash) + float(delta)
                col3.metric("Nouveau solde", f"{new_total:,.0f} {CURRENCY}")
            else:
                new_total = col2.number_input(
                    f"Solde total ({CURRENCY})", min_value=0,
                    value=int(current_cash), step=1000, key="pf_cash_set",
                )
                col3.metric("Nouveau solde", f"{new_total:,.0f} {CURRENCY}")
            col_s, col_c = st.columns(2)
            submitted_cash = col_s.form_submit_button(
                "Enregistrer", type="primary", use_container_width=True,
            )
            if col_c.form_submit_button("Annuler", use_container_width=True):
                st.session_state["pf_cash_open"] = False
                st.rerun()
            if submitted_cash:
                set_portfolio_cash(float(new_total))
                st.session_state.portfolio_cash = float(new_total)
                st.session_state["pf_cash_open"] = False
                st.success(f"Cash mis à jour : {new_total:,.0f} {CURRENCY}")
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ─── Panneau Import OCR (si bouton cliqué + OCR disponible) ──
    if show_import or st.session_state.get("pf_import_open"):
        st.session_state["pf_import_open"] = True
        if _ocr_available():
            st.markdown(
                "<div style='background:var(--bg-elev);border:1px solid var(--border);"
                "border-radius:10px;padding:14px 16px;margin:10px 0;'>"
                "<div style='font-size:14px;font-weight:600;margin-bottom:8px;'>Importer depuis un screenshot SGI</div>"
                "<div style='font-size:12.5px;color:var(--ink-3);margin-bottom:10px;'>"
                "Max 1 Mo. Format PNG/JPG. La photo est supprimée après OCR."
                "</div>",
                unsafe_allow_html=True,
            )
            _uploader_nonce = st.session_state.get("sgi_uploader_nonce", 0)
            screenshot = st.file_uploader(
                "Screenshot", type=["png", "jpg", "jpeg"],
                key=f"sgi_screenshot_{_uploader_nonce}",
                label_visibility="collapsed",
            )
            if screenshot is not None:
                MAX_BYTES = 1 * 1024 * 1024
                size = getattr(screenshot, "size", None) or len(screenshot.getvalue())
                if size > MAX_BYTES:
                    st.error(f"Image trop lourde ({size/1024:.0f} Ko). Max : 1 Mo.")
                else:
                    with st.spinner("Analyse OCR…"):
                        extracted = _extract_portfolio_from_image(screenshot)
                    st.session_state["sgi_uploader_nonce"] = _uploader_nonce + 1
                    if extracted:
                        st.success(f"{len(extracted)} position(s) détectée(s)")
                        _render_extracted_positions(extracted, load_tickers())
                    else:
                        st.warning("Aucune position détectée. Saisie manuelle :")
                        _render_batch_input(load_tickers())
            if st.button("Fermer", key="pf_import_close"):
                st.session_state["pf_import_open"] = False
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("OCR non disponible sur cette instance.")

    if portfolio.empty:
        st.info("Aucune position en portefeuille. Cliquez sur **Ajouter position** en haut.")
        return

    # --- Portfolio summary (pas de divider — la hiérarchie suffit) ---

    # Try to get current prices : 1) from DB market_data (fast, always there),
    # 2) fallback to live fetch only if DB is empty or very stale.
    price_map = {}
    try:
        from data.storage import get_connection
        conn = get_connection()
        md_rows = conn.execute(
            "SELECT ticker, price FROM market_data WHERE price > 0"
        ).fetchall()
        conn.close()
        price_map = {r[0]: r[1] for r in md_rows}
    except Exception:
        price_map = {}

    # If DB is empty, try a live scrape as last resort
    if not price_map:
        try:
            quotes = fetch_daily_quotes()
            price_map = dict(zip(quotes["ticker"], quotes["last"]))
        except Exception:
            price_map = {}

    # Enrich portfolio with current prices
    portfolio["current_price"] = portfolio["ticker"].map(price_map)
    portfolio["invested"] = portfolio["quantity"] * portfolio["avg_price"]
    portfolio["current_value"] = portfolio.apply(
        lambda r: r["quantity"] * r["current_price"] if pd.notna(r["current_price"]) else r["invested"],
        axis=1,
    )
    portfolio["pnl"] = portfolio["current_value"] - portfolio["invested"]
    portfolio["pnl_pct"] = portfolio["pnl"] / portfolio["invested"] * 100

    # ═══════════════════════════════════════════════════════════════════
    # 4 KPI cards : Valeur totale / P&L cumulé / Yield pondéré / Positions
    # ═══════════════════════════════════════════════════════════════════
    cash = st.session_state.portfolio_cash
    total_invested = portfolio["invested"].sum()
    total_value = portfolio["current_value"].sum()
    total_pnl = total_value - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    total_portfolio = total_value + cash

    _stocks_dict = _load_all_stocks_dict()
    total_div = 0
    for _, pos in portfolio.iterrows():
        fund = _stocks_dict.get(pos["ticker"])
        if fund and fund.get("dps"):
            total_div += fund["dps"] * pos["quantity"]
    yield_weighted = (total_div / total_value * 100) if total_value > 0 else 0

    # Nombre de secteurs
    tickers_data = load_tickers()
    ticker_to_sector = {t["ticker"]: t["sector"] for t in tickers_data}
    portfolio["sector"] = portfolio["ticker"].map(ticker_to_sector).fillna("Autre")
    n_sectors = portfolio["sector"].nunique()

    # Yield marché de référence (approx 4.1% pour BRVM)
    MARKET_YIELD_REF = 4.1

    def _kpi_card(label, value, sub, tone="neutral"):
        arrow = {"up": "▲", "down": "▼"}.get(tone, "")
        sub_color = {"up": "var(--up)", "down": "var(--down)"}.get(tone, "var(--ink-3)")
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;min-height:92px;'>"
            f"<div class='label-xs' style='margin-bottom:6px;'>{label}</div>"
            f"<div style='font-size:22px;font-weight:600;letter-spacing:-0.02em;"
            f"color:var(--ink);font-variant-numeric:tabular-nums;line-height:1.15;'>{value}</div>"
            f"<div style='font-size:11.5px;color:{sub_color};margin-top:6px;font-weight:500;'>"
            f"{arrow + ' ' if arrow else '— '}{sub}</div>"
            f"</div>"
        )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(_kpi_card("Valeur totale", f"{total_portfolio:,.0f}", CURRENCY),
                     unsafe_allow_html=True)
    with c2:
        pnl_sign = "−" if total_pnl < 0 else "+"
        pnl_str = f"{pnl_sign}{abs(total_pnl):,.0f}"
        pnl_sub = f"{'−' if total_pnl_pct < 0 else '+'}{abs(total_pnl_pct):.2f}%"
        pnl_tone = "up" if total_pnl >= 0 else "down"
        st.markdown(_kpi_card("P&L cumulé", pnl_str, pnl_sub, pnl_tone),
                     unsafe_allow_html=True)
    with c3:
        yield_tone = "up" if yield_weighted >= MARKET_YIELD_REF else "down"
        yield_sub = f"vs {MARKET_YIELD_REF:.1f}% marché"
        st.markdown(_kpi_card("Yield pondéré", f"{yield_weighted:.2f}%", yield_sub, yield_tone),
                     unsafe_allow_html=True)
    with c4:
        st.markdown(_kpi_card("Positions", str(len(portfolio)),
                                f"{n_sectors} secteur{'s' if n_sectors > 1 else ''}"),
                     unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════
    # Tableau Positions editorial
    # ═══════════════════════════════════════════════════════════════════
    section_heading("Positions", spacing="loose")

    header_style = (
        "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
        "color:var(--ink-3);font-weight:500;padding:9px 10px;"
        "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
    )
    cell_style = "padding:10px;font-size:13px;border-bottom:1px solid var(--border);"
    num_style = cell_style + "text-align:right;font-variant-numeric:tabular-nums;"

    rows_html = (
        f"<tr>"
        f"<th style='{header_style};text-align:left;'>Ticker</th>"
        f"<th style='{header_style};text-align:left;'>Nom</th>"
        f"<th style='{header_style};text-align:right;'>Qté</th>"
        f"<th style='{header_style};text-align:right;'>PRU</th>"
        f"<th style='{header_style};text-align:right;'>Cours</th>"
        f"<th style='{header_style};text-align:right;'>P&L</th>"
        f"<th style='{header_style};text-align:right;'>Poids</th>"
        f"<th style='{header_style};text-align:right;'>Yield</th>"
        f"</tr>"
    )
    for _, pos in portfolio.iterrows():
        poids_pct = (pos["current_value"] / total_value * 100) if total_value else 0
        fund = _stocks_dict.get(pos["ticker"]) or {}
        dps = fund.get("dps") or 0
        cur_price = pos.get("current_price")
        yield_pos = (dps / cur_price * 100) if cur_price else 0
        pnl = pos.get("pnl")
        cur_str = f"{cur_price:,.0f}" if pd.notna(cur_price) else "—"
        if pd.notna(cur_price) and pnl is not None:
            pnl_sign = "−" if pnl < 0 else "+"
            pnl_str = f"{pnl_sign}{abs(pnl):,.0f}"
            pnl_color = "var(--up)" if pnl >= 0 else "var(--down)"
        else:
            pnl_str = "—"
            pnl_color = "var(--ink-3)"

        rows_html += (
            f"<tr>"
            f"<td style='{cell_style}'><span class='ticker'>{pos['ticker']}</span></td>"
            f"<td style='{cell_style};font-weight:500;'>{pos['company_name']}</td>"
            f"<td style='{num_style}'>{pos['quantity']:,.0f}</td>"
            f"<td style='{num_style}'>{pos['avg_price']:,.0f}</td>"
            f"<td style='{num_style}'>{cur_str}</td>"
            f"<td style='{num_style};color:{pnl_color};font-weight:600;'>{pnl_str}</td>"
            f"<td style='{num_style}'>{poids_pct:.1f}%</td>"
            f"<td style='{num_style}'>{yield_pos:.2f}%</td>"
            f"</tr>"
        )

    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:10px;"
        f"overflow:hidden;background:var(--bg-elev);margin-bottom:16px;'>"
        f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
        unsafe_allow_html=True,
    )

    # Actions par ligne : Ouvrir + Supprimer sous le tableau (pour chaque pos)
    with st.expander("Actions par position", expanded=False):
        for _, pos in portfolio.iterrows():
            cols = st.columns([3, 1, 1])
            cols[0].markdown(f"**{pos['company_name']}** · {pos['ticker']}")
            with cols[1]:
                ticker_analyze_button(
                    pos["ticker"],
                    key=f"pf_goto_{pos['id']}",
                    help_text=f"Analyser {pos['ticker']}",
                    use_container_width=True,
                )
            if cols[2].button("Supprimer", key=f"del_{pos['id']}",
                                use_container_width=True):
                delete_position(pos["id"])
                st.rerun()

    # Allocation
    section_heading("Allocation", spacing="loose")
    col_pie1, col_pie2 = st.columns(2)

    with col_pie1:
        st.markdown(
            "<div class='label-xs' style='margin-bottom:6px;'>Par titre</div>",
            unsafe_allow_html=True,
        )
        labels = portfolio["company_name"].tolist()
        values = portfolio["current_value"].tolist()
        if cash > 0:
            labels.append("Cash")
            values.append(cash)
        fig = pie_chart(labels, values, "Allocation par titre")
        st.plotly_chart(fig, use_container_width=True)

    with col_pie2:
        st.markdown(
            "<div class='label-xs' style='margin-bottom:6px;'>Par secteur</div>",
            unsafe_allow_html=True,
        )
        tickers_data = load_tickers()
        ticker_to_sector = {t["ticker"]: t["sector"] for t in tickers_data}
        portfolio["sector"] = portfolio["ticker"].map(ticker_to_sector).fillna("Autre")
        sector_alloc = portfolio.groupby("sector")["current_value"].sum()
        sec_labels = sector_alloc.index.tolist()
        sec_values = sector_alloc.values.tolist()
        if cash > 0:
            sec_labels.append("Cash")
            sec_values.append(cash)
        fig = pie_chart(sec_labels, sec_values, "Allocation sectorielle")
        st.plotly_chart(fig, use_container_width=True)

    # Sections suivantes (pas de divider — hiérarchie portée par les titres)
    _render_portfolio_analysis(portfolio, cash, total_value, total_portfolio, ticker_to_sector)
    _render_position_recommendations(portfolio, total_value, cash)
    if cash > 0:
        _render_cash_recommendations(portfolio, cash, total_portfolio, ticker_to_sector)
    _render_info_box()


def _render_portfolio_analysis(portfolio, cash, total_value, total_portfolio, ticker_to_sector):
    """Analyse l'équilibre du portefeuille et identifie les points d'attention."""
    from utils.ui_helpers import section_heading, kpi_card
    section_heading("Analyse d'équilibre", spacing="loose")

    if total_portfolio <= 0:
        return

    diagnostics = []

    # 1. Ratio cash / portefeuille total
    cash_pct = (cash / total_portfolio * 100) if total_portfolio > 0 else 0

    # 2. Concentration sectorielle
    portfolio_sectors = portfolio.copy()
    portfolio_sectors["sector"] = portfolio_sectors["ticker"].map(ticker_to_sector).fillna("Autre")
    sector_alloc = portfolio_sectors.groupby("sector")["current_value"].sum()
    sector_pcts = (sector_alloc / total_value * 100).sort_values(ascending=False)
    nb_sectors = len(sector_pcts)
    top_sector = sector_pcts.index[0] if not sector_pcts.empty else ""
    top_sector_pct = sector_pcts.iloc[0] if not sector_pcts.empty else 0

    # 3. Concentration par titre
    title_pcts = (portfolio.groupby("ticker")["current_value"].sum() / total_value * 100).sort_values(ascending=False)
    nb_titres = len(title_pcts)
    top_ticker_pct = title_pcts.iloc[0] if not title_pcts.empty else 0
    top_ticker = title_pcts.index[0] if not title_pcts.empty else ""

    # Tone helpers pour KPIs
    cash_tone = "down" if (cash_pct > 50 or cash_pct == 0) else ("ocre" if (cash_pct > 30 or cash_pct < 10) else "up")
    sect_tone = "down" if nb_sectors == 1 else ("ocre" if top_sector_pct > 70 else "up")
    titr_tone = "down" if top_ticker_pct > 50 else ("ocre" if top_ticker_pct > 35 else "up")

    col1, col2, col3 = st.columns(3)
    with col1:
        kpi_card("Ratio Cash", f"{cash_pct:.1f}", unit="%",
                 sub="Part liquidités / total", tone=cash_tone)
    with col2:
        kpi_card("Secteurs", str(nb_sectors),
                 sub=(f"Top · {top_sector} {top_sector_pct:.0f}%" if top_sector else "—"),
                 tone=sect_tone)
    with col3:
        kpi_card("Titres", str(nb_titres),
                 sub=(f"Top · {top_ticker} {top_ticker_pct:.0f}%" if top_ticker else "—"),
                 tone=titr_tone)

    # ─── Construction des diagnostics (status: ok | warn | risk) ───
    if cash_pct > 50:
        diagnostics.append(("risk", "Cash très élevé", f"{cash_pct:.0f}% du portefeuille en liquidités. Capital sous-utilisé."))
    elif cash_pct > 30:
        diagnostics.append(("warn", "Cash élevé", f"{cash_pct:.0f}% en liquidités. Opportunité d'investissement."))
    elif cash_pct > 10:
        diagnostics.append(("ok", "Cash correct", f"{cash_pct:.0f}% en liquidités. Réserve de sécurité."))
    elif cash_pct > 0:
        diagnostics.append(("warn", "Cash faible", f"Seulement {cash_pct:.0f}% en liquidités. Peu de marge."))
    else:
        diagnostics.append(("risk", "Pas de cash", "Aucune liquidité disponible."))

    if nb_sectors == 1:
        diagnostics.append(("risk", "Mono-secteur", f"100% dans {top_sector}. Aucune diversification."))
    elif top_sector_pct > 70:
        diagnostics.append(("warn", "Forte concentration sectorielle", f"{top_sector_pct:.0f}% dans {top_sector}."))
    elif nb_sectors >= 3:
        diagnostics.append(("ok", "Bonne diversification sectorielle", f"{nb_sectors} secteurs · principal {top_sector} ({top_sector_pct:.0f}%)."))
    else:
        diagnostics.append(("warn", "Diversification moyenne", f"{nb_sectors} secteurs seulement."))

    if top_ticker_pct > 50:
        diagnostics.append(("risk", "Forte concentration titre", f"{top_ticker} = {top_ticker_pct:.0f}% du portefeuille."))
    elif top_ticker_pct > 35:
        diagnostics.append(("warn", "Concentration modérée", f"{top_ticker} pèse {top_ticker_pct:.0f}%."))
    else:
        diagnostics.append(("ok", "Bonne répartition par titre", f"Titre principal {top_ticker} à {top_ticker_pct:.0f}%."))

    # 4. Analyse fondamentale des positions — lecture depuis snapshots
    # (plus de get_fundamentals + compute_hybrid_score par ticker)
    stocks_dict = _load_all_stocks_dict()
    scoring_dict = _load_scoring_dict()
    total_div = 0
    positions_analysis = []
    for ticker in portfolio["ticker"].unique():
        fund = stocks_dict.get(ticker)
        pos_value = portfolio[portfolio["ticker"] == ticker]["current_value"].sum()
        weight = pos_value / total_value * 100 if total_value > 0 else 0
        if not fund:
            continue

        dps = fund.get("dps") or 0
        qty = portfolio[portfolio["ticker"] == ticker]["quantity"].sum()
        div = dps * qty
        total_div += div

        snap = scoring_dict.get(ticker)
        if snap:
            positions_analysis.append({
                "ticker": ticker,
                "weight": weight,
                "score": snap.get("hybrid_score") or 0,
                "verdict": snap.get("verdict") or "—",
                "div_contribution": div,
            })

    # Positions sous-performantes
    weak = [p for p in positions_analysis if p["score"] < 40]
    if weak:
        tickers_weak = ", ".join(f"{p['ticker']} ({p['verdict']})" for p in weak)
        diagnostics.append(("warn", "Positions fragiles", f"Score faible : {tickers_weak}"))

    # 5. Rendement dividende du portefeuille
    if total_div > 0 and total_value > 0:
        pf_yield = total_div / total_value * 100
        if pf_yield >= 5:
            diagnostics.append(("ok", "Bon rendement dividende", f"Rendement portefeuille : {pf_yield:.1f}%"))
        elif pf_yield >= 3:
            diagnostics.append(("ok", "Rendement correct", f"Rendement portefeuille : {pf_yield:.1f}%"))
        else:
            diagnostics.append(("warn", "Rendement faible", f"{pf_yield:.1f}% · envisager titres mieux rémunérés."))

    # ─── Affichage éditorial des diagnostics ───
    section_heading("Diagnostic", spacing="default")
    dot_tone_map = {"ok": "up", "warn": "ocre", "risk": "down"}
    rows = []
    for status, label, detail in diagnostics:
        tone = dot_tone_map.get(status, "neutral")
        rows.append(
            f"<tr>"
            f"<td style='padding:10px 12px;border-bottom:1px solid var(--border);width:18px;'>"
            f"<span class='dot {tone}'></span></td>"
            f"<td style='padding:10px 12px;border-bottom:1px solid var(--border);"
            f"font-weight:600;font-size:13px;white-space:nowrap;'>{label}</td>"
            f"<td style='padding:10px 12px;border-bottom:1px solid var(--border);"
            f"color:var(--ink-2);font-size:13px;'>{detail}</td>"
            f"</tr>"
        )
    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:10px;"
        f"overflow:hidden;background:var(--bg-elev);margin-bottom:14px;'>"
        f"<table style='width:100%;border-collapse:collapse;'>{''.join(rows)}</table></div>",
        unsafe_allow_html=True,
    )


def _render_position_recommendations(portfolio, total_value, cash):
    """Recommandation globale du portefeuille : action synthétique, priorités
    d'achat/vente (basées sur les signaux consolidés), usage du cash, diversification."""
    from config import load_tickers

    section_heading("Recommandation globale", spacing="loose")
    st.markdown(
        "<div style='font-size:13px;color:var(--ink-3);margin-bottom:10px;'>"
        "Synthèse combinant les signaux consolidés et l'état du portefeuille "
        "(P&L, concentration, cash).</div>",
        unsafe_allow_html=True,
    )

    # ---- 1. Scanner TOUS les titres via scoring_snapshot (1 requete) ----
    # Etait une boucle N+1 de ~48 tickers × 3 requetes = ~150 round-trips
    # Supabase = 20+ sec. Maintenant lecture cachee 5 min.
    tickers_meta = load_tickers()
    held_tickers = set(portfolio["ticker"].unique())
    scoring_dict_all = _load_scoring_dict()
    stocks_dict_all = _load_all_stocks_dict()

    scans = []
    for t in tickers_meta:
        ticker = t["ticker"]
        snap = scoring_dict_all.get(ticker)
        fund = stocks_dict_all.get(ticker)
        if not snap or not fund:
            continue
        # Reconstitue le dict result que consomment les blocs suivants
        result = {
            "hybrid_score": snap.get("hybrid_score"),
            "fundamental_score": snap.get("fundamental_score"),
            "technical_score": snap.get("technical_score"),
            "recommendation": {
                "verdict": snap.get("verdict"),
                "stars": snap.get("stars"),
            },
            "trend": {"trend": snap.get("trend")},
            "signals": snap.get("_signals") or [],
        }
        cons = snap.get("_consolidated") or {}

        is_held = ticker in held_tickers
        weight = 0.0
        pnl_pct = 0.0
        if is_held:
            pos = portfolio[portfolio["ticker"] == ticker].iloc[0]
            weight = (pos["current_value"] / total_value * 100) if total_value else 0
            pnl_pct = pos.get("pnl_pct", 0) or 0

        scans.append({
            "ticker": ticker,
            "name": t.get("name", ticker),
            "sector": t.get("sector", ""),
            "price": fund.get("price") or 0,
            "verdict": cons["verdict"],
            "confidence": cons.get("confidence", 0),
            "icon": cons["icon"],
            "net_score": cons["consolidated_signals"]["net_score"],
            "buy_score": cons["consolidated_signals"]["buy_score"],
            "sell_score": cons["consolidated_signals"]["sell_score"],
            "dps": fund.get("dps") or 0,
            "is_held": is_held,
            "weight": weight,
            "pnl_pct": pnl_pct,
            "conflict": cons.get("conflict", False),
            "signals_top": _top_signals(cons["consolidated_signals"]),
        })

    if not scans:
        st.info("Pas de données pour générer une recommandation.")
        return

    held_scans = [s for s in scans if s["is_held"]]
    unheld_scans = [s for s in scans if not s["is_held"]]

    # ---- 2. Ventes prioritaires (positions à alléger / vendre) ----
    sells = sorted(
        [s for s in held_scans if s["verdict"] in ("VENTE FORTE CONFIRMÉE", "VENTE")
         or s["net_score"] <= -4],
        key=lambda s: (s["verdict"] != "VENTE FORTE CONFIRMÉE", -s["weight"]),
    )

    # ---- 3. Achats prioritaires ----
    # Renforcement : titres détenus avec signal achat fort et poids < 35%
    reinforce = sorted(
        [s for s in held_scans
         if s["verdict"] in ("ACHAT FORT CONFIRMÉ", "ACHAT")
         and s["weight"] < 35 and not s["conflict"]],
        key=lambda s: (s["verdict"] != "ACHAT FORT CONFIRMÉ", -s["confidence"]),
    )
    # Nouvelles positions : titres non détenus avec signal achat fort
    new_buys = sorted(
        [s for s in unheld_scans
         if s["verdict"] in ("ACHAT FORT CONFIRMÉ", "ACHAT")
         and not s["conflict"] and s["confidence"] >= 55],
        key=lambda s: (s["verdict"] != "ACHAT FORT CONFIRMÉ", -s["confidence"]),
    )

    # ---- 4. Analyse diversification ----
    cash_pct = (cash / (total_value + cash) * 100) if (total_value + cash) > 0 else 0
    sectors_held = {}
    for s in held_scans:
        sec = s["sector"] or "Autre"
        sectors_held[sec] = sectors_held.get(sec, 0) + s["weight"]
    nb_sectors = len(sectors_held)
    top_sector = max(sectors_held.items(), key=lambda x: x[1]) if sectors_held else ("—", 0)

    nb_titres = len(held_scans)
    top_ticker = max(held_scans, key=lambda s: s["weight"]) if held_scans else None

    # ---- 5. Action synthétique globale ----
    nb_conflicts = sum(1 for s in scans if s["conflict"])
    global_action, global_icon, global_color = _compute_global_action(
        nb_sells=len(sells), nb_reinforce=len(reinforce),
        nb_new_buys=len(new_buys), cash_pct=cash_pct,
        nb_sectors=nb_sectors, top_weight=(top_ticker["weight"] if top_ticker else 0),
    )

    # ---- Carte verdict éditoriale ----
    st.markdown(
        f"<div style='border:1px solid var(--border);border-left:4px solid {global_color};"
        f"border-radius:10px;padding:16px 18px;background:var(--bg-elev);margin-bottom:18px;'>"
        f"<div class='label-xs' style='margin-bottom:4px;'>Verdict portefeuille</div>"
        f"<div style='font-size:17px;font-weight:600;color:var(--ink);"
        f"letter-spacing:-0.01em;'>{global_action}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ---- 3 colonnes : Ventes / Renforcer / Nouveaux achats ----
    col_sell, col_reinforce, col_new = st.columns(3)

    with col_sell:
        section_heading("À vendre / alléger", spacing="default")
        if sells:
            from utils.ui_helpers import tag as _tag, ticker as _tkr, delta as _delta
            for s in sells[:5]:
                _action = "VENDRE" if s["verdict"] == "VENTE FORTE CONFIRMÉE" else "ALLÉGER"
                col_info, col_btn = st.columns([5, 1])
                with col_info:
                    st.markdown(
                        f"{_tag(_action, 'down')} {s['name']} {_tkr(s['ticker'])}<br>"
                        f"<small class='muted'>Poids {s['weight']:.1f}% · "
                        f"P&L {_delta(s['pnl_pct'], with_arrow=False)} · "
                        f"Confiance {s['confidence']}%</small>",
                        unsafe_allow_html=True,
                    )
                    if s["signals_top"]:
                        st.caption(f"↳ {s['signals_top']}")
                with col_btn:
                    ticker_analyze_button(
                        s["ticker"], label=None,
                        key=f"reco_sell_{s['ticker']}",
                    )
                st.markdown("")
        else:
            st.markdown(
                "<div style='color:var(--ink-3);font-size:13px;padding:6px 0;'>"
                "Aucune vente recommandée.</div>",
                unsafe_allow_html=True,
            )

    with col_reinforce:
        section_heading("À renforcer (détenus)", spacing="default")
        if reinforce:
            from utils.ui_helpers import tag as _tag, ticker as _tkr, delta as _delta
            for s in reinforce[:5]:
                label = "ACHAT FORT" if s["verdict"] == "ACHAT FORT CONFIRMÉ" else "ACHAT"
                col_info, col_btn = st.columns([5, 1])
                with col_info:
                    st.markdown(
                        f"{_tag(label, 'up')} {s['name']} {_tkr(s['ticker'])}<br>"
                        f"<small class='muted'>Poids actuel {s['weight']:.1f}% · "
                        f"P&L {_delta(s['pnl_pct'], with_arrow=False)} · "
                        f"Confiance {s['confidence']}%</small>",
                        unsafe_allow_html=True,
                    )
                    if s["signals_top"]:
                        st.caption(f"↳ {s['signals_top']}")
                with col_btn:
                    ticker_analyze_button(
                        s["ticker"], label=None,
                        key=f"reco_reinforce_{s['ticker']}",
                    )
                st.markdown("")
        else:
            st.markdown(
                "<div style='color:var(--ink-3);font-size:13px;padding:6px 0;'>"
                "Pas de position à renforcer.</div>",
                unsafe_allow_html=True,
            )

    with col_new:
        section_heading("Nouvelles opportunités", spacing="default")
        if new_buys:
            from utils.ui_helpers import tag as _tag, ticker as _tkr
            for s in new_buys[:5]:
                label = "ACHAT FORT" if s["verdict"] == "ACHAT FORT CONFIRMÉ" else "ACHAT"
                yield_pct = (s['dps']/s['price']*100) if s['price'] else 0
                col_info, col_btn = st.columns([5, 1])
                with col_info:
                    st.markdown(
                        f"{_tag(label, 'up')} {s['name']} {_tkr(s['ticker'])}<br>"
                        f"<small class='muted'>Prix "
                        f"<span style='font-variant-numeric:tabular-nums'>{s['price']:,.0f}</span> · "
                        f"Yield {yield_pct:.1f}% · Confiance {s['confidence']}%</small>",
                        unsafe_allow_html=True,
                    )
                    if s["signals_top"]:
                        st.caption(f"↳ {s['signals_top']}")
                with col_btn:
                    ticker_analyze_button(
                        s["ticker"], label=None,
                        key=f"reco_new_{s['ticker']}",
                    )
                st.markdown("")
        else:
            st.markdown(
                "<div style='color:var(--ink-3);font-size:13px;padding:6px 0;'>"
                "Pas d'opportunité majeure détectée.</div>",
                unsafe_allow_html=True,
            )

    # ---- Recommandations cash et diversification ----
    col_cash, col_div = st.columns(2)

    with col_cash:
        section_heading("Utilisation du cash", spacing="loose")
        _render_cash_suggestion(cash, cash_pct, new_buys, reinforce)

    with col_div:
        section_heading("Diversification", spacing="loose")
        _render_diversification_suggestion(
            nb_sectors, top_sector, nb_titres,
            top_ticker, unheld_scans, sectors_held,
        )


def _top_signals(signals_cons: dict, limit: int = 2) -> str:
    """Retourne un résumé court des top signaux (mix buy/sell), sans emoji."""
    parts = []
    for s in signals_cons["buy"][:limit]:
        parts.append(f"↑ {s.get('family', '?')} · {s['signal']}")
    for s in signals_cons["sell"][:limit]:
        parts.append(f"↓ {s.get('family', '?')} · {s['signal']}")
    return " | ".join(parts)


def _compute_global_action(nb_sells, nb_reinforce, nb_new_buys,
                           cash_pct, nb_sectors, top_weight):
    """Calcule un libellé d'action globale pour le portefeuille.
    Retourne (texte, icône_legacy, couleur_v3).
    Les couleurs utilisent les tokens du design v3 (CSS vars non exploitables ici :
    on renvoie les couleurs équivalentes hardcoded)."""
    # Tokens v3 : primary=#1F5D3A, terracotta=#C94C2A, ocre=#C99A3B, ink-3=#8A8275
    issues = []
    if nb_sells >= 2:
        issues.append(f"{nb_sells} positions à réduire")
    if top_weight >= 50:
        issues.append("concentration excessive sur 1 titre")
    if nb_sectors <= 1:
        issues.append("mono-sectoriel")

    if issues:
        return (
            "Action urgente — " + ", ".join(issues),
            "", "#C94C2A",
        )

    if nb_sells >= 1 and cash_pct < 10:
        return (
            "Rotation recommandée — vendre d'abord, puis réinvestir",
            "", "#C99A3B",
        )

    if cash_pct > 30 and (nb_reinforce + nb_new_buys) >= 2:
        return (
            f"Déployer le cash ({cash_pct:.0f}% disponible) sur les opportunités identifiées",
            "", "#1F5D3A",
        )

    if cash_pct > 30:
        return (
            f"Conserver le cash ({cash_pct:.0f}%) en attendant de meilleures entrées",
            "", "#C99A3B",
        )

    if nb_reinforce >= 1 or nb_new_buys >= 1:
        return (
            "Portefeuille sain — quelques ajustements d'opportunité possibles",
            "", "#1F5D3A",
        )

    return (
        "Portefeuille équilibré — rester en position, surveiller les signaux",
        "", "#8A8275",
    )


def _render_cash_suggestion(cash, cash_pct, new_buys, reinforce):
    """Affiche une suggestion concrète d'utilisation du cash."""
    if cash <= 0:
        st.info("Pas de cash disponible.")
        return

    candidates = (reinforce or []) + (new_buys or [])
    candidates = [c for c in candidates if c["price"] > 0]

    if not candidates:
        st.warning(
            f"Cash disponible : **{cash:,.0f} {CURRENCY}** — aucune opportunité forte "
            "identifiée pour le moment. Patience recommandée."
        )
        return

    # Répartir le cash proportionnellement à la confiance sur top 3 candidats
    top3 = candidates[:3]
    weights = [c["confidence"] for c in top3]
    total_w = sum(weights) or 1
    allocations = []
    for c, w in zip(top3, weights):
        budget = cash * (w / total_w)
        nb_shares = int(budget // c["price"]) if c["price"] > 0 else 0
        actual_cost = nb_shares * c["price"]
        allocations.append({
            "ticker": c["ticker"], "name": c["name"],
            "pct": (w / total_w) * 100,
            "budget": actual_cost, "nb_shares": nb_shares,
            "price": c["price"],
        })

    st.caption(
        f"Allocation suggérée des **{cash:,.0f} {CURRENCY}** pondérée par la confiance :"
    )
    for a in allocations:
        if a["nb_shares"] > 0:
            col_a, col_b = st.columns([6, 1])
            with col_a:
                st.markdown(
                    f"- **{a['name']}** ({a['ticker']}) : "
                    f"{a['pct']:.0f}% → **{a['nb_shares']} titres** "
                    f"à {a['price']:,.0f} = {a['budget']:,.0f} {CURRENCY}"
                )
            with col_b:
                ticker_analyze_button(
                    a["ticker"], label=None,
                    key=f"cash_alloc_{a['ticker']}",
                )
    total_used = sum(a["budget"] for a in allocations)
    remaining = cash - total_used
    if remaining > 0:
        st.caption(f"Cash résiduel : {remaining:,.0f} {CURRENCY} ({remaining/cash*100:.0f}%)")


def _render_diversification_suggestion(nb_sectors, top_sector, nb_titres,
                                         top_ticker, unheld_scans, sectors_held):
    """Affiche des suggestions de diversification."""
    from config import load_tickers
    if nb_titres == 0:
        st.info("Portefeuille vide.")
        return

    from utils.ui_helpers import flag_dot as _flag
    msgs = []
    top_sec_name, top_sec_weight = top_sector

    if nb_sectors == 1:
        msgs.append(f"{_flag('risk')} — **Mono-sectoriel** : 100% sur {top_sec_name}. Ajouter d'autres secteurs.")
    elif top_sec_weight > 70:
        msgs.append(f"{_flag('warn')} — **Forte concentration** : {top_sec_weight:.0f}% sur {top_sec_name}.")
    elif nb_sectors >= 3:
        msgs.append(f"{_flag('ok')} — **Diversification OK** : {nb_sectors} secteurs.")

    if top_ticker and top_ticker["weight"] > 40:
        msgs.append(
            f"{_flag('warn')} — **{top_ticker['ticker']}** pèse {top_ticker['weight']:.0f}% — "
            "envisager d'alléger ou de renforcer les autres."
        )

    # Suggérer un secteur manquant avec de bonnes opportunités
    tickers_meta = load_tickers()
    all_sectors = {t["sector"] for t in tickers_meta if t.get("sector")}
    missing_sectors = all_sectors - set(sectors_held.keys())

    best_candidate = None
    if missing_sectors and unheld_scans:
        for scan in unheld_scans:
            if scan["sector"] in missing_sectors and scan["verdict"] in (
                "ACHAT FORT CONFIRMÉ", "ACHAT"
            ):
                if not best_candidate or scan["confidence"] > best_candidate["confidence"]:
                    best_candidate = scan

    if not msgs and not best_candidate:
        st.markdown(
            "<div style='color:var(--ink-3);font-size:13px;padding:6px 0;'>"
            "Diversification satisfaisante.</div>",
            unsafe_allow_html=True,
        )
    else:
        for m in msgs:
            st.markdown(m, unsafe_allow_html=True)
        if best_candidate:
            col_msg, col_btn = st.columns([5, 1])
            with col_msg:
                st.markdown(
                    f"Pour diversifier : **{best_candidate['name']}** "
                    f"({best_candidate['ticker']}) secteur _{best_candidate['sector']}_ "
                    f"— {best_candidate['verdict']}"
                )
            with col_btn:
                ticker_analyze_button(
                    best_candidate["ticker"], label=None,
                    key=f"div_candidate_{best_candidate['ticker']}",
                )


def _render_cash_recommendations(portfolio, cash, total_portfolio, ticker_to_sector):
    """Zone de chat intelligent pour recommandations d'investissement."""
    from analysis.llm_chat import chat

    section_heading("Conseiller d'investissement", spacing="loose")
    st.caption(
        f"Cash disponible : **{cash:,.0f} {CURRENCY}**. "
        "Décrivez vos préférences et l'assistant analysera toutes les données disponibles."
    )

    # Initialize chat history
    if "pf_chat_history" not in st.session_state:
        st.session_state.pf_chat_history = []

    # Quick suggestion buttons — only shown when chat is empty
    if not st.session_state.pf_chat_history:
        st.markdown("**Suggestions rapides :**")
        cols = st.columns(4)
        suggestions = [
            "Titres les plus sûrs avec faible risque",
            "Meilleur rendement dividende",
            "Diversifier hors secteur bancaire",
            "Renforcer mes meilleures positions",
        ]
        for i, sug in enumerate(suggestions):
            if cols[i].button(f"💡 {sug}", key=f"sug_{i}"):
                # Stash the pending prompt so it's picked up after rerun
                st.session_state["pf_pending_prompt"] = sug
                st.rerun()

    # Display chat history
    for msg in st.session_state.pf_chat_history:
        with st.chat_message(msg["role"], avatar="🧑‍💼" if msg["role"] == "user" else "📊"):
            st.markdown(msg["content"])

    # Chat input
    user_input = st.chat_input(
        "Ex: Je veux des titres sûrs avec peu de risque d'effondrement...",
        key="pf_chat_input",
    )

    # Pick up prompt from either chat_input or a suggestion button
    pending = st.session_state.pop("pf_pending_prompt", None)
    prompt = user_input or pending

    if prompt:
        st.session_state.pf_chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="🧑‍💼"):
            st.markdown(prompt)

        with st.chat_message("assistant", avatar="📊"):
            with st.spinner("Analyse en cours..."):
                response = chat(
                    query=prompt,
                    mode="portfolio",
                    chat_history=st.session_state.pf_chat_history[:-1],
                )
            st.markdown(response)

        st.session_state.pf_chat_history.append({"role": "assistant", "content": response})


def _render_info_box():
    """Boîte de dialogue pour information générale sur le portefeuille."""
    section_heading("Notes & informations", spacing="loose")
    st.markdown("Utilisez cet espace pour noter vos observations, stratégie ou informations de marché.")

    if "portfolio_notes" not in st.session_state:
        st.session_state.portfolio_notes = ""

    notes = st.text_area(
        "Vos notes de portefeuille",
        value=st.session_state.portfolio_notes,
        height=150,
        placeholder="Ex: Attendre la publication des résultats annuels de Sonatel avant de renforcer.\n"
                    "Objectif : atteindre 40% de rendement dividende global.\n"
                    "Surveiller le secteur bancaire pour opportunités après correction...",
        key="portfolio_notes_input",
    )
    st.session_state.portfolio_notes = notes

    # Quick info cards
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            "**📌 Rappels importants**\n"
            "- Les dividendes BRVM sont généralement versés en **mai-juin**\n"
            "- Les publications annuelles sont attendues en **mars-avril**\n"
            "- Le marché est ouvert du **lundi au vendredi, 9h-15h30 GMT**"
        )
    with col2:
        st.markdown(
            "**📏 Règles de gestion recommandées**\n"
            "- Garder **10-20%** en liquidités pour les opportunités\n"
            "- Ne pas dépasser **30%** sur un seul titre\n"
            "- Diversifier sur **3+ secteurs** minimum\n"
            "- Réévaluer les positions chaque trimestre"
        )


@st.cache_resource
def _get_ocr_reader():
    """Cache le reader easyocr pour éviter de recharger le modèle à chaque appel."""
    import easyocr
    return easyocr.Reader(["fr", "en"], gpu=False, verbose=False)


KNOWN_STOCKS = {
    "ECOBANK": "ECOC.ci", "ECOBANK CI": "ECOC.ci",
    "SONATEL": "SNTS.sn", "SONATEL SN": "SNTS.sn",
    "NSIA BANQUE": "NSBC.ci", "NSIA BANQUE CI": "NSBC.ci", "NSIA BQ": "NSBC.ci",
    "SGB": "SGBC.ci", "SGB CI": "SGBC.ci", "SGBCI": "SGBC.ci",
    "ORANGE CI": "ORAC.ci", "ORANGE": "ORAC.ci",
    "TOTAL CI": "TTLC.ci", "TOTALENERGIES": "TTLC.ci",
    "BOA CI": "BOAC.ci", "BOA BENIN": "BOAB.bj",
    "BOA BF": "BOABF.bf", "BOA MALI": "BOAM.ml",
    "BOA NIGER": "BOAN.ne", "BOA SENEGAL": "BOAS.sn",
    "CORIS BANK": "CBIBF.bf",
    "SOLIBRA": "SLBC.ci", "SICABLE": "CABC.ci",
    "FILTISAC": "FTSC.ci", "PALMCI": "PALC.ci",
    "SAPH": "SPHC.ci", "SITAB": "STBC.ci",
    "BERNABE": "BNBC.ci", "CFAO CI": "CFAC.ci",
    "TRACTAFRIC": "PRSC.ci", "SERVAIR": "APTS.ci",
    "SICOR": "SICC.ci", "CROWN SIEM": "SIMC.ci",
    "ONTBF": "ONTBF.bf", "ONATEL BF": "ONTBF.bf", "ONATEL": "ONTBF.bf",
    "CIE": "CIEC.ci", "SODECI": "SDCC.ci",
    "SETAO": "STAC.ci", "MOVIS": "MVSC.ci",
    "NEI CEDA": "NEIC.ci", "VIVO ENERGY": "SHEC.ci",
    "BOLLORE": "SDSC.ci", "UNILEVER": "UNLC.ci",
    "NESTLE": "NTLC.ci", "SODE CI": "SDCC.ci",
    "ETI": "ETIT.tg", "ORAGROUP": "ORGT.tg",
}


def _match_stock_name(text: str) -> tuple:
    """Match text against known BRVM stock names. Returns (ticker, matched_name) or (None, None)."""
    text_upper = text.upper().strip()
    # Try longest matches first to avoid partial matches (e.g., "BOA CI" vs "BOA")
    for name_pattern in sorted(KNOWN_STOCKS.keys(), key=len, reverse=True):
        if name_pattern.upper() in text_upper:
            return KNOWN_STOCKS[name_pattern], name_pattern
    return None, None


def _extract_portfolio_from_image(uploaded_file) -> list:
    """
    Extrait les positions d'un screenshot de portefeuille SGI via OCR.
    Essaie easyocr d'abord, puis pytesseract en fallback.
    Retourne une liste de dicts: [{titre, qte, cmp, cours}, ...].

    Si aucune librairie OCR n'est installée sur le déploiement (ex. Streamlit
    Cloud free tier, où easyocr+torch est trop lourd), on retourne [] sans
    spammer la sidebar d'erreurs d'imports. Un seul message propre est
    affiché par l'appelant (_render_add_position_form).
    """
    import io
    from PIL import Image

    image_bytes = uploaded_file.getvalue()
    image = Image.open(io.BytesIO(image_bytes))

    # Method 1 : easyocr (best, with positions)
    try:
        reader = _get_ocr_reader()
    except ImportError:
        reader = None
    except Exception:
        reader = None

    if reader is not None:
        try:
            ocr_results = reader.readtext(image_bytes, paragraph=False)
            if ocr_results:
                positions = _parse_ocr_results(ocr_results)
                if positions:
                    return positions
        except Exception:
            pass

    # Method 2 : pytesseract (text only)
    try:
        import pytesseract
    except ImportError:
        pytesseract = None

    if pytesseract is not None:
        try:
            gray = image.convert("L")
            w, h = gray.size
            if w < 1500:
                scale = 1500 / w
                gray = gray.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
            raw_text = pytesseract.image_to_string(gray, lang="fra+eng", config="--psm 6")
            if raw_text and raw_text.strip():
                parsed = _parse_text_lines(raw_text)
                if parsed:
                    return parsed
        except Exception:
            pass

    return []


def _ocr_available() -> bool:
    """True si au moins une librairie OCR est installée."""
    try:
        import easyocr  # noqa: F401
        return True
    except ImportError:
        pass
    try:
        import pytesseract  # noqa: F401
        return True
    except ImportError:
        pass
    return False


def _parse_ocr_results(results: list) -> list:
    """Parse easyocr results (with bounding box positions) into portfolio positions."""
    import re

    # Sort by vertical position, then horizontal
    results.sort(key=lambda r: (r[0][0][1], r[0][0][0]))
    texts = [(r[1].strip(), r[0][0][0], r[0][0][1]) for r in results if r[1].strip()]

    # Group into rows by Y position
    rows = []
    current_row = []
    last_y = -100
    for text, x, y in texts:
        if abs(y - last_y) > 15:
            if current_row:
                rows.append(current_row)
            current_row = [(text, x)]
            last_y = y
        else:
            current_row.append((text, x))
    if current_row:
        rows.append(current_row)

    positions = []
    for row in rows:
        row.sort(key=lambda r: r[1])
        row_text = " ".join([t for t, _ in row])
        row_texts = [t for t, _ in row]

        ticker, matched_name = _match_stock_name(row_text)
        if not ticker:
            continue

        numbers = _extract_numbers(row_texts)
        pos = _classify_numbers(numbers, matched_name, ticker)
        if pos:
            positions.append(pos)

    return positions


def _parse_text_lines(raw_text: str) -> list:
    """Parse raw OCR text (line by line) into portfolio positions."""
    import re

    positions = []
    for line in raw_text.split("\n"):
        line = line.strip()
        if not line:
            continue

        ticker, matched_name = _match_stock_name(line)
        if not ticker:
            continue

        # Extract all tokens and find numbers
        tokens = line.split()
        numbers = _extract_numbers(tokens)
        pos = _classify_numbers(numbers, matched_name, ticker)
        if pos:
            positions.append(pos)

    return positions


def _extract_numbers(tokens: list) -> list:
    """Extract numeric values from a list of text tokens."""
    import re
    numbers = []
    for t in tokens:
        cleaned = re.sub(r'[\s\xa0]', '', t)
        cleaned = cleaned.replace(',', '.').replace('O', '0').replace('o', '0')
        # Remove currency symbols and common OCR artifacts
        cleaned = re.sub(r'[FCFA€$%]', '', cleaned, flags=re.IGNORECASE)
        try:
            val = float(cleaned)
            numbers.append(val)
        except ValueError:
            # Try extracting embedded number
            num_match = re.search(r'[\d]+[\s\d]*[\d]+|[\d]+', t.replace('\xa0', ''))
            if num_match:
                try:
                    val = float(num_match.group().replace(' ', ''))
                    if val > 0:
                        numbers.append(val)
                except ValueError:
                    pass
    return numbers


def _classify_numbers(numbers: list, matched_name: str, ticker: str) -> dict:
    """Classify extracted numbers into quantity, CMP, cours for a portfolio position."""
    if not numbers:
        return None

    # Heuristic: quantity < 500, prices >= 1000
    small_nums = [n for n in numbers if 0 < n < 500]
    large_nums = [n for n in numbers if n >= 1000]

    qte = int(small_nums[0]) if small_nums else None
    cmp = int(large_nums[0]) if len(large_nums) >= 1 else 0
    cours = int(large_nums[1]) if len(large_nums) >= 2 else 0

    if qte and qte > 0:
        return {
            "titre": matched_name,
            "ticker": ticker,
            "qte": qte,
            "cmp": cmp,
            "cours": cours,
        }
    return None


def _render_extracted_positions(extracted: list, tickers_data: list):
    """Affiche les positions extraites et permet de les valider/corriger avant import."""
    ticker_options = {f"{t['ticker']} - {t['name']}": t["ticker"] for t in tickers_data}
    option_list = list(ticker_options.keys())

    st.markdown("**Vérifiez et corrigez les positions détectées :**")

    with st.form("validate_ocr_import"):
        validated = []
        for i, pos in enumerate(extracted):
            col1, col2, col3, col4 = st.columns([3, 1, 1.5, 1.5])

            # Find matching option
            default_idx = 0
            for j, opt in enumerate(option_list):
                if pos["ticker"] in opt:
                    default_idx = j
                    break

            ticker_sel = col1.selectbox(
                f"Titre {i+1}", option_list, index=default_idx,
                key=f"ocr_ticker_{i}",
            )
            qte = col2.number_input(
                "Qté", min_value=0, value=pos["qte"],
                key=f"ocr_qty_{i}",
            )
            cmp = col3.number_input(
                "PRU (CMP)", min_value=0, value=pos["cmp"],
                key=f"ocr_cmp_{i}",
            )
            cours = col4.number_input(
                "Cours actuel", min_value=0, value=pos["cours"],
                key=f"ocr_cours_{i}",
            )
            if qte > 0 and cmp > 0:
                validated.append((ticker_sel, qte, cmp))

        if st.form_submit_button("✅ Valider et importer"):
            if validated:
                for sel, qty, pru in validated:
                    ticker = sel.split(" - ")[0]
                    name = sel.split(" - ")[1] if " - " in sel else ""
                    save_position(ticker, name, qty, pru)
                st.success(f"✅ {len(validated)} position(s) importée(s) !")
                st.rerun()
            else:
                st.warning("Aucune position valide à importer")


def _render_batch_input(tickers_data):
    """Formulaire de saisie en lot pour import screenshot SGI."""
    options = [""] + [f"{t['ticker']} - {t['name']}" for t in tickers_data]
    nb_lines = st.number_input("Nombre de lignes à saisir", min_value=1, max_value=20, value=3, key="batch_lines")

    with st.form("batch_import"):
        positions = []
        for i in range(nb_lines):
            col1, col2, col3 = st.columns([3, 1, 1])
            ticker_sel = col1.selectbox(f"Titre {i+1}", options, key=f"batch_ticker_{i}")
            qty = col2.number_input("Qte", min_value=0, value=0, key=f"batch_qty_{i}")
            pru = col3.number_input("PRU (FCFA)", min_value=0, value=0, key=f"batch_pru_{i}")
            if ticker_sel and qty > 0 and pru > 0:
                positions.append((ticker_sel, qty, pru))

        if st.form_submit_button("💾 Importer toutes les positions"):
            if positions:
                for sel, qty, pru in positions:
                    ticker = sel.split(" - ")[0]
                    name = sel.split(" - ")[1] if " - " in sel else ""
                    save_position(ticker, name, qty, pru)
                st.success(f"✅ {len(positions)} position(s) importée(s) !")
                st.rerun()
            else:
                st.warning("Aucune position valide à importer")
