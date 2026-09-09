"""
Page 6 : Suivi Portefeuille
Gestion des positions et performance.
"""

import streamlit as st
import pandas as pd

from config import load_tickers, CURRENCY
from data.storage import (
    save_position, get_portfolio, delete_position, update_position,
    get_fundamentals, get_cached_prices, get_all_stocks_for_analysis,
    get_portfolio_cash, set_portfolio_cash,
    get_dividends, save_dividend, delete_dividend,
    get_total_dividends_received,
    save_account_fee, get_account_fees, delete_account_fee,
    get_total_account_fees, ACCOUNT_FEE_CATEGORIES,
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

    # Lecture systématique du cash depuis la DB à chaque render (pas de cache
    # session_state). Évite les divergences entre l'affichage et la valeur
    # persistée — le cache ne nous gagne rien (1 SELECT trivial).
    current_cash_db = get_portfolio_cash()
    st.session_state.portfolio_cash = current_cash_db

    # Flash de confirmation après save cash (survit au st.rerun)
    flash = st.session_state.pop("pf_cash_flash", None)
    if flash:
        st.success(flash)

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
            show_import = st.button("Saisir en lot", use_container_width=True,
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
                "border-radius:12px;padding:14px 16px;margin:10px 0;'>",
                unsafe_allow_html=True,
            )
            tickers_data = load_tickers()
            options = [f"{t['ticker']} - {t['name']}" for t in tickers_data]
            with st.form("add_position"):
                col1, col2, col3 = st.columns([2, 1, 1])
                selection = col1.selectbox("Titre", options)
                quantity = col2.number_input("Quantité", min_value=1, value=10)
                purchase_date = col3.date_input("Date d'achat")
                col4, col5 = st.columns(2)
                avg_price_str = col4.text_input(
                    f"Cours d'exécution ({CURRENCY})", value="1000",
                    help="Prix par action, hors frais. Virgule ou point acceptés "
                         "(ex : 16320 ou 16320,00).",
                )
                fees_str = col5.text_input(
                    f"Frais totaux de l'achat ({CURRENCY})", value="0",
                    help="Somme des frais liés à CET achat : commission + BRVM/DC-BR + TVA. "
                         "Lisez-les sur votre bordereau SGI. Laissez 0 si inconnus.",
                )
                notes = st.text_input("Notes (optionnel)")
                col_s, col_c = st.columns(2)
                submitted = col_s.form_submit_button("Enregistrer", type="primary",
                                                       use_container_width=True)
                if col_c.form_submit_button("Annuler", use_container_width=True):
                    st.session_state["pf_add_open"] = False
                    st.rerun()
                if submitted:
                    def _parse_amount(s):
                        return float(
                            (s or "0").replace(" ", "").replace(" ", "")
                            .replace(",", ".")
                        )
                    try:
                        avg_price = _parse_amount(avg_price_str)
                        if avg_price <= 0:
                            raise ValueError("Cours doit être > 0")
                    except (ValueError, AttributeError):
                        st.error(
                            f"Cours invalide : « {avg_price_str} ». "
                            f"Exemples : 16320 · 16320,00 · 16320.00"
                        )
                    else:
                        try:
                            fees = _parse_amount(fees_str)
                            if fees < 0:
                                raise ValueError("Frais négatifs interdits")
                        except (ValueError, AttributeError):
                            st.error(
                                f"Frais invalides : « {fees_str} ». Mettez 0 si inconnus."
                            )
                        else:
                            ticker = selection.split(" - ")[0]
                            name = selection.split(" - ")[1] if " - " in selection else ""
                            save_position(ticker, name, quantity, avg_price,
                                          str(purchase_date), notes, fees=fees)
                            st.session_state["pf_add_open"] = False
                            st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    # ─── Panneau Cash (ajouter / ajuster liquidités) ──
    if show_cash or st.session_state.get("pf_cash_open"):
        st.session_state["pf_cash_open"] = True
        current_cash = st.session_state.get("portfolio_cash", 0) or 0
        st.markdown(
            "<div style='background:var(--bg-elev);border:1px solid var(--border);"
            "border-radius:12px;padding:14px 16px;margin:10px 0;'>"
            "<div style='font-size:14px;font-weight:600;margin-bottom:4px;'>Cash disponible</div>"
            "<div style='font-size:12.5px;color:var(--ink-3);margin-bottom:10px;'>"
            f"Solde courant : {current_cash:,.0f} {CURRENCY}. Ajoutez un montant (positif pour "
            "dépôt, négatif pour retrait) ou saisissez un solde total."
            "</div>",
            unsafe_allow_html=True,
        )
        # Radio HORS du form : un widget dans st.form() n'enregistre sa valeur
        # qu'au submit, donc changer le mode ne re-rendrait pas le bon input.
        # En le sortant, le clic sur le radio déclenche un rerun et le form
        # re-rend avec la bonne branche if/else.
        mode = st.radio(
            "Mode", ["Ajouter / retirer", "Définir le solde"],
            horizontal=True, key="pf_cash_mode",
        )
        with st.form("cash_form"):
            col_input, col_preview = st.columns([3, 2])
            if mode == "Ajouter / retirer":
                delta = col_input.number_input(
                    f"Montant ({CURRENCY})", value=0, step=1000, key="pf_cash_delta",
                    help="Positif = dépôt, négatif = retrait",
                )
                new_total = float(current_cash) + float(delta)
            else:
                new_total = col_input.number_input(
                    f"Solde total ({CURRENCY})", min_value=0,
                    value=int(current_cash), step=1000, key="pf_cash_set",
                )
            col_preview.metric("Nouveau solde", f"{new_total:,.0f} {CURRENCY}")
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
                # Message persistant via session_state : st.success() est perdu
                # à cause du st.rerun() qui suit. On l'affiche après rerun.
                st.session_state["pf_cash_flash"] = (
                    f"Cash mis à jour : {new_total:,.0f} {CURRENCY}"
                )
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ─── Panneau de saisie en lot ────────────────────────────────────
    # L'import se faisait par capture d'ecran, lue par tesseract. Le moteur ne
    # peut plus etre installe sur l'instance : son paquet systeme passe par un
    # `apt-get` que le constructeur de Streamlit Cloud fait echouer. Offrir une
    # fonction qui echoue au moment de s'en servir vaut moins que ne pas
    # l'offrir — la saisie en lot, elle, a toujours marche.
    if show_import or st.session_state.get("pf_import_open"):
        st.session_state["pf_import_open"] = True
        st.markdown(
            "<div style='background:var(--bg-elev);border:1px solid "
            "var(--border);border-radius:12px;padding:14px 16px;margin:10px 0;'>"
            "<div style='font-size:14px;font-weight:600;margin-bottom:8px;'>"
            "Saisir plusieurs positions</div>"
            "<div style='font-size:12.5px;color:var(--ink-3);"
            "margin-bottom:10px;'>Reprenez les lignes de votre relevé SGI : "
            "titre, quantité, prix de revient.</div>",
            unsafe_allow_html=True,
        )
        _render_batch_input(load_tickers())
        if st.button("Fermer", key="pf_import_close"):
            st.session_state["pf_import_open"] = False
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if portfolio.empty:
        st.info("Aucune position en portefeuille. Cliquez sur **Ajouter position** en haut.")
        return

    # ─── Trois onglets : où j'en suis, que faire, quoi ajouter ───────
    # La page reunissait positions, dividendes, frais, allocation, risque,
    # equilibre, diagnostic, ventes, renforcements, opportunites et cash —
    # seize cents lignes pour trois questions differentes. Des ONGLETS
    # plutot que trois entrees de menu : la navigation, les liens et
    # l'authentification ne bougent pas, et c'est deja le motif d'« Analyse
    # d'un titre ».
    onglet_perf, onglet_reco, onglet_neuf = st.tabs(
        ["Performance", "Recommandations", "Risque et optimisation"])

    with onglet_perf:
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
        # Le coût de revient inclut désormais les frais de transaction (Type 1)
        if "fees" not in portfolio.columns:
            portfolio["fees"] = 0
        portfolio["fees"] = pd.to_numeric(portfolio["fees"], errors="coerce").fillna(0)
        portfolio["invested"] = (
            portfolio["quantity"] * portfolio["avg_price"] + portfolio["fees"]
        )
        portfolio["current_value"] = portfolio.apply(
            lambda r: r["quantity"] * r["current_price"] if pd.notna(r["current_price"]) else r["invested"],
            axis=1,
        )
        portfolio["pnl"] = portfolio["current_value"] - portfolio["invested"]
        portfolio["pnl_pct"] = portfolio["pnl"] / portfolio["invested"] * 100

        # ═══════════════════════════════════════════════════════════════════
        # 4 KPI cards : Valeur totale / Total Return / Yield / Positions
        # ═══════════════════════════════════════════════════════════════════
        cash = st.session_state.portfolio_cash
        total_invested = portfolio["invested"].sum()
        total_value = portfolio["current_value"].sum()
        total_pnl = total_value - total_invested
        total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
        total_portfolio = total_value + cash

        # Dividendes encaissés + frais de compte + Total Return
        total_dividends_received = get_total_dividends_received()
        total_account_fees = get_total_account_fees()
        total_return = total_pnl + total_dividends_received - total_account_fees
        total_return_pct = (total_return / total_invested * 100) if total_invested > 0 else 0

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
            """Carte au gabarit du canevas v4."""
            arrow = {"up": "▲", "down": "▼"}.get(tone, "")
            accent = {"up": "var(--up)", "down": "var(--down)",
                      "warn": "var(--warn)"}.get(tone, "var(--ink-4)")
            teinte = {"up": "var(--up)", "down": "var(--down)",
                      "warn": "var(--warn)"}.get(tone, "var(--ink-3)")
            poids = 600 if tone in ("up", "down", "warn") else 400
            return (
                f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
                f"border-top:2px solid {accent};border-radius:12px;padding:15px 17px;"
                f"display:flex;flex-direction:column;gap:5px;height:100%;'>"
                f"<span style='font-size:10.5px;font-weight:600;letter-spacing:0.09em;"
                f"text-transform:uppercase;color:var(--ink-3);'>{label}</span>"
                f"<span style='font-variant-numeric:tabular-nums;font-size:27px;"
                f"font-weight:600;letter-spacing:-0.015em;line-height:1.05;"
                f"color:var(--ink);'>{value}</span>"
                + (f"<span style='font-size:11.5px;font-weight:{poids};"
                   f"color:{teinte};'>{arrow + ' ' if arrow else ''}{sub}</span>"
                   if sub else "")
                + "</div>"
            )

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(_kpi_card("Valeur totale",
                                  f"{total_portfolio:,.0f}".replace(",", " "),
                                  CURRENCY),
                         unsafe_allow_html=True)
        with c2:
            ret_sign = "−" if total_return < 0 else "+"
            ret_str = f"{ret_sign}{abs(total_return):,.0f}"
            pnl_sign_txt = "−" if total_pnl < 0 else "+"
            parts = [
                f"{'−' if total_return_pct < 0 else '+'}{abs(total_return_pct):.2f}%",
                f"PV {pnl_sign_txt}{abs(total_pnl):,.0f}",
            ]
            if total_dividends_received > 0:
                parts.append(f"Div +{total_dividends_received:,.0f}")
            if total_account_fees > 0:
                parts.append(f"Frais −{total_account_fees:,.0f}")
            ret_sub = " · ".join(parts)
            ret_tone = "up" if total_return >= 0 else "down"
            st.markdown(_kpi_card("Total Return", ret_str, ret_sub, ret_tone),
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
        # P2 : le canevas dit d'ou vient le PRU. Sans cette phrase, un lecteur
        # qui recalcule le cout de revient a partir du PRU affiche tombe sur
        # un ecart qu'il ne s'explique pas — les frais d'achat y sont deja.
        st.caption(
            "Le **PRU** inclut les frais d'achat : commission de courtage, "
            "frais BRVM et TVA sont incorporés au coût de revient de la ligne. "
            "La **variation** se lit par titre, face au PRU — le P&L, lui, "
            "dépend aussi de la taille de la position."
        )

        header_style = (
            "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
            "color:var(--ink-3);font-weight:500;padding:9px 10px;"
            "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
        )
        cell_style = "padding:10px;font-size:13px;border-bottom:1px solid var(--border-soft);"
        num_style = cell_style + "text-align:right;font-variant-numeric:tabular-nums;"

        # Echelle commune a toutes les lignes : bornee au plus grand ecart
        # observe, pour qu'aucune barre ne sature et que deux lignes se
        # comparent d'un coup d'oeil.
        from utils.ui_helpers import barre_signee
        _ecarts = [((p.get("current_price") - (p.get("avg_price") or 0))
                    / (p.get("avg_price") or 1) * 100)
                   for _, p in portfolio.iterrows()
                   if pd.notna(p.get("current_price")) and (p.get("avg_price") or 0)]
        _echelle_var = max((abs(e) for e in _ecarts), default=10.0) or 10.0

        def _barre_variation(v):
            return barre_signee(v, borne=_echelle_var, mini="80px",
                                legende=(f"{v:+.1f} % vs PRU"
                                         if v is not None else ""))

        rows_html = (
            f"<tr>"
            f"<th style='{header_style};text-align:left;'>Ticker</th>"
            f"<th style='{header_style};text-align:left;'>Nom</th>"
            f"<th style='{header_style};text-align:right;'>Qté</th>"
            f"<th style='{header_style};text-align:right;'>PRU</th>"
            f"<th style='{header_style};text-align:right;'>Cours</th>"
            f"<th style='{header_style};text-align:right;'>P&L</th>"
            f"<th style='{header_style};text-align:center;'>Variation</th>"
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
            # LA VARIATION, PAS SEULEMENT LE GAIN. Le P&L en francs melange
            # deux choses : combien la ligne a bouge, et combien on en detient.
            # Une ligne a +2 % sur une grosse position rapporte plus qu'une
            # ligne a +40 % sur une petite, et la colonne P&L les classe donc
            # a l'envers de leur performance. La barre signee dit la variation
            # par titre, echelle commune a toutes les lignes.
            _pru = pos.get("avg_price") or 0
            _var_pct = (((cur_price - _pru) / _pru * 100)
                        if (pd.notna(cur_price) and _pru) else None)
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
                f"<td style='{num_style}'>{pos['avg_price']:,.2f}</td>"
                f"<td style='{num_style}'>{cur_str}</td>"
                f"<td style='{num_style};color:{pnl_color};font-weight:600;'>{pnl_str}</td>"
                f"<td style='{cell_style};min-width:96px;'>"
                + _barre_variation(_var_pct) +
                f"</td>"
                f"<td style='{num_style}'>{poids_pct:.1f}%</td>"
                f"<td style='{num_style}'>{yield_pos:.2f}%</td>"
                f"</tr>"
            )

        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:12px;"
            f"overflow:hidden;background:var(--bg-elev);margin-bottom:16px;'>"
            f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
            unsafe_allow_html=True,
        )

        # Actions par ligne : Ouvrir / Modifier / Supprimer
        with st.expander("Actions par position", expanded=False):
            for _, pos in portfolio.iterrows():
                pid = int(pos["id"])
                edit_flag_key = f"pf_edit_open_{pid}"

                cols = st.columns([3, 1, 1, 1])
                cols[0].markdown(f"**{pos['company_name']}** · {pos['ticker']}")
                with cols[1]:
                    ticker_analyze_button(
                        pos["ticker"],
                        key=f"pf_goto_{pid}",
                        help_text=f"Analyser {pos['ticker']}",
                        use_container_width=True,
                    )
                if cols[2].button("Modifier", key=f"edit_{pid}",
                                    use_container_width=True):
                    st.session_state[edit_flag_key] = not st.session_state.get(edit_flag_key, False)
                    st.rerun()
                if cols[3].button("Supprimer", key=f"del_{pid}",
                                    use_container_width=True):
                    delete_position(pid)
                    st.rerun()

                # ─── Panneau Modifier (inline sous la ligne) ──
                if st.session_state.get(edit_flag_key):
                    st.markdown(
                        "<div style='background:var(--bg-elev);border:1px solid var(--border);"
                        "border-radius:12px;padding:12px 14px;margin:6px 0 10px 0;'>",
                        unsafe_allow_html=True,
                    )
                    with st.form(f"edit_form_{pid}"):
                        st.markdown(
                            f"<div class='label-xs' style='margin-bottom:8px;'>"
                            f"Modifier {pos['ticker']} — {pos['company_name']}</div>",
                            unsafe_allow_html=True,
                        )
                        c1, c2 = st.columns(2)
                        new_qty = c1.number_input(
                            "Quantité", min_value=1,
                            value=int(pos["quantity"]), step=1,
                            key=f"edit_qty_{pid}",
                        )
                        new_pru = c2.number_input(
                            f"PRU ({CURRENCY})", min_value=0.01,
                            value=float(pos["avg_price"]), step=0.01,
                            format="%.2f", key=f"edit_pru_{pid}",
                        )
                        c_s, c_c = st.columns(2)
                        saved = c_s.form_submit_button(
                            "Enregistrer", type="primary", use_container_width=True,
                        )
                        cancelled = c_c.form_submit_button(
                            "Annuler", use_container_width=True,
                        )
                        if saved:
                            ok = update_position(pid, new_qty, new_pru)
                            if ok:
                                st.session_state[edit_flag_key] = False
                                st.success(f"{pos['ticker']} mis à jour")
                                st.rerun()
                            else:
                                st.error("Échec de la mise à jour.")
                        if cancelled:
                            st.session_state[edit_flag_key] = False
                            st.rerun()
                    st.markdown("</div>", unsafe_allow_html=True)

        # ═══════════════════════════════════════════════════════════════════
        # Section Dividendes à venir — calculés, jamais saisis
        # ═══════════════════════════════════════════════════════════════════
        _render_dividendes_a_venir(portfolio)

        # ═══════════════════════════════════════════════════════════════════
        # Section Dividendes encaissés
        # ═══════════════════════════════════════════════════════════════════
        section_heading("Dividendes encaissés", spacing="loose")

        dividends_df = get_dividends()
        _add_key = "pf_div_add_open"
        if _add_key not in st.session_state:
            st.session_state[_add_key] = False

        hcol, bcol = st.columns([5, 1])
        hcol.caption(
            f"**{len(dividends_df)}** dividende(s) · Total net encaissé : "
            f"**{total_dividends_received:,.0f} {CURRENCY}**"
        )
        if bcol.button("+ Ajouter", key="pf_div_add_btn",
                        use_container_width=True):
            st.session_state[_add_key] = not st.session_state[_add_key]

        # Formulaire d'ajout (repliable)
        if st.session_state[_add_key]:
            st.markdown(
                "<div style='background:var(--bg-elev);border:1px solid var(--border);"
                "border-radius:12px;padding:14px 16px;margin:10px 0;'>",
                unsafe_allow_html=True,
            )
            tickers_data = load_tickers()
            div_options = [f"{t['ticker']} - {t['name']}" for t in tickers_data]
            with st.form("add_dividend"):
                c1, c2, c3, c4 = st.columns([2.5, 1.4, 1.6, 1])
                div_sel = c1.selectbox("Titre", div_options, key="div_ticker")
                div_date = c2.date_input("Date paiement", key="div_date")
                div_net_str = c3.text_input(
                    f"Net reçu ({CURRENCY})", value="0",
                    help="Virgule ou point acceptés (ex. 19536,00)",
                    key="div_net",
                )
                div_fy = c4.number_input(
                    "Exercice", min_value=2015, max_value=2100, value=2025,
                    step=1, key="div_fy",
                )
                div_notes = st.text_input("Notes (optionnel)", key="div_notes")

                csave, ccancel = st.columns(2)
                saved = csave.form_submit_button(
                    "Enregistrer", type="primary", use_container_width=True,
                )
                if ccancel.form_submit_button("Annuler", use_container_width=True):
                    st.session_state[_add_key] = False
                    st.rerun()
                if saved:
                    try:
                        net_val = float(div_net_str.replace(" ", "")
                                          .replace(" ", "")
                                          .replace(",", "."))
                        if net_val <= 0:
                            raise ValueError("Le montant doit être positif")
                    except (ValueError, AttributeError):
                        st.error(
                            f"Montant net invalide : « {div_net_str} ». "
                            f"Exemples : 19536 · 19 536,00 · 19536.00"
                        )
                    else:
                        ticker = div_sel.split(" - ")[0]
                        save_dividend({
                            "ticker": ticker,
                            "payment_date": str(div_date),
                            "net_amount": net_val,
                            "fiscal_year": int(div_fy),
                            "notes": div_notes,
                        })
                        st.session_state[_add_key] = False
                        st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        # Tableau des dividendes
        if dividends_df.empty:
            st.caption("Aucun dividende enregistré. Cliquez « + Ajouter » ci-dessus.")
        else:
            header_style = (
                "font-size:11.5px;color:var(--ink-3);letter-spacing:0.02em;"
                "text-transform:uppercase;padding:6px 8px;border-bottom:1px solid var(--border);"
            )
            num_style = (
                "font-family:var(--font-mono);font-size:14px;padding:8px;"
                "border-bottom:1px solid var(--border-soft);text-align:right;"
            )
            cell_style = (
                "font-size:14px;padding:8px;border-bottom:1px solid var(--border-soft);"
            )
            html = (
                "<table style='width:100%;border-collapse:collapse;"
                "background:var(--bg-elev);border-radius:12px;overflow:hidden;"
                "border:1px solid var(--border);'>"
                "<thead><tr>"
                f"<th style='{header_style};text-align:left;'>Date</th>"
                f"<th style='{header_style};text-align:left;'>Ticker</th>"
                f"<th style='{header_style};text-align:right;'>Net encaissé</th>"
                f"<th style='{header_style};text-align:center;'>Exercice</th>"
                f"<th style='{header_style};text-align:left;'>Notes</th>"
                "</tr></thead><tbody>"
            )
            for _, div in dividends_df.iterrows():
                date_str = str(div.get("payment_date") or "")[:10]
                html += (
                    "<tr>"
                    f"<td style='{cell_style}'>{date_str}</td>"
                    f"<td style='{cell_style}'><code>{div['ticker']}</code></td>"
                    f"<td style='{num_style}'>{float(div['net_amount']):,.0f}</td>"
                    f"<td style='{cell_style};text-align:center;'>{div.get('fiscal_year') or ''}</td>"
                    f"<td style='{cell_style};color:var(--ink-3);font-size:12.5px;'>"
                    f"{(div.get('notes') or '')[:60]}</td>"
                    "</tr>"
                )
            html += "</tbody></table>"
            st.markdown(html, unsafe_allow_html=True)

            # Actions par ligne : bouton supprimer sous le tableau (compact)
            with st.expander("Supprimer un dividende", expanded=False):
                for _, div in dividends_df.iterrows():
                    dc1, dc2, dc3 = st.columns([3, 1.5, 1.5])
                    dc1.markdown(
                        f"`{div['ticker']}` · {str(div.get('payment_date') or '')[:10]} · "
                        f"{float(div['net_amount']):,.0f} {CURRENCY}"
                    )
                    if dc2.button("Supprimer", key=f"div_del_{div['id']}",
                                    use_container_width=True):
                        delete_dividend(int(div["id"]))
                        st.rerun()

        # ═══════════════════════════════════════════════════════════════════
        # Section Frais de compte (Type 2)
        # ═══════════════════════════════════════════════════════════════════
        section_heading("Frais de compte", spacing="loose")

        account_fees_df = get_account_fees()
        _add_fee_key = "pf_fee_add_open"
        if _add_fee_key not in st.session_state:
            st.session_state[_add_fee_key] = False

        fhcol, fbcol = st.columns([5, 1])
        fhcol.caption(
            f"**{len(account_fees_df)}** frais · Total payé : "
            f"**{total_account_fees:,.0f} {CURRENCY}** "
            "(débités automatiquement du cash)"
        )
        if fbcol.button("+ Ajouter", key="pf_fee_add_btn",
                         use_container_width=True):
            st.session_state[_add_fee_key] = not st.session_state[_add_fee_key]

        if st.session_state[_add_fee_key]:
            st.markdown(
                "<div style='background:var(--bg-elev);border:1px solid var(--border);"
                "border-radius:12px;padding:14px 16px;margin:10px 0;'>",
                unsafe_allow_html=True,
            )
            CATEGORY_LABEL = {
                "droits_garde": "Droits de garde",
                "tenue_compte": "Tenue de compte",
                "virement": "Frais de virement",
                "commission_dividende": "Commission sur dividende",
                "conversion": "Conversion de devise",
                "autre": "Autre",
            }
            cat_options = [(c, CATEGORY_LABEL.get(c, c))
                            for c in ACCOUNT_FEE_CATEGORIES]
            with st.form("add_account_fee"):
                fc1, fc2, fc3 = st.columns([1.4, 1.8, 1])
                fee_date = fc1.date_input("Date", key="fee_date")
                fee_cat = fc2.selectbox(
                    "Catégorie",
                    options=[c for c, _ in cat_options],
                    format_func=lambda c: CATEGORY_LABEL.get(c, c),
                    key="fee_cat",
                )
                fee_amount_str = fc3.text_input(
                    f"Montant ({CURRENCY})", value="0",
                    help="Virgule ou point acceptés",
                    key="fee_amount",
                )
                fee_notes = st.text_input("Notes (optionnel)", key="fee_notes")

                cs, cc = st.columns(2)
                f_saved = cs.form_submit_button(
                    "Enregistrer", type="primary", use_container_width=True,
                )
                if cc.form_submit_button("Annuler", use_container_width=True):
                    st.session_state[_add_fee_key] = False
                    st.rerun()
                if f_saved:
                    try:
                        amt = float(fee_amount_str.replace(" ", "")
                                     .replace(" ", "").replace(",", "."))
                        if amt <= 0:
                            raise ValueError("Montant doit être positif")
                    except (ValueError, AttributeError):
                        st.error(
                            f"Montant invalide : « {fee_amount_str} »."
                        )
                    else:
                        save_account_fee({
                            "fee_date": str(fee_date),
                            "category": fee_cat,
                            "amount": amt,
                            "notes": fee_notes,
                        })
                        st.session_state[_add_fee_key] = False
                        st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        if account_fees_df.empty:
            st.caption("Aucun frais de compte enregistré.")
        else:
            CATEGORY_LABEL_TABLE = {
                "droits_garde": "Droits de garde",
                "tenue_compte": "Tenue de compte",
                "virement": "Frais de virement",
                "commission_dividende": "Commission sur dividende",
                "conversion": "Conversion de devise",
                "autre": "Autre",
            }
            header_style = (
                "font-size:11.5px;color:var(--ink-3);letter-spacing:0.02em;"
                "text-transform:uppercase;padding:6px 8px;"
                "border-bottom:1px solid var(--border);"
            )
            num_style = (
                "font-family:var(--font-mono);font-size:14px;padding:8px;"
                "border-bottom:1px solid var(--border-soft);text-align:right;"
            )
            cell_style = (
                "font-size:14px;padding:8px;border-bottom:1px solid var(--border-soft);"
            )
            html = (
                "<table style='width:100%;border-collapse:collapse;"
                "background:var(--bg-elev);border-radius:12px;overflow:hidden;"
                "border:1px solid var(--border);'>"
                "<thead><tr>"
                f"<th style='{header_style};text-align:left;'>Date</th>"
                f"<th style='{header_style};text-align:left;'>Catégorie</th>"
                f"<th style='{header_style};text-align:right;'>Montant</th>"
                f"<th style='{header_style};text-align:left;'>Notes</th>"
                "</tr></thead><tbody>"
            )
            for _, fee in account_fees_df.iterrows():
                date_str = str(fee.get("fee_date") or "")[:10]
                cat_lbl = CATEGORY_LABEL_TABLE.get(fee.get("category"),
                                                    fee.get("category") or "—")
                html += (
                    "<tr>"
                    f"<td style='{cell_style}'>{date_str}</td>"
                    f"<td style='{cell_style}'>{cat_lbl}</td>"
                    f"<td style='{num_style};color:var(--down);'>−{float(fee['amount']):,.0f}</td>"
                    f"<td style='{cell_style};color:var(--ink-3);font-size:12.5px;'>"
                    f"{(fee.get('notes') or '')[:60]}</td>"
                    "</tr>"
                )
            html += "</tbody></table>"
            st.markdown(html, unsafe_allow_html=True)

            with st.expander("Supprimer un frais (recrédite le cash)",
                              expanded=False):
                for _, fee in account_fees_df.iterrows():
                    fc1, fc2 = st.columns([4, 1.5])
                    cat_lbl = CATEGORY_LABEL_TABLE.get(
                        fee.get("category"), fee.get("category") or "—"
                    )
                    fc1.markdown(
                        f"`{cat_lbl}` · {str(fee.get('fee_date') or '')[:10]} · "
                        f"{float(fee['amount']):,.0f} {CURRENCY}"
                    )
                    if fc2.button("Supprimer", key=f"fee_del_{fee['id']}",
                                    use_container_width=True):
                        delete_account_fee(int(fee["id"]))
                        st.rerun()

        # ── Performance mensuelle des lignes ──
        # Le tableau des positions donne le gain DEPUIS l'achat : un chiffre
        # unique, qui ne dit pas si la ligne monte régulièrement ou si elle a
        # tout pris en un mois. La carte de chaleur étale les douze derniers
        # mois — en ligne, la régularité d'un titre ; en colonne, un mois où
        # tout le portefeuille a bougé ensemble.
        try:
            from analysis.risque import series_mensuelles as _series_m
            from utils.ui_helpers import heatmap as _heatmap_p
            _series = _series_m()
        except Exception:                                       # noqa: BLE001
            _series = {}

        if _series:
            _MOIS_COURT = ["janv.", "févr.", "mars", "avr.", "mai", "juin",
                           "juil.", "août", "sept.", "oct.", "nov.", "déc."]
            _FENETRE = 12

            # Les mois affichés sont ceux de la série la plus longue parmi les
            # lignes détenues : une position récente laisse des cases vides à
            # gauche, ce qui est la vérité et non un défaut.
            _par_titre = {}
            for _, _pos in portfolio.iterrows():
                _t = _pos.get("ticker")
                if _t not in _series:
                    continue
                _rend, _points = _series[_t]
                _par_titre[_t] = {
                    (p[0].year, p[0].month): r
                    for p, r in zip(_points[1:], _rend)
                }

            _mois = sorted({m for d in _par_titre.values() for m in d})[-_FENETRE:]
            _lignes_pf = []
            if _mois:
                # Une ligne par TITRE, pas par lot : un portefeuille qui
                # détient trois fois Ecobank à des prix différents n'a qu'une
                # seule série de cours. Trois lignes identiques n'apprenaient
                # rien et faisaient croire à trois sociétés.
                _vus = set()
                for _, _pos in portfolio.iterrows():
                    _t = _pos.get("ticker")
                    if _t not in _par_titre or _t in _vus:
                        continue
                    _vals = [_par_titre[_t].get(m) for m in _mois]
                    if all(v is None for v in _vals):
                        continue
                    _vus.add(_t)
                    _lignes_pf.append((
                        _pos.get("company_name") or _t,
                        [None if v is None else v * 100 for v in _vals],
                    ))

            if _lignes_pf:
                _colonnes = [f"{_MOIS_COURT[m - 1]} {str(a)[2:]}"
                             for a, m in _mois]
                section_heading("Performance mensuelle des lignes",
                                spacing="loose")
                _heatmap_p(
                    _lignes_pf, _colonnes, intitule_colonne="Ligne",
                    footer="Rendement total mensuel, dividende compris et "
                           "compté au mois de son versement réel. Lecture en "
                           "ligne : la régularité d'un titre. En colonne : un "
                           "mois où tout le portefeuille a bougé ensemble. "
                           "Une case vide est un mois sans cotation connue.",
                )

        # ── Allocation : anneaux à figure centrale (canevas v4) ──
        # Le camembert obligeait à survoler chaque part pour en connaître la
        # valeur. L'anneau porte le total en son centre et la légende donne le
        # montant ET le pourcentage : la part et la masse se lisent ensemble.
        from utils.ui_helpers import donut
        section_heading("Allocation", spacing="loose")

        def _mds(v):
            """Un portefeuille se lit en millions, pas en francs."""
            return (f"{v / 1e9:.2f} Md" if abs(v) >= 1e9
                    else f"{v / 1e6:.1f} M" if abs(v) >= 1e6
                    else f"{v:,.0f}".replace(",", " "))

        tickers_data = load_tickers()
        ticker_to_sector = {t["ticker"]: t["sector"] for t in tickers_data}
        portfolio["sector"] = portfolio["ticker"].map(ticker_to_sector).fillna("Autre")

        col_pie1, col_pie2 = st.columns(2)
        with col_pie1:
            st.markdown(
                "<div class='label-xs' style='margin-bottom:6px;'>Par titre</div>",
                unsafe_allow_html=True,
            )
            # « Par titre » veut dire par titre : les lots d'une même société
            # se cumulent, sinon Ecobank apparaissait en trois parts distinctes.
            _par_soc = (portfolio.groupby("company_name")["current_value"]
                        .sum().sort_values(ascending=False))
            _seg = list(zip(_par_soc.index.tolist(), _par_soc.values.tolist()))
            if cash > 0:
                _seg.append(("Cash", cash))
            donut(_seg, _mds(total_portfolio),
                  f"{len(_par_soc)} titres", montant_fmt=_mds)

        with col_pie2:
            st.markdown(
                "<div class='label-xs' style='margin-bottom:6px;'>Par secteur</div>",
                unsafe_allow_html=True,
            )
            sector_alloc = portfolio.groupby("sector")["current_value"].sum()
            _seg_s = list(zip(sector_alloc.index.tolist(),
                              sector_alloc.values.tolist()))
            if cash > 0:
                _seg_s.append(("Cash", cash))
            donut(_seg_s, f"{len(sector_alloc)}", "secteurs",
                  montant_fmt=_mds)


    with onglet_reco:
        # TOUTES les recommandations du modele, sans ajustement : ce qu'il dit
        # de la societe et de la tendance, alleger, renforcer et acheter.
        _render_portfolio_analysis(portfolio, cash, total_value,
                                   total_portfolio, ticker_to_sector)
        _render_position_recommendations(portfolio, total_value, cash,
                                         volet="tout")
        # Le « Conseiller d'investissement » a ete retire le 08/09, sur
        # arbitrage, comme l'assistant de la page Signaux. Les suggestions
        # d'emploi du cash restent, elles, dans les blocs au-dessus, qui les
        # calculent au lieu de les converser.

    with onglet_neuf:
        # Les memes questions, sous l'angle du risque — et elargies a toute la
        # cote : on ne peut pas ameliorer un portefeuille en ne regardant que
        # ce qu'il contient deja.
        _render_risque_ensemble(portfolio)
        _render_recommandations_ajustees(portfolio)
        _render_optimisation(portfolio, cash)

    _render_info_box()


def _render_dividendes_a_venir(portfolio):
    """Ce que le portefeuille va encaisser, et quand.

    Rien n'est saisi ici : le Bulletin Officiel donne le brut par action, la
    date de mise en paiement et les deux taux d'IRVM ; le portefeuille donne
    les quantites. Le net se calcule. C'est la difference avec la section
    voisine, « Dividendes encaisses », qui enregistre ce qui est ARRIVE.
    """
    from utils.ui_helpers import section_heading, kpi_grille
    from analysis.dividendes import dividendes_attendus

    section_heading("Dividendes à venir", spacing="loose")

    profil = st.segmented_control(
        "Régime fiscal", ["physique", "morale"], default="physique",
        format_func=lambda p: f"Personne {p}", key="pf_div_profil",
    ) or "physique"

    try:
        att = dividendes_attendus(portfolio, profil)
    except Exception as e:                                      # noqa: BLE001
        st.caption(f"Calcul indisponible : {e}")
        return

    lignes = att["lignes"]
    if not lignes:
        st.caption(
            "Aucun dividende annoncé sur vos lignes pour l'instant. Cette "
            "section ne montre que les **annonces en cours** du Bulletin "
            "Officiel dont la date de mise en paiement n'est pas passée — "
            "ce qui a déjà été versé est en dessous."
        )
    else:
        prochaine = lignes[0]
        kpi_grille([
            {"label": "Net attendu", "value": f"{att['total_net']:,.0f}",
             "sub": f"{CURRENCY} · {len(lignes)} ligne(s) concernée(s)",
             "accent": "var(--up)"},
            {"label": "Brut annoncé", "value": f"{att['total_brut']:,.0f}",
             "sub": f"avant IRVM ({att['profil']})"},
            {"label": "Retenue IRVM", "value": f"−{att['total_retenue']:,.0f}",
             "sub": "prélevée à la source",
             "accent": "var(--down)", "sub_color": "var(--down)"},
            {"label": "Prochain versement", "value": _date_fr(prochaine["date"]),
             "sub": f"{prochaine['emetteur']} · J+{prochaine['jours']}"},
        ], mini="185px")

        entete = ("font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;"
                  "color:var(--ink-3);font-weight:500;padding:9px 10px;"
                  "border-bottom:1px solid var(--border);background:var(--bg-sunken);")
        cell = "padding:8px 10px;font-size:13px;border-bottom:1px solid var(--border);"
        nb = cell + "text-align:right;font-variant-numeric:tabular-nums;"
        html = (f"<tr><th style='{entete};text-align:left;'>Paiement</th>"
                f"<th style='{entete};text-align:left;'>Titre</th>"
                f"<th style='{entete};text-align:right;'>Quantité</th>"
                f"<th style='{entete};text-align:right;'>Brut / action</th>"
                f"<th style='{entete};text-align:right;'>Brut</th>"
                f"<th style='{entete};text-align:right;'>IRVM</th>"
                f"<th style='{entete};text-align:right;'>Net</th></tr>")
        for l in lignes:
            html += (
                f"<tr><td style='{cell}'>{_date_fr(l['date'])}"
                f"<span style='color:var(--ink-4);font-size:11px;'> · "
                f"J+{l['jours']}</span></td>"
                f"<td style='{cell}'><span class='ticker'>{l['ticker']}</span> "
                f"<span style='color:var(--ink-2);'>{l['emetteur']}</span></td>"
                f"<td style='{nb}'>{l['quantite']:,.0f}</td>"
                f"<td style='{nb}'>{l['brut_action']:,.2f}</td>"
                f"<td style='{nb}'>{l['brut']:,.0f}</td>"
                f"<td style='{nb};color:var(--down);'>−{l['retenue']:,.0f} "
                f"<span style='color:var(--ink-4);font-size:11px;'>"
                f"({l['taux']:.0f} %)</span></td>"
                f"<td style='{nb};font-weight:600;'>{l['net']:,.0f}</td></tr>")
        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:12px;"
            f"overflow:hidden;background:var(--bg-elev);margin-top:6px;'>"
            f"<table style='width:100%;border-collapse:collapse;'>{html}</table>"
            f"</div>", unsafe_allow_html=True)
        st.caption(
            f"Brut par action et taux d'IRVM lus dans le Bulletin Officiel, "
            f"quantités lues dans vos positions. Le net est **calculé** : "
            f"quantité × brut, moins la retenue. Rien n'est recopié."
        )

    # Une annonce sans ticker ne se rattache a aucune ligne. La taire ferait
    # manquer un versement a qui detient le titre.
    if att["orphelines"]:
        noms = ", ".join(f"{o['emetteur']} ({_date_fr(o['date'])})"
                         for o in att["orphelines"])
        st.warning(
            f"Annonce(s) de dividende que le Bulletin ne rattache à aucun "
            f"ticker : **{noms}**. Si vous détenez ce titre, le montant "
            f"ci-dessus ne le compte pas."
        )


def _date_fr(iso: str) -> str:
    """2026-09-16 → 16/09."""
    t = str(iso or "")
    return f"{t[8:10]}/{t[5:7]}" if len(t) >= 10 else t



def _render_risque_ensemble(portfolio):
    """Le risque du portefeuille, qui n'est pas la somme de celui des lignes.

    Deux titres qui ne bougent pas ensemble s'annulent en partie. Sur les
    portefeuilles reels, l'ecart va de 11 a 44 % : additionner les volatilites
    des lignes surestimerait le risque de moitie.

    La CONTRIBUTION AU RISQUE est la mesure qui change le regard. Un titre pese
    un poids, et une part du risque, et les deux different — sur un
    portefeuille reel, Ecobank Transnational pese 17 % et apporte 35 % du
    risque, quand Ecobank Cote d'Ivoire pese 12 % et n'en apporte que 3.
    Aucune mesure titre par titre ne peut le dire : cela ne se voit que dans
    l'ensemble.
    """
    from utils.ui_helpers import section_heading, kpi_grille
    from analysis.risque import formater
    try:
        from analysis.portefeuille_risque import (mesures_portefeuille,
                                                  lecture_portefeuille)
        positions = tuple(sorted(
            (r["ticker"], float(r.get("current_value") or 0))
            for _, r in portfolio.iterrows()))
        p = mesures_portefeuille(positions)
    except Exception as err:                                    # noqa: BLE001
        st.caption(f"Risque d'ensemble indisponible : {err}")
        return
    if not p or not p.get("lignes"):
        return

    section_heading("Risque d'ensemble", spacing="loose")

    phrases = lecture_portefeuille(p)
    if phrases:
        st.markdown(
            "<div style='background:var(--bg-elev);border:1px solid "
            "var(--border);border-left:3px solid var(--ocre);"
            "border-radius:12px;padding:16px 18px;'>"
            + "".join(
                f"<div style='font-size:13.5px;line-height:1.65;"
                f"color:var(--ink-2);margin-bottom:6px;'>· "
                f"{_gras_html(ph)}</div>" for ph in phrases)
            + "</div>", unsafe_allow_html=True)

    _cartes_risque_ensemble(p, formater, kpi_grille)

    entete = ("font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;"
              "color:var(--ink-3);font-weight:500;padding:9px 10px;"
              "border-bottom:1px solid var(--border);background:var(--bg-sunken);")
    cell = "padding:8px 10px;font-size:13px;border-bottom:1px solid var(--border);"
    nb = cell + "text-align:right;font-variant-numeric:tabular-nums;"

    html = (f"<tr><th style='{entete};text-align:left;'>Ligne</th>"
            f"<th style='{entete};text-align:right;'>Poids</th>"
            f"<th style='{entete};text-align:right;'>Part du risque</th>"
            f"<th style='{entete};text-align:right;'>Volatilité</th>"
            f"<th style='{entete};text-align:right;'>Sortie</th></tr>")
    for l in p["lignes"]:
        # Un ecart entre le poids et la part du risque est l'information : le
        # colorer le rend lisible d'un coup d'oeil.
        if l["concentre"]:
            teinte, note = "var(--down)", " concentre"
        elif l["diversifie"]:
            teinte, note = "var(--up)", " diversifie"
        else:
            teinte, note = "var(--ink)", ""
        jours = l["jours_sortie"]
        sortie = ("—" if jours is None else
                  "< 1 séance" if jours < 1 else f"{jours:.0f} séances")
        html += (
            f"<tr><td style='{cell}'><span class='ticker'>{l['ticker']}</span>"
            f"<span style='color:var(--ink-3);font-size:11px;'>{note}</span></td>"
            f"<td style='{nb}'>{l['poids']:.1%}</td>"
            f"<td style='{nb};font-weight:600;color:{teinte};'>"
            f"{l['contribution_risque']:.1%}</td>"
            f"<td style='{nb}'>"
            f"{'—' if l['volatilite'] is None else format(l['volatilite'], '.1%')}</td>"
            f"<td style='{nb};color:var(--ink-3);'>{sortie}</td></tr>")
    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:12px;"
        f"overflow:hidden;background:var(--bg-elev);margin-top:14px;'>"
        f"<table style='width:100%;border-collapse:collapse;'>{html}</table>"
        f"</div>", unsafe_allow_html=True)
    st.caption(
        f"Volatilité mesurée sur **{p['observations']} mois**, dividendes "
        f"compris. Les quatre cartes reconstituent le portefeuille **aux "
        f"poids d'aujourd'hui** sur ces mêmes mois : ce n'est pas "
        f"l'historique du compte, qui a connu d'autres lignes, mais ce que "
        f"le portefeuille tel qu'il est aurait traversé. "
        f"La **part du risque** tient compte des liens entre les "
        f"lignes : elle diffère du poids, et c'est tout l'intérêt. La colonne "
        f"**Sortie** estime le temps de vente au rythme d'échange habituel du "
        f"titre.")


MOIS_COURTS = ("janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.",
               "août", "sept.", "oct.", "nov.", "déc.")


def _cartes_risque_ensemble(p, formater, kpi_grille):
    """Les quatre mesures que porte l'ensemble, et qu'aucune ligne ne donne.

    Le canevas ouvre l'onglet par cette rangee : la volatilite reellement
    subie face a celle qu'on aurait en additionnant les lignes, la
    sensibilite au marche, la pire chute, et ce que tout cela rapporte. Une
    carte dont la donnee manque n'est pas posee — une case vide vaut mieux
    qu'un chiffre fabrique.
    """
    t = p.get("trajectoire") or {}
    cartes = []

    vol = p.get("volatilite")
    if vol is not None:
        somme = p.get("volatilite_sans_diversification")
        gain = p.get("gain_diversification")
        cartes.append(dict(
            label="Volatilité du portefeuille",
            value=formater("volatilite", vol),
            sub=(f"somme des lignes : {formater('volatilite', somme)}"
                 if somme else "une seule ligne mesurable"),
            accent="var(--up)" if (gain or 0) > 0 else "var(--ink-4)"))

    beta = t.get("beta")
    if beta is not None:
        # Un beta dont la correlation est faible est juste et ne veut rien
        # dire : le portefeuille ne suit pas assez le marche pour qu'une
        # sensibilite au marche ait un sens. On le dit plutot que de le taire.
        if not t.get("beta_significatif"):
            sous = (f"lien au marché trop faible "
                    f"(r = {t['correlation_marche']:.2f})"
                    if t.get("correlation_marche") is not None else
                    "lien au marché non mesurable")
        else:
            sous = ("plus calme que la cote" if beta < 0.90 else
                    "plus nerveux que la cote" if beta > 1.10 else
                    "au rythme de la cote")
        cartes.append(dict(label="Bêta agrégé", value=f"{beta:.2f}",
                           sub=sous, accent="var(--ink-4)"))

    pire = t.get("perte_maximale")
    if pire:
        creux = t.get("creux")
        quand = (f"{MOIS_COURTS[creux[1] - 1]} {creux[0]}, " if creux else "")
        recup = t.get("mois_recuperation")
        cartes.append(dict(
            label="Perte maximale simulée",
            value=formater("perte_maximale", pire),
            sub=(quand + (f"effacée en {recup} mois" if recup
                          else "pas encore effacée")),
            accent="var(--ink-4)", couleur_valeur="var(--down)"))

    sharpe = t.get("sharpe")
    if sharpe is not None:
        mediane = t.get("sharpe_median_cote")
        cartes.append(dict(
            label="Rendement par unité de risque",
            value=formater("sharpe", sharpe),
            sub=(f"médiane de la cote : {formater('sharpe', mediane)}"
                 if mediane is not None
                 else "rendement en excès du sans-risque"),
            accent=("var(--up)" if mediane is not None and sharpe > mediane
                    else "var(--ink-4)"),
            taille="22px"))

    kpi_grille(cartes)


def _gras_html(texte):
    """Le gras Markdown en HTML : ces phrases sont rendues dans un bloc HTML."""
    import re as _re
    return _re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", texte)



# Au-dela, les trois premieres lignes font le resultat a elles seules : c'est
# le seuil que le canevas porte sous la carte.
SEUIL_CONCENTRATION_TROIS = 50.0


def _render_portfolio_analysis(portfolio, cash, total_value, total_portfolio, ticker_to_sector):
    """Analyse l'équilibre du portefeuille et identifie les points d'attention."""
    from utils.ui_helpers import section_heading, kpi_grille
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

    # CONCENTRATION DES TROIS PREMIERES LIGNES. Le nombre de titres ne dit rien
    # de l'equilibre : sept lignes dont trois pesent 70 % est un portefeuille
    # de trois titres avec quatre figurants. Au-dela de la moitie, le resultat
    # d'ensemble depend de trois paris.
    top3_pct = title_pcts.head(3).sum() if not title_pcts.empty else 0
    conc_tone = "down" if top3_pct > SEUIL_CONCENTRATION_TROIS else "up"
    conc_sub = (f"seuil de vigilance {SEUIL_CONCENTRATION_TROIS:.0f} %"
                if nb_titres >= 3 else
                f"{nb_titres} ligne{'s' if nb_titres > 1 else ''} en tout")

    _accent = {"up": "var(--up)", "down": "var(--down)",
               "ocre": "var(--ocre)"}
    kpi_grille([
        dict(label="Ratio Cash", value=f"{cash_pct:.1f} %",
             sub="Part liquidités / total",
             accent=_accent.get(cash_tone, "var(--ink-4)")),
        dict(label="Secteurs", value=str(nb_sectors),
             sub=(f"Top · {top_sector} {top_sector_pct:.0f} %"
                  if top_sector else "—"),
             accent=_accent.get(sect_tone, "var(--ink-4)")),
        dict(label="Titres", value=str(nb_titres),
             sub=(f"Top · {top_ticker} {top_ticker_pct:.0f} %"
                  if top_ticker else "—"),
             accent=_accent.get(titr_tone, "var(--ink-4)")),
        dict(label="Concentration 3 lignes", value=f"{top3_pct:.1f} %",
             sub=conc_sub, accent=_accent[conc_tone]),
    ])

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
    # EN CARTES, PAS EN TABLEAU. Un tableau aligne des colonnes et l'oeil y
    # cherche une comparaison qui n'existe pas : ces constats ne se comparent
    # pas entre eux, ils s'additionnent. Le canevas en fait des cartes a filet
    # et pastille, ou chacun se lit pour lui-meme.
    from utils.ui_helpers import cartes_constats
    section_heading("Diagnostic", spacing="default")
    st.markdown(
        f"<div style='font-family:var(--font-mono);font-size:11.5px;"
        f"color:var(--ink-3);margin:-6px 0 8px;'>{len(diagnostics)} constat"
        f"{'s' if len(diagnostics) > 1 else ''}</div>",
        unsafe_allow_html=True)
    _ton = {"ok": "up", "warn": "ocre", "risk": "down"}
    cartes_constats([(_ton.get(statut, "primary"), libelle, detail)
                     for statut, libelle, detail in diagnostics])


def _barre_titres(paires, cle, intitule="Ouvrir l'analyse d'un titre"):
    """UNE barre d'ouverture par onglet, plutot qu'une sous chaque tableau.

    Trois selecteurs empiles sous des colonnes de hauteurs differentes ne
    s'alignent jamais, et chacun ramenait son propre libelle : le regard
    croisait quatre polices pour une seule action. Une barre unique, en bas de
    l'onglet, regroupe tous les titres qui viennent d'etre cites.
    """
    from utils.nav import goto_ticker
    paires = [(t, n) for t, n in dict(paires).items() if t]
    if not paires:
        return
    st.markdown(
        f"<div style='border-top:1px solid var(--border);margin-top:18px;"
        f"padding-top:12px;'><div class='label-xs' style='margin-bottom:6px;'>"
        f"{intitule}</div></div>", unsafe_allow_html=True)
    col_sel, col_btn = st.columns([5, 1])
    with col_sel:
        choix = st.selectbox(
            intitule, options=[f"{t} — {n}" for t, n in paires],
            key=f"{cle}_select", label_visibility="collapsed")
    with col_btn:
        if st.button("Ouvrir", key=f"{cle}_btn", use_container_width=True):
            goto_ticker(choix.split(" — ")[0])



def _table_suggestions(liste, intitule, action, tone, montrer_poids=True):
    """Les suggestions en tableau, comme la page Risque et optimisation.

    Les listes s'affichaient en blocs empiles, chacun avec son bouton
    d'analyse. La page de risque, elle, tabule — et se lit d'un coup d'oeil.
    Aligner les deux evite au lecteur de changer de grammaire visuelle en
    changeant d'onglet. Le bouton par ligne disparait ; un selecteur sous le
    tableau ouvre n'importe lequel des titres cites.
    """
    from utils.ui_helpers import section_heading
    section_heading(intitule, spacing="default")
    if not liste:
        st.markdown("<div style='color:var(--ink-3);font-size:13px;"
                    "padding:6px 0;'>Aucune suggestion.</div>",
                    unsafe_allow_html=True)
        return
    entete = ("font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;"
              "color:var(--ink-3);font-weight:500;padding:9px 10px;"
              "border-bottom:1px solid var(--border);background:var(--bg-sunken);")
    cell = "padding:8px 10px;font-size:13px;border-bottom:1px solid var(--border);"
    nb = cell + "text-align:right;font-variant-numeric:tabular-nums;"
    couleur = {"up": "var(--up)", "down": "var(--down)"}.get(tone, "var(--ink)")
    html = (f"<tr><th style='{entete};text-align:left;'>Titre</th>"
            + (f"<th style='{entete};text-align:right;'>Poids</th>"
               f"<th style='{entete};text-align:right;'>P&L</th>"
               if montrer_poids else "")
            + f"<th style='{entete};text-align:right;'>Confiance</th>"
              f"<th style='{entete};text-align:left;'>Signal</th></tr>")
    for s in liste:
        colonnes = ""
        if montrer_poids:
            pnl = s.get("pnl_pct") or 0
            teinte_pnl = "var(--up)" if pnl >= 0 else "var(--down)"
            colonnes = (f"<td style='{nb}'>{s.get('weight', 0):.1f} %</td>"
                        f"<td style='{nb};color:{teinte_pnl};'>{pnl:+.1f} %</td>")
        html += (
            f"<tr><td style='{cell}'>"
            f"<span class='ticker'>{s['ticker']}</span> "
            f"<span style='color:var(--ink-2);'>{s.get('name', '')}</span></td>"
            + colonnes
            + f"<td style='{nb};font-weight:600;color:{couleur};'>"
              f"{s.get('confidence', 0):.0f} %</td>"
              f"<td style='{cell};color:var(--ink-3);font-size:12px;'>"
              f"{s.get('signals_top') or '—'}</td></tr>")
    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:12px;"
        f"overflow:hidden;background:var(--bg-elev);'>"
        f"<table style='width:100%;border-collapse:collapse;'>{html}</table>"
        f"</div>", unsafe_allow_html=True)




def _render_position_recommendations(portfolio, total_value, cash,
                                     volet="tout"):
    """Les suggestions, par volet.

    `volet` vaut « detenus » — ce qu'il faut faire de ce qu'on a — ou
    « nouveaux » — ce qu'on pourrait ajouter. Les deux repondent a des
    questions differentes et vivent desormais dans deux onglets ; le calcul,
    lui, reste commun, parce qu'il faut connaitre les positions detenues pour
    savoir ce qui manque.
    """
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
    if volet != "nouveaux":
        st.markdown(
        f"<div style='border:1px solid var(--border);border-left:4px solid {global_color};"
        f"border-radius:12px;padding:16px 18px;background:var(--bg-elev);margin-bottom:18px;'>"
        f"<div class='label-xs' style='margin-bottom:4px;'>Verdict portefeuille</div>"
        f"<div style='font-size:17px;font-weight:600;color:var(--ink);"
        f"letter-spacing:-0.01em;'>{global_action}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    st.caption(
        "Ces suggestions pèsent **la société et la tendance**, sans tenir "
        "compte du risque ni de la liquidité. La section suivante montre ce "
        "que l'ajustement y change.")

    # ---- Colonnes, selon le volet demande ----
    if volet == "nouveaux":
        col_sell = col_reinforce = None
        col_new = st.container()
    elif volet == "detenus":
        col_sell, col_reinforce = st.columns(2)
        col_new = None
    else:                                   # « tout » : les trois cotes a cote
        col_sell, col_reinforce, col_new = st.columns(3)

    # ── Ce qu'il faut faire, dans cet ordre ──
    # Trois tableaux côte à côte posent tous la même question implicite : par
    # quoi je commence ? La réponse tient dans un rang. L'ordre n'est pas
    # arbitraire — on libère d'abord, on réemploie ensuite, on élargit enfin.
    from utils.ui_helpers import plan_etapes
    _etapes = []
    if volet != "nouveaux" and sells:
        _poids = sum(x.get("weight", 0) or 0 for x in sells[:5])
        _etapes.append({
            "accent": "var(--down)", "rang_libelle": "libérer", "genre": "vente",
            "action": "Sortir ou alléger les lignes passées à la vente",
            "lignes": [(x["ticker"], x.get("name", ""), "") for x in sells[:5]],
            "motif": "Le verdict est passé au négatif sur ces lignes. Une "
                     "position en perte n'est pas une raison de la garder : "
                     "c'est souvent la raison pour laquelle le verdict a "
                     "basculé.",
            "impact_libelle": "Libère", "impact": f"{_poids:.1f} %",
            "impact_part": min(100, _poids * 2),
            "impact_sub": "du portefeuille",
        })
    if volet != "nouveaux" and reinforce:
        _poids_r = sum(x.get("weight", 0) or 0 for x in reinforce[:5])
        _etapes.append({
            "accent": "var(--up)", "rang_libelle": "réemployer", "genre": "renfort",
            "action": "Renforcer ce qui est déjà détenu et bien noté",
            "lignes": [(x["ticker"], x.get("name", ""), "") for x in reinforce[:5]],
            "motif": "Renforcer coûte moins de frais qu'ouvrir une ligne, et "
                     "ne rajoute pas de société à suivre. C'est la voie la "
                     "moins chère avant d'élargir.",
            "impact_libelle": "Poids actuel", "impact": f"{_poids_r:.1f} %",
            "impact_part": min(100, _poids_r * 2),
            "impact_sub": "avant renforcement",
        })
    if volet != "detenus" and new_buys:
        _etapes.append({
            "accent": "var(--primary)", "rang_libelle": "élargir", "genre": "achat",
            "action": "Ouvrir de nouvelles lignes",
            "lignes": [(x["ticker"], x.get("name", ""), "") for x in new_buys[:5]],
            "motif": "Une ligne de plus est une société de plus à suivre. "
                     "Elle se justifie quand elle apporte un secteur ou un "
                     "profil que le portefeuille n'a pas.",
        })
    if _etapes:
        # L'étiquette suit le RANG réel, pas le genre de l'action : quand il
        # n'y a rien à vendre, la première étape ne doit pas s'annoncer
        # « ensuite ».
        _rangs = ["D'ABORD", "ENSUITE", "ENFIN"]
        for _i, _e in enumerate(_etapes):
            _e["tag"] = _rangs[_i] if _i < len(_rangs) else "PUIS"
        _urgentes = sum(1 for e in _etapes if e.get("genre") == "vente")
        plan_etapes(
            "Ce qu'il faut faire, dans cet ordre", _etapes,
            f"{len(_etapes)} ÉTAPE{'S' if len(_etapes) > 1 else ''}"
            + (f" · {_urgentes} URGENTE" if _urgentes else ""),
        )

    if volet != "nouveaux":
        with col_sell:
            _table_suggestions(sells[:5], "À vendre / alléger",
                               "sell", "down")
    if volet != "nouveaux":
        with col_reinforce:
            _table_suggestions(reinforce[:5], "À renforcer (détenus)",
                               "reinforce", "up")
    if volet != "detenus":
        with col_new:
            _table_suggestions(new_buys[:5], "Nouvelles opportunités",
                               "new", "up", montrer_poids=False)
    if volet == "detenus":
        _barre_titres([(x["ticker"], x.get("name", ""))
                       for x in sells[:5] + reinforce[:5]], "reco_detenus")
        return

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

    _barre_titres([(x["ticker"], x.get("name", ""))
                   for x in (sells[:5] + reinforce[:5] + new_buys[:5]
                             if volet == "tout" else new_buys[:5])],
                  "reco_tout")


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
    # Tokens v3 : primary=#0E7A54, terracotta=#C94C2A, ocre=#C99A3B, ink-3=#8A8275
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
            "", "#0E7A54",
        )

    if cash_pct > 30:
        return (
            f"Conserver le cash ({cash_pct:.0f}%) en attendant de meilleures entrées",
            "", "#C99A3B",
        )

    if nb_reinforce >= 1 or nb_new_buys >= 1:
        return (
            "Portefeuille sain — quelques ajustements d'opportunité possibles",
            "", "#0E7A54",
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
            # Pas de bouton par ligne : la barre d'ouverture en bas d'onglet
            # rassemble tous les titres cites, et la lecture reste sur le texte.
            st.markdown(
                f"- **{a['name']}** ({a['ticker']}) : "
                f"{a['pct']:.0f}% → **{a['nb_shares']} titres** "
                f"à {a['price']:,.0f} = {a['budget']:,.0f} {CURRENCY}"
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
            st.markdown(
                f"Pour diversifier : **{best_candidate['name']}** "
                f"({best_candidate['ticker']}) secteur "
                f"_{best_candidate['sector']}_ — {best_candidate['verdict']}"
            )



def _render_recommandations_ajustees(portfolio):
    """Les memes suggestions, une fois le risque et la liquidite comptes.

    Les deux versions s'affichent COTE A COTE, et c'est le point. Une
    recommandation ajustee qui remplacerait l'autre cacherait ce que
    l'ajustement change ; en les montrant ensemble, l'ecart devient
    l'information — et il se justifie ligne par ligne.

    Trois regles, et trois seulement, toutes verifiables sur les chiffres
    affiches plus haut :

      CONCENTRE   une ligne qui apporte nettement plus de risque que son poids
                  se retrograde d'un cran. Alleger la reduit le risque plus que
                  son poids ne le suggere.
      DIVERSIFIE  une ligne qui apporte nettement moins de risque que son poids
                  gagne un cran : elle amortit les autres.
      ILLIQUIDE   une ligne dont la sortie demande plus de cinq seances ne se
                  renforce pas. On n'augmente pas une position dont on ne peut
                  pas sortir, quelle que soit la qualite de la societe.
    """
    from utils.ui_helpers import section_heading
    try:
        from analysis.portefeuille_risque import mesures_portefeuille
        positions = tuple(sorted(
            (r["ticker"], float(r.get("current_value") or 0))
            for _, r in portfolio.iterrows()))
        p = mesures_portefeuille(positions)
    except Exception as err:                                    # noqa: BLE001
        st.caption(f"Ajustement au risque indisponible : {err}")
        return
    if not p or not p.get("lignes"):
        return

    scoring = _load_scoring_dict()
    ECHELLE = ["ALLÉGER", "CONSERVER", "ACHAT", "ACHAT FORT"]

    def _cran(verdict):
        v = (verdict or "").upper()
        if "FORT" in v and "ACHAT" in v:
            return 3
        if "ACHAT" in v:
            return 2
        if "CONSERVER" in v or "PRUDENCE" in v:
            return 1
        return 0

    section_heading("Les mêmes suggestions, ajustées", spacing="loose")
    st.caption(
        "Le risque et la liquidité ne changent pas la qualité d'une société — "
        "ils changent la **place** qu'elle mérite dans **ce** portefeuille. "
        "Une ligne excellente qui concentre le risque reste excellente : elle "
        "pèse simplement trop lourd ici.")

    entete = ("font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;"
              "color:var(--ink-3);font-weight:500;padding:9px 10px;"
              "border-bottom:1px solid var(--border);background:var(--bg-sunken);")
    cell = "padding:9px 10px;font-size:13px;border-bottom:1px solid var(--border);"

    html = (f"<tr><th style='{entete};text-align:left;'>Ligne</th>"
            f"<th style='{entete};text-align:left;'>Sans ajustement</th>"
            f"<th style='{entete};text-align:left;'>Ajusté au risque "
            f"et à la liquidité</th>"
            f"<th style='{entete};text-align:left;'>Pourquoi</th></tr>")
    changements = 0
    for l in p["lignes"]:
        base = _cran((scoring.get(l["ticker"]) or {}).get("verdict"))
        ajuste, motifs = base, []
        if l["concentre"]:
            ajuste -= 1
            motifs.append(f"apporte {l['contribution_risque']:.0%} du risque "
                          f"pour {l['poids']:.0%} du portefeuille")
        elif l["diversifie"]:
            ajuste += 1
            motifs.append(f"n'apporte que {l['contribution_risque']:.0%} du "
                          f"risque pour {l['poids']:.0%} du portefeuille")
        jours = l["jours_sortie"]
        if jours and jours > 5 and ajuste > 1:
            ajuste = 1
            motifs.append(f"sortie en {jours:.0f} séances : on ne renforce "
                          f"pas ce dont on ne peut pas sortir")
        # Un cran gagne au-dela du maximum, ou perdu en dessous du minimum,
        # ne change rien : afficher son motif laisserait croire a un
        # ajustement qui n'a pas eu lieu.
        borne = max(0, min(3, ajuste))
        if borne == base and motifs:
            motifs = [m + (" — déjà au maximum de l'échelle" if ajuste > 3
                           else " — déjà au minimum" if ajuste < 0 else "")
                      for m in motifs]
        ajuste = borne
        differe = ajuste != base
        changements += differe
        teinte = ("var(--down)" if ajuste < base else
                  "var(--up)" if ajuste > base else "var(--ink-3)")
        html += (
            f"<tr><td style='{cell}'>"
            f"<span class='ticker'>{l['ticker']}</span></td>"
            f"<td style='{cell};color:var(--ink-2);'>{ECHELLE[base]}</td>"
            f"<td style='{cell};font-weight:600;color:{teinte};'>"
            f"{ECHELLE[ajuste]}</td>"
            f"<td style='{cell};color:var(--ink-3);font-size:12px;'>"
            f"{' · '.join(motifs) if motifs else '—'}</td></tr>")

    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:12px;"
        f"overflow:hidden;background:var(--bg-elev);'>"
        f"<table style='width:100%;border-collapse:collapse;'>{html}</table>"
        f"</div>", unsafe_allow_html=True)
    st.caption(
        f"**{changements} suggestion(s) sur {len(p['lignes'])}** changent une "
        f"fois le risque et la liquidité comptés."
        if changements else
        "Aucune suggestion ne change : les poids de ce portefeuille sont "
        "cohérents avec les risques que chaque ligne y apporte.")



def _render_optimisation(portfolio, cash):
    """Ce qu'il faudrait acheter, ou alleger, pour mieux payer le risque porte.

    La question n'est pas « ce titre est-il bon », mais « ce titre ameliore-t-il
    CET ensemble ». Un titre agite mais decorrele reduit le risque du tout ; un
    excellent titre qui double une position deja lourde l'augmente.
    """
    from utils.ui_helpers import section_heading
    from analysis.risque import formater
    try:
        from analysis.portefeuille_risque import candidats_amelioration
        positions = tuple(sorted(
            (r["ticker"], float(r.get("current_value") or 0))
            for _, r in portfolio.iterrows()))
        r = candidats_amelioration(positions, float(cash or 0))
    except Exception as err:                                    # noqa: BLE001
        st.caption(f"Optimisation indisponible : {err}")
        return
    if not r:
        return

    section_heading("Maximiser le retour, minimiser le risque", spacing="loose")

    # Le seuil se regle ici plutot que d'etre impose : ce qui compte comme
    # « trop etroit » depend de la taille des positions qu'on envisage. Le voir
    # bouger apprend plus que de le subir — abaisse, il elargit le champ des
    # candidats ; releve, il ne garde que les valeurs ou l'on entre et sort
    # sans peine. Par defaut, le quart le moins echange de la cote.
    defaut = (r.get("seuil_illiquidite") or 0) / 1e6
    col_seuil, col_effet = st.columns([1, 2])
    with col_seuil:
        st.markdown("<div class='label-xs' style='margin-bottom:4px;'>"
                    "Seuil de liquidité · millions échangés par mois</div>",
                    unsafe_allow_html=True)
        seuil_m = st.number_input(
            "Seuil de liquidité", min_value=0.0, max_value=500.0,
            value=float(round(defaut, 1)), step=5.0, key="pf_seuil_liquidite",
            label_visibility="collapsed",
            help="Les titres qui échangent moins que cela sont écartés du "
                 "classement : un titre qui ne s'échange pas paraît décorrélé "
                 "sans l'être.")
    if abs(seuil_m * 1e6 - (r.get("seuil_illiquidite") or 0)) > 1:
        r = candidats_amelioration(positions, float(cash or 0), seuil_m * 1e6)
        if not r:
            return
    with col_effet:
        st.markdown(
            f"<div style='padding-top:22px;font-size:12.5px;"
            f"color:var(--ink-3);'>{len(r['ecartes_illiquides'])} titre(s) "
            f"écarté(s) · {len([c for c in r['candidats'] if c['ameliore']])} "
            f"candidat(s) retenu(s) sur {len(r['candidats'])} mesurés</div>",
            unsafe_allow_html=True)

    st.caption(
        f"Un titre **améliore** le portefeuille si son rendement, rapporté à "
        f"ce qu'il ajoute **au risque déjà porté**, dépasse le même rapport "
        f"calculé sur le portefeuille — aujourd'hui **{r['ratio_portefeuille']:.0f}**. "
        f"Ce n'est pas la volatilité du titre qui compte, mais sa "
        f"**corrélation** : un titre agité mais décorrélé réduit le risque de "
        f"l'ensemble.")

    entete = ("font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;"
              "color:var(--ink-3);font-weight:500;padding:9px 10px;"
              "border-bottom:1px solid var(--border);background:var(--bg-sunken);")
    cell = "padding:8px 10px;font-size:13px;border-bottom:1px solid var(--border);"
    nb = cell + "text-align:right;font-variant-numeric:tabular-nums;"

    def _table(liste, titre_bloc):
        if not liste:
            return
        html = (f"<tr><th style='{entete};text-align:left;'>{titre_bloc}</th>"
                f"<th style='{entete};text-align:right;'>Apport</th>"
                f"<th style='{entete};text-align:right;'>Rendement annuel</th>"
                f"<th style='{entete};text-align:right;'>Corrélation</th>"
                f"<th style='{entete};text-align:right;'>Position max</th></tr>")
        for c in liste:
            if c["decorrele"]:
                apport, teinte = "décorrélé", "var(--up)"
            else:
                apport = f"{c['ratio']:.0f}"
                teinte = ("var(--up)" if c["ameliore"] else "var(--down)")
            # La correlation est l'information qui explique le rang : basse,
            # elle vaut mieux qu'un rendement eleve.
            html += (
                f"<tr><td style='{cell}'>"
                f"<span class='ticker'>{c['ticker']}</span> "
                f"<span style='color:var(--ink-2);'>{_noms.get(c['ticker'], '')}</span>"
                + (" <span class='muted' style='font-size:11px;'>détenu "
                   f"{c['poids']:.0%}</span>" if c["detenu"] else "")
                + f"</td>"
                f"<td style='{nb};font-weight:600;color:{teinte};'>{apport}</td>"
                f"<td style='{nb}'>{c['rendement_annuel']:.1%}</td>"
                f"<td style='{nb}'>"
                f"{'—' if c['correlation'] is None else format(c['correlation'], '+.2f')}</td>"
                f"<td style='{nb};color:var(--ink-3);'>"
                f"{formater('montant_echange', c['taille_max'])}</td></tr>")
        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:12px;"
            f"overflow:hidden;background:var(--bg-elev);margin-top:10px;'>"
            f"<table style='width:100%;border-collapse:collapse;'>{html}</table>"
            f"</div>", unsafe_allow_html=True)

    # Le nom se lit mieux qu'un code : personne ne retient que BOAS est Bank of
    # Africa Senegal.
    _noms = {}
    try:
        from data.db import read_sql_df
        _t = read_sql_df("SELECT ticker, company_name FROM market_data")
        _noms = {r0["ticker"]: r0["company_name"] for _, r0 in _t.iterrows()}
    except Exception:                                           # noqa: BLE001
        pass

    ameliorent = [c for c in r["candidats"] if c["ameliore"]]
    degradent = [c for c in r["candidats"] if not c["ameliore"]]
    _table(ameliorent[:5], "Améliorent le portefeuille")
    st.caption(
        "**Position max** : ce qui se revendrait en cinq séances au rythme "
        "d'échange habituel du titre. Au-delà, la ligne se détient bien mais "
        "ne se vend pas."
        + (f" Avec **{cash:,.0f} FCFA** de liquidités disponibles."
           if cash else ""))

    # ── Ce qu'on ferait du cash ──────────────────────────────────────────
    # Le classement marginal ne regarde que le couple rendement-risque PASSE.
    # Un titre peut y bien figurer et etre une societe qui se degrade : la
    # repartition croise donc les deux, et n'en retient que trois au plus.
    plan = None
    if cash and cash > 0:
        try:
            from analysis.portefeuille_risque import allocation_suggeree
            scores = tuple(sorted(_load_scoring_dict().items(),
                                  key=lambda kv: kv[0]))
            plan = allocation_suggeree(positions, float(cash), scores,
                                       seuil_m * 1e6)
        except Exception:                                       # noqa: BLE001
            plan = None
        if plan and plan.get("lignes"):
            section_heading("Répartition suggérée du cash", spacing="loose")
            st.caption(
                f"Le modèle a essayé **{plan['nb_essais']} combinaisons** "
                f"d'une, deux et trois lignes parmi {plan['eligibles']} "
                f"candidats, et retenu celle qui donne le meilleur rendement "
                f"par unité de risque. **Trois lignes ne valent pas mieux que "
                f"deux si la troisième ne fait que diluer.**")
            html = (f"<tr><th style='{entete};text-align:left;'>Titre</th>"
                    f"<th style='{entete};text-align:right;'>Montant</th>"
                    f"<th style='{entete};text-align:left;'>Avis du modèle</th>"
                    f"</tr>")
            for l in plan["lignes"]:
                borne = ("<span class='muted' style='font-size:11px;'> · "
                         "plafonné par la liquidité</span>"
                         if l["borne_par_liquidite"] else "")
                html += (
                    f"<tr><td style='{cell}'>"
                    f"<span class='ticker'>{l['ticker']}</span> "
                    f"<span style='color:var(--ink-2);'>"
                    f"{_noms.get(l['ticker'], '')}</span></td>"
                    f"<td style='{nb};font-weight:600;'>"
                    f"{l['montant']:,.0f}{borne}</td>"
                    f"<td style='{cell};color:var(--ink-3);'>"
                    f"{l['verdict']} · score {l['score']:.0f}/100</td></tr>")
            st.markdown(
                f"<div style='border:1px solid var(--border);"
                f"border-radius:12px;overflow:hidden;background:var(--bg-elev);'>"
                f"<table style='width:100%;border-collapse:collapse;'>{html}"
                f"</table></div>", unsafe_allow_html=True)

            # L'effet, avant et apres : une recommandation qui ne montre pas
            # son effet ne se verifie pas.
            if plan.get("volatilite_avant") and plan.get("volatilite_apres"):
                cols = st.columns(3)
                for col, (intitule, avant, apres, fmt) in zip(cols, (
                        ("Volatilité", plan["volatilite_avant"],
                         plan["volatilite_apres"], "pct"),
                        ("Rendement annuel", plan["rendement_avant"],
                         plan["rendement_apres"], "pct"),
                        ("Rendement par unité de risque",
                         plan["sharpe_avant"], plan["sharpe_apres"], "dec"))):
                    if avant is None or apres is None:
                        continue
                    ecrire = (lambda v: f"{v:.1%}") if fmt == "pct" else (
                        lambda v: f"{v:.2f}")
                    mieux = (apres < avant) if intitule == "Volatilité" else (
                        apres > avant)
                    teinte = "var(--up)" if mieux else "var(--down)"
                    with col:
                        st.markdown(
                            f"<div style='background:var(--bg-elev);border:1px "
                            f"solid var(--border);border-radius:12px;"
                            f"padding:12px 14px;'>"
                            f"<div class='label-xs'>{intitule}</div>"
                            f"<div style='font-size:18px;font-weight:600;"
                            f"margin-top:4px;'>{ecrire(avant)} "
                            f"<span style='color:var(--ink-3);'>→</span> "
                            f"<span style='color:{teinte};'>{ecrire(apres)}"
                            f"</span></div></div>", unsafe_allow_html=True)

            if plan.get("ex_aequo", 0) > 1:
                st.caption(
                    f"**{plan['ex_aequo']} combinaisons se tiennent à moins "
                    f"d'un pour cent** l'une de l'autre. Les départager serait "
                    f"arbitraire : à égalité, la plus simple est retenue — "
                    f"moins de lignes, moins de frais, moins à surveiller.")

            with st.expander("Les combinaisons essayées, du meilleur au moins bon"):
                h = (f"<tr><th style='{entete};text-align:left;'>Combinaison</th>"
                     f"<th style='{entete};text-align:right;'>Rdt/risque</th>"
                     f"<th style='{entete};text-align:right;'>Volatilité</th>"
                     f"<th style='{entete};text-align:right;'>Rendement</th>"
                     f"</tr>")
                for e in plan["essais"]:
                    retenue = (e["tickers"] == [l["ticker"] for l in plan["lignes"]])
                    fond = ("background:var(--bg-sunken);" if retenue else "")
                    h += (f"<tr style='{fond}'><td style='{cell}'>"
                          f"{' + '.join(e['tickers'])}"
                          + ("  <span class='muted' style='font-size:11px;'>"
                             "retenue</span>" if retenue else "")
                          + f"</td><td style='{nb};font-weight:600;'>"
                            f"{e['sharpe']:.3f}</td>"
                            f"<td style='{nb}'>{e['volatilite']:.1%}</td>"
                            f"<td style='{nb}'>{e['rendement']:.1%}</td></tr>")
                st.markdown(
                    f"<div style='border:1px solid var(--border);"
                    f"border-radius:12px;overflow:hidden;'>"
                    f"<table style='width:100%;border-collapse:collapse;'>{h}"
                    f"</table></div>", unsafe_allow_html=True)

    if degradent:
        with st.expander(f"Les {len(degradent)} autres, qui dégraderaient "
                         f"le couple rendement-risque"):
            _table(degradent, "Dégradent le portefeuille")

    # Tous les titres cites sur cet onglet, ouvrables d'un seul endroit.
    _cites = ([(c["ticker"], _noms.get(c["ticker"], "")) for c in ameliorent[:5]]
              + [(l["ticker"], _noms.get(l["ticker"], ""))
                 for l in ((plan or {}).get("lignes") or [])])
    _barre_titres(_cites, "optim")

    detenus_ecartes = [t for t in r["ecartes_illiquides"]
                       if t in {p[0] for p in positions}]
    avertissements = []
    if detenus_ecartes:
        avertissements.append(
            f"**{', '.join(detenus_ecartes)}** : détenu(s) mais écarté(s) du "
            f"classement. Un titre qui ne cote pas paraît décorrélé — son "
            f"cours ne bouge pas quand le marché bouge — et le classement le "
            f"récompenserait pour son illiquidité.")
    avertissements.append(
        "**Le rendement passé n'est pas le rendement attendu.** Ce classement "
        "dit ce qui **aurait** amélioré le portefeuille sur "
        f"{r['observations']} mois, pas ce qui l'améliorera. Il désigne des "
        "candidats à examiner ; il ne décide de rien.")
    st.markdown(
        "<div style='background:var(--bg-elev);border:1px solid var(--border);"
        "border-left:3px solid var(--ocre);border-radius:12px;"
        "padding:14px 16px;margin-top:14px;'>"
        + "".join(f"<div style='font-size:12.5px;line-height:1.6;"
                  f"color:var(--ink-2);margin-bottom:6px;'>· {_gras_html(a)}</div>"
                  for a in avertissements)
        + "</div>", unsafe_allow_html=True)


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
            "**Rappels importants**\n"
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
            pru = col3.number_input(
                "PRU (FCFA)", min_value=0.0, value=0.0,
                step=0.01, format="%.2f", key=f"batch_pru_{i}",
            )
            if ticker_sel and qty > 0 and pru > 0:
                positions.append((ticker_sel, qty, pru))

        if st.form_submit_button("💾 Importer toutes les positions"):
            if positions:
                for sel, qty, pru in positions:
                    ticker = sel.split(" - ")[0]
                    name = sel.split(" - ")[1] if " - " in sel else ""
                    save_position(ticker, name, qty, pru)
                st.success(f"{len(positions)} position(s) importée(s).")
                st.rerun()
            else:
                st.warning("Aucune position valide à importer")
