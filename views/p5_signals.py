"""
Page 5 : Signaux d'achat/vente
Choix par secteur ou par titre, scan automatique.
"""

import streamlit as st
import pandas as pd

from config import load_tickers
from data.storage import (
    get_fundamentals, get_cached_prices,
    get_analyzable_tickers, get_all_stocks_for_analysis,
    get_all_cached_prices,
    save_signal_snapshots, save_recommendation_snapshot,
)
from data.db import read_sql_df
from analysis.scoring import compute_hybrid_score, compute_consolidated_verdict
from utils.charts import stars_display
from utils.nav import ticker_quick_picker
from utils.auth import is_admin

import json as _json


@st.cache_data(ttl=300, show_spinner=False)
def _load_scoring_snapshot() -> pd.DataFrame:
    """Lit scoring_snapshot en une requête. Retourne un DataFrame indexé par ticker."""
    try:
        return read_sql_df(
            "SELECT ticker, company_name, sector, price, hybrid_score, "
            "fundamental_score, technical_score, verdict, stars, trend, "
            "nb_signals, signals_json, consolidated_json FROM scoring_snapshot"
        )
    except Exception:
        return pd.DataFrame()


def render():
    from utils.ui_helpers import section_heading
    st.title("Signaux d'achat / vente")
    st.caption("Verdict consolidé par titre — recommandation fondamentale + signaux techniques")

    analyzable = get_analyzable_tickers()
    if not analyzable:
        st.warning("Aucune donnée disponible.")
        return

    # ═══════════════════════════════════════════════════════════════════
    # Filtres : ANALYSER PAR (pills) · CHOISIR UN SECTEUR · FILTRER VERDICT
    # (row compacte, 3 colonnes, avec labels-xs au-dessus)
    # ═══════════════════════════════════════════════════════════════════
    col_mode, col_sect, col_verdict = st.columns([1.3, 1.5, 1.2])

    with col_mode:
        st.markdown("<div class='label-xs' style='margin-bottom:4px;'>Analyser par</div>",
                    unsafe_allow_html=True)
        mode = st.radio(
            "Mode", ["Secteur", "Titres spécifiques", "Tout"],
            horizontal=True, label_visibility="collapsed",
        )

    if mode == "Secteur":
        sectors = sorted(set(t["sector"] for t in analyzable if t.get("sector")))
        with col_sect:
            st.markdown("<div class='label-xs' style='margin-bottom:4px;'>Choisir un secteur</div>",
                        unsafe_allow_html=True)
            selected_sector = st.selectbox("Secteur", sectors, label_visibility="collapsed")
        target_tickers = [t for t in analyzable if t["sector"] == selected_sector]
    elif mode == "Titres spécifiques":
        options = [f"{t['ticker']} · {t['name']}" for t in analyzable]
        with col_sect:
            st.markdown("<div class='label-xs' style='margin-bottom:4px;'>Choisir des titres</div>",
                        unsafe_allow_html=True)
            selected = st.multiselect(
                "Titres", options, default=options[:5],
                label_visibility="collapsed",
            )
        target_tickers = [
            {"ticker": s.split(" · ")[0], "name": s.split(" · ")[1] if " · " in s else ""}
            for s in selected
        ]
    else:
        target_tickers = analyzable
        # col_sect reste vide dans ce mode

    with col_verdict:
        st.markdown("<div class='label-xs' style='margin-bottom:4px;'>Filtrer verdict</div>",
                    unsafe_allow_html=True)
        verdict_filter_choice = st.selectbox(
            "Verdict", ["Tous", "ACHAT FORT", "ACHAT", "CONSERVER", "PRUDENCE", "ÉVITER"],
            label_visibility="collapsed",
        )

    if not target_tickers:
        st.info("Aucun titre à analyser.")
        return

    # --- Scan ---
    all_signals = []
    stock_summaries = []
    # Par-titre : (ticker → dict avec consolidated verdict, buy, sell, etc.)
    per_ticker = []

    # ─── Lecture depuis scoring_snapshot (pré-calculé par le cron quotidien) ──
    # Remplace la boucle compute_hybrid_score × 48 qui prenait ~1 min sur Cloud.
    snap = _load_scoring_snapshot()
    snap_by_ticker = {}
    if not snap.empty:
        snap_by_ticker = {r["ticker"]: r.to_dict() for _, r in snap.iterrows()}

    target_set = {t["ticker"] for t in target_tickers}
    ticker_name_map = {t["ticker"]: t.get("name", "") for t in target_tickers}

    snapshot_used = bool(snap_by_ticker) and any(tk in snap_by_ticker for tk in target_set)

    if snapshot_used:
        for t in target_tickers:
            ticker = t["ticker"]
            row = snap_by_ticker.get(ticker)
            if not row:
                continue
            name = row.get("company_name") or t.get("name", ticker)
            sector = row.get("sector", "")
            price = row.get("price") or 0

            # Parse signals_json
            try:
                signals = _json.loads(row.get("signals_json") or "[]")
            except Exception:
                signals = []
            try:
                consolidated = _json.loads(row.get("consolidated_json") or "{}")
            except Exception:
                consolidated = {}

            for sig in signals:
                all_signals.append({"ticker": ticker, "name": name, **sig})

            stock_summaries.append({
                "ticker": ticker, "name": name, "sector": sector, "price": price,
                "hybrid_score": row.get("hybrid_score"),
                "verdict": row.get("verdict"),
                "stars": row.get("stars"),
                "trend": row.get("trend"),
                "nb_signals": row.get("nb_signals") or 0,
            })

            # Reconstitue un result minimal à partir du snapshot pour le rendu
            result = {
                "hybrid_score": row.get("hybrid_score"),
                "fundamental_score": row.get("fundamental_score"),
                "technical_score": row.get("technical_score"),
                "recommendation": {
                    "verdict": row.get("verdict"),
                    "stars": row.get("stars"),
                },
                "trend": {"trend": row.get("trend")},
                "signals": signals,
            }
            per_ticker.append({
                "ticker": ticker, "name": name, "sector": sector, "price": price,
                "result": result,
                "consolidated": consolidated,
            })

    else:
        # ─── Fallback : calcul live (lent, utilisé si snapshot vide) ────────
        if is_admin():
            st.warning(
                "⚠️ Snapshots vides. Cliquez sur **📸 Regénérer snapshots** dans la sidebar "
                "pour accélérer cette page (passage de ~1 min à <1 s)."
            )
        all_stocks = get_all_stocks_for_analysis()
        all_prices = get_all_cached_prices()
        fund_by_ticker = {}
        if not all_stocks.empty:
            for _, r in all_stocks.iterrows():
                fund_by_ticker[r["ticker"]] = r.to_dict()

        with st.spinner(f"Calcul live pour {len(target_tickers)} titres…"):
            for t in target_tickers:
                ticker = t["ticker"]
                display_name = t.get("name", ticker)
                fund = fund_by_ticker.get(ticker)
                if fund:
                    import math as _m
                    fund = {k: (None if isinstance(v, float) and _m.isnan(v) else v)
                            for k, v in fund.items()}
                if not fund:
                    continue

                price_df = all_prices.get(ticker, pd.DataFrame())
                result = compute_hybrid_score(fund, price_df)

                name = fund.get("company_name") or display_name
                sector = fund.get("sector", "")
                ticker_signals = []
                for sig in result.get("signals", []):
                    enriched = {"ticker": ticker, "name": name, **sig}
                    all_signals.append(enriched)
                    ticker_signals.append(sig)

                ratios = result["ratios"]
                checklist = ratios.get("checklist", [])
                passed = sum(1 for c in checklist if c["passed"] is True)
                total = len(checklist)
                if passed == total and total > 0:
                    extra = {"type": "achat", "signal": "Checklist complete",
                             "strength": 5, "details": f"Tous les {total} critères valides"}
                    all_signals.append({"ticker": ticker, "name": name, **extra})
                    ticker_signals.append(extra)
                elif passed >= total - 1 and total > 0:
                    extra = {"type": "achat", "signal": "Checklist quasi-complete",
                             "strength": 3, "details": f"{passed}/{total} critères valides"}
                    all_signals.append({"ticker": ticker, "name": name, **extra})
                    ticker_signals.append(extra)

                stock_summaries.append({
                    "ticker": ticker, "name": name, "sector": sector,
                    "price": fund.get("price", 0),
                    "hybrid_score": result["hybrid_score"],
                    "verdict": result["recommendation"]["verdict"],
                    "stars": result["recommendation"]["stars"],
                    "trend": result["trend"]["trend"],
                    "nb_signals": len([s for s in result.get("signals", [])
                                       if s["type"] in ("achat", "vente")]),
                })

                enriched_result = dict(result)
                enriched_result["signals"] = list(result.get("signals", [])) + [
                    s for s in ticker_signals if s not in result.get("signals", [])
                ]
                consolidated = compute_consolidated_verdict(enriched_result)
                per_ticker.append({
                    "ticker": ticker, "name": name, "sector": sector,
                    "price": fund.get("price", 0),
                    "result": result, "consolidated": consolidated,
                })

    total_snapshots_saved = 0
    total_recos_saved = 0

    # ═══════════════════════════════════════════════════════════════════
    # 4 KPI cards : SIGNAUX ACHAT · SIGNAUX VENTE · CONSERVER · CONTRADICTIONS
    # ═══════════════════════════════════════════════════════════════════
    n_total = len(per_ticker)
    n_achat = sum(1 for p in per_ticker
                  if "ACHAT" in (p.get("consolidated", {}).get("verdict", "").upper()))
    n_vente = sum(1 for p in per_ticker
                  if "VENTE" in (p.get("consolidated", {}).get("verdict", "").upper()))
    n_conserver = sum(1 for p in per_ticker
                      if "CONSERVER" in (p.get("consolidated", {}).get("verdict", "").upper())
                      or "NEUTRE" in (p.get("consolidated", {}).get("verdict", "").upper()))
    n_contra = sum(1 for p in per_ticker
                   if p.get("consolidated", {}).get("conflict"))

    def _kpi(label, value, sub, tone):
        arrow = {"up": "▲", "down": "▼"}.get(tone, "—")
        sub_color = {"up": "var(--up)", "down": "var(--down)",
                     "warn": "var(--ocre)"}.get(tone, "var(--ink-3)")
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;min-height:90px;'>"
            f"<div class='label-xs' style='margin-bottom:6px;'>{label}</div>"
            f"<div style='font-size:26px;font-weight:600;letter-spacing:-0.02em;"
            f"color:var(--ink);font-variant-numeric:tabular-nums;line-height:1;'>{value}</div>"
            f"<div style='font-size:11.5px;color:{sub_color};margin-top:6px;font-weight:500;'>"
            f"{arrow} {sub}</div>"
            f"</div>"
        )

    # Insertion AVANT la barre de filtres pour une hiérarchie visuelle
    # (on re-utilise les colonnes mais en push via HTML)
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(_kpi("Signaux achat", n_achat, f"sur {n_total} titres", "up"),
                    unsafe_allow_html=True)
    with k2:
        st.markdown(_kpi("Signaux vente", n_vente, f"sur {n_total} titres", "down"),
                    unsafe_allow_html=True)
    with k3:
        st.markdown(_kpi("Conserver", n_conserver, f"sur {n_total} titres", "neutral"),
                    unsafe_allow_html=True)
    with k4:
        st.markdown(_kpi("Contradictions", n_contra, "à surveiller",
                          "warn" if n_contra else "neutral"),
                    unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════
    # Synthèse par titre — tableau HTML éditorial unique
    # (fusionne les deux anciennes tables Consolidated + Résumé)
    # ═══════════════════════════════════════════════════════════════════
    from utils.ui_helpers import section_heading
    section_heading("Synthèse par titre", spacing="loose")

    # Filtrage par verdict (choice = "Tous" ou un verdict spécifique)
    def _match_verdict(entry, choice):
        if choice == "Tous":
            return True
        v = (entry.get("consolidated", {}).get("verdict") or "").upper()
        v_reco = (entry.get("result", {}).get("recommendation", {}).get("verdict") or "").upper()
        target = choice.upper().replace("É", "E")
        return target in v or target in v_reco

    filtered = [e for e in per_ticker if _match_verdict(e, verdict_filter_choice)]

    if not filtered:
        st.info("Aucun titre ne correspond au filtre.")
    else:
        # Tri : ACHAT FORT, ACHAT, CONSERVER, PRUDENCE, VENTE, VENTE FORTE
        order_map = {"ACHAT FORT": 0, "ACHAT": 1, "CONSERVER": 2, "NEUTRE": 2,
                     "PRUDENCE": 3, "VENTE": 4, "VENTE FORTE": 5, "EVITER": 6}

        def _rank(entry):
            v = (entry.get("result", {}).get("recommendation", {}).get("verdict") or "").upper()
            for k, r in order_map.items():
                if k in v:
                    return r
            return 99

        filtered_sorted = sorted(filtered,
                                  key=lambda e: (_rank(e),
                                                 -(e.get("result", {}).get("hybrid_score") or 0)))

        header_style = (
            "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
            "color:var(--ink-3);font-weight:500;padding:9px 10px;"
            "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
        )
        cell_style = "padding:10px;font-size:13px;border-bottom:1px solid var(--border);"

        def _verdict_tag(v):
            if not v:
                return "<span class='muted'>—</span>"
            up = v.upper()
            if "ACHAT FORT" in up:
                return "<span class='tag up' style='text-transform:none;font-weight:600;'>ACHAT FORT</span>"
            if "ACHAT" in up:
                return "<span class='tag up' style='text-transform:none;font-weight:600;'>ACHAT</span>"
            if "VENTE FORTE" in up:
                return "<span class='tag down' style='text-transform:none;font-weight:600;'>VENTE FORTE</span>"
            if "VENTE" in up or "EVITER" in up:
                return "<span class='tag down' style='text-transform:none;font-weight:600;'>VENTE</span>"
            if "CONSERVER" in up or "NEUTRE" in up:
                return "<span class='tag ocre' style='text-transform:none;font-weight:600;'>CONSERVER</span>"
            if "PRUDENCE" in up:
                return "<span class='tag ocre' style='text-transform:none;font-weight:600;'>PRUDENCE</span>"
            return f"<span class='tag neutral'>{v}</span>"

        def _trend_cell(tr):
            if not tr:
                return "<span class='muted'>—</span>"
            tone = {"haussiere": "up", "baissiere": "down"}.get(tr, "neutral")
            return f"<span class='dot {tone}'></span>{tr}"

        def _signals_summary(cons, kind):
            """kind='buy' ou 'sell'."""
            if not cons:
                return "<span class='muted'>—</span>"
            sigs = cons.get("consolidated_signals", {}).get(kind, [])
            if not sigs:
                return "<span class='muted'>—</span>"
            parts = [f"{s['signal']}" for s in sigs[:2]]
            extra = f" +{len(sigs) - 2}" if len(sigs) > 2 else ""
            return ", ".join(parts) + extra

        rows_html = (
            f"<tr>"
            f"<th style='{header_style};text-align:left;'>Verdict</th>"
            f"<th style='{header_style};text-align:left;'>Ticker</th>"
            f"<th style='{header_style};text-align:left;'>Nom</th>"
            f"<th style='{header_style};text-align:right;'>Prix</th>"
            f"<th style='{header_style};text-align:right;'>Score</th>"
            f"<th style='{header_style};text-align:right;'>Conf.</th>"
            f"<th style='{header_style};text-align:left;'>Tendance</th>"
            f"<th style='{header_style};text-align:left;'>Signaux achat</th>"
            f"<th style='{header_style};text-align:left;'>Signaux vente</th>"
            f"</tr>"
        )
        for e in filtered_sorted:
            res = e.get("result", {})
            cons = e.get("consolidated", {})
            verdict = res.get("recommendation", {}).get("verdict") or cons.get("verdict") or ""
            score = res.get("hybrid_score") or 0
            conf = cons.get("confidence") or 0
            trend_name = res.get("trend", {}).get("trend") or "—"
            price = e.get("price") or 0

            rows_html += (
                f"<tr>"
                f"<td style='{cell_style}'>{_verdict_tag(verdict)}</td>"
                f"<td style='{cell_style}'><span class='ticker'>{e.get('ticker','')}</span></td>"
                f"<td style='{cell_style};font-weight:500;'>{e.get('name','')}</td>"
                f"<td style='{cell_style};text-align:right;font-variant-numeric:tabular-nums;'>"
                f"{price:,.0f}</td>"
                f"<td style='{cell_style};text-align:right;font-weight:600;"
                f"font-variant-numeric:tabular-nums;'>{score:.0f}/100</td>"
                f"<td style='{cell_style};text-align:right;font-variant-numeric:tabular-nums;'>"
                f"{int(conf)}%</td>"
                f"<td style='{cell_style}'>{_trend_cell(trend_name)}</td>"
                f"<td style='{cell_style};color:var(--ink-2);'>{_signals_summary(cons, 'buy')}</td>"
                f"<td style='{cell_style};color:var(--ink-2);'>{_signals_summary(cons, 'sell')}</td>"
                f"</tr>"
            )

        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:10px;"
            f"overflow:hidden;background:var(--bg-elev);margin-bottom:16px;'>"
            f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
            unsafe_allow_html=True,
        )

        # Quick picker
        picker_options = [
            (e["ticker"], f"{e['ticker']} · {e.get('name','')}")
            for e in filtered_sorted
        ]
        ticker_quick_picker(picker_options, key="sig_goto",
                             label="Ouvrir l'analyse d'un titre")

    # Assistant chat
    section_heading("Assistant Signaux", spacing="loose")
    _render_signals_chat(all_signals, stock_summaries)


def _render_consolidated_view(per_ticker):
    """Vue synthétique par titre : verdict consolidé (recommandation + signaux),
    signaux dédupliqués par famille, alerte contradictions. Présentation en tableau."""
    if not per_ticker:
        st.info("Aucun titre à analyser.")
        return

    from utils.ui_helpers import section_heading
    section_heading("Synthèse par titre")
    st.caption(
        "Verdict consolidé : recommandation fondamentale + signaux techniques dédupliqués par famille. "
        "Colonnes **Achats** / **Ventes** listent la famille et le signal retenu (le plus fort par famille). "
        "Colonne **Contradictions** : familles avec signaux achat ET vente."
    )

    # Filter controls
    col_a, col_b = st.columns([2, 2])
    with col_a:
        verdict_filter = st.multiselect(
            "Filtrer par verdict",
            ["ACHAT FORT CONFIRMÉ", "ACHAT", "NEUTRE", "VENTE", "VENTE FORTE CONFIRMÉE", "⚠️ CONTRADICTION"],
            default=[],
            key="cons_filter_verdict",
        )
    with col_b:
        show_only_with_signals = st.checkbox(
            "Afficher seulement les titres avec signaux", value=True, key="cons_only_signals",
        )

    # Build rows
    rows = []
    for entry in per_ticker:
        cons = entry["consolidated"]
        if verdict_filter and cons["verdict"] not in verdict_filter:
            continue
        signals_cons = cons["consolidated_signals"]
        has_signals = bool(signals_cons["buy"] or signals_cons["sell"])
        if show_only_with_signals and not has_signals:
            continue

        def _fmt_signal_list(sig_list):
            if not sig_list:
                return "—"
            return "  \n".join(
                f"• {s.get('family', '?')} · {s['signal']} ({s.get('strength', 0)}★)"
                for s in sig_list
            )

        # Sort rank: 0=Achat fort, 1=Achat, 2=Vente forte, 3=Vente, 4=Contradiction, 5=Neutre
        verdict = cons["verdict"]
        if verdict == "ACHAT FORT CONFIRMÉ":
            rank = 0
        elif verdict == "ACHAT":
            rank = 1
        elif verdict == "VENTE FORTE CONFIRMÉE":
            rank = 2
        elif verdict == "VENTE":
            rank = 3
        elif verdict.startswith("⚠️") or cons.get("conflict"):
            rank = 4
        else:
            rank = 5

        rows.append({
            "Verdict": f"{cons['icon']} {cons['verdict']}",
            "Ticker": entry["ticker"],
            "Nom": entry["name"],
            "Secteur": entry["sector"],
            "Prix": entry["price"] if entry["price"] else None,
            "Score": cons["hybrid_score"],
            "Confiance": cons["confidence"],
            "Tendance": cons.get("trend") or "—",
            "🟢 Achats": _fmt_signal_list(signals_cons["buy"]),
            "🔴 Ventes": _fmt_signal_list(signals_cons["sell"]),
            "Net": cons["consolidated_signals"]["net_score"],
            "Contradictions": (
                "⚠️ " + ", ".join(signals_cons["contradictions"])
                if signals_cons["contradictions"] else "—"
            ),
            "_rank": rank,
            "_confidence": cons.get("confidence", 0),
        })

    if not rows:
        st.info("Aucun titre ne correspond aux filtres.")
        return

    df = pd.DataFrame(rows).sort_values(
        ["_rank", "_confidence"],
        ascending=[True, False],
    ).drop(columns=["_rank", "_confidence"])

    # Format numeric cols for display
    df["Prix"] = df["Prix"].apply(lambda x: f"{x:,.0f}" if x and not pd.isna(x) else "—")
    df["Score"] = df["Score"].apply(lambda x: f"{x:.0f}/100" if x and not pd.isna(x) else "—")
    df["Confiance"] = df["Confiance"].apply(lambda x: f"{int(x)}%")
    df["Net"] = df["Net"].apply(lambda x: f"{x:+d}")

    st.caption(f"{len(df)} titre(s) affiché(s)")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(600, 80 + 35 * len(df)),
        column_config={
            "🟢 Achats": st.column_config.TextColumn(width="large"),
            "🔴 Ventes": st.column_config.TextColumn(width="large"),
            "Nom": st.column_config.TextColumn(width="medium"),
            "Verdict": st.column_config.TextColumn(width="medium"),
        },
    )

    # Quick jump to stock analysis
    picker_options = [
        (row["Ticker"], f"{row['Ticker']} — {row['Nom']} ({row['Verdict']})")
        for _, row in df.iterrows()
    ]
    ticker_quick_picker(picker_options, key="sig_goto", label="🔍 Ouvrir l'analyse d'un titre")


def _render_signals_chat(all_signals, stock_summaries):
    """Zone de chat intelligent pour discuter des signaux."""
    from analysis.llm_chat import chat

    st.subheader("💬 Assistant Signaux")
    st.caption(
        "Posez des questions sur les signaux, les titres, les risques — "
        "l'assistant a accès à toutes les données fondamentales, techniques et aux actualités du marché."
    )

    if "sig_chat_history" not in st.session_state:
        st.session_state.sig_chat_history = []

    # Display chat history
    for msg in st.session_state.sig_chat_history:
        with st.chat_message(msg["role"], avatar="🧑‍💼" if msg["role"] == "user" else "📡"):
            st.markdown(msg["content"])

    # Chat input
    user_input = st.chat_input(
        "Ex: NEI CEDA est risqué car la société a perdu de l'argent... / Quel est le yield de Société Générale ?",
        key="sig_chat_input",
    )

    # Pick up prompt from either chat_input or a pending suggestion click
    pending = st.session_state.pop("sig_pending_prompt", None)
    prompt = user_input or pending

    if prompt:
        st.session_state.sig_chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="🧑‍💼"):
            st.markdown(prompt)

        with st.chat_message("assistant", avatar="📡"):
            with st.spinner("Analyse en cours..."):
                response = chat(
                    query=prompt,
                    mode="signals",
                    chat_history=st.session_state.sig_chat_history[:-1],
                    signals_data=all_signals,
                    stock_summaries=stock_summaries,
                )
            st.markdown(response)

        st.session_state.sig_chat_history.append({"role": "assistant", "content": response})

    # Quick suggestions when empty
    if not st.session_state.sig_chat_history:
        st.markdown("**Suggestions :**")
        cols = st.columns(4)
        suggestions = [
            "Quels signaux d'achat sont les plus fiables ?",
            "Quels titres présentent le plus de risque ?",
            "Quel est le yield de Société Générale CI ?",
            "Y a-t-il des signaux contradictoires ?",
        ]
        for i, sug in enumerate(suggestions):
            if cols[i].button(f"💡 {sug}", key=f"sig_sug_{i}"):
                st.session_state["sig_pending_prompt"] = sug
                st.rerun()

