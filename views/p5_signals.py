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
    save_signal_snapshots, save_recommendation_snapshot,
)
from analysis.scoring import compute_hybrid_score, compute_consolidated_verdict
from utils.charts import stars_display
from utils.nav import ticker_quick_picker


def render():
    st.markdown('<div class="main-header">📡 Signaux d\'Achat / Vente</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Choisissez un secteur ou des titres spécifiques à analyser</div>', unsafe_allow_html=True)

    analyzable = get_analyzable_tickers()
    if not analyzable:
        st.warning("Aucune donnée disponible.")
        return

    # --- Mode de selection ---
    mode = st.radio("Analyser par", ["Secteur", "Titres spécifiques", "Tous les titres avec données"], horizontal=True)

    if mode == "Secteur":
        sectors = sorted(set(t["sector"] for t in analyzable if t.get("sector")))
        selected_sector = st.selectbox("Choisir un secteur", sectors)
        target_tickers = [t for t in analyzable if t["sector"] == selected_sector]
        if not target_tickers:
            st.warning(f"Aucun titre avec des données dans le secteur {selected_sector}.")
            return

    elif mode == "Titres spécifiques":
        options = [f"{t['ticker']} - {t['name']}" for t in analyzable]
        selected = st.multiselect("Choisir des titres", options, default=options[:5])
        target_tickers = [
            {"ticker": s.split(" - ")[0], "name": s.split(" - ")[1] if " - " in s else ""}
            for s in selected
        ]

    else:
        target_tickers = analyzable

    # --- Scan ---
    all_signals = []
    stock_summaries = []
    # Par-titre : (ticker → dict avec consolidated verdict, buy, sell, etc.)
    per_ticker = []

    all_stocks = get_all_stocks_for_analysis()

    # Build name lookup from target_tickers (always has correct names from config)
    ticker_name_map = {t["ticker"]: t.get("name", "") for t in target_tickers}

    total_snapshots_saved = 0
    total_recos_saved = 0
    with st.spinner(f"Analyse de {len(target_tickers)} titres..."):
        for t in target_tickers:
            ticker = t["ticker"]
            display_name = t.get("name", ticker)
            fund = get_fundamentals(ticker)
            if not fund and not all_stocks.empty:
                row = all_stocks[all_stocks["ticker"] == ticker]
                if not row.empty:
                    fund = row.iloc[0].to_dict()
            if not fund:
                continue

            price_df = get_cached_prices(ticker)
            result = compute_hybrid_score(fund, price_df)

            # Use display_name from config as fallback for missing/None company_name
            name = fund.get("company_name") or display_name
            sector = fund.get("sector", "")
            current_price = fund.get("price") or 0
            # Try to use most recent close from price_df as the reference
            if not price_df.empty and "close" in price_df.columns:
                try:
                    current_price = float(price_df.sort_values("date").iloc[-1]["close"]) or current_price
                except Exception:
                    pass

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
                extra = {
                    "type": "achat", "signal": "Checklist complete", "strength": 5,
                    "details": f"Tous les {total} critères Value & Dividendes valides",
                }
                all_signals.append({"ticker": ticker, "name": name, **extra})
                ticker_signals.append(extra)
            elif passed >= total - 1 and total > 0:
                extra = {
                    "type": "achat", "signal": "Checklist quasi-complete", "strength": 3,
                    "details": f"{passed}/{total} critères valides",
                }
                all_signals.append({"ticker": ticker, "name": name, **extra})
                ticker_signals.append(extra)

            stock_summaries.append({
                "ticker": ticker, "name": name,
                "sector": sector,
                "price": fund.get("price", 0),
                "hybrid_score": result["hybrid_score"],
                "verdict": result["recommendation"]["verdict"],
                "stars": result["recommendation"]["stars"],
                "trend": result["trend"]["trend"],
                "nb_signals": len([s for s in result.get("signals", []) if s["type"] in ("achat", "vente")]),
            })

            # Consolidated verdict per ticker
            # Inject the checklist-based signals into result so consolidation sees them
            enriched_result = dict(result)
            enriched_result["signals"] = list(result.get("signals", [])) + [
                s for s in ticker_signals if s not in result.get("signals", [])
            ]
            consolidated = compute_consolidated_verdict(enriched_result)
            per_ticker.append({
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "price": fund.get("price", 0),
                "result": result,
                "consolidated": consolidated,
            })

            # --- Auto-capture for long-term calibration ---
            try:
                total_snapshots_saved += save_signal_snapshots(
                    ticker=ticker, signals=ticker_signals, price=current_price,
                    company_name=name, sector=sector,
                )
                if save_recommendation_snapshot(
                    ticker=ticker,
                    recommendation=result["recommendation"],
                    hybrid_score=result["hybrid_score"],
                    fundamental_score=result["fundamental_score"],
                    technical_score=result["technical_score"],
                    price=current_price,
                    trend=result["trend"]["trend"],
                    company_name=name, sector=sector,
                ):
                    total_recos_saved += 1
            except Exception:
                pass

    if total_snapshots_saved or total_recos_saved:
        st.caption(
            f"💾 Snapshot enregistré pour calibrage : {total_snapshots_saved} nouveau(x) signal(aux), "
            f"{total_recos_saved} recommandation(s)."
        )

    # --- Vue consolidée par titre ---
    st.markdown("---")
    _render_consolidated_view(per_ticker)

    # --- Summary Table ---
    st.markdown("---")
    st.subheader("📋 Résumé")
    if stock_summaries:
        sum_df = pd.DataFrame(stock_summaries).sort_values("hybrid_score", ascending=False)
        sum_df["stars_display"] = sum_df["stars"].apply(stars_display)
        sum_df["price_fmt"] = sum_df["price"].apply(lambda x: f"{x:,.0f}" if x else "N/A")
        sum_df["score_fmt"] = sum_df["hybrid_score"].apply(lambda x: f"{x:.0f}/100")
        trend_emoji = {"haussiere": "📈", "baissiere": "📉", "neutre": "➡️", "indetermine": "❓"}
        sum_df["trend_display"] = sum_df["trend"].apply(lambda x: f"{trend_emoji.get(x, '❓')} {x}")

        st.dataframe(
            sum_df[["ticker", "name", "sector", "price_fmt", "score_fmt", "verdict", "stars_display", "trend_display", "nb_signals"]].rename(columns={
                "ticker": "Ticker", "name": "Nom", "sector": "Secteur", "price_fmt": "Prix",
                "score_fmt": "Score", "verdict": "Verdict", "stars_display": "Rating",
                "trend_display": "Tendance", "nb_signals": "Signaux",
            }),
            use_container_width=True, hide_index=True,
        )

    # --- Chat zone ---
    st.markdown("---")
    _render_signals_chat(all_signals, stock_summaries)


def _render_consolidated_view(per_ticker):
    """Vue synthétique par titre : verdict consolidé (recommandation + signaux),
    signaux dédupliqués par famille, alerte contradictions. Présentation en tableau."""
    if not per_ticker:
        st.info("Aucun titre à analyser.")
        return

    st.subheader("🧭 Synthèse par titre")
    st.caption(
        "Verdict consolidé par titre : recommandation fondamentale + signaux techniques dédupliqués par famille. "
        "Colonnes **🟢 Achats** / **🔴 Ventes** listent la famille et le signal retenu (le plus fort par famille). "
        "⚠️ en colonne **Contradictions** = au moins une famille a des signaux achat ET vente."
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

