"""
Page 3 : Screening multi-critères
Affiche uniquement les titres avec des données exploitables.
"""

import streamlit as st
import pandas as pd

from data.storage import get_all_stocks_for_analysis, get_analyzable_tickers
from analysis.fundamental import compute_ratios, format_ratio
from utils.nav import ticker_quick_picker
from utils.ui_helpers import section_heading


def render():
    # Hiérarchie v3 : Title + caption → univers → filtres → résultat
    st.title("Screening")
    st.caption("Filtrer les titres BRVM par secteur et ratios fondamentaux")

    all_stocks = get_all_stocks_for_analysis()
    if all_stocks.empty:
        st.warning("Aucune donnée disponible. Lancez l'enrichissement des données de marché.")
        return

    # ─── Univers d'analyse ───
    section_heading("Univers d'analyse", spacing="tight")
    available_sectors = sorted(all_stocks["sector"].dropna().unique().tolist())
    col_sector, col_info = st.columns([3, 1])

    with col_sector:
        selected_sectors = st.multiselect("Secteurs", available_sectors, label_visibility="collapsed",
                                          placeholder="Tous les secteurs")
        if not selected_sectors:
            selected_sectors = available_sectors

    with col_info:
        st.metric("Titres avec données", f"{len(all_stocks)}")

    filtered_stocks = all_stocks[all_stocks["sector"].isin(selected_sectors)]

    # Sélecteur de tickers — ticker · nom (sans marqueur [fondamentaux]/[marché])
    ticker_options = [f"{r['ticker']} · {r['company_name']}"
                      for _, r in filtered_stocks.iterrows()]
    selected_tickers = st.multiselect(
        f"Titres ({len(filtered_stocks)} disponibles)",
        ticker_options,
        placeholder="Tous les titres du/des secteur(s) sélectionné(s)",
    )
    if not selected_tickers:
        selected_tickers = ticker_options
    target_tickers = {s.split(" · ")[0] for s in selected_tickers}

    # ─── Filtres fondamentaux ───
    section_heading("Filtres fondamentaux")

    col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns(5)

    with col_f1:
        st.markdown("**Dividend Yield (%)**")
        min_yield = st.number_input("Yield min", min_value=0.0, max_value=30.0, value=0.0, step=0.5, key="yield_min") / 100
        max_yield = st.number_input("Yield max", min_value=0.0, max_value=30.0, value=30.0, step=0.5, key="yield_max") / 100
    with col_f2:
        st.markdown("**PER**")
        min_per = st.number_input("PER min", min_value=0.0, max_value=100.0, value=0.0, step=1.0, key="per_min")
        max_per = st.number_input("PER max", min_value=0.0, max_value=100.0, value=100.0, step=1.0, key="per_max")
    with col_f3:
        st.markdown("**ROE (%)**")
        min_roe = st.number_input("ROE min", min_value=0.0, max_value=100.0, value=0.0, step=1.0, key="roe_min") / 100
        max_roe = st.number_input("ROE max", min_value=0.0, max_value=100.0, value=100.0, step=1.0, key="roe_max") / 100
    with col_f4:
        st.markdown("**Payout Ratio (%)**")
        min_payout = st.number_input("Payout min", min_value=0.0, max_value=200.0, value=0.0, step=5.0, key="payout_min") / 100
        max_payout = st.number_input("Payout max", min_value=0.0, max_value=200.0, value=200.0, step=5.0, key="payout_max") / 100
    with col_f5:
        st.markdown("**D/E (Dette/Equity)**")
        min_de = st.number_input("D/E min", min_value=0.0, max_value=20.0, value=0.0, step=0.5, key="de_min")
        max_de = st.number_input("D/E max", min_value=0.0, max_value=20.0, value=20.0, step=0.5, key="de_max")

    # --- Compute ratios (plus de divider — la hiérarchie suffit) ---
    results = []
    for _, row in filtered_stocks.iterrows():
        ticker = row.get("ticker", "")
        if ticker not in target_tickers:
            continue
        data = row.to_dict()
        # Replace NaN with None for compute_ratios
        for k, v in data.items():
            if pd.isna(v) if isinstance(v, (float, int)) else False:
                data[k] = None
        try:
            ratios = compute_ratios(data)
            # Use market_dividend_yield as fallback
            dy = ratios.get("dividend_yield")
            if not dy and data.get("market_dividend_yield"):
                dy = data["market_dividend_yield"]

            results.append({
                "ticker": ticker,
                "name": data.get("company_name") or "",
                "sector": data.get("sector") or "",
                "price": data.get("price") or 0,
                "has_fundamentals": bool(data.get("has_fundamentals")),
                "dividend_yield": dy,
                "per": ratios.get("per"),
                "roe": ratios.get("roe"),
                "net_margin": ratios.get("net_margin"),
                "payout_ratio": ratios.get("payout_ratio"),
                "debt_equity": ratios.get("debt_equity"),
                "pb": ratios.get("pb"),
                "eps": ratios.get("eps"),
                "dps": ratios.get("dps") or data.get("dps"),
                "beta": data.get("beta"),
                "rsi": data.get("rsi"),
                "fundamental_score": ratios.get("fundamental_score"),
                "checklist_passed": sum(1 for c in ratios.get("checklist", []) if c["passed"] is True),
                "checklist_total": len(ratios.get("checklist", [])),
            })
        except Exception:
            continue

    if not results:
        st.warning("Aucun titre avec des données dans la selection.")
        return

    screen_df = pd.DataFrame(results)

    # Apply ratio filters
    mask = pd.Series(True, index=screen_df.index)
    # Yield filter (min/max)
    if min_yield > 0:
        mask &= screen_df["dividend_yield"].fillna(0) >= min_yield
    if max_yield < 0.30:
        mask &= screen_df["dividend_yield"].fillna(0) <= max_yield
    # PER filter (min/max) — only apply to stocks with PER > 0
    if min_per > 0:
        mask &= (screen_df["per"].fillna(0) >= min_per) | (screen_df["per"].fillna(0) <= 0)
    if max_per < 100:
        mask &= (screen_df["per"].fillna(999) <= max_per) & (screen_df["per"].fillna(0) > 0)
    # ROE filter (min/max)
    if min_roe > 0:
        mask &= screen_df["roe"].fillna(0) >= min_roe
    if max_roe < 1.0:
        mask &= screen_df["roe"].fillna(0) <= max_roe
    # Payout filter (min/max)
    if min_payout > 0:
        mask &= screen_df["payout_ratio"].fillna(0) >= min_payout
    if max_payout < 2.0:
        mask &= screen_df["payout_ratio"].fillna(0) <= max_payout
    # D/E filter (min/max)
    if min_de > 0:
        mask &= screen_df["debt_equity"].fillna(0) >= min_de
    if max_de < 20:
        mask &= screen_df["debt_equity"].fillna(0) <= max_de

    filtered = screen_df[mask].sort_values("fundamental_score", ascending=False)

    # ─── Résultats ───
    section_heading(f"Résultats · {len(filtered)} titre(s)", spacing="loose")

    if filtered.empty:
        st.info("Aucun titre ne correspond. Élargissez vos critères.")
        return

    display_df = filtered.copy()
    display_df["dividend_yield"] = display_df["dividend_yield"].apply(lambda x: f"{x:.2%}" if pd.notna(x) and x != 0 else "—")
    display_df["per"] = display_df["per"].apply(lambda x: f"{x:.1f}" if pd.notna(x) and x != 0 else "—")
    display_df["roe"] = display_df["roe"].apply(lambda x: f"{x:.1%}" if pd.notna(x) and x != 0 else "—")
    display_df["net_margin"] = display_df["net_margin"].apply(lambda x: f"{x:.1%}" if pd.notna(x) and x != 0 else "—")
    display_df["payout_ratio"] = display_df["payout_ratio"].apply(lambda x: f"{x:.0%}" if pd.notna(x) and x != 0 else "—")
    display_df["debt_equity"] = display_df["debt_equity"].apply(lambda x: f"{x:.2f}x" if pd.notna(x) and x != 0 else "—")
    display_df["checklist"] = display_df.apply(lambda r: f"{r['checklist_passed']}/{r['checklist_total']}" if r['checklist_total'] > 0 else "—", axis=1)
    display_df["price"] = display_df["price"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) and x > 0 else "—")
    display_df["score"] = display_df["fundamental_score"].apply(lambda x: f"{x:.0f}/50")

    display_df["beta_fmt"] = display_df.get("beta", pd.Series()).apply(lambda x: f"{x:.2f}" if pd.notna(x) and x else "—")
    display_df["rsi_fmt"] = display_df.get("rsi", pd.Series()).apply(lambda x: f"{x:.0f}" if pd.notna(x) and x else "—")
    display_df["dps_fmt"] = display_df["dps"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")

    # Colonne "data" retirée : les emojis 📊/📈 n'apportent pas de valeur dans un tableau
    # éditorial. L'info est déjà dans la présence ou non des ratios calculés.
    show_cols = {
        "ticker": "Ticker", "name": "Nom", "sector": "Secteur", "price": "Prix",
        "dividend_yield": "Yield", "dps_fmt": "DPS", "per": "PER", "roe": "ROE",
        "payout_ratio": "Payout", "debt_equity": "D/E",
        "beta_fmt": "Beta", "rsi_fmt": "RSI",
        "checklist": "Check", "score": "Score",
    }

    st.dataframe(
        display_df[list(show_cols.keys())].rename(columns=show_cols),
        use_container_width=True,
        height=min(len(filtered) * 40 + 50, 700),
    )

    # Quick jump to analysis
    picker_options = [
        (row["ticker"], f"{row['ticker']} — {row['name']}")
        for _, row in filtered.iterrows()
    ]
    ticker_quick_picker(picker_options, key="screen_goto", label="Ouvrir l'analyse d'un titre")

    csv = filtered[["ticker", "name", "sector", "price", "dividend_yield", "per", "roe",
                     "net_margin", "payout_ratio", "debt_equity", "fundamental_score"]].to_csv(index=False)
    st.download_button("Exporter CSV", csv, "brvm_screening.csv", "text/csv")
