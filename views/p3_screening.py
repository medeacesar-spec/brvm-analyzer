"""
Page 3 : Screening multi-criteres
Affiche uniquement les titres avec des donnees exploitables.
"""

import streamlit as st
import pandas as pd

from data.storage import get_all_stocks_for_analysis, get_analyzable_tickers
from analysis.fundamental import compute_ratios, format_ratio


def render():
    st.markdown('<div class="main-header">🎯 Screening Multi-Criteres</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Filtrez les titres BRVM ayant des donnees disponibles</div>', unsafe_allow_html=True)

    all_stocks = get_all_stocks_for_analysis()
    if all_stocks.empty:
        st.warning("Aucune donnee disponible. Lancez l'enrichissement depuis sikafinance.")
        return

    # --- Selection Secteurs & Titres ---
    st.markdown("### Selection de l'univers d'analyse")

    available_sectors = sorted(all_stocks["sector"].dropna().unique().tolist())
    col_sector, col_info = st.columns([3, 1])

    with col_sector:
        selected_sectors = st.multiselect("Secteurs", available_sectors)
        if not selected_sectors:
            selected_sectors = available_sectors

    with col_info:
        st.metric("Titres avec donnees", f"{len(all_stocks)}")

    # Filter by sector
    filtered_stocks = all_stocks[all_stocks["sector"].isin(selected_sectors)]

    # Ticker selection
    ticker_options = [f"{r['ticker']} - {r['company_name']}" + (" 📊" if r.get("has_fundamentals") else " 📈")
                      for _, r in filtered_stocks.iterrows()]
    selected_tickers = st.multiselect(
        f"Titres ({len(filtered_stocks)} disponibles)",
        ticker_options,
    )
    if not selected_tickers:
        selected_tickers = ticker_options
    target_tickers = {s.split(" - ")[0] for s in selected_tickers}

    # --- Ratio Filters ---
    st.markdown("### Filtres fondamentaux")
    col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns(5)

    with col_f1:
        min_yield = st.slider("Yield min (%)", 0.0, 15.0, 0.0, 0.5) / 100
    with col_f2:
        max_per = st.slider("PER max", 0.0, 50.0, 50.0, 1.0)
    with col_f3:
        min_roe = st.slider("ROE min (%)", 0.0, 40.0, 0.0, 1.0) / 100
    with col_f4:
        max_payout = st.slider("Payout max (%)", 0.0, 150.0, 150.0, 5.0) / 100
    with col_f5:
        max_de = st.slider("D/E max", 0.0, 10.0, 10.0, 0.5)

    # --- Compute ratios ---
    st.markdown("---")
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
        st.warning("Aucun titre avec des donnees dans la selection.")
        return

    screen_df = pd.DataFrame(results)

    # Apply ratio filters
    mask = pd.Series(True, index=screen_df.index)
    if min_yield > 0:
        mask &= screen_df["dividend_yield"].fillna(0) >= min_yield
    if max_per < 50:
        mask &= (screen_df["per"].fillna(999) <= max_per) & (screen_df["per"].fillna(0) > 0)
    if min_roe > 0:
        mask &= screen_df["roe"].fillna(0) >= min_roe
    if max_payout < 1.5:
        mask &= screen_df["payout_ratio"].fillna(0) <= max_payout
    if max_de < 10:
        mask &= screen_df["debt_equity"].fillna(0) <= max_de

    filtered = screen_df[mask].sort_values("fundamental_score", ascending=False)

    # --- Results ---
    st.markdown("---")
    st.markdown(f"### {len(filtered)} titre(s) correspondent a vos criteres")

    if filtered.empty:
        st.info("Aucun titre ne correspond. Elargissez vos criteres.")
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
    display_df["data"] = display_df["has_fundamentals"].apply(lambda x: "📊" if x else "📈")

    display_df["beta_fmt"] = display_df.get("beta", pd.Series()).apply(lambda x: f"{x:.2f}" if pd.notna(x) and x else "—")
    display_df["rsi_fmt"] = display_df.get("rsi", pd.Series()).apply(lambda x: f"{x:.0f}" if pd.notna(x) and x else "—")
    display_df["dps_fmt"] = display_df["dps"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")

    show_cols = {
        "data": "", "ticker": "Ticker", "name": "Nom", "sector": "Secteur", "price": "Prix",
        "dividend_yield": "Yield", "dps_fmt": "DPS", "per": "PER", "roe": "ROE",
        "beta_fmt": "Beta", "rsi_fmt": "RSI",
        "checklist": "Check", "score": "Score",
    }

    st.dataframe(
        display_df[list(show_cols.keys())].rename(columns=show_cols),
        use_container_width=True,
        height=min(len(filtered) * 40 + 50, 700),
    )

    csv = filtered[["ticker", "name", "sector", "price", "dividend_yield", "per", "roe",
                     "net_margin", "payout_ratio", "debt_equity", "fundamental_score"]].to_csv(index=False)
    st.download_button("📥 Exporter CSV", csv, "brvm_screening.csv", "text/csv")
