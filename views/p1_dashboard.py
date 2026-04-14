"""
Page 1 : Dashboard Marché BRVM
Lit uniquement depuis la base SQLite locale (pre-chargee par app.py au demarrage).
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from data.storage import get_connection, get_cached_prices


def _load_quotes_from_db() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        """SELECT ticker, company_name as name, sector, price as last,
           variation, market_cap, beta, rsi, dps, updated_at
           FROM market_data WHERE price > 0 ORDER BY ticker""",
        conn,
    )
    conn.close()
    return df


def _load_indices_from_db() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT name, value, variation FROM indices_cache", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def _compute_period_performance(quotes: pd.DataFrame) -> dict:
    results = {"day": [], "week": [], "month": []}
    today = datetime.now()
    days_since_monday = today.weekday()
    last_friday = today - timedelta(days=days_since_monday + 3)
    last_monday = last_friday - timedelta(days=4)
    month_ago = today - timedelta(days=30)

    for _, row in quotes.iterrows():
        ticker, name, last_price = row.get("ticker", ""), row.get("name", ""), row.get("last", 0)
        if not ticker or not last_price:
            continue
        day_var = row.get("variation", 0) or 0
        results["day"].append({"ticker": ticker, "name": name, "price": last_price, "variation": day_var})

        prices = get_cached_prices(ticker)
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

    return {k: pd.DataFrame(v) for k, v in results.items()}


def _render_top5(df: pd.DataFrame, label: str):
    if df.empty or "variation" not in df.columns:
        return
    col_top, col_bottom = st.columns(2)
    positive = df[df["variation"] > 0.01]
    negative = df[df["variation"] < -0.01]
    with col_top:
        st.markdown(f"**📈 Hausses {label}**")
        if not positive.empty:
            for _, row in positive.nlargest(5, "variation").iterrows():
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.write(f"**{row['name']}**")
                c2.write(f"{row['price']:,.0f}")
                c3.markdown(f"<span style='color:#28a745'>+{row['variation']:.2f}%</span>", unsafe_allow_html=True)
        else:
            st.caption("Aucune hausse")
    with col_bottom:
        st.markdown(f"**📉 Baisses {label}**")
        if not negative.empty:
            for _, row in negative.nsmallest(5, "variation").iterrows():
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.write(f"**{row['name']}**")
                c2.write(f"{row['price']:,.0f}")
                c3.markdown(f"<span style='color:#dc3545'>{row['variation']:.2f}%</span>", unsafe_allow_html=True)
        else:
            st.caption("Aucune baisse")


def render():
    st.markdown('<div class="main-header">🏠 Dashboard Marche BRVM</div>', unsafe_allow_html=True)

    quotes = _load_quotes_from_db()
    if quotes.empty:
        st.warning("Donnees en cours de chargement... Patientez quelques secondes puis rafraichissez.")
        return

    # --- KPIs ---
    col1, col2, col3, col4 = st.columns(4)
    positive = quotes[quotes["variation"] > 0] if "variation" in quotes.columns else pd.DataFrame()
    negative = quotes[quotes["variation"] < 0] if "variation" in quotes.columns else pd.DataFrame()

    col1.metric("Titres en hausse", f"{len(positive)}")
    col2.metric("Titres en baisse", f"{len(negative)}")
    col3.metric("Titres stables", f"{len(quotes) - len(positive) - len(negative)}")
    total_mcap = quotes["market_cap"].sum() if "market_cap" in quotes.columns else 0
    col4.metric("Capitalisation", f"{total_mcap/1e6:,.0f} Mds" if total_mcap else "N/A")

    st.markdown("---")

    # --- Top 5 sur 3 horizons ---
    perf = _compute_period_performance(quotes)
    tab_day, tab_week, tab_month = st.tabs(["📅 Dernier jour", "📆 Derniere semaine", "🗓️ Dernier mois"])
    with tab_day:
        _render_top5(perf.get("day", pd.DataFrame()), "du jour")
    with tab_week:
        week_df = perf.get("week", pd.DataFrame())
        if not week_df.empty and week_df["variation"].abs().sum() > 0:
            _render_top5(week_df, "de la semaine")
        else:
            st.info("Prix historiques en cours de chargement...")
    with tab_month:
        month_df = perf.get("month", pd.DataFrame())
        if not month_df.empty and month_df["variation"].abs().sum() > 0:
            _render_top5(month_df, "du mois")
        else:
            st.info("Prix historiques en cours de chargement...")

    st.markdown("---")

    # --- Tableau complet ---
    st.subheader("📋 Toutes les cotations")
    sectors = ["Tous"] + sorted(quotes["sector"].dropna().unique().tolist())
    selected_sector = st.selectbox("Filtrer par secteur", sectors)
    display_df = quotes[quotes["sector"] == selected_sector] if selected_sector != "Tous" else quotes

    show_cols = {"ticker": "Ticker", "name": "Nom", "sector": "Secteur",
                 "last": "Prix (FCFA)", "variation": "Var (%)",
                 "market_cap": "Cap (M)", "beta": "Beta", "rsi": "RSI", "dps": "DPS"}
    available = {k: v for k, v in show_cols.items() if k in display_df.columns}
    st.dataframe(display_df[list(available.keys())].rename(columns=available), use_container_width=True, height=600)

    # --- Indices ---
    st.markdown("---")
    st.subheader("📊 Indices BRVM")
    indices = _load_indices_from_db()
    if not indices.empty:
        cols = st.columns(min(len(indices), 4))
        for i, (_, idx) in enumerate(indices.iterrows()):
            with cols[i % 4]:
                delta_str = f"{idx['variation']:.2f}%" if pd.notna(idx.get("variation")) else None
                st.metric(idx["name"], f"{idx['value']:,.0f}" if pd.notna(idx.get("value")) else "—", delta=delta_str)
    else:
        st.info("Indices non disponibles")
