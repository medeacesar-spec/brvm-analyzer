"""
BRVM Analyzer - Application principale Streamlit.
Dashboard d'aide a la decision d'investissement sur la BRVM.
Donnees chargees automatiquement au demarrage.
"""

import time
import streamlit as st

from data.storage import get_connection, init_db, seed_known_report_links

st.set_page_config(
    page_title="BRVM Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: bold;
        color: #2e86c1;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #aab7c4;
        margin-bottom: 2rem;
    }
    .flag-ok { color: #28a745; font-weight: bold; }
    .flag-warn { color: #ffc107; font-weight: bold; }
    .flag-risk { color: #dc3545; font-weight: bold; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { padding: 8px 16px; }
</style>
""", unsafe_allow_html=True)


# ─── AUTO-SYNC AU DEMARRAGE ───
def _sync_daily_quotes():
    """
    Sync rapide : cotations du jour uniquement (~2 secondes).
    Met a jour prix et variation pour les 48 titres.
    """
    from data.scraper import fetch_daily_quotes
    from data.storage import save_market_data
    from config import load_tickers

    try:
        quotes = fetch_daily_quotes()
    except Exception:
        return 0

    tickers = load_tickers()
    ticker_meta = {t["ticker"]: t for t in tickers}

    conn = get_connection()
    updated = 0
    for _, row in quotes.iterrows():
        ticker = row.get("ticker", "")
        price = row.get("last", 0)
        if not ticker or not price:
            continue
        # Update price + variation only (fast)
        existing = conn.execute("SELECT ticker FROM market_data WHERE ticker=?", (ticker,)).fetchone()
        if existing:
            conn.execute(
                "UPDATE market_data SET price=?, variation=?, updated_at=CURRENT_TIMESTAMP WHERE ticker=?",
                (price, row.get("variation", 0), ticker),
            )
        else:
            meta = ticker_meta.get(ticker, {})
            conn.execute(
                """INSERT INTO market_data (ticker, company_name, sector, price, variation, updated_at)
                   VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (ticker, meta.get("name", ""), meta.get("sector", ""), price, row.get("variation", 0)),
            )
        updated += 1
    conn.commit()
    conn.close()
    return updated


def _sync_full_details():
    """
    Sync complet : cotations + details (beta, RSI, dividendes) pour chaque titre.
    Utilise uniquement au premier lancement ou quand l'utilisateur force le refresh.
    ~30 secondes pour 48 titres.
    """
    from data.scraper import fetch_daily_quotes, fetch_stock_details, fetch_sector_indices
    from data.storage import save_market_data
    from config import load_tickers

    init_db()
    tickers = load_tickers()

    # 1. Cotations du jour (rapide)
    try:
        quotes = fetch_daily_quotes()
        price_map = dict(zip(quotes["ticker"], quotes["last"]))
        var_map = dict(zip(quotes["ticker"], quotes["variation"]))
    except Exception:
        price_map = {}
        var_map = {}

    # 2. Details par titre (lent mais complet)
    ok = 0
    for t in tickers:
        ticker = t["ticker"]
        try:
            details = fetch_stock_details(ticker)
            if "error" in details:
                continue
            dps = None
            div_hist = details.get("dividend_history", [])
            if div_hist:
                latest = max(div_hist, key=lambda d: d.get("year", 0))
                dps = latest.get("amount")
            price = price_map.get(ticker) or details.get("price") or 0
            save_market_data({
                "ticker": ticker, "name": t["name"], "sector": t["sector"],
                "price": price, "variation": var_map.get(ticker, 0),
                "market_cap": details.get("market_cap"),
                "beta": details.get("beta"), "rsi": details.get("rsi"),
                "dps": dps, "dividend_history": div_hist,
            })
            ok += 1
        except Exception:
            pass
        time.sleep(0.3)

    # 3. Indices
    try:
        indices = fetch_sector_indices()
        conn = get_connection()
        conn.execute("CREATE TABLE IF NOT EXISTS indices_cache (name TEXT PRIMARY KEY, value REAL, variation REAL, updated_at TIMESTAMP)")
        for _, idx in indices.iterrows():
            conn.execute("INSERT OR REPLACE INTO indices_cache (name, value, variation, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                         (idx["name"], idx.get("value"), idx.get("variation")))
        conn.commit()
        conn.close()
    except Exception:
        pass

    # 4. Rapports
    seed_known_report_links()
    return ok


def _sync_incremental_prices():
    """
    Telecharge les prix historiques manquants uniquement
    (depuis la derniere date en cache jusqu'a aujourd'hui).
    """
    from data.scraper import fetch_historical_prices
    from data.storage import get_cached_prices, cache_prices
    from config import load_tickers
    from datetime import datetime, timedelta

    tickers = load_tickers()
    today = datetime.now()
    updated = 0

    for t in tickers:
        ticker = t["ticker"]
        existing = get_cached_prices(ticker)
        if existing.empty:
            # Pas de cache → telecharger 3 mois pour commencer
            start = today - timedelta(days=90)
        else:
            last_date = existing["date"].max()
            if hasattr(last_date, "date"):
                last_date = last_date.date()
            days_missing = (today.date() - last_date).days
            if days_missing <= 1:
                continue  # Deja a jour
            start = datetime.combine(last_date + timedelta(days=1), datetime.min.time())

        try:
            df = fetch_historical_prices(
                ticker,
                start_date=start.strftime("%Y-%m-%d"),
                end_date=today.strftime("%Y-%m-%d"),
            )
            if not df.empty:
                cache_prices(ticker, df)
                updated += 1
        except Exception:
            pass
        time.sleep(0.3)

    return updated


def _check_data_status():
    """Verifie l'etat des donnees en base. Retourne (count, is_fresh)."""
    try:
        conn = get_connection()
        count = conn.execute("SELECT COUNT(*) FROM market_data WHERE price > 0").fetchone()[0]
        updated = conn.execute("SELECT MAX(updated_at) FROM market_data").fetchone()[0]
        conn.close()
        if count == 0:
            return 0, False
        # Donnees fraiches si mises a jour aujourd'hui
        if updated:
            from datetime import datetime
            try:
                last_update = datetime.strptime(str(updated)[:19], "%Y-%m-%d %H:%M:%S")
                is_fresh = (datetime.now() - last_update).total_seconds() < 86400
                return count, is_fresh
            except Exception:
                pass
        return count, False
    except Exception:
        return 0, False


# ─── CHARGEMENT INITIAL ───
count, is_fresh = _check_data_status()

if count == 0:
    # Premiere utilisation : sync complet obligatoire (~30s)
    st.markdown("## 📊 BRVM Analyzer")
    st.markdown("### Premier lancement - Chargement des donnees...")
    st.markdown("Cette operation prend ~30 secondes et ne se fait qu'une seule fois.")
    with st.spinner("Telechargement des 48 titres BRVM..."):
        result = _sync_full_details()
    st.success(f"✅ {result} titres charges !")
    # Aussi charger les prix recents
    with st.spinner("Chargement des prix historiques recents..."):
        _sync_incremental_prices()
    st.rerun()
elif not is_fresh and "sync_done" not in st.session_state:
    # Donnees existantes → sync incremental rapide (~2-5s)
    with st.spinner("Mise a jour des cotations du jour..."):
        _sync_daily_quotes()
        _sync_incremental_prices()
    st.session_state.sync_done = True
# Si donnees fraiches (< 24h) → pas de sync, chargement instantane


# ─── SIDEBAR ───
st.sidebar.markdown("## 📊 BRVM Analyzer")

# Data status
conn = get_connection()
try:
    count = conn.execute("SELECT COUNT(*) FROM market_data WHERE price > 0").fetchone()[0]
    updated = conn.execute("SELECT MAX(updated_at) FROM market_data").fetchone()[0]
except Exception:
    count = 0
    updated = "?"
conn.close()

st.sidebar.caption(f"✅ {count} titres | MAJ: {str(updated)[:16] if updated else '?'}")

# Refresh button
col_r1, col_r2 = st.sidebar.columns(2)
if col_r1.button("🔄 Cotations"):
    with st.spinner("Mise a jour..."):
        _sync_daily_quotes()
    st.rerun()
if col_r2.button("🔄 Complet"):
    with st.spinner("Sync complet..."):
        _sync_full_details()
        _sync_incremental_prices()
    st.rerun()

st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    [
        "🏠 Dashboard Marche",
        "🔍 Analyse d'un Titre",
        "🎯 Screening",
        "⚖️ Comparateur",
        "📡 Signaux",
        "💼 Portefeuille",
        "🤖 Assistant Investisseur",
        "📅 Publications & Qualitative",
    ],
    index=0,
)

# Import and run the selected page
if page == "🏠 Dashboard Marche":
    from views.p1_dashboard import render
    render()
elif page == "🔍 Analyse d'un Titre":
    from views.p2_stock_analysis import render
    render()
elif page == "🎯 Screening":
    from views.p3_screening import render
    render()
elif page == "⚖️ Comparateur":
    from views.p4_comparator import render
    render()
elif page == "📡 Signaux":
    from views.p5_signals import render
    render()
elif page == "💼 Portefeuille":
    from views.p6_portfolio import render
    render()
elif page == "🤖 Assistant Investisseur":
    from views.p7_assistant import render
    render()
elif page == "📅 Publications & Qualitative":
    from views.p8_publications import render
    render()

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown(
    "<small>BRVM Analyzer v1.0<br>"
    "Donnees: sikafinance.com<br>"
    "⚠️ Outil d'aide a la decision uniquement</small>",
    unsafe_allow_html=True,
)
