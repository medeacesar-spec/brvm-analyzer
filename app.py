"""
BRVM Analyzer - Application principale Streamlit.
Dashboard d'aide a la decision d'investissement sur la BRVM.
Données chargées automatiquement au démarrage.
"""

import logging
import time
import streamlit as st

from data.storage import get_connection, init_db, seed_known_report_links
from utils.auth import render_auth_widget, require_login, is_admin

# Logger global : remontent dans les logs Streamlit Cloud (stderr)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
_log = logging.getLogger("brvm_analyzer")

st.set_page_config(
    page_title="BRVM Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS — Modern dashboard design (light theme, rounded cards, clean typography)
st.markdown("""
<style>
    /* ── Global ── */
    .stApp {
        background-color: #F5F6FA;
    }
    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
        max-width: 1400px;
    }

    /* ── Headers ── */
    .main-header {
        font-size: 1.6rem;
        font-weight: 700;
        color: #1B2559;
        margin-bottom: 0.2rem;
        letter-spacing: -0.02em;
    }
    .sub-header {
        font-size: 0.95rem;
        color: #8F9BBA;
        margin-bottom: 1.5rem;
    }

    /* ── KPI cards ── */
    [data-testid="stMetric"] {
        background: #FFFFFF;
        border-radius: 16px;
        padding: 16px 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        border: 1px solid #E9EDF7;
    }
    [data-testid="stMetricLabel"] {
        color: #8F9BBA;
        font-size: 0.82rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    [data-testid="stMetricValue"] {
        color: #1B2559;
        font-size: 1.5rem;
        font-weight: 700;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.85rem;
        font-weight: 600;
    }

    /* ── Cards (expanders, containers) ── */
    [data-testid="stExpander"] {
        background: #FFFFFF;
        border-radius: 14px;
        border: 1px solid #E9EDF7;
        box-shadow: 0 2px 8px rgba(0,0,0,0.03);
        margin-bottom: 0.5rem;
    }
    [data-testid="stExpander"] summary {
        font-weight: 600;
        color: #1B2559;
    }

    /* ── DataFrames / Tables ── */
    [data-testid="stDataFrame"] {
        background: #FFFFFF;
        border-radius: 14px;
        border: 1px solid #E9EDF7;
        box-shadow: 0 2px 8px rgba(0,0,0,0.03);
        overflow: hidden;
    }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: #FFFFFF;
        border-radius: 12px;
        padding: 4px;
        border: 1px solid #E9EDF7;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 8px 18px;
        font-weight: 600;
        font-size: 0.88rem;
        color: #8F9BBA;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: #4318FF;
        color: #FFFFFF;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: #1B2559;
        border-right: none;
    }
    [data-testid="stSidebar"] * {
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3,
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown small,
    [data-testid="stSidebar"] .stMarkdown span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stRadio label span,
    [data-testid="stSidebar"] p {
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] .stRadio label {
        color: #FFFFFF !important;
        font-weight: 500;
        padding: 6px 0;
    }
    [data-testid="stSidebar"] .stRadio label:hover {
        color: #FFFFFF !important;
        background: rgba(255,255,255,0.1);
        border-radius: 8px;
    }
    [data-testid="stSidebar"] [data-testid="stMetric"] {
        background: rgba(255,255,255,0.08);
        border: 1px solid rgba(255,255,255,0.12);
    }
    [data-testid="stSidebar"] [data-testid="stMetricLabel"] {
        color: #C4C9DE !important;
    }
    [data-testid="stSidebar"] [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] hr {
        border-color: rgba(255,255,255,0.15);
    }
    [data-testid="stSidebar"] .stCaption,
    [data-testid="stSidebar"] .stCaption p {
        color: #C4C9DE !important;
    }
    [data-testid="stSidebar"] .stButton button {
        color: #FFFFFF !important;
    }

    /* ── Buttons ── */
    .stButton > button {
        background: #4318FF;
        color: white;
        border: none;
        border-radius: 10px;
        font-weight: 600;
        font-size: 0.85rem;
        padding: 8px 16px;
        transition: all 0.2s;
    }
    .stButton > button:hover {
        background: #3311CC;
        box-shadow: 0 4px 12px rgba(67,24,255,0.3);
    }
    [data-testid="stSidebar"] .stButton > button {
        background: rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.15);
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: rgba(255,255,255,0.2);
    }

    /* ── Sidebar expander : fond transparent, texte blanc lisible ── */
    [data-testid="stSidebar"] [data-testid="stExpander"],
    [data-testid="stSidebar"] details {
        background: rgba(255,255,255,0.08) !important;
        border: 1px solid rgba(255,255,255,0.15) !important;
        box-shadow: none !important;
    }
    /* Header (summary) : on force le fond transparent par-dessus le blanc de Streamlit */
    [data-testid="stSidebar"] details > summary,
    [data-testid="stSidebar"] [data-testid="stExpander"] summary,
    [data-testid="stSidebar"] [data-testid="stExpanderToggleIcon"],
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] > div:first-child,
    [data-testid="stSidebar"] [data-baseweb="accordion"],
    [data-testid="stSidebar"] [data-baseweb="accordion"] > div {
        background: transparent !important;
        background-color: transparent !important;
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] details > summary *,
    [data-testid="stSidebar"] [data-testid="stExpander"] * {
        color: #FFFFFF !important;
    }
    /* Inputs dans sidebar : garder fond blanc mais texte sombre lisible */
    [data-testid="stSidebar"] input[type="text"],
    [data-testid="stSidebar"] input[type="email"],
    [data-testid="stSidebar"] input[type="password"],
    [data-testid="stSidebar"] input[type="number"] {
        background: #FFFFFF !important;
        color: #1B2559 !important;
    }
    [data-testid="stSidebar"] input::placeholder {
        color: #8F9BBA !important;
    }

    /* ── Selectbox / Inputs ── */
    .stSelectbox > div > div {
        border-radius: 10px;
        border: 1px solid #E9EDF7;
        background: #FFFFFF;
    }
    .stSlider > div > div {
        color: #4318FF;
    }

    /* ── Charts ── */
    [data-testid="stPlotlyChart"] {
        background: #FFFFFF;
        border-radius: 14px;
        border: 1px solid #E9EDF7;
        box-shadow: 0 2px 8px rgba(0,0,0,0.03);
        padding: 8px;
    }

    /* ── Progress bars ── */
    .stProgress > div > div {
        background: #4318FF;
        border-radius: 8px;
    }

    /* ── Flag badges ── */
    .flag-ok { color: #05CD99; font-weight: bold; }
    .flag-warn { color: #FFB547; font-weight: bold; }
    .flag-risk { color: #EE5D50; font-weight: bold; }

    /* ── Subheaders ── */
    h2, h3, .stSubheader {
        color: #1B2559 !important;
        font-weight: 700;
    }

    /* ── Horizontal rules ── */
    hr {
        border-color: #E9EDF7;
        margin: 1rem 0;
    }

    /* ── Info / Warning boxes ── */
    .stAlert {
        border-radius: 12px;
        border: none;
    }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: #F5F6FA; }
    ::-webkit-scrollbar-thumb { background: #C4C9DE; border-radius: 3px; }
</style>
""", unsafe_allow_html=True)


# ─── AUTO-SYNC AU DEMARRAGE ───
def _sync_daily_quotes():
    """
    Sync rapide : cotations du jour uniquement (~2 secondes).
    Met a jour prix et variation pour les 48 titres,
    ET append aujourd'hui dans price_cache (close/high/low/open/volume).
    """
    from data.scraper import fetch_daily_quotes
    from config import load_tickers
    from datetime import datetime

    try:
        quotes = fetch_daily_quotes()
    except Exception:
        return 0

    tickers = load_tickers()
    ticker_meta = {t["ticker"]: t for t in tickers}
    today_str = datetime.now().strftime("%Y-%m-%d")

    conn = get_connection()
    updated = 0
    for _, row in quotes.iterrows():
        ticker = row.get("ticker", "")
        price = row.get("last", 0) or 0
        if not ticker or not price:
            continue
        # Update market_data
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

        # Also upsert today's row into price_cache so historical analyses reflect the latest close
        open_p = row.get("open", 0) or price
        high_p = row.get("high", 0) or price
        low_p = row.get("low", 0) or price
        volume = row.get("volume_shares", 0) or 0
        conn.execute(
            """INSERT INTO price_cache (ticker, date, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(ticker, date) DO UPDATE SET
                 open=excluded.open, high=excluded.high, low=excluded.low,
                 close=excluded.close, volume=excluded.volume""",
            (ticker, today_str, open_p, high_p, low_p, price, volume),
        )
    conn.commit()
    conn.close()
    return updated


def _scrape_brvm_indices():
    """Scrape les 12 indices BRVM depuis brvm.org/fr/indices."""
    import re
    import requests
    from bs4 import BeautifulSoup

    resp = requests.get(
        "https://www.brvm.org/fr/indices",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30, verify=False,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    def _parse_num(text):
        if not text:
            return None
        c = text.replace("\xa0", "").replace(" ", "")
        if "," in c:
            c = c.replace(".", "").replace(",", ".")
        c = re.sub(r"[^\d.\-]", "", c)
        try:
            return float(c)
        except (ValueError, TypeError):
            return None

    indices = []
    for table in soup.find_all("table", class_="table"):
        thead = table.find("thead")
        if not thead or "Fermeture" not in thead.get_text():
            continue
        tbody = table.find("tbody")
        if not tbody:
            continue
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue
            name = cells[0].get_text(strip=True)
            close = _parse_num(cells[2].get_text(strip=True))
            var_span = cells[3].find("span", class_=["text-bad", "text-good"])
            variation = _parse_num(var_span.get_text(strip=True)) if var_span else None
            if var_span and "text-bad" in var_span.get("class", []) and variation and variation > 0:
                variation = -variation
            ytd = None
            if len(cells) >= 5:
                ytd_span = cells[4].find("span", class_=["text-bad", "text-good"])
                ytd = _parse_num(ytd_span.get_text(strip=True)) if ytd_span else None
                if ytd_span and "text-bad" in ytd_span.get("class", []) and ytd and ytd > 0:
                    ytd = -ytd
            if name and close is not None:
                cat = "total_return" if "TOTAL RETURN" in name.upper() else \
                      "principal" if any(s in name.upper() for s in ["COMPOSITE", "BRVM-30", "PRESTIGE", "PRINCIPAL"]) else \
                      "sectoriel"
                indices.append((name, close, variation, ytd, cat))

    if not indices:
        return

    conn = get_connection()
    # Ensure columns exist (idempotent, tolerant des drivers abortant la txn).
    try:
        conn.execute("SELECT prev_close FROM indices_cache LIMIT 1")
    except Exception:
        # Postgres abort la transaction sur un SELECT en erreur → rollback obligatoire
        try:
            conn.rollback()
        except Exception:
            pass
        for col, ctype in [("prev_close", "REAL"), ("ytd_variation", "REAL"), ("category", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE indices_cache ADD COLUMN {col} {ctype}")
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
    conn.execute("DELETE FROM indices_cache")
    for name, close, var, ytd, cat in indices:
        conn.execute(
            "INSERT INTO indices_cache (name, value, variation, ytd_variation, category, updated_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
            (name, close, var, ytd, cat),
        )
    conn.commit()
    conn.close()


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
        except Exception as e:
            _log.exception("save_market_data failed for %s: %s", ticker, e)
        time.sleep(0.3)

    # 3. Indices (from brvm.org — 12 indices: principaux + sectoriels + total return)
    try:
        _scrape_brvm_indices()
    except Exception:
        # Fallback to sikafinance indices
        try:
            indices = fetch_sector_indices()
            conn = get_connection()
            conn.execute("CREATE TABLE IF NOT EXISTS indices_cache (name TEXT PRIMARY KEY, value REAL, variation REAL, updated_at TIMESTAMP)")
            for _, idx in indices.iterrows():
                conn.execute(
                    """INSERT INTO indices_cache (name, value, variation, updated_at)
                       VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                       ON CONFLICT(name) DO UPDATE SET
                         value=excluded.value, variation=excluded.variation,
                         updated_at=CURRENT_TIMESTAMP""",
                    (idx["name"], idx.get("value"), idx.get("variation")),
                )
            conn.commit()
            conn.close()
        except Exception:
            pass

    # 4. Rapports
    seed_known_report_links()
    return ok


def _sync_incremental_prices():
    """
    Telecharge les prix historiques manquants.
    - Si pas de cache : charge 5 ans de données mensuelles via l'API JSON (rapide, 1 requete)
    - Si cache existant : incremental journalier depuis la derniere date
    """
    from data.scraper import fetch_historical_prices, fetch_historical_prices_page
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
            # Pas de cache → télécharger 5 ans de mensuel via API (1 requete, ~60 rows)
            try:
                df = fetch_historical_prices_page(ticker, period="mensuel", years_back=5)
                if not df.empty:
                    cache_prices(ticker, df)
                    updated += 1
            except Exception as e:
                _log.exception("cache_prices initial failed for %s: %s", ticker, e)
            time.sleep(0.3)
        else:
            last_date = existing["date"].max()
            if hasattr(last_date, "date"):
                last_date = last_date.date()
            days_missing = (today.date() - last_date).days
            if days_missing < 1:
                continue  # Déjà à jour (cache contient aujourd'hui ou plus récent)
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
            except Exception as e:
                _log.exception("cache_prices incremental failed for %s: %s", ticker, e)
            time.sleep(0.3)

    return updated


def _check_data_status():
    """Verifie l'etat des données en base. Retourne (count, is_fresh).

    Logique de fraîcheur adaptée au cycle BRVM :
    - BRVM clôture ≈ 15h00 WAT. On considère les données à jour pour la journée
      seulement si `updated_at` est postérieur à 15h30 du dernier jour ouvré.
    - Samedi/Dimanche : le dernier jour ouvré est vendredi.
    - En semaine avant 15h30 : le dernier jour ouvré est hier (lundi → vendredi précédent).
    - En semaine après 15h30 : le dernier jour ouvré est aujourd'hui.
    """
    from datetime import datetime, timedelta
    try:
        conn = get_connection()
        count = conn.execute("SELECT COUNT(*) FROM market_data WHERE price > 0").fetchone()[0]
        updated = conn.execute("SELECT MAX(updated_at) FROM market_data").fetchone()[0]
        conn.close()
        if count == 0:
            return 0, False
        if not updated:
            return count, False

        try:
            last_update = datetime.strptime(str(updated)[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return count, False

        now = datetime.now()
        # BRVM close + buffer de publication
        close_hour, close_minute = 15, 30

        # Déterminer la date de clôture attendue la plus récente
        today = now.date()
        weekday = today.weekday()  # Mon=0 ... Sun=6

        if weekday == 5:  # Samedi → dernière clôture = vendredi
            ref_date = today - timedelta(days=1)
        elif weekday == 6:  # Dimanche → vendredi
            ref_date = today - timedelta(days=2)
        else:
            # Jour ouvré
            after_close = (now.hour, now.minute) >= (close_hour, close_minute)
            if after_close:
                ref_date = today
            else:
                # Reculer au dernier jour ouvré précédent
                delta = 1
                if weekday == 0:  # Lundi avant clôture → vendredi précédent
                    delta = 3
                ref_date = today - timedelta(days=delta)

        required_ts = datetime.combine(
            ref_date, datetime.min.time()
        ).replace(hour=close_hour, minute=close_minute)

        is_fresh = last_update >= required_ts
        return count, is_fresh
    except Exception:
        return 0, False


# ─── CHARGEMENT INITIAL ───
# Vérification unique par session : dès qu'on a vu des données en base,
# on ne relance JAMAIS de sync automatique (évite les boucles sur Streamlit Cloud
# où chaque navigation ré-exécute app.py).
if not st.session_state.get("db_verified"):
    count, is_fresh = _check_data_status()

    if count > 0:
        # DB OK → on mémorise définitivement pour cette session
        st.session_state.db_verified = True
        # Sync incrémental optionnel, seulement si vraiment pas frais
        if not is_fresh and not st.session_state.get("sync_done"):
            # Page de garde pendant la mise à jour quotidienne.
            st.markdown("## 📊 BRVM Analyzer")
            st.info(
                "⏳ **Mise à jour des cotations en cours**\n\n"
                "Récupération des prix du jour et des historiques manquants "
                "(sikafinance.com). Cela peut prendre **1 à 3 minutes** selon le nombre "
                "de titres à rafraîchir. Cette opération ne se fait qu'une fois par session.\n\n"
                "_Merci de patienter — la page se rechargera automatiquement._"
            )
            with st.spinner("Téléchargement des cotations…"):
                try:
                    _sync_daily_quotes()
                    _sync_incremental_prices()
                except Exception as _e:
                    _log.exception("Daily sync failed: %s", _e)
                    st.session_state.daily_sync_error = f"{type(_e).__name__}: {_e}"
            st.session_state.sync_done = True
            st.rerun()
        # Indices BRVM : rafraîchissement léger une fois par session, pour TOUS
        # (1 requête HTTP ~1s, indépendant de la fraîcheur des cotations).
        if not st.session_state.get("indices_synced"):
            try:
                _scrape_brvm_indices()
                st.session_state.indices_error = None
            except Exception as _e:
                st.session_state.indices_error = f"{type(_e).__name__}: {_e}"
            st.session_state.indices_synced = True
    else:
        # count == 0 : vraiment vide (ou erreur de connexion)
        # On tente un sync complet MAIS on ne boucle pas indéfiniment
        if st.session_state.get("full_sync_attempted"):
            st.error(
                "⚠️ Impossible de charger les données. "
                "Problème de connexion à la base ? Rechargez la page ou contactez l'admin."
            )
            st.stop()
        st.session_state.full_sync_attempted = True

        st.markdown("## 📊 BRVM Analyzer")
        st.markdown("### Premier lancement — Chargement des données…")
        st.markdown(
            "Cette opération prend **quelques minutes** (scraping de sikafinance "
            "pour les 48 titres BRVM + prix historiques). Elle ne se fait qu'une seule fois."
        )
        with st.spinner("Téléchargement des 48 titres BRVM…"):
            try:
                result = _sync_full_details()
                st.success(f"✅ {result} titres chargés !")
            except Exception as e:
                st.error(f"Erreur scraping : {e}")
                st.stop()
        with st.spinner("Chargement des prix historiques récents…"):
            try:
                _sync_incremental_prices()
            except Exception:
                pass
        st.session_state.db_verified = True
        st.rerun()

# ─── Revue mensuelle automatique des poids de calibration ───
if "calibration_review_checked" not in st.session_state:
    try:
        from analysis.calibration import is_review_due, run_monthly_review
        if is_review_due():
            result = run_monthly_review(force=False, notes="Auto (démarrage)")
            if not result.get("skipped"):
                st.sidebar.info(
                    f"⚖️ Revue mensuelle effectuée : "
                    f"{result.get('calibrated_signals', 0)} signaux, "
                    f"{result.get('calibrated_recos', 0)} verdicts calibrés."
                )
    except Exception:
        pass
    st.session_state.calibration_review_checked = True


# ─── SIDEBAR ───
st.sidebar.markdown(
    "<div style='text-align:center;padding:1rem 0 0.5rem;'>"
    "<span style='font-size:1.4rem;font-weight:800;color:#FFFFFF;letter-spacing:-0.02em;'>📊 BRVM</span><br>"
    "<span style='font-size:0.85rem;color:#8F9BBA;'>Analyzer</span></div>",
    unsafe_allow_html=True,
)

# Data status
conn = get_connection()
try:
    count = conn.execute("SELECT COUNT(*) FROM market_data WHERE price > 0").fetchone()[0]
    updated = conn.execute("SELECT MAX(updated_at) FROM market_data").fetchone()[0]
except Exception:
    count = 0
    updated = "?"
conn.close()

st.sidebar.caption(f"{count} titres | MAJ: {str(updated)[:16] if updated else '?'}")

# Debug indices + daily sync (admin uniquement)
_idx_err = st.session_state.get("indices_error")
_sync_err = st.session_state.get("daily_sync_error")
if (_idx_err or _sync_err) and is_admin():
    if _idx_err:
        st.sidebar.error(f"⚠️ Indices : {_idx_err}")
    if _sync_err:
        st.sidebar.error(f"⚠️ Sync : {_sync_err}")
    if st.sidebar.button("🔄 Retenter", key="retry_all_btn"):
        try:
            _scrape_brvm_indices()
            st.session_state.indices_error = None
        except Exception as _e:
            st.session_state.indices_error = f"{type(_e).__name__}: {_e}"
        try:
            _sync_daily_quotes()
            _sync_incremental_prices()
            st.session_state.daily_sync_error = None
        except Exception as _e:
            st.session_state.daily_sync_error = f"{type(_e).__name__}: {_e}"
        st.rerun()

# Refresh buttons — admin connecté uniquement (pas en mode local implicite)
from utils.auth import is_logged_in as _is_logged_in
if is_admin() and _is_logged_in():
    col_r1, col_r2 = st.sidebar.columns(2)
    if col_r1.button("🔄 Cotations"):
        with st.spinner("Mise à jour..."):
            _sync_daily_quotes()
        st.rerun()
    if col_r2.button("🔄 Complet"):
        with st.spinner("Sync complet..."):
            _sync_full_details()
            _sync_incremental_prices()
        st.rerun()

st.sidebar.markdown("---")

# Widget authentification (connexion Google OAuth ou mode dev)
render_auth_widget()
st.sidebar.markdown("---")

_PAGE_OPTIONS = [
    "🏠 Dashboard Marché",
    "🔍 Analyse d'un Titre",
    "🎯 Screening",
    "⚖️ Comparateur",
    "📡 Signaux",
    "💼 Portefeuille",
    "🤖 Assistant Investisseur",
    "📈 Performance des titres",
    "🎯 Historique Signaux & Recommandations",
    "📅 Infos Générales Marché",
]

# If another page requested navigation (e.g. a ticker-link button), honor it.
_pending_page = st.session_state.pop("pending_page", None)
if _pending_page and _pending_page in _PAGE_OPTIONS:
    st.session_state["nav_radio"] = _pending_page
elif "nav_radio" not in st.session_state:
    # Initialize default only the first time, before the widget is created
    st.session_state["nav_radio"] = _PAGE_OPTIONS[0]

# IMPORTANT: when using `key=`, don't also pass `index=`; Streamlit will warn.
page = st.sidebar.radio(
    "Navigation",
    _PAGE_OPTIONS,
    key="nav_radio",
)

# Import and run the selected page
if page == "🏠 Dashboard Marché":
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
    if require_login("le suivi de portefeuille"):
        from views.p6_portfolio import render
        render()
elif page == "🤖 Assistant Investisseur":
    if require_login("l'Assistant Investisseur"):
        from views.p7_assistant import render
        render()
elif page == "📈 Performance des titres":
    from views.p9_performance import render
    render()
elif page == "🎯 Historique Signaux & Recommandations":
    from views.p10_calibration import render
    render()
elif page == "📅 Infos Générales Marché":
    from views.p8_publications import render
    render()

# Footer
st.sidebar.markdown("---")

# Stop app button with confirmation — admin only
if "confirm_shutdown" not in st.session_state:
    st.session_state.confirm_shutdown = False

if is_admin() and _is_logged_in() and not st.session_state.confirm_shutdown:
    if st.sidebar.button("🛑 Arrêter l'application", use_container_width=True):
        st.session_state.confirm_shutdown = True
        st.rerun()
elif st.session_state.confirm_shutdown:
    st.sidebar.warning("Confirmer l'arrêt ?")
    col_yes, col_no = st.sidebar.columns(2)
    with col_yes:
        if st.button("✅ Oui", key="shutdown_yes", use_container_width=True):
            import os, subprocess
            st.sidebar.success("Arrêt en cours… vous pouvez fermer cet onglet.")
            # Kill THIS process (the Streamlit server itself) from a detached
            # background shell 1 sec later, so the response has time to reach the browser.
            my_pid = os.getpid()
            subprocess.Popen(
                ["/bin/bash", "-c", f"sleep 1 && kill -9 {my_pid}"],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            st.stop()
    with col_no:
        if st.button("❌ Non", key="shutdown_no", use_container_width=True):
            st.session_state.confirm_shutdown = False
            st.rerun()

from data.db import db_info as _db_info
_info = _db_info()
_db_label = "🗄️ SQLite local" if _info["type"] == "sqlite" else "☁️ Postgres cloud"
if _info["type"] == "sqlite" and _info.get("size_mb"):
    _db_label += f" ({_info['size_mb']} MB)"

st.sidebar.markdown(
    "<div style='text-align:center;padding:0.5rem 0;'>"
    "<span style='font-size:0.75rem;color:#8F9BBA;'>BRVM Analyzer v1.0</span><br>"
    "<span style='font-size:0.7rem;color:#6A7199;'>Données: sikafinance.com</span><br>"
    f"<span style='font-size:0.65rem;color:#6A7199;'>{_db_label}</span></div>",
    unsafe_allow_html=True,
)
