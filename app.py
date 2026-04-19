"""
BRVM Analyzer - Application principale Streamlit.
Dashboard d'aide a la decision d'investissement sur la BRVM.
Données chargées automatiquement au démarrage.
"""

import logging
import time
from pathlib import Path

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

# ─── Design v2 (palette Africain moderne) : CSS externalisé ───
# Le fichier style.css vit à la racine du projet (voir README
# design/streamlit_theme/). Les couleurs de base sont aussi dans
# .streamlit/config.toml pour que Streamlit les utilise nativement.
#
# ATTENTION : ne PAS utiliser f"<style>{css}</style>" — Streamlit peut
# rendre le contenu comme markdown si la CSS contient des séquences
# ambigües ("====", "# heading", etc. dans les commentaires). On
# concatène en string brute pour garantir que le <style> reste une
# balise HTML à la racine du bloc.
_css_path = Path(__file__).parent / "style.css"
if _css_path.exists():
    _css_content = _css_path.read_text()
    st.markdown(
        "<style>\n" + _css_content + "\n</style>",
        unsafe_allow_html=True,
    )


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
    # Réconciliation du schéma dès le démarrage : ajoute les colonnes
    # manquantes sur les installations existantes (évite UndefinedColumn
    # sur n'importe quelle page qui référence total_assets, eps, per, etc.).
    try:
        init_db()
    except Exception as _e:
        _log.exception("init_db failed at startup: %s", _e)
        st.session_state.init_db_error = f"{type(_e).__name__}: {_e}"

    count, is_fresh = _check_data_status()

    if count > 0:
        # DB OK → on mémorise définitivement pour cette session
        st.session_state.db_verified = True
        # Sync incrémental optionnel, seulement si vraiment pas frais
        if not is_fresh and not st.session_state.get("sync_done"):
            # Page de garde pendant la mise à jour quotidienne.
            st.markdown("## 📊 BRVM Analyzer")
            st.info(
                "⏳ **Mise à jour quotidienne en cours — durée estimée ~1 minute**\n\n"
                "Récupération des cotations du jour et des prix manquants pour "
                "les 48 titres (sikafinance.com). Cette opération ne se fait qu'une "
                "fois par jour et par session.\n\n"
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
        st.markdown("### Premier lancement — Initialisation")
        st.info(
            "⏳ **Durée estimée : ~2 minutes**\n\n"
            "L'application récupère les cotations, profils et prix historiques "
            "des **48 titres BRVM** depuis sikafinance.com. Un délai anti-ban de "
            "300 ms est appliqué entre chaque titre (~48 × 1.3 s + rapports + indices).\n\n"
            "**Cette opération ne se fait qu'une seule fois.** Les sessions "
            "suivantes utilisent la base de données Supabase et se chargent en "
            "moins de 3 secondes."
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
    "<div style='text-align:center;padding:0.5rem 0 0.5rem;'>"
    "<span style='font-size:1.35rem;font-weight:700;color:var(--ink);letter-spacing:-0.02em;'>"
    "<span style='color:var(--terracotta);'>📊</span> BRVM</span><br>"
    "<span style='font-size:0.78rem;color:var(--ink-3);letter-spacing:0.04em;text-transform:uppercase;font-weight:500;'>Analyzer</span></div>",
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

    # Bouton snapshot quotidien : reconstruit les tables pré-calculées
    # (scoring, performance, historique signaux) → pages < 1 s ensuite.
    if st.sidebar.button("📸 Regénérer snapshots", use_container_width=True,
                          help="Précalcule les agrégats pour accélérer les pages Signaux/Performance/Historique"):
        from scripts.build_daily_snapshot import build_all
        with st.spinner("Construction des snapshots (~30 s)…"):
            res = build_all()
        if res.get("status") == "ok":
            st.sidebar.success(
                f"✅ Snapshots en {res['duration_sec']}s — "
                f"{res.get('scoring',0)} scoring, {res.get('ticker_perf',0)} perf, "
                f"{res.get('signal_perf',0)} signaux"
            )
            # Invalide le cache st.cache_data
            try:
                st.cache_data.clear()
            except Exception:
                pass
        else:
            st.sidebar.error(f"❌ Échec : {res.get('error','inconnu')}")
        st.rerun()

# Indicateur de fraîcheur des snapshots (visible pour tous)
try:
    _meta_conn = get_connection()
    _meta = _meta_conn.execute(
        "SELECT value FROM snapshot_meta WHERE key='last_build_at'"
    ).fetchone()
    _meta_conn.close()
    if _meta and _meta[0]:
        _dt_str = str(_meta[0])[:16].replace("T", " ")
        st.sidebar.caption(f"📸 Snapshots : {_dt_str}")
    else:
        st.sidebar.caption("📸 Snapshots : jamais générés")
except Exception:
    pass

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
    "<span style='font-size:0.72rem;color:var(--ink-3);font-weight:500;'>BRVM Analyzer v1.0</span><br>"
    "<span style='font-size:0.68rem;color:var(--ink-4);'>Données · sikafinance.com</span><br>"
    f"<span style='font-size:0.62rem;color:var(--ink-4);'>{_db_label}</span></div>",
    unsafe_allow_html=True,
)
