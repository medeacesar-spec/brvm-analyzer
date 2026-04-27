"""
Build daily snapshot : précalcul des agrégats lourds pour des pages
ultra-rapides (<1 s).

Remplit 3 tables :
  - scoring_snapshot           : 1 ligne/ticker avec hybrid_score, verdict, signals
  - ticker_performance_snapshot: 1 ligne/ticker avec perf 3M/6M/1A/2A/3A/Max
  - signal_performance_snapshot: 1 ligne/event avec horizons 1M/3M/6M/1A

Exécution :
  - Manuelle admin : bouton "🔄 Regénérer snapshots" dans la sidebar
  - Automatique    : GitHub Actions quotidien à 17h WAT (après clôture BRVM)

Idempotent : peut tourner N fois, l'état final est toujours cohérent.
"""

import json
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Permet de lancer le script depuis n'importe où
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from config import load_tickers
from data.db import get_connection, read_sql_df
from data.storage import (
    init_db,
    get_all_stocks_for_analysis,
    get_all_cached_prices,
    get_signal_history,
    compute_signal_performance,
    save_market_data,
)
from analysis.scoring import compute_hybrid_score, compute_consolidated_verdict


# ─────────────────────────────────────────────────────────────
# 0. Ingestion prix du jour (avant tout calcul)
# ─────────────────────────────────────────────────────────────

def _last_business_day_str() -> str:
    """Dernière date ouvrée (YYYY-MM-DD). Si on tourne le lundi, c'est lundi ;
    si on tourne le samedi ou dimanche, c'est vendredi."""
    d = datetime.now()
    while d.weekday() >= 5:  # 5=sam, 6=dim
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def ingest_today_prices(conn) -> int:
    """Scrape les cotations du jour sur brvm.org et alimente :
    - market_data (prix courant + variation)
    - price_cache (close sous la vraie date de séance brvm.org)
    - snapshot_meta (last_session_date/time/kind/raw) pour le Dashboard.
    Retourne le nombre de tickers mis à jour."""
    try:
        from data.scraper import fetch_daily_quotes, fetch_session_info
    except Exception as e:
        print(f"  [ingest] import KO: {e}")
        return 0

    # ── Date et heure de séance officielle (brvm.org) ──
    try:
        session = fetch_session_info()
    except Exception:
        session = {}
    session_date = session.get("date")
    session_time = session.get("time")
    is_open = session.get("is_open")

    try:
        quotes = fetch_daily_quotes()
    except Exception as e:
        print(f"  [ingest] fetch_daily_quotes KO: {e}")
        return 0

    if quotes.empty:
        print("  [ingest] aucune cotation scrapée")
        return 0

    # Date autoritative pour price_cache : celle de brvm.org. Fallback
    # _last_business_day_str() si le header n'a pas pu être parsé.
    date_str = session_date or _last_business_day_str()
    today_str = datetime.now().strftime("%Y-%m-%d")
    if is_open:
        kind = "mi-seance"
    elif session_date and session_date == today_str:
        kind = "cloture"
    else:
        kind = "cloture-veille"

    n_market = 0
    n_cache = 0

    for _, row in quotes.iterrows():
        ticker = row.get("ticker") or ""
        last_price = row.get("last") or 0
        if not ticker or not last_price:
            continue

        # market_data (latest)
        try:
            save_market_data({
                "ticker": ticker,
                "company_name": row.get("name") or ticker,
                "price": last_price,
                "variation": row.get("variation") or 0,
            })
            n_market += 1
        except Exception:
            pass

        # price_cache (close du jour, upsert)
        try:
            conn.execute(
                """INSERT INTO price_cache (ticker, date, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ticker, date) DO UPDATE SET
                     open=excluded.open, high=excluded.high, low=excluded.low,
                     close=excluded.close, volume=excluded.volume""",
                (
                    ticker, date_str,
                    row.get("open") or last_price,
                    row.get("high") or last_price,
                    row.get("low") or last_price,
                    last_price,
                    row.get("volume_shares") or 0,
                ),
            )
            n_cache += 1
        except Exception as e:
            print(f"  [ingest] price_cache insert KO pour {ticker}: {e}")

    conn.commit()

    # ── Persiste les metas séance pour le Dashboard ──
    try:
        for k, v in [
            # Fallback sur date_str (dernier jour ouvré) si le header brvm.org
            # n'a pas été parsé — évite un snapshot_meta vide qui casse l'UI.
            ("last_session_date", session_date or date_str),
            ("last_session_time", session_time or ""),
            ("last_session_kind", kind),
            ("last_session_is_open", "1" if is_open else "0"),
            ("last_session_raw", session.get("raw") or ""),
        ]:
            conn.execute(
                """INSERT INTO snapshot_meta (key, value, updated_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(key) DO UPDATE SET
                     value = excluded.value, updated_at = CURRENT_TIMESTAMP""",
                (k, v),
            )
        conn.commit()
    except Exception:
        pass

    print(f"  [ingest] market_data {n_market} · price_cache {n_cache} "
           f"({date_str} · {kind})")
    return n_cache


# ─────────────────────────────────────────────────────────────
# 1. Scoring snapshot (remplace p5 Signaux live compute)
# ─────────────────────────────────────────────────────────────

def build_scoring_snapshot(conn, all_stocks: pd.DataFrame, all_prices: dict) -> int:
    """Pour chaque ticker : calcule hybrid_score + consolidated verdict,
    stocke signals_json dans scoring_snapshot.

    Optimisation : compute en boucle Python, puis INSERT batch via executemany
    (1 seul round-trip au lieu de 49).
    """
    if all_stocks.empty:
        print("  [scoring] all_stocks vide — skip")
        return 0

    import math as _m
    rows_to_insert = []
    history_to_save = []  # buffer pour signal_history (auto-historisation)
    verdict_daily_rows = []  # buffer pour verdict_daily (journal append-only)

    # Fallback secteur depuis tickers.json : fundamentals/market_data peuvent
    # avoir sector NULL pour certains tickers (BICC, ECOC, ETIT, SGBC, SNTS…)
    # alors que tickers.json est la source autoritaire de cette taxonomie.
    try:
        from config import load_tickers as _load_tickers
        _ticker_sectors = {t["ticker"]: t.get("sector", "") for t in _load_tickers()}
    except Exception:
        _ticker_sectors = {}

    # Date canonique de la session : on lit snapshot_meta (rempli par
    # ingest_today_prices juste avant), fallback sur _last_business_day_str()
    # si jamais le scrape header brvm.org a échoué.
    try:
        _r = conn.execute(
            "SELECT value FROM snapshot_meta WHERE key = ?",
            ("last_session_date",),
        ).fetchone()
        session_date_str = (_r[0] if _r else "") or _last_business_day_str()
    except Exception:
        session_date_str = _last_business_day_str()

    for _, row in all_stocks.iterrows():
        ticker = row["ticker"]
        fund = {k: (None if isinstance(v, float) and _m.isnan(v) else v)
                for k, v in row.to_dict().items()}
        price_df = all_prices.get(ticker, pd.DataFrame())

        try:
            result = compute_hybrid_score(fund, price_df)
            consolidated = compute_consolidated_verdict(result)
        except Exception as e:
            print(f"  [scoring] {ticker} : erreur compute_hybrid_score → {e}")
            continue

        reco = result.get("recommendation", {}) or {}
        trend = (result.get("trend", {}) or {}).get("trend", "")
        signals = result.get("signals", []) or []
        nb_signals = sum(1 for s in signals if s.get("type") in ("achat", "vente"))
        company_name = fund.get("company_name") or ticker
        # Fallback sur tickers.json si fundamentals/market_data n'ont pas
        # de secteur (cas frequent pour BICC, ECOC, ETIT, SGBC, SNTS).
        sector = (fund.get("sector") or "").strip() or _ticker_sectors.get(ticker, "")
        ref_price = fund.get("price") or 0

        rows_to_insert.append((
            ticker, company_name, sector, ref_price,
            result.get("hybrid_score"),
            result.get("fundamental_score"),
            result.get("technical_score"),
            reco.get("verdict"),
            reco.get("stars"),
            trend,
            nb_signals,
            json.dumps(signals, default=str),
            json.dumps(consolidated, default=str),
        ))

        history_to_save.append({
            "ticker": ticker,
            "company_name": company_name,
            "sector": sector,
            "price": ref_price,
            "signals": signals,
            "reco": reco,
            "hybrid_score": result.get("hybrid_score"),
            "fundamental_score": result.get("fundamental_score"),
            "technical_score": result.get("technical_score"),
            "trend": trend,
        })

        verdict_daily_rows.append((
            ticker, session_date_str,
            reco.get("verdict"),
            reco.get("stars"),
            result.get("hybrid_score"),
            result.get("fundamental_score"),
            result.get("technical_score"),
            ref_price,
            trend,
            nb_signals,
            sector,
            company_name,
        ))

    # Batch insert via executemany (1 round-trip au lieu de 49)
    conn.execute("DELETE FROM scoring_snapshot")
    if rows_to_insert:
        cur = conn.cursor()
        cur.executemany(
            """INSERT INTO scoring_snapshot
               (ticker, company_name, sector, price,
                hybrid_score, fundamental_score, technical_score,
                verdict, stars, trend, nb_signals,
                signals_json, consolidated_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows_to_insert,
        )
    conn.commit()
    print(f"  [scoring] {len(rows_to_insert)} tickers écrits (batch)")

    # ── Journal quotidien append-only (verdict_daily) ──
    # Source de vérité pour toute analyse rétrospective : trajectoires,
    # backtests, hit rate, score evolution, cohorts. UPSERT sur (ticker,date)
    # pour rester idempotent si build_daily_snapshot tourne 2x dans la journée.
    if verdict_daily_rows:
        try:
            cur = conn.cursor()
            cur.executemany(
                """INSERT INTO verdict_daily
                   (ticker, date, verdict, stars,
                    hybrid_score, fundamental_score, technical_score,
                    price, trend, nb_signals, sector, company_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (ticker, date) DO UPDATE SET
                     verdict = excluded.verdict,
                     stars = excluded.stars,
                     hybrid_score = excluded.hybrid_score,
                     fundamental_score = excluded.fundamental_score,
                     technical_score = excluded.technical_score,
                     price = excluded.price,
                     trend = excluded.trend,
                     nb_signals = excluded.nb_signals,
                     sector = excluded.sector,
                     company_name = excluded.company_name,
                     computed_at = CURRENT_TIMESTAMP""",
                verdict_daily_rows,
            )
            conn.commit()
            print(f"  [verdict_daily] {len(verdict_daily_rows)} lignes upsertées"
                  f" pour {session_date_str}")
        except Exception as e:
            print(f"  [verdict_daily] échec batch insert: {e}")

    # ── Auto-historisation dans signal_history ──
    # Avant : signal_history n'était peuplée que via les visites admin sur p2
    # → seuls les tickers manuellement ouverts y figuraient (ETIT only).
    # Désormais : chaque build quotidien archive TOUS les verdicts + signaux,
    # ce qui rend possible l'analyse rétrospective à 30j sans dépendre des
    # interactions UI. La règle de merge 7j (cf. _upsert_signal_event) évite
    # le bloat : un même verdict qui persiste ne crée pas de doublon.
    from data.storage import save_signal_snapshots, save_recommendation_snapshot
    n_new_signals = 0
    n_new_recos = 0
    n_errors = 0
    for h in history_to_save:
        try:
            n_new_signals += save_signal_snapshots(
                ticker=h["ticker"],
                signals=h["signals"],
                price=h["price"],
                company_name=h["company_name"],
                sector=h["sector"],
            )
            if save_recommendation_snapshot(
                ticker=h["ticker"],
                recommendation=h["reco"],
                hybrid_score=h["hybrid_score"],
                fundamental_score=h["fundamental_score"],
                technical_score=h["technical_score"],
                price=h["price"],
                trend=h["trend"],
                company_name=h["company_name"],
                sector=h["sector"],
            ):
                n_new_recos += 1
        except Exception as e:
            n_errors += 1
            if n_errors <= 3:
                print(f"  [history] {h['ticker']} KO: {e}")
    print(f"  [history] +{n_new_recos} reco events · +{n_new_signals} signaux"
          f" · {n_errors} erreurs")

    return len(rows_to_insert)


# ─────────────────────────────────────────────────────────────
# 2. Ticker performance snapshot (remplace p9 Performance)
# ─────────────────────────────────────────────────────────────

PERIODS = {
    "perf_1m":  timedelta(days=30),
    "perf_3m":  timedelta(days=91),
    "perf_6m":  timedelta(days=182),
    "perf_1a":  timedelta(days=365),
    "perf_2a":  timedelta(days=730),
    "perf_3a":  timedelta(days=1095),
    "perf_max": None,
}


def _perf_from_cutoff(df: pd.DataFrame, cutoff) -> float:
    """% de variation depuis cutoff jusqu'au dernier close."""
    if df.empty or "close" not in df.columns:
        return None
    df_sorted = df.sort_values("date")
    df_period = df_sorted[df_sorted["date"] >= pd.Timestamp(cutoff)] if cutoff else df_sorted
    if len(df_period) < 2:
        return None
    start = df_period.iloc[0]["close"]
    end = df_period.iloc[-1]["close"]
    if not start or start == 0:
        return None
    return (end - start) / start


def build_ticker_performance(conn, all_prices: dict) -> int:
    """Pour chaque ticker : calcule perf 1M/3M/6M/1A/2A/3A/Max (batch insert)."""
    tickers_meta = {t["ticker"]: t for t in load_tickers()}
    today = datetime.now().date()
    rows_to_insert = []

    for ticker, df in all_prices.items():
        if df.empty or "close" not in df.columns:
            continue
        meta = tickers_meta.get(ticker, {})
        df_sorted = df.sort_values("date")
        last_row = df_sorted.iloc[-1]
        last_price = last_row["close"]
        last_date = last_row["date"]

        perfs = {}
        for col, delta in PERIODS.items():
            cutoff = (today - delta) if delta else None
            perfs[col] = _perf_from_cutoff(df, cutoff)

        rows_to_insert.append((
            ticker,
            meta.get("name", ticker),
            meta.get("sector", ""),
            float(last_price) if last_price else None,
            str(last_date)[:10] if last_date is not None else None,
            perfs.get("perf_1m"), perfs.get("perf_3m"), perfs.get("perf_6m"),
            perfs.get("perf_1a"), perfs.get("perf_2a"), perfs.get("perf_3a"),
            perfs.get("perf_max"),
        ))

    conn.execute("DELETE FROM ticker_performance_snapshot")
    if rows_to_insert:
        cur = conn.cursor()
        cur.executemany(
            """INSERT INTO ticker_performance_snapshot
               (ticker, company_name, sector, last_price, last_date,
                perf_1m, perf_3m, perf_6m, perf_1a, perf_2a, perf_3a, perf_max)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows_to_insert,
        )
    conn.commit()
    print(f"  [ticker_perf] {len(rows_to_insert)} tickers écrits (batch)")
    return len(rows_to_insert)


# ─────────────────────────────────────────────────────────────
# 3. Signal performance snapshot (remplace p10 Historique)
# ─────────────────────────────────────────────────────────────

def build_signal_performance(conn, all_prices: dict) -> int:
    """Calcule les horizons 1M/3M/6M/1A pour chaque événement signal_history.

    Optimisation : utilise all_prices déjà chargé (1 requête batch) au lieu
    des ~6 queries par événement de compute_signal_performance. Évite de
    saturer le transaction pooler Supabase (qui coupe après ~1 min d'activité).
    """
    from datetime import timedelta as _td

    df = get_signal_history()
    if df.empty:
        print("  [signal_perf] signal_history vide — skip")
        return 0

    def _price_at_or_before(sorted_prices: pd.DataFrame, target_date) -> float:
        """Dernier close ≤ target_date. sorted_prices a déjà date croissante."""
        mask = sorted_prices["date"] <= pd.Timestamp(target_date)
        sub = sorted_prices[mask]
        if sub.empty:
            return None
        return sub.iloc[-1]["close"]

    def _price_at_or_after(sorted_prices: pd.DataFrame, target_date) -> float:
        """Premier close ≥ target_date."""
        mask = sorted_prices["date"] >= pd.Timestamp(target_date)
        sub = sorted_prices[mask]
        if sub.empty:
            return None
        return sub.iloc[0]["close"]

    # Préparer : price_df trié par date pour chaque ticker
    prices_sorted = {
        tkr: pdf.sort_values("date").reset_index(drop=True)
        for tkr, pdf in all_prices.items() if not pdf.empty and "close" in pdf.columns
    }

    def _f(v):
        if v is None or pd.isna(v):
            return None
        return float(v)

    rows_to_insert = []
    for _, row in df.iterrows():
        event_id = row.get("id")
        if event_id is None or pd.isna(event_id):
            continue
        ticker = row.get("ticker")
        start_date = row.get("first_seen_date")
        last_seen = row.get("last_seen_date")
        ref_price = row.get("price_at_start")

        pdf = prices_sorted.get(ticker)
        if pdf is None or pdf.empty or not start_date:
            rows_to_insert.append((int(event_id), None, None, None, None, None, None, None))
            continue

        try:
            start_dt = datetime.strptime(str(start_date)[:10], "%Y-%m-%d")
        except Exception:
            continue

        if not ref_price or (isinstance(ref_price, float) and pd.isna(ref_price)):
            ref_price = _price_at_or_before(pdf, start_dt)

        latest = pdf.iloc[-1]["close"] if not pdf.empty else None
        current_price = float(latest) if latest else None
        perf_since = None
        if ref_price and current_price:
            perf_since = (current_price - ref_price) / ref_price

        horizons = {"perf_1m": 30, "perf_3m": 91, "perf_6m": 182, "perf_1a": 365}
        perfs = {}
        for col, days in horizons.items():
            target = start_dt + _td(days=days)
            p = _price_at_or_after(pdf, target)
            perfs[col] = (p - ref_price) / ref_price if (ref_price and p) else None

        duration = None
        try:
            if start_date and last_seen:
                d1 = datetime.strptime(str(start_date)[:10], "%Y-%m-%d")
                d2 = datetime.strptime(str(last_seen)[:10], "%Y-%m-%d")
                duration = max(0, (d2 - d1).days)
        except Exception:
            pass

        rows_to_insert.append((
            int(event_id),
            _f(current_price),
            _f(perfs.get("perf_1m")),
            _f(perfs.get("perf_3m")),
            _f(perfs.get("perf_6m")),
            _f(perfs.get("perf_1a")),
            _f(perf_since),
            duration,
        ))

    # Batch insert via executemany (1 round-trip au lieu de N)
    conn.execute("DELETE FROM signal_performance_snapshot")
    if rows_to_insert:
        cur = conn.cursor()
        cur.executemany(
            """INSERT INTO signal_performance_snapshot
               (event_id, current_price, perf_1m, perf_3m, perf_6m, perf_1a,
                perf_since_start, duration_days)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows_to_insert,
        )
    conn.commit()
    print(f"  [signal_perf] {len(rows_to_insert)} événements écrits (batch)")
    return len(rows_to_insert)


# ─────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────

def set_meta(conn, key: str, value: str):
    """Upsert snapshot_meta."""
    conn.execute(
        """INSERT INTO snapshot_meta (key, value, updated_at)
           VALUES (?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(key) DO UPDATE SET
             value=excluded.value, updated_at=CURRENT_TIMESTAMP""",
        (key, value),
    )
    conn.commit()


def build_all() -> dict:
    """Construit tous les snapshots. Retourne un résumé."""
    t0 = time.time()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] build_daily_snapshot — start")

    init_db()
    conn = get_connection()

    result = {"started_at": datetime.now().isoformat(timespec="seconds")}

    try:
        # ── Étape 0 : scrape les cotations du jour (prix + close price_cache) ──
        # Sans ça, les snapshots reposeraient sur la dernière clôture en cache
        # (ex. vendredi soir si rien n'a tourné lundi avant 16h UTC).
        ingested = ingest_today_prices(conn)
        result["ingested_prices"] = ingested

        # ── Étape 0bis : découverte + intégration des nouveaux PDFs ──
        # 1. scan_brvm_reports : ajoute à report_links les PDFs fraichement
        #    publies sur brvm.org pour les 48 societes.
        # 2. extract_pending_pubs : extract uniquement les PDFs lies aux
        #    publications encore "À intégrer" (ne re-traite pas tout).
        # → Idempotent : si rien de nouveau, ces deux scripts sont quasi-no-ops.
        try:
            from scripts.scan_brvm_reports import main as _scan_brvm
            print("  [pdfs] scan brvm.org pour nouveaux PDFs …")
            _scan_brvm()
        except Exception as e:
            print(f"  [pdfs] scan_brvm_reports KO (non bloquant): {e}")
        try:
            from scripts.extract_pending_pubs import main as _extract_pending
            print("  [pdfs] extract pending publications …")
            _extract_pending()
        except Exception as e:
            print(f"  [pdfs] extract_pending_pubs KO (non bloquant): {e}")

        # Préchargement batch (réutilisé par les 3 builders)
        print("  [load] all_stocks + all_prices …")
        all_stocks = get_all_stocks_for_analysis()
        all_prices = get_all_cached_prices()
        print(f"  [load] {len(all_stocks)} stocks, {len(all_prices)} tickers avec prix")

        result["scoring"] = build_scoring_snapshot(conn, all_stocks, all_prices)
        result["ticker_perf"] = build_ticker_performance(conn, all_prices)
        result["signal_perf"] = build_signal_performance(conn, all_prices)

        duration = round(time.time() - t0, 1)
        set_meta(conn, "last_build_at", datetime.now().isoformat(timespec="seconds"))
        set_meta(conn, "last_build_duration_sec", str(duration))
        set_meta(conn, "last_build_status", "ok")
        result["duration_sec"] = duration
        result["status"] = "ok"
        print(f"[done] snapshots construits en {duration}s")
    except Exception as e:
        traceback.print_exc()
        try:
            set_meta(conn, "last_build_status", f"error: {type(e).__name__}: {e}")
        except Exception:
            pass
        result["status"] = "error"
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        conn.close()

    return result


if __name__ == "__main__":
    out = build_all()
    print(json.dumps(out, indent=2))
    sys.exit(0 if out.get("status") == "ok" else 1)
