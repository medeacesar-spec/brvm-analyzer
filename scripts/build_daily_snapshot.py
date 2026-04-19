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
)
from analysis.scoring import compute_hybrid_score, compute_consolidated_verdict


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

        rows_to_insert.append((
            ticker,
            fund.get("company_name") or ticker,
            fund.get("sector", ""),
            fund.get("price") or 0,
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
