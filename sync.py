"""
Script de synchronisation des donnees BRVM.
Charge toutes les donnees en base SQLite pour une utilisation offline.

Usage:
    python3 sync.py              # Sync complet (prix + marche + rapports)
    python3 sync.py --prices     # Prix historiques seulement
    python3 sync.py --market     # Donnees marche (cotations + details) seulement
    python3 sync.py --reports    # Rapports BRVM seulement
"""

import sys
import time
import argparse
from datetime import datetime

sys.path.insert(0, ".")

from config import load_tickers
from data.scraper import (
    fetch_daily_quotes, fetch_historical_prices, fetch_stock_details,
    fetch_sector_indices,
)
from data.storage import (
    init_db, save_market_data, cache_prices, get_cached_prices,
    seed_known_report_links, get_connection,
)


def sync_market_data():
    """Synchronise cotations du jour + details (beta, RSI, dividendes) pour les 48 titres."""
    tickers = load_tickers()
    print(f"\n{'='*60}")
    print(f"  SYNC DONNEES MARCHE - {len(tickers)} titres")
    print(f"{'='*60}")

    # 1. Cotations du jour
    print("\n[1/2] Cotations du jour depuis sikafinance...")
    try:
        quotes = fetch_daily_quotes()
        price_map = dict(zip(quotes["ticker"], quotes["last"]))
        var_map = dict(zip(quotes["ticker"], quotes["variation"]))
        print(f"  OK: {len(quotes)} cotations chargees")
    except Exception as e:
        print(f"  ERREUR: {e}")
        price_map = {}
        var_map = {}

    # 2. Details par titre (beta, RSI, dividendes, market cap)
    print(f"\n[2/2] Details par titre (beta, RSI, dividendes)...")
    ok = 0
    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        try:
            details = fetch_stock_details(ticker)
            if "error" in details:
                print(f"  [{i+1}/{len(tickers)}] SKIP {ticker:12} (erreur scrape)")
                continue

            # DPS depuis historique dividendes
            dps = None
            div_hist = details.get("dividend_history", [])
            if div_hist:
                latest = max(div_hist, key=lambda d: d.get("year", 0))
                dps = latest.get("amount")

            # Prix depuis cotations du jour (plus fiable)
            price = price_map.get(ticker) or details.get("price") or 0

            save_market_data({
                "ticker": ticker,
                "name": t["name"],
                "sector": t["sector"],
                "price": price,
                "variation": var_map.get(ticker, 0),
                "market_cap": details.get("market_cap"),
                "beta": details.get("beta"),
                "rsi": details.get("rsi"),
                "dps": dps,
                "dividend_history": div_hist,
            })
            print(f"  [{i+1}/{len(tickers)}] OK  {ticker:12} prix={price:>10,.0f}  dps={dps}")
            ok += 1
        except Exception as e:
            print(f"  [{i+1}/{len(tickers)}] ERR {ticker:12} {str(e)[:40]}")
        time.sleep(0.4)

    print(f"\n  Resume: {ok}/{len(tickers)} titres synchronises")

    # 3. Indices sectoriels
    print("\n  Indices sectoriels...")
    try:
        indices = fetch_sector_indices()
        # Store in a simple cache table
        conn = get_connection()
        conn.execute("CREATE TABLE IF NOT EXISTS indices_cache (name TEXT PRIMARY KEY, value REAL, variation REAL, updated_at TIMESTAMP)")
        for _, idx in indices.iterrows():
            conn.execute(
                "INSERT OR REPLACE INTO indices_cache (name, value, variation, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                (idx["name"], idx.get("value"), idx.get("variation")),
            )
        conn.commit()
        conn.close()
        print(f"  OK: {len(indices)} indices")
    except Exception as e:
        print(f"  ERREUR indices: {e}")


def sync_historical_prices(months_back=36):
    """Telecharge les prix historiques pour tous les titres (3 ans par defaut)."""
    tickers = load_tickers()
    print(f"\n{'='*60}")
    print(f"  SYNC PRIX HISTORIQUES - {len(tickers)} titres ({months_back} mois)")
    print(f"{'='*60}")

    ok = 0
    for i, t in enumerate(tickers):
        ticker = t["ticker"]

        # Skip si deja en cache avec assez de donnees
        existing = get_cached_prices(ticker)
        if not existing.empty and len(existing) > months_back * 15:
            print(f"  [{i+1}/{len(tickers)}] SKIP {ticker:12} (deja {len(existing)} jours en cache)")
            ok += 1
            continue

        print(f"  [{i+1}/{len(tickers)}] Chargement {ticker:12}...", end=" ", flush=True)
        try:
            df = fetch_historical_prices(ticker, months_back=months_back)
            if not df.empty:
                cache_prices(ticker, df)
                print(f"OK ({len(df)} jours)")
                ok += 1
            else:
                print("aucune donnee")
        except Exception as e:
            print(f"ERREUR: {str(e)[:40]}")
        time.sleep(0.3)

    print(f"\n  Resume: {ok}/{len(tickers)} titres avec prix historiques")


def sync_reports():
    """Charge les liens vers les rapports annuels connus."""
    print(f"\n{'='*60}")
    print(f"  SYNC RAPPORTS ANNUELS")
    print(f"{'='*60}")
    seed_known_report_links()
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM report_links").fetchone()[0]
    tickers_count = conn.execute("SELECT COUNT(DISTINCT ticker) FROM report_links").fetchone()[0]
    conn.close()
    print(f"  OK: {count} rapports pour {tickers_count} titres")


def sync_all():
    """Synchronisation complete."""
    start = datetime.now()
    print(f"\n BRVM ANALYZER - SYNCHRONISATION COMPLETE")
    print(f" Debut: {start.strftime('%Y-%m-%d %H:%M:%S')}")

    init_db()
    sync_market_data()
    sync_historical_prices(months_back=36)
    sync_reports()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*60}")
    print(f"  SYNCHRONISATION TERMINEE en {elapsed:.0f}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synchronisation des donnees BRVM")
    parser.add_argument("--prices", action="store_true", help="Prix historiques seulement")
    parser.add_argument("--market", action="store_true", help="Donnees marche seulement")
    parser.add_argument("--reports", action="store_true", help="Rapports seulement")
    parser.add_argument("--months", type=int, default=36, help="Nombre de mois de prix (defaut: 36)")
    args = parser.parse_args()

    init_db()

    if args.prices:
        sync_historical_prices(args.months)
    elif args.market:
        sync_market_data()
    elif args.reports:
        sync_reports()
    else:
        sync_all()
