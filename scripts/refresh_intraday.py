"""
Refresh intraday léger : scrape brvm.org → upsert market_data + price_cache.
Pas de rebuild de snapshots (les pages qui lisent market_data sont déjà temps-réel).

Appelé :
  - Toutes les 2h en séance (09h/11h/13h/15h UTC Mon-Fri) via GitHub Actions
  - À la clôture (16h UTC) via daily_snapshot.yml (build_daily_snapshot.build_all)
  - Manuellement via le bouton Regénérer snapshots (admin)

Durée typique : < 5 secondes (un seul GET HTTP + ~48 upserts).
Idempotent : peut tourner N fois sans doublon.
"""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from data.db import get_connection
from data.storage import init_db, save_market_data
from data.scraper import fetch_daily_quotes


def _last_business_day_str() -> str:
    d = datetime.now()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def refresh_intraday() -> dict:
    t0 = time.time()
    init_db()

    try:
        quotes = fetch_daily_quotes()
    except Exception as e:
        return {"status": "error", "error": str(e), "duration_sec": 0}

    if quotes.empty:
        return {"status": "empty", "quotes": 0, "duration_sec": 0}

    date_str = _last_business_day_str()
    conn = get_connection()
    n_market = 0
    n_cache = 0

    for _, row in quotes.iterrows():
        ticker = row.get("ticker") or ""
        last_price = row.get("last") or 0
        if not ticker or not last_price:
            continue

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
        except Exception:
            pass

    conn.commit()

    # Trace dans snapshot_meta pour debug (last intraday refresh)
    try:
        conn.execute(
            """INSERT INTO snapshot_meta (key, value, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(key) DO UPDATE SET
                 value = excluded.value, updated_at = CURRENT_TIMESTAMP""",
            ("last_intraday_refresh", datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()
    except Exception:
        pass

    conn.close()
    duration = round(time.time() - t0, 2)
    return {
        "status": "ok",
        "quotes": len(quotes),
        "market_data_updated": n_market,
        "price_cache_updated": n_cache,
        "date": date_str,
        "duration_sec": duration,
    }


if __name__ == "__main__":
    out = refresh_intraday()
    print(out)
    sys.exit(0 if out.get("status") == "ok" else 1)
