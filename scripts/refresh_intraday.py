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

    # ── Récupère d'abord la date/heure de séance officielle brvm.org ──
    # Sans ça, on écrirait les prix de clôture veille sous la date du jour
    # quand le scraper tourne avant l'ouverture du marché.
    try:
        from data.scraper import fetch_session_info
        session = fetch_session_info()
    except Exception:
        session = {}
    session_date = session.get("date")  # YYYY-MM-DD séance affichée
    session_time = session.get("time")  # HH:MM
    is_open = session.get("is_open")

    try:
        quotes = fetch_daily_quotes()
    except Exception as e:
        return {"status": "error", "error": str(e), "duration_sec": 0}

    if quotes.empty:
        return {"status": "empty", "quotes": 0, "duration_sec": 0}

    # Date d'écriture price_cache : la date de séance brvm.org (autoritative).
    # Fallback sur _last_business_day_str() si le scraping de l'entête a échoué.
    date_str = session_date or _last_business_day_str()
    today_str = datetime.now().strftime("%Y-%m-%d")
    # Qualification de la donnée :
    # - séance ouverte : mi-séance (prix intra-séance)
    # - séance fermée ET date == aujourd'hui : clôture du jour
    # - séance fermée ET date < aujourd'hui : données veille (avant ouverture)
    if is_open:
        data_kind = "mi-seance"
    elif session_date and session_date == today_str:
        data_kind = "cloture"
    else:
        data_kind = "cloture-veille"

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

    # ── Fallback capitalisations : remplit shares/market_cap/float_pct pour
    # les tickers où ces champs sont NULL/0 dans market_data. Ne touche PAS
    # aux tickers déjà renseignés (evite d'ecraser les fondamentaux manuels).
    n_cap_filled = 0
    try:
        from data.scraper import fetch_capitalizations_brvm
        caps = fetch_capitalizations_brvm()
        if not caps.empty:
            for _, c in caps.iterrows():
                ticker = c["ticker"]
                try:
                    row = conn.execute(
                        "SELECT shares, market_cap, float_pct FROM market_data "
                        "WHERE ticker = ?", (ticker,)
                    ).fetchone()
                    if not row:
                        continue
                    # Row est tuple ou dict-like selon driver
                    cur_shares = row[0] if not isinstance(row, dict) else row.get("shares")
                    cur_mcap = row[1] if not isinstance(row, dict) else row.get("market_cap")
                    cur_fp = row[2] if not isinstance(row, dict) else row.get("float_pct")
                    updates = []
                    params = []
                    if (not cur_shares or cur_shares == 0) and c["shares"]:
                        updates.append("shares = ?")
                        params.append(float(c["shares"]))
                    if (not cur_mcap or cur_mcap == 0) and c["total_market_cap"]:
                        updates.append("market_cap = ?")
                        params.append(float(c["total_market_cap"]))
                    if (not cur_fp or cur_fp == 0) and c["float_pct"]:
                        updates.append("float_pct = ?")
                        params.append(float(c["float_pct"]))
                    if updates:
                        params.append(ticker)
                        conn.execute(
                            f"UPDATE market_data SET {', '.join(updates)} "
                            f"WHERE ticker = ?", params,
                        )
                        n_cap_filled += 1
                except Exception:
                    continue
            conn.commit()
    except Exception as e:
        print(f"  [caps] fallback KO: {e}")

    # ── Volumes du jour + PER officiel + agregats marche (brvm.org/volumes/0) ──
    n_vol = 0
    market_totals = {}
    try:
        from data.scraper import fetch_volumes_brvm
        vol_data = fetch_volumes_brvm()
        vol_df = vol_data.get("tickers")
        market_totals = vol_data.get("market", {}) or {}
        if vol_df is not None and not vol_df.empty:
            for _, v in vol_df.iterrows():
                ticker = v["ticker"]
                try:
                    # Update du volume_xof dans price_cache (date du jour)
                    conn.execute(
                        "UPDATE price_cache SET volume = ? "
                        "WHERE ticker = ? AND date = ?",
                        (float(v["volume_shares"] or 0), ticker, date_str),
                    )
                    n_vol += 1
                except Exception:
                    pass
            conn.commit()
    except Exception as e:
        print(f"  [volumes] KO: {e}")

    # Persiste les agregats marche dans snapshot_meta pour le dashboard
    try:
        for k, v in market_totals.items():
            conn.execute(
                """INSERT INTO snapshot_meta (key, value, updated_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(key) DO UPDATE SET
                     value = excluded.value, updated_at = CURRENT_TIMESTAMP""",
                (f"market_{k}", str(v)),
            )
        conn.commit()
    except Exception:
        pass

    # Trace dans snapshot_meta pour debug (last intraday refresh)
    # + contexte seance pour l'affichage Dashboard
    try:
        meta_entries = [
            ("last_intraday_refresh", datetime.now().isoformat(timespec="seconds")),
            ("last_session_date", session_date or ""),
            ("last_session_time", session_time or ""),
            ("last_session_kind", data_kind),  # mi-seance | cloture | cloture-veille
            ("last_session_raw", session.get("raw") or ""),
        ]
        for k, v in meta_entries:
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

    conn.close()
    duration = round(time.time() - t0, 2)
    return {
        "status": "ok",
        "quotes": len(quotes),
        "market_data_updated": n_market,
        "price_cache_updated": n_cache,
        "caps_filled": n_cap_filled,
        "volumes_updated": n_vol,
        "market_totals": market_totals,
        "date": date_str,
        "session_date": session_date,
        "session_time": session_time,
        "session_kind": data_kind,
        "duration_sec": duration,
    }


if __name__ == "__main__":
    out = refresh_intraday()
    print(out)
    sys.exit(0 if out.get("status") == "ok" else 1)
