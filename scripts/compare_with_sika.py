#!/usr/bin/env python3
"""Compare les données DB aux données sikafinance (source de vérité).
- Pour chaque ticker, scrape la page societe sikafinance
- Compare revenue, net_income, dps, eps, per par année
- Signale les écarts > 5%
- Optionnellement (--fill) remplit les trous dans la DB

Usage:
    python3 scripts/compare_with_sika.py             # rapport seul
    python3 scripts/compare_with_sika.py --fill      # remplit les trous
    python3 scripts/compare_with_sika.py --overwrite # remplit + écrase écarts > 5%
"""
import os
import sys
import sqlite3
import time
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

from config import DB_PATH, load_tickers
from scripts.scrape_societe import scrape_societe, HEADERS


TOL = 0.05  # 5% tolerance for discrepancy


def _pct_diff(a, b):
    if a is None or b is None or b == 0:
        return None
    return (a - b) / abs(b)


def compare(fill: bool = False, overwrite: bool = False):
    tickers = load_tickers()
    print(f"Comparing {len(tickers)} tickers with sikafinance…\n")

    conn = get_connection()
    session = requests.Session()
    session.headers.update(HEADERS)

    gaps = []
    mismatches = []
    filled = 0
    overwritten = 0

    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        print(f"[{i+1}/{len(tickers)}] {ticker}…", end=" ", flush=True)
        try:
            sika = scrape_societe(session, ticker)
        except Exception as e:
            print(f"skip ({e})")
            continue

        if not sika.get("financials"):
            print("no sika data")
            continue

        # Rows in our DB for this ticker
        rows = conn.execute(
            "SELECT fiscal_year, revenue, net_income, dps, eps, per, equity "
            "FROM fundamentals WHERE ticker = ?",
            (ticker,),
        ).fetchall()
        db_by_year = {r["fiscal_year"]: dict(r) for r in rows}

        for year, fin in sika.get("financials", {}).items():
            db_row = db_by_year.get(year)

            # GAP : no DB row for this year — we'll insert a minimal row
            if not db_row:
                gaps.append((ticker, year, "no row", fin))
                if fill:
                    sector = t.get("sector", "")
                    name = t.get("name", ticker)
                    conn.execute(
                        """INSERT OR IGNORE INTO fundamentals
                           (ticker, company_name, sector, fiscal_year,
                            revenue, net_income, dps, eps, per, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                        (ticker, name, sector, year,
                         fin.get("revenue"), fin.get("net_income"),
                         fin.get("dps"), fin.get("eps"), fin.get("per")),
                    )
                    filled += 1
                continue

            # FIELD-LEVEL COMPARISON
            for field in ("revenue", "net_income", "dps", "eps", "per"):
                sika_val = fin.get(field)
                db_val = db_row.get(field)

                if sika_val is None:
                    continue

                if db_val is None or db_val == 0:
                    # GAP in DB
                    gaps.append((ticker, year, field, sika_val))
                    if fill:
                        conn.execute(
                            f"UPDATE fundamentals SET {field} = ?, updated_at = CURRENT_TIMESTAMP "
                            "WHERE ticker = ? AND fiscal_year = ?",
                            (sika_val, ticker, year),
                        )
                        filled += 1
                else:
                    # Compare
                    diff = _pct_diff(db_val, sika_val)
                    if diff is not None and abs(diff) > TOL:
                        mismatches.append(
                            (ticker, year, field, db_val, sika_val, diff)
                        )
                        if overwrite:
                            conn.execute(
                                f"UPDATE fundamentals SET {field} = ?, updated_at = CURRENT_TIMESTAMP "
                                "WHERE ticker = ? AND fiscal_year = ?",
                                (sika_val, ticker, year),
                            )
                            overwritten += 1

        print("ok")
        time.sleep(0.3)

    conn.commit()

    # Report
    print(f"\n{'='*70}")
    print(f"RAPPORT DE COMPARAISON")
    print(f"{'='*70}")
    print(f"Gaps (trous DB comblés par sika)  : {len(gaps)}")
    print(f"Mismatches (écart > {TOL*100:.0f}%)       : {len(mismatches)}")

    if gaps:
        print(f"\n--- GAPS ({len(gaps)} au total, aperçu 20) ---")
        for g in gaps[:20]:
            val = g[3] if not isinstance(g[3], dict) else "—"
            val_str = f"{val:,.0f}" if isinstance(val, (int, float)) else str(val)
            print(f"  {g[0]:<10} {g[1] or '?':<6} {g[2]:<12} sika={val_str}")
        if len(gaps) > 20:
            print(f"  ... et {len(gaps)-20} autres")

    if mismatches:
        print(f"\n--- MISMATCHES ({len(mismatches)} au total, aperçu 30) ---")
        for m in sorted(mismatches, key=lambda x: -abs(x[5]))[:30]:
            ticker, year, field, db_val, sika_val, diff = m
            print(
                f"  {ticker:<10} {year:<6} {field:<12} "
                f"DB={db_val:>14,.0f} vs SIKA={sika_val:>14,.0f} "
                f"({diff*100:+.1f}%)"
            )

    if fill or overwrite:
        print(f"\n--- ACTIONS ---")
        if fill:
            print(f"  Trous remplis (DB ← sika)  : {filled}")
        if overwrite:
            print(f"  Écarts écrasés (DB ← sika) : {overwritten}")

    if not (fill or overwrite):
        print(f"\n💡 Relancer avec --fill pour remplir les trous, --overwrite pour aussi écraser les écarts.")

    conn.close()


if __name__ == "__main__":
    fill = "--fill" in sys.argv or "--overwrite" in sys.argv
    overwrite = "--overwrite" in sys.argv
    compare(fill=fill, overwrite=overwrite)
