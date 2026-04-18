#!/usr/bin/env python3
"""
Scrape le nombre d'actions (shares) et la capitalisation boursiere
pour tous les tickers BRVM depuis sikafinance.com, puis met a jour
les tables market_data et fundamentals.
"""

import os
import re
import sys
import time
import sqlite3

import requests
from bs4 import BeautifulSoup

# Ajouter le repertoire racine au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

from config import DB_PATH, SIKA_COTATION_URL, load_tickers

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def parse_number(text: str):
    """Parse a French-formatted number string into a float."""
    if not text:
        return None
    # Remove non-breaking spaces, regular spaces, and currency symbols
    cleaned = text.replace("\xa0", "").replace(" ", "").replace("FCFA", "").strip()
    # Handle French decimal comma
    # If there's a comma and no dot, treat comma as decimal separator
    # If there are dots used as thousands separators, remove them first
    # Common patterns: "1.234.567" or "1 234 567" (thousands) or "12,5" (decimal)
    if "," in cleaned:
        # Remove dots (thousands separators) then replace comma with dot
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        # Dots might be thousands separators if multiple exist
        if cleaned.count(".") > 1:
            cleaned = cleaned.replace(".", "")
    # Remove any remaining non-numeric chars except dot and minus
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def ensure_shares_column(conn: sqlite3.Connection):
    """Add shares column to market_data if it does not exist."""
    cursor = conn.execute("PRAGMA table_info(market_data)")
    columns = [row[1] for row in cursor.fetchall()]
    if "shares" not in columns:
        conn.execute("ALTER TABLE market_data ADD COLUMN shares REAL")
        print("[DB] Added 'shares' column to market_data table.")
    conn.commit()


def scrape_shares_for_ticker(session: requests.Session, ticker: str) -> dict:
    """
    Scrape la page sikafinance cotation pour un ticker.
    Retourne dict avec 'shares' et 'market_cap' (ou None).
    """
    url = f"{SIKA_COTATION_URL}{ticker}"
    result = {"shares": None, "market_cap": None}

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERREUR] Connexion echouee pour {ticker}: {e}")
        return result

    soup = BeautifulSoup(resp.text, "lxml")

    # Parcourir tous les tableaux de la page
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True).lower()
            raw_value = cells[1].get_text(strip=True)

            # Nombre d'actions / shares
            if any(kw in label for kw in ["nombre d'actions", "nombre d actions", "nb actions", "actions en circulation"]):
                val = parse_number(raw_value)
                if val is not None and val > 0:
                    result["shares"] = val

            # Capitalisation boursiere / market cap
            if "capitalisation" in label:
                val = parse_number(raw_value)
                if val is not None and val > 0:
                    result["market_cap"] = val

            # Capital social (fallback info, not shares count but useful)
            if "capital social" in label and result["shares"] is None:
                # Capital social is not the same as shares, skip
                pass

    # Also look in <dl>, <div> or other non-table structures
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            label = dt.get_text(strip=True).lower()
            raw_value = dd.get_text(strip=True)

            if any(kw in label for kw in ["nombre d'actions", "nombre d actions", "nb actions"]):
                val = parse_number(raw_value)
                if val is not None and val > 0:
                    result["shares"] = val

            if "capitalisation" in label:
                val = parse_number(raw_value)
                if val is not None and val > 0:
                    result["market_cap"] = val

    # Try to find shares in any element with matching text patterns
    if result["shares"] is None:
        for elem in soup.find_all(string=re.compile(r"nombre\s+d.actions", re.IGNORECASE)):
            parent = elem.find_parent()
            if parent:
                # Look for the next sibling or adjacent element with a number
                next_elem = parent.find_next_sibling()
                if next_elem:
                    val = parse_number(next_elem.get_text(strip=True))
                    if val is not None and val > 0:
                        result["shares"] = val

    return result


def main():
    tickers = load_tickers()
    total = len(tickers)
    print(f"Scraping shares data for {total} BRVM tickers from sikafinance.com\n")

    # Prepare DB
    conn = get_connection()
    ensure_shares_column(conn)

    session = requests.Session()
    session.headers.update(HEADERS)

    success_count = 0
    market_cap_count = 0
    results = []

    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        name = t.get("name", ticker)
        print(f"[{i+1}/{total}] {ticker} ({name})...", end=" ", flush=True)

        data = scrape_shares_for_ticker(session, ticker)
        shares = data["shares"]
        market_cap = data["market_cap"]

        status_parts = []
        if shares is not None:
            status_parts.append(f"shares={shares:,.0f}")
            success_count += 1
        else:
            status_parts.append("shares=N/A")

        if market_cap is not None:
            status_parts.append(f"mcap={market_cap:,.0f}")
            market_cap_count += 1

        print(" | ".join(status_parts))

        results.append({
            "ticker": ticker,
            "name": name,
            "shares": shares,
            "market_cap": market_cap,
        })

        # Delay between requests
        if i < total - 1:
            time.sleep(0.5)

    # --- Update market_data table ---
    print(f"\n--- Updating market_data table ---")
    updated_md = 0
    inserted_md = 0
    for r in results:
        if r["shares"] is None and r["market_cap"] is None:
            continue

        # Check if row exists
        row = conn.execute(
            "SELECT ticker FROM market_data WHERE ticker = ?", (r["ticker"],)
        ).fetchone()

        if row:
            # Build dynamic UPDATE
            set_parts = []
            params = []
            if r["shares"] is not None:
                set_parts.append("shares = ?")
                params.append(r["shares"])
            if r["market_cap"] is not None:
                set_parts.append("market_cap = ?")
                params.append(r["market_cap"])
            set_parts.append("updated_at = CURRENT_TIMESTAMP")
            params.append(r["ticker"])
            conn.execute(
                f"UPDATE market_data SET {', '.join(set_parts)} WHERE ticker = ?",
                params,
            )
            updated_md += 1
        else:
            # Insert minimal row
            conn.execute(
                """INSERT INTO market_data (ticker, company_name, shares, market_cap, updated_at)
                   VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (r["ticker"], r["name"], r["shares"], r["market_cap"]),
            )
            inserted_md += 1

    conn.commit()
    print(f"  market_data: {updated_md} updated, {inserted_md} inserted")

    # --- Update fundamentals table (latest fiscal_year per ticker) ---
    print(f"\n--- Updating fundamentals table (latest fiscal_year) ---")
    updated_fund = 0
    for r in results:
        if r["shares"] is None:
            continue

        # Find the latest fiscal_year for this ticker
        row = conn.execute(
            "SELECT id, fiscal_year FROM fundamentals WHERE ticker = ? ORDER BY fiscal_year DESC LIMIT 1",
            (r["ticker"],),
        ).fetchone()

        if row:
            conn.execute(
                "UPDATE fundamentals SET shares = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (r["shares"], row[0]),
            )
            updated_fund += 1

    conn.commit()
    conn.close()
    print(f"  fundamentals: {updated_fund} rows updated with shares")

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total tickers scraped: {total}")
    print(f"Shares found:         {success_count}/{total}")
    print(f"Market cap found:     {market_cap_count}/{total}")
    print(f"market_data updated:  {updated_md} | inserted: {inserted_md}")
    print(f"fundamentals updated: {updated_fund}")

    if success_count == 0:
        print(
            "\nWARNING: No shares data found. The page structure may have changed."
            "\nCheck a sample page manually: https://www.sikafinance.com/marches/cotation_SNTS.sn"
        )


if __name__ == "__main__":
    main()
