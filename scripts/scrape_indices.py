#!/usr/bin/env python3
"""
Scrape les indices BRVM (principaux + sectoriels + total return)
depuis https://www.brvm.org/fr/indices
Met a jour la table indices_cache.
"""

import os
import re
import sys
import sqlite3

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

from config import DB_PATH

BRVM_INDICES_URL = "https://www.brvm.org/fr/indices"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def parse_number(text):
    """Parse a French-formatted number."""
    if not text:
        return None
    cleaned = text.replace("\xa0", "").replace(" ", "").replace("FCFA", "").strip()
    if not cleaned or cleaned == "-":
        return None
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        if cleaned.count(".") > 1:
            cleaned = cleaned.replace(".", "")
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def scrape_indices():
    """Scrape all indices from brvm.org/fr/indices."""
    indices = []

    try:
        resp = requests.get(BRVM_INDICES_URL, headers=HEADERS, timeout=30, verify=False)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERREUR] Impossible de charger {BRVM_INDICES_URL}: {e}")
        return indices

    soup = BeautifulSoup(resp.text, "lxml")

    # --- Parse the 3 indices tables ---
    # Each has: Nom | Fermeture precedente | Fermeture | Variation (%) | Variation 31 decembre (%)
    for table in soup.find_all("table", class_="table"):
        thead = table.find("thead")
        if not thead:
            continue
        headers_text = thead.get_text()
        if "Nom" not in headers_text or "Fermeture" not in headers_text:
            # Check if it's the activity table
            if "Activit" in headers_text or "Top" in headers_text or "Flop" in headers_text:
                continue
            continue

        tbody = table.find("tbody")
        if not tbody:
            continue

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue

            name = cells[0].get_text(strip=True)
            prev_close = parse_number(cells[1].get_text(strip=True))
            close = parse_number(cells[2].get_text(strip=True))

            # Variation: extract from span.text-bad or span.text-good
            var_span = cells[3].find("span", class_=["text-bad", "text-good"])
            variation = parse_number(var_span.get_text(strip=True)) if var_span else None
            if var_span and "text-bad" in var_span.get("class", []) and variation and variation > 0:
                variation = -variation

            # Variation YTD
            ytd_variation = None
            if len(cells) >= 5:
                ytd_span = cells[4].find("span", class_=["text-bad", "text-good"])
                ytd_variation = parse_number(ytd_span.get_text(strip=True)) if ytd_span else None
                if ytd_span and "text-bad" in ytd_span.get("class", []) and ytd_variation and ytd_variation > 0:
                    ytd_variation = -ytd_variation

            if name and close is not None:
                indices.append({
                    "name": name,
                    "value": close,
                    "prev_close": prev_close,
                    "variation": variation,
                    "ytd_variation": ytd_variation,
                })

    # --- Parse activity sidebar ---
    activity_table = soup.find("table", class_="activity")
    activity = {}
    if activity_table:
        for tr in activity_table.find("tbody").find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                activity[label] = value

    return indices, activity


def ensure_columns(conn):
    """Add missing columns to indices_cache."""
    cursor = conn.execute("PRAGMA table_info(indices_cache)")
    cols = [row[1] for row in cursor.fetchall()]
    for col, ctype in [("prev_close", "REAL"), ("ytd_variation", "REAL"), ("category", "TEXT")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE indices_cache ADD COLUMN {col} {ctype}")
            print(f"[DB] Added '{col}' to indices_cache")
    conn.commit()


def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    print("Scraping indices from brvm.org...\n")
    result = scrape_indices()
    if not result:
        print("No data returned.")
        return

    indices, activity = result

    if not indices:
        print("No indices found on page.")
        return

    # Categorize
    for idx in indices:
        name = idx["name"]
        if "TOTAL RETURN" in name.upper():
            idx["category"] = "total_return"
        elif any(s in name.upper() for s in ["COMPOSITE", "BRVM-30", "PRESTIGE", "PRINCIPAL"]):
            idx["category"] = "principal"
        else:
            idx["category"] = "sectoriel"

    # Display
    print(f"{'Nom':<45} {'Valeur':>10} {'Var %':>8} {'YTD %':>8}")
    print("-" * 75)
    for idx in indices:
        var_str = f"{idx['variation']:+.2f}" if idx['variation'] is not None else "N/A"
        ytd_str = f"{idx['ytd_variation']:+.2f}" if idx['ytd_variation'] is not None else "N/A"
        print(f"{idx['name']:<45} {idx['value']:>10.2f} {var_str:>8} {ytd_str:>8}")

    # Activity
    if activity:
        print(f"\nActivites du marche:")
        for k, v in activity.items():
            print(f"  {k}: {v}")

    # Save to DB
    conn = get_connection()
    ensure_columns(conn)

    # Clear old data and insert fresh
    conn.execute("DELETE FROM indices_cache")

    for idx in indices:
        conn.execute(
            """INSERT INTO indices_cache (name, value, variation, prev_close, ytd_variation, category, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (idx["name"], idx["value"], idx["variation"],
             idx["prev_close"], idx["ytd_variation"], idx["category"])
        )

    conn.commit()
    conn.close()

    print(f"\n{len(indices)} indices saved to indices_cache.")


if __name__ == "__main__":
    main()
