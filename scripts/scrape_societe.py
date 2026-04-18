#!/usr/bin/env python3
"""
Scrape la page SOCIETE de sikafinance.com pour chaque ticker BRVM.
Recupere : nombre de titres, flottant, valorisation, CA, RN, BNPA, PER, Dividende
sur 5 ans, puis met a jour fundamentals et market_data.
"""

import os
import re
import sys
import time
import sqlite3

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

from config import DB_PATH, SIKA_BASE_URL, load_tickers

SIKA_SOCIETE_URL = f"{SIKA_BASE_URL}/marches/societe/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def parse_number(text):
    """Parse a French-formatted number string into a float."""
    if not text:
        return None
    cleaned = text.replace("\xa0", "").replace(" ", "").replace("MFCFA", "").replace("FCFA", "").replace("%", "").strip()
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


def scrape_societe(session, ticker):
    """
    Scrape /marches/societe/{ticker} pour recuperer:
    - shares (nombre de titres)
    - float_pct (flottant)
    - market_cap (valorisation)
    - Table financiere multi-annees: CA, RN, BNPA, PER, Dividende
    """
    url = f"{SIKA_SOCIETE_URL}{ticker}"
    result = {
        "ticker": ticker,
        "shares": None,
        "float_pct": None,
        "market_cap": None,
        "financials": {},  # {year: {revenue, net_income, eps, per, dps}}
    }

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERREUR] {ticker}: {e}")
        return result

    soup = BeautifulSoup(resp.text, "lxml")

    # --- Extract from <p> tags: Nombre de titres, Flottant, Valorisation ---
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)

        if "Nombre de titres" in text:
            # Format: "Nombre de titres : 54 435 300"
            match = re.search(r":\s*([\d\s\xa0.]+)", text)
            if match:
                val = parse_number(match.group(1))
                if val and val > 0:
                    result["shares"] = val

        elif "Flottant" in text and "%" in text:
            match = re.search(r":\s*([\d,.\s\xa0]+)%", text)
            if match:
                val = parse_number(match.group(1))
                if val is not None:
                    result["float_pct"] = val

        elif "Valorisation" in text:
            match = re.search(r":\s*([\d\s\xa0.]+)\s*M", text)
            if match:
                val = parse_number(match.group(1))
                if val and val > 0:
                    result["market_cap"] = val  # Already in millions FCFA

    # --- Extract financial table (tabSociete) ---
    table = soup.find("table", class_="tabSociete")
    if not table:
        # Fallback: find table with CA/Resultat rows
        for t in soup.find_all("table"):
            text = t.get_text()
            if "Chiffre d'affaires" in text and "sultat net" in text:
                table = t
                break

    if table:
        # Get years from header
        header = table.find("thead")
        years = []
        if header:
            for th in header.find_all("th"):
                txt = th.get_text(strip=True)
                if txt.isdigit() and len(txt) == 4:
                    years.append(int(txt))

        # Get data rows
        rows = table.find("tbody")
        if rows:
            row_data = {}
            for tr in rows.find_all("tr"):
                cells = tr.find_all("td")
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True).lower()
                values = []
                for cell in cells[1:]:
                    values.append(parse_number(cell.get_text(strip=True)))

                if "chiffre d" in label and "croissance" not in label:
                    row_data["revenue"] = values
                elif "sultat net" in label and "croissance" not in label:
                    row_data["net_income"] = values
                elif "croissance ca" in label or "croissance du ca" in label or "croissance c.a" in label:
                    row_data["revenue_growth"] = values
                elif ("croissance rn" in label or "croissance du rn" in label
                      or "croissance résultat" in label or "croissance resultat" in label):
                    row_data["net_income_growth"] = values
                elif "bnpa" in label:
                    row_data["eps"] = values
                elif "per" == label.strip():
                    row_data["per"] = values
                elif "dividende" in label:
                    row_data["dps"] = values

            # Map years to financials
            # NOTE: sikafinance data is in MILLIONS FCFA for revenue/net_income
            # Convert to actual FCFA (* 1e6). EPS, DPS, PER, growth stay as-is.
            # Growth rates come from sika already in %, we normalize to fraction (0.1 = 10%)
            for i, year in enumerate(years):
                fin = {}
                for key, vals in row_data.items():
                    if i < len(vals) and vals[i] is not None:
                        if key in ("revenue", "net_income"):
                            fin[key] = vals[i] * 1e6
                        elif key in ("revenue_growth", "net_income_growth"):
                            # Sika publishes percent values (e.g. "10.4" for 10.4%)
                            # → convert to fraction (0.104). Always divide by 100.
                            fin[key] = vals[i] / 100
                        else:
                            fin[key] = vals[i]
                if fin:
                    result["financials"][year] = fin

    return result


def ensure_columns(conn):
    """Ensure all needed columns exist."""
    cursor = conn.execute("PRAGMA table_info(market_data)")
    md_cols = [row[1] for row in cursor.fetchall()]
    for col in ["shares", "float_pct"]:
        if col not in md_cols:
            conn.execute(f"ALTER TABLE market_data ADD COLUMN {col} REAL")
            print(f"[DB] Added '{col}' column to market_data")

    cursor = conn.execute("PRAGMA table_info(fundamentals)")
    fund_cols = [row[1] for row in cursor.fetchall()]
    for col in ["total_assets", "eps", "per", "revenue_growth", "net_income_growth"]:
        if col not in fund_cols:
            conn.execute(f"ALTER TABLE fundamentals ADD COLUMN {col} REAL")
            print(f"[DB] Added '{col}' column to fundamentals")
    conn.commit()


def main():
    tickers = load_tickers()
    total = len(tickers)
    print(f"Scraping SOCIETE data for {total} BRVM tickers from sikafinance.com\n")

    conn = get_connection()
    ensure_columns(conn)

    session = requests.Session()
    session.headers.update(HEADERS)

    shares_count = 0
    fin_count = 0
    all_results = []

    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        name = t.get("name", ticker)
        print(f"[{i+1}/{total}] {ticker} ({name})...", end=" ", flush=True)

        data = scrape_societe(session, ticker)
        all_results.append(data)

        parts = []
        if data["shares"]:
            parts.append(f"shares={data['shares']:,.0f}")
            shares_count += 1
        if data["float_pct"] is not None:
            parts.append(f"float={data['float_pct']:.1f}%")
        if data["market_cap"]:
            parts.append(f"mcap={data['market_cap']:,.0f}M")
        years = sorted(data["financials"].keys())
        if years:
            parts.append(f"fin={years[0]}-{years[-1]}")
            fin_count += 1
        else:
            parts.append("fin=none")

        print(" | ".join(parts) if parts else "no data")

        if i < total - 1:
            time.sleep(0.4)

    # --- Update market_data ---
    print(f"\n--- Updating market_data ---")
    updated_md = 0
    for r in all_results:
        if not r["shares"] and not r["market_cap"]:
            continue
        set_parts = []
        params = []
        if r["shares"]:
            set_parts.append("shares = ?")
            params.append(r["shares"])
        if r["float_pct"] is not None:
            set_parts.append("float_pct = ?")
            params.append(r["float_pct"])
        if r["market_cap"]:
            set_parts.append("market_cap = ?")
            params.append(r["market_cap"])
        if not set_parts:
            continue
        set_parts.append("updated_at = CURRENT_TIMESTAMP")
        params.append(r["ticker"])

        row = conn.execute("SELECT ticker FROM market_data WHERE ticker = ?", (r["ticker"],)).fetchone()
        if row:
            conn.execute(f"UPDATE market_data SET {', '.join(set_parts)} WHERE ticker = ?", params)
            updated_md += 1
    conn.commit()
    print(f"  market_data: {updated_md} updated with shares/mcap/float")

    # --- Update fundamentals ---
    print(f"\n--- Updating fundamentals ---")
    inserted_fund = 0
    updated_fund = 0

    for r in all_results:
        ticker = r["ticker"]
        if not r["financials"]:
            continue

        # Get ticker metadata
        ticker_info = next((t for t in tickers if t["ticker"] == ticker), {})
        company_name = ticker_info.get("name", ticker)
        sector = ticker_info.get("sector", "")

        for year, fin in r["financials"].items():
            # Check if row exists
            existing = conn.execute(
                "SELECT id, revenue, net_income, dps, shares FROM fundamentals WHERE ticker = ? AND fiscal_year = ?",
                (ticker, year)
            ).fetchone()

            if existing:
                # Only update fields that are NULL or 0 in existing data
                set_parts = []
                params = []

                # Always update shares from sikafinance (most reliable)
                if r["shares"]:
                    set_parts.append("shares = ?")
                    params.append(r["shares"])
                if r["float_pct"] is not None:
                    set_parts.append("float_pct = ?")
                    params.append(r["float_pct"])

                # Revenue and net_income: ALWAYS overwrite with sika data (authoritative, in actual FCFA)
                if fin.get("revenue"):
                    set_parts.append("revenue = ?")
                    params.append(fin["revenue"])
                if fin.get("net_income"):
                    set_parts.append("net_income = ?")
                    params.append(fin["net_income"])
                if fin.get("dps"):
                    set_parts.append("dps = ?")
                    params.append(fin["dps"])
                if fin.get("eps"):
                    set_parts.append("eps = ?")
                    params.append(fin["eps"])
                if fin.get("per"):
                    set_parts.append("per = ?")
                    params.append(fin["per"])
                if fin.get("revenue_growth") is not None:
                    set_parts.append("revenue_growth = ?")
                    params.append(fin["revenue_growth"])
                if fin.get("net_income_growth") is not None:
                    set_parts.append("net_income_growth = ?")
                    params.append(fin["net_income_growth"])

                if set_parts:
                    set_parts.append("updated_at = CURRENT_TIMESTAMP")
                    params.append(ticker)
                    params.append(year)
                    conn.execute(
                        f"UPDATE fundamentals SET {', '.join(set_parts)} WHERE ticker = ? AND fiscal_year = ?",
                        params
                    )
                    updated_fund += 1
            else:
                # Insert new row
                conn.execute(
                    """INSERT INTO fundamentals
                       (ticker, company_name, sector, fiscal_year, shares, float_pct,
                        revenue, net_income, dps, eps, per,
                        revenue_growth, net_income_growth, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                    (ticker, company_name, sector, year,
                     r["shares"], r["float_pct"],
                     fin.get("revenue"), fin.get("net_income"),
                     fin.get("dps"), fin.get("eps"), fin.get("per"),
                     fin.get("revenue_growth"), fin.get("net_income_growth"))
                )
                inserted_fund += 1

    conn.commit()

    # --- Also update historical N-3..N0 fields for the latest year ---
    print(f"\n--- Updating N-3..N0 historical fields ---")
    hist_updated = 0
    for r in all_results:
        ticker = r["ticker"]
        if not r["financials"]:
            continue
        years = sorted(r["financials"].keys())
        if len(years) < 2:
            continue

        # Latest year = N0
        latest_year = years[-1]
        hist = {}
        for offset, suffix in enumerate(["n0", "n1", "n2", "n3"]):
            idx = len(years) - 1 - offset
            if idx < 0:
                break
            y = years[idx]
            fin = r["financials"][y]
            if fin.get("revenue"):
                hist[f"revenue_{suffix}"] = fin["revenue"]
            if fin.get("net_income"):
                hist[f"net_income_{suffix}"] = fin["net_income"]
            if fin.get("dps"):
                hist[f"dps_{suffix}"] = fin["dps"]

        if hist:
            set_parts = [f"{k} = ?" for k in hist.keys()]
            set_parts.append("updated_at = CURRENT_TIMESTAMP")
            params = list(hist.values()) + [ticker, latest_year]
            conn.execute(
                f"UPDATE fundamentals SET {', '.join(set_parts)} WHERE ticker = ? AND fiscal_year = ?",
                params
            )
            hist_updated += 1

    conn.commit()
    conn.close()

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total tickers scraped:  {total}")
    print(f"Shares found:           {shares_count}/{total}")
    print(f"Financials found:       {fin_count}/{total}")
    print(f"market_data updated:    {updated_md}")
    print(f"fundamentals inserted:  {inserted_fund}")
    print(f"fundamentals updated:   {updated_fund}")
    print(f"N-3..N0 history set:    {hist_updated}")


if __name__ == "__main__":
    main()
