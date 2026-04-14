"""
Extracteur de donnees financieres depuis les PDFs des etats financiers BRVM.
Telecharge les PDFs et extrait CA, Resultat Net, Capitaux Propres, etc.
"""

import io
import re
import tempfile
from typing import Optional

import requests
import pdfplumber

from config import load_tickers
from data.storage import get_report_links, save_fundamentals, get_connection


def _parse_amount(text: str) -> Optional[float]:
    """Parse un montant depuis le texte PDF (ex: '1 084,1' → 1084100000000)."""
    if not text:
        return None
    cleaned = text.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    # Remove parentheses (negative)
    negative = "(" in cleaned
    cleaned = cleaned.replace("(", "").replace(")", "")
    try:
        val = float(cleaned)
        if negative:
            val = -val
        return val
    except (ValueError, TypeError):
        return None


def extract_from_pdf(pdf_path: str) -> dict:
    """
    Extrait les chiffres cles depuis un PDF d'etats financiers BRVM.
    Cherche: CA, Resultat Net, Capitaux Propres, EBIT, Total Actif, Dettes.
    Retourne un dict avec les valeurs trouvees.
    """
    data = {
        "revenue": None,
        "net_income": None,
        "equity": None,
        "ebit": None,
        "total_debt": None,
        "total_assets": None,
        "interest_expense": None,
        "multiplier": 1,  # 1 = FCFA, 1e6 = millions, 1e9 = milliards
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_text = ""
            all_tables = []

            for page in pdf.pages[:15]:
                text = page.extract_text() or ""
                all_text += text + "\n"
                tables = page.extract_tables()
                for table in tables:
                    if table and len(table) > 2:
                        all_tables.append(table)

            # Detect multiplier
            text_lower = all_text.lower()
            if "milliards" in text_lower or "en milliards" in text_lower:
                data["multiplier"] = 1_000_000_000
            elif "millions" in text_lower or "en millions" in text_lower:
                data["multiplier"] = 1_000_000
            elif "milliers" in text_lower or "en milliers" in text_lower:
                data["multiplier"] = 1_000

            mult = data["multiplier"]

            # Search in tables
            for table in all_tables:
                # Detect which column has the current year values
                # Skip header-like columns (Note, references)
                header_row = table[0] if table else []
                val_col_idx = None
                for idx, h in enumerate(header_row):
                    h_str = str(h).strip() if h else ""
                    if re.match(r"20\d{2}$", h_str) or h_str in ("Montant", "Valeur"):
                        val_col_idx = idx
                        break
                # If no header detected, use column 2 (skip label + note)
                if val_col_idx is None:
                    val_col_idx = 2 if len(header_row) > 3 else 1

                for row in table:
                    if not row or not row[0]:
                        continue

                    # Handle multi-line cells: take first line only
                    label_full = str(row[0]).strip()
                    label = label_full.split("\n")[0].strip().lower()

                    # Get value from the detected column
                    val = None
                    cell_text = str(row[val_col_idx]).strip() if val_col_idx < len(row) and row[val_col_idx] else ""
                    # Handle multi-line values: take first line
                    cell_text = cell_text.split("\n")[0].strip()
                    val = _parse_amount(cell_text)

                    if val is None or abs(val) < 0.001:
                        continue

                    # Match patterns
                    if re.search(r"chiffre\s*d.affaires|produits?\s*d.exploitation|revenus?\s*nets?", label):
                        if data["revenue"] is None:
                            data["revenue"] = val * mult

                    elif re.search(r"r.sultat\s*net", label):
                        data["net_income"] = val * mult

                    elif re.search(r"r.sultat\s*d.exploitation|r.sultat\s*op.rationnel", label):
                        data["ebit"] = val * mult

                    elif re.search(r"capitaux\s*propres|fonds\s*propres", label):
                        if data["equity"] is None:
                            data["equity"] = val * mult

                    elif re.search(r"total\s*(de\s*l.)?actif|total\s*bilan", label):
                        data["total_assets"] = val * mult

                    elif re.search(r"dettes?\s*financi.res|emprunts?\s*et\s*dettes|endettement\s*net", label):
                        if data["total_debt"] is None:
                            data["total_debt"] = abs(val) * mult

                    elif re.search(r"co.t\s*de\s*l.endettement|charges?\s*d.int.r.ts?|charges?\s*financi.res?", label):
                        data["interest_expense"] = abs(val) * mult

            # Also search in plain text for common patterns
            if data["net_income"] is None:
                match = re.search(r"r.sultat\s*net[^:]*?:\s*([\d\s,\.]+)", text_lower)
                if match:
                    val = _parse_amount(match.group(1))
                    if val:
                        data["net_income"] = val * mult

    except Exception as e:
        data["error"] = str(e)

    return data


def download_and_extract(url: str) -> dict:
    """Telecharge un PDF et extrait les donnees financieres."""
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        result = extract_from_pdf(tmp_path)
        import os
        os.unlink(tmp_path)
        return result
    except Exception as e:
        return {"error": str(e)}


def extract_all_reports(progress_callback=None):
    """
    Pour chaque rapport PDF en base, telecharge et extrait les donnees financieres.
    Met a jour la table fundamentals avec les donnees extraites.
    """
    reports = get_report_links()
    if reports.empty:
        return 0

    # Only process etats_financiers PDFs
    pdf_reports = reports[
        (reports["report_type"].isin(["etats_financiers"])) &
        (reports["url"].str.endswith(".pdf"))
    ]

    tickers_data = load_tickers()
    ticker_meta = {t["ticker"]: t for t in tickers_data}

    updated = 0
    for i, (_, report) in enumerate(pdf_reports.iterrows()):
        ticker = report["ticker"]
        year = int(report["fiscal_year"]) if report.get("fiscal_year") else None
        url = report["url"]

        if progress_callback:
            progress_callback(i, len(pdf_reports), ticker)

        # Check if we already have fundamentals for this ticker/year
        conn = get_connection()
        existing = conn.execute(
            "SELECT revenue, net_income FROM fundamentals WHERE ticker=? AND fiscal_year=?",
            (ticker, year),
        ).fetchone()
        conn.close()

        if existing and existing["revenue"] and existing["net_income"]:
            continue  # Already have complete data

        result = download_and_extract(url)
        if "error" in result:
            continue

        # Only save if we got meaningful data
        if result.get("revenue") or result.get("net_income") or result.get("equity"):
            meta = ticker_meta.get(ticker, {})

            # Get existing price and shares from market_data
            conn = get_connection()
            market = conn.execute(
                "SELECT price, shares, dps FROM market_data WHERE ticker=?", (ticker,)
            ).fetchone()
            conn.close()

            fund_data = {
                "ticker": ticker,
                "company_name": meta.get("name", ""),
                "sector": meta.get("sector", ""),
                "currency": "XOF",
                "fiscal_year": year,
                "price": market["price"] if market else 0,
                "shares": market["shares"] if market and market["shares"] else None,
                "revenue": result.get("revenue"),
                "net_income": result.get("net_income"),
                "equity": result.get("equity"),
                "total_debt": result.get("total_debt"),
                "ebit": result.get("ebit"),
                "interest_expense": result.get("interest_expense"),
                "dps": market["dps"] if market else None,
            }
            save_fundamentals(fund_data)
            updated += 1

    return updated
