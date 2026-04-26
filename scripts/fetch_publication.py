#!/usr/bin/env python3
"""Télécharge le PDF d'une publication et rafraîchit les données sika du ticker associé.
Fonction `auto_fetch_publication(pub_id)` utilisable depuis Streamlit.
"""
import os
import re
import sys
import urllib.parse

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH
from data.storage import get_connection, mark_publication_integrated


PDF_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pdfs")

HEADERS_RB = {
    "User-Agent": "curl/8.7.1",
    "Accept": "*/*",
}
HEADERS_SIKA = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def _safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-. ]", "_", s.strip().lower())
    return re.sub(r"_+", "_", s)[:120]


def _find_pdf_in_page(html: str, base_url: str) -> str:
    """Cherche un lien direct vers un PDF dans le HTML de la page de détails."""
    soup = BeautifulSoup(html, "lxml")
    # Direct PDF link
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            if href.startswith("http"):
                return href
            return urllib.parse.urljoin(base_url, href)
    # data attributes ou boutons download
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "download" in href.lower() or "document" in href.lower():
            return urllib.parse.urljoin(base_url, href)
    return None


def download_pdf(url: str, ticker: str, title: str) -> str:
    """Télécharge un PDF depuis une URL richbourse ou sika.
    Retourne le chemin local du fichier téléchargé, ou None."""
    # Choose right headers based on domain
    is_rb = "richbourse" in url
    headers = HEADERS_RB if is_rb else HEADERS_SIKA

    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[fetch] Error opening {url}: {e}")
        return None

    content_type = r.headers.get("Content-Type", "").lower()
    # If it's already the PDF → save it
    if "pdf" in content_type:
        pdf_bytes = r.content
        pdf_url = url
    else:
        # It's the details HTML page → find the PDF inside
        pdf_url = _find_pdf_in_page(r.text, url)
        if not pdf_url:
            print(f"[fetch] No PDF link found in {url}")
            return None
        try:
            r2 = requests.get(pdf_url, headers=headers, timeout=60, allow_redirects=True)
            r2.raise_for_status()
            if "pdf" not in r2.headers.get("Content-Type", "").lower():
                print(f"[fetch] {pdf_url} did not return a PDF")
                return None
            pdf_bytes = r2.content
        except requests.RequestException as e:
            print(f"[fetch] Error downloading {pdf_url}: {e}")
            return None

    # Save to pdfs/{ticker}/
    tkr_dir = ticker if ticker else "_unknown"
    dest_dir = os.path.join(PDF_ROOT, tkr_dir)
    os.makedirs(dest_dir, exist_ok=True)
    filename = _safe_filename(title) + ".pdf"
    dest_path = os.path.join(dest_dir, filename)

    with open(dest_path, "wb") as fh:
        fh.write(pdf_bytes)
    return dest_path


def refresh_ticker_from_sika(ticker: str) -> dict:
    """Re-scrape la page société sikafinance pour un ticker et met à jour fundamentals."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
    from scrape_societe import scrape_societe, HEADERS

    session = requests.Session()
    session.headers.update(HEADERS)
    result = scrape_societe(session, ticker)

    if not result.get("financials"):
        return {"inserted": 0, "updated": 0, "error": "Aucune donnée sika"}

    conn = get_connection()
    inserted = 0
    updated = 0
    for year, fin in result["financials"].items():
        existing = conn.execute(
            "SELECT id FROM fundamentals WHERE ticker = ? AND fiscal_year = ?",
            (ticker, year),
        ).fetchone()
        if existing:
            set_parts = []
            params = []
            for key in ("revenue", "net_income", "dps", "eps", "per",
                        "revenue_growth", "net_income_growth"):
                if fin.get(key) is not None:
                    set_parts.append(f"{key} = ?")
                    params.append(fin[key])
            if result.get("shares"):
                set_parts.append("shares = ?")
                params.append(result["shares"])
            if set_parts:
                set_parts.append("updated_at = CURRENT_TIMESTAMP")
                params.append(ticker)
                params.append(year)
                conn.execute(
                    f"UPDATE fundamentals SET {', '.join(set_parts)} WHERE ticker = ? AND fiscal_year = ?",
                    params,
                )
                updated += 1
        else:
            conn.execute(
                """INSERT INTO fundamentals
                   (ticker, fiscal_year, shares,
                    revenue, net_income, dps, eps, per,
                    revenue_growth, net_income_growth, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (ticker, year, result.get("shares"),
                 fin.get("revenue"), fin.get("net_income"),
                 fin.get("dps"), fin.get("eps"), fin.get("per"),
                 fin.get("revenue_growth"), fin.get("net_income_growth")),
            )
            inserted += 1
    conn.commit()
    conn.close()

    return {"inserted": inserted, "updated": updated}


def complete_missing_fundamentals_from_sika(
    ticker: str, fiscal_year: int = None,
) -> dict:
    """Complete les champs MANQUANTS (NULL) dans fundamentals depuis sikafinance.

    Contrairement a refresh_ticker_from_sika qui ECRASE, cette fonction ne
    touche que les champs NULL — preserve les valeurs deja en base
    (notamment celles extraites du PDF qui sont plus precises que sika).

    Args:
        ticker  : ticker BRVM (ex 'CBIBF.bf')
        fiscal_year : annee specifique (None = toutes les annees sika)

    Retourne {
        'inserted': int,        # nouvelles rows creees
        'updated': int,         # rows mises a jour (au moins 1 champ comble)
        'fields_filled': dict,  # {year: [list of fields filled]}
        'error': str | None,
    }
    """
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
    from scrape_societe import scrape_societe, HEADERS

    session = requests.Session()
    session.headers.update(HEADERS)
    result = scrape_societe(session, ticker)

    if not result.get("financials"):
        return {
            "inserted": 0, "updated": 0, "fields_filled": {},
            "error": "Aucune donnée sikafinance pour ce ticker",
        }

    conn = get_connection()
    inserted = 0
    updated = 0
    fields_filled: dict = {}

    for year, fin in result["financials"].items():
        if fiscal_year is not None and int(year) != int(fiscal_year):
            continue

        existing_row = conn.execute(
            """SELECT revenue, net_income, dps, eps, per,
                       revenue_growth, net_income_growth, shares
               FROM fundamentals
               WHERE ticker = ? AND fiscal_year = ?""",
            (ticker, year),
        ).fetchone()

        if existing_row is None:
            # Pas de row — INSERT avec ce que sika a
            conn.execute(
                """INSERT INTO fundamentals
                   (ticker, fiscal_year, shares,
                    revenue, net_income, dps, eps, per,
                    revenue_growth, net_income_growth, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (
                    ticker, year, result.get("shares"),
                    fin.get("revenue"), fin.get("net_income"),
                    fin.get("dps"), fin.get("eps"), fin.get("per"),
                    fin.get("revenue_growth"), fin.get("net_income_growth"),
                ),
            )
            inserted += 1
            filled = [k for k in (
                "revenue", "net_income", "dps", "eps", "per",
                "revenue_growth", "net_income_growth",
            ) if fin.get(k) is not None]
            if result.get("shares"):
                filled.append("shares")
            fields_filled[int(year)] = filled
            continue

        # Row existante — ne touche QUE les NULL
        existing_dict = dict(existing_row) if isinstance(existing_row, dict) else {
            "revenue": existing_row[0], "net_income": existing_row[1],
            "dps": existing_row[2], "eps": existing_row[3], "per": existing_row[4],
            "revenue_growth": existing_row[5], "net_income_growth": existing_row[6],
            "shares": existing_row[7],
        }
        set_parts = []
        params = []
        filled = []
        for key in ("revenue", "net_income", "dps", "eps", "per",
                     "revenue_growth", "net_income_growth"):
            if existing_dict.get(key) is None and fin.get(key) is not None:
                set_parts.append(f"{key} = ?")
                params.append(fin[key])
                filled.append(key)
        if existing_dict.get("shares") is None and result.get("shares"):
            set_parts.append("shares = ?")
            params.append(result["shares"])
            filled.append("shares")

        if set_parts:
            set_parts.append("updated_at = CURRENT_TIMESTAMP")
            params.append(ticker)
            params.append(year)
            conn.execute(
                f"UPDATE fundamentals SET {', '.join(set_parts)} "
                f"WHERE ticker = ? AND fiscal_year = ?",
                params,
            )
            updated += 1
            fields_filled[int(year)] = filled

    conn.commit()
    conn.close()
    return {
        "inserted": inserted, "updated": updated,
        "fields_filled": fields_filled, "error": None,
    }


def _is_financial_statement(title: str, pub_type: str = None) -> bool:
    """Indique si la publication est un état financier / rapport d'activité à télécharger.
    On ignore : convocations, AG, dividendes, augmentations de capital, notes d'info, franchissements."""
    t = (title or "").lower()
    if pub_type in ("annuel", "trimestriel", "semestriel"):
        return True
    keywords_yes = [
        "etats financiers", "états financiers", "rapport dactivites",
        "rapport d'activites", "rapport d'activités", "rapport dactivités",
        "rapport annuel", "1er trimestre", "2eme trimestre", "3eme trimestre",
        "1er semestre", "exercice 2",
    ]
    keywords_no = [
        "assemblee generale", "assemblée générale", "convocation",
        "paiement de dividendes", "augmentation de capital",
        "note dinformation", "franchissement de seuil", "transaction sur dossier",
    ]
    if any(k in t for k in keywords_no):
        return False
    return any(k in t for k in keywords_yes)


def auto_fetch_publication(pub_id: int) -> dict:
    """Action globale pour une publication donnée :
    1. Re-scrape sika pour le ticker (met à jour fundamentals)
    2. Télécharge le PDF seulement si c'est un état financier / rapport d'activités
    3. Marque is_new=0

    Retourne un résumé avec ticker, pdf_path, sika_result, success."""
    conn = get_connection()
    row = conn.execute(
        "SELECT ticker, title, url, pub_type FROM publications WHERE id = ?", (pub_id,),
    ).fetchone()
    conn.close()

    if not row:
        return {"success": False, "error": "Publication introuvable"}

    ticker = row["ticker"]
    title = row["title"]
    url = row["url"]
    pub_type = row["pub_type"]

    result = {"ticker": ticker, "title": title, "success": False}

    # 1. Refresh sika data
    if ticker:
        try:
            sika_res = refresh_ticker_from_sika(ticker)
            result["sika"] = sika_res
        except Exception as e:
            result["sika_error"] = str(e)
    else:
        result["sika"] = {"error": "Pas de ticker associé"}

    # 2. Download PDF only if this is a financial statement
    if url and _is_financial_statement(title, pub_type):
        try:
            pdf_path = download_pdf(url, ticker, title)
            result["pdf_path"] = pdf_path
        except Exception as e:
            result["pdf_error"] = str(e)
    elif url:
        result["pdf_skipped"] = "Pas un état financier (AG/dividende/etc.)"

    # 3. Mark integrated
    mark_publication_integrated(pub_id)

    result["success"] = (
        (result.get("sika", {}).get("updated", 0) > 0
         or result.get("sika", {}).get("inserted", 0) > 0)
        or result.get("pdf_path") is not None
    )
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 fetch_publication.py <pub_id>")
        sys.exit(1)
    pid = int(sys.argv[1])
    print(auto_fetch_publication(pid))
