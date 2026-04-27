#!/usr/bin/env python3
"""Scrape brvm.org/fr/rapports-societe-cotes/{slug} pour collecter les liens
PDF de chaque société et les inserer dans la table `report_links`.

Une fois les URLs en base, `extract_pdfs.py` peut les traiter pour remplir
`fundamentals` / `quarterly_data`.

Usage:
    python3 scripts/scan_brvm_reports.py [--ticker TICKER]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402


# Mapping ticker BRVM (avec suffixe pays) → slug brvm.org/fr/rapports-societe-cotes/{slug}
TICKER_TO_BRVM_SLUG = {
    # Banques
    "BICC.ci": "bici-ci",
    "BICB.bj": "biic",                          # BICI Bénin = BIIC
    "BOAB.bj": "bank-africa-bn",                # Bénin = bn (et non bj)
    "BOABF.bf": "bank-africa-bf",
    "BOAC.ci": "bank-africa-ci",
    "BOAM.ml": "bank-africa-ml",
    "BOAN.ne": "bank-africa-ng",                # Niger = ng
    "BOAS.sn": "bank-africa-sn",
    "CBIBF.bf": "coris-bank-international",
    "ECOC.ci": "ecobank-ci",
    "ETIT.tg": "ecobank-tg",
    "NSBC.ci": "nsbc",
    "ORGT.tg": "oragroup",
    "SGBC.ci": "sgci",                          # SGB CI = sgci sur brvm.org
    "SIBC.ci": "sib",
    # Telecoms / utilities
    "ONTBF.bf": "onatel-bf",
    "ORAC.ci": "orange-ci",
    "SNTS.sn": "sonatel",
    "CIEC.ci": "cie-ci",
    "SDCC.ci": "sodeci",
    # Distribution / Industrie
    "ABJC.ci": "servair-abidjan-ci",
    "BNBC.ci": "bernabe-ci",
    "CABC.ci": "sicable",
    "CFAC.ci": "cfao-motors-ci",
    "FTSC.ci": "filtisac-ci",
    "NEIC.ci": "nei-ceda-ci",
    "NTLC.ci": "nestle-ci",
    "PRSC.ci": "tractafric-ci",
    "SAFC.ci": "safca-ci",
    "SCRC.ci": "sucrivoire",
    "SDSC.ci": "bollore-transport-logistics",   # AGL = ex-Bollore Transport
    "SEMC.ci": "crown-siem-ci",
    "SHEC.ci": "vivo-energy-ci",
    "SICC.ci": "sicor",
    "SIVC.ci": "air-liquide-ci",                # Erium = ex-Air Liquide CI
    "SLBC.ci": "solibra",
    "SMBC.ci": "smb",
    "SOGC.ci": "sogb",
    "SPHC.ci": "saph-ci",
    "STAC.ci": "setao-ci",
    "STBC.ci": "sitab",
    "SVOC.ci": "movis-ci",
    "TTLC.ci": "total",
    "TTLS.sn": "total-senegal-sa",
    "UNLC.ci": "unilever-ci",
    "UNXC.ci": "uniwax-ci",
    "LNBB.bj": "lnb",
    "PALC.ci": "palm-ci",
}


# Détection du type + fiscal_year depuis le nom de fichier brvm.org
# Exemple : 20260423_-_rapport_dactivites_annuel_-_exercice_2025_-_coris_bank_international_bf.pdf
_PDF_PATTERNS = [
    # rapport annuel
    (re.compile(r"rapport[_-]dactivit[eé]s?[_-]annuel.*exercice[_-](\d{4})", re.I),
     "rapport_annuel"),
    (re.compile(r"rapport[_-]annuel.*exercice[_-](\d{4})", re.I),
     "rapport_annuel"),
    (re.compile(r"rapport[_-]dactivit[eé]s?[_-]exercice[_-](\d{4})", re.I),
     "rapport_annuel"),
    # etats financiers
    (re.compile(r"etats?[_-]financiers?.*exercice[_-](\d{4})", re.I),
     "etats_financiers"),
    # semestriel
    (re.compile(r"rapport[_-]dactivit[eé]s?.*1er[_-]semestre[_-](\d{4})", re.I),
     "rapport_semestriel"),
    (re.compile(r"rapport[_-]dactivit[eé]s?.*2[eé]me[_-]semestre[_-](\d{4})", re.I),
     "rapport_semestriel"),
    # trimestriel
    (re.compile(r"rapport[_-]dactivit[eé]s?.*1er[_-]trimestre[_-](\d{4})", re.I),
     "rapport_trimestriel"),
    (re.compile(r"rapport[_-]dactivit[eé]s?.*2[eé]me[_-]trimestre[_-](\d{4})", re.I),
     "rapport_trimestriel"),
    (re.compile(r"rapport[_-]dactivit[eé]s?.*3[eé]me[_-]trimestre[_-](\d{4})", re.I),
     "rapport_trimestriel"),
    (re.compile(r"rapport[_-]dactivit[eé]s?.*4[eé]me[_-]trimestre[_-](\d{4})", re.I),
     "rapport_trimestriel"),
]


def _classify_pdf(url: str) -> tuple[str | None, int | None]:
    """Retourne (report_type, fiscal_year) depuis l'URL du PDF brvm.org.
    Retourne (None, None) si non reconnaissable.
    """
    fname = url.rsplit("/", 1)[-1].lower()
    for pat, kind in _PDF_PATTERNS:
        m = pat.search(fname)
        if m:
            try:
                year = int(m.group(1))
            except (ValueError, IndexError):
                year = None
            return kind, year
    return None, None


def _make_title(report_type: str, year: int, ticker: str) -> str:
    name = ticker.split(".")[0]
    type_label = {
        "rapport_annuel": "Rapport activités annuel",
        "etats_financiers": "Etats financiers",
        "rapport_semestriel": "Rapport activités semestriel",
        "rapport_trimestriel": "Rapport activités trimestriel",
    }.get(report_type, report_type)
    return f"{type_label} {year} - {name}"


def scrape_company_pdfs(slug: str, session: requests.Session) -> list[dict]:
    """Retourne la liste des PDFs trouvés sur la page société brvm.org.

    Chaque dict contient : url, report_type, fiscal_year.
    """
    url = f"https://www.brvm.org/fr/rapports-societe-cotes/{slug}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ! [{slug}] HTTP error: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    pdfs: list[dict] = []
    seen_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.lower().endswith(".pdf"):
            continue
        # Normalise URL (parfois relative)
        if href.startswith("/"):
            href = f"https://www.brvm.org{href}"
        if href in seen_urls:
            continue
        seen_urls.add(href)
        report_type, year = _classify_pdf(href)
        if not report_type or year is None:
            continue
        pdfs.append({
            "url": href, "report_type": report_type, "fiscal_year": year,
        })
    return pdfs


def save_to_report_links(
    conn, ticker: str, pdfs: list[dict],
) -> tuple[int, int]:
    """INSERT OR IGNORE les PDFs dans report_links. Retourne (added, skipped)."""
    added = 0
    skipped = 0
    for pdf in pdfs:
        title = _make_title(pdf["report_type"], pdf["fiscal_year"], ticker)
        try:
            cur = conn.execute(
                """INSERT INTO report_links
                   (ticker, title, report_type, fiscal_year, url, source)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ticker, url) DO NOTHING""",
                (
                    ticker, title, pdf["report_type"], pdf["fiscal_year"],
                    pdf["url"], "brvm.org",
                ),
            )
            rc = cur.rowcount if hasattr(cur, "rowcount") else 1
            if rc > 0:
                added += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  ! [{ticker}] insert error: {e}")
            skipped += 1
    return added, skipped


def main(only_ticker: str | None = None):
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (BRVM analyzer)"})

    targets = TICKER_TO_BRVM_SLUG
    if only_ticker:
        if only_ticker not in targets:
            print(f"Ticker {only_ticker} not in mapping. Aborting.")
            return
        targets = {only_ticker: targets[only_ticker]}

    print(f"Scanning brvm.org pour {len(targets)} sociétés…\n")

    conn = get_connection()
    total_added = 0
    total_skipped = 0
    total_pdfs = 0
    for i, (ticker, slug) in enumerate(targets.items(), 1):
        try:
            pdfs = scrape_company_pdfs(slug, session)
        except Exception as e:
            print(f"  [{i}/{len(targets)}] {ticker} ({slug}) ! {e}")
            continue
        added, skipped = save_to_report_links(conn, ticker, pdfs)
        total_pdfs += len(pdfs)
        total_added += added
        total_skipped += skipped
        print(f"  [{i}/{len(targets)}] {ticker:10} ({slug:30}) "
              f"{len(pdfs)} PDF · +{added} new · {skipped} dup")
        # Rate-limit gentle (brvm.org pas blindé)
        time.sleep(0.4)
    conn.commit()
    conn.close()

    print()
    print("=" * 60)
    print(f"Total PDFs trouvés     : {total_pdfs}")
    print(f"Nouveaux insérés       : {total_added}")
    print(f"Doublons (déjà connus) : {total_skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Limit scan to a single ticker")
    args = parser.parse_args()
    main(only_ticker=args.ticker)
