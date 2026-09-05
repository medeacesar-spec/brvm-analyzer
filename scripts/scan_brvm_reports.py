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
# Le nom de fichier brvm.org commence par la date de PUBLICATION, qui n'est
# pas l'exercice : « 20260611_-_rapport_de_gestion_-_exercice_2025_... » a ete
# publie en 2026 et porte sur 2025. On retire donc ce prefixe avant toute
# recherche d'annee.
_PREFIXE_PUBLICATION = re.compile(r"^\d{8}[_-]+")
_ANNEE = re.compile(r"(?:19|20)\d{2}")
_ANNEE_EXERCICE = re.compile(r"exercices?[_-]((?:19|20)\d{2})", re.I)

# Certaines filiales publient sous intitule anglais.
_ETATS = re.compile(
    r"etats?[_-]financiers?|resultats?[_-]financiers?|(?:^|[_-])efp(?=[_.-])"
    r"|financial[_-]statements?", re.I)
_ANNUEL = re.compile(
    r"rapport[_-](?:annuel[_-])?de[_-]gestion|rapport[_-]annuel"
    r"|rapport[_-]dactivit[eé]s?[_-]annuel|annual[_-]report|(?:^|[_-])r[ga](?=[_.-])",
    re.I)
_SEMESTRE = re.compile(
    r"1er[_-]semestre|premier[_-]semestre"
    r"|2(?:nd|[eé]me|d)[_-]semestre|second[_-]semestre|deuxi[eè]me[_-]semestre", re.I)
_TRIMESTRE = re.compile(r"[1-4](?:er|[eé]me|nd|d)?[_-]trimestre|trimestriel", re.I)
_ACTIVITES = re.compile(r"rapport[_-]dactivit[eé]s?", re.I)
_ANNEE_APRES_ETAT = re.compile(
    r"(?:etats?[_-]financiers?|resultats?[_-]financiers?"
    r"|financial[_-]statements?)[_-]*((?:19|20)\d{2})", re.I)

# Un document porteur de chiffres, meme accompagne d'une attestation, doit
# etre lu : « attestation_des_cac_sur_le_rapport_dactivites_du_1er_semestre »
# EST le rapport semestriel. On n'ecarte que les pieces qui ne portent aucun
# etat : communiques, convocations, resolutions, rapport RSE.
_SANS_CHIFFRES = re.compile(
    r"rapport[_-]rse|communiqu[eé]|convocation|ordre[_-]du[_-]jour"
    r"|proces[_-]verbal|resolutions?[_-]", re.I)


def _classify_pdf(url: str):
    """Retourne (report_type, fiscal_year) depuis l'URL du PDF brvm.org.

    Retourne (None, None) si non reconnaissable ou sans interet chiffre.

    L'ordre des tests conserve le comportement anterieur sur les documents
    deja collectes : un fichier portant « etats financiers » ET « exercice
    AAAA » reste un jeu d'etats annuels, meme s'il mentionne aussi un
    trimestre. Les formes nouvelles ne sont examinees qu'ensuite.

    L'annee vient d'abord de « exercice AAAA », qui ne ment pas. A defaut —
    un fichier sur sept ne la porte pas, « rapport_annuel_2023_-_eviosys… »
    par exemple — on prend la DERNIERE annee du nom, la date de publication
    ayant ete retiree au prealable.
    """
    fichier = url.rsplit("/", 1)[-1].lower()
    corps = _PREFIXE_PUBLICATION.sub("", fichier)

    porte_un_etat = (_ETATS.search(corps) or _ANNUEL.search(corps)
                     or _ACTIVITES.search(corps))
    if _SANS_CHIFFRES.search(corps) and not porte_un_etat:
        return None, None

    exercice = _ANNEE_EXERCICE.search(corps)
    genre = None

    if exercice and _ETATS.search(corps):
        genre = "etats_financiers"
    elif exercice and (_ANNUEL.search(corps) or _ACTIVITES.search(corps)) \
            and not _SEMESTRE.search(corps) and not _TRIMESTRE.search(corps):
        genre = "rapport_annuel"
    elif _ETATS.search(corps) and (_SEMESTRE.search(corps)
                                   or _TRIMESTRE.search(corps)) \
            and _ANNEE_APRES_ETAT.search(corps):
        # Document combine : « etats financiers 2024 ET rapport d'activites du
        # 1er trimestre 2025 ». L'annuel porte la substance, le trimestre n'est
        # qu'une annexe. On retient donc l'annuel — et l'annee qui SUIT le
        # libelle des etats, non la derniere du nom, qui est celle du
        # trimestre.
        genre = "etats_financiers"
        return genre, int(_ANNEE_APRES_ETAT.search(corps).group(1))
    elif _SEMESTRE.search(corps):
        genre = "rapport_semestriel"
    elif _TRIMESTRE.search(corps):
        genre = "rapport_trimestriel"
    elif _ETATS.search(corps):
        genre = "etats_financiers"
    elif _ANNUEL.search(corps) or _ACTIVITES.search(corps):
        genre = "rapport_annuel"

    if not genre:
        return None, None
    if exercice:
        return genre, int(exercice.group(1))
    annees = _ANNEE.findall(corps)
    if not annees:
        return None, None
    return genre, int(annees[-1])


def _make_title(report_type: str, year: int, ticker: str) -> str:
    """Libelle lisible du document, tel qu'il apparait dans les listes."""
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
