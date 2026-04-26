#!/usr/bin/env python3
"""Surveille les publications récentes sur richbourse.com et sikafinance.com.
Détecte les états financiers, rapports trimestriels/annuels, assemblées, etc.
Peuple la table `publications` avec is_new=1 pour déclencher la bannière dashboard.

Usage:
    python3 scripts/scan_publications.py [--limit 50]
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

from config import DB_PATH, load_tickers, SIKA_BASE_URL


# Cloudflare sur richbourse bloque les User-Agents navigateurs modernes mais
# laisse passer les UA minimalistes style curl.
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


# ──────────────────────────────────────────────────────────────────────────
# Mapping ticker → slug connu sur richbourse
# ──────────────────────────────────────────────────────────────────────────

RICHBOURSE_NAME_TO_TICKER = {
    "bici-ci": "BICC.ci",
    "bicb-bn": "BICB.bj",
    "bicicibf": "CBIBF.bf",
    "boa-cote-divoire": "BOAC.ci",
    "boa-ci": "BOAC.ci",
    "boa-benin": "BOAB.bj",
    "boa-bn": "BOAB.bj",
    "boa-burkina": "BOABF.bf",
    "boa-bf": "BOABF.bf",
    "boa-mali": "BOAM.ml",
    "boa-ml": "BOAM.ml",
    "boa-niger": "BOAN.ne",
    "boa-ne": "BOAN.ne",
    "boa-senegal": "BOAS.sn",
    "boa-sn": "BOAS.sn",
    "sonatel": "SNTS.sn",
    "sonatel-sn": "SNTS.sn",
    "ecobank-ci": "ECOC.ci",
    "orange-ci": "ORAC.ci",
    "orac-ci": "ORAC.ci",
    "nsia-banque": "NSBC.ci",
    "nsia-banque-ci": "NSBC.ci",
    "societe-generale-ci": "SGBC.ci",
    "sgbc-ci": "SGBC.ci",
    "sgci": "SGBC.ci",
    "bernabe": "BNBC.ci",
    "bernabe-ci": "BNBC.ci",
    "cfao": "CFAC.ci",
    "cfao-motors-ci": "CFAC.ci",
    "cfac-ci": "CFAC.ci",
    "cie-ci": "CIEC.ci",
    "ciec-ci": "CIEC.ci",
    "sodeci-ci": "SDCC.ci",
    "sdcc-ci": "SDCC.ci",
    "filtisac-ci": "FTSC.ci",
    "ftsc-ci": "FTSC.ci",
    "nestle-ci": "NTLC.ci",
    "nei-ceda": "NEIC.ci",
    "palmci": "PALC.ci",
    "palc-ci": "PALC.ci",
    "ontbf": "ONTBF.bf",
    "ontbf-bf": "ONTBF.bf",
    "safca-ci": "SAFC.ci",
    "siva-ci": "SIVC.ci",
    # SICABLE = CABC.ci (pas SICC.ci qui est SICOR). Corrigé d'après brvm.org.
    "sicable-ci": "CABC.ci",
    "cabc-ci": "CABC.ci",
    # SICOR = SICC.ci (à ne pas confondre avec SIVC = Erium / ex-Air Liquide)
    "sicor-ci": "SICC.ci",
    "sicc-ci": "SICC.ci",
    "sib-ci": "SIBC.ci",
    "sibc-ci": "SIBC.ci",
    "solibra-ci": "SLBC.ci",
    "smb-ci": "SMBC.ci",
    "sogb-ci": "SOGC.ci",
    "sph-ci": "SPHC.ci",
    "sitab-ci": "STBC.ci",
    "stac-ci": "STAC.ci",
    "sucrivoire-ci": "SCRC.ci",
    "total-ci": "TTLC.ci",
    "ttlc-ci": "TTLC.ci",
    "totalenergies-marketing-ci": "TTLC.ci",
    "totalenergies-ci": "TTLC.ci",
    "total-senegal": "TTLS.sn",
    "totalenergies-senegal": "TTLS.sn",
    "totalenergies-marketing-senegal": "TTLS.sn",
    "tractafric-motors-ci": "PRSC.ci",
    "unilever-ci": "UNLC.ci",
    "uniwax-ci": "UNXC.ci",
    "vivo-energy-ci": "SHEC.ci",
    "shec-ci": "SHEC.ci",
    "loterie-nationale": "LNBB.bj",
    "lnb-bj": "LNBB.bj",
    "loterie-nationale-du-benin": "LNBB.bj",
    "sdsc-ci": "SDSC.ci",
    "stade-dabidjan": "ABJC.ci",
    "abj-ci": "ABJC.ci",
    # Coris Bank International Burkina Faso = CBIBF.bf (pas CABC.ci qui est Sicable)
    "coris-bank-international-bf": "CBIBF.bf",
    "coris-bank-bf": "CBIBF.bf",
    "coris-bank": "CBIBF.bf",
    "cbibf": "CBIBF.bf",
    # SEMC = EVIOSYS PACKAGING SIEM (ex-Crown Siem). Servair = ABJC, pas SEMC.
    "semc-ci": "SEMC.ci",
    "siem-ci": "SEMC.ci",
    "crown-siem-ci": "SEMC.ci",
    "eviosys-packaging-siem-ci": "SEMC.ci",
    "servair-ci": "ABJC.ci",
    "servair-abidjan": "ABJC.ci",
    "sdsc-ci": "SDSC.ci",
    "setao-ci": "STAC.ci",
    "movis-ci": "SVOC.ci",
    "air-liquide-ci": "SIVC.ci",
    "orgt-tg": "ORGT.tg",
    "oragroup": "ORGT.tg",
    "eti-tg": "ETIT.tg",
    "ecobank-transnational": "ETIT.tg",
}


def _detect_ticker_from_slug(slug: str) -> str:
    """Essaie d'extraire un ticker BRVM depuis un slug richbourse."""
    slug_lower = slug.lower()
    # Try longest first
    for key, ticker in sorted(RICHBOURSE_NAME_TO_TICKER.items(), key=lambda x: -len(x[0])):
        if key in slug_lower:
            return ticker
    return None


def _detect_pub_type(text: str) -> tuple:
    """Retourne (pub_type, period, is_financial). Accepte texte ou slug (tirets)."""
    # Normaliser : remplacer tirets et underscores par espaces, minuscules
    t = text.lower().replace("-", " ").replace("_", " ")
    if ("trimestriel" in t or "1er trimestre" in t or "2eme trimestre" in t
            or "3eme trimestre" in t or "2e trimestre" in t or "3e trimestre" in t
            or "4eme trimestre" in t or "4e trimestre" in t):
        return ("trimestriel", None, True)
    if "semestriel" in t or "1er semestre" in t or "2eme semestre" in t or "2e semestre" in t:
        return ("semestriel", None, True)
    if "etats financiers" in t or "états financiers" in t:
        return ("annuel", None, True)
    if "rapport d" in t and "annuel" in t:
        return ("annuel", None, True)
    if "exercice" in t and ("etats" in t or "rapport" in t):
        return ("annuel", None, True)
    if "assemblee generale" in t or "assemblée générale" in t:
        return ("gouvernance", None, False)
    if "dividende" in t:
        return ("dividende", None, False)
    if "augmentation de capital" in t:
        return ("corporate", None, False)
    return ("autre", None, False)


def _extract_fiscal_year(text: str) -> int:
    """Extrait l'année fiscale d'un titre/slug (ex: 'exercice 2025' → 2025)."""
    t = text.lower().replace("-", " ").replace("_", " ")
    m = re.search(r"exercice\s+(\d{4})", t)
    if m:
        return int(m.group(1))
    m = re.search(r"trimestre\s+(\d{4})", t)
    if m:
        return int(m.group(1))
    m = re.search(r"semestre\s+(\d{4})", t)
    if m:
        return int(m.group(1))
    return None


# ──────────────────────────────────────────────────────────────────────────
# Scrapers
# ──────────────────────────────────────────────────────────────────────────

def scan_richbourse(limit: int = 50) -> list:
    """Scrape la liste des actualités récentes de richbourse.com.
    La page /common/actualite/index renvoie 403 sans session. On utilise donc
    la page d'accueil (accessible) qui liste les ~15 dernières actualités.
    """
    session = requests.Session()
    session.headers.update(HEADERS_RB)

    resp = None
    for url in [
        "https://www.richbourse.com/common/actualite/index",
        "https://www.richbourse.com/",
    ]:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                break
        except requests.RequestException:
            continue

    if resp is None or resp.status_code != 200:
        raise RuntimeError(f"Impossible d'accéder à richbourse (dernier statut: {resp.status_code if resp else 'N/A'})")

    soup = BeautifulSoup(resp.text, "lxml")

    publications = []
    seen_slugs = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"/common/actualite/details/(.+)$", href)
        if not m:
            continue
        slug = m.group(1)
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        # Extract date
        date_match = re.match(r"(\d{2})-(\d{2})-(\d{4})-(.+)$", slug)
        if not date_match:
            continue
        day, mon, year, rest = date_match.groups()
        pub_date = f"{year}-{mon}-{day}"

        # Normalize + prettify title (slug → titre lisible avec accents)
        title = rest.replace("-", " ").strip()
        title = re.sub(r"\s+", " ", title)
        try:
            from utils.text import prettify_publication_title
            title = prettify_publication_title(title)
        except Exception:
            title = title.capitalize()

        ticker = _detect_ticker_from_slug(rest)
        pub_type, period, is_financial = _detect_pub_type(rest)
        fiscal_year = _extract_fiscal_year(rest)

        publications.append({
            "ticker": ticker or "",
            "title": title,
            "type": pub_type,
            "period": period,
            "url": f"https://www.richbourse.com{href}",
            "date": pub_date,
            "fiscal_year": fiscal_year,
            "source": "richbourse",
            "is_financial": is_financial,
        })

        if len(publications) >= limit:
            break

    return publications


def scan_sikafinance(limit: int = 30) -> list:
    """Scrape les actualités récentes de sikafinance.com."""
    url = f"{SIKA_BASE_URL}/marches/actualites_bourse_brvm"
    try:
        resp = requests.get(url, headers=HEADERS_SIKA, timeout=30)
        resp.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    publications = []

    # Sika actualités sont typiquement des liens vers /common/actualites_bourse_brvm_*
    # ou /marches/actus_*
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/actualites" not in href.lower() and "/actus" not in href.lower():
            continue
        title = a.get_text(strip=True)
        if len(title) < 15 or len(title) > 250:
            continue
        if title in seen:
            continue
        seen.add(title)

        full_url = href if href.startswith("http") else f"{SIKA_BASE_URL}{href}"
        pub_type, period, is_financial = _detect_pub_type(title)
        fiscal_year = _extract_fiscal_year(title)

        publications.append({
            "ticker": "",  # sika doesn't put ticker in URL
            "title": title,
            "type": pub_type,
            "period": period,
            "url": full_url,
            "date": None,
            "fiscal_year": fiscal_year,
            "source": "sikafinance",
            "is_financial": is_financial,
        })

        if len(publications) >= limit:
            break

    return publications


# ──────────────────────────────────────────────────────────────────────────
# DB integration
# ──────────────────────────────────────────────────────────────────────────

def save_publications(publications: list) -> int:
    """Sauvegarde les publications, ignorant les doublons. Retourne le nombre de NOUVELLES."""
    conn = get_connection()
    n_new = 0
    for p in publications:
        try:
            cur = conn.execute(
                """INSERT OR IGNORE INTO publications
                   (ticker, title, pub_type, period, url, pub_date, fiscal_year,
                    source, is_new)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    p["ticker"], p["title"], p["type"], p.get("period"),
                    p["url"], p["date"], p.get("fiscal_year"), p["source"],
                ),
            )
            if cur.rowcount > 0:
                n_new += 1
        except sqlite3.Error as e:
            print(f"[WARN] Failed to insert {p.get('title')}: {e}")
    conn.commit()
    conn.close()
    return n_new


def main(limit: int = 50):
    print(f"Scanning publications (limit={limit})…\n")

    # Ensure the publications table has source + fiscal_year columns
    conn = get_connection()
    cols = [row[1] for row in conn.execute("PRAGMA table_info(publications)").fetchall()]
    if "source" not in cols:
        conn.execute("ALTER TABLE publications ADD COLUMN source TEXT")
    if "fiscal_year" not in cols:
        conn.execute("ALTER TABLE publications ADD COLUMN fiscal_year INTEGER")
    conn.commit()
    conn.close()

    all_pubs = []

    print("→ richbourse.com …")
    try:
        rb = scan_richbourse(limit=limit)
        print(f"   {len(rb)} publications trouvées")
        all_pubs.extend(rb)
    except Exception as e:
        print(f"   ERREUR : {e}")

    print("→ sikafinance.com …")
    try:
        sika = scan_sikafinance(limit=limit)
        print(f"   {len(sika)} publications trouvées")
        all_pubs.extend(sika)
    except Exception as e:
        print(f"   ERREUR : {e}")

    # Save
    n_new = save_publications(all_pubs)
    print(f"\n✅ {n_new} nouvelles publications ajoutées (total scanné: {len(all_pubs)})")

    if all_pubs:
        print("\nExemples :")
        financial = [p for p in all_pubs if p.get("is_financial")]
        for p in financial[:10]:
            tk = p["ticker"] or "?"
            print(f"  [{tk:<10} {p['date'] or '—':<10}] {p['type']:<12} {p['title'][:80]}")


if __name__ == "__main__":
    limit = 50
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        try:
            limit = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            pass
    main(limit=limit)
