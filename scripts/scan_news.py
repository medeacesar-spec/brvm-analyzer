#!/usr/bin/env python3
"""Collecte les depeches sikafinance et les rattache aux titres de la cote.

A ne pas confondre avec `scan_publications.py`, qui recense les publications
OFFICIELLES des emetteurs (rapports, etats financiers) et alimente le circuit
d'extraction. Ici on collecte du JOURNALISME : analyses de marche, actualite
sectorielle, macro — tout ce qui peut faire bouger un cours sans emaner de
l'emetteur lui-meme.

Les deux flux restent dans des tables separees : `publications` pilote les
alertes et l'extraction, `news_articles` alimente la revue de presse.

Usage :
  python3 scripts/scan_news.py            # collecte et enregistre
  python3 scripts/scan_news.py --dry-run  # affiche sans rien enregistrer
"""
from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_tickers  # noqa: E402
from data.db import get_connection  # noqa: E402

BASE = "https://www.sikafinance.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

# Rubriques suivies. La rubrique "communiques_brvm" est volontairement absente :
# elle ne contient que des liens PDF vers les publications officielles, deja
# couvertes par scan_publications.py + l'extraction.
SOURCES = [
    ("brvm", f"{BASE}/marches/actualites_bourse_brvm"),
    ("economie", f"{BASE}/marches/actualites_economiques_cemac"),
    ("senegal", f"{BASE}/marches/actualites_economiques_senegal"),
]

# Au-dela, on collecte du bruit : les rubriques rejouent leurs archives en
# bas de page. 15 par rubrique couvre largement une journee.
LIMITE_PAR_RUBRIQUE = 15

# ── Autres sources regionales, par flux RSS ───────────────────────────────
# Le RSS plutot que le scraping : un flux change de forme bien plus rarement
# qu'une page, et il porte deja titre, date et chapeau.
#
# Agence Ecofin est declaree alors qu'elle repond 403 a toute requete, y
# compris avec un en-tete de navigateur (verifie le 09/09/2026 sur six
# chemins differents ; seul /rss passe, et il rend un flux VIDE). Elle reste
# ici pour deux raisons : le jour ou le blocage tombe, la collecte reprend
# sans qu'on y touche ; et surtout un echec doit se VOIR dans le journal,
# pas disparaitre parce qu'on a retire la source de la liste.
FLUX_RSS = [
    ("financialafrik", "https://www.financialafrik.com/feed/"),
    ("jeuneafrique", "https://www.jeuneafrique.com/feed/"),
    ("agenceecofin", "https://www.agenceecofin.com/rss"),
]

LIMITE_PAR_FLUX = 30

# Une depeche sikafinance a une URL en /marches/<slug>_<id>
ARTICLE_RE = re.compile(r"^/marches/.+_(\d+)$")
DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")


def log(msg):
    print(msg, flush=True)


def _get(url, timeout=45):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# ──────────────────────────────────────────────────────────────────────────
# Rattachement aux societes cotees
# ──────────────────────────────────────────────────────────────────────────

def _build_index():
    """Index {expression a chercher: ticker}, du plus specifique au plus court.

    On repart du referentiel (48 societes) enrichi des alias de langage courant
    deja maintenus dans analysis/llm_chat.py.
    """
    index = {}
    for t in load_tickers():
        ticker = t["ticker"]
        nom = (t.get("name") or "").strip()
        if len(nom) >= 4:
            index[nom.lower()] = ticker
        # Le code sans suffixe pays ("SNTS") apparait souvent tel quel
        code = ticker.split(".")[0]
        if len(code) >= 4:
            index[code.lower()] = ticker

    try:
        from analysis.llm_chat import _TICKER_ALIASES
        for ticker, alias in _TICKER_ALIASES.items():
            for a in alias:
                if len(a) >= 4:
                    index[a.lower()] = ticker
    except Exception:
        pass

    return index


_INDEX = None


def detect_tickers(texte: str) -> list:
    """Tickers cites dans le texte, sans doublon, ordre d'apparition."""
    global _INDEX
    if _INDEX is None:
        _INDEX = _build_index()

    bas = " " + re.sub(r"\s+", " ", (texte or "").lower()) + " "
    trouves = []
    for expr, ticker in _INDEX.items():
        if ticker in trouves:
            continue
        # bornes de mot : evite que "sib" matche "possible"
        if re.search(r"(?<![\w-])" + re.escape(expr) + r"(?![\w-])", bas):
            trouves.append(ticker)
    return trouves


# Une depeche sur le cacao ne cite aucune societe cotee mais concerne
# directement les agro-industriels de la cote. Cette passerelle rattache un
# sujet sectoriel aux secteurs du referentiel, pour que la revue puisse dire
# quelles lignes sont exposees.
SUJETS_SECTORIELS = [
    (r"cacao|caf[ée]|h[ée]v[ée]a|caoutchouc|palmier|huile de palme|coton"
     r"|anacarde|campagne agricole|agro-industrie", "Agriculture"),
    (r"t[ée]l[ée]com|num[ée]rique|internet|mobile money|fibre|4g|5g|data center"
     r"|cloud", "Telecommunications"),
    (r"\bbanque|bancaire|cr[ée]dit|bceao|taux directeur|liquidit[ée] bancaire"
     r"|microfinance|assurance", "Banque"),
    (r"[ée]lectricit[ée]|\beau\b|distribution d.eau|[ée]nergie [ée]lectrique",
     "Services publics"),
    (r"carburant|p[ée]trole|raffinerie|\bgaz\b|hydrocarbure", "Distribution"),
    (r"\bciment\b|\bindustrie\b|\busine\b|manufactur|production industrielle",
     "Industrie"),
    # Bornes de mots indispensables : sans elles, "port" matche rapport,
    # important, exportation… et tout devenait du Transport.
    (r"\bports?\b|\blogistique\b|\btransport|\bfret\b|\bcorridor",
     "Transport"),
]


def detect_secteurs(texte: str) -> list:
    """Secteurs de la cote concernes par une depeche sans emetteur identifie."""
    bas = (texte or "").lower()
    return [sect for motif, sect in SUJETS_SECTORIELS if re.search(motif, bas)]


# ──────────────────────────────────────────────────────────────────────────
# Classement thematique
# ──────────────────────────────────────────────────────────────────────────

# L'ordre compte : une depeche de seance citant un dividende reste une depeche
# de marche. On classe sur le TITRE, qui est explicite chez sikafinance ; le
# corps ferait deraper le classement au premier mot croise.
THEMES = [
    ("marche", r"\bbrvm\b|indice|capitalisation boursi[èe]re|s[ée]ance"
               r"|hausse journali[èe]re|baisse journali[èe]re|palmar[èe]s"),
    ("operation", r"augmentation de capital|introduction en bourse|\bopa\b|\bops\b"
                  r"|scission|fusion|rachat d.actions|attribution gratuite"
                  r"|emprunt obligataire|notation financi[èe]re|admission [àa] la cote"),
    ("dividende", r"dividende|coupon"),
    ("resultats", r"r[ée]sultat|b[ée]n[ée]fice|chiffre d.affaires|produit net bancaire"
                  r"|perte nette|exercice \d{4}|semestre|trimestre"),
]


def detect_theme(titre: str, sur_societe_cotee: bool) -> str:
    """Theme de la depeche.

    `sur_societe_cotee` evite l'ecueil principal : "Le marche de l'assurance
    affiche 436 milliards de chiffre d'affaires" n'est pas une publication de
    resultats, c'est de l'actualite sectorielle. Sans emetteur cote en sujet,
    une depeche reste du contexte.
    """
    bas = (titre or "").lower()
    for nom, motif in THEMES:
        if re.search(motif, bas):
            if nom in ("resultats", "dividende", "operation") and not sur_societe_cotee:
                return "secteur"
            return nom
    return "secteur"


# ──────────────────────────────────────────────────────────────────────────
# Scraping
# ──────────────────────────────────────────────────────────────────────────

def scan_listing(rubrique: str, url: str, vus: set) -> list:
    """Depeches d'une rubrique : titre, chapeau et date sont deja sur la liste.

    `vus` est partage entre rubriques : les memes depeches sont relayees d'une
    rubrique a l'autre, on ne les traite qu'une fois.
    """
    try:
        html = _get(url)
    except Exception as e:
        log(f"[{rubrique}] liste inaccessible : {e}")
        return []

    soup = BeautifulSoup(html, "lxml")
    items = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not ARTICLE_RE.match(href):
            continue
        titre = a.get_text(" ", strip=True)
        if len(titre) < 25:
            continue
        lien = BASE + href
        if lien in vus:
            continue
        vus.add(lien)

        bloc = a.find_parent(["div", "li", "tr", "article"])
        contexte = bloc.get_text(" ", strip=True) if bloc else titre

        chapeau = contexte.replace(titre, "", 1).strip()
        chapeau = re.sub(r"\s+", " ", chapeau)[:400]

        d = DATE_RE.search(contexte)
        date_pub = f"{d.group(3)}-{d.group(2)}-{d.group(1)}" if d else None

        items.append({
            "source": f"sikafinance/{rubrique}",
            "url": lien,
            "title": titre,
            "lead": chapeau,
            "published_at": date_pub,
        })
        if len(items) >= LIMITE_PAR_RUBRIQUE:
            break

    log(f"[{rubrique}] {len(items)} depeche(s)")
    return items


def scan_rss(nom: str, url: str, vus: set) -> list:
    """Depeches d'un flux RSS : titre, lien, date et chapeau y sont deja.

    On ne va pas chercher le corps de l'article sur ces sources : le chapeau
    d'un flux fait deja deux a quatre phrases, ce qui suffit a ponderer et a
    citer. Aller chercher trente articles complets a chaque passage, sur des
    sites qu'on ne maitrise pas, se paierait en blocages.
    """
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    try:
        brut = _get(url).encode("utf-8", "ignore")
        racine = ET.fromstring(brut)
    except Exception as e:
        log(f"[{nom}] flux inaccessible : {type(e).__name__}: {e}")
        return []

    articles = racine.findall(".//item") or racine.findall(
        "{http://www.w3.org/2005/Atom}entry")
    if not articles:
        log(f"[{nom}] flux vide (0 article) — source a verifier")
        return []

    items = []
    for a in articles:
        titre = (a.findtext("title") or "").strip()
        lien = (a.findtext("link") or "").strip()
        if not titre or not lien or lien in vus:
            continue
        vus.add(lien)

        # La date se lit dans trois formats selon la source. Jeune Afrique
        # met de l'ISO 8601 dans `pubDate`, la ou la norme RSS demande du
        # RFC 822 ; sans les deux essais, trente depeches arrivent sans date
        # et sortent du plafond journalier faute de jour ou les ranger.
        date_pub = None
        DC = "{http://purl.org/dc/elements/1.1/}date"
        ATOM = "{http://www.w3.org/2005/Atom}published"
        brut_date = (a.findtext("pubDate") or a.findtext(DC)
                     or a.findtext(ATOM) or a.findtext("date") or "").strip()
        if brut_date:
            for lecture in (
                lambda v: parsedate_to_datetime(v),
                lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")),
            ):
                try:
                    date_pub = lecture(brut_date).strftime("%Y-%m-%d")
                    break
                except Exception:
                    continue
            if date_pub is None:
                log(f"[{nom}] date illisible : {brut_date[:32]!r}")

        chapeau = (a.findtext("description") or "").strip()
        if chapeau:
            # Les chapeaux RSS arrivent souvent en HTML echappe.
            chapeau = BeautifulSoup(chapeau, "lxml").get_text(" ", strip=True)
            chapeau = re.sub(r"\s+", " ", chapeau)[:600]

        items.append({
            "source": nom,
            "url": lien,
            "title": titre,
            "lead": chapeau,
            "published_at": date_pub,
        })
        if len(items) >= LIMITE_PAR_FLUX:
            break

    log(f"[{nom}] {len(items)} depeche(s)")
    return items


def fetch_body(url: str) -> str:
    """Corps de l'article. Sikafinance le rend dans #containerPage."""
    try:
        html = _get(url)
    except Exception as e:
        log(f"  corps inaccessible ({e})")
        return ""

    soup = BeautifulSoup(html, "lxml")
    for t in soup(["script", "style", "nav", "footer", "header", "aside"]):
        t.decompose()

    conteneur = soup.find(id="containerPage")
    if conteneur is None:
        # repli : le bloc qui contient le plus de texte en paragraphes
        meilleur, taille = None, 0
        for d in soup.find_all(["div", "article", "section"]):
            txt = " ".join(p.get_text(" ", strip=True) for p in d.find_all("p"))
            if len(txt) > taille:
                meilleur, taille = d, len(txt)
        conteneur = meilleur

    if conteneur is None:
        return ""

    corps = " ".join(p.get_text(" ", strip=True) for p in conteneur.find_all("p"))
    return re.sub(r"\s+", " ", corps).strip()


# ──────────────────────────────────────────────────────────────────────────
# Enregistrement
# ──────────────────────────────────────────────────────────────────────────

def save_news(items: list) -> int:
    """Insere les depeches inconnues. L'URL sert de cle d'unicite."""
    if not items:
        return 0
    conn = get_connection()
    nouvelles = 0
    try:
        for it in items:
            cur = conn.execute("SELECT 1 FROM news_articles WHERE url = ?",
                               (it["url"],))
            if cur.fetchone():
                continue
            conn.execute(
                """INSERT INTO news_articles
                   (source, url, title, published_at, lead, body,
                    tickers, tickers_cites, secteurs, theme)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (it["source"], it["url"], it["title"], it["published_at"],
                 it["lead"], it["body"], ",".join(it["tickers"]),
                 ",".join(it["tickers_cites"]),
                 ",".join(it.get("secteurs") or []), it["theme"]),
            )
            nouvelles += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"echec enregistrement : {type(e).__name__}: {e}")
    finally:
        conn.close()
    return nouvelles


def main(dry_run: bool = False):
    debut = time.time()
    log(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] scan_news — start")

    items = []
    vus = set()
    for rubrique, url in SOURCES:
        items.extend(scan_listing(rubrique, url, vus))
    for nom, url in FLUX_RSS:
        items.extend(scan_rss(nom, url, vus))

    for it in items:
        # Le corps ne se recupere que chez sikafinance, dont on connait la
        # structure. Ailleurs, le chapeau du flux tient lieu de texte.
        if not it["source"].startswith("sikafinance/"):
            it["body"] = ""
            entete = f"{it['title']} {it['lead']}"
            sujets = detect_tickers(entete)
            it["tickers"] = sujets
            it["tickers_cites"] = []
            it["secteurs"] = detect_secteurs(entete) if not sujets else []
            it["theme"] = detect_theme(it["title"], bool(sujets))
            continue

        # Le chapeau de la liste suffit a cerner une depeche macro. On ne va
        # chercher l'article complet que s'il touche la cote — sikafinance
        # coupe la connexion quand on enchaine trop de requetes.
        entete = f"{it['title']} {it['lead']}"
        interesse = bool(detect_tickers(entete)) or it["source"].endswith("/brvm")
        it["body"] = fetch_body(it["url"]) if interesse else ""
        # Une societe sujet de la depeche (titre ou chapeau) n'a pas le meme
        # poids qu'une societe citee au detour d'une phrase : la mine de Doropo
        # cite NSIA Banque parmi ses preteurs sans etre une nouvelle NSIA.
        sujets = detect_tickers(entete)
        cites = [t for t in detect_tickers(it["body"]) if t not in sujets]
        it["tickers"] = sujets
        it["tickers_cites"] = cites
        it["secteurs"] = detect_secteurs(f"{entete} {it['body'][:800]}") if not sujets else []
        it["theme"] = detect_theme(it["title"], bool(sujets))
        time.sleep(1.5 if interesse else 0.2)   # on ne martele pas le site

    if dry_run:
        for it in items:
            log("")
            sujets = ",".join(it["tickers"]) or "—"
            cites = ",".join(it["tickers_cites"])
            secteurs = ",".join(it.get("secteurs") or [])
            log(f"{it['published_at'] or '?':10} [{it['theme']:9}] {sujets}"
                + (f"   (cite : {cites})" if cites else "")
                + (f"   [secteurs : {secteurs}]" if secteurs else ""))
            log(f"  {it['title']}")
            log(f"  {(it['body'] or it['lead'])[:220]}…")
        log("")
        log(f"{len(items)} depeche(s) — mode simulation, rien enregistre")
    else:
        n = save_news(items)
        log(f"{n} nouvelle(s) depeche(s) sur {len(items)} vue(s)")

    log(f"[done] scan_news en {time.time() - debut:.0f}s")
    return items


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
