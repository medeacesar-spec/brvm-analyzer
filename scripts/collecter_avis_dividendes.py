#!/usr/bin/env python3
"""Collecte les avis de paiement de dividendes publies par la BRVM.

La page /fr/esv/paiement-de-dividendes tabule ce que les avis PDF disent en
prose : emetteur, exercice comptable, date de paiement, date ex-dividende et
montant par action. Quarante-quatre pages, quatre cent quatorze avis, quarante
-trois societes, des exercices 2015 a 2025. Aucun PDF n'a besoin d'etre ouvert.

Trois pieges, tous verifies sur les donnees :

1. **La colonne dit « net », elle porte le brut.** L'avis SOLIBRA de l'exercice
   2025 annonce 2 127 FCFA ; la societe declare dans ses comptes « Dividendes
   Bruts 35 012 206 680 » pour 16 460 800 actions, soit 2 127,01 par action.
   Les deux tombent au franc pres. Le libelle du site est fautif.

2. **Ecobank Transnational paie en dollars.** Le site melange les unites d'une
   ligne a l'autre — « 0,16 cents de dollars » ici, « 0,9 FCFA » la pour le
   meme exercice. Les lignes en devise etrangere sont ecartees, jamais
   converties a l'aveugle.

3. **Les colonnes se reperent par leur en-tete, jamais par balayage.** Prendre
   « le premier montant de la ligne » attribuait a ETI un montant en francs lu
   dans une colonne voisine.

Usage :
    python3 scripts/collecter_avis_dividendes.py [--simuler] [--pages N]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
import unicodedata
import urllib.request

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

ENTETES = {"User-Agent": "Mozilla/5.0"}
BASE = "https://www.brvm.org/fr/esv/paiement-de-dividendes"
MOIS = {m: i for i, m in enumerate(
    "janvier fevrier mars avril mai juin juillet aout septembre octobre "
    "novembre decembre".split(), 1)}
DEVISES = {"FCFA": "XOF", "FCF": "XOF", "FCA": "XOF", "CFA": "XOF", "F": "XOF",
           "XOF": "XOF", "USD": "USD", "$": "USD", "CENTS": "USD",
           "EUR": "EUR", "€": "EUR"}

# Le nom que la BRVM donne a l'emetteur ne se devine pas depuis le ticker :
# « BOLLORE TRANSPORT & LOGISTICS » est Africa Global Logistics (SDSC),
# « AIR LIQUIDE CI » est devenu Erium (SIVC), « VIVO ENERGY CI » etait Shell
# (SHEC). La table est donc explicite, et verifiee un a un contre
# data/brvm_tickers.json.
EMETTEURS = {
    "AIR LIQUIDE CI": "SIVC.ci",
    "BANK OF AFRICA BF": "BOABF.bf",
    "BANK OF AFRICA BN": "BOAB.bj",
    "BANK OF AFRICA CI": "BOAC.ci",
    "BANK OF AFRICA ML": "BOAM.ml",
    "BANK OF AFRICA NG": "BOAN.ne",
    "BANK OF AFRICA SN": "BOAS.sn",
    "BERNABE CI": "BNBC.ci",
    "BICI CI": "BICC.ci",
    "BIIC": "BICB.bj",
    "BOLLORE TRANSPORT & LOGISTICS": "SDSC.ci",
    "CFAO MOTORS CI": "CFAC.ci",
    "CIE CI": "CIEC.ci",
    "CORIS BANK INTERNATIONAL": "CBIBF.bf",
    "CROWN SIEM CI": "SEMC.ci",
    "ECOBANK CI": "ECOC.ci",
    "ECOBANK TG": "ETIT.tg",
    "FILTISAC CI": "FTSC.ci",
    "LNB": "LNBB.bj",
    "NEI-CEDA CI": "NEIC.ci",
    "NESTLE CI": "NTLC.ci",
    "NSBC": "NSBC.ci",
    "ONATEL BF": "ONTBF.bf",
    "ORAGROUP": "ORGT.tg",
    "ORANGE CI": "ORAC.ci",
    "PALM CI": "PALC.ci",
    "SAFCA": "SAFC.ci",
    "SAPH CI": "SPHC.ci",
    "SDSC": "SDSC.ci",
    "SERVAIR ABIDJAN CI": "ABJC.ci",
    "SETAO CI": "STAC.ci",
    "SGCI": "SGBC.ci",
    "SIB": "SIBC.ci",
    "SICABLE": "CABC.ci",
    "SICOR": "SICC.ci",
    "SITAB": "STBC.ci",
    "SMB": "SMBC.ci",
    "SODE CI": "SDCC.ci",
    "SODECI": "SDCC.ci",
    "SOGB": "SOGC.ci",
    "SOLIBRA": "SLBC.ci",
    "SONATEL": "SNTS.sn",
    "SUCRIVOIRE": "SCRC.ci",
    "TOTAL": "TTLC.ci",
    "TOTAL SENEGAL S.A.": "TTLS.sn",
    "TRACTAFRIC CI": "PRSC.ci",
    "UNILEVER CI": "UNLC.ci",
    "UNIWAX CI": "UNXC.ci",
    "VIVO ENERGY CI": "SHEC.ci",
}


def _pli(texte: str) -> str:
    """Majuscules sans accents : la BRVM n'accentue pas toujours ses emetteurs."""
    texte = unicodedata.normalize("NFD", texte or "")
    return "".join(c for c in texte
                   if unicodedata.category(c) != "Mn").upper().strip()


def _date(texte: str):
    m = re.match(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", _pli(texte).lower())
    if not m or m.group(2) not in MOIS:
        return None
    return f"{m.group(3)}-{MOIS[m.group(2)]:02d}-{int(m.group(1)):02d}"


def _montant(texte: str):
    """Rend (valeur, devise). La devise ne se suppose jamais."""
    if not texte:
        return None, None
    m = re.search(r"(-?[\d   ]+(?:[.,]\d+)?)\s*([A-Za-z€$]+)?", texte.strip())
    if not m:
        return None, None
    try:
        valeur = float(re.sub(r"[   ]", "", m.group(1)).replace(",", "."))
    except ValueError:
        return None, None
    suffixe = (m.group(2) or "").upper().strip()
    return valeur, DEVISES.get(suffixe, suffixe or None)


def moissonner(pages: int = 44, pause: float = 0.6) -> list:
    avis = []
    for page in range(pages):
        url = BASE if page == 0 else f"{BASE}?page={page}"
        try:
            html = urllib.request.urlopen(
                urllib.request.Request(url, headers=ENTETES),
                timeout=45).read().decode("utf-8", "replace")
        except Exception as err:                       # noqa: BLE001
            print(f"  page {page} ECHEC {err}", file=sys.stderr)
            continue
        soupe = BeautifulSoup(html, "html.parser")
        lien = soupe.find("a", href=re.compile(r"dividende.*\.pdf", re.I))
        if not lien:
            continue
        table = lien.find_parent("table")
        titres = [_pli(c.get_text(" ", strip=True))
                  for c in table.find_all("tr")[0].find_all(["th", "td"])]

        def colonne(motif, defaut):
            return next((i for i, t in enumerate(titres) if motif in t), defaut)

        i_em, i_ex = colonne("EMETTEUR", 0), colonne("EXERCICE", 3)
        i_pa, i_mt = colonne("DATE DE PAIEMENT", 4), colonne("MONTANT", 6)
        for ligne in table.find_all("tr")[1:]:
            cel = [c.get_text(" ", strip=True) for c in ligne.find_all("td")]
            pdf = ligne.find("a", href=re.compile(r"\.pdf", re.I))
            if len(cel) <= i_mt or not pdf:
                continue
            valeur, devise = _montant(cel[i_mt])
            avis.append({
                "ticker": EMETTEURS.get(_pli(cel[i_em])),
                "emetteur": cel[i_em],
                "exercice": int(cel[i_ex])
                            if re.fullmatch(r"(19|20)\d{2}", cel[i_ex]) else None,
                "paiement": _date(cel[i_pa]),
                "montant": valeur,
                "devise": devise,
                "avis": pdf["href"] if pdf["href"].startswith("http")
                        else "https://www.brvm.org" + pdf["href"],
            })
        time.sleep(pause)
    return avis


def retenir(avis: list) -> dict:
    """Un seul avis par (titre, exercice) : le plus recemment paye.

    Les devises etrangeres sont ecartees : Ecobank Transnational paie en
    dollars et le site melange dollars, cents et francs pour un meme exercice.
    """
    garde = {}
    for a in avis:
        if not (a["ticker"] and a["exercice"]) or a["montant"] is None:
            continue
        if a["devise"] not in ("XOF", None):
            continue
        # Ecobank Transnational paie en dollars, et le site etiquette parfois
        # ces montants « FCFA » : pour le seul exercice 2025 il publie
        # « 0,16 cents de dollars » et « 0,9 FCFA ». Aucune ligne de cet
        # emetteur n'est donc exploitable telle quelle.
        if a["ticker"] == "ETIT.tg":
            continue
        cle = (a["ticker"], a["exercice"])
        if cle not in garde or (a["paiement"] or "") > (garde[cle]["paiement"] or ""):
            garde[cle] = a
    return garde


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--simuler", action="store_true")
    ap.add_argument("--pages", type=int, default=44)
    args = ap.parse_args()

    avis = moissonner(args.pages)
    garde = retenir(avis)
    print(f"{len(avis)} avis moissonnes · {len(garde)} couples (titre, exercice)")

    cnx = get_connection()
    base = {(dict(r)["ticker"], dict(r)["fiscal_year"]): dict(r)["dps"]
            for r in cnx.execute(
                "SELECT ticker, fiscal_year, dps FROM fundamentals").fetchall()}

    ecrits = ecarts = identiques = 0
    a_instruire = []
    for (ticker, exercice), a in sorted(garde.items()):
        ancien, montant = base.get((ticker, exercice)), a["montant"]
        # Tolerance RELATIVE : la BRVM arrondit parfois au franc ce que le
        # rapport annuel donne au centime — 206,00 contre 206,19. Comparer en
        # valeur absolue classait ces arrondis comme des ecarts a instruire.
        if ancien and abs(ancien / montant - 1) < 0.005:
            identiques += 1
            continue
        # Un ecart de 10 ou 12 % est la retenue a la source : la base portait
        # le net, l'avis porte le brut. Tout autre ecart n'est pas une retenue
        # — division d'actions, acompte paye seul, erreur d'echelle — et ne se
        # corrige pas ici.
        if ancien:
            rapport = ancien / montant
            retenue = ("12 %" if abs(rapport - 0.88) < 0.005 else
                       "10 %" if abs(rapport - 0.90) < 0.005 else None)
            if retenue is None:
                a_instruire.append((ticker, exercice, ancien, montant, rapport))
                continue
            ecarts += 1
        note = f"brut, avis BRVM du {a['paiement']} · {a['avis']}"
        if not args.simuler:
            if (ticker, exercice) in base:
                cnx.execute(
                    "UPDATE fundamentals SET dps=%s, dps_note=%s, updated_at=NOW() "
                    "WHERE ticker=%s AND fiscal_year=%s",
                    (montant, note, ticker, exercice))
            else:
                cnx.execute(
                    "INSERT INTO fundamentals (ticker, fiscal_year, dps, dps_note) "
                    "VALUES (%s,%s,%s,%s)", (ticker, exercice, montant, note))
        ecrits += 1
    if not args.simuler:
        cnx.commit()
    cnx.close()

    print(f"  identiques a la base : {identiques}")
    print(f"  ecrits               : {ecrits} (dont {ecarts} retenues a la source levees)")
    print(f"  a instruire          : {len(a_instruire)}")
    for t, an, ancien, montant, rapport in a_instruire:
        print(f"     {t:9s} {an}  base {ancien:9.2f} · avis {montant:9.2f} "
              f"· base/avis {rapport:6.4f}")


if __name__ == "__main__":
    main()
