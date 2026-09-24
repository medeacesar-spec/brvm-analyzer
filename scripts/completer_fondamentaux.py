#!/usr/bin/env python3
"""Complete TOUS les champs fondamentaux manquants, en une lecture par document.

L'ETAT DES LIEUX QUI JUSTIFIE CE SCRIPT (24/09/2026, exercices 2021-2025)

  chiffre d'affaires  98 %      resultat d'exploitation  64 %
  resultat net        97 %      capitaux propres         62 %
  dividende/action    89 %      total de bilan           56 %
  nombre d'actions    89 %      dette totale             40 %
  EBITDA              34 %      charges d'interets       14 %
  cout du risque      22 %      investissements          14 %
  depots              19 %      flux de tresorerie       13 %
                                dividendes verses         6 %

Le compte de resultat est presque complet ; le bilan l'est a moitie, et les
flux de tresorerie n'existent pratiquement pas. Or ce sont eux qui portent le
flux de tresorerie disponible, la couverture des interets, le levier — la
moitie de l'analyse fondamentale.

UNE SEULE LECTURE PAR DOCUMENT

Le meme etat financier porte le bilan, le compte de resultat ET le tableau
des flux. Le lire une fois pour n'en tirer que les capitaux propres, puis le
relire pour la dette, serait payer deux fois une extraction qui coute des
minutes quand le document est un scan. Chaque document est donc telecharge
une fois, extrait une fois, et TOUS ses champs manquants sont remplis.

CE QUI N'EST JAMAIS ECRASE

Une valeur deja presente reste en place. Beaucoup ont ete verifiees a la
main contre le document d'origine ; une extraction automatique n'a pas
autorite pour les remplacer.

LES CONTROLES, CHAMP PAR CHAMP

Un chiffre tire d'un PDF ne vaut rien tant qu'il n'a pas ete recoupe. Chaque
valeur est confrontee a l'echelle du titre — son chiffre d'affaires, ses
capitaux propres, l'exercice connu le plus proche — et toute valeur qui sort
des bornes est SIGNALEE, jamais ecrite. Les bornes sont larges a dessein :
elles attrapent les erreurs d'echelle et de colonne, qui sont les deux
pieges reels de ces documents, sans pretendre juger la gestion d'une
societe.

Usage :
  python3 scripts/completer_fondamentaux.py                  # analyse seule
  python3 scripts/completer_fondamentaux.py --limite 12
  python3 scripts/completer_fondamentaux.py --ecrire
  python3 scripts/completer_fondamentaux.py --titre PALC.ci --ecrire
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from data.db import get_connection, read_sql_df  # noqa: E402
from data.pdf_extractor import extract_from_pdf  # noqa: E402

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOSSIER = os.path.join(RACINE, "data", "pdf_fondamentaux")
CACHE = os.path.join(DOSSIER, "_extractions.json")
ENTETES = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
DEBUT, FIN = 2021, 2025

CHAMPS = ("revenue", "net_income", "equity", "total_assets", "total_debt", "ebit",
          "interest_expense", "cfo", "capex", "dividends_total", "cost_of_risk",
          "deposits", "ebitda", "shares")

# Borne superieure de chaque champ, en multiple d'une reference du titre.
# « ca » = chiffre d'affaires de l'exercice, « actif » = total de bilan.
BORNES = {
    "ebit": ("ca", 1.5), "interest_expense": ("ca", 0.5), "cfo": ("ca", 2.0),
    "capex": ("ca", 1.5), "ebitda": ("ca", 2.0), "cost_of_risk": ("ca", 1.0),
    "total_debt": ("actif", 1.0), "deposits": ("actif", 1.0),
}
NEGATIFS_ADMIS = {"net_income", "ebit", "cfo", "capex", "ebitda", "equity"}
FACTEUR_VOISIN = 4       # ecart maximal avec l'exercice connu le plus proche


def _charger_cache() -> dict:
    if os.path.exists(CACHE):
        try:
            return json.load(open(CACHE, encoding="utf-8"))
        except Exception:                                        # noqa: BLE001
            return {}
    return {}


def _ecrire_cache(cache: dict) -> None:
    os.makedirs(DOSSIER, exist_ok=True)
    json.dump(cache, open(CACHE, "w", encoding="utf-8"), indent=1)


def etat_de_la_base() -> tuple:
    f = read_sql_df(
        f"SELECT ticker, fiscal_year, sector, {', '.join(CHAMPS)} FROM fundamentals "
        "WHERE fiscal_year BETWEEN ? AND ?", params=(DEBUT, FIN))
    docs = read_sql_df(
        "SELECT ticker, fiscal_year, url FROM report_links "
        "WHERE report_type IN ('etats_financiers', 'rapport_annuel') "
        "AND fiscal_year BETWEEN ? AND ?", params=(DEBUT, FIN))
    par_doc = {}
    for _, r in docs.iterrows():
        par_doc.setdefault((r["ticker"], int(r["fiscal_year"])), []).append(r["url"])
    return f, par_doc


def _mots_du_titre(ticker: str) -> set:
    """Les mots qui doivent apparaitre dans le nom d'un document de ce titre."""
    from config import load_tickers
    nom = next((t.get("name", "") for t in load_tickers()
                if t["ticker"] == ticker), "")
    mots = {m for m in re.split(r"[^a-z0-9]+", nom.lower()) if len(m) > 3}
    mots.add(ticker.split(".")[0].lower())
    return mots


def choisir_document(ticker: str, urls: list) -> str:
    """Le document le plus plausible pour ce titre.

    Le plus recent n'est pas toujours le bon : l'exercice 2024 de BIIC Benin
    referencait DEUX documents, le sien et celui de la BIDC — un autre
    etablissement — et le tri par date retenait le second. On prefere donc un
    nom de fichier qui rappelle la societe ; a defaut, le plus recent.
    """
    mots = _mots_du_titre(ticker)
    nommes = [u for u in urls
              if any(m in u.rsplit("/", 1)[-1].lower() for m in mots)]
    return sorted(nommes or urls)[-1]


def travail(f, par_doc, titre=None) -> list:
    """[(ticker, exercice, url, [champs manquants])]"""
    sortie = []
    for _, r in f.iterrows():
        if titre and r["ticker"] != titre:
            continue
        trous = [c for c in CHAMPS if r[c] != r[c]]
        urls = par_doc.get((r["ticker"], int(r["fiscal_year"])))
        if trous and urls:
            sortie.append((r["ticker"], int(r["fiscal_year"]),
                           choisir_document(r["ticker"], urls), trous))
    return sorted(sortie)


def reperes(f, ticker: str) -> dict:
    """Ce que la base sait deja du titre : de quoi juger un chiffre extrait."""
    lot = f[f["ticker"] == ticker]
    connus = {c: {int(r["fiscal_year"]): r[c] for _, r in lot.iterrows()
                  if r[c] == r[c] and r[c]} for c in CHAMPS}
    secteurs = [r["sector"] for _, r in lot.iterrows() if r["sector"]]
    return {"connus": connus,
            "banque": bool(secteurs and "banque" in str(secteurs[0]).lower())}


def controler(champ: str, valeur, exercice: int, ligne, repere: dict) -> str:
    """Le motif de rejet, ou None si la valeur peut etre ecrite."""
    if valeur is None or valeur != valeur or valeur == 0:
        return "absent du document"
    if valeur < 0 and champ not in NEGATIFS_ADMIS:
        return f"negatif ({valeur/1e9:.2f} Md)"

    ca = ligne.get("revenue") if ligne.get("revenue") == ligne.get("revenue") else None
    actif = ligne.get("total_assets") if ligne.get("total_assets") == ligne.get("total_assets") else None
    if champ in BORNES:
        base, facteur = BORNES[champ]
        reference = ca if base == "ca" else actif
        if reference and abs(valeur) > facteur * abs(reference):
            quoi = "chiffre d'affaires" if base == "ca" else "total de bilan"
            return (f"{abs(valeur)/1e9:.1f} Md pour {abs(reference)/1e9:.1f} Md de "
                    f"{quoi} — {abs(valeur/reference):.1f}x")

    if champ == "equity" and actif and abs(valeur) > abs(actif):
        return f"capitaux propres {valeur/1e9:.1f} Md au-dessus du bilan {actif/1e9:.1f} Md"
    if champ == "dividends_total":
        resultat = ligne.get("net_income")
        if resultat == resultat and resultat and abs(valeur) > 3 * abs(resultat):
            return f"{valeur/1e9:.1f} Md verses pour {resultat/1e9:.1f} Md de resultat"

    voisins = repere["connus"].get(champ) or {}
    if voisins:
        proche = min(voisins, key=lambda a: abs(a - exercice))
        ref = voisins[proche]
        if ref and abs(valeur) > FACTEUR_VOISIN * abs(ref):
            return (f"{valeur/1e9:.2f} Md contre {ref/1e9:.2f} Md en {proche} "
                    f"— facteur {abs(valeur/ref):.0f}")
        if ref and abs(valeur) * FACTEUR_VOISIN < abs(ref):
            return (f"{valeur/1e9:.2f} Md contre {ref/1e9:.2f} Md en {proche} "
                    f"— divise par {abs(ref/valeur):.0f}")
    return None


def extraire(url: str, ticker: str, exercice: int, session, cache: dict) -> dict:
    cle = f"{ticker}|{exercice}|{url[-60:]}"
    if cle in cache:
        return cache[cle]
    os.makedirs(DOSSIER, exist_ok=True)
    chemin = os.path.join(DOSSIER, f"{ticker}_{exercice}_{url.rsplit('/', 1)[-1][:55]}")
    if not os.path.exists(chemin):
        r = session.get(url, timeout=120, verify=False)
        r.raise_for_status()
        open(chemin, "wb").write(r.content)
    donnees = extract_from_pdf(chemin)
    cache[cle] = {c: donnees.get(c) for c in CHAMPS}
    _ecrire_cache(cache)
    return cache[cle]


def main(ecrire: bool, titre: str, limite: int) -> None:
    f, par_doc = etat_de_la_base()
    cibles = travail(f, par_doc, titre)
    if limite:
        cibles = cibles[:limite]
    trous = sum(len(t[3]) for t in cibles)
    print(f"{len(cibles)} exercice(s) a lire · {trous} champ(s) manquant(s)\n")

    session = requests.Session()
    session.headers.update(ENTETES)
    cache = _charger_cache()
    cnx = get_connection() if ecrire else None
    retenus = rejetes = lus = 0
    motifs = {}

    for ticker, exercice, url, manquants in cibles:
        ligne = f[(f["ticker"] == ticker) & (f["fiscal_year"] == exercice)].iloc[0]
        try:
            donnees = extraire(url, ticker, exercice, session, cache)
            lus += 1
        except Exception as err:                                  # noqa: BLE001
            print(f"  {ticker:9} {exercice}  lecture KO : {type(err).__name__}: {err}")
            continue
        repere = reperes(f, ticker)
        garde, ecartes = {}, []
        for champ in manquants:
            motif = controler(champ, donnees.get(champ), exercice, ligne, repere)
            if motif:
                if motif != "absent du document":
                    ecartes.append(f"{champ} {motif}")
                    motifs[champ] = motifs.get(champ, 0) + 1
                continue
            garde[champ] = donnees[champ]
        retenus += len(garde)
        rejetes += len(ecartes)
        resume = ", ".join(f"{c} {v/1e9:.2f}" for c, v in list(garde.items())[:6])
        print(f"  {ticker:9} {exercice}  {len(garde):2d}/{len(manquants):2d} retenus"
              + (f" · {resume}" if garde else "")
              + (f"\n              SIGNALE : " + " · ".join(ecartes) if ecartes else ""))
        if ecrire and garde:
            cnx.execute(
                "UPDATE fundamentals SET " + ", ".join(f"{c} = ?" for c in garde)
                + " WHERE ticker = ? AND fiscal_year = ?",
                (*garde.values(), ticker, exercice))
    if cnx:
        cnx.commit()
        cnx.close()
    print(f"\n{lus} document(s) lus · {retenus} champ(s) retenus · {rejetes} signale(s)")
    if motifs:
        print("  signalements par champ :", ", ".join(f"{c} {n}" for c, n in sorted(motifs.items())))
    if not ecrire:
        print("  Rien n'a ete ecrit : relancer avec --ecrire.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    ap.add_argument("--titre")
    ap.add_argument("--limite", type=int, default=0)
    a = ap.parse_args()
    main(a.ecrire, a.titre, a.limite)
