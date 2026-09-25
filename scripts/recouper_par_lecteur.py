#!/usr/bin/env python3
"""Passe le lecteur d'etats financiers sur les documents annuels deja
telecharges, et confronte ce qu'il lit a la base. N'ECRIT RIEN.

POURQUOI

Le lecteur (`analysis/lecture_syscohada.py`) dit desormais COMMENT il a
choisi chaque colonne. Au 24/09/2026, les valeurs lues « par l'ancre »
(l'exercice precedent connu retombe dans la ligne) ou « par brut-net »
(brut - amortissements = net) n'ont jamais ete fausses ; celles lues « par
l'en-tete » ont rendu les deux seules erreurs connues. On peut donc passer
le lecteur sur tout le fonds documentaire et ne retenir que le sur.

TROIS ISSUES POUR CHAQUE VALEUR SURE

  concorde  la base porte la meme valeur, a un pour mille pres ;
  ecart     la base porte autre chose — a LIRE a la main. C'est ainsi que
            les totaux de bilan de Vivo et d'Erium ont ete trouves faux ;
  trou      la base est vide pour ce champ : une valeur a PROPOSER, apres
            lecture, jamais a ecrire d'office.

Les valeurs lues par l'en-tete sont comptees a part et jamais proposees.

Usage :
  python3 scripts/recouper_par_lecteur.py --sans-scans
  python3 scripts/recouper_par_lecteur.py                # avec OCR, long
  python3 scripts/recouper_par_lecteur.py --titre SNTS.sn
  python3 scripts/recouper_par_lecteur.py --sans-scans --fiches
"""
from __future__ import annotations

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pdfplumber  # noqa: E402

from analysis.lecture_syscohada import lire_detaille  # noqa: E402
from data.db import read_sql_df  # noqa: E402

DOSSIER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "pdf_fondamentaux")
CACHE = os.path.join(DOSSIER, ".ocr_rangees")
CHAMPS = ("revenue", "net_income", "equity", "total_assets", "total_debt",
          "ebit", "ebitda", "interest_expense", "cfo", "capex",
          "dividends_total", "deposits", "cost_of_risk")
# L'ancre ne porte que sur les champs dont l'exercice precedent est bien
# couvert et dont la definition ne varie pas d'un document a l'autre.
ANCRES = ("revenue", "net_income", "equity", "total_assets")
SURS = ("ancre", "brut-net")
FICHIER = re.compile(r"^([A-Z0-9]+\.[a-z]{2})_(20\d\d)_")


def _illisible(texte):
    visibles = [c for c in texte if not c.isspace()]
    return len(visibles) > 200 and sum(c.isalnum() for c in visibles) < 0.5 * len(visibles)


def _ocr(chemin, rangs):
    import fitz
    from PIL import Image
    from data.pdf_extractor import _ocr_rangees
    sortie = {}
    with fitz.open(chemin) as doc:
        for i in rangs:
            pix = doc[i].get_pixmap(dpi=300)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            sortie[i] = "\n".join(_ocr_rangees(img))
    return sortie


def texte(chemin, avec_ocr):
    """(texte, mode) — le texte natif, les pages illisibles et les scans
    passes par l'OCR en rangees si `avec_ocr`, avec cache disque."""
    with pdfplumber.open(chemin) as pdf:
        pages = [(p.extract_text() or "") for p in pdf.pages]
    a_lire = [i for i, p in enumerate(pages) if _illisible(p)]
    scan = len("".join(pages)) <= 800
    if scan:
        a_lire = list(range(min(len(pages), 15)))
    if not a_lire:
        return "\n".join(pages), "texte"
    if not avec_ocr:
        return ("\n".join(pages), "texte") if not scan else (None, "scan ignore")
    os.makedirs(CACHE, exist_ok=True)
    cache = os.path.join(CACHE, os.path.basename(chemin) + ".txt")
    if not os.path.exists(cache):
        lues = _ocr(chemin, a_lire)
        open(cache, "w").write("\n\f\n".join(lues[i] for i in a_lire))
    for i, lu in zip(a_lire, open(cache).read().split("\n\f\n")):
        pages[i] = lu
    return "\n".join(pages), ("ocr" if scan else "texte+ocr")


def _fiches(titres) -> dict:
    """{ticker: {exercice: {revenue, net_income}}} — les fiches societe,
    seconde source independante (19 concordances sur 19 avec les documents
    lus a la main, voir `recouper_fiches_societe.py`)."""
    import importlib.util
    import time
    import requests
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrape_societe.py")
    spec = importlib.util.spec_from_file_location("scrape_societe", chemin)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0"
    sortie = {}
    for t in sorted(titres):
        try:
            sortie[t] = {int(a): v for a, v in
                         (module.scrape_societe(session, t).get("financials") or {}).items()}
        except Exception as err:                                # noqa: BLE001
            print(f"  fiche {t} indisponible : {err}")
        time.sleep(1)
    return sortie


def main(avec_ocr: bool, titre: str = None, avec_fiches: bool = False) -> None:
    base = read_sql_df("SELECT ticker, fiscal_year, " + ", ".join(CHAMPS)
                       + " FROM fundamentals")
    connu = {(r.ticker, int(r.fiscal_year)): r for r in base.itertuples()}
    # LE SYSCOHADA AVANT L'IFRS. La base et les fiches societe suivent les
    # comptes SYSCOHADA ; quand un exercice a les deux documents,
    # TotalEnergies Senegal par exemple, l'ordre alphabetique mettait
    # « ifrs » devant « syscohada ».
    fichiers = sorted((f for f in os.listdir(DOSSIER)
                       if FICHIER.match(f) and not f.startswith(".")),
                      key=lambda f: (FICHIER.match(f).groups(), "ifrs" in f.lower(), f))
    if titre:
        fichiers = [f for f in fichiers if f.startswith(titre)]

    issues = {"concorde": [], "ecart": [], "trou": [], "devine": []}
    vus = set()
    for f in fichiers:
        ticker, annee = FICHIER.match(f).groups()
        annee = int(annee)
        if (ticker, annee) in vus:        # un seul document par exercice
            continue
        try:
            t, mode = texte(os.path.join(DOSSIER, f), avec_ocr)
        except Exception as err:                                # noqa: BLE001
            print(f"  {f[:60]:60} illisible : {err}")
            continue
        if t is None:
            continue
        vus.add((ticker, annee))
        precedent = connu.get((ticker, annee - 1))
        ancres = {}
        if precedent is not None:
            for c in ANCRES:
                v = getattr(precedent, c)
                if v == v and v:
                    ancres[c] = float(v)
        ligne = connu.get((ticker, annee))
        for champ, (valeur, comment) in lire_detaille(t, ancres=ancres).items():
            en_base = getattr(ligne, champ) if ligne is not None else None
            en_base = float(en_base) if en_base == en_base and en_base else None
            fiche = (ticker, annee, champ, valeur, en_base, f"{mode}/{comment}", f)
            if comment not in SURS:
                issues["devine"].append(fiche)
            elif en_base is None:
                issues["trou"].append(fiche)
            # UN POUR MILLE, pas un pour cent : un tableau en millions n'a
            # pas d'arrondi de 124 M. A 1 %, le resultat SGBCI 2025 recopie
            # de 2024 (101 228 au lieu de 101 352) passait pour concordant.
            elif abs(abs(valeur) - abs(en_base)) <= max(0.001 * abs(en_base), 1e6):
                issues["concorde"].append(fiche)
            else:
                issues["ecart"].append(fiche)

    fiches = _fiches({t for t, _, c, *_ in issues["ecart"]
                      if c in ("revenue", "net_income")}) if avec_fiches else {}

    Md = 1e9
    print(f"\n{len(vus)} exercice(s) lus sur {len(fichiers)} document(s)\n")
    for cle, titre_bloc in (("ecart", "ECARTS — valeur sure, la base porte autre chose"),
                            ("trou", "TROUS — valeur sure, la base est vide")):
        print(titre_bloc)
        for t_, a, c, v, b, m, f in sorted(issues[cle]):
            fiche = (fiches.get(t_) or {}).get(a, {}).get(c)
            verdict = ""
            if "ifrs" in f.lower():
                verdict = "  => document IFRS : autre referentiel que la base"
            elif fiche:
                # La fiche est en millions : a un pour mille pres, pas a 2 %.
                # A 2 %, elle « confirmait » a la fois 604 978 et 600 708 chez
                # Vivo, et donnait tort a une base verifiee a la main.
                proche = lambda x: x and abs(abs(x) - abs(fiche)) <= max(0.001 * abs(fiche), 1e6)
                verdict = ("  => fiche d'accord avec les deux : ecart d'arrondi" if proche(v) and proche(b)
                           else "  => la BASE a tort (fiche = document)" if proche(v)
                           # La base a souvent ete REMPLIE depuis la fiche :
                           # « fiche = base » n'est alors qu'une source.
                           else "  => document seul contre fiche = base : a lire" if proche(b)
                           else f"  => fiche {fiche/Md:.3f} Md : trois sources, trois valeurs")
                # L'OCR PERD LES SIGNES : parentheses et tirets ne survivent
                # pas toujours. Filtisac 2021 et Sucrivoire 2022 sont des
                # PERTES a la fiche et en base, lues positives. La comparaison
                # ci-dessus porte sur les valeurs absolues : on le dit.
                if (v < 0) != (fiche < 0):
                    verdict += " · SIGNE a verifier (fiche %s)" % ("negative" if fiche < 0 else "positive")
            print(f"  {t_:9} {a} {c:16} lu {v/Md:12.3f} Md"
                  + (f" · base {b/Md:12.3f} Md · x{v/b:6.2f}" if b else "")
                  + f"  [{m}]{verdict}")
        print()
    print(f"concorde : {len(issues['concorde'])} · ecarts : {len(issues['ecart'])}"
          f" · trous : {len(issues['trou'])} · devinees (non proposees) : "
          f"{len(issues['devine'])}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sans-scans", action="store_true")
    ap.add_argument("--titre")
    ap.add_argument("--fiches", action="store_true",
                    help="tranche les ecarts de CA et de resultat par la fiche societe")
    a = ap.parse_args()
    main(not a.sans_scans, a.titre, a.fiches)
