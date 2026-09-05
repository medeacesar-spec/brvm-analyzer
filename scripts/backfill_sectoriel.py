#!/usr/bin/env python3
"""
Rattrape les colonnes sectorielles jamais ecrites en base.

Contexte : l'extracteur savait lire l'EBITDA, le cout du risque, les frais
generaux, le resultat brut d'exploitation, le resultat avant impot, les depots
et les credits. Mais les quatre appelants de `save_fundamentals` recopiaient
chacun une liste de champs en dur, etablie avant l'ajout de ces colonnes. Les
valeurs etaient donc extraites puis jetees : 0 ligne renseignee sur 263, et
les grilles sectorielles restaient vides.

Ce script repasse sur les rapports deja references et ecrit ce qui manque.
Il est interruptible : relance-le, il reprend ou il en etait.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection
from data.pdf_extractor import download_and_extract
from data.storage import save_fundamentals, champs_extraits

SECTORIELS = ("ebitda", "operating_expenses", "gross_operating_income",
              "pretax_income", "deposits", "loans", "cost_of_risk",
              "ordinary_income", "hao_income")


def rapports_a_traiter(conn, depuis: int) -> list:
    lignes = conn.execute(
        "SELECT ticker, fiscal_year, url, report_type FROM report_links "
        "WHERE fiscal_year >= ? AND url IS NOT NULL "
        "ORDER BY ticker, fiscal_year DESC",
        (depuis,),
    ).fetchall()
    vus, sortie = set(), []
    for ligne in lignes:
        d = dict(ligne)
        cle = (d["ticker"], d["fiscal_year"], d["url"])
        if cle in vus:
            continue
        vus.add(cle)
        sortie.append(d)
    return sortie


def main(depuis: int = 2023, avec_ocr: bool = True) -> None:
    conn = get_connection()
    rapports = rapports_a_traiter(conn, depuis)
    conn.close()
    print(f"Rapports a repasser (>= {depuis}) : {len(rapports)}", flush=True)

    ecrits = vides = erreurs = 0
    debut = time.time()

    for i, r in enumerate(rapports, 1):
        etiquette = f"[{i}/{len(rapports)}] {r['ticker']} {r['fiscal_year']}"
        try:
            res = download_and_extract(r["url"], use_ocr=False)
            if not champs_extraits(res) and avec_ocr:
                # Rapport scanne : l'OCR par coordonnees prend le relais.
                res = download_and_extract(r["url"], use_ocr=True)
        except Exception as exc:
            print(f"{etiquette} ERREUR {exc}", flush=True)
            erreurs += 1
            continue

        if res.get("error"):
            print(f"{etiquette} ERREUR {res['error']}", flush=True)
            erreurs += 1
            continue

        extraits = champs_extraits(res)
        if not extraits:
            vides += 1
            continue

        donnees = {"ticker": r["ticker"], "fiscal_year": r["fiscal_year"]}
        donnees.update(extraits)
        try:
            save_fundamentals(donnees)
        except Exception as exc:
            print(f"{etiquette} SAUVEGARDE KO {exc}", flush=True)
            erreurs += 1
            continue

        nouveaux = {k: v for k, v in extraits.items() if k in SECTORIELS}
        ecrits += 1
        if nouveaux:
            detail = ", ".join(f"{k}={v:,.0f}" for k, v in nouveaux.items())
            print(f"{etiquette} OK {detail}", flush=True)

    duree = time.time() - debut
    print(f"\nEcrits {ecrits} | sans donnee {vides} | erreurs {erreurs} "
          f"| {duree/60:.1f} min", flush=True)


if __name__ == "__main__":
    annee = 2023
    for a in sys.argv[1:]:
        if a.isdigit():
            annee = int(a)
    main(depuis=annee, avec_ocr="--sans-ocr" not in sys.argv)
