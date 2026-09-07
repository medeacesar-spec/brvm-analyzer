#!/usr/bin/env python3
"""Ajoute la DATE DE PAIEMENT du dividende, et la remplit depuis les avis BRVM.

Le montant ne suffit pas. Pour une performance totale, le MOIS de versement
compte autant : ranger tous les dividendes en juillet cree un pic artificiel
qui gonfle la volatilite — celle de Sonatel passait de 15,9 a 19,9 % pour cette
seule raison, et son ratio de Sharpe de 1,54 a 1,24.

La date vivait jusqu'ici dans le texte de `dps_note`, et seulement pour les
168 valeurs ecrites par la collecte des avis. Les autres — celles que la
collecte n'a pas touchees parce qu'elles differaient d'une operation sur titre
— n'en avaient aucune. Une colonne les porte toutes.

Usage :
    python3 scripts/migrate_dps_paiement.py [--simuler]
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection                              # noqa: E402
from scripts.collecter_avis_dividendes import (                 # noqa: E402
    EMETTEURS, _pli, moissonner)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--simuler", action="store_true")
    ap.add_argument("--pages", type=int, default=44)
    args = ap.parse_args()

    cnx = get_connection()
    if not args.simuler:
        cnx.execute("ALTER TABLE fundamentals "
                    "ADD COLUMN IF NOT EXISTS dps_paiement DATE")
        cnx.commit()

    avis = moissonner(args.pages)
    dates = {}
    for a in avis:
        ticker = EMETTEURS.get(_pli(a["emetteur"]))
        if not (ticker and a["exercice"] and a["paiement"]):
            continue
        cle = (ticker, a["exercice"])
        # Le paiement le plus recent fait foi, comme pour le montant.
        if cle not in dates or a["paiement"] > dates[cle]:
            dates[cle] = a["paiement"]

    ecrits = 0
    for (ticker, exercice), jour in sorted(dates.items()):
        if args.simuler:
            ecrits += 1
            continue
        cnx.execute("UPDATE fundamentals SET dps_paiement=%s "
                    "WHERE ticker=%s AND fiscal_year=%s AND dps IS NOT NULL",
                    (jour, ticker, exercice))
        ecrits += 1
    if not args.simuler:
        cnx.commit()

    reste = dict(cnx.execute(
        "SELECT count(*) n FROM fundamentals "
        "WHERE dps IS NOT NULL AND dps_paiement IS NULL").fetchone())["n"] \
        if not args.simuler else None
    cnx.close()
    print(f"{len(avis)} avis · {len(dates)} couples dates · {ecrits} lignes visees")
    if reste is not None:
        print(f"{reste} dividende(s) sans date de paiement — mois habituel du "
              f"titre par defaut")
    return 0


if __name__ == "__main__":
    sys.exit(main())
