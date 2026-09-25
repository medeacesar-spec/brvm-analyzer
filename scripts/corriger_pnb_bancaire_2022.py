#!/usr/bin/env python3
"""Trois produits nets bancaires 2022 que TROIS sources contredisent.

La base portait, pour 2022, un montant qui ressemble a un trimestre : NSIA
35,9 Md entre 76,6 (2021) et 91,0 (2023), Oragroup 52,8 Md entre 187 et 215,
SIB 21,8 Md entre 76,5 et 95,6. Le document 2022, la colonne comparative du
document 2023 et la fiche societe disent tous trois autre chose :

  NSIA      « Produit Net Bancaire 76 622 80 105 » (2022)
            « Produit Net Bancaire 80 105 91 002 » (2023)         fiche 80,1 Md
  Oragroup  « Produit net Bancaire 222 431 187 315 » (2022)
            « Produit net Bancaire 215 280 222 431 » (2023)       fiche 222,4 Md
  SIB       « C.10 PRODUIT NET BANCAIRE 76 532 83 542 » (2022, scan)
            « C10 PRODUIT NET BANCAIRE 83 542 95 571 » (2023)     fiche 83,5 Md

Trouves le 25/09 en verifiant l'identite bancaire PNB - charges = RBE : ces
trois exercices la violaient de 100 % et plus. Deux sont ensuite ressortis
seuls du recoupement, une fois les ancres 2021 corrigees par la #196 — l'effet
de cascade.

Usage :
  python3 scripts/corriger_pnb_bancaire_2022.py --simuler
  python3 scripts/corriger_pnb_bancaire_2022.py
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

Md = 1_000_000_000

VALEURS = {
    ("NSBC.ci", 2022): {"revenue": 80_105_000_000,
                        "source": "Etats financiers 2022 et 2023 - NSIA Banque CI, en millions ; fiche 80,1 Md"},
    ("ORGT.tg", 2022): {"revenue": 222_431_000_000,
                        "source": "Etats financiers 2022 et 2023 - Oragroup, en millions ; fiche 222,4 Md"},
    ("SIBC.ci", 2022): {"revenue": 83_542_000_000,
                        "source": "Rapports d'activite 2022 et 2023 - SIB, en millions ; fiche 83,5 Md"},
}


def main(simuler: bool) -> None:
    cnx = get_connection()
    ecrits = 0
    for (ticker, exercice), valeurs in sorted(VALEURS.items()):
        champs = {c: v for c, v in valeurs.items() if c != "source"}
        ligne = cnx.execute(
            "SELECT " + ", ".join(champs) + " FROM fundamentals "
            "WHERE ticker = ? AND fiscal_year = ?", (ticker, exercice)).fetchone()
        if ligne is None:
            print(f"\n{ticker} {exercice} — ligne absente, rien n'est ecrit"); continue
        avant = dict(ligne)
        print(f"\n{ticker} {exercice} — {valeurs['source']}")
        for champ, apres in champs.items():
            vieux = avant.get(champ)
            marque = "=" if vieux and abs(vieux / apres - 1) < 0.001 else "->"
            print(f"   {champ:13} {(vieux or 0)/Md:10.4f} Md {marque} {apres/Md:10.4f} Md")
        if simuler:
            continue
        cnx.execute(
            "UPDATE fundamentals SET " + ", ".join(f"{c} = ?" for c in champs)
            + " WHERE ticker = ? AND fiscal_year = ?",
            (*champs.values(), ticker, exercice))
        ecrits += 1
    if not simuler:
        cnx.commit()
    cnx.close()
    print(f"\n{len(VALEURS)} ligne(s) examinee(s), {ecrits} ecrite(s)."
          + (" Mode simulation : rien n'a ete ecrit." if simuler else ""))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--simuler", action="store_true")
    main(ap.parse_args().simuler)
