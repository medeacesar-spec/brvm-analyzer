#!/usr/bin/env python3
"""Sonatel : aligner la base sur les comptes CONSOLIDES du groupe.

LE DEFAUT

La base portait, pour un meme exercice, des postes de deux perimetres :
le chiffre d'affaires et le resultat net du GROUPE (consolides, IFRS), et
le resultat d'exploitation, l'EBE et parfois les capitaux propres de la
SOCIETE MERE seule (comptes sociaux SYSCOHADA de Sonatel SA). En 2024 :
chiffre d'affaires groupe 1 776,4 Md, resultat d'exploitation SA 155,0 Md,
EBITDA 0,7 Md (illisible). La grille telecoms affichait une marge
d'EBITDA de 0 % et une dette de 616 fois l'EBITDA.

LA REGLE

Le groupe est la societe cotee que l'on analyse, et c'est son chiffre
d'affaires que la base porte depuis toujours : tous les postes suivent
donc le perimetre consolide. L'EBITDA est l'EBITDAaL publie par Sonatel
(EBITDA apres loyers, qui remplace l'EBITDA ajuste depuis 2023).

LES SOURCES (tous montants en millions FCFA)

  2022  capitaux propres 898 523 ; investissements 262 989
        — etats financiers 2023 (comparatif) et 2024 (troisieme colonne)
  2023  EBITDAaL 747,4 (839,2 - 91,8 de hausse, rapport 2024 ; « EBITDAal
        ajuste 747 » en annexe) ; capitaux propres 1 065 724 ; total
        2 573 858 ; investissements 288 126 — etats 2023 et 2024
  2024  EBITDAaL 839,2 ; resultat d'exploitation 619 524 ; investissements
        300 900 — rapport 2024 et comparatif des etats 2025
  2025  EBITDAaL 921,2 (taux 47,9 %) ; capitaux propres 1 399 263 ; total
        3 270 175 ; investissements 289 218 — etats financiers 2025.
        Les capitaux propres 2025 avaient deja ete portes a 1 399,3 Md le
        06/09, puis reecrits a 1 001,9 le 17/09 (cahier, #33).

  Tresorerie generee par l'exploitation (cfo) : 463 254 (2021), 464 136
  (2022), 643 077 (2023), 659 991 (2024), 681 606 (2025) — tableaux de
  flux consolides des etats 2023, 2024 et 2025, chaque exercice dans deux
  documents sauf 2025. La base portait 35,2 Md en 2022 et 233,0 en 2024 :
  la marge de flux libre sortait negative.

La dette financiere n'est pas touchee : sa definition consolidee (emprunts,
obligations, dettes locatives IFRS 16) reste a arreter.

Usage :
  python3 scripts/corriger_sonatel_consolide.py            # simulation
  python3 scripts/corriger_sonatel_consolide.py --ecrire
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, read_sql_df  # noqa: E402

M = 1e6
VALEURS = {
    2021: {"capex": 221_399 * M, "cfo": 463_254 * M},
    2022: {"equity": 898_523 * M, "capex": 262_989 * M, "cfo": 464_136 * M},
    2023: {"ebitda": 747_400 * M, "equity": 1_065_724 * M,
           "total_assets": 2_573_858 * M, "capex": 288_126 * M, "cfo": 643_077 * M},
    2024: {"ebitda": 839_200 * M, "ebit": 619_524 * M, "capex": 300_900 * M,
           "cfo": 659_991 * M},
    2025: {"ebitda": 921_200 * M, "equity": 1_399_263 * M,
           "total_assets": 3_270_175 * M, "capex": 289_218 * M, "cfo": 681_606 * M},
}


def main(ecrire: bool) -> None:
    cnx = get_connection() if ecrire else None
    for an, champs in VALEURS.items():
        avant = read_sql_df("SELECT " + ", ".join(champs) + " FROM fundamentals "
                            "WHERE ticker = 'SNTS.sn' AND fiscal_year = ?", params=(an,))
        for c, v in champs.items():
            b = avant.iloc[0][c] if len(avant) else None
            print(f"  {an} {c:13} {(b or 0)/1e9:10.1f} Md -> {v/1e9:10.1f} Md")
        if ecrire:
            cnx.execute("UPDATE fundamentals SET " + ", ".join(f"{c} = ?" for c in champs)
                        + ", updated_at = CURRENT_TIMESTAMP WHERE ticker = 'SNTS.sn' "
                        "AND fiscal_year = ?", (*champs.values(), an))
    if ecrire:
        cnx.commit()
        cnx.close()
        print("\nEcrit.")
    else:
        print("\nMode simulation : rien n'a ete ecrit.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
