#!/usr/bin/env python3
"""Orange CI et Onatel : remplacer les montants de PERIODE et les copies par
les comptes annuels.

LE DEFAUT (releve le 25/09/2026 sur la grille telecoms)

Orange CI : les lignes annuelles portaient des montants TRIMESTRIELS. EBITDA
2023 = 259,1 Md, l'EBITDAaL a neuf mois (rapport du 3e trimestre 2023) ;
EBITDA 2025 = 305,3, celui a neuf mois 2025 ; EBIT 2023 = 100,3, celui du
premier semestre. Les capitaux propres 2023 etaient la part du groupe
(627,2) et non le total (724,0), recopiee en 2024. Les flux de tresorerie
d'exploitation 2024 et 2025 (150,0 et 20,0) etaient des lignes « Autres
emprunts bancaires » du meme tableau.

Onatel (comptes sociaux SYSCOHADA) : capitaux propres, dette et total 2024
recopies de 2023 ; total 2025 de 616 Md, l'actif immobilise brut au lieu du
total general (324,9).

LES SOURCES (en milliards FCFA)

Orange CI, comptes consolides :
  EBITDAaL  2022 373,8 (rapport 2022) · 2023 377,9 (390,3 / 1,033 et
            373,8 x 1,011 concordent a 0,1 pres) · 2024 390,3 · 2025 424,1
            (etats 2025, chiffres cles, avec le comparatif 390,3)
  EBIT      2021 245,539 · 2022 260,834 (etats 2022) · 2023 257,960 (etats
            2023, comparatif 2024 : 258,0) · 2024 263,2 · 2025 287,2
  Capitaux propres totaux  2023 724,0 · 2024 713,8 · 2025 707,8
  Total du bilan  2021 1 755,021 · 2022 2 017,757
  Investissements  2023 130,5 (rapport integre) · 2024 159,5 · 2025 184,2
  Tresorerie d'exploitation  2024 256,3 · 2025 335,5
Onatel, comptes sociaux :
  Capitaux propres  2022 63,172 · 2024 62,561
  Dettes financieres  2022 61,609 · 2024 67,226
  Total du bilan  2024 301,493 · 2025 324,902
  Investissements (incorporelles + corporelles)  2024 41,148 · 2025 44,512
  Tresorerie d'exploitation  2024 62,599 · 2025 68,487

Non touches, a lire : la dette d'Orange CI (221,5 recopiee 2023-2024 ;
definition consolidee a arreter) ; capitaux propres et dette 2025 d'Onatel
(62,6 et 67,2, egaux a 2024), illisibles dans le document 2025.

Usage :
  python3 scripts/corriger_orange_onatel.py            # simulation
  python3 scripts/corriger_orange_onatel.py --ecrire
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, read_sql_df  # noqa: E402

Md = 1e9
VALEURS = {
    ("ORAC.ci", 2021): {"ebit": 245.539 * Md, "total_assets": 1755.021 * Md},
    ("ORAC.ci", 2022): {"ebitda": 373.8 * Md, "ebit": 260.834 * Md,
                        "total_assets": 2017.757 * Md},
    ("ORAC.ci", 2023): {"ebitda": 377.9 * Md, "ebit": 257.960 * Md,
                        "equity": 724.0 * Md, "capex": 130.5 * Md},
    ("ORAC.ci", 2024): {"ebitda": 390.3 * Md, "equity": 713.8 * Md,
                        "capex": 159.5 * Md, "cfo": 256.3 * Md},
    ("ORAC.ci", 2025): {"ebitda": 424.1 * Md, "ebit": 287.2 * Md, "equity": 707.8 * Md,
                        "capex": 184.2 * Md, "cfo": 335.5 * Md},
    ("ONTBF.bf", 2022): {"equity": 63.172 * Md, "total_debt": 61.609 * Md},
    ("ONTBF.bf", 2024): {"equity": 62.561 * Md, "total_debt": 67.226 * Md,
                         "total_assets": 301.493 * Md, "capex": 41.148 * Md,
                         "cfo": 62.599 * Md},
    ("ONTBF.bf", 2025): {"total_assets": 324.902 * Md, "capex": 44.512 * Md,
                         "cfo": 68.487 * Md},
}


def main(ecrire: bool) -> None:
    cnx = get_connection() if ecrire else None
    for (t, an), champs in VALEURS.items():
        avant = read_sql_df("SELECT " + ", ".join(champs) + " FROM fundamentals "
                            "WHERE ticker = ? AND fiscal_year = ?", params=(t, an))
        for c, v in champs.items():
            b = avant.iloc[0][c] if len(avant) else None
            b = b if b == b and b is not None else 0
            print(f"  {t:9} {an} {c:13} {b/Md:9.1f} Md -> {v/Md:9.1f} Md")
        if ecrire:
            cnx.execute("UPDATE fundamentals SET " + ", ".join(f"{c} = ?" for c in champs)
                        + ", updated_at = CURRENT_TIMESTAMP WHERE ticker = ? AND fiscal_year = ?",
                        (*champs.values(), t, an))
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
