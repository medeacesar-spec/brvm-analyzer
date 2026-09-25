#!/usr/bin/env python3
"""Premier lot de corrections issues de la sonde de vraisemblance (25/09/2026).

Chaque valeur ci-dessous a ete relue dans les etats annuels ; « deux
documents » veut dire l'exercice N et le comparatif de N+1. Montants en
milliards FCFA.

  BICI CI 2022 PNB            12,157 -> 47,275  (comparatif des etats 2023)
  SODECI 2022 CA              37,080 -> 160,671 (etats 2023 : 175,458 et une
                                                 hausse de 14,787 Md)
  CBI Burkina 2022 total      1 952,98 -> 2 289,034 (rapport 2023)
  CBI Burkina 2023 total      199,567 -> 2 488,601 (deux documents ; la base
                                                 portait la colonne de VARIATION)
  CBI Burkina 2023 depots     1 439,2 -> 1 527,974 (deux documents)
  CBI Burkina 2024 total      trou -> 2 682,791 (rapport 2024)
  CIE 2021 et 2022 total      558,2 et 524,3 -> 1 679,593 et 1 816,387 (etats
                                                 2022, les deux colonnes)
  Ecobank CI 2024 CP          178,208 (copie de 2023) -> 199,352 (deux documents)
  SGBCI 2023 CP               345,011 (copie de 2022) -> 403,973 (deux documents)
  SGBCI 2024 CP               351,197 -> 451,721 (deux documents)
  SIB 2024 total              1 606,0 (copie de 2023) -> 1 685,249 (deux documents)
  TotalEnergies Senegal CP    2022 30,937 / 2023 7,522 / 2024 22,263 (trois
                              definitions melees) -> total consolide 34,561 /
                              29,928 / 29,522 (etats consolides 2023 et 2024)
  Unilever CI 2022 CP         -10,773 -> -11,299 (comparatif des etats 2023)
  Servair 2025 total          9,632 -> 9,227 (etats 2025) ; 2024 : 1,561,
                              illisible dans le document, vide
  Tractafric 2024 flux d'exploitation  976,193 (unite x 1 000) -> 0,158
                              (comparatif SYSCOHADA des etats 2025) ; 2025 : 1,855
  SAFCA 2021 EBIT             5,566 (au-dessus du PNB) -> -0,550 (etats 2021) ;
                              2025 : 7,200, au-dessus du PNB, vide

Usage :
  python3 scripts/corriger_vraisemblance_lot1.py            # simulation
  python3 scripts/corriger_vraisemblance_lot1.py --ecrire
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, read_sql_df  # noqa: E402

Md = 1e9
VIDE = None
VALEURS = {
    ("BICC.ci", 2022): {"revenue": 47.275 * Md},
    ("SDCC.ci", 2022): {"revenue": 160.671 * Md},
    ("CBIBF.bf", 2022): {"total_assets": 2289.034 * Md},
    ("CBIBF.bf", 2023): {"total_assets": 2488.601 * Md, "deposits": 1527.974 * Md},
    ("CBIBF.bf", 2024): {"total_assets": 2682.791 * Md},
    ("CIEC.ci", 2021): {"total_assets": 1679.593 * Md},
    ("CIEC.ci", 2022): {"total_assets": 1816.387 * Md},
    ("ECOC.ci", 2024): {"equity": 199.352 * Md},
    ("SGBC.ci", 2023): {"equity": 403.973 * Md},
    ("SGBC.ci", 2024): {"equity": 451.721 * Md},
    ("SIBC.ci", 2024): {"total_assets": 1685.249 * Md},
    ("TTLS.sn", 2022): {"equity": 34.561 * Md},
    ("TTLS.sn", 2023): {"equity": 29.928 * Md},
    ("TTLS.sn", 2024): {"equity": 29.522 * Md},
    ("UNLC.ci", 2022): {"equity": -11.299 * Md},
    ("ABJC.ci", 2024): {"total_assets": VIDE},
    ("ABJC.ci", 2025): {"total_assets": 9.227 * Md},
    ("PRSC.ci", 2024): {"cfo": 0.158 * Md},
    ("PRSC.ci", 2025): {"cfo": 1.855 * Md},
    ("SAFC.ci", 2021): {"ebit": -0.550 * Md},
    ("SAFC.ci", 2025): {"ebit": VIDE},
}


def main(ecrire: bool) -> None:
    cnx = get_connection() if ecrire else None
    for (t, an), champs in VALEURS.items():
        avant = read_sql_df("SELECT " + ", ".join(champs) + " FROM fundamentals "
                            "WHERE ticker = ? AND fiscal_year = ?", params=(t, an))
        for c, v in champs.items():
            b = avant.iloc[0][c] if len(avant) else None
            b = b if b is not None and b == b else None
            print(f"  {t:9} {an} {c:13} "
                  + (f"{b/Md:10.3f}" if b is not None else "      vide")
                  + " -> " + (f"{v/Md:10.3f} Md" if v is not None else "vide"))
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
