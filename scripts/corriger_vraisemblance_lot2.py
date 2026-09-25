#!/usr/bin/env python3
"""Deuxieme lot de corrections issues de la sonde de vraisemblance (25/09/2026).

Montants en milliards FCFA, relus dans les etats annuels. Les etats BOA
ecrivent le COMPARATIF D'ABORD (colonne N-1, puis N) : c'est ce que la base
avait lu a l'envers pour BOA Mali et BOA Burkina 2025.

  SGBCI 2024 EBIT             120,046 (copie de 2023) -> 127,249
                              (RBE 163,484 - cout du risque 36,235, rapport 2024)
  SIB 2024 credits            1 037,0 (copie de 2023) -> 1 101,236 (chainage de
                              deux documents)
  Bernabe 2024 EBITDA, EBIT   0,489 et 0,524 -> 2,166 et 1,193 (etats 2024 et
                              comparatif 2025). Les premieres valeurs venaient
                              d'une lecture a un seul document ecrite le 25/09 :
                              le lecteur y avait mele deux exercices.
  Filtisac 2024 EBIT          0,882 -> -0,882 (signe perdu ; etats 2024 et 2025)
  CIE 2024 total              1 975,050 (copie de 2023) -> 2 019,136
  CIE 2025 total              2 019,140 (copie de 2024) -> 2 018,446 (etats 2025)
  BOA Burkina 2024 credits    577,808 -> 587,385 (etats 2024 et 2025)
  BOA Burkina 2024 cout du risque  4,303 -> -4,303 (une charge)
  BOA Burkina 2025 EBIT       25,697 (2024) -> 22,182 ; RBE 30,537 ; cout du
                              risque -8,355 (etats 2025, seconde colonne)
  BOA Mali 2025 credits, depots  276,168 et 420,336 (2024) -> 253,648 et
                              502,890 (etats 2025, seconde colonne)
  Chainage de deux documents (scripts/chainer_comparatifs.py) contre la base :
  CFAO 2022 EBIT              1,068 -> 9,881 ; 2023 : 41,109 -> 11,109 (faute
                              de frappe, un 4 pour un 1)
  CIE frais financiers        2022 : 4,247 -> 3,036 ; 2024 : 18,087 -> 3,291
  Filtisac 2021 EBIT          -0,179 -> 1,049
  NEI-CEDA 2023 EBIT          1,850 -> 0,850
  CIE 2020 et Erium 2020 CA   722,6 et 4 780,5 Md, sans rapport avec les
                              exercices voisins (231,8 et 7,6) : vides.

Usage :
  python3 scripts/corriger_vraisemblance_lot2.py            # simulation
  python3 scripts/corriger_vraisemblance_lot2.py --ecrire
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
    ("SGBC.ci", 2024): {"ebit": 127.249 * Md},
    ("SIBC.ci", 2024): {"loans": 1101.236 * Md},
    ("BNBC.ci", 2024): {"ebitda": 2.166 * Md, "ebit": 1.193 * Md},
    ("FTSC.ci", 2024): {"ebit": -0.882 * Md},
    ("CIEC.ci", 2024): {"total_assets": 2019.136 * Md},
    ("CIEC.ci", 2025): {"total_assets": 2018.446 * Md},
    ("BOABF.bf", 2024): {"loans": 587.385 * Md, "cost_of_risk": -4.303 * Md},
    ("BOABF.bf", 2025): {"ebit": 22.182 * Md, "gross_operating_income": 30.537 * Md,
                         "cost_of_risk": -8.355 * Md},
    ("BOAM.ml", 2025): {"loans": 253.648 * Md, "deposits": 502.890 * Md},
    ("CFAC.ci", 2022): {"ebit": 9.881 * Md},
    ("CFAC.ci", 2023): {"ebit": 11.109 * Md},
    ("CIEC.ci", 2022): {"interest_expense": 3.036 * Md},
    ("CIEC.ci", 2024): {"interest_expense": 3.291 * Md},
    ("FTSC.ci", 2021): {"ebit": 1.049 * Md},
    ("NEIC.ci", 2023): {"ebit": 0.850 * Md},
    ("CIEC.ci", 2020): {"revenue": VIDE},
    ("SIVC.ci", 2020): {"revenue": VIDE},
}


def main(ecrire: bool) -> None:
    cnx = get_connection() if ecrire else None
    for (t, an), champs in VALEURS.items():
        avant = read_sql_df("SELECT " + ", ".join(champs) + " FROM fundamentals "
                            "WHERE ticker = ? AND fiscal_year = ?", params=(t, an))
        for c, v in champs.items():
            b = avant.iloc[0][c] if len(avant) else None
            b = b if b is not None and b == b else None
            print(f"  {t:9} {an} {c:22} "
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
