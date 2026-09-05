#!/usr/bin/env python3
"""
Annule les valeurs sectorielles ecrites par un rattrapage defectueux.

Le 05/09/2026, `backfill_sectoriel.py` a ecrit des postes de FLUX issus de
rapports trimestriels et semestriels dans des lignes ANNUELLES : le chiffre
d'affaires d'un trimestre se retrouvait a cote de charges annuelles. Trois
lignes sur quatre verifiables violaient l'identite comptable
« PNB - frais generaux = resultat brut d'exploitation » — Societe Generale CI
affichait meme un resultat brut d'exploitation superieur a son PNB.

Ces neuf colonnes etaient TOUTES vides avant ce rattrapage (0 ligne sur 263,
verifie). Les remettre a NULL restaure donc exactement l'etat anterieur : il
n'y a aucune donnee anterieure a perdre. Le rattrapage est ensuite relance
avec la regle flux / stock et le controle d'identite.

Lancer avec --dry-run pour voir ce qui serait efface sans rien ecrire.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

COLONNES = ("ebitda", "operating_expenses", "gross_operating_income",
            "pretax_income", "deposits", "loans", "cost_of_risk",
            "ordinary_income", "hao_income")


def main(dry_run: bool = False) -> None:
    conn = get_connection()

    condition = " OR ".join(f"{c} IS NOT NULL" for c in COLONNES)
    lignes = [dict(l) for l in conn.execute(
        f"SELECT id, ticker, fiscal_year FROM fundamentals WHERE {condition} "
        "ORDER BY ticker, fiscal_year"
    ).fetchall()]

    print(f"Lignes portant au moins une valeur sectorielle : {len(lignes)}")
    for l in lignes:
        print(f"  {l['ticker']} {l['fiscal_year']}")

    if dry_run:
        print("\n--dry-run : rien n'a ete modifie.")
        conn.close()
        return

    remise = ", ".join(f"{c} = NULL" for c in COLONNES)
    for l in lignes:
        conn.execute(f"UPDATE fundamentals SET {remise} WHERE id = ?", (l["id"],))
    conn.commit()

    restant = [dict(l) for l in conn.execute(
        f"SELECT id FROM fundamentals WHERE {condition}"
    ).fetchall()]
    print(f"\nLignes remises a NULL : {len(lignes)} | restant : {len(restant)}")
    conn.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
