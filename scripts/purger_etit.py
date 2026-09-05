#!/usr/bin/env python3
"""Efface les lectures d'Ecobank produites avant le correctif bi-devise.

La reprise de `extraire_periodes.py` ecarte toute periode qui porte deja un
montant : sans purge, les valeurs fausses d'ETI — un chiffre d'affaires mille
fois trop petit — survivraient au correctif indefiniment. On ne corrige pas
une valeur en base : on efface la lecture pour que le document soit relu.

Ne touche qu'aux lignes dont le montant est incoherent avec l'ordre de
grandeur du titre, jamais aux lectures deja justes.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

TICKER = "ETIT.tg"
# Un semestre d'Ecobank pese plus de 500 milliards FCFA. En dessous de dix,
# la lecture a perdu son echelle.
PLANCHER = 10_000_000_000


def main(appliquer: bool = False) -> None:
    conn = get_connection()
    lignes = [dict(l) for l in conn.execute(
        "SELECT fiscal_year, periode, revenue, net_income FROM quarterly_data "
        "WHERE ticker = ?", (TICKER,)).fetchall()]

    a_purger = [l for l in lignes
                if (l.get("revenue") or 0) and abs(l["revenue"]) < PLANCHER]

    print(f"{TICKER} : {len(lignes)} periodes, {len(a_purger)} a relire")
    for l in a_purger:
        print(f"   {l['fiscal_year']} {l['periode']:3s} "
              f"CA={(l['revenue'] or 0)/1e9:8.2f} Mds")

    if not appliquer:
        print("\nsimulation — relancer avec --appliquer pour effacer")
        conn.close()
        return

    for l in a_purger:
        conn.execute(
            "DELETE FROM quarterly_data WHERE ticker = ? AND fiscal_year = ? "
            "AND periode = ?", (TICKER, l["fiscal_year"], l["periode"]))
    conn.commit()
    print(f"\n{len(a_purger)} periode(s) effacee(s) : le prochain passage les relira")
    conn.close()


if __name__ == "__main__":
    main(appliquer="--appliquer" in sys.argv)
