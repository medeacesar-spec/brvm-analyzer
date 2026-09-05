#!/usr/bin/env python3
"""
Ajoute la colonne `periode` a `quarterly_data`.

La table ne portait qu'un entier `quarter`, ce qui ne distingue pas un
trimestre d'un semestre : le premier semestre 2025 d'Orange CI, 579,3 Mds sur
six mois, y figurait comme « trimestre 2 ». Comparer une telle valeur au
deuxieme trimestre d'un autre exercice n'a aucun sens.

`periode` prend les formes T1, T2, T3, T4, S1, S2 — la lettre dit la duree
couverte, le chiffre le rang. La colonne `quarter` est conservee telle quelle
pour ne rien casser de l'existant.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, is_postgres


def main() -> None:
    conn = get_connection()
    try:
        if is_postgres():
            conn.execute("ALTER TABLE quarterly_data "
                         "ADD COLUMN IF NOT EXISTS periode TEXT")
        else:
            try:
                conn.execute("ALTER TABLE quarterly_data ADD COLUMN periode TEXT")
            except Exception:
                pass
        conn.commit()
        print("OK : colonne periode", flush=True)
    except Exception as exc:
        if "already exists" in str(exc).lower() or "duplicate" in str(exc).lower():
            print("deja presente : periode", flush=True)
        else:
            raise

    # Reprise de l'existant : sans indication de duree, un enregistrement
    # ancien reste un trimestre, ce qu'il etait cense etre.
    lignes = [dict(l) for l in conn.execute(
        "SELECT id, quarter FROM quarterly_data WHERE periode IS NULL"
    ).fetchall()]
    for ligne in lignes:
        rang = ligne.get("quarter")
        if rang in (1, 2, 3, 4):
            conn.execute("UPDATE quarterly_data SET periode = ? WHERE id = ?",
                         (f"T{rang}", ligne["id"]))
    conn.commit()
    print(f"lignes reprises : {len(lignes)}", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
