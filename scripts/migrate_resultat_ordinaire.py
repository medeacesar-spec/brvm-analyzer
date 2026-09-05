#!/usr/bin/env python3
"""Ajoute a `fundamentals` les lignes du « Tableau d'activité et de résultats ».

- `ordinary_income` : résultat des activités ordinaires, pour neutraliser
  l'exceptionnel dans la valorisation.
- `hao_income` : résultat hors activités ordinaires (HAO).
- `cost_of_risk` : coût du risque des banques, à rapporter au PNB.

Colonnes nullables : aucune donnée existante n'est touchée. Idempotent.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

COLONNES = ("ordinary_income", "hao_income", "cost_of_risk")


def main():
    conn = get_connection()
    try:
        for col in COLONNES:
            try:
                conn.execute(
                    f"ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS {col} REAL")
                print(f"OK : colonne {col}", flush=True)
            except Exception as e:
                # SQLite ne connait pas IF NOT EXISTS sur ADD COLUMN
                if "duplicate" in str(e).lower() or "exist" in str(e).lower():
                    print(f"déjà présente : {col}", flush=True)
                else:
                    raise
        conn.commit()
        cur = conn.execute(
            "SELECT COUNT(*) FROM fundamentals WHERE ordinary_income IS NOT NULL")
        print("lignes renseignées :", cur.fetchone()[0], flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
