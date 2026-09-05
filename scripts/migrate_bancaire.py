#!/usr/bin/env python3
"""Ajoute a `fundamentals` les lignes du compte de resultat bancaire.

Le resultat net d'une banque n'est qu'un symptome : le diagnostic se lit dans
le coefficient d'exploitation (frais generaux / PNB) et dans le cout du risque
rapporte au PNB — l'analyse dite « en ciseau ». Ces colonnes portent aussi les
encours clientele, qui donnent le ratio credits / depots.

Colonnes nullables : aucune donnee existante n'est touchee. Idempotent.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

COLONNES = ("operating_expenses", "gross_operating_income", "pretax_income",
            "deposits", "loans")


def main():
    conn = get_connection()
    try:
        for col in COLONNES:
            try:
                conn.execute(
                    f"ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS {col} REAL")
                print(f"OK : colonne {col}", flush=True)
            except Exception as e:
                if "duplicate" in str(e).lower() or "exist" in str(e).lower():
                    print(f"déjà présente : {col}", flush=True)
                else:
                    raise
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
