#!/usr/bin/env python3
"""Ajoute `dps_note` a `fundamentals` : la note qui accompagne un dividende.

Un dividende n'est pas toujours un simple dividende. FILTISAC 2024 distribue
2 000,67 F par action, dont 680,67 F de prime de fusion — un versement
exceptionnel qui ne se reproduira pas. Afficher le seul chiffre induit en
erreur sur le rendement recurrent ; le masquer sous-estime ce qui a ete
reellement encaisse. D'ou l'asterisque et sa note.

Colonne nullable : aucune donnee existante n'est touchee. Idempotent.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402


def main():
    conn = get_connection()
    try:
        try:
            conn.execute("ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS dps_note TEXT")
            print("OK : colonne dps_note", flush=True)
        except Exception as e:
            if "duplicate" in str(e).lower() or "exist" in str(e).lower():
                print("déjà présente : dps_note", flush=True)
            else:
                raise
        conn.commit()
        cur = conn.execute(
            "SELECT COUNT(*) FROM fundamentals WHERE dps_note IS NOT NULL")
        print("notes renseignées :", cur.fetchone()[0], flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
