#!/usr/bin/env python3
"""Ajoute la colonne `ebitda` a `fundamentals`.

L'extracteur calculait deja cette valeur mais n'avait pas ou l'ecrire. C'est
l'indicateur de reference des telecoms, qui communiquent en EBITDAaL.

Colonne nullable, idempotent.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402


def main():
    conn = get_connection()
    try:
        try:
            conn.execute("ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS ebitda REAL")
            print("OK : colonne ebitda", flush=True)
        except Exception as e:
            if "duplicate" in str(e).lower() or "exist" in str(e).lower():
                print("déjà présente : ebitda", flush=True)
            else:
                raise
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
