#!/usr/bin/env python3
"""Cree la table `telecom_parcs` : les parcs clients des operateurs.

Pour un operateur telecom, le chiffre d'affaires n'est que la consequence du
parc. Sonatel T1-2026 illustre pourquoi les deux se lisent ensemble : le parc
mobile RECULE de 2,9 % — durcissement des regles d'identification — pendant que
la fibre progresse de 26,5 % et Orange Money de 5,5 %. Un seul chiffre
d'affaires en croissance masquerait entierement ce mouvement.

Une ligne par parc et par periode. Idempotent.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

DDL = [
    """CREATE TABLE IF NOT EXISTS telecom_parcs (
        id          SERIAL PRIMARY KEY,
        ticker      TEXT NOT NULL,
        fiscal_year INTEGER,
        periode     TEXT,
        parc        TEXT NOT NULL,
        valeur      REAL,
        variation   REAL,
        source_url  TEXT,
        updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (ticker, fiscal_year, periode, parc)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_parcs_ticker ON telecom_parcs (ticker)",
]


def main():
    conn = get_connection()
    try:
        for sql in DDL:
            conn.execute(sql)
            print("OK :", sql.split("\n")[0][:60], flush=True)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
