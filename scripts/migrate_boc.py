#!/usr/bin/env python3
"""Cree les tables du Bulletin Officiel de la Cote.

- `boc_bulletins`  : la synthese quotidienne (indices, capitalisation, volumes)
- `boc_operations` : les operations a venir, dividendes BRUTS en tete
- `boc_assemblees` : le calendrier des assemblees generales

Le BOC republie l'etat complet a chaque parution : operations et assemblees
sont donc remplacees par bulletin, pas accumulees.

Idempotent.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

DDL = [
    """CREATE TABLE IF NOT EXISTS boc_bulletins (
        date_bulletin  TEXT PRIMARY KEY,
        numero         TEXT,
        url            TEXT,
        capitalisation REAL,
        volume         REAL,
        valeur         REAL,
        hausses        REAL,
        baisses        REAL,
        stables        REAL,
        per_moyen      REAL,
        indices        TEXT,
        hausses_top    TEXT,
        baisses_top    TEXT,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS boc_operations (
        id             SERIAL PRIMARY KEY,
        date_bulletin  TEXT NOT NULL,
        emetteur       TEXT,
        ticker         TEXT,
        type           TEXT,
        brut           REAL,
        irvm_physique  INTEGER,
        irvm_morale    INTEGER,
        date_operation TEXT,
        detail         TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS boc_assemblees (
        id            SERIAL PRIMARY KEY,
        date_bulletin TEXT NOT NULL,
        societe       TEXT,
        ticker        TEXT,
        type          TEXT,
        date_ag       TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_boc_ops_date ON boc_operations (date_operation)",
    "CREATE INDEX IF NOT EXISTS idx_boc_ops_ticker ON boc_operations (ticker)",
    "CREATE INDEX IF NOT EXISTS idx_boc_ag_date ON boc_assemblees (date_ag)",
]


def main():
    conn = get_connection()
    try:
        for sql in DDL:
            conn.execute(sql)
            print("OK :", sql.split("\n")[0][:64], flush=True)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
