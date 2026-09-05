#!/usr/bin/env python3
"""
Cree la table `controle_anomalies`.

Le controle de vraisemblance annulait les valeurs impossibles sans rien
conserver : on savait qu'il avait mordu, jamais sur quoi. Impossible de
corriger de facon ciblee — il fallait relire les journaux d'execution, quand
ils existaient encore.

Chaque anomalie est desormais consignee : le titre, l'exercice, les champs
annules, le motif, et la date. Une anomalie qui ne reapparait pas au passage
suivant est marquee resolue plutot que supprimee, pour garder trace de ce qui
a ete repare.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, is_postgres


def main() -> None:
    conn = get_connection()
    serial = ("SERIAL PRIMARY KEY" if is_postgres()
              else "INTEGER PRIMARY KEY AUTOINCREMENT")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS controle_anomalies (
            id {serial},
            ticker TEXT NOT NULL,
            fiscal_year INTEGER,
            champs TEXT,
            motif TEXT,
            valeur REAL,
            detectee_le TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolue_le TIMESTAMP
        )
    """)
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_anomalie_unique "
                     "ON controle_anomalies (ticker, fiscal_year, motif)")
    except Exception as exc:
        print(f"index : {exc}", flush=True)
    conn.commit()
    n = dict(conn.execute(
        "SELECT COUNT(*) AS n FROM controle_anomalies").fetchone())["n"]
    print(f"OK : table controle_anomalies ({n} lignes)", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
