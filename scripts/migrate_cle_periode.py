#!/usr/bin/env python3
"""
Corrige la cle d'unicite de `quarterly_data`.

La table etait unique sur (ticker, exercice, quarter). Or `quarter` ne porte
que le RANG de la periode : un premier trimestre et un premier semestre valent
tous deux 1. Le semestre ECRASAIT donc le trimestre.

Constate sur Sonatel : le premier trimestre 2026 a disparu au profit du
premier semestre 2026, tous deux ranges sous quarter = 1. Les deux
publications existent pourtant et couvrent des durees differentes — c'est
precisement leur difference qui permet de deduire le deuxieme trimestre.

La cle devient (ticker, exercice, periode), ou `periode` distingue T1 de S1.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, is_postgres


def main() -> None:
    conn = get_connection()

    # Sans periode, la nouvelle cle serait ambigue : on la deduit du rang.
    orphelines = [dict(l) for l in conn.execute(
        "SELECT id, quarter FROM quarterly_data "
        "WHERE periode IS NULL OR periode = ''").fetchall()]
    for ligne in orphelines:
        rang = ligne.get("quarter")
        conn.execute("UPDATE quarterly_data SET periode = ? WHERE id = ?",
                     (f"T{rang}" if rang in (1, 2, 3, 4) else "T1", ligne["id"]))
    conn.commit()
    print(f"periodes deduites : {len(orphelines)}", flush=True)

    if is_postgres():
        try:
            conn.execute("ALTER TABLE quarterly_data "
                         "DROP CONSTRAINT IF EXISTS "
                         "quarterly_data_ticker_fiscal_year_quarter_key")
            conn.commit()
            print("ancienne contrainte retiree", flush=True)
        except Exception as exc:
            print(f"retrait de l'ancienne contrainte : {exc}", flush=True)
        try:
            conn.execute("ALTER TABLE quarterly_data "
                         "ADD CONSTRAINT quarterly_data_ticker_annee_periode_key "
                         "UNIQUE (ticker, fiscal_year, periode)")
            conn.commit()
            print("OK : cle (ticker, exercice, periode)", flush=True)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                print("cle deja en place", flush=True)
            else:
                raise
    else:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS "
                     "idx_quarterly_periode ON quarterly_data "
                     "(ticker, fiscal_year, periode)")
        conn.commit()
        print("OK : index unique (ticker, exercice, periode)", flush=True)

    n = dict(conn.execute(
        "SELECT COUNT(*) AS n FROM quarterly_data").fetchone())["n"]
    print(f"lignes : {n}", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
