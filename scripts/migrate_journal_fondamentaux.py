#!/usr/bin/env python3
"""Journalise toute modification de `fundamentals`, au niveau de la base.

Une ligne de `fundamentals` porte un titre et un exercice, et s'ecrase a chaque
correction. Le 6 septembre 2026, les capitaux propres de SITAB sont passes de
46,058 a 45,642 milliards : la valeur d'avant a disparu sans laisser de trace.
On ne pouvait donc ni auditer une correction, ni dater l'entree d'une donnee,
ni expliquer pourquoi un score avait bouge un jour precis.

LE DECLENCHEUR PLUTOT QUE LES SCRIPTS. Instrumenter chaque script laisserait
dehors tout ce qu'on oublie — une saisie a la main, un script ecrit demain, une
correction passee en console. Le declencheur voit tout ce qui touche la table,
quelle qu'en soit l'origine.

La comparaison se fait par `jsonb` plutot que colonne par colonne : la table en
compte plus de quarante, elle en gagnera d'autres, et une liste ecrite a la
main serait fausse au premier ajout.

Usage :
    python3 scripts/migrate_journal_fondamentaux.py [--verifier]
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

TABLE = """
CREATE TABLE IF NOT EXISTS fundamentals_journal (
    id           BIGSERIAL PRIMARY KEY,
    ticker       TEXT NOT NULL,
    fiscal_year  INTEGER,
    champ        TEXT NOT NULL,
    avant        TEXT,
    apres        TEXT,
    operation    TEXT NOT NULL,
    application  TEXT,
    ecrit_le     TIMESTAMP NOT NULL DEFAULT NOW()
)
"""

INDEX = """
CREATE INDEX IF NOT EXISTS fundamentals_journal_titre
    ON fundamentals_journal (ticker, fiscal_year, ecrit_le DESC)
"""

FONCTION = """
CREATE OR REPLACE FUNCTION journaliser_fundamentals() RETURNS trigger AS $journal$
DECLARE
    cle    text;
    avant  jsonb;
    apres  jsonb;
BEGIN
    apres := to_jsonb(NEW);
    avant := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE '{}'::jsonb END;
    FOR cle IN SELECT jsonb_object_keys(apres) LOOP
        -- L'identifiant et les horodatages changent a chaque ecriture sans
        -- rien apprendre : les journaliser noierait les vraies corrections.
        IF cle NOT IN ('id', 'created_at', 'updated_at')
           AND (avant -> cle) IS DISTINCT FROM (apres -> cle)
           AND NOT (TG_OP = 'INSERT' AND (apres -> cle) = 'null'::jsonb) THEN
            INSERT INTO fundamentals_journal
                (ticker, fiscal_year, champ, avant, apres, operation, application)
            VALUES (NEW.ticker, NEW.fiscal_year, cle,
                    avant ->> cle, apres ->> cle, TG_OP,
                    current_setting('application_name', true));
        END IF;
    END LOOP;
    RETURN NEW;
END;
$journal$ LANGUAGE plpgsql
"""

DECLENCHEUR = """
DROP TRIGGER IF EXISTS journal_fundamentals ON fundamentals;
CREATE TRIGGER journal_fundamentals
    AFTER INSERT OR UPDATE ON fundamentals
    FOR EACH ROW EXECUTE FUNCTION journaliser_fundamentals()
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verifier", action="store_true",
                    help="ecrit puis annule une correction, pour prouver que "
                         "le declencheur mord")
    args = ap.parse_args()

    cnx = get_connection()
    cnx.execute(TABLE)
    cnx.execute(INDEX)
    cnx.execute(FONCTION)
    for ordre in DECLENCHEUR.strip().split(";"):
        if ordre.strip():
            cnx.execute(ordre)
    cnx.commit()
    print("journal et declencheur en place")

    if args.verifier:
        avant = dict(cnx.execute(
            "SELECT dps FROM fundamentals WHERE ticker='SNTS.sn' "
            "AND fiscal_year=2024").fetchone())["dps"]
        cnx.execute("UPDATE fundamentals SET dps=%s WHERE ticker='SNTS.sn' "
                    "AND fiscal_year=2024", (avant,))          # meme valeur
        cnx.execute("UPDATE fundamentals SET dps=%s WHERE ticker='SNTS.sn' "
                    "AND fiscal_year=2024", (avant + 1,))      # vraie difference
        cnx.execute("UPDATE fundamentals SET dps=%s WHERE ticker='SNTS.sn' "
                    "AND fiscal_year=2024", (avant,))          # remise en etat
        cnx.commit()
        lignes = [dict(r) for r in cnx.execute(
            "SELECT champ, avant, apres, operation, ecrit_le FROM "
            "fundamentals_journal WHERE ticker='SNTS.sn' AND fiscal_year=2024 "
            "ORDER BY id DESC LIMIT 4")]
        print(f"\n{len(lignes)} entree(s) — une ecriture SANS changement ne "
              f"doit rien journaliser :")
        for l in lignes:
            print(f"   {l['champ']:12s} {l['avant']} -> {l['apres']} "
                  f"({l['operation']})")
    total = dict(cnx.execute(
        "SELECT count(*) n FROM fundamentals_journal").fetchone())["n"]
    cnx.close()
    print(f"\njournal : {total} entree(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
