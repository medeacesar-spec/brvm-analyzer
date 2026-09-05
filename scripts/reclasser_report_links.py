#!/usr/bin/env python3
"""
Reapplique la classification aux documents deja collectes.

Un correctif du classificateur ne vaut que pour les collectes A VENIR : les
lignes deja ecrites gardent leur type et leur exercice. Coris Bank portait
ainsi un « etats financiers 2026 » qui etait en realite le rapport d'examen
limite des commissaires aux comptes sur les comptes au 30 juin 2026 — des
comptes INTERMEDIAIRES. Cet exercice 2026 fictif remontait ensuite dans la
comparaison sectorielle.

Le script ne touche que le type et l'annee, jamais l'URL ni le titre, et
--dry-run montre ce qu'il ferait.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection
from scripts.scan_brvm_reports import _classify_pdf


def main(dry_run: bool = False) -> None:
    conn = get_connection()
    lignes = [dict(l) for l in conn.execute(
        "SELECT id, ticker, report_type, fiscal_year, url FROM report_links "
        "WHERE url IS NOT NULL ORDER BY ticker, fiscal_year DESC"
    ).fetchall()]

    corrections = []
    for ligne in lignes:
        genre, annee = _classify_pdf(ligne["url"])
        if not genre or not annee:
            continue
        if genre == ligne["report_type"] and annee == ligne["fiscal_year"]:
            continue
        corrections.append((ligne, genre, annee))

    print(f"documents examines : {len(lignes)}")
    print(f"a reclasser        : {len(corrections)}\n")
    for ligne, genre, annee in corrections:
        print(f"  {ligne['ticker']:9s} {ligne['report_type']}({ligne['fiscal_year']}) "
              f"-> {genre}({annee})")
        print(f"      {ligne['url'].rsplit('/', 1)[-1][:78]}")

    if dry_run:
        print("\n--dry-run : rien n'a ete modifie.")
        conn.close()
        return

    for ligne, genre, annee in corrections:
        conn.execute(
            "UPDATE report_links SET report_type = ?, fiscal_year = ? WHERE id = ?",
            (genre, annee, ligne["id"]),
        )
    conn.commit()
    print(f"\n{len(corrections)} documents reclasses.")
    conn.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
