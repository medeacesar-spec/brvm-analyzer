#!/usr/bin/env python3
"""
Renseigne `fundamentals.sector` la ou il est vide.

Pourquoi : les grilles sectorielles (bancaire, telecoms, et desormais toutes
les autres) sont conditionnees au secteur porte par la ligne `fundamentals`.
Or 48 lignes sur 263 — couvrant 32 societes dont Sonatel et Ecobank — avaient
`sector` a NULL. Consequence : aucune grille ne s'affichait, pour personne.

Le secteur est repris de `market_data` (qui l'a pour les 48 tickers), puis a
defaut du referentiel `data/brvm_tickers.json`.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection


def _referentiel() -> dict:
    chemin = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "data", "brvm_tickers.json")
    try:
        with open(chemin, encoding="utf-8") as fh:
            brut = json.load(fh)
    except Exception as exc:
        print(f"  referentiel illisible ({exc}) — on s'appuiera sur market_data seul")
        return {}
    if isinstance(brut, dict):
        paires = brut.items()
    else:
        paires = [(x.get("ticker"), x) for x in brut]
    return {t: (v.get("sector") or "").strip() for t, v in paires if t}


def main(dry_run: bool = False) -> None:
    conn = get_connection()
    reference = _referentiel()

    # Les lignes psycopg de ce projet sont des mappings, pas des tuples :
    # les deballer par position rend les NOMS de colonnes, pas les valeurs.
    marche = {}
    for ligne in conn.execute(
        "SELECT DISTINCT ticker, sector FROM market_data "
        "WHERE sector IS NOT NULL AND sector <> ''"
    ).fetchall():
        d = dict(ligne)
        marche[d["ticker"]] = (d["sector"] or "").strip()

    vides = [dict(ligne) for ligne in conn.execute(
        "SELECT id, ticker FROM fundamentals WHERE sector IS NULL OR sector = ''"
    ).fetchall()]
    print(f"Lignes fundamentals sans secteur : {len(vides)}")

    corrigees, orphelines = 0, []
    for ligne in vides:
        ligne_id, ticker = ligne["id"], ligne["ticker"]
        secteur = marche.get(ticker) or reference.get(ticker) or ""
        if not secteur:
            orphelines.append(ticker)
            continue
        if not dry_run:
            conn.execute("UPDATE fundamentals SET sector = ? WHERE id = ?",
                         (secteur, ligne_id))
        corrigees += 1

    if not dry_run:
        conn.commit()

    print(f"Lignes renseignees : {corrigees}")
    if orphelines:
        print(f"Sans secteur connu ({len(set(orphelines))} tickers) : "
              f"{', '.join(sorted(set(orphelines)))}")

    restant = dict(conn.execute(
        "SELECT COUNT(*) AS n FROM fundamentals WHERE sector IS NULL OR sector = ''"
    ).fetchone())["n"]
    print(f"Restant sans secteur : {restant}")
    conn.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
