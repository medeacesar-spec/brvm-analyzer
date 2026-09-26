#!/usr/bin/env python3
"""Tient `price_monthly` a jour a partir des seances de `price_cache`.

Le mensuel n'etait alimente que par des imports ponctuels (sikafinance, puis
l'export RichBourse du 10/09/2026). Aucun robot ne l'avancait : les mesures de
risque et l'optimisation du portefeuille se seraient arretees a septembre 2026
sans que rien ne le signale.

Chaque jour, le mois precedent et le mois en cours sont recalcules depuis les
seances : cloture de la derniere seance du mois, volume cumule, date au 1er du
mois (la convention de la table). Le mois en cours n'est pas lu par les mesures
de risque tant qu'il n'est pas clos (`analysis.risque`) ; il est ecrit pour
que le mois soit complet des sa derniere seance.

Un mois ne garde qu'une ligne : les autres lignes du meme mois (le collecteur
sikafinance datait le mois de sa premiere seance) sont retirees.

Usage :
    python3 scripts/mettre_a_jour_mensuel.py [--simuler]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import tickers_retires                   # noqa: E402
from data.db import get_connection, read_sql_df      # noqa: E402


def _mois_a_recalculer(aujourd_hui: date) -> list:
    debut = aujourd_hui.replace(day=1)
    precedent = (date(debut.year - 1, 12, 1) if debut.month == 1
                 else date(debut.year, debut.month - 1, 1))
    return [precedent, debut]


def main(simuler: bool = False) -> int:
    hors_cote = tickers_retires()
    ecrits = 0
    cnx = None if simuler else get_connection()
    try:
        for mois in _mois_a_recalculer(date.today()):
            suivant = (date(mois.year + 1, 1, 1) if mois.month == 12
                       else date(mois.year, mois.month + 1, 1))
            d = read_sql_df(
                "SELECT ticker, date, close, volume FROM price_cache "
                "WHERE date >= ? AND date < ? AND close > 0",
                params=(mois.isoformat(), suivant.isoformat()))
            if d.empty:
                continue
            d = d[~d["ticker"].isin(hors_cote)].sort_values(["ticker", "date"])
            agrege = d.groupby("ticker").agg(close=("close", "last"),
                                             volume=("volume", "sum"))
            print(f"  [mensuel] {mois:%Y-%m} : {len(agrege)} titres")
            if simuler:
                continue
            valeurs = ",".join(
                f"('{t}', '{mois.isoformat()}', {r.close:.6f}, {float(r.volume or 0):.1f})"
                for t, r in agrege.iterrows())
            cles = ",".join(f"'{t}'" for t in agrege.index)
            cnx.execute(f"DELETE FROM price_monthly WHERE ticker IN ({cles}) "
                        f"AND date >= '{mois.isoformat()}' AND date < '{suivant.isoformat()}'")
            cnx.execute("INSERT INTO price_monthly (ticker, date, close, volume) "
                        f"VALUES {valeurs}")
            cnx.commit()
            ecrits += len(agrege)
    finally:
        if cnx:
            cnx.close()
    return ecrits


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--simuler", action="store_true")
    main(ap.parse_args().simuler)
