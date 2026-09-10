#!/usr/bin/env python3
"""Backtest du signal TECHNIQUE, sur tout l'historique des cours.

Il ne lit QUE les cours. Aucun fondamental n'entre ici, et c'est le but :
savoir ce que le technique prédit à lui seul, avant de le mélanger à quoi
que ce soit.

MÉTHODE

À la fin de chaque mois, on calcule le score technique du titre avec les
seules données disponibles à cette date, puis on regarde ce que le titre a
fait pendant les 1, 3, 6 et 12 mois suivants — RELATIVEMENT au Composite.
Un titre qui gagne 8 % quand la place en fait 12 a perdu 4 points.

Les indicateurs (SMA, RSI, MACD, Bollinger) sont tous CAUSAUX : la valeur
au mois i ne dépend que des mois ≤ i. Ils sont donc calculés une seule fois
sur la série complète, puis lus mois par mois. Le résultat est identique à
un recalcul à chaque date, pour un coût cent fois moindre.

CE QUE LE RÉSULTAT NE DIT PAS

Le score est mesuré sur la série MENSUELLE. Les paramètres mensuels de
`technical.py` (MONTHLY_PARAMS) ne sont pas ceux du journalier : ce backtest
juge le signal tel que l'application le calcule en vue longue, pas tel
qu'elle l'affiche dans l'onglet Technique en vue quotidienne.

Usage :
  python3 scripts/backtest_technique.py
  python3 scripts/backtest_technique.py --depuis 2010
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd                                              # noqa: E402

from analysis.backtest import (HORIZONS, Marche, agreger, entete,        # noqa: E402
                               ligne, moyennes, par_periode,
                               par_tranche, rendements_futurs,
                               series_mensuelles_cours)
from analysis.technical import (compute_all_indicators,          # noqa: E402
                                compute_technical_score)

# Huit mois : le minimum que `compute_technical_score` exige en mensuel.
# En deçà il rend 25, un score neutre qui ne mesure rien.
MINIMUM_HISTORIQUE = 8

INDICES = {"BRVMC", "BRVM30"}


def observations(depuis: int = 0) -> list:
    series = series_mensuelles_cours()
    if "BRVMC" not in series:
        print("Composite absent de price_monthly : rien à mesurer contre.")
        return []
    marche = Marche(series["BRVMC"])

    tout = []
    for ticker, points in sorted(series.items()):
        if ticker in INDICES or len(points) < MINIMUM_HISTORIQUE + max(HORIZONS):
            continue
        df = pd.DataFrame({"date": pd.to_datetime([d for d, _ in points]),
                           "close": [c for _, c in points],
                           "volume": [0.0] * len(points)})
        df = compute_all_indicators(df)

        dernier = len(points) - 1 - min(HORIZONS)
        for i in range(MINIMUM_HISTORIQUE, dernier + 1):
            jour = points[i][0]
            if jour.year < depuis:
                continue
            score = compute_technical_score(df.iloc[:i + 1])
            bruts = rendements_futurs(points, i)
            relatifs = {}
            for h in HORIZONS:
                r, m = bruts.get(h), marche.rendement(jour, h)
                relatifs[h] = (r - m) if (r is not None and m is not None) else None
            tout.append({"ticker": ticker, "jour": jour, "score": score,
                         "rendements": bruts, "relatifs": relatifs})
    return tout






def main(depuis: int = 0):
    obs = observations(depuis)
    if not obs:
        return
    titres = len({o["ticker"] for o in obs})
    annees = sorted({o["jour"].year for o in obs})
    print(f"\nBACKTEST TECHNIQUE — {len(obs):,} décisions, {titres} titres, "
          f"{annees[0]}-{annees[-1]}")
    print("Rendement RELATIF au Composite. « médiane » puis « part gagnante ».\n")

    print(entete())
    _ens = agreger(obs)
    print(ligne("ENSEMBLE", _ens))
    print(moyennes(_ens))

    print("\n  Par tranche de score technique (sur 50) :")
    for lib, mes in par_tranche(obs, [0, 20, 25, 30, 35, 40]).items():
        print(ligne(lib, mes))

    print("\n  Par période :")
    for lib, mes in par_periode(obs).items():
        print(ligne(lib, mes))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--depuis", type=int, default=0)
    a = ap.parse_args()
    main(a.depuis)
