#!/usr/bin/env python3
"""Backtest du signal FONDAMENTAL, sur les états financiers publiés.

Il ne lit QUE les fondamentaux et le cours du jour — jamais un indicateur
technique. Le pendant de `backtest_technique.py`, mesuré au même banc, pour
que les deux résultats se comparent et se cumulent.

LA RÈGLE QUI DÉCIDE DE TOUT : NE RIEN LIRE QUI N'ÉTAIT PAS PUBLIÉ

Sur la BRVM, l'exercice N paraît au printemps N+1. Au 31 janvier 2023, la
dernière liasse connue est donc celle de 2021, pas celle de 2022. Utiliser
2022 ce jour-là ferait « prédire » le cours avec des résultats que personne
n'avait — le backtest paraîtrait excellent et ne vaudrait rien.
`exercice_connu_le` applique ce décalage, et c'est la seule raison pour
laquelle ces chiffres méritent d'être lus.

CE QUE CE BACKTEST NE PEUT PAS COUVRIR

Les cours remontent à 1998 ; les fondamentaux, non. Chiffre d'affaires et
résultat net ne sont complets qu'à partir de l'exercice 2021 — 47 titres,
contre 29 en 2020 et un ou deux avant. Les capitaux propres ne le sont qu'en
2025, le total actif en 2024.

La fenêtre utile est donc de QUATRE ANS, contre vingt-huit pour le technique.
Ce n'est pas un défaut de méthode, c'est l'état de la base : le dire est plus
utile que de produire une courbe qui remonterait à 2005 sur trois titres.

Usage :
  python3 scripts/backtest_fondamental.py
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.backtest import (HORIZONS, Marche, agreger, entete,        # noqa: E402
                               exercice_connu_le, ligne, moyennes,
                               par_periode, par_tranche, rendements_futurs,
                               series_mensuelles_cours)
from analysis.fundamental import compute_ratios                  # noqa: E402
from data.db import read_sql_df                                  # noqa: E402

INDICES = {"BRVMC", "BRVM30"}

# En deçà, `compute_ratios` rend un score de remplissage qui ne mesure rien.
CHAMPS_INDISPENSABLES = ("revenue", "net_income")


def liasses() -> dict:
    """{(ticker, exercice): dict} — les états financiers, par exercice."""
    d = read_sql_df(
        "SELECT ticker, fiscal_year, company_name, sector, revenue, net_income, "
        "equity, total_debt, total_assets, ebit, interest_expense, cfo, capex, "
        "dividends_total, dps, shares FROM fundamentals")
    par_cle = {}
    for _, r in d.iterrows():
        if r["fiscal_year"] is None:
            continue
        par_cle[(r["ticker"], int(r["fiscal_year"]))] = {
            k: (None if v is None or (isinstance(v, float) and v != v) else v)
            for k, v in r.items()}
    return par_cle


def observations(depuis: int = 0) -> list:
    series = series_mensuelles_cours()
    if "BRVMC" not in series:
        print("Composite absent de price_monthly : rien à mesurer contre.")
        return []
    marche = Marche(series["BRVMC"])
    etats = liasses()

    tout, sans_liasse = [], defaultdict(int)
    for ticker, points in sorted(series.items()):
        if ticker in INDICES or len(points) < 1 + min(HORIZONS):
            continue
        dernier = len(points) - 1 - min(HORIZONS)
        for i in range(dernier + 1):
            jour, cours = points[i]
            if jour.year < depuis:
                continue
            liasse = etats.get((ticker, exercice_connu_le(jour)))
            if not liasse or any(liasse.get(c) is None
                                 for c in CHAMPS_INDISPENSABLES):
                sans_liasse[jour.year] += 1
                continue

            donnees = dict(liasse)
            donnees["price"] = cours          # le cours du jour, lui, est connu
            ratios = compute_ratios(donnees)
            score = ratios.get("fundamental_score")
            if score is None:
                continue

            bruts = rendements_futurs(points, i)
            relatifs = {}
            for h in HORIZONS:
                r, m = bruts.get(h), marche.rendement(jour, h)
                relatifs[h] = (r - m) if (r is not None and m is not None) else None
            tout.append({"ticker": ticker, "jour": jour, "score": score,
                         "detail": ratios.get("fundamental_breakdown") or {},
                         "exercice": exercice_connu_le(jour),
                         "rendements": bruts, "relatifs": relatifs})

    if sans_liasse:
        total = sum(sans_liasse.values())
        print(f"  {total:,} décision(s) écartées faute de liasse publiée "
              f"à la date — normal avant 2022, voir l'en-tête du script.")
    return tout




def main(depuis: int = 0):
    obs = observations(depuis)
    if not obs:
        print("Aucune observation exploitable.")
        return
    titres = len({o["ticker"] for o in obs})
    annees = sorted({o["jour"].year for o in obs})
    exercices = sorted({o["exercice"] for o in obs})
    print(f"\nBACKTEST FONDAMENTAL — {len(obs):,} décisions, {titres} titres, "
          f"{annees[0]}-{annees[-1]}")
    print(f"Exercices utilisés : {exercices[0]} à {exercices[-1]}.")
    print("Rendement RELATIF au Composite. « médiane » puis « part gagnante ».\n")

    print(entete())
    _ens = agreger(obs)
    print(ligne("ENSEMBLE", _ens))
    print(moyennes(_ens))

    print("\n  Par tranche de score fondamental (sur 50) :")
    for lib, mes in par_tranche(obs, [0, 15, 20, 25, 30, 35]).items():
        print(ligne(lib, mes))

    print("\n  Par période :")
    for lib, mes in par_periode(obs, decoupe=2).items():
        print(ligne(lib, mes))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--depuis", type=int, default=0)
    a = ap.parse_args()
    main(a.depuis)
