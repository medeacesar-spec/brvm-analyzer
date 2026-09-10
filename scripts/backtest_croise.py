#!/usr/bin/env python3
"""Croise les deux backtests : le technique apporte-t-il quelque chose au
fondamental, et réciproquement ?

C'est la raison d'être de la séparation. Tant que le score hybride
additionnait les deux, on ne pouvait pas savoir lequel portait l'information
— ni si le second en ajoutait après le premier. Ici, chaque décision reçoit
SES DEUX notes, et on regarde ce que donne chaque combinaison.

LECTURE

Le tableau se lit comme une carte. Si le technique apporte quelque chose, une
même tranche fondamentale doit se comporter différemment selon la tranche
technique — et inversement. Si les colonnes se ressemblent toutes, le second
score ne fait que répéter le premier.

La période commune est celle du fondamental : 2020-2026. Le technique remonte
à 1999, mais on ne peut croiser que là où les deux existent.

Usage :
  python3 scripts/backtest_croise.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.backtest import HORIZONS, agreger                  # noqa: E402

# Chargés par chemin : le script doit tourner depuis la racine du projet
# (sinon la résolution de DATABASE_URL bascule sur le SQLite local), mais ses
# deux voisins vivent dans scripts/.
import importlib.util as _u                                      # noqa: E402

def _voisin(nom):
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)), nom + ".py")
    spec = _u.spec_from_file_location(nom, chemin)
    module = _u.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

fond = _voisin("backtest_fondamental")
tech = _voisin("backtest_technique")

HORIZON = 12          # l'horizon où les deux signaux se séparent le mieux

TRANCHES_TECH = [(0, 25), (25, 30), (30, 50)]
TRANCHES_FOND = [(0, 20), (20, 30), (30, 50)]


def _tranche(valeur, bornes):
    for bas, haut in bornes:
        if bas <= valeur < haut:
            return f"{bas:g}–{haut:g}"
    return None


def main():
    print("Calcul des deux séries…")
    obs_t = {(o["ticker"], o["jour"]): o for o in tech.observations()}
    obs_f = {(o["ticker"], o["jour"]): o for o in fond.observations()}
    communes = sorted(set(obs_t) & set(obs_f))
    if not communes:
        print("Aucune décision commune aux deux backtests.")
        return

    croise = {}
    for cle in communes:
        t, f = obs_t[cle], obs_f[cle]
        lt = _tranche(t["score"], TRANCHES_TECH)
        lf = _tranche(f["score"], TRANCHES_FOND)
        if lt and lf:
            croise.setdefault((lf, lt), []).append(t)   # rendements identiques

    annees = sorted({j.year for _, j in communes})
    print(f"\nCROISEMENT — {len(communes):,} décisions communes, "
          f"{annees[0]}-{annees[-1]}, horizon {HORIZON} mois")
    print("Médiane du rendement relatif au Composite, et part gagnante.\n")

    libelles_t = [f"{a:g}–{b:g}" for a, b in TRANCHES_TECH]
    entete = "fond. / tech."
    print(f"  {entete:16}" + "".join(f"{lt:>18}" for lt in libelles_t))
    for a, b in TRANCHES_FOND:
        lf = f"{a:g}–{b:g}"
        cellules = []
        for lt in libelles_t:
            lot = croise.get((lf, lt), [])
            m = agreger(lot).get(HORIZON)
            cellules.append("             —" if not m else
                            f"{m['mediane']*100:+7.1f} % {m['part_gagnante']*100:3.0f}%")
        print(f"  {lf:16}" + "".join(f"{c:>18}" for c in cellules))

    print(f"\n  effectifs :")
    for a, b in TRANCHES_FOND:
        lf = f"{a:g}–{b:g}"
        n = [len(croise.get((lf, lt), [])) for lt in libelles_t]
        print(f"  {lf:16}" + "".join(f"{x:>18,}" for x in n))


if __name__ == "__main__":
    main()
