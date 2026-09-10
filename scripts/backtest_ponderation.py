#!/usr/bin/env python3
"""Quelle pondération entre fondamental et technique ?

Le score hybride vaut aujourd'hui `fondamental + technique`, deux barèmes
sur cinquante : une somme simple, 50/50. Ce script mesure ce que donneraient
d'autres partages, du tout-technique au tout-fondamental.

CE QU'ON MESURE : LE POUVOIR DE SÉPARATION

Un score ne vaut pas par sa moyenne, mais par sa capacité à RANGER. Trois
mesures, de la plus fine à la plus parlante :

  rho    corrélation de rang entre le score et le rendement à venir. Zéro =
         le score ne range rien. C'est la mesure la plus honnête, elle utilise
         toutes les décisions et pas seulement les extrêmes.
  écart  différence de rendement médian entre le cinquième supérieur et le
         cinquième inférieur. C'est ce qu'on gagne à suivre le score plutôt
         qu'à l'ignorer.
  haut   part des décisions gagnantes dans le cinquième supérieur.

LE PIÈGE, ET COMMENT IL EST ÉVITÉ

Chercher le meilleur poids sur toutes les données, c'est trouver celui qui
raconte le mieux le passé — pas celui qui prédira. Le balayage est donc fait
DEUX FOIS : sur une période d'apprentissage, puis vérifié sur une période
que l'apprentissage n'a jamais vue. Si le poids retenu s'effondre hors
échantillon, c'est qu'il ne valait rien.

Usage :
  python3 scripts/backtest_ponderation.py
"""
from __future__ import annotations

import importlib.util as _u
import os
import statistics as st
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _voisin(nom):
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)), nom + ".py")
    spec = _u.spec_from_file_location(nom, chemin)
    module = _u.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HORIZON = 12
POIDS = [0.0, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0]
COUPURE = 2024          # apprentissage avant, verification a partir de


def _rangs(valeurs: list) -> list:
    """Rangs moyens, ex aequo partages."""
    ordre = sorted(range(len(valeurs)), key=lambda i: valeurs[i])
    rangs = [0.0] * len(valeurs)
    i = 0
    while i < len(ordre):
        j = i
        while j + 1 < len(ordre) and valeurs[ordre[j + 1]] == valeurs[ordre[i]]:
            j += 1
        moyen = (i + j) / 2 + 1
        for k in range(i, j + 1):
            rangs[ordre[k]] = moyen
        i = j + 1
    return rangs


def _rho(a: list, b: list) -> float:
    """Corrélation de rang de Spearman, sans dépendance externe."""
    if len(a) < 3:
        return 0.0
    ra, rb = _rangs(a), _rangs(b)
    ma, mb = st.mean(ra), st.mean(rb)
    num = sum((x - ma) * (y - mb) for x, y in zip(ra, rb))
    den = (sum((x - ma) ** 2 for x in ra) * sum((y - mb) ** 2 for y in rb)) ** 0.5
    return num / den if den else 0.0


def mesurer(lot: list, poids: float) -> dict:
    """lot : [(score_fond, score_tech, rendement_relatif)]."""
    if len(lot) < 50:
        return None
    scores = [poids * f + (1 - poids) * t for f, t, _ in lot]
    rendements = [r for _, _, r in lot]
    rho = _rho(scores, rendements)

    apparies = sorted(zip(scores, rendements))
    n = len(apparies)
    taille = max(n // 5, 1)
    bas = [r for _, r in apparies[:taille]]
    haut = [r for _, r in apparies[-taille:]]
    return {
        "n": n,
        "rho": rho,
        "ecart": st.median(haut) - st.median(bas),
        "haut_median": st.median(haut),
        "haut_gagnant": sum(1 for r in haut if r > 0) / len(haut),
        "bas_gagnant": sum(1 for r in bas if r > 0) / len(bas),
    }


def _tableau(titre: str, lot: list):
    print(f"\n  {titre} — {len(lot):,} décisions")
    print(f"  {'poids fond.':>12} {'rho':>7} {'écart haut-bas':>16} "
          f"{'haut : médiane':>16} {'gagn.':>7} {'bas gagn.':>10}")
    meilleur, best_rho = None, -9
    for p in POIDS:
        m = mesurer(lot, p)
        if not m:
            continue
        etoile = ""
        if m["rho"] > best_rho:
            best_rho, meilleur = m["rho"], p
        print(f"  {p*100:>10.0f} % {m['rho']:>7.3f} {m['ecart']*100:>15.1f} % "
              f"{m['haut_median']*100:>15.1f} % {m['haut_gagnant']*100:>6.0f}% "
              f"{m['bas_gagnant']*100:>9.0f}%")
    return meilleur


def main():
    tech = _voisin("backtest_technique")
    fond = _voisin("backtest_fondamental")
    print("Calcul des deux séries…")
    ot = {(o["ticker"], o["jour"]): o for o in tech.observations()}
    of = {(o["ticker"], o["jour"]): o for o in fond.observations()}

    lot = []
    for cle in sorted(set(ot) & set(of)):
        r = ot[cle]["relatifs"].get(HORIZON)
        if r is None:
            continue
        lot.append((of[cle]["score"], ot[cle]["score"], r, cle[1].year))

    if not lot:
        print("Aucune décision commune mesurable.")
        return

    annees = sorted({a for *_, a in lot})
    print(f"\nPONDÉRATION — horizon {HORIZON} mois, {annees[0]}-{annees[-1]}")
    print("0 % = tout technique · 100 % = tout fondamental")

    entier = [(f, t, r) for f, t, r, _ in lot]
    appr = [(f, t, r) for f, t, r, a in lot if a < COUPURE]
    hors = [(f, t, r) for f, t, r, a in lot if a >= COUPURE]

    _tableau("SUR TOUT L'ÉCHANTILLON (à ne pas prendre pour une promesse)", entier)
    retenu = _tableau(f"APPRENTISSAGE — avant {COUPURE}", appr)
    _tableau(f"VÉRIFICATION — {COUPURE} et après (jamais vue)", hors)

    if retenu is not None:
        m = mesurer(hors, retenu)
        print(f"\n  Le poids retenu sur l'apprentissage est {retenu*100:.0f} % "
              f"de fondamental.")
        if m:
            print(f"  Hors échantillon, il donne rho = {m['rho']:.3f}, "
                  f"écart {m['ecart']*100:+.1f} %, {m['haut_gagnant']*100:.0f} % "
                  f"de gagnants en haut de tableau.")


if __name__ == "__main__":
    main()
