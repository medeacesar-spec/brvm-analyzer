"""Banc de mesure commun aux backtests technique et fondamental.

CE QUE CE MODULE FAIT, ET CE QU'IL REFUSE DE FAIRE

Il prend des DÉCISIONS datées — « au 30 juin 2019, ce titre valait tant » —
et mesure ce qui s'est passé APRÈS. Il ne calcule aucun score : les deux
moteurs le font chacun de leur côté, et c'est délibéré. Le score technique ne
lit que les cours, le score fondamental que les états financiers ; les
mélanger ici interdirait de savoir lequel des deux porte le pouvoir prédictif.

TROIS RÈGLES, DONT DEUX QUI FONT ÉCHOUER LA PLUPART DES BACKTESTS

1. AUCUN REGARD VERS L'AVENIR. Une décision au 30 juin 2019 ne peut lire que
   ce qui était connu au 30 juin 2019. Pour les cours c'est évident ; pour les
   fondamentaux ce l'est moins — l'exercice 2018 n'est publié qu'au printemps
   2019, et l'utiliser en janvier 2019 serait tricher. `exercice_connu_le`
   applique ce décalage.

2. LE RENDEMENT SE MESURE CONTRE LE MARCHÉ. Un signal qui rapporte 8 % quand
   la cote en fait 12 est un mauvais signal. Toutes les mesures sont donc
   RELATIVES au Composite sur la même fenêtre. C'est la différence entre
   mesurer un signal et mesurer un marché haussier.

3. ON NE JETTE PAS LES PÉRIODES QUI DÉRANGENT. Le backtest court sur tout
   l'historique disponible, y compris les années où la place a reculé. Un
   signal qui ne tient que sur 2021-2024 doit se voir comme tel, et la
   ventilation par période est là pour ça.
"""
from __future__ import annotations

import statistics as st
from collections import defaultdict
from datetime import date

import pandas as pd

from data.db import read_sql_df

# Horizons mesurés, en mois.
HORIZONS = (1, 3, 6, 12)

# Cadence UEMOA : l'exercice N est publié au printemps N+1. Avant mai, la
# dernière liasse connue est donc celle de N-2. Voir docs/lecture_des_etats.
MOIS_PUBLICATION = 5

# En deçà, une moyenne de rendements ne veut rien dire.
MINIMUM_OBSERVATIONS = 20


def exercice_connu_le(jour: date) -> int:
    """Le dernier exercice PUBLIÉ à cette date.

    Au 30 juin 2019, l'exercice 2018 est publié : on peut s'en servir. Au
    31 janvier 2019, il ne l'est pas encore — la dernière liasse connue est
    2017. Sans ce décalage, le backtest fondamental lirait des résultats que
    personne n'avait sous les yeux, et tous ses chiffres seraient faux.
    """
    return jour.year - 1 if jour.month >= MOIS_PUBLICATION else jour.year - 2


def series_mensuelles_cours() -> dict:
    """{ticker: [(date, close), …]} depuis price_monthly, du plus ancien."""
    d = read_sql_df("SELECT ticker, date, close FROM price_monthly "
                    "WHERE close > 0 ORDER BY ticker, date")
    series = defaultdict(list)
    for _, r in d.iterrows():
        jour = r["date"]
        if not isinstance(jour, date):
            jour = pd.Timestamp(jour).date()
        series[r["ticker"]].append((jour, float(r["close"])))
    return dict(series)


def volumes_mensuels() -> dict:
    """{ticker: {(annee, mois): volume}} — le volume echange chaque mois.

    Il vit a part de `series_mensuelles_cours` parce que les cours et les
    volumes n'ont pas la meme fiabilite : 97 % des mois portent un volume,
    3 % non, et une seance sans transaction n'est pas une seance a volume
    nul. Le sous-critere volume doit pouvoir dire « je ne sais pas » plutot
    que de compter zero — c'est la difference entre une absence de donnee et
    un jugement defavorable.
    """
    d = read_sql_df("SELECT ticker, date, volume FROM price_monthly "
                    "WHERE volume > 0 ORDER BY ticker, date")
    par_titre = defaultdict(dict)
    for _, r in d.iterrows():
        jour = r["date"]
        if not isinstance(jour, date):
            jour = pd.Timestamp(jour).date()
        par_titre[r["ticker"]][(jour.year, jour.month)] = float(r["volume"])
    return dict(par_titre)


def rendements_futurs(points: list, i: int) -> dict:
    """Rendement du titre à 1, 3, 6 et 12 mois après le point `i`."""
    sortie = {}
    depart = points[i][1]
    for h in HORIZONS:
        j = i + h
        sortie[h] = (points[j][1] / depart - 1) if j < len(points) and depart else None
    return sortie


class Marche:
    """Le Composite, pour rapporter chaque rendement à celui de la place."""

    def __init__(self, points: list):
        self.par_mois = {(d.year, d.month): c for d, c in points}
        self.suite = points

    def rendement(self, jour: date, horizon: int) -> float | None:
        indices = [k for k, (d, _) in enumerate(self.suite)
                   if (d.year, d.month) == (jour.year, jour.month)]
        if not indices:
            return None
        i = indices[0]
        j = i + horizon
        if j >= len(self.suite) or not self.suite[i][1]:
            return None
        return self.suite[j][1] / self.suite[i][1] - 1


def agreger(observations: list) -> dict:
    """Résume une liste d'observations en mesures lisibles.

    `observations` : liste de dicts {score, jour, rendements, relatifs}.
    """
    par_horizon = {}
    for h in HORIZONS:
        rels = [o["relatifs"][h] for o in observations
                if o["relatifs"].get(h) is not None]
        if len(rels) < MINIMUM_OBSERVATIONS:
            par_horizon[h] = None
            continue
        gagnants = sum(1 for r in rels if r > 0)
        par_horizon[h] = {
            "n": len(rels),
            "moyenne": st.mean(rels),
            "mediane": st.median(rels),
            "part_gagnante": gagnants / len(rels),
            "ecart_type": st.pstdev(rels) if len(rels) > 1 else 0.0,
        }
    return par_horizon


def par_tranche(observations: list, bornes: list, cle: str = "score") -> dict:
    """Ventile les observations par tranche de score, puis agrège chacune.

    C'est le test qui compte : un score n'a de valeur que si les tranches
    hautes battent les tranches basses. Un score qui « marche » en moyenne
    mais dont les tranches ne se classent pas ne prédit rien — il suit.
    """
    lots = defaultdict(list)
    for o in observations:
        v = o.get(cle)
        if v is None:
            continue
        libelle = None
        for bas, haut in zip(bornes, bornes[1:]):
            if bas <= v < haut:
                libelle = f"{bas:g}–{haut:g}"
                break
        if libelle is None and v >= bornes[-1]:
            libelle = f"≥{bornes[-1]:g}"
        if libelle:
            lots[libelle].append(o)
    return {lib: agreger(obs) for lib, obs in sorted(lots.items())}


def par_periode(observations: list, decoupe: int = 5) -> dict:
    """Ventile par tranche d'années, pour voir si un signal s'est éteint."""
    lots = defaultdict(list)
    for o in observations:
        debut = (o["jour"].year // decoupe) * decoupe
        lots[f"{debut}–{debut + decoupe - 1}"].append(o)
    return {lib: agreger(obs) for lib, obs in sorted(lots.items())}


def entete(horizons=HORIZONS) -> str:
    return (f"  {'':14} {'n':>6}  "
            + "  ".join(f"{h:>2}m  med   gagn" for h in horizons))


def ligne(libelle: str, mesures: dict, horizons=HORIZONS) -> str:
    """Une ligne de résultat : MÉDIANE et part gagnante, pas la moyenne.

    La moyenne est trompeuse ici, et le backtest fondamental l'a montré : la
    tranche de score la plus BASSE y affichait +49 % de moyenne à un an tout
    en ne gagnant que 45 % du temps. Quelques titres décuplés tiraient toute
    la tranche. La médiane dit ce qui arrive au titre du milieu ; la part
    gagnante, à quelle fréquence le pari passe. Les deux ensemble décrivent
    la distribution, la moyenne seule la déguise.
    """
    bouts = []
    for h in horizons:
        m = mesures.get(h)
        bouts.append("      —      " if not m else
                     f"{m['mediane']*100:+6.1f} % {m['part_gagnante']*100:3.0f}%")
    n = next((m["n"] for m in mesures.values() if m), 0)
    return f"  {libelle:14} {n:>6}  " + "  ".join(bouts)


def moyennes(mesures: dict, horizons=HORIZONS) -> str:
    """Les moyennes, en second rang — utiles pour voir l'asymétrie."""
    bouts = []
    for h in horizons:
        m = mesures.get(h)
        bouts.append("       —" if not m else f"{m['moyenne']*100:+7.1f} %")
    return f"  {'  (moyennes)':14} {'':>6}  " + "  ".join(f"{b:>13}" for b in bouts)
