#!/usr/bin/env python3
"""Quels groupes de titres battent le Composite ? (docs/chantiers.md, § 4)

LE BANC. Chaque 1er janvier, une regle choisit des titres avec ce qui etait
connu au 31 decembre ; on les detient toute l'annee, a poids egaux, et on
compare au BRVM Composite. Aucune regle n'est ajustee sur les resultats : les
seuils (un cinquieme de la cote, trois ans d'historique) sont fixes d'avance.

CE QUI EST MESURE, ET CE QUI NE L'EST PAS

- Les COURS, ajustes des divisions, SANS les dividendes : le Composite est
  lui aussi un indice de prix. Comparer des prix a des prix est juste ; un
  groupe a fort rendement est desavantage, et il faudra le dire.
- Les survivants, plus Movis (radie en 2025). Les societes radiees avant 2016
  manquent (docs/recherche_donnees_cours.md) : les groupes de petits titres
  peu echanges en sont flattes.
- Ni frais ni impot. Une rotation annuelle coute de l'ordre de 1 a 2 % par an
  a la BRVM : un ecart plus petit que cela ne se gagne pas.
- Un titre suspendu garde son dernier cours (rendement nul) ; Movis, dont le
  capital a ete annule en 2019, compte pour -100 % cette annee-la.

Usage :
    python3 scripts/recherche_groupes.py            # tableaux a l'ecran
    python3 scripts/recherche_groupes.py --markdown  # tableaux pour la note
"""
from __future__ import annotations

import argparse
import math
import os
import statistics as st
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_tickers                  # noqa: E402
from data.db import read_sql_df                  # noqa: E402

INDICE = "BRVMC"
PREMIERE, DERNIERE = 1999, 2025       # annees completes de detention
PART = 0.2                            # un cinquieme de la cote
# Capital reduit a zero par annulation des actions (avis 125-2019 pour
# Unilever, 070-2019 pour Movis) : l'actionnaire d'avant l'operation perd sa
# mise. Unilever a repris sa cotation en 2020 ; le cours d'apres n'est pas
# celui des memes actions, la serie ne se lit pas a travers.
EVENEMENTS = {("SVOC.ci", 2019): -1.0}
HORIZONS = [("5 ans", 2021), ("10 ans", 2016), ("20 ans", 2006), ("27 ans", 1999)]
COUPURE = 2012                        # deux moities : 1999-2011 et 2012-2025


def charger():
    m = read_sql_df("SELECT ticker, date, close, volume FROM price_monthly WHERE close > 0")
    m["date"] = pd.to_datetime(m["date"])
    m["mois"] = m["date"].dt.to_period("M")
    m = m.sort_values(["ticker", "date"]).drop_duplicates(["ticker", "mois"], keep="last")
    cours = m.pivot(index="mois", columns="ticker", values="close")
    volume = m.pivot(index="mois", columns="ticker", values="volume")
    # Un mois sans ligne garde le dernier cours, a l'interieur de la vie du
    # titre seulement : ni avant sa premiere cotation, ni apres la derniere.
    tous = pd.period_range(cours.index.min(), cours.index.max(), freq="M")
    cours, volume = cours.reindex(tous), volume.reindex(tous)
    dernier = cours.apply(pd.Series.last_valid_index)
    cours = cours.ffill()
    for t in cours.columns:
        cours.loc[dernier[t] + 1:, t] = None
    secteurs = {t["ticker"]: t.get("sector") or "Autres" for t in load_tickers(inclure_retires=True)}
    a = read_sql_df("SELECT ticker, fiscal_year, shares FROM fundamentals "
                    "WHERE shares IS NOT NULL AND shares > 0")
    actions = a.sort_values("fiscal_year").groupby("ticker")["shares"].last().to_dict()
    return cours, volume.fillna(0), secteurs, actions


def point(cours, p):
    return cours.loc[p] if p in cours.index else pd.Series(dtype=float)


def fin_annee(cours, an):
    return point(cours, pd.Period(f"{an}-12", "M"))


def rendement_12_mois(cours, p0):
    """Rendement de chaque titre coté a la fin du mois p0, sur les douze mois
    suivants. p0 = decembre de l'annee N-1 : l'annee civile N."""
    debut, fin = point(cours, p0), point(cours, p0 + 12)
    r = {}
    for t in debut.dropna().index:
        if t == INDICE or "." not in t:
            continue
        evenement = next((v for (tk, an), v in EVENEMENTS.items()
                          if tk == t and p0 < pd.Period(f"{an}-12", "M") <= p0 + 12), None)
        if evenement is not None:
            r[t] = evenement
        elif pd.notna(fin.get(t)):
            r[t] = fin[t] / debut[t] - 1
    return r


def rendement_annuel(cours, an):
    return rendement_12_mois(cours, pd.Period(f"{an - 1}-12", "M"))


def regles(cours, volume, secteurs, actions):
    """{nom: fonction(p0, univers) -> liste de titres}. p0 : le dernier mois
    clos a la decision ; l'univers : les titres cotes ce jour-la."""
    def perf(p0, annees):
        a, b = point(cours, p0 - 12 * annees), point(cours, p0)
        return {t: b[t] / a[t] - 1 for t in b.dropna().index
                if "." in t and pd.notna(a.get(t)) and a[t] > 0}

    def vol(p0):
        fen = cours.loc[p0 - 35:p0].pct_change(fill_method=None).iloc[1:]
        return {t: fen[t].dropna().std() for t in fen.columns
                if "." in t and fen[t].notna().sum() >= 24}

    def echange(p0):
        c, v = cours.loc[p0 - 11:p0], volume.loc[p0 - 11:p0]
        return {t: float((c[t] * v[t]).sum()) for t in c.columns
                if "." in t and c[t].notna().sum() >= 6}

    def capitalisation(p0):
        """Cours ajuste x nombre d'actions d'aujourd'hui. Le cours ajuste
        ramene les divisions et attributions gratuites au nombre d'actions
        actuel : le produit est la capitalisation d'alors, TANT QUE le capital
        n'a change que par division. Une augmentation de capital en numeraire
        la gonfle pour les annees d'avant. Reconstitution, pas mesure."""
        b = point(cours, p0)
        return {t: float(b[t] * actions[t]) for t in b.dropna().index
                if t in actions}

    def haut(d, univers, part=PART, n=None):
        d = {t: v for t, v in d.items() if t in univers and not math.isnan(v)}
        k = n or max(1, round(len(d) * part))
        return sorted(d, key=lambda t: (-d[t], t))[:k]

    def bas(d, univers, part=PART, n=None):
        return haut({t: -v for t, v in d.items()}, univers, part, n)

    r = {
        "Toute la cote, poids égaux": lambda an, u: sorted(u),
        "Capitalisation : le cinquième le plus gros": lambda an, u: haut(capitalisation(an), u),
        "Capitalisation : le cinquième le plus petit": lambda an, u: bas(capitalisation(an), u),
        "La plus grosse capitalisation (1 titre)": lambda an, u: haut(capitalisation(an), u, n=1),
        "La plus petite capitalisation (1 titre)": lambda an, u: bas(capitalisation(an), u, n=1),
        "Momentum 1 an : le cinquième le plus fort": lambda an, u: haut(perf(an, 1), u),
        "Momentum 1 an : le cinquième le plus faible": lambda an, u: bas(perf(an, 1), u),
        "Le plus fort de l'an dernier (1 titre)": lambda an, u: haut(perf(an, 1), u, n=1),
        "Le plus faible de l'an dernier (1 titre)": lambda an, u: bas(perf(an, 1), u, n=1),
        "Momentum 3 ans : le cinquième le plus fort": lambda an, u: haut(perf(an, 3), u),
        "Momentum 3 ans : le cinquième le plus faible": lambda an, u: bas(perf(an, 3), u),
        "Faible volatilité : le cinquième le plus calme": lambda an, u: bas(vol(an), u),
        "Forte volatilité : le cinquième le plus agité": lambda an, u: haut(vol(an), u),
        "Liquidité : le cinquième le plus échangé": lambda an, u: haut(echange(an), u),
        "Liquidité : le cinquième le moins échangé": lambda an, u: bas(echange(an), u),
        "Le plus échangé (1 titre)": lambda an, u: haut(echange(an), u, n=1),
    }
    for sect in sorted(set(secteurs.values())):
        r[f"Secteur : {sect}"] = (lambda s: lambda an, u: sorted(t for t in u if secteurs.get(t) == s))(sect)
    return r


def _liquides(cours, volume, p0, univers):
    """La moitie de la cote la plus echangee sur les douze mois ecoules : les
    titres qu'on aurait reellement pu acheter a la date de decision."""
    c, v = cours.loc[p0 - 11:p0], volume.loc[p0 - 11:p0]
    montants = {t: float((c[t] * v[t]).sum()) for t in univers if t in c.columns}
    rang = sorted(montants, key=lambda t: -montants[t])
    return set(rang[:max(1, len(rang) // 2)])


_DONNEES = {}


def donnees():
    if not _DONNEES:
        _DONNEES["v"] = charger()
    return _DONNEES["v"]


def banc(variante="moyenne", mois=1):
    """`variante` : « moyenne » (poids egaux, toute la cote), « mediane »
    (le titre median du groupe, insensible a un titre multiplie par dix),
    « liquides » (poids egaux, parmi la moitie la plus echangee).

    `mois` : le mois ou l'on achete (1 = janvier). Les resultats sont ranges
    par l'annee ou la detention COMMENCE : l'annee 2010 d'un depart en juillet
    va de juillet 2010 a juin 2011."""
    cours, volume, secteurs, actions = donnees()
    decisions = {an: pd.Period(f"{an}-{mois:02d}", "M") - 1
                 for an in range(PREMIERE, DERNIERE + 1)}
    decisions = {an: p0 for an, p0 in decisions.items() if p0 + 12 <= cours.index.max()}
    indice = {an: point(cours, p0 + 12)[INDICE] / point(cours, p0)[INDICE] - 1
              for an, p0 in decisions.items()}
    par_an = {an: rendement_12_mois(cours, p0) for an, p0 in decisions.items()}
    resultats = {}
    for nom, regle in regles(cours, volume, secteurs, actions).items():
        annees = {}
        for an, r in par_an.items():
            univers = set(r)
            if variante == "liquides":
                univers = _liquides(cours, volume, decisions[an], univers)
            choix = [t for t in regle(decisions[an], univers) if t in r]
            if choix:
                agreger = st.median if variante == "mediane" else st.mean
                annees[an] = (agreger(r[t] for t in choix), len(choix), choix)
        resultats[nom] = annees
    return resultats, indice


def _annualise(rendements):
    v = 1.0
    for x in rendements:
        v *= 1 + x
    return v ** (1 / len(rendements)) - 1 if rendements else None


def mesures(annees, indice, depuis):
    ans = [a for a in sorted(annees) if a >= depuis]
    if len(ans) < 3:
        return None
    g = [annees[a][0] for a in ans]
    i = [indice[a] for a in ans]
    ecarts = [x - y for x, y in zip(g, i)]
    return {"groupe": _annualise(g), "indice": _annualise(i),
            "ecart": _annualise(g) - _annualise(i),
            "bat": sum(e > 0 for e in ecarts) / len(ecarts),
            "mediane": st.median(ecarts), "annees": len(ans),
            "titres": st.median(annees[a][1] for a in ans)}


def stabilite(annees, indice):
    """L'ecart annualise sur chaque moitie ; stable si meme signe."""
    moities = []
    for a0, a1 in ((PREMIERE, COUPURE - 1), (COUPURE, DERNIERE)):
        ans = [a for a in annees if a0 <= a <= a1]
        if len(ans) < 5:
            return None, None, None
        moities.append(_annualise([annees[a][0] for a in ans]) - _annualise([indice[a] for a in ans]))
    return moities[0], moities[1], (moities[0] > 0) == (moities[1] > 0)


REGLES_MOIS = [
    "Toute la cote, poids égaux",
    "Momentum 1 an : le cinquième le plus fort",
    "Capitalisation : le cinquième le plus gros",
    "Capitalisation : le cinquième le plus petit",
    "Secteur : Banque",
]
NOMS_MOIS = ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août",
             "sept.", "oct.", "nov.", "déc."]


def saisonnalite():
    """Le mois civil compte-t-il ? Rendement du Composite et du titre median,
    mois par mois, 1999-2025. Indice de prix : le detachement des dividendes
    (mai a juillet) y pese sans que l'actionnaire ait rien perdu."""
    cours = donnees()[0]
    r = cours.pct_change(fill_method=None)
    r = r[(r.index.year >= PREMIERE) & (r.index.year <= DERNIERE)]
    titres = [t for t in r.columns if "." in t]
    lignes = []
    for m in range(1, 13):
        sel = r[r.index.month == m]
        ind = sel[INDICE].dropna()
        med = sel[titres].median(axis=1).dropna()
        lignes.append((NOMS_MOIS[m - 1], ind.mean(), ind.median(), (ind > 0).mean(),
                       med.mean(), len(ind)))
    return lignes


def main(markdown=False):
    pct = lambda v: "—" if v is None else f"{v * 100:+.1f}".replace(".", ",")
    for variante in ("moyenne", "mediane", "liquides"):
        resultats, indice = banc(variante)
        print(f"\n### Achat en janvier — variante : {variante}\n")
        print("| Groupe | " + " | ".join(f"Écart {h}" for h, _ in HORIZONS)
              + " | Années battues | 1999-2011 | 2012-2025 | Stable | Titres |")
        print("|" + "---|" * (len(HORIZONS) + 6))
        for nom, annees in resultats.items():
            cols = [mesures(annees, indice, d) for _, d in HORIZONS]
            tout = cols[-1]
            if not tout or tout["titres"] < 3 and "1 titre" not in nom:
                continue          # un « groupe » d'un ou deux titres n'en est pas un
            m1, m2, stable = stabilite(annees, indice)
            print(f"| {nom} | " + " | ".join(pct(c["ecart"]) if c else "—" for c in cols)
                  + f" | {tout['bat']:.0%} | {pct(m1)} | {pct(m2)} | "
                  + f"{'—' if stable is None else ('oui' if stable else 'non')} | "
                  + f"{int(tout['titres'])} |")

    print("\n### Le mois d'achat change-t-il le résultat ? (écart annualisé, 1999-2024)\n")
    for variante in ("moyenne", "mediane", "liquides"):
        tab = {}
        for m in range(1, 13):
            resultats, indice = banc(variante, m)
            for nom in REGLES_MOIS:
                a = {an: v for an, v in resultats[nom].items() if an <= DERNIERE - 1}
                mm = mesures(a, indice, PREMIERE)
                tab.setdefault(nom, []).append(mm["ecart"] if mm else None)
        print(f"\nVariante {variante}\n")
        print("| Groupe | " + " | ".join(NOMS_MOIS) + " | Écart min | Écart max |")
        print("|" + "---|" * 15)
        for nom, v in tab.items():
            ok = [x for x in v if x is not None]
            print(f"| {nom} | " + " | ".join(pct(x) for x in v)
                  + f" | {pct(min(ok))} | {pct(max(ok))} |")

    print("\n### Saisonnalité du Composite (1999-2025)\n")
    print("| Mois | Moyenne | Médiane | Mois en hausse | Titre médian (moyenne) | Années |")
    print("|---|---|---|---|---|---|")
    for nom, moy, med, hausse, titre, n in saisonnalite():
        print(f"| {nom} | {pct(moy)} | {pct(med)} | {hausse:.0%} | {pct(titre)} | {n} |")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--markdown", action="store_true")
    main(ap.parse_args().markdown)
