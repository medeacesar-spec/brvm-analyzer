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
    return cours, volume.fillna(0), secteurs


def fin_annee(cours, an):
    cle = pd.Period(f"{an}-12", "M")
    return cours.loc[cle] if cle in cours.index else pd.Series(dtype=float)


def rendement_annuel(cours, an):
    """Rendement de chaque titre coté au 31/12/an-1, sur l'annee `an`."""
    debut, fin = fin_annee(cours, an - 1), fin_annee(cours, an)
    r = {}
    for t in debut.dropna().index:
        if t == INDICE or "." not in t:
            continue
        if (t, an) in EVENEMENTS:
            r[t] = EVENEMENTS[(t, an)]
        elif pd.notna(fin.get(t)):
            r[t] = fin[t] / debut[t] - 1
    return r


def regles(cours, volume, secteurs):
    """{nom: fonction(an, univers) -> liste de titres}. L'univers : les titres
    cotes au 31/12/an-1, avec ce qu'on en savait ce jour-la."""
    def perf(an, annees):
        a, b = fin_annee(cours, an - 1 - annees), fin_annee(cours, an - 1)
        return {t: b[t] / a[t] - 1 for t in b.dropna().index
                if "." in t and pd.notna(a.get(t)) and a[t] > 0}

    def vol(an):
        fin = pd.Period(f"{an - 1}-12", "M")
        fen = cours.loc[fin - 35:fin].pct_change(fill_method=None).iloc[1:]
        return {t: fen[t].dropna().std() for t in fen.columns
                if "." in t and fen[t].notna().sum() >= 24}

    def echange(an):
        fin = pd.Period(f"{an - 1}-12", "M")
        c, v = cours.loc[fin - 11:fin], volume.loc[fin - 11:fin]
        return {t: float((c[t] * v[t]).sum()) for t in c.columns
                if "." in t and c[t].notna().sum() >= 6}

    def haut(d, univers, part=PART, n=None):
        d = {t: v for t, v in d.items() if t in univers and not math.isnan(v)}
        k = n or max(1, round(len(d) * part))
        return sorted(d, key=lambda t: (-d[t], t))[:k]

    def bas(d, univers, part=PART, n=None):
        return haut({t: -v for t, v in d.items()}, univers, part, n)

    r = {
        "Toute la cote, poids égaux": lambda an, u: sorted(u),
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


def _liquides(cours, volume, an, univers):
    """La moitie de la cote la plus echangee sur l'annee ecoulee : les titres
    qu'on aurait reellement pu acheter en debut d'annee."""
    fin = pd.Period(f"{an - 1}-12", "M")
    c, v = cours.loc[fin - 11:fin], volume.loc[fin - 11:fin]
    montants = {t: float((c[t] * v[t]).sum()) for t in univers if t in c.columns}
    rang = sorted(montants, key=lambda t: -montants[t])
    return set(rang[:max(1, len(rang) // 2)])


def banc(variante="moyenne"):
    """`variante` : « moyenne » (poids egaux, toute la cote), « mediane »
    (le titre median du groupe, insensible a un titre multiplie par dix),
    « liquides » (poids egaux, parmi la moitie la plus echangee)."""
    cours, volume, secteurs = charger()
    indice = {an: fin_annee(cours, an)[INDICE] / fin_annee(cours, an - 1)[INDICE] - 1
              for an in range(PREMIERE, DERNIERE + 1)}
    resultats = {}
    par_an = {an: rendement_annuel(cours, an) for an in range(PREMIERE, DERNIERE + 1)}
    for nom, regle in regles(cours, volume, secteurs).items():
        annees = {}
        for an, r in par_an.items():
            univers = set(r)
            if variante == "liquides":
                univers = _liquides(cours, volume, an, univers)
            choix = [t for t in regle(an, univers) if t in r]
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


def main(markdown=False):
    pct = lambda v: "—" if v is None else f"{v * 100:+.1f}".replace(".", ",")
    for variante in ("moyenne", "mediane", "liquides"):
        resultats, indice = banc(variante)
        print(f"\n### Variante : {variante}\n")
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
    print("\nComposite, par an :", {a: round(v * 100, 1) for a, v in indice.items()})


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--markdown", action="store_true")
    main(ap.parse_args().markdown)
