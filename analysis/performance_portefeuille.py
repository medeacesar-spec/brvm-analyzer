"""La performance du portefeuille sur des periodes : jour, semaine… maximum.

Demande du 03/10/2026 : l'onglet Performance ne montrait que la performance
depuis le premier achat. On la decline sur dix periodes.

PERFORMANCE PONDEREE PAR LE TEMPS (demande du 06/10/2026). Chaque jour, le
rendement est celui des lignes detenues a la cloture de la VEILLE ; les
rendements quotidiens s'enchainent : (1 + r1)(1 + r2)… - 1. Une periode se lit
entre deux points de cette serie.

UN ACHAT ENTRE AU COURS DE CLOTURE DE SON JOUR. Ni ses frais, ni l'ecart entre
son prix et la cloture ne comptent comme performance : acheter SIBC le 06/10
ne doit pas faire baisser la performance des lignes deja detenues. Ces couts
restent dans le gain sur montant investi (cartes de l'onglet). La version
precedente (03/10) rapportait le gain a l'argent investi : un achat du jour,
frais compris, tirait toute la periode vers le bas.

LA REFERENCE est le BRVM Composite sur les memes jours, a partir de la meme
cloture. Pour une periode plus longue que le portefeuille, les deux se
mesurent depuis le premier achat.

Cours seuls : les dividendes encaisses sont comptes dans le Total Return, pas
ici. Le Composite est lui aussi un indice de prix.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd

from data.db import read_sql_df

INDICE = "BRVMC"
PERIODES = ["Jour", "Semaine", "Mois", "3 mois", "6 mois", "YTD", "1 an",
            "3 ans", "5 ans", "Max"]


def _debut(periode: str, derniere: date, seances: list) -> Optional[date]:
    """Date de cloture qui ouvre la periode."""
    if periode == "Jour":
        avant = [s for s in seances if s < derniere]
        return avant[-1] if avant else None
    if periode == "Semaine":
        return derniere - timedelta(days=7)
    mois = {"Mois": 1, "3 mois": 3, "6 mois": 6, "1 an": 12, "3 ans": 36,
            "5 ans": 60}.get(periode)
    if mois:
        return (pd.Timestamp(derniere) - pd.DateOffset(months=mois)).date()
    if periode == "YTD":
        return date(derniere.year - 1, 12, 31)
    return None                                  # Max : depuis l'achat


def _cours_au(serie: pd.Series, jour: date) -> Optional[float]:
    """Derniere cloture connue a cette date."""
    avant = serie[serie.index <= pd.Timestamp(jour)]
    return float(avant.iloc[-1]) if len(avant) else None


def _quotidien(lignes: list, prix_du_jour: dict, recul: Optional[date] = None):
    """La serie quotidienne commune a la courbe et au tableau.

    Rend (serie, cours) : `serie` indexee par seance depuis le premier achat,
    colonnes `indice` (1 a la cloture du premier jour), `valeur`, `investi` ;
    `cours` : clotures par titre (Composite compris), propagees.
    """
    lignes = [l for l in lignes if (l.get("quantity") or 0) > 0 and l.get("purchase_date")]
    if not lignes:
        return None, None
    achats = [(pd.Timestamp(str(l["purchase_date"])[:10]), l["ticker"], float(l["quantity"]),
               float(l["quantity"]) * float(l.get("avg_price") or 0) + float(l.get("fees") or 0))
              for l in lignes]
    debut = min(a[0] for a in achats)
    lecture = debut - pd.Timedelta(days=15)
    if recul is not None:
        lecture = min(lecture, pd.Timestamp(recul) - pd.Timedelta(days=15))
    tickers = sorted({a[1] for a in achats} | {INDICE})
    cles = ",".join(f"'{t}'" for t in tickers)
    d = read_sql_df(f"SELECT ticker, date, close FROM price_cache WHERE ticker IN ({cles}) "
                    "AND close > 0 AND date >= ?", params=(lecture.date().isoformat(),))
    if d.empty:
        return None, None
    d["date"] = pd.to_datetime(d["date"])
    cours = d.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    if INDICE not in cours:
        return None, None
    cours = cours.ffill()
    # Le dernier jour porte le cours affiche par l'application.
    for t, p in (prix_du_jour or {}).items():
        if t in cours.columns and p:
            cours.loc[cours.index[-1], t] = float(p)

    def _valeur(lots, jour):
        return sum(q * cours.at[jour, t] for _, t, q, _ in lots
                   if t in cours.columns and pd.notna(cours.at[jour, t]))

    jours = list(cours.index[cours.index >= debut])
    rangs, indice, veille = [], 1.0, None
    for j in jours:
        if veille is not None:
            # Les lignes detenues a la cloture de la veille, et elles seules.
            tenus = [a for a in achats if a[0] <= veille and a[1] in cours.columns
                     and pd.notna(cours.at[veille, a[1]]) and pd.notna(cours.at[j, a[1]])]
            v0, v1 = _valeur(tenus, veille), _valeur(tenus, j)
            if v0:
                indice *= v1 / v0
        detenus = [a for a in achats if a[0] <= j]
        if detenus:
            rangs.append((j, indice, _valeur(detenus, j), sum(a[3] for a in detenus)))
            veille = j
    if not rangs:
        return None, None
    serie = pd.DataFrame(rangs, columns=["date", "indice", "valeur", "investi"]).set_index("date")
    serie.attrs["achats"] = achats
    return serie, cours


def performance_par_periode(lignes: list, prix_du_jour: dict) -> Optional[dict]:
    """`lignes` : [{ticker, quantity, avg_price, fees, purchase_date}].

    Rend {periodes: {periode: {portefeuille, indice, ecart, debut,
    depuis_achat, lignes: {ticker: rendement}}}, derniere_seance}."""
    recul = date.today() - timedelta(days=5 * 366 + 40)
    serie, cours = _quotidien(lignes, prix_du_jour, recul=recul)
    if serie is None or len(serie) == 0:
        return None
    achats = serie.attrs["achats"]
    seances = [x.date() for x in cours.index]
    derniere = serie.index[-1].date()
    fin = serie.index[-1]
    premier = serie.index[0]

    sortie = {}
    for periode in PERIODES:
        debut = _debut(periode, derniere, seances)
        if debut is None or pd.Timestamp(debut) < premier:
            base, depuis_achat = premier, periode != "Max"
        else:
            base = serie.index[serie.index <= pd.Timestamp(debut)][-1]
            depuis_achat = False
        rp = float(serie.at[fin, "indice"] / serie.at[base, "indice"] - 1)
        ri = float(cours.at[fin, INDICE] / cours.at[base, INDICE] - 1)
        # Par titre : son cours, depuis la base ou depuis sa premiere cloture
        # en portefeuille si l'achat est posterieur.
        par_ligne = {}
        for t in sorted({a[1] for a in achats}):
            entree = min(a[0] for a in achats if a[1] == t)
            depart = [j for j in serie.index if j >= max(base, entree)]
            if t not in cours.columns or not depart or depart[0] > fin:
                continue
            p0, p1 = cours.at[depart[0], t], cours.at[fin, t]
            if pd.notna(p0) and pd.notna(p1) and p0:
                par_ligne[t] = float(p1 / p0 - 1)
        sortie[periode] = {
            "portefeuille": rp, "indice": ri, "ecart": rp - ri,
            "debut": base.date(), "depuis_achat": depuis_achat,
            "lignes": par_ligne,
        }
    return {"periodes": sortie, "derniere_seance": derniere}


def serie_performance(lignes: list, prix_du_jour: dict) -> Optional[pd.DataFrame]:
    """La performance du portefeuille, jour par jour, en pourcentage.

    Demande du 03/10/2026 : une courbe en %, avec 6 mois, 1 an, 5 ans, max.
    Meme serie que le tableau par periode (voir `_quotidien`) : rendements
    quotidiens enchaines sur les lignes detenues la veille ; un achat entre
    au cours de cloture de son jour. Le Composite part de la meme cloture.
    """
    serie, cours = _quotidien(lignes, prix_du_jour)
    if serie is None or len(serie) == 0:
        return None
    sortie = pd.DataFrame(index=serie.index)
    sortie["portefeuille"] = serie["indice"] - 1
    sortie["valeur"] = serie["valeur"]
    sortie["investi"] = serie["investi"]
    composite = cours.loc[serie.index, INDICE]
    sortie["indice"] = composite / composite.iloc[0] - 1
    return sortie
