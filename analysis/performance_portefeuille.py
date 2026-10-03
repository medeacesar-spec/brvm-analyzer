"""La performance du portefeuille sur des periodes : jour, semaine… maximum.

Demande du 03/10/2026 : l'onglet Performance ne montrait que la performance
depuis le premier achat. On la decline sur dix periodes.

LA REGLE, ligne par ligne (un lot achete a une date) :

- achetee AVANT le debut de la periode : elle part du cours de cloture a
  cette date ;
- achetee PENDANT la periode : elle part de son cout de revient (prix
  d'achat et frais). C'est ce qui a ete reellement gagne ou perdu ;
- elle arrive au cours du jour.

LA REFERENCE est le BRVM Composite, mais pas « le Composite sur la periode » :
un portefeuille ouvert en mars compare a cinq ans d'indice ne dirait rien. On
place les MEMES montants aux MEMES dates dans l'indice, ligne par ligne. L'ecart
mesure alors le choix des titres, rien d'autre.

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


def performance_par_periode(lignes: list, prix_du_jour: dict) -> Optional[dict]:
    """`lignes` : [{ticker, quantity, avg_price, fees, purchase_date}].

    Rend {periode: {portefeuille, indice, ecart, depuis_achat, lignes:
    {ticker: rendement}}} et la date de la derniere seance."""
    lignes = [l for l in lignes if (l.get("quantity") or 0) > 0]
    if not lignes:
        return None
    tickers = sorted({l["ticker"] for l in lignes} | {INDICE})
    debut_lecture = (date.today() - timedelta(days=5 * 366 + 40)).isoformat()
    premier_achat = min(str(l.get("purchase_date") or date.today())[:10] for l in lignes)
    debut_lecture = min(debut_lecture, premier_achat)
    cles = ",".join(f"'{t}'" for t in tickers)
    d = read_sql_df(f"SELECT ticker, date, close FROM price_cache WHERE ticker IN ({cles}) "
                    "AND close > 0 AND date >= ?", params=(debut_lecture,))
    if d.empty:
        return None
    d["date"] = pd.to_datetime(d["date"])
    series = {t: g.set_index("date")["close"].sort_index() for t, g in d.groupby("ticker")}
    indice = series.get(INDICE)
    if indice is None or indice.empty:
        return None
    seances = [x.date() for x in indice.index]
    derniere = seances[-1]
    indice_fin = float(indice.iloc[-1])

    sortie = {}
    for periode in PERIODES:
        debut = _debut(periode, derniere, seances)
        depart = arrivee = depart_idx = arrivee_idx = 0.0
        par_ligne = {}
        tous_apres = True
        for l in lignes:
            t, q = l["ticker"], float(l["quantity"])
            achat = pd.to_datetime(str(l.get("purchase_date") or derniere)[:10]).date()
            cout = q * float(l.get("avg_price") or 0) + float(l.get("fees") or 0)
            fin = prix_du_jour.get(t)
            if fin is None and t in series:
                fin = float(series[t].iloc[-1])
            if fin is None or not cout:
                continue
            if debut is not None and achat <= debut:
                prix = _cours_au(series.get(t, pd.Series(dtype=float)), debut)
                if prix is None:
                    continue
                v0, jour0 = q * prix, debut
                tous_apres = False
            else:
                v0, jour0 = cout, achat
            v1 = q * fin
            ind0 = _cours_au(indice, jour0)
            if not ind0:
                continue
            depart += v0
            arrivee += v1
            depart_idx += v0
            arrivee_idx += v0 * indice_fin / ind0
            a = par_ligne.setdefault(t, [0.0, 0.0])
            a[0] += v0
            a[1] += v1
        if not depart:
            continue
        rp = arrivee / depart - 1
        ri = arrivee_idx / depart_idx - 1
        sortie[periode] = {
            "portefeuille": rp, "indice": ri, "ecart": rp - ri,
            "debut": debut,
            # Toutes les lignes achetees pendant la periode : la mesure est
            # celle de « depuis l'achat », et doit le dire.
            "depuis_achat": tous_apres and periode != "Max",
            "lignes": {t: v[1] / v[0] - 1 for t, v in par_ligne.items() if v[0]},
        }
    return {"periodes": sortie, "derniere_seance": derniere}


def serie_performance(lignes: list, prix_du_jour: dict) -> Optional[pd.DataFrame]:
    """La performance du portefeuille, jour par jour, en pourcentage.

    Demande du 03/10/2026 : une courbe en %, avec 6 mois, 1 an, 5 ans, max.

    PERFORMANCE PONDEREE PAR LE TEMPS. Chaque jour, le rendement est celui des
    lignes detenues la veille ; un achat du jour entre a son cout de revient
    et ne cree aucun saut. Sans cela, chaque versement ferait monter la
    courbe comme un gain. Les rendements quotidiens s'enchainent :
    (1 + r1)(1 + r2)… - 1. Le Composite, sur les memes jours, sert de repere.

    C'est pourquoi la courbe peut differer un peu du tableau par periode, qui
    rapporte le gain a l'argent reellement investi : un achat fait juste avant
    une hausse pese davantage dans le second que dans la premiere.
    """
    lignes = [l for l in lignes if (l.get("quantity") or 0) > 0]
    if not lignes:
        return None
    achats = [(pd.Timestamp(str(l.get("purchase_date"))[:10]), l["ticker"], float(l["quantity"]),
               float(l["quantity"]) * float(l.get("avg_price") or 0) + float(l.get("fees") or 0))
              for l in lignes if l.get("purchase_date")]
    if not achats:
        return None
    debut = min(a[0] for a in achats)
    tickers = sorted({a[1] for a in achats} | {INDICE})
    cles = ",".join(f"'{t}'" for t in tickers)
    d = read_sql_df(f"SELECT ticker, date, close FROM price_cache WHERE ticker IN ({cles}) "
                    "AND close > 0 AND date >= ?", params=((debut - pd.Timedelta(days=15)).date().isoformat(),))
    if d.empty:
        return None
    d["date"] = pd.to_datetime(d["date"])
    cours = d.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    if INDICE not in cours:
        return None
    jours = cours.index[cours.index >= debut]
    cours = cours.ffill()
    # Le dernier jour porte le cours affiche par l'application.
    for t, p in prix_du_jour.items():
        if t in cours.columns and p:
            cours.loc[cours.index[-1], t] = float(p)
    lignes_jour = []
    perf, prec = 1.0, None
    for j in jours:
        detenus = [a for a in achats if a[0] <= j]
        valeur = sum(q * cours.at[j, t] for _, t, q, _ in detenus if pd.notna(cours.at[j, t]))
        flux = sum(c for a, _, _, c in detenus if a.normalize() == j.normalize()) \
            + sum(c for a, _, _, c in detenus if prec is None and a < j)
        if prec is None:
            r = valeur / flux - 1 if flux else 0.0
        else:
            r = (valeur - flux) / prec - 1 if prec else 0.0
        perf *= 1 + r
        prec = valeur
        lignes_jour.append((j, perf - 1, valeur, sum(c for _, _, _, c in detenus)))
    sortie = pd.DataFrame(lignes_jour, columns=["date", "portefeuille", "valeur", "investi"]).set_index("date")
    indice = cours.loc[sortie.index, INDICE]
    sortie["indice"] = indice / indice.iloc[0] - 1
    # Le Composite part du cours de la veille du premier achat, comme le
    # portefeuille part de son cout.
    avant = cours.loc[cours.index < sortie.index[0], INDICE]
    if len(avant):
        sortie["indice"] = indice / avant.iloc[-1] - 1
    return sortie
