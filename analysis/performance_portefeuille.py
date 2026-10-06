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

LES DIVIDENDES COMPTENT (demande du 06/10/2026 : « harmoniser », une seule
performance, ponderee par le temps). Le dividende net encaisse s'ajoute au
rendement de son jour de paiement, pour la ligne qui l'a verse. La carte
Total Return, la courbe et le tableau lisent la meme serie.

LA REFERENCE est le Composite Total Return quand sa serie couvre la periode,
le Composite de cours sinon ; sur les memes jours, depuis la meme cloture.
Pour une periode plus longue que le portefeuille, les deux se mesurent depuis
le premier achat.

Cours seuls : les dividendes encaisses sont comptes dans le Total Return, pas
ici. Le Composite est lui aussi un indice de prix.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd

from data.db import read_sql_df

INDICE = "BRVMC"
# Le Composite TOTAL RETURN (dividendes compris) est la bonne reference d'un
# portefeuille dont la performance compte les dividendes. Sa serie ne
# s'accumule que depuis le 06/10/2026 : tant qu'elle ne couvre pas le debut
# d'une periode, la reference reste le Composite de cours, et l'ecran le dit.
INDICE_TR = "BRVMC_TR"
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


def _dividendes(lignes: list) -> list:
    """[(date de paiement, ticker, montant net)] des utilisateurs des lignes."""
    users = sorted({str(l.get("user_id")) for l in lignes
                    if l.get("user_id") not in (None, "", "None", "nan")})
    if not users:
        return []
    cles = ",".join(f"'{u}'" for u in users if "'" not in u)
    try:
        d = read_sql_df(f"SELECT payment_date, ticker, net_amount FROM dividends "
                        f"WHERE user_id IN ({cles}) AND payment_date IS NOT NULL "
                        "AND net_amount > 0")
    except Exception:                           # noqa: BLE001
        return []
    return [(pd.Timestamp(str(r.payment_date)[:10]), r.ticker, float(r.net_amount))
            for r in d.itertuples()]


def _quotidien(lignes: list, prix_du_jour: dict, recul: Optional[date] = None):
    """Memoise cinq minutes : la carte, la courbe et le tableau lisent la meme
    serie dans la meme page."""
    cle = tuple(sorted(tuple(sorted((k, str(v)) for k, v in l.items()
                                    if k in ("ticker", "quantity", "avg_price", "fees",
                                             "purchase_date", "user_id")))
                       for l in lignes))
    prix = tuple(sorted((str(k), float(v)) for k, v in (prix_du_jour or {}).items() if v))
    return _quotidien_memo(cle, prix, recul)


def _quotidien_memo_brut(cle: tuple, prix: tuple, recul):
    return _quotidien_calcul([dict(x) for x in cle], dict(prix), recul)


try:
    from data.storage import _maybe_cache_data
    _quotidien_memo = _maybe_cache_data(ttl=300)(_quotidien_memo_brut)
except Exception:                               # noqa: BLE001
    _quotidien_memo = _quotidien_memo_brut


def _quotidien_calcul(lignes: list, prix_du_jour: dict, recul: Optional[date] = None):
    """La serie quotidienne commune a la carte, a la courbe et au tableau.

    Rend (serie, cours) : `serie` indexee par seance depuis le premier achat,
    colonnes `indice` (1 a la cloture du premier jour, dividendes compris),
    `valeur`, `investi`, et une colonne `t:<ticker>` par titre (meme calcul,
    titre seul) ; `cours` : clotures propagees, references comprises.
    """
    def _n(v):
        try:
            x = float(v)
            return 0.0 if x != x else x
        except (TypeError, ValueError):
            return 0.0
    lignes = [l for l in lignes if _n(l.get("quantity")) > 0
              and l.get("purchase_date") not in (None, "", "None", "NaT", "nan")]
    if not lignes:
        return None, None
    achats = [(pd.Timestamp(str(l["purchase_date"])[:10]), l["ticker"], _n(l["quantity"]),
               _n(l["quantity"]) * _n(l.get("avg_price")) + _n(l.get("fees")))
              for l in lignes]
    debut = min(a[0] for a in achats)
    lecture = debut - pd.Timedelta(days=15)
    if recul is not None:
        lecture = min(lecture, pd.Timestamp(recul) - pd.Timedelta(days=15))
    tickers = sorted({a[1] for a in achats} | {INDICE, INDICE_TR})
    cles = ",".join(f"'{t}'" for t in tickers)
    d = read_sql_df(f"SELECT ticker, date, close FROM price_cache WHERE ticker IN ({cles}) "
                    "AND close > 0 AND date >= ?", params=(lecture.date().isoformat(),))
    if d.empty:
        return None, None
    d["date"] = pd.to_datetime(d["date"])
    brut = d.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    if INDICE not in brut:
        return None, None
    cours = brut.ffill()
    # Le Composite TR ne se propage pas avant sa premiere valeur connue.
    if INDICE_TR in brut:
        premier_tr = brut[INDICE_TR].first_valid_index()
        cours.loc[cours.index < premier_tr, INDICE_TR] = float("nan")
    # Le dernier jour porte le cours affiche par l'application.
    for t, p in (prix_du_jour or {}).items():
        if t in cours.columns and p:
            cours.loc[cours.index[-1], t] = float(p)
    versements = _dividendes(lignes)

    def _chaine(lots):
        """Indice quotidien d'un ensemble de lots : lignes detenues la veille,
        dividendes du jour compris, jours enchaines."""
        sortie, indice, veille = [], 1.0, None
        for j in jours:
            if veille is not None:
                tenus = [a for a in lots if a[0] <= veille
                         and pd.notna(cours.at[veille, a[1]]) and pd.notna(cours.at[j, a[1]])]
                v0 = sum(q * cours.at[veille, t] for _, t, q, _ in tenus)
                v1 = sum(q * cours.at[j, t] for _, t, q, _ in tenus)
                titres = {a[1] for a in tenus}
                div = sum(m for dj, t, m in versements
                          if t in titres and veille < dj <= j)
                if v0:
                    indice *= (v1 + div) / v0
            if any(a[0] <= j for a in lots):
                sortie.append((j, indice))
                veille = j
            else:
                sortie.append((j, None))
        return sortie

    achats = [a for a in achats if a[1] in cours.columns]
    if not achats:
        return None, None
    jours = list(cours.index[cours.index >= debut])
    global_ = _chaine(achats)
    rangs = []
    for (j, ind) in global_:
        detenus = [a for a in achats if a[0] <= j]
        if ind is None or not detenus:
            continue
        rangs.append((j, ind, sum(q * cours.at[j, t] for _, t, q, _ in detenus
                                  if pd.notna(cours.at[j, t])),
                      sum(a[3] for a in detenus)))
    if not rangs:
        return None, None
    serie = pd.DataFrame(rangs, columns=["date", "indice", "valeur", "investi"]).set_index("date")
    for t in sorted({a[1] for a in achats}):
        s = dict(_chaine([a for a in achats if a[1] == t]))
        serie[f"t:{t}"] = [s.get(j) for j in serie.index]
    serie.attrs["achats"] = achats
    return serie, cours


def _reference(cours: pd.DataFrame, base, fin) -> tuple:
    """(rendement, code) : Composite TR s'il couvre la base, Composite sinon."""
    if INDICE_TR in cours.columns and pd.notna(cours.at[base, INDICE_TR]) \
            and pd.notna(cours.at[fin, INDICE_TR]):
        return float(cours.at[fin, INDICE_TR] / cours.at[base, INDICE_TR] - 1), INDICE_TR
    return float(cours.at[fin, INDICE] / cours.at[base, INDICE] - 1), INDICE


def performance_par_periode(lignes: list, prix_du_jour: dict) -> Optional[dict]:
    """`lignes` : [{ticker, quantity, avg_price, fees, purchase_date, user_id}].

    Rend {periodes: {periode: {portefeuille, indice, reference, ecart, debut,
    depuis_achat, lignes: {ticker: rendement}}}, derniere_seance}."""
    recul = date.today() - timedelta(days=5 * 366 + 40)
    serie, cours = _quotidien(lignes, prix_du_jour, recul=recul)
    if serie is None or len(serie) == 0:
        return None
    seances = [x.date() for x in cours.index]
    derniere = serie.index[-1].date()
    fin = serie.index[-1]
    premier = serie.index[0]
    titres = [c[2:] for c in serie.columns if c.startswith("t:")]

    sortie = {}
    for periode in PERIODES:
        debut = _debut(periode, derniere, seances)
        if debut is None or pd.Timestamp(debut) < premier:
            base, depuis_achat = premier, periode != "Max"
        else:
            base = serie.index[serie.index <= pd.Timestamp(debut)][-1]
            depuis_achat = False
        rp = float(serie.at[fin, "indice"] / serie.at[base, "indice"] - 1)
        ri, ref = _reference(cours, base, fin)
        par_ligne = {}
        for t in titres:
            s = serie[f"t:{t}"]
            depart = s[(s.index >= base) & s.notna()]
            if len(depart) and pd.notna(s.at[fin]) and depart.iloc[0]:
                par_ligne[t] = float(s.at[fin] / depart.iloc[0] - 1)
        sortie[periode] = {
            "portefeuille": rp, "indice": ri, "reference": ref, "ecart": rp - ri,
            "debut": base.date(), "depuis_achat": depuis_achat,
            "lignes": par_ligne,
        }
    return {"periodes": sortie, "derniere_seance": derniere}


def serie_performance(lignes: list, prix_du_jour: dict) -> Optional[pd.DataFrame]:
    """La performance du portefeuille, jour par jour, en pourcentage.

    Meme serie que la carte et le tableau (voir `_quotidien`). Colonnes :
    portefeuille, valeur, investi, indice (Composite de cours, depuis la
    meme cloture) et indice_tr (Composite Total Return, la ou il existe).
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
    if INDICE_TR in cours.columns:
        tr = cours.loc[serie.index, INDICE_TR]
        if tr.notna().any():
            sortie["indice_tr"] = tr / tr.dropna().iloc[0] - 1
    return sortie
