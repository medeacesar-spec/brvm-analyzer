"""Le risque d'un portefeuille, qui n'est pas la somme de celui de ses lignes.

Deux titres qui ne bougent pas ensemble s'annulent en partie. Mesure faite sur
les portefeuilles reels de l'application, l'ecart est considerable :

    portefeuille A   volatilite reelle 14,5 %   moyenne ponderee 26,0 %
    portefeuille B   volatilite reelle 14,8 %   moyenne ponderee 22,3 %
    portefeuille C   volatilite reelle 24,5 %   moyenne ponderee 27,4 %

Additionner les volatilites des lignes surestimerait le risque de moitie. Il
faut la matrice de covariance, que les series mensuelles rendent enfin
calculables.

LA CONTRIBUTION AU RISQUE est la mesure qui change le regard. Un titre pese un
poids dans le portefeuille et une PART DE SON RISQUE, et les deux different :
sur un portefeuille reel, Ecobank Transnational pese 16,5 % et apporte 34,7 %
du risque, quand Ecobank Cote d'Ivoire pese 12,2 % et n'en apporte que 3,1 %.
Le second diversifie, le premier concentre. Aucune mesure titre par titre ne
peut le dire — cela ne se voit que dans l'ensemble.
"""
from __future__ import annotations

import math
import statistics as st
from typing import Optional

from analysis.risque import _rendements_mensuels, toutes_les_mesures
from data.db import get_connection
from data.storage import _maybe_cache_data

# En dessous, une position se solde en une seance : la liquidite n'est pas une
# contrainte et le dire serait alarmiste. Sur les portefeuilles actuels — un a
# six millions — toutes les lignes sont sous ce seuil.
JOURS_SORTIE_NEGLIGEABLES = 1.0
SEANCES_PAR_MOIS = 21


def _covariance(serie_a: list, serie_b: list) -> float:
    n = len(serie_a)
    ma, mb = st.mean(serie_a), st.mean(serie_b)
    return sum((x - ma) * (y - mb) for x, y in zip(serie_a, serie_b)) / (n - 1)


@_maybe_cache_data(ttl=300)
def mesures_portefeuille(positions: tuple) -> Optional[dict]:
    """Risque, concentration et liquidite d'un ensemble de positions.

    `positions` est un tuple de (ticker, valeur en francs) — un tuple et non un
    dictionnaire, pour que la memoisation puisse en faire une cle.
    """
    # Un meme titre peut arriver en PLUSIEURS LOTS — trois achats d'Ecobank a
    # des dates differentes font trois lignes. Un dictionnaire par
    # comprehension n'en garderait que la derniere, et le poids serait faux
    # sans que rien ne le signale.
    valeurs = {}
    for ticker, valeur in positions:
        if valeur and valeur > 0:
            valeurs[ticker] = valeurs.get(ticker, 0) + valeur
    total = sum(valeurs.values())
    if not total:
        return None

    cnx = get_connection()
    try:
        series = _rendements_mensuels(cnx)
    finally:
        cnx.close()
    mesures_titres = toutes_les_mesures()

    poids = {t: v / total for t, v in valeurs.items()}
    # On ne mesure que les lignes dont on a l'historique ; les autres sont
    # nommees a part plutot que silencieusement ignorees.
    suivis = [t for t in poids if t in series]
    hors_mesure = sorted(t for t in poids if t not in series)
    if not suivis:
        return {"total": total, "hors_mesure": hors_mesure, "lignes": [],
                "volatilite": None}

    # Toutes les series sont ramenees a la longueur de la plus courte : une
    # covariance ne se calcule pas sur des periodes qui ne se recouvrent pas.
    n = min(len(series[t][0]) for t in suivis)
    rendements = {t: series[t][0][-n:] for t in suivis}
    part = {t: poids[t] for t in suivis}
    somme = sum(part.values())
    part = {t: p / somme for t, p in part.items()}       # renormalise

    variance = sum(part[a] * part[b] * _covariance(rendements[a], rendements[b])
                   for a in suivis for b in suivis)
    volatilite = math.sqrt(variance) * math.sqrt(12) if variance > 0 else 0.0
    moyenne_ponderee = sum(
        part[t] * (mesures_titres.get(t, {}).get("volatilite") or 0)
        for t in suivis)

    lignes = []
    for t in sorted(suivis, key=lambda x: -poids[x]):
        # Contribution au risque : la part de la variance totale qu'apporte
        # cette ligne, compte tenu de ses liens avec toutes les autres.
        contribution = (part[t] * sum(part[b] * _covariance(rendements[t],
                                                            rendements[b])
                                      for b in suivis) / variance
                        if variance else None)
        echange = (mesures_titres.get(t) or {}).get("montant_echange")
        jours = (valeurs[t] / (echange / SEANCES_PAR_MOIS)) if echange else None
        lignes.append({
            "ticker": t,
            "valeur": valeurs[t],
            "poids": poids[t],
            "contribution_risque": contribution,
            "concentre": (contribution is not None
                          and contribution > poids[t] * 1.25),
            "diversifie": (contribution is not None
                           and contribution < poids[t] * 0.75),
            "jours_sortie": jours,
            "volatilite": (mesures_titres.get(t) or {}).get("volatilite"),
            "sharpe": (mesures_titres.get(t) or {}).get("sharpe"),
        })

    herfindahl = sum(p ** 2 for p in poids.values())
    return {
        "total": total,
        "volatilite": volatilite,
        "volatilite_sans_diversification": moyenne_ponderee,
        "gain_diversification": (1 - volatilite / moyenne_ponderee
                                 if moyenne_ponderee else None),
        "concentration": herfindahl,
        "lignes_equivalentes": (1 / herfindahl) if herfindahl else None,
        "lignes": lignes,
        "hors_mesure": hors_mesure,
        "observations": n,
        "sortie_contraignante": any(
            (l["jours_sortie"] or 0) > JOURS_SORTIE_NEGLIGEABLES
            for l in lignes),
    }


def lecture_portefeuille(p: dict) -> list:
    """Ce que l'ensemble dit, et qu'aucune ligne ne dit seule."""
    if not p or not p.get("lignes"):
        return []
    phrases = []
    gain = p.get("gain_diversification")
    if gain is not None:
        if gain >= 0.25:
            phrases.append(
                f"Les lignes **ne bougent pas ensemble** : le portefeuille "
                f"vibre à {p['volatilite']:.1%} par an là où la moyenne de ses "
                f"titres donnerait {p['volatilite_sans_diversification']:.1%}. "
                f"La diversification retire **{gain:.0%}** du risque.")
        else:
            phrases.append(
                f"Le portefeuille vibre à {p['volatilite']:.1%} par an, contre "
                f"{p['volatilite_sans_diversification']:.1%} pour la moyenne de "
                f"ses titres : la diversification n'en retire que "
                f"**{gain:.0%}**. Les lignes montent et descendent ensemble.")

    equiv = p.get("lignes_equivalentes")
    if equiv is not None:
        phrases.append(
            f"Malgré **{len(p['lignes'])} lignes**, la concentration équivaut "
            f"à **{equiv:.1f} positions égales**." if equiv < len(p["lignes"]) * 0.8
            else f"Les **{len(p['lignes'])} lignes** sont réparties de façon "
                 f"équilibrée.")

    concentres = [l for l in p["lignes"] if l["concentre"]]
    if concentres:
        pire = max(concentres, key=lambda l: l["contribution_risque"])
        phrases.append(
            f"**{pire['ticker']} pèse {pire['poids']:.0%} du portefeuille mais "
            f"{pire['contribution_risque']:.0%} de son risque.** Alléger cette "
            f"ligne réduirait le risque plus que son poids ne le suggère.")
    diversifiants = [l for l in p["lignes"] if l["diversifie"]]
    if diversifiants:
        meilleur = min(diversifiants, key=lambda l: l["contribution_risque"])
        phrases.append(
            f"À l'inverse **{meilleur['ticker']} apporte "
            f"{meilleur['contribution_risque']:.0%} du risque pour "
            f"{meilleur['poids']:.0%} du portefeuille** : il amortit les "
            f"autres.")

    if not p.get("sortie_contraignante"):
        phrases.append(
            "**La liquidité n'est pas une contrainte à cette taille** : chaque "
            "ligne se solderait en moins d'une séance.")
    else:
        lents = sorted((l for l in p["lignes"] if (l["jours_sortie"] or 0) > 1),
                       key=lambda l: -l["jours_sortie"])[:2]
        phrases.append(
            "**Sortie difficile** sur " + ", ".join(
                f"{l['ticker']} ({l['jours_sortie']:.0f} séances)"
                for l in lents) + " au rythme d'échange habituel.")

    if p.get("hors_mesure"):
        phrases.append(
            f"{', '.join(p['hors_mesure'])} : moins de deux ans de cotation, "
            f"donc hors de ce calcul. Le risque affiché ne les couvre pas.")
    return phrases
