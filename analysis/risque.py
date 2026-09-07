"""Mesures de risque d'un titre, lues sur la serie mensuelle des cours.

Le score du modele ne dit rien du risque, et c'est deliberé : il juge une
societe et une tendance, pas la facon dont le cours y arrive. Un titre peut
etre excellent et eprouvant a detenir. Ces mesures-la vivent donc a part.

Trois choix de methode, tous discutables et tous assumes :

RENDEMENT TOTAL. Les mesures portent sur cours ET dividendes. Une performance
de cours seule sous-estime un titre a huit pour cent de rendement, et sur cette
place beaucoup d'actionnaires achetent pour le revenu. Onatel en est
l'illustration : son Sharpe passe de -0,39 a -0,05 des qu'on compte ses
distributions.

TAUX SANS RISQUE A SIX POUR CENT. Il n'existe pas de taux sans risque unique
pour l'UEMOA dans nos donnees ; six pour cent est l'ordre de grandeur d'un bon
du Tresor regional. C'est une HYPOTHESE, elle s'affiche comme telle, et elle
ne change aucun classement — seulement le niveau des Sharpe.

L'IMMOBILITE N'EST PAS DE LA STABILITE. Un titre qui ne cote pas garde son
dernier cours : son rendement mensuel est nul alors que le marche a bouge. Sa
volatilite paraît basse, sa correlation faible — l'inverse de la verite. La
proportion de mois sans variation accompagne donc chaque mesure, pour que le
lecteur sache quand s'en mefier. Ecobank Transnational est a quarante-deux pour
cent, Unilever a vingt-trois.
"""
from __future__ import annotations

import math
import statistics as st
from collections import defaultdict
from typing import Optional

from data.db import get_connection
from data.storage import _maybe_cache_data

TAUX_SANS_RISQUE = 0.06
MINIMUM_MOIS = 24                 # en deçà, aucune mesure n'est publiée
MINIMUM_PAIRS = 3                 # une médiane sur deux sociétés décrit une société
IMMOBILITE_SUSPECTE = 0.20        # au-delà, les mesures sont annoncées douteuses


def _rendements_mensuels(cnx) -> dict:
    """Rendement total mois par mois, pour tous les titres a la fois.

    Une seule lecture pour toute la cote : la page compare le titre a ses
    pairs et au marche, elle a besoin de tout le monde de toute facon.
    """
    cours = defaultdict(list)
    for ligne in cnx.execute("SELECT ticker, date, close FROM price_monthly "
                             "WHERE close > 0 ORDER BY ticker, date"):
        ligne = dict(ligne)
        cours[ligne["ticker"]].append((ligne["date"], ligne["close"]))

    # Le MOIS de versement compte autant que le montant : ranger tous les
    # dividendes en juillet cree un pic artificiel qui gonfle la volatilite —
    # celle de Sonatel passait de 15,9 a 19,9 % pour cette seule raison. La
    # date reelle est en base, colonne `dps_paiement`, remplie depuis les avis
    # de la BRVM. Pour les exercices sans avis, on retient le mois habituel du
    # titre, et a defaut juillet.
    dividendes = defaultdict(dict)
    for ligne in cnx.execute("SELECT ticker, fiscal_year, dps, dps_paiement "
                             "FROM fundamentals WHERE dps IS NOT NULL AND dps > 0"):
        ligne = dict(ligne)
        jour = ligne.get("dps_paiement")
        paiement = (jour.year, jour.month) if jour else None
        dividendes[ligne["ticker"]][ligne["fiscal_year"]] = (ligne["dps"], paiement)

    series = {}
    for ticker, points in cours.items():
        if len(points) < MINIMUM_MOIS:
            continue
        connus = [p[1] for p in dividendes.get(ticker, {}).values() if p[1]]
        habituel = st.mode([m for _, m in connus]) if connus else 7
        verses = {}
        for exercice, (montant, paiement) in dividendes.get(ticker, {}).items():
            cle = paiement or (exercice + 1, habituel)
            verses[cle] = verses.get(cle, 0) + montant
        rendements = []
        for i in range(1, len(points)):
            (_, avant), (jour, apres) = points[i - 1], points[i]
            verse = verses.get((jour.year, jour.month), 0)
            rendements.append((apres + verse) / avant - 1)
        series[ticker] = (rendements, points)
    return series


def _mesures(rendements: list, points: list) -> dict:
    mensuel_sans_risque = (1 + TAUX_SANS_RISQUE) ** (1 / 12) - 1
    n = len(rendements)
    volatilite = st.stdev(rendements) * math.sqrt(12)
    sous_seuil = [min(r - mensuel_sans_risque, 0) ** 2 for r in rendements]
    semi = math.sqrt(sum(sous_seuil) / n) * math.sqrt(12)
    exces = [r - mensuel_sans_risque for r in rendements]

    # Perte maximale : la plus forte chute d'un sommet a un creux, sur la
    # trajectoire reellement vecue par l'actionnaire. On garde le sommet qui
    # l'a precedee, pour savoir combien de mois il a fallu pour le retrouver —
    # « combien j'ai perdu » et « combien de temps » sont deux questions.
    trajectoire, valeur = [], 1.0
    for r in rendements:
        valeur *= (1 + r)
        trajectoire.append(valeur)
    sommet, sommet_a, pire, creux_a = trajectoire[0], 0, 0.0, 0
    sommet_de_la_pire = trajectoire[0]
    for i, v in enumerate(trajectoire):
        if v > sommet:
            sommet, sommet_a = v, i
        chute = v / sommet - 1
        if chute < pire:
            pire, creux_a, sommet_de_la_pire = chute, i, sommet
    recuperation = None
    for i in range(creux_a + 1, len(trajectoire)):
        if trajectoire[i] >= sommet_de_la_pire:
            recuperation = i - creux_a
            break

    immobiles = sum(1 for r in rendements if r == 0) / n
    return {
        "volatilite": volatilite,
        "semi_volatilite": semi,
        "asymetrie": (semi / volatilite) if volatilite else None,
        "perte_maximale": pire,
        "mois_recuperation": recuperation,
        "sharpe": (st.mean(exces) / st.stdev(exces) * math.sqrt(12)
                   if st.stdev(exces) else None),
        # `semi` est deja annualisee : l'exces mensuel se ramene a l'annee en
        # le multipliant par douze, pas par racine de douze une fois de plus.
        "sortino": (st.mean(exces) * 12 / semi) if semi else None,
        "rendement_annualise": (points[-1][1] / points[0][1]) ** (12 / n) - 1,
        "part_mois_immobiles": immobiles,
        "observations": n,
        "peu_liquide": immobiles >= IMMOBILITE_SUSPECTE,
    }


# La page appelle ce calcul DEUX fois — l'onglet Risque et la carte de
# Recommandation — et Streamlit rejoue tout a chaque interaction. Or il lit
# toute la serie mensuelle de la cote a chaque appel, parce qu'il compare le
# titre a ses pairs. Cinq minutes de memoire suffisent a rendre la navigation
# fluide sans risquer d'afficher des mesures perimees.
@_maybe_cache_data(ttl=300)
def profil_de_risque(ticker: str, secteur: Optional[str] = None) -> Optional[dict]:
    """Mesures du titre, et les memes mesures medianes chez ses pairs.

    Le repere sectoriel ne se publie qu'a partir de trois pairs — une mediane
    sur deux societes decrit une societe. En dessous, on bascule sur le marche
    entier et on le DIT : `portee` vaut « secteur » ou « marché ». Ecrire
    « secteur » quand on montre le marche a deja trompe des lecteurs.
    """
    cnx = get_connection()
    try:
        series = _rendements_mensuels(cnx)
        if ticker not in series:
            return None
        secteurs = {}
        for ligne in cnx.execute("SELECT ticker, sector FROM market_data "
                                 "WHERE sector IS NOT NULL"):
            ligne = dict(ligne)
            secteurs[ligne["ticker"]] = ligne["sector"]
    finally:
        cnx.close()

    secteur = secteur or secteurs.get(ticker)
    indices = {"BRVMC", "BRVM30"}
    tous = {t: _mesures(*v) for t, v in series.items() if t not in indices}
    if ticker not in tous:
        return None

    pairs = [t for t, s in secteurs.items()
             if s == secteur and t in tous and t != ticker]
    portee = "secteur" if len(pairs) >= MINIMUM_PAIRS else "marché"
    comparables = pairs if portee == "secteur" else [t for t in tous if t != ticker]

    champs = ("volatilite", "semi_volatilite", "perte_maximale", "sharpe",
              "sortino", "rendement_annualise")
    medianes = {}
    for champ in champs:
        valeurs = [tous[t][champ] for t in comparables if tous[t].get(champ) is not None]
        medianes[champ] = st.median(valeurs) if len(valeurs) >= MINIMUM_PAIRS else None

    marche = {}
    for champ in champs:
        valeurs = [m[champ] for t, m in tous.items() if m.get(champ) is not None]
        marche[champ] = st.median(valeurs) if valeurs else None

    # Rang du titre sur la volatilite, du plus calme au plus agite.
    classement = sorted(tous, key=lambda t: tous[t]["volatilite"])
    indice_reference = {}
    if "BRVMC" in series:
        indice_reference = _mesures(*series["BRVMC"])

    return {
        "titre": tous[ticker],
        "medianes": medianes,
        "marche": marche,
        "indice": indice_reference,
        "portee": portee,
        "secteur": secteur,
        "nb_pairs": len(comparables),
        "rang_volatilite": classement.index(ticker) + 1,
        "nb_titres": len(classement),
        "taux_sans_risque": TAUX_SANS_RISQUE,
    }
