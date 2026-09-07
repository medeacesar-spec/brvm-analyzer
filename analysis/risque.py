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
# Les mesures ordonnees, et le sens dans lequel elles sont bonnes.
# « moindre est meilleur » pour un risque, l'inverse pour un rendement.
MESURES = (
    ("volatilite", "Volatilité", True),
    ("semi_volatilite", "Semi-volatilité", True),
    ("perte_maximale", "Perte maximale", False),   # -5 % vaut mieux que -40 %
    ("sharpe", "Rendement par unité de risque", False),
    ("rendement_annualise", "Rendement annualisé", False),
)

# Chaque mesure se dit DEUX fois. « En clair » pour qui veut comprendre ce que
# le chiffre change pour lui ; « techniquement » pour qui veut verifier le
# calcul ou le refaire. Les deux registres cohabitent parce qu'ils ne
# s'adressent pas au meme lecteur, et qu'aucun des deux ne remplace l'autre :
# une definition seule n'aide personne a decider, une paraphrase seule ne se
# verifie pas.
EXPLICATIONS = {
    "volatilite": (
        "De combien le cours bouge d'un mois sur l'autre, hausses et baisses "
        "confondues. Un titre à 15 % remue deux fois moins qu'un titre à 30 %. "
        "Elle ne dit rien du sens : un titre très volatil peut n'avoir fait que "
        "monter.",
        "Écart-type des rendements mensuels totaux — dividendes compris — "
        "annualisé en multipliant par la racine de douze.",
    ),
    "semi_volatilite": (
        "La même chose, mais en ne comptant que les mois où le titre a fait "
        "moins bien qu'un placement sans risque. Un investisseur ne craint pas "
        "la hausse. Comparée à la volatilité, elle dit si l'agitation joue "
        "pour ou contre lui.",
        "Racine de la moyenne des carrés des écarts NÉGATIFS au seuil — le "
        "taux sans risque ramené au mois — annualisée par la racine de douze. "
        "C'est le dénominateur du ratio de Sortino.",
    ),
    "perte_maximale": (
        "La pire chute vécue sur la période, du plus haut au plus bas : ce "
        "qu'aurait perdu quelqu'un entré au plus mauvais moment. La durée de "
        "récupération dit combien de mois il a fallu pour effacer cette perte "
        "— et parfois qu'elle ne l'a jamais été.",
        "Maximum drawdown : minimum de (valeur ÷ sommet courant − 1) le long "
        "de la trajectoire cumulée des rendements totaux. La récupération "
        "compte les mois entre le creux et le retour au sommet qui le "
        "précédait.",
    ),
    "sharpe": (
        "Combien de rendement le titre paie pour chaque unité d'agitation "
        "qu'il fait subir. Au-dessus de 1, il paie bien ; en dessous de zéro, "
        "il a rapporté moins qu'un placement sans risque tout en faisant "
        "courir un risque.",
        "Ratio de Sharpe : moyenne des rendements en excès du taux sans "
        "risque, divisée par leur écart-type, annualisée par la racine de "
        "douze. Le ratio de Sortino fait le même calcul en remplaçant "
        "l'écart-type par la semi-volatilité.",
    ),
    "rendement_annualise": (
        "Ce que le titre a rapporté par an en moyenne sur la période, "
        "dividendes compris. C'est le gain ; tout le reste de ce tableau en "
        "est le prix.",
        "Taux de croissance géométrique entre le premier et le dernier cours "
        "de la période. Un taux géométrique, non arithmétique : il tient "
        "compte de la composition d'une année sur l'autre.",
    ),
}


def _situer(valeur, population: dict, champ: str, moindre_est_mieux: bool):
    """Ou se place ce titre parmi les autres : rang, mediane et moyenne.

    Le rang dit plus qu'un chiffre absolu. « Volatilite 23,7 % » ne parle a
    personne ; « 7e titre le plus calme sur 45 » se comprend sans formation.

    La mediane ET la moyenne figurent toutes deux, et c'est deliberé : leur
    ecart dit si le groupe est homogene ou tire par un cas extreme. Sur la
    volatilite de la cote, la mediane vaut 36 % et la moyenne 42 % — l'ecart,
    ce sont les quelques lignes peu echangees qui montent a 80 et 106 %.
    """
    valeurs = [m[champ] for m in population.values() if m.get(champ) is not None]
    if valeur is None or len(valeurs) < MINIMUM_PAIRS:
        return None
    ordonnees = sorted(valeurs, reverse=not moindre_est_mieux)
    rang = sum(1 for v in ordonnees if (v < valeur if moindre_est_mieux
                                        else v > valeur)) + 1
    return {
        "rang": rang,
        "effectif": len(valeurs),
        "mediane": st.median(valeurs),
        "moyenne": st.mean(valeurs),
    }


def _rang(n: int) -> str:
    """« 1er », « 2e »… Ecrire « 1e » trahit une phrase ecrite par une machine."""
    return "1er" if n == 1 else f"{n}e"


def lecture(profil: dict) -> list:
    """Ce que les mesures disent, en phrases, sans conseiller quoi que ce soit.

    Une lecture, pas un avis : elle rapporte ce que les chiffres montrent et
    s'arrete la. Le modele a deja un verdict, et il ne porte pas sur le risque.
    """
    m = profil["titre"]
    phrases = []

    vol = profil["situations"]["volatilite"]["marché"]
    rdt = profil["situations"]["rendement_annualise"]["marché"]
    if vol and rdt:
        n = vol["effectif"]
        phrases.append(
            f"Sur les {n} titres mesurables de la cote, celui-ci est le "
            f"**{_rang(vol['rang'])} plus calme** et le "
            f"**{_rang(rdt['rang'])} plus rentable**.")
        # Un rendement negatif se dit d'abord : aucune position de risque ne
        # rattrape une perte, et comparer deux rangs le ferait oublier.
        if (m.get("rendement_annualise") or 0) < 0:
            phrases.append(
                f"Il a **perdu {abs(m['rendement_annualise']):.1%} par an** sur "
                f"la période. Aucune mesure de risque ne rattrape un rendement "
                f"négatif : sa place parmi les titres calmes ne le rend pas "
                f"prudent, seulement lent à descendre.")
        else:
            # Les deux rangs se lisent dans le meme sens : petit vaut mieux.
            # Le rendement mieux classe que le risque decrit un titre qui paie
            # plus que son agitation ne le laisserait attendre ; l'inverse
            # decrit un titre defensif. Aucun des deux n'est meilleur en soi.
            ecart = vol["rang"] - rdt["rang"]
            if ecart > n * 0.20:
                phrases.append(
                    "Il **rend mieux qu'il n'agite** : son rendement le classe "
                    "nettement au-dessus de ce que son niveau de risque "
                    "laisserait attendre.")
            elif ecart < -n * 0.20:
                phrases.append(
                    "Il est **plus calme que rentable** : un profil défensif, "
                    "qui achète de la tranquillité au prix du rendement.")
            else:
                phrases.append(
                    "Rendement et risque le classent au même niveau : il paie "
                    "ce qu'il fait courir, ni plus ni moins.")

    asym = m.get("asymetrie")
    if asym is not None:
        if asym > 0.60:
            phrases.append(
                f"**Les baisses dominent son agitation** — elles en font "
                f"{asym:.0%}. Un titre peut être peu volatil et pourtant "
                f"pénible à détenir : c'est le cas ici.")
        elif asym < 0.40:
            phrases.append(
                f"Son agitation est surtout **haussière** : les baisses n'en "
                f"font que {asym:.0%}.")

    perte, recup = m.get("perte_maximale"), m.get("mois_recuperation")
    if perte is not None:
        if recup:
            phrases.append(
                f"Sa pire chute a coûté **{abs(perte):.0%}**, effacés en "
                f"**{recup} mois**.")
        else:
            phrases.append(
                f"Sa pire chute a coûté **{abs(perte):.0%}**, et **le sommet "
                f"d'avant n'a jamais été retrouvé** sur la période mesurée.")

    if m.get("peu_liquide"):
        phrases.append(
            f"**Ces mesures le flattent.** Il ne cote pas "
            f"{m['part_mois_immobiles']:.0%} du temps, et un cours immobile "
            f"passe pour un cours stable : sa volatilité réelle est plus "
            f"élevée que celle affichée.")
    return phrases


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

    # Le titre se situe DEUX fois : parmi ses pairs de metier, et parmi toute
    # la cote. Les deux reponses different souvent, et l'ecart est instructif —
    # une banque peu agitee pour une banque peut rester agitee pour la cote.
    du_secteur = {t: tous[t] for t in pairs + [ticker]}
    situations = {}
    for champ, _, moindre in MESURES:
        situations[champ] = {
            "secteur": (_situer(tous[ticker][champ], du_secteur, champ, moindre)
                        if len(pairs) >= MINIMUM_PAIRS else None),
            "marché": _situer(tous[ticker][champ], tous, champ, moindre),
        }

    indice_reference = _mesures(*series["BRVMC"]) if "BRVMC" in series else {}

    profil = {
        "titre": tous[ticker],
        "situations": situations,
        "indice": indice_reference,
        "portee": portee,
        "secteur": secteur,
        "nb_pairs_secteur": len(pairs),
        "nb_titres": len(tous),
        "taux_sans_risque": TAUX_SANS_RISQUE,
    }
    profil["lecture"] = lecture(profil)
    return profil
