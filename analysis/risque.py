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
IMMOBILITE_SUSPECTE = 0.20        # au-delà, l'immobilité mérite une explication

# UN TITRE IMMOBILE N'EST PAS FORCEMENT ILLIQUIDE. Ecobank Transnational cote
# 71 francs : le plus petit saut possible, un franc, vaut 1,41 % de son cours.
# Il ne PEUT pas bouger un peu. Ses 37 % de mois sans variation viennent du pas
# de cotation, pas d'une absence d'echanges — il traite 148 millions par mois,
# rang 23 sur 45. A l'inverse SICOR n'en traite que 2,9 millions avec 3 % de
# mois immobiles.
#
# L'illiquidite se mesure donc au MONTANT ECHANGE, et relativement a la cote :
# est peu liquide le QUART LE MOINS ECHANGE. Un seuil absolu vieillirait — les
# montants traites montent avec le marche — quand un quartile suit de lui-meme.
# Au 7 septembre 2026 il tombe a 44 millions par mois, soit deux millions par
# seance.
QUANTILE_ILLIQUIDE = 0.25


def _seuil_illiquidite(montants) -> float:
    """Le premier quartile des montants echanges, ou zero si trop peu de titres."""
    valeurs = sorted(m for m in montants if m)
    if len(valeurs) < 4:
        return 0.0
    return valeurs[int(QUANTILE_ILLIQUIDE * (len(valeurs) - 1))]


@_maybe_cache_data(ttl=300)
def series_mensuelles() -> dict:
    """Les series de rendement de toute la cote, memoisees.

    Elles sont lues par le profil d'un titre, par le tableau de la cote, par le
    risque du portefeuille et par chaque simulation d'allocation — quatre fois
    la meme lecture de base et le meme calcul. Memoisees, elles ramenent le
    reglage du seuil de liquidite d'une dizaine de secondes a l'instantane.
    """
    cnx = get_connection()
    try:
        return _rendements_mensuels(cnx)
    finally:
        cnx.close()


def _rendements_mensuels(cnx) -> dict:
    """Rendement total mois par mois, pour tous les titres a la fois.

    Une seule lecture pour toute la cote : la page compare le titre a ses
    pairs et au marche, elle a besoin de tout le monde de toute facon.
    """
    cours = defaultdict(list)
    for ligne in cnx.execute("SELECT ticker, date, close, volume FROM "
                             "price_monthly WHERE close > 0 "
                             "ORDER BY ticker, date"):
        ligne = dict(ligne)
        cours[ligne["ticker"]].append(
            (ligne["date"], ligne["close"], ligne["volume"] or 0))

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
            (_, avant, _), (jour, apres, _) = points[i - 1], points[i]
            verse = verses.get((jour.year, jour.month), 0)
            rendements.append((apres + verse) / avant - 1)
        series[ticker] = (rendements, points)
    return series


def _mediane(valeurs):
    """La mediane, ou rien si la liste est vide — un titre sans volume connu
    ne doit pas faire echouer tout le profil."""
    valeurs = [v for v in valeurs if v is not None]
    return st.median(valeurs) if valeurs else None


def _serie_marche(series: dict) -> dict:
    """Rendement mensuel du BRVM Composite, indexe par (annee, mois).

    Le beta se mesure contre un marche ; sur cette place, c'est le Composite.
    Le BRVM-30 ne remonte qu'a 2023 et ne couvre que trente valeurs — il
    ferait un repere plus court et plus etroit.
    """
    if "BRVMC" not in series:
        return {}
    rendements, points = series["BRVMC"]
    return {(points[i + 1][0].year, points[i + 1][0].month): r
            for i, r in enumerate(rendements)}


def _mesures(rendements: list, points: list, marche: dict = None) -> dict:
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
    echange_median = _mediane([v * p for _, p, v in points[1:]])

    # BETA. La sensibilite au marche : de combien bouge le titre quand le
    # Composite bouge d'un point. Mesure sur les rendements MENSUELS, pas
    # quotidiens — sur cette place, un titre qui ne cote pas garde son cours,
    # et une serie quotidienne serait surtout faite de zeros.
    #
    # Le meme piege demeure au mois, en plus doux : les mois immobiles tirent
    # le beta vers zero et la correlation avec. `part_mois_immobiles` est donc
    # publiee a cote, et un titre au-dela du seuil doit se lire avec elle.
    beta = correlation = None
    observations_beta = 0
    if marche:
        paires = [(r, marche[(points[i + 1][0].year, points[i + 1][0].month)])
                  for i, r in enumerate(rendements)
                  if (points[i + 1][0].year, points[i + 1][0].month) in marche]
        if len(paires) >= MINIMUM_MOIS:
            observations_beta = len(paires)
            titre = [a for a, _ in paires]
            indice = [b for _, b in paires]
            var_m = st.pvariance(indice)
            if var_m:
                moy_t, moy_i = st.mean(titre), st.mean(indice)
                cov = sum((a - moy_t) * (b - moy_i)
                          for a, b in paires) / len(paires)
                beta = cov / var_m
                ec_t, ec_i = st.pstdev(titre), st.pstdev(indice)
                if ec_t and ec_i:
                    correlation = cov / (ec_t * ec_i)
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
        # LIQUIDITE. Le montant echange est le plus concret des trois :
        # Sonatel traite 3 878 millions par mois, Unilever 0,9 — un facteur
        # quatre mille. L'illiquidite d'Amihud rapporte le mouvement du cours
        # au montant qui l'a provoque : combien de pour cent de variation par
        # million de francs echange. Elle mesure ce que coute VRAIMENT une
        # sortie, la ou le montant echange ne dit que la taille du marche.
        "montant_echange": echange_median,
        "impact_transaction": _mediane(
            [abs(r) / (points[i + 1][2] * points[i + 1][1] / 1e6)
             for i, r in enumerate(rendements)
             if points[i + 1][2] and points[i + 1][1]]),
        "part_mois_immobiles": immobiles,
        "observations": n,
        "beta": beta,
        "correlation_marche": correlation,
        "observations_beta": observations_beta,
        # `peu_liquide` se pose plus tard, une fois toute la cote mesuree : il
        # designe le quart le moins echange, ce qu'un titre seul ne peut pas
        # savoir. L'immobilite, elle, se lit sur la seule serie du titre — et
        # ne se confond pas avec l'illiquidite : sur un titre a faible nominal,
        # le pas de cotation suffit a figer le cours.
        "immobile": immobiles >= IMMOBILITE_SUSPECTE,
        "peu_liquide": False,
    }


# La page appelle ce calcul DEUX fois — l'onglet Risque et la carte de
# Recommandation — et Streamlit rejoue tout a chaque interaction. Or il lit
# toute la serie mensuelle de la cote a chaque appel, parce qu'il compare le
# titre a ses pairs. Cinq minutes de memoire suffisent a rendre la navigation
# fluide sans risquer d'afficher des mesures perimees.
# Les mesures ordonnees, et le sens dans lequel elles sont bonnes.
# « moindre est meilleur » pour un risque, l'inverse pour un rendement.
# Le RISQUE et la LIQUIDITE ne sont pas de meme nature, et les melanger dans
# un seul tableau brouille les deux. La volatilite coute pendant qu'on detient ;
# l'illiquidite ne coute qu'au moment ou l'on veut sortir. Un excellent titre
# illiquide reste excellent — il est seulement difficile a quitter. La
# liquidite dit donc ce qu'on PEUT faire, pas ce qu'on risque.
MESURES_RISQUE = (
    ("volatilite", "Volatilité", True),
    ("semi_volatilite", "Semi-volatilité", True),
    ("perte_maximale", "Perte maximale", False),   # -5 % vaut mieux que -40 %
    ("sharpe", "Rendement par unité de risque", False),
    ("rendement_annualise", "Rendement annualisé", False),
)

MESURES_LIQUIDITE = (
    ("montant_echange", "Montant échangé par mois", False),
    ("impact_transaction", "Impact d'une transaction", True),
    ("part_mois_immobiles", "Mois sans variation de cours", True),
)

MESURES = MESURES_RISQUE + MESURES_LIQUIDITE

# Chaque mesure se dit DEUX fois. « En clair » pour qui veut comprendre ce que
# le chiffre change pour lui ; « techniquement » pour qui veut verifier le
# calcul ou le refaire. Les deux registres cohabitent parce qu'ils ne
# s'adressent pas au meme lecteur, et qu'aucun des deux ne remplace l'autre :
# une definition seule n'aide personne a decider, une paraphrase seule ne se
# verifie pas.
def formater(champ: str, valeur) -> str:
    """Chaque mesure a son unite, et les confondre rend la page absurde.

    Un pourcentage, un ratio et un montant en francs ne s'ecrivent pas de la
    meme facon : afficher le montant echange comme un pourcentage donnerait
    « 387 806 147 000 % ». Le format vit donc ici, avec la mesure, plutot que
    dans la page — pour que tout ce qui lit ces valeurs les ecrive pareil.
    """
    if valeur is None:
        return "—"
    if champ == "montant_echange":
        if valeur >= 1e9:
            return f"{valeur / 1e9:.2f} Md"
        if valeur >= 1e6:
            return f"{valeur / 1e6:.1f} M"
        return f"{valeur / 1e3:.0f} k"
    if champ == "impact_transaction":
        return f"{valeur:.4f}"
    if champ in ("sharpe", "sortino", "asymetrie"):
        return f"{valeur:.2f}"
    return f"{valeur * 100:.1f} %"


# Le resume tient sur une ligne, sous le libelle, dans le tableau lui-meme.
# Il ne remplace pas l'explication longue : il evite d'avoir a l'ouvrir pour
# se rappeler ce qu'on regarde. Quelques mots, la formule si elle tient.
RESUMES = {
    "volatilite":
        "Écart-type des rendements mensuels, annualisé",
    "semi_volatilite":
        "Même calcul, sur les seuls mois sous le taux sans risque",
    "perte_maximale":
        "Plus forte chute d'un sommet à un creux",
    "sharpe":
        "Ratio de Sharpe : rendement en excès du sans-risque, "
        "divisé par l'écart-type",
    "rendement_annualise":
        "Croissance moyenne par an, dividendes compris",
    "montant_echange":
        "Valeur médiane traitée en un mois, en francs",
    "impact_transaction":
        "Illiquidité d'Amihud : variation du cours par million échangé",
    "part_mois_immobiles":
        "Part des mois où le cours n'a pas bougé du tout",
}

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
    "montant_echange": (
        "Combien de francs changent de main en un mois. C'est la taille du "
        "marché du titre : sur cette place, Sonatel en traite près de quatre "
        "milliards quand certaines lignes n'en traitent pas un million. En "
        "dessous d'un certain seuil, vendre sa position prend des semaines, "
        "ou fait chuter le cours soi-même.",
        "Médiane du produit volume × cours sur les mois de la période. La "
        "médiane et non la moyenne : une seule transaction de bloc suffirait "
        "à donner l'illusion d'un marché actif.",
    ),
    "impact_transaction": (
        "De combien le cours bouge quand un million de francs s'échange. "
        "C'est le vrai coût d'une sortie : là où le montant échangé dit la "
        "taille du marché, celui-ci dit ce qu'il en coûte d'y entrer ou d'en "
        "sortir. Plus il est bas, plus on peut acheter ou vendre sans "
        "déplacer le cours contre soi.",
        "Ratio d'illiquidité d'Amihud : médiane de |rendement| divisé par le "
        "montant échangé, exprimé ici en pourcentage de variation par million "
        "de FCFA traité.",
    ),
    "part_mois_immobiles": (
        "La proportion de mois où le cours n'a pas bougé d'un franc. Un titre "
        "immobile n'est pas un titre stable : c'est un titre que personne "
        "n'échange. Cette part fausse toutes les autres mesures dans le sens "
        "qui flatte — un rendement nul passe pour du calme.",
        "Part des rendements mensuels exactement égaux à zéro. Mesure "
        "d'illiquidité de Lesmond : l'absence de transaction laisse le "
        "dernier cours en place et fabrique un faux rendement nul.",
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


def synthese(profil: dict) -> dict:
    """Deux phrases, une par nature : ce qu'on subit, ce qu'il en coute de sortir.

    Elles ne se melangent pas. La premiere resume le couple risque-rendement,
    la seconde la liquidite — un titre peut etre tranquille et impossible a
    vendre, ou agite et liquide comme l'eau. Les fondre en une seule phrase
    obligerait a arbitrer entre les deux a la place du lecteur.
    """
    m = profil["titre"]
    vol = profil["situations"]["volatilite"]["marché"]
    rdt = profil["situations"]["rendement_annualise"]["marché"]
    ech = (profil["situations"].get("montant_echange") or {}).get("marché")

    if not (vol and rdt):
        return {"risque": None, "liquidite": None}

    n = vol["effectif"]
    calme = vol["rang"] <= n / 3
    agite = vol["rang"] > 2 * n / 3
    paye = rdt["rang"] <= n / 3
    faible = rdt["rang"] > 2 * n / 3

    if (m.get("rendement_annualise") or 0) < 0:
        risque = ("Il a **perdu de l'argent** sur la période, quelle que soit "
                  "la tranquillité apparente de son cours.")
    elif calme and paye:
        risque = ("**Rare** : il rend beaucoup en bougeant peu — le haut du "
                  "panier sur les deux tableaux à la fois.")
    elif calme and faible:
        risque = ("**Défensif** : peu d'agitation, peu de rendement. Il "
                  "protège plus qu'il ne fait gagner.")
    elif agite and paye:
        risque = ("**Nerveux mais payant** : il rend beaucoup et le fait "
                  "durement sentir. À ne détenir qu'en sachant l'encaisser.")
    elif agite and faible:
        risque = ("**Le plus mauvais compromis** : il agite beaucoup pour "
                  "rendre peu.")
    else:
        risque = ("**Dans la moyenne de la cote**, autant par son agitation "
                  "que par son rendement.")

    liquidite = None
    if ech:
        montant = formater("montant_echange", m.get("montant_echange"))
        if ech["rang"] > 2 * ech["effectif"] / 3:
            liquidite = (f"**Difficile à quitter** : {montant} de FCFA "
                         f"échangés par mois seulement. La taille d'une "
                         f"position s'y décide avant l'achat, pas à la vente.")
        elif ech["rang"] <= ech["effectif"] / 3:
            liquidite = (f"**Facile à quitter** : {montant} de FCFA échangés "
                         f"par mois. Entrer et sortir n'y pose pas de "
                         f"difficulté.")
        else:
            liquidite = (f"**Liquidité moyenne** : {montant} de FCFA par "
                         f"mois. Une position modeste se dénoue sans "
                         f"difficulté, une position lourde demande du temps.")
    return {"risque": risque, "liquidite": liquidite}


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

    # La liquidite est le risque qu'on ne decouvre qu'en essayant de vendre.
    echange = (profil["situations"].get("montant_echange") or {}).get("marché")
    if echange and m.get("montant_echange") is not None:
        n = echange["effectif"]
        montant = formater("montant_echange", m["montant_echange"])
        if echange["rang"] > 2 * n / 3:
            phrases.append(
                f"**Il ne s'en échange que {montant} de FCFA par mois** — "
                f"{_rang(echange['rang'])} marché de la cote sur {n}. Sortir "
                f"d'une position importante peut demander des semaines, ou "
                f"faire baisser le cours soi-même.")
        elif echange["rang"] <= n / 3:
            phrases.append(
                f"Il s'en échange **{montant} de FCFA par mois**, "
                f"{_rang(echange['rang'])} marché de la cote : entrer et en "
                f"sortir n'y pose pas de difficulté.")

    # Immobilite et illiquidite se confondent souvent, et ce n'est pas la meme
    # chose : l'une decrit un cours qui ne peut pas bouger finement, l'autre un
    # marche ou l'on n'echange pas. Les dire separement evite un contresens.
    if m.get("peu_liquide"):
        phrases.append(
            f"**Marché très étroit** : "
            f"{formater('montant_echange', m.get('montant_echange'))} de FCFA "
            f"échangés par mois. Ses mesures reposent sur peu de transactions "
            f"et sont à prendre avec prudence.")
    elif m.get("immobile"):
        phrases.append(
            f"Son cours n'a pas bougé {m['part_mois_immobiles']:.0%} des mois. "
            f"**Ce n'est pas forcément un manque d'échanges** : sur un titre à "
            f"faible nominal, le plus petit saut de cotation pèse déjà un ou "
            f"deux pour cent, et le cours ne peut pas varier moins. Ses "
            f"mesures sont donc plus grossières que celles des autres.")
    return phrases


@_maybe_cache_data(ttl=300)
def toutes_les_mesures() -> dict:
    """Les mesures de tous les titres d'un coup, pour les pages qui listent.

    `profil_de_risque` sert une fiche : il calcule toute la cote pour situer un
    titre, et rend un objet riche. Le screening, la performance et le
    portefeuille n'ont pas besoin de cette richesse quarante-cinq fois — ils
    veulent un tableau. Passer par la fiche titre par titre reviendrait a
    relire la meme serie a chaque ligne.

    Les indices sont ecartes : ils n'ont pas leur place dans un classement de
    titres, et fausseraient les medianes.
    """
    series = series_mensuelles()
    marche = _serie_marche(series)
    mesures = {t: _mesures(*v, marche=marche) for t, v in series.items()
               if t not in ("BRVMC", "BRVM30")}
    seuil = _seuil_illiquidite(m.get("montant_echange") for m in mesures.values())
    for m in mesures.values():
        echange = m.get("montant_echange")
        m["peu_liquide"] = bool(echange is not None and echange <= seuil)
        m["seuil_illiquidite"] = seuil
    return mesures


@_maybe_cache_data(ttl=300)
def profil_de_risque(ticker: str, secteur: Optional[str] = None) -> Optional[dict]:
    """Mesures du titre, et les memes mesures medianes chez ses pairs.

    Le repere sectoriel ne se publie qu'a partir de trois pairs — une mediane
    sur deux societes decrit une societe. En dessous, on bascule sur le marche
    entier et on le DIT : `portee` vaut « secteur » ou « marché ». Ecrire
    « secteur » quand on montre le marche a deja trompe des lecteurs.
    """
    series = series_mensuelles()
    if ticker not in series:
        return None
    cnx = get_connection()
    try:
        secteurs = {}
        for ligne in cnx.execute("SELECT ticker, sector FROM market_data "
                                 "WHERE sector IS NOT NULL"):
            ligne = dict(ligne)
            secteurs[ligne["ticker"]] = ligne["sector"]
    finally:
        cnx.close()

    secteur = secteur or secteurs.get(ticker)
    indices = {"BRVMC", "BRVM30"}
    marche = _serie_marche(series)
    tous = {t: _mesures(*v, marche=marche) for t, v in series.items()
            if t not in indices}
    seuil = _seuil_illiquidite(m.get("montant_echange") for m in tous.values())
    for m in tous.values():
        echange = m.get("montant_echange")
        m["peu_liquide"] = bool(echange is not None and echange <= seuil)
        m["seuil_illiquidite"] = seuil
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
    profil["synthese"] = synthese(profil)
    return profil
