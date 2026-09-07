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

import itertools
import math
import statistics as st
from typing import Optional

from analysis.risque import (series_mensuelles, toutes_les_mesures,
                             TAUX_SANS_RISQUE)
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

    series = series_mensuelles()
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

# Combien de seances on accepte de mettre pour sortir d'une position. Au-dela,
# la ligne est un piege : elle se detient bien et ne se vend pas.
SEANCES_DE_SORTIE_ACCEPTABLES = 5


@_maybe_cache_data(ttl=300)
def candidats_amelioration(positions: tuple, cash: float = 0.0,
                           seuil_illiquidite: Optional[float] = None) -> Optional[dict]:
    """Quels titres ameliorent le couple rendement-risque du portefeuille.

    LA REGLE. Ajouter une petite quantite d'un titre ameliore le portefeuille
    si son rendement en exces du sans-risque, rapporte a sa COVARIANCE avec le
    portefeuille, depasse le meme rapport calcule sur le portefeuille lui-meme.
    Ce n'est pas la volatilite du titre qui compte, c'est la part de cette
    volatilite qui s'ajoute a celle qu'on porte deja : un titre agite mais
    decorrele peut reduire le risque de l'ensemble.

    Filtisac l'illustre sur un portefeuille reel : 61 % de volatilite propre,
    mais une covariance NEGATIVE avec le portefeuille. Il monte quand le reste
    descend.

    TROIS RESERVES, et elles sont serieuses.

    Le rendement passe n'est pas le rendement attendu. Ce classement dit ce qui
    AURAIT ameliore le portefeuille sur cinq ans, pas ce qui l'ameliorera. Il
    designe des candidats a examiner, il ne decide de rien.

    Soixante observations pour quarante-cinq titres : un optimiseur de Markowitz
    complet produirait ici des poids extremes et instables, en ajustant la
    matrice de covariance au bruit. Un classement marginal, titre par titre,
    resiste mieux — c'est pourquoi on s'y tient.

    Un titre qui ne cote pas parait decorrele : son cours ne bouge pas quand le
    marche bouge. L'optimiseur recompenserait donc l'illiquidite. Les titres au
    dela du seuil d'immobilite sont ecartes du classement, et la liquidite de
    chaque candidat s'affiche a cote de son rang.
    """
    valeurs = {}
    for ticker, valeur in positions:
        if valeur and valeur > 0:
            valeurs[ticker] = valeurs.get(ticker, 0) + valeur
    total = sum(valeurs.values())
    if not total:
        return None

    series = series_mensuelles()
    mesures = toutes_les_mesures()
    seuil = seuil_illiquidite
    if seuil is None and mesures:
        seuil = next(iter(mesures.values())).get("seuil_illiquidite")

    suivis = [t for t in valeurs if t in series]
    if not suivis:
        return None
    n = min(len(series[t][0]) for t in suivis)
    poids = {t: valeurs[t] / total for t in suivis}
    somme = sum(poids.values())
    poids = {t: p / somme for t, p in poids.items()}

    # La serie du portefeuille lui-meme, mois par mois.
    rendements = {t: series[t][0][-n:] for t in series if len(series[t][0]) >= n}
    portefeuille = [sum(poids[t] * rendements[t][i] for t in suivis)
                    for i in range(n)]
    moyenne_pf = st.mean(portefeuille)
    variance_pf = st.variance(portefeuille)
    if variance_pf <= 0:
        return None
    mensuel_sans_risque = (1 + TAUX_SANS_RISQUE) ** (1 / 12) - 1
    ratio_portefeuille = (moyenne_pf - mensuel_sans_risque) / variance_pf

    candidats = []
    for ticker, serie in rendements.items():
        if ticker in ("BRVMC", "BRVM30"):
            continue
        mesure = mesures.get(ticker) or {}
        echange_titre = mesure.get("montant_echange")
        # Le seuil se regle depuis la page : abaisse, il elargit le champ des
        # candidats ; releve, il ne garde que les valeurs ou l'on entre et sort
        # sans peine. Le voir bouger vaut mieux que le subir.
        trop_etroit = (echange_titre is not None and echange_titre <= seuil
                       if seuil is not None else mesure.get("peu_liquide"))
        if trop_etroit:
            continue                    # sa decorrelation serait un artefact
        covariance = _covariance(serie, portefeuille)
        moyenne = st.mean(serie)
        exces = moyenne - mensuel_sans_risque
        # Covariance negative : le titre amortit le portefeuille. Le rapport
        # n'a alors plus de sens, mais la conclusion est claire.
        if covariance <= 0:
            ratio, decorrele = float("inf"), True
        else:
            ratio, decorrele = exces / covariance, False
        echange = mesure.get("montant_echange") or 0
        # Ce qu'on peut detenir sans etre piege : ce qui se vend en cinq
        # seances au rythme habituel du titre.
        taille_max = (echange / SEANCES_PAR_MOIS
                      * SEANCES_DE_SORTIE_ACCEPTABLES) if echange else None
        candidats.append({
            "ticker": ticker,
            "detenu": ticker in valeurs,
            "poids": poids.get(ticker),
            "ratio": ratio,
            "decorrele": decorrele,
            "ameliore": (decorrele or ratio > ratio_portefeuille) and exces > 0,
            "rendement_annuel": (1 + moyenne) ** 12 - 1,
            "covariance": covariance,
            "correlation": (covariance / (st.stdev(serie)
                                          * st.stdev(portefeuille))
                            if st.stdev(serie) else None),
            "montant_echange": echange or None,
            "taille_max": taille_max,
            "volatilite": mesure.get("volatilite"),
        })
    candidats.sort(key=lambda c: (-c["ratio"], c["ticker"]))

    ecartes = sorted(t for t, m in mesures.items()
                     if (m.get("montant_echange") is not None
                         and seuil is not None
                         and m["montant_echange"] <= seuil))
    return {
        "ratio_portefeuille": ratio_portefeuille,
        "rendement_portefeuille": (1 + moyenne_pf) ** 12 - 1,
        "candidats": candidats,
        "ecartes_illiquides": ecartes,
        "cash": cash,
        "observations": n,
        "seuil_illiquidite": seuil,
    }

def _qualite(ticker: str, scores: dict) -> tuple:
    """Ce que le modele pense de la societe, pour ne pas acheter n'importe quoi.

    Le classement marginal ne regarde que le couple rendement-risque PASSE. Un
    titre peut y bien figurer et etre une societe en perdition : Oragroup a un
    rendement negatif, Onatel distribue plus qu'elle ne gagne. Croiser les deux
    evite de recommander un titre que le modele juge par ailleurs mauvais.

    Rend (score sur 100, verdict).
    """
    ligne = scores.get(ticker) or {}
    return (ligne.get("hybrid_score") or 0), (ligne.get("verdict") or "")


# Combien de candidats on met en concurrence. Au-dela, le nombre de
# combinaisons explose sans rien apprendre : les titres de rang douze n'entrent
# jamais dans une repartition de trois lignes.
CANDIDATS_EN_LICE = 8


def _table_covariance(rendements):
    """La matrice de covariance, calculee UNE fois.

    Sans elle, chaque combinaison essayee refait la serie du portefeuille mois
    par mois : quatre-vingt-douze combinaisons coutaient dix-huit secondes,
    soit une page inutilisable. La variance d'un portefeuille est une forme
    quadratique sur cette matrice — une fois qu'on l'a, chaque essai se calcule
    en quelques multiplications.
    """
    moyennes = {t: st.mean(r) for t, r in rendements.items()}
    tickers = sorted(rendements)
    n = len(next(iter(rendements.values())))
    centres = {t: [x - moyennes[t] for x in rendements[t]] for t in tickers}
    cov = {}
    for i, a in enumerate(tickers):
        for b in tickers[i:]:
            v = sum(x * y for x, y in zip(centres[a], centres[b])) / (n - 1)
            cov[(a, b)] = cov[(b, a)] = v
    return cov, moyennes


def _volatilite_et_rendement(valeurs, cov, moyennes):
    """Volatilite annualisee et rendement annuel, par la forme quadratique."""
    total = sum(valeurs.values())
    suivis = [t for t in valeurs if t in moyennes]
    if not suivis or total <= 0:
        return None, None
    poids = {t: valeurs[t] / total for t in suivis}
    somme = sum(poids.values())
    poids = {t: p / somme for t, p in poids.items()}
    variance = sum(poids[a] * poids[b] * cov[(a, b)]
                   for a in suivis for b in suivis)
    moyenne = sum(poids[t] * moyennes[t] for t in suivis)
    if variance <= 0:
        return None, None
    return math.sqrt(variance) * math.sqrt(12), (1 + moyenne) ** 12 - 1


@_maybe_cache_data(ttl=300)
def allocation_suggeree(positions: tuple, cash: float, scores: tuple,
                        seuil_illiquidite: Optional[float] = None,
                        nombre_max: int = 3) -> Optional[dict]:
    """La meilleure repartition du cash — sur une, deux ou trois lignes.

    ON N'EST PAS OBLIGE D'ALLER JUSQU'A TROIS. Trois lignes valent mieux que
    deux si elles se decorrelent ; elles valent moins si la troisieme ne fait
    que diluer. Le modele essaie donc TOUTES les combinaisons d'une, deux et
    trois lignes parmi les meilleurs candidats, simule chacune, et retient
    celle qui donne le meilleur rendement par unite de risque.

    Deux conditions pour entrer en lice, et les deux comptent :

      le titre doit AMELIORER le couple rendement-risque du portefeuille ;
      et le modele doit le juger correct — un bon rapport rendement-risque
      passe ne fait pas une bonne societe.

    Le resultat n'est pas une consigne mais une SIMULATION : il affiche le
    portefeuille avant et apres, et les combinaisons rivales, pour que le choix
    se juge sur des chiffres plutot que sur une autorite.
    """
    scores = dict(scores)
    base = candidats_amelioration(positions, cash, seuil_illiquidite)
    if not base or cash <= 0:
        return None

    eligibles = []
    for c in base["candidats"]:
        if not c["ameliore"]:
            continue
        score, verdict = _qualite(c["ticker"], scores)
        if "VENTE" in verdict.upper() or "ÉVITER" in verdict.upper():
            continue
        if score and score < 50:
            continue
        eligibles.append({**c, "score": score, "verdict": verdict})
        if len(eligibles) >= CANDIDATS_EN_LICE:
            break
    if not eligibles:
        return {"lignes": [], "essais": [],
                "raison": "aucun candidat ne réunit les deux conditions"}

    valeurs = {}
    for ticker, valeur in positions:
        if valeur and valeur > 0:
            valeurs[ticker] = valeurs.get(ticker, 0) + valeur

    series = series_mensuelles()
    concernes = set(valeurs) | {c["ticker"] for c in eligibles}
    suivis = [t for t in concernes if t in series]
    n = min(len(series[t][0]) for t in suivis)
    rendements = {t: series[t][0][-n:] for t in suivis}

    cov, moyennes = _table_covariance(rendements)
    mensuel_sans_risque = (1 + TAUX_SANS_RISQUE) ** (1 / 12) - 1
    vol_avant, rdt_avant = _volatilite_et_rendement(valeurs, cov, moyennes)

    def _repartir(combinaison):
        """Le cash reparti au prorata du rang marginal, borne par la liquidite."""
        fini = [c["ratio"] for c in combinaison if c["ratio"] != float("inf")]
        plafond = max(fini) if fini else 1.0
        bruts = {c["ticker"]: (plafond if c["ratio"] == float("inf")
                               else c["ratio"]) for c in combinaison}
        somme = sum(bruts.values()) or 1.0
        lignes = []
        for c in combinaison:
            montant = cash * bruts[c["ticker"]] / somme
            limite = c.get("taille_max")
            borne = limite is not None and montant > limite
            lignes.append({**c, "montant": limite if borne else montant,
                           "borne_par_liquidite": borne})
        return lignes

    essais = []
    for taille in range(1, min(nombre_max, len(eligibles)) + 1):
        for combinaison in itertools.combinations(eligibles, taille):
            lignes = _repartir(list(combinaison))
            apres = dict(valeurs)
            for l in lignes:
                apres[l["ticker"]] = apres.get(l["ticker"], 0) + l["montant"]
            vol, rdt = _volatilite_et_rendement(apres, cov, moyennes)
            if not vol:
                continue
            essais.append({
                "tickers": [l["ticker"] for l in lignes],
                "lignes": lignes,
                "volatilite": vol,
                "rendement": rdt,
                # Le rendement par unite de risque : c'est lui qu'on maximise.
                "sharpe": ((1 + rdt) ** (1 / 12) - 1 - mensuel_sans_risque)
                          * 12 / vol,
                "place": sum(l["montant"] for l in lignes),
            })
    if not essais:
        return {"lignes": [], "essais": [], "raison": "aucune simulation possible"}
    essais.sort(key=lambda e: -e["sharpe"])
    meilleur = essais[0]
    # Quand plusieurs combinaisons se tiennent a moins d'un pour cent, les
    # departager serait arbitraire — mieux vaut le dire. A egalite, la plus
    # SIMPLE gagne : moins de lignes, moins de frais et moins a surveiller.
    proches = [e for e in essais
               if meilleur["sharpe"] - e["sharpe"] <= abs(meilleur["sharpe"]) * 0.01]
    if len(proches) > 1:
        meilleur = min(proches, key=lambda e: (len(e["tickers"]), -e["sharpe"]))

    return {
        "lignes": meilleur["lignes"],
        "essais": essais[:6],
        "nb_essais": len(essais),
        "cash": cash,
        "place": meilleur["place"],
        "reste": cash - meilleur["place"],
        "volatilite_avant": vol_avant,
        "volatilite_apres": meilleur["volatilite"],
        "rendement_avant": rdt_avant,
        "rendement_apres": meilleur["rendement"],
        "sharpe_avant": ((1 + rdt_avant) ** (1 / 12) - 1 - mensuel_sans_risque)
                        * 12 / vol_avant if vol_avant else None,
        "sharpe_apres": meilleur["sharpe"],
        "eligibles": len(eligibles),
        "ex_aequo": len(proches),
    }
