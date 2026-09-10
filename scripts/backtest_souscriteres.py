#!/usr/bin/env python3
"""Les poids INTERNES à chaque score valent-ils quelque chose ?

Le partage entre fondamental et technique a déjà été mesuré
(`backtest_ponderation.py`). Ce script descend d'un cran : à l'intérieur de
chaque périmètre, il demande si les six — puis les quatre — sous-critères
méritent les points qu'on leur donne.

  technique : tendance 15 · RSI 10 · MACD 10 · Bollinger 5 · volume 5 · momentum 5
  fondamental : rentabilité 15 · endettement 10 · valorisation 15 · dividendes 10

Ces barèmes n'ont jamais été mesurés. Ils ont été posés — raisonnablement,
mais posés. La question est simple : la tendance mérite-t-elle trois fois le
MACD ?

CE QU'ON MESURE

D'abord chaque sous-critère SEUL, par sa corrélation de rang (rho) avec le
rendement à venir, relatif au Composite. Un sous-critère dont le rho est nul
ne range rien : ses points sont du bruit, quel que soit leur nombre.

Ensuite le barème complet, comparé à trois alternatives : poids égaux, poids
proportionnels au rho mesuré, et le meilleur barème trouvé par recherche.

LE PIÈGE, LE MÊME QUE POUR LA PONDÉRATION GLOBALE

Chercher les meilleurs poids sur toutes les données, c'est trouver ceux qui
racontent le mieux le passé. Tout est donc mesuré DEUX FOIS : appris avant
2024, vérifié sur 2024 et après — des années que la recherche n'a jamais
vues. Un barème qui gagne à l'apprentissage et perd à la vérification est du
surapprentissage, et il faut le dire plutôt que de le publier.

UNE PRÉCAUTION DE LECTURE

Un sous-critère prend peu de valeurs distinctes (le MACD n'en prend que
quatre : 2, 5, 7, 10). Son rho est donc mécaniquement écrasé par les ex
aequo : il est comparable aux autres sous-critères, pas au rho d'un score
continu. On lit le CLASSEMENT des sous-critères, pas la valeur absolue.

Usage :
  python3 scripts/backtest_souscriteres.py
  python3 scripts/backtest_souscriteres.py --horizon 6
"""
from __future__ import annotations

import argparse
import importlib.util as _u
import itertools
import os
import random
import statistics as st
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _voisin(nom):
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)), nom + ".py")
    spec = _u.spec_from_file_location(nom, chemin)
    module = _u.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


COUPURE = 2024              # apprentissage avant, verification a partir de
MINIMUM = 50                # en deca, aucune mesure n'est publiee

# Les baremes en vigueur, tels que le code les applique aujourd'hui.
BAREME_TECHNIQUE = {"tendance": 15, "rsi": 10, "macd": 10,
                    "bollinger": 5, "volume": 5, "momentum": 5}
BAREME_FONDAMENTAL = {"rentabilite": 15, "endettement": 10,
                      "valorisation": 15, "dividendes": 10}

# Le maximum que chaque sous-critere peut marquer : c'est par lui qu'on
# normalise avant de re-ponderer, sinon un sous-critere note sur 15 pese
# trois fois un sous-critere note sur 5 avant meme qu'on lui donne un poids.
PLAFOND = dict(BAREME_TECHNIQUE)
PLAFOND.update(BAREME_FONDAMENTAL)

# Poids testes par la recherche exhaustive. Volontairement grossiers : la
# donnee ne porte pas une precision au point de pourcentage, et un pas fin
# ne ferait qu'augmenter les chances de tomber sur un optimum de hasard.
PALIERS = (0, 1, 2, 3)


# ---------------------------------------------------------------- statistique

def _rangs(valeurs: list) -> list:
    ordre = sorted(range(len(valeurs)), key=lambda i: valeurs[i])
    rangs = [0.0] * len(valeurs)
    i = 0
    while i < len(ordre):
        j = i
        while j + 1 < len(ordre) and valeurs[ordre[j + 1]] == valeurs[ordre[i]]:
            j += 1
        moyen = (i + j) / 2 + 1
        for k in range(i, j + 1):
            rangs[ordre[k]] = moyen
        i = j + 1
    return rangs


def _rho(a: list, b: list) -> float:
    """Corrélation de rang de Spearman."""
    if len(a) < 3:
        return 0.0
    ra, rb = _rangs(a), _rangs(b)
    ma, mb = st.mean(ra), st.mean(rb)
    num = sum((x - ma) * (y - mb) for x, y in zip(ra, rb))
    den = (sum((x - ma) ** 2 for x in ra) * sum((y - mb) ** 2 for y in rb)) ** 0.5
    return num / den if den else 0.0


def _ecart_haut_bas(scores: list, rendements: list) -> tuple:
    """Médiane du cinquième supérieur moins celle du cinquième inférieur.

    CORRECTION DU 10/09/2026 — le tri departageait les ex aequo par le
    rendement. Un sous-critere ne prend que quatre a neuf valeurs distinctes :
    des centaines de decisions partagent le meme score, et `sorted(zip(...))`
    les rangeait alors du plus mauvais rendement au meilleur. Le « cinquieme
    superieur » etait donc choisi en partie sur le resultat qu'il pretendait
    mesurer, ce qui produisait des ecarts de 90 points sans aucun sens. Les
    ex aequo sont desormais departages par un tirage fixe, independant du
    rendement, et le tirage est le meme d'une execution a l'autre.
    """
    alea = random.Random(20260910)
    ordre = sorted(range(len(scores)), key=lambda i: (scores[i], alea.random()))
    taille = max(len(ordre) // 5, 1)
    bas = [rendements[i] for i in ordre[:taille]]
    haut = [rendements[i] for i in ordre[-taille:]]
    return (st.median(haut) - st.median(bas),
            sum(1 for r in haut if r > 0) / len(haut))


# ------------------------------------------------------------------- collecte

def collecter(horizon: int) -> tuple:
    """Deux lots : [(detail, rendement_relatif, annee)] technique et fondamental."""
    tech = _voisin("backtest_technique")
    fond = _voisin("backtest_fondamental")

    lot_t = []
    for o in tech.observations():
        r = o["relatifs"].get(horizon)
        if r is not None and o["detail"].get("suffisant"):
            lot_t.append((o["detail"], r, o["jour"].year))

    lot_f = []
    for o in fond.observations():
        r = o["relatifs"].get(horizon)
        if r is not None and o["detail"]:
            lot_f.append((o["detail"], r, o["jour"].year))

    return lot_t, lot_f


# -------------------------------------------------------------------- mesures

def rho_solo(lot: list, critere: str) -> tuple:
    """(rho, n, couverture) du sous-critère pris seul.

    Les décisions où le sous-critère est absent (None) sont écartées, pas
    comptées zéro : une donnée manquante n'est pas un mauvais score.
    """
    paires = [(d[critere], r) for d, r, _ in lot if d.get(critere) is not None]
    if len(paires) < MINIMUM:
        return None, len(paires), len(paires) / max(len(lot), 1)
    s = [p for p, _ in paires]
    y = [r for _, r in paires]
    distinctes = len(set(s))
    return _rho(s, y), len(paires), distinctes


def score_pondere(detail: dict, poids: dict) -> float | None:
    """Recompose un score à partir de poids arbitraires.

    Chaque sous-critère est d'abord ramené à [0, 1] par son plafond, puis
    multiplié par son poids. Sans cette normalisation, changer les poids ne
    changerait rien : le barème d'origine est déjà encodé dans les points.
    """
    total, somme = 0.0, 0.0
    for critere, p in poids.items():
        if p == 0:
            continue
        v = detail.get(critere)
        if v is None:
            continue
        total += p * (v / PLAFOND[critere])
        somme += p
    return (total / somme) if somme else None


def mesurer(lot: list, poids: dict) -> dict | None:
    apparies = [(score_pondere(d, poids), r) for d, r, _ in lot]
    apparies = [(s, r) for s, r in apparies if s is not None]
    if len(apparies) < MINIMUM:
        return None
    s = [x for x, _ in apparies]
    y = [r for _, r in apparies]
    ecart, gagn = _ecart_haut_bas(s, y)
    return {"n": len(s), "rho": _rho(s, y), "ecart": ecart, "haut_gagnant": gagn}


def chercher(lot: list, criteres: list) -> dict:
    """Le barème qui maximise rho, par recherche exhaustive sur PALIERS."""
    meilleur, best = None, -9.0
    for combinaison in itertools.product(PALIERS, repeat=len(criteres)):
        if not any(combinaison):
            continue
        poids = dict(zip(criteres, combinaison))
        m = mesurer(lot, poids)
        if m and m["rho"] > best:
            best, meilleur = m["rho"], poids
    return meilleur


# ------------------------------------------------------------------ affichage

def _titre(t):
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}")


def _table_solo(lot: list, bareme: dict, nom: str):
    """Chaque sous-critère seul, ET la stabilité de son rho dans le temps.

    La colonne « avant » / « après » est la plus importante du tableau. Un
    sous-critère dont le rho change de signe entre les deux moitiés de
    l'échantillon ne mesure rien de durable : sa valeur d'ensemble est un
    accident de période, et la re-pondérer serait courir après du bruit.
    """
    appr = [x for x in lot if x[2] < COUPURE]
    hors = [x for x in lot if x[2] >= COUPURE]
    milieu = st.median([a for *_, a in appr]) if appr else 0
    moitie1 = [x for x in appr if x[2] < milieu]
    moitie2 = [x for x in appr if x[2] >= milieu]

    print(f"\n  Chaque sous-critère SEUL — {nom}")
    print(f"  {'sous-critère':>14} {'poids':>7} {'rho':>8} {'décisions':>11} "
          f"{'paliers':>8} {'| rho <' + str(int(milieu)):>12} "
          f"{'rho ≥' + str(int(milieu)):>11} {'rho ≥' + str(COUPURE):>11} "
          f"{'stable':>8}")
    resultats = {}
    for critere, poids in bareme.items():
        rho, n, distinctes = rho_solo(lot, critere)
        resultats[critere] = rho
        if rho is None:
            print(f"  {critere:>14} {poids:>6}p {'—':>8} {n:>11,} "
                  f"{'(trop rare)':>8}")
            continue
        morceaux = [rho_solo(m, critere)[0] for m in (moitie1, moitie2, hors)]
        connus = [r for r in morceaux if r is not None]
        stable = "oui" if connus and all(
            (r > 0) == (rho > 0) for r in connus) else "NON"
        bouts = "".join(f"{r:>11.4f} " if r is not None else f"{'—':>11} "
                        for r in morceaux)
        print(f"  {critere:>14} {poids:>6}p {rho:>8.4f} {n:>11,} "
              f"{distinctes:>8} | {bouts}{stable:>7}")
    return resultats


def _table_baremes(appr: list, hors: list, entier: list, propositions: dict):
    print(f"\n  {'barème':>22} {'rho appr.':>10} {'rho hors':>10} "
          f"{'écart hors':>12} {'haut gagn.':>11}")
    for nom, poids in propositions.items():
        ma, mh = mesurer(appr, poids), mesurer(hors, poids)
        if not ma or not mh:
            print(f"  {nom:>22} {'—':>10} {'—':>10}")
            continue
        print(f"  {nom:>22} {ma['rho']:>10.4f} {mh['rho']:>10.4f} "
              f"{mh['ecart'] * 100:>11.1f} % {mh['haut_gagnant'] * 100:>10.0f}%")
        me = mesurer(entier, poids)
        if me:
            print(f"  {'  (tout l échantillon)':>22} {me['rho']:>10.4f}")


def _detail_poids(poids: dict) -> str:
    return " · ".join(f"{c} {p}" for c, p in poids.items() if p)


def analyser(lot: list, bareme: dict, nom: str):
    if len(lot) < MINIMUM * 2:
        print(f"\n  {nom} : {len(lot)} décisions, trop peu pour conclure.")
        return
    annees = sorted({a for *_, a in lot})
    _titre(f"{nom.upper()} — {len(lot):,} décisions, {annees[0]}-{annees[-1]}")

    rhos = _table_solo(lot, bareme, nom)

    appr = [x for x in lot if x[2] < COUPURE]
    hors = [x for x in lot if x[2] >= COUPURE]
    if len(appr) < MINIMUM or len(hors) < MINIMUM:
        print(f"\n  Découpe {COUPURE} impossible : {len(appr)} / {len(hors)} "
              f"décisions. Pas de vérification hors échantillon.")
        return

    criteres = list(bareme)
    egaux = {c: 1 for c in criteres}
    # Poids proportionnels au rho mesure, les rho negatifs mis a zero :
    # un sous-critere qui range a l'envers ne merite pas d'etre inverse a la
    # legere, il merite d'etre ecarte le temps de comprendre pourquoi.
    positifs = {c: max(0.0, rhos.get(c) or 0.0) for c in criteres}
    plus_fort = max(positifs.values()) or 1.0
    par_rho = {c: round(3 * v / plus_fort) for c, v in positifs.items()}
    if not any(par_rho.values()):
        par_rho = dict(egaux)

    trouve = chercher(appr, criteres)

    negatifs = [c for c in criteres if (rhos.get(c) or 0.0) < 0]
    propositions = {"en vigueur": bareme, "poids égaux": egaux,
                    "au prorata du rho": par_rho}
    if negatifs:
        # Le barème en vigueur, moins les seuls sous-critères qui rangent à
        # l'envers. C'est la modification la plus petite et la plus lisible
        # que les mesures suggèrent : on ne redistribue rien, on retire.
        ampute = {c: (0 if c in negatifs else p) for c, p in bareme.items()}
        propositions["sans " + " ni ".join(negatifs)] = ampute
    if trouve:
        propositions[f"cherché avant {COUPURE}"] = trouve

    print(f"\n  Barèmes comparés — apprentissage < {COUPURE} ({len(appr):,}), "
          f"vérification ≥ {COUPURE} ({len(hors):,})")
    _table_baremes(appr, hors, lot, propositions)

    print(f"\n  au prorata du rho  : {_detail_poids(par_rho)}")
    if trouve:
        print(f"  cherché avant {COUPURE} : {_detail_poids(trouve)}")

    # Le verdict : le bareme cherche bat-il le bareme en vigueur SUR LES
    # ANNEES QU'IL N'A PAS VUES ? C'est la seule comparaison qui engage.
    ref = mesurer(hors, bareme)
    for nom_prop, poids in propositions.items():
        if nom_prop == "en vigueur" or not poids:
            continue
        m = mesurer(hors, poids)
        if not m or not ref:
            continue
        delta = m["rho"] - ref["rho"]
        verdict = "GAGNE" if delta > 0.01 else ("perd" if delta < -0.01
                                                else "équivalent")
        print(f"  hors échantillon, « {nom_prop} » {verdict} "
              f"({delta:+.4f} de rho)")


def main(horizon: int):
    print(f"Calcul des deux séries (horizon {horizon} mois)…")
    lot_t, lot_f = collecter(horizon)
    print("\nRendement RELATIF au Composite. rho = corrélation de rang entre "
          "le score\net le rendement à venir ; zéro signifie « ne range rien ».")
    analyser(lot_t, BAREME_TECHNIQUE, "technique")
    analyser(lot_f, BAREME_FONDAMENTAL, "fondamental")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--horizon", type=int, default=12, choices=(1, 3, 6, 12))
    a = ap.parse_args()
    main(a.horizon)
