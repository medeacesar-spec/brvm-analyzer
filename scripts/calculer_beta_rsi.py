#!/usr/bin/env python3
"""Calcule le beta et le RSI de chaque titre, et les ecrit dans market_data.

Les colonnes `market_data.beta` et `market_data.rsi` existaient depuis
l'origine et n'ont jamais ete remplies : quarante-huit lignes vides, que le
tableau des cotations affichait sans le dire. Ce script les alimente.

DEUX MESURES, DEUX SERIES, DEUX PIEGES DIFFERENTS.

**Le beta se mesure au MOIS.** Il repond a « de combien bouge le titre quand
le Composite bouge d'un point ». Le tenter au jour n'aurait pas de sens ici :
un titre qui ne cote pas garde son dernier cours, sa serie quotidienne est
surtout faite de zeros, et le beta tomberait vers zero pour la seule raison
que le titre est calme. Le calcul reprend donc les rendements mensuels
TOTAUX de `analysis.risque` — dividendes compris, mois de versement reel — et
le BRVM Composite comme marche. Vingt-quatre mois minimum, comme le reste du
module.

Le piege demeure au mois, en plus doux : les mois immobiles tirent le beta
vers zero. La correlation est donc calculee et ecrite avec lui — sur cette
place elle est FAIBLE (0,05 a 0,69 en septembre 2026), ce qui veut dire que le
marche explique peu des mouvements et qu'un beta seul tromperait.

**Le RSI se mesure au JOUR.** C'est un indicateur de momentum court : le
lisser au mois le viderait de son sens. Il se lit donc dans `price_cache`,
sur la formule de Wilder deja implementee dans `analysis.technical`. Deux
garde-fous : quinze cotations minimum, et une derniere cotation de moins de
trente jours — un RSI calcule sur un cours arrete depuis six mois decrit un
marche qui n'existe plus.

Usage :
    python3 scripts/calculer_beta_rsi.py [--simuler]
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection                              # noqa: E402

RSI_PERIODE = 14
RSI_MINIMUM_POINTS = RSI_PERIODE + 1
RSI_FRAICHEUR_JOURS = 30


def _rsi_wilder(closes: list, periode: int = RSI_PERIODE):
    """RSI de Wilder, sans pandas : le script tourne aussi en atelier.

    Reprend trait pour trait `analysis.technical._compute_rsi` — moyenne
    simple sur la premiere fenetre, puis lissage exponentiel de Wilder.
    """
    if len(closes) < periode + 1:
        return None
    gains, pertes = [], []
    for i in range(1, len(closes)):
        ecart = closes[i] - closes[i - 1]
        gains.append(max(ecart, 0.0))
        pertes.append(max(-ecart, 0.0))
    moy_g = sum(gains[:periode]) / periode
    moy_p = sum(pertes[:periode]) / periode
    for i in range(periode, len(gains)):
        moy_g = (moy_g * (periode - 1) + gains[i]) / periode
        moy_p = (moy_p * (periode - 1) + pertes[i]) / periode
    if moy_p == 0:
        # Que des hausses sur la fenetre : le RSI sature a 100, il ne se
        # divise pas par zero.
        return 100.0 if moy_g > 0 else None
    rs = moy_g / moy_p
    return 100 - (100 / (1 + rs))


def calculer_rsi(cnx) -> tuple:
    """RSI du dernier point de chaque titre, plus les titres ecartes."""
    cours = defaultdict(list)
    for ligne in cnx.execute(
            "SELECT ticker, date, close FROM price_cache "
            "WHERE close > 0 ORDER BY ticker, date"):
        ligne = dict(ligne)
        cours[ligne["ticker"]].append((ligne["date"], float(ligne["close"])))

    aujourdhui = max((p[-1][0] for p in cours.values()), default=None)
    if aujourdhui is None:
        return {}, {}
    if not isinstance(aujourdhui, date):
        aujourdhui = date.fromisoformat(str(aujourdhui)[:10])

    valeurs, ecartes = {}, {}
    for ticker, points in cours.items():
        derniere = points[-1][0]
        if not isinstance(derniere, date):
            derniere = date.fromisoformat(str(derniere)[:10])
        if len(points) < RSI_MINIMUM_POINTS:
            ecartes[ticker] = f"{len(points)} cotations, il en faut {RSI_MINIMUM_POINTS}"
            continue
        if aujourdhui - derniere > timedelta(days=RSI_FRAICHEUR_JOURS):
            ecartes[ticker] = f"derniere cotation le {derniere}, trop ancienne"
            continue
        r = _rsi_wilder([c for _, c in points])
        if r is None:
            ecartes[ticker] = "serie sans variation exploitable"
            continue
        valeurs[ticker] = round(r, 1)
    return valeurs, ecartes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--simuler", action="store_true")
    args = ap.parse_args()

    from analysis.risque import toutes_les_mesures
    mesures = toutes_les_mesures()
    betas = {t: v["beta"] for t, v in mesures.items() if v.get("beta") is not None}
    print(f"beta : {len(betas)} titres sur {len(mesures)} mesures")
    faibles = [t for t, v in mesures.items()
               if v.get("correlation_marche") is not None
               and abs(v["correlation_marche"]) < 0.30]
    print(f"       dont {len(faibles)} avec une correlation au marche < 0,30 "
          f"— leur beta ne decrit presque rien")

    cnx = get_connection()
    rsi, ecartes = calculer_rsi(cnx)
    print(f"RSI  : {len(rsi)} titres · {len(ecartes)} ecartes")
    for t, motif in sorted(ecartes.items()):
        print(f"       {t:12s} {motif}")

    ecrits = 0
    for ticker in sorted(set(betas) | set(rsi)):
        b = betas.get(ticker)
        r = rsi.get(ticker)
        if not args.simuler:
            cnx.execute(
                "UPDATE market_data SET beta=%s, rsi=%s, updated_at=NOW() "
                "WHERE ticker=%s",
                (round(b, 3) if b is not None else None, r, ticker))
        ecrits += 1
    if not args.simuler:
        cnx.commit()
    cnx.close()

    verbe = "a ecrire" if args.simuler else "ecrits"
    print(f"\n{ecrits} titres {verbe}")
    if args.simuler:
        print("(simulation — rien n'a ete ecrit)")


if __name__ == "__main__":
    main()
