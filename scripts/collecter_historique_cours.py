#!/usr/bin/env python3
"""Collecte l'historique de cours de toute la cote, depuis sikafinance.

Pourquoi : `price_cache` ne porte qu'une mediane de 208 seances par titre sur
cinq ans — des releves epars, pris au fil des passages du robot, pas une serie.
On ne calcule pas une correlation sur des dates qui ne coincident pas, ni une
performance comparee sur des series trouees. Un point mensuel par titre, aux
memes dates pour tous, rend les titres comparables entre eux et a l'indice.

La source : la page « historiques » de sikafinance interroge
`POST /api/general/GetHistos` avec `{ticker, datedeb, datefin, xperiod}`, ou
xperiod vaut 0 pour le journalier, 7 pour l'hebdomadaire, 30 pour le mensuel,
91 pour le trimestriel et 365 pour l'annuel. Le journalier est plafonne a trois
mois par requete ; le mensuel ne l'est pas.

Les INDICES passent par la meme porte : « BRVMC » et « BRVM30 » repondent.
« BRVMPRES » n'existe pas cote sikafinance — l'indice des prestiges n'y est pas
publie, et le demander rend « nodata ».

Deux cadences sont utiles et ne servent pas a la meme chose :

  MENSUELLE sur cinq ans — soixante et un points, assez pour une correlation
            et une performance comparee sans que le bruit d'une seance isolee
            ne domine ;
  TRIMESTRIELLE sur dix ans — quarante points, la seule cadence qui remonte
            assez loin pour voir un cycle, et la seule qui coincide avec le
            rythme des publications de la cote.

Usage :
    python3 scripts/collecter_historique_cours.py                     # mensuel, 5 ans
    python3 scripts/collecter_historique_cours.py --periode=T --annees=10
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_tickers          # noqa: E402
from data.db import get_connection       # noqa: E402

API = "https://www.sikafinance.com/api/general/GetHistos"
NAVIGATEUR = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

# Les indices que sikafinance publie. Ils sont ranges avec les titres : un
# indice se compare a un titre, et les separer obligerait a joindre deux tables
# pour la moindre correlation.
INDICES = ("BRVMC", "BRVM30")

# Ce que sikafinance attend dans `xperiod`, le nom de la cadence, et la table
# qui l'accueille. Deux tables plutot qu'une seule a colonne de cadence : les
# deux series ne se lisent jamais ensemble — cinq ans de points mensuels pour
# une correlation, dix ans de points trimestriels pour un cycle — et les
# melanger dans une requete donnerait des doublons de dates silencieux.
CADENCES = {
    "M": ("30", "mensuelle", "price_monthly"),
    "T": ("91", "trimestrielle", "price_quarterly"),
}


def _demander(ticker: str, debut: str, fin: str, xperiod: str,
              essais: int = 3) -> list:
    corps = json.dumps({"ticker": ticker, "datedeb": debut, "datefin": fin,
                        "xperiod": xperiod}).encode()
    entetes = {
        "User-Agent": NAVIGATEUR,
        "Content-Type": "application/json;charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://www.sikafinance.com/marches/historiques/{ticker}",
    }
    for tentative in range(essais):
        try:
            requete = urllib.request.Request(API, data=corps, headers=entetes,
                                             method="POST")
            reponse = urllib.request.urlopen(requete, timeout=60)
            donnees = json.loads(reponse.read().decode("utf-8", "replace"))
        except Exception as err:                                # noqa: BLE001
            if tentative == essais - 1:
                raise
            time.sleep(2 * (tentative + 1))
            continue
        if donnees.get("error"):
            # « nodata » n'est pas une panne : le titre n'est pas suivi, ou
            # n'existait pas sur la periode. On le distingue d'un echec reseau.
            return []
        return donnees.get("lst") or []
    return []


def _jour(texte: str):
    """« 07/09/2021 » vers une date. Le jour n'est pas toujours le premier du
    mois : sikafinance rend la premiere seance, pas le premier du calendrier."""
    j, m, a = texte.split("/")
    return date(int(a), int(m), int(j))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--periode", choices=sorted(CADENCES), default="M",
                    help="M mensuelle, T trimestrielle")
    ap.add_argument("--annees", type=int, default=None,
                    help="profondeur ; 5 ans en mensuel, 10 en trimestriel")
    ap.add_argument("--simuler", action="store_true")
    ap.add_argument("--pause", type=float, default=0.7)
    args = ap.parse_args()

    xperiod, nom_cadence, table = CADENCES[args.periode]
    annees = args.annees if args.annees else (5 if args.periode == "M" else 10)
    fin = date.today()
    debut = fin - timedelta(days=int(annees * 365.25))
    tickers = sorted({t if isinstance(t, str) else t.get("ticker")
                      for t in load_tickers()})
    cibles = list(INDICES) + tickers
    print(f"{len(cibles)} series demandees, du {debut} au {fin}, "
          f"en cadence {nom_cadence} · table {table}")

    cnx = get_connection()
    if not args.simuler:
        cnx.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                ticker  TEXT NOT NULL,
                date    DATE NOT NULL,
                open    DOUBLE PRECISION,
                high    DOUBLE PRECISION,
                low     DOUBLE PRECISION,
                close   DOUBLE PRECISION,
                volume  DOUBLE PRECISION,
                PRIMARY KEY (ticker, date)
            )""")
        cnx.commit()

    ecrits = vides = echecs = 0
    for rang, ticker in enumerate(cibles, 1):
        try:
            points = _demander(ticker, debut.isoformat(), fin.isoformat(), xperiod)
        except Exception as err:                                # noqa: BLE001
            echecs += 1
            print(f"  [{rang:2d}/{len(cibles)}] {ticker:10s} ECHEC {str(err)[:60]}")
            continue
        if not points:
            vides += 1
            print(f"  [{rang:2d}/{len(cibles)}] {ticker:10s} aucune donnee")
            continue
        if not args.simuler:
            for p in points:
                cnx.execute(
                    f"INSERT INTO {table} (ticker, date, open, high, low, "
                    f"close, volume) VALUES (%s,%s,%s,%s,%s,%s,%s) "
                    f"ON CONFLICT (ticker, date) DO UPDATE SET "
                    f"open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                    f"close=EXCLUDED.close, volume=EXCLUDED.volume",
                    (ticker, _jour(p["Date"]), p.get("Open"), p.get("High"),
                     p.get("Low"), p.get("Close"), p.get("Volume")))
            cnx.commit()
        ecrits += len(points)
        print(f"  [{rang:2d}/{len(cibles)}] {ticker:10s} {len(points):3d} points "
              f"· {points[0]['Date']} -> {points[-1]['Date']}")
        time.sleep(args.pause)
    cnx.close()

    print(f"\n{ecrits} points · {vides} serie(s) vide(s) · {echecs} echec(s)")
    return 1 if echecs else 0


if __name__ == "__main__":
    sys.exit(main())
