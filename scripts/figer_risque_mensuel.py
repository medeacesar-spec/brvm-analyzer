#!/usr/bin/env python3
"""Fige les mesures de risque du mois, pour qu'elles fassent serie.

Le risque etait recalcule a chaque affichage et jamais conserve. Il ne s'en
gardait donc aucune trace : impossible de dire si un titre s'est calme ou
agite depuis un an, ni de croiser son risque avec son score, son rendement ou
son secteur. Une mesure qu'on ne garde pas ne se compare qu'a elle-meme.

Une fois par mois suffit, et ce n'est pas une economie : la source est
mensuelle, et une volatilite calculee sur soixante mois ne bouge pas d'un jour
a l'autre. La figer chaque jour donnerait soixante fois la meme valeur en
laissant croire a une serie.

Les RANGS sont figes avec les valeurs. Sans eux, comparer deux dates
obligerait a recalculer tout le classement du passe — et un rang depend de la
cote telle qu'elle etait ce mois-la, pas telle qu'elle est aujourd'hui.

Usage :
    python3 scripts/figer_risque_mensuel.py [--simuler]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.risque import profil_de_risque                    # noqa: E402
from config import load_tickers                                 # noqa: E402
from data.db import get_connection                              # noqa: E402

TABLE = """
CREATE TABLE IF NOT EXISTS risque_mensuel (
    ticker               TEXT NOT NULL,
    mois                 DATE NOT NULL,
    secteur              TEXT,
    volatilite           DOUBLE PRECISION,
    semi_volatilite      DOUBLE PRECISION,
    asymetrie            DOUBLE PRECISION,
    perte_maximale       DOUBLE PRECISION,
    mois_recuperation    INTEGER,
    sharpe               DOUBLE PRECISION,
    sortino              DOUBLE PRECISION,
    rendement_annualise  DOUBLE PRECISION,
    part_mois_immobiles  DOUBLE PRECISION,
    montant_echange      DOUBLE PRECISION,
    impact_transaction   DOUBLE PRECISION,
    observations         INTEGER,
    rang_volatilite      INTEGER,
    rang_rendement       INTEGER,
    rang_sharpe          INTEGER,
    effectif             INTEGER,
    fige_le              TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, mois)
)
"""

CHAMPS = ("volatilite", "semi_volatilite", "asymetrie", "perte_maximale",
          "mois_recuperation", "sharpe", "sortino", "rendement_annualise",
          "part_mois_immobiles", "montant_echange", "impact_transaction",
          "observations")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--simuler", action="store_true")
    args = ap.parse_args()

    # Le premier du mois courant : deux passages le meme mois ecrasent la
    # meme ligne plutot que d'en creer une seconde.
    aujourdhui = date.today()
    mois = date(aujourdhui.year, aujourdhui.month, 1)

    cnx = get_connection()
    if not args.simuler:
        cnx.execute(TABLE)
        cnx.commit()

    tickers = sorted({t if isinstance(t, str) else t.get("ticker")
                      for t in load_tickers()})
    ecrits, sans = 0, []
    for ticker in tickers:
        try:
            profil = profil_de_risque(ticker)
        except Exception as err:                                # noqa: BLE001
            print(f"  {ticker:10s} ECHEC {str(err)[:60]}")
            continue
        if not profil:
            sans.append(ticker)
            continue
        m = profil["titre"]
        rangs = {c: (profil["situations"].get(c, {}).get("marché") or {})
                 for c in ("volatilite", "rendement_annualise", "sharpe")}
        effectif = (rangs["volatilite"].get("effectif")
                    or rangs["rendement_annualise"].get("effectif"))
        valeurs = ([ticker, mois, profil["secteur"]]
                   + [m.get(c) for c in CHAMPS]
                   + [rangs["volatilite"].get("rang"),
                      rangs["rendement_annualise"].get("rang"),
                      rangs["sharpe"].get("rang"), effectif])
        if not args.simuler:
            colonnes = ("ticker, mois, secteur, " + ", ".join(CHAMPS)
                        + ", rang_volatilite, rang_rendement, rang_sharpe, effectif")
            trous = ", ".join(["%s"] * len(valeurs))
            maj = ", ".join(f"{c}=EXCLUDED.{c}" for c in
                            ("secteur",) + CHAMPS + ("rang_volatilite",
                             "rang_rendement", "rang_sharpe", "effectif"))
            cnx.execute(f"INSERT INTO risque_mensuel ({colonnes}) VALUES ({trous}) "
                        f"ON CONFLICT (ticker, mois) DO UPDATE SET {maj}, "
                        f"fige_le=NOW()", tuple(valeurs))
        ecrits += 1
    if not args.simuler:
        cnx.commit()
        histo = [dict(r) for r in cnx.execute(
            "SELECT mois::text AS m, count(*) n FROM risque_mensuel "
            "GROUP BY mois ORDER BY mois")]
    else:
        histo = []
    cnx.close()

    print(f"{ecrits} titre(s) figes au {mois}")
    if sans:
        print(f"{len(sans)} sans historique suffisant : {', '.join(sans)}")
    for h in histo:
        print(f"   {h['m']} · {h['n']} titres")
    return 0


if __name__ == "__main__":
    sys.exit(main())
