#!/usr/bin/env python3
"""Le dividende DECLARE d'un exercice, calcule et non recopie.

DEUX NOTIONS QU'IL NE FAUT PAS CONFONDRE

`dividends_total` vient du tableau des flux de tresorerie : ce sont les
dividendes VERSES pendant l'annee, minoritaires des filiales compris. Chez
Sonatel, l'exercice 2022 porte ainsi 187,3 Md quand le dividende declare aux
actionnaires vaut 166,7 Md — l'ecart est le versement aux minoritaires
d'Orange Mali et consorts.

`dividends_declared` est le dividende DE L'EXERCICE : le dividende par
action, tel que l'avis de paiement de la BRVM le publie, multiplie par le
nombre d'actions. C'est cette notion qu'il faut pour un taux de
distribution.

Les ecraser l'une par l'autre donnerait une serie ou chaque ligne repond a
une question differente. Elles vivent donc dans deux colonnes.

CE QUE CELA DEBLOQUE

Le dividende verse n'etait renseigne que pour 6 % des exercices 2021-2025,
parce qu'il faut ouvrir le tableau des flux pour le lire. Le dividende
declare, lui, se calcule : la BRVM publie 414 avis de paiement de 2015 a
2025, et le nombre d'actions est connu pour 89 % des exercices.

LES ALERTES

Le script ne se contente pas d'ecrire : il SIGNALE tout exercice ou les deux
notions s'ecartent de plus de 2 %. Un ecart de quelques pour cent s'explique
(minoritaires, arrondis) ; un facteur cent est une erreur d'echelle. La
liste est courte a dessein — elle doit pouvoir se verifier a la main.

Usage :
  python3 scripts/dividendes_declares.py
  python3 scripts/dividendes_declares.py --ecrire
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, read_sql_df  # noqa: E402

TOLERANCE = 0.02
DEBUT = 2015


def colonne(cnx) -> None:
    """Cree `dividends_declared` si elle manque."""
    try:
        cnx.execute("SELECT dividends_declared FROM fundamentals LIMIT 1").fetchone()
    except Exception:                                            # noqa: BLE001
        try:
            cnx.rollback()
        except Exception:                                        # noqa: BLE001
            pass
        cnx.execute("ALTER TABLE fundamentals ADD COLUMN dividends_declared REAL")
        cnx.commit()
        print("colonne dividends_declared creee")


def main(ecrire: bool) -> None:
    d = read_sql_df(
        "SELECT ticker, fiscal_year, dps, shares, dividends_total, net_income "
        "FROM fundamentals WHERE fiscal_year >= ?", params=(DEBUT,))
    cnx = get_connection()
    colonne(cnx)

    ecrits, alertes = 0, []
    for _, r in d.iterrows():
        if not (r["dps"] and r["shares"]) or r["dps"] != r["dps"] or r["shares"] != r["shares"]:
            continue
        declare = float(r["dps"]) * float(r["shares"])
        verse = r["dividends_total"]
        if verse == verse and verse:
            ecart = declare / float(verse) - 1
            if abs(ecart) > TOLERANCE:
                alertes.append((r["ticker"], int(r["fiscal_year"]), float(verse),
                                declare, ecart))
        resultat = r["net_income"]
        if resultat == resultat and resultat and resultat > 0 and declare > 3 * resultat:
            alertes.append((r["ticker"], int(r["fiscal_year"]), None, declare,
                            declare / float(resultat)))
            continue
        if ecrire:
            cnx.execute("UPDATE fundamentals SET dividends_declared = ? "
                        "WHERE ticker = ? AND fiscal_year = ?",
                        (declare, r["ticker"], int(r["fiscal_year"])))
        ecrits += 1

    if ecrire:
        cnx.commit()
    cnx.close()

    print(f"\n{ecrits} exercice(s) avec un dividende declare calcule "
          f"(dividende par action x nombre d'actions)")
    print(f"{len(alertes)} alerte(s) a verifier a la main :\n")
    print(f"  {'titre':9} {'exercice':>8} {'verse (flux)':>14} {'declare':>14} {'ecart':>9}")
    for ticker, annee, verse, declare, ecart in sorted(alertes):
        if verse is None:
            print(f"  {ticker:9} {annee:>8} {'—':>14} {declare/1e9:>11.2f} Md  "
                  f"{ecart:>6.0f}x le resultat")
        else:
            print(f"  {ticker:9} {annee:>8} {verse/1e9:>11.2f} Md {declare/1e9:>11.2f} Md "
                  f"{ecart*100:>+8.1f} %")
    if not ecrire:
        print("\nRien n'a ete ecrit : relancer avec --ecrire.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
