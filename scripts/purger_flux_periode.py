#!/usr/bin/env python3
"""Retire d'une ligne annuelle les FLUX qui appartiennent a une periode.

Un exercice annuel dont le chiffre d'affaires est, au franc pres, celui d'un
rapport trimestriel n'est pas un exercice : c'est un trimestre range dans la
mauvaise case. Le benefice y est divise par quatre et le P/E multiplie
d'autant — NSBC ressortait a 151,7 sur un exercice 2026 qui n'est que son
premier trimestre, contre 15,0 sur l'exercice 2025 reellement publie.

`backfill_sectoriel.py` interdit deja cette ecriture : les FLUX ne viennent
que des rapports annuels. Les lignes visees ici sont anterieures a cette
regle, et rien ne les efface.

On ne touche QU'AUX flux, jamais aux stocks : les depots et les credits d'un
bulletin trimestriel restent la meilleure photo connue du bilan. Et on epargne
les periodes qui couvrent bien douze mois — le « S2 » de BOA Burkina est un
cumul annuel, sa valeur est a sa place.
"""

import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

# Duree couverte par chaque libelle de periode, en mois. Les publications
# BRVM sont cumulatives depuis le debut de l'exercice.
COUVERTURE = {"T1": 3, "S1": 6, "T2": 6, "T3": 9, "T4": 12, "S2": 12}

# Postes de FLUX : ils se rapportent a une duree, et n'ont donc de sens que
# rapportes a la meme duree que la ligne qui les porte.
FLUX = ("revenue", "net_income", "ebit", "ebitda", "operating_expenses",
        "gross_operating_income", "pretax_income", "cost_of_risk",
        "ordinary_income", "hao_income", "cfo", "capex", "interest_expense")

# Tolerance d'egalite : l'extraction peut arrondir au millier pres.
ECART_MAX = 0.005


def _exercices_non_clos(conn):
    """Exercices qui portent des FLUX alors qu'ils ne peuvent pas etre clos.

    Un exercice N se publie au printemps N+1 : en septembre 2026, l'exercice
    2026 n'existe pas encore. Tout chiffre d'affaires ou resultat net porte
    par une telle ligne vient forcement d'une publication de PERIODE.

    La sonde d'egalite exacte ne les attrape pas tous : la ligne 2026
    d'Ecobank porte 1 281,3 milliards, qui est le produit net bancaire du
    semestre lu dans sa colonne en DOLLARS — il ne correspond a aucune ligne
    trimestrielle en francs. Le calendrier, lui, ne se trompe pas.
    """
    limite = datetime.date.today().year
    lignes = [dict(r) for r in conn.execute(
        "SELECT ticker, fiscal_year, revenue, net_income FROM fundamentals "
        "WHERE fiscal_year >= ? "
        "AND ((revenue IS NOT NULL AND revenue <> 0) "
        "     OR (net_income IS NOT NULL AND net_income <> 0))",
        (limite,)).fetchall()]
    # Un rapport ANNUEL depose sur cet exercice dementirait le calendrier :
    # on le respecte plutot que la regle.
    annuels = {(dict(r)["ticker"], dict(r)["fiscal_year"]) for r in conn.execute(
        "SELECT ticker, fiscal_year FROM report_links "
        "WHERE report_type IN ('rapport_annuel', 'etats_financiers') "
        "AND fiscal_year >= ?", (limite,)).fetchall()}
    return [l for l in lignes
            if (l["ticker"], l["fiscal_year"]) not in annuels]


def _suspects(conn):
    annuels = {(d["ticker"], d["fiscal_year"]): d for d in (
        dict(r) for r in conn.execute(
            "SELECT ticker, fiscal_year, revenue FROM fundamentals "
            "WHERE revenue IS NOT NULL AND revenue <> 0").fetchall())}
    periodes = [dict(r) for r in conn.execute(
        "SELECT ticker, fiscal_year, periode, revenue FROM quarterly_data "
        "WHERE revenue IS NOT NULL AND revenue <> 0").fetchall()]

    trouves = {}
    for p in periodes:
        mois = COUVERTURE.get((p.get("periode") or "").upper())
        if not mois or mois >= 12:
            continue          # un cumul de douze mois est bien un exercice
        ligne = annuels.get((p["ticker"], p["fiscal_year"]))
        if not ligne:
            continue
        a, b = ligne["revenue"], p["revenue"]
        if abs(a - b) / max(abs(a), abs(b)) < ECART_MAX:
            trouves[(p["ticker"], p["fiscal_year"])] = (p["periode"], mois, b)
    return trouves


def main(appliquer: bool = False) -> None:
    conn = get_connection()

    non_clos = _exercices_non_clos(conn)
    print(f"{len(non_clos)} exercice(s) portant des flux sans pouvoir etre "
          f"clos :\n")
    for ligne in non_clos:
        print(f"   {ligne['ticker']:10s} {ligne['fiscal_year']}   "
              f"CA={(ligne['revenue'] or 0) / 1e9:9,.1f}   "
              f"RN={(ligne['net_income'] or 0) / 1e9:8,.2f}")

    trouves = _suspects(conn)
    print()

    print(f"{len(trouves)} ligne(s) annuelle(s) portant un flux de periode :\n")
    for (ticker, annee), (periode, mois, valeur) in sorted(trouves.items()):
        print(f"   {ticker:10s} exercice {annee} = {periode:3s} "
              f"({mois} mois)   CA={valeur / 1e9:9,.2f} Mds")

    cibles = set(trouves) | {(l["ticker"], l["fiscal_year"]) for l in non_clos}
    if not cibles:
        conn.close()
        return

    if not appliquer:
        print("\nsimulation — relancer avec --appliquer")
        conn.close()
        return

    colonnes = ", ".join(f"{c} = NULL" for c in FLUX)
    for ticker, annee in cibles:
        conn.execute(
            f"UPDATE fundamentals SET {colonnes} "
            "WHERE ticker = ? AND fiscal_year = ?", (ticker, annee))
    conn.commit()
    print(f"\n{len(cibles)} ligne(s) nettoyee(s) — les stocks sont conserves")
    conn.close()


if __name__ == "__main__":
    main(appliquer="--appliquer" in sys.argv)
