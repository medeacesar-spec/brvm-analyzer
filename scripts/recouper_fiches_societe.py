#!/usr/bin/env python3
"""Recoupe les fondamentaux avec les fiches societe, qui donnent cinq exercices.

POURQUOI CETTE SOURCE, ET POURQUOI ON PEUT S'Y FIER

Les etats financiers de la BRVM sont, pour l'essentiel, des scans : sur
vingt-deux documents telecharges, deux portaient du texte, et l'extraction
automatique rend dix valeurs justes sur trente-six (voir PR #178). La fiche
societe, elle, publie CINQ exercices d'un coup, en texte.

Restait a savoir si elle dit vrai. Elle a donc ete confrontee aux dix-neuf
valeurs lues A LA MAIN dans les documents officiels et ecrites en base apres
recoupement (PR #175, #176) : **dix-neuf concordances, zero divergence**,
NSIA, SODECI, SITAB, Vivo, Palm, Oragroup, SMB, LNB et TotalEnergies SN
compris. C'est ce controle qui autorise ce script a ecrire.

CE QU'IL ECRIT, ET CE QU'IL SE CONTENTE DE SIGNALER

Il n'ecrit QUE sur double confirmation : la fiche donne un montant annuel,
et le montant deja en base correspond, au 2 % pres, a une periode partielle
que `quarterly_data` connait — un T1, un S1, un T3. Deux sources
independantes disent alors la meme chose : la ligne annuelle porte un
trimestre.

Tout le reste est SIGNALE, jamais ecrit :

- les rapports extremes (facteur dix et plus), qui sentent l'erreur
  d'echelle ou de signe ;
- les ecarts moderes, qui relevent le plus souvent d'une definition
  differente — comptes sociaux contre comptes consolides, produit net
  bancaire contre produits totaux — et qu'aucune regle automatique ne
  tranche.

Usage :
  python3 scripts/recouper_fiches_societe.py
  python3 scripts/recouper_fiches_societe.py --ecrire
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from config import load_tickers  # noqa: E402
from data.db import get_connection, read_sql_df  # noqa: E402

DEBUT, FIN = 2021, 2025
CHAMPS = ("revenue", "net_income")
TOLERANCE = 0.02          # au-dela, la base et la fiche ne disent pas la meme chose
EXTREME = 10              # rapport au-dela duquel on soupconne l'echelle


def _scraper():
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrape_societe.py")
    spec = importlib.util.spec_from_file_location("scrape_societe", chemin)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def periodes_partielles() -> dict:
    """{(ticker, exercice): [(periode, revenue, net_income)]} — les cumuls publies."""
    d = read_sql_df("SELECT ticker, fiscal_year, periode, revenue, net_income "
                    "FROM quarterly_data WHERE periode IN ('T1','T2','T3','S1')")
    sortie = {}
    for _, r in d.iterrows():
        if r["fiscal_year"] != r["fiscal_year"]:
            continue
        sortie.setdefault((r["ticker"], int(r["fiscal_year"])), []).append(
            (r["periode"], r["revenue"], r["net_income"]))
    return sortie


def periode_correspondante(base, champ, periodes) -> str:
    """Le nom de la periode partielle qui porte ce montant, s'il en est une."""
    colonne = 1 if champ == "revenue" else 2
    for lot in periodes or ():
        valeur = lot[colonne]
        if valeur == valeur and valeur and base and abs(valeur / base - 1) < TOLERANCE:
            return lot[0]
    return None


def main(ecrire: bool) -> None:
    scraper = _scraper()
    session = requests.Session()
    session.headers.update(scraper.HEADERS)
    base = read_sql_df(
        "SELECT ticker, fiscal_year, revenue, net_income FROM fundamentals "
        "WHERE fiscal_year BETWEEN ? AND ?", params=(DEBUT, FIN))
    partielles = periodes_partielles()
    cnx = get_connection() if ecrire else None

    corriges, combles = [], []
    echelle, moderes, sans_fiche = [], [], []
    for t in load_tickers():
        ticker = t["ticker"]
        try:
            fiche = scraper.scrape_societe(session, ticker)["financials"]
        except Exception as err:                                  # noqa: BLE001
            print(f"  {ticker:9} fiche illisible : {type(err).__name__}"); continue
        if not fiche:
            sans_fiche.append(ticker)
            continue
        for exercice, valeurs in sorted(fiche.items()):
            if not (DEBUT <= exercice <= FIN):
                continue
            ligne = base[(base.ticker == ticker) & (base.fiscal_year == exercice)]
            for champ in CHAMPS:
                neuf = valeurs.get(champ)
                if not neuf:
                    continue
                vieux = ligne.iloc[0][champ] if len(ligne) else None
                if vieux is None or vieux != vieux:
                    combles.append((ticker, exercice, champ, neuf))
                    continue
                if abs(neuf / vieux - 1) <= TOLERANCE:
                    continue
                periode = periode_correspondante(vieux, champ, partielles.get((ticker, exercice)))
                rapport = neuf / vieux
                if periode:
                    corriges.append((ticker, exercice, champ, vieux, neuf, periode))
                elif rapport > EXTREME or rapport < 1 / EXTREME or rapport < 0:
                    echelle.append((ticker, exercice, champ, vieux, neuf, rapport))
                else:
                    moderes.append((ticker, exercice, champ, vieux, neuf, rapport))
        time.sleep(0.4)

    if ecrire:
        for ticker, exercice, champ, _, neuf, _ in corriges:
            cnx.execute(f"UPDATE fundamentals SET {champ} = ? "
                        "WHERE ticker = ? AND fiscal_year = ?", (neuf, ticker, exercice))
        for ticker, exercice, champ, neuf in combles:
            n = cnx.execute(f"UPDATE fundamentals SET {champ} = ? "
                            "WHERE ticker = ? AND fiscal_year = ?",
                            (neuf, ticker, exercice)).rowcount
            if not n:
                cnx.execute("INSERT INTO fundamentals (ticker, fiscal_year, " + champ + ") "
                            "VALUES (?, ?, ?)", (ticker, exercice, neuf))
        cnx.commit()
        cnx.close()

    print(f"\n{len(corriges)} periode(s) partielle(s) remplacee(s) par l'exercice "
          f"— double confirmation")
    for tk, an, ch, v, n, p in sorted(corriges)[:60]:
        print(f"  {tk:9} {an} {ch:11} {v/1e9:8.2f} Md = {p:2} -> {n/1e9:8.2f} Md")
    print(f"\n{len(combles)} trou(s) comble(s)")
    for tk, an, ch, n in sorted(combles):
        print(f"  {tk:9} {an} {ch:11} {n/1e9:8.2f} Md")
    print(f"\nALERTE · {len(echelle)} rapport(s) extreme(s) — echelle ou signe :")
    for tk, an, ch, v, n, r in sorted(echelle):
        print(f"  {tk:9} {an} {ch:11} base {v/1e9:10.2f} Md · fiche {n/1e9:9.2f} Md ({r:+.3f}x)")
    print(f"\nALERTE · {len(moderes)} ecart(s) modere(s) — definition ou periode :")
    for tk, an, ch, v, n, r in sorted(moderes):
        print(f"  {tk:9} {an} {ch:11} base {v/1e9:9.2f} Md · fiche {n/1e9:9.2f} Md ({r:.2f}x)")
    if sans_fiche:
        print(f"\n{len(sans_fiche)} titre(s) sans fiche exploitable : {', '.join(sans_fiche)}")
    if not ecrire:
        print("\nRien n'a ete ecrit : relancer avec --ecrire.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
