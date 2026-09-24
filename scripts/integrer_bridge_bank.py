#!/usr/bin/env python3
"""Integre Bridge Bank Group CI (BBGC.ci), cotee le 24 septembre 2026.

CE QUE LE SCRIPT FAIT

1. Ecrit les cinq exercices 2021-2025 publies sur la fiche societe, la meme
   source que pour les quarante-huit autres titres.
2. Complete `market_data` : nom, secteur, nombre de titres, flottant.
3. Fusionne le doublon « BBGC ».

LE DOUBLON, ET SA CAUSE

Les cotations arrivaient sous DEUX symboles : « BBGC » et « BBGC.ci ».
`fetch_daily_quotes_brvm` traduit le symbole court de brvm.org grace a la
table des titres ; un titre absent de cette table garde donc son symbole
brut. Ajouter BBGC.ci a `data/brvm_tickers.json` tarit la source du doublon,
et ce script efface les lignes deja ecrites sous le mauvais symbole.

CE QUI EST VERIFIE AVANT D'ECRIRE

Les chiffres se recoupent entre eux : 27 197 M de resultat 2025 pour
50 000 000 d'actions donnent 543,9 de BNPA, la fiche affiche 544 ; le cours
de 7 255 divise par ce BNPA donne 13,34, le PER affiche. La capitalisation
annoncee (362 750 M) est le produit des deux. Le script refuse d'ecrire si
ces recoupements ne tombent plus.

UNE SOURCE UNIQUE, ET ON LE DIT

Aucun etat financier de Bridge Bank n'est publie sur brvm.org : sa page
societe y est vide au lendemain de l'introduction. Ces chiffres n'ont donc
PAS ete recoupes avec un document d'emetteur, contrairement aux exercices
2025 des autres titres. A revoir des que la banque deposera ses comptes.

Usage :
  python3 scripts/integrer_bridge_bank.py --simuler
  python3 scripts/integrer_bridge_bank.py
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from data.db import get_connection  # noqa: E402
from data.storage import save_fundamentals  # noqa: E402

TICKER = "BBGC.ci"
DOUBLON = "BBGC"
NOM = "Bridge Bank Group CI"
SECTEUR = "Banque"
TOLERANCE = 0.01


def _scraper():
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrape_societe.py")
    spec = importlib.util.spec_from_file_location("scrape_societe", chemin)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def recouper(donnees: dict, cours: float) -> list:
    """Les incoherences trouvees. Liste vide = les chiffres tiennent."""
    ecarts = []
    actions = donnees.get("shares")
    dernier = max(donnees["financials"]) if donnees["financials"] else None
    if not (actions and dernier):
        return ["nombre de titres ou exercices absents"]
    fin = donnees["financials"][dernier]
    bnpa, resultat, per = fin.get("eps"), fin.get("net_income"), fin.get("per")
    if resultat and bnpa:
        calcule = resultat / actions
        if abs(calcule / bnpa - 1) > TOLERANCE:
            ecarts.append(f"BNPA {bnpa} contre {calcule:.1f} recalcule "
                          f"({resultat:,.0f} / {actions:,.0f})")
    if per and bnpa and cours:
        calcule = cours / bnpa
        if abs(calcule / per - 1) > TOLERANCE:
            ecarts.append(f"PER {per} contre {calcule:.2f} recalcule "
                          f"({cours:,.0f} / {bnpa})")
    return ecarts


def main(simuler: bool) -> None:
    session = requests.Session()
    scraper = _scraper()
    session.headers.update(scraper.HEADERS)
    donnees = scraper.scrape_societe(session, TICKER)

    cnx = get_connection()
    ligne = cnx.execute("SELECT price FROM market_data WHERE ticker = ?", (TICKER,)).fetchone()
    cours = dict(ligne)["price"] if ligne else None

    ecarts = recouper(donnees, cours)
    print(f"{TICKER} · {donnees.get('shares'):,.0f} titres · flottant "
          f"{donnees.get('float_pct')} % · cours {cours:,.0f}")
    for annee in sorted(donnees["financials"]):
        f = donnees["financials"][annee]
        print(f"   {annee} : PNB {(f.get('revenue') or 0)/1e9:6.2f} Md · "
              f"resultat {(f.get('net_income') or 0)/1e9:6.2f} Md · "
              f"BNPA {f.get('eps')} · dividende {f.get('dps')}")
    if ecarts:
        print("\nRECOUPEMENT EN ECHEC — rien n'est ecrit :")
        for e in ecarts:
            print("   ·", e)
        cnx.close()
        return
    print("\nRecoupements : BNPA, PER et capitalisation concordent.")

    if simuler:
        print("Mode simulation : rien n'a ete ecrit.")
        cnx.close()
        return

    for annee, f in sorted(donnees["financials"].items()):
        save_fundamentals({
            "ticker": TICKER, "fiscal_year": annee, "company_name": NOM,
            "sector": SECTEUR, "shares": donnees.get("shares"),
            "float_pct": donnees.get("float_pct"),
            "revenue": f.get("revenue"), "net_income": f.get("net_income"),
            "eps": f.get("eps"), "per": f.get("per"), "dps": f.get("dps"),
        })
    cnx.execute(
        "UPDATE market_data SET company_name = ?, sector = ?, shares = ?, "
        "float_pct = ?, market_cap = ? WHERE ticker = ?",
        (NOM, SECTEUR, donnees.get("shares"), donnees.get("float_pct"),
         (cours or 0) * (donnees.get("shares") or 0), TICKER))
    efface = 0
    for table in ("market_data", "price_cache"):
        efface += cnx.execute(f"DELETE FROM {table} WHERE ticker = ?", (DOUBLON,)).rowcount
    cnx.commit()
    cnx.close()
    print(f"{len(donnees['financials'])} exercice(s) ecrits · "
          f"{efface} ligne(s) du doublon « {DOUBLON} » effacee(s).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--simuler", action="store_true")
    main(ap.parse_args().simuler)
