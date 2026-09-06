#!/usr/bin/env python3
"""Releve toutes les quinzaines ce que la BRVM a publie, et le traite.

La cote publie en continu : etats financiers d'exercice au printemps, rapports
de periode tout au long de l'annee, avis de paiement de dividendes a chaque
assemblee. Entre deux passages a la main, des semaines de publications
restaient dehors — et le seul moyen de s'en apercevoir etait de tomber sur un
titre au ratio absurde.

La routine fait quatre choses, dans cet ordre, et rend compte de chacune :

  1. RECENSER   ce que la BRVM a mis en ligne depuis le dernier passage
  2. TRAITER    les etats financiers et les rapports de periode nouvellement
                parus
  3. COLLECTER  les avis de paiement de dividendes
  4. CONTROLER  ce qui vient d'etre ecrit, avec les neuf sondes de coherence

Elle ne juge pas la qualite des chiffres — `coherence_interne.py` le fait — et
ne corrige rien. Elle DIT : voila ce qui est paru, voila ce qui est entre en
base, voila ce qui a echoue. Le code de sortie vaut 1 des qu'une etape echoue
ou qu'un document parait sans entrer, pour que l'atelier ouvre un billet.

Usage :
    python3 scripts/routine_quinzaine.py [--jours=15] [--simuler]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ce qui compte comme une publication a traiter, et par quel script.
ANNUELS = ("etats_financiers", "rapport_annuel")
PERIODES = ("rapport_trimestriel", "rapport_semestriel")


def _etat(cnx) -> dict:
    """Photographie chiffree de la base, pour mesurer ce qu'une etape apporte."""
    un = lambda requete: dict(cnx.execute(requete).fetchone())["n"]  # noqa: E731
    return {
        "documents": un("SELECT count(*) n FROM report_links"),
        "exercices": un("SELECT count(*) n FROM fundamentals "
                        "WHERE revenue IS NOT NULL"),
        "periodes": un("SELECT count(*) n FROM quarterly_data "
                       "WHERE revenue IS NOT NULL"),
        "dividendes": un("SELECT count(*) n FROM fundamentals "
                         "WHERE dps IS NOT NULL"),
    }


def _lancer(intitule: str, commande: list, journal: list) -> bool:
    """Execute une etape et retient son sort, sans jamais interrompre la suite.

    Une etape qui echoue ne doit pas emporter les autres : un scan bloque par
    le site de la BRVM ne dispense pas de collecter les avis de dividendes.
    """
    print(f"\n──── {intitule} ────", flush=True)
    try:
        issue = subprocess.run(commande, cwd=RACINE, timeout=3600)
        bon = issue.returncode == 0
    except subprocess.TimeoutExpired:
        print("  ! delai depasse")
        bon = False
    except Exception as err:                                   # noqa: BLE001
        print(f"  ! {err}")
        bon = False
    journal.append((intitule, bon))
    return bon


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jours", type=int, default=15,
                    help="fenetre consideree comme « la periode »")
    ap.add_argument("--simuler", action="store_true",
                    help="recense et controle, sans rien ecrire")
    args = ap.parse_args()

    depuis = datetime.utcnow() - timedelta(days=args.jours)
    cnx = get_connection()
    avant = _etat(cnx)
    connus = {dict(r)["url"] for r in cnx.execute("SELECT url FROM report_links")}
    cnx.close()

    print(f"Routine de quinzaine · fenetre de {args.jours} jours "
          f"(depuis le {depuis:%Y-%m-%d})")
    print(f"Depart : {avant['documents']} documents · "
          f"{avant['exercices']} exercices · {avant['periodes']} periodes · "
          f"{avant['dividendes']} dividendes")

    journal = []
    python = sys.executable

    # 1. RECENSER -----------------------------------------------------------
    _lancer("1. Recensement des publications BRVM",
            [python, "scripts/scan_brvm_reports.py"], journal)

    cnx = get_connection()
    parus = [dict(r) for r in cnx.execute(
        "SELECT ticker, report_type, fiscal_year, url, created_at "
        "FROM report_links ORDER BY id DESC LIMIT 400")
        if dict(r)["url"] not in connus]
    cnx.close()

    print(f"\n{len(parus)} publication(s) nouvelle(s) :")
    for p in parus[:40]:
        print(f"   {p['ticker']:10s} {str(p['fiscal_year']):5s} "
              f"{p['report_type']:22s} {p['url'].rsplit('/', 1)[-1][:56]}")
    if len(parus) > 40:
        print(f"   … et {len(parus) - 40} autres")

    if args.simuler:
        print("\n(simulation : aucune ecriture)")
    else:
        # 2. TRAITER --------------------------------------------------------
        # Les deux familles ne se lisent pas pareil : un exercice porte des
        # flux annuels, une periode un cumul. Deux scripts, deux etapes.
        if any(p["report_type"] in ANNUELS for p in parus) or not parus:
            _lancer("2a. Etats financiers et rapports annuels",
                    [python, "scripts/extract_pdfs.py"], journal)
        if any(p["report_type"] in PERIODES for p in parus) or not parus:
            _lancer("2b. Rapports de periode",
                    [python, "scripts/extraire_periodes.py"], journal)

        # 3. COLLECTER ------------------------------------------------------
        _lancer("3. Avis de paiement de dividendes",
                [python, "scripts/collecter_avis_dividendes.py"], journal)

    # 4. CONTROLER ----------------------------------------------------------
    _lancer("4. Sondes de coherence",
            [python, "scripts/coherence_interne.py"], journal)

    cnx = get_connection()
    apres = _etat(cnx)
    # Une publication qui parait sans rien apporter n'est pas forcement une
    # erreur — un rapport peut ne contenir que de la prose — mais c'est la
    # seule facon de reperer un format que le moteur ne sait pas lire.
    muets = [p for p in parus if p["report_type"] in ANNUELS + PERIODES]
    cnx.close()

    print("\n════ Releve ════")
    for cle, libelle in (("documents", "documents recenses"),
                         ("exercices", "exercices renseignes"),
                         ("periodes", "periodes renseignees"),
                         ("dividendes", "dividendes renseignes")):
        ecart = apres[cle] - avant[cle]
        print(f"   {libelle:24s} {apres[cle]:5d}  ({ecart:+d})")

    echecs = [nom for nom, bon in journal if not bon]
    print()
    for nom, bon in journal:
        print(f"   {'OK ' if bon else 'ECHEC'}  {nom}")

    rien_gagne = (apres["exercices"] + apres["periodes"]
                  == avant["exercices"] + avant["periodes"])
    if muets and rien_gagne and not args.simuler:
        print(f"\n⚠ {len(muets)} publication(s) exploitable(s) parue(s) sans "
              f"qu'aucune valeur n'entre en base — format non reconnu ?")

    if echecs:
        print(f"\n⚠ {len(echecs)} etape(s) en echec : {', '.join(echecs)}")
        return 1
    if muets and rien_gagne and not args.simuler:
        return 1
    print("\nRoutine terminee sans echec.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
