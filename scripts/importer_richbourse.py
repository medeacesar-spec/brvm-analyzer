#!/usr/bin/env python3
"""Importe l'historique long des cours exporte depuis RichBourse.

CE QUE CE SCRIPT AJOUTE

La base tenait 13 644 seances depuis avril 2021, et une serie MENSUELLE de
61 points. Les exports couvrent 1998-2007 selon les titres, en JOURNALIER.
Les mesures de risque, qui lisent `price_monthly`, passent donc d'un
horizon de cinq ans a une vingtaine d'annees de matiere disponible.

TROIS CHOIX, TOUS VERIFIES AVANT D'ETRE FAITS

1. ON IMPORTE LE COURS **AJUSTE**, PAS LE COURS COTE.

   Verifie le 10/09/2026 sur DIX-HUIT titres, 57 mois chacun : la serie
   deja en base est l'ajustee, sans une exception. BOA Senegal en janvier
   2024 : la base porte 2 267, l'ajuste vaut 2 267, le cours reellement
   cote ce jour-la valait 3 400.

   Importer le cours cote creerait une falaise a la jonction des deux
   series. Le 29 aout 2024, BOA Senegal passe de 4 690 a 3 135 en une
   seance — moins 33 %. Le porteur n'a rien perdu : une action gratuite
   pour deux. La serie ajustee dit +0,3 %, ce qui est la verite.

   L'ajustement de RichBourse couvre les divisions du nominal et les
   attributions gratuites. Il ne couvre PAS les dividendes : Sonatel,
   6 503 seances depuis 1998 et un dividende chaque annee, n'a qu'UNE
   rupture — la division par 10 de novembre 2012. `analysis/risque.py`
   continue donc d'ajouter les dividendes lui-meme.

2. ON N'ECRASE JAMAIS UNE SEANCE DEJA EN BASE.

   Les lignes existantes portent parfois ouverture, plus haut et plus bas,
   que l'export ne donne pas. Les remplacer perdrait de l'information pour
   n'en gagner aucune.

3. LA SERIE MENSUELLE GARDE SA CONVENTION.

   Verifie sur 2024, douze mois sur douze : `price_monthly` date le mois
   par son PREMIER jour mais porte la cloture de sa DERNIERE seance, et
   le volume CUMULE du mois. On ajoute les mois manquants, on ne refait
   pas ceux qui existent.

Usage :
  python3 scripts/importer_richbourse.py --dry-run
  python3 scripts/importer_richbourse.py
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_tickers                      # noqa: E402
from data.db import get_connection, read_sql_df      # noqa: E402

DOSSIER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "csv")

# Le code entre parentheses du nom de fichier ne suffit pas toujours.
EXCEPTIONS = {"SMB": "SMBC.ci"}

# Les indices n'ont pas de code entre parentheses et n'ont ni volume ni cours
# ajuste — trois colonnes seulement : Date, Variation, Cours.
#
# Le Composite est indispensable, et pas pour le decor : `analysis/risque.py`
# mesure le beta et la correlation CONTRE lui. Tant qu'il s'arretait a 61 mois
# en base, ces deux mesures etaient bornees a cinq ans quelle que soit la
# profondeur du titre. Son export remonte a septembre 1998, soit la meme
# profondeur que les plus anciennes societes : la borne saute.
#
# Le BRVM 30 ne commence qu'en janvier 2023 — c'est un indice recent, il
# n'y a rien a gagner ailleurs.
INDICES = {
    "brvm-composite.csv": "BRVMC",
    "brvm-30.csv": "BRVM30",
}


def correspondance() -> tuple:
    """{chemin: ticker} et la liste des fichiers non rattaches."""
    racines = defaultdict(list)
    for t in load_tickers():
        racines[t["ticker"].split(".")[0].upper()].append(t["ticker"])

    couples, orphelins = {}, []
    for chemin in sorted(_fichiers()):
        nom = os.path.basename(chemin)
        if nom in INDICES:
            couples[chemin] = INDICES[nom]
            continue
        m = re.search(r"\(([A-Za-z]+)\)\.csv$", nom)
        code = m.group(1).upper() if m else None
        cible = EXCEPTIONS.get(code)
        if cible is None:
            candidats = racines.get(code, [])
            cible = candidats[0] if len(candidats) == 1 else None
        if cible:
            couples[chemin] = cible
        else:
            orphelins.append(os.path.basename(chemin))
    return couples, orphelins


def _fichiers() -> list:
    if not os.path.isdir(DOSSIER):
        return []
    return [os.path.join(DOSSIER, f) for f in os.listdir(DOSSIER)
            if f.lower().endswith(".csv")]


def lire(chemin: str) -> list:
    """(date, cours, volume) triees, seances cotees seulement.

    Deux formats : les societes portent « Cours Ajuste » et « Volume Ajuste
    Total » ; les indices, une seule colonne « Cours » et pas de volume — ils
    n'en ont pas, un indice ne s'echange pas.
    """
    seances = []
    with io.open(chemin, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            jour = (r.get("Date") or "").strip()
            brut = (r.get("Cours Ajuste") or r.get("Cours") or "").strip()
            if not jour or not brut:
                continue
            try:
                cours = float(brut)
                volume = float((r.get("Volume Ajuste Total") or 0) or 0)
            except ValueError:
                continue
            if cours <= 0:
                continue
            seances.append((jour, cours, volume))
    seances.sort()
    return seances


PAQUET = 2000


def _inserer(conn, requete: str, valeurs: list) -> None:
    """Insere par paquets de PAQUET lignes."""
    cur = conn.cursor()
    for i in range(0, len(valeurs), PAQUET):
        cur.executemany(requete, valeurs[i:i + PAQUET])


def importer(simulation: bool = False) -> dict:
    couples, orphelins = correspondance()
    if not couples:
        print(f"Aucun fichier exploitable dans {DOSSIER}.")
        return {}
    for f in orphelins:
        print(f"  [ignore] {f} : ticker non identifie")

    connues = defaultdict(set)
    for _, r in read_sql_df("SELECT ticker, date FROM price_cache").iterrows():
        connues[r["ticker"]].add(str(r["date"])[:10])
    mois_connus = defaultdict(set)
    for _, r in read_sql_df("SELECT ticker, date FROM price_monthly").iterrows():
        mois_connus[r["ticker"]].add(str(r["date"])[:7])

    n_seances = n_mois = 0
    detail = []
    conn = None if simulation else get_connection()
    try:
        for chemin, ticker in sorted(couples.items(), key=lambda kv: kv[1]):
            seances = lire(chemin)
            if not seances:
                continue

            nouvelles = [s for s in seances if s[0] not in connues[ticker]]
            if conn and nouvelles:
                # Par LOTS, pas ligne a ligne : sur un lien a 0,37 s la
                # requete, 148 000 inserts unitaires demanderaient quinze
                # heures. `executemany` les envoie par paquets.
                _inserer(conn,
                         "INSERT INTO price_cache (ticker, date, close, volume) "
                         "VALUES (?, ?, ?, ?)",
                         [(ticker, j, c, v) for j, c, v in nouvelles])

            # Mensuel : derniere seance du mois, volume cumule du mois.
            par_mois = defaultdict(list)
            for jour, cours, volume in seances:
                par_mois[jour[:7]].append((jour, cours, volume))
            a_ecrire = []
            for mois, lot in sorted(par_mois.items()):
                if mois in mois_connus[ticker]:
                    continue
                lot.sort()
                a_ecrire.append((ticker, f"{mois}-01", lot[-1][1],
                                 sum(x[2] for x in lot)))
            if conn and a_ecrire:
                _inserer(conn,
                         "INSERT INTO price_monthly (ticker, date, close, volume) "
                         "VALUES (?, ?, ?, ?)", a_ecrire)
            ajoutes = len(a_ecrire)

            n_seances += len(nouvelles)
            n_mois += ajoutes
            detail.append((ticker, seances[0][0], len(seances),
                           len(nouvelles), ajoutes))
        if conn:
            conn.commit()
    finally:
        if conn:
            conn.close()

    print(f"\n{'titre':10} {'depuis':>12} {'seances CSV':>12} "
          f"{'ajoutees':>10} {'mois ajoutes':>13}")
    for t, debut, total, neuves, mois in detail:
        print(f"{t:10} {debut:>12} {total:>12,} {neuves:>10,} {mois:>13}")
    verbe = "seraient ajoutees" if simulation else "ajoutees"
    print(f"\n{n_seances:,} seance(s) {verbe}, {n_mois:,} mois.")
    if simulation:
        print("Mode simulation : rien n'a ete ecrit.")
    return {"seances": n_seances, "mois": n_mois, "titres": len(detail)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="compte sans rien ecrire")
    a = ap.parse_args()
    importer(simulation=a.dry_run)
