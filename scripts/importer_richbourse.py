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

from analysis.indices import INDICES as _REGISTRE            # noqa: E402
from analysis.indices import fichiers as _fichiers_indices  # noqa: E402
from config import load_tickers                      # noqa: E402
from data.db import get_connection, read_sql_df      # noqa: E402

DOSSIER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "csv")

# Le code entre parentheses du nom de fichier ne suffit pas toujours.
EXCEPTIONS = {"SMB": "SMBC.ci"}

# Les indices n'ont pas de code entre parentheses et n'ont ni volume ni cours
# ajuste — trois colonnes seulement : Date, Variation, Cours.
#
# Leur liste ne vit PAS ici mais dans `analysis/indices.py`, avec les bornes
# de validite de chaque serie. Ce script ne fait que consommer ce registre :
# ajouter un indice se fait la-bas, et tout le reste de l'application le voit
# au meme moment.
INDICES = _fichiers_indices()


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
    """Tous les CSV sous `csv/`, sous-dossiers COMPRIS.

    Les exports d'indices sont ranges par famille (« 0 - Indices generaux »,
    « 1 - Indices sectoriels anciens »…). Une lecture a plat du seul premier
    niveau les aurait tous manques sans rien signaler : le script aurait
    annonce « 47 fichiers » et personne n'aurait su qu'il en restait dix-sept.
    """
    trouves = []
    for racine, _, noms in os.walk(DOSSIER):
        trouves += [os.path.join(racine, n) for n in noms
                    if n.lower().endswith(".csv")]
    return trouves


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


def _borner(ticker: str, seances: list) -> list:
    """Coupe une serie d'indice a ses bornes de publication.

    L'ancienne nomenclature sectorielle s'arrete au 31 decembre 2025, la
    nouvelle commence au 2 janvier 2025 : elles se chevauchent d'un an sans
    mesurer les memes paniers. Un export qui deborderait de ces bornes
    prolongerait une serie au-dela de ce que la BRVM a publie, et la jonction
    entre les deux nomenclatures deviendrait invisible.
    """
    metadonnees = _REGISTRE.get(ticker)
    if not metadonnees:
        return seances
    debut = metadonnees["debut"].isoformat()
    fin = metadonnees["fin"].isoformat() if metadonnees["fin"] else "9999-99-99"
    return [s for s in seances if debut <= s[0] <= fin]


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

    def deja_en_base(table: str, ticker: str) -> set:
        """Les dates deja presentes pour CE ticker.

        La version precedente lisait `price_cache` en entier — 170 000 lignes
        aujourd'hui — avant de commencer. Le lien Supabase a fini par lacher
        en cours de transfert (« SSL connection has been closed »), et le
        script mourait avant d'avoir ecrit une seule ligne. Une lecture par
        titre coute une soixantaine d'allers-retours, tient dans le temps
        d'une requete, et ne charge jamais que ce qu'on va comparer.
        """
        d = read_sql_df(f"SELECT date FROM {table} WHERE ticker = ?",
                        params=(ticker,))
        return {str(x)[:10] for x in d["date"]} if not d.empty else set()

    n_seances = n_mois = 0
    detail = []
    conn = None if simulation else get_connection()
    try:
        for chemin, ticker in sorted(couples.items(), key=lambda kv: kv[1]):
            seances = lire(chemin)
            seances = _borner(ticker, seances)
            if not seances:
                continue

            connues = deja_en_base("price_cache", ticker)
            mois_connus = {j[:7] for j in deja_en_base("price_monthly", ticker)}
            nouvelles = [s for s in seances if s[0] not in connues]
            if conn and nouvelles:
                # Par LOTS, pas ligne a ligne : sur un lien a 0,37 s la
                # requete, 148 000 inserts unitaires demanderaient quinze
                # heures. `executemany` les envoie par paquets.
                _inserer(conn,
                         "INSERT INTO price_cache (ticker, date, close, volume) "
                         "VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                         [(ticker, j, c, v) for j, c, v in nouvelles])

            # Mensuel : derniere seance du mois, volume cumule du mois.
            par_mois = defaultdict(list)
            for jour, cours, volume in seances:
                par_mois[jour[:7]].append((jour, cours, volume))
            a_ecrire = []
            for mois, lot in sorted(par_mois.items()):
                if mois in mois_connus:
                    continue
                lot.sort()
                a_ecrire.append((ticker, f"{mois}-01", lot[-1][1],
                                 sum(x[2] for x in lot)))
            if conn and a_ecrire:
                _inserer(conn,
                         "INSERT INTO price_monthly (ticker, date, close, volume) "
                         "VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING", a_ecrire)
            ajoutes = len(a_ecrire)

            if conn:
                # Un commit PAR TITRE, et non un seul a la fin. Quarante-huit
                # mille lignes en une transaction, c'est une heure pendant
                # laquelle la moindre coupure annule tout le travail. Les
                # inserts etant idempotents (ON CONFLICT DO NOTHING), relancer
                # le script apres une coupure reprend ou il s'etait arrete.
                conn.commit()

            n_seances += len(nouvelles)
            n_mois += ajoutes
            detail.append((ticker, seances[0][0], len(seances),
                           len(nouvelles), ajoutes))
    finally:
        if conn:
            conn.close()

    print(f"\n{'titre':26} {'depuis':>12} {'seances CSV':>12} "
          f"{'ajoutees':>10} {'mois ajoutes':>13}")
    for t, debut, total, neuves, mois in detail:
        print(f"{t:26} {debut:>12} {total:>12,} {neuves:>10,} {mois:>13}")
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
