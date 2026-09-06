#!/usr/bin/env python3
"""Relit les documents en TEXTE des societes a qui il manque un poste.

Corriger l'extracteur ne corrige pas la base : les valeurs y restent telles
qu'une version anterieure les a ecrites. Rejouer le rattrapage complet coute
sept heures de coureurs, dont l'essentiel part en OCR sur des centaines de
scans — pour, souvent, quelques lignes manquantes sur des documents en texte
natif que la lecture ordinaire suffit a ouvrir.

Ce script ne relit QUE ce qui manque, et seulement en texte. Les documents
scannes sont comptes et laisses de cote : ils relevent de l'OCR ou de la
saisie, pas d'une enieme relecture qui ne rendra rien.
"""

import os
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection
from data.pdf_extractor import download_and_extract
from data.storage import save_fundamentals, champs_extraits

TYPES_ANNUELS = ("etats_financiers", "rapport_annuel")


def _a_relire(conn, champ):
    """(ticker, exercice) dont `champ` manque sur l'exercice affiche."""
    manquants = []
    for ligne in conn.execute(
            "SELECT DISTINCT ticker FROM fundamentals "
            "WHERE sector IS NOT NULL ORDER BY ticker").fetchall():
        ticker = dict(ligne)["ticker"]
        dernier = conn.execute(
            "SELECT fiscal_year FROM fundamentals WHERE ticker = ? "
            "AND revenue IS NOT NULL AND revenue <> 0 "
            f"AND ({champ} IS NULL OR {champ} = 0) "
            "ORDER BY fiscal_year DESC LIMIT 1", (ticker,)).fetchone()
        if dernier:
            manquants.append((ticker, dict(dernier)["fiscal_year"]))
    return manquants


def main(champ: str = "equity", ouvriers: int = 4,
         appliquer: bool = False) -> None:
    conn = get_connection()
    cibles = _a_relire(conn, champ)
    documents = []
    for ticker, annee in cibles:
        for ligne in conn.execute(
                "SELECT url FROM report_links WHERE ticker = ? "
                "AND fiscal_year = ? AND report_type IN "
                f"({','.join('?' * len(TYPES_ANNUELS))}) ",
                (ticker, annee) + TYPES_ANNUELS).fetchall():
            documents.append((ticker, annee, dict(ligne)["url"]))
    conn.close()

    print(f"{len(cibles)} societe(s) sans {champ} · "
          f"{len(documents)} document(s) annuel(s) a relire\n", flush=True)

    compteurs = {"trouve": 0, "scan": 0, "vide": 0}
    resultats = []

    def traiter(item):
        ticker, annee, url = item
        try:
            # use_ocr=False : les scans sont comptes, pas travailles.
            res = download_and_extract(url, use_ocr=False)
        except Exception as exc:
            return ticker, annee, None, f"erreur {str(exc)[:40]}"
        if res.get("error"):
            return ticker, annee, None, "telechargement"
        if not champs_extraits(res):
            return ticker, annee, None, "SCAN — aucun texte"
        # On n'ecrit QUE le poste manquant. Reecrire tout ce que le document
        # rend reviendrait a remplacer des valeurs deja verifiees par la
        # lecture d'un document qui n'est pas forcement le meilleur.
        valeur = res.get(champ)
        return ticker, annee, valeur, (
            f"{valeur / 1e9:,.2f} Mds" if valeur else "texte lu, poste absent")

    with ThreadPoolExecutor(max_workers=ouvriers) as pool:
        for ticker, annee, valeur, note in pool.map(traiter, documents):
            if valeur:
                compteurs["trouve"] += 1
                resultats.append((ticker, annee, valeur))
            elif note.startswith("SCAN"):
                compteurs["scan"] += 1
            else:
                compteurs["vide"] += 1
            print(f"  {ticker:10s} {annee}  {note}", flush=True)

    print(f"\ntrouve {compteurs['trouve']} · scans {compteurs['scan']} "
          f"· sans le poste {compteurs['vide']}")

    if not resultats:
        return
    if not appliquer:
        print("\nsimulation — relancer avec --appliquer")
        return

    # Plusieurs documents d'un meme exercice peuvent differer — la SITAB en a
    # trois, qui rendent 46,06, 46,20 et 45,64. La mediane les departage sans
    # privilegier l'ordre de lecture.
    par_exercice = {}
    for ticker, annee, valeur in resultats:
        par_exercice.setdefault((ticker, annee), []).append(valeur)

    conn = get_connection()
    for (ticker, annee), valeurs in sorted(par_exercice.items()):
        retenue = statistics.median(valeurs)
        if len(valeurs) > 1:
            print(f"  {ticker:10s} {annee}  {len(valeurs)} lectures, "
                  f"mediane retenue : {retenue / 1e9:,.2f} Mds")
        conn.execute(f"UPDATE fundamentals SET {champ} = ? "
                     "WHERE ticker = ? AND fiscal_year = ?",
                     (retenue, ticker, annee))
    conn.commit()
    conn.close()
    print(f"\n{len(par_exercice)} exercice(s) mis a jour")


if __name__ == "__main__":
    champ = "equity"
    for arg in sys.argv[1:]:
        if arg.startswith("--champ="):
            champ = arg.split("=", 1)[1]
    main(champ=champ, appliquer="--appliquer" in sys.argv)
