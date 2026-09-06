#!/usr/bin/env python3
"""Relit les exercices que les sondes de coherence signalent.

`coherence_interne.py` dit ou la base se contredit ; `relire_manquants.py` ne
comble que les trous. Entre les deux, il manquait le cas le plus interessant :
une valeur PRESENTE mais fausse.

La sonde `fige` en donne la liste la plus sure. Aucun bilan ne se reproduit au
franc pres d'un exercice a l'autre : quand deux annees portent le meme
montant, l'une des deux a ete recopiee — le plus souvent parce que la colonne
de l'exercice PRECEDENT a ete lue a la place de la bonne, ce que la lecture
d'en-tete corrige depuis.

On relit donc ces exercices avec l'extracteur courant. La valeur n'est
remplacee que si elle CHANGE et que le doublon disparait : sans cela, on
n'aurait fait que reecrire la meme erreur.
"""

import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection
from data.pdf_extractor import download_and_extract
from scripts.coherence_interne import sondes

TYPES_ANNUELS = ("etats_financiers", "rapport_annuel")


def _base(conn):
    annuels = defaultdict(dict)
    for ligne in conn.execute(
            "SELECT ticker, fiscal_year, revenue, net_income, equity, "
            "total_assets, deposits, loans, operating_expenses, "
            "gross_operating_income, total_debt, sector "
            "FROM fundamentals WHERE fiscal_year >= 2020"):
        ligne = dict(ligne)
        annuels[ligne["ticker"]][ligne["fiscal_year"]] = ligne
    return annuels


def main(appliquer: bool = False, ouvriers: int = 4) -> None:
    conn = get_connection()
    annuels = _base(conn)

    # Le libelle de la sonde `fige` nomme le poste : « equity identique en
    # 2023 et 2024 ». On en tire le champ a relire.
    cibles = {}
    for sonde, ticker, annee, phrase in sondes(annuels, {}):
        if sonde != "fige":
            continue
        champ = phrase.split()[0]
        cibles[(ticker, annee, champ)] = phrase

    documents = []
    for (ticker, annee, champ) in cibles:
        for ligne in conn.execute(
                "SELECT url FROM report_links WHERE ticker = ? AND fiscal_year = ? "
                f"AND report_type IN ({','.join('?' * len(TYPES_ANNUELS))})",
                (ticker, annee) + TYPES_ANNUELS).fetchall():
            documents.append((ticker, annee, champ, dict(ligne)["url"]))
    conn.close()

    print(f"{len(cibles)} valeur(s) figee(s) · {len(documents)} document(s) a relire\n",
          flush=True)

    def traiter(item):
        ticker, annee, champ, url = item
        try:
            res = download_and_extract(url, use_ocr=False)
        except Exception as exc:
            return ticker, annee, champ, None, f"erreur {str(exc)[:36]}"
        return ticker, annee, champ, res.get(champ), None

    retenus = {}
    with ThreadPoolExecutor(max_workers=ouvriers) as pool:
        for ticker, annee, champ, valeur, souci in pool.map(traiter, documents):
            ancienne = annuels[ticker][annee].get(champ)
            if souci:
                note = souci
            elif valeur is None:
                note = "le document ne rend pas ce poste"
            elif ancienne and abs(valeur - ancienne) < abs(ancienne) * 0.005:
                note = "relecture identique — la valeur tient"
            else:
                note = (f"{(ancienne or 0) / 1e9:,.2f} -> {valeur / 1e9:,.2f} Mds")
                retenus[(ticker, annee, champ)] = valeur
            print(f"  {ticker:10s} {annee} {champ:14s} {note}", flush=True)

    if not retenus:
        print("\naucune valeur ne change a la relecture")
        return
    if not appliquer:
        print(f"\nsimulation — {len(retenus)} valeur(s) a reecrire, "
              f"relancer avec --appliquer")
        return

    conn = get_connection()
    for (ticker, annee, champ), valeur in retenus.items():
        conn.execute(f"UPDATE fundamentals SET {champ} = ? "
                     "WHERE ticker = ? AND fiscal_year = ?", (valeur, ticker, annee))
    conn.commit()
    conn.close()
    print(f"\n{len(retenus)} valeur(s) reecrite(s)")


if __name__ == "__main__":
    main(appliquer="--appliquer" in sys.argv)
