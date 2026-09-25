#!/usr/bin/env python3
"""Passe le lecteur du compte de resultat SYSCOHADA
(`analysis/lecture_resultat.py`) sur les documents des societes non
bancaires, et confronte ses lectures a la base.

UNE LECTURE N'EST RETENUE QUE SI ses soldes s'enchainent jusqu'au resultat
net de la base (qui fixe l'unite et l'exercice), et un poste que s'il est
contenu dans une identite verifiee :

  EBITDA (EBE)              EBE + reprises - dotations = resultat d'exploitation
  EBIT                      resultat d'exploitation + resultat financier = RAO
  frais financiers          produits financiers - charges = resultat financier
  resultat ordinaire (RAO)  RAO + HAO - impot - participation = resultat net
  resultat avant impot      resultat net + impot + participation

UNE VALEUR N'EST CONFIRMEE QUE PAR DEUX DOCUMENTS : l'exercice N lu dans le
document N et dans le comparatif du document N+1, d'accord a un pour mille.

Avec --ecrire, les valeurs confirmees comblent les trous ; avec --corriger
en plus, elles remplacent une valeur de la base qui les contredit (ancienne
valeur imprimee). Seuls les exercices 2021 a 2025 sont ecrits.

UN SEUL DOCUMENT (--un-document). Decision du 25/09/2026 : une lecture a
un seul document dont les soldes s'enchainent jusqu'au resultat net de la
base est ecrite aussi, trous et ecarts. Au controle, ces lectures
retombaient sur la base 47 fois ; la ou deux documents etaient
disponibles, c'est toujours la base qui avait tort.

Un garde-fou de plus pour elles : sans colonnes, le lecteur peut apparier
le RAO d'un exercice a l'impot d'un autre, et l'identite tombe juste quand
meme. Palm CI rendait ainsi le meme resultat d'exploitation, 19,393 Md,
pour 2024 et 2025. Une valeur lue a l'identique pour deux exercices du
meme titre est ecartee — sauf si la base la porte deja ainsi.

Usage :
  python3 scripts/recouper_resultat.py              # simulation
  python3 scripts/recouper_resultat.py --titre CIEC.ci
  python3 scripts/recouper_resultat.py --ecrire [--corriger]
"""
from __future__ import annotations

import argparse
import itertools
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.lecture_bancaire import _egal  # noqa: E402
from analysis.lecture_resultat import lire_exercice  # noqa: E402
from data.db import read_sql_df  # noqa: E402
from scripts import recouper_par_lecteur as R  # noqa: E402

DEBUT, FIN = 2021, 2025
# poste lu -> colonne de la base
COLONNES = {
    "ebe": "ebitda",
    "rex": "ebit",
    "frais_fin": "interest_expense",
    "rao": "ordinary_income",
    "pretax": "pretax_income",
}


def lectures(titre: str = None):
    base = read_sql_df("SELECT ticker, fiscal_year, net_income, " + ", ".join(COLONNES.values())
                       + " FROM fundamentals WHERE sector <> 'Banque'")
    connu = {(r.ticker, int(r.fiscal_year)): r for r in base.itertuples()}
    titres = set(base.ticker)
    lu = defaultdict(list)                  # (ticker, an, poste) -> [(valeur, doc)]
    for f in sorted(os.listdir(R.DOSSIER)):
        m = R.FICHIER.match(f)
        if not m or m.group(1) not in titres or (titre and m.group(1) != titre):
            continue
        ticker, annee = m.group(1), int(m.group(2))
        try:
            texte, _ = R.texte(os.path.join(R.DOSSIER, f), True)
        except Exception as err:                                # noqa: BLE001
            print(f"  {f[:60]:60} illisible : {err}")
            continue
        if not texte:
            continue
        for an in (annee, annee - 1):
            ligne = connu.get((ticker, an))
            if ligne is None or not (ligne.net_income == ligne.net_income and ligne.net_income):
                continue
            for lecture in lire_exercice(texte, float(ligne.net_income)):
                for poste in COLONNES:
                    v = lecture.get(poste)
                    if v is None:
                        continue
                    if poste == "frais_fin":
                        v = abs(v)
                    if not any(_egal(v, x) and d == f for x, d in lu[(ticker, an, poste)]):
                        lu[(ticker, an, poste)].append((v, f))
    return lu, connu


def main(ecrire: bool, titre: str = None, corriger: bool = False,
         un_document: bool = False) -> None:
    lu, connu = lectures(titre)
    Md = 1e9
    confirmees, seules, desaccords = [], [], []
    for (t, an, poste), sources in sorted(lu.items()):
        if not DEBUT <= an <= FIN:
            continue
        docs = {d for _, d in sources}
        v = sources[0][0]
        if not all(_egal(x, v) for x, _ in sources):
            desaccords.append((t, an, poste, sources))
            continue
        c = COLONNES[poste]
        b = getattr(connu[(t, an)], c)
        b = float(b) if b == b and b is not None else None
        (confirmees if len(docs) >= 2 else seules).append((t, an, c, v, b))

    def issue(v, b):
        return "TROU" if b is None else "concorde" if _egal(v, b) else "ECART"

    for titre_bloc, liste in (("CONFIRMEES — deux documents d'accord", confirmees),
                              ("UN SEUL DOCUMENT — soldes enchaines, a lire", seules)):
        print(f"\n{titre_bloc}")
        for t, an, c, v, b in liste:
            if issue(v, b) != "concorde":
                print(f"  {t:9} {an} {c:17} {v/Md:10.3f} Md  "
                      + (f"base {b/Md:10.3f} Md  " if b is not None else "") + issue(v, b))
    if desaccords:
        print("\nDESACCORDS ENTRE DOCUMENTS — a lire")
        for t, an, p, sources in desaccords:
            print(f"  {t:9} {an} {p:9} " + " · ".join(f"{x/Md:.3f} ({d[:22]})" for x, d in sources))

    if un_document:
        # Le meme montant lu pour deux exercices : des colonnes melees.
        par_poste = defaultdict(list)
        for (t, an, poste), sources in lu.items():
            par_poste[(t, poste)].append((an, sources[0][0]))
        doubles = set()
        for (t, poste), lus in par_poste.items():
            for (a1, v1), (a2, v2) in itertools.combinations(lus, 2):
                if a1 != a2 and _egal(v1, v2):
                    doubles |= {(t, a1, COLONNES[poste]), (t, a2, COLONNES[poste])}
        retenues = [x for x in seules if (x[0], x[1], x[2]) not in doubles]
        for x in seules:
            if (x[0], x[1], x[2]) in doubles:
                print(f"  ecarte (meme montant deux exercices) : {x[0]} {x[1]} {x[2]} {x[3]/Md:.3f} Md")
        confirmees = confirmees + retenues
    trous = [x for x in confirmees if x[4] is None]
    ecarts = [x for x in confirmees if x[4] is not None and not _egal(x[3], x[4])]
    print(f"\nconfirmees : {len(confirmees)} (concordent {len(confirmees)-len(trous)-len(ecarts)}"
          f" · trous {len(trous)} · ecarts {len(ecarts)}) · un seul document : {len(seules)}"
          f" · desaccords : {len(desaccords)}")
    if not ecrire:
        print("\nMode simulation : rien n'a ete ecrit.")
        return
    from data.db import get_connection
    conn = get_connection()
    try:
        for t, an, c, v, _ in trous:
            conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                         f"WHERE ticker = ? AND fiscal_year = ? AND {c} IS NULL", (v, t, an))
        if corriger:
            for t, an, c, v, b in ecarts:
                print(f"  corrige {t} {an} {c} : {b/Md:.3f} -> {v/Md:.3f} Md")
                conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                             f"WHERE ticker = ? AND fiscal_year = ?", (v, t, an))
        conn.commit()
    finally:
        conn.close()
    print(f"\n{len(trous)} trou(s) comble(s)"
          + (f", {len(ecarts)} ecart(s) corrige(s)." if corriger else "."))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--titre")
    ap.add_argument("--ecrire", action="store_true")
    ap.add_argument("--corriger", action="store_true",
                    help="avec --ecrire : remplace les ecarts confirmes par deux documents")
    ap.add_argument("--un-document", action="store_true",
                    help="ecrit aussi les lectures a un seul document (decision du 25/09)")
    a = ap.parse_args()
    main(a.ecrire, a.titre, a.corriger, a.un_document)
