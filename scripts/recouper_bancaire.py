#!/usr/bin/env python3
"""Passe le lecteur bancaire (`analysis/lecture_bancaire.py`) sur les
documents des banques, et confronte ses lectures a la base.

UNE LECTURE N'EST RETENUE QUE SI

  - le PNB de la colonne retombe sur celui de la base (il fixe la colonne
    et l'unite) ;
  - les soldes de la colonne s'enchainent : PNB - charges = RBE, et
    RBE +/- cout du risque = resultat d'exploitation, pour celles des deux
    identites dont les postes sont lus.

LES ENCOURS (credits et depots de la clientele) se lisent autrement : par
le chainage des comparatifs (voir `lecture_bancaire.encours`). Un ecart
grossier (facteur deux) est corrige avec `--corriger` ; un ecart modere,
qui peut tenir au perimetre, est seulement liste.

UNE VALEUR N'EST CONFIRMEE QUE PAR DEUX DOCUMENTS

L'exercice N se lit dans le document N (colonne de l'exercice) et dans le
document N+1 (colonne comparative). Les deux lectures doivent concorder a
un pour mille. C'est la regle de toutes les corrections de la base : deux
sources, jamais une.

Avec `--ecrire`, seules les valeurs CONFIRMEES qui comblent un TROU sont
ecrites. Avec `--corriger` en plus, les valeurs CONFIRMEES qui contredisent
la base la remplacent : deux documents d'accord, et des soldes qui
s'enchainent, contre une valeur qui ne s'enchaine pas. Au 25/09/2026, la
plupart de ces ecarts etaient l'exercice PRECEDENT recopie (le cout du
risque de NSIA Banque, le resultat d'exploitation de la SGBCI : la base
portait en N la valeur de N-1). L'ancienne valeur est imprimee.

Seuls les exercices 2021 a 2025 sont ecrits.

Usage :
  python3 scripts/recouper_bancaire.py            # simulation
  python3 scripts/recouper_bancaire.py --titre SGBC.ci
  python3 scripts/recouper_bancaire.py --ecrire
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.lecture_bancaire import chainer, encours, lire_exercice, _egal  # noqa: E402
from data.db import read_sql_df  # noqa: E402
from scripts import recouper_par_lecteur as R  # noqa: E402

DEBUT, FIN = 2021, 2025
# poste lu -> colonne de la base
COLONNES = {
    "charges": "operating_expenses",
    "rbe": "gross_operating_income",
    "cdr": "cost_of_risk",
    "rex": "ebit",
}


def lectures(titre: str = None) -> tuple:
    base = read_sql_df(
        "SELECT ticker, fiscal_year, revenue, net_income, operating_expenses, "
        "gross_operating_income, cost_of_risk, ebit FROM fundamentals "
        "WHERE sector = 'Banque'")
    connu = {(r.ticker, int(r.fiscal_year)): r for r in base.itertuples()}
    banques = set(base.ticker)
    fichiers = sorted(f for f in os.listdir(R.DOSSIER)
                      if R.FICHIER.match(f) and R.FICHIER.match(f).group(1) in banques)
    if titre:
        fichiers = [f for f in fichiers if f.startswith(titre)]

    # (ticker, exercice, poste) -> [(valeur, document)]
    lu = defaultdict(list)
    rejets = 0
    for f in fichiers:
        ticker, annee = R.FICHIER.match(f).groups()
        annee = int(annee)
        try:
            texte, _ = R.texte(os.path.join(R.DOSSIER, f), True)
        except Exception as err:                                # noqa: BLE001
            print(f"  {f[:60]:60} illisible : {err}")
            continue
        if texte is None:
            continue
        for exercice in (annee, annee - 1):
            ligne = connu.get((ticker, exercice))
            if ligne is None or not (ligne.revenue == ligne.revenue and ligne.revenue):
                continue
            rn = ligne.net_income if ligne.net_income == ligne.net_income else None
            for lecture in lire_exercice(texte, float(ligne.revenue), rn):
                ids = lecture["_identites"]
                if not ids or not all(ids.values()):
                    rejets += 1
                    continue
                valeurs = {"charges": lecture.get("charges"), "rbe": lecture.get("rbe"),
                           "cdr": lecture.get("cdr"), "rex": lecture.get("rex")}
                for poste, v in valeurs.items():
                    if v is None:
                        continue
                    # Un poste n'est sur que si une identite le contient.
                    if poste == "charges" and "pnb-charges=rbe" not in ids:
                        continue
                    if poste in ("cdr", "rex") and "rbe+cdr=rex" not in ids:
                        continue
                    if not any(_egal(v, x) and d == f for x, d in lu[(ticker, exercice, poste)]):
                        lu[(ticker, exercice, poste)].append((v, f))
    return lu, connu, rejets


# Un encours de clientele vaut entre 2 et 60 fois le PNB de l'exercice
# (SGBCI : 10 fois). L'ecart entre deux unites etant de mille, une seule
# unite tombe dans cette bande : c'est ainsi qu'on la fixe, quand le
# document l'annonce mal (SIB : un tableau en millions lu en milliers).
BANDE_PNB = (2, 60)
# Au-dela d'un facteur deux, la base n'a pas une autre definition de
# l'encours : elle a tort (SGBCI 2022-2024, dix fois l'exercice precedent).
# En deca, l'ecart peut tenir au perimetre (social ou consolide, brut ou
# net) : on le liste, a lire.
GROSSIER = 2.0


def encours_chaines(titre: str = None) -> list:
    """[(ticker, exercice, champ, valeur, en_base)] : les encours de fin
    d'exercice communs aux documents N et N+1, remis a l'unite par le PNB."""
    base = read_sql_df("SELECT ticker, fiscal_year, revenue, loans, deposits "
                       "FROM fundamentals WHERE sector = 'Banque'")
    connu = {(r.ticker, int(r.fiscal_year)): r for r in base.itertuples()}
    banques = set(base.ticker)
    lus = {}
    for f in sorted(os.listdir(R.DOSSIER)):
        m = R.FICHIER.match(f)
        if not m or m.group(1) not in banques or (titre and m.group(1) != titre):
            continue
        try:
            texte, _ = R.texte(os.path.join(R.DOSSIER, f), True)
        except Exception:                                        # noqa: BLE001
            continue
        if not texte:
            continue
        for champ, valeurs in encours(texte).items():
            lus.setdefault((m.group(1), int(m.group(2)), champ), set()).update(valeurs)
    sortie = []
    for (t, a, champ), valeurs in sorted(lus.items()):
        ligne = connu.get((t, a))
        pnb = float(ligne.revenue) if ligne is not None and ligne.revenue == ligne.revenue else None
        if not pnb:
            continue
        retenus = []
        for v in chainer(valeurs, lus.get((t, a + 1, champ), set())):
            for f in (1e-6, 1e-3, 1.0, 1e3, 1e6):
                if BANDE_PNB[0] <= v * f / pnb <= BANDE_PNB[1]:
                    retenus.append(v * f)
        if len(retenus) != 1:                    # rien, ou deux candidats
            continue
        b = getattr(ligne, champ)
        sortie.append((t, a, champ, retenus[0], float(b) if b == b and b is not None else None))
    return sortie


def main(ecrire: bool, titre: str = None, corriger: bool = False,
         un_document: bool = False) -> None:
    lu, connu, rejets = lectures(titre)
    Md = 1e9
    confirmees, seules, desaccords = [], [], []
    for (ticker, exercice, poste), sources in sorted(lu.items()):
        docs = {d for _, d in sources}
        valeur = sources[0][0]
        d_accord = all(_egal(v, valeur) for v, _ in sources)
        if not d_accord:
            desaccords.append((ticker, exercice, poste, sources))
            continue
        ligne = connu.get((ticker, exercice))
        colonne = COLONNES[poste]
        en_base = getattr(ligne, colonne) if ligne is not None else None
        en_base = float(en_base) if en_base == en_base and en_base is not None else None
        fiche = (ticker, exercice, poste, colonne, valeur, en_base, sorted(docs))
        (confirmees if len(docs) >= 2 else seules).append(fiche)

    def _issue(v, b):
        if b is None:
            return "TROU"
        return "concorde" if _egal(v, b) else "ECART"

    print(f"\nlectures ecartees (soldes qui ne s'enchainent pas) : {rejets}\n")
    for titre_bloc, liste in (("CONFIRMEES — deux documents d'accord", confirmees),
                              ("UN SEUL DOCUMENT — soldes enchaines, a lire", seules)):
        print(titre_bloc)
        for t, a, p, c, v, b, docs in liste:
            print(f"  {t:9} {a} {c:24} {v/Md:10.3f} Md  "
                  + (f"base {b/Md:10.3f} Md  " if b is not None else "")
                  + _issue(v, b))
        print()
    if desaccords:
        print("DESACCORDS ENTRE DOCUMENTS — a lire")
        for t, a, p, sources in desaccords:
            print(f"  {t:9} {a} {p:8} " + " · ".join(f"{v/Md:.3f} ({d[:22]})" for v, d in sources))
        print()

    confirmees = [x for x in confirmees if DEBUT <= x[1] <= FIN]
    if un_document:
        # Decision du 25/09 (sonde de vraisemblance) : les lectures a un seul
        # document, soldes enchaines, s'ecrivent aussi — la base portait des
        # resultats d'exploitation TRIMESTRIELS dans ses lignes annuelles
        # (NSIA 2025 : 7,94 Md, celui du T1 ; le document annuel dit 42,77).
        # Meme garde-fou que pour le compte de resultat : un montant lu a
        # l'identique pour deux exercices du meme titre trahit des colonnes
        # melees et n'est pas ecrit.
        par_poste = defaultdict(list)
        for t, a, p, c, v, b, docs in seules:
            par_poste[(t, c)].append((a, v))
        doubles = {(t, a, c) for (t, c), lus in par_poste.items()
                   for a, v in lus for a2, v2 in lus if a2 != a and _egal(v, v2)}
        confirmees = confirmees + [x for x in seules if DEBUT <= x[1] <= FIN
                                   and (x[0], x[1], x[3]) not in doubles]
    trous = [x for x in confirmees if x[5] is None]
    ecarts = [x for x in confirmees if x[5] is not None and not _egal(x[4], x[5])]
    print(f"confirmees : {len(confirmees)} (concordent {len(confirmees)-len(trous)-len(ecarts)}"
          f" · trous {len(trous)} · ecarts {len(ecarts)}) · un seul document : {len(seules)}"
          f" · desaccords : {len(desaccords)}")

    print("\nENCOURS — chaines entre deux documents consecutifs")
    en_trou, en_ecart, a_lire = [], [], []
    for t, a, c, v, b in encours_chaines(titre):
        if not DEBUT <= a <= FIN:
            continue
        if b is None:
            issue = "TROU"
            en_trou.append((t, a, c, v))
        elif _egal(v, b):
            issue = "concorde"
        elif not 1 / GROSSIER <= v / b <= GROSSIER:
            issue = "ECART GROSSIER"
            en_ecart.append((t, a, c, v, b))
        else:
            issue = "ecart a lire (perimetre ?)"
            a_lire.append((t, a, c, v, b))
        print(f"  {t:9} {a} {c:9} {v/Md:10.3f} Md  "
              + (f"base {b/Md:10.3f} Md  " if b is not None else "") + issue)

    if not ecrire:
        print("\nMode simulation : rien n'a ete ecrit.")
        return
    from data.db import get_connection
    conn = get_connection()
    try:
        for t, a, p, c, v, b, docs in trous:
            conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                         f"WHERE ticker = ? AND fiscal_year = ? AND {c} IS NULL", (v, t, a))
        for t, a, c, v in en_trou:
            conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                         f"WHERE ticker = ? AND fiscal_year = ? AND {c} IS NULL", (v, t, a))
        if corriger:
            for t, a, c, v, b in en_ecart:
                print(f"  corrige {t} {a} {c} : {b/Md:.3f} -> {v/Md:.3f} Md")
                conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                             f"WHERE ticker = ? AND fiscal_year = ?", (v, t, a))
            for t, a, p, c, v, b, docs in ecarts:
                print(f"  corrige {t} {a} {c} : {b/Md:.3f} -> {v/Md:.3f} Md")
                conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                             f"WHERE ticker = ? AND fiscal_year = ?", (v, t, a))
        conn.commit()
    finally:
        conn.close()
    print(f"\n{len(trous) + len(en_trou)} trou(s) comble(s)"
          + (f", {len(ecarts) + len(en_ecart)} ecart(s) corrige(s)." if corriger else ".")
          + f" {len(a_lire)} ecart(s) d'encours a lire, non touches.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--titre")
    ap.add_argument("--ecrire", action="store_true")
    ap.add_argument("--corriger", action="store_true",
                    help="avec --ecrire : remplace aussi les ecarts confirmes par deux documents")
    ap.add_argument("--un-document", action="store_true",
                    help="ecrit aussi les lectures a un seul document (decision du 25/09)")
    a = ap.parse_args()
    main(a.ecrire, a.titre, a.corriger, a.un_document)
