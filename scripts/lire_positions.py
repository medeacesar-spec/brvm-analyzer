#!/usr/bin/env python3
"""Passe la lecture par position (`analysis/lecture_positions.py`) sur les
documents a texte, et confronte a la base.

Un tableau n'est lu que si ses colonnes et son unite sont PROUVEES par des
valeurs deja connues (une par annee du tableau). Le chiffre d'affaires et
le resultat net servent d'ancres et ne sont jamais ecrits.

  TROU      la base est vide — comble avec --ecrire (un document suffit : le
            tableau est verifie ligne a ligne par l'ancrage) ;
  ECART     la base porte autre chose — corrige avec --corriger SEULEMENT si
            deux documents s'accordent ; sinon liste, a lire.

Bornes de vraisemblance (rapport au chiffre d'affaires) : les memes que le
chainage. Un montant hors bornes est deux colonnes recollees ou une autre
unite : ecarte.

Usage :
  python3 scripts/lire_positions.py
  python3 scripts/lire_positions.py --ecrire [--corriger]
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.lecture_bancaire import _egal  # noqa: E402
from analysis.lecture_positions import lire_document, lire_pdf  # noqa: E402
from data.db import read_sql_df  # noqa: E402
from scripts import recouper_par_lecteur as R  # noqa: E402

DEBUT, FIN = 2021, 2025
LUS = ("ebitda", "ebit", "interest_expense", "cfo", "capex", "total_assets", "equity",
       "revenue", "net_income")
BORNES = {"ebitda": (0.002, 1.0), "ebit": (0.0, 1.0), "interest_expense": (0.0005, 0.3),
          "cfo": (0.0, 1.5), "capex": (0.001, 1.5), "total_assets": (0.2, 100.0),
          "equity": (0.0, 30.0)}


def main(ecrire: bool, corriger: bool, ocr: bool = False) -> None:
    df = read_sql_df("SELECT * FROM fundamentals")
    connu = {}
    for r in df.to_dict("records"):
        for c in LUS:
            v = r.get(c)
            if v is not None and v == v and v != 0:
                connu[(r["ticker"], int(r["fiscal_year"]), c)] = float(v)
    banques = set(df[df["sector"] == "Banque"]["ticker"])
    lus = defaultdict(list)                      # (t, a, c) -> [(v, doc)]
    for f in sorted(os.listdir(R.DOSSIER)):
        m = R.FICHIER.match(f)
        if not m:
            continue
        t = m.group(1)
        base = {(a, c): v for (tt, a, c), v in connu.items() if tt == t}
        try:
            lu = lire_document(lire_pdf(os.path.join(R.DOSSIER, f), avec_ocr=ocr), base)
        except Exception:                                        # noqa: BLE001
            continue
        par_an = defaultdict(dict)
        for (a, c), v in lu.items():
            par_an[a][c] = v
        for a, postes in par_an.items():
            if not DEBUT <= a <= FIN:
                continue
            vals = {c: postes[c] for c in ("ebitda", "ebit", "cfo", "total_assets", "equity")
                    if c in postes}
            if "interest_expense" in postes:
                vals["interest_expense"] = abs(postes["interest_expense"])
            if t in banques:
                # Le compte de resultat bancaire a son propre lecteur.
                vals = {c: v for c, v in vals.items() if c in ("total_assets", "equity")}
            elif "capex_corporelles" in postes:
                vals["capex"] = abs(postes["capex_corporelles"]) + abs(
                    postes.get("capex_incorporelles", 0))
            elif "capex_global" in postes:
                vals["capex"] = abs(postes["capex_global"])
            ca = connu.get((t, a, "revenue"))
            for c, v in vals.items():
                lo, hi = BORNES[c]
                if not ca or not (lo * abs(ca) <= abs(v) <= hi * abs(ca) * (30 if t in banques else 1)):
                    continue
                if not any(_egal(v, x) and d == f for x, d in lus[(t, a, c)]):
                    lus[(t, a, c)].append((v, f))

    Md = 1e9
    trous, a_corriger, a_lire, desaccords, concordent = [], [], [], [], 0
    for (t, a, c), sources in sorted(lus.items()):
        v = sources[0][0]
        if not all(_egal(x, v) for x, _ in sources):
            desaccords.append((t, a, c, sources))
            continue
        b = connu.get((t, a, c))
        if b is None:
            trous.append((t, a, c, v))
        elif _egal(v, b):
            concordent += 1
        elif len({d for _, d in sources}) >= 2:
            a_corriger.append((t, a, c, v, b))
        else:
            a_lire.append((t, a, c, v, b))
    for titre, liste in (("TROUS", trous), ("ECARTS A DEUX DOCUMENTS", a_corriger),
                         ("ECARTS A UN DOCUMENT (a lire)", a_lire)):
        print(f"\n{titre}")
        for x in liste:
            print(f"  {x[0]:9} {x[1]} {x[2]:17} {x[3]/Md:10.3f} Md"
                  + (f"  base {x[4]/Md:10.3f}" if len(x) > 4 else ""))
    if desaccords:
        print("\nDESACCORDS ENTRE DOCUMENTS")
        for t, a, c, s in desaccords:
            print(f"  {t} {a} {c} " + " · ".join(f"{x/Md:.3f}" for x, _ in s))
    print(f"\nconcordent {concordent} · trous {len(trous)} · a corriger {len(a_corriger)}"
          f" · a lire {len(a_lire)} · desaccords {len(desaccords)}")
    if not ecrire:
        print("Mode simulation : rien n'a ete ecrit.")
        return
    from data.db import get_connection
    conn = get_connection()
    try:
        for t, a, c, v in trous:
            conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                         f"WHERE ticker = ? AND fiscal_year = ? AND {c} IS NULL", (v, t, a))
        if corriger:
            for t, a, c, v, b in a_corriger:
                print(f"  corrige {t} {a} {c} : {b/Md:.3f} -> {v/Md:.3f} Md")
                conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                             f"WHERE ticker = ? AND fiscal_year = ?", (v, t, a))
        conn.commit()
    finally:
        conn.close()
    print(f"\n{len(trous)} trou(s) comble(s)" + (f", {len(a_corriger)} corrige(s)." if corriger else "."))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    ap.add_argument("--corriger", action="store_true")
    ap.add_argument("--ocr", action="store_true", help="lit aussi les pages scannees (long)")
    a = ap.parse_args()
    main(a.ecrire, a.corriger, a.ocr)
