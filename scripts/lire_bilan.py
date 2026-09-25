#!/usr/bin/env python3
"""Comble le total du bilan et les capitaux propres manquants avec UN
document (`analysis/lecture_bilan.py`). Ne corrige jamais une valeur.

LA COLONNE EST CONFIRMEE DEUX FOIS

Le resultat net de l'exercice fixe la colonne. Mais un meme document peut
ranger ses tableaux dans deux ordres : le rapport SGBCI 2024 donne le
resultat 2024 en premiere colonne et les capitaux propres 2024 en seconde.
Lue par le seul resultat net, la SGBCI 2024 ressortait avec les capitaux
propres de 2023.

On lit donc le document pour DEUX exercices, N et son voisin (N-1 dans le
document N, N+1 dans le document N+1). La lecture de N n'est retenue que
si celle du voisin retombe sur la valeur deja en base : l'autre colonne
porte bien l'autre exercice, ligne par ligne.

Usage :
  python3 scripts/lire_bilan.py            # simulation
  python3 scripts/lire_bilan.py --ecrire
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.lecture_bancaire import _egal  # noqa: E402
from analysis.lecture_bilan import lire  # noqa: E402
from data.db import read_sql_df  # noqa: E402
from scripts import recouper_par_lecteur as R  # noqa: E402

DEBUT, FIN = 2021, 2025
# Contredits par l'ancre inversee (scripts/ancre_inverse.py), autre methode
# lisant le meme document : a lire a la main, jamais ecrits. Le 25/09, les
# deux methodes s'accordaient sur neuf valeurs et divergeaient sur celle-ci
# (14,023 contre 10,803 Md).
CONTREDITS = {("SIVC.ci", 2024, "total_assets")}
CHAMPS = ("equity", "total_assets")


def _v(ligne, c):
    if ligne is None:
        return None
    x = getattr(ligne, c)
    return float(x) if x is not None and x == x and x != 0 else None


def main(ecrire: bool) -> None:
    base = read_sql_df("SELECT ticker, fiscal_year, net_income, equity, total_assets "
                       "FROM fundamentals")
    connu = {(r.ticker, int(r.fiscal_year)): r for r in base.itertuples()}
    trouves = {}                                  # (t, an, champ) -> {valeur, doc}
    for f in sorted(os.listdir(R.DOSSIER)):
        m = R.FICHIER.match(f)
        if not m:
            continue
        t, an = m.group(1), int(m.group(2))
        cibles = [a for a in (an, an - 1) if DEBUT <= a <= FIN
                  and any(_v(connu.get((t, a)), c) is None for c in CHAMPS)]
        if not cibles:
            continue
        try:
            texte, _ = R.texte(os.path.join(R.DOSSIER, f), True)
        except Exception:                                        # noqa: BLE001
            continue
        if not texte:
            continue
        lectures = {}
        for a in (an, an - 1):
            rn = _v(connu.get((t, a)), "net_income")
            if rn is not None:
                lectures[a] = lire(texte, rn)
        for a in cibles:
            voisin = an - 1 if a == an else an
            for c in CHAMPS:
                if _v(connu.get((t, a)), c) is not None or c not in lectures.get(a, {}):
                    continue
                attendu = _v(connu.get((t, voisin)), c)
                lu_voisin = lectures.get(voisin, {}).get(c)
                if attendu is None or lu_voisin is None or not _egal(lu_voisin, attendu):
                    continue
                v = lectures[a][c]
                if _egal(v, attendu):               # la meme colonne lue deux fois
                    continue
                trouves.setdefault((t, a, c), set()).add(round(v))
    Md = 1e9
    retenus = []
    for (t, a, c), vals in sorted(trouves.items()):
        if (t, a, c) in CONTREDITS:
            print(f"  contredit {t} {a} {c} : a lire")
            continue
        if len(vals) != 1:
            print(f"  desaccord {t} {a} {c} : " + " · ".join(f"{x/Md:.3f}" for x in vals))
            continue
        v = float(vals.pop())
        retenus.append((t, a, c, v))
        print(f"  {t:9} {a} {c:13} {v/Md:10.3f} Md")
    print(f"\n{len(retenus)} trou(s) a combler.")
    if not ecrire:
        print("Mode simulation : rien n'a ete ecrit.")
        return
    from data.db import get_connection
    conn = get_connection()
    try:
        for t, a, c, v in retenus:
            conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                         f"WHERE ticker = ? AND fiscal_year = ? AND {c} IS NULL", (v, t, a))
        conn.commit()
    finally:
        conn.close()
    print("Ecrit.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
