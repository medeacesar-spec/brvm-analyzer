#!/usr/bin/env python3
"""Sonde de vraisemblance des fondamentaux annuels. N'ECRIT RIEN.

POURQUOI

Les lecteurs d'etats financiers recoupent chaque montant avec le chiffre
d'affaires et le resultat net connus. Quand ces deux-la sont justes et le
reste faux, rien ne sonne. La grille telecoms de Sonatel affichait ainsi, le
25/09/2026, une marge d'EBITDA de 0 % et une dette de 616 fois l'EBITDA :
chiffre d'affaires du GROUPE, resultat d'exploitation de la SOCIETE MERE
seule, EBITDA illisible. Orange CI portait dans ses lignes annuelles des
montants a neuf mois. Aucune de ces valeurs n'etait impossible isolement ;
toutes l'etaient pour le metier.

La sonde pose donc les questions du metier, sur chaque exercice 2021-2025 :

  EBIT > EBITDA              le resultat d'exploitation ne depasse pas l'EBE
  marge                      EBITDA, EBIT hors de [-50 %, 80 %] du CA ;
                             resultat net au-dela du CA (hors banques)
  copie                      le meme montant deux exercices de suite
  saut                       un facteur trois ou plus d'un exercice a
                             l'autre, sur un poste de stock ou de flux stable
  bilan                      capitaux propres ou dette au-dela du total
  periode                    un montant annuel egal a un montant trimestriel
                             ou semestriel du meme exercice (`quarterly_data`)
  banque                     credits / depots hors de [0,3 ; 1,5] ; depots
                             au-dela du total du bilan

Chaque constat dit si la valeur a ete ecrite par le lot du 14/09/2026
(11 h 18 - 11 h 53, 2 145 valeurs reecrites, dont les demi-exercices de
Sonatel) : c'est la premiere piste a suivre.

EFFACER LES RESULTATS D'EXPLOITATION DE PERIODE (--effacer-periodes)

Decision du 25/09/2026 : un resultat d'exploitation annuel egal a celui
d'UNE publication trimestrielle ou semestrielle du meme exercice, ecrit par
le lot du 14/09, est faux a coup sur. Quand aucun document annuel ne permet
de le remplacer (les lecteurs `recouper_resultat.py` et
`recouper_bancaire.py` passent avant), il est vide : une case vide vaut
mieux qu'un trimestre pris pour une annee. Le chiffre d'affaires et le
resultat net, recoupes a la fiche societe, ne sont jamais vides ; une
egalite avec deux publications a la fois (BICI Benin 2025) est ambigue et
laissee. L'ancienne valeur reste dans `fundamentals_journal`.

Usage :
  python3 scripts/sonde_vraisemblance.py
  python3 scripts/sonde_vraisemblance.py --titre ORAC.ci
  python3 scripts/sonde_vraisemblance.py --effacer-periodes
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import read_sql_df  # noqa: E402

DEBUT, FIN = 2021, 2025
Md = 1e9
STABLES = ("revenue", "equity", "total_assets", "ebitda", "capex", "cfo",
           "deposits", "loans")
COPIES = ("revenue", "equity", "total_assets", "total_debt", "ebitda", "ebit",
          "capex", "cfo", "deposits", "loans", "operating_expenses",
          "gross_operating_income", "interest_expense")
LOT = ("2026-09-14 11:18", "2026-09-14 11:54")
# Les montants qui ne peuvent pas etre derisoires (25/09/2026). Des frais
# financiers de 29 FCFA pour 19 Md de chiffre d'affaires (Sicable 2024) sont
# une erreur d'unite, pas une gestion exemplaire : rapportes au chiffre
# d'affaires, ils tombent sous le cent-millieme.
MONTANTS = ("interest_expense", "capex", "total_debt", "ebitda", "ebit", "cfo", "equity",
            "total_assets", "operating_expenses", "gross_operating_income", "deposits",
            "loans", "cost_of_risk")
DERISOIRE = 1e-5
# La dette, ses interets, les investissements et le cout du risque peuvent
# etre reellement minimes : SITAB porte 1,4 million de FCFA d'emprunts en
# 2025, pour 268 Md de chiffre d'affaires. Pour eux, seul un montant sous
# cent mille FCFA trahit une erreur d'unite.
PEUVENT_ETRE_MINIMES = ("interest_expense", "total_debt", "capex", "cost_of_risk")
PLANCHER_ABSOLU = 1e5


def _v(ligne, champ):
    x = ligne.get(champ)
    return float(x) if x is not None and x == x and x != 0 else None


def main(titre: str = None, effacer: bool = False) -> None:
    base = read_sql_df("SELECT * FROM fundamentals WHERE fiscal_year BETWEEN ? AND ?",
                       params=(DEBUT - 1, FIN))
    if titre:
        base = base[base.ticker == titre]
    lignes = {(r["ticker"], int(r["fiscal_year"])): r for r in base.to_dict("records")}
    trim = read_sql_df("SELECT ticker, fiscal_year, periode, revenue, net_income, ebit "
                       "FROM quarterly_data WHERE fiscal_year BETWEEN ? AND ?",
                       params=(DEBUT, FIN))
    periodes = defaultdict(list)
    for r in trim.to_dict("records"):
        for c in ("revenue", "net_income", "ebit"):
            if r[c] == r[c] and r[c]:
                periodes[(r["ticker"], int(r["fiscal_year"]), c)].append((r["periode"], float(r[c])))
    lot = read_sql_df("SELECT DISTINCT ticker, fiscal_year, champ FROM fundamentals_journal "
                      "WHERE ecrit_le BETWEEN ? AND ?", params=LOT)
    du_lot = {(r.ticker, int(r.fiscal_year), r.champ) for r in lot.itertuples()}

    constats = []
    a_effacer = []

    def noter(t, an, champs, genre, detail):
        marque = " [lot 14/09]" if any((t, an, c) in du_lot for c in champs) else ""
        constats.append((t, an, genre, detail + marque))

    for (t, an), l in sorted(lignes.items()):
        if not DEBUT <= an <= FIN:
            continue
        banque = (l.get("sector") or "").lower().startswith("banque")
        ca, rn = _v(l, "revenue"), _v(l, "net_income")
        e, x = _v(l, "ebitda"), _v(l, "ebit")
        if not banque and e is not None and x is not None and x > e * 1.01 and x > 0:
            noter(t, an, ("ebit", "ebitda"), "EBIT > EBITDA",
                  f"EBIT {x/Md:.2f} > EBITDA {e/Md:.2f} Md")
        if ca and not banque:
            for c, v in (("ebitda", e), ("ebit", x)):
                if v is not None and not -0.5 <= v / ca <= 0.8:
                    noter(t, an, (c, "revenue"), "marge", f"{c} = {v/ca:.0%} du CA "
                          f"({v/Md:.2f} / {ca/Md:.2f} Md)")
            if rn is not None and abs(rn) > ca:
                noter(t, an, ("net_income", "revenue"), "marge",
                      f"resultat net {rn/Md:.2f} > CA {ca/Md:.2f} Md")
            for c in ("capex", "cfo"):
                v = _v(l, c)
                if v is not None and abs(v) > 1.2 * ca:
                    noter(t, an, (c,), "marge", f"{c} {v/Md:.2f} > 1,2 x CA {ca/Md:.2f} Md")
        if ca and abs(ca) > 1e9:
            for c in MONTANTS:
                v = _v(l, c)
                if v is not None and abs(v) < DERISOIRE * abs(ca) and (
                        c not in PEUVENT_ETRE_MINIMES or abs(v) < PLANCHER_ABSOLU):
                    noter(t, an, (c,), "derisoire", f"{c} = {v:,.0f} FCFA pour un CA de "
                          f"{ca/Md:.2f} Md : erreur d'unite")
        # Les ratios de la grille, bornes comme a l'affichage
        # (`analysis.sectors.PLAUSIBLE`).
        i = _v(l, "interest_expense")
        if not banque and x is not None and i and not -100 <= x / abs(i) <= 100_000:
            noter(t, an, ("ebit", "interest_expense"), "ratio",
                  f"couverture des interets {x/abs(i):,.0f} x ({x/Md:.2f} Md / "
                  f"{abs(i)/Md:.4f} Md)")
        dt = _v(l, "total_debt")
        if not banque and dt is not None and e and e > 0 and dt / e > 30:
            noter(t, an, ("total_debt", "ebitda"), "ratio",
                  f"dette / EBITDA {dt/e:.1f} x ({dt/Md:.2f} / {e/Md:.2f} Md)")
        prec = lignes.get((t, an - 1))
        if prec is not None:
            for c in COPIES:
                a, b = _v(l, c), _v(prec, c)
                if a is not None and b is not None and abs(a) > 1e8 and a == b:
                    noter(t, an, (c,), "copie", f"{c} = {a/Md:.3f} Md, comme en {an-1}")
            for c in STABLES:
                a, b = _v(l, c), _v(prec, c)
                if a and b and (a / b >= 3 or a / b <= 1 / 3) and abs(max(a, b)) > 1e9:
                    noter(t, an, (c,), "saut", f"{c} {b/Md:.2f} -> {a/Md:.2f} Md (x{a/b:.2f})")
        tot = _v(l, "total_assets")
        if tot:
            for c in ("equity", "total_debt", "deposits", "loans"):
                v = _v(l, c)
                if v is not None and v > tot * 1.01:
                    noter(t, an, (c, "total_assets"), "bilan",
                          f"{c} {v/Md:.2f} > total {tot/Md:.2f} Md")
        if banque:
            d, cr = _v(l, "deposits"), _v(l, "loans")
            if d and cr and not 0.3 <= cr / d <= 1.5:
                noter(t, an, ("loans", "deposits"), "banque",
                      f"credits / depots = {cr/d:.2f} ({cr/Md:.1f} / {d/Md:.1f} Md)")
        for c in ("revenue", "net_income", "ebit"):
            v = _v(l, c)
            egales = [per for per, p in periodes.get((t, an, c), [])
                      if v is not None and per not in ("T4", "S2", "A")
                      and abs(v - p) <= 0.005 * abs(p)]
            if c == "ebit" and len(set(egales)) == 1 and (t, an, c) in du_lot:
                a_effacer.append((t, an, c, v, egales[0]))
            for per, p in periodes.get((t, an, c), []):
                # T4 et S2 sont des cumuls a douze mois : l'exercice lui-meme.
                if v is not None and per not in ("T4", "S2", "A") and abs(v - p) <= 0.005 * abs(p):
                    noter(t, an, (c,), "periode", f"{c} {v/Md:.2f} Md = publication {per}")

    par_genre = defaultdict(int)
    for t, an, g, d in constats:
        par_genre[g] += 1
    courant = None
    for t, an, g, d in constats:
        if t != courant:
            print(f"\n{t}")
            courant = t
        print(f"  {an}  {g:14} {d}")
    print(f"\n{len(constats)} constat(s) sur {len({c[0] for c in constats})} titre(s) : "
          + " · ".join(f"{g} {n}" for g, n in sorted(par_genre.items())))
    print(f"dont {sum('[lot 14/09]' in c[3] for c in constats)} sur des valeurs ecrites par le lot du 14/09")
    if not effacer:
        return
    from data.db import get_connection
    conn = get_connection()
    try:
        for t, an, c, v, per in a_effacer:
            print(f"  vide {t} {an} {c} {v/Md:.2f} Md (= publication {per})")
            conn.execute(f"UPDATE fundamentals SET {c} = NULL, updated_at = CURRENT_TIMESTAMP "
                         f"WHERE ticker = ? AND fiscal_year = ?", (t, an))
        conn.commit()
    finally:
        conn.close()
    print(f"{len(a_effacer)} resultat(s) d'exploitation de periode vide(s).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--titre")
    ap.add_argument("--effacer-periodes", action="store_true",
                    help="vide les resultats d'exploitation de periode du lot du 14/09")
    a = ap.parse_args()
    main(a.titre, a.effacer_periodes)
