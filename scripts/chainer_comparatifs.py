#!/usr/bin/env python3
"""Lit EBITDA, resultat d'exploitation, frais financiers et investissements
des societes non bancaires par le chainage des comparatifs
(`analysis/chainage.py`), et confronte a la base.

UNE VALEUR CHAINEE EST CONFIRMEE PAR CONSTRUCTION : elle figure, identique a
un pour mille, dans deux documents consecutifs. Mais la lecture d'un poste
peut tomber sur un sous-total voisin (un « resultat d'exploitation » de
secteur, dans un rapport de gestion). D'ou trois issues :

  concorde   la base porte la meme valeur ;
  TROU       la base est vide — comble avec --ecrire ;
  ecart      la base porte autre chose — liste, JAMAIS ecrase : un ecart
             modere peut tenir a la definition (EBITDA publie ou EBE
             SYSCOHADA), un ecart grossier merite une lecture.

LES INVESTISSEMENTS additionnent les acquisitions d'immobilisations
incorporelles et corporelles ; les financieres n'en sont pas. Les deux
lignes doivent etre chainees, sinon rien n'est lu — sauf si le document
ne publie aucune ligne d'incorporelles.

Usage :
  python3 scripts/chainer_comparatifs.py
  python3 scripts/chainer_comparatifs.py --titre SMBC.ci
  python3 scripts/chainer_comparatifs.py --ecrire
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.chainage import POSTES, candidats, facteur, lire  # noqa: E402
from analysis.lecture_bancaire import _egal  # noqa: E402
from data.db import read_sql_df  # noqa: E402
from scripts import recouper_par_lecteur as R  # noqa: E402

DEBUT, FIN = 2021, 2025
CHAMPS = ("ebitda", "ebit", "interest_expense", "capex", "total_assets", "equity", "cfo")
# Pour une banque, seuls les soldes de bilan : son compte de resultat a son
# propre lecteur (`recouper_bancaire.py`).
BANCAIRES = ("total_assets", "equity")
# Plafonds rapportes au chiffre d'affaires : au-dela, un sous-total voisin
# ou une autre unite a ete lu.
PLAFOND = {"ebitda": 1.0, "ebit": 1.0, "interest_expense": 0.3, "capex": 1.5,
           "total_assets": 100.0, "equity": 30.0, "cfo": 1.5}
# Planchers : en deca, c'est un numero de note ou un pourcentage qui s'est
# chaine (SAPH, frais financiers « 0,002 Md » pour 224 Md de chiffre
# d'affaires).
PLANCHER = {"ebitda": 0.002, "ebit": 0.002, "interest_expense": 0.0005, "capex": 0.001,
            "total_assets": 0.2, "equity": 0.005, "cfo": 0.001}
# Au-dela d'un facteur deux, la base n'a pas une autre definition du solde :
# elle a tort (colonne de variation, copie, unite). En deca, l'ecart peut
# tenir au perimetre (part du groupe ou total, social ou consolide) : on le
# liste. Meme regle que pour les encours bancaires.
GROSSIER = 2.0


def _isole(t, an, c, v, lus, connu) -> bool:
    """Vrai si `v` s'ecarte d'un facteur trois de chacun de ses voisins
    connus (lecture chainee, sinon base), et qu'il en a au moins un."""
    voisins = []
    for a in (an - 1, an + 1):
        x = lus.get((a, c))
        if x is None and connu.get((t, a)) is not None:
            x = getattr(connu[(t, a)], c)
            x = float(x) if x == x and x else None
        if x:
            voisins.append(x)
    return bool(voisins) and all(not 1 / 3 <= abs(v / x) <= 3 for x in voisins)


def main(ecrire: bool, titre: str = None, corriger: bool = False) -> None:
    base = read_sql_df("SELECT ticker, fiscal_year, revenue, " + ", ".join(CHAMPS)
                       + ", sector FROM fundamentals")
    connu = {(r.ticker, int(r.fiscal_year)): r for r in base.itertuples()}
    titres = set(base.ticker)
    par_titre = {}
    for f in sorted(os.listdir(R.DOSSIER)):
        m = R.FICHIER.match(f)
        if not m or m.group(1) not in titres or (titre and m.group(1) != titre):
            continue
        t, an = m.group(1), int(m.group(2))
        try:
            texte, _ = R.texte(os.path.join(R.DOSSIER, f), True)
        except Exception:                                        # noqa: BLE001
            continue
        if not texte:
            continue
        revenus = [float(x.revenue) for x in (connu.get((t, an)), connu.get((t, an - 1)))
                   if x is not None and x.revenue == x.revenue and x.revenue]
        fct = facteur(texte, revenus)
        if fct is None:
            continue
        docs = par_titre.setdefault(t, {})
        # Deux documents du meme exercice (etats financiers et rapport) : on
        # les reunit, le chainage ne retient que ce qui revient d'une annee
        # sur l'autre.
        postes = docs.setdefault(an, {})
        for poste, motif in POSTES.items():
            for v, neg in candidats(texte, motif).items():
                postes.setdefault(poste, {})[v * fct] = neg

    Md = 1e9
    issues = {"concorde": [], "TROU": [], "GROSSIER": [], "ecart": []}
    for t, docs in sorted(par_titre.items()):
        lus = lire(docs)
        for an in range(DEBUT, FIN + 1):
            ligne = connu.get((t, an))
            if ligne is None or not (ligne.revenue == ligne.revenue and ligne.revenue):
                continue
            banque = (ligne.sector or "").lower().startswith("banque")
            valeurs = {c: lus.get((an, c)) for c in ("ebitda", "ebit", "interest_expense",
                                                     "total_assets", "equity", "cfo")}
            if banque:
                valeurs = {c: v for c, v in valeurs.items() if c in BANCAIRES}
            corp, incorp = lus.get((an, "capex_corporelles")), lus.get((an, "capex_incorporelles"))
            sans_incorporelles = not any(docs.get(a, {}).get("capex_incorporelles")
                                         for a in (an, an + 1))
            if not banque and corp is not None and (incorp is not None or sans_incorporelles):
                valeurs["capex"] = corp + (incorp or 0)
            # L'EBE couvre le resultat d'exploitation plus les dotations nettes :
            # plus petit que lui, l'un des deux est un sous-total voisin (NEI-CEDA
            # 2021 : 0,787 contre 0,788). On ne garde ni l'un ni l'autre.
            e, r = valeurs.get("ebitda"), valeurs.get("ebit")
            if e is not None and r is not None and e < r and not _egal(e, r, 0) or (
                    e is not None and r is not None and _egal(e, r, 0)):
                valeurs["ebitda"] = valeurs["ebit"] = None
            for c, v in valeurs.items():
                if v is None or not (PLANCHER[c] * abs(ligne.revenue) <= abs(v)
                                     <= PLAFOND[c] * abs(ligne.revenue)):
                    continue
                # UNE TRONCATURE PEUT SE CHAINER. L'OCR de NEI-CEDA perd le
                # MEME premier chiffre dans deux documents consecutifs
                # (« 637 643 384 » pour 5 637 643 384) : le commun existe, et
                # il est faux. Un solde de bilan qui s'ecarte d'un facteur
                # trois de ses deux voisins est ecarte.
                if c in ("equity", "total_assets") and _isole(t, an, c, v, lus, connu):
                    continue
                b = getattr(ligne, c)
                b = float(b) if b == b and b is not None else None
                if b is None:
                    issue = "TROU"
                elif _egal(v, b):
                    issue = "concorde"
                elif not 1 / GROSSIER <= abs(v / b) <= GROSSIER or (v < 0) != (b < 0) and c == "equity":
                    issue = "GROSSIER"
                # Une COPIE de l'exercice precedent n'est pas une affaire de
                # definition : TotalEnergies CI portait en 2022 le total et les
                # capitaux propres de 2021, que le chainage lit pour 2021.
                elif lus.get((an - 1, c)) is not None and _egal(b, lus[(an - 1, c)]):
                    issue = "GROSSIER"
                else:
                    issue = "ecart"
                issues[issue].append((t, an, c, v, b))

    for issue in ("TROU", "GROSSIER", "ecart"):
        print(f"\n{issue}")
        for t, an, c, v, b in issues[issue]:
            print(f"  {t:9} {an} {c:17} {v/Md:10.3f} Md"
                  + (f"  base {b/Md:10.3f} Md  x{v/b:5.2f}" if b else ""))
    print(f"\nconcorde : {len(issues['concorde'])} · trous : {len(issues['TROU'])}"
          f" · ecarts grossiers : {len(issues['GROSSIER'])} · ecarts moderes (a lire) : "
          f"{len(issues['ecart'])}")
    if not ecrire:
        print("\nMode simulation : rien n'a ete ecrit.")
        return
    from data.db import get_connection
    conn = get_connection()
    try:
        for t, an, c, v, _ in issues["TROU"]:
            conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                         f"WHERE ticker = ? AND fiscal_year = ? AND {c} IS NULL", (v, t, an))
        if corriger:
            for t, an, c, v, b in issues["GROSSIER"]:
                print(f"  corrige {t} {an} {c} : {b/Md:.3f} -> {v/Md:.3f} Md")
                conn.execute(f"UPDATE fundamentals SET {c} = ?, updated_at = CURRENT_TIMESTAMP "
                             f"WHERE ticker = ? AND fiscal_year = ?", (v, t, an))
        conn.commit()
    finally:
        conn.close()
    print(f"\n{len(issues['TROU'])} trou(s) comble(s)"
          + (f", {len(issues['GROSSIER'])} ecart(s) grossier(s) corrige(s)." if corriger else "."))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--titre")
    ap.add_argument("--ecrire", action="store_true")
    ap.add_argument("--corriger", action="store_true",
                    help="avec --ecrire : corrige les ecarts grossiers (facteur deux)")
    a = ap.parse_args()
    main(a.ecrire, a.titre, a.corriger)
