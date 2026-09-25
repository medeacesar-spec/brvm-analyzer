"""Lecture du compte de resultat SYSCOHADA en bloc, soldes enchaines.

LA METHODE DU LECTEUR BANCAIRE, APPLIQUEE AUX AUTRES SOCIETES

Le compte de resultat SYSCOHADA enchaine ses soldes :

    XD  EXCEDENT BRUT D'EXPLOITATION
    TJ  + reprises d'amortissements et de provisions
    RL  - dotations aux amortissements et aux provisions
    XE  = RESULTAT D'EXPLOITATION
    TK..RN  produits et charges financiers (dont RM, les frais financiers)
    XF  = RESULTAT FINANCIER
    XG  = RESULTAT DES ACTIVITES ORDINAIRES        (XE + XF)
    XH  + RESULTAT HORS ACTIVITES ORDINAIRES
    RQ, RS  - participation, impot sur le resultat
    XI  = RESULTAT NET

Le resultat net est connu pour 97 % des exercices : il fixe l'unite et
l'exercice. On cherche alors, parmi tous les decoupages de chaque ligne, les
valeurs ou les soldes s'enchainent. Un poste n'est rendu que s'il est
contenu dans une identite verifiee, et que toutes les solutions trouvees
lui donnent la meme valeur.

POURQUOI PAS LES COLONNES

Le lecteur bancaire garde la meme colonne d'une ligne a l'autre. Ici, un
numero de note s'intercale (« XD EXCEDENT BRUT D'EXPLOITATION (XC+RK) 28
29 082 278 415 28 804 684 061 » chez CIE) et decale les colonnes. On ne
suppose donc rien de la colonne : ce sont les identites, exactes a
quelques unites pres, qui apparient les montants. Qu'un ensemble de
morceaux verifie par hasard trois soustractions a l'unite pres est
negligeable.

LE SIGNE

Chaque solde est essaye avec ses deux signes ; le resultat net porte celui
de la base (recoupe a la fiche societe). L'impot et la participation
s'ajoutent toujours en charge. Les lignes vides (pas de HAO, pas de
participation, pas de reprises) valent zero.
"""
from __future__ import annotations

import bisect
import itertools
import re

from analysis.lecture_bancaire import FACTEURS, MAX_COLONNES, NUMERO, _egal, decoupages, groupes
from analysis.lecture_syscohada import _plier

# L'ordre du tableau. Un libelle n'est rattache qu'a un poste situe APRES le
# dernier poste reconnu : « Reprises de provisions et depreciations
# financieres » (TL) ne se confond pas avec les reprises d'exploitation (TJ).
POSTES = (
    ("ebe", r"exc[e]dent\s+brut\s+d.?exploitation"),
    ("reprises", r"reprises?\s+d.?amortissements|reprises?\s+de\s+provisions"),
    ("dotations", r"dotations?\s+aux\s+amortissements"),
    ("rex", r"resultat\s+d.?exploitation"),
    ("rev_fin", r"revenus\s+financiers"),
    ("gains_change", r"gains\s+de\s+change"),
    ("rep_fin", r"reprises?\s+de\s+provisions\s+et\s+(de\s+)?depreciations\s+financ"),
    ("transf_fin", r"transferts?\s+de\s+charges\s+financ"),
    ("frais_fin", r"frais\s+financiers"),
    ("pertes_change", r"pertes\s+de\s+change"),
    ("dot_fin", r"dotations?\s+aux\s+provisions\s+et\s+(aux\s+)?depreciations\s+financ"),
    ("rfin", r"resultat\s+financier"),
    ("rao", r"resultat\s+(des\s+)?activites\s+ordinaires"),
    ("rhao", r"resultat\s+hors\s+activites\s+ordinaires"),
    ("participation", r"participation\s+des\s+travailleurs"),
    ("impot", r"impots?\s+\w+\s+(le|les)\s+(resultat|benefice)"),
    ("rn", r"resultat\s+net|benefice\s+net|perte\s+nette"),
)
MOTIFS = [(p, re.compile(m)) for p, m in POSTES]
ORDRE = {p: i for i, (p, _) in enumerate(POSTES)}
FENETRE = 45
# Les lignes qui, vides, valent zero.
FACULTATIFS = ("reprises", "rev_fin", "gains_change", "rep_fin", "transf_fin",
               "pertes_change", "dot_fin", "rhao", "participation")


def blocs(texte: str) -> list:
    """[{poste: groupes}] : un bloc s'ouvre sur l'EBE et se ferme sur le
    resultat net. Plusieurs postes peuvent partager une ligne (« Revenus
    financiers ... Frais financiers ... ») : chacun lit ce qui suit son
    libelle."""
    lignes = texte.split("\n")
    sortie = []
    for i, ligne in enumerate(lignes):
        if not MOTIFS[0][1].search(_plier(ligne)):
            continue
        bloc, dernier = {}, -1
        for brute in lignes[i:i + FENETRE]:
            brute = NUMERO.sub("", brute)
            plie = _plier(brute)
            trouves = []
            for poste, motif in MOTIFS:
                if ORDRE[poste] <= dernier:
                    continue
                m = motif.search(plie)
                if m:
                    trouves.append((m.start(), m.end(), poste))
            for debut, fin, poste in sorted(trouves):
                if ORDRE[poste] <= dernier:
                    continue
                # « Resultat net de l'exercice » figure aussi au passif, que
                # la mise en page place parfois a cote du compte de resultat
                # (SMB) : avant le resultat des activites ordinaires, ce
                # n'est pas le solde final.
                if poste == "rn" and "rao" not in bloc:
                    continue
                bloc[poste] = groupes(brute[fin:])
                dernier = ORDRE[poste]
            if "rn" in bloc:
                break
        if "rn" in bloc and "rao" in bloc:
            sortie.append(bloc)
    return sortie


def _valeurs(bloc: dict, poste: str, f: float) -> set:
    """Les montants possibles d'une ligne, en FCFA. Une ligne facultative
    absente ou vide vaut zero."""
    gs = bloc.get(poste)
    if not gs:
        return {0.0} if poste in FACULTATIFS else set()
    sortie = {abs(v) * f for k in range(1, MAX_COLONNES + 1)
              for d in decoupages(gs, k) for v in d}
    if poste in FACULTATIFS:
        sortie.add(0.0)
    return sortie


def _resoudre(bloc: dict, rn: float, f: float) -> list:
    """Toutes les lectures ou les soldes s'enchainent jusqu'au resultat net."""
    tol = 3 * f
    egal = lambda a, b: abs(a - b) <= max(tol, 1e-9 * abs(b))
    V = {p: _valeurs(bloc, p, f) for p, _ in POSTES}
    solutions = []
    # XI = XG + XH - participation - impot
    for rao, rhao, part, impot in itertools.product(
            V["rao"], V["rhao"], V["participation"], V["impot"] or {0.0}):
        for s_rao, s_hao in itertools.product((1, -1), (1, -1)):
            if not egal(s_rao * rao + s_hao * rhao - part - impot, rn):
                continue
            base = {"rn": rn, "rao": s_rao * rao, "pretax": rn + part + impot,
                    "_ids": ["rao+hao-impot=rn"]}
            # XG = XE + XF
            suites = []
            for rex, rfin in itertools.product(V["rex"], V["rfin"]):
                for s_rex, s_fin in itertools.product((1, -1), (1, -1)):
                    if egal(s_rex * rex + s_fin * rfin, base["rao"]):
                        suites.append((s_rex * rex, s_fin * rfin))
            if not suites:
                solutions.append(base)
                continue
            for rex, rfin in suites:
                lu = dict(base, rex=rex, rfin=rfin, _ids=base["_ids"] + ["rex+rfin=rao"])
                # XE = XD + TJ - RL
                for ebe, rep, dot in itertools.product(V["ebe"], V["reprises"], V["dotations"]):
                    for s_ebe in (1, -1):
                        if dot and egal(s_ebe * ebe + rep - dot, rex):
                            lu.setdefault("_ebe", set()).add(round(s_ebe * ebe / max(f, 1)))
                if len(lu.get("_ebe", ())) == 1:
                    lu["ebe"] = next(iter(lu["_ebe"])) * max(f, 1)
                    lu["_ids"].append("ebe+rep-dot=rex")
                lu.pop("_ebe", None)
                # XF = produits financiers - charges financieres
                # Produits d'un cote, charges de l'autre : on cherche les
                # charges dont l'ecart aux produits fait le resultat financier.
                frais = set()
                produits = sorted({sum(c) for c in itertools.product(
                    V["rev_fin"], V["gains_change"], V["rep_fin"], V["transf_fin"])})
                for fr, pertes, dotf in itertools.product(
                        V["frais_fin"], V["pertes_change"], V["dot_fin"]):
                    if not fr:
                        continue
                    cible = rfin + fr + pertes + dotf
                    i = bisect.bisect_left(produits, cible - tol)
                    if i < len(produits) and produits[i] <= cible + tol:
                        frais.add(round(fr / max(f, 1)))
                if len(frais) == 1:
                    lu["frais_fin"] = next(iter(frais)) * max(f, 1)
                    lu["_ids"].append("produits-charges=rfin")
                solutions.append(lu)
    return solutions


def _accord(solutions: list) -> dict:
    """Les postes sur lesquels toutes les solutions s'accordent.

    Seules comptent les solutions qui enchainent le PLUS d'identites : un
    decoupage qui n'atteint que le resultat des activites ordinaires ne
    contredit pas celui qui remonte jusqu'a l'EBE, il en dit moins.
    """
    plus = max(len(s["_ids"]) for s in solutions)
    solutions = [s for s in solutions if len(s["_ids"]) == plus]
    sortie = {}
    postes = {p for s in solutions for p in s if not p.startswith("_")}
    for p in postes:
        vals = [s.get(p) for s in solutions]
        if any(v is None for v in vals):
            continue
        if all(_egal(v, vals[0]) for v in vals):
            sortie[p] = vals[0]
    return sortie


def lire_exercice(texte: str, rn: float) -> list:
    """Les lectures de l'exercice dont le resultat net vaut `rn` (signe, en
    FCFA) : une par bloc, restreinte aux postes sur lesquels toutes les
    solutions s'accordent."""
    sortie = []
    for bloc in blocs(texte):
        if not bloc.get("rn"):
            continue
        facteurs = {f for k in range(1, MAX_COLONNES + 1)
                    for d in decoupages(bloc["rn"], k) for v in d
                    for f in FACTEURS if abs(v) >= 100 and _egal(abs(v) * f, abs(rn))}
        solutions = []
        for f in sorted(facteurs):
            solutions += _resoudre(bloc, rn, f)
        if not solutions:
            continue
        # Une lecture par unite : la plus fine qui s'enchaine l'emporte, les
        # autres n'etant que les memes montants arrondis.
        lu = _accord(solutions)
        if len(lu) > 2:
            sortie.append(lu)
    return sortie
