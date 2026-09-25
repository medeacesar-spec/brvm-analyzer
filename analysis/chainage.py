"""Lire un poste par le chainage des comparatifs, pour les etats SYSCOHADA.

LE PRINCIPE

Un montant de l'exercice N figure dans DEUX documents : celui de N (sa
colonne) et celui de N+1 (le comparatif). Le montant commun aux deux lignes
du meme poste est donc celui de N — sans ancre, sans supposer l'ordre des
colonnes, sans lire l'en-tete. La methode a d'abord servi aux encours des
banques (`lecture_bancaire.encours`) ; elle vaut pour tout poste publie
chaque annee.

TROIS PIEGES, TROIS REGLES

  Les morceaux. Le decoupage exhaustif d'une ligne (« 1 872 917 » donne
  aussi « 1 872 » et « 872 917 ») fabrique des communs parasites : un
  commun dont les chiffres commencent ou finissent un autre commun est
  ecarte (`lecture_bancaire.chainer`).

  L'historique. Certains rapports publient trois exercices (SAPH : 2023,
  2022 et 2021). Les documents N et N+1 partagent alors aussi N-1. Un
  commun present dans le document N-1 est l'exercice N-1 : il est ecarte.
  S'il reste plus d'un candidat, rien n'est lu.

  L'unite. Elle est fixee PAR DOCUMENT, par le chiffre d'affaires : le
  facteur (1, mille, un million) qui fait retomber un montant de la ligne
  « Chiffre d'affaires » sur le chiffre d'affaires connu de la base, a un
  pour mille pres. Un document sans ce point d'appui n'est pas lu.

LE SIGNE

L'OCR perd les signes, il n'en invente pas. Un resultat d'exploitation
marque negatif (tiret ou parentheses) dans l'un des deux documents est une
perte. Les investissements et les frais financiers s'ecrivent en positif,
comme dans la base.
"""
from __future__ import annotations

import re

from analysis.lecture_bancaire import _egal, chainer, groupes
from analysis.lecture_syscohada import _plier

POSTES = {
    "ebitda": re.compile(r"exc[eé]dent\s+brut\s+d.?exploitation"),
    "ebit": re.compile(r"r[eé]sultat\s+d.?exploitation"),
    "interest_expense": re.compile(r"frais\s+financiers\s+et\s+charges\s+assimil"),
    "capex_corporelles": re.compile(
        r"acquisitions?\s+d.?immobilisations\s+corporelles"),
    "capex_incorporelles": re.compile(
        r"acquisitions?\s+d.?immobilisations\s+incorporelles"),
    # Les soldes de bilan (25/09/2026). Un total de fin N figure dans le
    # bilan N et dans le comparatif du bilan N+1, quelle que soit la mise en
    # page : c'est ce qui permet de lire les etats resumes (SITAB, Nestle),
    # qui n'ont pas les sous-totaux du bilan SYSCOHADA complet.
    "total_assets": re.compile(
        r"total\s+(general|de\s+l.?actif|du\s+bilan|bilan|actif|passif)"
        r"(?!\s*(immobilise|circulant|non\s+courant|courant|et\s+capitaux))"),
    "equity": re.compile(
        r"(total\s+(des\s+)?)?capitaux\s+propres(\s+et\s+ressources\s+assimilees)?"
        r"(?!\s*(part|attribuable|et\s+passif|consolid|de\s+l|au\s+|du\s+|des\s+))"),
    "cfo": re.compile(
        r"flux\s+(net\s+)?de\s+tresorerie\s+(net\s+)?(provenant|lie|genere)s?\s+(des|aux|par\s+les)"
        r"\s+activites\s+(operationnelles|d.?exploitation)"
        r"|tresorerie\s+(nette\s+)?generee\s+par\s+les\s+activites\s+d.?exploitation"),
}
# Une ligne qui porte le libelle mais pas le poste : les FLUX de capitaux
# propres du tableau de tresorerie, le tableau de VARIATION des capitaux
# propres, le passif « total general » d'un bilan fonctionnel...
EXCLUSIONS = {
    # « Total du passif et des capitaux propres » est le TOTAL du bilan
    # (Sonatel 2023 : 2 573,9 Md lus comme capitaux propres).
    "equity": re.compile(r"(provenant|variation|flux|rentabilite|retour|ratio|passif|autres)"),
    "total_assets": re.compile(r"(variation|flux|tresorerie)"),
}
# Le point d'appui de l'unite : le chiffre d'affaires, ou le produit net
# bancaire, qui en tient lieu dans la base pour les banques.
CHIFFRE_AFFAIRES = re.compile(r"chiffre\s+d.?affaires|produit\s+net\s+bancaire")
FACTEURS = (1.0, 1e3, 1e6)
SIGNES = ("ebitda", "ebit", "equity", "cfo")


def candidats(texte: str, motif) -> dict:
    """{valeur absolue en unites du document: negative ?} sur les lignes
    du poste.

    PAS DE DECOUPAGE EXHAUSTIF ICI. Il convient au bloc bancaire, ou
    l'arithmetique trie les decoupages ; seul, le chainage ne trie rien, et
    les morceaux se chainent au hasard : « -397 -1 251 » donnait un « 251 »
    qui retombait sur un autre montant, et les investissements CIE 2023
    sortaient a 7,451 Md au lieu de 8,451. On prend les decoupages du
    lecteur general (le principal et ses deux secours), et le chainage
    confirme.
    """
    from analysis.lecture_syscohada import (montants_alternatifs, montants_de_ligne,
                                            montants_tetes_fusionnees)
    sortie = {}
    exclusion = next((e for p, e in EXCLUSIONS.items() if POSTES.get(p) is motif), None)
    for ligne in texte.split("\n"):
        plie = _plier(ligne)
        if exclusion is not None and exclusion.search(plie):
            continue
        for m in motif.finditer(plie):
            # Deux tableaux cote a cote : on lit ce qui suit CE libelle,
            # jusqu'au premier mot.
            gs = groupes(ligne[m.end():])
            if not gs:
                continue
            reste = " ".join(sg + ch for sg, ch in gs)
            for lecture in (montants_de_ligne, montants_alternatifs,
                            montants_tetes_fusionnees):
                for v in lecture(reste):
                    if abs(v) >= 100:
                        sortie[abs(v)] = sortie.get(abs(v), False) or v < 0
    return sortie


def facteur(texte: str, revenus: list):
    """Le facteur qui fait retomber la ligne « Chiffre d'affaires » sur un
    chiffre d'affaires connu (celui de N ou de N-1), ou None."""
    vus = candidats(texte, CHIFFRE_AFFAIRES)
    trouves = {f for f in FACTEURS for v in vus for r in revenus
               if r and _egal(v * f, r)}
    if len(trouves) == 1:
        return trouves.pop()
    # « 139 353 102 670 » (le chiffre d'affaires en millions, suivi du
    # comparatif) retombe en francs sur 139 353 M arrondis de la base, comme
    # « 139 353 » en millions. L'unite ANNONCEE par le document departage.
    from analysis.lecture_syscohada import _echelle
    annoncee = _echelle(texte)
    return annoncee if annoncee in trouves else None


def lire(docs: dict) -> dict:
    """{(exercice, poste): valeur en FCFA} a partir de
    docs = {exercice: {poste: {valeur FCFA: negative ?}}}."""
    sortie = {}
    for an, postes in docs.items():
        suivant = docs.get(an + 1)
        if not suivant:
            continue
        precedent = docs.get(an - 1, {})
        for poste, valeurs in postes.items():
            autres = suivant.get(poste, {})
            communs = chainer(set(valeurs), set(autres))
            anciens = set(precedent.get(poste, {}))
            communs = [c for c in communs if not any(_egal(c, x) for x in anciens)]
            if len(communs) != 1:
                continue
            v = communs[0]
            negatif = valeurs.get(v, False) or any(
                n for x, n in autres.items() if _egal(x, v))
            sortie[(an, poste)] = -v if (negatif and poste in SIGNES) else v
    return sortie
