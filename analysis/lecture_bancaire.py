"""Lecture du compte de resultat d'une banque (plan comptable bancaire UEMOA).

POURQUOI UN LECTEUR A PART

Le lecteur general (`lecture_syscohada.py`) lit un poste a la fois. Pour les
banques, il ne verifiait l'identite « PNB - charges - dotations = RBE » que
sur 4 exercices sur 41 : les libelles des dotations varient, et une lecture
isolee du resultat brut d'exploitation tombait souvent dans la mauvaise
colonne. Le compte de resultat bancaire a pourtant une particularite
precieuse : ses soldes se suivent dans un ordre fixe et s'enchainent par
soustraction.

    PRODUIT NET BANCAIRE
  - CHARGES GENERALES D'EXPLOITATION
  - DOTATIONS AUX AMORTISSEMENTS ... DES IMMOBILISATIONS
  = RESULTAT BRUT D'EXPLOITATION
  +/- COUT DU RISQUE
  = RESULTAT D'EXPLOITATION
  ...
  = RESULTAT NET

On lit donc le BLOC entier, et une lecture n'est retenue que si ses soldes
s'enchainent. Une erreur d'OCR (« 710 189 » pour « 110 189 » chez Ecobank)
ou un decalage de colonne casse l'enchainement et ecarte la lecture, au lieu
de l'ecrire.

LE DECOUPAGE DES COLONNES PAR L'ARITHMETIQUE

« 3 667 878 530 705 521 710 » (BOA Benin, cout du risque) porte deux
montants qu'aucun espace ne distingue du separateur de milliers : 3 667 878 530
et 705 521 710, ou 3 667 878 et 530 705 521 710, ou... Le lecteur general
tranche par des regles, et se trompait ici. On enumere au contraire TOUS les
decoupages possibles de chaque ligne, et l'on garde celui ou les soldes
s'enchainent — s'il est unique. Deux decoupages qui s'enchainent tous deux
ne donnent rien : on ne choisit pas.

L'UNITE ET LA COLONNE PAR L'ANCRE

L'unite annoncee est peu fiable (millions en en-tete, milliards dans le
commentaire). Le PNB de l'exercice est connu : c'est le chiffre d'affaires
de la base, recoupe a la fiche societe. La colonne ET l'unite sont celles
ou ce PNB retombe, a un pour mille pres.

DEUX PRESENTATIONS

  les etats financiers : charges generales et dotations sur deux lignes ;
  le rapport d'activite : « Frais generaux » ou « Frais de gestion » sur une
  seule ligne, dotations comprises.

Dans les deux cas, `charges` rendu ici inclut les dotations : c'est la
convention de la base, ou PNB - charges = RBE (voir `coherence_interne.py`).
"""
from __future__ import annotations

import re

from analysis.lecture_syscohada import _plier

POSTES = (
    ("pnb", re.compile(r"produit\s+net\s+bancaire")),
    ("charges_generales", re.compile(
        r"charges\s+generales\s+d.?exploitation|frais\s+generaux|frais\s+de\s+gestion")),
    ("dotations", re.compile(r"dotations?\s+aux\s+amortissements")),
    ("rbe", re.compile(r"resultat\s+brut\s+d.?exploitation")),
    ("cdr", re.compile(r"cout\s+(net\s+)?du\s+risque")),
    ("rex", re.compile(r"resultat\s+d.?exploitation")),
    ("rn", re.compile(r"resultat\s+net")),
)
# Un numero de ligne en tete : « 13 DOTATION AUX AMORTISSEMENTS ... ».
NUMERO = re.compile(r"^\s*\d{1,2}\s+(?=[^\d\s])")
# Un pourcentage ou un decimal (« +11,0% », « 83,5 ») : jamais un montant
# de ces tableaux, qui sont en unites entieres.
DECIMAL = re.compile(r"[~+\-]?\d+[,.]\d+\s*%?|[~+\-]?\d+\s*%")
GROUPE = re.compile(r"^([(\-−]?)(\d+)\)?$")
FACTEURS = (1.0, 1e3, 1e6, 1e9)
FENETRE = 25
MAX_COLONNES = 5


def _egal(a: float, b: float, unite: float = 1.0) -> bool:
    """A un pour mille pres, ou a trois unites du tableau (les arrondis)."""
    return abs(abs(a) - abs(b)) <= max(0.001 * abs(b), 3 * unite)


def groupes(ligne: str) -> list:
    """Les groupes de chiffres qui suivent le libelle : [(signe, chiffres)].

    On s'arrete au premier mot apres les montants : les rapports d'activite
    font suivre le tableau d'une colonne de commentaire sur la meme ligne.
    """
    ligne = DECIMAL.sub(" ", ligne)
    sortie = []
    for mot in ligne.split():
        m = GROUPE.match(mot)
        if m:
            sortie.append((m.group(1), m.group(2)))
        elif sortie and any(c.isalpha() for c in mot):
            break
    return sortie


def decoupages(gs: list, k: int) -> list:
    """Tous les decoupages des groupes en exactement `k` montants.

    Un montant commence par un groupe d'un a trois chiffres — ou plus, quand
    l'OCR a colle deux groupes — et se poursuit par des groupes d'exactement
    trois chiffres. Un signe ou une parenthese ouvre toujours un montant.
    """
    n = len(gs)
    sortie = []

    def suite(i, reste, acc):
        if reste == 0:
            if i == n:
                sortie.append(tuple(acc))
            return
        if i == n:
            return
        signe, tete = gs[i]
        chiffres = tete
        j = i + 1
        while True:
            v = float(chiffres)
            suite(j, reste - 1, acc + [-v if signe else v])
            if j == n or gs[j][0] or len(gs[j][1]) != 3:
                break
            chiffres += gs[j][1]
            j += 1

    suite(0, k, [])
    return sortie


def blocs(texte: str) -> list:
    """Les blocs du compte de resultat : [{poste: [groupes]}]. Un bloc
    s'ouvre sur le PNB et se ferme sur le resultat net."""
    lignes = texte.split("\n")
    sortie = []
    for i, ligne in enumerate(lignes):
        plie = _plier(ligne)
        m = POSTES[0][1].search(plie)
        if not m or not groupes(ligne[m.end():]):
            continue
        bloc, en_attente = {}, ""
        for brute in lignes[i:i + FENETRE]:
            # Un libelle coupe sur deux lignes : la premiere n'a pas de montant.
            brute = (en_attente + " " + NUMERO.sub("", brute)).strip()
            plie = _plier(brute)
            for poste, motif in POSTES:
                m = motif.search(plie)
                if m and poste not in bloc:
                    gs = groupes(brute[m.end():])
                    if gs:
                        bloc[poste] = gs
                        en_attente = ""
                    else:
                        en_attente = brute if len(brute) < 200 else ""
                    break
            else:
                en_attente = brute if (not groupes(brute) and len(brute) < 200) else ""
            if "rn" in bloc:
                break
        if "rbe" in bloc and ("charges_generales" in bloc or "cdr" in bloc):
            sortie.append(bloc)
    return sortie


def _candidats(bloc: dict, poste: str, k: int, j: int, f: float) -> set:
    if poste not in bloc:
        return set()
    return {d[j] * f for d in decoupages(bloc[poste], k)}


def _resoudre(bloc: dict, pnb: float, k: int, j: int, f: float) -> list:
    """Les lectures de la colonne j ou les soldes s'enchainent."""
    unite = f
    c = {p: _candidats(bloc, p, k, j, f) for p, _ in POSTES}
    solutions = []
    rbes = c["rbe"] or {None}
    for rbe in rbes:
        lu = {"pnb": pnb}
        ids = {}
        if rbe is not None and c["charges_generales"]:
            combos = [(ch, dot) for ch in c["charges_generales"]
                      for dot in (c["dotations"] or {0.0})
                      if _egal(abs(pnb) - abs(ch) - abs(dot), rbe, unite)]
            if len({(round(ch), round(dot)) for ch, dot in combos}) != 1:
                continue
            ch, dot = combos[0]
            lu["charges"] = -(abs(ch) + abs(dot))
            ids["pnb-charges=rbe"] = True
        if rbe is not None and c["cdr"] and c["rex"]:
            combos = [(cdr, rex) for cdr in c["cdr"] for rex in c["rex"]
                      for s in (1, -1) if _egal(abs(rbe) + s * abs(cdr), rex, unite)]
            if len({(round(abs(x)), round(abs(y))) for x, y in combos}) == 1:
                cdr, rex = combos[0]
                lu["rex"] = abs(rex)
                # Le signe par l'enchainement, pas par l'OCR qui le perd :
                # une charge si le resultat d'exploitation est sous le RBE.
                lu["cdr"] = abs(rex) - abs(rbe)
                ids["rbe+cdr=rex"] = True
        if not ids:
            continue
        lu["rbe"] = rbe
        if len(c["rn"]) == 1:
            lu["rn"] = next(iter(c["rn"]))
        lu["_identites"] = ids
        lu["_unite"] = f
        solutions.append(lu)
    return solutions


def _meme_lecture(a: dict, b: dict) -> bool:
    """Deux lectures qui s'accordent sur tous leurs postes communs. L'une
    peut en porter moins : un decoupage en cinq colonnes perd le cout du
    risque que le decoupage en deux retrouve."""
    cles = lambda x: {p for p in x if not p.startswith("_") and p != "rn"}
    return all(_egal(a[p], b[p]) for p in cles(a) & cles(b))


def lire_exercice(texte: str, pnb: float, rn: float = None) -> list:
    """Les lectures de l'exercice dont le PNB vaut `pnb` (en FCFA).

    Rend [{poste: valeur en FCFA, "_identites": {...}, "_rn": bool}], au
    plus une par bloc, et seulement si elle est unique. `_rn` dit si le
    resultat net lu retombe aussi sur celui de la base.
    """
    sortie = []
    for bloc in blocs(texte):
        trouvees = []
        for k in range(1, MAX_COLONNES + 1):
            for d in decoupages(bloc["pnb"], k):
                for j, v in enumerate(d):
                    # Quatre chiffres significatifs au moins : « 263 » milliards
                    # retombe a un pour mille sur 263 207 millions, et ouvre une
                    # lecture grossiere dont les arrondis passent les identites.
                    if abs(v) < 1000:
                        continue
                    for f in FACTEURS:
                        if _egal(v * f, pnb):
                            trouvees += _resoudre(bloc, v * f, k, j, f)
        # Le meme montant decoupe de deux facons (« 47 832 439 739 » francs
        # ou « 47 832 439 » milliers) : une seule lecture, la plus fine.
        distinctes = []
        # La plus complete d'abord, puis la plus fine.
        for t in sorted(trouvees, key=lambda t: (-len(t), t["_unite"])):
            if not any(_meme_lecture(t, u) for u in distinctes):
                distinctes.append(t)
        if len(distinctes) != 1:
            continue
        lu = distinctes[0]
        lu["_rn"] = rn is not None and "rn" in lu and _egal(lu["rn"], rn)
        sortie.append(lu)
    return sortie
