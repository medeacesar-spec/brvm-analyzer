"""Lire les etats financiers SYSCOHADA dans un texte, y compris apres OCR.

POURQUOI CETTE COUCHE EXISTE

Les etats financiers de la BRVM sont, pour la plupart, des IMAGES : sur un
echantillon de vingt-deux documents telecharges, deux seulement portaient du
texte. La reconnaissance de caracteres les rend lisibles — elle a suffi pour
lire AGL, SICOR ou BOA Niger a la main — mais le texte qu'elle produit est
abime : « RESULIAI DELEXERCICE », « Chiffred'@ffaires », des chiffres colles
ou coupes.

`data/pdf_extractor.py` cherche des tableaux et des libellés propres. Ce
module part de l'hypothese inverse : le texte est sale, les libelles sont
approximatifs, et seule la STRUCTURE tient — un libelle, puis deux colonnes
de montants, l'exercice et son comparatif.

CE QU'IL NE FAIT PAS

Il ne remplace pas le lecteur existant : il le complete la ou celui-ci ne
rend rien. Et il ne decide jamais seul — chaque valeur qu'il propose passe
par les controles de `scripts/completer_fondamentaux.py`.
"""
from __future__ import annotations

import re
import unicodedata

# Un libelle par champ. Les motifs sont volontairement laches : l'OCR mange
# des lettres, colle des mots, confond « l » et « I ». On cible les suites de
# caracteres qui survivent le mieux.
LIBELLES = {
    "revenue": (r"chiffre\s*d.?\s*affaires?(?!\s*hao)",
                r"produit\s*net\s*bancaire", r"\bPNB\b"),
    "net_income": (r"r[ée]sultat\s*net\s*(de\s*l.?\s*exercice)?",
                   r"r[ée]sultat\s*de\s*l.?\s*exercice",
                   r"b[ée]n[ée]fice\s*net"),
    "equity": (r"total\s*capitaux\s*propres",
               r"capitaux\s*propres\s*et\s*ressources",
               r"total\s*des\s*capitaux\s*propres"),
    "total_assets": (r"total\s*(g[ée]n[ée]ral|actif|bilan)",
                     r"total\s*de\s*l.?\s*actif"),
    "total_debt": (r"emprunts?\s*et\s*dettes\s*financi",
                   r"total\s*dettes\s*financi", r"dettes\s*financi[èe]res"),
    "ebit": (r"r[ée]sultat\s*d.?\s*exploitation",),
    "ebitda": (r"exc[ée]dent\s*brut\s*d.?\s*exploitation", r"\bEBE\b"),
    "interest_expense": (r"frais\s*financiers", r"charges\s*d.?\s*int[ée]r[êe]ts"),
    "cfo": (r"flux\s*de\s*tr[ée]sorerie\s*provenant\s*des?\s*activit[ée]s\s*op",
            r"tr[ée]sorerie\s*nette\s*d.?\s*exploitation"),
    "capex": (r"d[ée]caissements?\s*li[ée]s?\s*aux\s*acquisitions?\s*d.?\s*immobilisations?\s*corporelles",),
    "dividends_total": (r"dividendes\s*vers[ée]s",),
    "deposits": (r"d[ée]p[ôo]ts\s*(et\s*comptes\s*)?(de\s*la\s*)?client[èe]le",),
    "cost_of_risk": (r"co[ûu]t\s*du\s*risque", r"provisions?\s*sur\s*cr[ée]ances"),
}

# Champs dont un montant negatif a un sens dans un tableau de flux.
SIGNE_LIBRE = {"net_income", "ebit", "ebitda", "cfo", "capex", "dividends_total"}

# Un montant : des groupes de chiffres separes par des espaces ou des points.
MONTANT = re.compile(r"\(?-?\d[\d  .']{2,}\d\)?")

# Un groupe de trois chiffres, l'unite de ces tableaux.
TRIPLET = re.compile(r"\d{3}")


def _plier(texte: str) -> str:
    """Minuscules, accents retires, espaces normalises."""
    texte = unicodedata.normalize("NFD", texte)
    texte = "".join(c for c in texte if unicodedata.category(c) != "Mn")
    return re.sub(r"[ \t\xa0]+", " ", texte.lower())


def _nombre(brut: str) -> float | None:
    negatif = brut.strip().startswith("(") or brut.strip().startswith("-")
    chiffres = re.sub(r"[^\d]", "", brut)
    if len(chiffres) < 4:
        return None
    valeur = float(chiffres)
    return -valeur if negatif else valeur


def _scinder_colonnes(brut: str) -> list:
    """Separe deux colonnes collees par l'extraction.

    C'est LE piege de ces tableaux. « CHIFFRE D'AFFAIRES 197 629 996
    172 182 502 » porte deux montants — l'exercice et son comparatif — que
    rien ne separe visiblement : le meme espace sert de separateur de
    milliers et de separateur de colonnes. Lu naivement, Palm CI affiche un
    chiffre d'affaires de 197 629 996 172 182 502 francs.

    La structure tranche : chaque montant s'ecrit en groupes de trois
    chiffres apres son premier groupe. Un nombre pair de groupes qui se
    coupe en deux moities de meme longueur est donc deux montants, pas un.
    """
    morceaux = brut.strip().strip("()").replace(".", " ").split()
    if len(morceaux) < 2:
        return [brut]
    # Tous les groupes sauf le premier font trois chiffres : un seul montant
    # ne peut pas avoir deux « premiers groupes ».
    triplets = [m for m in morceaux if len(m) == 3]
    if len(triplets) == len(morceaux) and len(morceaux) % 2 == 0:
        milieu = len(morceaux) // 2
        return [" ".join(morceaux[:milieu]), " ".join(morceaux[milieu:])]
    for coupe in range(1, len(morceaux)):
        gauche, droite = morceaux[:coupe], morceaux[coupe:]
        if (len(gauche) == len(droite)
                and all(len(m) == 3 for m in gauche[1:])
                and all(len(m) == 3 for m in droite[1:])
                and len(gauche[0]) <= 3 and len(droite[0]) <= 3):
            return [" ".join(gauche), " ".join(droite)]
    return [brut]


def montants_de_ligne(ligne: str) -> list:
    """Les montants d'une ligne, dans l'ordre. Le premier est l'exercice."""
    sortie = []
    for m in MONTANT.finditer(ligne):
        negatif = m.group(0).strip().startswith(("(", "-"))
        for morceau in _scinder_colonnes(m.group(0)):
            v = _nombre(morceau)
            if v is not None:
                sortie.append(-abs(v) if negatif else v)
    return sortie


def _echelle(texte: str) -> float:
    """Le multiplicateur annonce par le document (milliers, millions)."""
    plie = _plier(texte[:4000])
    if re.search(r"en milliards", plie):
        return 1_000_000_000.0
    if re.search(r"en millions", plie):
        return 1_000_000.0
    if re.search(r"en milliers", plie):
        return 1_000.0
    return 1.0


ANNEES = re.compile(r"(?:^|[^\d])((?:19|20)\d{2})(?:[^\d]|$)")


def colonne_de_l_exercice(texte: str) -> int:
    """0 si l'exercice courant est la premiere colonne, 1 s'il est la seconde.

    L'ordre n'est pas une convention : NSIA Banque ecrit « Produit Net
    Bancaire 97 819 112 928 », le comparatif d'abord ; Palm CI ecrit
    l'inverse. Prendre le premier montant sans regarder l'en-tete donne donc
    l'exercice precedent une fois sur deux — et personne ne s'en apercoit,
    puisque le chiffre est plausible.

    On cherche la premiere ligne d'en-tete qui porte deux millesimes et on
    lit leur ordre.
    """
    for ligne in texte.split("\n")[:120]:
        annees = [int(a) for a in ANNEES.findall(ligne)]
        annees = [a for a in annees if 2000 <= a <= 2100]
        if len(annees) < 2 or annees[0] == annees[1] or abs(annees[0] - annees[1]) > 3:
            continue
        # Une ligne d'EN-TETE, pas une phrase : deux millesimes voisins et
        # peu de mots autour. « Fait a Abidjan, le 13 mai 2026 … 2025 » n'en
        # est pas une, et c'est elle qui trompait la detection.
        mots = [m for m in re.split(r"[^\wÀ-ÿ]+", ligne) if m and not m.isdigit()]
        if len(mots) > 6:
            continue
        return 0 if annees[0] > annees[1] else 1
    return 0


def lire(texte: str, echelle: float = None) -> dict:
    """{champ: montant} — ce que le texte livre, sans jugement de valeur.

    Pour chaque champ, la PREMIERE ligne qui porte l'un de ses libelles et au
    moins un montant. La colonne retenue est celle de l'exercice courant,
    determinee par l'en-tete — elle n'est pas toujours la premiere.
    """
    facteur = echelle if echelle else _echelle(texte)
    colonne = colonne_de_l_exercice(texte)
    lignes = [l for l in _plier(texte).split("\n") if l.strip()]
    sortie = {}
    for champ, motifs in LIBELLES.items():
        for ligne in lignes:
            if not any(re.search(m, ligne) for m in motifs):
                continue
            valeurs = montants_de_ligne(ligne)
            if not valeurs:
                continue
            valeur = valeurs[min(colonne, len(valeurs) - 1)] * facteur
            if valeur < 0 and champ not in SIGNE_LIBRE:
                valeur = abs(valeur)
            sortie[champ] = valeur
            break
    return sortie
