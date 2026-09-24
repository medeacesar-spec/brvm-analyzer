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

# Libelles SYSCOHADA du bilan et du tableau des flux, releves sur les
# documents de la cote. Le plan comptable impose les intitules — « TOTAL
# CAPITAUX PROPRES ET RESSOURCES ASSIMILEES », « Frais financiers », « FLUX DE
# TRESORERIE PROVENANT DES ACTIVITES OPERATIONNELLES » — et ils reviennent
# d'un emetteur a l'autre, a l'OCR pres.
LIBELLES["equity"] += (r"total\s*capitaux\s*propres\s*et\s*ressources",
                       r"capitaux\s*propres\s*et\s*ressources\s*assimilees",
                       r"situation\s*nette")
LIBELLES["total_assets"] += (r"total\s*(de\s*l.?\s*)?actif", r"total\s*passif",
                             r"total\s*du\s*bilan")
LIBELLES["total_debt"] += (r"total\s*dettes\s*financieres\s*et\s*ressources",
                           r"dettes\s*a\s*long\s*terme")
LIBELLES["cfo"] += (r"flux\s*de\s*tresorerie\s*provenant\s*des\s*activites\s*operationnelles",
                    r"tresorerie\s*nette\s*provenant\s*de\s*l.?\s*exploitation")
LIBELLES["capex"] += (r"decaissements?\s*li[ée]s?\s*aux\s*acquisitions",)
LIBELLES["dividends_total"] += (r"dividendes\s*verses?",)
LIBELLES["interest_expense"] += (r"interets?\s*et\s*charges\s*assimilees",
                                 r"charges\s*financieres")
LIBELLES["deposits"] += (r"dettes\s*envers\s*la\s*clientele",)

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
    """Separe les colonnes collees par l'extraction.

    C'EST LE PIEGE PRINCIPAL DE CES TABLEAUX

    « Chiffre d'Affaires 604 978 411 174 600 707 830 161 4 270 581 013 » porte
    TROIS montants — l'exercice, le comparatif, l'ecart — que rien ne separe
    visiblement : le meme espace sert de separateur de milliers et de
    separateur de colonnes. Lu naivement, Vivo Energy affiche un chiffre
    d'affaires de 6 x 10^25 francs.

    Deux regles, dans cet ordre :

    1. APRES LE PREMIER GROUPE, un groupe de moins de trois chiffres commence
       un nouveau montant. « 1 234 567 » est un seul nombre ; « … 161 4 270 »
       en contient deux, car « 4 » ne peut etre qu'un groupe de tete.
    2. Ce qui reste et compte un nombre PAIR de groupes, dont chaque moitie
       est un montant bien forme, se coupe en deux : l'exercice et son
       comparatif. « 97 819 112 928 » donne ainsi 97 819 et 112 928 — le
       groupe de tete d'un montant peut compter un, deux ou trois chiffres.
       Quatre groupes au minimum : « 112 928 » reste un seul montant.
    """
    morceaux = brut.strip().strip("()").replace(".", " ").split()
    if len(morceaux) < 2:
        return [brut]

    segments, courant = [], [morceaux[0]]
    for groupe in morceaux[1:]:
        if len(groupe) < 3:
            segments.append(courant)
            courant = [groupe]
        else:
            courant.append(groupe)
    segments.append(courant)

    sortie = []
    for segment in segments:
        # QUATRE GROUPES AU MINIMUM. « 112 928 » est un seul montant — le
        # produit net bancaire de NSIA en millions — et le couper en deux
        # donnerait 112 et 928. A partir de quatre groupes, deux moities de
        # deux groupes chacune font deux nombres d'au moins quatre chiffres :
        # la lecture redevient sure.
        moitie = len(segment) // 2
        if (len(segment) >= 4 and len(segment) % 2 == 0
                and all(len(g) == 3 for g in segment)):
            sortie.append(" ".join(segment[:moitie]))
            sortie.append(" ".join(segment[moitie:]))
        else:
            sortie.append(" ".join(segment))
    return sortie


def _scinder_en_deux(brut: str) -> list:
    """Coupe un montant en deux moities, meme si son groupe de tete est court.

    Lecture de SECOURS, appelee seulement quand l'ancre ne reconnait rien
    dans la lecture normale. « 97 819 112 928 » est deux montants chez NSIA
    — 97 819 et 112 928 — mais « 75 047 177 792 » en est UN seul chez SITAB.
    Rien dans la suite de chiffres ne les distingue : seule la valeur connue
    de l'exercice precedent tranche, et c'est elle qui decide d'appeler cette
    lecture ou de s'en tenir a la premiere.
    """
    morceaux = brut.strip().strip("()").replace(".", " ").split()
    if len(morceaux) < 4 or len(morceaux) % 2:
        return []
    moitie = len(morceaux) // 2
    if not all(len(g) == 3 for g in morceaux[1:moitie]):
        return []
    if not all(len(g) == 3 for g in morceaux[moitie + 1:]):
        return []
    return [" ".join(morceaux[:moitie]), " ".join(morceaux[moitie:])]


def montants_alternatifs(ligne: str) -> list:
    """Les montants de la ligne, lus avec le decoupage de secours."""
    sortie = []
    for m in MONTANT.finditer(ligne):
        negatif = m.group(0).strip().startswith(("(", "-"))
        for morceau in _scinder_en_deux(m.group(0)) or [m.group(0)]:
            v = _nombre(morceau)
            if v is not None:
                sortie.append(-abs(v) if negatif else v)
    return sortie


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
    """Le multiplicateur annonce par le document.

    L'UNITE EST CELLE DE L'EN-TETE, PAS CELLE DU COMMENTAIRE

    Le document de NSIA Banque porte « (en millions FCFA) » au-dessus de son
    tableau, et « 40,7 milliards » dans le commentaire de la page suivante.
    Chercher le plus grand multiple trouve « milliards » et multiplie tout
    par mille : le produit net bancaire passait de 112,9 Md a 112 928 Md.

    On cherche donc d'abord la mention PARENTHESEE, qui est la facon dont ces
    etats financiers annoncent leur unite — « (en milliers de FCFA) », « (en
    millions FCFA) ». A defaut, la premiere mention rencontree, l'en-tete
    venant avant la prose.
    """
    plie = _plier(texte[:6000])
    unites = {"milliard": 1_000_000_000.0, "million": 1_000_000.0,
              "millier": 1_000.0}

    entete = re.search(r"\(\s*en\s+(milliard|million|millier)", plie)
    if entete:
        return unites[entete.group(1)]

    premiere, rang = 1.0, len(plie) + 1
    for mot, facteur in unites.items():
        m = re.search(r"en\s+" + mot, plie)
        if m and m.start() < rang:
            premiere, rang = facteur, m.start()
    return premiere


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


def _colonne_par_ancre(valeurs: list, ancre, facteur: float):
    """L'indice de la colonne de l'exercice, deduit du comparatif connu.

    C'EST LA CLEF DU PROBLEME DES COLONNES

    Un tableau porte deux montants par ligne : l'exercice et son comparatif.
    L'ordre change d'un emetteur a l'autre — NSIA ecrit le comparatif
    d'abord, Palm CI l'exercice — et l'en-tete est souvent illisible apres
    extraction. Deviner revient a se tromper une fois sur deux.

    Mais nous CONNAISSONS l'exercice precedent : chiffre d'affaires et
    resultat net couvrent 98 % des exercices 2021-2025. La colonne qui
    retombe sur ce que nous savons deja est donc le comparatif, et l'autre
    est celle que l'on cherche. Aucune devinette.
    """
    if ancre is None or len(valeurs) < 2:
        return None
    for i, valeur in enumerate(valeurs):
        montant = valeur * facteur
        if not ancre or abs(abs(montant) - abs(ancre)) > 0.01 * abs(ancre):
            continue
        # Le voisin de la MEME PAIRE, pas le suivant dans la ligne. Chez Palm
        # CI, « Resultat net 15 508 655 15 861 643 Services exterieurs
        # -23 133 174 » porte quatre montants : l'ancre tombe sur le second,
        # et prendre « le suivant » ramenait les services exterieurs.
        return i - 1 if i > 0 else i + 1
    return None


def lire(texte: str, echelle: float = None, ancres: dict = None) -> dict:
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
            trouve = next((re.search(m, ligne) for m in motifs
                           if re.search(m, ligne)), None)
            if trouve is None:
                continue
            # LES MONTANTS QUI SUIVENT LE LIBELLE, pas ceux de toute la ligne.
            # L'extraction met parfois deux colonnes du bilan cote a cote :
            # « Comptes de regularisation 8 949 11 987 Capitaux propres et
            # ressources assimilees 211 371 233 303 ». Lire la ligne entiere
            # donnait 8 949 comme capitaux propres.
            reste = ligne[trouve.end():]
            valeurs = montants_de_ligne(reste) or montants_de_ligne(ligne)
            if not valeurs:
                continue
            ancre = (ancres or {}).get(champ)
            choix = _colonne_par_ancre(valeurs, ancre, facteur)
            if choix is None and ancre:
                # L'ancre ne reconnait rien : peut-etre deux montants colles
                # dont le groupe de tete est court. On tente la lecture de
                # secours, et on ne la garde que si l'ancre s'y retrouve.
                secours = montants_alternatifs(reste) or montants_alternatifs(ligne)
                autre = _colonne_par_ancre(secours, ancre, facteur)
                if autre is not None:
                    valeurs, choix = secours, autre
            index = choix if choix is not None else colonne
            valeur = valeurs[min(index, len(valeurs) - 1)] * facteur
            if valeur < 0 and champ not in SIGNE_LIBRE:
                valeur = abs(valeur)
            sortie[champ] = valeur
            break
    return sortie
