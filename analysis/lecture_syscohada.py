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
    "revenue": (r"chiffre\s*d.?\s*[a@]ffaires?(?!\s*hao)",
                r"produit\s*net\s*bancaire", r"\bPNB\b"),
    "net_income": (r"r[ée]sultat\s*net\s*(de\s*l.?\s*exercice)?",
                   r"r[ée]sultat\s*de\s*l.?\s*exercice",
                   r"b[ée]n[ée]fice\s*net"),
    "equity": (r"total\s*capitaux\s*propres",
               r"capitaux\s*propres\s*et\s*ressources",
               r"total\s*des\s*capitaux\s*propres"),
    "total_assets": (r"total\s*(g[ée]n[ée]ral|actif(?!\s*(immobilis|circulant))|bilan)",
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
LIBELLES["total_assets"] += (r"total\s*(de\s*l.?\s*)?actif(?!\s*(immobilis|circulant))",
                             r"total\s*passif(?!\s*(circulant|non\s*courant|courant))",
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

# LIBELLES DE SECOURS, lus seulement quand aucun libelle principal ne rend
# rien. SITAB, distributeur, n'ecrit pas de chiffre d'affaires : ses ventes
# de marchandises en tiennent lieu. Chez un industriel, la meme ligne n'est
# qu'une partie du chiffre d'affaires — d'ou le rang de secours. SMB ecrit
# « CAPITAUX PROPRES 42 913 35 294 », sans « total » : le libelle nu n'est
# retenu qu'en debut de ligne et suivi d'un montant, pour ne pas prendre un
# titre de section.
LIBELLES_SECOURS = {
    "revenue": (r"ventes?\s*de\s*marchandises",),
    "equity": (r"^capitaux\s*propres(?=\s*\(?-?\d|\s*$)",),
}

# Champs dont un montant negatif a un sens dans un tableau de flux.
SIGNE_LIBRE = {"net_income", "ebit", "ebitda", "cfo", "capex", "dividends_total"}

# Un montant : des groupes de chiffres separes par des espaces ou des points.
# Trois caracteres au moins : « 409 » est un montant dans un tableau en
# millions (BOA Niger). Le nombre minimal de CHIFFRES, fixe selon l'unite,
# fait ensuite le tri (voir `_MINIMUM_CHIFFRES`).
MONTANT = re.compile(r"\(?-?\d[\d  .']{1,}\d\)?")

# Un groupe de trois chiffres, l'unite de ces tableaux.
TRIPLET = re.compile(r"\d{3}")


def _plier(texte: str) -> str:
    """Minuscules, accents retires, espaces normalises."""
    texte = unicodedata.normalize("NFD", texte)
    texte = "".join(c for c in texte if unicodedata.category(c) != "Mn")
    return re.sub(r"[ \t\xa0]+", " ", texte.lower())


# Nombre minimal de chiffres d'un montant. Quatre en francs ou en milliers :
# en dessous, c'est un numero de note ou un pourcentage. TROIS quand le
# document est en millions — « Resultat net de l'exercice 409 » chez BOA
# Niger vaut 409 millions, et le rejeter laissait le champ vide. Fixe par
# `lire_detaille` selon l'echelle du document.
_MINIMUM_CHIFFRES = [4]


def _nombre(brut: str) -> float | None:
    negatif = brut.strip().startswith("(") or brut.strip().startswith("-")
    chiffres = re.sub(r"[^\d]", "", brut)
    if len(chiffres) < _MINIMUM_CHIFFRES[0]:
        return None
    valeur = float(chiffres)
    return -valeur if negatif else valeur


def _chiffres(groupe: str) -> int:
    return sum(c.isdigit() for c in groupe)


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

    # UN GROUPE DE QUATRE CHIFFRES OU PLUS n'est pas un groupe de milliers :
    # c'est un nombre a part, colle au suivant — le plus souvent un
    # millesime, « 2025 1 844 ».
    segments, courant = [], [morceaux[0]]
    for groupe in morceaux[1:]:
        if _chiffres(groupe) > 3 or _chiffres(courant[-1]) > 3:
            segments.append(courant)
            courant = [groupe]
        elif len(groupe) < 3:
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


ANNEE_SEULE = re.compile(r"\(?-?((?:19|20)\d{2})\)?")


def _est_une_annee(brut: str) -> bool:
    """Un millesime nu n'est pas un montant.

    « effective depuis septembre 2020 et apporte 13 % a 25 % du chiffre
    d'affaires global » : faute de montant apres le libelle, la ligne entiere
    etait lue, et LNB affichait un chiffre d'affaires de 2 020 francs. Dans
    ces etats, un montant de quatre chiffres s'ecrit « 2 020 » ; quatre
    chiffres colles entre 1990 et 2100 sont une date.
    """
    m = ANNEE_SEULE.fullmatch(brut.strip())
    return bool(m) and 1990 <= int(m.group(1)) <= 2100


def _est_decimal(ligne: str, m) -> bool:
    """Un montant suivi d'une virgule et d'un chiffre est du commentaire.

    Les etats financiers ecrivent des entiers. « classement par total bilan
    2025 1 844,6 Md FCFA » est une phrase du rapport de gestion : BICI Benin
    affichait un total de bilan de 1 844 francs.
    """
    return bool(re.match(r",\d", ligne[m.end():m.end() + 2]))


def _fusionner_tetes(brut: str) -> str:
    """Recolle un groupe de tete que la mise en page a coupe chiffre a chiffre.

    Bernabe ecrit « 4 2 454 158 321 » pour 42 454 158 321, Filtisac « 4 65
    981 » pour 465 981 : l'espacement des caracteres coupe le premier groupe.
    Deux groupes courts consecutifs dont la reunion tient en trois chiffres,
    suivis d'un groupe de trois, sont recolles.

    Lecture de SECOURS seulement : « 7 4 270 » peut etre deux montants. C'est
    l'ancre qui dit laquelle des deux lectures est la bonne.
    """
    morceaux = brut.strip().strip("()").replace(".", " ").split()
    sortie, i = [], 0
    while i < len(morceaux):
        tete = morceaux[i]
        j = i + 1
        while (len(tete) < 3 and j < len(morceaux) and len(morceaux[j]) < 3
               and len(tete) + len(morceaux[j]) <= 3):
            tete += morceaux[j]
            j += 1
        if j > i + 1 and not (j < len(morceaux) and len(morceaux[j]) == 3):
            tete, j = morceaux[i], i + 1
        sortie.append(tete)
        i = j
    return " ".join(sortie)


def montants_tetes_fusionnees(ligne: str) -> list:
    """Les montants de la ligne, groupes de tete recolles."""
    ligne = RENVOI_NOTE.sub(" ", ligne)
    sortie = []
    for m in MONTANT.finditer(ligne):
        if _est_une_annee(m.group(0)):
            continue
        negatif = m.group(0).strip().startswith(("(", "-"))
        for morceau in _scinder_colonnes(_fusionner_tetes(m.group(0))):
            v = _nombre(morceau)
            if v is not None:
                sortie.append(-abs(v) if negatif else v)
    return sortie


def montants_alternatifs(ligne: str) -> list:
    """Les montants de la ligne, lus avec le decoupage de secours."""
    ligne = RENVOI_NOTE.sub(" ", ligne)
    sortie = []
    for m in MONTANT.finditer(ligne):
        if _est_une_annee(m.group(0)):
            continue
        negatif = m.group(0).strip().startswith(("(", "-"))
        for morceau in _scinder_en_deux(m.group(0)) or [m.group(0)]:
            v = _nombre(morceau)
            if v is not None:
                sortie.append(-abs(v) if negatif else v)
    return sortie


def _sans_doublons_d_unite(valeurs: list) -> list:
    """Retire un montant repete dans une unite plus petite juste avant lui.

    BICI Benin : « Resultat net 36 236 705 101 36 237 29 058 27 270 » porte
    le resultat 2025 en FRANCS, puis les trois colonnes certifiees en
    MILLIONS. Le premier n'est pas une colonne de plus, c'est le deuxieme
    ecrit autrement : leur rapport vaut un million, a l'arrondi pres. Deux
    voisins dans un rapport de mille ou d'un million, a 0,2 % pres, sont
    le meme montant ; on garde celui de l'unite du tableau.
    """
    sortie = list(valeurs)
    i = 0
    while i < len(sortie) - 1:
        a, b = abs(sortie[i]), abs(sortie[i + 1])
        if b and any(abs(a / (b * k) - 1) <= 0.002 for k in (1_000, 1_000_000)):
            del sortie[i]
            continue
        i += 1
    return sortie


def montants_de_ligne(ligne: str) -> list:
    """Les montants d'une ligne, dans l'ordre. Le premier est l'exercice."""
    return _sans_doublons_d_unite(_montants_bruts(ligne))


# Un renvoi a l'annexe : « Chiffre d'affaires 4.2 1 776 443 1 620 701 »
# chez Sonatel. Un ou deux chiffres, un point, un ou deux chiffres, isoles.
# Les milliers separes par des points (« 172.235.053 ») ont des groupes de
# TROIS chiffres et ne sont pas touches.
RENVOI_NOTE = re.compile(r"(?<![\d.])\d{1,2}\.\d{1,2}(?![\d.])")


def _montants_bruts(ligne: str) -> list:
    ligne = RENVOI_NOTE.sub(" ", ligne)
    sortie = []
    for m in MONTANT.finditer(ligne):
        if _est_une_annee(m.group(0)) or _est_decimal(ligne, m):
            continue
        negatif = m.group(0).strip().startswith(("(", "-"))
        for morceau in _scinder_colonnes(m.group(0)):
            if _est_une_annee(morceau):
                continue
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


DECLARATION_UNITE = re.compile(
    r"\ben\s+(milliard|million|millier)s?\s+(de\s+)?(francs?\s+)?f\.?\s*\.?\s*cfa")
UNITES = {"milliard": 1_000_000_000.0, "million": 1_000_000.0, "millier": 1_000.0}


def echelles_par_ligne(lignes: list, defaut: float) -> list:
    """L'unite de chaque ligne : celle de la derniere declaration qui la precede.

    L'UNITE PEUT CHANGER DANS UN MEME DOCUMENT. SODECI publie ses comptes
    SYSCOHADA en milliers puis ses comptes IFRS en millions ; BOA Niger
    n'annonce « en millions de F CFA » qu'au milieu du rapport des
    commissaires, et son resultat de 409 millions etait lu comme 409 francs.
    Chaque ligne prend donc l'unite de la derniere mention « en millions /
    milliers de FCFA » rencontree avant elle — et, avant toute mention,
    celle du document.
    """
    sortie, courante = [], defaut
    for ligne in lignes:
        m = DECLARATION_UNITE.search(ligne)
        if m:
            courante = UNITES[m.group(1)]
        sortie.append(courante)
    return sortie


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
    if not ancre or len(valeurs) < 2:
        return None
    # LA COLONNE LA PLUS PROCHE, pas la premiere a 1 % pres. Vivo : 604 978
    # (2025) et 600 708 (2024) different de 0,7 % ; l'ancre 600 708 acceptait
    # donc la PREMIERE colonne, et le lecteur rendait 2024 pour 2025.
    ecarts = [(abs(abs(v * facteur) - abs(ancre)) / abs(ancre), i)
              for i, v in enumerate(valeurs)]
    ecart, i = min(ecarts)
    # TROIS POUR MILLE. La base porte des montants exacts, ou arrondis au
    # million : l'ecart reel est bien en dessous. A 1 %, l'ancre reconnaissait
    # des notions VOISINES — chez TotalEnergies Senegal, un chiffre
    # d'affaires 2024 de 484,9 Md « retrouve » dans des ventes de
    # marchandises de 481,0 Md. Mieux vaut une lecture devinee, jamais
    # proposee, qu'une lecture sure a tort.
    if ecart > 0.003:
        return None
    # Le voisin de la MEME PAIRE, pas le suivant dans la ligne. Chez Palm
    # CI, « Resultat net 15 508 655 15 861 643 Services exterieurs
    # -23 133 174 » porte quatre montants : l'ancre tombe sur le second,
    # et prendre « le suivant » ramenait les services exterieurs.
    return i - 1 if i > 0 else i + 1


TITRE_REFERENTIEL = re.compile(r"\b(syscohada|ifrs)\b")


def _sans_sections_ifrs(lignes: list) -> list:
    """Les lignes des etats SYSCOHADA, quand le document porte aussi l'IFRS.

    SODECI publie dans le meme document ses comptes individuels SYSCOHADA
    (en milliers) et ses comptes IFRS (en millions). Le resultat IFRS,
    « 5 881 » millions, n'est pas le resultat SYSCOHADA de 4 662 738
    milliers — et la base, comme les fiches societe, suit le SYSCOHADA.

    Un titre court qui nomme un referentiel ouvre une section. Si le
    document en a des deux sortes, les sections IFRS sont ecartees ; s'il
    n'en a qu'une, rien ne change — BICI Benin, en IFRS seul, reste lu.
    """
    sections, courant = [], None
    for ligne in lignes:
        m = TITRE_REFERENTIEL.search(ligne)
        if m and len(ligne) < 120:
            courant = m.group(1)
        sections.append(courant)
    if not {"syscohada", "ifrs"} <= set(sections):
        return lignes
    return [l for l, sec in zip(lignes, sections) if sec != "ifrs"]


def _colonne_nette(valeurs: list):
    """2 si la ligne porte brut, amortissements, net : la colonne nette.

    L'actif SYSCOHADA a quatre colonnes — brut, amortissements et
    depreciations, net de l'exercice, net du precedent. « Total general
    25 414 101 911 10 803 021 818 14 611 080 094 14 023 255 232 » chez Erium :
    la premiere colonne est le brut, et le total du bilan est la troisieme.

    L'arithmetique le prouve sans lire l'en-tete : brut moins amortissements
    egale net, au franc pres. Sans cette egalite, on ne conclut rien.
    """
    # QUATRE COLONNES, PAS TROIS. « Exercice, precedent, variation » verifie
    # la meme egalite : 604 978 - 600 707 = 4 270. Passe sur 128 exercices,
    # la regle a trois colonnes rendait la variation pour le chiffre
    # d'affaires, les depots ou le resultat d'exploitation — quinze fois.
    # L'actif SYSCOHADA porte quatre colonnes, et le net de l'exercice
    # precedent est du meme ordre que celui de l'exercice.
    if len(valeurs) < 4:
        return None
    brut, amort, net, net_precedent = (abs(v) for v in valeurs[:4])
    if not (brut and amort and net and net_precedent):
        return None
    if not 0.5 <= net / net_precedent <= 2:
        return None
    if abs(brut - amort - net) <= 0.001 * brut:
        return 2
    return None


SOUS_TOTAL = re.compile(r"sous\s*-?\s*$")


def _lignes_du_champ(lignes: list, motifs: tuple, echelles: list = None) -> list:
    """Les lignes candidates d'un champ, de la plus sure a la moins sure.

    Chaque candidate est (rang, ligne, reste, valeurs). Le document fixe
    l'ordre a rang egal ; trois choses le changent.

    LE TABLEAU AVANT LA PROSE. Le rapport de gestion precede les etats :
    « le resultat net a enregistre une diminution de 36 %, atteignant 4,6
    milliards » vient quinze pages avant « Resultat Net 4 622 243 779
    7 175 554 255 ». Une ligne d'etat financier porte l'exercice ET son
    comparatif : deux montants apres le libelle la classent en tete, un seul
    ensuite.

    LA LIGNE ENTIERE EN DERNIER. Quand aucun montant ne suit le libelle, on
    lit les montants qui le precedent — l'extraction met parfois deux
    colonnes du bilan cote a cote. C'est la lecture la moins sure.

    LE TOTAL AVANT LE SOUS-TOTAL. « Sous-Total Capitaux Propres part du
    groupe 69 479 » contient « total capitaux propres » : le motif le
    reconnait, et comme la ligne precede « TOTAL CAPITAUX PROPRES 113 165 »
    dans le bilan d'Oragroup, c'est elle qui gagnait. La part du groupe
    n'est pas le total — les minoritaires en sont exclus. Un libelle precede
    de « sous- » ne sert que s'il n'y en a pas d'autre.
    """
    candidates = []
    for rang_ligne, ligne in enumerate(lignes):
        trouve = next((re.search(m, ligne) for m in motifs
                       if re.search(m, ligne)), None)
        if trouve is None:
            continue
        facteur = echelles[rang_ligne] if echelles else 1.0
        _MINIMUM_CHIFFRES[0] = 3 if facteur >= 1_000_000 else 4
        # LE MONTANT SUR LA LIGNE SUIVANTE. L'OCR d'un tableau rend souvent
        # le libelle seul, puis ses montants a la ligne : « Capitaux propres »
        # puis « 2 975 325 212 » chez SICOR. On ne l'accepte que si la ligne
        # ne porte QUE le libelle et la suivante QUE des montants — une ligne
        # de plusieurs libelles suivie d'une ligne de valeurs ne dit pas
        # laquelle va avec laquelle.
        suivante = lignes[rang_ligne + 1] if rang_ligne + 1 < len(lignes) else ""
        if (not re.search(r"[a-z]{2}", ligne[:trouve.start()] + ligne[trouve.end():])
                and re.fullmatch(r"[\d\s().\-]+", suivante.strip() or "x")):
            valeurs = montants_de_ligne(suivante)
            if valeurs:
                rang = 1 if len(valeurs) == 1 else 0
                candidates.append((rang, ligne, suivante, valeurs, facteur))
                continue
        # LES MONTANTS QUI SUIVENT LE LIBELLE, pas ceux de toute la ligne.
        # « Comptes de regularisation 8 949 11 987 Capitaux propres et
        # ressources assimilees 211 371 233 303 » : lire la ligne entiere
        # donnait 8 949 comme capitaux propres.
        reste = ligne[trouve.end():]
        valeurs = montants_de_ligne(reste)
        if len(valeurs) >= 2:
            rang = 0
        elif valeurs:
            rang = 1
        else:
            reste = ligne
            valeurs = montants_de_ligne(ligne)
            rang = 2
        if not valeurs:
            continue
        if SOUS_TOTAL.search(ligne[:trouve.start()]):
            rang += 3
        candidates.append((rang, ligne, reste, valeurs, facteur))
    return sorted(candidates, key=lambda c: c[0])


def lire_detaille(texte: str, echelle: float = None, ancres: dict = None) -> dict:
    """{champ: (montant, mode)} — le montant, et comment sa colonne a ete choisie.

    TROIS MODES, ET ILS NE VALENT PAS LA MEME CHOSE

    - « ancre » : l'exercice precedent connu est retombe dans la ligne ; la
      colonne voisine est l'exercice. Aucune devinette.
    - « brut-net » : brut - amortissements = net au franc pres.
    - « en-tete » : la colonne est deduite de l'ordre des millesimes dans
      l'en-tete. C'est une DEVINETTE — au 24/09, elle a rendu les deux seules
      valeurs fausses connues (BICI Benin, dont une ligne melange francs et
      millions), alors que les deux autres modes n'en ont rendu aucune.

    Pour chaque champ, la MEILLEURE ligne qui porte l'un de ses libelles et au
    moins un montant — ligne d'etat avant prose, total avant sous-total (voir
    `_lignes_du_champ`). La colonne retenue est celle de l'exercice courant :
    par l'ancre si elle s'y retrouve, par l'arithmetique brut - amortissements
    = net a l'actif, par l'en-tete a defaut.
    """
    colonne = colonne_de_l_exercice(texte)
    lignes = _sans_sections_ifrs([l for l in _plier(texte).split("\n") if l.strip()])
    # Une echelle imposee par l'appelant vaut pour tout le document.
    echelles = ([echelle] * len(lignes) if echelle
                else echelles_par_ligne(lignes, _echelle(texte)))
    sortie = {}
    for champ, motifs in LIBELLES.items():
        candidates = (_lignes_du_champ(lignes, motifs, echelles)
                      or _lignes_du_champ(lignes, LIBELLES_SECOURS.get(champ, ()),
                                          echelles))
        ancre = (ancres or {}).get(champ)
        lues = [_lire_candidate(champ, c, ancre, colonne) for c in candidates]
        # LA LIGNE OU L'ANCRE SE RETROUVE, D'ABORD. Onatel porte trois lignes
        # « resultat net » : un resume sans unite (« 21 129 21 471 »), puis
        # l'etat en francs (« 21 471 148 928 21 129 276 785 »). La premiere,
        # lue en francs, rendait 21 129 francs ; la troisieme contient le
        # resultat de l'exercice precedent, a l'unite pres — c'est la ligne de
        # l'etat. A defaut de ligne sure, la premiere, comme avant.
        sures = [l for l in lues if l and l[1] != "en-tete"]
        retenue = sures[0] if sures else next((l for l in lues if l), None)
        if retenue:
            sortie[champ] = retenue
    return sortie


# Aucun emetteur de la cote n'approche cent mille milliards de FCFA : le plus
# gros bilan est de l'ordre de 20 000 Md. Au-dela, l'unite de la ligne est
# fausse — une mention « en millions » plus haut dans le document appliquee a
# un tableau en francs (Sonatel 2024 : 1,38 x 10^18).
PLAFOND = 1e14


def _lire_candidate(champ, candidate, ancre, colonne):
    """(montant, mode) lu sur une ligne candidate, ou None."""
    rang, ligne, reste, valeurs, facteur = candidate
    _MINIMUM_CHIFFRES[0] = 3 if facteur >= 1_000_000 else 4
    choix = _colonne_par_ancre(valeurs, ancre, facteur)
    if choix is not None:
        # L'ANCRE PEUT SE RETROUVER DANS UNE LECTURE QUI A PERDU UN
        # CHIFFRE. « Resultat net 2 2 318 122 7 313 440 » chez Bernabe :
        # la lecture normale jette le « 2 » isole, trop court pour un
        # montant, rend 2 318 122 et 7 313 440 — et l'ancre, qui vaut
        # 7 313 440, s'y retrouve. Le resultat lu etait dix fois trop
        # petit. Si la lecture aux tetes recollees differe ET retombe
        # elle aussi sur l'ancre, c'est elle qui explique tous les
        # chiffres de la ligne.
        recollee = montants_tetes_fusionnees(reste)
        autre = _colonne_par_ancre(recollee, ancre, facteur)
        if recollee != valeurs and autre is not None:
            valeurs, choix = recollee, autre
    if choix is None and ancre:
        # L'ancre ne reconnait rien : peut-etre deux montants colles
        # dont le groupe de tete est court. On tente la lecture de
        # secours, et on ne la garde que si l'ancre s'y retrouve.
        for lecture in (montants_alternatifs, montants_tetes_fusionnees):
            secours = lecture(reste)
            autre = _colonne_par_ancre(secours, ancre, facteur)
            if autre is not None:
                valeurs, choix = secours, autre
                break
    if choix is not None:
        mode = "ancre"
    else:
        choix = _colonne_nette(valeurs) if champ == "total_assets" else None
        mode = "brut-net" if choix is not None else "en-tete"
    if not valeurs:
        return None
    index = choix if choix is not None else colonne
    valeur = valeurs[min(index, len(valeurs) - 1)] * facteur
    if abs(valeur) > PLAFOND:
        # Le plafond juge la valeur FINALE : « 97 819 112 928 » en millions
        # depasse le plafond tant qu'il est colle, pas une fois coupe en deux.
        if facteur > 1:
            return _lire_candidate(champ, (rang, ligne, reste,
                                           _montants_bruts(reste) or valeurs, 1.0),
                                   ancre, colonne)
        return None
    if valeur < 0 and champ not in SIGNE_LIBRE:
        valeur = abs(valeur)
    return (valeur, mode)


def lire(texte: str, echelle: float = None, ancres: dict = None) -> dict:
    """{champ: montant} — ce que le texte livre, sans jugement de valeur."""
    return {c: v for c, (v, _) in lire_detaille(texte, echelle, ancres).items()}
