"""Lire les tableaux financiers par la POSITION des montants sur la page.

POURQUOI

Tous les lecteurs precedents partaient du TEXTE de la page, ligne a ligne,
et devaient deviner a quelle colonne appartient chaque montant : deux
tableaux cote a cote sur une meme page (compte de resultat a gauche,
tableau de flux a droite chez TotalEnergies CI), un numero de note colle
aux montants, l'ordre des colonnes qui change d'un tableau a l'autre
(SGBCI). Le PDF, lui, sait OU est chaque mot. Un montant appartient a la
colonne de l'annee dont l'en-tete le surplombe.

LA METHODE

1. Les EN-TETES : les annees (« 2024 », « 2 024 », « 31/12/2024 ») posees
   cote a cote sur une meme ligne forment l'en-tete d'un tableau, dont
   elles delimitent la largeur.
2. Les MONTANTS : les groupes de chiffres voisins (« 621 » « 042 ») sont
   recolles en un montant, avec son signe (tiret, parentheses).
3. Chaque montant va au tableau dont l'en-tete le surplombe, puis a la
   colonne de meme RANG : les montants s'alignent a droite et ne tombent
   pas exactement sous leur annee (TotalEnergies : 2024 finit a 900, ses
   montants a 941) ; mais la k-ieme colonne de montants, de gauche a
   droite, est la k-ieme annee. Une ligne qui n'a pas autant de colonnes
   de montants que d'annees est ecartee — sauf les lignes a une seule
   valeur, rangees dans la colonne la plus proche.
4. Le LIBELLE est le texte a gauche des montants, sur la meme ligne ou, a
   defaut, sur la ligne juste au-dessus (les tableaux de flux coupent le
   libelle et les montants sur deux lignes).
"""
from __future__ import annotations

import re

ANNEE = re.compile(r"^(?:\d{2}/\d{2}/)?(20\d{2})$")
CHIFFRES = re.compile(r"^\(?[-−–]?\(?\d{1,3}\)?$|^\(?[-−–]?\d{4,}\)?$")
TOL_Y = 3.5


def _annees(mots: list) -> list:
    """[(annee, x0, x1, top)] ; « 2 024 » est recolle en 2024."""
    sortie = []
    i = 0
    while i < len(mots):
        m = mots[i]
        t = m["text"].strip(":")
        a = ANNEE.match(t)
        if a:
            sortie.append((int(a.group(1)), m["x0"], m["x1"], m["top"]))
        elif t == "2" and i + 1 < len(mots) and re.fullmatch(r"0[0-9]{2}", mots[i + 1]["text"]) \
                and abs(mots[i + 1]["top"] - m["top"]) < TOL_Y and mots[i + 1]["x0"] - m["x1"] < 6:
            sortie.append((2000 + int(mots[i + 1]["text"][1:]), m["x0"], mots[i + 1]["x1"], m["top"]))
            i += 1
        i += 1
    return sortie


def tableaux(mots: list) -> list:
    """Les en-tetes : [{"top", "x0", "x1", "annees": [(annee, xc)]}], une par
    ligne portant au moins deux annees distinctes, groupees par proximite."""
    ans = sorted(_annees(mots), key=lambda a: (round(a[3]), a[1]))
    sortie = []
    for an, x0, x1, top in ans:
        for t in sortie:
            if abs(t["top"] - top) < TOL_Y and x0 - t["x1"] < 160 and an not in [a for a, _ in t["annees"]]:
                t["annees"].append((an, (x0 + x1) / 2))
                t["x1"] = max(t["x1"], x1)
                break
        else:
            sortie.append({"top": top, "x0": x0, "x1": x1, "annees": [(an, (x0 + x1) / 2)]})
    return [t for t in sortie if len(t["annees"]) >= 2]


def montants(mots: list) -> list:
    """[(valeur, x0, x1, top)] : groupes de chiffres recolles."""
    sortie = []
    courant = None
    for m in sorted(mots, key=lambda w: (round(w["top"] / TOL_Y), w["x0"])):
        t = m["text"].replace("−", "-").replace("–", "-")
        if not CHIFFRES.match(t):
            if courant:
                sortie.append(courant)
            courant = None
            continue
        chiffres = re.sub(r"\D", "", t)
        negatif = t.startswith("-") or t.startswith("(")
        if (courant and abs(m["top"] - courant["top"]) < TOL_Y and 0 <= m["x0"] - courant["x1"] < 7
                and len(chiffres) == 3 and not negatif):
            courant["txt"] += chiffres
            courant["x1"] = m["x1"]
            courant["neg"] = courant["neg"] or t.endswith(")")
        else:
            if courant:
                sortie.append(courant)
            courant = {"txt": chiffres, "x0": m["x0"], "x1": m["x1"], "top": m["top"], "neg": negatif}
    if courant:
        sortie.append(courant)
    # Plus de quinze chiffres : deux colonnes recollees (BOA Benin,
    # « 104674009283112 »), pas un montant.
    return [(-float(c["txt"]) if c["neg"] else float(c["txt"]), c["x0"], c["x1"], c["top"])
            for c in sortie if c["txt"] and len(c["txt"]) <= 15]


def lignes(mots: list) -> list:
    """[(tableau, libelle, {annee: valeur})] pour tous les tableaux de la page."""
    tabs = tableaux(mots)
    if not tabs:
        return []
    vals = montants(mots)
    textes = [m for m in mots if re.search(r"[A-Za-zÀ-ÿ]{2,}", m["text"])]
    sortie = []
    for i, tab in enumerate(tabs):
        # Largeur du tableau : de son en-tete jusqu'au prochain en-tete a
        # droite ; hauteur : jusqu'au prochain en-tete dessous, meme largeur.
        annees = sorted(tab["annees"], key=lambda a: a[1])
        gauche = tab["x0"] - 140
        droite = tab["x1"] + 110
        dessous = min([t["top"] for t in tabs if t["top"] > tab["top"] + 20
                       and not (t["x1"] < gauche or t["x0"] > droite)] or [1e9])
        dans = [v for v in vals if gauche <= (v[1] + v[2]) / 2 <= droite
                and tab["top"] + 4 < v[3] < dessous and not (1900 < abs(v[0]) < 2100 and v[0] == int(v[0]))]
        par_ligne = {}
        for v in dans:
            cle = next((k for k in par_ligne if abs(k - v[3]) < TOL_Y), v[3])
            par_ligne.setdefault(cle, []).append(v)
        for top, vs in sorted(par_ligne.items()):
            vs = sorted(vs, key=lambda v: v[2])
            # Seules comptent les colonnes a droite du debut de l'en-tete :
            # une note ou un pourcentage a gauche n'est pas un exercice.
            vs = [v for v in vs if v[2] >= annees[0][1] - 60]
            if len(vs) == len(annees):
                valeurs = {a: v[0] for (a, _), v in zip(annees, vs)}
            elif len(vs) == 1:
                a = min(annees, key=lambda a: abs(a[1] - (vs[0][1] + vs[0][2]) / 2))[0]
                valeurs = {a: vs[0][0]}
            else:
                continue
            premier = min(v[1] for v in vs)
            # Le libelle commence apres le dernier montant situe a sa gauche
            # sur la meme ligne : un bilan en deux volets (actif | passif)
            # ne doit pas preter au passif le libelle de l'actif.
            borne = max([v[2] for v in vals if abs(v[3] - top) < TOL_Y and v[2] < premier - 2]
                        or [gauche - 400])
            libelle = [w for w in textes if abs(w["top"] - top) < TOL_Y
                       and w["x0"] > borne and w["x1"] <= premier + 2]
            if not libelle:
                dessus = [w for w in textes if 0 < top - w["top"] < 14
                          and w["x0"] > borne and w["x1"] <= premier + 2]
                if dessus:
                    haut = max(w["top"] for w in dessus)
                    libelle = [w for w in dessus if abs(w["top"] - haut) < TOL_Y]
            texte = " ".join(w["text"] for w in sorted(libelle, key=lambda w: w["x0"]))
            if texte:
                sortie.append((i, texte, valeurs))
    return sortie


def lire_pdf(chemin: str) -> list:
    """Toutes les lignes lues par position dans un PDF a texte."""
    import pdfplumber
    sortie = []
    with pdfplumber.open(chemin) as pdf:
        for n, page in enumerate(pdf.pages):
            try:
                mots = page.extract_words(use_text_flow=False)
            except Exception:                                    # noqa: BLE001
                continue
            sortie += [((n, i), lib, v) for i, lib, v in lignes(mots)]
    return sortie


# --- Des lignes aux postes ------------------------------------------------

def _p(motif):
    return re.compile(motif)


POSTES = {
    "revenue": _p(r"^(total\s+)?chiffre\s+d.?affaires(?!\s+(&|et)\s+autres)|^produit\s+net\s+bancaire"),
    "net_income": _p(r"^(resultat|benefice)\s+net(\s+de\s+l.?exercice)?$|^resultat\s+net\s+\(|^perte\s+nette"),
    "ebitda": _p(r"exc[e]dent\s+brut\s+d.?exploitation"),
    "ebit": _p(r"^resultat\s+d.?exploitation"),
    "interest_expense": _p(r"frais\s+financiers|charges\s+d.?interets"),
    "capex_corporelles": _p(r"acquisitions?\s+d.?immobilisations\s+corporelles$"),
    "capex_incorporelles": _p(r"acquisitions?\s+d.?immobilisations\s+incorporelles$"),
    "capex_global": _p(r"acquisitions?\s+d.?immobilisations(\s+corporelles\s+et\s+incorporelles)?$"),
    "cfo": _p(r"(flux|tresorerie).*(provenant|generee|lies?)\s+(des|par\s+les|aux)\s+activites\s+(operationnelles|d.?exploitation)$"),
    "total_assets": _p(r"^total\s+(general|de\s+l.?actif|actif|du\s+bilan|bilan)$"),
    "equity": _p(r"^(total\s+(des\s+)?)?capitaux\s+propres(\s+et\s+ressources\s+assimilees)?$"),
}


def _plie(libelle):
    from analysis.lecture_syscohada import _plier
    plie = re.sub(r"^[a-z]{2}\s+|\(.*?\)|[*:]", " ", _plier(libelle)).strip()
    return re.sub(r"\s+", " ", plie)


ANCRES = ("revenue", "net_income", "equity", "total_assets", "ebit", "ebitda", "cfo")
FACTEURS = (1.0, 1e3, 1e6, 1e9)


def lire_document(lignes_lues: list, base: dict) -> dict:
    """{(annee, poste): valeur en FCFA} pour les tableaux VERIFIES.

    `base` : {(annee, champ): valeur connue}. Un tableau n'est retenu que si,
    pour CHACUNE de ses annees presentes dans la base, une de ses lignes y
    retombe a un pour mille — pour un facteur d'unite unique. C'est ce qui
    prouve, tableau par tableau, l'ordre des colonnes et l'unite : la CIE
    publie trois « Total actif » (social, consolide, concession) et un
    tableau aux colonnes inversees ; la SIB son bilan en milliards et son
    compte de resultat en millions.
    """
    from analysis.lecture_bancaire import _egal
    par_tableau = {}
    for tab, libelle, valeurs in lignes_lues:
        plie = _plie(libelle)
        postes_tab = par_tableau.setdefault(tab, {})
        for poste, motif in POSTES.items():
            if poste not in postes_tab and motif.search(plie):
                postes_tab[poste] = valeurs
    lus = {}
    for tab, postes_tab in par_tableau.items():
        annees = {a for v in postes_tab.values() for a in v}
        connues = {a for a in annees if any((a, c) in base for c in ANCRES)}
        if len(connues) < 2:
            continue
        retenus = []
        for f in FACTEURS:
            ancre = {a: False for a in connues}
            contredit = False
            for poste in ANCRES:
                for a, v in postes_tab.get(poste, {}).items():
                    b = base.get((a, poste))
                    if b is None or a not in ancre:
                        continue
                    if _egal(abs(v) * f, abs(b)):
                        ancre[a] = True
            if all(ancre.values()):
                retenus.append(f)
        if len(retenus) != 1:
            continue
        f = retenus[0]
        for poste, valeurs in postes_tab.items():
            for a, v in valeurs.items():
                lus.setdefault((a, poste), set()).add(round(v * f))
    # Deux tableaux verifies qui divergent sur un meme poste : on s'abstient.
    return {k: float(next(iter(v))) for k, v in lus.items() if len(v) == 1}
