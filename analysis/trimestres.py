"""
Normalisation des publications de periode en TRIMESTRES calendaires.

Les emetteurs BRVM publient en CUMUL depuis le debut de l'exercice, jamais par
periode isolee. Verifie sur les donnees : BOA Burkina 2025 donne 28,7 Mds au
premier semestre et 43,3 au « troisieme trimestre », soit exactement le
rapport 9/6 ; BNBC 2024 donne 10,8 au premier trimestre et 32,8 au troisieme,
soit trois fois. Ces documents couvrent donc 3, 6 et 9 mois.

Un trimestre isole s'en deduit par difference :

    Q1 = cumul 3 mois
    Q2 = cumul 6 mois  - cumul 3 mois
    Q3 = cumul 9 mois  - cumul 6 mois
    Q4 = exercice      - cumul 9 mois

C'est ce qui permet de comparer des periodes de meme duree, seule comparaison
licite. Un semestre confronte a un trimestre annoncerait un doublement qui
n'existe pas.

Aucune valeur n'est inventee : un trimestre ne se deduit que si les deux
cumuls qui l'encadrent existent ET se suivent. Un cumul qui decroit signale
une donnee fausse, et rien n'est produit.
"""

from typing import Optional

# Duree couverte, en mois, par chaque libelle de publication.
_COUVERTURE = {"T1": 3, "S1": 6, "T2": 6, "T3": 9, "T4": 12, "S2": 12}

LIBELLES = {1: "T1", 2: "T2", 3: "T3", 4: "T4"}


def _variation(actuel, precedent):
    """Variation d'un trimestre au MEME trimestre de l'exercice precedent."""
    if actuel is None or precedent in (None, 0):
        return None
    return (actuel - precedent) / abs(precedent) * 100


def _cumuls(lignes: list) -> dict:
    """Cumuls disponibles pour un exercice : {mois couverts: ligne}."""
    sortie = {}
    for ligne in lignes:
        periode = (ligne.get("periode") or "").upper()
        if not periode and ligne.get("quarter") in (1, 2, 3, 4):
            periode = f"T{ligne['quarter']}"
        mois = _COUVERTURE.get(periode)
        if not mois:
            continue
        # A egalite de couverture, la publication la plus riche l'emporte.
        ancienne = sortie.get(mois)
        if ancienne is None or _renseignement(ligne) > _renseignement(ancienne):
            sortie[mois] = ligne
    return sortie


def _renseignement(ligne: dict) -> int:
    return sum(1 for c in ("revenue", "net_income")
               if ligne.get(c) not in (None, 0))


def _difference(recent, anterieur, champ) -> Optional[float]:
    """Trimestre isole = cumul courant - cumul precedent."""
    a = (recent or {}).get(champ)
    if a in (None, 0):
        return None
    if anterieur is None:
        return a                      # premier cumul : c'est deja un trimestre
    b = anterieur.get(champ)
    if b in (None, 0):
        return None
    ecart = a - b
    # Un cumul qui DECROIT n'est pas un cumul : l'une des deux valeurs est
    # fausse. On prefere ne rien produire plutot qu'un trimestre negatif.
    if ecart < 0 and a > 0:
        return None
    return ecart


def trimestres_normalises(ticker: str) -> list:
    """Trimestres calendaires deduits des publications, du plus recent au plus
    ancien. Chaque entree porte l'annee, le rang du trimestre, et le fait que
    la valeur soit deduite ou publiee telle quelle."""
    from data.db import get_connection

    conn = get_connection()
    try:
        lignes = [dict(l) for l in conn.execute(
            """SELECT fiscal_year, quarter, periode, revenue, net_income
               FROM quarterly_data WHERE ticker = ?""", (ticker,)).fetchall()]
        annuels = {dict(l)["fiscal_year"]: dict(l) for l in conn.execute(
            """SELECT fiscal_year, revenue, net_income FROM fundamentals
               WHERE ticker = ? AND revenue IS NOT NULL AND revenue <> 0""",
            (ticker,)).fetchall()}
    except Exception:
        conn.close()
        return []
    conn.close()

    par_annee = {}
    for ligne in lignes:
        par_annee.setdefault(ligne.get("fiscal_year"), []).append(ligne)

    sortie = []
    for annee, groupe in par_annee.items():
        if not annee:
            continue
        cumuls = _cumuls(groupe)
        # L'exercice clos fournit le cumul douze mois, donc le quatrieme
        # trimestre par difference.
        if 12 not in cumuls and annee in annuels:
            cumuls[12] = annuels[annee]

        for rang, (mois, precedent) in enumerate(
                [(3, None), (6, 3), (9, 6), (12, 9)], start=1):
            courant = cumuls.get(mois)
            if courant is None:
                continue
            anterieur = cumuls.get(precedent) if precedent else None
            if precedent and anterieur is None:
                continue          # on ne devine pas le cumul manquant
            ca = _difference(courant, anterieur, "revenue")
            rn = _difference(courant, anterieur, "net_income")

            # Un trimestre pese environ un quart de l'exercice. Au-dela de
            # 60 %, la difference ne decrit pas un trimestre mais l'ecart
            # entre deux cumuls dont l'un est faux : Societe Generale CI
            # ressortait a 194,7 Mds au troisieme trimestre 2025 quand ses
            # autres trimestres font 66 Mds, soit 74 % de l'exercice a lui
            # seul. On prefere une case vide a un trimestre invente.
            reference = (annuels.get(annee) or {}).get("revenue")
            if ca and reference and abs(ca) > 0.6 * abs(reference):
                ca = None
            # Un cumul identique au precedent produit un trimestre nul : ce
            # n'est pas une activite nulle, c'est une valeur non mise a jour.
            if not ca:
                ca = None
            if ca is None and rn is None:
                continue
            sortie.append({
                "annee": annee,
                "trimestre": rang,
                "libelle": f"{LIBELLES[rang]} {annee}",
                "revenue": ca,
                "net_income": rn,
                "deduit": anterieur is not None,
            })

    sortie.sort(key=lambda e: (e["annee"], e["trimestre"]), reverse=True)
    return sortie


def derniers_trimestres(ticker: str, combien: int = 4) -> list:
    """Les N derniers trimestres CALENDAIRES, du plus recent au plus ancien.

    Calendaires, et non « les N derniers disponibles » : la suite doit etre
    continue. Partant de T1 2026, elle donne T1 2026, T4 2025, T3 2025,
    T2 2025 — et un trimestre manquant reste une case vide a sa place plutot
    que d'etre remplace par un trimestre plus ancien.

    Sans cela, on additionnait des trimestres separes par des trous : Orange
    CI cumulait T4 2025, T3 2025, T1 2024 et T4 2023, d'ou une « croissance »
    de 250 % qui ne mesurait rien.
    """
    connus = {(t["annee"], t["trimestre"]): t
              for t in trimestres_normalises(ticker)}
    if not connus:
        return []

    annee, rang = max(connus)
    suite = []
    for _ in range(combien):
        entree = dict(connus.get((annee, rang), {
            "annee": annee, "trimestre": rang,
            "libelle": f"{LIBELLES[rang]} {annee}",
            "revenue": None, "net_income": None, "deduit": False,
            "absent": True,
        }))
        # Le MEME trimestre de l'exercice precedent : c'est la seule
        # comparaison qui neutralise la saisonnalite. Confronter un trimestre
        # au precedent ferait passer un creux saisonnier pour un recul.
        precedent = connus.get((entree["annee"] - 1, entree["trimestre"]))
        entree["revenue_n1"] = (precedent or {}).get("revenue")
        entree["net_income_n1"] = (precedent or {}).get("net_income")
        entree["revenue_var"] = _variation(entree.get("revenue"),
                                           entree["revenue_n1"])
        entree["net_income_var"] = _variation(entree.get("net_income"),
                                              entree["net_income_n1"])
        suite.append(entree)
        rang -= 1
        if rang == 0:
            rang, annee = 4, annee - 1
    return suite


def moyenne_mobile(ticker: str) -> Optional[dict]:
    """Somme des quatre derniers trimestres, comparee au dernier exercice clos.

    C'est la lecture qui repond a « ou en est la societe MAINTENANT » : quatre
    trimestres glissants couvrent douze mois, donc se comparent legitimement a
    un exercice. La croissance qui en ressort precede de plusieurs mois celle
    que l'exercice suivant entérinera.
    """
    from data.db import get_connection

    quatre = derniers_trimestres(ticker, 4)
    if len(quatre) < 4:
        return None

    # La somme n'a de sens que si les QUATRE trimestres consecutifs sont la :
    # trois trimestres compares a un exercice annoncent une chute de 25 %
    # qui n'existe pas.
    somme_ca = sum(t["revenue"] or 0 for t in quatre)
    somme_rn = sum(t["net_income"] or 0 for t in quatre)
    complet_ca = all(t.get("revenue") for t in quatre)
    complet_rn = all(t.get("net_income") is not None and t.get("revenue")
                     for t in quatre)

    # La reference doit etre un exercice ANNUEL CLOS. Prendre simplement la
    # ligne la plus recente comparait douze mois glissants a un trimestre :
    # Societe Generale CI ressortait a +494 % de croissance, ses 393,9 Mds sur
    # douze mois etant confrontes aux 66,3 Mds d'un exercice 2026 alimente par
    # un seul trimestre.
    from data.storage import exercices_annuels

    annuels = exercices_annuels([ticker]).get(ticker, set())
    conn = get_connection()
    try:
        candidats = [dict(l) for l in conn.execute(
            """SELECT fiscal_year, revenue, net_income FROM fundamentals
               WHERE ticker = ? AND revenue IS NOT NULL AND revenue <> 0
               ORDER BY fiscal_year DESC""", (ticker,)).fetchall()]
    except Exception:
        candidats = []
    conn.close()

    exercice = None
    for candidat in candidats:
        if not annuels or candidat.get("fiscal_year") in annuels:
            exercice = candidat
            break
    if exercice is None:
        return None

    def _croissance(actuel, reference):
        if actuel is None or not reference:
            return None
        return (actuel - reference) / abs(reference) * 100

    return {
        "periode": f"{quatre[-1]['libelle']} → {quatre[0]['libelle']}",
        "exercice_ref": exercice.get("fiscal_year"),
        "revenue": somme_ca if complet_ca else None,
        "revenue_ref": exercice.get("revenue"),
        "revenue_croissance": (_croissance(somme_ca, exercice.get("revenue"))
                               if complet_ca else None),
        "net_income": somme_rn if complet_rn else None,
        "net_income_ref": exercice.get("net_income"),
        "net_income_croissance": (_croissance(somme_rn, exercice.get("net_income"))
                                  if complet_rn else None),
    }
