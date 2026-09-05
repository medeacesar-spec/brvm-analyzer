"""
Referentiel sectoriel : ce que chaque secteur exige qu'on regarde.

Le principe est qu'un meme ratio ne dit pas la meme chose selon le metier.
Une marge d'EBITDA de 8 % est mediocre pour un operateur telecoms et tout a
fait normale pour un distributeur. Une rotation de l'actif de 0,4 est faible
en distribution et attendue pour un service public. Juger tout le monde sur la
meme echelle revient a se tromper de diagnostic sur la moitie de la cote.

Ce module porte donc, pour chaque secteur :
  - `grille`  : les indicateurs a afficher, dans l'ordre de lecture ;
  - `seuils`  : les bornes de jugement PROPRES au secteur ;
  - `lecture` : la phrase qui explique ce que la grille cherche a voir.

Les indicateurs eux-memes sont calcules dans `analysis.fundamental` ; ce
module ne fait que decider lesquels montrer et comment les juger.
"""

from typing import Optional

# `sens` dit dans quel sens se lit le ratio :
#   "haut" — plus c'est eleve, mieux c'est (marge, rotation, couverture)
#   "bas"  — plus c'est bas, mieux c'est (endettement, cout du risque)
# `bornes` se lit de la plus favorable a la moins favorable.

_SECTEURS = {
    "banque": {
        "libelle": "Grille bancaire",
        "lecture": (
            "Pour une banque, le résultat net n'est qu'un symptôme. Le "
            "coefficient d'exploitation mesure ce que coute la machine, le "
            "coût du risque ce que coute le portefeuille : c'est le "
            "croisement des deux — l'analyse « en ciseau » — qui fait le "
            "diagnostic."
        ),
        "grille": [
            ("Coefficient d'exploitation", "coefficient_exploitation", "pct",
             "Frais généraux rapportés au produit net bancaire"),
            ("Coût du risque / PNB", "cout_risque_pnb", "pct",
             "Ce que le portefeuille de crédits coûte en provisions"),
            ("Marge brute d'exploitation", "marge_brute_exploitation", "pct",
             "Ce qui reste du PNB une fois la machine payée"),
            ("Crédits / dépôts", "credits_depots", "pct",
             "Part des dépôts transformée en crédits"),
            ("Rendement des actifs", "roa", "pct",
             "Ce que rapporte le bilan, au-delà des fonds propres"),
        ],
        "seuils": {
            "coefficient_exploitation": ("bas", [
                (0.55, "OK", "Machine efficace ({v} %)"),
                (0.65, "Vigilance", "Frais généraux élevés ({v} %)"),
                (None, "Risque", "Structure de coûts lourde ({v} %)")]),
            "cout_risque_pnb": ("bas", [
                (0.10, "OK", "Coût du risque contenu ({v} %)"),
                (0.20, "Vigilance", "Coût du risque en hausse ({v} %)"),
                (None, "Risque", "Le portefeuille pèse lourd ({v} %)")]),
            "credits_depots": ("bas", [
                (0.85, "OK", "Liquidité confortable ({v} %)"),
                (1.00, "Vigilance", "Transformation tendue ({v} %)"),
                (None, "Risque", "Credits au-delà des dépôts ({v} %)")]),
        },
    },
    "telecommunications": {
        "libelle": "Grille télécoms",
        "lecture": (
            "Les operateurs communiquent en EBITDAaL, pas en résultat net. La "
            "marge d'EBITDA, l'intensite capitalistique et le levier disent la "
            "trajectoire bien mieux que le bas du compte de résultat — et le "
            "parc client la precede toujours."
        ),
        "grille": [
            ("Marge d'EBITDA", "marge_ebitda", "pct",
             "Rentabilité opérationnelle avant le poids des réseaux"),
            ("Intensité capitalistique", "intensite_capex", "pct",
             "Investissements rapportés au chiffre d'affaires"),
            ("Dette / EBITDA", "dette_ebitda", "fois",
             "Nombre d'années d'EBITDA pour rembourser la dette"),
            ("Marge de flux libre", "fcf_margin", "pct",
             "Trésorerie dégagée après investissements"),
        ],
        "seuils": {
            "marge_ebitda": ("haut", [
                (0.35, "OK", "Marge d'EBITDA élevée ({v} %)"),
                (0.25, "Vigilance", "Marge d'EBITDA moyenne ({v} %)"),
                (None, "Risque", "Marge d'EBITDA faible ({v} %)")]),
            "intensite_capex": ("bas", [
                (0.18, "OK", "Investissement maîtrisé ({v} % du CA)"),
                (0.25, "Vigilance", "Investissement lourd ({v} % du CA)"),
                (None, "Risque", "Investissement très lourd ({v} % du CA)")]),
            "dette_ebitda": ("bas", [
                (2.0, "OK", "Levier confortable ({v} x)"),
                (3.0, "Vigilance", "Levier a surveiller ({v} x)"),
                (None, "Risque", "Levier élevé ({v} x)")]),
        },
    },
    "industrie": {
        "libelle": "Grille industrielle",
        "lecture": (
            "Une industrie se juge sur son outil : ce qu'il produit par franc "
            "investi (rotation de l'actif), ce qu'il faut lui reinjecter "
            "chaque année pour le maintenir, et si le résultat d'exploitation "
            "couvre confortablement les intérêts."
        ),
        "grille": [
            ("Marge d'exploitation", "marge_exploitation", "pct",
             "Performance du métier, avant financement et impôt"),
            ("Marge d'EBITDA", "marge_ebitda", "pct",
             "Rentabilité avant amortissement de l'outil"),
            ("Rotation de l'actif", "rotation_actif", "fois",
             "Chiffre d'affaires engendré par franc d'actif"),
            ("Intensité capitalistique", "intensite_capex", "pct",
             "Ce que le maintien de l'outil coûte chaque année"),
            ("Couverture des intérêts", "interest_coverage", "fois",
             "Combien de fois le résultat couvre les intérêts"),
            ("Dette / EBITDA", "dette_ebitda", "fois",
             "Années d'EBITDA nécessaires au remboursement"),
        ],
        "seuils": {
            "marge_exploitation": ("haut", [
                (0.10, "OK", "Marge d'exploitation solide ({v} %)"),
                (0.05, "Vigilance", "Marge d'exploitation étroite ({v} %)"),
                (None, "Risque", "Exploitation peu rentable ({v} %)")]),
            "marge_ebitda": ("haut", [
                (0.18, "OK", "Bonne marge pour l'industrie ({v} %)"),
                (0.10, "Vigilance", "Marge moyenne ({v} %)"),
                (None, "Risque", "Marge faible ({v} %)")]),
            "rotation_actif": ("haut", [
                (1.0, "OK", "Outil bien employé ({v} x)"),
                (0.6, "Vigilance", "Outil sous-employé ({v} x)"),
                (None, "Risque", "Actif peu productif ({v} x)")]),
            "intensite_capex": ("bas", [
                (0.08, "OK", "Investissement contenu ({v} % du CA)"),
                (0.15, "Vigilance", "Investissement lourd ({v} % du CA)"),
                (None, "Risque", "Investissement très lourd ({v} % du CA)")]),
        },
    },
    "distribution": {
        "libelle": "Grille distribution",
        "lecture": (
            "La distribution ne vit pas de ses marges, qui sont minces par "
            "nature, mais de sa rotation : c'est le nombre de fois que "
            "l'actif se transforme en chiffre d'affaires qui fait le "
            "résultat. Juger un distributeur a sa marge nette conduit a le "
            "condamner à tort."
        ),
        "grille": [
            ("Rotation de l'actif", "rotation_actif", "fois",
             "Le moteur du modèle : combien l'actif fait tourner de ventes"),
            ("Marge d'exploitation", "marge_exploitation", "pct",
             "Marge du métier, structurellement étroite"),
            ("Marge nette", "net_margin", "pct",
             "Ce qui reste au bout — quelques points suffisent"),
            ("Rendement des actifs", "roa", "pct",
             "Rotation multipliee par la marge : le vrai juge de paix"),
            ("Couverture des intérêts", "interest_coverage", "fois",
             "Combien de fois le résultat couvre les intérêts"),
        ],
        "seuils": {
            "rotation_actif": ("haut", [
                (1.8, "OK", "Rotation élevée ({v} x)"),
                (1.2, "Vigilance", "Rotation moyenne ({v} x)"),
                (None, "Risque", "Rotation faible pour le secteur ({v} x)")]),
            "marge_exploitation": ("haut", [
                (0.05, "OK", "Marge correcte en distribution ({v} %)"),
                (0.025, "Vigilance", "Marge étroite ({v} %)"),
                (None, "Risque", "Marge très faible ({v} %)")]),
            "net_margin": ("haut", [
                (0.03, "OK", "Marge nette normale pour le secteur ({v} %)"),
                (0.015, "Vigilance", "Marge nette mince ({v} %)"),
                (None, "Risque", "Marge nette très mince ({v} %)")]),
        },
    },
    "agriculture": {
        "libelle": "Grille agricole",
        "lecture": (
            "Une exploitation agricole subit le cours mondial de sa matière "
            "première. Le niveau du résultat d'une année dit donc moins que sa "
            "régularité : c'est la volatilité sur quatre exercices qui "
            "distingue une bonne année d'un bon actif."
        ),
        "grille": [
            ("Volatilité du résultat", "volatilite_resultat", "pct",
             "Dispersion du résultat sur quatre exercices"),
            ("Marge d'exploitation", "marge_exploitation", "pct",
             "Performance du métier sur l'exercice"),
            ("Marge d'EBITDA", "marge_ebitda", "pct",
             "Rentabilité avant amortissement des plantations"),
            ("Intensité capitalistique", "intensite_capex", "pct",
             "Entretien et renouvellement des plantations"),
            ("Rotation de l'actif", "rotation_actif", "fois",
             "Ce que l'actif planté engendre en ventes"),
            ("Dette / EBITDA", "dette_ebitda", "fois",
             "Endettement rapporté à une année de bonne récolte"),
        ],
        "seuils": {
            "volatilite_resultat": ("bas", [
                (0.35, "OK", "Résultat régulier ({v} % d'écart)"),
                (0.70, "Vigilance", "Résultat cyclique ({v} % d'écart)"),
                (None, "Risque", "Résultat très irrégulier ({v} % d'écart)")]),
            "marge_ebitda": ("haut", [
                (0.20, "OK", "Bonne marge de campagne ({v} %)"),
                (0.10, "Vigilance", "Marge moyenne ({v} %)"),
                (None, "Risque", "Marge faible ({v} %)")]),
            "intensite_capex": ("bas", [
                (0.12, "OK", "Entretien contenu ({v} % du CA)"),
                (0.20, "Vigilance", "Renouvellement lourd ({v} % du CA)"),
                (None, "Risque", "Investissement très lourd ({v} % du CA)")]),
            "rotation_actif": ("haut", [
                (0.7, "OK", "Actif productif ({v} x)"),
                (0.4, "Vigilance", "Actif peu productif ({v} x)"),
                (None, "Risque", "Actif très peu productif ({v} x)")]),
        },
    },
    "services publics": {
        "libelle": "Grille services publics",
        "lecture": (
            "Un service public porte un réseau : l'actif est énorme, la "
            "rotation faible et l'investissement permanent — c'est normal et "
            "ce n'est pas un defaut. Ce qui compte est la régularité de la "
            "marge et la capacité a servir la dette que le réseau impose."
        ),
        "grille": [
            ("Marge d'EBITDA", "marge_ebitda", "pct",
             "Rentabilité avant amortissement du réseau"),
            ("Marge d'exploitation", "marge_exploitation", "pct",
             "Performance après amortissement du réseau"),
            ("Intensité capitalistique", "intensite_capex", "pct",
             "Investissement permanent qu'exige le réseau"),
            ("Dette / EBITDA", "dette_ebitda", "fois",
             "Années d'EBITDA pour rembourser la dette du réseau"),
            ("Couverture des intérêts", "interest_coverage", "fois",
             "Capacité à servir la dette"),
            ("Rotation de l'actif", "rotation_actif", "fois",
             "Faible par construction : l'actif est le réseau"),
        ],
        "seuils": {
            "marge_ebitda": ("haut", [
                (0.25, "OK", "Marge solide ({v} %)"),
                (0.15, "Vigilance", "Marge moyenne ({v} %)"),
                (None, "Risque", "Marge faible pour un réseau ({v} %)")]),
            "intensite_capex": ("bas", [
                (0.20, "OK", "Investissement normal pour un réseau ({v} % du CA)"),
                (0.30, "Vigilance", "Cycle d'investissement lourd ({v} % du CA)"),
                (None, "Risque", "Investissement très lourd ({v} % du CA)")]),
            "dette_ebitda": ("bas", [
                (3.5, "OK", "Levier tenable pour un réseau ({v} x)"),
                (5.0, "Vigilance", "Levier élevé ({v} x)"),
                (None, "Risque", "Levier très élevé ({v} x)")]),
            "rotation_actif": ("haut", [
                (0.5, "OK", "Réseau bien employé ({v} x)"),
                (0.3, "Vigilance", "Réseau sous-employé ({v} x)"),
                (None, "Risque", "Actif peu productif ({v} x)")]),
        },
    },
    "transport": {
        "libelle": "Grille transport",
        "lecture": (
            "Le transport combine un actif lourd et une demande cyclique. On "
            "y regarde donc ce que la flotte ou l'infrastructure fait tourner "
            "de ventes, et si la marge resiste au bas de cycle."
        ),
        "grille": [
            ("Marge d'exploitation", "marge_exploitation", "pct",
             "Performance du métier sur l'exercice"),
            ("Rotation de l'actif", "rotation_actif", "fois",
             "Ce que l'actif immobilisé engendre en ventes"),
            ("Marge d'EBITDA", "marge_ebitda", "pct",
             "Rentabilité avant amortissement de la flotte"),
            ("Intensité capitalistique", "intensite_capex", "pct",
             "Renouvellement de la flotte ou de l'infrastructure"),
            ("Dette / EBITDA", "dette_ebitda", "fois",
             "Endettement rapporté a l'EBITDA"),
            ("Volatilité du résultat", "volatilite_resultat", "pct",
             "Sensibilité au cycle sur quatre exercices"),
        ],
        "seuils": {
            "marge_exploitation": ("haut", [
                (0.08, "OK", "Marge solide ({v} %)"),
                (0.04, "Vigilance", "Marge étroite ({v} %)"),
                (None, "Risque", "Exploitation peu rentable ({v} %)")]),
            "rotation_actif": ("haut", [
                (1.2, "OK", "Actif bien employé ({v} x)"),
                (0.8, "Vigilance", "Actif sous-employé ({v} x)"),
                (None, "Risque", "Actif peu productif ({v} x)")]),
            "dette_ebitda": ("bas", [
                (3.0, "OK", "Levier confortable ({v} x)"),
                (4.5, "Vigilance", "Levier a surveiller ({v} x)"),
                (None, "Risque", "Levier élevé ({v} x)")]),
        },
    },
}

# Les societes classees « Autres » n'ont pas de metier commun : on leur
# applique la lecture generaliste plutot qu'une grille de circonstance.
_GENERALISTE = {
    "libelle": "Grille générale",
    "lecture": (
        "Faute de métier commun a ce classement, la lecture reste "
        "généraliste : marge, rotation de l'actif et couverture de la dette."
    ),
    "grille": [
        ("Marge d'exploitation", "marge_exploitation", "pct",
         "Performance du métier, avant financement et impôt"),
        ("Marge nette", "net_margin", "pct",
         "Ce qui reste au bout"),
        ("Rotation de l'actif", "rotation_actif", "fois",
         "Chiffre d'affaires engendré par franc d'actif"),
        ("Rendement des actifs", "roa", "pct",
         "Ce que rapporte l'actif employé"),
        ("Couverture des intérêts", "interest_coverage", "fois",
         "Combien de fois le résultat couvre les intérêts"),
    ],
    "seuils": {},
}


def _cle_secteur(secteur: Optional[str]) -> Optional[str]:
    """Ramene un libelle de secteur a la cle du referentiel.

    Tolere les accents, la casse et les variantes rencontrees en base
    (« Telecommunications », « Télécoms », « Banque »).
    """
    if not secteur:
        return None
    bas = str(secteur).strip().lower()
    bas = (bas.replace("é", "e").replace("è", "e").replace("ê", "e")
              .replace("à", "a").replace("ô", "o").replace("û", "u"))
    if "banque" in bas or "bank" in bas:
        return "banque"
    if "telecom" in bas:
        return "telecommunications"
    if "industr" in bas:
        return "industrie"
    if "distribution" in bas or "commerce" in bas:
        return "distribution"
    if "agricult" in bas:
        return "agriculture"
    if "service" in bas and "public" in bas:
        return "services publics"
    if "transport" in bas:
        return "transport"
    return None


def profil_secteur(secteur: Optional[str]) -> dict:
    """Retourne la grille et les seuils du secteur, ou la lecture généraliste."""
    cle = _cle_secteur(secteur)
    return _SECTEURS.get(cle, _GENERALISTE)


def seuils_secteur(secteur: Optional[str]) -> dict:
    return profil_secteur(secteur).get("seuils", {})


def juger(ratio: str, valeur, secteur: Optional[str]):
    """Juge une valeur selon les bornes du secteur.

    Retourne (niveau, commentaire) ou None si le secteur ne se prononce pas —
    charge alors a l'appelant de garder son jugement generique.
    """
    if valeur is None:
        return None
    regle = seuils_secteur(secteur).get(ratio)
    if not regle:
        return None
    sens, bornes = regle
    for borne, niveau, gabarit in bornes:
        atteint = (
            borne is None
            or (sens == "haut" and valeur >= borne)
            or (sens == "bas" and valeur <= borne)
        )
        if atteint:
            # Le gabarit porte l'unite : « {v} x » pour un multiple,
            # « {v} % » pour un pourcentage. On ne devine pas au mot pres.
            multiple = "{v} x" in gabarit
            affiche = f"{valeur:.2f}" if multiple else f"{valeur*100:.0f}"
            return niveau, gabarit.format(v=affiche)
    return None


def secteurs_connus() -> list:
    """Liste des secteurs disposant d'une grille dediee."""
    return sorted(_SECTEURS.keys())


# Indicateurs comparables d'un secteur a l'autre. Volontairement restreints
# aux mesures qui gardent le meme sens partout : une marge d'EBITDA ou une
# intensite capitalistique ne se comparent pas entre une banque et un
# distributeur, un rendement du dividende si.
# En deca de ce nombre d'observations, on n'affiche pas de mediane
# sectorielle : elle refleterait une societe, pas un secteur.
MIN_OBSERVATIONS = 3

COMPARABLES_INTERSECTEURS = [
    ("Marge nette", "net_margin", "pct"),   # banques : rapportee au PNB
    ("Rendement des fonds propres", "roe", "pct"),
    ("Rendement des actifs", "roa", "pct"),
    ("PER", "per", "fois"),
    ("Cours / actif net", "pb", "fois"),
    ("Rendement du dividende", "dividend_yield", "pct"),
    ("Taux de distribution", "payout_ratio", "pct"),
    ("Croissance du chiffre d'affaires", "revenue_growth", "pct"),
]


def medianes_intersecteurs() -> list:
    """Mediane de chaque indicateur universel, secteur par secteur.

    Le propos est de rendre les secteurs comparables ENTRE EUX, ce que les
    grilles ne permettent pas : on ne compare pas un coefficient
    d'exploitation bancaire a une intensite capitalistique telecoms. Ne
    figurent donc ici que les mesures qui gardent le meme sens partout —
    rentabilite, valorisation, rendement.

    Retourne une liste de dictionnaires, un par secteur, triee par effectif.
    """
    from data.storage import get_all_stocks_for_analysis
    from analysis.fundamental import _actif_fiable

    try:
        df = get_all_stocks_for_analysis()
    except Exception:
        return []
    if df is None or df.empty:
        return []

    par_secteur = {}
    for _, ligne in df.iterrows():
        d = ligne.to_dict()
        secteur = (d.get("sector") or "").strip()
        if not secteur:
            continue

        prix = d.get("price") or 0
        titres = d.get("shares") or 0
        ca = d.get("revenue") or 0
        rn = d.get("net_income") or 0
        fp = d.get("equity") or 0
        actif = d.get("total_assets") or 0
        dividende = d.get("dps") or 0

        # Meme garde-fou que dans la fiche : un total de bilan incoherent
        # fabriquerait un rendement des actifs spectaculaire et faux.
        est_banque = "banque" in secteur.lower() or "bank" in secteur.lower()
        if not _actif_fiable(actif, fp, ca, est_banque):
            actif = 0

        bnpa = (rn / titres) if titres and rn else None
        anpa = (fp / titres) if titres and fp else None
        mesures = {
            "net_margin": (rn / ca) if ca and rn else None,
            "roe": (rn / fp) if fp and rn else None,
            "roa": (rn / actif) if actif and rn else None,
            "per": (prix / bnpa) if bnpa and bnpa > 0 and prix else None,
            "pb": (prix / anpa) if anpa and anpa > 0 and prix else None,
            "dividend_yield": (dividende / prix) if prix and dividende else None,
            "payout_ratio": (dividende / bnpa) if bnpa and bnpa > 0 and dividende else None,
        }
        # Croissance : sur les deux derniers exercices renseignes.
        ca_n0, ca_n1 = d.get("revenue_n0"), d.get("revenue_n1")
        if ca_n0 and ca_n1:
            mesures["revenue_growth"] = (ca_n0 - ca_n1) / abs(ca_n1)
        else:
            mesures["revenue_growth"] = None

        # Bornes de vraisemblance : une valeur aberrante deplacerait la
        # mediane d'un secteur de deux ou trois societes.
        plafonds = {"per": 100, "pb": 20, "roe": 2, "roa": 1,
                    "net_margin": 1, "dividend_yield": 0.5,
                    "payout_ratio": 3, "revenue_growth": 5}
        for cle in list(mesures):
            valeur = mesures.get(cle)
            plafond = plafonds.get(cle)
            if valeur is not None and valeur != valeur:      # NaN
                mesures[cle] = None
            elif (valeur is not None and plafond is not None
                    and abs(valeur) > plafond):
                mesures[cle] = None

        par_secteur.setdefault(secteur, []).append(mesures)

    def _fini(v):
        """Ecarte None et les NaN que pandas laisse passer (NaN n'est pas None
        et se compare a tout sans jamais lever : il faut le tester en propre)."""
        return v is not None and v == v

    def _mediane(valeurs):
        valeurs = sorted(v for v in valeurs if _fini(v))
        if not valeurs:
            return None
        milieu = len(valeurs) // 2
        if len(valeurs) % 2:
            return valeurs[milieu]
        return (valeurs[milieu - 1] + valeurs[milieu]) / 2

    sortie = []
    for secteur, lignes in par_secteur.items():
        entree = {"secteur": secteur, "effectif": len(lignes)}
        for _, cle, _ in COMPARABLES_INTERSECTEURS:
            valeurs = [l.get(cle) for l in lignes if _fini(l.get(cle))]
            entree[f"{cle}__n"] = len(valeurs)
            # Une « mediane sectorielle » calculee sur une ou deux societes
            # n'en est pas une : mieux vaut ne rien afficher que publier un
            # repere que le lecteur croira solide.
            entree[cle] = _mediane(valeurs) if len(valeurs) >= MIN_OBSERVATIONS else None
        sortie.append(entree)

    return sorted(sortie, key=lambda e: -e["effectif"])
