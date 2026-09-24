"""Les indices de la BRVM : le registre, et lui seul.

POURQUOI CE FICHIER EXISTE

Les indices vivent dans les MEMES tables que les titres — `price_cache` et
`price_monthly` — parce qu'ils se lisent de la meme facon : une date, une
cloture. Le prix de ce choix est qu'un indice ressemble a un titre pour
toute requete qui ne fait pas attention. Le Composite s'est deja invite dans
un classement de titres, et il y arrive premier : un indice est une moyenne,
il est moins volatil que ses composants et bat mecaniquement la plupart
d'entre eux au rapport rendement/risque.

Tant qu'il n'y avait que deux indices en base, six modules ecrivaient
`("BRVMC", "BRVM30")` chacun dans son coin. En passer a dix-sept aurait
demande de retrouver ces six endroits sans en oublier un, et le seul moyen
de le savoir aurait ete de voir « BRVM - FINANCES » apparaitre en tete du
screening. D'ou ce registre unique : on ajoute un indice ICI, et les six
appelants suivent.

DEUX CLASSIFICATIONS SECTORIELLES, QU'ON NE RECOLLE PAS

La BRVM a change de nomenclature sectorielle : les sept indices historiques
(base 100 au 16 juin 1999) s'arretent au 31 decembre 2025, les six nouveaux
demarrent au 2 janvier 2025. Ils se CHEVAUCHENT sur un an, et ils ne
mesurent pas les memes paniers — « Industrie » et « Industriels » ne
contiennent pas les memes societes.

Les coller bout a bout fabriquerait une serie de vingt-sept ans qui n'a
jamais existe, avec une rupture invisible au milieu. Les deux series
restent donc separees, chacune avec ses bornes, et l'affichage montre la
nouvelle A COTE de l'ancienne plutot qu'a sa place.
CE QUI SE MET A JOUR, ET CE QUI NE SE MET PAS

`collecter_historique_cours.py` rafraichit les indices via sikafinance, avec
les symboles de CE fournisseur — « BRVMC » et « BRVM30 ». Les quinze autres
series viennent d'un export RichBourse et sont donc FIGEES a leur date
d'import. Ce n'est pas genant pour les sept indices sectoriels historiques,
que la BRVM ne publie plus depuis le 31 decembre 2025 ; ce l'est pour les
huit encore vivants, arretes au 9 septembre 2026.

Les rendre courants demande de connaitre le symbole sikafinance de chacun.
Le deviner reviendrait a interroger un service tiers a l'aveugle : tant que
ces symboles ne sont pas etablis, `fin_export` dit jusqu'ou la serie va, et
mieux vaut une borne affichee qu'une serie qu'on croit a jour.
"""
from __future__ import annotations

from datetime import date

# --- Le registre -----------------------------------------------------------
#
# `fichier` renvoie a l'export RichBourse, sous `csv/`. `debut` et `fin`
# bornent la validite de l'indice : `fin` a None veut dire « toujours
# publie ». `famille` sert a l'affichage et au filtrage.
INDICES = {
    # Indices generaux. BRVMC et BRVM30 sont anterieurs a ce registre et
    # gardent leurs codes historiques : les renommer casserait les series
    # deja en base et les six modules qui les nomment.
    "BRVMC": {"libelle": "BRVM Composite", "famille": "general",
              "fichier": "brvm-composite.csv",
              "debut": date(1998, 9, 16), "fin": None},
    "BRVM30": {"libelle": "BRVM 30", "famille": "general",
               "fichier": "brvm-30.csv",
               "debut": date(2023, 1, 2), "fin": None},
    "IDX_PRESTIGE": {"libelle": "BRVM Prestige", "famille": "general",
                     "fichier": "brvm-prestige.csv",
                     "debut": date(2023, 1, 2), "fin": None},
    "IDX_PRINCIPAL": {"libelle": "BRVM Principal", "famille": "general",
                      "fichier": "brvm-principal.csv",
                      "debut": date(2023, 1, 2), "fin": None},

    # Nomenclature sectorielle HISTORIQUE, base 100 au 16 juin 1999,
    # publiee jusqu'au 31 decembre 2025.
    "IDX_AGRICULTURE": {"libelle": "Agriculture (ancienne)",
                        "famille": "sectoriel_ancien",
                        "fichier": "brvm-agriculture.csv",
                        "debut": date(1999, 6, 16), "fin": date(2025, 12, 31)},
    "IDX_AUTRES": {"libelle": "Autres secteurs (ancienne)",
                   "famille": "sectoriel_ancien",
                   "fichier": "brvm-autres secteurs.csv",
                   "debut": date(1999, 6, 16), "fin": date(2025, 12, 31)},
    "IDX_DISTRIBUTION": {"libelle": "Distribution (ancienne)",
                         "famille": "sectoriel_ancien",
                         "fichier": "brvm-distribution.csv",
                         "debut": date(1999, 6, 16), "fin": date(2025, 12, 31)},
    "IDX_FINANCES": {"libelle": "Finances (ancienne)",
                     "famille": "sectoriel_ancien",
                     "fichier": "brvm-finances.csv",
                     "debut": date(1999, 6, 16), "fin": date(2025, 12, 31)},
    "IDX_INDUSTRIE": {"libelle": "Industrie (ancienne)",
                      "famille": "sectoriel_ancien",
                      "fichier": "brvm-industrie.csv",
                      "debut": date(1999, 6, 16), "fin": date(2025, 12, 31)},
    "IDX_SERVICES_PUBLICS": {"libelle": "Services publics (ancienne)",
                             "famille": "sectoriel_ancien",
                             "fichier": "brvm-services publics.csv",
                             "debut": date(1999, 6, 16), "fin": date(2025, 12, 31)},
    "IDX_TRANSPORT": {"libelle": "Transport (ancienne)",
                      "famille": "sectoriel_ancien",
                      "fichier": "brvm-transport.csv",
                      "debut": date(1999, 6, 16), "fin": date(2025, 12, 31)},

    # Nouvelle nomenclature, depuis le 2 janvier 2025.
    "IDX_CONSO_BASE": {"libelle": "Consommation de base",
                       "famille": "sectoriel_nouveau",
                       "fichier": "brvm-consommation de base.csv",
                       "debut": date(2025, 1, 2), "fin": None},
    "IDX_CONSO_DISCRETIONNAIRE": {"libelle": "Consommation discrétionnaire",
                                  "famille": "sectoriel_nouveau",
                                  "fichier": "brvm-consommation discretionnaire.csv",
                                  "debut": date(2025, 1, 2), "fin": None},
    "IDX_ENERGIE": {"libelle": "Énergie", "famille": "sectoriel_nouveau",
                    "fichier": "brvm-energie.csv",
                    "debut": date(2025, 1, 2), "fin": None},
    "IDX_INDUSTRIELS": {"libelle": "Industriels", "famille": "sectoriel_nouveau",
                        "fichier": "brvm-industriels.csv",
                        "debut": date(2025, 1, 2), "fin": None},
    "IDX_SERVICES_PUBLICS_N": {"libelle": "Services publics",
                               "famille": "sectoriel_nouveau",
                               "fichier": "brvm-services publics nouveaux.csv",
                               "debut": date(2025, 1, 2), "fin": None},
    "IDX_TELECOM": {"libelle": "Télécommunications",
                    "famille": "sectoriel_nouveau",
                    "fichier": "brvm-telecommunications.csv",
                    "debut": date(2025, 1, 2), "fin": None},
}

# Le marche de reference pour le beta et la correlation. Le Composite, et pas
# le BRVM 30 : celui-ci ne remonte qu'a 2023 et ne couvre que trente valeurs.
INDICE_MARCHE = "BRVMC"

# Derniere seance couverte par l'export RichBourse. Au-dela, seuls BRVMC et
# BRVM30 continuent d'avancer, par sikafinance.
FIN_EXPORT = date(2026, 9, 9)

# Les deux seuls indices qu'un collecteur rafraichit aujourd'hui.
INDICES_A_JOUR = frozenset({"BRVMC", "BRVM30"})


def est_a_jour(ticker: str) -> bool:
    """Vrai si cet indice continue d'etre alimente apres FIN_EXPORT."""
    return ticker in INDICES_A_JOUR

# Le jeu de codes, pour filtrer. C'est ce que les appelants importent.
CODES = frozenset(INDICES)


def est_indice(ticker: str) -> bool:
    """Vrai si ce code designe un indice et non une societe cotee."""
    return ticker in CODES


def sans_indices(tickers) -> list:
    """La meme collection, indices retires. Accepte tout iterable."""
    return [t for t in tickers if t not in CODES]


def libelle(ticker: str) -> str:
    """Le nom lisible d'un indice, ou le code s'il n'en a pas."""
    return (INDICES.get(ticker) or {}).get("libelle", ticker)


def par_famille(famille: str) -> list:
    """Les codes d'une famille, dans l'ordre du registre."""
    return [c for c, m in INDICES.items() if m["famille"] == famille]


def fichiers() -> dict:
    """{nom de fichier CSV: code} — ce que l'importateur consomme."""
    return {m["fichier"]: c for c, m in INDICES.items()}
