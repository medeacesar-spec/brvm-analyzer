"""
Suivi de l'acquisition des donnees, societe par societe.

Repond a trois questions concretes :
  - ou en est-on sur l'ensemble de la cote ?
  - que manque-t-il a une societe donnee ?
  - quels documents restent a lire pour combler ces trous ?

Le principe retenu : on ne compte comme acquise qu'une donnee UTILISABLE. Un
total de bilan present mais incoherent — Bank of Africa Benin portait 21,8 M
FCFA d'actif pour 112,8 Mds de fonds propres — compte comme manquant, parce
que c'est ce qu'il est en pratique : il ne produit qu'un ratio faux.
"""

from typing import Optional

# Le socle commun : ce que toute analyse exige, quel que soit le metier.
SOCLE = [
    ("Cours", "price", "Marché"),
    ("Nombre de titres", "shares", "Marché"),
    ("Chiffre d'affaires", "revenue", "Résultat"),
    ("Résultat net", "net_income", "Résultat"),
    ("Capitaux propres", "equity", "Bilan"),
    ("Total du bilan", "total_assets", "Bilan"),
    ("Dividende par action", "dps", "Dividende"),
]

# Ce que chaque metier exige EN PLUS pour que sa grille se remplisse.
SECTORIEL = {
    "banque": [
        ("Frais généraux", "operating_expenses"),
        ("Résultat brut d'exploitation", "gross_operating_income"),
        ("Coût du risque", "cost_of_risk"),
        ("Dépôts clientèle", "deposits"),
        ("Crédits à la clientèle", "loans"),
    ],
    "telecommunications": [
        ("EBITDA", "ebitda"),
        ("Investissements", "capex"),
        ("Dette financière", "total_debt"),
    ],
    "industrie": [
        ("Résultat d'exploitation", "ebit"),
        ("EBITDA", "ebitda"),
        ("Investissements", "capex"),
        ("Charges d'intérêts", "interest_expense"),
        ("Dette financière", "total_debt"),
    ],
    "distribution": [
        ("Résultat d'exploitation", "ebit"),
        ("Charges d'intérêts", "interest_expense"),
        ("Dette financière", "total_debt"),
    ],
    "agriculture": [
        ("Résultat d'exploitation", "ebit"),
        ("EBITDA", "ebitda"),
        ("Investissements", "capex"),
        ("Dette financière", "total_debt"),
    ],
    "services publics": [
        ("Résultat d'exploitation", "ebit"),
        ("EBITDA", "ebitda"),
        ("Investissements", "capex"),
        ("Charges d'intérêts", "interest_expense"),
        ("Dette financière", "total_debt"),
    ],
    "transport": [
        ("Résultat d'exploitation", "ebit"),
        ("EBITDA", "ebitda"),
        ("Investissements", "capex"),
        ("Dette financière", "total_debt"),
    ],
}

TYPES_ANNUELS = ("rapport_annuel", "etats_financiers")


def _present(valeur) -> bool:
    return valeur is not None and valeur != 0 and valeur == valeur


def attendus(secteur: Optional[str]) -> list:
    """Liste (libelle, champ, groupe) attendue pour ce secteur."""
    from analysis.sectors import _cle_secteur
    cle = _cle_secteur(secteur)
    sectoriels = [(lib, champ, "Métier") for lib, champ in SECTORIEL.get(cle, [])]
    return SOCLE + sectoriels


def etat_societe(fundamentals: dict) -> dict:
    """Detaille ce qui est acquis et ce qui manque pour une societe."""
    from analysis.fundamental import _actif_fiable

    secteur = fundamentals.get("sector") or ""
    est_banque = "banque" in secteur.lower() or "bank" in secteur.lower()
    champs = attendus(secteur)

    acquis, manquants, douteux = [], [], []
    for libelle, champ, groupe in champs:
        valeur = fundamentals.get(champ)
        if champ == "total_assets" and _present(valeur):
            # Un total de bilan incoherent ne vaut pas mieux que rien : il ne
            # produit qu'un ratio faux, et se signale a part pour qu'on sache
            # qu'il y a une valeur A CORRIGER, pas une valeur A COLLECTER.
            if not _actif_fiable(valeur, fundamentals.get("equity"),
                                 fundamentals.get("revenue"), est_banque):
                douteux.append((libelle, groupe))
                continue
        (acquis if _present(valeur) else manquants).append((libelle, groupe))

    total = len(champs)
    return {
        "ticker": fundamentals.get("ticker"),
        "nom": fundamentals.get("company_name") or fundamentals.get("ticker"),
        "secteur": secteur or "—",
        "exercice": fundamentals.get("fiscal_year"),
        "acquis": acquis,
        "manquants": manquants,
        "douteux": douteux,
        "total": total,
        "part": len(acquis) / total if total else 0.0,
    }


def _dossiers_consolides() -> dict:
    """Reconstitue, en DEUX requetes, le dossier de chaque societe.

    Interroger `get_fundamentals` ticker par ticker demandait pres de deux
    cents allers-retours vers la base : l'ecran mettait plusieurs minutes a
    s'afficher. On charge tout d'un coup et on consolide en memoire.

    La regle de consolidation reprend celle de la fiche : on part du dernier
    exercice ayant un chiffre d'affaires et un resultat net, puis on complete
    les postes absents avec l'exercice le plus recent qui les porte. Un poste
    repris d'un autre exercice reste une donnee acquise — elle existe et se
    lit — meme si la fiche affiche une autre annee.
    """
    from data.db import get_connection

    conn = get_connection()
    marche = {}
    for ligne in conn.execute(
        "SELECT ticker, company_name, sector, price, shares, dps "
        "FROM market_data"
    ).fetchall():
        d = dict(ligne)
        marche[d["ticker"]] = d

    exercices = {}
    for ligne in conn.execute(
        "SELECT * FROM fundamentals ORDER BY ticker, fiscal_year DESC"
    ).fetchall():
        d = dict(ligne)
        exercices.setdefault(d["ticker"], []).append(d)
    conn.close()

    dossiers = {}
    for ticker, marche_ligne in marche.items():
        annees = exercices.get(ticker) or []
        base = None
        for d in annees:
            if _present(d.get("revenue")) and _present(d.get("net_income")):
                base = dict(d)
                break
        if base is None:
            base = dict(annees[0]) if annees else {"ticker": ticker}

        for d in annees:
            for champ, valeur in d.items():
                if champ in ("id", "ticker", "fiscal_year"):
                    continue
                if not _present(base.get(champ)) and _present(valeur):
                    base[champ] = valeur

        # Le cours, le nombre de titres et le dividende viennent du marche
        # quand la fiche fondamentale ne les porte pas.
        for champ in ("price", "shares", "dps"):
            if not _present(base.get(champ)):
                base[champ] = marche_ligne.get(champ)
        base.setdefault("company_name", marche_ligne.get("company_name"))
        if not (base.get("sector") or "").strip():
            base["sector"] = marche_ligne.get("sector") or ""
        base["ticker"] = ticker
        dossiers[ticker] = base

    return dossiers


def etat_cote() -> list:
    """Etat de l'acquisition pour toutes les societes, les moins bien loties
    en tete : c'est la qu'il y a du travail."""
    dossiers = _dossiers_consolides()
    lignes = [etat_societe(d) for d in dossiers.values()]
    return sorted(lignes, key=lambda e: (e["part"], e["ticker"]))


def reste_a_lire(limite_annee: int = 2023) -> list:
    """Documents annuels references dont les donnees ne sont pas en base.

    On ne se fie pas au journal des tentatives, incomplet : on compare ce que
    le rapport DEVRAIT renseigner a ce que porte reellement l'exercice
    correspondant. Un exercice complet n'a plus a etre lu.

    Tout est charge en trois requetes : recenser les trous ne doit pas couter
    plus cher que de les combler.
    """
    from data.db import get_connection

    conn = get_connection()
    secteurs = {}
    for ligne in conn.execute(
        "SELECT ticker, sector FROM market_data "
        "WHERE sector IS NOT NULL AND sector <> ''"
    ).fetchall():
        d = dict(ligne)
        secteurs[d["ticker"]] = d["sector"]

    exercices = {}
    for ligne in conn.execute("SELECT * FROM fundamentals").fetchall():
        d = dict(ligne)
        exercices[(d["ticker"], d["fiscal_year"])] = d

    marques = ",".join("?" * len(TYPES_ANNUELS))
    rapports = [dict(l) for l in conn.execute(
        f"""SELECT ticker, fiscal_year, report_type, title, url
            FROM report_links
            WHERE fiscal_year >= ? AND report_type IN ({marques})
            ORDER BY ticker, fiscal_year DESC""",
        (limite_annee, *TYPES_ANNUELS)).fetchall()]
    conn.close()

    sortie, vus = [], set()
    for r in rapports:
        cle = (r["ticker"], r["fiscal_year"])
        if cle in vus:
            continue
        vus.add(cle)

        exercice = exercices.get(cle) or {}
        secteur = (exercice.get("sector") or secteurs.get(r["ticker"]) or "")
        absents = [lib for lib, champ, _ in attendus(secteur)
                   if not _present(exercice.get(champ))]
        # Le cours et le nombre de titres ne viennent pas d'un rapport annuel :
        # les compter ici ferait porter au document un trou qu'il ne peut pas
        # combler.
        absents = [a for a in absents if a not in ("Cours", "Nombre de titres")]
        if not absents:
            continue
        sortie.append({
            "ticker": r["ticker"],
            "exercice": r["fiscal_year"],
            "type": r["report_type"],
            "titre": r["title"],
            "url": r["url"],
            "manque": absents,
            "nb_manque": len(absents),
        })
    return sorted(sortie, key=lambda e: (-e["nb_manque"], e["ticker"]))


def anomalies_ouvertes() -> list:
    """Anomalies relevees par le controle de vraisemblance, non encore reparees.

    Chacune est accompagnee des documents publies pour ce titre et cet
    exercice : c'est ce qui permet de corriger de facon CIBLEE plutot que de
    relancer un rattrapage complet en esperant que le probleme se resolve.
    """
    from data.db import get_connection

    conn = get_connection()
    try:
        lignes = [dict(l) for l in conn.execute(
            """SELECT ticker, fiscal_year, champs, motif, detectee_le
               FROM controle_anomalies WHERE resolue_le IS NULL
               ORDER BY ticker, fiscal_year DESC"""
        ).fetchall()]
    except Exception:
        conn.close()
        return []

    documents = {}
    try:
        for ligne in conn.execute(
            """SELECT ticker, fiscal_year, report_type, url FROM report_links
               WHERE url IS NOT NULL"""
        ).fetchall():
            d = dict(ligne)
            documents.setdefault((d["ticker"], d["fiscal_year"]), []).append(d)
    except Exception:
        pass
    conn.close()

    for ligne in lignes:
        pieces = documents.get((ligne["ticker"], ligne["fiscal_year"]), [])
        # Les documents annuels d'abord : ce sont eux qui portent les postes
        # que le controle a annules.
        pieces.sort(key=lambda p: 0 if p["report_type"] in TYPES_ANNUELS else 1)
        ligne["documents"] = pieces[:3]
    return lignes
