"""
Lecture des publications de periode : trimestres et semestres.

Ces publications ne font pas reference — l'exercice annuel seul le fait, le
cycle de publication UEMOA voulant qu'un exercice N ne paraisse qu'au
printemps N+1. Mais elles disent la TENDANCE, ce qu'un exercice clos ne
montre qu'un an plus tard.

Une regle gouverne tout ce module : **on ne compare jamais deux periodes de
duree differente**. Un premier semestre se compare a un premier semestre, un
premier trimestre a un premier trimestre. Rapprocher un semestre d'un
trimestre reviendrait a annoncer un doublement qui n'existe pas.
"""

from typing import Optional

# Ordre de lecture : du plus court au plus long, puis par rang.
_ORDRE = {"T1": 1, "T2": 2, "T3": 3, "T4": 4, "S1": 5, "S2": 6}

LIBELLES = {
    "T1": "1er trimestre", "T2": "2e trimestre",
    "T3": "3e trimestre", "T4": "4e trimestre",
    "S1": "1er semestre", "S2": "2e semestre",
}


def _variation(actuel, precedent) -> Optional[float]:
    if actuel is None or precedent in (None, 0):
        return None
    return (actuel - precedent) / abs(precedent) * 100


def tendance_periodes(ticker: str, profondeur: int = 3) -> dict:
    """Compare chaque periode publiee a la MEME periode de l'exercice anterieur.

    Retourne {"exercice", "lignes"} ou chaque ligne porte le libelle de la
    periode, les montants des deux exercices et leurs variations.
    """
    from data.db import get_connection

    conn = get_connection()
    try:
        lignes = [dict(l) for l in conn.execute(
            """SELECT fiscal_year, quarter, periode, revenue, net_income, ebit
               FROM quarterly_data WHERE ticker = ?
               ORDER BY fiscal_year DESC""",
            (ticker,),
        ).fetchall()]
    except Exception:
        conn.close()
        return {"exercice": None, "lignes": []}
    conn.close()

    par_cle = {}
    for ligne in lignes:
        periode = (ligne.get("periode") or "").upper()
        if not periode:
            rang = ligne.get("quarter")
            periode = f"T{rang}" if rang in (1, 2, 3, 4) else ""
        if periode not in _ORDRE:
            continue
        par_cle[(ligne.get("fiscal_year"), periode)] = ligne

    if not par_cle:
        return {"exercice": None, "lignes": []}

    dernier = max(annee for annee, _ in par_cle if annee)
    sortie = []
    for (annee, periode), ligne in par_cle.items():
        if annee != dernier:
            continue
        # Meme periode, exercice precedent : la seule comparaison licite.
        anterieure = par_cle.get((annee - 1, periode))
        sortie.append({
            "periode": periode,
            "libelle": LIBELLES.get(periode, periode),
            "annee": annee,
            "annee_ref": (annee - 1) if anterieure else None,
            "revenue": ligne.get("revenue"),
            "revenue_ref": (anterieure or {}).get("revenue"),
            "revenue_var": _variation(ligne.get("revenue"),
                                      (anterieure or {}).get("revenue")),
            "net_income": ligne.get("net_income"),
            "net_income_ref": (anterieure or {}).get("net_income"),
            "net_income_var": _variation(ligne.get("net_income"),
                                         (anterieure or {}).get("net_income")),
        })

    sortie.sort(key=lambda e: _ORDRE.get(e["periode"], 99))
    return {"exercice": dernier, "lignes": sortie}


def historique_periode(ticker: str, periode: str, profondeur: int = 4) -> list:
    """Serie d'une meme periode sur plusieurs exercices, du plus ancien au plus
    recent. Sert a lire une pente plutot qu'un seul ecart."""
    from data.db import get_connection

    conn = get_connection()
    try:
        lignes = [dict(l) for l in conn.execute(
            """SELECT fiscal_year, periode, quarter, revenue, net_income
               FROM quarterly_data WHERE ticker = ?
               ORDER BY fiscal_year DESC""",
            (ticker,),
        ).fetchall()]
    except Exception:
        conn.close()
        return []
    conn.close()

    serie = []
    for ligne in lignes:
        p = (ligne.get("periode") or "").upper()
        if not p and ligne.get("quarter") in (1, 2, 3, 4):
            p = f"T{ligne['quarter']}"
        if p == periode.upper():
            serie.append(ligne)
    serie.sort(key=lambda e: e.get("fiscal_year") or 0)
    return serie[-profondeur:]
