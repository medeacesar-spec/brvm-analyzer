"""Dividendes a venir : ce que le portefeuille va encaisser, et quand.

L'information est publiee : le Bulletin Officiel de la Cote donne, pour
chaque dividende annonce, le montant BRUT par action, la date de mise en
paiement et les deux taux d'IRVM applicables. Le portefeuille donne les
quantites. Le net attendu se calcule — il n'a jamais besoin d'etre saisi.

Regle du projet : on recalcule, on ne recopie pas. Le brut par action vient
du bulletin, la quantite du portefeuille, et le net est le produit des deux
diminue de la retenue. Aucun de ces trois nombres n'est stocke tel quel.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from data.db import read_sql_df

# L'IRVM est publiee par le bulletin lui-meme, colonne par colonne. On ne la
# code donc pas en dur : ces cles ne servent qu'a choisir LA colonne.
PROFILS = {
    "physique": ("irvm_physique", "personne physique"),
    "morale": ("irvm_morale", "personne morale"),
}


def annonces_de_dividende() -> pd.DataFrame:
    """Les dividendes annonces, dedoublonnes.

    `boc_operations` rejoue la meme operation d'un bulletin a l'autre : au
    09/09/2026, Nestle CI y figurait six fois et NEI-CEDA huit, a montant et
    date identiques. Sommer sans dedoublonner multiplierait le dividende
    attendu par le nombre de bulletins qui l'ont annonce.
    """
    try:
        d = read_sql_df(
            """SELECT DISTINCT ticker, emetteur, brut, irvm_physique,
                      irvm_morale, date_operation
               FROM boc_operations
               WHERE type = 'dividende' AND brut IS NOT NULL
               ORDER BY date_operation""")
    except Exception:
        return pd.DataFrame()
    return d if d is not None else pd.DataFrame()


def dividendes_attendus(portefeuille: pd.DataFrame, profil: str = "physique",
                        a_partir_de: date | None = None) -> dict:
    """Ce que le portefeuille va encaisser sur les annonces en cours.

    Retourne un dict :
      lignes        : une par position concernee, deja calculee
      total_brut / total_retenue / total_net
      orphelines    : annonces sans ticker, donc non rattachables
      profil        : le libelle du taux applique
    """
    colonne, libelle = PROFILS.get(profil, PROFILS["physique"])
    vide = {"lignes": [], "total_brut": 0.0, "total_retenue": 0.0,
            "total_net": 0.0, "orphelines": [], "profil": libelle}

    annonces = annonces_de_dividende()
    if annonces.empty:
        return vide

    aujourd_hui = a_partir_de or date.today()

    # Une annonce sans ticker ne peut etre rattachee a aucune ligne. Elle est
    # rendue a part plutot qu'ignoree : au 09/09/2026, TotalEnergies Marketing
    # CI etait dans ce cas, et une position TTLC.ci n'aurait rien vu venir.
    orphelines = []
    for _, a in annonces.iterrows():
        if a["ticker"]:
            continue
        d = str(a["date_operation"] or "")[:10]
        if d and d >= aujourd_hui.isoformat():
            orphelines.append({"emetteur": a["emetteur"], "brut": a["brut"],
                               "date": d})

    if portefeuille is None or portefeuille.empty:
        return {**vide, "orphelines": orphelines}

    quantites = {}
    for _, p in portefeuille.iterrows():
        t = p.get("ticker")
        q = p.get("quantity")
        if not t or q is None or pd.isna(q):
            continue
        quantites[t] = quantites.get(t, 0) + float(q)

    lignes = []
    for _, a in annonces.iterrows():
        t = a["ticker"]
        if not t or t not in quantites:
            continue
        jour = str(a["date_operation"] or "")[:10]
        if not jour or jour < aujourd_hui.isoformat():
            continue          # deja paye : c'est l'affaire des encaissements

        taux = a.get(colonne)
        taux = float(taux) if taux is not None and not pd.isna(taux) else 0.0
        quantite = quantites[t]
        brut_action = float(a["brut"])
        brut = quantite * brut_action
        retenue = brut * taux / 100.0

        lignes.append({
            "ticker": t,
            "emetteur": a["emetteur"],
            "quantite": quantite,
            "brut_action": brut_action,
            "taux": taux,
            "brut": brut,
            "retenue": retenue,
            "net": brut - retenue,
            "date": jour,
            "jours": (pd.Timestamp(jour).date() - aujourd_hui).days,
        })

    lignes.sort(key=lambda l: (l["date"], -l["net"]))
    return {
        "lignes": lignes,
        "total_brut": sum(l["brut"] for l in lignes),
        "total_retenue": sum(l["retenue"] for l in lignes),
        "total_net": sum(l["net"] for l in lignes),
        "orphelines": orphelines,
        "profil": libelle,
    }
