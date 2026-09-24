"""Depuis quand un titre est-il cote ?

POURQUOI CETTE QUESTION MERITE UN MODULE

Une introduction en bourse ressemble, pour toutes nos regles, a un titre
mort : aucune publication, aucun rapport annuel, une seance de cotation.
Bridge Bank est entree a la cote le 24 septembre 2026 et l'application l'a
aussitot signalee « potentiellement dormante », tout en proposant a
l'administrateur de « charger les prix historiques » — un historique qui
n'existe pas et ne peut pas exister.

Les deux messages sont faux de la meme facon : ils supposent qu'un titre
sans passe est un titre qui s'est eteint. Ce module donne de quoi
distinguer les deux.
"""
from __future__ import annotations

from datetime import date, datetime

from data.db import read_sql_df

# En deca, on considere qu'un titre vient d'entrer a la cote : c'est la
# duree qu'il faut pour qu'un exercice soit clos, publie, et qu'une premiere
# comparaison annuelle devienne possible.
MOIS_NOUVEAU = 12


def premiere_cotation(ticker: str):
    """La date de la premiere seance connue, ou None."""
    d = read_sql_df("SELECT MIN(date) AS debut FROM price_cache "
                    "WHERE ticker = ? AND close > 0", params=(ticker,))
    if d.empty:
        return None
    debut = d.iloc[0]["debut"]
    if not debut or debut != debut:
        return None
    if isinstance(debut, str):
        try:
            return datetime.fromisoformat(debut[:10]).date()
        except ValueError:
            return None
    return debut if isinstance(debut, date) else getattr(debut, "date", lambda: None)()


def seances_connues(ticker: str) -> int:
    d = read_sql_df("SELECT COUNT(*) AS n FROM price_cache "
                    "WHERE ticker = ? AND close > 0", params=(ticker,))
    return int(d.iloc[0]["n"]) if not d.empty else 0


def anciennete_mois(ticker: str):
    """Nombre de mois depuis la premiere seance, ou None si on l'ignore."""
    debut = premiere_cotation(ticker)
    if not debut:
        return None
    return (date.today() - debut).days / 30.44


def est_nouvellement_cote(ticker: str, mois: float = MOIS_NOUVEAU) -> bool:
    """Vrai si le titre est entre a la cote il y a moins de `mois` mois."""
    age = anciennete_mois(ticker)
    return age is not None and age < mois


def _jour_fr(jour) -> str:
    MOIS = ("janvier", "fevrier", "mars", "avril", "mai", "juin", "juillet",
            "aout", "septembre", "octobre", "novembre", "decembre")
    return f"{jour.day} {MOIS[jour.month - 1]} {jour.year}"


def message_nouvelle_cotation(ticker: str, besoin: str = None):
    """La phrase a afficher a la place d'un « historique insuffisant ».

    Rend None si le titre n'est pas une introduction recente — l'appelant
    garde alors son message habituel.
    """
    if not est_nouvellement_cote(ticker):
        return None
    debut = premiere_cotation(ticker)
    seances = seances_connues(ticker)
    phrase = (f"Titre introduit le {_jour_fr(debut)} : "
              f"{seances} séance{'s' if seances > 1 else ''} cotée"
              f"{'s' if seances > 1 else ''} à ce jour. "
              "L'historique se constitue séance après séance.")
    if besoin:
        phrase += f" {besoin}"
    return phrase
