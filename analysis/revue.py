"""Revue de presse : assemble depeches, chiffres et portefeuille.

Principe : on ne reformule jamais. Chaque entree combine
  - des chiffres CALCULES par l'app (extraits des rapports, avec variations),
  - une citation TEXTUELLE de la source (commentaire de l'emetteur ou depeche).

Aucun service externe, aucune generation de texte : ce qui est affiche a ete
soit calcule a partir des donnees en base, soit cite mot pour mot.
"""
from __future__ import annotations

import re

import pandas as pd

from config import load_tickers
from data.db import read_sql_df

# Ordre d'affichage des rubriques
RUBRIQUES = [
    ("portefeuille", "Vos lignes"),
    ("resultats", "Résultats et publications"),
    ("marche", "Séances et marché"),
    ("secteur", "Contexte sectoriel et macro"),
]

_THEME_RUBRIQUE = {
    "resultats": "resultats",
    "dividende": "resultats",
    "operation": "resultats",
    "marche": "marche",
    "secteur": "secteur",
}


def _noms() -> dict:
    return {t["ticker"]: t.get("name") or t["ticker"] for t in load_tickers()}


def _secteurs() -> dict:
    """{secteur: [tickers]} pour rattacher une depeche sectorielle a la cote."""
    par_secteur = {}
    for t in load_tickers():
        par_secteur.setdefault(t.get("sector") or "Autres", []).append(t["ticker"])
    return par_secteur


def _liste(champ) -> list:
    if not champ or (isinstance(champ, float) and pd.isna(champ)):
        return []
    return [x for x in str(champ).split(",") if x]


# ──────────────────────────────────────────────────────────────────────────
# Chiffres
# ──────────────────────────────────────────────────────────────────────────

def _fmt_montant(v) -> str:
    """En milliards, l'unite dans laquelle se lisent les comptes BRVM."""
    if v is None or pd.isna(v):
        return "—"
    mds = float(v) / 1_000_000_000
    if abs(mds) >= 100:
        return f"{mds:,.0f} Mds".replace(",", " ")
    if abs(mds) >= 1:
        return f"{mds:,.1f} Mds".replace(",", " ").replace(".", ",")
    return f"{float(v) / 1_000_000:,.0f} M".replace(",", " ")


def _fmt_variation(actuel, precedent) -> str:
    if actuel is None or precedent is None or pd.isna(actuel) or pd.isna(precedent):
        return ""
    if not precedent:
        return ""
    pct = (float(actuel) - float(precedent)) / abs(float(precedent)) * 100
    signe = "+" if pct >= 0 else "−"
    return f" ({signe}{abs(pct):.1f} %)"


def _table_trimestres():
    """Tout quarterly_data en une requete : une par depeche saturait le pooler."""
    try:
        return read_sql_df(
            """SELECT ticker, fiscal_year, quarter, revenue, net_income
               FROM quarterly_data
               ORDER BY fiscal_year DESC, quarter DESC"""
        )
    except Exception:
        return None


def chiffres_recents(ticker: str, table=None) -> str:
    """Derniere periode publiee, avec la variation sur un an quand elle existe."""
    df = table if table is not None else _table_trimestres()
    if df is None or df.empty:
        return ""
    df = df[df["ticker"] == ticker]
    if df.empty:
        return ""

    cur = df.iloc[0]
    prec = df[(df["fiscal_year"] == cur["fiscal_year"] - 1)
              & (df["quarter"] == cur["quarter"])]
    prec = prec.iloc[0] if not prec.empty else None

    periode = f"T{int(cur['quarter'])} {int(cur['fiscal_year'])}"
    morceaux = []
    if pd.notna(cur.get("revenue")):
        var = _fmt_variation(cur["revenue"], prec["revenue"] if prec is not None else None)
        morceaux.append(f"CA {_fmt_montant(cur['revenue'])}{var}")
    if pd.notna(cur.get("net_income")):
        var = _fmt_variation(cur["net_income"],
                             prec["net_income"] if prec is not None else None)
        morceaux.append(f"résultat net {_fmt_montant(cur['net_income'])}{var}")
    return f"{periode} — " + " · ".join(morceaux) if morceaux else ""


# ──────────────────────────────────────────────────────────────────────────
# Assemblage
# ──────────────────────────────────────────────────────────────────────────

# Les chapeaux repris des pages de liste trainent l'horodatage de publication
# et les points de suspension de la troncature du site.
_PARASITES = re.compile(r"\s*\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2})?\s*$")


def _extrait(texte: str, maxi: int = 420) -> str:
    """Citation courte, coupee sur une fin de phrase."""
    t = re.sub(r"\s+", " ", texte or "").strip()
    t = _PARASITES.sub("", t).strip()
    t = re.sub(r"\.{3,}$|…$", "", t).strip()
    if len(t) <= maxi:
        return t
    coupe = t[:maxi]
    point = coupe.rfind(". ")
    return (coupe[:point + 1] if point > maxi // 2 else coupe).strip() + " […]"


def build_revue(jours: int = 10, portefeuille: list | None = None) -> dict:
    """Rubriques de la revue, chacune une liste d'entrees pretes a afficher."""
    portefeuille = set(portefeuille or [])
    noms = _noms()
    par_secteur = _secteurs()
    trimestres = _table_trimestres()

    try:
        df = read_sql_df(
            """SELECT source, url, title, published_at, lead, body,
                      tickers, tickers_cites, secteurs, theme
               FROM news_articles
               ORDER BY published_at DESC NULLS LAST, id DESC
               LIMIT 200"""
        )
    except Exception:
        return {}
    if df is None or df.empty:
        return {}

    if jours:
        limite = (pd.Timestamp.today() - pd.Timedelta(days=jours)).strftime("%Y-%m-%d")
        df = df[(df["published_at"].isna()) | (df["published_at"] >= limite)]

    rubriques = {cle: [] for cle, _ in RUBRIQUES}

    for _, r in df.iterrows():
        sujets = _liste(r["tickers"])
        cites = _liste(r["tickers_cites"])
        secteurs = _liste(r["secteurs"])

        # Titres de la cote exposes a une depeche sans emetteur identifie
        exposes = []
        if not sujets:
            for s in secteurs:
                exposes.extend(par_secteur.get(s, []))

        # Une societe nommee dans la depeche compte ; une societe simplement
        # exposee par son secteur ne suffit pas a faire remonter l'article en
        # tete, sinon toute nouvelle bancaire d'un pays voisin y atterrit.
        nommes = set(sujets) | set(cites)
        en_portefeuille = sorted(portefeuille & nommes)
        exposees = sorted(portefeuille & set(exposes))

        entree = {
            "titre": r["title"],
            "date": r["published_at"] or "",
            "url": r["url"],
            "source": r["source"],
            "theme": r["theme"],
            "sujets": [(t, noms.get(t, t)) for t in sujets],
            "cites": [(t, noms.get(t, t)) for t in cites],
            "secteurs": secteurs,
            "portefeuille": [(t, noms.get(t, t)) for t in en_portefeuille],
            "exposees": [(t, noms.get(t, t)) for t in exposees],
            "texte": _extrait(r["body"] or r["lead"]),
            "chiffres": chiffres_recents(sujets[0], trimestres) if sujets else "",
        }

        if en_portefeuille:
            rubriques["portefeuille"].append(entree)
        else:
            rubriques[_THEME_RUBRIQUE.get(r["theme"], "secteur")].append(entree)

    return rubriques
