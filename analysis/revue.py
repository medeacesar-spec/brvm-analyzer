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
from analysis.pertinence import score_depeche

# Ordre d'affichage des rubriques
RUBRIQUES = [
    ("portefeuille", "Vos lignes"),
    ("resultats", "Résultats et publications"),
    ("marche", "Séances et marché"),
    ("secteur", "Contexte sectoriel et macro"),
]

# LA REVUE SE LIT PAR PAGES : 1 j, 3 j, 7 j. Arbitrage du 15/09/2026.
#
# Vingt depeches AU PLUS par page, portefeuille compris : a cinquante, on
# ne lit plus. Le plafond precedent de vingt-cinq etait deja « au total »
# sur main, mais l'app deployee plafonnait encore PAR JOUR — et « 1 j »
# couvre deux dates, d'ou les cinquante observees.
#
# Les pages sont DISJOINTES. « 3 j » ne remontre pas ce que « 1 j » a deja
# montre : elle prend les vingt plus pertinentes des trois derniers jours
# PARMI CELLES QUE « 1 j » N'A PAS RETENUES, et « 7 j » fait de meme apres
# les deux autres. Passer d'une page a l'autre apporte donc toujours du neuf.
#
# Le choix se fait sur la pertinence, et sur elle seule. Les lignes du
# portefeuille passent devant a l'interieur de la page, mais ne la depassent
# plus : elles comptent dans les vingt.
PAGES = (1, 3, 7)
PAR_PAGE = 20

# Le libelle de la vue lisait ce nom ; il reste un alias du plafond.
MAX_AFFICHEES = PAR_PAGE

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


def chiffres_detail(ticker: str, table=None) -> list:
    """Memes chiffres que `chiffres_recents`, mais poste par poste.

    La chaine reste utile en repli ; la carte du redesign v4 affiche chaque
    poste separement, avec son libelle et sa couleur — une variation lue en
    vert ou en rouge se saisit sans lire la phrase.
    Renvoie une liste de (libelle, valeur, ton) ou ton vaut up / down / neutre.
    """
    df = table if table is not None else _table_trimestres()
    if df is None or df.empty:
        return []
    df = df[df["ticker"] == ticker]
    if df.empty:
        return []

    cur = df.iloc[0]
    prec = df[(df["fiscal_year"] == cur["fiscal_year"] - 1)
              & (df["quarter"] == cur["quarter"])]
    prec = prec.iloc[0] if not prec.empty else None
    periode = f"T{int(cur['quarter'])} {int(cur['fiscal_year'])}"
    precedente = f"T{int(cur['quarter'])} {int(cur['fiscal_year']) - 1}"

    postes = []
    for champ, libelle in (("revenue", "CA"), ("net_income", "Résultat net")):
        if not pd.notna(cur.get(champ)):
            continue
        postes.append((f"{libelle} · {periode}", _fmt_montant(cur[champ]), "neutre"))
        if prec is None or not pd.notna(prec.get(champ)) or not prec[champ]:
            continue
        pct = (float(cur[champ]) - float(prec[champ])) / abs(float(prec[champ])) * 100
        signe = "+" if pct >= 0 else "−"
        # Virgule decimale, comme les montants juste a cote.
        valeur = f"{signe}{abs(pct):.1f} %".replace(".", ",")
        postes.append((f"vs {precedente}", valeur,
                       "up" if pct >= 0 else "down"))
    return postes


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


def _limite(jours: int) -> str:
    """Premiere date couverte par une fenetre de `jours` jours.

    `published_at` est une date sans heure : la fenetre « 1 j » couvre
    aujourd'hui ET hier. C'est voulu — le matin, avant la collecte de 16 h,
    une page limitee au seul jour serait presque vide.
    """
    return (pd.Timestamp.today() - pd.Timedelta(days=jours)).strftime("%Y-%m-%d")


def _cle(entree: dict) -> str:
    """Ce qui fait qu'une depeche est « la meme ».

    L'URL ne suffit pas : sikafinance publie parfois le meme article sous
    deux rubriques, « brvm » et « economie », avec deux adresses. Le titre
    normalise les rapproche.
    """
    titre = re.sub(r"\W+", " ", str(entree.get("titre") or "").lower()).strip()
    return titre or str(entree.get("url") or "")


def build_revue(jours: int = 1, portefeuille: list | None = None) -> dict:
    """La page `jours` de la revue : au plus PAR_PAGE depeches, par rubrique.

    Retourne {rubrique: [entrees]} plus `_examinees` (depeches de la fenetre
    encore disponibles pour cette page) et `_ecartees`.
    """
    portefeuille = set(portefeuille or [])
    noms = _noms()
    par_secteur = _secteurs()

    # Filtre de date EN BASE, et plus de LIMIT 400 : la semaine du 9 au 15
    # septembre compte a elle seule 402 depeches, et la page « 7 j » perdait
    # silencieusement son premier jour.
    try:
        df = read_sql_df(
            """SELECT source, url, title, published_at, lead, body,
                      tickers, tickers_cites, secteurs, theme
               FROM news_articles
               WHERE published_at >= ?
               ORDER BY published_at DESC, id DESC""",
            params=(_limite(jours),),
        )
    except Exception:
        return {}
    if df is None or df.empty:
        return {}

    retenues = []
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

        note, motifs = score_depeche(
            titre=r["title"], texte=r["body"] or r["lead"],
            tickers_sujets=sujets, tickers_cites=cites,
            secteurs=secteurs, theme=r["theme"])

        retenues.append({
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
            "chiffres": "",
            "chiffres_detail": [],
            "score": note,
            "motifs": motifs,
            "rubrique": ("portefeuille" if en_portefeuille
                         else _THEME_RUBRIQUE.get(r["theme"], "secteur")),
            "_jour": str(r["published_at"])[:10],
        })

    page, examinees = _page(retenues, jours)

    # Les chiffres ne se calculent que pour les depeches affichees : les
    # calculer pour les quatre cents de la semaine, c'etait autant de travail
    # jete a chaque ouverture.
    trimestres = _table_trimestres() if any(e["sujets"] for e in page) else None
    rubriques = {cle: [] for cle, _ in RUBRIQUES}
    for entree in page:
        if entree["sujets"]:
            ticker = entree["sujets"][0][0]
            entree["chiffres"] = chiffres_recents(ticker, trimestres)
            entree["chiffres_detail"] = chiffres_detail(ticker, trimestres)
        rubriques[entree["rubrique"]].append(entree)
    rubriques["_examinees"] = examinees
    rubriques["_ecartees"] = max(examinees - len(page), 0)
    return rubriques


def _choisir(candidates: list, deja: set) -> list:
    """Les PAR_PAGE meilleures, sans doublon ni depeche deja montree.

    Ordre de choix : une ligne du portefeuille d'abord, puis la note, puis
    la plus recente. L'heure ne sert qu'a departager deux notes egales.
    """
    ordre = sorted(candidates, key=lambda e: e["_jour"], reverse=True)
    ordre.sort(key=lambda e: (e["rubrique"] != "portefeuille", -e["score"]))
    choisies, vues = [], set(deja)
    for entree in ordre:
        cle = _cle(entree)
        if cle in vues:
            continue
        vues.add(cle)
        choisies.append(entree)
        if len(choisies) == PAR_PAGE:
            break
    return choisies


def _page(entrees: list, jours: int) -> tuple:
    """La page demandee, apres retrait de ce que les pages plus courtes montrent.

    Retourne (entrees de la page, nombre de candidates de sa fenetre).

    Pour la page « 3 j », on rejoue d'abord la page « 1 j » : ce qu'elle
    retient est retire, puis on choisit parmi le reste des trois jours. Pour
    « 7 j », on rejoue « 1 j » puis « 3 j ». Le calcul est le meme que celui
    que le lecteur a vu en passant d'une page a l'autre, donc aucune depeche
    ne peut apparaitre deux fois.
    """
    paliers = sorted({p for p in PAGES if p < jours} | {jours})
    deja = set()
    page, examinees = [], 0
    for palier in paliers:
        limite = _limite(palier)
        candidates = [e for e in entrees
                      if e["_jour"] >= limite and _cle(e) not in deja]
        page = _choisir(candidates, deja)
        examinees = len({_cle(e) for e in candidates})
        deja |= {_cle(e) for e in page}

    # A l'affichage, du plus recent au mieux note : la selection est faite,
    # l'ordre redevient chronologique.
    page.sort(key=lambda e: (e["_jour"], e["score"]), reverse=True)
    return page, examinees
