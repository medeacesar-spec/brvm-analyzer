#!/usr/bin/env python3
"""Ajuste l'historique d'un titre fractionne (ou regroupe), une fois et une seule.

Pourquoi : le 26/10/2026, Sonatel a ete fractionnee a raison de dix actions
nouvelles pour une ancienne (avis BRVM 304 et 309-2026). Le cours est passe de
43 850 a 4 385 FCFA de reference sans que rien ne se perde. Sans ajustement,
l'application y voyait une chute de 89 % : courbes, volatilite, rendements,
PER (cours / BNPA d'avant) a 1,1, rendement du dividende a 41 %, et des lignes
de portefeuille valorisees au dixieme.

LA REGLE : tout ce qui est anterieur a la premiere seance apres l'operation
s'exprime en actions NOUVELLES.
  - cours (ouverture, haut, bas, cloture) : / ratio ; volumes : x ratio ;
  - lignes de portefeuille achetees avant : quantite x ratio, prix / ratio
    (le montant investi et les frais ne changent pas) ;
  - donnees par action des fondamentaux (dividende, BNPA, cours) : / ratio ;
    nombre d'actions : x ratio ;
  - prix enregistres des verdicts et des signaux : / ratio.
Les montants en francs (dividendes encaisses, resultats, capitalisation) ne
changent pas.

UNE FOIS ET UNE SEULE. Chaque (evenement, table) applique est inscrit dans
`evenements_appliques` ; un second passage ne fait rien. Pour les cours, une
seconde garde verifie le saut lui-meme : on n'ajuste que si la derniere cloture
d'avant l'operation vaut bien environ « ratio » fois la cloture de reference.
Un historique deja ajuste (par la source ou a la main) n'est pas divise deux
fois.

Les evenements viennent de data/evenements_cote.json (type « fractionnement »,
« ratio » = actions nouvelles pour une ancienne ; un regroupement s'ecrit avec
un ratio inferieur a 1, par exemple 0.1 pour dix anciennes contre une).

Usage :
    python3 scripts/appliquer_fractionnements.py            # simulation
    python3 scripts/appliquer_fractionnements.py --ecrire
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

REGISTRE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data", "evenements_cote.json")

# Tables de cours : (table, borne) — la borne dit quelles lignes sont
# anterieures a l'operation. Une ligne mensuelle est datee du 1er du mois et
# porte le cours de fin de mois : le mois de l'operation, deja a cheval, est
# recalcule chaque soir depuis le journalier (mettre_a_jour_mensuel).
COURS = [
    ("price_cache", "jour"),
    ("price_cache_quarantaine", "jour"),
    ("price_monthly", "mois"),
    ("price_monthly_quarantaine", "mois"),
    ("price_quarterly", "trimestre"),
]


def _borne(debut: date, maille: str) -> str:
    if maille == "mois":
        return date(debut.year, debut.month, 1).isoformat()
    if maille == "trimestre":
        return date(debut.year, 3 * ((debut.month - 1) // 3) + 1, 1).isoformat()
    return debut.isoformat()


def evenements() -> list:
    with open(REGISTRE, encoding="utf-8") as f:
        tous = json.load(f)["evenements"]
    return [e for e in tous if e.get("type") == "fractionnement" and e.get("ratio")]


def _deja(cnx, e, cible) -> bool:
    if not _TABLE_PRESENTE:
        return False
    return cnx.execute(
        "SELECT 1 FROM evenements_appliques WHERE ticker = ? AND type = ? "
        "AND date_effet = ? AND cible = ?",
        (e["ticker"], e["type"], e["debut"], cible)).fetchone() is not None


def _marquer(cnx, e, cible, lignes: int, detail: str) -> None:
    cnx.execute(
        "INSERT INTO evenements_appliques (ticker, type, date_effet, ratio, cible, "
        "lignes, detail) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (e["ticker"], e["type"], e["debut"], float(e["ratio"]), cible, lignes, detail))


def _reference(cnx, ticker: str, debut: str):
    """Premiere cloture a partir de l'operation, dans le journalier."""
    r = cnx.execute("SELECT close FROM price_cache WHERE ticker = ? AND date >= ? "
                    "AND close > 0 ORDER BY date LIMIT 1", (ticker, debut)).fetchone()
    return float(r[0]) if r else None


def appliquer(e: dict, cnx, ecrire: bool) -> list:
    k = float(e["ratio"])
    tk, debut = e["ticker"], date.fromisoformat(e["debut"])
    compte = []

    ref = _reference(cnx, tk, e["debut"])
    for table, maille in COURS:
        cible = table
        if _deja(cnx, e, cible):
            compte.append((cible, "deja applique", 0))
            continue
        borne = _borne(debut, maille)
        avant = cnx.execute(f"SELECT close FROM {table} WHERE ticker = ? AND date < ? "
                            "AND close > 0 ORDER BY date DESC LIMIT 1", (tk, borne)).fetchone()
        n = cnx.execute(f"SELECT COUNT(*) FROM {table} WHERE ticker = ? AND date < ?",
                        (tk, borne)).fetchone()[0]
        if not n:
            compte.append((cible, "rien avant l'operation", 0))
            continue
        # Garde : le saut doit etre la. Une cloture d'avant a moins de la
        # racine du ratio fois la reference est deja exprimee en actions
        # nouvelles (ajustee par la source) : on n'y touche pas.
        if ref and avant and avant[0]:
            saut = float(avant[0]) / ref
            if not (k ** 0.5 <= saut if k > 1 else saut <= k ** 0.5):
                compte.append((cible, f"pas de saut ({saut:.2f}) : deja ajuste", 0))
                if ecrire:
                    _marquer(cnx, e, cible, 0, f"saut {saut:.2f}, rien a faire")
                continue
        compte.append((cible, f"cours / {k:g}, volumes x {k:g}", n))
        if ecrire:
            cnx.execute(
                f"UPDATE {table} SET open = open / ?, high = high / ?, low = low / ?, "
                "close = close / ?, volume = volume * ? WHERE ticker = ? AND date < ?",
                (k, k, k, k, k, tk, borne))
            _marquer(cnx, e, cible, n, f"date < {borne}")

    # Lignes de portefeuille achetees avant (ou sans date : elles l'etaient).
    if not _deja(cnx, e, "portfolio"):
        n = cnx.execute("SELECT COUNT(*) FROM portfolio WHERE ticker = ? AND "
                        "(purchase_date IS NULL OR purchase_date < ?)",
                        (tk, e["debut"])).fetchone()[0]
        compte.append(("portfolio", f"quantite x {k:g}, prix / {k:g}", n))
        if ecrire:
            cnx.execute(
                "UPDATE portfolio SET quantity = quantity * ?, avg_price = avg_price / ?, "
                "notes = COALESCE(notes || ' ; ', '') || ? WHERE ticker = ? AND "
                "(purchase_date IS NULL OR purchase_date < ?)",
                (k, k, f"fractionnement {k:g} pour 1 du {e['debut']}", tk, e["debut"]))
            _marquer(cnx, e, "portfolio", n, "")

    # Donnees par action : tous les exercices, pour une serie homogene.
    if not _deja(cnx, e, "fundamentals"):
        n = cnx.execute("SELECT COUNT(*) FROM fundamentals WHERE ticker = ?",
                        (tk,)).fetchone()[0]
        compte.append(("fundamentals", f"dividende, BNPA, cours / {k:g} ; actions x {k:g}", n))
        if ecrire:
            cnx.execute(
                "UPDATE fundamentals SET dps = dps / ?, dps_n0 = dps_n0 / ?, "
                "dps_n1 = dps_n1 / ?, dps_n2 = dps_n2 / ?, dps_n3 = dps_n3 / ?, "
                "eps = eps / ?, price = price / ?, shares = shares * ? WHERE ticker = ?",
                (k, k, k, k, k, k, k, k, tk))
            _marquer(cnx, e, "fundamentals", n, "tous exercices")
    if not _deja(cnx, e, "market_data"):
        compte.append(("market_data", f"actions x {k:g}, dividende / {k:g}", 1))
        if ecrire:
            cnx.execute("UPDATE market_data SET shares = shares * ?, dps = dps / ? "
                        "WHERE ticker = ?", (k, k, tk))
            _marquer(cnx, e, "market_data", 1, "")

    # Prix enregistres par les verdicts et les signaux.
    for table, col, jour in (("verdict_daily", "price", "date"),
                             ("signal_history", "price_at_start", "first_seen_date")):
        if _deja(cnx, e, table):
            continue
        n = cnx.execute(f"SELECT COUNT(*) FROM {table} WHERE ticker = ? AND {jour} < ?",
                        (tk, e["debut"])).fetchone()[0]
        compte.append((table, f"{col} / {k:g}", n))
        if ecrire:
            cnx.execute(f"UPDATE {table} SET {col} = {col} / ? WHERE ticker = ? "
                        f"AND {jour} < ?", (k, tk, e["debut"]))
            _marquer(cnx, e, table, n, "")
    return compte


_TABLE_PRESENTE = True


def creer_table(cnx) -> None:
    cnx.execute(
        "CREATE TABLE IF NOT EXISTS evenements_appliques ("
        " ticker TEXT NOT NULL, type TEXT NOT NULL, date_effet TEXT NOT NULL,"
        " ratio REAL, cible TEXT NOT NULL, lignes INTEGER, detail TEXT,"
        " applique_le TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        " PRIMARY KEY (ticker, type, date_effet, cible))")


def main(ecrire: bool = False) -> int:
    global _TABLE_PRESENTE
    cnx = get_connection()
    try:
        if ecrire:
            creer_table(cnx)
            cnx.commit()
        else:
            # La simulation n'ecrit rien, pas meme la table de suivi.
            r = cnx.execute("SELECT to_regclass('public.evenements_appliques')").fetchone()
            _TABLE_PRESENTE = bool(r and r[0])
        for e in evenements():
            if date.fromisoformat(e["debut"]) > date.today():
                continue                        # pas encore effectif
            print(f"== {e['ticker']} : {e['type']} {e['ratio']} pour 1 au {e['debut']}")
            for cible, quoi, n in appliquer(e, cnx, ecrire):
                print(f"   {cible:28} {n:6} ligne(s)  {quoi}")
        if ecrire:
            cnx.commit()
            print("Ecrit.")
        else:
            cnx.rollback()
            print("Simulation : rien n'a ete ecrit.")
    finally:
        cnx.close()
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    sys.exit(main(ap.parse_args().ecrire))
