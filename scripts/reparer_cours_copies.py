#!/usr/bin/env python3
"""Series de cours recopiees d'un titre sur un autre : quarantaine et recollecte.

CE QUI S'EST PASSE

Deux exports RichBourse ont ete telecharges sous le mauvais nom : le fichier
« sicor-ci(SICC).csv » contient les cours de SAPH, « unilever-ci(UNLC).csv »
ceux de Sucrivoire — meme empreinte MD5, octet pour octet. L'import
(`importer_richbourse.py`) les a rattaches a leur nom de fichier : Sicor a
porte 5 021 seances de SAPH (1998 a mars 2026), Unilever 2 160 seances de
Sucrivoire (2016 a mars 2026). La fiche d'Unilever affichait ainsi « +3 808 %
sur un an » : le cours sautait chaque debut de mois des 1 000 FCFA de
Sucrivoire aux 50 000 d'Unilever (audit du 26/09/2026).

CE QUE FAIT CE SCRIPT

1. Repere les couples de titres qui partagent au moins SEUIL seances
   identiques (meme date, meme cloture, meme volume) : ce n'est pas un
   hasard, c'est une copie.
2. Decide a qui la serie appartient en la confrontant a sikafinance sur
   quelques seances : le titre dont sikafinance donne AUTRE CHOSE est la
   victime de la copie.
3. Deplace les seances copiees de la victime dans `price_cache_quarantaine`
   (rien n'est efface : une erreur de diagnostic se defait par un INSERT).
4. Recollecte les seances de la victime sur sikafinance, par tranches de
   trois mois (plafond de l'API en journalier), sans jamais ecraser une
   seance deja en base. sikafinance sert des cours AJUSTES des divisions,
   comme la base (verifie sur Sonatel 2010 et BOA Senegal 2024).

Usage :
  python3 scripts/reparer_cours_copies.py            # diagnostic seul
  python3 scripts/reparer_cours_copies.py --ecrire
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from collections import Counter
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from data.db import get_connection, read_sql_df  # noqa: E402

SEUIL = 50        # seances identiques au-dela desquelles c'est une copie
TEMOINS = 12      # seances confrontees a sikafinance pour designer la victime


def _sika():
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "collecter_historique_cours.py")
    spec = importlib.util.spec_from_file_location("collecte", chemin)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod._demander


def _jour(txt: str) -> str:
    j, m, a = txt.split("/")
    return f"{a}-{m}-{j}"


def couples_copies() -> dict:
    p = read_sql_df(
        "SELECT ticker, date, close, volume FROM price_cache "
        "WHERE ticker NOT LIKE 'IDX%%' AND ticker NOT LIKE 'BRVM%%' "
        "AND volume > 0")
    p["cle"] = (p["date"].astype(str) + "|" + p["close"].astype(str) + "|"
                + p["volume"].astype(str))
    par_cle = p.groupby("cle")["ticker"].apply(lambda s: tuple(sorted(set(s))))
    par_cle = par_cle[par_cle.apply(len) == 2]
    compte = Counter(par_cle.values)
    sortie = {}
    for couple, n in compte.items():
        if n >= SEUIL:
            dates = sorted(k.split("|")[0] for k, v in par_cle.items() if v == couple)
            sortie[couple] = dates
    return sortie


def victime(couple, dates, demander) -> tuple:
    """(titre victime, titre source, detail) — ou (None, None, detail)."""
    temoins = dates[-TEMOINS:]
    base = read_sql_df(
        "SELECT ticker, date, close FROM price_cache WHERE ticker IN (?, ?) "
        "AND date >= ? AND date <= ?",
        params=(couple[0], couple[1], temoins[0], temoins[-1]))
    base = {(r.ticker, str(r.date)): r.close for r in base.itertuples()}
    accord = {}
    for tk in couple:
        pts = demander(tk, temoins[0], temoins[-1], "0")
        sika = {_jour(p["Date"]): p["Close"] for p in pts}
        communs = [d for d in temoins if d in sika]
        bons = sum(1 for d in communs
                   if abs(sika[d] - base.get((tk, d), -1)) <= max(1, 0.005 * sika[d]))
        accord[tk] = (bons, len(communs))
    a, b = couple
    detail = f"accord sikafinance {a} {accord[a][0]}/{accord[a][1]}, {b} {accord[b][0]}/{accord[b][1]}"
    if accord[a][1] and accord[b][1]:
        if accord[a][0] > accord[b][0] and accord[b][0] <= accord[b][1] // 4:
            return b, a, detail
        if accord[b][0] > accord[a][0] and accord[a][0] <= accord[a][1] // 4:
            return a, b, detail
    return None, None, detail


PAQUET = 400


def _num(v):
    return "NULL" if v is None else f"{float(v):.6f}"


def recollecter(ticker: str, debut: str, fin: str, demander, cnx, ecrire: bool,
                exclus=()) -> int:
    """Seances sikafinance manquantes en base, inserees par paquets.

    `exclus` : dates que la base porte encore mais qui partiront en
    quarantaine — en simulation, elles ne doivent pas masquer la recollecte.
    """
    existants = set(pd.to_datetime(read_sql_df(
        "SELECT date FROM price_cache WHERE ticker = ?",
        params=(ticker,))["date"]).dt.strftime("%Y-%m-%d")) - set(exclus)
    lignes = []
    d0 = date.fromisoformat(debut)
    d1 = date.fromisoformat(fin)
    while d0 <= d1:
        d2 = min(d0 + timedelta(days=88), d1)
        for p in demander(ticker, d0.isoformat(), d2.isoformat(), "0"):
            j = _jour(p["Date"])
            if j in existants or not p.get("Close"):
                continue
            existants.add(j)
            lignes.append((j, p.get("Open"), p.get("High"), p.get("Low"),
                           p["Close"], p.get("Volume")))
        d0 = d2 + timedelta(days=1)
        time.sleep(0.3)
    if ecrire:
        for i in range(0, len(lignes), PAQUET):
            vals = ",".join(
                f"('{ticker}', '{j}', {_num(o)}, {_num(h)}, {_num(l)}, "
                f"{_num(c)}, {_num(v)})" for j, o, h, l, c, v in lignes[i:i + PAQUET])
            cnx.execute("INSERT INTO price_cache (ticker, date, open, high, low, close, "
                        "volume) VALUES " + vals + " ON CONFLICT (ticker, date) DO NOTHING")
            cnx.commit()
    return len(lignes)


def main(ecrire: bool) -> None:
    demander = _sika()
    couples = couples_copies()
    if not couples:
        print("Aucune serie recopiee.")
        return
    cnx = get_connection() if ecrire else None
    if ecrire:
        cnx.execute("""CREATE TABLE IF NOT EXISTS price_cache_quarantaine (
            ticker TEXT, date TEXT, open DOUBLE PRECISION, high DOUBLE PRECISION,
            low DOUBLE PRECISION, close DOUBLE PRECISION, volume DOUBLE PRECISION,
            motif TEXT, mis_le TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cnx.commit()
    for couple, dates in couples.items():
        v, s, detail = victime(couple, dates, demander)
        print(f"{couple[0]} / {couple[1]} : {len(dates)} seances identiques "
              f"({dates[0]} -> {dates[-1]}) ; {detail}")
        if not v:
            print("   victime indeterminee : rien n'est fait")
            continue
        print(f"   {v} porte les cours de {s}")
        if ecrire:
            motif = (f"copie de {s} (export RichBourse mal nomme), "
                     f"reperee le {date.today().isoformat()}")
            for i in range(0, len(dates), PAQUET):
                vals = ",".join(f"('{v}', '{j}')" for j in dates[i:i + PAQUET])
                cnx.execute(
                    "INSERT INTO price_cache_quarantaine "
                    "(ticker, date, open, high, low, close, volume, motif) "
                    "SELECT p.ticker, p.date, p.open, p.high, p.low, p.close, p.volume, "
                    f"'{motif}' FROM price_cache p WHERE (p.ticker, p.date) IN ({vals})")
                cnx.execute(f"DELETE FROM price_cache WHERE (ticker, date) IN ({vals})")
                cnx.commit()
        n = recollecter(v, dates[0], dates[-1], demander, cnx, ecrire, exclus=dates)
        if ecrire:
            cnx.commit()
        print(f"   {len(dates)} seances en quarantaine, {n} seances recollectees "
              f"sur sikafinance" + ("" if ecrire else " (simulation)"))
    if ecrire:
        cnx.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
