#!/usr/bin/env python3
"""Aligne les seances d'avril 2021 a septembre 2026 sur les exports RichBourse.

CE QUE L'AUDIT DU 26/09/2026 A TROUVE

`price_cache` tenait, avant l'import RichBourse, des seances ecrites depuis
avril 2021 par deux robots. L'import n'ecrase jamais une seance existante :
ces lignes sont restees, et deux familles sont fausses.

1. LES POINTS MENSUELS GLISSES DANS LE JOURNALIER. Le premier jour de chaque
   mois porte l'ouverture du mois, son plus haut, son plus bas, la cloture de
   sa DERNIERE seance et le volume CUMULE du mois — le format de
   `price_monthly`, pas une seance. Sonatel au 01/07/2021 : 14 280 et 333 125
   titres, quand la seance du jour valait 13 400 et 1 296 titres. Le cours
   n'est de surcroit PAS ajuste : SAFCA passe chaque debut de mois de 470 a
   800, BOA CI de 2 125 a 2 575. Environ quarante lignes par titre, 1 876 en
   tout : la courbe fait un aller-retour chaque debut de mois, et toute
   volatilite, tout plus haut, toute performance mensuelle en sont faux.
   Certaines tombent un jour ferie (1er janvier, 1er mai) : aucune seance n'a
   eu lieu.

2. DES LIGNES DATEES D'UN JOUR SANS SEANCE, samedi ou dimanche : le robot a
   ecrit la derniere seance sous la date de son passage.

CE QUE FAIT CE SCRIPT, pour chaque titre dont l'export est fiable (Sicor et
Unilever ne le sont pas : voir `reparer_cours_copies.py`) :

- une seance presente des deux cotes mais dont le cours ou le volume differe
  prend le cours et le volume de l'export (serie AJUSTEE, coherente sur
  28 ans), et ouverture = plus haut = plus bas = cloture, faute de mieux ;
- une ligne datee d'un jour ou l'export n'a pas de seance, jusqu'a la
  derniere date de l'export, part en quarantaine (`price_cache_quarantaine`) ;
- apres la derniere date de l'export, les lignes datees d'un samedi ou d'un
  dimanche partent en quarantaine.

Rien n'est efface : la quarantaine garde chaque ligne et son motif.

Usage :
  python3 scripts/aligner_cours_richbourse.py            # simulation
  python3 scripts/aligner_cours_richbourse.py --ecrire
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection, read_sql_df  # noqa: E402

DEBUT = "2021-04-01"
EXPORTS_FAUX = {"SICC.ci", "UNLC.ci"}   # fichiers recopies d'un autre titre
# L'import ecarte les deux fichiers d'un doublon ; ceux-ci sont les
# vrais, verifies sur sikafinance (reparer_cours_copies.py).
PROPRIETAIRES = {"saph-ci(SPHC).csv": "SPHC.ci", "sucrivoire-ci(SCRC).csv": "SCRC.ci"}
TOLERANCE_COURS = 0.005
TOLERANCE_VOLUME = 0.5
PAQUET = 400


def _importeur():
    chemin = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "importer_richbourse.py")
    spec = importlib.util.spec_from_file_location("imp", chemin)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _quarantaine(cnx, lignes, motif):
    for i in range(0, len(lignes), PAQUET):
        vals = ",".join(f"('{t}', '{d}')" for t, d in lignes[i:i + PAQUET])
        cnx.execute(
            "INSERT INTO price_cache_quarantaine "
            "(ticker, date, open, high, low, close, volume, motif) "
            "SELECT p.ticker, p.date, p.open, p.high, p.low, p.close, p.volume, "
            f"'{motif}' FROM price_cache p WHERE (p.ticker, p.date) IN ({vals})")
        cnx.execute(f"DELETE FROM price_cache WHERE (ticker, date) IN ({vals})")


def _remplacer(cnx, lignes, motif):
    """lignes : [(ticker, date, cours, volume)]. L'ancienne ligne est gardee
    en quarantaine avant d'etre remplacee."""
    _quarantaine_copie(cnx, [(t, d) for t, d, _, _ in lignes], motif)
    for i in range(0, len(lignes), PAQUET):
        vals = ",".join(f"('{t}', '{d}', {c:.6f}, {v:.1f})"
                        for t, d, c, v in lignes[i:i + PAQUET])
        cnx.execute(
            "UPDATE price_cache AS p SET close = x.c, open = x.c, high = x.c, "
            "low = x.c, volume = x.v FROM (VALUES " + vals + ") "
            "AS x(t, d, c, v) WHERE p.ticker = x.t AND p.date = x.d")


def _quarantaine_copie(cnx, cles, motif):
    for i in range(0, len(cles), PAQUET):
        vals = ",".join(f"('{t}', '{d}')" for t, d in cles[i:i + PAQUET])
        cnx.execute(
            "INSERT INTO price_cache_quarantaine "
            "(ticker, date, open, high, low, close, volume, motif) "
            "SELECT p.ticker, p.date, p.open, p.high, p.low, p.close, p.volume, "
            f"'{motif}' FROM price_cache p WHERE (p.ticker, p.date) IN ({vals})")


def main(ecrire: bool) -> None:
    imp = _importeur()
    couples, _ = imp.correspondance()
    for chemin in imp._fichiers():
        if os.path.basename(chemin) in PROPRIETAIRES:
            couples[chemin] = PROPRIETAIRES[os.path.basename(chemin)]
    base = read_sql_df(
        "SELECT ticker, date, close, volume FROM price_cache WHERE date >= ?",
        params=(DEBUT,))
    base["date"] = pd.to_datetime(base["date"]).dt.strftime("%Y-%m-%d")

    a_remplacer, sans_seance, week_end = [], [], []
    for chemin, tk in sorted(couples.items(), key=lambda kv: kv[1]):
        if tk in EXPORTS_FAUX:
            continue
        csv = pd.DataFrame(imp.lire(chemin), columns=["date", "c", "v"])
        if csv.empty:
            continue
        fin = csv["date"].max()
        b = base[base["ticker"] == tk]
        m = b.merge(csv, on="date", how="inner")
        ecart_c = (m["close"] - m["c"]).abs() > TOLERANCE_COURS * m["c"]
        ecart_v = ((m["volume"].fillna(0) - m["v"]).abs()
                   > TOLERANCE_VOLUME * m["v"].clip(lower=1))
        for r in m[ecart_c | ecart_v].itertuples():
            a_remplacer.append((tk, r.date, float(r.c), float(r.v or 0)))
        hors = b[(~b["date"].isin(csv["date"])) & (b["date"] <= fin)]
        # Un jour ouvre sans echange (volume nul) n'est pas une anomalie : le
        # titre n'a simplement pas traite. Seules partent les lignes qui
        # pretendent a des echanges que l'export ignore, et celles d'un
        # samedi ou d'un dimanche.
        we_hors = pd.to_datetime(hors["date"]).dt.dayofweek >= 5
        hors = hors[we_hors | (hors["volume"].fillna(0) > 0)]
        sans_seance += [(tk, d) for d in hors["date"]]
        apres = b[b["date"] > fin]
        we = apres[pd.to_datetime(apres["date"]).dt.dayofweek >= 5]
        week_end += [(tk, d) for d in we["date"]]

    print(f"seances a reprendre de l'export : {len(a_remplacer)}")
    print(f"lignes sans seance dans l'export : {len(sans_seance)}")
    print(f"lignes datees d'un week-end apres l'export : {len(week_end)}")
    par_titre = pd.Series([t for t, *_ in a_remplacer]).value_counts()
    print("par titre (reprises) :", par_titre.head(8).to_dict(), "...")

    if not ecrire:
        print("\nMode simulation : rien n'a ete ecrit.")
        return
    cnx = get_connection()
    cnx.execute("""CREATE TABLE IF NOT EXISTS price_cache_quarantaine (
        ticker TEXT, date TEXT, open DOUBLE PRECISION, high DOUBLE PRECISION,
        low DOUBLE PRECISION, close DOUBLE PRECISION, volume DOUBLE PRECISION,
        motif TEXT, mis_le TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    jour = date.today().isoformat()
    _remplacer(cnx, a_remplacer,
               f"remplacee par l export RichBourse (point mensuel ou cours non ajuste), {jour}")
    _quarantaine(cnx, sans_seance, f"aucune seance ce jour dans l export RichBourse, {jour}")
    _quarantaine(cnx, week_end, f"datee d un week-end, {jour}")
    cnx.commit()
    cnx.close()
    print("\nEcrit.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
