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

3. LE MENSUEL (`price_monthly`, lu par les mesures de risque et les tests de
   strategie) n'avait pas ete revu. Sicor et Unilever y portaient encore les
   cours de SAPH et de Sucrivoire (Unilever a 3 755 FCFA en decembre 2016
   pour 19 000). Ailleurs, le collecteur sikafinance avait pris un cours de
   milieu de mois pour octobre ou novembre 2022 (Nestle : 6 200 pour 7 095),
   et SAFCA suivait un autre ajustement de sa division (+1 %, 55 mois).

CE QUE FAIT CE SCRIPT, pour chaque titre :

- une seance presente des deux cotes mais dont le cours ou le volume differe
  prend le cours et le volume de l'export (serie AJUSTEE, coherente sur
  28 ans), et ouverture = plus haut = plus bas = cloture, faute de mieux ;
- une ligne datee d'un jour ou l'export n'a pas de seance, jusqu'a la
  derniere date de l'export, part en quarantaine (`price_cache_quarantaine`) ;
- apres la derniere date de l'export, les lignes datees d'un samedi ou d'un
  dimanche partent en quarantaine ;
- une seance de l'export absente de la base y est ajoutee ;
- au mensuel, jusqu'au mois qui precede la fin de l'export (le dernier peut
  etre incomplet), chaque mois prend la cloture de sa derniere seance et le
  volume cumule de l'export ; un mois sans seance dans l'export part en
  quarantaine (`price_monthly_quarantaine`).

Rien n'est efface : la quarantaine garde chaque ligne et son motif.

Usage :
  python3 scripts/aligner_cours_richbourse.py            # simulation
  python3 scripts/aligner_cours_richbourse.py --ecrire
  python3 scripts/aligner_cours_richbourse.py --titres SICC.ci UNLC.ci --depuis 1998-01-01
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
# Sicor et Unilever : les exports recopies (SAPH, Sucrivoire) ont ete
# remplaces le 26/09/2026 par de vrais telechargements, verifies sur
# sikafinance. L'import refuse desormais un fichier identique a un autre.
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


def _mensuel(tk, csv, mensuel):
    """(lignes a mettre en quarantaine avec leur motif, mois a ecrire).

    Un mois garde UNE ligne, celle qui concorde avec l'export. Le collecteur
    sikafinance datait le mois de sa premiere seance (le 2 ou le 3), l'import
    du 1er : certains mois portaient deux lignes, et un test de strategie y
    aurait compte deux points."""
    fin = csv["date"].max()[:7]
    g = (csv.assign(mois=csv["date"].str[:7]).groupby("mois")
         .agg(c=("c", "last"), v=("v", "sum")))
    g = g[g.index < fin]
    m = mensuel[(mensuel["ticker"] == tk) & (mensuel["mois"] < fin)]
    sortir, ecrire = [], []
    for mois, lot in m.groupby("mois"):
        if mois not in g.index:
            sortir += [(tk, d, "aucune seance ce mois dans l export RichBourse")
                       for d in lot["date"]]
            continue
        c, v = g.loc[mois, "c"], g.loc[mois, "v"]
        bon = ((lot["close"] - c).abs() <= TOLERANCE_COURS * c) & \
              ((lot["volume"].fillna(0) - v).abs() <= TOLERANCE_VOLUME * max(v, 1))
        garde = lot[bon]["date"].iloc[0] if bon.any() else None
        sortir += [(tk, d, "remplacee par l export RichBourse" if garde is None
                    else "doublon du mois") for d in lot["date"] if d != garde]
        if garde is None:
            ecrire.append((tk, f"{mois}-01", float(c), float(v)))
    manquants = g[~g.index.isin(m["mois"])]
    ecrire += [(tk, f"{mois}-01", float(r.c), float(r.v)) for mois, r in manquants.iterrows()]
    return sortir, ecrire


def _mensuel_ecrire(cnx, sortir, ecrire, jour):
    """L'ancienne ligne part en quarantaine, puis la bonne est ecrite."""
    for i in range(0, len(sortir), PAQUET):
        lot = sortir[i:i + PAQUET]
        vals = ",".join(f"('{t}', '{d}', '{m}, {jour}')" for t, d, m in lot)
        cnx.execute(
            "INSERT INTO price_monthly_quarantaine "
            "(ticker, date, open, high, low, close, volume, motif) "
            "SELECT p.ticker, p.date::text, p.open, p.high, p.low, p.close, "
            "p.volume, x.m FROM price_monthly p "
            f"JOIN (VALUES {vals}) AS x(t, d, m) ON p.ticker = x.t AND p.date::text = x.d")
        cnx.execute("DELETE FROM price_monthly p USING (VALUES "
                    + ",".join(f"('{t}', '{d}')" for t, d, _ in lot)
                    + ") AS x(t, d) WHERE p.ticker = x.t AND p.date::text = x.d")
    for i in range(0, len(ecrire), PAQUET):
        vals = ",".join(f"('{t}', '{d}', {c:.6f}, {v:.1f})"
                        for t, d, c, v in ecrire[i:i + PAQUET])
        cnx.execute("INSERT INTO price_monthly (ticker, date, close, volume) "
                    f"VALUES {vals} ON CONFLICT DO NOTHING")


def main(ecrire: bool, titres=None, depuis: str = DEBUT) -> None:
    imp = _importeur()
    couples, _ = imp.correspondance()
    for chemin in imp._fichiers():
        if os.path.basename(chemin) in PROPRIETAIRES:
            couples[chemin] = PROPRIETAIRES[os.path.basename(chemin)]
    if titres:
        couples = {c: t for c, t in couples.items() if t in titres}
    base = read_sql_df(
        "SELECT ticker, date, close, volume FROM price_cache WHERE date >= ?",
        params=(depuis,))
    base["date"] = pd.to_datetime(base["date"]).dt.strftime("%Y-%m-%d")
    mensuel = read_sql_df("SELECT ticker, date, close, volume FROM price_monthly")
    mensuel["date"] = pd.to_datetime(mensuel["date"]).dt.strftime("%Y-%m-%d")
    mensuel["mois"] = mensuel["date"].str[:7]

    a_remplacer, sans_seance, week_end, a_ajouter = [], [], [], []
    mois_sortir, mois_ecrire = [], []
    for chemin, tk in sorted(couples.items(), key=lambda kv: kv[1]):
        csv = pd.DataFrame(imp.lire(chemin), columns=["date", "c", "v"])
        csv = pd.DataFrame(imp._borner(tk, list(csv.itertuples(index=False, name=None))),
                           columns=["date", "c", "v"])
        if csv.empty:
            continue
        o, e = _mensuel(tk, csv, mensuel)
        mois_sortir += o
        mois_ecrire += e
        csv = csv[csv["date"] >= depuis]
        if csv.empty:
            continue
        fin = csv["date"].max()
        b = base[base["ticker"] == tk]
        a_ajouter += [(tk, d, float(c), float(v)) for d, c, v in
                      csv[~csv["date"].isin(b["date"])].itertuples(index=False)]
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
    print(f"seances de l'export absentes de la base : {len(a_ajouter)}")
    print(f"lignes mensuelles a retirer : {len(mois_sortir)} "
          f"{pd.Series([m for *_, m in mois_sortir], dtype=object).value_counts().to_dict()}")
    print(f"mois a ecrire depuis l'export : {len(mois_ecrire)}")
    for nom, lot in (("reprises", a_remplacer), ("ajouts", a_ajouter),
                     ("mois ecrits", mois_ecrire)):
        if lot:
            par_titre = pd.Series([t for t, *_ in lot]).value_counts()
            print(f"par titre ({nom}) :", par_titre.head(8).to_dict())

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
    for i in range(0, len(a_ajouter), PAQUET):
        vals = ",".join(f"('{t}', '{d}', {c:.6f}, {v:.1f})"
                        for t, d, c, v in a_ajouter[i:i + PAQUET])
        cnx.execute("INSERT INTO price_cache (ticker, date, close, volume) "
                    f"VALUES {vals} ON CONFLICT DO NOTHING")
    cnx.commit()
    cnx.execute("""CREATE TABLE IF NOT EXISTS price_monthly_quarantaine (
        ticker TEXT, date TEXT, open DOUBLE PRECISION, high DOUBLE PRECISION,
        low DOUBLE PRECISION, close DOUBLE PRECISION, volume DOUBLE PRECISION,
        motif TEXT, mis_le TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    _mensuel_ecrire(cnx, mois_sortir, mois_ecrire, jour)
    cnx.commit()
    cnx.close()
    print("\nEcrit.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    ap.add_argument("--titres", nargs="*", help="limiter a ces titres")
    ap.add_argument("--depuis", default=DEBUT,
                    help="premiere seance revue au quotidien (defaut %(default)s)")
    a = ap.parse_args()
    main(a.ecrire, a.titres, a.depuis)
