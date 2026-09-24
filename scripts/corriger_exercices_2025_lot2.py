#!/usr/bin/env python3
"""Second lot d'exercices lus dans les etats financiers publies sur brvm.org.

Suite de `corriger_exercices_2025.py`. Sept titres affichaient encore un PER
improbable. Les lire un par un a montre que TOUS NE SONT PAS DES ERREURS :

  Erium        PER 97  — la base est JUSTE. 10,1 Md de produits, 179 M de
                         resultat : le rapport le confirme ligne pour ligne.
  AGL          PER 187 — l'exercice 2025 est JUSTE lui aussi. Le resultat
                         tombe de 21,1 Md a 785 M, c'est un effondrement reel.
                         Seule sa ligne 2024 est fausse : elle porte le
                         premier semestre.
  Bernabe      PER 1689 — le resultat 2025 vaut 22,3 M et non 7,3 M, qui est
                         celui de 2024. Le PER reste eleve : la societe gagne
                         reellement tres peu.
  BOA Niger    PER 235 — la base est JUSTE. Le rapport des commissaires aux
                         comptes ecrit « un resultat net beneficiaire de
                         409 millions FCFA » : le chiffre tient.
  SICOR        cas a part, traite en fin de script : le document range sous
                         « exercice 2025 » est le rapport annuel 2024.

C'est le sens de la verification : un PER improbable n'est pas toujours une
erreur de donnee, et l'ecrire sans avoir lu le document aurait remplace un
faux chiffre par un autre.

Usage :
  python3 scripts/corriger_exercices_2025_lot2.py --simuler
  python3 scripts/corriger_exercices_2025_lot2.py
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

Md = 1_000_000_000

VALEURS = {
    ("BOAN.ne", 2025): {
        "total_assets": 291_750_000_000,   # seule donnee qui manquait
        "source": "Rapport des commissaires aux comptes - exercice 2025 - BOA Niger (01/04/2026), en millions",
    },
    ("SMBC.ci", 2025): {
        "revenue": 206_740_000_000,
        "net_income": 13_075_000_000,
        "total_assets": 180_003_000_000,
        "source": "Etats financiers et projet d'affectation - exercice 2025 - SMB CI (12/05/2026), en millions",
    },
    ("BNBC.ci", 2025): {
        "net_income": 22_318_122,          # la base portait 7 313 440, le resultat 2024
        "source": "Etats financiers - exercice 2025 - Bernabe CI (30/04/2026), en francs",
    },
    ("BNBC.ci", 2024): {
        "revenue": 45_312_422_329,         # la base portait 10,8 Md, son T1
        "net_income": 7_313_440,
        "source": "idem, colonne 2024",
    },
    ("SDSC.ci", 2024): {
        "revenue": 85_643_038_000,         # la base portait 42,7 Md, son S1
        "net_income": 21_068_974_000,      # la base portait 18,6 Md, son S1
        "source": "Etats financiers certifies - exercice 2025 - AGL CI (17/09/2026), colonne 2024, en milliers",
    },
    ("LNBB.bj", 2025): {
        "revenue": 98_598_057_859,         # la base portait 57,1 Md, le CA des seuls jeux en ligne
        "net_income": 4_622_243_779,
        "total_assets": 32_608_867_766,
        "source": "Etats financiers - exercice 2025 - LNB Benin (30/04/2026), en francs",
    },
    ("LNBB.bj", 2024): {
        "revenue": 102_543_616_960,        # la base portait 30,1 Md
        "source": "idem, colonne 2024",
    },
}


# SICOR a plus d'un an de retard : le document que `report_links` range sous
# « exercice 2025 » est son rapport annuel 2024, et la ligne 2025 de la base
# porte donc des chiffres de 2024 — avec, en prime, le resultat en positif
# quand l'exercice est une PERTE de 128,6 M. On deplace la ligne sur 2024, on
# corrige les trois montants, et on rectifie l'annee du document pour que la
# prochaine extraction ne refasse pas l'erreur.
SICOR = {
    "revenue": 546_774_960,
    "net_income": -128_639_513,
    "equity": 2_975_325_212,
}
SICOR_DOCUMENT = "rapport_dactivites_annuel_et_etats_financiers_-_exercice_2025_-_sicor"


def corriger_sicor(cnx, simuler: bool) -> None:
    ligne = cnx.execute(
        "SELECT id, revenue, net_income, equity FROM fundamentals "
        "WHERE ticker = ? AND fiscal_year = ?", ("SICC.ci", 2025)).fetchone()
    if ligne is None:
        print("\nSICOR : deja corrige"); return
    avant = dict(ligne)
    print("\nSICC.ci 2025 -> 2024 — Rapport annuel et etats financiers - exercice 2024 - SICOR (12/06/2026)")
    for champ, apres in SICOR.items():
        print(f"   {champ:13} {(avant.get(champ) or 0)/Md:10.4f} Md -> {apres/Md:10.4f} Md")
    existe = cnx.execute("SELECT 1 FROM fundamentals WHERE ticker = ? AND fiscal_year = ?",
                         ("SICC.ci", 2024)).fetchone()
    if existe:
        print("   une ligne 2024 existe deja : rien n'est deplace"); return
    if simuler:
        return
    cnx.execute(
        "UPDATE fundamentals SET fiscal_year = 2024, revenue = ?, net_income = ?, "
        "equity = ? WHERE id = ?",
        (SICOR["revenue"], SICOR["net_income"], SICOR["equity"], avant["id"]))
    n = cnx.execute(
        "UPDATE report_links SET fiscal_year = 2024 "
        "WHERE ticker = ? AND fiscal_year = 2025 AND url LIKE ?",
        ("SICC.ci", "%" + SICOR_DOCUMENT + "%")).rowcount
    print(f"   document reclasse en 2024 : {n} lien(s)")


def main(simuler: bool) -> None:
    cnx = get_connection()
    ecrits = 0
    for (ticker, exercice), valeurs in sorted(VALEURS.items()):
        champs = {c: v for c, v in valeurs.items() if c != "source"}
        ligne = cnx.execute(
            "SELECT " + ", ".join(champs) + " FROM fundamentals "
            "WHERE ticker = ? AND fiscal_year = ?", (ticker, exercice)).fetchone()
        avant = dict(ligne) if ligne else {}
        print(f"\n{ticker} {exercice} — {valeurs['source']}")
        for champ, apres in champs.items():
            vieux = avant.get(champ)
            marque = "=" if vieux and abs(vieux / apres - 1) < 0.001 else "->"
            print(f"   {champ:13} {(vieux or 0)/Md:10.4f} Md {marque} {apres/Md:10.4f} Md")
        if simuler:
            continue
        if ligne is None:
            cnx.execute(
                "INSERT INTO fundamentals (ticker, fiscal_year, " + ", ".join(champs) + ") "
                "VALUES (?, ?, " + ", ".join(["?"] * len(champs)) + ")",
                (ticker, exercice, *champs.values()))
        else:
            cnx.execute(
                "UPDATE fundamentals SET " + ", ".join(f"{c} = ?" for c in champs)
                + " WHERE ticker = ? AND fiscal_year = ?",
                (*champs.values(), ticker, exercice))
        ecrits += 1
    corriger_sicor(cnx, simuler)
    if not simuler:
        cnx.commit()
    cnx.close()
    print(f"\n{len(VALEURS)} ligne(s) examinee(s), {ecrits} ecrite(s)."
          + (" Mode simulation : rien n'a ete ecrit." if simuler else ""))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--simuler", action="store_true")
    main(ap.parse_args().simuler)
