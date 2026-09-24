#!/usr/bin/env python3
"""Quatorze montants que le document ET la fiche societe contredisent.

COMMENT ILS ONT ETE TROUVES

`scripts/recouper_par_lecteur.py --sans-scans --fiches` passe le lecteur
d'etats financiers sur les 128 exercices lisibles en texte de
`data/pdf_fondamentaux/`, et ne retient que les valeurs dont la colonne est
SURE (l'exercice precedent connu retombe dans la ligne). Quand une telle
valeur contredit la base, la fiche societe tranche. Ici, pour chaque ligne,
le document et la fiche disent la meme chose, a 2 % pres ; la base, autre
chose. C'est la double confirmation qu'exige ce depot.

QUELQUES CAS PARLANTS

  SGBCI 2022     la base portait 49,3 Md de chiffre d'affaires : un
                 trimestre. Document et fiche : 215,1 Md.
  Sucrivoire     507 Md en base pour 2023 ; 68,1 Md au document et a la fiche.
  SODECI 2023    28,5 Md en base : la « variation du passif circulant »,
                 colonne voisine dans le document, lue par un extracteur
                 precedent. Document 2024 (comparatif) et fiche : 175,5 Md.
                 Cette valeur fausse servait d'ANCRE au lecteur, qui s'est
                 trompe a son tour sur 2024 : une ancre ne vaut que la base
                 qui la fournit.

CE QUI N'EST PAS CORRIGE ICI

  SODECI 2022    fiche 160,7 Md, base 37,1 Md, mais le document 2023 est un
                 scan : il faut la passe OCR pour une seconde source.
  Les ecarts de capitaux propres et de total de bilan : la fiche ne les
  publie pas, il faut les lire a la main.

Usage :
  python3 scripts/corriger_par_lecteur_et_fiche.py --simuler
  python3 scripts/corriger_par_lecteur_et_fiche.py
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

Md = 1_000_000_000

# (titre, exercice) : {champ: montant du document}, fiche societe, document
VALEURS = {
    ("NSBC.ci", 2021): ({"revenue": 76_622_000_000}, "fiche 76,6 Md",
                        "Etats financiers - exercice 2021 - NSIA Banque CI"),
    ("ORGT.tg", 2021): ({"revenue": 187_315_000_000}, "fiche 187,3 Md",
                        "Etats financiers - exercice 2021 - Oragroup"),
    ("SAFC.ci", 2021): ({"revenue": 3_567_000_000}, "fiche 3,6 Md",
                        "Etats financiers - exercice 2021 - SAFCA CI"),
    ("SCRC.ci", 2023): ({"revenue": 68_134_694_000}, "fiche 68,1 Md",
                        "Etats financiers IFRS - exercice 2023 - Sucrivoire"),
    ("SGBC.ci", 2022): ({"revenue": 215_101_000_000, "net_income": 74_612_000_000},
                        "fiche 215,1 et 74,6 Md",
                        "Rapport d'activite 2022 - Societe Generale CI"),
    ("SGBC.ci", 2025): ({"revenue": 276_048_000_000}, "fiche 276,0 Md",
                        "Rapport d'activites annuel et etats financiers 2025 - SGCI"),
    ("STAC.ci", 2021): ({"net_income": 1_119_448_000}, "fiche 1,12 Md",
                        "Etats financiers - exercice 2021 - SETAO CI"),
    ("STBC.ci", 2022): ({"revenue": 150_593_180_425}, "fiche 150,6 Md",
                        "Etats financiers certifies et approuves - exercice 2022 - SITAB"),
    ("STBC.ci", 2024): ({"net_income": 44_730_358_142}, "fiche 44,7 Md",
                        "Etats financiers - exercice 2024 - SITAB"),
    ("TTLC.ci", 2025): ({"revenue": 588_709_000_000, "net_income": 9_087_000_000},
                        "fiche 588,7 et 9,1 Md",
                        "Etats financiers approuves - exercice 2025 - TotalEnergies CI"),
    ("SDCC.ci", 2023): ({"revenue": 175_458_474_000}, "fiche 175,5 Md",
                        "Etats financiers - exercice 2024 - SODECI, colonne 2023, en milliers"),
}


def main(simuler: bool) -> None:
    cnx = get_connection()
    ecrits = 0
    for (ticker, exercice), (champs, fiche, source) in sorted(VALEURS.items()):
        ligne = cnx.execute(
            "SELECT " + ", ".join(champs) + " FROM fundamentals "
            "WHERE ticker = ? AND fiscal_year = ?", (ticker, exercice)).fetchone()
        if ligne is None:
            print(f"\n{ticker} {exercice} — ligne absente, rien n'est ecrit"); continue
        avant = dict(ligne)
        print(f"\n{ticker} {exercice} — {source} · {fiche}")
        for champ, apres in champs.items():
            vieux = avant.get(champ)
            marque = "=" if vieux and abs(vieux / apres - 1) < 0.001 else "->"
            print(f"   {champ:13} {(vieux or 0)/Md:10.4f} Md {marque} {apres/Md:10.4f} Md")
        if simuler:
            continue
        cnx.execute(
            "UPDATE fundamentals SET " + ", ".join(f"{c} = ?" for c in champs)
            + " WHERE ticker = ? AND fiscal_year = ?",
            (*champs.values(), ticker, exercice))
        ecrits += 1
    if not simuler:
        cnx.commit()
    cnx.close()
    print(f"\n{len(VALEURS)} ligne(s) examinee(s), {ecrits} ecrite(s)."
          + (" Mode simulation : rien n'a ete ecrit." if simuler else ""))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--simuler", action="store_true")
    main(ap.parse_args().simuler)
