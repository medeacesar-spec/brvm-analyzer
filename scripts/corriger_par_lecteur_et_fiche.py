#!/usr/bin/env python3
"""Vingt-sept montants que le document ET la fiche societe contredisent.

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

  SITAB 2024     le document ecrit 44 730 358 142 sur trois lignes ; la fiche
                 et la base, 44,174 Md. Mais la base a souvent ete REMPLIE
                 depuis la fiche : « fiche = base » n'est alors qu'une source.
                 Document seul contre fiche seule : a lire, pas a ecrire.

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
    # Resultat 2025 recopie de 2024 : « RESULTAT NET 101 228 101 352 »,
    # variation 124 — l'ordre est 2024 puis 2025.
    ("SGBC.ci", 2025): ({"revenue": 276_048_000_000, "net_income": 101_352_000_000},
                        "fiche 276,0 et 101,352 Md",
                        "Rapport d'activites annuel et etats financiers 2025 - SGCI, en millions"),
    ("STAC.ci", 2021): ({"net_income": 1_119_448_000}, "fiche 1,12 Md",
                        "Etats financiers - exercice 2021 - SETAO CI"),
    # La base portait 358,6 Md : faux d'un facteur 2,4. Document 150,6 Md,
    # fiche 151,1 Md : l'ecart de 0,4 % est une definition — le document
    # donne les ventes de marchandises, la convention retenue pour SITAB
    # 2025, lue et verifiee a la main (#175).
    ("STBC.ci", 2022): ({"revenue": 150_593_180_425}, "fiche 151,1 Md",
                        "Etats financiers certifies et approuves - exercice 2022 - SITAB"),
    ("TTLC.ci", 2025): ({"revenue": 588_709_000_000, "net_income": 9_087_000_000},
                        "fiche 588,7 et 9,1 Md",
                        "Etats financiers approuves - exercice 2025 - TotalEnergies CI"),
    # La ligne 2024 de SIB etait une COPIE de 2023 : 95,6 Md et 43,5 Md.
    ("SIBC.ci", 2024): ({"revenue": 102_763_000_000, "net_income": 50_234_000_000},
                        "fiche 102,8 et 50,2 Md",
                        "Rapport d'activites annuel - exercice 2024 - SIB, en millions"),
    # La base portait 0,165 Md : le resultat 2024 (-165 M), recopie sans son
    # signe.
    ("SAFC.ci", 2025): ({"net_income": 701_000_000}, "fiche 0,701 Md",
                        "Etats financiers - exercice 2025 - SAFCA CI"),
    ("ONTBF.bf", 2024): ({"net_income": 21_471_148_928}, "fiche 21,471 Md",
                         "Rapport annuel de gestion - exercice 2024 - Onatel, en francs"),
    ("TTLS.sn", 2024): ({"net_income": 7_091_000_000}, "fiche 7,091 Md",
                        "Rapport annuel - exercice 2024 - TotalEnergies Senegal, en millions"),
    # --- Lus dans des SCANS, par l'OCR en rangees (passe du 25/09) ----------
    ("BOAM.ml", 2022): ({"revenue": 35_307_377_468}, "fiche 35,307 Md",
                        "Etats financiers certifies 2022 - BOA Mali (scan)"),
    # Quasi-copie de 2024 (36,157 -> 36,159) que la sonde « fige » ne voyait pas.
    ("BOAM.ml", 2025): ({"revenue": 37_996_330_474}, "fiche 37,997 Md",
                        "Etats financiers - exercice 2025 - BOA Mali (scan)"),
    ("BOAN.ne", 2023): ({"net_income": 10_076_732_131}, "fiche 10,077 Md",
                        "Etats financiers - exercice 2023 - BOA Niger (scan)"),
    ("ECOC.ci", 2022): ({"revenue": 99_155_000_000}, "fiche 99,155 Md",
                        "Etats financiers - exercice 2022 - Ecobank CI (scan)"),
    # PERTE : l'OCR perd le signe, la fiche et la base le donnent negatif.
    ("FTSC.ci", 2021): ({"revenue": 41_473_743_000, "net_income": -1_276_336_000},
                        "fiche 41,474 et -1,276 Md",
                        "Etats financiers - exercice 2021 - Filtisac (scan), en milliers"),
    ("NEIC.ci", 2024): ({"revenue": 6_744_255_774}, "fiche 6,744 Md",
                        "Etats financiers approuves - exercice 2024 - NEI-CEDA (scan)"),
    # PERTE, meme remarque : fiche -8,756 Md, base -6,99 Md.
    ("SCRC.ci", 2022): ({"net_income": -8_755_668_861}, "fiche -8,756 Md",
                        "Etats financiers SYSCOHADA - exercice 2022 - Sucrivoire (scan)"),
    ("SDSC.ci", 2022): ({"revenue": 86_997_124_000}, "fiche 86,997 Md",
                        "Etats financiers - exercice 2022 - AGL CI (scan), en milliers"),
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
