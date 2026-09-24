#!/usr/bin/env python3
"""Trois totaux de bilan 2025 que les documents contredisent.

Releves le 24/09/2026 en passant le lecteur d'etats financiers (#189) sur des
documents hors de son etalon, puis lus a la main (cahier, #28 a #31).

  Vivo Energy  la base portait 207 064 380 555 pour 2024 ET 2025 : le
               comparatif recopie. Le document : « TOTAL ACTIF 193 561 278 972
               207 064 380 555 TOTAL PASSIF 193 561 278 972 … » sous l'en-tete
               « Exercice 2025 Exercice 2024 ». Actif = passif.
  Erium        la base portait 18 525 784 452, qui est le « TOTAL ACTIF
               IMMOBILISE » BRUT. Le total du bilan est le net : brut
               25 414 101 911 - amortissements 10 803 021 818 = 14 611 080 094,
               egal au total du passif.
  NSIA Banque  la base portait 2 562 Md. L'etat, en millions sous l'en-tete
               « 2024 2025 », donne 3 073 062 ; le rapport de gestion le
               confirme : « passant de 2 510 milliards FCFA au 31 decembre
               2024 a 3 073 ». Deux lectures du meme document, et +22 %
               annonces : 3 073 062 / 2 510 429 = 1,224.

CE QUI N'EST PAS CORRIGE

  NSIA 2024    la base porte 2 230 Md, le comparatif du document 2 510 Md —
               mais l'etat precise que les donnees 2024 ont ete corrigees
               depuis la publication precedente. Le chiffre retraite n'est pas
               celui publie en 2025 ; on ne le remplace pas sans le document
               2024.
  BICI Benin   la base est JUSTE. La premiere colonne de l'etat est une
               colonne de travail en francs, dont les totaux ne se recoupent
               pas (215,99 Md de capitaux propres pour 135,1 en colonne
               certifiee). Les colonnes certifiees « 31.12.2025 » en millions
               et la page des chiffres cles donnent 1 844,6 et 135,1 : ce que
               la base porte.

Usage :
  python3 scripts/corriger_totaux_bilan_2025.py --simuler
  python3 scripts/corriger_totaux_bilan_2025.py
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

Md = 1_000_000_000

VALEURS = {
    ("SHEC.ci", 2025): {
        "total_assets": 193_561_278_972,   # la base portait 207 064 380 555, le total 2024
        "source": "Etats financiers provisoires au 31/12/2025 - Vivo Energy CI (03/06/2026), en francs",
    },
    ("SIVC.ci", 2025): {
        "total_assets": 14_611_080_094,    # la base portait l'actif immobilise brut
        "source": "Etats financiers - exercice 2025 - Erium CI (26/06/2026), en francs",
    },
    ("NSBC.ci", 2025): {
        "total_assets": 3_073_062_000_000,  # la base portait 2 562 Md
        "source": "Etats financiers et communique - exercice 2025 - NSIA Banque CI (13/05/2026), en millions",
    },
}


def main(simuler: bool) -> None:
    cnx = get_connection()
    ecrits = 0
    for (ticker, exercice), valeurs in sorted(VALEURS.items()):
        champs = {c: v for c, v in valeurs.items() if c != "source"}
        ligne = cnx.execute(
            "SELECT " + ", ".join(champs) + " FROM fundamentals "
            "WHERE ticker = ? AND fiscal_year = ?", (ticker, exercice)).fetchone()
        if ligne is None:
            print(f"\n{ticker} {exercice} — ligne absente, rien n'est ecrit"); continue
        avant = dict(ligne)
        print(f"\n{ticker} {exercice} — {valeurs['source']}")
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
