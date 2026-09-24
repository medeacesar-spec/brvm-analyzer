#!/usr/bin/env python3
"""Les neuf alertes du recoupement, verifiees une par une.

D'OU ELLES VIENNENT

`recouper_fiches_societe.py` signale, sans jamais les ecrire, les valeurs ou
la base et la fiche societe divergent d'un facteur dix ou changent de signe.
Neuf cas sont sortis. Chacun a ete instruit : document officiel quand il en
existe un, nos propres periodes trimestrielles sinon.

LE VERDICT : LA FICHE AVAIT RAISON NEUF FOIS SUR NEUF

Et la base se trompait de trois facons differentes :

  - elle portait l'exercice PRECEDENT — Setao 2022 affichait 1,119 Md, qui
    est son resultat 2021 ; NEI-CEDA 2024 affichait 1,109 Md, qui est son
    cumul a neuf mois de 2023 ;
  - elle portait une periode partielle — Uniwax 2022 affichait 21,6 Md de
    chiffre d'affaires pour 36,4 reels ;
  - elle portait un benefice la ou l'exercice etait une PERTE — Safca trois
    fois, Setao, Uniwax. C'est le plus grave : le PER, le score fondamental
    et le classement s'en trouvent inverses.

Deux cas n'ont aucun document publie : Safca 2022 et Erium 2021. Ils sont
corriges sur la seule fiche, et la colonne `source` le dit.

Usage :
  python3 scripts/corriger_alertes_recoupement.py --simuler
  python3 scripts/corriger_alertes_recoupement.py
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

Md = 1_000_000_000

VALEURS = {
    ("NTLC.ci", 2025): {
        "revenue": 233_261_162_741,
        "source": "Etats financiers - exercice 2025 - Nestle CI (30/04/2026) : "
                  "« Sous total Chiffre d'affaires 233 261 162 741 ». La base portait "
                  "0,12 Md quand les neuf premiers mois en faisaient 173,4.",
    },
    ("UNXC.ci", 2022): {
        "revenue": 36_372_536_951,
        "net_income": -1_298_674_972,
        "source": "Etats financiers - exercice 2022 - Uniwax CI (28/04/2023) : "
                  "« CHIFFRE D'AFFAIRES 36 372 536 951 » et « RESULTAT NET DE "
                  "L'EXERCICE - 1 298 674 972 ». La base portait un benefice.",
    },
    ("STAC.ci", 2022): {
        "net_income": -70_544_000,
        "source": "Etats financiers - exercice 2022 - Setao CI (26/04/2023) : "
                  "« Resultat net -70 544 » en milliers, contre 1 119 448 en 2021. "
                  "La base portait le resultat 2021.",
    },
    ("SAFC.ci", 2021): {
        "net_income": -603_000_000,
        "source": "Etats financiers - exercice 2021 - Safca CI (25/04/2022) : "
                  "« RESULTAT NET - 603 » en millions, contre - 1 640 en 2020.",
    },
    ("SAFC.ci", 2024): {
        "net_income": -165_000_000,
        "source": "Rapport d'activites 4e trimestre 2024 - Safca CI (03/07/2026) : "
                  "« le resultat net ressort a -165 millions FCFA, en nette "
                  "amelioration par rapport a la perte de -579 millions en 2023 ».",
    },
    ("NEIC.ci", 2024): {
        "net_income": -760_000_000,
        "source": "Fiche societe. La base portait 1,109 Md, qui est EXACTEMENT le "
                  "cumul a neuf mois de 2023 deja en base (1 109 162 000). Le "
                  "premier semestre 2024 etait deja en perte de 20,5 M.",
    },
    ("NEIC.ci", 2025): {
        "revenue": 5_140_000_000,
        "source": "Fiche societe. La base portait 534,24 Md, impossible : le bilan "
                  "du document 2025 affiche un total general de 6,68 Md, et le "
                  "chiffre d'affaires du titre va de 5 a 9 Md depuis 2020.",
    },
    ("SAFC.ci", 2022): {
        "net_income": -230_000_000,
        "source": "Fiche societe SEULE — aucun etat financier 2022 publie pour ce "
                  "titre. A confirmer au prochain depot.",
    },
    ("SIVC.ci", 2021): {
        "revenue": 7_650_000_000,
        "source": "Fiche societe SEULE — aucun etat financier 2021 publie. La base "
                  "portait 1 692,5 Md quand le titre fait 8 a 10 Md les autres "
                  "annees. A confirmer au prochain depot.",
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
        avant = dict(ligne) if ligne else {}
        print(f"\n{ticker} {exercice}")
        for champ, apres in champs.items():
            vieux = avant.get(champ)
            print(f"   {champ:11} {(vieux or 0)/Md:10.3f} Md -> {apres/Md:10.3f} Md")
        print(f"   {valeurs['source']}")
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
    print(f"\n{len(VALEURS)} alerte(s) instruite(s), {ecrits} ecrite(s)."
          + (" Mode simulation : rien n'a ete ecrit." if simuler else ""))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--simuler", action="store_true")
    main(ap.parse_args().simuler)
