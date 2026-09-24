#!/usr/bin/env python3
"""Les exercices 2025 lus dans les etats financiers publies sur brvm.org.

POURQUOI CE SCRIPT EXISTE

Huit titres portaient dans `fundamentals` une ligne « exercice 2025 » qui ne
contenait qu'un trimestre : NSIA Banque y affichait 22,4 Md de produits, soit
son T1, la ou l'exercice en compte 112,9. Le cours rapporte a ce resultat
partiel donnait un PER de 72 au lieu de 13. Leurs etats financiers annuels
etaient pourtant publies depuis des mois, et n'avaient jamais ete extraits.

CE QUI EST ECRIT, ET D'OU CELA VIENT

Chaque montant ci-dessous a ete LU dans le document officiel, puis recoupe
avec la colonne comparative du meme document et avec le cumul a neuf mois
deja en base. Les valeurs sont donc citees, jamais deduites. La colonne
`source` porte la date et le nom du document.

Les exercices 2024 corrigés le sont sur la foi de la colonne comparative du
document 2025 — le meme tableau, la meme norme, la meme societe. Plusieurs
s'y revelent partiels a leur tour : le produit net bancaire 2024 de NSIA
valait 97,8 Md et non 72,6, qui est son cumul a neuf mois.

LE CAS DE BIIC BENIN

Son rapport 2025 est en IFRS, et on pouvait craindre que la ligne 2024 de la
base soit en SYSCOHADA — melanger deux referentiels dans une serie n'aurait
rien voulu dire. Le doute est leve par le document lui-meme : il annonce un
PNB 2025 de 52,8 Md « en hausse de 16,5 % sur 2024 retraite », soit 45,3 Md,
exactement ce que la base porte pour 2024. Les deux annees sont donc bien
dans le meme referentiel.

Son resultat net 2024 y est toutefois RETRAITE a 29,058 Md, quand la base
porte 30,341 Md, le chiffre alors publie. La ligne 2024 n'est pas touchee :
un retraitement posterieur ne remplace pas ce qui a ete publie.

Usage :
  python3 scripts/corriger_exercices_2025.py --simuler
  python3 scripts/corriger_exercices_2025.py
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

Md = 1_000_000_000

# (ticker, exercice) -> {champ: valeur}, et la source.
VALEURS = {
    ("NSBC.ci", 2025): {
        "revenue": 112_928_000_000,        # PNB, compte de resultat, en millions
        "net_income": 40_712_000_000,
        "equity": 233_303_000_000,
        "source": "Etats financiers et communique - exercice 2025 - NSIA Banque CI (13/05/2026)",
    },
    ("NSBC.ci", 2024): {
        "revenue": 97_819_000_000,         # colonne comparative du meme tableau
        "source": "idem, colonne 2024 (la base portait 72,6 Md, son cumul a neuf mois)",
    },
    ("ORGT.tg", 2025): {
        "revenue": 186_609_000_000,        # produit net bancaire, en millions
        "net_income": 21_643_000_000,
        "equity": 113_165_000_000,
        "total_assets": 4_014_192_000_000,
        "source": "Etats financiers - exercice 2025 - Oragroup TG (04/05/2026), IFRS consolide",
    },
    ("ORGT.tg", 2024): {
        "revenue": 195_436_000_000,
        "net_income": -44_363_000_000,
        "source": "idem, colonne 2024 (la base portait 159,0 Md et -13,2 Md)",
    },
    ("PALC.ci", 2025): {
        "revenue": 197_629_996_000,        # en milliers dans le document
        "net_income": 15_508_655_000,
        "equity": 142_638_984_000,
        "source": "Etats financiers - exercice 2025 - Palm CI (23/03/2026)",
    },
    ("PALC.ci", 2024): {
        "revenue": 172_182_502_000,
        "net_income": 15_861_643_000,
        "source": "idem, colonne 2024 (la base portait 107,5 Md, une periode partielle)",
    },
    ("SDCC.ci", 2025): {
        "revenue": 189_400_000_000,
        "net_income": 4_663_000_000,
        "total_assets": 472_700_000_000,
        "source": "Etats financiers - exercice 2025 - SODECI (27/04/2026)",
    },
    ("SHEC.ci", 2025): {
        "revenue": 604_978_411_174,
        "net_income": 6_028_125_958,
        "source": "Etats financiers - exercice 2025 - Vivo Energy CI (03/06/2026)",
    },
    ("STBC.ci", 2025): {
        "revenue": 268_020_013_096,        # ventes de marchandises
        "net_income": 36_463_616_375,
        "total_assets": 75_047_177_792,
        "source": "Etats financiers - exercice 2025 - SITAB CI (25/06/2026, annule et remplace)",
    },
    ("STBC.ci", 2024): {
        "revenue": 213_794_216_965,
        "net_income": 44_173_762_491,
        "source": "idem, colonne 2024 (version qui annule et remplace la precedente)",
    },
    ("TTLS.sn", 2025): {
        "revenue": 455_209_000_000,
        "net_income": 6_146_527_000,
        "source": "Etats financiers SYSCOHADA - exercice 2025 - TotalEnergies Marketing SN (30/04/2026)",
    },
    ("FTSC.ci", 2025): {
        "revenue": 32_108_628_000,         # en milliers dans le document
        "net_income": 465_981_000,
        "source": "Etats financiers - exercice 2025 - Filtisac CI (10/06/2026)",
    },
    ("BICB.bj", 2025): {
        "revenue": 52_800_000_000,         # PNB, tableau de bord IFRS (arrondi au dixieme)
        "net_income": 36_236_705_101,      # annexe, au franc
        "equity": 135_118_000_000,
        "total_assets": 1_844_600_000_000,
        "source": "Rapport d'activites annuel et etats financiers IFRS - exercice 2025 - BIIC (19/06/2026)",
    },
    ("FTSC.ci", 2024): {
        "revenue": 30_694_517_000,
        "source": "idem, colonne 2024 (la base portait 18,5 Md, une periode partielle)",
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
        print(f"\n{ticker} {exercice} — {valeurs['source']}")
        for champ, apres in champs.items():
            vieux = avant.get(champ)
            marque = "=" if vieux and abs(vieux / apres - 1) < 0.001 else "->"
            print(f"   {champ:13} {(vieux or 0)/Md:10.3f} Md {marque} {apres/Md:10.3f} Md")
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
    if not simuler:
        cnx.commit()
    cnx.close()
    print(f"\n{len(VALEURS)} ligne(s) examinee(s), {ecrits} ecrite(s)."
          + (" Mode simulation : rien n'a ete ecrit." if simuler else ""))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--simuler", action="store_true")
    main(ap.parse_args().simuler)
