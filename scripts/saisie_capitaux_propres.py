#!/usr/bin/env python3
"""Capitaux propres releves A LA MAIN sur des bilans que l'OCR ne rend pas.

Sept banques de la cote publient leur bilan en image, et tesseract y separe
les libelles des montants : « CAPITAL SOUSCRIT », « RESERVES », « RESULTAT DE
L'EXERCICE » se suivent sans aucune valeur. Aucun elargissement de vocabulaire
ni aucune regle de colonne ne rattache un poste a un nombre qui n'est pas sur
sa ligne.

Trois de ces bilans ont ete lus a l'oeil. Les inscrire ici, avec leur source
et leur mode d'obtention, vaut mieux que de laisser sept ratios cours/actif
net introuvables — et mieux que de les redemander.

Chaque valeur est RECOUPEE avant d'etre ecrite : la somme des composantes doit
retrouver le total quand le bilan porte les deux, et le rapport aux fonds
propres doit rester dans la plage bancaire (5 a 20 % du total de bilan).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

# (ticker, exercice) -> (montant en FCFA, provenance)
RELEVES = {
    # Coris Bank International BF — BILAN_PUB au 31/12/2025, ligne
    # « 9. Capitaux propres et ressources assimilees », en MILLIONS de FCFA.
    # Recoupe par ses composantes : 32 000 + 30 500 + 137 850 + 64 702
    # + 65 495 = 330 547, a une unite pres du total publie.
    ("CBIBF.bf", 2025): (330_548_000_000, "BILAN_PUB 31/12/2025, ligne 9"),
    ("CBIBF.bf", 2024): (287_734_000_000, "BILAN_PUB 31/12/2025, colonne N-1"),

    # BOA Benin — bilan publie, ligne « RBP_0090 9. CAPITAUX PROPRES ET
    # RESSOURCES ASSIMILEES », en francs pleins. L'exercice ancien est a
    # GAUCHE dans ce document.
    ("BOAB.bj", 2025): (117_506_988_771, "RBP_0090, colonne 31/12/2025"),
    ("BOAB.bj", 2024): (117_396_355_692, "RBP_0090, colonne 31/12/2024"),

    # BOA Burkina — la ligne 9 est VIDE dans ce bilan : le total se
    # reconstitue par ses composantes, capital souscrit + primes + reserves
    # + report a nouveau + resultat de l'exercice.
    ("BOABF.bf", 2025): (126_516_071_556,
                         "44 000 000 000 + 0 + 56 141 255 805 "
                         "+ 7 122 800 590 + 19 252 015 161"),
    ("BOABF.bf", 2024): (129_272_440_868,
                         "44 000 000 000 + 0 + 52 778 372 252 "
                         "+ 10 074 844 931 + 22 419 223 685"),
}

# Un bilan bancaire tient ses fonds propres dans cette plage. Hors d'elle, la
# valeur relevee est fausse ou l'actif l'est.
PLAGE = (0.03, 0.30)


def main(appliquer: bool = False) -> None:
    conn = get_connection()
    print(f"{'ticker':10s} {'ex':>5s} {'capitaux propres':>18s} "
          f"{'actif':>12s} {'ratio':>7s}  provenance")

    a_ecrire = []
    for (ticker, annee), (montant, source) in sorted(RELEVES.items()):
        ligne = conn.execute(
            "SELECT equity, total_assets FROM fundamentals "
            "WHERE ticker = ? AND fiscal_year = ?", (ticker, annee)).fetchone()
        ligne = dict(ligne) if ligne else {}
        actif = ligne.get("total_assets")
        ratio = montant / actif if actif else None

        verdict = ""
        if ligne.get("equity"):
            verdict = f"DEJA {ligne['equity'] / 1e9:,.1f} — non ecrase"
        elif ratio is not None and not (PLAGE[0] <= ratio <= PLAGE[1]):
            verdict = f"REJET : ratio hors plage bancaire"
        elif not ligne:
            verdict = "REJET : aucun exercice en base"
        else:
            a_ecrire.append((ticker, annee, montant))

        print(f"{ticker:10s} {annee:5d} {montant / 1e9:15,.2f} Mds "
              f"{(actif or 0) / 1e9:9,.0f} Mds "
              f"{(ratio * 100 if ratio else 0):6.1f}%  {verdict or source}")

    if not appliquer:
        print(f"\nsimulation — {len(a_ecrire)} valeur(s) a ecrire, "
              f"relancer avec --appliquer")
        conn.close()
        return

    for ticker, annee, montant in a_ecrire:
        conn.execute(
            "UPDATE fundamentals SET equity = ? "
            "WHERE ticker = ? AND fiscal_year = ?", (montant, ticker, annee))
    conn.commit()
    print(f"\n{len(a_ecrire)} valeur(s) ecrite(s)")
    conn.close()


if __name__ == "__main__":
    main(appliquer="--appliquer" in sys.argv)
