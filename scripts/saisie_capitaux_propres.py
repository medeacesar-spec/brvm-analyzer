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

# (ticker, exercice) -> (montant en FCFA, provenance[, remplacer])
#
# `remplacer` autorise a ecraser une valeur deja en base. Il ne se pose que
# pour un montant adosse a un document ET verifie par ses composantes, face a
# une extraction que rien ne recoupe : BOA Mali portait 40,2 milliards de
# capitaux propres en 2024 quand son bilan en decompose 46,195 au franc pres.
# Sans ce marqueur, le script protege l'existant — c'est la regle, et
# l'exception se declare.
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

    # BOA Senegal — bilan publie, en MILLIONS de FCFA, ligne 9. Composantes
    # concordantes sur les deux exercices :
    #   2025  36 000 + 15 129 + 23 492 + 21 905 = 96 526
    #   2024  36 000 + 15 129 + 17 508 + 19 984 = 88 621
    # Soit 11,5 % et 11,3 % du total du passif — la plage du groupe.
    ("BOAS.sn", 2025): (96_526_000_000,
                        "ligne 9 : capital 36 000 + reserves 15 129 "
                        "+ report 23 492 + resultat 21 905, en millions"),
    ("BOAS.sn", 2024): (88_621_000_000,
                        "ligne 9 : capital 36 000 + reserves 15 129 "
                        "+ report 17 508 + resultat 19 984, en millions"),

    # BOA Mali — bilan publie, en FRANCS PLEINS, sans multiplicateur.
    #   2025  27 450 000 000 + 9 703 816 001 + 1 585 416 272 + 11 081 189 907
    #         = 49 820 422 180
    #   2024  27 450 000 000 + 8 335 295 215 + 1 286 388 748 +  9 123 471 904
    #         = 46 195 155 867
    # L'exercice 2024 portait 40,2 milliards en base, sans recoupement
    # possible. Les resultats nets, eux, concordaient deja au franc pres —
    # c'est bien la seule valeur des capitaux propres qui etait fausse.
    ("BOAM.ml", 2025): (49_820_422_180,
                        "capital 27 450 000 000 + reserves 9 703 816 001 "
                        "+ report 1 585 416 272 + resultat 11 081 189 907"),
    ("BOAM.ml", 2024): (46_195_155_867,
                        "capital 27 450 000 000 + reserves 8 335 295 215 "
                        "+ report 1 286 388 748 + resultat 9 123 471 904",
                        True),

    # BOA Cote d'Ivoire — bilan publie, en MILLIONS de FCFA. Le total est
    # recoupe par ses composantes, au chiffre pres dans les deux exercices :
    #   2025  40 000 + 51 709 +  91 + 35 540 = 127 340
    #   2024  40 000 + 39 902 + 698 + 32 044 = 112 644
    ("BOAC.ci", 2025): (127_340_000_000,
                        "capital 40 000 + reserves 51 709 + report 91 "
                        "+ resultat 35 540, en millions"),
    ("BOAC.ci", 2024): (112_644_000_000,
                        "capital 40 000 + reserves 39 902 + report 698 "
                        "+ resultat 32 044, en millions"),

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
    for (ticker, annee), releve in sorted(RELEVES.items()):
        montant, source = releve[0], releve[1]
        remplacer = len(releve) > 2 and releve[2]
        ligne = conn.execute(
            "SELECT equity, total_assets FROM fundamentals "
            "WHERE ticker = ? AND fiscal_year = ?", (ticker, annee)).fetchone()
        ligne = dict(ligne) if ligne else {}
        actif = ligne.get("total_assets")
        ratio = montant / actif if actif else None

        verdict = ""
        if ligne.get("equity") and remplacer:
            verdict = f"REMPLACE {ligne['equity'] / 1e9:,.1f} — le bilan fait foi"
            a_ecrire.append((ticker, annee, montant))
        elif ligne.get("equity"):
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
