#!/usr/bin/env python3
"""Confronte chaque societe A ELLE-MEME, sans reference exterieure.

Un chiffre faux se reconnait rarement seul : 108,7 milliards de produit net
bancaire est un montant parfaitement ordinaire. Il devient suspect quand la
meme banque en declarait 95,6 l'annee d'avant et 8,0 a neuf mois. La cote
n'est pas le bon juge non plus — les societes y sont trop differentes.

Chaque societe est donc son propre temoin. Sept sondes, toutes internes :

  cumul        l'exercice annuel contre son propre cumul a neuf mois
  saut         un poste qui change d'ordre de grandeur d'une annee a l'autre
  fige         le meme montant, au franc pres, sur deux exercices de suite
  signe        un poste qui ne peut pas etre negatif et qui l'est
  identite     produit net bancaire moins charges egale resultat brut
  bilan        les encours d'une banque contre son total de bilan
  dette        la dette financiere contre le passif exigible, et la dette
               d'une banque, qui n'existe pas — ses depots ne sont pas un
               endettement
  periode      un cumul de periode inferieur a celui qui le precede
  distribution une societe qui distribue durablement plus qu'elle ne gagne
  rendement    un rendement que cette place n'a jamais servi

Aucune ne demande de savoir ce que la societe aurait DU publier. Elles ne
disent pas ou est la verite : elles disent ou la base se contredit.
"""

import os
import statistics
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

# Duree couverte par chaque libelle de periode, en mois.
COUVERTURE = {"T1": 3, "S1": 6, "T2": 6, "T3": 9, "T4": 12, "S2": 12}

# Postes qu'aucune societe ne peut afficher en negatif.
JAMAIS_NEGATIFS = ("revenue", "total_assets", "deposits", "loans")

# Postes suivis d'un exercice a l'autre.
SUIVIS = ("revenue", "net_income", "equity", "total_assets", "deposits",
          "loans", "total_debt")


def _mds(valeur):
    return (valeur or 0) / 1e9


def sondes(annuels, periodes):
    """Rend la liste des contradictions, (sonde, ticker, exercice, phrase)."""
    trouves = []

    for ticker, exercices in annuels.items():
        annees = sorted(exercices)

        # --- saut d'ordre de grandeur, et montant fige -------------------
        for poste in SUIVIS:
            serie = [(an, exercices[an].get(poste)) for an in annees
                     if exercices[an].get(poste)]
            for (an1, v1), (an2, v2) in zip(serie, serie[1:]):
                if abs(v2) > abs(v1) * 5 or abs(v2) * 5 < abs(v1):
                    trouves.append((
                        "saut", ticker, an2,
                        f"{poste} passe de {_mds(v1):,.1f} a {_mds(v2):,.1f} Mds "
                        f"entre {an1} et {an2}"))
                elif v1 == v2 and abs(v1) > 1e8:
                    # Deux exercices au franc pres, c'est une valeur recopiee :
                    # aucun poste reel ne se reproduit a l'identique.
                    trouves.append((
                        "fige", ticker, an2,
                        f"{poste} identique en {an1} et {an2} : "
                        f"{_mds(v1):,.2f} Mds"))

        for annee, ligne in exercices.items():
            # --- signe impossible ----------------------------------------
            for poste in JAMAIS_NEGATIFS:
                if (ligne.get(poste) or 0) < 0:
                    trouves.append((
                        "signe", ticker, annee,
                        f"{poste} negatif : {_mds(ligne[poste]):,.2f} Mds"))

            # --- identite du compte de resultat bancaire -----------------
            pnb, charges = ligne.get("revenue"), ligne.get("operating_expenses")
            rbe = ligne.get("gross_operating_income")
            if pnb and charges and rbe:
                attendu = abs(pnb) - abs(charges)
                if abs(attendu - abs(rbe)) > 0.05 * abs(rbe):
                    trouves.append((
                        "identite", ticker, annee,
                        f"PNB {_mds(pnb):,.1f} moins charges {_mds(abs(charges)):,.1f} "
                        f"fait {_mds(attendu):,.1f}, et non {_mds(rbe):,.1f}"))

            # --- la dette financiere contre l'identite du bilan ----------
            # Actif = capitaux propres + passif exigible. La dette financiere
            # n'est qu'une PART de ce passif : le reste, ce sont les
            # fournisseurs, le fiscal et le social. Une dette qui depasse ce
            # plafond a donc absorbe des dettes d'exploitation, ou un total.
            #
            # Ce controle existait dans le lecteur de PDF, ou il compare les
            # trois valeurs d'un MEME document. Il manquait ici : la relecture
            # ciblee n'ecrit qu'un champ, sans revoir les autres, et six
            # valeurs sont passees — dont les 3 704 milliards d'Oragroup, qui
            # sont ses depots. Un garde-fou qui ne vit que dans le lecteur ne
            # protege pas la base.
            dette = ligne.get("total_debt")
            capitaux = ligne.get("equity")
            if dette and (ligne.get("sector") or "").lower().startswith("banq"):
                trouves.append((
                    "dette", ticker, annee,
                    f"une banque n'a pas de dette au sens du ratio : "
                    f"{_mds(dette):,.0f} Mds sont ses ressources clientele"))
            elif dette and ligne.get("total_assets") and capitaux is not None:
                exigible = ligne["total_assets"] - capitaux
                if dette > exigible * 1.02:
                    trouves.append((
                        "dette", ticker, annee,
                        f"dette {_mds(dette):,.1f} superieure au passif "
                        f"exigible {_mds(exigible):,.1f} Mds"))

            # --- coherence de bilan --------------------------------------
            actif = ligne.get("total_assets")
            for poste in ("deposits", "loans"):
                encours = ligne.get(poste)
                if actif and encours and abs(encours) > abs(actif):
                    trouves.append((
                        "bilan", ticker, annee,
                        f"{poste} {_mds(encours):,.0f} depasse le total de bilan "
                        f"{_mds(actif):,.0f} Mds"))

            # --- l'exercice contre son propre cumul a neuf mois ----------
            cumuls = periodes.get((ticker, annee)) or {}
            neuf = cumuls.get("T3")
            if neuf and ligne.get("revenue"):
                extrapole = neuf * 4 / 3
                ecart = ligne["revenue"] / extrapole - 1
                if abs(ecart) > 0.20:
                    trouves.append((
                        "cumul", ticker, annee,
                        f"exercice {_mds(ligne['revenue']):,.1f} contre neuf mois "
                        f"{_mds(neuf):,.1f} extrapoles a {_mds(extrapole):,.1f} "
                        f"({ecart * 100:+.0f} %)"))

    # --- un cumul de periode ne decroit pas ---------------------------------
    for (ticker, annee), cumuls in periodes.items():
        connus = sorted(((COUVERTURE[p], p, v) for p, v in cumuls.items()
                         if p in COUVERTURE and v))
        for (m1, p1, v1), (m2, p2, v2) in zip(connus, connus[1:]):
            # « T2 » et « S1 » couvrent tous deux six mois : ce sont deux
            # lectures de la MEME periode, pas une progression. Les comparer
            # ne prouverait qu'un desaccord entre deux documents, ce que la
            # sonde `saut` dit deja mieux.
            if m2 == m1:
                continue
            if v2 < v1 * 0.98:
                trouves.append((
                    "periode", ticker, annee,
                    f"{p2} ({m2} mois) vaut {_mds(v2):,.1f} Mds, moins que "
                    f"{p1} ({m1} mois) a {_mds(v1):,.1f}"))

    # --- une distribution n'excede pas durablement le resultat --------------
    #
    # Une societe peut payer plus qu'elle ne gagne une annee, en puisant dans
    # ses reserves : c'est une decision, pas une erreur. Deux exercices de
    # suite, ou plus du double du resultat, c'est autre chose — le plus
    # souvent un dividende lu a la mauvaise echelle ou un resultat ampute.
    # Bernabe distribuait 141 fois son resultat 2024 ; le fautif etait le
    # resultat, decale d'un exercice.
    for ticker, exercices in annuels.items():
        excedents = {}
        for annee, ligne in exercices.items():
            dps, actions = ligne.get("dps"), ligne.get("shares")
            resultat = ligne.get("net_income")
            if not (dps and actions and resultat and resultat > 0):
                continue
            rapport = dps * actions / resultat
            if rapport > 1:
                excedents[annee] = rapport
        for annee, rapport in sorted(excedents.items()):
            durable = (annee - 1) in excedents or (annee + 1) in excedents
            if not durable and rapport <= 2:
                continue
            motif = "deux exercices de suite" if durable else "plus du double"
            trouves.append((
                "distribution", ticker, annee,
                f"distribue {rapport:.1f} fois son resultat net "
                f"({_mds(exercices[annee]['dps'] * exercices[annee]['shares']):,.2f} "
                f"contre {_mds(exercices[annee]['net_income']):,.2f} Mds) — {motif}"))

    # --- un rendement que cette place n'a jamais servi ----------------------
    #
    # Au-dela de vingt-cinq pour cent, ce n'est plus un rendement : c'est un
    # dividende lu mille fois trop grand, ou un cours mille fois trop petit.
    # Filtisac a affiche 93 % — le dividende valait 2 001 au lieu de 145.
    # Le cours ne figure pas sur toutes les lignes : on retient le plus recent
    # connu du titre, ce qui suffit a distinguer un rendement d'une aberration.
    for ticker, exercices in annuels.items():
        cours = next((exercices[an].get("price") for an in sorted(exercices, reverse=True)
                      if exercices[an].get("price")), None)
        if not cours or cours <= 0:
            continue
        for annee in sorted(exercices):
            dps = exercices[annee].get("dps")
            if dps and dps / cours > 0.25:
                trouves.append((
                    "rendement", ticker, annee,
                    f"dividende {dps:,.2f} pour un cours de {cours:,.0f} — "
                    f"{100 * dps / cours:.0f} % de rendement"))

    return trouves


def main() -> None:
    conn = get_connection()
    annuels = defaultdict(dict)
    for ligne in conn.execute(
            "SELECT ticker, fiscal_year, revenue, net_income, equity, "
            "total_assets, deposits, loans, operating_expenses, "
            "gross_operating_income, total_debt, sector, "
            "dps, shares, price "
            "FROM fundamentals WHERE fiscal_year >= 2020"):
        ligne = dict(ligne)
        annuels[ligne["ticker"]][ligne["fiscal_year"]] = ligne

    periodes = defaultdict(dict)
    for ligne in conn.execute(
            "SELECT ticker, fiscal_year, periode, revenue FROM quarterly_data "
            "WHERE revenue IS NOT NULL AND revenue <> 0"):
        ligne = dict(ligne)
        periodes[(ligne["ticker"], ligne["fiscal_year"])][
            (ligne["periode"] or "").upper()] = ligne["revenue"]
    conn.close()

    trouves = sondes(annuels, periodes)

    from collections import Counter
    familles = Counter(sonde for sonde, _, _, _ in trouves)
    print(f"{len(annuels)} societes · {len(trouves)} contradictions\n")
    for sonde, nombre in familles.most_common():
        print(f"  {sonde:12s} {nombre:4d}")

    print("\n=== detail ===")
    for sonde, ticker, annee, phrase in sorted(trouves):
        print(f"  {sonde:12s} {ticker:10s} {annee}  {phrase}")


if __name__ == "__main__":
    main()
