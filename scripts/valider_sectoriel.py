#!/usr/bin/env python3
"""
Controle de vraisemblance des valeurs sectorielles, apres extraction.

L'extraction peut se tromper de colonne, de multiplicateur ou de periode sans
que rien ne le signale : la valeur obtenue reste un nombre. Seul le
recoupement la contredit. Quatre controles, tous fondes sur une impossibilite
et non sur un seuil d'opinion :

  1. l'EBITDA ne peut pas depasser le chiffre d'affaires — constate sur Orange
     CI 2025, ou un EBITDA de 424 Mds accompagnait un CA de 197 Mds ;
  2. le cout du risque ne peut pas depasser le produit net bancaire ;
  3. l'identite « PNB - frais generaux = resultat brut d'exploitation » doit
     tomber a 2 % pres quand les trois postes sont presents ;
  4. l'echelle doit rester coherente avec les autres exercices du meme titre —
     constate sur la SIB 2025, dont les montants etaient mille fois trop
     petits (CA de 0,1 Md quand les autres exercices donnent ~90 Mds).

Le script n'efface que la valeur fautive, jamais la ligne. --dry-run montre ce
qui serait annule sans rien ecrire.
"""

import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

FACTEUR_ECHELLE = 20      # au-dela, le multiplicateur a saute
TOLERANCE_IDENTITE = 0.02


def _valeur(ligne, champ):
    v = ligne.get(champ)
    return v if v not in (None, 0) else None


def main(dry_run: bool = False) -> None:
    conn = get_connection()
    lignes = [dict(l) for l in conn.execute(
        "SELECT id, ticker, fiscal_year, sector, revenue, ebitda, cost_of_risk, "
        "operating_expenses, gross_operating_income, deposits, loans "
        "FROM fundamentals "
        "ORDER BY ticker, fiscal_year"
    ).fetchall()]

    # Reference d'echelle : la mediane des chiffres d'affaires connus du titre.
    par_ticker = {}
    for l in lignes:
        ca = _valeur(l, "revenue")
        if ca:
            par_ticker.setdefault(l["ticker"], []).append(abs(ca))

    annulations = []
    for l in lignes:
        ca = _valeur(l, "revenue")
        eb = _valeur(l, "ebitda")
        cdr = _valeur(l, "cost_of_risk")
        chg = _valeur(l, "operating_expenses")
        rbe = _valeur(l, "gross_operating_income")

        if eb and ca and abs(eb) > abs(ca):
            annulations.append((l, ["ebitda"],
                                f"EBITDA {abs(eb)/1e9:,.1f} Mds > CA {abs(ca)/1e9:,.1f} Mds"))
        if cdr and ca and abs(cdr) > abs(ca):
            annulations.append((l, ["cost_of_risk"],
                                f"cout du risque > produit net bancaire"))
        if ca and chg and rbe:
            ecart = abs((abs(ca) - abs(chg)) - abs(rbe)) / abs(rbe)
            if ecart > TOLERANCE_IDENTITE:
                annulations.append((
                    l, ["operating_expenses", "gross_operating_income"],
                    f"identite non tenue ({ecart*100:.0f} % d'ecart)"))

        # Encours : depots et credits se controlent l'un par l'autre ET par
        # l'historique du titre. La SIB portait 1,29 Md de depots en 2024
        # quand ses autres exercices en donnent 1 090 a 1 289 MILLIARDS —
        # facteur mille. Le ratio credits/depots ressortait a 85 548 %, ce
        # que la mediane du secteur masquait et que la moyenne a revele.
        for champ in ("deposits", "loans"):
            valeur = _valeur(l, champ)
            if not valeur:
                continue
            autres = [abs(x[champ]) for x in lignes
                      if x["ticker"] == l["ticker"] and x is not l
                      and _valeur(x, champ)]
            if len(autres) < 2:
                continue
            repere = statistics.median(autres)
            # On ne signale QUE les valeurs trop PETITES. Un multiplicateur
            # perdu divise, il ne multiplie jamais. Et le sens compte : chez
            # Coris Bank, deux exercices sur quatre portent un encours mille
            # fois trop faible ; la mediane bascule alors du mauvais cote et
            # un controle symetrique condamnerait les bonnes annees.
            if repere and abs(valeur) * FACTEUR_ECHELLE < repere:
                annulations.append((
                    l, [champ],
                    f"encours aberrant : {champ} a {abs(valeur)/1e9:,.2f} Mds "
                    f"contre {repere/1e9:,.1f} Mds les autres exercices"))

        # Echelle : on compare aux AUTRES exercices du meme titre.
        autres = [v for v in par_ticker.get(l["ticker"], []) if ca is None or v != abs(ca)]
        if ca and len(autres) >= 2:
            repere = statistics.median(autres)
            if repere and (abs(ca) > FACTEUR_ECHELLE * repere
                           or abs(ca) * FACTEUR_ECHELLE < repere):
                annulations.append((
                    l, ["ebitda", "cost_of_risk", "operating_expenses",
                        "gross_operating_income", "pretax_income"],
                    f"echelle incoherente : CA {abs(ca)/1e9:,.2f} Mds contre "
                    f"{repere/1e9:,.1f} Mds les autres exercices"))

    print(f"Lignes examinees : {len(lignes)}")
    print(f"Anomalies : {len(annulations)}\n")
    for ligne, champs, motif in annulations:
        print(f"  {ligne['ticker']} {ligne['fiscal_year']} — {motif}")
        print(f"      annule : {', '.join(champs)}")

    if dry_run:
        print("\n--dry-run : rien n'a ete modifie.")
        conn.close()
        return

    for ligne, champs, _ in annulations:
        remise = ", ".join(f"{c} = NULL" for c in champs)
        conn.execute(f"UPDATE fundamentals SET {remise} WHERE id = ?", (ligne["id"],))
    conn.commit()

    _consigner(conn, annulations)
    print(f"\n{len(annulations)} anomalies annulees et consignees.")
    conn.close()


def _consigner(conn, annulations) -> None:
    """Garde trace de chaque anomalie, pour permettre une correction ciblee.

    Sans cela, le controle annulait les valeurs impossibles et l'information
    disparaissait avec elles : on savait qu'il avait mordu, jamais sur quoi.
    Corriger supposait de relire les journaux d'execution, quand ils
    existaient encore.

    Une anomalie qui ne reapparait pas au passage suivant est marquee resolue
    plutot que supprimee : la trace de ce qui a ete repare vaut autant que
    celle de ce qui reste a reparer.
    """
    presentes = {(l["ticker"], l["fiscal_year"], m) for l, _, m in annulations}
    try:
        for ligne, champs, motif in annulations:
            conn.execute(
                """INSERT INTO controle_anomalies
                   (ticker, fiscal_year, champs, motif, detectee_le, resolue_le)
                   VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL)
                   ON CONFLICT (ticker, fiscal_year, motif) DO UPDATE SET
                     champs = excluded.champs,
                     detectee_le = CURRENT_TIMESTAMP,
                     resolue_le = NULL""",
                (ligne["ticker"], ligne["fiscal_year"],
                 ", ".join(champs), motif),
            )

        # Ce qui ne ressort plus du controle est repare : on le date.
        ouvertes = [dict(l) for l in conn.execute(
            "SELECT id, ticker, fiscal_year, motif FROM controle_anomalies "
            "WHERE resolue_le IS NULL").fetchall()]
        for a in ouvertes:
            if (a["ticker"], a["fiscal_year"], a["motif"]) not in presentes:
                conn.execute(
                    "UPDATE controle_anomalies SET resolue_le = CURRENT_TIMESTAMP "
                    "WHERE id = ?", (a["id"],))
        conn.commit()
    except Exception as exc:
        # Le journal ne doit jamais empecher le controle de faire son office.
        print(f"consignation impossible ({exc}) — les annulations sont faites",
              flush=True)


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
