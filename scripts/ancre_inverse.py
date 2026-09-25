#!/usr/bin/env python3
"""Lire l'exercice N-1 dans le document de l'exercice N, en cascade.

POURQUOI

Chaque etat financier porte DEUX exercices : le sien et son comparatif. Le
lecteur (`analysis/lecture_syscohada.py`) trouve la colonne de l'exercice en
reperant l'exercice PRECEDENT, deja connu : c'est l'ancre. Or les trous de la
base sont surtout dans les exercices ANCIENS — les capitaux propres sont
connus pour 43 titres sur 48 en 2025, 13 en 2021. L'ancre classique ne sert
donc pas la ou il faut.

L'ancre inversee part de ce qu'on sait de l'exercice N et lit, dans le
document N, la colonne voisine : l'exercice N-1. La fonction qui trouve la
colonne rend deja le VOISIN de celle qui retombe sur l'ancre, quel que soit
l'ordre des colonnes — NSIA ecrit le comparatif d'abord, Palm CI l'inverse.

LA CASCADE

Une valeur N-1 confirmee devient a son tour l'ancre du document N-1, qui
donne N-2, et ainsi de suite. Le calcul tourne en memoire jusqu'a ce qu'il
ne trouve plus rien.

CE QUI S'ECRIT, ET CE QUI SE PROPOSE

Une lecture par ancre inversee est SURE quant a la colonne, mais une seule
source. Elle ne s'ecrit que si une SECONDE source la confirme a un pour
mille :
  - le document N-1 lui-meme, lu dans sa propre colonne de l'exercice (deux
    documents independants disent la meme chose) ;
  - ou la fiche societe, pour le chiffre d'affaires et le resultat.
Le reste est propose, jamais ecrit.

Usage :
  python3 scripts/ancre_inverse.py                 # simulation, le defaut
  python3 scripts/ancre_inverse.py --ecrire
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.lecture_syscohada import lire_detaille  # noqa: E402
from data.db import get_connection, read_sql_df  # noqa: E402

_ici = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "recouper_par_lecteur", os.path.join(_ici, "recouper_par_lecteur.py"))
R = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(R)

DEBUT, FIN = 2021, 2025
# Les champs dont la definition ne varie pas d'un document a l'autre : un
# total, pas un sous-poste. Les flux (investissements, tresorerie) et les
# charges d'interets changent de libelle et de perimetre d'un emetteur a
# l'autre ; ils restent a la lecture directe.
CHAMPS = ("revenue", "net_income", "equity", "total_assets", "ebit", "ebitda",
          "total_debt", "deposits", "loans", "cost_of_risk",
          "operating_expenses", "gross_operating_income")
FICHE = ("revenue", "net_income")


def _egal(a, b):
    return a is not None and b is not None and \
        abs(abs(a) - abs(b)) <= max(0.001 * abs(b), 1e6)


def documents() -> dict:
    """{(titre, exercice): texte} — un document par exercice, SYSCOHADA d'abord."""
    fichiers = sorted((f for f in os.listdir(R.DOSSIER)
                       if R.FICHIER.match(f) and not f.startswith(".")),
                      key=lambda f: (R.FICHIER.match(f).groups(), "ifrs" in f.lower(), f))
    textes = {}
    for f in fichiers:
        titre, an = R.FICHIER.match(f).groups()
        cle = (titre, int(an))
        if cle in textes:
            continue
        try:
            t, _ = R.texte(os.path.join(R.DOSSIER, f), True)
        except Exception as err:                                # noqa: BLE001
            print(f"  {f[:60]} illisible : {err}")
            continue
        if t:
            textes[cle] = t
    return textes


def main(ecrire: bool) -> None:
    base = read_sql_df("SELECT ticker, fiscal_year, " + ", ".join(CHAMPS)
                       + " FROM fundamentals")
    connu = {}
    for r in base.itertuples():
        for c in CHAMPS:
            v = getattr(r, c)
            if v == v and v:
                connu[(r.ticker, int(r.fiscal_year), c)] = float(v)
    en_base = dict(connu)

    textes = documents()
    print(f"{len(textes)} documents lus\n")

    # Lecture DIRECTE de chaque document, une fois : la seconde source.
    directe = {}
    for (titre, an), t in textes.items():
        ancres = {c: connu[(titre, an - 1, c)] for c in CHAMPS
                  if (titre, an - 1, c) in connu}
        for c, (v, _) in lire_detaille(t, ancres=ancres).items():
            directe[(titre, an, c)] = v

    trouves = {}          # (titre, an, champ) -> (valeur, document source)
    passe = 0
    while True:
        passe += 1
        nouveaux = 0
        for (titre, an), t in textes.items():
            cible = an - 1
            if not DEBUT <= cible <= FIN:
                continue
            ancres = {c: connu[(titre, an, c)] for c in CHAMPS
                      if (titre, an, c) in connu and (titre, cible, c) not in connu}
            if not ancres:
                continue
            lu = lire_detaille(t, ancres=ancres)
            for c in ancres:
                if c not in lu or lu[c][1] != "ancre":
                    continue
                v = lu[c][0]
                # La colonne voisine ne doit pas etre l'ancre elle-meme : un
                # document qui repete le meme montant sur deux colonnes ne
                # dit rien de l'exercice precedent.
                if _egal(v, ancres[c]):
                    continue
                connu[(titre, cible, c)] = v
                trouves[(titre, cible, c)] = (v, an)
                nouveaux += 1
        print(f"passe {passe} : {nouveaux} valeur(s)")
        if not nouveaux:
            break

    fiches = R._fiches({t for (t, _, c) in trouves if c in FICHE})
    confirme, propose = [], []
    for (titre, an, c), (v, source) in sorted(trouves.items()):
        seconde = None
        if _egal(v, directe.get((titre, an, c))):
            seconde = f"document {an}"
        fiche = ((fiches.get(titre) or {}).get(an) or {}).get(c) if c in FICHE else None
        if seconde is None and _egal(v, fiche):
            seconde = "fiche"
        # Le signe : l'OCR le perd. On garde celui de la fiche, sinon du
        # document direct, sinon celui lu.
        if fiche is not None and _egal(v, fiche):
            v = abs(v) if fiche > 0 else -abs(v)
        ligne = (titre, an, c, v, source, seconde)
        (confirme if seconde else propose).append(ligne)

    Md = 1e9
    for titre_bloc, lignes in (("CONFIRMEES — deux sources, ecrites avec --ecrire", confirme),
                               ("PROPOSEES — une seule source, a lire", propose)):
        print(f"\n{titre_bloc} : {len(lignes)}")
        for titre, an, c, v, source, seconde in lignes:
            print(f"  {titre:9} {an} {c:24} {v/Md:12.3f} Md  "
                  f"(document {source}" + (f" + {seconde})" if seconde else ")"))

    par_champ = {}
    for titre, an, c, *_ in confirme:
        par_champ[c] = par_champ.get(c, 0) + 1
    print(f"\nconfirmees par champ : {par_champ}")

    if not ecrire:
        print("\nMode simulation : rien n'a ete ecrit.")
        return
    cnx = get_connection()
    ecrits = 0
    for titre, an, c, v, source, seconde in confirme:
        if (titre, an, c) in en_base:           # jamais ecraser : on comble
            continue
        # LE SIGNE DES POSTES BANCAIRES. Les charges s'ecrivent en negatif,
        # comme le lecteur bancaire. Le cout du risque peut etre une charge
        # OU une reprise nette : sans l'enchainement RBE -> resultat
        # d'exploitation, que seul `recouper_bancaire.py` verifie, le signe
        # perdu par l'OCR ne se retrouve pas. On ne l'ecrit pas.
        if c == "cost_of_risk":
            print(f"  non ecrit (signe inconnu) : {titre} {an} {c}")
            continue
        if c == "operating_expenses":
            v = -abs(v)
        existe = cnx.execute("SELECT 1 FROM fundamentals WHERE ticker = ? AND fiscal_year = ?",
                             (titre, an)).fetchone()
        if existe:
            cnx.execute(f"UPDATE fundamentals SET {c} = ? WHERE ticker = ? AND fiscal_year = ?",
                        (v, titre, an))
        else:
            cnx.execute(f"INSERT INTO fundamentals (ticker, fiscal_year, {c}) VALUES (?, ?, ?)",
                        (titre, an, v))
        ecrits += 1
    cnx.commit()
    cnx.close()
    print(f"\n{ecrits} valeur(s) ecrite(s).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
