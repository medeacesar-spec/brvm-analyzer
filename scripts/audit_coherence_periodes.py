#!/usr/bin/env python3
"""Audit de coherence de `quarterly_data`.

Ne corrige rien : signale. Toute correction passe par une lecture du PDF de
l'emetteur — « recouper et recalculer », jamais recopier.

Trois controles, du plus sur au plus indicatif :

  1. IMPOSSIBLE  — chiffre d'affaires nul ou negatif, resultat net superieur
                   au chiffre d'affaires. Ces valeurs ne peuvent pas etre
                   justes, quelle que soit la convention de l'emetteur.

  2. ECHELLE     — une valeur a plus de N fois (ou N fois moins que) la
                   mediane du meme titre. Attrape les erreurs d'unite, qui
                   sont toujours des facteurs 1000 : millions lus comme
                   milliards, milliers (KXOF) lus comme unites.

  3. MONOTONIE   — sur les emetteurs qui publient en CUMULE depuis le debut
                   de l'exercice, T1 <= S1 <= T3 <= T4. Attrape les colonnes
                   voisines prises pour la bonne : comparatif N-1, variation
                   en valeur, exercice complet.

Le controle 3 ne vaut PAS pour tout le monde. Verifie au PDF le 09/09/2026 :
SITAB publie des trimestres AUTONOMES depuis 2025 (T3 2025 = 64,8 Md quand
S1 = 131,0 Md) mais publiait en cumule en 2023 (T3 2023 = 123,6 Md) ; CFAO
Mobility publie egalement des trimestres autonomes (« ce trimestre, 2 942
vehicules ont ete vendus »). Ces cas sont listes dans AUTONOMES et exclus,
avec la reference du rapport qui l'etablit.

Usage :
  python3 scripts/audit_coherence_periodes.py            # les trois controles
  python3 scripts/audit_coherence_periodes.py --echelle  # un seul
  python3 scripts/audit_coherence_periodes.py --seuil 20
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from data.db import read_sql_df  # noqa: E402

# Les donnees BRVM sont cumulees depuis le debut de l'exercice. T2 et S1
# designent le meme arrete (30 juin) ; T4 et S2 le meme (31 decembre).
RANG = {"T1": 1, "T2": 2, "S1": 2, "T3": 3, "T4": 4, "S2": 4}

# (ticker, exercice) qui publient le TRIMESTRE SEUL et non le cumul.
# Chaque entree a ete verifiee dans le rapport de l'emetteur.
AUTONOMES = {
    ("STBC.ci", 2025): "rapport T3 2025 : CA 64,8 Md contre 131,0 Md au S1",
    ("STBC.ci", 2026): "meme convention que 2025",
    ("CFAC.ci", 2025): "rapport T3 2025 : « ce trimestre, 2 942 vehicules »",
}
# STBC.ci 2023 publiait en CUMULE (T3 2023 = 123,6 Md, volumes 5 364,7 milliers
# de tiges pour un exercice a ~6 700) mais son T4 est autonome : la societe a
# change de convention en cours de route. Son exercice 2023 est donc exclu du
# seul controle T3 -> T4.
CUMUL_PUIS_AUTONOME = {("STBC.ci", 2023)}


def _charger() -> pd.DataFrame:
    d = read_sql_df(
        "SELECT id, ticker, fiscal_year, periode, revenue, net_income "
        "FROM quarterly_data ORDER BY ticker, fiscal_year, periode")
    d["rang"] = d["periode"].map(RANG)
    return d


def controle_impossible(d: pd.DataFrame) -> list:
    out = []
    for _, r in d.iterrows():
        rev, ni = r["revenue"], r["net_income"]
        if pd.notna(rev) and rev <= 0:
            out.append((r, f"chiffre d'affaires nul ou negatif ({rev:,.0f})"))
        if pd.notna(rev) and pd.notna(ni) and rev > 0 and abs(ni) > rev:
            out.append((r, f"resultat net ({ni:,.0f}) > chiffre d'affaires "
                           f"({rev:,.0f})"))
    return out


def controle_echelle(d: pd.DataFrame, seuil: float) -> list:
    out = []
    for _, g in d.groupby("ticker"):
        for col, nom in (("revenue", "chiffre d'affaires"),
                         ("net_income", "resultat net")):
            s = g[col].dropna()
            s = s[s > 0]
            if len(s) < 4:          # trop peu d'historique pour juger
                continue
            med = float(s.median())
            for _, r in g.iterrows():
                v = r[col]
                if pd.isna(v) or v == 0:
                    continue
                v = abs(float(v))
                if v > med * seuil:
                    out.append((r, f"{nom} {v / med:,.0f} fois la mediane du "
                                   f"titre ({med:,.0f})"))
                elif v * seuil < med:
                    out.append((r, f"{nom} {med / v:,.0f} fois plus petit que "
                                   f"la mediane du titre ({med:,.0f})"))
    return out


def controle_monotonie(d: pd.DataFrame) -> list:
    out = []
    ok = d.dropna(subset=["revenue", "rang"])
    for (tk, fy), g in ok.groupby(["ticker", "fiscal_year"]):
        fy = int(fy)
        if (tk, fy) in AUTONOMES:
            continue
        g = g.sort_values("rang")
        prec = None
        for _, r in g.iterrows():
            if prec is not None:
                if (tk, fy) in CUMUL_PUIS_AUTONOME and r["rang"] == 4:
                    prec = r
                    continue
                if r["rang"] == prec["rang"]:
                    base = max(float(prec["revenue"]), 1.0)
                    if abs(r["revenue"] - prec["revenue"]) / base > 0.02:
                        out.append((r, f"meme arrete que {prec['periode']} "
                                       f"({prec['revenue']:,.0f}) mais valeur "
                                       f"differente"))
                elif r["revenue"] < prec["revenue"]:
                    out.append((r, f"cumul en baisse : {prec['periode']}="
                                   f"{prec['revenue']:,.0f} puis "
                                   f"{r['periode']}={r['revenue']:,.0f}"))
            prec = r
    return out


def _afficher(titre: str, lignes: list) -> None:
    print(f"\n=== {titre} : {len(lignes)} ===")
    for r, motif in lignes:
        print(f"  id {int(r['id']):5d}  {r['ticker']:9} {int(r['fiscal_year'])} "
              f"{str(r['periode']):2}  {motif}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--impossible", action="store_true")
    ap.add_argument("--echelle", action="store_true")
    ap.add_argument("--monotonie", action="store_true")
    ap.add_argument("--seuil", type=float, default=10.0,
                    help="facteur d'ecart a la mediane (defaut 10)")
    a = ap.parse_args()
    tous = not (a.impossible or a.echelle or a.monotonie)

    d = _charger()
    print(f"{len(d)} ligne(s), {d['ticker'].nunique()} titre(s)")

    total = 0
    if tous or a.impossible:
        r = controle_impossible(d); total += len(r)
        _afficher("IMPOSSIBLE", r)
    if tous or a.echelle:
        r = controle_echelle(d, a.seuil); total += len(r)
        _afficher(f"ECHELLE (facteur > {a.seuil:.0f})", r)
    if tous or a.monotonie:
        r = controle_monotonie(d); total += len(r)
        _afficher("MONOTONIE DU CUMUL", r)

    print(f"\n{total} signalement(s). Chacun se tranche en lisant le PDF de "
          f"l'emetteur, jamais en divisant par mille.")
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
