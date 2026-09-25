#!/usr/bin/env python3
"""Collecte les parcs clients des trois operateurs, sur tous leurs rapports.

POURQUOI

La grille telecoms le dit : le parc precede toujours le chiffre d'affaires.
Or `telecom_parcs` ne portait qu'une periode, pour un seul operateur — Sonatel
en 2026 —, parce que l'extraction des parcs n'etait appelee que sur les
publications du fil d'actualite du jour. Les rapports trimestriels,
semestriels et annuels de 2021 a 2026 sont tous dans `report_links`.

CE QUE FAIT LE SCRIPT

Pour chaque rapport de Sonatel, Orange CI et Onatel : telechargement dans
`data/pdf_telecoms/` (une fois), extraction par `extract_from_pdf` — le meme
lecteur qu'en production —, puis rangement a la periode du document : T1, S1,
T3, ou A pour l'exercice.

Un parc de moins de mille ou de plus de deux cents millions d'abonnes n'est
pas un parc : il est ecarte et signale. Rien n'est ecrit sans --ecrire.

LA COHERENCE DE LA SERIE

Le premier passage (25/09/2026) a rendu, a cote de series regulieres, des
valeurs impossibles : Orange CI a 0,03 M au T1 2023 (un tableau « en
milliers » lu sans son unite), une fibre Sonatel a 12 M (le mobile money
lu sur la mauvaise ligne), un mobile money Orange au-dessus du parc total.
Deux regles les ecartent :
  - une valeur a plus de 40 % de la mediane de sa serie (meme operateur,
    meme parc, trois valeurs au moins) ;
  - un sous-parc superieur au parc total de la meme periode.
On ne les corrige pas (multiplier par mille serait deviner) : on les signale.

L'extraction est longue (OCR des rapports annuels). Son resultat est garde
dans `data/pdf_telecoms/.parcs.json` : un second passage ne relit rien.

Usage :
  python3 scripts/collecter_parcs_telecoms.py
  python3 scripts/collecter_parcs_telecoms.py --ecrire
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from data.db import read_sql_df  # noqa: E402
from data.pdf_extractor import extract_from_pdf  # noqa: E402
from data.storage import save_telecom_parcs  # noqa: E402

OPERATEURS = ("SNTS.sn", "ORAC.ci", "ONTBF.bf")
DOSSIER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "pdf_telecoms")
MINI, MAXI = 1_000, 200_000_000
ECART_MEDIANE = 0.4
CACHE = os.path.join(DOSSIER, ".parcs.json")


def _valeur(detail):
    return detail.get("valeur") if isinstance(detail, dict) else detail


def coherents(retenus: list) -> tuple:
    """(retenus, ecartes) apres les deux regles de coherence."""
    series = {}
    ordre = {"T1": 1, "S1": 2, "T3": 3, "A": 4}
    for t, a, per, bons, _ in retenus:
        for parc, d in bons.items():
            series.setdefault((t, parc), []).append(((a, ordre.get(per, 0)), _valeur(d)))
    # LA REFERENCE EST LE NIVEAU RECENT, pas la mediane de toute la serie.
    # Sonatel publiait jusqu'en 2022 un parc mobile du SENEGAL (11 M) et
    # publie depuis 2024 celui du GROUPE (41 M) sous le meme libelle : la
    # mediane historique gardait l'ancien perimetre et ecartait le parc
    # actuel. On juge chaque valeur contre la mediane des quatre plus
    # recentes : un changement de perimetre garde le perimetre en vigueur.
    medianes = {}
    for k, v in series.items():
        if len(v) >= 3:
            recents = [x for _, x in sorted(v, reverse=True)[:4]]
            medianes[k] = sorted(recents)[len(recents) // 2]
    totaux = {}
    for t, a, per, bons, _ in retenus:
        if "parc_total" in bons:
            totaux[(t, a, per)] = _valeur(bons["parc_total"])
    sortie, ecartes = [], []
    for t, a, per, bons, url in retenus:
        gardes = {}
        for parc, d in bons.items():
            v, med = _valeur(d), medianes.get((t, parc))
            total = totaux.get((t, a, per))
            if med and abs(v / med - 1) > ECART_MEDIANE:
                ecartes.append((t, a, per, parc, v, f"mediane {med/1e6:.2f} M"))
            elif parc != "parc_total" and total and v > total:
                ecartes.append((t, a, per, parc, v, f"au-dessus du total {total/1e6:.2f} M"))
            else:
                gardes[parc] = d
        if gardes:
            sortie.append((t, a, per, gardes, url))
    return sortie, ecartes


def periode(url: str, report_type: str) -> str:
    """T1, S1, T3 ou A, d'apres le nom du fichier."""
    nom = url.rsplit("/", 1)[-1].lower()
    if report_type in ("etats_financiers", "rapport_annuel"):
        return "A"
    if re.search(r"(1er|premier)[_-]semestre|semestriel|30[_-]juin|juin", nom):
        return "S1"
    m = re.search(r"([1-4])(?:er|eme|[eè]me|nd|d)?[_-]trimestre", nom)
    if m:
        return "S1" if m.group(1) == "2" else f"T{m.group(1)}"
    return "S1" if report_type == "rapport_semestriel" else "?"


def main(ecrire: bool) -> None:
    liens = read_sql_df(
        "SELECT ticker, fiscal_year, report_type, url FROM report_links "
        "WHERE ticker IN (?, ?, ?) AND fiscal_year >= 2021 ORDER BY ticker, fiscal_year, url",
        params=OPERATEURS)
    os.makedirs(DOSSIER, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0"
    retenus, ecartes, vides = [], [], 0
    cache = json.load(open(CACHE)) if os.path.exists(CACHE) else {}
    for l in liens.itertuples():
        per = periode(l.url, l.report_type)
        chemin = os.path.join(DOSSIER, f"{l.ticker}_{l.fiscal_year}_{l.url.rsplit('/', 1)[-1][:60]}")
        try:
            if not os.path.exists(chemin):
                r = session.get(l.url, timeout=120)
                r.raise_for_status()
                open(chemin, "wb").write(r.content)
            nom = os.path.basename(chemin)
            if nom not in cache:
                cache[nom] = (extract_from_pdf(chemin) or {}).get("parcs") or {}
                json.dump(cache, open(CACHE, "w"))
            parcs = cache[nom]
        except Exception as err:                                # noqa: BLE001
            print(f"  {l.ticker} {l.fiscal_year} {per} : {err}")
            continue
        if not parcs:
            vides += 1
            continue
        bons = {}
        for parc, detail in parcs.items():
            v = detail.get("valeur") if isinstance(detail, dict) else detail
            if v and MINI <= v <= MAXI:
                bons[parc] = detail
            else:
                ecartes.append((l.ticker, l.fiscal_year, per, parc, v))
        if bons:
            retenus.append((l.ticker, int(l.fiscal_year), per, bons, l.url))
            print(f"  {l.ticker:9} {l.fiscal_year} {per:2}  "
                  + " · ".join(f"{p} {(d.get('valeur') if isinstance(d, dict) else d)/1e6:.2f} M"
                               for p, d in sorted(bons.items())))
    print(f"\n{len(liens)} rapports · {len(retenus)} avec parcs · {vides} sans parc lisible")
    for e in ecartes:
        print(f"  ecarte : {e[0]} {e[1]} {e[2]} {e[3]} = {e[4]}")
    retenus, incoherents = coherents(retenus)
    for t, a, per, parc, v, motif in incoherents:
        print(f"  incoherent : {t} {a} {per} {parc} = {v/1e6:.2f} M ({motif})")
    print(f"{sum(len(b) for *_, b, _ in retenus)} parc(s) retenu(s)")
    if not ecrire:
        print("\nMode simulation : rien n'a ete ecrit.")
        return
    n = sum(save_telecom_parcs(t, a, p, b, u) for t, a, p, b, u in retenus)
    # Une valeur jugee incoherente a pu etre ecrite par un passage anterieur
    # (ou par le fil d'actualite) : on la retire, sauf si une valeur coherente
    # vient d'etre ecrite a la meme place.
    ecrits = {(t, a, p, parc) for t, a, p, b, _ in retenus for parc in b}
    from data.db import get_connection
    conn = get_connection()
    retires = 0
    try:
        for t, a, per, parc, v, _ in incoherents:
            if (t, a, per, parc) in ecrits:
                continue
            conn.execute("DELETE FROM telecom_parcs WHERE ticker = ? AND fiscal_year = ? "
                         "AND periode = ? AND parc = ?", (t, a, per, parc))
            retires += 1
        conn.commit()
    finally:
        conn.close()
    print(f"\n{n} parc(s) ecrit(s), {retires} valeur(s) incoherente(s) retiree(s).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ecrire", action="store_true")
    main(ap.parse_args().ecrire)
