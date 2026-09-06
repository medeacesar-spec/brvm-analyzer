#!/usr/bin/env python3
"""
Rattrape les colonnes sectorielles jamais ecrites en base.

Contexte : l'extracteur savait lire l'EBITDA, le cout du risque, les frais
generaux, le resultat brut d'exploitation, le resultat avant impot, les depots
et les credits. Mais les quatre appelants de `save_fundamentals` recopiaient
chacun une liste de champs en dur, etablie avant l'ajout de ces colonnes. Les
valeurs etaient donc extraites puis jetees : 0 ligne renseignee sur 263, et
les grilles sectorielles restaient vides.

Ce script repasse sur les rapports deja references et ecrit ce qui manque.
Il est interruptible : relance-le, il reprend ou il en etait.
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection
from data.pdf_extractor import download_and_extract
from data.storage import save_fundamentals, champs_extraits

SECTORIELS = ("ebitda", "operating_expenses", "gross_operating_income",
              "pretax_income", "deposits", "loans", "cost_of_risk",
              "ordinary_income", "hao_income")

# Seuls ces rapports couvrent l'exercice entier.
TYPES_ANNUELS = {"rapport_annuel", "etats_financiers"}

# Un FLUX se rapporte a une periode : le chiffre d'affaires d'un trimestre
# n'a rien a faire dans une ligne annuelle, et le coefficient d'exploitation
# calcule sur des charges trimestrielles et un PNB annuel ne veut rien dire.
# Un STOCK est une photo a une date : les depots et les credits d'un bulletin
# trimestriel restent la meilleure valeur connue et peuvent etre repris.
POSTES_FLUX = {
    "revenue", "net_income", "ebit", "ebitda", "operating_expenses",
    "gross_operating_income", "pretax_income", "cost_of_risk",
    "ordinary_income", "hao_income", "interest_expense", "cfo", "capex",
    "dividends_total",
}


def _identite_bancaire_tenue(valeurs: dict) -> bool:
    """Verifie PNB - frais generaux = resultat brut d'exploitation.

    Quand les trois postes sont presents, l'identite doit tomber juste. Si
    elle ne tombe pas, l'un des trois a ete lu dans la mauvaise colonne : on
    prefere ne rien ecrire plutot qu'ecrire un jeu incoherent.
    """
    pnb = valeurs.get("revenue")
    charges = valeurs.get("operating_expenses")
    rbe = valeurs.get("gross_operating_income")
    if not (pnb and charges and rbe):
        return True
    attendu = abs(pnb) - abs(charges)
    return abs(attendu - abs(rbe)) <= 0.02 * abs(rbe)


def rapports_a_traiter(conn, depuis: int) -> list:
    lignes = conn.execute(
        "SELECT ticker, fiscal_year, url, report_type FROM report_links "
        "WHERE fiscal_year >= ? AND url IS NOT NULL "
        # Une note de recherche n'est pas un etat financier. Elle aligne
        # l'historique et ses PROJECTIONS sur une meme ligne — « Capitaux
        # propres 108 810 132 524 164 905 189 719 215 330 275 713 359 475
        # 476 382 » chez NSIA Banque — et aucune convention de colonne n'y
        # tient : la premiere valeur est l'annee la plus ancienne, la derniere
        # une prevision a quatre ans. Meme reduits aux postes de bilan, ces
        # documents n'apportent que des chiffres invérifiables.
        "AND report_type <> 'analyse' "
        # Exercices recents d'abord : la fiche d'un titre affiche son
        # dernier exercice renseigne (2025 pour 35 societes sur 48). Traiter
        # ticker par ticker ferait attendre la moitie de la cote.
        "ORDER BY fiscal_year DESC, ticker",
        (depuis,),
    ).fetchall()
    vus, sortie = set(), []
    for ligne in lignes:
        d = dict(ligne)
        cle = (d["ticker"], d["fiscal_year"], d["url"])
        if cle in vus:
            continue
        vus.add(cle)
        sortie.append(d)
    return sortie


def _deja_renseignes(conn, seuil: int) -> set:
    """Exercices portant deja au moins `seuil` postes sectoriels.

    Relire un rapport dont l'exercice est deja renseigne coute une dizaine de
    minutes d'OCR pour reecrire les memes valeurs. Sur 641 documents, c'est
    la difference entre un traitement qui aboutit et un traitement qui expire.
    """
    colonnes = ("operating_expenses", "gross_operating_income", "cost_of_risk",
                "deposits", "loans", "ebitda", "pretax_income")
    compte = " + ".join(
        f"(CASE WHEN {c} IS NOT NULL AND {c} <> 0 THEN 1 ELSE 0 END)"
        for c in colonnes)
    try:
        lignes = conn.execute(
            f"SELECT ticker, fiscal_year FROM fundamentals "
            f"WHERE ({compte}) >= {int(seuil)}"
        ).fetchall()
    except Exception:
        return set()
    return {(dict(l)["ticker"], dict(l)["fiscal_year"]) for l in lignes}


def main(depuis: int = 2023, avec_ocr: bool = True, ouvriers: int = 4,
         lot: int = 0, sur: int = 1, seuil_complet: int = 0) -> None:
    conn = get_connection()
    rapports = rapports_a_traiter(conn, depuis)

    if seuil_complet > 0:
        acquis = _deja_renseignes(conn, seuil_complet)
        avant = len(rapports)
        rapports = [r for r in rapports
                    if (r["ticker"], r["fiscal_year"]) not in acquis]
        print(f"deja renseignes, ecartes : {avant - len(rapports)}", flush=True)
    conn.close()

    if sur > 1:
        # Decoupage deterministe : le lot k prend un document sur `sur`, en
        # partant du k-ieme. Le tri etant par exercice decroissant, chaque lot
        # recoit ainsi un melange d'annees et de societes plutot qu'un bloc
        # homogene — aucun lot ne herite de tous les scans les plus lourds.
        rapports = [r for i, r in enumerate(rapports) if i % sur == lot]
        print(f"lot {lot + 1}/{sur} : {len(rapports)} documents", flush=True)
    print(f"Rapports a repasser (>= {depuis}) : {len(rapports)} "
          f"| {ouvriers} en parallele | OCR {'oui' if avec_ocr else 'non'}",
          flush=True)

    compteurs = {"ecrits": 0, "vides": 0, "erreurs": 0}
    verrou = threading.Lock()
    debut = time.time()
    total = len(rapports)

    def traiter(indice_rapport):
        i, r = indice_rapport
        etiquette = f"[{i}/{total}] {r['ticker']} {r['fiscal_year']}"
        try:
            res = download_and_extract(r["url"], use_ocr=False)
            if not champs_extraits(res) and avec_ocr:
                # Rapport scanne : l'OCR par coordonnees prend le relais.
                res = download_and_extract(r["url"], use_ocr=True)
        except Exception as exc:
            with verrou:
                compteurs["erreurs"] += 1
            print(f"{etiquette} ERREUR {exc}", flush=True)
            return

        if res.get("error"):
            with verrou:
                compteurs["erreurs"] += 1
            print(f"{etiquette} ERREUR {res['error']}", flush=True)
            return

        extraits = champs_extraits(res)

        # Un rapport de periode ne livre que ses postes de bilan.
        annuel = (r.get("report_type") or "") in TYPES_ANNUELS
        if not annuel:
            extraits = {k: v for k, v in extraits.items()
                        if k not in POSTES_FLUX}

        if extraits and not _identite_bancaire_tenue(extraits):
            print(f"{etiquette} REJET identite PNB - frais = RBE non tenue",
                  flush=True)
            extraits = {k: v for k, v in extraits.items()
                        if k not in ("revenue", "operating_expenses",
                                     "gross_operating_income")}

        if not extraits:
            with verrou:
                compteurs["vides"] += 1
            return

        donnees = {"ticker": r["ticker"], "fiscal_year": r["fiscal_year"]}
        donnees.update(extraits)
        # L'ecriture reste serialisee : le pooler Supabase n'aime pas les
        # ecritures concurrentes depuis un meme script, et le gain de temps
        # est du cote du telechargement, pas de l'insertion.
        try:
            with verrou:
                save_fundamentals(donnees)
                compteurs["ecrits"] += 1
        except Exception as exc:
            with verrou:
                compteurs["erreurs"] += 1
            print(f"{etiquette} SAUVEGARDE KO {exc}", flush=True)
            return

        nouveaux = {k: v for k, v in extraits.items() if k in SECTORIELS}
        if nouveaux:
            detail = ", ".join(f"{k}={v:,.0f}" for k, v in nouveaux.items())
            print(f"{etiquette} OK {detail}", flush=True)

    with ThreadPoolExecutor(max_workers=ouvriers) as pool:
        list(pool.map(traiter, enumerate(rapports, 1)))

    duree = time.time() - debut
    print(f"\nEcrits {compteurs['ecrits']} | sans donnee {compteurs['vides']} "
          f"| erreurs {compteurs['erreurs']} | {duree/60:.1f} min", flush=True)


if __name__ == "__main__":
    annee, ouvriers, lot, sur, seuil = 2023, 4, 0, 1, 0
    for a in sys.argv[1:]:
        if a.isdigit():
            annee = int(a)
        elif a.startswith("--ouvriers="):
            ouvriers = int(a.split("=", 1)[1])
        elif a.startswith("--lot="):
            lot = int(a.split("=", 1)[1])
        elif a.startswith("--sur="):
            sur = int(a.split("=", 1)[1])
        elif a.startswith("--deja="):
            seuil = int(a.split("=", 1)[1])
    main(depuis=annee, avec_ocr="--sans-ocr" not in sys.argv, ouvriers=ouvriers,
         lot=lot, sur=sur, seuil_complet=seuil)
