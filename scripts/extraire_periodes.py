#!/usr/bin/env python3
"""
Alimente `quarterly_data` depuis les rapports trimestriels et semestriels.

Ces publications etaient collectees — 609 documents — et quasiment jamais
lues : quinze lignes en base. Elles ne font pas reference, l'exercice annuel
seul le fait, mais elles disent la TENDANCE, ce qu'un exercice clos ne montre
qu'un an plus tard.

La periode est deduite du nom du fichier (« 1er trimestre 2026 » -> T1) puis
confrontee au document. On ne compare jamais deux periodes de duree
differente : un premier semestre se compare a un premier semestre.
"""

import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection
from data.pdf_extractor import download_and_extract
from data.storage import save_quarterly_data

_RANGS = {"1er": 1, "1": 1, "premier": 1, "2eme": 2, "2ème": 2, "2nd": 2,
          "deuxieme": 2, "deuxième": 2, "second": 2, "3eme": 3, "3ème": 3,
          "troisieme": 3, "troisième": 3, "4eme": 4, "4ème": 4,
          "quatrieme": 4, "quatrième": 4}


def periode_du_nom(url: str):
    """Retourne ('T1'|'S1'|…, rang) ou (None, None)."""
    nom = url.rsplit("/", 1)[-1].lower()
    semestre = re.search(
        r"(1er|premier|2eme|2ème|2nd|second|deuxieme|deuxième)[_\- ]*semestre", nom)
    if semestre:
        rang = _RANGS.get(semestre.group(1), 1)
        return f"S{rang}", rang
    trimestre = re.search(
        r"(1er|2eme|2ème|3eme|3ème|4eme|4ème|premier|deuxieme|troisieme|quatrieme)"
        r"[_\- ]*trimestre", nom)
    if trimestre:
        rang = _RANGS.get(trimestre.group(1), 1)
        return f"T{rang}", rang
    return None, None


def main(depuis: int = 2023, ouvriers: int = 4) -> None:
    conn = get_connection()
    rapports = [dict(l) for l in conn.execute(
        """SELECT ticker, fiscal_year, url, report_type FROM report_links
           WHERE report_type IN ('rapport_trimestriel', 'rapport_semestriel')
             AND fiscal_year >= ? AND url IS NOT NULL
           ORDER BY fiscal_year DESC, ticker""",
        (depuis,),
    ).fetchall()]
    conn.close()

    liste, vus = [], set()
    for r in rapports:
        periode, rang = periode_du_nom(r["url"])
        if not periode:
            continue
        cle = (r["ticker"], r["fiscal_year"], periode)
        if cle in vus:
            continue
        vus.add(cle)
        r["periode"], r["rang"] = periode, rang
        liste.append(r)

    print(f"publications de periode a lire : {len(liste)}", flush=True)
    compteurs = {"ok": 0, "vide": 0, "err": 0}
    verrou = threading.Lock()
    debut = time.time()

    def traiter(item):
        i, r = item
        try:
            res = download_and_extract(r["url"], use_ocr=False)
            if not (res.get("revenue") or res.get("net_income")):
                res = download_and_extract(r["url"], use_ocr=True)
        except Exception:
            with verrou:
                compteurs["err"] += 1
            return
        if res.get("error"):
            with verrou:
                compteurs["err"] += 1
            return

        ca = res.get("revenue") or res.get("revenue_bank")
        rn = res.get("net_income")
        if not ca and not rn:
            with verrou:
                compteurs["vide"] += 1
            return

        donnees = {
            "ticker": r["ticker"], "fiscal_year": r["fiscal_year"],
            "quarter": r["rang"], "periode": r["periode"],
            "revenue": ca, "net_income": rn, "ebit": res.get("ebit"),
            "source": "brvm.org PDF",
        }
        try:
            with verrou:
                save_quarterly_data(donnees)
                compteurs["ok"] += 1
            print(f"[{i}/{len(liste)}] {r['ticker']} {r['fiscal_year']} "
                  f"{r['periode']} CA={(ca or 0)/1e9:,.1f} "
                  f"RN={(rn or 0)/1e9:,.1f}", flush=True)
        except Exception as exc:
            with verrou:
                compteurs["err"] += 1
            print(f"[{i}] {r['ticker']} sauvegarde KO : {exc}", flush=True)

    with ThreadPoolExecutor(max_workers=ouvriers) as pool:
        list(pool.map(traiter, enumerate(liste, 1)))

    print(f"\nOK {compteurs['ok']} | sans donnee {compteurs['vide']} "
          f"| erreurs {compteurs['err']} | {(time.time()-debut)/60:.1f} min",
          flush=True)


if __name__ == "__main__":
    annee = 2023
    for a in sys.argv[1:]:
        if a.isdigit():
            annee = int(a)
    main(depuis=annee)
