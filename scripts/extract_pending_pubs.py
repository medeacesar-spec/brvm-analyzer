#!/usr/bin/env python3
"""Extrait les PDFs correspondant aux publications "À intégrer" sur Infos Marché.

Workflow :
  1. Lit publications avec status="À intégrer" (via analysis.publications)
  2. Pour chaque, trouve le report_link correspondant (ticker, fiscal_year, type)
  3. Extract le PDF
  4. Route vers `fundamentals` (annuel) ou `quarterly_data` (trim/sem)

Détecte le quarter depuis le nom de fichier brvm.org :
  - 1er_trimestre → Q=1
  - 2eme_trimestre → Q=2
  - 3eme_trimestre → Q=3
  - 4eme_trimestre → Q=4
  - 1er_semestre → Q=2 (proxy mid-year)
  - 2eme_semestre → Q=4

Usage :
  python3 scripts/extract_pending_pubs.py
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import read_sql_df  # noqa: E402
from data.pdf_extractor import download_and_extract  # noqa: E402
from data.storage import save_fundamentals, save_quarterly_data  # noqa: E402


# Map publications.pub_type → liste des report_links.report_type compatibles
TYPE_MAP = {
    "annuel": ("rapport_annuel", "etats_financiers"),
    "trimestriel": ("rapport_trimestriel",),
    "semestriel": ("rapport_semestriel",),
}


def _detect_quarter(url: str, report_type: str) -> int | None:
    """Retourne le numero de trimestre 1-4 depuis l'URL du PDF.
    Pour les semestriels, mappe S1→Q2, S2→Q4.
    """
    fname = url.rsplit("/", 1)[-1].lower()
    patterns = [
        (r"1er[_-]trimestre", 1),
        (r"2[eé]me[_-]trimestre", 2),
        (r"3[eé]me[_-]trimestre", 3),
        (r"4[eé]me[_-]trimestre", 4),
        (r"1er[_-]semestre", 2),
        (r"2[eé]me[_-]semestre", 4),
    ]
    for pat, q in patterns:
        if re.search(pat, fname):
            return q
    return None


def _find_report_link(ticker: str, fiscal_year: int, pub_type: str):
    """Cherche le report_link correspondant le plus recent (par date dans
    l'URL). Retourne dict ou None.
    """
    types = TYPE_MAP.get(pub_type)
    if not types:
        return None
    placeholders = ",".join(["?"] * len(types))
    df = read_sql_df(
        f"""SELECT id, url, report_type FROM report_links
            WHERE ticker = ? AND fiscal_year = ?
              AND report_type IN ({placeholders})
            ORDER BY url DESC""",  # URL contient YYYYMMDD → tri = plus récent en tête
        params=(ticker, fiscal_year) + types,
    )
    if df.empty:
        return None
    r = df.iloc[0]
    return {"id": int(r["id"]), "url": r["url"], "report_type": r["report_type"]}


def main():
    # 1. Lit les pending depuis analysis.publications
    from analysis.publications import get_publications_with_status
    pubs = get_publications_with_status(limit=200)
    if pubs.empty:
        print("Aucune publication.")
        return
    pending = pubs[pubs["status"] == "À intégrer"].copy()
    if pending.empty:
        print("Aucune publication À intégrer.")
        return

    print(f"Pending : {len(pending)} publication(s)\n")

    n_ok = 0
    n_skip_no_pdf = 0
    n_skip_no_data = 0
    n_err = 0
    summary = []

    for i, (_, p) in enumerate(pending.iterrows(), 1):
        tk = p["ticker"]
        fy = int(p["fiscal_year"]) if p["fiscal_year"] else None
        pt = p["pub_type"]
        title = p.get("title_pretty") or p.get("title") or ""
        label = f"[{i}/{len(pending)}] {tk:8} {fy} {pt:12}"

        if not fy or pt not in TYPE_MAP:
            print(f"{label} SKIP (type/year non gere)")
            n_skip_no_pdf += 1
            continue

        link = _find_report_link(tk, fy, pt)
        if not link:
            print(f"{label} SKIP (pas de PDF dans report_links)")
            n_skip_no_pdf += 1
            summary.append((tk, fy, pt, "no_pdf", title))
            continue

        try:
            result = download_and_extract(link["url"], use_ocr=False)
        except Exception as e:
            print(f"{label} ERREUR download/extract : {e}")
            n_err += 1
            summary.append((tk, fy, pt, f"err: {e}", title))
            continue

        if result.get("error"):
            print(f"{label} ERREUR : {result['error']}")
            n_err += 1
            summary.append((tk, fy, pt, f"err: {result['error']}", title))
            continue

        revenue = result.get("revenue")
        net_income = result.get("net_income")
        ebit = result.get("ebit")

        # Pas de chiffre clé → skip
        if not any(v is not None for v in (revenue, net_income, ebit)):
            print(f"{label} SKIP (parser n'a rien extrait)")
            n_skip_no_data += 1
            summary.append((tk, fy, pt, "no_data", title))
            continue

        # Route vers la bonne table
        if pt == "annuel":
            try:
                save_fundamentals({
                    "ticker": tk, "fiscal_year": fy,
                    "revenue": revenue, "net_income": net_income, "ebit": ebit,
                    "equity": result.get("equity"),
                    "total_debt": result.get("total_debt"),
                    "interest_expense": result.get("interest_expense"),
                    "cfo": result.get("cfo"), "capex": result.get("capex"),
                    "dividends_total": result.get("dividends_total"),
                })
                print(f"{label} OK fundamentals (rev={revenue or 0:,.0f}, ni={net_income or 0:,.0f})")
                n_ok += 1
                summary.append((tk, fy, pt, "ok", title))
            except Exception as e:
                print(f"{label} SAVE ERR : {e}")
                n_err += 1
                summary.append((tk, fy, pt, f"save_err: {e}", title))
            continue

        # Trimestriel ou semestriel → quarterly_data
        q = _detect_quarter(link["url"], link["report_type"])
        if q is None:
            print(f"{label} SKIP (quarter non detecte dans URL)")
            n_skip_no_data += 1
            summary.append((tk, fy, pt, "no_quarter", title))
            continue
        try:
            save_quarterly_data({
                "ticker": tk, "fiscal_year": fy, "quarter": q,
                "revenue": revenue, "net_income": net_income, "ebit": ebit,
                "source": "brvm.org PDF", "notes": title,
            })
            print(f"{label} OK quarterly_data Q{q} (rev={revenue or 0:,.0f}, ni={net_income or 0:,.0f})")
            n_ok += 1
            summary.append((tk, fy, pt, f"ok Q{q}", title))
        except Exception as e:
            print(f"{label} SAVE ERR : {e}")
            n_err += 1
            summary.append((tk, fy, pt, f"save_err: {e}", title))

    # Résumé
    print()
    print("=" * 60)
    print(f"OK             : {n_ok}")
    print(f"Skip (no PDF)  : {n_skip_no_pdf}")
    print(f"Skip (no data) : {n_skip_no_data}")
    print(f"Erreurs        : {n_err}")
    if n_skip_no_data or n_err:
        print()
        print("Detail des cas non traites :")
        for tk, fy, pt, status, title in summary:
            if status not in ("ok",) and not status.startswith("ok "):
                print(f"  {tk:8} {fy} {pt:12} : {status}")


if __name__ == "__main__":
    main()
