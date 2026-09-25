#!/usr/bin/env python3
"""
Extract financial data from PDF reports stored in `report_links` table
and save extracted fundamentals to the `fundamentals` table.

CE QUE LE SCRIPT A CASSE, ET LES TROIS REGLES QUI L'EN EMPECHENT

La routine de quinzaine l'appelle a chaque passage (etape 2a). Le 14/09/2026,
de 11 h 18 a 11 h 53, il a reecrit 2 145 valeurs de `fundamentals` : il
parcourait TOUS les liens de `report_links` — trimestriels et semestriels
compris — et ecrasait la ligne ANNUELLE de l'exercice avec ce qu'il lisait.
Le chiffre d'affaires de Sonatel 2025 est devenu celui du premier semestre
(958 Md), l'EBITDA 221 Md ; le resultat d'exploitation annuel de NSIA,
d'Ecobank, de la SIB, d'Oragroup, d'ETI... celui d'un trimestre. Retrouve
le 25/09 par `scripts/sonde_vraisemblance.py` et le journal des
fondamentaux, puis corrige.

  1. Seuls les documents ANNUELS (etats financiers, rapports annuels) sont
     lus. Les periodes vont dans `quarterly_data`, par
     `extraire_periodes.py`.
  2. Seuls les TROUS sont combles. Une valeur deja en base a ete recoupee
     (lecteurs d'etats financiers, fiches societe, corrections a deux
     sources) : l'extracteur generaliste, qui ne recoupe rien, ne la
     remplace jamais.
  3. Seuls les documents RECENSES DEPUIS PEU (20 jours par defaut, la
     fenetre de la routine) sont lus. `--tout` relit l'ensemble.
  4. Seuls les exercices RECENTS (l'annee en cours et la precedente) sont
     lus. Une publication nouvelle concerne le dernier exercice clos ; un
     document ancien que le recensement retrouve (la collecte paginee du
     25/09 en a ajoute 286 d'un coup, jusqu'a 2018) releve des lecteurs
     qui recoupent, pas de cet extracteur.

Usage :
  python3 scripts/extract_pdfs.py            # documents annuels des 20 derniers jours
  python3 scripts/extract_pdfs.py --jours 60
  python3 scripts/extract_pdfs.py --tout
"""

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.storage import (get_report_links, save_fundamentals, get_connection,
                          champs_extraits)
from data.pdf_extractor import download_and_extract


ANNUELS = ("rapport_annuel", "etats_financiers")
CHAMPS = ("revenue", "net_income", "equity", "total_debt", "ebit", "interest_expense",
          "cfo", "capex", "dividends_total", "total_assets", "shares")


def _deja_en_base(ticker, fiscal_year) -> set:
    """Les champs deja renseignes pour cet exercice : ils ne seront pas touches."""
    from data.db import read_sql_df
    ligne = read_sql_df("SELECT * FROM fundamentals WHERE ticker = ? AND fiscal_year = ?",
                        params=(ticker, int(fiscal_year)))
    if ligne.empty:
        return set()
    l = ligne.iloc[0]
    return {c for c in ligne.columns if l[c] is not None and l[c] == l[c]}


def main(jours: int = 20, tout: bool = False):
    # 1. Les liens ANNUELS, recenses recemment (voir les trois regles).
    df = get_report_links()
    if df.empty:
        print("No reports found. Exiting.")
        return
    df = df[df["report_type"].isin(ANNUELS)]
    if not tout and "created_at" in df.columns:
        limite = datetime.utcnow() - timedelta(days=jours)
        df = df[df["created_at"] >= limite]
        df = df[df["fiscal_year"] >= datetime.utcnow().year - 1]
    df = df.reset_index(drop=True)
    print(f"Documents annuels a lire : {len(df)}"
          + ("" if tout else f" (recenses depuis {jours} jours)"))
    if df.empty:
        return

    print(df[['ticker', 'report_type', 'fiscal_year', 'url']].to_string())
    print("\n" + "=" * 80)
    print("Starting extraction...\n")

    success_count = 0
    skip_count = 0
    error_count = 0
    errors = []

    for idx, row in df.iterrows():
        ticker = row['ticker']
        fiscal_year = row.get('fiscal_year')
        url = row['url']
        report_type = row.get('report_type', '')
        title = row.get('title', '')

        label = f"[{idx+1}/{len(df)}] {ticker} {fiscal_year} ({report_type})"
        print(f"{label}: downloading & extracting...")

        try:
            result = download_and_extract(url, use_ocr=False)
        except Exception as e:
            print(f"  ERROR downloading: {e}")
            errors.append((ticker, fiscal_year, str(e)))
            error_count += 1
            continue

        if result.get("error"):
            print(f"  ERROR extracting: {result['error']}")
            errors.append((ticker, fiscal_year, result['error']))
            error_count += 1
            continue

        # Check if we got any meaningful data
        fields = ["revenue", "net_income", "equity", "total_debt", "ebit",
                  "interest_expense", "cfo", "capex", "dividends_total"]
        extracted = {f: result.get(f) for f in fields if result.get(f) is not None}

        if not extracted:
            print(f"  SKIP: no financial data extracted")
            skip_count += 1
            continue

        # Build fundamentals record
        fund_data = {
            "ticker": ticker,
            "fiscal_year": int(fiscal_year) if fiscal_year else None,
            "revenue": result.get("revenue"),
            "net_income": result.get("net_income"),
            "equity": result.get("equity"),
            "total_debt": result.get("total_debt"),
            "ebit": result.get("ebit"),
            "interest_expense": result.get("interest_expense"),
            "cfo": result.get("cfo"),
            "capex": result.get("capex"),
            "dividends_total": result.get("dividends_total"),
            "total_assets": result.get("total_assets"),
            "shares": result.get("shares"),
        }
        for _champ, _valeur in champs_extraits(result).items():
            if fund_data.get(_champ) is None:
                fund_data[_champ] = _valeur

        # Regle 2 : ne combler que les trous.
        if fiscal_year:
            presents = _deja_en_base(ticker, fiscal_year)
            fund_data = {k: v for k, v in fund_data.items()
                         if k in ("ticker", "fiscal_year") or k not in presents}
            if all(v is None for k, v in fund_data.items() if k not in ("ticker", "fiscal_year")):
                print("  SKIP: rien a combler")
                skip_count += 1
                continue
            extracted = {k: v for k, v in extracted.items() if k in fund_data}

        try:
            row_id = save_fundamentals(fund_data)
            extracted_str = ", ".join(f"{k}={v:,.0f}" for k, v in extracted.items())
            print(f"  OK (row {row_id}): {extracted_str}")
            success_count += 1
        except Exception as e:
            print(f"  ERROR saving: {e}")
            errors.append((ticker, fiscal_year, f"save error: {e}"))
            error_count += 1

    # Summary
    print("\n" + "=" * 80)
    print("EXTRACTION SUMMARY")
    print(f"  Total reports:  {len(df)}")
    print(f"  Saved OK:       {success_count}")
    print(f"  Skipped (empty): {skip_count}")
    print(f"  Errors:         {error_count}")

    if errors:
        print("\nERROR DETAILS:")
        for ticker, year, msg in errors:
            print(f"  {ticker} {year}: {msg}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jours", type=int, default=20)
    ap.add_argument("--tout", action="store_true", help="relit tous les documents annuels")
    a = ap.parse_args()
    main(a.jours, a.tout)
