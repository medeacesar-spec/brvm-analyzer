#!/usr/bin/env python3
"""
Extract financial data from PDF reports stored in `report_links` table
and save extracted fundamentals to the `fundamentals` table.
"""

import os
import sys
import time
import traceback

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.storage import get_report_links, save_fundamentals, get_connection
from data.pdf_extractor import download_and_extract


def main():
    # 1. Fetch all report links
    df = get_report_links()
    print(f"Total reports in DB: {len(df)}")
    if df.empty:
        print("No reports found. Exiting.")
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
    main()
