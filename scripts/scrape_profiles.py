#!/usr/bin/env python3
"""
Scrape les profils qualitatifs (description, dirigeants, contact, actus)
depuis sikafinance.com pour tous les tickers BRVM.
Alimente les tables company_profiles et company_news.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_tickers
from data.scraper import fetch_company_profile, fetch_company_news
from data.storage import save_company_profile, save_company_news


def main():
    tickers = load_tickers()
    total = len(tickers)
    print(f"Scraping company profiles & news for {total} BRVM tickers\n")

    profiles_ok = 0
    news_ok = 0

    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        name = t.get("name", ticker)
        print(f"[{i+1}/{total}] {ticker} ({name})...", end=" ", flush=True)

        # Profile
        profile = fetch_company_profile(ticker)
        has_desc = bool(profile.get("description"))
        has_dg = bool(profile.get("dg") or profile.get("president"))
        if has_desc or has_dg:
            save_company_profile(profile)
            profiles_ok += 1

        # News
        articles = fetch_company_news(ticker, max_articles=8)
        if articles:
            save_company_news(ticker, articles)
            news_ok += 1

        parts = []
        if has_desc:
            parts.append(f"desc={len(profile['description'])}c")
        dg_name = profile.get("dg") or profile.get("president") or ""
        if has_dg:
            parts.append(f"DG={dg_name[:20]}")
        parts.append(f"news={len(articles)}")
        print(" | ".join(parts) if parts else "no data")

        if i < total - 1:
            time.sleep(0.4)

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total tickers:     {total}")
    print(f"Profiles found:    {profiles_ok}/{total}")
    print(f"News found:        {news_ok}/{total}")


if __name__ == "__main__":
    main()
