#!/usr/bin/env python3
"""Cree la table `news_articles` (revue de presse).

Volontairement distincte de `publications` : cette derniere pilote les alertes
et le circuit d'extraction des etats financiers, on n'y melange pas du
journalisme sous peine de declencher de fausses relances "a integrer".

Idempotent : peut etre rejoue sans risque.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection  # noqa: E402

DDL = [
    """CREATE TABLE IF NOT EXISTS news_articles (
        id           SERIAL PRIMARY KEY,
        source       TEXT NOT NULL,
        url          TEXT NOT NULL UNIQUE,
        title        TEXT NOT NULL,
        published_at TEXT,
        lead         TEXT,
        body         TEXT,
        tickers      TEXT,
        tickers_cites TEXT,
        secteurs     TEXT,
        theme        TEXT,
        created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_news_published ON news_articles (published_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_theme ON news_articles (theme)",
]


def main():
    conn = get_connection()
    try:
        for sql in DDL:
            conn.execute(sql)
            print("OK :", sql.split("\n")[0][:70], flush=True)
        conn.commit()
        cur = conn.execute("SELECT COUNT(*) FROM news_articles")
        print("lignes en table :", cur.fetchone()[0], flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
