#!/usr/bin/env python3
"""Audit des données fondamentales : détecte les valeurs aberrantes.
Usage: python3 scripts/audit_data.py [--fix]

--fix : met automatiquement à NULL les champs aberrants détectés.
"""
import os
import sys
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import get_connection

from config import DB_PATH


ANOMALY_RULES = [
    # (name, predicate lambda, columns to clear)
    (
        "Net margin |NI/Rev| > 100%",
        lambda r, ni, eq, debt, sector: (r and ni and abs(r) > 0 and abs(ni / r) > 1.0),
        ["revenue", "net_income"],
    ),
    (
        "Revenue < 1M (tronqué)",
        lambda r, ni, eq, debt, sector: (r is not None and 0 < abs(r) < 1e6),
        ["revenue"],
    ),
    (
        "Equity < 1M (tronqué)",
        lambda r, ni, eq, debt, sector: (eq is not None and 0 < abs(eq) < 1e6),
        ["equity"],
    ),
    (
        "ROE > 200% (equity positif)",
        lambda r, ni, eq, debt, sector: (ni and eq and eq > 0 and abs(ni / eq) > 2.0),
        ["equity"],
    ),
    (
        "Valeur > 10T FCFA",
        lambda r, ni, eq, debt, sector: any(
            v is not None and abs(v) > 1e13 for v in (r, ni, eq, debt)
        ),
        ["revenue", "net_income", "equity", "total_debt"],
    ),
    (
        "D/E > 20x (hors banque)",
        lambda r, ni, eq, debt, sector: (
            debt and eq and abs(eq) > 0 and abs(debt / eq) > 20
            and "banque" not in (sector or "").lower()
            and "bank" not in (sector or "").lower()
        ),
        ["total_debt"],
    ),
]


def audit(fix: bool = False):
    conn = get_connection()
    rows = conn.execute("""
        SELECT id, ticker, fiscal_year, sector, revenue, net_income,
               equity, total_debt
        FROM fundamentals
    """).fetchall()

    print(f"Audit de {len(rows)} lignes fundamentals…\n")
    anomalies = []
    for row in rows:
        rev = row["revenue"]
        ni = row["net_income"]
        eq = row["equity"]
        debt = row["total_debt"]
        sector = row["sector"]

        for name, pred, cols_to_clear in ANOMALY_RULES:
            try:
                if pred(rev, ni, eq, debt, sector):
                    anomalies.append({
                        "id": row["id"],
                        "ticker": row["ticker"],
                        "year": row["fiscal_year"],
                        "rule": name,
                        "cols": cols_to_clear,
                        "details": {
                            "rev": rev, "ni": ni, "eq": eq, "debt": debt,
                        },
                    })
            except Exception:
                continue

    if not anomalies:
        print("✅ Aucune anomalie détectée.")
        conn.close()
        return

    print(f"⚠️  {len(anomalies)} anomalies détectées :\n")
    for a in anomalies:
        d = a["details"]
        detail_str = ", ".join(
            f"{k}={v:,.0f}" if isinstance(v, (int, float)) and v else f"{k}=NULL"
            for k, v in d.items()
        )
        print(f"  [{a['ticker']:<10} {a['year'] or '?':<6}] {a['rule']:<32} → clear {a['cols']}")
        print(f"    {detail_str}")

    if fix:
        print("\n🔧 Correction en cours…")
        for a in anomalies:
            set_clause = ", ".join(f"{c} = NULL" for c in a["cols"])
            conn.execute(
                f"UPDATE fundamentals SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (a["id"],),
            )
        conn.commit()
        print(f"✅ {len(anomalies)} anomalies corrigées.")
    else:
        print("\n💡 Relancez avec --fix pour nettoyer automatiquement.")

    conn.close()


if __name__ == "__main__":
    fix = "--fix" in sys.argv
    audit(fix=fix)
