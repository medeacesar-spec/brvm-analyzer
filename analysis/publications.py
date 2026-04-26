"""Helpers d'analyse des publications (richbourse) :
charge les publications avec leur statut d'intégration en base.

Statuts possibles par publication :
  - "Nouveau"     : publications.is_new = 1 (jamais vue avant)
  - "À intégrer"  : annuel dont fiscal_year > max(fundamentals.fiscal_year)
                    OU trimestriel/semestriel non présent dans quarterly_data
  - "Intégré"     : sinon (annuel ou quarterly déjà en base)
  - ""             : pour les publications informationnelles (gouvernance,
                    dividende, autre) — pas d'intégration attendue.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from data.db import read_sql_df


_STATUS_NEW = "Nouveau"
_STATUS_PENDING = "À intégrer"
_STATUS_INTEGRATED = "Intégré"
_STATUS_NA = ""


def get_publications_with_status(
    ticker: Optional[str] = None,
    limit: int = 200,
) -> pd.DataFrame:
    """Charge les publications (filtrées par ticker si fourni) enrichies de :
      - status : "Nouveau" / "À intégrer" / "Intégré" / "" (informationnel)
      - status_icon : "🆕" / "⏳" / "✅" / "·"

    Colonnes retournées : id, ticker, title, pub_type, fiscal_year, url,
    pub_date, is_new, status, status_icon.

    Tri : pub_date DESC NULLS LAST.
    """
    if ticker:
        pubs = read_sql_df(
            """SELECT id, ticker, title, pub_type, fiscal_year, url, pub_date, is_new
               FROM publications
               WHERE ticker = ? AND COALESCE(ignored, 0) = 0
               ORDER BY pub_date DESC NULLS LAST, created_at DESC
               LIMIT ?""",
            params=(ticker, limit),
            parse_dates=["pub_date"],
        )
    else:
        pubs = read_sql_df(
            """SELECT id, ticker, title, pub_type, fiscal_year, url, pub_date, is_new
               FROM publications
               WHERE COALESCE(ignored, 0) = 0
               ORDER BY pub_date DESC NULLS LAST, created_at DESC
               LIMIT ?""",
            params=(limit,),
            parse_dates=["pub_date"],
        )
    if pubs.empty:
        return pubs

    # Indicateurs d'intégration : max(fiscal_year) par ticker dans
    # fundamentals + années présentes dans quarterly_data.
    fund = read_sql_df(
        "SELECT ticker, MAX(fiscal_year) AS max_year FROM fundamentals "
        "WHERE revenue IS NOT NULL GROUP BY ticker"
    )
    fund_map = dict(zip(fund["ticker"], fund["max_year"])) if not fund.empty else {}

    quart = read_sql_df(
        "SELECT DISTINCT ticker, fiscal_year FROM quarterly_data"
    )
    quart_set: set[tuple[str, int]] = set()
    if not quart.empty:
        for _, r in quart.iterrows():
            try:
                quart_set.add((r["ticker"], int(r["fiscal_year"])))
            except (TypeError, ValueError):
                continue

    def _row_status(row) -> str:
        if row.get("is_new"):
            return _STATUS_NEW
        pt = (row.get("pub_type") or "").lower()
        fy = row.get("fiscal_year")
        try:
            fy_int = int(fy) if fy is not None and not pd.isna(fy) else None
        except (TypeError, ValueError):
            fy_int = None
        ticker = row.get("ticker")
        if pt == "annuel" and fy_int is not None:
            mx = fund_map.get(ticker)
            if mx is None or fy_int > int(mx):
                return _STATUS_PENDING
            return _STATUS_INTEGRATED
        if pt in ("trimestriel", "semestriel") and fy_int is not None:
            if (ticker, fy_int) not in quart_set:
                return _STATUS_PENDING
            return _STATUS_INTEGRATED
        # Informationnel (gouvernance, dividende, autre) → pas d'intégration attendue
        return _STATUS_NA

    pubs = pubs.copy()
    pubs["status"] = pubs.apply(_row_status, axis=1)
    icon_map = {
        _STATUS_NEW: "🆕", _STATUS_PENDING: "⏳",
        _STATUS_INTEGRATED: "✅", _STATUS_NA: "·",
    }
    pubs["status_icon"] = pubs["status"].map(icon_map).fillna("·")
    return pubs


def count_pending_for_ticker(ticker: str) -> dict:
    """Retourne {pending: int, new: int} pour les publications du ticker.
    Utilisé pour afficher des KPI compacts (sans devoir recharger toute la table).
    """
    df = get_publications_with_status(ticker=ticker, limit=200)
    if df.empty:
        return {"pending": 0, "new": 0}
    return {
        "pending": int((df["status"] == _STATUS_PENDING).sum()),
        "new": int((df["status"] == _STATUS_NEW).sum()),
    }
