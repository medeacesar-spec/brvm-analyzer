"""
Stockage persistant SQLite pour les données fondamentales,
le portefeuille et le profil investisseur.
Import depuis les fichiers Excel existants.
"""

import json
import os
import sqlite3
from typing import Optional

import pandas as pd

from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Crée les tables si elles n'existent pas."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fundamentals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            company_name TEXT,
            sector TEXT,
            currency TEXT DEFAULT 'XOF',
            fiscal_year INTEGER,
            price REAL,
            shares REAL,
            float_pct REAL,
            revenue REAL,
            net_income REAL,
            equity REAL,
            total_debt REAL,
            ebit REAL,
            interest_expense REAL,
            cfo REAL,
            capex REAL,
            dividends_total REAL,
            dps REAL,
            -- Historique N-3 à N
            revenue_n3 REAL,
            revenue_n2 REAL,
            revenue_n1 REAL,
            revenue_n0 REAL,
            net_income_n3 REAL,
            net_income_n2 REAL,
            net_income_n1 REAL,
            net_income_n0 REAL,
            dps_n3 REAL,
            dps_n2 REAL,
            dps_n1 REAL,
            dps_n0 REAL,
            -- Métadonnées
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, fiscal_year)
        );

        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            company_name TEXT,
            quantity REAL NOT NULL,
            avg_price REAL NOT NULL,
            purchase_date TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS price_cache (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (ticker, date)
        );

        CREATE TABLE IF NOT EXISTS investor_profile (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            risk_profile TEXT,
            horizon TEXT,
            budget REAL,
            preferred_sectors TEXT,
            preferred_tickers TEXT,
            excluded_tickers TEXT,
            objective TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS market_data (
            ticker TEXT PRIMARY KEY,
            company_name TEXT,
            sector TEXT,
            price REAL,
            variation REAL,
            market_cap REAL,
            beta REAL,
            rsi REAL,
            dps REAL,
            dividend_history TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            title TEXT,
            pub_type TEXT,
            period TEXT,
            url TEXT,
            pub_date TEXT,
            is_new INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, title)
        );

        CREATE TABLE IF NOT EXISTS quarterly_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            fiscal_year INTEGER,
            quarter INTEGER,
            revenue REAL,
            net_income REAL,
            ebit REAL,
            source TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, fiscal_year, quarter)
        );

        CREATE TABLE IF NOT EXISTS qualitative_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            category TEXT,
            content TEXT NOT NULL,
            source TEXT,
            note_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS report_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            title TEXT,
            report_type TEXT,
            fiscal_year INTEGER,
            url TEXT NOT NULL,
            source TEXT DEFAULT 'brvm.org',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, url)
        );
    """)
    conn.commit()
    conn.close()


def get_analyzable_tickers() -> list:
    """
    Retourne les tickers qui ont assez de donnees pour etre analyses.
    Un titre est analysable s'il a des fondamentaux (Excel import)
    OU des donnees de marche avec prix + DPS.
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT ticker, company_name, sector, 'fundamentals' as source
        FROM fundamentals
        UNION
        SELECT DISTINCT ticker, company_name, sector, 'market' as source
        FROM market_data
        WHERE price IS NOT NULL AND price > 0
        ORDER BY ticker
    """).fetchall()
    conn.close()

    # Deduplicate by ticker, prefer fundamentals source
    seen = {}
    for r in rows:
        t = r["ticker"]
        if t not in seen or r["source"] == "fundamentals":
            seen[t] = {"ticker": t, "name": r["company_name"], "sector": r["sector"],
                       "has_fundamentals": r["source"] == "fundamentals"}
    return list(seen.values())


# --- Fundamentals CRUD ---

def save_fundamentals(data: dict) -> int:
    """Insère ou met à jour les données fondamentales d'un titre."""
    conn = get_connection()
    cols = [
        "ticker", "company_name", "sector", "currency", "fiscal_year",
        "price", "shares", "float_pct", "revenue", "net_income", "equity",
        "total_debt", "ebit", "interest_expense", "cfo", "capex",
        "dividends_total", "dps",
        "revenue_n3", "revenue_n2", "revenue_n1", "revenue_n0",
        "net_income_n3", "net_income_n2", "net_income_n1", "net_income_n0",
        "dps_n3", "dps_n2", "dps_n1", "dps_n0",
    ]
    values = [data.get(c) for c in cols]

    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    update_clause = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in ("ticker", "fiscal_year"))

    cursor = conn.execute(
        f"""INSERT INTO fundamentals ({col_names})
            VALUES ({placeholders})
            ON CONFLICT(ticker, fiscal_year) DO UPDATE SET
            {update_clause}, updated_at=CURRENT_TIMESTAMP""",
        values,
    )
    conn.commit()
    row_id = cursor.lastrowid
    conn.close()
    return row_id


def get_fundamentals(ticker: str, fiscal_year: Optional[int] = None) -> Optional[dict]:
    """Récupère les données fondamentales d'un titre."""
    conn = get_connection()
    if fiscal_year:
        row = conn.execute(
            "SELECT * FROM fundamentals WHERE ticker=? AND fiscal_year=?",
            (ticker, fiscal_year),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM fundamentals WHERE ticker=? ORDER BY fiscal_year DESC LIMIT 1",
            (ticker,),
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_fundamentals() -> pd.DataFrame:
    """Récupère toutes les données fondamentales (dernière année par titre)."""
    conn = get_connection()
    df = pd.read_sql_query(
        """SELECT f.* FROM fundamentals f
           INNER JOIN (
               SELECT ticker, MAX(fiscal_year) as max_year
               FROM fundamentals GROUP BY ticker
           ) latest ON f.ticker = latest.ticker AND f.fiscal_year = latest.max_year
           ORDER BY f.ticker""",
        conn,
    )
    conn.close()
    return df


def list_tickers_with_fundamentals() -> list:
    """Liste les tickers qui ont des données fondamentales."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT ticker, company_name FROM fundamentals ORDER BY ticker"
    ).fetchall()
    conn.close()
    return [{"ticker": r["ticker"], "name": r["company_name"]} for r in rows]


# --- Excel Import ---

def import_from_excel(filepath: str) -> dict:
    """
    Importe les données depuis un fichier Excel au format 'Analyse Hybride'.
    Lit la feuille 'Inputs' et retourne un dict compatible avec save_fundamentals.
    """
    import openpyxl

    wb = openpyxl.load_workbook(filepath, data_only=True)
    ws = wb["Inputs"]

    def cell_val(row, col=2):
        return ws.cell(row=row, column=col).value

    data = {
        "company_name": cell_val(4),
        "ticker": _resolve_ticker(cell_val(5)),
        "sector": cell_val(6),
        "currency": cell_val(7) or "XOF",
        "fiscal_year": cell_val(8),
        "price": cell_val(9),
        "shares": cell_val(10),
        "revenue": cell_val(13),
        "net_income": cell_val(14),
        "equity": cell_val(15),
        "total_debt": cell_val(16),
        "ebit": cell_val(17),
        "interest_expense": cell_val(18),
        "cfo": cell_val(19),
        "capex": cell_val(20),
        "dividends_total": cell_val(21),
        "dps": cell_val(22),
    }

    # Parse float percentage from column C row 10
    float_text = ws.cell(row=10, column=3).value
    if float_text and isinstance(float_text, str) and "%" in float_text:
        try:
            data["float_pct"] = float(float_text.replace("%", "").replace(",", ".").strip())
        except ValueError:
            data["float_pct"] = None
    else:
        data["float_pct"] = None

    # Historical data (columns C=N-3, D=N-2, E=N-1, F=N)
    hist_cols = {3: "n3", 4: "n2", 5: "n1", 6: "n0"}
    for col_idx, suffix in hist_cols.items():
        data[f"revenue_{suffix}"] = ws.cell(row=26, column=col_idx).value
        data[f"net_income_{suffix}"] = ws.cell(row=27, column=col_idx).value
        data[f"dps_{suffix}"] = ws.cell(row=28, column=col_idx).value

    wb.close()
    return data


def _resolve_ticker(mnemo: str) -> str:
    """Convertit un mnémonique (ECOC) en ticker complet (ECOC.ci)."""
    if not mnemo:
        return ""
    mnemo = mnemo.strip()
    if "." in mnemo:
        return mnemo

    # Map common mnemonics to full tickers
    from config import load_tickers
    tickers = load_tickers()
    for t in tickers:
        if t["ticker"].split(".")[0].upper() == mnemo.upper():
            return t["ticker"]
    return mnemo


# --- Price Cache ---

def cache_prices(ticker: str, df: pd.DataFrame):
    """Stocke les prix historiques en cache SQLite."""
    if df.empty:
        return
    conn = get_connection()
    for _, row in df.iterrows():
        conn.execute(
            """INSERT OR REPLACE INTO price_cache (ticker, date, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                ticker,
                row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"]),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
            ),
        )
    conn.commit()
    conn.close()


def get_cached_prices(ticker: str) -> pd.DataFrame:
    """Récupère les prix en cache pour un ticker."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT date, open, high, low, close, volume FROM price_cache WHERE ticker=? ORDER BY date",
        conn,
        params=(ticker,),
        parse_dates=["date"],
    )
    conn.close()
    return df


# --- Portfolio ---

def save_position(ticker: str, company_name: str, quantity: float, avg_price: float,
                  purchase_date: str = None, notes: str = None) -> int:
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO portfolio (ticker, company_name, quantity, avg_price, purchase_date, notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (ticker, company_name, quantity, avg_price, purchase_date, notes),
    )
    conn.commit()
    row_id = cursor.lastrowid
    conn.close()
    return row_id


def get_portfolio() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM portfolio ORDER BY ticker", conn)
    conn.close()
    return df


def delete_position(position_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM portfolio WHERE id=?", (position_id,))
    conn.commit()
    conn.close()


# --- Investor Profile ---

def save_investor_profile(profile: dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO investor_profile (id, risk_profile, horizon, budget,
           preferred_sectors, preferred_tickers, excluded_tickers, objective, updated_at)
           VALUES (1, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(id) DO UPDATE SET
           risk_profile=excluded.risk_profile, horizon=excluded.horizon,
           budget=excluded.budget, preferred_sectors=excluded.preferred_sectors,
           preferred_tickers=excluded.preferred_tickers,
           excluded_tickers=excluded.excluded_tickers,
           objective=excluded.objective, updated_at=CURRENT_TIMESTAMP""",
        (
            profile.get("risk_profile"),
            profile.get("horizon"),
            profile.get("budget"),
            json.dumps(profile.get("preferred_sectors", []), ensure_ascii=False),
            json.dumps(profile.get("preferred_tickers", []), ensure_ascii=False),
            json.dumps(profile.get("excluded_tickers", []), ensure_ascii=False),
            profile.get("objective"),
        ),
    )
    conn.commit()
    conn.close()


def get_investor_profile() -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM investor_profile WHERE id=1").fetchone()
    conn.close()
    if not row:
        return None
    profile = dict(row)
    for key in ("preferred_sectors", "preferred_tickers", "excluded_tickers"):
        if profile.get(key):
            try:
                profile[key] = json.loads(profile[key])
            except (json.JSONDecodeError, TypeError):
                profile[key] = []
    return profile


# --- Market Data (auto-scraped from sikafinance) ---

def save_market_data(data: dict):
    """Sauvegarde les donnees de marche scrapees pour un titre."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO market_data (ticker, company_name, sector, price, variation,
           market_cap, beta, rsi, dps, dividend_history, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(ticker) DO UPDATE SET
           company_name=excluded.company_name, sector=excluded.sector,
           price=excluded.price, variation=excluded.variation,
           market_cap=excluded.market_cap, beta=excluded.beta, rsi=excluded.rsi,
           dps=excluded.dps, dividend_history=excluded.dividend_history,
           updated_at=CURRENT_TIMESTAMP""",
        (
            data.get("ticker"), data.get("name"), data.get("sector"),
            data.get("price"), data.get("variation"),
            data.get("market_cap"), data.get("beta"), data.get("rsi"),
            data.get("dps"),
            json.dumps(data.get("dividend_history", []), ensure_ascii=False),
        ),
    )
    conn.commit()
    conn.close()


def get_market_data(ticker: str = None) -> pd.DataFrame:
    """Recupere les donnees de marche. Si ticker=None, retourne tout."""
    conn = get_connection()
    if ticker:
        df = pd.read_sql_query(
            "SELECT * FROM market_data WHERE ticker=?", conn, params=(ticker,)
        )
    else:
        df = pd.read_sql_query("SELECT * FROM market_data ORDER BY ticker", conn)
    conn.close()
    return df


def get_all_stocks_for_analysis() -> pd.DataFrame:
    """
    Fusionne market_data (auto-scrape) + fundamentals (Excel/manuel).
    Priorite aux donnees fondamentales quand disponibles.
    """
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            COALESCE(f.ticker, m.ticker) as ticker,
            COALESCE(f.company_name, m.company_name) as company_name,
            COALESCE(f.sector, m.sector) as sector,
            COALESCE(m.price, f.price) as price,
            m.variation, m.market_cap, m.beta, m.rsi,
            f.shares, f.revenue, f.net_income, f.equity, f.total_debt,
            f.ebit, f.interest_expense, f.cfo, f.capex,
            f.dividends_total,
            COALESCE(f.dps, m.dps) as dps,
            m.dividend_yield as market_dividend_yield,
            f.fiscal_year, f.float_pct,
            f.revenue_n3, f.revenue_n2, f.revenue_n1, f.revenue_n0,
            f.net_income_n3, f.net_income_n2, f.net_income_n1, f.net_income_n0,
            f.dps_n3, f.dps_n2, f.dps_n1, f.dps_n0,
            CASE WHEN f.ticker IS NOT NULL THEN 1 ELSE 0 END as has_fundamentals,
            m.updated_at as market_updated_at
        FROM market_data m
        LEFT JOIN (
            SELECT ff.* FROM fundamentals ff
            INNER JOIN (
                SELECT ticker, MAX(fiscal_year) as max_year
                FROM fundamentals GROUP BY ticker
            ) latest ON ff.ticker = latest.ticker AND ff.fiscal_year = latest.max_year
        ) f ON m.ticker = f.ticker
        UNION
        SELECT
            f.ticker, f.company_name, f.sector, f.price,
            NULL as variation, NULL as market_cap, NULL as beta, NULL as rsi,
            f.shares, f.revenue, f.net_income, f.equity, f.total_debt,
            f.ebit, f.interest_expense, f.cfo, f.capex,
            f.dividends_total, f.dps,
            NULL as market_dividend_yield,
            f.fiscal_year, f.float_pct,
            f.revenue_n3, f.revenue_n2, f.revenue_n1, f.revenue_n0,
            f.net_income_n3, f.net_income_n2, f.net_income_n1, f.net_income_n0,
            f.dps_n3, f.dps_n2, f.dps_n1, f.dps_n0,
            1 as has_fundamentals,
            NULL as market_updated_at
        FROM fundamentals f
        INNER JOIN (
            SELECT ticker, MAX(fiscal_year) as max_year
            FROM fundamentals GROUP BY ticker
        ) latest ON f.ticker = latest.ticker AND f.fiscal_year = latest.max_year
        WHERE f.ticker NOT IN (SELECT ticker FROM market_data)
        ORDER BY ticker
    """, conn)
    conn.close()
    return df


# --- Publications Tracking ---

def save_publication(pub: dict) -> bool:
    """Sauvegarde une publication. Retourne True si c'est une nouvelle."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO publications (ticker, title, pub_type, period, url, pub_date)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(ticker, title) DO NOTHING""",
            (pub.get("ticker"), pub.get("title"), pub.get("type"),
             pub.get("period"), pub.get("url"), pub.get("date")),
        )
        conn.commit()
        is_new = conn.execute("SELECT changes()").fetchone()[0] > 0
    except Exception:
        is_new = False
    conn.close()
    return is_new


def get_publications(ticker: str = None, only_new: bool = False) -> pd.DataFrame:
    conn = get_connection()
    query = "SELECT * FROM publications"
    params = []
    conditions = []
    if ticker:
        conditions.append("ticker=?")
        params.append(ticker)
    if only_new:
        conditions.append("is_new=1")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY created_at DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def mark_publications_read(ticker: str = None):
    conn = get_connection()
    if ticker:
        conn.execute("UPDATE publications SET is_new=0 WHERE ticker=?", (ticker,))
    else:
        conn.execute("UPDATE publications SET is_new=0")
    conn.commit()
    conn.close()


# --- Quarterly Data ---

def save_quarterly_data(data: dict) -> int:
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO quarterly_data (ticker, fiscal_year, quarter, revenue,
           net_income, ebit, source, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(ticker, fiscal_year, quarter) DO UPDATE SET
           revenue=excluded.revenue, net_income=excluded.net_income,
           ebit=excluded.ebit, source=excluded.source, notes=excluded.notes""",
        (data.get("ticker"), data.get("fiscal_year"), data.get("quarter"),
         data.get("revenue"), data.get("net_income"), data.get("ebit"),
         data.get("source"), data.get("notes")),
    )
    conn.commit()
    row_id = cursor.lastrowid
    conn.close()
    return row_id


def get_quarterly_data(ticker: str, fiscal_year: int = None) -> pd.DataFrame:
    conn = get_connection()
    if fiscal_year:
        df = pd.read_sql_query(
            "SELECT * FROM quarterly_data WHERE ticker=? AND fiscal_year=? ORDER BY quarter",
            conn, params=(ticker, fiscal_year),
        )
    else:
        df = pd.read_sql_query(
            "SELECT * FROM quarterly_data WHERE ticker=? ORDER BY fiscal_year DESC, quarter",
            conn, params=(ticker,),
        )
    conn.close()
    return df


def get_publication_calendar() -> pd.DataFrame:
    """
    Genere un calendrier attendu des publications pour chaque titre suivi.
    Les entreprises BRVM publient generalement:
    - T1: fin avril / debut mai
    - S1: fin aout / debut septembre
    - T3: fin octobre / debut novembre
    - Annuel: fin mars / debut avril
    """
    conn = get_connection()
    # Get all tracked tickers
    tickers = pd.read_sql_query(
        """SELECT DISTINCT ticker, company_name, sector FROM (
            SELECT ticker, company_name, sector FROM fundamentals
            UNION
            SELECT ticker, company_name, sector FROM market_data
        ) ORDER BY ticker""",
        conn,
    )
    conn.close()

    if tickers.empty:
        return pd.DataFrame()

    from datetime import datetime
    current_year = datetime.now().year
    current_month = datetime.now().month

    calendar = []
    for _, row in tickers.iterrows():
        ticker = row["ticker"]
        name = row["company_name"]
        sector = row["sector"]

        # Expected publication dates
        expected = [
            {"period": f"T1 {current_year}", "expected_month": 5, "type": "trimestriel"},
            {"period": f"S1 {current_year}", "expected_month": 9, "type": "semestriel"},
            {"period": f"T3 {current_year}", "expected_month": 11, "type": "trimestriel"},
            {"period": f"Annuel {current_year - 1}", "expected_month": 4, "type": "annuel"},
        ]

        for exp in expected:
            status = "a_venir"
            if exp["expected_month"] < current_month:
                status = "en_retard"
            elif exp["expected_month"] == current_month:
                status = "attendu_ce_mois"

            calendar.append({
                "ticker": ticker,
                "company_name": name,
                "sector": sector,
                "period": exp["period"],
                "type": exp["type"],
                "expected_month": exp["expected_month"],
                "status": status,
            })

    return pd.DataFrame(calendar)


# --- Qualitative Notes ---

def save_qualitative_note(ticker: str, category: str, content: str,
                          source: str = None, note_date: str = None) -> int:
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO qualitative_notes (ticker, category, content, source, note_date)
           VALUES (?, ?, ?, ?, ?)""",
        (ticker, category, content, source, note_date),
    )
    conn.commit()
    row_id = cursor.lastrowid
    conn.close()
    return row_id


def get_qualitative_notes(ticker: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM qualitative_notes WHERE ticker=? ORDER BY created_at DESC",
        conn, params=(ticker,),
    )
    conn.close()
    return df


def delete_qualitative_note(note_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM qualitative_notes WHERE id=?", (note_id,))
    conn.commit()
    conn.close()


# --- Report Links ---

def save_report_link(data: dict) -> bool:
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO report_links (ticker, title, report_type, fiscal_year, url, source)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(ticker, url) DO UPDATE SET
               title=excluded.title, report_type=excluded.report_type,
               fiscal_year=excluded.fiscal_year""",
            (data.get("ticker"), data.get("title"), data.get("report_type"),
             data.get("fiscal_year"), data.get("url"), data.get("source", "brvm.org")),
        )
        conn.commit()
        saved = True
    except Exception:
        saved = False
    conn.close()
    return saved


def get_report_links(ticker: str = None) -> pd.DataFrame:
    conn = get_connection()
    if ticker:
        df = pd.read_sql_query(
            "SELECT * FROM report_links WHERE ticker=? ORDER BY fiscal_year DESC",
            conn, params=(ticker,),
        )
    else:
        df = pd.read_sql_query(
            "SELECT * FROM report_links ORDER BY fiscal_year DESC, ticker",
            conn,
        )
    conn.close()
    return df


def seed_known_report_links():
    """Pre-charge les liens connus vers les rapports annuels 2023-2024 des 48 titres BRVM."""
    known_reports = [
        # === SONATEL ===
        {"ticker": "SNTS.sn", "title": "Etats financiers 2024 - Sonatel", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250221_-_etats_financiers_-_exercice_2024_-_sonatel_sn.pdf"},
        {"ticker": "SNTS.sn", "title": "Rapport annuel 2024 - Sonatel", "report_type": "rapport_annuel", "fiscal_year": 2024, "url": "https://sonatel.sn/wp-content/uploads/2025/04/SONATEL_RA-2024_A4-Digital.pdf"},
        {"ticker": "SNTS.sn", "title": "Rapport activites T3 2024 - Sonatel", "report_type": "rapport_trimestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20241025_-_rapport_dactivites_-_3eme_trimestre_2024_-_sonatel_sn.pdf"},
        {"ticker": "SNTS.sn", "title": "Etats financiers 2023 - Sonatel", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240222_-_etats_financiers_-_exercice_2023_-_sonatel_sn.pdf"},
        # === ORANGE CI ===
        {"ticker": "ORAC.ci", "title": "Rapport annuel integre 2024 - Orange CI", "report_type": "rapport_annuel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250416_-_rapport_annuel_integre_-_exercice_2024_-_orange_ci.pdf"},
        {"ticker": "ORAC.ci", "title": "Etats financiers 2024 - Orange CI", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250221_-_etats_financiers_-_exercice_2024_-_orange_ci.pdf"},
        # === ECOBANK CI ===
        {"ticker": "ECOC.ci", "title": "Rapport activites 2024 - Ecobank CI", "report_type": "rapport_annuel", "fiscal_year": 2024, "url": "https://www.brvm.org/fr/ecobank-cote-divoire-rapport-dactivites-annuel-exercice-2024"},
        {"ticker": "ECOC.ci", "title": "Note de recherche 2024 - Ecobank CI", "report_type": "analyse", "fiscal_year": 2024, "url": "https://www.bridge-securities.com/images/app/contenu/312/VFNotedeRecherche2024ECOBANKCIFR.pdf"},
        # === NSIA BANQUE ===
        {"ticker": "NSBC.ci", "title": "Etats financiers certifies 2024 - NSIA Banque", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/en/nsia-banque-cote-divoire-financial-statements-certified-statutory-auditors-2024"},
        {"ticker": "NSBC.ci", "title": "Note de recherche - NSIA Banque CI", "report_type": "analyse", "fiscal_year": 2024, "url": "https://www.bridge-securities.com/images/app/contenu/332/NSIABANQUECINotedeRechercheFR.pdf"},
        {"ticker": "NSBC.ci", "title": "Etats financiers 2023 - NSIA Banque", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240328_-_etats_financiers_-_exercice_2023_-_nsia_banque_ci.pdf"},
        # === SGBCI ===
        {"ticker": "SGBC.ci", "title": "Rapport activites 2024 - SGBCI", "report_type": "rapport_annuel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250207_-_rapport_dactivites_annuel_-_exercice_2024_-_societe_generale_ci.pdf"},
        {"ticker": "SGBC.ci", "title": "Page rapports BRVM - SGBCI", "report_type": "rapport_annuel", "fiscal_year": 2024, "url": "https://www.brvm.org/fr/rapports-societe-cotes/sgb-ci"},
        # === ETI TOGO ===
        {"ticker": "ETIT.tg", "title": "Etats financiers certifies 2024 - ETI TG", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250328_-_etats_financiers_certifies_par_les_commissaires_aux_comptes_-_exercice_2024_-_eti_tg.pdf"},
        {"ticker": "ETIT.tg", "title": "Etats financiers 2023 - ETI TG", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240502_-_etats_financiers_-_exercice_2023_-_eti_tg.pdf"},
        # === BICICI ===
        {"ticker": "BICC.ci", "title": "Etats financiers 2024 - BICICI", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250328_-_etats_financiers_-_exercice_2024_-_bici_ci.pdf"},
        {"ticker": "BICC.ci", "title": "Rapport activites S1 2024 - BICICI", "report_type": "rapport_semestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20241031_-_rapport_dactivites_-_1er_semestre_2024_-_bici_ci.pdf"},
        # === SIB CI ===
        {"ticker": "SIBC.ci", "title": "Rapport activites 2024 - SIB CI", "report_type": "rapport_annuel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250305_-_rapport_dactivites_annuel_-_exercice_2024_-_sib_ci.pdf"},
        # === BOA SENEGAL ===
        {"ticker": "BOAS.sn", "title": "Etats financiers certifies 2024 - BOA Senegal", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250319_-_etats_financiers_certifies_par_les_cacs_-_exercice_2024_-_boa_senegal.pdf"},
        # === BOA NIGER ===
        {"ticker": "BOAN.ne", "title": "Etats financiers 2023 - BOA Niger", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240325_-_etats_financiers_-_exercice_2023_-_boa_niger.pdf"},
        # === PALM CI ===
        {"ticker": "PALC.ci", "title": "Etats financiers provisoires 2024 - Palm CI", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250417_-_etats_financiers_provisoires_valides_par_le_ca_-_exercice_2024_-_palm_ci.pdf"},
        # === FILTISAC ===
        {"ticker": "FTSC.ci", "title": "Etats financiers 2024 - Filtisac CI", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250523_-_etats_financiers_-_exercice_2024_-_filtisac_ci.pdf"},
        {"ticker": "FTSC.ci", "title": "Rapport activites T1 2024 - Filtisac", "report_type": "rapport_trimestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20240513_-_rapport_dactivites_-_1er_trimestre_2024_-_filtisac_ci_0.pdf"},
        # === SODECI ===
        {"ticker": "SDCC.ci", "title": "Etats financiers 2024 - SODECI", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250430_-_etats_financiers_-_exercice_2024_-_sode_ci.pdf"},
        {"ticker": "SDCC.ci", "title": "Rapport activites S1 2024 - SODECI", "report_type": "rapport_semestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20241031_-_rapport_dactivites_-_1er_semestre_2024_-_sode_ci.pdf"},
        # === CIE CI ===
        {"ticker": "CIEC.ci", "title": "Rapport activites S1 2024 certifie - CIE CI", "report_type": "rapport_semestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20241030_-_rapport_dactivites_certifie_par_les_cacs_-_1er_semestre_2024_-_cie_ci.pdf"},
        # === SERVAIR ABIDJAN ===
        {"ticker": "ABJC.ci", "title": "Etats financiers IFRS 2024 - Servair Abidjan", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250430_-_etats_financiers_-_norme_ifrs_-_exercice_2024_-_servair_abidjan_ci.pdf"},
        {"ticker": "ABJC.ci", "title": "Etats financiers 2023 - Servair Abidjan", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240819_-_etats_financiers_approuves_-_exercice_2023_-_servair_abidjan_ci.pdf"},
        # === ONATEL BF ===
        {"ticker": "ONTBF.bf", "title": "Rapport gestion 2024 - Onatel BF", "report_type": "rapport_annuel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250718_-_rapport_annuel_de_gestion_-_exercice_2024_-_onatel_bf.pdf"},
        {"ticker": "ONTBF.bf", "title": "Resultats financiers 2023 - Onatel BF", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240404_-_resultats_financiers_-_exercice_2023_-_onatel_bf.pdf"},
        # === ORAGROUP TOGO ===
        {"ticker": "ORGT.tg", "title": "Rapport activites S1 2024 - Oragroup", "report_type": "rapport_semestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20240930_-_rapport_dactivites_-_1er_semestre_2024_-_oragroup_tg.pdf"},
        # === SITAB ===
        {"ticker": "STBC.ci", "title": "Rapport activites S1 2024 - SITAB", "report_type": "rapport_semestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20241031_-_rapport_dactivites_-_1er_semestre_2024_-_sitab_ci.pdf"},
        # === SAFCA ===
        {"ticker": "SAFC.ci", "title": "Rapport activites T4 2024 - SAFCA", "report_type": "rapport_trimestriel", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20240624_-_rapport_dactivites_-_4eme_trimestre_2024_-_safca_ci.pdf"},
        # === LOTERIE NATIONALE BENIN ===
        {"ticker": "LNBB.bj", "title": "Etats financiers 2024 - LNB Benin", "report_type": "etats_financiers", "fiscal_year": 2024, "url": "https://www.brvm.org/sites/default/files/20250602_-_etats_financiers_-_exercice_2024_-_lnb_bn.pdf"},
        # === TOTAL CI ===
        {"ticker": "TTLC.ci", "title": "Etats financiers definitifs 2023 - TotalEnergies CI", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240910_-_etats_financiers_definitifs_2023_-_totalenergies_marketing_ci.pdf"},
        # === SUCRIVOIRE ===
        {"ticker": "SCRC.ci", "title": "Etats financiers 2023 - Sucrivoire", "report_type": "etats_financiers", "fiscal_year": 2023, "url": "https://www.brvm.org/sites/default/files/20240510_-_etats_financiers_-_exercice_2023_-_sucrivoire_ci.pdf"},
    ]
    for report in known_reports:
        save_report_link(report)


# Initialize DB on import
init_db()
