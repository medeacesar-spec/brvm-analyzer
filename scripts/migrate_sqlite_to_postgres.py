#!/usr/bin/env python3
"""
Migration SQLite → Postgres (Supabase/Neon).

Prérequis :
    pip install 'psycopg[binary]'

Usage :
    # 1. Définir l'URL Postgres cible
    export DATABASE_URL="postgresql://user:password@host:5432/dbname?sslmode=require"

    # 2. Lancer la migration
    python3 scripts/migrate_sqlite_to_postgres.py

    # Options
    python3 scripts/migrate_sqlite_to_postgres.py --schema-only
    python3 scripts/migrate_sqlite_to_postgres.py --skip-schema     # données seulement
    python3 scripts/migrate_sqlite_to_postgres.py --truncate        # vider avant import

Le script :
  1. Applique le schéma (CREATE TABLE) sur Postgres via `init_db()` (passe par la
     couche db.py qui fait la traduction SQLite → PG).
  2. Copie toutes les données depuis la SQLite locale vers Postgres.
  3. Réinitialise les séquences (auto-increment) pour éviter les collisions.
"""

import os
import sqlite3
import sys
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH


# Ordre d'import pour respecter les dépendances de clés (pas de FK ici, mais
# c'est plus propre dans les logs)
TABLES_IN_ORDER = [
    "fundamentals",
    "market_data",
    "price_cache",
    "indices_cache",
    "portfolio",
    "portfolio_settings",
    "investor_profile",
    "quarterly_data",
    "qualitative_notes",
    "publications",
    "report_links",
    "company_profiles",
    "company_news",
    "signal_history",
    "calibration_reviews",
    "ignored_gaps",
]


def _list_tables(conn) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_bak'"
    )
    return [r[0] for r in cur.fetchall()]


def _get_columns(conn, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def _apply_schema():
    """Crée les tables sur Postgres via init_db() (utilise le wrapper db.py)."""
    from data.storage import init_db
    print("→ Application du schéma sur Postgres (init_db)…")
    # S'assurer qu'on est bien en mode Postgres
    from data.db import is_postgres
    if not is_postgres():
        print("❌ DATABASE_URL ne pointe pas vers Postgres. Annulation.")
        sys.exit(1)
    init_db()
    print("✅ Schéma appliqué")


def _truncate_postgres(tables: List[str]):
    from data.db import get_connection
    conn = get_connection()
    print(f"→ TRUNCATE de {len(tables)} tables Postgres…")
    for t in tables:
        try:
            conn.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE")
            print(f"    {t}: truncated")
        except Exception as e:
            print(f"    {t}: skip ({e})")
    conn.commit()
    conn.close()


def _copy_table(sqlite_conn, pg_conn_raw, table: str) -> int:
    """Copie toutes les lignes d'une table SQLite vers Postgres via psycopg brut.
    Utilise autocommit pour éviter les transactions bloquées Postgres."""
    try:
        cur = sqlite_conn.execute(f"SELECT * FROM {table}")
    except sqlite3.OperationalError as e:
        print(f"    {table}: table absente en SQLite ({e})")
        return 0

    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        print(f"    {table}: vide")
        return 0

    col_names = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    inserted = 0
    errors = 0
    first_err = None
    for r in rows:
        try:
            with pg_conn_raw.cursor() as cur:
                cur.execute(sql, tuple(r))
            inserted += 1
        except Exception as e:
            errors += 1
            if first_err is None:
                first_err = str(e)[:200]

    status = f"{inserted}/{len(rows)} lignes"
    if errors:
        status += f" · {errors} erreurs"
    print(f"    {table}: {status}")
    if first_err:
        print(f"    ↳ 1re erreur: {first_err}")
    return inserted


def _reset_sequences(pg_conn, tables: List[str]):
    """Réinitialise les séquences auto-increment pour éviter les collisions."""
    print("→ Réinitialisation des séquences Postgres…")
    for table in tables:
        try:
            # Chercher la colonne id si elle existe
            cur = pg_conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s AND column_name = 'id'",
                (table,),
            )
            row = cur.fetchone()
            if row:
                pg_conn.execute(
                    f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                    f"COALESCE((SELECT MAX(id) FROM {table}), 1))"
                )
                print(f"    {table}: séquence id → max(id)")
        except Exception as e:
            print(f"    {table}: skip ({e})")
    pg_conn.commit()


def migrate(skip_schema: bool = False, truncate: bool = False):
    # Vérifications
    if not os.path.exists(DB_PATH):
        print(f"❌ SQLite introuvable : {DB_PATH}")
        sys.exit(1)

    from data.db import is_postgres, get_connection
    if not is_postgres():
        print("❌ DATABASE_URL n'est pas défini ou ne pointe pas vers Postgres.")
        print("   Définir : export DATABASE_URL=\"postgresql://...\"")
        sys.exit(1)

    # 1. Schéma
    if not skip_schema:
        _apply_schema()

    # 2. Connexions
    sqlite_conn = sqlite3.connect(DB_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    all_tables = _list_tables(sqlite_conn)
    ordered_tables = [t for t in TABLES_IN_ORDER if t in all_tables]
    # Ajouter les tables non listées (nouvelles)
    for t in all_tables:
        if t not in ordered_tables:
            ordered_tables.append(t)

    print(f"\n{'='*60}")
    print(f"MIGRATION SQLite → Postgres")
    print(f"{'='*60}")
    print(f"SQLite  : {DB_PATH}")
    print(f"Tables  : {len(ordered_tables)}")
    print()

    # 3. Truncate optionnel
    if truncate:
        _truncate_postgres(ordered_tables)

    # 4. Copie — connexion psycopg brute en autocommit (robuste avec pooler Supabase)
    import psycopg
    from data.db import _get_database_url
    pg_conn_raw = psycopg.connect(_get_database_url(), autocommit=True)
    total_rows = 0
    for table in ordered_tables:
        print(f"  {table}:")
        n = _copy_table(sqlite_conn, pg_conn_raw, table)
        total_rows += n

    # 5. Réinitialisation des séquences
    pg_conn = get_connection()
    _reset_sequences(pg_conn, ordered_tables)

    pg_conn_raw.close()
    pg_conn.close()
    sqlite_conn.close()

    print(f"\n{'='*60}")
    print(f"✅ MIGRATION TERMINÉE : {total_rows} lignes copiées")
    print(f"{'='*60}")


if __name__ == "__main__":
    skip_schema = "--skip-schema" in sys.argv
    schema_only = "--schema-only" in sys.argv
    truncate = "--truncate" in sys.argv

    if schema_only:
        _apply_schema()
    else:
        migrate(skip_schema=skip_schema, truncate=truncate)
