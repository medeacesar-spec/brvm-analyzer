"""Les indices du jour, lus sur brvm.org.

POURQUOI CE MODULE EXISTE A PART

Ce scrape vivait dans `app.py` et tournait A CHAQUE OUVERTURE DE SESSION,
avant le premier affichage, avec trente secondes d'attente avant d'abandonner.
Un commentaire le disait « leger, une requete HTTP d'une seconde » — ce qui
n'est vrai que lorsque brvm.org repond. Le 24/09/2026, le site a mis plus de
trente secondes : chaque visiteur attendait la page, puis l'administrateur
lisait « ReadTimeout ».

Sorti d'`app.py`, il devient appelable par les taches planifiees, qui le font
hors de toute attente humaine. L'application ne s'en sert plus qu'en dernier
recours, si le cache est vieux, et avec un delai court.
"""
from __future__ import annotations

from datetime import datetime

from data.db import get_connection

# Delai d'attente par defaut. Court : cette lecture est un confort, jamais un
# prealable — les indices affichés viennent du cache tant qu'elle echoue.
DELAI_DEFAUT = 8

# Au-dela, le cache est juge trop vieux pour etre servi sans tentative de
# rafraichissement. Les taches planifiees passent quatre fois par jour.
FRAICHEUR_HEURES = 6


def age_du_cache_heures():
    """Age du plus recent indice en cache, en heures. None si le cache est vide."""
    conn = get_connection()
    try:
        ligne = conn.execute("SELECT MAX(updated_at) AS m FROM indices_cache").fetchone()
    except Exception:
        return None
    finally:
        conn.close()
    instant = dict(ligne).get("m") if ligne else None
    if not instant:
        return None
    if isinstance(instant, str):
        try:
            instant = datetime.fromisoformat(instant)
        except ValueError:
            return None
    return (datetime.now() - instant).total_seconds() / 3600.0


def cache_perime(seuil_heures: float = FRAICHEUR_HEURES) -> bool:
    """Vrai si le cache manque ou date de plus de `seuil_heures`."""
    age = age_du_cache_heures()
    return age is None or age > seuil_heures


def rafraichir_indices(delai: int = DELAI_DEFAUT) -> int:
    """Relit les 12 indices sur brvm.org et remplit `indices_cache`.

    Rend le nombre d'indices ecrits.
    """
    import re
    import requests
    from bs4 import BeautifulSoup

    resp = requests.get(
        "https://www.brvm.org/fr/indices",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=delai, verify=False,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    def _parse_num(text):
        if not text:
            return None
        c = text.replace("\xa0", "").replace(" ", "")
        if "," in c:
            c = c.replace(".", "").replace(",", ".")
        c = re.sub(r"[^\d.\-]", "", c)
        try:
            return float(c)
        except (ValueError, TypeError):
            return None

    indices = []
    for table in soup.find_all("table", class_="table"):
        thead = table.find("thead")
        if not thead or "Fermeture" not in thead.get_text():
            continue
        tbody = table.find("tbody")
        if not tbody:
            continue
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue
            name = cells[0].get_text(strip=True)
            close = _parse_num(cells[2].get_text(strip=True))
            var_span = cells[3].find("span", class_=["text-bad", "text-good"])
            variation = _parse_num(var_span.get_text(strip=True)) if var_span else None
            if var_span and "text-bad" in var_span.get("class", []) and variation and variation > 0:
                variation = -variation
            ytd = None
            if len(cells) >= 5:
                ytd_span = cells[4].find("span", class_=["text-bad", "text-good"])
                ytd = _parse_num(ytd_span.get_text(strip=True)) if ytd_span else None
                if ytd_span and "text-bad" in ytd_span.get("class", []) and ytd and ytd > 0:
                    ytd = -ytd
            if name and close is not None:
                cat = "total_return" if "TOTAL RETURN" in name.upper() else \
                      "principal" if any(s in name.upper() for s in ["COMPOSITE", "BRVM-30", "PRESTIGE", "PRINCIPAL"]) else \
                      "sectoriel"
                indices.append((name, close, variation, ytd, cat))

    if not indices:
        return 0

    # Dédoublonnage par nom : brvm.org affiche parfois le même indice
    # (ex. BRVM-30) dans plusieurs tables (principaux, total_return, etc.).
    # On garde la première occurrence rencontrée pour éviter le
    # UniqueViolation sur la PK `name` lors des INSERT suivants.
    _seen_names = set()
    indices = [idx for idx in indices
               if not (idx[0] in _seen_names or _seen_names.add(idx[0]))]

    conn = get_connection()
    # Ensure columns exist (idempotent, tolerant des drivers abortant la txn).
    try:
        conn.execute("SELECT prev_close FROM indices_cache LIMIT 1")
    except Exception:
        # Postgres abort la transaction sur un SELECT en erreur → rollback obligatoire
        try:
            conn.rollback()
        except Exception:
            pass
        for col, ctype in [("prev_close", "REAL"), ("ytd_variation", "REAL"), ("category", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE indices_cache ADD COLUMN {col} {ctype}")
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
    conn.execute("DELETE FROM indices_cache")
    # INSERT idempotent : même si un nom apparaît plusieurs fois dans le
    # scrape (cf brvm.org qui répète parfois un indice dans plusieurs
    # sections), ON CONFLICT DO UPDATE met à jour au lieu de lever.
    for name, close, var, ytd, cat in indices:
        conn.execute(
            "INSERT INTO indices_cache (name, value, variation, ytd_variation, category, updated_at) "
            "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT (name) DO UPDATE SET "
            "  value = EXCLUDED.value, variation = EXCLUDED.variation, "
            "  ytd_variation = EXCLUDED.ytd_variation, category = EXCLUDED.category, "
            "  updated_at = CURRENT_TIMESTAMP",
            (name, close, var, ytd, cat),
        )
    conn.commit()
    conn.close()
    return len(indices)
