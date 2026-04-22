"""
Module de scraping pour sikafinance.com
Recupere les cotations du jour, les donnees historiques et les infos detaillees des titres BRVM.
"""

import io
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import SIKA_AAZ_URL, SIKA_COTATION_URL, SIKA_DOWNLOAD_URL, SIKA_BASE_URL

# Headers pour simuler un navigateur
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def _get_session():
    """Cree une session HTTP reutilisable."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


BRVM_QUOTES_URL = "https://www.brvm.org/fr/cours-actions/0"
BRVM_CAPITALIZATIONS_URL = "https://www.brvm.org/fr/capitalisations/0"
BRVM_VOLUMES_URL = "https://www.brvm.org/fr/volumes/0"


def _parse_brvm_number(text: str) -> float:
    """Parse un nombre affiché par brvm.org : espaces (y.c. insécables) comme
    milliers et virgule décimale FR. Retourne 0.0 en cas d'échec."""
    if text is None:
        return 0.0
    s = str(text).replace("\xa0", " ").replace(" ", "").replace(",", ".").strip()
    s = s.replace("%", "").replace("+", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


_MONTHS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


def _parse_brvm_session_header(soup) -> dict:
    """Extrait depuis la page brvm.org : date et heure de session, statut ouvert/fermé.
    Retourne {"date": "YYYY-MM-DD", "time": "HH:MM", "is_open": bool, "raw": str}
    ou un dict partiel si parsing KO."""
    import re as _re
    out = {"date": None, "time": None, "is_open": None, "raw": None}

    hdr = soup.find("p", class_="header-seance")
    if hdr:
        raw = hdr.get_text(strip=True)
        out["raw"] = raw
        # Ex : "Mercredi, 22 avril, 2026 - 09:32"
        m = _re.search(r"(\d{1,2})\s+(\w+)[,\s]+(\d{4})\s*-\s*(\d{1,2}):(\d{2})", raw)
        if m:
            day, month_fr, year, hh, mm = m.groups()
            month = _MONTHS_FR.get(month_fr.lower())
            if month:
                out["date"] = f"{int(year):04d}-{month:02d}-{int(day):02d}"
                out["time"] = f"{int(hh):02d}:{int(mm):02d}"

    stat = soup.find("div", class_=lambda c: c and ("seance-ouverte" in c or "seance-fermee" in c))
    if stat:
        cls = " ".join(stat.get("class") or [])
        out["is_open"] = "seance-ouverte" in cls

    return out


def fetch_daily_quotes_brvm() -> pd.DataFrame:
    """
    Cotations du jour depuis brvm.org/fr/cours-actions/0 (source officielle).

    Pourquoi pas sikafinance : leur page /aaz charge le gros du tbody en JS,
    BeautifulSoup ne voit que ~5 titres (BOA*) sur le HTML statique. brvm.org
    rend tout côté serveur : 46 lignes complètes disponibles directement.

    Retourne un DataFrame avec les mêmes colonnes que fetch_daily_quotes()
    pour drop-in replacement : ticker, name, open, high, low, volume_shares,
    volume_xof, last, variation.

    Les tickers retournés sont dépourvus de suffixe (ex. "SNTS", "ETIT").
    L'appelant peut les mapper vers les suffixes internes (.sn, .tg, ...) via
    config.load_tickers si besoin.
    """
    session = _get_session()
    try:
        resp = session.get(BRVM_QUOTES_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise ConnectionError(f"Erreur de connexion à brvm.org: {e}")

    soup = BeautifulSoup(resp.text, "lxml")

    # La page contient plusieurs tables (top5, flop5, activity, puis cotations).
    # On cible la table dont le header contient 'Symbole' ET 'Cours Clôture'.
    target_table = None
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        has_symbole = any("symbole" in h for h in headers)
        has_close = any("clôt" in h or "clot" in h for h in headers)
        if has_symbole and has_close:
            target_table = tbl
            break
    if target_table is None:
        return pd.DataFrame()

    # Map brvm.org court (SNTS, ETIT, ABJC) → ticker interne avec suffixe
    # (SNTS.sn, ETIT.tg, ABJC.ci). On s'appuie sur brvm_tickers.json.
    try:
        from config import load_tickers as _load_tickers
        _ticker_map = {t["ticker"].split(".")[0].upper(): t["ticker"]
                        for t in _load_tickers()}
    except Exception:
        _ticker_map = {}

    tbody = target_table.find("tbody") or target_table
    rows = []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 7:
            continue

        symbole_short = cells[0].get_text(strip=True)
        if not symbole_short:
            continue
        # Ticker complet (avec .ci/.sn/.tg/.bj/.bf) si présent dans notre config
        symbole = _ticker_map.get(symbole_short.upper(), symbole_short)
        name = cells[1].get_text(strip=True)
        volume = _parse_brvm_number(cells[2].get_text())
        prev_close = _parse_brvm_number(cells[3].get_text())
        open_p = _parse_brvm_number(cells[4].get_text())
        close_p = _parse_brvm_number(cells[5].get_text())

        # Variation : souvent dans un <span class="text-good/text-bad/text-nul">
        var_cell = cells[6]
        var_span = var_cell.find("span")
        var_text = (var_span.get_text(strip=True) if var_span
                     else var_cell.get_text(strip=True))
        # Signe : text-bad = négatif
        is_neg = bool(var_span and "text-bad" in (var_span.get("class") or []))
        variation = _parse_brvm_number(var_text)
        if is_neg and variation > 0:
            variation = -variation

        rows.append({
            "ticker": symbole,
            "name": name,
            "open": open_p,
            "high": max(open_p, close_p) if open_p and close_p else close_p,
            "low": min(open_p, close_p) if open_p and close_p else close_p,
            "volume_shares": volume,
            "volume_xof": volume * close_p if volume and close_p else 0.0,
            "last": close_p or prev_close,
            "variation": variation,
            "prev_close": prev_close,
        })

    return pd.DataFrame(rows)


def fetch_session_info() -> dict:
    """Retourne la date/heure/statut de la session BRVM courante depuis
    brvm.org/fr/cours-actions/0.

    Clés : date (YYYY-MM-DD de la séance affichée), time (HH:MM), is_open
    (bool séance ouverte), raw (texte original du header)."""
    session = _get_session()
    try:
        resp = session.get(BRVM_QUOTES_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise ConnectionError(f"Erreur brvm.org session info: {e}")
    soup = BeautifulSoup(resp.text, "lxml")
    return _parse_brvm_session_header(soup)


def fetch_capitalizations_brvm() -> pd.DataFrame:
    """Scrape https://www.brvm.org/fr/capitalisations/0 — table officielle des
    capitalisations par titre. Utile en fallback quand shares / market_cap /
    float_pct manquent dans notre DB.

    Colonnes retournees : ticker (avec suffixe .ci/.sn/...), name, shares,
    price, float_market_cap, total_market_cap, weight_pct.
    """
    session = _get_session()
    try:
        resp = session.get(BRVM_CAPITALIZATIONS_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise ConnectionError(f"Erreur de connexion à brvm.org/capitalisations: {e}")

    soup = BeautifulSoup(resp.text, "lxml")

    target = None
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if any("nombre de titres" in h for h in headers):
            target = tbl
            break
    if target is None:
        return pd.DataFrame()

    try:
        from config import load_tickers as _load_tickers
        _map = {t["ticker"].split(".")[0].upper(): t["ticker"]
                 for t in _load_tickers()}
    except Exception:
        _map = {}

    rows = []
    tbody = target.find("tbody") or target
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 7:
            continue
        short = cells[0].get_text(strip=True)
        if not short:
            continue
        full_ticker = _map.get(short.upper(), short)
        name = cells[1].get_text(strip=True)
        shares = _parse_brvm_number(cells[2].get_text())
        price = _parse_brvm_number(cells[3].get_text())
        float_cap = _parse_brvm_number(cells[4].get_text())
        total_cap = _parse_brvm_number(cells[5].get_text())
        weight = _parse_brvm_number(cells[6].get_text())

        float_pct = (float_cap / total_cap * 100) if total_cap and float_cap else None

        rows.append({
            "ticker": full_ticker,
            "name": name,
            "shares": shares,
            "price": price,
            "float_market_cap": float_cap,
            "total_market_cap": total_cap,
            "weight_pct": weight,
            "float_pct": float_pct,
        })

    return pd.DataFrame(rows)


def fetch_volumes_brvm() -> dict:
    """Scrape https://www.brvm.org/fr/volumes/0 — volumes d'échange du jour.

    Complete /cours-actions/0 avec :
    - valeur_echangee (FCFA) par titre
    - PER officiel BRVM (référence indépendante de nos calculs)
    - part dans la valeur globale échangée (% market share du jour)
    - total marché (valeur des transactions, capitalisation actions/obligations)

    Retourne un dict {"tickers": DataFrame, "market": dict}.
    """
    session = _get_session()
    try:
        resp = session.get(BRVM_VOLUMES_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise ConnectionError(f"Erreur de connexion à brvm.org/volumes: {e}")

    soup = BeautifulSoup(resp.text, "lxml")

    try:
        from config import load_tickers as _load_tickers
        _map = {t["ticker"].split(".")[0].upper(): t["ticker"]
                 for t in _load_tickers()}
    except Exception:
        _map = {}

    # Table titres (Nombre de titres échangés + Valeur échangée + PER)
    tbl_tickers = None
    tbl_market = None
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if any("nombre de titres" in h for h in headers) and any("per" in h for h in headers):
            tbl_tickers = tbl
        if any("activités du marché" in h for h in headers):
            tbl_market = tbl

    rows = []
    if tbl_tickers:
        tbody = tbl_tickers.find("tbody") or tbl_tickers
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 6:
                continue
            short = cells[0].get_text(strip=True)
            if not short:
                continue
            full_ticker = _map.get(short.upper(), short)
            rows.append({
                "ticker": full_ticker,
                "name": cells[1].get_text(strip=True),
                "volume_shares": _parse_brvm_number(cells[2].get_text()),
                "volume_xof": _parse_brvm_number(cells[3].get_text()),
                "per_brvm": _parse_brvm_number(cells[4].get_text()) or None,
                "trade_share_pct": _parse_brvm_number(cells[5].get_text()),
            })

    market = {}
    if tbl_market:
        tbody = tbl_market.find("tbody") or tbl_market
        for tr in tbody.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue
            key = cells[0].lower()
            val = _parse_brvm_number(cells[1])
            if "transactions" in key:
                market["transactions_value_xof"] = val
            elif "capitalisation actions" in key:
                market["equity_market_cap"] = val
            elif "capitalisation des obligations" in key:
                market["bonds_market_cap"] = val

    return {"tickers": pd.DataFrame(rows), "market": market}


def fetch_daily_quotes() -> pd.DataFrame:
    """
    Cotations du jour — prend brvm.org en priorité (HTML server-rendered complet),
    fallback sikafinance.com/marches/aaz si brvm.org est indisponible.

    Returns:
        DataFrame avec colonnes: ticker, name, open, high, low, volume_shares,
                                  volume_xof, last, variation
    """
    # Source 1 : brvm.org (officiel, complet)
    try:
        df = fetch_daily_quotes_brvm()
        if not df.empty and len(df) > 10:  # sanity : au moins 10 titres
            return df
    except Exception:
        pass

    # Fallback : sikafinance (peut être tronqué mais mieux que rien)
    return _fetch_daily_quotes_sikafinance()


def _fetch_daily_quotes_sikafinance() -> pd.DataFrame:
    """Scraper historique sikafinance — gardé en fallback."""
    session = _get_session()
    try:
        resp = session.get(SIKA_AAZ_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise ConnectionError(f"Erreur de connexion a sikafinance: {e}")

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", id="tblShare")
    if table is None:
        # Fallback : chercher le premier grand tableau
        table = soup.find("table", class_="table")

    if table is None:
        return pd.DataFrame()

    rows = []
    tbody = table.find("tbody")
    if tbody is None:
        tbody = table

    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 8:
            continue

        # Extraire le ticker depuis le lien
        link = cells[0].find("a")
        name = cells[0].get_text(strip=True)
        ticker = ""
        if link and link.get("href"):
            href = link["href"]
            # Pattern: /marches/cotation_ECOC.ci
            if "cotation_" in href:
                ticker = href.split("cotation_")[-1]

        def parse_num(cell):
            text = cell.get_text(strip=True).replace("\xa0", "").replace(" ", "").replace(",", ".")
            try:
                return float(text)
            except (ValueError, TypeError):
                return 0.0

        rows.append({
            "ticker": ticker,
            "name": name,
            "open": parse_num(cells[1]),
            "high": parse_num(cells[2]),
            "low": parse_num(cells[3]),
            "volume_shares": parse_num(cells[4]),
            "volume_xof": parse_num(cells[5]),
            "last": parse_num(cells[6]),
            "variation": parse_num(cells[7]),
        })

    return pd.DataFrame(rows)


def fetch_historical_prices(ticker: str, start_date: Optional[str] = None,
                            end_date: Optional[str] = None,
                            months_back: int = 60) -> pd.DataFrame:
    """
    Recupere l'historique des prix depuis sikafinance.com.
    Utilise le formulaire POST avec token CSRF.
    Le site limite a ~1 mois par requete, donc on boucle.
    """
    session = _get_session()

    if end_date:
        dt_end = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
    else:
        dt_end = datetime.now()

    if start_date:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
    else:
        dt_start = dt_end - timedelta(days=months_back * 30)

    url = f"{SIKA_DOWNLOAD_URL}{ticker}"

    # 1. GET la page pour obtenir le token CSRF
    try:
        page_resp = session.get(url, timeout=30)
        page_resp.raise_for_status()
    except requests.RequestException:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    soup = BeautifulSoup(page_resp.text, "lxml")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    csrf_token = token_input.get("value", "") if token_input else ""

    # 2. POST par tranches de 1 mois
    all_data = []
    current_end = dt_end

    while current_end > dt_start:
        current_start = max(current_end - timedelta(days=30), dt_start)

        form_data = {
            "dtFrom": current_start.strftime("%Y-%m-%d"),
            "dtTo": current_end.strftime("%Y-%m-%d"),
            "__RequestVerificationToken": csrf_token,
        }

        try:
            resp = session.post(url, data=form_data, timeout=30)
            content_type = resp.headers.get("Content-Type", "")

            if resp.status_code == 200 and ("csv" in content_type or "octet" in content_type or ";" in resp.text[:200]):
                try:
                    df = pd.read_csv(io.StringIO(resp.text), sep=";", encoding="utf-8", decimal=",")
                    if not df.empty and len(df.columns) >= 3:
                        all_data.append(df)
                except Exception:
                    try:
                        df = pd.read_csv(io.StringIO(resp.text), sep=",", encoding="utf-8")
                        if not df.empty and len(df.columns) >= 3:
                            all_data.append(df)
                    except Exception:
                        pass
        except requests.RequestException:
            pass

        current_end = current_start - timedelta(days=1)
        time.sleep(0.5)

    if not all_data:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.concat(all_data, ignore_index=True)

    # Normaliser les noms de colonnes
    col_map = {
        "code": "ticker_code", "date": "date",
        "ouverture": "open", "plus haut": "high", "plus bas": "low",
        "cl": "close", "volume": "volume",
    }
    rename = {}
    for col in df.columns:
        col_lower = col.strip().lower()
        for pattern, target in col_map.items():
            if pattern in col_lower:
                rename[col] = target
                break
    if rename:
        df = df.rename(columns=rename)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    expected = ["date", "open", "high", "low", "close", "volume"]
    for col in expected:
        if col not in df.columns:
            df[col] = 0.0
    return df[expected]


def fetch_historical_prices_page(ticker: str, period: str = "mensuel",
                                 years_back: int = 5) -> pd.DataFrame:
    """
    Recupere l'historique des prix via l'API JSON sikafinance /api/general/GetHistos.
    Supporte les periodicites: journalier, hebdomadaire, mensuel, trimestriel, annuel.
    Le mode mensuel donne jusqu'a 5 ans d'historique en une seule requete.

    Args:
        ticker: ex "SNTS.sn"
        period: "journalier", "hebdomadaire", "mensuel", "trimestriel", "annuel"
        years_back: nombre d'annees en arriere

    Returns:
        DataFrame avec colonnes: date, open, high, low, close, volume
    """
    period_map = {
        "journalier": "0",
        "hebdomadaire": "7",
        "mensuel": "30",
        "trimestriel": "91",
        "annuel": "365",
    }
    xperiod = period_map.get(period, "30")

    dt_end = datetime.now()
    dt_start = dt_end - timedelta(days=years_back * 365)

    payload = {
        "ticker": ticker,
        "datedeb": dt_start.strftime("%d/%m/%Y"),
        "datefin": dt_end.strftime("%d/%m/%Y"),
        "xperiod": xperiod,
    }

    session = _get_session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Accept"] = "application/json"

    try:
        resp = session.post(
            f"{SIKA_BASE_URL}/api/general/GetHistos",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    lst = data.get("lst", [])
    if not lst:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    rows = []
    for item in lst:
        rows.append({
            "date": item.get("Date"),
            "open": item.get("Open"),
            "high": item.get("High"),
            "low": item.get("Low"),
            "close": item.get("Close"),
            "volume": item.get("Volume"),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def aggregate_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Agrege des donnees journalieres en hebdomadaire."""
    if df.empty or "date" not in df.columns:
        return df
    df = df.copy()
    df["week"] = df["date"].dt.to_period("W")
    agg = df.groupby("week").agg({
        "date": "last",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).reset_index(drop=True)
    return agg


def aggregate_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Agrege des donnees journalieres en mensuel."""
    if df.empty or "date" not in df.columns:
        return df
    df = df.copy()
    df["month"] = df["date"].dt.to_period("M")
    agg = df.groupby("month").agg({
        "date": "last",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).reset_index(drop=True)
    return agg


def fetch_best_available_prices(ticker: str, years_back: int = 5) -> dict:
    """
    Recupere les meilleurs prix disponibles pour un titre :
    1. Essaye les prix journaliers (limites a ~1-2 ans)
    2. Agrege en hebdo et mensuel
    3. Essaye aussi la page historiques pour plus de profondeur

    Returns:
        dict avec 'daily', 'weekly', 'monthly' DataFrames
    """
    # Journalier depuis le CSV download
    daily = fetch_historical_prices(ticker, months_back=years_back * 12)

    # Agréger
    weekly = aggregate_to_weekly(daily) if not daily.empty else pd.DataFrame()
    monthly = aggregate_to_monthly(daily) if not daily.empty else pd.DataFrame()

    # Essayer aussi la page historiques pour plus de donnees
    try:
        page_data = fetch_historical_prices_page(ticker, period="mensuel")
        if not page_data.empty and len(page_data) > len(monthly):
            monthly = page_data
    except Exception:
        pass

    return {
        "daily": daily,
        "weekly": weekly,
        "monthly": monthly,
    }


def fetch_stock_details(ticker: str) -> dict:
    """
    Recupere les details d'un titre depuis sa page sikafinance.

    Args:
        ticker: Code du titre (ex: "ECOC.ci")

    Returns:
        Dict avec: price, variation, market_cap, beta, rsi,
                   dividend_history, performance_periods
    """
    session = _get_session()
    url = f"{SIKA_COTATION_URL}{ticker}"

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"error": str(e)}

    soup = BeautifulSoup(resp.text, "lxml")
    details = {
        "ticker": ticker,
        "price": 0.0,
        "variation": 0.0,
        "market_cap": 0.0,
        "beta": None,
        "rsi": None,
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "prev_close": 0.0,
        "volume_shares": 0,
        "volume_xof": 0,
        "dividend_history": [],
        "performance": {},
    }

    # Extraire les donnees depuis la page
    # Le prix principal est generalement dans un element prominent
    price_elem = soup.find("span", class_="cours")
    if price_elem is None:
        price_elem = soup.find("div", class_="stock-price")
    if price_elem is None:
        # Chercher dans les balises h1/h2
        for tag in soup.find_all(["h1", "h2", "span", "div"]):
            text = tag.get_text(strip=True).replace("\xa0", "").replace(" ", "")
            try:
                val = float(text.replace(",", "."))
                if val > 100:  # Prix BRVM > 100 FCFA generalement
                    details["price"] = val
                    break
            except (ValueError, TypeError):
                continue
    else:
        text = price_elem.get_text(strip=True).replace("\xa0", "").replace(" ", "").replace(",", ".")
        try:
            details["price"] = float(text)
        except (ValueError, TypeError):
            pass

    # Chercher les tableaux de donnees
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True).replace("\xa0", "").replace(" ", "").replace(",", ".")

                try:
                    num_val = float(value.replace("%", ""))
                except (ValueError, TypeError):
                    num_val = None

                if "beta" in label and num_val is not None:
                    details["beta"] = num_val
                elif "rsi" in label and num_val is not None:
                    details["rsi"] = num_val
                elif "capitalisation" in label and num_val is not None:
                    details["market_cap"] = num_val
                elif "ouverture" in label and num_val is not None:
                    details["open"] = num_val
                elif "haut" in label and num_val is not None:
                    details["high"] = num_val
                elif "bas" in label and num_val is not None:
                    details["low"] = num_val

    # Chercher l'historique des dividendes
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if any("dividende" in h or "rendement" in h or "yield" in h for h in headers):
            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    year_text = cells[0].get_text(strip=True)
                    amount_text = cells[1].get_text(strip=True).replace("\xa0", "").replace(" ", "").replace(",", ".")
                    try:
                        year = int(year_text)
                        amount = float(amount_text)
                        yield_val = None
                        if len(cells) >= 3:
                            y_text = cells[2].get_text(strip=True).replace("%", "").replace(",", ".")
                            try:
                                yield_val = float(y_text)
                            except (ValueError, TypeError):
                                pass
                        details["dividend_history"].append({
                            "year": year,
                            "amount": amount,
                            "yield": yield_val,
                        })
                    except (ValueError, TypeError):
                        continue

    return details


def fetch_sector_indices() -> pd.DataFrame:
    """
    Recupere les indices sectoriels BRVM depuis sikafinance.
    """
    session = _get_session()
    try:
        resp = session.get(SIKA_AAZ_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "lxml")

    indices = []
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if any("indice" in h or "index" in h for h in headers):
            for tr in table.find_all("tr")[1:]:
                cells = tr.find_all("td")
                if len(cells) >= 3:
                    def _parse(cell):
                        t = cell.get_text(strip=True).replace("\xa0", "").replace(" ", "").replace(",", ".").replace("%", "")
                        try:
                            return float(t)
                        except (ValueError, TypeError):
                            return None
                    indices.append({
                        "name": cells[0].get_text(strip=True),
                        "value": _parse(cells[1]),
                        "variation": _parse(cells[2]),
                    })

    return pd.DataFrame(indices)


def enrich_stock_from_sika(ticker: str) -> dict:
    """
    Enrichit automatiquement un titre avec toutes les donnees disponibles
    sur sikafinance: prix, beta, RSI, market cap, historique dividendes.
    Retourne un dict partiel compatible avec save_fundamentals/save_market_data.
    """
    details = fetch_stock_details(ticker)
    if "error" in details:
        return details

    # Construire les DPS historiques depuis l'historique des dividendes
    div_history = details.get("dividend_history", [])
    dps_by_year = {d["year"]: d["amount"] for d in div_history if d.get("amount")}
    yield_by_year = {d["year"]: d["yield"] for d in div_history if d.get("yield")}

    # Dernier DPS connu
    latest_dps = None
    if dps_by_year:
        latest_year = max(dps_by_year.keys())
        latest_dps = dps_by_year[latest_year]

    return {
        "ticker": ticker,
        "price": details.get("price"),
        "market_cap": details.get("market_cap"),
        "beta": details.get("beta"),
        "rsi": details.get("rsi"),
        "dps": latest_dps,
        "dividend_history": div_history,
        "dps_by_year": dps_by_year,
        "yield_by_year": yield_by_year,
    }


def enrich_all_stocks(tickers: list, progress_callback=None) -> list:
    """
    Enrichit tous les titres donnes avec les donnees sikafinance.
    Args:
        tickers: liste de dicts avec 'ticker' et 'name'
        progress_callback: callable(current, total, ticker) pour suivi progression
    Returns:
        liste de dicts enrichis
    """
    results = []
    total = len(tickers)
    for i, t in enumerate(tickers):
        ticker = t["ticker"]
        if progress_callback:
            progress_callback(i, total, ticker)
        try:
            data = enrich_stock_from_sika(ticker)
            data["name"] = t.get("name", "")
            data["sector"] = t.get("sector", "")
            results.append(data)
        except Exception:
            results.append({"ticker": ticker, "name": t.get("name", ""), "error": "scrape_failed"})
        time.sleep(0.4)  # Politeness
    return results


def fetch_brvm_publications(verify_ssl: bool = False) -> list:
    """
    Tente de recuperer les publications recentes (etats financiers, rapports)
    depuis brvm.org ou sikafinance.
    Retourne une liste de dicts avec: ticker, title, date, type, url
    """
    publications = []
    session = _get_session()

    # Essayer sikafinance pour les actualites/publications
    try:
        for ticker_suffix in ["SNTS.sn", "ECOC.ci", "ORAC.ci", "SGBC.ci", "NSBC.ci"]:
            url = f"{SIKA_COTATION_URL}{ticker_suffix}"
            resp = session.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            # Chercher les liens vers des documents/communications
            for link in soup.find_all("a"):
                href = link.get("href", "")
                text = link.get_text(strip=True).lower()
                if any(kw in text for kw in [
                    "resultat", "bilan", "etats financiers", "rapport",
                    "trimestriel", "semestriel", "annuel", "comptes",
                    "chiffre d'affaires", "publication",
                ]):
                    full_url = href if href.startswith("http") else f"{SIKA_BASE_URL}{href}"
                    pub_type = "annuel"
                    if "trimestriel" in text or "t1" in text or "t2" in text or "t3" in text:
                        pub_type = "trimestriel"
                    elif "semestriel" in text or "s1" in text:
                        pub_type = "semestriel"

                    publications.append({
                        "ticker": ticker_suffix,
                        "title": link.get_text(strip=True),
                        "type": pub_type,
                        "url": full_url,
                        "date": datetime.now().strftime("%Y-%m-%d"),
                    })
            time.sleep(0.3)
    except Exception:
        pass

    return publications


def fetch_all_quotes_with_details() -> pd.DataFrame:
    """
    Recupere toutes les cotations et enrichit avec les details de chaque titre.
    A utiliser avec parcimonie (beaucoup de requetes).
    """
    quotes = fetch_daily_quotes()
    if quotes.empty:
        return quotes

    enriched = []
    for _, row in quotes.iterrows():
        if row["ticker"]:
            details = fetch_stock_details(row["ticker"])
            row_dict = row.to_dict()
            row_dict.update({
                "market_cap": details.get("market_cap", 0),
                "beta": details.get("beta"),
                "rsi": details.get("rsi"),
            })
            enriched.append(row_dict)
            time.sleep(0.3)  # Politeness

    return pd.DataFrame(enriched) if enriched else quotes


def fetch_company_profile(ticker: str) -> dict:
    """
    Scrape la page SOCIETE de sikafinance pour recuperer le profil qualitatif :
    description, dirigeants, contact, actionnariat.
    """
    import re

    url = f"{SIKA_BASE_URL}/marches/societe/{ticker}"
    result = {
        "ticker": ticker,
        "description": None,
        "business": None,
        "president": None,
        "dg": None,
        "dga": None,
        "phone": None,
        "fax": None,
        "address": None,
        "website": None,
        "major_shareholder": None,
        "major_shareholder_pct": None,
    }

    session = _get_session()
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        return result

    soup = BeautifulSoup(resp.text, "lxml")

    # --- Extract description from <p> tags only (not divs, to avoid concatenated noise) ---
    desc_candidates = []
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        # Skip short, nav, or script text
        if len(text) < 80:
            continue
        if any(kw in text.lower() for kw in ["javascript", "cookie", "navigateur", "copyright"]):
            continue
        # Skip if it looks like a page header (contains ticker codes, navigation items)
        if "COURSGRAPHIQUES" in text.replace(" ", "") or "ACTUSANALYSE" in text.replace(" ", ""):
            continue
        lower = text.lower()
        if any(kw in lower for kw in [
            "creee en", "cree en", "fondee en", "fonde en",
            "est un", "est une", "est le", "est la",
            "operateur", "banque", "societe", "filiale", "groupe",
            "activite", "entreprise", "compagnie",
        ]):
            desc_candidates.append(text)

    if desc_candidates:
        # Pick the best description paragraph (longest, but only from actual <p> tags)
        result["description"] = max(desc_candidates, key=len)

    # --- Extract individual <p> tags for structured info ---
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if not text or len(text) > 500:
            continue  # Skip very long concatenated blocks

        # Phone
        if ("phone" in text.lower() or "tel" in text.lower() or "(+2" in text) and "fax" not in text.lower():
            phone_match = re.search(r"\(\+\d+\)\s*[\d\s\-\.]+", text)
            if phone_match and not result["phone"]:
                result["phone"] = phone_match.group(0).strip()

        # Fax
        if "fax" in text.lower():
            fax_match = re.search(r"Fax.*?(\(\+\d+\)\s*[\d\s\-\.]+)", text, re.IGNORECASE)
            if fax_match:
                result["fax"] = fax_match.group(1).strip()

        # Address - look for lines with country names or city names
        if any(loc in text for loc in ["Abidjan", "Dakar", "Bamako", "Cotonou", "Lome",
                                        "Ouagadougou", "Niger", "Senegal", "Ivoire", "Togo",
                                        "Benin", "Burkina", "Mali", "Guinee"]):
            if len(text) < 200 and not result["address"]:
                # Avoid picking up description paragraphs as address
                if not any(kw in text.lower() for kw in ["creee", "fondee", "filiale", "activite"]):
                    result["address"] = text

        # President / DG / DGA - only from short, focused lines
        lower_text = text.lower()
        if len(text) < 200:
            # President du Conseil
            pca_match = re.search(r"[Pp]r[eé]sident[e]?\s+(?:du\s+)?[Cc]onseil[^:]*:\s*(.+?)(?:Directeur|Administrateur|Secr|$)", text)
            if pca_match and not result["president"]:
                result["president"] = pca_match.group(1).strip().rstrip(",").strip()

            # PDG (President Directeur General)
            pdg_match = re.search(r"[Pp]r[eé]sident[e]?\s+[Dd]irecteur\s+[Gg][eé]n[eé]ral[e]?\s*:?\s*(.+?)$", text)
            if pdg_match and not result["dg"]:
                name = pdg_match.group(1).strip().rstrip(",").strip()
                if len(name) > 2:
                    result["dg"] = name
                    result["president"] = result["president"] or name

            # DG (Directeur General) - but not "Directeur General Adjoint"
            if "directeur" in lower_text and "g" in lower_text and "adjoint" not in lower_text and "president" not in lower_text:
                dg_match = re.search(r"[Dd]irecteur\s+[Gg][eé]n[eé]ral[e]?\s*:?\s*(.+?)(?:Administrateur|Secr|Directeur|$)", text)
                if dg_match and not result["dg"]:
                    name = dg_match.group(1).strip().rstrip(",").strip()
                    if len(name) > 2 and len(name) < 80:
                        result["dg"] = name

            # DGA
            if "adjoint" in lower_text:
                dga_match = re.search(r"[Aa]djoint[e]?\s*:?\s*(.+?)$", text)
                if dga_match and not result["dga"]:
                    name = dga_match.group(1).strip().rstrip(",").strip()
                    if len(name) > 2 and len(name) < 80:
                        result["dga"] = name

        # Major shareholder
        if ("actionnaire" in lower_text or ("capital" in lower_text and "%" in text)) and len(text) < 300:
            pct_match = re.search(r"([\d,\.]+)\s*%", text)
            if pct_match:
                try:
                    result["major_shareholder_pct"] = float(pct_match.group(1).replace(",", "."))
                except ValueError:
                    pass
                # Extract name: typically before the percentage
                name_part = text.split(":")[1].strip() if ":" in text else text
                before_pct = name_part[:name_part.find(pct_match.group(0))].strip().rstrip("(").strip()
                if before_pct and len(before_pct) > 2:
                    result["major_shareholder"] = before_pct

    return result


def fetch_company_news(ticker: str, max_articles: int = 10) -> list:
    """
    Scrape les actualites recentes d'un titre depuis sikafinance.
    Retourne une liste de dicts: title, date, url, summary.
    """
    url = f"{SIKA_COTATION_URL}{ticker}"
    articles = []

    session = _get_session()
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        return articles

    soup = BeautifulSoup(resp.text, "lxml")

    # News articles are typically in divs/lists with links containing dates
    # Look for article-like elements
    import re

    # Pattern 1: Look for news items with dates (dd/mm/yy format)
    for container in soup.find_all(["div", "li", "tr"]):
        links = container.find_all("a")
        for link in links:
            href = link.get("href", "")
            title = link.get_text(strip=True)

            # Filter: must be a meaningful article title (not navigation)
            if len(title) < 20 or len(title) > 300:
                continue
            if any(kw in title.lower() for kw in ["connexion", "inscription", "accueil", "forum",
                                                    "cookie", "politique", "mention",
                                                    "actualités du march", "actualites du march"]):
                continue

            # Check if it looks like a news article URL
            if "/actualites/" in href or "/blog/" in href or "/article/" in href or "actualite" in href:
                full_url = href if href.startswith("http") else f"{SIKA_BASE_URL}{href}"

                # Try to find a date near this link
                parent = link.parent
                date_text = parent.get_text() if parent else ""
                date_match = re.search(r"(\d{2}/\d{2}/\d{2,4})", date_text)
                article_date = date_match.group(1) if date_match else None

                # Avoid duplicates
                if not any(a["title"] == title for a in articles):
                    articles.append({
                        "title": title,
                        "date": article_date,
                        "url": full_url,
                    })

                if len(articles) >= max_articles:
                    break
        if len(articles) >= max_articles:
            break

    # Pattern 2: Look for "communiques" / corporate documents
    for link in soup.find_all("a"):
        href = link.get("href", "")
        title = link.get_text(strip=True)
        if any(kw in title.lower() for kw in ["communique", "avis de convocation", "assemblee",
                                                "publication", "rapport", "etats financiers"]):
            if len(title) > 10:
                full_url = href if href.startswith("http") else f"{SIKA_BASE_URL}{href}"
                if not any(a["title"] == title for a in articles):
                    articles.append({
                        "title": title,
                        "date": None,
                        "url": full_url,
                    })

    return articles[:max_articles]
