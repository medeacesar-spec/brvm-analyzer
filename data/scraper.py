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


def fetch_daily_quotes() -> pd.DataFrame:
    """
    Recupere les cotations du jour depuis sikafinance.com/marches/aaz.

    Returns:
        DataFrame avec colonnes: ticker, name, open, high, low, volume_shares,
                                  volume_xof, last, variation
    """
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
    Le site limite a 1 mois par requete, donc on boucle.

    Args:
        ticker: Code du titre (ex: "ECOC.ci", "SNTS.sn")
        start_date: Date de debut (format YYYY-MM-DD), defaut = months_back mois avant
        end_date: Date de fin (format YYYY-MM-DD), defaut = aujourd'hui
        months_back: Nombre de mois en arriere si start_date non specifie

    Returns:
        DataFrame avec colonnes: date, open, high, low, close, volume
    """
    session = _get_session()

    if end_date:
        dt_end = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        dt_end = datetime.now()

    if start_date:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        dt_start = dt_end - timedelta(days=months_back * 30)

    all_data = []
    current_end = dt_end

    while current_end > dt_start:
        current_start = max(current_end - timedelta(days=30), dt_start)

        params = {
            "Du": current_start.strftime("%d/%m/%Y"),
            "au": current_end.strftime("%d/%m/%Y"),
        }

        url = f"{SIKA_DOWNLOAD_URL}{ticker}"
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 50:
                # Essayer de parser le CSV
                try:
                    df = pd.read_csv(
                        io.StringIO(resp.text),
                        sep=";",
                        encoding="utf-8",
                        decimal=",",
                    )
                    if not df.empty:
                        all_data.append(df)
                except Exception:
                    # Essayer avec virgule comme separateur
                    try:
                        df = pd.read_csv(
                            io.StringIO(resp.text),
                            sep=",",
                            encoding="utf-8",
                        )
                        if not df.empty:
                            all_data.append(df)
                    except Exception:
                        pass
        except requests.RequestException:
            pass  # Skip ce mois en cas d'erreur

        current_end = current_start - timedelta(days=1)
        time.sleep(0.5)  # Politeness delay

    if not all_data:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.concat(all_data, ignore_index=True)

    # Normaliser les noms de colonnes
    col_map = {
        "Code de la valeur": "ticker_code",
        "Date": "date",
        "Cours d'ouverture": "open",
        "Plus haut": "high",
        "Plus bas": "low",
        "Cours de clôture": "close",
        "Cours de cloture": "close",
        "Volume d'actions échangées": "volume",
        "Volume d'actions echangees": "volume",
    }

    # Mapper les colonnes qu'on trouve
    rename = {}
    for old, new in col_map.items():
        for col in df.columns:
            if old.lower() in col.lower() or col.lower() in old.lower():
                rename[col] = new
                break

    if rename:
        df = df.rename(columns=rename)

    # Convertir la date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date").reset_index(drop=True)
        df = df.drop_duplicates(subset=["date"], keep="last")

    # Assurer les colonnes numeriques
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    expected_cols = ["date", "open", "high", "low", "close", "volume"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = 0.0

    return df[expected_cols]


def fetch_historical_prices_page(ticker: str, period: str = "mensuel",
                                 years_back: int = 5) -> pd.DataFrame:
    """
    Scrape la page /marches/historiques/ de sikafinance pour les prix
    hebdomadaires ou mensuels (plus complets que le CSV journalier).

    Args:
        ticker: ex "SNTS.sn"
        period: "hebdomadaire" ou "mensuel"
        years_back: nombre d'annees en arriere

    Returns:
        DataFrame avec colonnes: date, open, high, low, close, volume
    """
    session = _get_session()
    base_url = f"{SIKA_BASE_URL}/marches/historiques/{ticker}"

    # La page historiques utilise des parametres de periode
    period_map = {"journalier": "Journaliere", "hebdomadaire": "Hebdomadaire", "mensuel": "Mensuelle"}
    period_label = period_map.get(period, "Mensuelle")

    all_rows = []
    try:
        # Premiere requete pour obtenir la page et le token
        resp = session.get(base_url, timeout=30)
        if resp.status_code != 200:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        soup = BeautifulSoup(resp.text, "lxml")

        # Chercher le tableau de prix historiques
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if any(h in headers for h in ["date", "ouverture", "clôture", "cloture"]):
                for tr in table.find_all("tr")[1:]:
                    cells = tr.find_all("td")
                    if len(cells) >= 5:
                        def _p(cell):
                            t = cell.get_text(strip=True).replace("\xa0", "").replace(" ", "").replace(",", ".")
                            try:
                                return float(t)
                            except (ValueError, TypeError):
                                return None

                        date_text = cells[0].get_text(strip=True)
                        all_rows.append({
                            "date": date_text,
                            "open": _p(cells[1]),
                            "high": _p(cells[2]),
                            "low": _p(cells[3]),
                            "close": _p(cells[4]),
                            "volume": _p(cells[5]) if len(cells) > 5 else None,
                        })
    except Exception:
        pass

    if not all_rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
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
