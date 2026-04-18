"""
Configuration centrale pour BRVM Analyzer.
Seuils des ratios, constantes, paramètres de scraping.
"""

import json
import os

# --- Chemins ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "brvm_analyzer.db")
TICKERS_PATH = os.path.join(DATA_DIR, "brvm_tickers.json")

# --- URLs Sikafinance ---
SIKA_BASE_URL = "https://www.sikafinance.com"
SIKA_AAZ_URL = f"{SIKA_BASE_URL}/marches/aaz"
SIKA_COTATION_URL = f"{SIKA_BASE_URL}/marches/cotation_"  # + TICKER
SIKA_DOWNLOAD_URL = f"{SIKA_BASE_URL}/marches/download/"  # + TICKER

# --- Seuils Ratios (Approche Valeur & Dividendes BRVM) ---
RATIO_THRESHOLDS = {
    "roe": {
        "excellent": 0.20,
        "good": 0.15,
        "label": "ROE",
        "formula": "Résultat net / Capitaux propres",
        "rules": [
            (0.20, "OK", "Excellent"),
            (0.15, "OK", "Solide"),
            (0.10, "Vigilance", "Moyen"),
            (0.0, "Risque", "Faible"),
        ],
    },
    "net_margin": {
        "excellent": 0.15,
        "good": 0.10,
        "label": "Marge nette",
        "formula": "Résultat net / Chiffre d'affaires",
        "rules": [
            (0.15, "OK", "Tres bon"),
            (0.10, "OK", "Bon"),
            (0.05, "Vigilance", "Moyen"),
            (0.0, "Risque", "Faible"),
        ],
    },
    "debt_equity": {
        "max": 1.5,
        "label": "Dette / Capitaux propres",
        "formula": "Dette financiere / Capitaux propres",
        "note": "Hors banques",
        "rules": [
            # lower is better, inverted logic
        ],
    },
    "interest_coverage": {
        "comfortable": 3.0,
        "tension": 2.0,
        "label": "Couverture des intérêts",
        "formula": "EBIT / Charges d'intérêts",
        "rules": [
            (3.0, "OK", "Confortable"),
            (2.0, "Vigilance", "Tendu"),
            (0.0, "Risque", "Critique"),
        ],
    },
    "fcf_margin": {
        "excellent": 0.10,
        "good": 0.05,
        "label": "FCF Margin",
        "formula": "FCF / Chiffre d'affaires",
        "rules": [
            (0.10, "OK", "Tres bon"),
            (0.05, "OK", "Bon"),
            (0.0, "Vigilance", "Faible"),
        ],
    },
    "dividend_yield": {
        "target": 0.06,
        "label": "Dividend Yield",
        "formula": "DPS / Prix",
        "rules": [
            (0.06, "OK", "Cible atteinte"),
            (0.04, "Vigilance", "Sous la cible"),
            (0.0, "Risque", "Faible"),
        ],
    },
    "payout_ratio": {
        "max_ideal": 0.70,
        "max_sustainable": 1.0,
        "label": "Payout ratio",
        "formula": "DPS / EPS",
        "rules": [
            # lower is better (capped)
        ],
    },
    "per": {
        "attractive": 10,
        "value": 15,
        "label": "PER",
        "formula": "Prix / EPS",
        "rules": [
            # lower is better
        ],
    },
    "pb": {
        "max": 2.0,
        "label": "P/B (Price to Book)",
        "formula": "Prix / (Equity / Actions)",
        "note": "Hors banques: comparer a ROE & qualite",
    },
    "dividend_cash_coverage": {
        "comfortable": 1.2,
        "label": "Couverture dividende (cash)",
        "formula": "FCF / Dividendes verses",
        "rules": [
            (1.2, "OK", "Confort"),
            (1.0, "Vigilance", "Juste"),
            (0.0, "Risque", "Non couvert"),
        ],
    },
}

# --- Checklist Value & Dividendes ---
VALUE_CHECKLIST = {
    "dividend_yield": {"target": 0.06, "direction": ">=", "label": "Dividend Yield >= 6%"},
    "payout_ratio": {"target": 0.70, "direction": "<=", "label": "Payout ratio <= 70%"},
    "roe": {"target": 0.15, "direction": ">=", "label": "ROE >= 15%"},
    "per": {"target": 15, "direction": "<=", "label": "PER <= 15"},
    "debt_equity": {"target": 1.5, "direction": "<=", "label": "Dette/Equity <= 1.5 (hors banques)"},
    "dividend_cash_coverage": {"target": 1.2, "direction": ">=", "label": "Couverture dividende >= 1.2x"},
}

# --- Parametres Techniques ---
TECHNICAL_PARAMS = {
    "sma_short": 20,
    "sma_medium": 50,
    "sma_long": 200,
    "rsi_period": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "bollinger_period": 20,
    "bollinger_std": 2,
}

# --- Secteurs BRVM ---
SECTORS = {
    "Banque": "Finance - Banque",
    "Assurance": "Finance - Assurance",
    "Telecommunications": "Services - Telecoms",
    "Distribution": "Commerce - Distribution",
    "Industrie": "Industrie",
    "Agriculture": "Agriculture",
    "Transport": "Transport & Logistique",
    "Services publics": "Services publics",
    "Autres": "Autres",
}

# --- Couleurs drapeaux ---
FLAG_COLORS = {
    "OK": "#28a745",        # Vert
    "Vigilance": "#ffc107", # Jaune
    "Risque": "#dc3545",    # Rouge
    "Negatif": "#dc3545",
    "Eleve": "#ffc107",
    "Cher": "#dc3545",
    "Faible": "#ffc107",
    "A revoir": "#ffc107",
}

# --- Devise ---
CURRENCY = "FCFA"
CURRENCY_CODE = "XOF"


def load_tickers():
    """Charge la liste des tickers BRVM depuis le fichier JSON."""
    with open(TICKERS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)
