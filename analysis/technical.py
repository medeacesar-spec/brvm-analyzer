"""
Moteur d'analyse technique - Indicateurs et détection de patterns.
RSI, MACD, Bollinger, Moyennes Mobiles, Supports/Résistances.
"""

from typing import Optional

import numpy as np
import pandas as pd

from config import TECHNICAL_PARAMS


def compute_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule tous les indicateurs techniques sur un DataFrame de prix.
    Le DataFrame doit contenir: date, open, high, low, close, volume.
    Retourne le DataFrame enrichi des colonnes indicateurs.
    """
    if df.empty or "close" not in df.columns:
        return df

    df = df.copy().sort_values("date").reset_index(drop=True)

    # Moyennes Mobiles
    df["sma20"] = df["close"].rolling(window=TECHNICAL_PARAMS["sma_short"]).mean()
    df["sma50"] = df["close"].rolling(window=TECHNICAL_PARAMS["sma_medium"]).mean()
    df["sma200"] = df["close"].rolling(window=TECHNICAL_PARAMS["sma_long"]).mean()

    # Bandes de Bollinger
    period = TECHNICAL_PARAMS["bollinger_period"]
    std_dev = TECHNICAL_PARAMS["bollinger_std"]
    df["bb_middle"] = df["close"].rolling(window=period).mean()
    rolling_std = df["close"].rolling(window=period).std()
    df["bb_upper"] = df["bb_middle"] + (rolling_std * std_dev)
    df["bb_lower"] = df["bb_middle"] - (rolling_std * std_dev)

    # RSI
    df["rsi"] = _compute_rsi(df["close"], TECHNICAL_PARAMS["rsi_period"])

    # MACD
    fast = TECHNICAL_PARAMS["macd_fast"]
    slow = TECHNICAL_PARAMS["macd_slow"]
    signal = TECHNICAL_PARAMS["macd_signal"]
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    df["macd"] = ema_fast - ema_slow
    df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    df["macd_histogram"] = df["macd"] - df["macd_signal"]

    # Volume moyen
    df["volume_sma20"] = df["volume"].rolling(window=20).mean() if "volume" in df.columns else None

    # Variation quotidienne
    df["daily_return"] = df["close"].pct_change()

    return df


def _compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calcule le RSI (Relative Strength Index)."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    # Use exponential moving average after initial SMA
    for i in range(period, len(series)):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (period - 1) + loss.iloc[i]) / period

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def detect_support_resistance(df: pd.DataFrame, window: int = 20, threshold: float = 0.02) -> dict:
    """
    Détecte les niveaux de support et résistance basés sur les pivots locaux.

    Returns:
        dict avec 'supports' et 'resistances' (listes de prix)
    """
    if df.empty or len(df) < window * 2:
        return {"supports": [], "resistances": []}

    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    current_price = closes[-1]

    supports = []
    resistances = []

    # Detect local minima (supports) and maxima (resistances)
    for i in range(window, len(df) - window):
        # Local minimum
        if lows[i] == min(lows[i - window:i + window + 1]):
            supports.append(lows[i])
        # Local maximum
        if highs[i] == max(highs[i - window:i + window + 1]):
            resistances.append(highs[i])

    # Cluster nearby levels
    supports = _cluster_levels(supports, threshold)
    resistances = _cluster_levels(resistances, threshold)

    # Filter: supports below current price, resistances above
    supports = sorted([s for s in supports if s < current_price], reverse=True)[:5]
    resistances = sorted([r for r in resistances if r > current_price])[:5]

    return {"supports": supports, "resistances": resistances}


def _cluster_levels(levels: list, threshold: float = 0.02) -> list:
    """Regroupe les niveaux proches en clusters."""
    if not levels:
        return []

    levels = sorted(levels)
    clusters = [[levels[0]]]

    for level in levels[1:]:
        if abs(level - clusters[-1][-1]) / clusters[-1][-1] < threshold:
            clusters[-1].append(level)
        else:
            clusters.append([level])

    return [np.mean(c) for c in clusters]


def detect_trend(df: pd.DataFrame) -> dict:
    """
    Détecte la tendance actuelle basée sur les moyennes mobiles.

    Returns:
        dict avec 'trend' (haussiere/baissiere/neutre),
        'strength' (forte/moderee/faible), 'details'
    """
    if df.empty or len(df) < 200:
        return {"trend": "indetermine", "strength": "N/A", "details": "Historique insuffisant"}

    last = df.iloc[-1]
    price = last["close"]
    sma20 = last.get("sma20")
    sma50 = last.get("sma50")
    sma200 = last.get("sma200")

    if pd.isna(sma20) or pd.isna(sma50) or pd.isna(sma200):
        return {"trend": "indetermine", "strength": "N/A", "details": "Indicateurs insuffisants"}

    signals = []

    # Price vs MAs
    if price > sma20 > sma50 > sma200:
        signals.append(("haussiere", "forte", "Prix > MM20 > MM50 > MM200"))
    elif price > sma50 > sma200:
        signals.append(("haussiere", "moderee", "Prix > MM50 > MM200"))
    elif price > sma200:
        signals.append(("haussiere", "faible", "Prix > MM200"))
    elif price < sma20 < sma50 < sma200:
        signals.append(("baissiere", "forte", "Prix < MM20 < MM50 < MM200"))
    elif price < sma50 < sma200:
        signals.append(("baissiere", "moderee", "Prix < MM50 < MM200"))
    elif price < sma200:
        signals.append(("baissiere", "faible", "Prix < MM200"))
    else:
        signals.append(("neutre", "faible", "Pas de tendance claire"))

    return {
        "trend": signals[0][0],
        "strength": signals[0][1],
        "details": signals[0][2],
    }


def generate_signals(df: pd.DataFrame) -> list:
    """
    Génère des signaux d'achat/vente basés sur les indicateurs techniques.

    Returns:
        liste de dicts avec: type (achat/vente), signal, strength (1-5), details
    """
    if df.empty or len(df) < 50:
        return []

    signals = []
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    # --- RSI ---
    rsi = last.get("rsi")
    if rsi is not None and not pd.isna(rsi):
        if rsi < TECHNICAL_PARAMS["rsi_oversold"]:
            signals.append({
                "type": "achat",
                "signal": "RSI en survente",
                "strength": 4,
                "details": f"RSI = {rsi:.1f} (< {TECHNICAL_PARAMS['rsi_oversold']})",
            })
        elif rsi > TECHNICAL_PARAMS["rsi_overbought"]:
            signals.append({
                "type": "vente",
                "signal": "RSI en surachat",
                "strength": 3,
                "details": f"RSI = {rsi:.1f} (> {TECHNICAL_PARAMS['rsi_overbought']})",
            })

    # --- Golden Cross / Death Cross (MM20 x MM50) ---
    sma20_now = last.get("sma20")
    sma50_now = last.get("sma50")
    sma20_prev = prev.get("sma20")
    sma50_prev = prev.get("sma50")

    if all(v is not None and not pd.isna(v) for v in [sma20_now, sma50_now, sma20_prev, sma50_prev]):
        if sma20_prev <= sma50_prev and sma20_now > sma50_now:
            signals.append({
                "type": "achat",
                "signal": "Golden Cross MM20/MM50",
                "strength": 5,
                "details": "La MM20 croise la MM50 par le haut",
            })
        elif sma20_prev >= sma50_prev and sma20_now < sma50_now:
            signals.append({
                "type": "vente",
                "signal": "Death Cross MM20/MM50",
                "strength": 4,
                "details": "La MM20 croise la MM50 par le bas",
            })

    # --- MACD crossover ---
    macd_now = last.get("macd")
    macd_sig_now = last.get("macd_signal")
    macd_prev = prev.get("macd")
    macd_sig_prev = prev.get("macd_signal")

    if all(v is not None and not pd.isna(v) for v in [macd_now, macd_sig_now, macd_prev, macd_sig_prev]):
        if macd_prev <= macd_sig_prev and macd_now > macd_sig_now:
            signals.append({
                "type": "achat",
                "signal": "MACD croisement haussier",
                "strength": 3,
                "details": "MACD croise sa ligne de signal par le haut",
            })
        elif macd_prev >= macd_sig_prev and macd_now < macd_sig_now:
            signals.append({
                "type": "vente",
                "signal": "MACD croisement baissier",
                "strength": 3,
                "details": "MACD croise sa ligne de signal par le bas",
            })

    # --- Bollinger Bands ---
    bb_lower = last.get("bb_lower")
    bb_upper = last.get("bb_upper")
    price = last["close"]

    if bb_lower is not None and not pd.isna(bb_lower) and price <= bb_lower:
        signals.append({
            "type": "achat",
            "signal": "Prix sur bande Bollinger basse",
            "strength": 3,
            "details": f"Prix ({price:,.0f}) touche la bande basse ({bb_lower:,.0f})",
        })
    elif bb_upper is not None and not pd.isna(bb_upper) and price >= bb_upper:
        signals.append({
            "type": "vente",
            "signal": "Prix sur bande Bollinger haute",
            "strength": 2,
            "details": f"Prix ({price:,.0f}) touche la bande haute ({bb_upper:,.0f})",
        })

    # --- Volume spike ---
    vol = last.get("volume")
    vol_avg = last.get("volume_sma20")
    if vol is not None and vol_avg is not None and not pd.isna(vol_avg) and vol_avg > 0:
        if vol > vol_avg * 2:
            signals.append({
                "type": "info",
                "signal": "Volume anormalement eleve",
                "strength": 2,
                "details": f"Volume {vol:,.0f} vs moyenne {vol_avg:,.0f} (x{vol / vol_avg:.1f})",
            })

    return signals


def compute_technical_score(df: pd.DataFrame) -> float:
    """
    Calcule un score technique sur 50 points.
    Pondération :
    - Tendance (MM alignment): 15 pts
    - RSI: 10 pts
    - MACD: 10 pts
    - Bollinger position: 5 pts
    - Volume: 5 pts
    - Momentum (prix vs supports): 5 pts
    """
    if df.empty or len(df) < 50:
        return 25  # Score neutre

    score = 0
    last = df.iloc[-1]

    # Tendance (15 pts)
    trend = detect_trend(df)
    if trend["trend"] == "haussiere":
        if trend["strength"] == "forte":
            score += 15
        elif trend["strength"] == "moderee":
            score += 10
        else:
            score += 7
    elif trend["trend"] == "neutre":
        score += 7
    else:  # baissiere
        if trend["strength"] == "forte":
            score += 0
        elif trend["strength"] == "moderee":
            score += 3
        else:
            score += 5

    # RSI (10 pts) - score maximal autour de 40-60
    rsi = last.get("rsi")
    if rsi is not None and not pd.isna(rsi):
        if 40 <= rsi <= 60:
            score += 7
        elif 30 <= rsi < 40:
            score += 10  # Survente = opportunité
        elif rsi < 30:
            score += 8
        elif 60 < rsi <= 70:
            score += 5
        else:  # > 70
            score += 2

    # MACD (10 pts)
    macd = last.get("macd")
    macd_hist = last.get("macd_histogram")
    if macd is not None and not pd.isna(macd):
        if macd > 0 and macd_hist is not None and macd_hist > 0:
            score += 10
        elif macd > 0:
            score += 7
        elif macd_hist is not None and macd_hist > 0:
            score += 5
        else:
            score += 2

    # Bollinger (5 pts)
    bb_lower = last.get("bb_lower")
    bb_upper = last.get("bb_upper")
    bb_middle = last.get("bb_middle")
    price = last["close"]
    if all(v is not None and not pd.isna(v) for v in [bb_lower, bb_upper, bb_middle]):
        bb_range = bb_upper - bb_lower
        if bb_range > 0:
            position = (price - bb_lower) / bb_range
            if 0.2 <= position <= 0.5:
                score += 5  # Bonne zone d'achat
            elif position < 0.2:
                score += 4  # Survente
            elif 0.5 < position <= 0.8:
                score += 3
            else:
                score += 1

    # Volume (5 pts)
    vol = last.get("volume")
    vol_avg = last.get("volume_sma20")
    if vol is not None and vol_avg is not None and not pd.isna(vol_avg) and vol_avg > 0:
        vol_ratio = vol / vol_avg
        if 0.8 <= vol_ratio <= 1.5:
            score += 4
        elif vol_ratio > 1.5:
            score += 5  # Volume élevé = intérêt
        else:
            score += 2

    # Momentum (5 pts) - Performance récente
    if len(df) >= 20:
        perf_20d = (df["close"].iloc[-1] / df["close"].iloc[-20] - 1) if df["close"].iloc[-20] != 0 else 0
        if 0 < perf_20d <= 0.05:
            score += 5
        elif 0.05 < perf_20d <= 0.10:
            score += 4
        elif perf_20d > 0.10:
            score += 3  # Suracheté
        elif -0.05 <= perf_20d <= 0:
            score += 3
        else:
            score += 1

    return score
