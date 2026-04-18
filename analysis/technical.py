"""
Moteur d'analyse technique - Indicateurs et détection de patterns.
RSI, MACD, Bollinger, Moyennes Mobiles, Supports/Résistances.
Auto-détecte la fréquence des données (journalière vs mensuelle) et adapte les paramètres.
"""

from typing import Optional

import numpy as np
import pandas as pd

from config import TECHNICAL_PARAMS

# Paramètres adaptés pour données mensuelles
MONTHLY_PARAMS = {
    "sma_short": 3,       # 3 mois
    "sma_medium": 6,      # 6 mois
    "sma_long": 12,       # 12 mois (1 an)
    "rsi_period": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
    "macd_fast": 6,
    "macd_slow": 12,
    "macd_signal": 4,
    "bollinger_period": 12,
    "bollinger_std": 2,
}

# Labels pour les moyennes mobiles selon la fréquence
SMA_LABELS = {
    "daily": {"short": "MM20", "medium": "MM50", "long": "MM200"},
    "monthly": {"short": "MM3m", "medium": "MM6m", "long": "MM12m"},
}


def _detect_frequency(df: pd.DataFrame) -> str:
    """Détecte si les données sont journalières ou mensuelles."""
    if len(df) < 3:
        return "daily"
    sorted_dates = df["date"].sort_values()
    median_gap = sorted_dates.diff().dropna().median()
    return "monthly" if median_gap.days > 15 else "daily"


def _get_params(df: pd.DataFrame) -> tuple:
    """Retourne (params_dict, frequency_str) adapté à la fréquence des données."""
    freq = _detect_frequency(df)
    if freq == "monthly":
        return MONTHLY_PARAMS, "monthly"
    return TECHNICAL_PARAMS, "daily"


def compute_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule tous les indicateurs techniques sur un DataFrame de prix.
    Le DataFrame doit contenir: date, open, high, low, close, volume.
    Auto-détecte la fréquence et adapte les paramètres.
    Retourne le DataFrame enrichi des colonnes indicateurs.
    """
    if df.empty or "close" not in df.columns:
        return df

    df = df.copy().sort_values("date").reset_index(drop=True)
    params, freq = _get_params(df)

    # Stocker la fréquence pour usage en aval
    df.attrs["frequency"] = freq

    # Moyennes Mobiles
    df["sma20"] = df["close"].rolling(window=params["sma_short"]).mean()
    df["sma50"] = df["close"].rolling(window=params["sma_medium"]).mean()
    df["sma200"] = df["close"].rolling(window=params["sma_long"]).mean()

    # Bandes de Bollinger
    period = params["bollinger_period"]
    std_dev = params["bollinger_std"]
    df["bb_middle"] = df["close"].rolling(window=period).mean()
    rolling_std = df["close"].rolling(window=period).std()
    df["bb_upper"] = df["bb_middle"] + (rolling_std * std_dev)
    df["bb_lower"] = df["bb_middle"] - (rolling_std * std_dev)

    # RSI
    df["rsi"] = _compute_rsi(df["close"], params["rsi_period"])

    # MACD
    fast = params["macd_fast"]
    slow = params["macd_slow"]
    signal = params["macd_signal"]
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    df["macd"] = ema_fast - ema_slow
    df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    df["macd_histogram"] = df["macd"] - df["macd_signal"]

    # Volume moyen
    vol_window = params.get("sma_short", 20)
    df["volume_sma20"] = df["volume"].rolling(window=vol_window).mean() if "volume" in df.columns else None

    # Variation par période
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


def detect_support_resistance(df: pd.DataFrame, window: int = None, threshold: float = 0.03) -> dict:
    """
    Détecte les niveaux de support et résistance basés sur les pivots locaux.
    Le window s'adapte automatiquement à la fréquence des données.

    Returns:
        dict avec 'supports' et 'resistances' (listes de prix)
    """
    freq = df.attrs.get("frequency", _detect_frequency(df)) if not df.empty else "daily"

    if window is None:
        window = 3 if freq == "monthly" else 20

    min_len = window * 2 + 1
    if df.empty or len(df) < min_len:
        return {"supports": [], "resistances": []}

    # Use close as fallback when high/low are 0 (bad data)
    df_clean = df.copy()
    df_clean["high"] = df_clean.apply(
        lambda r: r["high"] if r["high"] > 0 else r["close"], axis=1
    )
    df_clean["low"] = df_clean.apply(
        lambda r: r["low"] if r["low"] > 0 else r["close"], axis=1
    )

    highs = df_clean["high"].values
    lows = df_clean["low"].values
    closes = df_clean["close"].values
    current_price = closes[-1]

    supports = []
    resistances = []

    # Detect local minima (supports) and maxima (resistances)
    for i in range(window, len(df_clean) - window):
        # Local minimum
        if lows[i] == min(lows[i - window:i + window + 1]):
            supports.append(lows[i])
        # Local maximum
        if highs[i] == max(highs[i - window:i + window + 1]):
            resistances.append(highs[i])

    # Also check recent price levels (last 12 periods) for additional S/R
    recent = df.tail(max(12, window * 2))
    if len(recent) >= 3:
        # Add significant lows/highs from recent data
        q25_low = recent["low"].quantile(0.15)
        q75_high = recent["high"].quantile(0.85)
        for _, row in recent.iterrows():
            if row["low"] <= q25_low:
                supports.append(row["low"])
            if row["high"] >= q75_high:
                resistances.append(row["high"])

    # Cluster nearby levels
    supports = _cluster_levels(supports, threshold)
    resistances = _cluster_levels(resistances, threshold)

    # Filter: supports below current price, resistances above, exclude zeros
    supports = sorted([s for s in supports if 0 < s < current_price * 0.995], reverse=True)[:5]
    resistances = sorted([r for r in resistances if r > current_price * 1.005])[:5]

    return {"supports": supports, "resistances": resistances}


def _cluster_levels(levels: list, threshold: float = 0.02) -> list:
    """Regroupe les niveaux proches en clusters."""
    if not levels:
        return []

    levels = sorted(levels)
    clusters = [[levels[0]]]

    for level in levels[1:]:
        if clusters[-1][-1] != 0 and abs(level - clusters[-1][-1]) / abs(clusters[-1][-1]) < threshold:
            clusters[-1].append(level)
        else:
            clusters.append([level])

    return [np.mean(c) for c in clusters]


def detect_trend(df: pd.DataFrame) -> dict:
    """
    Détecte la tendance actuelle basée sur les moyennes mobiles.
    S'adapte à la fréquence des données (journalière/mensuelle).

    Returns:
        dict avec 'trend' (haussiere/baissiere/neutre),
        'strength' (forte/moderee/faible), 'details'
    """
    freq = df.attrs.get("frequency", _detect_frequency(df)) if not df.empty else "daily"
    labels = SMA_LABELS.get(freq, SMA_LABELS["daily"])

    # Minimum data: need enough for the longest MA to be computed
    min_len = 13 if freq == "monthly" else 200
    if df.empty or len(df) < min_len:
        return {"trend": "indetermine", "strength": "N/A", "details": "Historique insuffisant"}

    last = df.iloc[-1]
    price = last["close"]
    sma_short = last.get("sma20")  # column name stays sma20 but params adapted
    sma_mid = last.get("sma50")
    sma_long = last.get("sma200")

    if pd.isna(sma_short) or pd.isna(sma_mid) or pd.isna(sma_long):
        # Try with just short + medium if long isn't available
        if pd.notna(sma_short) and pd.notna(sma_mid):
            if price > sma_short > sma_mid:
                return {"trend": "haussiere", "strength": "moderee",
                         "details": f"Prix > {labels['short']} > {labels['medium']}"}
            elif price < sma_short < sma_mid:
                return {"trend": "baissiere", "strength": "moderee",
                         "details": f"Prix < {labels['short']} < {labels['medium']}"}
            else:
                return {"trend": "neutre", "strength": "faible", "details": "Pas de tendance claire"}
        return {"trend": "indetermine", "strength": "N/A", "details": "Indicateurs insuffisants"}

    # Price vs MAs
    if price > sma_short > sma_mid > sma_long:
        return {"trend": "haussiere", "strength": "forte",
                "details": f"Prix > {labels['short']} > {labels['medium']} > {labels['long']}"}
    elif price > sma_mid > sma_long:
        return {"trend": "haussiere", "strength": "moderee",
                "details": f"Prix > {labels['medium']} > {labels['long']}"}
    elif price > sma_long:
        return {"trend": "haussiere", "strength": "faible",
                "details": f"Prix > {labels['long']}"}
    elif price < sma_short < sma_mid < sma_long:
        return {"trend": "baissiere", "strength": "forte",
                "details": f"Prix < {labels['short']} < {labels['medium']} < {labels['long']}"}
    elif price < sma_mid < sma_long:
        return {"trend": "baissiere", "strength": "moderee",
                "details": f"Prix < {labels['medium']} < {labels['long']}"}
    elif price < sma_long:
        return {"trend": "baissiere", "strength": "faible",
                "details": f"Prix < {labels['long']}"}
    else:
        return {"trend": "neutre", "strength": "faible", "details": "Pas de tendance claire"}


def generate_signals(df: pd.DataFrame) -> list:
    """
    Génère des signaux d'achat/vente basés sur les indicateurs techniques.

    Returns:
        liste de dicts avec: type (achat/vente), signal, strength (1-5), details
    """
    freq = df.attrs.get("frequency", _detect_frequency(df)) if not df.empty else "daily"
    min_len = 8 if freq == "monthly" else 50
    if df.empty or len(df) < min_len:
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

    # --- Croisement court-terme MM20 x MM50 ---
    sma20_now = last.get("sma20")
    sma50_now = last.get("sma50")
    sma20_prev = prev.get("sma20")
    sma50_prev = prev.get("sma50")

    if all(v is not None and not pd.isna(v) for v in [sma20_now, sma50_now, sma20_prev, sma50_prev]):
        if sma20_prev <= sma50_prev and sma20_now > sma50_now:
            signals.append({
                "type": "achat",
                "signal": "Croisement haussier MM20/MM50",
                "strength": 4,
                "details": "La MM20 croise la MM50 par le haut (court terme)",
            })
        elif sma20_prev >= sma50_prev and sma20_now < sma50_now:
            signals.append({
                "type": "vente",
                "signal": "Croisement baissier MM20/MM50",
                "strength": 3,
                "details": "La MM20 croise la MM50 par le bas (court terme)",
            })

    # --- Golden Cross / Death Cross STANDARD (MM50 x MM200) ---
    # Requires enough history; silently skip if sma200 is not available for both points.
    sma200_now = last.get("sma200")
    sma200_prev = prev.get("sma200")

    if all(v is not None and not pd.isna(v) for v in [sma50_now, sma200_now, sma50_prev, sma200_prev]):
        if sma50_prev <= sma200_prev and sma50_now > sma200_now:
            signals.append({
                "type": "achat",
                "signal": "Golden Cross (MM50/MM200)",
                "strength": 5,
                "details": "La MM50 croise la MM200 par le haut — signal haussier long terme",
            })
        elif sma50_prev >= sma200_prev and sma50_now < sma200_now:
            signals.append({
                "type": "vente",
                "signal": "Death Cross (MM50/MM200)",
                "strength": 5,
                "details": "La MM50 croise la MM200 par le bas — signal baissier long terme",
            })
        else:
            # Proximity alert: within 2% spread — cross imminent
            if sma200_now and sma50_now:
                spread = (sma50_now - sma200_now) / sma200_now
                # MM50 below but approaching MM200 from below
                if -0.02 <= spread < 0 and sma50_now > sma50_prev:
                    signals.append({
                        "type": "achat",
                        "signal": "Golden Cross imminent",
                        "strength": 3,
                        "details": f"MM50 à {spread:+.1%} de la MM200 et en progression",
                    })
                # MM50 above but falling towards MM200
                elif 0 < spread <= 0.02 and sma50_now < sma50_prev:
                    signals.append({
                        "type": "vente",
                        "signal": "Death Cross imminent",
                        "strength": 3,
                        "details": f"MM50 à {spread:+.1%} de la MM200 et en baisse",
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
    freq = df.attrs.get("frequency", _detect_frequency(df)) if not df.empty else "daily"
    min_len = 8 if freq == "monthly" else 50
    if df.empty or len(df) < min_len:
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
    lookback = 3 if freq == "monthly" else 20
    if len(df) >= lookback:
        perf_20d = (df["close"].iloc[-1] / df["close"].iloc[-lookback] - 1) if df["close"].iloc[-lookback] != 0 else 0
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
