"""
Scoring hybride et génération de recommandations.
Combine analyse fondamentale (50 pts) + technique (50 pts) = score /100.
"""

from typing import Optional

import pandas as pd

from analysis.fundamental import compute_ratios
from analysis.technical import (
    compute_all_indicators,
    compute_technical_score,
    detect_support_resistance,
    detect_trend,
    generate_signals,
)


def compute_hybrid_score(fundamentals: dict, price_df: pd.DataFrame) -> dict:
    """
    Calcule le score hybride complet pour un titre.

    Args:
        fundamentals: dict des données fondamentales (depuis storage)
        price_df: DataFrame des prix historiques

    Returns:
        dict complet avec: fundamental_score, technical_score, hybrid_score,
        ratios, signals, trend, supports, resistances, recommendation
    """
    # Analyse fondamentale
    ratios = compute_ratios(fundamentals)
    fund_score = ratios["fundamental_score"]

    # Analyse technique
    if not price_df.empty and len(price_df) >= 20:
        price_df = compute_all_indicators(price_df)
        tech_score = compute_technical_score(price_df)
        tech_signals = generate_signals(price_df)
        trend = detect_trend(price_df)
        sr_levels = detect_support_resistance(price_df)
    else:
        tech_score = 25  # Neutre
        tech_signals = []
        trend = {"trend": "indetermine", "strength": "N/A", "details": "Pas de donnees prix"}
        sr_levels = {"supports": [], "resistances": []}

    hybrid_score = fund_score + tech_score

    # Recommandation
    recommendation = _generate_recommendation(hybrid_score, ratios, trend, tech_signals, sr_levels)

    return {
        "fundamental_score": fund_score,
        "technical_score": tech_score,
        "hybrid_score": hybrid_score,
        "ratios": ratios,
        "signals": tech_signals,
        "trend": trend,
        "supports": sr_levels["supports"],
        "resistances": sr_levels["resistances"],
        "recommendation": recommendation,
    }


def _generate_recommendation(
    score: float,
    ratios: dict,
    trend: dict,
    signals: list,
    sr_levels: dict,
) -> dict:
    """
    Génère une recommandation structurée basée sur le score hybride.
    """
    # Verdict global
    if score >= 75:
        verdict = "ACHAT FORT"
        verdict_color = "#28a745"
        stars = 5
    elif score >= 60:
        verdict = "ACHAT"
        verdict_color = "#28a745"
        stars = 4
    elif score >= 45:
        verdict = "CONSERVER"
        verdict_color = "#ffc107"
        stars = 3
    elif score >= 30:
        verdict = "PRUDENCE"
        verdict_color = "#fd7e14"
        stars = 2
    else:
        verdict = "EVITER"
        verdict_color = "#dc3545"
        stars = 1

    # Points forts
    strengths = []
    if ratios.get("roe") and ratios["roe"] >= 0.20:
        strengths.append(f"ROE excellent ({ratios['roe']:.1%})")
    if ratios.get("dividend_yield") and ratios["dividend_yield"] >= 0.06:
        strengths.append(f"Rendement attractif ({ratios['dividend_yield']:.1%})")
    if ratios.get("per") and 0 < ratios["per"] <= 12:
        strengths.append(f"Valorisation attractive (PER {ratios['per']:.1f})")
    if ratios.get("net_margin") and ratios["net_margin"] >= 0.15:
        strengths.append(f"Marge nette solide ({ratios['net_margin']:.1%})")
    if trend["trend"] == "haussiere":
        strengths.append(f"Tendance haussiere {trend['strength']}")

    # Points de vigilance
    warnings = []
    if ratios.get("per") and ratios["per"] > 15:
        warnings.append(f"PER eleve ({ratios['per']:.1f})")
    if ratios.get("payout_ratio") and ratios["payout_ratio"] > 0.80:
        warnings.append(f"Payout ratio eleve ({ratios['payout_ratio']:.1%})")
    if ratios.get("debt_equity") and ratios["debt_equity"] > 1.5:
        warnings.append(f"Endettement eleve ({ratios['debt_equity']:.2f}x)")
    if trend["trend"] == "baissiere":
        warnings.append(f"Tendance baissiere {trend['strength']}")

    # Zones d'entrée
    entry_zones = []
    supports = sr_levels.get("supports", [])
    if supports:
        entry_zones.append({
            "zone": f"{supports[0]:,.0f} FCFA",
            "label": "Support principal (zone 1)",
            "risk_reward": "Favorable",
        })
        if len(supports) > 1:
            entry_zones.append({
                "zone": f"{supports[1]:,.0f} FCFA",
                "label": "Support secondaire (zone 2)",
                "risk_reward": "Tres favorable",
            })

    return {
        "verdict": verdict,
        "verdict_color": verdict_color,
        "stars": stars,
        "score": score,
        "strengths": strengths,
        "warnings": warnings,
        "entry_zones": entry_zones,
    }


def rank_stocks(stocks_data: list) -> pd.DataFrame:
    """
    Classe une liste de titres par score hybride décroissant.

    Args:
        stocks_data: liste de dicts avec 'ticker', 'name', 'fundamentals', 'price_df'

    Returns:
        DataFrame classé avec scores et recommandations
    """
    results = []
    for stock in stocks_data:
        try:
            result = compute_hybrid_score(stock["fundamentals"], stock.get("price_df", pd.DataFrame()))
            results.append({
                "ticker": stock["ticker"],
                "name": stock.get("name", ""),
                "sector": stock.get("fundamentals", {}).get("sector", ""),
                "price": stock.get("fundamentals", {}).get("price", 0),
                "hybrid_score": result["hybrid_score"],
                "fundamental_score": result["fundamental_score"],
                "technical_score": result["technical_score"],
                "verdict": result["recommendation"]["verdict"],
                "stars": result["recommendation"]["stars"],
                "dividend_yield": result["ratios"].get("dividend_yield"),
                "per": result["ratios"].get("per"),
                "roe": result["ratios"].get("roe"),
                "trend": result["trend"]["trend"],
            })
        except Exception:
            continue

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("hybrid_score", ascending=False).reset_index(drop=True)
    return df


def recommend_for_profile(
    ranked_df: pd.DataFrame,
    profile: dict,
) -> list:
    """
    Filtre et recommande des titres selon le profil investisseur.

    Args:
        ranked_df: DataFrame classé (depuis rank_stocks)
        profile: dict du profil investisseur (risk_profile, horizon, budget,
                 preferred_sectors, objective, etc.)

    Returns:
        liste de dicts recommandés avec allocation suggérée
    """
    if ranked_df.empty:
        return []

    df = ranked_df.copy()

    # Filtrer par secteurs préférés
    preferred_sectors = profile.get("preferred_sectors", [])
    if preferred_sectors:
        df = df[df["sector"].isin(preferred_sectors)]

    # Filtrer par tickers exclus
    excluded = profile.get("excluded_tickers", [])
    if excluded:
        df = df[~df["ticker"].isin(excluded)]

    # Filtrer par tickers préférés (priorité)
    preferred_tickers = profile.get("preferred_tickers", [])

    # Ajuster le score min selon le profil de risque
    risk = profile.get("risk_profile", "equilibre")
    if risk == "prudent":
        min_score = 55
        # Favoriser les titres à dividende élevé et faible volatilité
        if "dividend_yield" in df.columns:
            df["adjusted_score"] = df["hybrid_score"] + df["dividend_yield"].fillna(0) * 100
        else:
            df["adjusted_score"] = df["hybrid_score"]
    elif risk == "dynamique":
        min_score = 35
        # Favoriser la croissance et le momentum
        df["adjusted_score"] = df["hybrid_score"]
    else:  # equilibre
        min_score = 45
        df["adjusted_score"] = df["hybrid_score"]

    # Filtrer par score minimum
    df = df[df["hybrid_score"] >= min_score]

    # Priorité aux tickers préférés
    if preferred_tickers:
        preferred_mask = df["ticker"].isin(preferred_tickers)
        df_preferred = df[preferred_mask]
        df_others = df[~preferred_mask]
        df = pd.concat([df_preferred, df_others])

    # Top 3
    top = df.head(3)
    if top.empty:
        return []

    # Allocation du budget
    budget = profile.get("budget", 0)
    objective = profile.get("objective", "mixte")

    recommendations = []
    total_score = top["adjusted_score"].sum()

    for _, row in top.iterrows():
        weight = row["adjusted_score"] / total_score if total_score > 0 else 1 / len(top)
        allocated = budget * weight
        price = row.get("price", 0) or 0
        nb_shares = int(allocated / price) if price > 0 else 0
        actual_amount = nb_shares * price

        recommendations.append({
            "ticker": row["ticker"],
            "name": row["name"],
            "sector": row["sector"],
            "hybrid_score": row["hybrid_score"],
            "verdict": row["verdict"],
            "stars": row["stars"],
            "dividend_yield": row.get("dividend_yield"),
            "per": row.get("per"),
            "roe": row.get("roe"),
            "weight": weight,
            "allocated_budget": allocated,
            "price": price,
            "nb_shares": nb_shares,
            "actual_amount": actual_amount,
        })

    return recommendations
