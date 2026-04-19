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

    # Analyse technique (min 8 points pour mensuel, 20 pour journalier)
    if not price_df.empty and len(price_df) >= 8:
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
    elif ratios.get("roe") and ratios["roe"] >= 0.15:
        strengths.append(f"ROE solide ({ratios['roe']:.1%})")
    if ratios.get("dividend_yield") and ratios["dividend_yield"] >= 0.06:
        strengths.append(f"Rendement attractif ({ratios['dividend_yield']:.1%})")
    elif ratios.get("dividend_yield") and ratios["dividend_yield"] >= 0.04:
        strengths.append(f"Rendement correct ({ratios['dividend_yield']:.1%})")
    if ratios.get("per") and 0 < ratios["per"] <= 12:
        strengths.append(f"Valorisation attractive (PER {ratios['per']:.1f})")
    if ratios.get("net_margin") and ratios["net_margin"] >= 0.15:
        strengths.append(f"Marge nette solide ({ratios['net_margin']:.1%})")
    elif ratios.get("net_margin") and ratios["net_margin"] >= 0.10:
        strengths.append(f"Marge nette correcte ({ratios['net_margin']:.1%})")
    if ratios.get("interest_coverage") and ratios["interest_coverage"] >= 3:
        strengths.append(f"Bonne couverture des intérêts ({ratios['interest_coverage']:.1f}x)")
    if ratios.get("fcf_margin") and ratios["fcf_margin"] >= 0.10:
        strengths.append(f"FCF Margin solide ({ratios['fcf_margin']:.1%})")
    # Faible endettement : uniquement si on a une vraie valeur > 0 mais <= 0.5
    # (zéro pile = souvent donnée absente, évite l'interprétation abusive)
    de = ratios.get("debt_equity")
    if de is not None and 0 < de <= 0.5:
        strengths.append(f"Faible endettement ({de:.2f}x)")
    if trend["trend"] == "haussiere":
        strengths.append(f"Tendance haussière {trend['strength']}")
    # Achat signals
    buy_signals = [s for s in signals if s["type"] == "achat"]
    if buy_signals:
        strengths.append(f"{len(buy_signals)} signal(aux) d'achat technique(s)")

    # Points de vigilance
    warnings = []
    if ratios.get("per") and ratios["per"] > 20:
        warnings.append(f"PER élevé ({ratios['per']:.1f})")
    elif ratios.get("per") and ratios["per"] > 15:
        warnings.append(f"PER au-dessus de la moyenne ({ratios['per']:.1f})")
    if ratios.get("payout_ratio") and ratios["payout_ratio"] > 0.90:
        warnings.append(f"Payout ratio très élevé ({ratios['payout_ratio']:.1%})")
    elif ratios.get("payout_ratio") and ratios["payout_ratio"] > 0.70:
        warnings.append(f"Payout ratio élevé ({ratios['payout_ratio']:.1%})")
    if ratios.get("debt_equity") and ratios["debt_equity"] > 2.0:
        warnings.append(f"Endettement très élevé ({ratios['debt_equity']:.2f}x)")
    elif ratios.get("debt_equity") and ratios["debt_equity"] > 1.0:
        warnings.append(f"Endettement à surveiller ({ratios['debt_equity']:.2f}x)")
    if trend["trend"] == "baissiere":
        warnings.append(f"Tendance baissière {trend['strength']}")
    if ratios.get("roe") is not None and ratios["roe"] < 0.10:
        warnings.append(f"ROE faible ({ratios['roe']:.1%})")
    if ratios.get("net_margin") is not None and ratios["net_margin"] < 0.05:
        warnings.append(f"Marge nette faible ({ratios['net_margin']:.1%})")
    if ratios.get("dividend_yield") is not None and ratios["dividend_yield"] < 0.02:
        warnings.append("Rendement dividende très faible")
    # Sell signals
    sell_signals = [s for s in signals if s["type"] == "vente"]
    if sell_signals:
        warnings.append(f"{len(sell_signals)} signal(aux) de vente technique(s)")

    # Zones d'entrée
    entry_zones = []
    supports = sr_levels.get("supports", [])
    resistances = sr_levels.get("resistances", [])
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
                "risk_reward": "Très favorable",
            })
    # If no supports found, suggest zones based on current price
    if not supports:
        # Use percentage-based zones as fallback
        price = ratios.get("price") or 0
        if price > 0:
            entry_zones.append({
                "zone": f"{price * 0.95:,.0f} FCFA (-5%)",
                "label": "Zone d'achat estimée (repli -5%)",
                "risk_reward": "Favorable si correction",
            })
            entry_zones.append({
                "zone": f"{price * 0.90:,.0f} FCFA (-10%)",
                "label": "Zone d'achat forte (repli -10%)",
                "risk_reward": "Très favorable",
            })
        elif resistances:
            entry_zones.append({
                "zone": "Prix actuel (pas de support identifié en dessous)",
                "label": "Zone actuelle",
                "risk_reward": "Neutre — surveiller la résistance à {0:,.0f} FCFA".format(resistances[0]),
            })

    # Add resistance targets
    if resistances:
        entry_zones.append({
            "zone": f"{resistances[0]:,.0f} FCFA",
            "label": "Objectif (résistance)",
            "risk_reward": f"Potentiel +{((resistances[0] / supports[0]) - 1) * 100:.0f}% depuis support 1" if supports else "À surveiller",
        })
    elif not resistances and supports:
        # Price at all-time highs — no resistance above
        price = ratios.get("price") or 0
        if price > 0:
            entry_zones.append({
                "zone": f"Au-dessus de {price:,.0f} FCFA",
                "label": "Plus haut historique (pas de résistance)",
                "risk_reward": "Territoire inexploré — potentiel ouvert",
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


# ============================================================
# Consolidation des signaux par titre
# ============================================================

# Mapping signal name → famille, pour dédupliquer les signaux redondants.
SIGNAL_FAMILIES = {
    "Golden Cross (MM50/MM200)": "Moyennes Mobiles",
    "Death Cross (MM50/MM200)": "Moyennes Mobiles",
    "Golden Cross imminent": "Moyennes Mobiles",
    "Death Cross imminent": "Moyennes Mobiles",
    "Croisement haussier MM20/MM50": "Moyennes Mobiles",
    "Croisement baissier MM20/MM50": "Moyennes Mobiles",
    "RSI en survente": "RSI",
    "RSI en surachat": "RSI",
    "MACD croisement haussier": "MACD",
    "MACD croisement baissier": "MACD",
    "Prix sur bande Bollinger basse": "Bollinger",
    "Prix sur bande Bollinger haute": "Bollinger",
    "Volume anormalement eleve": "Volume",
    "Volume anormalement élevé": "Volume",
    "Checklist complete": "Checklist fondamentale",
    "Checklist quasi-complete": "Checklist fondamentale",
    "Proche support": "Supports/Résistances",
    "Proche resistance": "Supports/Résistances",
    "Proche résistance": "Supports/Résistances",
}


def _signal_family(signal_name: str) -> str:
    if not signal_name:
        return "Autre"
    # Exact match then partial
    if signal_name in SIGNAL_FAMILIES:
        return SIGNAL_FAMILIES[signal_name]
    low = signal_name.lower()
    if "cross" in low or "mm" in low:
        return "Moyennes Mobiles"
    if "rsi" in low:
        return "RSI"
    if "macd" in low:
        return "MACD"
    if "bollinger" in low:
        return "Bollinger"
    if "volume" in low:
        return "Volume"
    if "checklist" in low:
        return "Checklist fondamentale"
    if "support" in low or "résistance" in low or "resistance" in low:
        return "Supports/Résistances"
    return "Autre"


def consolidate_signals(signals: list) -> dict:
    """Regroupe les signaux par famille, élimine les doublons (redondances dans
    le même sens) et met en évidence les contradictions.
    Applique les POIDS CALIBRÉS sur la force des signaux si l'historique est
    suffisamment long (voir analysis.calibration).

    Args:
        signals: liste brute de signaux [{type, signal, strength, details}, ...]

    Returns:
        {
            "buy":   [signaux uniques par famille, sens achat],
            "sell":  [signaux uniques par famille, sens vente],
            "info":  [signaux de type info],
            "contradictions": [liste de familles avec signaux opposés],
            "buy_score":  somme des forces achat pondérées,
            "sell_score": somme des forces vente pondérées,
            "net_score":  buy_score - sell_score,
            "calibration_applied": bool,
        }
    """
    if not signals:
        return {
            "buy": [], "sell": [], "info": [],
            "contradictions": [],
            "buy_score": 0, "sell_score": 0, "net_score": 0,
            "calibration_applied": False,
        }

    # Load calibration lazily
    try:
        from analysis.calibration import get_calibration
        cal = get_calibration()
        calibration_applied = cal.get("enabled", False)
        sig_weights = cal.get("signals", {}) if calibration_applied else {}
    except Exception:
        calibration_applied = False
        sig_weights = {}

    # Group by (family, type) and keep strongest (raw strength before weighting)
    best_by_key = {}
    for sig in signals:
        stype = sig.get("type", "")
        family = _signal_family(sig.get("signal", ""))
        key = (family, stype)
        cur = best_by_key.get(key)
        if cur is None or (sig.get("strength") or 0) > (cur.get("strength") or 0):
            enriched = dict(sig)
            enriched["family"] = family
            # Apply calibration weight if available
            sig_name = sig.get("signal", "")
            cal_info = sig_weights.get(sig_name)
            if cal_info and cal_info.get("calibrated"):
                weight = cal_info["weight"]
                enriched["weight"] = weight
                enriched["effective_strength"] = (sig.get("strength") or 0) * weight
                enriched["calibrated"] = True
            else:
                enriched["weight"] = 1.0
                enriched["effective_strength"] = sig.get("strength") or 0
                enriched["calibrated"] = False
            best_by_key[key] = enriched

    # Detect contradictions (same family, both achat and vente)
    families_buy = {f for (f, t) in best_by_key if t == "achat"}
    families_sell = {f for (f, t) in best_by_key if t == "vente"}
    contradictions = sorted(families_buy & families_sell)

    buy = sorted(
        [s for s in best_by_key.values() if s.get("type") == "achat"],
        key=lambda s: -(s.get("effective_strength") or 0),
    )
    sell = sorted(
        [s for s in best_by_key.values() if s.get("type") == "vente"],
        key=lambda s: -(s.get("effective_strength") or 0),
    )
    info = [s for s in best_by_key.values() if s.get("type") == "info"]

    buy_score = sum(s.get("effective_strength") or 0 for s in buy)
    sell_score = sum(s.get("effective_strength") or 0 for s in sell)

    return {
        "buy": buy,
        "sell": sell,
        "info": info,
        "contradictions": contradictions,
        "buy_score": buy_score,
        "sell_score": sell_score,
        "net_score": buy_score - sell_score,
        "calibration_applied": calibration_applied,
    }


def compute_consolidated_verdict(hybrid_result: dict) -> dict:
    """Calcule un verdict consolidé par titre combinant:
    - la recommandation fondamentale hybride (verdict + score)
    - les signaux techniques dédupliqués
    - la tendance

    Retourne un dict avec verdict synthétique, icône, couleur, confiance
    et motif (pourquoi ce verdict)."""
    hybrid_reco = hybrid_result.get("recommendation", {})
    hybrid_verdict = (hybrid_reco.get("verdict") or "").upper()
    hybrid_score = hybrid_result.get("hybrid_score", 0)
    trend = hybrid_result.get("trend", {}).get("trend", "")

    cons = consolidate_signals(hybrid_result.get("signals", []))
    buy_score = cons["buy_score"]
    sell_score = cons["sell_score"]
    net = cons["net_score"]
    has_contradictions = bool(cons["contradictions"])

    # Classify hybrid stance
    if hybrid_verdict in ("ACHAT FORT",):
        hybrid_stance = 2
    elif hybrid_verdict in ("ACHAT",):
        hybrid_stance = 1
    elif hybrid_verdict in ("CONSERVER",):
        hybrid_stance = 0
    elif hybrid_verdict in ("PRUDENCE",):
        hybrid_stance = -1
    elif hybrid_verdict in ("EVITER", "ÉVITER"):
        hybrid_stance = -2
    else:
        hybrid_stance = 0

    # Classify technical stance
    if net >= 5:
        tech_stance = 2
    elif net >= 2:
        tech_stance = 1
    elif net > -2:
        tech_stance = 0
    elif net > -5:
        tech_stance = -1
    else:
        tech_stance = -2

    combined = hybrid_stance + tech_stance
    # Conflict: hybrid and tech disagree significantly
    conflict = (hybrid_stance >= 1 and tech_stance <= -1) or (hybrid_stance <= -1 and tech_stance >= 1)

    if combined >= 3:
        verdict = "ACHAT FORT CONFIRMÉ"
        icon = "🟢🟢"
        color = "#28a745"
    elif combined >= 1:
        verdict = "ACHAT"
        icon = "🟢"
        color = "#28a745"
    elif combined <= -3:
        verdict = "VENTE FORTE CONFIRMÉE"
        icon = "🔴🔴"
        color = "#dc3545"
    elif combined <= -1:
        verdict = "VENTE"
        icon = "🔴"
        color = "#dc3545"
    else:
        verdict = "NEUTRE"
        icon = "⚪"
        color = "#ffc107"

    if conflict:
        verdict = "⚠️ CONTRADICTION"
        icon = "⚠️"
        color = "#fd7e14"

    # Confidence score (0-100)
    if abs(combined) >= 3:
        confidence = 85
    elif abs(combined) == 2:
        confidence = 70
    elif abs(combined) == 1:
        confidence = 55
    else:
        confidence = 40
    if conflict:
        confidence = max(25, confidence - 30)
    if has_contradictions:
        confidence = max(20, confidence - 10)

    # Adjust confidence by calibrated verdict weight (historical success rate)
    try:
        from analysis.calibration import get_verdict_weight, get_calibration
        cal = get_calibration()
        if cal.get("enabled"):
            vw = get_verdict_weight(hybrid_verdict)
            # Bring confidence closer to / further from base by calibration weight
            # weight=1.0 → no change, weight>1 → boost, weight<1 → reduce
            confidence = int(max(10, min(95, confidence * vw)))
    except Exception:
        pass

    # Reason
    reason_parts = []
    if hybrid_verdict:
        reason_parts.append(f"Fondamental : {hybrid_verdict} ({hybrid_score:.0f}/100)")
    if buy_score > 0:
        reason_parts.append(f"Achat technique : {buy_score} pts ({len(cons['buy'])} signal(aux))")
    if sell_score > 0:
        reason_parts.append(f"Vente technique : {sell_score} pts ({len(cons['sell'])} signal(aux))")
    if trend:
        reason_parts.append(f"Tendance {trend}")
    if has_contradictions:
        reason_parts.append(
            f"Contradictions familles : {', '.join(cons['contradictions'])}"
        )

    return {
        "verdict": verdict,
        "icon": icon,
        "color": color,
        "confidence": confidence,
        "reason": " · ".join(reason_parts),
        "consolidated_signals": cons,
        "hybrid_verdict": hybrid_verdict,
        "hybrid_score": hybrid_score,
        "trend": trend,
        "conflict": conflict,
    }
