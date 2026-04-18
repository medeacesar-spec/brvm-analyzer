"""
Module de chat intelligent — moteur de réponse local.
Analyse les questions en langage naturel et génère des réponses
contextuelles à partir de toutes les données collectées (fondamentaux,
technique, actualités, profils, portefeuille).

Aucune API externe requise.
"""

import re
import pandas as pd
import streamlit as st

from config import load_tickers, CURRENCY, RATIO_THRESHOLDS, VALUE_CHECKLIST


# ── Ticker aliases (noms courants non présents dans brvm_tickers.json) ──

_TICKER_ALIASES = {
    "SGBC.ci": ["société générale", "societe generale", "sgci", "sg ci", "sgbci"],
    "SNTS.sn": ["sonatel", "orange sn"],
    "ORAC.ci": ["orange ci", "orange côte d'ivoire"],
    "ECOC.ci": ["ecobank ci", "ecobank côte d'ivoire"],
    "ETIT.tg": ["ecobank togo", "ecobank transnational"],
    "SIBC.ci": ["sib", "société ivoirienne de banque", "societe ivoirienne de banque"],
    "BOAB.bj": ["boa bénin", "boa benin", "bank of africa bénin"],
    "BOAC.ci": ["boa ci", "bank of africa ci"],
    "BOAN.ne": ["boa niger", "bank of africa niger"],
    "CBIBF.bf": ["coris bank", "coris"],
    "NSBC.ci": ["nsia banque", "nsia bank"],
    "CIEC.ci": ["cie ci", "cie"],
    "SDCC.ci": ["sodeci"],
    "TTLS.sn": ["total sénégal", "total senegal", "totalenergies sn"],
    "SHEC.ci": ["vivo energy", "vivo"],
    "SMBC.ci": ["smc", "smb ci"],
    "PALC.ci": ["palm ci"],
    "SPHC.ci": ["saph"],
    "FTSC.ci": ["filtisac", "fittisac"],
    "NEIC.ci": ["nei ceda", "nei-ceda"],
    "NTLC.ci": ["nestlé ci", "nestle ci", "nestle"],
    "UNLC.ci": ["unilever ci", "unilever"],
    "BNBC.ci": ["bernabé", "bernabe"],
    "ABJC.ci": ["abidjan catering", "servair"],
    "PRSC.ci": ["premia", "tractafric"],
    "TTRC.ci": ["total ci", "totalenergies ci"],
    "SDSC.ci": ["bolloré", "bollore", "africa global logistics"],
    "SOGC.ci": ["sogb", "sogb ci"],
    "SCRC.ci": ["sucrivoire"],
    "SICC.ci": ["sicor"],
    "CABC.ci": ["sicable"],
    "ORGT.tg": ["oragroup"],
    "ONTBF.bf": ["onatel", "onatel bf"],
    "BICC.ci": ["bici", "bnp ci"],
    "STBC.ci": ["sitab"],
    "SEMC.sn": ["seter", "setao"],
}


# ── Ticker detection ──

def _find_tickers_in_text(text: str) -> list:
    """Identifie les tickers mentionnés dans le texte (par code, nom ou alias)."""
    tickers_data = load_tickers()
    text_upper = text.upper()
    text_lower = text.lower()
    found = []

    for t in tickers_data:
        ticker = t["ticker"]
        name = t.get("name", "")

        # Match ticker code
        if ticker.upper() in text_upper:
            found.append(ticker)
            continue

        if not name:
            continue

        # Match full name
        if len(name) > 3 and name.lower() in text_lower:
            found.append(ticker)
            continue

        # Match consecutive words from name (e.g. "société générale" in "Société Générale CI")
        words = name.split()
        matched = False
        for n in range(len(words), 0, -1):
            for start in range(len(words) - n + 1):
                fragment = " ".join(words[start:start + n]).lower()
                if len(fragment) >= 4 and fragment in text_lower:
                    found.append(ticker)
                    matched = True
                    break
            if matched:
                break

        # Match aliases
        if not matched:
            aliases = _TICKER_ALIASES.get(ticker, [])
            for alias in aliases:
                if alias in text_lower:
                    found.append(ticker)
                    break

    return list(dict.fromkeys(found))  # Dedupe preserving order


# ── Metric detection ──

_METRIC_PATTERNS = {
    "dividend_yield": ["yield", "dividende", "rendement dividende", "dps", "distribution"],
    "per": ["per", "p/e", "price earning", "price/earning", "valorisation"],
    "roe": ["roe", "return on equity", "rentabilité", "rentabilite"],
    "net_margin": ["marge nette", "marge", "margin"],
    "debt_equity": ["dette", "d/e", "endettement", "leverage", "levier"],
    "payout_ratio": ["payout", "taux de distribution"],
    "price": ["prix", "cours", "cotation"],
    "market_cap": ["capitalisation", "cap", "market cap"],
    "net_income": ["résultat net", "resultat net", "bénéfice", "benefice", "profit", "perte"],
    "revenue": ["chiffre d'affaires", "ca", "revenue", "ventes"],
    "eps": ["eps", "bénéfice par action", "benefice par action"],
    "fcf": ["fcf", "free cash flow", "cash flow libre"],
    "fcf_margin": ["fcf margin", "marge fcf"],
    "interest_coverage": ["couverture", "couverture intérêts", "interest coverage"],
    "pb": ["p/b", "price to book"],
    "hybrid_score": ["score", "note", "notation", "rating"],
    "sector": ["secteur"],
    "trend": ["tendance", "trend", "technique"],
}

_INTENT_PATTERNS = {
    "risque": ["risque", "risqué", "dangereux", "perte", "perdu", "effondrement",
               "fragile", "faible", "mauvais", "négatif", "dette", "endett",
               "prudence", "éviter", "attention", "chute"],
    "opportunite": ["opportunité", "acheter", "achat", "meilleur", "top", "solide",
                    "recommand", "sous-évalué", "pas cher", "décote", "potentiel"],
    "dividende": ["dividende", "rendement", "yield", "distribution", "revenu", "coupon"],
    "croissance": ["croissance", "growth", "hausse", "haussier", "momentum", "monter"],
    "technique": ["technique", "rsi", "macd", "bollinger", "support", "résistance",
                  "golden cross", "death cross", "croisement", "tendance"],
    "comparer": ["comparer", "comparaison", "versus", "entre", "mieux", "pire", "lequel"],
    "secteur": ["secteur", "banque", "bancaire", "télécom", "industrie", "agriculture",
                "distribution", "transport", "assurance"],
    "diversifier": ["diversifier", "diversification", "nouveau", "élargir", "différent"],
    "securite": ["sûr", "sécurité", "stable", "défensif", "protéger", "capital", "prudent"],
    "renforcer": ["renforcer", "ajouter", "augmenter", "moyenner", "accumuler"],
}


def _detect_metrics(text: str) -> list:
    """Détecte les métriques demandées dans le texte."""
    text_lower = text.lower()
    found = []
    for metric_key, keywords in _METRIC_PATTERNS.items():
        for kw in keywords:
            if kw in text_lower:
                found.append(metric_key)
                break
    return found


def _detect_intents(text: str) -> list:
    """Détecte les intentions de l'utilisateur."""
    text_lower = text.lower()
    found = []
    for intent_id, keywords in _INTENT_PATTERNS.items():
        for kw in keywords:
            if kw in text_lower:
                found.append(intent_id)
                break
    return found


# ── Data retrieval ──

def _get_full_ticker_data(ticker: str) -> dict:
    """Récupère TOUTES les données disponibles pour un ticker."""
    from data.storage import (
        get_fundamentals, get_cached_prices, get_market_data,
        get_company_profile, get_company_news, get_qualitative_notes,
    )
    from analysis.scoring import compute_hybrid_score

    result = {"ticker": ticker}

    # Config info
    tickers_data = load_tickers()
    cfg = next((t for t in tickers_data if t["ticker"] == ticker), {})
    result["name"] = cfg.get("name", ticker)
    result["sector"] = cfg.get("sector", "")

    # Fundamentals
    fund = get_fundamentals(ticker)
    result["fundamentals"] = fund or {}

    # Scoring
    if fund:
        price_df = get_cached_prices(ticker)
        try:
            score_result = compute_hybrid_score(fund, price_df)
            result["scoring"] = {
                "fundamental_score": score_result["fundamental_score"],
                "technical_score": score_result["technical_score"],
                "hybrid_score": score_result["hybrid_score"],
                "verdict": score_result["recommendation"]["verdict"],
                "stars": score_result["recommendation"]["stars"],
                "strengths": score_result["recommendation"].get("strengths", []),
                "warnings": score_result["recommendation"].get("warnings", []),
                "trend": score_result["trend"],
                "signals": score_result.get("signals", []),
                "supports": score_result.get("supports", []),
                "resistances": score_result.get("resistances", []),
                "checklist": score_result["ratios"].get("checklist", []),
                "ratios": score_result["ratios"],  # All computed ratios
            }
            # Merge computed ratios into fundamentals for metric lookup
            for k, v in score_result["ratios"].items():
                if k not in ("checklist", "flags", "fundamental_score") and v is not None:
                    if not result["fundamentals"].get(k):
                        result["fundamentals"][k] = v
        except Exception:
            result["scoring"] = {}

    # Profile
    profile = get_company_profile(ticker)
    result["profile"] = profile or {}

    # News
    news = get_company_news(ticker, limit=5)
    result["news"] = [row.to_dict() for _, row in news.iterrows()] if not news.empty else []

    # Notes
    notes = get_qualitative_notes(ticker)
    result["notes"] = [row.get("note", "") for _, row in notes.iterrows()] if not notes.empty else []

    return result


def _get_all_stocks_ranked() -> pd.DataFrame:
    """Récupère et classe tous les titres analysables."""
    if "chat_ranked_cache" in st.session_state:
        return st.session_state.chat_ranked_cache

    from data.storage import get_all_stocks_for_analysis, get_cached_prices
    from analysis.scoring import compute_hybrid_score

    all_stocks = get_all_stocks_for_analysis()
    if all_stocks.empty:
        return pd.DataFrame()

    tickers_data = load_tickers()
    ticker_names = {t["ticker"]: t["name"] for t in tickers_data}
    ticker_sectors = {t["ticker"]: t.get("sector", "") for t in tickers_data}

    rows = []
    for _, row in all_stocks.iterrows():
        data = row.to_dict()
        for k, v in data.items():
            if isinstance(v, (float, int)) and pd.isna(v):
                data[k] = None
        ticker = data.get("ticker", "")
        name = data.get("company_name") or ticker_names.get(ticker, ticker)
        sector = ticker_sectors.get(ticker, "")

        try:
            price_df = get_cached_prices(ticker)
            result = compute_hybrid_score(data, price_df)
            # Prefer COMPUTED ratios (fresh, normalized) over raw data fields,
            # which may come from the market_data join under different aliases.
            computed = result.get("ratios", {})

            # Dividend yield fallback : compute from dps/price if ratio missing
            dy = computed.get("dividend_yield")
            if dy is None:
                price = data.get("price") or 0
                dps = data.get("dps") or 0
                if price > 0 and dps > 0:
                    dy = dps / price
                else:
                    dy = data.get("market_dividend_yield")

            rows.append({
                "ticker": ticker, "name": name, "sector": sector,
                "price": data.get("price") or 0,
                "hybrid_score": result["hybrid_score"],
                "fundamental_score": result["fundamental_score"],
                "technical_score": result["technical_score"],
                "verdict": result["recommendation"]["verdict"],
                "trend": result["trend"]["trend"],
                "dividend_yield": dy,
                "per": computed.get("per") or data.get("per"),
                "roe": computed.get("roe") or data.get("roe"),
                "net_margin": computed.get("net_margin") or data.get("net_margin"),
                "debt_equity": computed.get("debt_equity") or data.get("debt_equity"),
                "payout_ratio": computed.get("payout_ratio") or data.get("payout_ratio"),
                "dps": data.get("dps"),
                "net_income": data.get("net_income"),
                "revenue": data.get("revenue"),
                "market_cap": data.get("market_cap"),
                "eps": computed.get("eps") or data.get("eps"),
                "fcf": computed.get("fcf") or data.get("fcf"),
            })
        except Exception:
            pass

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("hybrid_score", ascending=False)
    st.session_state.chat_ranked_cache = df
    return df


# ── Response generators ──

def _format_metric(key: str, value, short=False) -> str:
    """Formate une métrique pour affichage."""
    if value is None:
        return "N/A"
    if key in ("roe", "net_margin", "dividend_yield", "payout_ratio", "fcf_margin"):
        return f"{value:.1%}"
    if key in ("per", "pb", "debt_equity", "interest_coverage", "dividend_cash_coverage"):
        return f"{value:.2f}"
    if key == "hybrid_score":
        return f"{value:.0f}/100"
    if key in ("price", "eps", "dps"):
        return f"{value:,.0f} {CURRENCY}" if not short else f"{value:,.0f}"
    if key in ("net_income", "revenue", "market_cap", "fcf", "equity", "total_debt", "ebit"):
        if abs(value) >= 1e9:
            return f"{value/1e9:.2f} Mds {CURRENCY}"
        elif abs(value) >= 1e6:
            return f"{value/1e6:.1f} M {CURRENCY}"
        return f"{value:,.0f} {CURRENCY}"
    if key == "trend":
        emojis = {"haussiere": "📈 Haussière", "baissiere": "📉 Baissière", "neutre": "➡️ Neutre"}
        return emojis.get(value, f"❓ {value}")
    return str(value)


_METRIC_LABELS = {
    "dividend_yield": "Dividend Yield", "per": "PER", "roe": "ROE",
    "net_margin": "Marge nette", "debt_equity": "Dette/Equity",
    "payout_ratio": "Payout ratio", "price": "Prix", "market_cap": "Capitalisation",
    "net_income": "Résultat net", "revenue": "Chiffre d'affaires",
    "eps": "EPS", "fcf": "Free Cash Flow", "fcf_margin": "FCF Margin",
    "interest_coverage": "Couverture intérêts", "pb": "P/B",
    "hybrid_score": "Score hybride", "sector": "Secteur", "trend": "Tendance",
    "fundamental_score": "Score fondamental", "technical_score": "Score technique",
}


def _build_ticker_response(ticker: str, metrics: list, intents: list) -> str:
    """Construit une réponse détaillée pour un ticker spécifique."""
    data = _get_full_ticker_data(ticker)
    fund = data["fundamentals"]
    scoring = data.get("scoring", {})
    profile = data.get("profile", {})
    lines = []

    name = data["name"]
    sector = data["sector"]
    lines.append(f"### {name} ({ticker})")

    if scoring:
        verdict = scoring.get("verdict", "N/A")
        score = scoring.get("hybrid_score", 0)
        trend_info = scoring.get("trend", {})
        trend = trend_info.get("trend", "") if isinstance(trend_info, dict) else ""
        trend_str = {"haussiere": "📈", "baissiere": "📉", "neutre": "➡️"}.get(trend, "❓")
        lines.append(f"**Score : {score:.0f}/100** · Verdict : **{verdict}** · {trend_str} · Secteur : {sector}")
    else:
        lines.append(f"Secteur : {sector}")

    # If specific metrics requested, show those
    if metrics:
        lines.append("")
        for m in metrics:
            val = fund.get(m) if fund else None
            if val is None and scoring:
                val = scoring.get(m)
            label = _METRIC_LABELS.get(m, m)
            formatted = _format_metric(m, val)
            if val is not None:
                # Add context/assessment
                assessment = _assess_metric(m, val)
                lines.append(f"- **{label}** : {formatted} {assessment}")
            else:
                lines.append(f"- **{label}** : Donnée non disponible")
    else:
        # Show full profile
        lines.append("")
        if fund:
            key_metrics = [
                "price", "market_cap", "revenue", "net_income", "roe", "net_margin",
                "dividend_yield", "per", "debt_equity", "payout_ratio", "eps", "fcf",
            ]
            lines.append("**Fondamentaux :**")
            for m in key_metrics:
                val = fund.get(m)
                if val is not None:
                    label = _METRIC_LABELS.get(m, m)
                    assessment = _assess_metric(m, val)
                    lines.append(f"- {label} : **{_format_metric(m, val)}** {assessment}")

    # Scoring details
    if scoring:
        checklist = scoring.get("checklist", [])
        if checklist:
            passed = sum(1 for c in checklist if c.get("passed"))
            lines.append(f"\n**Checklist Value & Dividendes** : {passed}/{len(checklist)}")
            for c in checklist:
                icon = "✅" if c.get("passed") else "❌"
                lines.append(f"  {icon} {c['label']}")

        # Signals
        signals = scoring.get("signals", [])
        buy_sigs = [s for s in signals if s["type"] == "achat"]
        sell_sigs = [s for s in signals if s["type"] == "vente"]
        if buy_sigs:
            lines.append(f"\n**{len(buy_sigs)} signal(aux) d'achat :**")
            for s in buy_sigs:
                lines.append(f"- {'⭐' * s['strength']} {s['signal']} — {s['details']}")
        if sell_sigs:
            lines.append(f"\n**{len(sell_sigs)} signal(aux) de vente :**")
            for s in sell_sigs:
                lines.append(f"- {'⚠️'} {s['signal']} — {s['details']}")

        # Strengths / warnings
        strengths = scoring.get("strengths", [])
        warnings = scoring.get("warnings", [])
        if strengths:
            lines.append("\n🟢 **Points forts :** " + " · ".join(strengths))
        if warnings:
            lines.append("🔴 **Points de vigilance :** " + " · ".join(warnings))

        # Supports / resistances
        supports = scoring.get("supports", [])
        resistances = scoring.get("resistances", [])
        if supports:
            lines.append(f"\n📊 Supports : {', '.join(f'{int(s):,}' for s in supports[:3])}")
        if resistances:
            lines.append(f"📊 Résistances : {', '.join(f'{int(r):,}' for r in resistances[:3])}")

    # Risk assessment if intent is "risque"
    if "risque" in intents and fund:
        lines.append("\n**⚠️ Évaluation du risque :**")
        risk_flags = []
        ni = fund.get("net_income")
        if ni is not None and ni < 0:
            risk_flags.append(f"🔴 Résultat net négatif ({_format_metric('net_income', ni)})")
        roe = fund.get("roe")
        if roe is not None and roe < 0:
            risk_flags.append(f"🔴 ROE négatif ({roe:.1%})")
        de = fund.get("debt_equity")
        if de is not None and de > 2:
            risk_flags.append(f"🔴 Endettement élevé (D/E : {de:.2f})")
        trend_info = scoring.get("trend", {})
        trend = trend_info.get("trend", "") if isinstance(trend_info, dict) else ""
        if trend == "baissiere":
            risk_flags.append("🔴 Tendance technique baissière")
        score = scoring.get("hybrid_score", 50)
        if score < 35:
            risk_flags.append(f"🔴 Score très faible ({score:.0f}/100)")

        if risk_flags:
            for rf in risk_flags:
                lines.append(f"  {rf}")
            lines.append(f"\n→ **{len(risk_flags)} facteur(s) de risque identifié(s).** Prudence recommandée.")
        else:
            lines.append("  🟢 Aucun facteur de risque majeur détecté sur les données disponibles.")

    # Profile info
    if profile.get("description") and not metrics:
        desc = profile["description"][:300]
        lines.append(f"\n📋 *{desc}{'...' if len(profile['description']) > 300 else ''}*")

    # News
    if data["news"]:
        lines.append("\n**Actualités récentes :**")
        for art in data["news"][:3]:
            date = art.get("article_date") or (art.get("created_at", "")[:10] if art.get("created_at") else "")
            title = art.get("title", "")
            lines.append(f"- [{date}] {title}" if date else f"- {title}")

    return "\n".join(lines)


def _assess_metric(key: str, value) -> str:
    """Donne une appréciation courte d'une métrique."""
    if value is None:
        return ""
    assessments = {
        "roe": lambda v: "🟢 excellent" if v >= 0.20 else "🟢 solide" if v >= 0.15 else "🟡 moyen" if v >= 0.10 else "🔴 faible" if v >= 0 else "🔴 négatif",
        "net_margin": lambda v: "🟢 très bon" if v >= 0.15 else "🟢 bon" if v >= 0.10 else "🟡 moyen" if v >= 0.05 else "🔴 faible",
        "dividend_yield": lambda v: "🟢 cible BRVM atteinte" if v >= 0.06 else "🟡 correct" if v >= 0.04 else "🔴 faible" if v > 0 else "",
        "per": lambda v: "🟢 attractif" if 0 < v <= 10 else "🟢 raisonnable" if v <= 15 else "🟡 au-dessus de la moyenne" if v <= 20 else "🔴 élevé" if v > 0 else "",
        "debt_equity": lambda v: "🟢 faible" if v <= 0.5 else "🟢 acceptable" if v <= 1.0 else "🟡 modéré" if v <= 1.5 else "🔴 élevé",
        "payout_ratio": lambda v: "🟢 soutenable" if v <= 0.70 else "🟡 élevé" if v <= 0.90 else "🔴 très élevé",
        "net_income": lambda v: "🔴 en perte" if v < 0 else "🟢 bénéficiaire",
        "hybrid_score": lambda v: "🟢 ACHAT FORT" if v >= 75 else "🟢 ACHAT" if v >= 60 else "🟡 CONSERVER" if v >= 45 else "🔴 PRUDENCE" if v >= 30 else "🔴 ÉVITER",
    }
    fn = assessments.get(key)
    return fn(value) if fn else ""


def _build_ranking_response(intents: list, signals_data: list = None, stock_summaries: list = None) -> str:
    """Construit une réponse basée sur un classement global."""
    ranked = _get_all_stocks_ranked()
    if ranked.empty:
        return "❌ Pas de données disponibles pour l'analyse."

    lines = []

    for intent in intents:
        if intent == "risque":
            lines.append("### ⚠️ Titres présentant le plus de risque")
            risky = ranked.nsmallest(5, "hybrid_score")
            for _, r in risky.iterrows():
                alerts = []
                ni = r.get("net_income")
                if ni is not None and ni < 0:
                    alerts.append("résultat net négatif")
                roe = r.get("roe")
                if roe is not None and roe < 0:
                    alerts.append("ROE négatif")
                de = r.get("debt_equity")
                if de is not None and de > 2:
                    alerts.append(f"D/E élevé ({de:.1f})")
                trend_e = {"haussiere": "📈", "baissiere": "📉", "neutre": "➡️"}.get(r.get("trend", ""), "❓")
                alert_str = f" — ⚡ {', '.join(alerts)}" if alerts else ""
                lines.append(
                    f"- 🔴 **{r['name']}** ({r['ticker']}) — Score {r['hybrid_score']:.0f}/100, "
                    f"{r['verdict']} {trend_e}{alert_str}"
                )
            lines.append("")

        elif intent == "opportunite" or intent == "securite":
            label = "🟢 Meilleures opportunités" if intent == "opportunite" else "🛡️ Titres les plus sûrs"
            lines.append(f"### {label}")
            if intent == "securite":
                safe = ranked[(ranked["hybrid_score"] >= 45) & (ranked["trend"] != "baissiere")]
                top = safe.nlargest(5, "fundamental_score") if not safe.empty else ranked.nlargest(5, "hybrid_score")
            else:
                top = ranked.nlargest(5, "hybrid_score")
            for _, r in top.iterrows():
                dy = r.get("dividend_yield")
                dy_str = f"Yield {dy:.1%}" if pd.notna(dy) and dy else ""
                per = r.get("per")
                per_str = f"PER {per:.1f}" if pd.notna(per) and per and per > 0 else ""
                extras = " · ".join(filter(None, [dy_str, per_str]))
                extras_str = f" ({extras})" if extras else ""
                trend_e = {"haussiere": "📈", "baissiere": "📉", "neutre": "➡️"}.get(r.get("trend", ""), "❓")
                lines.append(
                    f"- 🟢 **{r['name']}** ({r['ticker']}) — Score **{r['hybrid_score']:.0f}**/100, "
                    f"{r['verdict']} {trend_e}{extras_str}"
                )
            lines.append("")

        elif intent == "dividende":
            lines.append("### 💰 Meilleurs rendements dividende")
            has_dy = ranked[ranked["dividend_yield"].notna() & (ranked["dividend_yield"] > 0)]
            top_dy = has_dy.nlargest(5, "dividend_yield") if not has_dy.empty else pd.DataFrame()
            if not top_dy.empty:
                for _, r in top_dy.iterrows():
                    lines.append(
                        f"- 💰 **{r['name']}** ({r['ticker']}) — "
                        f"Yield **{r['dividend_yield']:.1%}**, Score {r['hybrid_score']:.0f}/100, {r['verdict']}"
                    )
            else:
                lines.append("- Aucune donnée de dividende disponible.")
            lines.append("")

        elif intent == "croissance":
            lines.append("### 📈 Titres en croissance / momentum haussier")
            bullish = ranked[ranked["trend"] == "haussiere"]
            top_bull = bullish.nlargest(5, "technical_score") if not bullish.empty else pd.DataFrame()
            if not top_bull.empty:
                for _, r in top_bull.iterrows():
                    lines.append(
                        f"- 📈 **{r['name']}** ({r['ticker']}) — "
                        f"Score tech {r['technical_score']:.0f}/50, Score global {r['hybrid_score']:.0f}/100"
                    )
            else:
                lines.append("- Aucun titre en tendance haussière actuellement.")
            lines.append("")

        elif intent == "technique":
            lines.append("### 📊 Synthèse technique")
            bullish = ranked[ranked["trend"] == "haussiere"]
            bearish = ranked[ranked["trend"] == "baissiere"]
            lines.append(f"- 📈 **{len(bullish)}** titre(s) en tendance haussière")
            lines.append(f"- 📉 **{len(bearish)}** titre(s) en tendance baissière")
            lines.append(f"- ➡️ **{len(ranked) - len(bullish) - len(bearish)}** neutre/indéterminé")

            if signals_data:
                buy_count = len([s for s in signals_data if s["type"] == "achat"])
                sell_count = len([s for s in signals_data if s["type"] == "vente"])
                lines.append(f"- Signaux : **{buy_count}** achat vs **{sell_count}** vente")

                # Contradictions
                tickers_buy = {s["ticker"] for s in signals_data if s["type"] == "achat"}
                tickers_sell = {s["ticker"] for s in signals_data if s["type"] == "vente"}
                contradictions = tickers_buy & tickers_sell
                if contradictions:
                    lines.append(f"\n⚡ **Signaux contradictoires** sur : {', '.join(contradictions)}")
            lines.append("")

        elif intent == "secteur":
            lines.append("### 🏢 Synthèse par secteur")
            for sector, group in ranked.groupby("sector"):
                if not sector:
                    continue
                avg_score = group["hybrid_score"].mean()
                emoji = "🟢" if avg_score >= 55 else "🟡" if avg_score >= 40 else "🔴"
                best = group.nlargest(1, "hybrid_score").iloc[0] if len(group) > 0 else None
                best_str = f" — Meilleur : {best['name']} ({best['hybrid_score']:.0f})" if best is not None else ""
                lines.append(
                    f"- {emoji} **{sector}** ({len(group)} titres) — "
                    f"Score moyen {avg_score:.0f}/100{best_str}"
                )
            lines.append("")

        elif intent == "comparer":
            lines.append("### 🔄 Comparaison rapide")
            top3 = ranked.nlargest(3, "hybrid_score")
            bottom3 = ranked.nsmallest(3, "hybrid_score")
            lines.append("**Top 3 :**")
            for _, r in top3.iterrows():
                lines.append(f"- 🏆 **{r['name']}** — Score {r['hybrid_score']:.0f}, {r['verdict']}")
            lines.append("\n**Bas du classement :**")
            for _, r in bottom3.iterrows():
                lines.append(f"- ⚠️ **{r['name']}** — Score {r['hybrid_score']:.0f}, {r['verdict']}")
            lines.append("")

        elif intent == "diversifier":
            lines.append("### 🌐 Opportunités de diversification")
            # Show best ticker per sector
            for sector, group in ranked.groupby("sector"):
                if not sector:
                    continue
                best = group.nlargest(1, "hybrid_score").iloc[0]
                if best["hybrid_score"] >= 45:
                    lines.append(
                        f"- **{sector}** : {best['name']} ({best['ticker']}) — "
                        f"Score {best['hybrid_score']:.0f}/100"
                    )
            lines.append("")

        elif intent == "renforcer":
            # Only makes sense with portfolio context
            portfolio = _get_portfolio_tickers()
            if portfolio:
                lines.append("### 📌 Positions à renforcer")
                in_pf = ranked[ranked["ticker"].isin(portfolio)]
                top_pf = in_pf.nlargest(5, "hybrid_score") if not in_pf.empty else pd.DataFrame()
                if not top_pf.empty:
                    for _, r in top_pf.iterrows():
                        lines.append(
                            f"- **{r['name']}** ({r['ticker']}) — Score {r['hybrid_score']:.0f}/100, {r['verdict']}"
                        )
                else:
                    lines.append("- Aucune donnée disponible pour vos positions.")
            lines.append("")

    return "\n".join(lines) if lines else ""


def _get_portfolio_tickers() -> set:
    """Récupère les tickers en portefeuille."""
    try:
        from data.storage import get_portfolio
        pf = get_portfolio()
        return set(pf["ticker"].unique()) if not pf.empty else set()
    except Exception:
        return set()


def _build_portfolio_response(query: str, intents: list, metrics: list) -> str:
    """Construit une réponse spécifique au portefeuille."""
    from data.storage import get_portfolio, get_fundamentals, get_portfolio_cash

    portfolio = get_portfolio()
    if portfolio.empty:
        return "Aucun portefeuille enregistré."

    # Prefer session state (just-edited value) else load from DB
    cash = st.session_state.get("portfolio_cash")
    if cash is None:
        cash = get_portfolio_cash()
    ranked = _get_all_stocks_ranked()

    lines = []

    # Select candidates based on intent (even when cash = 0, so we can still rank)
    top = pd.DataFrame()
    header = ""
    empty_msg = ""

    if "dividende" in intents:
        header = "Rendement dividende"
        # Relaxed filter : any stock with a positive yield; fallback = top 5 by yield
        has_dy = ranked[ranked["dividend_yield"].notna() & (ranked["dividend_yield"] > 0)]
        if has_dy.empty:
            empty_msg = "Aucun titre avec un yield calculable (DPS ou prix manquant)."
        else:
            top = has_dy.nlargest(5, "dividend_yield")
    elif "securite" in intents or "risque" in intents:
        header = "Sécurité"
        safe = ranked[(ranked["hybrid_score"] >= 50) & (ranked["trend"] != "baissiere")]
        top = safe.nlargest(5, "fundamental_score") if not safe.empty else ranked.nlargest(5, "hybrid_score")
    elif "croissance" in intents:
        header = "Croissance"
        bull = ranked[ranked["trend"] == "haussiere"]
        top = bull.nlargest(5, "technical_score") if not bull.empty else ranked.nlargest(5, "hybrid_score")
    elif "diversifier" in intents:
        header = "Diversification"
        tickers_data = load_tickers()
        sector_map = {t["ticker"]: t.get("sector", "") for t in tickers_data}
        pf_sectors = {sector_map.get(t, "") for t in portfolio["ticker"].unique()}
        outside = ranked[~ranked["sector"].isin(pf_sectors)]
        top = outside.nlargest(5, "hybrid_score") if not outside.empty else ranked.nlargest(5, "hybrid_score")
    elif "renforcer" in intents:
        header = "Renforcement des positions"
        pf_tickers = set(portfolio["ticker"].unique())
        in_pf = ranked[ranked["ticker"].isin(pf_tickers)]
        top = in_pf.nlargest(5, "hybrid_score") if not in_pf.empty else ranked.nlargest(5, "hybrid_score")
    else:
        header = "Meilleures opportunités"
        top = ranked.nlargest(5, "hybrid_score")

    # ── Branch 1 : cash > 0 → allocation concrète
    if cash > 0:
        emoji = {
            "Rendement dividende": "💰",
            "Sécurité": "🛡️",
            "Croissance": "📈",
            "Diversification": "🌐",
            "Renforcement des positions": "📌",
            "Meilleures opportunités": "💡",
        }.get(header, "💡")
        lines.append(f"### {emoji} Allocation cash ({cash:,.0f} {CURRENCY}) — {header}")

        if top.empty and empty_msg:
            lines.append(empty_msg)

        if not top.empty:
            nb_reco = min(3, len(top))
            top_n = top.head(nb_reco)
            total_score = top_n["hybrid_score"].sum()
            pf_tickers = set(portfolio["ticker"].unique())

            for i, (_, r) in enumerate(top_n.iterrows()):
                weight = r["hybrid_score"] / total_score if total_score > 0 else 1 / nb_reco
                allocated = cash * weight
                price = r.get("price") or 0
                nb_shares = int(allocated / price) if price > 0 else 0
                actual = nb_shares * price

                already = "📌 déjà en portefeuille" if r["ticker"] in pf_tickers else "🆕 nouveau"
                dy = r.get("dividend_yield")
                dy_str = f"Yield {dy:.1%}" if pd.notna(dy) and dy else ""
                per = r.get("per")
                per_str = f"PER {per:.1f}" if pd.notna(per) and per and per > 0 else ""
                extras = " · ".join(filter(None, [dy_str, per_str]))

                lines.append(
                    f"**{i+1}. {r['name']}** ({r['ticker']}) — "
                    f"Score **{r['hybrid_score']:.0f}**/100 · {r['verdict']} · {already}"
                )
                if extras:
                    lines.append(f"   {extras}")
                lines.append(
                    f"   → **{nb_shares} actions** à {price:,.0f} = **{actual:,.0f} {CURRENCY}** "
                    f"({weight*100:.0f}% du cash)"
                )
                lines.append("")

            remaining = cash - sum(
                int(cash * (r["hybrid_score"] / total_score) / max(r.get("price", 1) or 1, 1))
                * (r.get("price") or 0)
                for _, r in top_n.iterrows()
            )
            if remaining > 0:
                lines.append(f"💵 Reste non investi : **{remaining:,.0f} {CURRENCY}**")

    # ── Branch 2 : cash = 0 → classement + contexte portefeuille (pas d'allocation)
    else:
        emoji = {
            "Rendement dividende": "💰",
            "Sécurité": "🛡️",
            "Croissance": "📈",
            "Diversification": "🌐",
            "Renforcement des positions": "📌",
            "Meilleures opportunités": "💡",
        }.get(header, "💡")
        lines.append(f"### {emoji} Classement — {header}")
        lines.append("_💡 Pas de cash renseigné : pas d'allocation numérique. Renseignez le cash disponible dans le portefeuille pour une allocation détaillée._")
        lines.append("")

        if top.empty:
            if empty_msg:
                lines.append(empty_msg)
            else:
                lines.append("Aucun titre ne correspond.")
        else:
            pf_tickers = set(portfolio["ticker"].unique())
            nb_reco = min(5, len(top))
            for i, (_, r) in enumerate(top.head(nb_reco).iterrows()):
                already = "📌 déjà en portefeuille" if r["ticker"] in pf_tickers else "🆕 nouveau"
                dy = r.get("dividend_yield")
                dy_str = f"Yield **{dy:.1%}**" if pd.notna(dy) and dy else ""
                per = r.get("per")
                per_str = f"PER {per:.1f}" if pd.notna(per) and per and per > 0 else ""
                roe = r.get("roe")
                roe_str = f"ROE {roe:.1%}" if pd.notna(roe) and roe else ""
                extras = " · ".join(filter(None, [dy_str, per_str, roe_str]))

                lines.append(
                    f"**{i+1}. {r['name']}** ({r['ticker']}) — "
                    f"Score **{r['hybrid_score']:.0f}**/100 · {r['verdict']} · {already}"
                )
                if extras:
                    lines.append(f"   · {extras}")
                lines.append("")

    if not lines:
        lines.append("Aucune donnée disponible pour cette requête.")

    return "\n".join(lines)


# ── Main chat function ──

def chat(
    query: str,
    mode: str = "signals",
    chat_history: list = None,
    signals_data: list = None,
    stock_summaries: list = None,
) -> str:
    """
    Analyse la requête utilisateur et génère une réponse contextuelle.
    Fonctionne entièrement en local — aucune API externe nécessaire.
    """
    tickers = _find_tickers_in_text(query)
    metrics = _detect_metrics(query)
    intents = _detect_intents(query)

    # Also check chat history for ticker context
    if chat_history:
        for msg in chat_history[-4:]:
            tickers.extend(_find_tickers_in_text(msg.get("content", "")))
        tickers = list(dict.fromkeys(tickers))

    parts = []

    # 1. Ticker-specific responses
    if tickers:
        for ticker in tickers[:3]:  # Max 3 tickers per response
            parts.append(_build_ticker_response(ticker, metrics, intents))

    # 2. Ranking / intent-based responses (if no ticker or if generic intents)
    if not tickers and intents:
        if mode == "portfolio":
            pf_response = _build_portfolio_response(query, intents, metrics)
            if pf_response:
                parts.append(pf_response)
        ranking_response = _build_ranking_response(intents, signals_data, stock_summaries)
        if ranking_response:
            parts.append(ranking_response)

    # 3. Fallback
    if not parts:
        if metrics and not tickers:
            # User asked for a metric but no ticker — show ranking by that metric
            parts.append(_build_metric_ranking(metrics))
        else:
            parts.append(
                "🤔 Je n'ai pas trouvé de question précise. Vous pouvez :\n\n"
                "- **Mentionner un titre** : *« Quel est le yield de Sonatel ? »*\n"
                "- **Demander un classement** : *« Quels titres ont le meilleur dividende ? »*\n"
                "- **Évaluer un risque** : *« NEI CEDA est-il risqué ? »*\n"
                "- **Comparer** : *« Comparer le secteur bancaire »*\n"
                "- **Chercher des opportunités** : *« Titres sous-évalués avec bon rendement »*"
            )

    response = "\n\n".join(parts)
    response += "\n\n---\n*Posez une autre question ou mentionnez un titre pour approfondir.*"
    return response


def _build_metric_ranking(metrics: list) -> str:
    """Construit un classement par métrique spécifique."""
    ranked = _get_all_stocks_ranked()
    if ranked.empty:
        return "❌ Pas de données disponibles."

    lines = []
    for m in metrics[:2]:  # Max 2 metrics
        label = _METRIC_LABELS.get(m, m)
        col = m

        if col not in ranked.columns:
            continue

        has_data = ranked[ranked[col].notna()]
        if has_data.empty:
            lines.append(f"### {label} — Aucune donnée disponible")
            continue

        # Sort direction
        ascending = col in ("per", "debt_equity", "payout_ratio")
        if ascending:
            top = has_data.nsmallest(5, col)
            lines.append(f"### {label} — Top 5 (les plus bas)")
        else:
            top = has_data.nlargest(5, col)
            lines.append(f"### {label} — Top 5")

        for _, r in top.iterrows():
            val = r[col]
            formatted = _format_metric(m, val)
            lines.append(
                f"- **{r['name']}** ({r['ticker']}) — {label} : **{formatted}**, "
                f"Score {r['hybrid_score']:.0f}/100"
            )
        lines.append("")

    return "\n".join(lines)


def render_api_key_input():
    """No-op — plus besoin de clé API."""
    pass


def is_available() -> bool:
    """Toujours disponible — fonctionne en local."""
    return True
