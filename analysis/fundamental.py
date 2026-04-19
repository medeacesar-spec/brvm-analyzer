"""
Moteur d'analyse fondamentale - Calcul des ratios selon le modèle BRVM Value & Dividendes.
Reproduit fidèlement la feuille 'Ratios' des fichiers Excel existants.
"""

from typing import Optional

from config import RATIO_THRESHOLDS, VALUE_CHECKLIST


def get_sector_benchmarks(sector: str = None) -> dict:
    """Retourne les médianes/min/max par secteur pour PER, P/B, ROE, Yield, Marge nette.
    Si sector=None, retourne aussi la médiane globale BRVM.
    Cache léger via fonctools non nécessaire ici, appelé 1 fois par page.
    """
    import pandas as pd
    from data.storage import get_all_stocks_for_analysis

    try:
        df = get_all_stocks_for_analysis()
    except Exception:
        return {}

    if df.empty:
        return {}

    # Compute ratios for each row
    records = []
    for _, row in df.iterrows():
        d = row.to_dict()
        price = d.get("price") or 0
        shares = d.get("shares") or 0
        revenue = d.get("revenue") or 0
        ni = d.get("net_income") or 0
        equity = d.get("equity") or 0
        dps = d.get("dps") or 0

        eps = (ni / shares) if shares and shares > 0 and ni else None
        per = (price / eps) if eps and eps > 0 else None
        bvps = (equity / shares) if shares and shares > 0 and equity else None
        pb = (price / bvps) if bvps and bvps > 0 else None
        roe = (ni / equity) if equity and equity > 0 and ni else None
        margin = (ni / revenue) if revenue and revenue > 0 and ni else None
        yield_ = (dps / price) if price and price > 0 and dps else None

        records.append({
            "ticker": d.get("ticker"),
            "sector": d.get("sector") or "",
            "per": per if per and 0 < per < 100 else None,  # exclude extremes
            "pb": pb if pb and 0 < pb < 20 else None,
            "roe": roe if roe and -1 < roe < 2 else None,
            "net_margin": margin if margin and -1 < margin < 1 else None,
            "dividend_yield": yield_ if yield_ and 0 < yield_ < 0.5 else None,
        })

    df_r = pd.DataFrame(records)

    def _stats(subdf):
        out = {}
        # Élargi : tous les ratios comparables (PER/PB, ROE, Marge, Yield,
        # Payout, FCF Margin, D/E, Couverture int., Couv. div cash)
        for col in [
            "per", "pb", "roe", "net_margin", "dividend_yield",
            "payout_ratio", "fcf_margin", "debt_equity",
            "interest_coverage", "dividend_cash_coverage",
        ]:
            if col not in subdf.columns:
                continue
            vals = subdf[col].dropna()
            if len(vals) >= 2:
                out[col] = {
                    "median": float(vals.median()),
                    "min": float(vals.min()),
                    "max": float(vals.max()),
                    "count": int(len(vals)),
                }
        return out

    result = {"global": _stats(df_r)}
    if sector:
        sub = df_r[df_r["sector"] == sector]
        if len(sub) >= 2:
            result["sector"] = _stats(sub)
            result["sector_name"] = sector
            result["sector_peers"] = sub["ticker"].tolist()
    return result


def compare_to_sector(ratio_name: str, value: float, benchmarks: dict,
                       prefer_low: bool = False) -> dict:
    """Compare une valeur à la médiane sectorielle. Retourne un dict avec
    badge, couleur, écart %.
    - prefer_low=True pour PER, P/B (plus bas = mieux)
    - prefer_low=False pour ROE, Yield, Marge (plus haut = mieux)
    """
    if value is None or not benchmarks:
        return None
    bench = benchmarks.get("sector", {}).get(ratio_name)
    scope = "secteur"
    if not bench:
        bench = benchmarks.get("global", {}).get(ratio_name)
        scope = "marché"
    if not bench:
        return None
    median = bench["median"]
    if median == 0:
        return None
    diff = (value - median) / abs(median)

    # Couleurs : design tokens v2 (terracotta/ocre/deep-green)
    # - var(--up) = vert profond (meilleur que médiane)
    # - var(--ocre) = ocre (proche médiane / attention)
    # - var(--down) = rouge terre (moins bon que médiane)
    if prefer_low:
        if diff <= -0.20:
            badge, color = "⬇️ Bien sous médiane", "var(--up)"
        elif diff <= -0.05:
            badge, color = "⬇️ Sous médiane", "var(--up)"
        elif diff <= 0.05:
            badge, color = "= Médiane", "var(--ocre)"
        elif diff <= 0.20:
            badge, color = "⬆️ Au-dessus médiane", "var(--down)"
        else:
            badge, color = "⬆️ Bien au-dessus", "var(--down)"
    else:
        if diff >= 0.20:
            badge, color = "⬆️ Bien au-dessus", "var(--up)"
        elif diff >= 0.05:
            badge, color = "⬆️ Au-dessus médiane", "var(--up)"
        elif diff >= -0.05:
            badge, color = "= Médiane", "var(--ocre)"
        elif diff >= -0.20:
            badge, color = "⬇️ Sous médiane", "var(--down)"
        else:
            badge, color = "⬇️ Bien sous médiane", "var(--down)"

    return {
        "badge": badge,
        "color": color,
        "diff": diff,
        "median": median,
        "min": bench["min"],
        "max": bench["max"],
        "count": bench["count"],
        "scope": scope,
    }


def compute_ratios(data: dict) -> dict:
    """
    Calcule tous les ratios fondamentaux à partir des données financières.

    Args:
        data: dict contenant les champs de la table fundamentals
              (revenue, net_income, equity, total_debt, ebit, interest_expense,
               cfo, capex, dividends_total, dps, price, shares, sector, etc.)

    Returns:
        dict avec tous les ratios calculés + drapeaux + checklist
    """
    import math

    def _safe(val, default=0):
        """Convert NaN/None to default."""
        if val is None:
            return default
        try:
            if math.isnan(val):
                return default
        except (TypeError, ValueError):
            pass
        return val

    price = _safe(data.get("price"), 0)
    shares = _safe(data.get("shares"), 0)
    revenue = _safe(data.get("revenue"), 0)
    net_income = _safe(data.get("net_income"), 0)
    equity = _safe(data.get("equity"), 0)
    # total_debt : on garde la trace de "donnée manquante" (None en DB) vs
    # "vraiment 0" (entreprise sans dette financière). Sans cette distinction,
    # un ticker sans donnée affichait à tort "Faible endettement 0.00×".
    total_debt_raw = data.get("total_debt")
    total_debt_missing = total_debt_raw is None
    total_debt = _safe(total_debt_raw, 0)
    ebit = _safe(data.get("ebit"), 0)
    interest_expense = _safe(data.get("interest_expense"), 0)
    cfo = data.get("cfo")
    if cfo is not None:
        cfo = _safe(cfo, None)
    capex = _safe(data.get("capex"), 0)
    dividends_total = _safe(data.get("dividends_total"), 0)
    dps = _safe(data.get("dps"), 0)
    sector = (data.get("sector") or "").lower() if isinstance(data.get("sector"), str) else ""
    is_bank = "banque" in sector or "bank" in sector

    ratios = {}

    # --- Croissance CA ---
    rev_n1 = data.get("revenue_n1")
    rev_n0 = data.get("revenue_n0")
    if rev_n1 and rev_n0 and rev_n1 != 0:
        ratios["revenue_growth"] = (rev_n0 - rev_n1) / rev_n1
    else:
        ratios["revenue_growth"] = None

    # --- ROE ---
    ratios["roe"] = net_income / equity if equity != 0 else None

    # --- Marge nette ---
    ratios["net_margin"] = net_income / revenue if revenue != 0 else None

    # --- Dette / Capitaux propres ---
    # Si la donnée dette est absente (NULL en DB), on renvoie None plutôt que
    # 0 pour éviter l'interprétation "Faible endettement" abusive.
    if total_debt_missing or equity == 0:
        ratios["debt_equity"] = None
    else:
        ratios["debt_equity"] = total_debt / equity

    # --- Couverture des intérêts ---
    ratios["interest_coverage"] = ebit / interest_expense if interest_expense != 0 else None

    # --- FCF ---
    if cfo is not None:
        ratios["fcf"] = cfo - capex
    else:
        ratios["fcf"] = -capex if capex else None
        ratios["fcf_note"] = "CFO non disponible"

    # --- FCF Margin ---
    if ratios["fcf"] is not None and revenue != 0:
        ratios["fcf_margin"] = ratios["fcf"] / revenue
    else:
        ratios["fcf_margin"] = None

    # --- EPS ---
    ratios["eps"] = net_income / shares if shares != 0 else None

    # --- DPS (utilisé) ---
    if dps:
        ratios["dps"] = dps
    elif dividends_total and shares:
        ratios["dps"] = dividends_total / shares
    else:
        ratios["dps"] = 0

    # --- Dividend Yield ---
    ratios["dividend_yield"] = ratios["dps"] / price if price != 0 else None

    # --- Payout Ratio ---
    if ratios.get("eps") and ratios["eps"] != 0:
        ratios["payout_ratio"] = ratios["dps"] / ratios["eps"]
    else:
        ratios["payout_ratio"] = None

    # --- PER ---
    ratios["per"] = price / ratios["eps"] if ratios.get("eps") and ratios["eps"] != 0 else None

    # --- P/B (Price to Book) ---
    book_value_per_share = equity / shares if shares != 0 else 0
    ratios["pb"] = price / book_value_per_share if book_value_per_share != 0 else None

    # --- Couverture du dividende (cash) ---
    if ratios.get("fcf") is not None and dividends_total and dividends_total != 0:
        ratios["dividend_cash_coverage"] = ratios["fcf"] / dividends_total
    else:
        ratios["dividend_cash_coverage"] = None

    # --- Capitalisation boursière ---
    ratios["market_cap"] = price * shares if price and shares else None

    # --- Levier bancaire (banques uniquement) ---
    if is_bank:
        # Approximation : total_debt peut servir de proxy pour total actif
        # Mais idéalement on utiliserait le total actif
        ratios["bank_leverage"] = None  # Nécessite total actif

    # --- Drapeaux ---
    ratios["flags"] = _compute_flags(ratios, is_bank)

    # --- Checklist Value & Dividendes ---
    ratios["checklist"] = _compute_checklist(ratios, is_bank)

    # --- Score fondamental ---
    ratios["fundamental_score"] = _compute_fundamental_score(ratios, is_bank)

    return ratios


def _compute_flags(ratios: dict, is_bank: bool) -> dict:
    """Calcule les drapeaux (OK/Vigilance/Risque) pour chaque ratio."""
    flags = {}

    # ROE
    roe = ratios.get("roe")
    if roe is not None:
        if roe >= 0.20:
            flags["roe"] = ("OK", "Excellent")
        elif roe >= 0.15:
            flags["roe"] = ("OK", "Solide")
        elif roe >= 0.10:
            flags["Vigilance"] = ("Vigilance", "Moyen")
            flags["roe"] = ("Vigilance", "Moyen")
        else:
            flags["roe"] = ("Risque", "Faible")
    else:
        flags["roe"] = ("Risque", "N/A")

    # Marge nette
    nm = ratios.get("net_margin")
    if nm is not None:
        if nm >= 0.15:
            flags["net_margin"] = ("OK", "Tres bon")
        elif nm >= 0.10:
            flags["net_margin"] = ("OK", "Bon")
        elif nm >= 0.05:
            flags["net_margin"] = ("Vigilance", "Moyen")
        else:
            flags["net_margin"] = ("Risque", "Faible")
    else:
        flags["net_margin"] = ("Risque", "N/A")

    # Dette / Equity — None si donnée absente, ne pas interpréter comme "très faible"
    de = ratios.get("debt_equity")
    if is_bank:
        flags["debt_equity"] = ("OK", "Banque - non applicable")
    elif de is None:
        flags["debt_equity"] = ("—", "Donnée absente")
    elif de <= 0.5:
        flags["debt_equity"] = ("OK", "Tres faible")
    elif de <= 1.0:
        flags["debt_equity"] = ("OK", "Acceptable")
    elif de <= 1.5:
        flags["debt_equity"] = ("Vigilance", "Eleve")
    else:
        flags["debt_equity"] = ("Risque", "Excessif")

    # Couverture intérêts
    ic = ratios.get("interest_coverage")
    if ic is not None:
        if ic >= 3.0:
            flags["interest_coverage"] = ("OK", "Confortable")
        elif ic >= 2.0:
            flags["interest_coverage"] = ("Vigilance", "Tendu")
        else:
            flags["interest_coverage"] = ("Risque", "Critique")
    else:
        flags["interest_coverage"] = ("OK", "Pas de dette")

    # FCF
    fcf = ratios.get("fcf")
    if fcf is not None:
        if fcf > 0:
            flags["fcf"] = ("OK", "Positif")
        else:
            flags["fcf"] = ("Risque", "Negatif")
    else:
        flags["fcf"] = ("Vigilance", "Non disponible")

    # FCF Margin
    fm = ratios.get("fcf_margin")
    if fm is not None:
        if fm >= 0.10:
            flags["fcf_margin"] = ("OK", "Tres bon")
        elif fm >= 0.05:
            flags["fcf_margin"] = ("OK", "Bon")
        elif fm >= 0:
            flags["fcf_margin"] = ("Vigilance", "Faible")
        else:
            flags["fcf_margin"] = ("Risque", "Negatif")
    else:
        flags["fcf_margin"] = ("Vigilance", "Non disponible")

    # Dividend Yield
    dy = ratios.get("dividend_yield")
    if dy is not None:
        if dy >= 0.06:
            flags["dividend_yield"] = ("OK", "Cible atteinte")
        elif dy >= 0.04:
            flags["dividend_yield"] = ("Vigilance", "Sous la cible")
        else:
            flags["dividend_yield"] = ("Risque", "Faible")
    else:
        flags["dividend_yield"] = ("Risque", "N/A")

    # Payout ratio
    pr = ratios.get("payout_ratio")
    if pr is not None:
        if 0.40 <= pr <= 0.70:
            flags["payout_ratio"] = ("OK", "Sain")
        elif pr < 0.40:
            flags["payout_ratio"] = ("OK", "Conservateur")
        elif pr <= 1.0:
            flags["payout_ratio"] = ("Vigilance", "Eleve")
        else:
            flags["payout_ratio"] = ("Risque", "Non soutenable")
    else:
        flags["payout_ratio"] = ("Risque", "N/A")

    # PER
    per = ratios.get("per")
    if per is not None:
        if per < 0:
            flags["per"] = ("Risque", "Negatif (perte)")
        elif per < 10:
            flags["per"] = ("OK", "Attractif (absolu)")
        elif per <= 15:
            flags["per"] = ("OK", "Value (absolu)")
        elif per <= 20:
            flags["per"] = ("Vigilance", "Elevé - vérifier secteur")
        else:
            flags["per"] = ("Risque", "Cher - vérifier secteur")
    else:
        flags["per"] = ("Risque", "N/A")

    # P/B
    pb = ratios.get("pb")
    if pb is not None:
        if is_bank:
            flags["pb"] = ("OK", "Banque - comparer ROE")
        elif pb < 1.0:
            flags["pb"] = ("OK", "Sous la valeur comptable")
        elif pb < 2.0:
            flags["pb"] = ("OK", "Raisonnable")
        else:
            flags["pb"] = ("Vigilance", "Eleve")
    else:
        flags["pb"] = ("Risque", "N/A")

    # Couverture dividende cash
    dcc = ratios.get("dividend_cash_coverage")
    if dcc is not None:
        if dcc >= 1.2:
            flags["dividend_cash_coverage"] = ("OK", "Confort")
        elif dcc >= 1.0:
            flags["dividend_cash_coverage"] = ("Vigilance", "Juste")
        else:
            flags["dividend_cash_coverage"] = ("Risque", "Non couvert")
    else:
        flags["dividend_cash_coverage"] = ("Vigilance", "Non disponible")

    return flags


def _compute_checklist(ratios: dict, is_bank: bool) -> list:
    """Évalue la checklist Value & Dividendes."""
    results = []

    checks = [
        ("Dividend Yield >= 6%", ratios.get("dividend_yield"), 0.06, ">="),
        ("Payout ratio <= 70%", ratios.get("payout_ratio"), 0.70, "<="),
        ("ROE >= 15%", ratios.get("roe"), 0.15, ">="),
        ("PER <= 15", ratios.get("per"), 15, "<="),
        ("Couverture dividende >= 1.2x", ratios.get("dividend_cash_coverage"), 1.2, ">="),
    ]
    if not is_bank:
        checks.append(("Dette/Equity <= 1.5", ratios.get("debt_equity"), 1.5, "<="))

    for label, value, target, direction in checks:
        if value is None:
            results.append({"label": label, "value": value, "target": target, "passed": None})
        elif direction == ">=" and value >= target:
            results.append({"label": label, "value": value, "target": target, "passed": True})
        elif direction == "<=" and value <= target:
            results.append({"label": label, "value": value, "target": target, "passed": True})
        else:
            results.append({"label": label, "value": value, "target": target, "passed": False})

    return results


def _compute_fundamental_score(ratios: dict, is_bank: bool) -> float:
    """
    Calcule un score fondamental sur 50 points (la moitié du score hybride).
    Pondération :
    - ROE: 10 pts
    - Dividend Yield: 10 pts
    - PER: 8 pts
    - Payout ratio: 7 pts
    - Marge nette: 5 pts
    - Dette/Equity: 5 pts (hors banques)
    - Couverture dividende: 5 pts
    """
    score = 0

    # ROE (10 pts)
    roe = ratios.get("roe")
    if roe is not None:
        if roe >= 0.25:
            score += 10
        elif roe >= 0.20:
            score += 8
        elif roe >= 0.15:
            score += 6
        elif roe >= 0.10:
            score += 3
        else:
            score += 1

    # Dividend Yield (10 pts)
    dy = ratios.get("dividend_yield")
    if dy is not None:
        if dy >= 0.08:
            score += 10
        elif dy >= 0.06:
            score += 8
        elif dy >= 0.04:
            score += 5
        elif dy >= 0.02:
            score += 2

    # PER (8 pts)
    per = ratios.get("per")
    if per is not None and per > 0:
        if per < 8:
            score += 8
        elif per < 10:
            score += 7
        elif per <= 12:
            score += 5
        elif per <= 15:
            score += 3
        else:
            score += 1

    # Payout ratio (7 pts)
    pr = ratios.get("payout_ratio")
    if pr is not None:
        if 0.40 <= pr <= 0.60:
            score += 7
        elif 0.30 <= pr <= 0.70:
            score += 5
        elif pr <= 1.0:
            score += 2

    # Marge nette (5 pts)
    nm = ratios.get("net_margin")
    if nm is not None:
        if nm >= 0.20:
            score += 5
        elif nm >= 0.15:
            score += 4
        elif nm >= 0.10:
            score += 3
        elif nm >= 0.05:
            score += 1

    # Dette/Equity (5 pts) - hors banques. None = donnée absente → score neutre
    if not is_bank:
        de = ratios.get("debt_equity")
        if de is None:
            score += 2  # neutre si donnée manquante
        elif de <= 0.3:
            score += 5
        elif de <= 0.5:
            score += 4
        elif de <= 1.0:
            score += 3
        elif de <= 1.5:
            score += 1
    else:
        score += 3  # Score neutre pour les banques

    # Couverture dividende (5 pts)
    dcc = ratios.get("dividend_cash_coverage")
    if dcc is not None:
        if dcc >= 2.0:
            score += 5
        elif dcc >= 1.5:
            score += 4
        elif dcc >= 1.2:
            score += 3
        elif dcc >= 1.0:
            score += 1
    else:
        score += 2  # Score neutre si pas de données

    return score


def format_ratio(value, fmt: str = "pct") -> str:
    """Formate un ratio pour l'affichage."""
    import math
    if value is None:
        return "N/A"
    try:
        if math.isnan(value):
            return "N/A"
    except (TypeError, ValueError):
        return "N/A"
    if fmt == "pct":
        return f"{value:.2%}"
    elif fmt == "x":
        return f"{value:.2f}x"
    elif fmt == "number":
        return f"{value:,.0f}"
    elif fmt == "decimal":
        return f"{value:.2f}"
    return str(value)
