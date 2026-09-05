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


# Cout des capitaux propres retenu pour la BRVM : taux sans risque regional
# (emprunts d'Etat UEMOA ~6 %) augmente d'une prime de risque actions de ~7 %.
COUT_CAPITAUX_PROPRES = 0.13


def _charges_credibles(charges, produit) -> bool:
    """Dit si des frais generaux peuvent servir a un coefficient d'exploitation.

    Aucune banque ne fonctionne avec des frais generaux representant 0,1 % de
    son produit net bancaire : une telle valeur est un fragment de ligne pris
    pour un total. Oragroup portait 0,2 Md de charges face a 186,6 Mds de PNB,
    d'ou un coefficient d'exploitation de 0,1 % la ou le secteur se situe entre
    35 et 70 %. A l'inverse, des charges superieures a 120 % du produit ne
    decrivent plus une exploitation mais une erreur de colonne.

    Le test ne juge pas la performance : il ecarte ce qui ne peut pas etre un
    total de charges.
    """
    if not charges or not produit:
        return False
    part = abs(charges) / abs(produit)
    return 0.10 <= part <= 1.20


def _actif_fiable(total_actif, fonds_propres, chiffre_affaires, is_bank):
    """Dit si le total de bilan extrait est exploitable.

    Un total de bilan faux empoisonne silencieusement le rendement des actifs
    et la rotation de l'actif — deux indicateurs mis en avant dans les grilles
    sectorielles. Trois recoupements suffisent a ecarter les valeurs qui ne
    peuvent pas etre un total :

      - l'actif ne peut pas etre inferieur aux fonds propres (actif = fonds
        propres + dettes, et les dettes ne sont pas negatives) ;
      - une banque porte un bilan de plusieurs fois son produit net bancaire,
        jamais l'inverse ;
      - hors banque, une rotation de l'actif superieure a 5 n'existe pas sur
        la cote : c'est le signe qu'une ligne partielle a ete prise pour le
        total.

    Constate sur les donnees : CFAO CI portait un actif inferieur a ses fonds
    propres, et trois filiales BOA un actif des centaines de fois plus petit
    que leur PNB.
    """
    if not total_actif or total_actif <= 0:
        return False
    if fonds_propres and fonds_propres > 0 and total_actif < fonds_propres:
        return False
    if chiffre_affaires and chiffre_affaires > 0:
        rotation = chiffre_affaires / total_actif
        if is_bank and rotation > 1:
            return False
        if not is_bank and rotation > 5:
            return False
    return True


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

    # --- BNPA normalise ---
    # Le benefice d'un seul exercice est un mauvais socle de valorisation quand
    # il est gonfle par de l'exceptionnel. FILTISAC 2024 : 18,6 Mds de resultat
    # net dont 18,86 Mds de resultat HAO, pour 4,34 Mds d'activites ordinaires.
    # Son BNPA affiche 1 318 pour un pouvoir beneficiaire ordinaire de ~308, et
    # l'exercice suivant retombe a 33. La mediane des exercices connus lisse ces
    # a-coups sans effacer une tendance de fond.
    serie_ni = [data.get(f"net_income_{s}") for s in ("n3", "n2", "n1", "n0")]
    serie_eps = sorted(v / shares for v in serie_ni
                       if v not in (None, 0) and shares) if shares else []
    ratios["eps_serie"] = serie_eps
    if len(serie_eps) >= 3:
        milieu = len(serie_eps) // 2
        ratios["eps_normalise"] = (
            serie_eps[milieu] if len(serie_eps) % 2
            else (serie_eps[milieu - 1] + serie_eps[milieu]) / 2
        )
    else:
        ratios["eps_normalise"] = None

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

    # --- Part exceptionnelle du resultat ---
    # Un benefice porte par le HAO ne se reproduira pas : FILTISAC 2024 affiche
    # 18,6 Mds de resultat net dont 18,86 Mds de HAO, et retombe a 466 M l'annee
    # suivante. Valoriser sur un tel exercice revient a extrapoler un fusil a un
    # coup.
    hao = data.get("hao_income")
    if hao is not None and net_income:
        ratios["part_exceptionnelle"] = hao / net_income
    else:
        ratios["part_exceptionnelle"] = None

    # --- Resultat des activites ordinaires par action ---
    rao = data.get("ordinary_income")
    ratios["ordinary_income"] = rao
    ratios["eps_ordinaire"] = (rao / shares) if rao and shares else None

    # --- Cout du risque rapporte au PNB (banques) ---
    # L'analyse « en ciseau » : pour une banque, le resultat net n'est qu'un
    # symptome. Le diagnostic se lit dans le rapport entre le cout du risque et
    # le produit net bancaire.
    cdr = data.get("cost_of_risk")
    pnb = data.get("revenue_bank") or (revenue if is_bank else None)
    if cdr is not None and pnb:
        ratios["cout_risque_pnb"] = abs(cdr) / abs(pnb)
    else:
        ratios["cout_risque_pnb"] = None

    # --- Grille bancaire ---
    # Pour une banque, le resultat net ne dit rien de la trajectoire. Le
    # coefficient d'exploitation mesure ce que coute la machine ; le cout du
    # risque mesure ce que coute le portefeuille. C'est le croisement des deux
    # — l'analyse « en ciseau » — qui fait le diagnostic.
    charges = data.get("operating_expenses")
    rbe = data.get("gross_operating_income")
    depots = data.get("deposits")
    credits = data.get("loans")
    pnb_ref = data.get("revenue_bank") or (revenue if is_bank else None)

    _charges_ok = _charges_credibles(charges, pnb_ref)
    ratios["coefficient_exploitation"] = (
        abs(charges) / abs(pnb_ref) if _charges_ok else None)
    # Le resultat brut d'exploitation ne peut pas depasser le produit dont il
    # est issu : au-dela, c'est que l'un des deux vient d'une autre periode.
    ratios["marge_brute_exploitation"] = (
        rbe / pnb_ref
        if rbe and pnb_ref and abs(rbe) <= abs(pnb_ref) else None)
    # Des encours negatifs n'existent pas : Oragroup ressortait a -45 % de
    # credits sur depots, un signe capte dans la mauvaise colonne.
    ratios["credits_depots"] = (
        credits / depots
        if credits and depots and credits > 0 and depots > 0 else None)
    ratios["depots"] = depots
    ratios["credits"] = credits
    # Rendement des actifs : une banque se juge aussi sur ce que rapporte son
    # bilan, pas seulement ses fonds propres.
    _actif_brut = data.get("total_assets")
    _actif_ok = _actif_fiable(_actif_brut, equity, revenue, is_bank)
    ratios["total_assets"] = _actif_brut if _actif_ok else None
    ratios["actif_incoherent"] = bool(_actif_brut) and not _actif_ok
    ratios["roa"] = (net_income / _actif_brut
                     if _actif_ok and net_income else None)

    # --- Grille telecoms ---
    # Le secteur se juge sur trois axes que le resultat net ne montre pas : la
    # marge d'EBITDA (rentabilite operationnelle avant le poids des reseaux),
    # l'intensite capitalistique (ce que le reseau engloutit chaque annee) et
    # le levier rapporte a l'EBITDA. Les operateurs communiquent d'ailleurs en
    # EBITDAaL, pas en resultat net.
    ebitda_v = data.get("ebitda")
    capex_v = abs(capex) if capex else None
    ratios["ebitda"] = ebitda_v
    ratios["marge_ebitda"] = (ebitda_v / revenue) if ebitda_v and revenue else None
    ratios["intensite_capex"] = (capex_v / revenue) if capex_v and revenue else None
    ratios["dette_ebitda"] = (
        total_debt / ebitda_v if ebitda_v and not total_debt_missing and total_debt
        else None)

    # --- Indicateurs communs aux secteurs industriels et commerciaux ---
    # Trois mesures qui, hors banque et telecoms, disent l'essentiel :
    #  - la marge d'exploitation isole la performance du metier, avant la
    #    structure financiere et l'impot ;
    #  - la rotation de l'actif dit combien de chiffre d'affaires un franc
    #    d'actif engendre — determinante en distribution (elevee) comme en
    #    services publics (faible), et illisible sans le secteur ;
    #  - la volatilite du resultat mesure le caractere cyclique, ce qui compte
    #    davantage que le niveau du resultat pour l'agriculture et les matieres
    #    premieres.
    ratios["marge_exploitation"] = (ebit / revenue) if ebit and revenue else None
    ratios["rotation_actif"] = (
        revenue / _actif_brut if revenue and _actif_ok else None)

    _serie = [data.get(f"net_income_n{i}") for i in range(3, -1, -1)]
    _serie = [v for v in _serie if v not in (None, 0)]
    if len(_serie) >= 3:
        _moyenne = sum(_serie) / len(_serie)
        if _moyenne:
            _variance = sum((v - _moyenne) ** 2 for v in _serie) / len(_serie)
            ratios["volatilite_resultat"] = (_variance ** 0.5) / abs(_moyenne)
        else:
            ratios["volatilite_resultat"] = None
    else:
        ratios["volatilite_resultat"] = None

    # --- P/B (Price to Book) ---
    book_value_per_share = equity / shares if shares != 0 else 0
    ratios["bvps"] = book_value_per_share or None
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
    ratios["flags"] = _compute_flags(ratios, is_bank, data.get("sector"))

    # --- Checklist Value & Dividendes ---
    ratios["checklist"] = _compute_checklist(ratios, is_bank)

    # --- Score fondamental ---
    _breakdown = _compute_fundamental_breakdown(ratios, is_bank)
    ratios["fundamental_score"] = _breakdown["total"]
    ratios["fundamental_breakdown"] = _breakdown

    return ratios


def _compute_flags(ratios: dict, is_bank: bool, secteur: str = None) -> dict:
    """Calcule les drapeaux (OK/Vigilance/Risque) pour chaque ratio.

    Les bornes generiques ci-dessous sont ensuite RELUES a la lumiere du
    secteur : une marge d'EBITDA de 8 % condamne un operateur telecoms et
    ne dit rien d'anormal chez un distributeur. Voir `analysis.sectors`.
    """
    flags = {}

    # Part exceptionnelle du resultat
    part = ratios.get("part_exceptionnelle")
    if part is not None:
        if abs(part) >= 0.50:
            flags["part_exceptionnelle"] = (
                "Risque", f"Résultat porté à {abs(part)*100:.0f}% par l'exceptionnel")
        elif abs(part) >= 0.25:
            flags["part_exceptionnelle"] = (
                "Vigilance", f"{abs(part)*100:.0f}% du résultat est exceptionnel")
        else:
            flags["part_exceptionnelle"] = ("OK", "Résultat essentiellement courant")

    # Marge d'EBITDA — la reference du secteur telecom
    me = ratios.get("marge_ebitda")
    if me is not None:
        if me >= 0.40:
            flags["marge_ebitda"] = ("OK", f"Marge d'EBITDA élevée ({me*100:.0f} %)")
        elif me >= 0.30:
            flags["marge_ebitda"] = ("OK", f"Marge d'EBITDA solide ({me*100:.0f} %)")
        elif me >= 0.20:
            flags["marge_ebitda"] = ("Vigilance", f"Marge d'EBITDA moyenne ({me*100:.0f} %)")
        else:
            flags["marge_ebitda"] = ("Risque", f"Marge d'EBITDA faible ({me*100:.0f} %)")

    # Intensite capitalistique : un reseau se paie tous les ans
    ic = ratios.get("intensite_capex")
    if ic is not None:
        if ic <= 0.20:
            flags["intensite_capex"] = ("OK", f"Investissement contenu ({ic*100:.0f} % du CA)")
        elif ic <= 0.30:
            flags["intensite_capex"] = ("Vigilance", f"Investissement lourd ({ic*100:.0f} % du CA)")
        else:
            flags["intensite_capex"] = ("Risque", f"Investissement très lourd ({ic*100:.0f} % du CA)")

    # Levier rapporte a l'EBITDA
    de = ratios.get("dette_ebitda")
    if de is not None:
        if de <= 2:
            flags["dette_ebitda"] = ("OK", f"Levier confortable ({de:.1f}×)")
        elif de <= 3:
            flags["dette_ebitda"] = ("Vigilance", f"Levier à surveiller ({de:.1f}×)")
        else:
            flags["dette_ebitda"] = ("Risque", f"Levier élevé ({de:.1f}×)")

    # Coefficient d'exploitation : ce que coute la machine bancaire
    ce = ratios.get("coefficient_exploitation")
    if ce is not None:
        if ce <= 0.50:
            flags["coefficient_exploitation"] = (
                "OK", f"Machine efficace ({ce*100:.0f} % du PNB)")
        elif ce <= 0.65:
            flags["coefficient_exploitation"] = (
                "OK", f"Coefficient d'exploitation correct ({ce*100:.0f} %)")
        elif ce <= 0.75:
            flags["coefficient_exploitation"] = (
                "Vigilance", f"Structure de coûts lourde ({ce*100:.0f} % du PNB)")
        else:
            flags["coefficient_exploitation"] = (
                "Risque", f"Coefficient d'exploitation à {ce*100:.0f} % du PNB")

    # Credits / depots : au-dela de 100 %, la banque prete plus qu'elle ne
    # collecte et depend de ressources de marche, plus cheres et plus volatiles.
    cd = ratios.get("credits_depots")
    if cd is not None:
        if cd <= 0.80:
            flags["credits_depots"] = ("OK", f"Liquidité confortable ({cd*100:.0f} %)")
        elif cd <= 1.00:
            flags["credits_depots"] = ("OK", f"Crédits/dépôts à {cd*100:.0f} %")
        else:
            flags["credits_depots"] = (
                "Vigilance", f"Crédits supérieurs aux dépôts ({cd*100:.0f} %)")

    # Cout du risque rapporte au PNB (banques) — l'analyse « en ciseau »
    cdr = ratios.get("cout_risque_pnb")
    if cdr is not None:
        if cdr >= 0.30:
            flags["cout_risque_pnb"] = (
                "Risque", f"Coût du risque à {cdr*100:.0f}% du PNB")
        elif cdr >= 0.15:
            flags["cout_risque_pnb"] = (
                "Vigilance", f"Coût du risque à {cdr*100:.0f}% du PNB")
        else:
            flags["cout_risque_pnb"] = (
                "OK", f"Coût du risque contenu ({cdr*100:.0f}% du PNB)")

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

    # --- Relecture sectorielle ---
    # Le referentiel a le dernier mot quand il se prononce : ses bornes sont
    # calibrees metier par metier. Il complete aussi les ratios que la grille
    # generique ne jugeait pas du tout (rotation de l'actif, volatilite).
    if secteur:
        try:
            from analysis.sectors import juger
            for cle, valeur in list(ratios.items()):
                if isinstance(valeur, (int, float)):
                    verdict = juger(cle, valeur, secteur)
                    if verdict:
                        flags[cle] = verdict
        except Exception:
            pass

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
    """Calcule score fondamental total /50. Wrapper autour de _compute_fundamental_breakdown."""
    bd = _compute_fundamental_breakdown(ratios, is_bank)
    return bd["total"]


def _compute_fundamental_breakdown(ratios: dict, is_bank: bool) -> dict:
    """Décompose le score fondamental en 4 sous-scores thématiques
    (pour l'affichage type "card breakdown" du design v3) :

    - Rentabilité (/15) : ROE (10) + Marge nette (5)
    - Endettement (/10) : Dette/Equity (5) + Couverture intérêts (5)
    - Valorisation (/15) : PER (8) + P/B (7)
    - Dividendes (/10) : Yield (10)  (payout + couverture dans "Rentabilité dividende"
                                      absorbé dans les points forts / vigilance)

    Retourne {rentabilite, endettement, valorisation, dividendes, total, profile}.
    """
    # --- Rentabilité (15 pts) ---
    rent = 0
    roe = ratios.get("roe")
    if roe is not None:
        if roe >= 0.25: rent += 10
        elif roe >= 0.20: rent += 8
        elif roe >= 0.15: rent += 6
        elif roe >= 0.10: rent += 3
        else: rent += 1
    nm = ratios.get("net_margin")
    if nm is not None:
        if nm >= 0.20: rent += 5
        elif nm >= 0.15: rent += 4
        elif nm >= 0.10: rent += 3
        elif nm >= 0.05: rent += 1

    # --- Endettement (10 pts) ---
    endet = 0
    if is_bank:
        endet = 6  # N/A → neutre haut
    else:
        de = ratios.get("debt_equity")
        if de is None: endet += 2
        elif de <= 0.3: endet += 5
        elif de <= 0.5: endet += 4
        elif de <= 1.0: endet += 3
        elif de <= 1.5: endet += 1
        ic = ratios.get("interest_coverage")
        if ic is None: endet += 2
        elif ic >= 5: endet += 5
        elif ic >= 3: endet += 4
        elif ic >= 2: endet += 2
        elif ic >= 1: endet += 1

    # --- Valorisation (15 pts) ---
    valo = 0
    per = ratios.get("per")
    if per is not None and per > 0:
        if per < 8: valo += 8
        elif per < 10: valo += 7
        elif per <= 12: valo += 5
        elif per <= 15: valo += 3
        else: valo += 1
    pb = ratios.get("pb")
    if pb is not None and pb > 0:
        if pb < 1: valo += 7
        elif pb < 1.5: valo += 5
        elif pb < 2: valo += 3
        else: valo += 1
    elif pb is None:
        valo += 3  # neutre si pas de donnée

    # --- Dividendes (10 pts) ---
    div = 0
    dy = ratios.get("dividend_yield")
    if dy is not None:
        if dy >= 0.08: div += 10
        elif dy >= 0.06: div += 8
        elif dy >= 0.04: div += 5
        elif dy >= 0.02: div += 2
    # Bonus -2 si payout > 100% (non soutenable), +0 sinon
    pr = ratios.get("payout_ratio")
    if pr is not None and pr > 1.0:
        div = max(0, div - 2)

    total = rent + endet + valo + div

    # --- Profil narratif (court : "Moyen - valorisation tendue, distribution attractive") ---
    fragments = []
    if rent >= 11: fragments.append("rentabilité solide")
    elif rent <= 5: fragments.append("rentabilité faible")
    if endet >= 7: fragments.append("endettement maîtrisé")
    elif endet <= 3: fragments.append("endettement élevé")
    if valo >= 11: fragments.append("valorisation attractive")
    elif valo <= 4: fragments.append("valorisation tendue")
    if div >= 7: fragments.append("distribution attractive")
    elif div <= 3: fragments.append("distribution faible")

    if total >= 38:
        quality = "Excellent"
    elif total >= 28:
        quality = "Bon"
    elif total >= 18:
        quality = "Moyen"
    else:
        quality = "Faible"

    profile = quality + (" — " + ", ".join(fragments[:2]) if fragments else "")

    return {
        "rentabilite": rent,
        "endettement": endet,
        "valorisation": valo,
        "dividendes": div,
        "total": total,
        "profile": profile,
        "quality": quality,
    }


def compute_target_price(ratios: dict, sector: Optional[str] = None,
                          benchmarks: Optional[dict] = None) -> dict:
    """Calcule un prix cible simple à partir de deux méthodes transparentes :

    - **PER fair** : min(PER médian secteur, 15) × EPS
      (borne haute 15 pour rester Value ; utile quand EPS > 0)
    - **Yield fair** : DPS / max(yield médian secteur, 5%)
      (borne basse 5% qui est l'ancre BRVM pour un investisseur revenus)

    Si les deux méthodes sont disponibles, moyenne simple (pondération 1:1).
    Retourne dict avec target_price, components (liste de méthodes utilisées
    avec détail), current_price, delta_abs, delta_pct, confidence.
    """
    price = ratios.get("price") or 0
    eps = ratios.get("eps")
    dps = ratios.get("dps") or 0

    # Résout les benchmarks si non fournis
    if benchmarks is None:
        try:
            benchmarks = get_sector_benchmarks(sector)
        except Exception:
            benchmarks = {}
    sec_key = sector if sector and benchmarks and sector in benchmarks else "global"
    sec_b = benchmarks.get(sec_key, {}) if benchmarks else {}

    components = []

    # ── Méthode 1 : PER sectoriel borné à 15 ──
    per_med = None
    if isinstance(sec_b, dict):
        per_stats = sec_b.get("per") or {}
        per_med = per_stats.get("median") if isinstance(per_stats, dict) else None
    fair_per = None
    if per_med and per_med > 0:
        fair_per = min(per_med, 15)
    elif not per_med:
        fair_per = 12  # défaut prudent si pas de benchmark

    if eps and eps > 0 and fair_per:
        price_per_raw = fair_per * eps
        # Cap anti-aberration : si le marché décote structurellement un titre
        # (ex. ETIT PER 1.6× vs sectoriel 10×), la méthode PER donnerait un
        # target 10× le prix, physiquement peu crédible. On borne à 3× le
        # prix actuel pour rester dans une fourchette d'upside réaliste.
        price_per_capped = min(price_per_raw, 3 * price) if price else price_per_raw
        capped = price_per_capped < price_per_raw
        formula = f"PER {fair_per:.1f}× × EPS {eps:,.0f}"
        if capped:
            formula += " (plafonné à 3× cours)"
        components.append({
            "method": "PER sectoriel",
            "formula": formula,
            "price": price_per_capped,
            "raw_price": price_per_raw,
            "capped": capped,
        })

    # ── Méthode 2 : Yield cible ≥ 5% ──
    y_med = None
    if isinstance(sec_b, dict):
        y_stats = sec_b.get("dividend_yield") or {}
        y_med = y_stats.get("median") if isinstance(y_stats, dict) else None
    fair_yield = max(y_med, 0.05) if y_med else 0.06

    if dps and dps > 0 and fair_yield:
        price_yield_raw = dps / fair_yield
        # Même cap anti-aberration côté upper bound
        price_yield_capped = min(price_yield_raw, 3 * price) if price else price_yield_raw
        capped_y = price_yield_capped < price_yield_raw
        formula_y = f"DPS {dps:,.0f} / yield {fair_yield*100:.1f}%"
        if capped_y:
            formula_y += " (plafonné à 3× cours)"
        components.append({
            "method": "Yield cible",
            "formula": formula_y,
            "price": price_yield_capped,
            "raw_price": price_yield_raw,
            "capped": capped_y,
        })

    if not components:
        return {
            "target_price": None,
            "current_price": price,
            "delta_abs": None,
            "delta_pct": None,
            "components": [],
            "confidence": "indéterminée",
        }

    target = sum(c["price"] for c in components) / len(components)
    delta_abs = target - price if price else None
    delta_pct = (delta_abs / price * 100) if price else None

    # Confiance : dispersion faible entre méthodes → élevée
    if len(components) == 2:
        spread = abs(components[0]["price"] - components[1]["price"]) / target
        confidence = "élevée" if spread < 0.20 else ("moyenne" if spread < 0.50 else "faible")
    else:
        confidence = "moyenne"  # une seule méthode disponible

    return {
        "target_price": target,
        "current_price": price,
        "delta_abs": delta_abs,
        "delta_pct": delta_pct,
        "components": components,
        "confidence": confidence,
    }


def compute_valorisation_croisee(ratios: dict, sector: Optional[str] = None,
                                  benchmarks: Optional[dict] = None) -> dict:
    """Valorisation croisee — lecture ENRICHIE, affichee A COTE de l'historique.

    `compute_target_price` reste la reference et n'est pas modifiee : c'est elle
    qui a produit tout l'historique de suivi, et la remplacer rendrait les
    cibles d'hier incomparables avec celles d'aujourd'hui. Cette fonction
    propose une seconde lecture, plus complete, sans rien ecraser.

    Aucune methode n'est fiable seule sur la BRVM : on les croise et on lit
    leur dispersion comme un indicateur de confiance.

    - **PER sectoriel** : min(PER median du secteur, 15) x BNPA. Le BNPA retenu
      est le BNPA NORMALISE (mediane des exercices connus) des qu'il existe :
      valoriser sur le benefice d'une seule annee revient a extrapoler un
      exercice exceptionnel (FILTISAC 2024 : BNPA 1 318 dont l'essentiel en
      resultat HAO, puis 33 l'annee suivante).
    - **Rendement cible** : DPS / max(rendement median, 5 %). La BRVM est un
      marche de rendement, c'est le poids le plus fort.
    - **PBR justifie** : (ROE / cout des capitaux propres) x valeur comptable
      par action. Ancre solide pour les banques, qui pesent le tiers de la cote.
    - **Valeur comptable** : servie comme PLANCHER, pas comme cible. Une societe
      qui cote sous ses fonds propres n'est pas forcement une bonne affaire,
      mais l'ecart merite d'etre signale.

    Retourne aussi une fourchette (methode la plus basse / la plus haute), qui
    dit bien plus qu'un point unique.
    """
    price = ratios.get("price") or 0
    dps = ratios.get("dps") or 0
    roe = ratios.get("roe")
    bvps = ratios.get("bvps")

    # BNPA normalise si disponible, sinon le dernier connu
    eps_normalise = ratios.get("eps_normalise")
    eps_annuel = ratios.get("eps")
    eps = eps_normalise or eps_annuel
    eps_lisse = bool(eps_normalise)

    # Resout les benchmarks si non fournis
    if benchmarks is None:
        try:
            benchmarks = get_sector_benchmarks(sector)
        except Exception:
            benchmarks = {}
    sec_key = sector if sector and benchmarks and sector in benchmarks else "global"
    sec_b = benchmarks.get(sec_key, {}) if benchmarks else {}

    def _mediane(champ):
        if not isinstance(sec_b, dict):
            return None
        stats = sec_b.get(champ) or {}
        return stats.get("median") if isinstance(stats, dict) else None

    components = []

    def _ajouter(methode, formule, valeur, poids):
        """Enregistre une methode, plafonnee a 3x le cours.

        Le marche decote structurellement certains titres (ETIT se paie 3,7x ses
        benefices quand le secteur est a 10x) : sans plafond, une methode
        proposerait une cible a dix fois le cours, ce qui n'a aucun sens
        operationnel.
        """
        if not valeur or valeur <= 0:
            return
        plafonne = min(valeur, 3 * price) if price else valeur
        components.append({
            "method": methode,
            "formula": formule + (" (plafonné à 3× cours)" if plafonne < valeur else ""),
            "price": plafonne,
            "raw_price": valeur,
            "capped": plafonne < valeur,
            "poids": poids,
        })

    # ── Methode 1 : PER sectoriel borne a 15 ──
    per_med = _mediane("per")
    fair_per = min(per_med, 15) if per_med and per_med > 0 else 12
    if eps and eps > 0:
        libelle_eps = "BNPA normalisé" if eps_lisse else "BNPA"
        _ajouter("PER sectoriel",
                 f"PER {fair_per:.1f}× × {libelle_eps} {eps:,.0f}",
                 fair_per * eps, 0.35)

    # ── Methode 2 : rendement cible >= 5 % ──
    y_med = _mediane("dividend_yield")
    fair_yield = max(y_med, 0.05) if y_med else 0.06
    if dps and dps > 0:
        _ajouter("Rendement cible",
                 f"DPS {dps:,.0f} / rendement {fair_yield*100:.1f}%",
                 dps / fair_yield, 0.40)

    # ── Methode 3 : PBR justifie par la rentabilite ──
    # Un PBR se justifie par le ROE rapporte au cout des capitaux propres :
    # une societe qui rentabilise ses fonds propres au-dela de ce que l'actionnaire
    # exige vaut plus que sa valeur comptable, et inversement.
    if roe and roe > 0 and bvps and bvps > 0:
        pbr_justifie = roe / COUT_CAPITAUX_PROPRES
        _ajouter("PBR justifié",
                 f"ROE {roe*100:.1f}% / {COUT_CAPITAUX_PROPRES*100:.0f}% "
                 f"× VC {bvps:,.0f}",
                 pbr_justifie * bvps, 0.25)

    if not components:
        return {
            "target_price": None,
            "current_price": price,
            "delta_abs": None,
            "delta_pct": None,
            "components": [],
            "confidence": "indéterminée",
            "fourchette_basse": None,
            "fourchette_haute": None,
            "plancher": bvps,
            "eps_lisse": eps_lisse,
        }

    poids_total = sum(c["poids"] for c in components)
    target = sum(c["price"] * c["poids"] for c in components) / poids_total
    basse = min(c["price"] for c in components)
    haute = max(c["price"] for c in components)
    delta_abs = target - price if price else None
    delta_pct = (delta_abs / price * 100) if price else None

    # Confiance : c'est l'ECART ENTRE METHODES qui la fonde. Deux methodes qui
    # convergent valent mieux que quatre qui se contredisent.
    if len(components) >= 2 and target:
        dispersion = (haute - basse) / target
        confidence = ("élevée" if dispersion < 0.20
                      else "moyenne" if dispersion < 0.50 else "faible")
    else:
        confidence = "moyenne"

    return {
        "target_price": target,
        "current_price": price,
        "delta_abs": delta_abs,
        "delta_pct": delta_pct,
        "components": components,
        "confidence": confidence,
        "fourchette_basse": basse,
        "fourchette_haute": haute,
        "plancher": bvps,
        "eps_lisse": eps_lisse,
    }


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
        # Un montant sous dix francs perd tout son sens arrondi a l'entier :
        # le dividende par action d'ETI vaut 0,93 FCFA et s'affichait « 1 ».
        return f"{value:,.2f}" if abs(value) < 10 else f"{value:,.0f}"
    elif fmt == "decimal":
        return f"{value:.2f}"
    return str(value)
