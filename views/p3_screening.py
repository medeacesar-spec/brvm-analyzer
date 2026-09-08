"""
Page 3 : Screening multi-critères
Filtrage par secteur + filtres fondamentaux (Yield / PER / ROE / Payout / D/E)
avec tableau éditorial incluant Score et Verdict.
"""

import streamlit as st
import pandas as pd
import json as _json

from data.storage import get_all_stocks_for_analysis, get_analyzable_tickers
from data.db import read_sql_df
from analysis.fundamental import compute_ratios, format_ratio
from utils.nav import ticker_quick_picker
from utils.ui_helpers import section_heading


@st.cache_data(ttl=300, show_spinner=False)
def _load_verdicts_dict() -> dict:
    """Retourne {ticker: (verdict, hybrid_score)} depuis scoring_snapshot
    pour afficher la colonne Verdict de manière cohérente avec les autres
    pages. 1 requête Supabase cachée 5 min."""
    try:
        df = read_sql_df(
            "SELECT ticker, verdict, hybrid_score FROM scoring_snapshot"
        )
    except Exception:
        return {}
    if df.empty:
        return {}
    return {r["ticker"]: (r.get("verdict"), r.get("hybrid_score"))
            for _, r in df.iterrows()}


def render():
    # Hiérarchie v3 : Title + caption → univers → filtres → résultats
    st.title("Screening multi-critères")
    st.caption("Les filtres comptables et les filtres de marché sont "
               "séparés : un titre peut tenir tous les seuils du bilan et "
               "n'échanger que 900 000 francs par mois.")

    all_stocks = get_all_stocks_for_analysis()
    if all_stocks.empty:
        st.warning("Aucune donnée disponible. Lancez l'enrichissement des données de marché.")
        return

    verdicts = _load_verdicts_dict()
    # Volatilite, rendement et liquidite viennent de la serie mensuelle, pas
    # des fondamentaux : un titre peut etre excellent au bilan et impossible a
    # vendre. Charge une fois pour toute la page.
    try:
        from analysis.risque import toutes_les_mesures, formater as _fmt_risque
        mesures_risque = toutes_les_mesures()
    except Exception:                                           # noqa: BLE001
        mesures_risque, _fmt_risque = {}, None

    # ─── Univers d'analyse ──────────────────────────────────────────────
    section_heading("Univers d'analyse", spacing="tight")
    available_sectors = sorted(all_stocks["sector"].dropna().unique().tolist())

    col_sectors, col_tickers = st.columns(2)

    with col_sectors:
        st.markdown(
            "<div class='label-xs' style='margin-bottom:4px;'>Secteurs</div>",
            unsafe_allow_html=True,
        )
        selected_sectors = st.multiselect(
            "Secteurs", available_sectors,
            label_visibility="collapsed",
            placeholder="Tous les secteurs",
        )
        if not selected_sectors:
            selected_sectors = available_sectors

    filtered_stocks = all_stocks[all_stocks["sector"].isin(selected_sectors)]

    with col_tickers:
        st.markdown(
            f"<div class='label-xs' style='margin-bottom:4px;'>"
            f"Titres · {len(filtered_stocks)} disponibles</div>",
            unsafe_allow_html=True,
        )
        ticker_options = [f"{r['ticker']} · {r['company_name']}"
                          for _, r in filtered_stocks.iterrows()]
        selected_tickers = st.multiselect(
            "Titres", ticker_options,
            label_visibility="collapsed",
            placeholder="Tous les titres",
        )
        if not selected_tickers:
            selected_tickers = ticker_options
        target_tickers = {s.split(" · ")[0] for s in selected_tickers}

    # La rangée de KPI se remplit APRÈS le calcul, mais s'affiche AVANT les
    # onglets : c'est elle qui rend visible l'effet d'un filtre pendant qu'on
    # est encore dans l'onglet des filtres.
    zone_kpi = st.container()

    onglet_fond, onglet_risque, onglet_res = st.tabs(
        ["Filtres fondamentaux", "Risque et liquidité", "Résultats"])

    # UN SEUIL SEUL NE DIT RIEN. « PER ≤ 15 » ne se juge qu'à son effet : sur
    # cette cote il retire neuf titres ou vingt-neuf selon l'exercice publié.
    # Le canevas met donc les seuils en TABLEAU, avec la colonne qui manquait —
    # ce que chaque critère écarte. Elle ne peut se remplir qu'après le calcul :
    # on réserve ici l'emplacement, comme la rangée de KPI juste au-dessus.
    effets = {}

    _ENTETE = ("font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;"
               "color:var(--ink-3);font-weight:600;padding:0 0 7px;"
               "border-bottom:1px solid var(--border);")
    _CRITERE = ("font-size:13px;font-weight:600;padding-top:9px;"
                "color:var(--ink);")
    _EXPLIQUE = "font-size:12.5px;color:var(--ink-3);padding-top:11px;"
    _COLONNES = [2.1, 1, 1, 2.8]

    def _entete_seuils(derniere: str):
        cols = st.columns(_COLONNES)
        for col, txt, align in zip(cols, ("Critère", "Minimum", "Maximum",
                                          derniere),
                                   ("left", "right", "right", "left")):
            with col:
                st.markdown(f"<div style='{_ENTETE}text-align:{align};'>"
                            f"{txt}</div>", unsafe_allow_html=True)

    def _ligne_seuil(label, key_prefix, default_max, step=1.0, divide=False,
                     min_abs=0.0, max_abs=None, pourquoi=None, colonne=None):
        """Une ligne du tableau des seuils.

        Les deux champs restent des champs — un tableau qu'on ne peut pas
        régler ne servirait à rien. C'est la mise en colonnes et la dernière
        cellule qui changent : l'effet mesuré, ou la raison du critère.
        """
        c1, c2, c3, c4 = st.columns(_COLONNES)
        with c1:
            st.markdown(f"<div style='{_CRITERE}'>{label}</div>",
                        unsafe_allow_html=True)
        with c2:
            vmin = st.number_input(
                f"{label} min", min_value=min_abs, max_value=max_abs,
                value=0.0, step=step, key=f"{key_prefix}_min",
                label_visibility="collapsed")
        with c3:
            vmax = st.number_input(
                f"{label} max", min_value=min_abs, max_value=max_abs,
                value=default_max, step=step, key=f"{key_prefix}_max",
                label_visibility="collapsed")
        with c4:
            if pourquoi is not None:
                st.markdown(f"<div style='{_EXPLIQUE}'>{pourquoi}</div>",
                            unsafe_allow_html=True)
            else:
                effets[label] = (st.empty(), colonne)
        if divide:
            return vmin / 100, vmax / 100
        return vmin, vmax

    with onglet_fond:
        _entete_seuils("Effet sur l'univers")
        min_yield, max_yield = _ligne_seuil(
            "Dividend Yield", "yield", 30.0, step=0.5, divide=True,
            max_abs=30.0, colonne="dividend_yield")
        min_per, max_per = _ligne_seuil(
            "PER", "per", 100.0, step=1.0, max_abs=100.0, colonne="per")
        min_roe, max_roe = _ligne_seuil(
            "ROE", "roe", 100.0, step=1.0, divide=True, max_abs=100.0,
            colonne="roe")
        min_payout, max_payout = _ligne_seuil(
            "Payout", "payout", 200.0, step=5.0, divide=True, max_abs=200.0,
            colonne="payout_ratio")
        min_de, max_de = _ligne_seuil(
            "D/E", "de", 20.0, step=0.5, max_abs=20.0, colonne="debt_equity")
        pied_fond = st.empty()

    with onglet_risque:
        # Separes des fondamentaux, et c'est delibere : ils ne se lisent pas
        # dans les comptes mais dans le cours. Un titre peut tenir tous les
        # seuils comptables et n'echanger que 900 000 francs par mois.
        if mesures_risque:
            st.markdown(
                "<div style='background:var(--bg-elev);border:1px solid "
                "var(--border);border-left:2px solid var(--warn);"
                "border-radius:0 12px 12px 0;padding:12px 16px;"
                "margin-bottom:14px;'>"
                "<div style='font-size:13px;color:var(--ink-2);line-height:1.5;"
                "max-width:76ch;'>Ces filtres ne se lisent pas au bilan mais "
                "dans le cours. Un titre peut afficher 42/50 au fondamental et "
                "n'échanger que 900 000 francs par mois : excellent sur le "
                "papier, impossible à vendre en pratique.</div></div>",
                unsafe_allow_html=True,
            )
            _entete_seuils("Pourquoi")
            min_vol, max_vol = _ligne_seuil(
                "Volatilité annuelle", "vol", 150.0, step=5.0, divide=True,
                max_abs=150.0,
                pourquoi="écart-type des rendements mensuels, annualisé")
            min_rdt, max_rdt = _ligne_seuil(
                "Rendement annualisé", "rdt", 100.0, step=5.0, divide=True,
                min_abs=-100.0, max_abs=200.0,
                pourquoi="sur les cinq dernières années, dividendes compris")
            c1, c2, c3, c4 = st.columns(_COLONNES)
            with c1:
                st.markdown(f"<div style='{_CRITERE}'>Échangé par mois</div>",
                            unsafe_allow_html=True)
            with c2:
                min_echange = st.number_input(
                    "Montant échangé minimum", min_value=0.0, value=0.0,
                    step=10.0, key="echange_min", label_visibility="collapsed",
                    help="En millions de FCFA.") * 1e6
            with c3:
                st.markdown(f"<div style='{_EXPLIQUE}text-align:right;'>—</div>",
                            unsafe_allow_html=True)
            with c4:
                st.markdown(
                    f"<div style='{_EXPLIQUE}'>en millions de FCFA ; sous "
                    f"10 M par mois, un titre se revend mal</div>",
                    unsafe_allow_html=True)
            st.caption(
                "Un titre sans historique mensuel suffisant n'est **pas** "
                "écarté par ces trois critères : il n'a simplement pas ces "
                "mesures, et l'exclure reviendrait à punir une introduction "
                "récente de sa jeunesse."
            )
        else:
            min_vol, max_vol = 0.0, 99.0
            min_rdt, max_rdt = -99.0, 99.0
            min_echange = 0.0

    # ─── Compute ratios ─────────────────────────────────────────────────
    results = []
    for _, row in filtered_stocks.iterrows():
        ticker = row.get("ticker", "")
        if ticker not in target_tickers:
            continue
        data = row.to_dict()
        for k, v in data.items():
            if pd.isna(v) if isinstance(v, (float, int)) else False:
                data[k] = None
        try:
            ratios = compute_ratios(data)
            dy = ratios.get("dividend_yield")
            if not dy and data.get("market_dividend_yield"):
                dy = data["market_dividend_yield"]

            verdict, hybrid = verdicts.get(ticker, (None, None))
            risque = mesures_risque.get(ticker) or {}
            # Un titre sans historique mensuel suffisant n'est pas ecarte : il
            # n'a simplement pas ces mesures. L'exclure reviendrait a punir une
            # introduction recente de sa jeunesse.
            if risque:
                vol = risque.get("volatilite")
                rdt = risque.get("rendement_annualise")
                ech = risque.get("montant_echange")
                if vol is not None and not (min_vol <= vol <= max_vol):
                    continue
                if rdt is not None and not (min_rdt <= rdt <= max_rdt):
                    continue
                if ech is not None and ech < min_echange:
                    continue

            results.append({
                "ticker": ticker,
                "name": data.get("company_name") or "",
                "sector": data.get("sector") or "",
                "price": data.get("price") or 0,
                "dividend_yield": dy,
                "per": ratios.get("per"),
                "roe": ratios.get("roe"),
                "payout_ratio": ratios.get("payout_ratio"),
                "debt_equity": ratios.get("debt_equity"),
                "fundamental_score": ratios.get("fundamental_score"),
                "hybrid_score": hybrid,
                "verdict": verdict,
                "volatilite": risque.get("volatilite"),
                "rendement_annualise": risque.get("rendement_annualise"),
                "montant_echange": risque.get("montant_echange"),
            })
        except Exception:
            continue

    if not results:
        st.warning("Aucun titre avec des données dans la sélection.")
        return

    screen_df = pd.DataFrame(results)

    # Apply ratio filters.
    #
    # UN MASQUE PAR CRITERE, et non un seul masque cumule : la colonne « effet
    # sur l'univers » demande de savoir ce que CHAQUE critere retire a lui
    # seul. Le resultat final est leur conjonction — rien ne change au
    # filtrage, seule la comptabilite s'ajoute.
    _vrai = lambda: pd.Series(True, index=screen_df.index)
    masques = {}

    m = _vrai()
    if min_yield > 0:
        m &= screen_df["dividend_yield"].fillna(0) >= min_yield
    if max_yield < 0.30:
        m &= screen_df["dividend_yield"].fillna(0) <= max_yield
    masques["Dividend Yield"] = m

    m = _vrai()
    if min_per > 0:
        m &= (screen_df["per"].fillna(0) >= min_per) | (screen_df["per"].fillna(0) <= 0)
    if max_per < 100:
        m &= (screen_df["per"].fillna(999) <= max_per) & (screen_df["per"].fillna(0) > 0)
    masques["PER"] = m

    m = _vrai()
    if min_roe > 0:
        m &= screen_df["roe"].fillna(0) >= min_roe
    if max_roe < 1.0:
        m &= screen_df["roe"].fillna(0) <= max_roe
    masques["ROE"] = m

    m = _vrai()
    if min_payout > 0:
        m &= screen_df["payout_ratio"].fillna(0) >= min_payout
    if max_payout < 2.0:
        m &= screen_df["payout_ratio"].fillna(0) <= max_payout
    masques["Payout"] = m

    m = _vrai()
    if min_de > 0:
        m &= screen_df["debt_equity"].fillna(0) >= min_de
    if max_de < 20:
        m &= screen_df["debt_equity"].fillna(0) <= max_de
    masques["D/E"] = m

    mask = _vrai()
    for m in masques.values():
        mask &= m

    # La colonne qui manquait, remplie une fois le compte fait. Elle distingue
    # ce qu'un seuil ecarte de ce qu'une DONNEE MANQUANTE ecarte : ici, un
    # ratio absent vaut zero et tombe sous le seuil — le titre sort sans que
    # rien ne le dise. C'est desormais dit, ligne par ligne.
    _sans_donnee_total = 0
    for libelle, (emplacement, colonne) in effets.items():
        garde = masques.get(libelle)
        if garde is None:
            continue
        exclus = ~garde
        n = int(exclus.sum())
        manquants = 0
        if colonne and colonne in screen_df.columns:
            manquants = int((exclus & screen_df[colonne].isna()).sum())
        _sans_donnee_total += manquants
        if not n:
            texte = "ne retire rien"
        else:
            texte = f"retire {n} titre{'s' if n > 1 else ''}"
            if manquants:
                texte += (f", dont <b>{manquants} faute de donnée</b>")
        emplacement.markdown(
            f"<div style='{_EXPLIQUE}'>{texte}</div>", unsafe_allow_html=True)

    pied_fond.caption(
        "Sur ces cinq critères, un titre dont le ratio est **absent** est "
        "traité comme s'il valait zéro : il tombe sous le seuil et sort de "
        "l'univers. La colonne le dit ligne par ligne."
        + (f" Actuellement **{_sans_donnee_total}** exclusion"
           f"{'s' if _sans_donnee_total > 1 else ''} de ce seul fait."
           if _sans_donnee_total else "")
        + " Les trois critères de l'onglet voisin, eux, ne les écartent pas."
    )

    filtered = screen_df[mask].sort_values("fundamental_score", ascending=False, na_position="last")

    # ─── Rangée de KPI (au-dessus des onglets) ──────────────────────────
    _yield_med = filtered["dividend_yield"].dropna()
    _score_med = filtered["fundamental_score"].dropna()
    with zone_kpi:
        _k1, _k2, _k3, _k4 = st.columns(4)
        with _k1:
            # Carte sombre : l'univers est le point de depart, pas un resultat.
            st.markdown(
                "<div style='background:var(--primary-2);"
                "border:1px solid var(--primary-2);border-radius:12px;"
                "padding:15px 17px;'>"
                "<div style='font-size:10.5px;font-weight:600;"
                "letter-spacing:0.09em;text-transform:uppercase;"
                "color:var(--on-dark-3);'>Univers filtré</div>"
                "<div style='font-size:29px;font-weight:600;"
                "letter-spacing:-0.02em;color:var(--on-dark);"
                f"font-variant-numeric:tabular-nums;'>{len(screen_df)}</div>"
                "<div style='font-size:11.5px;color:var(--on-dark-2);'>"
                f"titres sur {len(all_stocks)}</div></div>",
                unsafe_allow_html=True,
            )
        from utils.ui_helpers import kpi_v4
        with _k2:
            kpi_v4("Correspondances", f"{len(filtered)}", "après filtres",
                   accent="var(--primary)")
        with _k3:
            _y = _yield_med.median() * 100 if not _yield_med.empty else None
            kpi_v4("Yield médian", f"{_y:.2f} %" if _y is not None else "—",
                   "de la sélection",
                   accent="var(--up)" if _y and _y >= 6 else "var(--ink-4)",
                   sub_color="var(--up)" if _y and _y >= 6 else "")
        with _k4:
            _sc = _score_med.median() if not _score_med.empty else None
            kpi_v4("Score médian",
                   f"{_sc:.0f} / 50" if _sc is not None else "—",
                   "fondamental", accent="var(--ink-4)")

    with onglet_res:
        # ─── Résultats ──────────────────────────────────────────────────────

        if filtered.empty:
            st.info("Aucun titre ne correspond. Élargissez vos critères.")
            return

        # ── Rendement contre qualité du bilan ──
        # Le tableau donne les deux colonnes ; il ne dit pas qu'elles
        # s'opposent souvent. Le nuage le montre d'un coup : en haut à
        # droite, les titres qui tiennent les deux à la fois. L'aire du
        # disque porte le montant échangé — un point gros et bien placé est
        # aussi un point qu'on peut revendre.
        _n = filtered.dropna(subset=["fundamental_score", "dividend_yield"])
        if len(_n) >= 3:
            import plotly.graph_objects as go
            from utils.charts import COLORS
            _ech = _n["montant_echange"].fillna(0)
            # sizemode="area" : c'est l'AIRE du disque qui porte le montant,
            # pas son diametre. En diametre, un titre dix fois plus liquide
            # occupait cent fois la surface et ecrasait le reste du nuage.
            _sizeref = (2.0 * _ech.max() / (36.0 ** 2)) if _ech.max() > 0 else 1
            # Etiqueter les quarante-six titres rend le nuage illisible : on ne
            # nomme que ceux qui tiennent les deux reperes, plus les cinq
            # meilleurs scores. Le survol nomme tous les autres.
            _notables = (
                ((_n["fundamental_score"] >= 35) & (_n["dividend_yield"] >= 0.06))
                | _n["fundamental_score"].rank(ascending=False, method="min").le(5)
            )
            _fig = go.Figure(go.Scatter(
                x=_n["fundamental_score"], y=_n["dividend_yield"] * 100,
                mode="markers+text",
                text=[nom if garde else "" for nom, garde
                      in zip(_n["name"], _notables)],
                customdata=_n["name"],
                textposition="top center",
                textfont=dict(size=10, color=COLORS["text"]),
                marker=dict(
                    size=_ech.clip(lower=_ech.max() * 0.02 if _ech.max() else 1),
                    sizemode="area", sizeref=_sizeref, sizemin=6,
                    color=_n["dividend_yield"].apply(
                        lambda v: COLORS["green"] if v and v >= 0.06
                        else COLORS["yellow"]),
                    opacity=0.8, line=dict(color="#FFFFFF", width=1.5)),
                hovertemplate="%{customdata}<br>Score %{x:.0f}/50 · "
                              "Yield %{y:.2f} %<extra></extra>",
            ))
            _fig.add_hline(y=6, line=dict(color=COLORS["secondary"], width=1,
                                          dash="dash"))
            _fig.add_vline(x=35, line=dict(color=COLORS["secondary"], width=1,
                                           dash="dash"))
            _fig.update_layout(
                height=340, margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=COLORS["bg"],
                xaxis=dict(title="Score fondamental →",
                           gridcolor=COLORS["border"], zeroline=False),
                yaxis=dict(title="↑ Dividend Yield (%)",
                           gridcolor=COLORS["border"], zeroline=False),
                showlegend=False, font=dict(color=COLORS["text"], size=11),
            )
            st.plotly_chart(_fig, use_container_width=True,
                            key="screen_nuage")
            st.caption(
                "Le trait horizontal marque la cible de 6 % de rendement, le "
                "vertical un score de 35/50. En haut à droite : les deux à la "
                "fois. L'aire du disque est le montant échangé par mois. "
                "Seuls les titres qui tiennent les deux repères, et les cinq "
                "meilleurs scores, sont nommés — le survol nomme les autres."
            )

        # Rendu HTML éditorial avec colonnes : Ticker / Nom / Secteur / Prix /
        # Yield / PER / ROE / Payout / Score / Verdict (tag coloré)
        def _verdict_tag(verdict):
            if not verdict:
                return "<span class='muted'>—</span>"
            v = verdict.upper()
            if "ACHAT FORT" in v:
                return "<span class='tag up' style='text-transform:none;'>ACHAT FORT</span>"
            if "ACHAT" in v or "ACHETER" in v:
                return "<span class='tag up' style='text-transform:none;'>ACHETER</span>"
            if "CONSERVER" in v:
                return "<span class='tag ocre' style='text-transform:none;'>CONSERVER</span>"
            if "PRUDENCE" in v:
                return "<span class='tag ocre' style='text-transform:none;'>PRUDENCE</span>"
            if "VENTE" in v or "EVITER" in v or "ÉVITER" in v:
                return "<span class='tag down' style='text-transform:none;'>ÉVITER</span>"
            return f"<span class='tag neutral'>{verdict}</span>"

        def _fmt_pct(v, digits=2):
            if v is None or pd.isna(v) or not v:
                return "—"
            return f"{v*100:.{digits}f}%"

        def _fmt_dec(v, digits=1):
            if v is None or pd.isna(v) or not v:
                return "—"
            return f"{v:.{digits}f}"

        def _fmt_int(v):
            if v is None or pd.isna(v) or not v:
                return "—"
            return f"{v:,.0f}"

        def _fmt_echange(v):
            """Un montant echange se lit en milliards ou en millions, jamais en
            francs : la colonne servirait a compter des zeros."""
            if v is None or pd.isna(v) or not v:
                return "—"
            return f"{v / 1e9:.1f} Md" if v >= 1e9 else f"{v / 1e6:.0f} M"

        header_style = (
            "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
            "color:var(--ink-3);font-weight:500;padding:10px;"
            "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
        )
        cell_style = "padding:10px;font-size:13px;border-bottom:1px solid var(--border-soft);"
        num_style = cell_style + "text-align:right;font-variant-numeric:tabular-nums;"

        rows_html = (
            f"<tr>"
            f"<th style='{header_style};text-align:left;'>Ticker</th>"
            f"<th style='{header_style};text-align:left;'>Nom</th>"
            f"<th style='{header_style};text-align:left;'>Secteur</th>"
            f"<th style='{header_style};text-align:right;'>Prix</th>"
            f"<th style='{header_style};text-align:right;'>Yield</th>"
            f"<th style='{header_style};text-align:right;'>PER</th>"
            f"<th style='{header_style};text-align:right;'>ROE</th>"
            f"<th style='{header_style};text-align:right;'>Payout</th>"
            f"<th style='{header_style};text-align:right;'>Volat.</th>"
            f"<th style='{header_style};text-align:right;'>Rdt/an</th>"
            f"<th style='{header_style};text-align:right;'>Échangé</th>"
            f"<th style='{header_style};text-align:right;'>Score</th>"
            f"<th style='{header_style};text-align:left;'>Verdict</th>"
            f"</tr>"
        )
        for _, r in filtered.iterrows():
            score = r.get("fundamental_score")
            score_str = f"{score:.0f}/50" if score is not None else "—"
            rows_html += (
                f"<tr>"
                f"<td style='{cell_style}'><span class='ticker'>{r['ticker']}</span></td>"
                f"<td style='{cell_style};font-weight:500;'>{r['name']}</td>"
                f"<td style='{cell_style};color:var(--ink-3);'>{r['sector']}</td>"
                f"<td style='{num_style}'>{_fmt_int(r['price'])}</td>"
                f"<td style='{num_style}'>{_fmt_pct(r['dividend_yield'])}</td>"
                f"<td style='{num_style}'>{_fmt_dec(r['per'])}</td>"
                f"<td style='{num_style}'>{_fmt_pct(r['roe'], 1)}</td>"
                f"<td style='{num_style}'>{_fmt_pct(r['payout_ratio'], 0)}</td>"
                f"<td style='{num_style}'>{_fmt_pct(r.get('volatilite'), 0)}</td>"
                f"<td style='{num_style}'>"
                f"{_fmt_pct(r.get('rendement_annualise'), 0)}</td>"
                f"<td style='{num_style}'>{_fmt_echange(r.get('montant_echange'))}</td>"
                f"<td style='{num_style};font-weight:600;'>{score_str}</td>"
                f"<td style='{cell_style}'>{_verdict_tag(r.get('verdict'))}</td>"
                f"</tr>"
            )

        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:12px;"
            f"overflow:hidden;background:var(--bg-elev);margin-bottom:16px;'>"
            f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table>"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Quick jump + export
        col_nav, col_csv = st.columns([3, 1])
        with col_nav:
            picker_options = [
                (row["ticker"], f"{row['ticker']} — {row['name']}")
                for _, row in filtered.iterrows()
            ]
            ticker_quick_picker(picker_options, key="screen_goto",
                                 label="Ouvrir l'analyse d'un titre")
        with col_csv:
            csv = filtered[["ticker", "name", "sector", "price", "dividend_yield",
                            "per", "roe", "payout_ratio", "debt_equity",
                            "volatilite", "rendement_annualise", "montant_echange",
                            "fundamental_score", "verdict"]].to_csv(index=False)
            st.download_button("Exporter CSV", csv, "brvm_screening.csv", "text/csv",
                               use_container_width=True)
