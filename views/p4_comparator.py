"""
Page 4 : Comparateur sectoriel
Choisir un secteur → titres auto-proposes pour comparaison.
"""

import streamlit as st
import pandas as pd
import numpy as np

from config import load_tickers
from data.storage import (
    get_fundamentals, get_cached_prices, list_tickers_with_fundamentals,
    get_all_stocks_for_analysis, get_analyzable_tickers,
)
from analysis.fundamental import compute_ratios, format_ratio
from utils.nav import ticker_analyze_button
from utils.charts import radar_chart, performance_chart, COLORS
from utils.ui_helpers import section_heading


def render():
    # Hiérarchie v3 : Title + caption → sélecteur → tableau → charts
    st.title("Comparateur")
    st.caption("Comparer les titres d'un secteur, ou une sélection libre "
               "de 2 à 5 titres.")

    analyzable = get_analyzable_tickers()
    all_stocks = get_all_stocks_for_analysis()

    if not analyzable:
        st.warning("Aucune donnée disponible.")
        return

    # Boutons segmentés plutôt que radio : le canevas montre une bascule
    # à deux positions, pas une liste à cocher.
    mode = st.segmented_control(
        "Mode", ["Par secteur", "Sélection libre"],
        default="Par secteur", key="cmp_mode") or "Par secteur"

    if mode == "Par secteur":
        sectors = sorted(set(t["sector"] for t in analyzable if t.get("sector")))
        selected_sector = st.selectbox("Secteur", sectors)
        sector_tickers = [t for t in analyzable if t["sector"] == selected_sector]
        st.caption(f"{len(sector_tickers)} titres avec données · secteur {selected_sector}")

        options = [f"{t['ticker']} · {t['name']}" for t in sector_tickers]
        selected = st.multiselect(
            "Titres à comparer", options,
            default=options[:min(5, len(options))],
        )
        tickers = [s.split(" · ")[0] for s in selected]
    else:
        all_options = [f"{t['ticker']} · {t['name']}" for t in analyzable]
        selected = st.multiselect("Sélectionnez 2 à 5 titres", all_options, default=[])
        tickers = [s.split(" · ")[0] for s in selected]

    if len(tickers) < 2:
        st.info("Sélectionnez au moins 2 titres pour comparer.")
        return

    # Load data for selected tickers — depuis all_stocks (1 requête cachée)
    # au lieu de N appels get_fundamentals (N round-trips Supabase).
    import math as _m
    stocks = {}
    if not all_stocks.empty:
        stocks_by_ticker = {r["ticker"]: r.to_dict() for _, r in all_stocks.iterrows()}
    else:
        stocks_by_ticker = {}
    for ticker in tickers:
        data = stocks_by_ticker.get(ticker)
        if not data:
            continue
        data = {k: (None if isinstance(v, float) and _m.isnan(v) else v)
                for k, v in data.items()}
        if data.get("price"):
            ratios = compute_ratios(data)
            stocks[ticker] = {"fundamentals": data, "ratios": ratios}

    if len(stocks) < 2:
        st.warning("Données insuffisantes pour au moins 2 titres. Importez des données fondamentales.")
        return

    onglet_tableau, onglet_profil, onglet_perf = st.tabs(
        ["Tableau comparatif", "Profil comparatif", "Performance comparée"])

    with onglet_tableau:
        # --- Tableau comparatif ---

        metrics = [
            ("Prix (FCFA)",       "price",              "number"),
            ("ROE",               "roe",                "pct"),
            ("Marge nette",       "net_margin",         "pct"),
            ("PER",               "per",                "decimal"),
            ("Dividend Yield",    "dividend_yield",     "pct"),
            ("Payout ratio",      "payout_ratio",       "pct"),
            ("Dette/Equity",      "debt_equity",        "x"),
            ("P/B",               "pb",                 "x"),
            ("EPS (FCFA)",        "eps",                "number"),
            ("DPS (FCFA)",        "dps",                "number"),
            ("Checklist V&D",     "_checklist",         "text"),
            ("Score fondamental", "fundamental_score",  "decimal"),
        ]

        comp_data = {"Indicateur": [m[0] for m in metrics]}
        for ticker, data in stocks.items():
            name = data["fundamentals"].get("company_name") or ticker
            values = []
            for _, key, fmt in metrics:
                if key == "price":
                    val = data["fundamentals"].get("price")
                    values.append(format_ratio(val, fmt))
                elif key == "_checklist":
                    cl = data["ratios"].get("checklist", [])
                    passed = sum(1 for c in cl if c["passed"] is True)
                    total = len(cl)
                    values.append(f"{passed} / {total}" if total else "—")
                elif key == "fundamental_score":
                    val = data["ratios"].get("fundamental_score")
                    values.append(format_ratio(val, fmt))
                else:
                    val = data["ratios"].get(key)
                    values.append(format_ratio(val, fmt))
            comp_data[f"{name} · {ticker}"] = values

        # ── Rendu au modèle du canevas : le meilleur de chaque ligne ressort ──
        # `st.dataframe` alignait douze lignes de chiffres tous de la même
        # couleur : pour savoir qui gagne sur le ROE, il fallait comparer à
        # l'œil, ligne par ligne. Le canevas met le meilleur en gras et en
        # encre pleine, les autres en gris. La question « lequel est le
        # meilleur sur ce critère » se lit alors sans calcul.
        #
        # Le sens du « meilleur » dépend du critère : un PER ou un ratio
        # d'endettement bas vaut mieux qu'un haut. Les critères sans ordre
        # (prix, EPS, DPS) n'ont pas de gagnant et restent neutres.
        PREFERE_BAS = {"per", "debt_equity", "pb", "payout_ratio"}
        SANS_ORDRE = {"price", "eps", "dps"}
        _colonnes = [c for c in comp_data if c != "Indicateur"]

        def _brut(ticker_col, cle):
            """Valeur numérique brute, pour comparer sans passer par le texte."""
            _t = ticker_col.rsplit(" · ", 1)[-1]
            _d = stocks.get(_t)
            if not _d:
                return None
            if cle == "price":
                return _d["fundamentals"].get("price")
            if cle == "_checklist":
                _cl = _d["ratios"].get("checklist", [])
                return sum(1 for c in _cl if c["passed"] is True) if _cl else None
            return _d["ratios"].get(cle)

        _th = ("font-size:10px;text-transform:uppercase;letter-spacing:0.09em;"
               "color:var(--ink-3);font-weight:600;padding:9px 12px;"
               "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
               "white-space:nowrap;")
        _td = ("padding:10px 12px;border-bottom:1px solid var(--border-soft);"
               "font-size:13px;")
        _tdn = (_td + "text-align:right;font-variant-numeric:tabular-nums;"
                "white-space:nowrap;")

        _html = (f"<tr><th style='{_th}text-align:left;'>Indicateur</th>"
                 + "".join(f"<th style='{_th}text-align:right;'>{c}</th>"
                           for c in _colonnes) + "</tr>")
        for _i, (_lib, _cle, _fmt) in enumerate(metrics):
            _bruts = {c: _brut(c, _cle) for c in _colonnes}
            _valides = {c: v for c, v in _bruts.items()
                        if v is not None and not pd.isna(v)}
            _gagnant = None
            if _cle not in SANS_ORDRE and len(_valides) > 1:
                _gagnant = (min(_valides, key=_valides.get)
                            if _cle in PREFERE_BAS
                            else max(_valides, key=_valides.get))
            _html += f"<tr><td style='{_td}font-weight:500;'>{_lib}</td>"
            for _c in _colonnes:
                _txt = comp_data[_c][_i]
                _meilleur = _c == _gagnant
                _html += (
                    f"<td style='{_tdn}"
                    + ("font-weight:600;color:var(--ink);"
                       if _meilleur else "color:var(--ink-3);")
                    + f"'>{_txt}</td>"
                )
            _html += "</tr>"

        st.markdown(
            "<div style='background:var(--bg-elev);border:1px solid var(--border);"
            "border-radius:12px;overflow:hidden;'>"
            "<div style='overflow-x:auto;'>"
            "<table style='width:100%;border-collapse:collapse;'>"
            f"{_html}</table></div>"
            "<div style='padding:10px 12px;background:var(--bg-footer);"
            "font-size:11.5px;color:var(--ink-3);'>"
            "La meilleure valeur de chaque ligne est en gras. Pour le PER, le "
            "payout, la dette et le P/B, « meilleur » veut dire plus bas. "
            "Prix, EPS et DPS n'ont pas de gagnant : ils dépendent du nombre "
            "d'actions, pas de la qualité.</div></div>",
            unsafe_allow_html=True,
        )

        # Boutons "Ouvrir" pour chaque ticker sélectionné
        if tickers:
            btn_cols = st.columns(len(tickers))
            for i, ticker in enumerate(tickers):
                with btn_cols[i]:
                    ticker_analyze_button(
                        ticker, label=ticker,
                        key=f"cmp_goto_{ticker}", use_container_width=True,
                    )


    with onglet_profil:
        # --- Bar chart horizontal monochrome (remplace le radar arc-en-ciel) ---
        # Principe design v3 #07 : dataviz monochrome. Bar horizontal groupé
        # est plus lisible que le radar pour des valeurs chiffrées.

        import plotly.graph_objects as go
        def _borne(v):
            """Un score sur cent ne descend pas sous zéro.

            Sans borne basse, un ROE négatif sortait à −154 : absurde sur une
            échelle 0-100, et franchement trompeur dans la carte de chaleur,
            dont l'intensité suit la valeur ABSOLUE — la pire société y
            paraissait la plus forte.
            """
            return max(0.0, min(100.0, float(v)))

        bar_metrics = [
            ("ROE", lambda r: _borne((r.get("roe") or 0) / 0.30 * 100)),
            ("Marge", lambda r: _borne((r.get("net_margin") or 0) / 0.25 * 100)),
            ("Yield", lambda r: _borne((r.get("dividend_yield") or 0) / 0.10 * 100)),
            ("Valorisation", lambda r: _borne((20 - (r.get("per") or 0)) / 20 * 100)
                              if (r.get("per") or 0) > 0 else 0.0),
            ("Croissance", lambda r: _borne((r.get("revenue_growth") or 0) / 0.15 * 100)),
            ("Score global", lambda r: _borne((r.get("fundamental_score") or 0) / 50 * 100)),
        ]
        # Palette monochrome (design v3) — deep green + accent ocre + neutre
        mono_palette = [
            COLORS["primary"], COLORS["accent"], COLORS["secondary"],
            "#4A8A5F", "#D97E4F", "#A69D8D",  # variantes
        ]

        # AXES PARALLELES, PAS BARRES GROUPEES. Les barres groupees repondent
        # « combien vaut ce titre sur ce critere », un critere a la fois. Le
        # canevas trace une LIGNE par titre a travers les six axes : un profil
        # se lit alors a sa forme, et surtout les lignes SE CROISENT — le
        # croisement est l'information, il montre l'axe ou l'ordre s'inverse,
        # c'est-a-dire l'endroit ou le choix se joue. Des barres groupees ne
        # peuvent pas le montrer : elles rangent chaque critere a part.
        # Le bloc voisin porte son titre ; celui-ci n'en avait pas, et la
        # figure ouvrait l'onglet sans dire ce qu'elle montrait.
        st.markdown(
            "<div style='display:flex;align-items:baseline;gap:10px;"
            "margin:2px 0 10px;'>"
            "<h2 style='font-size:17px;font-weight:600;margin:0;"
            "letter-spacing:-0.015em;'>Profil comparatif · axes parallèles</h2>"
            "<span style='font-family:var(--font-mono);font-size:11.5px;"
            "color:var(--ink-3);'>SCORE 0-100</span></div>",
            unsafe_allow_html=True,
        )

        _axes = [m[0] for m in bar_metrics]
        _profils = {}
        fig = go.Figure()
        for i, (ticker, data) in enumerate(stocks.items()):
            r = data["ratios"]
            name = data["fundamentals"].get("company_name") or ticker
            values = [fn(r) for _, fn in bar_metrics]
            _profils[name] = values
            fig.add_trace(go.Scatter(
                x=_axes, y=values, name=name,
                mode="lines+markers",
                line=dict(color=mono_palette[i % len(mono_palette)], width=2.5),
                marker=dict(size=7,
                            color=mono_palette[i % len(mono_palette)]),
                hovertemplate="%{x} · %{y:.0f}/100<extra>" + name + "</extra>",
            ))
        fig.update_layout(
            height=380,
            template="plotly_white",
            paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
            font=dict(color=COLORS["text"],
                      family="ui-sans-serif, -apple-system, sans-serif", size=12),
            # Une verticale par axe : c'est ce qui fait lire la figure comme
            # des axes paralleles plutot que comme une courbe dans le temps.
            xaxis=dict(showgrid=True, gridcolor=COLORS["border"], gridwidth=1,
                       showline=False, ticks="", type="category"),
            yaxis=dict(title="Score (0-100)", range=[0, 100],
                       gridcolor=COLORS["border"], zeroline=False),
            margin=dict(l=10, r=10, t=10, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1,
                        font=dict(size=11, color=COLORS["text_secondary"])),
        )
        st.plotly_chart(fig, use_container_width=True)

        # OU LES LIGNES SE CROISENT. Le canevas le dit en toutes lettres ;
        # ici on le CALCULE, plutot que de recopier son exemple : entre deux
        # axes voisins, deux titres se croisent quand l'ordre s'inverse de
        # l'un a l'autre.
        #
        # Encore faut-il que ce soit une information. Sur cinq titres, il se
        # croise quelque chose entre chaque paire d'axes : nommer les cinq
        # intervalles ne dit plus rien, et la phrase « ailleurs le classement
        # ne change pas » designe un ailleurs vide. La lecture depend donc du
        # NOMBRE de croisements — ce qui est rare est ce qui informe, que ce
        # soient les croisements ou les stabilites.
        _intervalles = [f"{_axes[_i]} et {_axes[_i + 1]}"
                        for _i in range(len(_axes) - 1)]
        _croise = set()
        _noms_profils = list(_profils)
        for _a in range(len(_noms_profils)):
            for _b in range(_a + 1, len(_noms_profils)):
                _va, _vb = _profils[_noms_profils[_a]], _profils[_noms_profils[_b]]
                for _i in range(len(_axes) - 1):
                    if (_va[_i] - _vb[_i]) * (_va[_i + 1] - _vb[_i + 1]) < 0:
                        _croise.add(_i)
        # Dans l'ordre des axes, jamais alphabetique : on suit la figure.
        _ou_croise = [_intervalles[_i] for _i in sorted(_croise)]
        _ou_stable = [l for _i, l in enumerate(_intervalles) if _i not in _croise]

        _tete = ("Six axes, une ligne par titre : un profil se lit à sa forme, "
                 "et plus la ligne est haute, meilleur est le score sur l'axe. ")
        _et = lambda liste: "<b>" + "</b>, <b>".join(liste[:-1]) + \
                            ("</b> et <b>" if len(liste) > 1 else "") + \
                            liste[-1] + "</b>"
        if len(_profils) < 2:
            _lecture = ("Un seul titre mesurable : la ligne donne son profil, "
                        "sans comparaison possible.")
        elif not _ou_croise:
            _lecture = _tete + (
                "<b>Aucune ligne n'en croise une autre</b> : le classement est "
                "le même sur les six axes, et le choix ne dépend d'aucun "
                "arbitrage.")
        elif len(_ou_croise) <= len(_intervalles) / 2:
            _lecture = _tete + (
                f"Les lignes se croisent entre {_et(_ou_croise)} — c'est là que "
                f"l'ordre s'inverse, donc là que le choix se joue. Ailleurs, le "
                f"classement ne change pas.")
        elif _ou_stable:
            _lecture = _tete + (
                f"Les lignes se croisent presque partout : le classement ne "
                f"tient qu'entre {_et(_ou_stable)}. <b>Aucun titre ne domine "
                f"les six axes</b> — le choix est un arbitrage, pas un "
                f"classement.")
        else:
            _lecture = _tete + (
                f"Les lignes se croisent sur les {len(_intervalles)} "
                f"intervalles : <b>aucun titre ne domine</b>, et l'ordre change "
                f"d'un axe au suivant. Le choix se fait en décidant quels axes "
                f"comptent, pas en lisant un classement.")
        from utils.ui_helpers import note as _note
        _note("", _lecture, ton="primary")

        # ── Le même profil, en intensité ──
        # Le graphique groupé répond « quelle est la forme de ce titre ». La
        # carte de chaleur répond à l'autre question, que le graphique rend
        # laborieuse : sur CE critère, lequel gagne ? Une lecture en ligne,
        # une lecture en colonne, les mêmes six scores.
        from utils.ui_helpers import heatmap
        _noms = [data["fundamentals"].get("company_name") or t
                 for t, data in stocks.items()]
        _lignes = []
        for _lib, _fn in bar_metrics:
            _lignes.append((_lib, [_fn(d["ratios"]) for d in stocks.values()]))
        st.markdown(
            "<div style='display:flex;align-items:baseline;gap:10px;"
            "margin:26px 0 12px;'>"
            "<h2 style='font-size:17px;font-weight:600;margin:0;"
            "letter-spacing:-0.015em;'>Le même profil, en intensité</h2>"
            "<span style='font-family:var(--font-mono);font-size:11.5px;"
            "color:var(--ink-3);'>SCORE 0-100</span></div>",
            unsafe_allow_html=True,
        )
        heatmap(
            _lignes, _noms, echelle=100, mode="intensite",
            intitule_colonne="Critère",
            formatter=lambda v: f"{v:.0f}",
            footer="Lecture en ligne : qui gagne sur ce critère. En colonne : "
                   "le profil d'ensemble d'un titre. Les scores sont bornés à "
                   "[0, 100] : au-delà du seuil du critère la case sature, et "
                   "une valeur négative (ROE ou marge en perte) vaut zéro — "
                   "elle ne se compare pas sur une échelle de mérite.",
        )


    with onglet_perf:
        # --- Performance Chart ---

        price_data = {}
        for ticker in tickers:
            prices = get_cached_prices(ticker)
            if not prices.empty and "close" in prices.columns:
                price_data[ticker] = prices.set_index("date")["close"]

        if len(price_data) >= 2:
            fig = performance_chart(price_data, "Performance normalisee (base 100)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas assez de données de prix pour comparer les performances. Chargez les prix depuis la page Analyse.")
