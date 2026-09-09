"""
Page 9 : Performance historique
Classement des titres et secteurs par performance sur différentes périodes.
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import plotly.graph_objects as go
import plotly.express as px

from config import load_tickers
from data.storage import get_cached_prices
from data.db import read_sql_df
from utils.charts import COLORS
from utils.nav import ticker_quick_picker
from utils.auth import is_admin


# ── Period definitions ──

PERIODS = {
    "3M": timedelta(days=90),
    "6M": timedelta(days=182),
    "1A": timedelta(days=365),
    "2A": timedelta(days=730),
    "3A": timedelta(days=1095),
    "Max": None,
}


def _compute_performance(df: pd.DataFrame, cutoff_date):
    """Return % change from cutoff_date to latest close."""
    if df.empty or "close" not in df.columns:
        return None
    df_sorted = df.sort_values("date")
    if cutoff_date is not None:
        df_period = df_sorted[df_sorted["date"] >= pd.Timestamp(cutoff_date)]
    else:
        df_period = df_sorted
    if len(df_period) < 2:
        return None
    start_price = df_period.iloc[0]["close"]
    end_price = df_period.iloc[-1]["close"]
    if not start_price or start_price == 0:
        return None
    return (end_price - start_price) / start_price


@st.cache_data(ttl=300, show_spinner=False)
def _load_all_performances_from_snapshot() -> pd.DataFrame:
    """Lit les performances précalculées depuis ticker_performance_snapshot.
    Retourne un DataFrame avec les mêmes colonnes que l'ancien _load_all_performances
    (3M, 6M, 1A, 2A, 3A, Max) pour compatibilité avec le reste de la page."""
    try:
        df = read_sql_df(
            "SELECT ticker, company_name AS name, sector, last_price AS price, "
            "perf_1m, perf_3m, perf_6m, perf_1a, perf_2a, perf_3a, perf_max "
            "FROM ticker_performance_snapshot"
        )
    except Exception:
        return pd.DataFrame()

    # Map vers les noms de colonnes attendus par la page (3M/6M/1A/2A/3A/Max)
    rename = {
        "perf_3m": "3M", "perf_6m": "6M", "perf_1a": "1A",
        "perf_2a": "2A", "perf_3a": "3A", "perf_max": "Max",
    }
    df = df.rename(columns=rename)
    return df


def _load_all_performances_live() -> pd.DataFrame:
    """Fallback : calcul live (lent) utilisé si le snapshot est vide."""
    tickers_data = load_tickers()
    today = datetime.now().date()
    rows = []

    for t in tickers_data:
        ticker = t["ticker"]
        name = t.get("name", ticker)
        sector = t.get("sector", "")
        df = get_cached_prices(ticker)
        if df.empty:
            continue

        last_price = df.sort_values("date").iloc[-1]["close"]
        row = {
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "price": last_price,
        }

        for label, delta in PERIODS.items():
            cutoff = (today - delta) if delta else None
            row[label] = _compute_performance(df, cutoff)

        rows.append(row)

    return pd.DataFrame(rows)


def _load_all_performances() -> tuple:
    """Retourne (df, from_snapshot: bool)."""
    df = _load_all_performances_from_snapshot()
    if df.empty:
        return _load_all_performances_live(), False
    return df, True


def _format_pct(val):
    if val is None or pd.isna(val):
        return "—"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.1%}"


def _color_pct(val):
    if val is None or pd.isna(val):
        return "color: #8F9BBA"
    return f"color: {COLORS['green']}" if val >= 0 else f"color: {COLORS['red']}"


def render():
    from utils.ui_helpers import section_heading
    st.title("Performance des Titres")

    perf_df, from_snapshot = _load_all_performances()
    if perf_df.empty:
        st.warning("Aucune donnée de prix disponible.")
        return

    # UN TITRE RETIRE DE LA COTE N'A PLUS DE PERFORMANCE A CLASSER. Le retrait
    # de SVOC.ci a ete « diffuse partout » le 05/09, mais pas ici : il figurait
    # encore au classement, et le snapshot le porte sans secteur ni nom — d'ou
    # une ligne sans intitule dans le tableau sectoriel. Le tableau de bord
    # applique deja ce filtre ; cette page le rejoint.
    from config import tickers_retires
    _hors_cote = tickers_retires()
    if _hors_cote:
        perf_df = perf_df[~perf_df["ticker"].isin(_hors_cote)].copy()

    # Ceinture et bretelles : un secteur vide ne fait pas une ligne anonyme.
    if "sector" in perf_df.columns:
        perf_df["sector"] = (perf_df["sector"].fillna("").astype(str).str.strip()
                             .replace("", "Non classé"))

    # ─── Ligne titre : caption gauche + period pills droite ───
    col_sub, col_period = st.columns([3, 2])

    with col_period:
        _periodes = list(PERIODS.keys())
        period = st.segmented_control(
            "Période", _periodes, default=_periodes[2],
            label_visibility="collapsed", key="perf_periode",
        ) or _periodes[2]

    col_perf = period
    valid = perf_df.dropna(subset=[col_perf]).copy()
    if valid.empty:
        st.warning(f"Pas assez de données historiques pour la période {period}.")
        return
    valid_sorted = valid.sort_values(col_perf, ascending=False)

    with col_sub:
        st.caption(f"Classement sur {period.lower()} · {len(valid)} titres "
                   f"avec historique suffisant.")

    if not from_snapshot and is_admin():
        st.caption("Snapshot vide — « Regénérer snapshots » dans la barre latérale.")

    # ─── 4 KPI cards ──────────────────────────────────────────────────
    best = valid_sorted.iloc[0]
    worst = valid_sorted.iloc[-1]
    median_perf = valid[col_perf].median()
    positive_count = (valid[col_perf] >= 0).sum()
    total = len(valid)
    pct_market = positive_count / total * 100 if total else 0

    def _kpi(label, value, sub, arrow_tone="neutral"):
        """Carte au gabarit du canevas : filet superieur colore, valeur en
        19 px quand c'est un NOM (les deux extremes) plutot qu'un nombre —
        « Coris Bank International » en 27 px deborderait."""
        arrow = {"up": "▲", "down": "▼"}.get(arrow_tone, "")
        accent = {"up": "var(--up)", "down": "var(--down)"}.get(
            arrow_tone, "var(--ink-4)")
        teinte = {"up": "var(--up)", "down": "var(--down)"}.get(
            arrow_tone, "var(--ink-3)")
        nombre = str(value).replace(" ", "").replace("+", "").replace("-", "")
        taille = "27px" if nombre[:1].isdigit() else "19px"
        return (
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-top:2px solid {accent};border-radius:12px;padding:15px 17px;"
            f"display:flex;flex-direction:column;gap:5px;height:100%;'>"
            f"<span style='font-size:10.5px;font-weight:600;letter-spacing:0.09em;"
            f"text-transform:uppercase;color:var(--ink-3);'>{label}</span>"
            f"<span style='font-variant-numeric:tabular-nums;font-size:{taille};"
            f"font-weight:600;letter-spacing:-0.015em;line-height:1.15;"
            f"white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
            f"color:var(--ink);'>{value}</span>"
            f"<span style='font-size:11.5px;font-weight:600;color:{teinte};'>"
            f"{arrow + ' ' if arrow else ''}{sub}</span></div>"
        )

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(_kpi("Meilleure perf.", best["name"],
                          _format_pct(best[col_perf]), "up"),
                     unsafe_allow_html=True)
    with k2:
        st.markdown(_kpi("Pire perf.", worst["name"],
                          _format_pct(worst[col_perf]), "down"),
                     unsafe_allow_html=True)
    with k3:
        st.markdown(_kpi("Perf. médiane", _format_pct(median_perf),
                          f"{total} titres", "neutral"),
                     unsafe_allow_html=True)
    with k4:
        st.markdown(_kpi("Titres en hausse", f"{positive_count} / {total}",
                          f"{pct_market:.0f}% du marché",
                          "up" if pct_market >= 50 else "down"),
                     unsafe_allow_html=True)

    # ─── Tabs ─────────────────────────────────────────────────────────
    tab_stocks, tab_sectors, tab_chart, tab_multi = st.tabs(
        ["Classement", "Par secteur", "Graphique", "Tableau multi-périodes"]
    )

    # ───────── TAB 1 : Classement avec bars horizontaux Top / Pires ──
    with tab_stocks:
        n_show = st.slider("Nombre de titres affichés", 5, len(valid_sorted),
                           min(8, len(valid_sorted)), key="p9_n_show",
                           label_visibility="collapsed")

        col_top, col_bot = st.columns(2)

        # UNE BARRE SIGNEE, ET UNE SEULE ECHELLE. L'ancienne barre valait
        # `abs(valeur) / max` et prenait la couleur de la LISTE : dans « Pires
        # performers », un titre qui avait gagne 13,5 % recevait une barre
        # rouge plus longue que celle d'un titre ayant perdu 5,1 %. La barre
        # se lisait comme une baisse et n'en etait pas une.
        #
        # Desormais la barre part d'un axe central, va a droite si la valeur
        # est positive, et prend la couleur du SIGNE. L'echelle est celle du
        # classement entier, la meme des deux cotes : les deux listes se
        # comparent, ce qu'elles ne faisaient pas quand chacune se normalisait
        # sur son propre maximum.
        from utils.ui_helpers import barre_signee
        _echelle = (max(abs(v) for v in valid_sorted[col_perf]) * 100
                    if not valid_sorted.empty else 1.0) or 1.0

        def _hbar_list(df_subset, title, dot_class):
            """Nom, barre signee sur l'echelle du classement, valeur a droite."""
            inner = ""
            for _, r in df_subset.iterrows():
                v = r[col_perf]
                pct = v * 100
                teinte = "var(--up)" if v >= 0 else "var(--down)"
                sign = "+" if v >= 0 else ""
                inner += (
                    f"<div style='display:flex;align-items:center;gap:10px;"
                    f"padding:7px 0;border-bottom:1px solid var(--border-soft);font-size:13px;'>"
                    f"<div style='min-width:100px;color:var(--ink);font-weight:500;'>{r['name']}</div>"
                    f"<div style='flex:1;'>"
                    + barre_signee(pct, borne=_echelle, mini="60px",
                                   legende=f"{sign}{pct:.1f} %")
                    + f"</div>"
                    f"<div style='min-width:65px;text-align:right;color:{teinte};"
                    f"font-weight:600;font-variant-numeric:tabular-nums;'>"
                    f"{sign}{pct:.1f}%</div>"
                    f"</div>"
                )
            st.markdown(
                f"<div style='font-size:14px;font-weight:600;color:var(--ink);"
                f"margin-bottom:10px;'><span class='dot {dot_class}'></span>{title}</div>"
                f"<div>{inner}</div>",
                unsafe_allow_html=True,
            )

        with col_top:
            _hbar_list(valid_sorted.head(n_show), "Top performers", "up")
        with col_bot:
            _hbar_list(valid_sorted.tail(n_show).sort_values(col_perf),
                       "Pires performers", "down")

        st.caption(
            f"Barre signée : à droite de l'axe si la performance est positive, "
            f"à gauche sinon, et la couleur suit le signe — non la liste. "
            f"Échelle commune aux deux colonnes, ±{_echelle:.0f} %, "
            f"le maximum du classement. **Les « pires » sont les derniers du "
            f"classement, pas nécessairement des baisses** : quand le marché "
            f"monte, cette colonne contient des hausses."
        )

    # ───────── TAB 2: By sector ─────────
    with tab_sectors:
        # ── Secteurs × périodes ──
        # Le graphique en barres ne montre qu'UNE fenêtre à la fois : on y
        # lit quel secteur mène aujourd'hui, jamais s'il mène depuis
        # longtemps. La carte de chaleur met les six fenêtres côte à côte —
        # en ligne, la persistance d'un secteur ; en colonne, ce que la
        # période favorise.
        from utils.ui_helpers import heatmap as _heatmap
        _fenetres = [p for p in PERIODS if p in perf_df.columns]
        if _fenetres and "sector" in perf_df.columns:
            # dropna ne suffit pas : un secteur vide est une chaîne vide,
            # pas un NaN, et sortait en ligne fantôme à +0,00 %.
            _av_sect = perf_df.dropna(subset=["sector"]).copy()
            _av_sect = _av_sect[_av_sect["sector"].astype(str).str.strip() != ""]
            _par_secteur = _av_sect.groupby("sector")[_fenetres].mean() * 100
            if not _par_secteur.empty:
                _derniere = _fenetres[-1]
                _par_secteur = _par_secteur.sort_values(_derniere,
                                                        ascending=False)
                _lignes_h = [(nom, [None if pd.isna(v) else float(v)
                                    for v in ligne])
                             for nom, ligne in _par_secteur.iterrows()]
                st.markdown(
                    "<div style='display:flex;align-items:baseline;gap:10px;"
                    "margin:6px 0 12px;'>"
                    "<h2 style='font-size:17px;font-weight:600;margin:0;"
                    "letter-spacing:-0.015em;'>Secteurs × périodes</h2>"
                    "<span style='font-family:var(--font-mono);"
                    "font-size:11.5px;color:var(--ink-3);'>"
                    "MOYENNE PAR SECTEUR</span></div>",
                    unsafe_allow_html=True,
                )
                _heatmap(
                    _lignes_h, _fenetres,
                    footer="Moyenne simple des titres du secteur, non "
                           "pondérée. Lecture en ligne : la persistance d'un "
                           "secteur. En colonne : ce que la période favorise. "
                           "Une case vide est une fenêtre sans historique "
                           "suffisant. La teinte sature au 85e centile : "
                           "sans cela, la colonne la plus large délaverait "
                           "toutes les autres.",
                )

        section_heading(f"Performance sectorielle — {period}", spacing="loose")

        sector_perf = (
            valid.groupby("sector")[col_perf]
            .agg(["mean", "median", "count", "min", "max"])
            .sort_values("mean", ascending=False)
        )

        # UN TABLEAU, PAS UN GRAPHIQUE PUIS LE MEME TABLEAU. L'onglet montrait
        # un graphique en barres de la MOYENNE, puis un tableau brut qui
        # redonnait les memes chiffres — et la moyenne seule ne dit pas si un
        # secteur est homogene ou tire par un titre. Le canevas met une seule
        # table a six colonnes, ou la MEDIANE et l'ETENDUE repondent
        # precisement a cette question. C'est le raisonnement que l'onglet
        # Risque tient deja sur la cote entiere.
        from utils.ui_helpers import barre_signee
        _echelle = (max(abs(v) for v in sector_perf["mean"]) * 100
                    if not sector_perf.empty else 1.0) or 1.0

        _th = ("font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
               "color:var(--ink-3);font-weight:500;padding:9px 12px;"
               "border-bottom:1px solid var(--border);background:var(--bg-sunken);")
        _td = "padding:10px 12px;font-size:13px;border-bottom:1px solid var(--border-soft);"
        _num = _td + "text-align:right;font-variant-numeric:tabular-nums;"

        lignes = (
            f"<tr>"
            f"<th style='{_th};text-align:left;'>Secteur</th>"
            f"<th style='{_th};text-align:right;'>Titres</th>"
            f"<th style='{_th};text-align:right;'>Moyenne</th>"
            f"<th style='{_th};text-align:right;'>Médiane</th>"
            f"<th style='{_th};text-align:left;'>Position</th>"
            f"<th style='{_th};text-align:right;'>Étendue</th>"
            f"</tr>"
        )
        for secteur, r in sector_perf.iterrows():
            _moy, _med = r["mean"] * 100, r["median"] * 100
            _t = "var(--up)" if _moy >= 0 else "var(--down)"
            lignes += (
                f"<tr>"
                f"<td style='{_td};font-weight:600;'>{secteur}</td>"
                f"<td style='{_num};color:var(--ink-3);'>{int(r['count'])}</td>"
                f"<td style='{_num};font-weight:600;color:{_t};'>{_moy:+.1f} %</td>"
                f"<td style='{_num}'>{_med:+.1f} %</td>"
                f"<td style='{_td};min-width:110px;'>"
                + barre_signee(_moy, borne=_echelle, mini="90px",
                               legende=f"moyenne {_moy:+.1f} %") +
                f"</td>"
                f"<td style='{_num};color:var(--ink-3);'>"
                f"{r['min'] * 100:+.0f} / {r['max'] * 100:+.0f}</td>"
                f"</tr>"
            )
        st.markdown(
            f"<div style='border:1px solid var(--border);border-radius:12px;"
            f"overflow:hidden;background:var(--bg-elev);'>"
            f"<table style='width:100%;border-collapse:collapse;'>{lignes}</table></div>",
            unsafe_allow_html=True,
        )

        # L'ECART ENTRE MOYENNE ET MEDIANE EST L'INFORMATION. Il se calcule,
        # plutot que de laisser le lecteur comparer six paires de nombres.
        _tires = [(i, abs(r["mean"] - r["median"]) * 100)
                  for i, r in sector_perf.iterrows()
                  if int(r["count"]) > 1
                  and abs(r["mean"] - r["median"]) * 100 > max(
                      3.0, abs(r["median"] * 100))]
        _phrase = (
            "La **position** situe la moyenne du secteur sur l'échelle commune "
            f"à tous, ±{_echelle:.0f} %. L'**étendue** donne le plus faible et "
            "le plus fort de ses titres : deux secteurs de même moyenne n'ont "
            "pas la même dispersion."
        )
        if _tires:
            _noms = ", ".join(f"**{n}**" for n, _ in sorted(
                _tires, key=lambda x: -x[1])[:3])
            _phrase += (
                f" Moyenne et médiane s'écartent nettement sur {_noms} : "
                f"là, le chiffre d'ensemble tient à un titre ou deux, et "
                f"c'est la médiane qu'il faut lire."
            )
        st.caption(_phrase)

        # Drill-down by sector
        st.markdown("---")
        selected_sector = st.selectbox(
            "Détail par secteur",
            sorted(valid["sector"].unique()),
        )
        sector_stocks = valid_sorted[valid_sorted["sector"] == selected_sector]
        if not sector_stocks.empty:
            fig_drill = _bar_chart(
                sector_stocks, col_perf,
                f"Performance {period} — {selected_sector}",
            )
            st.plotly_chart(fig_drill, use_container_width=True)
        else:
            st.info(f"Aucun titre avec données dans le secteur {selected_sector}.")

    # ───────── TAB 3: Price chart ─────────
    with tab_chart:
        st.subheader("Évolution comparée des prix (base 100)")

        # Let user pick tickers to compare
        options = [f"{r['ticker']} - {r['name']}" for _, r in valid_sorted.iterrows()]
        # Default: top 3 + bottom 1
        defaults = options[:3] + options[-1:] if len(options) >= 4 else options[:3]
        selected = st.multiselect(
            "Choisir des titres à comparer",
            options,
            default=defaults,
            max_selections=10,
        )

        if selected:
            sel_tickers = [s.split(" - ")[0] for s in selected]
            today = datetime.now().date()
            delta = PERIODS[period]
            cutoff = pd.Timestamp(today - delta) if delta else None

            fig_line = go.Figure()
            for ticker in sel_tickers:
                df = get_cached_prices(ticker)
                if df.empty:
                    continue
                df = df.sort_values("date")
                if cutoff is not None:
                    df = df[df["date"] >= cutoff]
                if len(df) < 2:
                    continue
                base = df.iloc[0]["close"]
                if not base or base == 0:
                    continue
                df["indexed"] = (df["close"] / base) * 100
                name = next(
                    (t["name"] for t in load_tickers() if t["ticker"] == ticker),
                    ticker,
                )
                fig_line.add_trace(go.Scatter(
                    x=df["date"], y=df["indexed"],
                    mode="lines", name=name,
                    hovertemplate="%{x|%d %b %Y}<br>%{y:.1f}<extra></extra>",
                ))

            fig_line.add_hline(
                y=100, line_dash="dash",
                line_color=COLORS["text_secondary"],
                annotation_text="Base 100",
            )
            fig_line.update_layout(
                title=f"Performance comparée — {period} (base 100)",
                yaxis_title="Indice (base 100)",
                xaxis_title="",
                plot_bgcolor=COLORS["bg"],
                paper_bgcolor=COLORS["bg"],
                font=dict(color=COLORS["text"]),
                legend=dict(orientation="h", yanchor="bottom", y=-0.25),
                height=500,
                hovermode="x unified",
            )
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("Sélectionnez des titres pour afficher le graphique comparé.")

    # ───────── TAB 4 : Tableau multi-périodes ──────────────
    with tab_multi:
        summary = perf_df[["ticker", "name", "sector", "price"] + list(PERIODS.keys())].copy()
        summary = summary.sort_values(period, ascending=False, na_position="last")
        summary["price_fmt"] = summary["price"].apply(
            lambda x: f"{x:,.0f}" if x and not pd.isna(x) else "—"
        )

        display_cols = {"ticker": "Ticker", "name": "Nom", "sector": "Secteur", "price_fmt": "Prix"}
        for p in PERIODS:
            col_name = f"perf_{p}"
            summary[col_name] = summary[p].apply(_format_pct)
            display_cols[col_name] = p

        st.dataframe(
            summary[list(display_cols.keys())].rename(columns=display_cols),
            use_container_width=True,
            hide_index=True,
            height=600,
        )

        picker_options = [
            (row["ticker"], f"{row['ticker']} — {row['name']}")
            for _, row in summary.iterrows()
        ]
        ticker_quick_picker(picker_options, key="perf_goto",
                             label="Ouvrir l'analyse d'un titre")

    _render_rendement_rapporte_au_risque()
    _render_comparaison_secteurs()


def _render_rendement_rapporte_au_risque():
    """Le classement que la performance brute ne donne pas.

    Les classements par periode disent QUI a le plus monte. Ils ne disent pas
    a quel prix : un titre peut gagner cinquante pour cent en faisant vivre a
    son porteur deux chutes de trente. Ni s'il etait seulement possible d'en
    sortir.

    Cette section repond aux deux questions. Elle porte sur CINQ ANS et sur le
    rendement TOTAL, dividendes compris — ce qui la rend volontairement
    differente des colonnes de periode au-dessus, qui sont des performances de
    cours a court terme. Melanger les deux horizons dans un meme tableau
    donnerait des lignes qui ne se comparent pas.
    """
    from utils.ui_helpers import section_heading
    try:
        from analysis.risque import toutes_les_mesures, formater
        mesures = toutes_les_mesures()
    except Exception as err:                                    # noqa: BLE001
        st.caption(f"Mesures de risque indisponibles : {err}")
        return
    if not mesures:
        return

    noms = {}
    try:
        from data.db import read_sql_df
        table = read_sql_df("SELECT ticker, company_name, sector FROM market_data")
        noms = {r["ticker"]: (r["company_name"], r["sector"])
                for _, r in table.iterrows()}
    except Exception:                                           # noqa: BLE001
        pass

    section_heading("Rendement rapporté au risque · 5 ans", spacing="loose")
    st.caption(
        "Classement par **ratio de Sharpe** : le gain au-delà du taux sans "
        "risque, divisé par l'agitation qu'il a fallu supporter. Rendement "
        "**total, dividendes compris**, sur soixante mois — donc sans rapport "
        "avec les performances de période ci-dessus, qui sont des cours à "
        "court terme. La colonne **Échangé** rappelle qu'un bon classement ne "
        "sert à rien si l'on ne peut pas entrer ni sortir."
    )

    lignes = sorted(
        ((v.get("sharpe"), t, v) for t, v in mesures.items()
         if v.get("sharpe") is not None), reverse=True)

    entete = ("font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;"
              "color:var(--ink-3);font-weight:500;padding:9px 10px;"
              "border-bottom:1px solid var(--border);background:var(--bg-sunken);")
    cell = "padding:8px 10px;font-size:13px;border-bottom:1px solid var(--border);"
    nb = cell + "text-align:right;font-variant-numeric:tabular-nums;"

    html = (f"<tr><th style='{entete};text-align:left;'>#</th>"
            f"<th style='{entete};text-align:left;'>Titre</th>"
            f"<th style='{entete};text-align:right;'>Sharpe</th>"
            f"<th style='{entete};text-align:right;'>Rendement</th>"
            f"<th style='{entete};text-align:right;'>Volatilité</th>"
            f"<th style='{entete};text-align:right;'>Pire chute</th>"
            f"<th style='{entete};text-align:right;'>Échangé/mois</th></tr>")
    for rang, (sharpe, ticker, v) in enumerate(lignes, 1):
        nom, _secteur = noms.get(ticker, (ticker, ""))
        teinte = ("var(--up)" if sharpe >= 1 else
                  "var(--down)" if sharpe < 0 else "var(--ink)")
        # Un titre qu'on ne peut pas vendre merite d'etre signale la ou on le
        # classe, pas seulement sur sa fiche.
        alerte = (" <span class='tag ocre' style='text-transform:none;"
                  "font-size:10px;'>peu liquide</span>"
                  if v.get("peu_liquide") else "")
        html += (
            f"<tr><td style='{cell};color:var(--ink-3);'>{rang}</td>"
            f"<td style='{cell}'><span class='ticker'>{ticker}</span> "
            f"<span style='color:var(--ink-2);'>{nom}</span>{alerte}</td>"
            f"<td style='{nb};font-weight:600;color:{teinte};'>{sharpe:.2f}</td>"
            f"<td style='{nb}'>"
            f"{formater('rendement_annualise', v.get('rendement_annualise'))}</td>"
            f"<td style='{nb}'>{formater('volatilite', v.get('volatilite'))}</td>"
            f"<td style='{nb}'>"
            f"{formater('perte_maximale', v.get('perte_maximale'))}</td>"
            f"<td style='{nb}'>"
            f"{formater('montant_echange', v.get('montant_echange'))}</td></tr>")

    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:12px;"
        f"overflow:hidden;background:var(--bg-elev);'>"
        f"<table style='width:100%;border-collapse:collapse;'>{html}</table>"
        f"</div>", unsafe_allow_html=True)

    manquants = 47 - len(lignes)
    if manquants > 0:
        st.caption(
            f"{manquants} titre(s) n'apparaissent pas : moins de deux ans de "
            f"cotation, donc aucune mesure fiable. Une introduction récente "
            f"n'est pas un mauvais titre, elle est seulement trop jeune pour "
            f"être jugée ici.")


def _render_comparaison_secteurs():
    """Situe les secteurs les uns par rapport aux autres.

    Ce tableau a sa place ici, sur une page qui regarde le marche dans son
    ensemble, et non sur la fiche d'un titre : quand on analyse une banque, on
    veut la comparer aux autres banques, pas a la mediane de la cote. Il ne
    retient donc que les mesures qui gardent le meme sens d'un metier a
    l'autre — rentabilite, valorisation, rendement.
    """
    try:
        from analysis.sectors import (medianes_intersecteurs,
                                      COMPARABLES_INTERSECTEURS,
                                      MIN_OBSERVATIONS)
        inter = medianes_intersecteurs()
    except Exception:
        return
    if not inter:
        return

    with st.expander("Comparaison entre secteurs", expanded=False):
        entetes = "".join(
            f"<th style='padding:6px 10px;font-size:11px;font-weight:500;"
            f"color:var(--ink-3);text-align:right;white-space:nowrap;'>{lib}</th>"
            for lib, _, _ in COMPARABLES_INTERSECTEURS)
        corps = ""
        for e in inter:
            cellules = ""
            for _, cle, forme in COMPARABLES_INTERSECTEURS:
                v = e.get(cle)
                if v is None:
                    txt = "<span style='color:var(--ink-3);'>—</span>"
                elif forme == "fois":
                    txt = f"{v:.1f} ×"
                else:
                    txt = f"{v*100:.1f} %"
                cellules += (
                    f"<td style='padding:6px 10px;font-size:12.5px;"
                    f"text-align:right;font-variant-numeric:tabular-nums;'>"
                    f"{txt}</td>")
            corps += (
                f"<tr style='border-top:1px solid var(--border);'>"
                f"<td style='padding:6px 10px 6px 0;font-size:12.5px;'>"
                f"{e['secteur']}</td>"
                f"<td style='padding:6px 10px;font-size:11.5px;"
                f"color:var(--ink-3);text-align:right;'>{e['effectif']}</td>"
                f"{cellules}</tr>")
        st.markdown(
            f"<div style='overflow-x:auto;'>"
            f"<table style='width:100%;border-collapse:collapse;'>"
            f"<thead><tr>"
            f"<th style='padding:6px 10px;font-size:11px;font-weight:500;"
            f"color:var(--ink-3);text-align:left;'>Secteur</th>"
            f"<th style='padding:6px 10px;font-size:11px;font-weight:500;"
            f"color:var(--ink-3);text-align:right;'>Sociétés</th>"
            f"{entetes}</tr></thead><tbody>{corps}</tbody></table></div>",
            unsafe_allow_html=True)
        st.caption(
            f"Médianes sectorielles, publiées seulement à partir de "
            f"{MIN_OBSERVATIONS} sociétés renseignées — en deçà, la médiane "
            f"refléterait une société et non un secteur, et la case reste "
            f"vide. Pour les banques, la marge nette se rapporte au produit "
            f"net bancaire et non à un chiffre d'affaires : elle n'est pas "
            f"comparable telle quelle aux autres secteurs."
        )


def _display_perf_table(df: pd.DataFrame, col_perf: str):
    """Display a compact performance table."""
    display = df[["ticker", "name", col_perf]].copy()
    display["Perf."] = display[col_perf].apply(_format_pct)
    display["Prix"] = df["price"].apply(lambda x: f"{x:,.0f}" if x and not pd.isna(x) else "—")

    for _, row in display.iterrows():
        val = row[col_perf]
        if val is None or pd.isna(val):
            icon = "⬜"
        elif val >= 0.2:
            icon = ""
        elif val >= 0:
            icon = "🟢"
        elif val >= -0.2:
            icon = "🔴"
        else:
            icon = "💥"

        c1, c2, c3 = st.columns([3, 1.5, 1.5])
        c1.write(f"{icon} **{row['name']}** ({row['ticker']})")
        c2.write(row["Prix"])
        c3.write(row["Perf."])


def _bar_chart(df: pd.DataFrame, col_perf: str, title: str) -> go.Figure:
    """Horizontal bar chart of performances."""
    df_plot = df.sort_values(col_perf, ascending=True).copy()
    colors = [
        COLORS["green"] if v >= 0 else COLORS["red"]
        for v in df_plot[col_perf]
    ]

    fig = go.Figure(go.Bar(
        x=df_plot[col_perf] * 100,
        y=df_plot["name"],
        orientation="h",
        marker_color=colors,
        text=[f"{v:+.1f}%" for v in df_plot[col_perf] * 100],
        textposition="outside",
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Performance (%)",
        yaxis_title="",
        plot_bgcolor=COLORS["bg"],
        paper_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=max(400, len(df_plot) * 28),
        margin=dict(l=200),
    )
    return fig
