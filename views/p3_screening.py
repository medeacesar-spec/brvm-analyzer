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
    st.caption("Filtrer les titres BRVM ayant des données disponibles")

    all_stocks = get_all_stocks_for_analysis()
    if all_stocks.empty:
        st.warning("Aucune donnée disponible. Lancez l'enrichissement des données de marché.")
        return

    verdicts = _load_verdicts_dict()

    # ─── Univers d'analyse ──────────────────────────────────────────────
    section_heading("Univers d'analyse", spacing="tight")
    available_sectors = sorted(all_stocks["sector"].dropna().unique().tolist())

    col_sectors, col_tickers, col_card = st.columns([2, 2, 1])

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

    with col_card:
        # Card "Univers filtré" — card bordée
        n_filtered = len([s for s in selected_tickers if s.split(" · ")[0] in target_tickers])
        st.markdown(
            f"<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-radius:10px;padding:14px 16px;min-height:76px;'>"
            f"<div class='label-xs' style='margin-bottom:4px;'>Univers filtré</div>"
            f"<div style='font-size:26px;font-weight:600;color:var(--ink);"
            f"letter-spacing:-0.02em;font-variant-numeric:tabular-nums;"
            f"line-height:1;'>{n_filtered}</div>"
            f"<div style='font-size:11.5px;color:var(--ink-3);margin-top:4px;'>— titres</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # ─── Filtres fondamentaux ───────────────────────────────────────────
    section_heading("Filtres fondamentaux", spacing="loose")

    col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns(5)

    def _filter_col(label, key_prefix, default_max, step=1.0, divide=False,
                    min_abs=0.0, max_abs=None):
        """Affiche un label-xs + 2 inputs min/max serrés."""
        st.markdown(
            f"<div class='label-xs' style='margin-bottom:4px;'>{label}</div>",
            unsafe_allow_html=True,
        )
        cmin, cmax = st.columns(2)
        with cmin:
            vmin = st.number_input(
                f"{label} min", min_value=min_abs, max_value=max_abs,
                value=0.0, step=step, key=f"{key_prefix}_min",
                label_visibility="collapsed",
            )
        with cmax:
            vmax = st.number_input(
                f"{label} max", min_value=min_abs, max_value=max_abs,
                value=default_max, step=step, key=f"{key_prefix}_max",
                label_visibility="collapsed",
            )
        if divide:
            return vmin / 100, vmax / 100
        return vmin, vmax

    with col_f1:
        min_yield, max_yield = _filter_col(
            "Dividend Yield", "yield", 30.0, step=0.5, divide=True, max_abs=30.0
        )
    with col_f2:
        min_per, max_per = _filter_col(
            "PER", "per", 100.0, step=1.0, divide=False, max_abs=100.0
        )
    with col_f3:
        min_roe, max_roe = _filter_col(
            "ROE", "roe", 100.0, step=1.0, divide=True, max_abs=100.0
        )
    with col_f4:
        min_payout, max_payout = _filter_col(
            "Payout", "payout", 200.0, step=5.0, divide=True, max_abs=200.0
        )
    with col_f5:
        min_de, max_de = _filter_col(
            "D/E", "de", 20.0, step=0.5, divide=False, max_abs=20.0
        )

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
            })
        except Exception:
            continue

    if not results:
        st.warning("Aucun titre avec des données dans la sélection.")
        return

    screen_df = pd.DataFrame(results)

    # Apply ratio filters
    mask = pd.Series(True, index=screen_df.index)
    if min_yield > 0:
        mask &= screen_df["dividend_yield"].fillna(0) >= min_yield
    if max_yield < 0.30:
        mask &= screen_df["dividend_yield"].fillna(0) <= max_yield
    if min_per > 0:
        mask &= (screen_df["per"].fillna(0) >= min_per) | (screen_df["per"].fillna(0) <= 0)
    if max_per < 100:
        mask &= (screen_df["per"].fillna(999) <= max_per) & (screen_df["per"].fillna(0) > 0)
    if min_roe > 0:
        mask &= screen_df["roe"].fillna(0) >= min_roe
    if max_roe < 1.0:
        mask &= screen_df["roe"].fillna(0) <= max_roe
    if min_payout > 0:
        mask &= screen_df["payout_ratio"].fillna(0) >= min_payout
    if max_payout < 2.0:
        mask &= screen_df["payout_ratio"].fillna(0) <= max_payout
    if min_de > 0:
        mask &= screen_df["debt_equity"].fillna(0) >= min_de
    if max_de < 20:
        mask &= screen_df["debt_equity"].fillna(0) <= max_de

    filtered = screen_df[mask].sort_values("fundamental_score", ascending=False, na_position="last")

    # ─── Résultats ──────────────────────────────────────────────────────
    section_heading(f"{len(filtered)} titres correspondent", spacing="loose")

    if filtered.empty:
        st.info("Aucun titre ne correspond. Élargissez vos critères.")
        return

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

    header_style = (
        "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
        "color:var(--ink-3);font-weight:500;padding:10px;"
        "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
    )
    cell_style = "padding:10px;font-size:13px;border-bottom:1px solid var(--border);"
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
            f"<td style='{num_style};font-weight:600;'>{score_str}</td>"
            f"<td style='{cell_style}'>{_verdict_tag(r.get('verdict'))}</td>"
            f"</tr>"
        )

    st.markdown(
        f"<div style='border:1px solid var(--border);border-radius:10px;"
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
                        "fundamental_score", "verdict"]].to_csv(index=False)
        st.download_button("Exporter CSV", csv, "brvm_screening.csv", "text/csv",
                           use_container_width=True)
