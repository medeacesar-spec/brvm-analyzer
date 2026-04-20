"""
Page 10 : Historique & Calibrage
Suivi long terme des événements de signaux et recommandations pour calibration.

Modèle "événement" : une ligne unique par signal/recommandation.
Si le même signal réapparaît dans les 7 jours, on MAJ (pas de doublon).
Si gap > 7 jours, un nouvel événement est créé.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

from data.storage import (
    get_signal_history, compute_signal_performance,
    delete_signal_event, delete_signal_events_bulk,
)
from data.db import read_sql_df
from utils.charts import COLORS
from utils.nav import ticker_quick_picker
from utils.auth import is_admin
from utils.ui_helpers import section_heading, kpi_card, tag, ticker as ticker_chip, delta


@st.cache_data(ttl=300, show_spinner=False)
def _load_signal_perf_snapshot() -> pd.DataFrame:
    """Charge signal_performance_snapshot en une seule requête.
    Cache 5 min pour éviter les re-queries lors des changements de filtres."""
    try:
        return read_sql_df(
            "SELECT event_id, current_price, perf_1m, perf_3m, perf_6m, "
            "perf_1a, perf_since_start, duration_days FROM signal_performance_snapshot"
        )
    except Exception:
        return pd.DataFrame()


VERDICT_ORDER = ["ACHAT FORT", "ACHAT", "CONSERVER", "NEUTRE", "VENTE", "VENTE FORTE"]
VERDICT_COLORS = {
    "ACHAT FORT": COLORS["green"],
    "ACHAT": "#7ED957",
    "CONSERVER": COLORS["yellow"],
    "NEUTRE": COLORS["yellow"],
    "VENTE": "#FF8C42",
    "VENTE FORTE": COLORS["red"],
}

# Verdict -> (dot_tone, tag_tone)
VERDICT_TONES = {
    "ACHAT FORT": ("up", "up"),
    "ACHAT": ("up", "up"),
    "CONSERVER": ("ocre", "ocre"),
    "NEUTRE": ("ocre", "ocre"),
    "PRUDENCE": ("ocre", "ocre"),
    "VENTE": ("down", "down"),
    "VENTE FORTE": ("down", "down"),
    "EVITER": ("down", "down"),
}


def _fmt_pct(v):
    if v is None or pd.isna(v):
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1%}"


def _fmt_num(v):
    if v is None or pd.isna(v):
        return "—"
    return f"{v:,.0f}"


def _duration_days(first, last):
    try:
        d1 = datetime.strptime(str(first), "%Y-%m-%d")
        d2 = datetime.strptime(str(last), "%Y-%m-%d")
        return (d2 - d1).days
    except Exception:
        return None


def _delta_cell(v):
    """Cellule tabulaire : retourne HTML coloré pour une fraction (ex 0.034 = +3.4%)."""
    if v is None or pd.isna(v):
        return "<span style='color:var(--ink-3)'>—</span>"
    pct = v * 100
    return delta(pct, with_arrow=False)


def _fmt_date_fr(d):
    try:
        dt = datetime.strptime(str(d), "%Y-%m-%d")
        return dt.strftime("%d/%m")
    except Exception:
        return str(d)


def render():
    st.title("Historique Signaux & Recommandations")
    st.caption("Performance agrégée · calibration du modèle")

    df_all = get_signal_history()
    if df_all.empty:
        st.info(
            "Aucun signal enregistré pour l'instant. Visitez les pages "
            "**Signaux** et **Analyse d'un Titre** pour commencer à capturer des événements."
        )
        return

    st.caption(
        "Suivi long terme des signaux et recommandations. "
        "1 ligne = 1 événement unique. Si un signal réapparaît dans les 7 jours, "
        "il met à jour l'événement existant (pas de doublon). Au-delà de 7 jours, "
        "la prochaine occurrence devient un nouvel événement."
    )

    # --- KPIs header ---
    n_events = len(df_all)
    sig_all = df_all[df_all["entry_type"] == "signal"]
    n_signals = len(sig_all)
    n_recos = len(df_all[df_all["entry_type"] == "recommendation"])
    n_tickers = df_all["ticker"].nunique()
    oldest = df_all["first_seen_date"].min()
    newest = df_all["last_seen_date"].max()

    n_buy = int((sig_all["signal_type"] == "achat").sum()) if not sig_all.empty else 0
    n_sell = int((sig_all["signal_type"] == "vente").sum()) if not sig_all.empty else 0
    n_info = int((sig_all["signal_type"] == "info").sum()) if not sig_all.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi_card("Événements", f"{n_events:,}",
                 sub=f"Période {_fmt_date_fr(oldest)} → {_fmt_date_fr(newest)}")
    with c2:
        kpi_card("Signaux uniques", f"{n_signals:,}",
                 sub=f"{n_buy} achat · {n_sell} vente · {n_info} info")
    with c3:
        kpi_card("Recommandations", f"{n_recos:,}",
                 sub=f"{n_recos} verdicts actifs")
    with c4:
        kpi_card("Titres suivis", f"{n_tickers}",
                 sub="BRVM · tous secteurs")

    # --- Filters ---
    col_f1, col_f2, col_f3, col_f4 = st.columns([2, 2, 2, 3])

    with col_f1:
        tickers = ["Tous"] + sorted(df_all["ticker"].unique().tolist())
        sel_ticker = st.selectbox("Ticker", tickers)

    with col_f2:
        status_mode = st.selectbox(
            "Statut",
            ["Tous", "Actifs (vus il y a ≤ 7j)", "Inactifs (plus vus depuis > 7j)"],
        )

    with col_f3:
        entry_mode = st.selectbox(
            "Type",
            ["Tous", "Signaux uniquement", "Recommandations uniquement"],
        )

    # Apply filters
    df = df_all.copy()
    if sel_ticker != "Tous":
        df = df[df["ticker"] == sel_ticker]

    if status_mode != "Tous":
        today = datetime.now().date()
        df["_last_dt"] = pd.to_datetime(df["last_seen_date"], errors="coerce").dt.date
        cutoff = today - timedelta(days=7)
        if status_mode.startswith("Actifs"):
            df = df[df["_last_dt"] >= cutoff]
        else:
            df = df[df["_last_dt"] < cutoff]
        df = df.drop(columns=["_last_dt"])

    if entry_mode == "Signaux uniquement":
        df = df[df["entry_type"] == "signal"]
    elif entry_mode == "Recommandations uniquement":
        df = df[df["entry_type"] == "recommendation"]

    if df.empty:
        st.warning("Aucun événement pour ces filtres.")
        return

    # Performance depuis snapshot précalculé, fallback live
    snap = _load_signal_perf_snapshot()
    if not snap.empty and "id" in df.columns:
        df = df.merge(snap, how="left", left_on="id", right_on="event_id")
        if "event_id" in df.columns:
            df = df.drop(columns=["event_id"])
        snapshot_used = True
    else:
        with st.spinner("Snapshot vide — calcul live (lent)…"):
            df = compute_signal_performance(df)
        snapshot_used = False

    if "duration_days" not in df.columns or df["duration_days"].isna().all():
        df["duration_days"] = df.apply(
            lambda r: _duration_days(r.get("first_seen_date"), r.get("last_seen_date")),
            axis=1,
        )

    if not snapshot_used and is_admin():
        st.warning(
            "Snapshot de performance vide. Cliquez sur **Regénérer snapshots** "
            "dans la sidebar pour accélérer cette page."
        )

    # Quick picker (4e colonne filtres)
    present_tickers = sorted(df["ticker"].unique().tolist())
    with col_f4:
        if present_tickers:
            picker_options = [(t, t) for t in present_tickers]
            ticker_quick_picker(
                picker_options, key="calib_goto",
                label="Ouvrir l'analyse d'un titre",
            )

    # --- Tabs (sans émojis) ---
    tab1, tab2, tab3, tab_cal, tab4 = st.tabs([
        "Vue d'ensemble", "Signaux", "Recommandations",
        "Calibration", "Données brutes"
    ])

    # ============================================================
    # TAB 1 : OVERVIEW
    # ============================================================
    with tab1:
        section_heading("Performance agrégée par type", spacing="loose")

        sig_df = df[df["entry_type"] == "signal"].copy()
        reco_df_all = df[df["entry_type"] == "recommendation"].copy()

        # 3 cards : Achat / Vente / Recommandations (horizon 3M)
        def _stats_buy(d):
            vals = d["perf_3m"].dropna() if "perf_3m" in d.columns else pd.Series([], dtype=float)
            if len(vals) == 0:
                return None, None, 0
            return (vals > 0).mean(), vals.mean(), len(vals)

        def _stats_sell(d):
            vals = d["perf_3m"].dropna() if "perf_3m" in d.columns else pd.Series([], dtype=float)
            if len(vals) == 0:
                return None, None, 0
            return (vals < 0).mean(), vals.mean(), len(vals)

        buy_df = sig_df[sig_df["signal_type"] == "achat"] if not sig_df.empty else pd.DataFrame()
        sell_df = sig_df[sig_df["signal_type"] == "vente"] if not sig_df.empty else pd.DataFrame()

        buy_rate, buy_avg, buy_n = _stats_buy(buy_df) if not buy_df.empty else (None, None, 0)
        sell_rate, sell_avg, sell_n = _stats_sell(sell_df) if not sell_df.empty else (None, None, 0)

        reco_vals = reco_df_all["perf_3m"].dropna() if "perf_3m" in reco_df_all.columns else pd.Series([], dtype=float)
        if len(reco_vals) > 0:
            reco_rate = (reco_vals > 0).mean()
            reco_avg = reco_vals.mean()
            reco_n = len(reco_vals)
        else:
            reco_rate, reco_avg, reco_n = None, None, 0

        def _render_perf_card(label, rate, avg, n, footer, tone):
            pct_txt = f"{rate*100:.0f}%" if rate is not None else "—"
            sub = f"{n} signaux" if n else "Aucun échantillon"
            avg_html = delta(avg * 100, with_arrow=True) if avg is not None else ""
            avg_line = f"{avg_html} moy." if avg is not None else ""
            accent = {"up": "var(--up)", "down": "var(--down)", "ocre": "var(--ocre)"}.get(tone, "var(--primary)")
            st.markdown(
                f"<div style='border:1px solid var(--border);border-left:3px solid {accent};"
                f"border-radius:10px;padding:14px 16px;background:var(--bg-elev);min-height:138px;'>"
                f"<div class='label-xs' style='color:var(--ink-3);margin-bottom:6px;'>{label}</div>"
                f"<div style='font-size:28px;font-weight:600;font-variant-numeric:tabular-nums;'>{pct_txt}</div>"
                f"<div style='font-size:12px;color:var(--ink-2);margin-top:2px;'>{sub}</div>"
                f"<div style='font-size:12px;margin-top:6px;'>{avg_line}</div>"
                f"<div style='font-size:11.5px;color:var(--ink-3);margin-top:8px;'>{footer}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

        colA, colB, colC = st.columns(3)
        with colA:
            _render_perf_card("SIGNAUX D'ACHAT", buy_rate, buy_avg, buy_n,
                              "Prix monte depuis 1re apparition", "up")
        with colB:
            _render_perf_card("SIGNAUX DE VENTE", sell_rate, sell_avg, sell_n,
                              "Prix baisse depuis 1re apparition", "down")
        with colC:
            _render_perf_card("RECOMMANDATIONS", reco_rate, reco_avg, reco_n,
                              "Hybride fondamental + technique", "ocre")

        # Detail table
        section_heading("Détail par type de signal", spacing="loose")
        if not sig_df.empty:
            agg = (
                sig_df.groupby(["signal_type", "signal_name"])
                .agg(
                    nb=("id", "count"),
                    force_moy=("signal_strength", "mean"),
                    occ_moy=("occurrence_count", "mean"),
                    perf_1m=("perf_1m", "mean"),
                    perf_3m=("perf_3m", "mean"),
                    perf_6m=("perf_6m", "mean"),
                    perf_1a=("perf_1a", "mean"),
                    perf_start=("perf_since_start", "mean"),
                )
                .reset_index()
                .sort_values("nb", ascending=False)
            )

            header_style = (
                "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
                "color:var(--ink-3);font-weight:500;padding:9px 10px;"
                "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
            )
            cell_style = "padding:10px 12px;font-size:13px;border-bottom:1px solid var(--border);"
            num_style = cell_style + "text-align:right;font-variant-numeric:tabular-nums;"

            rows_html = (
                f"<tr>"
                f"<th style='{header_style};text-align:left;'>Type</th>"
                f"<th style='{header_style};text-align:left;'>Signal</th>"
                f"<th style='{header_style};text-align:right;'>N</th>"
                f"<th style='{header_style};text-align:right;'>Force</th>"
                f"<th style='{header_style};text-align:right;'>Occ.</th>"
                f"<th style='{header_style};text-align:right;'>1M</th>"
                f"<th style='{header_style};text-align:right;'>3M</th>"
                f"<th style='{header_style};text-align:right;'>6M</th>"
                f"<th style='{header_style};text-align:right;'>1A</th>"
                f"<th style='{header_style};text-align:right;'>Depuis début</th>"
                f"</tr>"
            )

            for _, r in agg.iterrows():
                stype = r["signal_type"]
                if stype == "achat":
                    type_html = tag("ACHAT", tone="up")
                elif stype == "vente":
                    type_html = tag("VENTE", tone="down")
                else:
                    type_html = tag((stype or "—").upper(), tone="ocre")
                force_v = r["force_moy"]
                force_s = f"{force_v:.1f}" if pd.notna(force_v) else "—"
                occ_v = r["occ_moy"]
                occ_s = f"{occ_v:.1f}" if pd.notna(occ_v) else "—"
                rows_html += (
                    f"<tr>"
                    f"<td style='{cell_style}'>{type_html}</td>"
                    f"<td style='{cell_style};font-weight:500;'>{r['signal_name']}</td>"
                    f"<td style='{num_style}'>{int(r['nb']):,}</td>"
                    f"<td style='{num_style}'>{force_s}</td>"
                    f"<td style='{num_style}'>{occ_s}</td>"
                    f"<td style='{num_style}'>{_delta_cell(r['perf_1m'])}</td>"
                    f"<td style='{num_style}'>{_delta_cell(r['perf_3m'])}</td>"
                    f"<td style='{num_style}'>{_delta_cell(r['perf_6m'])}</td>"
                    f"<td style='{num_style}'>{_delta_cell(r['perf_1a'])}</td>"
                    f"<td style='{num_style}'>{_delta_cell(r['perf_start'])}</td>"
                    f"</tr>"
                )

            st.markdown(
                f"<div style='border:1px solid var(--border);border-radius:10px;"
                f"overflow:hidden;background:var(--bg-elev);margin-bottom:16px;'>"
                f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
                unsafe_allow_html=True,
            )
        else:
            st.info("Pas encore de signaux.")

    # ============================================================
    # TAB 2 : SIGNALS — tableau éditable avec suppression
    # ============================================================
    with tab2:
        sig_df = df[df["entry_type"] == "signal"].sort_values(
            ["last_seen_date", "ticker"],
            ascending=[False, True],
        ).copy()
        if sig_df.empty:
            st.info("Aucun signal dans cette sélection.")
        else:
            from analysis.scoring import _signal_family

            sig_df["family"] = sig_df["signal_name"].apply(_signal_family)

            # Contradictions
            conflict_keys = set()
            for (ticker_, family), gdf in sig_df.groupby(["ticker", "family"]):
                if gdf["signal_type"].nunique() > 1:
                    conflict_keys.add((ticker_, family))
            sig_df["conflict"] = sig_df.apply(
                lambda r: "⚠" if (r["ticker"], r["family"]) in conflict_keys else "",
                axis=1,
            )

            # Direction (HTML n'est pas rendu par data_editor -> utiliser texte clair)
            sig_df["dir_icon"] = sig_df["signal_type"].map({
                "achat": "● Achat", "vente": "● Vente", "info": "● Info"
            }).fillna("—")

            # Header row : title + actions
            head_left, head_right = st.columns([3, 2])
            with head_left:
                section_heading(f"Événements de signaux · {len(sig_df)}")
            st.caption(
                "Cocher pour sélectionner. La colonne ⚠ « Contradiction » marque les "
                "tickers dont la même famille a à la fois des signaux achat et vente actifs."
            )

            # Build table
            table_df = pd.DataFrame({
                "☐": False,
                "Ticker": sig_df["ticker"].values,
                "Nom": sig_df["company_name"].fillna(sig_df["ticker"]).values,
                "Direction": sig_df["dir_icon"].values,
                "Famille": sig_df["family"].values,
                "Signal": sig_df["signal_name"].values,
                "F.": sig_df["signal_strength"].fillna(0).astype(int).values,
                "⚠": sig_df["conflict"].values,
                "1re app.": sig_df["first_seen_date"].values,
                "Dernière vue": sig_df["last_seen_date"].values,
                "Durée (j)": sig_df["duration_days"].fillna(0).astype(int).values,
                "Occ.": sig_df["occurrence_count"].fillna(1).astype(int).values,
                "Prix début": sig_df["price_at_start"].apply(_fmt_num).values,
                "Prix actuel": sig_df["current_price"].apply(_fmt_num).values,
                "Depuis début": sig_df["perf_since_start"].apply(_fmt_pct).values,
                "Perf. 1M": sig_df["perf_1m"].apply(_fmt_pct).values,
                "Perf. 3M": sig_df["perf_3m"].apply(_fmt_pct).values,
                "Perf. 6M": sig_df["perf_6m"].apply(_fmt_pct).values,
                "Perf. 1A": sig_df["perf_1a"].apply(_fmt_pct).values,
                "Détails": sig_df["signal_details"].fillna("").values,
                "_id": sig_df["id"].values,
            })

            edited = st.data_editor(
                table_df,
                use_container_width=True,
                hide_index=True,
                height=min(650, 80 + 35 * len(table_df)),
                key="signals_table_editor",
                column_config={
                    "☐": st.column_config.CheckboxColumn(
                        "☐", help="Cocher pour sélectionner", default=False, width="small",
                    ),
                    "_id": None,
                    "Détails": st.column_config.TextColumn(width="medium"),
                },
                disabled=[c for c in table_df.columns if c != "☐"],
            )

            ids_to_delete = edited.loc[edited["☐"] == True, "_id"].astype(int).tolist()

            # Action bar (en haut visuel : on l'a placée au-dessus via head_right)
            with head_right:
                bcol1, bcol2 = st.columns(2)
                with bcol1:
                    csv_sig = sig_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "Exporter CSV",
                        data=csv_sig,
                        file_name=f"signaux_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        key="sig_csv_btn",
                        use_container_width=True,
                    )
                with bcol2:
                    disabled = len(ids_to_delete) == 0
                    label = (
                        f"Supprimer ({len(ids_to_delete)})" if not disabled
                        else "Supprimer sélection"
                    )
                    if st.button(
                        label,
                        type="primary",
                        key="bulk_delete_signals_btn",
                        disabled=disabled,
                        use_container_width=True,
                    ):
                        n = delete_signal_events_bulk(ids_to_delete)
                        st.success(f"{n} événement(s) supprimé(s).")
                        st.rerun()

    # ============================================================
    # TAB 3 : RECOMMENDATIONS
    # ============================================================
    with tab3:
        reco_df = df[df["entry_type"] == "recommendation"].sort_values(
            ["last_seen_date", "ticker"],
            ascending=[False, True],
        ).copy()
        if reco_df.empty:
            st.info("Aucune recommandation dans cette sélection.")
        else:
            section_heading(f"Événements de recommandations · {len(reco_df)}")
            st.caption(
                "Chaque ligne = période pendant laquelle un titre a eu un même verdict. "
                "Si le verdict change ou disparaît > 7 jours puis revient, un nouvel événement est créé."
            )

            # Summary by verdict
            verdict_perf = (
                reco_df.dropna(subset=["verdict"])
                .groupby("verdict")
                .agg(
                    nb=("id", "count"),
                    avg_score=("hybrid_score", "mean"),
                    avg_duration=("duration_days", "mean"),
                    perf_1m=("perf_1m", "mean"),
                    perf_3m=("perf_3m", "mean"),
                    perf_6m=("perf_6m", "mean"),
                    perf_1a=("perf_1a", "mean"),
                    perf_start=("perf_since_start", "mean"),
                )
                .reset_index()
            )
            verdict_perf["order"] = verdict_perf["verdict"].apply(
                lambda v: VERDICT_ORDER.index(v) if v in VERDICT_ORDER else 99,
            )
            verdict_perf = verdict_perf.sort_values("order").drop(columns=["order"])

            if not verdict_perf.empty:
                section_heading("Performance moyenne par verdict", spacing="loose")

                header_style = (
                    "font-size:10.5px;text-transform:uppercase;letter-spacing:0.08em;"
                    "color:var(--ink-3);font-weight:500;padding:9px 10px;"
                    "border-bottom:1px solid var(--border);background:var(--bg-sunken);"
                )
                cell_style = "padding:10px 12px;font-size:13px;border-bottom:1px solid var(--border);"
                num_style = cell_style + "text-align:right;font-variant-numeric:tabular-nums;"

                rows_html = (
                    f"<tr>"
                    f"<th style='{header_style};text-align:left;'>Verdict</th>"
                    f"<th style='{header_style};text-align:right;'>N</th>"
                    f"<th style='{header_style};text-align:right;'>Score moy.</th>"
                    f"<th style='{header_style};text-align:right;'>Durée moy.</th>"
                    f"<th style='{header_style};text-align:right;'>1M</th>"
                    f"<th style='{header_style};text-align:right;'>3M</th>"
                    f"<th style='{header_style};text-align:right;'>6M</th>"
                    f"<th style='{header_style};text-align:right;'>1A</th>"
                    f"<th style='{header_style};text-align:right;'>Depuis début</th>"
                    f"</tr>"
                )
                for _, r in verdict_perf.iterrows():
                    v = r["verdict"]
                    dot_tone, _ = VERDICT_TONES.get(v, ("ocre", "ocre"))
                    verdict_html = (
                        f"<span class='dot {dot_tone}'></span>"
                        f"<span style='font-weight:500;letter-spacing:0.02em;'>{v}</span>"
                    )
                    score = r["avg_score"]
                    score_s = f"{score:.0f}/100" if pd.notna(score) else "—"
                    dur = r["avg_duration"]
                    dur_s = f"{dur:.0f} j" if pd.notna(dur) else "—"
                    rows_html += (
                        f"<tr>"
                        f"<td style='{cell_style}'>{verdict_html}</td>"
                        f"<td style='{num_style}'>{int(r['nb'])}</td>"
                        f"<td style='{num_style}'>{score_s}</td>"
                        f"<td style='{num_style}'>{dur_s}</td>"
                        f"<td style='{num_style}'>{_delta_cell(r['perf_1m'])}</td>"
                        f"<td style='{num_style}'>{_delta_cell(r['perf_3m'])}</td>"
                        f"<td style='{num_style}'>{_delta_cell(r['perf_6m'])}</td>"
                        f"<td style='{num_style}'>{_delta_cell(r['perf_1a'])}</td>"
                        f"<td style='{num_style}'>{_delta_cell(r['perf_start'])}</td>"
                        f"</tr>"
                    )
                st.markdown(
                    f"<div style='border:1px solid var(--border);border-radius:10px;"
                    f"overflow:hidden;background:var(--bg-elev);margin-bottom:16px;'>"
                    f"<table style='width:100%;border-collapse:collapse;'>{rows_html}</table></div>",
                    unsafe_allow_html=True,
                )

                chart_df = verdict_perf.dropna(subset=["perf_3m"])
                if not chart_df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=chart_df["verdict"],
                        y=chart_df["perf_3m"] * 100,
                        marker_color=[VERDICT_COLORS.get(v, COLORS["primary"])
                                      for v in chart_df["verdict"]],
                        text=[f"{v:+.1f}%" for v in chart_df["perf_3m"] * 100],
                        textposition="outside",
                    ))
                    fig.update_layout(
                        title="Performance 3M moyenne par verdict",
                        yaxis_title="Perf. 3M (%)",
                        plot_bgcolor=COLORS["bg"], paper_bgcolor=COLORS["bg"],
                        font=dict(color=COLORS["text"]), height=400,
                    )
                    st.plotly_chart(fig, use_container_width=True)

            section_heading("Liste des recommandations", spacing="loose")
            st.caption("Cocher pour sélectionner des lignes à supprimer.")

            # Stars rendu texte (data_editor ne supporte pas HTML)
            def _stars_txt(n):
                try:
                    n = int(n)
                except Exception:
                    n = 0
                return "★" * n + "☆" * max(0, 5 - n) + f"  {n}/5"

            def _trend_txt(t):
                if not t or pd.isna(t):
                    return "—"
                tl = str(t).lower()
                if "haus" in tl:
                    return "▲ Haussière"
                if "bais" in tl:
                    return "▼ Baissière"
                return str(t)

            reco_table = pd.DataFrame({
                "☐": False,
                "Ticker": reco_df["ticker"].values,
                "Nom": reco_df["company_name"].fillna(reco_df["ticker"]).values,
                "Verdict": reco_df["verdict"].fillna("—").values,
                "Score": reco_df["hybrid_score"].apply(
                    lambda x: f"{x:.0f}/100" if x and not pd.isna(x) else "—"
                ).values,
                "★": [_stars_txt(s) for s in reco_df["stars"].fillna(0).values],
                "Tendance": [_trend_txt(t) for t in reco_df["trend"].fillna("—").values],
                "1re app.": reco_df["first_seen_date"].values,
                "Dernière vue": reco_df["last_seen_date"].values,
                "Durée (j)": reco_df["duration_days"].fillna(0).astype(int).values,
                "Occ.": reco_df["occurrence_count"].fillna(1).astype(int).values,
                "Prix début": reco_df["price_at_start"].apply(_fmt_num).values,
                "Prix actuel": reco_df["current_price"].apply(_fmt_num).values,
                "Depuis début": reco_df["perf_since_start"].apply(_fmt_pct).values,
                "Perf. 1M": reco_df["perf_1m"].apply(_fmt_pct).values,
                "Perf. 3M": reco_df["perf_3m"].apply(_fmt_pct).values,
                "Perf. 6M": reco_df["perf_6m"].apply(_fmt_pct).values,
                "Perf. 1A": reco_df["perf_1a"].apply(_fmt_pct).values,
                "_id": reco_df["id"].values,
            })

            edited_reco = st.data_editor(
                reco_table,
                use_container_width=True,
                hide_index=True,
                height=min(650, 80 + 35 * len(reco_table)),
                key="recos_table_editor",
                column_config={
                    "☐": st.column_config.CheckboxColumn(
                        "☐", help="Cocher pour sélectionner", default=False, width="small",
                    ),
                    "_id": None,
                },
                disabled=[c for c in reco_table.columns if c != "☐"],
            )

            reco_ids_to_delete = edited_reco.loc[
                edited_reco["☐"] == True, "_id"
            ].astype(int).tolist()

            col_b1, col_b2, _ = st.columns([1.2, 1.2, 3])
            with col_b1:
                csv_reco = reco_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Exporter CSV",
                    data=csv_reco,
                    file_name=f"recommandations_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key="reco_csv_btn",
                    use_container_width=True,
                )
            with col_b2:
                disabled = len(reco_ids_to_delete) == 0
                label = (
                    f"Supprimer ({len(reco_ids_to_delete)})" if not disabled
                    else "Supprimer sélection"
                )
                if st.button(
                    label,
                    type="primary",
                    key="bulk_delete_recos_btn",
                    disabled=disabled,
                    use_container_width=True,
                ):
                    n = delete_signal_events_bulk(reco_ids_to_delete)
                    st.success(f"{n} recommandation(s) supprimée(s).")
                    st.rerun()

    # ============================================================
    # TAB CAL : CALIBRATION
    # ============================================================
    with tab_cal:
        from analysis.calibration import (
            get_calibration, MIN_DAYS_HISTORY, MIN_SAMPLES,
            WEIGHT_MIN, WEIGHT_MAX, REFERENCE_HORIZON,
            is_review_due, next_review_date, run_monthly_review,
            get_review_history, _get_last_review_date, REVIEW_INTERVAL_DAYS,
        )

        cal = get_calibration(force_refresh=False)
        days_available = cal.get("days_available", 0)
        enabled = cal.get("enabled", False)

        # Carte bordée : header + caption + KPIs + progress
        status_tag = tag("ACTIVE", tone="up") if enabled else tag("EN ATTENTE", tone="ocre")
        progress_pct = min(100, int(days_available / MIN_DAYS_HISTORY * 100)) if MIN_DAYS_HISTORY else 0

        # Ouvre la carte
        st.markdown(
            "<div style='border:1px solid var(--border);border-radius:12px;"
            "padding:18px 20px;background:var(--bg-elev);margin-bottom:18px;'>",
            unsafe_allow_html=True,
        )

        head_l, head_r = st.columns([4, 1])
        with head_l:
            section_heading("Calibration des signaux et recommandations", spacing="tight")
        with head_r:
            st.markdown(
                f"<div style='text-align:right;padding-top:4px;'>{status_tag}</div>",
                unsafe_allow_html=True,
            )

        st.caption(
            f"Pondération dérivée de la performance historique à 3 mois. "
            f"S'active dès que l'historique couvre ≥ {MIN_DAYS_HISTORY} jours et que le "
            f"signal a ≥ {MIN_SAMPLES} échantillons. Poids bornés dans "
            f"[{WEIGHT_MIN}, {WEIGHT_MAX}], multipliés à la force brute."
        )

        k1, k2, k3 = st.columns(3)
        with k1:
            kpi_card("Historique", f"{days_available}", unit=" jours",
                     sub="Période d'observation")
        with k2:
            kpi_card("Seuil activation", f"{MIN_DAYS_HISTORY}", unit=" jours",
                     sub="Calibration auto")
        with k3:
            kpi_card("Progression", f"{progress_pct}", unit="%",
                     sub=f"{days_available} / {MIN_DAYS_HISTORY} jours",
                     tone="up" if enabled else "neutral")

        st.markdown(
            "<div class='label-xs' style='margin-top:14px;margin-bottom:6px;'>"
            "PROGRESSION VERS L'ACTIVATION</div>"
            f"<div style='height:6px;background:var(--bg-sunken);border-radius:3px;"
            f"overflow:hidden;'>"
            f"<div style='height:100%;width:{progress_pct}%;background:var(--primary);'></div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        if not enabled:
            st.caption("Poids neutres (1.0×) appliqués d'ici là.")

        st.markdown("</div>", unsafe_allow_html=True)

        # --- Monthly review ---
        section_heading(
            f"Revue mensuelle · tous les {REVIEW_INTERVAL_DAYS} jours",
            spacing="loose",
        )
        last_review = _get_last_review_date()
        next_review = next_review_date()
        due = is_review_due()

        days_to_next = None
        if next_review is not None:
            try:
                days_to_next = (next_review - datetime.now().date()).days
            except Exception:
                days_to_next = None

        r1, r2, r3 = st.columns(3)
        with r1:
            kpi_card(
                "Dernière revue",
                last_review.strftime("%d/%m/%Y") if last_review else "—",
                sub="Snapshot archivé" if last_review else "Jamais effectuée",
            )
        with r2:
            next_sub = (
                f"Dans {days_to_next} jours" if (days_to_next is not None and days_to_next >= 0)
                else ("En retard" if (days_to_next is not None and days_to_next < 0) else "À la 1re revue")
            )
            kpi_card(
                "Prochaine revue",
                next_review.strftime("%d/%m/%Y") if next_review else "—",
                sub=next_sub,
            )
        with r3:
            if due:
                kpi_card("Statut", "En retard", sub="Revue à lancer", tone="ocre")
            else:
                kpi_card("Statut", "À jour", sub="▲ Système OK", tone="up")

        col_b1, col_b2 = st.columns([1, 2])
        with col_b1:
            if is_admin():
                if st.button(
                    "Lancer la revue maintenant",
                    type="primary" if due else "secondary",
                    use_container_width=True,
                ):
                    result = run_monthly_review(force=True)
                    if result.get("skipped"):
                        st.warning(result.get("reason", "Non fait"))
                    else:
                        st.success(
                            f"Revue enregistrée : {result['calibrated_signals']} signaux "
                            f"et {result['calibrated_recos']} verdicts calibrés "
                            f"(historique {result['days_available']}j)."
                        )
                    st.rerun()
            else:
                st.caption("Action réservée à l'administrateur.")
        with col_b2:
            st.caption(
                "La revue recalcule les taux de succès et met à jour les poids. "
                "Un snapshot est archivé pour suivre l'évolution dans le temps. "
                "Possible à tout moment mais recommandé une fois par mois."
                + (" Réservée à l'administrateur." if not is_admin() else "")
            )

        # --- Review history ---
        history = get_review_history()
        if history:
            section_heading("Historique des revues", spacing="loose")
            hist_rows = []
            for h in history:
                hist_rows.append({
                    "Date": h["review_date"],
                    "Historique (j)": h["days_available"] or 0,
                    "Signaux": h["n_signals"] or 0,
                    "Signaux calibrés": h["calibrated_signals"] or 0,
                    "Verdicts": h["n_recos"] or 0,
                    "Verdicts calibrés": h["calibrated_recos"] or 0,
                    "Notes": h.get("notes") or "",
                })
            st.dataframe(
                pd.DataFrame(hist_rows),
                use_container_width=True, hide_index=True,
            )

            if len(history) >= 2:
                with st.expander("Évolution des poids entre revues", expanded=False):
                    latest_signals = history[0].get("payload", {}).get("signals", {})
                    tracked = sorted(
                        latest_signals.keys(),
                        key=lambda k: -(latest_signals[k].get("n_samples") or 0),
                    )[:6]

                    fig = go.Figure()
                    for sig_name in tracked:
                        dates = []
                        weights = []
                        for rev in reversed(history):
                            sig_info = rev.get("payload", {}).get("signals", {}).get(sig_name)
                            if sig_info:
                                dates.append(rev["review_date"])
                                weights.append(sig_info.get("weight", 1.0))
                        if dates:
                            fig.add_trace(go.Scatter(
                                x=dates, y=weights, mode="lines+markers",
                                name=sig_name[:30],
                            ))
                    fig.add_hline(y=1.0, line_dash="dash", line_color="gray",
                                  annotation_text="Neutre (1.0)")
                    fig.update_layout(
                        yaxis_title="Poids", xaxis_title="Revue",
                        height=400, hovermode="x unified",
                    )
                    st.plotly_chart(fig, use_container_width=True)

        # Signals weights table
        section_heading("Poids par type de signal", spacing="loose")
        sig_rows = []
        for name, info in cal.get("signals", {}).items():
            sig_rows.append({
                "Signal": name,
                "Direction": info.get("direction", "—"),
                "Échantillons": info.get("n_samples", 0),
                "Taux succès 3M": (
                    f"{info['success_rate']:.0%}" if info.get("success_rate") is not None else "—"
                ),
                "Perf. moyenne 3M": (
                    f"{info['avg_return']:+.1%}" if info.get("avg_return") is not None else "—"
                ),
                "Poids appliqué": f"{info.get('weight', 1.0):.2f}×",
                "Calibré": "Oui" if info.get("calibrated") else "Non",
            })
        if sig_rows:
            st.dataframe(
                pd.DataFrame(sig_rows).sort_values("Échantillons", ascending=False),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Aucun signal historique à pondérer pour l'instant.")

        # Reco weights table
        section_heading("Poids par verdict de recommandation", spacing="loose")
        reco_rows = []
        for verdict, info in cal.get("recommendations", {}).items():
            reco_rows.append({
                "Verdict": verdict,
                "Direction": info.get("direction", "—"),
                "Échantillons": info.get("n_samples", 0),
                "Taux succès 3M": (
                    f"{info['success_rate']:.0%}" if info.get("success_rate") is not None else "—"
                ),
                "Perf. moyenne 3M": (
                    f"{info['avg_return']:+.1%}" if info.get("avg_return") is not None else "—"
                ),
                "Poids appliqué": f"{info.get('weight', 1.0):.2f}×",
                "Calibré": "Oui" if info.get("calibrated") else "Non",
            })
        if reco_rows:
            st.dataframe(
                pd.DataFrame(reco_rows).sort_values("Échantillons", ascending=False),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Aucune recommandation historique à pondérer.")

        with st.expander("Comment fonctionne la calibration ?"):
            st.markdown(
                f"""
**Objectif** : remplacer les poids statiques des signaux (force 1 à 5 fixe) par des
poids dérivés de leur performance passée réelle.

**Calcul** :
- Pour chaque type de signal, on collecte toutes les occurrences dans l'historique
- On mesure la performance du prix à {REFERENCE_HORIZON.replace('perf_', '').upper()}
  **depuis la première apparition** de chaque événement
- **Taux de succès** :
  - Signal **achat** : % de cas où le prix a monté
  - Signal **vente** : % de cas où le prix a baissé
- **Poids** = interpolation linéaire :
  - 50% succès (aléatoire) → poids **1.0**
  - 100% succès → poids **{WEIGHT_MAX}**
  - 0% succès → poids **{WEIGHT_MIN}**

**Application** :
- La force d'un signal calibré est multipliée par son poids avant consolidation
- Les signaux non-calibrés gardent un poids de **1.0**
- La confiance du verdict global est ajustée par le poids du verdict

**Seuils** :
- Activation : au moins **{MIN_DAYS_HISTORY} jours** d'historique
- Par signal : au moins **{MIN_SAMPLES} échantillons** pour être calibré
                """
            )

    # ============================================================
    # TAB 4 : RAW DATA
    # ============================================================
    with tab4:
        head_l, head_r = st.columns([3, 2])
        with head_l:
            section_heading(f"Toutes les données · {len(df)} lignes")
        st.caption(
            "Vue technique brute. Toutes les colonnes du modèle ; utile pour export "
            "ou debugging. Double-cliquer pour éditer."
        )

        with head_r:
            b1, b2, b3 = st.columns(3)
            with b1:
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Exporter CSV",
                    data=csv,
                    file_name=f"signal_events_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key="raw_csv_btn",
                    use_container_width=True,
                )
            with b2:
                json_bytes = df.to_json(orient="records", force_ascii=False).encode("utf-8")
                st.download_button(
                    "Exporter JSON",
                    data=json_bytes,
                    file_name=f"signal_events_{datetime.now().strftime('%Y%m%d')}.json",
                    mime="application/json",
                    key="raw_json_btn",
                    use_container_width=True,
                )
            with b3:
                st.button("Colonnes", key="raw_cols_btn",
                          use_container_width=True, disabled=True,
                          help="Sélection de colonnes : à venir")

        st.dataframe(df, use_container_width=True, hide_index=True, height=600)
