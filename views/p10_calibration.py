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


def render():
    st.markdown(
        '<div class="main-header">🎯 Historique Signaux & Recommandations</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="sub-header">Suivi long terme des signaux et recommandations. '
        'Une ligne = un événement unique. Si un signal réapparaît dans les 7 jours, '
        'il met à jour son événement existant (pas de doublon). Au-delà de 7 jours '
        'sans réapparition, la prochaine occurrence sera considérée comme un nouvel événement.</div>',
        unsafe_allow_html=True,
    )

    df_all = get_signal_history()
    if df_all.empty:
        st.info(
            "Aucun signal enregistré pour l'instant. Visitez les pages "
            "**Signaux** et **Analyse d'un Titre** pour commencer à capturer des événements."
        )
        return

    # --- Stats header ---
    n_events = len(df_all)
    n_signals = len(df_all[df_all["entry_type"] == "signal"])
    n_recos = len(df_all[df_all["entry_type"] == "recommendation"])
    n_tickers = df_all["ticker"].nunique()
    oldest = df_all["first_seen_date"].min()
    newest = df_all["last_seen_date"].max()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Événements", f"{n_events:,}")
    c2.metric("Signaux uniques", f"{n_signals:,}")
    c3.metric("Recommandations", f"{n_recos:,}")
    c4.metric("Titres suivis", f"{n_tickers}")
    st.caption(f"Période : {oldest} → {newest}")

    # --- Filters ---
    st.markdown("---")
    col_f1, col_f2, col_f3 = st.columns([2, 2, 2])

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

    # Performance : lecture depuis le snapshot précalculé (plus de N+1).
    # Fallback sur compute_signal_performance live si le snapshot est vide
    # (1er lancement avant que le cron/bouton admin ne soit exécuté).
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

    # --- Add computed duration column (si pas déjà dans le snapshot) ---
    if "duration_days" not in df.columns or df["duration_days"].isna().all():
        df["duration_days"] = df.apply(
            lambda r: _duration_days(r.get("first_seen_date"), r.get("last_seen_date")),
            axis=1,
        )

    if not snapshot_used and is_admin():
        st.warning(
            "⚠️ Snapshot de performance vide. Cliquez sur **📸 Regénérer snapshots** "
            "dans la sidebar pour accélérer cette page."
        )

    # Quick jump to analysis of any ticker present in this history
    present_tickers = sorted(df["ticker"].unique().tolist())
    if present_tickers:
        picker_options = [(t, t) for t in present_tickers]
        ticker_quick_picker(
            picker_options, key="calib_goto",
            label="🔍 Ouvrir l'analyse d'un titre",
        )

    # --- Tabs ---
    tab1, tab2, tab3, tab_cal, tab4 = st.tabs([
        "📊 Vue d'ensemble", "📡 Signaux", "🎯 Recommandations",
        "⚖️ Calibration", "📋 Données brutes"
    ])

    # ============================================================
    # TAB 1 : OVERVIEW
    # ============================================================
    with tab1:
        st.subheader("Performance agrégée par type")

        sig_df = df[df["entry_type"] == "signal"].copy()

        # Buy / Sell success rates
        if not sig_df.empty:
            buy_df = sig_df[sig_df["signal_type"] == "achat"]
            sell_df = sig_df[sig_df["signal_type"] == "vente"]

            colA, colB = st.columns(2)
            with colA:
                st.markdown("#### 🟢 Signaux d'achat")
                st.caption("Un signal d'achat \"réussit\" si le prix monte depuis la 1re apparition")
                for horizon, label in [("perf_1m", "1M"), ("perf_3m", "3M"),
                                        ("perf_6m", "6M"), ("perf_1a", "1A"),
                                        ("perf_since_start", "Depuis début")]:
                    vals = buy_df[horizon].dropna()
                    if len(vals) > 0:
                        success = (vals > 0).sum() / len(vals)
                        avg = vals.mean()
                        st.metric(
                            f"Horizon {label}",
                            f"{success:.0%}",
                            f"{_fmt_pct(avg)} moy. ({len(vals)} sig.)",
                        )

            with colB:
                st.markdown("#### 🔴 Signaux de vente")
                st.caption("Un signal de vente \"réussit\" si le prix baisse depuis la 1re apparition")
                for horizon, label in [("perf_1m", "1M"), ("perf_3m", "3M"),
                                        ("perf_6m", "6M"), ("perf_1a", "1A"),
                                        ("perf_since_start", "Depuis début")]:
                    vals = sell_df[horizon].dropna()
                    if len(vals) > 0:
                        success = (vals < 0).sum() / len(vals)
                        avg = vals.mean()
                        st.metric(
                            f"Horizon {label}",
                            f"{success:.0%}",
                            f"{_fmt_pct(avg)} moy. ({len(vals)} sig.)",
                        )

        # Performance by signal name
        st.markdown("---")
        st.markdown("### Détail par type de signal")
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
            agg["force_moy"] = agg["force_moy"].round(1)
            agg["occ_moy"] = agg["occ_moy"].round(1)
            for c in ["perf_1m", "perf_3m", "perf_6m", "perf_1a", "perf_start"]:
                agg[c] = agg[c].apply(_fmt_pct)

            st.dataframe(
                agg.rename(columns={
                    "signal_type": "Type", "signal_name": "Signal",
                    "nb": "Nb événements", "force_moy": "Force moy.",
                    "occ_moy": "Occ. moy.",
                    "perf_1m": "Perf. 1M",
                    "perf_3m": "Perf. 3M",
                    "perf_6m": "Perf. 6M",
                    "perf_1a": "Perf. 1A",
                    "perf_start": "Perf. depuis début",
                }),
                use_container_width=True, hide_index=True,
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

            st.subheader(f"Événements de signaux ({len(sig_df)})")
            st.caption(
                "Cocher la colonne **🗑️** pour sélectionner une ou plusieurs lignes, "
                "puis cliquer sur le bouton de suppression en bas. "
                "La colonne **⚠️ Contradiction** signale les tickers dont la même famille de signal a "
                "à la fois des signaux achat et vente actifs."
            )

            sig_df["family"] = sig_df["signal_name"].apply(_signal_family)

            # Flag contradictions per (ticker, family)
            conflict_keys = set()
            for (ticker, family), gdf in sig_df.groupby(["ticker", "family"]):
                if gdf["signal_type"].nunique() > 1:
                    conflict_keys.add((ticker, family))
            sig_df["conflict"] = sig_df.apply(
                lambda r: "⚠️" if (r["ticker"], r["family"]) in conflict_keys else "",
                axis=1,
            )

            # Icon for direction
            sig_df["dir_icon"] = sig_df["signal_type"].map({
                "achat": "🟢 Achat", "vente": "🔴 Vente", "info": "ℹ️ Info"
            }).fillna("—")

            # Build table for data_editor
            table_df = pd.DataFrame({
                "🗑️": False,
                "Ticker": sig_df["ticker"].values,
                "Nom": sig_df["company_name"].fillna(sig_df["ticker"]).values,
                "Direction": sig_df["dir_icon"].values,
                "Famille": sig_df["family"].values,
                "Signal": sig_df["signal_name"].values,
                "Force": sig_df["signal_strength"].fillna(0).astype(int).values,
                "⚠️": sig_df["conflict"].values,
                "1re apparition": sig_df["first_seen_date"].values,
                "Dernière vue": sig_df["last_seen_date"].values,
                "Durée (j)": sig_df["duration_days"].fillna(0).astype(int).values,
                "Occ.": sig_df["occurrence_count"].fillna(1).astype(int).values,
                "Prix début": sig_df["price_at_start"].apply(_fmt_num).values,
                "Prix actuel": sig_df["current_price"].apply(_fmt_num).values,
                "Perf. depuis début": sig_df["perf_since_start"].apply(_fmt_pct).values,
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
                    "🗑️": st.column_config.CheckboxColumn(
                        "🗑️", help="Cocher pour supprimer", default=False, width="small",
                    ),
                    "_id": None,  # hidden
                    "Détails": st.column_config.TextColumn(width="medium"),
                },
                disabled=[c for c in table_df.columns if c != "🗑️"],
            )

            ids_to_delete = edited.loc[edited["🗑️"] == True, "_id"].astype(int).tolist()

            if ids_to_delete:
                st.markdown("---")
                col_b1, col_b2 = st.columns([1, 3])
                with col_b1:
                    if st.button(
                        f"🗑️ Supprimer {len(ids_to_delete)} événement(s)",
                        type="primary",
                        key="bulk_delete_signals_btn",
                    ):
                        n = delete_signal_events_bulk(ids_to_delete)
                        st.success(f"{n} événement(s) supprimé(s).")
                        st.rerun()
                with col_b2:
                    st.caption("Cette action est irréversible.")

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
            st.subheader(f"Événements de recommandations ({len(reco_df)})")
            st.caption(
                "Chaque ligne = une période pendant laquelle un titre a eu un même verdict. "
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
                st.markdown("#### Performance moyenne par verdict")
                display_v = verdict_perf.copy()
                display_v["avg_score"] = display_v["avg_score"].apply(
                    lambda x: f"{x:.0f}/100" if x and not pd.isna(x) else "—",
                )
                display_v["avg_duration"] = display_v["avg_duration"].apply(
                    lambda x: f"{x:.0f} j" if x and not pd.isna(x) else "—",
                )
                for c in ["perf_1m", "perf_3m", "perf_6m", "perf_1a", "perf_start"]:
                    display_v[c] = display_v[c].apply(_fmt_pct)

                st.dataframe(
                    display_v.rename(columns={
                        "verdict": "Verdict", "nb": "Nb événements",
                        "avg_score": "Score moy.",
                        "avg_duration": "Durée moy.",
                        "perf_1m": "Perf. 1M", "perf_3m": "Perf. 3M",
                        "perf_6m": "Perf. 6M", "perf_1a": "Perf. 1A",
                        "perf_start": "Perf. depuis début",
                    }),
                    use_container_width=True, hide_index=True,
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

            st.markdown("---")
            st.markdown("#### Liste des recommandations")
            st.caption("Cocher la colonne 🗑️ pour sélectionner des lignes à supprimer.")

            reco_table = pd.DataFrame({
                "🗑️": False,
                "Ticker": reco_df["ticker"].values,
                "Nom": reco_df["company_name"].fillna(reco_df["ticker"]).values,
                "Verdict": reco_df["verdict"].fillna("—").values,
                "Score": reco_df["hybrid_score"].apply(
                    lambda x: f"{x:.0f}/100" if x and not pd.isna(x) else "—"
                ).values,
                "★": reco_df["stars"].fillna(0).astype(int).values,
                "Tendance": reco_df["trend"].fillna("—").values,
                "1re apparition": reco_df["first_seen_date"].values,
                "Dernière vue": reco_df["last_seen_date"].values,
                "Durée (j)": reco_df["duration_days"].fillna(0).astype(int).values,
                "Occ.": reco_df["occurrence_count"].fillna(1).astype(int).values,
                "Prix début": reco_df["price_at_start"].apply(_fmt_num).values,
                "Prix actuel": reco_df["current_price"].apply(_fmt_num).values,
                "Perf. depuis début": reco_df["perf_since_start"].apply(_fmt_pct).values,
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
                    "🗑️": st.column_config.CheckboxColumn(
                        "🗑️", help="Cocher pour supprimer", default=False, width="small",
                    ),
                    "_id": None,
                },
                disabled=[c for c in reco_table.columns if c != "🗑️"],
            )

            reco_ids_to_delete = edited_reco.loc[
                edited_reco["🗑️"] == True, "_id"
            ].astype(int).tolist()

            if reco_ids_to_delete:
                st.markdown("---")
                col_b1, col_b2 = st.columns([1, 3])
                with col_b1:
                    if st.button(
                        f"🗑️ Supprimer {len(reco_ids_to_delete)} recommandation(s)",
                        type="primary",
                        key="bulk_delete_recos_btn",
                    ):
                        n = delete_signal_events_bulk(reco_ids_to_delete)
                        st.success(f"{n} recommandation(s) supprimée(s).")
                        st.rerun()
                with col_b2:
                    st.caption("Cette action est irréversible.")

    # ============================================================
    # TAB CAL : CALIBRATION (poids appliqués aux signaux/recommandations)
    # ============================================================
    with tab_cal:
        from analysis.calibration import (
            get_calibration, MIN_DAYS_HISTORY, MIN_SAMPLES,
            WEIGHT_MIN, WEIGHT_MAX, REFERENCE_HORIZON,
            is_review_due, next_review_date, run_monthly_review,
            get_review_history, _get_last_review_date, REVIEW_INTERVAL_DAYS,
        )

        st.subheader("⚖️ Calibration des signaux et recommandations")
        st.caption(
            f"Pondération dérivée de la performance historique à 3 mois. "
            f"Active dès que l'historique couvre au moins **{MIN_DAYS_HISTORY} jours** "
            f"et qu'un signal a ≥ **{MIN_SAMPLES} échantillons**. "
            f"Poids dans [{WEIGHT_MIN}, {WEIGHT_MAX}] : multiplié à la force brute du signal."
        )

        # force_refresh=False : on respecte le cache module (30 min). Le calcul
        # complet est couteux (appelle compute_signal_performance = N+1 queries).
        # Le snapshot quotidien GitHub Actions ne touche pas ce cache, mais celui-ci
        # se regenerera tout seul au bout de 30 min, ou sur clic "Lancer la revue".
        cal = get_calibration(force_refresh=False)

        # Status header
        days_available = cal.get("days_available", 0)
        enabled = cal.get("enabled", False)
        col_s1, col_s2, col_s3 = st.columns(3)
        col_s1.metric("Historique", f"{days_available} jours")
        col_s2.metric("Seuil activation", f"{MIN_DAYS_HISTORY} jours")
        col_s3.metric("Statut", "🟢 Actif" if enabled else "⏸️ En attente")

        if not enabled:
            progress = min(1.0, days_available / MIN_DAYS_HISTORY)
            st.progress(
                progress,
                text=f"Progression : {days_available}/{MIN_DAYS_HISTORY} jours "
                     f"({int(progress*100)}%). Poids neutres (1.0) appliqués d'ici là.",
            )

        # --- Monthly review controls ---
        st.markdown("---")
        st.markdown(f"### 🗓️ Revue mensuelle (tous les {REVIEW_INTERVAL_DAYS} jours)")
        last_review = _get_last_review_date()
        next_review = next_review_date()
        due = is_review_due()

        col_r1, col_r2, col_r3 = st.columns(3)
        col_r1.metric(
            "Dernière revue",
            last_review.strftime("%d/%m/%Y") if last_review else "— Jamais",
        )
        col_r2.metric(
            "Prochaine revue",
            next_review.strftime("%d/%m/%Y") if next_review else "— (à la 1re)",
        )
        col_r3.metric("Statut", "⚠️ À faire" if due else "✅ À jour")

        col_b1, col_b2 = st.columns([1, 2])
        with col_b1:
            if is_admin():
                if st.button(
                    "🔄 Lancer la revue maintenant",
                    type="primary" if due else "secondary",
                    use_container_width=True,
                ):
                    result = run_monthly_review(force=True)
                    if result.get("skipped"):
                        st.warning(result.get("reason", "Non fait"))
                    else:
                        st.success(
                            f"✅ Revue enregistrée : {result['calibrated_signals']} signaux "
                            f"et {result['calibrated_recos']} verdicts calibrés "
                            f"(historique {result['days_available']}j)."
                        )
                    st.rerun()
            else:
                st.caption("🔒 Action admin")
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
            st.markdown("#### Historique des revues")
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

            # Evolution chart: show how each signal weight changed over reviews
            if len(history) >= 2:
                with st.expander("📈 Évolution des poids entre revues", expanded=False):
                    # Pick top signals by total samples in latest review
                    latest_signals = history[0].get("payload", {}).get("signals", {})
                    tracked = sorted(
                        latest_signals.keys(),
                        key=lambda k: -(latest_signals[k].get("n_samples") or 0),
                    )[:6]

                    import plotly.graph_objects as go
                    fig = go.Figure()
                    for sig_name in tracked:
                        dates = []
                        weights = []
                        for rev in reversed(history):  # chronological
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

        # Signals table
        st.markdown("---")
        st.markdown("### 📡 Poids par type de signal")
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
                "Calibré": "✅" if info.get("calibrated") else "⏸️",
            })
        if sig_rows:
            st.dataframe(
                pd.DataFrame(sig_rows).sort_values("Échantillons", ascending=False),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Aucun signal historique à pondérer pour l'instant.")

        # Recommendations table
        st.markdown("### 🎯 Poids par verdict de recommandation")
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
                "Calibré": "✅" if info.get("calibrated") else "⏸️",
            })
        if reco_rows:
            st.dataframe(
                pd.DataFrame(reco_rows).sort_values("Échantillons", ascending=False),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Aucune recommandation historique à pondérer.")

        with st.expander("ℹ️ Comment fonctionne la calibration ?"):
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
        st.subheader(f"Toutes les données ({len(df)} lignes)")
        st.dataframe(df, use_container_width=True, hide_index=True, height=600)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Télécharger CSV",
            data=csv,
            file_name=f"signal_events_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
