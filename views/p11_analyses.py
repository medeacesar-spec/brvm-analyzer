"""Page 11 : Analyses rétrospectives (Cohorte / Trajectoires / Backtest).

Source de données : `verdict_daily` (peuplée par build_daily_snapshot).
Tant que la collecte n'a pas démarré, les onglets affichent un état "en
cours" plutôt que des erreurs.
"""

import streamlit as st
import pandas as pd

from config import CURRENCY
from analysis.verdict_history import (
    has_history,
    get_current_cohort,
    get_trajectories,
    compute_verdict_performance,
)
from utils.nav import ticker_analyze_button
from utils.ui_helpers import section_heading


VERDICT_OPTIONS = ["ACHAT FORT", "ACHAT", "CONSERVER", "PRUDENCE", "EVITER"]


def render():
    st.title("Analyses")
    st.caption(
        "Cohortes en cours · Trajectoires Achat fort → Achat · "
        "Backtest performance par verdict"
    )

    if not has_history():
        st.info(
            "🟡 Collecte des verdicts quotidiens en cours.\n\n"
            "La table `verdict_daily` se remplit à chaque build "
            "(cron 16h UTC, lundi à vendredi). Les analyses ci-dessous "
            "s'enrichiront automatiquement chaque jour. "
            "Pour activer immédiatement la collecte, utilise le bouton admin "
            "**Regénérer snapshots** dans la sidebar."
        )

    tab_cohort, tab_traj, tab_back = st.tabs([
        "Cohorte en cours", "Trajectoires", "Backtest",
    ])

    with tab_cohort:
        _render_cohort()
    with tab_traj:
        _render_trajectories()
    with tab_back:
        _render_backtest()


# ─────────────────────────────────────────────────────────────────────
# Onglet 1 : Cohorte en cours
# ─────────────────────────────────────────────────────────────────────

def _render_cohort():
    section_heading("Tickers actuellement classés", spacing="tight")
    verdict = st.selectbox(
        "Verdict", VERDICT_OPTIONS, index=0, key="cohort_verdict",
        label_visibility="collapsed",
    )
    df = get_current_cohort(verdict)
    if df.empty:
        st.caption(f"Aucun ticker actuellement en {verdict}.")
        return

    # KPI row
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tickers en cohorte", f"{len(df)}")
    col2.metric(
        "Perf moyenne",
        f"{df['perf_pct'].mean():+.1f} %" if df["perf_pct"].notna().any() else "—",
    )
    col3.metric(
        "Perf médiane",
        f"{df['perf_pct'].median():+.1f} %" if df["perf_pct"].notna().any() else "—",
    )
    col4.metric(
        "Durée moyenne",
        f"{df['days_in'].mean():.0f} j" if not df.empty else "—",
    )

    # Tableau
    display = df.copy()
    display["entry_price"] = display["entry_price"].apply(
        lambda v: f"{v:,.0f}" if pd.notna(v) and v > 0 else "—"
    )
    display["current_price"] = display["current_price"].apply(
        lambda v: f"{v:,.0f}" if pd.notna(v) and v > 0 else "—"
    )
    display["perf_pct"] = display["perf_pct"].apply(
        lambda v: f"{v:+.1f} %" if pd.notna(v) else "—"
    )
    display["hybrid_score"] = display["hybrid_score"].apply(
        lambda v: f"{v:.0f}" if pd.notna(v) else "—"
    )
    st.dataframe(
        display.rename(columns={
            "ticker": "Ticker", "company_name": "Société", "sector": "Secteur",
            "entry_date": "Entrée", "days_in": "Durée (j)",
            "entry_price": f"Prix entrée ({CURRENCY})",
            "current_price": f"Prix actuel ({CURRENCY})",
            "perf_pct": "Perf", "hybrid_score": "Score",
        }),
        use_container_width=True, hide_index=True,
    )


# ─────────────────────────────────────────────────────────────────────
# Onglet 2 : Trajectoires
# ─────────────────────────────────────────────────────────────────────

def _render_trajectories():
    section_heading("Trajectoires Achat fort → Achat", spacing="tight")
    st.caption(
        "Une trajectoire = suite consécutive de jours en ACHAT FORT et/ou "
        "ACHAT. Elle se termine au premier verdict en dehors de cet ensemble."
    )

    col_filter, _ = st.columns([2, 5])
    status_filter = col_filter.selectbox(
        "Statut", ["Toutes", "En cours", "Terminées"], key="traj_status",
    )
    active_map = {"Toutes": None, "En cours": True, "Terminées": False}
    df = get_trajectories(active=active_map[status_filter])

    if df.empty:
        st.caption("Aucune trajectoire détectée pour le moment.")
        return

    # KPI
    n_active = len(df[df["status"] == "en_cours"])
    n_closed = len(df[df["status"] == "terminee"])
    closed_perf = df[df["status"] == "terminee"]["gain_total_pct"]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Trajectoires", f"{len(df)}")
    col2.metric("En cours", f"{n_active}")
    col3.metric("Terminées", f"{n_closed}")
    col4.metric(
        "Gain moyen (terminées)",
        f"{closed_perf.mean():+.1f} %" if not closed_perf.empty
        and closed_perf.notna().any() else "—",
    )

    # Tableau
    display = df.copy()
    display["status_emoji"] = display["status"].map({
        "en_cours": "🟢 En cours", "terminee": "✅ Terminée",
    })
    for col in ("gain_achat_fort_pct", "gain_achat_pct", "gain_total_pct"):
        display[col] = display[col].apply(
            lambda v: f"{v:+.1f} %" if pd.notna(v) else "—"
        )
    for col in ("entry_price", "exit_price"):
        display[col] = display[col].apply(
            lambda v: f"{v:,.0f}" if pd.notna(v) and v > 0 else "—"
        )
    cols_to_show = [
        "ticker", "company_name", "status_emoji", "start_date", "end_date",
        "duration_days", "gain_achat_fort_pct", "gain_achat_pct",
        "gain_total_pct", "entry_price", "exit_price",
    ]
    st.dataframe(
        display[cols_to_show].rename(columns={
            "ticker": "Ticker", "company_name": "Société",
            "status_emoji": "Statut",
            "start_date": "Début", "end_date": "Fin",
            "duration_days": "Durée (j)",
            "gain_achat_fort_pct": "Gain ACHAT FORT",
            "gain_achat_pct": "Gain ACHAT",
            "gain_total_pct": "Gain total",
            "entry_price": f"Entrée ({CURRENCY})",
            "exit_price": f"Sortie ({CURRENCY})",
        }),
        use_container_width=True, hide_index=True,
    )


# ─────────────────────────────────────────────────────────────────────
# Onglet 3 : Backtest performance
# ─────────────────────────────────────────────────────────────────────

def _render_backtest():
    section_heading(
        "Performance moyenne après une nouvelle recommandation",
        spacing="tight",
    )
    st.caption(
        "Pour chaque entrée historique en `verdict` (transition d'un autre "
        "verdict vers celui-ci), on mesure la performance du titre à H jours."
    )

    col_v, col_h = st.columns([2, 2])
    verdict = col_v.selectbox(
        "Verdict", VERDICT_OPTIONS, index=0, key="bt_verdict",
    )
    horizon = col_h.selectbox(
        "Horizon (jours)", [7, 30, 60, 90, 180], index=1, key="bt_horizon",
    )

    result = compute_verdict_performance(verdict=verdict, horizon_days=horizon)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Entrées détectées", f"{result['n_entries']}")
    col2.metric("Évaluées", f"{result['n_evaluated']}",
                help=f"Entrées avec horizon {horizon}j complet")
    col3.metric(
        "Perf moyenne",
        f"{result['mean_pct']:+.1f} %" if result["mean_pct"] is not None else "—",
    )
    col4.metric(
        "Hit rate",
        f"{result['hit_rate_pct']:.0f} %" if result["hit_rate_pct"] is not None else "—",
        help="% d'entrées avec gain > 0 à l'horizon",
    )

    if result["mean_pct"] is not None:
        st.caption(
            f"Sur {result['n_evaluated']} entrées en `{verdict}` évaluées à "
            f"+{horizon}j : moyenne **{result['mean_pct']:+.1f} %**, "
            f"médiane {result['median_pct']:+.1f} %, hit rate "
            f"{result['hit_rate_pct']:.0f} %."
        )

    details = result["details"]
    if details.empty:
        st.info(
            "Pas encore assez de données pour évaluer ce verdict à cet horizon. "
            f"Il faut au moins {horizon} jours d'historique après chaque entrée."
        )
        return

    with st.expander(f"Détail des {len(details)} entrées", expanded=False):
        d = details.copy()
        d["entry_price"] = d["entry_price"].apply(
            lambda v: f"{v:,.0f}" if pd.notna(v) else "—"
        )
        d["exit_price"] = d["exit_price"].apply(
            lambda v: f"{v:,.0f}" if pd.notna(v) else "—"
        )
        d["gain_pct"] = d["gain_pct"].apply(
            lambda v: f"{v:+.1f} %" if pd.notna(v) else "—"
        )
        d["horizon_complete"] = d["horizon_complete"].map(
            {True: "Oui", False: f"Partiel ({horizon}j non atteint)"}
        )
        st.dataframe(
            d.rename(columns={
                "ticker": "Ticker",
                "entry_date": "Date entrée",
                "entry_price": f"Prix entrée ({CURRENCY})",
                "exit_date": "Date sortie",
                "exit_price": f"Prix sortie ({CURRENCY})",
                "days_observed": "Jours observés",
                "horizon_complete": "Horizon complet",
                "gain_pct": "Gain",
            }),
            use_container_width=True, hide_index=True,
        )
