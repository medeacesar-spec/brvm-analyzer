"""Analyses rétrospectives basées sur le journal `verdict_daily`.

`verdict_daily` est un append-only journal écrit chaque jour par
build_daily_snapshot. Il permet :
  - score_evolution(ticker) : trajectoire des scores sur N jours
  - get_current_cohort(verdict) : tickers actuellement classés `verdict`
                                  avec date d'entrée, durée, perf depuis
  - get_trajectories() : chaînes ACHAT FORT → ACHAT (gains par phase)
  - compute_verdict_performance(verdict, H) : perf moyenne/médiane à H
                                              jours après une nouvelle reco

NOTE : ces analyses ne fonctionnent que pour les jours où `verdict_daily`
a été peuplé. Avant le démarrage de la collecte, les pages doivent
afficher un message "données en cours de collecte".
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from data.db import read_sql_df


# Verdicts considérés comme "trajectoire d'achat" — utilisés par get_trajectories.
TRAJECTORY_VERDICTS = ("ACHAT FORT", "ACHAT")


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _date_n_days_ago(n: int) -> str:
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")


def has_history(min_rows: int = 1) -> bool:
    """Renvoie True si verdict_daily contient au moins `min_rows` lignes.
    Utile pour afficher un état 'collecte en cours' dans l'UI."""
    try:
        df = read_sql_df("SELECT COUNT(*) AS n FROM verdict_daily")
        return not df.empty and int(df.iloc[0]["n"]) >= min_rows
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────
# 1. Score evolution (un ticker)
# ─────────────────────────────────────────────────────────────────────

def get_score_evolution(ticker: str, days: int = 90) -> pd.DataFrame:
    """Retourne l'historique des scores et verdicts sur les N derniers jours.

    Colonnes : date, verdict, stars, hybrid_score, fundamental_score,
               technical_score, price, trend, nb_signals.
    """
    df = read_sql_df(
        """SELECT date, verdict, stars, hybrid_score, fundamental_score,
                  technical_score, price, trend, nb_signals
           FROM verdict_daily
           WHERE ticker = ? AND date >= ?
           ORDER BY date""",
        (ticker, _date_n_days_ago(days)),
        parse_dates=["date"],
    )
    return df


# ─────────────────────────────────────────────────────────────────────
# 2. Cohorte actuelle (tickers dans un verdict aujourd'hui)
# ─────────────────────────────────────────────────────────────────────

def get_current_cohort(verdict: str = "ACHAT FORT") -> pd.DataFrame:
    """Liste des tickers dont LE DERNIER verdict observé == `verdict`,
    avec :
      - entry_date : date du début du streak courant pour ce verdict
      - days_in : nombre de jours dans ce verdict
      - entry_price : prix au début du streak
      - current_price : prix le plus récent
      - perf_pct : performance depuis l'entrée
      - hybrid_score : score actuel

    Tri : perf_pct décroissant.
    """
    # Charge tout l'historique pour calculer les streaks proprement
    full = read_sql_df(
        """SELECT ticker, company_name, sector, date, verdict, hybrid_score, price
           FROM verdict_daily
           ORDER BY ticker, date"""
    )
    if full.empty:
        return pd.DataFrame()

    # Pour chaque ticker, trouver son verdict le plus récent
    last_per_ticker = full.sort_values("date").groupby("ticker").tail(1)
    in_cohort = last_per_ticker[last_per_ticker["verdict"] == verdict]
    if in_cohort.empty:
        return pd.DataFrame()

    rows = []
    for _, current_row in in_cohort.iterrows():
        ticker = current_row["ticker"]
        ticker_hist = full[full["ticker"] == ticker].sort_values("date")
        # Trouve le début du streak : remonte tant que verdict == cible
        streak_start_idx = None
        for i in range(len(ticker_hist) - 1, -1, -1):
            if ticker_hist.iloc[i]["verdict"] == verdict:
                streak_start_idx = i
            else:
                break
        if streak_start_idx is None:
            continue
        streak_start = ticker_hist.iloc[streak_start_idx]
        days_in = (
            pd.to_datetime(current_row["date"])
            - pd.to_datetime(streak_start["date"])
        ).days + 1
        entry_price = streak_start.get("price") or 0
        current_price = current_row.get("price") or 0
        perf_pct = (
            (current_price - entry_price) / entry_price * 100
            if entry_price and entry_price > 0 else None
        )
        rows.append({
            "ticker": ticker,
            "company_name": current_row.get("company_name") or ticker,
            "sector": current_row.get("sector") or "",
            "entry_date": streak_start["date"],
            "days_in": days_in,
            "entry_price": entry_price,
            "current_price": current_price,
            "perf_pct": perf_pct,
            "hybrid_score": current_row.get("hybrid_score"),
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values("perf_pct", ascending=False, na_position="last")
    return out


# ─────────────────────────────────────────────────────────────────────
# 3. Trajectoires ACHAT FORT → ACHAT
# ─────────────────────────────────────────────────────────────────────

def _detect_trajectories_for_ticker(hist: pd.DataFrame) -> list:
    """Découpe l'historique d'un ticker en trajectoires.

    Une trajectoire = suite consécutive de jours où verdict ∈ {ACHAT FORT, ACHAT}.
    Elle s'arrête quand le verdict sort de cet ensemble OU à la fin de l'historique
    (auquel cas la trajectoire est `active`).
    """
    if hist.empty:
        return []
    hist = hist.sort_values("date").reset_index(drop=True)
    trajectories = []
    current = None
    for _, row in hist.iterrows():
        in_trajectory = row["verdict"] in TRAJECTORY_VERDICTS
        if in_trajectory:
            if current is None:
                current = {
                    "ticker": row["ticker"],
                    "company_name": row.get("company_name") or row["ticker"],
                    "sector": row.get("sector") or "",
                    "phases": [],  # liste de (verdict, date_in, price_in)
                }
            # Détecte le début d'une nouvelle phase si verdict change
            if not current["phases"] or current["phases"][-1]["verdict"] != row["verdict"]:
                current["phases"].append({
                    "verdict": row["verdict"],
                    "date_start": row["date"],
                    "price_start": row.get("price") or 0,
                })
            # Met à jour la fin de la dernière phase
            current["phases"][-1]["date_end"] = row["date"]
            current["phases"][-1]["price_end"] = row.get("price") or 0
        else:
            if current is not None:
                current["closed"] = True
                current["close_date"] = row["date"]
                current["close_verdict"] = row["verdict"]
                current["close_price"] = row.get("price") or 0
                trajectories.append(current)
                current = None
    # Trajectoire en cours à la fin
    if current is not None:
        current["closed"] = False
        trajectories.append(current)
    return trajectories


def get_trajectories(active: Optional[bool] = None) -> pd.DataFrame:
    """Toutes les trajectoires détectées dans verdict_daily.

    Une trajectoire = enchaînement de jours en ACHAT FORT et/ou ACHAT.
    Sort à chaque changement vers un autre verdict.

    `active=True` ne renvoie que les trajectoires en cours, `False` que les
    terminées, `None` les deux.

    Colonnes : ticker, company_name, sector, status, start_date, end_date,
               duration_days, gain_achat_fort_pct, gain_achat_pct, gain_total_pct,
               entry_price, exit_price.
    """
    full = read_sql_df(
        """SELECT ticker, company_name, sector, date, verdict, price
           FROM verdict_daily
           ORDER BY ticker, date"""
    )
    if full.empty:
        return pd.DataFrame()

    rows = []
    for ticker, hist in full.groupby("ticker"):
        for traj in _detect_trajectories_for_ticker(hist):
            phases = traj["phases"]
            if not phases:
                continue
            entry_price = phases[0]["price_start"] or 0
            if traj.get("closed"):
                exit_price = traj.get("close_price") or phases[-1]["price_end"] or 0
                end_date = traj["close_date"]
                status = "terminee"
            else:
                exit_price = phases[-1]["price_end"] or 0
                end_date = phases[-1]["date_end"]
                status = "en_cours"

            # Gains par phase (si la phase existe)
            gain_fort = None
            gain_achat = None
            for phase in phases:
                p_in = phase["price_start"] or 0
                p_out = phase["price_end"] or 0
                if not p_in:
                    continue
                gain_phase = (p_out - p_in) / p_in * 100
                if phase["verdict"] == "ACHAT FORT":
                    gain_fort = (gain_fort or 0) + gain_phase
                elif phase["verdict"] == "ACHAT":
                    gain_achat = (gain_achat or 0) + gain_phase

            gain_total = (
                (exit_price - entry_price) / entry_price * 100
                if entry_price and entry_price > 0 else None
            )
            start_date = phases[0]["date_start"]
            duration = (
                pd.to_datetime(end_date) - pd.to_datetime(start_date)
            ).days + 1

            rows.append({
                "ticker": ticker,
                "company_name": traj["company_name"],
                "sector": traj["sector"],
                "status": status,
                "start_date": start_date,
                "end_date": end_date,
                "duration_days": duration,
                "gain_achat_fort_pct": gain_fort,
                "gain_achat_pct": gain_achat,
                "gain_total_pct": gain_total,
                "entry_price": entry_price,
                "exit_price": exit_price,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if active is True:
        df = df[df["status"] == "en_cours"]
    elif active is False:
        df = df[df["status"] == "terminee"]
    return df.sort_values("gain_total_pct", ascending=False, na_position="last")


# ─────────────────────────────────────────────────────────────────────
# 4. Backtest : performance moyenne par verdict à H jours
# ─────────────────────────────────────────────────────────────────────

def compute_verdict_performance(
    verdict: str = "ACHAT FORT",
    horizon_days: int = 30,
) -> dict:
    """Pour chaque ENTRÉE en `verdict` (transition d'un autre verdict vers
    celui-ci), calcule la perf à `horizon_days` jours après.

    Retourne {
        "n_entries": int,           # nombre d'entrées qualifiantes
        "n_evaluated": int,         # entrées avec horizon complet (assez de données)
        "mean_pct": float | None,
        "median_pct": float | None,
        "hit_rate_pct": float | None,  # % d'entrées avec gain > 0
        "details": DataFrame,       # par entrée : ticker, entry_date, entry_price,
                                    #             exit_date, exit_price, gain_pct
    }
    """
    full = read_sql_df(
        """SELECT ticker, date, verdict, price
           FROM verdict_daily
           ORDER BY ticker, date"""
    )
    if full.empty:
        return {"n_entries": 0, "n_evaluated": 0, "mean_pct": None,
                "median_pct": None, "hit_rate_pct": None,
                "details": pd.DataFrame()}

    # Détecte les ENTRÉES = transitions vers `verdict`
    full = full.sort_values(["ticker", "date"]).reset_index(drop=True)
    full["prev_verdict"] = full.groupby("ticker")["verdict"].shift(1)
    entries = full[
        (full["verdict"] == verdict)
        & (full["prev_verdict"] != verdict)
        & (full["price"] > 0)
    ].copy()

    if entries.empty:
        return {"n_entries": 0, "n_evaluated": 0, "mean_pct": None,
                "median_pct": None, "hit_rate_pct": None,
                "details": pd.DataFrame()}

    # Pour chaque entrée, cherche le prix à entry_date + horizon (ou plus
    # récent disponible si l'horizon n'est pas atteint).
    # On utilise verdict_daily.price, mais on pourrait aussi joindre price_cache.
    details = []
    for _, e in entries.iterrows():
        ticker = e["ticker"]
        entry_date = pd.to_datetime(e["date"])
        entry_price = e["price"]
        target_date = entry_date + timedelta(days=horizon_days)
        # Cherche le prix le plus proche AVANT ou égal à target_date
        ticker_hist = full[full["ticker"] == ticker].copy()
        ticker_hist["dt"] = pd.to_datetime(ticker_hist["date"])
        candidates = ticker_hist[ticker_hist["dt"] <= target_date]
        candidates = candidates[candidates["dt"] >= entry_date]
        if candidates.empty:
            continue
        # Prend la dernière dispo (la plus proche de target_date sans dépasser)
        exit_row = candidates.sort_values("dt").iloc[-1]
        exit_price = exit_row["price"] or 0
        if exit_price == 0:
            continue
        gain = (exit_price - entry_price) / entry_price * 100
        days_observed = (pd.to_datetime(exit_row["date"]) - entry_date).days
        details.append({
            "ticker": ticker,
            "entry_date": e["date"],
            "entry_price": entry_price,
            "exit_date": exit_row["date"],
            "exit_price": exit_price,
            "days_observed": days_observed,
            "horizon_complete": days_observed >= horizon_days,
            "gain_pct": gain,
        })

    detail_df = pd.DataFrame(details)
    n_entries = len(entries)
    if detail_df.empty:
        return {"n_entries": n_entries, "n_evaluated": 0,
                "mean_pct": None, "median_pct": None, "hit_rate_pct": None,
                "details": detail_df}

    # On rapporte les stats sur les entrées avec horizon complet, mais on
    # garde aussi les partielles dans les détails (transparence).
    complete = detail_df[detail_df["horizon_complete"]]
    if complete.empty:
        return {"n_entries": n_entries, "n_evaluated": 0,
                "mean_pct": None, "median_pct": None, "hit_rate_pct": None,
                "details": detail_df}
    mean_pct = float(complete["gain_pct"].mean())
    median_pct = float(complete["gain_pct"].median())
    hit_rate = float((complete["gain_pct"] > 0).mean() * 100)
    return {
        "n_entries": n_entries,
        "n_evaluated": len(complete),
        "mean_pct": mean_pct,
        "median_pct": median_pct,
        "hit_rate_pct": hit_rate,
        "details": detail_df,
    }
