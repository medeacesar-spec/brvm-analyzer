"""
Calibration des signaux et recommandations à partir de l'historique.

Pour chaque type de signal (famille × direction) et chaque verdict, on calcule:
- Taux de succès (prix évolue dans le sens attendu à 3M)
- Performance moyenne à 3M
- Un poids [0.5 .. 1.5] dérivé du succès
- Nombre d'échantillons

Le modèle n'active les poids qu'après MIN_DAYS_HISTORY jours d'historique.
Avant ce seuil, un poids neutre de 1.0 est utilisé (fallback = comportement par défaut).
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


# Seuil minimal d'historique avant d'utiliser les poids calibrés
MIN_DAYS_HISTORY = 30
# Nombre minimal d'échantillons pour considérer un signal "calibré"
MIN_SAMPLES = 5
# Horizon de référence pour l'évaluation (3 mois)
REFERENCE_HORIZON = "perf_3m"
# Bornes du poids multiplicateur
WEIGHT_MIN = 0.5
WEIGHT_MAX = 1.5
# Poids neutre (pas encore calibré)
NEUTRAL_WEIGHT = 1.0


def _has_enough_history() -> bool:
    """Vérifie si l'historique couvre au moins MIN_DAYS_HISTORY jours."""
    from data.storage import get_signal_history
    df = get_signal_history()
    if df.empty:
        return False
    oldest = pd.to_datetime(df["first_seen_date"], errors="coerce").min()
    if pd.isna(oldest):
        return False
    age_days = (datetime.now() - oldest.to_pydatetime()).days
    return age_days >= MIN_DAYS_HISTORY


def _success_to_weight(success_rate: float, direction: str) -> float:
    """Convertit un taux de succès en poids multiplicateur.
    Pour 'achat' : success = prix monte. Pour 'vente' : success = prix baisse.
    Un signal à 50% (random) = poids 1.0 (neutre).
    Un signal à 100% = poids WEIGHT_MAX. À 0% = poids WEIGHT_MIN."""
    # Linear mapping: 0% → WEIGHT_MIN, 50% → 1.0, 100% → WEIGHT_MAX
    if success_rate >= 0.5:
        return 1.0 + (success_rate - 0.5) * 2 * (WEIGHT_MAX - 1.0)
    return 1.0 - (0.5 - success_rate) * 2 * (1.0 - WEIGHT_MIN)


def compute_signal_calibration() -> dict:
    """Analyse l'historique des signaux et calcule les poids calibrés par nom de signal.

    Returns:
        {
            "enabled": bool,   # True si l'historique est assez long
            "min_days": int,   # seuil requis
            "days_available": int,
            "signals": {
                "Golden Cross (MM50/MM200)": {
                    "direction": "achat",
                    "n_samples": 12,
                    "success_rate": 0.67,
                    "avg_return": 0.085,
                    "weight": 1.24,
                    "calibrated": True,
                },
                ...
            },
            "recommendations": {
                "ACHAT FORT CONFIRMÉ": { same shape },
                ...
            },
        }
    """
    from data.storage import get_signal_history, compute_signal_performance
    from data.db import read_sql_df

    result = {
        "enabled": False,
        "min_days": MIN_DAYS_HISTORY,
        "days_available": 0,
        "signals": {},
        "recommendations": {},
    }

    df = get_signal_history()
    if df.empty:
        return result

    oldest = pd.to_datetime(df["first_seen_date"], errors="coerce").min()
    if not pd.isna(oldest):
        result["days_available"] = (datetime.now() - oldest.to_pydatetime()).days

    result["enabled"] = result["days_available"] >= MIN_DAYS_HISTORY

    # Optim : lire la performance depuis signal_performance_snapshot (rempli
    # quotidiennement par le cron) au lieu de recalculer via
    # compute_signal_performance (N+1 = 756 round-trips Supabase = 60+ sec).
    try:
        snap = read_sql_df(
            "SELECT event_id, current_price, perf_1m, perf_3m, perf_6m, "
            "perf_1a, perf_since_start FROM signal_performance_snapshot"
        )
    except Exception:
        snap = pd.DataFrame()

    if not snap.empty and "id" in df.columns:
        df = df.merge(snap, how="left", left_on="id", right_on="event_id")
        if "event_id" in df.columns:
            df = df.drop(columns=["event_id"])
    else:
        # Fallback : calcul live (lent) si le snapshot n'existe pas encore
        df = compute_signal_performance(df)

    # --- Signals ---
    sig_df = df[df["entry_type"] == "signal"].copy()
    if not sig_df.empty:
        for (signal_name, signal_type), grp in sig_df.groupby(["signal_name", "signal_type"]):
            perf_vals = grp[REFERENCE_HORIZON].dropna()
            n = len(perf_vals)

            if signal_type == "achat":
                n_success = int((perf_vals > 0).sum())
            elif signal_type == "vente":
                n_success = int((perf_vals < 0).sum())
            else:
                continue

            success_rate = n_success / n if n > 0 else None
            avg_ret = float(perf_vals.mean()) if n > 0 else None
            calibrated = result["enabled"] and n >= MIN_SAMPLES

            if calibrated and success_rate is not None:
                weight = _success_to_weight(success_rate, signal_type)
                weight = max(WEIGHT_MIN, min(WEIGHT_MAX, weight))
            else:
                weight = NEUTRAL_WEIGHT

            result["signals"][signal_name] = {
                "direction": signal_type,
                "n_samples": n,
                "n_with_perf": n,
                "success_rate": success_rate,
                "avg_return": avg_ret,
                "weight": weight,
                "calibrated": calibrated,
            }

    # --- Recommendations (grouped by verdict) ---
    reco_df = df[df["entry_type"] == "recommendation"].copy()
    if not reco_df.empty:
        for verdict, grp in reco_df.groupby("verdict"):
            if not verdict:
                continue
            perf_vals = grp[REFERENCE_HORIZON].dropna()
            n = len(perf_vals)

            v_up = verdict.upper()
            if "ACHAT" in v_up:
                n_success = int((perf_vals > 0).sum())
                direction = "achat"
            elif "VENTE" in v_up or "EVITER" in v_up or "ÉVITER" in v_up:
                n_success = int((perf_vals < 0).sum())
                direction = "vente"
            else:
                direction = "neutre"
                n_success = 0

            success_rate = n_success / n if n > 0 and direction != "neutre" else None
            avg_ret = float(perf_vals.mean()) if n > 0 else None
            calibrated = result["enabled"] and n >= MIN_SAMPLES

            if calibrated and success_rate is not None:
                weight = _success_to_weight(success_rate, direction)
                weight = max(WEIGHT_MIN, min(WEIGHT_MAX, weight))
            else:
                weight = NEUTRAL_WEIGHT

            result["recommendations"][verdict] = {
                "direction": direction,
                "n_samples": n,
                "success_rate": success_rate,
                "avg_return": avg_ret,
                "weight": weight,
                "calibrated": calibrated,
            }

    return result


# --- Monthly review mechanism ---

REVIEW_INTERVAL_DAYS = 30


def _get_last_review_date() -> Optional[datetime]:
    """Date du dernier enregistrement de revue de calibration."""
    from data.storage import get_connection
    conn = get_connection()
    row = conn.execute(
        "SELECT review_date FROM calibration_reviews ORDER BY review_date DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        return None
    try:
        return datetime.strptime(row[0], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def is_review_due() -> bool:
    """Indique si une revue mensuelle est à faire."""
    last = _get_last_review_date()
    if last is None:
        # No review ever done: only "due" if calibration is enabled
        cal = get_calibration()
        return cal.get("enabled", False)
    return (datetime.now() - last).days >= REVIEW_INTERVAL_DAYS


def next_review_date() -> Optional[datetime]:
    """Date prévue de la prochaine revue (dernière + 30j), ou None si jamais faite."""
    last = _get_last_review_date()
    if last is None:
        return None
    return last + timedelta(days=REVIEW_INTERVAL_DAYS)


def run_monthly_review(notes: str = None, force: bool = False) -> dict:
    """Exécute une revue mensuelle des poids de calibration :
    1. Recalcule les poids depuis l'historique actuel
    2. Stocke un snapshot dans `calibration_reviews`
    3. Invalide le cache pour que la nouvelle calibration soit appliquée immédiatement

    Retourne un résumé de la revue (compte des éléments calibrés).
    Si `force=False` et revue pas encore due, retourne dict avec skipped=True."""
    import json
    from data.storage import get_connection

    if not force and not is_review_due():
        last = _get_last_review_date()
        next_dt = next_review_date()
        return {
            "skipped": True,
            "reason": "Revue non due",
            "last_review": last.strftime("%Y-%m-%d") if last else None,
            "next_review": next_dt.strftime("%Y-%m-%d") if next_dt else None,
        }

    # Force-refresh calibration
    cal = get_calibration(force_refresh=True)

    n_signals = len(cal.get("signals", {}))
    n_recos = len(cal.get("recommendations", {}))
    calibrated_sig = sum(
        1 for info in cal.get("signals", {}).values() if info.get("calibrated")
    )
    calibrated_reco = sum(
        1 for info in cal.get("recommendations", {}).values() if info.get("calibrated")
    )

    # Serialize snapshot (strip non-JSON-safe types)
    payload = {
        "enabled": cal.get("enabled"),
        "days_available": cal.get("days_available"),
        "min_days": cal.get("min_days"),
        "signals": cal.get("signals", {}),
        "recommendations": cal.get("recommendations", {}),
    }

    conn = get_connection()
    conn.execute(
        """INSERT INTO calibration_reviews
           (review_date, days_available, n_signals, n_recos,
            calibrated_signals, calibrated_recos, payload, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            datetime.now().strftime("%Y-%m-%d"),
            cal.get("days_available", 0),
            n_signals, n_recos, calibrated_sig, calibrated_reco,
            json.dumps(payload, default=str),
            notes,
        ),
    )
    conn.commit()
    conn.close()

    return {
        "skipped": False,
        "review_date": datetime.now().strftime("%Y-%m-%d"),
        "days_available": cal.get("days_available"),
        "enabled": cal.get("enabled"),
        "n_signals": n_signals,
        "n_recos": n_recos,
        "calibrated_signals": calibrated_sig,
        "calibrated_recos": calibrated_reco,
    }


def get_review_history() -> list:
    """Retourne toutes les revues précédentes, la plus récente en premier."""
    import json
    from data.storage import get_connection
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, review_date, days_available, n_signals, n_recos,
                  calibrated_signals, calibrated_recos, payload, notes, created_at
           FROM calibration_reviews ORDER BY review_date DESC"""
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        rec = dict(r)
        if rec.get("payload"):
            try:
                rec["payload"] = json.loads(rec["payload"])
            except (ValueError, TypeError):
                pass
        out.append(rec)
    return out


# --- Cached access (session-scoped) ---

_calibration_cache: Optional[dict] = None
_calibration_cache_ts: Optional[datetime] = None
_CACHE_TTL_MINUTES = 30


def get_calibration(force_refresh: bool = False) -> dict:
    """Retourne la calibration, en cache pendant 30 minutes pour éviter de
    recalculer à chaque rendu."""
    global _calibration_cache, _calibration_cache_ts
    now = datetime.now()
    if (not force_refresh
        and _calibration_cache is not None
        and _calibration_cache_ts is not None
        and (now - _calibration_cache_ts).total_seconds() < _CACHE_TTL_MINUTES * 60):
        return _calibration_cache
    _calibration_cache = compute_signal_calibration()
    _calibration_cache_ts = now
    return _calibration_cache


def get_signal_weight(signal_name: str, direction: str = None) -> float:
    """Retourne le poids calibré d'un signal, ou 1.0 par défaut."""
    cal = get_calibration()
    if not cal["enabled"]:
        return NEUTRAL_WEIGHT
    info = cal["signals"].get(signal_name)
    if not info:
        return NEUTRAL_WEIGHT
    return info["weight"]


def get_verdict_weight(verdict: str) -> float:
    """Retourne le poids calibré d'une recommandation."""
    cal = get_calibration()
    if not cal["enabled"]:
        return NEUTRAL_WEIGHT
    info = cal["recommendations"].get(verdict)
    if not info:
        return NEUTRAL_WEIGHT
    return info["weight"]
