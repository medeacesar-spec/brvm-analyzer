"""Labels de session BRVM — source unique de vérité pour l'affichage.

Résout le bug 'Clôture veille' figé : le session_kind persisté au scrape
reste obsolète après minuit. Ici on recalcule à chaque render depuis
session_date vs today (TZ Africa/Abidjan).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Africa/Abidjan")

_JOURS_FR = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
_MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
            "juillet", "août", "septembre", "octobre", "novembre", "décembre"]


@dataclass
class SessionLabel:
    status: str       # "Mi-séance 12:11" | "Clôture" | "Clôture veille" | "Clôture du 17 avril"
    date_long: str    # "Mercredi 22 avril 2026"
    date_short: str   # "22/04"
    sidebar: str      # "Clôture · màj 16:40"
    caption: str      # "Clôture · Mercredi 22 avril 2026"
    day_tab: str      # "Jour · 22/04"


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_is_open(v) -> Optional[bool]:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "open")


def _format_update_time(iso: Optional[str], now: datetime) -> str:
    """'HH:MM' si la màj est aujourd'hui, 'DD/MM HH:MM' sinon."""
    if not iso:
        return ""
    raw = str(iso).replace("T", " ")
    dt = None
    for length, fmt in ((19, "%Y-%m-%d %H:%M:%S"), (16, "%Y-%m-%d %H:%M")):
        try:
            dt = datetime.strptime(raw[:length], fmt)
            break
        except Exception:
            continue
    if dt is None:
        return raw[:16]
    if dt.date() == now.date():
        return dt.strftime("%H:%M")
    return dt.strftime("%d/%m %H:%M")


def build_session_label(
    session_date: Optional[str],
    session_time: Optional[str],
    is_open: Optional[object] = None,
    last_update_iso: Optional[str] = None,
    now: Optional[datetime] = None,
) -> SessionLabel:
    """Construit les labels d'affichage pour une session BRVM.

    La règle 'veille' est purement calendaire (TZ Abidjan) :
      - date == today      → "Clôture" (ou "Mi-séance" si is_open)
      - date == today - 1  → "Clôture veille"
      - date <  today - 1  → "Clôture du DD mois"  (évite 'veille' le lundi
                              quand la dernière séance est vendredi)
    """
    if now is None:
        now = datetime.now(_TZ)
    sdate = _parse_date(session_date)
    open_flag = _parse_is_open(is_open)
    stime = (session_time or "").strip()

    if sdate is None:
        return SessionLabel(
            status="—",
            date_long="—",
            date_short="—",
            sidebar="Données indisponibles",
            caption="Bourse régionale des valeurs mobilières · 48 titres suivis",
            day_tab="Jour",
        )

    today = now.date()
    days_ago = (today - sdate).days
    jour = _JOURS_FR[sdate.weekday()].capitalize()
    date_long = f"{jour} {sdate.day} {_MOIS_FR[sdate.month - 1]} {sdate.year}"
    date_short = f"{sdate.day:02d}/{sdate.month:02d}"

    if open_flag and sdate == today and stime:
        status = f"Mi-séance {stime}"
        caption = f"Mi-séance · {date_long} · relevé {stime}"
    elif days_ago <= 0:
        status = "Clôture"
        caption = f"Clôture · {date_long}"
    elif days_ago == 1:
        status = "Clôture veille"
        caption = f"Clôture veille · {date_long}"
    else:
        status = f"Clôture du {sdate.day} {_MOIS_FR[sdate.month - 1]}"
        caption = f"Clôture · {date_long}"

    update = _format_update_time(last_update_iso, now)
    sidebar = f"{status} · màj {update}" if update else status
    day_tab = f"Jour · {date_short}"

    return SessionLabel(
        status=status,
        date_long=date_long,
        date_short=date_short,
        sidebar=sidebar,
        caption=caption,
        day_tab=day_tab,
    )
