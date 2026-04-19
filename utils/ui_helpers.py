"""
BRVM Analyzer — UI helpers pour Streamlit.
À importer : from utils.ui_helpers import kpi_card, delta, tag, ticker, flag_dot
"""
from typing import Optional
import streamlit as st


def kpi_card(label: str, value, unit: str = "", delta_pct: Optional[float] = None,
             sub: str = "", tone: str = "neutral"):
    """
    KPI card avec accent latéral coloré.
    tone: 'up' (vert), 'down' (rouge), 'neutral' (gris).
    Alternative à st.metric si tu veux l'accent latéral.
    """
    bottom = ""
    if delta_pct is not None:
        cls = "delta-up" if delta_pct >= 0 else "delta-down"
        sign = "+" if delta_pct >= 0 else ""
        extra = f" · {sub}" if sub else ""
        bottom = f'<div class="sub"><span class="{cls}">{sign}{delta_pct:.2f}%</span>{extra}</div>'
    elif sub:
        bottom = f'<div class="sub">{sub}</div>'

    unit_html = f'<span class="unit">{unit}</span>' if unit else ""

    st.markdown(f"""
        <div class="kpi-card {tone}">
            <div class="label">{label}</div>
            <div class="value">{value}{unit_html}</div>
            {bottom}
        </div>
    """, unsafe_allow_html=True)


def delta(pct: float, with_arrow: bool = True) -> str:
    """Renvoie le HTML d'une variation colorée. À utiliser dans st.markdown(unsafe_allow_html=True)."""
    if pct > 0:
        arrow = "▲ " if with_arrow else ""
        return f'<span class="delta-up">{arrow}+{pct:.2f}%</span>'
    if pct < 0:
        arrow = "▼ " if with_arrow else ""
        return f'<span class="delta-down">{arrow}{pct:.2f}%</span>'
    return '<span style="color:#8A8275">—</span>'


def tag(label: str, tone: str = "neutral") -> str:
    """Badge uppercase. Tones: up, down, ocre, terra, neutral."""
    return f'<span class="tag {tone}">{label}</span>'


def ticker(code: str) -> str:
    """Chip code titre en mono."""
    return f'<span class="ticker">{code}</span>'


def flag_dot(status: str) -> str:
    """Indicateur de ratio. status: ok | warn | risk."""
    m = {
        "ok":   ("up",   "OK"),
        "warn": ("ocre", "Vigilance"),
        "risk": ("down", "Risque"),
    }
    tone, label = m[status]
    return f'<span class="dot {tone}"></span>{label}'


def section_title(txt: str):
    """Titre de section avec underline discret (style éditorial h2)."""
    st.markdown(f'<h2 class="section-title">{txt}</h2>', unsafe_allow_html=True)


def section_heading(txt: str, spacing: str = "default"):
    """Titre de section compact (h3 style, bold regular).
    Remplace l'usage de label-xs quand on veut une VRAIE section lisible
    (pas un micro-label d'annotation). Utilisé pour Ratios calculés,
    Historique, Hausses du jour, Indices principaux, etc.

    spacing : "tight" (margin top 6px) | "default" (14px) | "loose" (22px)
    """
    mt = {"tight": 6, "default": 14, "loose": 22}.get(spacing, 14)
    st.markdown(
        f'<div class="section-heading" '
        f'style="font-size:15px;font-weight:600;color:var(--ink);'
        f'letter-spacing:-0.01em;margin:{mt}px 0 10px 0;">{txt}</div>',
        unsafe_allow_html=True,
    )


def stars(n: int, max_n: int = 5) -> str:
    """Rating en étoiles unicode, couleur ocre."""
    filled = "★" * n
    empty = "☆" * (max_n - n)
    return f'<span style="color:#C99A3B;letter-spacing:2px">{filled}{empty}</span>'


def load_theme(css_path: str = "style.css"):
    """À appeler une fois au début de app.py après st.set_page_config()."""
    from pathlib import Path
    p = Path(css_path)
    if p.exists():
        st.markdown(f"<style>{p.read_text()}</style>", unsafe_allow_html=True)
