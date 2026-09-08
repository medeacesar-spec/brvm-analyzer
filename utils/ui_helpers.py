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


# ─────────────────────────────────────────────────────────────────
# Composants du redesign v4
# Référence : design/BRVM Analyzer - Redesign v4.dc.html
# ─────────────────────────────────────────────────────────────────

def breadth_bar(up: int, flat: int, down: int, label: str = "Largeur du marché"):
    """Barre de largeur du marché : hausses / stables / baisses en une bande.

    Une rangée de compteurs dit *combien* ; cette barre dit *dans quelle
    proportion*, ce qui se lit d'un coup d'œil et pas en comparant trois
    nombres. Les trois segments sont dans l'ordre du canevas — hausses,
    stables, baisses — pour que la lecture aille du vert au rouge.
    """
    total = up + flat + down
    if total <= 0:
        return
    segments = [
        (up, "var(--up)", f"{up} ↑"),
        (flat, "var(--border-strong)", f"{flat} ="),
        (down, "var(--down)", f"{down} ↓"),
    ]
    bandes = "".join(
        f"<div style='width:{n / total * 100:.1f}%;background:{couleur};'></div>"
        for n, couleur, _ in segments if n
    )
    compteurs = "".join(
        f"<span style='font-weight:600;color:{couleur};'>{texte}</span>"
        for n, couleur, texte in segments
    )
    st.markdown(
        "<div style='background:var(--bg-elev);border:1px solid var(--border);"
        "border-radius:12px;padding:14px 18px;display:flex;align-items:center;"
        "gap:18px;flex-wrap:wrap;margin-bottom:14px;'>"
        "<span style='font-size:10.5px;font-weight:600;color:var(--ink-3);"
        f"letter-spacing:0.09em;text-transform:uppercase;flex:0 0 auto;'>{label}</span>"
        "<div style='flex:1;min-width:200px;display:flex;height:9px;"
        f"border-radius:999px;overflow:hidden;background:var(--border-soft);'>{bandes}</div>"
        "<div style='display:flex;gap:16px;font-family:var(--font-mono);"
        f"font-size:12px;flex:0 0 auto;'>{compteurs}</div>"
        "</div>",
        unsafe_allow_html=True,
    )


def heatmap(lignes, colonnes, echelle: float = None, footer: str = ""):
    """Carte de chaleur : une ligne par entité, une colonne par fenêtre.

    `lignes` : liste de (libellé, [valeurs en %]) — une valeur par colonne,
    `None` pour une case sans donnée (rendue vide, jamais en zéro : un trou
    n'est pas une stabilité).
    `colonnes` : libellés des fenêtres, sans la première colonne du libellé.
    `echelle` : variation en % qui sature la couleur. Par défaut, le plus
    grand écart observé — ainsi la teinte reste lisible même un mois calme.
    """
    valeurs = [v for _, vals in lignes for v in vals if v is not None]
    if not valeurs:
        return
    if not echelle:
        echelle = max(abs(v) for v in valeurs) or 1.0

    def _fond(v):
        if v is None:
            return "var(--bg-elev)"
        intensite = min(abs(v) / echelle, 1.0) * 0.85
        # Teintes du canevas : vert #0E7A54 en hausse, rouge #C0392B en baisse.
        r, g, b = (14, 122, 84) if v >= 0 else (192, 57, 43)
        return f"rgba({r},{g},{b},{intensite:.3f})"

    def _encre(v):
        if v is None:
            return "var(--ink-4)"
        return "#FFFFFF" if min(abs(v) / echelle, 1.0) > 0.55 else "var(--ink)"

    entetes = (
        "<div style='padding:9px 10px;background:var(--bg-sunken);"
        "border-bottom:1px solid var(--border);font-size:10px;font-weight:600;"
        "color:var(--ink-3);letter-spacing:0.09em;text-transform:uppercase;"
        "white-space:nowrap;'>Secteur</div>"
        + "".join(
            "<div style='padding:9px 10px;background:var(--bg-sunken);"
            "border-bottom:1px solid var(--border);font-size:10px;font-weight:600;"
            "color:var(--ink-3);letter-spacing:0.09em;text-transform:uppercase;"
            f"text-align:right;white-space:nowrap;'>{c}</div>"
            for c in colonnes
        )
    )
    cellules = ""
    for libelle, vals in lignes:
        cellules += (
            "<div style='padding:9px 10px;border-bottom:1px solid var(--bg-elev);"
            "border-right:1px solid var(--bg-elev);font-size:12.5px;"
            f"font-weight:500;color:var(--ink);white-space:nowrap;'>{libelle}</div>"
        )
        for v in vals:
            texte = "—" if v is None else f"{v:+.2f} %"
            cellules += (
                "<div style='padding:9px 10px;border-bottom:1px solid var(--bg-elev);"
                "border-right:1px solid var(--bg-elev);text-align:right;"
                f"background:{_fond(v)};'>"
                "<span style='font-family:var(--font-mono);font-size:12px;"
                f"font-weight:500;color:{_encre(v)};white-space:nowrap;'>{texte}</span>"
                "</div>"
            )
    pied = (
        "<div style='padding:10px 12px;background:var(--bg-footer);"
        f"font-size:11.5px;color:var(--ink-3);'>{footer}</div>" if footer else ""
    )
    st.markdown(
        "<div style='background:var(--bg-elev);border:1px solid var(--border);"
        "border-radius:12px;overflow:hidden;'>"
        "<div style='overflow-x:auto;'>"
        "<div style='display:grid;grid-template-columns:minmax(140px,1.6fr) "
        f"repeat({len(colonnes)},minmax(78px,1fr));min-width:{140 + 90 * len(colonnes)}px;'>"
        f"{entetes}{cellules}</div></div>{pied}</div>",
        unsafe_allow_html=True,
    )


def status_strip(statut: str, seance: str, maj: str = "",
                 titres: int = 0, total: int = 0):
    """Bandeau de contexte de séance, en haut de chaque page.

    Le canevas v4 ouvre chaque page par une bande pleine largeur qui répond à
    deux questions avant toute donnée : *de quelle séance parle-t-on* et
    *depuis quand la page est-elle à jour*. Sans elle, un chiffre affiché ne
    dit pas s'il date de la clôture d'hier ou du relevé de midi.
    """
    droite = ""
    morceaux = []
    if maj:
        morceaux.append(f"MAJ {maj}")
    if total:
        morceaux.append(f"{titres}/{total} titres")
    if morceaux:
        droite = (
            "<span style='font-family:var(--font-mono);font-size:11.5px;"
            f"color:var(--ink-3);'>{' · '.join(morceaux)}</span>"
        )
    st.markdown(
        "<div style='display:flex;align-items:center;justify-content:space-between;"
        "flex-wrap:wrap;gap:12px;padding-bottom:14px;margin-bottom:6px;"
        "border-bottom:1px solid var(--border);'>"
        "<div style='display:flex;align-items:center;gap:10px;flex-wrap:wrap;'>"
        "<span style='display:inline-flex;align-items:center;gap:7px;"
        "padding:4px 9px;border-radius:5px;background:var(--bg-sunken);"
        "color:var(--ink-2);font-family:var(--font-mono);font-size:11px;"
        "font-weight:600;letter-spacing:0.04em;'>"
        "<span style='width:6px;height:6px;border-radius:50%;"
        "background:var(--ink-2);'></span>"
        f"{statut.upper()}</span>"
        f"<span style='font-size:13px;color:var(--ink-2);'>{seance}</span>"
        f"</div>{droite}</div>",
        unsafe_allow_html=True,
    )
