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


def heatmap(lignes, colonnes, echelle: float = None, footer: str = "",
            mode: str = "signe", intitule_colonne: str = "Secteur",
            formatter=None):
    """Carte de chaleur : une ligne par entité, une colonne par fenêtre.

    `lignes` : liste de (libellé, [valeurs en %]) — une valeur par colonne,
    `None` pour une case sans donnée (rendue vide, jamais en zéro : un trou
    n'est pas une stabilité).
    `colonnes` : libellés des fenêtres, sans la première colonne du libellé.
    `echelle` : valeur qui sature la couleur. Par défaut, le plus grand écart
    observé — ainsi la teinte reste lisible même un mois calme.
    `mode` : « signe » colore en vert au-dessus de zéro et en rouge en
    dessous — c'est la lecture d'une variation. « intensite » emploie une
    seule teinte navy du clair au foncé : pour un score, où il n'y a pas de
    négatif, deux couleurs feraient croire à un seuil qui n'existe pas.
    """
    valeurs = [v for _, vals in lignes for v in vals if v is not None]
    if not valeurs:
        return
    if not echelle:
        # Le maximum absolu comme échelle rendait la carte illisible dès
        # qu'UNE case sortait du lot : sur les secteurs, la colonne « Max »
        # à +903 % délavait les quarante autres cases. On sature au 85e
        # centile — les extrêmes s'affichent à pleine teinte, le reste garde
        # son contraste.
        tries = sorted(abs(v) for v in valeurs)
        rang = max(0, int(0.85 * (len(tries) - 1)))
        echelle = tries[rang] or max(tries) or 1.0

    def _fond(v):
        if v is None:
            return "var(--bg-elev)"
        intensite = min(abs(v) / echelle, 1.0) * 0.85
        if mode == "intensite":
            # Navy #1B3A6B, du clair au foncé.
            r, g, b = (27, 58, 107)
        else:
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
        f"white-space:nowrap;'>{intitule_colonne}</div>"
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
            if v is None:
                texte = "—"
            elif formatter is not None:
                texte = formatter(v)
            else:
                texte = f"{v:+.2f} %"
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


def _carte_kpi_html(label: str, value: str, sub: str = "",
                    accent: str = "var(--ink-4)", sub_color: str = "",
                    dark: bool = False, bar_pct: float = None,
                    taille: str = "27px", couleur_valeur: str = "") -> str:
    """Le HTML d'une carte de KPI au modèle du canevas v4.

    Rendu séparé de l'écriture : `kpi_v4` en pose une, `kpi_grille` en pose
    une rangée dans une grille qui se replie. Les deux doivent produire
    exactement la même carte, d'où ce gabarit unique.

    `st.metric` ne sait pas teinter une carte : toutes portent le même filet
    neutre. Or le canevas donne à chacune la couleur de ce qu'elle dit — les
    hausses en vert, les baisses en rouge — et c'est ce filet qui rend la
    rangée lisible avant qu'on ait lu un seul chiffre.

    `dark` rend la carte navy pleine, réservée à la valeur qui domine les
    autres (un score total, un univers de départ) plutôt qu'à leur égale.
    """
    if dark:
        fond, bord, c_label = "var(--primary-2)", "var(--primary-2)", "var(--on-dark-3)"
        c_val, c_sub, piste = "var(--on-dark)", "var(--on-dark-2)", "rgba(255,255,255,0.18)"
        barre = "var(--primary-soft)"
        filet = f"1px solid {bord}"
    else:
        fond, bord, c_label = "var(--bg-elev)", "var(--border)", "var(--ink-3)"
        c_val = couleur_valeur or "var(--ink)"
        c_sub = sub_color or "var(--ink-3)"
        piste, barre = "var(--bg-sunken)", accent
        filet = f"2px solid {accent}"

    html_barre = ""
    if bar_pct is not None:
        html_barre = (
            f"<div style='height:4px;background:{piste};border-radius:999px;"
            f"overflow:hidden;margin:3px 0 1px;'>"
            f"<div style='width:{max(0, min(100, bar_pct)):.0f}%;height:100%;"
            f"border-radius:999px;background:{barre};'></div></div>"
        )
    poids_sub = 600 if sub_color else 400
    return (
        f"<div style='background:{fond};border:1px solid {bord};"
        f"border-top:{filet};border-radius:12px;padding:15px 17px;"
        "display:flex;flex-direction:column;gap:5px;height:100%;'>"
        "<span style='font-size:10.5px;font-weight:600;letter-spacing:0.09em;"
        f"text-transform:uppercase;color:{c_label};'>{label}</span>"
        # nowrap : dans une rangée de sept cartes sur écran étroit, « +1,92 % »
        # se brisait en trois lignes, un caractère par ligne. Mieux vaut
        # écourter que d'empiler.
        f"<span style='font-variant-numeric:tabular-nums;font-size:{taille};"
        f"font-weight:600;letter-spacing:-0.015em;line-height:1.05;"
        f"white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
        f"color:{c_val};'>{value}</span>"
        f"{html_barre}"
        f"<span style='font-size:11.5px;font-weight:{poids_sub};"
        f"line-height:1.3;color:{c_sub};'>{sub}</span></div>"
    )


def kpi_v4(*args, **kwargs):
    """Une carte de KPI, posée seule dans la colonne courante."""
    st.markdown(_carte_kpi_html(*args, **kwargs), unsafe_allow_html=True)


def kpi_grille(cartes, mini: str = "210px"):
    """Une rangée de cartes de KPI dans une grille qui se replie.

    `st.columns(N)` impose N colonnes quelle que soit la largeur : à quatre
    cartes sur un écran de portable, les libellés se brisent au milieu d'un
    mot — « ENDETTEMEN / T ». Le canevas emploie partout
    `repeat(auto-fit, minmax(...))`, qui passe de quatre colonnes à deux puis
    à une selon la place, sans jamais couper un libellé.

    `cartes` est une liste de dictionnaires aux arguments de `kpi_v4`.
    """
    cartes = [c for c in cartes if c]
    if not cartes:
        return
    st.markdown(
        f"<div style='display:grid;gap:12px;margin:2px 0 6px;"
        f"grid-template-columns:repeat(auto-fit,minmax({mini},1fr));'>"
        + "".join(_carte_kpi_html(**c) for c in cartes)
        + "</div>",
        unsafe_allow_html=True,
    )


def note(titre: str, texte: str, ton: str = "primary"):
    """Encart à filet latéral : une lecture, pas une donnée.

    Le canevas s'en sert partout où l'application doit DIRE quelque chose —
    pourquoi deux colonnes ne se contredisent pas, ce qu'un chiffre ne dit
    pas, quelle hypothèse a été prise. `st.info` mettait ces phrases dans une
    boîte bleue d'alerte, ce qui les faisait lire comme des avertissements.

    `ton` : primary (lecture), warn (vigilance), up, down.
    """
    accents = {"primary": "var(--primary)", "warn": "var(--warn)",
               "up": "var(--up)", "down": "var(--down)"}
    accent = accents.get(ton, "var(--primary)")
    entete = (
        f"<div style='font-size:14.5px;font-weight:600;margin-bottom:6px;'>"
        f"{titre}</div>" if titre else ""
    )
    st.markdown(
        "<div style='background:var(--bg-elev);border:1px solid var(--border);"
        f"border-left:2px solid {accent};border-radius:0 12px 12px 0;"
        "padding:14px 18px;margin:12px 0;'>"
        f"{entete}"
        "<div style='font-size:13px;color:var(--ink-2);line-height:1.55;"
        f"max-width:76ch;text-wrap:pretty;'>{texte}</div></div>",
        unsafe_allow_html=True,
    )


# Séquence de teintes du canevas v4, pour les blocs à catégories.
TEINTES_V4 = ["#1B3A6B", "#8A5A00", "#0E7A54", "#5A7CA8", "#A8C4EA",
              "#6E7581", "#C0392B", "#12294B", "#AEB4BE"]


def donut(segments, grand: str, sous_titre: str = "", montant_fmt=None):
    """Anneau à figure centrale, avec sa légende chiffrée.

    Le canevas préfère l'anneau au camembert plein, et pour une raison
    lisible : le centre porte le total, si bien que la part et la masse se
    lisent d'un seul regard. La légende donne le montant ET le pourcentage —
    un camembert seul oblige à survoler chaque part pour connaître sa valeur.

    `segments` : liste de (libellé, montant). Les teintes suivent la séquence
    du canevas ; au-delà de neuf catégories, elle se répète — c'est le signe
    qu'il faut regrouper, pas ajouter des couleurs.
    """
    segments = [(str(l), float(v)) for l, v in segments if v and float(v) > 0]
    if not segments:
        return
    total = sum(v for _, v in segments)
    if total <= 0:
        return
    if montant_fmt is None:
        def montant_fmt(v):
            return f"{v:,.0f}".replace(",", " ")

    RAYON = 66
    CIRCONFERENCE = 2 * 3.141592653589793 * RAYON
    arcs, lignes, decalage = "", "", 0.0
    for i, (libelle, valeur) in enumerate(segments):
        part = valeur / total
        longueur = part * CIRCONFERENCE
        couleur = TEINTES_V4[i % len(TEINTES_V4)]
        arcs += (
            f"<circle cx='90' cy='90' r='{RAYON}' fill='none' "
            f"stroke='{couleur}' stroke-width='26' "
            f"stroke-dasharray='{longueur:.2f} {CIRCONFERENCE - longueur:.2f}' "
            f"stroke-dashoffset='{-decalage:.2f}'></circle>"
        )
        decalage += longueur
        lignes += (
            "<div style='display:flex;align-items:center;gap:10px;"
            "padding-bottom:7px;border-bottom:1px solid var(--border-soft);'>"
            f"<span style='width:10px;height:10px;border-radius:3px;"
            f"flex-shrink:0;background:{couleur};'></span>"
            "<span style='font-size:13px;flex:1;min-width:0;"
            "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;'>"
            f"{libelle}</span>"
            "<span style='font-family:var(--font-mono);font-size:12px;"
            f"color:var(--ink-3);'>{montant_fmt(valeur)}</span>"
            "<span style='font-size:12.5px;font-weight:600;"
            "font-variant-numeric:tabular-nums;width:52px;text-align:right;'>"
            f"{part * 100:.1f} %</span></div>"
        )
    st.markdown(
        "<div style='background:var(--bg-elev);border:1px solid var(--border);"
        "border-radius:12px;padding:20px 22px;display:flex;align-items:center;"
        "gap:30px;flex-wrap:wrap;'>"
        "<div style='position:relative;width:180px;height:180px;flex:0 0 auto;'>"
        "<svg viewBox='0 0 180 180' style='width:180px;height:180px;"
        f"transform:rotate(-90deg);'>{arcs}</svg>"
        "<div style='position:absolute;inset:0;display:flex;"
        "flex-direction:column;align-items:center;justify-content:center;"
        "gap:2px;'>"
        "<span style='font-size:22px;font-weight:600;letter-spacing:-0.02em;"
        f"font-variant-numeric:tabular-nums;'>{grand}</span>"
        "<span style='font-size:10.5px;font-weight:600;color:var(--ink-3);"
        f"letter-spacing:0.08em;text-transform:uppercase;'>{sous_titre}</span>"
        "</div></div>"
        "<div style='flex:1;min-width:220px;display:flex;"
        f"flex-direction:column;gap:8px;'>{lignes}</div></div>",
        unsafe_allow_html=True,
    )


def plan_etapes(titre: str, etapes, compte: str = ""):
    """Plan d'action ordonné, une carte numérotée par étape.

    Le canevas ne se contente pas de lister ce qu'il y a à faire : il le
    NUMÉROTE. Trois tableaux côte à côte — vendre, renforcer, acheter — posent
    tous la même question implicite, « par quoi je commence ». La réponse
    tient dans un rang.

    `etapes` : liste de dicts avec `action`, `tag`, `motif`, `accent`,
    `rang_libelle`, `lignes` (liste de (ticker, nom, quantité)), et
    facultativement `impact_libelle`, `impact`, `impact_part` (0-100),
    `impact_sub`. L'impact n'est affiché que s'il est fourni : le canevas en
    montre un pour chaque étape, l'application ne sait pas toujours le
    chiffrer, et une case vide vaut mieux qu'un nombre inventé.
    """
    if not etapes:
        return
    st.markdown(
        "<div style='display:flex;align-items:baseline;gap:10px;"
        "margin:26px 0 12px;'>"
        "<h2 style='font-size:17px;font-weight:600;margin:0;"
        f"letter-spacing:-0.015em;'>{titre}</h2>"
        + (f"<span style='font-family:var(--font-mono);font-size:11.5px;"
           f"color:var(--ink-3);'>{compte}</span>" if compte else "")
        + "</div>",
        unsafe_allow_html=True,
    )
    for i, e in enumerate(etapes, start=1):
        accent = e.get("accent", "var(--primary)")
        puces = "".join(
            "<span style='display:inline-flex;align-items:center;gap:6px;"
            "padding:3px 8px;border-radius:6px;background:var(--bg-sunken);'>"
            "<span style='font-family:var(--font-mono);font-size:10.5px;"
            f"font-weight:600;color:var(--ink-2);'>{t}</span>"
            f"<span style='font-size:12px;color:var(--ink);'>{n}</span>"
            + (f"<span style='font-family:var(--font-mono);font-size:11.5px;"
               f"font-weight:600;color:{accent};'>{q}</span>" if q else "")
            + "</span>"
            for t, n, q in e.get("lignes", [])
        )
        impact = ""
        if e.get("impact"):
            impact = (
                "<div style='flex:0 0 176px;padding:14px 18px;"
                "border-left:1px solid var(--border-soft);display:flex;"
                "flex-direction:column;gap:4px;justify-content:center;'>"
                "<span style='font-size:10px;font-weight:600;"
                "color:var(--ink-3);letter-spacing:0.08em;"
                f"text-transform:uppercase;'>{e.get('impact_libelle', 'Impact')}"
                "</span>"
                "<span style='font-size:19px;font-weight:600;"
                "font-variant-numeric:tabular-nums;letter-spacing:-0.02em;"
                f"color:{accent};'>{e['impact']}</span>"
                "<div style='height:5px;background:var(--bg-sunken);"
                "border-radius:999px;overflow:hidden;margin-top:3px;'>"
                f"<div style='width:{max(0, min(100, e.get('impact_part', 0))):.0f}%;"
                f"height:100%;border-radius:999px;background:{accent};'></div>"
                "</div>"
                "<span style='font-size:11px;color:var(--ink-3);'>"
                f"{e.get('impact_sub', '')}</span></div>"
            )
        st.markdown(
            "<div style='background:var(--bg-elev);border:1px solid var(--border);"
            f"border-left:4px solid {accent};border-radius:0 12px 12px 0;"
            "display:flex;align-items:stretch;flex-wrap:wrap;"
            "margin-bottom:12px;'>"
            "<div style='flex:0 0 66px;background:var(--bg-sunken);"
            "display:flex;flex-direction:column;align-items:center;"
            "justify-content:center;gap:2px;padding:16px 0;'>"
            "<span style='font-family:var(--font-mono);font-size:22px;"
            f"font-weight:600;color:{accent};line-height:1;'>{i}</span>"
            "<span style='font-size:9px;font-weight:600;letter-spacing:0.08em;"
            f"text-transform:uppercase;color:{accent};'>"
            f"{e.get('rang_libelle', '')}</span></div>"
            "<div style='flex:1;min-width:240px;padding:14px 18px;"
            "display:flex;flex-direction:column;gap:7px;'>"
            "<div style='display:flex;align-items:center;gap:9px;"
            "flex-wrap:wrap;'>"
            "<span style='font-size:15.5px;font-weight:600;"
            f"letter-spacing:-0.015em;'>{e.get('action', '')}</span>"
            + (f"<span style='display:inline-flex;align-items:center;"
               f"padding:2px 8px;border-radius:5px;font-size:10.5px;"
               f"font-weight:600;letter-spacing:0.04em;"
               f"background:var(--bg-sunken);color:{accent};'>"
               f"{e['tag']}</span>" if e.get("tag") else "")
            + "</div>"
            + (f"<div style='display:flex;align-items:center;gap:8px;"
               f"flex-wrap:wrap;'>{puces}</div>" if puces else "")
            + "<div style='font-size:13px;color:var(--ink-2);line-height:1.5;"
            f"max-width:74ch;text-wrap:pretty;'>{e.get('motif', '')}</div>"
            "</div>"
            f"{impact}</div>",
            unsafe_allow_html=True,
        )
