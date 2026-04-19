# BRVM Analyzer — Streamlit theme kit

**But :** appliquer le nouveau design à ton app Streamlit **sans** ajouter de composant React.
Seulement du CSS injecté + HTML léger via `st.markdown(..., unsafe_allow_html=True)`.
Aucune dépendance supplémentaire. Temps de téléchargement inchangé.

## 1. Config Streamlit native

Crée/modifie `.streamlit/config.toml` :

```toml
[theme]
base = "light"
primaryColor = "#B8532A"
backgroundColor = "#FAF8F4"
secondaryBackgroundColor = "#F3EFE8"
textColor = "#1C1A17"
font = "sans serif"
```

Ça gère 80 % du look (couleurs des widgets, sidebar, accent).

## 2. CSS overrides

Copie `style.css` à la racine du projet, puis dans ton `app.py` au tout début :

```python
import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="BRVM Analyzer",
    page_icon="📊",  # ou laisse défaut
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject theme
css = Path("style.css").read_text()
st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
```

Ça surcharge les composants Streamlit natifs (`st.metric`, `st.tabs`, `st.button`,
`st.dataframe`, `st.expander`, inputs) pour qu'ils matchent le design.

## 3. Mapping : composant React proposé → équivalent Streamlit

| Design proposé       | Streamlit natif / HTML léger |
|----------------------|------------------------------|
| KPI card             | `st.metric(...)` (déjà stylé) OU helper `kpi_card()` ci-dessous |
| Tabs sous-lignés     | `st.tabs([...])` (déjà stylé) |
| Table de positions   | `st.dataframe(df, use_container_width=True)` avec `column_config` |
| Sparkline            | `st.line_chart(series, height=40)` (petit, sans axes) |
| Donut allocation     | `plotly.graph_objects.Pie(hole=0.6)` — tu as déjà Plotly probablement |
| Perf chart           | `st.line_chart(df)` ou `plotly.express.line` |
| Delta +/-%           | Helper HTML `<span class="delta-up">+7.37%</span>` |
| Ticker chip          | `<span class="ticker">ABJC.ci</span>` |
| Ratio flag (dot)     | `<span class="dot up"></span> OK — Bon` |
| Segmented (jour/semaine/mois) | `st.radio(..., horizontal=True)` stylé |
| Stars rating         | `"★" * n + "☆" * (5-n)` en markdown |
| Checklist ✓/✗        | `st.success()` / `st.error()` OU helpers HTML |

## 4. Helpers Python (à copier dans `ui_helpers.py`)

```python
import streamlit as st

def kpi_card(label: str, value, unit: str = "", delta: float | None = None,
             sub: str = "", tone: str = "neutral"):
    """Alternative à st.metric si tu veux l'accent latéral coloré."""
    delta_html = ""
    if delta is not None:
        cls = "delta-up" if delta >= 0 else "delta-down"
        sign = "+" if delta >= 0 else ""
        delta_html = f'<div class="sub"><span class="{cls}">{sign}{delta:.2f}%</span> {sub}</div>'
    elif sub:
        delta_html = f'<div class="sub">{sub}</div>'
    st.markdown(f"""
        <div class="kpi-card {tone}">
            <div class="label">{label}</div>
            <div class="value">{value}<span class="unit">{unit}</span></div>
            {delta_html}
        </div>
    """, unsafe_allow_html=True)

def delta(pct: float) -> str:
    """Renvoie un span HTML pour une variation. À utiliser dans st.markdown()."""
    if pct > 0:  return f'<span class="delta-up">▲ +{pct:.2f}%</span>'
    if pct < 0:  return f'<span class="delta-down">▼ {pct:.2f}%</span>'
    return f'<span style="color:#8A8275">—</span>'

def tag(label: str, tone: str = "neutral") -> str:
    """Renvoie un badge HTML. Tones: up, down, ocre, terra, neutral."""
    return f'<span class="tag {tone}">{label}</span>'

def ticker(code: str) -> str:
    return f'<span class="ticker">{code}</span>'

def flag_dot(status: str) -> str:
    """status: ok | warn | risk"""
    m = {"ok": ("up", "OK"), "warn": ("ocre", "Vigilance"), "risk": ("down", "Risque")}
    tone, label = m[status]
    return f'<span class="dot {tone}"></span>{label}'

def section_title(txt: str):
    st.markdown(f'<h2 class="section-title">{txt}</h2>', unsafe_allow_html=True)
```

## 5. Exemple : Dashboard Marché en Streamlit

```python
import streamlit as st
from ui_helpers import kpi_card, delta, tag, ticker, section_title

st.markdown('<div class="label-xs">Tableau de bord</div>', unsafe_allow_html=True)
st.title("Marché BRVM")
st.caption("Données du vendredi 17 avril 2026 · Dernier jour de cotation")

# KPI row — 2 options
# Option A : st.metric natif (rapide)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Titres en hausse", 14, "+2 vs veille")
c2.metric("Titres en baisse", 27, "+8 vs veille", delta_color="inverse")
c3.metric("Titres stables", 7)
c4.metric("Capitalisation", "15 387 Mds", "-0.42%", delta_color="inverse")
c5.metric("Volume échangé", "892 M")

# Option B : kpi_card avec accent latéral
# c1, c2, c3, c4 = st.columns(4)
# with c1: kpi_card("Titres en hausse", 14, delta=2, tone="up")
# with c2: kpi_card("Titres en baisse", 27, delta=8, tone="down")

st.divider()

# Hausses / Baisses
col_h, col_b = st.columns(2)
with col_h:
    section_title("Hausses du jour")
    for name, tkr, price, pct in [
        ("SODECI", "SDCC.ci", 7140, 7.37),
        ("Bank of Africa Niger", "BOAN.ne", 3200, 6.84),
    ]:
        a, b, c = st.columns([3, 1, 1])
        a.markdown(f"**{name}** {ticker(tkr)}", unsafe_allow_html=True)
        b.markdown(f'<div style="text-align:right;font-variant-numeric:tabular-nums">{price:,}</div>', unsafe_allow_html=True)
        c.markdown(f'<div style="text-align:right">{delta(pct)}</div>', unsafe_allow_html=True)

# Tabs
tab1, tab2, tab3 = st.tabs(["Fondamental", "Technique", "Recommandation"])
with tab1:
    st.write("...")
```

## 6. Poids total ajouté

| Artefact               | Poids       |
|------------------------|-------------|
| `style.css`            | ~6 Ko       |
| `ui_helpers.py`        | ~2 Ko       |
| Config TOML            | <1 Ko       |
| **Total**              | **< 10 Ko** |

Aucune dépendance npm/React/lib graphique supplémentaire.
Si Plotly est déjà utilisé → 0 Ko de plus.

## 7. Ce qu'on garde du design

✓ Palette complète (terracotta / ocre / deep green sur neutres chauds)
✓ Typo système (0 téléchargement)
✓ KPI avec accent latéral, tags, delta colorés, tickers, flag dots
✓ Tabs sous-lignés, inputs discrets, dataframes éditoriaux
✓ Sidebar + sections

## 8. Ce qu'on **ne** porte **pas** (et pourquoi)

✗ SparkLines custom SVG → remplacées par `st.line_chart` height=40 (même résultat visuel en Streamlit)
✗ Donut custom → `plotly.graph_objects.Pie(hole=0.6)` (1 ligne)
✗ Animations fines → pas critiques, Streamlit re-render à chaque interaction
✗ Header horizontal custom → la top bar Streamlit native suffit avec `st.set_page_config(layout="wide")`

## 9. Ordre d'implémentation recommandé

1. Poser `.streamlit/config.toml` + `style.css` → visuellement 70 % du gain immédiat.
2. Remplacer les `st.metric` existants sans toucher à la logique → même API.
3. Helpers `delta()`, `tag()`, `ticker()` dans les tables → gain lisibilité.
4. Enlever les emojis des titres (`st.title("📊 Dashboard")` → `st.title("Dashboard")`).
5. Tester le dark mode éventuel plus tard (tokens prêts dans le CSS).
