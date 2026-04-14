"""
Fonctions graphiques Plotly réutilisables pour le dashboard BRVM.
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

# Palette BRVM
COLORS = {
    "primary": "#1a5276",
    "secondary": "#2e86c1",
    "accent": "#f39c12",
    "green": "#28a745",
    "red": "#dc3545",
    "yellow": "#ffc107",
    "bg": "#0e1117",
    "card_bg": "#1e2130",
    "text": "#fafafa",
}


def candlestick_chart(
    df: pd.DataFrame,
    title: str = "",
    show_volume: bool = True,
    show_sma: bool = True,
    show_bollinger: bool = False,
    show_rsi: bool = True,
    show_macd: bool = True,
    height: int = 800,
) -> go.Figure:
    """
    Crée un graphique chandelier complet avec indicateurs techniques.
    """
    row_count = 1
    row_heights = [0.5]
    subplot_titles = [title or "Prix"]

    if show_volume:
        row_count += 1
        row_heights.append(0.1)
        subplot_titles.append("Volume")
    if show_rsi:
        row_count += 1
        row_heights.append(0.15)
        subplot_titles.append("RSI")
    if show_macd:
        row_count += 1
        row_heights.append(0.15)
        subplot_titles.append("MACD")

    fig = make_subplots(
        rows=row_count,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=subplot_titles,
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Prix",
            increasing_line_color=COLORS["green"],
            decreasing_line_color=COLORS["red"],
        ),
        row=1, col=1,
    )

    # SMA
    if show_sma:
        for col, name, color in [
            ("sma20", "MM20", "#e74c3c"),
            ("sma50", "MM50", "#f39c12"),
            ("sma200", "MM200", "#3498db"),
        ]:
            if col in df.columns:
                fig.add_trace(
                    go.Scatter(x=df["date"], y=df[col], name=name, line=dict(width=1, color=color)),
                    row=1, col=1,
                )

    # Bollinger Bands
    if show_bollinger and "bb_upper" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"], y=df["bb_upper"], name="BB Sup",
                line=dict(width=1, color="rgba(174,199,232,0.4)", dash="dot"),
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"], y=df["bb_lower"], name="BB Inf",
                line=dict(width=1, color="rgba(174,199,232,0.4)", dash="dot"),
                fill="tonexty", fillcolor="rgba(174,199,232,0.1)",
            ),
            row=1, col=1,
        )

    current_row = 2

    # Volume
    if show_volume and "volume" in df.columns:
        colors = [COLORS["green"] if c >= o else COLORS["red"]
                  for c, o in zip(df["close"], df["open"])]
        fig.add_trace(
            go.Bar(x=df["date"], y=df["volume"], name="Volume", marker_color=colors, opacity=0.7),
            row=current_row, col=1,
        )
        current_row += 1

    # RSI
    if show_rsi and "rsi" in df.columns:
        fig.add_trace(
            go.Scatter(x=df["date"], y=df["rsi"], name="RSI", line=dict(color=COLORS["secondary"], width=1.5)),
            row=current_row, col=1,
        )
        fig.add_hline(y=70, line_dash="dash", line_color=COLORS["red"], opacity=0.5, row=current_row, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color=COLORS["green"], opacity=0.5, row=current_row, col=1)
        fig.add_hrect(y0=30, y1=70, fillcolor="rgba(46,134,193,0.05)", line_width=0, row=current_row, col=1)
        current_row += 1

    # MACD
    if show_macd and "macd" in df.columns:
        fig.add_trace(
            go.Scatter(x=df["date"], y=df["macd"], name="MACD", line=dict(color=COLORS["secondary"], width=1.5)),
            row=current_row, col=1,
        )
        fig.add_trace(
            go.Scatter(x=df["date"], y=df["macd_signal"], name="Signal", line=dict(color=COLORS["accent"], width=1)),
            row=current_row, col=1,
        )
        if "macd_histogram" in df.columns:
            colors_hist = [COLORS["green"] if v >= 0 else COLORS["red"] for v in df["macd_histogram"]]
            fig.add_trace(
                go.Bar(x=df["date"], y=df["macd_histogram"], name="Histogramme", marker_color=colors_hist, opacity=0.6),
                row=current_row, col=1,
            )

    fig.update_layout(
        height=height,
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=50, r=20, t=60, b=30),
    )

    return fig


def radar_chart(data: dict, title: str = "Comparaison") -> go.Figure:
    """
    Crée un graphique radar pour comparer des titres.

    Args:
        data: dict {ticker: {metric: value, ...}, ...}
              Les valeurs doivent être normalisées 0-100.
    """
    fig = go.Figure()

    categories = list(next(iter(data.values())).keys())

    for ticker, values in data.items():
        r = [values.get(cat, 0) for cat in categories]
        r.append(r[0])  # Close the polygon
        fig.add_trace(go.Scatterpolar(
            r=r,
            theta=categories + [categories[0]],
            fill="toself",
            name=ticker,
            opacity=0.6,
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100]),
            bgcolor=COLORS["bg"],
        ),
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        title=title,
        font=dict(color=COLORS["text"]),
        height=500,
    )

    return fig


def performance_chart(data: dict, title: str = "Performance comparee") -> go.Figure:
    """
    Graphique de performance normalisée (base 100) pour comparer des titres.

    Args:
        data: dict {ticker: pd.Series (indexed by date), ...}
    """
    fig = go.Figure()

    for ticker, series in data.items():
        if series.empty:
            continue
        normalized = (series / series.iloc[0]) * 100
        fig.add_trace(go.Scatter(
            x=normalized.index,
            y=normalized.values,
            name=ticker,
            mode="lines",
        ))

    fig.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(
        title=title,
        yaxis_title="Performance (base 100)",
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=450,
    )

    return fig


def gauge_chart(value: float, max_value: float = 100, title: str = "Score") -> go.Figure:
    """Crée un indicateur de type jauge pour les scores."""
    if value >= 60:
        color = COLORS["green"]
    elif value >= 40:
        color = COLORS["yellow"]
    else:
        color = COLORS["red"]

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title=dict(text=title, font=dict(size=16, color=COLORS["text"])),
        number=dict(suffix=f"/{int(max_value)}", font=dict(size=28, color=COLORS["text"])),
        gauge=dict(
            axis=dict(range=[0, max_value], tickcolor=COLORS["text"]),
            bar=dict(color=color),
            bgcolor=COLORS["card_bg"],
            borderwidth=0,
            steps=[
                dict(range=[0, max_value * 0.3], color="rgba(220,53,69,0.2)"),
                dict(range=[max_value * 0.3, max_value * 0.6], color="rgba(255,193,7,0.2)"),
                dict(range=[max_value * 0.6, max_value], color="rgba(40,167,69,0.2)"),
            ],
        ),
    ))

    fig.update_layout(
        height=250,
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        margin=dict(l=20, r=20, t=50, b=20),
    )

    return fig


def pie_chart(labels: list, values: list, title: str = "") -> go.Figure:
    """Graphique camembert pour l'allocation."""
    fig = go.Figure(go.Pie(
        labels=labels,
        values=values,
        hole=0.4,
        textposition="inside",
        textinfo="label+percent",
    ))

    fig.update_layout(
        title=title,
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=400,
        margin=dict(l=20, r=20, t=50, b=20),
    )

    return fig


def flag_badge(flag: str, label: str) -> str:
    """Retourne du HTML pour un badge de drapeau coloré."""
    color_map = {
        "OK": "#28a745",
        "Vigilance": "#ffc107",
        "Risque": "#dc3545",
    }
    color = color_map.get(flag, "#6c757d")
    return f'<span style="background-color:{color};color:white;padding:2px 8px;border-radius:4px;font-size:0.85em;">{flag}</span> {label}'


def stars_display(count: int, max_stars: int = 5) -> str:
    """Retourne des étoiles pour le rating."""
    filled = "★" * count
    empty = "☆" * (max_stars - count)
    return filled + empty
