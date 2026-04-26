"""Helpers pour la navigation cross-page : ouvrir un titre dans la page
"Analyse d'un Titre" depuis n'importe où."""

import streamlit as st


PAGE_STOCK_ANALYSIS = "Analyse d'un Titre"


def goto_ticker(ticker: str) -> None:
    """Stocke le ticker demandé + la page cible et déclenche un rerun.
    L'app.py détecte `pending_page` et force la navigation.
    La page p2 détecte `target_ticker` et pré-sélectionne le titre."""
    st.session_state["target_ticker"] = ticker
    st.session_state["pending_page"] = PAGE_STOCK_ANALYSIS
    st.rerun()


def ticker_analyze_button(
    ticker: str,
    label: str = None,
    key: str = None,
    help_text: str = None,
    use_container_width: bool = False,
) -> bool:
    """Rend un bouton qui, au clic, ouvre la page Analyse sur ce titre."""
    key = key or f"goto_{ticker}"
    btn_label = label if label is not None else ticker
    if st.button(
        btn_label,
        key=key,
        help=help_text or f"Analyser {ticker}",
        use_container_width=use_container_width,
    ):
        goto_ticker(ticker)
        return True
    return False


def ticker_quick_picker(options: list, key: str = "quick_ticker", label: str = "Analyser un titre") -> None:
    """Affiche un selectbox + bouton 'Analyser' pour un gros tableau.
    `options` doit être une liste de (ticker, display_label)."""
    if not options:
        return
    with st.container():
        col_sel, col_btn = st.columns([4, 1])
        with col_sel:
            choice = st.selectbox(
                label,
                options=[o[1] for o in options],
                key=f"{key}_select",
            )
        with col_btn:
            st.markdown('<div style="padding-top:1.75rem"></div>', unsafe_allow_html=True)
            if st.button("Ouvrir ➜", key=f"{key}_btn"):
                idx = [o[1] for o in options].index(choice)
                goto_ticker(options[idx][0])
