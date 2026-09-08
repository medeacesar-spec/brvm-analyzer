"""
Authentification utilisateur.

En production (Streamlit Cloud ou local avec OAuth configuré dans secrets.toml):
    - `st.login("google")` déclenche le flow OAuth
    - `st.user.is_logged_in` / `st.user.email` / `st.user.name`
    - `st.logout()` déconnecte

En développement (sans OAuth configuré):
    - L'utilisateur peut se "connecter" manuellement en mode dev via la sidebar
      (saisie d'un email fictif)
    - Cela permet de tester le multi-utilisateur sans configurer Google Cloud

Le module expose :
    - `require_login()` : utilisé au début des pages user-scoped
    - `render_auth_widget()` : sidebar (bouton login/logout + badge user)
    - `get_user_email()` / `get_user_name()` / `is_logged_in()`
"""

import os

import streamlit as st


# ─────────────────────────────────────────────────────────────
# Admin : liste des emails + fallback mode dev
# ─────────────────────────────────────────────────────────────

# En local sans OAuth, l'utilisateur 'local' est admin par défaut
# (il gère sa propre instance).
_LOCAL_DEFAULT_ADMIN = True


def _admin_emails() -> list:
    """Liste des emails admin lus depuis secrets.toml → [auth].admin_emails."""
    try:
        return list(st.secrets.get("auth", {}).get("admin_emails", []))
    except Exception:
        return []


def is_admin() -> bool:
    """True si l'utilisateur courant a les droits admin.

    Règles :
    - Mode 'local' (pas d'auth) : True (utilisateur propriétaire de l'instance)
    - Mode dev avec case 'Admin' cochée : True
    - Mode OAuth : True si email dans [auth].admin_emails de secrets.toml
    """
    # 1. Mode local (aucun login) → admin par défaut
    if not is_logged_in():
        return _LOCAL_DEFAULT_ADMIN and not oauth_enabled()

    # 2. Mode dev : checkbox dans le widget login
    if st.session_state.get("dev_user_email"):
        return bool(st.session_state.get("dev_is_admin", False))

    # 3. OAuth : liste whitelist dans secrets
    email = get_user_email()
    if email and email in _admin_emails():
        return True
    return False


def require_admin(feature_name: str = "cette fonctionnalité") -> bool:
    """À appeler avant une action admin. Retourne True si OK, sinon affiche
    un message et retourne False."""
    if is_admin():
        return True
    st.warning(
        f"**Accès administrateur requis** pour {feature_name}. "
        "Cette action est réservée au compte admin de l'application."
    )
    return False


# ─────────────────────────────────────────────────────────────
# Détection du mode OAuth
# ─────────────────────────────────────────────────────────────

def _oauth_configured() -> bool:
    """Vérifie si secrets.toml contient une section [auth] exploitable."""
    try:
        return bool(st.secrets.get("auth", {}).get("redirect_uri"))
    except Exception:
        return False


def oauth_enabled() -> bool:
    """Renvoie True si l'OAuth Google est disponible et configuré."""
    return _oauth_configured()


# ─────────────────────────────────────────────────────────────
# État utilisateur (uniforme OAuth + mode dev)
# ─────────────────────────────────────────────────────────────

def is_logged_in() -> bool:
    """True si l'utilisateur est connecté (OAuth ou mode dev)."""
    # Mode dev override (tests, pas d'OAuth)
    if st.session_state.get("dev_user_email"):
        return True
    # OAuth natif Streamlit
    try:
        user = getattr(st, "user", None)
        if user is not None:
            return bool(getattr(user, "is_logged_in", False))
    except Exception:
        pass
    return False


def get_user_email():
    """Email de l'utilisateur connecté, ou None."""
    if st.session_state.get("dev_user_email"):
        return st.session_state["dev_user_email"]
    try:
        user = getattr(st, "user", None)
        if user and getattr(user, "is_logged_in", False):
            return getattr(user, "email", None)
    except Exception:
        pass
    return None


def get_user_name():
    """Nom affiché de l'utilisateur."""
    if st.session_state.get("dev_user_email"):
        return st.session_state["dev_user_email"].split("@")[0]
    try:
        user = getattr(st, "user", None)
        if user and getattr(user, "is_logged_in", False):
            return getattr(user, "name", None) or getattr(user, "email", None)
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────
# Gating
# ─────────────────────────────────────────────────────────────

def require_login(feature_name: str = "cette fonctionnalité") -> bool:
    """À appeler au début des pages user-scoped. Retourne True si OK,
    sinon affiche un écran de connexion et retourne False."""
    if is_logged_in():
        return True

    st.markdown(
        f'<div class="main-header">Connexion requise</div>',
        unsafe_allow_html=True,
    )
    st.info(
        f"Pour accéder à **{feature_name}**, connectez-vous avec votre compte Google. "
        "Vos données (portefeuille, cash, profil investisseur) sont privées et "
        "isolées par compte."
    )
    _render_login_buttons(container=st, key_prefix="main")
    return False


# ─────────────────────────────────────────────────────────────
# Widgets UI
# ─────────────────────────────────────────────────────────────

def _render_login_buttons(container=st.sidebar, key_prefix: str = "sidebar"):
    """Affiche le bouton de connexion OAuth.
    `key_prefix` évite les clés dupliquées quand le bouton est rendu
    simultanément dans la sidebar ET dans le main (cas require_login)."""
    if oauth_enabled():
        if container.button("Se connecter avec Google",
                            use_container_width=True,
                            key=f"login_google_btn_{key_prefix}"):
            try:
                st.login("google")
            except Exception as e:
                st.error(f"Erreur de connexion : {e}")
    else:
        container.caption(
            "OAuth Google non configuré. Mode local actif — toutes "
            "les fonctions admin sont disponibles sans login."
        )

    _render_dev_login(container, key_prefix)


def dev_login_autorise() -> bool:
    """Le raccourci développeur n'existe que si on l'a explicitement demandé.

    `dev_user_email` et `dev_is_admin` sont LUS partout dans ce module depuis
    l'origine, et n'ont jamais eu d'interface pour les écrire : impossible de
    voir les pages réservées à l'administrateur sans un vrai compte Google,
    donc impossible de les vérifier en développement.

    Ce raccourci comble le manque, et il est fermé par défaut. Il n'apparaît
    que si la variable d'environnement `BRVM_DEV_LOGIN` vaut « 1 ». Streamlit
    Cloud ne la définit pas ; il faudrait l'ajouter délibérément aux réglages
    du déploiement pour l'ouvrir en ligne — ce qu'il ne faut pas faire.

    Il ne crée aucun compte et ne vérifie aucun mot de passe : il pose une
    identité de test dans la session en cours, et rien d'autre. Fermer
    l'onglet l'efface.
    """
    return os.environ.get("BRVM_DEV_LOGIN") == "1"


def _render_dev_login(container, key_prefix: str):
    """Formulaire d'identité de test — visible seulement en mode développeur."""
    if not dev_login_autorise() or is_logged_in():
        return
    with container.expander("Accès développeur", expanded=False):
        st.caption(
            "Identité de test, posée dans cette session uniquement. Aucun "
            "compte n'est créé, aucun mot de passe n'est vérifié."
        )
        # L'adresse par defaut est pilotable par l'environnement : les pages
        # sous connexion sont scopees par courriel, et verifier le rendu du
        # portefeuille REEL demandait de retaper l'adresse a chaque session.
        # `BRVM_DEV_EMAIL=...` a cote de `BRVM_DEV_LOGIN=1` suffit desormais.
        courriel = st.text_input(
            "Adresse", value=os.environ.get("BRVM_DEV_EMAIL", "dev@local"),
            key=f"dev_email_{key_prefix}")
        admin = st.checkbox("Droits administrateur", value=True,
                            key=f"dev_admin_{key_prefix}")
        if st.button("Ouvrir la session de test",
                     key=f"dev_login_{key_prefix}",
                     use_container_width=True):
            st.session_state["dev_user_email"] = courriel or "dev@local"
            st.session_state["dev_is_admin"] = bool(admin)
            st.rerun()


def render_auth_widget():
    """Rend le widget de connexion dans la sidebar — à appeler dans app.py."""
    if is_logged_in():
        name = get_user_name() or get_user_email()
        email = get_user_email()
        is_dev = bool(st.session_state.get("dev_user_email"))
        admin = is_admin()
        # Redesign v4 : carte translucide sur navy, pastille ADMIN en mono,
        # aucun emoji (le rôle se lit au badge, pas à l'icône).
        badges = []
        if is_dev:
            badges.append("DEV")
        if admin:
            badges.append("ADMIN")
        badge_html = "".join(
            "<span style='font-family:var(--font-mono);font-size:9px;"
            "font-weight:600;letter-spacing:0.08em;padding:2px 5px;"
            "border-radius:4px;background:var(--primary-soft);"
            f"color:var(--primary-2);'>{b}</span>"
            for b in badges
        )
        st.sidebar.markdown(
            "<div style='padding:10px 11px;border-radius:8px;"
            "background:rgba(168,196,234,0.14);"
            "border:1px solid rgba(168,196,234,0.34);margin-bottom:0.5rem;'>"
            "<div style='display:flex;align-items:center;gap:7px;"
            "flex-wrap:wrap;margin-bottom:3px;'>"
            "<span style='font-size:12.5px;font-weight:600;"
            f"color:var(--on-dark);'>{name}</span>{badge_html}</div>"
            "<span style='font-family:var(--font-mono);font-size:10.5px;"
            f"color:var(--on-dark-3);'>{email}</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        if st.sidebar.button("Se déconnecter",
                             use_container_width=True, key="logout_btn"):
            _logout()
    else:
        # En mode local pur (pas d'OAuth), l'utilisateur est implicitement admin local
        if not oauth_enabled():
            st.sidebar.markdown(
                "<div style='padding:10px 11px;border-radius:8px;"
                "background:rgba(168,196,234,0.14);"
                "border:1px solid rgba(168,196,234,0.34);margin-bottom:0.5rem;'>"
                "<div style='display:flex;align-items:center;gap:7px;"
                "margin-bottom:3px;'>"
                "<span style='font-size:12.5px;font-weight:600;"
                "color:var(--on-dark);'>Mode local</span>"
                "<span style='font-family:var(--font-mono);font-size:9px;"
                "font-weight:600;letter-spacing:0.08em;padding:2px 5px;"
                "border-radius:4px;background:var(--primary-soft);"
                "color:var(--primary-2);'>ADMIN</span></div>"
                "<span style='font-family:var(--font-mono);font-size:10.5px;"
                "color:var(--on-dark-3);'>Instance mono-utilisateur</span>"
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            st.sidebar.markdown(
                "<div style='padding:10px 11px;border-radius:8px;"
                "background:rgba(255,255,255,0.06);"
                "border:1px solid rgba(255,255,255,0.16);margin-bottom:0.5rem;'>"
                "<span style='font-size:12.5px;font-weight:600;"
                "color:var(--on-dark-2);'>Non connecté</span>"
                "</div>",
                unsafe_allow_html=True,
            )
        _render_login_buttons(container=st.sidebar)


def _logout():
    """Déconnecte l'utilisateur (mode OAuth ou dev)."""
    if st.session_state.get("dev_user_email"):
        del st.session_state["dev_user_email"]
    else:
        try:
            st.logout()
        except Exception:
            pass
    # Reset any per-user caches
    for k in list(st.session_state.keys()):
        if k.startswith("pf_") or k.startswith("portfolio_") or k == "chat_ranked_cache":
            del st.session_state[k]
    st.rerun()
