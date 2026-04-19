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
        f"🔒 **Accès administrateur requis** pour {feature_name}. "
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
        f'<div class="main-header">🔒 Connexion requise</div>',
        unsafe_allow_html=True,
    )
    st.info(
        f"Pour accéder à **{feature_name}**, connectez-vous avec votre compte Google. "
        "Vos données (portefeuille, cash, profil investisseur) sont privées et "
        "isolées par compte."
    )
    _render_login_buttons(container=st)
    return False


# ─────────────────────────────────────────────────────────────
# Widgets UI
# ─────────────────────────────────────────────────────────────

def _render_login_buttons(container=st.sidebar):
    """Affiche les boutons de connexion (OAuth et/ou dev)."""
    if oauth_enabled():
        if container.button("🔐 Se connecter avec Google",
                            use_container_width=True, key="login_google_btn"):
            try:
                st.login("google")
            except Exception as e:
                st.error(f"Erreur de connexion : {e}")
    else:
        container.caption(
            "⚠️ OAuth Google non configuré. Utiliser le mode développement ci-dessous."
        )

    # Mode dev — affichage direct (pas d'expander pour éviter les conflits CSS)
    container.markdown("##### 🛠️ Mode développement")
    container.caption("Simuler un utilisateur sans Google OAuth.")
    email = container.text_input(
        "Email simulé",
        value=st.session_state.get("dev_user_email", ""),
        key="dev_email_input",
        placeholder="alice@example.com",
    )
    dev_admin = container.checkbox(
        "👑 Admin (import PDFs, synchro, etc.)",
        value=False, key="dev_admin_checkbox",
    )
    if container.button("✅ Entrer en mode dev", key="dev_login_btn",
                        use_container_width=True):
        if email and "@" in email:
            st.session_state["dev_user_email"] = email.strip().lower()
            st.session_state["dev_is_admin"] = dev_admin
            st.rerun()
        else:
            container.warning("Entrez une adresse email valide.")


def render_auth_widget():
    """Rend le widget de connexion dans la sidebar — à appeler dans app.py."""
    if is_logged_in():
        name = get_user_name() or get_user_email()
        email = get_user_email()
        is_dev = bool(st.session_state.get("dev_user_email"))
        admin = is_admin()
        admin_tag = " 👑 admin" if admin else ""
        tag = "🛠️ dev" if is_dev else "🔐"
        # Design v2 : badge clair avec accent terracotta pour admin, crème pour user.
        if admin:
            border_color = "var(--terracotta)"
            bg = "var(--terracotta-bg)"
            accent_color = "var(--terracotta-2)"
        else:
            border_color = "var(--border)"
            bg = "var(--bg-sunken)"
            accent_color = "var(--ink-2)"
        st.sidebar.markdown(
            f"<div style='padding:0.55rem 0.75rem;"
            f"background:{bg};border:1px solid {border_color};"
            f"border-radius:8px;margin-bottom:0.5rem;'>"
            f"<b style='color:{accent_color};font-size:0.82rem;'>{tag} {name}{admin_tag}</b><br>"
            f"<small style='color:var(--ink-3);font-size:0.72rem;'>{email}</small>"
            f"</div>",
            unsafe_allow_html=True,
        )
        if st.sidebar.button("🚪 Se déconnecter",
                             use_container_width=True, key="logout_btn"):
            _logout()
    else:
        # En mode local pur (pas d'OAuth), l'utilisateur est implicitement admin local
        if not oauth_enabled():
            st.sidebar.markdown(
                "<div style='padding:0.55rem 0.75rem;"
                "background:var(--terracotta-bg);"
                "border:1px solid var(--terracotta);"
                "border-radius:8px;margin-bottom:0.5rem;'>"
                "<b style='color:var(--terracotta-2);font-size:0.82rem;'>🗄️ Mode local · 👑 admin</b><br>"
                "<small style='color:var(--ink-3);font-size:0.72rem;'>Instance mono-utilisateur</small>"
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            st.sidebar.markdown(
                "<div style='padding:0.55rem 0.75rem;background:var(--bg-sunken);"
                "border:1px solid var(--border);border-radius:8px;margin-bottom:0.5rem;'>"
                "<b style='color:var(--ink-2);font-size:0.82rem;'>🔒 Non connecté</b>"
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
