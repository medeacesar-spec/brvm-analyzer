"""Keep-alive Streamlit Cloud.

Ouvre l'app dans un Chromium headless et y maintient une VRAIE session
(WebSocket ouverte + interactions) pendant ~1 min. Streamlit Community Cloud
compte l'activite sur les sessions WebSocket, pas sur les requetes HTTP.

Deux pieges decouverts le 2026-08-19 :
  1. brvm-analyzer.streamlit.app sert une page enveloppe ; l'app tourne dans
     une IFRAME dont l'URL contient "/~/+/". Chercher [data-testid="stApp"]
     dans la page principale renvoie toujours 0.
  2. L'ecran "Your app is waking up!" ne bascule pas tout seul vers l'app une
     fois celle-ci demarree — il faut recharger la page.

Script autonome : ne depend que de playwright (pas de requirements.txt).
Sort toujours en code 0 — un keep-alive rate ne doit pas casser le workflow.
"""

import sys
import time

URL = "https://brvm-analyzer.streamlit.app/"

# L'app elle-meme est servie dans une iframe sous ce chemin
APP_FRAME_MARKER = "/~/+/"

# WebSocket de session Streamlit : la preuve qu'une session est enregistree
STREAM_WS_MARKER = "_stcore/stream"

# Marqueurs de l'ecran "app endormie / en cours de reveil" de Streamlit Cloud
SLEEP_MARKERS = (
    "gone to sleep",
    "zzzz",
    "get this app back up",
    "waking up",
    "is booting up",
    "spinning up",
)

WAKE_TIMEOUT_S = 900      # 15 min max pour un reveil a froid
POLL_EVERY_S = 10         # frequence de verification pendant le reveil
RELOAD_EVERY_S = 60       # l'ecran de reveil est fige : on recharge
DWELL_S = 60              # duree de session active une fois l'app rendue


def log(msg):
    print(f"[keep-alive] {msg}", flush=True)


def app_frame(page):
    """L'iframe qui contient reellement l'app Streamlit, ou None."""
    for f in page.frames:
        if APP_FRAME_MARKER in f.url:
            return f
    return None


def app_rendered(page):
    f = app_frame(page)
    if f is None:
        return False
    try:
        return f.locator('[data-testid="stApp"]').count() > 0
    except Exception:
        return False


def looks_asleep(page):
    try:
        text = (page.inner_text("body") or "").lower()
    except Exception:
        return False
    return any(m in text for m in SLEEP_MARKERS)


def try_click_wake(page):
    """Clique le bouton de reveil s'il est present. True si clique."""
    for sel in (
        "text=Yes, get this app back up!",
        "button:has-text('get this app back up')",
    ):
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible():
                btn.click(timeout=5000)
                log(f"bouton de reveil clique ({sel})")
                return True
        except Exception:
            continue
    return False


def keep_alive():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("playwright indisponible — abandon")
        return False

    ws_urls = []
    t0 = time.time()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.on("websocket", lambda ws: ws_urls.append(ws.url))

        log(f"GET {URL}")
        try:
            page.goto(URL, timeout=90000, wait_until="domcontentloaded")
        except Exception as e:
            log(f"navigation warning: {e}")

        log(f"page chargee en {time.time() - t0:.1f}s, title={page.title()!r}")

        # --- Phase 1 : attendre que l'app soit reellement rendue -------------
        rendered = False
        last_reload = time.time()
        while time.time() - t0 < WAKE_TIMEOUT_S:
            if app_rendered(page):
                rendered = True
                log(f"app rendue apres {time.time() - t0:.0f}s")
                break
            if looks_asleep(page):
                try_click_wake(page)
            if time.time() - last_reload > RELOAD_EVERY_S:
                log(f"pas encore rendue a {time.time() - t0:.0f}s "
                    f"(endormie={looks_asleep(page)}) — reload")
                try:
                    page.reload(timeout=90000, wait_until="domcontentloaded")
                except Exception as e:
                    log(f"reload warning: {e}")
                last_reload = time.time()
            time.sleep(POLL_EVERY_S)

        if not rendered:
            log(f"ECHEC : app non rendue apres {time.time() - t0:.0f}s")
            log(f"frames vues : {[f.url for f in page.frames]}")
            log(f"websockets vues : {ws_urls or 'aucune'}")
            context.close()
            browser.close()
            return False

        # --- Phase 2 : maintenir une session active --------------------------
        # Bouger la souris / scroller envoie de vrais messages sur la WebSocket,
        # ce qu'un simple chargement de page ne fait pas.
        dwell_start = time.time()
        while time.time() - dwell_start < DWELL_S:
            try:
                page.mouse.move(400 + (int(time.time()) % 200), 300)
                page.mouse.wheel(0, 120)
                page.mouse.wheel(0, -120)
            except Exception as e:
                log(f"interaction warning: {e}")
            time.sleep(5)

        stream_ws = [u for u in ws_urls if STREAM_WS_MARKER in u]
        log(f"session active {time.time() - dwell_start:.0f}s, "
            f"total {time.time() - t0:.0f}s, "
            f"websockets={len(ws_urls)} (stream={len(stream_ws)})")
        for u in ws_urls[:5]:
            log(f"  ws: {u}")

        context.close()
        browser.close()

        if not stream_ws:
            log("ATTENTION : aucune WebSocket _stcore/stream — session "
                "probablement non comptabilisee par Streamlit Cloud")
            return False
    return True


if __name__ == "__main__":
    try:
        ok = keep_alive()
    except Exception as e:
        log(f"fatal: {type(e).__name__}: {e}")
        ok = False
    log("resultat: " + ("SUCCES" if ok else "ECHEC"))
    sys.exit(0)
