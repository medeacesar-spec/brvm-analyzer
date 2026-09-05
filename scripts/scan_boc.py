#!/usr/bin/env python3
"""Collecte et synthetise le Bulletin Officiel de la Cote (BRVM).

Le BOC parait chaque jour de bourse en PDF entierement textuel sur
brvm.org/fr/bulletins-officiels-de-la-cote. Il porte ce qu'aucune autre
source ne donne d'un bloc :

- les indices, la capitalisation et les volumes du jour, chiffres officiels ;
- les **operations a venir**, avec le dividende **BRUT** par action, sa date de
  mise en paiement et le taux de retenue applicable — « IRVM applicable de
  12 % pour les personnes physiques et 10 % pour les personnes morales ».
  C'est la source qui tranche le debat brut/net : sikafinance publie le net
  des personnes morales, d'ou les ecarts de 10 % ou 12 % selon les titres ;
- le calendrier des assemblees generales.

Usage :
  python3 scripts/scan_boc.py             # dernier bulletin, enregistre
  python3 scripts/scan_boc.py --dry-run   # affiche sans rien enregistrer
  python3 scripts/scan_boc.py --jours 10  # remonte les 10 derniers bulletins
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_tickers  # noqa: E402
from data.db import get_connection  # noqa: E402

BASE = "https://www.brvm.org"
# Seule la page anglaise liste effectivement les bulletins ; la page francaise
# existe mais ne publie aucun lien. Le contenu reste bilingue — les operations
# y sont redigees en francais.
LISTE = f"{BASE}/en/bulletins-officiels-de-la-cote"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "fr-FR,fr;q=0.9",
}


def log(msg):
    print(msg, flush=True)


# ──────────────────────────────────────────────────────────────────────────
# Rattachement des emetteurs aux tickers
# ──────────────────────────────────────────────────────────────────────────

_INDEX = None


def _index():
    global _INDEX
    if _INDEX is not None:
        return _INDEX
    idx = {}
    for t in load_tickers():
        nom = (t.get("name") or "").strip().lower()
        if len(nom) >= 4:
            idx[nom] = t["ticker"]
        code = t["ticker"].split(".")[0].lower()
        if len(code) >= 3:
            idx[code] = t["ticker"]
    try:
        from analysis.llm_chat import _TICKER_ALIASES
        for tk, alias in _TICKER_ALIASES.items():
            for a in alias:
                if len(a) >= 4:
                    idx[a.lower()] = tk
    except Exception as e:
        # Sans les alias, « BOA CI » ou « SGBCI » ne se rattachent plus. L'echec
        # doit se voir dans les logs plutot que degrader le rattachement en
        # silence (llm_chat importe streamlit, absent de certains contextes).
        log(f"[boc] alias indisponibles ({type(e).__name__}) — "
            f"rattachement dégradé")
    _INDEX = idx
    return idx


def _normaliser(t: str) -> str:
    """Minuscules, ponctuation ramenee a des espaces, espaces resserres.

    Le bulletin ecrit « NEI-CEDA CI » quand le referentiel dit « nei ceda ci » :
    sans normalisation, le trait d'union suffit a faire echouer le rattachement.
    """
    return re.sub(r"\s+", " ", re.sub(r"[^\w]+", " ", (t or "").lower())).strip()


def ticker_de(nom: str):
    """Ticker correspondant a un nom d'emetteur du bulletin, ou None."""
    q = _normaliser(nom)
    if not q:
        return None
    index = {_normaliser(k): v for k, v in _index().items()}

    # 1. le nom du bulletin contient un libelle connu
    for expr, tk in sorted(index.items(), key=lambda kv: -len(kv[0])):
        if re.search(r"(?<!\w)" + re.escape(expr) + r"(?!\w)", " " + q + " "):
            return tk
    # 2. forme abregee : le bulletin dit « SMB », le referentiel « smb ci »
    candidats = {tk for expr, tk in index.items()
                 if expr == q or expr.startswith(q + " ")}
    return candidats.pop() if len(candidats) == 1 else None


# ──────────────────────────────────────────────────────────────────────────
# Lecture du PDF
# ──────────────────────────────────────────────────────────────────────────

def bulletins_disponibles(limite: int = 1) -> list:
    """URLs des derniers bulletins, du plus recent au plus ancien."""
    try:
        r = requests.get(LISTE, headers=HEADERS, timeout=45, verify=False)
        r.raise_for_status()
    except Exception as e:
        log(f"[boc] liste inaccessible : {e}")
        return []
    soup = BeautifulSoup(r.text, "lxml")
    urls = []
    for a in soup.find_all("a", href=True):
        h = a["href"]
        m = re.search(r"boc_\w+_(\d{8})", h)
        if not m:
            continue
        u = h if h.startswith("http") else BASE + h
        if u not in [x[1] for x in urls]:
            urls.append((m.group(1), u))
    urls.sort(reverse=True)
    return urls[:limite]


def _nombre(txt):
    if not txt:
        return None
    t = txt.replace("\xa0", "").replace(" ", "").replace(",", "")
    try:
        return float(t)
    except ValueError:
        return None


def _bloc(texte, debut, fin=None):
    i = texte.find(debut)
    if i < 0:
        return ""
    j = texte.find(fin, i + len(debut)) if fin else -1
    return texte[i + len(debut): j if j > 0 else len(texte)]


def synthetiser(pdf_bytes: bytes) -> dict:
    """Extrait du bulletin les elements exploitables."""
    import fitz
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = [p.get_text() for p in doc]
    doc.close()
    p1 = pages[0] if pages else ""
    out = {"indices": [], "operations": [], "assemblees": []}

    m = re.search(r"N°\s*(\d+)", p1)
    out["numero"] = m.group(1) if m else None

    for nom in ("BRVM COMPOSITE", "BRVM PRESTIGE", "BRVM 30", "BRVM PRINCIPAL"):
        m = re.search(re.escape(nom) + r"\s*\n([\d.,]+)\s*\nDaily Change\s*\n([-\d.,]+)\s*%"
                      r"\s*\nChange\s*/\s*YTD\s*\n([-\d.,]+)\s*%", p1)
        if m:
            out["indices"].append({"nom": nom, "niveau": _nombre(m.group(1)),
                                   "jour": _nombre(m.group(2)),
                                   "ytd": _nombre(m.group(3))})

    for cle, motif in (
        ("capitalisation", r"Capitalization \(XOF\)\(Equities & Rights\)\s*\n([\d,]+)"),
        ("volume", r"Volume \(Equities & Rights\)\s*\n([\d,]+)"),
        ("valeur", r"Value \(XOF\) \(Equities & Rights\)\s*\n([\d,]+)"),
        ("hausses", r"Gains\s*\n(\d+)"),
        ("baisses", r"Losses\s*\n(\d+)"),
        ("stables", r"Flat\s*\n(\d+)"),
        ("per_moyen", r"PER \(Average\)\s*\(\*\*\)\s*\n([\d.,]+)"),
    ):
        m = re.search(motif, p1)
        out[cle] = _nombre(m.group(1)) if m else None

    def palmares(titre):
        bloc = _bloc(p1, titre, "TOP") or _bloc(p1, titre, "TOTAL RETURN")
        lignes = [l.strip() for l in bloc.split("\n") if l.strip()]
        if lignes and "Shares" in lignes[0]:
            lignes = lignes[4:]
        res = []
        for i in range(0, len(lignes) - 3, 4):
            if not re.match(r"^[\d,.]+$", lignes[i + 1]):
                break
            res.append({"titre": lignes[i], "cours": _nombre(lignes[i + 1]),
                        "jour": lignes[i + 2], "annee": lignes[i + 3]})
        return res

    out["hausses_top"] = palmares("TOP GAINS")
    out["baisses_top"] = palmares("TOP LOSSES")

    # ── Operations a venir ──
    # Le bloc s'arrete a la rubrique NOTICES, dont les lignes ressemblent a des
    # emetteurs et polluaient la lecture.
    page_ops = next((p for p in pages if "UPCOMING OPERATIONS" in p), "")
    brut_ops = _bloc(page_ops, "UPCOMING OPERATIONS", "NOTICES")
    lignes = [l.strip() for l in brut_ops.split("\n")
              if l.strip() and l.strip() not in ("Issuer", "Operation")]

    def est_emetteur(l):
        if len(l) > 45 or re.match(r"(Paiement|Première|Premiere|D[ée]tachement)", l):
            return False
        lettres = [c for c in l if c.isalpha()]
        return bool(lettres) and sum(c.isupper() for c in lettres) / len(lettres) > 0.7

    groupes, courant = [], None
    for l in lignes:
        if est_emetteur(l):
            courant = {"emetteur": l, "texte": []}
            groupes.append(courant)
        elif courant is not None:
            courant["texte"].append(l)

    for g in groupes:
        detail = re.sub(r"\s+", " ", " ".join(g["texte"])).strip()
        if not detail:
            continue
        montant = re.search(r"de\s+([\d\s.,]+?)\s*F\s*CFA\s+par\s+action", detail)
        irvm = re.search(r"(\d+)\s*%\s*pour les personnes physiques.*?"
                         r"(\d+)\s*%\s*pour les personnes morales", detail)
        quand = re.search(r"le\s+(\d{2}/\d{2}/\d{4})", detail)
        brut = None
        if montant:
            t = montant.group(1).replace(" ", "").replace("\xa0", "")
            t = t.replace(".", "").replace(",", ".") if "," in t else t
            try:
                brut = float(t)
            except ValueError:
                brut = None
        out["operations"].append({
            "emetteur": g["emetteur"],
            "ticker": ticker_de(g["emetteur"]),
            "type": "dividende" if brut else "autre",
            "brut": brut,
            "irvm_physique": int(irvm.group(1)) if irvm else None,
            "irvm_morale": int(irvm.group(2)) if irvm else None,
            "date_operation": (f"{quand.group(1)[6:]}-{quand.group(1)[3:5]}-"
                               f"{quand.group(1)[:2]}") if quand else None,
            "detail": detail[:400],
        })

    # ── Calendrier des assemblees ──
    page_cal = next((p for p in pages if re.search(r"YEAR\s*:\s*\d{4}", p)), "")
    lignes = [l.strip() for l in page_cal.split("\n") if l.strip()]
    for i, l in enumerate(lignes):
        if l in ("Ordinaire", "Extraordinaire", "Mixte") and i >= 1:
            date = lignes[i + 1] if i + 1 < len(lignes) else ""
            if re.match(r"\d{2}/\d{2}/\d{4}", date):
                societe = lignes[i - 1]
                out["assemblees"].append({
                    "societe": societe,
                    "ticker": ticker_de(societe),
                    "type": l,
                    "date_ag": f"{date[6:10]}-{date[3:5]}-{date[:2]}",
                })
    return out


# ──────────────────────────────────────────────────────────────────────────
# Enregistrement
# ──────────────────────────────────────────────────────────────────────────

def enregistrer(date_bulletin: str, url: str, s: dict) -> int:
    conn = get_connection()
    ecrits = 0
    try:
        conn.execute(
            """INSERT INTO boc_bulletins
               (date_bulletin, numero, url, capitalisation, volume, valeur,
                hausses, baisses, stables, per_moyen, indices,
                hausses_top, baisses_top)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (date_bulletin) DO UPDATE SET
                 numero = excluded.numero, url = excluded.url,
                 capitalisation = excluded.capitalisation,
                 volume = excluded.volume, valeur = excluded.valeur,
                 hausses = excluded.hausses, baisses = excluded.baisses,
                 stables = excluded.stables, per_moyen = excluded.per_moyen,
                 indices = excluded.indices,
                 hausses_top = excluded.hausses_top,
                 baisses_top = excluded.baisses_top""",
            (date_bulletin, s.get("numero"), url, s.get("capitalisation"),
             s.get("volume"), s.get("valeur"), s.get("hausses"), s.get("baisses"),
             s.get("stables"), s.get("per_moyen"),
             json.dumps(s.get("indices"), ensure_ascii=False),
             json.dumps(s.get("hausses_top"), ensure_ascii=False),
             json.dumps(s.get("baisses_top"), ensure_ascii=False)),
        )
        ecrits += 1

        # Les operations et assemblees sont remplacees pour ce bulletin :
        # le BOC republie l'etat complet a chaque parution.
        conn.execute("DELETE FROM boc_operations WHERE date_bulletin = ?",
                     (date_bulletin,))
        for o in s.get("operations", []):
            conn.execute(
                """INSERT INTO boc_operations
                   (date_bulletin, emetteur, ticker, type, brut,
                    irvm_physique, irvm_morale, date_operation, detail)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (date_bulletin, o["emetteur"], o["ticker"], o["type"], o["brut"],
                 o["irvm_physique"], o["irvm_morale"], o["date_operation"],
                 o["detail"]),
            )
            ecrits += 1

        conn.execute("DELETE FROM boc_assemblees WHERE date_bulletin = ?",
                     (date_bulletin,))
        for a in s.get("assemblees", []):
            conn.execute(
                """INSERT INTO boc_assemblees
                   (date_bulletin, societe, ticker, type, date_ag)
                   VALUES (?, ?, ?, ?, ?)""",
                (date_bulletin, a["societe"], a["ticker"], a["type"], a["date_ag"]),
            )
            ecrits += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"[boc] echec enregistrement : {type(e).__name__}: {e}")
    finally:
        conn.close()
    return ecrits


def main(dry_run: bool = False, jours: int = 1):
    debut = time.time()
    log(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] scan_boc — start")

    for aaaammjj, url in bulletins_disponibles(jours):
        date_bulletin = f"{aaaammjj[:4]}-{aaaammjj[4:6]}-{aaaammjj[6:]}"
        try:
            pdf = requests.get(url, headers=HEADERS, timeout=90, verify=False).content
            s = synthetiser(pdf)
        except Exception as e:
            log(f"[boc] {date_bulletin} illisible : {type(e).__name__}: {e}")
            continue

        divid = [o for o in s["operations"] if o["type"] == "dividende"]
        log(f"[boc] {date_bulletin} n°{s.get('numero')} — "
            f"{len(s['indices'])} indices · {len(divid)} dividende(s) annonce(s) · "
            f"{len(s['assemblees'])} assemblee(s)")

        if dry_run:
            for o in divid:
                log(f"        {o['emetteur'][:26]:26} {o['brut']:>10,.2f} F brut "
                    f"le {o['date_operation']}  (IRVM {o['irvm_physique']}/"
                    f"{o['irvm_morale']} %)  [{o['ticker'] or 'non rattache'}]")
        else:
            log(f"        {enregistrer(date_bulletin, url, s)} enregistrement(s)")
        time.sleep(0.5)

    log(f"[done] scan_boc en {time.time() - debut:.0f}s")


if __name__ == "__main__":
    args = sys.argv[1:]
    n = 1
    if "--jours" in args:
        n = int(args[args.index("--jours") + 1])
    main(dry_run="--dry-run" in args, jours=n)
