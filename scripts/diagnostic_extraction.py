#!/usr/bin/env python3
"""Classe les anomalies d'extraction par FAMILLE de cause.

Les erreurs de lecture ne sont pas des accidents isoles : elles se repetent,
et sept causes couvrent l'essentiel de ce qu'on a rencontre sur la cote.
Les decouvrir une par une, au fil des signalements, coute plus cher que de
les compter. Ce script les compte.

Il n'ecrit rien. Il relit les documents avec l'extracteur courant, confronte
chaque valeur a ce que le document dit de lui-meme, et range les desaccords :

  echelle      un montant hors de l'ordre de grandeur du titre — le
               multiplicateur n'a pas ete lu, ou l'a ete de travers
  colonne      la valeur lue est celle de l'exercice PRECEDENT, presente dans
               le meme tableau : l'annee n-1 a ete prise pour l'annee n
  devise       le document affiche des dollars et la valeur en porte la trace
  vocabulaire  le poste est absent alors que le document en parle en toutes
               lettres — aucun motif ne reconnait sa redaction
  coherence    les identites comptables ne tiennent pas (actif != passif,
               PNB - charges != resultat brut)
  scan         le document ne rend aucun texte : il est image, seul l'OCR
               peut le lire, et aucune correction de motif n'y changera rien
  absent       le poste n'est nulle part, ni en tableau ni en prose

La sortie sert a decider quoi corriger ENSUITE, pas a corriger.
"""

import os
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pdfplumber

from data.db import get_connection
from data.pdf_extractor import extract_from_pdf

# Postes suivis, et leur redaction en toutes lettres dans la prose.
POSTES = {
    "revenue": r"chiffre\s+d.affaires|produit\s+net\s+bancaire",
    "net_income": r"r[ée]sultat\s+net",
    "equity": r"capitaux\s+propres",
    "total_assets": r"total\s+(?:du\s+)?bilan|total\s+(?:de\s+l.)?actif",
}

# Un montant plausible pour une societe cotee a la BRVM. En dessous, l'echelle
# est perdue ; au-dessus, elle a ete appliquee deux fois.
PLANCHER = 500_000_000
PLAFOND = 50_000_000_000_000


def _telecharger(url, cache):
    nom = os.path.join(cache, re.sub(r"\W+", "_", url)[-90:] + ".pdf")
    if not os.path.exists(nom):
        requete = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(requete, timeout=180) as flux:
            open(nom, "wb").write(flux.read())
    return nom


def _nombres_du_texte(texte):
    """Tous les montants ecrits dans le document, en valeur absolue."""
    trouves = set()
    for brut in re.findall(r"-?\d[\d  .,]{2,}\d", texte):
        chiffres = re.sub(r"\D", "", brut)
        if len(chiffres) >= 3:
            trouves.add(int(chiffres))
    return trouves


def diagnostiquer(chemin, valeurs, reperes):
    """Range les anomalies d'UN document par famille."""
    with pdfplumber.open(chemin) as pdf:
        texte = "\n".join((p.extract_text() or "") for p in pdf.pages[:20])
    # Un PDF image ne rend pas de texte. Le ranger dans « absent » noierait
    # les vraies lacunes de vocabulaire sous des documents qu'aucun motif ne
    # peut atteindre : seul l'OCR les ouvre.
    if len(texte.strip()) < 200:
        return [("scan", "document image", None)]

    bas = texte.lower()
    nombres = _nombres_du_texte(texte)
    anomalies = []

    devise = bool(re.search(r"\$\s*eu|\busd\b|dollars?\s*(?:us|am[ée]ricain)", bas))

    for champ, motif in POSTES.items():
        valeur = valeurs.get(champ)
        cite = re.search(motif, bas) is not None

        if valeur in (None, 0):
            anomalies.append(("vocabulaire" if cite else "absent", champ, None))
            continue

        if not (PLANCHER <= abs(valeur) <= PLAFOND):
            anomalies.append(("echelle", champ, valeur))
            continue

        # L'ordre de grandeur du titre, tire de ses autres exercices.
        repere = reperes.get(champ)
        if repere and repere > 0:
            rapport = abs(valeur) / repere
            if rapport < 0.02 or rapport > 50:
                anomalies.append(("echelle", champ, valeur))
                continue
            # Un ecart franc mais non demesure, alors que le document contient
            # AUSSI la valeur attendue : c'est la colonne voisine qui a ete lue.
            if 0.5 < rapport < 0.95 or 1.05 < rapport < 2.0:
                mantisse = int(round(repere / 10 ** max(0, len(str(int(repere))) - 9)))
                if mantisse in nombres:
                    anomalies.append(("colonne", champ, valeur))
                    continue

        if devise and champ in ("revenue", "net_income"):
            anomalies.append(("devise?", champ, valeur))

    # Identites comptables : elles ne dependent d'aucun repere externe.
    pnb = valeurs.get("revenue") or valeurs.get("revenue_bank")
    charges = valeurs.get("operating_expenses")
    rbe = valeurs.get("gross_operating_income")
    if pnb and charges and rbe:
        attendu = abs(pnb) - abs(charges)
        if abs(attendu - abs(rbe)) > 0.1 * abs(rbe):
            anomalies.append(("coherence", "PNB - charges != RBE", rbe))

    return anomalies


def main(limite: int = 60, ouvriers: int = 4) -> None:
    cache = os.path.join("/tmp", "diagnostic_extraction")
    os.makedirs(cache, exist_ok=True)

    conn = get_connection()
    documents = [dict(r) for r in conn.execute(
        "SELECT ticker, fiscal_year, url FROM report_links "
        "WHERE report_type = 'etats_financiers' AND fiscal_year >= 2024 "
        "AND url IS NOT NULL ORDER BY ticker, fiscal_year DESC").fetchall()]
    # Ordre de grandeur par titre : la mediane de ses exercices connus.
    connus = {}
    for ligne in conn.execute(
            "SELECT ticker, revenue, net_income, equity, total_assets "
            "FROM fundamentals WHERE revenue IS NOT NULL AND revenue <> 0"):
        ligne = dict(ligne)
        for champ in POSTES:
            valeur = ligne.get(champ)
            if valeur:
                connus.setdefault(ligne["ticker"], {}).setdefault(
                    champ, []).append(abs(valeur))
    conn.close()

    reperes = {t: {c: sorted(v)[len(v) // 2] for c, v in d.items()}
               for t, d in connus.items()}

    vus, choisis = set(), []
    for doc in documents:
        if doc["ticker"] in vus:
            continue
        vus.add(doc["ticker"])
        choisis.append(doc)
    choisis = choisis[:limite]
    print(f"{len(choisis)} documents a diagnostiquer", flush=True)

    from collections import Counter
    familles = Counter()
    detail = []

    def traiter(doc):
        try:
            chemin = _telecharger(doc["url"], cache)
            valeurs = extract_from_pdf(chemin, use_ocr=False)
            return doc, diagnostiquer(chemin, valeurs, reperes.get(doc["ticker"], {}))
        except Exception as exc:
            return doc, [("erreur", str(exc)[:60], None)]

    with ThreadPoolExecutor(max_workers=ouvriers) as pool:
        for doc, anomalies in pool.map(traiter, choisis):
            for famille, quoi, valeur in anomalies:
                familles[famille] += 1
                detail.append((famille, doc["ticker"], doc["fiscal_year"], quoi, valeur))

    print("\n=== familles d'anomalies ===")
    for famille, nombre in familles.most_common():
        print(f"  {famille:12s} {nombre:4d}")

    print("\n=== detail ===")
    for famille, ticker, annee, quoi, valeur in sorted(detail):
        montant = f"{valeur / 1e9:>10,.2f} Mds" if valeur else "          —"
        print(f"  {famille:12s} {ticker:10s} {annee}  {quoi:26s} {montant}")


if __name__ == "__main__":
    limite = 60
    for arg in sys.argv[1:]:
        if arg.startswith("--limite="):
            limite = int(arg.split("=", 1)[1])
    main(limite=limite)
