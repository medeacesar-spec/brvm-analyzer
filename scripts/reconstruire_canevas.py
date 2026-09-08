#!/usr/bin/env python3
"""Reconstruit le canevas de design en page exécutable, pour le comparer à l'app.

Le canevas v4 vit dans un artefact Claude Design publié. Ce qu'on en récupère
est un **bundle** : une page unique dont toutes les ressources — le gabarit,
le runtime, React, les polices — sont compressées et encodées en base64 dans
deux balises `<script type="__bundler/...">`. Ouvert tel quel hors de son hôte,
il ne s'exécute pas.

Ce script le décompresse et le remonte en site statique local, ce qui permet de
naviguer dans le canevas page par page et onglet par onglet, à côté de
l'application, et de comparer sur pièces plutôt que de mémoire.

Deux pièges, tous deux rencontrés :

1. **Le gabarit référence ses ressources par UUID nu** (`<script src="a1be...">`).
   Il faut réécrire chaque référence vers le fichier réellement écrit.
2. **React doit être chargé avant React-DOM.** Les deux sont dans le manifeste
   mais aucun n'est référencé par le gabarit : le runtime les attend sur
   `window`. Chargés dans le désordre, la page reste blanche et la console dit
   « ReactDOM.render is not a function ».

Usage :
    python3 scripts/reconstruire_canevas.py <artefact.html> [dossier_sortie]
    python3 -m http.server 8777 --directory <dossier_sortie>

L'artefact source s'obtient en lisant l'artefact publié depuis une session
Claude Code (l'outil Artifact enregistre le HTML complet dans un fichier local).
"""
from __future__ import annotations

import base64
import gzip
import json
import os
import re
import sys

EXTENSIONS = {
    "text/javascript": "js", "text/html": "html", "text/css": "css",
    "font/woff2": "woff2", "image/png": "png", "image/svg+xml": "svg",
}


def _bloc(html: str, nom: str):
    m = re.search(r'<script type="__bundler/%s">(.*?)</script>' % nom, html, re.S)
    return json.loads(m.group(1)) if m else None


def reconstruire(source: str, dest: str) -> str:
    html = open(source, encoding="utf-8").read()
    manifeste = _bloc(html, "manifest") or {}
    gabarit = _bloc(html, "template")
    if not gabarit:
        raise SystemExit("Pas de gabarit dans ce fichier : est-ce bien un bundle ?")

    os.makedirs(dest, exist_ok=True)
    noms = {}
    for uuid, e in manifeste.items():
        data = base64.b64decode(e["data"])
        if e.get("compressed"):
            data = gzip.decompress(data)
        nom = f"{uuid}.{EXTENSIONS.get(e['mime'], 'bin')}"
        open(os.path.join(dest, nom), "wb").write(data)
        noms[uuid] = nom

    for uuid, nom in noms.items():
        gabarit = gabarit.replace(uuid, nom)

    # React et React-DOM ne sont pas référencés par le gabarit : le runtime les
    # attend sur window. On les injecte, react EN PREMIER.
    def _est_react(nom):
        tete = open(os.path.join(dest, nom), encoding="utf-8", errors="ignore").read(400)
        return nom.endswith(".js") and "react" in tete.lower()

    manquants = [n for n in noms.values() if _est_react(n) and n not in gabarit]
    if manquants:
        def _dom_en_second(nom):
            tete = open(os.path.join(dest, nom), encoding="utf-8",
                        errors="ignore").read(300).lower()
            return ("dom" in tete, nom)
        balises = "".join(f'<script src="{n}"></script>'
                          for n in sorted(manquants, key=_dom_en_second))
        gabarit = gabarit.replace("<script src=", balises + "<script src=", 1)

    chemin = os.path.join(dest, "index.html")
    open(chemin, "w", encoding="utf-8").write(gabarit)
    print(f"{len(noms)} ressources · react injecté : {manquants or 'déjà référencé'}")
    print(f"→ {chemin}")
    return chemin


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    reconstruire(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "canevas")
