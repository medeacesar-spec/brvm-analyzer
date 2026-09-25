"""Lire le total du bilan et les capitaux propres dans UN document, par la
colonne du resultat net.

POURQUOI

Le chainage des comparatifs (`analysis/chainage.py`) lit un solde de fin N
dans le document N et le comparatif du document N+1. Il ne peut rien quand
le document N+1 n'a pas de bilan — un rapport d'activite, comme celui de
BICI CI pour 2025 — ou n'existe pas encore : c'est le cas de 48 des 76
capitaux propres manquants au 25/09/2026.

LA COLONNE PAR LE RESULTAT NET

Le passif porte le resultat de l'exercice (« CJ Resultat net de
l'exercice », « Resultat de l'exercice » chez les banques). Il est connu
pour 98 % des exercices, recoupe a la fiche societe. Sa POSITION sur la
ligne — premier ou second montant — donne la colonne de l'exercice, et son
rapport a la base donne l'unite. On lit alors le meme rang sur les lignes
des capitaux propres et du total, a condition qu'elles portent le meme
nombre de montants : un numero de note ou une colonne de variation en plus,
et la ligne est ignoree.

DEUX CONTROLES

  le total de l'actif et celui du passif doivent concorder quand les deux
  lignes sont lues ;
  les capitaux propres ne depassent pas le total, et contiennent le
  resultat de l'exercice en valeur absolue la plupart du temps — on ne
  l'exige pas (une perte peut faire des capitaux propres inferieurs).

Une lecture a un seul document ne comble que des TROUS (voir
`scripts/lire_bilan.py`).
"""
from __future__ import annotations

import re

from analysis.lecture_bancaire import _egal, groupes
from analysis.lecture_syscohada import _plier, montants_de_ligne

RESULTAT = re.compile(r"resultat\s+(net\s+)?de\s+l.?exercice")
CAPITAUX = re.compile(r"(total\s+(des\s+)?)?capitaux\s+propres(\s+et\s+ressources\s+assimilees)?")
EXCLUS_CP = re.compile(r"(provenant|variation|flux|passif|autres|part\s+du\s+groupe|attribuable|minoritaire)")
TOTAL_ACTIF = re.compile(r"total\s+(de\s+l.?actif|actif|general|du\s+bilan|bilan)(?!\s*(immobilise|circulant|non\s+courant|courant))")
TOTAL_PASSIF = re.compile(r"total\s+(du\s+)?passif(?!\s*(circulant|non\s+courant|courant))")
FACTEURS = (1.0, 1e3, 1e6)


def _montants(ligne: str, motif) -> list:
    """Les montants qui suivent le libelle, lus par le lecteur general."""
    m = motif.search(_plier(ligne))
    if not m:
        return []
    gs = groupes(ligne[m.end():])
    if not gs:
        return []
    return montants_de_ligne(" ".join(s + c for s, c in gs))


def lire(texte: str, resultat_net: float) -> dict:
    """{"equity": v, "total_assets": v} en FCFA pour l'exercice dont le
    resultat net vaut `resultat_net` ; vide si la colonne ne se fixe pas."""
    lignes = texte.split("\n")
    ancres = set()
    for ligne in lignes:
        vals = _montants(ligne, RESULTAT)
        for j, v in enumerate(vals):
            for f in FACTEURS:
                if abs(v) >= 100 and _egal(abs(v) * f, abs(resultat_net)):
                    ancres.add((len(vals), j, f))
    if len(ancres) != 1:            # aucune, ou deux lectures qui divergent
        return {}
    k, j, f = ancres.pop()

    def au_rang(motif, exclus=None):
        lus = set()
        for ligne in lignes:
            if exclus is not None and exclus.search(_plier(ligne)):
                continue
            vals = _montants(ligne, motif)
            if len(vals) == k:
                lus.add(round(vals[j] * f))
        return lus

    sortie = {}
    actifs, passifs = au_rang(TOTAL_ACTIF), au_rang(TOTAL_PASSIF)
    totaux = actifs | passifs
    if len(totaux) == 1 or (actifs and passifs and actifs == passifs and len(actifs) == 1):
        sortie["total_assets"] = float(next(iter(totaux)))
    cps = au_rang(CAPITAUX, EXCLUS_CP)
    if len(cps) == 1:
        cp = float(next(iter(cps)))
        if "total_assets" not in sortie or abs(cp) < sortie["total_assets"]:
            sortie["equity"] = cp
    return sortie
