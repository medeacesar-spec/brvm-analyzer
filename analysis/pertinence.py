"""Pondération des dépêches de la revue de presse.

Le problème que ce module résout : en ouvrant la collecte à d'autres sources
régionales, le volume triple, et la plupart des articles ne concernent pas la
cote. Trier par date remonterait alors les dépêches les moins pertinentes.

Le score est **explicable** : chaque point gagné ou perdu a un motif nommé,
rendu à l'appelant avec le score. Une dépêche retenue doit pouvoir dire
pourquoi, et une dépêche écartée aussi. Aucun modèle, aucune génération de
texte : des règles lisibles, que l'on peut discuter et corriger.

L'échelle n'a pas de sens absolu — seul l'ordre compte, et le seuil de
rétention est un rang, pas une note.
"""
from __future__ import annotations

import re

# Les huit pays de l'UEMOA, ceux de la BRVM. Une dépêche sur le Cameroun ou
# le Nigeria peut être excellente et sans rapport avec la cote.
PAYS_UEMOA = [
    (r"b[ée]nin|cotonou", "Bénin"),
    (r"burkina|ouagadougou|bobo-dioulasso", "Burkina Faso"),
    (r"c[ôo]te d.ivoire|ivoirien|abidjan|yamoussoukro", "Côte d'Ivoire"),
    (r"guin[ée]e-bissau|bissau", "Guinée-Bissau"),
    (r"\bmali\b|malien|bamako", "Mali"),
    (r"\bniger\b|nig[ée]rien|niamey", "Niger"),
    (r"s[ée]n[ée]gal|dakar", "Sénégal"),
    (r"\btogo\b|togolais|lom[ée]", "Togo"),
    (r"uemoa|umoa|bceao|zone franc|franc cfa|\bfcfa\b", "UEMOA"),
]

# Pays africains hors zone : un signal d'éloignement, pas une faute.
PAYS_HORS_ZONE = re.compile(
    r"nigeria|nig[ée]rian|cameroun|camerounais|maroc|marocain|alg[ée]rie"
    r"|tunisie|[ée]gypte|kenya|[ée]thiopie|afrique du sud|ghana|gh[ae]n[ée]en"
    r"|rdc|congo|gabon|tchad|centrafric|angola|mozambique|tanzanie|ouganda"
    r"|rwanda|zambie|zimbabwe|madagascar|maurice", re.I)

# Sujets qui touchent la cote même sans nommer d'émetteur.
SUJETS_DE_COTE = re.compile(
    r"\bbrvm\b|bourse r[ée]gionale|march[ée] financier r[ée]gional|amf-umoa"
    r"|\bcrepmf\b|introduction en bourse|emprunt obligataire|march[ée] des titres"
    r"|capitalisation boursi[èe]re|indice compos[ai]te", re.I)

# Un article qui parle d'argent public sans toucher une entreprise cotée.
BRUIT = re.compile(
    r"\bcoupe du monde\b|\bfootball\b|\bcan \d{4}\b|nomination au gouvernement"
    r"|remaniement|f[ée]te de|concours de recrutement", re.I)

# Poids. Volontairement ronds : ce sont des arbitrages, pas des mesures.
POIDS = {
    "emetteur_sujet": 40,      # la dépêche PORTE sur une société cotée
    "emetteur_cite": 12,       # elle en cite une au passage
    "plafond_emetteurs": 80,
    "theme_publication": 18,   # résultats, dividende, opération sur titre
    "theme_marche": 14,        # séance, indice, capitalisation
    "sujet_de_cote": 16,       # parle du marché régional sans nommer d'émetteur
    "secteur_cote": 10,        # touche un secteur représenté à la cote
    "plafond_secteurs": 20,
    "pays_uemoa": 8,
    "plafond_pays": 16,
    "hors_zone_sans_lien": -30,
    "bruit": -40,
}


def score_depeche(titre: str = "", texte: str = "", tickers_sujets=None,
                  tickers_cites=None, secteurs=None, theme: str = "") -> tuple:
    """Retourne (score, raisons) — `raisons` étant une liste de libellés.

    `tickers_sujets` : émetteurs cotés dont la dépêche PARLE.
    `tickers_cites`  : émetteurs simplement mentionnés.
    `secteurs`       : secteurs de la cote reconnus dans le texte.
    """
    sujets = list(tickers_sujets or [])
    cites = [t for t in (tickers_cites or []) if t not in sujets]
    secteurs = list(secteurs or [])
    bas = f"{titre or ''}\n{texte or ''}".lower()

    score = 0
    raisons = []

    if sujets:
        gain = min(len(sujets) * POIDS["emetteur_sujet"], POIDS["plafond_emetteurs"])
        score += gain
        raisons.append(f"porte sur {', '.join(sujets)} (+{gain})")
    if cites:
        gain = min(len(cites) * POIDS["emetteur_cite"],
                   POIDS["plafond_emetteurs"] - (score if sujets else 0))
        gain = max(gain, 0)
        if gain:
            score += gain
            raisons.append(f"cite {', '.join(cites)} (+{gain})")

    if theme in ("resultats", "dividende", "operation") and sujets:
        score += POIDS["theme_publication"]
        raisons.append(f"publication d'émetteur (+{POIDS['theme_publication']})")
    elif theme == "marche":
        score += POIDS["theme_marche"]
        raisons.append(f"séance ou indice (+{POIDS['theme_marche']})")

    if SUJETS_DE_COTE.search(bas):
        score += POIDS["sujet_de_cote"]
        raisons.append(f"sujet de place (+{POIDS['sujet_de_cote']})")

    if secteurs:
        gain = min(len(secteurs) * POIDS["secteur_cote"], POIDS["plafond_secteurs"])
        score += gain
        raisons.append(f"secteur {', '.join(secteurs)} (+{gain})")

    pays = [nom for motif, nom in PAYS_UEMOA if re.search(motif, bas)]
    if pays:
        gain = min(len(pays) * POIDS["pays_uemoa"], POIDS["plafond_pays"])
        score += gain
        raisons.append(f"{', '.join(pays[:3])} (+{gain})")

    # Éloignement : seulement si RIEN ne rattache la dépêche à la cote.
    rattache = bool(sujets or cites or secteurs or pays
                    or SUJETS_DE_COTE.search(bas))
    if PAYS_HORS_ZONE.search(bas) and not rattache:
        score += POIDS["hors_zone_sans_lien"]
        raisons.append(f"hors zone, sans lien avec la cote "
                       f"({POIDS['hors_zone_sans_lien']})")

    if BRUIT.search(titre or ""):
        score += POIDS["bruit"]
        raisons.append(f"hors sujet économique ({POIDS['bruit']})")

    return score, raisons
