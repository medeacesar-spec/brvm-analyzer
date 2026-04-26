"""Helpers de normalisation de texte (noms d'émetteurs BRVM, etc.)."""

import re

# Acronymes qui doivent rester en MAJUSCULES dans les noms d'émetteurs.
# Source : noms officiels brvm.org (cours-actions/0).
_ACRONYMS = {
    "CIE", "SMB", "BICI", "NSIA", "NEI", "CFAO", "SAPH", "SOGB", "SITAB",
    "SAFCA", "SODECI", "SODE", "SICOR", "BRVM", "EVIOSYS", "SIEM",
    "TOTALENERGIES",  # rendu en TotalEnergies plus bas (override)
}

# Mots-outils en français qui doivent rester en minuscules au milieu d'un nom.
_LOWER_MIDDLE = {
    "of", "the", "de", "du", "la", "le", "et", "pour", "des", "au",
}


def normalize_company_name(raw: str) -> str:
    """Convertit un nom d'émetteur ALL CAPS (style brvm.org) en Title Case
    lisible avec accents corrects et acronymes préservés.

    Exemples :
      "TOTALENERGIES MARKETING COTE D'IVOIRE" → "TotalEnergies Marketing Côte d'Ivoire"
      "BANK OF AFRICA SENEGAL"               → "Bank of Africa Sénégal"
      "BICI COTE D'IVOIRE"                   → "BICI Côte d'Ivoire"
    """
    if not raw:
        return raw
    s = re.sub(r"\s+", " ", str(raw).strip())
    if not s:
        return s
    s = s.title()

    # Apostrophe : "D'I" → "d'I" pour COTE D'IVOIRE
    s = re.sub(r"\bD'(?=[A-Za-z])", "d'", s)
    # L'<mot> au milieu : minuscule
    s = re.sub(r"\bL'(?=[A-Za-z])", "l'", s)

    # Country names FR avec accents corrects
    s = s.replace("Cote d'Ivoire", "Côte d'Ivoire")
    s = s.replace("Cote D'Ivoire", "Côte d'Ivoire")
    s = re.sub(r"\bSenegal\b", "Sénégal", s)
    s = re.sub(r"\bBenin\b", "Bénin", s)

    # Mots de liaison en minuscule (au milieu seulement)
    for w in _LOWER_MIDDLE:
        s = re.sub(rf"(?<=\S)\s{w.title()}\b", f" {w}", s)

    # Acronymes préservés en MAJUSCULES
    for ac in _ACRONYMS:
        s = re.sub(rf"\b{ac.title()}\b", ac, s, flags=re.IGNORECASE)

    # Branding TotalEnergies (camel case officiel)
    s = re.sub(r"\bTOTALENERGIES\b", "TotalEnergies", s)

    # Première lettre toujours en majuscule
    if s and s[0].islower():
        s = s[0].upper() + s[1:]

    return s


# ─────────────────────────────────────────────────────────────────────
# Titres de publications richbourse
# ─────────────────────────────────────────────────────────────────────

# Mots français dont les accents/apostrophes ont sauté dans les slugs URL
# de richbourse. Le scraper récupère le slug "rapport-dactivites-annuel"
# et on doit reconstituer "Rapport d'activités annuel".
_FR_WORD_FIXES = {
    # Mots avec accents
    "activites": "activités",
    "etats": "états",
    "etat": "état",
    "exercice": "exercice",
    "generale": "générale",
    "general": "général",
    "ordinaire": "ordinaire",
    "extraordinaire": "extraordinaire",
    "assemblee": "assemblée",
    "societe": "société",
    "communique": "communiqué",
    "annule": "annulé",
    "remplace": "remplacé",
    "homologation": "homologation",
    "homologue": "homologué",
    "trimestre": "trimestre",
    "semestre": "semestre",
    "trimestriel": "trimestriel",
    "semestriel": "semestriel",
    "semestrielle": "semestrielle",
    "rapport": "rapport",
    "transaction": "transaction",
    "convocation": "convocation",
    "augmentation": "augmentation",
    "capital": "capital",
    "syscohada": "SYSCOHADA",
    "previsions": "prévisions",
    "publication": "publication",
    "franchissement": "franchissement",
    "seuil": "seuil",
    "tarifs": "tarifs",
    "operation": "opération",
    "operations": "opérations",
    "fusion": "fusion",
    "scission": "scission",
    "introduction": "introduction",
    "cotation": "cotation",
    "precedent": "précédent",
    "remunerations": "rémunérations",
    "remuneration": "rémunération",
    "syndicale": "syndicale",
    "comite": "comité",
    "comites": "comités",
    "comptes": "comptes",
    "annee": "année",
    "donnees": "données",
    "renumeration": "rémunération",
    "financiere": "financière",
    "financier": "financier",
    "financiers": "financiers",
    "financieres": "financières",
    "premiere": "première",
    "premier": "premier",
    "deuxieme": "deuxième",
    "troisieme": "troisième",
    "quatrieme": "quatrième",
    "regulier": "régulier",
    "reguliere": "régulière",
    "specifique": "spécifique",
    "specifiques": "spécifiques",
    "interet": "intérêt",
    "interets": "intérêts",
    "echeance": "échéance",
    "echeances": "échéances",
    "presentation": "présentation",
    "modification": "modification",
    "verification": "vérification",
    "complete": "complète",
    "complet": "complet",
    "tresorerie": "trésorerie",
    "siege": "siège",
    "depot": "dépôt",
    "depots": "dépôts",
    "resolutions": "résolutions",
    "resolution": "résolution",
    "decision": "décision",
    "decisions": "décisions",
    "delegation": "délégation",
    "delegue": "délégué",
    "deleguee": "déléguée",
    "delegues": "délégués",
}

# Tickers / sociétés à capitaliser dans les titres (préfixes typiques)
_COMPANY_CAPS = {
    "boa": "BOA", "bici": "BICI", "nsia": "NSIA", "sib": "SIB",
    "sgbci": "SGBCI", "sgbc": "SGBC", "ecobank": "Ecobank",
    "sonatel": "Sonatel", "orange": "Orange", "uniwax": "Uniwax",
    "unilever": "Unilever", "nestle": "Nestlé", "vivo": "Vivo",
    "sicable": "Sicable", "sicor": "SICOR", "saph": "SAPH",
    "sogb": "SOGB", "sitab": "SITAB", "smb": "SMB", "safca": "SAFCA",
    "filtisac": "Filtisac", "palmci": "Palm CI", "palm": "Palm",
    "onatel": "Onatel", "oragroup": "Oragroup", "totalenergies": "TotalEnergies",
    "total": "TotalEnergies", "tractafric": "Tractafric",
    "servair": "Servair", "setao": "Setao", "solibra": "Solibra",
    "sucrivoire": "Sucrivoire", "bernabe": "Bernabé", "cfao": "CFAO",
    "cie": "CIE", "sodeci": "SODECI", "sode": "SODECI",
    "loterie": "Loterie", "nationale": "Nationale",
    "cbibf": "CBIBF", "coris": "Coris", "eviosys": "EVIOSYS",
    "siem": "SIEM", "crown": "Crown", "erium": "Erium",
    "africa": "Africa", "global": "Global", "logistics": "Logistics",
    "mansa": "Mansa", "sgi": "SGI", "eti": "ETI",
}

# Sigles de pays / suffixes
_GEO_FIXES = {
    "cote divoire": "Côte d'Ivoire",
    "cote d ivoire": "Côte d'Ivoire",
    " ci ": " Côte d'Ivoire ",  # appliqué seulement si entouré d'espaces (heuristique)
    " bf ": " Burkina Faso ",
    " sn ": " Sénégal ",
    " bj ": " Bénin ",
    " bn ": " Bénin ",   # variante richbourse pour Bénin
    " ml ": " Mali ",
    " ne ": " Niger ",
    " tg ": " Togo ",
    " bf-": " Burkina Faso-",
}


# Mots agglutinés produits par les slugs richbourse (où l'apostrophe a sauté).
# Whitelist explicite, plus sûre qu'une regex \bd<voyelle> qui casse "de"/"le".
_AGGLUTINATED_FIXES = {
    "dactivites": "d'activités",
    "dactivite": "d'activité",
    "dinformation": "d'information",
    "dinformations": "d'informations",
    "doperation": "d'opération",
    "doperations": "d'opérations",
    "daugmentation": "d'augmentation",
    "dadministration": "d'administration",
    "dadherents": "d'adhérents",
    "dappel": "d'appel",
    "dengagement": "d'engagement",
    "dexercice": "d'exercice",
    "dexploitation": "d'exploitation",
    "dintention": "d'intention",
    "dintroduction": "d'introduction",
    "dordre": "d'ordre",
    "letat": "l'état",
    "letats": "l'état",  # singulier suffit — fallback
    "limpact": "l'impact",
    "lordre": "l'ordre",
    "lassemblee": "l'assemblée",
    "lavis": "l'avis",
    "lentreprise": "l'entreprise",
    "lexercice": "l'exercice",
    "linfo": "l'info",
}


def prettify_publication_title(raw: str) -> str:
    """Reconstitue les accents et apostrophes d'un titre richbourse.

    Le scraper extrait le titre depuis le slug URL (sans accents ni
    apostrophes). Cette fonction restaure :
      - agglutinations  : "dactivites" → "d'activités" (whitelist)
      - accents FR      : "etats financiers" → "états financiers"
      - tickers/sociétés capitalisés (BOA, BICI, NSIA, ...)
      - pays            : "cote divoire" → "Côte d'Ivoire"

    Idempotente : appliquée 2× donne le même résultat.
    """
    if not raw:
        return raw
    s = re.sub(r"\s+", " ", str(raw).strip().lower())

    # 1) Agglutinations connues (whitelist — pas de \bd<voyelle> qui casse "de")
    pattern = r"\b(" + "|".join(re.escape(k) for k in _AGGLUTINATED_FIXES) + r")\b"
    s = re.sub(pattern, lambda m: _AGGLUTINATED_FIXES[m.group(0)], s)

    # 2) Mots français accentués
    pattern = r"\b(" + "|".join(re.escape(k) for k in _FR_WORD_FIXES) + r")\b"
    s = re.sub(pattern, lambda m: _FR_WORD_FIXES.get(m.group(0), m.group(0)), s)

    # 3) Pays / suffixes géographiques
    for k, v in _GEO_FIXES.items():
        s = s.replace(k, v)

    # 4) Capitalisation token par token, en préservant les apostrophes
    parts = []
    for token in s.split(" "):
        if not token:
            parts.append(token)
            continue
        low = token.lower().strip(".,;:!?")
        # Si commence par "d'" ou "l'", capitalise après l'apostrophe
        # uniquement quand le radical est un acronyme/société.
        if "'" in token and len(token) > 2:
            prefix, _, rest = token.partition("'")
            rest_low = rest.lower()
            if rest_low in _COMPANY_CAPS:
                parts.append(prefix.lower() + "'" + _COMPANY_CAPS[rest_low])
            elif rest:
                # garde "d'", "l'" en minuscule + capitalise le mot après
                parts.append(prefix.lower() + "'" + rest[:1].upper() + rest[1:])
            else:
                parts.append(token)
            continue
        if low in _COMPANY_CAPS:
            suffix = token[len(low):] if token.lower().startswith(low) else ""
            parts.append(_COMPANY_CAPS[low] + suffix)
        else:
            if low in {"de", "des", "du", "le", "la", "les", "et",
                       "pour", "par", "au", "aux", "a", "en", "sur"}:
                parts.append(low)
            else:
                parts.append(token[:1].upper() + token[1:])
    s = " ".join(parts)

    # 5) Première lettre du tout en majuscule
    if s and s[0].islower():
        s = s[0].upper() + s[1:]

    # 6) Fix accents perdus pendant capitalisation (ex: Cote → Côte)
    # Quand le mot avait été accentué dans _FR_WORD_FIXES, la capitalisation
    # ASCII l'a écrasé. On ré-applique sur les variantes capitalisées.
    fix_post_cap = {
        "Cote D'Ivoire": "Côte d'Ivoire",
        "Cote d'Ivoire": "Côte d'Ivoire",
        "Cote": "Côte",
        "Etats": "États",
        "Etat": "État",
        "Annule": "Annulé",
        "Remplace": "Remplacé",
        "Generale": "Générale",
        "General": "Général",
        "Assemblee": "Assemblée",
        "Societe": "Société",
        "Communique": "Communiqué",
        "Operation": "Opération",
        "Activites": "Activités",
        "Active": "Activé",
    }
    for k, v in fix_post_cap.items():
        s = re.sub(rf"\b{k}\b", v, s)

    # 7) Compactage final
    s = re.sub(r"\s+", " ", s).strip()
    return s
