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
