"""Lire un poste par le chainage des comparatifs, pour les etats SYSCOHADA.

LE PRINCIPE

Un montant de l'exercice N figure dans DEUX documents : celui de N (sa
colonne) et celui de N+1 (le comparatif). Le montant commun aux deux lignes
du meme poste est donc celui de N — sans ancre, sans supposer l'ordre des
colonnes, sans lire l'en-tete. La methode a d'abord servi aux encours des
banques (`lecture_bancaire.encours`) ; elle vaut pour tout poste publie
chaque annee.

TROIS PIEGES, TROIS REGLES

  Les morceaux. Le decoupage exhaustif d'une ligne (« 1 872 917 » donne
  aussi « 1 872 » et « 872 917 ») fabrique des communs parasites : un
  commun dont les chiffres commencent ou finissent un autre commun est
  ecarte (`lecture_bancaire.chainer`).

  L'historique. Certains rapports publient trois exercices (SAPH : 2023,
  2022 et 2021). Les documents N et N+1 partagent alors aussi N-1. Un
  commun present dans le document N-1 est l'exercice N-1 : il est ecarte.
  S'il reste plus d'un candidat, rien n'est lu.

  L'unite. Elle est fixee PAR DOCUMENT, par le chiffre d'affaires : le
  facteur (1, mille, un million) qui fait retomber un montant de la ligne
  « Chiffre d'affaires » sur le chiffre d'affaires connu de la base, a un
  pour mille pres. Un document sans ce point d'appui n'est pas lu.

LE SIGNE

L'OCR perd les signes, il n'en invente pas. Un resultat d'exploitation
marque negatif (tiret ou parentheses) dans l'un des deux documents est une
perte. Les investissements et les frais financiers s'ecrivent en positif,
comme dans la base.
"""
from __future__ import annotations

import re

from analysis.lecture_bancaire import _egal, chainer, groupes
from analysis.lecture_syscohada import _plier

# LE VOCABULAIRE (25/09/2026). Chaque poste se cherche sous TOUS les libelles
# qu'il prend dans les documents de la cote : plan SYSCOHADA, normes IFRS
# (Sonatel, Orange CI), etats resumes, rapports de gestion — et sous les
# deformations de l'OCR (« emprunis », « linancieres », « deltes »,
# « immobillsations »). Les fragments ci-dessous les tolerent.
_EMPR = r"empr\w{0,4}"                  # emprunts, emprunt, emprunis, emprunls
_DETT = r"de[lt]{1,2}es?"               # dettes, deltes, delles
_FIN = r"[f(l]?inanc\w*"                # financieres, linancieres, (inancieres
_IMMO = r"imm\w*"                       # immobilisations, immobillsations, immos
_ACQ = r"(acqui\w*|achats?)"            # acquisitions, acquisltions, achats
_CORP = r"(?<!in)corpor\w*"             # corporelles, pas incorporelles
_INCORP = r"incorpor\w*"

POSTES = {
    "ebitda": re.compile(r"exc[eé]dent\s+brut\s+d.?exploitation"),
    "ebit": re.compile(r"r[eé]sultat\s+d.?exploitation"),
    # LES FRAIS FINANCIERS, du plus precis au plus large (voir VARIANTES) :
    # la ligne SYSCOHADA RM, puis les interets nommes, puis les frais
    # financiers seuls, enfin les charges financieres totales.
    "interest_expense": re.compile(
        r"frais\s+fin\w*\s+(et\s+)?charges\s+assimil\w*"
        r"|interets?\s+et\s+charges\s+assimil\w*"),
    "interets_nommes": re.compile(
        r"charges?\s+d.?\s*interets?|interets?\s+(sur|des)\s+(les\s+)?emprunts"
        r"|interets?\s+(et\s+frais\s+)?financiers|interets?\s+debiteurs|interets?\s+bancaires"
        r"|interets?\s+(sur|des)\s+dettes|cout\s+de\s+l.?endettement\s+financier\s+brut"),
    "frais_financiers": re.compile(r"frais\s+financiers(?!\s*(et|nets?\b))"),
    "charges_financieres": re.compile(
        r"(total\s+(des\s+)?)?charges\s+financieres(?!\s*nettes)"),
    # LES INVESTISSEMENTS : corporels et incorporels separes, ou une ligne
    # pour les deux.
    "capex_corporelles": re.compile(
        _ACQ + r"\s+(d.?\s*|des\s+|en\s+)?(" + _IMMO + r"|actifs?)\s+" + _CORP
        + r"|investissements?\s+" + _CORP
        + r"|" + _IMMO + r"\s+" + _CORP + r"\s+acqui\w*"),
    "capex_incorporelles": re.compile(
        _ACQ + r"\s+(d.?\s*|des\s+|en\s+)?(" + _IMMO + r"|actifs?)\s+" + _INCORP
        + r"|investissements?\s+" + _INCORP
        + r"|" + _IMMO + r"\s+" + _INCORP + r"\s+acqui\w*"),
    # Une seule ligne pour les deux (25/09/2026) : « Decaissements lies aux
    # acquisitions d'immobilisations » (SITAB), « Acquisitions
    # d'immobilisations corporelles et incorporelles » (Sonatel, Orange CI),
    # « Investissements de l'exercice », « eCapex ».
    "capex_global": re.compile(
        _ACQ + r"\s+(d.?\s*|des\s+)?" + _IMMO
        + r"\b(\s+corporelles\s+et\s+incorporelles|\s+incorporelles\s+et\s+corporelles)?"
        r"(?!\s*(corpor|incorpor|financ))"
        r"|investissements?\s+(industriels|de\s+l.?exercice|bruts|realises|totaux"
        r"|corporels\s+et\s+incorporels)"
        r"|depenses?\s+d.?investissements?|\be?capex\b"
        r"|decaissements?\s+(lies?\s+aux|sur|pour)\s+(les\s+)?investissements?"),
    # Les soldes de bilan (25/09/2026). Un total de fin N figure dans le
    # bilan N et dans le comparatif du bilan N+1, quelle que soit la mise en
    # page : c'est ce qui permet de lire les etats resumes (SITAB, Nestle),
    # qui n'ont pas les sous-totaux du bilan SYSCOHADA complet.
    "total_assets": re.compile(
        r"total\s+(gen\w*|de\s+l.?actif|du\s+bilan|bilan|actifs?|des\s+actifs|passif)"
        r"(?!\s*(immobilis|circulant|non\s*.?\s*courant|courant|et\s+capitaux|net))"
        r"|total\s+(du\s+)?passif\s+et\s+(des\s+)?capitaux\s+propres"
        r"|total\s+(des\s+)?capitaux\s+propres\s+et\s+(des\s+)?passifs?"
        r"|total\s+(de\s+l.?)?actif\s+(=|egal)"),
    "equity": re.compile(
        r"(total\s+(des\s+)?)?capitaux\s+propres(\s+et\s+ressources\s+assimilees)?"
        r"(?!\s*(part|attribuable|et\s+passif|consolid|de\s+l|au\s+|du\s+|des\s+))"),
    "cfo": re.compile(
        r"flux\s+(net\s+)?de\s+tresorerie\s+(net\s+)?(provenant|lie|genere)s?\s+(des|aux|par\s+les)"
        r"\s+activites\s+(operationnelles|d.?exploitation)"
        r"|tresorerie\s+(nette\s+)?generee\s+par\s+les\s+activites\s+d.?exploitation"),
    # LA DETTE FINANCIERE n'a pas de ligne unique : on la reconstitue par
    # ses composantes (voir DETTE_FORMULES).
    #
    # Emprunts, forme SYSCOHADA (poste DA, part courante comprise).
    "dette_emprunts": re.compile(
        _EMPR + r"\s*(et|el|&)\s*(autres\s+|aulres\s+)?" + _DETT + r"\s+" + _FIN
        + r"(\s+diverses)?"
        r"|" + _EMPR + r"\s+et\s+" + _DETT + r"\s+(aupres|envers)\s+(des\s+)?etablissements"
        r"|" + _EMPR + r"\s+et\s+" + _DETT + r"\s+(bancaires|a\s+long|a\s+moyen)"),
    # ... ou en deux lignes : « Emprunts » puis « Autres dettes financieres ».
    "dette_emprunts_seuls": re.compile(
        r"^\W*([a-z]{2}\s+)?" + _EMPR + r"(?=\s*[\d(\-])"),
    "dette_autres_fin": re.compile(
        r"(autres|aulres)\s+" + _DETT + r"\s+" + _FIN + r"(\s+diverses)?"
        r"|" + _DETT + r"\s+" + _FIN + r"\s+diverses"),
    # Etats resumes : « Dettes financieres » sur une seule ligne.
    "dette_fin_seule": re.compile(
        r"^\W*([a-z]{2}\s+)?" + _DETT + r"\s+" + _FIN + r"(?=\s*[\d(\-])"),
    # Le total SYSCOHADA DD, provisions pour risques comprises : on les
    # retranche (DETTE_FORMULES).
    "dette_dd": re.compile(
        _DETT + r"\s+" + _FIN + r"\s+et\s+(autres\s+)?ressources(\s+assimilees)?"),
    "dette_provisions": re.compile(
        r"provisions?\s+(financieres\s+)?pour\s+risques(\s+et\s+charges)?"),
    # Emprunts, forme IFRS : la part non courante...
    "dette_nc": re.compile(
        r"(" + _DETT + r"|passifs?)\s+" + _FIN
        + r"\s+(non\s*.?\s*courant|a\s+(long|moyen|plus\s+d.un\s+an))"
        r"|" + _DETT + r"\s+" + _FIN + r"\s+(a\s+)?(long|moyen)\s+(et\s+moyen\s+)?terme"
        r"|" + _EMPR + r"\s+(bancaires\s+)?(a\s+)?(long|moyen)\s+(et\s+moyen\s+)?terme"
        r"|" + _EMPR + r"\s+(a\s+)?(lt|mt|mlt)\b|" + _EMPR + r"\s+(part|portion)\s+(a\s+)?long"
        r"|" + _EMPR + r"\s+(et\s+dettes\s+)?non\s*.?\s*courants?"
        r"|" + _DETT + r"\s+(bancaires\s+)?a\s+(long|moyen)\s+terme"
        r"|credits?\s+(bancaires\s+)?a\s+(long|moyen)\s+terme"
        r"|prets\s+et\s+emprunts\s+portant\s+interets"
        r"|" + _EMPR + r"\s+obligataires?|" + _DETT + r"\s+subordonnees?"),
    # ... et la part courante.
    "dette_c": re.compile(
        r"(" + _DETT + r"|passifs?)\s+" + _FIN
        + r"\s+(courant|a\s+(court|moins\s+d.un\s+an))"
        r"|" + _DETT + r"\s+" + _FIN + r"\s+(a\s+)?court\s+terme"
        r"|" + _EMPR + r"\s+(bancaires\s+)?(a\s+)?court\s+terme|" + _EMPR + r"\s+(a\s+)?ct\b"
        r"|part\s+(courante|a\s+moins\s+d.un\s+an)\s+des\s+(emprunts|dettes)"
        r"|" + _EMPR + r"\s+(et\s+dettes\s+)?courants?"),
    # Location : SYSCOHADA (poste DB) et IFRS 16.
    "dette_location": re.compile(
        _DETT + r"\s+(de|sur|liees?\s+aux?)\s+(contrats?\s+de\s+)?(location|credit.?bail)"
        r"|" + _DETT + r"\s+de\s+location.?(acquisition|financement)"
        r"|" + _EMPR + r"\s+(lies?\s+aux|sur)\s+droits?\s+d.utilisation"
        r"|" + _DETT + r"\s+(sur|liees?\s+aux)\s+droits?\s+d.utilisation"
        r"|(" + _DETT + r"|passifs?|obligations?)\s+locati(f|ve)s?\b"),
    "dette_location_nc": re.compile(
        r"(" + _DETT + r"|passifs?|obligations?)\s+locati(f|ve)s?\s+non\s*.?\s*courant"
        r"|" + _DETT + r"\s+(de|sur)\s+location\s+non\s*.?\s*courant"),
    "dette_location_c": re.compile(
        r"(" + _DETT + r"|passifs?|obligations?)\s+locati(f|ve)s?\s+courant"
        r"|" + _DETT + r"\s+(de|sur)\s+location\s+courant"),
    # Tresorerie passif : le total SYSCOHADA (poste DT)...
    "dette_tresorerie": re.compile(r"(total\s+)?tresorerie\s*.?\s*passif"),
    # ... ou, sans ce total, ses lignes : credit bancaire court terme,
    # escompte, decouverts (IFRS : concours bancaires courants).
    "dette_banques": re.compile(
        r"banques?[,\s][^0-9]{0,44}credits?\s+de\s+tresorerie"
        r"|credits?\s+de\s+tresorerie|credits?\s+de\s+campagne"
        r"|concours\s+bancaires?(\s+courants?)?|decouverts?\s+bancaires?"
        r"|soldes?\s+crediteurs?\s+de\s+banques?"),
    "dette_escompte": re.compile(
        r"banques?[,\s][^0-9]{0,20}credits?\s+d.escompte|credits?\s+d.escompte"),
}
# Les MOUVEMENTS de la dette (tableau de flux, notes) portent les memes mots
# que ses encours. Le tableau de flux SYSCOHADA code ses lignes FA a FQ :
# « FO Emprunts », « FP Autres dettes financieres » sont des encaissements
# de l'exercice, pas un encours a la cloture.
_MOUVEMENTS = re.compile(
    r"variation|flux|encaissement|decaissement|remboursement|nouveaux|augmentation|diminution"
    r"|cout|charges|interets|emission|souscription|produits|tableau|nette\s+de"
    r"|endettement\s+net|dette\s+nette|^\W*[+\-]|^\W*f[a-q]\s|origines|emplois|ressources\s+stables")
# Une ligne qui porte le libelle mais pas le poste : les FLUX de capitaux
# propres du tableau de tresorerie, le tableau de VARIATION des capitaux
# propres, le passif « total general » d'un bilan fonctionnel...
_INTERETS_EXCLUS = re.compile(
    r"transfert|reprises|dotations|variation|flux|couverture|taux|ratio|nets?\b|nettes\b")
EXCLUSIONS = {
    # « Total du passif et des capitaux propres » est le TOTAL du bilan
    # (Sonatel 2023 : 2 573,9 Md lus comme capitaux propres).
    "equity": re.compile(r"(provenant|variation|flux|rentabilite|retour|ratio|passif|autres)"),
    "total_assets": re.compile(r"(variation|flux|tresorerie)"),
    "interest_expense": _INTERETS_EXCLUS,
    "interets_nommes": _INTERETS_EXCLUS,
    "frais_financiers": _INTERETS_EXCLUS,
    "charges_financieres": _INTERETS_EXCLUS,
    # Un taux d'investissement, une cession : pas un montant investi.
    "capex_global": re.compile(r"taux|intensite|ratio|%|cessions?|produits|financ"),
    "dette_emprunts": re.compile(_MOUVEMENTS.pattern + r"|provisions|ressources\s+assimilees"),
    "dette_emprunts_seuls": _MOUVEMENTS,
    "dette_autres_fin": re.compile(_MOUVEMENTS.pattern + r"|empr|provisions"),
    "dette_fin_seule": re.compile(_MOUVEMENTS.pattern + r"|autres|aulres|total|provisions"),
    "dette_dd": re.compile(r"variation|flux|remboursement|^\W*[+\-]"),
    "dette_provisions": re.compile(r"dotations?|reprises?|variation|flux|actif|^\W*[+\-]"),
    "dette_nc": _MOUVEMENTS,
    "dette_c": _MOUVEMENTS,
    # La ligne generique ne prend pas les parts courante et non courante :
    # elles ont leurs postes, et les additionner deux fois double la dette.
    "dette_location": re.compile(_MOUVEMENTS.pattern + r"|courant"),
    "dette_location_nc": _MOUVEMENTS,
    "dette_location_c": _MOUVEMENTS,
    "dette_banques": _MOUVEMENTS,
    "dette_escompte": _MOUVEMENTS,
    "dette_tresorerie": re.compile(r"variation|flux|nette|actif\s*-|actif\s+moins"),
}
# Un champ, plusieurs lignes possibles, de la plus precise a la plus large :
# la premiere chainee l'emporte.
VARIANTES = {
    "interest_expense": ("interest_expense", "interets_nommes", "frais_financiers",
                         "charges_financieres"),
}
# Le point d'appui de l'unite : le chiffre d'affaires, ou le produit net
# bancaire, qui en tient lieu dans la base pour les banques.
CHIFFRE_AFFAIRES = re.compile(r"chiffre\s+d.?affaires|produit\s+net\s+bancaire")
FACTEURS = (1.0, 1e3, 1e6)
SIGNES = ("ebitda", "ebit", "equity", "cfo")


def candidats(texte: str, motif) -> dict:
    """{valeur absolue en unites du document: negative ?} sur les lignes
    du poste.

    PAS DE DECOUPAGE EXHAUSTIF ICI. Il convient au bloc bancaire, ou
    l'arithmetique trie les decoupages ; seul, le chainage ne trie rien, et
    les morceaux se chainent au hasard : « -397 -1 251 » donnait un « 251 »
    qui retombait sur un autre montant, et les investissements CIE 2023
    sortaient a 7,451 Md au lieu de 8,451. On prend les decoupages du
    lecteur general (le principal et ses deux secours), et le chainage
    confirme.
    """
    from analysis.lecture_syscohada import (montants_alternatifs, montants_de_ligne,
                                            montants_tetes_fusionnees)
    sortie = {}
    exclusion = next((e for p, e in EXCLUSIONS.items() if POSTES.get(p) is motif), None)
    for ligne in texte.split("\n"):
        plie = _plier(ligne)
        if exclusion is not None and exclusion.search(plie):
            continue
        for m in motif.finditer(plie):
            # Deux tableaux cote a cote : on lit ce qui suit CE libelle,
            # jusqu'au premier mot.
            gs = groupes(ligne[m.end():])
            if not gs:
                continue
            reste = " ".join(sg + ch for sg, ch in gs)
            for lecture in (montants_de_ligne, montants_alternatifs,
                            montants_tetes_fusionnees):
                for v in lecture(reste):
                    if abs(v) >= 100:
                        sortie[abs(v)] = sortie.get(abs(v), False) or v < 0
    return sortie


# Les points d'appui de secours (25/09/2026) : 52 documents non bancaires
# n'avaient pas de ligne « Chiffre d'affaires » lisible (libelle « Ventes de
# marchandises », « Produits des activites ordinaires », tableau scanne). Le
# resultat net et le total du bilan, connus de la base, fixent l'unite aussi
# bien.
RESULTAT_NET = re.compile(r"resultat\s+net|benefice\s+net|perte\s+nette")
TOTAL_BILAN = POSTES["total_assets"]


def facteur(texte: str, revenus: list, secours: tuple = ()):
    """Le facteur qui fait retomber la ligne « Chiffre d'affaires » sur un
    chiffre d'affaires connu (celui de N ou de N-1), ou None.

    `secours` : ((motif, [valeurs connues]), ...) essayes dans l'ordre quand
    le chiffre d'affaires ne fixe pas l'unite."""
    for motif, connues in ((CHIFFRE_AFFAIRES, revenus),) + tuple(secours):
        f = _facteur(texte, motif, connues)
        if f is not None:
            return f
    return None


def _facteur(texte: str, motif, revenus: list):
    vus = candidats(texte, motif)
    trouves = {f for f in FACTEURS for v in vus for r in revenus
               if r and _egal(v * f, abs(r))}
    if len(trouves) == 1:
        return trouves.pop()
    # « 139 353 102 670 » (le chiffre d'affaires en millions, suivi du
    # comparatif) retombe en francs sur 139 353 M arrondis de la base, comme
    # « 139 353 » en millions. L'unite ANNONCEE par le document departage.
    from analysis.lecture_syscohada import _echelle
    annoncee = _echelle(texte)
    return annoncee if annoncee in trouves else None


def lire(docs: dict) -> dict:
    """{(exercice, poste): valeur en FCFA} a partir de
    docs = {exercice: {poste: {valeur FCFA: negative ?}}}."""
    sortie = {}
    for an, postes in docs.items():
        suivant = docs.get(an + 1)
        if not suivant:
            continue
        precedent = docs.get(an - 1, {})
        for poste, valeurs in postes.items():
            autres = suivant.get(poste, {})
            communs = chainer(set(valeurs), set(autres))
            anciens = set(precedent.get(poste, {}))
            communs = [c for c in communs if not any(_egal(c, x) for x in anciens)]
            if len(communs) != 1:
                continue
            v = communs[0]
            negatif = valeurs.get(v, False) or any(
                n for x, n in autres.items() if _egal(x, v))
            sortie[(an, poste)] = -v if (negatif and poste in SIGNES) else v
    return sortie


# LES FORMULES DE LA DETTE, dans l'ordre de preference. Chaque formule
# additionne (+1) ou retranche (-1) des composantes ; elle s'applique si au
# moins un poste d'emprunt est chaine et si chaque autre composante l'est
# aussi, ou n'est chiffree par aucun des deux documents (elle vaut alors
# zero). Bilan SYSCOHADA : les deux premieres ; emprunts en deux lignes :
# les deux suivantes ; IFRS : puis ; etats resumes ; et en dernier recours
# le total DD, provisions pour risques retranchees.
_TRESO = (("dette_tresorerie", 1),)
_BANQUES = (("dette_banques", 1), ("dette_escompte", 1))
_LOCATION = (("dette_location", 1),)
_LOCATION_IFRS = (("dette_location_nc", 1), ("dette_location_c", 1), ("dette_location", 1))
DETTE_FORMULES = tuple(
    emprunts + location + tresorerie
    for emprunts, location in (
        ((("dette_emprunts", 1),), _LOCATION),
        ((("dette_emprunts_seuls", 1), ("dette_autres_fin", 1)), _LOCATION),
        ((("dette_nc", 1), ("dette_c", 1)), _LOCATION_IFRS),
        ((("dette_fin_seule", 1),), _LOCATION),
        ((("dette_dd", 1), ("dette_provisions", -1)), ()),
    )
    for tresorerie in (_TRESO, _BANQUES)
)
EMPRUNTS = ("dette_emprunts", "dette_emprunts_seuls", "dette_autres_fin", "dette_nc",
            "dette_c", "dette_fin_seule", "dette_dd")
FAMILLE_TRESORERIE = ("dette_tresorerie", "dette_banques", "dette_escompte")


def dette(lus: dict, docs: dict, an: int):
    """La dette financiere de l'exercice `an`, ou None."""
    presente = lambda c: any(docs.get(a, {}).get(c) for a in (an, an + 1))
    for formule in DETTE_FORMULES:
        somme = 0.0
        for comp, signe in formule:
            v = lus.get((an, comp))
            if v is not None:
                somme += signe * abs(v)
            elif presente(comp):
                break
        else:
            noms = [c for c, _ in formule]
            if not any(lus.get((an, c)) is not None for c in noms if c in EMPRUNTS):
                continue
            # La tresorerie passif se lit par son total OU par ses lignes :
            # une formule qui ne chaine aucune des deux, alors que le
            # document en chiffre, oublierait le credit court terme.
            if any(presente(c) for c in FAMILLE_TRESORERIE) and not any(
                    lus.get((an, c)) is not None for c in noms if c in FAMILLE_TRESORERIE):
                continue
            if somme > 0:
                return somme
    return None
