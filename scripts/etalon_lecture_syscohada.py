#!/usr/bin/env python3
"""Mesure ce que `analysis/lecture_syscohada.py` lit vraiment.

L'ETALON

Quatorze documents dont les valeurs ont ete lues A LA MAIN dans le document
lui-meme, puis ecrites en base apres recoupement (PR #175, #176). Ce sont
donc des reponses connues, et non un jeu de test fabrique.

CE QUE LA MESURE DIT

  24/09/2026, premiere version   10 justes · 17 fausses ·  9 absentes
  apres les correctifs 1 a 4      17 justes · 10 fausses ·  9 absentes
  apres les correctifs 5 a 9      27 justes ·  0 fausse  ·  9 absentes
  etalon elargi (41 valeurs)      32 justes ·  0 fausse  ·  9 absentes
  libelles de secours (SITAB, SMB) 34 justes ·  0 fausse  ·  7 absentes
  montant a la ligne (SICOR), pages
  illisibles en OCR, sans l'IFRS   36 justes ·  0 fausse  ·  5 absentes
  OCR par rangees, unite par ligne,
  montants de 3 chiffres en millions 40 justes ·  0 fausse  ·  1 absente

  L'ETALON NE SUFFIT PLUS. `scripts/recouper_par_lecteur.py` passe le
  lecteur sur les 128 exercices lisibles de `data/pdf_fondamentaux/` : deux
  pieges que l'etalon ne contenait pas y sont apparus — la regle brut-net
  prenait la colonne « variation » (N - N-1 = variation), et la regle des
  tetes coupees recollait un renvoi de note (« 4.2 1 776 443 » chez Sonatel).
  Tous deux corriges. Trois autres ensuite, toujours a grande echelle :
  l'ancre acceptait la premiere colonne a 1 % (Vivo 2025 : 604 978 et
  600 708 different de 0,7 %) — elle retient desormais la plus proche, a
  3 pour mille ; une mention « en millions » s'appliquait a un tableau en
  francs (plafond de 10^14 FCFA) ; et parmi plusieurs lignes, celle ou
  l'ancre se retrouve passe devant (Onatel).

  Au 25/09 : 171 concordances a un pour mille. Deux ecarts seulement
  opposent encore le document lu en mode sur a la fiche : SODECI 2024,
  dont l'ancre est fausse EN BASE, et SITAB 2024, ou le document ecrit
  44 730 358 142 sur trois lignes quand la fiche dit 44,174 Md.

  PAR MODE DE CHOIX DE LA COLONNE (voir `lire_detaille`) : 25 valeurs par
  l'ancre, 1 par brut - amortissements = net, 8 par l'en-tete — toutes
  justes. Hors etalon, les deux valeurs fausses de BICI Benin sont toutes
  deux lues « en-tete » : c'est le mode qui devine. Le jour ou le lecteur
  ecrira, il n'ecrira que les deux autres.

  Le 24/09 au soir, cinq valeurs lues dans les documents ont ete ajoutees
  (Vivo, Erium) — l'etalon passe a 41 valeurs. Deux d'entre elles
  contredisent la base : la, c'est la base qui a tort.

QUATRE CORRECTIFS, CHACUN MESURE

1. L'ANCRE tranche l'ordre des colonnes. Nous connaissons l'exercice
   PRECEDENT pour 98 % des titres : la colonne qui retombe dessus est le
   comparatif, l'autre est celle qu'on cherche. NSIA ecrit le comparatif
   d'abord, Palm CI l'exercice — plus besoin de deviner.
2. L'ECHELLE se lit dans l'en-tete parenthese. Le document de NSIA porte
   « (en millions FCFA) » au-dessus du tableau et « 40,7 milliards » dans le
   commentaire : chercher le plus grand multiple multipliait tout par mille.
3. LES MONTANTS COLLES se scindent par leur structure en groupes de trois.
   Quand l'ancre ne reconnait rien, une lecture de secours essaie une coupe
   plus permissive — « 97 819 112 928 » est deux montants chez NSIA, mais
   « 75 047 177 792 » en est un seul chez SITAB.
4. LES MONTANTS QUI SUIVENT LE LIBELLE, et non ceux de toute la ligne :
   « Comptes de regularisation 8 949 11 987 Capitaux propres 211 371
   233 303 » donnait 8 949 de capitaux propres.

CINQ CORRECTIFS DE PLUS, LE 24/09 AU SOIR

5. LE TOTAL AVANT LE SOUS-TOTAL. « Sous-Total Capitaux Propres part du
   groupe » contient « total capitaux propres » et precedait le vrai total
   chez Oragroup.
6. TOUT LE DOCUMENT, pas ses huit premieres pages. Chez LNB, les etats sont
   aux pages 19 a 21 : le diagnostic « montants dans des tableaux » etait
   faux, ils etaient simplement plus loin.
7. LE TABLEAU AVANT LA PROSE, ET PAS DE MILLESIMES. Une ligne d'etat porte
   l'exercice et son comparatif ; le rapport de gestion, un seul chiffre —
   ou une annee : LNB affichait un chiffre d'affaires de 2 020 francs, lu
   dans « effective depuis septembre 2020 ». Les decimales (« 1 844,6 Md »)
   sont du commentaire.
8. LES TETES COUPEES. « 4 2 454 158 321 » chez Bernabe, « 4 65 981 » chez
   Filtisac : l'espacement des caracteres coupe le premier groupe. Ce n'est
   pas l'OCR — ces documents sont du texte. La lecture recollee n'est
   retenue que si l'ancre s'y retrouve.
9. BRUT, AMORTISSEMENTS, NET. L'actif SYSCOHADA a quatre colonnes ; quand
   brut - amortissements = net au franc pres, le total est la troisieme.

CE QUI RESTE, ET POURQUOI LE LECTEUR N'ECRIT TOUJOURS PAS

Plus aucune valeur fausse sur l'etalon — mais l'etalon est aussi le jeu sur
lequel le lecteur a ete regle. HORS de ce jeu, BICI Benin donne encore deux
valeurs fausses : une meme ligne y melange francs et millions (« Total des
capitaux propres 215 986 878 257 135 118 114 620 85 176 ») et le tableau en
millions n'annonce pas son unite. Tant qu'un document inconnu peut rendre
une valeur fausse, `completer_fondamentaux.py` ne s'appuie pas sur ce
lecteur : chaque valeur passe par un recoupement.

L'absente restante, les capitaux propres de BOA Niger, n'est lue que dans
« Capitaux propres hors resultat et report a nouveau debiteur » — un poste
des fonds propres prudentiels, pas le total du bilan. Le lecteur ne le
confond pas, et c'est voulu.

La voie sure reste celle des PR #175 et #176 : lire le document, verifier
chaque montant contre la colonne comparative et le cumul a neuf mois, puis
ecrire. Ce script existe pour mesurer le jour ou l'automatisation deviendra
fiable.

Usage :
  python3 scripts/etalon_lecture_syscohada.py
"""

import os, sys, re
sys.path.insert(0, "/Users/mdegbe/brvm-analyzer")
import pdfplumber
from analysis.lecture_syscohada import lire_detaille
from data.db import read_sql_df


def ancres(ticker, exercice):
    """Ce que la base sait de l'exercice PRECEDENT.

    C'est l'ancre qui tranche l'ordre des colonnes : celle qui retombe sur ce
    que nous savons deja est le comparatif, l'autre est l'exercice cherche.
    """
    d = read_sql_df("SELECT revenue, net_income, equity, total_assets FROM fundamentals "
                    "WHERE ticker = ? AND fiscal_year = ?", params=(ticker, exercice - 1))
    if d.empty:
        return {}
    ligne = d.iloc[0]
    return {c: float(ligne[c]) for c in ("revenue", "net_income", "equity", "total_assets")
            if ligne[c] == ligne[c] and ligne[c]}
from data.pdf_extractor import _ocr_document

D = os.environ.get("BRVM_DOSSIER_PDF", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "pdf_etalon"))
ETALON = {   # valeurs verifiees a la main dans les documents, en FCFA
 "NSBC": {"revenue": 112_928e6, "net_income": 40_712e6, "equity": 233_303e6},
 "SDCC": {"net_income": 4_663e6, "total_assets": 472_700e6},
 "STBC": {"revenue": 268_020_013_096, "net_income": 36_463_616_375, "total_assets": 75_047_177_792},
 "TTLS.sn_20260430_-_etats_financiers_syscohada": {"net_income": 6_146_527e3},
 "SHEC": {"revenue": 604_978_411_174, "net_income": 6_028_125_958,
          # Lu le 24/09 : « TOTAL ACTIF 193 561 278 972 207 064 380 555 TOTAL
          # PASSIF 193 561 278 972 … ». La base porte 207 064 380 555 pour
          # 2024 ET 2025 — le comparatif recopie.
          "total_assets": 193_561_278_972},
 "ORGT": {"revenue": 186_609e6, "net_income": 21_643e6, "equity": 113_165e6},
 "FTSC": {"revenue": 32_108_628e3, "net_income": 465_981e3},
 "PALC": {"revenue": 197_629_996e3, "net_income": 15_508_655e3, "equity": 142_638_984e3},
 "SMBC": {"revenue": 206_740e6, "net_income": 13_075e6, "equity": 42_913e6, "total_assets": 180_003e6},
 "BNBC": {"revenue": 42_454_158_321, "net_income": 22_318_122, "equity": 17_769_953_951},
 "LNBB": {"revenue": 98_598_057_859, "net_income": 4_622_243_779, "total_assets": 32_608_867_766},
 "SICC": {"revenue": 546_774_960, "net_income": -128_639_513, "equity": 2_975_325_212},
 "SDSC": {"revenue": 92_004_268e3, "net_income": 784_970e3},
 "BOAN": {"net_income": 409e6, "equity": 37_154e6},
 # Ajoute le 24/09, HORS du jeu sur lequel le lecteur a ete regle. Actif
 # net = brut - amortissements = total du passif = 14 611 080 094. La base
 # porte 18 525 800 000.
 "SIVC": {"revenue": 10_074_573_973, "net_income": 179_293e3, "equity": 2_526_541_669,
          "total_assets": 14_611_080_094},
}
def _illisible(texte):
    """Une page « texte » dont la police n'a pas de table de caracteres.

    SODECI : les etats financiers sont bien du texte, mais extraits ils
    donnent « !"#"$%&'(#()'!*$%'(+' ». Moins d'une lettre sur deux parmi
    les caracteres visibles : la page doit passer par l'OCR comme un scan.
    """
    visibles = [c for c in texte if not c.isspace()]
    return len(visibles) > 200 and sum(c.isalnum() for c in visibles) < 0.5 * len(visibles)


def _ocr_pages(chemin, rangs):
    """OCR des pages, lignes reconstruites par la hauteur des mots."""
    import fitz
    from PIL import Image
    from data.pdf_extractor import _ocr_rangees
    sortie = {}
    with fitz.open(chemin) as doc:
        for i in rangs:
            pix = doc[i].get_pixmap(dpi=300)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            sortie[i] = "\n".join(_ocr_rangees(img))
    return sortie


def texte_du_pdf(chemin):
    with pdfplumber.open(chemin) as pdf:
        pages = [(p.extract_text() or "") for p in pdf.pages]
    t = "\n".join(pages)
    illisibles = [i for i, p in enumerate(pages) if _illisible(p)]
    if len(t) > 800 and illisibles:
        base = os.path.basename(chemin).split("_")[0].split(".")[0]
        cache = os.path.join(D, base + "_ocr_pages_rangees.txt")
        if not os.path.exists(cache):
            lues = _ocr_pages(chemin, illisibles)
            open(cache, "w").write("\n\f\n".join(lues[i] for i in illisibles))
        lues = open(cache).read().split("\n\f\n")
        for i, lu in zip(illisibles, lues):
            pages[i] = lu
        return "\n".join(pages), "texte+ocr"
    if len(t) > 800:
        return t, "texte"
    base = os.path.basename(chemin).split("_")[0].split(".")[0]
    cache = os.path.join(D, base + "_ocr_rangees.txt")
    if not os.path.exists(cache):
        # Quinze pages, comme `_ocr_document` en production : chez SICOR,
        # les capitaux propres sont dans le rapport des commissaires, page 10.
        lues = _ocr_pages(chemin, range(min(len(pages), 15)))
        open(cache, "w").write("\n".join(lues[i] for i in sorted(lues)))
    return open(cache).read(), "ocr rangees"

justes = faux = absents = 0
par_mode = {}
for prefixe, attendu in ETALON.items():
    fs = [x for x in os.listdir(D) if x.startswith(prefixe) and not x.endswith(".txt")]
    if not fs:
        print(f"  {prefixe:12} document absent"); continue
    t, mode = texte_du_pdf(os.path.join(D, fs[0]))
    racine = prefixe.split("_")[0]
    ticker = racine if "." in racine else racine + (
        ".sn" if racine == "TTLS" else ".tg" if racine == "ORGT"
        else ".bj" if racine in ("LNBB", "BICB") else ".ci")
    exercice = 2024 if racine == "SICC" else 2025
    lu = lire_detaille(t, ancres=ancres(ticker, exercice))
    details = []
    for champ, cible in attendu.items():
        if champ not in lu:
            absents += 1; details.append(f"{champ}=—"); continue
        v, colonne = lu[champ]
        ok = abs(abs(v) - abs(cible)) <= 0.01 * abs(cible)
        justes += ok; faux += (not ok)
        par_mode.setdefault(colonne, [0, 0])[0 if ok else 1] += 1
        marque = "" if colonne == "ancre" else f"({colonne})"
        details.append(f"{champ}={'OK' if ok else f'{v/1e9:.2f}≠{cible/1e9:.2f}'}{marque}")
    print(f"  {prefixe[:12]:12} [{mode:10}] " + " · ".join(details))
total = justes + faux + absents
print(f"\n{justes} juste(s) · {faux} faux · {absents} absent(s) sur {total} valeurs attendues")
for mode, (bons, mauvais) in sorted(par_mode.items()):
    print(f"   colonne par {mode:9} {bons:3} juste(s) · {mauvais} fausse(s)")
