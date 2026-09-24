#!/usr/bin/env python3
"""Mesure ce que `analysis/lecture_syscohada.py` lit vraiment.

L'ETALON

Quatorze documents dont les valeurs ont ete lues A LA MAIN dans le document
lui-meme, puis ecrites en base apres recoupement (PR #175, #176). Ce sont
donc des reponses connues, et non un jeu de test fabrique.

CE QUE LA MESURE DIT, AU 24/09/2026

  10 justes · 17 fausses · 9 absentes, sur 36 valeurs attendues.

CE QU'IL FAUT EN CONCLURE

Le lecteur N'EST PAS branche sur l'ecriture, et il ne doit pas l'etre tant
que ce rapport ne s'inverse pas. La raison tient en un exemple : pour NSIA
Banque, il rend 97 819 millions de produit net bancaire — le chiffre de
2024, pas celui de 2025. Ce montant est PLAUSIBLE : il passerait tous les
controles d'echelle et de voisinage de `completer_fondamentaux.py`. Une
valeur fausse mais vraisemblable est pire qu'une valeur absente, parce que
rien ne la signale.

Les trois causes d'erreur, par ordre d'importance :

1. L'ordre des colonnes. NSIA ecrit le comparatif d'abord, Palm CI
   l'exercice d'abord. La detection par l'en-tete ne suffit pas : beaucoup
   de ces tableaux n'ont pas d'en-tete lisible apres extraction.
2. Les montants colles. « 604 978 411 174 600 707 830 161 » porte deux
   colonnes ; le decoupage par groupes de trois chiffres en resout une
   partie, pas toutes.
3. L'OCR des scans. « RESULIAI DELEXERCICE », des chiffres coupes en deux
   (« 4 2 454 158 321 ») : le libelle n'est plus reconnaissable.

La voie sure reste celle des PR #175 et #176 : lire le document, verifier
chaque montant contre la colonne comparative et le cumul a neuf mois, puis
ecrire. Ce script existe pour mesurer le jour ou l'automatisation deviendra
fiable.

Usage :
  python3 scripts/etalon_lecture_syscohada.py
"""

sys.path.insert(0, "/Users/mdegbe/brvm-analyzer")
import pdfplumber
from analysis.lecture_syscohada import lire
from data.pdf_extractor import _ocr_document

D = os.environ.get("BRVM_DOSSIER_PDF", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "pdf_etalon"))
ETALON = {   # valeurs verifiees a la main dans les documents, en FCFA
 "NSBC": {"revenue": 112_928e6, "net_income": 40_712e6, "equity": 233_303e6},
 "SDCC": {"net_income": 4_663e6, "total_assets": 472_700e6},
 "STBC": {"revenue": 268_020_013_096, "net_income": 36_463_616_375, "total_assets": 75_047_177_792},
 "TTLS.sn_20260430_-_etats_financiers_syscohada": {"net_income": 6_146_527e3},
 "SHEC": {"revenue": 604_978_411_174, "net_income": 6_028_125_958},
 "ORGT": {"revenue": 186_609e6, "net_income": 21_643e6, "equity": 113_165e6},
 "FTSC": {"revenue": 32_108_628e3, "net_income": 465_981e3},
 "PALC": {"revenue": 197_629_996e3, "net_income": 15_508_655e3, "equity": 142_638_984e3},
 "SMBC": {"revenue": 206_740e6, "net_income": 13_075e6, "equity": 42_913e6, "total_assets": 180_003e6},
 "BNBC": {"revenue": 42_454_158_321, "net_income": 22_318_122, "equity": 17_769_953_951},
 "LNBB": {"revenue": 98_598_057_859, "net_income": 4_622_243_779, "total_assets": 32_608_867_766},
 "SICC": {"revenue": 546_774_960, "net_income": -128_639_513, "equity": 2_975_325_212},
 "SDSC": {"revenue": 92_004_268e3, "net_income": 784_970e3},
 "BOAN": {"net_income": 409e6, "equity": 37_154e6},
}
def texte_du_pdf(chemin):
    with pdfplumber.open(chemin) as pdf:
        t = "\n".join((p.extract_text() or "") for p in pdf.pages[:8])
    if len(t) > 800:
        return t, "texte"
    base = os.path.basename(chemin).split("_")[0].split(".")[0]
    cache = os.path.join(D, base + "_ocr.txt")
    if os.path.exists(cache):
        return open(cache).read(), "ocr (cache)"
    res = _ocr_document(chemin, maxi=8)
    t = res[0] if isinstance(res, tuple) else res
    open(cache, "w").write(t)
    return t, "ocr"

justes = faux = absents = 0
for prefixe, attendu in ETALON.items():
    fs = [x for x in os.listdir(D) if x.startswith(prefixe) and not x.endswith(".txt")]
    if not fs:
        print(f"  {prefixe:12} document absent"); continue
    t, mode = texte_du_pdf(os.path.join(D, fs[0]))
    lu = lire(t)
    details = []
    for champ, cible in attendu.items():
        v = lu.get(champ)
        if v is None:
            absents += 1; details.append(f"{champ}=—"); continue
        ok = abs(abs(v) - abs(cible)) <= 0.01 * abs(cible)
        justes += ok; faux += (not ok)
        details.append(f"{champ}={'OK' if ok else f'{v/1e9:.2f}≠{cible/1e9:.2f}'}")
    print(f"  {prefixe[:12]:12} [{mode:10}] " + " · ".join(details))
total = justes + faux + absents
print(f"\n{justes} juste(s) · {faux} faux · {absents} absent(s) sur {total} valeurs attendues")
