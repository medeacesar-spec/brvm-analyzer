"""
Extracteur de donnees financieres depuis les PDFs des etats financiers BRVM.
Telecharge les PDFs et extrait CA, Resultat Net, Capitaux Propres,
EBIT, Total Actif, Dettes, CFO, CAPEX, Charges financieres, Dividendes verses, etc.

Gere les formats:
- SYSCOHADA (REF codes: XB, XI, CP, DD, etc.)
- IFRS (labels textuels)
- Banques PCEB/OHADA (PNB, marge d'intermediation, chiffres cles)
- Rapports du CA bancaires (tableaux chiffres cles)

Inclut un fallback OCR (pytesseract, ou easyocr si installe) pour les PDFs scannes.
"""

import io
import math
import os
import re
import tempfile
import threading
from typing import Optional, List

import requests
import pdfplumber

from config import load_tickers
from data.storage import (get_report_links, save_fundamentals, get_connection,
                          champs_extraits)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

# Convention de separation des milliers, propre au document en cours.
# `_parse_amount` est appelee des centaines de fois sans contexte : le drapeau
# vit donc dans un stockage par thread — les scripts de rattrapage lisent
# quatre documents en parallele, et la convention de l'un ne doit pas
# contaminer celle du voisin.
_contexte = threading.local()


def _virgule_est_millier() -> bool:
    return getattr(_contexte, "virgule_millier", False)


def _regler_convention_virgule(texte: str) -> None:
    """Le document prouve-t-il que sa virgule separe les milliers ?

    Deux preuves internes, aucune supposition :
      1. un nombre a DEUX groupes au moins — « 1,424,257 » — qui ne se lit
         d'aucune autre facon ;
      2. un en-tete bi-devise (« milliers $EU | millions FCFA »), qui trahit
         une redaction anglo-saxonne meme quand tous les nombres de la page
         n'ont qu'un seul groupe.

    Le tableau d'activite d'Ecobank releve du second cas : « Produit net
    bancaire 6 36,223 3 56,715 ». Lu a la francaise, le produit net bancaire
    du premier trimestre 2026 tombait a 356,7 millions FCFA au lieu de
    356,7 milliards — mille fois trop petit, et incoherent avec le semestre
    suivant du meme emetteur, ecrit lui sans virgule.
    """
    try:
        if re.search(r"\d{1,3}(?:,\d{3}){2,}", texte or ""):
            _contexte.virgule_millier = True
            return
        _contexte.virgule_millier = _colonne_fcfa(texte)[0] is not None
    except Exception:
        _contexte.virgule_millier = False


def _parse_amount(text: str) -> Optional[float]:
    """Parse un montant depuis le texte PDF.

    Le point est ambigu en francais : separateur de milliers ou virgule
    decimale. Une resolution d'AG ecrit « un dividende brut de 1.940 fcfa par
    action » — lire 1,94 revient a diviser le montant par mille, en silence.
    On tranche par la forme : un point suivi d'exactement trois chiffres est un
    separateur de milliers, sauf si une virgule joue deja le role decimal.
    """
    if not text:
        return None
    brut = text.strip().replace("\xa0", "").replace(" ", "")
    negative = ("(" in brut and ")" in brut) or brut.startswith("-")
    brut = brut.replace("(", "").replace(")", "").lstrip("-")
    # L'OCR laisse des scories en fin de cellule : « 268537063| », « 1 234] »
    brut = re.sub(r"[|\]\)\}»\*]+$", "", brut).rstrip(".").rstrip("%")

    if re.match(r"^\d{1,3}(?:,\d{3}){2,}(?:\.\d+)?$", brut):
        # Convention anglo-saxonne : la virgule separe les milliers, le point
        # est decimal. Ecobank Transnational publie ainsi — « Produit net
        # bancaire 1,424,261 millions FCFA » — et la lecture francaise rendait
        # None sur CHAQUE nombre du document, donc aucune donnee.
        # On n'applique la regle qu'a partir de DEUX groupes de trois
        # chiffres : c'est la seule forme non ambigue. « 2,448 » pourrait
        # aussi bien valoir 2 448 que 2,448, et reste lu a la francaise.
        cleaned = brut.replace(",", "")
    elif _virgule_est_millier() and re.match(r"^\d+,\d{3}$", brut):
        # Un seul groupe, mais le document a deja prouve sa convention
        # (cf. `_regler_convention_virgule`) : « 356,715 » vaut 356 715.
        # La regle exige exactement trois chiffres apres la virgule, si bien
        # qu'un pourcentage francais (« 23,5 % ») ou un cours (« 1,94 ») reste
        # lu a la francaise.
        cleaned = brut.replace(",", "")
    elif "," in brut:
        # La virgule est decimale : les points ne peuvent etre que des milliers.
        cleaned = brut.replace(".", "").replace(",", ".")
    elif re.match(r"^\d{1,3}(?:\.\d{3})+$", brut):
        # 1.940 · 34.837.331.686 -> milliers
        cleaned = brut.replace(".", "")
    else:
        cleaned = brut

    if not re.match(r'^[\d.]+$', cleaned):
        return None
    try:
        val = float(cleaned)
        if negative:
            val = -val
        return val
    except (ValueError, TypeError):
        return None


def _first_line(text: str) -> str:
    if not text:
        return ""
    for line in str(text).split("\n"):
        line = line.strip()
        if line:
            return line
    return ""


# ---------------------------------------------------------------------------
# SYSCOHADA REF-code extraction
# ---------------------------------------------------------------------------

SYSCOHADA_REFS = {
    "XB": "revenue",
    "XE": "ebit",
    "XI": "net_income",
    "XD": "ebitda",
    "CP": "equity",
    "DD": "total_debt",
    "DZ": "total_assets",
    "AZ": "total_assets",
    "ZB": "cfo",
    "ZC": "capex",
}


def _extract_syscohada(all_tables: list, mult: float) -> dict:
    """Extraction basee sur les codes REF SYSCOHADA."""
    data = {}

    for table in all_tables:
        if not table or len(table) < 3:
            continue

        # Check if this table uses REF codes
        has_ref = False
        for row in table[:5]:
            if row and row[0]:
                first = str(row[0]).split("\n")[0].strip().upper()
                if first in ("REF", "AD", "AE", "CA", "TA", "XA", "ZA"):
                    has_ref = True
                    break
        if not has_ref:
            continue

        # Find the current year value column
        val_col = None
        for row in table[:3]:  # Check headers
            if not row:
                continue
            for idx, h in enumerate(row):
                h_str = str(h).strip().upper() if h else ""
                if "EXERCICE" in h_str and ("N" in h_str or "31/12" in h_str):
                    if "N-1" not in h_str:
                        val_col = idx
                        break
            if val_col is not None:
                break

        for row in table:
            if not row or not row[0]:
                continue

            ref_cell = str(row[0]).strip()
            first_ref = ref_cell.split("\n")[0].strip().upper()

            if first_ref in SYSCOHADA_REFS:
                field = SYSCOHADA_REFS[first_ref]
                if field in data:
                    continue

                # Try value columns
                cols = [val_col] if val_col else []
                cols.extend(c for c in [5, 4, 3, 2] if c not in cols)

                for c in cols:
                    if c is not None and c < len(row) and row[c]:
                        cell = _first_line(str(row[c]))
                        v = _parse_amount(cell)
                        if v is not None:
                            data[field] = v * mult
                            break

            # Interest expense: look for RM row (frais financiers)
            all_refs = ref_cell.split("\n")
            for ref_line in all_refs:
                ref = ref_line.strip().upper()
                if ref == "RM":
                    sub_idx = all_refs.index(ref_line)
                    # The value is in a following row
                    try:
                        table_idx = table.index(row)
                        target = table[table_idx + sub_idx]
                        if target:
                            cols_try = [val_col] if val_col else []
                            cols_try.extend(c for c in [5, 4, 3, 2] if c not in cols_try)
                            for c in cols_try:
                                if c is not None and c < len(target) and target[c]:
                                    v = _parse_amount(_first_line(str(target[c])))
                                    if v is not None and "interest_expense" not in data:
                                        data["interest_expense"] = abs(v) * mult
                                        break
                    except (ValueError, IndexError):
                        pass
                elif ref == "FN":
                    sub_idx = all_refs.index(ref_line)
                    try:
                        table_idx = table.index(row)
                        target = table[table_idx + sub_idx]
                        if target:
                            cols_try = [val_col] if val_col else []
                            cols_try.extend(c for c in [5, 4, 3, 2] if c not in cols_try)
                            for c in cols_try:
                                if c is not None and c < len(target) and target[c]:
                                    v = _parse_amount(_first_line(str(target[c])))
                                    if v is not None and "dividends_paid" not in data:
                                        data["dividends_paid"] = abs(v) * mult
                                        break
                    except (ValueError, IndexError):
                        pass

    return data


# ---------------------------------------------------------------------------
# Row-by-row label extraction (reliable method)
# ---------------------------------------------------------------------------

def _extract_from_tables_rowlabel(all_tables: list, mult: float) -> dict:
    """
    Extraction ligne par ligne: row[0] = label, row[val_col] = valeur.
    Methode fiable pour IFRS et SYSCOHADA sans codes REF.
    """
    data = {}

    for table in all_tables:
        if not table or len(table) < 2:
            continue

        # Detect value column from header
        header_row = table[0] if table else []
        val_col_idx = None
        for idx, h in enumerate(header_row):
            h_str = str(h).strip() if h else ""
            if re.match(r".*20\d{2}.*", h_str) or re.match(r"Exercice\s*(au\s*)?N$", h_str, re.I):
                val_col_idx = idx
                break
            if "Exercice" in h_str and "N-1" not in h_str:
                val_col_idx = idx
                break
        if val_col_idx is None:
            val_col_idx = 2 if len(header_row) > 3 else 1

        for row in table:
            if not row or not row[0]:
                continue

            label_full = str(row[0]).strip()
            label = label_full.split("\n")[0].strip().lower()

            # Get value
            val = None
            cell_text = str(row[val_col_idx]).strip() if val_col_idx < len(row) and row[val_col_idx] else ""
            cell_text = _first_line(cell_text)
            val = _parse_amount(cell_text)

            if val is None or abs(val) < 0.001:
                continue

            # Match patterns
            if re.search(r"chiffre\s*d.affaires|produits?\s*d.exploitation|revenus?\s*nets?", label):
                if "revenue" not in data:
                    data["revenue"] = val * mult

            elif re.search(r"r.sultat\s*net\b|b.n.fice\s*net", label):
                data["net_income"] = val * mult

            elif re.search(r"r.sultat\s*d.exploitation|r.sultat\s*op.rationnel", label):
                data["ebit"] = val * mult

            elif re.search(r"exc.dent\s*brut\s*d.exploitation|ebitda", label):
                data["ebitda"] = val * mult

            elif re.search(r"total\s*(?:des\s*)?capitaux\s*propres|fonds\s*propres", label):
                if "equity" not in data:
                    data["equity"] = val * mult

            elif re.search(r"capitaux\s*propres\s*(?:-|part)\s*(?:du\s*)?groupe", label):
                if "equity" not in data:
                    data["equity"] = val * mult

            elif re.search(r"total\s*(?:de\s*l.)?actif|total\s*bilan|total\s*g.n.ral", label):
                if "total_assets" not in data and "circulant" not in label:
                    data["total_assets"] = val * mult

            elif re.search(r"total\s*dettes?\s*financi.res|dettes?\s*financi.res\s*et\s*ressources", label):
                if "total_debt" not in data:
                    data["total_debt"] = abs(val) * mult

            elif re.search(r"co.t\s*de\s*l.endettement|charges?\s*d.int.r.ts?|frais\s*financiers", label):
                data["interest_expense"] = abs(val) * mult

            elif re.search(r"flux\s*(?:de\s*)?tr.sorerie.*op.ration|capacit.\s*d.autofinancement", label):
                data["cfo"] = val * mult

            elif re.search(r"flux\s*(?:de\s*)?tr.sorerie.*investissement", label):
                data["capex"] = abs(val) * mult

            elif re.search(r"dividendes?\s*vers.s|dividendes?\s*pay.s", label):
                data["dividends_paid"] = abs(val) * mult

            elif re.search(r"produit\s*net\s*bancaire", label):
                if "revenue_bank" not in data:
                    data["revenue_bank"] = val * mult

            elif re.search(r"r.sultat\s*avant\s*imp.ts?", label):
                if "ebit" not in data:
                    data["ebit"] = val * mult

    return data


# ---------------------------------------------------------------------------
# IFRS dual-column extraction (actif left, passif right)
# ---------------------------------------------------------------------------

def _extract_ifrs_dual(all_tables: list, mult: float) -> dict:
    """
    Extrait depuis les tables IFRS avec actif a gauche et passif a droite.
    Typique de CIE, Sonatel, etc.
    """
    data = {}

    for table in all_tables:
        if not table or len(table) < 5:
            continue
        num_cols = max(len(row) for row in table if row)
        if num_cols < 6:
            continue

        # Check if this is a dual actif/passif table
        is_dual = False
        for row in table[:5]:
            if not row:
                continue
            row_text = " ".join(str(c) for c in row if c).lower()
            # Detect IFRS consolidated: has both left/right columns with year headers
            if ("actif" in row_text and ("passif" in row_text or "capitaux" in row_text)):
                is_dual = True
                break
            # Detect by header structure: Note columns on both sides
            if ("capitaux propres" in row_text and ("passif" in row_text or "31 d" in row_text)):
                is_dual = True
                break
            # Check for dual year columns (left and right)
            year_count = sum(1 for c in row if c and re.search(r"31\s*[Dd].cembre|20\d{2}", str(c)))
            if year_count >= 3:  # At least 3 year headers = dual table
                is_dual = True
                break
        if not is_dual:
            continue

        # In dual tables: cols 0-3 = actif side, cols 4-7 = passif side
        # Value columns are typically 2 (actif current) and 6 (passif current)
        for row in table:
            if not row:
                continue

            # Check RIGHT side (passif) for equity, total passif
            for col_offset in range(4, min(num_cols, 8)):
                cell = row[col_offset] if col_offset < len(row) else None
                if not cell:
                    continue
                cell_str = str(cell).strip()
                for line in cell_str.split("\n"):
                    label = line.strip().lower()

                    if re.search(r"total\s*(?:des\s*)?capitaux\s*propres", label):
                        # Value is in next column(s)
                        for vc in range(col_offset + 1, min(col_offset + 3, len(row))):
                            if vc < len(row) and row[vc]:
                                v = _parse_amount(_first_line(str(row[vc])))
                                if v is not None and "equity" not in data:
                                    data["equity"] = v * mult
                                    break

                    elif re.search(r"total\s*passif\s*et\s*capitaux|total\s*passif", label):
                        for vc in range(col_offset + 1, min(col_offset + 3, len(row))):
                            if vc < len(row) and row[vc]:
                                v = _parse_amount(_first_line(str(row[vc])))
                                if v is not None and "total_assets" not in data:
                                    data["total_assets"] = v * mult
                                    break

            # Check LEFT side for total actif
            if row[0]:
                label = _first_line(str(row[0])).lower()
                if re.search(r"total\s*actif", label):
                    for vc in [2, 3, 1]:
                        if vc < len(row) and row[vc]:
                            v = _parse_amount(_first_line(str(row[vc])))
                            if v is not None and "total_assets" not in data:
                                data["total_assets"] = v * mult
                                break

    return data


# ---------------------------------------------------------------------------
# Bank report "chiffres cles" extraction
# ---------------------------------------------------------------------------

# Un poste qualifie n'est pas le poste. « Autres capitaux propres » est une
# SOUS-LIGNE du bilan, et « FLUX DE TRESORERIE PROVENANT DES CAPITAUX
# PROPRES » n'est meme pas un solde de bilan. Africa Global Logistics voyait
# ainsi ses capitaux propres ramenes a 0,71 milliard — la sous-ligne — pour un
# chiffre d'affaires de 92 milliards, et son ratio cours/actif net grimpait a
# 244. La recherche du libelle ici n'est pas ancree en debut de chaine, il
# faut donc ecarter explicitement ces prefixes.
# Ce qui precede un libelle dans une cellule de bilan a deux panneaux :
# « 4 946 Total des capitaux propres » colle la valeur du panneau gauche au
# libelle du panneau droit.
_AVANT_LIBELLE = re.compile(r"^[\s\d.,()\-\u2013\u2014']*")
# Un libelle de total se lit comme le poste qu'il totalise.
_PREFIXE_TOTAL = re.compile(r"^totaux?\s+(?:des?\s+|de\s+l[ae\u2019']\s*)?")

def _serie_pluriannuelle(cellules) -> bool:
    """Six colonnes de montants ou plus : ce n'est pas un etat financier.

    Une note d'analyse aligne l'historique et ses projections sur la meme
    ligne — « Capitaux propres 108 810 132 524 164 905 189 719 215 330 275 713
    359 475 476 382 » chez NSIA Banque. Aucune convention de position n'y
    tient : la premiere colonne est l'annee la plus ancienne, la derniere une
    prevision a quatre ans, et la valeur de l'exercice se trouve au milieu.
    Prendre l'une ou l'autre donnait 108,8 ou 476,4 milliards quand la prose
    du meme document annonce 215,3. Sans en-tete pour designer la colonne, on
    s'abstient.
    """
    montants = 0
    for cellule in cellules:
        texte = str(cellule or "")
        if "%" in texte or len(re.sub(r"\D", "", texte)) < 3:
            continue
        if _parse_amount(texte) not in (None, 0):
            montants += 1
    return montants >= 6


_LIBELLE_QUALIFIE = re.compile(
    r"^\s*(?:autres?|dont|flux|variation|mouvements?|part\b|quote[-\s]part"
    r"|sous[-\s]total|report)\b")


def _extract_bank_chiffres_cles(all_tables: list, mult: float) -> dict:
    """
    Extrait depuis les tableaux 'Chiffres Cles' dans les rapports bancaires.
    """
    data = {}

    for table in all_tables:
        if not table or len(table) < 3:
            continue

        # Check if relevant financial table (has total bilan, PNB, or resultat)
        table_text = ""
        for row in table[:5]:
            if row:
                table_text += " ".join(str(c) for c in row if c).lower() + " "

        has_financial = any(kw in table_text for kw in [
            "total bilan", "produit net bancaire", "marge bancaire",
            "chiffres cl", "bilan", "résultat net", "resultat net"
        ])
        if not has_financial:
            continue

        # For each row, try to match label and find the LAST numeric column (most recent year)
        for row in table:
            if not row or len(row) < 2:
                continue
            # Cette lecture retient la DERNIERE colonne chiffree. Sur une serie
            # pluriannuelle, c'est la projection la plus lointaine.
            if _serie_pluriannuelle(row):
                continue

            # Collect all labels from multi-line cells
            labels_cells = []
            for col_idx, cell in enumerate(row):
                if cell and len(str(cell).strip()) > 3:
                    lines = str(cell).strip().split("\n")
                    for line_idx, line in enumerate(lines):
                        labels_cells.append((col_idx, line_idx, line.strip().lower()))

            for col_idx, line_idx, label in labels_cells:
                field = None
                if re.search(r"total\s*bilan", label):
                    field = "total_assets"
                elif re.search(r"produit\s*net\s*bancaire|marge\s*bancaire\s*nette", label):
                    field = "revenue_bank"
                elif re.search(r"r.sultat\s*net\b", label):
                    field = "net_income"
                elif re.search(r"r.sultat\s*avant\s*imp", label):
                    field = "ebit"
                elif re.search(r"capitaux\s*propres|fonds\s*propres", label):
                    field = "equity"
                elif re.search(r"co.t\s*du\s*risque", label):
                    field = "cost_of_risk"

                if field and _LIBELLE_QUALIFIE.match(label):
                    continue
                if field and field not in data:
                    # Find value: look in cells to the right, matching line_idx for multi-line
                    # Use the SECOND-TO-LAST or LAST numeric cell (most recent year, before variation)
                    values_found = []
                    for try_col in range(col_idx + 1, len(row)):
                        if not row[try_col]:
                            continue
                        lines = str(row[try_col]).strip().split("\n")
                        target_line = lines[line_idx] if line_idx < len(lines) else lines[0] if lines else ""
                        v = _parse_amount(target_line.strip())
                        if v is not None:
                            values_found.append(v)

                    # Most recent year is typically the second-to-last value
                    # (last one is variation %). But for 3-column tables it's the last.
                    if values_found:
                        # If we have 3+ values, take second-to-last (before variation column)
                        # Check if last value looks like a percentage
                        if len(values_found) >= 3:
                            last = values_found[-1]
                            if abs(last) < 200:  # Likely a percentage
                                val = values_found[-2]
                            else:
                                val = values_found[-1]
                        else:
                            val = values_found[-1]
                        data[field] = val * mult

    return data


# ---------------------------------------------------------------------------
# Text extraction (fallback)
# ---------------------------------------------------------------------------

# Un montant peut s'ecrire "27 539", "70045 411" (OCR qui fusionne un groupe),
# "-2 396" ou "1 921,60". Les groupes de 3 chiffres separes par espace ou point
# sont rattaches au meme nombre ; un groupe plus court ouvre un nouveau nombre,
# ce qui permet de decouper "27 539 29 062" en deux colonnes.
# Le lookahead final est indispensable : l'OCR colle parfois deux colonnes
# ("8 640 318 8656926"), et sans lui le groupe " 865" serait rattache au
# premier nombre.
# La virgule figure dans la classe des separateurs de milliers : les
# emetteurs qui publient en anglais (Ecobank Transnational) ecrivent
# « 1,424,261 ». Sans cela le tokeniseur coupait au premier groupe.
_NUM_TOKEN = re.compile(r"-?\d+(?:[ .,]\d{3})*(?:,\d+)?(?!\d)")

# L'ordre compte : "resultat des activites ordinaires" doit etre teste AVANT
# "resultat net", sinon le motif le plus court capterait la ligne.
_TABLE_LABELS = [
    (r"chiffre\s+d.affaires", "revenue"),
    (r"produit\s+net\s+bancaire", "revenue_bank"),
    # « ordinaires » est optionnel : SOGB intercale la ligne de montants entre
    # « Résultat des activités » et sa fin de libelle. Aucune ambiguite, il n'y
    # a pas d'autre ligne commencant par « Résultat des activités » dans ces
    # tableaux (le titre « Tableau d'activité et de résultat » ne commence pas
    # la ligne par ce libelle).
    (r"r.sultat\s+des\s+activit.s(?:\s+ordinaires)?", "ordinary_income"),
    (r"r.sultat\s+hao\b", "hao_income"),
    (r"co.t\s+du\s+risque", "cost_of_risk"),
    # Lignes du compte de resultat bancaire (plan PCEB) : elles portent le
    # diagnostic que le seul resultat net ne donne pas.
    (r"frais\s+g.n.raux", "operating_expenses"),
    (r"charges\s+g.n.rales\s+d.exploitation", "operating_expenses"),
    (r"r.sultat\s+brut\s+d.exploitation", "gross_operating_income"),
    (r"r.sultat\s+avant\s+imp.t", "pretax_income"),
    # Les telecoms communiquent en EBITDAaL (« after Leases »), pas en EBITDA :
    # c'est l'indicateur de reference du secteur, celui que suit le marche.
    (r"ebitda\s*a?\s*l?\b", "ebitda"),
    (r"exc.dent\s+brut\s+d.exploitation", "ebitda"),
    (r"d.p.ts?\s+(?:de\s+la\s+)?client[eè]le", "deposits"),
    (r"ressources\s+client[eè]le", "deposits"),
    (r"cr.dits?\s+nets?\s+[àa]\s+la\s+client[eè]le", "loans"),
    (r"cr.dits?\s+[àa]\s+la\s+client[eè]le", "loans"),
    (r"r.sultat\s+net", "net_income"),
    (r"capitaux\s+propres", "equity"),
    (r"total\s+bilan", "total_assets"),
    (r"total\s+(?:de\s+l.)?actif", "total_assets"),

    # ── Variantes rencontrees d'un emetteur a l'autre ──
    # Le vocabulaire ci-dessus decrivait les libelles observes sur quelques
    # rapports. Sur un echantillon de trente publications de periode, dix-neuf
    # ne rendaient rien : les emetteurs n'ecrivent pas tous « chiffre
    # d'affaires » ni « resultat net ». Ces formes couvrent les redactions
    # rencontrees ailleurs, en texte natif comme apres OCR.
    #
    # L'ordre compte : ces motifs viennent APRES les precedents, plus
    # specifiques, et la recherche s'arrete au premier champ trouve.
    # SYSCOHADA distingue le chiffre d'affaires (compte XA) des produits
    # d'exploitation, qui lui ajoutent les subventions et la production
    # stockee. Ces deux libelles MAJORENT donc le chiffre d'affaires : ils ne
    # servent qu'a defaut du poste exact — cf. `_MOTIFS_REPLI`.
    (r"produits?\s+d.exploitation", "revenue"),
    (r"total\s+des\s+produits", "revenue"),
    (r"revenus?\s+(?:consolid|net|d.exploitation)", "revenue"),
    (r"\bpnb\b", "revenue_bank"),
    (r"b.n.fice\s+net", "net_income"),
    (r"r.sultat\s+de\s+l.exercice", "net_income"),
    (r"r.sultat\s+(?:net\s+)?part\s+du\s+groupe", "net_income"),
    (r"r.sultat\s+(?:d.exploitation|op.rationnel)", "ebit"),
    (r"charges\s+d.exploitation", "operating_expenses"),
    (r"dotations?\s+(?:nettes?\s+)?aux\s+provisions", "cost_of_risk"),
    (r"encours\s+(?:de\s+)?cr.dits?", "loans"),
    (r"pr.ts?\s+[àa]\s+la\s+client[eè]le", "loans"),
    (r"d.p.ts?\s+et\s+comptes\s+courants", "deposits"),
    (r"total\s+du\s+bilan", "total_assets"),
]


# Les extracteurs PDF coupent les libelles longs en fin de ligne : SOGB rend
# « Résultat des activités » puis « ordinaires » sur la ligne suivante, ce qui
# rend le libelle introuvable. On recolle avant d'analyser.
_LIBELLES_SCINDES = [
    (r"(activit[ée]s)\s*\n\s*(ordinaires)", r"\1 \2"),
    (r"(r[ée]sultat)\s*\n\s*(des\s+activit[ée]s)", r"\1 \2"),
    (r"(r[ée]sultat)\s*\n\s*(net|hao\b)", r"\1 \2"),
    (r"(co[uû]t)\s*\n\s*(du\s+risque)", r"\1 \2"),
    (r"(produit\s+net)\s*\n\s*(bancaire)", r"\1 \2"),
    (r"(chiffre)\s*\n\s*(d.affaires)", r"\1 \2"),
    (r"(capitaux)\s*\n\s*(propres)", r"\1 \2"),
]


def _recoller_libelles(texte: str) -> str:
    for motif, remplacement in _LIBELLES_SCINDES:
        texte = re.sub(motif, remplacement, texte, flags=re.IGNORECASE)
    return _recoller_milliers(texte)


def _recoller_milliers(texte: str) -> str:
    """Supprime les espaces parasites autour d'un separateur de milliers.

    L'extraction des etats d'Ecobank Transnational rend « 2 ,448,994 » : un
    espace s'est glisse avant la virgule. Le tokeniseur coupait alors au
    premier groupe et lisait 2 au lieu de 2 448 994.
    """
    texte = re.sub(r"(?<=\d)\s+(?=[.,]\d{3}\b)", "", texte)
    texte = re.sub(r"(?<=[.,])\s+(?=\d{3}\b)", "", texte)
    return texte


# Un montant suivi d'un marqueur de devise etrangere n'est pas un montant en
# francs. Ecobank ouvre son communique par « Produit net bancaire : +17% a
# 2,4 milliards $EU ( 1 424 milliards FCFA) » : la lecture de prose retenait
# 2,4 milliards et ce chiffre en DOLLARS ecrasait le produit net bancaire du
# tableau, 1 424 milliards FCFA — la banque paraissait cinq cents fois plus
# petite qu'elle ne l'est. L'echelle facultative laisse passer aussi bien
# « 2,4 milliards $EU » que « 801 millions de dollars » ou « 40 000 USD ».
_APRES_MONTANT_DEVISE = re.compile(
    r"^\s*(?:(?:de\s+|d')?(?:milliards?|millions?|milliers?|mds?|mio|k)\s*)?"
    r"(?:de\s+|d')?(?:\$|dollars?\b|usd\b|us\s*\$|euros?\b|eur\b)")


def _montant_en_devise_etrangere(bas: str, fin: int) -> bool:
    """Le montant qui s'acheve a `fin` est-il libelle en devise etrangere ?"""
    return bool(_APRES_MONTANT_DEVISE.match(bas[fin:fin + 30]))


def _extract_from_text(all_text: str, mult: float,
                       appliquer_plancher: bool = True) -> dict:
    """Extraction depuis le texte brut.

    Approche conservatrice : on exige un séparateur fort (`:` ou unité
    explicite type "Mds FCFA") après le label pour éviter les faux positifs
    sur les rapports en slides où plusieurs chiffres sont juxtaposés sans
    structure (Sonatel Q1 — voir notes/parser_slides.md).
    """
    data = {}
    text_lower = _recoller_libelles(all_text).lower()

    # `{0,80}` et non `*` : sans borne de distance, un label allait chercher
    # le premier `:` du document (souvent un numero de registre en pied de
    # page) et le prenait pour son montant. La borne laisse passer un retour
    # a la ligne entre le label et sa valeur, ce qui arrive souvent.
    patterns = [
        (r"r.sultat\s*net[^:]{0,80}?:\s*([\d\s,\.]+)", "net_income"),
        (r"chiffre\s*d.affaires[^:]{0,80}?:\s*([\d\s,\.]+)", "revenue"),
        (r"capitaux\s*propres[^:]{0,80}?:\s*([\d\s,\.]+)", "equity"),
        (r"produit\s*net\s*bancaire[^:]{0,80}?:\s*([\d\s,\.]+)", "revenue_bank"),
        (r"total\s*(?:de\s*l.)?actif[^:]{0,80}?:\s*([\d\s,\.]+)", "total_assets"),
        (r"total\s*bilan[^:]{0,80}?:\s*([\d\s,\.]+)", "total_assets"),
    ]

    for pattern, field in patterns:
        match = re.search(pattern, text_lower)
        if match and field not in data:
            if _montant_en_devise_etrangere(text_lower, match.end(1)):
                continue
            val = _parse_amount(match.group(1))
            if val:
                data[field] = val * mult

    # Lignes de tableau : "Chiffre d'affaires 70045 411| 69531684| ..." ou
    # "Produit Net bancaire 27 539 29 062 -1522". Aucun `:`, plusieurs colonnes.
    # On retient la PREMIERE colonne chiffree, qui est la periode courante dans
    # les "Tableau d'activité et de résultats" normalises BRVM.
    # Etats a double devise (Ecobank : "milliers $eu | millions fcfa") : la
    # premiere colonne n'est pas en FCFA. Trop risque, on laisse les autres
    # methodes decider plutot que de lire la mauvaise colonne.
    devise_etrangere = re.search(r"\$\s*eu|\busd\b|dollars?\s*(?:us|americain)",
                                 text_lower) is not None

    if not devise_etrangere:
        for line in text_lower.split("\n"):
            for label_pat, field in _TABLE_LABELS:
                if field in data:
                    continue
                m = re.match(r"\s*" + label_pat + r"\b", line)
                if not m:
                    continue
                for tok in _NUM_TOKEN.findall(line[m.end():]):
                    val = _parse_amount(tok)
                    if val is None or val == 0:
                        continue
                    # Moins de 3 chiffres = probablement un fragment de nombre
                    # mal decoupe par l'extraction ("4 93 630" lu "4"), pas un
                    # montant de tableau financier.
                    if len(re.sub(r"\D", "", tok)) < 3:
                        continue
                    # Un millesime isole ("2026") n'est pas un montant
                    if float(val).is_integer() and 1990 <= abs(val) <= 2100:
                        continue
                    montant = val * mult
                    # Plancher de plausibilite : aucune societe cotee a la BRVM
                    # n'affiche un chiffre d'affaires ou un actif de quelques
                    # centaines de francs. En dessous, c'est que l'unite du
                    # tableau n'a pas ete identifiee — mieux vaut ne rien
                    # remonter que de remonter un montant divise par un million.
                    if appliquer_plancher and abs(montant) < 100_000_000:
                        continue
                    data[field] = montant
                    break

    # Deuxieme passe : libelle SEUL sur sa ligne, montants sur les lignes
    # suivantes. Tesseract rend le meme tableau tantot ligne par ligne, tantot
    # colonne par colonne — dans le second cas on lit "Résultat net" puis
    # "8 640 318" sur la ligne d'apres. Une ligne portant PLUSIEURS libelles
    # est ignoree : impossible de savoir a qui appartient le premier montant.
    if not devise_etrangere:
        lignes = text_lower.split("\n")
        for i, ligne in enumerate(lignes):
            libelles = [(pat, champ) for pat, champ in _TABLE_LABELS
                        if re.search(pat, ligne)]
            if len(libelles) != 1:
                continue
            label_pat, field = libelles[0]
            if field in data:
                continue
            if not re.match(r"\s*" + label_pat + r"[\s:.]*$", ligne):
                continue
            for suivante in lignes[i + 1:i + 4]:
                trouve = False
                for tok in _NUM_TOKEN.findall(suivante):
                    val = _parse_amount(tok)
                    if val is None or val == 0:
                        continue
                    if len(re.sub(r"\D", "", tok)) < 3:
                        continue
                    if float(val).is_integer() and 1990 <= abs(val) <= 2100:
                        continue
                    montant = val * mult
                    if appliquer_plancher and abs(montant) < 100_000_000:
                        continue
                    data[field] = montant
                    trouve = True
                    break
                if trouve:
                    break

    # Patterns "label suivi de chiffre + unité explicite" (slides type
    # Sonatel : "Revenus Consolidés ... 504,2 Mds FCFA"). On exige l'unité
    # explicite pour éviter les confusions sur PDFs en slides.
    UNIT_PAT = (
        r"\s*(\d{1,4}(?:[,.]\d{1,3})?)\s*"
        r"(mds?|milliards?)\s*(?:fcfa|f\s*cfa)?"
    )
    slide_patterns = [
        (r"revenus?\s*consolid[^.]{0,200}?" + UNIT_PAT, "revenue", 1e9),
        (r"chiffre\s*d.affaires[^.]{0,200}?" + UNIT_PAT, "revenue", 1e9),
    ]
    for pat, field, base_mult in slide_patterns:
        if field in data:
            continue
        m = re.search(pat, text_lower)
        if m and not _montant_en_devise_etrangere(text_lower, m.end()):
            val = _parse_amount(m.group(1))
            if val and val > 1:
                data[field] = val * base_mult

    # Shares
    match = re.search(
        r"(\d[\d\s\.]+)\s*actions?\s*(?:d.une\s*valeur|de\s*nominal)",
        text_lower,
    )
    if match:
        val = _parse_amount(match.group(1))
        if val and val > 1000:
            data["shares"] = val

    return data


_MULT_WORDS = (
    (r"milliards?", 1_000_000_000),
    (r"millions?", 1_000_000),
    (r"milliers?", 1_000),
)


def _detect_multiplier(text: str):
    """Detecte l'unite dans laquelle les montants sont libelles.

    Trois niveaux, du plus fiable au plus permissif :
      1. abreviation collee — "Indicateurs en MFCFA" (BOA), KFCFA, MdFCFA
      2. declaration explicite — "en milliers de FCFA", "(en millions)"
      3. simple mention du mot, en dernier recours

    Dans les niveaux 2 et 3 on retient la mention la PLUS PRECOCE du document :
    l'en-tete du tableau fait foi, pas un commentaire situe plus bas. Sans
    cette regle, le rapport SITAB ("Indicateurs en milliers FCFA" dans le
    tableau, "70,0 milliards de FCFA" dans le commentaire) etait lu avec un
    facteur 1 000 000 de trop.
    """
    low = (text or "").lower()

    m = re.search(r"\ben\s+(md|m|k)\s*f\s*\.?\s*cfa\b", low)
    if m:
        return {"md": 1_000_000_000, "m": 1_000_000, "k": 1_000}[m.group(1)], True

    def _declaration(match):
        """Ecarte les mentions precedees d'un nombre : dans "1 268 milliards
        fcfa" l'unite decrit une phrase, pas l'en-tete d'une colonne."""
        avant = low[max(0, match.start() - 10):match.start()].rstrip()
        return not (avant and avant[-1].isdigit())

    # Le filtre _declaration ne s'applique pas au dernier niveau : c'est
    # justement lui qui doit rattraper les tournures en toutes lettres du type
    # "totalise 27 539 millions de FCFA".
    for patterns, exiger_declaration, fiable in (
        ((r"\b{w}\s*(?:de\s*)?f\s*\.?\s*cfa\b",), True, True),   # unite + devise
        ((r"\((?:en\s+)?{w}\b", r"\ben\s+{w}\b"), True, True),    # declaration
        ((r"\b{w}\s*(?:de\s*)?f\s*\.?\s*cfa\b",
          r"\b{w}\s+de\s+francs",), False, False),                  # prose
    ):
        best = None
        for word, mult in _MULT_WORDS:
            for pat in patterns:
                for m in re.finditer(pat.format(w=word), low):
                    if exiger_declaration and not _declaration(m):
                        continue
                    if best is None or m.start() < best[0]:
                        best = (m.start(), mult)
                    break
        if best is not None:
            return best[1], fiable

    return 1, True


# Libelles qui APPROCHENT un poste sans l'etre. Retenir « Produits
# d'exploitation » alors que le meme document ecrit « Chiffre d'affaires »
# quelques lignes plus loin fausse la marge autant que la croissance : la
# difference, ce sont les subventions et la production stockee.
_MOTIFS_REPLI = frozenset({
    r"produits?\s+d.exploitation",
    r"total\s+des\s+produits",
})

_MONEY_FIELDS = ("revenue", "revenue_bank", "net_income", "equity", "total_assets")


def _resolve_multiplier(text: str, lignes_cellules: list = None) -> int:
    """Multiplicateur a appliquer, indice faible arbitre par l'ordre de grandeur.

    Un en-tete ("Indicateurs en MFCFA") fait foi. Une tournure en prose
    ("s'etablit a 11 528 millions de FCFA") est bien plus fragile : SOGB
    l'ecrit alors que son tableau est deja en FCFA pleins, et l'appliquer
    multipliait le chiffre d'affaires par un million. On ne la suit donc que
    si les montants bruts sont trop petits pour etre des FCFA.
    """
    mult, fiable = _detect_multiplier(text)
    if mult == 1:
        return mult

    # La sonde doit lire les colonnes correctement, sinon deux colonnes fusionnees
    # ("122 316 132 725") gonflent le maximum et font sauter le multiplicateur.
    if lignes_cellules:
        brut = {}
        for cellules in lignes_cellules:
            if len(cellules) < 2:
                continue
            lib = cellules[0].lower().strip()
            for motif, champ in _TABLE_LABELS:
                if champ in brut or not re.match(r"\s*" + motif + r"\b", lib):
                    continue
                for cel in cellules[1:]:
                    v = _parse_amount(cel)
                    if v and len(re.sub(r"\D", "", cel)) >= 3:
                        brut[champ] = v
                        break
    else:
        brut = {}

    # La sonde par cellules ne regarde que le PREMIER champ de chaque ligne.
    # Un bilan SYSCOHADA a codes REF y met le code, jamais le libelle : « CP |
    # TOTAL CAPITAUX PROPRES ET RESSOURCES ASSIMILEES | 45 642 447 915 ». Elle
    # ne trouvait donc rien sur le rapport annuel de la SITAB, et le
    # multiplicateur de 10^9 lu quelque part dans ses soixante pages n'etait
    # pas arbitre : les capitaux propres devenaient 4,6 x 10^19 et sautaient au
    # plafond de vraisemblance. Sonde muette, on retombe sur le texte.
    if not brut:
        brut = _extract_from_text(text, 1, appliquer_plancher=False)

    valeurs = [abs(v) for k, v in brut.items() if k in _MONEY_FIELDS and v]
    maxi = max(valeurs) if valeurs else 0

    # Indice faible + montants deja en FCFA pleins : on ignore l'indice.
    if not fiable and maxi >= 100_000_000:
        return 1

    # Plafond de vraisemblance : la capitalisation de toute la BRVM tourne
    # autour de 18 000 milliards FCFA. Un poste a 100 000 milliards signale un
    # multiplicateur de trop, souvent lu dans une tournure ou le nombre est
    # colle a l'unite ("1108milliards fcfa" chez Sonatel).
    while mult > 1 and maxi * mult > 100_000_000_000_000:
        mult //= 1000

    return mult


# ---------------------------------------------------------------------------
# OCR for scanned PDFs
# ---------------------------------------------------------------------------

_ocr_reader = None


def _get_ocr_reader():
    """Reader easyocr si la lib est installee. Optionnelle : easyocr tire torch,
    trop lourd pour Streamlit Cloud free tier — pytesseract prend le relais."""
    global _ocr_reader
    if _ocr_reader is None:
        try:
            import easyocr
            _ocr_reader = easyocr.Reader(['fr', 'en'], gpu=False, verbose=False)
        except Exception:
            return None
    return _ocr_reader


def ocr_available() -> bool:
    """True si au moins un moteur OCR est utilisable."""
    try:
        import pytesseract
        pytesseract.get_tesseract_version()
        return True
    except Exception:
        pass
    try:
        import easyocr  # noqa: F401
        return True
    except ImportError:
        return False


def _ocr_image(img, psm: int = 6) -> str:
    """OCR d'une image PIL. easyocr si dispo, sinon pytesseract (tesseract-ocr
    + packs fra/eng au niveau OS, cf packages.txt).

    `psm` = page segmentation mode de tesseract. Le 6 ("bloc uniforme") rend en
    general un tableau ligne par ligne ; le 4 ("colonne de texte de taille
    variable") s'en sort mieux quand le 6 separe les libelles des chiffres.
    """
    reader = _get_ocr_reader()
    if reader is not None:
        try:
            import numpy as np
            results = reader.readtext(np.array(img), detail=0, paragraph=True)
            text = "\n".join(results)
            if text.strip():
                return text
        except Exception as e:
            print(f"OCR easyocr error: {e}")

    try:
        import pytesseract
    except ImportError:
        return ""

    gray = img.convert("L")
    # Tesseract lit mal en dessous de ~1500px de large sur du tableau financier
    w, h = gray.size
    if w < 1500:
        from PIL import Image as _Image
        scale = 1500 / w
        gray = gray.resize((int(w * scale), int(h * scale)), _Image.LANCZOS)

    for lang in ("fra+eng", "fra", "eng", None):
        try:
            if lang is None:
                return pytesseract.image_to_string(gray, config=f"--psm {psm}")
            return pytesseract.image_to_string(gray, lang=lang,
                                               config=f"--psm {psm}")
        except Exception as e:
            # Pack de langue absent → on retente avec le suivant
            last_err = e
            continue
    print(f"OCR pytesseract error: {last_err}")
    return ""


# Ancre en debut de ligne, comme le parseur de tableau : sans cela, une phrase
# du commentaire ("le resultat des activites ordinaires atteint 11,5 milliards")
# passait pour une ligne de tableau et masquait l'echec de segmentation.
_LIGNE_UTILE = re.compile(
    r"^\s*(?:chiffre\s+d.affaires|produit\s+net\s+bancaire|r.sultat|"
    r"co.t\s+du\s+risque|capitaux\s+propres|total\s+bilan)[^\d\n]{0,40}[-\u2212]?\d",
    re.IGNORECASE)


def _lignes_exploitables(texte: str) -> int:
    """Nombre de lignes ou un libelle comptable est suivi d'un montant."""
    return sum(1 for l in (texte or "").split("\n") if _LIGNE_UTILE.search(l))


def _ocr_cellules(img, psm: int = 6) -> list:
    """Lignes decoupees en cellules a partir des coordonnees rendues par l'OCR.

    Meme principe que `_lignes_par_coordonnees` pour le texte natif : sur un
    tableau scanne, l'espace qui separe deux colonnes est indistinguable de
    celui qui separe deux groupes de milliers *dans le texte*, mais pas dans
    les positions. Sans cela, un meme rapport rend un produit net bancaire de
    78,9 Mds a une extraction et 65,1 Mds a la suivante, selon la facon dont
    tesseract a recolle les colonnes ce jour-la.
    """
    try:
        import pytesseract
        from pytesseract import Output
    except ImportError:
        return []

    gris = img.convert("L")
    try:
        d = pytesseract.image_to_data(gris, lang="fra+eng",
                                      config=f"--psm {psm}", output_type=Output.DICT)
    except Exception:
        try:
            d = pytesseract.image_to_data(gris, config=f"--psm {psm}",
                                          output_type=Output.DICT)
        except Exception:
            return []

    mots = []
    for i, texte in enumerate(d.get("text", [])):
        texte = (texte or "").strip()
        if not texte:
            continue
        try:
            conf = float(d["conf"][i])
        except (ValueError, TypeError):
            conf = -1
        if conf < 40:          # en dessous, tesseract devine plus qu'il ne lit
            continue
        mots.append({
            "texte": texte, "x0": d["left"][i], "x1": d["left"][i] + d["width"][i],
            "hauteur": d["height"][i],
            "ligne": (d["block_num"][i], d["par_num"][i], d["line_num"][i]),
        })
    if not mots:
        return []

    hauteur = sorted(m["hauteur"] for m in mots)[len(mots) // 2] or 20
    ecart_colonne = 2.0 * hauteur

    groupes = {}
    for m in mots:
        groupes.setdefault(m["ligne"], []).append(m)

    out = []
    for cle in sorted(groupes):
        ligne = sorted(groupes[cle], key=lambda m: m["x0"])
        cellules, courante, precedent = [], [], None
        for m in ligne:
            if precedent is not None and m["x0"] - precedent["x1"] > ecart_colonne:
                cellules.append(" ".join(courante))
                courante = []
            courante.append(m["texte"])
            precedent = m
        if courante:
            cellules.append(" ".join(courante))
        if cellules:
            out.append(cellules)
    return out


def _extract_with_ocr_cellules(pdf_path: str) -> list:
    """Lignes-cellules de toutes les pages d'un PDF scanne."""
    try:
        import fitz
        from PIL import Image
    except ImportError:
        return []
    try:
        doc = fitz.open(pdf_path)
    except Exception:
        return []
    lignes = []
    try:
        for i in range(min(len(doc), 15)):
            try:
                pix = doc[i].get_pixmap(dpi=300)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                lignes.extend(_ocr_cellules(img))
            except Exception as e:
                print(f"OCR cellules page {i + 1}: {e}", flush=True)
    finally:
        doc.close()
    return lignes


def _ocr_document(pdf_path: str, maxi: int = 15, psm: int = 6,
                  avec_cellules: bool = True):
    """Rend chaque page UNE fois et en tire les deux lectures d'un coup.

    Le rendu d'une page a 300 ppp est cher. Or le document etait parcouru
    DEUX fois : une passe pour reconstituer les colonnes par coordonnees, une
    autre pour le texte a plat. Chaque page etait donc rasterisee deux fois
    pour rien — sur un rapport scanne de quinze pages, cela double le temps
    de lecture sans rien apporter.

    La page est ici rendue une seule fois, donnee successivement aux deux
    lecteurs, puis liberee avant la suivante : on ne garde jamais quinze
    images pleine resolution en memoire, ce qui avait deja tue trois
    traitements en local.

    `avec_cellules` permet au repli de segmentation de ne redemander que le
    texte : la lecture par coordonnees ne depend pas du mode de segmentation,
    la refaire serait du rendu perdu.

    Retourne (texte a plat, lignes de cellules).
    """
    try:
        import fitz
        from PIL import Image
    except ImportError:
        return "", []
    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        print(f"OCR: ouverture PDF impossible ({exc})", flush=True)
        return "", []

    texte, cellules = "", []
    try:
        for i in range(min(len(doc), maxi)):
            img = None
            try:
                pix = doc[i].get_pixmap(dpi=300)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                texte += _ocr_image(img, psm=psm) + "\n\n"
                if avec_cellules:
                    cellules.extend(_ocr_cellules(img))
            except Exception as exc:
                print(f"OCR page {i + 1}: {exc}", flush=True)
            finally:
                del img
    finally:
        doc.close()
    return texte, cellules


def _extract_with_ocr(pdf_path: str) -> str:
    """Rend chaque page en image via PyMuPDF puis l'OCR.

    300 dpi : en dessous, les chiffres des tableaux scannes passent mal.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError as e:
        print(f"OCR error: PyMuPDF indisponible ({e})")
        return ""

    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"OCR error: ouverture PDF ({e})")
        return ""

    def _rendu(psm):
        texte = ""
        from PIL import Image
        for i in range(min(len(doc), 15)):
            try:
                pix = doc[i].get_pixmap(dpi=300)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                texte += _ocr_image(img, psm=psm) + "\n\n"
            except Exception as e:
                print(f"OCR error page {i + 1}: {e}")
        return texte

    try:
        all_text = _rendu(6)
        # Le decoupage de tesseract n'est pas stable d'un document a l'autre :
        # il lui arrive de rendre les libelles dans un bloc et les chiffres dans
        # un autre, ce qui rend le tableau illisible ligne par ligne. Quand
        # aucune ligne "libelle + montant" ne ressort, on retente avec un autre
        # mode de segmentation plutot que d'abandonner le tableau.
        if _lignes_exploitables(all_text) == 0:
            autre = _rendu(4)
            if _lignes_exploitables(autre) > 0:
                print("OCR: segmentation psm 6 sterile, bascule en psm 4",
                      flush=True)
                all_text = autre
    finally:
        doc.close()
    return all_text


# Lecture par la PROSE, en dernier recours. Quand l'OCR desorganise le tableau
# (BOA Burkina S1-2026 : les six libelles fusionnes sur une ligne, les montants
# eparpilles), le commentaire de l'emetteur reste lisible et chiffre : « Le
# produit net bancaire totalise 27 539 millions de FCFA ». L'unite est prise
# DANS la phrase, pas dans le multiplicateur du document : c'est ce qui rend
# cette lecture independante des autres.
_ECHELLES = {
    "milliers": 1_000, "millier": 1_000,
    "millions": 1_000_000, "million": 1_000_000, "mio": 1_000_000,
    "mios": 1_000_000,
    "milliards": 1_000_000_000, "milliard": 1_000_000_000,
    # Les emetteurs abregent : Onatel ecrit « 146,18 Mds FCFA », jamais
    # « 146,18 milliards ». Sans ces formes, la prose restait muette.
    "mds": 1_000_000_000, "md": 1_000_000_000,
    "mrds": 1_000_000_000, "mrd": 1_000_000_000,
}

# Les plus longues d'abord, sinon « md » consommerait le debut de « mds ».
_MOTIF_ECHELLE = "|".join(sorted(_ECHELLES, key=len, reverse=True))

# (etiquette, champ, fenetre de recherche apres l'etiquette)
_ETIQUETTES_PROSE = [
    (r"produit\s+net\s+bancaire", "revenue_bank", 80),
    (r"chiffre\s+d.affaires", "revenue", 80),
    (r"r.sultat\s+net", "net_income", 100),
    # Les operateurs communiquent en EBITDAaL : « l'EBITDAaL s'etablit a
    # 424,1 milliards FCFA, soit 35,4 % du chiffre d'affaires ».
    (r"ebitda\s*a?l?\b", "ebitda", 90),
    (r"r.sultat\s+des\s+activit.s\s+ordinaires", "ordinary_income", 100),
    # Encours clientele : les banques BRVM les citent dans leur commentaire,
    # pas dans le tableau.
    (r"ressources\s+client[eè]le", "deposits", 120),
    (r"d[eé]p[oô]ts?\s+(?:de\s+la\s+)?client[eè]le", "deposits", 120),
    (r"cr[eé]dits?\s+nets?\s+[aà]\s+la\s+client[eè]le", "loans", 120),
]


def _extract_from_prose(texte: str) -> dict:
    """Montants cites en toutes lettres dans le commentaire de l'emetteur.

    Deux formes coexistent, et il faut les deux : le montant assorti d'une
    echelle (« 146,18 Mds FCFA ») et le montant brut en unites (« un resultat
    net beneficiaire de 15 886 921 152 FCFA »). Onatel emploie les deux dans
    le meme rapport de gestion.
    """
    bas = re.sub(r"\s+", " ", (texte or "").lower())
    trouve = {}

    def _saute_un_autre_libelle(depart: int, arrivee: int, sauf: str) -> bool:
        """Vrai si un AUTRE poste s'intercale entre l'etiquette et le montant.

        Un titre sans point final laisse la fenetre deborder sur la phrase
        suivante. Le rapport Onatel enchaine « ... DE RESULTAT NET AU 31
        DECEMBRE 2025 » puis « le chiffre d'affaires s'etablit a 146,18 Mds » :
        146,18 Mds se retrouvait enregistre comme resultat net. On refuse donc
        tout appariement qui saute par-dessus un autre libelle.
        """
        entre = bas[depart:arrivee]
        for autre, champ_autre, _ in _ETIQUETTES_PROSE:
            if champ_autre != sauf and re.search(autre, entre):
                return True
        return False

    for etiquette, champ, fenetre in _ETIQUETTES_PROSE:
        if champ in trouve:
            continue
        montant = None

        # On parcourt TOUTES les occurrences : la premiere est souvent un
        # titre, dont l'appariement est refuse. S'arreter la ferait perdre la
        # phrase du corps de texte, qui porte la vraie valeur.
        for m in re.finditer(
                etiquette + r"[^.]{0," + str(fenetre) + r"}?([\d][\d\s.,]*?)\s*"
                r"(" + _MOTIF_ECHELLE + r")\b", bas):
            if _saute_un_autre_libelle(m.start(), m.start(1), champ):
                continue
            if _montant_en_devise_etrangere(bas, m.end()):
                continue
            val = _parse_amount(m.group(1))
            if val:
                montant = val * _ECHELLES[m.group(2)]
                break

        if montant is None:
            # Montant deja exprime en francs : l'echelle vaut 1.
            for m in re.finditer(
                    etiquette + r"[^.]{0," + str(fenetre)
                    + r"}?([\d][\d\s.,]*?)\s*f\s?cfa", bas):
                if _saute_un_autre_libelle(m.start(), m.start(1), champ):
                    continue
                if _montant_en_devise_etrangere(bas, m.end()):
                    continue
                val = _parse_amount(m.group(1))
                if val:
                    montant = val
                    break

        if montant and abs(montant) >= 100_000_000:
            trouve[champ] = montant
    return trouve


# ---------------------------------------------------------------------------
# Commentaire de l'emetteur
# ---------------------------------------------------------------------------

# Les rapports d'activite BRVM suivent un plan normalise : un tableau chiffre,
# puis une section de commentaire redigee. C'est la seule prose de premiere
# main disponible sur une societe cotee — elle alimente la revue de presse.
_COMMENT_DEBUT = re.compile(
    r"(?:^|\n)\s*(?:i{1,3}|2|II)\s*[-.)\s]*\s*commentaires?\b[^\n]*",
    re.IGNORECASE,
)

# Fin de section : mentions de bas de page et formules de cloture
_COMMENT_FIN = re.compile(
    r"(?:^|\n)\s*(?:fait\s+[àa]\s|le\s+directeur|la\s+direction\s+g[ée]n[ée]rale"
    r"|si[èe]ge\s+social|soci[ée]t[ée]\s+anonyme\s+[àa]\s+conseil"
    r"|www\.|t[ée]l\s*\.?\s*:)",
    re.IGNORECASE,
)


def extract_commentary(texte: str, maxi: int = 1500) -> str:
    """Section "Commentaires" d'un rapport d'activite, ou chaine vide.

    Le texte est renvoye tel quel, sans reformulation : c'est une citation de
    l'emetteur, pas une synthese.
    """
    if not texte:
        return ""

    m = _COMMENT_DEBUT.search(texte)
    if m is None:
        return ""

    suite = texte[m.end():]
    fin = _COMMENT_FIN.search(suite)
    if fin is not None:
        suite = suite[:fin.start()]

    # Normalisation : les retours a la ligne du PDF coupent au milieu des
    # phrases, on ne garde que les vrais changements de paragraphe.
    suite = re.sub(r"\n{2,}", "\x00", suite)
    suite = re.sub(r"\s*\n\s*", " ", suite)
    suite = suite.replace("\x00", "\n")
    suite = re.sub(r"[ \t]{2,}", " ", suite).strip()

    if len(suite) < 80:
        return ""
    if len(suite) > maxi:
        coupe = suite[:maxi]
        dernier = max(coupe.rfind(". "), coupe.rfind(" ; "))
        suite = (coupe[:dernier + 1] if dernier > maxi // 2 else coupe).strip() + " […]"
    return suite


# ---------------------------------------------------------------------------
# Reconstruction des colonnes par coordonnees
# ---------------------------------------------------------------------------

def _lignes_par_coordonnees(page, ecart_colonne: float = 12.0,
                            avec_positions: bool = False) -> list:
    """Reconstitue les lignes d'un tableau en cellules, a partir des positions.

    Sans cela, « Produit Net Bancaire 122 316 132 725 +8,5% » est illisible :
    l'espace qui separe deux colonnes est le meme que celui qui separe les
    milliers, et le tokeniseur fusionne 122 316 et 132 725 en un seul nombre de
    122 milliards. Le nombre monstrueux fait ensuite sauter le multiplicateur
    puis le plancher de plausibilite, et toute la ligne est perdue.

    Les coordonnees, elles, ne mentent pas : un blanc large separe deux
    colonnes, un blanc etroit separe deux groupes de chiffres.
    """
    try:
        mots = page.extract_words(use_text_flow=False, keep_blank_chars=False)
    except Exception:
        return []

    lignes = {}
    for m in mots:
        cle = round(m["top"] / 3)          # tolerance verticale
        lignes.setdefault(cle, []).append(m)

    out = []
    for cle in sorted(lignes):
        mots_ligne = sorted(lignes[cle], key=lambda m: m["x0"])
        cellules, courante = [], []
        precedent = None

        def _fermer():
            if not courante:
                return
            texte = " ".join(m["text"] for m in courante)
            cellules.append({"texte": texte, "x0": courante[0]["x0"],
                             "x1": courante[-1]["x1"]}
                            if avec_positions else texte)

        for m in mots_ligne:
            if precedent is not None and m["x0"] - precedent["x1"] > ecart_colonne:
                _fermer()
                courante = []
            courante.append(m)
            precedent = m
        _fermer()
        if cellules:
            out.append(cellules)
    return out


# Parcs clients des operateurs telecoms. Le chiffre d'affaires n'est que la
# consequence du parc : c'est lui qui dit si l'operateur recrute ou s'erode.
_PARCS = [
    (r"clients?\s+mobile\b", "parc_mobile"),
    (r"actifs?\s+orange\s+money|clients?\s+orange\s+money|mobile\s+money",
     "parc_mobile_money"),
    (r"clients?\s+fibre", "parc_fibre"),
    (r"clients?\s+fixe\s+broadband|fixe\s+haut\s+d.bit", "parc_fixe_broadband"),
    (r"data\s+mobile", "parc_data_mobile"),
    (r"actifs?\s+4g", "parc_4g"),
    (r"parc\s+fmi", "parc_total"),
]

# Les operateurs ne presentent pas tous leurs parcs en tableau. Onatel les
# raconte : « Le nombre d'abonnes actifs passe de 12,14 millions en 2024 a
# 12,61 millions de clients en 2025 ». Sans lecture de cette prose, un
# operateur entier restait absent de la comparaison des parcs — alors que le
# recrutement de clients precede le chiffre d'affaires.
_FAMILLES_COURTES = [
    (r"(?:orange|moov|mobile)\s+money", "parc_mobile_money"),
    (r"data\s+mobile", "parc_data_mobile"),
    (r"fibre", "parc_fibre"),
    (r"mobile\b", "parc_mobile"),
    # « fixe » seul est refuse : dans le rapport integre d'Orange CI, la mise
    # en page accole « 38,5 M abonnes » et le mot « Fixe » de la puce
    # suivante, ce qui donnerait plus d'abonnes fixes que mobiles. On exige
    # une denomination explicite.
    (r"fixe\s+haut\s+d[eé]bit|internet\s+fixe|fixe\s+broadband",
     "parc_fixe_broadband"),
]

_PARCS_PROSE = [
    (r"(?:abonn[eé]s?\s+actifs?|base\s+clients?|parc\s+(?:global|total)"
     r"|nombre\s+d.abonn[eé]s?)", "parc_total"),
    (r"parc\s+(?:clients?\s+)?mobile\b", "parc_mobile"),
    (r"(?:mobile\s+money|moov\s+money|orange\s+money)", "parc_mobile_money"),
    (r"parc\s+(?:de\s+la\s+)?fibre|fibre\s+optique", "parc_fibre"),
    (r"internet\s+fixe|fixe\s+haut\s+d[eé]bit", "parc_fixe_broadband"),
    (r"parc\s+data\s+mobile", "parc_data_mobile"),
]

# En deca, ce n'est pas un parc : c'est un numero de colonne, une annee ou un
# pourcentage pris pour un effectif. Onatel rendait ainsi des « parcs » de 12.
_PARC_PLANCHER = 1_000


# Certaines societes ne publient pas un rapport mais une PRESENTATION. Sonatel
# diffuse un jeu de diapositives ou les valeurs sont posees SOUS leurs
# libelles, centrees, et non dans un tableau :
#
#     EBITDAAL      CAPEX     Résultat Net
#        242,6       83,4        113,8
#
# Ni le tableau ni la prose ne les rapprochent. Le critere qui les relie est
# l'alignement des CENTRES : mesure sur le document, l'ecart entre le centre
# du libelle et celui de sa valeur vaut 3,2 points pour EBITDAAL, 1,4 pour
# CAPEX, 0,3 pour « Résultat Net ». Un simple recouvrement, lui, laissait le
# mot « Ebitda » de la phrase d'introduction capter l'appariement.
# Chaque entree est une SUITE de motifs mot a mot, du plus specifique au plus
# general : « produit net bancaire » doit etre tente avant « produit », sans
# quoi la banque verrait son PNB pris pour autre chose. La correspondance est
# EXACTE sur chaque mot, pour qu'une occurrence en prose ne capte pas
# l'appariement.
_ETIQUETTES_DIAPO = [
    ([r"^produit$", r"^net$", r"^bancaire$"], "revenue_bank"),
    ([r"^pnb$"], "revenue_bank"),
    ([r"^chiffre$", r"^d.affaires$"], "revenue"),
    ([r"^revenus?$", r"^consolid[ée]s?$"], "revenue"),
    ([r"^r[ée]sultat$", r"^net$"], "net_income"),
    ([r"^b[ée]n[ée]fice$", r"^net$"], "net_income"),
    ([r"^r[ée]sultat$", r"^d.exploitation$"], "ebit"),
    ([r"^ebitda\s*a?l?$"], "ebitda"),
    ([r"^ebitda$"], "ebitda"),
    ([r"^capex$"], "capex"),
    ([r"^investissements?$"], "capex"),
    ([r"^co[uû]t$", r"^du$", r"^risque$"], "cost_of_risk"),
]
_ETIQUETTES_DIAPO = [([re.compile(m, re.I) for m in motifs], champ)
                     for motifs, champ in _ETIQUETTES_DIAPO]

_NOMBRE_DIAPO = re.compile(r"^-?\d{1,3}(?:[ .]\d{3})*(?:,\d+)?$")

# Au-dela, le nombre appartient a une autre colonne.
_ECART_CENTRE_MAX = 40.0
# Au-dela, il appartient a la diapositive suivante.
_ECART_VERTICAL_MAX = 90.0


def _echelle_de_page(texte: str):
    """Unite declaree sur la diapositive : « en Mds FCFA », « en millions ».

    Sans mention explicite on ne conclut pas : appliquer le multiplicateur du
    document a une diapositive qui n'en declare aucun reviendrait a inventer
    un ordre de grandeur.
    """
    bas = (texte or "").lower()
    if re.search(r"\b(?:en\s+)?(?:mds|milliards?)\b", bas):
        return 1_000_000_000
    if re.search(r"\b(?:en\s+)?millions?\b", bas):
        return 1_000_000
    if re.search(r"\b(?:en\s+)?milliers?\b", bas):
        return 1_000
    return None


def _extract_depuis_presentation(page, echelle: float) -> dict:
    """Associe chaque libelle a la valeur centree sous lui.

    Les pourcentages sont ignores : « +9,8% YoY » accompagne la valeur, il ne
    la remplace pas. Les libelles sont cherches en correspondance EXACTE sur
    le mot, pour qu'une occurrence en prose ne capte pas l'appariement.
    """
    try:
        mots = page.extract_words(use_text_flow=False, keep_blank_chars=False)
    except Exception:
        return {}
    if not mots:
        return {}

    nombres = []
    for m in mots:
        if "%" in m["text"]:
            continue
        brut = m["text"].strip()
        if not _NOMBRE_DIAPO.match(brut):
            continue
        valeur = _parse_amount(brut)
        if valeur is None or valeur == 0:
            continue
        nombres.append({"centre": (m["x0"] + m["x1"]) / 2,
                        "haut": m["top"], "valeur": valeur})
    if not nombres:
        return {}

    trouve = {}
    for i, mot in enumerate(mots):
        for motifs, champ in _ETIQUETTES_DIAPO:
            if champ in trouve:
                continue
            if i + len(motifs) > len(mots):
                continue
            suite = mots[i:i + len(motifs)]
            if not all(m.match(x["text"].strip())
                       for m, x in zip(motifs, suite)):
                continue

            centre = (min(x["x0"] for x in suite)
                      + max(x["x1"] for x in suite)) / 2
            sommet = min(x["top"] for x in suite)

            candidats = []
            for n in nombres:
                if n["haut"] <= sommet or n["haut"] - sommet > _ECART_VERTICAL_MAX:
                    continue
                ecart = abs(n["centre"] - centre)
                if ecart > _ECART_CENTRE_MAX:
                    continue
                candidats.append((ecart, n["haut"] - sommet, n["valeur"]))
            if not candidats:
                continue
            candidats.sort()
            trouve.setdefault(champ, candidats[0][2] * echelle)

    # Le chiffre d'affaires echappe a la regle des centres : il est mis en
    # avant a part, sa valeur posee a DROITE du libelle et non dessous. On
    # s'appuie alors sur une certitude comptable — un chiffre d'affaires
    # consolide depasse toujours l'EBITDA, le capex et le resultat net — donc
    # c'est le plus grand nombre de la diapositive. La regle ne s'applique que
    # si le libelle est present ET l'EBITDA deja identifie, faute de quoi on
    # ne conclut pas.
    if "revenue" not in trouve and trouve.get("ebitda"):
        texte_page = " ".join(m["text"] for m in mots).lower()
        if re.search(r"revenus?\s+consolid|chiffre\s+d.affaires", texte_page):
            plus_grand = max(n["valeur"] for n in nombres)
            if plus_grand * echelle > trouve["ebitda"]:
                trouve["revenue"] = plus_grand * echelle
    return trouve


def _extract_parcs_prose(texte: str) -> dict:
    """Parcs clients cites dans la prose du rapport de gestion.

    Deux tournures couvrent l'essentiel :
      - « passe de 12,14 millions en 2024 a 12,61 millions en 2025 », qui
        donne la valeur ET la variation, calculee plutot que recopiee ;
      - « 12,61 millions de clients », qui ne donne que la valeur.
    """
    bas = re.sub(r"\s+", " ", (texte or "").lower())
    unites = "|".join(sorted(_UNITES_PARC, key=len, reverse=True))
    trouve = {}

    # Forme inverse : le chiffre precede la famille, qui est nommee seule.
    # Orange CI ecrit « 37,6 M abonnes mobile (+8,6 %) », « 40,4 M clients
    # Orange money (+10,2 %) ». Le « M » n'est accepte qu'accole a « abonnes »
    # ou « clients », ou il ne peut signifier que million.
    for famille, champ in _FAMILLES_COURTES:
        if champ in trouve:
            continue
        m = re.search(
            r"([\d][\d\s.,]*?)\s*(m|" + unites + r")\b\s*"
            r"(?:d.)?(?:abonn[eé]s?|clients?)\s+" + famille
            + r"(?:[^.]{0,25}?\(\s*([+-]?[\d.,]+)\s*%)?", bas)
        if not m:
            continue
        valeur = _parse_amount(m.group(1))
        if not valeur:
            continue
        unite = m.group(2)
        echelle = 1_000_000 if unite == "m" else _UNITES_PARC[unite]
        variation = None
        if m.group(3):
            brut = m.group(3).strip()
            variation = _parse_amount(brut.lstrip("+-"))
            if variation is not None and brut.startswith("-"):
                variation = -variation
        trouve[champ] = {"valeur": valeur * echelle, "variation": variation}

    for etiquette, champ in _PARCS_PROSE:
        if champ in trouve:
            continue

        evolution = re.search(
            etiquette + r"[^.]{0,140}?passe\s+de\s+([\d][\d\s.,]*?)\s*"
            r"(" + unites + r")\b[^.]{0,60}?[aà]\s+([\d][\d\s.,]*?)\s*"
            r"(" + unites + r")\b", bas)
        if evolution:
            avant = _parse_amount(evolution.group(1))
            apres = _parse_amount(evolution.group(3))
            if avant and apres:
                depart = avant * _UNITES_PARC[evolution.group(2)]
                arrivee = apres * _UNITES_PARC[evolution.group(4)]
                trouve[champ] = {
                    "valeur": arrivee,
                    "variation": ((arrivee - depart) / depart * 100
                                  if depart else None),
                }
                continue

        simple = re.search(
            etiquette + r"[^.]{0,120}?([\d][\d\s.,]*?)\s*"
            r"(" + unites + r")\b\s*(?:de\s+)?(?:clients?|abonn[eé]s?)", bas)
        if simple:
            valeur = _parse_amount(simple.group(1))
            if valeur:
                trouve[champ] = {
                    "valeur": valeur * _UNITES_PARC[simple.group(2)],
                    "variation": None,
                }

    return {c: d for c, d in trouve.items()
            if d.get("valeur") and d["valeur"] >= _PARC_PLANCHER}


_UNITES_PARC = {"millions": 1_000_000, "million": 1_000_000,
                "milliers": 1_000, "k": 1_000, "millier": 1_000}


def _extract_parcs(pages_cellules: list) -> dict:
    """Parcs clients lus dans les vignettes ou tableaux d'un rapport telecom.

    La mise en page est en tuiles : une ligne de libelles, puis la valeur, puis
    l'unite, puis la variation annuelle — chaque tuile occupant sa colonne. On
    rattache donc la valeur au libelle par le RECOUVREMENT HORIZONTAL, seul
    lien fiable entre des lignes distinctes.
    """
    parcs = {}
    for lignes in pages_cellules:
        for i, cellules in enumerate(lignes):
            for cel in cellules:
                bas = cel["texte"].lower().strip()
                champ = next((c for motif, c in _PARCS if re.search(motif, bas)), None)
                if champ is None or champ in parcs:
                    continue
                # On cherche, dans les lignes suivantes, un nombre dont la
                # colonne recouvre celle du libelle.
                valeur = variation = None
                # L'unite peut etre dans le libelle — « Parc FMI (en milliers) »
                m_unite = re.search(r"\((?:en\s+)?(milliers?|millions?)\)", bas)
                unite = _UNITES_PARC.get(m_unite.group(1)) if m_unite else None

                # On parcourt toute la fenetre sans s'arreter : la variation
                # annuelle arrive apres l'unite, deux ou trois lignes plus bas.
                # Fenetre large : du texte narratif s'intercale entre le
                # libelle, la valeur et sa variation dans les mises en page en
                # tuiles. Le recouvrement horizontal reste le garde-fou.
                for suivante in lignes[i + 1:i + 9]:
                    for c2 in suivante:
                        if c2["x1"] < cel["x0"] - 5 or c2["x0"] > cel["x1"] + 5:
                            continue          # autre colonne
                        t = c2["texte"].strip()
                        if re.search(r"[-+]?[\d.,]+\s*%", t):
                            if variation is None:
                                v = re.search(r"([-+]?[\d.,]+)\s*%", t)
                                variation = _parse_amount(v.group(1).lstrip("+"))
                                if v.group(1).startswith("-") and variation:
                                    variation = -abs(variation)
                        elif valeur is None and re.fullmatch(r"[\d][\d\s.,]*", t):
                            valeur = _parse_amount(t)
                        elif unite is None and t.lower() in _UNITES_PARC:
                            unite = _UNITES_PARC[t.lower()]
                if valeur:
                    parcs[champ] = {"valeur": valeur * (unite or 1),
                                    "variation": variation}
    return parcs


_ECHELLES_DEVISE = {"milliers": 1e3, "millions": 1e6, "milliards": 1e9}


def _colonne_fcfa(texte: str):
    """Repere la colonne en francs d'un etat presente en double devise.

    Ecobank Transnational publie ses comptes sur deux devises cote a cote :
    « milliers $EU | millions FCFA | milliers $EU | millions FCFA », soit
    l'exercice courant puis le precedent. Lire la premiere colonne chiffree
    revient a prendre des dollars pour des francs — le produit net bancaire
    2025 ressortait a 2 449 Mds au lieu de 1 424 Mds, l'ecart etant le taux
    de change.

    Retourne (rang de la colonne en francs parmi les cellules chiffrees,
    multiplicateur declare) ou (None, None) si l'etat n'est pas bi-devise.
    """
    bas = (texte or "").lower()
    apres = re.search(
        r"(milliers|millions|milliards)\s*\$\s*eu\s+"
        r"(milliers|millions|milliards)\s*f\s?cfa", bas)
    if apres:
        return 1, _ECHELLES_DEVISE[apres.group(2)]
    avant = re.search(
        r"(milliers|millions|milliards)\s*f\s?cfa\s+"
        r"(milliers|millions|milliards)\s*\$\s*eu", bas)
    if avant:
        return 0, _ECHELLES_DEVISE[avant.group(1)]
    return None, None


_MOTIF_ANNEE = re.compile(r"\b(?:\d{1,2}[/-]\d{1,2}[/-])?(20\d{2})\b")
_UNITES_ENTETE = {"milliard": 1e9, "milliards": 1e9, "million": 1e6,
                  "millions": 1e6, "millier": 1e3, "milliers": 1e3}


def _contexte_des_entetes(lignes: list) -> dict:
    """Ce que l'en-tete de chaque tableau dit de ses colonnes.

    Deux suppositions courantes se revelent fausses des qu'un emetteur
    presente ses comptes autrement.

    La premiere est que l'exercice en cours occupe la premiere colonne. Le
    rapport annuel de la SIB titre ses colonnes « 31/12/2024 | 31/12/2025 » :
    l'annee ancienne est a GAUCHE. Lire la premiere colonne y rendait le
    produit net bancaire de 2024 sous l'etiquette 2025, et le total du bilan
    de 2024 — 1 685 milliards au lieu de 1 882.

    La seconde est qu'un document n'a qu'une unite. Le meme rapport en aligne
    deux : un tableau « Donnees (en milliards FCFA) » et un tableau a codes
    « C10 PRODUIT NET BANCAIRE 102 763 108 663 », en millions. Un
    multiplicateur unique ne peut pas servir les deux, et le plafond de
    vraisemblance rabattait 10^9 a 10^6 — divisant par mille le tableau en
    milliards.

    On lit donc l'en-tete : quelle colonne porte l'exercice le plus recent, et
    dans quelle unite ce tableau-ci s'exprime. Retourne, par indice de ligne,
    (rang de la colonne parmi les colonnes de valeur, unite ou None).
    """
    def _unite(cellules):
        for cel in cellules:
            m = re.search(r"\ben\s+(milliards?|millions?|milliers?)\b",
                          (cel or "").lower())
            if m:
                return _UNITES_ENTETE[m.group(1)]
        return None

    # L'unite peut preceder OU suivre la ligne de millesimes : la SIB pose
    # « 31/12/2024 | 31/12/2025 » puis, la ligne d'apres seulement,
    # « Donnees (en milliards FCFA) ». On releve donc toutes les mentions
    # d'abord, et on prend la plus proche de l'en-tete, dans un sens comme
    # dans l'autre.
    unites = {i: _unite(c) for i, c in enumerate(lignes) if c and _unite(c)}

    def _unite_voisine(i):
        for ecart in (0, 1, -1, 2, -2):
            if i + ecart in unites:
                return unites[i + ecart]
        return None

    contexte = {}
    courant = None
    for i, cellules in enumerate(lignes):
        if not cellules:
            continue

        # Le millesime peut occuper la premiere cellule : la SIB pose sa ligne
        # de dates toute seule, « 31/12/2024 | 31/12/2025 », et met l'unite sur
        # la ligne suivante. On compte donc le rang parmi les cellules PORTANT
        # une annee, sans presumer qu'un libelle occupe la premiere.
        # `findall` et non `search` : le bilan de la SIB loge ses deux dates
        # dans UNE cellule, « 31/12/2024 31/12/2025 », alors que les valeurs
        # correspondantes occupent bien deux cellules dans les lignes du
        # tableau. Ne lire qu'un millesime par cellule faisait manquer cet
        # en-tete, et le total du bilan restait celui de 2024.
        annees = [int(a) for cel in cellules if len(cel or "") <= 60
                  for a in _MOTIF_ANNEE.findall(cel or "")]
        # DEUX ou TROIS millesimes, pas davantage. Un etat financier compare
        # l'exercice a celui qui precede, parfois a l'avant-dernier. Une serie
        # de sept annees n'est pas un etat financier : c'est le tableau
        # prospectif d'une note d'analyse, « 2020 … 2024 2025 2026 », ou les
        # dernieres colonnes sont des PREVISIONS. Y suivre le millesime le plus
        # recent donnait a NSIA Banque un produit net bancaire de 117,6
        # milliards — la projection 2026 — au lieu des 97,8 realises en 2024.
        # Lire un pronostic comme un realise est pire que lire la mauvaise
        # colonne : on ne peut plus s'en apercevoir.
        distincts = sorted(set(annees))
        if 2 <= len(distincts) <= 3 and len(annees) <= 4:
            courant = (annees.index(max(annees)), _unite_voisine(i))
            continue
        if len(distincts) > 3:
            courant = None
            continue

        # La prose qui suit un tableau n'en fait pas partie : un commentaire
        # ne se lit pas en colonnes, et laisser le contexte courir jusqu'au
        # tableau suivant reviendrait a lui appliquer l'en-tete du precedent.
        if (len(cellules) == 1 and len(cellules[0] or "") > 80):
            courant = None
            continue

        if courant:
            contexte[i] = courant
    return contexte


# Les postes qui COMPOSENT les capitaux propres d'un bilan SYSCOHADA. Le
# plan comptable les enumere dans cet ordre, du capital au resultat de
# l'exercice, et leur somme EST le total.
_COMPOSANTES_CAPITAUX_PROPRES = (
    r"capital(?:\s+social|\s+souscrit)?\b",
    r"apporteurs?\s+capital\s+non\s+appel",
    r"primes?\s+(?:li[ée]es?\s+au\s+capital|de\s+fusion|et\s+r[ée]serves)",
    r"[ée]carts?\s+de\s+r[ée][ée]valuation",
    r"r[ée]serves?\s+indisponibles?",
    r"r[ée]serves?\s+libres?",
    r"r[ée]serve\s+l[ée]gale",
    # Les banques n'en detaillent pas toujours la nature : le bilan de BOA
    # Burkina porte « RESERVES » tout court. Ce motif vient APRES les formes
    # detaillees, faute de quoi il les absorberait toutes.
    r"r[ée]serves?\s*$",
    r"report\s+[àa]\s+nouveau",
    r"r[ée]sultat\s+net\s+de\s+l.exercice",
    # « RESULTAT DE L'EXERCICE (+/-) », sans « net » : meme raison d'ordre.
    r"r[ée]sultat\s+de\s+l.exercice",
    r"subventions?\s+d.investissement",
    r"provisions?\s+r[ée]glement[ée]es?",
    r"autres\s+capitaux\s+propres",
)


def _capitaux_propres_par_composantes(lignes: list, mult: float,
                                      rang: int = 0) -> Optional[float]:
    """Somme les postes qui composent les capitaux propres, faute de total.

    Tous les bilans n'ecrivent pas leur total. Celui de Nestle Cote d'Ivoire
    enumere capital, prime de fusion, ecart de reevaluation, reserves, report
    a nouveau et resultat de l'exercice — puis passe aux dettes sans totaliser.
    Filtisac fait de meme. Leurs capitaux propres etaient donc absents, alors
    que le bilan les donne poste par poste.

    C'est une identite comptable a l'interieur d'UN SEUL bilan, pas une
    estimation d'une annee sur l'autre : aucun dividende a deviner, aucune
    erreur qui se cumule. Le report « capitaux propres N-1 plus resultat N »
    se trompe en moyenne de 15 % — exactement le taux de distribution ; la
    somme des composantes, elle, se recoupe au franc pres avec les bilans qui
    portent leur total.

    Trois postes au moins, dont le capital : en dessous, ce n'est pas un bloc
    de capitaux propres mais des libelles isoles rencontres par hasard.
    """
    entetes = _contexte_des_entetes(lignes)
    reperes = []
    for indice, cellules in enumerate(lignes):
        if not cellules or len(cellules) < 2:
            continue
        if indice not in entetes and _serie_pluriannuelle(cellules[1:]):
            continue
        rang_ligne, mult_ligne = entetes.get(indice, (rang, None))
        unite = entetes.get(indice, (0, None))[1]
        mult_ligne = mult_ligne or mult
        for depart, cellule in enumerate(cellules):
            texte = (cellule or "").lower()
            if depart:
                texte = _AVANT_LIBELLE.sub("", texte)
            texte = texte.strip()
            if not texte or len(texte) > 60:
                continue
            for motif in _COMPOSANTES_CAPITAUX_PROPRES:
                if not re.match(r"\s*" + motif, texte):
                    continue
                vus = 0
                for cel in cellules[depart + 1:]:
                    val = _parse_amount(cel)
                    if (val is None or val == 0
                            or len(re.sub(r"\D", "", cel or ""))
                            < (1 if unite else 3)):
                        continue
                    if float(val).is_integer() and 1990 <= abs(val) <= 2100:
                        continue
                    if vus < rang_ligne:
                        vus += 1
                        continue
                    reperes.append((indice, motif, val * mult_ligne))
                    break
                break

    # Le bloc des capitaux propres d'un bilan est CONTIGU : capital, primes,
    # reserves, report a nouveau et resultat se suivent. Un poste isole
    # quarante lignes plus bas appartient a autre chose — chez Filtisac, la
    # note d'affectation du resultat porte son propre « report a nouveau »,
    # 468 millions qui n'ont rien a faire dans le total du bilan. On ne
    # additionne donc qu'un bloc, celui qui reunit le plus de postes.
    blocs, courant = [], []
    for repere in reperes:
        if courant and repere[0] - courant[-1][0] > 8:
            blocs.append(courant)
            courant = []
        courant.append(repere)
    if courant:
        blocs.append(courant)

    def _distincts(bloc):
        return {motif: valeur for _, motif, valeur in bloc}

    if not blocs:
        return None
    meilleur = max(blocs, key=lambda b: len(_distincts(b)))
    trouves = _distincts(meilleur)

    if len(trouves) < 3 or not any(m.startswith("capital") for m in trouves):
        return None
    total = sum(trouves.values())
    return total if abs(total) >= 1e8 else None


def _extract_from_cells(lignes: list, mult: float, rang: int = 0,
                        rang_impose: bool = False,
                        declares: set = None) -> dict:
    """Lit les libelles connus dans des lignes deja decoupees en cellules.

    `rang_impose` protege le cas bi-devise : quand `_colonne_fcfa` a deja
    designe la colonne en francs, l'en-tete du tableau n'a pas a la
    contredire. Ecobank titre ses colonnes « 30 Juin 2026 | 30 Juin 2025 »
    une ligne au-dessus de « milliers $EU | millions FCFA » : suivre les
    millesimes y ramenerait la colonne en dollars.

    `declares` recueille les champs dont l'unite vient de l'EN-TETE du tableau
    plutot que d'une sonde sur l'ensemble du document. La fusion s'en sert
    d'arbitre : une unite lue prime sur une unite devinee.
    """
    entetes = {} if rang_impose else _contexte_des_entetes(lignes)
    if declares is None:
        declares = set()
    data = {}

    def _valeur_apres(cellules, depart, rang_ligne, mult_ligne, chiffres_min):
        """Premier montant exploitable a partir de la cellule `depart`.

        Si la premiere colonne chiffree est inexploitable, on abandonne le
        libelle : continuer reviendrait a prendre la colonne suivante, qui est
        un autre exercice ou une variation. Sur un scan SITAB, cette errance
        ramenait 513 728 — la colonne « variation en valeur » — comme chiffre
        d'affaires.
        """
        vus = 0
        for cel in cellules[depart:]:
            val = _parse_amount(cel)
            if (val is None or val == 0
                    or len(re.sub(r"\D", "", cel or "")) < chiffres_min):
                continue                          # cellule vide ou parasite
            if float(val).is_integer() and 1990 <= abs(val) <= 2100:
                continue                          # millesime
            # `rang` vaut 0 dans le cas courant. Il vaut 1 sur un etat
            # bi-devise ou la colonne en francs suit celle en dollars, ou
            # lorsque l'en-tete place l'exercice recent en deuxieme position.
            if vus < rang_ligne:
                vus += 1
                continue
            montant = val * mult_ligne
            return montant if abs(montant) >= 100_000_000 else None
        return None

    def _candidats(cellules):
        """Les couples (indice de cellule, libelle) a confronter aux motifs.

        Un bilan SYSCOHADA se presente en DEUX PANNEAUX, actif a gauche et
        passif a droite, et la ligne rendue melange les deux : « Actifs
        circulants HAO | 0 | 0 CHIFFRE D'AFFAIRES | 197 629 996 ». Le poste du
        panneau droit etait invisible tant qu'on ne regardait que la premiere
        cellule. Les cellules suivantes sont donc essayees, debarrassees de ce
        qui precede le libelle, et un libelle de total se lit comme le poste
        qu'il totalise — « Total des capitaux propres » vaut « capitaux
        propres ».
        """
        couples = [(0, (cellules[0] or "").lower().strip())]
        for indice_cel, cellule in enumerate(cellules[1:], start=1):
            texte = _AVANT_LIBELLE.sub("", (cellule or "").lower()).strip()
            if not texte or len(texte) > 60 or _LIBELLE_QUALIFIE.match(texte):
                continue
            couples.append((indice_cel, texte))
            sans_total = _PREFIXE_TOTAL.sub("", texte)
            if sans_total != texte:
                couples.append((indice_cel, sans_total))
        return couples

    # Deux passes : les libelles exacts d'abord, les approximations
    # ensuite. Sans cela, l'ordre des lignes du document decidait — un
    # « Total des produits d'exploitation » place avant le « Chiffre
    # d'affaires » l'emportait sur lui.
    for autoriser_repli in (False, True):
        for indice, cellules in enumerate(lignes):
            # Une cellule unique n'est pas forcement une ligne vide : ce peut etre
            # un libelle dont les chiffres sont tombes sur la ligne suivante. On la
            # garde si elle est COURTE — un libelle de poste comptable, pas une
            # phrase de commentaire qui irait ensuite s'emparer des chiffres d'un
            # tableau voisin.
            if not cellules or (len(cellules) < 2 and len(cellules[0] or "") > 60):
                continue
            rang_ligne, mult_ligne = entetes.get(indice, (rang, None))
            unite_declaree = entetes.get(indice, (0, None))[1]
            mult_ligne = mult_ligne or mult

            # Une ligne a SIX colonnes de montants n'est pas un etat financier :
            # c'est la serie pluriannuelle d'une note d'analyse, ou les dernieres
            # colonnes sont des projections et la PREMIERE l'annee la plus
            # ancienne. « Capitaux propres 108 810 132 524 164 905 189 719 215 330
            # 275 713 359 475 476 382 » chez NSIA Banque : prendre la premiere
            # colonne rendait 108,8 milliards quand la prose du meme document
            # annonce 215,3 pour l'exercice en cours. Sans en-tete pour designer
            # la bonne colonne, aucune convention de position ne tient — on
            # s'abstient. La regle ne s'applique pas quand `_colonne_fcfa` a deja
            # tranche : un etat bi-devise aligne legitimement six colonnes.
            if (not rang_impose and indice not in entetes
                    and _serie_pluriannuelle(cellules[1:])):
                continue
            # Trois chiffres au moins, sauf quand l'en-tete a declare l'unite du
            # tableau : « Donnees (en milliards FCFA) » rend « -3,6 » parfaitement
            # lisible, et l'exiger a trois chiffres faisait disparaitre le cout du
            # risque de la SIB.
            chiffres_min = 1 if unite_declaree else 3

            for depart, libelle in _candidats(cellules):
                for motif, champ in _TABLE_LABELS:
                    if champ in data:
                        continue
                    if (motif in _MOTIFS_REPLI) != autoriser_repli:
                        continue
                    if not re.match(r"\s*" + motif + r"\b", libelle):
                        continue

                    montant = _valeur_apres(cellules, depart + 1, rang_ligne,
                                            mult_ligne, chiffres_min)
                    if montant is not None:
                        data[champ] = montant
                        if unite_declaree:
                            declares.add(champ)
                        break

                    # Libelle seul sur sa ligne, chiffres sur la suivante. La SIB
                    # ecrit « Resultat Brut d'exploitation » puis, ligne d'apres,
                    # « 62,6 | 66,7 | 4,1 | 7% ». La ligne d'apres doit commencer
                    # par un NOMBRE : la CIE pose « Capitaux propres - part
                    # Groupe » puis « Interets ne conferant pas le controle | 209 |
                    # 189 », qui appartient a un AUTRE poste — s'y servir donnait
                    # 0,209 milliard de capitaux propres a une societe qui en
                    # declare 43.
                    if depart or len(cellules) > 1 or indice + 1 >= len(lignes):
                        break
                    suivante = lignes[indice + 1]
                    if not suivante or _parse_amount(suivante[0]) is None:
                        break
                    rang_suite, mult_suite = entetes.get(indice + 1, (rang, None))
                    unite_suite = entetes.get(indice + 1, (0, None))[1]
                    montant = _valeur_apres(suivante, 0, rang_suite,
                                            mult_suite or mult,
                                            1 if unite_suite else 3)
                    if montant is not None:
                        data[champ] = montant
                        if unite_suite:
                            declares.add(champ)
                    break
    return data


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

# Pages depouillees d'office, puis limite au-dela de laquelle on ne cherche
# plus. Un rapport annuel BRVM depasse rarement quatre-vingts pages.
_PAGES_PREMIERE_PASSE = 20
_PAGES_MAXI = 80

# Ce qui signale une page de bilan ou de compte de resultat au-dela de la
# premiere passe. Les codes REF SYSCOHADA (CP, DZ, XI, BZ) sont le repere le
# plus sur : ils n'apparaissent nulle part ailleurs.
_MARQUEURS_BILAN = re.compile(
    r"total\s+capitaux\s+propres|capitaux\s+propres\s+et\s+ressources"
    r"|total\s+g[ée]n[ée]ral|total\s+(?:de\s+l.)?actif|total\s+passif"
    r"|total\s+bilan|produit\s+net\s+bancaire"
    r"|^\s*(?:CP|DZ|BZ|XI|XB)\s+[A-ZÉ]", re.IGNORECASE | re.MULTILINE)


def extract_from_pdf(pdf_path: str, use_ocr: bool = True) -> dict:
    """Extrait les chiffres cles depuis un PDF d'etats financiers BRVM."""
    data = {
        "revenue": None,
        "net_income": None,
        "equity": None,
        "ebit": None,
        "ebitda": None,
        "total_debt": None,
        "total_assets": None,
        "interest_expense": None,
        "cfo": None,
        "capex": None,
        "dividends_total": None,
        "shares": None,
        "multiplier": 1,
        "commentary": "",
        "revenue_bank": None,
        "ordinary_income": None,
        "hao_income": None,
        "cost_of_risk": None,
        "parcs": None,
        "operating_expenses": None,
        "gross_operating_income": None,
        "pretax_income": None,
        "deposits": None,
        "loans": None,
    }

    is_scanned = False
    _contexte.virgule_millier = False

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_text = ""
            all_tables = []

            lignes_cellules = []
            pages_positions = []
            diapo_data = {}
            def _depouiller(page):
                """Toutes les lectures d'UNE page."""
                nonlocal all_text
                text = page.extract_text() or ""
                all_text += text + "\n"
                # Presentation : les valeurs sont posees sous leurs libelles,
                # centrees, sans tableau. L'echelle est declaree sur la page
                # elle-meme (« en Mds FCFA »), pas dans l'en-tete du document.
                _regler_convention_virgule(all_text)
                _echelle_page = _echelle_de_page(text)
                if _echelle_page:
                    for _champ, _valeur in _extract_depuis_presentation(
                            page, _echelle_page).items():
                        diapo_data.setdefault(_champ, _valeur)
                lignes_cellules.extend(_lignes_par_coordonnees(page))
                pages_positions.append(
                    _lignes_par_coordonnees(page, avec_positions=True))
                for table in page.extract_tables():
                    if table and len(table) > 1:
                        all_tables.append(table)

            for page in pdf.pages[:_PAGES_PREMIERE_PASSE]:
                _depouiller(page)

            # Les etats financiers d'un rapport annuel viennent APRES le
            # rapport de gestion. Le bilan de la SITAB occupe la page 31 d'un
            # document de soixante : s'arreter a la vingtieme laissait ses
            # capitaux propres — 45,6 milliards, ligne « CP TOTAL CAPITAUX
            # PROPRES ET RESSOURCES ASSIMILEES » — introuvables, alors qu'ils
            # etaient bien collectes.
            #
            # On ne depouille pas pour autant tout le document : la lecture par
            # coordonnees et la detection de tableaux coutent cher. Le texte
            # seul est bien moins cher, et il suffit a reconnaitre une page de
            # bilan. Seules ces pages-la sont ensuite depouillees entierement.
            for page in pdf.pages[_PAGES_PREMIERE_PASSE:_PAGES_MAXI]:
                apercu = page.extract_text() or ""
                if _MARQUEURS_BILAN.search(apercu):
                    _depouiller(page)

            text_stripped = all_text.strip()
            if len(text_stripped) < 100 and not all_tables:
                is_scanned = True

            # Prose de l'emetteur : alimente la revue de presse, sans effet
            # sur les chiffres.
            if not data.get("commentary"):
                data["commentary"] = extract_commentary(all_text)

            if not is_scanned:
                _regler_convention_virgule(all_text)
                # Detect multiplier
                data["multiplier"] = _resolve_multiplier(all_text, lignes_cellules)

                mult = data["multiplier"]

                # Etat bi-devise : la colonne en francs n'est pas la premiere,
                # et son echelle est declaree dans l'en-tete plutot que dans
                # la sonde generale du multiplicateur.
                _rang_fcfa, _mult_fcfa = _colonne_fcfa(all_text)
                if _rang_fcfa is None:
                    _rang_fcfa = 0
                else:
                    mult = _mult_fcfa

                # Detect SYSCOHADA format
                is_syscohada = False
                for table in all_tables:
                    for row in table[:5]:
                        if row and row[0]:
                            first = str(row[0]).split("\n")[0].strip().upper()
                            if first in ("REF", "AD", "AE", "CA", "CB", "TA", "XA"):
                                is_syscohada = True
                                break
                    if is_syscohada:
                        break

                # Layer 1: text extraction
                text_data = _extract_from_text(all_text, mult)

                # Couche coordonnees : elle tranche l'ambiguite des colonnes
                # separees par des espaces, que le texte brut ne permet pas.
                _unites_declarees = set()
                cell_data = _extract_from_cells(
                    lignes_cellules, mult, rang=_rang_fcfa,
                    rang_impose=_mult_fcfa is not None,
                    declares=_unites_declarees)

                # Layer 2: row-by-row label matching
                label_data = _extract_from_tables_rowlabel(all_tables, mult)

                # Layer 3: IFRS dual-column tables
                ifrs_data = _extract_ifrs_dual(all_tables, mult)

                # Layer 4: Bank chiffres cles
                bank_data = _extract_bank_chiffres_cles(all_tables, mult)

                # Layer 5: SYSCOHADA REF codes
                ref_data = _extract_syscohada(all_tables, mult) if is_syscohada else {}

                # Merge (higher layers override)
                # Order: text < bank < label < cells < ifrs < ref.
                #
                # La lecture par coordonnees passe DEVANT `label_data` : cette
                # derniere depend de la detection de tableaux de pdfplumber, qui
                # associe parfois un libelle a la mauvaise cellule. Sur le
                # rapport Orange CI 2025 elle rendait un resultat net de 857 Mds
                # — superieur a l'EBITDA, donc impossible — la ou le texte et
                # les coordonnees s'accordaient tous deux sur 167,8 Mds.
                # IFRS et SYSCOHADA restent au-dessus : leurs codes REF ancrent
                # semantiquement la valeur, ce qu'aucune heuristique de mise en
                # page ne peut garantir.
                # La prose de l'emetteur n'etait lue que sur les documents
                # SCANNES. Les rapports de gestion, en texte natif, n'y
                # passaient donc jamais — et c'est pourtant la qu'Onatel donne
                # son resultat net (« 15,89 Mds FCFA »), absent de tout
                # tableau. Elle vient en PREMIER dans la fusion, donc en
                # dernier recours : un tableau reste plus precis qu'une phrase.
                prose_data = _extract_from_prose(all_text)

                all_extracted = {}
                for d in [prose_data, diapo_data, text_data, bank_data,
                          label_data, cell_data, ifrs_data, ref_data]:
                    all_extracted.update({k: v for k, v in d.items() if v is not None})

                # Une unite DECLAREE prime sur une unite devinee. Le bilan de
                # Palm CI porte « En milliers FRANCS CFA » au-dessus de ses
                # colonnes, et la lecture par coordonnees y trouve un total
                # d'actif de 199,1 milliards. La couche IFRS, qui applique le
                # multiplicateur devine pour tout le document, rendait 0,203
                # milliard sur ce meme poste — et passait devant. Le document
                # n'est meme pas en IFRS. Quand l'en-tete d'un tableau dit son
                # unite, rien ne justifie de lui preferer une sonde.
                for _champ in _unites_declarees:
                    if cell_data.get(_champ) is not None:
                        all_extracted[_champ] = cell_data[_champ]

                # Arbitrage des nombres tronques par une frontiere de colonne.
                # Le rapport integre d'Orange CI annonce « 1 197,1 milliards
                # FCFA » ; le decoupage en cellules laissait le « 1 » dans la
                # colonne voisine et le tableau rendait 197,1 — un dixieme du
                # chiffre d'affaires du groupe, sans que rien ne le signale.
                # Quand la valeur du tableau est le SUFFIXE en chiffres de
                # celle annoncee en prose, c'est une troncature et non un
                # desaccord : la prose l'emporte.
                for champ, valeur_prose in prose_data.items():
                    valeur = all_extracted.get(champ)
                    if valeur is None or valeur == valeur_prose:
                        continue
                    if abs(valeur) >= abs(valeur_prose):
                        continue
                    chiffres = str(int(abs(valeur)))
                    complets = str(int(abs(valeur_prose)))
                    if len(complets) > len(chiffres) and complets.endswith(chiffres):
                        all_extracted[champ] = valeur_prose

                # Une valeur lue dans un TABLEAU prime sur une valeur devinee
                # dans la mise en page. Les etats d'Ecobank donnent le produit
                # net bancaire en clair — 1 424 261 millions FCFA — pendant que
                # la lecture de diapositive ramasse un « 14 » centre quelque
                # part sur la page. Sans cet arbitrage, le chiffre d'affaires
                # du groupe tombait a 14 milliards.
                tabulaire = {}
                for d in [bank_data, label_data, cell_data, ifrs_data, ref_data]:
                    tabulaire.update({k: v for k, v in d.items() if v is not None})

                # Map to output
                data["revenue"] = all_extracted.get("revenue")
                if data["revenue"] is None or (
                        "revenue" not in tabulaire
                        and tabulaire.get("revenue_bank") is not None):
                    data["revenue"] = (all_extracted.get("revenue_bank")
                                       or data["revenue"])

                data["net_income"] = all_extracted.get("net_income")
                data["equity"] = all_extracted.get("equity")
                if data["equity"] is None:
                    data["equity"] = _capitaux_propres_par_composantes(
                        lignes_cellules, mult, rang=_rang_fcfa)
                data["ebit"] = all_extracted.get("ebit")
                data["ebitda"] = all_extracted.get("ebitda")
                data["total_assets"] = all_extracted.get("total_assets")
                data["shares"] = all_extracted.get("shares")
                # Lignes du « Tableau d'activité et de résultats » : elles
                # servent a neutraliser l'exceptionnel et, pour les banques,
                # a lire le coût du risque rapporté au PNB.
                # Parcs clients : pour un operateur telecom, le chiffre
                # d'affaires n'est que la consequence du parc.
                parcs = _extract_parcs(pages_positions)
                # Un effectif sous le plancher n'est pas un parc : Onatel
                # rendait des « parcs » de 12 clients, lus dans une colonne
                # d'annee.
                parcs = {c: d for c, d in parcs.items()
                         if (d.get("valeur") if isinstance(d, dict) else d)
                         and (d.get("valeur") if isinstance(d, dict) else d)
                         >= _PARC_PLANCHER}
                # Les rapports de gestion racontent les parcs au lieu de les
                # tabuler : la prose prend le relais, sans jamais remplacer
                # une valeur deja lue dans un tableau, plus precise.
                for champ, detail in _extract_parcs_prose(all_text).items():
                    parcs.setdefault(champ, detail)
                if parcs:
                    data["parcs"] = parcs

                data["ordinary_income"] = all_extracted.get("ordinary_income")
                data["hao_income"] = all_extracted.get("hao_income")
                data["cost_of_risk"] = all_extracted.get("cost_of_risk")
                for champ in ("operating_expenses", "gross_operating_income",
                              "pretax_income", "deposits", "loans"):
                    data[champ] = all_extracted.get(champ)
                if data.get("revenue_bank") is None:
                    data["revenue_bank"] = all_extracted.get("revenue_bank")

                if all_extracted.get("total_debt") is not None:
                    data["total_debt"] = abs(all_extracted["total_debt"])
                if all_extracted.get("interest_expense") is not None:
                    data["interest_expense"] = abs(all_extracted["interest_expense"])

                data["cfo"] = all_extracted.get("cfo")

                if all_extracted.get("capex") is not None:
                    data["capex"] = abs(all_extracted["capex"])
                if all_extracted.get("dividends_paid") is not None:
                    data["dividends_total"] = abs(all_extracted["dividends_paid"])

    except Exception as e:
        data["error"] = str(e)
        return data

    # OCR fallback : declenche aussi quand les 5 layers n'ont rien trouve,
    # meme si le PDF n'est pas detecte comme scanne (cas slides type Sonatel
    # ou les chiffres sont rendus en grandes typos sans pattern label : value).
    _has_data = any(
        data.get(f) is not None
        for f in ("revenue", "net_income", "ebit", "ebitda", "equity")
    )
    if (is_scanned or not _has_data) and use_ocr:
        try:
            # Un seul parcours du document pour les deux lectures.
            ocr_text, ocr_cellules = _ocr_document(pdf_path)
            if _lignes_exploitables(ocr_text) == 0:
                # Segmentation sterile : on retente le texte a plat dans
                # l'autre mode, sans refaire la lecture par coordonnees, qui
                # ne depend pas de la segmentation.
                autre, _ = _ocr_document(pdf_path, psm=4, avec_cellules=False)
                if _lignes_exploitables(autre) > _lignes_exploitables(ocr_text):
                    print("OCR: segmentation psm 6 sterile, bascule en psm 4",
                          flush=True)
                    ocr_text = autre
            if ocr_text and len(ocr_text) > 100:
                if not data.get("commentary"):
                    data["commentary"] = extract_commentary(ocr_text)
                _regler_convention_virgule(ocr_text)
                ocr_mult = _resolve_multiplier(ocr_text, ocr_cellules)
                data["multiplier"] = ocr_mult
                ocr_data = _extract_from_text(ocr_text, ocr_mult)
                # Les cellules priment : elles savent ou s'arrete une colonne.
                ocr_data.update({k: v for k, v in
                                 _extract_from_cells(ocr_cellules, ocr_mult).items()
                                 if v is not None})
                # Merge tous les champs extractibles depuis l'OCR (couvre ebit,
                # ebitda, capex, cfo en plus des 5 fields originaux).
                for field in ("revenue", "revenue_bank", "net_income", "equity",
                               "total_assets", "shares", "ebit", "ebitda",
                               "capex", "cfo", "ordinary_income", "hao_income",
                               "cost_of_risk", "operating_expenses",
                               "gross_operating_income", "pretax_income",
                               "deposits", "loans"):
                    if data.get(field) is None and ocr_data.get(field):
                        data[field] = ocr_data[field]
                if data.get("revenue") is None and ocr_data.get("revenue_bank"):
                    data["revenue"] = ocr_data["revenue_bank"]

                # La reconstitution des capitaux propres par leurs composantes
                # vaut aussi pour un document SCANNE — et c'est meme la qu'elle
                # sert le plus. Le bilan de BOA Burkina laisse sa ligne
                # « CAPITAUX PROPRES ET RESSOURCES ASSIMILEES » VIDE et detaille
                # capital souscrit, reserves, report a nouveau et resultat juste
                # en dessous ; celui de Coris Bank porte son total mais dans un
                # tableau que la reconstitution en cellules rend mal. Sept
                # banques de la cote publient ainsi, en image.
                if data.get("equity") is None and ocr_cellules:
                    data["equity"] = _capitaux_propres_par_composantes(
                        ocr_cellules, ocr_mult)

                # Dernier recours : les montants cites dans le commentaire.
                prose = _extract_from_prose(ocr_text)
                for champ, montant in prose.items():
                    if data.get(champ) is None:
                        data[champ] = montant
                        # L'emetteur arrondit dans sa prose (« 70,0 milliards »
                        # pour 70 045 411 000) : on trace la provenance pour
                        # que l'audit ne prenne pas cet arrondi pour une erreur.
                        data.setdefault("champs_prose", []).append(champ)
                        print(f"[prose] {champ} = {montant:,.0f} "
                              f"(lu dans le commentaire)", flush=True)
                if data.get("revenue") is None and prose.get("revenue_bank"):
                    data["revenue"] = prose["revenue_bank"]
                data["ocr_used"] = True
        except Exception as e:
            data["ocr_error"] = str(e)

    # Plancher de sortie : sous ces seuils, la valeur ne vient pas d'un etat
    # financier de societe cotee mais d'une unite non identifiee (tableau en
    # millions lu comme des francs) ou d'un fragment de nombre. Mieux vaut un
    # trou, comble par la saisie manuelle, qu'un chiffre faux d'un facteur
    # 1 000 000 dans les ratios.
    # Plafond : la BRVM entiere capitalise ~18 000 milliards FCFA et le plus
    # gros bilan bancaire de la cote tourne autour de 28 000 milliards. Au-dela
    # de 50 000 milliards on lit un numero de registre, pas un montant.
    _PLAFOND = 5e13
    _PLANCHERS = {"revenue": 1e8, "revenue_bank": 1e8, "total_assets": 1e8,
                  "equity": 1e8, "deposits": 1e8, "loans": 1e8,
                  "net_income": 1e6, "ordinary_income": 1e6, "hao_income": 1e6,
                  "cost_of_risk": 1e6, "operating_expenses": 1e6,
                  "gross_operating_income": 1e6, "pretax_income": 1e6,
                  "total_debt": 1e6, "ebit": 1e6, "ebitda": 1e6,
                  "interest_expense": 1e5, "cfo": 1e6, "capex": 1e6,
                  "dividends_total": 1e6}

    for k, plancher in _PLANCHERS.items():
        v = data.get(k)
        if v is None or v == 0:
            continue
        if abs(v) < plancher:
            data[k] = None
        elif abs(v) > _PLAFOND:
            # Une valeur qui ne devient absurde qu'APRES multiplication etait
            # deja exprimee en francs. Un rapport annuel de soixante pages
            # raconte ses resultats en milliards dans sa prose et depose ses
            # etats en francs pleins : le multiplicateur du document, lu dans
            # la prose, ne vaut rien pour le bilan. Les capitaux propres de la
            # SITAB — 45 642 447 915 francs — devenaient 4,6 x 10^19 et
            # disparaissaient. On rend la valeur brute quand elle, et elle
            # seule, redevient plausible.
            mult = data.get("multiplier") or 1
            brute = v / mult
            data[k] = (brute if mult > 1 and plancher <= abs(brute) <= _PLAFOND
                       else None)

    # Un signe qui ne peut pas exister. Une societe ne vend pas pour moins que
    # rien, un bilan ne totalise pas un montant negatif, une banque ne prete
    # pas -188 milliards. Quand un de ces postes ressort negatif, c'est qu'une
    # colonne de VARIATION a ete lue a la place de la valeur — BOA Burkina
    # sortait un chiffre d'affaires de -1,53 milliard au premier trimestre
    # 2026, Oragroup des credits a -188,75 milliards. La valeur est ecartee :
    # un poste absent se voit, un poste negatif se propage en ratios.
    #
    # Les capitaux propres et le resultat net n'y figurent PAS : ils peuvent
    # legitimement etre negatifs, et Oragroup a bel et bien perdu 44 milliards
    # en 2024.
    for k in ("revenue", "revenue_bank", "total_assets", "deposits",
              "loans", "shares"):
        if (data.get(k) or 0) < 0:
            data[k] = None

    return data


# ---------------------------------------------------------------------------
# Download + extract
# ---------------------------------------------------------------------------

def download_and_extract(url: str, use_ocr: bool = True) -> dict:
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name
        result = extract_from_pdf(tmp_path, use_ocr=use_ocr)
        os.unlink(tmp_path)
        return result
    except Exception as e:
        return {"error": str(e)}


def extract_from_local_pdf(pdf_path: str, use_ocr: bool = True) -> dict:
    if not os.path.exists(pdf_path):
        return {"error": f"File not found: {pdf_path}"}
    return extract_from_pdf(pdf_path, use_ocr=use_ocr)


# ---------------------------------------------------------------------------
# Batch: local folders
# ---------------------------------------------------------------------------

FOLDER_TICKER_MAP = {
    "1- Air Liquide": "SIVC.ci",
    "2- BOA BF": "BOABF.bf",
    "3- BOA BN": "BOAB.bj",
    "4- BOA CI": "BOAC.ci",
    "5- BOA ML": "BOAM.ml",
    "6- BOA NG": "BOAN.ne",
    "7- BOA SN": "BOAS.sn",
    "8- Bridge Bank CI": "BICB.bj",
    "9- Bernabe CI": "BNBC.ci",
    "10- BICI": "BICC.ci",
    "11- BIIC": "TTLC.ci",
    "12- Bollore Transport": "SDSC.ci",
    "13- CFAO CI": "CFAC.ci",
    "14- CIE CI": "CIEC.ci",
    "15- CORIS BANK INTERNATIONAL": "CBIBF.bf",
    "16- CI Telecom": "ORAC.ci",
    "17- CROWN SIEM CI": "SEMC.ci",
    "18- DC BR": None,
    "19- ECOBANK CI": "ECOC.ci",
    "20- ECOBANK TG": "ETIT.tg",
    "31- NEI-CEDA CI": "NEIC.ci",
    "32- NESTLE CI": "NTLC.ci",
    "34- NSBC": "NSBC.ci",
    "35- ONATEL": "ONTBF.bf",
    "36- ORAGROUP": "ORGT.tg",
    "37- ORANGE CI": "ORAC.ci",
    "38- PALM CI": "PALC.ci",
    "39- SAFCA CI": "SAFC.ci",
    "41- SAPH CI": "SPHC.ci",
    "43- SERVAIR ABIDJAN CI": "ABJC.ci",
    "45- SGB CI": "SGBC.ci",
    "46- SIB": "SIBC.ci",
    "47- SICABLE": "CABC.ci",
    "48- SICOR": "SICC.ci",
    "50- SITAB": "STBC.ci",
    "51- SMB": "SMBC.ci",
    "53- SODECI": "SDCC.ci",
    "54- SOGB": "SOGC.ci",
    "55- SOLIBRA": "SLBC.ci",
    "56- SONATEL": "SNTS.sn",
    "57- SUCRIVOIRE": "SCRC.ci",
    "63- TOTAL": "TTRC.ci",
    "64- TOTAL SENEGAL S.A.": "TTLS.sn",
    "65- TPBF": "UNLC.ci",
    "67- TPCI": "PRSC.ci",
    "68- TRACTAFRIC CI": "CFAC.ci",
    "69- TRITRAF CI": "TTRC.ci",
    "25- FILTISAC": "FTSC.ci",
    "29- LNB": "LNBB.bj",
    "30- MOVIS CI": "SVOC.ci",
    "44- SETAO CI": "STAC.ci",
    "70- UNILEVER CI": "UNLC.ci",
    "71 -UNIWAX CI": "UNXC.ci",
    "72- VIVO ENERGY CI": "SHEC.ci",
}


def _detect_fiscal_year(filename: str) -> Optional[int]:
    fn_lower = filename.lower()
    # Skip quarterly/semi-annual (unless annual)
    if any(kw in fn_lower for kw in [
        "trimestre", "semestre", "_1t_", "_2t_", "_3t_", "_s1_", "_s2_",
        "1er_trimestre", "3eme_trimestre", "1er_semestre",
    ]):
        if "annuel" not in fn_lower:
            return None

    # Skip multi-year files
    if re.search(r"exercices?_\d{4}_a_\d{4}", fn_lower):
        return None

    match = re.search(r"exercice[_\s-]*(\d{4})", fn_lower)
    if match:
        year = int(match.group(1))
        if 2018 <= year <= 2026:
            return year

    match = re.search(r"20(\d{2})", filename)
    if match:
        year = 2000 + int(match.group(1))
        if 2020 <= year <= 2026:
            return year
    return None


def _is_financial_statement(filename: str) -> bool:
    fn_lower = filename.lower()
    is_ef = any(kw in fn_lower for kw in [
        "etats_financiers", "etats_finaniers", "etats_financier",
        "resultats_financiers", "resultats_financier",
        "rapport_dactivite_annuel", "rapport_dactivites_annuel",
        "rapport_annuel",
        "rapport_du_ca_a_l", "rapport_du_ca_a_lago",
    ])
    is_excluded = any(kw in fn_lower for kw in [
        "attestation_des_c", "rapport_des_cac",
    ])
    return is_ef and not is_excluded


def extract_all_local_pdfs(base_dir: str = None, progress_callback=None, use_ocr: bool = False):
    if base_dir is None:
        base_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pdfs")

    tickers_data = load_tickers()
    ticker_meta = {t["ticker"]: t for t in tickers_data}

    processed = 0
    extracted = 0
    errors = []

    folders = sorted([d for d in os.listdir(base_dir)
                       if os.path.isdir(os.path.join(base_dir, d)) and not d.startswith(".")])

    for folder in folders:
        ticker = FOLDER_TICKER_MAP.get(folder)
        if not ticker:
            continue

        folder_path = os.path.join(base_dir, folder)
        pdf_files = sorted([f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")])

        for pdf_file in pdf_files:
            if not _is_financial_statement(pdf_file):
                continue

            fiscal_year = _detect_fiscal_year(pdf_file)
            if fiscal_year is None:
                continue

            pdf_path = os.path.join(folder_path, pdf_file)
            processed += 1

            if progress_callback:
                progress_callback(processed, 0, f"{ticker} {fiscal_year}")

            result = extract_from_pdf(pdf_path, use_ocr=use_ocr)

            if "error" in result:
                errors.append(f"{ticker} {fiscal_year}: {result['error']}")
                continue

            has_data = any(result.get(f) is not None for f in
                         ["revenue", "net_income", "equity", "ebit", "total_assets",
                          "total_debt", "interest_expense", "cfo", "capex"])

            if not has_data:
                errors.append(f"{ticker} {fiscal_year}: VIDE")
                continue

            meta = ticker_meta.get(ticker, {})

            conn = get_connection()
            market = conn.execute(
                "SELECT price, shares, dps FROM market_data WHERE ticker=?", (ticker,)
            ).fetchone()
            existing = conn.execute(
                "SELECT * FROM fundamentals WHERE ticker=? AND fiscal_year=?",
                (ticker, fiscal_year)
            ).fetchone()
            conn.close()

            existing_dict = dict(existing) if existing else {}

            def _best(field):
                new_val = result.get(field)
                old_val = existing_dict.get(field)
                return new_val if new_val is not None else old_val

            fund_data = {
                "ticker": ticker,
                "company_name": meta.get("name", existing_dict.get("company_name", "")),
                "sector": meta.get("sector", existing_dict.get("sector", "")),
                "currency": "XOF",
                "fiscal_year": fiscal_year,
                "price": market["price"] if market else existing_dict.get("price", 0),
                "shares": (result.get("shares") or
                          (market["shares"] if market and market["shares"] else None) or
                          existing_dict.get("shares")),
                "revenue": _best("revenue"),
                "net_income": _best("net_income"),
                "equity": _best("equity"),
                "total_debt": _best("total_debt"),
                "ebit": _best("ebit"),
                "interest_expense": _best("interest_expense"),
                "cfo": _best("cfo"),
                "capex": _best("capex"),
                "dividends_total": _best("dividends_total"),
                "total_assets": _best("total_assets"),
                "dps": market["dps"] if market else existing_dict.get("dps"),
            }
            # Complete avec les champs d'extraction que ce dictionnaire ne
            # nommait pas (ebitda, cout du risque, depots, credits...).
            for _champ, _valeur in champs_extraits(result, _best).items():
                if fund_data.get(_champ) is None:
                    fund_data[_champ] = _valeur

            save_fundamentals(fund_data)
            extracted += 1

    return processed, extracted, errors


# ---------------------------------------------------------------------------
# Batch: database report links
# ---------------------------------------------------------------------------

def extract_all_reports(progress_callback=None, use_ocr: bool = False):
    reports = get_report_links()
    if reports.empty:
        return 0

    pdf_reports = reports[
        (reports["report_type"].isin(["etats_financiers"])) &
        (reports["url"].str.endswith(".pdf"))
    ]

    tickers_data = load_tickers()
    ticker_meta = {t["ticker"]: t for t in tickers_data}

    updated = 0
    for i, (_, report) in enumerate(pdf_reports.iterrows()):
        ticker = report["ticker"]
        year = int(report["fiscal_year"]) if report.get("fiscal_year") else None
        url = report["url"]

        if progress_callback:
            progress_callback(i, len(pdf_reports), ticker)

        conn = get_connection()
        existing = conn.execute(
            "SELECT revenue, net_income, equity, ebit, cfo FROM fundamentals WHERE ticker=? AND fiscal_year=?",
            (ticker, year),
        ).fetchone()
        conn.close()

        if existing and existing["revenue"] and existing["net_income"] and existing["equity"] and existing["ebit"]:
            continue

        result = download_and_extract(url, use_ocr=use_ocr)
        if "error" in result:
            continue

        if any(result.get(f) for f in ["revenue", "net_income", "equity", "ebit", "total_assets"]):
            meta = ticker_meta.get(ticker, {})
            conn = get_connection()
            market = conn.execute(
                "SELECT price, shares, dps FROM market_data WHERE ticker=?", (ticker,)
            ).fetchone()
            conn.close()

            fund_data = {
                "ticker": ticker,
                "company_name": meta.get("name", ""),
                "sector": meta.get("sector", ""),
                "currency": "XOF",
                "fiscal_year": year,
                "price": market["price"] if market else 0,
                "shares": market["shares"] if market and market["shares"] else None,
                "revenue": result.get("revenue"),
                "net_income": result.get("net_income"),
                "equity": result.get("equity"),
                "total_debt": result.get("total_debt"),
                "ebit": result.get("ebit"),
                "interest_expense": result.get("interest_expense"),
                "cfo": result.get("cfo"),
                "capex": result.get("capex"),
                "dividends_total": result.get("dividends_total"),
                "total_assets": result.get("total_assets"),
                "dps": market["dps"] if market else None,
            }
            for _champ, _valeur in champs_extraits(result).items():
                if fund_data.get(_champ) is None:
                    fund_data[_champ] = _valeur
            save_fundamentals(fund_data)
            updated += 1

    return updated
