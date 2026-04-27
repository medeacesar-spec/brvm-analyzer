"""
Extracteur de donnees financieres depuis les PDFs des etats financiers BRVM.
Telecharge les PDFs et extrait CA, Resultat Net, Capitaux Propres,
EBIT, Total Actif, Dettes, CFO, CAPEX, Charges financieres, Dividendes verses, etc.

Gere les formats:
- SYSCOHADA (REF codes: XB, XI, CP, DD, etc.)
- IFRS (labels textuels)
- Banques PCEB/OHADA (PNB, marge d'intermediation, chiffres cles)
- Rapports du CA bancaires (tableaux chiffres cles)

Inclut un fallback OCR via easyocr pour les PDFs scannes (images).
"""

import io
import math
import os
import re
import tempfile
from typing import Optional, List

import requests
import pdfplumber

from config import load_tickers
from data.storage import get_report_links, save_fundamentals, get_connection


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_amount(text: str) -> Optional[float]:
    """Parse un montant depuis le texte PDF."""
    if not text:
        return None
    cleaned = text.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    negative = ("(" in cleaned and ")" in cleaned) or cleaned.startswith("-")
    cleaned = cleaned.replace("(", "").replace(")", "").lstrip("-")
    cleaned = cleaned.rstrip(".").rstrip("%")
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

def _extract_from_text(all_text: str, mult: float) -> dict:
    """Extraction depuis le texte brut.

    Approche conservatrice : on exige un séparateur fort (`:` ou unité
    explicite type "Mds FCFA") après le label pour éviter les faux positifs
    sur les rapports en slides où plusieurs chiffres sont juxtaposés sans
    structure (Sonatel Q1 — voir notes/parser_slides.md).
    """
    data = {}
    text_lower = all_text.lower()

    patterns = [
        (r"r.sultat\s*net[^:]*?:\s*([\d\s,\.]+)", "net_income"),
        (r"chiffre\s*d.affaires[^:]*?:\s*([\d\s,\.]+)", "revenue"),
        (r"capitaux\s*propres[^:]*?:\s*([\d\s,\.]+)", "equity"),
        (r"produit\s*net\s*bancaire[^:]*?:\s*([\d\s,\.]+)", "revenue_bank"),
        (r"total\s*(?:de\s*l.)?actif[^:]*?:\s*([\d\s,\.]+)", "total_assets"),
        (r"total\s*bilan[^:]*?:\s*([\d\s,\.]+)", "total_assets"),
    ]

    for pattern, field in patterns:
        match = re.search(pattern, text_lower)
        if match and field not in data:
            val = _parse_amount(match.group(1))
            if val:
                data[field] = val * mult

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
        if m:
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


# ---------------------------------------------------------------------------
# OCR for scanned PDFs
# ---------------------------------------------------------------------------

_ocr_reader = None


def _get_ocr_reader():
    global _ocr_reader
    if _ocr_reader is None:
        try:
            import easyocr
            _ocr_reader = easyocr.Reader(['fr', 'en'], gpu=False, verbose=False)
        except ImportError:
            return None
    return _ocr_reader


def _extract_with_ocr(pdf_path: str) -> str:
    """Convert scanned PDF pages to images via PyMuPDF, then OCR with easyocr."""
    try:
        import fitz  # PyMuPDF
        reader = _get_ocr_reader()
        if reader is None:
            return ""
        doc = fitz.open(pdf_path)
        all_text = ""
        max_pages = min(len(doc), 15)
        for i in range(max_pages):
            page = doc[i]
            pix = page.get_pixmap(dpi=200)
            import numpy as np
            from PIL import Image
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img_array = np.array(img)
            results = reader.readtext(img_array, detail=0, paragraph=True)
            page_text = "\n".join(results)
            all_text += page_text + "\n\n"
        doc.close()
        return all_text
    except ImportError:
        # Fallback to pdf2image if PyMuPDF not available
        try:
            from pdf2image import convert_from_path
            reader = _get_ocr_reader()
            if reader is None:
                return ""
            images = convert_from_path(pdf_path, dpi=200, first_page=1, last_page=15)
            all_text = ""
            for img in images:
                import numpy as np
                img_array = np.array(img)
                results = reader.readtext(img_array, detail=0, paragraph=True)
                page_text = "\n".join(results)
                all_text += page_text + "\n\n"
            return all_text
        except Exception as e:
            print(f"OCR error: {e}")
            return ""
    except Exception as e:
        print(f"OCR error: {e}")
        return ""


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

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
    }

    is_scanned = False

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_text = ""
            all_tables = []

            for page in pdf.pages[:20]:
                text = page.extract_text() or ""
                all_text += text + "\n"
                tables = page.extract_tables()
                for table in tables:
                    if table and len(table) > 1:
                        all_tables.append(table)

            text_stripped = all_text.strip()
            if len(text_stripped) < 100 and not all_tables:
                is_scanned = True

            if not is_scanned:
                # Detect multiplier
                text_lower = all_text.lower()
                if "en milliards" in text_lower or "(milliards" in text_lower:
                    data["multiplier"] = 1_000_000_000
                elif "en millions" in text_lower or "(millions" in text_lower or "millions de francs" in text_lower:
                    data["multiplier"] = 1_000_000
                elif "en milliers" in text_lower or "(milliers" in text_lower:
                    data["multiplier"] = 1_000

                mult = data["multiplier"]

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

                # Layer 2: row-by-row label matching
                label_data = _extract_from_tables_rowlabel(all_tables, mult)

                # Layer 3: IFRS dual-column tables
                ifrs_data = _extract_ifrs_dual(all_tables, mult)

                # Layer 4: Bank chiffres cles
                bank_data = _extract_bank_chiffres_cles(all_tables, mult)

                # Layer 5: SYSCOHADA REF codes
                ref_data = _extract_syscohada(all_tables, mult) if is_syscohada else {}

                # Merge (higher layers override)
                # Order: text < bank < label < ifrs < ref (IFRS/SYSCOHADA most reliable)
                all_extracted = {}
                for d in [text_data, bank_data, label_data, ifrs_data, ref_data]:
                    all_extracted.update({k: v for k, v in d.items() if v is not None})

                # Map to output
                data["revenue"] = all_extracted.get("revenue")
                if data["revenue"] is None:
                    data["revenue"] = all_extracted.get("revenue_bank")

                data["net_income"] = all_extracted.get("net_income")
                data["equity"] = all_extracted.get("equity")
                data["ebit"] = all_extracted.get("ebit")
                data["ebitda"] = all_extracted.get("ebitda")
                data["total_assets"] = all_extracted.get("total_assets")
                data["shares"] = all_extracted.get("shares")

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
            ocr_text = _extract_with_ocr(pdf_path)
            if ocr_text and len(ocr_text) > 100:
                ocr_lower = ocr_text.lower()
                ocr_mult = 1
                if "milliards" in ocr_lower:
                    ocr_mult = 1_000_000_000
                elif "millions" in ocr_lower:
                    ocr_mult = 1_000_000
                elif "milliers" in ocr_lower:
                    ocr_mult = 1_000
                data["multiplier"] = ocr_mult
                ocr_data = _extract_from_text(ocr_text, ocr_mult)
                # Merge tous les champs extractibles depuis l'OCR (couvre ebit,
                # ebitda, capex, cfo en plus des 5 fields originaux).
                for field in ("revenue", "net_income", "equity", "total_assets",
                               "shares", "ebit", "ebitda", "capex", "cfo"):
                    if data.get(field) is None and ocr_data.get(field):
                        data[field] = ocr_data[field]
                if data.get("revenue") is None and ocr_data.get("revenue_bank"):
                    data["revenue"] = ocr_data["revenue_bank"]
                data["ocr_used"] = True
        except Exception as e:
            data["ocr_error"] = str(e)

    # Sanity checks
    for k in ["revenue", "net_income", "equity", "total_assets", "total_debt",
              "ebit", "ebitda", "interest_expense", "cfo", "capex", "dividends_total"]:
        v = data.get(k)
        if v is not None and abs(v) > 1e16:
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
            save_fundamentals(fund_data)
            updated += 1

    return updated
