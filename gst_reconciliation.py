"""
Seven Labs Vision — GST Audit Reconciliation Engine
Compares Tally books data with GST portal data (GSTR-1, GSTR-2B, GSTR-3B)
and generates detailed audit reconciliation reports.
"""

import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")

# Import shared GST ledger detection from gst_engine (single source of truth)
from gst_engine import _detect_gst_ledgers as _detect_gst_ledgers_shared

# Optional imports — degrade gracefully
try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    import openpyxl
    HAS_EXCEL = True
except ImportError:
    HAS_EXCEL = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# ═══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_cols(conn, table):
    """Return set of column names for a table. Returns empty set on failure."""
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _safe_float(val):
    """Convert to float, returning 0.0 on failure."""
    if val is None or val == "":
        return 0.0
    try:
        return round(float(val), 2)
    except (ValueError, TypeError):
        return 0.0


def _normalize_invoice_no(inv_no):
    """Normalize invoice number for fuzzy matching.
    Strips leading zeros, spaces, hyphens, slashes and lowercases.
    """
    if not inv_no:
        return ""
    s = str(inv_no).strip().upper()
    # Remove common prefixes/separators
    s = re.sub(r"[\s\-/\\]+", "", s)
    # Strip leading zeros
    s = s.lstrip("0") or "0"
    return s


def _normalize_gstin(gstin):
    """Normalize GSTIN — uppercase, strip spaces."""
    if not gstin:
        return ""
    return str(gstin).strip().upper().replace(" ", "")


def _parse_portal_date(date_str):
    """Parse portal date formats (DD-MM-YYYY, DD/MM/YYYY) to YYYYMMDD."""
    if not date_str:
        return ""
    date_str = str(date_str).strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y%m%d")
        except ValueError:
            continue
    # Already YYYYMMDD?
    if re.match(r"^\d{8}$", date_str):
        return date_str
    return date_str


def _format_display_date(yyyymmdd):
    """YYYYMMDD -> DD-MM-YYYY."""
    if not yyyymmdd or len(str(yyyymmdd)) < 8:
        return str(yyyymmdd or "")
    s = str(yyyymmdd)
    return f"{s[6:8]}-{s[4:6]}-{s[0:4]}"


def _period_label(from_date, to_date):
    """Generate period label from YYYYMMDD dates."""
    month_names = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }
    if from_date and to_date:
        f_mm = str(from_date)[4:6]
        f_yy = str(from_date)[0:4]
        t_mm = str(to_date)[4:6]
        t_yy = str(to_date)[0:4]
        return f"{month_names.get(f_mm, f_mm)} {f_yy} - {month_names.get(t_mm, t_mm)} {t_yy}"
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
#  GST LEDGER DETECTION — uses shared implementation from gst_engine.py
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_gst_ledgers(conn):
    """Delegate to gst_engine._detect_gst_ledgers (single source of truth).
    Handles unified CGST/SGST/IGST ledgers and voucher-context-based detection.
    """
    return _detect_gst_ledgers_shared(conn)


# ═══════════════════════════════════════════════════════════════════════════════
#  PORTAL DATA PARSERS
# ═══════════════════════════════════════════════════════════════════════════════

def _read_file_data(file_path_or_data, file_type="json"):
    """Read file content — handles path string, bytes, or already-parsed data.
    Raises ValueError with clear message on malformed files.
    """
    if file_path_or_data is None:
        return {} if file_type == "json" else b""
    if isinstance(file_path_or_data, dict) or isinstance(file_path_or_data, list):
        return file_path_or_data

    if isinstance(file_path_or_data, bytes):
        raw = file_path_or_data
    elif hasattr(file_path_or_data, "read"):
        raw = file_path_or_data.read()
        if hasattr(file_path_or_data, "seek"):
            file_path_or_data.seek(0)
    elif isinstance(file_path_or_data, str) and os.path.isfile(file_path_or_data):
        with open(file_path_or_data, "rb") as f:
            raw = f.read()
    else:
        raw = file_path_or_data if isinstance(file_path_or_data, str) else str(file_path_or_data)
        if file_type == "json":
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, ValueError) as e:
                raise ValueError(f"Malformed JSON file: {e}")
        return raw

    if file_type == "json":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"Malformed JSON file: {e}")
    return raw


def _extract_tables_from_pdf(file_path_or_data):
    """Extract tables from PDF using pdfplumber. Returns list of dicts.
    Handles malformed PDFs with clear error messages.
    """
    if not HAS_PDF:
        raise ImportError("pdfplumber is required for PDF parsing. Install: pip install pdfplumber")

    if file_path_or_data is None:
        return []

    if isinstance(file_path_or_data, str) and os.path.isfile(file_path_or_data):
        pdf = pdfplumber.open(file_path_or_data)
    elif hasattr(file_path_or_data, "read"):
        import io
        content = file_path_or_data.read()
        if hasattr(file_path_or_data, "seek"):
            file_path_or_data.seek(0)
        pdf = pdfplumber.open(io.BytesIO(content))
    elif isinstance(file_path_or_data, bytes):
        import io
        pdf = pdfplumber.open(io.BytesIO(file_path_or_data))
    else:
        raise ValueError("Cannot read PDF from provided data")

    all_rows = []
    headers = None
    try:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue
            for table in tables:
                if not table:
                    continue
                for row_idx, row in enumerate(table):
                    if not row:
                        continue
                    if row_idx == 0 and headers is None:
                        headers = [str(c or "").strip() for c in row]
                        continue
                    if headers and len(row) == len(headers):
                        rec = {}
                        for i, h in enumerate(headers):
                            rec[h] = str(row[i] or "").strip()
                        all_rows.append(rec)
    except Exception as e:
        raise ValueError(f"Failed to extract tables from PDF: {e}")
    finally:
        try:
            pdf.close()
        except Exception:
            pass
    return all_rows


def _parse_excel_to_dicts(file_path_or_data):
    """Parse Excel/CSV to list of dicts. Handles malformed files with clear errors."""
    if not HAS_PANDAS:
        raise ImportError("pandas is required for Excel parsing. Install: pip install pandas openpyxl")
    if file_path_or_data is None:
        return []
    try:
        if isinstance(file_path_or_data, str) and file_path_or_data.endswith(".csv"):
            df = pd.read_csv(file_path_or_data)
        elif isinstance(file_path_or_data, str):
            df = pd.read_excel(file_path_or_data)
        elif hasattr(file_path_or_data, "name") and str(getattr(file_path_or_data, "name", "")).endswith(".csv"):
            df = pd.read_csv(file_path_or_data)
        else:
            df = pd.read_excel(file_path_or_data)
        return df.fillna("").to_dict("records")
    except Exception as e:
        raise ValueError(f"Failed to parse Excel/CSV file: {e}")


def _map_excel_gstr2b_row(row):
    """Map an Excel/PDF GSTR-2B row to standardized format."""
    # Common column name variations
    gstin = row.get("GSTIN of Supplier", row.get("ctin", row.get("GSTIN", row.get("Supplier GSTIN", ""))))
    inv_no = row.get("Invoice Number", row.get("inum", row.get("Invoice No", row.get("Inv No", ""))))
    inv_date = row.get("Invoice Date", row.get("idt", row.get("Inv Date", "")))
    taxable = _safe_float(row.get("Taxable Value", row.get("txval", row.get("Taxable Amount", 0))))
    igst = _safe_float(row.get("Integrated Tax", row.get("igst", row.get("IGST", 0))))
    cgst = _safe_float(row.get("Central Tax", row.get("cgst", row.get("CGST", 0))))
    sgst = _safe_float(row.get("State/UT Tax", row.get("sgst", row.get("SGST", 0))))
    cess = _safe_float(row.get("Cess", row.get("cess", 0)))
    rate = _safe_float(row.get("Rate", row.get("rt", 0)))
    pos = row.get("Place Of Supply", row.get("pos", ""))
    rev = row.get("Reverse Charge", row.get("rev", "N"))
    itc_avl = row.get("ITC Available", row.get("itcavl", "Y"))

    return {
        "gstin": _normalize_gstin(gstin),
        "invoice_no": str(inv_no).strip(),
        "invoice_date": _parse_portal_date(str(inv_date)),
        "invoice_value": round(taxable + igst + cgst + sgst + cess, 2),
        "taxable_value": taxable,
        "igst": igst,
        "cgst": cgst,
        "sgst": sgst,
        "cess": cess,
        "rate": rate,
        "pos": str(pos),
        "reverse_charge": str(rev).upper().startswith("Y"),
        "itc_available": str(itc_avl).upper().startswith("Y"),
    }


def _map_excel_gstr1_row(row):
    """Map an Excel/PDF GSTR-1 row to standardized format."""
    gstin = row.get("GSTIN/UIN of Recipient", row.get("ctin", row.get("GSTIN", "")))
    inv_no = row.get("Invoice Number", row.get("inum", row.get("Invoice No", "")))
    inv_date = row.get("Invoice Date", row.get("idt", row.get("Inv Date", "")))
    taxable = _safe_float(row.get("Taxable Value", row.get("txval", 0)))
    igst = _safe_float(row.get("Integrated Tax", row.get("iamt", row.get("IGST", 0))))
    cgst = _safe_float(row.get("Central Tax", row.get("camt", row.get("CGST", 0))))
    sgst = _safe_float(row.get("State/UT Tax", row.get("samt", row.get("SGST", 0))))
    cess = _safe_float(row.get("Cess", row.get("csamt", 0)))
    rate = _safe_float(row.get("Rate", row.get("rt", 0)))
    pos = row.get("Place Of Supply", row.get("pos", ""))

    return {
        "gstin": _normalize_gstin(gstin),
        "invoice_no": str(inv_no).strip(),
        "invoice_date": _parse_portal_date(str(inv_date)),
        "invoice_value": round(taxable + igst + cgst + sgst + cess, 2),
        "taxable_value": taxable,
        "igst": igst,
        "cgst": cgst,
        "sgst": sgst,
        "cess": cess,
        "rate": rate,
        "pos": str(pos),
    }


# ── GSTR-2B PARSER ──────────────────────────────────────────────────────────

def parse_gstr2b(file_path_or_data, file_type="json"):
    """Parse GSTR-2B from JSON, Excel, or PDF.
    Returns standardized list of invoice dicts:
    [{gstin, invoice_no, invoice_date, taxable_value, igst, cgst, sgst, cess, rate, ...}]
    """
    invoices = []

    if file_type == "pdf":
        rows = _extract_tables_from_pdf(file_path_or_data)
        for row in rows:
            inv = _map_excel_gstr2b_row(row)
            if inv["gstin"] and inv["invoice_no"]:
                invoices.append(inv)
        return invoices

    if file_type in ("excel", "xlsx", "xls", "csv"):
        rows = _parse_excel_to_dicts(file_path_or_data)
        for row in rows:
            inv = _map_excel_gstr2b_row(row)
            if inv["gstin"] and inv["invoice_no"]:
                invoices.append(inv)
        return invoices

    # JSON format — standard GST portal structure
    data = _read_file_data(file_path_or_data, "json")

    # Navigate to B2B section — handle multiple JSON structures
    b2b_list = []
    if isinstance(data, dict):
        # Standard: data.docdata.b2b or data.data.docdata.b2b
        docdata = data.get("docdata", data.get("data", {}).get("docdata", {}))
        if isinstance(docdata, dict):
            b2b_list = docdata.get("b2b", [])
        # Flat structure: data.b2b
        if not b2b_list:
            b2b_list = data.get("b2b", [])

    for supplier in b2b_list:
        ctin = _normalize_gstin(supplier.get("ctin", ""))
        for inv in supplier.get("inv", []):
            inv_no = str(inv.get("inum", "")).strip()
            inv_date = _parse_portal_date(inv.get("idt", ""))
            inv_value = _safe_float(inv.get("val", 0))
            pos = str(inv.get("pos", ""))
            rev = str(inv.get("rev", "N")).upper().startswith("Y")
            itc_avl = str(inv.get("itcavl", "Y")).upper().startswith("Y")

            for item in inv.get("items", []):
                rate = _safe_float(item.get("rt", 0))
                txval = _safe_float(item.get("txval", 0))
                igst = _safe_float(item.get("igst", 0))
                cgst = _safe_float(item.get("cgst", 0))
                sgst = _safe_float(item.get("sgst", 0))
                cess = _safe_float(item.get("cess", 0))

                invoices.append({
                    "gstin": ctin,
                    "invoice_no": inv_no,
                    "invoice_date": inv_date,
                    "invoice_value": inv_value,
                    "taxable_value": txval,
                    "igst": igst,
                    "cgst": cgst,
                    "sgst": sgst,
                    "cess": cess,
                    "rate": rate,
                    "pos": pos,
                    "reverse_charge": rev,
                    "itc_available": itc_avl,
                })

    return invoices


# ── GSTR-1 PARSER ───────────────────────────────────────────────────────────

def parse_gstr1(file_path_or_data, file_type="json"):
    """Parse GSTR-1 from JSON, Excel, or PDF.
    Returns standardized list of invoice dicts.
    """
    invoices = []

    if file_type == "pdf":
        rows = _extract_tables_from_pdf(file_path_or_data)
        for row in rows:
            inv = _map_excel_gstr1_row(row)
            if inv["invoice_no"]:
                invoices.append(inv)
        return invoices

    if file_type in ("excel", "xlsx", "xls", "csv"):
        rows = _parse_excel_to_dicts(file_path_or_data)
        for row in rows:
            inv = _map_excel_gstr1_row(row)
            if inv["invoice_no"]:
                invoices.append(inv)
        return invoices

    # JSON
    data = _read_file_data(file_path_or_data, "json")
    if not isinstance(data, dict):
        return invoices

    # B2B section
    for customer in data.get("b2b", []):
        ctin = _normalize_gstin(customer.get("ctin", ""))
        for inv in customer.get("inv", []):
            inv_no = str(inv.get("inum", "")).strip()
            inv_date = _parse_portal_date(inv.get("idt", ""))
            inv_value = _safe_float(inv.get("val", 0))
            pos = str(inv.get("pos", ""))
            rchrg = str(inv.get("rchrg", "N")).upper().startswith("Y")

            for item in inv.get("itms", []):
                det = item.get("itm_det", {})
                rate = _safe_float(det.get("rt", 0))
                txval = _safe_float(det.get("txval", 0))
                igst = _safe_float(det.get("iamt", 0))
                cgst = _safe_float(det.get("camt", 0))
                sgst = _safe_float(det.get("samt", 0))
                cess = _safe_float(det.get("csamt", 0))

                invoices.append({
                    "gstin": ctin,
                    "invoice_no": inv_no,
                    "invoice_date": inv_date,
                    "invoice_value": inv_value,
                    "taxable_value": txval,
                    "igst": igst,
                    "cgst": cgst,
                    "sgst": sgst,
                    "cess": cess,
                    "rate": rate,
                    "pos": pos,
                    "reverse_charge": rchrg,
                    "supply_type": "B2B",
                })

    # B2CS section
    for entry in data.get("b2cs", []):
        pos = str(entry.get("pos", ""))
        rate = _safe_float(entry.get("rt", 0))
        txval = _safe_float(entry.get("txval", 0))
        igst = _safe_float(entry.get("iamt", 0))
        cgst = _safe_float(entry.get("camt", 0))
        sgst = _safe_float(entry.get("samt", 0))
        cess = _safe_float(entry.get("csamt", 0))

        invoices.append({
            "gstin": "",
            "invoice_no": f"B2CS-{pos}-{rate}",
            "invoice_date": "",
            "invoice_value": round(txval + igst + cgst + sgst + cess, 2),
            "taxable_value": txval,
            "igst": igst,
            "cgst": cgst,
            "sgst": sgst,
            "cess": cess,
            "rate": rate,
            "pos": pos,
            "supply_type": "B2CS",
        })

    # CDNR section (credit/debit notes)
    for customer in data.get("cdnr", []):
        ctin = _normalize_gstin(customer.get("ctin", ""))
        for note in customer.get("nt", []):
            note_no = str(note.get("nt_num", "")).strip()
            note_date = _parse_portal_date(note.get("nt_dt", ""))
            note_value = _safe_float(note.get("val", 0))
            note_type = note.get("ntty", "")  # C=Credit, D=Debit

            for item in note.get("itms", []):
                det = item.get("itm_det", {})
                rate = _safe_float(det.get("rt", 0))
                txval = _safe_float(det.get("txval", 0))
                igst = _safe_float(det.get("iamt", 0))
                cgst = _safe_float(det.get("camt", 0))
                sgst = _safe_float(det.get("samt", 0))
                cess = _safe_float(det.get("csamt", 0))

                invoices.append({
                    "gstin": ctin,
                    "invoice_no": note_no,
                    "invoice_date": note_date,
                    "invoice_value": note_value,
                    "taxable_value": txval,
                    "igst": igst,
                    "cgst": cgst,
                    "sgst": sgst,
                    "cess": cess,
                    "rate": rate,
                    "pos": "",
                    "supply_type": f"CDNR-{note_type}",
                })

    return invoices


# ── GSTR-3B PARSER ──────────────────────────────────────────────────────────

def parse_gstr3b(file_path_or_data, file_type="json"):
    """Parse GSTR-3B summary data.
    Returns standardized dict with sections 3.1, 4, 6.1
    """
    result = {
        "sec_3_1": {
            "outward_taxable": {"taxable": 0, "igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
            "outward_zero_rated": {"taxable": 0, "igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
            "outward_nil_exempt": {"taxable": 0, "igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
            "inward_reverse_charge": {"taxable": 0, "igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
            "non_gst_outward": {"taxable": 0, "igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
        },
        "sec_4": {
            "itc_available": {"igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
            "itc_reversed": {"igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
            "net_itc": {"igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
            "ineligible_itc": {"igst": 0, "cgst": 0, "sgst": 0, "cess": 0},
        },
        "sec_6_1": {
            "igst": 0, "cgst": 0, "sgst": 0, "cess": 0,
        },
    }

    if file_type == "pdf":
        # PDF parsing for 3B is complex — try to extract key values
        rows = _extract_tables_from_pdf(file_path_or_data)
        # Attempt to map known row labels
        for row in rows:
            vals = list(row.values())
            label = str(vals[0]).strip().lower() if vals else ""
            if "outward taxable" in label or "(other than" in label:
                result["sec_3_1"]["outward_taxable"] = _parse_3b_row_values(vals[1:])
            elif "inward supplies" in label and "reverse charge" in label:
                result["sec_3_1"]["inward_reverse_charge"] = _parse_3b_row_values(vals[1:])
        return result

    if file_type in ("excel", "xlsx", "xls", "csv"):
        rows = _parse_excel_to_dicts(file_path_or_data)
        for row in rows:
            vals = list(row.values())
            label = str(vals[0]).strip().lower() if vals else ""
            if "outward taxable" in label:
                result["sec_3_1"]["outward_taxable"] = _parse_3b_row_values(vals[1:])
        return result

    # JSON
    data = _read_file_data(file_path_or_data, "json")
    if not isinstance(data, dict):
        return result

    # Navigate to ret_period data
    ret = data.get("ret_period", data)

    # Section 3.1 — Outward supplies
    sec3_1 = ret.get("sup_details", ret.get("sec_3_1", ret.get("3.1", {})))
    if isinstance(sec3_1, dict):
        mapping = {
            "osup_det": "outward_taxable",
            "osup_zero": "outward_zero_rated",
            "osup_nil_exmp": "outward_nil_exempt",
            "isup_rev": "inward_reverse_charge",
            "osup_nongst": "non_gst_outward",
        }
        for key, target in mapping.items():
            sec = sec3_1.get(key, {})
            if isinstance(sec, dict):
                result["sec_3_1"][target] = {
                    "taxable": _safe_float(sec.get("txval", 0)),
                    "igst": _safe_float(sec.get("iamt", 0)),
                    "cgst": _safe_float(sec.get("camt", 0)),
                    "sgst": _safe_float(sec.get("samt", 0)),
                    "cess": _safe_float(sec.get("csamt", 0)),
                }

    # Section 4 — ITC
    sec4 = ret.get("itc_elg", ret.get("sec_4", ret.get("4", {})))
    if isinstance(sec4, dict):
        itc_avl = sec4.get("itc_avl", [])
        if isinstance(itc_avl, list):
            total_avl = {"igst": 0, "cgst": 0, "sgst": 0, "cess": 0}
            for item in itc_avl:
                total_avl["igst"] += _safe_float(item.get("iamt", 0))
                total_avl["cgst"] += _safe_float(item.get("camt", 0))
                total_avl["sgst"] += _safe_float(item.get("samt", 0))
                total_avl["cess"] += _safe_float(item.get("csamt", 0))
            result["sec_4"]["itc_available"] = total_avl
        elif isinstance(itc_avl, dict):
            result["sec_4"]["itc_available"] = {
                "igst": _safe_float(itc_avl.get("iamt", 0)),
                "cgst": _safe_float(itc_avl.get("camt", 0)),
                "sgst": _safe_float(itc_avl.get("samt", 0)),
                "cess": _safe_float(itc_avl.get("csamt", 0)),
            }

        itc_rev = sec4.get("itc_rev", [])
        if isinstance(itc_rev, list):
            total_rev = {"igst": 0, "cgst": 0, "sgst": 0, "cess": 0}
            for item in itc_rev:
                total_rev["igst"] += _safe_float(item.get("iamt", 0))
                total_rev["cgst"] += _safe_float(item.get("camt", 0))
                total_rev["sgst"] += _safe_float(item.get("samt", 0))
                total_rev["cess"] += _safe_float(item.get("csamt", 0))
            result["sec_4"]["itc_reversed"] = total_rev

        itc_net = sec4.get("itc_net", {})
        if isinstance(itc_net, dict):
            result["sec_4"]["net_itc"] = {
                "igst": _safe_float(itc_net.get("iamt", 0)),
                "cgst": _safe_float(itc_net.get("camt", 0)),
                "sgst": _safe_float(itc_net.get("samt", 0)),
                "cess": _safe_float(itc_net.get("csamt", 0)),
            }
        else:
            # Compute net from available - reversed
            avl = result["sec_4"]["itc_available"]
            rev = result["sec_4"]["itc_reversed"]
            result["sec_4"]["net_itc"] = {
                "igst": round(avl["igst"] - rev["igst"], 2),
                "cgst": round(avl["cgst"] - rev["cgst"], 2),
                "sgst": round(avl["sgst"] - rev["sgst"], 2),
                "cess": round(avl["cess"] - rev["cess"], 2),
            }

        itc_inelg = sec4.get("itc_inelg", [])
        if isinstance(itc_inelg, list):
            total_inelg = {"igst": 0, "cgst": 0, "sgst": 0, "cess": 0}
            for item in itc_inelg:
                total_inelg["igst"] += _safe_float(item.get("iamt", 0))
                total_inelg["cgst"] += _safe_float(item.get("camt", 0))
                total_inelg["sgst"] += _safe_float(item.get("samt", 0))
                total_inelg["cess"] += _safe_float(item.get("csamt", 0))
            result["sec_4"]["ineligible_itc"] = total_inelg

    # Section 6.1 — Tax payable
    sec6 = ret.get("intr_ltfee", ret.get("sec_6_1", ret.get("6.1", {})))
    if isinstance(sec6, dict):
        tax_pay = sec6.get("intr_details", sec6)
        if isinstance(tax_pay, dict):
            result["sec_6_1"] = {
                "igst": _safe_float(tax_pay.get("iamt", 0)),
                "cgst": _safe_float(tax_pay.get("camt", 0)),
                "sgst": _safe_float(tax_pay.get("samt", 0)),
                "cess": _safe_float(tax_pay.get("csamt", 0)),
            }

    return result


def _parse_3b_row_values(vals):
    """Parse a row of 3B table values into {taxable, igst, cgst, sgst, cess}."""
    floats = [_safe_float(v) for v in vals[:5]]
    while len(floats) < 5:
        floats.append(0.0)
    return {
        "taxable": floats[0],
        "igst": floats[1],
        "cgst": floats[2],
        "sgst": floats[3],
        "cess": floats[4],
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  BOOKS DATA EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

def get_books_purchases(db_path, from_date, to_date):
    """Get all purchase invoices from Tally with GST breakup.
    Returns list of dicts: [{gstin, invoice_no, date, party, taxable_value, cgst, sgst, igst, rate}]
    Defensive: checks all columns exist before querying.
    """
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return []

    gst = _detect_gst_ledgers(conn)

    all_input = gst["input_cgst"] + gst["input_sgst"] + gst["input_igst"]
    if not all_input:
        conn.close()
        return []

    # Check which columns exist
    vch_cols = _safe_cols(conn, "trn_voucher")
    acct_cols = _safe_cols(conn, "trn_accounting")

    if "GUID" not in vch_cols or "LEDGERNAME" not in acct_cols:
        conn.close()
        return []

    _partygstin_col = "v.PARTYGSTIN" if "PARTYGSTIN" in vch_cols else "''"
    _pos_col = "v.PLACEOFSUPPLY" if "PLACEOFSUPPLY" in vch_cols else "''"
    _vtype_col = "v.VOUCHERTYPENAME" if "VOUCHERTYPENAME" in vch_cols else "''"
    _party_col = "v.PARTYLEDGERNAME" if "PARTYLEDGERNAME" in vch_cols else "''"
    _vnum_col = "v.VOUCHERNUMBER" if "VOUCHERNUMBER" in vch_cols else "''"
    _date_col = "v.DATE" if "DATE" in vch_cols else "''"

    placeholders = ",".join(["?"] * len(all_input))

    # Build voucher type filter only if column exists
    vtype_filter = "AND v.VOUCHERTYPENAME != 'Debit Note'" if "VOUCHERTYPENAME" in vch_cols else ""
    date_filter = f"AND v.DATE >= ? AND v.DATE <= ?" if "DATE" in vch_cols else ""
    date_params = [from_date, to_date] if "DATE" in vch_cols else []

    try:
        vouchers = conn.execute(f"""
            SELECT DISTINCT v.GUID, {_date_col}, {_vnum_col}, {_party_col},
                   {_partygstin_col}, {_pos_col}, {_vtype_col}
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ({placeholders})
              {vtype_filter}
              {date_filter}
            ORDER BY v.GUID
        """, all_input + date_params).fetchall()
    except Exception:
        conn.close()
        return []

    results = []
    seen = set()
    for guid, date, vchno, party, gstin, pos, vchtype in vouchers:
        if guid in seen:
            continue
        seen.add(guid)

        entries = conn.execute("""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a WHERE a.VOUCHER_GUID = ?
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0
        rate = 0.0

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in gst["purchases"]:
                taxable += abs(amt)
            elif ledger in gst["input_cgst"]:
                cgst += abs(amt)
            elif ledger in gst["input_sgst"]:
                sgst += abs(amt)
            elif ledger in gst["input_igst"]:
                igst += abs(amt)

        total_tax = cgst + sgst + igst
        if taxable > 0:
            rate = round(total_tax / taxable * 100, 1)

        if taxable > 0 or total_tax > 0:
            results.append({
                "guid": guid,
                "date": date or "",
                "invoice_no": vchno or "",
                "party": party or "",
                "gstin": _normalize_gstin(gstin),
                "pos": pos or "",
                "voucher_type": vchtype or "",
                "taxable_value": round(taxable, 2),
                "cgst": round(cgst, 2),
                "sgst": round(sgst, 2),
                "igst": round(igst, 2),
                "total_tax": round(total_tax, 2),
                "invoice_value": round(taxable + total_tax, 2),
                "rate": rate,
            })

    conn.close()
    return results


def get_books_sales(db_path, from_date, to_date):
    """Get all sales invoices from Tally with GST breakup.
    Returns list of dicts: [{gstin, invoice_no, date, party, taxable_value, cgst, sgst, igst, rate}]
    Defensive: checks all columns exist before querying.
    """
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return []

    gst = _detect_gst_ledgers(conn)

    all_output = gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"]
    if not all_output:
        conn.close()
        return []

    # Check which columns exist
    vch_cols = _safe_cols(conn, "trn_voucher")
    acct_cols = _safe_cols(conn, "trn_accounting")

    if "GUID" not in vch_cols or "LEDGERNAME" not in acct_cols:
        conn.close()
        return []

    _consignee_col = "v.CONSIGNEEGSTIN" if "CONSIGNEEGSTIN" in vch_cols else "''"
    _partygstin_col = "v.PARTYGSTIN" if "PARTYGSTIN" in vch_cols else "''"
    _pos_col = "v.PLACEOFSUPPLY" if "PLACEOFSUPPLY" in vch_cols else "''"
    _vtype_col = "v.VOUCHERTYPENAME" if "VOUCHERTYPENAME" in vch_cols else "''"
    _party_col = "v.PARTYLEDGERNAME" if "PARTYLEDGERNAME" in vch_cols else "''"
    _vnum_col = "v.VOUCHERNUMBER" if "VOUCHERNUMBER" in vch_cols else "''"
    _date_col = "v.DATE" if "DATE" in vch_cols else "''"

    placeholders = ",".join(["?"] * len(all_output))

    vtype_filter = "AND v.VOUCHERTYPENAME != 'Credit Note'" if "VOUCHERTYPENAME" in vch_cols else ""
    date_filter = f"AND v.DATE >= ? AND v.DATE <= ?" if "DATE" in vch_cols else ""
    date_params = [from_date, to_date] if "DATE" in vch_cols else []

    try:
        vouchers = conn.execute(f"""
            SELECT DISTINCT v.GUID, {_date_col}, {_vnum_col}, {_party_col},
                   {_partygstin_col}, {_consignee_col}, {_pos_col}, {_vtype_col}
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ({placeholders})
              {vtype_filter}
              {date_filter}
            ORDER BY v.GUID
        """, all_output + date_params).fetchall()
    except Exception:
        conn.close()
        return []

    results = []
    seen = set()
    for guid, date, vchno, party, gstin, consignee_gstin, pos, vchtype in vouchers:
        if guid in seen:
            continue
        seen.add(guid)

        entries = conn.execute("""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a WHERE a.VOUCHER_GUID = ?
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in gst["sales"]:
                taxable += abs(amt)
            elif ledger in gst["output_cgst"]:
                cgst += abs(amt)
            elif ledger in gst["output_sgst"]:
                sgst += abs(amt)
            elif ledger in gst["output_igst"]:
                igst += abs(amt)

        total_tax = cgst + sgst + igst
        rate = round(total_tax / taxable * 100, 1) if taxable > 0 else 0.0

        # Use consignee GSTIN if party GSTIN is missing
        effective_gstin = _normalize_gstin(gstin) or _normalize_gstin(consignee_gstin)

        if taxable > 0 or total_tax > 0:
            results.append({
                "guid": guid,
                "date": date or "",
                "invoice_no": vchno or "",
                "party": party or "",
                "gstin": effective_gstin,
                "pos": pos or "",
                "voucher_type": vchtype or "",
                "taxable_value": round(taxable, 2),
                "cgst": round(cgst, 2),
                "sgst": round(sgst, 2),
                "igst": round(igst, 2),
                "total_tax": round(total_tax, 2),
                "invoice_value": round(taxable + total_tax, 2),
                "rate": rate,
            })

    conn.close()
    return results


def _get_books_summary(db_path, from_date, to_date):
    """Get summary totals from books for 3B comparison."""
    purchases = get_books_purchases(db_path, from_date, to_date)
    sales = get_books_sales(db_path, from_date, to_date)

    sales_total = {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0}
    for s in sales:
        sales_total["taxable"] += s["taxable_value"]
        sales_total["cgst"] += s["cgst"]
        sales_total["sgst"] += s["sgst"]
        sales_total["igst"] += s["igst"]

    purchase_total = {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0}
    for p in purchases:
        purchase_total["taxable"] += p["taxable_value"]
        purchase_total["cgst"] += p["cgst"]
        purchase_total["sgst"] += p["sgst"]
        purchase_total["igst"] += p["igst"]

    # Round
    for d in (sales_total, purchase_total):
        for k in d:
            d[k] = round(d[k], 2)

    return {
        "sales": sales_total,
        "purchases": purchase_total,
        "output_tax": round(sales_total["cgst"] + sales_total["sgst"] + sales_total["igst"], 2),
        "input_tax": round(purchase_total["cgst"] + purchase_total["sgst"] + purchase_total["igst"], 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  RECONCILIATION ENGINES
# ═══════════════════════════════════════════════════════════════════════════════

def _build_match_key(gstin, invoice_no):
    """Build a normalized match key from GSTIN + invoice number."""
    return f"{_normalize_gstin(gstin)}|{_normalize_invoice_no(invoice_no)}"


def reconcile_itc(portal_data, books_data):
    """Reconcile GSTR-2B (portal) vs Books purchases.
    Returns detailed mismatch report.
    """
    # Index portal invoices by match key
    portal_map = {}
    for inv in portal_data:
        key = _build_match_key(inv["gstin"], inv["invoice_no"])
        if key in portal_map:
            # Aggregate items for same invoice
            portal_map[key]["taxable_value"] += inv["taxable_value"]
            portal_map[key]["cgst"] += inv["cgst"]
            portal_map[key]["sgst"] += inv["sgst"]
            portal_map[key]["igst"] += inv["igst"]
            portal_map[key]["cess"] += inv.get("cess", 0)
        else:
            portal_map[key] = {**inv, "cess": inv.get("cess", 0)}

    # Index books invoices
    books_map = {}
    for inv in books_data:
        key = _build_match_key(inv["gstin"], inv["invoice_no"])
        if key in books_map:
            books_map[key]["taxable_value"] += inv["taxable_value"]
            books_map[key]["cgst"] += inv["cgst"]
            books_map[key]["sgst"] += inv["sgst"]
            books_map[key]["igst"] += inv["igst"]
        else:
            books_map[key] = {**inv}

    matched = []
    only_in_portal = []
    only_in_books = []
    amount_mismatches = []
    rate_mismatches = []

    all_keys = set(portal_map.keys()) | set(books_map.keys())

    for key in sorted(all_keys):
        p = portal_map.get(key)
        b = books_map.get(key)

        if p and not b:
            only_in_portal.append({
                "gstin": p["gstin"],
                "invoice_no": p["invoice_no"],
                "invoice_date": _format_display_date(p.get("invoice_date", "")),
                "taxable_value": round(p["taxable_value"], 2),
                "cgst": round(p["cgst"], 2),
                "sgst": round(p["sgst"], 2),
                "igst": round(p["igst"], 2),
                "total_tax": round(p["cgst"] + p["sgst"] + p["igst"], 2),
                "remark": "ITC available in portal but not booked",
            })
        elif b and not p:
            only_in_books.append({
                "gstin": b["gstin"],
                "invoice_no": b["invoice_no"],
                "invoice_date": _format_display_date(b.get("date", "")),
                "party": b.get("party", ""),
                "taxable_value": round(b["taxable_value"], 2),
                "cgst": round(b["cgst"], 2),
                "sgst": round(b["sgst"], 2),
                "igst": round(b["igst"], 2),
                "total_tax": round(b["cgst"] + b["sgst"] + b["igst"], 2),
                "remark": "Booked in books but not in GSTR-2B",
            })
        else:
            # Both exist — check for mismatches
            tax_diff = abs(
                (p["cgst"] + p["sgst"] + p["igst"]) -
                (b["cgst"] + b["sgst"] + b["igst"])
            )
            txval_diff = abs(p["taxable_value"] - b["taxable_value"])

            entry = {
                "gstin": p["gstin"],
                "invoice_no": p["invoice_no"],
                "invoice_date": _format_display_date(p.get("invoice_date", "")),
                "party": b.get("party", ""),
                "portal_taxable": round(p["taxable_value"], 2),
                "books_taxable": round(b["taxable_value"], 2),
                "portal_tax": round(p["cgst"] + p["sgst"] + p["igst"], 2),
                "books_tax": round(b["cgst"] + b["sgst"] + b["igst"], 2),
                "taxable_diff": round(p["taxable_value"] - b["taxable_value"], 2),
                "tax_diff": round(
                    (p["cgst"] + p["sgst"] + p["igst"]) -
                    (b["cgst"] + b["sgst"] + b["igst"]),
                    2
                ),
                "portal_cgst": round(p["cgst"], 2),
                "portal_sgst": round(p["sgst"], 2),
                "portal_igst": round(p["igst"], 2),
                "books_cgst": round(b["cgst"], 2),
                "books_sgst": round(b["sgst"], 2),
                "books_igst": round(b["igst"], 2),
            }

            if txval_diff <= 1 and tax_diff <= 1:
                matched.append(entry)
            else:
                remarks = []
                if txval_diff > 1:
                    remarks.append(f"Taxable value diff: {round(txval_diff, 2)}")
                if tax_diff > 1:
                    remarks.append(f"Tax diff: {round(tax_diff, 2)}")
                entry["remark"] = "; ".join(remarks)
                amount_mismatches.append(entry)

            # Rate mismatch check
            p_rate = p.get("rate", 0)
            b_rate = b.get("rate", 0)
            if p_rate > 0 and b_rate > 0 and abs(p_rate - b_rate) > 0.1:
                rate_mismatches.append({
                    **entry,
                    "portal_rate": p_rate,
                    "books_rate": b_rate,
                    "remark": f"Rate mismatch: Portal {p_rate}% vs Books {b_rate}%",
                })

    # Compute totals
    portal_total = {
        "cgst": round(sum(inv["cgst"] for inv in portal_data), 2),
        "sgst": round(sum(inv["sgst"] for inv in portal_data), 2),
        "igst": round(sum(inv["igst"] for inv in portal_data), 2),
    }
    portal_total["total"] = round(portal_total["cgst"] + portal_total["sgst"] + portal_total["igst"], 2)

    books_total = {
        "cgst": round(sum(inv["cgst"] for inv in books_data), 2),
        "sgst": round(sum(inv["sgst"] for inv in books_data), 2),
        "igst": round(sum(inv["igst"] for inv in books_data), 2),
    }
    books_total["total"] = round(books_total["cgst"] + books_total["sgst"] + books_total["igst"], 2)

    difference = {
        "cgst": round(portal_total["cgst"] - books_total["cgst"], 2),
        "sgst": round(portal_total["sgst"] - books_total["sgst"], 2),
        "igst": round(portal_total["igst"] - books_total["igst"], 2),
    }
    difference["total"] = round(portal_total["total"] - books_total["total"], 2)

    return {
        "portal_total": portal_total,
        "books_total": books_total,
        "difference": difference,
        "matched_invoices": matched,
        "only_in_portal": only_in_portal,
        "only_in_books": only_in_books,
        "amount_mismatches": amount_mismatches,
        "rate_mismatches": rate_mismatches,
        "total_portal_invoices": len(portal_map),
        "total_books_invoices": len(books_map),
        "matched_count": len(matched),
        "mismatch_count": len(amount_mismatches) + len(only_in_portal) + len(only_in_books),
    }


def reconcile_output(portal_data, books_data):
    """Reconcile GSTR-1 (portal) vs Books sales.
    Returns detailed mismatch report.
    """
    # Filter only B2B from portal for invoice-level matching
    b2b_portal = [inv for inv in portal_data if inv.get("gstin")]
    b2cs_portal = [inv for inv in portal_data if not inv.get("gstin") and inv.get("supply_type") == "B2CS"]

    # Index portal B2B by match key
    portal_map = {}
    for inv in b2b_portal:
        key = _build_match_key(inv["gstin"], inv["invoice_no"])
        if key in portal_map:
            portal_map[key]["taxable_value"] += inv["taxable_value"]
            portal_map[key]["cgst"] += inv["cgst"]
            portal_map[key]["sgst"] += inv["sgst"]
            portal_map[key]["igst"] += inv["igst"]
        else:
            portal_map[key] = {**inv}

    # Books — only B2B (has GSTIN)
    b2b_books = [inv for inv in books_data if inv.get("gstin")]
    b2c_books = [inv for inv in books_data if not inv.get("gstin")]

    books_map = {}
    for inv in b2b_books:
        key = _build_match_key(inv["gstin"], inv["invoice_no"])
        if key in books_map:
            books_map[key]["taxable_value"] += inv["taxable_value"]
            books_map[key]["cgst"] += inv["cgst"]
            books_map[key]["sgst"] += inv["sgst"]
            books_map[key]["igst"] += inv["igst"]
        else:
            books_map[key] = {**inv}

    matched = []
    only_in_portal = []
    only_in_books = []
    amount_mismatches = []
    pos_mismatches = []

    all_keys = set(portal_map.keys()) | set(books_map.keys())

    for key in sorted(all_keys):
        p = portal_map.get(key)
        b = books_map.get(key)

        if p and not b:
            only_in_portal.append({
                "gstin": p["gstin"],
                "invoice_no": p["invoice_no"],
                "invoice_date": _format_display_date(p.get("invoice_date", "")),
                "taxable_value": round(p["taxable_value"], 2),
                "cgst": round(p["cgst"], 2),
                "sgst": round(p["sgst"], 2),
                "igst": round(p["igst"], 2),
                "total_tax": round(p["cgst"] + p["sgst"] + p["igst"], 2),
                "remark": "Filed in GSTR-1 but not in books (fictitious filing?)",
            })
        elif b and not p:
            only_in_books.append({
                "gstin": b["gstin"],
                "invoice_no": b["invoice_no"],
                "invoice_date": _format_display_date(b.get("date", "")),
                "party": b.get("party", ""),
                "taxable_value": round(b["taxable_value"], 2),
                "cgst": round(b["cgst"], 2),
                "sgst": round(b["sgst"], 2),
                "igst": round(b["igst"], 2),
                "total_tax": round(b["cgst"] + b["sgst"] + b["igst"], 2),
                "remark": "In books but NOT filed in GSTR-1",
            })
        else:
            tax_diff = abs(
                (p["cgst"] + p["sgst"] + p["igst"]) -
                (b["cgst"] + b["sgst"] + b["igst"])
            )
            txval_diff = abs(p["taxable_value"] - b["taxable_value"])

            entry = {
                "gstin": p["gstin"],
                "invoice_no": p["invoice_no"],
                "invoice_date": _format_display_date(p.get("invoice_date", "")),
                "party": b.get("party", ""),
                "portal_taxable": round(p["taxable_value"], 2),
                "books_taxable": round(b["taxable_value"], 2),
                "portal_tax": round(p["cgst"] + p["sgst"] + p["igst"], 2),
                "books_tax": round(b["cgst"] + b["sgst"] + b["igst"], 2),
                "taxable_diff": round(p["taxable_value"] - b["taxable_value"], 2),
                "tax_diff": round(
                    (p["cgst"] + p["sgst"] + p["igst"]) -
                    (b["cgst"] + b["sgst"] + b["igst"]),
                    2
                ),
            }

            if txval_diff <= 1 and tax_diff <= 1:
                matched.append(entry)
            else:
                remarks = []
                if txval_diff > 1:
                    remarks.append(f"Taxable value diff: {round(txval_diff, 2)}")
                if tax_diff > 1:
                    remarks.append(f"Tax diff: {round(tax_diff, 2)}")
                entry["remark"] = "; ".join(remarks)
                amount_mismatches.append(entry)

            # POS mismatch — IGST vs CGST+SGST
            p_has_igst = p["igst"] > 0
            b_has_igst = b["igst"] > 0
            if p_has_igst != b_has_igst:
                pos_mismatches.append({
                    **entry,
                    "portal_supply_type": "Inter-state" if p_has_igst else "Intra-state",
                    "books_supply_type": "Inter-state" if b_has_igst else "Intra-state",
                    "remark": "Place of supply mismatch (IGST vs CGST+SGST)",
                })

    # Totals
    portal_total = {
        "cgst": round(sum(inv["cgst"] for inv in portal_data), 2),
        "sgst": round(sum(inv["sgst"] for inv in portal_data), 2),
        "igst": round(sum(inv["igst"] for inv in portal_data), 2),
    }
    portal_total["total"] = round(portal_total["cgst"] + portal_total["sgst"] + portal_total["igst"], 2)

    books_total = {
        "cgst": round(sum(inv["cgst"] for inv in books_data), 2),
        "sgst": round(sum(inv["sgst"] for inv in books_data), 2),
        "igst": round(sum(inv["igst"] for inv in books_data), 2),
    }
    books_total["total"] = round(books_total["cgst"] + books_total["sgst"] + books_total["igst"], 2)

    difference = {
        "cgst": round(portal_total["cgst"] - books_total["cgst"], 2),
        "sgst": round(portal_total["sgst"] - books_total["sgst"], 2),
        "igst": round(portal_total["igst"] - books_total["igst"], 2),
    }
    difference["total"] = round(portal_total["total"] - books_total["total"], 2)

    return {
        "portal_total": portal_total,
        "books_total": books_total,
        "difference": difference,
        "matched_invoices": matched,
        "only_in_portal": only_in_portal,
        "only_in_books": only_in_books,
        "amount_mismatches": amount_mismatches,
        "pos_mismatches": pos_mismatches,
        "total_portal_invoices": len(portal_map),
        "total_books_invoices": len(books_map),
        "matched_count": len(matched),
        "mismatch_count": len(amount_mismatches) + len(only_in_portal) + len(only_in_books),
    }


def reconcile_summary(gstr3b_data, books_summary):
    """Reconcile GSTR-3B summary vs Books totals.
    Returns section-wise comparison.
    """
    checks = []

    # Section 3.1 — Outward supplies
    sec3_1 = gstr3b_data.get("sec_3_1", {})
    portal_outward = sec3_1.get("outward_taxable", {})
    books_sales = books_summary.get("sales", {})

    for component in ["igst", "cgst", "sgst"]:
        portal_val = _safe_float(portal_outward.get(component, 0))
        books_val = _safe_float(books_sales.get(component, 0))
        diff = round(portal_val - books_val, 2)
        checks.append({
            "section": "3.1 - Outward Taxable Supplies",
            "component": component.upper(),
            "portal_value": portal_val,
            "books_value": books_val,
            "difference": diff,
            "status": "Match" if abs(diff) <= 1 else "MISMATCH",
        })

    portal_outward_txval = _safe_float(portal_outward.get("taxable", 0))
    books_sales_txval = _safe_float(books_sales.get("taxable", 0))
    diff_txval = round(portal_outward_txval - books_sales_txval, 2)
    checks.append({
        "section": "3.1 - Outward Taxable Supplies",
        "component": "Taxable Value",
        "portal_value": portal_outward_txval,
        "books_value": books_sales_txval,
        "difference": diff_txval,
        "status": "Match" if abs(diff_txval) <= 1 else "MISMATCH",
    })

    # Section 4 — ITC
    sec4 = gstr3b_data.get("sec_4", {})
    portal_itc = sec4.get("itc_available", sec4.get("net_itc", {}))
    books_purchase = books_summary.get("purchases", {})

    for component in ["igst", "cgst", "sgst"]:
        portal_val = _safe_float(portal_itc.get(component, 0))
        books_val = _safe_float(books_purchase.get(component, 0))
        diff = round(portal_val - books_val, 2)
        checks.append({
            "section": "4 - ITC Claimed",
            "component": component.upper(),
            "portal_value": portal_val,
            "books_value": books_val,
            "difference": diff,
            "status": "Match" if abs(diff) <= 1 else "MISMATCH",
        })

    # Section 6.1 — Tax payable
    sec6 = gstr3b_data.get("sec_6_1", {})
    books_output_tax = books_summary.get("output_tax", 0)
    books_input_tax = books_summary.get("input_tax", 0)
    books_net_tax = round(books_output_tax - books_input_tax, 2)
    portal_net_tax = round(
        _safe_float(sec6.get("igst", 0)) +
        _safe_float(sec6.get("cgst", 0)) +
        _safe_float(sec6.get("sgst", 0)),
        2
    )

    checks.append({
        "section": "6.1 - Net Tax Payable",
        "component": "Total",
        "portal_value": portal_net_tax,
        "books_value": books_net_tax,
        "difference": round(portal_net_tax - books_net_tax, 2),
        "status": "Match" if abs(portal_net_tax - books_net_tax) <= 1 else "MISMATCH",
    })

    mismatches = [c for c in checks if c["status"] == "MISMATCH"]

    return {
        "checks": checks,
        "total_checks": len(checks),
        "passed": len(checks) - len(mismatches),
        "failed": len(mismatches),
        "mismatches": mismatches,
    }


def _cross_checks(gstr1_data, gstr2b_data, gstr3b_data, books_summary):
    """Run cross-checks between different returns."""
    checks = []

    # 1. GSTR-1 total vs GSTR-3B Section 3.1
    if gstr1_data and gstr3b_data:
        gstr1_total_tax = round(
            sum(inv["cgst"] + inv["sgst"] + inv["igst"] for inv in gstr1_data), 2
        )
        sec3_1 = gstr3b_data.get("sec_3_1", {}).get("outward_taxable", {})
        gstr3b_output = round(
            _safe_float(sec3_1.get("igst", 0)) +
            _safe_float(sec3_1.get("cgst", 0)) +
            _safe_float(sec3_1.get("sgst", 0)),
            2
        )
        diff = round(gstr1_total_tax - gstr3b_output, 2)
        checks.append({
            "check": "GSTR-1 Output Tax vs GSTR-3B Sec 3.1",
            "gstr1_value": gstr1_total_tax,
            "gstr3b_value": gstr3b_output,
            "difference": diff,
            "status": "Match" if abs(diff) <= 1 else "MISMATCH",
            "remark": "GSTR-1 and GSTR-3B output tax should match" if abs(diff) > 1 else "OK",
        })

    # 2. GSTR-2B ITC vs GSTR-3B Section 4
    if gstr2b_data and gstr3b_data:
        gstr2b_total_itc = round(
            sum(inv["cgst"] + inv["sgst"] + inv["igst"] for inv in gstr2b_data), 2
        )
        sec4 = gstr3b_data.get("sec_4", {}).get("itc_available", {})
        gstr3b_itc = round(
            _safe_float(sec4.get("igst", 0)) +
            _safe_float(sec4.get("cgst", 0)) +
            _safe_float(sec4.get("sgst", 0)),
            2
        )
        diff = round(gstr3b_itc - gstr2b_total_itc, 2)
        checks.append({
            "check": "GSTR-3B ITC Claimed vs GSTR-2B Available",
            "gstr3b_value": gstr3b_itc,
            "gstr2b_value": gstr2b_total_itc,
            "difference": diff,
            "status": "Match" if abs(diff) <= 1 else ("EXCESS CLAIM" if diff > 1 else "UNDER CLAIM"),
            "remark": "ITC claimed in 3B exceeds 2B available" if diff > 1 else (
                "ITC under-claimed vs 2B" if diff < -1 else "OK"
            ),
        })

    # 3. Books purchase register vs GSTR-2B total
    if gstr2b_data and books_summary:
        books_purchase_tax = books_summary.get("input_tax", 0)
        gstr2b_total_itc = round(
            sum(inv["cgst"] + inv["sgst"] + inv["igst"] for inv in gstr2b_data), 2
        )
        diff = round(books_purchase_tax - gstr2b_total_itc, 2)
        checks.append({
            "check": "Books Purchase Tax vs GSTR-2B Total",
            "books_value": books_purchase_tax,
            "portal_value": gstr2b_total_itc,
            "difference": diff,
            "status": "Match" if abs(diff) <= 1 else "MISMATCH",
            "remark": "Purchase register and 2B should broadly match" if abs(diff) > 1 else "OK",
        })

    # 4. Books sales register vs GSTR-1 total
    if gstr1_data and books_summary:
        books_sales_tax = books_summary.get("output_tax", 0)
        gstr1_total_tax = round(
            sum(inv["cgst"] + inv["sgst"] + inv["igst"] for inv in gstr1_data), 2
        )
        diff = round(books_sales_tax - gstr1_total_tax, 2)
        checks.append({
            "check": "Books Sales Tax vs GSTR-1 Total",
            "books_value": books_sales_tax,
            "portal_value": gstr1_total_tax,
            "difference": diff,
            "status": "Match" if abs(diff) <= 1 else "MISMATCH",
            "remark": "Sales register and GSTR-1 should match" if abs(diff) > 1 else "OK",
        })

    return checks


def _generate_risk_flags(itc_recon, output_recon, summary_recon, cross_checks):
    """Generate risk flags based on reconciliation results."""
    flags = []

    # ITC risks
    if itc_recon:
        portal_only_tax = sum(
            inv.get("total_tax", inv.get("cgst", 0) + inv.get("sgst", 0) + inv.get("igst", 0))
            for inv in itc_recon.get("only_in_portal", [])
        )
        if portal_only_tax > 0:
            flags.append({
                "severity": "MEDIUM",
                "category": "ITC",
                "description": f"ITC of Rs {portal_only_tax:,.2f} available in GSTR-2B but not claimed in books ({len(itc_recon.get('only_in_portal', []))} invoices)",
            })

        books_only_tax = sum(
            inv.get("total_tax", inv.get("cgst", 0) + inv.get("sgst", 0) + inv.get("igst", 0))
            for inv in itc_recon.get("only_in_books", [])
        )
        if books_only_tax > 0:
            flags.append({
                "severity": "HIGH",
                "category": "ITC",
                "description": f"ITC of Rs {books_only_tax:,.2f} claimed in books but NOT in GSTR-2B ({len(itc_recon.get('only_in_books', []))} invoices) — risk of disallowance",
            })

        if itc_recon.get("amount_mismatches"):
            total_diff = sum(abs(inv.get("tax_diff", 0)) for inv in itc_recon["amount_mismatches"])
            flags.append({
                "severity": "MEDIUM",
                "category": "ITC",
                "description": f"Tax amount mismatch of Rs {total_diff:,.2f} across {len(itc_recon['amount_mismatches'])} purchase invoices",
            })

        if itc_recon.get("rate_mismatches"):
            flags.append({
                "severity": "MEDIUM",
                "category": "ITC",
                "description": f"GST rate mismatch in {len(itc_recon['rate_mismatches'])} purchase invoices — verify correct rate applied",
            })

    # Output risks
    if output_recon:
        if output_recon.get("only_in_portal"):
            flags.append({
                "severity": "HIGH",
                "category": "Output Tax",
                "description": f"{len(output_recon['only_in_portal'])} invoices filed in GSTR-1 but not found in books — possible fictitious filing",
            })

        if output_recon.get("only_in_books"):
            flags.append({
                "severity": "HIGH",
                "category": "Output Tax",
                "description": f"{len(output_recon['only_in_books'])} sales invoices in books but NOT filed in GSTR-1 — non-compliance risk",
            })

        if output_recon.get("pos_mismatches"):
            flags.append({
                "severity": "MEDIUM",
                "category": "Output Tax",
                "description": f"Place of supply mismatch in {len(output_recon['pos_mismatches'])} invoices (IGST vs CGST+SGST)",
            })

    # Summary risks
    if summary_recon and summary_recon.get("failed", 0) > 0:
        flags.append({
            "severity": "HIGH",
            "category": "GSTR-3B",
            "description": f"GSTR-3B has {summary_recon['failed']} mismatches with books — verify before filing",
        })

    # Cross-check risks
    for cc in (cross_checks or []):
        if cc.get("status") not in ("Match", "OK"):
            severity = "HIGH" if "EXCESS" in cc.get("status", "") else "MEDIUM"
            flags.append({
                "severity": severity,
                "category": "Cross-Check",
                "description": f"{cc['check']}: {cc['status']} — Diff: Rs {abs(cc.get('difference', 0)):,.2f}",
            })

    # Sort by severity
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    flags.sort(key=lambda f: severity_order.get(f["severity"], 99))

    return flags


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN AUDIT FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

def full_gst_audit(db_path, gstr2b_path=None, gstr1_path=None, gstr3b_path=None,
                   from_date=None, to_date=None,
                   gstr2b_type="json", gstr1_type="json", gstr3b_type="json"):
    """Run complete GST audit. Returns comprehensive report dict.

    Args:
        db_path: Path to Tally SQLite database
        gstr2b_path: GSTR-2B file (path, bytes, or file-like)
        gstr1_path: GSTR-1 file
        gstr3b_path: GSTR-3B file
        from_date: Start date YYYYMMDD
        to_date: End date YYYYMMDD
        gstr2b_type: File type for GSTR-2B (json, excel, pdf)
        gstr1_type: File type for GSTR-1
        gstr3b_type: File type for GSTR-3B
    """
    result = {
        "period": _period_label(from_date, to_date),
        "from_date": from_date,
        "to_date": to_date,
        "itc_reconciliation": None,
        "output_reconciliation": None,
        "summary_reconciliation": None,
        "cross_checks": None,
        "risk_flags": [],
        "has_gstr2b": gstr2b_path is not None,
        "has_gstr1": gstr1_path is not None,
        "has_gstr3b": gstr3b_path is not None,
    }

    gstr2b_data = None
    gstr1_data = None
    gstr3b_data = None
    books_summary = None

    # Parse portal files
    if gstr2b_path is not None:
        try:
            gstr2b_data = parse_gstr2b(gstr2b_path, gstr2b_type)
        except Exception as e:
            result["gstr2b_error"] = str(e)

    if gstr1_path is not None:
        try:
            gstr1_data = parse_gstr1(gstr1_path, gstr1_type)
        except Exception as e:
            result["gstr1_error"] = str(e)

    if gstr3b_path is not None:
        try:
            gstr3b_data = parse_gstr3b(gstr3b_path, gstr3b_type)
        except Exception as e:
            result["gstr3b_error"] = str(e)

    # Get books data
    if from_date and to_date:
        try:
            books_summary = _get_books_summary(db_path, from_date, to_date)
        except Exception as e:
            result["books_error"] = str(e)

    # ITC Reconciliation (GSTR-2B vs Books)
    if gstr2b_data and from_date and to_date:
        try:
            books_purchases = get_books_purchases(db_path, from_date, to_date)
            result["itc_reconciliation"] = reconcile_itc(gstr2b_data, books_purchases)
        except Exception as e:
            result["itc_error"] = str(e)

    # Output Reconciliation (GSTR-1 vs Books)
    if gstr1_data and from_date and to_date:
        try:
            books_sales = get_books_sales(db_path, from_date, to_date)
            result["output_reconciliation"] = reconcile_output(gstr1_data, books_sales)
        except Exception as e:
            result["output_error"] = str(e)

    # Summary Reconciliation (GSTR-3B vs Books)
    if gstr3b_data and books_summary:
        try:
            result["summary_reconciliation"] = reconcile_summary(gstr3b_data, books_summary)
        except Exception as e:
            result["summary_error"] = str(e)

    # Cross-checks
    try:
        result["cross_checks"] = _cross_checks(gstr1_data, gstr2b_data, gstr3b_data, books_summary)
    except Exception as e:
        result["cross_checks_error"] = str(e)

    # Risk flags
    result["risk_flags"] = _generate_risk_flags(
        result.get("itc_reconciliation"),
        result.get("output_reconciliation"),
        result.get("summary_reconciliation"),
        result.get("cross_checks"),
    )

    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  EXCEL REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate_excel_report(audit_result, output_path):
    """Generate Excel audit report with multiple sheets.

    Requires openpyxl (pip install openpyxl).
    Returns the output_path on success.
    """
    if not HAS_PANDAS:
        raise ImportError("pandas and openpyxl required. Install: pip install pandas openpyxl")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

        # ── Summary Sheet ────────────────────────────────────────────
        summary_rows = []
        summary_rows.append({"Item": "Period", "Value": audit_result.get("period", "")})

        itc = audit_result.get("itc_reconciliation")
        if itc:
            summary_rows.append({"Item": "", "Value": ""})
            summary_rows.append({"Item": "=== ITC RECONCILIATION ===", "Value": ""})
            summary_rows.append({"Item": "Portal ITC (Total)", "Value": itc["portal_total"]["total"]})
            summary_rows.append({"Item": "Books ITC (Total)", "Value": itc["books_total"]["total"]})
            summary_rows.append({"Item": "Difference", "Value": itc["difference"]["total"]})
            summary_rows.append({"Item": "Matched Invoices", "Value": itc["matched_count"]})
            summary_rows.append({"Item": "Only in Portal", "Value": len(itc["only_in_portal"])})
            summary_rows.append({"Item": "Only in Books", "Value": len(itc["only_in_books"])})
            summary_rows.append({"Item": "Amount Mismatches", "Value": len(itc["amount_mismatches"])})

        output = audit_result.get("output_reconciliation")
        if output:
            summary_rows.append({"Item": "", "Value": ""})
            summary_rows.append({"Item": "=== OUTPUT TAX RECONCILIATION ===", "Value": ""})
            summary_rows.append({"Item": "GSTR-1 Output Tax (Total)", "Value": output["portal_total"]["total"]})
            summary_rows.append({"Item": "Books Output Tax (Total)", "Value": output["books_total"]["total"]})
            summary_rows.append({"Item": "Difference", "Value": output["difference"]["total"]})
            summary_rows.append({"Item": "Matched Invoices", "Value": output["matched_count"]})
            summary_rows.append({"Item": "Only in GSTR-1", "Value": len(output["only_in_portal"])})
            summary_rows.append({"Item": "Only in Books", "Value": len(output["only_in_books"])})

        # Risk flags
        flags = audit_result.get("risk_flags", [])
        if flags:
            summary_rows.append({"Item": "", "Value": ""})
            summary_rows.append({"Item": "=== RISK FLAGS ===", "Value": ""})
            for f in flags:
                summary_rows.append({"Item": f"[{f['severity']}] {f['category']}", "Value": f["description"]})

        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

        # ── ITC Detail Sheets ────────────────────────────────────────
        if itc:
            if itc["matched_invoices"]:
                pd.DataFrame(itc["matched_invoices"]).to_excel(
                    writer, sheet_name="ITC - Matched", index=False)
            if itc["only_in_portal"]:
                pd.DataFrame(itc["only_in_portal"]).to_excel(
                    writer, sheet_name="ITC - Only Portal", index=False)
            if itc["only_in_books"]:
                pd.DataFrame(itc["only_in_books"]).to_excel(
                    writer, sheet_name="ITC - Only Books", index=False)
            if itc["amount_mismatches"]:
                pd.DataFrame(itc["amount_mismatches"]).to_excel(
                    writer, sheet_name="ITC - Mismatches", index=False)
            if itc.get("rate_mismatches"):
                pd.DataFrame(itc["rate_mismatches"]).to_excel(
                    writer, sheet_name="ITC - Rate Mismatch", index=False)

        # ── Output Detail Sheets ─────────────────────────────────────
        if output:
            if output["matched_invoices"]:
                pd.DataFrame(output["matched_invoices"]).to_excel(
                    writer, sheet_name="Output - Matched", index=False)
            if output["only_in_portal"]:
                pd.DataFrame(output["only_in_portal"]).to_excel(
                    writer, sheet_name="Output - Only GSTR1", index=False)
            if output["only_in_books"]:
                pd.DataFrame(output["only_in_books"]).to_excel(
                    writer, sheet_name="Output - Only Books", index=False)
            if output["amount_mismatches"]:
                pd.DataFrame(output["amount_mismatches"]).to_excel(
                    writer, sheet_name="Output - Mismatches", index=False)

        # ── 3B Summary Sheet ─────────────────────────────────────────
        summary_recon = audit_result.get("summary_reconciliation")
        if summary_recon and summary_recon.get("checks"):
            pd.DataFrame(summary_recon["checks"]).to_excel(
                writer, sheet_name="3B Reconciliation", index=False)

        # ── Cross-Checks Sheet ───────────────────────────────────────
        cc = audit_result.get("cross_checks")
        if cc:
            pd.DataFrame(cc).to_excel(writer, sheet_name="Cross-Checks", index=False)

        # ── Risk Flags Sheet ─────────────────────────────────────────
        if flags:
            pd.DataFrame(flags).to_excel(writer, sheet_name="Risk Flags", index=False)

    return output_path


# ═══════════════════════════════════════════════════════════════════════════════
#  FILE TYPE DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def detect_file_type(filename):
    """Detect file type from filename/extension."""
    if not filename:
        return "json"
    name = str(filename).lower()
    if name.endswith(".json"):
        return "json"
    elif name.endswith(".pdf"):
        return "pdf"
    elif name.endswith(".csv"):
        return "csv"
    elif name.endswith((".xlsx", ".xls")):
        return "excel"
    return "json"


# ═══════════════════════════════════════════════════════════════════════════════
#  QUICK TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("GST Reconciliation Engine — Seven Labs Vision")
    print("=" * 50)

    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        gst = _detect_gst_ledgers(conn)
        print(f"Output CGST ledgers: {gst['output_cgst']}")
        print(f"Input CGST ledgers: {gst['input_cgst']}")
        print(f"Sales ledgers: {gst['sales']}")
        print(f"Purchase ledgers: {gst['purchases']}")

        purchases = get_books_purchases(DB_PATH, "20250401", "20260131")
        sales = get_books_sales(DB_PATH, "20250401", "20260131")
        print(f"\nPurchase invoices: {len(purchases)}")
        print(f"Sales invoices: {len(sales)}")

        if purchases:
            total_itc = sum(p["total_tax"] for p in purchases)
            print(f"Total ITC in books: {total_itc:,.2f}")
        if sales:
            total_output = sum(s["total_tax"] for s in sales)
            print(f"Total output tax in books: {total_output:,.2f}")

        conn.close()
    else:
        print(f"Database not found at {DB_PATH}")
