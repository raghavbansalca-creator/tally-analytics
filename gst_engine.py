"""
GST Computation Engine for ROHIT PHARMA
Extracts GST data from Tally SQLite database for GSTR-1, GSTR-2, and GSTR-3B.
"""

import sqlite3
import os
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")

COMPANY_GSTIN = "09BALPA2486E1ZR"
COMPANY_STATE = "Uttar Pradesh"

# ── GST LEDGER CLASSIFICATION ──────────────────────────────────────────────

OUTPUT_CGST_LEDGERS = [
    "OUTPUT CGST", "OUTPUT CGST 2.5%", "Output CGST 6%", "Output CGST 9%",
]
OUTPUT_SGST_LEDGERS = [
    "OUTPUT SGST", "OUTPUT SGST 2.5%", "Output SGST 6%", "Output SGST 9%",
]
OUTPUT_IGST_LEDGERS = [
    "OUTPUT IGST", "OUTPUT IGST 12%", "OUTPUT IGST 18%", "OUTPUT IGST - 5%",
]
INPUT_CGST_LEDGERS = [
    "INPUT CGST", "INPUT CGST 2.5%", "Input CGST 6%", "INPUT CGST 9%",
]
INPUT_SGST_LEDGERS = [
    "INPUT SGST", "INPUT SGST 2.5%", "Input SGST 6%", "INPUT SGST 9%",
]
INPUT_IGST_LEDGERS = [
    "INPUT IGST 12%", "INPUT IGST 5%", "IGST",
]

ALL_OUTPUT_GST = OUTPUT_CGST_LEDGERS + OUTPUT_SGST_LEDGERS + OUTPUT_IGST_LEDGERS
ALL_INPUT_GST = INPUT_CGST_LEDGERS + INPUT_SGST_LEDGERS + INPUT_IGST_LEDGERS
ALL_GST_LEDGERS = ALL_OUTPUT_GST + ALL_INPUT_GST

SALES_LEDGERS = ["SALES 5%", "SALES 12%", "SALE - EXUP 5%", "SALE - EX UP 12%"]
PURCHASE_LEDGERS = ["PURCHASE 5%", "Purchase 12%", "Purchase 18%"]


def get_conn():
    return sqlite3.connect(DB_PATH)


def _safe_float(val):
    """Convert a value to float, returning 0.0 on failure."""
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _classify_gst_ledger(name):
    """Return (type, component) for a GST ledger name.
    type: 'output' or 'input'
    component: 'cgst', 'sgst', 'igst'
    """
    upper = name.upper()
    if "OUTPUT" in upper or (name in OUTPUT_IGST_LEDGERS):
        if "CGST" in upper:
            return ("output", "cgst")
        elif "SGST" in upper:
            return ("output", "sgst")
        elif "IGST" in upper:
            return ("output", "igst")
    if "INPUT" in upper or name == "IGST":
        if "CGST" in upper:
            return ("input", "cgst")
        elif "SGST" in upper:
            return ("input", "sgst")
        elif "IGST" in upper or name == "IGST":
            return ("input", "igst")
    return (None, None)


def _extract_rate_from_ledger(name):
    """Extract GST rate from ledger name like 'OUTPUT CGST 2.5%' -> 5.0 (doubled for CGST/SGST)."""
    import re
    m = re.search(r"(\d+\.?\d*)%?$", name.replace("%", "").strip())
    if m:
        rate = float(m.group(1))
        upper = name.upper()
        if "CGST" in upper or "SGST" in upper:
            return rate * 2  # CGST 2.5% means total GST rate = 5%
        return rate
    return 0.0


def _dedupe_filter():
    """Return SQL WHERE clause fragment to avoid double-counting.
    Data has each row duplicated: once with GSTTAXRATE='' and once with GSTTAXRATE='0'.
    We take only the rows where GSTTAXRATE is NULL or empty string.
    If GSTTAXRATE column doesn't exist, return empty string (no filter needed).
    """
    try:
        conn = get_conn()
        cols = [c[1] for c in conn.execute("PRAGMA table_info(trn_accounting)").fetchall()]
        conn.close()
        if "GSTTAXRATE" in cols:
            return "AND (a.GSTTAXRATE IS NULL OR a.GSTTAXRATE = '')"
        return ""
    except:
        return ""


# ── AVAILABLE MONTHS ───────────────────────────────────────────────────────

def get_available_months(conn=None):
    """Return list of (YYYYMM, display_label) for months with GST data."""
    close = False
    if conn is None:
        conn = get_conn()
        close = True

    rows = conn.execute("""
        SELECT DISTINCT SUBSTR(DATE, 1, 6) as month
        FROM trn_voucher
        WHERE VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE', 'Purchase', 'Credit Note', 'Debit Note')
        ORDER BY month
    """).fetchall()

    if close:
        conn.close()

    months = []
    month_names = {
        "01": "January", "02": "February", "03": "March", "04": "April",
        "05": "May", "06": "June", "07": "July", "08": "August",
        "09": "September", "10": "October", "11": "November", "12": "December",
    }
    for (m,) in rows:
        yyyy = m[:4]
        mm = m[4:6]
        label = f"{month_names.get(mm, mm)} {yyyy}"
        months.append((m, label))

    return months


# ══════════════════════════════════════════════════════════════════════════════
#  GSTR-1: OUTPUT TAX (SALES)
# ══════════════════════════════════════════════════════════════════════════════

def gstr1_b2b_invoices(conn, month=None):
    """B2B Sales: Invoice-wise with party GSTIN, invoice no, date, taxable, CGST, SGST, IGST.
    Returns list of dicts.
    """
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""
    dedup = _dedupe_filter()

    # Get all Sales/SALE INVOICE vouchers with GSTIN
    vouchers_sql = f"""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.PARTYGSTIN,
               v.PLACEOFSUPPLY, v.VOUCHERTYPENAME
        FROM trn_voucher v
        WHERE v.VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE')
          AND v.PARTYGSTIN IS NOT NULL AND v.PARTYGSTIN != ''
          {month_filter}
        ORDER BY v.DATE, v.VOUCHERNUMBER
    """
    vouchers = conn.execute(vouchers_sql).fetchall()

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos, vchtype in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        # Get accounting entries for this voucher (deduplicated)
        entries = conn.execute(f"""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
              {dedup}
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in SALES_LEDGERS:
                taxable += abs(amt)
            elif ledger in OUTPUT_CGST_LEDGERS:
                cgst += abs(amt)
            elif ledger in OUTPUT_SGST_LEDGERS:
                sgst += abs(amt)
            elif ledger in OUTPUT_IGST_LEDGERS:
                igst += abs(amt)

        if taxable > 0 or cgst > 0 or sgst > 0 or igst > 0:
            # Determine GST rate
            total_tax = cgst + sgst + igst
            gst_rate = round((total_tax / taxable * 100), 1) if taxable > 0 else 0.0

            results.append({
                "date": _format_date(date),
                "date_raw": date,
                "invoice_no": vchno,
                "party": party,
                "gstin": gstin,
                "place_of_supply": pos or "",
                "taxable_value": round(taxable, 2),
                "cgst": round(cgst, 2),
                "sgst": round(sgst, 2),
                "igst": round(igst, 2),
                "total_tax": round(total_tax, 2),
                "invoice_value": round(taxable + total_tax, 2),
                "gst_rate": gst_rate,
            })

    return results


def gstr1_b2c_invoices(conn, month=None):
    """B2C Sales: Where party has no GSTIN. Returns list of dicts."""
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""
    dedup = _dedupe_filter()

    vouchers_sql = f"""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.PLACEOFSUPPLY
        FROM trn_voucher v
        WHERE v.VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE')
          AND (v.PARTYGSTIN IS NULL OR v.PARTYGSTIN = '')
          {month_filter}
        ORDER BY v.DATE, v.VOUCHERNUMBER
    """
    vouchers = conn.execute(vouchers_sql).fetchall()

    results = []
    seen_guids = set()
    for guid, date, vchno, party, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute(f"""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
              {dedup}
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in SALES_LEDGERS:
                taxable += abs(amt)
            elif ledger in OUTPUT_CGST_LEDGERS or ledger in INPUT_CGST_LEDGERS:
                cgst += abs(amt)
            elif ledger in OUTPUT_SGST_LEDGERS or ledger in INPUT_SGST_LEDGERS:
                sgst += abs(amt)
            elif ledger in OUTPUT_IGST_LEDGERS or ledger in INPUT_IGST_LEDGERS:
                igst += abs(amt)

        if taxable > 0 or cgst > 0 or sgst > 0 or igst > 0:
            total_tax = cgst + sgst + igst
            gst_rate = round((total_tax / taxable * 100), 1) if taxable > 0 else 0.0
            results.append({
                "date": _format_date(date),
                "date_raw": date,
                "invoice_no": vchno,
                "party": party,
                "place_of_supply": pos or COMPANY_STATE,
                "taxable_value": round(taxable, 2),
                "cgst": round(cgst, 2),
                "sgst": round(sgst, 2),
                "igst": round(igst, 2),
                "total_tax": round(total_tax, 2),
                "invoice_value": round(taxable + total_tax, 2),
                "gst_rate": gst_rate,
            })

    return results


def gstr1_credit_notes(conn, month=None):
    """Credit Notes (Sales Returns). Returns list of dicts."""
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""
    dedup = _dedupe_filter()

    vouchers = conn.execute(f"""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.PARTYGSTIN, v.PLACEOFSUPPLY
        FROM trn_voucher v
        WHERE v.VOUCHERTYPENAME = 'Credit Note'
          {month_filter}
        ORDER BY v.DATE, v.VOUCHERNUMBER
    """).fetchall()

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute(f"""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
              {dedup}
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in SALES_LEDGERS:
                taxable += abs(amt)
            elif ledger in OUTPUT_CGST_LEDGERS:
                cgst += abs(amt)
            elif ledger in OUTPUT_SGST_LEDGERS:
                sgst += abs(amt)
            elif ledger in OUTPUT_IGST_LEDGERS:
                igst += abs(amt)

        if taxable > 0 or cgst > 0 or sgst > 0 or igst > 0:
            total_tax = cgst + sgst + igst
            results.append({
                "date": _format_date(date),
                "date_raw": date,
                "note_no": vchno,
                "party": party,
                "gstin": gstin or "",
                "place_of_supply": pos or "",
                "taxable_value": round(taxable, 2),
                "cgst": round(cgst, 2),
                "sgst": round(sgst, 2),
                "igst": round(igst, 2),
                "total_tax": round(total_tax, 2),
                "note_value": round(taxable + total_tax, 2),
            })

    return results


def gstr1_hsn_summary(conn, month=None):
    """HSN-wise summary of outward supplies."""
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""
    dedup = _dedupe_filter()

    # Check if GSTHSNNAME column exists
    cols = [c[1] for c in conn.execute("PRAGMA table_info(trn_accounting)").fetchall()]
    if "GSTHSNNAME" not in cols:
        return []

    # Get HSN data from accounting entries on sales vouchers
    rows = conn.execute(f"""
        SELECT a.GSTHSNNAME, a.LEDGERNAME, SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE')
          AND a.GSTHSNNAME IS NOT NULL AND a.GSTHSNNAME != ''
          {dedup}
          {month_filter}
        GROUP BY a.GSTHSNNAME, a.LEDGERNAME
    """).fetchall()

    hsn_data = defaultdict(lambda: {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0})
    for hsn, ledger, total in rows:
        if ledger in SALES_LEDGERS:
            hsn_data[hsn]["taxable"] += total
        elif ledger in OUTPUT_CGST_LEDGERS:
            hsn_data[hsn]["cgst"] += total
        elif ledger in OUTPUT_SGST_LEDGERS:
            hsn_data[hsn]["sgst"] += total
        elif ledger in OUTPUT_IGST_LEDGERS:
            hsn_data[hsn]["igst"] += total

    results = []
    for hsn, vals in sorted(hsn_data.items()):
        total_tax = vals["cgst"] + vals["sgst"] + vals["igst"]
        gst_rate = round((total_tax / vals["taxable"] * 100), 1) if vals["taxable"] > 0 else 0
        results.append({
            "hsn_code": hsn,
            "description": _hsn_description(hsn),
            "taxable_value": round(vals["taxable"], 2),
            "cgst": round(vals["cgst"], 2),
            "sgst": round(vals["sgst"], 2),
            "igst": round(vals["igst"], 2),
            "total_tax": round(total_tax, 2),
            "gst_rate": gst_rate,
        })

    return results


def gstr1_monthly_summary(conn):
    """Month-wise summary of all output GST."""
    dedup = _dedupe_filter()

    rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE', 'Credit Note')
          {dedup}
        GROUP BY month, a.LEDGERNAME
        ORDER BY month
    """).fetchall()

    monthly = defaultdict(lambda: {
        "sales_taxable": 0, "output_cgst": 0, "output_sgst": 0, "output_igst": 0,
        "cn_taxable": 0, "cn_cgst": 0, "cn_sgst": 0, "cn_igst": 0,
    })

    # Also get credit note amounts separately
    cn_rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME = 'Credit Note'
          {dedup}
        GROUP BY month, a.LEDGERNAME
    """).fetchall()

    cn_data = defaultdict(lambda: {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0})
    for month, ledger, total in cn_rows:
        if ledger in SALES_LEDGERS:
            cn_data[month]["taxable"] += total
        elif ledger in OUTPUT_CGST_LEDGERS:
            cn_data[month]["cgst"] += total
        elif ledger in OUTPUT_SGST_LEDGERS:
            cn_data[month]["sgst"] += total
        elif ledger in OUTPUT_IGST_LEDGERS:
            cn_data[month]["igst"] += total

    # Sales (including SALE INVOICE)
    sales_rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE')
          {dedup}
        GROUP BY month, a.LEDGERNAME
    """).fetchall()

    for month, ledger, total in sales_rows:
        if ledger in SALES_LEDGERS:
            monthly[month]["sales_taxable"] += total
        elif ledger in OUTPUT_CGST_LEDGERS or ledger in INPUT_CGST_LEDGERS:
            monthly[month]["output_cgst"] += total
        elif ledger in OUTPUT_SGST_LEDGERS or ledger in INPUT_SGST_LEDGERS:
            monthly[month]["output_sgst"] += total
        elif ledger in OUTPUT_IGST_LEDGERS or ledger in INPUT_IGST_LEDGERS:
            monthly[month]["output_igst"] += total

    results = []
    for month in sorted(monthly.keys()):
        d = monthly[month]
        cn = cn_data.get(month, {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0})
        net_taxable = d["sales_taxable"] - cn["taxable"]
        net_cgst = d["output_cgst"] - cn["cgst"]
        net_sgst = d["output_sgst"] - cn["sgst"]
        net_igst = d["output_igst"] - cn["igst"]
        net_total_tax = net_cgst + net_sgst + net_igst

        results.append({
            "month": month,
            "month_label": _month_label(month),
            "gross_taxable": round(d["sales_taxable"], 2),
            "cn_taxable": round(cn["taxable"], 2),
            "net_taxable": round(net_taxable, 2),
            "cgst": round(net_cgst, 2),
            "sgst": round(net_sgst, 2),
            "igst": round(net_igst, 2),
            "total_tax": round(net_total_tax, 2),
        })

    return results


# ══════════════════════════════════════════════════════════════════════════════
#  INPUT TAX CREDIT (PURCHASES)
# ══════════════════════════════════════════════════════════════════════════════

def input_tax_invoices(conn, month=None):
    """Purchase-wise input tax credit detail. Returns list of dicts."""
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""
    dedup = _dedupe_filter()

    vouchers = conn.execute(f"""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.PARTYGSTIN, v.PLACEOFSUPPLY
        FROM trn_voucher v
        WHERE v.VOUCHERTYPENAME = 'Purchase'
          {month_filter}
        ORDER BY v.DATE, v.VOUCHERNUMBER
    """).fetchall()

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute(f"""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
              {dedup}
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in PURCHASE_LEDGERS:
                taxable += abs(amt)
            elif ledger in INPUT_CGST_LEDGERS:
                cgst += abs(amt)
            elif ledger in INPUT_SGST_LEDGERS:
                sgst += abs(amt)
            elif ledger in INPUT_IGST_LEDGERS:
                igst += abs(amt)

        if taxable > 0 or cgst > 0 or sgst > 0 or igst > 0:
            total_tax = cgst + sgst + igst
            results.append({
                "date": _format_date(date),
                "date_raw": date,
                "invoice_no": vchno,
                "supplier": party,
                "gstin": gstin or "",
                "place_of_supply": pos or "",
                "taxable_value": round(taxable, 2),
                "cgst": round(cgst, 2),
                "sgst": round(sgst, 2),
                "igst": round(igst, 2),
                "total_tax": round(total_tax, 2),
                "invoice_value": round(taxable + total_tax, 2),
            })

    return results


def input_tax_debit_notes(conn, month=None):
    """Debit Notes (Purchase returns / adjustments reducing ITC)."""
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""
    dedup = _dedupe_filter()

    vouchers = conn.execute(f"""
        SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.PARTYGSTIN, v.PLACEOFSUPPLY
        FROM trn_voucher v
        WHERE v.VOUCHERTYPENAME = 'Debit Note'
          {month_filter}
        ORDER BY v.DATE, v.VOUCHERNUMBER
    """).fetchall()

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute(f"""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
              {dedup}
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

        for ledger, amt in entries:
            amt = _safe_float(amt)
            if ledger in PURCHASE_LEDGERS:
                taxable += abs(amt)
            elif ledger in INPUT_CGST_LEDGERS:
                cgst += abs(amt)
            elif ledger in INPUT_SGST_LEDGERS:
                sgst += abs(amt)
            elif ledger in INPUT_IGST_LEDGERS:
                igst += abs(amt)

        if taxable > 0 or cgst > 0 or sgst > 0 or igst > 0:
            total_tax = cgst + sgst + igst
            results.append({
                "date": _format_date(date),
                "date_raw": date,
                "note_no": vchno,
                "supplier": party,
                "gstin": gstin or "",
                "taxable_value": round(taxable, 2),
                "cgst": round(cgst, 2),
                "sgst": round(sgst, 2),
                "igst": round(igst, 2),
                "total_tax": round(total_tax, 2),
            })

    return results


def input_tax_monthly_summary(conn):
    """Month-wise summary of input tax credit."""
    dedup = _dedupe_filter()

    # Purchases
    purchase_rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME = 'Purchase'
          {dedup}
        GROUP BY month, a.LEDGERNAME
    """).fetchall()

    monthly = defaultdict(lambda: {
        "purchase_taxable": 0, "input_cgst": 0, "input_sgst": 0, "input_igst": 0,
    })

    for month, ledger, total in purchase_rows:
        if ledger in PURCHASE_LEDGERS:
            monthly[month]["purchase_taxable"] += total
        elif ledger in INPUT_CGST_LEDGERS:
            monthly[month]["input_cgst"] += total
        elif ledger in INPUT_SGST_LEDGERS:
            monthly[month]["input_sgst"] += total
        elif ledger in INPUT_IGST_LEDGERS:
            monthly[month]["input_igst"] += total

    # Debit Notes (reverse ITC)
    dn_rows = conn.execute(f"""
        SELECT SUBSTR(v.DATE,1,6) as month,
               a.LEDGERNAME,
               SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME = 'Debit Note'
          {dedup}
        GROUP BY month, a.LEDGERNAME
    """).fetchall()

    dn_data = defaultdict(lambda: {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0})
    for month, ledger, total in dn_rows:
        if ledger in PURCHASE_LEDGERS:
            dn_data[month]["taxable"] += total
        elif ledger in INPUT_CGST_LEDGERS:
            dn_data[month]["cgst"] += total
        elif ledger in INPUT_SGST_LEDGERS:
            dn_data[month]["sgst"] += total
        elif ledger in INPUT_IGST_LEDGERS:
            dn_data[month]["igst"] += total

    results = []
    all_months = sorted(set(list(monthly.keys()) + list(dn_data.keys())))
    for month in all_months:
        d = monthly[month]
        dn = dn_data.get(month, {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0})
        net_taxable = d["purchase_taxable"] - dn["taxable"]
        net_cgst = d["input_cgst"] - dn["cgst"]
        net_sgst = d["input_sgst"] - dn["sgst"]
        net_igst = d["input_igst"] - dn["igst"]
        net_itc = net_cgst + net_sgst + net_igst

        results.append({
            "month": month,
            "month_label": _month_label(month),
            "gross_taxable": round(d["purchase_taxable"], 2),
            "dn_taxable": round(dn["taxable"], 2),
            "net_taxable": round(net_taxable, 2),
            "cgst": round(net_cgst, 2),
            "sgst": round(net_sgst, 2),
            "igst": round(net_igst, 2),
            "total_itc": round(net_itc, 2),
        })

    return results


# ══════════════════════════════════════════════════════════════════════════════
#  GSTR-3B COMPUTATION
# ══════════════════════════════════════════════════════════════════════════════

def gstr3b_summary(conn, month=None):
    """Compute GSTR-3B style summary for a given month.
    Returns dict with sections: 3.1, 4, 6.1
    """
    dedup = _dedupe_filter()
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    # ── 3.1 Outward Supplies ──
    # Sales (taxable outward)
    sales_entries = conn.execute(f"""
        SELECT a.LEDGERNAME, SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME IN ('Sales', 'SALE INVOICE')
          {dedup} {month_filter}
        GROUP BY a.LEDGERNAME
    """).fetchall()

    out_taxable = 0.0
    out_cgst = 0.0
    out_sgst = 0.0
    out_igst = 0.0

    for ledger, total in sales_entries:
        if ledger in SALES_LEDGERS:
            out_taxable += total
        elif ledger in OUTPUT_CGST_LEDGERS or ledger in INPUT_CGST_LEDGERS:
            out_cgst += total
        elif ledger in OUTPUT_SGST_LEDGERS or ledger in INPUT_SGST_LEDGERS:
            out_sgst += total
        elif ledger in OUTPUT_IGST_LEDGERS or ledger in INPUT_IGST_LEDGERS:
            out_igst += total

    # Credit Notes (reduce output)
    cn_entries = conn.execute(f"""
        SELECT a.LEDGERNAME, SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME = 'Credit Note'
          {dedup} {month_filter}
        GROUP BY a.LEDGERNAME
    """).fetchall()

    cn_taxable = 0.0
    cn_cgst = 0.0
    cn_sgst = 0.0
    cn_igst = 0.0

    for ledger, total in cn_entries:
        if ledger in SALES_LEDGERS:
            cn_taxable += total
        elif ledger in OUTPUT_CGST_LEDGERS:
            cn_cgst += total
        elif ledger in OUTPUT_SGST_LEDGERS:
            cn_sgst += total
        elif ledger in OUTPUT_IGST_LEDGERS:
            cn_igst += total

    section_3_1 = {
        "a_taxable": round(out_taxable, 2),
        "a_igst": round(out_igst, 2),
        "a_cgst": round(out_cgst, 2),
        "a_sgst": round(out_sgst, 2),
        "a_total_tax": round(out_cgst + out_sgst + out_igst, 2),
        "cn_taxable": round(cn_taxable, 2),
        "cn_igst": round(cn_igst, 2),
        "cn_cgst": round(cn_cgst, 2),
        "cn_sgst": round(cn_sgst, 2),
        "net_taxable": round(out_taxable - cn_taxable, 2),
        "net_igst": round(out_igst - cn_igst, 2),
        "net_cgst": round(out_cgst - cn_cgst, 2),
        "net_sgst": round(out_sgst - cn_sgst, 2),
        "net_total_tax": round((out_cgst + out_sgst + out_igst) - (cn_cgst + cn_sgst + cn_igst), 2),
    }

    # ── 4. Eligible ITC ──
    purchase_entries = conn.execute(f"""
        SELECT a.LEDGERNAME, SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME = 'Purchase'
          {dedup} {month_filter}
        GROUP BY a.LEDGERNAME
    """).fetchall()

    in_taxable = 0.0
    in_cgst = 0.0
    in_sgst = 0.0
    in_igst = 0.0

    for ledger, total in purchase_entries:
        if ledger in PURCHASE_LEDGERS:
            in_taxable += total
        elif ledger in INPUT_CGST_LEDGERS:
            in_cgst += total
        elif ledger in INPUT_SGST_LEDGERS:
            in_sgst += total
        elif ledger in INPUT_IGST_LEDGERS:
            in_igst += total

    # Debit Notes (reverse ITC)
    dn_entries = conn.execute(f"""
        SELECT a.LEDGERNAME, SUM(ABS(CAST(a.AMOUNT AS REAL))) as total
        FROM trn_accounting a
        JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
        WHERE v.VOUCHERTYPENAME = 'Debit Note'
          {dedup} {month_filter}
        GROUP BY a.LEDGERNAME
    """).fetchall()

    dn_cgst = 0.0
    dn_sgst = 0.0
    dn_igst = 0.0

    for ledger, total in dn_entries:
        if ledger in INPUT_CGST_LEDGERS:
            dn_cgst += total
        elif ledger in INPUT_SGST_LEDGERS:
            dn_sgst += total
        elif ledger in INPUT_IGST_LEDGERS:
            dn_igst += total

    section_4 = {
        "itc_igst": round(in_igst, 2),
        "itc_cgst": round(in_cgst, 2),
        "itc_sgst": round(in_sgst, 2),
        "itc_total": round(in_cgst + in_sgst + in_igst, 2),
        "reversal_igst": round(dn_igst, 2),
        "reversal_cgst": round(dn_cgst, 2),
        "reversal_sgst": round(dn_sgst, 2),
        "reversal_total": round(dn_cgst + dn_sgst + dn_igst, 2),
        "net_itc_igst": round(in_igst - dn_igst, 2),
        "net_itc_cgst": round(in_cgst - dn_cgst, 2),
        "net_itc_sgst": round(in_sgst - dn_sgst, 2),
        "net_itc_total": round((in_cgst + in_sgst + in_igst) - (dn_cgst + dn_sgst + dn_igst), 2),
    }

    # ── 6.1 Payment of Tax ──
    net_output_igst = section_3_1["net_igst"]
    net_output_cgst = section_3_1["net_cgst"]
    net_output_sgst = section_3_1["net_sgst"]

    net_input_igst = section_4["net_itc_igst"]
    net_input_cgst = section_4["net_itc_cgst"]
    net_input_sgst = section_4["net_itc_sgst"]

    # IGST ITC can be used against CGST and SGST liability
    # Step 1: Use IGST ITC against IGST liability
    igst_remaining = net_input_igst
    igst_payable = max(net_output_igst - igst_remaining, 0)
    igst_remaining = max(igst_remaining - net_output_igst, 0)

    # Step 2: Use remaining IGST ITC against CGST liability
    cgst_liability = max(net_output_cgst - net_input_cgst, 0)
    cgst_from_igst = min(igst_remaining, cgst_liability)
    cgst_payable = cgst_liability - cgst_from_igst
    igst_remaining -= cgst_from_igst

    # Step 3: Use remaining IGST ITC against SGST liability
    sgst_liability = max(net_output_sgst - net_input_sgst, 0)
    sgst_from_igst = min(igst_remaining, sgst_liability)
    sgst_payable = sgst_liability - sgst_from_igst
    igst_remaining -= sgst_from_igst

    total_payable = igst_payable + cgst_payable + sgst_payable

    section_6_1 = {
        "igst_liability": round(net_output_igst, 2),
        "igst_itc_used": round(net_input_igst - igst_remaining - cgst_from_igst - sgst_from_igst, 2) if net_input_igst > 0 else 0,
        "igst_payable": round(igst_payable, 2),
        "cgst_liability": round(net_output_cgst, 2),
        "cgst_itc_used": round(net_input_cgst + cgst_from_igst, 2),
        "cgst_payable": round(cgst_payable, 2),
        "sgst_liability": round(net_output_sgst, 2),
        "sgst_itc_used": round(net_input_sgst + sgst_from_igst, 2),
        "sgst_payable": round(sgst_payable, 2),
        "total_liability": round(net_output_igst + net_output_cgst + net_output_sgst, 2),
        "total_itc_used": round(section_4["net_itc_total"] - igst_remaining, 2),
        "total_payable": round(total_payable, 2),
        "igst_credit_remaining": round(igst_remaining, 2),
    }

    return {
        "month": month,
        "month_label": _month_label(month) if month else "All Months",
        "company_gstin": COMPANY_GSTIN,
        "section_3_1": section_3_1,
        "section_4": section_4,
        "section_6_1": section_6_1,
    }


def gst_monthly_comparison(conn):
    """Month-wise output vs input comparison with net payable."""
    output_summary = gstr1_monthly_summary(conn)
    input_summary = input_tax_monthly_summary(conn)

    input_map = {r["month"]: r for r in input_summary}

    results = []
    all_months = sorted(set([r["month"] for r in output_summary] + list(input_map.keys())))

    for month in all_months:
        out = next((r for r in output_summary if r["month"] == month), None)
        inp = input_map.get(month)

        out_tax = out["total_tax"] if out else 0
        inp_tax = inp["total_itc"] if inp else 0
        net = out_tax - inp_tax

        results.append({
            "month": month,
            "month_label": _month_label(month),
            "output_taxable": out["net_taxable"] if out else 0,
            "output_tax": round(out_tax, 2),
            "input_taxable": inp["net_taxable"] if inp else 0,
            "input_tax": round(inp_tax, 2),
            "net_payable": round(net, 2),
            "status": "Payable" if net > 0 else "Refundable" if net < 0 else "Nil",
        })

    return results


# ── UTILITY HELPERS ────────────────────────────────────────────────────────

def _format_date(d):
    """Convert YYYYMMDD to DD-MM-YYYY."""
    if d and len(d) == 8:
        return f"{d[6:8]}-{d[4:6]}-{d[0:4]}"
    return d or ""


def _month_label(m):
    """Convert YYYYMM to readable label."""
    month_names = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }
    if m and len(m) == 6:
        return f"{month_names.get(m[4:6], m[4:6])} {m[0:4]}"
    return m or ""


def _hsn_description(hsn):
    """Return description for known pharma HSN codes."""
    hsn_map = {
        "300490": "Medicaments (mixed or unmixed)",
        "30049011": "Ayurvedic medicaments",
        "30049099": "Other medicaments",
        "30042": "Antibiotics",
        "30043": "Hormones",
        "30044": "Alkaloids",
        "30041": "Penicillins",
    }
    return hsn_map.get(hsn, "Pharmaceutical products")


def format_indian(amount):
    """Format number in Indian numbering system (e.g., 12,34,567.89)."""
    if amount is None:
        return "0.00"
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return "0.00"

    is_negative = amount < 0
    amount = abs(amount)

    # Split into integer and decimal parts
    int_part = int(amount)
    dec_part = f"{amount - int_part:.2f}"[1:]  # ".XX"

    s = str(int_part)
    if len(s) <= 3:
        result = s
    else:
        # Last 3 digits, then groups of 2
        result = s[-3:]
        s = s[:-3]
        while s:
            result = s[-2:] + "," + result
            s = s[:-2]

    result = result + dec_part
    if is_negative:
        result = "-" + result

    return result
