"""
GST Computation Engine — Dynamic (works for any company)
Extracts GST data from Tally SQLite database for GSTR-1, GSTR-2, and GSTR-3B.
Auto-detects GST ledgers, company GSTIN, and state from the database.
"""

import sqlite3
import os
import re
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "tally_data.db")

# ── SHARED DEFENSIVE UTILITIES ────────────────────────────────────────────
_TABLE_COLS = {}


def _get_cols(conn, table):
    """Return set of column names for a table (cached per session)."""
    if table not in _TABLE_COLS:
        try:
            _TABLE_COLS[table] = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        except sqlite3.OperationalError:
            _TABLE_COLS[table] = set()
    return _TABLE_COLS[table]


def _table_exists(conn, table):
    """Check if a table exists in the database."""
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        return row is not None
    except sqlite3.OperationalError:
        return False


def clear_col_cache():
    """Clear the column cache (call after re-sync)."""
    _TABLE_COLS.clear()


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


# ── DYNAMIC GST LEDGER DETECTION ──────────────────────────────────────────

def _get_company_gstin(conn=None):
    """Get company GSTIN from metadata (stored during sync)."""
    close = False
    if conn is None:
        conn = get_conn()
        close = True
    try:
        row = conn.execute("SELECT value FROM _metadata WHERE key = 'company_gstin'").fetchone()
        result = row[0] if row else ""
    except Exception:
        result = ""
    if close:
        conn.close()
    return result


def _get_company_state(conn=None):
    """Get company state from metadata (stored during sync)."""
    close = False
    if conn is None:
        conn = get_conn()
        close = True
    try:
        row = conn.execute("SELECT value FROM _metadata WHERE key = 'company_state'").fetchone()
        result = row[0] if row else ""
    except Exception:
        result = ""
    if close:
        conn.close()
    return result


def _get_duty_groups(conn):
    """Get all groups recursively under 'Duties & Taxes'."""
    groups = set()
    try:
        rows = conn.execute("SELECT NAME, PARENT FROM mst_group").fetchall()
    except Exception:
        return groups
    children_map = {}
    for name, parent in rows:
        children_map.setdefault((parent or "").upper(), []).append(name)
    queue = ["Duties & Taxes"]
    while queue:
        current = queue.pop(0)
        groups.add(current)
        for child in children_map.get(current.upper(), []):
            if child not in groups:
                queue.append(child)
    return groups


def _detect_gst_ledgers(conn):
    """Auto-detect GST ledgers from mst_ledger based on parent group and name patterns.
    Returns dict with keys: output_cgst, output_sgst, output_igst,
                            input_cgst, input_sgst, input_igst,
                            unified_cgst, unified_sgst, unified_igst,
                            sales, purchases
    Unified keys hold ledgers that serve as BOTH output and input (e.g., plain "CGST").
    When unified ledgers exist, they are also added to output_* and input_* lists
    so downstream code works without modification.
    """
    cache_key = "_gst_ledger_cache"
    # Check if we already cached this
    if hasattr(_detect_gst_ledgers, cache_key):
        return getattr(_detect_gst_ledgers, cache_key)

    result = {
        "output_cgst": [], "output_sgst": [], "output_igst": [],
        "input_cgst": [], "input_sgst": [], "input_igst": [],
        "unified_cgst": [], "unified_sgst": [], "unified_igst": [],
        "sales": [], "purchases": [],
    }

    try:
        rows = conn.execute(
            "SELECT name, parent FROM mst_ledger ORDER BY name"
        ).fetchall()
    except Exception:
        return result

    # Build set of all groups under Duties & Taxes (recursive)
    duty_groups = _get_duty_groups(conn)

    for name, parent in rows:
        if not name:
            continue
        upper_name = name.upper()
        upper_parent = (parent or "").upper()

        # Check if ledger is under Duties & Taxes hierarchy (recursive)
        is_duty = (parent or "") in duty_groups

        # Output GST detection (explicit OUTPUT in name)
        if "OUTPUT" in upper_name or (is_duty and "OUT" in upper_name):
            if "CGST" in upper_name:
                result["output_cgst"].append(name)
            elif "SGST" in upper_name or "UTGST" in upper_name:
                result["output_sgst"].append(name)
            elif "IGST" in upper_name:
                result["output_igst"].append(name)

        # Input GST detection (explicit INPUT in name)
        elif "INPUT" in upper_name or (is_duty and "INP" in upper_name):
            if "CGST" in upper_name:
                result["input_cgst"].append(name)
            elif "SGST" in upper_name or "UTGST" in upper_name:
                result["input_sgst"].append(name)
            elif "IGST" in upper_name:
                result["input_igst"].append(name)

        # Plain CGST/SGST/IGST under Duties & Taxes (no OUTPUT/INPUT prefix)
        # These are unified ledgers — used for both output and input
        elif is_duty and upper_name in ("CGST",):
            result["unified_cgst"].append(name)
        elif is_duty and upper_name in ("SGST", "UTGST"):
            result["unified_sgst"].append(name)
        elif is_duty and upper_name in ("IGST",):
            result["unified_igst"].append(name)

        # Sales ledgers — under Sales Accounts or similar
        elif "SALES" in upper_parent or "SALE" in upper_parent:
            result["sales"].append(name)
        elif upper_parent == "DIRECT INCOMES" and "SALE" in upper_name:
            result["sales"].append(name)

        # Purchase ledgers — under Purchase Accounts or similar
        elif "PURCHASE" in upper_parent:
            result["purchases"].append(name)
        elif upper_parent == "DIRECT EXPENSES" and "PURCHASE" in upper_name:
            result["purchases"].append(name)

    # ── Unified ledger fallback ──
    # If no separate OUTPUT ledgers found but unified ones exist,
    # add unified ledgers to BOTH output and input lists.
    # Downstream code uses voucher context to determine direction.
    if not result["output_cgst"] and result["unified_cgst"]:
        result["output_cgst"].extend(result["unified_cgst"])
        result["input_cgst"].extend(result["unified_cgst"])
    if not result["output_sgst"] and result["unified_sgst"]:
        result["output_sgst"].extend(result["unified_sgst"])
        result["input_sgst"].extend(result["unified_sgst"])
    if not result["output_igst"] and result["unified_igst"]:
        result["output_igst"].extend(result["unified_igst"])
        result["input_igst"].extend(result["unified_igst"])

    # Cache it
    setattr(_detect_gst_ledgers, cache_key, result)
    return result


def _has_unified_gst(gst):
    """Check if the detected GST ledgers include any unified (combined) ledgers."""
    return bool(gst.get("unified_cgst") or gst.get("unified_sgst") or gst.get("unified_igst"))


def _get_voucher_type_families(conn):
    """Use mst_voucher_type PARENT hierarchy to classify voucher types.
    Returns dict with keys: sales, purchase, credit_note, debit_note, receipt, payment
    Each value is a set of voucher type names belonging to that family.
    """
    cache_key = "_vchtype_family_cache"
    if hasattr(_get_voucher_type_families, cache_key):
        return getattr(_get_voucher_type_families, cache_key)

    families = {
        "sales": set(), "purchase": set(), "credit_note": set(),
        "debit_note": set(), "receipt": set(), "payment": set(),
    }
    parent_map = {
        "SALES": "sales", "PURCHASE": "purchase",
        "CREDIT NOTE": "credit_note", "DEBIT NOTE": "debit_note",
        "RECEIPT": "receipt", "PAYMENT": "payment",
    }
    try:
        rows = conn.execute("SELECT NAME, PARENT FROM mst_voucher_type").fetchall()
        for name, parent in rows:
            if not name:
                continue
            upper_parent = (parent or "").upper()
            upper_name = name.upper()
            for key_parent, family_key in parent_map.items():
                if upper_parent == key_parent or upper_name == key_parent:
                    families[family_key].add(name)
    except Exception:
        families["sales"] = {"Sales"}
        families["purchase"] = {"Purchase"}
        families["credit_note"] = {"Credit Note"}
        families["debit_note"] = {"Debit Note"}
        families["receipt"] = {"Receipt"}
        families["payment"] = {"Payment"}

    setattr(_get_voucher_type_families, cache_key, families)
    return families


def _clear_vchtype_cache():
    """Clear the voucher type family cache."""
    cache_key = "_vchtype_family_cache"
    if hasattr(_get_voucher_type_families, cache_key):
        delattr(_get_voucher_type_families, cache_key)


def _clear_gst_cache():
    """Clear the cached GST ledger detection (call after re-sync)."""
    cache_key = "_gst_ledger_cache"
    if hasattr(_detect_gst_ledgers, cache_key):
        delattr(_detect_gst_ledgers, cache_key)
    _clear_vchtype_cache()


def _classify_gst_ledger(name, gst_ledgers):
    """Return (type, component) for a GST ledger name.
    type: 'output' or 'input'
    component: 'cgst', 'sgst', 'igst'
    """
    if name in gst_ledgers["output_cgst"]:
        return ("output", "cgst")
    if name in gst_ledgers["output_sgst"]:
        return ("output", "sgst")
    if name in gst_ledgers["output_igst"]:
        return ("output", "igst")
    if name in gst_ledgers["input_cgst"]:
        return ("input", "cgst")
    if name in gst_ledgers["input_sgst"]:
        return ("input", "sgst")
    if name in gst_ledgers["input_igst"]:
        return ("input", "igst")
    return (None, None)


def _extract_rate_from_ledger(name):
    """Extract GST rate from ledger name like 'OUTPUT CGST 2.5%' -> 5.0 (doubled for CGST/SGST)."""
    m = re.search(r"(\d+\.?\d*)\s*%?$", name.replace("%", "").strip())
    if m:
        rate = float(m.group(1))
        upper = name.upper()
        if "CGST" in upper or "SGST" in upper:
            return rate * 2  # CGST 2.5% means total GST rate = 5%
        return rate
    return 0.0


# ── AVAILABLE MONTHS ───────────────────────────────────────────────────────

def get_available_months(conn=None):
    """Return list of (YYYYMM, display_label) for months with GST data."""
    close = False
    if conn is None:
        conn = get_conn()
        close = True

    try:
        rows = conn.execute("""
            SELECT DISTINCT SUBSTR(v.DATE, 1, 6) as month
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            JOIN mst_ledger l ON l.name = a.LEDGERNAME
            WHERE UPPER(l.parent) LIKE '%DUTI%' OR UPPER(l.parent) LIKE '%TAX%'
            ORDER BY month
        """).fetchall()
    except sqlite3.OperationalError:
        rows = []

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

def _get_output_gst_voucher_guids(conn, gst, month=None):
    """Find all voucher GUIDs that contain output GST entries (excluding Credit Note family).
    For unified ledgers, restricts to Sales-family voucher types.
    """
    all_output = list(set(gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"]))
    if not all_output:
        return set()
    placeholders = ",".join(["?"] * len(all_output))
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    # Use voucher type families to exclude credit notes properly
    vch_families = _get_voucher_type_families(conn)
    cn_types = list(vch_families.get("credit_note", set())) or ["Credit Note"]
    cn_ph = ",".join(["?"] * len(cn_types))

    has_unified = _has_unified_gst(gst)
    extra_filter = ""
    extra_params = []
    if has_unified:
        # For unified ledgers, restrict to Sales-family voucher types
        sales_types = list(vch_families.get("sales", set())) or ["Sales"]
        sales_ph = ",".join(["?"] * len(sales_types))
        extra_filter = f"AND v.VOUCHERTYPENAME IN ({sales_ph})"
        extra_params = sales_types

    try:
        params = all_output + extra_params + cn_types
        rows = conn.execute(f"""
            SELECT DISTINCT v.GUID
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ({placeholders})
              {extra_filter}
              AND v.VOUCHERTYPENAME NOT IN ({cn_ph})
              {month_filter}
        """, params).fetchall()
        return {r[0] for r in rows}
    except sqlite3.OperationalError:
        return set()


def gstr1_b2b_invoices(conn, month=None):
    """B2B Sales: Invoice-wise with party GSTIN, invoice no, date, taxable, CGST, SGST, IGST.
    Detects sales invoices by presence of output GST entries (works for any voucher type).
    For unified GST ledgers (same ledger for input & output), filters by sales voucher types.
    """
    gst = _detect_gst_ledgers(conn)
    has_unified = _has_unified_gst(gst)
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    # Check column existence
    vcols = _get_cols(conn, "trn_voucher")
    has_partygstin = "PARTYGSTIN" in vcols
    has_pos = "PLACEOFSUPPLY" in vcols

    # Find vouchers with output GST AND party GSTIN
    all_output = gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"]
    if not all_output:
        return []
    if not has_partygstin:
        return []  # Cannot identify B2B without PARTYGSTIN column
    placeholders = ",".join(["?"] * len(all_output))
    pos_col = "v.PLACEOFSUPPLY" if has_pos else "'' AS PLACEOFSUPPLY"

    # For unified GST ledgers, restrict to sales voucher types to avoid
    # picking up purchases, receipts, payments, etc.
    vchtype_filter = ""
    vchtype_params = []
    if has_unified:
        vch_families = _get_voucher_type_families(conn)
        sales_types = list(vch_families.get("sales", set()))
        if not sales_types:
            sales_types = ["Sales"]
        vchtype_ph = ",".join(["?"] * len(sales_types))
        vchtype_filter = f"AND v.VOUCHERTYPENAME IN ({vchtype_ph})"
        vchtype_params = sales_types

    # Build CN exclusion list from voucher type families
    cn_exclude = ""
    cn_params = []
    if has_unified:
        cn_types = list(vch_families.get("credit_note", set()))
    else:
        cn_types = ["Credit Note"]
    if cn_types:
        cn_ph = ",".join(["?"] * len(cn_types))
        cn_exclude = f"AND v.VOUCHERTYPENAME NOT IN ({cn_ph})"
        cn_params = cn_types

    try:
        params = all_output + vchtype_params + cn_params
        vouchers = conn.execute(f"""
            SELECT DISTINCT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, v.PARTYGSTIN,
                   {pos_col}, v.VOUCHERTYPENAME
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ({placeholders})
              {vchtype_filter}
              {cn_exclude}
              AND v.PARTYGSTIN IS NOT NULL AND v.PARTYGSTIN != ''
              {month_filter}
            ORDER BY v.DATE, v.VOUCHERNUMBER
        """, params).fetchall()
    except sqlite3.OperationalError:
        return []

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos, vchtype in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute("""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
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

        if taxable > 0 or cgst > 0 or sgst > 0 or igst > 0:
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
    """B2C Sales: Where party has no GSTIN.
    Detects sales by presence of output GST entries.
    For unified GST ledgers, filters by sales voucher types.
    """
    gst = _detect_gst_ledgers(conn)
    has_unified = _has_unified_gst(gst)
    company_state = _get_company_state(conn)
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    vcols = _get_cols(conn, "trn_voucher")
    has_partygstin = "PARTYGSTIN" in vcols
    has_pos = "PLACEOFSUPPLY" in vcols

    all_output = gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"]
    if not all_output:
        return []
    placeholders = ",".join(["?"] * len(all_output))
    pos_col = "v.PLACEOFSUPPLY" if has_pos else "'' AS PLACEOFSUPPLY"
    gstin_filter = "AND (v.PARTYGSTIN IS NULL OR v.PARTYGSTIN = '')" if has_partygstin else ""

    # For unified GST ledgers, restrict to sales voucher types
    vchtype_filter = ""
    vchtype_params = []
    if has_unified:
        vch_families = _get_voucher_type_families(conn)
        sales_types = list(vch_families.get("sales", set()))
        if not sales_types:
            sales_types = ["Sales"]
        vchtype_ph = ",".join(["?"] * len(sales_types))
        vchtype_filter = f"AND v.VOUCHERTYPENAME IN ({vchtype_ph})"
        vchtype_params = sales_types

    # Build CN exclusion list
    cn_exclude = ""
    cn_params = []
    if has_unified:
        cn_types = list(vch_families.get("credit_note", set()))
    else:
        cn_types = ["Credit Note"]
    if cn_types:
        cn_ph = ",".join(["?"] * len(cn_types))
        cn_exclude = f"AND v.VOUCHERTYPENAME NOT IN ({cn_ph})"
        cn_params = cn_types

    try:
        params = all_output + vchtype_params + cn_params
        vouchers = conn.execute(f"""
            SELECT DISTINCT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, {pos_col}
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ({placeholders})
              {vchtype_filter}
              {cn_exclude}
              {gstin_filter}
              {month_filter}
            ORDER BY v.DATE, v.VOUCHERNUMBER
        """, params).fetchall()
    except sqlite3.OperationalError:
        return []

    results = []
    seen_guids = set()
    for guid, date, vchno, party, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute("""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
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

        if taxable > 0 or cgst > 0 or sgst > 0 or igst > 0:
            total_tax = cgst + sgst + igst
            gst_rate = round((total_tax / taxable * 100), 1) if taxable > 0 else 0.0
            results.append({
                "date": _format_date(date),
                "date_raw": date,
                "invoice_no": vchno,
                "party": party,
                "place_of_supply": pos or company_state,
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
    """Credit Notes (Sales Returns).
    Detects by Credit Note voucher type family with output GST entries.
    Uses voucher type families to capture all CN sub-types (e.g., CREDIT NOTE ONLINE).
    """
    gst = _detect_gst_ledgers(conn)
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    vcols = _get_cols(conn, "trn_voucher")
    gstin_col = "v.PARTYGSTIN" if "PARTYGSTIN" in vcols else "'' AS PARTYGSTIN"
    pos_col = "v.PLACEOFSUPPLY" if "PLACEOFSUPPLY" in vcols else "'' AS PLACEOFSUPPLY"

    # Use voucher type family for credit note detection
    vch_families = _get_voucher_type_families(conn)
    cn_types = list(vch_families.get("credit_note", set()))
    if not cn_types:
        cn_types = ["Credit Note"]
    cn_ph = ",".join(["?"] * len(cn_types))

    all_output = gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"]
    if not all_output:
        # Fallback: just get Credit Note vouchers
        try:
            vouchers = conn.execute(f"""
                SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, {gstin_col}, {pos_col}
                FROM trn_voucher v
                WHERE v.VOUCHERTYPENAME IN ({cn_ph})
                  {month_filter}
                ORDER BY v.DATE, v.VOUCHERNUMBER
            """, cn_types).fetchall()
        except sqlite3.OperationalError:
            return []
    else:
        placeholders = ",".join(["?"] * len(all_output))
        try:
            vouchers = conn.execute(f"""
                SELECT DISTINCT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, {gstin_col}, {pos_col}
                FROM trn_voucher v
                JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                WHERE v.VOUCHERTYPENAME IN ({cn_ph})
                  AND a.LEDGERNAME IN ({placeholders})
                  {month_filter}
                ORDER BY v.DATE, v.VOUCHERNUMBER
            """, cn_types + all_output).fetchall()
        except sqlite3.OperationalError:
            return []

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute("""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
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
    gst = _detect_gst_ledgers(conn)
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    acct_cols = _get_cols(conn, "trn_accounting")
    if "GSTHSNNAME" not in acct_cols:
        return []

    # Detect sales vouchers dynamically: vouchers containing output GST entries
    all_output = gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"]
    if not all_output:
        return []
    out_ph = ",".join(["?"] * len(all_output))

    try:
        rows = conn.execute(f"""
            SELECT a.GSTHSNNAME, a.LEDGERNAME, ABS(SUM(CAST(a.AMOUNT AS REAL))) as total
            FROM trn_accounting a
            JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
            WHERE a.VOUCHER_GUID IN (
                SELECT DISTINCT a2.VOUCHER_GUID FROM trn_accounting a2
                WHERE a2.LEDGERNAME IN ({out_ph})
            )
              AND v.VOUCHERTYPENAME != 'Credit Note'
              AND a.GSTHSNNAME IS NOT NULL AND a.GSTHSNNAME != ''
              {month_filter}
            GROUP BY a.GSTHSNNAME, a.LEDGERNAME
        """, all_output).fetchall()
    except sqlite3.OperationalError:
        rows = []

    hsn_data = defaultdict(lambda: {"taxable": 0, "cgst": 0, "sgst": 0, "igst": 0})
    for hsn, ledger, total in rows:
        if ledger in gst["sales"]:
            hsn_data[hsn]["taxable"] += total
        elif ledger in gst["output_cgst"]:
            hsn_data[hsn]["cgst"] += total
        elif ledger in gst["output_sgst"]:
            hsn_data[hsn]["sgst"] += total
        elif ledger in gst["output_igst"]:
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
    """Month-wise summary of all output GST.
    Uses signed amounts across ALL voucher types — this matches Tally's closing
    balances exactly (credit notes are negative, reverse charge nets to zero).
    Detects sales vouchers dynamically by presence of output GST entries.
    """
    gst = _detect_gst_ledgers(conn)

    # Build set of output GST ledger names for fast lookup
    all_output_set = set(gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"])

    # All entries with signed amounts (Tally convention: positive=credit, negative=debit)
    try:
        all_rows = conn.execute("""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   a.LEDGERNAME,
                   SUM(CAST(a.AMOUNT AS REAL)) as signed_total,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as abs_total,
                   v.VOUCHERTYPENAME
            FROM trn_accounting a
            JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
            GROUP BY month, a.LEDGERNAME, v.VOUCHERTYPENAME
        """).fetchall()
    except sqlite3.OperationalError:
        return []

    # Identify which voucher types are sales types (contain output GST entries)
    # and which are credit note types
    sales_vchtypes = set()
    cn_vchtypes = set()
    for month, ledger, signed, absolute, vchtype in all_rows:
        if ledger in all_output_set and absolute > 0:
            if vchtype and "credit" in vchtype.lower():
                cn_vchtypes.add(vchtype)
            else:
                sales_vchtypes.add(vchtype)

    monthly = defaultdict(lambda: {
        "sales_taxable": 0, "cn_taxable": 0,
        "output_cgst": 0, "output_sgst": 0, "output_igst": 0,
    })

    for month, ledger, signed, absolute, vchtype in all_rows:
        is_sales = vchtype in sales_vchtypes
        is_cn = vchtype in cn_vchtypes

        if ledger in gst["sales"]:
            if is_sales:
                monthly[month]["sales_taxable"] += absolute
            elif is_cn:
                monthly[month]["cn_taxable"] += absolute

        # For GST amounts, use signed sum across all voucher types
        if ledger in gst["output_cgst"]:
            monthly[month]["output_cgst"] += signed
        elif ledger in gst["output_sgst"]:
            monthly[month]["output_sgst"] += signed
        elif ledger in gst["output_igst"]:
            monthly[month]["output_igst"] += signed

    results = []
    for month in sorted(monthly.keys()):
        d = monthly[month]
        # Skip months with no output GST activity
        if d["sales_taxable"] == 0 and d["output_cgst"] == 0 and d["output_sgst"] == 0 and d["output_igst"] == 0:
            continue
        net_taxable = d["sales_taxable"] - d["cn_taxable"]
        # GST amounts are already net (signed sums)
        net_cgst = d["output_cgst"]
        net_sgst = d["output_sgst"]
        net_igst = d["output_igst"]
        net_total_tax = net_cgst + net_sgst + net_igst

        results.append({
            "month": month,
            "month_label": _month_label(month),
            "gross_taxable": round(d["sales_taxable"], 2),
            "cn_taxable": round(d["cn_taxable"], 2),
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
    """Purchase-wise input tax credit detail.
    Detects by presence of input GST entries (works for any voucher type).
    """
    gst = _detect_gst_ledgers(conn)
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    vcols = _get_cols(conn, "trn_voucher")
    gstin_col = "v.PARTYGSTIN" if "PARTYGSTIN" in vcols else "'' AS PARTYGSTIN"
    pos_col = "v.PLACEOFSUPPLY" if "PLACEOFSUPPLY" in vcols else "'' AS PLACEOFSUPPLY"

    all_input = gst["input_cgst"] + gst["input_sgst"] + gst["input_igst"]
    if not all_input:
        return []
    placeholders = ",".join(["?"] * len(all_input))
    try:
        vouchers = conn.execute(f"""
            SELECT DISTINCT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, {gstin_col}, {pos_col}
            FROM trn_voucher v
            JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
            WHERE a.LEDGERNAME IN ({placeholders})
              AND v.VOUCHERTYPENAME != 'Debit Note'
              {month_filter}
            ORDER BY v.DATE, v.VOUCHERNUMBER
        """, all_input).fetchall()
    except sqlite3.OperationalError:
        return []

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute("""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

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
    """Debit Notes (Purchase returns / adjustments reducing ITC).
    Detects by Debit Note voucher type with input GST entries.
    """
    gst = _detect_gst_ledgers(conn)
    month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'" if month else ""

    vcols = _get_cols(conn, "trn_voucher")
    gstin_col = "v.PARTYGSTIN" if "PARTYGSTIN" in vcols else "'' AS PARTYGSTIN"
    pos_col = "v.PLACEOFSUPPLY" if "PLACEOFSUPPLY" in vcols else "'' AS PLACEOFSUPPLY"

    all_input = gst["input_cgst"] + gst["input_sgst"] + gst["input_igst"]
    if not all_input:
        try:
            vouchers = conn.execute(f"""
                SELECT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, {gstin_col}, {pos_col}
                FROM trn_voucher v
                WHERE v.VOUCHERTYPENAME = 'Debit Note'
                  {month_filter}
                ORDER BY v.DATE, v.VOUCHERNUMBER
            """).fetchall()
        except sqlite3.OperationalError:
            return []
    else:
        placeholders = ",".join(["?"] * len(all_input))
        try:
            vouchers = conn.execute(f"""
                SELECT DISTINCT v.GUID, v.DATE, v.VOUCHERNUMBER, v.PARTYLEDGERNAME, {gstin_col}, {pos_col}
                FROM trn_voucher v
                JOIN trn_accounting a ON a.VOUCHER_GUID = v.GUID
                WHERE v.VOUCHERTYPENAME = 'Debit Note'
                  AND a.LEDGERNAME IN ({placeholders})
                  {month_filter}
                ORDER BY v.DATE, v.VOUCHERNUMBER
            """, all_input).fetchall()
        except sqlite3.OperationalError:
            return []

    results = []
    seen_guids = set()
    for guid, date, vchno, party, gstin, pos in vouchers:
        if guid in seen_guids:
            continue
        seen_guids.add(guid)

        entries = conn.execute("""
            SELECT a.LEDGERNAME, CAST(a.AMOUNT AS REAL) as amt
            FROM trn_accounting a
            WHERE a.VOUCHER_GUID = ?
        """, (guid,)).fetchall()

        taxable = 0.0
        cgst = 0.0
        sgst = 0.0
        igst = 0.0

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
    """Month-wise summary of input tax credit.
    Uses signed amounts across ALL voucher types to match Tally closing balances.
    Detects purchase vouchers dynamically by presence of input GST entries.
    """
    gst = _detect_gst_ledgers(conn)

    # Build set of input GST ledger names for fast lookup
    all_input_set = set(gst["input_cgst"] + gst["input_sgst"] + gst["input_igst"])

    try:
        all_rows = conn.execute("""
            SELECT SUBSTR(v.DATE,1,6) as month,
                   a.LEDGERNAME,
                   SUM(CAST(a.AMOUNT AS REAL)) as signed_total,
                   SUM(ABS(CAST(a.AMOUNT AS REAL))) as abs_total,
                   v.VOUCHERTYPENAME
            FROM trn_accounting a
            JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
            GROUP BY month, a.LEDGERNAME, v.VOUCHERTYPENAME
        """).fetchall()
    except sqlite3.OperationalError:
        return []

    # Identify purchase and debit note voucher types dynamically
    purchase_vchtypes = set()
    dn_vchtypes = set()
    for month, ledger, signed, absolute, vchtype in all_rows:
        if ledger in all_input_set and absolute > 0:
            if vchtype and "debit" in vchtype.lower():
                dn_vchtypes.add(vchtype)
            else:
                purchase_vchtypes.add(vchtype)

    monthly = defaultdict(lambda: {
        "purchase_taxable": 0, "dn_taxable": 0,
        "input_cgst": 0, "input_sgst": 0, "input_igst": 0,
    })

    for month, ledger, signed, absolute, vchtype in all_rows:
        is_purchase = vchtype in purchase_vchtypes
        is_dn = vchtype in dn_vchtypes

        if ledger in gst["purchases"]:
            if is_purchase:
                monthly[month]["purchase_taxable"] += absolute
            elif is_dn:
                monthly[month]["dn_taxable"] += absolute

        # For ITC amounts, use signed sum (negative = reversal)
        if ledger in gst["input_cgst"]:
            monthly[month]["input_cgst"] += signed
        elif ledger in gst["input_sgst"]:
            monthly[month]["input_sgst"] += signed
        elif ledger in gst["input_igst"]:
            monthly[month]["input_igst"] += signed

    results = []
    for month in sorted(monthly.keys()):
        d = monthly[month]
        if d["purchase_taxable"] == 0 and d["input_cgst"] == 0 and d["input_sgst"] == 0 and d["input_igst"] == 0:
            continue
        net_taxable = d["purchase_taxable"] - d["dn_taxable"]
        # ITC amounts are already net (signed sums) — negative in Tally = ITC available
        net_cgst = abs(d["input_cgst"])
        net_sgst = abs(d["input_sgst"])
        net_igst = abs(d["input_igst"])
        net_itc = net_cgst + net_sgst + net_igst

        results.append({
            "month": month,
            "month_label": _month_label(month),
            "gross_taxable": round(d["purchase_taxable"], 2),
            "dn_taxable": round(d["dn_taxable"], 2),
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
    When month is None (all months), uses closing balances from mst_ledger
    which are verified to match Tally exactly.
    When month is specified, computes from voucher entries for that month.
    """
    gst = _detect_gst_ledgers(conn)
    company_gstin = _get_company_gstin(conn)

    if month:
        # Month-specific: sum from vouchers that contain output/input GST
        month_filter = f"AND SUBSTR(v.DATE,1,6) = '{month}'"
        has_unified = _has_unified_gst(gst)

        # Determine voucher type families for unified ledger direction detection
        vch_families = _get_voucher_type_families(conn) if has_unified else {}
        sales_types = vch_families.get("sales", set()) if has_unified else set()
        purchase_types = vch_families.get("purchase", set()) if has_unified else set()
        cn_types = vch_families.get("credit_note", set()) if has_unified else set()
        dn_types = vch_families.get("debit_note", set()) if has_unified else set()

        # Build deduplicated GST ledger lists
        all_gst_ledgers = list(set(
            gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"] +
            gst["input_cgst"] + gst["input_sgst"] + gst["input_igst"]
        ))
        all_output = list(set(gst["output_cgst"] + gst["output_sgst"] + gst["output_igst"]))
        all_input = list(set(gst["input_cgst"] + gst["input_sgst"] + gst["input_igst"]))

        # Sets for fast lookup
        unified_cgst_set = set(gst.get("unified_cgst", []))
        unified_sgst_set = set(gst.get("unified_sgst", []))
        unified_igst_set = set(gst.get("unified_igst", []))
        out_cgst_set = set(gst["output_cgst"])
        out_sgst_set = set(gst["output_sgst"])
        out_igst_set = set(gst["output_igst"])
        in_cgst_set = set(gst["input_cgst"])
        in_sgst_set = set(gst["input_sgst"])
        in_igst_set = set(gst["input_igst"])
        sales_set = set(gst["sales"])
        purchase_set = set(gst["purchases"])

        # Output: sum absolute amounts from vouchers containing output GST
        out_taxable = 0.0
        out_cgst = 0.0
        out_sgst = 0.0
        out_igst = 0.0

        if all_output:
            ph = ",".join(["?"] * len(all_output))
            if has_unified:
                # For unified ledgers, use voucher type to identify output
                out_type_list = list(sales_types)
                if not out_type_list:
                    out_type_list = ["Sales"]
                out_type_ph = ",".join(["?"] * len(out_type_list))
                out_guids = conn.execute(f"""
                    SELECT DISTINCT a.VOUCHER_GUID FROM trn_accounting a
                    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                    WHERE a.LEDGERNAME IN ({ph})
                      AND v.VOUCHERTYPENAME IN ({out_type_ph})
                      AND v.VOUCHERTYPENAME NOT IN (SELECT NAME FROM mst_voucher_type WHERE UPPER(PARENT)='CREDIT NOTE')
                      {month_filter}
                """, all_output + out_type_list).fetchall()
            else:
                out_guids = conn.execute(f"""
                    SELECT DISTINCT a.VOUCHER_GUID FROM trn_accounting a
                    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                    WHERE a.LEDGERNAME IN ({ph}) AND CAST(a.AMOUNT AS REAL) > 0
                      {month_filter}
                """, all_output).fetchall()
            out_guid_set = {r[0] for r in out_guids}

            for guid in out_guid_set:
                entries = conn.execute(
                    "SELECT LEDGERNAME, CAST(AMOUNT AS REAL) FROM trn_accounting WHERE VOUCHER_GUID=?",
                    (guid,)
                ).fetchall()
                for ledger, amt in entries:
                    amt = _safe_float(amt)
                    if ledger in sales_set:
                        out_taxable += abs(amt)
                    elif ledger in out_cgst_set:
                        out_cgst += abs(amt)
                    elif ledger in out_sgst_set:
                        out_sgst += abs(amt)
                    elif ledger in out_igst_set:
                        out_igst += abs(amt)

        # Credit notes for this month
        cn_taxable = 0.0
        cn_cgst = 0.0
        cn_sgst = 0.0
        cn_igst = 0.0

        if all_output:
            ph = ",".join(["?"] * len(all_output))
            # Use voucher type family for credit notes
            cn_type_list = list(cn_types) if cn_types else ["Credit Note"]
            cn_type_ph = ",".join(["?"] * len(cn_type_list))
            cn_guids = conn.execute(f"""
                SELECT DISTINCT a.VOUCHER_GUID FROM trn_accounting a
                JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                WHERE a.LEDGERNAME IN ({ph}) AND v.VOUCHERTYPENAME IN ({cn_type_ph})
                  {month_filter}
            """, all_output + cn_type_list).fetchall()
            for (guid,) in cn_guids:
                entries = conn.execute(
                    "SELECT LEDGERNAME, CAST(AMOUNT AS REAL) FROM trn_accounting WHERE VOUCHER_GUID=?",
                    (guid,)
                ).fetchall()
                for ledger, amt in entries:
                    amt = _safe_float(amt)
                    if ledger in sales_set:
                        cn_taxable += abs(amt)
                    elif ledger in out_cgst_set:
                        cn_cgst += abs(amt)
                    elif ledger in out_sgst_set:
                        cn_sgst += abs(amt)
                    elif ledger in out_igst_set:
                        cn_igst += abs(amt)

        # Input: from purchase vouchers with input GST
        in_taxable = 0.0
        in_cgst = 0.0
        in_sgst = 0.0
        in_igst = 0.0

        if all_input:
            ph_in = ",".join(["?"] * len(all_input))
            if has_unified:
                # For unified ledgers, use voucher type to identify input
                in_type_list = list(purchase_types)
                if not in_type_list:
                    in_type_list = ["Purchase"]
                in_type_ph = ",".join(["?"] * len(in_type_list))
                in_guids = conn.execute(f"""
                    SELECT DISTINCT a.VOUCHER_GUID FROM trn_accounting a
                    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                    WHERE a.LEDGERNAME IN ({ph_in})
                      AND v.VOUCHERTYPENAME IN ({in_type_ph})
                      AND v.VOUCHERTYPENAME NOT IN (SELECT NAME FROM mst_voucher_type WHERE UPPER(PARENT)='DEBIT NOTE')
                      {month_filter}
                """, all_input + in_type_list).fetchall()
            else:
                in_guids = conn.execute(f"""
                    SELECT DISTINCT a.VOUCHER_GUID FROM trn_accounting a
                    JOIN trn_voucher v ON v.GUID = a.VOUCHER_GUID
                    WHERE a.LEDGERNAME IN ({ph_in}) AND v.VOUCHERTYPENAME != 'Debit Note'
                      {month_filter}
                """, all_input).fetchall()
            for (guid,) in in_guids:
                entries = conn.execute(
                    "SELECT LEDGERNAME, CAST(AMOUNT AS REAL) FROM trn_accounting WHERE VOUCHER_GUID=?",
                    (guid,)
                ).fetchall()
                for ledger, amt in entries:
                    amt = _safe_float(amt)
                    if ledger in purchase_set:
                        in_taxable += abs(amt)
                    elif ledger in in_cgst_set:
                        in_cgst += abs(amt)
                    elif ledger in in_sgst_set:
                        in_sgst += abs(amt)
                    elif ledger in in_igst_set:
                        in_igst += abs(amt)

    else:
        # All months: use closing balances from mst_ledger (verified exact match with Tally)
        out_taxable = 0.0
        out_cgst = 0.0
        out_sgst = 0.0
        out_igst = 0.0
        in_taxable = 0.0
        in_cgst = 0.0
        in_sgst = 0.0
        in_igst = 0.0
        cn_taxable = 0.0
        cn_cgst = 0.0
        cn_sgst = 0.0
        cn_igst = 0.0

        has_unified = _has_unified_gst(gst)

        try:
            _gst_cols = {r[1] for r in conn.execute("PRAGMA table_info(mst_ledger)").fetchall()}
            _gst_bc = "COMPUTED_CB" if "COMPUTED_CB" in _gst_cols else "CLOSINGBALANCE"
            rows = conn.execute(f"SELECT name, {_gst_bc} FROM mst_ledger").fetchall()
        except Exception:
            rows = []
        for name, cb in rows:
            bal = _safe_float(cb)
            if bal == 0:
                continue

            if has_unified:
                # Unified ledgers: use sign of closing balance to determine direction.
                # Negative CB = net output liability (credit balance in Tally)
                # Positive CB = net input credit (debit balance in Tally)
                if name in gst.get("unified_cgst", []):
                    if bal < 0:
                        out_cgst += abs(bal)
                    else:
                        in_cgst += bal
                elif name in gst.get("unified_sgst", []):
                    if bal < 0:
                        out_sgst += abs(bal)
                    else:
                        in_sgst += bal
                elif name in gst.get("unified_igst", []):
                    if bal < 0:
                        out_igst += abs(bal)
                    else:
                        in_igst += bal
                elif name in gst["sales"]:
                    out_taxable += abs(bal)
                elif name in gst["purchases"]:
                    in_taxable += abs(bal)
            else:
                # Separate OUTPUT/INPUT ledgers: original logic
                if name in gst["output_cgst"]:
                    out_cgst += bal
                elif name in gst["output_sgst"]:
                    out_sgst += bal
                elif name in gst["output_igst"]:
                    out_igst += bal
                elif name in gst["input_cgst"]:
                    in_cgst += abs(bal)
                elif name in gst["input_sgst"]:
                    in_sgst += abs(bal)
                elif name in gst["input_igst"]:
                    in_igst += abs(bal)
                elif name in gst["sales"]:
                    out_taxable += abs(bal)
                elif name in gst["purchases"]:
                    in_taxable += abs(bal)

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

    section_4 = {
        "itc_igst": round(in_igst, 2),
        "itc_cgst": round(in_cgst, 2),
        "itc_sgst": round(in_sgst, 2),
        "itc_total": round(in_cgst + in_sgst + in_igst, 2),
        "reversal_igst": 0.0,
        "reversal_cgst": 0.0,
        "reversal_sgst": 0.0,
        "reversal_total": 0.0,
        "net_itc_igst": round(in_igst, 2),
        "net_itc_cgst": round(in_cgst, 2),
        "net_itc_sgst": round(in_sgst, 2),
        "net_itc_total": round(in_cgst + in_sgst + in_igst, 2),
    }

    # ── 6.1 Payment of Tax ──
    net_output_igst = section_3_1["net_igst"]
    net_output_cgst = section_3_1["net_cgst"]
    net_output_sgst = section_3_1["net_sgst"]

    net_input_igst = section_4["net_itc_igst"]
    net_input_cgst = section_4["net_itc_cgst"]
    net_input_sgst = section_4["net_itc_sgst"]

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
        "company_gstin": company_gstin,
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
    """Return description for known HSN codes."""
    hsn_map = {
        "300490": "Medicaments (mixed or unmixed)",
        "30049011": "Ayurvedic medicaments",
        "30049099": "Other medicaments",
        "30042": "Antibiotics",
        "30043": "Hormones",
        "30044": "Alkaloids",
        "30041": "Penicillins",
    }
    return hsn_map.get(hsn, hsn)


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

    int_part = int(amount)
    dec_part = f"{amount - int_part:.2f}"[1:]

    s = str(int_part)
    if len(s) <= 3:
        result = s
    else:
        result = s[-3:]
        s = s[:-3]
        while s:
            result = s[-2:] + "," + result
            s = s[:-2]

    result = result + dec_part
    if is_negative:
        result = "-" + result

    return result
